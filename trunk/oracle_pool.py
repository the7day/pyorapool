#!/usr/bin/python
# vim: ai ts=4 sts=4 et sw=4

# Copyright (C) 2007-2008 Music Pictures Ltd 2007-2010. 
# Authors for this file:
#                       Stefan Bethge <stefan@musicpictures.com>
#                       Simon Redfern <simon@musicpictures.com>

## This program is free software; you can redistribute it and/or modify
## it under the terms of the GNU Lesser General Public License as published
## by the Free Software Foundation; version 3 or newer.
##
## This program is distributed in the hope that it will be useful,
## but WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
## GNU Lesser General Public License for more details. 

import Pyro.core
import threading
import cx_Oracle
import time
from util.uuid import *
from oracle_pool_procedures import *
import logging as log

#import django settings
#replace settings variable with your own
#if you want to use this without django

import os
os.environ['NLS_LANG'] = '.UTF8'
os.environ['DJANGO_SETTINGS_MODULE']='settings'

if os.name != 'nt':
    import signal

from django.conf import settings
from django.utils.encoding import smart_str, force_unicode

if os.name != 'nt':
    log.basicConfig(
        level=log.INFO,
        format='%(asctime)s %(levelname)-8s %(message)s',
        datefmt='%m-%d %H:%M',
        filename='/var/log/calypso/oracle_pool.log', 
        filemode='a'
    )
else:
    log.basicConfig(
        level=log.INFO,
        format='%(asctime)s %(levelname)-8s %(message)s',
        datefmt='%m-%d %H:%M',
        filename='C:\Oracle\log\oracle_pool.log', 
        filemode='a'
    )
        

# timeout for unused connections in seconds
timeout = settings.PYRO_POOL_TIMEOUT

# timeout for connections that are locked but might be dead
timeout_locked = settings.PYRO_POOL_LOCKED_TIMEOUT

connections = []
lock = threading.RLock()

#serverside_cursors = []

def to_unicode(s):
    """
    Convert strings to Unicode objects (and return all other data types
    unchanged).
    """
    if isinstance(s, basestring):
        return force_unicode(s)
    return s

#wrapper classes for django backend
class CursorWrapper(Pyro.core.ObjBase):
    cursor = None
    id = str(uuid1)
    charset = 'utf-8'
    is_wrapper = True
    daemon = None
    _insert_id_var = None
    
    #check for wrapper with hasattr(cursor, is_wrapper) ?

    def __init__(self, connection, *args, **kwargs):
        Pyro.core.ObjBase.__init__(self)
        global daemon
        self.daemon = daemon

        self.cursor = connection.cursor(*args, **kwargs)
        # This looks like a hard limit on number of cursors?
        self.cursor.arraysize = 100
        #self._insert_id_var = self.cursor._insert_id_var
        
    def get_description(self):
        result = self.cursor.description
        log.debug('description: %s' % result)
        return result

    description = property(get_description)

    def _format_params(self, params):
        if isinstance(params, dict):
            result = {}
            charset = self.charset
            for key, value in params.items():
                result[smart_str(key, charset)] = smart_str(value, charset)
            return result
        else:
            return tuple([smart_str(p, self.charset, True) for p in params])

    def close(self):
        self.cursor.close()
        #TODO:implement check and give error if anything is called after close?
        #or destroy self somehow
        #del self ?

    #not used right now
    def convert_cx_var(self, data):
        result = []
        if data:
            for dat in data:
                print 'dat: %s' % dat
                print 'dat.alloc: %s' % dat.allocelems
                for i in range(0,dat.allocelems):
                    val = dat.getvalue(pos=i) 
                    if val:
                        print 'val: %s' % val
                        result.append(val)
        print 'result: %s' % result
        return result

    def execute(self, query, params=None):
        if params is None:
            params = []
        else:
            params = self._format_params(params)

        #log.debug('execute query %s with params %s' % (str(query), str(params)))

        ## taken from django source / oracle backend
        args = [(':arg%d' % i) for i in range(len(params))]
        # cx_Oracle wants no trailing ';' for SQL statements.  For PL/SQL, it
        # it does want a trailing ';' but not a trailing '/'.  However, these
        # characters must be included in the original query in case the query
        # is being passed to SQL*Plus.
        if query.endswith(';') or query.endswith('/'):
            query = query[:-1]
        query = smart_str(query, self.charset) % tuple(args)
        try:
            self.cursor.execute(query, params)
            #result = self.cursor.fetchall()
            log.debug('execute query %s with params %s' % (str(query), str(params)))
        except Exception, e:
            log.warning("Exception: %s" % e)

    def executemany(self, query, params=None):
        try:
            args = [(':arg%d' % i) for i in range(len(params[0]))]
        except (IndexError, TypeError):
            # No params given, nothing to do
            return None
        # cx_Oracle wants no trailing ';' for SQL statements.  For PL/SQL, it
        # it does want a trailing ';' but not a trailing '/'.  However, these
        # characters must be included in the original query in case the query
        # is being passed to SQL*Plus.
        if query.endswith(';') or query.endswith('/'):
            query = query[:-1]
        query = smart_str(query, self.charset) % tuple(args)
        new_param_list = [self._format_params(i) for i in params]
        #return self.cursor.executemany(query, new_param_list)
        try:
            self.cursor.executemany(query, new_param_list)
        except Exception, e:
            log.warning("Exception: %s" % e)

    def fetchone(self):
        return to_unicode(self.cursor.fetchone())

    def fetchmany(self, size=None):
        if size is None:
            size = self.cursor.arraysize
        return tuple([tuple([to_unicode(e) for e in r]) for r in self.cursor.fetchmany(size)])

    def fetchall(self):
        return tuple([tuple([to_unicode(e) for e in r]) for r in self.cursor.fetchall()])
 
    def var(self, *args, **kwargs):
        #TODO: need wrapper for that
        return self.cursor.var(*args, **kwargs)

    def rowcount(self):
        return self.cursor.rowcount()


    """
    #doesn't work right now, refcursors can't be pickled, proxy?
    def callproc(self, statement, args):
        # replace serverside id with real cursor
        # this is a workaround for pyro shortcoming
        # that does not allow proxies as an argument somehow
        count = 0
        for arg in args:
            for c in serverside_cursors:
                if arg == 'id=%s' % c.id:
                    args[count] = c
            count+=1

        self.cursor.callproc(statement, args)

    #return a unique id for this cursor and keep it
    def get_serverside_cursor(self):
        global serverside_cursors
        for c in serverside_cursors:
            if c.id == self.id:
                return 'id=%s' % c.id
        serverside_cursors.append(self)
        return 'id=%s' % self.id
   
    def remove_serverside_cursor(self):
        global serverside_cursors
        for c in serverside_cursors:
            if c.id == self.id:
                serverside_cursors.remove(c)
    """

class ConnectionWrapper(Pyro.core.ObjBase):
    connection = None
    locked = False
    last_used = time.time()
    id = last_used
    daemon = None

    def __init__(self, *args, **kwargs):
        Pyro.core.ObjBase.__init__(self)
        global daemon
        self.daemon = daemon

        self.id = id(self)
        try:
            #self.connection = cx_Oracle.connect(threaded=True ,*args, **kwargs)
            self.connection =  cx_Oracle.connect(settings.DATABASE_USER_CX
                                                 , settings.DATABASE_PASSWORD
                                                 , '//%s:%s/%s' % (settings.DATABASE_HOST
                                                 , settings.DATABASE_PORT
                                                 , settings.DATABASE_NAME)
                                                 , threaded=True
                                                 )
   
        except cx_Oracle.DatabaseError, e:
            #if ORA-12170 in e: #timeout
            log.critical("error while connecting to oracle: %s" % e)
            #re-raise
            raise cx_Oracle.DatabaseError(e)
    
    def release(self):
        log.debug('release %s' % id(self))
        #self.commit()
        self.locked = False
        self.last_used = time.time()

    def internal_cursor(self, *args, **kwargs):
        return self.connection.cursor()

    def cursor(self, *args, **kwargs):
        cw = CursorWrapper(self.connection, *args, **kwargs)
        self.getDaemon().connect(cw)
        return cw.getAttrProxy()
    
    def commit(self):
        self.connection.commit()
        log.debug("commit called")

    def rollback(self):
        self.connection.rollback()
        log.debug("rollback called")

    def close(self):
        # We have a pool, so we are just releasing (unlocking) the connection so it can be used again. 
        self.release()
        log.debug('close called')

class StoredProcedures(Pyro.core.ObjBase):
    def __init__(self):
        Pyro.core.ObjBase.__init__(self)

    def call(self, package, name, *args, **kwargs):
        klass = globals()[package]
        function = klass.__dict__[name]
        #instance = globals()[package](internal_connect)
        instance = globals()[package]()

        #get a free connection
        connection = internal_connect()

        try:
            result = function(instance, connection, *args, **kwargs)
        except Exception, e:
            import sys, traceback
            for line in traceback.format_exception(sys.exc_type, sys.exc_value, sys.exc_traceback):
                log.exception(line)
            raise e
        finally:
            connection.release()

        return result
        
    def status(self):  
        lock.acquire()
        try:
            global connections
            status = '%d connections in pool' % len(connections)
            for c in connections:
                status += '\n %s: %s (locked: %s)' % (c.id, time.asctime(time.localtime(c.last_used)), c.locked)
        finally:
            lock.release()

        return status


#used for providing remote connections, used in django backend
class OraclePool(Pyro.core.ObjBase):
    def __init__(self):
        Pyro.core.ObjBase.__init__(self)

    def connect(self):
        return internal_connect(return_proxy=True)

    def drop(self, connection):
        lock.acquire()
        try:
            global connections
            for c in connections:
                if c.id == connection.id:
                    log.debug('Dropping connection %s from pool' % str(id(c)))
                    try:
                        c.connection.close()
                    except Exception, e:
                        log.debug('Got exception while closing connection: %s' % str(e))
                    self.getDaemon().disconnect(c)
                    connections.remove(c)
                    break
        finally:
            lock.release()

    def status(self):
        global connections
        status = '%d connections in pool' % len(connections)
        return status

#really connect to oracle and pool the connection
def internal_connect(return_proxy=False, force_connect=False):
    '''
        internal_connect
            connect to oracle or give a free connection

        accepts:
            force_connect - connect even if there are free connections

        returns:
            connection wrapper object as a proxy or not
    '''
   
    lock.acquire() #thread locking
    try:
        global connections
        chosen_connection = None
        connections_count = 0

        if connections:
            connections_count = len(connections)

        if not force_connect:
            log.debug(('**Starting to look for alive, non-locked oracle connection within %s connections...')  % str(connections_count))

            #sort connection list so we always use the oldest connections first
            connections.sort(lambda x, y: cmp(x.last_used,y.last_used))

            #look for free connection
            i = 0
            for c in connections:
                i = i + 1
                log.debug(('-- Connection loop %s ') % str(i))
                if c.locked == False:
                    log.debug('---- Connection NOT locked')
                    chosen_connection = c
                    cursor = chosen_connection.internal_cursor()
                    try:
                        log.debug(('Before testing an existing connection %s with simple select.') % str(id(chosen_connection)))
                        log.debug('list of connections follows')
                        for c in connections:
                            log.debug('connection: %s, locked: %s, time: %s' % (id(c),c.locked, c.last_used))
                        
                        #time.sleep(2)
                        
                        cursor.execute('select * from DUAL')
                        # Exception while testing existing connection: ORA-24909: call in progress
                        # However - ORA-03114: not connected to ORACLE is caught well.
                        log.debug(('After testing an existing connection %s with simple select.') % str(id(chosen_connection)))
                        ok = True
                    except Exception, e:
                        log.warning('Exception while testing existing connection: %s. I will set the connection to None so it will not be returned' % e)
                        ok = False
                        chosen_connection = None
                        c.locked = True #so it won't be used again and then times out
                    if ok == True:
                        log.debug(('I tested an existing connection %s and it is OK.') % str(id(chosen_connection)))
                        break
                else:
                    log.debug('---- Connection LOCKED')
                    

        #return proxy if a connection that has been created before is wanted as a proxy now
        if return_proxy and chosen_connection:
            chosen_connection.getDaemon().connect(chosen_connection)

        #no free connection, create new connection
        if not chosen_connection:
            # Note: ConnectionWrapper is getting settings.
            # works if put here but need to check with windows etc
            chosen_connection = ConnectionWrapper()
     
            log.debug(('***Created a new connection: %s ') % str(id(chosen_connection)))
            if return_proxy:
                chosen_connection.getDaemon().connect(chosen_connection)
                
            connections.append(chosen_connection)
        else:
            log.debug(('I am returning the good existing connection: %s ') % str(id(chosen_connection)))
            
        if not force_connect:
            chosen_connection.locked = True
        chosen_connection.last_used = time.time()
        
        if return_proxy: 
            log.debug(('Before return proxy'))
            return chosen_connection.getAttrProxy()
        else:
            log.debug(('Before return'))
            return chosen_connection
    finally:
        lock.release()

#connection handling callback
def handle_connections(ins):
    lock.acquire()
    try:
        global connections
        #global daemon
        
        free_connections = 0

        #iterate over copy of connections
        for c in connections[:]:
            #remove connections if timeout has been reached
            if time.time() - c.last_used > timeout_locked:
                log.debug('Disconnecting connection %s because of timeout' % str(id(c)))
                try:
                    c.connection.close()
                except Exception, e:
                    log.info('Caught exception on closing connection: %s' % str(e))
                #daemon.disconnect(c)
                log.debug('Removing locked connection object %s from pool' % str(id(c)))
                connections.remove(c)

            elif time.time() - c.last_used > timeout:
                # just trying explicit
                if c.locked == False:
                #if not c.locked:
                    log.debug('Disconnecting connection %s because of timeout' % str(id(c)))
                    try:
                        c.connection.close()
                    except cx_Oracle.DatabaseError, e:
                        log.warning('Error while closing connection: %s' % e)
                    #daemon.disconnect(c)
                    log.debug('Removing connection object %s from pool' % str(id(c)))
                    connections.remove(c)

            #see if there are free connections
            if not c.locked:
                free_connections+=1

        if free_connections < settings.PYRO_POOL_MIN_SPARE_CONNECTIONS:
            #add as many new connections
            try:
                for i in range(0, settings.PYRO_POOL_MIN_SPARE_CONNECTIONS - free_connections):
                    internal_connect(force_connect=True)

                log.debug('created new spare connection(s), list of connections follows')
            except Exception, e:
                log.error('failed to create spare connections: %s' % e)

            for c in connections:
                log.debug('connection: %s, locked: %s, time: %s' % (id(c),c.locked, c.last_used))
    finally:
        lock.release()
        
Pyro.config.PYRO_PRINT_REMOTE_TRACEBACK = True
Pyro.config.PYRO_DETAILED_TRACEBACK = True

log.debug('Before Pyro.core.initServer')

Pyro.core.initServer()
daemon=Pyro.core.Daemon()
daemon.connect(OraclePool(),"OraclePool")
uri=daemon.connect(StoredProcedures(), "StoredProcedures")

log.info('The daemon runs on port:%s' % daemon.port)
log.info('The object\'s uri is:%s' % uri)

def onSignal(signum, stackframe):                 # handler for kill signals 
    print 'Killed with signal %d. Shutting down.' % signum
    global connections
    for c in connections:
        c.close()
    daemon.shutdown()

if os.name != 'nt':
    signal.signal(1, onSignal) 
    signal.signal(2, onSignal) 
    signal.signal(3, onSignal) 
    signal.signal(15, onSignal) 

try:
    #custom requestLoop if we want to debug 
    '''
    import select
   
    while True:
        socks=daemon.getServerSockets()
        ins,outs,exs=select.select(socks,[],[],2)   # 'foreign' event loop
        for s in socks:
                if s in ins:
                        daemon.handleRequests()
                        handle_connections(None)
                        break    # no need to continue with the for loop
    '''             
    
    daemon.requestLoop(callback=handle_connections)
except KeyboardInterrupt: 
    daemon.shutdown()

