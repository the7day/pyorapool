#!/usr/bin/python

# vim: ai ts=4 sts=4 et sw=4

# Copyright (C) 2007-2008 Music Pictures Ltd 2007, 2008. 
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
import cx_Oracle
import time

try:
    import uuid
except:
    #python < 2.5
    from util.uuid import *

from oracle_pool_procedures import *
import logging as log

log.basicConfig(
    level=log.INFO,
    format='%(asctime)s %(levelname)-8s %(message)s',
    datefmt='%m-%d %H:%M',
    filename='/var/log/pyorapool.log', 
    filemode='a'
)

import os
os.environ['NLS_LANG'] = '.UTF8'

#import django stuff
os.environ['DJANGO_SETTINGS_MODULE']='settings'

from django.conf import settings
from django.utils.encoding import smart_str, force_unicode

# timeout for unused connections in seconds
timeout = settings.PYRO_POOL_TIMEOUT

# timeout for connections that are locked but might be dead
timeout_locked = settings.PYRO_POOL_LOCKED_TIMEOUT

#the pool (tm)
connections = []

#serverside_cursors = []

def to_unicode(s):
    """
    Convert strings to Unicode objects (and return all other data types
    unchanged).
    """
    if isinstance(s, basestring):
        return force_unicode(s)
    return s

class CursorWrapper(Pyro.core.ObjBase):
    '''
    wrapper classes for django backend (oracle_pyro)
    '''
    cursor = None
    id = str(uuid1)
    charset = 'utf-8'

    def __init__(self, connection, *args, **kwargs):
        Pyro.core.ObjBase.__init__(self)
        self.cursor = connection.cursor(*args, **kwargs)
        # This looks like a hard limit on number of cursors?

        #TODO: create getter/setter method for this,
        #so it can be set using the proxy
        self.cursor.arraysize = 100

    def description(self, *args, **kwargs):
        return self.cursor.description(*args, **kwargs)

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

        log.debug('execute query %s with params %s' % (str(query), str(params)))

        ## taken from django source / oracle backend
        args = [(':arg%d' % i) for i in range(len(params))]
        # cx_Oracle wants no trailing ';' for SQL statements.  For PL/SQL, it
        # it does want a trailing ';' but not a trailing '/'.  However, these
        # characters must be included in the original query in case the query
        # is being passed to SQL*Plus.
        if query.endswith(';') or query.endswith('/'):
            query = query[:-1]
        query = smart_str(query, self.charset) % tuple(args)
        #execute creates cx_Oracle.STRING/NUMBER/.. values, convert to python values
        result = ''
        try:
            result = self.cursor.execute(query, params)
            #TODO: should result be returned, what for(no data in it)?
        except Exception, e:
            log.warning("Exception: %s" % e)
        #return self.convert_cx_var(result)

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

    '''
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
    '''

class ConnectionWrapper(Pyro.core.ObjBase):
    '''
    wrapper that can be passed as an object/proxy over pyro
    '''
    connection = None
    locked = False
    last_used = time.time()
    id = time.time()

    def __init__(self, *args, **kwargs):
        Pyro.core.ObjBase.__init__(self)
        self.connection = cx_Oracle.connect(threaded=True ,*args, **kwargs)
    
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
        self.release()
        log.debug('close called')

class StoredProcedures(Pyro.core.ObjBase):
    def __init__(self):
        Pyro.core.ObjBase.__init__(self)

    def call(self, package, name, *args, **kwargs):
        klass = globals()[package]
        function = klass.__dict__[name]
        instance = globals()[package](internal_connect)

        try:
            return function(instance, *args, **kwargs)
        except Exception, e:
            import sys, traceback
            for line in traceback.format_exception(sys.exc_type, sys.exc_value, sys.exc_traceback):
                log.exception(line)
            raise Exception(e)

    def status(self):
        global connections
        return '%d connection(s) in pool' % len(connections)


class OraclePool(Pyro.core.ObjBase):
    '''
    provide remote connections, used in django backend
    '''
    def __init__(self):
        Pyro.core.ObjBase.__init__(self)

    def connect(self):
        return internal_connect(return_proxy=True)

    def drop(self, connection):
        global connections
        for c in connections:
            if c.id == connection.id:
                log.debug('Dropping connection %s from pool' % str(id(c)))
                try:
                    c.connection.close()
                except Exception, e:
                    log.debug('Got exception while closing connection: %s' % str(e))
                self.getDaemon().disconnect(conn)
                connections.remove(c)
                break

    def status(self):
        return '%d connections in pool' % len(connections)

def internal_connect(return_proxy=False):
    '''
    connect to oracle and pool the connection
    
    set return_proxy to True to create and return a proxy object
    for the connection
    '''
    global connections
    conn = None
    connections_count = 0
    
    if connections:
        connections_count = len(connections)
        
    log.debug(('Looking for alive, non-locked oracle connection within %s connections ...')  % connections_count)

    #sort connection list so we always us the oldest connections first
    connections.sort(lambda x, y: cmp(x.last_used,y.last_used))

    #look for free connection
    for c in connections:
        if c.locked == False:
            conn = c
            cursor = conn.internal_cursor()
            try:
                log.debug(('Testing existing connection %s with simple select.') % str(id(conn)))
                log.debug('list of current connections:')
                for c in connections:
                    log.debug('connection: %s, locked: %s, time: %s' % (id(c),c.locked, c.last_used))

                cursor.execute('select 1 from DUAL')
                # Exception while testing existing connection: ORA-24909: call in progress
                # However - ORA-03114: not connected to ORACLE is caught well.
                ok = True
            except Exception, e:
                log.warning('Exception while testing existing connection: %s. I will set the conn to None so it will not be returned' % e)
                ok = False
                conn = None
                c.locked = True #so it won't be used again and then times out
            if ok:
                log.debug(('tested existing connection %s and it is OK.') % str(id(conn)))
                break

    #no free conn, create new connection
    if not conn:    
        conn = ConnectionWrapper(settings.DATABASE_USER_CX, settings.DATABASE_PASSWORD,'//%s/%s' % (settings.DATABASE_HOST, settings.DATABASE_NAME))
        log.debug(('no free connection found, created a new one: %s ') % str(id(conn)))
        if return_proxy:
            self.getDaemon().connect(conn)
        
        conn.locked = True
            
        connections.append(conn)
    else:
        log.debug(('returning existing connection: %s ') % str(id(conn)))
        
    conn.locked = True
    conn.last_used = time.time()
    
    if return_proxy: 
        return conn.getAttrProxy()
    else:
        return conn

def handle_connections(ins):
    '''
    callback that does the pooling and timeout logic
    '''
    global connections
    #global daemon

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
            if not c.locked:
                log.debug('Disconnecting connection %s because of timeout' % str(id(c)))
                try:
                    c.connection.close()
                except DatabaseError, e:
                    log.warning('Error while closing connection: %s' % e)
                #daemon.disconnect(c)
                log.debug('Removing connection object %s from pool' % str(id(c)))
                connections.remove(c)
        
Pyro.config.PYRO_PRINT_REMOTE_TRACEBACK = True
Pyro.config.PYRO_DETAILED_TRACEBACK = True

Pyro.core.initServer()
daemon=Pyro.core.Daemon()
daemon.connect(OraclePool(),"OraclePool")
uri=daemon.connect(StoredProcedures(), "StoredProcedures")

log.info('pyorapool started on port:%s' % daemon.port)

daemon.requestLoop(callback=handle_connections)

