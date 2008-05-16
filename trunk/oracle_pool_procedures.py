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

import os
os.environ['DJANGO_SETTINGS_MODULE']='settings'

import logging as log

import datetime

#uuid is used for debug messages
try:
    import uuid
except ImportError:
    import util.uuid as uuid

"""

Note!! If you get an error like below:
 look into your error log and see the real traceback

Traceback (most recent call last): 
...

File "/usr/lib/python2.4/site-packages/Pyro/core.py", line 444, in __invokePYRO__ constants.RIF_VarargsAndKeywords, vargs, kargs) 
File "/usr/lib/python2.4/site-packages/Pyro/protocol.py", line 373, in remoteInvocation answer = pickle.loads(answer) UnpicklingError: NEWOBJ class argument has NULL tp_new

"""

import Pyro.core

import os
os.environ["NLS_LANG"] = ".UTF8"
import cx_Oracle


def create_dict_from_cursor(cursor):
    rows = cursor.fetchall()
    desc = [item[0] for item in cursor.description]
    return [dict(zip(desc, item)) for item in rows]

def create_dict_from_cursor_or_none(cursor):
        try:
            return_rec = create_dict_from_cursor(cursor)
        except:
            return_rec = None 
        return return_rec


# template class for stored procedure/function wrappers
# to be called via pyro from the client: StoredProcedures.call(package, function)

# TODO: generalize this
class package:
    def __init__(self, connect):
        self.cx_connect = connect

    def fn_stored_function(self, parameter):
        cx_connection = self.cx_connect()

        cursor = cx_connection.internal_cursor()
        cx_parameter.setvalue(0, parameter.encode('UTF-8'))
        retVal = cursor.callfunc("package.stored_function", cx_Oracle.NUMBER, [cx_parameter])
        cursor.close()

        #important: release connection so it can be used again
        cx_connection.release()

        return retVal
    
    def pr_stored_procedure(self, parameter_in=None):
        cx_connection = self.cx_connect()

        cursor = cx_connection.internal_cursor()
        ref_cursor = cx_connection.internal_cursor()
        retVal = cursor.callproc("package.stored_procedure", [parameter_in, ref_cursor])
        data = ref_cursor.fetchall()
        cursor.close()

        ref_cursor.close()
        cx_connection.release()

        return data
    
