#!/usr/bin/env python
#
# $Id$
#

import os
import sys

import configuration_client
import file_clerk_client
import enstore_functions2

_deletion_list = []
_deletion_list_bfids = []

def register(filename):
    if filename == '/dev/null':
        return
    if filename not in _deletion_list:
        _deletion_list.append(filename)

def register_bfid(bfid):
    if bfid not in _deletion_list_bfids:
        _deletion_list_bfids.append(bfid)

def unregister(filename):
    if filename == '/dev/null':
        return
    if filename in _deletion_list:
        _deletion_list.remove(filename)

def unregister_bfid(bfid):
    if bfid in _deletion_list_bfids:
        _deletion_list_bfids.remove(bfid)

def delete():
    for f in _deletion_list:
        if os.path.exists(f):
            try:
                os.unlink(f)
            except:
                sys.stderr.write("Can't delete %s\n" %(f,))

    # get a configuration server and file clerk
    config_host = enstore_functions2.default_host()
    config_port = enstore_functions2.default_port()
    csc = configuration_client.ConfigurationClient((config_host,config_port))

    for b in _deletion_list_bfids:
        try:
            fcc = file_clerk_client.FileClient(csc, b)
            fcc.set_deleted("yes")
        except:
            sys.stderr.write("Can't delete %s from database\n" %(b,))
            
