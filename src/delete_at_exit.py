#!/usr/bin/env python
#
# $Id$
#

import os
import sys

_deletion_list = []

def register(filename):
    if filename == '/dev/null':
        return
    if filename not in _deletion_list:
        _deletion_list.append(filename)

def unregister(filename):
    if filename == '/dev/null':
        return
    if filename in _deletion_list:
        _deletion_list.remove(filename)

def delete():
    for f in _deletion_list:
        if os.path.exists(f):
            try:
                os.unlink(f)
            except:
                sys.stderr.write("Can't delete %s\n" %(f,))

            
