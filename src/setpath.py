#!/usr/bin/env python

# $Id$
    
import sys
import os

def addpath(p):
    if not p:
        return
    p = os.path.expandvars(p)
    p = os.path.normpath(p)
    if p not in sys.path:
        sys.path.insert(0,p)


addpath(os.path.join(edir, '$ENSTORE_DIR/src'))
addpath(os.path.join(edir, '$ENSTORE_DIR/modules'))

cdir = os.getcwd()
addpath(os.path.join(cdir, '../modules'))


if __name__=='__main__':
    print sys.path


    
