#!/usr/bin/env python

# $Id$
    
import sys
import os
import errno

def addpath(p):
    if not p:
        return
    p = os.path.expandvars(p)
    p = os.path.normpath(p)
    if p not in sys.path:
        sys.path.append(p)


addpath('$ENSTORE_DIR/src')
addpath('$ENSTORE_DIR/modules')

#
try:
    cdir = os.getcwd()
except OSError, msg:
    if msg.errno == errno.ENOENT:
        sys.stderr.write("%s: %s\n" % (os.strerror(msg.errno),
                                     "No current working directory"))
        sys.exit(1)
    else:
        sys.stderr.write(str(msg) + "\n")
        sys.exit(1)
        
addpath(os.path.join(cdir, '../modules'))


if __name__=='__main__':
    print sys.path


    
