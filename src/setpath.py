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


def set_enstore_paths():
    #These two paths are necessary for enstore.
    addpath('$ENSTORE_DIR/src')
    addpath('$ENSTORE_DIR/modules')

    #In case we are working in a different location...
    try:
        cdir = os.getcwd()
        addpath(os.path.join(cdir, '../modules'))
    except OSError, msg:
        if msg.errno == errno.ENOENT:
            sys.stderr.write("%s: %s\n" % (os.strerror(msg.errno),
                                         "No current working directory"))
            sys.exit(1)
        else:
            sys.stderr.write(str(msg) + "\n")
            sys.exit(1)

### Why should this be called when the module is imported?  Nothing seems
###  to break when it is commented out (perhaps a code versus cut
###  difference exists).  If they did need it, they could call this
###  function.
set_enstore_paths()

if __name__=="__main__":   # pragma: no cover
    print sys.path


    
