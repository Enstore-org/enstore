#!/usr/bin/env python
#
# $Id$
#

import os
import stat
import errno
import delete_at_exit

def _open1(pathname,mode=0666):
    delete_at_exit.register(pathname)
    fd = os.open(pathname, os.O_CREAT|os.O_EXCL|os.O_RDWR, mode)
    return fd
        
def _open2(pathname,mode=0666): #see "man open(2)"

    tmpname = "%s-%s-%s" % (pathname, os.uname()[1], os.getpid())
    delete_at_exit.register(tmpname)
    fd = os.open(tmpname, os.O_CREAT|os.O_RDWR, mode)

    ok = 0
    delete_at_exit.register(pathname)
    try:
        os.link(tmpname, pathname)
        ok = 1
    except:
        try:
            s = os.stat(pathname)
            if s and s[stat.ST_NLINK]==2:
                ok = 1
        except:
            ok = 0

    if ok:
        fd=os.open(pathname, os.O_CREAT|os.O_RDWR, mode)
        os.unlink(tmpname)
        delete_at_exit.unregister(tmpname)
        return fd
    else:
        return -1
            
open = _open2

