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

##  From man open(2)
##       O_EXCL  When used with O_CREAT, if the file already exists
##               it is an error and the open will fail.  O_EXCL  is
##               broken on NFS file systems, programs which rely on
##               it for performing locking  tasks  will  contain  a
##               race   condition.   The  solution  for  performing
##               atomic file locking using a lockfile is to  create
##               a  unique file on the same fs (e.g., incorporating
##               hostname and pid), use link(2) to make a  link  to
##               the  lockfile.  If  link()  returns 0, the lock is
##               successful.  Otherwise, use stat(2) on the  unique
##               file  to  check if its link count has increased to
##               2, in which case the lock is also successful.


def _open2(pathname,mode=0666):

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
            s = os.stat(tmpname)
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
        delete_at_exit.unregister(pathname)
        return -1
            
open = _open2

