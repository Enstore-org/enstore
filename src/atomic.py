#!/usr/bin/env python
#
# $Id$
#

import os
import stat
import errno
import delete_at_exit
import exceptions
import time

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

    #tmpname = "%s-%s-%s" % (pathname, os.uname()[1], os.getpid())
    tmpname = "%s_%s_%s_%s_%s_%s_%s_lock" % (
        os.uname()[1], os.getpid(), os.getuid(), os.getgid(),
        os.geteuid(), os.getegid(), time.ctime(time.time()).replace(" ", "_"))
    tmpname = os.path.join(os.path.dirname(pathname), tmpname)
    
    delete_at_exit.register(tmpname)
    fd_tmp = os.open(tmpname, os.O_CREAT|os.O_RDWR, mode)

    ok = 0
    s = None #initalize
    delete_at_exit.register(pathname)
    try:
        os.link(tmpname, pathname)
        ok = 1
    except OSError, detail:
        try:
            #There are timeout issues with pnfs... keep trying.
            for i in range(5):
                s = os.stat(tmpname)
                if s and s[stat.ST_NLINK]==2:
                    ok = 1
                    break
                time.sleep(1)
        except OSError:
            #ok = 0
            os.close(fd_tmp)
            delete_at_exit.unregister(pathname)
            raise OSError, detail

    if ok:
        fd=os.open(pathname, os.O_CREAT|os.O_RDWR, mode)
        os.unlink(tmpname)
        os.close(fd_tmp)
        delete_at_exit.unregister(tmpname)
        return fd
    else:
        delete_at_exit.unregister(pathname)
        if s and s[stat.ST_NLINK] > 2:
            #If there happen to be more than 2 hard links to the same file.
            # This should never happen.
            rtn_errno = getattr(errno, "EMLINK", "EIO")
            msg = os.strerror(rtn_errno) + ": " + str(s[stat.ST_NLINK])
        elif s:
            #If there is only one link to the file.  In this case the link
            # failed.  The use of "ENOLINK" is for Linux, IRIX and SunOS.
            # The "EFTYPE" is for OSF1.
            rtn_errno = getattr(errno, "ENOLINK", "EFTYPE")
            msg = os.strerror(rtn_errno) + ": " + str(s[stat.ST_NLINK])

        else:
            #If we get here, then something really bad happened.
            rtn_errno = getattr(errno, "ENOLINK", "EFTYPE")
            msg = os.strerror(rtn_errno) + ": " + "Unknown"

        os.close(fd_tmp)
        #return -(detail.errno) #return errno values as negative.
        raise OSError(rtn_errno, msg)
            
open = _open2

