#!/usr/bin/env python
#
# $Id$
#

import os
import sys
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

    #Create a unique temporary filename.
    tmpname = "%s_%s_%s_%s_%s_%s_%s_lock" % (
        os.uname()[1], os.getpid(), os.getuid(), os.getgid(),
        os.geteuid(), os.getegid(), time.ctime(time.time()).replace(" ", "_"))
    tmpname = os.path.join(os.path.dirname(pathname), tmpname)

    #Record encp to delete this temporary file on (failed) exit.
    delete_at_exit.register(tmpname)

    #Create and open the temporary file.
    try:
        fd_tmp = os.open(tmpname, os.O_CREAT|os.O_EXCL|os.O_RDWR, mode)
    except OSError:
        exc, msg, tb = sys.exc_info()
        #If the newly created file exists, try opening it without the
        # exclusive create.  This is probably a symptom of the O_EXCL
        # race condition mentioned above.  Since, this is a unique filename
        # two encps can not be attempting to create the temporary file
        # simultaniously.  Thus, this error should be ignored; though any
        # errors from this os.open() are real.
        if hasattr(msg, "errno") and msg.errno == errno.EEXIST:
            fd_tmp = os.open(tmpname, os.O_RDWR)
        else:
            raise exc, msg, tb

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
        fd=os.open(pathname, os.O_RDWR, mode)
        os.unlink(tmpname)
        os.close(fd_tmp)
        delete_at_exit.unregister(tmpname)
        return fd
    else:
        delete_at_exit.unregister(pathname)
        if os.path.basename(pathname) in os.listdir(os.path.dirname(pathname)):
            #Check if the filesystem is corrupted.  If there is a file
            # listed in a directory that does not point to a valid inode the
            # directory is corrupted.  When the user tries to write a file
            # with the same name as the corrupted file the link operation
            # will (now) fail.  To test for this case get the full directory
            # listing and check to see if it is there.  If so, corrupted
            # directory.  If not, some other error occured.
            rtn_errno = getattr(errno, "EFSCORRUPTED", getattr(errno, "EIO"))
            msg = os.strerror(rtn_errno) + ": " + "Filesystem is corrupt."
        elif s and s[stat.ST_NLINK] > 2:
            #If there happen to be more than 2 hard links to the same file.
            # This should never happen.
            rtn_errno = getattr(errno, "EMLINK", getattr(errno, "EIO"))
            msg = os.strerror(rtn_errno) + ": " + str(s[stat.ST_NLINK])
        elif s:
            #If there is only one link to the file.  In this case the link
            # failed.  The use of "ENOLINK" is for Linux, IRIX and SunOS.
            # The "EFTYPE" is for OSF1.
            rtn_errno = getattr(errno, "ENOLINK", getattr(errno, "EFTYPE"))
            msg = os.strerror(rtn_errno) + ": " + str(s[stat.ST_NLINK])

        else:
            #If we get here, then something really bad happened.
            rtn_errno = getattr(errno, "ENOLINK", getattr(errno, "EFTYPE"))
            msg = os.strerror(rtn_errno) + ": " + "Unknown"

        os.close(fd_tmp)
        #return -(detail.errno) #return errno values as negative.
        raise OSError(rtn_errno, msg)
            
open = _open2

