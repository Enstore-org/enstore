#!/usr/bin/env python
#
# $Id$
#

from future.utils import raise_
import os
import sys
import stat
import errno
import time
import Trace


def _open1(pathname, flags, mode=0o666):
    fd = os.open(pathname, flags, mode)
    return fd

# From man open(2)
# O_EXCL  When used with O_CREAT, if the file already exists
# it is an error and the open will fail.  O_EXCL  is
# broken on NFS file systems, programs which rely on
# it for performing locking  tasks  will  contain  a
# race   condition.   The  solution  for  performing
# atomic file locking using a lockfile is to  create
# a  unique file on the same fs (e.g., incorporating
# hostname and pid), use link(2) to make a  link  to
# the  lockfile.  If  link()  returns 0, the lock is
# successful.  Otherwise, use stat(2) on the  unique
# file  to  check if its link count has increased to
# 2, in which case the lock is also successful.


def unique_id():
    t = time.time()
    dp = ("%10.9f" % (t - int(t),)).split('.')[1]
    a = time.ctime(t).split(" ")
    b = "."
    c = b.join((a[4], dp))
    a[4] = c
    b = " "
    tm = b.join(a)
    return tm.replace(" ", "_")


def _open2(pathname, flags, mode=0o666):
    __pychecker__ = "unusednames=i"

    # Create a unique temporary filename.
    tmpname = "%s_%s_%s_%s_%s_%s_%s_lock" % (
        os.uname()[1], os.getpid(), os.getuid(), os.getgid(),
        os.geteuid(), os.getegid(), unique_id())
    tmpname = os.path.join(os.path.dirname(pathname), tmpname)

    # Create and open the temporary file.
    try:
        Trace.trace(5, "atomic.open 0 %s" % (tmpname,))
        fd_tmp = os.open(tmpname, os.O_CREAT | os.O_EXCL | os.O_RDWR, mode)
    except OSError as msg:
        Trace.trace(5, "atomic.open 1 %s" % (msg,))
        # If the newly created file exists, try opening it without the
        # exclusive create.  This is probably a symptom of the O_EXCL
        # race condition mentioned above.  Since, this is a unique filename
        # two encps can not be attempting to create the temporary file
        # simultaniously.  Thus, this error should be ignored; though any
        # errors from this os.open() are real.
        if getattr(msg, "errno", None) == errno.EEXIST:
            fd_tmp = os.open(tmpname, os.O_RDWR)
        else:
            raise_(OSError, msg)

    # Some default values.
    ok = False
    s = None  # initalize
    s2 = None  # initalize
    rtn_errno = 0
    message = os.strerror(rtn_errno)
    use_filename = pathname
    unlink_filename = None

    try:
        os.link(tmpname, pathname)
        ok = True
    except OSError as detail:
        Trace.trace(5, "atomic.open 2 %s" % (detail,))
        # If the output file already exists, we should be able to stop now.
        # However, EEXIST is given for four cases.
        # 1) The first is that the file does already exist.
        # 2) The second occurs from a race condition inherent to the NFS
        #    V2 protocol.  The link() call fails, but in fact does
        #    succeed to create the directory entry and increase the link
        #    count to two.
        # 3) There is a bug in pnfs that the directory entry is
        #    successfully created, but the link count fails to be
        #    increased from one to two.
        # 4) The target path exists in a directory listing, but a stat
        #    of the file fails with "No such file or directory".
        #    A.K.A. The 'pathname' file name is currently a ghost file.
        #
        # Unfortunately, this means that we need to enter the following
        # loop for all cases to determine which case it is.
        if getattr(detail, "errno", detail.args[0]) == errno.EEXIST:
            try:
                # There are timeout issues with pnfs... keep trying.
                for i in range(5):
                    # fstat() is faster than stat(), especially for large
                    # directories in PNFS.
                    s = os.fstat(fd_tmp)
                    if s and s[stat.ST_NLINK] == 2:
                        # We know it is case 2.
                        ok = True
                        break
                    if s and s[stat.ST_NLINK] > 2:
                        # If there happen to be more than 2 hard links to
                        # the same file.  This should never happen to the
                        # temporary file we just created.
                        rtn_errno = getattr(errno, str("EMLINK"),
                                            getattr(errno, str("EIO")))
                        message = "%s: %s" % (os.strerror(rtn_errno),
                                              str(s[stat.ST_NLINK]))
                        use_filename = tmpname
                        unlink_filename = tmpname
                        break

                    if not s2:
                        try:
                            s2 = os.stat(pathname)
                        except OSError:
                            s2 = None

                    if s and s2 and s[stat.ST_INO] != s2[stat.ST_INO]:
                        # We know it is case 1.
                        rtn_errno = errno.EEXIST
                        message = os.strerror(rtn_errno)
                        use_filename = pathname
                        unlink_filename = tmpname
                        break

                    time.sleep(1)

                else:
                    # If we get out of the loop, we know it must be either
                    # case 3 or case 4.

                    if s and s2 and s[stat.ST_INO] == s2[stat.ST_INO]:
                        # We know it is case 3.
                        rtn_errno = errno.EAGAIN
                        message = message = "%s: %s" % (os.strerror(rtn_errno),
                                                        "Filesystem is corrupt")
                        use_filename = pathname
                        unlink_filename = pathname

                        # This case is different.  Since there are two directory
                        # entries pointing to the file with a link count of 1,
                        # we need to delete the correct path.  This will leave
                        # the temporary lock file as a ghost file.

                    elif s and not s2:
                        # We know it is case 4.
                        rtn_errno = getattr(errno, str("EFSCORRUPTED"),
                                            errno.EIO)
                        message = "%s: %s" % (os.strerror(rtn_errno),
                                              "Filesystem is corrupt")
                        use_filename = pathname
                        unlink_filename = tmpname

                    else:
                        # Not sure what happend, but handle it.
                        rtn_errno = errno.EEXIST  # If clause guarantees this.
                        message = os.strerror(rtn_errno)
                        use_filename = pathname
                        unlink_filename = tmpname

            except OSError as detail2:
                Trace.trace(5, "atomic.open 3 %s" % (detail2,))
                os.close(fd_tmp)
                os.unlink(tmpname)
                raise sys.exc_info()
        # EBUSY is prevalent for Chimera when doing atomic.open().  This
        # was not observed for PNFS.
        elif getattr(detail, "errno", detail.args[0]) == errno.EBUSY:
            for i in range(5):
                try:
                    os.link(tmpname, pathname)
                    ok = True
                except OSError as detail2:
                    # Another EBUSY error, wait and try again.
                    if getattr(detail2, "errno",
                               detail2.args[0]) == errno.EBUSY:
                        time.sleep(1)
                    else:
                        # Some real error occured.
                        rtn_errno = getattr(detail2, "errno", detail2.args[0])
                        message = str(detail2)
                        use_filename = pathname
                        unlink_filename = tmpname
                        break
        else:
            # For all other errors.
            os.close(fd_tmp)
            os.unlink(tmpname)
            raise sys.exc_info()

    if ok:
        # Pull out only the information about how to open the file with respect
        # to reading or writing.
        second_chance_flags = flags & (os.O_WRONLY | os.O_RDONLY | os.O_RDWR)
        try:
            fd = os.open(pathname, second_chance_flags, mode)
            os.close(fd_tmp)
            os.unlink(tmpname)
            return fd
        except OSError as detail:
            Trace.trace(5, "atomic.open 4 %s" % (detail,))
            raise_(OSError, detail)
    else:
        os.close(fd_tmp)
        os.unlink(unlink_filename)
        # Remake the exception including the path name.  For some reason,
        # the exceptions raised by os.link() don't include a pathname.
        raise OSError(rtn_errno, message, use_filename)


# Since the point of this modules is to override the default open function,
# we want to suppress the "shadows builtin" warning message.
__pychecker__ = "no-shadowbuiltin"
open = _open2
