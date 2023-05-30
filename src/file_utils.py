#!/usr/bin/env python
###############################################################################
#
# $Author$
# $Date$
# $Id$
#
# Collection of utility functions taken from encp.py
# This module used in any Enstore utility that accesses files. It wraps os
# filesystem access functions to provide retry and impersonation functionality
# around filesystem access functions.
#
# Author: Dmitry Litvintsev (litvinse@fnal.gov) 09/05
#
###############################################################################

# system imports
import sys
import os
import stat
import threading
import types
import errno
import time
import traceback

try:
    import multiprocessing
except ImportError, msg:
    # sys.stderr.write("Failed to import multiprocessing: %s\n" % (str(msg),))
    pass
try:
    # To fully enable this, sem_lock must also be changed later in this file.
    pass  # import posix_ipc
except ImportError, msg:
    # sys.stderr.write("Failed to import posix_ipc: %s\n" % (str(msg),))
    pass

# enstore imports
import Trace
import atomic
import e_errors
import re


## mode is one of os.F_OK, os.W_OK, os.R_OK or os.X_OK.
## file_stats is the return from os.stat()

# The os.access() and the access(2) C library routine use the real id when
# testing for access.  This function does the same thing but for the
# effective ID.
def e_access(path, mode, unstable_filesystem=False):
    """
    Determine if the supplied access mode is allowed for the given path.

    Args:
        path (str): filepath
        mode (bits): access mode
        unstable_filesystem (bool): Whether the filesystem is unstable (e.g. PNFS)
            See comment on unstable filesystems. Default: False

    Returns:
        (int): 1 access is allowed, else 0
    """
    # Test for existence.
    try:
        file_stats = get_stat(path, unstable_filesystem=unstable_filesystem)
    except OSError:
        return 0

    return e_access_cmp(file_stats, mode)


# Check the bits to see if we have the requested mode access.
def e_access_cmp(file_stats, mode):
    """
    Determine if the supplied access mode is present in supplied file stats.

    Args:
        file_stats (bits): file state per the OS
        mode (bits): access mode

    Returns:
        (int): 1 access mode is present, else 0
    """
    stat_mode = file_stats[stat.ST_MODE]

    # Make sure a valid mode was passed in.
    if mode & (os.F_OK | os.R_OK | os.W_OK | os.X_OK) != mode:
        return 0

    # Need to check for each type of access permission.
    if mode == os.F_OK:
        # In order to get this far, the file must exist.
        return 1

    if mode & os.R_OK:  # Check for read permissions.
        # If the user is user root.
        if os.geteuid() == 0:
            pass
        # Anyone can read this file.
        elif stat_mode & stat.S_IROTH:
            pass
        # This is the files' owner.
        elif (stat_mode & stat.S_IRUSR) and \
                file_stats[stat.ST_UID] == os.geteuid():
            pass
        # The user has group access.
        elif (stat_mode & stat.S_IRGRP) and \
                (file_stats[stat.ST_GID] == os.geteuid() or
                 file_stats[stat.ST_GID] in os.getgroups()):
            pass
        else:
            return 0

    if mode & os.W_OK:  # Check for write permissions.
        # If the user is user root.
        if os.geteuid() == 0:
            pass
        # Anyone can write this file.
        elif stat_mode & stat.S_IWOTH:
            pass
        # This is the files' owner.
        elif (stat_mode & stat.S_IWUSR) and \
                file_stats[stat.ST_UID] == os.geteuid():
            pass
        # The user has group access.
        elif (stat_mode & stat.S_IWGRP) and \
                (file_stats[stat.ST_GID] == os.geteuid() or
                 file_stats[stat.ST_GID] in os.getgroups()):
            pass
        else:
            return 0

    if mode & os.X_OK:  # Check for execute permissions.
        # If the user is user root.
        if os.geteuid() == 0:
            pass
        # Anyone can execute this file.
        elif stat_mode & stat.S_IXOTH:
            pass
        # This is the files' owner.
        elif (stat_mode & stat.S_IXUSR) and \
                file_stats[stat.ST_UID] == os.geteuid():
            pass
        # The user has group access.
        elif (stat_mode & stat.S_IXGRP) and \
                (file_stats[stat.ST_GID] == os.geteuid() or
                 file_stats[stat.ST_GID] in os.getgroups()):
            pass
        else:
            return 0

    return 1


#############################################################################

# Get the mount point of the path.
def get_mount_point(path):
    """
    Get the mount point of supplied path by finding the directory at which
    the device changes.

    Args:
        path (str): Full path to search for mount point

    Returns:
        str or None: Mount point of device containing directory, if found
    """
    # Strip off one directory segment at a time.  We are looking for
    # where pnfs stops.
    current_path = path
    old_path = current_path
    old_stat = None
    current_path = os.path.dirname(current_path)
    while current_path:
        fstat = wrapper(get_stat, (current_path,))
        if old_stat and fstat[stat.ST_DEV] != old_stat[stat.ST_DEV]:
            # We found the change in device IDs.
            return old_path
        if current_path == "/":
            # We found the root path.  Keep from looping indefinitely.
            return current_path

        old_path = current_path
        old_stat = fstat
        current_path = os.path.dirname(current_path)

    return None


#############################################################################

## This section of code contains wrapper functions around os module functions
## in a thread safe manner with respect to seteuid().

# arg can be: filename, file descriptor, file object, a stat object
def get_stat(arg, use_lstat=False, unstable_filesystem=False):
    """
    Wrap os.fstat for various arg types

    Args:
        arg (str, fd, File, bits): File to get stats of, or stats object
        use_lstat (bool): Whether to use lstat
        unstable_filesystem (bool): Whether the filesystem is unstable (e.g. PNFS)
            See comment on unstable filesystems. Default: False

    Returns:
        bits: Stats object of supplied file
    """
    if isinstance(arg, types.StringType):
        if use_lstat:
            f_stat = wrapper(os.lstat, (arg,),
                             unstable_filesystem)
        else:
            f_stat = wrapper(os.stat, (arg,),
                             unstable_filesystem)
    elif isinstance(arg, types.IntType):
        f_stat = wrapper(os.fstat, (arg,))
    elif isinstance(arg, types.FileType):
        f_stat = wrapper(os.fstat, (arg.fileno(),))
    elif isinstance(arg, types.TupleType) or isinstance(arg, os.stat_result):
        f_stat = arg
    else:
        raise TypeError("Expected path, file descriptor or file object; "
                        "not %s" % (type(arg),))

    return f_stat


# Because open() is a builtin, pychecker gives a "(open) shadows builtin"
# warning.  This suppresses that warning, but it will do so for all functions
# in this module.
__pychecker__ = "no-shadowbuiltin"


# Open the file fname.  Mode has same meaning as builtin open().
def open(fname, mode="r", unstable_filesystem=False):
    """
    Wrap builtin 'open' function

    Args:
        fname (str): File name to open
        mode (str): Mode to use when opening file. Default: "r"
        unstable_filesystem (bool): Whether the filesystem is unstable (e.g. PNFS)
            See comment on unstable filesystems. Default: False

    Returns:
        (File): Opened file handle
    """
    file_p = wrapper(__builtins__['open'], (fname, mode,),
                     unstable_filesystem=unstable_filesystem)
    return file_p


# Open the file fname.  This is a wrapper for os.open() (atomic.open() is
# another level of wrapper for os.open()).
def open_fd(fname, flags, mode=0777, unstable_filesystem=False):
    """
    Wrap os.open, accepting flags

    Args:
        fname (str): File name to open
        flags (bits): Flags to use when opening file
        mode (bits): Mode to use when opening file
        unstable_filesystem (bool): Whether the filesystem is unstable (e.g. PNFS)
            See comment on unstable filesystems. Default: False

    Returns:
        (File): Opened file handle
    """
    if flags & os.O_CREAT:
        file_fd = wrapper(atomic.open, (fname, flags, mode,),
                          unstable_filesystem=unstable_filesystem)
    else:
        file_fd = wrapper(os.open, (fname, flags, mode,),
                          unstable_filesystem=unstable_filesystem)
    return file_fd


# Obtain the contents of the specified directory.
def listdir(dname, unstable_filesystem=False):
    """
    Wrap os.listdir

    Args:
        dname: Directory to list contents of
        unstable_filesystem (bool): Whether the filesystem is unstable (e.g. PNFS)
            See comment on unstable filesystems. Default: False

    Returns:
        List[str]: List of files in directory
    """
    directory_listing = wrapper(os.listdir, (dname,),
                                unstable_filesystem=unstable_filesystem)
    return directory_listing


# Change the permissions of file fname.  Perms have same meaning as os.chmod().
def chmod(fname, perms, unstable_filesystem=False):
    """
    Wrap os.chmod, accepting permissions

    Args:
        fname (str): File to change permissions of
        perms (bits): Permissions to set on file
        unstable_filesystem (bool): Whether the filesystem is unstable (e.g. PNFS)
            See comment on unstable filesystems. Default: False

    Returns:
        None
    """
    dummy = wrapper(os.chmod, (fname, perms),
                    unstable_filesystem=unstable_filesystem)
    return dummy


# Change the owner of file fname.  Perms have same meaning as os.chmod().
def chown(fname, uid, gid, unstable_filesystem=False):
    """
    Wrap os.chown, accepting uid and gid


    Args:
        fname (str): File to change ownership of
        uid (int): User ID to set for owning user
        gid (int): Group ID to set for file group
        unstable_filesystem (bool): Whether the filesystem is unstable (e.g. PNFS)
            See comment on unstable filesystems. Default: False

    Returns:
        None
    """
    dummy = wrapper(os.chown, (fname, uid, gid),
                    unstable_filesystem=unstable_filesystem)
    return dummy


# Update the times of file fname.  Access time and modification time are
# the same as os.chown().
def utime(fname, times, unstable_filesystem=False):
    """
    Wrap os.utime, accepting access and mod times to set on file

    Args:
        fname (str): File name of file to update
        times (Tuple(int)): (Last access time, mod time) to set on file
        unstable_filesystem (bool): Whether the filesystem is unstable (e.g. PNFS)
            See comment on unstable filesystems. Default: False

    Returns:
        None
    """
    dummy = wrapper(os.utime, (fname, times),
                    unstable_filesystem=unstable_filesystem)
    return dummy


# Remove the file fname from the filesystem.
def remove(fname, unstable_filesystem=False):
    """
    Wrap os.remove

    Args:
        fname (str): File name to remove from filesystem
        unstable_filesystem (bool): Whether the filesystem is unstable (e.g. PNFS)
            See comment on unstable filesystems. Default: False

    Returns:
        None
    """
    dummy = wrapper(os.remove, (fname,),
                    unstable_filesystem=unstable_filesystem)
    return dummy


#############################################################################

def readline(fp, unstable_filesystem=False):
    """
    Read line from supplied File object, supporting opened PNFS files

    Args:
        fp (File): File to read line from
        unstable_filesystem (bool): Whether the filesystem is unstable (e.g. PNFS)
            See comment on unstable filesystems. Default: False

    Returns:
        (str): File content
    """
    return __readline(fp, "readline", unstable_filesystem=unstable_filesystem)


def readlines(fp, unstable_filesystem=False):
    """
    Read lines from supplied File object, supporting opened PNFS files

    Args:
        fp (File): File to read line from
        unstable_filesystem (bool): Whether the filesystem is unstable (e.g. PNFS)
            See comment on unstable filesystems. Default: False

    Returns:
        (str): File content
    """
    return __readline(fp, "readlines", unstable_filesystem=unstable_filesystem)


#############################################################################

# If root is running the process, we may need to change the euid.  Is this
# only applicable to migration?
euid_lock = threading.RLock()


def acquire_lock_euid_egid():
    """
    Acquire lock for updating current euid/egid on this thread
    """
    if euid_lock._RLock__count > 0 and \
            euid_lock._RLock__owner == threading.currentThread():
        Trace.log(67, "lock count: %s" % (euid_lock._RLock__count,))
        Trace.log_stack_trace(severity=67)
    euid_lock.acquire()


def release_lock_euid_egid():
    """
    Release euid lock
    """
    try:
        euid_lock.release()
    except RuntimeError:
        pass  # Already unlocked.


# Match the effective uid/gid of a file.
# arg: could be a pathname, fileno or file object.
#
# We need to do this when user root, so that migration modify non-/pnfs/fs
# (and non-trusted) pnfs mount points.
def match_euid_egid(arg):
    """
    Set euid/egid of process to that of specified file

    Args:
        arg (str, int, File): Filename to check uid/gid of
    """
    if os.getuid() == 0 or os.getgid() == 0:

        f_stat = get_stat(arg)

        # Acquire the lock.
        acquire_lock_euid_egid()

        try:
            set_euid_egid(f_stat[stat.ST_UID], f_stat[stat.ST_GID])
        except:
            release_lock_euid_egid()
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]


# Set the effective uid/gid.
# euid - The new effective user ID.
# egid - The new effective group ID.
def set_euid_egid(euid, egid):
    """
    Set euid/egid of process to specified values to the OS
    Does not obtain locks

    Args:
        euid (int): User id to set via os.seteuid
        egid (int): User id to set via os.seteuid
    """
    if os.getuid() == 0:

        # We need to set these back to root, with uid first.  If the group
        # is changed we need to also set the euid, to that we will have
        # permissions to set the egid to another non-root user ID.
        if euid != 0 or egid != 0:
            os.seteuid(0)
        if egid != 0:
            os.setegid(0)

        # First look at the gid for setting them.
        if egid != os.getegid():
            os.setegid(egid)
        # Then look at the uid.
        if euid != os.geteuid():
            os.seteuid(euid)


# Release the lock.
def end_euid_egid(reset_ids_back=False):
    """
    Release euid/egid lock, resetting euid/egid to root if at least one is root
    and reset_ids_back is True

    Args:
        reset_ids_back (bool): Whether to reset IDs
    """
    if os.getuid() == 0 or os.getgid() == 0:
        if reset_ids_back:
            os.seteuid(0)
            os.setegid(0)

        release_lock_euid_egid()


#############################################################################

# this function does "rm -f path"
def rmdir(path):
    """
    Remove supplied directory path from file system

    Args:
        path(str): Directory path to remove (via rm -f)
    """
    if os.path.isdir(path):
        for direntry in os.listdir(os.path.abspath(path)):
            rmdir(os.path.join(path, direntry))
        try:
            os.rmdir(path)
        except OSError as e_msg:
            # ignore already deleted directories
            if e_msg.errno not in [errno.ENOENT]:
                raise OSError(e_msg)
    else:
        try:
            os.unlink(path)
        except OSError as e_msg:
            # ignore already deleted files
            if e_msg.errno not in [errno.ENOENT]:
                raise OSError(e_msg)


# Hard coded retry count for handling transient file system errors.
RETRY_COUNT = 4

# For PNFS, these errno values have been identified as possible
# incorrect answers.  If we wait a moment and try again, the
# issue sometimes goes away.
#
# Historically all systems using PNFS intermittently returned ENOENT
# falsely when a timeout occurred and the file really did exist.  This
# also, happens a lot if PNFS is auto-mounted. One node, flxi04,
# appears to be throwing EIO instead for these cases.
#
# Once, an open file gets ESTALE it should remain unusable until
# the filesystem is remounted.  However, PNFS is able to give this
# error and a few moments later everything is fine if asked again.
#
# EBUSY has been observed for both PNFS and Chimera.
UNSTABLE_RETRY_LIST = [errno.EBUSY, errno.ELOOP,
                       errno.ESTALE,
                       errno.ENOENT, errno.EIO,
                       ]
STABLE_RETRY_LIST = [errno.EBUSY, errno.ELOOP]

# Initial value to use with the semaphore to limit the number of simultaneous
# unstable file system accesses.
try:
    CPU_COUNT = multiprocessing.cpu_count()
except NameError:
    CPU_COUNT = 1

## unstable_filesystem comment
# In __wrapper() and __readline(), for unstable filesystems (aka PNFS)
# only allow a small number of simultaneous file system accesses.
# (REN): This is done through a sem_lock but... that is ALWAYS NONE. WAT
try:
    # posix_ipc is an extension module.  If available, use it.
    # sem_lock = posix_ipc.Semaphore("/encp", posix_ipc.O_CREAT, 0777, CPU_COUNT)
    sem_lock = None  # WAT. THIS CAN NOT THROW NameError
    # It has been like this since code was committed!
except NameError:
    try:
        # If posix_ipc is not available, just use the useless one from
        # the multiprocessing module.  Frozen executables with the python
        # 2.6 version don't succeed in including the _multiprocessing
        # helper module.
        sem_lock = multiprocessing.BoundedSemaphore(CPU_COUNT)
    except NameError:
        # Frozen python programs don't find _multiprocessing.so (except
        # on the build machine).  So, fail over to the even more useless
        # semaphore from the threading module.
        #
        # Useless might be a bit harsh, this will still have an effect
        # on migration since it is multithreaded.
        sem_lock = threading.BoundedSemaphore(CPU_COUNT)
# (REN): So what does unstable_filesystem actually do? It seems to only establish
# retry list.

# Useful for debugging.
if sem_lock:
    sys.stderr.write("type(sem_lock): %s with count: %s\n" % (type(sem_lock),
                                                              CPU_COUNT))


# Helper function for readline() and readlines() to retry transient errors.
#
# The fp parameter is a file object from a builtin open() call.
# The unstable_filesystem parameter should be set true by calls from pnfs.py.
# This is to allow for automatic retry of known suspect errors.
def __readline(fp, func="readline", unstable_filesystem=False):
    """
    Readline using either readline or readlines

    Args:
        fp (File): File handle to read line(s) from
        func (str): Function name to use ("readline" or "readlines")
            Default: "readline"
        unstable_filesystem (bool): Whether the filesystem is unstable (e.g. PNFS)
            See comment on unstable filesystems. Default: False

    Returns:
        (str): File content
    """
    if func not in ["readline", "readlines"]:
        raise TypeError("expected readline or readlines")

    if unstable_filesystem and sem_lock:
        sem_lock.acquire()

    try:
        try:
            file_content = getattr(fp, func)()
        finally:
            # Cleanup resources.
            if unstable_filesystem and sem_lock:
                # We need to release this lock, since __wrapper() will wait
                # until it can grab it.
                sem_lock.release()
    except (OSError, IOError) as e_msg:
        try:
            # The only thing unstable_filesystem actually does
            if unstable_filesystem:
                retry_list = UNSTABLE_RETRY_LIST
            else:
                retry_list = STABLE_RETRY_LIST

            if e_msg.args[0] in retry_list:
                #
                # Most .()() special PNFS files can be read once.  Seeking
                # back to the beginning and reading again will give an ESTALE
                # error.  Rarely, these files get into this state without
                # reading them first.  For these special files, we perform
                # the following trickery.
                #
                i = 0
                file_content = None
                while i < RETRY_COUNT and file_content is None:
                    i = i + 1  # don't hang forever

                    # Get another file object.  We need to sure to handle
                    # EUID/EGID values correctly, so use wrapper() and not
                    # __wrapper().
                    try:
                        fp1 = wrapper(__builtins__['open'], (fp.name, fp.mode))
                    except (OSError, IOError), msg2:
                        if msg2.args[0] in retry_list:
                            time.sleep(i)
                            continue
                        else:
                            raise sys.exc_info()[0], sys.exc_info()[1], \
                                sys.exc_info()[2]

                    if unstable_filesystem and sem_lock:
                        sem_lock.acquire()

                    # Get the file's contents.
                    try:
                        try:
                            file_content = getattr(fp1, func)()
                        finally:
                            # Cleanup resources.
                            fp1.close()
                            if unstable_filesystem and sem_lock:
                                sem_lock.release()
                    except (OSError, IOError), msg2:
                        if msg2.args[0] in retry_list:
                            time.sleep(i)
                            continue
                        else:
                            raise sys.exc_info()[0], sys.exc_info()[1], \
                                sys.exc_info()[2]

                    # We got the file, now stop looping.
                    break
                else:
                    if Trace.log_func != Trace.default_log_func:
                        message = "looped in __readline to many times: %s" \
                                  % (sys.exc_info()[1],)
                        Trace.log(e_errors.INFO, message)

                    raise sys.exc_info()[0], sys.exc_info()[1], \
                        sys.exc_info()[2]
            else:
                raise sys.exc_info()[0], sys.exc_info()[1], \
                    sys.exc_info()[2]
        except:
            # For these exceptions, sem_lock should already be released.
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
    except:
        raise sys.exc_info()[0], sys.exc_info()[1], \
            sys.exc_info()[2]

    return file_content


# Helper function for wrapper() to retry transient errors.
#
# The unstable_filesystem parameter should be set true by calls from pnfs.py.
# This is to allow for automatic retry of known suspect errors.
def __wrapper(function, args=(), unstable_filesystem=None):
    """
    Wrap supplied function with retry logic

    Args:
        function (Function): Function to wrap
        args (Tuple(*)): Arguments to pass to function
        unstable_filesystem: Whether the filesystem is unstable (e.g. PNFS)
            See comment on unstable filesystems. Default: False

    Returns:
        (Any): function return value
    """
    if unstable_filesystem and sem_lock:
        sem_lock.acquire()

    try:
        # The only thing unstable_filesystem actually does
        if unstable_filesystem:
            retry_list = UNSTABLE_RETRY_LIST
        else:
            retry_list = STABLE_RETRY_LIST

        count = 0
        while count < RETRY_COUNT:
            try:
                rtn = function(*args)
            except (OSError, IOError) as e_msg:
                count = count + 1
                if e_msg.errno in retry_list:
                    if count < RETRY_COUNT:
                        # sys.stderr.write(
                        #    "COUNT: %s  FUNCTION: %s  ARGS: %s  errno: %s [%s]\n" % \
                        #    (count, log_func_name, args, msg.errno, os.strerror(msg.errno)))
                        if e_msg.errno not in [errno.ENOENT, errno.EIO]:
                            time.sleep(count)
                        else:
                            # We don't want to take very long for open()
                            # when we get ENOENT.
                            time.sleep(1)
                        continue
                    else:
                        raise sys.exc_info()[0], sys.exc_info()[1], \
                            sys.exc_info()[2]
                else:
                    raise sys.exc_info()[0], sys.exc_info()[1], \
                        sys.exc_info()[2]

            if unstable_filesystem and sem_lock:
                sem_lock.release()
            return rtn

        exception_object = OSError(errno.EIO, "Unknown error")
        raise exception_object
    except:
        if unstable_filesystem and sem_lock:
            sem_lock.release()
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]


#
# wrapper to call os functions that takes care of euid/eid
#
# The unstable_filesystem parameter should be set true by calls from pnfs.py.
# This is to allow for automatic retry of known suspect errors.
def wrapper(function, args=(), unstable_filesystem=None):
    """
    Wrap supplied function with retry logic, try to resolve uid/gid issues
    First, attempt access with current euid/guid
    If this fails, try as root
    If that fails, try to match euid/guid to the file's as provided by fstat
    If that fails, set things back to original euid/guid and give up

    Args:
        function (Function): Function to wrap
        args (Tuple(*)): Arguments to pass to function
        unstable_filesystem: Whether the filesystem is unstable (e.g. PNFS)
            See comment on unstable filesystems. Default: False

    Returns:
        (Any): function return value
    """
    if not instanceof(args, types.TupleType):
        use_args = (args,)
    else:
        use_args = args

    try:
        rtn = __wrapper(function, use_args,
                        unstable_filesystem=unstable_filesystem)
    except (OSError, IOError) as e_msg:
        if e_msg.errno in [errno.EACCES, errno.EPERM] and \
                os.getuid() == 0:
            acquire_lock_euid_egid()
            current_euid = os.geteuid()
            current_egid = os.getegid()

            # We might need to go back to being root again to access
            # the target file or directory.
            try:
                if current_euid != 0:
                    os.seteuid(0)
                if current_egid != 0:
                    os.setegid(0)
            except:
                release_lock_euid_egid()
                raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]

            # The next thing to try is doing it again while being root.
            try:
                rtn = __wrapper(function, use_args)
            except (OSError, IOError), msg2:  # Anticipated errors.
                if msg2.errno in [errno.EACCES, errno.EPERM] and \
                        len(args) > 0:
                    # Root does not have modify permissions for
                    # non-trusted PNFS file systems or for NFS/Chimera
                    # without the no_root_squash mount option.  Match
                    # the ownership of the file for one last thing
                    # to try.
                    try:
                        if function == os.stat:
                            use_function = os.stat
                        elif function == os.fstat:
                            use_function = os.fstat
                        elif function == os.lstat:
                            use_function = os.lstat
                        elif len(args) and isinstance(args[0], types.IntType):
                            use_function = os.fstat
                        else:
                            # Are there situations that this is not correct?
                            use_function = os.stat

                        fstat = use_function(args[0])

                        if fstat[stat.ST_GID] != 0:
                            os.setegid(fstat[stat.ST_GID])
                        if fstat[stat.ST_UID] != 0:
                            os.seteuid(fstat[stat.ST_UID])
                    except:
                        release_lock_euid_egid()
                        raise sys.exc_info()[0], sys.exc_info()[1], \
                            sys.exc_info()[2]

                    # Perform the requested function, this time as the
                    # correct UID and GID.  This can still fail with
                    # permission errors if the owner does not have
                    # execute and read permissions for all directories
                    # in the path.
                    try:
                        # rtn = function(*use_args)
                        rtn = __wrapper(function, use_args)
                    except (OSError, IOError):  # Anticipated errors.
                        release_lock_euid_egid()
                        raise sys.exc_info()[0], sys.exc_info()[1], \
                            sys.exc_info()[2]
                    except:  # Un-anticipated errors.
                        release_lock_euid_egid()
                        raise sys.exc_info()[0], sys.exc_info()[1], \
                            sys.exc_info()[2]
                else:
                    release_lock_euid_egid()
                    raise sys.exc_info()[0], sys.exc_info()[1], \
                        sys.exc_info()[2]
            except:  # Un-anticipated errors.
                release_lock_euid_egid()
                raise sys.exc_info()[0], sys.exc_info()[1], \
                    sys.exc_info()[2]

            try:
                # First, set things back to root.
                if current_euid != os.geteuid() or \
                        current_egid != os.getegid():
                    os.seteuid(0)
                    os.setegid(0)

                # Second, set the effective IDs back to what they were.
                if current_egid != os.getegid():
                    os.setegid(current_egid)
                if current_euid != os.geteuid():
                    os.seteuid(current_euid)
            except:
                release_lock_euid_egid()
                raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]

            release_lock_euid_egid()
            return rtn
        else:
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
    except:
        if Trace.log_func != Trace.default_log_func:
            # Only send this to the log file, not to standard out/err.
            Trace.log_stack_trace()
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]

    return rtn
