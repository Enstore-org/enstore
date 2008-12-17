#!/usr/bin/env python
###############################################################################
#
# $Author$
# $Date$
# $Id$
#
# collection of utility functions taken from encp.py
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

## mode is one of os.F_OK, os.W_OK, os.R_OK or os.X_OK.
## file_stats is the return from os.stat()

#The os.access() and the access(2) C library routine use the real id when
# testing for access.  This function does the same thing but for the
# effective ID.
def e_access(path, mode):

    #Test for existance.
    try:
        file_stats = get_stat(path)
    except OSError:
        return 0
    
    return e_access_cmp(file_stats, mode)

#Check the bits to see if we have the requested mode access.
def e_access_cmp(file_stats, mode):
    stat_mode = file_stats[stat.ST_MODE]
    
    #Make sure a valid mode was passed in.
    if mode & (os.F_OK | os.R_OK | os.W_OK | os.X_OK) != mode:
        return 0

    # Need to check for each type of access permission.

    if mode == os.F_OK:
        # In order to get this far, the file must exist.
        return 1

    if mode & os.R_OK:  #Check for read permissions.
        #If the user is user root.
        if os.geteuid() == 0:
            #return 1
            pass
        #Anyone can read this file.
        elif (stat_mode & stat.S_IROTH):
            #return 1
            pass
        #This is the files owner.
        elif (stat_mode & stat.S_IRUSR) and \
           file_stats[stat.ST_UID] == os.geteuid():
            #return 1
            pass
        #The user has group access.
        elif (stat_mode & stat.S_IRGRP) and \
           (file_stats[stat.ST_GID] == os.geteuid() or
            file_stats[stat.ST_GID] in os.getgroups()):
            #return 1
            pass
        else:
            return 0

    if mode & os.W_OK:  #Check for write permissions.
        #If the user is user root.
        if os.geteuid() == 0:
            #return 1
            pass
        #Anyone can write this file.
        elif (stat_mode & stat.S_IWOTH):
            #return 1
            pass
        #This is the files owner.
        elif (stat_mode & stat.S_IWUSR) and \
           file_stats[stat.ST_UID] == os.geteuid():
            #return 1
            pass
        #The user has group access.
        elif (stat_mode & stat.S_IWGRP) and \
           (file_stats[stat.ST_GID] == os.geteuid() or
            file_stats[stat.ST_GID] in os.getgroups()):
            #return 1
            pass
        else:
            return 0
    
    if mode & os.X_OK:  #Check for execute permissions.
        #If the user is user root.
        if os.geteuid() == 0:
            #return 1
            pass
        #Anyone can execute this file.
        elif (stat_mode & stat.S_IXOTH):
            #return 1
            pass
        #This is the files owner.
        elif (stat_mode & stat.S_IXUSR) and \
           file_stats[stat.ST_UID] == os.geteuid():
            #return 1
            pass
        #The user has group access.
        elif (stat_mode & stat.S_IXGRP) and \
           (file_stats[stat.ST_GID] == os.geteuid() or
            file_stats[stat.ST_GID] in os.getgroups()):
            #return 1
            pass
        else:
            return 0

    return 1

#############################################################################

#arg can be: filename, file descritor, file object, a stat object
def get_stat(arg):
    try:
        if type(arg) == types.StringType:
            f_stat = os.stat(arg)
        elif type(arg) == types.IntType:
            f_stat = os.fstat(arg)
        elif type(arg) == types.FileType:
            f_stat = os.fstat(arg.fileno())
        elif type(arg) == types.TupleType or type(arg) == os.stat_result:
            f_stat = arg
        else:
            raise TypeError("Expected path, file descriptor or file object; "
                            "not %s" % (type(arg),))
    except OSError, msg:
        #If we were denied access and our effective IDS were not root's,
        # set the effective IDS to root so we can try again.
        if msg.errno in [errno.EACCES, errno.EPERM] and \
               os.getuid() == 0 and os.geteuid() != 0:
            euid_lock.acquire()
            current_euid = os.geteuid()
            current_egid = os.getegid()
            
            os.seteuid(0)
            os.setegid(0)

            try:
                #Calling stat again won't get stuck in a loop since the
                # effective IDS have been changed.
                f_stat = get_stat(arg)
            except OSError:
                euid_lock.release()
                raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]

            os.seteuid(current_euid)
            os.setegid(current_egid)

            euid_lock.release()
        else:
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]

    return f_stat


#############################################################################

#If root is running the process, we may need to change the euid.  Is this
# only applicable to migration?
euid_lock = threading.RLock()

#Match the effective uid/gid of a file.
# arg: could be a pathname, fileno or file object.
#
# We need to do this when user root, so that migration modify non-/pnfs/fs
# (and non-trusted) pnfs mount points.
def match_euid_egid(arg):

    if os.getuid() == 0:# and getattr(e, 'migration_or_duplication', None):

        f_stat = get_stat(arg)

        euid_lock.acquire()
                    
        try:
            set_euid_egid(f_stat[stat.ST_UID], f_stat[stat.ST_GID])
        except:
            euid_lock.release()
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]

def set_euid_egid(euid, egid):

    if os.getuid() == 0:

        #We need to set these back to root, with uid first.
        os.seteuid(0)
        os.setegid(0)

        #First look at the gid for setting them.
        os.setegid(egid)
        #Then look a the uid.
        os.seteuid(euid)

#Release the lock.
def end_euid_egid(reset_ids_back = False):

    if os.getuid() == 0 or os.getgid() == 0:
        if reset_ids_back:
            os.seteuid(0)
            os.setegid(0)
        
        try:
            euid_lock.release()
        except RuntimeError:
            pass  #Already unlocked.

#############################################################################
        
