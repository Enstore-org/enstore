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

## mode is one of os.F_OK, os.W_OK, os.R_OK or os.X_OK.
## file_stats is the return from os.stat()

#The os.access() and the access(2) C library routine use the real id when
# testing for access.  This function does the same thing but for the
# effective ID.
def e_access(path, mode):

    #Test for existance.
    try:
        file_stats = os.stat(path)
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

        if type(arg) == types.StringType:
            f_stat = os.stat(arg)
        elif type(arg) == types.IntType:
            f_stat = os.fstat(arg)
        elif type(arg) == types.FileType:
            f_stat = os.fstat(arg.fileno())
        else:
            raise TypeError("Expected path, file descriptor or file object; "
                            "not %s" % (type(arg),))

        euid_lock.acquire()

        try:
            #First look at the gid.
            if f_stat[stat.ST_GID] != os.getegid():
                if os.getegid() != 0:
                    os.setegid(0)
                os.setegid(f_stat[stat.ST_GID])
            #Then look a the uid.
            if f_stat[stat.ST_UID] != os.geteuid():
                if os.geteuid() != 0:
                    os.seteuid(0)
                os.seteuid(f_stat[stat.ST_UID])
        except:
            euid_lock.release()
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]


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
        
