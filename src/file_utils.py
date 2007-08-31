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
import string
import time
import errno
import socket
import select
import pprint
import rexec
import os
import stat
import Trace
import e_errors

#The os.access() and the access(2) C library routine use the real id when
# testing for access.  This function does the same thing but for the
# effective ID.

def e_access(path, mode):
    
    #Test for existance.
    try:
        file_stats = os.stat(path)
        stat_mode = file_stats[stat.ST_MODE]
    except OSError:
        return 0

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

def set_outfile_permissions(ticket):

    if not ticket.get('copy', None):  #Don't set permissions if copy.
        set_outfile_permissions_start_time = time.time()

        #Attempt to get the input files permissions and set the output file to
        # match them.
        if ticket['outfile'] != "/dev/null":
            try:
                #Dmitry handle remote file case
                perms = None
                if ( os.path.exists(ticket['infile']) ):
                    perms = os.stat(ticket['infile'])[stat.ST_MODE]
                else:
                    perms = ticket['wrapper']['pstat'][stat.ST_MODE]
                os.chmod(ticket['outfile'], perms)
                ticket['status'] = (e_errors.OK, None)
            except OSError, msg:
                Trace.log(e_errors.INFO, "chmod %s failed: %s" % \
                          (ticket['outfile'], msg))
                ticket['status'] = (e_errors.USERERROR,
                                    "Unable to set permissions.")

#        Trace.message(TIME_LEVEL, "Time to set_outfile_permissions: %s sec." %
#                      (time.time() - set_outfile_permissions_start_time,))
    return
    
