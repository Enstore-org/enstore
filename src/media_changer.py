#!/usr/bin/env python

#########################################################################
#
# $Id$
#
#########################################################################
#                                                                       #
# Media Changer server.                                                 #
# Media Changer is an abstract object representing a physical device or #
# operator who performs mounts / dismounts of tapes on tape drives.     #
# At startup the media changer process takes its argument from the      #
# command line and configures based on the dictionary entry in the      #
# Configuration Server for this type of Media Changer.                  #
# It accepts then requests from its clients and performs tape mounts    #
# and dismounts                                                         #
#                                                                       #
#########################################################################

# system imports
import os
import sys
import types
import time
import signal
import string
import socket
import struct
import fcntl
# if sys.version_info < (2, 2, 0):
#    import FCNTL #FCNTL is deprecated in python 2.2 and later.
#    fcntl.F_GETFL = FCNTL.F_SETLKW
#    fcntl.F_SETFL = FCNTL.F_WRLCK
#    fcntl.F_SETFL = FCNTL.F_RDLCK
#    fcntl.F_SETFL = FCNTL.F_UNLCK
import pprint
import re
import subprocess
import copy
import select
import errno
import multiprocessing
import traceback
import timeofday
# import ConfigParser - Unused

# enstore imports
import hostaddr
import configuration_client
import dispatching_worker
import generic_server
import monitored_server
import enstore_constants
import option
import Trace
import e_errors
import volume_clerk_client
import event_relay_messages
import callback
import enstore_functions2
import udp_common
# This import is broken, it is loaded conditionally
# for a media changer type which no longer exists.
# TODO: remove entirely
# import aml2


# The following are used by mtx
import threading

try:
    import mtx  # not everyone needs mtx media changer, running through library calls
except ImportError:
    mtx = None
    pass

# import log_client - Unused
import media_changer_client


def _lock(f, op):
    dummy = fcntl.fcntl(f.fileno(), fcntl.F_SETLKW,
                        struct.pack('2h8l', op,
                                    0, 0, 0, 0, 0, 0, 0, 0, 0))
    Trace.trace(21, '_lock ' + repr(dummy))


def writelock(f):
    _lock(f, fcntl.F_WRLCK)


def readlock(f):
    _lock(f, fcntl.F_RDLCK)


def unlock(f):
    _lock(f, fcntl.F_UNLCK)


# Set the default log level for routine operations to 88.  This will send
# the output to the DEBUGLOG but not the LOG file (by default).  Using
# --do-print this can be overridden.
ACTION_LOG_LEVEL = 88

# There exists two work queues.  One involves tape operations.  The other
# involves queries.
QUEUE_COUNT = 2

EMPTY = 'empty'  # volume "name" used in some classes
BUSY = 'busy'  # to indicate that the slot is under some activity, like dismount into ths slot.
INUSE = 'in use'  # to indicate that drive is in use


# Make sure the number of max_work is within bounds.  The
def bound_max_work(unbound_max_work):
    max_work = min(int(dispatching_worker.MAX_CHILDREN / QUEUE_COUNT),
                   int(unbound_max_work))
    return max(0, max_work)  # Allow zero for "pausing".


# media loader template class
class MediaLoaderMethods(dispatching_worker.DispatchingWorker,
                         generic_server.GenericServer):
    query_functions = ["displayDrive", "getVolState", "getDriveState", "listDrives",
                       "listVolumes", "listVolumes2", "listClean",
                       "listSlots"]
    work_functions = ["mount", "dismount", "insert", "eject", "cleanCycle",
                      "homeAndRestart"]

    def return_max_work(self):
        return self.max_work

    def __init__(self, medch, max_work, csc_local):
        self.logdetail = 1
        self.name = medch
        self.name_ext = "MC"
        generic_server.GenericServer.__init__(self, csc_local, medch,
                                              function=self.handle_er_msg)
        Trace.init(self.log_name)
        self.max_work = bound_max_work(max_work)
        self.workQueueClosed = 0
        self.insertRA = None
        #   pretend that we are the test system
        #   remember, in a system, there is only one bfs
        #   get our port and host from the name server
        #   exit if the host is not this machine
        self.mc_config = self.csc.get(medch)

        self.alive_interval = monitored_server.get_alive_interval(
            self.csc, medch, self.mc_config)
        dispatching_worker.DispatchingWorker.__init__(
            self, (self.mc_config['hostip'], self.mc_config['port']))
        self.idleTimeLimit = 600  # default idle time in seconds
        self.lastWorkTime = time.time()
        self.robotNotAtHome = 1
        self.set_this_error = e_errors.OK  # this is useful for testing mover errors
        self.timeInsert = time.time()

        # Set up the communications with the event relay task.
        self.erc.start([event_relay_messages.NEWCONFIGFILE])
        # start our heartbeat to the event relay process
        self.erc.start_heartbeat(self.name, self.alive_interval,
                                 self.return_max_work)

        # Create the lists of what is currently being done.
        self.work_list = []
        self.work_cleaning_list = []
        self.work_query_list = []

    # Also, works for dismount tickets too.
    @staticmethod
    def check_mount_ticket(ticket):
        if ticket['function'] in ("mount", "dismount"):
            if "vol_ticket" not in ticket:
                err_msg = 'MISSING VOL_TICKET'
                ticket['status'] = (e_errors.MALFORMED, 0, err_msg)
                Trace.log(e_errors.ERROR, "%s" % (ticket['status'],))
                return ticket['status']
            if "drive_id" not in ticket:
                err_msg = 'MISSING DRIVE_ID'
                ticket['status'] = (e_errors.MALFORMED, 0, err_msg)
                Trace.log(e_errors.ERROR, "%s" % (ticket['status'],))
                return ticket['status']
            if ticket['vol_ticket'].get('external_label') is None:
                err_msg = "MISSING EXTERNAL LABEL for %s %s" % \
                          (ticket["function"], ticket["drive_id"])
                ticket['status'] = (e_errors.MALFORMED, 0, err_msg)
                Trace.log(e_errors.ERROR, "%s" % (ticket['status'],))
                return ticket['status']

        return e_errors.OK, 0, None

    # retry function call - By default don't retry.
    def retry_function(self, function, *args):
        rc = function(*args)
        return rc

    #########################################################################
    # These functions are named by the media_changer_client.  All of these
    # functions should call self.DoWork().  By using self.DoWork() the
    # queue limit of requests is honored.
    #
    # Functions that use DoWork() do not need to call reply_to_caller()
    # explicitly.  set_max_work() and getwork() don't talk to the robot and
    # fall into this exception category.  robotQuery() falls into this
    # exception category because you don't want to be waiting for a tape to
    # mount/dismount for the queue to open up to find out if the robot is
    # working.
    #########################################################################

    # wrapper method for client - server communication
    def loadvol(self, ticket):
        ticket['function'] = "mount"
        Trace.trace(11, "loadvol %s" % (ticket,))

        # Make sure this request has all the necessary information included.
        check_status = self.check_mount_ticket(ticket)
        if not e_errors.is_ok(check_status[0]):
            return check_status

        return self.DoWork(self.load, ticket)

    # wrapper method for client - server communication
    def unloadvol(self, ticket):
        ticket['function'] = "dismount"
        Trace.trace(11, "unloadvol %s" % (ticket,))

        # Make sure this request has all the necessary information included.
        check_status = self.check_mount_ticket(ticket)
        if not e_errors.is_ok(check_status[0]):
            return check_status

        return self.DoWork(self.unload, ticket)

    # wrapper method for client - server communication
    def insertvol(self, ticket):
        ticket['function'] = "insert"
        return self.DoWork(self.insert, ticket)

    # wrapper method for client - server communication
    def ejectvol(self, ticket):
        ticket['function'] = "eject"
        return self.DoWork(self.eject, ticket)

    # Is this function even used anywhere?
    def homeAndRestartRobot(self, ticket):
        ticket['function'] = "homeAndRestart"
        return self.DoWork(self.robotHomeAndRestart, ticket)

    # Puts a clean request in the cleaning_work_list queue.
    def doCleaningCycle(self, ticket):
        ticket['function'] = "cleanCycle"
        return self.DoWork(self.cleanCycle, ticket)

    def set_max_work(self, ticket):
        self.max_work = bound_max_work(ticket['max_work'])
        ticket['status'] = (e_errors.OK, 0, None)
        self.reply_to_caller(ticket)

    def __getwork(self):
        # Pack info for the tape handling operations.
        result = []
        for i in self.work_list:
            Trace.trace(ACTION_LOG_LEVEL, 'getwork!: %s' % (i,))
            # TODO: These should use `if key in dict` not `try/except`
            external_label = ""
            drive_id = ""
            try:
                if i['vol_ticket']['external_label']:
                    external_label = i['vol_ticket']['external_label']
            except KeyError:
                pass
            try:
                if i['drive_id']:
                    drive_id = i['drive_id']
            except KeyError:
                pass

            result.append((i['function'], external_label, drive_id,
                           i.get('pid', None), i['r_a'][0], i['timestamp']))

        # Pack info for information request operations.
        query_result = []
        for i in self.work_query_list:
            external_label = ""
            drive_id = ""
            try:
                if i['external_label']:
                    external_label = i['external_label']
            except KeyError:
                pass
            try:
                if i['drive']:
                    drive_id = i['drive']
            except KeyError:
                pass

            query_result.append((i['function'], external_label, drive_id,
                                 i.get('pid', None), i['r_a'][0],
                                 i['timestamp']))

        # The order for both lists is:
        # 1: type of operation (aka function)
        # 2: external_label (if it applies)
        # 3: drive (if it applies)
        # 4: process id
        # 5: requesting IP address and port number
        # 6: human-readable timestamp

        return result, query_result

    def _getwork(self, ticket):
        result, query_result = self.__getwork()
        ticket['work_list'] = result
        ticket['work_query_list'] = query_result
        ticket['max_work'] = self.max_work
        ticket['status'] = (e_errors.OK, 0, None)
        return ticket

    def getwork(self, ticket):
        Trace.trace(ACTION_LOG_LEVEL, 'getwork %s' % (ticket,))

        ticket = self._getwork(ticket)
        self.reply_to_caller(ticket)

    # wrapper method for client - server communication
    def viewvol(self, ticket):
        ticket['function'] = "getVolState"
        return self.DoWork(self.getVolState, ticket)

    # wrapper method for client - server communication
    def viewdrive(self, ticket):
        ticket['function'] = "getDriveState"
        rtn = self.DoWork(self.getDriveState, ticket)
        return rtn

    # wrapper method for client - server communication
    def displaydrive(self, ticket):
        ticket['function'] = "displayDrive"
        rtn = self.DoWork(self.displayDrive, ticket)
        return rtn

    # wrapper method for client - server communication
    def robotQuery(self, ticket):
        # Don't retry this query or wait for the work queue to fit it in,
        # we want to know current status now.
        ticket['status'] = self.query_robot(ticket)
        self.reply_to_caller(ticket)

    # wrapper method for client - server communication
    def list_drives(self, ticket):
        ticket['function'] = "listDrives"
        return self.DoWork(self.listDrives, ticket)

    # wrapper method for client - server communication
    def list_volumes(self, ticket):
        ticket['function'] = "listVolumes"
        return self.DoWork(self.listVolumes, ticket)

    # wrapper method for client - server communication
    def list_volumes2(self, ticket):
        ticket['function'] = "listVolumes2"
        return self.DoWork(self.listVolumes2, ticket)

    # wrapper method for client - server communication
    def list_clean(self, ticket):
        ticket['function'] = "listClean"
        return self.DoWork(self.listClean, ticket)

    # wrapper method for client - server communication
    def list_slots(self, ticket):
        ticket['function'] = "listSlots"
        return self.DoWork(self.listSlots, ticket)

    #########################################################################
    # These functions are the internal functions used within the media changer
    # for the requests received from the media changer client.
    #########################################################################

    # load volume into the drive; default, overridden for other media changers
    def load(self, ticket):
        __pychecker__ = "no-argsused"
        return e_errors.OK, 0, None

    # unload volume from the drive; default overridden for other media changers
    def unload(self, ticket):
        __pychecker__ = "no-argsused"
        return e_errors.OK, 0, None

    # insert volume into the robot; default overridden for other media changers
    def insert(self, ticket):
        __pychecker__ = "no-argsused"
        return e_errors.OK, 0, None, ""  # return "" - no inserted volumes

    # eject volume from the robot; default overridden for other media changers
    def eject(self, ticket):
        __pychecker__ = "no-argsused"
        return e_errors.OK, 0, None

    def robotHomeAndRestart(self, ticket):
        __pychecker__ = "no-argsused"
        return e_errors.OK, 0, None

    def cleanCycle(self, ticket):
        __pychecker__ = "no-argsused"
        return e_errors.OK, 0, None

    # getVolState in the drive; default overridden for other media changers
    def getVolState(self, ticket):
        __pychecker__ = "no-argsused"
        return e_errors.OK, 0, None, "O"  # return 'O' - occupied aka unmounted

    # getDriveState for the drive; default overridden for other media changers
    def getDriveState(self, ticket):
        __pychecker__ = "no-argsused"
        return e_errors.OK, 0, None, ""

    # displayDrive for the drive; default overridden for other media changers
    def displayDrive(self, ticket):
        """
        Used to get drive WWN
        """
        __pychecker__ = "no-argsused"
        return e_errors.ERROR, 0, None, "Not implemented"

    def query_robot(self, ticket):
        __pychecker__ = "no-argsused"
        return e_errors.OK, 0, None

    def listDrives(self, ticket):
        __pychecker__ = "no-argsused"
        return e_errors.OK, 0, None

    def listVolumes(self, ticket):
        __pychecker__ = "no-argsused"
        return e_errors.OK, 0, None

    def listVolumes2(self, ticket):
        __pychecker__ = "no-argsused"
        return e_errors.OK, 0, None

    def listClean(self, ticket):
        __pychecker__ = "no-argsused"
        return e_errors.OK, 0, None

    def listSlots(self, ticket):
        __pychecker__ = "no-argsused"
        return e_errors.OK, 0, None

    #########################################################################
    # These functions are generic media changer internal functions.
    #########################################################################

    # prepare is overridden by dismount for mount; i.e. for tape drives we
    # always dismount before mount
    @staticmethod
    def prepare(_):
        __pychecker__ = "no-argsused"
        return e_errors.OK, 0, None

    def doWaitingInserts(self):
        return e_errors.OK, 0, None

    @staticmethod
    def getNretry():
        number_of_retries = 3
        return number_of_retries

    def add_to_work_list(self, ticket):
        if ticket['function'] in self.work_functions:
            queue = self.work_list
        else:
            queue = self.work_query_list

        # search the work list of the ticket
        for i in queue:
            # These elements contain the unique value r_a, which is set by
            # UDPserver.
            if i['r_a'] == ticket['r_a']:
                break
        else:
            ticket['timestamp'] = time.ctime()
            queue.append(ticket)

    def remove_from_work_list(self, ticket):
        Trace.trace(ACTION_LOG_LEVEL, 'remove_from_work_list %s' % (ticket,))
        Trace.trace(ACTION_LOG_LEVEL, 'remove_from_work_list work_functions %s' % (self.work_functions,))
        if ticket['function'] in self.work_functions:
            queue = self.work_list
        else:
            queue = self.work_query_list

        if 'r_a' not in ticket:
            Trace.log(e_errors.ERROR,
                      "Trying to remove item from work list that does not"
                      " contain an r_a value: %s" % (str(ticket),))
            # There is no way to match this to any of the queued items.
            # Thus, we skip it.
            return

        for i in queue:
            # These elements contain the unique value r_a, which is set by
            # UDPserver.
            if 'r_a' not in i:
                Trace.log(e_errors.ERROR,
                          "Found item in queue without an r_a value: %s" %
                          (str(i),))
                # This item can never be deleted though normal operation.
                # Thus, we remove it now to keep the queue from growing.
                queue.remove(i)
                continue

            if i['r_a'] == ticket['r_a']:
                queue.remove(i)
                break

    def exists_in_work_list(self, ticket):

        if ticket['function'] in self.work_functions:
            queue = self.work_list
        else:
            queue = self.work_query_list

        for i in queue:
            # These elements contain the unique value r_a, which is set by
            # UDPserver.
            if i['r_a'] == ticket['r_a']:
                return 1  # Found
        else:
            return 0  # Not found

    def length_of_work_list(self, ticket):
        if ticket['function'] in self.work_functions:
            queue = self.work_list
        else:
            queue = self.work_query_list

        return len(queue)

    def log_work_list(self, ticket=None):
        result, query_result = self.__getwork()
        if ticket and ticket['function'] in self.work_functions:
            Trace.log(ACTION_LOG_LEVEL,
                      "work_list: %s" %
                      (str(result),))
        elif ticket and ticket['function'] in self.query_functions:
            Trace.log(ACTION_LOG_LEVEL,
                      "work_query_list: %s" %
                      (str(query_result),))
        else:
            Trace.log(ACTION_LOG_LEVEL,
                      "work_list: %s" %
                      (str(result),))
            Trace.log(ACTION_LOG_LEVEL,
                      "work_query_list: %s" %
                      (str(query_result),))

    # Used in DoWork() and WorkDone() to create consistent message strings.
    @staticmethod
    def get_common_message_string(ticket):
        # Output what we intend to do.
        if ticket['function'] in ("mount", "dismount"):
            common_message = "%s %s %s" % \
                             (ticket['function'],
                              ticket['vol_ticket']['external_label'],
                              ticket['drive_id'])
        elif ticket['function'] in ("getVolState",):
            common_message = "%s %s" % \
                             (ticket['function'],
                              ticket['external_label'])
        elif ticket['function'] in ("display_drive", "getDriveState"):
            common_message = "%s %s" % \
                             (ticket['function'],
                              ticket['drive'])
        elif ticket['function'] in ("insert",):
            common_message = "%s" % \
                             (ticket['function'],)
        elif ticket['function'] in ("eject",):
            common_message = "%s %s" % \
                             (ticket['function'],
                              ticket['volList'])
        else:  # List requests.
            common_message = "%s" % (ticket['function'],)

        return common_message

    # Do the forking and call the function
    #
    # A return on None means the request was dropped for one reason or
    # another. Otherwise, the return value should be a status tuple.
    def DoWork(self, function, ticket):

        # The function immediately called by dispatching worker should have
        # set a 'function' field in the ticket.
        if 'function' not in ticket:
            err_msg = "MISSING FUNCTION KEY"
            ticket['status'] = (e_errors.MALFORMED, 0, err_msg)
            Trace.log(e_errors.ERROR, "%s" % (ticket['status'],))
            return ticket['status']

        # if this a duplicate request, drop it
        if self.exists_in_work_list(ticket):
            message = "duplicate request, drop it %s %s" % \
                      (repr(ticket['r_a']), repr(ticket['r_a']))
            Trace.log(ACTION_LOG_LEVEL, message)
            return

        # Output what we intend to do.
        common_message = self.get_common_message_string(ticket)
        Trace.log(ACTION_LOG_LEVEL, "REQUESTED %s" % (common_message,))

        ###
        ### Determine if we need to drop the request for one reason or another.
        ###

        # Let work list length exceed max_work for cleanCycle.
        if ticket['function'] == "cleanCycle":
            pass
        # elif ticket['function'] in ["homeAndRestart"]:
        #    pass
        elif ticket['function'] in ('mount', 'dismount', 'eject', 'insert', 'updatedb'):
            # If we have max number of working children processes, assume
            # client will resend the request.
            if len(self.work_list) >= self.max_work:
                message = "MC Overflow: %s %s" % \
                          (repr(self.max_work), common_message)
                Trace.log(e_errors.INFO, message)

                # Need to call reply_to_caller() here since the request has
                # not been processed far enough for WorkDone() to reply
                # for us.
                ticket['status'] = (e_errors.MC_QUEUE_FULL, 0,
                                    "retry later")
                self.reply_to_caller(ticket)
                return
            # Else if the work queue is temporarily closed, assume client
            # will resend the request.
            elif self.workQueueClosed and len(self.work_list) > 0:
                message = "MC Queue Closed: %s %s" % \
                          (repr(len(self.work_list)), common_message)
                Trace.log(e_errors.INFO, message)
                return

            # If drive is doing a clean cycle; drop request and assume client
            # will resend the request.
            if ticket['function'] in ("mount", "dismount"):
                for i in self.work_list:
                    try:
                        if (i['function'] == "cleanCycle" and
                                'drive_id' in i and
                                i['drive_id'] == ticket['drive_id']):
                            message = "Dropped %s request for %s in %s, " \
                                      "drive in cleaning cycle." % \
                                      (ticket['function'],
                                       ticket['vol_ticket']['external_label'],
                                       ticket['drive_id'])
                            Trace.log(e_errors.INFO, message)
                            return
                    except:
                        Trace.handle_error()
                        Trace.log(e_errors.ERROR, "ERROR %s" % (i,))
        else:  # query requests
            if len(self.work_query_list) >= self.max_work:
                # If the request work list is full, drop request and assume
                # the client will resend the request.
                Trace.log(ACTION_LOG_LEVEL, 'Dropping %s due to full work_list' % (common_message,))
                return

        ### Otherwise, we can process work.

        # Output what we are going to do.
        Trace.log(e_errors.INFO, "PROCESSING %s" % (common_message,))

        # If function is insert and queue not empty: close work queue and
        # set values to prepare for completing this operation once all
        # pending requests are fulfilled.
        if ticket['function'] == "insert":
            if len(self.work_list) > 0:
                self.workQueueClosed = 1
                self.timeInsert = time.time()
                self.insertRA = ticket['r_a']
                return
            else:
                self.workQueueClosed = 0

        # If not a duplicate request or dropped request; fork the work.
        pipe = os.pipe()
        pid = self.fork(ttl=None)  # no time limit
        if pid:
            #  in parent process
            ticket['pid'] = pid

            self.add_select_fd(pipe[0])  # wait for reading half of pipe.
            os.close(pipe[1])  # close writing half of pipe.
            # add entry to outstanding work
            self.add_to_work_list(ticket)
            # log the new work list
            self.log_work_list(ticket)
            return

        #  in child process
        message = "mcDoWork> child begin %s" % (common_message,)
        Trace.log(ACTION_LOG_LEVEL, message)

        os.close(pipe[0])  # Close reading half of pipe.

        # do the work ...

        # ... if this is a mount, dismount first
        if ticket['function'] == "mount":
            message = "mcDoWork> child prepare dismount for %s" % \
                      (common_message,)
            Trace.trace(ACTION_LOG_LEVEL, message)
            Trace.log(ACTION_LOG_LEVEL, message)

            # don't print a failure  (no tape mounted) message that is
            # really a success
            self.logdetail = 0
            # To Do: The downside to passing the volume we want to mount is
            # that there is a warning message about the MC expecting to
            # unmount that tape.  Some blank value should be passed to
            # avoid that false warning.
            dismount_ticket = {'work': 'unloadvol',
                               'vol_ticket': ticket['vol_ticket'],
                               'drive_id': ticket['drive_id'],
                               }
            sts = self.prepare(dismount_ticket)
            self.logdetail = 1  # back on

            message = "%s returned %s" % (message, sts[2])
            Trace.trace(ACTION_LOG_LEVEL, message)
            Trace.log(ACTION_LOG_LEVEL, message)

            # XXX - Why isn't sts processed for errors here?

        message = "mcDoWork> child doing %s" % (common_message,)
        Trace.trace(ACTION_LOG_LEVEL, message)
        Trace.log(ACTION_LOG_LEVEL, message)

        sts = function(ticket)  # Call the function!

        message = "mcDoWork> child %s returned %s" % (common_message, sts)
        Trace.trace(ACTION_LOG_LEVEL, message)
        Trace.log(ACTION_LOG_LEVEL, message)

        ticket["status"] = sts
        # Send status back to MC parent via pipe then via dispatching_worker
        # and WorkDone ticket so dispatching_worker calls WorkDone().
        ticket['work'] = "WorkDone"

        # There must be a better way to write to the pipe connected to the
        # parent process.  Probably with callback.py.
        ticket_repr = repr(('0', '0', ticket))
        bytecount = "%08d" % (len(ticket_repr),)
        try:
            os.write(pipe[1], bytecount)
            os.write(pipe[1], ticket_repr)
            os.close(pipe[1])
        except (OSError, IOError) as e:
            message = "mcDoWork> child %s failed reporting to parent: %s" \
                      % (common_message, str(e))
            Trace.trace(ACTION_LOG_LEVEL, message)
            Trace.log(ACTION_LOG_LEVEL, message)

        # noinspection PyProtectedMember
        os._exit(0)

    # dispatching_worker sends "WorkDone" ticket here, and we reply_to_caller()
    def WorkDone(self, ticket):

        # remove work from outstanding work list
        self.remove_from_work_list(ticket)

        # log what was done
        status = ticket.get('status', None)
        if status and e_errors.is_ok(status[0]):
            level = e_errors.INFO
        else:
            level = e_errors.ERROR
        common_message = self.get_common_message_string(ticket)
        Trace.log(level, "FINISHED %s returned %s" % (common_message, status))
        # log the new work list
        self.log_work_list(ticket)

        # report back to original client - probably a mover
        #
        # Some functions need to handle the reply directly (list_volumes).
        # They, should set 'no_reply' to python true in the ticket.
        if not ticket.get('no_reply', None):
            self.reply_to_caller(ticket)

        self.robotNotAtHome = 1
        self.lastWorkTime = time.time()

        # if work queue is closed and work_list is empty, do insert
        #
        # Shouldn't there be a better way of scheduling this?  Waiting until
        # a previous request completes doesn't seem correct.  This also leads
        # into how sts could/should be processed.
        self.doWaitingInserts()

    # simple elapsed timer
    @staticmethod
    def delta_t(begin):
        (ut, st, cut, cst, now) = os.times()
        return now - begin, now

    # Kill the process with pid, use cmd as the command string that is
    # getting killed for logging purposes.
    @staticmethod
    def kill_it(pid, cmd):
        message = "killing %d => %s" % (pid, cmd)
        print timeofday.tod(), message
        Trace.trace(e_errors.INFO, message)
        os.kill(pid, signal.SIGTERM)
        time.sleep(1)
        p, _ = os.waitpid(pid, os.WNOHANG)
        if p == 0:
            message = "kill -9ing %d => %s" % (pid, cmd)
            print timeofday.tod(), message
            Trace.trace(e_errors.INFO, message)
            os.kill(pid, signal.SIGKILL)
            time.sleep(2)
            os.waitpid(pid, os.WNOHANG)


#########################################################################
#
# AML2 robot loader server
#
#########################################################################
class AML2_MediaLoader(MediaLoaderMethods):

    def __init__(self, medch, max_work=7, csc_local=None):
        # noinspection PyGlobalUndefined
        global aml2

        MediaLoaderMethods.__init__(self, medch, max_work, csc_local)

        try:
            import aml2
        except ImportError:
            message = "Unable to load ACI library.  Exiting."
            Trace.log(e_errors.ERROR, message)
            sys.stderr.write("%s\n" % message)
            sys.exit(1)

        # robot choices are 'R1', 'R2' or 'Both'
        if 'RobotArm' in self.mc_config:  # error if robot not in config
            self.robotArm = string.strip(self.mc_config['RobotArm'])
        else:
            Trace.log(e_errors.ERROR, "ERROR:mc:aml2 no robot arm key in configuration")
            self.robotArm = string.strip(self.mc_config['RobotArm'])  # force the exception
            return

        if 'IOBoxMedia' in self.mc_config:  # error if IO box media assignments not in config
            self.mediaIOassign = self.mc_config['IOBoxMedia']
        else:
            Trace.log(e_errors.ERROR, "ERROR:mc:aml2 no IO box media assignments in configuration")
            self.mediaIOassign = self.mc_config['IOBoxMedia']  # force the exception
            return

        if 'DriveCleanTime' in self.mc_config:  # error if DriveCleanTime assignments not in config
            self.driveCleanTime = self.mc_config['DriveCleanTime']
        else:
            Trace.log(e_errors.ERROR, "ERROR:mc:aml2 no DriveCleanTime assignments in configuration")
            self.driveCleanTime = self.mc_config['DriveCleanTime']  # force the exception
            return

        if 'IdleTimeHome' in self.mc_config:
            if (isinstance(self.mc_config['IdleTimeHome'], types.IntType) and
                    self.mc_config['IdleTimeHome'] > 20):
                self.idleTimeLimit = self.mc_config['IdleTimeHome']
            else:
                Trace.log(e_errors.INFO, "mc:aml2 IdleHomeTime is not defined or too small, default used")
        self.prepare = self.unload

    # retry function call
    def retry_function(self, function, *args):
        count = self.getNretry()
        rpc_errors = 0
        sts = ("", 0, "")
        while count > 0 and sts[0] != e_errors.OK:
            try:
                sts = apply(function, args)
                if sts[1] != 0:
                    if self.logdetail:
                        Trace.log(e_errors.ERROR,
                                  'retry_function: function %s %s error %s' % (repr(function), args, sts[2]))
                if sts[1] == 1 and rpc_errors < 2:  # RPC failure
                    time.sleep(10)
                    rpc_errors = rpc_errors + 1
                elif (sts[1] == 5 or  # requested drive in use
                      sts[1] == 8 or  # DAS was unable to communicate with AMU
                      sts[1] == 10 or  # AMU was unable to communicate with robot
                      # sts[1] == 34 or        # The aci request timed out
                      sts[1] == 24):  # requested volume in use
                    count = count - 1
                    time.sleep(20)
                elif sts[1] == e_errors.MC_VOLNOTHOME:  # tape not in home position
                    count = count - 1
                    time.sleep(120)
                else:
                    break
            except:
                _, val, _ = Trace.handle_error()
                return "ERROR", 37, str(val)  # XXX very ad-hoc!
                ## this is "command error" in aml2.py
        return sts

    #########################################################################
    # These functions are overridden from the generic class.
    #########################################################################

    # load volume into the drive;
    def load(self, ticket):
        drive = ticket['drive_id']
        external_label = ticket['vol_ticket']['external_label']
        media_type = ticket['vol_ticket']['media_type']
        return self.retry_function(aml2.mount, external_label,
                                   drive, media_type)

    # unload volume from the drive
    def unload(self, ticket):
        drive = ticket['drive_id']
        external_label = ticket['vol_ticket']['external_label']
        media_type = ticket['vol_ticket']['media_type']
        return self.retry_function(aml2.dismount, external_label,
                                   drive, media_type)

    def insert(self, ticket):
        self.insertRA = None
        class_ticket = {'mcSelf': self}
        ticket['timeOfCmd'] = time.time()
        ticket['medIOassign'] = self.mediaIOassign
        return self.retry_function(aml2.insert, ticket, class_ticket)

    def eject(self, ticket):
        class_ticket = {'mcSelf': self}
        ticket['medIOassign'] = self.mediaIOassign
        return self.retry_function(aml2.eject, ticket, class_ticket)

    def robotHomeAndRestart(self, ticket):
        class_ticket = {'mcSelf': self}
        ticket['robotArm'] = self.robotArm
        return self.retry_function(aml2.robotHomeAndRestart,
                                   ticket, class_ticket)

    def getVolState(self, ticket):
        external_label = ticket['external_label']
        media_type = ticket['media_type']
        stat, vol_state = aml2.view(external_label, media_type)
        state = 'U'  # unknown
        if stat != 0:
            # return 'BAD', stat, 'aci_view return code', state
            return aml2.convert_status(stat)
        if vol_state is None:
            return 'BAD', stat, 'volume %s not found' % (external_label,), state
        # Return the correct media type.
        ticket['media_type'] = aml2.media_names.get(vol_state.media_type,
                                                    "unknown")
        return e_errors.OK, 0, "", vol_state.attrib

    def cleanCycle(self, in_ticket):
        __pychecker__ = "unusednames=i"

        # do drive cleaning cycle
        Trace.log(e_errors.INFO, 'mc:aml2 ticket=' + repr(in_ticket))
        # classTicket = { 'mcSelf' : self }
        try:
            drive = in_ticket['moverConfig']['mc_device']
        except KeyError:
            Trace.log(e_errors.ERROR, 'mc:aml2 no device field found in ticket.')
            status = 37
            return e_errors.DOESNOTEXIST, status, "no device field found in ticket"

        drive_type = drive[:2]  # ... need device type, not actual device
        try:
            if self.driveCleanTime:
                clean_time = self.driveCleanTime[drive_type][0]  # clean time in seconds
                drive_clean_cycles = self.driveCleanTime[drive_type][1]  # number of cleaning cycles
            else:
                clean_time = 60
                drive_clean_cycles = 1
        except KeyError:
            clean_time = 60
            drive_clean_cycles = 1

        vcc = volume_clerk_client.VolumeClerkClient(self.csc)
        min_remaining_bytes = 1
        vol_veto_list = []
        first_found = 0
        library_managers = in_ticket['moverConfig']['library']
        if isinstance(library_managers, types.StringType):
            lm = library_managers
            library = string.split(library_managers, ".")[0]
        elif isinstance(library_managers, types.ListType):
            lm = library_managers[0]
            library = string.split(library_managers[0], ".")[0]
        else:
            Trace.log(e_errors.ERROR, 'mc:aml2 library_manager field not found in ticket.')
            status = 37
            return e_errors.DOESNOTEXIST, status, "no library_manager field found in ticket"
        lm_info = self.csc.get(lm)
        if 'CleanTapeVolumeFamily' not in lm_info:
            Trace.log(e_errors.ERROR, 'mc: no CleanTapeVolumeFamily field found in ticket.')
            status = 37
            return e_errors.DOESNOTEXIST, status, "no CleanTapeVolumeFamily field found in ticket"
        clean_tape_volume_family = lm_info['CleanTapeVolumeFamily']
        v = vcc.next_write_volume(library,
                                  min_remaining_bytes, clean_tape_volume_family,
                                  vol_veto_list, first_found, exact_match=1)  # get which volume to use
        if v["status"][0] != e_errors.OK:
            Trace.log(e_errors.ERROR, "error getting cleaning volume:%s %s" %
                      (v["status"][0], v["status"][1]))
            return v["status"][0], 0, v["status"][1]

        for i in range(drive_clean_cycles):
            Trace.log(e_errors.INFO, "AML2 clean drive %s, vol. %s" % (drive, v['external_label']))
            # rt = self.load(v['external_label'], drive, v['media_type'])
            rt = self.retry_function(aml2.mount, v['external_label'],
                                     drive, v['media_type'])
            status = rt[1]
            if status != 0:  # mount returned error
                s1, s2, s3 = self.retry_function(aml2.convert_status, status)
                return s1, s2, s3

            time.sleep(clean_time)  # wait clean_time seconds
            # rt = self.unload(v['external_label'], drive, v['media_type'])
            rt = self.retry_function(aml2.dismount, v['external_label'],
                                     drive, v['media_type'])
            status = rt[1]
            if status != 0:  # dismount returned error
                s1, s2, s3 = self.retry_function(aml2.convert_status, status)
                return s1, s2, s3
            Trace.log(e_errors.INFO, "AML2 Clean returned %s" % (rt,))

        ret_ticket = vcc.get_remaining_bytes(v['external_label'])
        remaining_bytes = ret_ticket['remaining_bytes'] - 1
        vcc.set_remaining_bytes(v['external_label'], remaining_bytes, '\0', None)
        return e_errors.OK, 0, None

    def doWaitingInserts(self):
        # do delayed insertvols
        if self.workQueueClosed and len(self.work_list) == 0:
            self.workQueueClosed = 0
            ticket = {'function': 'insert',
                      'timeOfCmd': self.timeInsert,
                      'r_a': self.insertRA}
            self.DoWork(self.insert, ticket)
        return e_errors.OK, 0, None

    def query_robot(self, ticket):
        __pychecker__ = "no-argsused"

        # Name of the aci library function.
        command = "aci_robstat"

        t0 = time.time()
        status, status_code, response = self.robotStatus(self.robotArm)
        delta = time.time() - t0

        # got response, parse it and put it into the standard form
        if not e_errors.is_ok(status[0]):
            e = 19  # 19 = ???
            stat_msg = "robot status %i: %s => %i,%s, %f" % \
                       (e, command, status_code, response, delta)
            Trace.log(e_errors.ERROR, stat_msg)
            return status, e, response, "", stat_msg

        stat_msg = "%s => %i,%s, %f" % (command, status_code, response, delta)
        Trace.log(e_errors.INFO, stat_msg)
        return e_errors.OK, 0, stat_msg, "", ""

    def getDriveState(self, ticket):
        drive = ticket['drive']
        stat, drivestate = aml2.drive_state(drive)

        state = 'N'  # unknown
        if stat != 0:
            # return 'BAD', stat, "aci_drivestatus2 return code", state
            return aml2.convert_status(stat)
        if drivestate is None:
            return 'BAD', stat, "drive %s not found" % (drive,), state

        if drivestate.drive_state == aml2.ACI_DRIVE_UP:
            state = "U"  # UP
        elif drivestate.drive_state == aml2.ACI_DRIVE_DOWN:
            state = "D"  # DOWN

        # Update the ticket with additional information.
        drive_info = {
            'state': aml2.drive_state_names.get(drivestate.drive_state, "unknown"),
            'type': aml2.drive_names.get(str(drivestate.drive_state), "unknown"),
            'status': 0,
            'volume': drivestate.volser
        }
        ticket['drive_info'] = drive_info
        return (e_errors.OK, 0, drivestate.volser, "%s %d" %
                (state, drivestate.drive_state))

    def listDrives(self, ticket):
        stat, drives = aml2.drives_states()
        if stat != 0:
            # ticket['status'] = 'BAD', stat, "aci_drivestatus2 return code"
            ticket['status'] = aml2.convert_status(stat)
            drives = []  # To avoid TypeErrors.
        else:
            ticket['status'] = (e_errors.OK, 0, "")

        drive_list = []
        for drive in drives:
            use_state = aml2.drive_state_names.get(drive.drive_state,
                                                   drive.drive_state)
            use_type = aml2.drive_names.get(drive.type, drive.type)
            ##################################################
            # The aml2 is not very good at knowing the difference between
            # an LTO1 and LTO2 drive.  Ask the mover for the correct
            # drive type.
            movers = self.csc.get_movers2(3, 2)
            for mover in movers:
                if mover['mc_device'] == drive.drive_name:
                    import mover_client
                    flags = enstore_constants.NO_LOG | enstore_constants.NO_ALARM
                    mc_local = mover_client.MoverClient(self.csc,
                                                        mover['name'],
                                                        flags=flags,
                                                        rcv_timeout=3,
                                                        rcv_tries=2)
                    status = mc_local.status(3, 2)  # Get the status.
                    del mc_local  # Hope this helps with resource leaks!
                    if e_errors.is_ok(status['status'][0]):
                        # use_type = "%s=%s" % (status['drive_id'],
                        #                          use_type)
                        use_type = status['drive_id']
                    # Regardless of an error or not, we found the
                    # mover we were looking for.  Give up.
                    break
            ### Without an explicit collection here, even the "del mc" above
            ### does not work until python runs the garbage collector.
            ### If python takes to long we run out of FDs, so force python
            ### to reclaim those resources.
            import gc
            gc.collect()
            ##################################################

            if drive.volser:
                use_status = "mounted"
            else:
                use_status = EMPTY

            drive_list.append({"name": drive.drive_name,
                               "state": use_state,
                               "status": use_status,  # Filler for AML2.
                               "volume": drive.volser,
                               "type": use_type,
                               })

        ticket['drive_list'] = drive_list
        return e_errors.OK, 0, None

    def listVolumes(self, ticket):

        if not hostaddr.allow(ticket['callback_addr']):
            return

        # We modify it if not Okay.
        ticket['status'] = (e_errors.OK, 0, None, "", "")

        stat, volumes = aml2.list_volser()
        if stat != 0:
            ticket['status'] = aml2.convert_status(stat)
        else:
            ticket['status'] = (e_errors.OK, 0, None, "", "")

        volume_list = []
        for volume in volumes:
            use_media_type = aml2.media_names.get(volume.media_type, "UNKNOWN")

            volume_list.append({'volume': volume.volser,
                                'type': use_media_type,
                                'state': volume.attrib,
                                'location': "",
                                })
        reply = copy.copy(ticket)
        self.reply_to_caller(reply)
        ticket['no_reply'] = 1  # Tell WorkDone() not to send the ticket again.
        reply = copy.copy(ticket)
        reply['volume_list'] = volume_list
        address_family = socket.getaddrinfo(ticket['callback_addr'][0], None)[0][0]
        sock = socket.socket(address_family, socket.SOCK_STREAM)
        try:
            sock.connect(ticket['callback_addr'])
            r = callback.write_tcp_obj(sock, reply)
            sock.close()
            if r:
                message = "Error calling write_tcp_obj. Callback addr. %s" \
                          % (ticket['callback_addr'],)
                Trace.log(e_errors.ERROR, message)
                e = 6
                ticket['status'] = (e_errors.NET_ERROR, e, message)
        except:
            Trace.handle_error()
            Trace.log(e_errors.ERROR,
                      "Callback address %s" % (ticket['callback_addr'],))
            e = 6
            ticket['status'] = (e_errors.NET_ERROR, e, sys.exc_info()[2])

        return ticket['status']

    def listVolumes2(self, ticket):
        ticket['work'] = "list_volumes"  # Use old method for AML2.
        ticket['function'] = "listVolume"
        return self.listVolumes(ticket)

    def listSlots(self, ticket):
        # A bug in aci_getcellinfo() requires forking in list_slots().
        # If we are the parent, just return and keep on going.  This isn't
        # the best solution because we loose keeping the reply in the
        # udp_server in case the client didn't receive the reply.  If
        # a better solution is needed, then look at timed_command() in the
        # STK implementation.
        #
        # The bug is that for every call to aci_getcellinfo() three file
        # descriptors (that are sockets) are leaked.
        #
        # By using self.fork() instead of os.fork() we get automatic process
        # tracking and termination (if needed).
        #
        # pid = self.fork()
        # if pid != 0: # parent
        #    return

        # ... else this is the child.

        ### All this extra forking code to work around aci_getcellinfo()
        ### is not needed now that list_slots() uses DoWork() to call
        ### listSlots.  An implicit fork() is called in DoWork() for us.

        stat, slots = aml2.list_slots()
        if stat != 0:
            ticket['status'] = aml2.convert_status(stat)
        else:
            ticket['status'] = (e_errors.OK, 0, "")

        slot_list = []
        for slot_info in slots:
            # location = slot_info[0]

            for i in range(len(slot_info[1])):
                media_type = slot_info[1][i].eMediaType
                use_media_type = aml2.media_names.get(media_type,
                                                      media_type)
                slot_dict = {"location": slot_info[0],
                             "media_type": use_media_type,
                             }
                try:
                    slot_dict['total'] = slot_info[1][i].ulCount
                except IndexError:
                    slot_dict['total'] = 0
                try:
                    slot_dict['free'] = slot_info[2][i].ulCount
                except IndexError:
                    slot_dict['free'] = 0
                try:
                    slot_dict['used'] = slot_info[3][i].ulCount
                except IndexError:
                    slot_dict['used'] = 0
                try:
                    slot_dict['disabled'] = slot_info[4][i].ulCount
                except IndexError:
                    slot_dict['disabled'] = 0

                slot_list.append(slot_dict)

        ticket['slot_list'] = slot_list
        return e_errors.OK, 0, None

        # sys.exit(0)  #Remember we are the child here.

    def listClean(self, ticket):

        stat, volumes_list = aml2.list_volser()
        if stat != 0:
            ticket['status'] = aml2.convert_status(stat)
        else:
            ticket['status'] = (e_errors.OK, 0, "")

        vcc = volume_clerk_client.VolumeClerkClient(self.csc,
                                                    logc=self.logc,
                                                    alarmc=self.alarmc,
                                                    rcv_timeout=5,
                                                    rcv_tries=12)

        clean_list = []
        for volume_instance in volumes_list:
            volume = volume_instance.volser
            use_media_type = aml2.media_names.get(volume_instance.media_type,
                                                  "UNKNOWN")

            if volume[0:2] != "CL":
                #############################################
                # Assuming cleaning tapes begin with CL is an unfortunate
                # part of this implementation.
                #############################################
                continue

            vol_info = vcc.inquire_vol(volume, timeout=5, retry=12)
            if e_errors.is_ok(vol_info):
                location = "N/A"
                max_usage = "N/A"
                current_usage = "N/A"
                remaining_usage = vol_info['remaining_bytes']
                status = "N/A"
                # media_type = vol_info['media_type']
            else:
                location = "N/A"
                max_usage = "N/A"
                current_usage = "N/A"
                remaining_usage = "Unknown"
                status = "N/A"
                # media_type = "Unknown"

            clean_list.append({"volume": volume,
                               "location": location,
                               "max_usage": max_usage,
                               "current_usage": current_usage,
                               "remaining_usage": remaining_usage,
                               "status": status,
                               "type": use_media_type,
                               })

        ticket['status'] = (e_errors.OK, 0, None)
        reply = copy.copy(ticket)
        self.reply_to_caller(reply)
        ticket['no_reply'] = 1  # Tell WorkDone() not to send the ticket again.
        reply = copy.copy(ticket)
        reply['clean_list'] = clean_list
        address_family = socket.getaddrinfo(ticket['callback_addr'][0], None)[0][0]
        sock = socket.socket(address_family, socket.SOCK_STREAM)
        try:
            sock.connect(ticket['callback_addr'])
            r = callback.write_tcp_obj(sock, reply)
            sock.close()
            if r:
                Trace.log(e_errors.ERROR,
                          "Error calling write_tcp_obj. Callback addr. %s"
                          % (ticket['callback_addr'],))

        except:
            Trace.handle_error()
            Trace.log(e_errors.ERROR,
                      "Callback address %s" % (ticket['callback_addr'],))

        return e_errors.OK, 0, None

    #########################################################################
    # These functions are internal functions specific to AML2 media changer.
    #########################################################################

    def robotStatus(self, arm):
        return self.retry_function(aml2.robotStatus, arm)


#########################################################################
#
# STK robot loader server
#
#########################################################################
class STK_MediaLoader(MediaLoaderMethods):

    def __init__(self, medch, max_work=7, local_csc=None):
        MediaLoaderMethods.__init__(self, medch, max_work, local_csc)

        self.acls_host = self.mc_config.get('acls_host', 'UNKNOWN')
        self.acls_uname = self.mc_config.get('acls_uname', 'UNKNOWN')
        self.driveCleanTime = self.mc_config.get('DriveCleanTime',
                                                 {'9840': [60, 1], '9940': [60, 1]})
        self.acssa_version = self.mc_config.get('acssa_version', 'UNKNOWN')
        self.prepare = self.unload
        self.DEBUG = 0
        print "STK MediaLoader initialized"

    # retry function call
    def retry_function(self, function, *args):
        count = self.getNretry()
        sts = ("", 0, "")
        # retry every error
        while count > 0 and sts[0] != e_errors.OK:
            try:
                sts = apply(function, args)
                if sts[1] == 6:  # no record for display_drive
                    break
                if sts[1] != 0:
                    if self.logdetail:
                        Trace.log(e_errors.ERROR, 'retry_function: function %s  %s  sts[1] %s  sts[2] %s  count %s' % (
                                  repr(function), args, sts[1], sts[2], count))
                    if function == self.mount:
                        if sts[1] == e_errors.MC_VOLNOTHOME:
                            # Volume is mounted in another drive.
                            # break loop here
                            break
                        time.sleep(60)
                        fixsts = apply(self.dismount, args)
                        Trace.log(e_errors.INFO, 'Tried %s %s  status=%s %s  Desperation dismount  status %s %s' % (
                                  repr(function), args, sts[1], sts[2], fixsts[1], fixsts[2]))
                    time.sleep(60)
                    count = count - 1
                else:
                    break
            except:
                e, _, _ = Trace.handle_error()
                return str(e), 0, ""
        return sts

    # execute a stk cmd_proc command, but don't wait forever for it to complete
    # mostly stolen from Demo/tkinter/guido/ShellWindow.py - spawn function
    def timed_command(self, cmd, min_response_length=0, timeout=60):

        message = ""
        blanks = 0
        nread = 0

        now = timeofday.tod()
        p2cread, p2cwrite = os.pipe()
        c2pread, c2pwrite = os.pipe()
        command = "(echo %s;echo logoff)|/export/home/ACSSS/bin/cmd_proc 2>&1" % (cmd,)
        if self.acssa_version == '8.4':
            command = "(echo %s)|/export/home/ACSSS/bin/cmd_proc 2>&1" % (cmd,)
        Trace.trace(e_errors.INFO, "Sending %s" % (command,))
        cmd_lookfor = "ACSSA> %s" % (cmd,)

        # can not use dispatching work fork because we are already child.
        # need to kill explicitly and children can't kill
        (dum, mark) = self.delta_t(0)
        pid = os.fork()

        if pid == 0:
            # Child
            for i in 0, 1, 2:
                try:
                    os.close(i)
                except os.error:
                    pass
            if os.dup(p2cread) != 0:
                print 'ERROR: timed_command pc2cread bad read dup'
                Trace.log(e_errors.ERROR, 'timed_command pc2cread bad read dup')
            if os.dup(c2pwrite) != 1:
                print 'ERROR: timed_command c2pwrite bad write dup'
                Trace.log(e_errors.ERROR, 'timed_command c2pwrite bad write dup')
            if os.dup(c2pwrite) != 2:
                print 'ERROR: timed_command c2pwrite bad error dup'
                Trace.log(e_errors.ERROR, 'timed_command c2pwrite bad error dup')
            maxfd = 100  # Max number of file descriptors (os.getdtablesize()???)
            for i in range(3, maxfd):
                try:
                    os.close(i)
                except:
                    pass
            try:
                # I know this is hard-coded and inflexible. That is what I want in order to
                # prevent any possible security problem.

                os.execv('/usr/bin/rsh', [self.acls_host, '-l', self.acls_uname, command])
            finally:
                e, e_msg, e_tb = sys.exc_info()
                Trace.log(e_errors.ERROR, "timed_command execv failed:  %s %s %s" %
                          (e, e_msg, traceback.format_tb(e_tb)))
                # noinspection PyProtectedMember
                os._exit(1)

        os.close(p2cread)
        os.close(c2pwrite)
        os.close(p2cwrite)

        # wait for child to complete, or kill it
        start = time.time()
        if self.DEBUG:
            print timeofday.tod(), cmd
            Trace.trace(e_errors.INFO, "%s" % (cmd,))
        active = 0
        try:
            while active < timeout:
                p, _ = os.waitpid(pid, os.WNOHANG)
                if p != 0:
                    # When we detect that the process has exited, leave
                    # the loop.  This allows us to avoid the ETIMEDOUT
                    # and re-raised select errors.
                    break
                # We need to start reading this now for really long responses.
                # Otherwise, the buffer fills up with the child waiting
                # for the parent to read something from the full buffer.
                # And the parent waits for the child to finish.
                wait_duration = max(timeout - active, 0)
                try:
                    r, w, x = select.select([c2pread], [], [], wait_duration)
                except (select.error, OSError, IOError), msg_e:
                    Trace.log(79, "select error in timed_command(): %s" %
                              (str(msg_e),))
                    if msg_e.args[0] in [errno.EINTR]:
                        # The process was interrupted by a signal; we need
                        # to keep it going.
                        active = time.time() - start
                        continue
                    else:
                        # We want to jump to the error handling code.
                        raise sys.exc_info()[0], sys.exc_info()[1], \
                            sys.exc_info()[2]

                # If nothing was received, we want to wait again instead of
                # falling into the os.read().  If the robot side hangs
                # without closing the pipe we can time out in select(), but
                # not read().
                if c2pread not in r:
                    active = time.time() - start
                    time.sleep(1)
                    continue
                raw_msg = os.read(c2pread, 2000)
                if raw_msg:
                    if self.DEBUG:
                        print raw_msg,
                    message = message + raw_msg
                    # Need to reset the timeout period.
                    start = time.time()
                    active = 0
                else:
                    if raw_msg == '':
                        blanks = blanks + 1
                    active = time.time() - start
                    time.sleep(1)
            else:
                # We want to jump to the error handling code.
                raise select.error(errno.ETIMEDOUT, None)

        except (KeyboardInterrupt, SystemExit):
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except:
            # Log the original error.
            e, msg_e, e_tb = sys.exc_info()
            message = "timed_command wait for child failed doing: %s" % (cmd,)
            Trace.log(e_errors.ERROR, message)
            Trace.handle_error(e, msg_e, e_tb)
            del e_tb  # avoid cyclic references

            # Make sure to clean up after ourselves, so kill the forked process.
            try:
                self.kill_it(pid, cmd)
            except (KeyboardInterrupt, SystemExit):
                raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
            except:
                message = "Failed to kill %d: %s: %s" % (pid, cmd,
                                                         sys.exc_info()[1])
                Trace.log(e_errors.ERROR, message)

            # Close the socket and return.
            os.close(c2pread)
            return -1, [], self.delta_t(mark)[0]

        if p == 0:
            os.close(c2pread)
            return -2, [], self.delta_t(mark)[0]

        # now read response from the pipe (Some of
        if string.find(cmd, 'mount') != -1:  # this is a mount or a dismount command
            maxread = 100  # quick response on queries
        else:
            maxread = 10000  # slow response on mount/dismounts

        nlines = 0
        ntries = 0
        jonflag = 0
        # async message start with a date:  2001-12-20 07:33:17     0    Drive   0, 0,10,12: Cleaned.
        # unfortunately, not just async messages start with a date.  Alas, each message has to be parsed.
        async_date = re.compile(r'20\d\d-\d\d-\d\d \d\d:\d\d:\d\d')
        response = []
        while nlines < 4 and ntries < 3:
            ntries = ntries + 1
            # while blanks<2 and nread<maxread:
            while nread < maxread:
                # We should not need to use select() here, since the sending
                # process is known to have exited, if there is no more data
                # we know we have it all.
                raw_msg = os.read(c2pread, 2000)
                message = message + raw_msg
                if raw_msg:
                    if self.DEBUG:
                        print raw_msg,
                nread = nread + 1
                if raw_msg == '':
                    blanks = blanks + 1

            response = []
            resp = string.split(message, '\012')
            nl = 0
            for line in resp:
                if async_date.match(line):
                    if string.find(line, 'Place cartridges in CAP') != -1 or \
                            string.find(line, 'Remove cartridges from CAP') != -1 or \
                            string.find(line, 'Library error, LSM offline') != -1 or \
                            string.find(line, 'Library error, Transport failure') != -1 or \
                            string.find(line, 'Library error, LMU failure') != -1 or \
                            string.find(line, 'LMU Recovery Complete') != -1 or \
                            string.find(line, ': Offline.') != -1 or \
                            string.find(line, ': Online.') != -1 or \
                            string.find(line, ': Enter operation ') != -1 or \
                            string.find(line, 'Clean drive') != -1 or \
                            string.find(line, 'Cleaned') != -1:
                        if self.DEBUG:
                            print "DELETED:", line
                        jonflag = 1
                        continue
                if self.DEBUG:
                    print "response line =", nl, line
                response.append(l)
                nl = nl + 1
            nlines = len(response)

            nl = 0
            if jonflag and self.DEBUG:
                for line in response:
                    print "parsed lines =", nl, line
                    nl = nl + 1

        os.close(c2pread)
        size = len(response)

        status = 0
        look = 0
        while look in range(0, size):  # 1st part of response is STK copyright information
            if string.find(response[look], cmd_lookfor, 0) == 0:
                break
        if size != 0 and look == size: # FIXME: Never true!
            status = -4
            look = 0
        else:
            if len(response[look:]) < min_response_length:
                status = -5
        if self.DEBUG:
            rightnow = timeofday.tod()  # the times on fntt are not necessarily right, allows us to correlate log time
            rsp = [now, response[look:], rightnow]
            pprint.pprint(rsp)

        return status, response[look:], self.delta_t(mark)[0]

    #########################################################################
    # These functions are overridden from the generic class.
    #########################################################################

    # load volume into the drive;
    def load(self, ticket):
        drive = ticket['drive_id']
        external_label = ticket['vol_ticket']['external_label']
        media_type = ticket['vol_ticket'].get('media_type', "")
        return self.retry_function(self.mount, external_label,
                                   drive, media_type, ticket)

    # unload volume from the drive
    def unload(self, ticket):
        drive = ticket['drive_id']
        external_label = ticket['vol_ticket']['external_label']
        media_type = ticket['vol_ticket'].get('media_type', "")
        return self.retry_function(self.dismount, external_label,
                                   drive, media_type)

    def getDriveState(self, ticket):
        drive = ticket['drive']
        rt = self.retry_function(self.query_drive, drive)
        # Update the ticket with additional information.
        drive_info = {
            'state': rt[2][len(drive):len(drive) + 16].strip(),
            'status': rt[2][len(drive) + 16:len(drive) + 16 + 11].strip(),
            'volume': rt[2][len(drive) + 16 + 11:len(drive) + 16 + 11 + 11].strip(),
            'type': rt[2][len(drive) + 16 + 11 + 11:].strip()
        }
        ticket['drive_info'] = drive_info
        return rt[0], rt[1], rt[3], rt[4]

    def displayDrive(self, ticket):
        """
        Used to get drive WWN
        """
        drive = ticket['drive']
        rt = self.retry_function(self.display_drive, drive)
        # Update the ticket with additional information.
        drive_info = {}
        if e_errors.is_ok(rt[0]):
            drive_info['drive'], drive_info['Wwn'] = rt[2].split()
            ticket['drive_info'] = drive_info
        return rt[0], rt[1], rt[3], rt[4]

    # FIXME - what the devil is this?
    def getVolState(self, ticket):
        external_label = ticket['external_label']
        media_type = ticket['media_type']
        rt = self.retry_function(self.query, external_label, media_type)
        Trace.trace(11, "getVolState returned %s" % (rt,))
        if rt[3] == '\000':
            state = ''
        else:
            state = rt[3]
            if not state and rt[2]:  # volumes not in the robot
                state = rt[2]
        # Return the correct media type.
        try:
            ticket['media_type'] = rt[2].split()[-1]
        except (IndexError, ValueError, TypeError, AttributeError):
            pass
        # Return the location in a consistent fashion.  This removes leading
        # trailing and inserted space characters.
        try:
            ticket['location'] = rt[2][29:51].strip().replace(" ", "")
        except (IndexError, ValueError, TypeError, AttributeError):
            pass

        return rt[0], rt[1], rt[2], state

    def insert(self, ticket):
        __pychecker__ = "no-argsused"  # When fixed remove this pychecker line.
        return (e_errors.NOT_SUPPORTED, 0,
                "STK media changer does not support this operation.")

    def eject(self, ticket):
        __pychecker__ = "no-argsused"  # When fixed remove this pychecker line.
        return (e_errors.NOT_SUPPORTED, 0,
                "STK media changer does not support this operation.")

    def cleanCycle(self, in_ticket):
        __pychecker__ = "unusednames=i"

        # do drive cleaning cycle
        Trace.log(e_errors.INFO, 'mc:ticket=' + repr(in_ticket))
        # classTicket = { 'mcSelf' : self }
        try:
            drive = in_ticket['moverConfig']['mc_device']
        except KeyError:
            Trace.log(e_errors.ERROR, 'mc:no device field found in ticket.')
            status = 37
            return e_errors.DOESNOTEXIST, status, "no device field found in ticket"

        drive_type = drive[:2]  # ... need device type, not actual device
        try:
            if self.driveCleanTime:
                clean_time = self.driveCleanTime[drive_type][0]  # clean time in seconds
                drive_clean_cycles = self.driveCleanTime[drive_type][1]  # number of cleaning cycles
            else:
                clean_time = 60
                drive_clean_cycles = 1
        except KeyError:
            clean_time = 60
            drive_clean_cycles = 1

        vcc = volume_clerk_client.VolumeClerkClient(self.csc)
        min_remaining_bytes = 1
        vol_veto_list = []
        first_found = 0
        library_managers = in_ticket['moverConfig']['library']
        if isinstance(library_managers, types.StringType):
            lm = library_managers
            library = string.split(library_managers, ".")[0]
        elif isinstance(library_managers, types.ListType):
            lm = library_managers[0]
            library = string.split(library_managers[0], ".")[0]
        else:
            Trace.log(e_errors.ERROR, 'mc: library_manager field not found in ticket.')
            status = 37
            return e_errors.DOESNOTEXIST, status, "no library_manager field found in ticket"
        lm_info = self.csc.get(lm)
        if 'CleanTapeVolumeFamily' not in lm_info:
            Trace.log(e_errors.ERROR, 'mc: no CleanTapeVolumeFamily field found in ticket.')
            status = 37
            return e_errors.DOESNOTEXIST, status, "no CleanTapeVolumeFamily field found in ticket"
        clean_tape_volume_family = lm_info['CleanTapeVolumeFamily']
        v = vcc.next_write_volume(library,
                                  min_remaining_bytes, clean_tape_volume_family,
                                  vol_veto_list, first_found, exact_match=1)  # get which volume to use
        if v["status"][0] != e_errors.OK:
            Trace.log(e_errors.ERROR, "error getting cleaning volume:%s %s" %
                      (v["status"][0], v["status"][1]))
            return v["status"][0], 0, v["status"][1]

        for i in range(drive_clean_cycles):
            Trace.log(e_errors.INFO, "STK clean drive %s, vol. %s" % (drive, v['external_label']))
            # rt = self.load(v['external_label'], drive, v['media_type'])
            rt = self.retry_function(self.mount, v['external_label'],
                                     drive, v['media_type'])
            status = rt[0]
            if status != e_errors.OK:  # mount returned error
                return status, 0, None

            time.sleep(clean_time)  # wait clean_time seconds
            # rt = self.unload(v['external_label'], drive, v['media_type'])
            rt = self.retry_function(self.dismount, v['external_label'],
                                     drive, v['media_type'])
            status = rt[0]
            if status != e_errors.OK:
                return status, 0, None
            Trace.log(e_errors.INFO, "STK Clean returned %s" % (rt,))

        ret_ticket = vcc.get_remaining_bytes(v['external_label'])
        remaining_bytes = ret_ticket['remaining_bytes'] - 1
        vcc.set_remaining_bytes(v['external_label'], remaining_bytes, '\0', None)
        return e_errors.OK, 0, None

    def query_robot(self, ticket):
        __pychecker__ = "no-argsused"

        # build the command, and what to look for in the response
        command = "query server"
        answer_lookfor = "run"

        # execute the command and read the response
        # efb (dec 22, 2005) - up timeout from 10 to 60 as the queries are hanging
        # status,response,delta = self.timed_command(command,5,10)
        status, response, delta = self.timed_command(command, 5, 60)
        if status != 0:
            e = 18
            message = "query server %i: %s => %i,%s" % \
                      (e, command, status, response)
            Trace.log(e_errors.ERROR, message)
            return "ERROR", e, response, "", message

        # got response, parse it and put it into the standard form
        answer = string.strip(response[4])
        if string.find(answer, answer_lookfor, 0) != 0:
            e = 19
            message = "query server %i: %s => %i,%s, %f" % \
                      (e, command, status, response, delta)
            Trace.log(e_errors.ERROR, message)
            return "ERROR", e, response, "", message
        message = "%s => %i,%s, %f" % (command, status, answer[0:17], delta)
        Trace.log(e_errors.INFO, message)
        return e_errors.OK, 0, message, "", ""

    query_server = query_robot  # Backward compatibility. (Still needed?)

    def listDrives(self, ticket):

        # build the command, and what to look for in the response
        command = "query drive ALL"
        # answer_lookfor = "query drive ALL"

        # execute the command and read the response
        # FIXME - what if this hangs?
        # efb (dec 22, 2005) - up timeout from 10 to 60 as the queries are hanging
        # status,response, delta = self.timed_command(command,4,10)
        status, response, delta = self.timed_command(command, 4, 60)
        if status != 0:
            e = 4
            message = "QUERY_DRIVE %i: %s => %i,%s" % \
                      (e, command, status, response)
            Trace.log(e_errors.ERROR, message)
            ticket['status'] = ("ERROR", e, response, "", message)
            reply = copy.copy(ticket)
            self.reply_to_caller(reply)
            ticket['no_reply'] = 1  # Tell WorkDone() not to send the ticket again.
            return ticket['status']

        drive_list = []
        for line in response:
            if line[:2] != "  ":
                # This is some other information.
                continue

            name = line[2:13].strip()
            state = line[14:29].strip()
            status = line[30:41].strip()
            volume = line[42:52].strip()
            drive_type = line[53:].strip()

            drive_list.append({"name": name.replace(" ", ""),
                               "state": state,
                               "status": status,
                               "volume": volume,
                               "type": drive_type,
                               })

        ticket['drive_list'] = drive_list
        return e_errors.OK, 0, None, "", ""

    def listVolumes2(self, ticket):
        ticket['work'] = "list_volumes"  # Use old method for STK now too.
        ticket['function'] = "listVolumes"
        return self.listVolumes(ticket)

    def listVolumes(self, ticket):

        acsls_cmd = "query volume all"
        # acsls_look_for = "query volume all"

        command = "(echo %s;echo logoff)|/export/home/ACSSS/bin/cmd_proc 2>&1" \
                  % (acsls_cmd,)
        cmd_lookfor = "ACSSA> %s" % (acsls_cmd,)

        args = [self.acls_host, '-l', self.acls_uname, command]

        try:
            lv_proc = subprocess.Popen(args, executable="/usr/bin/rsh",
                                       stdin=None,
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.STDOUT,
                                       shell=False)
            ticket['status'] = (e_errors.OK, 0, None)
        except OSError:
            lv_proc = subprocess.Popen('true')
            ticket['status'] = (e_errors.OSERROR, 0, str(sys.exc_info()[1]))

        # Tell the client over udp that we are about to connect using TCP.
        reply = copy.copy(ticket)
        self.reply_to_caller(reply)
        if not e_errors.is_ok(ticket['status'][0]):
            return ticket['status']

        ticket['no_reply'] = 1  # Tell WorkDone() not to send the ticket again.
        reply = ticket.copy()  # Make a copy to keep things clean.  But why?
        address_family = socket.getaddrinfo(ticket['callback_addr'][0], None)[0][0]
        sock = socket.socket(address_family, socket.SOCK_STREAM)
        # Connect using TCP.
        try:
            sock.connect(ticket['callback_addr'])
        except socket.error as msg_e:
            Trace.handle_error()
            Trace.log(e_errors.ERROR,
                      "Callback address %s" % (ticket['callback_addr'],))

            # Close opened sockets.
            if lv_proc.stdout:
                lv_proc.stdout.close()
            return e_errors.NET_ERROR, 0, str(msg_e), "", ""

        # We need to skip the header here.  Normally, timed_command() will
        # skip this for us, but since we are not using it due to the
        # performance problems, we need to do this ourselves.
        try:
            line = -1
            while line:
                line = lv_proc.stdout.readline()
                if line.find(cmd_lookfor) > -1:
                    break
        except:
            # Close opened sockets.
            if lv_proc.stdout:
                lv_proc.stdout.close()
            sock.close()
            return

        line = -1
        volume_list = []
        while line:
            try:
                line = lv_proc.stdout.readline()

                if line.find("ACSSA") >= 0 or line.find("Volume Status") >= 0 \
                        or line.find("Identifier") >= 0 or len(line) == 0:
                    # This is some other information.
                    continue
                Trace.trace(21, "line %s" % (line,))
                # The returned line of interest looks like:
                # For ACSLS version 7:
                # TST102      ejected              1,-1,31, 7, 1         LTO-800G
                #
                # For ACSLS version 8:
                # PSC573              home              0, 1, 7, 0, 0         STK2P
                # get rid of extra whitespaces
                tline = ' '.join(line.translate(None, string.whitespace[:5]).split())
                # now it looks like:
                # PSC573 home 0, 1, 7, 0, 0 STK2P
                # get rid of space before number in address
                tline2 = tline.replace(", ", ",")
                # now it looks like:
                # PSC573 home 0,1,7,0,0 STK2P

                s_line = tline2.split(' ')
                volume = s_line[0]
                state = s_line[1]
                location = s_line[2]
                media_type = s_line[3]

                volume_list.append({"volume": volume,
                                    "state": state,
                                    "location": location,
                                    "type": media_type,
                                    })
            except:
                Trace.log(e_errors.ERROR,
                          "Skipping line while processing volumes list: %s"
                          % (line,))
                continue

        # Put the list of volumes into the reply ticket.
        reply['volume_list'] = volume_list
        reply['status'] = (e_errors.OK, 0, None)
        try:
            err = callback.write_tcp_obj(sock, reply)
            if err:
                Trace.log(e_errors.ERROR,
                          "Error calling write_tcp_obj. Callback addr. %s"
                          % (ticket['callback_addr'],))
        except:
            Trace.handle_error()
            Trace.log(e_errors.ERROR,
                      "Callback address %s" % (ticket['callback_addr'],))

            e = 6
            reply['status'] = (e_errors.NET_ERROR, e, str(sys.exc_info()[1]), "", "")

        # Don't forget to close the sockets and FIFOs.
        lv_proc.stdout.close()
        sock.close()
        return e_errors.OK, 0, None, "", ""

    def listSlots(self, ticket):
        # build the command, and what to look for in the response
        command = "query lsm all"

        # execute the command and read the response
        # FIXME - what if this hangs?
        # efb (dec 22, 2005) - up timeout from 10 to 60 as the queries are hanging
        # Obtain the list of movers so that we can build a list of valid
        # locations to report slots for.
        try:
            movers_list = self.csc.get_movers2(None, 5, 3)
        except KeyError:
            e = 20
            message = "Unable to obtain list of movers."
            Trace.log(e_errors.ERROR, message)
            ticket['status'] = (e_errors.NET_ERROR, e, "", "", message)
            reply = copy.copy(ticket)
            self.reply_to_caller(reply)
            ticket['no_reply'] = 1  # Tell WorkDone() not to send the ticket again.
            return ticket['status']

        # Build the list of ACSes that we have movers configured in.  ACSes
        # are the first number in the list like 0,0,1,27.
        #
        # FYI:  The second number is the LSM, the third is the cabinet and
        # the 4th is the position within the cabinet.
        valid_locations = []
        for mover_info in movers_list:
            if mover_info.get('media_changer', None) == self.name:
                valid_robot_location = \
                    mover_info['mc_device'].split(",")[0]
                if valid_robot_location not in valid_locations:
                    valid_locations.append(valid_robot_location)

        # status,response, delta = self.timed_command(command,4,10)
        status, response, delta = self.timed_command(command, 4, 60)
        if status != 0:
            e = 4
            message = "QUERY_SLOTS %i: %s => %i,%s" % \
                      (e, command, status, response)
            Trace.log(e_errors.ERROR, message)
            ticket['status'] = ("ERROR", e, response, "", message)
            reply = copy.copy(ticket)
            self.reply_to_caller(reply)
            ticket['no_reply'] = 1  # Tell WorkDone() not to send the ticket again.
            return ticket['status']

        slot_list = []
        for line in response:
            if line.find("ACSSA") >= 0 or line.find("LSM Status") >= 0 or \
                    line.find("Identifier") >= 0 or line.find("Count") >= 0 \
                    or len(line) == 0:
                # This is some other information.
                continue

            # Extract the LSM from the line of robot output.
            lsm = line[:13].strip().replace(" ", "")
            # Make sure the first number in the location, which is the ACS,
            # matches the list of valid ACSes in the valid_locations list.
            if lsm.split(",")[0] not in valid_locations:
                # This robot is not configured for this Enstore instance.
                continue

            # The number of free tapes in this ACS/LSM.
            free = int(line[31:42].strip())

            slot_list.append({"location": lsm,
                              "media_type": "all",
                              "total": "0",
                              "free": free,
                              "used": "0",
                              "disabled": "0",
                              })

            # Obtain specific full cell/slot count.
            command2 = "display cell %s,*,*,* -status full -c" % lsm
            status2, response2, delta2 = self.timed_command(command2, 4, 60)
            if status == 0:
                for line2 in response2:
                    if line2.find("ACSSA") >= 0 or \
                            line2.find("display cell") >= 0 or \
                            line2.find("Display Cell") >= 0 or \
                            line2.find("Number of cells selected") >= 0 \
                            or len(line2) == 0:
                        # This is some other information.
                        continue

                    try:
                        used = int(line2)
                        slot_list[-1]['used'] = str(used)
                        break
                    except (TypeError, ValueError):
                        # Other lines of text may be interspersed with what
                        # we are looking for.  Ignore them.
                        pass

            # Obtain specific total cell/slot count.
            command2 = "display cell %s,*,*,* -c" % lsm
            # answer_lookfor2 = "display cell"
            status2, response2, delta2 = self.timed_command(command2, 4, 60)
            if status == 0:
                for line2 in response2:
                    if line2.find("ACSSA") >= 0 or \
                            line2.find("display cell") >= 0 or \
                            line2.find("Display Cell") >= 0 or \
                            line2.find("Number of cells selected") >= 0 \
                            or len(line2) == 0:
                        # This is some other information.
                        continue

                    try:
                        total = int(line2)
                        slot_list[-1]['total'] = str(total)
                        break
                    except (TypeError, ValueError):
                        # Other lines of text may be interspersed with what
                        # we are looking for.  Ignore them.
                        pass

            # Obtain specific inaccessible/disabled cell/slot count.
            command2 = "display cell %s,*,*,* -status inaccessible -c" % lsm
            # answer_lookfor2 = "display cell"
            status2, response2, delta2 = self.timed_command(command2, 4, 60)
            inaccessible = 0
            if status == 0:
                for line2 in response2:
                    if line2.find("ACSSA") >= 0 or \
                            line2.find("display cell") >= 0 or \
                            line2.find("Display Cell") >= 0 or \
                            line2.find("Number of cells selected") >= 0 \
                            or len(line2) == 0:
                        # This is some other information.
                        continue

                    try:
                        inaccessible = int(line2)
                        # This value is added to the reserved count.
                        break
                    except (TypeError, ValueError):
                        # Other lines of text may be interspersed with what
                        # we are looking for.  Ignore them.
                        pass

            # Obtain specific inaccessible/disabled cell/slot count.
            command2 = "display cell %s,*,*,* -status reserved -c" % lsm
            # answer_lookfor2 = "display cell"
            status2, response2, delta2 = self.timed_command(command2, 4, 60)
            reserved = 0
            if status == 0:
                for line2 in response2:
                    if line2.find("ACSSA") >= 0 or \
                            line2.find("display cell") >= 0 or \
                            line2.find("Display Cell") >= 0 or \
                            line2.find("Number of cells selected") >= 0 \
                            or len(line2) == 0:
                        # This is some other information.
                        continue

                    try:
                        reserved = int(line2)
                        break
                    except (TypeError, ValueError):
                        # Other lines of text may be interspersed with what
                        # we are looking for.  Ignore them.
                        pass

            # Sum these two values for the disabled count.
            slot_list[-1]['disabled'] = str(reserved + inaccessible)

        ticket['slot_list'] = slot_list
        return e_errors.OK, 0, None, "", ""

    def listClean(self, ticket):
        # build the command, and what to look for in the response
        command = "query clean all"
        # answer_lookfor = "query clean all"

        clean_list = []

        # Send reply and Establish the connection first.
        ticket['status'] = (e_errors.OK, 0, None, "", "")
        reply = copy.copy(ticket)
        self.reply_to_caller(reply)
        try:
            hostinfo = socket.getaddrinfo(ticket['callback_addr'][0], None)
            sock = socket.socket(hostinfo[0][0], socket.SOCK_STREAM)
            sock.connect(ticket['callback_addr'])
        except:
            Trace.handle_error()
            Trace.log(e_errors.ERROR,
                      "Callback address %s" % (ticket['callback_addr'],))

            e = 6
            return e_errors.NET_ERROR, e, str(sys.exc_info()[1]), "", ""

        ticket['no_reply'] = 1  # Tell WorkDone() not to send the ticket again.
        reply = ticket.copy()  # Make a copy to keep things clean.  But why?

        # execute the command and read the response
        # FIXME - what if this hangs?
        # efb (dec 22, 2005) - up timeout from 10 to 60 as the queries are hanging
        # status,response, delta = self.timed_command(command,4,10)
        status, response, delta = self.timed_command(command, 4, 60)
        if status != 0:
            e = 4
            message = "QUERY_CLEAN %i: %s => %i,%s" % \
                      (e, command, status, response)
            Trace.log(e_errors.ERROR, message)
            return "ERROR", e, response, "", message
        else:
            # Get the information from the robot.
            for line in response:
                if line.find("ACSSA") >= 0 or \
                        line.find("Cleaning Cartridge Status") >= 0 or \
                        line.find("Identifier") >= 0 \
                        or len(line) == 0:
                    # This is some other information.
                    continue
                # The returned line of interest looks like:
                # For ACSLS version 7:
                # CLN179        1, 5,12, 8, 1  50         0              home       LTO-CLNU
                #
                # For ACSLS version 8:
                # CLN565                0, 1, 6, 0, 0 100         0              home       STK2W

                # get rid of extra whitespaces
                tline = ' '.join(line.translate(None, string.whitespace[:5]).split())
                # now it looks like:
                # CLN565 0, 1, 6, 0, 0 100 0 home STK2W
                # get rid of space before number in address
                tline2 = tline.replace(", ", ",")
                # now it looks like:
                # CLN565 0,1,6,0,0 100 0 home STK2W

                s_line = tline2.split(' ')
                Trace.trace(21, "line %s" % (s_line,))
                volume = s_line[0]
                location = s_line[1]
                max_usage = int(s_line[2])
                current_usage = int(s_line[3])
                status = s_line[4]
                media_type = s_line[5]

                remaining_usage = max_usage - current_usage  # AML2 compatibility
                clean_list.append({"volume": volume,
                                   "location": location,
                                   "max_usage": max_usage,
                                   "current_usage": current_usage,
                                   "remaining_usage": remaining_usage,
                                   "status": status,
                                   "type": media_type,
                                   })

            # Put the list of cleaning tapes into the reply ticket.
            reply['clean_list'] = clean_list

        # Send the information.  (success or failure)
        try:
            r = callback.write_tcp_obj(sock, reply)
            sock.close()
            if r:
                Trace.log(e_errors.ERROR,
                          "Error calling write_tcp_obj. Callback addr. %s"
                          % (ticket['callback_addr'],))
        except OSError:
            Trace.handle_error()
            Trace.log(e_errors.ERROR,
                      "Callback address %s" % (ticket['callback_addr'],))

            e = 6
            return e_errors.NET_ERROR, e, str(sys.exc_info()[1]), "", ""

        return e_errors.OK, 0, None, "", ""

    #########################################################################
    # These functions are internal functions specific to STK media changer.
    #########################################################################

    # Query a volume.
    def query(self, volume, _=""):

        # build the command, and what to look for in the response
        command = "query vol %s" % (volume,)
        answer_lookfor = "%s " % (volume,)

        # execute the command and read the response
        # efb (dec 22, 2005) - up timeout from 10 to 60 as the queries are hanging
        # status,response, delta = self.timed_command(command,4,10)
        status, response, delta = self.timed_command(command, 4, 60)
        if status != 0:
            e = 1
            msg_e = "QUERY %i: %s => %i,%s" % (e, command, status, response)
            Trace.log(e_errors.ERROR, msg_e)
            return "ERROR", e, response, '', msg_e

        # got response, parse it and put it into the standard form
        answer = string.strip(response[3])
        if string.find(answer, answer_lookfor, 0) != 0:
            e = 2
            msg_e = "QUERY %i: %s => %i,%s" % (e, command, status, response)
            Trace.log(e_errors.ERROR, msg_e)
            return "ERROR", e, response, '', msg_e
        elif string.find(answer, ' home ') != -1:
            msg_e = "%s => %i,%s" % (command, status, answer)
            Trace.log(e_errors.INFO, msg_e)
            return e_errors.OK, 0, answer, 'O', msg_e  # occupied
        elif string.find(answer, ' in drive ') != -1:
            msg_e = "%s => %i,%s" % (command, status, answer)
            Trace.log(e_errors.INFO, msg_e)
            return e_errors.OK, 0, answer, 'M', msg_e  # mounted
        elif string.find(answer, ' in transit ') != -1:
            msg_e = "%s => %i,%s" % (command, status, answer)
            Trace.log(e_errors.INFO, msg_e)
            return e_errors.OK, 0, answer, 'T', msg_e  # transit
        else:
            e = 3
            msg_e = "QUERY %i: %s => %i,%s" % (e, command, status, response)
            Trace.log(e_errors.ERROR, msg_e)
            return "ERROR", e, answer, '', msg_e

    def query_drive(self, drive):
        # build the command, and what to look for in the response
        command = "query drive %s" % (drive,)
        answer_lookfor = "%s " % (drive,)

        # execute the command and read the response
        # FIXME - what if this hangs?
        # efb (dec 22, 2005) - up timeout from 10 to 60 as the queries are hanging
        # status,response, delta = self.timed_command(command,4,10)
        status, response, delta = self.timed_command(command, 4, 60)
        if status != 0:
            e = 4
            msg_e = "QUERY_DRIVE %i: %s => %i,%s" % (e, command, status, response)
            Trace.log(e_errors.ERROR, msg_e)
            return "ERROR", e, response, '', msg_e

        # got response, parse it and put it into the standard form
        answer = string.strip(response[3])
        answer = string.replace(answer, ', ', ',')  # easier to part drive id
        if string.find(answer, answer_lookfor, 0) != 0:
            e = 5
            msg_e = "QUERY_DRIVE %i: %s => %i,%s" % (e, command, status, answer)
            Trace.log(e_errors.ERROR, msg_e)
            return "ERROR", e, answer, '', msg_e
        elif string.find(answer, ' online ') == -1:
            e = 6
            msg_e = "QUERY_DRIVE %i: %s => %i,%s" % (e, command, status, answer)
            Trace.log(e_errors.ERROR, msg_e)
            return "ERROR", e, answer, '', msg_e
        elif string.find(answer, ' available ') != -1:
            msg_e = "%s => %i,%s" % (command, status, answer)
            Trace.log(e_errors.INFO, msg_e)
            return e_errors.OK, 0, answer, '', msg_e  # empty
        elif string.find(answer, ' in use ') != -1:
            loc = string.find(answer, ' in use ')
            volume = string.split(answer[loc + 8:])[0]
            msg_e = "%s => %i,%s" % (command, status, answer)
            Trace.log(e_errors.INFO, msg_e)
            return e_errors.OK, 0, answer, volume, msg_e  # mounted and in use
        else:
            e = 7
            msg_e = "QUERY_DRIVE %i: %s => %i,%s" % (e, command, status, answer)
            Trace.log(e_errors.ERROR, msg_e)
            return "ERROR", e, answer, '', msg_e

    def display_drive(self, drive):

        # build the command, and what to look for in the response
        command = "display drive %s -f wwn" % (drive,)
        answer_lookfor = "%s " % (drive,)

        # execute the command and read the response
        status, response, delta = self.timed_command(command, 4, 60)
        if status != 0:
            e = 4
            msg_e = "DISPLAY_DRIVE %i: %s => %i,%s" % (e, command, status, response)
            Trace.log(e_errors.ERROR, msg_e)
            return "ERROR", e, response, '', msg_e

        # got response, parse it and put it into the standard form
        answer = string.strip(response[3])
        if 'No records found' in answer:
            e = 6
            msg_e = "DISPLAY_DRIVE %i: %s => %i,%s" % (e, command, status, response)
            Trace.log(e_errors.ERROR, msg_e)
            return "ERROR", e, response, '', msg_e

        answer = string.replace(answer, ', ', ',')  # easier to part drive id
        # the format of the answer is like:
        # 2    2    1      12     50.01.04.f0.00.a2.b5.06
        # convert it to what we expect
        answer = ' '.join(answer.translate(None, string.whitespace[:5]).split()).replace(' ', ',', 3)
        if answer.find(answer_lookfor, 0) != 0:
            e = 5
            msg_e = "DISPLAY_DRIVE %i: %s => %i,%s" % (e, command, status, answer)
            Trace.log(e_errors.ERROR, msg_e)
            return "ERROR", e, answer, '', msg_e
        else:
            msg_e = ''
            Trace.log(e_errors.INFO, msg_e)
            return e_errors.OK, 0, answer, '', msg_e  # mounted and in use

    def mount(self, volume, drive, media_type="", view_first=1, ticket=None):

        #############################################################
        # ok, this is a test only - see if we can mount readonly for
        # 9840 and 9940 tapes
        if ticket is None:
            ticket = {}
        if media_type in ('9840', '9940', '9940B'):
            vol_ticket = ticket.get('vol_ticket', {})
            si = vol_ticket.get('system_inhibit', ('none', 'none'))
            ui = vol_ticket.get('user_inhibit', ('none', 'none'))

            if enstore_functions2.is_readonly_state(si[1]) or \
                    enstore_functions2.is_readonly_state(ui[1]):
                readonly = 1
            else:
                readonly = 0
        else:
            readonly = 0
        #############################################################

        # build the command, and what to look for in the response
        command = "mount %s %s" % (volume, drive)
        if readonly:
            command = command + " readonly"
        answer_lookfor = "Mount: %s mounted on " % (volume,)

        # check if tape is in the storage location or somewhere else
        if view_first:
            status, stat, response, attrib, com_sent = self.query(volume, media_type)

            if stat != 0:
                e = e_errors.MC_FAILCHKVOL
                msg_e = "MOUNT %i: %s => %i,%s" % (e, command, stat, response)
                Trace.log(e_errors.ERROR, msg_e)
                return "ERROR", e, response, "", msg_e
            if attrib != "O":  # look for tape in tower (occupied="O")
                e = e_errors.MC_VOLNOTHOME
                msg_e = "MOUNT %i: Tape is not in home position. %s => %s,%s" % (e, command, status, response)
                Trace.log(e_errors.ERROR, msg_e)
                return "ERROR", e, response, "", msg_e

            # check if any tape is mounted in this drive
            status, stat, response, volser, com_sent = self.query_drive(drive)
            if stat != 0:
                e = e_errors.MC_FAILCHKDRV
                msg_e = "MOUNT %i: %s => %i,%s" % (e, command, stat, response)
                Trace.log(e_errors.ERROR, msg_e)
                return "ERROR", e, response, "", msg_e
            if volser != "":  # look for any tape mounted in this drive
                e = e_errors.MC_DRVNOTEMPTY
                msg_e = "MOUNT %i: Drive %s is not empty =>. %s => %s,%s" % (e, drive, command, status, response)
                Trace.log(e_errors.ERROR, msg_e)
                return "ERROR", e, response, "", msg_e

        # execute the command and read the response
        status, response, delta = self.timed_command(command, 2, 60 * 10)
        if status != 0:
            e = 12
            msg_e = "MOUNT %i: %s => %i,%s" % (e, command, status, response)
            Trace.log(e_errors.ERROR, msg_e)
            return "ERROR", e, response, "", msg_e

        # got response, parse it and put it into the standard form
        answer = string.strip(response[1])
        if string.find(answer, answer_lookfor, 0) != 0:
            # During cap operations acsls returns an error message containing the information that the volume was
            # actually mounted. If this is the case, process it
            compared = 0
            try:
                Trace.log(e_errors.INFO,
                          "Checking ASCLS message %s %s" % (response, answer_lookfor))  # remove after debugging AM

                for line in response:
                    if answer_lookfor in line:
                        # ok the volume is actually mounted
                        # but in what drive?
                        requested_drive = drive.split(',')
                        line = line.replace(',', ' ')
                        ar = line.split()
                        Trace.log(e_errors.INFO, "Requested Drive %s. Comparing to %s" % (
                                  requested_drive, ar))  # remove after debugging AM
                        same_drive = 0
                        for i in range(len(requested_drive)):
                            if int(requested_drive[-(i + 1)]) != int(ar[-(i + 1)]):
                                break
                        else:
                            same_drive = 1
                        if same_drive:
                            compared = 1
                            Trace.log(e_errors.INFO, "The error was false: %s" % (response,))
                            break
                else:
                    compared = 0
            except:
                Trace.handle_error()

            if compared == 0:
                e = 13
                if answer.find("Unreadable label") != -1:
                    e = e_errors.MC_VOLNOTFOUND
                msg_e = "MOUNT %i: %s => %i,%s" % (e, command, status, answer)
                Trace.log(e_errors.ERROR, msg_e)
                return "ERROR", e, response, "", msg_e
        msg_e = "%s => %i,%s" % (command, status, answer)
        Trace.log(e_errors.INFO, msg_e)
        return e_errors.OK, 0, msg_e, "", ""

    def dismount(self, volume, drive, _="", view_first=1):

        # build the command, and what to look for in the response
        command = "dismount VOLUME %s force" % (drive,)
        answer_lookfor = "Dismount: Forced dismount of "

        # check if any tape is mounted in this drive
        if view_first:
            status, stat, response, volser, com_sent = self.query_drive(drive)
            if stat != 0:
                e = e_errors.MC_FAILCHKDRV
                msg_e = "DISMOUNT %i: %s => %i,%s" % (e, command, stat, response)
                Trace.log(e_errors.ERROR, msg_e)
                return "ERROR", e, response, "", msg_e

            if volser == "":  # look for any tape mounted in this drive
                if volume != "Unknown":
                    # FIXME - this should be a real error. mover needs to know which tape it has.
                    e = 14
                    msg_e = "Dismount %i ignored: Drive %s is empty. Thought %s was there =>. %s => %s,%s" % (
                             e, drive, volume, command, status, response)
                    Trace.log(e_errors.INFO, msg_e)
                    return e_errors.OK, 0, response, "", msg_e
                else:  # don't know the volume on startup
                    e = 15
                    msg_e = "Dismount %i ignored: Drive %s is empty. Thought %s was there =>. %s => %s,%s" % (
                             e, drive, volume, command, status, response)
                    Trace.log(e_errors.INFO, msg_e)
                    return e_errors.OK, 0, response, "", msg_e

        # execute the command and read the response
        status, response, delta = self.timed_command(command, 2, 60 * 10)
        if status != 0:
            e = 16
            msg_e = "DISMOUNT %i: %s => %i,%s" % (e, command, status, response)
            Trace.log(e_errors.ERROR, msg_e)
            return "ERROR", e, response, "", msg_e

        # got response, parse it and put it into the standard form
        answer = string.strip(response[1])
        if string.find(answer, answer_lookfor, 0) != 0:
            # During cap operations acsls returns an error message containing the information that the volume
            # was actually mounted. If this is the case, process it
            compared = 0
            try:
                for line in response:
                    if answer_lookfor in line:
                        # ok the volume is actually mounted
                        # but in what drive?
                        requested_drive = drive.split(',')
                        line = line.replace(',', ' ')
                        ar = line.split()
                        Trace.log(e_errors.INFO, "Requested Drive %s. Comparing to %s" % (
                                  requested_drive, ar))  # remove after debugging AM
                        same_drive = 0
                        for i in range(len(requested_drive)):
                            if int(requested_drive[-(i + 1)]) != int(ar[-(i + 1)]):
                                break
                        else:
                            same_drive = 1
                        if same_drive:
                            compared = 1
                            Trace.log(e_errors.INFO, "The error was false: %s" % (response,))
                            break
                else:
                    compared = 0
            except:
                Trace.handle_error()

            if compared == 0:
                e = 17
                msg_e = "DISMOUNT %i: %s => %i,%s" % (e, command, status, answer)
                Trace.log(e_errors.ERROR, msg_e)
                return "ERROR", e, response, "", msg_e
        msg_e = "%s => %i,%s" % (command, status, answer)
        Trace.log(e_errors.INFO, msg_e)
        return e_errors.OK, 0, msg_e, "", ""


#########################################################################
#
# manual media changer
#
#########################################################################
class Manual_MediaLoader(MediaLoaderMethods):
    def __init__(self, medch, max_work=7, csc_local=None):
        MediaLoaderMethods.__init__(self, medch, max_work, csc_local)
        if 'DriveCleanTime' in self.mc_config:  # error if DriveCleanTime assignments not in config
            self.driveCleanTime = self.mc_config['DriveCleanTime']
        else:
            self.driveCleanTime = None
        self.media_changer_test = self.mc_config.get("test", None)

    #########################################################################
    # These functions are overridden from the generic class.
    #########################################################################

    def cleanCycle(self, in_ticket):
        # do drive cleaning cycle
        Trace.log(e_errors.INFO, 'mc: ticket=' + repr(in_ticket))
        try:
            drive = in_ticket['moverConfig']['mc_device']
        except KeyError:
            Trace.log(e_errors.ERROR, 'mc: no device field found in ticket.')
            status = 37
            return e_errors.DOESNOTEXIST, status, "no device field found in ticket"

        drive_type = drive[:2]  # ... need device type, not actual device
        try:
            if self.driveCleanTime:
                clean_time = self.driveCleanTime[drive_type][0]  # clean time in seconds
                drive_clean_cycles = self.driveCleanTime[drive_type][1]  # number of cleaning cycles
            else:
                clean_time = 60
                drive_clean_cycles = 1
        except KeyError:
            clean_time = 60
            drive_clean_cycles = 1

        vcc = volume_clerk_client.VolumeClerkClient(self.csc)
        min_remaining_bytes = 1
        vol_veto_list = []
        first_found = 0
        library_managers = in_ticket['moverConfig']['library']
        if isinstance(library_managers, types.StringType):
            lm = library_managers
            library = string.split(library_managers, ".")[0]
        elif isinstance(library_managers, types.ListType):
            lm = library_managers[0]
            library = string.split(library_managers[0], ".")[0]
        else:
            Trace.log(e_errors.ERROR, 'mc: library_manager field not found in ticket.')
            status = 37
            return e_errors.DOESNOTEXIST, status, "no library_manager field found in ticket"
        lm_info = self.csc.get(lm)
        if 'CleanTapeVolumeFamily' not in lm_info:
            Trace.log(e_errors.ERROR, 'mc: no CleanTapeVolumeFamily field found in ticket.')
            status = 37
            return e_errors.DOESNOTEXIST, status, "no CleanTapeVolumeFamily field found in ticket"

        clean_tape_volume_family = lm_info['CleanTapeVolumeFamily']
        v = vcc.next_write_volume(library,
                                  min_remaining_bytes, clean_tape_volume_family,
                                  vol_veto_list, first_found, exact_match=1)  # get which volume to use
        if v["status"][0] != e_errors.OK:
            Trace.log(e_errors.ERROR, "error getting cleaning volume:%s %s" %
                      (v["status"][0], v["status"][1]))
            return v["status"][0], 0, v["status"][1]

        for i in range(drive_clean_cycles):
            Trace.log(e_errors.INFO, "clean drive %s, vol. %s" % (drive, v['external_label']))
            t = {'vol_ticket': v, 'drive_id': drive}
            rt = self.loadvol(t)
            if not e_errors.is_ok(rt[0]):
                return rt
            time.sleep(clean_time)  # wait clean_time seconds
            rt = self.unloadvol(t)
            if not e_errors.is_ok(rt[0]):
                return rt
        ret_ticket = vcc.get_remaining_bytes(v['external_label'])
        remaining_bytes = ret_ticket['remaining_bytes'] - 1
        vcc.set_remaining_bytes(v['external_label'], remaining_bytes, '\0', None)
        return e_errors.OK, 0, None

    def getVolState(self, ticket):
        __pychecker__ = "no-argsused"
        return (e_errors.NOT_SUPPORTED, 0,
                "Manual media changer has no robot to query.")

    def getDriveState(self, ticket):
        __pychecker__ = "no-argsused"
        return (e_errors.NOT_SUPPORTED, 0,
                "Manual media changer has no robot to query.")

    def query_robot(self, ticket):
        __pychecker__ = "no-argsused"
        return (e_errors.NOT_SUPPORTED, 0,
                "Manual media changer has no robot to query.")

    def listDrives(self, ticket):
        __pychecker__ = "no-argsused"
        return (e_errors.NOT_SUPPORTED, 0,
                "Manual media changer has no robot to query.")

    def listVolumes(self, ticket):
        __pychecker__ = "no-argsused"
        return (e_errors.NOT_SUPPORTED, 0,
                "Manual media changer has no robot to query.")

    def listVolumes2(self, ticket):
        __pychecker__ = "no-argsused"
        return (e_errors.NOT_SUPPORTED, 0,
                "Manual media changer has no robot to query.")

    def listClean(self, ticket):
        __pychecker__ = "no-argsused"
        return (e_errors.NOT_SUPPORTED, 0,
                "Manual media changer has no robot to query.")

    def listSlots(self, ticket):
        __pychecker__ = "no-argsused"
        return (e_errors.NOT_SUPPORTED, 0,
                "Manual media changer has no robot to query.")

    #########################################################################
    # These functions are internal functions specific to Manual media changer.
    #########################################################################

    # load volume into the drive; default overridden for other media changers
    def load(self, ticket):
        drive = ticket['drive_id']
        external_label = ticket['vol_ticket']['external_label']

        if external_label:
            if self.media_changer_test:
                mc_popup = "mc_popup_test"
            else:
                mc_popup = "mc_popup"

            rt = os.system("%s 'Please load %s into %s'" %
                           (mc_popup, external_label, drive))
            if rt:
                return e_errors.UNKNOWN, 0, str(rt)

        return e_errors.OK, 0, None

    # unload volume from the drive; default overridden for other media changers
    def unload(self, ticket):
        drive = ticket['drive_id']
        external_label = ticket['vol_ticket']['external_label']

        if external_label:
            if self.media_changer_test:
                mc_popup = "mc_popup_test"
            else:
                mc_popup = "mc_popup"

            rt = os.system("%s 'Please unload %s from %s'" %
                           (mc_popup, external_label, drive))
            if rt:
                return e_errors.UNKNOWN, 0, str(rt)

        return e_errors.OK, 0, None


#########################################################################
#
# Raw Disk and stand alone tape media server
#
#########################################################################
class RDD_MediaLoader(MediaLoaderMethods):
    def __init__(self, medch, max_work=1, csc_local=None):
        MediaLoaderMethods.__init__(self, medch, max_work, csc_local)

    #########################################################################
    # These functions are overridden from the generic class.
    #########################################################################

    def getVolState(self, ticket):
        __pychecker__ = "no-argsused"
        return (e_errors.NOT_SUPPORTED, 0,
                "NULL media changer has no robot to query.")

    def getDriveState(self, ticket):
        __pychecker__ = "no-argsused"
        return (e_errors.NOT_SUPPORTED, 0,
                "NULL media changer has no robot to query.")

    def query_robot(self, ticket):
        __pychecker__ = "no-argsused"
        return (e_errors.NOT_SUPPORTED, 0,
                "NULL media changer has no robot to query.")

    def listDrives(self, ticket):
        __pychecker__ = "no-argsused"
        return (e_errors.NOT_SUPPORTED, 0,
                "NULL media changer has no robot to query.")

    def listVolumes(self, ticket):
        __pychecker__ = "no-argsused"
        return (e_errors.NOT_SUPPORTED, 0,
                "NULL media changer has no robot to query.")

    def listVolumes2(self, ticket):
        __pychecker__ = "no-argsused"
        return (e_errors.NOT_SUPPORTED, 0,
                "NULL media changer has no robot to query.")

    def listClean(self, ticket):
        __pychecker__ = "no-argsused"
        return (e_errors.NOT_SUPPORTED, 0,
                "NULL media changer has no robot to query.")

    def listSlots(self, ticket):
        __pychecker__ = "no-argsused"
        return (e_errors.NOT_SUPPORTED, 0,
                "NULL media changer has no robot to query.")

    #########################################################################
    # These functions are internal functions specific to NULL media changer.
    #########################################################################

    # load volume into the drive; default, overridden for other media changers
    def load(self, ticket):
        drive = ticket['drive_id']
        external_label = ticket['vol_ticket']['external_label']
        if 'delay' in self.mc_config.keys() and self.mc_config['delay']:
            # YES, THIS BLOCK IS FOR THE DEVELOPMENT ENVIRONMENT AND THE
            # OUTPUT OF THE PRINTS GO TO THE TERMINAL
            print "make sure tape %s in in drive %s" % (external_label, drive)
            time.sleep(self.mc_config['delay'])
            print 'continuing with reply'
        return e_errors.OK, 0, None

    # unload volume from the drive; default overridden for other media changers
    def unload(self, ticket):
        drive = ticket['drive_id']
        external_label = ticket['vol_ticket']['external_label']
        if 'delay' in self.mc_config.keys() and self.mc_config['delay']:
            message = "remove tape %s from drive %s" % (external_label, drive)
            Trace.log(e_errors.INFO, message)
            time.sleep(self.mc_config['delay'])
        return e_errors.OK, 0, None


# This method tries to execute function `f` with arguments `a` in time `t`.
# The return values are first an int 0 for completion and -1 for
# failure to complete in the allotted time.  The second return value
# is either a tuple of values which depend on what f returns if f
# completed, or the string 'timeout error' if f didn't return in the
# allotted time.
def return_by(f, a, t):
    q = multiprocessing.Queue()
    e = threading.Event()
    # h = multiprocessing.Process(target = execute_and_set, args   = (f, a, e, q))
    h = threading.Thread(target=execute_and_set, args=(f, a, e, q))
    h.start()
    e.wait(t)
    if q.empty():
        return -1, 'timeout error'
    else:
        return 0, q.get()


# This method executes function `f` with arguments `a`, puts the result in
# queue `q` and triggers the Event `e`.
def execute_and_set(f, a, e, q):
    q.put(apply(f, a))
    e.set()
    return


#########################################################################
#
# mtx robot loader server for Overland stackers
#
#########################################################################
class MTX_MediaLoader(MediaLoaderMethods):

    def __init__(self, medch, max_work=1, csc_local=None):

        MediaLoaderMethods.__init__(self, medch, max_work, csc_local)

        # Mark our cached status info as invalid
        self.drives = None
        self.slots = None
        self.status_valid = 0

        # Read the device name to use.
        if 'device_name' in self.mc_config:
            self.device_name = self.mc_config['device_name']
        else:
            self.device_name = '/dev/sgb'  # best guess
            Trace.log(e_errors.ERROR,
                      'mtx: no device specified.  Guessing /dev/sgb')

        # Read the value for the timeout on status commands.
        if 'status_timeout' in self.mc_config:
            self.status_timeout = self.mc_config['status_timeout']
        else:
            self.status_timeout = 5  # best guess
            Trace.log(e_errors.ERROR,
                      'mtx no status timeout specified.  Using 5 seconds')

        # Read the value for the timeout on mount commands.
        if 'mount_timeout' in self.mc_config:
            self.mount_timeout = self.mc_config['mount_timeout']
        else:
            self.mount_timeout = 120  # best guess
            Trace.log(e_errors.ERROR,
                      'mtx no mount timeout specified.  Using 120 seconds')
        self.sudo_cmd = self.mc_config.get('sudo_cmd', '')

        Trace.log(e_errors.INFO,
                  '%s initialized with device: %s status time limit: %s mount time limit: %s ' %
                  (self.__class__.__name__, self.device_name, self.status_timeout, self.mount_timeout))

    #########################################################################
    # These functions are overridden from the generic class.
    #########################################################################

    # query robot
    def query_robot(self, ticket):
        __pychecker__ = "no-argsused"
        rc = self.robot_status()
        return e_errors.OK, 0, rc, "", ""

    # load volume into the drive;
    def load(self, ticket):
        """
        external_label,    # volume external label
        drive,             # drive id
        media_type):       # media type
        """
        Trace.log(e_errors.INFO, 'MTX_MediaLoader: request to load pid %s' % (os.getpid(),))
        drive = ticket['drive_id']
        external_label = ticket['vol_ticket']['external_label']
        media_type = ticket['vol_ticket']['media_type']
        Trace.log(e_errors.INFO,
                  'MTX_MediaLoader: request to load %s of type %s into drive %s' % (external_label, media_type, drive))
        return self.retry_function(self.mtx_mount, external_label,
                                   drive, media_type)

    # unload volume from the drive
    def unload(self, ticket):
        """
        external_label,  # volume external label
        drive,           # drive id
        media_type):     # media type
        """
        Trace.log(e_errors.INFO, 'MTX_MediaLoader: request to unload pid %s' % (os.getpid(),))
        drive = ticket['drive_id']
        external_label = ticket['vol_ticket']['external_label']
        media_type = ticket['vol_ticket']['media_type']
        Trace.log(e_errors.INFO, 'MTX_MediaLoader: request to unload %s of type %s from drive %s' % (
                  external_label, media_type, drive))
        return self.retry_function(self.mtx_dismount, external_label,
                                   drive, media_type)

    def insert(self, ticket):
        __pychecker__ = "no-argsused"  # When fixed remove this pychecker line.
        return (e_errors.NOT_SUPPORTED, 0,
                "MTX media changer does not support this operation.")

    def eject(self, ticket):
        __pychecker__ = "no-argsused"  # When fixed remove this pychecker line.
        return (e_errors.NOT_SUPPORTED, 0,
                "MTX media changer does not support this operation.")

    #########################################################################
    # These functions are internal functions specific to MTX media changer.
    #########################################################################

    # Find the tape and mount it in the drive.
    def mtx_mount(self, volume, drive, _=None, __=None):
        try:
            dr = int(drive)
        except:
            Trace.handle_error()
            Trace.log(e_errors.ERROR, 'mtx_mount unrecognized drive: %s' % (drive,))
            return ('ERROR', e_errors.ERROR, [], '',
                    'mtx_mount unrecognized drive: %s' % (drive,))

        s, d = self.locate_volume(volume)

        if -1 == s:
            if -1 == d:
                Trace.log(e_errors.ERROR,
                          'mtx cant mount tape. Not in library')
                return ('ERROR', e_errors.ERROR, [], '',
                        'mtx cant mount tape. Not in library')
            else:
                Trace.log(e_errors.ERROR,
                          'mtx cant mount tape. Already in drive %d' % (d,))
                return ('ERROR', e_errors.ERROR, [], '',
                        'mtx cant mount tape. Already in drive %d' % (d,))

        Trace.log(e_errors.INFO, 'found %s in slot %s ...mounting' % (volume, s))
        a, b = return_by(self.load_unload_local, (s, dr, "load"), self.mount_timeout)

        self.status_valid = 0

        if -1 == a:
            Trace.log(e_errors.ERROR, 'mtx mount timeout')
            return 'ERROR', e_errors.ERROR, [], '', 'mtx mount timeout'
        else:
            return b

    # is tape drive empty
    def is_empty(self, drive):
        if 0 == self.status_valid:
            a, b = return_by(self.status_local, (), self.status_timeout)
            if -1 == a:
                Trace.log(e_errors.ERROR, 'mtx status request timeout')
                return False
            self.status_valid = 1

        if self.drives[drive] == EMPTY:
            return True
        else:
            return False

    # Find a free slot and unmount the tape from the drive.
    def mtx_dismount(self, volume, drive, _=None, __=None):

        try:
            dr = int(drive)
        except:
            Trace.handle_error()
            Trace.log(e_errors.ERROR, 'mtx_dismount unrecognized drive: %s' % (drive,))
            return ('ERROR', e_errors.ERROR, [], '',
                    'mtx_dismount unrecognized drive: %s' % (drive,))

        if self.is_empty(dr):  # no need to dismount
            return e_errors.OK, 0, None, "", ""

        s, ignore = self.locate_volume(EMPTY)

        if -1 == s:
            Trace.log(e_errors.ERROR, ' mtx unload: No free slots')
            return ('ERROR', e_errors.ERROR, [], '',
                    'mtx unload: No free slots')

        ignore, d = self.locate_volume(volume)

        if dr != d:
            Trace.log(e_errors.ERROR, ' mtx unload: %s is in %d, not %d' %
                      (volume, d, dr))
            return ('ERROR', e_errors.ERROR, [], '',
                    'mtx unload: %s is not in %d' %
                    (volume, dr))

        Trace.log(e_errors.INFO, ('found ', volume, ' in drive ', d,
                                  '...dismounting'))
        a, b = return_by(self.load_unload_local, (s, dr, "unload"), self.mount_timeout)

        self.status_valid = 0

        if -1 == a:
            Trace.log(e_errors.ERROR, ' mtx unmount timeout')
            return 'ERROR', e_errors.ERROR, [], '', ' mtx dismount timeout'
        else:
            return b

    # This method indicates where the tape is located within the
    # library by returning two numbers.  The first number is the slot
    # number containing the tape or negative one if the tape is not in
    # a slot.  The second number is the drive number the tape is in or
    # negative one if the tape is not in a drive.  (slots and drives
    # are both indexed starting at zero.)  If both numbers are
    # negative one then the tape is not in the library.  If both
    # numbers are not negative one then there is either a bug in this
    # function or multiple tapes that have the same label in the
    # library.
    def locate_volume(self, vol):
        Trace.log(e_errors.INFO, ' looking for volume %s' % (vol,))
        if 0 == self.status_valid:
            a, b = return_by(self.status_local, (), self.status_timeout)
            if -1 == a:
                Trace.log(e_errors.ERROR, ' mtx status request timeout')
                return -1, -1
            self.status_valid = 1
        found = 0
        idx_drive = 0
        for i in self.drives:
            if vol == i:
                found = 1
                break
            idx_drive = idx_drive + 1

        if 0 == found:
            idx_drive = -1

        found = 0
        idx_slot = 0
        for i in self.slots:
            if vol == i:
                found = 1
                break
            idx_slot = idx_slot + 1

        if 0 == found:
            idx_slot = -1

        return idx_slot, idx_drive

    #  This method tries to have device 'device' load or unload the tape in
    #  or from drive number drive back into slot number 'slot'.  The return
    #  value is anything that MTX printed to stderr.  If mtx hangs,
    #  this method will never return.
    def load_unload_local(self, slot, drive, load_command):
        if load_command not in ("load", "unload"):
            return 'ERROR', e_errors.ERROR, [], "%s" % (load_command,), "Wrong command"
        cmd = "%s mtx -f %s %s %d %d" % (self.sudo_cmd, self.device_name, load_command, slot + 1, drive)
        Trace.log(e_errors.INFO, "Invoking the following command: %s" % (cmd,))
        result = enstore_functions2.shell_command(cmd)
        if result:
            if result[1]:
                Trace.log(e_errors.ERROR,
                          'mtx %s returned this message: %s' % (load_command, result[1],))
                return 'ERROR', e_errors.ERROR, [], "", result[1]
            else:
                # No error
                return e_errors.OK, 0, None, "", ""
        else:
            return 'ERROR', e_errors.ERROR, [], "", "Lost command"

    # This method blocks while it returns the status of the media
    # changer at the specified device.
    # A return value are any messages that mtx printed to stderr.
    # If mtx hangs, this method will never return.
    def status_local(self):
        cmd = '%s mtx -f %s status' % (self.sudo_cmd, self.device_name)
        Trace.log(e_errors.INFO, "Invoking the following command: %s" % (cmd,))
        result = enstore_functions2.shell_command(cmd)
        self.drives = []  # list of barcodes for the tapes in the drives
        self.slots = []  # list of the barcodes for the tapes in the slots
        error_string = ''

        if result:
            if result[0]:  # stdout
                lines = result[0].split("\n")
                index = 0
                line = lines[index]
                while '' != line:
                    line = string.strip(line)
                    if string.find(line, 'Data Transfer Element') != -1:
                        if string.find(line, 'Empty') > 0:
                            self.drives.append(EMPTY)
                        elif string.find(line, 'VolumeTag') != -1:
                            i1 = string.find(line, '=') + 1
                            i2 = len(line)
                            self.drives.append(string.strip(line[i1:i2]))
                        else:
                            self.drives.append('unlabelled')
                    elif string.find(line, 'Storage Element') != -1:
                        if string.find(line, 'Empty') > 0:
                            self.slots.append(EMPTY)
                        elif string.find(line, 'VolumeTag') != -1:
                            i1 = string.find(line, '=') + 1
                            i2 = len(line)
                            self.slots.append(string.strip(line[i1:i2]))
                        else:
                            self.slots.append('unlabelled')

                    index = index + 1
                    line = lines[index]

            if result[1]:  # stderr
                lines = result[0].split("\n")
                index = 0
                line = lines[index]
                while '' != line:
                    error_string = error_string + line
                    index = index + 1
                    line = lines[index]

                if '' != error_string:
                    Trace.log(e_errors.ERROR,
                              'mtx status returned this message %s' % (error_string,))
        else:
            Trace.log(e_errors.ERROR,
                      'mtx status returned no result %s' % (result,))

        return error_string

    # return status of all drives and slots
    def robot_status(self):
        result = enstore_functions2.shell_command(
            "%s mtx -f %s status | grep 'Data Transfer Element'" % (self.sudo_cmd, self.device_name))

        return result

    # getVolState in the drive; default overridden for other media changers
    def getVolState(self, ticket):
        __pychecker__ = "no-argsused"
        Trace.log(ACTION_LOG_LEVEL, 'getVolState: %s' % (ticket,))
        ticket['status'] = e_errors.OK
        slot, drive = self.locate_volume(ticket['external_label'])
        Trace.log(e_errors.INFO, 'getVolState slot:%s, drive %s' % (slot, drive,))
        if slot < 0 and drive < 0:
            ticket['status'] = e_errors.MC_VOLNOTFOUND
        elif slot < 0 <= drive:
            ticket['state'] = 'M'
            ticket['location'] = drive
        elif slot >= 0 > drive:
            ticket['state'] = 'O'
            ticket['location'] = slot
        rc = (ticket.get('status'), ticket.get('location'), ticket.get('media_type'), ticket.get('state'))
        Trace.log(ACTION_LOG_LEVEL, 'getVolState: returning %s' % (rc,))
        return rc

    def getDriveState(self, ticket):
        Trace.log(ACTION_LOG_LEVEL, 'getDriveState: %s' % (ticket,))
        drive = int(ticket['drive'])
        self.status_local()
        drive_info = {}
        if drive >= len(self.drives):
            rc = [e_errors.MC_DRVNOTFOUND, 0, 'No such drive', 'No such drive']
            drive_info['state'] = 'noinfo'
            drive_info['status'] = 'N/A'
            Trace.log(ACTION_LOG_LEVEL, 'getDriveState: returning %s' % (rc,))
            return rc
        if self.drives[drive] == EMPTY:
            drive_info['volume'] = ''
        else:
            drive_info['volume'] = self.drives[drive]
        drive_info['state'] = 'online'
        drive_info['type'] = 'N/A'
        if drive_info['volume'] == '':
            drive_info['status'] = 'available'
        else:
            drive_info['status'] = 'in use'
        ticket['drive_info'] = drive_info
        Trace.log(ACTION_LOG_LEVEL, 'getDriveState: returning %s' % ((e_errors.OK, 0, '', ''),))
        return e_errors.OK, 0, '', ''


def get_mtx_status(device):
    """
    to use with new mtx to get information about all tape drives and volumes in the robot.
    """

    Trace.log(e_errors.INFO, "get_mtx_status %s" % (device,))
    message = ""
    p2cread, p2cwrite = os.pipe()
    c2pread, c2pwrite = os.pipe()

    pid = os.fork()

    if pid == 0:
        # Child
        for i in 0, 1, 2:
            try:
                os.close(i)
            except os.error:
                pass
        if os.dup(p2cread) != 0:
            Trace.log(e_errors.ERROR, 'ERROR: timed_command pc2cread bad read dup')
        if os.dup(c2pwrite) != 1:
            Trace.log(e_errors.ERROR, 'ERROR: timed_command c2pwrite bad write dup')
        if os.dup(c2pwrite) != 2:
            Trace.log(e_errors.ERROR, 'ERROR: timed_command c2pwrite bad error dup')
        maxfd = 10  # Max number of file descriptors (os.getdtablesize()???)
        for i in range(3, maxfd):
            try:
                os.close(i)
            except:
                pass
        try:
            # I know this is hard-coded and inflexible. That is what I want in order to
            # prevent any possible security problem.
            mtx.cvar.device = device
            mtx.cvar.absolute_addressing = 1
            mtx.open_device()
            mtx.status()
        except:
            e, e_msg, e_tb = sys.exc_info()
            Trace.log(e_errors.ERROR, 'command failed:  %s %s %s' % (e, e_msg, traceback.format_tb(e_tb)))
            os._exit(1)
        Trace.log(ACTION_LOG_LEVEL, "CHILD exits")

    try:
        sys.stdout.flush()
        sys.stderr.flush()
        os.close(p2cread)
        os.close(c2pwrite)
        os.close(p2cwrite)
    except:
        pass

    # wait for child to complete, or kill it
    start = time.time()
    active = 0
    try:
        while active < 60:
            p, _ = os.waitpid(pid, os.WNOHANG)
            if p != 0:
                Trace.log(ACTION_LOG_LEVEL, "BREAK")
                break
            # We need to start reading this now for really long responses.
            # Otherwise, the buffer fills up with the child waiting
            # for the parent to read something from the full buffer.
            # And the parent waits for the child to finish.
            e_msg = os.read(c2pread, 2000)
            if e_msg:
                message = message + e_msg
                # print "MSG",  message
                # Need to reset the timeout period.
                start = time.time()
                active = 0
            else:
                active = time.time() - start
                time.sleep(1)
        else:
            msg1 = "killing %d" % (pid,)
            print timeofday.tod(), msg1
            os.kill(pid, signal.SIGTERM)
            time.sleep(1)
            p, _ = os.waitpid(pid, os.WNOHANG)
            if p == 0:
                msg1 = "kill -9ing %d" % (pid,)
                print timeofday.tod(), msg1
                os.kill(pid, signal.SIGKILL)
                time.sleep(2)
                os.waitpid(pid, os.WNOHANG)
    except OSError, detail:
        os.close(c2pread)
        if detail[0] != errno.ECHILD:
            e, e_msg, e_tb = sys.exc_info()
            Trace.log(e_errors.ERROR, 'wait for child failed:  %s %s %s' % (e, e_msg, traceback.format_tb(e_tb)))
            return None
    except:
        e, e_msg, e_tb = sys.exc_info()
        Trace.log(e_errors.ERROR, 'wait for child failed:  %s %s %s' % (e, e_msg, traceback.format_tb(e_tb)))
        os.close(c2pread)
        return None
    os.close(c2pread)
    print "GET MTX STATUS"
    print "=========================="
    print message
    print "=========================="
    return message


############################################################
## This class is for drive and slot addresses matching with IBM addresses
## It uses direct calls to mtx routines via SWIG interface
#############################################################
class MTXN_MediaLoader(MediaLoaderMethods):
    def update_db(self, ticket):
        Trace.trace(ACTION_LOG_LEVEL, 'update_db called with %s' % (ticket,))
        ticket['function'] = 'updatedb'
        # Make sure this request has all the necessary information included.
        return self.DoWork(self.updatedb, ticket)

    def DoWork(self, function, ticket):

        # The function immediately called by dispatching worker should have
        # set a 'function' field in the ticket.
        Trace.trace(ACTION_LOG_LEVEL, 'DoWork received %s %s' % (function, ticket))
        if 'function' not in ticket:
            err_msg = "MISSING FUNCTION KEY"
            ticket['status'] = (e_errors.MALFORMED, 0, err_msg)
            Trace.log(e_errors.ERROR, "%s" % (ticket['status'],))
            return ticket['status']

        # if this a duplicate request, drop it
        if self.exists_in_work_list(ticket):
            message = "duplicate request, drop it %s %s" % \
                      (repr(ticket['r_a']), repr(ticket['r_a']))
            Trace.trace(ACTION_LOG_LEVEL, message)
            return

        # Output what we intend to do.
        common_message = self.get_common_message_string(ticket)
        Trace.trace(ACTION_LOG_LEVEL, "REQUESTED %s" % (common_message,))

        ###
        ### Determine if we need to drop the request for one reason or another.
        ###

        # Let work list length exceed max_work for cleanCycle.
        if ticket['function'] in ('cleanCycle', 'listClean', 'listVolumes'):
            pass

        elif ticket['function'] in ('mount', 'dismount', 'eject', 'insert'):
            # If we have max number of working children processes, assume
            # client will resend the request.
            if len(self.work_list) >= self.max_work:
                message = "MC Overflow: %s %s" % \
                          (repr(self.max_work), common_message)
                Trace.log(e_errors.INFO, message)

                # Need to call reply_to_caller() here since the request has
                # not been processed far enough for WorkDone() to reply
                # for us.
                ticket['status'] = (e_errors.MC_QUEUE_FULL, len(self.work_list),
                                    "retry later")
                self.reply_to_caller(ticket)
                return
            # Else if the work queue is temporarily closed, assume client
            # will resend the request.
            elif self.workQueueClosed and len(self.work_list) > 0:
                message = "MC Queue Closed: %s %s" % \
                          (repr(len(self.work_list)), common_message)
                Trace.log(e_errors.INFO, message)
                return

            # If drive is doing a clean cycle; drop request and assume client
            # will resend the request.
            if ticket['function'] in ("mount", "dismount"):
                for i in self.work_list:
                    try:
                        if (i['function'] == "cleanCycle" and
                                'drive_id' in i and
                                i['drive_id'] == ticket['drive_id']):
                            message = "Dropped %s request for %s in %s, " \
                                      "drive in cleaning cycle." % \
                                      (ticket['function'],
                                       ticket['vol_ticket']['external_label'],
                                       ticket['drive_id'])
                            Trace.log(e_errors.INFO, message)
                            return
                    except:
                        Trace.handle_error()
                        Trace.log(e_errors.ERROR, "ERROR %s" % (i,))
        else:  # query requests
            if len(self.work_query_list) >= self.max_work:
                # If the request work list is full, drop request and assume
                # the client will resend the request.
                return

        ### Otherwise, we can process work.

        # Output what we are going to do.
        Trace.log(e_errors.INFO, "PROCESSING %s" % (common_message,))

        # If function is insert and queue not empty: close work queue and
        # set values to prepare for completing this operation once all
        # pending requests are fulfilled.
        if ticket['function'] in ('insert', 'updatedb', 'getVolState', 'getDriveState'):
            Trace.log(ACTION_LOG_LEVEL, 'work_queue size %s' % (len(self.work_list),))
            if len(self.work_list) > 0:
                self.workQueueClosed = 1
                self.timeInsert = time.time()
                self.insertRA = ticket['r_a']
                return
            else:
                self.workQueueClosed = 0

        # If not a duplicate request or dropped request; fork the work.
        self.p = os.pipe()
        cmd_executor = multiprocessing.Process(target=self.executor, args=(function, ticket, self.p, common_message))
        self.add_select_fd(self.p[0])  # wait for reading half of pipe.
        # add entry to outstanding work
        if not ticket['function'] in ('listClean', 'listVolumes'):
            self.add_to_work_list(ticket)
        # log the new work list
        self.log_work_list(ticket)
        cmd_executor.start()
        return

    def executor(self, function, ticket, comm_pipe, common_message):
        #  in child process
        message = "mcDoWork> child begin %s" % (common_message,)
        Trace.trace(ACTION_LOG_LEVEL, message)
        # do the work ...

        # ... if this is a mount, dismount first
        if ticket['function'] == "mount":
            message = "mcDoWork> child prepare dismount for %s" % \
                      (common_message,)
            Trace.trace(ACTION_LOG_LEVEL, message)

            # don't print a failure  (no tape mounted) message that is
            # really a success
            self.logdetail = 0
            # To Do: The downside to passing the volume we want to mount is
            # that there is a warning message about the MC expecting to
            # unmount that tape.  Some blank value should be passed to
            # avoid that false warning.
            dismount_ticket = {'work': 'unloadvol',
                               'vol_ticket': ticket['vol_ticket'],
                               'drive_id': ticket['drive_id']}
            sts = self.prepare(dismount_ticket)
            self.logdetail = 1  # back on

            message = "%s returned %s" % (message, sts[2])
            Trace.trace(ACTION_LOG_LEVEL, message)

        message = "mcDoWork> child doing %s" % (common_message,)
        Trace.trace(ACTION_LOG_LEVEL, message)

        sts = function(ticket)  # Call the function!

        message = "mcDoWork> child %s returned %s" % (common_message, sts)
        Trace.trace(ACTION_LOG_LEVEL, message)

        ticket["status"] = sts
        # Send status back to MC parent via pipe then via dispatching_worker
        # and WorkDone ticket so dispatching_worker calls WorkDone().
        ticket['work'] = "WorkDone"

        # There must be a better way to write to the pipe connected to the
        # parent process.  Probably with callback.py.
        msg_e = repr(('0', '0', ticket))
        bytecount = "%08d" % (len(msg_e),)
        try:
            os.write(comm_pipe[1], bytecount)
            os.write(comm_pipe[1], msg_e)
            os.close(comm_pipe[1])
        except (OSError, IOError), msg_e:
            message = "mcDoWork> child %s failed reporting to parent: %s" \
                      % (common_message, str(msg_e))
            Trace.log(e_errors.ERROR, message)
        except:
            Trace.handle_error()

        return

    def WorkDone(self, ticket):
        # remove work from outstanding work list
        self.remove_from_work_list(ticket)
        # log what was done
        status = ticket.get('status', None)
        if status and e_errors.is_ok(status[0]):
            level = e_errors.INFO
        else:
            level = e_errors.ERROR
        common_message = self.get_common_message_string(ticket)
        Trace.log(level, "FINISHED %s returned %s" % (common_message, status))
        # log the new work list
        self.log_work_list(ticket)
        if not ticket.get('no_reply', None):
            self.reply_to_caller(ticket)
        try:
            os.close(self.p[1])
        except:
            e, e_msg, e_tb = sys.exc_info()
            Trace.log(e_errors.ERROR, "WorkDone failed:  %s %s %s" % (e, e_msg, traceback.format_tb(e_tb)))
            del e_tb

    def __init__(self, medch, max_work=1, csc_local=None):

        MediaLoaderMethods.__init__(self, medch, max_work, csc_local)

        Trace.init(self.log_name, 'yes')
        print time.ctime(), 'STARTING'
        self.p = None
        self.p2cread = None
        self.p2cwrite = None
        self.c2pread = None
        self.c2pwrite = None
        self.server = None
        self.slots = None
        self.drives = None
        self.cli_jar_file = None
        self.java_exec = None
        self.ibm_cli_host = None
        self.ibm_cli_u = None
        self.ibm_cli_pw = None
        self.max_work = 1
        self.work_functions.append('updatedb')
        self.work_functions.append('getVolState')
        self.query_functions.remove('getVolState')
        self.work_functions.append('getDriveState')
        self.query_functions.remove('getDriveState')
        # Mark our cached status info as invalid
        self.status_valid = 0
        self.debug = self.mc_config.get('debug', False)
        self.debug_messaging = self.mc_config.get('debug_messaging', False)
        # Read the device name to use.
        self.device_name = self.mc_config.get('device_name', '/dev/changer')

        # Read the value for the timeout on status commands.
        self.status_timeout = self.mc_config.get('status_timeout', 300)

        # Read the value for the timeout on mount commands.
        self.mount_timeout = self.mc_config.get('mount_timeout', 300)
        self.mount_retries = self.mc_config.get('mount_retries', 2)
        self.retry_count = self.mount_retries
        self.sudo_cmd = self.mc_config.get('sudo_cmd', '')

        self.cli_host = self.mc_config.get('remote_cli')  # host where CLI can be run on (used for diagnostics)
        self.use_legacy_status = self.mc_config.get('use_legacy_status', False)
        self.manager = multiprocessing.Manager()
        self.q = multiprocessing.Queue()
        self.mtx_server_started = self.manager.Value('i', 0)
        self.last_updated_db = self.manager.Value('i', 0)

        Trace.log(e_errors.INFO,
                  '%s initialized with device: %s status time limit: %s mount time limit: %s ' %
                  (self.__class__.__name__, self.device_name, self.status_timeout, self.mount_timeout))
        self.start_mtx_server()
        rc = self.status_local()
        if not e_errors.is_ok(rc[0]):
            Trace.alarm(e_errors.ERROR, 'can not get initial status, exiting with %s' % (rc,))
            self.server.terminate()
            sys.exit(1)
        print time.ctime(), 'STARTED'

    def _mtx_server(self, read_pipe, write_pipe, err_pipe):
        mtx.cvar.device = self.device_name
        mtx.cvar.absolute_addressing = 1
        mtx.set_scsi_timeout(self.mount_timeout)
        to = mtx.get_scsi_timeout()
        Trace.log(ACTION_LOG_LEVEL, 'mtx.get_scsi_timeout %s' % (to,))

        mtx.open_device()
        Trace.log(e_errors.INFO, "MTX server started")
        for i in 0, 1, 2:
            try:
                os.close(i)
            except os.error:
                pass
        if os.dup(read_pipe) != 0:
            Trace.log(e_errors.ERROR, '_mtx_server read_pipe bad read dup')
        if os.dup(write_pipe) != 1:
            Trace.log(e_errors.ERROR, '_mtx_server write_pipe bad write dup')
        if os.dup(err_pipe) != 2:
            Trace.log(e_errors.ERROR, '_mtx_server write_pipe bad error dup')
        maxfd = 10  # Max number of file descriptors (os.getdtablesize()???)
        for i in range(3, maxfd):
            try:
                os.close(i)
            except:
                pass
        self.mtx_server_started.value = 1
        Trace.log(e_errors.INFO, "MTX server: Starting loop")
        while True:
            try:
                r, _, _ = select.select([read_pipe], [], [], 60)

            except (select.error, OSError, IOError) as e:
                Trace.log(79, "select error in mtx_server: %s" %
                          (str(e),))

                if e.args[0] in [errno.EINTR]:
                    # The process was interrupted by a signal; we need
                    # to keep it going.
                    continue
                else:
                    # We want to jump to the error handling code.
                    raise sys.exc_info()[0], sys.exc_info()[1], \
                        sys.exc_info()[2]
            except:
                e, e_msg, e_tb = sys.exc_info()
                Trace.log(e_errors.ERROR, "MTX server failed:  %s %s %s" % (e, e_msg, traceback.format_tb(e_tb)))
                return
            if read_pipe not in r:
                continue
            raw_msg = os.read(read_pipe, 2000)
            Trace.trace(ACTION_LOG_LEVEL, "MTX server: received: %s" % (raw_msg,))
            # message must be: 'cmd', 'arg1, arg2, arg3'
            # try to execute:
            pars = raw_msg.strip().split(',')
            cmd = pars[0]
            args = pars[1:len(pars)]
            pid_to_send_back = args[2]
            response = 'pid_%s' % (pid_to_send_back,)
            Trace.log(ACTION_LOG_LEVEL, "MTX server: cmd: %s args %s" % (cmd, args))
            if cmd in ["Load", "Unload"]:
                retry_cnt = self.mount_retries
                while retry_cnt:
                    try:
                        Trace.log(ACTION_LOG_LEVEL, "MTX server: calling load_unload_local")
                        a, b = return_by(self.load_unload_local, (int(args[0]), int(args[1]), cmd), self.mount_timeout)
                        Trace.log(ACTION_LOG_LEVEL, "MTX server: load_unload_local returned %s %s" % (a, b))
                        if -1 == a:
                            Trace.log(ACTION_LOG_LEVEL, ' mtx load / unload timeout')
                            retry_cnt -= 1
                            if retry_cnt == 0:
                                Trace.log(ACTION_LOG_LEVEL, ' mtx load / unload timeout. Exiting retry loop')
                                response = '%s pid_%s' % (e_errors.TIMEDOUT, pid_to_send_back,)
                        else:
                            break
                    except:
                        Trace.log(e_errors.ERROR, 'error in mtx server: %s %s %s' %
                                  (sys.exc_info()[0],
                                   sys.exc_info()[1],
                                   sys.exc_info()[2]))
                        break

            elif cmd == 'status':
                mtx.status()
            elif cmd == 'TestUnitReady':
                mtx.Test_UnitReady()
            print response  # this is a terminator
            sys.stdout.flush()
            sys.stderr.flush()

        Trace.log(e_errors.ERROR, "MTX server exited (should never happen)")

    def start_mtx_server(self):
        self.p2cread, self.p2cwrite = os.pipe()
        self.c2pread, self.c2pwrite = os.pipe()

        self.server = multiprocessing.Process(target=self._mtx_server,
                                              args=(self.p2cread, self.c2pwrite, self.c2pwrite))
        self.server.start()
        t = 0
        while t < self.status_timeout:
            if self.mtx_server_started.value > 0:
                break
            else:
                time.sleep(1)
        if t >= self.status_timeout:
            msg_e = 'mtx server not started %s s. Exiting' % (self.status_timeout,)
            Trace.alarm(e_errors.ERROR, msg_e)
            self.server.terminate()
            os._exit(1)

    def receive_reply(self, end_of_response, timeout):
        #  command_string must be: 'cmd,arg1,arg2,arg3'
        message = ""
        blanks = 0
        start = time.time()
        active = 0
        self.delta_t(0)
        try:
            while active < timeout:
                """
                For some reason is_alive traces back.
                if not self.server.is_alive():
                    # When we detect that the process has exited, leave
                    # the loop.  This allows us to avoid the ETIMEDOUT
                    # and re-raised select errors.
                    break
                """
                # We need to start reading this now for really long responses.
                # Otherwise, the buffer fills up with the child waiting
                # for the parent to read something from the full buffer.
                # And the parent waits for the child to finish.
                wait_duration = max(timeout - active, 0)
                try:
                    r, _, _ = select.select([self.c2pread], [], [], wait_duration)
                except (select.error, OSError, IOError) as e:
                    Trace.log(79, "select error in receive_reply(): %s" %
                              (str(e),))
                    if e.args[0] in [errno.EINTR]:
                        r = []
                        # The process was interrupted by a signal; we need
                        # to keep it going.
                        active = time.time() - start
                        continue
                    else:
                        # We want to jump to the error handling code.
                        raise sys.exc_info()[0], sys.exc_info()[1], \
                            sys.exc_info()[2]

                # If nothing was received, we want to wait again instead of
                # falling into the os.read().  If the robot side hangs
                # without closing the pipe we can time out in select(), but
                # not read().
                if self.c2pread not in r:
                    active = time.time() - start
                    time.sleep(1)
                    continue
                raw_msg = os.read(self.c2pread, 2000)
                if raw_msg:
                    if self.debug_messaging:
                        print 'RAW_MSG', raw_msg
                    if end_of_response in raw_msg:
                        message = message + raw_msg[:raw_msg.find(
                            end_of_response)]  # in the last line leave all, but end_of_response
                        break
                    message = message + raw_msg
                    # Need to reset the timeout period.
                    start = time.time()
                    active = 0
                else:
                    if raw_msg == '':
                        blanks = blanks + 1
                    active = time.time() - start
                    time.sleep(1)
            else:
                # We want to jump to the error handling code.
                Trace.log(e_errors.ERROR, 'mtx server timeout')
                raise select.error(errno.ETIMEDOUT, None)

        except (KeyboardInterrupt, SystemExit):
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except:
            # Log the original error.
            e, e_msg, e_tb = sys.exc_info()
            Trace.handle_error(e, e_msg, e_tb)
            del e_tb  # avoid cyclic references
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        return message

    def send_command(self, command_string, timeout):
        Trace.log(ACTION_LOG_LEVEL, 'send_command: command %s timeout %s' % (command_string, timeout))
        #  command_string must be: 'cmd,arg1,arg2,arg3'
        end_of_response = 'pid_%s' % command_string.split(',')[-1]
        Trace.log(ACTION_LOG_LEVEL, 'send_command: expected end of reply %s' % (end_of_response,))
        os.write(self.p2cwrite, command_string)
        try:
            message = self.receive_reply(end_of_response, timeout)
        except:
            Trace.handle_error()
            return e_errors.ERROR, e_errors.ERROR, [], "", 'receive_reply error'
        if len(message) < 100:  # status command returns a large array, we do not want to log it
            Trace.log(ACTION_LOG_LEVEL, "send_command: message %s" % (message,))
        response = message.split('\012')

        if 'status' in command_string:
            if response[0] == '':
                response.pop(0)  # in case first line in response is empty
            if self.debug:
                ofn = '/var/log/enstore/tmp/enstore/%s_%s.mtx_status.out' % (self.name, end_of_response,)
                Trace.log(e_errors.INFO, 'the output of the status command is in %s' % (ofn,))
                of = open(ofn, 'w')
                for line in response:
                    of.write('%s\n' % (line,))
                of.close()
        else:
            if len(response) < 100:
                Trace.log(ACTION_LOG_LEVEL, "send_command: response %s" % (response,))
        if 'Load' in command_string or 'Unload' in command_string:
            for line in response:
                if 'Failed' in line:
                    return e_errors.ERROR, e_errors.ERROR, '', response
                if e_errors.TIMEDOUT in l:
                    return e_errors.ERROR, e_errors.TIMEDOUT, '', response
            if '' in message:
                return e_errors.OK, e_errors.OK, '', ''
            else:
                return e_errors.ERROR, e_errors.ERROR, [], "", ''
        elif 'status' in command_string:
            # expected first line in reply:
            # Storage Changer /dev/changer:56 Drives, 10295 Slots ( 255 Import/Export )
            if all(s in response[0] for s in ['Storage Changer', 'Drives', 'Slots']):
                return e_errors.OK, e_errors.OK, '', response
            else:
                return e_errors.ERROR, e_errors.ERROR, [], "", 'Wrong reply format'
        elif 'TestUnitReady' in command_string:
            # expected reply:
            # "Ready:yes" or Ready:no
            for line in response:
                if 'Ready' in line:
                    return e_errors.OK, e_errors.OK, '', response
            else:
                return e_errors.ERROR, e_errors.ERROR, '', response
        else:
            return e_errors.ERROR, e_errors.ERROR, [], "", ''

    #########################################################################
    # These functions are overridden from the generic class.
    #########################################################################

    def get_mtx_status(self):
        t = 0
        while t < self.status_timeout:
            if self.mtx_server_started.value > 0:
                break
            else:
                time.sleep(1)
        if t >= self.status_timeout:
            msg_e = 'mtx server not responded in %s s' % (self.status_timeout,)
            Trace.alarm(e_errors.ERROR, msg_e)
            return e_errors.ERROR, e_errors.ERROR, [], '', msg_e
        if self.use_legacy_status:
            result = get_mtx_status(self.device_name)
            if not result:
                Trace.log(e_errors.WARNING, 'get_mtx_status failed')
                return e_errors.ERROR, e_errors.ERROR, [], '', 'get_mtx_status failed'
            rc = (e_errors.OK, e_errors.OK, '', result.split('\n'))
        else:
            rc = self.send_command('status,%s,%s,%s' % ('', '', os.getpid()), self.status_timeout)
        return rc

    # query robot
    def query_robot(self, ticket):
        __pychecker__ = "no-argsused"
        rc = self.robot_status()
        return e_errors.OK, 0, rc, "", ""

    # load volume into the drive;
    def load(self, ticket):
        """
        external_label,    # volume external label
        drive,             # drive id
        media_type):       # media type
        """
        drive = ticket['drive_id']
        external_label = ticket['vol_ticket']['external_label']
        media_type = ticket['vol_ticket'].get('media_type', 'unknown')
        Trace.log(e_errors.INFO,
                  'MTX_MediaLoader: request to load %s of type %s into drive %s' % (external_label, media_type, drive))
        return self.retry_function(self.mtx_mount, external_label,
                                   drive, media_type)

    # unload volume from the drive
    def unload(self, ticket):
        """
        external_label,  # volume external label
        drive,           # drive id
        media_type):     # media type
        """
        drive = ticket['drive_id']
        external_label = ticket['vol_ticket']['external_label']
        media_type = ticket['vol_ticket'].get('media_type', 'unknown')
        Trace.log(e_errors.INFO, 'MTX_MediaLoader: request to unload %s of type %s from drive %s' % (
                  external_label, media_type, drive))
        return self.retry_function(self.mtx_dismount, external_label,
                                   drive, media_type)

    def do_retry(self, ret_val, returned_by_mtx_mount=False):
        retry = False
        update_db = False
        if ret_val[0] == -1:
            return True
        try:
            if ((isinstance(ret_val[3], list) and
                 (('mtx: Request Sense: Sense Key=Unit Attention' in ret_val[3])
                  or ('mtx: Request Sense: Sense Key=Aborted Command' in ret_val[3])
                  or ('mtx: Request Sense: Sense Key=Illegal Request' in ret_val[3])
                  or ('mtx: Request Sense: Sense Key=Not Ready' in ret_val[3])
                  or ret_val[1] == e_errors.TIMEDOUT))):
                if 'mtx: Request Sense: Sense Key=Illegal Request' in ret_val[3]:
                    if returned_by_mtx_mount:
                        update_db = True
                    else:
                        if 'mtx: Request Sense: Additional Sense Code = 3B' in ret_val[3]:
                            if ('mtx: Request Sense: Additional Sense Qualifier = 0D' in ret_val[3] or
                                    'mtx: Request Sense: Additional Sense Qualifier = 11' in ret_val[3]):
                                # Medium Destination Element Full or Media magazine not accessible
                                if self.retry_count == 0:  # retry one more time
                                    self.retry_count = 1
                                update_db = False
                            elif 'mtx: Request Sense: Additional Sense Qualifier = 0E' in ret_val[3]:
                                # Medium Source Element Empty (Looks as the dismount command was successful)
                                update_db = True
                elif 'mtx: Request Sense: Sense Key=Not Ready' in ret_val[3]:
                    Trace.log(ACTION_LOG_LEVEL,
                              'Received Request Sense: Sense Key=Not Ready. Will wait a minute and retry')
                    time.sleep(60)
                elif 'mtx: Request Sense: Sense Key=Unit Attention' in ret_val[3]:
                    sleep_time = 30
                    if 'mtx: Request Sense: Additional Sense Code = 28' in ret_val[3]:
                        # According to IBM recommendations,  wait and retry more times
                        if self.retry_count == 0:
                            self.retry_count = 1
                            if 'mtx: Request Sense: Additional Sense Qualifier = 00' in ret_val[3]:
                                update_db = True
                            sleep_time = 40
                    time.sleep(sleep_time)
                elif ret_val[1] == e_errors.TIMEDOUT:
                    update_db = True
                retry = True
                if update_db:
                    Trace.log(e_errors.INFO, 'will update db')
                    rt = self.updatedb_on_retry()
                    Trace.log(e_errors.INFO, 'update db returned %s' % (rt,))
        except:
            Trace.handle_error()
            retry = True
        if self.retry_count > 0:
            self.retry_count -= 1
        else:
            retry = False
        return retry

    def updatedb_on_retry(self):
        return self.updatedb({'drive': {'address': None}})

    def insert(self, ticket):
        __pychecker__ = "no-argsused"  # When fixed remove this pychecker line.
        Trace.log(ACTION_LOG_LEVEL, 'insert %s' % ticket)
        if ticket.get('external_label'):
            volume_to_insert = ticket.get('external_label')
            s, d = self.locate_volume(ticket['external_label'])
            if d >= 0:
                return (e_errors.ERROR, '%s is in drive %s, can not be inserted' % (
                        ticket['external_label'], self.drives[d]['address']), None, None)
            if s < 0:
                return e_errors.ERROR, e_errors.MC_VOLNOTFOUND, ticket['external_label'], 'Volume not found'
            if 'IMPORT/EXPORT' not in self.slots[s]['location']:
                return (e_errors.ERROR,
                        '%s is not in  IMPORT/EXPORT slot (%s)' % (ticket['external_label'], self.slots[s]['location']),
                        None, None)
        else:
            # Find first not empty Import/export slot
            for imp in self.slots:
                if 'IMPORT/EXPORT' in imp['location'] and EMPTY not in imp['volume']:
                    volume_to_insert = imp['volume']
                    s = self.slots.index(imp)
                    break
            else:
                return e_errors.ERROR, 'Nothing to import', None, None

        self.retry_count = 4
        retry = True
        rc = None
        while retry:
            s_slot, d = self.locate_volume(EMPTY)
            if s_slot < 0:
                return (e_errors.ERROR, '%s can not be inserted, no free slots' % (ticket['external_label'],),
                        None, None)

            stor_el = self.slots[s_slot]
            stor_el['volume'] = BUSY
            imp_el = self.slots[s]
            self.slots[s_slot] = stor_el
            Trace.log(ACTION_LOG_LEVEL, 'insert:  inserting %s from %s to %s' %
                      (volume_to_insert, imp_el['address'], stor_el['address']))
            rc = self.send_command('Unload,%s,%s,%s' % (stor_el['address'], imp_el['address'], os.getpid()),
                                   self.mount_timeout)
            Trace.trace(ACTION_LOG_LEVEL, "SCOMM RETURNED %s" % (rc,))
            if rc[1] == e_errors.OK:
                imp_el['volume'] = EMPTY
                stor_el['volume'] = self.slots[s]['volume']
                self.slots[s] = imp_el
                self.slots[s_slot] = stor_el
                rc = list(rc)
                rc[3] = "Imported %s from %s to %s" % (stor_el['volume'], imp_el['address'], stor_el['address'])
                rc = tuple(rc)
                break
            else:
                Trace.log(e_errors.INFO, "Unload for insert command returned: %s" % (rc,))
                if stor_el['volume'] == BUSY:
                    stor_el['volume'] = EMPTY
                    self.slots[s_slot] = stor_el
                retry = self.do_retry(rc, returned_by_mtx_mount=False)
                if retry:
                    # rc[0] == -1 - timeout

                    Trace.log(e_errors.INFO, 'retrying insert %s %s' % (imp_el, stor_el))
                    time.sleep(1)
                else:
                    break
        return rc

    def eject(self, ticket):
        __pychecker__ = "no-argsused"  # When fixed remove this pychecker line.
        try:
            external_label = ticket['volList'][0]
        except:
            return e_errors.ERROR, 'nothing to eject', '%s' % (ticket,), None
        s_slot, d = self.locate_volume(external_label)
        if d >= 0:
            return (e_errors.ERROR,
                    '%s is in drive %s, can not be ejected' % (external_label, self.drives[d]['address']), None, None)

        if s_slot < 0:
            return e_errors.ERROR, e_errors.MC_VOLNOTFOUND, external_label, 'Volume not found'
        if 'IMPORT/EXPORT' in self.slots[s_slot]['location']:
            return (e_errors.ERROR,
                    '%s is in  IMPORT/EXPORT slot (%s)' % (external_label, self.slots[s_slot]['location']), None, None)

        # Find first empty Import / export slot
        for imp in self.slots:
            if 'IMPORT/EXPORT' in imp['location'] and EMPTY in imp['volume']:
                s = self.slots.index(imp)
                break
        else:
            return e_errors.ERROR, 'No empty IMPORT/EXPORT slots', None, None

        stor_el = self.slots[s_slot]
        imp_el = self.slots[s]
        self.slots[s_slot] = stor_el
        retry_count = 4
        rc = None
        while retry_count > 0:
            rc = self.send_command('Unload,%s,%s,%s' % (imp_el['address'], stor_el['address'], os.getpid()),
                                   self.mount_timeout)
            Trace.trace(ACTION_LOG_LEVEL, "SCOMM RETURNED %s" % (rc,))
            if rc[1] == e_errors.OK:
                imp_el['volume'] = self.slots[s_slot]['volume']
                stor_el['volume'] = EMPTY
                self.slots[s] = imp_el
                self.slots[s_slot] = stor_el
                rc = list(rc)
                rc[3] = "Ejected %s from %s to %s" % (imp_el['volume'], stor_el['address'], imp_el['address'])
                rc = tuple(rc)
                break
            else:
                Trace.log(e_errors.INFO, "Unload for eject command returned: %s" % (rc,))
                if (rc[0] == -1) or ((isinstance(rc[3], list) and
                                      'mtx: Request Sense: Sense Key=Unit Attention' in rc[3])):
                    # rc[0] == -1 - timeout
                    Trace.log(e_errors.INFO, 'retrying eject %s %s' % (imp_el, stor_el))
                    time.sleep(1)
                    retry_count -= 1
                else:
                    break
        return rc

    #########################################################################
    # These functions are internal functions specific to MTX media changer.
    #########################################################################

    # Find the tape and mount it in the drive.
    def mtx_mount(self, volume, drive, _=None, __=None):
        rc = (e_errors.OK, e_errors.OK, [], '', '')
        self.retry_count = 1
        if self.mount_retries > 1:
            self.retry_count = self.mount_retries
        retry = True
        while retry:
            try:
                dr = self.locate_drive(drive)  # index in self.drives
            except:
                Trace.handle_error()
                Trace.log(e_errors.ERROR, 'mtx_mount unrecognized drive: %s' % (drive,))
                return (e_errors.ERROR, e_errors.ERROR, [], '',
                        'mtx_mount unrecognized drive: %s' % (drive,))
            if dr < 0:
                Trace.log(e_errors.ERROR, 'mtx_mount unrecognized drive: %s' % (drive,))
                return (e_errors.ERROR, e_errors.ERROR, [], '',
                        'mtx_mount unrecognized drive: %s' % (drive,))

            s, d = self.locate_volume(volume)  # indices in self.slots, and self.drives correspondingly
            if s == -1:
                if d == -1:
                    Trace.log(e_errors.ERROR,
                              'mtx cant mount tape. Not in library')
                    return (e_errors.ERROR, e_errors.ERROR, [], '',
                            'mtx cant mount tape. Not in library')
                elif dr == d:
                    return e_errors.OK, e_errors.OK, [], '', 'Already in drive %s' % (drive,)
                else:
                    Trace.log(e_errors.ERROR,
                              'mtx cant mount tape. Already in drive %s' % (drive,))
                    return (e_errors.ERROR, e_errors.MC_VOLNOTHOME, [], '',
                            'mtx cant mount tape. Already in drive %d' % (d,))

            if not self.is_empty(dr):
                Trace.log(e_errors.ERROR, 'mtx cant mount tape. Drive %s is not empty: %s' % (dr, self.drives[dr]))
                return (e_errors.ERROR, e_errors.MC_DRVNOTEMPTY, [], '',
                        'mtx cant mount tape. Drive %s is not empty: %s' % (drive, self.drives[dr]['volume']))
            Trace.log(e_errors.INFO, 'found %s in slot %s ...mounting' % (volume, self.slots[s]['address']))
            if not self.test_unit_ready():
                Trace.log(e_errors.ERROR, 'mount: Unit is not ready. Will try anyway')
            rc = self.send_command(
                'Load,%s,%s,%s' % (self.slots[s]['address'], self.drives[dr]['address'], os.getpid()),
                self.mount_timeout * self.mount_retries + 10)
            Trace.trace(ACTION_LOG_LEVEL, "Send Command returned %s" % (rc,))
            if rc[1] == e_errors.OK:
                Trace.trace(ACTION_LOG_LEVEL,
                            'updating DB: slots[%s]=%s drives[%s]=%s ' % (s, self.slots[s], dr, self.drives[dr]))
                mutable_dict = self.slots[s]
                mutable_dict['volume'] = EMPTY
                self.slots[s] = mutable_dict
                mutable_dict = self.drives[dr]
                mutable_dict['volume'] = volume
                self.drives[dr] = mutable_dict
                Trace.trace(ACTION_LOG_LEVEL,
                            'updated DB: slots[%s]=%s drives[%s]=%s' % (s, self.slots[s], dr, self.drives[dr]))
                break
            else:
                Trace.log(e_errors.INFO, "Load returned: %s" % (rc,))
                if rc[1] == e_errors.TIMEDOUT:
                    retry = self.do_retry(rc, returned_by_mtx_mount=True)
                    if retry:
                        Trace.log(e_errors.INFO, 'retrying mtx_mount %s %s' % (volume, drive))
                        time.sleep(1)
                    else:
                        break
        Trace.log(ACTION_LOG_LEVEL, 'mtx_mount returning: %s' % (rc,))
        return rc

    # is tape drive empty
    def is_empty(self, drive_index):
        if 0 == self.status_valid:
            a, b = return_by(self.status_local, (), self.status_timeout)
            if -1 == a:
                Trace.log(e_errors.ERROR, 'mtx status request timeout')
                return False
            self.status_valid = 1

        if self.drives[drive_index]['volume'] == EMPTY:
            return True
        else:
            return False

    # Find a free slot and unmount the tape from the drive.
    def mtx_dismount(self, volume, drive, _=None, __=None):
        rc = (e_errors.OK, e_errors.OK, [], '', '')
        self.retry_count = 1
        if self.mount_retries > 1:
            self.retry_count = self.mount_retries
        retry = True
        failed_dismount_addresses = []
        while retry:
            try:
                dr = self.locate_drive(drive)  # index in self.drives
            except:
                Trace.handle_error()
                Trace.log(e_errors.ERROR, 'mtx_dismount unrecognized drive: %s' % (drive,))
                return (e_errors.ERROR, e_errors.ERROR, [], '',
                        'mtx_dismount unrecognized drive: %s' % (drive,))

            if dr < 0:
                Trace.log(e_errors.ERROR, 'mtx_dismount unrecognized drive: %s' % (drive,))
                return (e_errors.ERROR, e_errors.ERROR, [], '',
                        'mtx_dismount unrecognized drive: %s' % (drive,))

            s, d = self.locate_volume(volume)  # indices in self.slots, and self.drives correspondingly
            if s > -1 and d < 0:
                # not in drive
                return (e_errors.ERROR, e_errors.ERROR, [], '',
                        'mtx unload: Not in drive. In slot %s' % (self.slots[s]['address'],))

            if self.is_empty(dr):  # no need to dismount
                return e_errors.OK, 0, None, "", ""

            s, ignore = self.locate_volume(EMPTY, failed_dismount_addresses)

            if -1 == s:
                Trace.log(e_errors.ERROR, ' mtx unload: No free slots')
                return (e_errors.ERROR, e_errors.ERROR, [], '',
                        'mtx unload: No free slots')

            ignore, d = self.locate_volume(volume)

            if dr != d:
                Trace.log(e_errors.ERROR, ' mtx unload: %s is in %s, not %s' %
                          (volume, self.drives[d]['address'], drive))
                return (e_errors.ERROR, e_errors.ERROR, [], '',
                        'mtx unload: %s is not in %s' %
                        (volume, drive))

            Trace.log(e_errors.INFO, ('found ', volume, ' in drive ', drive,
                                      '...dismounting'))

            s, ignore = self.locate_volume(EMPTY, failed_dismount_addresses)
            if -1 == s:
                Trace.log(e_errors.ERROR, ' mtx unload: No free slots')
                return ('ERROR', e_errors.ERROR, [], '',
                        'mtx unload: No free slots')
            stor_el = self.slots[s]
            stor_el['volume'] = BUSY
            self.slots[s] = stor_el
            if not self.test_unit_ready():
                Trace.log(e_errors.ERROR, 'dismount: Unit is not ready. Will try anyway')
            rc = self.send_command(
                'Unload,%s,%s,%s' % (self.slots[s]['address'], self.drives[dr]['address'], os.getpid()),
                self.mount_timeout * self.mount_retries + 10)
            Trace.trace(ACTION_LOG_LEVEL, "Send Command returned %s" % (rc,))

            if rc[1] == e_errors.OK:
                Trace.trace(ACTION_LOG_LEVEL,
                            'updating DB: slots[%s]=%s drives[%s]=%s' % (s, self.slots[s], dr, self.drives[dr]))
                stor_el = self.slots[s]
                stor_el['volume'] = volume
                self.slots[s] = stor_el
                drive_el = self.drives[dr]
                drive_el['volume'] = EMPTY
                self.drives[dr] = drive_el
                Trace.trace(ACTION_LOG_LEVEL,
                            'updated DB: slots[%s]=%s drives[%s]=%s' % (s, self.slots[s], dr, self.drives[dr]))
                break
            else:
                Trace.log(e_errors.INFO, "Unload command returned: %s" % (rc,))
                if stor_el['volume'] == BUSY:
                    stor_el['volume'] = EMPTY
                retry = self.do_retry(rc, returned_by_mtx_mount=False)
                if retry:
                    failed_dismount_addresses.append(self.slots[s]['address'])
                    Trace.log(e_errors.INFO, 'retrying mtx_dismount %s %s' % (self.drives[dr], stor_el))
                    time.sleep(1)
                else:
                    break
        Trace.trace(ACTION_LOG_LEVEL, "mtx_dismount: returning %s" % (rc,))
        return rc

    # This method indicates where the tape is located within the
    # library by returning two numbers.  The first number is the slot
    # number containing the tape or negative one if the tape is not in
    # a slot.  The second number is the drive number the tape is in or
    # negative one if the tape is not in a drive.  (slots and drives
    # are both indexed starting at zero.)  If both numbers are
    # negative one then the tape is not in the library.  If both
    # numbers are not negative one then there is either a bug in this
    # function or multiple tapes that have the same label in the
    # library.
    def locate_volume(self, vol, ignore_addresses=None):
        if ignore_addresses is None:
            ignore_addresses = []
        Trace.trace(ACTION_LOG_LEVEL, ' looking for volume %s status_valid %s' % (vol, self.status_valid))
        if self.status_valid == 0:
            a, b = return_by(self.status_local, (), self.status_timeout)
            if a == -1:
                # retry
                Trace.log(e_errors.ERROR, 'locate_volume: mtx status request timeout, retrying')
                a, b = return_by(self.status_local, (), self.status_timeout)
                if a == -1:
                    Trace.log(e_errors.ERROR, 'locate_volume: mtx status request timeout')
                    if self.cli_host:
                        # record what is seen using CLI
                        rc = self.setup_cli_command()
                        if not e_errors.is_ok(rc[0]):
                            Trace.log(e_errors.INFO, 'setup_cli_command returned %s' % (rc,))
                            return -1, -1
                        tm = time.localtime(time.time())
                        fn = '/var/log/enstore/tmp/enstore/DataCartridges.%s-%s-%s.%s:%s:%s' % \
                             (tm.tm_year, tm.tm_mon, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec)
                        cmd = '%s -jar %s -ip %s -u %s -p %s --viewDataCartridges' % (
                            self.java_exec, self.cli_jar_file, self.ibm_cli_host, self.ibm_cli_u, self.ibm_cli_pw)
                        res = enstore_functions2.shell_command('enrsh -n %s %s ' % (self.cli_host, cmd,))
                        if not res:
                            Trace.log(e_errors.ERROR, 'viewDataCartridges: IBM CLI returned %s' % (res[1],))
                        else:
                            f = open(fn, 'w')
                            f.write(res[0])
                            f.close()
                            Trace.log(e_errors.INFO, 'viewDataCartridges result is in %s' % (fn,))
                    return -1, -1
            self.status_valid = 1
        found = False
        if vol == EMPTY:
            idx_drive = -1  # we do not need to look for EMPTY volume in drives
        else:
            idx_drive = 0
            d = None
            for d in self.drives:
                try:
                    if vol == d['volume']:
                        found = True
                        break
                except KeyError as e:
                    Trace.log(e_errors.ERROR, 'exception in locate_volume %s %s' % (e, d))
                    return -1, -1
            if found:
                idx_drive = self.drives.index(d)

            if not found:
                idx_drive = -1
            else:
                Trace.trace(ACTION_LOG_LEVEL, 'found in drive %s' % (d['address'],))

        found = False
        idx_slot = 0
        s = None
        for s in self.slots:
            try:
                if vol == s['volume']:
                    if vol == EMPTY:
                        if 'IMPORT/EXPORT' in s['location']:
                            # we do not want to dismount into IMPORT/EXPORT storage element under normal
                            # conditions
                            continue
                    if s['address'] in ignore_addresses:
                        # We do not want to return addresses we've been asked to ignore.
                        continue
                    found = True
                    break
            except KeyError as e:
                Trace.log(e_errors.ERROR, 'exception in locate_volume %s %s' % (e, s))
                return -1, -1

        if found:
            idx_slot = self.slots.index(s)
        if not found:
            idx_slot = -1
        else:
            Trace.trace(ACTION_LOG_LEVEL, 'found in slot %s' % (s['address'],))

        return idx_slot, idx_drive

    def locate_drive(self, drive_address):
        Trace.trace(ACTION_LOG_LEVEL, ' looking for drive %s , status_valid %s' % (drive_address, self.status_valid))
        if self.status_valid == 0:
            a, b = return_by(self.status_local, (), self.status_timeout)
            if a == -1:
                # retry
                Trace.log(e_errors.ERROR, 'locate_drive: mtx status request timeout, retrying')
                a, b = return_by(self.status_local, (), self.status_timeout)
                if a == -1:
                    Trace.log(e_errors.ERROR, 'locate_drive:  mtx status request timeout')
                    if self.cli_host:
                        # record what is seen using CLI
                        rc = self.setup_cli_command()
                        if not e_errors.is_ok(rc[0]):
                            Trace.log(e_errors.INFO, 'setup_cli_command returned %s' % (rc,))
                            return -1, -1
                        tm = time.localtime(time.time())
                        fn = '/var/log/enstore/tmp/enstore/Drives.%s-%s-%s.%s:%s:%s' % \
                             (tm.tm_year, tm.tm_mon, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec)
                        cmd = '%s -jar %s -ip %s -u %s -p %s --viewDriveSummary' % (
                            self.java_exec, self.cli_jar_file, self.ibm_cli_host, self.ibm_cli_u, self.ibm_cli_pw)

                        res = enstore_functions2.shell_command('enrsh -n %s %s ' % (self.cli_host, cmd,))
                        if not res:
                            Trace.log(e_errors.ERROR, 'viewDriveSummary: IBM CLI returned %s' % (res[1],))
                        else:
                            f = open(fn, 'w')
                            f.write(res[0])
                            f.close()
                            Trace.log(e_errors.INFO, 'viewDriveSummary result is in %s' % (fn,))
                    return -1
            self.status_valid = 1
        found = False
        idx_drive = 0
        drive_addr = drive_address
        if isinstance(drive_addr, str) and drive_addr.isdigit():
            drive_addr = int(drive_address)
        if isinstance(drive_addr, int):
            key = 'address'
        else:
            key = 'location'
        d = None
        for d in self.drives:
            if drive_addr == d[key]:
                found = True
                break
        if found:
            idx_drive = self.drives.index(d)

        if not found:
            idx_drive = -1
        return idx_drive

    #  This method tries to have device 'device' load or unload the tape in
    #  or from drive number drive back into slot number 'slot'.  The return
    #  value is anything that MTX printed to stderr.  If mtx hangs,
    #  this method will never return.
    @staticmethod
    def load_unload_local(slot, drive, load_command):
        Trace.log(e_errors.INFO, "MTX %s slot %s drive %s" % (load_command, slot, drive))
        if load_command not in ("Load", "Unload"):
            return 'ERROR', e_errors.ERROR, [], "%s" % (load_command,), "Wrong command"
        if load_command == "Load":
            mtx.Move(slot, drive)
        else:
            mtx.Move(drive, slot)
        ## Below is for mount / dismount failure timeouts testing
        ## if not hasattr(self, 'do_to'):
        ##      self.do_to =  0
        ##      time.sleep(self.mount_timeout + 10)

    # Test if drive is ready
    def test_unit_ready(self):
        for cnt in range(10):
            rc = self.send_command('TestUnitReady,%s,%s,%s' % ('', '', os.getpid()), self.status_timeout)
            Trace.log(ACTION_LOG_LEVEL, 'TestUnitReady returned: %s' % (rc,))
            # rc pattern is (error_code, additional_error_code, message, response)
            # it is inherited from implementation of STK class.
            # error_code is one e_errors codes
            # additional_error_code is implementation specific, but for this class it is usually the same as error_code
            # message is arbitrary message
            # response - response received from mtx server
            if e_errors.is_ok(rc[0]):
                if 'Ready:yes' in rc[3][0]:
                    return True
            time.sleep(10)
        return False

    # This method blocks while it returns the status of the media
    # changer at the specified device.
    # A return value are any messages that mtx printed to stderr.
    # If mtx hangs, this method will never return.
    def status_local(self):
        Trace.log(ACTION_LOG_LEVEL, 'status_local')
        if not self.test_unit_ready():
            msg_e = 'status: Unit is not ready. Will not update database'
            Trace.log(e_errors.ERROR, ' %s' % (msg_e,))
            return e_errors.ERROR, 'msg', ''
        busy_slots = []
        rc = self.get_mtx_status()
        if not e_errors.is_ok(rc[0]):
            Trace.log(e_errors.ERROR, 'get_mtx_status returned: %s' % (rc[0],))
            return e_errors.ERROR, 'get_mtx_status returned: %s' % (rc[0],)
        if hasattr(self, 'slots'):  # clear lists
            # save indexes of busy slots
            for i, slot_d in enumerate(self.slots):
                vol = slot_d.get('volume')
                if vol and vol == BUSY:
                    Trace.log(ACTION_LOG_LEVEL, 'busy slot: %s %s' % (i, slot_d,))
                    busy_slots.append(i)
            Trace.log(ACTION_LOG_LEVEL, 'busy slots: %s' % (busy_slots,))
            del self.slots[:]
            del self.drives[:]
        else:
            self.slots = self.manager.list()
            self.drives = self.manager.list()
        error_string = ''
        lines = rc[3]
        if lines:
            index = 0
            line = lines[index]
            while '' != line:
                try:
                    line = string.strip(line)
                    lel = line.split()
                    if string.find(line, 'Data Transfer Element') != -1:
                        # Expected format:
                        #   Data Transfer Element 257 Phys Loc F2,C1,R2,Z0 SN  00078D2B6E ID ULT3580-TD8 :Empty
                        # or :
                        # Data Transfer Element 270 Phys Loc F2,C4,R4,Z0 SN  00078D2B59 ID ULT3580-TD8 :Full
                        #     (Storage Element 1038 Loaded):VolumeTag = VQ0006L8
                        self.drives.append({})  # mutable dictionary
                        d = self.drives[-1]
                        d['address'] = int(lel[3])
                        loc_and_zone = lel[6].split(':')[0]
                        zone_index = loc_and_zone.find('Z')
                        d['location'] = loc_and_zone[0:zone_index - 1]  # throw ,Z
                        d['zone'] = loc_and_zone[zone_index:len(loc_and_zone)]
                        d['SN'] = lel[8]
                        d['type'] = lel[10].split(':')[0]
                        d['volume'] = EMPTY
                        if string.find(line, 'Empty') != -1:
                            pass  # just to leave status as empty
                        elif string.find(line, 'VolumeTag') != -1:
                            i1 = string.find(line, '=') + 1
                            i2 = len(line)
                            d['volume'] = string.strip(line[i1:i2])
                        else:
                            d['volume'] = 'unlabelled'
                        self.drives[-1] = d
                    elif string.find(line, 'Storage Element') != -1:
                        # Expected format     Storage Element 1025 Phys Loc F08,C02,R01,T00  :Full  :VolumeTag=VQ0033L8
                        # or:
                        # Storage Element 1037 Phys Loc EMPTY :Empty :VolumeTag=
                        self.slots.append({})  # mutable dictionary
                        d = self.slots[-1]
                        d['address'] = int(lel[2])
                        d['location'] = lel[5]
                        d['volume'] = EMPTY
                        if len(self.slots) - 1 in busy_slots:  # this slot was reserved, leave it BUSY
                            d['volume'] = BUSY
                            Trace.log(ACTION_LOG_LEVEL, 'from status: %s' % (line,))
                        if string.find(line, 'Empty') != -1:
                            pass  # just to leave status as empty
                        elif string.find(line, 'VolumeTag') != -1:
                            i1 = string.find(line, '=') + 1
                            i2 = len(line)
                            d['volume'] = string.strip(line[i1:i2])
                        else:
                            d['volume'] = 'unlabelled'
                        self.slots[-1] = d
                    else:
                        Trace.log(ACTION_LOG_LEVEL, 'status_local: can not interpret line: %s' % (line,))
                    index = index + 1
                    line = lines[index]
                except:
                    Trace.handle_error(severity=ACTION_LOG_LEVEL)
            self.status_valid = 1
            self.last_updated_db.value = int(time.time())
        else:
            Trace.log(e_errors.ERROR, 'mtx status returned no result %s' % (rc[0],))
        if error_string:
            rc = (e_errors.ERROR, error_string, '')
        else:
            rc = (e_errors.OK, '', '')
        return rc

    # return status of all drives and slots
    def robot_status(self):
        result = enstore_functions2.shell_command(
            "%s mtx -f %s status | grep 'Data Transfer Element'" % (self.sudo_cmd, self.device_name))
        return result

    # getVolState in the drive; default overridden for other media changers
    def getVolState(self, ticket):
        __pychecker__ = "no-argsused"
        Trace.log(ACTION_LOG_LEVEL, 'getVolstate: %s' % (ticket,))
        retry_count = 2
        slot = -1
        while retry_count > 0:
            ticket['status'] = e_errors.OK
            slot, drive = self.locate_volume(ticket['external_label'])
            Trace.log(ACTION_LOG_LEVEL, 'getVolState slot:%s, drive %s' % (slot, drive,))
            if (slot < 0 and drive < 0) or (slot > 0 and drive >= 0):
                # if volume is not in drive and not in slot
                # or volume is in drive and in slot - retry with inventory
                retry_count -= 1
                if retry_count > 0:
                    self.status_valid = 0  # this will cause an inventory
                    Trace.log(ACTION_LOG_LEVEL, 'getVolState retrying')
                ticket['status'] = e_errors.MC_VOLNOTFOUND
                continue
            elif slot < 0 <= drive:
                ticket['state'] = 'M'
                ticket['phys_location'] = ('%s,%s' % (self.drives[drive]['location'], self.drives[drive]['zone']))
                ticket['location'] = self.drives[drive]['address']
                ticket['SN'] = self.drives[drive]['SN']
            elif slot >= 0:
                if drive < 0 or ticket['external_label'] == EMPTY:
                    ticket['state'] = 'O'
                    ticket['location'] = self.slots[slot]['address']
                    ticket['phys_location'] = self.slots[slot]['location']
            retry_count = 0
        if ticket['external_label'] == EMPTY and ticket.get('reserve'):
            if slot >= 0:
                slot_info = self.slots[slot]
                slot_info['volume'] = BUSY
                self.slots[slot] = slot_info
                ticket['external_label'] = self.slots[slot]['volume']
        Trace.log(ACTION_LOG_LEVEL, 'getVolState: returning %s' % (ticket,))
        return (ticket.get('status'), ticket.get('location'), ticket.get('media_type'), ticket.get('state'),
                ticket.get('external_label'))

    def getDriveState(self, ticket):
        retry_count = 2
        rc = []
        while retry_count > 0:
            ticket['status'] = e_errors.OK
            try:
                drive = self.locate_drive(ticket['drive'])
            except:
                drive = -1
            drive_info = {}
            if drive < 0:
                retry_count -= 1
                if retry_count > 0:
                    self.status_valid = 0  # this will cause an inventory
                    Trace.log(ACTION_LOG_LEVEL, 'getDriveState retrying')
                rc = [e_errors.MC_DRVNOTFOUND, 0, 'No such drive', ticket['drive']]
                drive_info['state'] = 'noinfo'
                drive_info['status'] = 'N/A'
                continue
            if self.drives[drive]['volume'] == EMPTY:
                drive_info['volume'] = ''
            else:
                drive_info['volume'] = self.drives[drive]['volume']
            drive_info['state'] = 'online'
            drive_info['type'] = self.drives[drive]['type']
            drive_info['phys_location'] = ('%s,%s' % (self.drives[drive]['location'], self.drives[drive]['zone']))
            drive_info['location'] = self.drives[drive]['address']
            drive_info['SN'] = self.drives[drive]['SN']
            if drive_info['volume'] == '':
                drive_info['status'] = 'available'
            else:
                drive_info['status'] = 'in use'
            ticket['drive_info'] = drive_info
            rc = [e_errors.OK, self.drives[drive]['location'], '', '']
            retry_count = 0
        return rc

    def listDrives(self, ticket):
        Trace.log(e_errors.INFO, 'MTX listDrives, called %s' % (ticket,))
        drive_list = []
        for d in self.drives:
            try:
                drive_info = copy.copy(d)
                drive_info['name'] = ('%s(%s,%s)' % (d['address'], d['location'], d['zone']))
                drive_info['state'] = 'online'
                drive_info['type'] = d['type']
                if drive_info['volume'] == EMPTY:
                    drive_info['status'] = 'available'
                    drive_info['volume'] = ''
                else:
                    drive_info['status'] = 'in use'
                drive_list.append(drive_info)
            except KeyError as e:
                Trace.log(e_errors.ERROR, 'exception in listDrives %s %s' % (e, d))
                return e_errors.ERROR, 0, None
        ticket['drive_list'] = drive_list
        return e_errors.OK, 0, None

    def dumpdb(self):
        """
        dump the content of the drive-volume lists into a file
        """
        t = time.strftime('%Y-%m-%d-%H:%M', time.localtime())
        with open('/var/log/enstore/tmp/enstore/%s_%s.mtx_dbdump.out' % (self.name, t,), 'w') as of:
            try:
                of.write(self._listVolumes())
            except:
                pass

    def updatedb(self, ticket):
        Trace.trace(ACTION_LOG_LEVEL, 'updatedb: ticket %s' % (ticket,))
        rc = [e_errors.OK, 0, '', '']
        if not ticket['drive']['address']:
            dt = int(time.time()) - self.last_updated_db.value
            if dt > 40:
                # request to re-load status information
                Trace.log(e_errors.INFO, 'Starting robot inventory')
                a, b = return_by(self.status_local, (), self.status_timeout)
                Trace.log(e_errors.INFO, 'Robot inventory finished')
                if -1 == a:
                    Trace.log(e_errors.ERROR, ' mtx status request timeout')
                    rc[0] = e_errors.ERROR
                rc[3] = 'Robot inventory finished'
                if self.debug:
                    self.dumpdb()
            else:
                # do not update so frequently
                rc[3] = 'Last updated %s seconds ago' % (dt,)
            return rc

        drive = self.locate_drive(ticket['drive']['address'])
        Trace.trace(ACTION_LOG_LEVEL, 'updatedb: drive index %s' % (drive,))
        if drive < 0:
            return [e_errors.ERROR, 0, 'No such drive', 'No such drive']
        drive_info = self.drives[drive]

        found = False
        s = None
        for s in self.slots:
            try:
                if ticket['volume']['address'] == s['address']:
                    found = True
                    break
            except:
                Trace.handle_error()
                return [e_errors.ERROR, 0, 'exception in updatedb']

        Trace.trace(ACTION_LOG_LEVEL, 'updatedb: slot %s' % (s,))
        if found:
            idx_slot = self.slots.index(s)
        else:
            return [e_errors.ERROR, 0, 'No slot with address %s' % (ticket['volume']['address'],), 'No such slot']
        slot_info = self.slots[idx_slot]
        Trace.trace(ACTION_LOG_LEVEL, 'updatedb: slot[%s] %s' % (idx_slot, slot_info,))

        drive_info['volume'] = ticket['drive']['volume']
        slot_info['volume'] = ticket['volume']['volume']
        self.drives[drive] = drive_info
        self.slots[idx_slot] = slot_info
        Trace.trace(ACTION_LOG_LEVEL, 'updatedb: returning %s' % (rc,))
        return rc

    def listSlots(self, ticket):
        Trace.trace(ACTION_LOG_LEVEL, 'listSlots')
        slot_list = []
        total = len(self.slots)
        used = 0
        free = 0
        for slot in self.slots:
            try:
                if slot['volume'] == EMPTY:
                    free += 1
                else:
                    used += 1
            except:
                Trace.handle_error()
        slot_list.append({'media_type': 'all',
                          'total': total,
                          'used': used,
                          'free': free,
                          'disabled': 0,
                          'location': 0})
        ticket['slot_list'] = slot_list
        return e_errors.OK, 0, None

    def setup_cli_command(self):
        """
        make up IBM CLI command
        """

        # Check if CLI host is configured
        if not self.cli_host:
            return e_errors.ERROR, 0, None, 'No CLI host specified, check configuration', ''

        # Check if CLI hos has java and TS4500CLI.jar installed
        if not hasattr(self, 'cli_jar_file'):
            cmd = 'enrsh -n %s %s' % (self.cli_host, 'locate TS4500CLI.jar')

            res = enstore_functions2.shell_command(cmd)
            if res[0]:
                self.cli_jar_file = res[0][:-1]
            else:
                return e_errors.ERROR, 0, None, 'No TS4500CLI.jar found', ''

        if not hasattr(self, 'java_exec'):
            res = enstore_functions2.shell_command('enrsh -n %s %s' % (self.cli_host, 'type -p java'))
            if res[0]:
                self.java_exec = res[0][:-1]
            else:
                return e_errors.ERROR, 0, None, 'No java found', ''

        if not hasattr(self, 'ibm_cli_host'):
            res = enstore_functions2.shell_command(
                'enrsh -n %s %s' % (self.cli_host, 'locate %s.IBMCLI' % self.name,))
            if res[0]:
                cli_file = res[0][:-1]
            else:
                return e_errors.ERROR, 0, None, 'No %s.IBMCLI found' % (self.name,), ''
            res = enstore_functions2.shell_command('enrsh -n %s %s' % (self.cli_host, 'cat %s' % cli_file,))
            if res[0]:
                try:
                    # the exec() below defines IBMCL* vars if they are defined in cli_file
                    # what could go wrong?
                    if 'IBMCLIHOST' not in vars():
                        # noinspection PyPep8Naming
                        IBMCLIHOST = None
                    if 'IBMCLIU' not in vars():
                        # noinspection PyPep8Naming
                        IBMCLIU = None
                    if 'IBMCLIPW' not in vars():
                        # noinspection PyPep8Naming
                        IBMCLIPW = None
                    exec (res[0])
                    # noinspection PyUnboundLocalVariable
                    self.ibm_cli_host = IBMCLIHOST
                    # noinspection PyUnboundLocalVariable
                    self.ibm_cli_u = IBMCLIU
                    # noinspection PyUnboundLocalVariable
                    self.ibm_cli_pw = IBMCLIPW
                except:
                    Trace.handle_error()
                    return e_errors.ERROR, 0, None, 'Error executing %s.IBMCLI' % (self.name,), ''
            else:
                return e_errors.ERROR, 0, None, 'Error reading %s.IBMCLI' % (self.name,), ''

        return e_errors.OK, 0, None, '', ''

    def listClean(self, ticket):
        # this command uses IBM CLI to list cleaning tapes.

        rc = self.setup_cli_command()
        ticket['status'] = rc

        if not e_errors.is_ok(ticket):
            self.reply_to_caller(ticket)
            return

        # Send reply and Establish the connection first.
        ticket['status'] = (e_errors.OK, 0, None, "", "")
        reply = copy.copy(ticket)
        self.reply_to_caller(reply)
        try:
            hostinfo = socket.getaddrinfo(ticket['callback_addr'][0], None)
            sock = socket.socket(hostinfo[0][0], socket.SOCK_STREAM)
            sock.connect(ticket['callback_addr'])
        except:
            Trace.handle_error()
            Trace.log(e_errors.ERROR,
                      "Callback address %s" % (ticket['callback_addr'],))
            return e_errors.NET_ERROR, 0, str(sys.exc_info()[1]), "", ""

        ticket['no_reply'] = 1  # Tell WorkDone() not to send the ticket again.
        reply = ticket.copy()  # Make a copy to keep things clean.  But why?

        cmd = '%s -jar %s -ip %s -u %s -p %s --viewCleaningCartridges' % (
            self.java_exec, self.cli_jar_file, self.ibm_cli_host, self.ibm_cli_u, self.ibm_cli_pw)
        res = enstore_functions2.shell_command('enrsh -n %s %s ' % (self.cli_host, cmd,))
        if not res:
            ticket['status'] = (e_errors.ERROR, 0, None, 'IBM CLI returned %s' % (res[1],), '')
            Trace.log(e_errors.ERROR, 'QUERY_CLEAN: IBM CLI returned %s' % (res[1],))
            return e_errors.ERROR, 0, None, 'IBM CLI returned %s' % (res[1],), ''
        reply['clean_list'] = res[0]
        reply['MC_class'] = self.__class__.__name__

        try:
            r = callback.write_tcp_obj(sock, reply)
            sock.close()
            if r:
                Trace.log(e_errors.ERROR,
                          "Error calling write_tcp_obj. Callback addr. %s"
                          % (ticket['callback_addr'],))
        except:
            Trace.handle_error()
            Trace.log(e_errors.ERROR,
                      "Callback address %s" % (ticket['callback_addr'],))

            return e_errors.NET_ERROR, 0, str(sys.exc_info()[1]), "", ""
        return e_errors.OK, 0, None, "", ""

    # return formatted text containing volume information
    def _listVolumes(self):
        msg_e = '     volume          state             location\n'
        for d in self.drives:
            if d['volume'] != EMPTY:
                msg_e += '%12s %12s %12s (%s,%s)\n' % \
                         (d['volume'],
                          'in drive',
                          d['address'],
                          d['location'],
                          d['zone'])
        for s in self.slots:
            msg_e += '%12s %12s %12s (%s)\n' % \
                     (s['volume'],
                      'home',
                      s['address'],
                      s['location'])
        return msg_e

    def listVolumes(self, ticket):
        # Send reply and Establish the connection first.
        ticket['status'] = (e_errors.OK, 0, None, "", "")
        reply = copy.copy(ticket)
        self.reply_to_caller(reply)
        try:
            hostinfo = socket.getaddrinfo(ticket['callback_addr'][0], None)
            sock = socket.socket(hostinfo[0][0], socket.SOCK_STREAM)
            sock.connect(ticket['callback_addr'])
        except:
            Trace.handle_error()
            Trace.log(e_errors.ERROR,
                      "Callback address %s" % (ticket['callback_addr'],))
            return e_errors.NET_ERROR, 0, str(sys.exc_info()[1]), "", ""

        ticket['no_reply'] = 1  # Tell WorkDone() not to send the ticket again.
        reply = ticket.copy()  # Make a copy to keep things clean.  But why?
        reply['volume_list'] = self._listVolumes()
        reply['MC_class'] = self.__class__.__name__
        try:
            r = callback.write_tcp_obj(sock, reply)
            sock.close()
            if r:
                Trace.log(e_errors.ERROR,
                          "Error calling write_tcp_obj. Callback addr. %s"
                          % (ticket['callback_addr'],))
        except:
            Trace.handle_error()
            Trace.log(e_errors.ERROR,
                      "Callback address %s" % (ticket['callback_addr'],))

            return e_errors.NET_ERROR, 0, str(sys.exc_info()[1]), "", ""
        return e_errors.OK, 0, None, "", ""


############################################################
## This class is for drive and slot addresses matching with IBM addresses
## It uses direct calls to mtx routines via SWIG interface
## The class is specifically created for Spectra Logic tape libraries
## It repeats MTXN_MediaLoader class but with some changes allowing to
## serialize incoming requests which can not be performed in parallel
#############################################################
class MTXN_MediaLoaderSL(MediaLoaderMethods):
    def update_db(self, ticket):
        Trace.trace(ACTION_LOG_LEVEL, 'update_db called with %s' % (ticket,))
        ticket['function'] = 'updatedb'
        # Make sure this request has all the necessary information included.
        return self.DoWork(self.updatedb, ticket)

    def DoWork(self, function, ticket):

        # The function immediately called by dispatching worker should have
        # set a 'function' field in the ticket.
        Trace.trace(ACTION_LOG_LEVEL, 'DoWork received %s %s' % (function, ticket))
        if 'function' not in ticket:
            err_msg = "MISSING FUNCTION KEY"
            ticket['status'] = (e_errors.MALFORMED, 0, err_msg)
            Trace.log(e_errors.ERROR, "%s" % (ticket['status'],))
            return ticket['status']

        # if this a duplicate request, drop it
        if self.exists_in_work_list(ticket):
            message = "duplicate request, drop it %s %s" % \
                      (repr(ticket['r_a']), repr(ticket['r_a']))
            Trace.trace(ACTION_LOG_LEVEL, message)
            return

        # Output what we intend to do.
        common_message = self.get_common_message_string(ticket)
        Trace.trace(ACTION_LOG_LEVEL, "MTXN_MediaLoaderSL: REQUESTED %s, %s %s" % (
            common_message, len(self.work_query_list), self.max_work))

        ###
        ### Determine if we need to drop the request for one reason or another.
        ###

        # Let work list length exceed max_work for cleanCycle.
        if ticket['function'] in ('cleanCycle', 'listClean', 'listVolumes', 'listDrives'):
            pass
        # elif ticket['function'] in ["homeAndRestart"]:
        #    pass
        elif ticket['function'] in ('mount', 'dismount', 'eject', 'insert'):
            # If we have max number of working children processes, assume
            # client will resend the request.
            if len(self.work_list) >= self.max_work:
                message = "MC Overflow: %s %s" % \
                          (repr(self.max_work), common_message)
                Trace.log(e_errors.INFO, message)

                # Need to call reply_to_caller() here since the request has
                # not been processed far enough for WorkDone() to reply
                # for us.
                ticket['status'] = (e_errors.MC_QUEUE_FULL, len(self.work_list),
                                    "retry later")
                self.reply_to_caller(ticket)
                return
            # Else if the work queue is temporarily closed, assume client
            # will resend the request.
            elif self.workQueueClosed and len(self.work_list) > 0:
                message = "MC Queue Closed: %s %s" % \
                          (repr(len(self.work_list)), common_message)
                Trace.log(e_errors.INFO, message)
                return

            # If drive is doing a clean cycle; drop request and assume client
            # will resend the request.
            if ticket['function'] in ("mount", "dismount"):
                for i in self.work_list:
                    try:
                        if (i['function'] == "cleanCycle" and
                                'drive_id' in i and
                                i['drive_id'] == ticket['drive_id']):
                            message = "Dropped %s request for %s in %s, " \
                                      "drive in cleaning cycle." % \
                                      (ticket['function'],
                                       ticket['vol_ticket']['external_label'],
                                       ticket['drive_id'])
                            Trace.log(e_errors.INFO, message)
                            return
                    except:
                        Trace.handle_error()
                        Trace.log(e_errors.ERROR, "ERROR %s" % (i,))
        else:  # query requests
            if len(self.work_query_list) >= self.max_work:
                # If the request work list is full, drop request and assume
                # the client will resend the request.
                return

        ### Otherwise, we can process work.

        # Output what we are going to do.
        Trace.log(e_errors.INFO, "PROCESSING %s" % (common_message,))

        # If function is insert and queue not empty: close work queue and
        # set values to prepare for completing this operation once all
        # pending requests are fulfilled.
        if ticket['function'] in ('insert', 'updatedb', 'getVolState', 'getDriveState'):
            Trace.log(ACTION_LOG_LEVEL, 'work_queue size %s' % (len(self.work_list),))
            if len(self.work_list) > 0:
                self.workQueueClosed = 1
                self.timeInsert = time.time()
                self.insertRA = ticket['r_a']
                return
            else:
                self.workQueueClosed = 0

        # If not a duplicate request or dropped request; fork the work.
        self.p = os.pipe()
        cmd_executor = multiprocessing.Process(target=self.executor, args=(function, ticket, self.p, common_message))
        self.add_select_fd(self.p[0])  # wait for reading half of pipe.
        # add entry to outstanding work
        if not ticket['function'] in ('listClean', 'listVolumes'):
            self.add_to_work_list(ticket)
        # log the new work list
        self.log_work_list(ticket)
        cmd_executor.start()
        return

    def executor(self, function, ticket, comm_pipe, common_message):
        """
        Execute incoming request.
        Runs in a child process
        """
        message = "executor> child begin %s" % (common_message,)
        Trace.log(ACTION_LOG_LEVEL, message)
        if hasattr(self, 'mcc'):
            self.mcc.u.reinit()  # this is needed to have process bound UDPClient
        # do the work ...

        # ... if this is a mount, dismount first
        if ticket['function'] == "mount":
            message = "executor> child prepare dismount for %s" % \
                      (common_message,)
            Trace.trace(ACTION_LOG_LEVEL, message)

            # don't print a failure  (no tape mounted) message that is
            # really a success
            self.logdetail = 0
            # To Do: The downside to passing the volume we want to mount is
            # that there is a warning message about the MC expecting to
            # unmount that tape.  Some blank value should be passed to
            # avoid that false warning.
            dismount_ticket = {'work': 'unloadvol',
                               'vol_ticket': ticket['vol_ticket'],
                               'drive_id': ticket['drive_id']}
            sts = self.prepare(dismount_ticket)
            self.logdetail = 1  # back on

            message = "%s returned %s" % (message, sts[2])
            Trace.trace(ACTION_LOG_LEVEL, message)

        message = "executor> child doing %s" % (common_message,)
        Trace.trace(ACTION_LOG_LEVEL, message)

        sts = function(ticket)  # Call the function!

        message = "executor> child %s returned %s" % (common_message, sts)
        Trace.trace(ACTION_LOG_LEVEL, message)

        ticket["status"] = sts
        # Send status back to MC parent via pipe then via dispatching_worker
        # and WorkDone ticket so dispatching_worker calls WorkDone().
        ticket['work'] = "WorkDone"
        Trace.trace(ACTION_LOG_LEVEL, 'executor> ticket %s' % (ticket,))

        # There must be a better way to write to the pipe connected to the
        # parent process.  Probably with callback.py.
        msg_e = repr(('0', '0', ticket))
        bytecount = "%08d" % (len(msg_e),)
        try:
            os.write(comm_pipe[1], bytecount)
            os.write(comm_pipe[1], msg_e)
            os.close(comm_pipe[1])
        except (OSError, IOError), msg_e:
            message = "executor> child %s failed reporting to parent: %s" \
                      % (common_message, str(msg_e))
            Trace.log(e_errors.ERROR, message)
        except:
            Trace.handle_error()

        return

    def WorkDone(self, ticket):
        # remove work from outstanding work list
        Trace.trace(ACTION_LOG_LEVEL, 'WorkDone: ticket %s' % (ticket,))
        self.remove_from_work_list(ticket)
        # log what was done
        status = ticket.get('status', None)
        if status and e_errors.is_ok(status[0]):
            level = e_errors.INFO
        else:
            level = e_errors.ERROR
        common_message = self.get_common_message_string(ticket)
        Trace.log(level, "FINISHED %s returned %s" % (common_message, status))
        if any(f in common_message for f in ('mount', 'updatedb', 'insert', 'eject')):
            self.mount_dismount_in_progress.value = 0
        # log the new work list
        self.log_work_list(ticket)
        if not ticket.get('no_reply', None):
            self.reply_to_caller(ticket)
        try:
            os.close(self.p[1])
        except:
            e, e_msg, e_tb = sys.exc_info()
            Trace.log(e_errors.ERROR, "WorkDone failed:  %s %s %s" % (e, e_msg, traceback.format_tb(e_tb)))
            del e_tb

    def __init__(self, medch, max_work=1, csc_local=None):
        MediaLoaderMethods.__init__(self, medch, max_work, csc_local)
        Trace.init(self.log_name, 'yes')
        print time.ctime(), 'STARTING'
        self.p = None
        self.p2cread = None
        self.p2cwrite = None
        self.c2pread = None
        self.c2pwrite = None
        self.server = None
        self.slots = None
        self.drives = None
        self.max_work = 1
        self.work_functions.append('updatedb')
        self.work_functions.append('getVolState')
        self.query_functions.remove('getVolState')
        self.work_functions.append('getDriveState')
        self.query_functions.remove('getDriveState')
        # Mark our cached status info as invalid
        self.status_valid = 0
        self.debug = self.mc_config.get('debug', False)
        self.debug_messaging = self.mc_config.get('debug_messaging', False)
        # Read the device name to use.
        self.device_name = self.mc_config.get('device_name', '/dev/changer')

        # Read the value for the timeout on status commands.
        self.status_timeout = self.mc_config.get('status_timeout', 300)

        # Read the value for the timeout on mount commands.
        self.mount_timeout = self.mc_config.get('mount_timeout', 300)
        self.mount_retries = self.mc_config.get('mount_retries', 2)
        self.retry_count = self.mount_retries
        self.sudo_cmd = self.mc_config.get('sudo_cmd', '')

        self.cli_host = self.mc_config.get('remote_cli')  # host where CLI can be run on (used for diagnostics)
        self.use_legacy_status = self.mc_config.get('use_legacy_status', False)
        self.manager = multiprocessing.Manager()
        self.mtx_server_started = self.manager.Value('i', 0)
        self.last_updated_db = self.manager.Value('i', 0)
        self.mount_dismount_queue = []
        self.mount_dismount_qs = 50
        self.mount_dismount_cnt = 0
        self.rcv_timeout = 2  # override UDPServer setting

        self.mount_dismount_in_progress = self.manager.Value("i", 0)
        self.dw_loop_lock = multiprocessing.Lock()  # used to suspend dispatching worker loop
        self.loop_to = 1

        Trace.log(e_errors.INFO,
                  '%s initialized with device: %s status time limit: %s mount time limit: %s ' %
                  (self.__class__.__name__, self.device_name, self.status_timeout, self.mount_timeout))
        self.start_mtx_server()
        rc = self.status_local()
        if not e_errors.is_ok(rc[0]):
            Trace.alarm(e_errors.ERROR, 'can not get initial status, exiting with %s' % (rc,))
            self.server.terminate()
            sys.exit(1)
        print time.ctime(), 'STARTED'

    def get_request(self):
        """
        Get request, coming from the client.
        Overrides class:`dispatching_worker.get_request`

        :rtype: :obj:`tuple` (:obj:`str` - message, :obj:`tuple` (:obj:`str`- IP address, :obj:`int` - port)
            - client address)
        """
        Trace.trace(6, 'get_request my %s %s %s' % (
            len(self.mount_dismount_queue), self.mount_dismount_in_progress.value, len(self.work_list)))
        if self.mount_dismount_in_progress.value and not self.work_list:
            self.mount_dismount_in_progress.value = 0  # reset flag
        Trace.trace(6, 'get_request my %s %s' % (len(self.mount_dismount_queue), self.mount_dismount_in_progress.value))
        Trace.trace(6, 'get_request: calling _get_request, work_list %s' % (self.work_list,))
        if self.use_raw:
            request, client_address = self._get_request_multi()
        else:
            request, client_address = self._get_request_single()

        if request:
            rq_pyobj = udp_common.r_eval(request)
            Trace.trace(6, 'RQ %s' % (rq_pyobj,))
            try:
                work = rq_pyobj[2].get('work')
            except Exception as e:
                Trace.log(e_errors.ERROR, 'exception %s processing %s' % (str(e), rq_pyobj))
                return request, client_address
            if work in ('loadvol', 'unloadvol', 'update_db', 'insertvol', 'ejectvol'):
                if len(self.mount_dismount_queue) < self.mount_dismount_qs:
                    for entry in self.mount_dismount_queue:
                        if client_address == entry[1]:
                            break
                    else:
                        if work in ('loadvol', 'unloadvol'):
                            self.mount_dismount_queue.append((request, client_address))  # append only unique request
                        else:
                            self.mount_dismount_queue.insert(0, (request, client_address))  # higher priority
                    request, client_address = None, None
                else:
                    return request, client_address

        if not self.mount_dismount_in_progress.value and len(self.mount_dismount_queue) > 0:
            self.mount_dismount_in_progress.value = 1
            rc = self.mount_dismount_queue.pop(0)
            Trace.trace(6, 'get_request my will return %s' % (rc,))
            return rc
        return request, client_address

    def serve_forever(self):
        """
        Overrides class:`dispatching_worker.serve_forever`
        """
        Trace.log(e_errors.INFO, "Requests starting %s TO %s" % (self, self.loop_to))
        count = 0
        while not self.is_child:
            self.do_one_request()
            self.collect_children()
            count = count + 1
            if count > 20:
                self.purge_stale_entries()
                count = 0
            if self.loop_to:
                time.sleep(self.loop_to)

        if self.is_child:
            Trace.trace(6, "serve_forever, child process exiting")
            os._exit(0)  # in case the child process doesn't explicitly exit
        else:
            Trace.trace(6, "serve_forever, shouldn't get here")

    def _mtx_server(self, read_pipe, write_pipe, err_pipe):
        mtx.cvar.device = self.device_name
        mtx.cvar.absolute_addressing = 1
        mtx.set_scsi_timeout(self.mount_timeout)
        to = mtx.get_scsi_timeout()
        Trace.log(ACTION_LOG_LEVEL, 'mtx.get_scsi_timeout %s' % (to,))

        mtx.open_device()
        Trace.log(e_errors.INFO, "MTX server started")
        for i in 0, 1, 2:
            try:
                os.close(i)
            except os.error:
                pass
        if os.dup(read_pipe) != 0:
            Trace.log(e_errors.ERROR, '_mtx_server read_pipe bad read dup')
        if os.dup(write_pipe) != 1:
            Trace.log(e_errors.ERROR, '_mtx_server write_pipe bad write dup')
        if os.dup(err_pipe) != 2:
            Trace.log(e_errors.ERROR, '_mtx_server write_pipe bad error dup')
        maxfd = 10  # Max number of file descriptors (os.getdtablesize()???)
        for i in range(3, maxfd):
            try:
                os.close(i)
            except:
                pass
        self.mtx_server_started.value = 1
        Trace.log(e_errors.INFO, "MTX server: Starting loop")
        while True:
            try:
                r, _, _ = select.select([read_pipe], [], [], 60)
            except (select.error, OSError, IOError) as e:
                Trace.log(79, "select error in mtx_server: %s" %
                          (str(e),))

                if e.args[0] in [errno.EINTR]:
                    r = []
                    # The process was interrupted by a signal; we need
                    # to keep it going.
                    continue
                else:
                    # We want to jump to the error handling code.
                    raise sys.exc_info()[0], sys.exc_info()[1], \
                        sys.exc_info()[2]
            except:
                e, e_msg, e_tb = sys.exc_info()
                Trace.log(e_errors.ERROR, "MTX server failed:  %s %s %s" % (e, e_msg, traceback.format_tb(e_tb)))
                return
            if read_pipe not in r:
                continue
            raw_msg = os.read(read_pipe, 2000)
            Trace.trace(ACTION_LOG_LEVEL, "MTX server: received: %s" % (raw_msg,))
            # message must be: 'cmd', 'arg1, arg2, arg3'
            # try to execute:
            pars = raw_msg.strip().split(',')
            cmd = pars[0]
            args = pars[1:len(pars)]
            pid_to_send_back = args[2]
            response = 'pid_%s' % (pid_to_send_back,)
            Trace.log(ACTION_LOG_LEVEL, "MTX server: cmd: %s args %s" % (cmd, args))
            # func = getattr(self,cmd)
            if cmd in ["Load", "Unload"]:
                retry_cnt = self.mount_retries
                start_time = time.time()
                while retry_cnt:
                    try:
                        Trace.log(ACTION_LOG_LEVEL,
                                  "MTX server: calling load_unload_local. to=%s" % (self.mount_timeout,))
                        a, b = return_by(self.load_unload_local, (int(args[0]), int(args[1]), cmd), self.mount_timeout)
                        Trace.log(ACTION_LOG_LEVEL,
                                  "MTX server: load_unload_local returned %s %s. Execution time %s" % (
                                      a, b, time.time() - start_time))
                        if -1 == a:
                            Trace.log(ACTION_LOG_LEVEL, ' mtx load / unload timeout')
                            retry_cnt -= 1
                            if retry_cnt == 0:
                                Trace.log(ACTION_LOG_LEVEL, ' mtx load / unload timeout. Exiting retry loop')
                                response = '%s pid_%s' % (e_errors.TIMEDOUT, pid_to_send_back,)
                        else:
                            break
                    except:
                        Trace.log(e_errors.ERROR, 'error in mtx server: %s %s %s' %
                                  (sys.exc_info()[0],
                                   sys.exc_info()[1],
                                   sys.exc_info()[2]))
                        break

            elif cmd == 'status':
                mtx.status()
            elif cmd == 'TestUnitReady':
                mtx.Test_UnitReady()
            print response  # this is a terminator
            sys.stdout.flush()
            sys.stderr.flush()

        Trace.log(e_errors.ERROR, "MTX server exited (should never happen)")

    def start_mtx_server(self):
        self.p2cread, self.p2cwrite = os.pipe()
        self.c2pread, self.c2pwrite = os.pipe()
        self.server = multiprocessing.Process(target=self._mtx_server,
                                              args=(self.p2cread, self.c2pwrite, self.c2pwrite))

        self.server.start()
        t = 0
        while t < self.status_timeout:
            if self.mtx_server_started.value > 0:
                break
            else:
                time.sleep(1)
        if t >= self.status_timeout:
            msg_e = 'mtx server not started %s s. Exiting' % (self.status_timeout,)
            Trace.alarm(e_errors.ERROR, msg_e)
            self.server.terminate()
            os._exit(1)

    def receive_reply(self, end_of_response, timeout):
        #  command_string must be: 'cmd,arg1,arg2,arg3'
        message = ""
        blanks = 0
        start = time.time()
        active = 0
        try:
            while active < timeout:
                """
                For some reason is_alive traces back and
                if not self.server.is_alive():
                    # When we detect that the process has exited, leave
                    # the loop.  This allows us to avoid the ETIMEDOUT
                    # and re-raised select errors.
                    break
                """
                # We need to start reading this now for really long responses.
                # Otherwise, the buffer fills up with the child waiting
                # for the parent to read something from the full buffer.
                # And the parent waits for the child to finish.
                wait_duration = max(timeout - active, 0)
                try:
                    r, w, x = select.select([self.c2pread], [], [], wait_duration)
                except (select.error, OSError, IOError) as msg_e:
                    Trace.log(79, "select error in receive_reply(): %s" %
                              (str(msg_e),))
                    if msg_e.args[0] in [errno.EINTR]:
                        # The process was interrupted by a signal; we need
                        # to keep it going.
                        active = time.time() - start
                        continue
                    else:
                        # We want to jump to the error handling code.
                        raise sys.exc_info()[0], sys.exc_info()[1], \
                            sys.exc_info()[2]

                # If nothing was received, we want to wait again instead of
                # falling into the os.read().  If the robot side hangs
                # without closing the pipe we can time out in select(), but
                # not read().
                if self.c2pread not in r:
                    active = time.time() - start
                    time.sleep(1)
                    continue
                raw_msg = os.read(self.c2pread, 2000)
                if raw_msg:
                    if self.debug_messaging:
                        print 'RAW_MSG', raw_msg
                    if end_of_response in raw_msg:
                        message = message + raw_msg[:raw_msg.find(
                            end_of_response)]  # in the last line leave all, but end_of_response
                        break
                    message = message + raw_msg
                    # Need to reset the timeout period.
                    start = time.time()
                    active = 0
                else:
                    if raw_msg == '':
                        blanks = blanks + 1
                    active = time.time() - start
                    time.sleep(1)
            else:
                # We want to jump to the error handling code.
                Trace.log(e_errors.ERROR, ' mtx server timeout')
                raise select.error(errno.ETIMEDOUT, None)

        except (KeyboardInterrupt, SystemExit):
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except:
            # Log the original error.
            e, e_msg, e_tb = sys.exc_info()
            Trace.handle_error(e, e_msg, e_tb)
            del e_tb  # avoid cyclic references
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        return message

    def send_command(self, command_string, timeout):
        Trace.log(ACTION_LOG_LEVEL, 'send_command: command %s timeout %s' % (command_string, timeout))
        #  command_string must be: 'cmd,arg1,arg2,arg3'
        end_of_response = 'pid_%s' % command_string.split(',')[-1]
        Trace.log(ACTION_LOG_LEVEL, 'send_command: expected end of reply %s' % (end_of_response,))
        os.write(self.p2cwrite, command_string)
        try:
            message = self.receive_reply(end_of_response, timeout)
        except:
            Trace.handle_error()
            return e_errors.ERROR, e_errors.ERROR, [], "", 'receive_reply error'
        if len(message) < 100:  # status command returns a large array, we do not want to log it
            Trace.log(ACTION_LOG_LEVEL, "send_command: message %s" % (message,))
        response = message.split('\012')
        if 'status' in command_string:
            if response[0] == '':
                response.pop(0)  # in case first line in response is empty
            if self.debug:
                ofn = '/var/log/enstore/tmp/enstore/%s_%s.mtx_status.out' % (self.name, end_of_response,)
                Trace.log(e_errors.INFO, 'the output of the status command is in %s' % (ofn,))
                of = open(ofn, 'w')
                for line in response:
                    of.write('%s\n' % (line,))
                of.close()
        else:
            if len(response) < 100:
                Trace.log(ACTION_LOG_LEVEL, "send_command: response %s" % (response,))
        if 'Load' in command_string or 'Unload' in command_string:
            for line in response:
                if 'Failed' in line:
                    return e_errors.ERROR, e_errors.ERROR, '', response
                if e_errors.TIMEDOUT in line:
                    return e_errors.ERROR, e_errors.TIMEDOUT, '', response
            if '' in message:
                return e_errors.OK, e_errors.OK, '', ''
            else:
                return e_errors.ERROR, e_errors.ERROR, [], "", ''
        elif 'status' in command_string:
            # expected first line in reply:
            # Storage Changer /dev/changer:56 Drives, 10295 Slots ( 255 Import/Export )
            if all(s in response[0] for s in ['Storage Changer', 'Drives', 'Slots']):
                return e_errors.OK, e_errors.OK, '', response
            else:
                return e_errors.ERROR, e_errors.ERROR, [], "", 'Wrong reply format'
        elif 'TestUnitReady' in command_string:
            # expected reply:
            # "Ready:yes" or Ready:no
            for line in response:
                if 'Ready' in line:
                    return e_errors.OK, e_errors.OK, '', response
            else:
                return e_errors.ERROR, e_errors.ERROR, '', response
        else:
            return e_errors.ERROR, e_errors.ERROR, [], "", ''

    #########################################################################
    # These functions are overridden from the generic class.
    #########################################################################

    def get_mtx_status(self):
        t = 0
        while t < self.status_timeout:
            if self.mtx_server_started.value > 0:
                break
            else:
                time.sleep(1)
        if t >= self.status_timeout:
            msg_e = 'mtx server not responded in %s s' % (self.status_timeout,)
            Trace.alarm(e_errors.ERROR, msg_e)
            return e_errors.ERROR, e_errors.ERROR, [], '', msg_e
        if self.use_legacy_status:
            result = get_mtx_status(self.device_name)
            if not result:
                Trace.log(e_errors.WARNING, 'get_mtx_status failed')
                return e_errors.ERROR, e_errors.ERROR, [], '', 'get_mtx_status failed'
            rc = (e_errors.OK, e_errors.OK, '', result.split('\n'))
        else:
            rc = self.send_command('status,%s,%s,%s' % ('', '', os.getpid()), self.status_timeout)
        return rc

    # query robot
    def query_robot(self, ticket):
        __pychecker__ = "no-argsused"
        rc = self.robot_status()
        return e_errors.OK, 0, rc, "", ""

    # load volume into the drive;
    def load(self, ticket):
        """
        external_label,    # volume external label
        drive,             # drive id
        media_type):       # media type
        """
        drive = ticket['drive_id']
        external_label = ticket['vol_ticket']['external_label']
        media_type = ticket['vol_ticket'].get('media_type', 'unknown')
        Trace.log(e_errors.INFO,
                  'MTX_MediaLoader: request to load %s of type %s into drive %s' % (external_label, media_type, drive))
        return self.retry_function(self.mtx_mount, external_label,
                                   drive, media_type)

    # unload volume from the drive
    def unload(self, ticket):
        """
        external_label,  # volume external label
        drive,           # drive id
        media_type):     # media type
        """
        drive = ticket['drive_id']
        external_label = ticket['vol_ticket']['external_label']
        media_type = ticket['vol_ticket'].get('media_type', 'unknown')
        Trace.log(e_errors.INFO, 'MTX_MediaLoader: request to unload %s of type %s from drive %s' % (
            external_label, media_type, drive))
        return self.retry_function(self.mtx_dismount, external_label,
                                   drive, media_type)

    def do_retry(self, ret_val, returned_by_mtx_mount=False):
        retry = False
        update_db = False
        if ret_val[0] == -1:
            return True
        Trace.log(ACTION_LOG_LEVEL, 'do_retry %s %s' % (ret_val, returned_by_mtx_mount))
        try:
            if ((isinstance(ret_val[3], list) and
                 (('mtx: Request Sense: Sense Key=Unit Attention' in ret_val[3])
                  or ('mtx: Request Sense: Sense Key=Aborted Command' in ret_val[3])
                  or ('mtx: Request Sense: Sense Key=Illegal Request' in ret_val[3])
                  or ('mtx: Request Sense: Sense Key=Not Ready' in ret_val[3])
                  or ret_val[1] == e_errors.TIMEDOUT))):
                if 'mtx: Request Sense: Sense Key=Illegal Request' in ret_val[3]:
                    if returned_by_mtx_mount:
                        update_db = True
                    else:
                        if 'mtx: Request Sense: Additional Sense Code = 3B' in ret_val[3]:
                            if ('mtx: Request Sense: Additional Sense Qualifier = 0D' in ret_val[3] or
                                    'mtx: Request Sense: Additional Sense Qualifier = 11' in ret_val[3]):
                                # Medium Destination Element Full or Media magazine not accessible
                                if self.retry_count == 0:  # retry one more time
                                    self.retry_count = 1
                                update_db = False
                            elif 'mtx: Request Sense: Additional Sense Qualifier = 0E' in ret_val[3]:
                                # Medium Source Element Empty (Looks as the dismount command was successful)
                                update_db = True
                                Trace.trace(ACTION_LOG_LEVEL, 'do_retry: update_db = %s' % (update_db,))
                elif 'mtx: Request Sense: Sense Key=Not Ready' in ret_val[3]:
                    Trace.log(ACTION_LOG_LEVEL,
                              'Received Request Sense: Sense Key=Not Ready. Will wait a minute and retry')
                    time.sleep(60)
                elif 'mtx: Request Sense: Sense Key=Unit Attention' in ret_val[3]:
                    sleeptime = 30
                    if 'mtx: Request Sense: Additional Sense Code = 28' in ret_val[3]:
                        # According to IBM recommendations,  wait and retry more times
                        if self.retry_count == 0:
                            self.retry_count = 1
                            if 'mtx: Request Sense: Additional Sense Qualifier = 00' in ret_val[3]:
                                update_db = True
                            sleeptime = 40
                    time.sleep(sleeptime)
                elif ret_val[1] == e_errors.TIMEDOUT:
                    update_db = True
                retry = True
                if update_db:
                    Trace.log(e_errors.INFO, 'will update db')
                    rt = self.updatedb_on_retry()
                    Trace.log(e_errors.INFO, 'update db returned %s' % (rt,))
        except:
            Trace.handle_error()
            retry = True
        if self.retry_count > 0:
            self.retry_count -= 1
        else:
            retry = False
        return retry

    def updatedb_on_retry(self):
        return self.updatedb({'drive': {'address': None}})

    def insert(self, ticket):
        __pychecker__ = "no-argsused"  # When fixed remove this pychecker line.
        Trace.log(ACTION_LOG_LEVEL, 'insert %s' % ticket)
        if ticket.get('external_label'):
            volume_to_insert = ticket.get('external_label')
            s, d = self.locate_volume(ticket['external_label'])
            if d >= 0:
                return (e_errors.ERROR, '%s is in drive %s, can not be inserted' % (
                    ticket['external_label'], self.drives[d]['address']), None, None)
            if s < 0:
                return e_errors.ERROR, e_errors.MC_VOLNOTFOUND, ticket['external_label'], 'Volume not found'
            if 'IMPORT/EXPORT' not in self.slots[s]['location']:
                return (e_errors.ERROR,
                        '%s is not in  IMPORT/EXPORT slot (%s)' % (ticket['external_label'], self.slots[s]['location']),
                        None, None)
        else:
            # Find first not empty Import/export slot
            for imp in self.slots:
                if 'IMPORT/EXPORT' in imp['location'] and EMPTY not in imp['volume']:
                    volume_to_insert = imp['volume']
                    s = self.slots.index(imp)
                    break
            else:
                return e_errors.ERROR, 'Nothing to import', None, None

        self.retry_count = 4
        retry = True
        rc = None
        while retry:
            s_slot, d = self.locate_volume(EMPTY)
            if s_slot < 0:
                return (e_errors.ERROR,
                        ' %s can not be inserted, no free slots' % (ticket['external_label'],), None, None)

            stor_el = self.slots[s_slot]
            stor_el['volume'] = BUSY
            imp_el = self.slots[s]
            self.slots[s_slot] = stor_el
            Trace.log(ACTION_LOG_LEVEL, 'insert:  inserting %s from %s to %s' %
                      (volume_to_insert, imp_el['address'], stor_el['address']))
            rc = self.send_command('Unload,%s,%s,%s' % (stor_el['address'], imp_el['address'], os.getpid()),
                                   self.mount_timeout)
            Trace.trace(ACTION_LOG_LEVEL, "SCOMM RETURNED %s" % (rc,))
            if rc[1] == e_errors.OK:
                imp_el['volume'] = EMPTY
                stor_el['volume'] = self.slots[s]['volume']
                self.slots[s] = imp_el
                self.slots[s_slot] = stor_el
                rc = list(rc)
                rc[3] = "Imported %s from %s to %s" % (stor_el['volume'], imp_el['address'], stor_el['address'])
                rc = tuple(rc)
                break
            else:
                Trace.log(e_errors.INFO, "Unload for insert command returned: %s" % (rc,))
                if stor_el['volume'] == BUSY:
                    stor_el['volume'] = EMPTY
                    self.slots[s_slot] = stor_el
                retry = self.do_retry(rc, returned_by_mtx_mount=False)
                if retry:
                    # retry
                    # rc[0] == -1 - timeout
                    Trace.log(e_errors.INFO, 'retrying insert %s %s' % (imp_el, stor_el))
                    time.sleep(1)
                else:
                    break
        return rc

    def eject(self, ticket):
        __pychecker__ = "no-argsused"  # When fixed remove this pychecker line.
        try:
            external_label = ticket['volList'][0]
        except:
            return e_errors.ERROR, 'nothing to eject', '%s' % (ticket,), None
        s_slot, d = self.locate_volume(external_label)
        if d >= 0:
            return (e_errors.ERROR,
                    '%s is in drive %s, can not be ejected' % (external_label, self.drives[d]['address']), None, None)

        if s_slot < 0:
            return e_errors.ERROR, e_errors.MC_VOLNOTFOUND, external_label, 'Volume not found'
        if 'IMPORT/EXPORT' in self.slots[s_slot]['location']:
            return (e_errors.ERROR,
                    '%s is in  IMPORT/EXPORT slot (%s)' % (external_label, self.slots[s_slot]['location']), None, None)

        # Find first empty Import / export slot
        for imp in self.slots:
            if 'IMPORT/EXPORT' in imp['location'] and EMPTY in imp['volume']:
                s = self.slots.index(imp)
                break
        else:
            return e_errors.ERROR, 'No empty IMPORT/EXPORT slots', None, None

        stor_el = self.slots[s_slot]
        imp_el = self.slots[s]
        self.slots[s_slot] = stor_el
        retry_count = 4
        rc = None
        while retry_count > 0:
            rc = self.send_command('Unload,%s,%s,%s' % (imp_el['address'], stor_el['address'], os.getpid()),
                                   self.mount_timeout)
            Trace.trace(ACTION_LOG_LEVEL, "SCOMM RETURNED %s" % (rc,))
            if rc[1] == e_errors.OK:
                imp_el['volume'] = self.slots[s_slot]['volume']
                stor_el['volume'] = EMPTY
                self.slots[s] = imp_el
                self.slots[s_slot] = stor_el
                rc = list(rc)
                rc[3] = "Ejected %s from %s to %s" % (imp_el['volume'], stor_el['address'], imp_el['address'])
                rc = tuple(rc)
                break
            else:
                Trace.log(e_errors.INFO, "Unload for eject command returned: %s" % (rc,))
                if (rc[0] == -1) or (isinstance(rc[3], list) and
                                     'mtx: Request Sense: Sense Key=Unit Attention' in rc[3]):
                    # retry
                    # rc[0] == -1 - timeout
                    Trace.log(e_errors.INFO, 'retrying eject %s %s' % (imp_el, stor_el))
                    time.sleep(1)
                    retry_count -= 1
                else:
                    break
        return rc

    def getwork(self, ticket):
        Trace.trace(ACTION_LOG_LEVEL, 'getwork %s' % (ticket,))

        ticket = self._getwork(ticket)
        ticket['request_list'] = self.mount_dismount_queue
        self.reply_to_caller(ticket)

    #########################################################################
    # These functions are internal functions specific to MTX media changer.
    #########################################################################

    # Find the tape and mount it in the drive.
    def mtx_mount(self, volume, drive, media_type="", view_first=1):
        __pychecker__ = "unusednames=media_type,view_first"

        rc = (e_errors.OK, e_errors.OK, [], '', '')
        self.retry_count = 1
        if self.mount_retries > 1:
            self.retry_count = self.mount_retries
        retry = True
        while retry:
            try:
                dr = self.locate_drive(drive)  # index in self.drives
            except:
                Trace.handle_error()
                Trace.log(e_errors.ERROR, 'mtx_mount unrecognized drive: %s' % (drive,))
                return (e_errors.ERROR, e_errors.ERROR, [], '',
                        'mtx_mount unrecognized drive: %s' % (drive,))
            if dr < 0:
                Trace.log(e_errors.ERROR, 'mtx_mount unrecognized drive: %s' % (drive,))
                return (e_errors.ERROR, e_errors.ERROR, [], '',
                        'mtx_mount unrecognized drive: %s' % (drive,))

            s, d = self.locate_volume(volume)  # indices in self.slots, and self.drives correspondingly
            if s == -1:
                if d == -1:
                    Trace.log(e_errors.ERROR,
                              'mtx cant mount tape. Not in library')
                    return e_errors.ERROR, e_errors.ERROR, [], '', 'mtx cant mount tape. Not in library'
                elif dr == d:
                    return e_errors.OK, e_errors.OK, [], '', 'Already in drive %s' % (drive,)
                else:
                    Trace.log(e_errors.ERROR,
                              'mtx cant mount tape. Already in drive %s' % (drive,))
                    return (e_errors.ERROR, e_errors.MC_VOLNOTHOME, [], '',
                            'mtx cant mount tape. Already in drive %d' % (d,))

            if not self.is_empty(dr):
                Trace.log(e_errors.ERROR, 'mtx cant mount tape. Drive %s is not empty: %s' % (dr, self.drives[dr]))
                return (e_errors.ERROR, e_errors.MC_DRVNOTEMPTY, [], '',
                        'mtx cant mount tape. Drive %s is not empty: %s' % (drive, self.drives[dr]['volume']))
            Trace.log(e_errors.INFO, 'found %s in slot %s ...mounting' % (volume, self.slots[s]['address']))
            if not self.test_unit_ready():
                Trace.log(e_errors.ERROR, 'mount: Unit is not ready. Will try anyway')
            rc = self.send_command(
                'Load,%s,%s,%s' % (self.slots[s]['address'], self.drives[dr]['address'], os.getpid()),
                self.mount_timeout * self.mount_retries + 10)
            Trace.trace(ACTION_LOG_LEVEL, "Send Command returned %s" % (rc,))
            if rc[1] == e_errors.OK:
                Trace.trace(ACTION_LOG_LEVEL,
                            'updating DB: slots[%s]=%s drives[%s]=%s' % (s, self.slots[s], dr, self.drives[dr]))
                mutable_dict = self.slots[s]
                mutable_dict['volume'] = EMPTY
                self.slots[s] = mutable_dict
                mutable_dict = self.drives[dr]
                mutable_dict['volume'] = volume
                self.drives[dr] = mutable_dict
                Trace.trace(ACTION_LOG_LEVEL,
                            'updated DB: slots[%s]=%s drives[%s]=%s' % (s, self.slots[s], dr, self.drives[dr]))
                break
            else:
                Trace.log(e_errors.INFO, "Load returned: %s" % (rc,))
                retry = self.do_retry(rc, returned_by_mtx_mount=True)
                if retry:
                    Trace.log(e_errors.INFO, 'retrying mtx_mount %s %s' % (volume, drive))
                    time.sleep(1)
                else:
                    break
        Trace.log(ACTION_LOG_LEVEL, 'mtx_mount returning: %s' % (rc,))
        return rc

    # is tape drive empty
    def is_empty(self, drive_index):
        if 0 == self.status_valid:
            a, b = return_by(self.status_local, (), self.status_timeout)
            if -1 == a:
                Trace.log(e_errors.ERROR, 'mtx status request timeout')
                return False
            self.status_valid = 1

        if self.drives[drive_index]['volume'] == EMPTY:
            return True
        else:
            return False

    # Find a free slot and unmount the tape from the drive.
    def mtx_dismount(self, volume, drive, media_type="", view_first=1):
        __pychecker__ = "unusednames=media_type,view_first"
        rc = (e_errors.OK, e_errors.OK, [], '', '')
        self.retry_count = 1
        if self.mount_retries > 1:
            self.retry_count = self.mount_retries
        retry = True
        failed_dismount_addresses = []
        while retry:
            try:
                dr = self.locate_drive(drive)  # index in self.drives
            except:
                Trace.handle_error()
                Trace.log(e_errors.ERROR, 'mtx_dismount unrecognized drive: %s' % (drive,))
                return (e_errors.ERROR, e_errors.ERROR, [], '',
                        'mtx_dismount unrecognized drive: %s' % (drive,))

            if dr < 0:
                Trace.log(e_errors.ERROR, 'mtx_dismount unrecognized drive: %s' % (drive,))
                return (e_errors.ERROR, e_errors.ERROR, [], '',
                        'mtx_dismount unrecognized drive: %s' % (drive,))

            s, d = self.locate_volume(volume)  # indices in self.slots, and self.drives correspondingly
            if s > -1 and d < 0:
                # not in drive
                if self.is_empty(dr):  # no need to dismount
                    return e_errors.OK, 0, None, "", ""
                else:
                    return (e_errors.ERROR, e_errors.ERROR, [], '',
                            'mtx unload: Not in drive. In slot %s' % (self.slots[s]['address'],))

            if self.is_empty(dr):  # no need to dismount
                return e_errors.OK, 0, None, "", ""

            s, ignore = self.locate_volume(EMPTY, failed_dismount_addresses)

            if -1 == s:
                Trace.log(e_errors.ERROR, ' mtx unload: No free slots')
                return e_errors.ERROR, e_errors.ERROR, [], '', 'mtx unload: No free slots'

            ignore, d = self.locate_volume(volume)

            if dr != d:
                if volume != 'Unknown':
                    Trace.log(e_errors.ERROR, ' mtx unload: %s is in %s, not %s' %
                              (volume, self.drives[d]['address'], drive))
                    return (e_errors.ERROR, e_errors.ERROR, [], '',
                            'mtx unload: %s is not in %s' %
                            (volume, drive))
                else:
                    # What is the volume?
                    volume = self.drives[dr]['volume']

            Trace.log(e_errors.INFO, ('found ', volume, ' in drive ', drive, '...dismounting'))

            s, ignore = self.locate_volume(EMPTY, failed_dismount_addresses)
            if -1 == s:
                Trace.log(e_errors.ERROR, ' mtx unload: No free slots')
                return 'ERROR', e_errors.ERROR, [], '', 'mtx unload: No free slots'
            stor_el = self.slots[s]
            stor_el['volume'] = BUSY
            self.slots[s] = stor_el
            if not self.test_unit_ready():
                Trace.log(e_errors.ERROR, 'dismount: Unit is not ready. Will try anyway')
            rc = self.send_command(
                'Unload,%s,%s,%s' % (self.slots[s]['address'], self.drives[dr]['address'], os.getpid()),
                self.mount_timeout * self.mount_retries + 10)
            Trace.trace(ACTION_LOG_LEVEL, "Send Command returned %s" % (rc,))

            if rc[1] == e_errors.OK:
                Trace.trace(ACTION_LOG_LEVEL,
                            'updating DB: slots[%s]=%s drives[%s]=%s' % (s, self.slots[s], dr, self.drives[dr]))
                stor_el = self.slots[s]
                stor_el['volume'] = volume
                self.slots[s] = stor_el
                drive_el = self.drives[dr]
                drive_el['volume'] = EMPTY
                self.drives[dr] = drive_el
                Trace.trace(ACTION_LOG_LEVEL,
                            'updated DB: slots[%s]=%s drives[%s]=%s' % (s, self.slots[s], dr, self.drives[dr]))
                break
            else:
                Trace.log(e_errors.INFO, "Unload command returned: %s" % (rc,))
                if stor_el['volume'] == BUSY:
                    stor_el['volume'] = EMPTY
                retry = self.do_retry(rc, returned_by_mtx_mount=False)
                if retry:
                    failed_dismount_addresses.append(self.slots[s]['address'])
                    Trace.log(e_errors.INFO, 'retrying mtx_dismount %s %s' % (self.drives[dr], stor_el))
                    time.sleep(1)
                else:
                    break
        Trace.trace(ACTION_LOG_LEVEL, "mtx_dismount: returning %s" % (rc,))
        return rc

    # This method indicates where the tape is located within the
    # library by returning two numbers.  The first number is the slot
    # number containing the tape or negative one if the tape is not in
    # a slot.  The second number is the drive number the tape is in or
    # negative one if the tape is not in a drive.  (slots and drives
    # are both indexed starting at zero.)  If both numbers are
    # negative one then the tape is not in the library.  If both
    # numbers are not negative one then there is either a bug in this
    # function or multiple tapes that have the same label in the
    # library.
    def locate_volume(self, vol, ignore_addresses=None):
        if ignore_addresses is None:
            ignore_addresses = []
        Trace.trace(ACTION_LOG_LEVEL, ' looking for volume %s status_valid %s' % (vol, self.status_valid))
        if self.status_valid == 0:
            a, b = return_by(self.status_local, (), self.status_timeout)
            if a == -1:
                # retry
                Trace.log(e_errors.ERROR, 'locate_volume: mtx status request timeout, retrying')
                a, b = return_by(self.status_local, (), self.status_timeout)
                if a == -1:
                    Trace.log(e_errors.ERROR, 'locate_volume: mtx status request timeout')
                    return -1, -1
            self.status_valid = 1
        found = False
        if vol == EMPTY:
            idx_drive = -1  # we do not need to look for EMPTY volume in drives
        else:
            idx_drive = 0
            d = None
            for d in self.drives:
                try:
                    if vol == d['volume']:
                        found = True
                        break
                except KeyError as e:
                    Trace.log(e_errors.ERROR, 'exception in locate_volume %s %s' % (e, d))
                    return -1, -1
            if found:
                idx_drive = self.drives.index(d)

            if not found:
                idx_drive = -1
            else:
                Trace.trace(ACTION_LOG_LEVEL, 'found in drive %s' % (d['address'],))

        found = False
        idx_slot = 0
        s = None
        for s in self.slots:
            try:
                if vol == s['volume']:
                    if vol == EMPTY:
                        if 'IMPORT/EXPORT' in s['location']:
                            # We do not want to dismount into IMPORT/EXPORT storage element under normal conditions.
                            continue
                    if s['address'] in ignore_addresses:
                        # We do not want to return addresses we've been asked to ignore.
                        continue
                    found = True
                    break
            except KeyError as e:
                Trace.log(e_errors.ERROR, 'exception in locate_volume %s %s' % (e, s))
                return -1, -1

        if found:
            idx_slot = self.slots.index(s)
        if not found:
            idx_slot = -1
        else:
            Trace.trace(ACTION_LOG_LEVEL, 'found in slot %s' % (s['address'],))

        return idx_slot, idx_drive

    def locate_drive(self, drive_address):
        Trace.trace(ACTION_LOG_LEVEL, ' looking for drive %s , status_valid %s' % (drive_address, self.status_valid))
        if self.status_valid == 0:
            a, b = return_by(self.status_local, (), self.status_timeout)
            if a == -1:
                # retry
                Trace.log(e_errors.ERROR, 'locate_drive: mtx status request timeout, retrying')
                a, b = return_by(self.status_local, (), self.status_timeout)
                if a == -1:
                    Trace.log(e_errors.ERROR, 'locate_drive:  mtx status request timeout')
                    return -1
            self.status_valid = 1
        found = False
        idx_drive = 0
        drive_addr = drive_address
        if isinstance(drive_addr, str) and drive_addr.isdigit():
            drive_addr = int(drive_address)
        if isinstance(drive_addr, int):
            key = 'address'
        else:
            key = 'location'
        d = None
        for d in self.drives:
            if drive_addr == d[key]:
                found = True
                break
        if found:
            idx_drive = self.drives.index(d)

        if not found:
            idx_drive = -1
        return idx_drive

    #  This method tries to have device 'device' load or unload the tape in
    #  or from drive number drive back into slot number 'slot'.  The return
    #  value is anything that MTX printed to stderr.  If mtx hangs,
    #  this method will never return.
    @staticmethod
    def load_unload_local(slot, drive, load_command):
        Trace.log(e_errors.INFO, "MTX %s slot %s drive %s" % (load_command, slot, drive))
        if load_command not in ("Load", "Unload"):
            return 'ERROR', e_errors.ERROR, [], "%s" % (load_command,), "Wrong command"
        if load_command == "Load":
            mtx.Move(slot, drive)
        else:
            mtx.Move(drive, slot)
        ## Below is for mount / dismount failure timeouts testing
        ## if not hasattr(self, 'do_to'):
        ##      self.do_to =  0
        ##      time.sleep(self.mount_timeout + 10)

    # Test if drive is ready
    def test_unit_ready(self):
        for cnt in range(10):
            rc = self.send_command('TestUnitReady,%s,%s,%s' % ('', '', os.getpid()), self.status_timeout)
            Trace.log(ACTION_LOG_LEVEL, 'TestUnitReady returned: %s' % (rc,))
            # rc pattern is (error_code, additional_error_code, message, response)
            # it is inherited from implementation of STK class.
            # error_code is one e_errors codes
            # additional_error_code is implementation specific, but for this class it is usually the same as error_code
            # message is arbitrary message
            # response - response received from mtx server
            if e_errors.is_ok(rc[0]):
                if 'Ready:yes' in rc[3][0]:
                    return True
            time.sleep(10)
        return False

    # This method blocks while it returns the status of the media
    # changer at the specified device.
    # A return value are any messages that mtx printed to stderr.
    # If mtx hangs, this method will never return.
    def status_local(self):
        Trace.log(ACTION_LOG_LEVEL, 'status_local')
        if not self.test_unit_ready():
            msg_e = 'status: Unit is not ready. Will not update database'
            Trace.log(e_errors.ERROR, ' %s' % (msg_e,))
            return e_errors.ERROR, 'msg', ''
        busy_slots = []
        rc = self.get_mtx_status()
        if not e_errors.is_ok(rc[0]):
            Trace.log(e_errors.ERROR, 'get_mtx_status returned: %s' % (rc[0],))
            return e_errors.ERROR, 'get_mtx_status returned: %s' % (rc[0],)
        if hasattr(self, 'slots'):  # clear lists
            # save indexes of busy slots
            for i, slot_d in enumerate(self.slots):
                vol = slot_d.get('volume')
                if vol and vol == BUSY:
                    Trace.log(ACTION_LOG_LEVEL, 'busy slot: %s %s' % (i, slot_d,))
                    busy_slots.append(i)
            Trace.log(ACTION_LOG_LEVEL, 'busy slots: %s' % (busy_slots,))
            del self.slots[:]
            del self.drives[:]
        else:
            self.slots = self.manager.list()
            self.drives = self.manager.list()
        errorString = ''
        lines = rc[3]
        if lines:
            index = 0
            line = lines[index]
            while '' != line:
                try:
                    line = string.strip(line)
                    lel = line.split()
                    if string.find(line, 'Data Transfer Element') != -1:
                        # Expected format:
                        #   Data Transfer Element 257 Phys Loc F2,C1,R2,Z0 SN  00078D2B6E ID ULT3580-TD8 :Empty
                        # or :
                        # Data Transfer Element 270 Phys Loc F2,C4,R4,Z0 SN  00078D2B59 ID ULT3580-TD8 :Full
                        #     (Storage Element 1038 Loaded):VolumeTag = VQ0006L8
                        self.drives.append({})  # mutable dictionary
                        d = self.drives[-1]
                        d['address'] = int(lel[3])
                        loc_and_zone = lel[6].split(':')[0]
                        zone_index = loc_and_zone.find('Z')
                        d['location'] = loc_and_zone[0:zone_index - 1]  # throw ,Z
                        d['zone'] = loc_and_zone[zone_index:len(loc_and_zone)]
                        d['SN'] = lel[8]
                        d['type'] = lel[10].split(':')[0]
                        d['volume'] = EMPTY
                        if string.find(line, 'Empty') != -1:
                            pass  # just to leave status as empty
                        elif string.find(line, 'VolumeTag') != -1:
                            i1 = string.find(line, '=') + 1
                            i2 = len(line)
                            d['volume'] = string.strip(line[i1:i2])
                            # self.drives.append(string.strip(line[i1:i2]))
                        else:
                            # self.drives.append('unlabelled')
                            d['volume'] = 'unlabelled'
                        self.drives[-1] = d
                    elif string.find(line, 'Storage Element') != -1:
                        # Expected format     Storage Element 1025 Phys Loc F08,C02,R01,T00  :Full  :VolumeTag=VQ0033L8
                        # or:
                        # Storage Element 1037 Phys Loc EMPTY :Empty :VolumeTag=
                        self.slots.append({})  # mutable dictionary
                        d = self.slots[-1]
                        d['address'] = int(lel[2])
                        d['location'] = lel[5]
                        d['volume'] = EMPTY
                        if len(self.slots) - 1 in busy_slots:  # this slot was reserved, leave it BUSY
                            d['volume'] = BUSY
                            Trace.log(ACTION_LOG_LEVEL, 'from status: %s' % (line,))
                        if string.find(line, 'Empty') != -1:
                            pass  # just to leave status as empty
                        elif string.find(line, 'VolumeTag') != -1:
                            i1 = string.find(line, '=') + 1
                            i2 = len(line)
                            d['volume'] = string.strip(line[i1:i2])
                            # self.slots.append(string.strip(line[i1:i2]))
                        else:
                            d['volume'] = 'unlabelled'
                        self.slots[-1] = d
                    else:
                        Trace.log(ACTION_LOG_LEVEL, 'status_local: can not interpret line: %s' % (line,))
                    index = index + 1
                    line = lines[index]
                except:
                    Trace.handle_error(severity=ACTION_LOG_LEVEL)
            self.status_valid = 1
            self.last_updated_db.value = int(time.time())
        else:
            Trace.log(e_errors.ERROR, 'mtx status returned no result %s' % (rc,))
        if errorString:
            rc = (e_errors.ERROR, errorString, '')
        else:
            rc = (e_errors.OK, '', '')
        return rc

    # return status of all drives and slots
    def robot_status(self):
        result = enstore_functions2.shell_command(
            "%s mtx -f %s status | grep 'Data Transfer Element'" % (self.sudo_cmd, self.device_name))
        return result

    # getVolState in the drive; default overridden for other media changers
    def getVolState(self, ticket):
        __pychecker__ = "no-argsused"
        Trace.log(ACTION_LOG_LEVEL, 'getVolstate: %s' % (ticket,))
        retry_count = 2
        slot = None
        while retry_count > 0:
            ticket['status'] = e_errors.OK
            slot, drive = self.locate_volume(ticket['external_label'])
            Trace.log(ACTION_LOG_LEVEL, 'getVolState slot:%s, drive %s' % (slot, drive,))
            if (slot < 0 and drive < 0) or (slot > 0 and drive >= 0):
                # if volume is not in drive and not in slot
                # or volume is in drive and in slot - retry with inventory
                retry_count -= 1
                if retry_count > 0:
                    self.status_valid = 0  # this will cause an inventory
                    Trace.log(ACTION_LOG_LEVEL, 'getVolState retrying')
                ticket['status'] = e_errors.MC_VOLNOTFOUND
                continue
            elif slot < 0 <= drive:
                ticket['state'] = 'M'
                ticket['phys_location'] = ('%s,%s' % (self.drives[drive]['location'], self.drives[drive]['zone']))
                ticket['location'] = self.drives[drive]['address']
                ticket['SN'] = self.drives[drive]['SN']
            elif slot >= 0:
                if drive < 0 or ticket['external_label'] == EMPTY:
                    ticket['state'] = 'O'
                    ticket['location'] = self.slots[slot]['address']
                    ticket['phys_location'] = self.slots[slot]['location']
            retry_count = 0
        if ticket['external_label'] == EMPTY and ticket.get('reserve'):
            if slot >= 0:
                slot_info = self.slots[slot]
                slot_info['volume'] = BUSY
                self.slots[slot] = slot_info
                ticket['external_label'] = self.slots[slot]['volume']
        Trace.log(ACTION_LOG_LEVEL, 'getVolState: returning %s' % (ticket,))
        return (ticket.get('status'), ticket.get('location'), ticket.get('media_type'), ticket.get('state'),
                ticket.get('external_label'))

    def getDriveState(self, ticket):
        retry_count = 2
        rc = None
        while retry_count > 0:
            ticket['status'] = e_errors.OK
            try:
                drive = self.locate_drive(ticket['drive'])
            except:
                drive = -1
            drive_info = {}
            if drive < 0:
                retry_count -= 1
                if retry_count > 0:
                    self.status_valid = 0  # this will cause an inventory
                    Trace.log(ACTION_LOG_LEVEL, 'getDriveState retrying')
                rc = [e_errors.MC_DRVNOTFOUND, 0, 'No such drive', ticket['drive']]
                drive_info['state'] = 'noinfo'
                drive_info['status'] = 'N/A'
                continue
            if self.drives[drive]['volume'] == EMPTY:
                drive_info['volume'] = ''
            else:
                drive_info['volume'] = self.drives[drive]['volume']
            drive_info['state'] = 'online'
            drive_info['type'] = self.drives[drive]['type']
            drive_info['phys_location'] = ('%s,%s' % (self.drives[drive]['location'], self.drives[drive]['zone']))
            drive_info['location'] = self.drives[drive]['address']
            drive_info['SN'] = self.drives[drive]['SN']
            if drive_info['volume'] == '':
                drive_info['status'] = 'available'
            else:
                drive_info['status'] = 'in use'
            ticket['drive_info'] = drive_info
            rc = [e_errors.OK, self.drives[drive]['location'], '', '']
            retry_count = 0
        return rc

    def listDrives(self, ticket):
        Trace.log(e_errors.INFO, 'MTX listDrives, called %s' % (ticket,))
        drive_list = []
        for d in self.drives:
            try:
                drive_info = copy.copy(d)
                drive_info['name'] = (
                            '%s[%s](%s,%s)' % (d['address'], self.drives.index(d) + 1, d['location'], d['zone']))
                drive_info['state'] = 'online'
                drive_info['type'] = d['type']
                if drive_info['volume'] == EMPTY:
                    drive_info['status'] = 'available'
                    drive_info['volume'] = ''
                else:
                    drive_info['status'] = 'in use'
                drive_list.append(drive_info)
            except KeyError as e:
                Trace.log(e_errors.ERROR, 'exception in listDrives %s %s' % (e, d))
                return e_errors.ERROR, 0, None
        ticket['drive_list'] = drive_list
        return e_errors.OK, 0, None

    def dumpdb(self):
        """
        dump the content of the drive-volume lists into a file
        """
        t = time.strftime('%Y-%m-%d-%H:%M', time.localtime())
        with open('/var/log/enstore/tmp/enstore/%s_%s.mtx_dbdump.out' % (self.name, t,), 'w') as of:
            try:
                of.write(self._listVolumes())
            except:
                pass

    def updatedb(self, ticket):
        Trace.trace(ACTION_LOG_LEVEL, 'updatedb: ticket %s' % (ticket,))
        rc = [e_errors.OK, 0, '', '']
        if not ticket['drive']['address']:
            dt = int(time.time()) - self.last_updated_db.value
            if dt > 40:
                # request to re-load status information
                Trace.log(e_errors.INFO, 'Starting robot inventory for %s' % (ticket,))
                a, b = return_by(self.status_local, (), self.status_timeout)
                Trace.log(e_errors.INFO, 'Robot inventory finished')
                if -1 == a:
                    Trace.log(e_errors.ERROR, ' mtx status request timeout')
                    rc[0] = e_errors.ERROR
                rc[3] = 'Robot inventory finished'
                if self.debug:
                    self.dumpdb()
            else:
                # do not update so frequently
                rc[3] = 'Last updated %s seconds ago' % (dt,)
            return rc

        drive = self.locate_drive(ticket['drive']['address'])
        Trace.trace(ACTION_LOG_LEVEL, 'updatedb: drive index %s' % (drive,))
        if drive < 0:
            return [e_errors.ERROR, 0, 'No such drive', 'No such drive']
        drive_info = self.drives[drive]

        found = False
        s = None
        for s in self.slots:
            try:
                if ticket['volume']['address'] == s['address']:
                    found = True
                    break
            except:
                Trace.handle_error()
                return [e_errors.ERROR, 0, 'exception in updatedb']

        Trace.trace(ACTION_LOG_LEVEL, 'updatedb: slot %s' % (s,))
        if found:
            idx_slot = self.slots.index(s)
        else:
            return [e_errors.ERROR, 0, 'No slot with address %s' % (ticket['volume']['address'],), 'No such slot']
        slot_info = self.slots[idx_slot]
        Trace.trace(ACTION_LOG_LEVEL, 'updatedb: slot[%s] %s' % (idx_slot, slot_info,))

        drive_info['volume'] = ticket['drive']['volume']
        slot_info['volume'] = ticket['volume']['volume']
        self.drives[drive] = drive_info
        self.slots[idx_slot] = slot_info
        Trace.trace(ACTION_LOG_LEVEL, 'updatedb: returning %s' % (rc,))
        return rc

    def listSlots(self, ticket):
        Trace.trace(ACTION_LOG_LEVEL, 'listSlots')
        slot_list = []
        total = len(self.slots)
        used = 0
        free = 0
        for slot in self.slots:
            try:
                if slot['volume'] == EMPTY:
                    free += 1
                else:
                    used += 1
            except:
                Trace.handle_error()
        slot_list.append({'media_type': 'all',
                          'total': total,
                          'used': used,
                          'free': free,
                          'disabled': 0,
                          'location': 0})
        ticket['slot_list'] = slot_list
        return e_errors.OK, 0, None

    @staticmethod
    def setup_cli_command():
        """
        No CLI for this class yet
        """
        return e_errors.ERROR, 0, None, 'No CLI yet implemented for this class', ''

    def listClean(self, ticket):
        """
        Not yet implemented
        """
        return e_errors.ERROR, 0, None, 'Not yet implemented for this class', ''

    # return formatted text containing volume information
    def _listVolumes(self):
        msg_t = '     volume          state             location\n'
        for d in self.drives:
            if d['volume'] != EMPTY:
                msg_t += '%12s %12s %12s (%s,%s)\n' % \
                         (d['volume'],
                          'in drive',
                          d['address'],
                          d['location'],
                          d['zone'])
        for s in self.slots:
            msg_t += '%12s %12s %12s (%s)\n' % \
                     (s['volume'],
                      'home',
                      s['address'],
                      s['location'])
        return msg_t

    def listVolumes(self, ticket):
        # Send reply and Establish the connection first.
        ticket['status'] = (e_errors.OK, 0, None, "", "")
        reply = copy.copy(ticket)
        self.reply_to_caller(reply)
        try:
            hostinfo = socket.getaddrinfo(ticket['callback_addr'][0], None)
            sock = socket.socket(hostinfo[0][0], socket.SOCK_STREAM)
            sock.connect(ticket['callback_addr'])
        except:
            Trace.handle_error()
            Trace.log(e_errors.ERROR,
                      "Callback address %s" % (ticket['callback_addr'],))
            return e_errors.NET_ERROR, 0, str(sys.exc_info()[1]), "", ""

        ticket['no_reply'] = 1  # Tell WorkDone() not to send the ticket again.
        reply = ticket.copy()  # Make a copy to keep things clean.  But why?
        reply['volume_list'] = self._listVolumes()
        reply['MC_class'] = self.__class__.__name__
        try:
            r = callback.write_tcp_obj(sock, reply)
            sock.close()
            if r:
                Trace.log(e_errors.ERROR,
                          "Error calling write_tcp_obj. Callback addr. %s"
                          % (ticket['callback_addr'],))
        except:
            Trace.handle_error()
            Trace.log(e_errors.ERROR,
                      "Callback address %s" % (ticket['callback_addr'],))

            return e_errors.NET_ERROR, 0, str(sys.exc_info()[1]), "", ""
        return e_errors.OK, 0, None, "", ""


############################################################
## This class is for drive and slot addresses matching with IBM addresses.
## It uses direct calls to mtx routines via SWIG interface.
## This class implements media changer functionality to get used inside the mover code.
## The tape drive, served by the mover must enable Control Path.
#############################################################

class MTXN_Local_MediaLoader(MTXN_MediaLoader):

    # Several functions in this method have the same as name but different argument count than
    # inherited functions from MTXN_MediaLoader. This seems dubious, but I'm not changing the
    # signatures now as I'm not sure how these are used.
    # noinspection PyMethodOverriding
    def loadvol(self, vol_info, mover, drive):
        ticket = {'work': 'loadvol',
                  'function': 'mount',
                  'vol_ticket': vol_info,
                  'drive_id': drive}
        Trace.trace(ACTION_LOG_LEVEL, "loadvol %s" % (ticket,))
        # Make sure this request has all the necessary information included.
        check_status = self.check_mount_ticket(ticket)
        if not e_errors.is_ok(check_status[0]):
            return check_status

        return self.DoWork(self.load, ticket)

    # wrapper method for client - server communication
    # noinspection PyMethodOverriding
    def unloadvol(self, vol_info, mover, drive):
        ticket = {'work': 'unloadvol',
                  'function': 'dismount',
                  'vol_ticket': vol_info,
                  'drive_id': drive}
        Trace.trace(ACTION_LOG_LEVEL, "unloadvol %s" % (ticket,))
        if vol_info['external_label'] == 'Unknown':
            # client wants to dismount volume, but does not know its label
            # check if any volume is in drive
            rc = self.locate_drive(drive)
            Trace.trace(ACTION_LOG_LEVEL, "unloadvol: drive info %s" % (rc,))
            if e_errors.is_ok(rc) and rc['drive_info']['status'] == INUSE:
                ticket['vol_ticket'] = {'external_label': rc['drive_info']['volume'],
                                        'media_type': ''}
            else:
                return ticket

        # Make sure this request has all the necessary information included.
        check_status = self.check_mount_ticket(ticket)
        if not e_errors.is_ok(check_status[0]):
            return check_status

        return self.DoWork(self.unload, ticket)

    def GetWork(self):
        ticket = {'status': (e_errors.OK, 0, None),
                  'max_work': self.max_work}
        return ticket

    def DoWork(self, function, ticket):
        Trace.trace(ACTION_LOG_LEVEL, 'DoWork received %s %s' % (function, ticket))
        if 'function' not in ticket:
            err_msg = "MISSING FUNCTION KEY"
            ticket['status'] = (e_errors.MALFORMED, 0, err_msg)
            Trace.log(e_errors.ERROR, "%s" % (ticket['status'],))
            return ticket['status']

        # Output what we intend to do.
        common_message = self.get_common_message_string(ticket)
        Trace.trace(ACTION_LOG_LEVEL, "REQUESTED %s" % (common_message,))

        ###
        ### Determine if we need to drop the request for one reason or another.
        ###

        # Let work list length exceed max_work for cleanCycle.
        if ticket['function'] == "cleanCycle":
            pass
        # elif ticket['function'] in ["homeAndRestart"]:
        #    pass
        elif ticket['function'] in ("mount", "dismount", "eject", "insert"):
            # If we have max number of working children processes, assume
            # client will resend the request.
            if self.work_in_progress:
                message = "MC Overflow: %s %s" % \
                          (repr(self.max_work), common_message)
                Trace.trace(ACTION_LOG_LEVEL, message)

                ticket['status'] = (e_errors.MC_QUEUE_FULL, 0,
                                    "retry later")
                return ticket

        ### Otherwise, we can process work.

        # Output what we are going to do.
        Trace.trace(ACTION_LOG_LEVEL, "PROCESSING %s" % (common_message,))

        self.work_in_progress = True
        return self.executor(function, ticket, common_message)

    # noinspection PyMethodOverriding
    def executor(self, function, ticket, common_message):
        """
        Execute incoming request.
        Runs in a child process
        """
        message = "executor> begin %s" % (common_message,)
        Trace.trace(ACTION_LOG_LEVEL, message)

        # do the work ...

        # ... if this is a mount, dismount first
        if ticket['function'] == "mount":
            message = "executor> prepare dismount for %s" % \
                      (common_message,)
            Trace.trace(ACTION_LOG_LEVEL, message)

            # don't print a failure  (no tape mounted) message that is
            # really a success
            self.logdetail = 0
            # To Do: The downside to passing the volume we want to mount is
            # that there is a warning message about the MC expecting to
            # unmount that tape.  Some blank value should be passed to
            # avoid that false warning.
            dismount_ticket = {'work': 'unloadvol',
                               'vol_ticket': ticket['vol_ticket'],
                               'drive_id': ticket['drive_id']}
            sts = self.prepare(dismount_ticket)
            self.logdetail = 1  # back on

            message = "%s returned %s" % (message, sts[2])
            Trace.trace(ACTION_LOG_LEVEL, message)

        message = "executor> doing %s" % (common_message,)
        Trace.trace(ACTION_LOG_LEVEL, message)

        sts = function(ticket)  # Call the function!

        message = "executor> function %s returned %s ticket %s" % (common_message, sts, ticket)
        Trace.trace(ACTION_LOG_LEVEL, message)

        ticket["status"] = sts
        self.work_in_progress = False
        return ticket

    def __init__(self, argdict):
        csc_local = argdict.get('csc')
        medch = argdict.get('name')
        mc_device = argdict.get('mc_device')
        status_timeout = argdict.get('status_timeout', 300)
        mount_timeout = argdict.get('mount_timeout', 300)
        mount_retries = argdict.get('mount_retries', 2)
        debug = argdict.get('debug', False)
        debug_messaging = argdict.get('debug_messaging', False)

        generic_server.GenericServer.__init__(self, csc_local, medch,
                                              function=self.handle_er_msg)
        Trace.init(self.log_name, 'yes')
        print time.ctime(), 'STARTING'

        self.max_work = 1

        # Mark our cached status info as invalid
        self.status_valid = 0
        self.debug = debug
        self.debug_messaging = debug_messaging
        self.device_name = mc_device

        self.status_timeout = status_timeout
        self.mount_timeout = mount_timeout
        self.mount_retries = mount_retries
        self.use_legacy_status = argdict.get('use_legacy_status', False)

        self.sudo_cmd = ''
        Trace.log(e_errors.INFO, 'remote MC %s' % (argdict.get('remote_media_changer'),))
        self.mcc = media_changer_client.MediaChangerClient(csc_local, argdict.get('remote_media_changer'))

        self.manager = multiprocessing.Manager()
        self.mtx_server_started = self.manager.Value('i', 0)

        self.start_mtx_server()
        Trace.log(e_errors.INFO,
                  '%s initialized with device: %s status time limit: %s mount time limit: %s ' %
                  (self.__class__.__name__, self.device_name, self.status_timeout, self.mount_timeout))
        self.work_in_progress = False  # set to True when work is in progress
        print time.ctime(), 'STARTED'

    def start_mtx_server(self):
        self.p2cread, self.p2cwrite = os.pipe()
        self.c2pread, self.c2pwrite = os.pipe()

        self.server = multiprocessing.Process(target=self._mtx_server,
                                              args=(self.p2cread, self.c2pwrite, self.c2pwrite))
        self.server.start()
        rc = self.get_mtx_status()
        if not e_errors.is_ok(rc[0]):
            Trace.log(e_errors.ERROR, 'get_mtx_status returned: %s' % (rc[0],))
            msg_e = 'mtx server not responded in %s s. Exiting' % (self.status_timeout,)
            Trace.alarm(e_errors.ERROR, msg_e)
            self.quit()
            os._exit(1)

    # noinspection PyMethodOverriding
    def quit(self):
        """
        overrides dispatching worker quit
        """
        self.server.terminate()
        Trace.log(e_errors.INFO, " Waiting for MTX loop to exit")
        self.server.join()
        Trace.log(e_errors.INFO, " Local MTX server exiting")
        self.manager.shutdown()
        return

    #########################################################################
    # These functions are overridden from the generic class.
    #########################################################################

    # query robot
    def query_robot(self, ticket):
        __pychecker__ = "no-argsused"
        rc = self.robot_status()
        return e_errors.OK, 0, rc, "", ""

    # load volume into the drive;
    def load(self, ticket):
        """
        external_label,    # volume external label
        drive,             # drive id
        media_type):       # media type
        """
        drive = ticket['drive_id']
        external_label = ticket['vol_ticket']['external_label']
        media_type = ticket['vol_ticket'].get('media_type')
        Trace.log(e_errors.INFO,
                  'MTX_MediaLoader: request to load %s of type %s into drive %s' % (external_label, media_type, drive))
        return self.retry_function(self.mtx_mount, external_label,
                                   drive, media_type)

    # unload volume from the drive
    def unload(self, ticket):
        """
        external_label,  # volume external label
        drive,           # drive id
        media_type):     # media type
        """
        drive = ticket['drive_id']
        external_label = ticket['vol_ticket']['external_label']
        media_type = ticket['vol_ticket'].get('media_type')
        Trace.log(e_errors.INFO, 'MTX_MediaLoader: request to unload %s of type %s from drive %s' % (
            external_label, media_type, drive))
        return self.retry_function(self.mtx_dismount, external_label,
                                   drive, media_type)

    def insert(self, ticket):
        __pychecker__ = "no-argsused"  # When fixed remove this pychecker line.
        return (e_errors.NOT_SUPPORTED, 0,
                "MTX media changer does not support this operation.")

    def eject(self, ticket):
        __pychecker__ = "no-argsused"  # When fixed remove this pychecker line.
        return (e_errors.NOT_SUPPORTED, 0,
                "MTX media changer does not support this operation.")

    def updatedb_on_retry(self):
        return self.update_db()

    #########################################################################
    # These functions are internal functions specific to MTX media changer.
    #########################################################################

    def update_db(self, volume_address=None, volume_name=None, drive_address=None, volume_name_in_drive=None):
        Trace.trace(ACTION_LOG_LEVEL,
                    ' update_db %s %s %s %s' % (volume_address, volume_name, drive_address, volume_name_in_drive))
        rc = self.mcc.update_db(volume_address, volume_name, drive_address, volume_name_in_drive)
        Trace.trace(ACTION_LOG_LEVEL, ' update_db returned %s' % (rc,))
        return rc

    # Find the tape and mount it in the drive.
    def mtx_mount(self, volume, drive, media_type="", view_first=1):
        __pychecker__ = "unusednames=media_type,view_first"
        Trace.log(e_errors.INFO, "mtx_mount %s %s" % (volume, drive))
        rc = (e_errors.OK, e_errors.OK, [], '', '')
        self.retry_count = 1
        if self.mount_retries > 1:
            self.retry_count = self.mount_retries
        retry = True
        while retry:
            dt = self.locate_drive(drive)
            Trace.trace(ACTION_LOG_LEVEL, "LOCATE DRIVE returned %s" % (dt,))
            if not e_errors.is_ok(dt):
                Trace.log(e_errors.ERROR, 'mtx_mount unrecognized drive: %s' % (dt,))
                rc = (e_errors.ERROR, e_errors.ERROR, [], '', 'mtx_mount unrecognized drive: %s' % (drive,))
                break

            drive_info = dt['drive_info']
            if drive_info['status'] == 'in use':
                if drive_info['volume'] == volume:
                    rc = (e_errors.OK, e_errors.OK, [], '', 'Already in drive %s' % (drive,))
                    break
            if drive_info['status'] != 'available':
                Trace.log(e_errors.ERROR,
                          'mtx cant mount tape. Drive %s is not empty: %s' % (drive, drive_info['volume']))
                rc = (e_errors.ERROR, e_errors.MC_DRVNOTEMPTY, [], '',
                      'mtx cant mount tape. Drive %s is not empty: %s' % (drive, drive_info['volume']))
                break
            vt = self.locate_volume(volume)
            Trace.trace(ACTION_LOG_LEVEL, "LOCATE VOLUME returned %s" % (vt,))
            if not e_errors.is_ok(vt):
                Trace.log(e_errors.ERROR, 'mtx_mount unrecognized volume: %s' % (volume,))
                rc = (e_errors.ERROR, e_errors.ERROR, [], '', 'mtx_mount unrecognized volume: %s' % (volume,))
                break

            if vt['state'] == 'M':
                rc = (e_errors.ERROR, e_errors.ERROR, [], '', 'Mounted in another device %s' % (vt['location'],))
                break
            elif vt['state'] != 'O':
                rc = (e_errors.ERROR, e_errors.ERROR, [], '', 'volume %s state%s' % (volume, vt['state'],))
                break
            Trace.log(e_errors.INFO, 'mtx_mount %s from %s(%s) into %s(%s) SN %s' %
                      (volume, vt['location'], vt['phys_location'], drive, drive_info['phys_location'],
                       drive_info['SN']))
            if not self.test_unit_ready():
                Trace.log(e_errors.ERROR, 'mount: Unit is not ready. Will try anyway')
            rc = self.send_command('Load,%s,%s,%s' % (vt['location'], drive, os.getpid()),
                                   self.mount_timeout * self.mount_retries + 10)
            Trace.trace(ACTION_LOG_LEVEL, "SCOMM RETURNED %s" % (rc,))
            if rc[1] == e_errors.OK:
                rt = self.update_db(vt['location'], EMPTY, drive, volume)
                if not e_errors.is_ok(rt):
                    rc = (e_errors.ERROR, e_errors.ERROR, [], '', 'update location failed with %s' % (rt,))
                    break
                Trace.log(e_errors.INFO, 'mtx_mount: mounted %s from %s(%s) into %s(%s) SN %s' %
                          (volume, vt['location'], vt['phys_location'], drive, drive_info['phys_location'],
                           drive_info['SN']))
                break
            else:
                Trace.log(e_errors.INFO, "Load returned: %s" % (rc,))
                retry = self.do_retry(rc, returned_by_mtx_mount=True)
                if retry:
                    Trace.log(e_errors.INFO, 'retrying mtx_mount %s %s' % (volume, drive))
                    time.sleep(1)
                else:
                    break
        Trace.log(ACTION_LOG_LEVEL, 'mtx_mount returning: %s' % (rc,))
        return rc

    # Find a free slot and unmount the tape from the drive.
    def mtx_dismount(self, volume, drive, media_type="", view_first=1):
        __pychecker__ = "unusednames=media_type,view_first"
        Trace.log(e_errors.INFO, "mtx_dismount %s %s" % (volume, drive))
        rc = (e_errors.OK, e_errors.OK, [], '', '')
        self.retry_count = 1
        if self.mount_retries > 1:
            self.retry_count = self.mount_retries
        retry = True
        while retry:
            dt = self.locate_drive(drive)
            if not e_errors.is_ok(dt):
                Trace.log(e_errors.ERROR, 'mtx_dismount unrecognized drive: %s' % (drive,))
                rc = (e_errors.ERROR, e_errors.ERROR, [], '',
                      'mtx_dismount unrecognized drive: %s' % (drive,))
                break
            Trace.trace(ACTION_LOG_LEVEL, "mtx_dismount: drive query %s" % (dt,))
            drive_info = dt['drive_info']
            if drive_info['status'] != 'in use':
                rc = (e_errors.OK, 0, None, "", "")  # no need to dismount
                break
            else:
                if drive_info['volume'] != volume:
                    rc = (e_errors.ERROR, e_errors.ERROR, [], '',
                          'mtx_dismount: %s is not in %d' % (volume, drive))
                    break
            vt = self.locate_volume(EMPTY, reserve=True)
            Trace.log(e_errors.INFO, "mtx_dismount: volume_query %s" % (vt,))
            if not e_errors.is_ok(vt):
                Trace.log(e_errors.ERROR, ' mtx_dismount: No free slots')
                rc = (e_errors.ERROR, e_errors.ERROR, [], '', 'mtx_dismount: No free slots')
                break

            Trace.log(e_errors.INFO, ('found ', volume, ' in drive ', drive, '...dismounting'))
            # Set free slot busy to avoid conflicts with parallel mover requests.
            rt = self.update_db(vt['location'], BUSY, drive, volume)
            if not e_errors.is_ok(rt):
                # updatedb failed, so fail the dismount request
                rt = self.update_db(vt['location'], EMPTY, drive, volume)
                rc = (e_errors.ERROR, e_errors.ERROR, [], '', 'update location failed with %s' % (rt,))
                break
            Trace.log(e_errors.INFO, 'mtx_dismount %s location %s(%s) from drive %s(%s) SN %s' %
                      (volume, vt['location'], vt['phys_location'], drive, drive_info['phys_location'],
                       drive_info['SN']))
            if not self.test_unit_ready():
                Trace.log(e_errors.ERROR, 'dismount: Unit is not ready. Will try anyway')
            rc = self.send_command('Unload,%s,%s,%s' % (vt['location'], drive, os.getpid()),
                                   self.mount_timeout * self.mount_retries + 10)
            if rc[1] == e_errors.OK:
                rt = self.update_db(vt['location'], volume, drive, EMPTY)
                if not e_errors.is_ok(rt):
                    rc = (e_errors.ERROR, e_errors.ERROR, [], '', 'update location failed with %s' % (rt,))
                    break
                Trace.log(e_errors.INFO, 'mtx_dismount: dismounted %s location %s(%s) from drive %s(%s) SN %s' %
                          (volume, vt['location'], vt['phys_location'], drive, drive_info['phys_location'],
                           drive_info['SN']))
                break
            else:
                Trace.log(e_errors.INFO, "Unload returned: %s" % (rc,))
                retry = self.do_retry(rc, returned_by_mtx_mount=False)
                if retry:
                    Trace.log(e_errors.INFO, 'retrying mtx_dismount %s %s' % (volume, drive))
                    time.sleep(1)
                else:
                    break
        Trace.log(ACTION_LOG_LEVEL, 'mtx_dismount returning: %s' % (rc,))
        return rc

    # This method indicates where the tape is located within the
    # library by returning two numbers.  The first number is the slot
    # number containing the tape or negative one if the tape is not in
    # a slot.  The second number is the drive number the tape is in or
    # negative one if the tape is not in a drive.  (slots and drives
    # are both indexed starting at zero.)  If both numbers are
    # negative one then the tape is not in the library.  If both
    # numbers are not negative one then there is either a bug in this
    # function or multiple tapes that have the same label in the
    # library.
    def locate_volume(self, vol, reserve=False):
        Trace.log(ACTION_LOG_LEVEL, ' looking for volume %s' % (vol,))
        return self.mcc.viewvol(vol, '', reserve=reserve)

    def locate_drive(self, drive_address):
        Trace.log(ACTION_LOG_LEVEL, ' looking for drive %s' % (drive_address,))
        return self.mcc.viewdrive(drive_address)

    # return status of all drives and slots
    def robot_status(self):
        result = enstore_functions2.shell_command(
            "%s mtx -f %s status | grep 'Data Transfer Element'" % (self.sudo_cmd, self.device_name))
        return result

    # getVolState in the drive; default overridden for other media changers
    def getVolState(self, ticket):
        __pychecker__ = "no-argsused"
        ticket['status'] = e_errors.OK
        slot, drive = self.locate_volume(ticket['external_label'])
        if slot < 0 and drive < 0:
            ticket['status'] = e_errors.MC_VOLNOTFOUND
        elif slot < 0 <= drive:
            ticket['state'] = 'M'
            ticket['location'] = drive
        elif slot >= 0 > drive:
            ticket['state'] = 'O'
            ticket['location'] = slot
        return ticket.get('status'), ticket.get('location'), ticket.get('media_type'), ticket.get('state')

    def getDriveState(self, ticket):
        drive = ticket['drive']
        Trace.log(e_errors.INFO, 'calling viewdrive %s' % (drive,))
        drive_info = self.mcc.viewdrive(drive)
        Trace.log(e_errors.INFO, 'drive_info %s' % (drive_info,))
        ticket.update(drive_info)
        return e_errors.OK, 0, '', ''

    def listDrives(self, ticket):
        drive_list = self.mcc.list_drives()
        ticket.update(drive_list)
        return e_errors.OK, 0, '', ''


############################################################
## This class is for drive and slot addresses matching with new mtx addresses.
## It uses direct calls to mtx routines via SWIG interface.
## This class implements media changer functionality to get used along with
## media changer providing common database.
#############################################################

class MTXN_CDB_MediaLoader(MTXN_MediaLoaderSL):
    def __init__(self, medch, max_work=1, csc_local=None):
        MTXN_MediaLoaderSL.__init__(self, medch, max_work, csc_local)
        self.mcc = media_changer_client.MediaChangerClient(csc_local, self.mc_config.get('remote_media_changer'))

    #########################################################################
    # These methods are copies from MTXN_Local_MediaLoader.
    #########################################################################

    def updatedb_on_retry(self):
        return self.update_db()

    def update_db(self, volume_address=None, volume_name=None, drive_address=None, volume_name_in_drive=None):
        Trace.trace(ACTION_LOG_LEVEL,
                    ' update_db %s %s %s %s' % (volume_address, volume_name, drive_address, volume_name_in_drive))
        rc = self.mcc.update_db(volume_address, volume_name, drive_address, volume_name_in_drive)
        Trace.trace(ACTION_LOG_LEVEL, ' update_db returned %s' % (rc,))
        return rc

    # Find the tape and mount it in the drive.
    def mtx_mount(self, volume, drive, media_type="", view_first=1):
        __pychecker__ = "unusednames=media_type,view_first"
        Trace.log(e_errors.INFO, "mtx_mount %s %s" % (volume, drive))
        rc = (e_errors.OK, e_errors.OK, [], '', '')
        self.retry_count = 1
        if self.mount_retries > 1:
            self.retry_count = self.mount_retries
        retry = True
        while retry:
            dt = self.locate_drive(drive)
            Trace.trace(ACTION_LOG_LEVEL, "LOCATE DRIVE returned %s" % (dt,))
            if not e_errors.is_ok(dt):
                Trace.log(e_errors.ERROR, 'mtx_mount unrecognized drive: %s' % (dt,))
                rc = (e_errors.ERROR, e_errors.ERROR, [], '',
                      'mtx_mount unrecognized drive: %s' % (drive,))
                break

            drive_info = dt['drive_info']
            if drive_info['status'] == 'in use':
                if drive_info['volume'] == volume:
                    rc = (e_errors.OK, e_errors.OK, [], '', 'Already in drive %s' % (drive,))
                    break
            if drive_info['status'] != 'available':
                Trace.log(e_errors.ERROR,
                          'mtx cant mount tape. Drive %s is not empty: %s' % (drive, drive_info['volume']))
                rc = (e_errors.ERROR, e_errors.MC_DRVNOTEMPTY, [], '',
                      'mtx cant mount tape. Drive %s is not empty: %s' % (drive, drive_info['volume']))
                break
            vt = self.locate_volume(volume)
            Trace.trace(ACTION_LOG_LEVEL, "LOCATE VOLUME returned %s" % (vt,))
            if not e_errors.is_ok(vt):
                Trace.log(e_errors.ERROR, 'mtx_mount unrecognized volume: %s' % (volume,))
                rc = (e_errors.ERROR, e_errors.ERROR, [], '',
                      'mtx_mount unrecognized volume: %s' % (volume,))
                break

            if vt['state'] == 'M':
                rc = (e_errors.ERROR, e_errors.ERROR, [], '', 'Mounted in another device %s' % (vt['location'],))
                break
            elif vt['state'] != 'O':
                rc = (e_errors.ERROR, e_errors.ERROR, [], '', 'volume %s state%s' % (volume, vt['state'],))
                break
            Trace.log(e_errors.INFO, 'mtx_mount %s from %s(%s) into %s(%s) SN %s' %
                      (volume, vt['location'], vt['phys_location'], drive, drive_info['phys_location'],
                       drive_info['SN']))
            if not self.test_unit_ready():
                Trace.log(e_errors.ERROR, 'mount: Unit is not ready. Will try anyway')
            rc = self.send_command('Load,%s,%s,%s' % (vt['location'], drive, os.getpid()),
                                   self.mount_timeout * self.mount_retries + 10)
            Trace.trace(ACTION_LOG_LEVEL, "SCOMM RETURNED %s" % (rc,))
            if rc[1] == e_errors.OK:
                rt = self.update_db(vt['location'], EMPTY, drive, volume)
                if not e_errors.is_ok(rt):
                    rc = (e_errors.ERROR, e_errors.ERROR, [], '', 'update location failed with %s' % (rt,))
                    break
                Trace.log(e_errors.INFO, 'mtx_mount: mounted %s from %s(%s) into %s(%s) SN %s' %
                          (volume, vt['location'], vt['phys_location'], drive, drive_info['phys_location'],
                           drive_info['SN']))
                break
            else:
                Trace.log(e_errors.INFO, "Load returned: %s" % (rc,))
                retry = self.do_retry(rc, returned_by_mtx_mount=True)
                if retry:
                    Trace.log(e_errors.INFO, 'retrying mtx_mount %s %s' % (volume, drive))
                    time.sleep(1)
                else:
                    break
        Trace.log(ACTION_LOG_LEVEL, 'mtx_mount returning: %s' % (rc,))
        return rc

    # Find a free slot and unmount the tape from the drive.
    def mtx_dismount(self, volume, drive, media_type="", view_first=1):
        __pychecker__ = "unusednames=media_type,view_first"
        Trace.log(e_errors.INFO, "mtx_dismount %s %s" % (volume, drive))
        rc = (e_errors.OK, e_errors.OK, [], '', '')
        self.retry_count = 1
        if self.mount_retries > 1:
            self.retry_count = self.mount_retries
        retry = True
        while retry:
            dt = self.locate_drive(drive)
            if not e_errors.is_ok(dt):
                Trace.log(e_errors.ERROR, 'mtx_dismount unrecognized drive: %s' % (drive,))
                rc = (e_errors.ERROR, e_errors.ERROR, [], '', 'mtx_dismount unrecognized drive: %s' % (drive,))
                break
            Trace.trace(ACTION_LOG_LEVEL, "mtx_dismount: drive query %s" % (dt,))
            drive_info = dt['drive_info']
            if drive_info['status'] != 'in use':
                rc = (e_errors.OK, 0, None, "", "")  # no need to dismount
                break
            else:
                if volume == 'Unknown':
                    if drive_info['volume'] != '':
                        volume = drive_info['volume']  # some volume is in drive
                if drive_info['volume'] != volume:
                    rc = (e_errors.ERROR, e_errors.ERROR, [], '', 'mtx_dismount: %s is not in %d' % (volume, drive))
                    break
            vt = self.locate_volume(EMPTY, reserve=True)
            Trace.log(e_errors.INFO, "mtx_dismount: volume_query %s" % (vt,))
            if not e_errors.is_ok(vt):
                Trace.log(e_errors.ERROR, ' mtx_dismount: No free slots')
                rc = (e_errors.ERROR, e_errors.ERROR, [], '', 'mtx_dismount: No free slots')
                break

            Trace.log(e_errors.INFO, ('found ', volume, ' in drive ', drive, '...dismounting'))
            # Set free slot busy to avoid conflicts with parallel mover requests.
            rt = self.update_db(vt['location'], BUSY, drive, volume)
            if not e_errors.is_ok(rt):
                # updatedb failed, so fail the dismount request
                rt = self.update_db(vt['location'], EMPTY, drive, volume)
                rc = (e_errors.ERROR, e_errors.ERROR, [], '', 'update location failed with %s' % (rt,))
                break
            Trace.log(e_errors.INFO, 'mtx_dismount %s location %s(%s) from drive %s(%s) SN %s' %
                      (volume, vt['location'], vt['phys_location'], drive, drive_info['phys_location'],
                       drive_info['SN']))
            if not self.test_unit_ready():
                Trace.log(e_errors.ERROR, 'dismount: Unit is not ready. Will try anyway')
            rc = self.send_command('Unload,%s,%s,%s' % (vt['location'], drive, os.getpid()),
                                   self.mount_timeout * self.mount_retries + 10)
            if rc[1] == e_errors.OK:
                rt = self.update_db(vt['location'], volume, drive, EMPTY)
                if not e_errors.is_ok(rt):
                    rc = (e_errors.ERROR, e_errors.ERROR, [], '', 'update location failed with %s' % (rt,))
                    break
                Trace.log(e_errors.INFO, 'mtx_dismount: dismounted %s location %s(%s) from drive %s(%s) SN %s' %
                          (volume, vt['location'], vt['phys_location'], drive, drive_info['phys_location'],
                           drive_info['SN']))
                break
            else:
                Trace.log(e_errors.INFO, "Unload returned: %s" % (rc,))
                retry = self.do_retry(rc, returned_by_mtx_mount=False)
                if retry:
                    Trace.log(e_errors.INFO, 'retrying mtx_dismount %s %s' % (volume, drive))
                    time.sleep(1)
                else:
                    break
        Trace.log(ACTION_LOG_LEVEL, 'mtx_dismount returning: %s' % (rc,))
        return rc

    # This method indicates where the tape is located within the
    # library by returning two numbers.  The first number is the slot
    # number containing the tape or negative one if the tape is not in
    # a slot.  The second number is the drive number the tape is in or
    # negative one if the tape is not in a drive.  (slots and drives
    # are both indexed starting at zero.)  If both numbers are
    # negative one then the tape is not in the library.  If both
    # numbers are not negative one then there is either a bug in this
    # function or multiple tapes that have the same label in the
    # library.
    def locate_volume(self, vol, reserve=False):
        Trace.log(ACTION_LOG_LEVEL, ' looking for volume %s' % (vol,))
        return self.mcc.viewvol(vol, '', reserve=reserve)

    def locate_drive(self, drive_address):
        Trace.log(ACTION_LOG_LEVEL, ' Looking for drive %s' % (drive_address,))
        rc = self.mcc.viewdrive(drive_address)
        Trace.log(ACTION_LOG_LEVEL, ' locate_drive returning %s' % (rc,))
        return rc

    # return status of all drives and slots
    def robot_status(self):
        result = enstore_functions2.shell_command(
            "%s mtx -f %s status | grep 'Data Transfer Element'" % (self.sudo_cmd, self.device_name))
        return result

    # getVolState in the drive
    def getVolState(self, ticket):
        __pychecker__ = "no-argsused"
        Trace.log(ACTION_LOG_LEVEL, 'getVolState %s' % (ticket,))
        ticket['status'] = e_errors.OK
        rc = self.locate_volume(ticket['external_label'])
        ticket.update(rc)
        Trace.log(ACTION_LOG_LEVEL, 'locate_volume returned %s' % (ticket,))
        return ticket.get('status')

    def getDriveState(self, ticket):
        drive = ticket['drive']
        Trace.log(e_errors.INFO, 'calling viewdrive %s' % (drive,))
        drive_info = self.mcc.viewdrive(drive)
        Trace.log(e_errors.INFO, 'viewdrive returned drive_info %s' % (drive_info,))
        ticket.update(drive_info)
        return e_errors.OK, 0, '', ''

    def listDrives(self, ticket):
        drive_list = self.mcc.list_drives()
        ticket.update(drive_list)
        return e_errors.OK, 0, '', ''


#########################################################################
#
# IBM robot media loader
#
#########################################################################
class IBM_3584_MediaLoader(MediaLoaderMethods):

    def __init__(self, medch, max_work=7, csc_local=None):
        MediaLoaderMethods.__init__(self, medch, max_work, csc_local)
        self.prepare = self.unload
        self.DEBUG = 0
        # self.driveCleanTime = self.mc_config.get('DriveCleanTime',{'9840':[60,1],'9940':[60,1]})
        self.device = self.mc_config.get('device', 'Unknown')
        self.rmchost = self.mc_config.get("rmchost", 'Unknown')
        print "IBM 3584 MediaLoader initialized"

    # retry function call
    def retry_function(self, function, *args):
        count = self.getNretry()
        sts = ("", 0, "")
        # retry every error
        while count > 0 and sts[0] != e_errors.OK:
            try:
                sts = apply(function, args)
                if sts[1] != 0:
                    if self.logdetail:
                        Trace.log(e_errors.ERROR, 'retry_function: function %s  %s  sts[1] %s  sts[2] %s  count %s' % (
                            repr(function), args, sts[1], sts[2], count))
                    if function == self.mount:
                        time.sleep(60)
                        fixsts = apply(self.dismount, args)
                        Trace.log(e_errors.INFO, 'Tried %s %s  status=%s %s  Desperation dismount  status %s %s' % (
                            repr(function), args, sts[1], sts[2], fixsts[1], fixsts[2]))
                    time.sleep(60)
                    count = count - 1
                else:
                    break
            except:
                e, _, _ = Trace.handle_error()
                return str(e), 0, ""
        return sts

    # execute a stk cmd_proc command, but don't wait forever for it to complete
    # mostly stolen from Demo/tkinter/guido/ShellWindow.py - spawn function
    def timed_command(self, cmd, min_response_length=0, timeout=60):

        message = ""
        blanks = 0
        nread = 0

        now = timeofday.tod()
        p2cread, p2cwrite = os.pipe()
        c2pread, c2pwrite = os.pipe()
        command = cmd
        cmd_lookfor = ""

        # can not use dispatching work fork because we are already child.
        # need to kill explicitly and children can't kill
        (dum, mark) = self.delta_t(0)
        pid = os.fork()

        if pid == 0:
            # Child
            for i in 0, 1, 2:
                try:
                    os.close(i)
                except os.error:
                    pass
            if os.dup(p2cread) != 0:
                print 'ERROR: timed_command pc2cread bad read dup'
                Trace.log(e_errors.ERROR, 'timed_command pc2cread bad read dup')
            if os.dup(c2pwrite) != 1:
                print 'ERROR: timed_command c2pwrite bad write dup'
                Trace.log(e_errors.ERROR, 'timed_command c2pwrite bad write dup')
            if os.dup(c2pwrite) != 2:
                print 'ERROR: timed_command c2pwrite bad error dup'
                Trace.log(e_errors.ERROR, 'timed_command c2pwrite bad error dup')
            MAXFD = 100  # Max number of file descriptors (os.getdtablesize()???)
            for i in range(3, MAXFD):
                try:
                    os.close(i)
                except:
                    pass
            try:
                # I know this is hard-coded and inflexible. That is what I want in order to
                # prevent any possible security problem.

                os.execv("/bin/bash", ["bash", "-c", command])
            finally:
                e, e_msg, e_tb = sys.exc_info()
                Trace.log(e_errors.ERROR, "timed_command execv failed:  %s %s %s" %
                          (e, e_msg, traceback.format_tb(e_tb)))
                os._exit(1)

        os.close(p2cread)
        os.close(c2pwrite)
        os.close(p2cwrite)

        # wait for child to complete, or kill it
        start = time.time()
        if self.DEBUG:
            print timeofday.tod(), cmd
            Trace.trace(e_errors.INFO, "%s" % (cmd,))
        active = 0
        try:
            while active < timeout:
                p, _ = os.waitpid(pid, os.WNOHANG)
                if p != 0:
                    break
                # We need to start reading this now for really long responses.
                # Otherwise, the buffer fills up with the child waiting
                # for the parent to read something from the full buffer.
                # And the parent waits for the child to finish.
                e_msg = os.read(c2pread, 2000)
                if e_msg:
                    if self.DEBUG:
                        print e_msg,
                    message = message + e_msg
                    # Need to reset the timeout period.
                    start = time.time()
                    active = 0
                else:
                    if e_msg == '':
                        blanks = blanks + 1
                    active = time.time() - start
                    time.sleep(1)
            else:
                e_msg = "killing %d => %s" % (pid, cmd)
                print timeofday.tod(), e_msg
                Trace.trace(e_errors.INFO, e_msg)
                os.kill(pid, signal.SIGTERM)
                time.sleep(1)
                p, _ = os.waitpid(pid, os.WNOHANG)
                if p == 0:
                    e_msg = "kill -9ing %d => %s" % (pid, cmd)
                    print timeofday.tod(), e_msg
                    Trace.trace(e_errors.INFO, e_msg)
                    os.kill(pid, signal.SIGKILL)
                    time.sleep(2)
                    p, _ = os.waitpid(pid, os.WNOHANG)
        except:
            e, e_msg, e_tb = sys.exc_info()
            Trace.log(e_errors.ERROR,
                      "timed_command wait for child failed:  %s %s %s" % (e, e_msg, traceback.format_tb(e_tb)))
            os.close(c2pread)
            return -1, [], self.delta_t(mark)[0]

        if p == 0:
            os.close(c2pread)
            return -2, [], self.delta_t(mark)[0]

        # now read response from the pipe (Some of
        if string.find(cmd, 'mount') != -1:  # this is a mount or a dismount command
            maxread = 100  # quick response on queries
        else:
            maxread = 10000  # slow response on mount/dismounts

        nlines = 0
        ntries = 0
        jonflag = 0
        # async message start with a date:  2001-12-20 07:33:17     0    Drive   0, 0,10,12: Cleaned.
        # unfortunately, not just async messages start with a date.  Alas, each message has to be parsed.
        async_date = re.compile(r'20\d\d-\d\d-\d\d \d\d:\d\d:\d\d')
        response = []
        while nlines < 4 and ntries < 3:
            ntries = ntries + 1
            while nread < maxread:
                e_msg = os.read(c2pread, 2000)
                message = message + e_msg
                if e_msg:
                    if self.DEBUG:
                        print e_msg,
                nread = nread + 1
                if e_msg == '':
                    blanks = blanks + 1
            response = []
            resp = string.split(message, '\012')
            nl = 0
            for line in resp:
                if async_date.match(line):
                    if string.find(line, 'Place cartridges in CAP') != -1 or \
                            string.find(line, 'Remove cartridges from CAP') != -1 or \
                            string.find(line, 'Library error, LSM offline') != -1 or \
                            string.find(line, 'Library error, Transport failure') != -1 or \
                            string.find(line, 'Library error, LMU failure') != -1 or \
                            string.find(line, 'LMU Recovery Complete') != -1 or \
                            string.find(line, ': Offline.') != -1 or \
                            string.find(line, ': Online.') != -1 or \
                            string.find(line, ': Enter operation ') != -1 or \
                            string.find(line, 'Clean drive') != -1 or \
                            string.find(line, 'Cleaned') != -1:
                        if self.DEBUG:
                            print "DELETED:", line
                        jonflag = 1
                        continue
                if self.DEBUG:
                    print "response line =", nl, line
                response.append(line)
                nl = nl + 1
            nlines = len(response)

            nl = 0
            if jonflag and self.DEBUG:
                for line in response:
                    print "parsed lines =", nl, line
                    nl = nl + 1

        os.close(c2pread)
        size = len(response)
        status = 0
        look = 0
        while look in range(0, size):  # 1st part of response is STK copyright information
            if string.find(response[look], cmd_lookfor, 0) == 0:
                break
        if size != 0 and look == size: # FIXME: Never true!
            status = -4
            look = 0
        else:
            if len(response[look:]) < min_response_length:
                status = -5
        if self.DEBUG:
            rightnow = timeofday.tod()  # the times on fntt are not necessarily right, allows us to correlate log time
            rsp = [now, response[look:], rightnow]
            pprint.pprint(rsp)

        return status, response[look:], self.delta_t(mark)[0]

    #########################################################################
    # These functions are overridden from the generic class.
    #########################################################################

    # load volume into the drive;
    def load(self, ticket):
        drive = ticket['drive_id']
        external_label = ticket['vol_ticket']['external_label']
        media_type = ticket['vol_ticket']['media_type']
        return self.retry_function(self.mount, external_label, drive, media_type)

    # unload volume from the drive
    def unload(self, ticket):
        drive = ticket['drive_id']
        external_label = ticket['vol_ticket']['external_label']
        media_type = ticket['vol_ticket']['media_type']
        return self.retry_function(self.dismount, external_label, drive, media_type)

    def insert(self, ticket):
        __pychecker__ = "no-argsused"  # When fixed remove this pychecker line.
        return (e_errors.NOT_SUPPORTED, 0,
                "IBM media changer does not support this operation.")

    def eject(self, ticket):
        __pychecker__ = "no-argsused"  # When fixed remove this pychecker line.
        return (e_errors.NOT_SUPPORTED, 0,
                "IBM media changer does not support this operation.")

    # FIXME - what the devil is this?
    def getVolState(self, ticket):
        external_label = ticket['external_label']
        media_type = ticket['media_type']
        rt = self.retry_function(self.query, external_label, media_type)
        Trace.trace(11, "getVolState returned %s" % (rt,))
        if rt[3] == '\000':
            state = ''
        else:
            state = rt[3]
            if not state and rt[2]:  # volumes not in the robot
                state = rt[2]
        # Return the correct media type.
        try:
            ticket['media_type'] = rt[2].split()[-1]
        except (IndexError, ValueError, TypeError, AttributeError):
            pass

        return rt[0], rt[1], rt[2], state

    def getDriveState(self, ticket):
        drive = ticket['drive']
        rt = self.retry_function(self.query_drive, drive)
        # Update the ticket with additional information.
        info = rt[2].split()
        drive_info = {
            'state': info[0].strip(),
            'status': "N/A",
            'volume': info[3].strip(),
            'type': "LTO3"
        }
        ticket['drive_info'] = drive_info
        return rt[0], rt[1], rt[3], rt[4]

    def cleanCycle(self, in_ticket):
        __pychecker__ = "no-argsused"  # When fixed remove this pychecker line.
        return (e_errors.NOT_SUPPORTED, 0,
                "IBM media changer does not support this operation.")

    def query_robot(self, ticket):
        __pychecker__ = "no-argsused"  # When fixed remove this pychecker line.
        return (e_errors.NOT_SUPPORTED, 0,
                "IBM media changer does not support this operation.")

    def listDrives(self, ticket):
        # build the command, and what to look for in the response
        command = "/usr/bin/smc -h %s -l %s -q D" % (self.rmchost, self.device,)
        # answer_lookfor = ""

        # execute the command and read the response
        # FIXME - what if this hangs?
        # efb (dec 22, 2005) - up timeout from 10 to 60 as the queries are hanging
        # status,response, delta = self.timed_command(command,4,10)
        status, response, delta = self.timed_command(command, 2, 60)
        if status != 0:
            e = 4
            msg_e = "QUERY_DRIVE %i: %s => %i,%s" % (e, command, status, response)
            Trace.log(e_errors.ERROR, msg_e)
            return "ERROR", e, response, "", msg_e

        drive_list = []
        for line in response:
            if len(line) > 1:
                rc = line.split()

                name = rc[0]
                state = "N/A"
                status = rc[2]
                if status.find("loaded") != -1:
                    volume = rc[3]
                else:
                    volume = ""
                drive_type = "LTO3"

                drive_list.append({"name": name,
                                   "state": state,
                                   "status": status,
                                   "volume": volume,
                                   "type": drive_type,
                                   })

        ticket['drive_list'] = drive_list
        return e_errors.OK, 0, None, "", ""

    def listVolumes(self, ticket):
        # When implementing this function, remember to set ticket['no_reply']
        # to 1 to prevent WorkDone() from sending the response again.
        __pychecker__ = "no-argsused"  # When fixed remove this pychecker line.
        return (e_errors.NOT_SUPPORTED, 0,
                "IBM media changer does not support this operation.")

    def list_volumes2(self, ticket):
        ticket['work'] = "list_volumes"  # Use old method for IBM.
        ticket['function'] = "listVolume"
        return self.listVolumes(ticket)

    def listClean(self, ticket):
        # When implementing this function, remember to set ticket['no_reply']
        # to 1 to prevent WorkDone() from sending the response again.
        __pychecker__ = "no-argsused"  # When fixed remove this pychecker line.
        return (e_errors.NOT_SUPPORTED, 0,
                "IBM media changer does not support this operation.")

    def listSlots(self, ticket):
        __pychecker__ = "no-argsused"  # When fixed remove this pychecker line.
        return (e_errors.NOT_SUPPORTED, 0,
                "IBM media changer does not support this operation.")

    #########################################################################
    # These functions are internal functions specific to IBM media changer.
    #########################################################################

    def query(self, volume, _=None):
        # build the command, and what to look for in the response
        # command = "query vol %s" % (volume,)
        # answer_lookfor = "%s " % (volume,)
        command = "/usr/bin/smc -h %s -l %s -q V -V '%s'" % (self.rmchost, self.device, volume,)
        answer_lookfor = "%s" % (volume,)

        # execute the command and read the response
        # efb (dec 22, 2005) - up timeout from 10 to 60 as the queries are hanging
        # status,response, delta = self.timed_command(command,4,10)
        status, response, delta = self.timed_command(command, 1, 60)
        if status != 0:
            e = 1
            msg_e = "QUERY %i: %s => %i,%s" % (e, command, status, response)
            Trace.log(e_errors.ERROR, msg_e)
            return "ERROR", e, response, '', msg_e

        # got response, parse it and put it into the standard form
        answer = string.strip(response[0])
        if string.find(answer, answer_lookfor, 0) != 0:
            e = 2
            msg_e = "QUERY %i: %s => %i,%s" % (e, command, status, response)
            Trace.log(e_errors.ERROR, msg_e)
            return "ERROR", e, response, '', msg_e
        elif string.find(answer, 'slot') != -1:
            msg_e = "%s => %i,%s" % (command, status, answer)
            Trace.log(e_errors.INFO, msg_e)
            return e_errors.OK, 0, answer, 'O', msg_e  # occupied
        elif string.find(answer, 'drive') != -1:
            msg_e = "%s => %i,%s" % (command, status, answer)
            Trace.log(e_errors.INFO, msg_e)
            return e_errors.OK, 0, answer, 'M', msg_e  # mounted
        else:
            e = 3
            msg_e = "QUERY %i: %s => %i,%s" % (e, command, status, response)
            Trace.log(e_errors.ERROR, msg_e)
            return "ERROR", e, answer, '', msg_e

    def query_drive(self, drive):

        # build the command, and what to look for in the response
        # command = "query drive %s" % (drive,)
        # answer_lookfor = "%s " % (drive,)
        command = "/usr/bin/smc -h %s -l %s -q D -D %s" % (self.rmchost, self.device, drive,)
        answer_lookfor = "%s" % (drive,)

        # execute the command and read the response
        # FIXME - what if this hangs?
        # efb (dec 22, 2005) - up timeout from 10 to 60 as the queries are hanging
        # status,response, delta = self.timed_command(command,4,10)
        status, response, delta = self.timed_command(command, 1, 60)
        if status != 0:
            e = 4
            msg_e = "QUERY_DRIVE %i: %s => %i,%s" % (e, command, status, response)
            Trace.log(e_errors.ERROR, msg_e)
            return "ERROR", e, response, '', msg_e

        # got response, parse it and put it into the standard form
        answer = string.strip(response[0])
        answer = string.replace(answer, ', ', ',')  # easier to part drive id
        if string.find(answer, answer_lookfor, 0) != 0:
            e = 5
            msg_e = "QUERY_DRIVE %i: %s => %i,%s" % (e, command, status, answer)
            Trace.log(e_errors.ERROR, msg_e)
            return "ERROR", e, answer, '', msg_e
        elif string.find(answer, 'free') != -1:
            msg_e = "%s => %i,%s" % (command, status, answer)
            Trace.log(e_errors.INFO, msg_e)
            return e_errors.OK, 0, answer, '', msg_e  # empty
        elif string.find(answer, 'loaded') != -1:
            loc = string.find(answer, 'loaded')
            volume = string.split(answer[loc + 7:])[0]
            msg_e = "%s => %i,%s" % (command, status, answer)
            Trace.log(e_errors.INFO, msg_e)
            return e_errors.OK, 0, answer, volume, msg_e  # mounted
        else:
            e = 7
            msg_e = "QUERY_DRIVE %i: %s => %i,%s" % (e, command, status, answer)
            Trace.log(e_errors.ERROR, msg_e)
            return "ERROR", e, answer, '', msg_e

    def mount(self, volume, drive, media_type="", view_first=1):

        # build the command, and what to look for in the response
        # smc command is mute on success, and only says something on failure
        command = "/usr/bin/smc -h %s -l %s -m -D %s -V %s" % (self.rmchost, self.device, drive, volume)

        # check if tape is in the storage location or somewhere else
        if view_first:
            status, stat, response, attrib, com_sent = self.query(volume, media_type)

            if stat != 0:
                e = e_errors.MC_FAILCHKVOL
                msg_e = "MOUNT %i: %s => %i,%s" % (e, command, stat, response)
                Trace.log(e_errors.ERROR, msg_e)
                return "ERROR", e, response, "", msg_e
            if attrib != "O":  # look for tape in tower (occupied="O")
                e = e_errors.MC_VOLNOTHOME
                msg_e = "MOUNT %i: Tape is not in home position. %s => %s,%s" % (e, command, status, response)
                Trace.log(e_errors.ERROR, msg_e)
                return "ERROR", e, response, "", msg_e

            # check if any tape is mounted in this drive
            status, stat, response, volser, com_sent = self.query_drive(drive)
            if stat != 0:
                e = e_errors.MC_FAILCHKDRV
                msg_e = "MOUNT %i: %s => %i,%s" % (e, command, stat, response)
                Trace.log(e_errors.ERROR, msg_e)
                return "ERROR", e, response, "", msg_e
            if volser != "":  # look for any tape mounted in this drive
                e = e_errors.MC_DRVNOTEMPTY
                msg_e = "MOUNT %i: Drive %s is not empty =>. %s => %s,%s" % (e, drive, command, status, response)
                Trace.log(e_errors.ERROR, msg_e)
                return "ERROR", e, response, "", msg_e

        # execute the command and read the response
        status, response, delta = self.timed_command(command, 1, 60 * 10)
        if status != 0:
            e = 12
            msg_e = "MOUNT %i: %s => %i,%s" % (e, command, status, response)
            Trace.log(e_errors.ERROR, msg_e)
            return "ERROR", e, response, "", msg_e

        # got response, parse it and put it into the standard form
        answer = string.strip(response[0])
        if string.find(answer, " ", 0) != -1:
            e = 13
            msg_e = "MOUNT %i: %s => %i,%s" % (e, command, status, answer)
            Trace.log(e_errors.ERROR, msg_e)
            return "ERROR", e, response, "", msg_e
        msg_e = "%s => %i,%s" % (command, status, answer)
        Trace.log(e_errors.INFO, msg_e)
        return e_errors.OK, 0, msg_e, "", ""

    def dismount(self, volume, drive, _=None, view_first=1):
        # build the command, and what to look for in the response
        command = "/usr/bin/smc -h %s -l %s -d -D %s -V %s" % (self.rmchost, self.device, drive, volume)

        # check if any tape is mounted in this drive
        if view_first:
            status, stat, response, volser, com_sent = self.query_drive(drive)
            if stat != 0:
                e = e_errors.MC_FAILCHKDRV
                msg_e = "DISMOUNT %i: %s => %i,%s" % (e, command, stat, response)
                Trace.log(e_errors.ERROR, msg_e)
                return "ERROR", e, response, "", msg_e

            if volser == "":  # look for any tape mounted in this drive
                if volume != "Unknown":
                    # FIXME - this should be a real error. mover needs to know which tape it has.
                    e = 14
                    msg_e = "Dismount %i ignored: Drive %s is empty. Thought %s was there =>. %s => %s,%s" % (
                        e, drive, volume, command, status, response)
                    Trace.log(e_errors.INFO, msg_e)
                    return e_errors.OK, 0, response, "", msg_e
                else:  # don't know the volume on startup
                    e = 15
                    msg_e = "Dismount %i ignored: Drive %s is empty. Thought %s was there =>. %s => %s,%s" % (
                        e, drive, volume, command, status, response)
                    Trace.log(e_errors.INFO, msg_e)
                    return e_errors.OK, 0, response, "", msg_e

        # execute the command and read the response
        status, response, delta = self.timed_command(command, 1, 60 * 10)
        if status != 0:
            e = 16
            msg_e = "DISMOUNT %i: %s => %i,%s" % (e, command, status, response)
            Trace.log(e_errors.ERROR, msg_e)
            return "ERROR", e, response, "", msg_e

        # got response, parse it and put it into the standard form
        answer = string.strip(response[0])
        if string.find(answer, " ", 0) != -1:
            e = 17
            msg_e = "DISMOUNT %i: %s => %i,%s" % (e, command, status, answer)
            Trace.log(e_errors.ERROR, msg_e)
            return "ERROR", e, response, "", msg_e
        msg_e = "%s => %i,%s" % (command, status, answer)
        Trace.log(e_errors.INFO, msg_e)
        return e_errors.OK, 0, msg_e, "", ""


#########################################################################
# The command line interface class.
#########################################################################
class MediaLoaderInterface(generic_server.GenericServerInterface):

    def __init__(self):
        # fill in the defaults for possible options
        self.name = None
        self.max_work = 7
        generic_server.GenericServerInterface.__init__(self)

    parameters = ["media_changer"]

    media_options = {
        option.LOG: {option.HELP_STRING: "",
                     option.VALUE_USAGE: option.REQUIRED,
                     option.VALUE_TYPE: option.STRING,
                     option.USER_LEVEL: option.ADMIN
                     },
        option.MAX_WORK: {option.HELP_STRING: "",
                          option.VALUE_USAGE: option.REQUIRED,
                          option.VALUE_TYPE: option.INTEGER,
                          option.USER_LEVEL: option.ADMIN
                          },
    }

    def valid_dictionaries(self):
        return (self.media_options,) + \
            generic_server.GenericServerInterface.valid_dictionaries(self)

    # parse the options like normal but make sure we have a media changer
    def parse_options(self):
        option.Interface.parse_options(self)
        # bomb out if we don't have a media_changer
        if len(self.args) < 1:
            self.missing_parameter(self.parameters)
            self.print_help()
            sys.exit(1)
        else:
            self.name = self.args[0]


if __name__ == "__main__":   # pragma: no cover
    Trace.init("MEDCHANGER")
    Trace.trace(6, "media changer called with args: %s" % (sys.argv,))

    # get an interface
    intf = MediaLoaderInterface()

    csc = configuration_client.ConfigurationClient((intf.config_host,
                                                    intf.config_port))
    keys = csc.get(intf.name)
    try:
        mc_type = keys['type']
    except:
        exc, msg, tb = sys.exc_info()
        Trace.log(e_errors.ERROR, "MC Error %s %s" % (exc, msg))
        sys.exit(1)
    if 'max_work' in keys:
        intf.max_work = int(keys['max_work'])

    import __main__

    constructor = getattr(__main__, mc_type)
    mc = constructor(intf.name, intf.max_work, (intf.config_host, intf.config_port))

    mc.handle_generic_commands(intf)

    while 1:
        try:
            Trace.log(e_errors.INFO, "Media Changer %s (re) starting" % (intf.name,))
            mc.serve_forever()
        except SystemExit, exit_code:
            sys.exit(exit_code)
        except:
            mc.serve_forever_error("media changer")
            continue
    Trace.log(e_errors.ERROR, "Media Changer finished (impossible)")
