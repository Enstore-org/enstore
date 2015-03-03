#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

"""
migrate.py -- migration

Design issues:
[1] file based migration
    -- file is the basic migration unit
    -- other aggregations are implemented on top of this mechanism
[2] file is always available
[3] migration is transparent to user [encp]
[4] no special resource is preserved
    -- migration competes resources with others
[5] progress and history are kept
    -- the process can be reissued with the same parameter and
       produce the same result with redundant work
[6] migration is reversible before the original tape is recycled

Migration:
[1] copy file to a new media
    -- through disk, on *srv1
[2] swap metadata
    -- new copy is immediate available after swapping
[3] final scan
    -- read migrated file as users do
    -- paranoid reassurance

Implementation issues:
[1] copying to disk, copying from disk and reading migrated file
    are done in five concurrent threads
    -- Reading of all files is done sequentially in one thread, then the
       active and deleted files are seperated into seperate threads for
       writing to tape and another two threads for scanning.
    -- These threads are synchronized using four queues; two for active
       files and two for deleted files.
    -- copy_files():
         copy files to disk
         put done file on copy_queue
    -- write_new_files():
         pick up job from copy_queue
         copy file to tape
         swap metadata
         put done file on scan_queue
    -- final_scan():
         pick up job from scan_queue
         read the file
         mark original file deleted
[2] eventually, all encp should be called without os.system()
    -- need to deal with not-thread-safe issues
       -- encp in different threads can be imported into different
          name spaces
[3] four states of a migrating file
    -- copied
    -- swapped
    -- checked
    -- closed


There are six main top level access points for migration:
migrate_files(), migrate_volume(), final_scan_files(), final_scan_volume(),
restore_files and restore_volume().  Below is a general layout of the
migration function calls.

The functions marked with a one (1) indicate only one can be executed.
The functions marked with a capital ell (L) indicate they are called in a loop.
The functions listed with an asterisk (*) are optional.
The functions listed with a plus sign (+) are started in a new thread.


migrate_files()   ->  migrate()  -> + copy_files()       -> read_files()
migrate_volume()  ->  migrate()  -> + write_new_files()  -> write_new_file() -> write_file()
                                 -> +* final_scan()      -> final_scan_file()
                                                              ^
                                                              |
final_scan_files()  -------------------------------------------
final_scan_volume() ------------------------------------------|

restore_files()   -> restore_file()
       ^
       |
restore_volume()

"""

# system imports
import pg
import time
#import thread
import threading
try:
    import multiprocessing
    multiprocessing_available = True
except ImportError:
    multiprocessing_available = False
    pass
import Queue
import os
import sys
import string
import select
import signal
import types
import copy
import errno
import exceptions
import re
import stat
import socket
import traceback

# enstore imports
import file_clerk_client
import volume_clerk_client
import configuration_client
import chimera
import namespace
import option
import e_errors
import encp_wrapper
import volume_family
import callback
import Trace
import enstore_functions2
import enstore_functions3
import find_pnfs_file
import enstore_constants
import file_utils
import volume_assert_wrapper
import delete_at_exit
from scanfiles import ThreadWithResult
import info_client
import file_cache_status
import checksum

debug = False    # debugging mode
debug_p = False  # more debug output

###############################################################################

##
## Begin multiple_threads / forked_processes global variables.
##

#Originally, multiple threads were used.  One for reading, one for writing
# an optionally one for reading the new copy.  Unfortunately, EXfer was
# found to have some limitations.
#
# The first is that Py_BEGIN_ALLOW_THREADS and Py_END_ALLOW_THREADS need
# to flank the calls to do_read_write() and do_read_write_threaded().
# These are the C level macros for releasing and acquiring the internal
# python thread-safe lock(s).
#
# The Second problem is that EXfer has global variables.  If two threads
# try to use EXfer at the same time, they end up overritten the same data
# structures and eventually a segfault occurs.
#
# The workaround (not all of the above was known about EXfer and the time
# work on the workaround began) is to use fork and pipes instead of threads.
#
#Currently, EXfer has now been fixed.  So, threads are in and processes
# are out.  Python 2.4 does not have the multiprocessing module to do
# this same functionality with processes.
USE_THREADS = True

#Instead of reading all the files with encp when scaning, we have a new
# mode where volume_assert checks the CRCs for all files on a tape.  Using
# this volume_assert functionality should significantly reduce the [networking]
# resources required to run the migration scan; while at the same time
# increasing performance.
USE_VOLUME_ASSERT = False
#Instead of using encp to read the files, try using "get" which is a
# cousin program to encp.
USE_GET = False

#When true, start  N="proc_limit" number of processes/threads workers  for reading and
# N="proc_limit" number of processes/threads for writing.
##
## Currently use of this mode results in a deadlock.
##
## Update: Deadlocks are fixed.
PARALLEL_FILE_TRANSFER = True
#We need to make sure that the multiprocessing module is available
# for PARALLEL_FILE_TRANSFER.  If it is not set, set it to off.
if PARALLEL_FILE_TRANSFER and \
   (not multiprocessing_available and not USE_THREADS):
    PARALLEL_FILE_TRANSFER = False
    try:
        sys.stderr.write("Warning: Module multiprocessing not available.\n")
        sys.stderr.flush()
    except (IOError):
        pass

#Pass --threaded to encp if true. This can be changed through "--single-threaded-encp" option
use_threaded_encp = True

#Number of read/write processes/threads pairs to juggle at once, it can be changed by call option
PROC_LIMIT_DEFAULT = 3
proc_limit = PROC_LIMIT_DEFAULT

##
## End multiple_threads / forked_processes global variables.
##

###############################################################################

#If true, use the file clerk to access the DB.  If false, access the DB
# directly.
USE_CLERKS = False

#Default size of the Queue class objects.
DEFAULT_QUEUE_SIZE = 1024

#The value sent over a pipe to signal the receiving end there are no more
# comming.
SENTINEL = "SENTINEL"

FILE_LIMIT = 25 #The maximum number of files to wait for at one time.

#deleted status values
NO = "no"
YES = "yes"
UNKNOWN = "unknown"

###############################################################################

#Make this global so we can kill processes if necessary.
pid_list = []
#Make this global so we can join threads if necessary.
tid_list = []

pnfs_is_trusted = None #boolean for if the admin pnfs path is trusted or not.
#boolean if we need to use seteuid().  This is only needed for duplication
# with --make-failed-copies.
do_seteuid = None

errors = 0	# over all errors per migration run

no_log_command = ['--migrated-from', '--migrated-to', '--status']

# This is the configuration part, which might come from configuration
# server in the production version

# designated file family
#use_file_family = None  #possibly overridden with --file-family
# designated buffer disk area
SPOOL_DIR = ''  #usually overrided with --spool-dir
# name of pnfs database directory for migrated files
# (i.e. /pnfs/fs/usr/Migration)
MIGRATION_DB = 'Migration'
# default encp priority for migration
ENCP_PRIORITY = 0
# If all else fails, guess with this mount point.
DEFAULT_FS_PNFS_PATH = "/pnfs/fs/usr"

DELETED_TMP = 'DELETED'

###############################################################################

##
## The following constants define migration specific values.  This should
## be overridden in duplicate.py for duplication.
##

MFROM = "<="
MTO = "=>"

MIGRATION_FILE_FAMILY_KEY = "-MIGRATION"  #Not equals to (==) safe.
DELETED_FILE_FAMILY = "DELETED_FILES"

INHIBIT_STATE = "migrated"
IN_PROGRESS_STATE = "migrating"
MIGRATION_NAME = "MIGRATION"
set_system_migrated_func=volume_clerk_client.VolumeClerkClient.set_system_migrated
set_system_migrating_func=volume_clerk_client.VolumeClerkClient.set_system_migrating

# migration log file
LOG_DIR = '/var/migration'
LOG_FILE = "MigrationLog@"+time.strftime("%Y-%m-%d.%H:%M:%S", time.localtime(time.time()))+'#'+`os.getpid()`
log_f = None

##
## End migration specific global variables.
##

###############################################################################

#Make a list of migrating states.
MIGRATION_STATES = ["migrating", "duplicating", "cloning"]
#Make a list of migrated states.
MIGRATED_STATES = ["migrated", "duplicated", "cloned"]

dbhost = None
dbport = None
dbname = None
dbuser = "enstore"

###############################################################################

# timestamp2time(ts) -- convert "YYYY-MM-DD HH:MM:SS" to time
def timestamp2time(s):
	if s == '1969-12-31 17:59:59':
		return -1
	else:
		# take care of daylight saving time
		tt = list(time.strptime(s, "%Y-%m-%d %H:%M:%S"))
		tt[-1] = -1
		return time.mktime(tuple(tt))

# time2timestamp(t) -- convert time to "YYYY-MM-DD HH:MM:SS"
def time2timestamp(t):
	return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t))

# return the average file size in bytes
def average_size(volume_record):
    # Protect against division by zero.
    if volume_record['sum_wr_access'] == 0:
        return 0

    bytes_used = volume_record['capacity_bytes'] - volume_record['remaining_bytes']
    avg_size = (bytes_used) / volume_record['sum_wr_access']
    return avg_size  #In bytes.

# return the expected drive rates in bytes per second
def drive_rate(volume_record):
    rate_name = "RATE_" + volume_record['media_type']
    media_rate = getattr(enstore_constants, rate_name, None)
    if media_rate == None:
        return None
    return media_rate * 1024 * 1024  #In bytes per second.

# return True or False if the target string is a valid media type.
def is_media_type(target):
    if type(target) != types.StringType:
        return False

    supported_media_types = []
    for object_name in dir(enstore_constants):
        if object_name[:len(enstore_constants.CAPACITY_PREFIX)] == enstore_constants.CAPACITY_PREFIX:
            supported_media_types.append(object_name[4:])

    if target in supported_media_types:
        return True

    return False

# return True or False if the target string is colon separated volume and
# location cookie.
def is_volume_and_location_cookie(target):
    try:
        volume, location_cookie = exctract_volume_and_location_cookie(target)
    except TypeError:
        #Did not find something that could resemble a volume and location
        # cookie.
        return False

    if enstore_functions3.is_volume(volume) and \
       enstore_functions3.is_location_cookie(location_cookie):
        return True

    return False


def exctract_volume_and_location_cookie(target):
    if type(target) != types.StringType:
        return False

    pieces = target.split(":")
    if len(pieces) == 2:
        #Consider this a tape or null volume.
        volume = pieces[0]
        location_cookie = pieces[1]
    elif len(pieces) == 4:
        #New style disk location cookie.
        volume = string.join(pieces[:3], ":")
        location_cookie = pieces[3]
    elif len(pieces) == 5:
        #Old style disk location cookie.
        volume = string.join(pieces[:3], ":")
        location_cookie = string.join(pieces[3:], ":")
    else:
        #We don't know what we have.
        return None

    return volume, location_cookie


# search_order()
#Return in the following order:
#  1) first bfid to check
#  2) second bfid to check
#  3) first bfid's file record
#  4) second bfid's file record
#This is necessary to optimize the search order for both migration and
# duplication.  It orders the bfids to determine which is the active one
# in PNFS.
def search_order_migration(src_bfid, src_file_record, dst_bfid,
                            dst_file_record, is_it_copied, is_it_swapped,
                            fcc, db):
    #src_bfid:  The bfid of the source file.
    #src_file_record:  The file record of the source file.
    #dst_bfid:  The bfid of the destination file (or None if not known).
    #dst_file_record:  The file record of the destination file (or None if
    #                  not known).
    #is_it_copied: boolean true if the copied step is completed,
    #              false otherwise.
    #is_it_swapped: boolean true if the swap step is completed,
    #               false otherwise.
    #fcc: File Clerk Client instance.
    #db: postgres connection object instance.

    #Arguments fcc and db used by duplicate.py version.
    __pychecker__="unusednames=fcc,db"

    #Handle finding the name differently for swapped and non-swapped files.
    if is_it_copied and is_it_swapped:
        #Already copied.
        use_bfid = dst_bfid
        alt_bfid = src_bfid
        use_file_record = dst_file_record
        use_alt_file_record = src_file_record
    else:
        #Still need to copy or swap.
        use_bfid = src_bfid
        alt_bfid = dst_bfid
        use_file_record = src_file_record
        use_alt_file_record = dst_file_record

    return use_bfid, alt_bfid, use_file_record, use_alt_file_record

#Duplication may override this.
search_order = search_order_migration

# initialize csc, db, ... etc.
def init(intf):
    global log_f, dbhost, dbport, dbname, dbuser, errors
    global SPOOL_DIR
    #global use_file_family
    global pnfs_is_trusted
    global do_seteuid
    global debug
    global debug_p
    global use_threaded_encp
    global proc_limit

    #Make getting debug information from the command line possible.
    if intf.debug:
        debug = intf.debug
    if ( intf.debug_level and type(intf.debug_level) is int and intf.debug_level > 1):
        debug_p = True

    if intf.do_print:
        Trace.do_print(intf.do_print)
    if intf.do_log:
        Trace.do_log(intf.do_log)

    if intf.proc_limit_is_set:
        if intf.proc_limit <= 0:
            sys.stderr.write("proc_limit is not positive, %d\n" %(intf.proc_limit,))
            sys.exit(1)
        proc_limit = intf.proc_limit
    if debug:
        print "set proc_limit=", proc_limit

    if intf.single_threaded_encp:
        use_threaded_encp = False

    if debug:
        print "set use_threaded_encp=", use_threaded_encp

    csc = configuration_client.ConfigurationClient((intf.config_host,intf.config_port))

    db_info = csc.get('database')
    if socket.gethostname() == "gccenmvr2a.fnal.gov":
        dbhost = "gccenmvr2a.fnal.gov"
    else:
        dbhost = db_info['dbhost']
    dbport = db_info['dbport']
    dbname = db_info['dbname']
    dbuser = db_info['dbuser']

    errors = 0

    #Verify that all libraries passed in exist.
    if intf.library and not intf.show:
        for library in intf.library.split(","):
            library_fullname = library + ".library_manager"
            lib_dict = csc.get(library_fullname)
            if not e_errors.is_ok(lib_dict):
                sys.stderr.write("library %s does not exist\n" %(library,))
                sys.exit(1)

    if debug:
        log("library check okay")

    #Make sure we got the spool directory from command line or
    # from configuration.
    if intf.spool_dir:
        SPOOL_DIR = intf.spool_dir
    if not SPOOL_DIR:
        SPOOL_DIR = enstore_functions2.default_value("SPOOL_DIR")
    ##if not SPOOL_DIR and getattr(intf, 'make_failed_copies', None):
    ##	crons_dict = csc.get('crons')
    ##	SPOOL_DIR = crons_dict.get("spool_dir", None)
    ##	if SPOOL_DIR and not os.path.exists(SPOOL_DIR):
    ##		os.makedirs(SPOOL_DIR)

    #Verify if PNFS is trusted.  There will likely only be one mount
    # point to check, but handle more if necessary.
    if os.getuid() == 0:
        #Get a list of /pnfs/fs/usr like mount points.
        amp = chimera.get_enstore_admin_mount_point() #amp = Admin Mount Point
        if len(amp) == 0 and \
            not getattr(intf, 'make_failed_copies', None) and \
            not getattr(intf, 'make_copies', None):
                #If PNFS is not trusted, give up.
                sys.stderr.write("no PNFS admin mount points found\n")
                sys.exit(1)

        for directory in amp:
            test_file = "%s/.is_pnfs_trusted_test" % (directory,)
            #Create the test file.
            try:
                open_file = open(test_file, "w")
            except (OSError, IOError), msg:
                #If PNFS is not trusted, give up.
                sys.stderr.write("%s is not trusted [1]: %s\n" % (amp, str(msg),))
                sys.exit(1)
            #Close the test file.
            open_file.close()
            #Remove the test file.
            try:
                os.remove(test_file)
            except (OSError, IOError), msg:
                if msg.args[0] == errno.ENOENT:
                    pass
                else:
                    #If PNFS is not trusted?  Give up?
                    sys.stderr.write("%s is not trusted [2]: %s\n" % (amp, str(msg),))
                    sys.exit(1)
        else: # "for directory in amp:"
            pnfs_is_trusted = True
            do_seteuid = False

    if debug:
        log("trusted PNFS check okay")

    #If we are running duplication in make_failed_copies mode,
    # do seteuid() calls.
    if not pnfs_is_trusted and getattr(intf, 'make_failed_copies', None):
        do_seteuid = True

    # check for no_log commands
    if not intf.migrated_to and not intf.migrated_from and \
       not intf.status and not intf.show and \
       not getattr(intf, "summary", None):
            #First, verify for these commands we are user root.
            if os.geteuid() != 0:
                sys.stderr.write("Must run as user root.\n")
                sys.exit(1)

            # check for directories

            #log dir
            if not os.access(LOG_DIR, os.F_OK):
                os.makedirs(LOG_DIR)
            if not os.access(LOG_DIR, os.W_OK):
                message = "Insufficient permissions to open log file."
                error_log(message)
                sys.exit(1)
            log_f = open(os.path.join(LOG_DIR, LOG_FILE), "a")
            log(MIGRATION_NAME, string.join(sys.argv, " "))

    if debug:
        log("log dir check okay")

    # check for spool_dir commands
    if not intf.migrated_to and not intf.migrated_from and \
       not intf.status and not intf.show and not intf.scan_volumes and \
       not intf.scan and not getattr(intf, "restore", None):
            #spool dir
            if not SPOOL_DIR:
                message = "No spool directory specified."
                error_log(message)
                sys.exit(1)
            if not os.access(SPOOL_DIR, os.W_OK):
                os.makedirs(SPOOL_DIR)

            #migration dir - Make sure it has correct permissions.
            admin_mount_points = chimera.get_enstore_admin_mount_point()
            for mp in admin_mount_points:
                mig_dir = os.path.join(mp, MIGRATION_DB)
                try:
                    d_stat = os.stat(mig_dir)
                    stat_mode = d_stat[stat.ST_MODE]
                except (OSError, IOError):
                    continue

                if not stat_mode & stat.S_IRUSR or \
                   not stat_mode & stat.S_IWUSR or \
                   not stat_mode & stat.S_IXUSR or \
                   not stat_mode & stat.S_IRGRP or \
                   not stat_mode & stat.S_IWGRP or \
                   not stat_mode & stat.S_IXGRP or \
                   not stat_mode & stat.S_IROTH or \
                   not stat_mode & stat.S_IWOTH or \
                   not stat_mode & stat.S_IXOTH:
                        message = "Bad permissions for %s.  " \
                                  "Expected 0777." % \
                                   (mig_dir,)
                        log(message)
                        sys.exit(1)

    if debug:
        log("spool_dir check okay")

    return


#Return two important values for the copy queue:
# 1) The number of files that should be read, per thread, before writes
# should start.
# 2) The optimal size the copy queue should be.
#
# src_bfids: A sequence of source BFIDs or file_records.
# intf: A MigrateInterface class.
def get_queue_numbers(src_bfids, intf, volume_record=None):
    if intf.read_to_end_of_tape:
        #If the user specified that all the files should be read, before
        # starting to write; achive this by setting the proceed_number
        # to the number of files on the tape.
        proceed_number = len(src_bfids)
        # Adjust size of the copy queue, if necessary.  Always add one to
        # leave room for the SENTINEL.
        queue_size = proceed_number + 1
        return proceed_number, queue_size

    if len(src_bfids) == 0:
        #If the volume contains only deleted files and --with-deleted
        # was not used; src_bfids will be an empty list.  In this
        # case, skip setting this value to avoid raising an
        # IndexError doing the "get_media_type(src_bfids[0], db)" below.
        proceed_number = 1
        queue_size = proceed_number + 1
        return proceed_number, queue_size

    # get a db connection
    db = pg.DB(host=dbhost, port=dbport, dbname=dbname, user=dbuser)

    ###############################################################
    #Determine the media speeds that the migration will be going at.

    #First, for the source volume.
    if type(src_bfids[0]) == types.ListType:
        #If the first item is itself a list, get the first item of that list.
        if type(src_bfids[0][0]) == types.DictType:
            src_bfid_0 = src_bfids[0][0]['bfid']
        else:
            src_bfid_0 = src_bfids[0][0]
    else:
        if type(src_bfids[0]) == types.DictType:
            src_bfid_0 = src_bfids[0]['bfid']
        else:
            src_bfid_0 = src_bfids[0]
    src_media_type = get_media_type(src_bfid_0, db)

    #Second, for the destination volume.
    if intf.library and type(intf.library) == types.StringType:
        dst_media_type = get_media_type(intf.library.split(",")[0], db)
    else:
        mig_path = get_migration_db_path()
        if mig_path != None:
            dst_media_type = get_media_type(mig_path, db)
        else:
            db.close()  #Avoid resource leaks.

            #If we get here, we would need to look at the tags
            # in the original directory, but since we don't have
            # that available here, lets just drop it for now.
            return FILE_LIMIT, DEFAULT_QUEUE_SIZE

    #These rates are in MegaBytes
    src_rate = getattr(enstore_constants,
                       "RATE_" + str(src_media_type), None)
    dst_rate = getattr(enstore_constants,
                       "RATE_" + str(dst_media_type), None)
    if debug:
        log("src_rate:", src_rate)
        log("dst_rate:", dst_rate)

    #If the tape speeds for the new media are faster then the old media; this
    # should be: int(NUM_OBJS * (1 - (old_rape_rate / new_tape_rate)))
    if src_rate and dst_rate:
        if volume_record:
            #If we know we have files belonging to one volume, then we can
            # include the write file mark time in the rate.

            file_mark_seconds = 3 #Observed time for many types of drives.
            #Knowning the time it takes to write a filemark, we can calculate
            # the equivalent amount of data in Megabytes.
            dst_filemark_MB = (file_mark_seconds * dst_rate)
            #We get the number of bytes used on the tape.
            use_bytes = volume_record['capacity_bytes'] \
                        - volume_record['remaining_bytes']
            #Get the average filesize on the tape.
            avg_size_MB = use_bytes / volume_record['sum_wr_access'] / enstore_constants.MB
            #Apply the ratios to find the slightly slower rate.  The smaller
            # the average size the bigger the impact the dst_filemark_MB
            # value has.
            dst_rate = (dst_rate * avg_size_MB) / \
                       (avg_size_MB + dst_filemark_MB)
            if debug:
                log("dst_rate considering filemarks:", dst_rate)

        if src_rate > dst_rate:
            #If the destination side is faster, set the proceed_number
            # (aka low water mark) to 1.  Set the fraction of the file
            # list that is likely to need to be buffered to the queue size.
            proceed_number = 1
            queue_size = int(len(src_bfids) * (dst_rate / src_rate))
        else:
            #Take the ratio of the two rates (we want to know the part
            # of the ratio that the destination is bigger, hence the "1 -").
            # By multiplying this number by the list size we find out what
            # number of files that should be read by the slower read stream
            # before the faster write stream should start writing in order
            # for both steams to be done at the same time.
            proceed_number = int(len(src_bfids) * (1 - (src_rate / dst_rate)))
            #Determine the theoretical queue_size to be the fraction of
            # files that we need to buffer in order to maintain a steady
            # state even at the peek of waiting for the proceed_number
            # of files to be read before we start writing.
            queue_size = int(len(src_bfids) * (src_rate / dst_rate))

        if debug:
            log("unrestricted proceed_number:", proceed_number)
        if debug:
            log("unrestricted queue size:", queue_size)
    else:
        proceed_number = FILE_LIMIT
        queue_size = DEFAULT_QUEUE_SIZE

    ###############################################################

    # Sometimes there are extra constraints for these values.  Apply
    # them here.

    if type(intf.proceed_number) == types.IntType:
        #Map the user supplied proceed number to be within the
        # bounds of 1 and the number of files in the list.
        proceed_number = min(intf.proceed_number, len(src_bfids))
        proceed_number = max(proceed_number, 1)
    else:
        #Adjust the file limit to account for the number of processes/threads.
        if PARALLEL_FILE_TRANSFER:
            use_file_limit = FILE_LIMIT / proc_limit
        else:
            use_file_limit = FILE_LIMIT

        #Put some form of bound on this value until its effect on performance
        # is better understood.
        proceed_number = min(proceed_number, use_file_limit)
        proceed_number = max(proceed_number, 1)

    queue_size = max(min(len(src_bfids), DEFAULT_QUEUE_SIZE), queue_size)
    #Make sure the queue size is larger than the proceed_number (aka
    # the low water mark).
    queue_size = max(proceed_number + 1, queue_size)

    #More often than not, the proceed_number (aka low water mark) is
    # lowered by these hard limits.  The opposite is true for queue_size,
    # where it is often raised to the file list length for tapes with less
    # than DEFAULT_QUEUE_SIZE files per thread.

    ###############################################################
    if debug:
        log("proceed_number:", proceed_number)
        log("queue_size:", queue_size)

    db.close()  #Avoid resource leaks.

    return proceed_number, queue_size

#If the source and destination media_types are the same, set this to be
# a cloning job rather than a migration.
def setup_cloning_migration():
	global IN_PROGRESS_STATE, INHIBIT_STATE
	global set_system_migrated_func, set_system_migrating_func
	IN_PROGRESS_STATE = "cloning"
	INHIBIT_STATE = "cloned"
	set_system_migrated_func=volume_clerk_client.VolumeClerkClient.set_system_cloned
	set_system_migrating_func=volume_clerk_client.VolumeClerkClient.set_system_cloning

#Duplication may override this.
setup_cloning = setup_cloning_migration

#
def detect_uncleared_deletion_lists(MY_TASK):
    #If the cleanup lists for this encp are not empty, something failed.
    # Log it and move on.
    delete_at_exit.deletion_list_lock.acquire()
    deletion_lists = delete_at_exit.get_deletion_lists()
    if len(deletion_lists.files) > 0 or len(deletion_lists.bfids) > 0:
            error_log(MY_TASK, "cleanup lists are not empty: %s %s" %
                      (deletion_lists.files, deletion_lists.bfids))
            delete_at_exit.clear_deletion_lists()
            #Continue on to return 0 (for success).  If we got here, then
            # the file sould have been read.
    delete_at_exit.deletion_list_lock.release()

###############################################################################

### Only files in this section are allowed to use the file_utils.euid_lock
### to allow for thread safe use of seteuid().

#Make the file world writeable.  Make sure this only lasts for a short
# period of time.  The reason this function exists is so that, a file
# with only read permissions can temporarly be given write permissions so
# that its metadata can be modified.
def make_writeable(path):

    file_utils.chmod(path, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | \
                     stat.S_IWGRP | stat.S_IROTH | stat.S_IWOTH)

# nullify_pnfs() -- nullify the pnfs entry so that when the entry is
#			removed, its layer4 won't be put in trashcan
#			hence won't be picked up by delfile
def nullify_pnfs(pname):
    for i in [1,4]:
        f = file_utils.open(chimera.layer_file(pname, i), 'w')
        f.write("\n");
        f.close()

def pnfs_find(bfid1, bfid2, pnfs_id, file_record = None,
              alt_file_record = None, intf = None):

    ### Defining a function inside another function is very weird, but legal
    ### in python.  This functionality should only be called within
    ### pnfs_find().  This is just a thin wrapper around
    ### find_pnfs_file.find_chimeraid_path().
    def __find_chimeraid_path(pnfs_id, bfid, file_record, path_type):
        __pychecker__ = "unusednames=i"

        for i in range(2):
            try:
                path = find_pnfs_file.find_chimeraid_path(
                    pnfs_id, bfid, file_record = file_record,
                    path_type = path_type)

                return path
            except (OSError, IOError), msg:
                if msg.args[0] == errno.ENOENT:
                    #PNFS has an issue with returning ENOENT when a file
                    # really does exist.  Ask again and see if the answer
                    # changes.
                    time.sleep(1)
                    continue
                else:
                    #Reraise the exception for all other errors.
                    raise sys.exc_info()[0], sys.exc_info()[1], \
                          sys.exc_info()[2]
        else:
            #The only way to get here is if the search got ENOENT every time.
            raise msg

    #This is a hack for running duplication on a machine without trusted
    # status.  We allow for all types of PNFS filesystems to be used.
    if intf and getattr(intf, "make_failed_copies", None):
        use_path_type = enstore_constants.BOTH
    else:
        use_path_type = enstore_constants.FS

    src = None
    try:
        src = __find_chimeraid_path(pnfs_id, bfid1,
                                 file_record = file_record,
                                 path_type = use_path_type)
    except (KeyboardInterrupt, SystemExit):
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
    except (OSError, IOError):
        exc_type, exc_value, exc_tb = sys.exc_info()

        if exc_value.args[0] in [errno.EEXIST, errno.ENOENT]:
            try:
                if bfid2:
                    #If the migration is interupted
                    # part way through the swap, we need
                    # to check if the other bfid is
                    # current in layer 1.
                    src = __find_chimeraid_path(pnfs_id, bfid2,
                                 file_record = alt_file_record,
                                 path_type = use_path_type)
            except (KeyboardInterrupt, SystemExit):
                raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
            except (OSError, IOError):
                pass
            except:
                pass

        if not src:
            # Don't fill the log file when the situation is known.
            #Trace.handle_error(exc_type, exc_value, exc_tb, severity=99)
            pass

        del exc_tb #avoid resource leaks

        if not src:
            raise exc_type, exc_value, sys.exc_info()[2]

    return src

def File(path):
    # get all pnfs metadata
    if do_seteuid:
        file_utils.match_euid_egid(path)
    else:
        file_utils.acquire_lock_euid_egid()
        #If another thread doesn't use "reset_ids_back = True" then
        # be sure that the euid and egid are for roots, which it what the
        # rest of this function assumes the euid and egid are set to.
        file_utils.set_euid_egid(0, 0)

    try:
        p_File = chimera.File(path)
    except (KeyboardInterrupt, SystemExit):
        if do_seteuid:
            file_utils.end_euid_egid(reset_ids_back = True)
        else:
            file_utils.release_lock_euid_egid()
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
    except (OSError, IOError):  #Anticipated errors.
        if do_seteuid:
            file_utils.end_euid_egid(reset_ids_back = True)
        else:
            file_utils.release_lock_euid_egid()
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
    except:  # Un-anticipated errors.
        if do_seteuid:
            file_utils.end_euid_egid(reset_ids_back = True)
        else:
            file_utils.release_lock_euid_egid()
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]

    if do_seteuid:
        file_utils.end_euid_egid(reset_ids_back = True)
    else:
        file_utils.release_lock_euid_egid()

    return p_File

def update_layers(pnfs_File):
    # now perform the writes to the file's layer 1 and layer 4
    if do_seteuid:
        # [AK] I think this was bug:
        #   file_utils.end_euid_egid(reset_ids_back = True)
        file_utils.match_euid_egid(pnfs_File.path)
    else:
        file_utils.acquire_lock_euid_egid()
        #If another thread doesn't use "reset_ids_back = True" then
        # be sure that the euid and egid are for roots, which it what the
        # rest of this function assumes the euid and egid are set to.
        file_utils.set_euid_egid(0, 0)

    try:
        pnfs_File.update()  #UPDATE LAYER 1 AND LAYER 4!
    except (KeyboardInterrupt, SystemExit):
        if do_seteuid:
            file_utils.end_euid_egid(reset_ids_back = True)
        else:
            file_utils.release_lock_euid_egid()
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
    except (OSError, IOError): # Anticipated errors.
        if do_seteuid:
            file_utils.end_euid_egid(reset_ids_back = True)
        else:
            file_utils.release_lock_euid_egid()
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
    except: # Un-anticipated errors.
        if do_seteuid:
            file_utils.end_euid_egid(reset_ids_back = True)
        else:
            file_utils.release_lock_euid_egid()
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]

    if do_seteuid:
        file_utils.end_euid_egid(reset_ids_back = True)
    else:
        file_utils.release_lock_euid_egid()

def change_pkg_name(file_old,file_new,volume):
    # change file name in layer 4 of name space
    if do_seteuid:
        file_utils.match_euid_egid(file_new)
        try:
            _change_pkg_name(file_old,file_new,volume)
        finally:
            file_utils.end_euid_egid(reset_ids_back = True)
    else:
        file_utils.acquire_lock_euid_egid()
        #If another thread doesn't use "reset_ids_back = True" then
        # be sure that the euid and egid are for roots, which it what the
        # rest of this function assumes the euid and egid are set to.
        file_utils.set_euid_egid(0, 0)
        try:
            _change_pkg_name(file_old,file_new,volume)
        finally:
            file_utils.release_lock_euid_egid()

def _change_pkg_name(file_old,file_new,volume):
    # change file name in layer 4 of name space

    try:
        sfs = namespace.StorageFS(file_new)
        xrefs = sfs.get_xreference(file_new)

        if volume:
            xrefs[0] = volume
        xrefs[4] = file_new

        new_xrefs = sfs.set_xreference(*xrefs)

    except:
        # placeholder for debugging
        raise

###############################################################################

#function: is a string name of the function to call using apply()
#arg_list: is the argument list to pass to the function using apply()
#my_task: overrides the default step name for logging errors
#on_exception: 2-tuple consisting of function and arugument list to execute
#          if function throws an exception.
def run_in_process(function, arg_list, my_task = "RUN_IN_PROCESS",
		   on_exception = None):
	global pid_list
	global errors
	global log_f

	try:
		pid = os.fork()
	except OSError, msg:
		error_log("fork() failed: %s\n" % (str(msg),))
		return
	if pid > 0:  #parent
		#Add this to the list.
		pid_list.append(pid)
	else: #child
		#Clear the list of the parent's other childern.  They are
		# not the childern of this current process.
		pid_list = []
		#Also, clear the error count for the child.
		errors = 0

		try:
			if debug:
				print "Starting %s." % (str(function),)
			apply(function, arg_list)
			res = errors
			if debug:
				print "Started %s." % (str(function),)
		except (KeyboardInterrupt, SystemExit):
			res = 1
		except:
			res = 1
			exc, msg, tb = sys.exc_info()
			Trace.handle_error(exc, msg, tb)
			del tb #Avoid cyclic references.
			error_log(my_task, str(exc), str(msg))

			#Execute this function only if an exception occurs.
			if type(on_exception) == types.TupleType \
			   and len(on_exception) == 2:
				try:
					apply(on_exception[0], on_exception[1])
				except:
					message = "exception handler: %s: %s"\
						  % (sys.exc_info()[0],
						     sys.exc_info()[1])
					Trace.log(e_errors.ERROR, message)

			try:
				#Make an attempt to tell the parent
				# process to stop.
				parent_pid = os.getppid()
				if parent_pid > 1:
					os.kill(parent_pid, signal.SIGTERM)
			except:
				pass

		#Try and force pending output to go where it needs to go.
                Trace.flush_and_sync(sys.stdout)
                Trace.flush_and_sync(sys.stderr)
                Trace.flush_and_sync(log_f)

		os._exit(res) #child exit


def wait_for_process(kill = False):
	global pid_list

	#If we want them to die right now, send the signal.
	if kill:
		os.kill(pid_list[0], signal.SIGTERM)

	#We need to wait for a process to finsish.
	done_pid, done_exit_status = os.wait()

	#Remove the pid from the list of active pids.
	try:
		pid_list.remove(done_pid)
	except (IndexError), msg:
		try:
			sys.stderr.write("%s\n" % (msg,))
			sys.stderr.flush()
		except IOError:
			pass

	return done_exit_status

def wait_for_processes(kill = False):
	global pid_list

	rtn = 0
	while len(pid_list) > 0:
		rtn = rtn + wait_for_process(kill)

	return rtn

def __run_in_thread(function, on_exception, arg_list):
    try:
        apply(function, arg_list)
    except:
        Trace.handle_error()
        error_log(threading.currentThread().getName(),
                  "UNHANDLED EXCEPTION", str(sys.exc_info()[1]))

        #Execute this function only if an exception occurs.
        if type(on_exception) == types.TupleType \
           and len(on_exception) == 2:
            try:
                apply(on_exception[0], on_exception[1])
            except:
                message = "exception handler: %s: %s"\
                          % (sys.exc_info()[0],
                             sys.exc_info()[1])
                Trace.log(e_errors.ERROR, message)

        #Try and force pending output to go where it needs to go.
        Trace.flush_and_sync(sys.stdout)
        Trace.flush_and_sync(sys.stderr)
        Trace.flush_and_sync(log_f)

        try:
            #Make an attempt to tell the entire process to stop.
            pid = os.getpid()
            if pid > 1:
                os.kill(pid, signal.SIGTERM)
        except:
            pass


        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]

#function: is a string name of the function to call using apply()
#arg_list: is the argument list to pass to the function using apply()
#my_task: overrides the default step name for logging errors
#on_exception: 2-tuple consisting of function and arugument list to execute
#          if function throws an exception.
def run_in_thread(function, arg_list, my_task = "RUN_IN_THREAD",
                  on_exception = None):
    global tid_list

    # start a thread
    if debug:
        print "Starting %s." % (str(function),)
    try:
        #We use ThreadWithResult wrapper around thread.Thread() for its ability
        # to tell you if it is ready to be joined; and not for the exit
        # status of the thread, since migration threads handle errors on
        # their own.
        tid = ThreadWithResult(target=__run_in_thread, name=my_task,
                               args=(function, on_exception, arg_list))
        tid_list.append(tid)  #append to the list of thread ids.
        tid.start()
    except (KeyboardInterrupt, SystemExit):
        pass
    except:
        exc, msg = sys.exc_info()[:2]
        error_log(my_task, "start_new_thread() failed: %s: %s\n" \
                  % (str(exc), str(msg)))


    if debug:
        print "Started %s [%s]." % (str(function), str(tid))

def wait_for_thread():
    global tid_list

    rtn = 0

    while len(tid_list) > 0:
        for i in range(len(tid_list)):

            #If we blindly go into the join(), we will be stuck waiting
            # for the thread to finish.  The problem with this, is that
            # the python thread join() function blocks signals like SIGINT
            # (Ctrl-C) allowing the other threads in the program to continue
            # to run.
            if tid_list[i].is_joinable:

                #We should only be trying to join threads that are done.
                try:
                    tid_list[i].join(1)
                except RuntimeError:
                    rtn = rtn + 1

                if debug:
                    print "Completed %s." % (str(tid_list[i]),)

                try:
                    del tid_list[i]
                except IndexError, msg:
                    rtn = rtn + 1
                    try:
                        sys.stderr.write("%s\n" % (msg,))
                        sys.stderr.flush()
                    except IOError:
                        pass

                return rtn #Only do one to avoid "i" being off.
        else:
            time.sleep(5)
    return rtn

def wait_for_threads():
    global tid_list

    rtn = 0

    while len(tid_list) > 0:
        for i in range(len(tid_list)):
            tid_list[i].join()
            result = tid_list[i].get_result()
            del tid_list[i]

            if result:
                rtn = rtn + 1

            #If we joined a thread, go back to the top of the while.
            # If we don't we will have a discrepancy between indexes
            # from before the "del" and after the "del" of ts_check[i].
            break

    return rtn

#Run in either threads or processes depending on the value of USE_THREADS.
def run_in_parallel(function, arg_list, my_task = "RUN_IN_PARALLEL",
		    on_exception = None):
	if USE_THREADS:
		run_in_thread(function, arg_list, my_task = my_task,
                              on_exception = on_exception)
	else:
		run_in_process(function, arg_list, my_task = my_task,
			       on_exception = on_exception)

def wait_for_parallel(kill = False):
	global tid_list, pid_list

	MY_TASK = "WAIT_FOR_PARALLEL"
	if USE_THREADS:
		log(MY_TASK, "thread_count:", str(len(tid_list)))
		return wait_for_threads()
	else:
		log(MY_TASK, "process_count:", str(len(pid_list)))
		return wait_for_processes(kill = kill)

###############################################################################

def get_migration_db_path():
	#This value of mig_path isn't the greatest.  First, it assumes that
	# the values underneath the Migration DB path all reference this
	# top directory.  Second, it assumes that there is a Migration
	# DB path, instead of using a temporary file in the sam directory
	# as the original file.
	#
	#The function get_enstore_admin_mount_point() can return more than
	# one fs path if more are mounted.  Loop accordingly.
	pnfs_fs_paths = chimera.get_enstore_admin_mount_point()
	for pnfs_fs_path in pnfs_fs_paths:
		try:
			mig_path = os.path.join(pnfs_fs_path,
						MIGRATION_DB)
			os.stat(mig_path)
			#If we get here, then we found a migration path.
			# Lets use it, hope it is the correct one.
			break
		except (OSError, IOError):
			continue
	else:
		return

	return mig_path

###############################################################################

#If the source path is just a made up string, return true, otherwise false.
def is_deleted_path(filepath):
    if filepath[:8] == "deleted-":  #Made up paths begin with "deleted-".
        return True

    return False

def is_migration_path(filepath):
    #Make sure this is a string.
    if type(filepath) != types.StringType:
        raise TypeError("Expected string filename.",
				e_errors.WRONGPARAMETER)

    dname, fname = os.path.split(filepath)

    #Is this good enough?  Or does something more stringent need to
    # be used.  Only check the directories (scans of the PNFS migration
    # DB were failing because they contained "Migration").
    if MIGRATION_DB in dname.split("/"):
        return 1

    if fname.startswith(".m."):
        return 1

    return 0

def is_library(library):
	# get its own configuration server client
	config_host = enstore_functions2.default_host()
	config_port = enstore_functions2.default_port()
	csc = configuration_client.ConfigurationClient((config_host,
							config_port))
	lm_list = csc.get_library_managers2()
	for lm_conf_dict in lm_list:
		if lm_conf_dict['library_manager'] == library:
			return 1

	return 0
###############################################################################

#Use the Trace.py module for the underlying logging.  This way Trace output
# from other modules will not clobber output from migration.

# open_log(*args) -- log message without final newline
def open_log(*args):
    global log_f

    ctime = time.ctime()
    thread_name = threading.currentThread().getName()

    if len(args) == 1 and type(args[0]) == types.TupleType:
        log_components = (ctime, thread_name) + args[0]
    else:
        log_components = (ctime, thread_name) + args
    message = string.join(map(str, log_components), " ")

    #This function starts a line of logging output, but does not necessarily
    # finish it.  We set append_newline to false, but this can be overridden
    # if the caller explicitly puts a newline at the end of the list of
    # things to log.
    Trace.message(0, message, append_newline=False)
    Trace.message(0, message, append_newline=False, out_fp=log_f)

# error_log(s) -- handling appending error message
def error_log(*args):
    global errors

    errors = errors + 1

    log_components = args + ("... ERROR\n",)
    open_log(log_components)

# warning_log(s) -- handling appending warning message
def warning_log(*args):
    log_components = args + ("... WARNING\n",)
    open_log(log_components)

# ok_log(s) -- handling appending ok message
def ok_log(*args):
    log_components = args + ("... OK\n",)
    open_log(log_components)

# log(*args) -- log message
def log(*args):
    log_components = args + ("\n",)
    open_log(log_components)

# close_log(*args) -- close open line of log ouput with final newline
def close_log(*args):
    global log_f

    message = string.join(map(str, args), " ")
    Trace.message(0, message)
    Trace.message(0, message, out_fp=log_f)

###############################################################################
###############################################################################

### The functions in this section of code access the Enstore DB.  Either
### directory or through the file clerk or volume clerk.


# The is_copied(), is_swapped(), is_checked() and is_closed() functions
# query the state of a migrating file
# If true, the timestamp is returned, other wise, None is returned

# __is_migrated_state(bfid, db): used by is_copied(), is_swapped(),
#       is_checked() and is_closed().  It obtains the migration state
#       information for the bfid.
def __is_migrated_state(bfid, find_src, find_dst, fcc, db, order_by = "copied"):
    if order_by not in ("copied", "swapped", "checked", "closed"):
        raise ValueError("Expected migration state, not %s" % (str(order_by)))

    if USE_CLERKS:
        ## Use the file clerk to obtain the migration information in the
        ## database.

        reply_ticket = fcc.find_migration_info(bfid,
                                               find_src = find_src,
                                               find_dst = find_dst,
                                               order_by = order_by)
        if e_errors.is_ok(reply_ticket):
            res = reply_ticket['migration_info']
        else:
            raise e_errors.EnstoreError(None, reply_ticket['status'][1],
                                        reply_ticket['status'][0])
    else:
        ## Use the database directly to obtain the migration information in
        ## the database.

        src_res = []
        dst_res = []

        # The dst_bfid's are sorted in descending order to make sure that any
        # multiple copies get processed first, then the originals.  This
        # ordering is for get_bifds() which is called from restore_files().

        if find_src:
            q = "select * from migration where src_bfid = '%s' order by dst_bfid DESC,%s ASC;" \
                % (bfid, order_by)
            if debug:
                log("__is_migrated_state():", q)
            src_res = db.query(q).dictresult()

        if find_dst:
            q = "select * from migration where dst_bfid = '%s' order by dst_bfid DESC,%s ASC;" \
                % (bfid, order_by)
            if debug:
                log("__is_migrated_state():", q)
            dst_res = db.query(q).dictresult()

        res = src_res + dst_res

    return res


# is_copied(bfid) -- has the file already been copied?
#	we check the source file
#       As a side effect, this one returns the destination bfid instead
#       of the timestamp that the copy step completed.
def is_copied_migration(bfid, fcc, db, all_copies=False):

    res = __is_migrated_state(bfid, 1, 0, fcc, db) # 1, 0 => source only

    #If all_copies is true, return the answer as a list.  Normally, this
    # isn't an issue, but for files that are migrated to multiple copies,
    # this is important.
    if all_copies:
        bfid_list = []
        for file_result in res:
            bfid_list.append(file_result['dst_bfid'])
        return bfid_list

    if len(res) == 0:
        return None
    elif len(res) == 1:
        return res[0]['dst_bfid']
    else: #len(res) > 1
        for file_result in res:
            if not file_result['dst_bfid']:
                return file_result['dst_bfid']
        else:
            return res[0]['dst_bfid']

#Duplication may override this.
is_copied = is_copied_migration

# is_copied_by_dst(bfid) -- has the file already been copied?
#	we check the destination file
#       As a side effect, this one returns the destination bfid instead
#       of the timestamp that the copy step completed.
def is_copied_by_dst(bfid, fcc, db):

    res = __is_migrated_state(bfid, 0, 1, fcc, db) # 0, 1 => destination only

    if len(res) == 0:
        return None
    elif len(res) == 1:
        return res[0]['src_bfid']
    else: #len(res) > 1
        for file_result in res:
            if not file_result['src_bfid']:
                return file_result['src_bfid']
        else:
            return res[0]['src_bfid']

# is_swapped(bfid) -- has the file already been swapped?
#	we check the source file
# observation: it returns string or None
def is_swapped(bfid, fcc, db):

    res = __is_migrated_state(bfid, 1, 0, fcc, db) # 1, 0 => source only

    if len(res) == 0:
        return None
    elif len(res) == 1:
        return res[0]['swapped']
    else: #len(res) > 1
        for file_result in res:
            if not file_result['swapped']:
                return file_result['swapped']
        else:
            return res[0]['swapped']

# is_swapped_by_dst(bfid) -- has the file already been swapped?
#	we check the destination file
def is_swapped_by_dst(bfid, fcc, db):

    res = __is_migrated_state(bfid, 0, 1, fcc, db) # 0, 1 => destination only

    if len(res) == 0:
        return None
    elif len(res) == 1:
        return res[0]['swapped']
    else: #len(res) > 1
        for file_result in res:
            if not file_result['swapped']:
                return file_result['swapped']
        else:
            return res[0]['swapped']

# is_checked_by_src(bfid) -- has the file already been checked?
#	we check the source file
def is_checked_by_src(bfid, fcc, db):

    res = __is_migrated_state(bfid, 1, 0, fcc, db) # 1, 0 => source only

    if len(res) == 0:
        return None
    elif len(res) == 1:
        return res[0]['checked']
    else: #len(res) > 1
        for file_result in res:
            if not file_result['checked']:
                return file_result['checked']
        else:
            return res[0]['checked']

# is_checked(bfid) -- has the file already been checked?
#	we check the destination file
def is_checked(bfid, fcc, db):

    res = __is_migrated_state(bfid, 0, 1, fcc, db) # 0, 1 => destination only

    if len(res) == 0:
        return None
    elif len(res) == 1:
        return res[0]['checked']
    else: #len(res) > 1
        for file_result in res:
            if not file_result['checked']:
                return file_result['checked']
        else:
            return res[0]['checked']

# is_closed(bfid) -- has the file already been closed?
#	we check the destination file
def is_closed(bfid, fcc, db):

    res = __is_migrated_state(bfid, 0, 1, fcc, db) # 0, 1 => destination only

    if len(res) == 0:
        return None
    elif len(res) == 1:
        return res[0]['closed']
    else: #len(res) > 1
        for file_result in res:
            if not file_result['closed']:
                return file_result['closed']
        else:
            return res[0]['closed']

# is_duplicated(bfid) -- check the source bfid
def is_duplicated(bfid, fcc, db, find_src=0, find_dst=1,
                  include_multiple_copies=False):

    src_res = []
    dst_res = []

    if USE_CLERKS:
        ## Use the file clerk to obtain the duplication/multiple_copy
        ## information in the database.

        if find_src:
            reply_ticket = fcc.find_original(bfid)
            if e_errors.is_ok(reply_ticket):
                src_res = [reply_ticket['original']]
            else:
                raise e_errors.EnstoreError(None, reply_ticket['status'][1],
                                        reply_ticket['status'][0])

        if find_dst:
            reply_ticket = fcc.find_copies(bfid)
            if e_errors.is_ok(reply_ticket):
                dst_res = reply_ticket['copies']
            else:
                raise e_errors.EnstoreError(None, reply_ticket['status'][1],
                                        reply_ticket['status'][0])

        #Need to exclude multiple copies if necessary.
        if not include_multiple_copies and (src_res or dst_res):
            reply_ticket = fcc.find_migrated(bfid)
            if e_errors.is_ok(reply_ticket):
                if src_res:
                    if reply_ticket['src_bfid'] not in src_res:
                        src_res = []
                if dst_res:
                    if reply_ticket['dst_bfid'] not in dst_res:
                        dst_res = []
            else:
                raise e_errors.EnstoreError(None, reply_ticket['status'][1],
                                        reply_ticket['status'][0])
    else:
        ## Use the database directly to obtain the migration information in
        ## the database.

        if find_src:
            if include_multiple_copies:
                #Need to include multiple copies.
                q = "select * from file_copies_map where alt_bfid = '%s';" \
                    % (bfid,)
            else:
                #Need to exclude multiple copies, return just duplication
                # copies.
                q = "select file_copies_map.* " \
                    "from file_copies_map,migration " \
                    "where alt_bfid = '%s' " \
                    "  and ((alt_bfid = src_bfid and " \
                    "        bfid = dst_bfid) " \
                    "       or " \
                    "       (alt_bfid = dst_bfid and " \
                    "        bfid = src_bfid) " \
                    "      );" \
                    % (bfid,)
            if debug:
                log("is_duplicated():", q)
            src_res = db.query(q).dictresult()

        if find_dst:
            if include_multiple_copies:
                #Need to include multiple copies.
                q = "select * from file_copies_map where bfid = '%s';" \
                    % (bfid,)
            else:
                #Need to exclude multiple copies, return just duplication
                # copies.
                q = "select file_copies_map.* " \
                    "from file_copies_map,migration " \
                    "where bfid = '%s' " \
                    "   and ((alt_bfid = src_bfid and " \
                    "         bfid = dst_bfid) " \
                    "        or " \
                    "        (alt_bfid = dst_bfid and " \
                    "        bfid = src_bfid) " \
                    "       );" \
                    % (bfid,)
            if debug:
                log("is_duplicated():", q)
            dst_res = db.query(q).dictresult()

    res = src_res + dst_res
    return res


# get_bfids(bfid) -- get the src and destination bfid
#	we check the destination file
#       The order will put multiple copies first.  This is okay since
#       get_bifds is only used in restore_files() and there processing the
#       original copy list is desired.  If this function gets used
#       by other functions then care needs to be taken.
def get_bfids(bfid, fcc, db):

    res = __is_migrated_state(bfid, 0, 1, fcc, db, order_by = "checked")

    if not len(res):
        return (None, None)
    else:
        return (res[0]['src_bfid'], res[0]['dst_bfid'])

# log_copied(src_bfid, dst_bfid) -- log a successful copy
def log_copied_migration(src_bfid, dst_bfid, fcc, db):
    #String used to check for match of one type of error to log special
    # error message.
    DUPLICATE_KEY_ERROR = "duplicate key violates unique constraint"
    #The special error/warning message reported when a DUPLICATE_KEY_ERROR
    # match is found.
    OBSOLETE_WARNING = "The database has an obsolete unique key constraint." \
                       "This will prevent multiple copies from being scanned."

    rtn_val = 0  #So far no errors.

    if USE_CLERKS:
        reply_ticket = fcc.set_copied(src_bfid, dst_bfid)
        if not e_errors.is_ok(reply_ticket):
            if str(reply_ticket['status'][1]).find(DUPLICATE_KEY_ERROR) != -1:
                    #The old unique constraint was on each src & dst
                    # column.  For modern migration capable of migrating
                    # to multiple copies this constraint needs to be on
                    # the pair of src & dst columns.
                    log(OBSOLETE_WARNING)

            error_log("LOG_COPIED", str(reply_ticket['status']))
            rtn_val = 1  #Error
    else:

        q = "insert into migration (src_bfid, dst_bfid, copied) \
                values ('%s', '%s', '%s');" % (src_bfid, dst_bfid,
                time2timestamp(time.time()))
        if debug:
            log("log_copied():", q)
        try:
            db.query(q)
        except pg.ProgrammingError, msg:
            if str(msg).find(DUPLICATE_KEY_ERROR) != -1:
                    #The old unique constraint was on each src & dst
                    # column.  For modern migration capable of migrating
                    # to multiple copies this constraint needs to be on
                    # the pair of src & dst columns.
                    log(OBSOLETE_WARNING)

            exc_type, exc_value = sys.exc_info()[:2]
            error_log("LOG_COPIED", str(exc_type), str(exc_value), q)
            rtn_val = 1  #Error
        except:
            exc_type, exc_value = sys.exc_info()[:2]
            error_log("LOG_COPIED", str(exc_type), str(exc_value), q)
            rtn_val = 1  #Error

    return rtn_val

#Duplication may override this.
log_copied = log_copied_migration

# log_uncopied(src_bfid, dst_bfid) -- log a successful uncopy (aka restore)
def log_uncopied_migration(src_bfid, dst_bfid, fcc, db):

    rtn_val = 0  #So far no errors.

    if USE_CLERKS:
        reply_ticket = fcc.unset_copied(src_bfid, dst_bfid)
        if not e_errors.is_ok(reply_ticket):
            error_log("LOG_UNCOPIED", str(reply_ticket['status']))
            rtn_val = 1  #Error
    else:
        q = "update migration set copied = NULL where \
                src_bfid = '%s' and dst_bfid = '%s'; \
                delete from migration where \
                src_bfid = '%s' and dst_bfid = '%s';" % \
        (src_bfid, dst_bfid, src_bfid, dst_bfid)
        if debug:
            log("log_uncopied():", q)
        try:
            db.query(q)
        except:
            exc_type, exc_value = sys.exc_info()[:2]
            error_log("LOG_UNCOPIED", str(exc_type), str(exc_value), q)
            rtn_val = 1  #Error

    return rtn_val  #Success.

#Duplication may override this.
log_uncopied = log_uncopied_migration

# log_swapped(src_bfid, dst_bfid) -- log a successful swap
def log_swapped(src_bfid, dst_bfid, fcc, db):
    if USE_CLERKS:
        reply_ticket = fcc.set_swapped(src_bfid, dst_bfid)
        if not e_errors.is_ok(reply_ticket):
            error_log("LOG_SWAPPED", str(reply_ticket['status']))
    else:
        q = "update migration set swapped = '%s' where \
                src_bfid = '%s' and dst_bfid = '%s';" % \
                        (time2timestamp(time.time()), src_bfid, dst_bfid)
        if debug:
            log("log_swapped():", q)
        try:
            db.query(q)
        except:
            exc_type, exc_value = sys.exc_info()[:2]
            error_log("LOG_SWAPPED", str(exc_type), str(exc_value), q)
    return

# log_unswapped(src_bfid, dst_bfid) -- log a successful unswap (aka restore)
def log_unswapped(src_bfid, dst_bfid, fcc, db):
    if USE_CLERKS:
        reply_ticket = fcc.unset_swapped(src_bfid, dst_bfid)
        if not e_errors.is_ok(reply_ticket):
            error_log("LOG_UNSWAPPED", str(reply_ticket['status']))
    else:
        q = "update migration set swapped = NULL where \
                src_bfid = '%s' and dst_bfid = '%s';" % \
                        (src_bfid, dst_bfid)
        if debug:
            log("log_unswapped():", q)
        try:
            db.query(q)
        except:
            exc_type, exc_value = sys.exc_info()[:2]
            error_log("LOG_UNSWAPPED", str(exc_type), str(exc_value), q)
    return

# log_checked(src_bfid, dst_bfid) -- log a successful readback
def log_checked(src_bfid, dst_bfid, fcc, db):
    if USE_CLERKS:
        reply_ticket = fcc.set_checked(src_bfid, dst_bfid)
        if not e_errors.is_ok(reply_ticket):
            error_log("LOG_CHECKED", str(reply_ticket['status']))
    else:
        q = "update migration set checked = '%s' where \
                src_bfid = '%s' and dst_bfid = '%s';"%(
                        time2timestamp(time.time()), src_bfid, dst_bfid)
        if debug:
            log("log_checked():", q)
        try:
            db.query(q)
        except:
            exc_type, exc_value = sys.exc_info()[:2]
            error_log("LOG_CHECKED", str(exc_type), str(exc_value), q)
    return

# log_closed(src_bfid, dst_bfid) -- log a successful readback after closing
def log_closed(src_bfid, dst_bfid, fcc, db):
    if USE_CLERKS:
        reply_ticket = fcc.set_closed(src_bfid, dst_bfid)
        if not e_errors.is_ok(reply_ticket):
            error_log("LOG_CLOSED", str(reply_ticket['status']))
    else:
        q = "update migration set closed = '%s' where \
                src_bfid = '%s' and dst_bfid = '%s';"%(
                        time2timestamp(time.time()), src_bfid, dst_bfid)
        if debug:
            log("log_closed():", q)
        try:
            db.query(q)
        except:
            exc_type, exc_value = sys.exc_info()[:2]
            error_log("LOG_CLOSED", str(exc_type), str(exc_value), q)
    return

# log_history(src_vol, dst_vol) -- log a migration history
def log_history(src_vol, dst_vol, vcc, db):
    MY_TASK = "LOG_HISTORY"

    if USE_CLERKS:
        reply_ticket = vcc.set_migration_history(src_vol, dst_vol)
        if not e_errors.is_ok(reply_ticket):
            error_log(MY_TASK, "111", str(reply_ticket['status']))
            return 1  #Error

        return 0  #Success
    else:
        # Obtain the unique volume id for the source and destination volumes.
        src_vol_id = get_volume_id(MY_TASK, src_vol, db)
        if src_vol_id == None:
            #An error occured.  get_volume_id() reports it own errors.
            return None
        dst_vol_id = get_volume_id(MY_TASK, dst_vol, db)
        if dst_vol_id == None:
            #An error occured.  get_volume_id() reports it own errors.
            return None

        # Determine if this pair is already in the migration_history table.
        q = "select * from migration_history " \
            " where src_vol_id = %s and dst_vol_id = %s" \
            % (src_vol_id, dst_vol_id)
        try:
            res = db.query(q).getresult()
        except:
            exc_type, exc_value = sys.exc_info()[:2]
            error_log(MY_TASK, str(exc_type), str(exc_value), q)
            return 1  #Error
        # If we found something, don't try anything again.
        if len(res) > 0:
            log(MY_TASK, "source volume %s (%s) and destination volume "
                "%s (%s) are already recorded as migrated at %s" \
                % (src_vol, src_vol_id, dst_vol, dst_vol_id, res[0][2]))
            return 0  #Success

        # Insert this volume combintation into the migration_history table.
        q = "insert into migration_history (src, src_vol_id, dst, dst_vol_id) values \
                ('%s', '%s', '%s', '%s');"%(src_vol, src_vol_id, dst_vol, dst_vol_id)
        if debug:
            log("log_history():", q)
        try:
            res = db.query(q)
        except:
            exc_type, exc_value = sys.exc_info()[:2]
            #If the volume is being rerun, ignore the unique constraint
            # error from postgres.
            if str(exc_value).startswith(
                "ERROR:  duplicate key violates unique constraint"):
                res = "0"
            else:
                error_log(MY_TASK, str(exc_type), str(exc_value), q)
                return 1  #Error

        try:
            #Make sure this is a numeric type.  If it is, then we know that
            # the insert succeeded.  This value is the oid or the row.
            long(res)

            ok_log(MY_TASK, "set %s to %s as closed" % (src_vol, dst_vol))
        except ValueError, msg:
            #Should never happen with unique volume ids.
            error_log(MY_TASK,
                      "did not set %s to %s as closed: %s" \
                      % (src_vol, dst_vol, str(msg)))
            return 1  #Error

        return 0  #Success

def log_history_closed(src_vol, dst_vol, vcc, db):
    MY_TASK = "LOG_HISTORY_CLOSED"
    if USE_CLERKS:
        reply_ticket = vcc.set_migration_history_closed(src_vol, dst_vol)
        if not e_errors.is_ok(reply_ticket):
            error_log(MY_TASK, str(reply_ticket['status']))
            return 1  #Error
    else:
        # Obtain the unique volume id for the source and destination volumes.
        src_vol_id = get_volume_id(MY_TASK, src_vol, db)
        if src_vol_id == None:
            #An error occured.  get_volume_id() reports it own errors.
            return None
        dst_vol_id = get_volume_id(MY_TASK, dst_vol, db)
        if dst_vol_id == None:
            #An error occured.  get_volume_id() reports it own errors.
            return None

        #Update the closed_time column in the migration_history table
        # for these two volumes.
        q = "update migration_history set closed_time = current_timestamp " \
            "where migration_history.src_vol_id = '%s' " \
            "      and migration_history.dst_vol_id = '%s';" % \
            (src_vol_id, dst_vol_id)

        if debug:
            log("log_history_closed():", q)
        try:
            res = db.query(q)
        except:
            exc_type, exc_value = sys.exc_info()[:2]
            error_log(MY_TASK, str(exc_type), str(exc_value), q)
            return 1

        res = long(res)  #Make sure this is a numeric type.

        if res == 1:
            ok_log(MY_TASK, "set %s to %s as closed" % (src_vol, dst_vol))
            return 0  #Success
        elif res > 0:
            #Should never happen with unique volume ids.
            error_log(MY_TASK,
                      "set %s %s to %s as closed" % (res, src_vol, dst_vol))
            return 1  #Error
        else:
            error_log(MY_TASK,
                      "did not set %s to %s as closed" % (src_vol, dst_vol))
            return 1  #Error

        return None  #Should never happen.

#Return True if the source volume has all of its destination volumes
# recorded in the migration_history table.  False otherwise.  Errors
# return None.
def is_migration_history_done(MY_TASK, src_vol, db):
    # MY_TASK - string to use in log() and error_log().
    # src_vol - string respresenting the source volume to check if all
    #           pairs of this volume are done or closed
    # db - A pg.DB instantiated object.

    if USE_CLERKS:
        #This mode not yet implemented for this function.
        error_log(MY_TASK, "Clerk implementation does not exist yet.")
        return None  #Error
    else:
        # Obtain the unique volume id for the source volume.
        src_vol_id = get_volume_id(MY_TASK, src_vol, db)
        if src_vol_id == None:
            #An error occured.  get_volume_id() reports it own errors.
            return None

        #Currently migrated_to() only supports direct DB access.
        to_volume_list = migrated_to(src_vol, db)

        for dst_volume in to_volume_list:
            # Obtain the unique volume id for the destination volume.
            dst_vol_id = get_volume_id(MY_TASK, dst_volume, db)
            if dst_vol_id == None:
                #An error occured.  get_volume_id() reports it own errors.
                return None

            q = "select * from migration_history " \
                "where src_vol_id = '%s' and dst_vol_id = '%s';" \
                % (src_vol_id, dst_vol_id)

            try:
                res = db.query(q).dictresult()
            except:
                exc_type, exc_value = sys.exc_info()[:2]
                error_log(MY_TASK, str(exc_type), str(exc_value), q)
                return None  #Error

            if len(res) == 0:
                #Their is not a record made for this source and destination
                # combination.
                return False
            elif not res[0]['time']:
                #This pair of source and destination volumes are not
                # recorded as closed.
                return False

        #All the volume pairs have been found to be recored as closed in
        # the migration_history table.
        return True

#Return True if the destination volume has all of its source volumes
# recorded in the migration_history table and the "closed_time" field
# is filled in.  False otherwise.  Errors return None.
def is_migration_history_closed(MY_TASK, dst_vol, db):
    # MY_TASK - string to use in log() and error_log().
    # src_vol - string respresenting the source volume to check if all
    #           pairs of this volume are done or closed
    # db - A pg.DB instantiated object.

    if USE_CLERKS:
        #This mode not yet implemented for this function.
        error_log(MY_TASK, "Clerk implementation does not exist yet.")
        return None  #Error
    else:
        # Obtain the unique volume id for the destination volume.
        dst_vol_id = get_volume_id(MY_TASK, dst_vol, db)
        if dst_vol_id == None:
            #An error occured.  get_volume_id() reports it own errors.
            return None

        #Currently migrated_to() only supports direct DB access.
        from_volume_list = migrated_from(dst_vol, db)

        for src_volume in from_volume_list:
            # Obtain the unique volume id for the source volume.
            src_vol_id = get_volume_id(MY_TASK, src_volume, db)
            if src_vol_id == None:
                #An error occured.  get_volume_id() reports it own errors.
                return None

            q = "select * from migration_history " \
                "where src_vol_id = '%s' and dst_vol_id = '%s';" \
                % (src_vol_id, dst_vol_id)

            try:
                res = db.query(q).dictresult()
            except:
                exc_type, exc_value = sys.exc_info()[:2]
                error_log(MY_TASK, str(exc_type), str(exc_value), q)
                return None  #Error

            if len(res) == 0:
                #Their is not a record made for this source and destination
                # combination.
                return False
            elif not res[0]['closed_time']:
                #This pair of source and destination volumes are not
                # recorded as closed.
                return False

        #All the volume pairs have been found to be recored as closed in
        # the migration_history table.
        return True

def get_volume_id(MY_TASK, volume, db):
    # MY_TASK - string to use in log() and error_log().
    # volume - string respresenting the volume get its DB id.
    # db - A pg.DB instantiated object.

    if USE_CLERKS:
        #This mode not yet implemented for this function.  More specificaly,
        # it should not be needed.
        error_log(MY_TASK, "Clerk implementation does not exist yet.")
        return None  #Error
    else:
        # Obtain the unique volume id for the destination volume.
        q = "select id from volume where label = '%s'" % (volume,)

        try:
            res = db.query(q).getresult()
            volume_id =  res[0][0]  #volume id
        except:
            exc_type, exc_value = sys.exc_info()[:2]
            error_log(MY_TASK, str(exc_type), str(exc_value), q)
            return None  #Error

        return volume_id

#Return the volume that the bfid refers to.
"""
def get_volume_from_bfid(bfid, fcc, db):
        if not enstore_functions3.is_bfid(bfid):
                return False

        q = "select label from volume,file where file.volume = volume.id " \
            " and file.bfid = '%s';" % (bfid,)

        try:
                res = db.query(q).getresult()
                return res[0][0]  #volume
        except:
                exc_type, exc_value = sys.exc_info()[:2]
                error_log("get_volume_from_bfid():", str(exc_type),
                          str(exc_value), q)

        return None
"""

#Returns false if the tape is marked NOTALLOWED or NOACCESS.
def __is_volume_allowed(volume_info):
    if volume_info == None:
        return False

    if type(volume_info) != types.DictType:
        raise TypeError("expected volume information; got %s instead" \
                        % (type(volume_info),))

    not_allowed_list = ("NOACCESS", "NOTALLOWED")

    if volume_info['system_inhibit'][0] in not_allowed_list:
        return False
    if volume_info['user_inhibit'][0] in not_allowed_list:
        return False

    return True

#Returns false if the tape is marked NOTALLOWED or NOACCESS.
def is_volume_allowed(volume, vcc, db):
    if not enstore_functions3.is_volume(volume):
        return False

    volume_dict = get_volume_info("", volume, vcc, db)
    if volume_dict == None:
        return False

    return __is_volume_allowed(volume_dict)

#Return the media_type that the bfid or volume refers to.  The first
# argument may also be a file path in pnfs.  It may also be a (short) library
# name now too.
def get_media_type(arguement, db):
	if enstore_functions3.is_bfid(arguement):
		q = "select media_type from volume,file where " \
		    " file.volume = volume.id and file.bfid = '%s';" % \
		    (arguement,)
		library = ""  #set empty to skip
	elif enstore_functions3.is_volume(arguement):
		q = "select media_type from volume where " \
		    " label = '%s';" % (arguement,)
		library = ""  #set empty to skip
	elif is_library(arguement):
		library = arguement
		q = "select media_type from volume where " \
		    "volume.library = '%s' limit 1;" % (library,)
	elif chimera.is_chimera_path(arguement, check_name_only = 1):
		try:
			t = chimera.Tag(arguement)
			library = t.get_library()
		except (OSError, IOError):
			exc_type, exc_value = sys.exc_info()[:2]
			error_log("get_media_type", str(exc_type),
				  str(exc_value), arguement)
			return None
		q = "select media_type from volume where " \
		    "volume.library = '%s' limit 1;" % (library,)
	else:
		return False


	try:
		res = db.query(q).getresult()
		if len(res) == 0:
			return None
		media_type = res[0][0]  #media_type
	except:
		exc_type, exc_value = sys.exc_info()[:2]
		error_log("get_media_type", str(exc_type),
			  str(exc_value), q)
		media_type = None

	#This is a hack for the ADIC.  Originally, the ADIC didn't know
	# what an LTO tape was, so every LTO1 and LTO2 was inserted as
	# a 3480 tape.  Thus, if we find the library is LTO OR LTO2
	# then we need to use this corrected type.  This only works if
	# the ADIC never gets LTO3 or LTO4 drives, considering the
	# end-of-life schedule for it, this shouldn't be a problem.
	if library and library.upper().find("LTO2") != -1:
		media_type = "LTO2"
	### Since, LTO1s were the first type, they could be LTO or LTO1.
	elif library and re.compile("LTO$").match(library) != None \
		 or library.upper().find("LTO1") != -1:
		media_type = "LTO1"

	return media_type

#Report if the volume pair was migrated or duplicated.
def get_migration_type(src_vol, dst_vol, vcc, db):
    if not src_vol or not dst_vol:
        #For tapes that are not yet migrated/duplicated, the dst_vol
        # will be None here.
        return None

    migration_result = None
    duplication_result = None
    cloning_result = None

    if USE_CLERKS:
        #Get the dictionary info for the source volume.
        src_volume_info = get_volume_info("", src_vol, vcc, db)
        if src_volume_info == None:
            return None  #info not found
        #Get the dictionary info for the destination volume.
        dst_volume_info = get_volume_info("", dst_vol, vcc, db)
        if dst_volume_info == None:
            return None  #info not found
        #Get the files that have been migrated, duplicated or cloned
        # between these two tapes.
        ml_reply = vcc.list_migrated_files(src_vol, dst_vol)
        if not e_errors.is_ok(ml_reply):
            return None  #info not found
        #Get the files that have been duplicated between these two tapes.
        dl_reply = vcc.list_duplicated_files(src_vol, dst_vol)
        if not e_errors.is_ok(dl_reply):
            return None  #info not found

        ## These first two cases look for duplication.

        #Check the system inhibit.
        if src_volume_info['system_inhibit'][1] in ('duplicating',
                                                    'duplicated'):
            duplication_result = "DUPLICATION"
        #Check for files reported in the database table.
        elif len(dl_reply['duplicated_files']) > 0:
            duplication_result = "DUPLICATION"

        ## Now determine if migration was done.

        #Check the system inhibit.
        if src_volume_info['system_inhibit'][1] in ('migrating',
                                                    'migrated'):
            migration_result = "MIGRATION"
        #Make sure that we have migrated files.
        ## It subtracts the duplicated count from the migrated count, because
        ## some tapes were started with one, but finished with the other.
        ## This will allow migration_result to be set, so that the
        ## if logic at the end of this function will report the inconsistant
        ## message.
        elif len(ml_reply['migrated_files']) - len(dl_reply['duplicated_files']) > 0:
            migration_result = "MIGRATION"

        ## Lastly, determine if cloning was done.
        if src_volume_info['media_type'] == dst_volume_info['media_type']:
            cloning_result = "CLONING"

    else:
        try:
            q_d = "select v1.label, v2.label " \
                  "from volume v1, volume v2 " \
                  "where v1.label = '%s' and v2.label = '%s' " \
                  "  /* In the line below, the double %% signs are for " \
                  "   * python's parser to leave one literal percent " \
                  "   * sign to be passed to the sql statement to use " \
                  "   * as the special pattern matching character. */ " \
                  "  /* The escaped underscores are for the sql " \
                  "   * to literally match an underscore, not any " \
                  "   * single character LIKE usually matches it to. */ " \
                  "  and (v2.file_family like '%%/_copy/_[0-9]' escape '/'" \
                  "      or " \
                  "       (v1.system_inhibit_1 in ('duplicating', " \
                  "                                'duplicated') " \
                  "        or (select count(alt_bfid) " \
                  "            from file f1,file f2,file_copies_map " \
                  "            where f1.volume = v1.id " \
                  "              and f1.bfid = file_copies_map.bfid" \
                  "              and f2.volume = v2.id " \
                  "              and f2.bfid = file_copies_map.alt_bfid " \
                  "            limit 1) " \
                  "           > 0)); " \
                  % (src_vol, dst_vol)
            q_m = "select v1.label, v2.label " \
                  "from volume v1, volume v2 " \
                  "where v1.label = '%s' and v2.label = '%s' " \
                  "  and (v2.file_family like '%%-MIGRATION' " \
                  "      or " \
                  "       (v1.system_inhibit_1 in ('migrating', " \
                  "                                'migrated') " \
                  "        or (select count(dst_bfid) " \
                  "            from file, migration " \
                  "            where v1.id = file.volume " \
                  "            and (file.bfid = migration.src_bfid or " \
                  "                 file.bfid = migration.dst_bfid) " \
                  "            /* Be sure to exclude duplication! */" \
                  "            and v1.system_inhibit_1 not in " \
                  "                                  ('duplicating', " \
                  "                                   'duplicated') " \
                  "            limit 1) " \
                  "           > 0));" \
                  % (src_vol, dst_vol)
            q_c = "select v1.label, v2.label " \
                  "from volume v1, volume v2 " \
                  "where v1.label = '%s' and v2.label = '%s' " \
                  "  and (v1.system_inhibit_1 in ('cloning', 'cloned') " \
                  "       or v1.media_type = v2.media_type); " \
                  % (src_vol, dst_vol)

            res = db.query(q_m).getresult()
            if len(res) != 0:
                    migration_result = "MIGRATION"
            res = db.query(q_d).getresult()
            if len(res) != 0:
                    duplication_result = "DUPLICATION"
            res = db.query(q_c).getresult()
            if len(res) != 0:
                    cloning_result = "CLONING"
        except IndexError:
                return None

    return __get_migration_type(migration_result, duplication_result,
                                None, cloning_result)

def __get_migration_type(migration_result, duplication_result,
                         multiple_copy_result, cloning_result):
    #print "migration_result:", migration_result
    #print "duplication_result:", duplication_result
    #print "cloning_result:", cloning_result
    #print "multiple_copy_result:", multiple_copy_result
    if migration_result and duplication_result:
        return "The metadata is inconsistent between migration " \
               "and duplication."
    elif (not migration_result and cloning_result) and duplication_result:
        #If duplicating to the same media...
        return "DUPLICATION"
    elif cloning_result:
        return "CLONING"
    elif migration_result:
        return "MIGRATION"
    elif duplication_result:
        return "DUPLICATION"
    elif multiple_copy_result:
        return "MULTIPLE_COPY"

    return None


#Helper for get_multiple_copy_bfids() and is_multiple_copy_bfid().
def __multiple_copy(bfid, db):
    # Extract the multiple copy list; exclude destinations that are unknown.
    q = "select alt_bfid from file_copies_map,file " \
        "where file_copies_map.bfid = '%s'" \
        "  and alt_bfid = file.bfid " \
        "  and file.deleted in ('y', 'n')" % (bfid,)
    res = db.query(q).getresult()
    return res

#Report the multiple copies a file has.
def get_multiple_copy_bfids(bfid, db):

    res = __multiple_copy(bfid, db)

    multiple_copy_list = []
    for row in res:
        multiple_copy_list.append(row[0])

    return multiple_copy_list

#Report if the bfid is a multiple copy bfid.
def is_multiple_copy_bfid(bfid, db):

    # Extract the multiple copy list; exclude destinations that are unknown.
    q = "select alt_bfid from file_copies_map,file " \
        "where file_copies_map.alt_bfid = '%s'" \
        "  and alt_bfid = file.bfid " \
        "  and file.deleted in ('y', 'n')" % (bfid,)
    res = db.query(q).getresult()

    if len(res) > 0:
        return True

    return False

#Helper for get_original_copy_bfid() and is_original_copy_bfid().
def __original_copy(bfid, db):
    # Extract the multiple copy list; exclude destinations that are unknown.
    q = "select file_copies_map.bfid from file_copies_map,file " \
        "where file_copies_map.alt_bfid = '%s'" \
        "  and file_copies_map.bfid = file.bfid " \
        "  and file.deleted in ('y', 'n')" % (bfid,)
    res = db.query(q).getresult()
    return res

#Report the original copy a file has.
def get_original_copy_bfid(bfid, db):

    res = __original_copy(bfid, db)

    original_copy_list = []
    for row in res:
        original_copy_list.append(row[0])

    return original_copy_list

#Report the root original copy a file has.
def get_the_original_copy_bfid(bfid, db):

    current_bfid = bfid
    res = -1 #dummy starter value
    while current_bfid:
        res = __original_copy(current_bfid, db)
        if len(res) == 0:
            return current_bfid
        elif len(res) == 1:
            current_bfid = res[0][0]
        else:
            #Should never happen!!!
            raise ValueError("Too many bfids found")

    return None

#Report if the bfid is an original copy bfid.
def is_original_copy_bfid(bfid, db):

    res = __original_copy(bfid, db)

    if len(res) > 0:
        return False

    return True


#
def search_directory(original_path):
	##
	## Determine the deepest directory that exists.
	##
	mig_dir = chimera.get_directory_name(migration_path(original_path, {}))
	search_mig_dir = mig_dir
	while 1:
		#We need to go through all this looping to find
		#a Migration directory that exists, since any new
		#Migration directory isn't created until the first
		#new copy is is about to be written to tape for
		#the corresponding non-Migration directory.
		try:
			os.stat(search_mig_dir)  #existance test
		except (OSError, IOError):
			if os.path.basename(search_mig_dir) == MIGRATION_DB:
				return search_mig_dir
				#break  #Didn't find it.

			#Try the next directory.
			search_mig_dir = os.path.dirname(search_mig_dir)

			if search_mig_dir == "/" \
			   or search_mig_dir == "":
				break  #Didn't find it.
			continue
		#If we get here, then we found what we were looking
		# for.
		return search_mig_dir

	return None


#Look for the media type that the file would be written to.
def search_media_type(original_path, db):
	search_dir = search_directory(original_path)
	if search_dir:
		media_type = get_media_type(search_dir, db)
	else:
		media_type = None

	return media_type

#Modify the sql result to match fcc.bfid_info() format.
def __correct_db_file_info(file_record):
    try:
        #First is the sanity cookie.
        file_record['sanity_cookie'] = (file_record['sanity_size'],
                                        file_record['sanity_crc'])
    except KeyError:
        pass
    try:
        del file_record['sanity_size']
    except KeyError:
        pass
    try:
        del file_record['sanity_crc']
    except KeyError:
        pass

    return file_record

#Obtain information for the bfid.
def get_file_info(MY_TASK, bfid, fcc, db):
    #use_clerks = USE_CLERKS

    use_clerks = True
    # SFA: force to always use file clerk instead of local query.
    # FC provides file package information we need
    if use_clerks:
        reply_ticket = fcc.bfid_info(bfid)
        if not e_errors.is_ok(reply_ticket):
            error_log(MY_TASK, "%s info not found: %s" \
                      % (bfid, reply_ticket['status']))
            return None
        return reply_ticket
    else:
        # get file info
        q = "select bfid, label as external_label, location_cookie, \
             pnfs_id as pnfsid, update, uid, gid, drive, \
             case when deleted = 'y' then '%s' \
                  when deleted = 'n' then '%s' \
                  else '%s' \
             end as deleted, \
             pnfs_path as pnfs_name0, size, crc as complete_crc, \
             sanity_size, sanity_crc \
             from file, volume \
             where file.volume = volume.id and \
             bfid = '%s';" % (YES, NO, UNKNOWN, bfid,)
        if debug:
            log(MY_TASK, q)
        res = db.query(q).dictresult()

        # does it exist?
        if not len(res):
            error_log(MY_TASK, "%s does not exist in db" % (bfid,))
            return None

        return_copy = copy.copy(res[0])

        #Modify the sql result to match fcc.bfid_info() format.
        return_copy = __correct_db_file_info(return_copy)

        return_copy['status'] = (e_errors.OK, None)

        return return_copy

volume_info_cache = {}  #Keyed by volume label.

#Obtain information for the volume.
def get_volume_info(MY_TASK, volume, vcc, db, use_cache=False):
    global volume_info_cache

    #First see if we should use the cache.
    if use_cache:
        return_copy = volume_info_cache.get(volume)
        if return_copy:
            return return_copy

    if USE_CLERKS:
        reply_ticket = vcc.inquire_vol(volume)
        if not e_errors.is_ok(reply_ticket):
            error_log(MY_TASK, "%s info not found: %s" \
                      % (volume, reply_ticket['status']))
            return None
    else:
        #get volume info
        q = "select label as external_label, block_size as blocksize, \
                    capacity_bytes, declared, eod_cookie, first_access, \
                    last_access, library, media_type, remaining_bytes, \
                    sum_mounts, sum_rd_access, sum_rd_err, sum_wr_access, \
                    sum_wr_err, \
                    system_inhibit_0, system_inhibit_1, \
                    user_inhibit_0, user_inhibit_1, \
                    si_time_0, si_time_1, \
                    storage_group || '.' || file_family || '.' || wrapper as volume_family, \
                    write_protected, comment, modification_time \
             from volume \
             where volume.label = '%s';" % (volume,)

        if debug:
            log(MY_TASK, q)
        res = db.query(q).dictresult()

        # does it exist?
        if not len(res):
            error_log(MY_TASK, "%s does not exist in db" % (volume,))
            return None

        return_copy = copy.copy(res[0])

        #Modify the sql result to match vcc.inquire_vol() format.

        try:
            #First is the system inhibit.
            return_copy['system_inhibit'] = [res[0]['system_inhibit_0'],
                                             res[0]['system_inhibit_1']]
        except KeyError:
            pass
        try:
            del return_copy['system_inhibit_0']
        except KeyError:
            pass
        try:
            del return_copy['system_inhibit_1']
        except KeyError:
            pass

        try:
            #Second is the user inhibit.
            return_copy['user_inhibit'] = [res[0]['user_inhibit_0'],
                                           res[0]['user_inhibit_1']]
        except KeyError:
            pass
        try:
            del return_copy['user_inhibit_0']
        except KeyError:
            pass
        try:
            del return_copy['user_inhibit_1']
        except KeyError:
            pass

        try:
            # Third is the si_time.
            return_copy['si_time'] = (res[0]['si_time_0'],
                                      res[0]['si_time_1'])
        except KeyError:
            pass
        try:
            del return_copy['si_time_0']
        except KeyError:
            pass
        try:
            del return_copy['si_time_1']
        except KeyError:
            pass

        return_copy['status'] = (e_errors.OK, None)

    volume_info_cache[volume] = return_copy
    return return_copy

def get_volume_info_for_bfid(MY_TASK, bfid, vcc, fcc, db):
    bfid_dict = get_file_info(MY_TASK, bfid, fcc, db)
    if bfid_dict == None:
        return None

    volume_dict = get_volume_info(MY_TASK, bfid_dict['external_label'],
                                  vcc, db)
    if volume_dict == None:
        return None

    return volume_dict

#Return the list of files to migrate for the volume.

def get_tape_list(MY_TASK, volume, fcc, db, intf, all_files = False):
    if USE_CLERKS:
        list_ticket = fcc.tape_list(volume)

        # Don't ever include unknown files.
        if intf.with_deleted:
            allowed_deleted_states = [YES, NO]
        else:
            allowed_deleted_states = [NO]  #Don't allow deleted files.

        #Get the list of all bad files.
        if intf.skip_bad:
            bad_ticket = fcc.show_bad()
            if e_errors.is_ok(bad_ticket):
                bad_files = bad_ticket['bad_files'] #A list of dictionaries.
            else:
                bad_files = []
        else:
            bad_files = []
        #Get just the list of bad bfids for this volume.  If intf.skip_bad
        # was given bad_files will be empty at this point.
        bad_bfids = []
        for bad_record in bad_files:
            if bad_record['label'] == volume:
                bad_bfids.append(bad_record['bfid'])

        return_list = []
        for file_record in list_ticket['tape_list']:
            if intf.scan:
                if get_bfids(file_record['bfid'], fcc, db) == (None, None):
                    continue  #Not a migration destination file to scan.
            else:
                if file_record['deleted'] not in allowed_deleted_states:
                    continue  #Skip unknown file or maybe deleted file.
                if file_record['bfid'] in bad_bfids:
                    continue  #Skip bad file.

            # This file is not in an excluded category, so lets migrate it.
            return_list.append(file_record)
    else:
        if intf.scan:
            #Here, all targets are destination.
            migration_match = "dst_bfid"
        else:
            migration_match = "src_bfid"
        if all_files:
            use_deleted_sql = "or deleted in ('y', 'u')"
            use_empty_sql = ""
        elif intf.with_deleted:
            use_deleted_sql = "or deleted = 'y'"
            use_empty_sql = "and pnfs_path != ''"
        elif intf.force:
            use_deleted_sql = "or (deleted = 'y' and migration.dst_bfid is not NULL)"
            use_empty_sql = "and pnfs_path != ''"
        else:
            use_deleted_sql = "or migration.dst_bfid is not NULL"
            use_empty_sql = "and pnfs_path != ''"
        if intf.skip_bad:
            use_skip_bad = "and bad_file.bfid is NULL"
        else:
            use_skip_bad = ""

        q = ( "select file.bfid, label as external_label, location_cookie, "
                " pnfs_id as pnfsid, update, uid, gid, drive, "
                " case when deleted = 'y' then '%s' "
                "      when deleted = 'n' then '%s' "
                "      else '%s' "
                " end as deleted, "
                " pnfs_path as pnfs_name0, size, crc as complete_crc, "
                " sanity_size, sanity_crc, "
                " package_id, active_package_files_count, package_files_count, "
                " archive_status, archive_mod_time, original_library, "
                " cache_status, cache_mod_time, cache_location "
            " from file "
            " left join bad_file on bad_file.bfid = file.bfid "
            " join volume on file.volume = volume.id "
            " left join migration on migration.%s = file.bfid "
            " where file.volume = volume.id and label = '%s' "
            "       and (deleted = 'n' %s) %s "
            " %s "
            " order by location_cookie;"
            % (YES, NO, UNKNOWN, migration_match, volume,
               use_deleted_sql, use_empty_sql, use_skip_bad)
            )

        if debug:
            log(MY_TASK, q)

        return_list = db.query(q).dictresult()

        for i in range(len(return_list)):
            #Modify the sql result to match fcc.bfid_info() format.
            return_list[i] = __correct_db_file_info(return_list[i])

    if debug:
        log(MY_TASK,
            "found %d files to migrate on %s" % (len(return_list), volume))

    return return_list #list of file record dictionaries

##########################################################################

def mark_deleted(MY_TASK, bfid, fcc, db):
    """
    mark bfid deleted in the file table
    """
    q = "select deleted from file where bfid = '%s';" % (bfid)
    res = db.query(q).getresult()
    if len(res):
        if res[0][0] != YES:
            if not fcc:
                # get its own file clerk client
                config_host = enstore_functions2.default_host()
                config_port = enstore_functions2.default_port()
                csc = configuration_client.ConfigurationClient((config_host,
                                                                config_port))
                fcc = file_clerk_client.FileClient(csc)

            res = fcc.set_deleted('yes', bfid = bfid)
            if res['status'][0] == e_errors.OK:
                ok_log(MY_TASK, "set %s deleted" % (bfid,))
            else:
                error_log(MY_TASK, "failed to set %s deleted" % (bfid,))
                return 1
        else:
            ok_log(MY_TASK, "%s has already been marked deleted" % (bfid,))

    return 0

def mark_undeleted(MY_TASK, bfid, fcc, db):
    """
    mark bfid undeleted in the file table
    """
    q = "select deleted from file where bfid = '%s';" % (bfid)
    res = db.query(q).getresult()
    if len(res):
        if res[0][0] != NO:
            if not fcc:
                # get its own file clerk client
                config_host = enstore_functions2.default_host()
                config_port = enstore_functions2.default_port()
                csc = configuration_client.ConfigurationClient((config_host,
                                                                config_port))
                fcc = file_clerk_client.FileClient(csc)

            res = fcc.set_deleted('no', bfid = bfid)
            if res['status'][0] == e_errors.OK:
                ok_log(MY_TASK, "set %s undeleted" % (bfid,))
            else:
                error_log(MY_TASK, "failed to set %s undeleted" % (bfid,))
                return 1
        else:
            ok_log(MY_TASK, "%s has already been marked undeleted" % (bfid,))

    return 0


##########################################################################

# migration_path(path) -- convert path to migration path
# a path is of the format: /pnfs/fs/usr/X/...
#                          a migration path is: /pnfs/fs/usr/Migration/X/...
# deleted is either 'no' or 'yes'; anything else is equal to 'no'
def migration_path(path, file_record, deleted = NO):
    admin_mount_points = chimera.get_enstore_admin_mount_point()
    if len(admin_mount_points) == 0:
        if file_record.get('deleted', None) == NO and deleted == NO:
            #If the admin path is not mounted, use the normal path...
            mig_dir, fname = os.path.split(path)
        else:
            #We have a deleted file without the /pnfs/fs mount point.

            if path[0] != "/":
                #We have a relative path.  We will get this for deleted files.
                #Return None.  The caller should then call this function again
                # with the original absolute path.
                return None

            #Need to find the mounted non-/pnfs/fs mount point.
            search_path, fname = os.path.split(chimera.get_enstore_pnfs_path(path))
            dirname = search_path
            while dirname:
                if os.path.ismount(dirname):
                    try:
                        if namespace.is_storage_local_path(dirname):
                            mig_dir = dirname
                            break
                    except NameError:
                        #If we only have pnfs support at this time.
                        if chimera.is_chimera_path(dirname):
                            mig_dir = dirname
                            break

                    #If we get here, we have found a mountpoint, but it
                    # is not for a storage filesystem.
                    return None

                dirname = os.path.dirname(dirname)
            else:
                #We should never be able to get here.  The absolute path
                # did not find the root directory (/) in the path.
                return None


        #...just be sure to stick .m. at the beginning and to
        # limit the character count.
        use_fname = ".m.%s" % (fname,)[:chimera.PATH_MAX]
        mig_path = os.path.join(mig_dir, use_fname)
        return mig_path
    elif len(admin_mount_points) >= 1:
        admin_mount_point = admin_mount_points[0]
    else:
        return None

    if not admin_mount_point:
        admin_mount_point = DEFAULT_FS_PNFS_PATH

    #Make the non-deleted migration path string.  Something that begins with
    # /pnfs/fs/usr/Migration/.  We need to do this before worrying about
    # deleted files, since already scanned files are normally marked deleted.
    # A re-scan needs to handle the source file marked deleted from
    # previous scan or not yet marked deleted, correctly.

    #Try the old quick way first.
    #  For paths like /pnfs/sam/dzero/... that have an extra non-PNFS
    #  directory ("sam") in the path the returned value will look like
    #  /pnfs/fs/usr/Migration/sam/dzero/.
    stripped_name_1 = chimera.strip_pnfs_mountpoint(path)
    #Already had the migration path.
    if stripped_name_1.startswith(MIGRATION_DB + "/"):
        non_deleted_path = os.path.join(admin_mount_point, stripped_name_1)
    else:
        non_deleted_path = os.path.join(admin_mount_point,
                                        MIGRATION_DB, stripped_name_1)

    #We want to use the non-deleted path if it still exists or we know that
    # the original path still exists.
    if chimera.is_admin_pnfs_path(path) or \
           file_utils.e_access(non_deleted_path, os.F_OK):
        return non_deleted_path

    #This is slower, but will catch more cases.  Don't clobber
    # non_deleted_path, it might be needed as the default return later on.
    #The reason for doing this are pnfs paths, like /pnfs/sam/dzero/...
    # having an extra non-PNFS directory ("sam") in the path, where the
    # migration path might begin with /pnfs/fs/usr/Migration/dzero/
    # or /pnfs/fs/usr/Migration/sam/dzero/ depending on the migration
    # code version originally used to migrate the file.
    try:
        #use_paths = pnfs.Pnfs().get_path(file_record['pnfsid'])
        use_paths = chimera.ChimeraFS().get_path(file_record['pnfsid'])
        for use_path in use_paths:
            stripped_name_2 = chimera.strip_pnfs_mountpoint(use_path)
            #Already had the migration path.
            if stripped_name_2.startswith(MIGRATION_DB + "/"):
                alt_non_deleted_path = os.path.join(admin_mount_point,
                                                    stripped_name_2)
            else:
                alt_non_deleted_path = os.path.join(admin_mount_point,
                                                    MIGRATION_DB,
                                                    stripped_name_2)
            #We want to use the non-deleted path if it still exists.
            if os.path.exists(alt_non_deleted_path):
                return alt_non_deleted_path
    except (OSError, IOError):
        pass

    #Handle a deleted file.
    if file_record.get('deleted', None) == YES or deleted == YES:
        old_path = os.path.join(admin_mount_point,
                                MIGRATION_DB,
                                DELETED_TMP,
                                string.join([file_record['external_label'],
                                             file_record['location_cookie']],
                                            ":"))
        if os.path.exists(old_path):
            #We want to use the old path if it still exists.
            return old_path

        #Let's make this one level deeper to create smaller directories,
        # instead of one directories where all temporary files in PNFS
        # are created.
        return os.path.join(admin_mount_point,
                            MIGRATION_DB,
                            DELETED_TMP,
                            file_record['external_label'],
                            string.join([file_record['external_label'],
                                         file_record['location_cookie']], ":"))

    return non_deleted_path

# temp_file(file_record) -- get a temporary destination file from file
def temp_file(file_record):
    if enstore_functions3.is_volume_disk(file_record['external_label']):
        #We need to treat disk files differently.
        return os.path.join(SPOOL_DIR, file_record['bfid'])

    return os.path.join(SPOOL_DIR,
                        "%s:%s" % (file_record['external_label'],
                                   file_record['location_cookie']))

##########################################################################

## This class is a customized class similar to python Queue.Queue() class.
## This may be able to be replaced with the multiprocessing module in
## python 2.6.  For compatiblity with 2.4, this is what we need.

class MigrateQueue:

    def __init__(self, maxsize, notify_every_time = True,
                 low_watermark = 1):
        self.queue = Queue.Queue(maxsize)

	self.finished = False  #Flag indicating the SENTINEL is in the queue.
	self.received_count = 0
	self.debug = debug
        self.initial_wait = True #Wait until low_watermark items are queued.
        self.low_watermark = low_watermark
        self.maxsize = maxsize

	self.r_pipe, self.w_pipe = os.pipe() #Used with processes.

        #Handle to the message-waiting-thread when processes are used.
	self.cur_thread = None

	try:
	    self.lock = multiprocessing.Lock()
        except NameError:
	    self.lock = threading.Lock()

        #If notify_every_time is true, then use the condition variable to
        # tell the consuming thread/process to go.  If false, notify
        # the waiting thread/process to go after all items have been received
        # from the queue.
        self.notify_every_time = notify_every_time
        try:
            self.cv = multiprocessing.Condition()
        except NameError:
            self.cv = threading.Condition()

    #We need to be able to control starting the pipe reading AFTER any fork()s.
    #
    # However, this also allows us to make sure that the
    # tape reading process can never overfill the buffer,
    # too.
    def start_waiting(self):

	if not USE_THREADS:
	    #If we are using processes, start a thread to continuely read
	    # items in the pipe.
	    if self.debug:
	        log("starting pipe read thread")

	    self.lock.acquire() #Acquire the lock to access a self data member.

	    try:
	        self.cur_thread = threading.Thread(
			target = self.get_from_pipe, args = ())
		self.cur_thread.start()
	    except:
	        self.lock.release()
		raise sys.exc_info()[0], sys.exc_info()[1], \
		      sys.exc_info()[2]

	    self.lock.release()

	    if self.debug:
	        log("started pipe read thread")

    def __del__(self):
        #log("INSIDE MigrateQueue.__del__()")  #DEBUG

        if self.cur_thread:
	    if self.debug:
	        log("joining thread")

	    self.lock.acquire() #Acquire the lock to access a self data member.

	    try:
	        self.cur_thread.join()
		self.cur_thread = None
	    except:
	        self.lock.release()
                Trace.handle_error()
		raise sys.exc_info()[0], sys.exc_info()[1], \
		      sys.exc_info()[2]

	    self.lock.release()

	    if self.debug:
	        log("joined thread")

        #The migration code seems to be leaving these pipes open despite
        # the explicit variable deletes of the MigrationQueue class.
        # Let us try closing them to avoid the resource leak.
        try:
            os.close(self.r_pipe)
        except:
            Trace.handle_error()
        try:
            os.close(self.w_pipe)
        except:
            Trace.handle_error()

        #log("LEAVING MigrateQueue.__del__()")  #DEBUG

    def qsize(self):
        return self.queue.qsize()

    def empty(self):
        return self.queue.empty()

    def full(self):
        return self.queue.full()

    def __get(self, block, timeout):
        #Acquire the lock to access a self data member.
	self.lock.acquire()
	try:
	    finished = self.finished #Get if the queue is finished.
        except:
	    self.lock.release()
	    raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        self.lock.release()

        #Enter the loop if:
	# 1) we are not done and waiting for the next item
	# 2) the queue is not empty
        job = None
	while (not job and not finished) or self.queue.qsize() > 0:
	    try:
	        job = self.queue.get(block, timeout)
	        if job == SENTINEL:
                    self.finished = True
		    return SENTINEL  #We are really done.
	        if job == None:
		    #Queue.get() should only return None if it was put in the
		    # queue, however, it appears to return None on its own.
		    # So, we go back to waiting for something we are looking
		    # for.
		    continue
	        break
	    except Queue.Empty:
	        job = None

        return job

    def get(self, block = False, timeout = .1):
        #Get the next file to copy and swap from the reading thread.
        if (USE_THREADS or multiprocessing_available): # and \
            #   self.notify_every_time:
            if self.debug:
                log("acquiring condition lock")
            self.cv.acquire()
            if self.debug:
                log("acquired condition lock")
            if self.notify_every_time:
                while not self.finished and \
                          ((self.initial_wait and \
                            self.queue.qsize() < self.low_watermark) \
                           or self.queue.qsize() == 0):
                    if self.debug:
                        log("waiting for condition")
                        log("self.finished", str(self.finished),
                            "self.initial_wait:", str(self.initial_wait),
                            "self.queue.qsize():", str(self.queue.qsize()),
                            "self.low_watermark:", str(self.low_watermark),
                            )

                    self.cv.wait()
            else:
                while not self.finished:
                    if self.debug:
                        log("waiting for condition")
                        log("self.finished", str(self.finished))

                    self.cv.wait()
            job = self.__get(block, timeout)
            if self.debug:
                log("releasing condition lock")
            self.cv.release()
        else:
            #This is the old way if using processes with python 2.4.  Or
            # if we only flag the condition when the queue is finished.
            while not self.finished \
                      and ((self.initial_wait \
                            and self.queue.qsize() < self.low_watermark) \
                           or self.queue.qsize() == 0):
                if self.debug:
                    log("self.finished", str(self.finished),
                        "self.initial_wait:", str(self.initial_wait),
                        "self.queue.qsize():", str(self.queue.qsize()),
                        "self.low_watermark:", str(self.low_watermark),
                        )
                time.sleep(1)
            job = self.__get(block, timeout)

        #Set a flag indicating that we have read the last item.
        if job == SENTINEL:
            if self.debug:
                log("received SENTINEL")
            #Acquire the lock to access a self data member.
            self.lock.acquire()
            self.finished = True
            self.initial_wait = False
            self.lock.release()
            return None  #We are really done.

        self.lock.acquire()
        #Disable the low_watermark threshold.
        self.initial_wait = False
        self.lock.release()

        return job

    #On Linux, there is a bug in select()
    # that prevents select from returning that a pipe is
    # available for writing until the buffer is totally
    # empty.  Since, the callback.write_obj() makes many
    # little writes, hangs were found to occur after the
    # first one, until the tape writting process started
    # reading the pipe buffer.
    # Apache has seen this before.
    def get_from_pipe(self, timeout = .1):
        MY_TASK = "GET_FROM_PIPE"

        if USE_THREADS:
	    return


        job = -1

        #Limit to return to revent reader from overwelming the writer.
	requests_obtained = 0

	#Acquire the lock to access a self data member.
	self.lock.acquire()
	try:
	    finished = self.finished
        except:
	    self.lock.release()
	    raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        self.lock.release()

        if 0: #self.debug:
	    log(MY_TASK, "job:", str(job),
		"requests_obtained:", str(requests_obtained),
		"queue.full():", str(self.queue.full()),
		"queue.finished:", str(finished))

	while not finished:
            if self.debug:
	        log(MY_TASK, "getting next file", str(timeout))

            try:
                r, w, x = select.select([self.r_pipe], [], [], timeout)
            except select.error, msg:
	        if msg.args[0] in (errno.EINTR, errno.EAGAIN):
                    #If a select (or other call) was interupted,
		    # this is not an error, but should continue.
		    continue

                #On an error, put the list ending None in the list.
		self.queue.put(SENTINEL)

		#Acquire the lock to access a self data member.
		self.lock.acquire()
                self.received_count = self.received_count + 1
                self.finished = True
	        self.lock.release()

                finished = True #End the while loop.

		break

            self.lock.acquire()
            if r:
                try:

                    #Set verbose to True for debugging.
		    job = callback.read_obj(self.r_pipe, verbose = False)

		    self.queue.put(job, block = True)
                    self.received_count = self.received_count + 1

		    if self.debug:
		        log(MY_TASK, "Queued request:", str(job))

                    #Set a flag indicating that we have read the last item.
                    if job == SENTINEL:
                        self.finished = True

                    #increment counter on success
		    requests_obtained = requests_obtained + 1
	        except (socket.error, select.error, e_errors.EnstoreError), msg:
	            if self.debug:
		        log(MY_TASK, str(msg))
	            #On an error, put the list ending SENTINEL in the list.
		    self.queue.put(SENTINEL, block = True)

                    self.received_count = self.received_count + 1
                    self.finished = True
                except e_errors.TCP_EXCEPTION:
	            if self.debug:
		        log(MY_TASK, e_errors.TCP_EXCEPTION)
                    #On an error, put the list ending None in the list.
		    self.queue.put(SENTINEL, block = True)

                    self.received_count = self.received_count + 1
                    self.finished = True
                except (KeyboardInterrupt, SystemExit):
                    self.lock.release()
                    raise sys.exc_info()[0], sys.exc_info()[1], \
                          sys.exc_info()[2]
		except:
                    self.lock.release()
                    raise sys.exc_info()[0], sys.exc_info()[1], \
                          sys.exc_info()[2]

            finished = self.finished
            self.lock.release()

        if self.debug:
	    log(MY_TASK, "queue.qsize():", str(self.queue.qsize()),
		"requests_obtained:", str(requests_obtained))

        return

    def put(self, item, block = False, timeout = .1):
        if USE_THREADS:
	    self.lock.acquire()
            self.received_count = self.received_count + 1
	    self.lock.release()


            #Notify the interested write thread that we have a
            # put a new item into the queue.  We do this for every item if
            # notify_every_time is true or if it is the sentinel value
            # (regardless if notify_every_time is true or not).
            TIMEOUT = 10
            if ((USE_THREADS or multiprocessing_available) and \
                   self.notify_every_time) or item == SENTINEL:
                while 1:
                    if self.debug:
                        log("acquiring condition lock")
                    self.cv.acquire()
                    if self.debug:
                        log("acquired condition lock")

                    try:
                        if self.queue.full():
                            #If we know it is full, skip waiting
                            # the TIMEOUT duration and notify
                            # the write thread now.
                            raise Queue.Full()

                        self.queue.put(item, block = True, timeout = TIMEOUT)
                        if item == SENTINEL:
                            #Flag the sentinel item is received.  This needs to
                            # happen here.  If not, then for a volume with
                            # fewer than (proc_limit * low_watermark) files,
                            # there is no trigger to start moving the files
                            # to new tapes.  Thus, resulting in a deadlock.
                            self.finished = True
                        if self.debug:
                            log("notifying condition")
                        self.cv.notify()
                        self.cv.release()
                        if self.debug:
                            log("released condition lock")
                        break
                    except Queue.Full:
                        log("queue is full (%s), "
                            "waiting %s more seconds" % \
                            (str(self.queue.qsize()), TIMEOUT))

                        #The write thread needs to be able to
                        # acquire the condition variable.
                        self.cv.notify()
                        self.cv.release()
                        #The need for sleep() is awful.  But without a
                        # python equivalent to the C sched_yield() function
                        # we need some way of transfering focus to the
                        # other thread.
                        time.sleep(TIMEOUT)

            else:
                if block:
                    self.queue.put(item, block = block)
                else:
                    self.queue.put(item, block = block, timeout = timeout)

        else: #processes
            #If item is the last when using processes, record it as such for
            # the sender too.
            if item == SENTINEL:
                #Acquire the lock to access a self data member.
                self.lock.acquire()
                try:
                    self.finished = True
                except:
                    self.lock.release()
                    raise sys.exc_info()[0], sys.exc_info()[1], \
                          sys.exc_info()[2]
                self.lock.release()

            if self.debug:
                log("sending next item %s on pipe" % (item,))
            callback.write_obj(self.w_pipe, item)
            if self.debug:
                log("item %s sent on pipe" % (item,))

##########################################################################

def show_migrated_from(volume_list, vcc, db):
    for vol in volume_list:
        from_list = migrated_from(vol, db)
        #We need to determine if migration or
        # duplication was used.
        try:
            mig_type = get_migration_type(from_list[0], vol, vcc, db)
            if mig_type in ["MIGRATION", "CLONING"]:
                mfrom = "<="
            elif mig_type == "DUPLICATION":
                mfrom = "<-"
            else:
                mfrom = "<=-?"
        except IndexError:
            mfrom = "<=-?"
        print "%s %s"%(vol, mfrom),
        for from_vol in from_list:
            print from_vol,
        print

def show_migrated_to(volume_list, vcc, db):
    for vol in volume_list:
        to_list = migrated_to(vol, db)
        #We need to know determine if migration or
        # duplication was used.
        try:
            mig_type = get_migration_type(vol, to_list[0], vcc, db)
            if mig_type in ["MIGRATION", "CLONING"]:
                mto = "=>"
            elif mig_type == "DUPLICATION":
                mto = "->"
            else:
                mto = "?-=>"
        except IndexError:
                mto = "?-=>"
        print "%s %s"%(vol, mto),
        for to_vol in to_list:
            print to_vol,
        print

#__find_record() is used by show_status() to quickly find the file_record
# for the bfid in the tape_list.
start_index = 0
def __find_record(tape_list, bfid):
    global start_index
    if start_index == len(tape_list):
        start_index = 0  #Start over.

    #These two variables are used to help backtrack when two file records
    # point to the same location cookie on a tape.
    repeat_start_index = False
    original_location_cookie = tape_list[start_index]['location_cookie']

    #If we happen to be migrating a disk volume, the optimazation logic
    # will not work.
    if enstore_functions3.is_location_cookie_disk(original_location_cookie):
        keep_start_index_zero = True
    else:
        keep_start_index_zero = False

    for i in xrange(start_index, len(tape_list)):
        if tape_list[i]['bfid'] == bfid:
            if not keep_start_index_zero:
                #If we have a tape location_cookie, consider increasing
                # the start_index search point.  The situation we don't
                # want to update this is when we have multiple locations
                # with the same location_cookie (which is rare, but happens).
                if repeat_start_index and \
                       tape_list[i]['location_cookie'] == \
                       tape_list[start_index]['location_cookie']:
                    pass
                else:
                    start_index = start_index + 1
            return tape_list[i]
        else:
            repeat_start_index = True
    else:
        start_index = 0
        return {}

def __reset_start_index():
    global start_index
    start_index = 0

#Return a list of dictionaries containing the migration, duplication and
# bad file status of the requested file and each paired file.
def __query_file_status(bfid, db):
    #bfid - A string of the source or destination bfid.
    #db - A pg.DB() object.

    if USE_CLERKS:
        pass  #Fix me.
    else:
        # This query retrieves all migration/duplication
        # information for this volume.  Any bad file
        # information is also recovered to save time
        # running an additional query.
        q = "select current_bfid, \
                src_bfid, dst_bfid, \
                copied, swapped, checked, closed, \
                bfid, alt_bfid, src_bad, dst_bad, debug \
         from ( \
         \
         /* \
          * This first of four unioned selects pulls out all \
          * the files that have been migrated, duplicated, \
          * cloned or totally left alone. \
          */ \
         \
         select bfids1.bfid as current_bfid, \
                case when src_bfid is not NULL \
                     then src_bfid \
                     else bfids1.bfid \
                end as src_bfid, mig1.dst_bfid, \
                mig1.copied, mig1.swapped, mig1.checked, \
                mig1.closed, mig1.remark, mc1.*, \
                bfs.bfid as src_bad, \
                bfd.bfid as dst_bad, \
                '1' as debug \
         from \
         (select f1.bfid \
            from file f1 \
            where f1.bfid = '%s' \
           ) as bfids1 \
         left join \
         migration mig1 on mig1.src_bfid = bfids1.bfid \
         left join \
         file_copies_map mc1 on (mc1.bfid = bfids1.bfid or \
                                 mc1.alt_bfid = bfids1.bfid) and \
                                (mc1.bfid = mig1.src_bfid or \
                                 mc1.bfid = mig1.dst_bfid) and \
                                (mc1.alt_bfid = mig1.src_bfid or \
                                 mc1.alt_bfid = mig1.dst_bfid) \
         left join \
         bad_file as bfs on (mig1.src_bfid = bfs.bfid or \
                             mc1.bfid = bfs.bfid or \
                             /* We only want bfids1.bfid for the \
                             source if nothing migration has \
                             happened yet. */ \
                             bfids1.bfid = bfs.bfid) \
         left join \
         bad_file as bfd on (mig1.dst_bfid = bfd.bfid or \
                             mc1.alt_bfid = bfd.bfid) \
         \
         union \
         \
         /* \
          * This second of four unioned selects pulls out all \
          * the files that have been migrated to, duplicated to, \
          * cloned to or totally left alone. \
          */ \
         \
         select bfids1.bfid as current_bfid, \
                mig1.src_bfid, \
                case when dst_bfid is not NULL \
                     then dst_bfid \
                     else bfids1.bfid \
                end as dst_bfid, \
                mig1.copied, mig1.swapped, mig1.checked, \
                mig1.closed, mig1.remark, mc1.*, \
                bfs.bfid as src_bad, \
                bfd.bfid as dst_bad, \
                '2' as debug \
         from \
         (select f1.bfid \
            from file f1 \
            where f1.bfid = '%s' \
           ) as bfids1 \
         left join \
         migration mig1 on mig1.dst_bfid = bfids1.bfid \
         left join \
         file_copies_map mc1 on (mc1.bfid = bfids1.bfid or \
                                 mc1.alt_bfid = bfids1.bfid) and \
                                (mc1.bfid = mig1.src_bfid or \
                                 mc1.bfid = mig1.dst_bfid) and \
                                (mc1.alt_bfid = mig1.src_bfid or \
                                 mc1.alt_bfid = mig1.dst_bfid) \
         left join \
         bad_file as bfs on (mig1.src_bfid = bfs.bfid or \
                             mc1.bfid = bfs.bfid) \
         left join \
         bad_file as bfd on (mig1.dst_bfid = bfd.bfid or \
                             mc1.alt_bfid = bfd.bfid) \
         \
         union \
         \
         /* \
          * This third of four unioned selects pulls out just \
          * original copies. \
          */ \
         \
         select bfids1.bfid as current_bfid, \
                NULL as src_bfid, NULL as dst_bfid, \
                NULL as copied, NULL as swapped, NULL as checked, \
                NULL as closed, \
                NULL as remark, \
                case when mig1.dst_bfid is not NULL \
                     then NULL \
                     when mc1.bfid is not NULL and mig1.src_bfid is not NULL \
                     then NULL \
                     when mc1.bfid is not NULL \
                     then mc1.bfid \
                     else bfids1.bfid \
                end as bfid, \
                case when mig1.dst_bfid is not NULL \
                     then NULL \
                     else mc1.alt_bfid \
                end as alt_bfid, \
                bfs.bfid as src_bad, \
                bfd.bfid as dst_bad, \
                '3' as debug\
         from \
         (select f1.bfid \
            from file f1 \
            where f1.bfid = '%s' \
         ) as bfids1 \
         left join \
         file_copies_map mc1 on mc1.bfid = bfids1.bfid \
         left join \
         migration mig1 on (mig1.src_bfid = mc1.bfid or \
                            mig1.dst_bfid = mc1.bfid) and \
                           (mig1.src_bfid = mc1.alt_bfid or \
                            mig1.dst_bfid = mc1.alt_bfid) \
         left join \
         bad_file as bfs on (mig1.src_bfid = bfs.bfid or \
                             mc1.bfid = bfs.bfid) \
         left join \
         bad_file as bfd on (mig1.dst_bfid = bfd.bfid or \
                             mc1.alt_bfid = bfd.bfid) \
         \
         union \
         \
         /* \
          * This forth of four unioned selects pulls out just \
          * multiple copies. \
          */ \
         \
         select bfids1.bfid as current_bfid, \
                NULL as src_bfid, NULL as dst_bfid, \
                NULL as copied, NULL as swapped, NULL as checked, \
                NULL as closed, \
                NULL as remark, \
                case when mig1.src_bfid is not NULL \
                     then NULL \
                     else mc1.bfid \
                end as bfid, \
                case when mig1.src_bfid is not NULL \
                     then NULL \
                     when mc1.alt_bfid is not NULL and mig1.dst_bfid is not NULL \
                     then mc1.alt_bfid \
                     else bfids1.bfid \
                end as alt_bfid, \
                bfs.bfid as src_bad, \
                bfd.bfid as dst_bad, \
                '4' as debug \
         from \
         (select f1.bfid \
            from file f1 \
            where f1.bfid = '%s' \
         ) as bfids1 \
         left join \
         file_copies_map mc1 on mc1.alt_bfid = bfids1.bfid \
         left join \
         migration mig1 on (mig1.src_bfid = mc1.bfid or \
                            mig1.dst_bfid = mc1.bfid) and \
                           (mig1.src_bfid = mc1.alt_bfid or \
                            mig1.dst_bfid = mc1.alt_bfid) \
         left join \
         bad_file as bfs on (mig1.src_bfid = bfs.bfid or \
                             mc1.bfid = bfs.bfid) \
         left join \
         bad_file as bfd on (mig1.dst_bfid = bfd.bfid or \
                             mc1.alt_bfid = bfd.bfid) \
         ) as blah \
         order by copied \
         ;" % (bfid, bfid, bfid, bfid)

        res = db.query(q).dictresult()
        return res

#Print out the header information.
def __print_header(src_volume, dst_volume):
    #src_volume - Name of the source volume.
    #dst_volume - Name of the destination volume.

    #In the header for the output, include the volume name in the
    # correct location.  Limit it to tape labels only; 6 in the
    # AAXX00 pattern and an addtional two to handle the L1 appended
    # to LTO tapes in ADIC robots.
    if src_volume:
        src_column_header = "(%.8s) src_bfid" % (src_volume,)
    else:
        src_column_header = "src_bfid"
    if dst_volume:
        dst_column_header = "(%.8s) dst_bfid" % (dst_volume,)
    else:
        dst_column_header = "dst_bfid"

    #if len(rows) > 0:
    #Output the header.
    print "%19s %1s%1s%1s %19s %1s%1s%1s %6s %6s %6s %6s" % \
          (src_column_header, "S", "D", "B",
           dst_column_header, "S", "D", "B",
           "copied", "swapped", "checked", "closed")


#Output to standard out the migration status information on a per-file basis.
def show_status_files(bfid_list, db, intf):
    #bfid_list - A string, or list thereof, of the source or destination bfid.
    #db - A pg.DB() instantiated object.
    #intf - A MigrateInterface() instantiated object.

    MY_TASK = "STATUS"
    exit_status = 0

#    #Fix me.  These will be necessary in order for USE_CLERKS to be set to
#    # true.
#    fcc = None
#    vcc = None
    # get its own file clerk client and volume clerk client
    config_host = enstore_functions2.default_host()
    config_port = enstore_functions2.default_port()
    csc = configuration_client.ConfigurationClient((config_host,config_port))
    fcc = file_clerk_client.FileClient(csc)
    vcc = volume_clerk_client.VolumeClerkClient(csc)

    #Make sure the list is a list.
    if type(bfid_list) != types.ListType:
        bfid_list = [bfid_list]

    if USE_CLERKS:
        error_log(MY_TASK, "Clerk DB access not implemented at this time.")
        exit_status = 1
        return exit_status

    for bfid in bfid_list:
        #Get the file record for the file.
        file_record = get_file_info(MY_TASK, bfid, fcc, db)
        if file_record == None:
            exit_status = exit_status + 1
            continue

        #Get the DB results for the bfid in the migration and file_copies_map
        # tables.
        res = __query_file_status(bfid, db)
        if not res:
            exit_status = exit_status + 1
            continue

        #Output the results.
        exit_status = exit_status + __show_status(MY_TASK, res, [file_record],
                                                  fcc, vcc, db, intf)

    return exit_status

#Output to standard out the migration status information on a per-volume basis.
def show_status_volumes(volume_list, db, intf):
    #volume_list - A string, or list thereof, of the source or destination
    #            volumes.
    #db - A pg.DB() instantiated object.
    #intf - A MigrateInterface() instantiated object.

    MY_TASK = "STATUS"
    exit_status = 0

#    #Fix me.  These will be necessary in order for USE_CLERKS to be set to
#    # true.
#    fcc = None
#    vcc = None
    # get its own file clerk client and volume clerk client
    config_host = enstore_functions2.default_host()
    config_port = enstore_functions2.default_port()
    csc = configuration_client.ConfigurationClient((config_host,config_port))
    fcc = file_clerk_client.FileClient(csc)
    vcc = volume_clerk_client.VolumeClerkClient(csc)

    #Make sure the list is a list.
    if type(volume_list) != types.ListType:
        volume_list = [volume_list]

    ## Here is the matrix that describes which method is used to show the
    ## migration status information of the volume(s).
    ##
    ##                  |     True               |        False    USE_CLERKS
    ## -----------------------------------------------------------------------
    ##                  |                        |
    ##  True            |        NA              | one db direct query
    ##  False           |        NA              | multiple db direct queries
    ##                  |
    ## USE_SINGLE_QUERY |
    ##
    ## Using one query to retrieve all the information for the volume
    ## works the fastest.

    USE_SINGLE_QUERY = True
    if USE_CLERKS:
        error_log(MY_TASK, "Clerk DB access not implemented at this time.")
        exit_status = 1
        return exit_status

    for volume in volume_list:

        #tape_list is a list of file records
        tape_list = get_tape_list(MY_TASK, volume, fcc, db, intf,
                                  all_files=True)
        # Detect empty tapes.
        if len(tape_list) == 0:
            log(MY_TASK, volume, "volume is empty")
            continue

        if USE_SINGLE_QUERY:
            #################################################################
            # Begin method using one large SQL query.  This query retrieves
            # all migration/duplication information for this volume.
            # Any bad file information is also recovered to save time
            # running an additional query.

            if USE_CLERKS:
                pass  #Fix me.
            else:
                q = "select current_bfid, \
                            src_bfid, dst_bfid, \
                            copied, swapped, checked, closed, \
                            bfid, alt_bfid, src_bad, dst_bad, debug \
                     from ( \
                     \
                     /* \
                      * This first of four unioned selects pulls out all \
                      * the files that have been migrated, duplicated, \
                      * cloned or totally left alone. \
                      */ \
                     \
                     select bfids1.bfid as current_bfid, location_cookie, \
                            case when src_bfid is not NULL \
                                 then src_bfid \
                                 else bfids1.bfid \
                            end as src_bfid, mig1.dst_bfid, \
                            mig1.copied, mig1.swapped, mig1.checked, \
                            mig1.closed, mig1.remark, mc1.*, \
                            bfs.bfid as src_bad, \
                            bfd.bfid as dst_bad, \
                            '1' as debug \
                     from \
                     (select f1.bfid, location_cookie \
                      from file f1, volume v1 \
                      where f1.volume = v1.id \
                        and v1.label = '%s' \
                      order by f1.location_cookie \
                     ) as bfids1 \
                     left join \
                     migration mig1 on mig1.src_bfid = bfids1.bfid \
                     left join \
                     file_copies_map mc1 on (mc1.bfid = bfids1.bfid or \
                                             mc1.alt_bfid = bfids1.bfid) and \
                                            (mc1.bfid = mig1.src_bfid or \
                                             mc1.bfid = mig1.dst_bfid) and \
                                            (mc1.alt_bfid = mig1.src_bfid or \
                                             mc1.alt_bfid = mig1.dst_bfid) \
                     left join \
                     bad_file as bfs on (mig1.src_bfid = bfs.bfid or \
                                         mc1.bfid = bfs.bfid or \
                                         /* We only want bfids1.bfid for the \
                                            source if nothing migration has \
                                            happened yet. */ \
                                         bfids1.bfid = bfs.bfid) \
                     left join \
                     bad_file as bfd on (mig1.dst_bfid = bfd.bfid or \
                                         mc1.alt_bfid = bfd.bfid) \
                     \
                     union \
                     \
                     /* \
                      * This second of four unioned selects pulls out all \
                      * the files that have been migrated to, duplicated to, \
                      * cloned to or totally left alone. \
                      */ \
                     \
                     select bfids1.bfid as current_bfid, location_cookie, \
                            mig1.src_bfid, \
                            case when mig1.dst_bfid is not NULL \
                                 then mig1.dst_bfid \
                                 else bfids1.bfid \
                            end as dst_bfid, \
                            mig1.copied, mig1.swapped, mig1.checked, \
                            mig1.closed, mig1.remark, mc1.*, \
                            bfs.bfid as src_bad, \
                            bfd.bfid as dst_bad, \
                            '2' as debug \
                     from \
                     (select f1.bfid, location_cookie \
                      from file f1, volume v1 \
                      where f1.volume = v1.id \
                        and v1.label = '%s' \
                      order by f1.location_cookie \
                     ) as bfids1 \
                     left join \
                     migration mig1 on mig1.dst_bfid = bfids1.bfid \
                     left join \
                     file_copies_map mc1 on (mc1.bfid = bfids1.bfid or \
                                             mc1.alt_bfid = bfids1.bfid) and \
                                            (mc1.bfid = mig1.src_bfid or \
                                             mc1.bfid = mig1.dst_bfid) and \
                                            (mc1.alt_bfid = mig1.src_bfid or \
                                             mc1.alt_bfid = mig1.dst_bfid) \
                     left join \
                     bad_file as bfs on (mig1.src_bfid = bfs.bfid or \
                                         mc1.bfid = bfs.bfid) \
                     left join \
                     bad_file as bfd on (mig1.dst_bfid = bfd.bfid or \
                                         mc1.alt_bfid = bfd.bfid) \
                     \
                     union \
                     \
                     /* \
                      * This third of four unioned selects pulls out just \
                      * original copies. \
                      * \
                      * The inner query can report multiple rows if a \
                      * primary duplication destination file has a multiple \
                      * copy.  The source multiple row is left after the \
                      * outer query, the duplication destination copy \
                      * is removed because its 'bfid' column is empty. \
                      */ \
                     \
                     select bfids1.bfid as current_bfid, location_cookie, \
                            NULL as src_bfid, NULL as dst_bfid, \
                            NULL as copied, NULL as swapped, NULL as checked, \
                            NULL as closed, \
                            NULL as remark, \
                            case when mig1.dst_bfid is not NULL \
                                 then NULL \
                                 when mc1.bfid is not NULL and mig1.src_bfid is not NULL \
                                 then NULL \
                                 when mc1.bfid is not NULL \
                                 then mc1.bfid \
                                 else bfids1.bfid \
                            end as bfid, \
                            case when mig1.dst_bfid is not NULL \
                                 then NULL \
                                 else mc1.alt_bfid \
                            end as alt_bfid, \
                            bfs.bfid as src_bad, \
                            bfd.bfid as dst_bad, \
                            '3' as debug \
                     from \
                     (select f1.bfid, location_cookie \
                      from file f1, volume v1 \
                      where f1.volume = v1.id \
                        and v1.label = '%s' \
                      order by f1.location_cookie \
                     ) as bfids1 \
                     left join \
                     file_copies_map mc1 on mc1.bfid = bfids1.bfid \
                     left join \
                     migration mig1 on (mig1.src_bfid = mc1.bfid or \
                                        mig1.dst_bfid = mc1.bfid) and \
                                       (mig1.src_bfid = mc1.alt_bfid or \
                                        mig1.dst_bfid = mc1.alt_bfid) \
                     left join \
                     bad_file as bfs on (mig1.src_bfid = bfs.bfid or \
                                         mc1.bfid = bfs.bfid) \
                     left join \
                     bad_file as bfd on (mig1.dst_bfid = bfd.bfid or \
                                         mc1.alt_bfid = bfd.bfid) \
                     \
                     union \
                     \
                     /* \
                      * This forth of four unioned selects pulls out just \
                      * multiple copies. \
                      */ \
                     \
                     select bfids1.bfid as current_bfid, location_cookie, \
                            NULL as src_bfid, NULL as dst_bfid, \
                            NULL as copied, NULL as swapped, NULL as checked, \
                            NULL as closed, \
                            NULL as remark, \
                            case when mig1.src_bfid is not NULL \
                                 then NULL \
                                 else mc1.bfid \
                            end as bfid, \
                            case when mig1.src_bfid is not NULL \
                                 then NULL \
                                 when mc1.alt_bfid is not NULL \
                                 then mc1.alt_bfid \
                                 else bfids1.bfid \
                            end as alt_bfid, \
                            bfs.bfid as src_bad, \
                            bfd.bfid as dst_bad, \
                            '4' as debug \
                     from \
                     (select f1.bfid, location_cookie \
                      from file f1, volume v1 \
                      where f1.volume = v1.id \
                        and v1.label = '%s' \
                      order by f1.location_cookie \
                     ) as bfids1 \
                     left join \
                     file_copies_map mc1 on mc1.alt_bfid = bfids1.bfid \
                     left join \
                     migration mig1 on (mig1.src_bfid = mc1.bfid or \
                                        mig1.dst_bfid = mc1.bfid) and \
                                       (mig1.src_bfid = mc1.alt_bfid or \
                                        mig1.dst_bfid = mc1.alt_bfid) \
                     left join \
                     bad_file as bfs on (mig1.src_bfid = bfs.bfid or \
                                         mc1.bfid = bfs.bfid) \
                     left join \
                     bad_file as bfd on (mig1.dst_bfid = bfd.bfid or \
                                         mc1.alt_bfid = bfd.bfid) \
                     ) as blah \
                     order by location_cookie, copied;" \
                % (volume, volume, volume, volume)

                res = db.query(q).dictresult()
                full_output_list = res

            # End method using one large SQL query.
            #################################################################
        else:
            #################################################################
            # Begin method that breaks the information gathering into multiple
            # queries or clerk requests.

            full_output_list = []
            #Loop over all the files on the tape.
            for file_record in tape_list:
                #Get the DB results for the bfid in the migration and
                # file_copies_map tables.
                res = __query_file_status(file_record['bfid'], db)
                if not res:
                    exit_status = exit_status + 1
                    error_log(MY_TASK, "Did not obtain migration information"
                              " for %s." % (file_record['bfid'],))
                    continue

                full_output_list = full_output_list + res

            # End method that breaks the information gathering into multiple
            # queries or clerk requests.
            #################################################################

        #Print the results to the terminal.
        exit_status = exit_status + __show_status(MY_TASK,
                                                  full_output_list, tape_list,
                                                  fcc, vcc, db,
                                                  intf, volume = volume)

    return exit_status


def __show_status(MY_TASK, full_output_list, tape_list, fcc, vcc, db, intf,
                  volume = ""):

    show_list = []   #Holds the search results.
    column_volume_list = []
    output_type = []

    exit_status = 0

    # At this point these are the field names that will be set with
    # the respective bfids:
    # Migration: src_bfid and dst_bfid
    # Multiple copy: bfid and alt_bfid
    # Duplication: src_bfid, dst_bfid, bfid and alt_bfid
    # Cloning: src_bfid and dst_bfid
    #
    # Nothing: src_bfid and bfid in separate rows.

    # A file can be involved in multiple ways for this output.
    # The max is three where a volume (1) is the destination of other
    # volumes, (2) has multiple copies of its files located on other
    # volumes and (3) has been migrated/cloned/duplicated itself.

    #################################################################
    # Search the list verifying if the volume is a source or
    # destination volume.  Or both.  Set show_list accordingly.

    DO_MIG_SRC = 1  #migration, duplication or cloning source
    DO_MC_SRC = 2   #multiple copy source
    DO_MIG_DST = 3  #migration, duplication or cloning destination
    DO_MC_DST = 4   #multiple copy destination
    if not intf.destination_only:
        if not intf.multiple_copy_only:
            for row in full_output_list:
                # The three cases the inner expression checks for are:
                # 1) Migration:  (row['dst_bfid'] and not row['bfid'])
                # 2) Duplication with original as primary:
                #    row['src_bfid'] == row['bfid'] and
                #    row['dst_bfid'] == row['alt_bfid']
                # 3) Duplication with copy as primary:
                #    row['src_bfid'] == row['alt_bfid'] and
                #    row['dst_bfid'] == row['bfid']
                if row['src_bfid'] == row['current_bfid'] \
                       and ((row['dst_bfid'] and not row['bfid']) \
                            or (row['src_bfid'] == row['bfid'] and \
                                row['dst_bfid'] == row['alt_bfid']) \
                            or (row['src_bfid'] == row['alt_bfid'] and \
                                row['dst_bfid'] == row['bfid'])):
                    #We found a source volume.
                    output_type.append(DO_MIG_SRC)
                    column_volume_list.append((volume, ""))
                    show_list.append(full_output_list)
                    break

        if not intf.migration_only:
            for row in full_output_list:
                if row['bfid'] == row['current_bfid'] \
                       and row['alt_bfid'] and not row['src_bfid']:
                    #We found a source volume.
                    output_type.append(DO_MC_SRC)
                    column_volume_list.append((volume, ""))
                    show_list.append(full_output_list)
                    break
    if not intf.source_only:
        if not intf.multiple_copy_only:
            for row in full_output_list:
                # The three cases the inner experession checks for are:
                # 1) Migration:  (row['dst_bfid'] and not row['bfid'])
                # 2) Duplication with original as primary:
                #    row['src_bfid'] == row['bfid'] and
                #    row['dst_bfid'] == row['alt_bfid']
                # 3) Duplication with copy as primary:
                #    row['src_bfid'] == row['alt_bfid'] and
                #    row['dst_bfid'] == row['bfid']
                if row['dst_bfid'] == row['current_bfid'] \
                       and ((row['src_bfid'] and not row['alt_bfid']) \
                            or (row['src_bfid'] == row['bfid'] and \
                                row['dst_bfid'] == row['alt_bfid']) \
                            or (row['src_bfid'] == row['alt_bfid'] and
                                row['dst_bfid'] == row['bfid'])):
                    #We found a destination volume.
                    output_type.append(DO_MIG_DST)
                    column_volume_list.append(("", volume))
                    show_list.append(full_output_list)
                    break

        if not intf.migration_only:
            for row in full_output_list:
                if row['alt_bfid'] == row['current_bfid'] \
                    and row['bfid'] and not row['dst_bfid']:
                    #We found a destination volume.
                    output_type.append(DO_MC_DST)
                    column_volume_list.append(("", volume))
                    show_list.append(full_output_list)
                    break

    if not intf.destination_only:
        if show_list == []:
            output_type.append(DO_MIG_SRC)
            column_volume_list.append((volume, ""))
            show_list.append(full_output_list)
    else:
        if show_list == []:
            output_type.append(DO_MIG_DST)
            column_volume_list.append(("", volume))
            show_list.append(full_output_list)

    #################################################################

    #
    # The code to print out the migration status information is the same
    # for all methods.
    #

    #Think of these as being used like lambda functions...
    def __print_y( value, uTrue="y", uFalse="" ):
        return ( uTrue, uFalse )[ not value ]
    def __print_deleted(file_record):
        if not file_record.has_key('deleted'):
            deleted = " "
        elif file_record['deleted'] == "yes":
            deleted = "Y"
        elif file_record['deleted'] == "no":
            deleted = "N"
        else:
            deleted = "U"
        return deleted
    def __print_bad(bad_status, file_record):
        if bad_status:
            bad = 'B'
        elif (file_record.has_key('pnfs_name0') and
              not file_record['pnfs_name0']) or \
              (file_record.has_key('pnfsid') and
               not file_record['pnfsid']):
            bad = "E"
        else:
            bad = " "
        return bad

    #Now loop over the files printing them out.
    for i in range(len(show_list)):
        rows = show_list[i]  #Create variable shortcut.

        #Start over from the begining of tape_list.
        __reset_start_index()

        #Reset the type of migration that is reported after each
        # ouput grouping.  This prevents the first one from being reported
        # for all the rest.
        mig_type = None
        migration = False
        duplication = False
        multiple_copy = False
        cloning = False

        #Need to print out the header information for each new output section.
        # A different section of output is produced if a volume has been
        # both a source and destination tape of separate migration attempts.
        print_header_output = True

        #Print out the information.
        last_current_bfid = None
        last_current_bfid_count = 0
        for row in rows:

            #print row['bfid'] != row['current_bfid']
            #print row['src_bfid']
            #print row['dst_bfid']
            if output_type[i] == DO_MC_SRC:
                if row['src_bfid'] or row['dst_bfid']:
                    #print "skipped [MC_SRC]:", row
                    continue
                if row['bfid'] != row['current_bfid']:
                    #print "skipped [MC_SRC]:", row
                    continue
            elif output_type[i] == DO_MC_DST:
                if row['src_bfid'] or row['dst_bfid']:
                    #print "skipped [MC_DST]:", row
                    continue
                if row['alt_bfid'] != row['current_bfid']:
                    #print "skipped [MC_DST]:", row
                    continue
            elif output_type[i] == DO_MIG_SRC and \
                     row['src_bfid'] != row['current_bfid']:
                #print "skipped [MIG_SRC]:", row
                continue
            elif output_type[i] == DO_MIG_DST and \
                     row['dst_bfid'] != row['current_bfid']:
                #print "skipped [MIG_DST]:", row
                continue

            ### I don't believe this loop is needed anymore...
            #Map any None values to empty strings.
            #for key in row.keys():
            #    if row[key] == None:
            #        row[key] = ""

            #Determine the duplication status for the source
            # and destination files.
            if row['bfid'] and row['src_bfid']:
                src_bfid = row['src_bfid']
                if row['src_bfid'] == row['bfid']:
                    src_dup = "P"
                else:
                    src_dup = "C"
            elif row['bfid']:
                src_bfid = row['bfid']
                if row['alt_bfid']:
                    src_dup = "O"
                else:
                    src_dup = " "
            else:
                if row['src_bfid']:
                    src_bfid = row['src_bfid']
                elif row['current_bfid']:
                    src_bfid = ""
                else:
                    src_bfid = ""
                src_dup = " "

            if row['alt_bfid'] and row['dst_bfid']:
                dst_bfid = row['dst_bfid']
                if row['dst_bfid'] == row['alt_bfid']:
                    dst_dup = "C"
                else:
                    dst_dup = "P"
            elif row['alt_bfid']:
                dst_bfid = row['alt_bfid']
                if row['bfid']:
                    dst_dup = "M"
                else:
                    dst_dup = " "
            else:
                if  row['dst_bfid']:
                    dst_bfid = row['dst_bfid']
                else:
                    dst_bfid = ""
                dst_dup = " "

            #When migrating to multiple copies, the same source
            # bfid will be listed twice, or more.  To help visually
            # make this obvious, in the S column show the multiple
            # copy number.  If the number of copies is greater than
            # nine, making it a more than one character long, replace
            # the number with and "X".
            if last_current_bfid == row['current_bfid']:
                last_current_bfid_count = last_current_bfid_count + 1
                if dst_dup == " ":
                    dst_dup = str(last_current_bfid_count)
                    if len(dst_dup) > 1:
                        dst_dup = "X"
            else:
                last_current_bfid = row['current_bfid']
                last_current_bfid_count = 0

            #Retrieve the corresponding file records.  This is the
            # slow part.  One side is already in tape_list.  However,
            # the other side needs to be retrieved one at a time,
            # if necessary, and may take a while.
            if column_volume_list[i][0]:
                src_file_record = __find_record(tape_list, src_bfid)
                if dst_bfid:
                    dst_file_record = get_file_info(MY_TASK, dst_bfid,
                                                    fcc, db)
                    if not dst_file_record:
                        #Turn None into {}.
                        dst_file_record = {}
                else:
                    dst_file_record = {}
            elif column_volume_list[i][1]:
                if src_bfid:
                    src_file_record = get_file_info(MY_TASK, src_bfid,
                                                    fcc, db)
                    if not src_file_record:
                        #Turn None into {}.
                        src_file_record = {}
                else:
                    src_file_record = {}
                dst_file_record = __find_record(tape_list, dst_bfid)
            else:
                # Single file status was requested.

                #Get source file information.
                if src_bfid:
                    #First see if we already have the information.
                    src_file_record = __find_record(tape_list, src_bfid)
                    if not src_file_record:
                        #If we don't have it, go and get it.
                        src_file_record = get_file_info(MY_TASK, src_bfid,
                                                        fcc, db)
                        if not src_file_record:
                            #Turn None into {}.
                            src_file_record = {}
                else:
                    src_file_record = {}

                #Get destination file information.
                if dst_bfid:
                    #First see if we already have the information.
                    dst_file_record = __find_record(tape_list, dst_bfid)
                    if not dst_file_record:
                        dst_file_record = get_file_info(MY_TASK, dst_bfid,
                                                        fcc, db)
                        if not dst_file_record:
                            #Turn None into {}.
                            dst_file_record = {}
                else:
                    dst_file_record = {}



            #Format the infomation for output.
            src_bad = __print_bad(row['src_bad'], src_file_record)
            src_del = __print_deleted(src_file_record)
            dst_bad = __print_bad(row['dst_bad'], dst_file_record)
            dst_del = __print_deleted(dst_file_record)
            copied = __print_y(row['copied'])
            swapped = __print_y(row['swapped'])
            checked = __print_y(row['checked'])
            closed = __print_y(row['closed'])

            #Print the volume header information before the first file in
            # a volume output or before each file in file output.
            if print_header_output:
                if volume:
                    #We can only include the specified volume in the header.
                    # The other side can/will have multiple volumes.
                    __print_header(column_volume_list[i][0],
                                   column_volume_list[i][1])
                elif len(rows) > 4:
                    #For a single bfid migration to multple copies, there
                    # will be multiple destination volumes.  For these
                    # bfids we need to avoid showing the first file's
                    # destination volume information.
                    __print_header(src_file_record.get('external_label', ""),
                                   "")
                else:
                    #For a single bfid, we want to include all the volume
                    # information to make it easier to determine where a
                    # file was migrated from to where it was migrated to.
                    __print_header(src_file_record.get('external_label', ""),
                                   dst_file_record.get('external_label', ""))
                print_header_output = False  #Only once per output section.

            #Output the information.
            line = "%19s %1s%1s%1s %19s %1s%1s%1s %6s %6s %6s %6s" % \
                   (src_bfid, src_dup, src_del, src_bad,
                    dst_bfid, dst_dup, dst_del, dst_bad,
                    copied, swapped, checked, closed)
            print line

            ##############################################################
            #If a file is not done yet, ignore bad/empty files, make
            # sure the exit status returned indicates the file is not
            # done.  Ignore deleted, bad and/or empty files if necessary.
            if src_bad != " " or dst_bad != " ":
                pass  #skip bad or empty files.
            elif src_bfid and not dst_bfid:
                # The file is not yet migrated/duplicated.
                if intf.with_deleted and src_del in ["N", "Y"]:
                    exit_status = 1
                elif src_del in ["N"]:
                    exit_status = 1
            elif (src_bfid and dst_bfid) and \
                     (copied != "y" and swapped != "y" and \
                      checked != "y" and closed != "y"):
                # The file is migrated/duplicated, but has not been
                # checked and/or closed.
                if src_dup not in ["0", "M"] and \
                   dst_dup not in ["O", "M"]:
                    exit_status = 1
            ##############################################################

            ##############################################################
            # Determine if we have cloning, migration, duplication or
            # multiple_copies.
            if src_dup in ["P", "C"]:
                duplication = True
            elif src_dup in ["O", "M"]:
                multiple_copy = True
            #Make sure that we have migration/cloning.
            elif src_bfid and dst_bfid:
                #Only check for migration or cloning if we haven't
                # determined cloning or migration, yet.
                if not cloning and not migration:
                    src_media_type = None
                    dst_media_type = None

                    if column_volume_list[i][0]:
                        #Get the source volume information.
                        src_volume = column_volume_list[i][0]
                        src_volume_dist = get_volume_info(MY_TASK,
                                                          src_volume,
                                                          vcc, db,
                                                          use_cache=True)
                        if e_errors.is_ok(src_volume_dist):
                            src_media_type = src_volume_dist['media_type']
                        else:
                            src_media_type = None

                        #Get the destination volume information.
                        dst_volume_dict = get_volume_info_for_bfid(
                            MY_TASK, dst_bfid, vcc, fcc, db)
                        if e_errors.is_ok(dst_volume_dict):
                            dst_media_type = dst_volume_dict['media_type']
                        else:
                            dst_media_type = None

                    elif column_volume_list[i][1]:
                        #Get the source volume information.
                        src_volume_dict = get_volume_info_for_bfid(
                            MY_TASK, src_bfid, vcc, fcc, db)
                        if e_errors.is_ok(src_volume_dict):
                            src_media_type = src_volume_dict['media_type']
                        else:
                            src_media_type = None

                        #Get the destination volume information.
                        dst_volume = column_volume_list[i][1]
                        dst_volume_dist = get_volume_info(MY_TASK,
                                                          dst_volume,
                                                          vcc, db,
                                                          use_cache=True)
                        if e_errors.is_ok(dst_volume_dist):
                            dst_media_type = dst_volume_dist['media_type']
                        else:
                            dst_media_type = None

                    #If the media_types are the same, we have cloning.
                    if src_media_type and dst_media_type \
                       and src_media_type == dst_media_type:
                        cloning = True
                    else:
                        migration = True

        ##############################################################

        mig_type = __get_migration_type(migration, duplication,
                                        multiple_copy, cloning)
        if mig_type:
            print "\n%s" % (mig_type,)
        print   #Add seperator line between groupings.

    return exit_status


def show_show(intf, db):
    MY_TASK = "SHOW STATUS"

    #Build the sql query.
    q = "select label,system_inhibit_1 from volume " \
        "where system_inhibit_0 != 'DELETED' " \
        " and media_type = '%s' " \
        % (intf.media_type,)
    if intf.library and \
           intf.library != None and intf.library != "None":
        q = q + "and library = '%s' " % (intf.library,)
    if intf.storage_group and \
           intf.storage_group != None and \
           intf.storage_group != "None":
        q = q + "and storage_group = '%s' " % (intf.storage_group,)
    if intf.file_family and \
           intf.file_family != None and intf.file_family != "None":
        q = q + "and file_family = '%s' " % (intf.file_family,)
    if intf.wrapper and \
           intf.wrapper != None and intf.wrapper != "None":
        q = q + "and wrapper = '%s' " % (intf.wrapper,)
    q = q + ";"

    if debug:
        log(MY_TASK, q)

    #Get the results.
    res = db.query(q).getresult()

    print "%10s %s" % ("volume", "system inhibit")
    for row in res:
        print "%10s %s" % (row[0], row[1])

##########################################################################
#For duplication only.

def dup_children(my_task, file_info, children, copy_bfid, fcc):
    """
    Create duplicates of package children in FC DB for copy of package file copy_bfid. It is metadata operation, no data copied.

    Get children of this package identified by file_info
    Children information is used to create children copies in enstore DB.
    This method returns number of errors encountered.

    @type  my_task: str
    @param my_task: task string to be typed in log recods
    @type  file_info: dict
    @param file_info: file info from file clerk for the original package
    @param children: packaged files, the children of the source package
    @type  copy_bfid: str
    @param copy_bfid: bfid of the copy of the source package file, does not have children yet
    @type  fcc:
    @param fcc: File Clerk client
    @rtype: int
    @return: Number of errors
    """
    # source package bfid
    bfid = file_info['bfid']

    copy_info = fcc.bfid_info(copy_bfid)
    if not e_errors.is_ok(copy_info['status']):
        error_log(my_task, "Failed to get copy info for bfid %s: %s" % (copy_bfid, copy_info['status']))
        return 1

    copy_info['package_id']                 = copy_info['bfid']
    copy_info['archive_status']             = file_cache_status.ArchiveStatus.ARCHIVED
    copy_info['cache_status']               = file_cache_status.CacheStatus.PURGED

    copy_info['package_files_count']        = file_info['package_files_count']
    copy_info['active_package_files_count'] = file_info['active_package_files_count']
    copy_info['archive_mod_time']           = file_info['archive_mod_time']

    rc = fcc.modify(copy_info)
    if rc['status'][0] != e_errors.OK:
        error_log(my_task, "Failed to modify copy for bfid %s: %s" % (bfid, rc['status']))
        return 1

    child_count = 0
    active_child_count = 0
    for rec in children['children']:
        # do not update original package record
        if rec['bfid'] == bfid:
            continue

        rec['original_bfid'] = rec['bfid']
        rec['package_id']    = copy_info['bfid']
        rec['cache_status']  = file_cache_status.CacheStatus.PURGED
        del rec['bfid']

        rc = fcc.new_bit_file({'fc':rec})
        if rc['status'][0] != e_errors.OK:
            error_log(my_task, "Failed to register copy for bfid %s: %s" % (bfid, rc['status']))
            return 1

        rec['bfid'] = rc['fc']['bfid']
        rc = fcc.modify(rec)
        if rc['status'][0] != e_errors.OK:
            error_log(my_task, "Failed to modify copy for bfid %s: %s" % (bfid, rc['status']))
            return 1

        child_count = child_count +1
        if rec['deleted'] != YES:
            active_child_count = active_child_count + 1

    if child_count != copy_info['package_files_count'] :
        error_log(my_task, "Failed to copy all children in dup_children() for package bfid %s: package_files_count=%s copied=%s"
				  % (bfid, copy_info['package_files_count'], child_count,) )
        return 1

    if active_child_count != copy_info['active_package_files_count'] :
        error_log(my_task, "Active children count mismatch in dup_children() for package bfid %s: active_package_files_count=%s copied=%s"
				  % (bfid, copy_info['active_package_files_count'], active_child_count,) )
        return 1

    return 0

def dup_packaged_meta_one(my_task, file_info, copy_bfid, fcc):
    """
    Create duplicates of package children in FC DB for copy of package file copy_bfid. It is metadata operation, no data copied.

    Get children of this package identified by file_info
    Children information is used to create children copies in enstore DB.
    This method returns number of errors encountered.

    @type  my_task: str
    @param my_task: task string to be typed in log recods
    @type  file_info: dict
    @param file_info: file info from file clerk for the original package
    @type  copy_bfid: str
    @param copy_bfid: copy-package bfid
    @type  fcc:
    @param fcc: File Clerk client
    @rtype: int
    @return: Number of errors
    """

    # find children for source package bfid:
    bfid = file_info['bfid']
    children = fcc.get_children(bfid)
    if not e_errors.is_ok(children['status']):
        error_log(my_task, "Failed to get children for bfid %s: %s" % (bfid, children['status']))
        return 1

    return dup_children(my_task, file_info, children, copy_bfid, fcc)

def dup_packaged_meta_all(my_task, file_info, fcc):
    """
    Create duplicates of package children in FC DB for all copies of package file. It is metadata operation, no data copied.

    Get children of this package identified by file_info
    Children information is used to create children copies in enstore DB.
    This method returns number of errors encountered.
    """

    # source package bfid:
    bfid = file_info['bfid']

    # find children of this package file
    children = fcc.get_children(bfid)
    if not e_errors.is_ok(children['status']):
        error_log(my_task, "Failed to get children for bfid %s: %s" % (bfid, children['status']))
        return 1

    # find copies of this package file
    copies = fcc.find_copies(bfid)
    if not e_errors.is_ok(copies['status']):
        error_log(my_task, "Failed to find copies for bfid %s: %s" % (bfid, copies['status']))
        return 1

    # create new bit-files for children with reference to the copy of original package
    return_exit_status = 0
    for copy_bfid in copies['copies']: # get a copy record
        rc = dup_children(my_task, file_info, children, copy_bfid, fcc)
        return_exit_status = return_exit_status + rc

    return return_exit_status

##########################################################################
#Set the remaining failed count to the negative value of what is currently
# remaining.
def update_failed_done(bfid, db):
    #Set the remaining value to the negative value of what was
    # remaining, just in case we have a problem and want to undo
    # this.
    q = "update active_file_copying " \
        "set remaining = -(remaining) " \
        "where bfid = '%s'" % (bfid,)

    #Get the results.
    db.query(q)

#For duplication only.

# These must be set to False in production:
DEBUG_DONT_WAIT = False
#DEBUG_DONT_WAIT = True

def make_failed_copies(vcc, fcc, db, intf):

    MY_TASK = "MAKE_FAILED_COPIES"

    #Build the sql query.
    fmt = ("select active_file_copying.*, volume.label "
        "from active_file_copying, volume, file "
        "where file.volume = volume.id "
        "      and remaining > 0 "
        "      and active_file_copying.bfid = file.bfid "
        "      and ( "
        "           time < CURRENT_TIMESTAMP - interval '%s' or "
        "           time is NULL)  "
        "      and file.pnfs_id is not NULL "
        "      and file.pnfs_id != '' "
        "      and file.pnfs_path is not NULL "
        "      and file.pnfs_path != '' "
        "order by volume.id,time;" )

    interval = "24 hours"
    if DEBUG_DONT_WAIT:
        interval = "1 minute"
        log("#### WARNING: wait time is set to '%s' in make_failed_copies(), check DEBUG_DONT_WAIT seting"
            % interval)

    q = fmt % (interval,)

    #Get the results.
    res = db.query(q).getresult()

    bfid_lists = {} #sort into list by volume.
    for row in res:
        #row[0] is bfid
        #row[1] is count
        #row[2] is time
        #row[3] is external_label

        if bfid_lists.get(row[3], None) == None:
            #We need to add an empty list for this volume.
            bfid_lists[row[3]] = []

        #Loop over the remaining count to insert the bfid N times
        # into the bfid list to duplicate.
        for unused in range(1, int(row[1]) + 1):
            if row[1] > 0:
                #Limit this to those bfids with positive
                # remaing copies-to-be-made counts.
                bfid_lists[row[3]].append(row[0])

    #Print out the list of files, if requested.
    if debug:
        for volume, bfid_list in bfid_lists.items():
            print "Volume:", volume, "First bfid:", bfid_list[0],
            print "Total bfids:", len(bfid_list)

    return_exit_status = 0

    # process volumes
    for volume, bfid_list in bfid_lists.items():
        #For debugging, do only one file.
        if debug:
            use_bfid_list = [bfid_list[0]]
            log("limiting to one file in debug mode")
        else:
            use_bfid_list = bfid_list

        exit_status = _make_copies(MY_TASK, volume, use_bfid_list, vcc, fcc, db, intf)
        return_exit_status = return_exit_status + exit_status

        # continue to process next volume in case of error
        if exit_status:
            continue

        ### The duplicatation was successfull.

        # process bfids in the copy volume
        # in case of error update error count but continue to process other copies
        for bfid in use_bfid_list:
            update_res = fcc.made_copy(bfid)
            if not e_errors.is_ok(update_res['status']):
                error_log(MY_TASK,"Failed to update active_file_copying for bfid %s." % (bfid,))
                return_exit_status = return_exit_status +1

    return return_exit_status

def make_copies(vcc, fcc, db, intf):
    MY_TASK = "MAKE_COPIES"

    return_exit_status = 0

    #Build the sql query to get the list of volumes with the user's criteria.
    q = "select label from volume " \
        "where system_inhibit_0 != 'DELETED' " \
        "  and media_type = '%s' " \
        "  and library not like '%%shelf%%' " \
        "  and file_family not like '%%-MIGRATION%%' " \
        "  and file_family not like '%%_copy_%%' " \
        % (intf.media_type,)
    if intf.library__ and \
           intf.library__ != None and intf.library__ != "None":
        # intf.library__ is used here to avoid conflict with --library.
        q = q + "and library = '%s' " % (intf.library__,)
    if intf.storage_group and \
           intf.storage_group != None and \
           intf.storage_group != "None":
        q = q + "and storage_group = '%s' " % (intf.storage_group,)
    if intf.file_family and \
           intf.file_family != None and intf.file_family != "None":
        q = q + "and file_family = '%s' " % (intf.file_family,)
    if intf.wrapper and \
           intf.wrapper != None and intf.wrapper != "None":
        q = q + "and wrapper = '%s' " % (intf.wrapper,)
    q = q + ";"

    if debug:
        log(MY_TASK, q)

    #Get the results.
    res = db.query(q).getresult()

    for row in res:
        volume = row[0]  #shortcut

        #Get the active files without multple copies on the current volume.

        fmt = ( "select file.bfid "
             "from file "
             "left join volume on volume.id = file.volume "
             "left join file_copies_map on file.bfid = file_copies_map.bfid "
             "left join migration on file.bfid = migration.src_bfid "
             "where label = '%s' "
             "      and "
             "      ("
             # Found a file not yet started.
             "       (file_copies_map.alt_bfid is NULL and "
             "        file.deleted = 'n') "
             "       or "
             # Found a file started but not yet finished to
             "       (volume.system_inhibit_1 not in ('duplicating', "
             "                                        'duplicated', "
             "                                        'migrating', "
             "                                        'migrated', "
             "                                        'cloning', "
             "                                        'cloned') "
             "        and migration.dst_bfid = file_copies_map.alt_bfid) "
             "      ) "
             "order by file.bfid;" )

        q1 = fmt % (volume,)
        res1 = db.query(q1).getresult()

        bfid_list = []
        if len(res1) > 0:
            for row in res1:
                bfid_list.append(row[0])

            if debug:
                #For debugging, do only one file.
                use_bfid_list = [bfid_list[0]]
            else:
                use_bfid_list = bfid_list

            exit_status = _make_copies(MY_TASK, volume, use_bfid_list,
                                       vcc, fcc, db, intf)
            return_exit_status = return_exit_status + exit_status

            if debug:
                log("limiting to one file in debug mode")
                break

    return return_exit_status  #Success

def _make_copies(MY_TASK, volume, bfid_list, vcc, fcc, db, intf):
    file_record_list = [] # Clear for each volume.

    return_exit_status = 0

    if debug:
        #For debugging, do only one file.
        use_bfid_list = [bfid_list[0]]
    else:
        use_bfid_list = bfid_list

    #Loop over each file making a multiple copy each time.
    for bfid in use_bfid_list:
        file_record = get_file_info(MY_TASK, bfid, fcc, db)
        if not e_errors.is_ok(file_record):
            error_log(MY_TASK,
                      "Unable to obtain file record for %s" % (bfid,))
            #For debugging, do only one file.
            if debug:
                log("limiting to one file in debug mode")
                break
            continue
        elif file_record['deleted'] == UNKNOWN:
            log(MY_TASK, "Setting unknown file %s as done." % (bfid,))

            #Can't duplicate failed original copy.
            update_failed_done(bfid, db)

            #For debugging, do only one file.
            if debug:
                log("limiting to one file in debug mode")
                break
            continue
        elif file_record['deleted'] == YES and \
             (not file_record['pnfsid'] or not file_record['pnfs_name0']):
            log(MY_TASK, "Setting failed file %s as done." % (bfid,))

            #Can't duplicate failed original copy.
            update_failed_done(bfid, db)

            #For debugging, do only one file.
            if debug:
                log("limiting to one file in debug mode")
                break
            continue
        elif file_record['external_label'].find(".deleted") != -1:
            log(MY_TASK, "Setting file %s on deleted volume %s as done." % \
                (bfid, file_record['external_label']))

            #Can't duplicate failed original copy.
            #update_failed_done(bfid, db)

            #For debugging, do only one file.
            if debug:
                log("limiting to one file in debug mode")
                break
            continue

        if is_swapped(bfid, fcc, db) and is_migration(bfid, db):
            #At th is point we know that the file has been migrated
            # through the swap step.  If --with-final-scan was used, also
            # check if the tape is already scanned.  If everything is
            # done decrement the file's remaining count.  Normally this
            # will never happen, but it might if a previous run is
            # interrupted.
            if (intf.with_final_scan and is_checked_by_src(bfid, fcc, db)) \
                   or not intf.with_final_scan:
                log(MY_TASK,
                    "Setting already migrated file %s as done." % (bfid,))

                #Can't duplicate since it has already been migrated.
                update_failed_done(bfid, db)

                #For debugging, do only one file.
                if debug:
                    log("limiting to one file in debug mode")
                    break
                continue

        if is_multiple_copy_bfid(bfid, db):
            original_bfid = get_the_original_copy_bfid(bfid, db)
            if is_copied(original_bfid, fcc, db):
                log(MY_TASK,
                       "found original copy %s already migrated for %s" \
                          % (original_bfid, bfid))

                #Can't duplicate since the original copy has already been
                # migrated.
                update_failed_done(bfid, db)

                #For debugging, do only one file.
                if debug:
                    log("limiting to one file in debug mode")
                    break
                continue

        file_record_list.append(file_record)

    #Do the duplication.
    if file_record_list:
        volume_record = get_volume_info(MY_TASK, volume, vcc, db)
        if not e_errors.is_ok(volume_record):
            error_log(MY_TASK, "Unable to obtain %s volume record" % (volume,))
            return 1

        exit_status = migrate(file_record_list, intf,
                              volume_record=volume_record)

        return_exit_status = return_exit_status + exit_status

        if not exit_status:
            for file_record in file_record_list:
                bfid = file_record['bfid']  #shortcut

                #Verify that an entry exists in the file_copies_map table.
                q = "select * from file_copies_map " \
                    "where bfid = '%s'" % (bfid,)

                #Get the results.
                try:
                    mc_res = db.query(q).dictresult()
                except:
                    exc_type, exc_value = sys.exc_info()[:2]
                    error_log(MY_TASK, str(exc_type), str(exc_value), q)
                    return 1 + return_exit_status

                #If the list is empty, the entry is missing.
                if len(mc_res) == 0:
                    error_log(MY_TASK, "missing file_copies_map entry for %s" \
                              % (bfid,))
                    return 1 + return_exit_status


                #Determine if the removal of the migration table entry has
                # already been done.
                q = "select * from migration " \
                    "where src_bfid = '%s'" % (bfid,)

                #Get the results.
                try:
                    mig_res = db.query(q).dictresult()
                except:
                    exc_type, exc_value = sys.exc_info()[:2]
                    error_log(MY_TASK, str(exc_type), str(exc_value), q)
                    return 1 + return_exit_status

                if len(mig_res) == 0:
                    #This has already been removed.  Probably on a previous
                    # run of the code.
                    continue

                log(MY_TASK, "Removing the bfid from the migration " \
                    "table for bfid %s." % (bfid,))

                #Build the sql query.
                #Remove this file from the migration table.  We do
                # not want the source volume to look like it has
                # started to be migrated/duplicated.
                q = "delete from migration " \
                    "where src_bfid = '%s'" % (bfid,)

                #Get the results.
                try:
                    delete_res = db.query(q)
                except:
                    exc_type, exc_value = sys.exc_info()[:2]
                    error_log(MY_TASK, str(exc_type), str(exc_value), q)
                    return 1 + return_exit_status

                #Make sure this is a numeric type.
                delete_res = long(delete_res)

                if delete_res == 1:
                    ok_log(MY_TASK, "Removed the bfid from the migration " \
                           "table for bfid %s." % (bfid,))
                elif delete_res > 0:
                    #Should never happen with unique volume ids.
                    error_log(MY_TASK,
                              "Removed %s the bfid from the migration " \
                              "table for bfid %s." % (delete_res, bfid,))
                    return 1 + return_exit_status  #Error
                else:
                    error_log(MY_TASK,
                              "Failed to removed the bfid from the " \
                              "migration table for bfid %s." % (bfid,))
                    return 1 + return_exit_status  #Error

    return return_exit_status

##########################################################################

def choose_remaining_volume(vcc, db, intf, skip_volume_list=[]):
    if USE_CLERKS:
        vols_dict = vcc.get_vols()
        if not e_errors.is_ok(vols_dict):
            return None

        not_full_volumes_list = []
        for volume_record in vols_dict['volumes']:
            #Keep searching if the volume is already in the skip list.
            if volume_record['label'] in skip_volume_list:
                continue

            #Keep searching if the media_type does not match.
            try:
                if volume_record['media_type'] != intf.args[0]:
                    continue
            except KeyError:
                #At this time, get_vols() does not include the media_type
                # with the rest of the volume information.  Ignore this
                # and hope that the library is supplied.
                pass
            except IndexError:
                #No media supplied by the user.  This error should never
                # happen, since at least the media_type needs to be specified
                # in order to get this far.
                continue

            #Keep searching if the library does not match.
            try:
                if volume_record['library'] != intf.args[1]:
                    continue
            except IndexError:
                #No storage group supplied by the user.
                pass

            #Keep searching if the storage_group does not match.
            try:
                if volume_record['storage_group'] != intf.args[2]:
                    continue
            except IndexError:
                #No storage group supplied by the user.
                pass

            #Keep searching if the file_family does not match.
            try:
                if volume_record['file_family'] != intf.args[3]:
                    continue
            except IndexError:
                #No storage group supplied by the user.
                pass

            #Keep searching if the file_family does not match.
            try:
                if volume_record['wrapper'] != intf.args[4]:
                    continue
            except IndexError:
                #No storage group supplied by the user.
                pass

            #Keep searching if the volume is not available.
            system_inhibit_0 = volume_record.get('system_inhibit_0',
                                      volume_record.get('system_inhibit',
                                                        ("none", "none")[0]))
            if  system_inhibit_0 in ("DELETED", "NOACCESS", "NOTALLOWED"):
                continue

            #Keep searching if the volume has already been started to be
            # migrated/duplicated/cloned.
            system_inhibit_1 = volume_record.get('system_inhibit_1',
                                      volume_record.get('system_inhibit',
                                                        ("none", "none")[1]))
            if system_inhibit_1 in \
               ("migrating", "migrated", "duplicating", "duplicated",
                "cloning", 'cloned'):
                continue

            #Favor, but don't exclude non-full tapes.  Put these in a
            # list for possible use if no full tapes are found.
            if system_inhibit_1 != "full":
                not_full_volumes_list.append(volume_record['label'])

            return volume_record['label']

        try:
            return not_full_volumes_list[0]
        except IndexError:
            #Must be all done.
            return None
    else:

        #Skip the volumes that match the following conditions.

        try:
            library = intf.args[1]
            library_sql = " and library = '%s' " % (library,)
        except IndexError:
            library_sql = ""

        try:
            storage_group = intf.args[2]
            storage_group_sql = " and storage_group = '%s' " % (storage_group,)
        except IndexError:
            storage_group_sql = ""

        try:
            file_family = intf.args[3]
            file_family_sql = " and file_family = '%s' " % (file_family,)
        except IndexError:
            file_family_sql = ""

        try:
            wrapper = intf.args[4]
            wrapper_sql = " and wrapper = '%s' " % (wrapper,)
        except IndexError:
            wrapper_sql = ""

        if skip_volume_list:
            skip_sql = " and label not in ('%s') " % string.join(skip_volume_list,
                                                                 "', '")
        else:
            skip_sql = ""


        q = "select label " \
            "from volume " \
            "where system_inhibit_0 not in ('DELETED', 'NOACCESS', 'NOALLOWED') " \
            "  and system_inhibit_1 not in ('migrating', " \
            "                               'migrated', " \
            "                               'duplicating', " \
            "                               'duplicated', " \
            "                               'cloning', " \
            "                               'cloned') " \
            "  and media_type = '%s' " \
            "  %s %s %s %s %s" \
            "limit 1;" % (intf.args[0], library_sql, storage_group_sql,
                          file_family_sql, wrapper_sql, skip_sql)

        if debug:
            log("choose_remaining_volume():", q)

        res = db.query(q).getresult()
        if len(res) == 0:
            return None
        else:
            return res[0][0]

def migrate_remaining_volumes(vcc, db, intf):

    rtn = 0  #return value.
    skip_volume_list = [] #List of volumes that failed for skipping.

    #Pick the first volume to migrate with the specified constraints.
    volume = choose_remaining_volume(vcc, db, intf)
    while volume:
        rtn_val = migrate_volume(volume, intf)
        rtn = rtn + rtn_val
        if rtn_val:
            skip_volume_list.append(volume)

        #Pick the next volume to migrate with the specified constraints.
        volume = choose_remaining_volume(vcc, db, intf,
                                         skip_volume_list = skip_volume_list)

    return rtn

##########################################################################

#def read_file(MY_TASK, src_bfid, src_path, tmp_path, volume,
#	      location_cookie, deleted, encp, intf):
def read_file(MY_TASK, read_job, encp, intf):
    #extract shortcuts
    src_file_record = read_job[0]
    src_volume_record = read_job[1]
    src_path = read_job[2]
    tmp_path = read_job[5]

    log(MY_TASK, "copying %s %s %s" \
        % (src_file_record['bfid'], src_volume_record['external_label'],
           src_file_record['location_cookie']))

    if src_file_record['deleted'] == NO and not os.access(src_path, os.R_OK):
        error_log(MY_TASK, "%s %s is not readable" \
                  % (src_file_record['bfid'], src_path))
        return 1

    # make sure the tmp file is not there - need to match the euid/egid
    # with the permissions of the directory and not the file itself.
    if file_utils.e_access(tmp_path, os.F_OK):
        log(MY_TASK, "tmp file %s exists, removing it first" \
            % (tmp_path,))
        try:
                file_utils.remove(tmp_path)
        except (OSError, IOError), msg:
                error_log(MY_TASK, "unable to remove %s as (uid %d, gid %d): %s" % (tmp_path, os.geteuid(), os.getegid(), str(msg)))
                return 1

    ## Build the encp command line.
    if intf.priority:
        use_priority = ["--priority", str(intf.priority)]
    else:
        use_priority = ["--priority", str(ENCP_PRIORITY)]
    if src_file_record['deleted'] == YES:
        use_override_deleted = ["--override-deleted"]
        use_path = ["--get-bfid", src_file_record['bfid']]
    else:
        use_override_deleted = []
        use_path = [src_path]
    if use_threaded_encp:
        use_threads = ["--threaded"]
    else:
        use_threads = []
    if debug:
        use_verbose = ["--verbose", "4"]
    else:
        use_verbose = []
    encp_options = ["--delayed-dismount", "2", "--ignore-fair-share",
                    "--bypass-filesystem-max-filesize-check"]
    #We need to use --get-bfid here because of multiple copies.
    #
    argv = ["encp"] + use_verbose + use_override_deleted + encp_options + \
           use_priority + use_threads + use_path + [tmp_path]

    if debug:
        cmd = string.join(argv)
        log(MY_TASK, "cmd =", cmd)

    try:
        res = encp.encp(argv)
    except:
        res = 1
        error_log(MY_TASK, "Unable to execute encp", sys.exc_info()[1])
    if res == 0:
        ok_log(MY_TASK, "%s %s to %s" \
               % (src_file_record['bfid'], src_path, tmp_path))
    else:
        error_log(MY_TASK,
                  "failed to copy %s %s to %s, error = %s" \
                  % (src_file_record['bfid'], src_path, tmp_path, encp.err_msg))
        return 1

    #
    detect_uncleared_deletion_lists(MY_TASK)

    return 0

def read_files(MY_TASK, read_jobs, encp, intf):
    src_bfids = []
    src_paths = []
    deleteds = []
    location_cookies = []
    for i in range(len(read_jobs)):
        #extract shortcuts
        src_file_record = read_jobs[i][0]
        src_volume_record = read_jobs[i][1]
        src_path = read_jobs[i][2]
        tmp_path = read_jobs[i][5]

        if src_file_record['deleted'] == NO \
               and not os.access(src_path, os.R_OK):
            error_log(MY_TASK, "%s %s is not readable" \
                      % (src_file_record['bfid'], src_path))
            return 1

        # make sure the tmp file is not there - need to match the euid/egid
        # with the permissions of the directory and not the file itself.
        if file_utils.e_access(tmp_path, os.F_OK):
            log(MY_TASK, "tmp file %s exists, removing it first" % (tmp_path,))
            try:
                file_utils.remove(tmp_path)
            except (OSError, IOError), msg:
                message = "unable to remove %s as (uid %d, gid %d): %s" \
                          % (tmp_path, os.geteuid(), os.getegid(), str(msg))
                error_log(MY_TASK, message)
                return 1

        src_bfids.append(src_file_record['bfid'])
        src_paths.append(src_path)
        deleteds.append(src_file_record['deleted'])
        location_cookies.append(src_file_record['location_cookie'])

    log(MY_TASK, "copying %s %s %s" \
        % (src_bfids, src_volume_record['external_label'], location_cookies))

    ## Build the encp command line.
    tmp_path = os.path.dirname(read_jobs[0][5])  #output directory
    if intf.priority:
        use_priority = ["--priority", str(intf.priority)]
    else:
        use_priority = ["--priority", str(ENCP_PRIORITY)]
    if use_threaded_encp:
        use_threads = ["--threaded"]
    else:
        use_threads = []
    if debug:
        use_verbose = ["--verbose", "4"]
    else:
        use_verbose = []


    if YES in deleteds:
        use_override_deleted = ["--override-deleted"]
        use_path = ["--get-bfids"] + src_bfids
    else:
        use_override_deleted = []
        use_path = src_paths


    encp_options = ["--delayed-dismount", "2", "--ignore-fair-share",
                    "--bypass-filesystem-max-filesize-check",
                    "--sequential-filenames"]
    #We need to use --get-bfid here because of multiple copies.
    #
    argv = ["get"] + use_verbose + use_override_deleted + encp_options + \
           use_priority + use_threads + use_path + [tmp_path]

    if debug:
        cmd = string.join(argv)
        log(MY_TASK, "cmd =", cmd)

    try:
            res = encp.get(argv)
    except:
            res = 1
            error_log(MY_TASK, "Unable to execute encp", sys.exc_info()[1])
    if res == 0:
            ok_log(MY_TASK, "%s %s to %s" \
                   % (src_bfids, src_paths, tmp_path))
    else:
            error_log(MY_TASK,
                      "failed to copy %s %s to %s, error = %s" \
                      % (src_bfids, src_paths, tmp_path, encp.err_msg))
            return 1

    #
    detect_uncleared_deletion_lists(MY_TASK)

    return 0


#Read a file from tape.  On success, return the tuple representing the
# job that will be sent to the write thread/process.
def copy_file(file_record, volume_record, encp, intf, vcc, fcc, db):
    MY_TASK = "COPYING_TO_DISK"

    if debug:
        time_to_copy_file = time.time()

    #If "get" is being used instead of encp, the file_record variable will
    # be a list.
    if type(file_record) == types.ListType:
        use_file_record_list = file_record
    else:
        use_file_record_list = [file_record]

    #Log just the bfids.
    log_bfids = []
    for fr in use_file_record_list:
        log_bfids.append(fr['bfid'])
    log(MY_TASK, "processing %s" % (log_bfids,))

    #
    read_jobs = []
    #dst_bfids = []
    pass_along_jobs = []

    #If the migration is being re-run, after a failure, we can optimize
    # things when many files in a row went to the same volume.
    dst_volume_record = None

    for src_file_record in use_file_record_list:

        #Excract the bfid to be a shortcut.
        src_bfid = src_file_record['bfid']
        if debug:
            log(MY_TASK, "source file_record", `src_file_record`)

        if debug:
            time_to_get_volume_info = time.time()

        #get volume info
        if type(volume_record) == types.DictType:
            src_volume_record = volume_record
        else:
            src_volume_record = get_volume_info(MY_TASK,
                                          src_file_record['external_label'],
                                                vcc, db)
            if not src_volume_record:
                error_log(MY_TASK, "%s does not exist in db" \
                          % (src_file_record['external_label'],))
                return
            if debug:
                log(MY_TASK, "source volume_record", `src_volume_record`)

        #If the fire record is not complete, don't copy the file.
        if (len(src_file_record['pnfsid']) == 0 and \
            len(src_file_record['pnfs_name0']) == 0) \
            or src_file_record['deleted'] == 'unknown':
                # Can't migrate an empty/failed file.
                error_log(MY_TASK, "can not copy failed file %s" % (src_bfid,))
                return

        if debug:
            message = "Time to get volume info: %.4f sec." % \
                      (time.time() - time_to_get_volume_info,)
            log(MY_TASK, message)

        if debug:
            time_to_get_duplicate_info = time.time()

        #We need to verify that we are doing the correct thing if only
        # given a bfid as input.
        dup_files = is_duplicated(src_bfid, fcc, db)
        dup_and_mc_pairs = is_duplicated(src_bfid, fcc, db,
                                         include_multiple_copies=True)
        #Pull out just the original destination copy bfid.  Do to the ability
        # of swapping originals and copies to change which one is the primary
        # copy, we need to use the correct value.
        dup_and_mc_files = []
        for mc_bfid_pair in dup_and_mc_pairs:
            if mc_bfid_pair['bfid'] == src_bfid:
                dup_and_mc_files.append(mc_bfid_pair['alt_bfid'])
            elif mc_bfid_pair['alt_bfid'] == src_bfid:
                dup_and_mc_files.append(mc_bfid_pair['bfid'])
            else:
                #This should never happen.  If if it does, then something
                # is very wrong.
                pass
        dup_and_mc_files.sort()
        # check if all copies have been copied and swapped for this source file
        is_it_copied_list = is_copied(src_bfid, fcc, db, all_copies=True)
        #If the goal is to make multiple copies long after the originals were
        # made, we need to add such files to the list of things to look at.
        # This is necessary to finish/fix partially completed copies from
        # previous attempts.
        if getattr(intf, 'make_failed_copies', None) or \
               getattr(intf, 'make_copies', None):
            #Only include each bfid once.
            for current_bfid in dup_and_mc_files:
                if current_bfid not in is_it_copied_list:
                    is_it_copied_list.append(current_bfid)
        if not is_it_copied_list:
            #If the list was empty, put one false item in the list so
            # that the following loop gets executed once.
            is_it_copied_list = [None]

        #Loop over all the destinations
        for is_it_copied in is_it_copied_list:
            dst_bfid = is_it_copied #side effect of is_copied()
            #dst_bfids.append(dst_bfid)  #Add it to the list.
            is_it_swapped = is_swapped_by_dst(dst_bfid, fcc, db)

            # get destination file info (if available)
            if dst_bfid:
                dst_file_record = get_file_info(MY_TASK, dst_bfid, fcc, db)
                if not dst_file_record:
                    error_log(MY_TASK, "%s does not exist in db" % (dst_bfid,))
                    return
            else:
                dst_file_record = None

            #If this is a re-run of the migration, we can obtain the
            # destination volume record now.
            if dst_bfid and (not dst_volume_record or \
                             dst_volume_record['external_label'] != dst_file_record['external_label']):
                dst_volume_record = vcc.inquire_vol(dst_file_record['external_label'])
                if not e_errors.is_ok(dst_volume_record):
                    error_log(MY_TASK, dst_volume_record['status'])
                    return

            #Perfrom some checks to make sure we are allowed to make a copy.
            if dup_files and is_it_copied and MIGRATION_NAME in ["MIGRATION",
                                                                 "CLONING"]:
                if is_it_swapped:
                    error_log(MY_TASK,
                            "trying to migrate file %s already duplicated to %s" \
                            % (src_bfid, dup_files))
                    return
                else:
                    #The swap step failed.  We need to assume that the user
                    # knows if migration or duplication was used in the first
                    # place, so that the metadata can successfully be set.
                    # Now that duplicate.log_copied() updates both the
                    # migration and file_copies_map tables, this logic should
                    # not be needed in the future.
                    pass
            elif dup_and_mc_files \
                 and (getattr(intf, 'make_failed_copies', None) or
                      getattr(intf, 'make_copies', None)):
                if dst_bfid not in dup_and_mc_files:
                    #Not a file we care about for making copies sometime
                    # after the original.
                    continue
                else:
                    #This is possibly a previous attempt that failed for
                    # some reason leaving an empty file as the multiple copy
                    # destination file.
                    pass

            elif not dup_files and is_it_copied and \
                     MIGRATION_NAME in ["DUPLICATION"]:
                if is_it_swapped:
                    if dst_volume_record:
                        #Determine if the file_families have the multiple
                        # copy signature of _copy_ in the string.
                        s_index = src_volume_record['volume_family'].find("_copy_")
                        d_index = dst_volume_record['volume_family'].find("_copy_")
                    else:
                        s_index = None
                        d_index = None

                    if s_index == -1 and d_index != -1:
                        #We get here if for some reason a previous attempt
                        # to make a multiple copy failed leaving the
                        # migration table entry filled, but not the
                        # file_copies_map table entry.  This is not an error,
                        # since we want to complete the multiple copy.
                        pass
                    else:
                        error_log(MY_TASK,
                            "trying to duplicate file %s already migrated to %s" \
                            % (src_bfid, dst_bfid))
                        return
                else:
                    #The swap step failed.  We need to assume that the user
                    # knows if migration or duplication was used in the first
                    # place, so that the metadata can successfully be set.
                    # Now that duplicate.log_copied() updates both the
                    # migration and file_copies_map tables, this logic should
                    # not be needed in the future.
                    pass
            #Warn about this, but allow it to proceed.
            if dup_files and not is_it_copied:
                warning_log(MY_TASK,
                            "trying to copy an original copy file, %s, " \
                            "with multiple copies %s" \
                            % (src_bfid, dup_files))

            if debug:
                message = "Time to get duplicate info: %.4f sec." % \
                          (time.time() - time_to_get_duplicate_info,)
                log(MY_TASK, message)

            if debug:
                time_to_get_temp_file = time.time()

            #Define the directory for the temporary file on disk.
            tmp_path = temp_file(src_file_record)

            if debug:
                message = "Time to get temp file: %.4f sec." % \
                          (time.time() - time_to_get_temp_file,)
                log(MY_TASK, message)

            if debug:
                time_to_get_src_path = time.time()

            #Handle finding the name differently for migration and duplication.
            use_bfid, alt_bfid, use_file_record, use_alt_file_record = \
                      search_order(src_bfid, src_file_record,
                                   dst_bfid, dst_file_record,
                                   is_it_copied, is_it_swapped, fcc, db)

            #We need to do this for files marked and unmarked deleted.  The reason
            # for doing this with the deleted files is to catch cases were the
            # metadata is inconsistent between PNFS and Enstore DB.
            try:
                #Determine if the destination volume contains just deleted files.
                if dst_volume_record:
                    is_deleted_volume = is_deleted_file_family(dst_volume_record['volume_family'])
                else:
                    is_deleted_volume = False

                if dst_volume_record and is_deleted_volume \
                   and dst_file_record['deleted'] == YES:
                    #When re-running migration, try and avoid looking for a
                    # file in PNFS that was deleted the first time around.
                    src_path = "deleted-%s-%s" % (src_bfid, tmp_path)
                else:
                    try:
                        src_path = pnfs_find(use_bfid, alt_bfid,
                                             src_file_record['pnfsid'],
                                             file_record = use_file_record,
                                             alt_file_record = use_alt_file_record,
                                             intf = intf)
                    except:
                        if src_file_record['deleted'] == YES and \
                           str(sys.exc_info()[1]).find("replaced") != -1:
                            #We can get here if:
                            # 1) bfid1 migrated to bfid2
                            # 2) bfid2 migrated to bfid3
                            # 3) migration is rerun for bfid1
                            #This happens because bfid3 is the primary file
                            # in PNFS now.
                            src_path = "deleted-%s-%s" % (src_bfid, tmp_path)
                        else:
                            raise sys.exc_info()[0], sys.exc_info()[1], \
                                  sys.exc_info()[2]

                #There is/was a bug in migrate that allowed for a destination
                # file to be set deleted while the PNFS entry was perfectly
                # correct.  Detect and correct this situation here.

                if is_it_swapped and dst_file_record['deleted'] == YES \
                       and not is_deleted_volume:

                    # Fix the deleted status of the file.
                    if mark_undeleted(MY_TASK, dst_bfid, None , db):
                        error_log(MY_TASK,
                              "bfid %s is marked deleted, but %s exists in PNFS"
                                  % (dst_bfid, src_path))
                        return

                #If the migration is interupted after the copy, but before the
                # swap we don't want to modify the src_file_record.  There must
                # have been a reason for the following code.

                #if not is_it_swapped and src_file_record['deleted'] == YES:
                #    # Fix the deleted status of the file.
                #    if mark_undeleted(MY_TASK, src_bfid, None, db):
                #        error_log(MY_TASK,
                #             "bfid %s is marked deleted, but %s exists in PNFS"
                #                  % (src_bfid, src_path))
                #        return
            except (OSError, IOError), msg:
                src_path = None

                #The file has been migrated, however the file has been
                # deleted; removing the entry from pnfs for both.
                # Prove this by checking the deleted status of the new copy.
                if msg.errno == errno.ENOENT and is_it_copied and is_it_swapped \
                       and dst_file_record['deleted'] == YES:
                    src_path = "deleted-%s-%s" % (src_bfid, tmp_path) # for debug

                #If the file has already been copied, swapped, checked and closed
                # handle this better than to put fear into the user with false
                # warning messags.  The most likely scenario for getting here is
                # that the user used --force on an already completed migration.
                elif msg.errno == errno.ENOENT \
                         and src_file_record['deleted'] == YES \
                         and is_it_copied and is_it_swapped \
                         and is_checked(dst_bfid, fcc, db) \
                         and is_closed(dst_bfid, fcc, db):
                    src_path = "deleted-%s-%s" % (src_bfid, tmp_path) # for debug

                #Lastly, we need to handle user deleted files by making up some
                # information.
                elif msg.errno == errno.ENOENT \
                         and src_file_record['deleted'] == YES \
                         and not is_it_swapped:
                    log(MY_TASK, "%s %s %s is a DELETED FILE" \
                        % (src_file_record['bfid'], src_file_record['pnfsid'],
                           src_file_record['pnfs_name0']))
                    src_path = "deleted-%s-%s"%(src_bfid, tmp_path) # for debug

                #Handle deleted files that have been replaced.
                elif getattr(msg, 'errno', msg.args[0]) == errno.EEXIST \
                     and src_file_record['deleted'] == YES \
                     and msg.args[1].find("replaced") != -1 \
                     and dst_file_record == None:
                    src_path = "deleted-%s-%s"%(src_bfid, tmp_path) # for debug

                #Handle the case where the swap didn't complete.
                elif msg.args[0] == errno.EEXIST \
                     and getattr(msg, 'filename', None) \
                     and is_migration_path(msg.filename) \
                     and src_file_record['deleted'] == YES \
                     and dst_file_record != None \
                     and dst_file_record['deleted'] == YES:
                    src_path = msg.filename

                #Handle the case where this version of the file is deleted and
                # a new file was written by the user with the same filename.
                elif msg.args[0] == errno.EEXIST \
                     and msg.args[1].find("replaced") != -1 \
                     and src_file_record['deleted'] == YES \
                     and dst_file_record != None \
                     and dst_file_record['deleted'] == YES:
                    src_path = msg.filename
                else:
                    if debug:
                        Trace.handle_error()
                    raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]

            if debug:
                message = "Time to get source path: %.4f sec." % \
                          (time.time() - time_to_get_src_path,)
                log(MY_TASK, message)

            if debug:
                log(MY_TASK, "src_path:", src_path)
                log(MY_TASK, "tmp_path:", tmp_path)

            if debug:
                time_to_check_temp_path = time.time()

            if dst_bfid == None:
                try:
                    tfstat = os.stat(tmp_path)
                except (OSError, IOError), msg:
                    if intf.use_disk_files:
                        # We wan't to give up on these files, so that migrating
                        # the dumped good files from a bad tape won't hang trying
                        # to read the bad files.
                        error_log(MY_TASK,
                                  "can not find temporary file: %s" % (str(msg),))
                        return
                    else:
                        #There is not a copy on disk, need to read the source
                        # media for the file.
                        tfstat = None

                if tfstat and tfstat[stat.ST_SIZE] == src_file_record['size']:

                    #Don't read this file (again), but put it in the list of
                    # files to pass to the write thread.
                    pass_along_jobs.append((src_file_record,
                                            src_volume_record,
                                            src_path,
                                            dst_file_record,  #destination file record
                                            dst_volume_record,
                                            tmp_path,
                                            None,  #migration path
                                            ))
                    ok_log(MY_TASK, "%s has already been copied to disk" \
                           % (src_bfid,))
                    continue  #try next file

                #We need to read this file, so put in on the list.
                read_jobs.append((src_file_record,
                                  src_volume_record,
                                  src_path,
                                  None,  #destination file record
                                  None,  #destination volume record
                                  tmp_path,
                                  None,  #migration path
                                  ))
            else:
                ok_log(MY_TASK, "%s has already been copied to %s" \
                   % (src_bfid, dst_bfid))
                pass_along_jobs.append((src_file_record,
                                        src_volume_record,
                                        src_path,
                                        dst_file_record,
                                        dst_volume_record,
                                        tmp_path,
                                        None,  #migration path
                                        ))

            if debug:
                message = "Time to check temp path: %.4f sec." % \
                          (time.time() - time_to_check_temp_path,)
                log(MY_TASK, message)

    if debug:
        time_to_read_file = time.time()

    if len(read_jobs) == 0:
        res = 0  #all files copied already
    elif len(read_jobs) == 1:
        res = read_file(MY_TASK, read_jobs[0], encp, intf)

        if debug:
            message = "Time to read_file: %.4f sec." % \
                      (time.time() - time_to_read_file,)
            log(MY_TASK, message)
    else:  #len(read_jobs) > 1
        #Use "get" to read the files.
        res = read_files(MY_TASK, read_jobs, encp, intf)

        if debug:
            message = "Time to read_files: %.4f sec." % \
                      (time.time() - time_to_read_file,)
            log(MY_TASK, message)
    if res:
        # An error occured.  Don't pass along to the write thread.
        return

    pass_along_jobs = pass_along_jobs + read_jobs

    if debug:
        message = "Time to copy file: %.4f sec." % \
                  (time.time() - time_to_copy_file,)
        log(MY_TASK, message)

    return pass_along_jobs


# copy_files(files) -- copy a list of files to disk and mark the status
# through copy_queue
def copy_files(thread_num, file_records, volume_record, copy_queue,
               deleted_copy_queue, grab_lock, release_lock, intf):

    MY_TASK = "COPYING_TO_DISK"

    # if files is not a list, make a list for it
    if type(file_records) != type([]):
            file_records = [file_records]

    # get a db connection
    db = pg.DB(host=dbhost, port=dbport, dbname=dbname, user=dbuser)
    # get its own file clerk client and volume clerk client
    config_host = enstore_functions2.default_host()
    config_port = enstore_functions2.default_port()
    csc = configuration_client.ConfigurationClient((config_host,
                                                    config_port))
    fcc = file_clerk_client.FileClient(csc)
    vcc = volume_clerk_client.VolumeClerkClient(csc)

    # get an encp
    name_ending = "_0"
    if thread_num:
            name_ending = "_%s" % (thread_num,)
    threading.currentThread().setName("READ%s" % (name_ending,))
    encp = encp_wrapper.Encp(tid = "READ%s" % (name_ending,))

    # copy files one by one
    for file_record in file_records:
        if grab_lock:  # and release_lock:
            grab_lock.acquire()
            release_lock.release()

        try:
            # Read the file from tape.
            pass_along_jobs = copy_file(file_record, volume_record,
                                        encp, intf, vcc, fcc, db)
        except (KeyboardInterrupt, SystemExit):
            raise sys.exc_info()[0], sys.exc_info()[1], \
                  sys.exc_info()[2]
        except:
            #We failed spectacularly!

            print "HERE WE ARE ",  sys.exc_info()
#            import traceback
            traceback.print_tb(sys.exc_info()[2])

            pass_along_jobs = None #Set this so we can continue.

            #Report the error so we can continue.
            exc_type, exc_value, exc_tb = sys.exc_info()
            Trace.handle_error(exc_type, exc_value, exc_tb)
            del exc_tb #avoid resource leaks
            if type(file_record) == types.ListType:  #using get
                use_bfid = file_record[0]['bfid']
            else:  #using encp
                use_bfid = file_record['bfid']
            error_log(MY_TASK, "reading file %s: (%s, %s)" \
                      % (use_bfid, str(exc_type), str(exc_value)))

        if pass_along_jobs:
            #pass_along_jobs will contain more than one file if "get" is used.
            for pass_along_job in pass_along_jobs:
                #If we succeeded, pass the job on to the write
                # process/thread.
                if debug:
                    log(MY_TASK, "Passing job %s to write step." \
                        % (pass_along_job[0]['bfid'],))
#                    log(MY_TASK, "DEBUG:: Passing job to write step. job=%s" \
#                        % (pass_along_job[0],))

                #Pass the file information to the correct write
                # thread based on if the file is deleted or not.
                # The deleted state is at index 5.
                if pass_along_job[0]['deleted'] == NO:
                    copy_queue.put(pass_along_job, block = True)
                elif pass_along_job[3] != None \
                     and pass_along_job[3]['deleted'] == NO:
                    copy_queue.put(pass_along_job, block = True)
                else:
                    if debug:
                        log(MY_TASK, "sending to deleted queue",
                            str(pass_along_job[0]['bfid']))
                    deleted_copy_queue.put(pass_along_job,
                                           block = True)

                if debug:
                    log(MY_TASK, "Done passing job %s." \
                        % (pass_along_job[0]['bfid'],))
        else:
            if type(file_record) == types.ListType:
                    #This is possibile if using "get" and all the files
                    # are on the same volume.
                    use_file_record = file_record[0]
            else:
                    use_file_record = file_record
            #If we failed, check if the tape is still accessable.
            #
            # Don't set this to volume_record.  If volume_record was passed
            # in as None we don't want to overwrite it.
            volume_dict = get_volume_info(MY_TASK,
                                            use_file_record['external_label'],
                                            vcc, db)
            if not volume_dict or \
                   not __is_volume_allowed(volume_dict):
                # If we get here the tape has been marked
                # NOACCESS or NOTALLOWED.
                message =  "volume %s is NOACCESS or NOTALLOWED" \
                          % (volume_dict['external_label'],)
                error_log(MY_TASK, message)

                #Is breaking out of the loop the correct thing
                # to do?  For volume migrations it is, because
                # copy_files() is passed a list of bfids from
                # one volume.  What if it is just passed any
                # old list of bfids from the command line?
                break

    # terminate the copy_queue
    if debug:
        log(MY_TASK, "no more to copy, terminating the copy queue")
    copy_queue.put(SENTINEL, block = True)
    if debug:
        log(MY_TASK, "no more to copy, terminating the deleted copy queue")
    deleted_copy_queue.put(SENTINEL, block = True)
    if debug:
        log(MY_TASK, "leaving thread/process")

    #Before launching the next thread, lets cleanup the stack on
    # this side.
    del copy_queue
    del deleted_copy_queue

    db.close()  #Avoid resource leaks.

##########################################################################

# migration_file_family(ff) -- making up a file family for migration
def migration_file_family_migration(bfid, ff, fcc, intf, deleted = NO):
    __pychecker__ = "unusednames=bfid,fcc" #Reserved for duplication.

    if deleted == YES:
        return DELETED_FILE_FAMILY + MIGRATION_FILE_FAMILY_KEY
    else:
        if intf.file_family:
            return intf.file_family + MIGRATION_FILE_FAMILY_KEY
        else:
            return ff + MIGRATION_FILE_FAMILY_KEY

#Duplication may override this.
migration_file_family = migration_file_family_migration

# normal_file_family(ff) -- making up a normal file family from a
#				migration file family
def normal_file_family_migration(ff):
    return ff.replace(MIGRATION_FILE_FAMILY_KEY, '')

#Duplication may override this.
normal_file_family = normal_file_family_migration

#Return True if the file_family has the pattern of a migration/duplication
# file.  False otherwise.
def is_migration_file_family_migration(ff):
    if ff.find(MIGRATION_FILE_FAMILY_KEY) == -1:
        return False

    return True

#Duplication may override this.
is_migration_file_family = is_migration_file_family_migration

#Return True if the file_family has 'DELETED_FILES' in the file_family.
# False otherwise.
def is_deleted_file_family(ff):
    if ff.find(DELETED_FILE_FAMILY) == -1:
        return False

    return True

# use_libraries() - Return the library or libraries to write to tape with.
#                   If multiple libraries are specified (to use the encp
#                   multiple copy feature) they are a comma seperated list
#                   with no white space.
#
# bfid - source bfid to migrate/duplicate
# filepath - source files full path in pnfs
# db - database object to directly query the database
# intf - interface class of migration or duplication
def use_libraries(bfid, filepath, file_record, db, intf):

    #Get the command line specified libraries (if any).
    user_libraries = getattr(intf, "library", "")
    if user_libraries == None:
        user_libraries = ""
    user_libraries = user_libraries.split(",")

    if file_record['deleted'] == "yes": #unknown should never get this far
        use_dirpath = chimera.get_directory_name(file_record['pnfs_name0'])
    else:
        use_dirpath = chimera.get_directory_name(filepath)

    #Get the pnfs specified libraries.
    dirs_to_try = []
    try:
        dirs_to_try.append(chimera.get_enstore_fs_path(use_dirpath))
    except OSError:
        pass

    try:
        dirs_to_try.append(chimera.get_enstore_pnfs_path(use_dirpath))
    except OSError:
        pass

    for dir_to_check in dirs_to_try:
        try:
            libs = chimera.Tag().readtag("library", dir_to_check)
            if libs is not None and len(libs) > 0:
                pnfs_libraries = libs[0].split(",")
                break #Found it!
            else:
                error_log("Library tag does not exist or empty in pnfs directory %s" % (dir_to_check,) )
        except (OSError, IOError):
            pass
    else:
        if getattr(intf, "library", ""):
            #Need to trust the user here.
            pnfs_libraries = intf.library
        else:
            error_log("Unable to determine correct library for deleted file.")
            log("HINT: use --library on the command line")
            return None

    #Get the number of copies remaining to be written to tape for this
    # special duplication mode of operation.
    if getattr(intf, "make_failed_copies", None):
        q = "select remaining from active_file_copying " \
            "where bfid = '%s';" % (bfid,)
        res = db.query(q).getresult()
        count = res[0][0]
    #Make sure the user specified enough libraries for this file.
    elif intf.library:
        if len(user_libraries) > 1 and \
               len(user_libraries) < len(pnfs_libraries):
            error_log("Destination directory writes %s copies; "
                      "only %s libraries specified for %s %s" %
                      (len(pnfs_libraries), len(pnfs_libraries),
                       bfid, filepath))
            return None
        else:
            count = len(user_libraries)
    else:
        count = len(pnfs_libraries)

    #The last count number of libraries are chosen.  This might be an
    # issue if --library and --make-failed-copies are used together.
    if intf.library:
        use_library = string.join(intf.library.split(",")[-(count):], ",")
    else:
        use_library = string.join(pnfs_libraries[-(count):], ",")

    return use_library

def p_show(p,tag=None):
    """
    print chimeraFile information

    @type  p: chimeraFile
    @param p: chimeraFile to be printed
    @type  tag: str
    @param tag: string 'tag' prepended at the beginning of each output line
    """
    print "%s            file = %s" % (tag,  p.path,)
    print "%s          volume = %s" % (tag,  p.volume,)
    print "%s location_cookie = %s" % (tag,  p.location_cookie,)
    print "%s            size = %s" % (tag,  p.size,)
    print "%s     file_family = %s" % (tag,  p.file_family,)
    print "%s          volmap = %s" % (tag,  p.volmap,)
    print "%s         pnfs_id = %s" % (tag,  p.pnfs_id,)
    print "%s        pnfs_vid = %s" % (tag,  p.pnfs_vid,)
    print "%s            bfid = %s" % (tag,  p.bfid,)
    print "%s           drive = %s" % (tag,  p.drive,)
    print "%s       meta-path = %s" % (tag,  p.p_path,)
    print "%s    complete_crc = %s" % (tag,  p.complete_crc,)
    return

# compare_metadata(p, f) -- compare metadata in pnfs (p) and filedb (f)
def compare_metadata(p, f, pnfsid = None, tag=None):
    if debug:
        log(tag+"::compare_metadata():", `f`)
#        sys.stdout.flush()
        p.show()
#        p_show(p,tag)
        sys.stdout.flush()
    if p.bfid != f['bfid']:
        return "bfid"
    if p.volume != f['external_label']:
        return "external_label"
    if p.location_cookie != f['location_cookie']:
        return "location_cookie"
    if long(p.size) != long(f['size']):
        return "size"

    if (pnfsid):
        pref = pnfsid
    else:
        pref = p.pnfs_id
    if (pref != f['pnfsid']):
        return "pnfsid"

    # some of old pnfs records do not have crc and drive information
    if p.complete_crc and long(p.complete_crc) != long(f['complete_crc']):
        return "crc"
    # do not check drive any more
    # if p.drive and p.drive != "unknown:unknown" and \
    #	p.drive != f['drive'] and f['drive'] != "unknown:unknown":
    #	return "drive"
    return None

#When migrating to multiple copies, we need to make sure we pick the
# ultimate original, not the first original.
# Word "altimate" (sic.) was used here and serves as prefix to name of several variables.
def find_original_migration(bfid, fcc):
    original_reply = fcc.find_the_original(bfid)
    f0 = {}
    if e_errors.is_ok(original_reply) \
           and original_reply['original'] != None \
           and original_reply['original'] != bfid:
        f0 = fcc.bfid_info(original_reply['original'])

    return f0

#Duplication may override this.
find_original = find_original_migration

#Get information about the files to copy and swap.  This is the common
# code for migration and duplication.  This function does not modify
# any metadata.
def _verify_metadata(MY_TASK, job, fcc, db):
    (src_file_record, src_volume_record, src_path,
     dst_file_record, dst_volume_record, tmp_path, mig_path) = job

    #shortcuts
    src_bfid = src_file_record['bfid']
    dst_bfid = dst_file_record['bfid']

    #If we happen to be migrating a multiple copy (which is only allowed
    # with --force), then we need to do not modify layers 1 and 4.
    is_migrating_multiple_copy = None

    log(MY_TASK, "swapping %s %s %s %s" % \
        (src_bfid, src_path, dst_bfid, mig_path))

    #It is possible to migrate a duplicate.  Need to handle finding
    # the original bfid's file recored, if there is one.
    #
    #It is also possible to make multiple copies while migrating, need
    # to check dst_bfid for this possibility.
    for mc_check_bfid in (src_bfid, dst_bfid):
        f0 = find_original(mc_check_bfid, fcc)
        if f0 and not e_errors.is_ok(dst_file_record):
            return_error = "original bfid: %s: %s" % \
                           (src_file_record['status'][0],
                            src_file_record['status'][1])
            return return_error, job, None, None, f0, \
                   is_migrating_multiple_copy
        if f0:
            break #found our original.

    """
    #For trusted pnfs systems, there isn't a problem,
    # but for untrusted we need to set the effective
    # IDs to the owner of the file.
    #
    # If the source PNFS file has been deleted only do the
    # chimera.File() instantiation; skip the euid/egid stuff to
    # avoid tracebacks.
    """

    # get all pnfs metadata - first the source file
    if src_file_record['deleted'] == "no":
        try:
            # This version handles the seteuid() locking.
            p1 = File(src_path)
        except (KeyboardInterrupt, SystemExit):
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except (OSError, IOError), msg:
            return str(msg), job, None, None, f0, is_migrating_multiple_copy
        except:
            exc, msg = sys.exc_info()[:2]
            return str(msg), job, None, None, f0, is_migrating_multiple_copy
    else:
        # What do we need an empty File class for?
        p1 = chimera.File(src_path)

    # get all pnfs metadata - second the destination file
    try:
        p2 = File(mig_path)
    except (KeyboardInterrupt, SystemExit):
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
    except (OSError, IOError), msg:
        return str(msg), job, None, None, f0, is_migrating_multiple_copy
    except:
        exc, msg = sys.exc_info()[:2]
        return str(msg), job, None, None, f0, is_migrating_multiple_copy

    ##################################################################
    #Handle deleted files specially.
    if src_file_record['deleted'] == YES:
        # We can skip the checking that is performed in the rest of
        # this function.
        res = ''
        return res, job, p1, p2, f0, is_migrating_multiple_copy
    ###################################################################

    #If we happen to be migrating a multiple copy (which is only allowed
    # with --force), then we need to do not modify layers 1 and 4.
    is_migrating_multiple_copy = False

    # check if the metadata are consistent
    res = compare_metadata(p1, src_file_record, tag="VM1.s [p1==src_file_record]")
    if debug_p:
        print "DEBUG: compare_metadata VM1.s res=%s src_file_record=%s" % (res,src_file_record,)
        p_show(p1,"VM1.s")

    # deal with already swapped metadata
    if res == "bfid":
        res = compare_metadata(p1, dst_file_record, tag="VM1.d [p1==dst_file_record]")
        if debug_p:
            print "DEBUG: compare_metadata VM1.d res=%s dst_file_record=%s" % (res,dst_file_record,)
            p_show(p1,"VM1.d")
        if res == "bfid" and f0:
            #Compare original, if applicable.
            res = compare_metadata(p1, f0, tag="VM1 [p1==f0]")
            if debug_p:
                print "DEBUG: compare_metadata VM1.f0 res=%s f0=%s" % (res,f0,)
                p_show(p1,"VM1.f0")
            if not res:
                #Note: Don't update layers 1 and 4!
                is_migrating_multiple_copy = True
        else:
            if not res:
                #The metadata has already been swapped.
                if debug_p:
                    print "DEBUG: compare_metadata is About to return OK res=%s job=%s p1=%s p2=%s " \
                        % (res,job,p1,p2,)
                return None, job, p1, p2, f0, is_migrating_multiple_copy
    if res:
        return_error = "[1] metadata %s %s are inconsistent on %s" \
                       % (src_bfid, src_path, res)
        return return_error, job, p1, p2, f0, is_migrating_multiple_copy

    if debug_p:
        print "DEBUG: compare_metadata - checking p2"

    if not p2.bfid and not p2.volume:
        #The migration path has already been deleted.  There is
        # no file to compare with.
        pass
    elif dst_file_record['deleted'] == UNKNOWN:
        #If the destination file has incomplete metadata skip the file
        # check, because it has been observed that other attempts may
        # have clobbered the migration path.  This avoids a very cryptic
        # error message.
        #
        # Note: This is only been observed when re-running failures from
        #       --make-failed-copies.
        #
        #[root@stkendm5a src]# ./migrate.py --status CDMS126809553600001
        #  (VOV014) src_bfid SDB   (VP1291) dst_bfid SDB copied swapped
        # CDMS126809553600001  N  CDMS127764084200000  N       y      y
        #
        #MIGRATION
        #
        #  (VOV014) src_bfid SDB   (VON980) dst_bfid SDB copied swapped
        # CDMS126809553600001 ON  CDMS126809555000000 MUE
        #
        #MULTIPLE_COPY
        #
        # In the above example, trying to proceed with finishing the multiple
        # copy CDMS126809555000000 was failing, because the failed multiple
        # copy CDMS127764084200000 owns the tempory PNFS file.  It may look
        # like CDMS127764084200000 exists from migration, however checking
        # the file_family of VP1291 reveals that it really is a failed
        # second attempt at duplication.
        #
        # enstore info --gvol VP1291 | grep volume_family
        #  'volume_family': 'minerva.minerva_copy_2.cpio_odc',
        pass
    else:
        res = compare_metadata(p2, dst_file_record, tag="VM2 [p2==dst_file_record]")
        # deal with already swapped file record
        if res == "pnfsid":
            res = compare_metadata(p2, dst_file_record, p1.pnfs_id, tag="VM2 [p2==dst_file_record+p1]")
        elif res == "bfid" and f0:
            res = compare_metadata(p2, f0, p1.pnfs_id, tag="VM2 [p2==f0,p1.pnfs_id]")
        if res:
            return_error = "[2] metadata %s %s are inconsistent on %s" \
                           % (dst_bfid, mig_path, res)
            return return_error, job, p1, p2, f0, is_migrating_multiple_copy

    # cross check
    err_msg = ""
    if src_file_record['size'] != dst_file_record['size']:
        err_msg = "%s and %s have different size" % (src_bfid, dst_bfid)
    elif src_file_record['complete_crc'] != dst_file_record['complete_crc']:
        # check against 1 seed crc
        seed_1_crc = checksum.convert_0_adler32_to_1_adler32(src_file_record['complete_crc'], src_file_record['size'])
        if seed_1_crc != dst_file_record['complete_crc']:
            err_msg = "%s and %s have different crc" % (src_bfid, dst_bfid)
    elif src_file_record['sanity_cookie'] != dst_file_record['sanity_cookie']:
        err_msg = "%s and %s have different sanity_cookie" % (src_bfid, dst_bfid)
        log(MY_TASK, str(src_file_record['sanity_cookie']), str(dst_file_record['sanity_cookie']))

    if err_msg:
        if dst_file_record['deleted'] == YES and not is_swapped(src_bfid, fcc, db):
            log(MY_TASK,
                "undoing migration of %s to %s do to error" % (src_bfid, dst_bfid))
            log_uncopied(src_bfid, dst_bfid, fcc, db)
        return err_msg, job, p1, p2, f0, is_migrating_multiple_copy


    job = (src_file_record, src_volume_record, src_path,
           dst_file_record, dst_volume_record, tmp_path, mig_path)

    return None, job, p1, p2, f0, is_migrating_multiple_copy

##################################################################
# swap_metadata(...) helper functions

PKG_PREFIX='package-'
PKG_SUFFIX='.tar'
def _move_package_file(src,volume,src_chimera_file):
        """
        move (rename) package file src=/pnfs_path/<old_volume>/<package> to /pnfs_path/<volume>/<package>

        @type  src: str
        @param src: source package full path
        @type  volume: str
        @param volume: destination volume name
        @type  chimera_file: chimera.File
        @param chimera_file: chimera file class instance for the source file
        @rtype: (str,None) or (None,str)
        @return: tuple (None, new_name) if package file moved OK
        @return: tuple (err_msg, None) in case of error
        """
        #split package path to package file name, volume name and everything else

        n=src.rsplit('/',2)
        if len(n) != 3:
            return (("Can not split package file path %s into /pnfs_path/<VOLUME>/<package>" % (src,)), None)
        fname=n[2]
        if not (fname.startswith(PKG_PREFIX) and fname.endswith(PKG_SUFFIX)):
            return (("Package file name %s does not look like '%s*%s', not moving package to <volume> dir in pnfs" \
                % (fname,PKG_PREFIX,PKG_SUFFIX)), None)

        # change volume to new volume and move package file in pnfs
        # we do not check old path contains old volume to fix situation when package file was in the wrong place
        n[1] = volume
        dest = '%s/%s/%s' % (n[0],n[1],n[2],)
        new_dir = '%s/%s' % (n[0],n[1],)

        if debug:
            log("MOVE_PACKAGE", "DEBUG:: Package file:\t%s" % src)
            log("MOVE_PACKAGE", "DEBUG:: Split:\t\t", n )
            log("MOVE_PACKAGE", "DEBUG:: New:\t\t", dest )
            log("MOVE_PACKAGE", "DEBUG:: New dir:\t" , new_dir )
            log("MOVE_PACKAGE", "DEBUG:: Package name:\t" , fname )

        try:
            os.makedirs(new_dir, 0755)
        except:
            exc = sys.exc_info()
            # any other error than package dir exists - return with error
            if not (exc[0] is exceptions.OSError and exc[1][0] == errno.EEXIST) :
                return (("Can not move package file in pnfs from %s to %s" % (src,dest,)), None)

        try:
            os.rename(src,dest)
        except:
            exc = sys.exc_info()
            # file has been moved on previous run?
            if exc[0] is exceptions.OSError and exc[1][0] == errno.ENOENT :
                # does the destination file exist?
                try:
                    statinfo = file_utils.get_stat(dest)
                except:
                    statinfo = None
                # is there a package in pnfs at destination path with right bfid and size?
                if statinfo is not None  :
                    cf = chimera.File(dest)
                    if cf.bfid == src_chimera_file.bfid \
                        and cf.size == src_chimera_file.size:
                            # package has been moved in pnfs/chimera in previous run
                            return (None,dest)
            return (("Can not move package file in pnfs from %s to %s" % (src,dest,)), None)

        # change package file name in pnfs layers
        try:
            change_pkg_name(src,dest,volume)
        except:
            return (("Can not update package file pnfs layers pnfs when moving package from %s to %s" % (src,dest,)), None)

        return (None,dest)

def _swap_metadata(MY_TASK, job, fcc, db, src_is_a_package=False):
    """
    swap_metadata() helper function -- swap metadata for src and dst

    @rtype: str or None
    @return: None if file swapped OK, otherwise error string

    Steps:
    [1] check the meta data consistency
    [2] f[bfid2][pnfsid] = f[bfid1][pnfsid] # use old pnfsid
    [3] pnfsid = f[bfid1][pnfsid]           # save it
    [4] p[src] = p[dst]                     # copy pnfs layer 4
    [5] p[src][pnfsid] = pnfsid
    """

    return_error, job, p1, p2, f0, is_migrating_multiple_copy = \
                  _verify_metadata(MY_TASK, job, fcc, db)

    if return_error:
        #The return_error value is a string with the error message.
        return return_error

    #Get information about the files to copy and swap.
    (src_file_record, src_volume_record, src_path,
     dst_file_record, dst_volume_record, tmp_path, mig_path) = job

    #shortcuts
    src_bfid = src_file_record['bfid']
    dst_bfid = dst_file_record['bfid']

    # Path conversion:
    # src_file_record['pnfs_name0'] is as written
    #     /pnfs/data2/file_aggregation/packages/ALEX.TestClone_7.cpio_odc/VOK310/package-M5-2012-04-11T17:48:35.884Z.tar
    # src_path is as mounted on this system ('canonical' pnfs file name)
    #     /pnfs/fs/usr/data2/file_aggregation/packages/ALEX.TestClone_7.cpio_odc/VOK310/package-M5-2012-04-11T17:48:35.884Z.tar
    # We will be replacing volume name like .../VOK310/...
    #   but the side effect will be modifying pnfs mount point to canonical setting

    # 4/4/13 As a stop gap measure to avoid failing the original file name check in encp,
    #   change file name to canonical for packages only but not for regular files:

    # pnfs path to be set in pnfs layer 4:
    # 1) This is the old way before SFA packaged based migration.
    #  - use the original name in FC src file record
    #  (same as prior to SFA)
    src_pnfs_name = src_file_record['pnfs_name0']

    # 2) This is to convert original file name for all files (packages and unpackaged)
    #     to canonical pnfs name /pnfs/fs/usr/...
    #src_pnfs_name = src_path

    if src_is_a_package :
        # move package file in pnfs to directory with new volume name
        volume = dst_file_record['external_label']
        err_msg, new_name = _move_package_file(src_pnfs_name,volume,p1)
        if err_msg is not None:
            return err_msg
        # use new pnfs location and use canonical pnfs file name
        src_pnfs_name = new_name
        src_path = new_name

    if debug_p:
        dst_before = fcc.bfid_info(dst_bfid)
        px = src_file_record['pnfsid']
        print "DEBUG: Before MODIFY pnfsid=%s dst_before=%s" % (px, dst_before,)

    # update Enstore DB file record for destination bfid to set pnfsid, file name as for src pnfs file
    mod_record = {'bfid': dst_bfid,
          'pnfsid':src_file_record['pnfsid'],
          'pnfs_name0':src_pnfs_name}
    if src_file_record['deleted'] == YES:
        if f0:
            #We need to use f0 when fixing files migrated to multiple
            # copies before the constraints in the DB were modified
            # to make the pair (src_bfid, dst_bfid) unique instead of
            # each column src_bfid & dst_bfid being unique.
            mod_record['deleted'] = f0['deleted']
        else:
            mod_record['deleted'] = YES

    res = fcc.modify(mod_record)
    if not e_errors.is_ok(res['status']):
        return "failed to change pnfsid for %s" % (dst_bfid,)

    if debug_p:
        dst_after = fcc.bfid_info(dst_bfid)
        px = src_file_record['pnfsid']
        print "DEBUG: After  MODIFY pnfsid=%s  dst_after=%s" % (px, dst_after,)

    # Update the layer 1 and layer 4 information.
    # Skip layers update if we are migrating a multiple_copy/duplicate or a deleted file.
    if not is_migrating_multiple_copy and not is_deleted_path(src_path) \
       and src_file_record['deleted'] == NO:
        ### swapping the PNFS layer metadata
        p1.volume = dst_file_record['external_label']
        p1.location_cookie = dst_file_record['location_cookie']
        p1.bfid = dst_bfid
        p1.drive = dst_file_record['drive']
        p1.complete_crc = dst_file_record['complete_crc']
        if src_is_a_package :
            p1.path = src_path

        # should we?
        # the best solution is to have encp ignore sanity check on file_family
        # p1.file_family = p2.file_family

        # check if p1 is writable - do this with euid/egid the same as the
        # the owner of the file

        try:
            src_stat = file_utils.get_stat(src_path)
        except (KeyboardInterrupt, SystemExit):
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except (OSError, IOError), msg: # Anticipated errors.
            return "%s is not accessable: %s" % (src_path, msg)
        except: # Un-anticipated errors.
            exc, msg = sys.exc_info()[:2]
            return str(msg)


        #If necessary, allow for the file to be set writable again.  We test
        # this way, since we need the original mode values from the stat()
        # above to reset them back if necessary.
        reset_permissions = False
        if not file_utils.e_access_cmp(src_stat, os.W_OK):
            reset_permissions = True
            try:
                make_writeable(src_path)
            except (KeyboardInterrupt, SystemExit):
                raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
            except (OSError, IOError), msg:  #Anticipated errors.
                return "%s is not writable as %s: %s" \
                       % (src_path, os.geteuid(), str(msg))
            except:
                exc, msg = sys.exc_info()[:2]
                return str(msg)

        # now perform the writes to the file's layer 1 and layer 4
        if src_file_record['deleted'] == "no":
            try:
                update_layers(p1)  #UPDATE LAYER 1 AND LAYER 4!
            except (KeyboardInterrupt, SystemExit):
                raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
            except (OSError, IOError), msg:
                return str(msg)
            except:
                exc, msg = sys.exc_info()[:2]
                return str(msg)

        if reset_permissions:
            #At this point, either the users is able to modify their
            # own file, or it was reset to allow all users to "write"
            # (A.K.A. enable chmod()) to the file.
            try:
                os.chmod(src_path, src_stat[stat.ST_MODE])
            except (OSError, IOError), msg:
                error_log("Unable to reset persisions for %s to %s" % \
                          (src_path, src_stat[stat.ST_MODE]))

    # Cross check:
    #  get information from pnfs and file clerk to verify pnfsid <-> bfid crossreferences just set
    #  are consistent
    p_check = chimera.File(src_path)
    file_record_check = fcc.bfid_info(dst_bfid)

    if debug_p:
        print "DEBUG: SM #0.p Check Again src_path=%s" % (src_path,)
        p_show(p_check,"SM #0")
        print "DEBUG: SM #0.r Check Again dst_bfid=%s, file_record_check=%s" % (dst_bfid,file_record_check,)

    if file_record_check['deleted'] == NO:
        if is_deleted_path(src_path):
            # Error
            return "a deleted path, %s, is not marked deleted" % (src_path,)

        res = compare_metadata(p_check, file_record_check, tag="SM1 [p_check==file_record_check]")
        if debug_p:
            print "DEBUG: compare_metadata #1.s res=%s file_record_check=%s" % (res,file_record_check,)
            p_show(p_check,"MC #1.s ")

        if res == "bfid" and f0:
            # Handle the possibility of migrating a multiple copy.
            res = compare_metadata(p_check, f0, tag="SM1 [p_check==f0]")

            if debug_p:
                print "DEBUG: compare_metadata #2.f0 res=%s file_record_check=%s" % (res,f0,)
                p_show(p_check,"MC #2.f0 ")

        if res:
            return "%s %s has inconsistent metadata on %s after swapping" \
                   % (dst_bfid, src_path, res)
    else:
        #We get here when the file is in the deleted state.  We need to skip
        # the metadata check between the EnstoreDB and PNFS/Chimera because
        # there is no PNFS/Chimera metadata to check with.
        #
        # An unknown file should never get this far, though, if it did it
        # would also fall through to here.
        pass

    if debug_p:
        print "DEBUG: OK _swap_metadata() for dst bfid %s" % (dst_bfid,)

    return None

def _switch_package(src_bfid, dst_bfid, fcc):
    """
    SFA: switch reference to package for all small files in the package (FCC method call)

    swap_metadata(...) helper function
    @type  src_bfid: str
    @param src_bfid: source package bfid
    @type  dst_bfid: str
    @param dst_bfid: destination package bfid
    @type  fcc: file_clerk_client.FileClient
    @param fcc:  file clerk client instance to use for operation

    @rtype: str or None
    @return: None if package switched OK, otherwise error string
    """

    MY_TASK = "SWITCH_PACKAGE"

    if debug:
        log(MY_TASK, "bfid=%s new_bfid=%s" % (src_bfid,dst_bfid))

    reply_ticket = fcc.swap_package({'bfid' : src_bfid , 'new_bfid' : dst_bfid})
    if not e_errors.is_ok(reply_ticket):
        error_log(MY_TASK, str(reply_ticket['status']))
        return "failed to switch package for bfid %s new_bfid %s" % (src_bfid,dst_bfid,)
    return None

def swap_metadata(job, fcc, db):
    """
    swap_metadata(job, fcc, db) -- swap metadata for src and dst

    @type  job: tuple
    @param job: tuple with job argumetns  (src_file_record, src_volume_record, src_path,
     dst_file_record, dst_volume_record, tmp_path, mig_path)
    @rtype: str or None
    @return: None if file swapped OK, otherwise error string
    """
    MY_TASK = "SWAP_METADATA"

    #Get information about the files to copy and swap.
    (src_file_record, src_volume_record, src_path,
     dst_file_record, dst_volume_record, tmp_path, mig_path) = job

    #shortcuts
    src_bfid = src_file_record['bfid']
    dst_bfid = dst_file_record['bfid']

    # check if migrated file is a package
    package_id = src_file_record.get("package_id", None)
    # shall we use active files?
    package_files_count = src_file_record.get("package_files_count", 0)

    src_is_a_package = (package_id is not None) and (src_bfid == package_id)

    # was_swapped: file was swapped in previous runs before this run
    # note: is_swapped() returns string with timestamp, make it boolean
    was_swapped = (dst_file_record is not None) and (is_swapped(src_bfid, fcc, db) is not None)

    if debug :
        log(MY_TASK, "src_bfid=%s package_id=%s package_files_count=%s" % (src_bfid,package_id,package_files_count,))

    if debug_p:
        bool_dst = (dst_file_record is not None)
        str_is = is_swapped(src_bfid, fcc, db)
        bool_was = ( str_is is not None)
        print "DEBUG: SWAP_METADATA bool_dst=%s bool_was=%s str_is=%s" % (bool_dst,bool_was,str_is)
        print "DEBUG: SWAP_METADATA src_is_a_package=%s src_bfid=%s package_id=%s package_files_count=%s" \
            % (src_is_a_package,src_bfid,package_id,package_files_count,)

    #log that the metadata updating is already done but do not return yet
    if was_swapped:
        ok_log(MY_TASK, "%s %s %s %s have already been swapped" \
               % (src_bfid, src_path,
                  dst_bfid, mig_path))
    else:
        err_swap_metadata = _swap_metadata(MY_TASK, job, fcc, db, src_is_a_package)
        if err_swap_metadata:
            error_log(MY_TASK, "%s %s %s %s failed due to %s" % (src_bfid, src_path, dst_bfid, mig_path, err_swap_metadata))
            return err_swap_metadata
    # at this point file metadata swapped OK

    # SFA: we check the number of files in package and switch the packaged files package_id if needed
    #  to process the case when package file has been swapped but packaged files have not been switched yet
    if src_is_a_package and package_files_count > 0:
        # switch parents for all small files in the source file (package) to destination file (package)
        err_switch_package = _switch_package(src_bfid, dst_bfid, fcc)

        if debug_p:
            print "DEBUG: SWAP_METADATA src_bfid=%s package_id=%s dst_bfid=%s err_switch_package=%s" \
                % (src_bfid,package_id,dst_bfid,err_switch_package,)

        if debug:
            log(MY_TASK, "src_bfid=%s package_id=%s err_switch_package=%s" % (src_bfid,package_id,err_switch_package))
        if err_switch_package:
            # per conversation with Sasha, we do not roll back swapped metadata for original files
            # when switching parent for the packaged file has failed
            error_log(MY_TASK, "%s %s %s %s failed when switching package for packaged files, error %s"
                      % (src_bfid, src_path, dst_bfid, mig_path, err_switch_package))
            return err_switch_package

    # todo: verify I report all cases and only once
    if not was_swapped:
        # report only in the case when it was not reported before (regular file has not been swapped)
        ok_log(MY_TASK, "%s %s %s %s have been swapped" % (src_bfid, src_path, dst_bfid, mig_path))
    return None

# tmp_path refers to the path that the file temporarily exists on disk.
# mig_path is the path that the file will be written to pnfs.
def write_file(MY_TASK,
	       src_bfid, src_path, tmp_path, mig_path,
               libraries, sg, ff, wrapper,
	       deleted, encp, intf):
        __pychecker__ = "unusednames=deleted" #Used to use; need in future?

	# check destination path
	if not mig_path:     # This can not happen!!!
		error_log(MY_TASK, "%s is not a pnfs entry" % (mig_path,))
		return 1
	# check if the directory is witeable
	try:
		(dst_directory, dst_basename) = os.path.split(mig_path)
		d_stat = file_utils.get_stat(dst_directory)
	except OSError, msg:
		if msg.errno == errno.ENOENT:
			d_stat = None
		else:
			error_log(MY_TASK, "failure stating %s: %s" % \
				  (mig_path, str(msg)))
			return 1
	# does the parent directory exist?
	if not d_stat:
		try:
			#We don't need to worry about being root here.
			# If the deepest directory is owned by root,
			# only root can create the subdirectory.  For
			# untrusted PNFSes, we can only fail and give
			# a good error message.  Trusted PNFSes will succeed.

			ok_log(MY_TASK, "making path %s" % (dst_directory,))
			os.makedirs(dst_directory)
		except (OSError, IOError), msg:
			if msg.args[0] == errno.EEXIST:
				#There is no error.  O_EXCL is not reliable
				# over NFS V2 (what PNFS uses).  Perhaps
				# makedirs() is hitting a this or a similar
				# problem for directories?
				pass
			else:
				error_log(MY_TASK,
					  "can not make path %s: %s" % \
					(dst_directory, str(sys.exc_info()[1])))
				if sys.exc_info()[1].errno == errno.EPERM and \
			   	   os.geteuid() == 0:
					log(MY_TASK,
					    "Question: Is PNFS trusted?")
					sys.exit(1)

				return 1
	if not d_stat and not os.access(dst_directory, os.W_OK):
		# can not create the file in that directory
		error_log(MY_TASK, "%s is not writable" % (dst_directory,))
		return 1

	# make sure the migration file is not there
	try:
		mig_stat = file_utils.get_stat(mig_path)
	except OSError, msg:
		if msg.errno == errno.ENOENT:
			mig_stat = None
		else:
			error_log(MY_TASK, "failure stating %s: %s" % \
				  (mig_path, str(msg)))
			return 1
	if mig_stat:
		log(MY_TASK, "migration file %s exists, removing it first" \
		    % (mig_path,))
		try:

			#Should the layers be nullified first?  When
			# migrating the same files over and over again the
			# answer is yes to avoid delfile complaing.
			# But what about production?
			nullify_pnfs(mig_path)
			file_utils.remove(mig_path)

		except (OSError, IOError), msg:
			error_log(MY_TASK,
				  "failed to delete migration file %s " \
                                  "as (uid %s, gid %s): %s" % \
				  (mig_path, os.geteuid(), os.getegid(),
				  str(msg)))
			return 1
                except:
                        error_log(MY_TASK,
				  "error trying to delete migration file %s " \
                                  "as (uid %s, gid %s): (%s: %s)" % \
				  (mig_path, os.geteuid(), os.getegid(),
				  str(sys.exc_info()[0]), str(sys.exc_info()[1])))
			return 1

        ## Build the encp command line.
        if intf.priority:
            use_priority = ["--priority", str(intf.priority)]
        else:
            use_priority = ["--priority", str(ENCP_PRIORITY)]
        if use_threaded_encp:
                use_threads = ["--threaded"]
        else:
                use_threads = []
        if debug:
                use_verbose = ["--verbose", "4"]
        else:
                use_verbose = []
        #dismount delay is the number of minutes a mover needs to wait
        # before dismounting a tape.  We set this to 2 minutes for each
        # library that a copy is written into.  This is to give a little
        # extra time to avoid writes bouncing between tapes with lots
        # of mounts and dismounts.
        user_libraries = libraries.split(",")
        dismount_delay = str(2 * len(user_libraries))
        encp_options = ["--delayed-dismount", dismount_delay,
                        "--ignore-fair-share"]
        #Override these tags to use the original values from the source tape.
        # --override-path is used to specify the correct path to be used
        # in the wrappers written with the file on tape, since this path
        # should match the original path not the temporary migration path
        # that the rest of the encp process will need to worry about.
        dst_options = ["--storage-group", sg, "--file-family", ff,
					   "--file-family-wrapper", wrapper,
                       "--library", libraries,
                       "--override-path", src_path,
                       "--file-family-width", str(intf.file_family_width),
                       "--no-crc"]

        sfa = []
        if intf.sfa_repackage:
            sfa += ['--enable-redirection']

        argv = ["encp"] + use_verbose + encp_options + sfa + use_priority + \
               dst_options + use_threads + [tmp_path, mig_path]

        if debug:
            cmd = string.join(argv)
            log(MY_TASK, 'cmd =', cmd)

        log(MY_TASK, "copying %s %s %s" % (src_bfid, tmp_path, mig_path))

        # Make the first attempt.
        res = encp.encp(argv)
        if res == 2:
                # If encp returns two (2), then we should not try again.  Encp
                # believes that a person needs to look into the problem.
                # Encp only returns two (2) for migration/duplication fatal
                # errors.
                error_log(MY_TASK, "failed to copy %s %s %s error = %s"
                    % (src_bfid, tmp_path, mig_path, encp.err_msg))
                return 1
        elif res == 1:
                log(MY_TASK, "failed to copy %s %s %s ... (RETRY)"
                    % (src_bfid, tmp_path, mig_path))
                # delete the target and retry once
                try:
                        file_utils.remove(mig_path)
                except (OSError, IOError), msg:
                        #If the file deletion failed becuase the file already
                        # does not exist, treat this case like a success.
                        # Fail all other errors.
                        if msg.args[0] != errno.ENOENT:
                                error_log(MY_TASK, "failed to remove %s as " \
                                          "(uid %s, gid %s): %s" % \
                                          (mig_path, os.geteuid(), os.getegid(), str(msg)))
                                return 1

		#Detect ghost files for better reporting.
		if dst_basename in os.listdir(dst_directory):
			message = "Tried to write to invalid directory entry."
			error_log(MY_TASK, message)
			log(MY_TASK,
			    "HINT: Remove %s using sclient." % (mig_path,))
			return 1

                # Make the second attempt.
                res = encp.encp(argv)
                if res:
                        error_log(MY_TASK, "failed to copy %s %s %s error = %s"
                                  % (src_bfid, tmp_path, mig_path,
                                     encp.err_msg))
                        # delete the target and give up
                        try:
                                file_utils.remove(mig_path)
                        except (OSError, IOError), msg:
                                #If the file deletion failed becuase the file
                                # already does not exist, treat this case like
                                # a success.  Fail all other errors.
                                if msg.args[0] != errno.ENOENT:

                                        error_log(MY_TASK,
                                                  "failed to remove %s as " \
                                                  "(uid %s, gid %s): %s" % \
                                                  (mig_path, os.geteuid(),
                                                   os.getegid(), str(msg)))

                        #Make sure all second attempt errors are handled
                        # correctly.
                        return 1
        elif res:
		#Some unknown error occured.
		error_log(MY_TASK, "failed to copy %s %s %s error = %s"
                    % (src_bfid, tmp_path, mig_path, encp.err_msg))
                return 1

        else:
                # log success of coping
                ok_log(MY_TASK, "%s %s is copied to %s" % \
                       (src_bfid, tmp_path, mig_path))

	if debug:
		log(MY_TASK, "written to tape %s %s %s"
		    % (src_bfid, tmp_path, mig_path))

        #
        detect_uncleared_deletion_lists(MY_TASK)

	return 0


# write_new_file() - Write a file temporarily sitting on disk to a new tape.
#   It is possible that multiple copy files are also written to additional
#   tapes.  Make sure to return None on error; this is what write_new_files()
#   is expecting.
def write_new_file(job, encp, vcc, fcc, intf, db):
    MY_TASK = "COPYING_TO_TAPE"

    time_to_write_new_file = time.time()

    #Get information about the files to copy and swap.
    (src_file_record, src_volume_record, src_path,
     dst_file_record, dst_volume_record, tmp_path, mig_path) = job

    src_bfid = src_file_record['bfid']

    if debug:
            log(MY_TASK, `job`)

    # check if it has already been copied
    if dst_file_record == None:
        is_it_copied = is_copied(src_bfid, fcc, db)
    else:
        is_it_copied = dst_file_record['bfid']
    dst_bfid = is_it_copied  #side effect: this is also the dst bfid

    has_tmp_file = False
    wrote_multiple_copies = False #set true if mupltiple copies written.
    mc_dst_bfids = [] #list of bfids of multiple copies written.
    if is_it_copied:
        ok_log(MY_TASK, "%s has already been copied to %s" \
               % (src_bfid, dst_bfid))

        if dst_file_record == None:
            # We need to be this draconian if we had to restart the
            # migration processes.
            dst_file_record = fcc.bfid_info(dst_bfid,
                                        timeout = 10, retry = 4)
            if not e_errors.is_ok(dst_file_record):
                error_log(MY_TASK, "no file record found(%s)" % (dst_bfid,))
                return

        if not mig_path:
            if dst_file_record['deleted'] == NO:
                #In order to determine the search order, we need to know
                # if it has been swapped or not.
                is_it_swapped = is_swapped(src_bfid, fcc, db)

                #Determine the search order of the bfids.  This is important,
                # because the defaluts for migration and duplication are
                # opposites and picking the wrong order slows things down.
                active_bfid, inactive_bfid, active_file_record, \
                             inactive_file_record = search_order(
                    src_bfid, src_file_record, dst_bfid, dst_file_record,
                    is_it_copied, is_it_swapped, fcc, db)

                try:
                    mig_path = pnfs_find(active_bfid, inactive_bfid,
                                         src_file_record['pnfsid'],
                                         file_record = active_file_record,
                                         alt_file_record = dst_file_record,
                                         intf = intf)
                    if not is_migration_path(mig_path):
                        #Need to make sure this is a migration path in case
                        # duplication is interupted.
                        mig_path = migration_path(mig_path, src_file_record)
                except (OSError, IOError), msg:
                    mig_path = migration_path(src_path, src_file_record)

            else:
                mig_path = migration_path(src_path, src_file_record)
                if mig_path == None:
                    #We need to use the original pathname, since the file is
                    # currently deleted (and /pnfs/fs was not able to be
                    # found).
                    mig_path = migration_path(src_file_record['pnfs_name0'],
                                              src_file_record)
                if mig_path == None:
                    #Is PNFS mounted?
                    error_log(MY_TASK, "No valid migration path found: %s" \
                              % (src_bfid,))
                    return
    else:
        if not mig_path:
            mig_path = migration_path(src_path, src_file_record)
            if mig_path == None and src_file_record['deleted'] != NO:
                #We need to use the original pathname, since the file is
                # currently deleted (and /pnfs/fs was not able to be found).

                mig_path = migration_path(src_file_record['pnfs_name0'],
                                          src_file_record)
            if mig_path == None:
                #Is PNFS mounted?
                error_log(MY_TASK, "No valid migration path found: %s" \
                          % (src_bfid,))
                return

        #Try and catch situations were an error left a zero
        # length file in the migration spool directory.  We
        # don't want to 'migrate' this wrong file to tape.
        try:
            #We want the size in layer 4, since large files
            # store a 1 for the size in pnfs.
            src_size = long(chimera.get_layer_4(src_path).get('size', None))
        except (OSError, IOError):
            src_size = None
        except (TypeError):
            if src_file_record['deleted'] == YES:
                #If the file is deleted, obtain the size from the Enstore DB.
                src_size = src_file_record['size']
            else:
                src_size = None
        try:
            tmp_size = long(os.stat(tmp_path)[stat.ST_SIZE])
        except (OSError, IOError):
            #We likely get here when the file is already
            # removed from the spooling directory.
            tmp_size = None
        if src_size != tmp_size:
            error_log(MY_TASK,
                      "size check mismatch %s(current %s, temp %s)" \
                      % (src_bfid, src_size, tmp_size))
            try:
                log(MY_TASK, "removing %s" % (tmp_path,))
                file_utils.remove(tmp_path)
            except (OSError, IOError), msg:
                log(MY_TASK, "error removing %s: %s" \
                    % (tmp_path, str(msg)))
            return

        #The library value can consist of a comma seperated list
        # of libraries, though in most cases there will be just one.
        # There are some 'odd' cases that use_libraries() handles
        # for us.
        libraries = use_libraries(src_bfid, src_path, src_file_record,
                                  db, intf)
        if libraries == None:
            #use_libraries() logs its own errors.
            return
        if len(libraries.split(",")) > 1:
            wrote_multiple_copies = True

        #Pull out these values from the volume_family.  Even for processing
        # a list with get, the volume information should all be the same.
        vf = src_volume_record['volume_family']
        storage_group = volume_family.extract_storage_group(vf)
        file_family = volume_family.extract_file_family(vf)
        wrapper = volume_family.extract_wrapper(vf)

        #The same goes for file families.  Migration and duplication
        # vary greatly with respect to the file family.  There are
        # some 'odd' cases that migration_file_family() handles for us.
        ff = migration_file_family(src_bfid, file_family, fcc, intf,
                                   src_file_record['deleted'])

        ## At this point src_path points to the original file's
        ## location in pnfs, tmp_path points to the temporary
        ## location on disk and mig_path points to the
        ## migration path in pnfs where the new copy is
        ## written to.

        rtn_code = write_file(MY_TASK, src_bfid, src_path,
                              tmp_path, mig_path,
                              libraries, storage_group, ff, wrapper,
                              src_file_record['deleted'], encp, intf)
        if rtn_code:
            return

    if debug:
        message = "Time to write new file: %.4f sec." % \
                  (time.time() - time_to_write_new_file,)
        log(MY_TASK, message)

    # Get bfid (and layer 4) of copied file.  We need these values
    # regardless if the file was already copied, or it was
    # just copied.
    if not is_it_copied:
        pf2 = chimera.File(mig_path)
        dst_bfid = pf2.bfid
        has_tmp_file = True
        if dst_bfid == None:
                error_log(MY_TASK,
                          "failed to get bfid of %s" % (mig_path,))
                return
        else:
                if wrote_multiple_copies:
                        #We need to obtain the list of the multiple
                        # copies written out.
                        mc_dst_bfids = get_multiple_copy_bfids(dst_bfid, db)

                # update success of copying (plus multiple copies)
                #
                # The dst_bfid needs to go first.  If a multiple copy
                # write is done using a migration DB table that
                # has seperate unique constraints for src & dst instead
                # of the more modern constraint on the pair of src
                # & dst columns, we need to make sure the original
                # copy gets into the database.
                for cur_dst_bfid in [dst_bfid] + mc_dst_bfids:
                        log_copied(src_bfid, cur_dst_bfid, fcc, db)

    else:
        if os.path.exists(tmp_path):
                #If the file is already copied, but the temporary on
                # disk file still remains, flag it for deletion.
                has_tmp_file = True

        #Need to get this list for re-runs of the migration.
        mc_dst_bfids = get_multiple_copy_bfids(dst_bfid, db)

    time_to_swap_metadata = time.time()

    keep_file = False
    mc_jobs = []  #Job tuples of multiple copies may go into this list.
    ## Perform modifications to the file metadata.  It does
    ## not need to be an actual swap (duplication isn't) but
    ## some type of modification is done.
    MY_TASK2 = "SWAPPING_METADATA"  #Switch to swapping task.
    if not is_swapped(src_bfid, fcc, db):

        if not dst_file_record:
            #Just written new files will get here.  Files already written
            # will just move on to getting the volume information.
            dst_file_record = fcc.bfid_info(dst_bfid,
                                            timeout = 10, retry = 4)
            if not e_errors.is_ok(dst_file_record):
                message = "no file record found(%s)" % (dst_bfid,)
                error_log(MY_TASK2, message)
                return

        #Obtain new original volume information.
        dst_volume_record = vcc.inquire_vol(dst_file_record['external_label'])
        if not e_errors.is_ok(dst_volume_record):
            error_log(MY_TASK2, dst_volume_record['status'])
            return

        #Update the job information with the latest information about
        # the destination file.
        job = (src_file_record, src_volume_record, src_path,
               dst_file_record, dst_volume_record, tmp_path, mig_path)

        res = swap_metadata(job, fcc, db)

        if not res:
            log_swapped(src_bfid, dst_bfid, fcc, db)
        elif res:
            keep_file = True
    else:
        #Reasons for calling swap_metadata even though it is already swapped:
        # 1) To have the correct migration/duplication function use the
        #    correct name in the log.
        # 2) In case the duplication updated the migration table, but
        #    failed to update the file_copies_map table.  The latter can
        #    now be set.
        job = (src_file_record, src_volume_record, src_path,
               dst_file_record, dst_volume_record, tmp_path, mig_path)
        res = swap_metadata(job, fcc, db)
        #res = None #No error.

    #It has been found that some make_failed_copies duplications have failed
    # in such a way that the file_copies_map table has a destination entry
    # pointing to a file record with empty metadata.  Running this again
    # fixes them in the swap metadata step, but we need to update
    # dst_file_record and job accordingly.  Just to be safe,
    # for all migrations/duplications get the dst_file_record again
    # if the pnfs path or pnfs id is missing from the record.
    if not dst_file_record['pnfs_name0'] or \
       not dst_file_record['pnfsid']:
        dst_file_record = get_file_info(MY_TASK, dst_bfid, fcc, db)
        if not e_errors.is_ok(dst_file_record):
            error_log(MY_TASK2,
                      "unable to obtain file information for %s" % (dst_bfid,))
            return
        else:
            #If no errors, update the job object too.
            job = (src_file_record, src_volume_record, src_path,
                   dst_file_record, dst_volume_record, tmp_path, mig_path)

    #Now do the same for all multiple copies (though not as much is
    # actully done).
    if not res:
        for cur_dst_bfid in mc_dst_bfids:
            if not is_swapped_by_dst(cur_dst_bfid, fcc, db) or \
                   not is_expected_restore_type(cur_dst_bfid, db):
                #Obtain new multiple copy volume information.
                mc_dst_file_record = fcc.bfid_info(cur_dst_bfid,
                                                   timeout = 10, retry = 4)
                if not e_errors.is_ok(dst_file_record):
                    message = "no file record found(%s)" % (cur_dst_bfid,)
                    error_log(MY_TASK2, message)
                    return

                #Obtain new multiple copy volume information.
                mc_dst_volume_record = vcc.inquire_vol(mc_dst_file_record['external_label'])
                if not e_errors.is_ok(mc_dst_volume_record):
                    error_log(MY_TASK2, mc_dst_volume_record['status'])
                    return

                mc_job = (src_file_record, src_volume_record, src_path,
                          mc_dst_file_record, mc_dst_volume_record, tmp_path,
                          mig_path)

                res = swap_metadata(mc_job, fcc, db)

                if not res:
                    #First we need to see if we need to include logging
                    # that the copy has been made.  This is likely if
                    # we are fixing multiple copies sometime after the
                    # original were done.
                    if not is_copied_by_dst(cur_dst_bfid, fcc, db):
                        log_copied(src_bfid, cur_dst_bfid, fcc, db)
                    #Now we can update the swapped field for these files.
                    log_swapped(src_bfid, cur_dst_bfid, fcc, db)
                    mc_jobs.append(mc_job)  #include this for return
                if res:
                    keep_file = True
            else:
                ok_log(MY_TASK2, "%s %s %s %s have already been swapped" \
                       % (src_bfid, src_path, dst_bfid, mig_path))
                job = (src_file_record, src_volume_record, src_path,
                       dst_file_record, dst_volume_record, tmp_path, mig_path)

    # Now remove the temporary file on disk.  We need to free
    # up the disk resources for more files.  We leave the file
    # in case of error, for failure analysis.
    if has_tmp_file and not keep_file:
        try:
                # remove tmp file
                file_utils.remove(tmp_path)
                ok_log(MY_TASK, "removed %s" % (tmp_path,))
        except (KeyboardInterrupt, SystemExit):
                raise sys.exc_info()[0], sys.exc_info()[1], \
                      sys.exc_info()[2]
        except (OSError, IOError):
                exc, msg = sys.exc_info()[:2]
                if msg.args[0] == errno.ENOENT:
                        #There is no error if the file already does
                        # not exist.
                        pass
                else:
                        error_log(MY_TASK,
                           "failed to remove temporary file %s as " \
                           "(uid %s, gid %s): %s" \
                           % (tmp_path, os.geteuid(), os.getegid(),
                              str(msg)))
                pass

    if debug:
        message = "Time to swap metadata: %.4f sec." % \
                  (time.time() - time_to_swap_metadata,)
        log(MY_TASK, message)

    #If we had an error while swapping, don't return the dst_bfid.
    if keep_file:
            return
    else:
            #return dst_bfid, mc_dst_bfids
            return job, mc_jobs

# write_new_files() -- second half of migration, driven by copy_queue
def write_new_files(thread_num, copy_queue, scan_queue, intf,
                    deleted_files = NO):
    if deleted_files == YES:
        MY_TASK = "COPYING_DELETED_TO_TAPE"
    else:
        MY_TASK = "COPYING_TO_TAPE"

    if debug:
        log(MY_TASK, "write_new_files() starts")

    if not USE_THREADS:
        #We need to delay starting this thread (only for processes)
        # until after the fork().
        copy_queue.start_waiting()

    # get a database connection
    db = pg.DB(host=dbhost, port=dbport, dbname=dbname, user=dbuser)

    # get an encp
    name_ending = "_0"
    if thread_num:
        name_ending = "_%s" % (thread_num,)

    if deleted_files == YES:
            name_ending = "%s_DEL" % (name_ending,)
    threading.currentThread().setName("WRITE%s" % (name_ending,))
    encp = encp_wrapper.Encp(tid = "WRITE%s" % (name_ending,))

    #Get a file and volume clerk clients.
    config_host = enstore_functions2.default_host()
    config_port = enstore_functions2.default_port()
    csc = configuration_client.ConfigurationClient(
            (config_host, config_port))
    fcc = file_clerk_client.FileClient(csc)
    vcc = volume_clerk_client.VolumeClerkClient(csc)

    while 1:

        if debug:
            log(MY_TASK, "Getting next job for write.")

        job = copy_queue.get(block = True)

        if not job:
            # We are done.  Nothing more to do.
            # log(MY_TASK, "DEBUG:: Received job for write - got None, exiting loop" )
            break
        if debug:
            log(MY_TASK, "Received job %s for write." % (job[0]['bfid'],))
            #log(MY_TASK, "DEBUG:: Received job for write. job=%s" % (job[0],))

        # Make the new copy.
        try:
            rtn = write_new_file(job, encp, vcc, fcc, intf, db)
            if rtn and rtn[0]:  #Success.
                pass_along_job = rtn[0]  #Original copy info.
                mc_pass_along_jobs = rtn[1]  #Multiple copy info.
            else:  #Failure
                pass_along_job = None  #Original copy info.
                mc_pass_along_jobs = []  #Multiple copy info.
        except (KeyboardInterrupt, SystemExit):
            raise sys.exc_info()[0], sys.exc_info()[1], \
                  sys.exc_info()[2]
        except:
            #We failed spectacularly!
            pass_along_job = None  #Original copy info.
            mc_pass_along_jobs = []  #Multiple copy info.

            #Report the error so we can continue.
            exc_type, exc_value, exc_tb = sys.exc_info()
            Trace.handle_error(exc_type, exc_value, exc_tb)
            del exc_tb #avoid resource leaks
            error_log(MY_TASK, str(exc_type),
                      str(exc_value), " writing file %s" % (job,))

        #At this point dst_bfid equals None if there was an error.

        #Tell the final_scan() thread the next file.
        if intf.with_final_scan and pass_along_job:

            #Scan the multiple copies first, so that the new original copy
            # metadata gets updated last.  These multiple copy scans don't
            # update as much metadata afterward as with the new original.
            for mc_job in mc_pass_along_jobs:
                scan_queue.put(mc_job, block = True)

            #We don't flag the condition variable here, because
            # scans done with --with-final-scan wait until all
            # writes are done.
            scan_queue.put(pass_along_job, block = True)

    #Before launching the next thread, lets cleanup the stack on
    # this side.
    del copy_queue
    del scan_queue

    db.close()  #Avoid resource leaks.

##########################################################################

## src_path doesn't need to be an actuall path in pnfs.  It could be
## "--get-bfid <bfid>" or --get
def scan_file(MY_TASK, job, src_path, dst_path, intf, encp):

    (src_file_record, src_volume_record, src_path,
     dst_file_record, dst_volume_record, tmp_path, mig_path) = job

    #src_bfid = src_file_record['bfid']  #shortcuts
    dst_bfid = dst_file_record['bfid']

    open_log(MY_TASK, "verifying", dst_bfid, src_path, '...')

    ## Build the encp command line.
    if intf.priority:
        use_priority = ["--priority", str(intf.priority)]
    else:
        use_priority = ["--priority", str(ENCP_PRIORITY)]

    if dst_file_record['deleted'] == YES:
        use_override_deleted = ["--override-deleted"]
    else:
        use_override_deleted = []

    if intf.use_volume_assert or USE_VOLUME_ASSERT:
        use_check = ["--check"] #Use encp to check the metadata.
    else:
        use_check = []

#     If the src file path begins with two dashes (--) 
#     it really switches specifying alternate reading methods to encp
#     The most likely are --get-bfid or --override-deleted.
#     
#     Deleted files are the most likely, but scaning a multiple
#     copy is also possible.
    if src_path[0:2] == "--":
        use_src_path = src_path.split()
    else:
        use_src_path = [src_path]

    encp_options = ["--delayed-dismount", "1", 
                    "--ignore-fair-share",
                    "--threaded", 
                    "--bypass-filesystem-max-filesize-check"]
    argv = ["encp"] + encp_options \
            + use_priority + use_override_deleted + use_check \
            + use_src_path + [dst_path]

    if debug:
        cmd = string.join(argv)
        log(MY_TASK, "cmd =", cmd)

    # Read the file
    try:
        res = encp.encp(argv)
    except:
        exc, msg, tb = sys.exc_info()
        traceback.print_tb(tb)
        print exc, msg
        res = 1

    if res != 0:
        close_log("ERROR")
        error_log(MY_TASK, 
                  "failed on %s %s error = %s"
                        % (dst_bfid, src_path, encp.err_msg))
        return 1

    close_log("OK")
    ok_log(MY_TASK, dst_bfid, src_path)

    detect_uncleared_deletion_lists(MY_TASK)

    return 0


# Return the actual filename and the filename for encp.  The filename for
# encp may not be a real filename (i.e. --get-bfid <bfid>).
#
# Note: Because is_multiple_copy is now a required argument for
#       get_filenames(), it can now be used for duplication eliminating
#       the need for a duplicate.get_filenames() version of this function.
def get_filenames(MY_TASK, job,
                  is_multiple_copy, fcc, db, intf):
    __pychecker__ = "unusednames=MY_TASK"

    (src_file_record, src_volume_record, src_path,
     dst_file_record, dst_volume_record, tmp_path, mig_path) = job

    src_bfid = src_file_record['bfid']  #shortcuts
    dst_bfid = dst_file_record['bfid']
    likely_path = dst_file_record['pnfs_name0']

    is_already_migrated = False  #In case the destination is also a source.

    if is_deleted_file_family(dst_volume_record['volume_family']):
        use_deleted = YES
    else:
        use_deleted = dst_file_record['deleted']

    if use_deleted == YES:
        use_path = "--override-deleted --get-bfid %s" \
                   % (dst_bfid,)
        pnfs_path = likely_path #Is anything else more correct?
    else:
        if is_multiple_copy:
            original_reply = fcc.find_the_original(dst_bfid)
            if e_errors.is_ok(original_reply) \
               and original_reply['original'] != None \
               and original_reply['original'] != dst_bfid:
                f0 = get_file_info(MY_TASK, original_reply['original'], fcc, db)

                if original_reply['original'] == src_bfid:
                    bfid1 = src_bfid
                    bfid2 = dst_bfid
                else:
                    bfid1 = original_reply['original']
                    bfid2 = src_bfid

                fr1 = f0
                fr2 = src_file_record

                pnfsid = f0['pnfsid']
            else:
                raise ValueError(
                    "No original copy found for %s." % (dst_bfid,))
        else:
            bfid1 = dst_bfid
            bfid2 = None

            fr1 = dst_file_record
            fr2 = None

            pnfsid = dst_file_record['pnfsid']

        try:
            # get the real path
            #pnfs_path = find_pnfs_file.find_chimeraid_path(
            #    pnfs_id, dst_bfid,
            #    likely_path = likely_path,
            #    path_type = find_pnfs_file.FS)
            pnfs_path = pnfs_find(bfid1, bfid2, pnfsid, fr1, fr2, intf)
        except (KeyboardInterrupt, SystemExit):
            raise sys.exc_info()[0], sys.exc_info()[1], \
                  sys.exc_info()[2]
        except (OSError, IOError), msg:
            dst_bfid2 = is_copied(dst_bfid, fcc, db)
            if msg.args[0] == errno.EEXIST and dst_bfid2:
                    #We have determined that the destination
                    # file has already become a source file
                    # for a different migration.
                    pnfs_path = likely_path
                    is_already_migrated = True
            else:
                    raise sys.exc_info()[0], sys.exc_info()[1], \
                          sys.exc_info()[2]
        except:
            raise sys.exc_info()[0], sys.exc_info()[1], \
                  sys.exc_info()[2]

        if type(pnfs_path) == type([]):
            pnfs_path = pnfs_path[0]

        #If the true target is a multiple copy, we need to make sure
        # we read the correct file.
        if is_already_migrated:
            use_path = "--skip-pnfs --get-bfid %s" % (dst_bfid,)
        elif is_multiple_copy:
            use_path = "--skip-pnfs --get-bfid %s" % (dst_bfid,)
        else:
            use_path = pnfs_path


    return (pnfs_path, use_path)

#This function contains common code for cleanup between migration and
# duplication.
def cleanup_after_scan_common(MY_TASK, mig_path):
    try:
        # rm the migration path.
        os.stat(mig_path)
        try:
            #If the file still exists, try deleting it.
            nullify_pnfs(mig_path)
            file_utils.remove(mig_path)

            ok_log(MY_TASK, "removed migration path %s" % (mig_path,))
        except (OSError, IOError), msg:
            #If we got the errors that:
            # 1) the file does not exist
            # or
            # 2) that the filename exists, but is no longer a
            #    regular file (i.e. is a directory)
            #then we don't need to worry.
            if msg.args[0] not in (errno.ENOENT, errno.EISDIR):
                error_log(MY_TASK,
                          "migration path %s was not deleted: %s" \
                          % (mig_path, str(msg)))
                return 1
    except (OSError, IOError), msg2:
        if msg2.args[0] in (errno.ENOENT,):
            #If the target we are trying to delete no longer
            # exists, there is no problem.
            pass
        else:
            error_log(MY_TASK,
                      "migration path %s was not deleted: %s" \
                      % (mig_path, str(msg)))
            return 1

    return 0

def cleanup_after_scan_migration(MY_TASK, mig_path, src_bfid, fcc, db):
    cas_exit_status = cleanup_after_scan_common(MY_TASK, mig_path)
    if cas_exit_status:
        return cas_exit_status

    # Make sure the original is marked deleted.  It could be argued, that
    # if we have deleted the file that we don't need to explicitly mark
    # the bfid deleted, but there is the delay between delfile running for
    # that to happen.  It's cleaner to do both.
    f = fcc.bfid_info(src_bfid)
    if e_errors.is_ok(f['status']) and f['deleted'] != YES:
        rtn_code = mark_deleted(MY_TASK, src_bfid, fcc, db)
        if rtn_code:
            #Error occured.
            return 1

#Duplication may override this.
cleanup_after_scan = cleanup_after_scan_migration

def final_scan_file(MY_TASK, job, fcc, encp, intf, db):

    (src_file_record, src_volume_record, src_path,
     dst_file_record, dst_volume_record, tmp_path, mig_path) = job

    src_bfid = src_file_record['bfid']  #shortcuts
    dst_bfid = dst_file_record['bfid']
    likely_path = dst_file_record['pnfs_name0']

    #We need to tell final_scan_file() if the file is a multiple
    # copy or not.
    if is_multiple_copy_bfid(dst_bfid, db):
        is_multiple_copy = True
    else:
        is_multiple_copy = False

    ct = is_checked(dst_bfid, fcc, db)
    if ct:
        ok_log(MY_TASK, dst_bfid, "is already checked at", ct)
        # make sure the migration path has been removed
        mig_path = migration_path(likely_path, src_file_record)

    else:
        #log(MY_TASK, "start checking %s %s"%(dst_bfid, src))
        try:
            (pnfs_path, use_path) = get_filenames(
                MY_TASK, job, is_multiple_copy, fcc, db, intf)
        except (OSError, IOError), msg:
            if msg.args[0] == errno.EBADF and \
                     msg.args[1].find("conflicting layer") != -1:
                #If we get here, we have a state where PNFS is returning
                # different values for the normal pathname and the
                # .(access)() pathname.  Remounting the filesystem usually
                # clears this situation.
                error_log(MY_TASK, msg.args[1])
                log(MY_TASK, "HINT: remount the PNFS filesystem and/or " \
                    "flush the PNFS file system buffer cache.")
                return 1

            exc_type, exc_value, exc_tb = sys.exc_info()
            Trace.handle_error(exc_type, exc_value, exc_tb)
            del exc_tb #avoid resource leaks
            error_log(MY_TASK, str(exc_type),str(exc_value),
                      " %s %s %s %s is not a valid pnfs file" \
                      % (dst_volume_record['external_label'],
                         dst_bfid,
                         dst_file_record['location_cookie'],
                         dst_file_record['pnfsid']))
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except:
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]

        # Make sure the destination volume is found as the volume mentioned
        # in layer 4. The file must be active.
        # The Error message is reported by is_expected_volume().
        if dst_file_record['deleted'] == NO:
            ret = is_expected_volume(
                    MY_TASK, 
                    dst_volume_record['external_label'], pnfs_path, fcc, db)
            if not ret:
                return 1

        # make sure the path is NOT a migration path
        if pnfs_path == None:
            error_log(MY_TASK,'none swapped file %s' % (pnfs_path))
            return 1

        if is_migration_path(pnfs_path):
            #It has been found that write failures from previous migrations
            # leave files with "Migration' in the path.  The scan should allow
            # these failures.
            if not is_migration_path(src_file_record['pnfs_name0']) and \
               src_file_record['deleted'] == NO:
                error_log(MY_TASK,'found Migration file %s' % (pnfs_path))
                return 1

        mig_path = migration_path(pnfs_path, src_file_record)

        # Replace src_path with the source path to use, which may not even
        # be a path in situations like scaning a deleted file.
        job = (src_file_record, src_volume_record, use_path,
               dst_file_record, dst_volume_record, tmp_path, mig_path)

        # Scan destination file by reading it to /dev/null
        rtn_code = scan_file(MY_TASK, job, use_path, "/dev/null", intf, encp)
        if rtn_code:
            return 1

        #Log the file as having been checked/scanned.
        log_checked(src_bfid, dst_bfid, fcc, db)

    # end of: if ct

    # Cleanup after scan if needed; otherwise we done.

    #Determine if this destination file is a multple copy or not.  Remove
    # the src_bfid from the list, so that we can tell the difference
    # between the cases listed below.
    original_copy_list = get_original_copy_bfid(dst_bfid, db)
    scrubbed_copy_list = []
    for i in range(len(original_copy_list)):
        if original_copy_list[i] != src_bfid:
            scrubbed_copy_list.append(original_copy_list[i])

    # There are two conditions when we do not want to cleanup.
    #    We indicate it by setting remove_migration_path to False
    # Those destination bfids are surrounded by asterisks (*) in diagram below.
    #
    # The M indicates an entry in the migration table.
    # The D indicates an entry in the file_copies_map table.
    #
    # 1) src_bfid -MD-> dst_bfid          Duplication with multiple copies
    #           |          |
    #           |          D
    #           |          |
    #           |          v
    #           |--MD--> *dst_bfid*
    #
    # 2) src_bfid -MD-> dst_bfid          Duplication to one copy
    #
    # 3) src_bfid -M--> dst_bfid          Migration with multiple copies
    #           |          |
    #           |          D
    #           |          |
    #           |          v
    #           |--M--> *dst_bfid*
    #
    # 4) src_bfid -M--> dst_bfid          Migration to one copy
    #
    # For all other dst_bfids we want to cleanup (True).

    if len(scrubbed_copy_list) > 0:
        remove_mig_path = False
    else:
        remove_mig_path = True

    # cleanup_after_scan() reports its own errors.
    #
    # Only do this for orignal destination copies, not for any of its possible
    # multplie copies.
    #
    # Duplication specific:
    #    If --make-copies or --make-failed-copies
    # were specified on the command line, call the duplication version
    # to cleanup anyway, since the "original" in these cases already
    # exists and won't be scanned.
    #
    # Duplication note: 
    #    When scanning multple copies, the original or multiple copy

    if (remove_mig_path 
            or getattr(intf, 'make_failed_copies', None) 
            or getattr(intf, 'make_copies', None)):
        return cleanup_after_scan(MY_TASK, mig_path, src_bfid, fcc, db)

#If given a list of destination bfids, scan them.
def final_scan_files(dst_bfids, intf):
        MY_TASK = "FINAL_SCAN"
        local_error = 0

        # get its own file & volume clerk client
        config_host = enstore_functions2.default_host()
        config_port = enstore_functions2.default_port()
        csc = configuration_client.ConfigurationClient((config_host,
                                                        config_port))
        fcc = file_clerk_client.FileClient(csc)
        vcc = volume_clerk_client.VolumeClerkClient(csc)

        #get a database connection
        db = pg.DB(host=dbhost, port=dbport, dbname=dbname, user=dbuser)

        # get an encp
        threading.currentThread().setName('FINAL_SCAN')
        encp = encp_wrapper.Encp(tid='FINAL_SCAN')

        for dst_bfid in dst_bfids:
            #Get the destination info.
            dst_file_record = get_file_info(MY_TASK, dst_bfid, fcc, db)
            dst_volume_record = get_volume_info(MY_TASK,
                                            dst_file_record['external_label'],
                                                vcc, db)

            #Get the source info.
            src_bfid = get_bfids(dst_bfid, fcc, db)[0]
            src_file_record = get_file_info(MY_TASK, src_bfid, fcc, db)
            src_volume_record = get_volume_info(MY_TASK,
                                            src_file_record['external_label'],
                                                vcc, db)

            job = (src_file_record, src_volume_record, None,
                   dst_file_record, dst_volume_record, None, None)

            ## Scan the file by reading it with encp.
            ## Note: if we are using volume assert, then final_scan_file()
            ##       uses --check with the encp to avoid redundant
            ##       reading of the file.
            rtn_code = final_scan_file(MY_TASK, job, fcc, encp, intf, db)
            if rtn_code:
                local_error = local_error + 1
                continue

            # If we get here, then the file has been scaned.  Consider
            # it closed too.
            ct = is_closed(dst_bfid, fcc, db)
            if not ct:
                log_closed(src_bfid, dst_bfid, fcc, db)
                close_log('OK')

        db.close()  #Avoid resource leaks.

        return local_error

# final_scan() -- last part of migration, driven by scan_queue
#   read the file as user to reasure everything is fine
def final_scan(thread_num, scan_list, intf, deleted_files = NO):
    if deleted_files == YES:
        MY_TASK = "FINAL_SCAN_DELETED"
    else:
        MY_TASK = "FINAL_SCAN"

    # get its own file clerk client
    config_host = enstore_functions2.default_host()
    config_port = enstore_functions2.default_port()
    csc = configuration_client.ConfigurationClient((config_host,
                                                    config_port))
    fcc = file_clerk_client.FileClient(csc)

    #get a database connection
    db = pg.DB(host=dbhost, port=dbport, dbname=dbname, user=dbuser)

    # get an encp
    name_ending = "_0"
    if thread_num:
        name_ending = "_%s" % (thread_num,)
    if deleted_files == YES:
        name_ending = "%s_DEL" % (name_ending,)
    threading.currentThread().setName("FINAL_SCAN%s" % (name_ending,))
    encp = encp_wrapper.Encp(tid = "FINAL_SCAN%s" % (name_ending,))

    #Loop over the files ready for scanning.
    for job in scan_list:

        try:
            final_scan_file(MY_TASK, job, fcc, encp, intf, db)
        except (KeyboardInterrupt, SystemExit):
            raise sys.exc_info()[0], sys.exc_info()[1], \
                  sys.exc_info()[2]
        except:
            #We failed spectacularly!

            #Report the error so we can continue.
            exc_type, exc_value, exc_tb = sys.exc_info()
            Trace.handle_error(exc_type, exc_value, exc_tb)
            del exc_tb #avoid resource leaks
            error_log(MY_TASK, str(exc_type),
                      str(exc_value),
                      " scanning file for %s" \
                      % (job,))
            break

    db.close()  #Avoid resource leaks.

# NOT DONE YET, consider deleted file in final scan
# Is the file deleted due to copying error?
# or was it deleted before migration?


# final_scan_volume(vol) -- final scan on a volume when it is closed to
#				write
# This is run without any other threads
#
# deal with deleted file
# if it is a migrated deleted file, check it, too
def final_scan_volume(vol, intf):
    MY_TASK = "FINAL_SCAN_VOLUME"
    local_error = 0
    # get its own fcc and vcc
    config_host = enstore_functions2.default_host()
    config_port = enstore_functions2.default_port()
    csc = configuration_client.ConfigurationClient((config_host,
                                                    config_port))
    fcc = file_clerk_client.FileClient(csc)
    vcc = volume_clerk_client.VolumeClerkClient(csc)

    # get an encp
    threading.currentThread().setName('FINAL_SCAN')
    encp = encp_wrapper.Encp(tid='FINAL_SCAN')
    volume_assert = volume_assert_wrapper.VolumeAssert(tid='FINAL_SCAN')


    log(MY_TASK, "verifying volume", vol)

    dst_volume_record = vcc.inquire_vol(vol)
    if dst_volume_record['status'][0] != e_errors.OK:
        error_log(MY_TASK,
                  "failed to find volume %s: %s" % (vol,
                                      dst_volume_record['status'][1]))
        return 1
    if debug:
        log(MY_TASK, "volume_info:", str(dst_volume_record))

    # get a db connection
    db = pg.DB(host=dbhost, port=dbport, dbname=dbname, user=dbuser)

    # make sure this is a migration volume
    sg, ff, wp = string.split(dst_volume_record['volume_family'], '.')
    is_migration_closed = is_migration_history_closed(MY_TASK, vol, db)
    if is_migration_closed == None:
        #Confirm that the history does not return an error case.  If it did
        # we get here.
        error_log(MY_TASK, "Unable to continue, migration history error")
        db.close()  #Avoid resource leaks.
        return 1
    if not migrated_from(vol, db):
        #This volume is not a destination volume.
        error_log(MY_TASK, "%s is not a %s volume" %
                  (vol, MIGRATION_NAME.lower()))
        db.close()  #Avoid resource leaks.
        return 1
    elif not is_migration_closed:
        #If the scanning is not recorded as completed in the migration_history
        # table, let the scanning proceed regardless of the following
        # system_inhibit and file_family tests.
        pass
    # make sure the volume is ok to scan (check system_inhibit 0)
    elif dst_volume_record['system_inhibit'][0] != 'none':
        error_log(MY_TASK, 'volume %s is "%s"' % (vol,
                                      dst_volume_record['system_inhibit'][0]))
        db.close()  #Avoid resource leaks.
        return 1
    # If the destination tapes are already migrated, don't continue.
    elif not is_migration_file_family(ff) and \
             not getattr(intf, "force", None):
        error_log(MY_TASK, "%s has a non-%s file family" %
                  (vol, MIGRATION_NAME.lower()))
        db.close()  #Avoid resource leaks.
        return 1
    #Verify that the system_inhibit 1 is in a valid state too.
    elif (dst_volume_record['system_inhibit'][1] != 'full' and \
            dst_volume_record['system_inhibit'][1] != 'none' and \
            dst_volume_record['system_inhibit'][1] != 'readonly') \
            and is_migrated_by_dst_vol(vol, intf, db):
        error_log(MY_TASK, 'volume %s is "%s"' % (vol,
                                      dst_volume_record['system_inhibit'][1]))
        db.close()  #Avoid resource leaks.
        return 1

    #Warn if the volume about to be scanned is not full.  Scan a non-
    # full tape will not allow future migration files to be written
    # onto it (without intervention anyway).
    if dst_volume_record['system_inhibit'][1] != 'full':
        warning_log(MY_TASK, 'volume %s is not "full"'%(vol))
    #If necessary set the system_inhibit_1 to readonly.  Leave "full"
    # alone, but change the others.
    if dst_volume_record['system_inhibit'][1] != "readonly" and \
           dst_volume_record['system_inhibit'][1] != 'full':
        vcc.set_system_readonly(vol)
        log(MY_TASK, 'set %s to readonly'%(vol))

    #Get the CRCs for all files on the tape.
    assert_errors = {}
    if intf.use_volume_assert or USE_VOLUME_ASSERT:
        log(MY_TASK, "asserting %s" % (vol,))
        volume_assert_options = "--crc-check"
        cmd = "volume_assert --volume %s %s" % (vol,
                                                volume_assert_options)
        try:
            res = volume_assert.volume_assert(cmd)
        except:
            exc, msg, tb = sys.exc_info()
#            import traceback
            traceback.print_tb(tb)
            print exc, msg
            local_error = local_error + 1
            db.close()  #Avoid resource leaks.
            return local_error

        #Look for the assert of this volume.  There should only be one though.
        for done_ticket in volume_assert.err_msg:
            if done_ticket['volume'] == vol:
                break
        else:
            message = "volume %s return information not found" % (vol,)
            error_log(MY_TASK, message)
            local_error = local_error + 1
            db.close()  #Avoid resource leaks.
            return local_error

        #At this point, done_ticket['return_file_list'] is a dictionary,
        # keyed by location cookie of any errors that occured reading
        # the files.

        if res == 0:
            ok_log(MY_TASK, vol)
            assert_errors = done_ticket['return_file_list']
        else: # error
            error_log(MY_TASK, "failed on %s error = %s"
                      % (vol, done_ticket['status']))
            assert_errors = done_ticket.get('return_file_list', [])
            if len(assert_errors) == 0:
                #If we had an error and didn't get anything in the file
                # list, then give up.
                local_error = local_error + 1
                db.close()  #Avoid resource leaks.
                return local_error
            else:
                #If the volume assert failed, but did return some file
                # information, try and proceed.
                pass

    #tape_list is a list of file records
    tape_list = get_tape_list(MY_TASK, vol, fcc, db, intf)
    # Detect empty tapes.
    if len(tape_list) == 0:
        log(MY_TASK, vol, "volume is empty")
        db.close()  #Avoid resource leaks.
        return 0

    #Loop over all the files on the tape and verify everything is okay.
    for dst_file_record in tape_list:
        dst_bfid = dst_file_record['bfid']

        #Get the source info.

        #First, get the src_bfid.
        (src_bfid, check_dst_bfid) = get_bfids(dst_bfid, fcc, db)
        if src_bfid == None and check_dst_bfid == None:
            if dst_file_record['deleted'] in [YES, UNKNOWN]:
                #The file is a failed migration file.
                message = "found failed migration file %s, skipping" \
                          % (dst_bfid,)
                log(MY_TASK, message)
            else:
                #Now for active files.  These could be from new files written
                # to the destination tape.  We only need to worry about this
                # here if the tape is being rescanned after being release to
                # users to write additional files onto it.
                message = "active file on destination tape without a source"
                warning_log(MY_TASK, message)

            continue
        #Second, get the bfid's file record.
        src_file_record = get_file_info(MY_TASK, src_bfid, fcc, db)
        if not e_errors.is_ok(src_file_record):
            error_log(MY_TASK,
                      "unable to obtain file information for %s" % (src_bfid,))
            local_error = local_error + 1
            continue
        #Third, get the source file's volume record.
        src_volume_record = get_volume_info(MY_TASK,
                                            src_file_record['external_label'],
                                            vcc, db, use_cache=True)

        job = (src_file_record, src_volume_record, None,
               dst_file_record, dst_volume_record, None, None)

        st = is_swapped(src_bfid, fcc, db)
        if not st:
            error_log(MY_TASK,
                      "%s %s has not been swapped" \
                      % (src_bfid, dst_bfid))
            local_error = local_error + 1
            continue
        ct = is_checked(dst_bfid, fcc, db)

        #If the user deleted the files, require --with-deleted be
        # used on the command line.  If the volume only contains
        # deleted files, which is determined from the file_family
        # part of the volume_family triple, the allow for scanning
        # without --with-deleted to be required on the command line.
        if dst_file_record['deleted'] == YES and \
               (intf.with_deleted or
                dst_volume_record['volume_family'].find(DELETED_FILE_FAMILY) != -1):
            pass #Just use likely_path; the file is deleted anyway.
        elif dst_file_record['deleted'] == YES and not intf.with_deleted:
            log(MY_TASK,
                "Skipping scan of deleted file: %s" \
                % (dst_bfid,))
            continue
        elif is_copied(dst_bfid, fcc, db):
            log(MY_TASK, "found destination bfid, %s, already migrated" \
                % (dst_bfid,))
            # This should be correct.  We don't need to pass a "src_path"
            # here as final_scan_file() figures that out on its own.
            pass
        elif ct:
            #Already checked.  We don't need to pass a "src_path"
            # here as final_scan_file() figures that out on its own.
            pass
        else:
            #Make sure we have the admin path.
            pass
            """
            try:
                likely_path = find_pnfs_file.find_chimeraid_path(
                    src_file_record['pnfsid'], dst_bfid,
                    likely_path = dst_file_record['pnfs_name0'],
                    path_type = enstore_constants.FS)
            except (OSError, IOError), msg:
                if msg.args[0] == errno.EBADF and \
                     msg.args[1].find("conflicting layer") != -1:
                    #If we get here, we have a state where PNFS is returning
                    # different values for the normal pathname and the
                    # .(access)() pathname.  Remounting the filesystem usually
                    # clears this situation.
                    local_error = local_error + 1
                    error_log(MY_TASK, msg.args[1])
                    log(MY_TASK, "HINT: remount the PNFS filesystem and/or " \
                        "flush the PNFS file system buffer cache.")
                else:
                    exc_type, exc_value, exc_tb = sys.exc_info()
                    Trace.handle_error(exc_type, exc_value, exc_tb)
                    del exc_tb #avoid resource leaks
                    error_log(MY_TASK, str(exc_type),
                              str(exc_value),
                              " %s %s %s %s is not a valid pnfs file" \
                              % (
                        dst_volume_record['external_label'],
                        dst_bfid,
                        dst_file_record['location_cookie'],
                        dst_file_record['pnfsid']))
                continue

            if not is_expected_volume(
                MY_TASK, vol, likely_path, fcc, db):
                #Error message reported from
                # is_expected_volume().
                local_error = local_error + 1
                continue
            """

        #If we are using volume_assert, check what the assert returned.
        if intf.use_volume_assert or USE_VOLUME_ASSERT:
            if not assert_errors.has_key(dst_file_record['location_cookie']):
                error_log(MY_TASK,
                          "assert of %s was not found in returned assert list"
                          % (dst_bfid,))
                local_error = local_error + 1
                continue
            if not e_errors.is_ok(assert_errors[dst_file_record['location_cookie']]):
                error_log(MY_TASK,
                          "assert of %s %s:%s failed" % \
                          (dst_bfid, vol, dst_file_record['location_cookie']))
                local_error = local_error + 1
                continue
            else:
                log(MY_TASK,
                    "assert of %s %s:%s succeeded" % \
                    (dst_bfid, vol, dst_file_record['location_cookie']))

        #We need to tell final_scan_file() if the file is a multiple
        # or not.
        #if is_multiple_copy_bfid(dst_bfid, db):
        #        is_multiple_copy = True
        #else:
        #        is_multiple_copy = False

        ## Scan the file by reading it with encp.
        ## Note: if we are using volume assert, then final_scan_file()
        ##       uses --check with the encp to avoid redundant
        ##       reading of the file.
        #rtn_code = final_scan_file(MY_TASK, src_bfid, dst_bfid,
        #			   pnfs_id, likely_path, deleted,
        #                           is_multiple_copy,
        #			   fcc, encp, intf, db)
        rtn_code = final_scan_file(MY_TASK, job, fcc, encp, intf, db)
        if rtn_code:
            local_error = local_error + 1

            if not intf.use_volume_assert and \
               not USE_VOLUME_ASSERT:
                #If we failed reading the file, check if
                # the tape is still accessable.
                if not is_volume_allowed(vol, vcc, db):
                    # If we get here the tape has been
                    # marked NOACCESS or NOTALLOWED.
                    message = "%s is NOACCESS or NOTALLOWED" \
                              % (vol,)
                    error_log(MY_TASK, message)

                    break
            continue

        # If we get here, then the file has been scaned.  Consider
        # it closed too.
        ct = is_closed(dst_bfid, fcc, db)
        if not ct:
            log_closed(src_bfid, dst_bfid, fcc, db)
            close_log('OK')

    # restore file family only if there is no error
    if not local_error and is_migrated_by_dst_vol(vol, intf, db):
        rtn_code = set_dst_volume_migrated(
            MY_TASK, vol, sg, ff, wp, vcc, db)
        if rtn_code:
            #Error occured.
            local_error = local_error + 1

    else:
        error_log(MY_TASK,
                  "skipping volume metadata update since not all files have been scanned")

    db.close()  #Avoid resource leaks.

    return local_error

def set_dst_volume_migrated(MY_TASK, vol, sg, ff, wp, vcc, db):

	## Prepare to set the file family back to that of the original.
	ff = normal_file_family(ff)
	vf = string.join((sg, ff, wp), '.')
	## Prepare to remove the readonly system inhibit set at the begining
	## of final_scan_volume().
	v = vcc.inquire_vol(vol)
	if v['system_inhibit'][1] == "readonly":
		#only if the volume is currently readonly should this be
		# set back to 'none'.
		system_inhibit = [v['system_inhibit'][0], 'none']
	else:
		system_inhibit = v['system_inhibit']

	#Update the information with the volume clerk.
	res = vcc.modify({'external_label':vol, 'volume_family':vf,
			  'system_inhibit':system_inhibit})
	if res['status'][0] == e_errors.OK:
		ok_log(MY_TASK, "restore file_family of", vol, "to", ff)
	else:
		error_log(MY_TASK, "failed to restore volume_family of", vol, "to", vf)
		return 1



	## Set comment with the list of volumes.
	from_list = migrated_from(vol, db)
	vol_list = ""
	for i in from_list:
		# set last access time to now
		reply_ticket = vcc.touch(i)
                if not e_errors.is_ok(reply_ticket):
                    error_log(MY_TASK,
                            "failed to update last access time for %s: %s" \
                              % (i, str(reply_ticket['status'])))
                    return 1
		# log history closed
                rtn_value = log_history_closed(i, vol, vcc, db)
                if rtn_value:
                    #log_history_closed gives its own error.
                    return 1
		# build comment
		vol_list = vol_list + ' ' + i
	if vol_list:
		res = vcc.set_comment(vol, MFROM + vol_list)
		if res['status'][0] == e_errors.OK:
			ok_log(MY_TASK, 'set comment of %s to "%s%s"' \
			       % (vol, MFROM, vol_list))
		else:
			error_log(MY_TASK,
                                  'failed to set comment of %s to "%s%s"' \
                                  % (vol, MFROM, vol_list))
			return 1



	return 0

##########################################################################

# migrate(file_records): -- migrate a list of files
#
# file_records - list of file_records to migrate
# intf - Instantiated MigrateInterface class
# volume_record - If all of these files in the list belong on the same tape,
#                 then this should be set to the volume information in the DB.
def migrate(file_records, intf, volume_record=None):
	global errors

	errors = 0

        #Limit the number of processes/threads if only one or two files
        # is in the list.
	if PARALLEL_FILE_TRANSFER:
		use_proc_limit = min(proc_limit, len(file_records))
	else:
		use_proc_limit = min(1, len(file_records))

	i = 0
	#Make a list of proc_limit length.  Each element should
	# itself be an empty list.
	#
	# Don't do "[[]] * proc_limit"!!!  That will only succeed
	# in creating proc_limit references to the same list.
	use_bfids_lists = []
	while i < use_proc_limit:
		use_bfids_lists.append([])
		i = i + 1

        #Put each bfid in one of the bfid lists.  This should resemble
        # a round-robin effect.

        if USE_GET and type(volume_record) == types.DictType:
            #Organize the round-robin into groups of files_per_thread
            # in length for each get.
            if intf.read_to_end_of_tape:
                files_per_thread = len(file_records)
            else:
                avg_size = average_size(volume_record)
                media_rate = drive_rate(volume_record)
                if avg_size == 0:
                    files_per_thread = 1
                elif media_rate == None:
                    log("WARNING: %s media type drive rate not known" % \
                        volume_record['media_type'])
                    files_per_thread = 1
                else:
                    #Return the number of files expected to be read in
                    # under a minute.
                    files_per_thread = int((media_rate * 60.0) / avg_size)
                    files_per_thread = max(1, files_per_thread)

            i = 0
            j = 0
            for j in range(0, len(file_records), files_per_thread):
                    use_bfids_lists[i].append(file_records[j:j+files_per_thread])
                    i = (i + 1) % use_proc_limit
        else:
            #Just put them into the round-robin order.
            i = 0
            for file_record in file_records:
                use_bfids_lists[i].append(file_record)
                i = (i + 1) % use_proc_limit

	#Create a set of locks for keeping the transfers in sync if
	# PARALLEL_FILE_TRANSFER is in use.
	sequential_locks = []
	for unused in range(use_proc_limit):
		if multiprocessing_available:
			sequential_locks.append(multiprocessing.Semaphore(1))
		else:
			sequential_locks.append(threading.Semaphore(1))

	#Lock all but the first lock.
	for lock in sequential_locks[1:]:
		if lock:
			lock.acquire()

	#Get scan queue once if volume assert is being used.
        # This needs to leave room for the SENTINEL.  In addition, we need
        # to handle cases where more than one multiple copy already exists in
        # --make-failed-copies mode; so lets make the queue length infinite
        # (set by -1).

        # For active files...
        scan_queue = MigrateQueue(-1, notify_every_time = False)
        scan_queue.debug = debug
        # and deleted files.
        deleted_scan_queue = MigrateQueue(-1, notify_every_time = False)
        deleted_scan_queue.debug = debug

	#For each list of bfids start the migrations.
	for i in range(len(use_bfids_lists)):
		bfid_list = use_bfids_lists[i]

                # Low water mark for the write-to-tape thread to proceed.
                # Also, get the size to use for the copy_queue.
                proceed_number, copy_queue_size = \
                                get_queue_numbers(bfid_list, intf,
                                                  volume_record=volume_record)

	        #Get new queues for each set of processes/threads.

                #Active file queue.
		copy_queue = MigrateQueue(copy_queue_size,
                                          low_watermark = proceed_number)
		copy_queue.debug = debug
                #Deleted file queue.
                deleted_copy_queue = MigrateQueue(copy_queue_size,
                                          low_watermark = proceed_number)
                deleted_copy_queue.debug = debug

		# Start the reading in parallel.
		run_in_parallel(copy_files,
		       (i, bfid_list, volume_record,
                        copy_queue, deleted_copy_queue,
			sequential_locks[i],
			sequential_locks[(i + 1) % use_proc_limit],
                        intf),
		       my_task = "COPY_TO_DISK",
		       on_exception = (handle_process_exception,
				       (copy_queue, SENTINEL)))

		# Start the writing of active files in parallel.
		run_in_parallel(write_new_files,
		       (i, copy_queue, scan_queue, intf),
		       my_task = "COPY_TO_TAPE",
		       on_exception = (handle_process_exception,
				       (scan_queue, SENTINEL)))
                # Start the writing of deleted files in parallel.
		run_in_parallel(write_new_files,
		       (i, deleted_copy_queue, deleted_scan_queue, intf, YES),
		       my_task = "COPY_TO_TAPE",
		       on_exception = (handle_process_exception,
				       (deleted_scan_queue, SENTINEL)))

		# Only the parent should get here.

        done_exit_status = wait_for_parallel()

        #If we are scanning too, start the scan in parallel.
        if intf.with_final_scan:
                if debug:
                        log("no more to copy, terminating the scan queue")

                #Since we are done, flag the condition variable.
                scan_queue.put(SENTINEL, block = True)
                deleted_scan_queue.put(SENTINEL, block = True)

                #For each list of bfids start the scanning.

                #First we need to sort the active destination locations.
                dst_scans = []
                job = scan_queue.get(block = True)
                while job:
                        dst_scans.append(job)
                        job = scan_queue.get(block = True)
                dst_scans.sort(key=lambda job: (job[3]['external_label'],
                                                job[3]['location_cookie']))
                #Second we need to sort the deleted destination locations.
                deleted_dst_scans = []
                job = deleted_scan_queue.get(block = True)
                while job:
                        deleted_dst_scans.append(job)
                        job = deleted_scan_queue.get(block = True)
                deleted_dst_scans.sort(key=lambda job: (job[3]['external_label'],
                                                        job[3]['location_cookie']))


                if not (intf.use_volume_assert or USE_VOLUME_ASSERT):
                        #Just put them into the round-robin order.
                        use_jobs_lists = []
                        use_deleted_jobs_lists = []
                        i = 0
                        while i < use_proc_limit:
                                use_jobs_lists.append([])
                                use_deleted_jobs_lists.append([])
                                i = i + 1

                        #First the active files in round-robin order.
                        i = 0
                        for job in dst_scans:
                                use_jobs_lists[i].append(job)
                                i = (i + 1) % use_proc_limit
                        #Second the deleted files in round-robin order.
                        i = 0
                        for job in deleted_dst_scans:
                                use_deleted_jobs_lists[i].append(job)
                                i = (i + 1) % use_proc_limit

                        #For each list of job tuples start the scans.
                        for i in range(len(use_jobs_lists)):
                                job_list = use_jobs_lists[i]
                                deleted_job_list = use_deleted_jobs_lists[i]

                                # For active files...
                                run_in_parallel(final_scan,
                                                (i, job_list, intf),
                                                my_task = "FINAL_SCAN")
                                # and deleted files.
                                run_in_parallel(final_scan,
                                             (i, deleted_job_list, intf, YES),
                                                my_task = "FINAL_SCAN")

	        #If using volume_assert, we only want one call to final_scan():
                else:
                    # For active files...
                    run_in_parallel(final_scan,
                                    (i, dst_scans, intf),
                                    my_task = "FINAL_SCAN")
                    # and deleted files.
                    run_in_parallel(final_scan,
                                    (i, deleted_dst_scans, intf, YES),
                                    my_task = "FINAL_SCAN")

        done_exit_status = done_exit_status + wait_for_parallel()

        #Before launching the next thread, lets cleanup the stack on
        # this side.
        del copy_queue
        del deleted_copy_queue
        del scan_queue
        del deleted_scan_queue

	errors = done_exit_status + errors
	return errors

def migrate_files(bfids, intf):
    MY_TASK = "%s_FILES" % (IN_PROGRESS_STATE.upper(),)

    # get its own fcc
    config_host = enstore_functions2.default_host()
    config_port = enstore_functions2.default_port()
    csc = configuration_client.ConfigurationClient((config_host, config_port))
    fcc = file_clerk_client.FileClient(csc)

    file_record_list = []
    for bfid in bfids:
        file_record = fcc.bfid_info(bfid)
        if e_errors.is_ok(file_record):
            file_record_list.append(file_record)
            if debug:
                log(MY_TASK, "append file record %s" % (file_record) )
        else:
            # abort on error
            error_log(MY_TASK, "can not find record of", bfid)
            return 1

    return migrate(file_record_list, intf)

# migrate_volume(vol) -- migrate a volume
def migrate_volume(vol, intf):
    #These probably should not be constants anymore, now that cloning
    # is handled differently.
    global INHIBIT_STATE, IN_PROGRESS_STATE
    global set_system_migrating_func, set_system_migrated_func

    global pid_list

    MY_TASK = "%s_VOLUME" % (IN_PROGRESS_STATE.upper(),)
    log(MY_TASK, "start", IN_PROGRESS_STATE, "volume", vol, "...")
    db = pg.DB(host=dbhost, port=dbport, dbname=dbname, user=dbuser)
    # get its own vcc
    config_host = enstore_functions2.default_host()
    config_port = enstore_functions2.default_port()
    csc = configuration_client.ConfigurationClient((config_host,
                                                    config_port))
    vcc = volume_clerk_client.VolumeClerkClient(csc)
    fcc = file_clerk_client.FileClient(csc)

    # check if vol is set to "readonly". If not, set it.
    volume_record = get_volume_info(MY_TASK, vol, vcc, db)
    if volume_record['status'][0] != e_errors.OK:
            error_log(MY_TASK, 'volume %s does not exist' % vol)
            db.close()  #Avoid resource leaks.
            return 1
    if volume_record['system_inhibit'][0] != 'none':
            error_log(MY_TASK, vol, 'is', volume_record['system_inhibit'][0])
            db.close()  #Avoid resource leaks.
            return 1

    #If volume is migrated, report that it is done and stop.

    #First, determine if all of the migration history is up-to-date.
    is_history_done = is_migration_history_done(MY_TASK, vol, db)
    if is_history_done == None:
        #Confirm that the history does not return an error case.  If it did
        # we get here.
        error_log(MY_TASK, "Unable to continue, migration history error")
        db.close()  #Avoid resource leaks.
        return 1
    #Second, determine if the system inhibit is correct.
    is_migrated_state = enstore_functions2.is_migrated_state(
        volume_record['system_inhibit'][1])
    #Third, determine if all files are copied and swapped.
    is_migrated_files =  is_migrated_by_src_vol(vol, intf, db,
                                                checked = 0, closed = 0)
    if is_migrated_state and is_migrated_files and is_history_done and \
           not getattr(intf, "force", None):
        log(MY_TASK, vol, "is already", volume_record['system_inhibit'][1])
        db.close()  #Avoid resource leaks.
        return 0

    #Make sure the library exists.
    library_fullname = volume_record['library'] + ".library_manager"
    lib_dict = csc.get(library_fullname)
    if not e_errors.is_ok(lib_dict):
        error_log("library %s does not exist" % (library_fullname,))
        db.close()  #Avoid resource leaks.
        return 1

    # Do not duplicate multiple copy tapes.  We want to duplicate just
    # the originals.
    file_family = volume_family.extract_file_family(volume_record["volume_family"])
    if re.compile(".*_copy_[1-9]*$").match(file_family) != None \
           and not intf.force:
        error_log("%s is a multiple copy volume" % (vol,))
        db.close()  #Avoid resource leaks.
        return 1

    #tape_list is a list of file records
    tape_list = get_tape_list(MY_TASK, vol, fcc, db, intf)
    # Detect empty tapes.
    if len(tape_list) == 0:
        log(MY_TASK, vol, "volume is empty")
        db.close()  #Avoid resource leaks.
        return 0

    media_types = []
    #Need to obtain the output media_type.  If --library
    # was used on the command line, go with that.  Otherwise,
    # set the media_type.
    if intf.library:
        media_type = get_media_type(intf.library, db)
        media_types = [media_type]
    else:
        """
        for row in res:
            original_path = row[2]
            media_type = search_media_type(original_path, db)

            if media_type and media_type not in media_types:
                media_types.append(media_type)
        """
        for file_record in tape_list:
            media_type = search_media_type(file_record['pnfs_name0'], db)
            if media_type and media_type not in media_types:
                media_types.append(media_type)

    #If we are certain that this is a cloning job, not a migration, then
    # we should handle it accordingly.
    if len(media_types) == 1 and media_types[0] == volume_record['media_type']:
        setup_cloning()

    #Here are some additional checks on the volume.  If necessary, it
    # will set the system_inhibit_1 value.
    if volume_record['system_inhibit'][1] not in [IN_PROGRESS_STATE, INHIBIT_STATE] \
           and \
           volume_record['system_inhibit'][1] in MIGRATION_STATES + MIGRATED_STATES:
        #If the system inhibit has already been set to another type
        # of migration, don't continue.
        log(MY_TASK, vol, 'has already been set to %s while trying to set it to %s' \
            % (volume_record['system_inhibit'][1], IN_PROGRESS_STATE))
        db.close()  #Avoid resource leaks.
        return 1
    if volume_record['system_inhibit'][1] == INHIBIT_STATE and \
           is_migrated_by_src_vol(vol, intf, db) and \
           not getattr(intf, "force", None):
        log(MY_TASK, vol, 'has already been %s' % INHIBIT_STATE)
        db.close()  #Avoid resource leaks.
        return 0
    if volume_record['system_inhibit'][1] != IN_PROGRESS_STATE:
        set_system_migrating_func(vcc, vol)
        log(MY_TASK, 'set %s to %s' % (vol, IN_PROGRESS_STATE))

    #Migrate the files in the list.
    res = migrate(tape_list, intf, volume_record=volume_record)

    #Do one last volume wide check.
    if res == 0 and is_migrated_by_src_vol(vol, intf, db, checked = 0, closed = 0):
        set_src_volume_migrated(MY_TASK, vol, vcc, db)

    else:
        message = "do not set %s to %s due to previous error" % \
                  (vol, INHIBIT_STATE)
        error_log(MY_TASK, message)

    db.close()  #Avoid resource leaks.

    return res

def set_src_volume_migrated(MY_TASK, vol, vcc, db):
	# mark the volume as migrated
	## Note: Don't use modify() here.  Doing so would prevent the
	## plotting and summary scripts from working correctly.  They look
	## for a state change to system_inhibit_1; not a modify.
	ticket = set_system_migrated_func(vcc, vol)
	if ticket['status'][0] == e_errors.OK:
		log(MY_TASK, "set %s to %s"%(vol, INHIBIT_STATE))
	else:
		error_log(MY_TASK, "failed to set %s %s: %s" \
			  % (vol, ticket['status'], INHIBIT_STATE))
	# set comment
	to_list = migrated_to(vol, db)
	if to_list == []:
		to_list = ['none']
	vol_list = ""
	for i in to_list:
		# log history
		rtn_value = log_history(vol, i, vcc, db)
                if rtn_value:
                    #log_history gives its own error.
                    return 1
		# build comment
		vol_list = vol_list + ' ' + i
	if vol_list:
		res = vcc.set_comment(vol, MTO+vol_list)
		if res['status'][0] == e_errors.OK:
			ok_log(MY_TASK, 'set comment of %s to "%s%s"' \
			       % (vol, MTO, vol_list))
		else:
			error_log(MY_TASK, 'failed to set comment of %s to "%s%s"' \
				  % (vol, MTO, vol_list))
			return 1
	return 0

##########################################################################

#Can be used for threads too.  It simply sends the value in write_value
# which should be a SENTINEL value to the queue or queues.
def handle_process_exception(queue, write_value):

    if type(queue) == types.ListType:
        queue_list = queue
    else:
        queue_list = [queue]

    for current_queue in queue_list:
        try:
            current_queue.put(write_value)
        except (OSError, IOError), msg:
            sys.stderr.write("Error sending abort sentinel to queue: %s\n" \
                             % (str(msg),))

##########################################################################

# migrated_from(vol, db) -- list all volumes that have migrated to vol
def migrated_from(vol, db):
	q = "select distinct va.label \
		from volume va, volume vb, file fa, file fb, migration \
		 where fa.volume = va.id and fb.volume = vb.id \
			and fa.bfid = migration.src_bfid \
			and fb.bfid = migration.dst_bfid \
			and vb.label = '%s' order by va.label;"%(vol)
	res = db.query(q).getresult()
	from_list = []
        from_del_list = []
	for i in res:
                #Sort the recycle (.deleted) volumes to the end of the
                # returned list.  This is necessary when the list is used
                # for setting the migration_history table closed column.
                # The .deleted may not always succeed, but this way
                # non-deleted tapes can be finished.
                if i[0][-8:] == ".deleted":
                        from_del_list.append(i[0])
                else:
		        from_list.append(i[0])

	return from_list + from_del_list

# migrated_to(vol, db) -- list all volumes that vol has migrated to
def migrated_to(vol, db):
	q = "select distinct vb.label \
		from volume va, volume vb, file fa, file fb, migration \
		 where fa.volume = va.id and fb.volume = vb.id \
			and fa.bfid = migration.src_bfid \
			and fb.bfid = migration.dst_bfid \
			and va.label = '%s' order by vb.label;"%(vol)
	res = db.query(q).getresult()
	to_list = []
	for i in res:
		to_list.append(i[0])

	return to_list

#for copied, swapped, checked and closed, if they are true, that part of
# the migration table is checked.
def is_migrated_by_src_vol(vol, intf, db, copied = 1, swapped = 1, checked = 1,
			   closed = 1):

	return is_migrated(vol, None, intf, db,
			   copied = copied, swapped = swapped,
			   checked = checked, closed = closed)

#for copied, swapped, checked and closed, if they are true, that part of
# the migration table is checked.
def is_migrated_by_dst_vol(vol, intf, db, copied = 1, swapped = 1, checked = 1,
			   closed = 1):

	return is_migrated(None, vol, intf, db,
			   copied = copied, swapped = swapped,
			   checked = checked, closed = closed)

#Only one of src_vol or dst_vol should be specifed, the other should be
# set to None.
def is_migrated(src_vol, dst_vol, intf, db, copied = 1, swapped = 1, checked = 1, closed = 1):
	check_copied = ""
	check_swapped = ""
	check_checked = ""
	check_closed = ""
	if copied:
		check_copied = " or migration.copied is NULL "
	if swapped:
		check_swapped = " or migration.swapped is NULL "
	if checked:
		check_checked = " or migration.checked is NULL "
	if closed:
		check_closed = " or migration.closed is NULL "

	if src_vol and not dst_vol:
		check_label = "volume.label = '%s'" % (src_vol,)
	elif dst_vol and not src_vol:
		check_label = "v2.label = '%s'" % (dst_vol,)
	elif not dst_vol and not src_vol:
		return False #should never happen
	else: # dst_vol and src_vol:
		return False #should never happen

	#Consider the deleted status the files.  All cases should ignore,
	# unknown files from transfer failures.
	if intf.with_deleted and dst_vol:
		deleted_files = " (f2.deleted = 'n' or f2.deleted = 'y') "
	elif not intf.with_deleted and dst_vol:
		deleted_files = " (f2.deleted = 'n') "
	elif intf.with_deleted and src_vol:
		deleted_files = " (file.deleted = 'n' or file.deleted = 'y') "
	elif not intf.with_deleted and src_vol:
		deleted_files = " (file.deleted = 'n') "

	if dst_vol:
		bad_files1 = ""
		bad_files2 = ""
	elif intf.skip_bad and src_vol:
		bad_files1 = "left join bad_file on bad_file.bfid = file.bfid"
		bad_files2 = " and bad_file.bfid is NULL "
	elif not intf.skip_bad and src_vol:
		bad_files1 = ""
		bad_files2 = ""

    # This query skips failed files
	q1 = "select file.bfid,volume.label,f2.bfid,v2.label," \
	     "migration.copied,migration.swapped,migration.checked,migration.closed " \
	     "from file " \
	     "left join volume on file.volume = volume.id " \
	     "left join migration on file.bfid = migration.src_bfid " \
	     "left join file f2 on f2.bfid = migration.dst_bfid " \
	     "left join volume v2 on f2.volume = v2.id " \
	     "%s " \
	     "where (f2.bfid is NULL %s %s %s %s ) " \
	     "      and %s and %s %s " \
	     "  and file.pnfs_id != '' "\
	     ";" % \
	     (bad_files1,
	      check_copied, check_swapped, check_checked, check_closed,
	      check_label, deleted_files, bad_files2)

	#Determine if the volume is the source volume with files to go.
	if debug:
		print q1
	res1 = db.query(q1).getresult()
	if len(res1) == 0:
		return True

	return False

#Report True if the bfid is a duplication bfid, False if not and None
# if there is some type of error.
def is_migration(bfid, db):
	q1 = "select * from migration " \
	    "where src_bfid = '%s' or dst_bfid = '%s';" % (bfid, bfid)

	q2 = "select bfid,alt_bfid from file_copies_map,migration " \
	     "where ((bfid = src_bfid and alt_bfid = dst_bfid) " \
	     "   or (bfid = dst_bfid and alt_bfid = src_bfid)) " \
	     "  and (bfid = '%s' or alt_bfid = '%s');" % (bfid, bfid)

	#Determine if the file is a duplicated file.
        if debug:
		print q1
	res1 = db.query(q1).getresult()

	#Determine if the file_copies_map (duplication only) table and the
	# migration (migration and duplication) table agree.
        if debug:
		print q2
	res2 = db.query(q2).getresult()

	if len(res1) >= 1 and len(res2) == 0:
                #We have a healthy migrated to/from file.
		return True
	elif len(res1) >= 1 and len(res2) >= 1:
		#This is a healthy duplicated to/from file.
		return False
	elif len(res1) == 0 and len(res2) >= 1:
		#How could this possiblely even happen?  It would require
		# the more restrictive query to return an answer the less
		# restrictive one did not.
		return None
	else:
		#The bfid was nowhere to be found.
		return False


#Report True if the bfid is a duplication bfid, False if not and None
# if there is some type of error.
def is_duplication(bfid, db):
	q1 = "select * from file_copies_map " \
	    "where bfid = '%s' or alt_bfid = '%s';" % (bfid, bfid)

	q2 = "select bfid,alt_bfid from file_copies_map,migration " \
	     "where ((bfid = src_bfid and alt_bfid = dst_bfid) " \
	     "   or (bfid = dst_bfid and alt_bfid = src_bfid)) " \
	     "  and (bfid = '%s' or alt_bfid = '%s');" % (bfid, bfid)

	#Determine if the file is a duplicated file.
        if debug:
		print q1
	res1 = db.query(q1).getresult()

	#Determine if the file_copies_map (duplication only) table and the
	# migration (migration and duplication) table agree.
        if debug:
		print q2
	res2 = db.query(q2).getresult()

	if len(res1) >= 1 and len(res2) == 0:
		#This is a bad situation.  The migration table has the
		# bfid going to/from a different bfid than the duplication
		# table.
		return None
	elif len(res1) >= 1 and len(res2) >= 1:
		#This is a healthy duplicated to/from file.
		return True
	elif len(res1) == 0 and len(res2) >= 1:
		#How could this possiblely even happen?  It would require
		# the more restrictive query to return an answer the less
		# restrictive one did not.
		return None
	else:
		#The bfid was nowhere to be found.
		return False

#Duplication overrides this to is_duplication.
is_expected_restore_type = is_migration

##########################################################################

#Note: fcc used only for duplicate.py version of this function.
def is_expected_volume_migration(MY_TASK, vol, likely_path, fcc, db):
	__pychecker__ = "unusednames=fcc"

	# make sure the volume is the same
	pf = chimera.File(likely_path)
	pf_volume = getattr(pf, "volume", None)
        if pf_volume == None:
                error_log(MY_TASK,
			  'wrong volume %s (expecting %s)' \
			  % (pf_volume, vol))
		return False  #Match not found.

        elif pf_volume != vol:
                #We don't have a match between pnfs layer 4 and the volume
                # given by the user to scan.  Double check if the volume
                # being requested for a scan is a multiple copy volume.
                mc_bfids = get_multiple_copy_bfids(pf.bfid, db)
                for mc_bfid in mc_bfids:
#                        mc_vol = get_volume_from_bfid(mc_bfid, db)
                        mc_reply = get_file_info(MY_TASK, mc_bfid, fcc, db)
                        if mc_reply == None:
                                error_log(MY_TASK,
                                          'file info for %s not found' \
                                          % (mc_bfid,))
                                return False
                        if vol == mc_reply['external_label']:
                                #The tape given by the user is a multiple
                                # copy tape.  Stop searching.
                                break
                else:
                        error_log(MY_TASK,
                                  'wrong volume %s (expecting %s)' \
                                  % (pf_volume, vol))
                        return False  #Match not found.

	return True  #Match found.

#Duplication may override this.
is_expected_volume = is_expected_volume_migration


##########################################################################

#restore_file(file_record):
#  Restore the source file record's file to be the primary file users access.
#  This works for both migration and duplication.
def restore_file(src_file_record, vcc, fcc, db, intf, src_volume_record=None):
    MY_TASK = "RESTORE"

    __pychecker__ = "unusednames=intf" #Remove when intf is used.

    ###########################################################
    # Verify the metadata.
    ###########################################################

    src_bfid = src_file_record['bfid']  #shortcut

    if src_volume_record == None or type(src_volume_record) != types.DictType:
        #Obtain volume information.
        src_volume_record = get_volume_info(MY_TASK,
                                        src_file_record['external_label'],
                                        vcc, db)
        if not e_errors.is_ok(src_volume_record):
            error_log(MY_TASK, src_volume_record['status'])
            sys.exit(1)

    #Determine if the file has been copied to a new tape already.  Need to
    # worry if the file has been migrated to multiple copies.
    is_it_copied_list = is_copied(src_bfid, fcc, db, all_copies=True)
    if len(is_it_copied_list) > 0:
        is_it_copied = is_it_copied_list[0]
    else:
        is_it_copied = None
    dst_bfid = is_it_copied #side effect of is_copied()
    if dst_bfid == None:
        check_src_bfid, check_dst_bfid = get_bfids(src_bfid, fcc, db)
        if check_dst_bfid == src_bfid:
            error_log("bfid %s is a destination bfid not"
                      " a source bfid" % (src_bfid,))
            sys.exit(1)
        if (check_src_bfid, check_dst_bfid) == (None, None):
            error_log("bfid %s has not been %s" % (src_bfid, INHIBIT_STATE))
            sys.exit(1)

    if dst_bfid:
        dst_file_record = get_file_info(MY_TASK, dst_bfid, fcc, db)
        if not e_errors.is_ok(dst_file_record):
            error_log(MY_TASK, dst_file_record['status'])
            sys.exit(1)

        dst_volume_record = get_volume_info(MY_TASK, dst_file_record['external_label'],
                                            vcc, db)
        if not e_errors.is_ok(dst_volume_record):
            error_log(MY_TASK, dst_volume_record['status'])
            sys.exit(1)
    else:
        dst_file_record = None
        dst_volume_record = None

    is_it_swapped = is_swapped(src_bfid, fcc, db)

    #Determine the search order of the bfids.  This is important, because
    # the defaults for migration and duplication are opposites and picking
    # the wrong order slows things down.
    active_bfid, nonactive_bfid, unused, unused = search_order(
        src_bfid, src_file_record, dst_bfid, dst_file_record,
        is_it_copied, is_it_swapped, fcc, db)

    #The restoring of a bfid is only allowed for migrated or
    # cloned files.  We need to fail duplicated files now.
    migration_type_answer = is_expected_restore_type(src_bfid, db)
    if migration_type_answer == None:
        #Some sort of error.
        error_log("bfid %s is not %s" % (src_bfid, INHIBIT_STATE))
        sys.exit(1)
    elif not migration_type_answer:
        error_log("bfid %s is not a %s bfid" % (src_bfid, MIGRATION_NAME.lower()))
        sys.exit(1)

    #We need to handle restoring a multiple copy.
    ob_reply = fcc.find_the_original(src_bfid)
    if e_errors.is_ok(ob_reply):
        original_bfid = ob_reply.get('original', None)

        #If this is its own original, ignore.
        if original_bfid and original_bfid == src_bfid:
            original_bfid = None
    else:
        original_bfid = None

    #Find the current location of the file.
    pairs_to_search = [(src_file_record['pnfsid'], active_bfid),
                       (src_file_record['pnfsid'], nonactive_bfid)]
    if original_bfid:
        #If we are restoring a migrated multiple_copy, add this
        # to the list of metadata to check.  Put this first.
        pairs_to_search.insert(0, (src_file_record['pnfsid'],
                                   original_bfid))
    if len(is_it_copied_list) > 1:
        #is_it_copied_list[1:] are extra migration copies that the file to be
        # restored has.  If a file has correctly been migrated to multiple
        # copies, this list should match that of copies_reply['copies'],
        # but VOB796 proved this wrong.  It was found to have metadata like
        #  CDMS117895302800000  N  CDMS126841354300000  Y       y      y
        #  CDMS117895302800000  N  CDMS126841359000000 1N       y
        # which were the result of running two migrations at the same time.
        # CDMS126841354300000 and CDMS126841359000000 are not related
        # as multiple copies of each other like the "1N" would normally
        # indicate.
        for extra_copy_bfid in is_it_copied_list[1:]:
            extra_file_record = get_file_info(MY_TASK, extra_copy_bfid,
                                              fcc, db)
            #As long as we are looping over the migration copies, we should
            # verify that they all have the same deleted status.  Otherwise,
            # we don't know whether to do an active or deleted file restore.
            if not e_errors.is_ok(extra_file_record):
                error_log(MY_TASK, extra_file_record['status'])
                sys.exit(1)
            if extra_file_record['deleted'] != dst_file_record['deleted']:
                error_log(MY_TASK, "Not all destination deleted statuses are"
                          " the same: (%s, %s) != (%s, %s)" % \
                          (dst_bfid, dst_file_record['deleted'],
                           extra_file_record['bfid'],
                           extra_file_record['deleted']))
                sys.exit(1)

            #If we are restoring a file with extra migration copies, add
            # this to the list of metadata to check.
            pairs_to_search.append((src_file_record['pnfsid'],extra_copy_bfid))

    for search_pnfsid, search_bfid in pairs_to_search:
        try:
            src = find_pnfs_file.find_chimeraid_path(
                search_pnfsid, search_bfid,
                path_type = enstore_constants.FS)
        except (KeyboardInterrupt, SystemExit):
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except OSError, msg:
            if getattr(msg, "errno", msg.args[0]) == errno.EEXIST and \
                msg.args[1].find("pnfs entry exists") != -1:
                #This "if" clause, was found to be needed to undue a migration
                # to multiple copies.  The original situation was a tape
                # that had two migrations running at the same time, but was
                # found to be similar in details.
                src = getattr(msg, "filename", None)
                if src:
                    break
            continue
        except:
            exc_type, exc_value, exc_tb = sys.exc_info()
            Trace.handle_error(exc_type, exc_value, exc_tb)
            del exc_tb #avoid resource leaks
            error_log(MY_TASK, str(exc_type),
                      str(exc_value),
                      "%s %s %s %s is not a valid pnfs file" \
                      % (src_file_record['external_label'],
                         src_file_record['bfid'],
                         src_file_record['location_cookie'],
                         src_file_record['pnfsid']))
            sys.exit(1)
        break
    else:
        if dst_file_record['deleted'] in (NO,):
            #Neither file matched.
            message = "Neither %s nor %s correlate to %s." \
                      % (active_bfid, nonactive_bfid,
                         src_file_record['pnfsid'])
            error_log(MY_TASK, message)
            sys.exit(1)

        #Just set this to something.
        src = src_file_record['pnfs_name0']

    #This would be used if get_path() was used, since it only
    # matches for pnfsid.  find_pnfs_file.find_chimeraid_path()
    # also checks to make sure the layer 1 bfid information
    # matches too; which should remove all duplicates.  The only
    # possible duplicates would be things like the same pnfs
    # filesystem mounted one machine in different locations; and
    # in this case taking the first one is fine.
    if type(src) == type([]):
        src = src[0]

    f_rec = src_file_record
    v_rec = src_volume_record

    p = chimera.File(src)
    p.bfid = src_bfid

    # restoring a multiple copy file
    if original_bfid:
        # override p with the primary copy information
        p.bfid = original_bfid

        # obtain original file information
        f_original = fcc.bfid_info(original_bfid)
        if not e_errors.is_ok(f_original):
            error_log(MY_TASK, f_original['status'])
            sys.exit(1)
        f_rec = f_original

        # obtain original volume information
        v_original = vcc.inquire_vol(f_original['external_label'])
        if not e_errors.is_ok(v_original):
            error_log(MY_TASK, v_original['status'])
            sys.exit(1)
        v_rec = v_original

    p.volume = f_rec['external_label']
    p.location_cookie = f_rec['location_cookie']
    p.drive = f_rec['drive']
    p.complete_crc = f_rec['complete_crc']
    p.file_family = volume_family.extract_file_family(v_rec['volume_family'])
    p.size = f_rec['size']

    if debug:
        p.show()

    #Knowing the path in pnfs, we can determine the special
    # temporary migration path in PNFS.
    if dst_volume_record['volume_family'].find("DELETED_FILES") != -1:
        #If the destination file_family part of the volume family informs
        # us that this is a deleted file, we need to handle it differently.
        mig_path = migration_path(src, src_file_record, deleted=YES)
    else:
        mig_path = migration_path(src, src_file_record)

    #If the destination copy has multiple copies, we need to
    # be able to clear them.
    if dst_bfid != None:
        copies_reply = fcc.find_copies(dst_bfid)
        if not e_errors.is_ok(copies_reply):
            error_log(MY_TASK,"failed to retrieve multiple copies list %s %s: %s"
                      % (dst_bfid, mig_path, str(copies_reply['status'])))
            return 1
    else:
        copies_reply = {}
        copies_reply['copies'] = []

    if dst_file_record and \
          dst_file_record['deleted'] in (YES,) and \
          src_file_record['deleted'] in (YES,):
        #If we are restoring a deleted file, we don't have much to do.
        deleted_restore = True
    elif dst_file_record and \
             dst_file_record['deleted'] in (NO,) and \
             src_file_record['deleted'] in (YES, NO):
        #We are restoring a normal-active file.
        deleted_restore = False
    elif dst_file_record and \
             dst_file_record['deleted'] in (YES,) and \
             src_file_record['deleted'] in (NO,) and \
             not is_closed(dst_bfid, fcc, db):
        #Found a non-deleted file that was migrated, the new
        # copy was deleted before it was scanned.  We don't know if
        # the original should be restored as active or if the
        # deleted status of the destination should be honored.
        error_log(MY_TASK, "found deleted and unscanned destination file "
                  "with active source file "
                  "(source %s %s)  (destination %s %s)"
                  % (src_bfid, src_file_record['deleted'],
                     dst_bfid, dst_file_record['deleted']))
        return 1
    elif dst_file_record:
        #The combination of deleted statuses for the source and
        # destination is not expected.
        error_log(MY_TASK, "comibination of deleted status is wrong: "
                  "(%s %s)  (%s %s)"
                  % (src_bfid, src_file_record['deleted'],
                     dst_bfid, dst_file_record['deleted']))
        return 1
    else:
        #How did we get here without any destination file record?
        message = "impossible situation: found destination bfid, " \
                      "but no destination file record"
        error_log(MY_TASK, message)
        sys.exit(1)

    ###########################################################
    # Make the metadata changes.
    ###########################################################

    if original_bfid:
        #Have the restored multiple copy match the deleted
        # status of the original.

        if f_original['deleted'] == "no":
            rtn_code = mark_undeleted(MY_TASK, src_bfid, fcc, db)
            if rtn_code:
                error_log(MY_TASK,"failed to undelete source file %s %s"
                          % (src_bfid, src,))
                return 1
        elif f_original['deleted'] == "yes":
            rtn_code = mark_deleted(MY_TASK, src_bfid, fcc, db)
            if rtn_code:
                error_log(MY_TASK,"failed to delete source file %s %s"
                          % (src_bfid, src,))
                return 1
    elif not deleted_restore:
        #We need to do the right thing if the file is deleted or not.

        #Undelete the source file; this is done for normal active files.
        rtn_code = mark_undeleted(MY_TASK, src_bfid, fcc, db)
        if rtn_code:
            error_log(MY_TASK,"failed to undelete source file %s %s"
                      % (src_bfid, src,))
            return 1

    #Remove the temporary migration path in PNFS if it still exists.
    if os.path.exists(mig_path):
        try:
            make_writeable(mig_path)
        except (OSError, IOError), msg:
            message = "unable to make writeable file"
            error_log(MY_TASK,"%s %s: %s" % (message, mig_path, str(msg)))
            return 1

        try:
            nullify_pnfs(mig_path)
        except (OSError, IOError), msg:
            message = "failed to clear layers for file"
            error_log(MY_TASK,"%s %s as (uid %s, gid %s): %s"
                      % (message, mig_path, os.geteuid(), os.getegid(), str(msg)))
            return 1

        log(MY_TASK, "removing %s" % (mig_path,))
        try:
            file_utils.remove(mig_path)
        except (OSError, IOError), msg:
            # if temp file does not exist, pass
            if msg.args[0] != errno.ENOENT:
                message = "failed to delete migration file"
                error_log(MY_TASK,"%s %s as (uid %s, gid %s): %s"
                          % (message, mig_path, os.geteuid(), os.getegid(), str(msg)))
                return 1

    ### Update layers 1 and 4:

    #Don't update if the original copy of the multiple
    # copy being restored is deleted (or unknown).

    if (original_bfid and f_original['deleted'] != "no"):
        pass
    # When restoring a deleted file, there isn't an filesystem
    # metadata to update.
    elif deleted_restore:
        pass
    #For some failures, the swap never truly happens.  If this
    # is the case skip the pnfs layer update.
    elif not is_migration_path(src):
        # set layer 1 and layer 4 to point to the original file
        try:
            update_layers(p)
        except (IOError, OSError, ValueError), msg:
            #A ValueError can happen when
            # pnfs.File.consistent() finds a problem.
            message = "failed to restore layers 1 and 4 for"
            error_log(MY_TASK,"%s %s %s: %s" % (message, src_bfid, src, str(msg)))
            return 1

        # check if the restored file is a package, and switch package parent for all files in it
        # "src" is not valid anymore as it points to the location of the package before it has been moved.
        rc_rp = restore_package(dst_file_record, dst_bfid, mig_path,
                                src_file_record, src_bfid, src, p, fcc)

    #Undo the migration for the destination file and any multiple copies that it has.
    # The is_it_copied_list[1:] are the bfids of any extra migration copies.
    for cur_dst_bfid in [dst_bfid] + copies_reply['copies'] + is_it_copied_list[1:]:
        #cur_dst_bfid will be None when restoring a multiple copy file.
        #It got done when the original was restored.
        if not cur_dst_bfid:
            return 1

        # mark the migration copy (and any multiple copies)
        # of the file deleted
        rtn_code = mark_deleted(MY_TASK, cur_dst_bfid, fcc, db)
        if rtn_code:
            error_log(MY_TASK,"failed to mark deleted migration file %s %s" \
                      % (dst_bfid, mig_path,))
            return 1

        # Modify migration table:
        # On error an exception should be raised preventing
        # us from continuing further.
        # ... remove the swapped timestamp
        log_unswapped(src_bfid, cur_dst_bfid, fcc, db)
        # ... remove the copied timestamp, source and destination bfid pair
        log_uncopied(src_bfid, cur_dst_bfid, fcc, db)

    return 0


def restore_package(dst_file_record, dst_bfid, dst_path,
                    src_file_record, src_bfid, src_path, cf, fcc):
    """
    restore packaged files metadata in FC file table

    helper function to restore metadata for files in package associated with "dst"
    to the package referred by "src."
    Effectively, this method calls  File Clerk to "swap" package for small files.
    Note, we keep same notation as elsewhere in the code, and in may seem unusial:
        we swap files from destination "dst" to the source "src" as we restore metadata,
        where "src" is source file for migration and "dst" is the destination file for migration.
    cf - chimera.File for original path
    """

    MY_TASK = "RESTORE_PKGD_FILES"

    # check if file to restore is a package
    dst_package_id = dst_file_record.get("package_id", None)
    dst_package_files_count = dst_file_record.get("package_files_count", 0)
    dst_is_a_package = (dst_package_id is not None) and (dst_bfid == dst_package_id)
    # SFA:
    #  we check the number of files in package and switch the packaged files dst_package_id if needed
    #  to process the case when package file has been restored but packaged files have not been switched yet
    # Do nothing if it is not package or empty package
    if not (dst_is_a_package and dst_package_files_count > 0):
        return None

    # switch parents for all small files in the migration destination file (package)
    # to migration-source file (package)
    err_switch_package = _switch_package(dst_bfid, src_bfid, fcc)
    if debug:
        log(MY_TASK, "from dst_bfid=%s dst_package_id=%s to src_bfid=%s err_switch_package=%s"
            % (dst_bfid,dst_package_id,src_bfid,err_switch_package))
    if err_switch_package:
        # we do not roll back swapped metadata for original files
        #   when switching parent for the packaged file has failed
        error_log(MY_TASK, "%s %s %s %s failed when switching package for packaged files during package restore, error %s"
                  % (dst_bfid, dst_path, src_bfid, src_path, err_switch_package))
        return err_switch_package

    ok_log(MY_TASK, "files in the package %s %s have been swapped to package %s %s"
           % (dst_bfid, dst_path, src_bfid, src_path))

    # move package file in pnfs in directory having correct Volume name
    volume = src_file_record['external_label']
    err_msg, new_name = _move_package_file(dst_path,volume,cf)

    return err_msg

# restore_files(bfids) -- restore pnfs entries using file records
def restore_files(bfids, intf, src_volume_record=None):
    global errors

    __pychecker__ = "unusednames=intf" #Remove when intf is used.

    MY_TASK = "RESTORE"
    db = pg.DB(host=dbhost, port=dbport, dbname=dbname, user=dbuser)
    # get its own file clerk client and volume clerk client
    config_host = enstore_functions2.default_host()
    config_port = enstore_functions2.default_port()
    csc = configuration_client.ConfigurationClient((config_host,
                                                    config_port))
    fcc = file_clerk_client.FileClient(csc)
    vcc = volume_clerk_client.VolumeClerkClient(csc)
    if type(bfids) != type([]):
        bfids = [bfids]
    for bfid in bfids:
        #Obtain file information.
        src_file_record = get_file_info(MY_TASK, bfid, fcc, db)
        if not e_errors.is_ok(src_file_record):
            error_log(MY_TASK, src_file_record['status'])
            sys.exit(1)

        restore_file(src_file_record, vcc, fcc, db, intf, src_volume_record)

    db.close()  #Avoid resource leaks.

    return errors  #global value

# restore_volume(vol) -- restore all migrated files on original volume
def restore_volume(vol, intf):
    global errors

    MY_TASK = "RESTORE_VOLUME"
    log(MY_TASK, "restoring", vol, "...")

    # get a db connection
    db = pg.DB(host=dbhost, port=dbport, dbname=dbname, user=dbuser)

    # get its own volume clerk client
    config_host = enstore_functions2.default_host()
    config_port = enstore_functions2.default_port()
    csc = configuration_client.ConfigurationClient((config_host,
                                                    config_port))
    vcc = volume_clerk_client.VolumeClerkClient(csc)

    #Get the current data and make sure the tape is available.
    src_volume_record = get_volume_info(MY_TASK, vol, vcc, db)
    if not e_errors.is_ok(src_volume_record):
        error_log(MY_TASK, "error obtaining %s volume record: %s" % \
                  (vol, src_volume_record['status']))
        db.close()  #Avoid resource leaks.
        sys.exit(1)
    if src_volume_record['system_inhibit'][0] in ("NOACCESS", "NOTALLOWED"):
        error_log(MY_TASK, "volume %s is %s" % \
                  (vol, src_volume_record['system_inhibit'][0]))
        db.close()  #Avoid resource leaks.
        sys.exit(1)
    if src_volume_record['user_inhibit'][0] in ("NOACCESS", "NOTALLOWED"):
        error_log(MY_TASK, "volume %s is %s" % \
                  (vol, src_volume_record['user_inhibit'][0]))
        db.close()  #Avoid resource leaks.
        sys.exit(1)

    #Get the list of files to restore.
    # (To do: make function; through db directly or through clerk)
    q = "select bfid from file, volume, migration where \
            file.volume = volume.id and label = '%s' and \
            bfid = src_bfid order by location_cookie;" % (vol,)
    res = db.query(q).getresult()
    bfids = []
    for i in res:
        if i[0] not in bfids:
            #Remove duplicates from multiple copies.
            bfids.append(i[0])

    #Restore the files.
    restore_files(bfids, intf, src_volume_record)

    #Clear the system inhibit and comment for this volume.
    if not errors:
        v = src_volume_record  #shortcut

        #Set the new inhibit state and the comment.
        # (To do: make function; through db directly or through clerk)
        if enstore_functions2.is_readonly_state(v['system_inhibit'][1]):
            system_inhibit = [v['system_inhibit'][0], "none"]
        else:
            system_inhibit = v['system_inhibit']
        comment = "volume restored after %s" % \
                  (MIGRATION_NAME.lower(),)
        res1 = vcc.modify({'external_label':vol, 'comment':comment,
                           'system_inhibit':system_inhibit})
        if not e_errors.is_ok(res1):
            error_log(MY_TASK, "failed to update volume %s" % (vol,))
        else:
            ok_log(MY_TASK, "updated %s metadata" % (vol,))


        #Update the last access time for the volume, so that the
        # inventory knows to re-inventory this volume instead of
        # using the incorrect/obsolete cached information.
        res2 = vcc.touch(vol)
        if not e_errors.is_ok(res2):
            error_log(MY_TASK,
                      "failed to last access time update %s" % (vol,))

    db.close()  #Avoid resource leaks.

    return errors #global value

##########################################################################

class MigrateInterface(option.Interface):
    def __init__(self, args=sys.argv, user_mode=0):
        self.priority = 0
        self.spool_dir = None
        self.library = None
        self.file_family = None
        self.with_deleted = None
        self.with_final_scan = None
        self.status = None
        self.show = None
        self.restore = None
        self.scan_volumes = 0
        self.migrated_from = None
        self.migrated_to = None
        self.skip_bad = None
        self.read_to_end_of_tape = None
        self.force = None
        self.use_disk_files = None
        self.proceed_number = None
        self.destination_only = None
        self.source_only = None
        self.use_volume_assert = None
        self.debug = None
        self.debug_level = 0
        self.proc_limit_is_set = None
        self.proc_limit = 0
        self.single_threaded_encp = None
        self.scan = None
        self.migration_only = None
        self.multiple_copy_only = None
        self.make_copies = None
        self.library__ = None
        self.infile = None
        self.file_family_width = 1
        self.sfa_repackage = 1

        self.do_print = []
        self.dont_print = []
        self.do_log = []
        self.dont_log = []
        self.do_alarm = []
        self.dont_alarm = []

        option.Interface.__init__(self, args=args, user_mode=user_mode)

    def valid_dictionaries(self):
#        return (self.help_options, self.migrate_options)
        return (self.help_options, self.migrate_options, self.trace_options)

    #  define our specific parameters
    parameters = [
		"[bfid1 [bfid2 [bfid3 ...]]] | [vol1 [vol2 [vol3 ...]]] | [file1 [file2 [file3 ...]]] | [vol1:lc1 [vol2:lc2 [vol3:lc3 ...]]]",
                "[media_type [library [storage_group [file_family [file_family_width] [wrapper]]]]]]",
		"--restore [bfid1 [bfid2 [bfid3 ...]] | [vol1 [vol2 [vol3 ...]]] | [file1 [file2 [file3 ...]]] | [vol1:lc1 [vol2:lc2 [vol3:lc3 ...]]]",
		"--scan [bfid1 [bfid2 [bfid3 ...]] | [vol1 [vol2 [vol3 ...]]] | [file1 [file2 [file3 ...]]] | [vol1:lc1 [vol2:lc2 [vol3:lc3 ...]]]",
		"--migrated-from <vol1 [vol2 [vol3 ...]]>",  #volumes only
		"--migrated-to <vol1 [vol2 [vol3 ...]]>",  #volumes only
		"--status [bfid1 [bfid2 [bfid3 ...]]] | [vol1 [vol2 [vol3 ...]]] | [file1 [file2 [file3 ...]]] | [vol1:lc1 [vol2:lc2 [vol3:lc3 ...]]]",
		"--show <media_type> [library [storage_group [file_family [wrapper]]]]]",
		]

    migrate_options = {
        option.DEBUG:{option.HELP_STRING:
                "Output extra debugging information",
                 option.VALUE_USAGE:option.IGNORED,
			     option.VALUE_TYPE:option.INTEGER,
			     option.USER_LEVEL:option.HIDDEN,
                 option.DEFAULT_VALUE:1,
                option.VALUE_NAME:'debug_level',
                option.VALUE_TYPE:option.INTEGER,
                option.VALUE_USAGE:option.OPTIONAL,
                option.FORCE_SET_DEFAULT:option.FORCE,
                option.VALUE_LABEL:"debug_level",
                 },
		option.DESTINATION_ONLY:{option.HELP_STRING:
					 "Used with --status to only list "
					 "output assuming the volume is a "
					 "destination volume.",
					 option.VALUE_USAGE:option.IGNORED,
					 option.VALUE_TYPE:option.INTEGER,
					 option.USER_LEVEL:option.USER,},
		option.FILE_FAMILY:{option.HELP_STRING:
				    "Specify an alternative file family to "
				    "override the pnfs file family tag.",
				    option.VALUE_USAGE:option.REQUIRED,
				    option.VALUE_TYPE:option.STRING,
				    option.USER_LEVEL:option.USER,},
		option.FILE_FAMILY_WIDTH:{option.HELP_STRING:
				    "Specify an alternative file family width to "
				    "override the pnfs file family width.",
				    option.VALUE_USAGE:option.REQUIRED,
				    option.VALUE_TYPE:option.INTEGER,
				    option.USER_LEVEL:option.USER,},
		option.FORCE:{option.HELP_STRING:
			      "Allow migration on already migrated volume.",
			      option.VALUE_USAGE:option.IGNORED,
			      option.VALUE_TYPE:option.INTEGER,
			      option.USER_LEVEL:option.HIDDEN},
                option.INFILE:{option.HELP_STRING:
			       "Read target list of bfids, volumes, "
                               "volume:location_cookie pairs or paths "
                               "from file.  Types can be intermixed.",
			      option.VALUE_USAGE:option.REQUIRED,
			      option.VALUE_TYPE:option.STRING,
			      option.USER_LEVEL:option.USER},
		option.LIBRARY:{option.HELP_STRING:
				"Specify an alternative library to override "
				"the pnfs library tag.",
				option.VALUE_USAGE:option.REQUIRED,
				option.VALUE_TYPE:option.STRING,
				option.VALUE_NAME:"library",
				option.USER_LEVEL:option.ADMIN,},
		option.MIGRATED_FROM:{option.HELP_STRING:
				      "Report the volumes that were copied"
				      " to this volume.",
				       option.VALUE_USAGE:option.IGNORED,
				       option.VALUE_TYPE:option.INTEGER,
				       option.USER_LEVEL:option.ADMIN,},
		option.MIGRATED_TO:{option.HELP_STRING:
				    "Report the volumes that were copied"
				    " from this volume.",
				    option.VALUE_USAGE:option.IGNORED,
				    option.VALUE_TYPE:option.INTEGER,
				    option.USER_LEVEL:option.ADMIN,},
                option.MIGRATION_ONLY:{option.HELP_STRING:
                                       "Used with --status to only list "
                                       "output assuming the target is not or"
                                       "has not a multiple copy.",
                                       option.VALUE_USAGE:option.IGNORED,
                                       option.VALUE_TYPE:option.INTEGER,
                                       option.USER_LEVEL:option.USER,},
                option.MULTIPLE_COPY_ONLY:{option.HELP_STRING:
                                           "Used with --status to only list "
                                           "output assuming the target is or"
                                           "has a multiple copy.",
                                           option.VALUE_USAGE:option.IGNORED,
                                           option.VALUE_TYPE:option.INTEGER,
                                           option.USER_LEVEL:option.USER,},
		option.PRIORITY:{option.HELP_STRING:
				 "Sets the initial job priority."
				 "  Only knowledgeable users should set this.",
				 option.VALUE_USAGE:option.REQUIRED,
				 option.VALUE_TYPE:option.INTEGER,
				 option.USER_LEVEL:option.USER,},
		option.PROCEED_NUMBER:{option.HELP_STRING:
			      "The number of files to wait before writing.",
			      option.VALUE_USAGE:option.REQUIRED,
			      option.VALUE_TYPE:option.INTEGER,
			      option.USER_LEVEL:option.HIDDEN},
		option.READ_TO_END_OF_TAPE:{option.HELP_STRING:
				 "Read to end of tape before starting "
				 "to write.",
				 option.VALUE_USAGE:option.IGNORED,
				 option.VALUE_TYPE:option.INTEGER,
				 option.USER_LEVEL:option.USER,},
		option.RESTORE:{option.HELP_STRING:
				 "Restores the original file or volume.",
				 option.VALUE_USAGE:option.IGNORED,
				 option.VALUE_TYPE:option.INTEGER,
				 option.USER_LEVEL:option.USER,},
                option.SCAN:{option.HELP_STRING:
				 "Scan completed volumes or individual bfids.",
				 option.VALUE_USAGE:option.IGNORED,
				 option.VALUE_TYPE:option.INTEGER,
				 option.USER_LEVEL:option.USER,},
		option.SCAN_VOLUMES:{option.HELP_STRING:
				 "Scan completed volumes.",
				 option.VALUE_USAGE:option.IGNORED,
				 option.VALUE_TYPE:option.INTEGER,
				 option.USER_LEVEL:option.HIDDEN,},
		option.SKIP_BAD:{option.HELP_STRING:
				 "Skip bad files.",
				 option.VALUE_USAGE:option.IGNORED,
				 option.VALUE_TYPE:option.INTEGER,
				 option.USER_LEVEL:option.USER,},
		option.SOURCE_ONLY:{option.HELP_STRING:
				    "Used with --status to only list "
				    "output assuming the volume is a "
				    "source volume.",
				    option.VALUE_USAGE:option.IGNORED,
				    option.VALUE_TYPE:option.INTEGER,
				    option.USER_LEVEL:option.USER,},
		option.SPOOL_DIR:{option.HELP_STRING:
				  "Specify the directory to use on disk.",
				  option.VALUE_USAGE:option.REQUIRED,
				  option.VALUE_TYPE:option.STRING,
				  option.USER_LEVEL:option.USER,},
		option.SHOW:{option.HELP_STRING:
			       "Report on the completion of volumes.",
				 option.VALUE_USAGE:option.IGNORED,
				 option.VALUE_TYPE:option.INTEGER,
				 option.USER_LEVEL:option.USER,
			     option.EXTRA_VALUES:[
					 {option.VALUE_NAME:"media_type",
					  option.VALUE_TYPE:option.STRING,
					  option.VALUE_USAGE:option.REQUIRED,},
                                         {option.VALUE_NAME:"library",
                                          option.VALUE_TYPE:option.STRING,
                                          option.VALUE_USAGE:option.OPTIONAL,},
                                         {option.VALUE_NAME:"storage_group",
                                          option.VALUE_TYPE:option.STRING,
                                          option.VALUE_USAGE:option.OPTIONAL,},
                                         {option.VALUE_NAME:"file_family",
                                          option.VALUE_TYPE:option.STRING,
                                          option.VALUE_USAGE:option.OPTIONAL,},
                                         {option.VALUE_NAME:"wrapper",
                                          option.VALUE_TYPE:option.STRING,
                                          option.VALUE_USAGE:option.OPTIONAL,},
						  ]},
		option.STATUS:{option.HELP_STRING:
			       "Report on the completion of a volume.\n"
                               "S = State of duplication:\n"
                               "    P = Primary/original copy; duplication\n"
                               "    C = Muliple copy; duplication\n"
                               "    O = Original/primary copy\n"
                               "    M = Multiple copy\n"
                               "D = Deleted state:\n"
                               "    N = Not deleted\n"
                               "    Y = Yes deleted\n"
                               "    U = Unknown; failed write\n"
                               "B = Bad file\n"
                               "    B = Bad file\n"
                               "    E = Empty metadata fields\n",
				 option.VALUE_USAGE:option.IGNORED,
				 option.VALUE_TYPE:option.INTEGER,
				 option.USER_LEVEL:option.USER,},
		option.USE_DISK_FILES:{option.HELP_STRING:
				       "Skip reading files on source volume, "
				       "use files already on disk.",
				       option.VALUE_USAGE:option.IGNORED,
				       option.VALUE_TYPE:option.INTEGER,
				       option.USER_LEVEL:option.ADMIN,},
        option.SINGLE_THREADED_ENCP:{option.HELP_STRING:
                       "Call encp WITHOUT threaded option ",
                       option.VALUE_USAGE:option.IGNORED,
                       option.VALUE_TYPE:option.INTEGER,
                       option.USER_LEVEL:option.ADMIN,},
       option.PROC_LIMIT:{option.HELP_STRING:
             "limit number of read and write migration workers to N max each",
              option.DEFAULT_NAME:'proc_limit_is_set',
              option.DEFAULT_VALUE:1,
              option.DEFAULT_TYPE:option.INTEGER,
              option.VALUE_NAME:'proc_limit',
              option.VALUE_TYPE:option.INTEGER,
              option.VALUE_USAGE:option.REQUIRED,
              option.VALUE_LABEL:"N",
              option.USER_LEVEL:option.USER,
              option.FORCE_SET_DEFAULT:option.FORCE,
        },
		option.USE_VOLUME_ASSERT:{option.HELP_STRING:
					  "Use volume assert when scanning "
					  "destination files.",
					  option.VALUE_USAGE:option.IGNORED,
					  option.VALUE_TYPE:option.INTEGER,
					  option.USER_LEVEL:option.ADMIN,},
		option.WITH_DELETED:{option.HELP_STRING:
				     "Include deleted files.",
				     option.VALUE_USAGE:option.IGNORED,
				     option.VALUE_TYPE:option.INTEGER,
				     option.USER_LEVEL:option.USER,},
		option.WITH_FINAL_SCAN:{option.HELP_STRING:
					"Do a final scan after all the"
					" files are recopied to tape.",
					option.VALUE_USAGE:option.IGNORED,
					option.VALUE_TYPE:option.INTEGER,
					option.USER_LEVEL:option.USER,},
        option.SFA_REPACKAGE:{option.HELP_STRING:
                    "Enable SFA repackaging",
                    option.VALUE_TYPE:option.INTEGER,},
		}


#Appened to bfid_list or volume_list the files to be migrated/scanned.
def get_targets(bfid_list_queue, volume_list_queue, isc, intf):
    #bfid_list_queue - empty bfids Queue to be filled by input from user.
    #volume_list_queue - empty volumes Queue to be filled by input from user.
    #isc - Info Server Client object
    #intf - MigrateInterface object

    if intf.infile:
        in_file = open(intf.infile, "r")
        list_of_targets = iter(in_file)
    else:
        list_of_targets = intf.args

    for target in list_of_targets:
        #If reading from file, remove trailing newline.
        target = target.strip()

        #Determine if the target is:
        #1) bfid          WAMS118340671500000
        #2) volume
        #   null or tape  VO0001
        #   disk          rain:zee.zaa_copy_1.null:1265730478610
        #3) volume:location
        #   null or tape  NULL09:0000_000000000_0000001
        #   old disk      rain:zee.zaa_copy_1.null:1265730478610:/data/rain//pnfs/mist/test_files/Makefile2:1192477203
        #   new disk      rain:zee.zaa_copy_1.null:1265730478610:/scratch/000/100/000/000/000/000/0AF/0001000000000000000AF418
        #4) path          /pnfs/xyz/abc
        #                 /pnfs/fs/usr/xyz/abc

        if enstore_functions3.is_bfid(target):
            bfid_list_queue.put(target, block=True)
        elif enstore_functions3.is_volume(target):
            volume_list_queue.put(target, block=True)
        elif is_media_type(target) and target == intf.args[0]:
            break
        elif is_volume_and_location_cookie(target):
            volume, location_cookie = \
                    exctract_volume_and_location_cookie(target)

            file_record = isc.find_file_by_location(volume,
                                                    location_cookie)
            if not e_errors.is_ok(file_record):
                error_log("%s: %s" % (file_record['status'][0],
                                      file_record['status'][1]))
                continue
            bfid_list_queue.put(file_record['bfid'], block=True)
        else:
            try:
                f = chimera.File(target)
                if f.bfid:
                    bfid_list_queue.put(f.bfid, block=True)
                else:
                    raise ValueError(target)
            except (SystemExit, KeyboardInterrupt):
                raise sys.exc_info()[0], \
                      sys.exc_info()[1], \
                      sys.exc_info()[2]
            except:
                error_log("can not find bfid of",
                          target)

    #Tell the main thread to stop waiting for items.
    bfid_list_queue.put(SENTINEL, block=True)
    volume_list_queue.put(SENTINEL, block=True)

def main(intf):
    init(intf)

    if intf.migrated_from:
        # get a db connection
        db = pg.DB(host=dbhost, port=dbport, dbname=dbname, user=dbuser)
        # get its own volume clerk client
        config_host = enstore_functions2.default_host()
        config_port = enstore_functions2.default_port()
        csc = configuration_client.ConfigurationClient((config_host,
                                                        config_port))
        vcc = volume_clerk_client.VolumeClerkClient(csc)

        show_migrated_from(intf.args, vcc, db)

        db.close()  #Avoid resource leaks.
        return 0

    elif intf.migrated_to:
        # get a db connection
        db = pg.DB(host=dbhost, port=dbport, dbname=dbname, user=dbuser)
        # get its own volume clerk client
        config_host = enstore_functions2.default_host()
        config_port = enstore_functions2.default_port()
        csc = configuration_client.ConfigurationClient((config_host,
                                                        config_port))
        vcc = volume_clerk_client.VolumeClerkClient(csc)

        show_migrated_to(intf.args, vcc, db)

        db.close()  #Avoid resource leaks.
        return 0

    elif intf.show:
        # get a db connection
        db = pg.DB(host=dbhost, port=dbport, dbname=dbname, user=dbuser)
        show_show(intf, db)
        db.close()  #Avoid resource leaks.

    elif intf.scan_volumes:
        message = "The --scan-volumes switch is depricated.  " \
                  "Use --scan instead.\n"
        sys.stderr.write(message)
        sys.exit(1)

    #For duplicate only.
    elif getattr(intf, "make_failed_copies", None):

        # get a db connection
        db = pg.DB(host=dbhost, port=dbport, dbname=dbname, user=dbuser)
        # get its own file clerk client and volume clerk client
        config_host = enstore_functions2.default_host()
        config_port = enstore_functions2.default_port()
        csc = configuration_client.ConfigurationClient((config_host,
                                                        config_port))
        fcc = file_clerk_client.FileClient(csc)
        vcc = volume_clerk_client.VolumeClerkClient(csc)

        ret_val = make_failed_copies(vcc, fcc, db, intf)

        db.close()  #Avoid resource leaks.
        return ret_val

    #For duplicate only.
    elif getattr(intf, "make_copies", None):

        # get a db connection
        db = pg.DB(host=dbhost, port=dbport, dbname=dbname, user=dbuser)
        # get its own file clerk client and volume clerk client
        config_host = enstore_functions2.default_host()
        config_port = enstore_functions2.default_port()
        csc = configuration_client.ConfigurationClient((config_host,
                                                        config_port))
        fcc = file_clerk_client.FileClient(csc)
        vcc = volume_clerk_client.VolumeClerkClient(csc)

        ret_val = make_copies(vcc, fcc, db, intf)

        db.close()  #Avoid resource leaks.
        return ret_val

    else:
        #--status, --restore, --scan and normal migration take the same
        # inputs.  Migrating/duplicating and scanning take one additional
        # input mode.

        rtn = 0  #return code

        # get its own info client
        config_host = enstore_functions2.default_host()
        config_port = enstore_functions2.default_port()
        csc = configuration_client.ConfigurationClient((config_host,
                                                        config_port))
        #Someday this probably could be done by the migration clerk.
        isc = info_client.infoClient(csc)

        bfid_list_queue = MigrateQueue(512)
        volume_list_queue = MigrateQueue(512)

        #Run the input reading in a different thread.  If the input list
        # has 10s of thousands of files or more we don't want to wait for
        # the last one to be read into memory before target processing
        # has begun.
        run_in_thread(get_targets, (bfid_list_queue, volume_list_queue,
                                    isc, intf),
                      my_task="GET_INPUT_TARGETS",
                      on_exception = (handle_process_exception,
                                      (bfid_list_queue, volume_list_queue),
                                      SENTINEL))

        if intf.status:
            # get a db connection
            db = pg.DB(host=dbhost, port=dbport, dbname=dbname, user=dbuser)

            bfid = bfid_list_queue.get(block=True)
            while bfid:
                rtn = rtn + show_status_files(bfid, db, intf)
                bfid = bfid_list_queue.get(block=True)
            volume = volume_list_queue.get(block=True)
            while volume:
                rtn = rtn + show_status_volumes(volume, db, intf)
                volume = volume_list_queue.get(block=True)

            db.close()  #Avoid resource leaks.
            return rtn
        elif intf.restore:
            bfid = bfid_list_queue.get(block=True)
            while bfid:
                restore_files(bfid, intf)
                bfid = bfid_list_queue.get(block=True)
            volume = volume_list_queue.get(block=True)
            while volume:
                restore_volume(volume, intf)
                volume = volume_list_queue.get(block=True)

            return errors

        elif intf.scan:
            bfid = bfid_list_queue.get(block=True)
            while bfid:
                rtn = rtn + final_scan_files([bfid], intf)
                bfid = bfid_list_queue.get(block=True)
            volume = volume_list_queue.get(block=True)
            while volume:
                rtn = rtn + final_scan_volume(volume, intf)
                volume = volume_list_queue.get(block=True)

            """
            The scan_remaining_volumes() function does not exist yet.
            """
            ##if not bfid_list and not volume_list and intf.args:
            ##    # get a db connection
            ##    db = pg.DB(host=dbhost, port=dbport, dbname=dbname, user=dbuser)
            ##    # get its own file clerk client and volume clerk client
            ##    config_host = enstore_functions2.default_host()
            ##    config_port = enstore_functions2.default_port()
            ##    csc = configuration_client.ConfigurationClient((config_host,
            ##                                                    config_port))
            ##    vcc = volume_clerk_client.VolumeClerkClient(csc)
            ##    rtn = rtn + scan_remaining_volumes(vcc, db, intf)
            ##
            ##    db.close()  #Avoid resource leaks.

            return rtn

        else:  #migration
            bfid = bfid_list_queue.get(block=True)
            if bfid:
                have_bfid = True
            else:
                have_bfid = False
            while bfid:
                rtn = rtn + migrate_files([bfid], intf)
                bfid = bfid_list_queue.get(block=True)
            volume = volume_list_queue.get(block=True)
            if volume:
                have_volume = True
            else:
                have_volume = False
            while volume:
                rtn = rtn + migrate_volume(volume, intf)
                volume = volume_list_queue.get(block=True)

            if not have_bfid and not have_volume and intf.args:
                # get a db connection
                db = pg.DB(host=dbhost, port=dbport, dbname=dbname, user=dbuser)
                # get its own file clerk client and volume clerk client
                config_host = enstore_functions2.default_host()
                config_port = enstore_functions2.default_port()
                csc = configuration_client.ConfigurationClient((config_host,
                                                                config_port))
                vcc = volume_clerk_client.VolumeClerkClient(csc)
                rtn = rtn + migrate_remaining_volumes(vcc, db, intf)

                db.close()  #Avoid resource leaks.
            return rtn

    return 0


def do_work(intf):

    try:
        exit_status = main(intf)
    except (SystemExit, KeyboardInterrupt):
        exc, msg = sys.exc_info()[:2]
        exit_status = 1
    except:
        exc, msg, tb = sys.exc_info()
        message = "Uncaught exception: %s, %s\n" % (exc, msg)
        try:
            error_log(message)
            # Send to the log server the traceback dump.
            # If unsuccessful, print the traceback to standard error.
            Trace.handle_error(exc, msg, tb)
        except (OSError, IOError):
            if msg.errno == errno.EPIPE:
                #User piped the output to another process, but
                # didn't read all the data from the migrate
                # process.
                pass
            else:
                raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        del tb #No cyclic references.
        exit_status = 1

    #We should try and kill our child processes.
    if USE_THREADS:
        wait_for_threads()
    else:
        wait_for_processes(kill = True)

    #With the possibility that exactly 256 failures could occur, the
    # default sys.exit() behavior when passed 256 is to return an exit
    # status to the caller.  Map all non-zero values to one.
    # @FIXME: the other exit values have some meening too; set to small value
    sys.exit(bool(exit_status))


if __name__ == '__main__':

    Trace.init(MIGRATION_NAME)
    Trace.do_message(0)

    delete_at_exit.setup_signal_handling()

    intf_of_migrate = MigrateInterface(sys.argv, 0) # zero means admin

    try:
        do_work(intf_of_migrate)
    except (OSError, IOError), msg:
        if msg.errno == errno.EPIPE:
            #User piped the output to another process, but
            # didn't read all the data from the migrate process.
            pass
        else:
            raise sys.exc_info()[0], sys.exc_info()[1], \
                  sys.exc_info()[2]

