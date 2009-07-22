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
    -- read mirgated file as users do
    -- paranoid reassurance

Implementation issues:
[1] copying to disk, copying from disk and reading migrated file
    are done in three concurrent threads
    -- These threads are synchronized using two queues
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
import re
import stat
import socket


# enstore imports
import file_clerk_client
import volume_clerk_client
import configuration_client
import pnfs
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

debug = False	# debugging mode

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

#When true, fork off a new process/thread that will handle migrating one file.
# Then keep multiple procesess going.
PARALLEL_FILE_MIGRATION = False

#When true, start PROC_LIMIT number of processes/threads for reading and
# PROC_LIMIT number of processes/threads for writing.
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
    sys.stderr.write("Warning: Module multiprocessing not available.\n")

#Pass --threaded to encp if true.
USE_THREADED_ENCP = True

##
## End multiple_threads / forked_processes global variables.
##

###############################################################################

#Number of processes/threads to juggle at once.
PROC_LIMIT = 3


#Default size of the Queue class objects.
DEFUALT_QUEUE_SIZE = 1024

#The value sent over a pipe to signal the receiving end there are no more
# comming.
SENTINEL = "SENTINEL"

FILE_LIMIT = 25 #The maximum number of files to wait for at one time.

###############################################################################

#Define the lock so that the output is not split on each line of log output.
try:
    io_lock = multiprocessing.Lock()
except NameError:
    io_lock = threading.Lock()

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

#If the tape speeds for the new media are faster then the old media; this
# should be: int(NUM_OBJS * (1 - (old_rape_rate / new_tape_rate)))
#If they are the same speed then go with 2.
proceed_number = 2

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

# initialize csc, db, ... etc.
def init(intf):
	global log_f, dbhost, dbport, dbname, dbuser, errors
	global SPOOL_DIR
	#global use_file_family
        global pnfs_is_trusted
        global do_seteuid
        global debug

        #Make getting debug information from the command line possible.
        if intf.debug:
            debug = intf.debug

	csc = configuration_client.ConfigurationClient((intf.config_host,
							intf.config_port))

	db_info = csc.get('database')
	dbhost = db_info['dbhost']
	dbport = db_info['dbport']
	dbname = db_info['dbname']
	dbuser = db_info['dbuser']

	errors = 0

        #Verify that all libraries passed in exist.
	if intf.library:
                for library in intf.library.split(","):
		        library_fullname = library + ".library_manager"
                        lib_dict = csc.get(library_fullname)
                        if not e_errors.is_ok(lib_dict):
                                sys.stderr.write("library %s does not exist\n" %
                                                 (library,))
                                sys.exit(1)

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
            amp = pnfs.get_enstore_admin_mount_point() #amp = Admin Mount Point
            if len(amp) == 0 and \
                   not getattr(intf, 'make_failed_copies', None):
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
                    sys.stderr.write("%s is not trusted: %s\n" % (amp, str(msg),))
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
                        sys.stderr.write("%s is not trusted: %s\n" % (amp, str(msg),))
                        sys.exit(1)
            else:
                pnfs_is_trusted = True
                do_seteuid = False

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
			message = "Insufficent permissions to open log file."
			error_log(message)
			sys.exit(1)
		log_f = open(os.path.join(LOG_DIR, LOG_FILE), "a")
		log(MIGRATION_NAME, string.join(sys.argv, " "))
	
	# check for spool_dir commands
	if not intf.migrated_to and not intf.migrated_from and \
	   not intf.status and not intf.show and not intf.scan_volumes and \
	   not getattr(intf, "restore", None):
		#spool dir
		if not SPOOL_DIR:
			message = "No spool directory specified."
			error_log(message)
			sys.exit(1)
		if not os.access(SPOOL_DIR, os.W_OK):
			os.makedirs(SPOOL_DIR)

		#migration dir - Make sure it has correct permissions.
		admin_mount_points = pnfs.get_enstore_admin_mount_point()
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


	#if intf.file_family:
	#	use_file_family = intf.file_family

	return

#Update the proceed_number global variable.
def set_proceed_number(src_bfids, copy_queue, scan_queue, intf):
	global proceed_number
	#global copy_queue, scan_queue

	#If the user specified that all the files should be read, before
	# starting to write; achive this by setting the proceed_number
	# ot the number of files on the tape.
	if intf.read_to_end_of_tape:
		proceed_number = len(src_bfids)
		copy_queue.__init__(proceed_number)
		scan_queue.__init__(proceed_number)
		return
	elif type(intf.proceed_number) == types.IntType:
		#Map the user supplied proceed number to be within the
		# bounds of 1 and the default queue size.
		proceed_number = min(intf.proceed_number, DEFUALT_QUEUE_SIZE)
		proceed_number = max(proceed_number, 1)
		return

	if len(src_bfids) == 0:
		#If the volume contains only deleted files and --with-deleted
		# was not used; src_bfids will be an empty list.  In this
		# case, skip setting this value to avoid raising an
		# IndexError doing the "get_media_type(src_bfids[0], db)"
		# below.
		return

	# get a db connection
	db = pg.DB(host=dbhost, port=dbport, dbname=dbname, user=dbuser)

	###############################################################
	#Determine the media speeds that the migration will be going at.
	src_media_type = get_media_type(src_bfids[0], db)
	if intf.library and type(intf.library) == types.StringType:
		dst_media_type = get_media_type(intf.library.split(",")[0], db)
	else:
		mig_path = get_migration_db_path()
		if mig_path != None:
			dst_media_type = get_media_type(mig_path, db)
		else:
			#If we get here, we would need to look at the tags
			# in the original directory, but since we don't have
			# that available here, lets just drop it for now.
			return
	
	src_rate = getattr(enstore_constants,
			       "RATE_" + str(src_media_type), None)
	dst_rate = getattr(enstore_constants,
			       "RATE_" + str(dst_media_type), None)
	
	if src_rate and dst_rate:
		proceed_number = int(len(src_bfids) * (1 - (src_rate / dst_rate)))

	#Adjust the file limit to account for the number of processes/threads.
	if PARALLEL_FILE_TRANSFER:
		use_limit = FILE_LIMIT / PROC_LIMIT
	else:
		use_limit = FILE_LIMIT
		
	#Put some form of bound on this value until its effect on performance
	# is better understood.
	proceed_number = min(proceed_number, use_limit)
	proceed_number = max(proceed_number, 1)
		
	###############################################################
	if debug:
		log("proceed_number:", proceed_number)

#If the source and destination media_types are the same, set this to be
# a cloning job rather than a migration.
def setup_cloning():
	global IN_PROGRESS_STATE, INHIBIT_STATE
	global set_system_migrated_func, set_system_migrating_func
	IN_PROGRESS_STATE = "cloning"
	INHIBIT_STATE = "cloned"
	set_system_migrated_func=volume_clerk_client.VolumeClerkClient.set_system_cloned
	set_system_migrating_func=volume_clerk_client.VolumeClerkClient.set_system_cloning

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

    if do_seteuid:
        file_utils.match_euid_egid(pname)
    #else:
    #    file_utils.acquire_lock_euid_egid()

    try:
        p1 = pnfs.File(pname)
        for i in [1,2,4]:
            f = open(p1.layer_file(i), 'w')
            f.close()
    except (KeyboardInterrupt, SystemExit):
        if do_seteuid:
            file_utils.end_euid_egid(reset_ids_back = True)
        #else:
        #    file_utils.release_lock_euid_egid()
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
    except (OSError, IOError), msg:
        #Don't worry if the file is already gone.
        if msg.errno not in [errno.ENOENT]:
            if do_seteuid:
                file_utils.end_euid_egid(reset_ids_back = True)
            #else:
            #    file_utils.release_lock_euid_egid()
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
    except:
        if do_seteuid:
            file_utils.end_euid_egid(reset_ids_back = True)
        #else:
        #    file_utils.release_lock_euid_egid()
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]

    #Now set the root ID's back.
    if do_seteuid:
        file_utils.end_euid_egid(reset_ids_back = True)
    #else:
    #    file_utils.release_lock_euid_egid()

def pnfs_find(bfid1, bfid2, pnfs_id, file_record = None, intf = None):

    if do_seteuid:
        # We need to keep other threads from changing the euid/egid
        # while find_pnfsid_path() is running.
        file_utils.acquire_lock_euid_egid()
        #Should be set to root.root already, but just in case.
        file_utils.set_euid_egid(0, 0)
    else:
        file_utils.acquire_lock_euid_egid()
        #If another thread doesn't use "reset_ids_back = True" then
        # be sure that the euid and egid are for roots, which it what the
        # rest of this function assumes the euid and egid are set to.
        file_utils.set_euid_egid(0, 0)

    #This is a hack for running duplication on a machine without trusted
    # status.  We allow for all types of PNFS filesystems to be used.
    if intf and getattr(intf, "make_failed_copies", None):
        use_path_type = find_pnfs_file.BOTH
    else:
        use_path_type = find_pnfs_file.FS

    src = None
    try:
        src = find_pnfs_file.find_pnfsid_path(
            pnfs_id, bfid1,
            file_record = file_record,
            path_type = use_path_type)
    except (KeyboardInterrupt, SystemExit):
        if do_seteuid:
            #Free lock on error.
            file_utils.release_lock_euid_egid()
        else:
            file_utils.release_lock_euid_egid()
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
    except (OSError, IOError):
        exc_type, exc_value, exc_tb = sys.exc_info()
        del exc_tb #avoid resource leaks

        try:
            if bfid2:
                #If the migration is interupted
                # part way through the swap, we need
                # to check if the other bfid is
                # current in layer 1.
                src = find_pnfs_file.find_pnfsid_path(
                    pnfs_id, bfid2,
                    path_type = use_path_type)
        except (KeyboardInterrupt, SystemExit):
            if do_seteuid:
                #Free lock on error.
                file_utils.release_lock_euid_egid()
            else:
                file_utils.release_lock_euid_egid()
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except OSError:
                pass
        except:
                pass

        if not src:
            if do_seteuid:  #Free lock on error.
                file_utils.release_lock_euid_egid()
            else:
                file_utils.release_lock_euid_egid()

            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]

    if do_seteuid:  #Free lock on error.
        file_utils.release_lock_euid_egid()
    else:
        file_utils.release_lock_euid_egid()

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
        p_File = pnfs.File(path)
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
        file_utils.end_euid_egid(reset_ids_back = True)
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
		try:
			sys.stdout.flush()
			sys.stderr.flush()
		except IOError:
			pass

		io_lock.acquire()
		try:
			log_f.flush()
			os.fsync(log_f.fileno())
		except (IOError, OSError):
			pass
		io_lock.release()

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
        try:
            sys.stdout.flush()
            sys.stderr.flush()
        except IOError:
            pass

        io_lock.acquire()
        try:
            log_f.flush()
            os.fsync(log_f.fileno())
        except (IOError, OSError):
            pass
        io_lock.release()

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

            #If we blinkly go into the join(), we will be stuck waiting
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
        while len(tid_list):
                wft_rtn = wait_for_thread()
                rtn = rtn + wft_rtn
                time.sleep(5)

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
	pnfs_fs_paths = pnfs.get_enstore_admin_mount_point()
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
	
def is_migration_path(filepath):
	#Make sure this is a string.
	if type(filepath) != types.StringType:
		raise TypeError("Expected string filename.",
				e_errors.WRONGPARAMETER)

	#Is this good enough?  Or does something more stringent need to
	# be used.
	if filepath.find(MIGRATION_DB) != -1:
		return 1

	if os.path.basename(filepath).startswith(".m."):
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

# The following three functions query the state of a migrating file
# If true, the timestamp is returned, other wise, None is returned

# is_copied(bfid) -- has the file already been copied?
#	we check the source file
def is_copied(bfid, db):
    q = "select * from migration where src_bfid = '%s';"%(bfid)
    if debug:
        log("is_copied():", q)
    res = db.query(q).dictresult()
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
        
# is_swapped(bfid) -- has the file already been swapped?
#	we check the source file
def is_swapped(bfid, db):
    q = "select * from migration where src_bfid = '%s';"%(bfid)
    if debug:
        log("is_swapped():", q)
    res = db.query(q).dictresult()
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

# is_checked(bfid) -- has the file already been checked?
#	we check the destination file
def is_checked(bfid, db):
    q = "select * from migration where dst_bfid = '%s';"%(bfid)
    if debug:
        log("is_checked():", q)
    res = db.query(q).dictresult()
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
def is_closed(bfid, db):
    q = "select * from migration where dst_bfid = '%s';"%(bfid)
    if debug:
        log("is_closed():", q)
    res = db.query(q).dictresult()
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

# get_bfids(bfid) -- has the file already been closed?
#	we check the destination file
def get_bfids(bfid, db):
	q = "select * from migration where dst_bfid = '%s';"%(bfid)
	if debug:
		log("is_closed():", q)
	res = db.query(q).dictresult()
	if not len(res):
		return (None, None)
	else:
		return (res[0]['src_bfid'], res[0]['dst_bfid'])

###############################################################################

# open_log(*args) -- log message without final line-feed
def open_log(*args):
	t = time.time()
        ctime = time.ctime(t)
        thread_name = threading.currentThread().getName()
	print ctime, thread_name,
	for i in args:
		print i,
	if log_f:
		log_f.write(ctime+" "+thread_name+" ")
		for i in args:
			log_f.write(str(i)+" ")
		log_f.flush()
		
# error_log(s) -- handling error message
def error_log(*args):
	global errors

	errors = errors + 1
	io_lock.acquire()
	open_log(*args)
	print '... ERROR'
	if log_f:
		log_f.write('... ERROR\n')
		log_f.flush()
	io_lock.release()

def ok_log(*args):
	io_lock.acquire()
	open_log(*args)
	print '... OK'
	if log_f:
		log_f.write('... OK\n')
		log_f.flush()
	io_lock.release()

# log(*args) -- log message
def log(*args):
	io_lock.acquire()
	open_log(*args)
	print
	if log_f:
		log_f.write('\n')
		log_f.flush()
	io_lock.release()

# close_log(*args) -- close open log
def close_log(*args):
	if log_f:
		for i in args:
			log_f.write(i+' ')
		log_f.write("\n")
		log_f.flush()
	for i in args:
		print i,
	print

# log_copied(bfid1, bfid2) -- log a successful copy
def log_copied(bfid1, bfid2, db):
    q = "insert into migration (src_bfid, dst_bfid, copied) \
            values ('%s', '%s', '%s');" % (bfid1, bfid2,
            time2timestamp(time.time()))
    if debug:
        log("log_copied():", q)
    try:
        db.query(q)
    except pg.ProgrammingError, msg:
        if str(msg).find("duplicate key violates unique constraint") != -1:
                #The old unique constraint was on each src & dst
                # column.  For modern migration capable of migrating
                # to multiple copies this constraint needs to be on
                # the pair of src & dst columns.
                log("The database has an obsolete unique key constraint.  This will prevent multiple copies from being scanned.")

        exc_type, exc_value = sys.exc_info()[:2]
        error_log("LOG_COPIED", str(exc_type), str(exc_value), q)
    except:
        exc_type, exc_value = sys.exc_info()[:2]
        error_log("LOG_COPIED", str(exc_type), str(exc_value), q)
    return

# log_uncopied(bfid1, bfid2) -- log a successful uncopy (aka restore)
def log_uncopied(bfid1, bfid2, db):
    q = "update migration set copied = NULL where \
            src_bfid = '%s' and dst_bfid = '%s'; \
            delete from migration where \
            src_bfid = '%s' and dst_bfid = '%s';" % \
    (bfid1, bfid2, bfid1, bfid2)
    if debug:
        log("log_uncopied():", q)
    try:
        db.query(q)
    except:
        exc_type, exc_value = sys.exc_info()[:2]
        error_log("LOG_UNCOPIED", str(exc_type), str(exc_value), q)
    return

# log_swapped(bfid1, bfid2) -- log a successful swap
def log_swapped(bfid1, bfid2, db):
    q = "update migration set swapped = '%s' where \
            src_bfid = '%s' and dst_bfid = '%s';" % \
                    (time2timestamp(time.time()), bfid1, bfid2)
    if debug:
        log("log_swapped():", q)
    try:
        db.query(q)
    except:
        exc_type, exc_value = sys.exc_info()[:2]
        error_log("LOG_SWAPPED", str(exc_type), str(exc_value), q)
    return

# log_unswapped(bfid1, bfid2) -- log a successful unswap (aka restore)
def log_unswapped(bfid1, bfid2, db):
    q = "update migration set swapped = NULL where \
            src_bfid = '%s' and dst_bfid = '%s';" % \
                    (bfid1, bfid2)
    if debug:
        log("log_unswapped():", q)
    try:
        db.query(q)
    except:
        exc_type, exc_value = sys.exc_info()[:2]
        error_log("LOG_UNSWAPPED", str(exc_type), str(exc_value), q)
    return

# log_checked(bfid1, bfid2) -- log a successful readback 
def log_checked(bfid1, bfid2, db):
    q = "update migration set checked = '%s' where \
            src_bfid = '%s' and dst_bfid = '%s';"%(
                    time2timestamp(time.time()), bfid1, bfid2)
    if debug:
        log("log_checked():", q)
    try:
        db.query(q)
    except:
        exc_type, exc_value = sys.exc_info()[:2]
        error_log("LOG_CHECKED", str(exc_type), str(exc_value), q)
    return

# log_closed(bfid1, bfid2) -- log a successful readback after closing
def log_closed(bfid1, bfid2, db):
    q = "update migration set closed = '%s' where \
            src_bfid = '%s' and dst_bfid = '%s';"%(
                    time2timestamp(time.time()), bfid1, bfid2)
    if debug:
        log("log_closed():", q)
    try:
        db.query(q)
    except:
        exc_type, exc_value = sys.exc_info()[:2]
        error_log("LOG_CLOSED", str(exc_type), str(exc_value), q)
    return

# log_history(src, dst) -- log a migration history
def log_history(src, dst, db):

	# Obtain the unique volume id for the source and destination volumes.
	q = "select id from volume where label = '%s'" % (src,)

	try:
		res = db.query(q).getresult()
		src_vol_id = res[0][0]  #volume id
	except:
		exc_type, exc_value = sys.exc_info()[:2]
		error_log("LOG_HISTORY", str(exc_type), str(exc_value), q)
		return
	q = "select id from volume where label = '%s'" % (dst,)

	try:
		res = db.query(q).getresult()
		dst_vol_id =  res[0][0]  #volume id
	except:
		exc_type, exc_value = sys.exc_info()[:2]
		error_log("LOG_HISTORY", str(exc_type), str(exc_value), q)
		return

	# Insert this volume combintation into the migration_history table.
	q = "insert into migration_history (src, src_vol_id, dst, dst_vol_id) values \
		('%s', '%s', '%s', '%s');"%(src, src_vol_id, dst, dst_vol_id)
	if debug:
		log("log_history():", q)
	try:
		db.query(q)
	except:
		exc_type, exc_value = sys.exc_info()[:2]
		#If the volume is being rerun, ignore the unique constraint
		# error from postgres.
		if not str(exc_value).startswith(
			"ERROR:  duplicate key violates unique constraint"):
			error_log("LOG_HISTORY", str(exc_type), str(exc_value), q)
	return

def log_history_closed(src, dst, db):

	# Obtain the unique volume id for the source and destination volumes.
	q = "select id from volume where label = '%s'" % (src,)

	try:
		res = db.query(q).getresult()
		src_vol_id = res[0][0]  #volume id
	except:
		exc_type, exc_value = sys.exc_info()[:2]
		error_log("LOG_HISTORY", str(exc_type), str(exc_value), q)
		return
	q = "select id from volume where label = '%s'" % (dst,)

	try:
		res = db.query(q).getresult()
		dst_vol_id =  res[0][0]  #volume id
	except:
		exc_type, exc_value = sys.exc_info()[:2]
		error_log("LOG_HISTORY", str(exc_type), str(exc_value), q)
		return
	
	#Update the closed_time column in the migration_history table
	# for these two volumes.
	q = "update migration_history set closed_time = current_timestamp " \
	    "where migration_history.src_vol_id = '%s' " \
            "      and migration_history.dst_vol_id = '%s';" % \
	    (src_vol_id, dst_vol_id)

	if debug:
		log("log_history():", q)
	try:
		db.query(q)
	except:
		exc_type, exc_value = sys.exc_info()[:2]
		error_log("LOG_HISTORY_CLOSED", str(exc_type), str(exc_value), q)
	return

# undo_log(src, dst) -- remove a source and destination bfid pair from the
#                       migration table.
def undo_log(src_bfid, dst_bfid, db):
	q = "delete from migration where \
	        src_bfid = '%s' and dst_bfid = '%s';" \
	    % (src_bfid, dst_bfid)
	
	if debug:
		log("undo_log():", q)
	try:
		db.query(q)
	except:
		exc_type, exc_value = sys.exc_info()[:2]
		error_log("UNDO_LOG", str(exc_type), str(exc_value), q)
	return

#Return the volume that the bfid refers to.
def get_volume_from_bfid(bfid, db):
	if not enstore_functions3.is_bfid(bfid):
		return False

	q = "select label from volume,file where file.volume = volume.id " \
	    " and file.bfid = '%s';" % (bfid,)

	try:
		res = db.query(q).getresult()
		return res[0][0]  #volume
	except:
		exc_type, exc_value = sys.exc_info()[:2]
		error_log("get_volume_from_bfid", str(exc_type),
			  str(exc_value), q)
	
	return None

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
	elif pnfs.is_pnfs_path(arguement, check_name_only = 1):
		try:
			t = pnfs.Tag(arguement)
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
		error_log("get_volume_from_bfid", str(exc_type),
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
def get_migration_type(src_vol, dst_vol, db):
	if not src_vol or not dst_vol:
		#For tapes that are not yet migrated/duplicated, the dst_vol
		# will be None here.
		return

	migration_result = None
	duplication_result = None
	cloning_result = None
	
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

	#print "migration_result:", migration_result
	#print "duplication_result:", duplication_result
	#print "cloning_result:", cloning_result
	if migration_result and duplication_result:
		return "The metadata is inconsistent between migration " \
		       "and duplication."
	elif (not migration_result and cloning_result) and duplication_result:
		#If duplicating to the same media...
		return duplication_result
	elif cloning_result:
		return cloning_result
	elif migration_result:
		return migration_result
	elif duplication_result:
		return duplication_result
		
	return None

#Report the multiple copies a file has.
def get_multiple_copy_bfids(bfid, db):

    q = "select alt_bfid from file_copies_map where bfid = '%s';" % (bfid,)
    res = db.query(q).getresult()

    multiple_copy_list = []
    for row in res:
        multiple_copy_list.append(row[0])

    return multiple_copy_list
    

#
def search_directory(original_path):
	##
	## Determine the deepest directory that exists.
	##
	mig_dir = pnfs.get_directory_name(migration_path(original_path))
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

def get_file_info(MY_TASK, bfid, db):
	# get file info
	q = "select bfid, label, location_cookie, pnfs_id, \
		storage_group, file_family, deleted, \
		pnfs_path, size, crc as complete_crc, \
		wrapper \
		from file, volume \
		where file.volume = volume.id and \
			bfid = '%s';"%(bfid)
	if debug:
		log(MY_TASK, q)
	res = db.query(q).dictresult()

	# does it exist?
	if not len(res):
		error_log(MY_TASK, "%s does not exists"%(bfid))
		return None

	return res[0]


##########################################################################

def mark_deleted(MY_TASK, src_bfid, fcc, db):
	# mark the original deleted
	q = "select deleted from file where bfid = '%s';" % (src_bfid)
	res = db.query(q).getresult()
	if len(res):
		if res[0][0] != 'y':
			res = fcc.set_deleted('yes', bfid=src_bfid)
			if res['status'][0] == e_errors.OK:
				ok_log(MY_TASK, "set %s deleted" % (src_bfid,))
			else:
				error_log(MY_TASK, "failed to set %s deleted" % (src_bfid,))
				return 1
		else:
			ok_log(MY_TASK, "%s has already been marked deleted" % (src_bfid,))

	return 0

def mark_undeleted(MY_TASK, src_bfid, fcc, db):
	# mark the original undeleted
	q = "select deleted from file where bfid = '%s';" % (src_bfid)
	res = db.query(q).getresult()
	if len(res):
		if res[0][0] != 'n':
			res = fcc.set_deleted('no', bfid=src_bfid)
			if res['status'][0] == e_errors.OK:
				ok_log(MY_TASK, "set %s undeleted" % (src_bfid,))
			else:
				error_log(MY_TASK, "failed to set %s undeleted" % (src_bfid,))
				return 1
		else:
			ok_log(MY_TASK, "%s has already been marked undeleted" % (src_bfid,))

	return 0


##########################################################################

# migration_path(path) -- convert path to migration path
# a path is of the format: /pnfs/fs/usr/X/...
#                          a migration path is: /pnfs/fs/usr/Migration/X/...
# deleted is either 'n' or 'y'; anything else is equal to 'n'
def migration_path(path, deleted = 'n'):
	admin_mount_points = pnfs.get_enstore_admin_mount_point()
	if len(admin_mount_points) == 0:
		#If the admin path is not mounted, use the normal path...
		dname, fname = os.path.split(path)
		#...just be sure to stick .m. at the beginning and to
		# limit the character count.
		use_fname = ".m.%s" % (fname,)[:pnfs.PATH_MAX]
		mig_path = os.path.join(dname, use_fname)
		return mig_path
	elif len(admin_mount_points) >= 1:
		admin_mount_point = admin_mount_points[0]
	else:
		return None

	if not admin_mount_point:
			admin_mount_point = DEFAULT_FS_PNFS_PATH

	if deleted == 'y':
		return os.path.join(admin_mount_point,
				    MIGRATION_DB,
				    DELETED_TMP,
				    os.path.basename(path))

	stripped_name = pnfs.strip_pnfs_mountpoint(path)

	#Already had the migration path.
	if stripped_name.startswith(MIGRATION_DB + "/"):
		return os.path.join(admin_mount_point, stripped_name)

	return os.path.join(admin_mount_point,
			    MIGRATION_DB, stripped_name)

# temp_file(file_record) -- get a temporary destination file from file
def temp_file(file_record):
	if enstore_functions3.is_volume_disk(file_record['label']):
		#We need to treat disk files differently.
		return os.path.join(SPOOL_DIR, file_record['bfid'])
	
	return os.path.join(SPOOL_DIR,
			    "%s:%s" % (file_record['label'],
				       file_record['location_cookie']))

##########################################################################

## This class is a customized class similar to python Queue.Queue() class.
## This may be able to be replaced with the multiprocessing module in
## python 2.6.  For compatiblity with 2.4, this is what we need.

class MigrateQueue:

    def __init__(self, maxsize, notify_every_time = True,
                 low_watermark = 1):
        self.queue = Queue.Queue(maxsize)

	self.finished = False
	self.received_count = 0
	self.debug = debug
        self.initial_wait = True #Wait until low_watermark items are queued.
        self.low_watermark = low_watermark
        
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
        if self.cur_thread:
	    if self.debug:
	        log("joining thread")

	    self.lock.acquire() #Acquire the lock to access a self data member.

	    try:
	        self.cur_thread.join()
		self.cur_thread = None
	    except:
	        self.lock.release()
		raise sys.exc_info()[0], sys.exc_info()[1], \
		      sys.exc_info()[2]
			    
	    self.lock.release()
	    
	    if self.debug:
	        log("joined thread")

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
		    return None  #We are really done.
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
        if (USE_THREADS or multiprocessing_available) and \
               self.notify_every_time:
            if self.debug:
                log("acquiring condition lock")
            self.cv.acquire()
            if self.debug:
                log("acquired condition lock")
            while not self.finished and ((self.initial_wait and \
                      self.queue.qsize() < self.low_watermark) \
                                               or self.queue.qsize() == 0):
                if self.debug:
                    log("waiting for condition")
                self.cv.wait()
            #job = copy_queue.get(block = True)
            job = self.__get(block, timeout)
            if self.debug:
                log("releasing condition lock")
            self.cv.release()
        else:
            #This is the old way if using processes with python 2.4.  Or
            # if we only flag the condition when the queue is finished.
            while not self.finished and ((self.initial_wait and \
                      self.queue.qsize() < self.low_watermark) \
                                               or self.queue.qsize() == 0):
                time.sleep(1)
            #job = self.queue.get(block = True)
            job = self.__get(block, timeout)

        #Set a flag indicating that we have read the last item.
        if job == SENTINEL:
            #Acquire the lock to access a self data member.
            self.lock.acquire()
            self.finished = True
            self.initial_wait = False
            self.lock.release()
            return None  #We are really done.
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
                self.queue.put(item, block = block,
                               timeout = timeout)

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

def show_migrated_from(volume_list, db):
    for vol in volume_list:
        from_list = migrated_from(vol, db)
        #We need to determine if migration or
        # duplication was used.
        try:
            mig_type = get_migration_type(from_list[0], vol, db)
            if mig_type == "MIGRATION":
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

def show_migrated_to(volume_list, db):
    for vol in volume_list:
        to_list = migrated_to(vol, db)
        #We need to know determine if migration or
        # duplication was used.
        try:
            mig_type = get_migration_type(vol, to_list[0], db)
            if mig_type == "MIGRATION":
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

def show_status(volume_list, db, intf):
    exit_status = 0

    for v in volume_list:
        #When the volume is a source volume.
        q1a = "select f1.bfid, f1.deleted as src_del, " \
              "       case when b1.bfid is not NULL then 'B' " \
              "            when f1.deleted = 'y' and f1.pnfs_id = '' then 'E' /*Mark failed and deleted files special*/ " \
              "            else ' ' " \
              "       end as src_bad, " \
              "       case when (select fcm.bfid " \
              "                  from file_copies_map fcm " \
              "                  where f1.bfid = fcm.bfid " \
              "                    and f2.bfid = fcm.alt_bfid) is not NULL" \
              "            then 'P'" \
              "            when (select fcm.bfid" \
              "                  from file_copies_map fcm" \
              "                  where f1.bfid = fcm.alt_bfid " \
              "                    and f2.bfid = fcm.bfid) is not NULL" \
              "            then 'C'" \
              "            /* This last section is for reporting if " \
              "               the destination copy has multiple " \
              "               copies. */" \
              "            when (select count(fcm.bfid)" \
              "                  from file_copies_map fcm" \
              "                  where f1.bfid = fcm.bfid) > 0" \
              "            then 'O'" \
              "            when (select count(fcm.bfid)" \
              "                  from file_copies_map fcm" \
              "                  where f1.bfid = fcm.alt_bfid) > 0" \
              "            then 'M'" \
              "            else NULL" \
              "       end as src_dup, " \
              "       f2.bfid, f2.deleted as dst_del, " \
              "       case when b2.bfid is not NULL then 'B' " \
              "            else ' ' " \
              "       end as dst_bad, " \
              "       case when (select fcm.bfid " \
              "                  from file_copies_map fcm " \
              "                  where f1.bfid = fcm.alt_bfid " \
              "                    and f2.bfid = fcm.bfid) is not NULL" \
              "            then 'P'" \
              "            when (select fcm.bfid" \
              "                  from file_copies_map fcm" \
              "                  where f1.bfid = fcm.bfid " \
              "                    and f2.bfid = fcm.alt_bfid) is not NULL" \
              "            then 'C'" \
              "            /* This last section is for reporting if " \
              "               the destination copy has multiple " \
              "               copies. */" \
              "            when (select count(fcm.bfid)" \
              "                  from file_copies_map fcm" \
              "                  where f2.bfid = fcm.bfid) > 0" \
              "            then 'O'" \
              "            when (select count(fcm.bfid)" \
              "                  from file_copies_map fcm" \
              "                  where f2.bfid = fcm.alt_bfid) > 0" \
              "            then 'M'" \
              "            else NULL" \
              "       end as dst_dup, " \
              "       copied, swapped, checked, closed " \
              "from volume, migration " \
              "right join file f1 on f1.bfid = migration.src_bfid " \
              "left join file f2 on f2.bfid = migration.dst_bfid " \
              "left join bad_file b1 on b1.bfid = f1.bfid " \
              "left join bad_file b2 on b2.bfid = f2.bfid " \
              "where f1.volume = volume.id " \
              "  and volume.label = '%s' " \
              "order by f1.location_cookie;" % (v,)

        #When the volume is a destination volume.
        q1b = "select f1.bfid, f1.deleted as src_del, " \
              "       case when b1.bfid is not NULL then 'B' " \
              "            when f1.deleted = 'y' and f1.pnfs_id = '' then 'E' /*Mark failed and deleted files special*/ " \
              "            else ' ' " \
              "       end as src_bad, " \
              "       case when (select fcm.bfid " \
              "                  from file_copies_map fcm " \
              "                  where f1.bfid = fcm.bfid " \
              "                    and f2.bfid = fcm.alt_bfid) is not NULL" \
              "            then 'P'" \
              "            when (select fcm.bfid" \
              "                  from file_copies_map fcm" \
              "                  where f1.bfid = fcm.alt_bfid " \
              "                    and f2.bfid = fcm.bfid) is not NULL" \
              "            then 'C'" \
              "            /* This last section is for reporting if " \
              "               the destination copy has multiple " \
              "               copies. */" \
              "            when (select count(fcm.bfid)" \
              "                  from file_copies_map fcm" \
              "                  where f1.bfid = fcm.bfid) > 0" \
              "            then 'O'" \
              "            when (select count(fcm.bfid)" \
              "                  from file_copies_map fcm" \
              "                  where f1.bfid = fcm.alt_bfid) > 0" \
              "            then 'M'" \
              "            else NULL" \
              "       end as src_dup, " \
              "       f2.bfid, f2.deleted as dst_del, " \
              "       case when b2.bfid is not NULL then 'B' " \
              "            else ' ' " \
              "       end as dst_bad, " \
              "       case when (select fcm.bfid " \
              "                  from file_copies_map fcm " \
              "                  where f1.bfid = fcm.alt_bfid " \
              "                    and f2.bfid = fcm.bfid) is not NULL" \
              "            then 'P'" \
              "            when (select fcm.bfid " \
              "                  from file_copies_map fcm " \
              "                  where f1.bfid = fcm.bfid " \
              "                    and f2.bfid = fcm.alt_bfid) is not NULL" \
              "            then 'C'" \
              "            /* This last section is for reporting if " \
              "               the destination copy has multiple " \
              "               copies. */" \
              "            when (select count(fcm.bfid)" \
              "                  from file_copies_map fcm" \
              "                  where f2.bfid = fcm.bfid) > 0" \
              "            then 'O'" \
              "            when (select count(fcm.bfid)" \
              "                  from file_copies_map fcm" \
              "                  where f2.bfid = fcm.alt_bfid) > 0" \
              "            then 'M'" \
              "            else NULL" \
              "       end as dst_dup, " \
              "       copied, swapped, checked, closed " \
              "from volume, migration " \
              "left join file f1 on f1.bfid = migration.src_bfid " \
              "right join file f2 on f2.bfid = migration.dst_bfid " \
              "left join bad_file b1 on b1.bfid = f1.bfid " \
              "left join bad_file b2 on b2.bfid = f2.bfid " \
              "where f2.volume = volume.id " \
              "  and volume.label = '%s' " \
              "order by f2.location_cookie;" % (v,)


        show_list = []
        if intf.source_only:
            res1a = db.query(q1a).getresult() #Get the results.
            show_list.append(res1a)
        elif intf.destination_only:
            res1b = db.query(q1b).getresult() #Get the results.
            show_list.append(res1b)
        else:
            #Get the results.
            res1a = db.query(q1a).getresult()
            res1b = db.query(q1b).getresult()

            for row in res1a:
                if row[4]:
                    #We found a source volume.
                    show_list.append(res1a)
                    break
            for row in res1b:
                if row[0]: #We found a destination volume.
                    show_list.append(res1b)
                    break

            if show_list == []:
                #If this volume has not been involved in
                # any migration, report that nothing
                # currently has been migrated.
                show_list.append(res1a)

        for rows in show_list:
            #migration type (MIGRATION, DUPLICATION or CLONING)
            mig_type = None

            #Output the header.
            print "%19s %1s%1s%1s %19s %1s%1s%1s %6s %6s %6s %6s" % \
                  ("src_bfid", "S", "D", "B",
                   "dst_bfid", "S", "D", "B",
                   "copied", "swapped", "checked", "closed")

            for row2 in rows:

                #Name these for easier reading.

                #source bfid
                if row2[0] == None:
                    src_bfid = ""
                else:
                    src_bfid = row2[0]
                #source bfid deleted status (in uppercase)
                if row2[1] == None:
                    src_del_status = ""
                else:
                    src_del_status = row2[1].upper()
                #source bfid bad status (in uppercase)
                if row2[2] == None:
                    src_bad_status = ""
                else:
                    src_bad_status = row2[2].upper()
                #source bfid duplication status
                if row2[3] == None:
                    src_dup_status = ""
                else:
                    src_dup_status = row2[3].upper()
                #destination bfid
                if row2[4] == None:
                    dst_bfid = ""
                else:
                    dst_bfid = row2[4]
                #destination bfid deleted status (in uppercase)
                if row2[5] == None:
                    dst_del_status = ""
                else:
                    dst_del_status = row2[5].upper()
                if row2[6] == None:
                    dst_bad_status = ""
                else:
                    dst_bad_status = row2[6].upper()
                #destination bfid duplication status
                if row2[7] == None:
                    dst_dup_status = ""
                else:
                    dst_dup_status = row2[7].upper()

                #Determine migration status.
                if row2[8]:
                    copied = "y"
                else:
                    copied = ""
                    exit_status = 1
                if row2[9]:
                    swapped = "y"
                else:
                    swapped = ""
                    exit_status = 1
                if row2[10]:
                    checked = "y"
                else:
                    checked = ""
                    exit_status = 1
                if row2[11]:
                    closed = "y"
                else:
                    closed = "" 
                    exit_status = 1

                line = "%19s %1s%1s%1s %19s %1s%1s%1s %6s %6s %6s %6s" % \
                       (src_bfid, src_dup_status,
                        src_del_status, src_bad_status,
                        dst_bfid, dst_dup_status,
                        dst_del_status, dst_bad_status,
                        copied, swapped, checked, closed)
                print line

                #We should only need to do this once.
                if not mig_type and len(rows) > 0:
                    src_vol = get_volume_from_bfid(src_bfid, db)
                    dst_vol = get_volume_from_bfid(dst_bfid, db)
                    mig_type = get_migration_type(src_vol, dst_vol, db)

            if mig_type:
                print "\n%s" % (mig_type,)

    return exit_status

def show_show(intf, db):
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

    #Get the results.
    res = db.query(q).getresult()

    print "%10s %s" % ("volume", "system inhibit")
    for row in res:
        print "%10s %s" % (row[0], row[1])

##########################################################################
        
#For duplication only.
def make_failed_copies(intf, db):
	MY_TASK = "MAKE_FAILED_COPIES"
	#Build the sql query.
	q = "select * from active_file_copying, volume, file " \
	    "where file.volume = volume.id " \
	    "      and remaining > 0 " \
	    "      and active_file_copying.bfid = file.bfid " \
	    "      and time < CURRENT_TIMESTAMP - interval '24 hours' " \
            "  --These 4 pnfs_id/pnfs_path checks remove failed original " \
            "  -- transfers from the output list. " \
            "      and file.pnfs_id is not NULL " \
            "      and file.pnfs_id != '' " \
            "      and file.pnfs_path is not NULL " \
            "      and file.pnfs_path != '' " \
	    "order by volume.id,time;"
	#Get the results.
	res = db.query(q).getresult()

	bfid_list = []
	for row in res:
		#row[0] is bfid
		#row[1] is count
		#row[2] is time

		#Loop over the remaining count to insert the bfid N times
		# into the bfid list to duplicate.
		for unused in range(1, int(row[1]) + 1):
			if row[1] > 0:
				#Limit this to those bfids with positive
				# remaing copies-to-be-made counts.
				bfid_list.append(row[0])

	#Loop over each file making a multiple copy each time.
        for bfid in bfid_list:
		#Do the duplication.
		exit_status = migrate([bfid], intf)

		if not exit_status:
			### The duplicatation was successfull.

			log(MY_TASK, "Decrementing the remaining count by " \
				     "one for bfid %s." % (bfid,))
			
			#Build the sql query.
			#Decrement the number of files remaining by one.
			### Note: What happens when this values reaches zero?
			q = "update active_file_copying " \
			    "set remaining = remaining - 1 " \
			    "where bfid = '%s'" % (bfid,)

			#Get the results.
			db.query(q)

			log(MY_TASK, "Removing the bfid from the migration " \
				     "table for bfid %s." % (bfid,))

			#Build the sql query.
			#Remove this file from the migration table.  We do
			# not want the source volume to look like it has
			# started to be migrated/duplicated.
			q = "delete from migration " \
			    "where src_bfid = '%s'" % (bfid,)

			#Get the results.
			db.query(q)

                #For debugging, do only one file.
                if debug:
                        log("limiting to one file in debug mode")
                        break
   
##########################################################################

def read_file(MY_TASK, src_bfid, src_path, tmp_path, volume,
	      location_cookie, deleted, encp, intf):

	log(MY_TASK, "copying %s %s %s" \
	    % (src_bfid, volume, location_cookie))

	if deleted == 'n' and not os.access(src_path, os.R_OK):
		error_log(MY_TASK, "%s %s is not readable" \
			  % (src_bfid, src_path))
		#continue
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
	if deleted == 'y':
		use_override_deleted = ["--override-deleted"]
		use_path = ["--get-bfid", src_bfid]
	else:
		use_override_deleted = []
		use_path = [src_path]
        if USE_THREADED_ENCP:
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
		       % (src_bfid, src_path, tmp_path))
	else:
		error_log(MY_TASK,
			  "failed to copy %s %s to %s, error = %s" \
			  % (src_bfid, src_path, tmp_path, encp.err_msg))
		return 1

	return 0

#Read a file from tape.  On success, return the tuple representing the
# job that will be sent to the write thread/process.
def copy_file(bfid, encp, intf, db):
	MY_TASK = "COPYING_TO_DISK"

	log(MY_TASK, "processing %s" % (bfid,))
	# get file info
	file_record = get_file_info(MY_TASK, bfid, db)
	if not file_record:
		error_log(MY_TASK, "%s does not exists" % (bfid,))
		return

	if debug:
		log(MY_TASK, `file_record`)

	# check if it has been copied and swapped
	is_it_copied = is_copied(bfid, db)
	dst_bfid = is_it_copied  #side effect
	is_it_swapped = is_swapped(bfid, db)

	#Define the directory for the temporary file on disk.
	tmp = temp_file(file_record)

	#Handle finding the name differently for deleted and non-deleted files.
	
	#Start with non-deleted files.
	if file_record['deleted'] == 'n':
		if is_it_copied and is_it_swapped:
			#Already copied.
			use_bfid = dst_bfid
                        alt_bfid = bfid
			use_file_record = None
		else:
			#Still need to copy.
			use_bfid = bfid
                        alt_bfid = dst_bfid
			use_file_record = file_record

                try:
                    src = pnfs_find(use_bfid, alt_bfid, file_record['pnfs_id'],
                                    file_record = use_file_record, intf = intf)
                except (OSError, IOError), msg:
                    src = None
                    if msg.errno == errno.ENOENT \
                           and is_it_copied and is_it_swapped:
                        #The file has been migrated, however the file has been
                        # deleted; removing the entry from pnfs for both.
                        # Prove this by checking the deleted status of the
                        # new copy.
                        fr2 = get_file_info(MY_TASK,
                                         dst_bfid, db)
                        if fr2 and fr2['deleted'] == 'y':
                            src = "deleted-%s-%s"%(bfid, tmp) # for debug
                        else:
                            raise sys.exc_info()[0], sys.exc_info()[1], \
                                  sys.exc_info()[2]

                    if not src:
                        import traceback
                        traceback.print_tb(sys.exc_info()[2])
                        error_log(MY_TASK, sys.exc_info()[0],
                                  sys.exc_info()[1],
                                  "%s %s %s %s is not a valid pnfs file" \
                                  % (file_record['label'],
                                     file_record['bfid'],
                                     file_record['location_cookie'],
                                     file_record['pnfs_id']))
                        
                        return

	#If the file has already been copied, swapped, checked and closed
	# handle this better than to put fear into the user with false
	# warning messags.  The most likely scenario for getting here is
	# that the user used --force on an already completed migration.
	elif file_record['deleted'] == 'y' and is_it_copied and is_it_swapped \
		 and is_checked(dst_bfid, db) and is_closed(dst_bfid, db):
		try:
			src = find_pnfs_file.find_pnfsid_path(
				file_record['pnfs_id'], dst_bfid,
				path_type = find_pnfs_file.FS)
		except (KeyboardInterrupt, SystemExit):
			raise sys.exc_info()[0], sys.exc_info()[1], \
                              sys.exc_info()[2]
		except OSError, msg:
			src = "deleted-%s-%s"%(bfid, tmp) # for debug

	#Lastly, we need to handle user deleted files by making up some
	# information.
	elif file_record['deleted'] == 'y' and \
		 (len(file_record['pnfs_id']) > 10 or \
                  len(file_record['pnfs_path']) > 1):
		log(MY_TASK, "%s %s %s is a DELETED FILE" \
		    % (file_record['bfid'], file_record['pnfs_id'],
		       file_record['pnfs_path']))
		src = "deleted-%s-%s"%(bfid, tmp) # for debug
		# do nothing more
        elif len(file_record['pnfs_id']) == 0 and \
                 len(file_record['pnfs_path']) == 0:
                # Can't migrate an empty/failed file.
                error_log(MY_TASK, "can not copy failed file %s" % (bfid,))
		return
	else:
		# what to do?
		error_log(MY_TASK, "can not copy %s" % (bfid,))
		return

	if debug:
		log(MY_TASK, "src:", src)
		log(MY_TASK, "tmp:", tmp)

	if dst_bfid:
		res = 0
		ok_log(MY_TASK, "%s has already been copied to %s" \
		       % (bfid, dst_bfid))
	else:
		if intf.use_disk_files:
			try:
				tfstat = os.stat(tmp)
			except (OSError, IOError), msg:
				error_log(MY_TASK, "can not find temporary file %s" % (tmp,))
				return
			if tfstat[stat.ST_SIZE] == file_record['size']:
				res = 0
			else:
				res = 1
		else:
                        res = read_file(MY_TASK, bfid, src, tmp,
					file_record['label'],
					file_record['location_cookie'],
					file_record['deleted'],
					encp, intf)
	if res:
		return
	
	pass_along_job = (bfid, src, tmp, file_record['file_family'],
			  file_record['storage_group'],
			  file_record['deleted'],
			  file_record['wrapper'])
	return pass_along_job

# copy_files(files) -- copy a list of files to disk and mark the status
# through copy_queue
def copy_files(thread_num, files, copy_queue, grab_lock, release_lock, intf):
	
	MY_TASK = "COPYING_TO_DISK"

	# if files is not a list, make a list for it
	if type(files) != type([]):
		files = [files]

	# get a db connection
	db = pg.DB(host=dbhost, port=dbport, dbname=dbname, user=dbuser)
		
	# get an encp
	name_ending = ""
	if thread_num:
		name_ending = "_%s" % (thread_num,)
	threading.currentThread().setName("READ%s" % (name_ending,))
	encp = encp_wrapper.Encp(tid = "READ%s" % (name_ending,))

	# copy files one by one
	for bfid in files:
		if grab_lock:  # and release_lock:
			grab_lock.acquire()
			release_lock.release()

		try:
			# Read the file from tape.
			pass_along_job = copy_file(bfid, encp, intf, db)
		except (KeyboardInterrupt, SystemExit):
                        raise sys.exc_info()[0], sys.exc_info()[1], \
                              sys.exc_info()[2]
		except:
			#We failed spectacularly!

			pass_along_job = None #Set this so we can continue.

			#Report the error so we can continue.
			exc_type, exc_value, exc_tb = sys.exc_info()
                        Trace.handle_error(exc_type, exc_value, exc_tb)
                        del exc_tb #avoid resource leaks
                        error_log(MY_TASK, str(exc_type),
                                  str(exc_value),
                                  " reading file %s: %s" \
                                  % (bfid, str(exc_value)))
			
		if pass_along_job:
			#If we succeeded, pass the job on to the write
			# process/thread.
			if debug:
				log(MY_TASK, "Passing job %s to write step." \
				    % (pass_along_job,))

                        copy_queue.put(pass_along_job, block = True)

			if debug:
				log(MY_TASK, "Done passing job.")

	# terminate the copy_queue
	log(MY_TASK, "no more to copy, terminating the copy queue")
	copy_queue.put(SENTINEL, block = True)

##########################################################################

# migration_file_family(ff) -- making up a file family for migration
def migration_file_family(bfid, ff, fcc, intf, deleted = 'n'):
        __pychecker__ = "unusednames=bfid,fcc" #Reserved for duplication.
        
	if deleted == 'y':
		return DELETED_FILE_FAMILY + MIGRATION_FILE_FAMILY_KEY
	else:
		if intf.file_family:
			return intf.file_family + MIGRATION_FILE_FAMILY_KEY
		else:
			return ff + MIGRATION_FILE_FAMILY_KEY

# normal_file_family(ff) -- making up a normal file family from a
#				migration file family
def normal_file_family(ff):
	return ff.replace(MIGRATION_FILE_FAMILY_KEY, '')

#Return True if the file_family has the pattern of a migration/duplication
# file.  False otherwise.
def is_migration_file_family(ff):
    if ff.find(MIGRATION_FILE_FAMILY_KEY) == -1:
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
        use_dirpath = pnfs.get_directory_name(file_record['pnfs_name0'])
    else:
        use_dirpath = pnfs.get_directory_name(filepath)

    #Get the pnfs specified libraries.
    dirs_to_try = []
    try:
        dirs_to_try.append(pnfs.get_enstore_fs_path(use_dirpath))
    except OSError:
        pass
    try:
        dirs_to_try.append(pnfs.get_enstore_pnfs_path(use_dirpath))
    except OSError:
        pass
    for dir_to_check in dirs_to_try:
        try:
            pnfs_libraries = pnfs.Tag().readtag("library", dir_to_check)[0].split(",")
            break #Found it!
        except OSError:
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

# compare_metadata(p, f) -- compare metadata in pnfs (p) and filedb (f)
def compare_metadata(p, f, pnfsid = None):
	if debug:
		p.show()
		log("compare_metadata():", `f`)
	if p.bfid != f['bfid']:
		return "bfid"
	if p.volume != f['external_label']:
		return "external_label"
	if p.location_cookie != f['location_cookie']:
		return "location_cookie"
	if long(p.size) != long(f['size']):
		return "size"
	if (pnfsid and f['pnfsid'] != pnfsid) or \
	   (not pnfsid and p.pnfs_id != f['pnfsid']):
		return "pnfsid"
	# some of old pnfs records do not have crc and drive information
	if p.complete_crc and long(p.complete_crc) != long(f['complete_crc']):
		return "crc"
	# do not check drive any more
	# if p.drive and p.drive != "unknown:unknown" and \
	#	p.drive != f['drive'] and f['drive'] != "unknown:unknown":
	#	return "drive"
	return None

# swap_metadata(bfid1, src, bfid2, dst) -- swap metadata for src and dst
#
# This got to be very paranoid.
#
# [1] check the meta data consistency
# [2] f[bfid2][pnfsid] = f[bfid1][pnfsid] # use old pnfsid
# [3] pnfsid = f[bfid1][pnfsid]           # save it
# [4] p[src] = p[dst]                     # copy pnfs layer 4
# [5] p[src][pnfsid] = pnfsid
#
# * return None if succeeds, otherwise, return error message
# * to avoid deeply nested "if ... else", it takes early error return
def swap_metadata(bfid1, src, bfid2, dst, db):
    MY_TASK = "SWAPPING_METADATA"

    # get its own file clerk client
    config_host = enstore_functions2.default_host()
    config_port = enstore_functions2.default_port()
    csc = configuration_client.ConfigurationClient((config_host,
                                                    config_port))
    fcc = file_clerk_client.FileClient(csc)

    # get all the file db metadata
    f1 = fcc.bfid_info(bfid1)
    if not e_errors.is_ok(f1):
        return "src_bfid: %s: %s" % (f1['status'][0], f1['status'][1])

    f2 = fcc.bfid_info(bfid2)
    if not e_errors.is_ok(f2):
        return "dst_bfid: %s: %s" % (f1['status'][0], f1['status'][1])

    #It is possible to migrate a duplicate.  Need to handle finding
    # the original bfid's file recored, if there is one.
    #
    #It is also possible to make multiple copies while migrating, need
    # to check bfid2 for this possibility.
    for mc_check_bfid in (bfid1, bfid2):
        original_reply = fcc.find_the_original(mc_check_bfid)
        f0 = {}
        if e_errors.is_ok(original_reply) \
               and original_reply['original'] != None \
               and original_reply['original'] != mc_check_bfid:
            f0 = fcc.bfid_info(original_reply['original'])
            if not e_errors.is_ok(f2):
                return "original bfid: %s: %s" % (f1['status'][0],
                                                  f1['status'][1])
        if f0:
            break #found our original.

    """
    #For trusted pnfs systems, there isn't a problem,
    # but for untrusted we need to set the effective
    # IDs to the owner of the file.
    #
    # If the source PNFS file has been deleted only do the
    # pnfs.File() instantiation; skip the euid/egid stuff to
    # avoid tracebacks.
    """


    # get all pnfs metadata - first the source file
    if f1['deleted'] == "no":
        try:
            # This version handles the seteuid() locking.
            p1 = File(src)
        except (KeyboardInterrupt, SystemExit):
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except (OSError, IOError), msg:
            return str(msg)
        except:
            exc, msg = sys.exc_info()[:2]
            return str(msg)
    else:
        # What do we need an empty File class for?
        p1 = pnfs.File(src)

    # get all pnfs metadata - second the destination file
    try:
        p2 = File(dst)
    except (KeyboardInterrupt, SystemExit):
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
    except (OSError, IOError), msg:
        return str(msg)
    except:
        exc, msg = sys.exc_info()[:2]
        return str(msg)

    ##################################################################
    #Handle deleted files specially.
    if f1['deleted'] == "yes":
        res = ''
        # copy the metadata
        finfo = copy.copy(f1)
        if e_errors.is_ok(f1):
            del finfo['status']
            finfo['bfid'] = bfid2
            #finfo['location_cookie'] = pf2.location_cookie
            finfo['location_cookie'] = f2['location_cookie']
            res2 = fcc.modify(finfo)
            if not e_errors.is_ok(res2['status']):
                res = res2['status'][1]
        else:
            res = "source file info missing"
        return res
    ###################################################################

    #If we happen to be migrating a multiple copy (which is only allowed
    # with --force), then we need to do not modify layers 1 and 4.
    is_migrating_multiple_copy = False

    # check if the metadata are consistent
    res = compare_metadata(p1, f1)
    # deal with already swapped metadata
    if res == "bfid":
        res = compare_metadata(p1, f2)
        if res == "bfid" and f0:
            #Compare original, if applicable.
            res = compare_metadata(p1, f0)
            if not res:
                #Note: Don't update layers 1 and 4!
                is_migrating_multiple_copy = True
        else:
            if not res:
                #The metadata has already been swapped.
                return None
    if res:
        return "[1] metadata %s %s are inconsistent on %s" % (bfid1, src, res)

    if not p2.bfid and not p2.volume:
        #The migration path has already been deleted.  There is
        # no file to compare with.
        pass
    else:
        res = compare_metadata(p2, f2)
        # deal with already swapped file record
        if res == "pnfsid":
            res = compare_metadata(p2, f2, p1.pnfs_id)
        elif res == "bfid" and f0:
            res = compare_metadata(p2, f0, p1.pnfs_id)
        if res:
            return "[2] metadata %s %s are inconsistent on %s" \
                   % (bfid2, dst, res)

    # cross check
    err_msg = ""
    if f1['size'] != f2['size']:
        err_msg = "%s and %s have different size" % (bfid1, bfid2)
    elif f1['complete_crc'] != f2['complete_crc']:
        err_msg = "%s and %s have different crc" % (bfid1, bfid2)
    elif f1['sanity_cookie'] != f2['sanity_cookie']:
        err_msg = "%s and %s have different sanity_cookie" % (bfid1, bfid2)
    if err_msg:
        if f2['deleted'] == "yes" and not is_swapped(bfid1, db):
            log(MY_TASK,
                "undoing migration of %s to %s do to error" % (bfid1, bfid2))
            undo_log(bfid1, bfid2, db)
        return err_msg

    ### swapping the Enstore DB metadata
    m1 = {'bfid': bfid2, 'pnfsid':f1['pnfsid'], 'pnfs_name0':f1['pnfs_name0']}
    res = fcc.modify(m1)
    if not e_errors.is_ok(res['status']):
        return "failed to change pnfsid for %s" % (bfid2,)

    #If we are migrating a multiple_copy/duplicate, we do not update the
    # layer 1 and layer 4 information.
    if not is_migrating_multiple_copy:
        ### swapping the PNFS layer metadata
        p1.volume = f2['external_label'] # p2.volume
        p1.location_cookie = f2['location_cookie'] # p2.location_cookie
        p1.bfid = f2['bfid'] # p2.bfid
        p1.drive = f2['drive'] # p2.drive
        p1.complete_crc = f2['complete_crc'] # p2.complete_crc
        # should we?
        # the best solution is to have encp ignore sanity check on file_family
        # p1.file_family = p2.file_family

        # check if p1 is writable - do this with euid/egid the same as the
        # the owner of the file
        try:
            src_stat = file_utils.get_stat(src)
        except (KeyboardInterrupt, SystemExit):
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except (OSError, IOError), msg: # Anticipated errors.
            return "%s is not accessable: %s" % (src, msg)
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
                make_writeable(src)
            except (KeyboardInterrupt, SystemExit):
                raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
            except (OSError, IOError), msg:  #Anticipated errors.
                return "%s is not writable as %s: %s" \
                       % (src, os.geteuid(), str(msg))
            except:
                exc, msg = sys.exc_info()[:2]
                return str(msg)


        # now perform the writes to the file's layer 1 and layer 4
        if f1['deleted'] == "no":
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
                os.chmod(src, src_stat[stat.ST_MODE])
            except (OSError, IOError), msg:
                error_log("Unable to reset persisions for %s to %s" % \
                          (src, src_stat[stat.ST_MODE]))

    # check it again
    p1 = pnfs.File(src)
    f1 = fcc.bfid_info(bfid2)
    res = compare_metadata(p1, f1)
    if res == "bfid" and f0:
        # Handle the possibility of migrating a multiple copy.
        res = compare_metadata(p1, f0)
    if res:
        return "swap_metadata(): %s %s has inconsistent metadata on %s"%(bfid2, src, res)

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
		mig_stat = os.stat(mig_path)
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
        if USE_THREADED_ENCP:
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
                        "--ignore-fair-share", "--threaded"]
	#Override these tags to use the original values from the source tape.
	# --override-path is used to specify the correct path to be used
	# in the wrappers written with the file on tape, since this path
	# should match the original path not the temporary migration path
	# that the rest of the encp process will need to worry about.
	dst_options = ["--storage-group", sg, "--file-family", ff,
		       "--file-family-wrapper", wrapper,
                       "--library", libraries,
		       "--override-path", src_path]

	argv = ["encp"] + use_verbose + encp_options + use_priority + \
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
		log(MY_TASK, "failed to copy %s %s %s error = %s"
                    % (src_bfid, tmp_path, mig_path, encp.err_msg))
        elif res == 1:
                log(MY_TASK, "failed to copy %s %s %s ... (RETRY)"
                    % (src_bfid, tmp_path, mig_path))
                # delete the target and retry once
                try:
                        file_utils.remove(mig_path)
                except (OSError, IOError), msg:
                        error_log(MY_TASK, "failed to remove %s as " \
                              "(uid %s, gid %s): %s" % \
                              (mig_path, os.geteuid(), os.getegid(), str(msg)))
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
                                error_log(MY_TASK, "failed to remove %s as " \
                                          "(uid %s, gid %s): %s" % \
                                          (mig_path, os.geteuid(),
                                           os.getegid(), str(msg)))
                        return 1
        elif res:
		#Some unknown error occured.
		log(MY_TASK, "failed to copy %s %s %s error = %s"
                    % (src_bfid, tmp_path, mig_path, encp.err_msg))

        else:
                # log success of coping
                ok_log(MY_TASK, "%s %s is copied to %s" % \
                       (src_bfid, tmp_path, mig_path))
		
	if debug:
		log(MY_TASK, "written to tape %s %s %s"
		    % (src_bfid, tmp_path, mig_path))

	return 0


def write_new_file(job, encp, fcc, intf, db):
	MY_TASK = "COPYING_TO_TAPE"

	#Get information about the files to copy and swap.
	(src_bfid, src_path, tmp_path, ff, sg, deleted, wrapper) = job

	if debug:
		log(MY_TASK, `job`)

	# check if it has already been copied
	is_it_copied = is_copied(src_bfid, db)
	dst_bfid = is_it_copied  #side effect: this is also the dst bfid
	has_tmp_file = False
        file_record = {} #empty record
        wrote_multiple_copies = False #set true if mupltiple copies written.
        mc_dst_bfids = [] #list of bfids of multiple copies written.
        if deleted == 'y':
            file_record['deleted'] = "yes"  #Minimally, set the deleted status.
        elif deleted == 'n':
            file_record['deleted'] = "no"   #Minimally, set the deleted status.
        else:
            file_record['deleted'] = "unknown"  #Should never happen.
	if is_it_copied:
		ok_log(MY_TASK, "%s has already been copied to %s" \
		       % (src_bfid, dst_bfid))

		# We need to be this draconian if we had to restart the
		# migration processes.
		file_record = fcc.bfid_info(dst_bfid, timeout = 10, retry = 4)
		if not e_errors.is_ok(file_record):
			error_log(MY_TASK,
				  "no file record found(%s)" % (dst_bfid,))
			return

                if file_record['deleted'] == "no":
		        try:
			        mig_path = find_pnfs_file.find_pnfsid_path(
                                    file_record['pnfsid'], dst_bfid,
                                    file_record = file_record)
		        except (OSError, IOError), msg:
			        mig_path = migration_path(src_path, deleted)
                else:
                        mig_path = migration_path(src_path, deleted)
	else:
		mig_path = migration_path(src_path, deleted)
                #Try and catch situations were an error left a zero
		# length file in the migration spool directory.  We
		# don't want to 'migrate' this wrong file to tape.
		try:
			#We want the size in layer 4, since large files
			# store a 1 for the size in pnfs.
			src_size = long(
				pnfs.get_layer_4(src_path).get('size',
							       None))
		except (OSError, IOError):
			src_size = None
		except (TypeError):
			if deleted == 'y':
				#If the file is deleted, obtain the size from
				# the Enstore DB.
				file_record = fcc.bfid_info(src_bfid, timeout = 10,
							    retry = 4)
				if e_errors.is_ok(file_record):
					src_size = file_record['size']
				else:
					src_size = None
			else:
				src_size = None
		try:
			tmp_size = long(
				os.stat(tmp_path)[stat.ST_SIZE])
		except (OSError, IOError):
			#We likely get here when the file is already
			# removed from the spooling directory.
			tmp_size = None			
		if src_size != tmp_size:
			error_log(MY_TASK,
			       "size check mismatch %s(current %s, temp %s)" \
				  % (src_bfid, src_size, tmp_size))
			try:
				log(MY_TASK,
				    "removing %s" % (tmp_path,))
				file_utils.remove(tmp_path)
			except (OSError, IOError), msg:
				log(MY_TASK, "error removing %s: %s" \
				    % (tmp_path, str(msg)))
			return

                #The library value can consist of a comma seperated list
                # of libraries, though in most cases there will be just one.
                # There are some 'odd' cases that use_libraries() handles
                # for us.
                libraries = use_libraries(src_bfid, src_path, file_record,
                                          db, intf)
                if libraries == None:
                        #use_libraries() logs its own errors.
                        return
                if len(libraries.split(",")) > 1:
                        wrote_multiple_copies = True
                #The same goes for file families.  Migration and duplication
                # vary greatly with respect to the file family.  There are
                # some 'odd' cases that migration_file_family() handles for us.
                ff = migration_file_family(src_bfid, ff, fcc, intf, deleted)

		## At this point src_path points to the original file's
		## location in pnfs, tmp_path points to the temporary
		## location on disk and mig_path points to the
		## migration path in pnfs where the new copy is
		## written to.

		rtn_code = write_file(MY_TASK, src_bfid, src_path,
				      tmp_path, mig_path,
				      libraries, sg, ff, wrapper,
				      deleted, encp, intf)
		if rtn_code:
			return

                
	# Get bfid (and layer 4) of copied file.  We need these values
	# regardless if the file was already copied, or it was
	# just copied.
	if not is_it_copied:
		pf2 = pnfs.File(mig_path)
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
                                log_copied(src_bfid, cur_dst_bfid, db)

        else:
                if os.path.exists(tmp_path):
                        #If the file is already copied, but the temporary on
                        # disk file still remains, flag it for deletion.
                        has_tmp_file = True

                #Need to get this list for re-runs of the migration.
                mc_dst_bfids = get_multiple_copy_bfids(dst_bfid, db)

	keep_file = False
	## Perform modifications to the file metadata.  It does
	## not need to be an actual swap (duplication isn't) but
	# some type of modification is done.
	MY_TASK2 = "SWAPPING_METADATA"  #Switch to swapping task.
	log(MY_TASK2, "swapping %s %s %s %s" % \
	    (src_bfid, src_path, dst_bfid, mig_path))
	if not is_swapped(src_bfid, db):

                for cur_dst_bfid in [dst_bfid] + mc_dst_bfids:
                        res = swap_metadata(src_bfid, src_path,
                                            cur_dst_bfid, mig_path, db)

                        if not res:
                                ok_log(MY_TASK2,
                                       "%s %s %s %s have been swapped" \
                                       % (src_bfid, src_path, cur_dst_bfid,
                                          mig_path))
                                log_swapped(src_bfid, cur_dst_bfid, db)
                        if res:
                                error_log(MY_TASK2,
                                          "%s %s %s %s failed due to %s" \
                                          % (src_bfid, src_path, cur_dst_bfid,
                                             mig_path, res))
                                keep_file = True
	else:
		ok_log(MY_TASK2, "%s %s %s %s have already been swapped" \
		       % (src_bfid, src_path, dst_bfid, mig_path))

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
		except:
			exc, msg = sys.exc_info()[:2]
			error_log(MY_TASK,
                                  "failed to remove temporary file %s as " \
                                  "(uid %s, gid %s): %s" \
				  % (tmp_path, os.geteuid(), os.getegid(),
                                     str(msg)))
			pass

	#If we had an error while swapping, don't return the dst_bfid.
	if keep_file:
		return
	else:
		return dst_bfid, mc_dst_bfids

# write_new_files() -- second half of migration, driven by copy_queue
def write_new_files(thread_num, copy_queue, scan_queue, intf):
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
	name_ending = ""
	if thread_num:
		name_ending = "_%s" % (thread_num,)
	threading.currentThread().setName("WRITE%s" % (name_ending,))
	encp = encp_wrapper.Encp(tid = "WRITE%s" % (name_ending,))

	#Get a file clerk client.
	config_host = enstore_functions2.default_host()
	config_port = enstore_functions2.default_port()
	csc = configuration_client.ConfigurationClient(
		(config_host, config_port))
	fcc = file_clerk_client.FileClient(csc)

	while 1:

		if debug:
			log(MY_TASK, "Getting next job for write.")

                job = copy_queue.get(block = True)
                    
                if not job:
                    # We are done.  Nothing more to do.
                    break
		if debug:
			log(MY_TASK, "Received job %s for write." % \
			    (job,))

		# Make the mew copy.
		try:
			rtn = write_new_file(job, encp, fcc, intf, db)
                        if rtn:  #Success.
                                dst_bfid = rtn[0]
                                mc_dst_bfids = rtn[1]
                        else:  #Failure
                                dst_bfid = None
                                mc_dst_bfids = []
		except (KeyboardInterrupt, SystemExit):
                        raise sys.exc_info()[0], sys.exc_info()[1], \
                              sys.exc_info()[2]
                except:
			#We failed spectacularly!

			dst_bfid = None #Set this so we can continue.
                        mc_dst_bfids = []

			#Report the error so we can continue.
			exc_type, exc_value, exc_tb = sys.exc_info()
                        Trace.handle_error(exc_type, exc_value, exc_tb)
                        del exc_tb #avoid resource leaks
                        error_log(MY_TASK, str(exc_type),
                                  str(exc_value),
                                  " writing file %s for %s" \
                                  % (dst_bfid, job))

		#At this point dst_bfid equals None if there was an error.
		
		#Tell the final_scan() thread the next file.
		if intf.with_final_scan and dst_bfid:
			(src_bfid, src_path, tmp_path, ff, sg, deleted, wrapper) = job
			#Even after these files have been "swapped," the
			# original filename is still the pnfsid we want.
			if deleted == 'n':
				pnfsid = pnfs.get_pnfsid(src_path)
			else:
				pnfsid = None
			scan_job = (src_bfid, dst_bfid, mc_dst_bfids, pnfsid, src_path, deleted)
                        #We don't flag the condition variable here, because
                        # scans done with --with-final-scan wait until all
                        # writes are done.
			scan_queue.put(scan_job, block = True)


	if intf.with_final_scan:
		log(MY_TASK, "no more to copy, terminating the scan queue")

                #Since we are done, flag the condition variable.
		scan_queue.put(SENTINEL, block = True)
	
##########################################################################

## src_path doesn't need to be an actuall path in pnfs.  It could be 
## "--get-bfid <bfid>" or --get
def scan_file(MY_TASK, dst_bfid, src_path, dst_path, deleted, intf, encp):
	open_log(MY_TASK, "verifying", dst_bfid, src_path, '...')
	
	## Build the encp command line.
	if intf.priority:
		use_priority = ["--priority", str(intf.priority)]
	else:
		use_priority = ["--priority", str(ENCP_PRIORITY)]
	if deleted == 'y':
		use_override_deleted = ["--override-deleted"]
	else:
		use_override_deleted = []
	if intf.use_volume_assert or USE_VOLUME_ASSERT:
		use_check = ["--check"] #Use encp to check the metadata.
	else:
		use_check = []
        if deleted == 'y':
                use_src_path = src_path.split()
        else:
                use_src_path = [src_path]

	encp_options = ["--delayed-dismount", "1", "--ignore-fair-share",
			"--threaded", "--bypass-filesystem-max-filesize-check"]
	argv = ["encp"] + encp_options + use_priority + use_override_deleted \
	       + use_check + use_src_path + [dst_path]

        if debug:
		cmd = string.join(argv)
		log(MY_TASK, "cmd =", cmd)

	#Read the file.
	try:
		res = encp.encp(argv)
	except:
		exc, msg, tb = sys.exc_info()
		import traceback
		traceback.print_tb(tb)
		print exc, msg
		res = 1
	
	if res == 0:
		close_log("OK")
		ok_log(MY_TASK, dst_bfid, src_path)
	else: # error
		close_log("ERROR")
		error_log(MY_TASK, "failed on %s %s error = %s"
			  % (dst_bfid, src_path, encp.err_msg))
		return 1

	return 0


# Return the actual filename and the filename for encp.  The filename for
# encp may not be a real filename (i.e. --get-bfid <bfid>).
def get_filenames(MY_TASK, dst_bfid, pnfs_id, likely_path, deleted):

        if deleted == 'y':
                use_path = "--override-deleted --get-bfid %s" \
                           % (dst_bfid,)
                pnfs_path = likely_path #Is anything else more correct?
        else:
                try:
                        # get the real path
                        pnfs_path = find_pnfs_file.find_pnfsid_path(
                                pnfs_id, dst_bfid,
                                likely_path = likely_path,
                                path_type = find_pnfs_file.FS)
                except (KeyboardInterrupt, SystemExit):
                        raise sys.exc_info()[0], sys.exc_info()[1], \
                              sys.exc_info()[2]
                except:
                        exc_type, exc_value, exc_tb = sys.exc_info()
                        Trace.handle_error(exc_type, exc_value, exc_tb)
                        del exc_tb #avoid resource leaks
                        error_log(MY_TASK, str(exc_type),
                                  str(exc_value),
                                  " %s %s is not a valid pnfs file" \
                                  % (
                                #vol,
                                     dst_bfid,
                                     #location_cookie,
                                     pnfs_id))
                        return (None, None)

                if type(pnfs_path) == type([]):
                        pnfs_path = pnfs_path[0]

                use_path = pnfs_path

        return (pnfs_path, use_path)

def cleanup_after_scan(MY_TASK, mig_path, src_bfid, fcc, db):
	try:
		# rm the migration path.  It could be argued, that if we
		# do this that we don't need to explicitly mark the bfid
		# deleted above, but there is the delay between delfile
		# running for that to happen.  It's cleaner to do both.
		os.stat(mig_path)
		try:
			#If the file still exists, try deleting it.
			nullify_pnfs(mig_path)
			file_utils.remove(mig_path)
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
			# exists, there is now problem.
			pass
		else:
			error_log(MY_TASK,
				  "migration path %s was not deleted: %s" \
                                  % (mig_path, str(msg)))
			return 1


	# make sure the original is marked deleted
	f = fcc.bfid_info(src_bfid)
	if e_errors.is_ok(f['status']) and f['deleted'] != 'yes':
		rtn_code = mark_deleted(MY_TASK, src_bfid, fcc, db)
		if rtn_code:
			#Error occured.
			return 1

def final_scan_file(MY_TASK, src_bfid, dst_bfid, pnfs_id, likely_path, deleted,
		    is_multiple_copy, fcc, encp, intf, db):
	ct = is_checked(dst_bfid, db)
	if not ct:
		#log(MY_TASK, "start checking %s %s"%(dst_bfid, src))

                (pnfs_path, use_path) = get_filenames(
                    MY_TASK, dst_bfid, pnfs_id, likely_path, deleted)

                # make sure the path is NOT a migration path
                if pnfs_path == None or is_migration_path(pnfs_path):
                        error_log(MY_TASK,
                                  'none swapped file %s' % \
                                  (pnfs_path))
                        return 1

		rtn_code = scan_file(
			MY_TASK, dst_bfid, use_path, "/dev/null",
			deleted, intf, encp)
		if rtn_code:
                    return 1
                else:
                    #Log the file as having been checked/scanned.
                    log_checked(src_bfid, dst_bfid, db)

		mig_path = migration_path(pnfs_path)
	else:
		ok_log(MY_TASK, dst_bfid, "is already checked at", ct)
		# make sure the migration path has been removed
		mig_path = migration_path(likely_path)

        if not is_multiple_copy:
		#cleanup_after_scan() reports its own errors.  Only do this
                # for orignal destination copies, not any of its possible
                # multplie copies.
		return cleanup_after_scan(MY_TASK, mig_path, src_bfid, fcc, db)

# final_scan() -- last part of migration, driven by scan_queue
#   read the file as user to reasure everything is fine
def final_scan(thread_num, scan_queue, intf):
	MY_TASK = "FINAL_SCAN"

	if not USE_THREADS:
		#We need to delay starting this thread (only for processes)
		# until after the fork().
		scan_queue.start_waiting()
	
	# get its own file clerk client
	config_host = enstore_functions2.default_host()
	config_port = enstore_functions2.default_port()
	csc = configuration_client.ConfigurationClient((config_host,
							config_port))
	fcc = file_clerk_client.FileClient(csc)

	#get a database connection
	db = pg.DB(host=dbhost, port=dbport, dbname=dbname, user=dbuser)

	# get an encp
	name_ending = ""
	if thread_num:
		name_ending = "_%s" % (thread_num,)
	threading.currentThread().setName("FINAL_SCAN%s" % (name_ending,))
	encp = encp_wrapper.Encp(tid = "FINAL_SCAN%s" % (name_ending,))

	#Loop over the files ready for scanning.
	job = scan_queue.get(block = True)
	while job:
		(src_bfid, dst_bfid, mc_dst_bfids, pnfs_id, likely_path, deleted) = job
                
                #If the destination file has multiple copies, we need to
                # check them all.  We need to scan any multiple copies first,
                # then when they are all fine we can try the original.
                for cur_dst_bfid in mc_dst_bfids + [dst_bfid]:
                        #Avoid having final_scan_file() calling find_copies().
                        # Pass this information along.
                        if cur_dst_bfid == dst_bfid:
                                is_multiple_copy = False
                        else:
                                is_multiple_copy = True
                    
                        try:
                                final_scan_file(MY_TASK, src_bfid,
                                                cur_dst_bfid,
                                                pnfs_id, likely_path, deleted,
                                                is_multiple_copy,
                                                fcc, encp, intf, db)
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
                                          " scanning file %s for %s" \
                                          % (cur_dst_bfid, job))
                                break


		#Get the next file.
		job = scan_queue.get(block = True)

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

	# get a db connection
	db = pg.DB(host=dbhost, port=dbport, dbname=dbname, user=dbuser)

	# get an encp
	threading.currentThread().setName('FINAL_SCAN')
	encp = encp_wrapper.Encp(tid='FINAL_SCAN')
	volume_assert = volume_assert_wrapper.VolumeAssert(tid='FINAL_SCAN')


	log(MY_TASK, "verifying volume", vol)

	v = vcc.inquire_vol(vol)
	if v['status'][0] != e_errors.OK:
		error_log(MY_TASK, "failed to find volume", vol)
		return 1

	# make sure the volume is ok to scan (check system_inhibit 0)
	if v['system_inhibit'][0] != 'none':
		error_log(MY_TASK, 'volume %s is "%s"'%(vol, v['system_inhibit'][0]))
		return 1

	# make sure this is a migration volume
	sg, ff, wp = string.split(v['volume_family'], '.')
	if migrated_from(vol, db): # or getattr(intf, "force", None):
		#We get here if the volume is a destination volume that has
		# other tapes migrated/duplicated to it.
		pass
	elif not is_migration_file_family(ff):
		error_log(MY_TASK, "%s is not a %s volume" %
			  (vol, MIGRATION_NAME.lower()))
		return 1

	#Verify that the sysetm_inhibit 1 is in a valid state too.
	if (v['system_inhibit'][1] != 'full' and \
		v['system_inhibit'][1] != 'none' and \
		v['system_inhibit'][1] != 'readonly') \
		and is_migrated_by_dst_vol(vol, intf, db):
		error_log(MY_TASK, 'volume %s is "%s"'%(vol, v['system_inhibit'][1]))
		return 1
	#Warn if the volume about to be scanned is not full.  Scan a non-
	# full tape will not allow future migration files to be written
	# onto it (without intervention anyway).
	if v['system_inhibit'][1] != 'full':
		log(MY_TASK, 'volume %s is not "full"'%(vol), "... WARNING")
	#If necessary set the system_inhibit_1 to readonly.  Leave "full"
	# alone, but change the others.
	if v['system_inhibit'][1] != "readonly" and \
	       v['system_inhibit'][1] != 'full':
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
			import traceback
			traceback.print_tb(tb)
			print exc, msg
			local_error = local_error + 1
			return local_error

		if res == 0:
			#close_log("OK")
			ok_log(MY_TASK, vol)
			#This is a dictionary, keyed by location cookie of
			# any errors that occured reading the files.
			assert_errors = volume_assert.err_msgs[0]['return_file_list']
		else: # error
			#close_log("ERROR")
			error_log(MY_TASK, "failed on %s error = %s"
				  % (vol, volume_assert.err_msgs[0]['status']))
			local_error = local_error + 1
			return local_error


	q = "select bfid, pnfs_id, pnfs_path, src_bfid, location_cookie, deleted  \
		from file, volume, migration \
		where file.volume = volume.id and \
			volume.label = '%s' and \
			dst_bfid = bfid \
		order by location_cookie;"%(vol)
	query_res = db.query(q).getresult()

	# Determine the list of files that should be scanned.
	for r in query_res:
		dst_bfid, pnfs_id, likely_path, src_bfid, location_cookie, deleted = r
		
		st = is_swapped(src_bfid, db)
		if not st:
			error_log(MY_TASK,
				  "%s %s has not been swapped" \
				  % (src_bfid, dst_bfid))
			local_error = local_error + 1
			continue

		#If the user deleted the files, require --with-deleted be
		# used on the command line.
		if deleted == 'y' and not intf.with_deleted:
			log(MY_TASK,
			    "Since migration %s was deleted." \
			    % (dst_bfid,))
			continue
		elif deleted == 'y' and intf.with_deleted:
			pass #Just use likely_path; the file is deleted anyway.
		else:
			#Make sure we have the admin path.
			try:
				likely_path = find_pnfs_file.find_pnfsid_path(
					pnfs_id, dst_bfid,
					likely_path = likely_path,
					path_type = find_pnfs_file.FS)
			except (OSError, IOError), msg:
				local_error = local_error + 1	
				error_log(MY_TASK, "Unable to determine path:",
					  dst_bfid, str(msg))
				continue

			if not is_expected_volume(
				MY_TASK, vol, likely_path, fcc):
				#Error message reported from
				# is_expected_volume().
				local_error = local_error + 1
				continue

		#If we are using volume_assert, check what the assert returned.
		if intf.use_volume_assert or USE_VOLUME_ASSERT:
			if not e_errors.is_ok(assert_errors[location_cookie]):
				error_log(MY_TASK,
					  "assert of %s %s:%s failed" % \
					  (dst_bfid, vol, location_cookie))
				local_error = local_error + 1
				continue
			else:
				log(MY_TASK,
				    "assert of %s %s:%s succeeded" % \
				    (dst_bfid, vol, location_cookie))

                #We need to tell final_scan_file() if the file is a multiple
                # or not.
                if get_multiple_copy_bfids(dst_bfid, db):
                        is_multiple_copy = True
                else:
                        is_multiple_copy = False

		## Scan the file by reading it with encp.
		## Note: if we are using volume assert, then final_scan_file()
		##       uses --check with the encp to avoid redundant
		##       reading of the file.
		rtn_code = final_scan_file(MY_TASK, src_bfid, dst_bfid,
					   pnfs_id, likely_path, deleted,
                                           is_multiple_copy,
					   fcc, encp, intf, db)
		if rtn_code:
			local_error = local_error + 1
			continue

                # If we get here, then the file has been scaned.  Consider
		# it closed too.
		ct = is_closed(dst_bfid, db)
		if not ct:
			log_closed(src_bfid, dst_bfid, db)
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
		vcc.touch(i)
		# log history closed
                log_history_closed(i, vol, db)
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

# migrate(bfid_list): -- migrate a list of files
def migrate(bfids, intf):
	global errors

	errors = 0

        #Limit the number of processes/threads if only one or two files
        # is in the list.
	if PARALLEL_FILE_TRANSFER:
		use_proc_limit = min(PROC_LIMIT, len(bfids))
	else:
		use_proc_limit = min(1, len(bfids))
	
	i = 0
	#Make a list of PROC_LIMIT length.  Each element should
	# itself be an empty list.
	#
	# Don't do "[[]] * PROC_LIMIT"!!!  That will only succeed
	# in creating PROC_LIMIT references to the same list.
	use_bfids_lists = []
	while i < use_proc_limit:
		use_bfids_lists.append([])
		i = i + 1

	#Put each bfid in one of the bfid lists.  This should resemble
	# a round-robin effect.
	i = 0
	for bfid in bfids:
		use_bfids_lists[i].append(bfid)
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
	if intf.use_volume_assert or USE_VOLUME_ASSERT:
		scan_queue = MigrateQueue(DEFUALT_QUEUE_SIZE,
                                          notify_every_time = False)
		scan_queue.debug = debug

	#For each list of bfids start the migrations.
	for i in range(len(use_bfids_lists)):
		bfid_list = use_bfids_lists[i]
		
	        #Get new queues for each set of processes/threads.
		copy_queue = MigrateQueue(DEFUALT_QUEUE_SIZE)
		copy_queue.debug = debug
		if not (intf.use_volume_assert or USE_VOLUME_ASSERT):
			#Get new queues for each set of processes/threads.
			scan_queue = MigrateQueue(DEFUALT_QUEUE_SIZE,
                                                  notify_every_time = False)
			scan_queue.debug = debug

		#Set the global proceed_number variable.
		set_proceed_number(bfid_list, copy_queue, scan_queue, intf)

		# Start the reading in parallel.
		run_in_parallel(copy_files,
		       (i, bfid_list, copy_queue,
			sequential_locks[i],
			sequential_locks[(i + 1) % use_proc_limit],
                        intf),
		       my_task = "COPY_TO_DISK",
		       on_exception = (handle_process_exception,
				       (copy_queue, SENTINEL)))

		# Start the writing in parallel
		run_in_parallel(write_new_files,
		       (i, copy_queue, scan_queue, intf),
		       my_task = "COPY_TO_TAPE",
		       on_exception = (handle_process_exception,
				       (scan_queue, SENTINEL)))

		# Only the parent should get here.

		#If we are scanning too, start the scan in parallel.
		if intf.with_final_scan and \
		       not (intf.use_volume_assert or USE_VOLUME_ASSERT):
			run_in_parallel(final_scan,
					(i, scan_queue, intf),
					my_task = "FINAL_SCAN")

	#If using volume_assert, we only want one call to final_scan():
	if intf.with_final_scan and \
	       (intf.use_volume_assert or USE_VOLUME_ASSERT):
		run_in_parallel(final_scan,
				(i, scan_queue, intf),
				my_task = "FINAL_SCAN")

	done_exit_status = wait_for_parallel()

	errors = done_exit_status + errors
	return errors

##########################################################################

#Can be used for threads too.  It simply sends the value in write_value
# which should be a SENTINEL value to the queue.
def handle_process_exception(queue, write_value):

    try:
        queue.put(write_value)
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
	for i in res:
		from_list.append(i[0])

	return from_list
	
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
	     "  and file.pnfs_id != '' -- skip failed files "\
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

##########################################################################

#Note: fcc used only for duplicate.py version of this function.
def is_expected_volume(MY_TASK, vol, likely_path, fcc):
	__pychecker__ = "unusednames=fcc"
	
	# make sure the volume is the same
	pf = pnfs.File(likely_path)
	pf_volume = getattr(pf, "volume", None)
	if pf_volume == None or pf_volume != vol:
		error_log(MY_TASK,
			  'wrong volume %s (expecting %s)' \
			  % (pf_volume, vol))
		return False  #Match not found.

	return True  #Match found.
		
##########################################################################
	
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

	# check if vol is set to "readonly". If not, set it.
	# check more
	v = vcc.inquire_vol(vol)
	if v['status'][0] != e_errors.OK:
		error_log(MY_TASK, 'volume %s does not exist'%vol)
		return 1
	if v['system_inhibit'][0] != 'none':
		error_log(MY_TASK, vol, 'is', v['system_inhibit'][0])
		return 1
	#If volume is migrated, report that it is done and stop.
	if enstore_functions2.is_migrated_state(v['system_inhibit'][1]) \
           and is_migrated_by_src_vol(vol, intf, db,
                                      checked = 0, closed = 0) \
	       and not getattr(intf, "force", None):
		log(MY_TASK, vol, "is already", v['system_inhibit'][1])
		return 0
	#Make sure the library exists.
	library_fullname = v['library'] + ".library_manager"
	lib_dict = csc.get(library_fullname)
	if not e_errors.is_ok(lib_dict):
		error_log("library %s does not exist" % (library_fullname,))
		return 1

        # Do not duplicate multiple copy tapes.  We want to duplicate just
        # the originals.
        file_family = volume_family.extract_file_family(v["volume_family"])
        if re.compile(".*_copy_[1-9]*$").match(file_family) != None \
               and not intf.force:
                error_log("%s is a multiple copy volume" % (vol,))
		return 1

	# now try to copy the file one by one
	if intf.with_deleted:
		use_deleted_sql = "or deleted = 'y'"
	elif intf.force:
		use_deleted_sql = "or (deleted = 'y' and migration.dst_bfid is not NULL)"
	else:
		use_deleted_sql = "or migration.dst_bfid is not NULL"
	if intf.skip_bad:
		use_skip_bad = "and bad_file.bfid is NULL"
	else:
		use_skip_bad = ""
	q = "select file.bfid,bad_file.bfid,file.pnfs_path from file " \
	    "left join bad_file on bad_file.bfid = file.bfid " \
	    "left join migration on migration.src_bfid = file.bfid " \
	    "join volume on file.volume = volume.id " \
	    "where file.volume = volume.id and label = '%s' " \
	    "      and (deleted = 'n' %s) and pnfs_path != '' " \
	    "%s " \
	    "order by location_cookie;" % (vol, use_deleted_sql, use_skip_bad)
	res = db.query(q).getresult()

	#Don't do anything for empty volumes.
	if len(res) == 0:
		log(MY_TASK, vol, "volume is empty")
		return 0

	#Build the list of files to migrate.
	bfids = []
	for row in res:
		bfids.append(row[0])

	media_types = []
	#Need to obtain the output media_type.  If --library
	# was used on the command line, go with that.  Otherwise,
	# set the media_type.
	if intf.library:
		media_type = get_media_type(intf.library, db)
		media_types = [media_type]
	else:
		for row in res:
			original_path = row[2]
			media_type = search_media_type(original_path, db)

			if media_type and media_type not in media_types:
				media_types.append(media_type)

	#If we are certain that this is a cloning job, not a migration, then
	# we should handle it accordingly.
	if len(media_types) == 1 and media_types[0] == v['media_type']:
		setup_cloning()
		
	#Here are some additional checks on the volume.  If necessary, it
	# will set the system_inhibit_1 value.
	if v['system_inhibit'][1] not in [IN_PROGRESS_STATE, INHIBIT_STATE] \
	       and \
	       v['system_inhibit'][1] in MIGRATION_STATES + MIGRATED_STATES:
		#If the system inhibit has already been set to another type
		# of migration, don't continue.
		log(MY_TASK, vol, 'has already been set to %s while trying to set it to %s' \
		    % (v['system_inhibit'][1], IN_PROGRESS_STATE))
		return 1
	if v['system_inhibit'][1] == INHIBIT_STATE and \
	       is_migrated_by_src_vol(vol, intf, db) and \
	       not getattr(intf, "force", None):
		log(MY_TASK, vol, 'has already been %s' % INHIBIT_STATE)
		return 0
	if v['system_inhibit'][1] != IN_PROGRESS_STATE:
		set_system_migrating_func(vcc, vol)
		log(MY_TASK, 'set %s to %s' % (vol, IN_PROGRESS_STATE))

	#Start to copy the files by starting a process per file.
	if PARALLEL_FILE_MIGRATION:
		res = 0
		copy_bfids_list = copy.copy(bfids)
		while len(copy_bfids_list):
			while len(pid_list) < FILE_LIMIT and \
				  len(copy_bfids_list) > 0:

				bfid = copy_bfids_list[0]
				run_in_parallel(migrate, ([bfid], intf))
				#Remove from the remaing to start list.
				copy_bfids_list.remove(bfid)

			if len(pid_list) >= 1:
				done_exit_status = wait_for_process()
                                #Remember the error.
				if done_exit_status:
					res = res + 1
				

		while len(pid_list):
			done_exit_status = wait_for_process()
			#Remember the error.
			if done_exit_status:
				res = res + 1	
				
	#Start to copy the files one by starting a process for all reads
	# and leaving the main thread for writes.
	else:
		res = migrate(bfids, intf)

	#Do one last volume wide check.
	if res == 0 and is_migrated_by_src_vol(vol, intf, db, checked = 0, closed = 0):
		set_src_volume_migrated(
			MY_TASK, vol, vcc, db)

	else:
		error_log(MY_TASK, "do not set %s to %s due to previous error"%(vol, INHIBIT_STATE))
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
		log_history(vol, i, db)
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

# restore(bfids) -- restore pnfs entries using file records
def restore(bfids, intf):
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

                ###########################################################
                # Verify the metadata.
                ###########################################################

                #Obtain file information.
		f = fcc.bfid_info(bfid)
		if not e_errors.is_ok(f):
			error_log(MY_TASK, f['status'])
			sys.exit(1)
		#Verify that the file has been deleted.
		#if f['deleted'] != 'yes':
		#	error_log(MY_TASK, "%s is not deleted"%(bfid))
		#	continue

		#Obtain volume information.
		v = vcc.inquire_vol(f['external_label'])
		if not e_errors.is_ok(v):
			error_log(MY_TASK, v['status'])
			sys.exit(1)

		#Determine if the file has been copied to a new tape already.
		is_it_copied = is_copied(bfid, db)
		dst_bfid = is_it_copied #side effect of is_copied()
		if dst_bfid == None:
			if get_bfids(bfid, db)[1] == bfid:
				error_log("bfid %s is a destination bfid not"
					  " a source bfid" % (bfid,))
				sys.exit(1)

		#If swapped, the current bfid is the new copy, ...
		if is_swapped(bfid, db):
			active_bfid = dst_bfid
			nonactive_bfid = bfid
		# ... otherwise we have a copied file that is not swapped
		# (probably something is very wrong, after all the user is
		# trying to set the original file back as the active file).
		else:
			active_bfid = bfid
			nonactive_bfid = dst_bfid

		#The restoring of a bfid is only allowed for migrated or
		# cloned files.  We need to fail duplicated files now.
		if is_duplication(bfid, db):
			error_log("bfid %s is a duplication bfid not"
				  " a migration/cloning bfid" % (bfid,))
			sys.exit(1)

                #We need to handle restoring a multiple copy.
                ob_reply = fcc.find_the_original(bfid)
                if e_errors.is_ok(ob_reply):
                        original_bfid = ob_reply.get('original', None)
                        if original_bfid and original_bfid == bfid:
                                #If this is its own original, ignore.
                                original_bfid = None
                else:
                        original_bfid = None

		#Find the current location of the file.
		pairs_to_search = [(f['pnfsid'], active_bfid),
				   (f['pnfsid'], nonactive_bfid)]
                if original_bfid:
                        #If we are restoring a migrated multiple_copy, add this
                        # to the list of metadata to check.  Put this first.
                        pairs_to_search.insert(0, (f['pnfsid'], original_bfid))
		for search_pnfsid, search_bfid in pairs_to_search:
			try:
				src = find_pnfs_file.find_pnfsid_path(
					search_pnfsid, search_bfid,
					path_type = find_pnfs_file.FS)
			except (KeyboardInterrupt, SystemExit):
				raise sys.exc_info()[0], sys.exc_info()[1], \
                                      sys.exc_info()[2]
			except OSError, msg:
				continue
				#src = find_pnfs_file.find_pnfsid_path(
				#	f['pnfsid'], nonactive_bfid,
				#	path_type = find_pnfs_file.FS)
			except:
				exc_type, exc_value, exc_tb = sys.exc_info()
				Trace.handle_error(exc_type, exc_value, exc_tb)
				del exc_tb #avoid resource leaks
				error_log(MY_TASK, str(exc_type),
					  str(exc_value),
					  "%s %s %s %s is not a valid pnfs file" \
					  % (f['external_label'], f['bfid'],
					     f['location_cookie'],
					     f['pnfsid']))
				sys.exit(1)

			break
		else:
			#Neither file matched.
			message = "Neither %s nor %s correlate to %s." \
				  % (active_bfid, nonactive_bfid, f['pnfsid'])
			error_log(MY_TASK, message)
			#continue
			src = f['pnfs_name0']

		#This would be used if get_path() was used, since it only
		# matches for pnfsid.  find_pnfs_file.find_pnfsid_path()
		# also checks to make sure the layer 1 bfid information
		# matches too; which should remove all duplicates.  The only
		# possible duplicates would be things like the same pnfs
		# filesystem mounted one machine in different locations; and
		# in this case taking the first one is fine.
		if type(src) == type([]):
			src = src[0]

                if original_bfid:
                        #If original_bfid is set, p needs to be overridden
                        # with the primary copy information.

                        #Obtain original file information.
                        f_original = fcc.bfid_info(original_bfid)
                        if not e_errors.is_ok(f_original):
                                error_log(MY_TASK, f_original['status'])
                                sys.exit(1)

                        #Obtain original volume information.
                        v_original = vcc.inquire_vol(f_original['external_label'])
                        if not e_errors.is_ok(v_original):
                                error_log(MY_TASK, v_original['status'])
                                sys.exit(1)
                                
                        p = pnfs.File(src)
                        p.volume = f_original['external_label']
                        p.location_cookie = f_original['location_cookie']
                        p.bfid = original_bfid
                        p.drive = f_original['drive']
                        p.complete_crc = f_original['complete_crc']
                        p.file_family = volume_family.extract_file_family(v_original['volume_family'])
                else:
                        #We are not restoring a multiple copy file.
                        
                        p = pnfs.File(src)
                        p.volume = f['external_label']
                        p.location_cookie = f['location_cookie']
                        p.bfid = bfid
                        p.drive = f['drive']
                        p.complete_crc = f['complete_crc']
                        p.file_family = volume_family.extract_file_family(v['volume_family'])

                if debug:
                        p.show()
                    
                #Knowing the path in pnfs, we can determine the special
		# temporary migration path in PNFS.
		mig_path = migration_path(src)

                ###########################################################
                # Make the metadata changes.
                ###########################################################
                
                if original_bfid:
                        #Have the restored multiple copy match the deleted
                        # status of the original.
                        if f_original['deleted'] == "no":
                                rtn_code = mark_undeleted(MY_TASK, bfid,
                                                          fcc, db)
                                if rtn_code:
                                        error_log(MY_TASK,
                                                  "failed to undelete source file %s %s" \
                                                  % (bfid, src,))
                                        continue
                        elif f_original['deleted'] == "yes":
                                 rtn_code = mark_deleted(MY_TASK, bfid,
                                                         fcc, db)
                                 if rtn_code:
                                        error_log(MY_TASK,
                                                  "failed to delete source file %s %s" \
                                                  % (bfid, src,))
                                        continue
                else:
                        # undelete the source file
                        rtn_code = mark_undeleted(MY_TASK, bfid, fcc, db)
                        if rtn_code:
                                error_log(MY_TASK,
                                          "failed to undelete source file %s %s" \
                                          % (bfid, src,))
                                continue

                #Remove the temporary path in PNFS if it still exists.
		if os.path.exists(mig_path):

                        try:
                                make_writeable(mig_path)
                        except (OSError, IOError), msg:
                                message = "unable to make writeable for file"
                                error_log(MY_TASK, 
                                          "%s %s: %s" % (message, mig_path,
                                                         str(msg)))
                                continue
                        
                        try:
                                nullify_pnfs(mig_path)
                        except (OSError, IOError), msg:
                                message = "failed to clear layers for file"
                                error_log(MY_TASK,
                                          "%s %s as (uid %s, gid %s): %s" \
                                          % (message, mig_path, os.geteuid(),
                                             os.getegid(), str(msg)))
                                continue

			try:
				file_utils.remove(mig_path)
			except (OSError, IOError), msg:
                                message = "failed to delete migration file"
                                error_log(MY_TASK,
                                          "%s %s as (uid %s, gid %s): %s" \
                                          % (message, mig_path, os.geteuid(),
                                             os.getegid(), str(msg)))
                                continue
                            

                ### Update layers 1 and 4.
		
                #Don't update if the original copy of the multiple
                # copy being restored is deleted (or unknown).
                if (original_bfid and f_original['deleted'] != "no"):
                        pass
                #For some failures, the swap never truly happens.  If this
		# is the case skip the pnfs layer update.
		elif not is_migration_path(src):

			# set layer 1 and layer 4 to point to the original file
			try:
				update_layers(p)
			except (IOError, OSError), msg:
                                message = "failed to restore layers 1 and 4 for"
                                error_log(MY_TASK,
                                          "%s %s %s: %s" \
                                          % (message, bfid, src, str(msg)))
                                continue

                #If the destination copy has multiple copies, we need to
                # be able to clear them.
                copies_reply = fcc.find_copies(dst_bfid)
                if not e_errors.is_ok(copies_reply):
                        error_log(MY_TASK,
			     "failed to retrieve multiple copies list %s %s" \
				  % (dst_bfid, mig_path,))
			continue

                #Undo the migration for the destination file and any
                # multiple copies that it has.
                for cur_dst_bfid in [dst_bfid] + copies_reply['copies']:
                
                        # mark the migration copy (and any multiple copies)
                        # of the file deleted
                        rtn_code = mark_deleted(MY_TASK, cur_dst_bfid, fcc, db)
                        if rtn_code:
                                error_log(MY_TASK,
                                     "failed to mark undeleted migration file %s %s" \
                                          % (dst_bfid, mig_path,))
                                continue

                        #Remove the swapped timestamp from the migration table.
                        # On error an exception should be raised preventing
                        # us from continuing further.
                        log_unswapped(bfid, cur_dst_bfid, db)
                        #Remove the copied timestamp from the migration table.
                        # On error an exception should be raised preventing
                        # us from continuing further.
                        log_uncopied(bfid, cur_dst_bfid, db)

# restore_volume(vol) -- restore all migrated files on original volume
def restore_volume(vol, intf):
	global errors
	
	MY_TASK = "RESTORE_VOLUME"
	db = pg.DB(host=dbhost, port=dbport, dbname=dbname, user=dbuser)
	log(MY_TASK, "restoring", vol, "...")
	q = "select bfid from file, volume, migration where \
		file.volume = volume.id and label = '%s' and \
		bfid = src_bfid order by location_cookie;" % (vol,)
	res = db.query(q).getresult()
	bfids = []
	for i in res:
		bfids.append(i[0])
	restore(bfids, intf)

	#Clear the system inhibit and comment for this volume.
	if not errors:
		# get its own volume clerk client
		config_host = enstore_functions2.default_host()
		config_port = enstore_functions2.default_port()
		csc = configuration_client.ConfigurationClient((config_host,
								config_port))
		vcc = volume_clerk_client.VolumeClerkClient(csc)

		#
		#Get and set the volume's metadata.
		#

		#Get the current data.
		v = vcc.inquire_vol(vol)

		#Set the new inhibit state and the comment.
		if enstore_functions2.is_readonly_state(v['system_inhibit'][1]):
			system_inhibit = [v['system_inhibit'][0], "none"]
		else:
			system_inhibit = v['system_inhibit']
		comment = "volume restored after %s" % \
			  (MIGRATION_NAME.lower(),)
		res1 = vcc.modify({'external_label':vol, 'comment':comment,
				   'system_inhibit':system_inhibit})
		if not e_errors.is_ok(res1):
			error_log(MY_TASK,
				  "failed to update volume %s" \
				  % (vol,))

		#Update the last access time for the volume, so that the
		# inventory knows to re-inventory this volume instead of
		# using the incorrect/obsolete cached information.
		res2 = vcc.touch(vol)
		if not e_errors.is_ok(res2):
			error_log(MY_TASK,
				  "failed to last access time update %s" \
				  % (vol,))

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

		option.Interface.__init__(self, args=args, user_mode=user_mode)

	def valid_dictionaries(self):
		return (self.help_options, self.migrate_options)

	#  define our specific parameters
	parameters = [
		"[bfid1 [bfid2 [bfid3 ...]]] | [vol1 [vol2 [vol3 ...]]] | [file1 [file2 [file3 ...]]]",
		"--restore [bfid1 [bfid2 [bfid3 ...]] | [vol1 [vol2 [vol3 ...]]]",
		"--scan-vol <vol1 [vol2 [vol3 ...]]>",
		"--migrated-from <vol1 [vol2 [vol3 ...]]>",
		"--migrated-to <vol1 [vol2 [vol3 ...]]>",
		"--status <vol1 [vol2 [vol3 ...]]>",
		"--show <media_type> ...",
		]
	
	migrate_options = {
		option.DEBUG:{option.HELP_STRING:
                              "Output extra debugging information",
                              option.VALUE_USAGE:option.IGNORED,
			      option.VALUE_TYPE:option.INTEGER,
			      option.USER_LEVEL:option.HIDDEN},
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
		option.FORCE:{option.HELP_STRING:
			      "Allow migration on already migrated volume.",
			      option.VALUE_USAGE:option.IGNORED,
			      option.VALUE_TYPE:option.INTEGER,
			      option.USER_LEVEL:option.HIDDEN},
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
		option.SCAN_VOLUMES:{option.HELP_STRING:
				 "Scan completed volumes.",
				 option.VALUE_USAGE:option.IGNORED,
				 option.VALUE_TYPE:option.INTEGER,
				 option.USER_LEVEL:option.USER,},
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
			       "Report on the completion of a volume.",
				 option.VALUE_USAGE:option.IGNORED,
				 option.VALUE_TYPE:option.INTEGER,
				 option.USER_LEVEL:option.USER,},
		option.USE_DISK_FILES:{option.HELP_STRING:
				       "Skip reading files on source volume, "
				       "use files already on disk.",
				       option.VALUE_USAGE:option.IGNORED,
				       option.VALUE_TYPE:option.INTEGER,
				       option.USER_LEVEL:option.ADMIN,},
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
		}


def main(intf):
	init(intf)

	if intf.migrated_from:
		# get a db connection
		db = pg.DB(host=dbhost, port=dbport, dbname=dbname, user=dbuser)
		show_migrated_from(intf.args, db)
		return 0

	elif intf.migrated_to:
		# get a db connection
		db = pg.DB(host=dbhost, port=dbport, dbname=dbname, user=dbuser)

		show_migrated_to(intf.args, db)
		return 0

	elif intf.status:
		# get a db connection
		db = pg.DB(host=dbhost, port=dbport, dbname=dbname, user=dbuser)
		
		exit_status = show_status(intf.args, db, intf)
		return exit_status

	elif intf.show:
		# get a db connection
		db = pg.DB(host=dbhost, port=dbport, dbname=dbname, user=dbuser)

		show_show(intf, db)

	elif intf.restore:
		bfid_list = []
		volume_list = []
		for target in intf.args:
			if enstore_functions3.is_bfid(target):
				bfid_list.append(target)
			elif enstore_functions3.is_volume(target):
				volume_list.append(target)
			else:
				message = "%s is not a volume or bfid.\n"
				sys.stderr.write(message % (target,))
				sys.exit(1)
				
		if bfid_list:
			restore(bfid_list, intf)
		for volume in volume_list:
			restore_volume(volume, intf)

	elif intf.scan_volumes:
		exit_status = 0
		for v in intf.args:
			exit_status = exit_status + final_scan_volume(v, intf)
                return exit_status

	#For duplicate only.
	elif getattr(intf, "make_failed_copies", None):
		
		# get a db connection
		db = pg.DB(host=dbhost, port=dbport, dbname=dbname, user=dbuser)

		return make_failed_copies(intf, db)
	
	else:
		bfid_list = []
		volume_list = []
		for target in intf.args:
			if enstore_functions3.is_bfid(target):
				bfid_list.append(target)
			elif enstore_functions3.is_volume(target):
				volume_list.append(target)
			else:
				try:
					f = pnfs.File(target)
					if f.bfid:
						bfid_list.append(f.bfid)
					else:
						raise ValueError(target)
				except:
					# abort on error
					error_log("can not find bfid of",
						  target)
					return 1

		rtn = 0
		if bfid_list:
			rtn = rtn + migrate(bfid_list, intf)
		for volume in volume_list:
			rtn = rtn + migrate_volume(volume, intf)

		return rtn

	return 0


def do_work(intf):

	try:
		exit_status = main(intf)
	except (SystemExit, KeyboardInterrupt):
		exc, msg = sys.exc_info()[:2]
		exit_status = 1
	except:
		#Get the uncaught exception.
		exc, msg, tb = sys.exc_info()
		message = "Uncaught exception: %s, %s\n" % (exc, msg)
		try:
			error_log(message)
		        #Send to the log server the traceback dump.  If
			# unsuccessful, print the traceback to standard error.
			Trace.handle_error(exc, msg, tb)
		except (OSError, IOError):
			if msg.errno == errno.EPIPE:
				#User piped the output to another process, but
				# didn't read all the data from the migrate
				# process.
				pass
			else:
				raise sys.exc_info()[0], sys.exc_info()[1], \
				      sys.exc_info()[2]
		del tb #No cyclic references.
		exit_status = 1

	#We should try and kill our child processes.
	if USE_THREADS:
		wait_for_threads()
	else:
		wait_for_processes(kill = True)
	
	sys.exit(exit_status)


if __name__ == '__main__':

	Trace.init(MIGRATION_NAME)

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

	
