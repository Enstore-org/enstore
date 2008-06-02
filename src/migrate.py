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
    -- migrating():
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
[3] three states of a migrating file
    -- copied
    -- swapped
    -- checked
"""

# system imports
import pg
import time
import thread
import threading
import Queue
import os
import sys
import string
import select

# enstore imports
import file_clerk_client
import volume_clerk_client
import configuration_client
import pnfs
import option
import e_errors
import encp_wrapper
import volume_family
import enstore_functions2
import callback
import Trace
import enstore_functions3

debug = False	# debugging mode

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
USE_THREADS = False #True

icheck = True	# instant readback check after swap
		# this is turned on by default for file based migration
		# It is turned off by default for volume based migration

errors = 0	# over all errors per migration run

no_log_command = ['--migrated-from', '--migrated-to', '--status']

# This is the configuration part, which might come from configuration
# server in the production version

# SPOOL_DIR='/diskb/Migration_tmp'
#SPOOL_DIR='/data/data2/Migration_Spool'
SPOOL_DIR=''
# DEFAULT_LIBRARY='CD-9940B'
DEFAULT_LIBRARY=''

f_prefix = '/pnfs/fs/usr'
f_p = string.split(f_prefix, '/')
f_n = len(f_p)

MIGRATION_DB = 'Migration'
# CMS_MIGRATION_DB = 'cms/WAX/repairing2bstayout/Migration'
CMS_MIGRATION_DB = 'cms/MIGRATION-9940A-TO-9940B'

DELETED_TMP = 'DELETED'

MFROM = "<="
MTO = "=>"

MIGRATION_FILE_FAMILY_KEY = "-MIGRATION"
DELETED_FILE_FAMILY = "DELETED_FILES"

INHIBIT_STATE = "migrated"
MIGRATION_NAME = "MIGRATION"
set_system_migrated_func=volume_clerk_client.VolumeClerkClient.set_system_migrated

ENCP_PRIORITY = 0
#csc = None

io_lock = thread.allocate_lock()

# job queue for coping files
copy_queue = Queue.Queue(1024)
scan_queue = Queue.Queue(1024)

#We add these items to the two queues.
copy_queue.received_count = 0
scan_queue.received_count = 0
copy_queue.finished = False
scan_queue.finished = False

#These are the pipes that will send jobs between the processes.
migrate_r_pipe, migrate_w_pipe = os.pipe()
scan_r_pipe, scan_w_pipe = os.pipe()

#If the tape speeds for the new media are faster then the old media; this
# should: int(NUM_OBJS * (1 - (old_rape_rate / new_tape_rate)))
#If they are the same speed then go with 2.
proceed_number = 2 

dbhost = None
dbport = None
dbname = None
dbuser = "enstore"

# migration log file
# LOG_DIR = SPOOL_DIR
LOG_DIR = '/var/migration'
LOG_FILE = "MigrationLog@"+time.strftime("%Y-%m-%d.%H:%M:%S", time.localtime(time.time()))+'#'+`os.getpid()`
log_f = None

# designated file family
use_file_family = None

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
	#global db, csc
	global log_f, dbhost, dbport, dbname, dbuser, errors
	global SPOOL_DIR

	csc = configuration_client.ConfigurationClient((intf.config_host,
							intf.config_port))

	db_info = csc.get('database')
	dbhost = db_info['dbhost']
	dbport = db_info['dbport']
	dbname = db_info['dbname']
	dbuser = db_info['dbuser']

	errors = 0

	#Make sure we got the spool directory from command line or
	# from configuration.
	if intf.spool_dir:
		SPOOL_DIR = intf.spool_dir
	if not SPOOL_DIR:
		SPOOL_DIR = enstore_functions2.default_value("SPOOL_DIR")

	# check for no_log commands
	if not intf.migrated_to and not intf.migrated_from and \
	   not intf.status and not intf.show:
		# check for directories

		#log dir
		if not os.access(LOG_DIR, os.F_OK):
			os.makedirs(LOG_DIR)
		if not os.access(LOG_DIR, os.W_OK):
			message = "Insufficent permissions to open log file.\n"
			sys.stderr.write(message)
			sys.exit(1)
		log_f = open(os.path.join(LOG_DIR, LOG_FILE), "a")

		#spool dir
		if not SPOOL_DIR:
			sys.stderr.write("No spool directory specified.\n")
			sys.exit(1)
		if not os.access(SPOOL_DIR, os.W_OK):
			os.makedirs(SPOOL_DIR)

	return

# The following three functions query the state of a migrating file
# If true, the timestamp is returned, other wise, None is returned

# is_copied(bfid) -- has the file already been copied?
def is_copied(bfid, db):
	q = "select * from migration where src_bfid = '%s';"%(bfid)
	if debug:
		log("is_copied():", q)
	res = db.query(q).dictresult()
	if not len(res):
		return None
	else:
		return res[0]['dst_bfid']

# is_swapped(bfid) -- has the file already been swapped?
def is_swapped(bfid, db):
	q = "select * from migration where src_bfid = '%s';"%(bfid)
	if debug:
		log("is_swapped():", q)
	res = db.query(q).dictresult()
	if not len(res):
		return None
	else:
		return res[0]['swapped']

# is_checked(bfid) -- has the file already been checked?
#	we check the destination file
def is_checked(bfid, db):
	q = "select * from migration where dst_bfid = '%s';"%(bfid)
	if debug:
		log("is_checked():", q)
	res = db.query(q).dictresult()
	if not len(res):
		return None
	else:
		return res[0]['checked']

# is_closed(bfid) -- has the file already been closed?
#	we check the destination file
def is_closed(bfid, db):
	q = "select * from migration where dst_bfid = '%s';"%(bfid)
	if debug:
		log("is_closed():", q)
	res = db.query(q).dictresult()
	if not len(res):
		return None
	else:
		return res[0]['closed']

# open_log(*args) -- log message without final line-feed
def open_log(*args):
	t = time.time()
	print time.ctime(t),
	for i in args:
		print i,
	if log_f:
		log_f.write(time.ctime(t)+' ')
		for i in args:
			log_f.write(i+' ')
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
	except:
		exc_type, exc_value = sys.exc_info()[:2]
		error_log("LOG_COPIED", str(exc_type), str(exc_value), q)
	return

# log_swapped(bfid1, bfid2) -- log a successful swap
def log_swapped(bfid1, bfid2, db):
	q = "update migration set swapped = '%s' where \
		src_bfid = '%s' and dst_bfid = '%s';"%(
			time2timestamp(time.time()), bfid1, bfid2)
	if debug:
		log("log_swapped():", q)
	try:
		db.query(q)
	except:
		exc_type, exc_value = sys.exc_info()[:2]
		error_log("LOG_SWAPPED", str(exc_type), str(exc_value), q)
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
	q = "insert into migration_history (src, dst) values \
		('%s', '%s');"%(src, dst)
	if debug:
		log("log_history():", q)
	try:
		db.query(q)
	except:
		exc_type, exc_value = sys.exc_info()[:2]
		error_log("LOG_HISTORY", str(exc_type), str(exc_value), q)
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

#Report if the volume pair was migrated or duplicated.
def get_migration_type(src_vol, dst_vol, db):
	try:
		#The first way will be the prefered way once the volume
		# clerk (et. al.) get updated.
		"""
		q = "select label from volume where " \
		     " (label = '%s' and system_inhibit_1 = '%s' ) " \
		     " or (label = '%s' and file_family like '%%%s%%') " \
		     % (src_vol, INHIBIT_STATE,
			dst_vol, MIGRATION_FILE_FAMILY_KEY)
	        res = db.query(q).getresult()
		if len(res) != 0:
			return MIGRATION_NAME
	        """
		q_d = "select label from volume where " \
		      " (label = '%s' or label = '%s') and " \
		      "  (comment like '%%->%%' or comment like '%%<-%%') " \
		      % (src_vol, dst_vol)
		q_m =  "select label from volume where " \
		      " (label = '%s' or label = '%s') and " \
		      "  (comment like '%%=>%%' or comment like '%%<=%%') " \
		      % (src_vol, dst_vol)
		res = db.query(q_m).getresult()
		if len(res) != 0:
			return "MIGRATION"
		res = db.query(q_d).getresult()
		if len(res) != 0:
			return "DUPLICATION"
	except IndexError:
		return None

	return None

##########################################################################

# migration_path(path) -- convert path to migration path
# a path is of the format: /pnfs/fs/usr/X/...
# a migration path is: /pnfs/fs/usr/Migration/X/...
def migration_path(path, sg, deleted = 'n'):
	if deleted == 'y':
		# NOT DONE YET
		if sg == 'cms':
			return f_prefix+'/'+CMS_MIGRATION_DB+'/'+DELETED_TMP+'/'+os.path.basename(path)
		else:
			return f_prefix+'/'+MIGRATION_DB+'/'+DELETED_TMP+'/'+os.path.basename(path)

	pl = string.split(path, '/')
	if pl[:f_n] != f_p:
		return None
	if sg == 'cms':	# special case, different pnfs server
		pl[f_n] = CMS_MIGRATION_DB+'/'+pl[f_n]
	else:
		pl[f_n] = MIGRATION_DB+'/'+pl[f_n]
	return string.join(pl, '/')

def deleted_path(path, vol, location_cookie):
	pl = string.split(path, '/')
	if pl[:f_n] != f_p:
		return None
	if pl[f_n] == 'cms':    # special case, different pnfs server
		dp = CMS_MIGRATION_DB+'/'+'DELETE_TMP'
	else:
		dp = MIGRATION_DB+'/'+DELETED_TMP
	return os.path.join(string.join(pl[:f_n], '/'), dp, vol+':'+location_cookie)

# temp_file(file) -- get a temporary destination file from file
def temp_file(vol, location_cookie):
	return os.path.join(SPOOL_DIR, vol+':'+location_cookie)


##########################################################################

def get_requests(queue, r_pipe, timeout = .1):

    if USE_THREADS:
	    return

	
    job = -1

    wait_time = timeout

    while job: # and queue.received_count < NUM_OBJS:
        try:
            r, w, x = select.select([r_pipe], [], [], wait_time)
        except select.error:
            #On an error, put the list ending None in the list.
            queue.put(None)
            queue.received_count = queue.received_count + 1
            break

        if r:
            try:
                #Set verbose to True for debugging.
                job = callback.read_obj(r_pipe, verbose = False)
                queue.put(job)
                queue.received_count = queue.received_count + 1
#                print "Queued request:", job

                #Set a flag indicating that we have read the last item.
                if job == None:
                    queue.finished = True
                
                wait_time = 0.1 #Make the followup wait time shorter.
            except e_errors.TCP_EXCEPTION:
                #On an error, put the list ending None in the list.
                queue.put(None)
                queue.received_count = queue.received_count + 1
		queue.finished = True
                break

        else:
            break

    return    #queue.received_count

def put_request(queue, w_pipe, job):
    if USE_THREADS:
        queue.received_count = queue.received_count + 1
        queue.put(job, True)
	if job == None:
		queue.finished = True
	return
	
    callback.write_obj(w_pipe, job)

##########################################################################

def get_queue_item(queue, r_pipe):

    if USE_THREADS:
        job = queue.get(True)
	#Set a flag indicating that we have read the last item.
	if job == None:
		queue.finished = True
	return job
    
    if queue.empty():
        wait_time = 10*60 #Make the initial wait time longer.
    else:
        wait_time = 0.1 #Make the followup wait time shorter.

    job = None
    while not job:
        get_requests(queue, r_pipe, timeout = wait_time)

        try:
            job = queue.get(False)
            break
        except Queue.Empty:
            job = None
            wait_time = 10*60 #Make the initial wait time longer.

    #Set a flag indicating that we have read the last item.
    #if job == None:
    #    queue.finished = True

    return job

##########################################################################
	
# copy_files(files) -- copy a list of files to disk and mark the status
# through copy_queue
def copy_files(files, intf):
	MY_TASK = "COPYING_TO_DISK"
	# get a db connection
	db = pg.DB(host=dbhost, port=dbport, dbname=dbname, user=dbuser)

	# get a file clerk client
	# fcc = file_clerk_client.FileClient(csc)

	# get an encp
	threading.currentThread().setName('READ')
	encp = encp_wrapper.Encp(tid='READ')

	# if files is not a list, make a list for it
	if type(files) != type([]):
		files = [files]

	# copy files one by one
	for bfid in files:
		log(MY_TASK, "processing %s"%(bfid))
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
			continue

		f = res[0]

		if debug:
			log(MY_TASK, `f`)
		log(MY_TASK, "copying %s %s %s"%(bfid, f['label'], f['location_cookie']))

		tmp = temp_file(f['label'], f['location_cookie'])

		if f['deleted'] == 'n':
			try:
				src = pnfs.Pnfs(mount_point='/pnfs/fs').get_path(f['pnfs_id'])
				if type(src) == type([]):
					src = src[0]
			except:
				exc_type, exc_value = sys.exc_info()[:2]
				error_log(MY_TASK, str(exc_type), str(exc_value), "%s %s %s %s is not a valid pnfs file"%(f['label'], f['bfid'], f['location_cookie'], f['pnfs_id']))
				continue
		elif f['deleted'] == 'y' and len(f['pnfs_id']) > 10:

			log(MY_TASK, "%s %s %s is a DELETED FILE"%(f['bfid'], f['pnfs_id'], f['pnfs_path']))
			src = "deleted-%s-%s"%(bfid, tmp) # for debug
			# do nothing more
		else:
			# what to do?
			error_log(MY_TASK, "can not copy %s"%(bfid))
			continue
			
		if debug:
			log(MY_TASK, "src:", src)
			log(MY_TASK, "tmp:", tmp)

		# check if it has been copied
		ct = is_copied(bfid, db)
		if ct:
			res = 0
			ok_log(MY_TASK, "%s has already been copied to %s"%(bfid, ct))
		else:
			if f['deleted'] == 'n' and not os.access(src, os.R_OK):
				error_log(MY_TASK, "%s %s is not readable"%(bfid, src))
				continue
			# make sure the tmp file is not there
			if os.access(tmp, os.F_OK):
				log(MY_TASK, "tmp file %s exists, removing it first"%(tmp))
				os.remove(tmp)

			if f['deleted'] == 'y':
				cmd = "encp --delayed-dismount 2 --priority %d --ignore-fair-share --bypass-filesystem-max-filesize-check --override-deleted --get-bfid %s %s"%(ENCP_PRIORITY, bfid, tmp)
			else:
				cmd = "encp --delayed-dismount 2 --priority %d --ignore-fair-share --bypass-filesystem-max-filesize-check %s %s"%(ENCP_PRIORITY, src, tmp)

			if debug:
				log(MY_TASK, "cmd =", cmd)

			res = encp.encp(cmd)
			if res == 0:
				ok_log(MY_TASK, "%s %s to %s"%(bfid, src, tmp))
			else:
				error_log(MY_TASK, "failed to copy %s %s to %s, error = %d"%(bfid, src, tmp, res))

		if res == 0:
			job = (bfid, src, tmp, f['file_family'],
			       f['storage_group'], f['deleted'], f['wrapper'])
			put_request(copy_queue, migrate_w_pipe, job)
			#copy_queue.put(job, True)

	# terminate the copy_queue
	log(MY_TASK, "no more to copy, terminating the copy queue")
	put_request(copy_queue, migrate_w_pipe, None)
	#copy_queue.put(None, True)

##########################################################################

# migration_file_family(ff) -- making up a file family for migration
def migration_file_family(ff, deleted = 'n'):
	global use_file_family
	if deleted == 'y':
		return DELETED_FILE_FAMILY+MIGRATION_FILE_FAMILY_KEY
	else:
		if use_file_family:
			return use_file_family+MIGRATION_FILE_FAMILY_KEY
		else:
			return ff+MIGRATION_FILE_FAMILY_KEY

# normal_file_family(ff) -- making up a normal file family from a
#				migration file family
def normal_file_family(ff):
	return ff.replace(MIGRATION_FILE_FAMILY_KEY, '')

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
def swap_metadata(bfid1, src, bfid2, dst):
	# get its own file clerk client
	config_host = enstore_functions2.default_host()
	config_port = enstore_functions2.default_port()
	csc = configuration_client.ConfigurationClient((config_host,
							config_port))
	fcc = file_clerk_client.FileClient(csc)
	# get all metadata
	p1 = pnfs.File(src)
	f1 = fcc.bfid_info(bfid1)
	p2 = pnfs.File(dst)
	f2 = fcc.bfid_info(bfid2)

	# check if the metadata are consistent
	res = compare_metadata(p1, f1)
	if res:
		return "metadata %s %s are inconsistent on %s"%(bfid1, src, res)

	res = compare_metadata(p2, f2)
	# deal with already swapped file record
	if res == 'pnfsid':
		res = compare_metadata(p2, f2, p1.pnfs_id)
	if res:
		return "metadata %s %s are inconsistent on %s"%(bfid2, dst, res)

	# cross check
	if f1['size'] != f2['size']:
		return "%s and %s have different size"%(bfid1, bfid2)
	if f1['complete_crc'] != f2['complete_crc']:
		return "%s and %s have different crc"%(bfid1, bfid2)
	if f1['sanity_cookie'] != f2['sanity_cookie']:
		return "%s and %s have different sanity_cookie"%(bfid1, bfid2)

	# check if p1 is writable
	if not os.access(src, os.W_OK):
		return "%s is not writable"%(src)

	# swapping metadata
	m1 = {'bfid': bfid2, 'pnfsid':f1['pnfsid'], 'pnfs_name0':f1['pnfs_name0']}
	res = fcc.modify(m1)
	# res = {'status': (e_errors.OK, None)}
	if not res['status'][0] == e_errors.OK:
		return "failed to change pnfsid for %s"%(bfid2)
	p1.volume = p2.volume
	p1.location_cookie = p2.location_cookie
	p1.bfid = p2.bfid
	p1.drive = p2.drive
	p1.complete_crc = p2.complete_crc
	# should we?
	# the best solution is to have encp ignore sanity check on file_family
	# p1.file_family = p2.file_family
	p1.update()
	# p1.show()

	# check it again
	p1 = pnfs.File(src)
	f1 = fcc.bfid_info(bfid2)
	res = compare_metadata(p1, f1)
	if res:
		return "swap_metadata(): %s %s has inconsistent metadata on %s"%(bfid2, src, res)

	return None

# migrating() -- second half of migration, driven by copy_queue
def migrating(intf):
	MY_TASK = "COPYING_TO_TAPE"
	# get a database connection
	db = pg.DB(host=dbhost, port=dbport, dbname=dbname, user=dbuser)
	# get its own file clerk client
	config_host = enstore_functions2.default_host()
	config_port = enstore_functions2.default_port()
	csc = configuration_client.ConfigurationClient((config_host,
							config_port))
	fcc = file_clerk_client.FileClient(csc)

	# get an encp
	threading.currentThread().setName('WRITE')
	encp = encp_wrapper.Encp(tid='WRITE')

	if debug:
		log(MY_TASK, "migrating() starts")

	job = get_queue_item(copy_queue, migrate_r_pipe)
	###job = copy_queue.get(True)
	while job:
		#Wait for the copy_queue to reach a minimal number before
		# starting to write the files.  If the queue is as full
		# as it will get move forward too.
		while not copy_queue.finished and \
			  copy_queue.received_count < proceed_number:
			get_requests(copy_queue, migrate_r_pipe)
			#wait for the read thread to process a bunch first.
			time.sleep(1)
		
		if debug:
			log(MY_TASK, `job`)
		(bfid, src, tmp, ff, sg, deleted, wrapper) = job

		dst = migration_path(src, sg, deleted)
		# check if it has already been copied
		bfid2 = is_copied(bfid, db)
		has_tmp_file = False
		if bfid2:
			ok_log(MY_TASK, "%s has already been copied to %s"%(bfid, bfid2))
		else:
			ff = migration_file_family(ff, deleted)
			# check dst
			if not dst:     # This can not happen!!!
				error_log(MY_TASK, "%s is not a pnfs entry"%(src))
				job = copy_queue.get(True)
				continue
			# check if the directory is witeable
			(dst_d, dst_f) = os.path.split(dst)
			# does the parent directory exist?
			if not os.access(dst_d, os.F_OK):
				try:
					os.makedirs(dst_d)
					ok_log(MY_TASK, "making path %s"%(dst_d))
				except:
					# can not do it
					error_log(MY_TASK, "can not make path %s"%(dst_d))
					job = copy_queue.get(True)
					continue
			if not os.access(dst_d, os.W_OK):
				# can not create the file in that directory
				error_log(MY_TASK, "%s is not writable"%(dst_d))
				job = copy_queue.get(True)
				continue

			if DEFAULT_LIBRARY:
				cmd = "encp --delayed-dismount 60 --priority %d --ignore-fair-share --library %s --storage-group %s --file-family %s --file-family-wrapper %s %s %s"%(ENCP_PRIORITY, DEFAULT_LIBRARY, sg, ff, wrapper, tmp, dst)
			else:
				cmd = "encp --delayed-dismount 60 --priority %d --ignore-fair-share --storage-group %s --file-family %s --file-family-wrapper %s %s %s"%(ENCP_PRIORITY, sg, ff, wrapper, tmp, dst)
			if debug:
				log(MY_TASK, 'cmd =', cmd)

			#We should be able to simply call the encp function
			# here.  However, on Linux, there is a bug in select()
			# that prevents select from returning that a pipe is
			# available for writing until the buffer is totally
			# empty.  Since, the callback.write_obj() makes many
			# little writes, hangs were found to occur after the
			# first one, until the tape writting process started
			# reading the pipe buffer.
			# Apache has seen this before.
			#
			# However, this also allows us to make sure that the
			# tape reading process can never overfill the buffer,
			# too.
			log(MY_TASK, "copying %s %s %s"%(bfid, src, tmp))
			if USE_THREADS:
				res = encp.encp(cmd)
			else:
				cur_thread = threading.Thread(
					target = encp.encp, args = (cmd,),
					name = "WRITE",)
				cur_thread.start()
				while cur_thread.isAlive():
					get_requests(copy_queue, migrate_r_pipe)
					cur_thread.join(.1)
				res = encp.exit_status
				del cur_thread #recover resources
		
			if res:
				log(MY_TASK, "failed to copy %s %s %s ... (RETRY)"%(bfid, src, tmp))
				# delete the target and retry once
				try:
					os.remove(dst)
				except:
					pass
				res = encp.encp(cmd)
				if res:
					error_log(MY_TASK, "failed to copy %s %s %s"%(bfid, src, tmp))
					# delete the target and retry once
					try:
						os.remove(dst)
					except:
						pass
					job = get_queue_item(copy_queue,
							     migrate_r_pipe)
					###job = copy_queue.get(True)
					continue
			if 1: #debug:
				log(MY_TASK, "written to tape %s %s %s"%(bfid, src, tmp))
			has_tmp_file = True
			# get bfid of copied file
			pf2 = pnfs.File(dst)
			bfid2 = pf2.bfid
			if bfid2 == None:
				error_log(MY_TASK, "failed to get bfid of %s"%(dst))
				job = get_queue_item(copy_queue,
						     migrate_r_pipe)
				###job = copy_queue.get(True)
				continue
			else:
				# log success of coping
				ok_log(MY_TASK, "%s %s %s is copied to %s"%(bfid, src, tmp, dst))
				log_copied(bfid, bfid2, db)

		keep_file = False
		# is it swapped?
		log("SWAPPING_METADATA", "swapping %s %s %s %s"%(bfid, src, bfid2, dst))
		if not is_swapped(bfid, db):
			if deleted == 'y':
				res = ''
				# copy the metadata
				finfo = fcc.bfid_info(bfid)
				if finfo['status'][0] == e_errors.OK:
					del finfo['status']
					finfo['bfid'] = bfid2
					finfo['location_cookie'] = pf2.location_cookie
					res2 = fcc.modify(finfo)
					if res2['status'][0] != e_errors.OK:
						res = res2['status'][1]
				else:
					res = "source file info missing"
			else:
				res = swap_metadata(bfid, src, bfid2, dst)
					
			if not res:
				ok_log("SWAPPING_METADATA", "%s %s %s %s have been swapped"%(bfid, src, bfid2, dst))
				log_swapped(bfid, bfid2, db)
				if icheck:
					scan_job = (bfid, bfid2, src, deleted)
					put_request(scan_queue, scan_w_pipe, scan_job)
					###scan_queue.put(scan_job, True)
			else:
				error_log("SWAPPING_METADATA", "%s %s %s %s failed due to %s"%(bfid, src, bfid2, dst, res))
				keep_file = True
		else:
			ok_log("SWAPPING_METADATA", "%s %s %s %s have already been swapped"%(bfid, src, bfid2, dst))
			if icheck:
				scan_job = (bfid, bfid2, src, deleted)
				put_request(scan_queue, scan_w_pipe, scan_job)
				###scan_queue.put(scan_job, True)

		if has_tmp_file and not keep_file:
			# remove tmp file
			try:
				os.remove(tmp)
				ok_log(MY_TASK, "removing %s"%(tmp))
			except:
				error_log(MY_TASK, "failed to remove temporary file %s"%(tmp))
				pass

		job = get_queue_item(copy_queue, migrate_r_pipe)
		###job = copy_queue.get(True)

	if icheck:
		log(MY_TASK, "no more to copy, terminating the scan queue")
		put_request(scan_queue, scan_w_pipe, None)
		###scan_queue.put(None, True)

# final_scan() -- last part of migration, driven by scan_queue
#   read the file as user to reasure everything is fine
def final_scan(intf):
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
	threading.currentThread().setName('FINAL_SCAN')
	encp = encp_wrapper.Encp(tid='FINAL_SCAN')

	#We need to wait for all copies to be written to tape before
	# trying to do a full scan.
	while not scan_queue.finished:
		get_requests(scan_queue, scan_r_pipe)
		#wait for the write thread to process everything first.
		time.sleep(1)

	###job = scan_queue.get(True)
	job = get_queue_item(scan_queue, scan_r_pipe)
	while job:
		(bfid, bfid2, src, deleted) = job
		log(MY_TASK, "start checking %s %s"%(bfid2, src))
		ct = is_checked(bfid2, db)
		if not ct:
			if deleted == 'y':
				use_override_deleted = "--override-deleted"
			else:
				use_override_deleted = ""

			#Use --get-bfid to read the file.  In this way,
			# encp is smart enough to know if the file is a
			# copy (either migrated or duplicated).
			cmd = "encp --delayed-dismount 1 --priority %d --bypass-filesystem-max-filesize-check --ignore-fair-share %s --get-bfid %s /dev/null"%(ENCP_PRIORITY, use_override_deleted, bfid2)

			res = encp.encp(cmd)
			if res == 0:
				log_checked(bfid, bfid2, db)
				ok_log(MY_TASK, bfid2, src)
			else: # error
				error_log(MY_TASK, "failed on %s %s"%(bfid2, src))

			# do not mark it deleted for now
			# # mark the original deleted
			# res = fcc.set_deleted('yes', bfid=bfid)
			# if res['status'][0] == e_errors.OK:
			# 	ok_log(MY_TASK, "set %s deleted"%(bfid))
			# else:
			# 	error_log(MY_TASK, "failed to set %d deleted"%(bfid))
		else:
			ok_log(MY_TASK, "%s %s was checked on %s"%(bfid2, src, ct))
			# make sure the original is marked deleted
			f = fcc.bfid_info(bfid)
			if f['status'] == e_errors.OK and f['deleted'] != 'yes':
				error_log(MY_TASK, "%s was not marked deleted"%(bfid))

		#Get the next file.
		job = get_queue_item(scan_queue, scan_r_pipe)
		###job = scan_queue.get(True)

# NOT DONE YET, consider deleted file in final scan
# Is the file deleted due to copying error?
# or was it deleted before migration?


# final_scan_volume(vol) -- final scan on a volume when it is closed to
#				write
# This is run without any other threads
#
# deal with deleted file
# if it is a migrated deleted file, check it, too
def final_scan_volume(vol):
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

	log(MY_TASK, "verifying volume", vol)

	v = vcc.inquire_vol(vol)
	if v['status'][0] != e_errors.OK:
		error_log(MY_TASK, "failed to find volume", vol)
		return 1

	# make sure the volume is ok to scan
	if v['system_inhibit'][0] != 'none':
		error_log(MY_TASK, 'volume %s is "%s"'%(vol, v['system_inhibit'][0]))
		return 1

	if (v['system_inhibit'][1] != 'full' and \
		v['system_inhibit'][1] != 'none' and \
		v['system_inhibit'][1] != 'readonly') \
		and is_migrated(vol, db):
		error_log(MY_TASK, 'volume %s is "%s"'%(vol, v['system_inhibit'][1]))
		return 1

	if v['system_inhibit'][1] != 'full':
		log(MY_TASK, 'volume %s is not "full"'%(vol), "... WARNING")

	# make sure this is a migration volume
	sg, ff, wp = string.split(v['volume_family'], '.')
	if ff.find(MIGRATION_FILE_FAMILY_KEY) == -1:
		error_log(MY_TASK, "%s is not a migration volume"%(vol))
		return 1

	q = "select bfid, pnfs_id, src_bfid, location_cookie, deleted  \
		from file, volume, migration \
		where file.volume = volume.id and \
			volume.label = '%s' and \
			dst_bfid = bfid \
		order by location_cookie;"%(vol)
	query_res = db.query(q).getresult()

	for r in query_res:
		bfid, pnfs_id, src_bfid, location_cookie, deleted = r
		st = is_swapped(src_bfid, db)
		if not st:
			error_log(MY_TASK, "%s %s has not been swapped"%(src_bfid, bfid))
			local_error = local_error + 1
			continue
		ct = is_checked(bfid, db)
		if not ct:
			if deleted == 'y':
				cmd = "encp --delayed-dismount 1 --priority %d --bypass-filesystem-max-filesize-check --ignore-fair-share --override-deleted --get-bfid %s /dev/null"%(ENCP_PRIORITY, bfid)
			else:
				# get the real path
				pnfs_path = pnfs.Pnfs(mount_point='/pnfs/fs').get_path(pnfs_id)
				if type(pnfs_path) == type([]):
					pnfs_path = pnfs_path[0]

				# make sure the path is NOT a migration path
				if pnfs_path[:22] == f_prefix+'/Migration':
					error_log(MY_TASK, 'none swapped file %s'%(pnfs_path))
					local_error = local_error + 1
					continue

				# make sure the volume is the same
				pf = pnfs.File(pnfs_path)
				if pf.volume != vol:
					error_log(MY_TASK, 'wrong volume %s (expecting %s)'%(pf.volume, vol))
					local_error = local_error + 1
					continue

				open_log(MY_TASK, "verifying", bfid, location_cookie, pnfs_path, '...')
				cmd = "encp --delayed-dismount 1 --priority %d --bypass-filesystem-max-filesize-check --ignore-fair-share %s /dev/null"%(ENCP_PRIORITY, pnfs_path)
			res = encp.encp(cmd)
			if res == 0:
				log_checked(src_bfid, bfid, db)
				close_log('OK')
			else:
				close_log("FAILED ... ERROR")
				local_error = local_error + 1
				continue

			# mark the original deleted
			q = "select deleted from file where bfid = '%s';"%(src_bfid)
			res = db.query(q).getresult()
			if len(res):
				if res[0][0] != 'y':
					res = fcc.set_deleted('yes', bfid=src_bfid)
					if res['status'][0] == e_errors.OK:
						ok_log(MY_TASK, "set %s deleted"%(src_bfid))
					else:
						error_log(MY_TASK, "failed to set %s deleted"%(src_bfid))
				else:
					ok_log(MY_TASK, "%s has already been marked deleted"%(src_bfid))
		else:
			ok_log(MY_TASK, bfid, "is already checked at", ct)
			# make sure the original is marked deleted
			q = "select deleted from file where bfid = '%s';"%(src_bfid)
			res = db.query(q).getresult()
			if not len(res) or res[0][0] != 'y':
				error_log(MY_TASK, "%s was not marked deleted"%(src_bfid))
				continue
		#############################################
		ct = is_closed(bfid, db)
		if not ct:
			log_closed(src_bfid, bfid, db)
			close_log('OK')
		#############################################
				
	# restore file family only if there is no error
	if not local_error and is_migrated(vol, db):
		ff = normal_file_family(ff)
		vf = string.join((sg, ff, wp), '.')
		res = vcc.modify({'external_label':vol, 'volume_family':vf})
		if res['status'][0] == e_errors.OK:
			ok_log(MY_TASK, "restore file_family of", vol, "to", ff)
		else:
			error_log(MY_TASK, "failed to restore volume_family of", vol, "to", vf)
			local_error = local_error + 1
		# set comment
		from_list = migrated_from(vol, db)
		vol_list = ""
		for i in from_list:
			# set last access time to now
			vcc.touch(i)
			vol_list = vol_list + ' ' + i
		if vol_list:
			res = vcc.set_comment(vol, MFROM+vol_list)
			if res['status'][0] == e_errors.OK:
				ok_log(MY_TASK, 'set comment of %s to "%s%s"'%(vol, MFROM, vol_list))
			else:
				error_log(MY_TASK, 'failed to set comment of %s to "%s%s"'%(vol, MFROM, vol_list))
	return local_error

# migrate(file_list): -- migrate a list of files
def migrate(files, intf):
	#If we don't do this between volumes, the first volume behaves
	# correctly, while the rest don't.
	copy_queue.received_count = 0
	scan_queue.received_count = 0
	copy_queue.finished = False
	scan_queue.finished = False
	
	if USE_THREADS:
		return _migrate_threads(files, intf)
	else:
		return _migrate_processes(files, intf)

def _migrate_threads(files, intf):
	global errors
	# reset errors every time
	errors = 0

	# start a thread to copy files out to disk
	c_id = thread.start_new_thread(copy_files, (files, intf))
	# main thread finishes the rest
	# (1) copy disk files to enstore
	# (2) swap meta-data
	# (3) final check
	if icheck:
		m_id = thread.start_new_thread(migrating, (intf,))
		final_scan(intf)
		m_id.join()
	else:
		migrating(intf)

	c_id.join()
	return errors

def _migrate_processes(files, intf):
	global errors
	# reset errors every time
	errors = 0

	# Start a process to copy files to disk.
	pid = os.fork()
	if pid == 0:  #child
		#os.close(migrate_r_pipe)

		print "Starting copy_files."
		copy_files(files, intf)
		print "Completed copy_files."

		#os.close(migrate_w_pipe)

		os._exit(errors)

	elif pid > 0: #parent
		#os.close(migrate_w_pipe)

		# main thread finishes the rest
		# (1) copy disk files to enstore
		# (2) swap meta-data
		# (3) final check
		if icheck:
		    # Start a process to copy files to disk.
		    pid2 = os.fork()
		    if pid2 == 0:  #child
			#os.close(scan_r_pipe)

			print "Starting migrating."
			migrating(intf)
			print "Completed migrating."

			#os.close(scan_w_pipe)

			os._exit(errors)
			
			# Keep the current process to write files to tape.
		    elif pid2 > 0: #parent
			#os.close(scan_w_pipe)

		        #Scan files on tape.
			print "Starting final_scan."
			final_scan(intf)
			print "Completed final_scan."

			#os.close(scan_r_pipe)

			done_pid, exit_status = os.waitpid(pid2, 0)
			if os.WIFEXITED(exit_status):
				errors = errors + os.WEXITSTATUS(exit_status)
			else:
				errors = errors + 1
		else:
		    # Keep the current process to write files to tape.
		    print "Starting migrating."
		    migrating(intf)
		    print "Completed migrating."

		done_pid, exit_status = os.waitpid(pid, 0)
		if os.WIFEXITED(exit_status):
			errors = errors + os.WEXITSTATUS(exit_status)
		else:
			errors = errors + 1
		
		#os.close(migrate_r_pipe)

	return errors

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

def is_migrated(vol, db):
	q = "select bfid,location_cookie from file,volume " \
	    "where file.volume = volume.id " \
	    " and volume.label = '%s' " \
	    "order by location_cookie;" % (vol,)
	res1 = db.query(q).getresult()
	for row in res1:
		q2 = "select * from migration " \
		     "where src_bfid = '%s' or dst_bfid = '%s';" % \
		     (row[0], row[0])
		res2 = db.query(q2).getresult()
		if len(res2) == 0:
			return False  #At least one file is not migrated.
		else:
			row2 = res2[0]
			try:
				if not row2[2] or not row2[3] or not row2[4] \
				       or not row2[5]:
					return False
			except IndexError:
				return False #At least one file is not migrated.
		
	return True #All files migrated.

# migrate_volume(vol) -- migrate a volume
def migrate_volume(vol, intf, with_deleted = None):
	MY_TASK = "MIGRATING_VOLUME"
	log(MY_TASK, "start migrating volume", vol, "...")
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
	if v['system_inhibit'][1] == INHIBIT_STATE and is_migrated(vol, db):
		log(MY_TASK, vol, 'has already been migrated')
		return 0
	if v['system_inhibit'][1] != "readonly":
		vcc.set_system_readonly(vol)
		log(MY_TASK, 'set %s to readonly'%(vol))

	# now try to copy the file one by one
	# get all bfids

	if with_deleted:
		q = "select bfid from file, volume \
			where file.volume = volume.id and label = '%s' \
			and (deleted = 'n' or deleted = 'y') and pnfs_path != '' \
		 	order by location_cookie;"%(vol)
	else:
		q = "select bfid from file, volume \
			where file.volume = volume.id and label = '%s' \
			and deleted = 'n' and pnfs_path != '' \
		 	order by location_cookie;"%(vol)
	res = db.query(q).getresult()

	bfids = []
	# start to copy the files one by one
	for r in res:
		bfids.append(r[0])

	res = migrate(bfids, intf)
	if res == 0:
		# mark the volume as migrated
		#ticket = vcc.set_system_migrated(vol)
		ticket = set_system_migrated_func(vcc, vol)
		if ticket['status'][0] == e_errors.OK:
			log(MY_TASK, "set %s to migrated"%(vol))
		else:
			error_log(MY_TASK, "failed to set %s migrated: %s" \
				  % (vol, ticket['status']))
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
				ok_log(MY_TASK, 'set comment of %s to "%s%s"'%(vol, MTO, vol_list))
			else:
				error_log(MY_TASK, 'failed to set comment of %s to "%s%s"'%(vol, MTO, vol_list))
	else:
		error_log(MY_TASK, "do not set %s to migrated due to previous error"%(vol))
	return res

# restore(bfids) -- restore pnfs entries using file records
def restore(bfids, intf):
	MY_TASK = "RESTORE"
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
		f = fcc.bfid_info(bfid)
		#Verify that the file has been deleted.
		if f['deleted'] != 'yes':
			error_log(MY_TASK, "%s is not deleted"%(bfid))
			continue
		#Verify that the file was copied and swapped.
		db = pg.DB(host=dbhost, port=dbport, dbname=dbname, user=dbuser)
		if not is_swapped(bfid, db):
			error_log(MY_TASK, "bfid %s was not swapped" % (bfid,))
			continue
		v = vcc.inquire_vol(f['external_label'])
		try:
			src = pnfs.Pnfs(mount_point='/pnfs/fs').get_path(f['pnfsid'])
		except OSError, msg:
			message = "Failed to restore %s: %s\n" % \
				  (f['pnfsid'], str(msg))
			sys.stderr.write(message)
			sys.exit(1)
		if type(src) == type([]):
			src = src[0]
		p = pnfs.File(src)

		p.volume = f['external_label']
		p.location_cookie = f['location_cookie']
		p.bfid = bfid
		p.drive = f['drive']
		p.complete_crc = f['complete_crc']
		p.file_family = volume_family.extract_file_family(v['volume_family'])
		# undelete the file
		res = fcc.set_deleted('no', bfid=bfid)
		if res['status'][0] == e_errors.OK:
			ok_log(MY_TASK, "undelete %s"%(bfid))
		else:
			error_log(MY_TASK, "failed to undelete %d"%(bfid))
		p.update()

# restore_volume(vol) -- restore all deleted files on vol
def restore_volume(vol, intf):
	MY_TASK = "RESTORE_VOLUME"
	db = pg.DB(host=dbhost, port=dbport, dbname=dbname, user=dbuser)
	log(MY_TASK, "restoring", vol, "...")
	q = "select bfid from file, volume where \
		file.volume = volume.id and label = '%s' and \
		deleted = 'y' order by location_cookie;"%(vol)
	res = db.query(q).getresult()
	bfids = []
	for i in res:
		bfids.append(i[0])
	restore(bfids, intf)

# nullify_pnfs() -- nullify the pnfs entry so that when the entry is
#			removed, its layer4 won't be put in trashcan
#			hence won't be picked up by delfile
def nullify_pnfs(p):
	p1 = pnfs.File(p)
	for i in [1,2,4]:
		f = open(p1.layer_file(i), 'w')
		f.close()

# is_bfid(s) -- check if s is a valid bfid
def is_bfid(s):
	l = len(s)
	if l == 18 or l == 19:	# with brand
		for i in s[:4]:
			if not i in string.letters:
				return 0
		for i in s[4:]:
			if not i in string.digits:
				return 0
		return 1
	elif l == 14 or l == 15: # without brand
		for i in s:
			if not i in string.digits:
				return 0
		return 1
	else:	# mistake
		return 0

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
		
		
		#self.bfids = 0
		#self.volumes = 0
		#self.volumes_with_deleted = 0
		#self.restore_bfids = 0
		#self.restore_volumes = 0
		self.scan_volumes = 0
		self.migrated_from = None
		self.migrated_to = None

		option.Interface.__init__(self, args=args, user_mode=user_mode)
		

	def valid_dictionaries(self):
		return (self.help_options, self.migrate_options)

	#  define our specific parameters
	"""
	parameters = ["--bfids <bfid1 [bfid2 [bfid3 ...]]>",
		      "--vol <vol1 [vol2 [vol3 ...]]>",
		      "--vol-with-deleted <vol1 [vol2 [vol3 ...]]>",
		      "--restore <bfid1 [bfid2 [bfid3 ...]]>",
		      "--restore-vol <vol1 [vol2 [vol3 ...]]>",
		      "--scan-volumes <vol1 [vol2 [vol3 ...]]>",
		      "--status <vol>",
		      "--migrated-from <vol>",
		      "--migrated-to <vol>",
		      ]
	"""
	parameters = [
		"[bfid1 [bfid2 [bfid3 ...]]] | [vol1 [vol2 [vol3 ...]]] | [file1 [file2 [file3 ...]]]",
		"--restore [bfid1 [bfid2 [bfid3 ...]] | [vol1 [vol2 [vol3 ...]]]",
		"--scan-vol <vol1 [vol2 [vol3 ...]]>",
		"--migrated-from <vol>",
		"--migrated-to <vol>",
		]
	
	migrate_options = {
		option.FILE_FAMILY:{option.HELP_STRING:
				    "Specify an alternative file family to "
				    "override the pnfs file family tag.",
				    option.VALUE_USAGE:option.REQUIRED,
				    option.VALUE_TYPE:option.STRING,
				    option.USER_LEVEL:option.USER,},
		option.LIBRARY:{option.HELP_STRING:
				"Specify an alternative library to override "
				"the pnfs library tag.",
				option.VALUE_USAGE:option.REQUIRED,
				option.VALUE_TYPE:option.STRING,
				option.VALUE_NAME:"library",
				option.USER_LEVEL:option.ADMIN,},
		option.MIGRATED_FROM:{option.HELP_STRING:
				      "Report the volumes that were copied"
				      " from this volume.",
				       option.VALUE_USAGE:option.IGNORED,
				       option.VALUE_TYPE:option.INTEGER,
				       option.USER_LEVEL:option.ADMIN,},
		option.MIGRATED_TO:{option.HELP_STRING:
				    "Report the volumes that were copied"
				    " to this volume.",
				    option.VALUE_USAGE:option.IGNORED,
				    option.VALUE_TYPE:option.INTEGER,
				    option.USER_LEVEL:option.ADMIN,},
		option.PRIORITY:{option.HELP_STRING:
				 "Sets the initial job priority."
				 "  Only knowledgeable users should set this.",
				 option.VALUE_USAGE:option.REQUIRED,
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
		option.WITH_DELETED:{option.HELP_STRING:
				     "Include deleted files.",
				     option.VALUE_USAGE:option.IGNORED,
				     option.VALUE_TYPE:option.INTEGER,
				     option.USER_LEVEL:option.USER,},
		option.WITH_FINAL_SCAN:{option.HELP_STRING:
					"Do a final scan after all the"
					"files are recopied to tape.",
					option.VALUE_USAGE:option.IGNORED,
					option.VALUE_TYPE:option.INTEGER,
					option.USER_LEVEL:option.USER,},
		}


def main(intf):
	global icheck

	init(intf)

	if intf.migrated_from:
		# get a db connection
		db = pg.DB(host=dbhost, port=dbport, dbname=dbname, user=dbuser)
		for vol in intf.args:
			from_list = migrated_from(vol, db)
			#We need to know determine if migration or
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
	elif intf.migrated_to:
		# get a db connection
		db = pg.DB(host=dbhost, port=dbport, dbname=dbname, user=dbuser)
		for vol in intf.args:
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
			if intf.with_final_scan:
				icheck = True
			else:
				icheck = False
			restore_volume(volume, intf)
	elif intf.scan_volumes:
		for v in intf.args:
			final_scan_volume(v)
	elif intf.status:
		exit_status = 0
		mig_type = None  #migration type (MIGRATION or DUPLICATION)
		
		# get a db connection
		db = pg.DB(host=dbhost, port=dbport, dbname=dbname, user=dbuser)
		for v in intf.args:
			
			#Reset this for each volume.  It flags if the volume
			# v is a src or dst volume for migration.
			is_dst_volume = False
			
			print "%19s %19s %6s %6s %6s %6s" % \
			      ("src_bfid", "dst_bfid", "copied", "swapped",
			       "checked", "closed")

			#Build the sql query.
			q = "select bfid,location_cookie from file,volume " \
			    "where file.volume = volume.id " \
			    " and volume.label = '%s' " \
			    "order by location_cookie;" % (v,)
			#Get the results.
			res1 = db.query(q).getresult()
			for row in res1:
				#Build the sql query.
				q2 = "select * from migration " \
				     "where src_bfid = '%s' or " \
				     " dst_bfid = '%s';" % (row[0], row[0])
				#Get the results.
				res2 = db.query(q2).getresult()
				for row2 in res2:
					if row2[2]:
						copied = "y"
					else:
						copied = ""
						exit_status = 1
					if row2[3]:
						swapped = "y"
					else:
						swapped = ""
						exit_status = 1
					if row2[4]:
						checked = "y"
					else:
						checked = ""
						exit_status = 1
					if row2[5]:
						closed = "y"
					else:
						closed = "" 
						exit_status = 1
					line = "%19s %19s %6s %6s %6s %6s" % \
					       (row2[0], row2[1], copied,
						swapped, checked, closed)
					print line
					if row[0] == row2[1]:
						is_dst_volume = True
				if len(res2) == 0:
					#If the volume is a destination
					# volume that does not have a match
					# in the migration, print in the
					# correct spot.
					if is_dst_volume:
						line = "%19s %19s" % \
						       ("", row[0],)
						print line
					else:
						#Not migrated yet.
						line = "%19s" % (row[0],)
						print line
						exit_status = 1
				if not mig_type and len(res2) > 0:
					src_sample_bfid = res2[0][0]
					dst_sample_bfid = res2[0][1]
					src_vol = get_volume_from_bfid(
						src_sample_bfid, db)
					dst_vol = get_volume_from_bfid(
						dst_sample_bfid, db)
					mig_type = get_migration_type(src_vol,
								      dst_vol,
								      db)
			if mig_type:
				print "\n%s" % (mig_type,)
				

				
		return exit_status
	elif intf.show:
		exit_status = 0
		# get a db connection
		db = pg.DB(host=dbhost, port=dbport, dbname=dbname, user=dbuser)
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
			q = q + "and storage_group = '%s' " % \
			    (intf.storage_group,)
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

		if bfid_list:
			migrate(bfid_list, intf)
		for volume in volume_list:
			if intf.with_final_scan:
				icheck = True
			else:
				icheck = False
			migrate_volume(volume, intf,
				       with_deleted = intf.with_deleted)


def do_work(intf):

	try:
		exit_status = main(intf)
	except (SystemExit, KeyboardInterrupt):
		exc, msg = sys.exc_info()[:2]
		Trace.log(e_errors.ERROR, "migrate aborted from: %s: %s" % (str(exc),str(msg)))
		exit_status = 1
	except:
		#Get the uncaught exception.
		exc, msg, tb = sys.exc_info()
		print "Uncaught exception:", exc, msg
		#Send to the log server the traceback dump.  If unsuccessful,
		# print the traceback to standard error.
		Trace.handle_error(exc, msg, tb)
		del tb #No cyclic references.
		exit_status = 1

	sys.exit(exit_status)

if __name__ == '__main__':

	intf_of_migrate = MigrateInterface(sys.argv, 0) # zero means admin

	do_work(intf_of_migrate)
	
	"""
	if len(sys.argv) < 2 or sys.argv[1] == "--help":
		usage()
		sys.exit(0)

	###init()

	# log command line
	cmd = string.join(sys.argv)
	if len(sys.argv) > 2 and not sys.argv[1] in no_log_command:
		log("COMMAND LINE:", cmd)

	# handle --priority <priority>
	if sys.argv[1] == "--priority":
		ENCP_PRIORITY = int(sys.argv[2])

		cmd1 = sys.argv[0]
		sys.argv = sys.argv[2:]
		sys.argv[0] = cmd1

	# handle --spool-dir <spool dirctory>
	if sys.argv[1] == "--spool-dir":
		SPOOL_DIR = sys.argv[2]
		
		cmd1 = sys.argv[0]
		sys.argv = sys.argv[2:]
		sys.argv[0] = cmd1

	if sys.argv[1] == "--library":
		DEFAULT_LIBRARY = sys.argv[2]

		cmd1 = sys.argv[0]
		sys.argv = sys.argv[2:]
		sys.argv[0] = cmd1

	# handle --use-file-family <file_family>
	if sys.argv[1] == "--use-file-family":
		if len(sys.argv) < 4:
			usage()
			sys.exit(0)
		# make sure the file family is not a mistake
		if sys.argv[2][:2] == "--" or \
			sys.argv[2].find("/") != -1 or \
			is_bfid(sys.argv[2]):
			print "Error: missing file family!"
			print "cmd =", cmd
			usage()
			sys.exit(0)
		if sys.argv[3][:2] == "--" and \
			sys.argv[3] != "--bfids" and \
			sys.argv[3] != "--vol" and \
			sys.argv[3] != "--vol-with-deleted":
			usage()
			sys.exit(0)

		use_file_family = sys.argv[2]
		cmd1 = sys.argv[0]
		sys.argv = sys.argv[2:]
		sys.argv[0] = cmd1

	init()

	if sys.argv[1] == "--vol":
		icheck = False
		for i in sys.argv[2:]:
			migrate_volume(i)
	elif sys.argv[1] == "--vol-with-deleted":
		icheck = False
		for i in sys.argv[2:]:
			migrate_volume(i, with_deleted = True)
	elif sys.argv[1] == "--bfids":
		files = []
		for i in sys.argv[2:]:
			files.append(i)
		migrate(files)
	elif sys.argv[1] == "--restore":
		restore(sys.argv[2:])
	elif sys.argv[1] == "--restore-vol":
		for i in sys.argv[2:]:
			restore_volume(i)
	elif sys.argv[1] == "--scan-vol":
		for i in sys.argv[2:]:
			final_scan_volume(i)
	elif sys.argv[1] == "--migrated-from":
		# get a db connection
		db = pg.DB(host=dbhost, port=dbport, dbname=dbname, user=dbuser)
		for i in sys.argv[2:]:
			from_list = migrated_from(i, db)
			print "%s %s"%(i, MFROM),
			for j in from_list:
				print j,
			print
	elif sys.argv[1] == "--migrated-to":
		# get a db connection
		db = pg.DB(host=dbhost, port=dbport, dbname=dbname, user=dbuser)
		for i in sys.argv[2:]:
			to_list = migrated_to(i, db)
			print "%s %s"%(i, MTO),
			for j in to_list:
				print j,
			print
	else:	# assuming all are files
		files = []
		for i in sys.argv[1:]:
			try:
				f = pnfs.File(i)
				files.append(f.bfid)
			except:
				# abort on error
				error_log("can not find bifd of", i)
				sys.exit(1)
		migrate(files)
	"""
