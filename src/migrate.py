#!/usr/bin/env python

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

import file_clerk_client
import volume_clerk_client
import configuration_client
import pnfs
import option
import os
import sys
import string
import e_errors
import encp_wrapper
import volume_family
import pg
import time
import thread
import Queue

debug = False	# debugging mode

icheck = True	# instant readback check after swap
		# this is turned on by default for file based migration
		# It is turned off by default for volume based migration

errors = 0	# over all errors per migration run

no_log_command = ['--migrated-from', '--migrated-to']

# This is the configuration part, which might come from configuration
# server in the production version

SPOOL_DIR='/diskb/Migration_tmp'
DEFAULT_LIBRARY='CD-9940B'

f_prefix = '/pnfs/fs/usr'
f_p = string.split(f_prefix, '/')
f_n = len(f_p)

MIGRATION_DB = 'Migration'
# CMS_MIGRATION_DB = 'cms/WAX/repairing2bstayout/Migration'
CMS_MIGRATION_DB = 'cms/MIGRATION-9940A-TO-9940B'

DELETED_TMP = 'DELETED'

MIGRATION_FILE_FAMILY_SUFFIX = "-MIGRATION"
DELETED_FILE_FAMILY = "DELETED_FILES"
lomffs = len(MIGRATION_FILE_FAMILY_SUFFIX)

csc = None

io_lock = thread.allocate_lock()

# job queue for coping files
copy_queue = Queue.Queue(1024)
scan_queue = Queue.Queue(1024)

dbhost = None
dbport = None
dbname = None
dbuser = "enstore"

# migration log file
LOG_DIR = SPOOL_DIR
LOG_FILE = "MigrationLog@"+time.strftime("%Y-%m-%d.%H:%M:%S", time.localtime(time.time()))+'#'+`os.getpid()`
log_f = None

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
def init():
	global db, csc, log_f, dbhost, dbport, dbname, errors
	intf = option.Interface()
	csc = configuration_client.ConfigurationClient((intf.config_host,
		intf.config_port))

	db_info = csc.get('database')
	dbhost = db_info['db_host']
	dbport = db_info['db_port']
	dbname = db_info['dbname']

	errors = 0

	# check for no_log commands
	if len(sys.argv) > 2 and not sys.argv[1] in no_log_command:
		log_f = open(os.path.join(LOG_DIR, LOG_FILE), "a")
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
# run_encp(args) -- excute encp using os.system()
# -- encp, like most of the enstore, is not thread safe.
#    in order to run multiple encp streams, we have to use os.system()
#    or alike, to give it a private process space.. 
def run_encp(args):
	# build command line
	# use lowest priority and do not count against fair share
	cmd = "encp --priority 0 --ignore-fair-share"
	for i in args:
		cmd = cmd + " " + i
	cmd = cmd + " >/dev/null 2>1"
	if debug:
		log(cmd)
	ret = os.system(cmd)
	return ret

# migration_path(path) -- convert path to migration path
# a path is of the format: /pnfs/fs/usr/X/...
# a migration path is: /pnfs/fs/usr/Migration/X/...
def migration_path(path):
	pl = string.split(path, '/')
	if pl[:f_n] != f_p:
		return None
	if pl[f_n] == 'cms':	# special case, different pnfs server
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
	return os.path.join(SPOOL_DIR, vol+':'+location_cookie)

# temp_file(file) -- get a temporary destination file from file
def temp_file(vol, location_cookie):
	return os.path.join(SPOOL_DIR, vol+':'+location_cookie)
	
# copy_files(files) -- copy a list of files to disk and mark the status
# through copy_queue
def copy_files(files):
	MY_TASK = "COPYING_TO_DISK"
	# get a db connection
	db = pg.DB(host=dbhost, port=dbport, dbname=dbname, user=dbuser)

	# get a file clerk client
	fcc = file_clerk_client.FileClient(csc)

	# get an encp
	encp = encp_wrapper.Encp()

	# if files is not a list, make a list for it
	if type(files) != type([]):
		files = [files]

	# copy files one by one
	for bfid in files:
		log(MY_TASK, "processing %s"%(bfid))
		# get file info
		q = "select bfid, label, location_cookie, pnfs_id, \
			storage_group, file_family, deleted \
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

		# check if it has been copied
		ct = is_copied(bfid, db)
		if ct:
			res = 0
			ok_log(MY_TASK, "%s has already been copied at %s"%(bfid, ct))
		else:
			tmp = temp_file(f['label'], f['location_cookie'])

			if f['deleted'] == 'n':
				try:
					src = pnfs.Pnfs(mount_point='/pnfs/fs').get_path(f['pnfs_id'])
				except:
					exc_type, exc_value = sys.exc_info()[:2]
					error_log(MY_TASK, str(exc_type), str(exc_value), "%s %s %s %s is not a valid pnfs file"%(f['label'], f['bfid'], f['location_cookie'], f['pnfs_id']))
					continue
			elif f['deleted'] == 'y' and len(f['pnfs_id']) > 10:

				# making up a pnfs entry for deleted file
				finfo = fcc.bfid_info(bfid)
				if finfo['status'][0] != e_errors.OK:
					error_log(MY_TASK, "can not find %s"%(bfid))
					continue

				src = deleted_path(finfo['pnfs_name0'])
				finfo['pnfs_name0'] = src
				pf = pnfs.File(findo)
				try:
					pf.create(finfo['pnfsid'])
				except:
					exc_type, exc_value = sys.exc_info()[:2]
					error_log(MY_TASK, str(exc_type), str(exc_value), "can not create %s"%(src))

			else:
				# what to do?
				error_log(MY_TASK, "can not copy %s"%(bfid))
				continue
				
			if debug:
				log(MY_TASK, "src:", src)
				log(MY_TASK, "tmp:", tmp)
			if not os.access(src, os.R_OK):
				error_log(MY_TASK, "%s %s is not readable"%(bfid, src))
				continue
			# make sure the tmp file is not there
			if os.access(tmp, os.F_OK):
				log(MY_TASK, "tmp file %s exists, remove it first"%(tmp))
				os.remove(tmp)
			cmd = "encp --priority 0 --ignore-fair-share --ecrc --bypass-filesystem-max-filesize-check %s %s"%(src, tmp)
			res = encp.encp(cmd)
			if res == 0:
				ok_log(MY_TASK, "%s %s to %s"%(bfid, src, tmp))
			else:
				error_log(MY_TASK, "failed to copy %s %s to %s, error = %d"%(bfid, src, tmp, res))

			if f['deleted'] == 'y':
				# clean up tmp pnfs entry
				nullify_pnfs(src)
				os.remove(src)
		if res == 0:
			copy_queue.put((bfid, src, tmp, f['file_family'],
				f['storage_group'], f['deleted']), True)

	# terminate the copy_queue
	log(MY_TASK, "no more to copy, terminating the copy queue")
	copy_queue.put(None, True)

# migration_file_family(ff) -- making up a file family for migration
def migration_file_family(ff):
	return ff+MIGRATION_FILE_FAMILY_SUFFIX

# normal_file_family(ff) -- making up a normal file family from a
#				migration file family
def normal_file_family(ff):
	if len(ff) > 10 and ff[-10:] == '-MIGRATION':
		return ff[:-10]
	else:
		return ff

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
def migrating():
	MY_TASK = "COPYING_TO_TAPE"
	# get a database connection
	db = pg.DB(host=dbhost, port=dbport, dbname=dbname, user=dbuser)

	# get an encp
	encp = encp_wrapper.Encp()

	if debug:
		log(MY_TASK, "migrating() starts")
	job = copy_queue.get(True)
	while job:
		if debug:
			log(MY_TASK, `job`)
		(bfid, src, tmp, ff, sg) = job
		ff = migration_file_family(ff)
		dst = migration_path(src)
		log(MY_TASK, "copying %s %s %s"%(bfid, src, tmp))
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

		# check if it has already been copied
		bfid2 = is_copied(bfid, db)
		if not bfid2:
			cmd = "encp --priority 0 --ignore-fair-share --library %s --storage-group %s --file-family %s %s %s"%(DEFAULT_LIBRARY, sg, ff, tmp, dst)
			res = encp.encp(cmd)
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
					job = copy_queue.get(True)
					continue

			# get bfid of copied file
			bfid2 = pnfs.File(dst).bfid
			if bfid2 == None:
				error_log(MY_TASK, "failed to get bfid of %s"%(dst))
				job = copy_queue.get(True)
				continue
			else:
				# log success of coping
				ok_log(MY_TASK, "%s %s %s is copied to %s"%(bfid, src, tmp, dst))
				log_copied(bfid, bfid2, db)

			# remove tmp file
			try:
				os.remove(tmp)
				ok_log(MY_TASK, "removing %s"%(tmp))
			except:
				error_log(MY_TASK, "failed to remove temporary file %s"%(tmp))
				pass

		# is it swapped?
		log("SWAPPING_METADATA", "swapping %s %s %s %s"%(bfid, src, bfid2, dst))
		if not is_swapped(bfid, db):
			res = swap_metadata(bfid, src, bfid2, dst)
			if not res:
				ok_log("SWAPPING_METADATA", "%s %s %s %s have been swapped"%(bfid, src, bfid2, dst))
				log_swapped(bfid, bfid2, db)
				if icheck:
					scan_queue.put((bfid, bfid2, src), True)
			else:
				error_log("SWAPPING_METADATA", "%s %s %s %s failed due to %s"%(bfid, src, bfid2, dst, res))
		else:
			ok_log("SWAPPING_METADATA", "%s %s %s %s have already been swapped"%(bfid, src, bfid2, dst))
			if icheck:
				scan_queue.put((bfid, bfid2, src), True)

		job = copy_queue.get(True)

	if icheck:
		scan_queue.put(None, True)

# final_scan() -- last part of migration, driven by scan_queue
#   read the file as user to reasure everything is fine
def final_scan():
	MY_TASK = "FINAL_SCAN"
	# get its own file clerk client
	fcc = file_clerk_client.FileClient(csc)

	#get a database connection
	db = pg.DB(host=dbhost, port=dbport, dbname=dbname, user=dbuser)

	# get an encp
	encp = encp_wrapper.Encp()

	job = scan_queue.get(True)
	while job:
		(bfid, bfid2, src) = job
		log(MY_TASK, "start checking %s %s"%(bfid2, src))
		ct = is_checked(bfid2, db)
		if not ct:
			cmd = "encp --priority 0 --bypass-filesystem-max-filesize-check --ignore-fair-share %s /dev/null"%(src)
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

		job = scan_queue.get(True)

# final_scan_volume(vol) -- final scan on a volume when it is closed to
#				write
# This is run without any other threads
def final_scan_volume(vol):
	MY_TASK = "FINAL_SCAN_VOLUME"
	local_error = 0
	# get its own fcc
	fcc = file_clerk_client.FileClient(csc)
	vcc = volume_clerk_client.VolumeClerkClient(csc)

	# get a db connection
	db = pg.DB(host=dbhost, port=dbport, dbname=dbname, user=dbuser)

	# get an encp
	encp = encp_wrapper.Encp()

	log(MY_TASK, "verifying volume", vol)

	v = vcc.inquire_vol(vol)
	if v['status'][0] != e_errors.OK:
		error_log(MY_TASK, "failed to find volume", vol)
		return 1

	# make sure the volume is ok to scan
	if v['system_inhibit'][0] != 'none':
		error_log(MY_TASK, 'volume %s is "%s"'%(vol, v['system_inhibit'][0]))
		return 1

	if v['system_inhibit'][1] != 'full' and \
		v['system_inhibit'][1] != 'none' and \
		v['system_inhibit'][1] != 'readonly':
		error_log(MY_TASK, 'volume %s is "%s"'%(vol, v['system_inhibit'][1]))
		return 1

	if v['system_inhibit'][1] != 'full':
		log(MY_TASK, 'volume %s is not "full"'%(vol), "... WARNING")

	# make sure this is a migration volume
	sg, ff, wp = string.split(v['volume_family'], '.')
	if ff[-lomffs:] != MIGRATION_FILE_FAMILY_SUFFIX:
		error_log(MY_TASK, "%s is not a migration volume"%(vol))
		return 1

	q = "select bfid, pnfs_id, src_bfid, location_cookie  \
		from file, volume, migration \
		where file.volume = volume.id and \
			volume.label = '%s' and \
			deleted = 'n' and dst_bfid = bfid \
		order by location_cookie;"%(vol)
	query_res = db.query(q).getresult()

	for r in query_res:
		bfid, pnfs_id, src_bfid, location_cookie = r
		st = is_swapped(src_bfid, db)
		if not st:
			error_log(MY_TASK, "%s %s has not been swapped"%(src_bfid, bfid))
			local_error = local_error + 1
			continue
		ct = is_closed(bfid, db)
		if not ct:
			# get the real path
			pnfs_path = pnfs.Pnfs(mount_point='/pnfs/fs').get_path(pnfs_id)

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
			cmd = "encp --priority 0 --bypass-filesystem-max-filesize-check --ignore-fair-share %s /dev/null"%(pnfs_path)
			res = encp.encp(cmd)
			if res == 0:
				log_closed(src_bfid, bfid, db)
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
			ok_log(MY_TASK, bfid, "is already closed at", ct)
			# make sure the original is marked deleted
			q = "select deleted from file where bfid = '%s';"%(src_bfid)
			res = db.query(q).getresult()
			if not len(res) or res[0][0] != 'y':
				error_log(MY_TASK, "%s was not marked deleted"%(src_bfid))
	# restore file family only if there is no error
	if not local_error:
		ff = normal_file_family(ff)
		vf = string.join((sg, ff, wp), '.')
		res = vcc.modify({'external_label':vol, 'volume_family':vf})
		if res['status'][0] == e_errors.OK:
			ok_log(MY_TASK, "restore file_family of", vol, "to", ff)
		else:
			error_log(MY_TASK, "failed to resotre volume_family of", vol, "to", vf)
			local_error = local_error + 1
		# set comment
		from_list = migrated_from(vol, db)
		vol_list = ""
		for i in from_list:
			# set last access time to now
			vcc.touch(i)
			vol_list = vol_list + ' ' + i
		if vol_list:
			res = vcc.set_comment(vol, "<="+vol_list)
			if res['status'][0] == e_errors.OK:
				ok_log(MY_TASK, 'set comment of %s to "<=%s"'%(vol, vol_list))
			else:
				error_log(MY_TASK, 'failed to set comment of %s to "<=%s"'%(vol, vol_list))
	return local_error

# migrate(file_list): -- migrate a list of files
def migrate(files):
	global errors
	# reset errors every time
	errors = 0
	# start a thread to copy files out to disk
	c_id = thread.start_new_thread(copy_files, (files,))
	# main thread finishes the rest
	# (1) copy disk files to enstore
	# (2) swap meta-data
	# (3) final check
	if icheck:
		m_id = thread.start_new_thread(migrating, ())
		final_scan()
		return errors
	else:
		migrating()
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

# migrate_volume(vol) -- migrate a volume
def migrate_volume(vol):
	MY_TASK = "MIGRATING_VOLUME"
	log(MY_TASK, "start migrating volume", vol, "...")
	db = pg.DB(host=dbhost, port=dbport, dbname=dbname, user=dbuser)
	# get its own vcc
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
	if v['system_inhibit'][1] == 'migrated':
		log(MY_TASK, vol, 'has already been migrated')
		return 0
	if v['system_inhibit'][1] != "readonly":
		vcc.set_system_readonly(vol)
		log(MY_TASK, 'set %s to readonly'%(vol))

	# now try to copy the file one by one
	# get all bfids
	q = "select bfid from file, volume \
		where file.volume = volume.id and label = '%s' \
		and deleted = 'n' and pnfs_path != '' \
		 order by location_cookie;"%(vol)
	res = db.query(q).getresult()

	bfids = []
	# start to copy the files one by one
	for r in res:
		bfids.append(r[0])
	res = migrate(bfids)
	if res == 0:
		# mark the volume as migrated
		ticket = vcc.set_system_migrated(vol)
		if ticket['status'][0] == e_errors.OK:
			log(MY_TASK, "set %s to migrated"%(vol))
		else:
			error_log(MY_TASK, "failed to set %s migrated"%(vol))
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
			res = vcc.set_comment(vol, "=>"+vol_list)
			if res['status'][0] == e_errors.OK:
				ok_log(MY_TASK, 'set comment of %s to "=>%s"'%(vol, vol_list))
			else:
				error_log(MY_TASK, 'failed to set comment of %s to "=>%s"'%(vol, vol_list))
	else:
		error_log(MY_TASK, "do not set %s to migrated due to previous error"%(vol))
	return res

# restore(bfids) -- restore pnfs entries using file records
def restore(bfids):
	# get its own file clerk client and volume clerk client
	fcc = file_clerk_client.FileClient(csc)
	vcc = volume_clerk_client.VolumeClerkClient(csc)
	if type(bfids) != type([]):
		bfids = [bfids]
	for bfid in bfids:
		MY_TASK = "RESTORE"
		f = fcc.bfid_info(bfid)
		if f['deleted'] != 'yes':
			error_log(MY_TASK, "%s is not deleted"%(bfid))
			continue
		v = vcc.inquire_vol(f['external_label'])
		src = pnfs.Pnfs(mount_point='/pnfs/fs').get_path(f['pnfsid'])
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
def restore_volume(vol):
	MY_TASK = "RESTORE_VOLUME"
	db = pg.DB(host=dbhost, port=dbport, dbname=dbname, user=dbuser)
	log(MY_TASK, "restoring", vol, "...")
	q = "select bfid from file, volume where \
		file.volume = volume.id and label = '%s' and \
		deleted = 'y';"%(vol)
	res = db.query(q).getresult()
	bfids = []
	for i in res:
		bfids.append(i[0])
	restore(bfids)

# nullify_pnfs() -- nullify the pnfs entry so that when the entry is
#			removed, its layer4 won't be put in trashcan
#			hence won't be picked up by delfile
def nullify_pnfs(p):
	p1 = pnfs.File(p)
	for i in [1,2,4]:
		f = open(p1.layer_file(i), 'w')
		f.close()

# usage() -- help
def usage():
	print "usage: %s <file list>"%(sys.argv[0])
	print "       %s --bfids <bfid list>"%(sys.argv[0])
	print "       %s --vol <volume list>"%(sys.argv[0])	
	print "       %s --infile <file>"%(sys.argv[0])
	print "       %s --restore <bfid list>"%(sys.argv[0])
	print "       %s --restore-vol <volume list>"%(sys.argv[0])
	print "       %s --scan-vol <volume list>"%(sys.argv[0])

if __name__ == '__main__':
	init()
	if len(sys.argv) < 2 or sys.argv[1] == "--help":
		usage()
		sys.exit(0)

	# log command line
	cmd = string.join(sys.argv)
	if len(sys.argv) > 2 and not sys.argv[1] in no_log_command:
		log("COMMAND LINE:", cmd)

	if sys.argv[1] == "--vol":
		icheck = False
		for i in sys.argv[2:]:
			migrate_volume(i)
	elif sys.argv[1] == "--bfids":
		files = []
		for i in sys.argv[2:]:
			files.append(i)
		migrate(sys.argv[2:])
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
			print "%s <="%(i),
			for j in from_list:
				print j,
			print
	elif sys.argv[1] == "--migrated-to":
		# get a db connection
		db = pg.DB(host=dbhost, port=dbport, dbname=dbname, user=dbuser)
		for i in sys.argv[2:]:
			to_list = migrated_to(i, db)
			print "%s =>"%(i),
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
