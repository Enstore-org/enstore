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
import encp
import volume_family
import pg
import time
import thread
import Queue

debug = True	# debugging mode

# This is the configuration part, which might come from configuration
# server in the production version

SPOOL_DIR='/diskb/Migration_tmp'
DEFAULT_LIBRARY='CD-9940B'

f_prefix = '/pnfs/fs/usr'
f_p = string.split(f_prefix, '/')
f_n = len(f_p)

db = None
csc = None

# job queue for coping files
copy_queue = Queue.Queue(1024)
scan_queue = Queue.Queue(1024)

# migration log file
LOG_DIR = SPOOL_DIR
LOG_FILE = "MigrationLog+"+`os.getpid()`+"@"+time.strftime("%Y-%m-%d.%H:%M:%S", time.localtime(time.time()))
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
	global db, csc, log_f
	intf = option.Interface()
	csc = configuration_client.ConfigurationClient((intf.config_host,
		intf.config_port))

	db_info = csc.get('database')
	db = pg.DB(host=db_info['db_host'] , port=db_info['db_port'],
		dbname=db_info['dbname'])
	log_f = open(os.path.join(LOG_DIR, LOG_FILE), "a")
	return

# The following three functions query the state of a migrating file
# If true, the timestamp is returned, other wise, None is returned

# is_copied(bfid) -- has the file already been copied?
def is_copied(bfid):
	q = "select * from migration where src_bfid = '%s';"%(bfid)
	if debug:
		log(q)
	res = db.query(q).dictresult()
	if not len(res):
		return None
	else:
		return res[0]['dst_bfid']

# is_swapped(bfid) -- has the file already been swapped?
def is_swapped(bfid):
	q = "select * from migration where src_bfid = '%s';"%(bfid)
	if debug:
		log(q)
	res = db.query(q).dictresult()
	if not len(res):
		return None
	else:
		return res[0]['swapped']

# is_checked(bfid) -- has the file already been checked?
#	we check the destination file
def is_checked(bfid):
	q = "select * from migration where dst_bfid = '%s';"%(bfid)
	if debug:
		log(q)
	res = db.query(q).dictresult()
	if not len(res):
		return None
	else:
		return res[0]['checked']

# is_closed(bfid) -- has the file already been closed?
#	we check the destination file
def is_closed(bfid):
	q = "select * from migration where dst_bfid = '%s';"%(bfid)
	if debug:
		log(q)
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
	open_log(*args)
	print '... ERROR'
	if log_f:
		log_f.write('... ERROR\n')
		log_f.flush()

def ok_log(*args):
	open_log(*args)
	print '... OK'
	if log_f:
		log_f.write('... OK\n')
		log_f.flush()

# log(*args) -- log message
def log(*args):
	open_log(*args)
	print
	if log_f:
		log_f.write('\n')
		log_f.flush()

# log_copied(bfid1, bfid2) -- log a successful copy
def log_copied(bfid1, bfid2):
	q = "insert into migration (src_bfid, dst_bfid, copied) \
		values ('%s', '%s', '%s');" % (bfid1, bfid2,
		time2timestamp(time.time()))
	if debug:
		log(q)
	try:
		db.query(q)
	except:
		exc_type, exc_value = sys.exc_info()[:2]
		error_log("LOG_COPIED", str(exc_type), str(exc_value))
	return

# log_swapped(bfid1, bfid2) -- log a successful swap
def log_swapped(bfid1, bfid2):
	q = "update migration set swapped = '%s' where \
		src_bfid = '%s' and dst_bfid = '%s';"%(
			time2timestamp(time.time()), bfid1, bfid2)
	if debug:
		log(q)
	try:
		db.query(q)
	except:
		exc_type, exc_value = sys.exc_info()[:2]
		error_log("LOG_SWAPPED", str(exc_type), str(exc_value))
	return

# log_checked(bfid1, bfid2) -- log a successful readback 
def log_checked(bfid1, bfid2):
	q = "update migration set checked = '%s' where \
		src_bfid = '%s' and dst_bfid = '%s';"%(
			time2timestamp(time.time()), bfid1, bfid2)
	if debug:
		log(q)
	try:
		db.query(q)
	except:
		exc_type, exc_value = sys.exc_info()[:2]
		error_log("LOG_CHECKED", str(exc_type), str(exc_value))
	return

# log_closed(bfid1, bfid2) -- log a successful readback after closing
def log_closed(bfid1, bfid2):
	q = "update migration set closed = '%s' where \
		src_bfid = '%s' and dst_bfid = '%s';"%(
			time2timestamp(time.time()), bfid1, bfid2)
	if debug:
		log(q)
	try:
		db.query(q)
	except:
		exc_type, exc_value = sys.exc_info()[:2]
		error_log("LOG_CLOSED", str(exc_type), str(exc_value))
	return


# run_encp(args) -- excute encp using os.system()
# -- encp, like most of the enstore, is not thread safe.
#    in order to run multiple encp streams, we have to use os.system()
#    or alike, to give it a private process space.. 
def run_encp(args):
	# build command line
	cmd = "encp"
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
	pl[f_n] = 'Migration/'+pl[f_n]
	return string.join(pl, '/')

# temp_file(file) -- get a temporary destination file from file
def temp_file(vol, location_cookie):
	return os.path.join(SPOOL_DIR, vol+':'+location_cookie)
	
# copy_files(files) -- copy a list of files to disk and mark the status
# through copy_queue
def copy_files(files):
	MY_TASK = "COPYING_TO_DISK"
	# if files is not a list, make a list for it
	if type(files) != type([]):
		files = [files]

	# copy files one by one
	for bfid in files:
		log(MY_TASK, "copying %s"%(bfid))
		# get file info
		q = "select bfid, label, location_cookie, pnfs_id, \
			storage_group, file_family from file, volume \
			where file.volume = volume.id and \
				bfid = '%s';"%(bfid)
		if debug:
			log(q)
		res = db.query(q).dictresult()

		# does it exist?
		if not len(res):
			error_log(MY_TASK, "%s does not exists"%(bfid))
			continue

		f = res[0]
		if debug:
			log(`f`)
		tmp = temp_file(f['label'], f['location_cookie'])
		src = pnfs.Pnfs(mount_point='/pnfs/fs').get_path(f['pnfs_id'])
		if debug:
			log("src:", src)
			log("tmp:", tmp)
		if not os.access(src, os.R_OK):
			error_log(MY_TASK, "%s %s is not readable"%(bfid, src))
			continue

		# check if it has been copied
		ct = is_copied(bfid)
		if ct:
			res = 0
			ok_log(MY_TASK, "%s has already been copied at %s"%(bfid, ct))
		else:
			# make sure the tmp file is not there
			if os.access(tmp, os.F_OK):
				log(MY_TASK, "tmp file %s exists, remove it first"%(tmp))
				os.remove(tmp)
			res = run_encp(['--ecrc', src, tmp])
			if res == 0:
				ok_log(MY_TASK, "%s %s to %s"%(bfid, src, tmp))
			else:
				error_log(MY_TASK, "failed to copy %s %s to %s, error = %d"%(bfid, src, tmp, res))

		if res == 0:
			copy_queue.put((bfid, src, tmp, f['file_family'],
				f['storage_group']), True)

	# terminate the copy_queue
	log(MY_TASK, "no more to copy, terminating the copy queue")
	copy_queue.put(None, True)

# migration_file_family(ff) -- making up a file family for migration
def migration_file_family(ff):
	return ff+'-MIGRATION'

# normal_file_family(ff) -- making up a normal file family from a
#				migration file family
def normal_file_family(ff):
	if len(ff) > 10 and ff[-10:] == '-MIGRATION':
		return ff[:-10]
	else:
		return ff

# compare_metadata(p, f) -- compare metadata in pnfs (p) and filedb (f)
def compare_metadata(p, f):
	if debug:
		p.show()
		log(`f`)
	if p.bfid != f['bfid'] or \
		p.volume != f['external_label'] or \
		p.location_cookie != f['location_cookie'] or \
		long(p.size) != long(f['size']) or \
		p.pnfs_id != f['pnfsid']:
		return "inconsistent"
	# some of old pnfs records do not have crc and drive information
	if p.complete_crc and long(p.complete_crc) != long(f['complete_crc']):
		return "inconsistent (different crc)"
	if p.drive and p.drive != "unknown:unknown" and \
		p.drive != f['drive']:
		return "inconsistent (different drive)"
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
		return "metadata %s %s are %s"%(bfid1, src, res)

	res = compare_metadata(p2, f2)
	if res:
		return "metadata %s %s are %s"%(bfid2, src, res)

	# cross check
	if f1['size'] != f2['size']:
		return "%s and %s have different size"%(bfid1, bfid2)
	if f1['complete_crc'] != f2['complete_crc']:
		return "%s and %s have different crc"%(bfid1, bfid2)
	if f1['sanity_cookie'] != f2['sanity_cookie']:
		return "%s and %s have different sanity_cookie"%(bfid1, bfid2)

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
	p1.file_family = p2.file_family
	p1.update()
	# p1.show()

	# check it again
	p1 = pnfs.File(src)
	f1 = fcc.bfid_info(bfid2)
	if compare_metadata(p1, f1):
		return "swap_metadata(): %s %s has inconsistent metadata"%(bfid2, src)

	return None

# migrating() -- second half of migration, driven by copy_queue
def migrating():
	MY_TASK = "COPYING_TO_TAPE"
	if debug:
		log("migrating()")
	job = copy_queue.get(True)
	while job:
		if debug:
			log(`job`)
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
		bfid2 = is_copied(bfid)
		if not bfid2:
			res = run_encp(['--library', DEFAULT_LIBRARY, '--storage-group', sg,
				'--file-family', ff, tmp, dst])
			if res:
				error_log(MY_TASK, "failed to copy %s %s %s"%(bfid, src, tmp))
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
				log_copied(bfid, bfid2)

			# remove tmp file
			try:
				os.remove(tmp)
				ok_log(MY_TASK, "removing %s"%(tmp))
			except:
				error_log(MY_TASK, "failed to remove temporary file %s"%(tmp))
				pass

		# is it swapped?
		log("SWAPPING_METADATA", "swapping %s %s %s %s"%(bfid, src, bfid2, dst))
		if not is_swapped(bfid):
			res = swap_metadata(bfid, src, bfid2, dst)
			if not res:
				ok_log("SWAPPING_METADATA", "%s %s %s %s have been swapped"%(bfid, src, bfid2, dst))
				log_swapped(bfid, bfid2)
				scan_queue.put((bfid, bfid2, src), True)
			else:
				error_log("SWAPPING_METADATA", "%s %s %s %s failed due to %s"%(bfid, src, bfid2, dst, res))
		else:
			ok_log("SWAPPING_METADATA", "%s %s %s %s have already been swapped"%(bfid, src, bfid2, dst))
			scan_queue.put((bfid, bfid2, src), True)

		job = copy_queue.get(True)

	scan_queue.put(None, True)

# final_scan() -- last part of migration, driven by scan_queue
#   read the file as user to reasure everything is fine
def final_scan():
	# get its own file clerk client
	fcc = file_clerk_client.FileClient(csc)
	MY_TASK = "FINAL_SCAN"
	job = scan_queue.get(True)
	while job:
		(bfid, bfid2, src) = job
		log(MY_TASK, "start checking %s %s"%(bfid2, src))
		ct = is_checked(bfid2)
		if not ct:
			res = run_encp([src, '/dev/null'])
			if res == 0:
				log_checked(bfid, bfid2)
				ok_log(MY_TASK, bfid2, src)
			else: # error
				error_log(MY_TASK, "failed on %s %s"%(bfid2, src))

			# mark the original deleted
			res = fcc.set_deleted('yes', bfid=bfid)
			if res['status'][0] == e_errors.OK:
				ok_log(MY_TASK, "set %s deleted"%(bfid))
			else:
				error_log(MY_TASK, "failed to set %d deleted"%(bfid))
		else:
			ok_log(MY_TASK, "%s %s was checked on %s"%(bfid2, src, ct))
			# make sure the original is marked deleted
			f = fcc.bfid_info(bfid)
			if f['status'] == e_errors.OK and f['deleted'] != 'yes':
				error_log(MY_TASK, "%s was not marked deleted"%(bfid))

		job = scan_queue.get(True)

# final_scan_volume(vol) -- final scan on a volume when it is closed to
#				write
def final_scan_volume(vol):
	# get its own fcc
	fcc = file_clerk_client.FileClient(csc)
	vcc = volume_clerk_client.VolumeClerkClient(csc)
	MY_TASK = "FINAL_SCAN_VOLUME"
	q = "select bfid, pnfs_path, deleted, src_bfid  \
		from file, volume, migration \
		where file.volume = volume.id and \
			volume.label = '%s' and \
			deleted = 'n' and dst_bfid = bfid;"%(vol)
	query_res = db.query(q).get_result()
	log(MY_TASK, "closing volume", vol)
	# switch the file family back
	v = vcc.inquire_vol(vol)
	if v['status'][0] != e_errors.OK:
		error_log(MY_TASK, "failed to find volume", vol)
		return
	sg, ff, wp = string.split(v[volume_family], '.')
	ff = normal_file_family(ff)
	vf = string.join((sg, ff, wp), '.')
	res = vcc.modify({'external_label':vol, 'volume_family':vf})
	if res['status'][0] == e_errors.OK:
		ok_log(MY_TASK, "restore volume_family of", vol, "to", vol)
	else:
		error_log(MY_TASK, "failed to resotre volume_family of", vol, "to", vf)
		return 
	for r in query_res:
		bfid, pnfs_path, deleted, src_bfid = r
		ct = is_closed(bfid)
		if not ct:
			res = run_encp([pnfs_path, '/dev/null'])
			if res == 0:
				log_closed(src_bfid, bfid)
				ok_log(MY_TASK, "closing", bfid, pnfs_path)
			else:
				error_log(MY_TASK, "closing", bfid, pnfs_path, "failed")
			# mark the original deleted
			q = "select deleted from file where bfid = '%s';"%(src_bfid)
			res = db.query(q).get_result()
			if len(res):
				if res[0][0] != 'y':
					fcc.set_deleted('yes', bfid=src_bfid)
		else:
			ok_log(MY_TASK, "checking", bfid, pnfs_path, "already done at", ct)
			# make sure the original is marked deleted
			q = "select deleted from file where bfid = '%s';"%(src_bfid)
			res = db.query(q).get_result()
			if not len(res) or res[0][0] != 'y':
				error_log(MY_TASK, "%s was not marked deleted"%(src_bfid))

# migrate(file_list): -- migrate a list of files
def migrate(files):
	# start a thread to copy files out to disk
	c_id = thread.start_new_thread(copy_files, (files,))
	# main thread finishes the rest
	# (1) copy disk files to enstore
	# (2) swap meta-data
	m_id = thread.start_new_thread(migrating, ())
	# (3) final check
	ret = final_scan()
	return ret

# migrate_volume(vol) -- migrate a volume
def migrate_volume(vol):
	MY_TASK = "MIGRATING_VOLUME"
	log(MY_TASK, "start migrating volume", vol, "...")
	# get its own vcc
	vcc = volume_clerk_client.VolumeClerkClient(csc)
	# check if vol is set to "readonly". If not, set it.
	v = vcc.inquire_vol(vol)
	if v['status'][0] != e_errors.OK:
		error_log(MY_TASK, 'volume %s does not exist'%vol)
		return 1
	if v['system_inhibit'][1] != "readonly":
		vcc.set_system_readonly(vol)
		log(MY_TASK, 'set %s to readonly'%(vol))

	# now try to copy the file one by one
	# get all bfids
	q = "select bfid from file, volume \
		where file.volume = volume.id and label = '%s' \
		and deleted = 'n';"%(vol)
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
	log(MY_TASK, "restoring", vol, "...")
	q = "select bfid from file, volume where \
		file.volume = volume.id and label = '%s' and \
		deleted = 'y';"%(vol)
	res = db.query(q).get_result()
	bfids = []
	for i in res:
		bfids.append(i[0])
	restore(bfids)

# nulify_pnfs() -- nulify the pnfs entry so that when the entry is
#			removed, its layer4 won't be put in trashcan
#			hence won't be picked up by delfile
def nulify_pnfs(p):
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

	if sys.argv[1] == "--vol":
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
	else:	# assuming all are files
		files = []
		for i in sys.argv[2:]:
			try:
				f = pnfs.File(i)
				files.append(f.bfid)
			except:
				# abort on error
				error_log("can not find bifd of", i)
				sys.exit(1)
		migrate(files)
