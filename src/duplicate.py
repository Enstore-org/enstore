#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

"""
duplicate.py -- duplication utility

Duplication is very similar to migration.
The difference is, duplication keeps both copies and makes the new files
as duplicates of the original ones.

The code is borrowed from migrate.py. It is imported and modified.
"""

# system imports
import sys
import os
import string
import pg
import threading

# enstore imports
import file_clerk_client
import volume_clerk_client
import pnfs
import migrate
import duplication_util
import e_errors
import encp_wrapper

# modifying migrate module
# migrate.DEFAULT_LIBRARY = 'LTO4'
migrate.DEFAULT_LIBRARY = 'D0-LTO4G1'
migrate.MIGRATION_FILE_FAMILY_KEY = "_copy_1"
migrate.MFROM = "<-"
migrate.MTO = "->"
migrate.LOG_DIR = "/var/duplication"
migrate.LOG_FILE = migrate.LOG_FILE.replace('Migration', 'Duplication')

# This is to change the behavior of migrate.swap_metadata.
# duplicate_metadata(bfid1, src, bfid2, dst) -- duplicate metadata for src and dst
#
# * return None if succeeds, otherwise, return error message
# * to avoid deeply nested "if ... else", it takes early error return
def duplicate_metadata(bfid1, src, bfid2, dst):
	# get its own file clerk client
	fcc = file_clerk_client.FileClient(migrate.csc)
	# get all metadata
	p1 = pnfs.File(src)
	f1 = fcc.bfid_info(bfid1)
	p2 = pnfs.File(dst)
	f2 = fcc.bfid_info(bfid2)

	# check if the metadata are consistent
	res = migrate.compare_metadata(p1, f1)
	if res:
		return "metadata %s %s are inconsistent on %s"%(bfid1, src, res)

	res = migrate.compare_metadata(p2, f2)
	# deal with already swapped file record
	if res == 'pnfsid':
		res = migrate.compare_metadata(p2, f2, p1.pnfs_id)
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

	# register duplication

	# get a duplication manager
	dm = duplication_util.DuplicationManager()
	rtn = dm.make_duplicate(bfid1, bfid2)
	dm.db.close()
	return rtn

# final_scan_volume(vol) -- final scan on a volume when it is closed to
#				write
# This is run without any other threads
#
# deal with deleted file
# if it is a migrated deleted file, check it, too
def final_scan_volume(vol):
	MY_TASK = "FINAL_SCAN_VOLUME"
	local_error = 0
	# get its own fcc
	fcc = file_clerk_client.FileClient(migrate.csc)
	vcc = volume_clerk_client.VolumeClerkClient(migrate.csc)

	# get a db connection
	db = pg.DB(host=migrate.dbhost, port=migrate.dbport, dbname=migrate.dbname, user=migrate.dbuser)

	# get an encp
	threading.currentThread().setName('FINAL_SCAN')
	encp = encp_wrapper.Encp(tid='FINAL_SCAN')

	migrate.log(MY_TASK, "verifying volume", vol)

	v = vcc.inquire_vol(vol)
	if v['status'][0] != e_errors.OK:
		migrate.error_log(MY_TASK, "failed to find volume", vol)
		return 1

	# make sure the volume is ok to scan
	if v['system_inhibit'][0] != 'none':
		migrate.error_log(MY_TASK, 'volume %s is "%s"'%(vol, v['system_inhibit'][0]))
		return 1

	if (v['system_inhibit'][1] != 'full' and \
		v['system_inhibit'][1] != 'none' and \
		v['system_inhibit'][1] != 'readonly') \
		and migrate.is_migrated(vol, db):
		migrate.error_log(MY_TASK, 'volume %s is "%s"'%(vol, v['system_inhibit'][1]))
		return 1

	if v['system_inhibit'][1] != 'full':
		migrate.log(MY_TASK, 'volume %s is not "full"'%(vol), "... WARNING")

	# make sure this is a migration volume
	sg, ff, wp = string.split(v['volume_family'], '.')
	if ff.find(migrate.MIGRATION_FILE_FAMILY_KEY) == -1:
		############################################
		migrate.error_log(MY_TASK, "%s is not a duplication volume"%(vol))
		############################################
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
		st = migrate.is_swapped(src_bfid, db)
		if not st:
			migrate.error_log(MY_TASK, "%s %s has not been swapped"%(src_bfid, bfid))
			local_error = local_error + 1
			continue
		ct = migrate.is_checked(bfid, db)
		if not ct:
			if deleted == 'y':
				cmd = "encp --delayed-dismount 1 --priority %d --bypass-filesystem-max-filesize-check --ignore-fair-share --override-deleted --get-bfid %s /dev/null"%(migrate.ENCP_PRIORITY, bfid)
			else:
				# get the real path
				pnfs_path = pnfs.Pnfs(mount_point='/pnfs/fs').get_path(pnfs_id)
				if type(pnfs_path) == type([]):
					pnfs_path = pnfs_path[0]

				# make sure the path is NOT a migration path
				if pnfs_path[:22] == migrate.f_prefix+'/Migration':
					migrate.error_log(MY_TASK, 'none swapped file %s'%(pnfs_path))
					local_error = local_error + 1
					continue

				#############################################
				#Get the original
				original_file_info = fcc.bfid_info(src_bfid)
				if not e_errors.is_ok(original_file_info):
					migrate.error_log(MY_TASK, 'No file info for original bfid (%s) of duplicate %s.' % (src_bfid, bfid))
					local_error = local_error + 1
					continue

				# make sure the volume is the same
				pf = pnfs.File(pnfs_path)
				if pf.volume != original_file_info['external_label']:
				#############################################
					migrate.error_log(MY_TASK, 'wrong volume %s (expecting %s)'%(pf.volume, vol))
					local_error = local_error + 1
					continue

				migrate.open_log(MY_TASK, "verifying", bfid, location_cookie, pnfs_path, '...')
				cmd = "encp --delayed-dismount 1 --priority %d --bypass-filesystem-max-filesize-check --ignore-fair-share --get-bfid %s /dev/null"%(migrate.ENCP_PRIORITY, bfid)
			res = encp.encp(cmd)
			if res == 0:
				migrate.log_checked(src_bfid, bfid, db)
				#migrate.close_log('OK')
			else:
				#migrate.close_log("FAILED ... ERROR")
				local_error = local_error + 1
				continue

			#############################################
			#############################################
		#############################################
		ct = migrate.is_closed(bfid, db)
		if not ct:
			migrate.log_closed(src_bfid, bfid, db)
			migrate.close_log('OK')
		#############################################
				
	# restore file family only if there is no error
	if not local_error:
		#ff = migrate.normal_file_family(ff)
		#vf = string.join((sg, ff, wp), '.')
		#res = vcc.modify({'external_label':vol, 'volume_family':vf})
		#if res['status'][0] == e_errors.OK:
		#	migrate.ok_log(MY_TASK, "restore file_family of", vol, "to", ff)
		#else:
		#	migrate.error_log(MY_TASK, "failed to restore volume_family of", vol, "to", vf)
		#	local_error = local_error + 1
		# set comment
		from_list = migrate.migrated_from(vol, db)
		vol_list = ""
		for i in from_list:
			# set last access time to now
			vcc.touch(i)
			vol_list = vol_list + ' ' + i
		if vol_list:
			res = vcc.set_comment(vol, migrate.MFROM+vol_list)
			if res['status'][0] == e_errors.OK:
				migrate.ok_log(MY_TASK, 'set comment of %s to "%s%s"'%(vol, migrate.MFROM, vol_list))
			else:
				migrate.error_log(MY_TASK, 'failed to set comment of %s to "%s%s"'%(vol, migrate.MFROM, vol_list))
	return local_error

# This is to change the behavior of migrate.log_swapped()
# log_swapped_and_closed(bfid1, bfid2, db)
def log_swapped_and_closed(bfid1, bfid2, db):
	# log_swapped
	q = "update migration set swapped = now(), checked = now(), closed = now() where \
		src_bfid = '%s' and dst_bfid = '%s';"%(bfid1, bfid2)
	try:
		db.query(q)
	except:
		exc_type, exc_value = sys.exc_info()[:2]
		migrate.error_log("LOG_SWAPPED", str(exc_type), str(exc_value), q)
	return
			
migrate.swap_metadata = duplicate_metadata
#migrate.log_swapped = log_swapped_and_closed
migrate.final_scan_volume = final_scan_volume

# init() -- initialization

#def init():
#	migrate.init()

"""
def usage():
	print "usage:"
	print "  %s <file list>"%(sys.argv[0])
	print "  %s --bfids <bfid list>"%(sys.argv[0])
	print "  %s --vol <volume list>"%(sys.argv[0])
	print "  %s --vol-with-deleted <volume list>"%(sys.argv[0])
"""

if __name__ == '__main__':

	intf_of_migrate = migrate.MigrateInterface(sys.argv, 0) # zero means admin

	migrate.do_work(intf_of_migrate)
	

	"""
	if len(sys.argv) < 2 or sys.argv[1] == "--help":
		usage()
		sys.exit(0)

	###init()

	# log command line
	cmd = string.join(sys.argv)
	if len(sys.argv) > 2:
		migrate.log("COMMAND LINE:", cmd)

	# handle --priority <priority>
	if sys.argv[1] == "--priority":
		migrate.ENCP_PRIORITY = int(sys.argv[2])

		cmd1 = sys.argv[0]
		sys.argv = sys.argv[2:]
		sys.argv[0] = cmd1

	# handle --spool-dir <spool dirctory>
	if sys.argv[1] == "--spool-dir":
		SPOOL_DIR = sys.argv[2]
		
		cmd1 = sys.argv[0]
		sys.argv = sys.argv[2:]
		sys.argv[0] = cmd1

	# handle library
	if sys.argv[1] == "--library":
		migrate.DEFAULT_LIBRARY = sys.argv[2]
		cmd1 = sys.argv[0]
		sys.argv = sys.argv[2:]
		sys.argv[0] = cmd1

	init()

	if sys.argv[1] == "--vol":
		migrate.icheck = False
		for i in sys.argv[2:]:
			migrate.migrate_volume(i)
	elif sys.argv[1] == "--vol-with-deleted":
		migrate.icheck = False
		for i in sys.argv[2:]:
			migrate.migrate_volume(i, with_deleted = True)
	elif sys.argv[1] == "--bfids":
		files = []
		for i in sys.argv[2:]:
			files.append(i)
		migrate.migrate(files)
	else:	# assuming all are files
		files = []
		for i in sys.argv[1:]:
			try:
				f = pnfs.File(i)
				files.append(f.bfid)
			except:
				# abort on error
				migrate.error_log("can not find bifd of", i)
				sys.exit(1)
		migrate.migrate(files)

	"""
