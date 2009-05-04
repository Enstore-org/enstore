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
import configuration_client
import file_clerk_client
import volume_clerk_client
import pnfs
import migrate
import duplication_util
import e_errors
import encp_wrapper
import enstore_functions2
import find_pnfs_file
import Trace
import option

# modifying migrate module
# migrate.DEFAULT_LIBRARY = 'LTO4'
#migrate.DEFAULT_LIBRARY = 'D0-LTO4G1'
#migrate.DEFAULT_LIBRARY = ""
migrate.MIGRATION_FILE_FAMILY_KEY = "_copy_1"
migrate.INHIBIT_STATE = "duplicated"
migrate.IN_PROGRESS_STATE = "duplicating"
migrate.MIGRATION_NAME = "DUPLICATION"
migrate.set_system_migrated_func=volume_clerk_client.VolumeClerkClient.set_system_duplicated
migrate.set_system_migrating_func=volume_clerk_client.VolumeClerkClient.set_system_duplicating
migrate.MFROM = "<-"
migrate.MTO = "->"
migrate.LOG_DIR = "/var/duplication"
migrate.LOG_FILE = migrate.LOG_FILE.replace('Migration', 'Duplication')

DuplicateInterface = migrate.MigrateInterface
"""
DuplicateInterface.migrate_options[option.LIST_FAILED_COPIES] = {
	option.HELP_STRING:
	"List originals where the multiple copy write failed.",
	option.VALUE_USAGE:option.IGNORED,
	option.VALUE_TYPE:option.INTEGER,
	option.USER_LEVEL:option.USER,
	}
"""
DuplicateInterface.migrate_options[option.MAKE_FAILED_COPIES] = {
	option.HELP_STRING:
	"Make duplicates where the multiple copy write failed.",
	option.VALUE_USAGE:option.IGNORED,
	option.VALUE_TYPE:option.INTEGER,
	option.USER_LEVEL:option.USER,
	}
del DuplicateInterface.migrate_options[option.RESTORE]

# This is to change the behavior of migrate.swap_metadata.
# duplicate_metadata(bfid1, src, bfid2, dst, db) -- duplicate metadata for src and dst
#
# * return None if succeeds, otherwise, return error message
# * to avoid deeply nested "if ... else", it takes early error return
def duplicate_metadata(bfid1, src, bfid2, dst, db):
	MY_TASK = "DUPLICATE_METADATA"

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
		err_msg = "%s and %s have different size"%(bfid1, bfid2)
	elif f1['complete_crc'] != f2['complete_crc']:
		err_msg = "%s and %s have different crc"%(bfid1, bfid2)
	elif f1['sanity_cookie'] != f2['sanity_cookie']:
		err_msg = "%s and %s have different sanity_cookie"%(bfid1, bfid2)
	if err_msg:
		if f2['deleted'] == "yes" and not migrate.is_swapped(bfid1, db):
			migrate.log(MY_TASK,
			    "undoing duplication of %s to %s do to error"         % (bfid1, bfid2))
			migrate.undo_log(bfid1, bfid2, db)
		return err_msg

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
			raise (sys.exc_info()[0],
			       sys.exc_info()[1],
			       sys.exc_info()[2])
		except:
			exc_type, exc_value, exc_tb = sys.exc_info()
			Trace.handle_error(exc_type, exc_value, exc_tb)
			del exc_tb #avoid resource leaks
			migrate.error_log(MY_TASK, str(exc_type),
				  str(exc_value),
				  " %s %s is not a valid pnfs file" \
				  % (
				#vol,
				     dst_bfid,
				     #location_cookie,
				     pnfs_id))
			#local_error = local_error + 1
			#continue
			return (None, None)

		if type(pnfs_path) == type([]):
			pnfs_path = pnfs_path[0]

		#Regardless of the path, we need to use the bfid
		# since the file we are scanning is a duplicate.
		use_path = "--get-bfid %s" % (dst_bfid,)

	return (pnfs_path, use_path)


def final_scan_file(MY_TASK, src_bfid, dst_bfid, pnfs_id, likely_path, deleted,
		    fcc, encp, intf, db):
	ct = migrate.is_checked(dst_bfid, db)
	if not ct:
		#log(MY_TASK, "start checking %s %s"%(dst_bfid, src))

		(pnfs_path, use_path) = get_filenames(
                    MY_TASK, dst_bfid, pnfs_id, likely_path, deleted)

                # make sure the path is NOT a migration path
                if pnfs_path == None or migrate.is_migration_path(pnfs_path):
                        migrate.error_log(MY_TASK,
                                  'none swapped file %s' % \
                                  (pnfs_path))
                        #local_error = local_error + 1
                        #continue
                        return 1

		rtn_code = migrate.scan_file(
			MY_TASK, dst_bfid, use_path, "/dev/null",
			deleted, intf, encp)
		if rtn_code:
                    #migrate.close_log("ERROR")
                    #migrate.error_log(MY_TASK,
		    #		     "failed on %s %s" % (dst_bfid, pnfs_path))
                    return 1
                else:
                    #Log the file as having been checked/scanned.
                    #migrate.close_log("OK")
                    migrate.log_checked(src_bfid, dst_bfid, db)
                    #migrate.ok_log(MY_TASK, dst_bfid, pnfs_path)

	else:
		migrate.ok_log(MY_TASK, dst_bfid, "is already checked at", ct)
		# make sure the original is marked deleted
		f = fcc.bfid_info(src_bfid)
		if f['status'] == e_errors.OK and f['deleted'] != 'yes':
			migrate.error_log(MY_TASK,
				  "%s was not marked deleted" \
				  % (src_bfid,))
			return 1

	return 0

# final_scan_volume(vol) -- final scan on a volume when it is closed to
#				write
# This is run without any other threads
#
# deal with deleted file
# if it is a duplicated deleted file, check it, too
def final_scan_volume(vol, intf):
	MY_TASK = "FINAL_SCAN_VOLUME"
	local_error = 0
	# get its own fcc
	config_host = enstore_functions2.default_host()
	config_port = enstore_functions2.default_port()
	csc = configuration_client.ConfigurationClient((config_host,
							config_port))
	fcc = file_clerk_client.FileClient(csc)
	vcc = volume_clerk_client.VolumeClerkClient(csc)

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
		and migrate.is_migrated_by_dst_vol(vol, intf, db):
		migrate.error_log(MY_TASK, 'volume %s is "%s"'%(vol, v['system_inhibit'][1]))
		return 1

	if v['system_inhibit'][1] != 'full':
		migrate.log(MY_TASK, 'volume %s is not "full"'%(vol), "... WARNING")

	if v['system_inhibit'][1] != "readonly" and \
	       v['system_inhibit'][1] != 'full':
		vcc.set_system_readonly(vol)
		migrate.log(MY_TASK, 'set %s to readonly'%(vol))

	# make sure this is a duplication volume
	sg, ff, wp = string.split(v['volume_family'], '.')
	if ff.find(migrate.MIGRATION_FILE_FAMILY_KEY) == -1:
		migrate.error_log(MY_TASK, "%s is not a %s volume" %
				  (vol, migrate.MIGRATION_NAME.lower()))
		return 1

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

		st = migrate.is_swapped(src_bfid, db)
		if not st:
			migrate.error_log(MY_TASK, "%s %s has not been swapped"%(src_bfid, dst_bfid))
			local_error = local_error + 1
			continue

		######################################################
		#Get the original
		original_file_info = fcc.bfid_info(src_bfid)
		if not e_errors.is_ok(original_file_info):
			migrate.error_log(MY_TASK, 'No file info for original bfid (%s) of duplicate %s.' % (src_bfid, dst_bfid))
			local_error = local_error + 1
			continue

		# make sure the original volume is the same
		pf = pnfs.File(likely_path)
		if pf.volume != original_file_info['external_label']:
			migrate.error_log(MY_TASK, 'wrong volume %s (expecting %s)'%(pf.volume, vol))
			local_error = local_error + 1
			continue
		#######################################################
		
		## Scan the file by reading it with encp.
		rtn_code = final_scan_file(MY_TASK, src_bfid, dst_bfid,
					   pnfs_id, likely_path, deleted,
					   fcc, encp, intf, db)
		if rtn_code:
			local_error = local_error + 1
			continue

		# If we get here, then the file has been scaned.  Consider
		# it closed too.
		ct = migrate.is_closed(dst_bfid, db)
		if not ct:
			migrate.log_closed(src_bfid, dst_bfid, db)
			migrate.close_log('OK')

	# restore file family only if there is no error
	if not local_error and migrate.is_migrated_by_dst_vol(vol, intf, db):
		rtn_code = migrate.set_volume_migrated(
			MY_TASK, vol, sg, ff, wp, vcc, db)
		if rtn_code:
			#Error occured.
			local_error = local_error + 1
			
	else:
		migrate.error_log(MY_TASK,
				  "skipping volume metadata update sinnce not all files have been scanned")
				
	return local_error

#The sg, ff and wp arguments are passed for compatibility with
# migrate.set_volume_migrated().
def set_volume_duplicated(MY_TASK, vol, sg, ff, wp, vcc, db):
	__pychecker__ = "unusednames=sg,ff,wp"
	
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
			migrate.ok_log(MY_TASK,
				       'set comment of %s to "%s%s"'
				       % (vol, migrate.MFROM, vol_list))
		else:
			migrate.error_log(MY_TASK,
				       'failed to set comment of %s to "%s%s"'
					  % (vol, migrate.MFROM, vol_list))
			return 1

	return 0

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

# The restore operation is not defined for duplication.  So, disable the
# functionality.
def restore(bfids, intf):
	__pychecker__ = "unusednames=bfids,intf"
	
	message = "Restore for duplication is not defined.\n"
	sys.stderr.write(message)
	sys.exit(1)
# The restore_volume operation is not defined for duplication.  So, disable the
# functionality.
def restore_volume(vol, intf):
	__pychecker__ = "unusednames=vol,intf"
	
	message = "Restore for duplication is not defined.\n"
	sys.stderr.write(message)
	sys.exit(1)

#Duplication doesn't do cloning.
def setup_cloning():
	pass

##
## Override migration functions with those for duplication.
##
migrate.swap_metadata = duplicate_metadata
#migrate.log_swapped = log_swapped_and_closed
migrate.final_scan_volume = final_scan_volume
migrate.restore = restore
migrate.restore_volume = restore_volume
migrate.set_volume_migrated = set_volume_duplicated
migrate.setup_cloning = setup_cloning

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

	Trace.init(migrate.MIGRATION_NAME)

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
