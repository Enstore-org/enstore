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
import re

# enstore imports
import configuration_client
import file_clerk_client
import volume_clerk_client
import pnfs
import migrate
import duplication_util
import e_errors
import enstore_functions2
import find_pnfs_file
import Trace
import option

# modifying migrate module
migrate.MIGRATION_FILE_FAMILY_KEY = "_copy_%s" #Not equals to (==) safe.
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
DuplicateInterface.migrate_options[option.MAKE_FAILED_COPIES] = {
	option.HELP_STRING:
	"Make duplicates where the multiple copy write failed.",
	option.VALUE_USAGE:option.IGNORED,
	option.VALUE_TYPE:option.INTEGER,
	option.USER_LEVEL:option.USER,
	}
del DuplicateInterface.migrate_options[option.RESTORE]


# migration_file_family(ff) -- making up a file family for migration
def migration_file_family(bfid, ff, fcc, intf, deleted = 'n'):
	reply_ticket = fcc.find_all_copies(bfid)
	if e_errors.is_ok(reply_ticket):
		count = len(reply_ticket['copies'])
	else:
		raise e_errors.EnstoreError(None, reply_ticket['status'][1],
					    reply_ticket['status'][0])
	
	if deleted == 'y':
		return migrate.DELETED_FILE_FAMILY + migrate.MIGRATION_FILE_FAMILY_KEY % (count,)
	else:
		if intf.file_family:
			return intf.file_family + migrate.MIGRATION_FILE_FAMILY_KEY % (count,)
		else:
			return ff + migrate.MIGRATION_FILE_FAMILY_KEY % (count,)

# normal_file_family(ff) -- making up a normal file family from a
#				migration file family
def normal_file_family(ff):
	return re.sub("_copy_[0-9]*", "", ff, 1)

#Return True if the file_family has the pattern of a migration/duplication
# file.  False otherwise.
def is_migration_file_family(ff):
    if re.search("_copy_[0-9]*", ff) == None:
        return False

    return True

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

	# get all the file db metadata
	f1 = fcc.bfid_info(bfid1)
        if not e_errors.is_ok(f1):
            return "src_bfid: %s: %s" % (f1['status'][0], f1['status'][1])
	
	f2 = fcc.bfid_info(bfid2)
        if not e_errors.is_ok(f2):
            return "dst_bfid: %s: %s" % (f1['status'][0], f1['status'][1])

        #It is possible to duplicate a duplicate.  Need to handle finding
	# the original bfid's file recored, if there is one.
        original_reply = fcc.find_the_original(bfid1)
	f0 = {}
	if e_errors.is_ok(original_reply) \
	       and original_reply['original'] != None \
	       and original_reply['original'] != bfid1:
		f0 = fcc.bfid_info(original_reply['original'])
		if not e_errors.is_ok(f2):
			return "original bfid: %s: %s" % (f1['status'][0],
							  f1['status'][1])

	# get all pnfs metadata - first the source file
	if f1['deleted'] == "no":
                try:
                        # This version handles the seteuid() locking.
                        p1 = migrate.File(src)
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
		p2 = migrate.File(dst)
	except (KeyboardInterrupt, SystemExit):
		raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
	except (OSError, IOError), msg:
		return str(msg)
	except:
		exc, msg = sys.exc_info()[:2]
		return str(msg)
	
	# check if the metadata are consistent
	res = migrate.compare_metadata(p1, f1)
	if res == "bfid" and f0:
		#Compare original, if applicable.
		res = migrate.compare_metadata(p1, f0)
	if res:
		return "[3] metadata %s %s are inconsistent on %s"%(bfid1, src, res)

	if not p2.bfid and not p2.volume:
		#The migration path has already been deleted.  There is
		# no file to compare with.
		pass
	else:
		res = migrate.compare_metadata(p2, f2)
		# deal with already swapped file record
		if res == 'pnfsid':
			res = migrate.compare_metadata(p2, f2, p1.pnfs_id)
		if res:
			return "[4] metadata %s %s are inconsistent on %s"%(bfid2, dst, res)

	# cross check
	if f1['size'] != f2['size']:
		err_msg = "%s and %s have different size" % (bfid1, bfid2)
	elif f1['complete_crc'] != f2['complete_crc']:
		err_msg = "%s and %s have different crc" % (bfid1, bfid2)
	elif f1['sanity_cookie'] != f2['sanity_cookie']:
		err_msg = "%s and %s have different sanity_cookie" \
			  % (bfid1, bfid2)
	else:
		err_msg = None
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

#This is a no-op for duplication.
def cleanup_after_scan(MY_TASK, mig_path, src_bfid, fcc, db):
	pass


def is_expected_volume(MY_TASK, vol, likely_path, fcc):

	#Confirm that the destination volume matches the volume that
	# pnfs is pointing to.  This is true for swapped duplicate
	# files.
	pf = pnfs.File(likely_path)
	pf_volume = getattr(pf, "volume", None)
	if pf_volume == None:
		message = "No file info for %s. " % (likely_path,)
		migrate.error_log(MY_TASK, message)
	elif pf_volume != vol:
		pf_bfid =  getattr(pf, "bfid", None)
		#Get the original and make sure the original volume
		# is the same.  This is true for non-swapped duplicate
		# files.
		original_file_info = fcc.bfid_info(pf_bfid)
		if not e_errors.is_ok(original_file_info):
			message = "No file info for bfid %s." % (pf_bfid,)
			migrate.error_log(MY_TASK, message)
			return False

		#If the original volume and the volume we are scaning
		# does not match the volume in pnfs layer 4, report
		# the error.
		if pf_volume != original_file_info['external_label']:
			message = "wrong volume %s (expecting %s or %s)" % \
				  (pf.volume, vol,
				   original_file_info['external_label'])
			migrate.error_log(MY_TASK, message)
			return False

	return True


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
migrate.is_expected_volume = is_expected_volume
migrate.cleanup_after_scan = cleanup_after_scan
migrate.swap_metadata = duplicate_metadata
migrate.get_filenames = get_filenames
migrate.restore = restore
migrate.restore_volume = restore_volume
migrate.setup_cloning = setup_cloning
migrate.migration_file_family = migration_file_family
migrate.normal_file_family = normal_file_family
migrate.is_migration_file_family = is_migration_file_family


if __name__ == '__main__':

	Trace.init(migrate.MIGRATION_NAME)

	intf_of_migrate = migrate.MigrateInterface(sys.argv, 0) # zero means admin

	migrate.do_work(intf_of_migrate)
	

