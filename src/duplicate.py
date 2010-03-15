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
def migration_file_family(bfid, ff, fcc, intf, deleted = migrate.NO):
	reply_ticket = fcc.find_all_copies(bfid)
	if e_errors.is_ok(reply_ticket):
		count = len(reply_ticket['copies'])
	else:
		raise e_errors.EnstoreError(None, reply_ticket['status'][1],
					    reply_ticket['status'][0])
	
	if deleted == migrate.YES:
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
# duplicate_metadata(job, fcc, db) -- duplicate metadata for src and dst
#
# * return None if succeeds, otherwise, return error message
# * to avoid deeply nested "if ... else", it takes early error return
#
# The format of job is a 7-tuple in the following order:
# 1) The file record of the file being duplicated.
# 2) The volume record of source file.
# 3) The current path in PNFS/Chimera of the file being duplicated.
# 4) The file record of the file written to the new volume.
# 5) The volume record of the volume the new file was written onto.
# 6) The path of the file used to cache the file on local disk.
# 7) The temporary path of the new copy in PNFS/Chimera.
# By the time duplicate_metadata() is called, all seven of these values
# will be filled in.
def duplicate_metadata(job, fcc, db):
    MY_TASK = "DUPLICATE_METADATA"

    #Get information about the files to copy and swap.
    (src_file_record, src_volume_record, src_path,
     dst_file_record, dst_volume_record, tmp_path, mig_path) = job

    #shortcuts
    src_bfid = src_file_record['bfid']
    dst_bfid = dst_file_record['bfid']

    # get its own file clerk client
    config_host = enstore_functions2.default_host()
    config_port = enstore_functions2.default_port()
    csc = configuration_client.ConfigurationClient((config_host,
						    config_port))
    fcc = file_clerk_client.FileClient(csc)

    #It is possible to duplicate a duplicate.  Need to handle finding
    # the original bfid's file recored, if there is one.
    original_reply = fcc.find_the_original(src_bfid)
    f0 = {}
    if e_errors.is_ok(original_reply) \
	   and original_reply['original'] != None \
	   and original_reply['original'] != src_bfid:
        f0 = fcc.bfid_info(original_reply['original'])
	if not e_errors.is_ok(dst_file_record):
	    return "original bfid: %s: %s" % (src_file_record['status'][0],
					      src_file_record['status'][1])

    # get all pnfs metadata - first the source file
    if src_file_record['deleted'] == "no":
	try:
		# This version handles the seteuid() locking.
		p1 = migrate.File(src_path)
	except (KeyboardInterrupt, SystemExit):
		raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
	except (OSError, IOError), msg:
		return str(msg)
	except:
		exc, msg = sys.exc_info()[:2]
		return str(msg)
    else:
        # What do we need an empty File class for?
	p1 = pnfs.File(src_path)

    # get all pnfs metadata - second the destination file
    try:
        p2 = migrate.File(mig_path)
    except (KeyboardInterrupt, SystemExit):
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
    except (OSError, IOError), msg:
        return str(msg)
    except:
        exc, msg = sys.exc_info()[:2]
	return str(msg)
	
    # check if the metadata are consistent
    res = migrate.compare_metadata(p1, src_file_record)
    if res == "bfid" and f0:
        #Compare original, if applicable.
	res = migrate.compare_metadata(p1, f0)
    if res:
        return "[3] metadata %s %s are inconsistent on %s" % \
	       (src_bfid, src_path, res)

    if not p2.bfid and not p2.volume:
        #The migration path has already been deleted.  There is
	# no file to compare with.
	pass
    else:
        res = migrate.compare_metadata(p2, dst_file_record)
	# deal with already swapped file record
	if res == 'pnfsid':
	    res = migrate.compare_metadata(p2, dst_file_record, p1.pnfs_id)
	if res:
	    return "[4] metadata %s %s are inconsistent on %s" % \
		   (dst_bfid, mig_path, res)

    # cross check
    if src_file_record['size'] != dst_file_record['size']:
        err_msg = "%s and %s have different size" % (src_bfid, dst_bfid)
    elif src_file_record['complete_crc'] != dst_file_record['complete_crc']:
        err_msg = "%s and %s have different crc" % (src_bfid, dst_bfid)
    elif src_file_record['sanity_cookie'] != dst_file_record['sanity_cookie']:
        err_msg = "%s and %s have different sanity_cookie" \
		  % (src_file_record, dst_file_record)
    else:
        err_msg = None
    if err_msg:
        if dst_file_record['deleted'] == migrate.YES \
	       and not migrate.is_swapped(src_bfid, fcc, db):
	    migrate.log(MY_TASK,
			"undoing duplication of %s to %s do to error" % \
			(src_bfid, dst_bfid))
	    migrate.log_uncopied(src_bfid, dst_bfid, fcc, db)
	return err_msg

    # check if p1 is writable
    if not os.access(src_path, os.W_OK):
        return "%s is not writable"%(src_path)

    # swapping metadata
    m1 = {'bfid': dst_bfid, 'pnfsid':src_file_record['pnfsid'],
	  'pnfs_name0':src_file_record['pnfs_name0']}
    res = fcc.modify(m1)
    # res = {'status': (e_errors.OK, None)}
    if not res['status'][0] == e_errors.OK:
        return "failed to change pnfsid for %s" % (dst_bfid,)

    # register duplication

    # get a duplication manager
    dm = duplication_util.DuplicationManager()
    rtn = dm.make_duplicate(src_bfid, dst_bfid)
    dm.db.close()
    return rtn


#This is a no-op for duplication.
def cleanup_after_scan(MY_TASK, mig_path, src_bfid, fcc, db):
	pass


#Note: db used only for migrate.py version of this function.
def is_expected_volume(MY_TASK, vol, likely_path, fcc, db):
	__pychecker__ = "unusednames=db"

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
	

