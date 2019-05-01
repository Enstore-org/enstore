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

The code is borrowed from migrate.py (now it is migrate_chimera.py). It is imported and modified.
"""

# system imports
import sys
import os
import string
import re
import errno

# enstore imports
import volume_clerk_client
#import pnfs
import chimera
import migrate_chimera
import duplication_util_chimera as duplication_util
import e_errors
import Trace
import option
import delete_at_exit

# modifying migrate module
migrate_chimera.MIGRATION_FILE_FAMILY_KEY = "_copy_%s" #Not equals to (==) safe.
migrate_chimera.INHIBIT_STATE = "duplicated"
migrate_chimera.IN_PROGRESS_STATE = "duplicating"
migrate_chimera.MIGRATION_NAME = "DUPLICATION"
migrate_chimera.set_system_migrated_func=volume_clerk_client.VolumeClerkClient.set_system_duplicated
migrate_chimera.set_system_migrating_func=volume_clerk_client.VolumeClerkClient.set_system_duplicating
migrate_chimera.MFROM = "<-"
migrate_chimera.MTO = "->"
migrate_chimera.LOG_DIR = "/var/duplication"
migrate_chimera.LOG_FILE = migrate_chimera.LOG_FILE.replace('Migration', 'Duplication')

DuplicateInterface = migrate_chimera.MigrateInterface
DuplicateInterface.migrate_options[option.MAKE_FAILED_COPIES] = {
    option.HELP_STRING:
    "Make duplicates where the multiple copy write failed.",
    option.VALUE_USAGE:option.IGNORED,
    option.VALUE_TYPE:option.INTEGER,
    option.USER_LEVEL:option.USER,
    }
DuplicateInterface.migrate_options[option.MAKE_COPIES] = {
    option.HELP_STRING:"Make copies of the supplied volume group.",
    option.VALUE_USAGE:option.IGNORED,
    option.VALUE_TYPE:option.INTEGER,
    option.USER_LEVEL:option.USER,
    option.FORCE_SET_DEFAULT:option.FORCE,
    option.EXTRA_VALUES:[
    {option.VALUE_NAME:"media_type",
     option.VALUE_TYPE:option.STRING,
     option.VALUE_USAGE:option.REQUIRED,},
    {option.VALUE_NAME:"library__",  #Avoid collision with --library.
     option.VALUE_TYPE:option.STRING,
     option.VALUE_USAGE:option.OPTIONAL,
     option.VALUE_LABEL:"library"},
    {option.VALUE_NAME:"storage_group",
     option.VALUE_TYPE:option.STRING,
     option.VALUE_USAGE:option.OPTIONAL,},
    {option.VALUE_NAME:"file_family",
     option.VALUE_TYPE:option.STRING,
     option.VALUE_USAGE:option.OPTIONAL,},
    {option.VALUE_NAME:"wrapper",
     option.VALUE_TYPE:option.STRING,
     option.VALUE_USAGE:option.OPTIONAL,},
    ]}

#Avoid duplicate code testing for possible okay error messages.
def handle_string_return_code(rtn_str,txt):
    if rtn_str and rtn_str.find(txt) != -1:
        return None  #Error returned, but for this case pretend it's okay.
    elif rtn_str:
        return rtn_str  #Error.

    return ""  #Success.

# search_order()
#Return in the following order:
#  1) first bfid to check
#  2) second bfid to check
#  3) first bfid's file record
#  4) second bfid's file record
#This is necessary to optimize the search order for both migration and
# duplication.  It orders the bfids to determine which is the active one
# in PNFS.
def search_order_duplication(src_bfid, src_file_record, dst_bfid,
                             dst_file_record,
                             is_it_copied, is_it_swapped, fcc, db):
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

    #arguments is_it_copied and is_it_swapped used by migrate_chimera.py version.
    __pychecker__="unusednames=is_it_copied,is_it_swapped"

    if dst_bfid:
        duplicates = migrate_chimera.is_duplicated(dst_bfid, fcc, db)
        if src_bfid in duplicates:
            #If the original and duplicate have been swapped to leave
            # the duplicate copy as the primary copy, we need to alter the
            # order.

            return dst_bfid, src_bfid, dst_file_record, src_file_record

    return src_bfid, dst_bfid, src_file_record, dst_file_record

# migration_file_family(ff) -- making up a file family for migration
def migration_file_family_duplication(bfid, ff, fcc, intf, deleted=migrate_chimera.NO):
    reply_ticket = fcc.find_all_copies(bfid)
    if e_errors.is_ok(reply_ticket):
        count = len(reply_ticket['copies'])
    else:
        raise e_errors.EnstoreError(None, reply_ticket['status'][1],
				    reply_ticket['status'][0])

    if deleted == migrate_chimera.YES:
        return migrate_chimera.DELETED_FILE_FAMILY + migrate_chimera.MIGRATION_FILE_FAMILY_KEY % (count,)
    else:
        if intf.file_family:
            return intf.file_family + migrate_chimera.MIGRATION_FILE_FAMILY_KEY % (count,)
        else:
            return ff + migrate_chimera.MIGRATION_FILE_FAMILY_KEY % (count,)

# normal_file_family(ff) -- making up a normal file family from a
#				migration file family
def normal_file_family_duplication(ff):
    return re.sub("_copy_[0-9]*", "", ff, 1)

#Return True if the file_family has the pattern of a migration/duplication
# file.  False otherwise.
def is_migration_file_family_duplication(ff):
    if re.search("_copy_[0-9]*", ff) == None:
        return False

    return True

#This does everything that the migrate_chimera.py version does, plus inserts
# the multiple_copy/duplication relationship into the file_copies_map
# table.
def log_copied_duplication(src_bfid, dst_bfid, fcc, db):
    rtn_val = migrate_chimera.log_copied_migration(src_bfid, dst_bfid, fcc, db)
    if rtn_val:
        return rtn_val  #Error.

    # register duplication

    # get a duplication manager
    dm = duplication_util.DuplicationManager()
    rtn_str = dm.make_duplicate(src_bfid, dst_bfid)
    dm.db.close()

    if handle_string_return_code(rtn_str, "are already copies"):
        return 1  #Error
    else:
        return 0  #Success

#This does everything that the migrate_chimera.py version does, plus removes
# the multiple_copy/duplication relationship from the file_copies_map
# table.
def log_uncopied_duplication(src_bfid, dst_bfid, fcc, db):
    rtn_val = migrate_chimera.log_uncopied_migration(src_bfid, dst_bfid, fcc, db)
    if rtn_val:
        return rtn_val  #Error.

    # unregister duplication

    # get a duplication manager
    dm = duplication_util.DuplicationManager()
    rtn_str = dm.unmake_duplicate(src_bfid, dst_bfid)
    dm.db.close()

    if handle_string_return_code(rtn_str, "are already removed"):
        return 1  #Error
    else:
        return 0  #Success.

#When duplicating to multiple copies, we need to make sure we pick the
# first original, not the ultimate original.
def find_original_duplication(bfid, fcc):
    original_reply = fcc.find_original(bfid)
    f0 = {}
    if e_errors.is_ok(original_reply) \
           and original_reply['original'] != None \
           and original_reply['original'] != bfid:
        f0 = fcc.bfid_info(original_reply['original'])

    return f0

# This is to change the behavior of migrate_chimera.swap_metadata.
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
def _duplicate_metadata(my_task, job, fcc, db):

    return_error, job, p1, p2, f0, is_migrating_multiple_copy = \
		  migrate_chimera._verify_metadata(my_task, job, fcc, db)

    if return_error:
        #The return_error value is a string with the error message.
        return return_error

    #Get information about the files to copy and swap.
    (src_file_record, src_volume_record, src_path,
     dst_file_record, dst_volume_record, tmp_path, mig_path) = job

    #shortcuts
    src_bfid = src_file_record['bfid']
    dst_bfid = dst_file_record['bfid']

    # SFA: check if duplicated file is a package
    package_id = src_file_record.get("package_id", None)
    package_files_count = src_file_record.get("package_files_count", 0)
    src_is_a_package = (package_id is not None) and (src_bfid == package_id)

    # update destination file metadata: set pnfs id and path
    mod_record = {'bfid': dst_bfid,
	  'pnfsid':src_file_record['pnfsid'],
	  'pnfs_name0':src_file_record['pnfs_name0']}
    if src_file_record['deleted'] == migrate_chimera.YES:
        if f0:
            #We need to do use f0 when fixing files migrated to multiple
            # copies before the constraints in the DB were modified
            # to make the pair (src_bfid, dst_bfid) unique instead of
            # each column src_bfid & dst_bfid being unique.
            mod_record['deleted'] = f0['deleted']
        else:
            mod_record['deleted'] = migrate_chimera.YES
    res = fcc.modify(mod_record)
    if not e_errors.is_ok(res['status']):
        return "failed to change pnfsid for %s" % (dst_bfid,)

    # SFA: if duplicated file is a package and not empty, create children for new package
    if src_is_a_package and package_files_count > 0:
        rc = migrate_chimera.dup_packaged_meta_one(my_task, src_file_record, dst_bfid, fcc)
        if rc:
            return "failed to create children bfids for package %s" % (dst_bfid,)
    
    # register duplication
    rtn = None
    try:
        # get duplication manager
        dm = duplication_util.DuplicationManager()
        if not dm.is_primary_and_copy(src_bfid, dst_bfid):
            #If the file_copies_map table was not set by log_copied(), we
            # need to do so now.  This can happen if the duplication began
            # with duplicate.py less than 1.37 or log_copied() starting with
            # revision 1.37 failed between updating the two tables.
            if src_file_record['deleted'] == migrate_chimera.YES:
                #The make_duplicate() function doesn't handle deleted files.
                rtn = dm.register_duplicate(src_bfid, dst_bfid)
            elif src_file_record['deleted'] == migrate_chimera.NO:
                rtn = dm.make_duplicate(src_bfid, dst_bfid)
            else:
                #We should never get here!
                pass
    finally:
        dm.db.close()

    if rtn:
        return handle_string_return_code(rtn, "are already copies")
    
    return rtn

def duplicate_metadata(job, fcc, db):
    my_task = "DUPLICATE_METADATA"

    #Get information about the files to copy and swap.
    (src_file_record, src_volume_record, src_path,
     dst_file_record, dst_volume_record, tmp_path, mig_path) = job

    #Don't continue if this is already done.  However, do log that the
    # metadata updating is already done.  Be sure to verify that
    # the migration table has the migration side as "swapped" and the
    # file_copies_map has recorded the multiple_copy side of the duplication.
    if dst_file_record and \
           migrate_chimera.is_swapped(src_file_record['bfid'], fcc, db) and \
           migrate_chimera.is_duplication(dst_file_record['bfid'], db) and \
           migrate_chimera.is_duplication(src_file_record['bfid'], db):
        migrate_chimera.ok_log(my_task, "%s %s %s %s have already been duplicated" \
               % (src_file_record['bfid'], src_path,
                  dst_file_record['bfid'], mig_path))
        return None

    res = _duplicate_metadata(my_task, job, fcc, db)

    if res:
        migrate_chimera.error_log(my_task,
                  "%s %s %s %s failed due to %s" \
                  % (src_file_record['bfid'], src_path,
                     dst_file_record['bfid'], mig_path, res))
    else:
        migrate_chimera.ok_log(my_task,
               "%s %s %s %s have been duplicated" \
               % (src_file_record['bfid'], src_path,
                  dst_file_record['bfid'], mig_path))

    return res

#This function shall be called for original destination copies.
#  This excludes multiple copies made during migration and multiple copies
#  of multiple copies.
#This shall, also, be called if --make-copies or --make-failed-copies was
# specified.
def cleanup_after_scan_duplication(my_task, mig_path, src_bfid, fcc, db):
    #src_bfid, fcc and db are migrate_chimera.py specific.
    __pychecker__ = "unusednames=src_bfid,fcc,db"
    return migrate_chimera.cleanup_after_scan_common(my_task, mig_path)


#Note: db used only for migrate_chimera.py version of this function.
def is_expected_volume_duplication(my_task, vol, likely_path, fcc, db):
	__pychecker__ = "unusednames=db"

	#Confirm that the destination volume matches the volume that
	# pnfs is pointing to.  This is true for swapped duplicate
	# files.
	pf = chimera.File(likely_path)
	pf_volume = getattr(pf, "volume", None)
	if pf_volume == None:
		message = "No file info for %s. " % (likely_path,)
		migrate_chimera.error_log(my_task, message)
	elif pf_volume != vol:
		pf_bfid =  getattr(pf, "bfid", None)
		#Get the original and make sure the original volume
		# is the same.  This is true for non-swapped duplicate
		# files.
		original_file_info = fcc.bfid_info(pf_bfid, 10, 10)
		if not e_errors.is_ok(original_file_info):
			message = "No file info for bfid %s." % (pf_bfid,)
			migrate_chimera.error_log(my_task, message)
                        pf.show()
			return False

		#If the original volume and the volume we are scanning
		# does not match the volume in pnfs layer 4, report
		# the error.
		if pf_volume != original_file_info['external_label']:
			message = "wrong volume %s (expecting %s or %s)" % \
				  (pf.volume, vol,
				   original_file_info['external_label'])
			migrate_chimera.error_log(my_task, message)
			return False

	return True


#Duplication doesn't do cloning.
def setup_cloning_duplication():
    pass

##
## Override migration functions with those for duplication.
##
migrate_chimera.is_expected_volume = is_expected_volume_duplication
migrate_chimera.cleanup_after_scan = cleanup_after_scan_duplication
migrate_chimera.find_original = find_original_duplication
migrate_chimera.swap_metadata = duplicate_metadata
migrate_chimera.is_expected_restore_type = migrate_chimera.is_duplication
migrate_chimera.setup_cloning = setup_cloning_duplication
migrate_chimera.migration_file_family = migration_file_family_duplication
migrate_chimera.normal_file_family = normal_file_family_duplication
migrate_chimera.is_migration_file_family = is_migration_file_family_duplication
migrate_chimera.log_copied = log_copied_duplication
migrate_chimera.log_uncopied = log_uncopied_duplication
migrate_chimera.search_order = search_order_duplication

if __name__ == '__main__':

        Trace.init(migrate_chimera.MIGRATION_NAME)
        Trace.do_message(0)

        delete_at_exit.setup_signal_handling()

        intf_of_migrate = migrate_chimera.MigrateInterface(sys.argv, 0) # zero means admin

        try:
            migrate_chimera.do_work(intf_of_migrate)
        except (OSError, IOError), msg:
            if msg.errno == errno.EPIPE:
                #User piped the output to another process, but
                # didn't read all the data from the migrate process.
                pass
            else:
                raise sys.exc_info()[0], sys.exc_info()[1], \
                      sys.exc_info()[2]
