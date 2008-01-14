#!/usr/bin/env python
"""
duplicate.py -- duplication utility

Duplication is very similar to migration.
The difference is, duplication keeps both copies and makes the new files
as duplicates of the original ones.

The code is borrowed from migrate.py. It is imported and modified.
"""

import file_clerk_client
import pnfs
import migrate
import duplication_util
import sys
import string

# get a duplication manager

dm = duplication_util.DuplicationManager()

# modifying migrate module
migrate.DEFAULT_LIBRARY = 'LTO4'
migrate.MIGRATION_FILE_FAMILY_KEY = "_copy_1"
migrate.MFROM = "<-"
migrate.MTO = "->"

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
	return dm.make_duplicate(bfid1, bfid2)

migrate.swap_metadata = duplicate_metadata

# init() -- initialization

def init():
	migrate.init()

def usage():
	print "usage:"
	print "  %s <file list>"%(sys.argv[0])
	print "  %s --bfids <bfid list>"%(sys.argv[0])
	print "  %s --vol <volume list>"%(sys.argv[0])

if __name__ == '__main__':
	if len(sys.argv) < 2 or sys.argv[1] == "--help":
		usage()
		sys.exit(0)

	init()

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


	if sys.argv[1] == "--vol":
		icheck = False
		for i in sys.argv[2:]:
			migrate.migrate_volume(i)
	elif sys.argv[1] == "--bfids":
		files = []
		for i in sys.argv[2:]:
			files.append(i)
		migrate.migrate(sys.argv[2:])
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
		migrate.migrate(files)
