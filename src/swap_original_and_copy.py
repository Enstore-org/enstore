#!/usr/bin/env python
"""
swap_original_and_copy
"""

import duplication_util
import sys

dm = duplication_util.DuplicationManager()

def usage():
	print "Usage:"
	print "%s --bfid bfid ..."%(sys.argv[0])
	print "%s --vol vol ..."%(sys.argv[0])

def swap_vol(vol):
	# check if all files are primary
	q = "select bfid from file, volume where file.volume = volume.id and label = '%s' and deleted = 'n';"%(vol)
	res = dm.db.query(q).getresult()
	for i in res:
		bfid = res[i][0]
		if not dm.is_primary(bfid):
			return "%s is not a primary file"
	# now, swap it
	for i in res:
		bfid = res[i][0]
		dm.swap_original_and_copy(bfid)
	
if __name__ == '__main__':
	if len(sys.argv) < 2:
		usage()
		sys.exit()

	if sys.argv[1] == '--vol':
		for i in sys.arg[2:]:
			print "swapping %s ..."%(i),
			res = swap_vol(i)
			if res:
				print res, "... ERROR"
			else:
				print "OK"
	elif sys.argv[1] == '--bfid':
		for i in sys.arg[2:]:
			print "swapping %s ..."%(i)
			if dm.is_primary(i):
				res = dm.swap_original_and_copy(i)
				if res:
					print res, "... ERROR"
				else:
					print "OK"
