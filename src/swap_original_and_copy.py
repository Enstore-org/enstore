#!/usr/bin/env python
"""
swap_original_and_copy
"""

# system imports
import sys

# enstore imports
import duplication_util
import enstore_functions3
import pnfs

dm = duplication_util.DuplicationManager()

def usage():
	print "Pass the original bfid(s) or volume(s)."
	print
	print "Usage:"
	print "%s [[bfid1] [bfid2] ...] | [[vol1] [vol2] ...]" % (sys.argv[0])

#Internal swap, that does not do any verification or checking itself.
def __swap_bfid(bfid):
	print "swapping %s ..." % (bfid)
	res = dm.swap_original_and_copy(bfid)
	if res:
		print res, "... ERROR"
		return 1
	else:
		print "OK"
		return 0

def swap_bfid(bfid):
	if dm.is_primary(bfid):
		return __swap_bfid(bfid)
	else:
		print "%s is not a primary file" % (bfid,)
		return 1
	
def swap_volume(vol):
	# check if all files are primary
	q = "select bfid from file, volume where file.volume = volume.id and label = '%s' and deleted = 'n';"%(vol)
	res = dm.db.query(q).getresult()
	for i in res:
		bfid = res[i][0]
		if not dm.is_primary(bfid):
			print "%s is not a primary file" % (bfid,)
			return 1
	# now, swap it
	for i in res:
		bfid = res[i][0]
		rtn = __swap_bfid(bfid)
		if rtn:
			return rtn

	return 0

def main():
	bfid_list = []
	volume_list = []
	for target in sys.argv[1:]:
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
				#error_log("can not find bfid of", target)
				sys.stderr.write("can not find bfid of %s\n" % (target,))
				return 1

	for bfid in bfid_list:
		rtn1 = swap_bfid(bfid)
	for volume in volume_list:
		rtn2 = swap_volume(volume)

	sys.exit(rtn1 + rtn2)
	
if __name__ == '__main__':
	if len(sys.argv) < 2:
		usage()
		sys.exit()

	main()
