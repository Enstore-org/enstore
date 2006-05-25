#!/usr/bin/env python

import operation
import time
import sys
import os

LIMIT = 10 

if __name__ == '__main__':
	stdout_save = sys.stdout
	if len(sys.argv) > 1:
		sys.stdout = open(sys.argv[1], "w")
	print "Recommended tap flipping jobs on %s"%(operation.cluster), time.ctime(time.time())
	print
	print "Recommended write protect on jobs:"
	print
	res = operation.recommend_write_protect_job(limit=1000000)
	for i in res:
		operation.show_cap(i, res[i])
	total = 0
	oncaps = len(res)
	for i in res.keys():
		total = total + len(res[i])
	print "%d tapes in %d caps"%(total, oncaps)
	print
	print"Recommended write protect off jobs:"
	print
	res = operation.recommend_write_permit_job(limit=1000000)
	for i in res:
		operation.show_cap(i, res[i])
	total = 0
	offcaps = len(res)
	for i in res.keys():
		total = total + len(res[i])
	print "%d tapes in %d caps"%(total, offcaps)

	sys.stdout = stdout_save
	if len(sys.argv) > 1 and (oncaps >= LIMIT or offcaps >= LIMIT):
		cmd = 'cat %s'%(sys.argv[1])
		os.system(cmd)
		cmd = '/usr/bin/Mail -s "tab flipping watch" %s < %s '%(os.environ['ENSTORE_MAIL'], sys.argv[1])
		os.system(cmd)
