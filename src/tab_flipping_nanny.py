#!/usr/bin/env python

import operation
import time

if __name__ == '__main__':
	print "Recommended tap flipping jobs on %s"%(operation.cluster), time.ctime(time.time())
	print
	print "Recommended write protect on jobs:"
	print
	res = operation.recommend_write_protect_job(limit=1000000)
	for i in res:
		operation.show_cap(i, res[i])
	total = 0
	caps = len(res)
	for i in res.keys():
		total = total + len(res[i])
	print "%d tapes in %d caps"%(total, caps)
	print
	print"Recommended write protect off jobs:"
	print
	res = operation.recommend_write_permit_job(limit=1000000)
	for i in res:
		operation.show_cap(i, res[i])
	total = 0
	caps = len(res)
	for i in res.keys():
		total = total + len(res[i])
	print "%d tapes in %d caps"%(total, caps)


