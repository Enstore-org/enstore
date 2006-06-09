#!/usr/bin/env python

import operation
import time
import sys
import os

LIMIT = 10 

one_day = 60 * 60 * 24
seven_days = one_day * 7

TMP_FILE = '/tmp/tab_flipping_nanny.tmp'

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
	# check last time a ticket was cut
	onltt = operation.get_last_write_protect_on_job_time()
	if time.time() - onltt > seven_days:
		print "==> Last ticket was cut 7 or more days ago ...", time.ctime(onltt)
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
	print
	# check last time a ticket was cut
	offltt = operation.get_last_write_protect_off_job_time()
	if time.time() - offltt > seven_days:
		print "==> Last ticket was cut 7 or more days ago ...", time.ctime(offltt)
		print
	sys.stdout = stdout_save
	if len(sys.argv) > 1 and (oncaps >= LIMIT or offcaps >= LIMIT):
		cmd = 'cat %s'%(sys.argv[1])
		os.system(cmd)
		cmd = '/usr/bin/Mail -s "tab flipping watch" %s < %s '%(os.environ['ENSTORE_MAIL'], sys.argv[1])
		os.system(cmd)

	# should we generate the ticket?
	if len(sys.argv) > 1 and oncaps and (
		oncaps >= LIMIT or time.time() - onltt > seven_days):
		res = operation.execute('auto_write_protect_on')
		f = open(TMP_FILE, 'w')
		f.write("A write_protection_on ticket is generated for %s at %s\n\n"%(operation.cluster, time.ctime(time.time())))
		for i in res:
			f.write(i+'\n')
		f.close()
		cmd = '/usr/bin/Mail -s "write protection on job generated" %s < %s '%(os.environ['ENSTORE_MAIL'], TMP_FILE)
		os.system(cmd)
