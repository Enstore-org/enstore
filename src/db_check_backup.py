#!/usr/bin/env python
'''
db_check_backup.py -- check the integrity of a backup

restore.py can be invoked with no arguments other than normal enstore
common arguments (--config-host, ... etc.) or with a specific time
as argument. In the former case, db_check_backup.py checks the last
backup, while in the latter case, it checks the first backup that was
taken after the specified time.

The usage is as follows:

db_check_backup.py [enstore-arguments] [month day time [year]]

If a time is specified, at least month day and time must be specified.
year is optional and is only useful if you take backup from previous
years, which is unlikely available.

The format of time could be one of the followings:

hh, hh:mm, hh:mm:ss

Since backup is taken at 10 minutes after each hour, hh is sufficient.
The other formats are good for restoring from non-scheduled backup,
such as the ones that were done manually for maintanence reasons.

When the check is done, if the backup is good, it certifies a backup by
putting an empty file named 'CERTIFIED' in the backup directory. If the
backup is corrupted, it puts 'CORRUPTED'
'''

import restore
import db
import sys
import os
import time


# main

if __name__ == "__main__":		# main

	# get dbHome, bckHost and bckHome to start with

	intf = restore.Interface()

	if intf.dbHome:
		dbHome = intf.dbHome
	else:
		dbHome = '.'

	bckHost, bckHome = restore.getBckHostHome(intf)

	print " dbHome = "+dbHome
	print "bckHost = "+bckHost
	print "bckHome = "+bckHome

	bckTime = restore.parse_time(intf.args)

	print "Check backup from",
	if bckTime == -1:
		print "last backup?",
	else:
		print time.ctime(bckTime)+'?',
	print "(y/n) ",
	ans = sys.stdin.readline()
	if ans[0] != 'y' and ans[0] != 'Y':
		sys.exit(0)

	t0 = time.time()
	print time.ctime(t0)

	# databases

	dbs = ['file', 'volume']

	# retriving the backup

	print "Retriving database from backup ..."
	bckFile = restore.retrieveBackup(dbHome, None, bckHost, bckHome, bckTime)
	bckDir = os.path.dirname(bckFile)

	print "done retriving database from backup ..."

	print restore.cctime()
	
	# run db_recover to put the databases in consistent state

	print "Synchronize database using db_recover ..."
	os.system("db_recover -h "+dbHome)
	print "done synchronizing database using db_recover"

	# got to find index files!

	indexf = restore.getIndex(dbHome, dbs)

	print "checking database ..."
	for i in dbs:
		status = 0
		d = db.DbTable(i, dbHome, '/tmp', indexf[i], 0) 
		print restore.cctime()
		print "Checking "+i+" ... "
		if d.inx:
			for j in d.inx.keys():
				print "checking", i+':'+j, "..."
				status = status + d.inx[j].check()
				if d.inx[j].missing:
					print "missing ..."
					for k in i.inx[j].missing:
						print k,
					print
				if d.inx[j].extra:
					print "extra ..."
					for k in i.inx[j].extra:
						print k,
					print
		else:
			try:
				c = d.newCursor()
				k, v = c.first()
				count = 0
				while k:
					v = d[k]
					count = count + 1
					k, v = c.next()

				# check for corrupted cursor
				k, v = c.last()
				k, v = c.next()
				if k or v:
					status = 1
			except:
				status = 1
				
		print "database", i, "is",
		if status:
			print "corrupted."
			break
		else:
			print "fine"

		d.close()

	if status:	# corrupted!
		print "stamping entire backup as corrupted ...",
		os.system("enrsh -n "+bckHost+" touch "+os.path.join(bckDir, 'CORRUPTED'))
		print "done"
	else:
		print "certify entire backup ...",
		os.system("enrsh -n "+bckHost+" touch "+os.path.join(bckDir, 'CERTIFIED'))
		print "done"

	t1 = time.time()
	print time.cctime(t1)
	print "total leap time: %d seconds"%(t1-t0)
