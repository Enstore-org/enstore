#!/usr/bin/env python

# db_retrive_backup.py -- retrive database files from backup

import restore
import sys
import time

if __name__ == '__main__':

	intf = restore.Interface()
	dbHome, jouHome, bckHost, bckHome = restore.getHomes(intf)

	print " dbHome =", dbHome
	print "jouHome =", jouHome
	print "bckHost =", bckHost
	print "bckHome =", bckHome

	bckTime = restore.parse_time(intf.args)
	print "Retriving from",
	if bckTime == -1:
		print "last backup?",
	else:
		print time.ctime(bckTime)+'?',
	print "(y/n) ",
	ans = sys.stdin.readline()
	if ans[0] != 'y' and ans[0] != 'Y':
		sys.exit(0)

	restore.retriveBackup(dbHome, jouHome, bckHost, bckHome, bckTime)
