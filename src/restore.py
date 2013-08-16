#!/usr/bin/env python
'''
restore.py

Restore database files from backup
This procedure trusts journal files. That is, using a good backup and
replaying all the journal records since then should be able to put
database files to a consistent state when the last journal was
written.

restore.py can be invoked with no arguments other than normal enstore
common arguments (--config-host, ... etc.) or with a specific time
as argument. In the former case, restore.py uses only the last backup,
while in the latter case, it starts from the first backup that was
taken after the specified time and all the journals after that backup.

The usage is as follows:

restore.py [enstore-arguments] [month day time [year]]

If a time is specified, at least month day and time must be specified.
year is optional and is only useful if you take backup from previous
years, which is unlikely available.

The format of time could be one of the followings:

hh, hh:mm, hh:mm:ss

Since backup is taken at 10 minutes after each hour, hh is sufficient.
The other formats are good for restoring from non-scheduled backup,
such as the ones that were done manually for maintanence reasons.
'''

import db
import os
import sys
import string
import time
import configuration_client
import hostaddr

# ddiff(o1, o2) -- comparing two objects
#		Complex objects, like lists and dictionaries, are
#		compared recurrsively.
#
#		Simple objects are compared by their text representation
#		Truncating error may happen.
#		This is on purpose so that internal time stamp, which is
#		a float, will not be considered different from the same
#		in journal file, which is a text representation and
#		probably with truncated precision

def ddiff(o1, o2):
	'''
ddiff(o1, o2) -- comparing two objects
		Complex objects, like lists and dictionaries, are
		compared recurrsively.

		Simple objects are compared by their text representation
		Truncating error may happen.
		This is on purpose so that internal time stamp, which is
		a float, will not be considered different from the same
		in journal file, which is a text representation and
		probably with truncated precision
	'''

	# different if of different types
	if type(o1) != type(o2):
		return 1

	# list?
	if type(o1) == type([]):
		if len(o1) != len(o2):
			return 1
		for i in range(0, len(o1)):
			if ddiff(o1[i], o2[i]):
				return 1
		return 0

	# dictionary?
	if type(o1) == type({}):
		if len(o1) != len(o2):
			return 1
		for i in o1.keys():
			if ddiff(o1[i], o2[i]):
				return 1
		return 0

	# for everything else
	return `o1` != `o2`

# suffix(s) -- returns the suffix of s

def suffix(s):

	s1 = string.split(s, '.')
	if len(s1) <= 1:
		return None
	else:
		return s1[-1]

# getDbJouHomes(intf) -- get dbHome and jouHome

def getDbJouHomes(intf):
	
	# find dbHome and jouHome
	try:
		dbInfo = configuration_client.ConfigurationClient(
			(intf.config_host, intf.config_port)).get(
			'database')
        	dbHome = dbInfo['db_dir']
        	try:  # backward compatible
			jouHome = dbInfo['jou_dir']
		except:
			jouHome = dbHome
	except:
		dbHome = os.environ['ENSTORE_DIR']
		jouHome = dbHome

	# overide dnHome and jouHome from interface ...

	if intf.dbHome:
		dbHome = intf.dbHome
	if intf.jouHome:
		jouHome = intf.jouHome

	return dbHome, jouHome

# getBckHostHome(intf) -- get bckHost and bckHome

def getBckHostHome(intf):

	# find backup host and path
	backup_config = configuration_client.ConfigurationClient(
		(intf.config_host,
		intf.config_port)).get('backup')

	local_host = hostaddr.gethostinfo()[0]
	try:
		bckHost = backup_config['host']
	except:
		bckHost = local_host

	bckHome = backup_config['dir']

	return bckHost, bckHome

# getHomes(intf) -- get dbHome, jouHome, bckHost and bckHome

def getHomes(intf):
	
	dbHome, jouHome = getDbJouHomes(intf)
	bckHost, bckHome = getBckHostHome(intf)

	return dbHome, jouHome, bckHost, bckHome

# backupDB(dbHome, jouHome, dbs) -- backup the old databas files

def backupDB(dbHome, jouHome, dbs):

	prefix = []
	for i in dbs:		# a deep copy
		prefix.append(i+'.')
	prefix.append('log.')

	for i in os.listdir(dbHome):
		for j in prefix:
			if string.find(i, j) == 0: # from the beginning
				f = os.path.join(dbHome, i)
				os.rename(f, f+".saved")

# cleanUp_db(dbHome) -- clean up dbHome by deleting backup files

def cleanUp_db(dbHome):

	# clean up dbHome

	for i in os.listdir(dbHome):
		if suffix(i) == "saved":
			f = os.path.join(dbHome, i)
			os.remove(f)

# cleanUp_jou(jouHome) -- clean up jouHome by deleting old journal files

def cleanUp_jou(jouHome):

	# remove extra journal files

	for i in os.listdir(jouHome):
		s = string.split(i, ".jou")
		if len(s) >= 2 and s[1] != '':
			f = os.path.join(jouHome, i)
			os.remove(f)
	
# cleanUp(dbHome, jouHome) -- clean up un-necessary files

def cleanUp(dbHome, jouHome):

	# clean up dbHome

	cleanUp_db(dbHome)

	# remove extra journal files

	cleanUp_jou(jouHome)

# revertDB(dbHome, jouHome) -- revert to previous database files
#				in case of aborting the restoration

def revertDB(dbHome, jouHome):

	# clean up dbHome

	for i in os.listdir(dbHome):
		s = string.split(i, ".saved")
		if len(s) == 2 and s[1] == '':
			f = os.path.join(dbHome, s[0])
			os.rename(f+'.saved', f)

	# remove extra journal files

	cleanUp_jou(jouHome)

# retrieveBackup(dbHome, jouHome, bckHost, bckHome, when)
#	actually retrieve backup version of database and journal files
#	from bckHost:bckHome
#
# "when" is a time stamp, in float, same as returned by time.time()
# default value of "when", if unspecified, is -1, meaning taking the
# last backup. If when is specified, the first backup that was taken
# after the specified time will be taken, as well as all the journal
# files after that backup.

def retrieveBackup(dbHome, jouHome, bckHost, bckHome, when = -1):
	'''
retrieveBackup(dbHome, jouHome, bckHost, bckHome, when)
	actually retrieve backup version of database and journal files
	from bckHost:bckHome

"when" is a time stamp, in float, same as returned by time.time()
default value of "when", if unspecified, is -1, meaning taking the
last backup. If when is specified, the first backup that was taken
after the specified time will be taken, as well as all the journal
files after that backup.
	'''

	compress_op = ""

	backups = []

	# get a list of backup directory available

	for i in os.popen("enrsh -n "+bckHost+" ls -1t "+bckHome).readlines():
		if i[:6] == 'dbase.':
			bf = string.strip(i)
			head, time1, time2 = string.split(bf, ".")
			timeStamp = float(time1+"."+time2)
			if timeStamp < when:
				break	# early exit
			backups.append(bf)
			if when < 0:
				break	# only take the last one

	# get the right order again

	backups.sort()

	# untar the database files

	bckFile = string.strip(os.popen("enrsh -n "+bckHost+" ls "+
		os.path.join(bckHome, backups[0], "dbase.tar*")).readline())
	s = suffix(bckFile)
	if s == "gz" or s == "Z":
		compress_op = " gunzip -c "
	else:
		compress_op = " cat "

	print "chdir to", dbHome, "...",
	os.chdir(dbHome)
	print "ok"

	cmd = "enrsh -n "+bckHost+" '"+compress_op+bckFile+"| dd bs=20b' | tar xvBfb - 20"
	print cmd
	os.system(cmd)

	# taking care of journal files

	if len(backups) > 1:
		if jouHome:
			print "chdir to", jouHome, "...",
			os.chdir(jouHome)
			print "ok"
			for i in backups[1:]:
				fileJou = os.path.join(bckHome, i, "file.tar.gz")
				volJou = os.path.join(bckHome, i, "volume.tar.gz")
				cmd = "enrsh -n "+bckHost+" '"+compress_op+fileJou+"| dd bs=20b' | tar xvBfb - 20"
				print cmd
				os.system(cmd)
				cmd = "enrsh -n "+bckHost+" '"+compress_op+volJou+"| dd bs=20b' | tar xvBfb - 20"
				print cmd
				os.system(cmd)

	return bckFile
	
# getIndex(dbHome, dbs) -- get the index name from the index files
#
#	index file names are of the following format:
#
#	database.index_field.index
#
#	where ".index" is the suffix, database is the name of the
#	primary database file and index_field is the name of the field
#	in the primary database file which the index is built on and for
#
#	Therefore, by scanning all the index files, we should be able to
#	figure out all the index fields

def getIndex(dbHome, dbs):
	indexf = {}
	for i in dbs:
		indexf[i] = []

	for i in os.listdir(dbHome):
		s = string.split(i, ".")
		if len(s) == 3 and s[0] in dbs and s[2] == "index":
			indexf[s[0]].append(s[1])
	return indexf

# similar to db.DbTable, without automatic journaling and backup up.

class DbTable(db.DbTable):

	# __init__() is almost the same as db.DbTable.__init__()
	# plus a "deletes" list containing the deleted keys

	def __init__(self, dbname, dbHome, jouHome, indlst=[]):
		db.DbTable.__init__(self, dbname, dbHome, jouHome, indlst, 0)

		# Now let's deal with all the journal files

		jfile = self.jouHome+"/"+dbname+".jou"
		cmd = "ls -1 "+jfile+".* 2>/dev/null"
		jfiles = map(string.strip, os.popen(cmd).readlines())
		if os.access(jfile, os.F_OK):
			jfiles.append(jfile)
		self.dict = {}
		for jf in jfiles:
			try:
				f = open(jf,"r")
			except IOError:
				print(jf+": not found")
				sys.exit(0)
			while 1:
				l = f.readline()
				if len(l) == 0: break
				exec(l)
			f.close()

		# read it twice to get the deletion list

		self.deletes = []
		for jf in jfiles:
			try:
				f = open(jf,"r")
			except IOError:
				print(jf+": not found")
				sys.exit(0)
			while 1:
				l = f.readline()
				if len(l) == 0: break
				if l[0:3] == 'del':
					k = string.split(l, "'")[1]
					if not self.dict.has_key(k):
						self.deletes.append(k)
			f.close()
			
	# cross_check() cross check journal dictionary and database

	def cross_check(self):

		error = 0

		# check if the items in db has the same value of that
		# in journal dictionary

		for i in self.dict.keys():
			if not self.has_key(i):
				print 'M> key('+i+') is not in database'
				error = error + 1
			elif ddiff(self.dict[i], self.__getitem__(i)):
				print 'C> database and journal disagree on key('+i+')'
				print 'C>  journal['+i+'] =', self.dict[i]
				print 'C> database['+i+'] =', self.__getitem__(i)
				error = error + 1
		# check if the deleted items are still in db

		for i in self.deletes:
			if self.has_key(i):
				print 'D> database['+i+'] should be deleted'
				error = error + 1

		return error

	# fix_db() fix db according to journal dictionary

	def fix_db(self):

		error = 0

		# check if the items in db has the same value of that
		# in journal dictionary

		for i in self.dict.keys():
			if not self.has_key(i):
				# add it
				print 'M> key('+i+') is not in database'
				self.__setitem__(i, self.dict[i])
				error = error + 1
				print 'F> database['+i+'] =', self.dict[i]
			elif ddiff(self.dict[i], self.__getitem__(i)):
				print 'C> database and journal disagree on key('+i+')'
				print 'C>  journal['+i+'] =', self.dict[i]
				print 'C> database['+i+'] =', self.__getitem__(i)
				self.__setitem__(i, self.dict[i])
				print 'F> database['+i+'] =', self.dict[i]
				error = error + 1
		# check if the deleted items are still in db

		for i in self.deletes:
			if self.has_key(i):
				print 'D> database['+i+'] should be deleted'
				self.__delitem__(i)
				print 'F> delete database['+i+']'
				error = error + 1

		return error

import option

class Interface(option.Interface):

	def __init__(self):
		self.dbHome = None
		self.jouHome = None
		option.Interface.__init__(self)

	def valid_dictionaries(self):
	    return (self.config_options, self.restore_options)

	restore_options = {
	    option.DBHOME:{option.HELP_STRING:"CHIH needs to fill this in",
			   option.VALUE_TYPE:option.STRING,
			   option.VALUE_USAGE:option.REQUIRED,
			   option.VALUE_LABEL:"directory",
			   option.USER_LEVEL:option.ADMIN,
			   },
	    option.JOUHOME:{option.HELP_STRING:"CHIH needs to fill this in",
			    option.VALUE_TYPE:option.STRING,
			    option.VALUE_USAGE:option.REQUIRED,
			    option.VALUE_LABEL:"directory",
			    option.USER_LEVEL:option.ADMIN,
			    }
	    }

	parameters = '[mon day hh[:mm[:ss]] [year]]'

# parse_time(args) -- get the time

def parse_time(args):

	l1 = len(args)

	if l1 > 2 and l1 < 5:
		(year, mon, day, hour, min, sec, wday, jday, dsave) \
			= time.localtime(time.time())
		min = 0
		sec = 0
		mon = int(args[0])		# month
		day = int(args[1])		# day
		# hh:mm:ss
		t = string.split(args[2], ':')
		l = len(t)
		hour = int(t[0])
		if l > 1:
			min = int(t[1])
		if l > 2:
			sec = int(t[2])
		if l1 > 3:
			year = int(args[3])	# year

		return time.mktime((year, mon, day, hour, min, sec, wday, jday, dsave))
	return -1

# cctime() -- current time in character format

def cctime():
	return time.ctime(time.time())

# main

if __name__ == "__main__":		# main

	# get dbHome, jouHome, bckHost and bckHome to start with

	intf = Interface()
	dbHome, jouHome, bckHost, bckHome = getHomes(intf)

	print " dbHome = "+dbHome
	print "jouHome = "+jouHome
	print "bckHost = "+bckHost
	print "bckHome = "+bckHome

	bckTime = parse_time(intf.args)

	print "Restore from",
	if bckTime == -1:
		print "last backup?",
	else:
		print time.ctime(bckTime)+'?',
	print "(y/n) ",
	ans = sys.stdin.readline()
	if ans[0] != 'y' and ans[0] != 'Y':
		sys.exit(0)

	print cctime()

	# databases
	dbs = ['file', 'volume']

	# save the current (bad?) database

	print "Saving current (bad?) database files ...",
	backupDB(dbHome, jouHome, dbs)
	print "done"

	print cctime()

	# retriving the backup

	print "Retriving database from backup ..."
	retrieveBackup(dbHome, jouHome, bckHost, bckHome, bckTime)
	print "done retriving database from backup ..."

	print cctime()
	
	# run db_recover to put the databases in consistent state

	print "Synchronize database using db_recover ..."
	os.system("db_recover -h "+dbHome)
	print "done synchronizing database using db_recover"

	# got to find index files!

	indexf = getIndex(dbHome, dbs)

	print "Restoring database ..."
	for i in dbs:
		d = DbTable(i, dbHome, jouHome, indexf[i]) 
		print cctime()
		print "Checking "+i+" ... "
		err = d.cross_check()
		if err:
			print "Fixing "+i+" ... "
			d.fix_db()
		else:
			print i+" is OK"
		d.close()

	print cctime()
