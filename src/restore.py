#
# restore.py
#

import db
import os
import sys
import string

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

# getHomes() -- get dbHome, jouHome, bckHost and bckHome

def getHomes():
	
	import configuration_client
	import hostaddr

	intf = Interface()

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

# revertDB(dbHome, jouHome) -- revert to previous database files

def revertDB(dbHome, jouHome):

	# clean up dbHome

	for i in os.listdir(dbHome):
		s = string.split(i, ".saved")
		if len(s) == 2 and s[1] == '':
			f = os.path.join(dbHome, s[0])
			os.rename(f+'.saved', f)

	# remove extra journal files

	cleanUp_jou(jouHome)

# cleanUp(dbHome, jouHome) -- clean up un-necessary files

def cleanUp(dbHome, jouHome):

	# clean up dbHome

	cleanUp_db(dbHome)

	# remove extra journal files

	cleanUp_jou(jouHome)

# cleanUp_db(dbHome) -- clean up dbHome by deleting backup files

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
	
# retriveBackup(dbHome, jouHome, bckHost, bckHome, when)
#	actually retrive backup version of database and journal files

def retriveBackup(dbHome, jouHome, bckHost, bckHome, when = -1):

	compress_op = ""

	backups = []

	# get a list of backup directory available

	for i in os.popen("enrsh -n "+bckHost+" ls -1r "+bckHome).readlines():
		bf = string.strip(i)
		head, time1, time2 = string.split(bf, ".")
		timeStamp = float(time1+"."+time2)
		if timeStamp < when:
			break	# early exit
		backups.append(bf)
		if when < 0:
			break	# only take the last one
	# get to the right order again

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
		print "chdir to", dbHome, "...",
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
	
# suffix(s) -- returns the suffix of s

def suffix(s):

	s1 = string.split(s, '.')
	if len(s1) <= 1:
		return None
	else:
		return s1[-1]

# getIndex(dbHome, dbs) -- get the index name from the index files

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
		jfiles = os.popen(cmd).readlines()
		jfiles.append(jfile+"\012")	# to be same as the rest
		self.dict = {}
		for jf in jfiles:
			jf = jf[:-1]		# trim \012
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
			jf = jf[:-1]
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

import interface

class Interface(interface.Interface):

	def __init__(self):
		interface.Interface.__init__(self)

	def options(self):
		return self.config_options()

if __name__ == "__main__":		# main

	# get dbHome, jouHome, bckHost and bckHome to start with

	dbHome, jouHome, bckHost, bckHome = getHomes()

	print " dbHome = "+dbHome
	print "jouHome = "+jouHome
	print "bckHost = "+bckHost
	print "bckHome = "+bckHome

	# find the last backup
	# so far, we only deal with the last backup
	# In theory, if it is not good, we should go all the way back
	# to the "last good backup"

	cmd = "enrsh "+bckHost+" ls -1t "+bckHome+" | head -1"
	bckHome = bckHome+"/"+os.popen(cmd).readline()[:-1]

	print "restore from "+bckHost+":"+bckHome

	# databases
	dbs = ['file', 'volume']

	# save the current (bad?) database

	print "Saving current (bad?) database files ...",
	backupDB(dbHome, jouHome, dbs)
	print "done"

	# retriving the backup

	bckTime = -1	# this should be passed thorugh command line
			# in text format
	retriveBackup(dbHome, jouHome, bckHost, bckHome, bckTime)

	# run db_recover to put the databases in consistent state

	print "Synchronize database using db_recover"
	os.system("db_recover -h "+dbHome)

	# got to find index files!

	indexf = getIndex(dbHome, dbs)

	print "Restoring databse ..."
	for i in dbs:
		print "dbHome", dbHome
		print "jouHome", jouHome
		d = DbTable(i, dbHome, jouHome, indexf[i]) 
		print "Checking "+i+" ... "
		err = d.cross_check()
		if err:
			print "Fixing "+i+" ... "
			d.fix_db()
		else:
			print i+" is OK"
