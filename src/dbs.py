# dbs.py -- database related small functions
#   Even though we are working with 2 different databases, we are assuming these
#   databases are both accessible from the same node.  This is due to the fact
#   that there is only one entry in the config file for the databases.  For no
#   particular reason, we choose the volume clerk node to check.

import db
import interface
import os
import getopt
import time
import sys
import string
import hostaddr

import interface
import configuration_client

# define in 1 place all the hoary pieces of the command needed to access an
# entire enstore system.
# Yes, all those blasted slashes are needed and I agree it is insane. We should
# loop on rsh and dump rgang
CMDa = "(F=~/\\\\\\`hostname\\\\\\`."
CMDb = ";echo >>\\\\\\$F 2>&1;date>>\\\\\\$F 2>&1;. /usr/local/etc/setups.sh>>\\\\\\$F 2>&1; setup enstore>>\\\\\\$F 2>&1;"
CMDc = ";echo >>\\\\\\$F 2>&1;date>>\\\\\\$F 2>&1;. /usr/local/etc/setups.sh>>\\\\\\$F 2>&1; setup enstore -r /devel/berman/enstore  -M ups -m enstore.table>>\\\\\\$F 2>&1;"
CMD1 = "%s%s%s"%(CMDa, "database", CMDb)
#CMD1 = "%s%s%s"%(CMDa, "database", CMDc)
# the tee is not robust - need to add code to check if we can write to tty (that is connected to console server)
CMD2 = "|tee /dev/console>>\\\\\\$F 2>&1;date>>\\\\\\$F 2>&1) 1>&- 2>&- <&- &"

def send_dbs_cmd(intf, farmlet, db):
    # build the command and send it to the correct node
    cmd = "enstore database --nocheck"
    if intf.status:
	    cmd = "%s --status "%cmd
    if intf.dump:
	    cmd = "%s --dump "%cmd
    if intf.restore_all:
	    cmd = "%s --restore_all "%cmd
	    db = ""
    if intf.all:
            cmd = "%s --all "%cmd
    cmd = "%s %s %s%s"%(CMD1, cmd, string.join(db), CMD2)
    # we need just the node name part of the host name
    node = string.split(farmlet, ".", 1)
    return os.system('/usr/local/bin/rgang %s \"%s\"'%(node[0], cmd))

# compare the 2 input nodes to see if they are the same.  one may be of the 
# form node.fnal.gov and the other just 'NODE' and they should still be reported 
# as the same.
def compare_nodes(node1, node2):
    dot = "."
    # do all comparisons in lowercase
    lnode1 = string.lower(node1)
    lnode2 = string.lower(node2)
    lnode1_pieces = string.split(lnode1, dot)
    lnode2_pieces = string.split(lnode2, dot)
    num_pieces = len(lnode1_pieces)
    i = 0
    while i < num_pieces:
	if not lnode1_pieces[i] == lnode2_pieces[i]:
	    return 0
	i = i + 1
	if i >= len(lnode2_pieces):
	    return 1
    else:
	return 1

def do_node_check(csc, dbs, intf):
    this_node = hostaddr.gethostinfo()[0]
    vc = csc.get("volume_clerk")
    vc_node = vc.get("host", "")
    if not compare_nodes(vc_node, this_node):
	    # we are not running on the volume_clerk node but we need
	    # to access it's db, so we must send a command to do
	    # this remotely and delete it from the things to do
	    send_dbs_cmd(intf, vc_node, dbs)
	    dbs = []
    return dbs

def restore(csc, intf, dbs, dbHome, jouHome):
	# find backup host and path
	backup_config = csc.get('backup')
	local_host = hostaddr.gethostinfo()[0]
	try:
		bckHost = backup_config['host']
	except KeyError:
		bckHost = local_host

	bckHome = backup_config['dir']

	print " dbHome = "+dbHome
	print "jouHome = "+jouHome
	print "bckHost = "+bckHost
	print "bckHome = "+bckHome

	# find the last backup
	# so far, we only deal with the last backup
	# In theory, if it is not good, we should go all the way back
	# to the "last good backup"

	cmd = "rsh "+bckHost+" ls -1t "+bckHome+" | head -1"
	bckHome = bckHome+"/"+os.popen(cmd).readline()[:-1]

	print "restore from "+bckHost+":"+bckHome

	# go to dbHome
	os.chdir(dbHome)
	print "cd "+dbHome

	# save the current (bad?) database
	print "Save current (bad?) database files ..."
	for i in dbs:
		cmd = "mv "+i+" "+i+".saved"
		try:	# if it doesn't succeed, never mind
			os.system(cmd)
			print cmd
		except:
			pass

	# save all log.* files
	print "Save current (bad?) log files"
	cmd = "ls -1 log.*"
	logs = os.popen(cmd).readlines()
	for i in logs:
		i = i[:-1]	# trim \012
		cmd = "mv "+i+" "+i+".saved"
		try:	# if it doesn't succeed, never mind
			os.system(cmd)
			print cmd
		except:
			pass

	# check the type of compression

	compression = os.popen("rsh -n "+bckHost+" ls "+bckHome+"/dbase.tar*").readline()[-2:-1]
	if compression == "z" or compression == "Z":	# decompress them first
		cmd = "rsh -n "+bckHost+" gunzip "+bckHome+"/*"
		print "decompressing the backup files ..."
		print cmd
		os.system(cmd)

	# get the database file from backup

	print "Retriving database file from backup ("+bckHost+":"+bckHome+")"

	cmd = "rsh -n "+bckHost+" dd if="+bckHome+"/dbase.tar bs=20b | tar xvBfb - 20"
	print cmd
	os.system(cmd)

	# run db_recover to put the databases in consistent state

	print "Synchronize database using db_recover"
	os.system("db_recover")

	# get the journal files in place
	# basically, we only need current journal file to restore the
	# databases from the previous backup. Just to be paranoid,
	# we still get some previous journal files ...

	os.chdir(jouHome)
	print "cd "+jouHome

	print "Retriving journal files from backup ("+bckHost+":"+bckHome+")"
	for i in dbs:
		cmd = "rsh -n "+bckHost+" dd if="+bckHome+"/"+i+\
			".tar bs=20b | tar xvBfb - 20 '"+i+".jou*'"
		os.system(cmd)
		print cmd

	print "Restoring database ..."
	for i in dbs:
		d = DbTable(i, dbHome, jouHome, [])
		print "Checking "+i+" ... "
		err = d.cross_check()
		if err:
			print "Fixing "+i+" ... "
			d.fix_db()
		else:
			print i+" is OK"

def do_work(intf):
    rtn = 0
    csc = configuration_client.ConfigurationClient((intf.config_host,
						    intf.config_port))
    # find dbHome and jouHome
    try:
	    dbInfo = csc.get('database')
	    dbHome = dbInfo['db_dir']
	    try:  # backward compatible
		    jouHome = dbInfo['jou_dir']
	    except KeyError:
		    jouHome = dbHome
    except:
	    dbHome = os.environ['ENSTORE_DIR']
	    jouHome = dbHome

    work_done = 0
    if intf.restore_all :
	    dbs = ['volume', 'file']
	    work_done = 1
	    # we may not need to check if this is the right node as we may have been
	    # sent this command explicitly because we are on the correct node
	    if not intf.nocheck:
		    # find out if we are running on the volume_clerk node.
		    # get the node that the volume clerk runs on, and our's
		    dbs = do_node_check(csc, dbs, intf)
	    if dbs:
		os.system("enstore stop --just file_clerk")
		os.system("enstore stop --just volume_clerk")
		os.system("enstore stop --just data")
		restore(csc, intf, dbs, dbHome, jouHome)
		os.system("enstore start --just file_clerk")
		os.system("enstore start --just volume_clerk")
    else:
	    if not intf.all:
		    dbs = intf.args
	    else: 
		    dbs = ['volume', 'file']
	    # we may not need to check if this is the right node as we may have been
	    # sent this command explicitly because we are on the correct node
	    if not intf.nocheck:
		    # find out if we are running on the volume_clerk node.
		    # get the node that the volume clerk runs on, and our's
		    dbs = do_node_check(csc, dbs, intf)

	    if intf.status:
		    work_done = 1
		    for i in dbs:
			    try:
				    d = DbTable(i, dbHome, jouHome, [])
				    print "Scanning "+i+" ..."
				    t0 = time.time()
				    l = len(d)
				    t = time.time() - t0
				    print "%d records in %f seconds : %f records/second" % (
					    l, t, l/t)
				    print "Checking "+i+" against journal ..."
				    err = d.cross_check()
				    if err:
					    print i+" is inconsistent with journal"
					    rtn = 1
				    else:
					    print i+" is OK"
			    except:
				    print i + " is corrupt"
				    rtn = 1

	    if intf.dump:
		    work_done = 1
		    for i in dbs:
			    d = DbTable(i, dbHome, jouHome, [])
			    d.dump()

    if not work_done:
 	    intf.print_help()
	    sys.exit(rtn)

    return rtn

cursor_open = 0
if 0: print cursor_open #quiet the linter

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
			elif `self.dict[i]` != `self.__getitem__(i)`:
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
			elif self.dict[i] != self.__getitem__(i):
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

class Interface(interface.Interface):

	def __init__(self, flag=1, opts=[]):
	        self.do_parse = flag
		self.restricted_opts = opts
 		self.status = 0
		self.dump = 0
		self.all = 0
		self.restore_all = 0
		self.nocheck = 0
		interface.Interface.__init__(self)

	def options(self):
	        if self.restricted_opts:
		        return self.restricted_opts
		else:
		        return self.config_options()+["dump", "status", "all", 
						      "restore_all", "nocheck"]

	def charopts(self):
		return ["cd"]

if __name__ == "__main__":

    # take care of common command line arguments

    intf = Interface()

    sys.exit(do_work(intf))

