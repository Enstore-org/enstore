# dbs.py -- database related small functions

import os
import getopt
import restore
import interface
import configuration_client

class Interface(interface.Interface):

	def __init__(self):
		self.status = 0
		self.dump = 0
		self.all = 0
		interface.Interface.__init__(self)

	def options(self):
		return self.config_options()+["dump", "status", "all"]

	def charopts(self):
		return ["cd"]

dbs = ['volume', 'file']
# take care of common command line arguments

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

if not intf.all:
	dbs = intf.args

if intf.status:
	for i in dbs:
		d = restore.DbTable(i, dbHome, jouHome, [])
		print "Checking "+i+" ... "
		err = d.cross_check()
		if err:
			print I+" is inconsistent with journal"
		else:
			print i+" is OK"

if intf.dump:
	for i in dbs:
		d = restore.DbTable(i, dbHome, jouHome, [])
		d.dump()
