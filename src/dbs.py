# dbs.py -- database related small functions

import os
import getopt
import time
import sys

import restore
import interface
import configuration_client

class Interface(interface.Interface):

	def __init__(self, flag=1, opts=[]):
	        self.do_parse = flag
		self.restricted_opts = opts
 		self.status = 0
		self.dump = 0
		self.all = 0
		interface.Interface.__init__(self)

	def options(self):
	        if self.restricted_opts:
		        return self.restricted_opts
		else:
		        return self.config_options()+["dump", "status", "all"]

	def charopts(self):
		return ["cd"]

def do_work(intf):

    dbs = ['volume', 'file']
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
		    try:
			    d = restore.DbTable(i, dbHome, jouHome, [])
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
			    else:
				    print i+" is OK"
		    except:
			    print i + " is corrupt"

    if intf.dump:
	    for i in dbs:
		    d = restore.DbTable(i, dbHome, jouHome, [])
		    d.dump()

    if not intf.all and not intf.status and not intf.dump:
	intf.print_help()
	sys.exit(0)

if __name__ == "__main__":

    # take care of common command line arguments

    intf = Interface()

    do_work(intf, dbs)
