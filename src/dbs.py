# dbs.py -- database related small functions

import os
import getopt
import time
import sys
import string

import restore
import interface
import configuration_client

# define in 1 place all the hoary pieces of the command needed to access an
# entire enstore system.
# Yes, all those blasted slashes are needed and I agree it is insane. We should
# loop on rsh and dump rgang
CMDa = "(F=~/\\\\\\`hostname\\\\\\`."
CMDb = ";echo >>\\\\\\$F;date>>\\\\\\$F;. /usr/local/etc/setups.sh>>\\\\\\$F; setup enstore>>\\\\\\$F;"
CMDc = ";echo >>\\\\\\$F;date>>\\\\\\$F;. /usr/local/etc/setups.sh>>\\\\\\$F; setup enstore -r /devel/berman/enstore  -M ups -m enstore.table>>\\\\\\$F;"
CMD1 = "%s%s%s"%(CMDa, "database", CMDc)
# the tee is not robust - need to add code to check if we can write to tty (that is connected to console server)
CMD2 = "|tee /dev/console>>\\\\\\$F;date>>\\\\\\$F) 1>&- 2>&- <&- &"

class Interface(interface.Interface):

	def __init__(self, flag=1, opts=[]):
	        self.do_parse = flag
		self.restricted_opts = opts
 		self.status = 0
		self.dump = 0
		self.all = 0
		self.nocheck = 0
		interface.Interface.__init__(self)

	def options(self):
	        if self.restricted_opts:
		        return self.restricted_opts
		else:
		        return self.config_options()+["dump", "status", "all", "nocheck"]

	def charopts(self):
		return ["cd"]

def send_dbs_cmd(intf, farmlet, db):
    # build the command and send it to the correct node
    cmd = "enstore database --nocheck"
    if intf.status:
	    cmd = "%s --status "%cmd
    if intf.dump:
	    cmd = "%s --dump "%cmd
    if intf.all:
            cmd = "%s --all "%cmd
    cmd = "%s %s %s%s"%(CMD1, cmd, db, CMD2)
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

def do_node_check(csc, dbs, do_v, do_f, intf):
    this_node = os.uname()[1]
    if do_v:
	    vc = csc.get("volume_clerk")
	    vc_node = vc.get("host", "")
	    on_vc_node = compare_nodes(vc_node, this_node)
	    if not on_vc_node:
		    # we are not running on the volume_clerk node but we need
		    # to access it's db, so we must send a command to do
		    # this remotely and delete it from the things to do
		    send_dbs_cmd(intf, vc_node, "volume")
		    try:
			i = dbs.index("volume")
			del dbs[i]
		    except ValueError:
			pass
    if do_f:
	    fc = csc.get("file_clerk")
	    fc_node = fc.get("host", "")
	    on_fc_node = compare_nodes(fc_node, this_node)
	    if not on_fc_node:
		    # we are not running on the file_clerk node but we need
		    # to access it's db, so we must send a command to do
		    # this remotely and delete it from the things to do
		    send_dbs_cmd(intf, fc_node, "file")
		    try:
			i = dbs.index("file")
			del dbs[i]
		    except ValueError:
			pass
    return dbs

def do_work(intf):

    do_volume = 0
    do_file = 0
    if not intf.all:
	    dbs = intf.args
	    for db in dbs:
		    if db == "volume":
			    do_volume = 1
		    elif db == "file":
			    do_file = 1

    else:
	    dbs = ['volume', 'file']
	    do_volume = 1
	    do_file = 1
    # find out if we are running on the file_clerk and/or volume_clerk node.
    # get the nodes that the volume clerk and file clerk run on, and our's
    csc = configuration_client.ConfigurationClient((intf.config_host,
						    intf.config_port))
    # we may not need to check if this is the right node as we may have been
    # sent this command explicitly because we are on the correct node
    if not intf.nocheck:
	    dbs = do_node_check(csc, dbs, do_volume, do_file, intf)

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
