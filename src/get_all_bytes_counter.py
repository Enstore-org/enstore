#!/usr/bin/env python
######################################################################
# src/$RCSfile$   $Revision$
#
import string
import os
import time
import signal

# since this is being run from a cron job on hppc, i do not want to import from
# enstore.  we would normally use get_remote_file & ping from enstore_functions and
# VQFORMATED from enstore_constants.  instead define them here
#import enstore_functions
#import enstore_constants
VQFORMATED = "VOLUME_QUOTAS_FORMATED"
DEAD = 0
ALIVE = 1

def ping(node):
    # ping the node to see if it is up.
    times_to_ping = 4
    # the timeout parameter does not work on d0ensrv2.
    timeout = 5
    #cmd = "ping -c %s -w %s %s"%(times_to_ping, timeout, node)
    cmd = "ping -c %s %s"%(times_to_ping, node)
    p = os.popen(cmd, 'r').readlines()
    for line in p:
        if not string.find(line, "transmitted") == -1:
            # this is the statistics line
            stats = string.split(line)
            if stats[0] == stats[3]:
                # transmitted packets = received packets
                return ALIVE
            else:
                return DEAD
    else:
        # we did not find the stat line
        return DEAD

def get_remote_file(node, file, newfile):
    # we have to make sure that the rcp does not hang in case the remote node is goofy
    pid = os.fork()
    if pid == 0:
        # this is the child
        rtn = os.system("enrcp %s:%s %s"%(node, file, newfile))
        os._exit(rtn)
    else:
        # this is the parent, allow a total of 30 seconds for the child
        for i in [0, 1, 2, 3, 4, 5]:
            rtn = os.waitpid(pid, os.WNOHANG)
            if rtn[0] == pid:
                return rtn[1] >> 8   # pick out the top 8 bits as the return code
            time.sleep(5)
        else:
            # the child has not finished, be brutal. it may be hung
            os.kill(pid, signal.SIGKILL)
            return 1

CTR_FILE = "/fnal/ups/prd/www_pages/enstore/enstore_system_user_data.html"
NODES = ["d0ensrv2", "cdfensrv2", "stkensrv2"]
TOTAL_FILE = "enstore_all_bytes"

if __name__ == "__main__":

    # get the 3 counter files and merge them into one
    # since we are not running on an enstore node, we will assume the web
    # directory is /fnal/ups/prd/www_pages/enstore.
    total = 0.0
    units = ""
    dead_nodes = []
    for node in NODES:
        # make sure node is up before rcping
        if ping(node) == ALIVE:
	    newfile = "/tmp/%s-%s"%(node, VQFORMATED)
	    rtn = get_remote_file(node, CTR_FILE, newfile)
	    if rtn == 0:
		# read it
		file = open(newfile)
		lines = file.readlines()
		for line in lines:
		    fields = string.split(line)
		    if len(fields) == 2:
			total = total + float(fields[0])
			units = fields[1]
		else:
		    file.close()
	    else:
		# no info from this node
		dead_nodes.append(node)
	else:
	    dead_nodes.append(node)
    else:
	# find out if we have any dead nodes
	if dead_nodes:
	    str = "(does not include - "
	    dead_nodes.sort()
	    for node in dead_nodes:
		str = "%s, %s"%(str, node)
	else:
	    str = ""
	# output the total count
	file = open(TOTAL_FILE, 'w')
	file.write("%s %s %s\n"%(total, units, str))
	file.close()
