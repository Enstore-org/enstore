#!/usr/bin/env python
######################################################################
# src/$RCSfile$   $Revision$
#
import string
import os
import time
import signal

# since this is being run from a cron job on hppc, i do not want to import from
# enstore.  we would normally use get_remote_file from enstore_functions and
# VQFORMATED from enstore_constants.  instead define them here
#import enstore_functions
#import enstore_constants
VQFORMATED = "VOLUME_QUOTAS_FORMATED"

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
    for node in NODES:
	
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
	# output the total count
	file = open(TOTAL_FILE)
	file.write("%s %s\n"%(total, units))
	file.close()
