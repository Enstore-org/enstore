#!/usr/bin/env python
######################################################################
# src/$RCSfile$   $Revision$
#
import string

import enstore_functions
import enstore_constants

CTR_FILE = "/fnal/ups/prd/www_pages/enstore/enstore_system_user_data.html"
NODES = ["d0ensrv2", "cdfensrv2", "stkensrv2"]
TOTAL_FILE = "enstore_all_bytes"

if __name__ == "__main__":

    # get the 3 counter files and merge them into one
    # since we are not running on an enstore node, we will assume the web
    # directory is /fnal/ups/prd/www_pages/enstore.
    total = 0.0
    for node in NODES:
	
	newfile = "%s-%s"%(node, enstore_constants.VQFORMATED)
	rtn = enstore_functions.get_remote_file(node, CTR_FILE, newfile)
	if rtn == 0:
	    # read it
	    file = open(newfile)
	    lines = file.readlines()
	    for line in lines:
		fields = string.split(line)
		if len(fields) == 2:
		    total = total + float(fields[0])
	    else:
		file.close()
    else:
	# output the total count
	file = open(TOTAL_FILE)
	file.write("%s Bytes\n"%(total,))
	file.close()
