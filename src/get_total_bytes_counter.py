#!/usr/bin/env python
######################################################################
# src/$RCSfile$   $Revision$
#
import string
import sys

import enstore_constants
import inventory

CDF = "cdf"
D0 = "d0"
STK = "stk"

LIBRARIES = {CDF : ["cdf", "A-Loan", "CDF-9940B", "CDF-Migration"],
	     D0 : ["mezsilo", "samlto", "samm2", "sammam", "shelf-sammam", "D0-9940B",
                   "samlto2"],
	     STK : ["9940", "eagle", "CMS-9940B", "CD-9940B", "dlt"]
	     }

def go(system, vq_file_name, vq_output_file, vq_output_file2):

    if system and system in LIBRARIES.keys():
	vq_libs = LIBRARIES[system]
    else:
	# assume all systems
	vq_libs = []
	for system in LIBRARIES.keys():
	    vq_libs = vq_libs + LIBRARIES[system]

    # read it in and pull out the libraries that we want
    vq_file = open(vq_file_name, 'r')
    total_bytes = 0.0
    for line in vq_file.readlines():
	fields = string.split(line)
	if len(fields) == 2:
	    lib = fields[0]
	    bytes = fields[1]
	else:
	    # this line has the wrong format, skip it
	    continue
	if lib in vq_libs:
	    # get rid of the newline
	    bytes = string.strip(bytes)
	    total_bytes = total_bytes + float(bytes)
    else:
	# output the file that has the number of bytes in it.
	vq_file.close()
	output_file = open(vq_output_file, 'w')
	output_file.write("%.3f TB\n"%(total_bytes/1099510000000.0))
	output_file.close()
	output_file = open(vq_output_file2, 'w')
	output_file.write("%.3f\n"%(total_bytes))
	output_file.close()


if __name__ == "__main__":

    # get the file we need to write
    argc = len(sys.argv)
    if argc > 1:
	vq_output_file = sys.argv[1]
        vq_output_file2 = "%s2"%(vq_output_file,)

	# get the system from the args
	if argc > 2:
	    system = sys.argv[2]
	else:
	    system = ""

	# get the file we need to read
	if argc > 3:
	    vq_file_name = sys.argv[3]
	else:
	    # we were not passed a name, get the default name from the 
	    # inventory file
	    dirs = inventory.inventory_dirs()
	    vq_file_name = inventory.get_vq_format_file(dirs[0])

	go(system, vq_file_name, vq_output_file, vq_output_file2)
    else:
	# this is an error we need to be given the file to write
	pass

