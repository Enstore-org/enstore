#!/usr/bin/env python
######################################################################
# src/$RCSfile$   $Revision$
#
import cgi
import string
import os
import posixpath
import sys
import tempfile
import re
import getpass
import enstore_utils_cgi

def go():
    # first print the two lines for the header
    print "Content-type: text/html"
    print

    # now start the real html
    print "<HTML><TITLE>Enstore Command Output</TITLE><BODY>"

    try:
        # get the data from the form
        form = cgi.FieldStorage()
        keys = form.keys()
        an_argv = []
        if form.has_key("search"):
            search_string = form["search"].value
        else:
	    search_string = ""

	# we need to find the location of enstore so we can import
	(config_host, config_port) = enstore_utils_cgi.find_enstore()
	config_port = int(config_port)

	import log_server
	import log_client
	import Trace
        if form.has_key("logfile"):
            logfile = form["logfile"].value
	    # as a convenience to the user, we will check if the user forgot to add
	    # the LOG- prefix onto the log file name, and add it ourselves.
	    for lkey in log_client.VALID_PERIODS.keys():
		if logfile == lkey:
		    # we found a match so we will not be adding the generic log
		    # file prefix to the name of the entered logfile
		    break
	    else:
                # assume that if the first character of the log file is a number
                # then, we need to add the file prefix.
                if logfile[0] in string.digits:
                    logfile = "%s%s"%(log_server.FILE_PREFIX, logfile)
        else:
            # the user did not enter an alarm timeframe, assume all
            logfile = "all"

	# get a list of the log files we need
	logc = log_client.LoggerClient((config_host, config_port))
	ticket = logc.get_logfiles(logfile, enstore_utils_cgi.TIMEOUT,
				   enstore_utils_cgi.RETRIES)
	logfile_names = ticket['logfiles']
	if not logfile_names:
	    # there were no matches
	    print "<BR><P>"
	    print "There were no log files found (to search for alarms) that matched the entered time frame."
	else:
	    # put the files in alphabetical order
	    logfile_names.sort()
	    # for each name, search the file for alarms and then each alarm using
	    # the search string
	    enstore_utils_cgi.agrep_html("%sALARM"%(Trace.MSG_TYPE,), search_string, 
					 logfile_names, 0)
    finally:
        print "</BODY></HTML>"


if __name__ == "__main__":

    go()
