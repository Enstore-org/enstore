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
            # the user did not enter a search string
            print "ERROR: Please enter a search string."
            raise SystemExit

	# we need to find the location of enstore so we can import
	(config_host, config_port) = enstore_utils_cgi.find_enstore()
	config_port = int(config_port)

	import log_client
	import log_server
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
		# assume that if the first character of the log file is a nubmer, then
		# we need to add the file prefix.
		if logfile[0] in string.digits:
		    logfile = "%s%s"%(log_server.FILE_PREFIX, logfile)
        else:
            # the user did not enter a logfile name assume all
	    logfile = "all"

	# get a list of the log files we need
	logc = log_client.LoggerClient((config_host, config_port))
	ticket = logc.get_logfiles(logfile, enstore_utils_cgi.TIMEOUT,
				   enstore_utils_cgi.RETRIES)
	logfile_names = ticket['logfiles']
	if logfile_names == []:
	    # there were no matches
	    print "<BR><P>"
	    print "There were no log files that matched the entered description."
	else:
	    # put the files in alphabetical order
	    logfile_names.sort()
	    # for each name, search the file using the  search string
	    enstore_utils_cgi.pgrep_html(search_string, logfile_names, 0)
    finally:
        print "</BODY></HTML>"


if __name__ == "__main__":

    go()
