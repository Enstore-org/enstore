#!/fnal/ups/prd/python/v1_5_2/Linux+2/bin/python
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
        if form.has_key("logfile"):
            logfile = form["logfile"].value
        else:
            # the user did not enter a logfile name
            print "ERROR: Please enter a logfile name."
            raise SystemExit

	# we need to find the location of enstore so we can import
	(config_host, config_port) = enstore_utils_cgi.find_enstore()
	config_port = int(config_port)

	# add the config port and host to the environment
	#os.environ['ENSTORE_CONFIG_HOST'] = config_host
	#os.environ['ENSTORE_CONFIG_PORT'] = config_port

	# get a list of the log files we need
	import log_client
	logc = log_client.LoggerClient((config_host, config_port))
	ticket = logc.get_logfiles(logfile, enstore_utils_cgi.TIMEOUT,
				   enstore_utils_cgi.RETRIES)
	logfile_names = ticket['logfiles']
	if logfile_names == []:
	    # there were no matches
	    print cmd
	    print "<BR><P><HR><P>"
	    print "There were no log files that matched the entered description."
	else:
	    # put the files in alphabetical order
	    logfile_names.sort()
	    # for each name, search the file using the  search string
	    enstore_utils_cgi.pgrep_html(search_string, logfile_names)
    finally:
        print "</BODY></HTML>"


if __name__ == "__main__":

    go()
