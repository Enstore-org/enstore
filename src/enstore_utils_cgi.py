######################################################################
# src/$RCSfile$   $Revision$

import string
import os
import sys
import getpass
import posixpath
import regex_syntax
import regex

TMP_DIR = "/tmp/enstore"
TIMEOUT = 3
RETRIES = 2

def find_libtppy(enstore_setups):
    es = string.strip(enstore_setups)
    es = string.split(es, "\"")
    for item in es:
	# look for the setup for the libtppy product as we must add something
	# here to sys.path too
	if item[0:7] == "libtppy":
	    libtppy_dir = os.popen(". /usr/local/etc/setups.sh;ups list -K @PROD_DIR %s"%item).readlines()
	    libtppy_dir = string.strip(libtppy_dir[0])
	    libtppy_dir = string.replace(libtppy_dir, "\"", "")
	    sys.path.append("%s/lib"%libtppy_dir)

def set_trace_key():
    # get who we are
    us = getpass.getuser()
    us_dir = "%s/%s"%(TMP_DIR, us)
    # check if the directory /tmp/enstore/us exists.  if not create it.
    if not posixpath.exists(TMP_DIR):
	# the path did not exist, create it
	os.mkdir(TMP_DIR)
	os.mkdir(us_dir)
    else:
	if not posixpath.exists(us_dir):
	    os.mkdir(us_dir)
    # set an environment variable that will tell trace where to put the key
    os.environ["TRACE_KEY"] = "%s/%s"%(us_dir, "trace.cgi")

def find_enstore():
    enstore_info = os.popen(". /usr/local/etc/setups.sh;setup enstore;ups list -K @PROD_DIR enstore;echo $ENSTORE_CONFIG_PORT;echo $ENSTORE_CONFIG_HOST;ups list -K action=setup enstore").readlines()
#    enstore_info = os.popen(". /usr/local/etc/setups.sh;setup enstore efb;ups list -K @PROD_DIR enstore;echo $ENSTORE_CONFIG_PORT;echo $ENSTORE_CONFIG_HOST;ups list -K action=setup enstore").readlines()
    enstore_dir = string.strip(enstore_info[0])
    enstore_dir = string.replace(enstore_dir, "\"", "")
    enstore_src = "%s/src"%enstore_dir
    enstore_modules = "%s/modules"%enstore_dir
    sys.path.append(enstore_src)
    sys.path.append(enstore_modules)
    find_libtppy(enstore_info[3])

    # fix up the config host and port to give to the command
    config_host = string.strip(enstore_info[2])
    config_port = string.strip(enstore_info[1])

    # we must create a pointer in the environment ot the trace key we are
    # going to use.   first see if the directory exists and if not create it.
    set_trace_key()

    return (config_host, config_port)


def pgrep_html(pat, files):
    regex.set_syntax(regex_syntax.RE_SYNTAX_EGREP)
    patr = regex.compile(pat)
    for file in glob.glob(files):
	lineno = 1
	for line in open(file, 'r').readlines():
	    if patr.search(line) >=0:
		print '[<B>%s</B>] %04d) ' %(file, lineno), line,
	    lineno = lineno + 1
	print "<HR>"
