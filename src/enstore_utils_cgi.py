######################################################################
# src/$RCSfile$   $Revision$

import string
import os
import sys
import getpass
import posixpath
import regex_syntax
import regex
import glob

TMP_DIR = "/tmp/enstore"
TIMEOUT = 3
RETRIES = 2

def add_dir_to_ppath(setups, product):
    plen = len(product)
    for item in setups:
	# look for the setup for the product as we must add something
	# here to sys.path too
	if item[0:plen] == product:
	    pdir = os.popen(". /usr/local/etc/setups.sh;ups list -K @PROD_DIR %s"%(item,)).readlines()
	    pdir = string.strip(pdir[0])
	    pdir = string.replace(pdir, "\"", "")
	    return pdir
    return None

def find_libtppy(enstore_setups):
    dir = add_dir_to_ppath(enstore_setups, "libtppy")
    if not dir is None:
	sys.path.append("%s/lib"%(dir,))

def find_htmlgen(enstore_setups):
    dir = add_dir_to_ppath(enstore_setups, "HTMLgen")
    if not dir is None:
	sys.path.append("%s"%(dir,))

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
#    enstore_info = os.popen(". /usr/local/etc/setups.sh;setup enstore efb;ups list -K @PROD_DIR enstore;echo $ENSTORE_CONFIG_PORT;echo $ENSTORE_CONFIG_HOST;ups list -K action=setup enstore efb").readlines()
    enstore_dir = string.strip(enstore_info[0])
    enstore_dir = string.replace(enstore_dir, "\"", "")
    enstore_src = "%s/src"%(enstore_dir,)
    enstore_modules = "%s/modules"%(enstore_dir,)
    sys.path.append(enstore_src)
    sys.path.append(enstore_modules)
    es = string.split(string.strip(enstore_info[3]), "\"")
    find_libtppy(es)
    find_htmlgen(es)

    # fix up the config host and port to give to the command
    config_host = string.strip(enstore_info[2])
    config_port = string.strip(enstore_info[1])

    # we must create a pointer in the environment ot the trace key we are
    # going to use.   first see if the directory exists and if not create it.
    set_trace_key()

    return (config_host, config_port)

def set_pattern_search(pat, sensit):
    regex.set_syntax(regex_syntax.RE_SYNTAX_EGREP)
    if sensit:
	# case sensitive pattern matching.
	patr = regex.compile(pat)
    else:
	# case insensitive pattern matching
	patr = regex.compile(pat, regex.casefold)
    return patr

def pgrep_html(pat, files, sensit):
    patr = set_pattern_search(pat, sensit)
    for file in files:
	filename = string.split(file, "/")[-1]
	print "<H3>%s</H3><BR>"%(file,)
	lineno = 1
	for line in open(file, 'r').readlines():
	    if patr.search(line) >=0:
		# only print out the name of the file and not the directory path
		print '[<B>%s</B>] %04d) %s<BR>' %(filename, lineno, line)
	    lineno = lineno + 1
	print "<HR>"

def agrep_html(pat1, pat2, files, sensit):
    patr1 = set_pattern_search(pat1, sensit)
    if pat2:
	patr2 = set_pattern_search(pat2, sensit)
    else:
	patr2 = None
    import enstore_html
    import alarm
    matched_alarms = {}
    for file in files:
	date = string.split(file, "/")[-1][4:]
	for line in open(file, 'r').readlines():
	    if patr1.search(line) >= 0:
		if patr2:
		    rtn = patr2.search(line)
		else:
		    rtn = 1
		if rtn >= 0:
		    # we have an alarm line that matches both search strings, turn it 
		    # into an alarm
		    anAlarm = alarm.LogFileAlarm(line, date)
		    matched_alarms[anAlarm.id] = anAlarm

    doc = enstore_html.EnAlarmSearchPage()
    doc.body(matched_alarms)
    print str(doc)
