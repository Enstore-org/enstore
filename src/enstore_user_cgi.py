#!/usr/hppc_home/www/cgi-bin/python
######################################################################
# src/$RCSfile$   $Revision$
#
import cgi
import string
import os
import sys
import tempfile
import re

ENSTORE_USER_NODES = "enstore_user_nodes.txt"
FERMI_DOMAIN = "^131.225."

def append_from_key(argv, value_text_key, form, alt_name=""):
    if not alt_name:
        alt_name = value_text_key
    if form.has_key(value_text_key):
        value_text = form[value_text_key].value
        argv.append("--%s=%s"%(alt_name,value_text))
    else:
        # no text was entered, if there should have been text, the parsing
        # of the command itself will pick this up and give an error
        argv.append("--%s"%alt_name)
    return argv
        
def append_from_value(argv, value, server, form, alt_name=""):
    value_text_key = "%s_%s"%(server, value)
    return append_from_key(argv, value_text_key, form, alt_name)

def print_keys(keys, form):
    for key in keys:
        try:
            print "%s = %s"%(key, form[key].value)
        except AttributeError:
            print "No value for %s"%key

def in_domain(host, domain):
    m = re.search(domain, host)
    if m is None:
        return 0
    else:
        return 1

def no_access(host):
    print "<FONT SIZE=+3>Sorry, your host (%s) is not allowed to query Enstore.</FONT>"%host
    print "</BODY></HTML>"
        
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

def find_enstore():
    enstore_info = os.popen(". /usr/local/etc/setups.sh;setup enstore;ups list -K @PROD_DIR enstore;echo $ENSTORE_CONFIG_PORT;echo $ENSTORE_CONFIG_HOST;ups list -K action=setup enstore").readlines()
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
    return (config_host, config_port)

def go():
    # first print the two lines for the header
    print "Content-type: text/html"
    print

    # now start the real html
    print "<HTML><TITLE>Enstore Command Output</TITLE><BODY>"

    # first determine if a request from this node is allowed to happen
    # the node had better be listed in a file called enstore_user_nodes.txt
    # which is located in the current directory
    try:
        filestat = os.stat(ENSTORE_USER_NODES)
        # read in the file
        filedes = open(ENSTORE_USER_NODES)
        # for each line in the file, see if the remote node is in the line
        found_it = 0
        host = os.environ["REMOTE_ADDR"]
        while 1:
            line = filedes.readline()
            if not line:
                break
            else:
                if in_domain(host, line):
                    found_it = 1
                    break
        filedes.close()
        if not found_it:
            no_access(host)
            raise SystemExit
    except OSError:
        # the file did not exist, only allow addresses from *.fnal.gov
        if not in_domain(os.environ["REMOTE_ADDR"], FERMI_DOMAIN):
            no_access(os.environ["REMOTE_ADDR"])
            raise SystemExit
    try:
        # get the data from the form
        form = cgi.FieldStorage()
        keys = form.keys()
        an_argv = []
        if form.has_key("server"):
            server = form["server"].value
        else:
            # the user did not select a server
            print "ERROR: Please select a command (e.g. library)."
            raise SystemExit
        # we will construct an argv and an argc to pass to our python
        # program 
        an_argv = ["enstore", server]

	# we need to find the location of enstore so we can import
	(config_host, config_port) = find_enstore()

	# add the config port and host to the environment
	os.environ['ENSTORE_CONFIG_HOST'] = config_host
	os.environ['ENSTORE_CONFIG_PORT'] = config_port

        # look for any of the possibly multiple checkbox info
        main_cbox_key = "%s_cbox"%server
        if form.has_key(main_cbox_key):
            main_cbox = form[main_cbox_key]
            if type(main_cbox) is type([]):
                # multiple checkboxes were checked
                for item in main_cbox:
                    value = item.value
                    an_argv = append_from_value(an_argv, value, server,
                                                form, value)
            else:
                value = main_cbox.value
                an_argv = append_from_value(an_argv, value, server,
                                            form, value)

        # get the main option field value
        main_opt_key = "%s_opts"%server
        if form.has_key(main_opt_key):
            main_opt = form[main_opt_key].value
        else:
            # the user did not select a command
            print "ERROR: Please select an option (and value) for this command (e.g. bfid)."
            raise SystemExit

        # get any text associated with the main option. the value of the main
        # option will have the same name as the text associated with that opt
        an_argv = append_from_key(an_argv, main_opt, form)

        # get any additional parameters if they exist
        main_opt_text_key = "%s_p"%main_opt
        if form.has_key(main_opt_text_key):
            main_opt_text = form[main_opt_text_key].value
            an_argv = an_argv + string.split(main_opt_text)
            
        # now that we have the argv built up, call the routines to do the real
        # stuff
        cmd = string.join(an_argv, " ")
        print cmd
        print "<BR><P><HR><P><PRE>"

	# do our stuff
	sys.argv = an_argv
	import enstore_user
	try:
	    enstore_user.do_work()
            print "</PRE>"
	except SystemExit:
            print "</PRE>"
    finally:
        if not an_argv:
            print "\nERROR: Could not process command"
        print "</BODY></HTML>"


if __name__ == "__main__":

    go()
