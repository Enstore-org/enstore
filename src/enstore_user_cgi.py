#!/usr/bin/env python
#/fnal/ups/prd/python/v1_5_2/Linux+2/bin/python
######################################################################
# src/$RCSfile$   $Revision$
#
import cgi
import string
import os
import sys
import tempfile
import enstore_utils_cgi

def append_from_key(argv, value_text_key, form, alt_name=""):
    if not alt_name:
        alt_name = value_text_key
    if form.has_key(value_text_key):
        value_text = form[value_text_key].value
        argv.append("--%s=%s"%(alt_name,value_text))
    else:
        # no text was entered, if there should have been text, the parsing
        # of the command itself will pick this up and give an error
        argv.append("--%s"%(alt_name,))
    return argv
        
def append_from_value(argv, value, server, form, alt_name=""):
    value_text_key = "%s_%s"%(server, value)
    return append_from_key(argv, value_text_key, form, alt_name)

def print_keys(keys, form):
    for key in keys:
        try:
            print "%s = %s"%(key, form[key].value)
        except AttributeError:
            print "No value for %s"%(key,)

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
	(config_host, config_port) = enstore_utils_cgi.find_enstore()

	# add the config port and host to the environment
	os.environ['ENSTORE_CONFIG_HOST'] = config_host
	os.environ['ENSTORE_CONFIG_PORT'] = config_port

        # look for any of the possibly multiple checkbox info
        main_cbox_key = "%s_cbox"%(server,)
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
        main_opt_key = "%s_opts"%(server,)
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
        main_opt_text_key = "%s_p"%(main_opt,)
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
