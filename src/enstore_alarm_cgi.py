#!/usr/bin/env python
#!/usr/products/python/v1_5_2/bin/python
#!/fnal/ups/prd/python/v1_5_2/Linux+2/bin/python
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
    print "<HTML><TITLE>Enstore Alarm Resolution</TITLE><BODY>"

    try:
        # get the data from the form
        form = cgi.FieldStorage()
        keys = form.keys()
        an_argv = []
	# there may be more than one alarm that was chosen to be
	# cancelled, so get them all
	alarms = []
        for key in form.keys():
            if key[0:5] == "alarm":
                alarms.append(float(string.strip(form[key].value)))
        else:
            if not alarms:
                # not to decide, is to decide
                print "ERROR: No alarm chosen for resolution."
                raise SystemExit

	# we need to find the location of enstore so we can import
	(config_host, config_port) = enstore_utils_cgi.find_enstore()
	config_port = int(config_port)

	import alarm_client
	import Trace
	import e_errors
	alc = alarm_client.AlarmClient((config_host, config_port))
	print "<PRE>"
	for alarm in alarms:
	    ticket = alc.resolve(alarm)
	    if ticket.get("status", "") == (e_errors.OK, None):
		msg = "Alarm with id = %s has been resolved."%(alarm,)
		print msg
		# log that the alarm was cancelled
		Trace.log(e_errors.INFO, msg)
	    else:
		print "Could not resolve alarm with id = %s."%(alarm,)
                if ticket.get('status', ""):
                    print "  Return status = (%s, %s)"%(ticket['status'][0],
                                                        ticket['status'][1])
	else:
	    print "</PRE>"
		
    finally:
        print "</BODY></HTML>"


if __name__ == "__main__":

    go()
