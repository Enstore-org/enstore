import os
import sys
import string
import re

import enstore_constants
import enstore_functions
import generic_client
import enstore_files
import configuration_server
import www_server

"""

Support for marking servers, networks and robots as  either in a scheduled outage
period or up again.

"""

# translation between config keys and displayed server names, the opposite of the server_map
# SERVER_NAMES and server_map could both be in one dict, but i access them in different ways and
# it is easier to have both.  but cleaner to have one, yes i know.
SERVER_NAMES = {enstore_constants.LOGS : 'log_server',
		enstore_constants.ALARMS : 'alarm_server',
		enstore_constants.FILEC : 'file_clerk',
		enstore_constants.INQ : 'inquisitor',
		enstore_constants.VOLC : 'volume_clerk'}

server_map = {"log_server" : enstore_constants.LOGS,
	      "alarm_server" : enstore_constants.ALARMS,
	      "configuration" : enstore_constants.CONFIGS,
	      "file_clerk" : enstore_constants.FILEC,
	      "inquisitor" : enstore_constants.INQ,
	      "volume_clerk" : enstore_constants.VOLC,
	      "enstore" : enstore_constants.ENSTORE,
	      "network" : enstore_constants.NETWORK,
	      "media" : enstore_functions.get_from_config_file(www_server.WWW_SERVER,
							       www_server.MEDIA_TAG,
							       www_server.MEDIA_TAG_DEFAULT)}
server_keys = server_map.keys()

class ScheduleInterface(generic_client.GenericClientInterface):

    def __init__(self, flag=1, opts=[]):
	# fill in the defaults
	self.do_parse = flag
        self.restricted_opts = opts
	# we know this server to be up
	self.up = ""
	# we know this server to be down
	self.down = ""
	self.show = ""
	# we are scheduling this server for an outage in the future
	self.outage = ""
	self.time = enstore_constants.UNKNOWN
	self.nooutage = ""
	generic_client.GenericClientInterface.__init__(self)

    def options(self):
	return self.help_options() + ["up=", "down=", "show", "nooutage=", "outage=",
				      "time="]

def find_server_match(text):
    total_matches = 0
    matched_server = None
    pattern = "^%s"%(text,)
    for server in server_keys:
	match = re.match(pattern, server, re.I)
	if not match is None:
	    total_matches = total_matches + 1
	    matched_server = server
    return (total_matches, matched_server)

def print_error(key):
    print "ERROR: Unrecognized server - %s."%(key,)

def is_valid(server):
    name = os.environ.get("ENSTORE_CONFIG_FILE", "")
    if name:
        cdict = configuration_server.ConfigurationDict()
        cdict.read_config(name)
        server_d = cdict.configdict.get(server, None)
        if server_d:
	    # this is a valid server
            return 1

    # this server was not listed in the dictionary
    return 0


def do_work(intf):
    # read in the existing file that marks the scheduled outage status
    # of things
    html_dir = enstore_functions.get_html_dir()
    # check if the html_dir is accessible
    if not os.path.exists(html_dir):
	print "ERROR: No access to html directory - %s"%(html_dir,)
	return

    sfile = enstore_files.ScheduleFile(html_dir, enstore_constants.OUTAGEFILE)
    outage_d, offline_d, seen_down_d = sfile.read()

    # mark things as up if indicated
    if intf.up:
	up_l = string.split(intf.up, ',')
	for key in up_l:
	    # map the entered name to the name in the outage dictionary
	    num, server = find_server_match(key)
	    if num == 1:
		# we found a match
		out_key = server_map[server]
		if offline_d.has_key(out_key):
		    del offline_d[out_key]
	    else:
		# this is not a standard server, make sure this is a valid key 
		# before preceeding
		if is_valid(key):
		    if offline_d.has_key(key):
			del offline_d[key]
		else:
		    print_error(key)

    # mark some things as known down
    if intf.down:
	down_l = string.split(intf.down, ',')
	for key in down_l:
	    # map the entered name to the name in the outage dictionary
	    num, server = find_server_match(key)
	    if num == 1:
		# we found a match
		offline_d[server_map[server]] = enstore_constants.OFFLINE
	    else:
		# make sure this is a valid key before preceeding
		if is_valid(key):
		    offline_d[key] = enstore_constants.OFFLINE
		else:
		    print_error(key)

    # mark some items as scheduled to be taken down in the future
    if intf.outage:
	outage_l = string.split(intf.outage, ',')
	for key in outage_l:
	    # map the entered name to the name in the outage dictionary
	    num, server = find_server_match(key)
	    if num == 1:
		# we found a match
		outage_d[server_map[server]] = intf.time
	    else:
		# make sure this is a valid key before preceeding
		if is_valid(key):
		    outage_d[key] = intf.time
		else:
		    print_error(key)

    # mark these items as no longer being scheduled for an outage
    if intf.nooutage:
	outage_l = string.split(intf.nooutage, ',')
	for key in outage_l:
	    # map the entered name to the name in the outage dictionary
	    num, server = find_server_match(key)
	    if num == 1:
		# we found a match
		out_key = server_map[server]
		if outage_d.has_key(out_key):
		    del outage_d[out_key]
	    else:
		# make sure this is a valid key before preceeding
		if is_valid(key):
		    if outage_d.has_key(key):
			del outage_d[key]
		else:
		    print_error(key)


    if not sfile.write(outage_d, offline_d, seen_down_d):
	print "ERROR: Could not write to file %s/%s"%(html_dir, 
						      enstore_constants.OUTAGEFILE)

    if intf.show:
	print "\n Enstore Items Scheduled To Be Down"
	print   " ----------------------------------"
	keys = outage_d.keys()
	keys.sort()
	for key in keys:
	    print "   %s : %s"%(key, outage_d[key])
	else:
	    print ""
	# now output the servers that are known down but not scheduled down
	keys = offline_d.keys()
	if keys:
	    keys.sort()
	    print "\n Enstore Items Known Down"
	    print   " ------------------------"
	    for key in keys:
		print "   %s"%(key,)
	    else:
		print ""
	# output the servers that we have seen down and been monitoring
	keys = seen_down_d.keys()
	if keys:
	    keys.sort()
	    print "\n Enstore Items Down and the Number of Times Seen Down"
	    print   " ----------------------------------------------------"
	    for key in keys:
		print "   %s : %s"%(key, seen_down_d[key])
	    else:
		print ""
	


if __name__ == "__main__" :

    intf = ScheduleInterface()

    sys.exit(do_work(intf))
