import time
import sys
import os
import stat

import Trace
import enstore_constants
import enstore_functions
import enstore_files
import generic_client
import enstore_up_down
import monitor_client
import enstore_files
import www_server
import alarm_server

"""

Support for creating the Enstore Status-At-A-Glance web page.
This main line must have read access to the configuration
file.

"""

class SaagInterface(generic_client.GenericClientInterface):

    def __init__(self, flag=1, opts=[]):
	# fill in the defaults for the possible options
	self.do_parse = flag
	self.restricted_opts = opts
	self.html_gen_host = None
	generic_client.GenericClientInterface.__init__(self)

    def options(self):
	return self.help_options() + ["html-gen-host="]
  
def do_work(intf):
    # we do not want anything printed out
    summary = 1
    
    Trace.init("ENSAAG")

    # get the location of where the html file should go.  we do not want to
    # go thru an enstore server because we need to do this even when enstore
    # is down.  so get the config file ourselves.
    html_dir = enstore_functions.get_html_dir()

    # get the information on any scheduled down time. 
    sfile = enstore_files.ScheduleFile(html_dir, enstore_constants.OUTAGEFILE)
    outage_d, offline_d, seen_down_d = sfile.read()

    # gather all of the latest status of the enstore system
    (rtn, enstat) = enstore_up_down.do_real_work()
    # and the network if it is not marked as down
    if not offline_d.has_key(enstore_constants.NETWORK):
	netstat = monitor_client.do_real_work(summary, intf.config_host, intf.config_port,
					      intf.html_gen_host)
    else:
	netstat = {enstore_constants.NETWORK : enstore_constants.WARNING}
    # no media status yet.
    medstat = {}

    # get the name and location of the alarm file so we can determine if there are any
    # alarms in it.  if yes, the alarm button goes red.
    log_file_dir = enstore_functions.get_from_config_file("log_server", "log_file_path", ".")
    alarm_file = "%s/%s"%(log_file_dir, alarm_server.DEFAULT_FILE_NAME)
    try:
	if os.stat(alarm_file)[stat.ST_SIZE] == 0L:
	    alarms = {enstore_constants.ANYALARMS : enstore_constants.UP}
	else:
	    alarms = {enstore_constants.ANYALARMS : enstore_constants.WARNING}
    except OSError:
	# there was no file, thus no alarms, everything is peachy keen
	alarms = {enstore_constants.ANYALARMS : enstore_constants.UP}
    # add the alarm html file so a link can be created to it on the main page.
    alarms[enstore_constants.URL] = alarm_server.DEFAULT_HTML_ALARM_FILE

    system_tag = enstore_functions.get_from_config_file(www_server.WWW_SERVER,
							www_server.SYSTEM_TAG,
							www_server.SYSTEM_TAG_DEFAULT)

    # get the host associated with each server
    config = enstore_functions.get_config_dict()
    nodes = {}
    # pull out the node information and create another dictionary with it. each element
    # of this second dictionary will be a list of servers on the node, which is the key.
    for server in enstat.keys():
	# translate the output server name to the config file key, remember, the config server
	# has no entry in the config file
	server = enstore_constants.SERVER_NAMES.get(server, server)
	if server != enstore_constants.CONFIGS:
	    if config.configdict.has_key(server):
		host = config.configdict[server].get('host', "")
		host = enstore_functions.strip_node(host)
	    else:
		host = ""
	else:
	    # we need to get the config node from the environment
	    host = os.environ.get("ENSTORE_CONFIG_HOST", "")
	    host = enstore_functions.strip_node(host)
	if host:
	    if nodes.has_key(host):
		nodes[host].append(server)
	    else:
		nodes[host] = [server,]

    # create the saag web page
    filename = "%s/%s"%(html_dir, enstore_constants.SAAGHTMLFILE)
    saag_file = enstore_files.HtmlSaagFile(filename, system_tag)
    saag_file.open()
    saag_file.write(enstat, netstat, medstat, alarms, nodes, outage_d, offline_d, 
		    enstore_files.status_html_file_name())
    saag_file.close()
    saag_file.install()

if __name__ == "__main__" :

    intf = SaagInterface()

    do_work(intf)
