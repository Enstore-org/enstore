import time
import sys
import os
import stat

import Trace
import enstore_constants
import enstore_functions
import enstore_functions2
import enstore_files
import generic_client
import enstore_up_down
import monitor_client
import www_server
import alarm_server
import option

"""

Support for creating the Enstore Status-At-A-Glance web page.
This main line must have read access to the configuration
file.

"""
class SaagInterface(generic_client.GenericClientInterface):

    def __init__(self, args=sys.argv, user_mode=1):
	# fill in the defaults for the possible options
	#self.do_parse = flag
	#self.restricted_opts = opts
	self.html_gen_host = None
	generic_client.GenericClientInterface.__init__(self, args=args,
                                                       user_mode=user_mode)

    def valid_dictionaries(self):
        return (self.help_options,)
    
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
    outage_d, offline_d, override_d = sfile.read()
    override_d_keys = override_d.keys()

    # gather all of the latest status of the enstore system
    (rtn, enstat) = enstore_up_down.do_real_work()

    # no media status yet.
    medstat = {}

    config = enstore_functions.get_config_dict()

    # set the dcache status balls.  either they are overridden in the sched
    # file or they are green
    other_saag_links = config.configdict.get('other_saag_links', {})
    if other_saag_links.has_key('status'):
        del other_saag_links['status']
    dlinks = other_saag_links.keys()
    dlinks.sort()
    for dlink in dlinks:
        if dlink in override_d_keys:
            other_saag_links[dlink].append(enstore_functions2.override_to_status(override_d[dlink]))
        else:
            other_saag_links[dlink].append(enstore_constants.UP)

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
    # add the alarm html file so a link can be created to it on main page.
    alarms[enstore_constants.URL] = alarm_server.DEFAULT_HTML_ALARM_FILE

    # check if the alarm status should be overridden
    if enstore_constants.ANYALARMS in override_d_keys:
	alarms[enstore_constants.ANYALARMS] = enstore_functions2.override_to_status(\
	    override_d[enstore_constants.ANYALARMS])

    system_tag = enstore_functions.get_from_config_file(www_server.WWW_SERVER,
							www_server.SYSTEM_TAG,
							www_server.SYSTEM_TAG_DEFAULT)

    # get the host associated with each server
    nodes = {}
    # pull out the node information and create another dictionary with it. each element
    # of this second dictionary will be a list of servers on the node, which is the key.
    for server in enstat.keys():
	# translate the output server name to the config file key, remember, the config server
	# has no entry in the config file
	server = enstore_constants.SERVER_NAMES.get(server, server)
	if server != enstore_constants.CONFIG_SERVER:
	    if config.configdict.has_key(server):
		host = config.configdict[server].get('host', "")
		host = enstore_functions2.strip_node(host)
	    else:
		host = ""
	else:
	    # we need to get the config node from the environment
	    host = os.environ.get("ENSTORE_CONFIG_HOST", "")
	    host = enstore_functions2.strip_node(host)
	if host:
	    if nodes.has_key(host):
		nodes[host].append(server)
	    else:
		nodes[host] = [server,]

    # create the saag web page
    filename = "%s/%s"%(html_dir, enstore_constants.SAAGHTMLFILE)
    saag_file = enstore_files.HtmlSaagFile(filename, system_tag)
    saag_file.open()
    saag_file.write(enstat, other_saag_links, medstat, alarms, nodes, outage_d, offline_d, 
		    enstore_files.status_html_file_name())
    saag_file.close()
    saag_file.install()

    # create the file that records just the status of enstore 
    filename = "%s/%s"%(html_dir, enstore_constants.ENSTORESTATUSFILE)
    es_file = enstore_files.EnstoreStatusFile(filename)
    es_file.open()
    es_file.write(enstat, outage_d, offline_d, override_d)
    es_file.close()
    es_file.install()

if __name__ == "__main__":   # pragma: no cover

    intf = SaagInterface(user_mode=0)

    do_work(intf)
