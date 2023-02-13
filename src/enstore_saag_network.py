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
class SaagNetworkInterface(generic_client.GenericClientInterface):

    def __init__(self, args=sys.argv, user_mode=1):
	# fill in the defaults for the possible options
	#self.do_parse = flag
	#self.restricted_opts = opts
	self.html_gen_host = None
	generic_client.GenericClientInterface.__init__(self, args=args,
                                                       user_mode=user_mode)

    def valid_dictionaries(self):
        return (self.help_options, self.saag_options)

    saag_options = {
        option.HTML_GEN_HOST:{option.HELP_STRING:
                              "ip/hostname of the html server",
                              option.VALUE_TYPE:option.STRING,
                              option.VALUE_USAGE:option.REQUIRED,
                              option.VALUE_LABEL:"node_name",
                              option.USER_LEVEL:option.ADMIN,
                              },
        }
    
def do_work(intf):
    # we do not want anything printed out
    summary = 1
    
    Trace.init("ENHSAAG")

    # get the location of where the html file should go.  we do not want to
    # go thru an enstore server because we need to do this even when enstore
    # is down.  so get the config file ourselves.
    html_dir = enstore_functions.get_html_dir()

    # get the information on any scheduled down time. 
    sfile = enstore_files.ScheduleFile(html_dir, enstore_constants.OUTAGEFILE)
    outage_d, offline_d, override_d = sfile.read()
    override_d_keys = override_d.keys()

    # and the network if it is not marked as down
    if not offline_d.has_key(enstore_constants.NETWORK):
	netstat = monitor_client.do_real_work(summary, intf.config_host, intf.config_port,
					      intf.html_gen_host)
    else:
	netstat = {enstore_constants.NETWORK : enstore_constants.WARNING}

    # check if the network status should be overridden
    if enstore_constants.NETWORK in override_d_keys:
	netstat[enstore_constants.NETWORK] = enstore_functions2.override_to_status(\
	    override_d[enstore_constants.NETWORK])

    system_tag = enstore_functions.get_from_config_file(www_server.WWW_SERVER,
							www_server.SYSTEM_TAG,
							www_server.SYSTEM_TAG_DEFAULT)

    # create the saag network web page
    filename = "%s/%s"%(html_dir, enstore_constants.SAAGNETWORKHTMLFILE)
    saag_file = enstore_files.HtmlSaagNetworkFile(filename, system_tag)
    saag_file.open()
    saag_file.write(netstat, outage_d, offline_d)
    saag_file.close()
    saag_file.install()

if __name__ == "__main__":   # pragma: no cover

    intf = SaagNetworkInterface(user_mode=0)

    do_work(intf)
