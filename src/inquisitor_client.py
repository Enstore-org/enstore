# system imports
#
import sys

# enstore imports
import generic_client
import udp_client
import enstore_constants
import enstore_functions
import interface
import Trace

MY_NAME = "INQ_CLIENT"
MY_SERVER = "inquisitor"

class Inquisitor(generic_client.GenericClient):

    def __init__(self, csc):
        generic_client.GenericClient.__init__(self, csc, MY_NAME)
        # we always need to be talking to our configuration server
        self.u = udp_client.UDPClient()
	self.server_name = MY_SERVER
        self.server_address = self.get_server_address(self.server_name)

    def update (self, server=""):
	t = {"work"       : "update" }
	# tell the inquisitor to update the enstore system status info
	return self.send(t)

    def update_and_exit (self):
	t = {"work"     : "update_and_exit"}
	# tell the inquisitor to get out of town
	return self.send(t)

    def set_update_interval (self, tout):
	t = {"work"         : "set_update_interval" ,
             "update_interval"  : tout }
	# tell the inquisitor to set the select timeout
	return self.send(t)

    def get_update_interval (self):
	t = {"work"     : "get_update_interval" }
	# tell the inquisitor to get the select wake up timeout
	return self.send(t)

    def max_encp_lines (self, value):
	# tell the inquisitor to set the value for the max num of displayed
	# encp lines
	return self.send({"work"       : "set_max_encp_lines" ,
                          "max_encp_lines"  : value })

    def get_max_encp_lines (self):
	# tell the inquisitor to return the maximum displayed encp lines
	return self.send({"work"       : "get_max_encp_lines" } )

    def refresh (self, value):
	# tell the inquisitor to set the value for the html file refresh
	return self.send({"work"     : "set_refresh" ,
                          "refresh"  : value })

    def get_refresh (self):
	# tell the inquisitor to return the current html file refresh value
	return self.send({"work"       : "get_refresh" } )

    def subscribe (self):
	# tell the inquisitor to subscribe to the event relay
	return self.send({"work"     : "subscribe" })

    def down (self, server_list, time, rcv_timeout=0, tries=0):
	# tell the inquisitor to mark the passed servers as down
	return self.send({"work"    : "down",
			  "servers" : server_list,
			  "time"    : time}, rcv_timeout, tries)

    def up (self, server_list, rcv_timeout=0, tries=0):
	# tell the inquisitor to mark the passed servers as up
	return self.send({"work"    : "up",
			  "servers" : server_list }, rcv_timeout, tries)

    def nooutage (self, server_list, rcv_timeout=0, tries=0):
	# tell the inquisitor to mark the passed servers as no longer scheduled 
	# for an outage
	return self.send({"work"    : "nooutage",
			  "servers" : server_list }, rcv_timeout, tries)

    def outage (self, server_list, time, rcv_timeout=0, tries=0):
	# tell the inquisitor to mark the passed servers as scheduled for an outage
	return self.send({"work"    : "outage",
			  "servers" : server_list,
			  "time"    : time}, rcv_timeout, tries)

    def show (self, rcv_timeout=0, tries=0):
	# tell the inquisitor to return the outage/status of the servers in the 
	# schedule file
	return self.send({"work"    : "show" }, rcv_timeout, tries)

    def print_show(self, ticket):
	print "\n Enstore Items Scheduled To Be Down"
	print   " ----------------------------------"
	outage_d = ticket["outage"]
	keys = outage_d.keys()
	keys.sort()
	for key in keys:
	    print "   %s : %s"%(key, outage_d[key])
	else:
	    print ""
	# now output the servers that are known down but not scheduled down
	offline_d = ticket["offline"]
	if offline_d:
	    keys = offline_d.keys()
	    keys.sort()
	    print "\n Enstore Items Known Down"
	    print   " ------------------------"
	    for key in keys:
		print "   %s : %s"%(key, offline_d[key])
	    else:
		print ""
	# output the servers that we have seen down and been monitoring
	seen_down_d = ticket["seen_down"]
	if seen_down_d:
	    keys = seen_down_d.keys()
	    keys.sort()
	    print "\n Enstore Items Down and the Number of Times Seen Down"
	    print   " ----------------------------------------------------"
	    for key in keys:
		print "   %s : %s"%(key, seen_down_d[key])
	    else:
		print ""


class InquisitorClientInterface(generic_client.GenericClientInterface):

    def __init__(self, flag=1, opts=[]):
        # fill in the defaults for the possible options
        self.do_parse = flag
        self.restricted_opts = opts
	self.update = ""
        self.alive_rcv_timeout = 0
        self.alive_retries = 0
	self.refresh = 0
	self.get_refresh = 0
	self.max_encp_lines = 0
	self.get_max_encp_lines = 0
	self.logfile_dir = ""
	self.start_time = ""
	self.stop_time = ""
        self.media_changer = []
        self.keep = 0
        self.keep_dir = ""
        self.output_dir = ""
        self.update_interval = -1
        self.get_update_interval = 0
	self.subscribe = None
	self.show = 0
	self.up = ""
	self.down = ""
	self.time = ""
	self.outage = ""
	self.nooutage = ""
	self.update_and_exit = 0
        generic_client.GenericClientInterface.__init__(self)
        
    # define the command line options that are valid
    def options(self):
        if self.restricted_opts:
            return self.restricted_opts
        else:
            return self.client_options() +[
                "update-interval=", "get-update-interval",
                "update", "dump", "update-and-exit",
                "refresh=", "get-refresh", "max-encp-lines=",
                "get-max-encp-lines", "subscribe", "up=", "down=",
		"outage=", "nooutage=", "show", "time="]

# this is where the work is actually done
def do_work(intf):
    # now get an inquisitor client
    iqc = Inquisitor((intf.config_host, intf.config_port))
    Trace.init(iqc.get_name(MY_NAME))

    ticket = iqc.handle_generic_commands(MY_SERVER, intf)
    if ticket:
        pass

    elif intf.dump:
        ticket = iqc.dump(intf.alive_rcv_timeout, intf.alive_retries)

    elif intf.update:
        ticket = iqc.update(intf.update)

    elif intf.update_and_exit:
        ticket = iqc.update_and_exit()

    elif not intf.update_interval == -1:
        ticket = iqc.set_update_interval(intf.update_interval)

    elif intf.get_update_interval:
        ticket = iqc.get_update_interval()
	print repr(ticket['update_interval'])

    elif intf.get_refresh:
        ticket = iqc.get_refresh()
	print repr(ticket['refresh'])

    elif intf.refresh:
        ticket = iqc.refresh(intf.refresh)

    elif intf.max_encp_lines:
        ticket = iqc.max_encp_lines(intf.max_encp_lines)

    elif intf.get_max_encp_lines:
        ticket = iqc.get_max_encp_lines()
	print repr(ticket['max_encp_lines'])

    elif intf.subscribe:
	ticket = iqc.subscribe()

    elif intf.up:
	ticket = iqc.up(intf.up)

    elif intf.down:
	ticket = iqc.down(intf.down, intf.time)

    elif intf.outage:
	ticket = iqc.outage(intf.outage, intf.time)

    elif intf.nooutage:
	ticket = iqc.nooutage(intf.nooutage)

    elif intf.show:
	ticket = iqc.show()
	if enstore_functions.is_ok(ticket):
	    iqc.print_show(ticket)

    else:
	intf.print_help()
        sys.exit(0)

    del iqc.csc.u
    del iqc.u     # del now, otherwise get name exception (just for python v1.5???)

    iqc.check_ticket(ticket)


if __name__ == "__main__" :
    Trace.init(MY_NAME)
    Trace.trace(6,"iqc called with args "+repr(sys.argv))

    # fill in interface
    intf = InquisitorClientInterface()

    do_work(intf)
