# system imports
#

# enstore imports
import generic_client
import udp_client
import interface
import Trace

MY_NAME = "INQUISITOR"

class Inquisitor(generic_client.GenericClient):

    def __init__(self, csc):
        generic_client.GenericClient.__init__(self, csc, MY_NAME)
        # we always need to be talking to our configuration server
        self.u = udp_client.UDPClient()
	self.server_name = "inquisitor"

    def send (self, ticket, rcv_timeout=0, tries=0):
        # who's our inquisitor server that we should send the ticket to?
        vticket = self.csc.get(self.server_name)
        # send user ticket and return answer back
        Trace.trace(12,"send addr="+repr((vticket['hostip'], vticket['port'])))
        return self.u.send(ticket, (vticket['hostip'], vticket['port']),
                           rcv_timeout, tries )

    def update (self, server=""):
	t = {"work"       : "update" }
	# see if we have a server or not
	if server:
	    t['server'] = server
	# tell the inquisitor to update the enstore system status info
	return self.send(t)

    def set_timeout (self, tout, server=""):
	t = {"work"       : "set_timeout" ,\
	     "timeout"    : tout }
	# see if we have a server or not
	if server:
	    t['server'] = server
	# tell the inquisitor to reset the timeout between gathering stats
	return self.send(t)

    def reset_timeout (self, server=""):
	t = {"work"       : "reset_timeout" }
	# see if we have a server or not
	if server:
	    t['server'] = server
	# tell the inquisitor to reset the timeout between gathering stats
	return self.send(t)

    def timestamp (self):
	# tell the inquisitor to timestamp the ascii file
	return self.send({"work"       : "do_timestamp" })

    def max_ascii_size (self, value):
	# tell the inquisitor to set the value for the timestamp for the ascii
	# file
	return self.send({"work"       : "set_maxi_size" ,\
                          "max_ascii_size"  : value })

    def get_max_ascii_size (self):
	# tell the inquisitor to return the maximum allowed ascii file size
	return self.send({"work"       : "get_maxi_size" } )

    def max_encp_lines (self, value):
	# tell the inquisitor to set the value for the max num of displayed
	# encp lines
	return self.send({"work"       : "set_max_encp_lines" ,\
                          "max_encp_lines"  : value })

    def get_max_encp_lines (self):
	# tell the inquisitor to return the maximum displayed encp lines
	return self.send({"work"       : "get_max_encp_lines" } )

    def refresh (self, value):
	# tell the inquisitor to set the value for the html file refresh
	return self.send({"work"     : "set_refresh" ,\
                          "refresh"  : value })

    def get_refresh (self):
	# tell the inquisitor to return the current html file refresh value
	return self.send({"work"       : "get_refresh" } )

    def get_timeout (self, server=""):
	t = {"work"       : "get_timeout" }
	# see if we have a server or not
	if server:
	    t['server'] = server
	# tell the inquisitor to return the timeout between gathering stats
	return self.send(t)

    def plot (self, logfile_dir="", start_time="", stop_time=""):
	# tell the inquisitor to plot bytes per unit of time
	t = {"work"        : "plot" }
	if logfile_dir:
	    t["logfile_dir"] = logfile_dir
	if start_time:
	    t["start_time"] = start_time
	if stop_time:
	    t["stop_time"] = stop_time
	return self.send(t)


class InquisitorClientInterface(generic_client.GenericClientInterface):

    def __init__(self):
        # fill in the defaults for the possible options
	self.update = 0
	self.timeout = 0
	self.reset_timeout = 0
	self.get_timeout = 0
        self.alive_rcv_timeout = 0
        self.alive_retries = 0
	self.timestamp = 0
	self.max_ascii_size = 0
	self.get_max_ascii_size = 0
	self.refresh = 0
	self.get_refresh = 0
	self.max_encp_lines = 0
	self.get_max_encp_lines = 0
	self.plot = 0
	self.logfile_dir = ""
	self.start_time = ""
	self.stop_time = ""
        generic_client.GenericClientInterface.__init__(self)

    #  define our specific help
    def parameters(self):
        if 0: print self #quiet lint
        return "server"

    # parse the options like normal but see if we have a server
    def parse_options(self):
        interface.Interface.parse_options(self)
        # see if we have a server
        if len(self.args) < 1 :
	    self.server = ""
        else:
            self.server = self.args[0]

    # define the command line options that are valid
    def options(self):
        return self.client_options() +\
               ["timeout=", "get_timeout", "reset_timeout"] +\
	       ["update", "timestamp", "max_ascii_size="] +\
	       ["get_max_ascii_size", "dump"] +\
	       ["refresh=", "get_refresh", "max_encp_lines="] +\
	       ["get_max_encp_lines", "plot", "logfile_dir="] +\
	       ["start_time=", "stop_time="]


if __name__ == "__main__" :
    import sys
    Trace.init(MY_NAME)
    Trace.trace(6,"iqc called with args "+repr(sys.argv))

    # fill in interface
    intf = InquisitorClientInterface()

    # now get an inquisitor client
    iqc = Inquisitor((intf.config_host, intf.config_port))
    Trace.init(iqc.get_name(MY_NAME))

    if intf.alive:
        ticket = iqc.alive(intf.alive_rcv_timeout,intf.alive_retries)

    elif intf.dump:
        ticket = iqc.dump(intf.alive_rcv_timeout, intf.alive_retries)

    elif intf.update:
        ticket = iqc.update(intf.server)

    elif intf.timeout:
        ticket = iqc.set_timeout(intf.timeout, intf.server)

    elif intf.get_timeout:
        ticket = iqc.get_timeout(intf.server)
	print repr(ticket['timeout'])

    elif intf.get_refresh:
        ticket = iqc.get_refresh()
	print repr(ticket['refresh'])

    elif intf.refresh:
        ticket = iqc.refresh(intf.refresh)

    elif intf.reset_timeout:
        ticket = iqc.reset_timeout(intf.server)

    elif intf.timestamp:
        ticket = iqc.timestamp()

    elif intf.max_ascii_size:
        ticket = iqc.max_ascii_size(intf.max_ascii_size)

    elif intf.get_max_ascii_size:
        ticket = iqc.get_max_ascii_size()
	print repr(ticket['max_ascii_size'])

    elif intf.max_encp_lines:
        ticket = iqc.max_encp_lines(intf.max_encp_lines)

    elif intf.get_max_encp_lines:
        ticket = iqc.get_max_encp_lines()
	print repr(ticket['max_encp_lines'])

    elif intf.plot:
	ticket = iqc.plot(intf.logfile_dir, intf.start_time, intf.stop_time)

    else:
	intf.print_help()
        sys.exit(0)

    del iqc.csc.u
    del iqc.u           # del now, otherwise get name exception (just for python v1.5???)

    iqc.check_ticket(ticket)

