# system imports
#
import sys

# enstore imports
import generic_client
import udp_client
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
        if server and not server == "all":
	    t['server'] = server
	# tell the inquisitor to update the enstore system status info
	return self.send(t)

    def set_interval (self, tout, server):
	t = {"work"     : "set_interval" ,
	     "interval" : tout ,
             "server"   : server}
	# tell the inquisitor to set the interval between gathering stats
	return self.send(t)

    def reset_interval (self, server):
	t = {"work"     : "reset_interval",
             "server"   : server}
	# tell the inquisitor to reset the interval between gathering stats
	return self.send(t)

    def set_inq_timeout (self, tout):
	t = {"work"         : "set_inq_timeout" ,
             "inq_timeout"  : tout }
	# tell the inquisitor to set the select timeout
	return self.send(t)

    def get_inq_timeout (self):
	t = {"work"     : "get_inq_timeout" }
	# tell the inquisitor to get the select wake up timeout
	return self.send(t)

    def reset_inq_timeout (self):
	t = {"work"     : "reset_inq_timeout" }
	# tell the inquisitor to reset the timeout for the inq select
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

    def get_interval (self, server):
	t = {"work"    : "get_interval",
             "server"  : server }
	# tell the inquisitor to return the interval between gathering stats
	return self.send(t)

    def plot (self, logfile_dir="", start_time="", stop_time="", mcs=None,
              keep=0, pts_dir="", output_dir=""):
        if mcs is None:
            mcs = []
	# tell the inquisitor to plot bytes per unit of time
	t = {"work"        : "plot" }
	if logfile_dir:
	    t["logfile_dir"] = logfile_dir
	if start_time:
	    t["start_time"] = start_time
	if stop_time:
	    t["stop_time"] = stop_time
        if keep:
            t["keep"] = keep
        if pts_dir:
            t["keep_dir"] = pts_dir
        if output_dir:
            t["out_dir"] = output_dir
        if mcs:
            # the user  specified a device to plot the data for.
            t['mcs'] = mcs
	return self.send(t)


class InquisitorClientInterface(generic_client.GenericClientInterface):

    def __init__(self, flag=1, opts=[]):
        # fill in the defaults for the possible options
        self.do_parse = flag
        self.restricted_opts = opts
	self.update = ""
	self.interval = 0
	self.reset_interval = ""
	self.get_interval = ""
        self.alive_rcv_timeout = 0
        self.alive_retries = 0
	self.refresh = 0
	self.get_refresh = 0
	self.max_encp_lines = 0
	self.get_max_encp_lines = 0
	self.plot = 0
	self.logfile_dir = ""
	self.start_time = ""
	self.stop_time = ""
        self.media_changer = []
        self.keep = 0
        self.keep_dir = ""
        self.output_dir = ""
        self.inq_timeout = -1
        self.reset_inq_timeout = 0
        self.get_inq_timeout = 0
        generic_client.GenericClientInterface.__init__(self)

    #  define our specific help
    def parameters(self):
        return "server"

    # parse the options like normal but see if we have a server
    def parse_options(self):
        interface.Interface.parse_options(self)
        # see if we have a server
        if self.interval:
            if len(self.args) < 1 :
                self.missing_parameter(self.parameters())
                self.print_help()
                sys.exit(1)
            else:
                self.server = self.args[0]

    # define the command line options that are valid
    def options(self):
        if self.restricted_opts:
            return self.restricted_opts
        else:
            return self.client_options() +\
                   ["interval=", "get_interval=", "reset_interval=",
                    "inq_timeout=", "get_inq_timeout", "reset_inq_timeout",
                    "update=", "dump",
                    "refresh=", "get_refresh", "max_encp_lines=",
                    "get_max_encp_lines", "plot", "logfile_dir=",
                    "start_time=", "stop_time=", "media_changer=", "keep",
                    "keep_dir=", "output_dir="]

# this is where the work is actually done
def do_work(intf):
    # now get an inquisitor client
    iqc = Inquisitor((intf.config_host, intf.config_port))
    Trace.init(iqc.get_name(MY_NAME))

    if intf.alive:
        ticket = iqc.alive(MY_SERVER, intf.alive_rcv_timeout,
                           intf.alive_retries)

    elif intf.dump:
        ticket = iqc.dump(intf.alive_rcv_timeout, intf.alive_retries)

    elif intf.update:
        ticket = iqc.update(intf.update)

    elif intf.interval:
        ticket = iqc.set_interval(intf.interval, intf.server)

    elif not intf.inq_timeout == -1:
        ticket = iqc.set_inq_timeout(intf.inq_timeout)

    elif intf.get_inq_timeout:
        ticket = iqc.get_inq_timeout()
	print repr(ticket['inq_timeout'])

    elif intf.reset_inq_timeout:
        ticket = iqc.reset_inq_timeout()

    elif intf.get_interval:
        ticket = iqc.get_interval(intf.get_interval)
	print repr(ticket['interval'])

    elif intf.get_refresh:
        ticket = iqc.get_refresh()
	print repr(ticket['refresh'])

    elif intf.refresh:
        ticket = iqc.refresh(intf.refresh)

    elif intf.reset_interval:
        ticket = iqc.reset_interval(intf.reset_interval)

    elif intf.max_encp_lines:
        ticket = iqc.max_encp_lines(intf.max_encp_lines)

    elif intf.get_max_encp_lines:
        ticket = iqc.get_max_encp_lines()
	print repr(ticket['max_encp_lines'])

    elif intf.plot:
	ticket = iqc.plot(intf.logfile_dir, intf.start_time, intf.stop_time,
                          intf.media_changer, intf.keep, intf.keep_dir,
                          intf.output_dir)

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
