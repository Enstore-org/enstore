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
	# wait a really long time as plotting takes awhile.
	return self.send(t, 3000)


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
	self.plot = 0
	self.logfile_dir = ""
	self.start_time = ""
	self.stop_time = ""
        self.media_changer = []
        self.keep = 0
        self.keep_dir = ""
        self.output_dir = ""
        self.update_interval = -1
        self.get_update_interval = 0
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
                "get-max-encp-lines", "plot", "logfile-dir=",
                "start-time=", "stop-time=", "media-changer=", "keep",
                "keep-dir=", "output-dir="]

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
