# system imports
#
import time
import string

# enstore imports
import configuration_client
import generic_client
import generic_cs
import backup_client
import udp_client
import callback
import interface
import Trace
import e_errors

class Inquisitor(generic_client.GenericClient):

    def __init__(self, csc=0, verbose=0, host=interface.default_host(), \
                 port=interface.default_port()):
        # we always need to be talking to our configuration server
        Trace.trace(10,'{__init__')
	self.print_id = "INQC"
        configuration_client.set_csc(self, csc, host, port, verbose)
        self.u = udp_client.UDPClient()
	self.verbose = verbose
        ticket = self.csc.get("inquisitor")
	try:
            self.print_id = ticket['logname']
        except:
            pass
        Trace.trace(10,'}__init')

    def send (self, ticket, rcv_timeout=0, tries=0):
        Trace.trace(12,"{send"+repr(ticket))
        # who's our inquisitor server that we should send the ticket to?
        vticket = self.csc.get("inquisitor")
        # send user ticket and return answer back
        Trace.trace(12,"send addr="+repr((vticket['hostip'], vticket['port'])))
        s = self.u.send(ticket, (vticket['hostip'], vticket['port']), rcv_timeout, tries )
        Trace.trace(12,"}send"+repr(s))
        return s

    def update (self, server="", verbose=0):
	Trace.trace(16,"{update")
	t = {"work"       : "update" }
	# see if we have a server or not
	if server != "":
	    t['server'] = server
	# tell the inquisitor to update the enstore system status info
	s = self.send(t)
        Trace.trace(16,"}update")
	return s

    def set_timeout (self, tout, server=""):
	Trace.trace(16,"{set_timeout")
	t = {"work"       : "set_timeout" ,\
	     "timeout"    : tout }
	# see if we have a server or not
	if server != "":
	    t['server'] = server
	# tell the inquisitor to reset the timeout between gathering stats
	s = self.send(t)
        Trace.trace(16,"}set_timeout")
	return s

    def reset_timeout (self, server=""):
	Trace.trace(16,"{reset_timeout")
	t = {"work"       : "reset_timeout" }
	# see if we have a server or not
	if server != "":
	    t['server'] = server
	# tell the inquisitor to reset the timeout between gathering stats
	s = self.send(t)
        Trace.trace(16,"}reset_timeout")
	return s

    def timestamp (self):
	Trace.trace(16,"{timestamp")
	# tell the inquisitor to timestamp the ascii file
	s = self.send({"work"       : "do_timestamp" })
        Trace.trace(16,"}timestamp")
	return s

    def max_ascii_size (self, value):
	Trace.trace(16,"{max_ascii_size")
	# tell the inquisitor to set the value for the timestamp for the ascii
	# file
	s = self.send({"work"       : "set_maxi_size" ,\
	               "max_ascii_size"  : value })
        Trace.trace(16,"}max_ascii_size")
	return s

    def get_max_ascii_size (self):
	Trace.trace(16,"{get_max_ascii_size")
	# tell the inquisitor to return the maximum allowed ascii file size
	s = self.send({"work"       : "get_maxi_size" } )
        Trace.trace(16,"}get_max_ascii_size")
	return s

    def max_encp_lines (self, value):
	Trace.trace(16,"{max_encp_lines")
	# tell the inquisitor to set the value for the max num of displayed
	# encp lines
	s = self.send({"work"       : "set_max_encp_lines" ,\
	               "max_encp_lines"  : value })
        Trace.trace(16,"}max_encp_lines")
	return s

    def get_max_encp_lines (self):
	Trace.trace(16,"{get_max_encp_lines")
	# tell the inquisitor to return the maximum displayed encp lines
	s = self.send({"work"       : "get_max_encp_lines" } )
        Trace.trace(16,"}get_max_encp_lines")
	return s

    def refresh (self, value):
	Trace.trace(16,"{refresh")
	# tell the inquisitor to set the value for the html file refresh
	s = self.send({"work"     : "set_refresh" ,\
	               "refresh"  : value })
        Trace.trace(16,"}refresh")
	return s

    def get_refresh (self):
	Trace.trace(16,"{get_refresh")
	# tell the inquisitor to return the current html file refresh value
	s = self.send({"work"       : "get_refresh" } )
        Trace.trace(16,"}get_refresh")
	return s

    def get_timeout (self, server=""):
	Trace.trace(16,"{get_timeout")
	t = {"work"       : "get_timeout" }
	# see if we have a server or not
	if server != "":
	    t['server'] = server
	# tell the inquisitor to return the timeout between gathering stats
	s = self.send(t)
        Trace.trace(16,"}get_timeout")
	return s

    def plot_bpt (self, logfile_dir="", start_time="", stop_time=""):
	Trace.trace(16,"{plot_bpt")
	# tell the inquisitor to plot bytes per unit of time
	t = {"work"        : "plot_bpt" }
	if not logfile_dir == "":
	    t["logfile_dir"] = logfile_dir
	if not start_time == "":
	    t["start_time"] = start_time
	if not stop_time == "":
	    t["stop_time"] = stop_time
	s = self.send(t)
        Trace.trace(16,"}plot_bpt")
	return s


class InquisitorClientInterface(interface.Interface):

    def __init__(self):
        Trace.trace(10,'{iqc.__init__')
        # fill in the defaults for the possible options
	self.update = 0
	self.timeout = 0
	self.reset_timeout = 0
	self.get_timeout = 0
        self.alive = 0
        self.alive_rcv_timeout = 0
        self.alive_retries = 0
	self.timestamp = 0
	self.max_ascii_size = 0
	self.get_max_ascii_size = 0
	self.verbose = 0
	self.got_server_verbose = 0
	self.dump = 0
	self.refresh = 0
	self.get_refresh = 0
	self.max_encp_lines = 0
	self.get_max_encp_lines = 0
	self.plot = 0
	self.logfile_dir = ""
	self.start_time = ""
	self.stop_time = ""
        interface.Interface.__init__(self)

        # now parse the options
        self.parse_options()
        Trace.trace(10,'}iqc.__init')

    #  define our specific help
    def parameters(self):
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
        Trace.trace(16,"{}options")
        return self.config_options()+self.alive_options() +\
	       self.verbose_options() +\
               ["timeout=", "get_timeout", "reset_timeout"] +\
	       ["update", "timestamp", "max_ascii_size="] +\
	       ["get_max_ascii_size", "dump"] +\
	       ["refresh=", "get_refresh", "max_encp_lines="] +\
	       ["get_max_encp_lines", "plot", "logfile_dir="] +\
	       ["start_time=", "stop_time="] +\
               self.help_options()


if __name__ == "__main__" :
    import sys
    Trace.init("IQ client")
    Trace.trace(1,"iqc called with args "+repr(sys.argv))

    # fill in interface
    intf = InquisitorClientInterface()

    # now get an inquisitor client
    iqc = Inquisitor(0, intf.verbose, intf.config_host, intf.config_port)

    if intf.alive:
        ticket = iqc.alive(intf.alive_rcv_timeout,intf.alive_retries)
	msg_id = generic_cs.ALIVE

    elif intf.got_server_verbose:
        ticket = iqc.set_verbose(intf.server_verbose, intf.alive_rcv_timeout,\
	                         intf.alive_retries)
	msg_id = generic_cs.CLIENT

    elif intf.dump:
        ticket = iqc.dump(intf.alive_rcv_timeout, intf.alive_retries)
	msg_id = generic_cs.CLIENT

    elif intf.update:
        ticket = iqc.update(intf.server, intf.verbose)
	msg_id = generic_cs.CLIENT

    elif intf.timeout:
        ticket = iqc.set_timeout(intf.timeout, intf.server)
	msg_id = generic_cs.CLIENT

    elif intf.get_timeout:
        ticket = iqc.get_timeout(intf.server)
	generic_cs.enprint(ticket['timeout'], generic_cs.PRETTY_PRINT)
	msg_id = generic_cs.CLIENT

    elif intf.get_refresh:
        ticket = iqc.get_refresh()
	generic_cs.enprint(ticket['refresh'], generic_cs.PRETTY_PRINT)
	msg_id = generic_cs.CLIENT

    elif intf.refresh:
        ticket = iqc.refresh(intf.refresh)
	msg_id = generic_cs.CLIENT

    elif intf.reset_timeout:
        ticket = iqc.reset_timeout(intf.server)
	msg_id = generic_cs.CLIENT

    elif intf.timestamp:
        ticket = iqc.timestamp()
	msg_id = generic_cs.CLIENT

    elif intf.max_ascii_size:
        ticket = iqc.max_ascii_size(intf.max_ascii_size)
	msg_id = generic_cs.CLIENT

    elif intf.get_max_ascii_size:
        ticket = iqc.get_max_ascii_size()
	generic_cs.enprint(ticket['max_ascii_size'], generic_cs.PRETTY_PRINT)
	msg_id = generic_cs.CLIENT

    elif intf.max_encp_lines:
        ticket = iqc.max_encp_lines(intf.max_encp_lines)
	msg_id = generic_cs.CLIENT

    elif intf.get_max_encp_lines:
        ticket = iqc.get_max_encp_lines()
	generic_cs.enprint(ticket['max_encp_lines'], generic_cs.PRETTY_PRINT)
	msg_id = generic_cs.CLIENT

    elif intf.plot:
	ticket = iqc.plot_bpt(intf.logfile_dir, intf.start_time, intf.stop_time)
	msg_id = generic_cs.CLIENT

    else:
	intf.print_help()
        sys.exit(0)

    del iqc.csc.u
    del iqc.u           # del now, otherwise get name exception (just for python v1.5???)

    iqc.check_ticket(ticket, msg_id)

