# system imports
#
import time
import string
import sys

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

class AlarmClient(generic_client.GenericClient):

    def __init__(self, csc=0, verbose=0, host=interface.default_host(), \
                 port=interface.default_port()):
        # we always need to be talking to our configuration server
        Trace.trace(10,'{__init__')
	self.print_id = "ALRMC"
        configuration_client.set_csc(self, csc, host, port, verbose)
        self.u = udp_client.UDPClient()
	self.verbose = verbose
        ticket = self.csc.get("alarm_server")
	try:
            self.print_id = ticket['logname']
        except:
            pass
        Trace.trace(10,'}__init')

    def send (self, ticket, rcv_timeout=0, tries=0):
        Trace.trace(12,"{send"+repr(ticket))
        # who's our alarm server that we should send the ticket to?
        vticket = self.csc.get("alarm_server")
        # send user ticket and return answer back
        Trace.trace(12,"send addr="+repr((vticket['hostip'], vticket['port'])))
        s = self.u.send(ticket, (vticket['hostip'], vticket['port']), rcv_timeout, tries )
        Trace.trace(12,"}send"+repr(s))
        return s

    def status(self, str, server, verbose=0):
	Trace.trace(16,"{send_status "+repr(str))
	# format the ticket to send to the alarm server
	ticket = {"work" : "status", \
	          "status" : str , \
	          "server" : server }
	s = self.send(ticket)
	Trace.trace(16,"}send_status ")
	return s
	
class AlarmClientInterface(interface.Interface):

    def __init__(self):
        Trace.trace(10,'{alarmci.__init__')
        # fill in the defaults for the possible options
        self.alive = 0
        self.alive_rcv_timeout = 0
        self.alive_retries = 0
	self.verbose = 0
	self.got_server_verbose = 0
	self.dump = 0
	self.status = ""
        interface.Interface.__init__(self)

        # now parse the options
        self.parse_options()
        Trace.trace(10,'}alarmci.__init')

    #  define our specific help
    def parameters(self):
        return "server"

    # parse the options like normal but see if we have a server
    def parse_options(self):
        interface.Interface.parse_options(self)
        # see if we have a server
        if len(self.args) < 1 :
	    if self.status:
	        self.missing_parameter(self.parameters())
	        self.print_help()
	        sys.exit(1)
	    else:
	        self.server = ""
        else:
            self.server = self.args[0]

    # define the command line options that are valid
    def options(self):
        Trace.trace(16,"{}options")
        return self.config_options()+self.alive_options() +\
	       self.verbose_options() +\
	       ["dump", "status="] +\
               self.help_options()


if __name__ == "__main__" :
    Trace.init("ALARM client")
    Trace.trace(1,"alrmc called with args "+repr(sys.argv))

    # fill in interface
    intf = AlarmClientInterface()

    # now get an alarm client
    alc = AlarmClient(0, intf.verbose, intf.config_host, intf.config_port)

    if intf.alive:
        ticket = alc.alive(intf.alive_rcv_timeout,intf.alive_retries)
	msg_id = generic_cs.ALIVE

    elif intf.got_server_verbose:
        ticket = alc.set_verbose(intf.server_verbose, intf.alive_rcv_timeout,\
	                         intf.alive_retries)
	msg_id = generic_cs.CLIENT

    elif intf.dump:
        ticket = alc.dump(intf.alive_rcv_timeout, intf.alive_retries)
	msg_id = generic_cs.CLIENT

    elif not intf.status == "":
        ticket = alc.status(intf.status, intf.server, intf.verbose)
	msg_id = generic_cs.CLIENT

    else:
	intf.print_help()
        sys.exit(0)

    del alc.csc.u
    del alc.u           # del now, otherwise get name exception (just for python v1.5???)

    alc.check_ticket(ticket, msg_id)

