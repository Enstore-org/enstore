# system imports
#
import sys
import os

# enstore imports
import configuration_client
import generic_client
import generic_cs
import udp_client
import interface
import Trace
import e_errors

class AlarmClient(generic_client.GenericClient):

    def __init__(self, csc=0, verbose=0, \
                 host=interface.default_host(), \
                 port=interface.default_port(), \
                 source="ALRMC"):
        # we always need to be talking to our configuration server
        Trace.trace(10,'{__init__')
        self.print_id = "ALRMC"
        self.pid = os.getpid()
        self.uid = os.getuid()
        self.source = source
        configuration_client.set_csc(self, csc, host, port, verbose)
        self.u = udp_client.UDPClient()
        self.verbose = verbose
        ticket = self.csc.get("alarm_server")
        try:
            self.print_id = ticket['logname']
        except KeyError:
            pass
        Trace.trace(10,'}__init')

    def send (self, ticket, rcv_timeout=0, tries=0):
        Trace.trace(12,"{send "+repr(ticket))
        # who's our alarm server that we should send the ticket to?
        vticket = self.csc.get("alarm_server")
        # send user ticket and return answer back
        Trace.trace(12,"send addr="+repr((vticket['hostip'], \
                                           vticket['port'])))
        s = self.u.send(ticket, (vticket['hostip'], vticket['port']), \
                        rcv_timeout, tries )
        Trace.trace(12,"}send "+repr(s))
        return s

    def alarm(self, severity=e_errors.DEFAULT_SEVERITY, \
              root_error=e_errors.DEFAULT_ROOT_ERROR,
              alarm_info={}, rcv_timeout=0, tries=0):
        Trace.trace(12,"{alarm ")
        # format the ticket to send to the alarm server
        ticket = {}
        ticket['work'] = "post_alarm"
        ticket['root_error'] = root_error
        ticket['severity'] = severity
        ticket['uid'] = self.uid
        ticket['pid'] = self.pid
        ticket['source'] = self.source
	ticket.update(alarm_info)
        s = self.send(ticket, rcv_timeout, tries )
	return s

    def ens_status(self, info, server="ALARMC", rcv_timeout=0, tries=0):
        # send the 'info' to the alarm server
        ticket = { "work" : "ens_status", "server" : server }
        ticket.update(info)
        s = self.send(ticket, rcv_timeout, tries )
	return s
        
    def get_patrol_file(self, rcv_timeout=0, tries=0):
        ticket = {'work' : 'get_patrol_filename'}
        s = self.send(ticket, rcv_timeout, tries)
        return s

class AlarmClientInterface(generic_client.GenericClientInterface,\
                           interface.Interface):

    def __init__(self):
        Trace.trace(10,'{alarmci.__init__')
        # fill in the defaults for the possible options
        self.alive_rcv_timeout = 0
        self.alive_retries = 0
        self.alarm = 0
        self.severity = e_errors.DEFAULT_SEVERITY
        self.root_error = e_errors.DEFAULT_ROOT_ERROR
        self.patrol_file = 0
        generic_client.GenericClientInterface.__init__(self)
        interface.Interface.__init__(self)

        Trace.trace(10,'}alarmci.__init__')

    # define the command line options that are valid
    def options(self):
        Trace.trace(16,"{}options")
        return self.client_options() +\
	       ["alarm", "severity=", "root_error=", "patrol_file"]


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

    elif intf.alarm:
        ticket = alc.alarm(intf.severity, intf.root_error)
	msg_id = generic_cs.CLIENT

    elif intf.patrol_file:
        ticket = alc.get_patrol_file()
        generic_cs.enprint(ticket['patrol_file'])
	msg_id = generic_cs.CLIENT
        
    else:
	intf.print_help()
        sys.exit(0)

    del alc.csc.u
    del alc.u           # del now, otherwise get name exception (just for python v1.5???)

    alc.check_ticket(ticket, msg_id)

