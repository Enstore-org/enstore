# system imports
#
import sys
import os
import pwd
import errno

# enstore imports
import alarm_server
import generic_client
import udp_client
import interface
import Trace
import e_errors

MY_NAME = "ALARM_CLIENT"
MY_SERVER = "alarm_server"

RCV_TIMEOUT = 3
RCV_TRIES = 2

class Lock:
    def __init__(self):
	self.locked = 0
    def unlock(self):
	self.locked = 0
	return None
    def test_and_set(self):
	s = self.locked
	self.locked=1
	return s

class AlarmClient(generic_client.GenericClient):

    def __init__(self, csc, rcv_timeout=RCV_TIMEOUT, rcv_tries=RCV_TRIES):
        # we always need to be talking to our configuration server
        self.csc = csc
        # need the following definition so the generic client init does not
        # get another alarm client
        self.is_alarm = 1
        generic_client.GenericClient.__init__(self, csc, MY_NAME)
        try:
            self.uid = pwd.getpwuid(os.getuid())[0]
        except:
            self.uid = "unknown"
        self.u = udp_client.UDPClient()
        ticket = self.csc.get(MY_SERVER)
        self.server_address = (ticket['hostip'], ticket['port'])
        self.rcv_timeout = rcv_timeout
        self.rcv_tries = rcv_tries
	Trace.set_alarm_func( self.alarm_func )
	self.alarm_func_lock = Lock() 

    def alarm_func(self, time, pid, name, args):
	# prevent infinite recursion (i.e if some function call by this
	# function does a trace and the alarm bit is set
	if self.alarm_func_lock.test_and_set(): return None
        ticket = {}
        ticket['work'] = "post_alarm"
        ticket[alarm_server.UID] = self.uid
        ticket[alarm_server.PID] = pid
        ticket[alarm_server.SOURCE] = name
        if args[0] == e_errors.ALARM:
            # we were called from Trace.alarm and args will be a dict
            ticket.update(args[2])
        else:
            # we were called from someplace like Trace.trace and we only
            # have a text string for an argument
            ticket['text'] = args[1]
        self.send(ticket, self.rcv_timeout, self.rcv_tries )
	return self.alarm_func_lock.unlock()

    def send(self, ticket, rcv_timeout, tries):
        try:
            x = self.u.send(ticket, self.server_address, rcv_timeout, tries)
        except errno.errorcode[errno.ETIMEDOUT]:
            x = {'status' : (e_errors.TIMEDOUT, None)}
        return x
        
    def alarm(self, severity=e_errors.DEFAULT_SEVERITY, \
              root_error=e_errors.DEFAULT_ROOT_ERROR,
              alarm_info=None):
        if alarm_info is None:
            alarm_info = {}
        Trace.alarm(severity, root_error, alarm_info )

    def resolve(self, id):
        # this alarm has been resolved.  we need to tell the alarm server
        ticket = {'work' : "resolve_alarm",
                  alarm_server.ALARM   : id}
        return self.send(ticket, self.rcv_timeout, self.rcv_tries)

    def get_patrol_file(self):
        ticket = {'work' : 'get_patrol_filename'}
        return self.send(ticket, self.rcv_timeout, self.rcv_tries)

class AlarmClientInterface(generic_client.GenericClientInterface,\
                           interface.Interface):

    def __init__(self, flag=1, opts=[]):
        self.do_parse = flag
        self.restricted_opts = opts
        # fill in the defaults for the possible options
        # we always want a default timeout and retries so that the alarm
        # client/server communications does not become a weak link
        self.alive_rcv_timeout = RCV_TIMEOUT
        self.alive_retries = RCV_TRIES
        self.alarm = 0
        self.resolve = 0
        self.dump = 0
        self.severity = e_errors.DEFAULT_SEVERITY
        self.root_error = e_errors.DEFAULT_ROOT_ERROR
        self.get_patrol_file = 0
        generic_client.GenericClientInterface.__init__(self)
        interface.Interface.__init__(self)

    # define the command line options that are valid
    def options(self):
        if self.restricted_opts:
            return self.restricted_opts
        else:
            return self.client_options() +\
                   ["raise", "severity=", "root_error=", "get_patrol_file",
                    "resolve=", "dump"]

def do_work(intf):
    # now get an alarm client
    alc = AlarmClient((intf.config_host, intf.config_port),
                      intf.alive_rcv_timeout, intf.alive_retries)
    Trace.init(alc.get_name(MY_NAME))

    if intf.alive:
        ticket = alc.alive(MY_SERVER, intf.alive_rcv_timeout,
                           intf.alive_retries)

    elif intf.resolve:
        ticket = alc.resolve(intf.resolve)

    elif intf.dump:
        ticket = alc.dump()

    elif intf.alarm:
        alc.alarm(intf.severity, intf.root_error)
        ticket = {}

    elif intf.get_patrol_file:
        ticket = alc.get_patrol_file()
        print(ticket['patrol_file'])
        
    else:
	intf.print_help()
        sys.exit(0)

    del alc.csc.u
    del alc.u           # del now, otherwise get name exception (just for python v1.5???)

    alc.check_ticket(ticket)

if __name__ == "__main__" :
    Trace.init(MY_NAME)
    Trace.trace(6,"alrmc called with args "+repr(sys.argv))

    # fill in interface
    intf = AlarmClientInterface()

    do_work(intf)
