# system imports
#
import sys
import os
import pwd

# enstore imports
import alarm_server
import generic_client
import udp_client
import interface
import Trace
import e_errors

MY_NAME = "ALARM_CLIENT"

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

    def __init__(self, csc):
        # we always need to be talking to our configuration server
        self.csc = csc
        # need the following definition so the generic client init does not
        # get another alarm client
        self.is_alarm = 1
        generic_client.GenericClient.__init__(self, csc, MY_NAME)
        self.uid = pwd.getpwuid(os.getuid())[0]
        self.u = udp_client.UDPClient()
        ticket = self.csc.get("alarm_server")
        self.server_address = (ticket['hostip'], ticket['port'])
	Trace.set_alarm_func( self.alarm_func )
	self.alarm_func_lock = Lock() 

    def alarm_func(self, time, pid, name, args):
	# prevent infinite recursion (i.e if some function call by this
	# function does a trace and the alarm bit is set
	if self.alarm_func_lock.test_and_set(): return None
        if 0: print time  # lint fix
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
        self.u.send(ticket, self.server_address, 0, 0 )
	return self.alarm_func_lock.unlock()

    def send(self, ticket, rcv_timeout=0, tries=0):
        # need this for the alive function
        return self.u.send(ticket, self.server_address, rcv_timeout, tries)
        
    def alarm(self, severity=e_errors.DEFAULT_SEVERITY, \
              root_error=e_errors.DEFAULT_ROOT_ERROR,
              alarm_info={}):
        if 0: print self    # lint fix
        Trace.alarm(severity, root_error, alarm_info )

    def resolve(self, id, rcv_timeout=0, tries=0):
        # this alarm has been resolved.  we need to tell the alarm server
        ticket = {'work' : "resolve_alarm",
                  alarm_server.ALARM   : id}
        return self.u.send(ticket, self.server_address, rcv_timeout, tries)

    def get_patrol_file(self, rcv_timeout=0, tries=0):
        ticket = {'work' : 'get_patrol_filename'}
        return self.u.send(ticket, self.server_address, rcv_timeout, tries)

class AlarmClientInterface(generic_client.GenericClientInterface,\
                           interface.Interface):

    def __init__(self):
        # fill in the defaults for the possible options
        self.alive_rcv_timeout = 0
        self.alive_retries = 0
        self.alarm = 0
        self.resolve = 0
        self.severity = e_errors.DEFAULT_SEVERITY
        self.root_error = e_errors.DEFAULT_ROOT_ERROR
        self.patrol_file = 0
        generic_client.GenericClientInterface.__init__(self)
        interface.Interface.__init__(self)

    # define the command line options that are valid
    def options(self):
        return self.client_options() +\
	       ["alarm", "severity=", "root_error=", "patrol_file", \
                "resolve="]

if __name__ == "__main__" :
    Trace.init(MY_NAME)
    Trace.trace(6,"alrmc called with args "+repr(sys.argv))

    # fill in interface
    intf = AlarmClientInterface()

    # now get an alarm client
    alc = AlarmClient((intf.config_host, intf.config_port))
    Trace.init(alc.get_name(MY_NAME))

    if intf.alive:
        ticket = alc.alive(intf.alive_rcv_timeout, intf.alive_retries)
        #print repr(ticket)

    elif intf.resolve:
        ticket = alc.resolve(intf.resolve)

    elif intf.dump:
        ticket = alc.dump()

    elif intf.alarm:
        alc.alarm(intf.severity, intf.root_error)
        ticket = {}

    elif intf.patrol_file:
        ticket = alc.get_patrol_file()
        print(ticket['patrol_file'])
        
    else:
	intf.print_help()
        sys.exit(0)

    del alc.csc.u
    del alc.u           # del now, otherwise get name exception (just for python v1.5???)

    alc.check_ticket(ticket)

