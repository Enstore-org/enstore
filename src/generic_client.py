###############################################################################
# src/$RCSfile$   $Revision$
#
#system imports
import sys
import errno
import pprint
import types
import os
import string

#enstore imports
import setpath
import Trace
import e_errors
import interface

class GenericClientInterface(interface.Interface):

    def __init__(self):
	self.dump = 0
	self.alive = 0
        self.do_print = []
        self.dont_print = []
        self.do_log = []
        self.dont_log = []
        self.do_alarm = []
        self.dont_alarm = []
	interface.Interface.__init__(self)

    def client_options(self):
	return (self.config_options() + 
	       self.alive_options()  + 
               self.trace_options() + 
               self.help_options() )
    
init_done = 0

class GenericClient:

    def __init__(self, csc, name):
        global init_done

        import configuration_client

        if init_done:
            self.csc = csc
            return
        
        init_done = 1
            
        if csc:
            if type(csc) == type(()):
                self.csc = configuration_client.ConfigurationClient(csc)
            else:
                # it is not a tuple of address and port, so we assume that it
                # is a configuration client object
                self.csc = csc
        else:
            # it is None or 0 (the default value from i.e. log_client)
            def_addr = (os.environ['ENSTORE_CONFIG_HOST'],
                        string.atoi(os.environ['ENSTORE_CONFIG_PORT']))
            self.csc = configuration_client.ConfigurationClient( def_addr )

        # try to find the logname for this object in the config dict.  use
        # the lowercase version of the name as the server key.  if this
        # object is not defined in the config dict, then just use the
        # passed in name.
        self.log_name = self.get_name(name)
        if not self.__dict__.get('is_logger', 0):
            import log_client
            self.logc = log_client.LoggerClient(self.csc, self.log_name,
                                                'log_server')
        if not self.__dict__.get('is_alarm', 0):
            import alarm_client
            self.alarmc = alarm_client.AlarmClient(self.csc)

    def get_server_address(self, MY_SERVER):
        ticket = self.csc.get(MY_SERVER)
        try:
            server_address = (ticket['hostip'], ticket['port'])
            return server_address
        except KeyError, detail:
            sys.stderr.write("Unknown server %s (no %s defined in config on %s)\n" %
                             ( MY_SERVER, detail, 
			       os.environ.get('ENSTORE_CONFIG_HOST','')))
            os._exit(1)

    def send(self, ticket, rcv_timeout=0, tries=0):
        try:
            x = self.u.send(ticket, self.server_address, rcv_timeout, tries)
        except errno.errorcode[errno.ETIMEDOUT]:
            x = {'status' : (e_errors.TIMEDOUT, None)}
        return x
        
    # return the name used for this client/server #XXX what is this nonsense? cgw
    def get_name(self, name):
        return name

    # check on alive status
    def alive(self, server, rcv_timeout=0, tries=0):
	t = self.csc.get(server, rcv_timeout, tries)
	if t['status'] == (e_errors.TIMEDOUT, None):
	    Trace.trace(14,"alive - ERROR, config server get timed out")
	    return {'status' : (e_errors.TIMEDOUT, None)}
	try:
            x = self.u.send({'work':'alive'}, (t['hostip'], t['port']),
                            rcv_timeout, tries)
	except errno.errorcode[errno.ETIMEDOUT]:
	    Trace.trace(14,"alive - ERROR, alive timed out")
	    x = {'status' : (e_errors.TIMEDOUT, None)}
        except KeyError, detail:
            sys.stderr.write("Unknown server %s (no key %s)\n" % (server, detail))
            os._exit(1)
        return x


    def trace_levels(self, server, work, levels):
        try:
            t = self.csc.get(server)
        except errno.errorcode[errno.ETIMEDOUT]:
	    return {'status' : (e_errors.TIMEDOUT, None)}
	try:
            x = self.u.send({'work': work,
                             'levels':levels}, (t['hostip'], t['port']))
	except errno.errorcode[errno.ETIMEDOUT]:
	    x = {'status' : (e_errors.TIMEDOUT, None)}
        except KeyError, detail:
            print "Unknown server", server
            sys.exit(-1)
        return x
    
    
    def handle_generic_commands(self, server, intf):
        ret = None
        if intf.alive:
            ret = self.alive(server, intf.alive_rcv_timeout,intf.alive_retries)
        if intf.do_print:
            ret = self.trace_levels(server, 'do_print', intf.do_print)
        if intf.dont_print:
            ret = self.trace_levels(server, 'dont_print', intf.dont_print)
        if intf.do_log:
            ret = self.trace_levels(server, 'do_log', intf.do_log)
        if intf.dont_log:
            ret = self.trace_levels(server, 'dont_log', intf.dont_log)
        if intf.do_alarm:
            ret = self.trace_levels(server, 'do_alarm', intf.do_alarm)
        if intf.dont_alarm:
            ret = self.trace_levels(server, 'dont_alarm', intf.dont_alarm)
        return ret

            
    # examine the final ticket to check for any errors
    def check_ticket(self, ticket):
	if not 'status' in ticket.keys(): return None
        if ticket['status'][0] == e_errors.OK:
            Trace.trace(14, repr(ticket))
            Trace.trace(14, 'exit ok' )
            sys.exit(0)
        else:
            print "BAD STATUS", ticket['status']
##
            
            Trace.trace(14, " BAD STATUS - "+repr(ticket['status']))
            sys.exit(1)
	return None

    # tell the server to spill its guts
    def dump(self, rcv_timeout=0, tries=0):
        x = self.send({'work':'dump'}, rcv_timeout, tries)
        return x
