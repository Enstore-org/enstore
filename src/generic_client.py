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
        if not init_done:
            # we only want to get these clients once per process
            init_done = 1
            import configuration_client
            if csc and (type(csc) == types.TupleType):
                self.csc = configuration_client.ConfigurationClient((csc[0],
                                                                    csc[1]))
            elif csc:
                # it is not a tuple of address and port, so we assume that it
                # is a configuration client object
                self.csc = csc
	    else:
		# it is a 0 (the default value from i.e. log_client)
		def_addr = (os.environ['ENSTORE_CONFIG_HOST'],
			    string.atoi(os.environ['ENSTORE_CONFIG_PORT']))
                self.csc = configuration_client.ConfigurationClient( def_addr )
		pass
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
        else:
            if csc and (not type(csc) == types.TupleType):
                # we assume this is a configuration client object since it is
                # not a tuple of the address and port.
                self.csc = csc

    # return the name used for this client/server
    def get_name(self, name):
        return name

    # check on alive status
    def alive(self, server, rcv_timeout=0, tries=0):
        try:
            t = self.csc.get(server, rcv_timeout, tries)
        except errno.errorcode[errno.ETIMEDOUT]:
            Trace.trace(14,"alive - ERROR, config server get timed out")
	    return {'status' : (e_errors.TIMEDOUT, None)}
	try:
            x = self.u.send({'work':'alive'}, (t['hostip'], t['port']),
                            rcv_timeout, tries)
	except errno.errorcode[errno.ETIMEDOUT]:
	    Trace.trace(14,"alive - ERROR, alive timed out")
	    x = {'status' : (e_errors.TIMEDOUT, None)}
        except KeyError, detail:
            print "Unknown server", server
            sys.exit(-1)
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
        if intf.alive:
            ret = self.alive(server, intf.alive_rcv_timeout,intf.alive_retries)
        elif intf.do_print:
            ret = self.trace_levels(server, 'do_print', intf.do_print)
        elif intf.dont_print:
            ret = self.trace_levels(server, 'dont_print', intf.dont_print)
        elif intf.do_log:
            ret = self.trace_levels(server, 'do_log', intf.do_log)
        elif intf.dont_log:
            ret = self.trace_levels(server, 'dont_log', intf.dont_log)
        elif intf.do_alarm:
            ret = self.trace_levels(server, 'do_alarm', intf.do_alarm)
        elif intf.dont_alarm:
            ret = self.trace_levels(server, 'dont_alarm', intf.dont_alarm)
        else:
            return None

        return ret
            
    # examine the final ticket to check for any errors
    def check_ticket(self, ticket):
	if not 'status' in ticket.keys(): return None
        if ticket['status'][0] == e_errors.OK:
            Trace.trace(14, repr(ticket))
            Trace.trace(14, 'exit ok' )
            sys.exit(0)
        else:
            print "BAD STATUS"
            pprint.pprint(ticket)
            Trace.trace(14, " BAD STATUS - "+repr(ticket['status']))
            sys.exit(1)
	return None

    # tell the server to spill its guts
    def dump(self, rcv_timeout=0, tries=0):
        x = self.send({'work':'dump'}, rcv_timeout, tries)
        return x

