###############################################################################
# src/$RCSfile$   $Revision$
#
#system imports
import sys
import errno
import pprint
import types
import os				# for os.environ for "default" config*
import string				# atoi for os.environ of config port

#enstore imports
import Trace
import e_errors
import interface

class GenericClientInterface(interface.Interface):

    def __init__(self):
	self.dump = 0
	self.alive = 0
	interface.Interface.__init__(self)

    def client_options(self):
	return self.config_options() + \
	       self.alive_options()  + self.help_options()
    
init_done = 0

class GenericClient:

    def __init__(self, csc, name):
        global init_done
        if not init_done:
            # we only want to get these clients once per process
            init_done = 1
            import configuration_client
            import log_client
            import alarm_client
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
                self.logc = log_client.LoggerClient(self.csc, self.log_name,
                                                    'log_server')
            if not self.__dict__.get('is_alarm', 0):
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
        t = self.csc.get(server)
	try:
            x = self.u.send({'work':'alive'}, (t['hostip'], t['port']),
                            rcv_timeout, tries)
	except errno.errorcode[errno.ETIMEDOUT]:
	    Trace.trace(14,"alive - ERROR, alive timed out")
	    x = {'status' : (e_errors.TIMEDOUT, None)}
        return x

    # examine the final ticket to check for any errors
    def check_ticket(self, ticket):
	if not 'status' in ticket.keys(): return None
        if ticket['status'][0] == e_errors.OK:
            Trace.trace(14, repr(ticket))
            Trace.trace(14, 'exit ok' )
            sys.exit(0)
        else:
            pprint.pprint("BAD STATUS: "+repr(ticket))
            Trace.trace(14, " BAD STATUS - "+repr(ticket['status']))
            sys.exit(1)
	return None

    # tell the server to spill it's guts
    def dump(self, rcv_timeout=0, tries=0):
        x = self.send({'work':'dump'}, rcv_timeout, tries)
        return x
