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
import socket

#enstore imports
import setpath
import Trace
import e_errors
import option
import udp_client
import enstore_constants

class GenericClientInterface(option.Interface):

    def __init__(self, args=sys.argv, user_mode=1):
        self.dump = 0
        self.alive = 0
        self.alive_rcv_timeout = 0
        self.alive_retries = 0
        self.do_print = []
        self.dont_print = []
        self.do_log = []
        self.dont_log = []
        self.do_alarm = []
        self.dont_alarm = []
        option.Interface.__init__(self, args=args, user_mode=user_mode)

    def client_options(self):
        return (self.alive_options()  + 
                self.trace_options() + 
                self.help_options() )
    
    def complete_server_name(self, server_name, server_type):
        if not server_name:
            return server_name

        #If the complete name of a server, for example is rain.mover, then
        # server_name is the string we want to check to see if it is appended
        # by "." + server_type.
        try:
            if server_name[(-len(server_type) - 1):] != "." + server_type:
                server_name = server_name + "." + server_type
            else:
                server_name = server_name
        except IndexError:
            #The string does not contain enough characters to end in
            # ".mover".  So, it must be added.
            server_name = server_name + "." + server_type

        return server_name

class GenericClient:

    def __init__(self, csc, name, server_address=None, flags=0, logc=None,
                 alarmc=None, rcv_timeout=0, rcv_tries=0, server_name=None):

        #Remember this if passed in.  self.server_name used by _is_csc().
        self._server_name = server_name
        
	if not flags & enstore_constants.NO_UDP and not self.__dict__.get('u', 0):
	    self.u = udp_client.UDPClient()

        #If this generic client is a configuration client, then don't set
        # self.csc.  Doing so would create a circular reference that creates
        # resource leaks.  Instead, for the duration of __init__() just use
        # csc.  Outside of __init__() use self._get_csc().
        if self._is_csc():
            # this is the configuration client, we don't need this other stuff
            csc = self
            #return
        #else:
            #self.csc = self

	# get the configuration client
	if not flags & enstore_constants.NO_CSC:
	    import configuration_client

	    if csc:
		if type(csc) == type(()):
		    self.csc = configuration_client.ConfigurationClient(csc)
		else:
		    # it is not a tuple of address and port, so we assume that
		    # it is a configuration client object
		    self.csc = csc
	    else:
		# it is None or 0 (the default value from i.e. log_client)
		def_addr = (os.environ['ENSTORE_CONFIG_HOST'],
			    string.atoi(os.environ['ENSTORE_CONFIG_PORT']))
		self.csc = configuration_client.ConfigurationClient( def_addr )

        if server_address:    
            self.server_address = server_address
        else:
            self.server_address = self.get_server_address(
                server_name, rcv_timeout=rcv_timeout, tries=rcv_tries)
                
        # try to find the logname for this object in the config dict.  use
        # the lowercase version of the name as the server key.  if this
        # object is not defined in the config dict, then just use the
        # passed in name.
        self.log_name = self.get_name(name)
	# get the log client
	if logc:
	    # we were given one, use it
	    self.logc = logc
	else:
	    if not flags & enstore_constants.NO_LOG:
		import log_client
		self.logc = log_client.LoggerClient(csc, self.log_name,
						    'log_server', 
		   flags=enstore_constants.NO_ALARM | enstore_constants.NO_LOG,
                                                    rcv_timeout=rcv_timeout,
                                                    rcv_tries=rcv_tries)

	# get the alarm client
	if alarmc:
	    # we were given one, use it
	    self.alarmc = alarmc
	else:
	    if not flags & enstore_constants.NO_ALARM:
		import alarm_client
		self.alarmc = alarm_client.AlarmClient(csc, 
		   flags=enstore_constants.NO_ALARM | enstore_constants.NO_LOG,
                                                       rcv_timeout=rcv_timeout,
                                                       rcv_tries=rcv_tries)

    def __del__(self):
        try:
            del self.u
        except AttributeError:
            pass
        try:
            del self.logc
        except AttributeError:
            pass
        try:
            del self.alarmc
        except AttributeError:
            pass

    def _is_csc(self):
        #If the server requested is the configuration server,
        # do something different.
        if self._server_name == enstore_constants.CONFIGURATION_SERVER or \
           self.__dict__.get('is_config', 0):
            return 1
        else:
            return 0

    def _get_csc(self):
        #If the server address requested is the configuration server,
        # do something different.
        if self._is_csc():
            return self
        else:
            return self.csc

    def get_server_address(self, MY_SERVER,  rcv_timeout=0, tries=0):
        #If the server address requested is the configuration server,
        # do something different.
        if MY_SERVER == enstore_constants.CONFIGURATION_SERVER or \
           self.__dict__.get('is_config', 0):
            host = os.environ.get("ENSTORE_CONFIG_HOST",'localhost')
            hostip = socket.gethostbyname(host)
            port = int(os.environ.get("ENSTORE_CONFIG_PORT",'localhost'))
            ticket = {'host':host, 'hostip':hostip, 'port':port,
                      'status':(e_errors.OK, None)}
        elif MY_SERVER == enstore_constants.MONITOR_SERVER:
            host = socket.gethostname()
            hostip = socket.gethostbyname(host)
            port = enstore_constants.MONITOR_PORT
            ticket = {'host':host, 'hostip':hostip, 'port':port,
                      'status':(e_errors.OK, None)}
        #For a normal server.
        else:
            ticket = self.csc.get(MY_SERVER, rcv_timeout, tries)

        #Check for errors.
        if ticket['status'][0] != e_errors.OK:
            return None
        
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
        except:
            exc, msg, tb = sys.exc_info()
            if exc == errno.errorcode[errno.ETIMEDOUT]:
                x = {'status' : (e_errors.TIMEDOUT, msg)}
            else:
                x = {'status' : (str(exc), str(msg))}
        return x
        
    # return the name used for this client/server #XXX what is this nonsense? cgw
    def get_name(self, name):
        return name

    # check on alive status
    def alive(self, server, rcv_timeout=0, tries=0):
        #Get the address information from config server.
        csc = self._get_csc()
        try:
            t = csc.get(server)
        except errno.errorcode[errno.ETIMEDOUT]:
            return {'status' : (e_errors.TIMEDOUT, None)}
        
        #Check for errors.
        if t['status'] == (e_errors.TIMEDOUT, None):
            Trace.trace(14,"alive - ERROR, config server get timed out")
            return {'status' : (e_errors.CONFIGDEAD, None)}
        elif t['status'] != (e_errors.OK, None):
            return {'status':t['status']}
        
        #Send and recieve the alive message.
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
        csc = self._get_csc()
        try:
            t = csc.get(server)
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
            Trace.trace(14, " BAD STATUS - "+repr(ticket['status']))
            sys.exit(1)
        return None

    # tell the server to spill its guts
    def dump(self, rcv_timeout=0, tries=0):
        x = self.send({'work':'dump'}, rcv_timeout, tries)
        return x

    # tell the server to 'go away' in a polite manner.
    def quit(self, rcv_timeout=0, tries=0):
        x = self.send({'work':'quit'}, rcv_timeout, tries)
        return x
