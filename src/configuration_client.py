###############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
import sys
import time
import errno

# enstore imports
import generic_client
import generic_cs
import interface
import udp_client
import Trace
import e_errors

# Import SOCKS module if it exists, else standard socket module socket
# This is a python module that works just like the socket module, but uses the
# SOCKS protocol to make connections through a firewall machine.
# See http://www.w3.org/People/Connolly/support/socksForPython.html or
# goto www.python.org and search for "import SOCKS"
try:
    import SOCKS; socket = SOCKS
except ImportError:
    import socket

def set_csc(self, csc=0, host=interface.default_host(),\
            port=interface.default_port(), verbose=0):
    Trace.trace(10,'{set_csc csc='+repr(csc))
    if csc == 0:
        self.csc = ConfigurationClient(host, port, verbose)
    else:
        self.csc = csc
    Trace.trace(10,'}set_csc csc='+repr(self.csc))

class ConfigurationClient(generic_client.GenericClient):

    def __init__(self, config_host, config_port, verbose):
        Trace.trace(10,'{__init__ cc')
        self.clear()
	self.print_id = "CONFIGC"
	self.enprint("Connecting to configuration server at "+\
	             config_host+" "+repr(config_port), \
	             generic_cs.CONNECTING, verbose)
	self.verbose = verbose
        self.config_address=(config_host,config_port)
        self.u = udp_client.UDPClient()
        Trace.trace(11,'}connect add='+repr(self.config_address)+\
                    ' udp='+repr(self.u))

    # return the address of the configuration server
    def get_address(self):
	return self.config_address

    # get rid of all cached values - go back to server for information
    def clear(self):
        Trace.trace(16,'{clear')
        self.cache = {}
        Trace.trace(16,'}clear')

    # get value for requested item from server, store locally in own cache
    def get_uncached(self, key, timeout=0, retry=0):
        Trace.trace(11,'{get_uncached key='+repr(key))
        request = {'work' : 'lookup', 'lookup' : key }
        while 1:
            try:
                self.cache[key] = self.u.send(request, self.config_address,\
                                              timeout, retry)
                break
            except socket.error:
	        self.output_socket_error("get_uncached")
        Trace.trace(11,'}get_uncached key='+repr(key)+'='+repr(self.cache[key]))
        return self.cache[key]

    # output the socket error
    def output_socket_error(self, id):
        if sys.exc_info()[1][0] == errno.CONNREFUSED:
            delay = 3
            Trace.trace(0,"}"+id+" retrying "+ \
	                str(sys.exc_info()[0])+str(sys.exc_info()[1]))
	    self.enprint(str(sys.exc_info()[1][0])+" "+\
	                 "socket error. configuration sending to "+\
	                 repr(self.config_address)+\
                         "server down?  retrying in "+repr(delay)+" seconds",\
	                 generic_cs.SOCKET_ERROR, self.verbose)
            time.sleep(delay)
        else:
            Trace.trace(0,"}+id+"+str(sys.exc_info()[0])+\
                        str(sys.exc_info()[1]))
            raise sys.exc_info()[0],sys.exc_info()[1]

    # return cached (or get from server) value for requested item
    def get(self, key, timeout=0, retry=0):
        Trace.trace(11,'{get (cached) key='+repr(key))
        # try the cache
        try:
            val = self.cache[key]
        except:
            val = self.get_uncached(key, timeout, retry)
        Trace.trace(11,'}get (cached) key='+repr(key)+'='+repr(val))
        return val


    # dump the configuration dictionary
    def list(self, timeout=0, retry=0):
        Trace.trace(16,'{list')
        request = {'work' : 'list' }
        while 1:
            try:
                self.config_list = self.u.send(request, self.config_address,\
	                                       timeout, retry )
                break
            except socket.error:
	        self.output_socket_error("list")
        Trace.trace(16,'}list')

    # get all keys in the configuration dictionary
    def get_keys(self, timeout=0, retry=0):
        Trace.trace(16,'{get_keys')
        request = {'work' : 'get_keys' }
        while 1:
            try:
                keys = self.u.send(request, self.config_address, timeout,\
	                           retry )
                Trace.trace(16,'}get_keys ' + repr(keys))
                return keys
            except socket.error:
	        self.output_socket_error("get_keys")
        Trace.trace(16,'}get_keys')

    # reload a new  configuration dictionary
    def load(self, configfile, timeout=0, retry=0):
        Trace.trace(10,'{load configfile='+repr(configfile))
        request = {'work' : 'load' ,  'configfile' : configfile }
        while 1:
            try:
                x = self.u.send(request, self.config_address, timeout, retry)
                Trace.trace(16,'}load '+repr(x))
                return x
            except socket.error:
	        self.output_socket_error("load retrying")
        Trace.trace(16,'}load')

    # check on alive status
    def alive(self, rcv_timeout=0, tries=0):
        Trace.trace(10,'{alive config_address='+repr(self.config_address))
        x = self.u.send({'work':'alive'},self.config_address, rcv_timeout,\
	                tries)
        Trace.trace(10,'}alive '+repr(x))
        return x

    # reset the servers verbose flag
    def set_verbose(self, verbosity, rcv_timeout=0, tries=0):
        Trace.trace(10,'{set_verbose (client)')
        x = self.u.send({'work':'set_verbose', 'verbose': verbosity}, \
	                self.config_address, rcv_timeout, tries)
        Trace.trace(10,'}set_verbose (client) '+repr(x))
        return x

    # get list of the Library manager movers
    def get_movers(self, library_manager, timeout=0, retry=0):
        Trace.trace(10,'{get_movers for '+repr(library_manager))
        request = {'work' : 'get_movers' ,  'library' : library_manager }
        while 1:
            try:
                x = self.u.send(request, self.config_address, timeout, retry)
                Trace.trace(16,'}get_movers '+repr(x))
                return x
            except socket.error:
	        self.output_socket_error("get_movers")
        Trace.trace(16,'}get_movers')
	

class ConfigurationClientInterface(interface.Interface):
    def __init__(self):
        # fill in the defaults for the possible options
	self.config_list = {}
       	self.config_file = ""
        self.verbose = 0
        self.dict = 0
        self.load = 0
        self.alive = 0
        self.alive_rcv_timeout = 0
        self.alive_retries = 0
       	self.get_keys = 0
	self.got_server_verbose = 0
        interface.Interface.__init__(self)

        # parse the options
        self.parse_options()

    # define the command line options that are valid
    def options(self):
        return self.config_options() + self.verbose_options()+\
	       ["config_file=","get_keys","dict","load"] + \
	       self.alive_options()+self.help_options()

if __name__ == "__main__":
    import sys
    Trace.init("config cli")
    Trace.trace(1,"config client called with args "+repr(sys.argv))

    # fill in interface
    intf = ConfigurationClientInterface()

    # now get a configuration client
    csc = ConfigurationClient(intf.config_host, intf.config_port,\
                               intf.verbose)

    if intf.alive:
        stati = csc.alive(intf.alive_rcv_timeout,intf.alive_retries)
	msg_id = generic_cs.ALIVE

    elif intf.got_server_verbose:
        stati = csc.set_verbose(intf.server_verbose, intf.alive_rcv_timeout,\
	                        intf.alive_retries)
	msg_id = generic_cs.CLIENT

    elif intf.dict:
        csc.list(intf.alive_rcv_timeout,intf.alive_retries)
	generic_cs.enprint(csc.config_list["list"])
        stati = csc.config_list
	msg_id = generic_cs.CLIENT

    elif intf.load:
        stati= csc.load(intf.config_file, intf.alive_rcv_timeout, \
	                intf.alive_retries)
	msg_id = generic_cs.CLIENT

    elif intf.get_keys:
        stati= csc.get_keys(intf.alive_rcv_timeout,intf.alive_retries)
	generic_cs.enprint(stati['get_keys'], generic_cs.PRETTY_PRINT)
	msg_id = generic_cs.CLIENT

    else:
	intf.print_help()
        sys.exit(0)


    del csc.u		# del now, otherwise get name exception (just for python v1.5???)

    csc.check_ticket(stati, msg_id)
