###############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
import sys
import time
import errno
import pprint

# enstore imports
import generic_client
import interface
import udp_client
import Trace
import callback
import e_errors

# Import SOCKS module if it exists, else standard socket module socket
# This is a python module that works just like the socket module, but uses the
# SOCKS protocol to make connections through a firewall machine.
# See http://www.w3.org/People/Connolly/support/socksForPython.html or
# goto www.python.org and search for "import SOCKS"
try:
    import SOCKS
    socket = SOCKS
except ImportError:
    import socket

MY_NAME = "CONFIG_CLIENT"
MY_SERVER = "configuration_server"

class ConfigurationClient(generic_client.GenericClient):

    def __init__(self, csc):
        self.clear()
	self.print_id = MY_NAME
	Trace.trace(8, "Connecting to configuration server at %s %s"\
                    %(csc[0], csc[1]))
        self.config_address=csc
        self.u = udp_client.UDPClient()
        Trace.trace(11,'add='+repr(self.config_address)+\
                    ' udp='+repr(self.u))

    # return the address of the configuration server
    def get_address(self):
	return self.config_address

    # get rid of all cached values - go back to server for information
    def clear(self):
        self.cache = {}

    # get value for requested item from server, store locally in own cache
    def get_uncached(self, key, timeout=0, retry=0):
        request = {'work' : 'lookup', 'lookup' : key }
        while 1:
            try:
                self.cache[key] = self.u.send(request, self.config_address,\
                                              timeout, retry)
                break
            except socket.error:
	        self.output_socket_error("get_uncached")
        return self.cache[key]

    # output the socket error
    def output_socket_error(self, id):
        if sys.exc_info()[1][0] == errno.CONNREFUSED:
            delay = 3
            Trace.trace(6,repr(id)+" retrying "+ \
	                str(sys.exc_info()[0])+str(sys.exc_info()[1]))
            Trace.trace(10,str(sys.exc_info()[1][0])+" "+\
	                 "socket error. configuration sending to "+\
	                 repr(self.config_address)+\
                         "server down?  retrying in "+repr(delay)+" seconds")
            time.sleep(delay)
        else:
            Trace.trace(6, repr(id)+" "+str(sys.exc_info()[0])+\
                        str(sys.exc_info()[1]))
            raise 

    # return cached (or get from server) value for requested item
    def get(self, key, timeout=0, retry=0):
        # try the cache
        return self.cache.get(key, self.get_uncached(key, timeout, retry))

    # dump the configuration dictionary
    def dump(self, timeout=0, retry=0):
        host, port, listen_socket = callback.get_callback()
        request = {'work' : 'dump',
                   'callback_addr'  : (host,port)
                   }
        try:
            listen_socket.listen(1)
            x=self.u.send(request, self.config_address, timeout, retry)
            control_socket, addr = listen_socket.accept()
            self.config_dump = callback.read_tcp_obj(control_socket)
        except socket.error:
            self.output_socket_error("dump")
                
    # get all keys in the configuration dictionary
    def get_keys(self, timeout=0, retry=0):
        request = {'work' : 'get_keys' }
        while 1:
            try:
                keys = self.u.send(request, self.config_address, timeout,\
	                           retry )
                return keys
            except socket.error:
	        self.output_socket_error("get_keys")

    # reload a new  configuration dictionary
    def load(self, configfile, timeout=0, retry=0):
        request = {'work' : 'load' ,  'configfile' : configfile }
        while 1:
            try:
                x = self.u.send(request, self.config_address, timeout, retry)
                return x
            except socket.error:
	        self.output_socket_error("load retrying")

    # check on alive status
    def alive(self, server, rcv_timeout=0, tries=0):
        Trace.trace(10,'alive config_address='+repr(self.config_address))
        try:
            x = self.u.send({'work':'alive'},self.config_address, rcv_timeout,
                          tries)
        except errno.errorcode[errno.ETIMEDOUT]:
	    Trace.trace(14,"alive - ERROR, alive timed out")
	    x = {'status' : (e_errors.TIMEDOUT, None)}
        return x

    # get list of the Library manager movers
    def get_movers(self, library_manager, timeout=0, retry=0):
        request = {'work' : 'get_movers' ,  'library' : library_manager }
        while 1:
            try:
                x = self.u.send(request, self.config_address, timeout, retry)
                return x
            except socket.error:
	        self.output_socket_error("get_movers")

    # get media changer associated with a library manager
    def get_media_changer(self, library_manager, timeout=0, retry=0):
        request = {'work' : 'get_media_changer' ,
                   'library' : library_manager }
        while 1:
            try:
                x = self.u.send(request, self.config_address, timeout, retry)
                return x
            except socket.error:
	        self.output_socket_error("get_media_changer")
	
    #get list of library managers
    def get_library_managers(self, ticket, timeout=0, retry=0):
        request = {'work': 'get_library_managers'}
        while 1:
            try:
                x = self.u.send(request, self.config_address, timeout, retry)
                return x
            except socket.error:
	        self.output_socket_error("get_library_managers")

    #get list of media changers
    def get_media_changers(self, ticket, timeout=0, retry=0):
        request = {'work': 'get_media_changers'}
        while 1:
            try:
                x = self.u.send(request, self.config_address, timeout, retry)
                return x
            except socket.error:
	        self.output_socket_error("get_media_changers")

    # get the configuration dictionary element(s) that contain the specified
    # key, value pair
    def get_dict_entry(self, keyValue, timeout=0, retry=0):
        request = {'work': 'get_dict_element',
                   'keyValue': keyValue }
        while 1:
            try:
                x = self.u.send(request, self.config_address, timeout, retry)
                return x
            except socket.error:
	        self.output_socket_error("get_dict_element")

        
class ConfigurationClientInterface(generic_client.GenericClientInterface):
    def __init__(self, flag=1, opts=[]):
        # fill in the defaults for the possible options
        self.do_parse = flag
        self.restricted_opts = opts
        self.config_dump = {}
        self.config_file = ""
        self.show = 0
        self.load = 0
        self.alive_rcv_timeout = 0
        self.alive_retries = 0
        self.summary = 0
        generic_client.GenericClientInterface.__init__(self)

        # if we are using the default host and port, warn the user
        interface.check_for_config_defaults()

    # define the command line options that are valid
    def options(self):
        if self.restricted_opts:
            return self.restricted_opts
        else:
            return self.client_options()+[
                "config_file=","summary","show","load"]

def do_work(intf):
    # now get a configuration client
    csc = ConfigurationClient((intf.config_host, intf.config_port))

    if intf.alive:
        stati = csc.alive(MY_SERVER, intf.alive_rcv_timeout,intf.alive_retries)

    elif intf.show:
        csc.dump(intf.alive_rcv_timeout,intf.alive_retries)
        print csc.config_dump["dump"]
        stati = csc.config_dump

    elif intf.load:
        stati= csc.load(intf.config_file, intf.alive_rcv_timeout, \
	                intf.alive_retries)

    elif intf.summary:
        stati= csc.get_keys(intf.alive_rcv_timeout,intf.alive_retries)
	pprint.pprint(stati['get_keys'])

    else:
	intf.print_help()
        sys.exit(0)


    del csc.u		# del now, otherwise get name exception (just for python v1.5???)

    csc.check_ticket(stati)

if __name__ == "__main__":
    Trace.init(MY_NAME)
    Trace.trace(6,"config client called with args "+repr(sys.argv))

    # fill in interface
    intf = ConfigurationClientInterface()

    do_work(intf)
