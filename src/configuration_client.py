###############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
import sys
import time
import errno
import pprint
import os
import socket
import select

# enstore imports
import generic_client
import interface
import udp_client
import Trace
import callback
import e_errors
import hostaddr

MY_NAME = "CONFIG_CLIENT"
MY_SERVER = "configuration_server"

class ConfigurationClient(generic_client.GenericClient):

    def __init__(self, address=None):
        if address is None:
            address = (os.environ.get("ENSTORE_CONFIG_HOST", 'localhost'),
                       int(os.environ.get("ENSTORE_CONFIG_PORT", 7500)))
        self.print_id = MY_NAME
        self.server_address=address
        self.u = udp_client.UDPClient()

    #Retrun these values when requested.
    def get_address(self):
        return self.server_address
    def get_timeout(self):
        return self.timeout
    def get_retry(self):
        return self.retry

    # return value for requested item
    def get(self, key, timeout=0, retry=0):
        self.timeout = timeout #Remember this.
        self.retry = retry     #Remember this.
        if key=='configuration_server':
            ret = {'hostip':self.server_address[0], 'port':self.server_address[1]}
        else:
            request = {'work' : 'lookup', 'lookup' : key }
            while 1:
                try:
                    ret = self.send(request, timeout, retry)
                    break
                except socket.error:
                    self.output_socket_error("get")
        return ret

    # dump the configuration dictionary
    def dump(self, timeout=0, retry=0):
        host, port, listen_socket = callback.get_callback()
        listen_socket.listen(4)
        
        request = {'work' : 'dump',
                   'callback_addr'  : (host,port)
                   }

        reply = self.send(request, timeout, retry)
        r, w, x = select.select([listen_socket], [], [], 15)
        if not r:
            raise errno.errorcode[errno.ETIMEDOUT], "timeout waiting for configuration server callback"
        control_socket, addr = listen_socket.accept()
        if not hostaddr.allow(addr):
            listen_socket.close()
            control_socket.close()
            raise errno.errorcode[errno.EPROTO], "address %s not allowed" %(address,)
        d = callback.read_tcp_obj(control_socket)
        listen_socket.close()
        return d
        
    # get all keys in the configuration dictionary
    def get_keys(self, timeout=0, retry=0):
        request = {'work' : 'get_keys' }
        keys = self.send(request,  timeout,  retry )
        return keys

    # reload a new  configuration dictionary
    def load(self, configfile, timeout=0, retry=0):
        request = {'work' : 'load' ,  'configfile' : configfile }
        x = self.send(request, timeout, retry)
        return x

    def alive(self, server, rcv_timeout=0, tries=0):
        return self.send({'work':'alive'}, rcv_timeout, tries)

    # get list of the Library manager movers
    def get_movers(self, library_manager, timeout=0, retry=0):
        request = {'work' : 'get_movers' ,  'library' : library_manager }
        return self.send(request, timeout, retry)

    # get media changer associated with a library manager
    def get_media_changer(self, library_manager, timeout=0, retry=0):
        request = {'work' : 'get_media_changer' ,
                   'library' : library_manager }
        return  self.send(request, timeout, retry)
	
    #get list of library managers
    def get_library_managers(self, ticket, timeout=0, retry=0):
        request = {'work': 'get_library_managers'}
        return self.send(request, timeout, retry)

    #get list of media changers
    def get_media_changers(self, ticket, timeout=0, retry=0):
        request = {'work': 'get_media_changers'}
        return self.send(request, timeout, retry)

    # get the configuration dictionary element(s) that contain the specified
    # key, value pair
    def get_dict_entry(self, keyValue, timeout=0, retry=0):
        request = {'work': 'get_dict_element',
                   'keyValue': keyValue }
        return self.send(request, timeout, retry)
        
class ConfigurationClientInterface(generic_client.GenericClientInterface):
    def __init__(self, flag=1, opts=[]):
        # fill in the defaults for the possible options
        self.do_parse = flag
        self.restricted_opts = opts
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
                "config-file=","summary","show","load"]

    #  define our specific help
    def parameters(self):
        return "element"

    # parse the options like normal but see if we have a server
    def parse_options(self):
        interface.Interface.parse_options(self)
        # see if we have an element 
        if self.show:
            if len(self.args) < 1 :
                # no parameter, show all
                self.element = ""
            else:
                self.element = self.args[0]

def do_work(intf):
    csc = ConfigurationClient((intf.config_host, intf.config_port))
    csc.csc = csc
    result = csc.handle_generic_commands(MY_SERVER, intf)
    if result:
        pass
    elif intf.show:
        result = csc.dump(intf.alive_rcv_timeout,intf.alive_retries)
        if intf.element:
            pprint.pprint(result["dump"].get(intf.element, {}))
        else:
            pprint.pprint(result["dump"])

    elif intf.load:
        result= csc.load(intf.config_file, intf.alive_rcv_timeout,
	                intf.alive_retries)

    elif intf.summary:
        result= csc.get_keys(intf.alive_rcv_timeout,intf.alive_retries)
        pprint.pprint(result['get_keys'])
        
    else:
	intf.print_help()
        sys.exit(0)

    csc.check_ticket(result)

if __name__ == "__main__":
    Trace.init(MY_NAME)

    # fill in interface
    intf = ConfigurationClientInterface()

    do_work(intf)
