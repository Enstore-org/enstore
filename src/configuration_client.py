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
import enstore_constants
import option
import udp_client
import Trace
import callback
import e_errors
import hostaddr
import option

MY_NAME = "CONFIG_CLIENT"
MY_SERVER = "configuration_server"

class ConfigurationClient(generic_client.GenericClient):

    def __init__(self, address=None):
        if address is None:
            address = (os.environ.get("ENSTORE_CONFIG_HOST", 'localhost'),
                       int(os.environ.get("ENSTORE_CONFIG_PORT", 7500)))
        self.print_id = MY_NAME
	flags = enstore_constants.NO_CSC | enstore_constants.NO_ALARM | \
		enstore_constants.NO_LOG
	generic_client.GenericClient.__init__(self, (), MY_NAME, address, flags=flags)
	self.new_config_obj = None
	self.saved_dict = {}

    #Retrun these values when requested.
    def get_address(self):
        return self.server_address
    def get_timeout(self):
        return self.timeout
    def get_retry(self):
        return self.retry

    def do_lookup(self, key, timeout, retry):
	request = {'work' : 'lookup', 'lookup' : key }
	while 1:
	    try:
		ret = self.send(request, timeout, retry)
		break
	    except socket.error:
		self.output_socket_error("get")
	self.saved_dict[key] = ret
	Trace.trace(13, "Get %s config info from server"%(key,))
	return ret

    # return value for requested item
    def get(self, key, timeout=0, retry=0):
        self.timeout = timeout #Remember this.
        self.retry = retry     #Remember this.
        if key=='configuration_server':
            ret = {'hostip':self.server_address[0], 'port':self.server_address[1]}
        else:
	    # if we have a new_config_obj, then only go to the config server if we
	    # have received a message saying a new one was loaded.
	    if not self.new_config_obj or self.new_config_obj.have_new_config():
		# clear out the cached copies
		self.saved_dict = {}
		ret = self.do_lookup(key, timeout, retry)
		if self.new_config_obj:
		    self.new_config_obj.reset_new_config()
	    else:
		# there was no new config loaded, just return what we have.  if we
		# do not have a stashed copy, go get it.
		if self.saved_dict.has_key(key):
		    Trace.trace(13, "Returning %s config info from saved_dict"%(key,))
		    Trace.trace(13, "saved_dict - %s"%(self.saved_dict,))
		    ret = self.saved_dict[key]
		else:
		    ret = self.do_lookup(key, timeout, retry)
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
        control_socket, address = listen_socket.accept()
        if not hostaddr.allow(address):
            listen_socket.close()
            control_socket.close()
            raise errno.errorcode[errno.EPROTO], "address %s not allowed" %(address,)
        try:
            d = callback.read_tcp_obj(control_socket)
        except e_errors.TCP_EXCEPTION:
            d = {'status':(e_errors.TCP_EXCEPTION, e_errors.TCP_EXCEPTION)}
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
    def __init__(self, args=sys.argv, user_mode=1):
        # fill in the defaults for the possible options
        #self.do_parse = flag
        #self.restricted_opts = opts
        self.config_file = ""
        self.show = 0
        self.load = 0
        self.alive_rcv_timeout = 0
        self.alive_retries = 0
        self.summary = 0

        generic_client.GenericClientInterface.__init__(self, args=args,
                                                       user_mode=user_mode)

        # if we are using the default host and port, warn the user
        option.check_for_config_defaults()

    def valid_dictionaries(self):
        return (self.help_options, self.alive_options, self.trace_options,
                self.config_options)

    config_options = {
        option.SHOW:{option.HELP_STRING:"print the current configuration",
                     option.DEFAULT_TYPE:option.INTEGER,
                     option.USER_LEVEL:option.ADMIN},
        option.LOAD:{option.HELP_STRING:"load a new configuration",
                     option.DEFAULT_TYPE:option.INTEGER,
		     option.USER_LEVEL:option.ADMIN},
        option.SUMMARY:{option.HELP_STRING:"summary for saag",
                        option.DEFAULT_TYPE:option.INTEGER,
			option.USER_LEVEL:option.ADMIN},
        option.CONFIG_FILE:{option.HELP_STRING:"config file to load",
                            option.VALUE_USAGE:option.REQUIRED,
                            option.DEFAULT_TYPE:option.STRING,
			    option.USER_LEVEL:option.ADMIN},
         }

    # parse the options like normal but make sure we have other args
    def parse_options(self):

        generic_client.GenericClientInterface.parse_options(self)

        try:
            self.element = self.args[0]
        except IndexError:
            self.element = None
        

def do_work(intf):
    csc = ConfigurationClient((intf.config_host, intf.config_port))
    csc.csc = csc
    result = csc.handle_generic_commands(MY_SERVER, intf)
    if intf.alive:
        if result['status'] == (e_errors.OK, None):
            print "Server configuration found at %s." % (result['address'],)
        else:
            print result
    if result:
        pass
    elif intf.show:
        result = csc.dump(intf.alive_rcv_timeout,intf.alive_retries)
        
        if e_errors.is_ok(result) and intf.element:
            pprint.pprint(result["dump"].get(intf.element, {}))
        elif e_errors.is_ok(result):
            pprint.pprint(result["dump"])
        else:
            pprint.pprint(result)
            
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
    intf = ConfigurationClientInterface(user_mode=0)

    do_work(intf)
