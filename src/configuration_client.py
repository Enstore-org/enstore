#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

# system imports
import sys
#import time
import errno
import pprint
import os
import socket
import select
import types
import time

# enstore imports
import generic_client
import enstore_constants
import enstore_functions2
import option
#import udp_client
import Trace
import callback
import e_errors
import hostaddr
#import enstore_erc_functions
#import event_relay_client
#import event_relay_messages

MY_NAME = enstore_constants.CONFIGURATION_CLIENT         #"CONFIG_CLIENT"
MY_SERVER = enstore_constants.CONFIGURATION_SERVER

class ConfigFlag:

    MSG_YES = 1
    MSG_NO = 0
    DISABLE = 1
    ENABLE = 0

    def __init__(self):
	self.new_config_file = self.MSG_NO
        self.do_caching = self.DISABLE

    def is_caching_enabled(self):
        return not self.do_caching

    def new_config_msg(self):
        #if self.do_caching == self.ENABLE:
        self.new_config_file = self.MSG_YES

    def reset_new_config(self):
        #if self.do_caching == self.ENABLE:
        self.new_config_file = self.MSG_NO
        
    def have_new_config(self):
        if self.do_caching == self.DISABLE:
            return 1
	elif self.new_config_file == self.MSG_YES:
	    return 1
	else:
	    return 0

    def disable_caching(self):
        self.do_caching = self.DISABLE

    def enable_caching(self):
        self.do_caching = self.ENABLE

class ConfigurationClient(generic_client.GenericClient):

    def __init__(self, address=None):
        if address is None:
            address = (enstore_functions2.default_host(),
                       enstore_functions2.default_port())
	flags = enstore_constants.NO_CSC | enstore_constants.NO_ALARM | \
		enstore_constants.NO_LOG
	generic_client.GenericClient.__init__(self, (), MY_NAME,
                                              address, flags=flags,
                                              server_name = MY_SERVER)
        self.new_config_obj = ConfigFlag()
	self.saved_dict = {}
        self.have_complete_config = 0
        self.config_load_timestamp = None

    #Return these values when requested.
    def get_address(self):
        return self.server_address
    def get_timeout(self):
        return self.timeout
    def get_retry(self):
        return self.retry

    #This function is needed by clients that use dump_and_save() to determine
    # if the cached configuration is the current configuration loaded into
    # the configuration_server.  Server's that use dump_and_save() have
    # have_new_config() to determine this same information from event_relay
    # NEWCONFIGFILE messages.
    def is_config_current(self):
        if self.config_load_timestamp == None:
            return False

        result = self.config_load_time(5, 5)
        if e_errors.is_ok(result):
            if result['config_load_timestamp'] <= self.config_load_timestamp:
                return True

        return False

    #Return which key in the 'known_config_servers' configuration dictionary
    # entry refers to this client's server (if present).  If there is
    # not an entry (like a developers test system) then a value is returned
    # based on the configuration servers nodename.
    def get_enstore_system(self, timeout=0, retry=0):

        while 1:
            ret = self.get('known_config_servers', timeout, retry)

            if e_errors.is_ok(ret):
                break
            else:
                #Return None if no responce from the configuration
                # server was received.
                return None
        
        for item in ret.items():
            if socket.getfqdn(item[1][0]) == \
               socket.getfqdn(self.server_address[0]):
                return item[0]

        #If we make it here, then we did receive a resonce from the
        # configuration server, however we did not find the system this
        # is looking for in the list received.
        return socket.getfqdn(self.server_address[0]).split(".")[0]

    def do_lookup(self, key, timeout, retry):
        request = {'work' : 'lookup', 'lookup' : key, 'new' : 1}

        ret = self.send(request, timeout, retry)

        if e_errors.is_ok(ret):
            try:
                #New format.  This is requested by new configuration clients
                # by adding the "'new' : 1" to the request ticket above.
                self.saved_dict[key] = ret[key]
                ret_val = ret[key]
            except KeyError:
                #Old format.
                self.saved_dict[key] = ret
                ret_val = ret
            Trace.trace(23, "Get %s config info from server"%(key,))
        else:
            ret_val = ret

        #Keep the hostaddr allow() information up-to-date on all lookups.
        hostaddr.update_domains(ret.get('domains', {}))
        
	return ret_val

    # return value for requested item
    def get(self, key, timeout=0, retry=0):
        self.timeout = timeout #Remember this.
        self.retry = retry     #Remember this.
        if key == enstore_constants.CONFIGURATION_SERVER:
            ret = {'hostip':self.server_address[0],
                   'port':self.server_address[1],
                   'status':(e_errors.OK, None)}
        else:
	    # if we have a new_config_obj, then only go to the config server if we
	    # have received a message saying a new one was loaded.
	    if not self.new_config_obj or self.new_config_obj.have_new_config():
		# clear out the cached copies
		self.saved_dict = {}
                #The config cache was just clobbered.
                self.have_complete_config = 0
                self.config_load_timestamp = None
		ret = self.do_lookup(key, timeout, retry)
		if self.new_config_obj:
		    self.new_config_obj.reset_new_config()
	    else:
                # there was no new config loaded, just return what we have.
                # if we do not have a stashed copy, go get it.
		if self.saved_dict.has_key(key):
		    Trace.trace(23, "Returning %s config info from saved_dict"%(key,))
		    Trace.trace(23, "saved_dict - %s"%(self.saved_dict,))
		    ret = self.saved_dict[key]
		else:
		    ret = self.do_lookup(key, timeout, retry)

        ##HACK:
        #Do a hack for the monitor server.  Since, it runs on all enstore
        # machines we need to add this information before continuing.
        if e_errors.is_ok(ret) and key == enstore_constants.MONITOR_SERVER:
            ret['host'] = socket.gethostname()
            ret['hostip'] = socket.gethostbyname(ret['host'])
            ret['port'] = enstore_constants.MONITOR_PORT
        ##END HACK.

        ###Usefull for debugging.
        #if ret['status'][0] == e_errors.KEYERROR:
        #    import traceback
        #    Trace.log(e_errors.INFO, "Key %s requested from:" % (key,))
        #    # log it
        #    for l in traceback.format_stack():
        #        Trace.log(e_errors.INFO, l)
                
        return ret

    # dump the configuration dictionary
    def dump(self, timeout=0, retry=0):
        host, port, listen_socket = callback.get_callback()
        listen_socket.listen(4)
        
        request = {'work' : 'dump',
                   'callback_addr'  : (host,port)
                   }
        reply = self.send(request, timeout, retry)
        if not e_errors.is_ok(reply):
            print "ERROR",reply
            return reply
        r, w, x = select.select([listen_socket], [], [], 15)
        if not r:
            reply = {'status' : (e_errors.TIMEDOUT,
                         "timeout waiting for configuration server callback")}
            return reply
        control_socket, address = listen_socket.accept()
        hostaddr.update_domains(reply.get('domains', {})) #Hackish.
        if not hostaddr.allow(address):
            listen_socket.close()
            control_socket.close()
            reply = {'status' : (e_errors.EPROTO,
                                 "address %s not allowed" % (address,))}
            return reply

        try:
            d = callback.read_tcp_obj(control_socket)
        except e_errors.TCP_EXCEPTION:
            d = {'status':(e_errors.TCP_EXCEPTION, e_errors.TCP_EXCEPTION)}
        listen_socket.close()
        control_socket.close()
        return d

    # dump the configuration dictionary and save it too
    def dump_and_save(self, timeout=0, retry=0):
        if not self.new_config_obj or self.new_config_obj.have_new_config() \
           or not self.is_config_current():

            config_ticket = self.dump(timeout = timeout, retry = retry)
            if e_errors.is_ok(config_ticket):
                self.saved_dict = config_ticket['dump'].copy()
                self.saved_dict['status'] = (e_errors.OK, None)
                self.have_complete_config = 1
                self.config_load_timestamp = \
                               config_ticket.get('config_load_timestamp', None)
                if self.new_config_obj:
                    self.new_config_obj.reset_new_config()

                return self.saved_dict  #Success.
            
            return config_ticket  #An error occured.

        return self.saved_dict #Used cached dictionary.

    def config_load_time(self, timeout=0, retry=0):
        request = {'work' : 'config_timestamp' }
        x = self.send(request,  timeout,  retry )
        return x

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

    # multithreaded on / off
    def threaded(self, on = 0, timeout=0, retry=0):
        request = {'work' : 'thread_on' , 'on':on }
        x = self.send(request, timeout, retry)
        return x

    #def alive(self, server, rcv_timeout=0, tries=0):
    #    return self.send({'work':'alive'}, rcv_timeout, tries)

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
    def get_library_managers(self, timeout=0, retry=0):
        request = {'work': 'get_library_managers'}
        return self.send(request, timeout, retry)

    #get list of media changers
    def get_media_changers(self, timeout=0, retry=0):
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
        self.server=""
        self.alive_rcv_timeout = 0
        self.alive_retries = 0
        self.summary = 0
        self.timestamp = 0
        self.threaded_impl = None
        self.list_library_managers = 0
        self.list_media_changers = 0
        self.list_movers = 0
        
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
                     #Default label is used for switches that take an
                     # unknown number arguments from intf.args and not
                     # from the specification in this dictionary.
                     option.DEFAULT_LABEL:"[value_name [value_name [...]]]",
                     option.USER_LEVEL:option.ADMIN,
                     #option.VALUE_LABEL:"[value_name [value_name [...]]]",
                     #option.EXTRA_VALUES:[{
                     #    option.VALUE_NAME:"server",
                     #    option.VALUE_TYPE:option.STRING,
                     #    option.VALUE_USAGE:option.OPTIONAL,
                     #    option.DEFAULT_TYPE:None,
                     #    option.DEFAULT_VALUE:None
                     #    }]
                     },
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
        option.LIST_LIBRARY_MANAGERS:{option.HELP_STRING:
                                      "list all library managers in "
                                      "configuration",
                                      option.DEFAULT_VALUE:option.DEFAULT,
                                      option.DEFAULT_TYPE:option.INTEGER,
                                      option.VALUE_USAGE:option.IGNORED,
                                      option.USER_LEVEL:option.ADMIN},
        option.LIST_MEDIA_CHANGERS:{option.HELP_STRING:
                                    "list all media changers in "
                                    "configuration",
                                    option.DEFAULT_VALUE:option.DEFAULT,
                                    option.DEFAULT_TYPE:option.INTEGER,
                                    option.VALUE_USAGE:option.IGNORED,
                                    option.USER_LEVEL:option.ADMIN},
        option.LIST_MOVERS:{option.HELP_STRING:
                            "list all movers in configuration",
                            option.DEFAULT_VALUE:option.DEFAULT,
                            option.DEFAULT_TYPE:option.INTEGER,
                            option.VALUE_USAGE:option.IGNORED,
                            option.USER_LEVEL:option.ADMIN},
        option.TIMESTAMP:{option.HELP_STRING:
                          "last time configfile was reloaded",
                          option.DEFAULT_TYPE:option.INTEGER,
                          option.USER_LEVEL:option.ADMIN},
        option.THREADED_IMPL:{option.HELP_STRING:
                              "Turn on / off threaded implementation",
                              option.VALUE_USAGE:option.REQUIRED,
                              option.DEFAULT_TYPE:option.INTEGER,
                              option.USER_LEVEL:option.ADMIN},
         }

def do_work(intf):
    csc = ConfigurationClient((intf.config_host, intf.config_port))
    csc.csc = csc
    result = csc.handle_generic_commands(MY_SERVER, intf)
    if intf.alive:
        if result['status'] == (e_errors.OK, None):
            print "Server configuration found at %s." % (result['address'],)
    if result:
        pass
    elif intf.show:
        try:
            result = csc.dump(intf.alive_rcv_timeout,intf.alive_retries)
        except (KeyboardInterrupt, SystemExit):
            sys.exit(1)
        except (socket.error, select.error), msg:
            if msg.args[0] == errno.ETIMEDOUT:
                result = {'status' : (e_errors.TIMEDOUT, str(msg))}
            else:
                result = {'status' : (e_errors.NET_ERROR, str(msg))}
            
       
        #If there is an error it is printed out at the end of the function
        # in csc.check_ticket().  On success, work as normal. 
        if e_errors.is_ok(result):
            #Loop through what the user specified (if anything) and return
            # the desired result(s).
            use_result = result['dump']
            for item in intf.args:
                if type(use_result) == types.DictType:
                    try:
                        use_result = use_result[item]
                    except KeyError:
                        result['status'] = (e_errors.KEYERROR,
                            "Unable to find requested information (1).\n")
                        break
                else:
                    result['status'] = (e_errors.CONFLICT,
                            "Unable to find requested information (2).\n")
                    break
            else:
                #If there wasn't a problem finding the information, print it.
                if type(use_result) == types.StringType:
                    #Suppress the '' that pprint.pprint() wants to surround
                    # native strings.
                    print use_result
                else:
                    pprint.pprint(use_result)

    elif intf.load:
        result= csc.load(intf.config_file, intf.alive_rcv_timeout,
	                intf.alive_retries)

    elif intf.summary:
        result= csc.get_keys(intf.alive_rcv_timeout,intf.alive_retries)
        pprint.pprint(result['get_keys'])

    elif intf.timestamp:
        result = csc.config_load_time(intf.alive_rcv_timeout,
                                      intf.alive_retries)
        if e_errors.is_ok(result):
            print time.ctime(result['config_load_timestamp'])
    elif intf.threaded_impl != None:
        result = csc.threaded(intf.threaded_impl, intf.alive_rcv_timeout,
                                      intf.alive_retries)
        print result
    elif intf.list_library_managers:
        try:
            result = csc.get_library_managers(
                timeout = intf.alive_rcv_timeout, retry = intf.alive_retries)
        except (KeyboardInterrupt, SystemExit):
            sys.exit(1)
        except (socket.error, select.error), msg:
            if msg.args[0] == errno.ETIMEDOUT:
                result = {'status' : (e_errors.TIMEDOUT, str(msg))}
            else:
                result = {'status' : (e_errors.NET_ERROR, str(msg))}

        if result.get("status", None) == None or e_errors.is_ok(result):
            msg_spec = "%25s %15s"
            print msg_spec % ("media changer", "host")
            for lm_name in result.values():
                lm_info = csc.get(lm_name['name'])
                print msg_spec % (lm_name['name'], lm_info['host'])

    elif intf.list_media_changers:
        try:
            result = csc.get_media_changers(
                timeout = intf.alive_rcv_timeout, retry = intf.alive_retries)
        except (KeyboardInterrupt, SystemExit):
            sys.exit(1)
        except (socket.error, select.error), msg:
            if msg.args[0] == errno.ETIMEDOUT:
                result = {'status' : (e_errors.TIMEDOUT, str(msg))}
            else:
                result = {'status' : (e_errors.NET_ERROR, str(msg))}

        if result.get("status", None) == None or e_errors.is_ok(result):
            msg_spec = "%25s %15s %20s"
            print msg_spec % ("media changer", "host", "type")
            for mc_name in result.values():
                mc_info = csc.get(mc_name['name'])
                print msg_spec % (mc_name['name'], mc_info['host'],
                                  mc_info['type'])

    elif intf.list_movers:
        try:
            movers_list = csc.get_movers(None,
                timeout = intf.alive_rcv_timeout, retry = intf.alive_retries)
            result = {'status' : (e_errors.OK, None)}
        except (KeyboardInterrupt, SystemExit):
            sys.exit(1)
        except (socket.error, select.error), msg:
            if msg.args[0] == errno.ETIMEDOUT:
                result = {'status' : (e_errors.TIMEDOUT, str(msg))}
            else:
                result = {'status' : (e_errors.NET_ERROR, str(msg))}

        if type(movers_list) == types.ListType:
            msg_spec = "%15s %15s %9s %10s %15s"
            print msg_spec % ("mover", "host", "mc_device", "driver", "library")
            for mover_name in movers_list:
                mover_info = csc.get(mover_name['mover'])
                print msg_spec % (mover_name['mover'], mover_info['host'],
                                  mover_info['mc_device'], mover_info['driver'],
                                  mover_info['library'])
    
    else:
	intf.print_help()
        sys.exit(0)

    csc.check_ticket(result)

if __name__ == "__main__":
    Trace.init(MY_NAME)

    # fill in interface
    intf = ConfigurationClientInterface(user_mode=0)

    do_work(intf)
