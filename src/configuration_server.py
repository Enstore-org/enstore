#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

# system imports
import sys
import string
import types
import os
import traceback
import socket
import time
import copy
import threading

# enstore imports
#import setpath
import dispatching_worker
import generic_server
import event_relay_client
import event_relay_messages
import enstore_constants
import option
import Trace
import e_errors
import hostaddr
import callback

MY_NAME = enstore_constants.CONFIGURATION_SERVER   #"CONFIG_SERVER"

class ConfigurationDict:

    def __init__(self):
        #self.print_id="CONFIG_DICT"
        self.serverlist = {}
        self.config_load_timestamp = None
        self.use_thread = 1

    def read_config(self, configfile):
        self.configdict={}
        try:
            f = open(configfile,'r')
        except:
            msg = (e_errors.DOESNOTEXIST,"Configuration Server: read_config %s: does not exist"%
                   (configfile,))
            Trace.log( e_errors.ERROR, msg[1] )
            return msg
        code = string.join(f.readlines(),'')

        configdict={};
        del configdict # Lint hack, otherwise lint can't see where configdict is defined.
        try:
            exec(code)
            ##I would like to do this in a restricted namespace, but
            ##the dict uses modules like e_errors, which it does not import
        except:
            exc,msg,tb = sys.exc_info()
            fmt =  traceback.format_exception(exc,msg,tb)[2:]
            ##report the name of the config file in the traceback instead of "<string>"
            fmt[0] = string.replace(fmt[0], "<string>", configfile)
            msg = "Configuration Server: "+string.join(fmt, "")
            Trace.log(e_errors.ERROR,msg)
            print msg
            os._exit(-1)
        # ok, we read entire file - now set it to real dictionary
        self.configdict=configdict
        return (e_errors.OK, None)

    # load the configuration dictionary - the default is a wormhole in pnfs
    def load_config(self, configfile):
        try:
            msg = self.read_config(configfile)
            if msg != (e_errors.OK, None):
                return msg
            self.serverlist={}
            conflict = 0
            for key in self.configdict.keys():
                if not self.configdict[key].has_key('status'):
                    self.configdict[key]['status'] = (e_errors.OK, None)
                for insidekey in self.configdict[key].keys():
                    if insidekey == 'host':
                        if not self.configdict[key].has_key('hostip'):
                            self.configdict[key]['hostip'] = \
                                hostaddr.name_to_address(
                                self.configdict[key]['host'])
                        if not self.configdict[key].has_key('port'):
                            self.configdict[key]['port'] = -1
                        # check if server is already configured
                        for configured_key in self.serverlist.keys():
                            if (self.serverlist[configured_key][1] == 
                                self.configdict[key]['hostip'] and 
                                self.serverlist[configured_key][2] == 
                                self.configdict[key]['port']):
                                msg = "Configuration Conflict detected for "\
                                      "hostip "+\
                                      repr(self.configdict[key]['hostip'])+ \
                                      "and port "+ \
                                      repr(self.configdict[key]['port'])
                                Trace.log(10, msg)
                                conflict = 1
                                break
                        if not conflict:
                            self.serverlist[key]= (self.configdict[key]['host'],self.configdict[key]['hostip'],self.configdict[key]['port'])
                        break

            if conflict:
                return(e_errors.CONFLICT, "Configuration conflict detected. "
                       "Check configuration file")

            # The other servers call the generic_server __init__ function
            # to get this function called.  The configuration server does
            # not use the generic_server __init__() function, so this must be
            # done expliticly (after the configfile is loaded).
            domains = self.configdict.get('domains', {})
            domains['system_name'] = self._get_system_name()
            hostaddr.update_domains(domains)

            #We have successfully loaded the config file.
            self.config_load_timestamp = time.time()
            return (e_errors.OK, None)

        # even if there is an error - respond to caller so he can process it
        except:
            exc,msg=sys.exc_info()[:2]
            return str(exc), str(msg)


    # just return the current value for the item the user wants to know about
    def lookup(self, ticket):
        # everything is based on lookup - make sure we have this
        try:
            key="lookup"
            lookup = ticket[key]
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, "Configuration Server: "+key+" key is missing")
            self.reply_to_caller(ticket)
            return

        # look up in our dictionary the lookup key
        out_ticket = {}
        try:
            if ticket.get('new', None):
                out_ticket[lookup] = self.configdict[lookup]
            else:
                #For backward compatibility.
                out_ticket = self.configdict[lookup]
            out_ticket['status'] = (e_errors.OK, None)
        except KeyError:
            out_ticket = {"status": (e_errors.KEYERROR,
                                     "Configuration Server: no such name: "
                                     +repr(lookup))}
        #The following section places into the udp reply ticket information
        # to prevent the configuration_client from having to pull it
        # down seperatly.
        if ticket.get('new', None):
            domains = self.configdict.get('domains', {})
            if domains:
                #Put the domains into the reply ticket.
                out_ticket['domains'] = copy.deepcopy(domains)
                #We need to insert the configuration servers domain into the
                # list.  Otherwise, the client will not have the configuration
                # server's domain in the valid_domains list.
                out_ticket['domains']['valid_domains'].append(hostaddr.getdomainaddr())
                out_ticket['domains']['system_name'] = self._get_system_name()

        self.reply_to_caller(out_ticket)


    # return a list of the dictionary keys back to the user
    def get_keys(self, ticket):

        skeys = self.configdict.keys()
        skeys.sort()
        out_ticket = {"status" : (e_errors.OK, None), "get_keys" : (skeys)}
        self.reply_to_caller(out_ticket)

    # run in thread
    def run_in_thread(self, thread_name, function, args=(), after_function=None):
        ##threads = threading.enumerate()
        ##for th in threads:
        ##    if th.isAlive():
        ##        thread_name = th.getName()
        ##        Trace.log(e_errors.INFO,"LOG: Thread %s is running" % (thread_name,))
        ##    else:
        ##        Trace.log(e_errors.INFO,"LOG: Thread %s is dead" % (thread_name,))

        if after_function:
            args = args + (after_function,)
        #Trace.log(e_errors.INFO, "create thread: target %s name %s args %s" % (function, thread_name, args))
        thread = threading.Thread(group=None, target=function,
                                  name=thread_name, args=args, kwargs={})
        #setattr(self, thread_name, thread)
        #Trace.log(e_errors.INFO, "starting thread %s"%(dir(thread,)))
        try:
            thread.start()
        except:
            exc, detail, tb = sys.exc_info()
            Trace.log(e_errors.ERROR, "starting thread %s: %s" % (thread_name, detail))
        return 0

    def dump(self, ticket):
        if self.use_thread:
            t = copy.deepcopy(ticket)
            self.run_in_thread('dump', self.make_dump,  args=(t,))
        else:
            self.make_dump(ticket)
        return
        

    # return a dump of the dictionary back to the user
    def make_dump(self, ticket):
        Trace.trace(15, 'DUMP: \n' + str(ticket))

        if not hostaddr.allow(ticket['callback_addr']):
            return

        ticket['status'] = (e_errors.OK, None)
        #The following section places into the udp reply ticket information
        # to prevent the configuration_client from having to pull it
        # down seperatly.
        domains = self.configdict.get('domains', {})
        if domains:
            #Put the domains into the reply ticket.
            ticket['domains'] = copy.deepcopy(domains)
            #We need to insert the configuration servers domain into the
            # list.  Otherwise, the client will not have the configuration
            # server's domain in the valid_domains list.
            ticket['domains']['valid_domains'].append(hostaddr.getdomainaddr())
            ticket['domains']['system_name'] = self._get_system_name()
            
        reply=ticket.copy()
        reply["dump"] = self.configdict
        reply["config_load_timestamp"] = self.config_load_timestamp
        self.reply_to_caller(ticket)
        addr = ticket['callback_addr']
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect(addr)
            r = callback.write_tcp_obj(sock,reply)
            sock.close()
            if r:
               Trace.log(e_errors.ERROR,"Error calling write_tcp_obj. Callback addr. %s"%(addr,))
            
        except:
            Trace.handle_error()
            Trace.log(e_errors.ERROR,"Callback address %s"%(addr,)) 
        return
            


    # reload the configuration dictionary, possibly from a new file
    def load(self, ticket):
        out_ticket = {"status":(e_errors.OK, None)}
	try:
            configfile = ticket["configfile"]
            out_ticket = {"status" : self.load_config(configfile)}
        except KeyError:
            out_ticket = {"status" : (e_errors.KEYERROR,
                                      "Configuration Server: no such name")}
	except:
            exc,msg=sys.exc_info()[:2]
            out_ticket = {"status" : (str(exc), str(msg))}

	# even if there is an error - respond to caller so he can process it
        self.reply_to_caller(out_ticket)

        if out_ticket["status"] == (e_errors.OK, None):
            # send an event relay message 
            self.erc.send(self.new_config_message)
            # Record in the log file the successfull reload.
            Trace.log(e_errors.INFO, "Configuration reloaded.")
        else:
            Trace.log(e_errors.ERROR, "Configuration reload failed: %s" %
                      (out_ticket['status'],))

    def config_timestamp(self, ticket):
        ticket['config_load_timestamp'] = self.config_load_timestamp
        ticket['status'] = (e_errors.OK, None)
        self.reply_to_caller(ticket)

    # get list of the Library manager movers
    ## XXX this function is misleadingly named - it gives movers for a particular library
    ## as specified in ticket['library']
    def get_movers(self, ticket):
	ret = self.get_movers_internal(ticket)
	self.reply_to_caller(ret)

    def get_movers_internal(self, ticket):
        ret = []
	if ticket.has_key('library'):
	    # search for the appearance of this library manager
	    # in all configured movers
	    for srv in self.configdict.keys():
		if string.find (srv, ".mover") != -1:
		    item = self.configdict[srv]
                    for key in ('library', 'libraries'):
                        if item.has_key(key):
                            if type(item[key]) == types.ListType:
                                for i in item[key]:
                                    if i == ticket['library']:
                                        mv = {'mover' : srv,
                                              'address' : (item['hostip'], 
                                                          item['port'])
                                              }
                                        ret.append(mv)
                            else:
                                if item[key] == ticket['library']:
                                    mv = {'mover' : srv,
                                          'address' : (item['hostip'], 
                                                       item['port'])
                                          }
                                    ret.append(mv)
        return ret

    def get_media_changer(self, ticket):
        movers = self.get_movers_internal(ticket)
        ##print "get_movers_internal %s returns %s" % (ticket, movers)
        ret = ''
        for m in movers:
            mv_name = m['mover']
            ret =  self.configdict[mv_name].get('media_changer','')
            if ret:
                break
        self.reply_to_caller(ret)
        
    #get list of library managers
    def get_library_managers(self, ticket):
        ret = {}
        for key in self.configdict.keys():
            index = string.find (key, ".library_manager")
            if index != -1:
                library_name = key[:index]
                item = self.configdict[key]
                ret[library_name] = {'address':(item['host'],item['port']),
				     'name': key}
        self.reply_to_caller(ret)

    def reply_serverlist( self, ticket ):
        out_ticket = {"status" : (e_errors.OK, None), 
                      "server_list" : self.serverlist }
        self.reply_to_caller(out_ticket)
 
        
    def get_dict_entry(self, skeyValue):
        slist = []
        for key in self.configdict.keys():
            if skeyValue in self.configdict[key].items():
                slist.append(key)
        return slist

    def get_dict_element(self, ticket):
        ret = {"status" : (e_errors.OK, None)}
        ret['servers'] = self.get_dict_entry(ticket['keyValue'])
        self.reply_to_caller(ret)

    #This function returns the key in the 'known_config_servers' sub-ticket
    # that corresponds to this system.  If it is not there then a default
    # based on the system name is returned.
    def _get_system_name(self):
        kcs = self.configdict.get('known_config_servers', {})
        server_address = os.environ.get('ENSTORE_CONFIG_HOST', "default2")

        for item in kcs.items():
            if socket.getfqdn(item[1][0]) == \
                   socket.getfqdn(server_address):
                return item[0]
        
        return socket.getfqdn(server_address).split(".")[0]

    # turn on / off threaded implementation
    def thread_on(self, ticket):
        key = ticket.get('on', 0)
        if key:
            key=int(key)
            if key != 0:
                key = 1
        self.use_thread = key
        ret = {"status" : (e_errors.OK, "thread is set to %s"%(self.use_thread))}
        self.reply_to_caller(ret)
        
        

class ConfigurationServer(ConfigurationDict, dispatching_worker.DispatchingWorker,
			  generic_server.GenericServer):

    def __init__(self, server_address,
                 configfile = enstore_constants.DEFAULT_CONF_FILE):
	self.running = 0
	#self.print_id = MY_NAME
        
        # make a configuration dictionary
        #cd = ConfigurationDict()
        ConfigurationDict.__init__(self)
        # default socket initialization - ConfigurationDict handles requests
        dispatching_worker.DispatchingWorker.__init__(self, server_address)
        self.request_dict_ttl = 10 # Config server is stateless,
                                   # duplicate requests don't hurt us.
        # load the config file user requested
        self.load_config(configfile)
        self.running = 1

	# set up for sending an event relay message whenever we get a
        # new config loaded
	self.new_config_message = event_relay_messages.EventRelayNewConfigFileMsg(
            server_address[0],
            server_address[1])
	self.new_config_message.encode()

	# start our heartbeat to the event reyeslay process
	self.erc = event_relay_client.EventRelayClient(self)
	self.erc.start_heartbeat(enstore_constants.CONFIG_SERVER, 
				 enstore_constants.CONFIG_SERVER_ALIVE_INTERVAL)

        

class ConfigurationServerInterface(generic_server.GenericServerInterface):

    def __init__(self):
        # fill in the defaults for possible options
        self.config_file = ""
        generic_server.GenericServerInterface.__init__(self)
        #self.parse_options()

    def valid_dictionaries(self):
        return generic_server.GenericServerInterface.valid_dictionaries(self) \
               + (self.configuration_options,)
    
    configuration_options = {
        option.CONFIG_FILE:{option.HELP_STRING:"specify the config file",
                            option.VALUE_TYPE:option.STRING,
                            option.VALUE_USAGE:option.REQUIRED,
                            option.USER_LEVEL:option.ADMIN,
                            }
        }


if __name__ == "__main__":
    Trace.init(MY_NAME)

    # get the interface
    intf = ConfigurationServerInterface()

    # get a configuration server
    cs = ConfigurationServer((intf.config_host, intf.config_port),
	                     intf.config_file)
    cs.handle_generic_commands(intf)
    # bomb out if we can't find the file
    statinfo = os.stat(intf.config_file)
    

    while 1:
        try:
            Trace.log(e_errors.INFO,"Configuration Server (re)starting")
            cs.serve_forever()
	except SystemExit, exit_code:
	    sys.exit(exit_code)
        except:
	    cs.serve_forever_error(MY_NAME)
            continue

    Trace.log(e_errors.INFO,"Configuration Server finished (impossible)")
