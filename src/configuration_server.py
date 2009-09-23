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
import errno

# enstore imports
#import setpath
import dispatching_worker
import generic_server
import event_relay_client
import event_relay_messages
import enstore_constants
import enstore_functions2
import option
import Trace
import e_errors
import hostaddr
import callback

MY_NAME = enstore_constants.CONFIGURATION_SERVER   #"CONFIG_SERVER"

class ConfigurationDict:

    def __init__(self):
        self.serverlist = {}
        self.config_load_timestamp = None
        self.use_thread = 1
        self.system_name = None  #Cache return value from _get_system_name().
        self.cached_domains = None #Cache return value from _get_system_name().
        self.do_copies = 2 # 2=deepcopy, 1=copy, 0=object reference

        #To keep the code as clean as possible, only ConfigurationDict
        # functions use these locks.  ConfigurationServer class functions use
        # wrapper functions that access these locks in a thread safe manner.
        self.config_lock = threading.Lock()
        self.member_lock = threading.Lock()

    ####################################################################
    ### read_config(), verify_and_update_config() and load_config()
    ### are the three functions used to read in the configuration file.

    def read_config(self, configfile):
        self.configdict={}
        try:
            f = open(configfile,'r')
        except (OSError, IOError), msg:
            if msg.args[0] in [errno.ENOENT]:
                status = (e_errors.DOESNOTEXIST,
                      "Configuration Server: read_config %s: does not exist"
                          % (configfile,))
            else:
                status = (e_errors.OSERROR,
                          "Configuration Server: read_config %s: %s"
                          % (configfile, str(msg)))
            Trace.log( e_errors.ERROR, status[1] )
            return status
        code = string.join(f.readlines(),'')

        # Lint hack, otherwise lint can't see where configdict is defined.
        configdict = {}
        del configdict 
        configdict = {}

        try:
            exec(code)
            ##I would like to do this in a restricted namespace, but
            ##the dict uses modules like e_errors, which it does not import
        except:
            exc, msg, tb = sys.exc_info()
            fmt =  traceback.format_exception(exc, msg, tb)[2:]
            ##report the name of the config file in the traceback instead of
            ##"<string>"
            fmt[0] = string.replace(fmt[0], "<string>", configfile)
            message = "Configuration Server: "+string.join(fmt, "")
            Trace.log(e_errors.ERROR, message)
            os._exit(-1)
        # ok, we read entire file - now set it to real dictionary
        self.configdict = configdict
        return (e_errors.OK, None)

    def verify_and_update_config(self):
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
                            self.configdict[key]['port'] and
                            self.configdict[key]['port'] !=-1):
                            message = "Configuration Conflict detected "\
                                  "for hostip "+\
                                  repr(self.configdict[key]['hostip'])+ \
                                  "and port "+ \
                                  repr(self.configdict[key]['port'])
                            Trace.log(10, message)
                            conflict = 1
                            break
                    if not conflict:
                        self.serverlist[key] = (
                            self.configdict[key]['host'],
                            self.configdict[key]['hostip'],
                            self.configdict[key]['port'])
                    break

        if conflict:
            return(e_errors.CONFLICT, "Configuration conflict detected. "
                   "Check configuration file")

        

        return (e_errors.OK, None)

    # load the configuration dictionary - the default is a wormhole in pnfs
    def load_config(self, configfile):
        
        try:
            self.config_lock.acquire()
            
            #Since we are loading a new configuration file,
            # 'known_config_servers' could change.  Set, system_name to None
            # so the next call to _get_system_name() resets this value.
            self.system_name = None
            self.cached_domains = None
            
            status = self.read_config(configfile)
            if not e_errors.is_ok(status):
                self.config_lock.release()   #Avoid deadlocks!
                return status

            status = self.verify_and_update_config()
            if not e_errors.is_ok(status):
                self.config_lock.release()   #Avoid deadlocks!
                return status

            #We have successfully loaded the config file.
            self.config_load_timestamp = time.time()

            self.config_lock.release()   #Avoid deadlocks!
            return (e_errors.OK, None)

        # even if there is an error - respond to caller so he can process it
        except:
            exc, msg, tb = sys.exc_info()
            Trace.handle_error(exc, msg, tb)
            del tb  #Avoid resource leaks!
            self.config_lock.release()   #Avoid deadlocks!
            return (e_errors.UNKNOWN, (str((str(exc), str(msg)))))

    ####################################################################

    ## get_dict_entry(), get_server_list(), get_config_keys() and
    ## get_config_dict():
    ## These are internal functions that pull information out of the
    ## configuration in a thread safe manner.  All other functions should
    ## use these functions instead of accessing self.configdict directly.
        
    def get_dict_entry(self, skeyValue):
        copy_level = self.get_copy_level()
        self.config_lock.acquire()
        try:
            t0 = time.time()
            if copy_level >= 2:
                value = copy.deepcopy(self.configdict[skeyValue])
            elif copy_level == 1:
                value = copy.copy(self.configdict[skeyValue])
            else:
                value = self.configdict[skeyValue]
            Trace.trace(25, "get_dict_entry: extract time: %f" % (time.time() - t0,))
        except:
            self.config_lock.release()
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        self.config_lock.release()
        return value

    def get_config_keys(self):
        copy_level = self.get_copy_level()
        self.config_lock.acquire()
        try:
            t0 = time.time()
            if copy_level >= 2:
                key_list = copy.deepcopy(self.configdict.keys())
            elif copy_level == 1:
                key_list = copy.copy(self.configdict.keys())
            else:
                key_list = self.configdict.keys()
            Trace.trace(25, "get_config_keys: extract time: %f" % (time.time() - t0,))
        except:
            self.config_lock.release()
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        self.config_lock.release()
        return key_list

    def get_config_dict(self):
        copy_level = self.get_copy_level()
        self.config_lock.acquire()
        try:
            t0 = time.time()
            if copy_level >= 2:
                configdict = copy.deepcopy(self.configdict)
            elif copy_level == 1:
                configdict = copy.copy(self.configdict)
            else:
                configdict = self.configdict
            Trace.trace(25, "get_config_dict: extract time: %f" % (time.time() - t0,))
        except:
            self.config_lock.release()
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        self.config_lock.release()
        return configdict

    ## The following should use member_lock, but use config_lock to avoid
    ## need to lock two locks to safely update the data member.

    def get_server_list(self):
        copy_level = self.get_copy_level()
        self.config_lock.acquire()
        try:
            t0 = time.time()
            if copy_level >= 2:
                slist = copy.deepcopy(self.serverlist)
            elif copy_level == 1:
                slist = copy.copy(self.serverlist)
            else:
                slist = self.serverlist
            Trace.trace(25, "get_server_list: extract time: %f" % (time.time() - t0,))
        except:
            self.config_lock.release()
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        self.config_lock.release()
        return slist

    ## The following use member_lock instead of config_lock.
    
    def get_config_load_timestamp(self):
        copy_level = self.get_copy_level()
        self.member_lock.acquire()
        try:
            t0 = time.time()
            if copy_level >= 2:
                clt = copy.deepcopy(self.config_load_timestamp)
            elif copy_level == 1:
                clt = copy.copy(self.config_load_timestamp)
            else:
                clt = self.config_load_timestamp
            Trace.trace(25, "get_config_load_timestamp: extract time: %f" % (time.time() - t0,))
        except:
            self.member_lock.release()
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        self.member_lock.release()
        return clt

    def get_threaded_imp(self):
        copy_level = self.get_copy_level()
        self.member_lock.acquire()
        try:
            t0 = time.time()
            if copy_level >= 2:
                use_thread = copy.deepcopy(self.use_thread)
            elif copy_level == 1:
                use_thread = copy.copy(self.use_thread)
            else:
                use_thread = self.use_thread
            Trace.trace(25, "get_threaded_imp: extract time: %f" % (time.time() - t0,))
        except:
            self.member_lock.release()
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        self.member_lock.release()
        return use_thread

    def set_threaded_imp(self, new_value):
        self.member_lock.acquire()
        try:
            self.use_thread = new_value
        except:
            self.member_lock.release()
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        self.member_lock.release()
        return

    def get_copy_level(self):
        self.member_lock.acquire()
        try:
            t0 = time.time()
            if self.do_copies >= 2:
                copy_level = copy.deepcopy(self.do_copies)
            elif self.do_copies == 1:
                copy_level = copy.copy(self.do_copies)
            else:
                copy_level = self.do_copies
            Trace.trace(25, "get_copy_level: extract time: %f" % (time.time() - t0,))
        except:
            self.member_lock.release()
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        self.member_lock.release()
        return copy_level


    def set_copy_level(self, new_value):
        self.member_lock.acquire()
        try:
            self.do_copies = new_value
        except:
            self.member_lock.release()
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        self.member_lock.release()
        return

    ####################################################################
    
    def get_movers_internal(self, ticket):
        ret = []
	if ticket.has_key('library'):
	    # search for the appearance of this library manager
	    # in all configured movers
	    for srv in self.get_config_keys():
		if string.find (srv, ".mover") != -1:
		    item = self.get_dict_entry(srv)
                    if not ticket['library']:
                        #If no library was specified, return all movers.
                        mv = {'mover' : srv,
                              'address' : (item['hostip'], 
                                           item['port'])
                              }
                        ret.append(mv)
                        continue
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

    #This function returns the key in the 'known_config_servers' sub-ticket
    # that corresponds to this system.  If it is not there then a default
    # based on the system name is returned.
    #
    # system_name is a variable where this is cached during the
    # first time the function is called.
    #
    # This function should only be called from _get_domain(), where the
    # thread safe locking is done.
    def __get_system_name(self):
        if self.system_name == None:
            try:
                kcs = self.get_dict_entry('known_config_servers')
            except:
                kcs = {}
            server_address = os.environ.get('ENSTORE_CONFIG_HOST', "default2")

            for item in kcs.items():
                if socket.getfqdn(item[1][0]) == \
                       socket.getfqdn(server_address):
                    return item[0]

            self.system_name = socket.getfqdn(server_address).split(".")[0]

        return self.system_name

    #Return the domains information in the configuration dictionary.
    # Append to the valid_domains 
    def _get_domains(self):

        self.member_lock.acquire()
        if getattr(self, "cached_domains", None) == None:
            should_set_now = True
        else:
            should_set_now = False
        #Release member_lock to prevent another thread from locking
        # config_lock and allowing for the possibility of a deadlock.
        self.member_lock.release()
            
        if should_set_now:
            t0 = time.time()
            try:
                domains = self.get_dict_entry('domains')
            except:
                domains = None # Some error.
                
            if domains != None:
                cached_domains = {}
                #Put the domains into the reply ticket.
                cached_domains['domains'] = domains
                #We need to insert the configuration servers domain into
                # the list.  Otherwise, the client will not have the
                # configuration server's domain in the valid_domains list.
                try:
                    cached_domains['domains']['valid_domains'].append(hostaddr.getdomainaddr())
                except KeyError:
                    pass

                #Don't call __get_system_name() while member_lock is locked.
                # It calls get_dict_entry() which locks config_lock and
                # could cause a deadlock between threads.
                cached_domains['domains']['system_name'] = self.__get_system_name()
                
                self.member_lock.acquire()
                self.cached_domains = cached_domains
                self.member_lock.release()
                
            Trace.trace(25, "_get_domains: extract time: %f" % (time.time() - t0,))

        return self.cached_domains

class ConfigurationServer(ConfigurationDict, dispatching_worker.DispatchingWorker,
			  generic_server.GenericServer):

    def __init__(self, server_address, configfile = None):
        
        # make a configuration dictionary
        ConfigurationDict.__init__(self)
        # load the config file user requested
        if configfile == None:
            configfile = enstore_functions2.default_file()
        self.load_config(configfile)

        # The other servers call the generic_server.__init__() function
        # to get hostaddr.update_domains() called.  The configuration server
        # does not use the generic_server __init__() function, so this must be
        # done expliticly (after the configfile is loaded).
        self.update_domains()
        
        # default socket initialization - ConfigurationDict handles requests
        dispatching_worker.DispatchingWorker.__init__(self, server_address)
        self.request_dict_ttl = 10 # Config server is stateless,
                                   # duplicate requests don't hurt us.
        
	# set up for sending an event relay message whenever we get a
        # new config loaded
	self.new_config_message = event_relay_messages.EventRelayNewConfigFileMsg(
            server_address[0],
            server_address[1])
	self.new_config_message.encode()

	# start our heartbeat to the event relay process
	self.erc = event_relay_client.EventRelayClient(self)
	self.erc.start_heartbeat(enstore_constants.CONFIG_SERVER, 
				 enstore_constants.CONFIG_SERVER_ALIVE_INTERVAL)

    #Called whenever a new configuration is loaded.
    def update_domains(self):
        # The other servers call the generic_server.__init__() function
        # to get hostaddr.update_domains function called.  The configuration
        # server does not use the generic_server.__init__() function, so
        # this must be done expliticly (after the configfile is loaded).
        domains = self._get_domains()
        if domains == None:
            domains = {}
        hostaddr.update_domains(domains)

    # Overridden dispatching_worker function.  This allows us to control
    # which functions are started in parallel or not.
    def invoke_function(self, function, args=(), after_function = None):
        if function.func_name in ['dump', 'dump2']:
            if self.get_threaded_imp():
                self.run_in_thread(None, function, args, after_function)
            else:
                self.run_in_process(None, function, args, after_function)
        else:
            dispatching_worker.DispatchingWorker.invoke_function(
                self, function, args)
        
    ####################################################################
        
    # just return the current value for the item the user wants to know about
    def lookup(self, ticket):
        # everything is based on lookup - make sure we have this
        try:
            key="lookup"
            lookup = ticket[key]
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR,
                                "Configuration Server: "+key+" key is missing")
            self.reply_to_caller(ticket)
            return

        # look up in our dictionary the lookup key
        try:
            if ticket.get('new', None):
                ticket[lookup] = self.get_dict_entry(lookup)

                #The following section places into the udp reply ticket
                # information to prevent the configuration_client from having
                # to pull it down seperatly.
                domains = self._get_domains()['domains']
                if domains != None:
                    ticket['domains'] = domains
            else:
                #For backward compatibility.
                ticket = self.get_dict_entry(lookup)
            ticket['status'] = (e_errors.OK, None)
        except KeyError:
            ticket = {"status": (e_errors.KEYERROR,
                                     "Configuration Server: no such name: "
                                     +repr(lookup))}

        self.send_reply(ticket)


    # return a list of the dictionary keys back to the user
    def get_keys(self, ticket):
        __pychecker__ = "unusednames=ticket"

        skeys = self.get_config_keys()
        skeys.sort()
        ticket['get_keys'] = (skeys)
        ticket['status'] = (e_errors.OK, None)
        self.send_reply(ticket)


    # return a dump of the dictionary back to the user
    def __make_dump(self, ticket):
        Trace.trace(15, 'DUMP: \n' + str(ticket))

        ticket['status'] = (e_errors.OK, None)

        #reply=ticket.copy()
        ticket['dump'] = self.get_config_dict()
        #The following section places into the udp reply ticket information
        # to prevent the configuration_client from having to pull it
        # down seperatly.
        ticket['config_load_timestamp'] = self.get_config_load_timestamp()
        domains = self._get_domains()['domains']
        if domains != None:
            ticket['domains'] = domains
        
        return ticket

    def dump(self, ticket):
        if not hostaddr.allow(ticket['callback_addr']):
            return None

        reply = self.__make_dump(ticket)
        if reply == None:
            return
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

    def dump2(self, ticket):
        reply = self.__make_dump(ticket)
        if reply == None:
            return
        self.send_reply_with_long_answer(reply)
            
    # reload the configuration dictionary, possibly from a new file
    def load(self, ticket):
	try:
            configfile = ticket['configfile']
            ticket['status'] = self.load_config(configfile)
        except KeyError:
            ticket['status'] = (e_errors.KEYERROR,
                                "Configuration Server: no such name")
	except:
            exc, msg = sys.exc_info()[:2]
            ticket['status'] = (e_errors.UNKNOWN, str(((str(exc), str(msg)))))

        try:
            self.update_domains()
        except:
            exc, msg = sys.exc_info()[:2]
            ticket['status'] = (e_errors.UNKNOWN, str(((str(exc), str(msg)))))

	# even if there is an error - respond to caller so he can process it
        self.reply_to_caller(ticket)

        if e_errors.is_ok(ticket['status']):
            # send an event relay message
            self.erc.send(self.new_config_message)
            # Record in the log file the successfull reload.
            Trace.log(e_errors.INFO, "Configuration reloaded.")
        else:
            Trace.log(e_errors.ERROR, "Configuration reload failed: %s" %
                      (ticket['status'],))

    def config_timestamp(self, ticket):
        ticket['config_load_timestamp'] = self.get_config_load_timestamp()
        ticket['status'] = (e_errors.OK, None)
        self.reply_to_caller(ticket)

    # get list of the Library manager movers
    ## XXX this function is misleadingly named - it gives movers for a
    ## particular library as specified in ticket['library']
    def get_movers(self, ticket):
	ret = self.get_movers_internal(ticket)
	self.reply_to_caller(ret)

    def get_media_changer(self, ticket):
        #__pychecker__ = "unusednames=ticket"
        
        movers = self.get_movers_internal(ticket)
        ##print "get_movers_internal %s returns %s" % (ticket, movers)
        ret = ''
        for m in movers:
            mv_name = m['mover']
            ret =  self.get_dict_entry(mv_name).get('media_changer','')
            if ret:
                break
        self.reply_to_caller(ret)
        
    #get list of library managers
    ### Not thread safe.  The ticket['r_a'] value isn't passed to
    ### reply_to_caller() via ret, so the reply the client asked for may
    ### not be what they get.
    def get_library_managers(self, ticket):
        __pychecker__ = "unusednames=ticket"
        
        ret = {}
        for key in self.get_config_keys():
            index = string.find (key, ".library_manager")
            if index != -1:
                library_name = key[:index]
                item = self.get_dict_entry(key)
                ret[library_name] = {'address':(item['host'],item['port']),
				     'name': key}
        self.reply_to_caller(ret)

    #get list of media changers
    ### Not thread safe.  The ticket['r_a'] value isn't passed to
    ### reply_to_caller() via ret, so the reply the client asked for may
    ### not be what they get.
    def get_media_changers(self, ticket):
        __pychecker__ = "unusednames=ticket"
        
        ret = {}
        for key in self.get_config_keys():
            index = string.find (key, ".media_changer")
            if index != -1:
                media_changer_name = key[:index]
                item = self.get_dict_entry(key)
                ret[media_changer_name] = {'address':(item['host'],
                                                      item['port']),
                                           'name': key}
        self.reply_to_caller(ret)

    def reply_serverlist( self, ticket):
        __pychecker__ = "unusednames=ticket"

        try:
            ticket['server_list'] = self.get_server_list()
            ticket['status'] = (e_errors.OK, None)
        except:
            ticket['status'] = (e_errors.UNKNOWN, str(sys.exc_info()[1]))
        
        self.reply_to_caller(ticket)
 
    def get_dict_element(self, ticket):
        try:
            ticket['value'] = self.get_dict_entry(ticket['keyValue'])
            ticket['status'] = (e_errors.OK, None)
        except:
            ticket['status'] = (e_errors.UNKNOWN, str(sys.exc_info()[1]))
        self.reply_to_caller(ticket)
    
    # turn on / off threaded implementation
    def thread_on(self, ticket):
        key = ticket.get('on', 0)
        if key:
            key=int(key)
            if key != 0:
                key = 1
        self.set_threaded_imp(key)
        ret = {"status" : (e_errors.OK,
                           "thread is set to %s" % (self.get_thread_imp()))}
        self.reply_to_caller(ret)

    # change the copy level: 2 = deepcopy, 1 = copy, 0 = direct reference
    def copy_level(self, ticket):
        key = ticket.get('copy_level', 2)
        if key:
            key=int(key)
            if key >= 2:
                key = 2
            elif key <= 0:
                key = 0
        self.set_copy_level(key)
        ret = {"status" : (e_errors.OK,
                           "copy level set to %s" % (self.get_copy_level()))}
        self.reply_to_caller(ret)
        


        

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
    ## By using "" instead of intf.config_host, we will allow the
    ## configuration server to respond to any request that arrives on
    ## any configured interface on the system.  This gives more flexibility
    ## if the ENSTORE_CONFIG_HOST value resovles to a different IP than the
    ## pysical IP on the same machine.
    cs = ConfigurationServer(("", intf.config_port),
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
