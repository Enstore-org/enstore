#!/usr/bin/env python
###############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
import sys
import string
import types
import os
import traceback
import socket

# enstore imports
import setpath

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

MY_NAME = "CONFIG_SERVER"

class ConfigurationDict:

    def __init__(self):
        self.print_id="CONFIG_DICT"
        self.serverlist = {}
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
            return (e_errors.OK, None)

        # even if there is an error - respond to caller so he can process it
        except:
            exc,msg,tb=sys.exc_info()
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
        try:
            out_ticket = self.configdict[lookup]
        except KeyError:
            out_ticket = {"status": (e_errors.KEYERROR,
                                     "Configuration Server: no such name: "
                                     +repr(lookup))}
        self.reply_to_caller(out_ticket)


    # return a list of the dictionary keys back to the user
    def get_keys(self, ticket):

        skeys = self.configdict.keys()
        skeys.sort()
        out_ticket = {"status" : (e_errors.OK, None), "get_keys" : (skeys)}
        self.reply_to_caller(out_ticket)


    # return a dump of the dictionary back to the user
    def dump(self, ticket):
        Trace.trace(15, 'DUMP', ticket)
        ticket['status']=(e_errors.OK, None)
        reply=ticket.copy()
        reply["dump"] = self.configdict
        self.reply_to_caller(ticket)
        addr = ticket['callback_addr']
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(addr)
        callback.write_tcp_obj(sock,reply)
        sock.close()


    # reload the configuration dictionary, possibly from a new file
    def load(self, ticket):
	try:
	    try:
		configfile = ticket["configfile"]
		out_ticket = {"status" : self.load_config(configfile)}
		if out_ticket["status"] == (e_errors.OK, None):
		    # send an event relay message 
		    self.erc.send(self.new_config_message)
	    except KeyError:
		out_ticket = {"status" : (e_errors.KEYERROR, "Configuration Server: no such name")}
	    self.reply_to_caller(out_ticket)


	# even if there is an error - respond to caller so he can process it
	except:
            exc,msg,tb=sys.exc_info()
	    ticket["status"] = str(exc),str(msg)
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


class ConfigurationServer(ConfigurationDict, dispatching_worker.DispatchingWorker,
			  generic_server.GenericServer):

    def __init__(self, csc, configfile=enstore_constants.DEFAULT_CONF_FILE):
	self.running = 0
	self.print_id = MY_NAME
        print csc
        # make a configuration dictionary
        cd =  ConfigurationDict()

        # default socket initialization - ConfigurationDict handles requests
        dispatching_worker.DispatchingWorker.__init__(self, csc)
        self.request_dict_ttl = 10 # Config server is stateless, duplicate requests don't hurt us
        # load the config file user requested
        self.load_config(configfile)
        self.running = 1

	# set up for sending an event relay message whenever we get a new config loaded
	self.new_config_message = event_relay_messages.EventRelayNewConfigFileMsg(csc[0],
										  csc[1])
	self.new_config_message.encode()

	# start our heartbeat to the event relay process
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
