###############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
import sys
import string
import types
import socket
import os

# enstore imports
import dispatching_worker
import generic_server
import interface
import Trace
import e_errors
import traceback

MY_NAME = "CONFIG_SERVER"

class ConfigurationDict(dispatching_worker.DispatchingWorker):

    def __init__(self):
	self.print_id="CONFIG_DICT"
        self.serverlist = {}
    def read_config(self, configfile):
        try:
            f = open(configfile,'r')
        except:
            msg = (e_errors.DOESNOTEXIST,"Configuration Server: read_config"
                   +str(configfile)+" does not exist")
            Trace.log( e_errors.ERROR, msg[1] )
            return msg
        code = string.join(f.readlines(),'')
        Trace.trace(9, "Configuration Server read_config: "
                     "loading enstore configuration from %s"%configfile)
        try:
            exec(code) #would like to do this in a restricted namespace, but
                       ##the dict uses modules like e_errors, which it does not import
        except:
            x = sys.exc_info()
            tb=x[2]
            msg="YO"

            fmt =  traceback.format_exception(x[0],x[1],x[2])[-4:]
            fmt[0] = string.replace(fmt[0], "<string>", configfile)

            msg = (e_errors.ERROR, "Configuration Server: "+
                   string.join(fmt, ""))
            print msg[1]
#            Trace.trace(msg[0],msg[1])
#            print msg[1]

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
		    self.configdict[key]['hostip'] = socket.gethostbyname(self.configdict[key]['host'])
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
         return (str(sys.exc_info()[0]), str(sys.exc_info()[1]))


    # just return the current value for the item the user wants to know about
    def lookup(self, ticket):
     try:
        # everything is based on lookup - make sure we have this
        try:
            key="lookup"
            lookup = ticket[key]
        except KeyError:
            Trace.trace(6,"lookup "+repr(key)+" key is missing")
            ticket["status"] = (e_errors.KEYERROR, "Configuration Server: "+key+" key is missing")
            self.reply_to_caller(ticket)
            return

        # look up in our dictionary the lookup key
        try:
            out_ticket = self.configdict[lookup]
        except KeyError:
            Trace.trace(8,"lookup no such name"+repr(lookup))
            out_ticket = {"status": (e_errors.KEYERROR,
                                     "Configuration Server: no such name: "
                                     +repr(lookup))}
        self.reply_to_caller(out_ticket)
        Trace.trace(6,"lookup "+repr(lookup)+"="+repr(out_ticket))
        return

     # even if there is an error - respond to caller so he can process it
     except:
         ticket["status"] = (str(sys.exc_info()[0]), str(sys.exc_info()[1]))
         self.reply_to_caller(ticket)
         Trace.trace(6,"lookup "+str(sys.exc_info()[0])+
                     str(sys.exc_info()[1]))
         return

    # return a dump of the dictionary back to the user
    def get_keys(self, ticket):
     try:
        skeys = self.configdict.keys()
	skeys.sort()
        out_ticket = {"status" : (e_errors.OK, None), "get_keys" : (skeys)}
        self.reply_to_caller(out_ticket)
        return

     # even if there is an error - respond to caller so he can process it
     except:
         ticket["status"] = str(sys.exc_info()[0])+str(sys.exc_info()[1])
         self.reply_to_caller(ticket)
         Trace.trace(6,"get_keys "+str(sys.exc_info()[0])+
                     str(sys.exc_info()[1]))
         return


    # return a dump of the dictionary back to the user
    def dump(self, ticket):
     try:
        sortedkey = self.configdict.keys()
        sortedkey.sort()
        formatted= "configdict = {}\n"
        for key in sortedkey:
           formatted= formatted + "\nconfigdict['" + key + "'] = {"
           len2 = len(key)
           count4 = 0
           for key2 in self.configdict[key].keys():
              if key2 not in ('hostip', 'status'):
                  count4 = count4+1
           count3 = 0
           sortedkeyinside = self.configdict[key].keys()
           sortedkeyinside.sort()
           for key2 in sortedkeyinside:
              if key2 in ('hostip','status'):
                  continue
              count3 = count3 + 1
              if count3 != 1:
                 formatted= formatted + "\n"
                 #for ks in range(len2):
                 #   formatted= formatted + " "
                 formatted= formatted + " "*len2
                 formatted= formatted + "                   '" + key2 + "'  : " + repr(self.configdict[key][key2])
              else:
                 formatted= formatted + " '"  + key2 + "'  : " + repr(self.configdict[key][key2])
              if count3 != count4:
                 formatted= formatted + ","
              else:
                 formatted= formatted + " }\n"
        out_ticket = {"status" : (e_errors.OK, None), "dump" : formatted}
        self.reply_to_caller(out_ticket)
        return

     # even if there is an error - respond to caller so he can process it
     except:
         ticket["status"] = str(sys.exc_info()[0])+str(sys.exc_info()[1])
         self.reply_to_caller(ticket)
         Trace.trace(6,"dump "+str(sys.exc_info()[0])+
                     str(sys.exc_info()[1]))
         return


    # reload the configuration dictionary, possibly from a new file
    def load(self, ticket):
	try:
	    try:
		configfile = ticket["configfile"]
		out_ticket = {"status" : self.load_config(configfile)}
	    except KeyError:
		out_ticket = {"status" : (e_errors.KEYERROR, "Configuration Server: no such name")}
	    self.reply_to_caller(out_ticket)
	    Trace.trace(6,"load"+repr(out_ticket))
	    return

	# even if there is an error - respond to caller so he can process it
	except:
	    ticket["status"] = (str(sys.exc_info()[0]),str(sys.exc_info()[1]))
	    self.reply_to_caller(ticket)
	    Trace.trace(6,"load "+str(sys.exc_info()[0])+
			str(sys.exc_info()[1]))
	    return

    # get list of the Library manager movers
    def get_movers(self, ticket):
	ret = []
	if ticket.has_key('library'):
	    # search for the appearance of this library manager
	    # in all configured movers
	    for key in self.configdict.keys():
		if string.find (key, ".mover") != -1:
		    item = self.configdict[key]
		    if item.has_key('library'):
			if type(item['library']) == types.ListType:
			    for i in item['library']:
				if i == ticket['library']:
				    mv = {'mover' : key,
					  'address' : (item['hostip'], 
						      item['port'])
					  }
				    ret.append(mv)
			else:
			    if item['library'] == ticket['library']:
				mv = {'mover' : key,
				      'address' : (item['hostip'], 
						   item['port'])
				      }
				ret.append(mv)

	self.reply_to_caller(ret)
	Trace.trace(6,"get_movers"+repr(ret))

    #get list of library managers
    def get_library_managers(self, ticket):
        ret = {}
        for key in self.configdict.keys():
            index = string.find (key, ".library_manager")
            if index != -1:
                library_name = key[:index]
                item = self.configdict[key]
                ret[library_name] = {'address':(item['host'],item['port'])}
        self.reply_to_caller(ret)
        Trace.trace(6,"get_library_managers"+repr(ret))

    def reply_serverlist( self, ticket ):
        out_ticket = {"status" : (e_errors.OK, None), 
                      "server_list" : self.serverlist }
        self.reply_to_caller(out_ticket)
 
        
    def get_dict_element(self, ticket):
        ret = {"status" : (e_errors.OK, None)}
        slist = []
        skeyValue = ticket['keyValue']
        for key in self.configdict.keys():
            if skeyValue in self.configdict[key].items():
                slist.append(key)
        ret['servers'] = slist
        self.reply_to_caller(ret)
        Trace.trace(6,"get_dict_element"+repr(ret))


class ConfigurationServer(ConfigurationDict, generic_server.GenericServer):

    def __init__(self, csc, configfile=interface.default_file()):
	self.running = 0
	self.print_id = MY_NAME
        Trace.trace(10,
            "Instantiating Configuration Server at %s %s using config file %s"
            %(csc[0], csc[1], configfile))

        # make a configuration dictionary
        cd =  ConfigurationDict()

        # default socket initialization - ConfigurationDict handles requests
        dispatching_worker.DispatchingWorker.__init__(self, csc)

        # now (and not before,please) load the config file user requested
        self.load_config(configfile)

	self.running = 1

        # always nice to let the user see what she has
        Trace.trace(10, repr(self.__dict__))

class ConfigurationServerInterface(generic_server.GenericServerInterface):

    def __init__(self):
        # fill in the defaults for possible options
	self.config_file = ""
        generic_server.GenericServerInterface.__init__(self)

        # bomb out if we can't find the file
        statinfo = os.stat(self.config_file)
        Trace.trace(10,'stat for '+repr(self.config_file)+' '+repr(statinfo))

    # define the command line options that are valid
    def options(self):
        return generic_server.GenericServerInterface.options(self)+\
	       ["config_file=",]


if __name__ == "__main__":
    Trace.init(MY_NAME)
    Trace.trace( 6, "called args="+repr(sys.argv) )
    import sys

    # get the interface
    intf = ConfigurationServerInterface()

    # get a configuration server
    cs = ConfigurationServer((intf.config_host, intf.config_port),
	                     intf.config_file)

    while 1:
        try:
            Trace.trace(6,"Configuration Server (re)starting")
            cs.serve_forever()
        except:
	    cs.serve_forever_error(MY_NAME)
            continue

    Trace.trace(6,"Configuration Server finished (impossible)")
