###############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
import sys
import string
import copy
import types
import socket
import os

# enstore imports
import dispatching_worker
import generic_server
import generic_cs
import interface
import Trace
import e_errors
import generic_cs

class ConfigurationDict(dispatching_worker.DispatchingWorker, \
                        generic_cs.GenericCS):

    def __init__(self):
	self.print_id="CONFIGD"

    # load the configuration dictionary - the default is a wormhole in pnfs
    def load_config(self, configfile,verbose=1):
     Trace.trace(6,"{load_config configfile="+repr(configfile))
     xconfigdict = {} # we will be loading new config file into xconfigdict - clear it
     try:
        try:
            f = open(configfile)
        except:
            msg = (e_errors.DOESNOTEXIST,"Configuration Server: load_config"\
                   +repr(configfile)+" does not exists")
            self.enprint(msg)
            Trace.trace(0,"}load_config "+msg)
            return msg
        line = ""

	self.enprint("Configuration Server load_config: "+\
                     "loading enstore configuration from "+configfile,
	             generic_cs.CLIENT, verbose)
        while 1:
            # read another line - appending it to what we already have
            nextline = f.readline()
            if nextline == "":
                break
            # strip the line - this includes blank space and NL characters
            nextline = string.strip(nextline)
            if len(nextline) == 0 or nextline[0] == '#':
                continue
            line = line+nextline
            # are we at end of line or is there a continuation character "\"
            if line[len(line)-1] == "\\":
                line = line[0:len(line)-1]
                continue
            # ok, we have a complete line - execute it - the exec creates a dictionary
            #   called xconfigdict (because the config file has lines like configdict =)
            #   once we know it is ok, we will change the name from xconfigdict to configdict
            try:
	        self.enprint(line, generic_cs.CLIENT, verbose)
                exec ("x"+line)
            except:
                f.close()
                msg = (e_errors.EXECERROR, "Configuration Server: "\
                      +"can not process line: ",line \
                      ,"\ndictionary unchanged.")
                self.enprint(msg)
                Trace.trace(0,"}load_config"+msg)
                return msg
            # start again
            line = ""
        f.close()
        # ok, we read entire file - now set it to real dictionary
        self.configdict=copy.deepcopy(xconfigdict)
        self.serverlist = {}
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
			if (self.serverlist[configured_key][1] == \
			   self.configdict[key]['hostip']) and \
			   (self.serverlist[configured_key][2] == \
			    self.configdict[key]['port']):
			    msg = "Configuration Conflict detected for "\
				  "hostip "+\
				  repr(self.configdict[key]['hostip'])+ \
				  "and port "+ \
				  repr(self.configdict[key]['port'])
			    generic_cs.enprint(msg, 1, 1)
			    conflict = 1
			    break
		    if not conflict:
			self.serverlist[key]= (self.configdict[key]['host'],self.configdict[key]['hostip'],self.configdict[key]['port'])
		    break
		
        Trace.trace(6,"}load_config ok")
	if conflict:
	    Trace.trace(6,"}load_config. Configuration conflict detected."\
			"Check configuration file")
	    return(e_errors.CONFLICT, "Configuration conflict detected. "\
		   "Check configuration file")
        Trace.trace(6,"}load_config ok")
        return (e_errors.OK, None)

     # even if there is an error - respond to caller so he can process it
     except:
         self.enprint(str(sys.exc_info()[0])+" "+str(sys.exc_info()[1]))
         Trace.trace(0,"}load_config "+str(sys.exc_info()[0])+\
                     str(sys.exc_info()[1]))
         return (str(sys.exc_info()[0]), str(sys.exc_info()[1]))


    # does the configuration dictionary exist?
    def config_exists(self):
     Trace.trace(20,"{config_exists")
     try:
        need = 0
        try:
            if len(self.configdict) == 0:
                need =1
        except:
            need = 1
        if need:
            configfile="/pnfs/enstore/.(config)(flags)/enstore.conf"
            msg ="Configuration Server: invalid dictionary, " \
                  +"loading "+repr(configfile)
            self.enprint(msg)
            Trace.trace(0,"config_exists "+msg)
            self.load_config(configfile)
        Trace.trace(20,"}config_exists")
        return

     # even if there is an error - respond to caller so he can process it
     except:
         self.enprint(str(sys.exc_info()[0])+str(sys.exc_info()[1]))
         Trace.trace(0,"}config_exists "+str(sys.exc_info()[0])+\
                     str(sys.exc_info()[1]))
         return


    # just return the current value for the item the user wants to know about
    def lookup(self, ticket):
     Trace.trace(6,"{lookup ")
     try:
        self.config_exists()
        # everything is based on lookup - make sure we have this
        try:
            key="lookup"
            lookup = ticket[key]
        except KeyError:
            Trace.trace(0,"lookup "+repr(key)+" key is missing")
            ticket["status"] = (e_errors.KEYERROR, "Configuration Server: "+key+" key is missing")
	    self.enprint(ticket, generic_cs.PRETTY_PRINT)
            self.reply_to_caller(ticket)
            Trace.trace(6,"}lookup")
            return

        # look up in our dictionary the lookup key
        try:
            out_ticket = self.configdict[lookup]
        except KeyError:
            Trace.trace(0,"lookup no such name"+repr(lookup))
            out_ticket = {"status": (e_errors.KEYERROR, "Configuration Server: no such name: "\
                          +repr(lookup))}
            self.enprint(out_ticket, generic_cs.PRETTY_PRINT)
        self.reply_to_caller(out_ticket)
        Trace.trace(6,"}lookup "+repr(lookup)+"="+repr(out_ticket))
        return

     # even if there is an error - respond to caller so he can process it
     except:
         ticket["status"] = (str(sys.exc_info()[0]), str(sys.exc_info()[1]))
         self.enprint(ticket, generic_cs.PRETTY_PRINT)
         self.reply_to_caller(ticket)
         Trace.trace(0,"}lookup "+str(sys.exc_info()[0])+\
                     str(sys.exc_info()[1]))
         return

    # return a dump of the dictionary back to the user
    def get_keys(self, ticket):
     Trace.trace(6,"{get_keys")
     try:
        self.config_exists()
        skeys = self.configdict.keys()
	skeys.sort()
        out_ticket = {"status" : (e_errors.OK, None), "get_keys" : (skeys)}
        self.reply_to_caller(out_ticket)
        Trace.trace(6,"}get_keys")
        return

     # even if there is an error - respond to caller so he can process it
     except:
         ticket["status"] = str(sys.exc_info()[0])+str(sys.exc_info()[1])
         self.enprint(ticket, generic_cs.PRETTY_PRINT)
         self.reply_to_caller(ticket)
         Trace.trace(0,"}get_keys "+str(sys.exc_info()[0])+\
                     str(sys.exc_info()[1]))
         return


    # return a dump of the dictionary back to the user
    def list(self, ticket):
     Trace.trace(6,"{list")
     try:
        self.config_exists()
        sortedkey = self.configdict.keys()
        sortedkey.sort()
        formatted= "configdict = {}\n"
        for key in sortedkey:
           formatted= formatted + "\nconfigdict['" + key + "'] = {"
           len2 = len(key)
           count4 = 0
           for key2 in self.configdict[key].keys():
              count4 = count4+1
           count3 = 0
           sortedkeyinside = self.configdict[key].keys()
           sortedkeyinside.sort()
           for key2 in sortedkeyinside:
              if key2 == 'hostip':
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
                 formatted= formatted + ", \\"
              else:
                 formatted= formatted + " }\n"
        #self.enprint(formatted)
        out_ticket = {"status" : (e_errors.OK, None), "list" : formatted}
        self.reply_to_caller(out_ticket)
        Trace.trace(6,"}list")
        return

     # even if there is an error - respond to caller so he can process it
     except:
         ticket["status"] = str(sys.exc_info()[0])+str(sys.exc_info()[1])
         self.enprint(ticket, generic_cs.PRETTY_PRINT)
         self.reply_to_caller(ticket)
         Trace.trace(0,"}list "+str(sys.exc_info()[0])+\
                     str(sys.exc_info()[1]))
         return


    # reload the configuration dictionary, possibly from a new file
    def load(self, ticket):
	Trace.trace(6,"{load")
	try:
	    try:
		configfile = ticket["configfile"]
		verbose = 1
		out_ticket = {"status" : self.load_config(configfile,verbose)}
	    except KeyError:
		out_ticket = {"status" : (e_errors.KEYERROR, "Configuration Server: no such name")}
	    self.reply_to_caller(out_ticket)
	    Trace.trace(6,"}load"+repr(out_ticket))
	    return

	# even if there is an error - respond to caller so he can process it
	except:
	    ticket["status"] = (str(sys.exc_info()[0]),str(sys.exc_info()[1]))
	    self.enprint(ticket, generic_cs.PRETTY_PRINT)
	    self.reply_to_caller(ticket)
	    Trace.trace(0,"}load "+str(sys.exc_info()[0])+\
			str(sys.exc_info()[1]))
	    return

    # get list of the Library manager movers
    def get_movers(self, ticket):
	Trace.trace(6,"{get_movers")
	ret = []
	#self.enprint(self.configdict, generic_cs.PRETTY_PRINT)
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
				    mv = {'mover' : key,\
					  'address' : (item['hostip'], \
						      item['port'])
					  }
				    ret.append(mv)
			else:
			    if item['library'] == ticket['library']:
				mv = {'mover' : key,\
				      'address' : (item['hostip'], \
						   item['port'])
				      }
				ret.append(mv)

	self.reply_to_caller(ret)
	Trace.trace(6,"}get_movers"+repr(ret))


    def reply_configdict( self, ticket ):
        Trace.trace(6,"{reply_configdict "+repr(ticket))
        out_ticket = {"status" : (e_errors.OK, None), "list" : self.configdict }
        self.reply_to_caller(out_ticket)
        Trace.trace(6,"}reply_configdict"+repr(out_ticket))

    def reply_serverlist( self, ticket ):
        Trace.trace(6,"{reply_serverlist "+repr(ticket))
        out_ticket = {"status" : (e_errors.OK, None), "server_list" : self.serverlist }
        self.reply_to_caller(out_ticket)
        Trace.trace(6,"}reply_serverlist"+repr(out_ticket))
	 

class ConfigurationServer(ConfigurationDict, generic_server.GenericServer):

    def __init__(self, verbose=0, host=interface.default_host(), \
                 port=interface.default_port(), \
                 configfile=interface.default_file()):
        Trace.trace(3,"{ConfigurationServer address="+repr(host)+" "+\
                    repr(port)+" configfile="+repr(configfile)+" verbose="+\
                    repr(verbose))
	self.running = 0
	self.print_id = "CONFIGS"
	self.enprint("Instantiating Configuration Server at "+repr(host)+", "+\
                     repr(port)+" using config file "+configfile, \
	             generic_cs.SERVER, verbose)

        # make a configuration dictionary
        cd =  ConfigurationDict()
        Trace.trace(10,'init ConfigurationDict '+repr(cd))

        # default socket initialization - ConfigurationDict handles requests
        dispatching_worker.DispatchingWorker.__init__(self, (host, port))

        # now (and not before,please) load the config file user requested
        self.load_config(configfile,verbose)

        #check that it is valid - or else load a "good" one
        self.config_exists()
	self.running = 1

        # always nice to let the user see what she has
	self.verbose = verbose
	self.enprint(self.__dict__, generic_cs.SERVER, verbose)

class ConfigurationServerInterface(generic_server.GenericServerInterface):

    def __init__(self):
        Trace.trace(10,'{csi.__init__')
        # fill in the defaults for possible options
	self.config_file = ""
        generic_server.GenericServerInterface.__init__(self)

        # bomb out if we can't find the file
        statinfo = os.stat(self.config_file)
        Trace.trace(10,'stat for '+repr(self.config_file)+' '+repr(statinfo))
        Trace.trace(10,'}csi.__init__')

    # define the command line options that are valid
    def options(self):
        Trace.trace(16, "{}options")
        return generic_server.GenericServerInterface.options(self)+\
	       ["config_file=",]


if __name__ == "__main__":
    Trace.init("configsrvr")
    Trace.trace(1,"{called args="+repr(sys.argv))
    import sys


    # get the interface
    intf = ConfigurationServerInterface()

    # get a configuration server
    cs = ConfigurationServer(intf.verbose, intf.config_host, intf.config_port,
	                     intf.config_file)

    while 1:
        try:
            Trace.trace(1,"Configuration Server (re)starting")
            cs.serve_forever()
        except:
	    cs.serve_forever_error("config server")
            continue

    Trace.trace(1,"Configuration Server finished (impossible)")
