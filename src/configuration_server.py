import os
import sys
import posixfile
import string
import regsub
import pprint
import copy
from SocketServer import *
from dict_to_a import *
from dispatching_worker import DispatchingWorker
from generic_server import GenericServer

class ConfigurationDict(DispatchingWorker) :

    # load the configuration dictionary - the default is a wormhole in pnfs
    def load_config(self, configfile,list=1) :
        try:
            f = open(configfile)
        except :
            return repr(configfile)+" does not exists"
        line = ""

        if list:
            print "ConfigurationDict load_config: "\
                  +"loading enstore configuration from ",configfile
        while 1:
            # read another line - appending it to what we already have
            nextline = f.readline()
            if nextline == "" :
                break
            # strip the line - this includes blank space and NL characters
            nextline = string.strip(nextline)
            if len(nextline) == 0 :
                continue
            line = line+nextline
            # are we at end of line or is there a continuation character "\"
            if line[len(line)-1] == "\\" :
                line = line[0:len(line)-1]
                continue
            # ok, we have a complete line - execute it
            try :
                exec ("x"+line)
            except :
                print "ConfigurationDict load_config: "\
                      +"can not process line: ",line \
                      ,"\ndictionary unchanged."
                f.close()
                return "bad"
            # start again
            line = ""
        f.close()
        # ok, we read entire file - now set it to real dictionary
        self.configdict=copy.deepcopy(xconfigdict)
        return "ok"

    # does the configuration dictionary exist?
    def config_exists(self) :
        need = 0
        try :
            if len(self.configdict) == 0 :
                need =1
        except:
            need = 1
        if need:
            configfile="/pnfs/enstore/.(config)(flags)/enstore.conf"
            print "ConfigurationDict.config_exists: invalid dictionary, " \
                  +"loading ",configfile
            self.load_config(configfile)

    # just return the current value for the item the user wants to know about
    def lookup(self, ticket) :
        self.config_exists()

        try :
            out_ticket = self.configdict[ticket["lookup"]]
        except KeyError:
            out_ticket = {"status" : "nosuchname"}
        self.reply_to_caller(out_ticket)

    # return a dump of the dictionary back to the user
    def list(self, ticket) :
        self.config_exists()
        out_ticket = {"status" : "ok", "list" : self.configdict}
        self.reply_to_caller(out_ticket)

    # reload the configuration dictionary, possibly from a new file
    def load(self, ticket) :
        try :
            configfile = ticket["configfile"]
            list = 1
            out_ticket = {"status" : self.load_config(configfile,list)}
        except KeyError:
            out_ticket = {"status" : "nosuchname"}
        self.reply_to_caller(out_ticket)


class ConfigurationServer(ConfigurationDict, GenericServer, UDPServer) :
    def __init__(self, server_address \
                 ,configfile="/pnfs/enstore/.(config)(flags)/enstore.conf"\
                 ,list=0):

        # make a configuration dictionary
        cd =  ConfigurationDict()
        # default socket initialization - ConfigurationDict handles requests
        TCPServer.__init__(self, server_address, cd)
        # now (and not before,please) load the config file user requested
        self.load_config(configfile,list)
        #check that it is valid - or else load a "good" one
        self.config_exists()
        # always nice to let the user see what she has
        if list:
            pprint.pprint(self.__dict__)

if __name__ == "__main__" :
    import getopt
    import socket

    # defaults
    host = "localhost"
    port = "7500"
    configfile = "/pnfs/enstore/.(config)(flags)/enstore.conf"
    list = 1

    # see what the user has specified. bomb out if wrong options specified
    options = ["host=","port=","configfile=","list", "nolist","help"]
    optlist,args=getopt.getopt(sys.argv[1:],'',options)
    for (opt,value) in optlist :
        if opt == "--host" :
            host = value
        elif opt == "--port" :
            port = value
        elif opt == "--list" :
            list = 1
        elif opt == "--nolist" :
            list = 0
        elif opt == "--help" :
            print sys.argv[0], options
	    print "   do not forget the '--' in front of each option"
	    sys.exit(0)

    # bomb out if can't translate host
    ip = socket.gethostbyname(host)

    # bomb out if port isn't numeric
    port = string.atoi(port)

    # bomb out if we can't find the file
    statinfo = os.stat(configfile)

    # instantiate, or bomb our, and then start server
    server_address = (host,port)
    if list:
        print "Instantiating Configuration Server at ", server_address\
              , " using config file ",configfile
    cs =  ConfigurationServer( server_address, configfile,list)

    cs.serve_forever()
