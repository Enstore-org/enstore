import string
import regsub
import pprint
import copy
from dispatching_worker import DispatchingWorker
from generic_server import GenericServer
from SocketServer import UDPServer, TCPServer

class ConfigurationDict(DispatchingWorker) :

    # load the configuration dictionary - the default is a wormhole in pnfs
    def load_config(self, configfile,list=1) :
        try:
            f = open(configfile)
        except :
            msg = "Configuration Server: load_config"\
                   +repr(configfile)+" does not exists"
            print msg
            return msg
        line = ""

        if list:
            print "Configuration Server load_config: "\
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
                f.close()
                msg = "Configuration Server: "\
                      +"can not process line: ",line \
                      ,"\ndictionary unchanged."
                print msg
                return msg
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
            print "Configuration Server: invalid dictionary, " \
                  +"loading ",configfile
            self.load_config(configfile)

    # just return the current value for the item the user wants to know about
    def lookup(self, ticket) :
        self.config_exists()
        # everything is based on lookup - make sure we have this
        try:
            key="lookup"
            lookup = ticket[key]
        except KeyError:
            ticket["status"] = "Configuration Server: "+key+" key is missing"
            pprint.pprint(ticket)
            self.reply_to_caller(ticket)
            return

        # look up in our dictionary the lookup key
        try :
            out_ticket = self.configdict[lookup]
        except KeyError:
            out_ticket = {"status" : "Configuration Server: no such name: "\
                          +repr(lookup)}
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
            out_ticket = {"status" : "Configuration Server: no such name"}
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
    import os
    import sys
    import getopt
    # Import SOCKS module if it exists, else standard socket module socket
    try:
        import SOCKS; socket = SOCKS
    except ImportError:
        import socket

    # defaults
    #config_host = "localhost"
    (config_host,ca,ci) = socket.gethostbyaddr(socket.gethostname())
    config_port = "7500"
    config_file = "/pnfs/enstore/.(config)(flags)/enstore.conf"
    list = 0

    # see what the user has specified. bomb out if wrong options specified
    options = ["config_host=","config_port=","config_file="\
               ,"list","help"]
    optlist,args=getopt.getopt(sys.argv[1:],'',options)
    for (opt,value) in optlist :
        if opt == "--config_host" :
            config_host = value
        elif opt == "--config_port" :
            config_port = value
        elif opt == "--config_file" :
            config_file = value
        elif opt == "--list" :
            list = 1
        elif opt == "--help" :
            print "python", sys.argv[0], options
            print "   do not forget the '--' in front of each option"
            sys.exit(0)

    # bomb out if can't translate host
    ip = socket.gethostbyname(config_host)

    # bomb out if port isn't numeric
    config_port = string.atoi(config_port)

    # bomb out if we can't find the file
    statinfo = os.stat(config_file)

    # instantiate, or bomb our, and then start server
    server_address = (config_host,config_port)
    if list:
        print "Instantiating Configuration Server at ", server_address\
              , " using config file ",config_file
    cs =  ConfigurationServer( server_address, config_file, list)

    cs.serve_forever()
