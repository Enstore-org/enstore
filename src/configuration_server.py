import sys
import posixfile
import string
import regsub
import pprint
from SocketServer import *
from dict_to_a import *
from dispatching_worker import DispatchingWorker
from generic_server import GenericServer


class ConfigurationDict(DispatchingWorker) :

# we really need an __init__, but I am not smart enough
# to make one when there is multiple inheritance and class arguments

#    def __init__(self,
#                configfile="/pnfs/enstore/.(config)(flags)/enstore.conf") :
#       self.configdict = {}
#       self.load_config(configfile)

    # load the configuration dictionary - the default is a wormhole in pnfs
    def load_config(self, configfile) :
        f = open(configfile)
        line = ""

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
            exec ("self."+line)
            # start again
            line = ""
        f.close()

    # does the configuration dictionary exist?
    def config_exists(self) :
        try :
            dict_exist = len(self.configdict)
        except:
            configfile="/pnfs/enstore/.(config)(flags)/enstore.conf"
            self.configdict = {}
            self.load_config(configfile)

    # dump out the current contents of the configuration dictionary
    def dump(self) :
        self.config_exists()
        return self.configdict

    # just return the current value for the item the user wants to know about
    def lookup(self, ticket) :
        self.config_exists()

        out_ticket = {"status" : "nosuchname"}
        try :
            out_ticket = self.configdict[ticket["lookup"]]
        except KeyError:
            pass    # send the previously set up error
        self.reply_to_caller(out_ticket)

    # return a dump of the dictionary back to the user
    def config_dump(self, ticket) :
        d=self.dump()
        out_ticket = {"status" : "ok", "config" : d}
        self.reply_to_caller(out_ticket)

class ConfigurationServer(ConfigurationDict, GenericServer, UDPServer) :
    pass

if __name__ == "__main__" :
    cd =  ConfigurationDict()
    cs =  ConfigurationServer( ("localhost", 7500), cd)
    current = cs.dump()
    cs.serve_forever()
