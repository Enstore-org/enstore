###############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
import pdb
import sys
import time
import errno

# enstore imports
import generic_client
import interface
import udp_client
import Trace

# Import SOCKS module if it exists, else standard socket module socket
# This is a python module that works just like the socket module, but uses the
# SOCKS protocol to make connections through a firewall machine.
# See http://www.w3.org/People/Connolly/support/socksForPython.html or
# goto www.python.org and search for "import SOCKS"
try:
    import SOCKS; socket = SOCKS
except ImportError:
    import socket

def set_csc(self, csc=0, host=interface.default_host(),\
            port=interface.default_port(), list=0):
    Trace.trace(10,'{set_csc csc='+repr(csc))
    if csc == 0:
        self.csc = ConfigurationClient(host, port, list)
    else:
        self.csc = csc
    Trace.trace(10,'}set_csc csc='+repr(self.csc))

class ConfigurationClient(generic_client.GenericClient) :

    def __init__(self, config_host, config_port, config_list):
        Trace.trace(10,'{__init__ cc')
        self.clear()
	if config_list>3 :
            print "Connecting to configuration server at ",\
	        config_host, config_port
        self.config_address=(config_host,config_port)
        self.u = udp_client.UDPClient()
        Trace.trace(11,'}connect add='+repr(self.config_address)+\
                    ' udp='+repr(self.u))

    # get rid of all cached values - go back to server for information
    def clear(self):
        Trace.trace(16,'{clear')
        self.cache = {}
        Trace.trace(16,'}clear')

    # get value for requested item from server, store locally in own cache
    def get_uncached(self, key):
        Trace.trace(11,'{get_uncached key='+repr(key))
        request = {'work' : 'lookup', 'lookup' : key }
        while 1:
            try:
                self.cache[key] = self.u.send(request, self.config_address )
                break
            except socket.error:
                if sys.exc_info()[1][0] == errno.CONNREFUSED:
                    delay = 3
                    Trace.trace(0,"}get_uncached retrying "+\
                                str(sys.exc_info()[0])+str(sys.exc_info()[1]))
                    print sys.exc_info()[1][0], "socket error. configuration "\
                          +"sending to",self.config_address\
                          ,"server down?  retrying in ",delay," seconds"
                    time.sleep(delay)
                else:
                    Trace.trace(0,"}get_uncached "+str(sys.exc_info()[0])+\
                                str(sys.exc_info()[1]))
                    raise sys.exc_info()[0],sys.exc_info()[1]
        Trace.trace(11,'}get_uncached key='+repr(key)+'='+repr(self.cache[key]))
        return self.cache[key]

    # return cached (or get from server) value for requested item
    def get(self, key):
        Trace.trace(11,'{get_uncached key='+repr(key))
        # try the cache
        try:
            val = self.cache[key]
        except:
            val = self.get_uncached(key)
        Trace.trace(11,'}get_cached key='+repr(key)+'='+repr(val))
        return val


    # dump the configuration dictionary
    def list(self):
        Trace.trace(16,'{list')
        request = {'work' : 'list' }
        while 1:
            try:
                self.config_list = self.u.send(request, self.config_address )
                break
            except socket.error:
                if sys.exc_info()[1][0] == errno.CONNREFUSED:
                    delay = 3
                    Trace.trace(0,"}list retrying "+\
                                str(sys.exc_info()[0])+str(sys.exc_info()[1]))
                    print sys.exc_info()[1][0], "socket error. configuration "\
                          +"sending to",self.config_address\
                          ,"server down?  retrying in ",delay," seconds"
                    time.sleep(delay)
                else:
                    Trace.trace(0,"}list "+str(sys.exc_info()[0])+\
                                str(sys.exc_info()[1]))
                    raise sys.exc_info()[0],sys.exc_info()[1]
        Trace.trace(16,'}list')

    # reload a new  configuration dictionary
    def load(self, configfile):
        Trace.trace(10,'{load configfile='+repr(configfile))
        request = {'work' : 'load' ,  'configfile' : configfile }
        while 1:
            try:
                x = self.u.send(request, self.config_address)
                Trace.trace(16,'}load '+repr(x))
                return x
            except socket.error:
                if sys.exc_info()[1][0] == errno.CONNREFUSED:
                    delay = 3
                    Trace.trace(0,"}load retrying "+\
                                str(sys.exc_info()[0])+str(sys.exc_info()[1]))
                    print sys.exc_info()[1][0], "socket error. configuration "\
                          +"sending to",self.config_address\
                          ,"server down?  retrying in ",delay," seconds"
                    time.sleep(delay)
                else:
                    Trace.trace(0,"}load "+str(sys.exc_info()[0])+\
                                str(sys.exc_info()[1]))
                    raise sys.exc_info()[0],sys.exc_info()[1]
        Trace.trace(16,'}load')

    # check on alive status
    def alive(self):
        Trace.trace(10,'{alive config_address='+repr(self.config_address))
        x = self.u.send({'work':'alive'},self.config_address )
        Trace.trace(10,'}alive '+repr(x))
        return x

class ConfigurationClientInterface(interface.Interface):
    def __init__(self):
        # fill in the defaults for the possible options
       	self.config_file = ""
        self.config_list = 0
        self.dict = 0
        self.load = 0
        self.alive = 0
        interface.Interface.__init__(self)

        # parse the options
        self.parse_options()

    # define the command line options that are valid
    def options(self):
        return self.config_options() + self.list_options()+\
	       ["config_file=","config_list","dict","load","alive"] + \
	       self.help_options()

if __name__ == "__main__":
    import sys
    import pprint
    Trace.init("config cli")
    Trace.trace(1,"config client called with args "+repr(sys.argv))

    # fill in interface
    intf = ConfigurationClientInterface()

    # now get a configuration client
    csc = ConfigurationClient(intf.config_host, intf.config_port,\
                               intf.config_list)
    stat = "ok"

    if intf.alive:
        stati = csc.alive()
        if intf.list:
            pprint.pprint(stati)
    elif intf.dict:
        csc.list()
        if intf.list:
            print csc.config_list["list"]
            #pprint.pprint(csc.config_list)
        stat = csc.config_list['status']

    elif intf.load:
        stati= csc.load(intf.config_file)
        if intf.list:
            pprint.pprint(stati)
        stat=stati['status']

    del csc.u		# del now, otherwise get name exception (just for python v1.5???)
    if stat == 'ok':
        Trace.trace(1,"config client exit ok")
        sys.exit(0)
    else:
        Trace.trace(0,"csc BAD STATUS - "+repr(stat))
        sys.exit(1)
