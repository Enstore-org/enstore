###############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
import pdb
import sys
import time
import errno

# enstore imports
import generic_client_server
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

def set_csc(self, csc=[], host=generic_client_server.default_host(), port=generic_client_server.default_port()):
    Trace.trace(10,'{set_csc csc='+repr(csc))
    if csc == []:
        # this calls GenericClientServer.__init__ too
        self.csc = configuration_client(host, port)
    else:
        self.csc = csc
    Trace.trace(10,'}set_csc csc='+repr(self.csc))

class configuration_client(generic_client_server.GenericClientServer):

    def __init__(self, host=generic_client_server.default_host(), port=generic_client_server.default_port()):
        Trace.trace(10,'{__init__ cc')
        self.clear()
        generic_client_server.GenericClientServer.__init__(self, host, port)
        self.config_list = 0
        self.dict = 0
        self.doload = 0
        self.doalive = 0
        self.dolist = 0
        self.config_file = ""
        Trace.trace(10,'}__init__ cc')

    # define the command line options that are valid
    def options(self):
        Trace.trace(20,'{}options')
        return generic_client_server.GenericClientServer.config_options(self)+\
               generic_client_server.GenericClientServer.list_options(self)  +\
               ["config_file=","config_list","dict","load","alive"] + \
               generic_client_server.GenericClientServer.options(self)

    # we cannot use the one in GenericClientServer because it assumes that the
    # value is actually stored in self.csc
    def parse_config_host(self, value):
        Trace.trace(11,'{parse_config_host value='+repr(value))
        self.config_host = value
        self.check_host()
        Trace.trace(11,'}parse_config_host')

    # we cannot use the one in GenericClientServer because it assumes that the
    # value is actually stored in self.csc
    def parse_config_port(self, value):
        Trace.trace(11,'{parse_config_port value='+repr(value))
        self.check_port(value)
        Trace.trace(11,'}parse_config_port')

    # connect to the configuration client
    def connect(self):
        Trace.trace(11,'{connect')
        if self.config_list:
            print "Connecting to configuration server at ",\
                self.config_host, self.config_port
        self.config_address=(self.config_host,self.config_port)
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


if __name__ == "__main__":
    import sys
    import pprint
    Trace.init("config cli")
    Trace.trace(1,"config client called with args "+repr(sys.argv))

    # fill in defaults
    csc = configuration_client()

    # see what the user has specified. bomb out if wrong options specified
    csc.parse_options()
    csc.connect()
    stat = "ok"

    if csc.doalive:
        stati = csc.alive()
        if csc.dolist:
            pprint.pprint(stati)

    elif csc.dict:
        csc.list()
        if csc.dolist:
            print csc.config_list["list"]
            #pprint.pprint(csc.config_list)
        stat = csc.config_list['status']

    elif csc.doload:
        stati= csc.load(csc.config_file)
        if csc.dolist:
            pprint.pprint(stati)
        stat=stati['status']

    if stat == 'ok':
        Trace.trace(1,"config client exit ok")
        sys.exit(0)
    else:
        Trace.trace(0,"csc BAD STATUS - "+repr(stat))
        sys.exit(1)
