from os import *
from sys import *
import string
import time
from udp_client import UDPClient
import errno
import pprint

# Import SOCKS module if it exists, else standard socket module socket
try:
    import SOCKS; socket = SOCKS
except ImportError:
    import socket

class configuration_client :

    def __init__(self):
        self.clear()
        self.u = UDPClient()

    # get rid of all cached values - go back to server for information
    def clear(self):
        self.cache = {}

    # get value for requested item from server, store locally in own cache
    def get_uncached(self, key, ns_host = 'localhost', ns_port = 7500):
        request = {'work' : 'lookup', 'lookup' : key }
        while 1 :
            try:
                self.cache[key] = self.u.send(request, (ns_host, ns_port) )
                break
            except socket.error :
                if sys.exc_info()[1][0] == errno.CONNREFUSED :
                    delay = 3
                    print sys.exc_info()[1][0], "socket error. configuration "\
                          +"server down?  retrying in ",delay," seconds"
                    time.sleep(delay)
                else :
                    raise sys.exc_info()[0],sys.exc_info()[1]
        return self.cache[key]

    # return cached (or get from server) value for requested item
    def get(self, key, ns_host = 'localhost', ns_port = 7500):
        # try the cache
        try :
            return self.cache[key]
        except :
            return self.get_uncached(key, ns_host, ns_port)

    # dump the configuration dictionary
    def list(self, ns_host = 'localhost', ns_port = 7500):
        request = {'work' : 'list' }
        while 1 :
            try:
                self.config_list = self.u.send(request, (ns_host, ns_port) )
                break
            except socket.error :
                if sys.exc_info()[1][0] == errno.CONNREFUSED :
                    delay = 3
                    print sys.exc_info()[1][0], "socket error. configuration "\
                          +"server down?  retrying in ",delay," seconds"
                    time.sleep(delay)
                else :
                    raise sys.exc_info()[0],sys.exc_info()[1]

    # reload a new  configuration dictionary
    def load(self, configfile, ns_host = 'localhost', ns_port = 7500):
        request = {'work' : 'load' ,  'configfile' : configfile }
        while 1 :
            try:
                return self.u.send(request, (ns_host, ns_port) )
            except socket.error :
                if sys.exc_info()[1][0] == errno.CONNREFUSED :
                    delay = 3
                    print sys.exc_info()[1][0], "socket error. configuration "\
                          +"server down?  retrying in ",delay," seconds"
                    time.sleep(delay)
                else :
                    raise sys.exc_info()[0],sys.exc_info()[1]

if __name__ == "__main__" :

    csc = configuration_client()

    try :
        if sys.argv[1] == "list" :
            csc.list()
            pprint.pprint(csc.config_list)
            stat = csc.config_list['status']
        elif sys.argv[1] == "load" :
            stati= csc.load(sys.argv[2])
            print stati
	    stat=stati['status']
        else :
            csc.list()
            stat = csc.config_list['status']
    except :
        csc.list()
        stat = csc.config_list['status']

    if stat == 'ok' :
        exit(0)
    else :
        exit(1)
