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
                info = self.u.send(request, (ns_host, ns_port) )
                break
            except socket.error :
                if sys.exc_value[0] == 111 :
                    #  preceding line needs to be of form: if sys.exc_info()[1][0] == errno.ENOENT :
                    time.sleep(3)
                    print sys.exc_info()[1][0], "socket error - retrying now"
                else :
                    raise sys.exc_info()[0],sys.exc_info()[1]
        self.cache[key] = info
        return info

    # return cached (or get from server) value for requested item
    def get(self, key, ns_host = 'localhost', ns_port = 7500):
        # try the cache
        try :
            return self.cache[key]
        except :
            return self.get_uncached(key, ns_host, ns_port)

    # dump the configuration dictionary
    def config(self, ns_host = 'localhost', ns_port = 7500):
        request = {'work' : 'config_dump' }
        while 1 :
            try:
                self.config_dump = self.u.send(request, (ns_host, ns_port) )
                break
            except socket.error :
                if sys.exc_value[0] == 111 :
                    #  preceding line needs to be of form: if sys.exc_info()[1][0] == errno.ENOENT :
                    time.sleep(3)
                    print sys.exc_info()[1][0], "socket error - retrying now"
                else :
                    raise sys.exc_info()[0],sys.exc_info()[1]
        pprint.pprint(self.config_dump)


if __name__ == "__main__" :
    csc = configuration_client()
    csc.config()
    if csc.config_dump['status'] == 'ok' :
	exit(0)
    else :
	exit(1)
