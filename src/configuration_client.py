import os
import sys
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

    def __init__(self, config_host="localhost", config_port=7500):
        self.clear()
        self.config_address=(config_host,config_port)
        self.u = UDPClient()

    # get rid of all cached values - go back to server for information
    def clear(self):
        self.cache = {}

    # get value for requested item from server, store locally in own cache
    def get_uncached(self, key):
        request = {'work' : 'lookup', 'lookup' : key }
        while 1 :
            try:
                self.cache[key] = self.u.send(request, self.config_address )
                break
            except socket.error :
                if sys.exc_info()[1][0] == errno.CONNREFUSED :
                    delay = 3
                    print sys.exc_info()[1][0], "socket error. configuration "\
                          +"sending to",self.config_address\
                          ,"server down?  retrying in ",delay," seconds"
                    time.sleep(delay)
                else :
                    raise sys.exc_info()[0],sys.exc_info()[1]
        return self.cache[key]

    # return cached (or get from server) value for requested item
    def get(self, key):
        # try the cache
        try :
            return self.cache[key]
        except :
            return self.get_uncached(key)

    # dump the configuration dictionary
    def list(self):
        request = {'work' : 'list' }
        while 1 :
            try:
                self.config_list = self.u.send(request, self.config_address )
                break
            except socket.error :
                if sys.exc_info()[1][0] == errno.CONNREFUSED :
                    delay = 3
                    print sys.exc_info()[1][0], "socket error. configuration "\
                          +"sending to",self.config_address\
                          ,"server down?  retrying in ",delay," seconds"
                    time.sleep(delay)
                else :
                    raise sys.exc_info()[0],sys.exc_info()[1]

    # reload a new  configuration dictionary
    def load(self, configfile):
        request = {'work' : 'load' ,  'configfile' : configfile }
        while 1 :
            try:
                return self.u.send(request, self.config_address)
            except socket.error :
                if sys.exc_info()[1][0] == errno.CONNREFUSED :
                    delay = 3
                    print sys.exc_info()[1][0], "socket error. configuration "\
                          +"sending to",self.config_address\
                          ,"server down?  retrying in ",delay," seconds"
                    time.sleep(delay)
                else :
                    raise sys.exc_info()[0],sys.exc_info()[1]

if __name__ == "__main__" :
    import getopt
    import socket

    # defaults
    config_host = "localhost"
    #(config_host,ca,ci) = socket.gethostbyaddr(socket.gethostname())
    config_port = "7500"
    config_file = ""
    config_list = 0
    dict = 0
    load = 0
    list = 0

    # see what the user has specified. bomb out if wrong options specified
    options = ["config_host=","config_port=","config_file=","config_list"\
               ,"list","dict","load","help"]
    optlist,args=getopt.getopt(sys.argv[1:],'',options)
    for (opt,value) in optlist :
        if opt == "--config_host" :
            config_host = value
        elif opt == "--config_port" :
            config_port = value
        elif opt == "--config_file" :
            config_file = value
        elif opt == "--config_list" :
            config_list = 1
        elif opt == "--dict" :
            dict = 1
        elif opt == "--load" :
            load = 1
        elif opt == "--list" :
            list = 1
        elif opt == "--help" :
            print "python ",sys.argv[0], options
            print "   do not forget the '--' in front of each option"
            sys.exit(0)

    # bomb out if can't translate host
    ip = socket.gethostbyname(config_host)

    # bomb out if port isn't numeric
    config_port = string.atoi(config_port)

    # bomb out if we can't find the file
    if len(config_file) :
        statinfo = os.stat(config_file)

    if config_list :
        print "Connecting to configuration server at ",config_host,config_port
    csc = configuration_client(config_host,config_port)
    stat = "ok"

    if dict:
        csc.list()
        if list:
            pprint.pprint(csc.config_list)
        stat = csc.config_list['status']

    elif load:
        stati= csc.load(config_file)
        if list:
            print stati
        stat=stati['status']

    if stat == 'ok' :
        sys.exit(0)
    else :
        sys.exit(1)
