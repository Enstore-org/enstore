import string
import os
import getopt
import sys

# Import SOCKS module if it exists, else standard socket module socket
# This is a python module that works just like the socket module, but uses the
# SOCKS protocol to make connections through a firewall machine.
# See http://www.w3.org/People/Connolly/support/socksForPython.html or
# goto www.python.org and search for "import SOCKS"
try:
    import SOCKS; socket = SOCKS
except ImportError:
    import socket

import pdb

def default_host():
    try:
	return os.environ['ENSTORE_CONFIG_HOST']
    except:
	print "can not get default host - reverting to localhost:",\
	      sys.exc_info()[0],sys.exc_info()[1]
	return("localhost")

def default_port():
    try:
	return os.environ['ENSTORE_CONFIG_PORT']
    except:
	print "can not get default port - reverting to 7500:",\
	      sys.exc_info()[0],sys.exc_info()[1]
	return("7500")

class Interface:

    def __init__(self, host=default_host(), port=default_port()):
        if host == "localhost" :
            (self.config_hostname, self.ca, self.ci) = \
                socket.gethostbyaddr(socket.gethostname())
            self.config_host = self.ci[0]
        else:            
	    self.config_host = host

	self.check_host()
	self.check_port(port)
        self.list = 0

    def charopts(self):
        return [""]

    def help_options(self):
	return ["help"]

    def config_options(self):
	return ["config_host=", "config_port="]

    def alive_rcv_options(self):
	return ["alive_rcv_timeout=","alive_retries="]

    def alive_options(self):
	return ["alive"]+self.alive_rcv_options()

    def list_options(self):
        return ["list", "verbose="]

    def help_line(self):
        return "python"+repr(sys.argv[0])+repr(self.options())

    def check_port(self, port):
	# bomb out if port isn't numeric
        if type(port) == type('string'):
	    self.config_port = string.atoi(port)
        else:
            self.config_port = port

    def check_host(self):
	# bomb out if can't translate host
	self.ip = socket.gethostbyname(self.config_host)

    def print_help(self):
        print "USAGE:"
	print self.help_line()
	print "" 
	print "     (do not forget the '--' in  front of each option)"

    def parse_config_host(self, value):
        try:
            self.csc.config_host = value
            self.csc.check_host()
        except AttributeError:
            self.config_host = value
            self.check_host()

    def parse_config_port(self, value):
        try:
            self.csc.check_port(value)
        except AttributeError:
            self.check_port(value)

    def strip(self, value):
	return value

    def parse_options(self):
        try:
            optlist,self.args=getopt.getopt(sys.argv[1:],self.charopts(),
                                            self.options())
        except:
            print "ERROR: ", sys.exc_info()[0], sys.exc_info()[1]
            self.print_help()
            sys.exit(1)

        for (opt,value) in optlist :
            value=self.strip(value)
            if opt == "--config_host" :
                self.parse_config_host(value)
            elif opt == "--config_port" :
                self.parse_config_port(value)
	    elif opt == "--bfids" :
        	self.bfids = 1
            elif opt == "--bfid" :
                self.bfid = value
	    elif opt == "--backup":
	        self.backup = 1
            elif opt == "--config_file" :
                self.config_file = value
                # bomb out if we can't find the file
    	        if len(self.config_file) :
                    statinfo = os.stat(self.config_file)
            elif opt == "--config_list" :
                self.config_list = 1
            elif opt == "--dict" :
                self.dict = 1
            elif opt == "--get_keys" :
                self.get_keys = 1
            elif opt == "--getwork" :
                self.getwork = 1
            elif opt == "--getmoverlist" :
                self.getmoverlist = 1
            elif opt == "--load" :
                self.load = 1
            elif opt == "--vols" :
                self.vols = 1
            elif opt == "--nextvol" :
                self.nextvol = 1
            elif opt == "--vol" :
                self.vol = value
            elif opt == "--addvol" :
                self.addvol = 1
            elif opt == "--delvol" :
                self.delvol = 1
            elif opt == "--clrvol" :
                self.clrvol = 1
            elif opt == "--test" :
                self.test = 1
            elif opt == "--logit" :
                self.logit1 = 1
                self.logmsg = value
            elif opt == "--alive" :
                self.alive = 1
            elif opt == "--alive_rcv_timeout" :
                self.alive_rcv_timeout = string.atoi(value)
            elif opt == "--alive_retries" :
                self.alive_retries = string.atoi(value)
            elif opt == "--timeout" :
                self.timeout = string.atoi(value)
            elif opt == "--get_timeout" :
                self.get_timeout = 1
            elif opt == "--update" :
                self.update = 1
            elif opt == "--updatefile" :
                self.updatefile = value
            elif opt == "--nocrc":
                self.chk_crc = 0
            elif opt == "--pri" :
                self.pri = string.atoi(value)
            elif opt == "--delpri" :
                self.delpri = string.atoi(value)
            elif opt == "--agetime" :
                self.agetime = string.atoi(value)
            elif opt == "--list":
                self.list = 1
            elif opt == "--verbose" :
                self.list = string.atoi(value)
            elif opt == "--d0sam":
                # if d0sam has been requested, just add 4096 to the list option
                self.list = self.list | 0x1000 
            elif opt == "--faccess":
  	        self.criteria['first_access']=self.check(value)
            elif opt == "--laccess":
	        self.criteria['last_access']=self.check(value)
            elif opt == "--declared":
	        self.criteria['declared']=self.check(value)
            elif opt == "--capacity":
	        self.criteria['capacity']=self.check(value)
            elif opt == "--rem_bytes":
	        self.criteria['rem_bytes']=self.check(value)
            elif opt == "--dbname":
	        self.dbname=value
            elif opt == "-v":
	        self.criteria['external_label']=string.split(value,',')
            elif opt == "-l":
	        self.criteria['library']=string.split(value,',')
            elif opt == "-f":
	        self.criteria['file_family']=string.split(value,',')
            elif opt == "-m":
	        self.criteria['media_type']=string.split(value,',') 
            elif opt == "-w":
	        self.criteria['wrapper']=string.split(value,',')
            elif opt == "-u":
	        self.criteria['user_inhibit']=string.split(value,',')
            elif opt == "-s":
	        self.criteria['system_inhibit']=string.split(value,',')
            elif opt == "--help" :
	        self.print_help()
                sys.exit(0)



