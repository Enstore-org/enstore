###############################################################################
# src/$RCSfile$   $Revision$
#
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
    return("localhost")

def default_port():
    return("7500")

class GenericClientServer:

    def __init__(self, host="localhost", port=0):
        if host == "localhost" :
            (self.config_host, self.ca, self.ci) = \
                socket.gethostbyaddr(socket.gethostname())
        else:            
	    self.config_host = host

	self.check_host()
	self.check_port(port)
        self.dolist = 0   # standard default

    def check_port(self, port):
	# bomb out if port isn't numeric
        if type(port) == type('string'):
	    self.config_port = string.atoi(port)
        else:
            self.config_port = port

    def check_host(self):
	# bomb out if can't translate host
	self.ip = socket.gethostbyname(self.config_host)

    def charopts(self):
        return [""]

    def options(self):
	return ["help"]

    def config_options(self):
	return ["config_host=", "config_port="]

    def list_options(self):
        return ["list", "verbose="]

    def help_line(self):
        return "python"+repr(sys.argv[0])+repr(self.options())

    def print_help(self):
        print "USAGE:"
	#self.help_line()
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
            optlist,self.args=getopt.getopt(sys.argv[1:],self.charopts(), \
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
            elif opt == "--getwork" :
                self.dogetwork = 1
            elif opt == "--load" :
                self.doload = 1
            elif opt == "--vols" :
                self.vols = 1
            elif opt == "--nextvol" :
                self.nextvol = 1
            elif opt == "--vol" :
                self.vol = value
            elif opt == "--addvol" :
                self.doaddvol = 1
            elif opt == "--delvol" :
                self.dodelvol = 1
            elif opt == "--clrvol" :
                self.clrvol = 1
            elif opt == "--test" :
                self.test = 1
            elif opt == "--logit" :
                self.logit1 = 1
                self.logmsg = value
            elif opt == "--alive" :
                self.doalive = 1
            elif opt == "--nocrc":
                self.chk_crc = 0
            elif opt == "--list":
                self.dolist = 1
            elif opt == "--verbose" :
                self.dolist = string.atoi(value)
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
