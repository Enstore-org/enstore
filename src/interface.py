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
    import SOCKS
    socket = SOCKS
except ImportError:
    import socket
import generic_cs

def getenv(var, default=None):
    val = os.environ.get(var)
    if val is None:
        generic_cs.enprint("%s not set in environment - reverting to %s" %
                           (var, default))
        val = default
    return val

def default_host():
    return getenv('ENSTORE_CONFIG_HOST', default='localhost')

def default_port():
    return string.atoi(getenv('ENSTORE_CONFIG_PORT', default='7500'))

def default_file():
    return "/pnfs/enstore/.(config)(flags)/enstore.conf"

class Interface:
    def __init__(self, host=default_host(), port=default_port()):
        if host == "localhost" :
            self.check_host(socket.gethostname())
        else:            
            self.check_host(host)

	self.check_port(port)

        # now parse the options
        self.parse_options()

    def check_host(self, host):
        (self.config_hostname, self.ca, self.ci) = socket.gethostbyaddr(host)
        self.config_host = self.ci[0]

    def charopts(self):
        if 0: print self # lint fix
        return [""]

    def help_options(self):
        if 0: print self # lint fix
	return ["help", "usage_line"]

    def config_options(self):
        if 0: print self # lint fix
	return ["config_host=", "config_port="]

    def alive_rcv_options(self):
        if 0: print self # lint fix
	return ["alive_rcv_timeout=","alive_retries="]

    def verbose_options(self):
        if 0: print self # lint fix
	return ["verbose=","server_verbose="]

    def alive_options(self):
        if 0: print self # lint fix
	return ["alive"]+self.alive_rcv_options()

    def format_options(self, opts, prefix):
        if 0: print self # lint fix
	# put the options in alphabetical order and add a "--" to the front of
	# each
	opts.sort()
	nopts = ""
	for opt in opts:
	    nopts = nopts+prefix+"--"+opt
	return nopts

    def missing_parameter(self, param):
        if 0: print self # lint fix
	generic_cs.enprint("ERROR: missing parameter "+repr(param))

    def parameters(self):
        if 0: print self # lint fix
	return " "

    def help_prefix(self):
        if 0: print self # lint fix
	return sys.argv[0]+" [opts] "

    def help_suffix(self):
        if 0: print self # lint fix
	return "\n\n\t where 'opts' are:\n"

    def help_line(self):
        return self.help_prefix()+self.parameters()+self.help_suffix()+self.format_options(self.options(), "\n\t\t")

    def check_port(self, port):
	# bomb out if port isn't numeric
        if type(port) == type('string'):
	    self.config_port = string.atoi(port)
        else:
            self.config_port = port

    def print_help(self):
        generic_cs.enprint("USAGE:\n  "+self.help_line()+"\n")

    def print_usage_line(self):
        generic_cs.enprint("["+self.format_options(self.options(), " ")+"] "+self.parameters()+"\n")

    def parse_config_host(self, value):
        try:
            self.csc.config_host = value
            self.csc.check_host(self.csc.config_host)
        except AttributeError:
            self.config_host = value
            self.check_host(self.config_host)

    def parse_config_port(self, value):
        try:
            self.csc.check_port(value)
        except AttributeError:
            self.check_port(value)

    def strip(self, value):
        if 0: print self # lint fix
	return value

    def parse_options(self):
        try:
            optlist,self.args=getopt.getopt(sys.argv[1:],self.charopts(),
                                            self.options())
        except:
            generic_cs.enprint("ERROR: "+str(sys.exc_info()[0])+" "+\
	                       str(sys.exc_info()[1]))
            self.print_help()
            sys.exit(1)
	    
        for (opt,value) in optlist :
            value=self.strip(value)
	    generic_cs.enprint("opt = "+repr(opt)+", value = "+repr(value), \
	                       generic_cs.INTERFACE, self.verbose)
            if opt == "--config_host" :
                self.parse_config_host(value)
            elif opt == "--config_port" :
                self.parse_config_port(value)
	    elif opt == "--bfids" :
        	self.bfids = 1
            elif opt == "--bfid" :
                self.bfid = value
            elif opt == "--tape_list" :
                self.tape_list = value
            elif opt == "--deleted" :
                self.deleted = value
	    elif opt == "--backup":
	        self.backup = 1
            elif opt == "--config_file" :
                self.config_file = value
                # bomb out if we can't find the file
                if len(self.config_file) :
                    statinfo = os.stat(self.config_file)
                    if 0: print statinfo # lint fix
            elif opt == "--dict" :
                self.dict = 1
            elif opt == "--get_keys" :
                self.get_keys = 1
            elif opt == "--getwork" :
                self.getwork = 1
            elif opt == "--getmoverlist" :
                self.getmoverlist = 1
            elif opt == "--get_suspect_vols" :
                self.get_susp_vols = 1
            elif opt == "--get_del_dismount" :
                self.get_del_dismounts = 1
            elif opt == "--del_work" :
                self.remove_work = 1
            elif opt == "--change_priority" :
                self.change_priority = 1
            elif opt == "--loadmovers" :
                self.load_mover_list = 1
            elif opt == "--load" :
                self.load = 1
            elif opt == "--vols" :
                self.vols = 1
	    # D0_TEMP
            elif opt == "--atmover" :
                self.atmover = 1
	    # END D0_TEMP
            elif opt == "--nextvol" :
                self.nextvol = 1
            elif opt == "--vol" :
                self.vol = value
            elif opt == "--statvol" :
                self.statvol = value
            elif opt == "--view" :
                self.view = value
            elif opt == "--newlib" :
                self.newlib = 1
            elif opt == "--rdovol" :
                self.rdovol = 1
            elif opt == "--noavol" :
                self.noavol = 1
            elif opt == "--addvol" :
                self.addvol = 1
            elif opt == "--delvol" :
                self.delvol = 1
            elif opt == "--force" :
                self.force = 1
            elif opt == "--clrvol" :
                self.clrvol = 1
            elif opt == "--decr_file_count" :
                self.decr_file_count = value
            elif opt == "--eod_cookie" :
                self.eod_cookie = value
            elif opt == "--size" :
                self.size = string.atoi(value)
            elif opt == "--device" :
                self.device = value
            elif opt == "--test" :
                self.test = 1
            elif opt == "--restore" :
                self.restore = 1
	        self.file = value
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
            elif opt == "--reset_timeout" :
                self.reset_timeout = 1
            elif opt == "--update" :
                self.update = 1
            elif opt == "--ascii_file" :
                self.ascii_file = value
            elif opt == "--timestamp" :
                self.timestamp = 1
            elif opt == "--max_ascii_size" :
                self.max_ascii_size = string.atoi(value)
            elif opt == "--max_encp_lines" :
                self.max_encp_lines = string.atoi(value)
            elif opt == "--get_max_encp_lines" :
                self.get_max_encp_lines = 1
            elif opt == "--get_max_ascii_size" :
                self.get_max_ascii_size = 1
            elif opt == "--html_file" :
                self.html_file = value
            elif opt == "--file" :
	        self.info = 1
                self.file = value
            elif opt == "--crc":
                self.chk_crc = 1
            elif opt == "--pri" :
                self.pri = string.atoi(value)
            elif opt == "--delpri" :
                self.delpri = string.atoi(value)
            elif opt == "--agetime" :
                self.agetime = string.atoi(value)
            elif opt == "--delayed_dismount" :
                self.delayed_dismount = string.atoi(value)
            elif opt == "--debug":
                self.debug = 1
            elif opt == "--dump":
                self.dump = 1
            elif opt == "--nosummon":
                self.summon = 1
            elif opt == "--verbose" :
                if value == "":
                    self.verbose = self.verbose | 1
                else:
                    self.verbose = self.verbose | string.atoi(value)
            elif opt == "--status":
                self.status = 1
            elif opt == "--maxwork":
                self.maxwork = string.atoi(value)
            elif opt == "--refresh":
                self.refresh = string.atoi(value)
            elif opt == "--get_refresh":
                self.get_refresh = 1
            elif opt == "--get_logfile_name":
                self.get_logfile_name = 1
            elif opt == "--get_last_logfile_name":
                self.get_last_logfile_name = 1
            elif opt == "--data_access_layer" or opt == "--d0sam":
                # if data_access_layer has been requested, just add 4096 to verbose option
                self.verbose = self.verbose | 0x1000 
            elif opt == "--server_verbose" :
	        self.got_server_verbose = 1
	        self.server_verbose = string.atoi(value)
            elif opt == "--logfile_dir":
                self.logfile_dir = value
            elif opt == "--start_time":
                self.start_time = value
            elif opt == "--stop_time":
                self.stop_time = value
            elif opt == "--plot":
                self.plot = 1
            elif opt == "--dbname":
	        self.dbname=value
            elif opt == "--queue":
	        self.queue_list=1
            elif opt == "--ephemeral":
	        self.output_file_family="ephemeral"
            elif opt == "--file_family":
	        self.output_file_family=value
            elif opt == "--mail_node":
	        self.mail_node=value
            elif opt == "--alarm" :
                self.alarm = 1
            elif opt == "--resolve" :
                self.resolve = string.atof(value)
            elif opt == "--patrol_file" :
                self.patrol_file=1
            elif opt == "--root_error" :
                self.root_error = value
            elif opt == "--severity" :
                self.severity = string.atoi(value)
            elif opt == "--help" :
	        self.print_help()
                sys.exit(0)
            elif opt == "--usage_line" :
	        self.print_usage_line()
                sys.exit(0)
