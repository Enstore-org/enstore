###############################################################################
# src/$RCSfile$   $Revision$
#
import string
import os
import getopt
import sys

# enstore imports
import Trace
import e_errors
import hostaddr

def getenv(var, default=None):
    val = os.environ.get(var)
    if val is None:
        used_default = 1
        val = default
    else:
        used_default = 0
    return val, used_default

DEFAULT_HOST = 'localhost'
DEFAULT_PORT = '7500'

used_default_config_host = 0
used_default_config_port = 0

def default_host():
    val, used_default = getenv('ENSTORE_CONFIG_HOST', default=DEFAULT_HOST)
    if used_default:
        global used_default_config_host
        used_default_config_host = 1
    return val

def default_port():
    val, used_default = getenv('ENSTORE_CONFIG_PORT', default=DEFAULT_PORT)
    val = int(val)
    if used_default:
        global used_default_config_port
        used_default_config_port = 1
    return val

def default_file():
    return "/pnfs/enstore/.(config)(flags)/enstore.conf"

def log_using_default(var, default):
    Trace.log(e_errors.INFO,
              "%s not set in environment or command line - reverting to %s"\
              %(var, default))

def check_for_config_defaults():
    # check if we are using the default host and port.  if this is true
    # then nothing was set in the environment or passed on the command
    # line. warn the user.
    if used_default_config_host:
        log_using_default('CONFIG HOST', DEFAULT_HOST)
    if used_default_config_port:
        log_using_default('CONFIG PORT', DEFAULT_PORT)

def str_to_tuple(s):
    # convert the string of the form "(val1, val2)" to a tuple of the form
    # (val1, val2) by doing the following -
    #              remove all surrounding whitespace
    #              remove the first char : the '(' char
    #              remove the last char  : the ')' char
    #              split into two based on ',' char
    tmp = string.strip(s)
    tmp = tmp[1:-1]
    return tuple(string.split(tmp, ",", 1))

def dash_to_underscore(s):
    ##accept - rather than _ in arguments - but only in the keywords, not
    ## the values!
    if s[:2] != '--':
        return s
    t = '--'
    eq=0
    for c in s[2:]:
        if c=='=':
            eq=1
        if c=='-' and not eq:
            c='_'
        t=t+c
    return t

class Interface:
    def __init__(self, host=default_host(), port=default_port()):
        if self.__dict__.get("do_parse", 1):
            if host == 'localhost' :
                self.check_host(hostaddr.gethostinfo()[0])
            else:            
                self.check_host(host)

                self.check_port(port)

            # now parse the options
            self.parse_options()

    def check_host(self, host):
        self.config_host = hostaddr.name_to_address(host)

    def charopts(self):
        return [""]

    def help_options(self):
	return ["help", "usage_line"]

    def config_options(self):
	return ["config_host=", "config_port="]

    def alive_rcv_options(self):
	return ["timeout=","retries="]

    def alive_options(self):
	return ["alive"]+self.alive_rcv_options()

    def format_options(self, opts, prefix):
	# put the options in alphabetical order and add a "--" to the front of
	# each
	opts.sort()
	nopts = ""
	for opt in opts:
	    nopts = nopts+prefix+"--"+opt
	return nopts

    def missing_parameter(self, param):
        Trace.trace(13,"ERROR: missing parameter %s"%(param,))
        print "ERROR: missing parameter %s"%(param,)

    def parameters(self):
	return " "

    def help_prefix(self):
	return sys.argv[0]+" [opts] "

    def help_suffix(self):
	return "\n\n\t where 'opts' are:\n"

    def help_line(self):
        return self.help_prefix()+self.parameters()+self.help_suffix()+self.format_options(self.options(), "\n\t\t")

    def check_port(self, port):
	# bomb out if port isn't numeric
        if type(port) == type('string'):
	    self.config_port = int(port)
        else:
            self.config_port = port

    def print_help(self):
        print "USAGE:\n"+self.help_line()+"\n"

    def print_usage_line(self, opts=[]):
        if not opts:
            opts = self.options()
        print "["+self.format_options(opts, " ")+"] "+self.parameters()+"\n"

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
	return value

    # This is a dummy option(), the derived class should supply a real
    # one

    def options(self):
        return []

    def parse_options(self):
        self.options_list = []
        try:
            argv=map(dash_to_underscore, sys.argv[1:])
            optlist,self.args=getopt.getopt(argv,self.charopts(),
                                            self.options())
        except getopt.error, detail:
            Trace.trace(9, "ERROR: getopt error %s"%(detail,))
            print "error: ", detail
            self.print_help()
	    sys.exit(1)
	    
        for (opt,value) in optlist :
            # keep a list of the options entered without the leading "--"
            self.options_list.append(string.replace(opt, "-", ""))

            value=self.strip(value)
            Trace.trace(10, "opt = %s, value = %s"%(opt,value))
            if opt == "--config_host" :
                self.parse_config_host(value)
            elif opt == "--config_port" :
                self.parse_config_port(value)
	    elif opt == "--bfids" :
        	self.bfids = 1
            elif opt == "--bfid" :
                self.bfid = value
            elif opt == "--list" :
                self.list = value
            elif opt == "--deleted" :
                self.deleted = value
	    elif opt == "--backup":
	        self.backup = 1
            elif opt == "--config_file" :
                self.config_file = value
                # bomb out if we can't find the file
                if len(self.config_file) :
                    statinfo = os.stat(self.config_file)
            elif opt == "--show" :
                self.show = 1
            elif opt == "--summary" :
                self.summary = 1
            elif opt == "--get_work" :
                self.get_work = 1
            elif opt == "--get_mover_list" :
                self.get_mover_list = 1
            elif opt == "--get_suspect_vols" :
                self.get_susp_vols = 1
            elif opt == "--get_delayed_dismount" :
                self.get_delayed_dismount = 1
            elif opt == "--delete_work" :
                self.work_to_delete = value
                self.delete_work = 1
            elif opt == "--change_priority" :
                self.change_priority = 1
            elif opt == "--summon" :
                self.summon = value
            elif opt == "--poll" :
                self.poll = 1
            elif opt == "--load_movers" :
                self.load_mover_list = 1
            elif opt == "--load" :
                self.load = 1
            elif opt == "--vols" :
                self.vols = 1
            elif opt == "--destroy" :
                self.rmvol = value
	    # D0_TEMP
            elif opt == "--atmover" :
                self.atmover = 1
	    # END D0_TEMP
            elif opt == "--next" :
                self.next = 1
            elif opt == "--vol" :
                self.vol = value
            elif opt == "--check" :
                self.check = value
            elif opt == "--import" :
                self._import = 1
            elif opt == "--export" :
                self._export = 1
            elif opt == "--clean_drive" :
                self.clean_drive = 1
            elif opt == "--new_library" :
                self.new_library = value
            elif opt == "--read_only" :
                self.read_only = value
            elif opt == "--no_access" :
                self.no_access = value
            elif opt == "--add" :
                self.add = value
            elif opt == "--delete" :
                self.delete = value
            elif opt == "--restore" :
                self.restore = value
            elif opt == "--recursive" :
                self.restore_dir = 1
            elif opt == "--all" :
                self.all = 1
            elif opt == "--force" :
                self.force = 1
            elif opt == "--clear" :
                self.clear = value
            elif opt == "--decr_file_count" :
                self.decr_file_count = value
            elif opt == "--message" :
                self.message = value
            elif opt == "--alive" :
                self.alive = 1
            elif opt == "--timeout" :
                self.alive_rcv_timeout = int(value)
            elif opt == "--retries" :
                self.alive_retries = int(value)
            elif opt == "--interval" :
                self.interval = int(value)
            elif opt == "--inq_timeout" :
                self.inq_timeout = int(value)
            elif opt == "--get_inq_timeout" :
                self.get_inq_timeout = 1
            elif opt == "--reset_inq_timeout" :
                self.reset_inq_timeout = 1
            elif opt == "--get_interval" :
                self.get_interval = value
            elif opt == "--reset_interval" :
                self.reset_interval = value
            elif opt == "--update" :
                self.update = value
            elif opt == "--update_and_exit" :
                self.update_and_exit = 1
            elif opt == "--max_encp_lines" :
                self.max_encp_lines = int(value)
            elif opt == "--get_max_encp_lines" :
                self.get_max_encp_lines = 1
            elif opt == "--crc":
                self.chk_crc = 1
            elif opt == "--priority" :
                self.priority = int(value)
            elif opt == "--delpri" :
                self.delpri = int(value)
            elif opt == "--age_time" :
                self.age_time = int(value)
            elif opt == "--delayed_dismount" :
                self.delayed_dismount = int(value)
            elif opt == "--dump":
                self.dump = 1
            elif opt == "--verbose" :
                if value == "":
                    self.verbose = self.verbose | 1
                else:
                    self.verbose = self.verbose | int(value)
            elif opt == "--status":
                self.status = 1
            elif opt == "--local_mover":
                self.local_mover = 1
                self.enable = int(value)
            elif opt == "--max_work":
                self.max_work = int(value)
            elif opt == "--mount" :
                self.mount = 1
            elif opt == "--dismount" :
                self.dismount = 1
            elif opt == "--refresh":
                self.refresh = int(value)
            elif opt == "--get_refresh":
                self.get_refresh = 1
            elif opt == "--get_logfile_name":
                self.get_logfile_name = 1
            elif opt == "--get_logfiles":
                self.get_logfiles = value
            elif opt == "--get_last_logfile_name":
                self.get_last_logfile_name = 1
            elif opt == "--data_access_layer":
                self.data_access_layer = 1
	    elif opt == "--use_IPC":
		self.use_IPC = 1
            elif opt == "--logfile_dir":
                self.logfile_dir = value
            elif opt == "--start_time":
                self.start_time = value
            elif opt == "--stop_time":
                self.stop_time = value
            elif opt == "--plot":
                self.plot = 1
            elif opt == "--get_queue":
	        self.queue_list=1
            elif opt == "--host":
	        self.host=value
            elif opt == "--ephemeral":
	        self.output_file_family="ephemeral"
            elif opt == "--file_family":
	        self.output_file_family=value
            elif opt == "--raise" :
                self.alarm = 1
            elif opt == "--resolve" :
                self.resolve = value
            elif opt == "--get_patrol_file" :
                self.get_patrol_file = 1
            elif opt == "--root_error" :
                self.root_error = value
            elif opt == "--severity" :
                self.severity = value
            elif opt == "--mc" :
                self.mcs = string.split(value, ",")
            elif opt == "--keep" :
                self.keep = 1
            elif opt == "--keep_dir" :
                self.keep_dir = value
            elif opt == "--output_dir" :
                self.output_dir = value
            elif opt == "--restore_all" :
                self.restore_all = 1
            elif opt == "--nocheck" :
                self.nocheck = 1
            elif opt == "--test_mode":
                file = globals().get('__file__', "")
                if file == "<frozen>":
                    print "test-mode not allowed in frozen binary"
                    sys.exit(-1)
                self.test_mode = 1
            elif opt == "--bytes":
                if not self.test_mode:
                    print "bytecount may only be specified in test mode"
                    sys.exit(-1)
                self.bytes = int(value)
            elif opt == "--get_crcs":
                self.get_crcs=value
            elif opt == "--set_crcs":
                self.set_crcs=value
            elif opt == "--start_draining":
                self.start_draining = value
            elif opt == "--stop_draining":
                self.stop_draining = 1
	    elif opt == "--prefix":
		self.prefix = value
	    elif opt == "--web_host":
		self.web_host = value
	    elif opt == "--caption_title":
		self.caption_title = value
	    elif opt == "--title":
		self.title = value
	    elif opt == "--title_gif":
		self.title_gif = value
	    elif opt == "--output":
		self.output = value
	    elif opt == "--description":
		self.description = value
	    elif opt == "--html_file":
		self.html_file = value
	    elif opt == "--input_dir":
		self.input_dir = value
	    elif opt == "--url":
		self.url = value
            elif opt == "--help" :
                self.print_help()
                sys.exit(0)
            elif opt == "--usage_line" :
	        self.print_usage_line()
                sys.exit(0)
