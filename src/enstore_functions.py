import time
import string
import os
import exceptions
import socket
import tempfile
import types
import pwd
import signal

import configuration_server
import enstore_constants
import enstore_files
import Trace
import e_errors
import www_server
import option

DEFAULTHTMLDIR = "."

# return both the user associated with the uid and the euid.
def get_user():
    uid = os.getuid()
    euid = os.geteuid()
    username = pwd.getpwuid(uid)[0]
    eusername = pwd.getpwuid(euid)[0]

# return a string version of a list
def print_list(aList, sep=" "):
    str = ""
    for item in aList:
	str = "%s%s%s"%(str, item, sep)
    else:
	# remove the last separator
	str = str[0:-1]
    return str

# format the mail
def format_mail(goal, question, metric): 
    return "\n\n  GOAL: %s\n\n  QUESTION: %s\n\n  METRIC: %s"%(goal, question,
							       metric)

# send mail
def send_mail(server, message, subject):
    mail_file = tempfile.mktemp()  
    os.system("date >> %s"%(mail_file,))
    os.system('echo "\n\tFrom: %s\n" >> %s' % (server, mail_file))
    os.system('echo "\t%s" >> %s' % (message, mail_file))
    os.system("/usr/bin/Mail -s \"%s\" $ENSTORE_MAIL < %s"%(subject, mail_file,))
    os.system("rm %s"%(mail_file,))

# this is done here to have central exception handling.  it cannot be
# done in the erc read routine, because we cannot import Trace in that
# module as Trace imports the erc module
def read_erc(erc, fd=None):
    try:
    	msg = erc.read()
    except socket.error, detail:
	Trace.log (e_errors.ERROR, 
		   "socket error - could not read from erc (%s)"%(detail,))
	return None
    return msg

def get_config_dict():
    name = os.environ.get("ENSTORE_CONFIG_FILE", "")
    if name:
        cdict = configuration_server.ConfigurationDict()
	if not cdict.load_config(name) == (e_errors.OK, None):
	    cdict = {}
    else:
        cdict = {}
    return cdict

def get_from_config_file(server, keyword, default):
    cdict = get_config_dict()
    if cdict:
        server_dict = cdict.configdict.get(server, None)
        if server_dict:
            return server_dict.get(keyword, default)
        else:
            return default
    else:
        return default

def get_media():
    return get_from_config_file(www_server.WWW_SERVER,
				www_server.MEDIA_TAG,
				www_server.MEDIA_TAG_DEFAULT)

# return the location of the html files from the config file
def get_html_dir():
    return get_from_config_file("inquisitor", "html_file", DEFAULTHTMLDIR)

# return a dictionary of the configuration server host and port
def get_config_server_info():
    dict = {'port' : option.default_port()}
    dict['host'] = option.default_host()
    return dict

def get_www_host():
    default = get_config_server_info()['host']
    return get_from_config_file("inquisitor", "www_host", default)

def get_mover_status_filename():
    return "enstore_movers.html"

def read_schedule_file(html_dir=None):
    if html_dir is None:
        html_dir = get_html_dir()
    # check if the html_dir is accessible
    sfile = None
    if os.path.exists(html_dir):
        sfile = enstore_files.ScheduleFile(html_dir, enstore_constants.OUTAGEFILE)
        outage_d, offline_d, override_d = sfile.read()
    else:
        outage_d = {}
        offline_d = {}
	override_d = {}
    return sfile, outage_d, offline_d, override_d

def read_seen_down_file(html_dir=None):
    if html_dir is None:
        html_dir = get_html_dir()
    # check if the html_dir is accessible
    sfile = None
    if os.path.exists(html_dir):
        sfile = enstore_files.SeenDownFile(html_dir, enstore_constants.SEENDOWNFILE)
        seen_down_d = sfile.read()
    else:
        seen_down_d = {}
    return sfile, seen_down_d

def override_to_status(override):
    # translate the override value to a real status
    if type(override) == types.ListType:
	# this is the new format
	override = override[0]
    index = enstore_constants.SAAG_STATUS.index(override)
    return enstore_constants.REAL_STATUS[index]

def get_days_ago(date, days_ago):
    # return the date that is days_ago before date
    seconds_ago = float(days_ago*86400)
    return date - seconds_ago

def get_remote_file(node, file, newfile):
    # we have to make sure that the rcp does not hang in case the remote node is goofy
    pid = os.fork()
    if pid == 0:
	# this is the child
	rtn = os.system("enrcp %s:%s %s"%(node, file, newfile))
	os._exit(rtn)
    else:
	# this is the parent, allow a total of 30 seconds for the child
	for i in [0, 1, 2, 3, 4, 5]:
	    rtn = os.waitpid(pid, os.WNOHANG)
	    if rtn[0] == pid:
		return rtn[1] >> 8   # pick out the top 8 bits as the return code
	    time.sleep(5)
	else:
	    # the child has not finished, be brutal. it may be hung
	    os.kill(pid, signal.SIGKILL)
	    return 1

# translate time.time output to a person readable format.
# strip off the day and reorganize things a little
YEARFMT = "%Y-%b-%d"
TIMEFMT = "%H:%M:%S"
def format_time(theTime, sep=" "):
    return time.strftime("%s%s%s"%(YEARFMT, sep, TIMEFMT), time.localtime(theTime))

PLOTYEARFMT = "%Y-%m-%d"
def format_plot_time(theTime, sep=" "):
    return time.strftime("%s"%(PLOTYEARFMT,), time.localtime(theTime))

def unformat_time(strTime, sep=" "):
    time_t = time.strptime(strTime,"%s%s%s"%(YEARFMT, sep, TIMEFMT))
    return time.mktime(time_t)

# return the directory
def get_dir(str):
    if os.path.isdir(str):
	return str
    else:
	# strip off the last set of chars after the last /
	file_spec = os.path.split(str)
	return file_spec[0]

# strip off anything before the '/'
def strip_file_dir(str):
    ind = string.rfind(str, "/")
    if not ind == -1:
        str2 = str[(ind+1):]
    else:
        str2 = str
    return str2

# remove the string .fnal.gov if it is in the input string
def strip_node(str):
    if type(str) == types.StringType:
	return string.replace(str, ".fnal.gov", "")
    else:
	return str

def is_this(server, suffix):
    stype = string.split(server, ".")
    if stype[len(stype)-1] == suffix:
        return 1
    return 0

# return true if the passed server name ends in "library_manager"
def is_library_manager(server):
    return is_this(server, enstore_constants.LIBRARY_MANAGER)

# return true if the passed server name ends in "mover"
def is_mover(server):
    return is_this(server, enstore_constants.MOVER)

# return true if the passed server name ends in "media_changer"
def is_media_changer(server):
    return is_this(server, enstore_constants.MEDIA_CHANGER)

def get_name(server):
    return string.split(server, ".")[0]

def get_bpd_subdir(dir):
    new_dir = "%s/%s"%(dir, enstore_constants.BPD_SUBDIR)
    if not os.path.exists(new_dir):
	# doesn't exist, use the old one
	new_dir = dir
    return new_dir

# return true if the passed server name is one of the following -
#   file_clerk, volume_clerk, alarm_server, inquisitor, log_server, config
#   server, event_relay
def is_generic_server(server):
    if server in enstore_constants.GENERIC_SERVERS:
        return 1
    return 0

def get_status(dict):
    status = dict.get('status', None)
    if status is None or type(status) != type(()):
        return None
    else:
        return status[0]

# check if the status in the dictionary signals a time out
def is_timedout(dict):
    status = dict.get('status', None)
    if status is None or type(status) != type(()):
        return None
    if status[0] == e_errors.TIMEDOUT:
        return 1
    else:
        return None

# check if the status in the dictionary signals everything is ok
def is_ok(dict):
    status = dict.get('status', None)
    if status is None or type(status) != type(()):
        return None
    if status[0] == e_errors.OK:
        return 1
    else:
        return None

def inqTrace(severity, msg):
    # add the pid to the front of the msg
    msg2 = "(%s) %s"%(os.getpid(), msg)
    Trace.trace(severity, msg2)

try:
    import threading
except ImportError:
    threading = None

if threading:    
    def run_in_thread(obj, thread_name, function, args=(), after_function=None):
        thread = getattr(obj, thread_name, None)
        for wait in range(5):
            if thread and thread.isAlive():
                Trace.trace(20, "thread %s is already running, waiting %s" % (thread_name, wait))
                time.sleep(1)
        if thread and thread.isAlive():
                Trace.log(e_errors.ERROR, "thread %s is already running" % (thread_name))
                return -1
        if after_function:
            args = args + (after_function,)
        thread = threading.Thread(group=None, target=function,
                                  name=thread_name, args=args, kwargs={})
        setattr(obj, thread_name, thread)
        try:
            thread.start()
        except exceptions.Exception, detail:
            Trace.log(e_errors.ERROR, "starting thread %s: %s" % (thread_name, detail))
        return 0


DEFAULT_HOST = 'localhost'
DEFAULT_PORT = 7500

def default_host():
    val = os.environ.get('ENSTORE_CONFIG_HOST')
    if val:
        return val
    else:
        return DEFAULT_HOST

def default_port():
    val = os.environ.get('ENSTORE_CONFIG_PORT')
    if val:
        return int(val)
    else:
        return DEFAULT_PORT
