import time
import string
import os
import exceptions
import tempfile
import types
import pwd
import signal
import socket

import configuration_server
import configuration_client
import enstore_constants
import enstore_files
import enstore_functions2
import Trace
import e_errors
import www_server
import option

DEFAULTHTMLDIR = "."

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

#Similar to get_from_config_file(), but returns the entire dictionary
# for the server.
def get_dict_from_config_file(server, default):
    cdict = get_config_dict()
    if cdict:
        server_dict = cdict.configdict.get(server, None)
        if server_dict:
            return server_dict
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
    if type(dict) == type({}):
        #If the entire dictionary is passed in, use just the status part.
        status = dict.get('status', None)
    elif type(dict) == type(()) and len(dict) == 2:
        #The status tuple was passed in directly, adjust accordingly.
        status = dict
    else:
        status = None
        
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


## Return the directory where temporary enstore files should go.
def get_enstore_tmp_dir():

    #First check the config file.  If local to the config server machine
    # read the config file directly.  Otherwise contact the config server.
    config_hostname = os.environ.get('ENSTORE_CONFIG_HOST', "localhost")
    hostnames = socket.gethostbyname_ex(socket.gethostname())
    hostnames = [hostnames[0]] + hostnames[1] + hostnames[2]
    if config_hostname in hostnames:
        rtn_dir = get_from_config_file("temp_dir", "temp_dir", None)
    else:
        def_host = enstore_functions2.default_host()
        def_port = enstore_functions2.default_port()
        csc = configuration_client.ConfigurationClient((def_host, def_port))
        temp_dict = csc.get("temp_dir", 3, 3)
        rtn_dir = temp_dict.get("temp_dir", None)

    #Use the temp. directory the config server had it.
    if rtn_dir != None:
        return rtn_dir

    #Next, use 'tmp' under ENSTORE_DIR.
    try:
        rtn_dir = os.path.join(os.environ['ENSTORE_DIR'], "tmp")
    except (OSError, KeyError):
        rtn_dir = "/tmp/enstore/"

    return rtn_dir
