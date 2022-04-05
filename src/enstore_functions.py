#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

import time
#import string
import os
import exceptions
#import tempfile
#import types
#import pwd
#import signal
import socket

import configuration_server
import configuration_client
import enstore_constants
import enstore_functions2
import Trace
import e_errors
import www_server
import option
import Interfaces

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
    dictionary = {'port' : enstore_functions2.default_port()}
    dictionary['host'] = enstore_functions2.default_host()
    return dictionary

def get_www_host():
    default = get_config_server_info()['host']
    return get_from_config_file("inquisitor", "www_host", default)

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
    hostname = socket.gethostname()
    hostinfo = socket.getaddrinfo(hostname, None)
    #hostnames = socket.gethostbyname_ex(socket.gethostname())
    #hostnames = [hostnames[0]] + hostnames[1] + hostnames[2]
    hostnames = [hostname, [hostname.split('.')[0]], [hostinfo[0][4][0]]]
    # Building set of hostnames which correspond to current machine
    # Interfaces.interfacesGet() returns set of network interfaces
    for item in Interfaces.interfacesGet().values():
        # For nodes with multiple IP addresses, we need to check for all
        # of them.
        if item['ip'] == "127.0.0.1": #Ignore localhost.
            continue
        # Get hostnames for each ip in set of network interfaces
        tmp_hostnames = socket.gethostbyaddr(item['ip'])
        tmp_hostnames = [tmp_hostnames[0]] + tmp_hostnames[1] + tmp_hostnames[2]
        for name in tmp_hostnames: #Keep the list unique.
            if name not in hostnames:
                hostnames.append(name)
    # Compare hostname from config to set of hostnames of this machine
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
        print('rtn_dir not None!')
        return rtn_dir
    print('rtn_dir None..')

    #Next, use 'tmp' under ENSTORE_DIR.
    tmp_dir = os.environ.get('ENSTORE_OUT',None)
    if tmp_dir == None:
        tmp_dir = os.environ.get('ENSTORE_HOME',None)
        if tmp_dir == None:
            tmp_dir = os.environ.get('ENSTORE_DIR','')
    
    try:
        rtn_dir = os.path.join(tmp_dir, "tmp")
    except (OSError, KeyError):
        rtn_dir = "/tmp/enstore/"

