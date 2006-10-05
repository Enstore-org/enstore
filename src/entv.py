#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

#system imports
import os
import sys
import socket
import select
import string
import time
#import thread
import threading
#import re
import rexec
import errno
import gc, inspect
import types
import pprint


#enstore imports
import enstore_display
import configuration_client
import enstore_functions2
#import enstore_constants
import enstore_erc_functions
import e_errors
#import mover_client
import event_relay_messages
import event_relay_client
import Trace
import generic_client
import option
import delete_at_exit
import host_config
import udp_client


#4-30-2002: For reasons unknown to me the order of the imports matters a lot.
# If the wrong order is done, then the dashed lines are not drawn.
#3-5-2003: Update to previous comment.  Tkinter must be imported after
# importing enstore_display.  I still don't know why this is.
#11-1-2004: Update to previous comment.  The reason enstore_display must
# be imported first is that the tkinter environmental variables are set
# at the top of the file.
import Tkinter

#########################################################################
# Globals
#########################################################################

_rexec = rexec.RExec()
def _eval(stuff):
    return _rexec.r_eval(stuff)

TEN_MINUTES=600   #600seconds = 10minutes
DEFAULT_BG_COLOR = '#add8e6'   #light blue

#When entv reads from a command file use these as defaults.
DEFAULT_SYSTEM_NAME = "Enstore"
DEFAULT_GEOMETRY = "1200x1600+0+0"
DEFAULT_ANIMATE = 1

#status_threads = None
messages_threads = []

#Should we need to stop (ie. cntl-C) this is the global flag.
stop_now = 0

#For cleanup_objects() to report problems.
old_list = []
old_len  = 0

#For callbacks called from the master window.  This is the list of canvases.
displays = []

#########################################################################
# common support functions
#########################################################################

def entv_client_version():
    ##this gets changed automatically in {enstore,encp}Cut
    ##You can edit it manually, but do not change the syntax
    version_string = "v0_0  CVS $Revision$ "
    entv_file = globals().get('__file__', "")
    if entv_file: version_string = version_string + entv_file
    return version_string

def open_files(message):
    print message,
    os.system("ls -l /proc/`(EPS | grep \"entv\" | head -n 1 | cut -c8-15 | tr -d ' ')`/fd | wc -l")

def endswith(s1,s2):
    return s1[-len(s2):] == s2


def dict_eval(data):
    ##This is like "eval" but it assumes the input is a
    ## dictionary; any trailing junk will be ignored.
    last_brace = string.rindex(data, '}')
    try:
        d = _eval(data[:last_brace+1])
    except (ValueError, KeyError, IndexError, TypeError):
        print "Error", data,
        d = {}
    return d


def print_object(item):
    if type(item) in [types.FrameType]:
        print type(item), str(item)[:60], item.f_code
    else:
        print type(item), str(item)[:60]

    i = 1
    for lsk in gc.get_referrers(item):
        if lsk == inspect.currentframe():
            continue

        if type(lsk) in [types.FrameType]:
            print "  %d: %s %s" % (i,
                                 str(pprint.pformat(lsk))[:100],
                                 lsk.f_code)
        else:
            print "  %d: %s" % (i, str(pprint.pformat(lsk))[:100])
        i = i + 1
    print

def cleanup_objects():
    global old_list
    global old_len

    #Get current information on the number of objects currently consuming
    # resources.
    new_list = gc.get_objects()
    new_len = len(new_list)
    del new_list[:]
    
    #Force garbage collection while the display is off while awaiting
    # initialization.
    gc.collect()
    del gc.garbage[:]
    gc.collect()
    uncollectable_count = len(gc.garbage)
    del gc.garbage[:]

    #First report what the garbage collection algorithm says...
    if uncollectable_count > 0:
        Trace.trace(10, "UNCOLLECTABLE COUNT: %s" % uncollectable_count)

    #Then (starting with the second pass) report the object count difference.
    if old_len == 0:
        old_len = new_len #Only set this on the first pass.
    else:
        if new_len - old_len > 2:
            Trace.trace(10, "NEW COUNT DIFFERENCE: %s - %s = %s"
                        % (new_len, old_len, new_len - old_len))
        if new_len - old_len >= 100:
            #Only return true if 
            return True

    return None

def memory_in_use():
    if os.uname()[0] == "Linux":
        f = open("/proc/%d/status" % (os.getpid(),), "r")
        proc_info = f.readlines()
        f.close()

        for item in proc_info:
            words = item.split()
            if words[0] == "VmSize:":
                return int(words[1])

        return None #Should never happen.
    else:
        return None

"""
def get_system(intf=None): #, system_name=None):
    global _system_csc
    if _system_csc:  #used cached version.
        return _system_csc

    #If intf is not given and the cached version is already known not to
    # exist throw an error an abort.
    if not intf:
        sys.stderr.write("Unknown error.  Aborting\n")
        sys.exit(1)

    #If running on 'canned' version, don't bother with configuration server.
    if intf.movers_file:
        return None
    
    default_config_host = enstore_functions2.default_host()
    default_config_port = enstore_functions2.default_port()

    # get a configuration server
    csc = configuration_client.ConfigurationClient((default_config_host,
                                                    default_config_port))

    #Get the list of all config servers and remove the 'status' element.
    config_servers = csc.get('known_config_servers', {})
    if config_servers['status'][0] == e_errors.OK:
        del config_servers['status']
    else:
        return None

    config_port = enstore_functions2.default_port()
    if intf.args:
        for system_name in config_servers.keys():
            if system_name not in intf.args:
                del config_servers[system_name]

    #if system_name != None: #len(intf.args) > 0:
    #    config_host = system_name   #intf.args[0]
    else:
        config_host = enstore_functions2.default_host()
        #config_host = os.environ.get("ENSTORE_CONFIG_HOST")

        #Based on the config file determine which config server was specified.
        for name in config_servers.keys():
            if len(config_host) >= len(name) and \
                   config_host[:len(name)] == name:
                #config_host = config_servers[name][0]
                #break
                pass
            else:
                del config_servers['name']

    config_host = config_servers.values()[0][0]
    _system_csc = configuration_client.ConfigurationClient((config_host,
                                                            config_port))
    return _system_csc
"""

def get_all_systems(csc, intf=None): #, system_name=None):
    #global _system_csc
    #if _system_csc:  #used cached version.
    #    return _system_csc

    #If intf is not given and the cached version is already known not to
    # exist throw an error an abort.
    if not intf:
        sys.stderr.write("Unknown error.  Aborting.\n")
        sys.exit(1)

    #If running on 'canned' version, don't bother with configuration server.
    if intf.movers_file:
        return None
    
    #Get the list of all config servers and remove the 'status' element.
    config_servers = csc.get('known_config_servers', {})
    if config_servers['status'][0] == e_errors.OK:
        del config_servers['status']
    else:
        return None

    if intf.args:
        for system_name in config_servers.keys():
            if system_name not in intf.args:
                del config_servers[system_name]

        if len(config_servers) == 0:
            sys.stderr.write("Unknown enstore systems.  Aborting.\n")
            sys.exit(1)
    else:
        config_host = enstore_functions2.default_host()

        #Based on the config file determine which config server was specified.
        for name in config_servers.keys():
            if len(config_host) >= len(name) and \
                   config_host[:len(name)] == name:
                pass
            else:
                del config_servers[name]

    #Special section for test systems that are not in their own
    # config file's 'known_config_servers' section.
    if not config_servers:
        i = 0
        while i < 3:
            try:
                ip = socket.gethostbyname(config_host)
                addr_info = socket.gethostbyaddr(ip)
                break
            except socket.error:
                time.sleep(1)
                continue
        else:
            sys.stderr.write("Unable to obtain ip information.  Aborting.\n")
            sys.exit(1)
        if addr_info[1] != []:
            short_name = addr_info[1][0]
        else:
            short_name = addr_info[0].split(".")[0]
        config_servers[short_name] = (ip,
                                      enstore_functions2.default_port())
        
    return config_servers

#def get_config(intf):
#    global _config_cache
#    if _config_cache:
#        return _config_cache
#    csc = intf.csc
#    try:
#        _config_cache = csc.dump()
#    except errno.errorcode[errno.ETIMEDOUT]:
#        return {}
#    return _config_cache


def get_system_name(intf, cscs_info):
    
    if intf.movers_file:
        return "local_host"

    if len(intf.args) > 1:
        #If more than one system is specified, return default text.
        return "Enstore: %s" % intf.args

    if len(cscs_info.keys()) == 0:
        sys.stderr.write("Config not found.\n")
        sys.exit(1)

    elif len(cscs_info.keys()) == 1:
        return cscs_info.keys()[0]

    else:
        return "Enstore: %s" % intf.args
        


#########################################################################
# .entrc file related functions
#########################################################################

def get_entvrc_filename():
    return os.environ["HOME"] + "/.entvrc"

def get_entvrc_file():
    lines = []

    #Get the file contents.
    try:
        f = open(get_entvrc_filename())
        for line in f.readlines():
            lines.append(line.strip())
        f.close()
    except (OSError, IOError):
        pass
    
    ###Don't remove blank lines and command lines from the output.  This
    ### output is used by the set_entvrc function which will use these
    ### extraneous lines.

    return lines

def get_entvrc(csc, intf):

    library_colors = {}
    client_colors = {}
    found_server = False

    try:
        if intf.movers_file:
            address = "localhost"
        elif intf.args:
            # get a configuration server
            #default_config_host = enstore_functions2.default_host()
            #default_config_port = enstore_functions2.default_port()
            #csc = configuration_client.ConfigurationClient(
            #    (default_config_host, default_config_port))

            config_servers = csc.get('known_config_servers', {})

            #Based on the config file determine which config server was
            # specified.
            for name in config_servers.keys():
                if name == intf.args[0]:
                    address = config_servers[name][0]
                    break
                elif len(config_servers[name][0]) >= len(intf.args[0]) and \
                   config_servers[name][0][len(intf.args[0]):] == intf.args[0]:
                    address = config_servers[name][0]
                    break
        else:
            #We need a default.
            address = os.environ['ENSTORE_CONFIG_HOST']

        #Only need to grab this once.
        entvrc_data = get_entvrc_file()

        for line in entvrc_data:

            #Check the line for problems or things to skip, like comments.
            if len(line) == 0:
                continue
            if line[0] == "#": #Skip comment lines.
                continue
            
            #Split the string and look for problems.
            words = line.strip().split()
            if not words:
                continue

            #If the line gives outline color for movers based on their
            # library manager, pass this information along.
            if words[0] == "library_color":
                try:
                    library_colors[words[1]] = words[2]
                except (IndexError, KeyError, AttributeError, ValueError,
                        TypeError):
                    pass
                continue

            #If the line gives fill color for clients based on their nodename.
            if words[0] == "client_color":
                try:
                    client_colors[words[1]] = words[2]
                except (IndexError, KeyError, AttributeError, ValueError,
                        TypeError):
                    pass
                continue

            #Check if the hostname matches that of the current
            # configuration server.
            if found_server == False and \
                   socket.getfqdn(words[0]) == socket.getfqdn(address):
                try:
                    geometry = words[1]
                except IndexError:
                    geometry = "1200x1600+0+0"
                try:
                    background = words[2]
                except IndexError:
                    background = DEFAULT_BG_COLOR
                try:
                    if words[3] == "animate":
                        animate = 1
                    else:
                        animate = 0
                except IndexError:
                    animate = 1
                found_server = True

        if not found_server:
            #If it wasn't found raise this to set the defaults.
            if entvrc_data and len(words):
                raise IndexError(words[0])
            else:
                raise IndexError("Unknown")
    except (IOError, IndexError):
        geometry = "1200x1600+0+0"
        background = DEFAULT_BG_COLOR
        animate = 1

    rtn_dict = {}
    rtn_dict['geometry'] = geometry
    rtn_dict['background'] = background
    rtn_dict['animate'] = animate
    rtn_dict['library_colors'] = library_colors
    rtn_dict['client_colors'] = client_colors

    return rtn_dict

def set_entvrc(display, address):
    
    #If there isn't geometry don't do anything.
    master_geometry = getattr(display, "master_geometry", None)
    if master_geometry == None:
        return

    try:
        #Do this now to save the time to do the conversion for every line.
        csc_server_name = socket.getfqdn(address)

        #Get the current .entvrc file data if possible.
        try:
            data = get_entvrc_file()
        except (OSError, IOError), msg:
            #If the file exists but still failed to open (ie permissions)
            # then skip this step.
            if msg.errno != errno.ENOENT:
                Trace.trace(1, str(msg))
                return
            #But if it simply did not exist, then prepare to create it.
            else:
                data = []

        #use a temporary file incase something goes wrong.
        tmp_filename = get_entvrc_filename() + "." + str(os.getpid()) + ".tmp"
        tmp_file = open(tmp_filename, "w")

        #Make sure this gets written to file if not already there.
        new_line_written = 0

        #Loop through any existing data from the file.
        getfqdn = socket.getfqdn  #Speed up lookups.
        for line in data:
            #t1 = time.time()
            #Split the line into its individual words.
            words = line.split()

            #If the line is empty, write an empty line and continue.
            if not words:
                tmp_file.write("\n")   #Skip empty lines.
                continue

            if words[0] == "client_color" or words[0] == "library_color" \
               or line[0] == "#":
                tmp_file.write("%s\n" % (line,))

                #The reason for looking for client_color, library_color
                # and comment lines is to avoid calling (socket.)getfqdn().
                # It really slows things down.

            #If this is the correct line to update; update it.
            elif getfqdn(words[0]) == csc_server_name:

                #We can't assume a user that puts together there own
                # .entvrc file will do it correctly.
                try:
                    background = words[2]
                except IndexError:
                    background = DEFAULT_BG_COLOR
                try:
                    if display.master.entv_do_animation.get():
                        animate = str("animate")
                    else:
                        animate = str("still")
                except AttributeError:
                    animate = str("animate")
                #Write the new geometry to the .entvrc file.
                tmp_file.write("%-25s %-20s %-10s %-7s\n" %
                               (csc_server_name, master_geometry,
                                background, animate))

                new_line_written = 1
            else:
                tmp_file.write("%s\n" % (line,))

        #If the enstore system entv display is not found, add it at the end.
        if not new_line_written:
            tmp_file.write("%-25s %-20s %-10s\n" %
                         (csc_server_name, master_geometry, DEFAULT_BG_COLOR))
            
        tmp_file.close()

        entv_file = open(get_entvrc_filename(), "a")
        os.unlink(get_entvrc_filename())
        os.link(tmp_filename, get_entvrc_filename())
        os.unlink(tmp_filename)
        entv_file.close()
                  
    except (IOError, IndexError, OSError), msg:
        Trace.trace(1, str(msg))
        pass #If the line isn't there to begin with don't change anything.

#########################################################################
# entv functions
#########################################################################

#The parameters fullnames and with_system determine what strings are
# appended to each mover name.  Assume short mover name test01.  This
# is what would be returned if fullnames and with_system are python false.
#fullnames true -> test01.mover
#with_system true -> test01@test01
def get_mover_list(intf, csc, fullnames=None, with_system=None):

    #If a 'canned' (aka recorded) entv is running, read the movers file
    # for the list of movers.
    if intf.movers_file:
        try:
            mf_fp = open(intf.movers_file, "r")
            data = mf_fp.readlines()
            for i in range(len(data)):
                data[i] = data[i][:-1]
                #If fullnames is python true, then make sure that the
                # entire ".mover" appended names (as they appears in the
                # configuration file) are returned.
                if fullnames:
                    data[i] = data[i] + ".mover"
                if with_system:
                    data[i] = data[i] + "@" + DEFAULT_SYSTEM_NAME
            mf_fp.close()
            data.sort()
            return data  #Return from here on success.
        except (OSError, IOError), msg:
            print str(msg)
            sys.exit(1)
    elif intf.messages_file:
        return []

    if csc.new_config_obj.is_caching_enabled():
        #If necessary, cache the entire mover_list.
        config = csc.dump_and_save()
        enstore_system = csc.get_enstore_system(3, 3)

        #Get the list of library managers.
        lm_list = []
        for key in config.keys():
            if not enstore_functions2.is_library_manager(key):
                continue

            lm_name = key.split(".")[0]
            if lm_name not in string.split(intf.dont_show, ","):
                #Store the full library manager name.
                # i.e. 'name.library_manager' not just 'name'
                lm_list.append(key)

        #Get the list of movers.
        mover_list = []
        for key in config.keys():
            if not enstore_functions2.is_mover(key):
                continue

            #Create the type of name the user is looking for.
            if fullnames:
                m_name = key
            else:
                m_name = key.split(".")[0]  #Get the short name.
            if with_system:
                m_name = m_name + "@" + enstore_system

            #First obtain the library manager name the mover is
            # associated with.
            if type(config[key]['library']) == types.ListType:
                lm_name = config[key]['library'][0]
            else:
                lm_name = config[key]['library']
            #If the mover's library manager is on the don't show list,
            # do not include this in the list of things to do.
            if lm_name not in lm_list:
                continue

            #Append the request (type of) mover name to the list.
            mover_list.append(m_name)

        mover_list.sort()
        return mover_list

    #If necessary, do this the long way without using the cached config.

    #Get the list of library managers.
    lm_dict = csc.get_library_managers()
    lm_list = lm_dict.keys()

    #If the user selected to hide some movers, remove their LM from the list.
    for ds_lm_name in string.split(intf.dont_show, ","):
        if ds_lm_name in lm_list:
            del lm_list[lm_list.index(ds_lm_name)]

    #For each LM that should have its movers displayed, go through and
    # get each mover's name.
    movers = []
    for lm in lm_list:
        try:
            mover_list = csc.get_movers(lm_dict[lm]['name'])
        except (TypeError, NameError):
            #sys.stderr.write(str(lm_dict[lm]))
            continue

        try:
            for mover in mover_list:
                #If fullnames is python false, then make sure that the
                # ".mover" appended names (as they appears in the
                # configuration file) are truncated before being returned.
                if not fullnames:
                    mover_name = mover['mover'][:-6]
                else:
                    mover_name = mover['mover']

                #If the mover name is not in the list of mover names; add it.
                # If it already is, then don't add it another time.  This
                # can happen for movers belonging to multiple libraries.
                if mover_name not in movers:
                    movers = movers + [mover_name,]
        except (ValueError, TypeError, IndexError, KeyError):
            msg = sys.exc_info()[1]
            Trace.trace(1, "No movers found: %s" % str(msg))

    movers.sort()
    if with_system:
        #The following line is rejected by mylint.py.  This would be the
        # preferable way to do this.
        #movers = map(lambda m: m + "@" + enstore_system, movers)
        for i in range(len(movers)):
            movers[i] = movers[i] + "@" + enstore_system
    return movers

#Takes the name of the mover and the status dictionary.
def handle_status(mover, status):
    if mover == "inquisitor":
        offline_dict = status.get('offline', {})
        outage_dict = status.get('outage', {})
        send_error_list = {}  #Really a dictionary.
        for mover_name, outage in outage_dict.items():
            send_error_list[mover_name] = "error %s %s" % \
                                          (mover_name.split(".")[0], outage)
        for mover_name, reason in offline_dict.items():
            send_error_list[mover_name] = "error %s %s" % \
                                          (mover_name.split(".")[0], reason)
        return send_error_list.values()
    
    state = status.get('state','Unknown')
    time_in_state = status.get('time_in_state', '0')
    mover_state = "state %s %s %s" % (mover, state, time_in_state)
    volume = status.get('current_volume', None)
    client = status.get('client', "Unknown")
    connect = "connect %s %s" % (mover, client)
    error_status = status.get('status', ('Unknown', None))[0]
    if not volume:
        return [mover_state]
    if state in ['ACTIVE', 'SEEK', 'SETUP']:
        loaded = "loaded %s %s" % (mover, volume)
        return [loaded, mover_state, connect]
    if state in ['HAVE_BOUND', 'DISMOUNT_WAIT']:
        loaded = "loaded %s %s" % (mover, volume)
        return [loaded, mover_state]
    if state in ['MOUNT_WAIT']:
        loading = "loading %s %s" %(mover, volume)
        return [loading, mover_state, connect]
    if state in ['ERROR']:
        error = "error %s %s" %(mover, error_status)
        return [mover_state, error]

    return [mover_state]

#########################################################################
# The following functions run in their own thread.
#########################################################################

"""
def request_mover_status(display, csc, intf):
    global stop_now

    #If running from 'canned' version.
    if intf.movers_file:
        return

    enstore_system = None
    while enstore_system == None:
        enstore_system = csc.get_enstore_system(3, 3)

        #If the user said it needs to die, then die.  Don't wait for all of
        # the movers to be contacted.  If there is a known problem then
        # this could possibly take a while to time out with each of the
        # movers.
        if stop_now or display.stopped:
            return

    #Get the list of movers that this event relay will be reporting on.
    movers = get_mover_list(intf, csc, 1)

    #While still starting up, we don't need the main thread contacting the
    # movers too.  Grabing this lock will help with performace during startup.
    # Remeber this; don't put any returns in the following loop, otherwise
    # the lock will never get released.
    enstore_display.startup_lock.acquire()
    
    for mover in movers:
        #Get the mover client and the mover status.
        mov = mover_client.MoverClient(csc, mover,
                  flags=enstore_constants.NO_ALARM | enstore_constants.NO_LOG,)
        status = mov.status(rcv_timeout=5, tries=1)

        try:
            #If we added a route to the mover, we should remove it.
            # Most clients would prefer to leave such routes in place,
            # but entv is not your normal client.  It talks to many
            # movers that makes the routing table huge.
            host_config.unset_route(mov.server_address[0])
            pass
        except (socket.error, OSError):
            pass
        except TypeError:
            # mov.server_address is equal to None
            pass
        del mov

        #If the user said it needs to die, then die.  Don't wait for all of
        # the movers to be contacted.  If there is a known problem then
        # this could possibly take a while to time out with each of the
        # movers.
        if stop_now or display.stopped:
            break

        #Process the commands.
        mover_name = mover[:-6] + "@" + enstore_system
        commands = handle_status(mover_name, status)
        if not commands:
            continue
        for command in commands:
            #Queue the command.
            display.queue_command(command)

    #Never forget to release a lock.
    enstore_display.startup_lock.release()
"""

def start_messages_thread(csc_addr, system_name, intf):
    global messages_threads

    messages_threads.append(threading.Thread(
        target = handle_messages,
        args = (csc_addr, system_name, intf),
        ))
    messages_threads[-1].start()

    return

def stop_messages_threads():
    global messages_threads

    __pychecker__ = "unusednames=i"

    for i in range(len(messages_threads)):
        messages_threads[0].join()
        del messages_threads[0]

    messages_threads = []

    return

def send_mover_request(csc, send_request_dict, mover_name, u, count = 0):
    mover_name = mover_name.split("@")[0].split(".")[0]
    mover_name = mover_name + ".mover"
    
    #Get the message, mover name and mover network address
    # for sending the status request.
    m_addr = csc.get(mover_name, {}).get('hostip', None)
    m_port = csc.get(mover_name, {}).get('port', None)
    if not m_addr or not m_port:
        return

    message = {'work' : 'status'}
    mover_system_name = mover_name.split(".")[0]
    tx_id = u.send_deferred(message, (m_addr, m_port))
    send_request_dict[tx_id] = {}
    send_request_dict[tx_id]['name']  = mover_system_name
    send_request_dict[tx_id]['time']  = time.time()
    send_request_dict[tx_id]['count'] = count

def send_sched_request(csc, send_request_dict, u, count = 0):

    #Get the address for sending the scheduled down information request.
    i_addr = csc.get('inquisitor', {}).get('hostip', None)
    i_port = csc.get('inquisitor', {}).get('port', None)
    if not i_addr or not i_port:
        return

    message = {'work' : 'show'}
    tx_id = u.send_deferred(message, (i_addr, i_port))
    send_request_dict[tx_id] = {}
    send_request_dict[tx_id]['name']  = 'inquisitor'
    send_request_dict[tx_id]['time']  = time.time()
    send_request_dict[tx_id]['count'] = count

#handle_messages() reads event relay messages from the specified event
# relay.  It is called within a new thread (one for each event relay).
#
#display: is an instance of the enstore_display.Display class.
#event_relay_addr is a 2-tuple of ip/hostname and port number.
#intf is an instance of the EntvInterface class.
#def handle_messages(display, csc, intf):
def handle_messages(csc_addr, system_name, intf):
    global stop_now

    #Prevent the main thread from queuing status requests.
    enstore_display.startup_lock.acquire()

    #This is a time hack to get a clean output file.
    #timeout_time = time.time() + intf.capture_timeout

    send_request_dict = {}

    u = udp_client.UDPClient()
    
    # we will get all of the info from the event relay.
    if intf.messages_file:
        messages_file = open(intf.messages_file, "r")
        
        enstore_system = DEFAULT_SYSTEM_NAME
    else:
        if not csc_addr:
            return
        if type(csc_addr) != types.TupleType or len(csc_addr) != 2:
            return
        
        csc = configuration_client.ConfigurationClient(csc_addr)

        enstore_system = None
        while enstore_system == None:
            enstore_system = csc.get_enstore_system(3, 3)

            #If the user said it needs to die, then die.  Don't wait for all of
            # the movers to be contacted.  If there is a known problem then
            # this could possibly take a while to time out with each of the
            # movers.
            if stop_now: # or display.stopped:
                return

        er_dict = None
        while er_dict == None or not e_errors.is_ok(er_dict):
            try:
                er_dict = csc.get('event_relay', 3, 3)
            except SystemExit, msg:
                raise msg
            except:
                pass

            #If the user said it needs to die, then die.  Don't wait for all of
            # the movers to be contacted.  If there is a known problem then
            # this could possibly take a while to time out with each of the
            # movers.
            if stop_now: # or display.stopped:
                return

            if er_dict == None or e_errors.is_timedout(er_dict):
                continue

            if not e_errors.is_ok(er_dict):
                time.sleep(60)
                #display.queue_command("reinit")
                enstore_display.message_queue.put_queue("reinit",
                                                        enstore_system)
                return

            er_addr = (er_dict.get('hostip', None), er_dict.get('port', None))
            erc = event_relay_client.EventRelayClient(
                event_relay_host = er_addr[0], event_relay_port = er_addr[1])
            retval = erc.start([event_relay_messages.ALL])
            if retval == erc.ERROR:
                Trace.trace(0, "Could not contact event relay.")

            #Get the list of movers that we need to send status requests to.
            movers = get_mover_list(intf, csc, 1)

            for mover_name in movers:
                send_mover_request(csc, send_request_dict, mover_name, u)
                
        #If the client fails to initialize then wait a minute and start over.
        # The largest known error to occur is that socket.socket() fails
        # to return a file descriptor because to many files are open.
        if stop_now: # or display.stopped:
            return

    start = time.time()
    count = 0
    
    #Allow the main thread to queue status requests.
    enstore_display.startup_lock.release()
    
    while not stop_now: # and not display.stopped:

        #Send any requests to the mover.
        mover_name = enstore_display.request_queue.get_queue(enstore_system)
        #mover_name = display.get_request(enstore_system)
        if mover_name == 'inquisitor':
            send_sched_request(csc, send_request_dict, u)
        elif mover_name:
            send_mover_request(csc, send_request_dict, mover_name, u)
            
        # If commands are listed, use 'canned' version of entv.
        if intf.messages_file:
            try:
                #Get the next line from the commands list file.
                line = messages_file.readline()
                #For each line strip off the timestamp information from
                # the espion.py.
                command = string.join(line.split()[5:])
                if not command:
                    break  #Is this correct to break here?
                #Store the command into the list (in this case of 1).
                commands = [command]
            except (OSError, IOError, TypeError, ValueError,
                    KeyError, IndexError):
                messages_file.seek(0, 0) #Position at beginning of file.
                continue

            #Don't overwhelm the display thread.
            time.sleep(0.03)
            #while len(display.command_queue) > 50:
            while enstore_display.message_queue.len_queue(enstore_system) > 20:
                time.sleep(0.01)
        else:
            #test whether there is a command ready to read, timeout in
            # 1 second.
            try:
                readable, unused, unused = select.select([erc.sock,
                                                          u.get_tsd().socket],
                                                         [], [], 1)
            except select.error, msg:
                if msg.args[0] == errno.EINTR:
                    erc.unsubscribe()
                    erc.sock.close()
                    sys.stderr.write("Exiting early.\n")
                    sys.exit(1)

            #If nothing received for 60 seconds, resubscribe.
            #if count > 60:
            if count > 15:
                erc.subscribe()
                count = 0
            #Update counts and do it again.
            if not readable:
                count = count + 1
                continue

            commands = []

            #Read any status reports from movers.
            if u.get_tsd().socket in readable:
                for tx_id in send_request_dict.keys():
                    try:
                        mstatus = u.recv_deferred(tx_id, 0.0)
                        if not e_errors.is_ok(mstatus):
                            del send_request_dict[tx_id]
                            continue

                        commands = commands + handle_status(
                            send_request_dict[tx_id]['name'], mstatus)

                        del send_request_dict[tx_id]
                    except errno.errorcode[errno.ETIMEDOUT]:
                        pass
            #Remove items that are in the queue without having recieved a
            # responce.
            elif send_request_dict:
                for tx_id in send_request_dict.keys():
                    if send_request_dict[tx_id]['count'] > 5:
                        #If there has not been any responce after 25 seconds
                        # (5 x 5) then give up.
                        del send_request_dict[tx_id]
                    elif time.time() - send_request_dict[tx_id]['time'] > 5:
                        #If ther hasn't been a responce resend the request.
                        send_mover_request(csc, send_request_dict,
                                        send_request_dict[tx_id]['name'],
                                        u,
                                        send_request_dict[tx_id]['count'] + 1)
                        del send_request_dict[tx_id]
                    
            #Read the next message from the event relay.
            if erc.sock in readable:
                msg = enstore_erc_functions.read_erc(erc)

                if msg and not getattr(msg, "status", None):
                    #Take the message from event relay.
                    commands = commands + ["%s %s" % (msg.type,
                                                      msg.extra_info)]

                ##If read_erc is valid it is a EventRelayMessage instance. If
                # it gets here it is a dictionary with a status field error.
                elif getattr(msg, "status", None):
                    Trace.trace(1, msg["status"])
                    #continue
                #elif msg == None:
                #    continue

            if not commands:
                continue
            
        #Those commands that use mover names need to have the system name
        # appended to the name.
        for i in range(len(commands)):
            words = commands[i].split(" ")
            if words[0] in ("connect", "disconnect", "loaded", "loading",
                            "state", "unload", "transfer", "error"):
                if len(words[1].split("@")) == 1:
                    #If the name already has the enstore_system appended to
                    # the end (from messages_file) then don't do this step.
                    commands[i] = "%s %s %s" % (words[0],
                                               words[1] + "@" + enstore_system,
                                               string.join(words[2:], " "))

        put_func = enstore_display.message_queue.put_queue #Shortcut.
        for command in commands:
            #For normal use put everything into the queue.
            put_func(command, system_name)  #, enstore_system)
            
        #If necessary, handle resubscribing.
        if not intf.messages_file:
            now = time.time()
            if now - start > TEN_MINUTES:
                # resubscribe
                erc.subscribe()
                start = now

    #End nicely.
    if not intf.messages_file:
        #Tell the event relay to stop sending us information.
        erc.unsubscribe()

        #Remove all of the routes that were set up to all of the movers.
        for mover_name in movers:
            try:
                m_addr = csc.get(mover_name, {}).get('hostip', None)
                #If we added a route to the mover, we should remove it.
                # Most clients would prefer to leave such routes in place,
                # but entv is not your normal client.  It talks to many
                # movers that makes the routing table huge.
                host_config.unset_route(m_addr)
                pass
            except (socket.error, OSError):
                pass
            except TypeError:
                # mov.server_address is equal to None
                pass

    return

#########################################################################
# The following function sets the window geometry.
#########################################################################

#tk is the toplevel window.
def set_geometry(tk, entvrc_info):

    #Don't draw the window until all geometry issues have been worked out.
    #tk.withdraw()

    geometry = entvrc_info.get('geometry', None)
    window_width = entvrc_info.get('window_width', None)
    window_height = entvrc_info.get('window_height', None)
    x_position = entvrc_info.get('x_position', None)
    y_position = entvrc_info.get('y_position', None)
    
    #self.library_colors = entvrc_info.get('library_colors', {})
    #self.client_colors = entvrc_info.get('client_colors', {})

    #Use the geometry argument first.
    if geometry != None:
        window_width, window_height, x_position, y_position = \
                      enstore_display.split_geometry(geometry)

    #If the initial size is larger than the screen size, use the
    #  screen size.
    if window_width != None and window_height != None:
        window_width = min(tk.winfo_screenwidth(), window_width)
        window_height= min(tk.winfo_screenheight(), window_height)
    else:
        window_width = 0
        window_height = 0
    if x_position != None and y_position != None:
        x_position = max(min(tk.winfo_screenwidth(), x_position), 0)
        y_position = max(min(tk.winfo_screenheight(), y_position), 0)
    else:
        x_position = 0
        y_position = 0

    #Formulate the size and location of the window.
    geometry = "%sx%s+%s+%s" % (window_width, window_height,
                                x_position, y_position)
    #Set the geometry.
    tk.geometry(geometry)
    #Force the update.  Without this the window frame is not considered in
    # the geometry on reinitializations.  The effect without this is
    # that the window appears to migrate upward without human intervention.
    tk.update()

#########################################################################
#  Create menubar
#########################################################################

def create_menubar(menu_defaults, master):
    if not master:
        return
    
    #Menubar attributes.
    master.entv_menubar = Tkinter.Menu(master = master)
    #Options menu.
    master.entv_option_menu = Tkinter.Menu(tearoff = 0) #,master = self.entv_menubar)

    #Create the animate check button and set animate accordingly.
    master.entv_do_animation = Tkinter.BooleanVar()
    master.connection_color = Tkinter.IntVar()
    master.connection_color.set(menu_defaults['connection_color'])
    
    #By default animation is off.  If we need to turn animation, do so now.
    #if menu_defaults['animate'] == enstore_display.ANIMATE:
    master.entv_do_animation.set(menu_defaults['animate'])
                                
    #Add the checkbutton to the menu.
    ## Note: There is no way to obtain the actual checkbutton object.
    ## This would make accessing it internally do-able.
    ## 
    ## The only way to have the check in the checkbutton turned on
    ## by default is to have the BooleanVar variable be a member
    ## of the class.  Having it as a local variable does not want to
    ## work (though I don't know why that would be).
    master.entv_option_menu.add_checkbutton(
        label = "Animate",
        indicatoron = Tkinter.TRUE,
        onvalue = enstore_display.ANIMATE,
        offvalue = enstore_display.STILL,
        variable = master.entv_do_animation,
        command = toggle_animation,
        )
    master.entv_option_menu.add_separator()
    master.entv_option_menu.add_radiobutton(
        label = "Connections use client color",
        indicatoron = Tkinter.TRUE,
        value = enstore_display.CLIENT_COLOR,
        variable = master.connection_color,
        command = toggle_connection_color,
        )
    master.entv_option_menu.add_radiobutton(
        label = "Connections use library color",
        indicatoron = Tkinter.TRUE,
        value = enstore_display.LIBRARY_COLOR,
        variable = master.connection_color,
        command = toggle_connection_color,
        )
    
    #Added the menus to there respective parent widgets.
    master.entv_menubar.add_cascade(label = "options",
                                    menu = master.entv_option_menu)
    master.config(menu = master.entv_menubar)

def toggle_animation():
    global displays

    for display in displays:
        if display.master.entv_do_animation.get() == enstore_display.ANIMATE:
            if not display.after_smooth_animation_id:
                display.after_smooth_animation_id = display.after(
                    enstore_display.ANIMATE_TIME, display.smooth_animation)
        else: #enstore_display.STILL
            if display.after_smooth_animation_id:
                display.after_cancel(display.after_smooth_animation_id)
                display.after_smooth_animation_id = None
                

def toggle_connection_color():
    global displays
    
    #Update the colors for the connections all at once.  The other way to
    # do this would be to poll the value of
    # display.master.connection_color.get() for every frame of animation.
    # that would be a big waste of CPU.
    for display in displays:
        cc = display.master.connection_color.get()
        for connection in display.connections.values():
            connection.update_color(cc)

def resize(event = None):
    global displays

    for display in displays:
        #Recalculating this for each display is not efficent.
        size = display.master.geometry().split("+")[0]
        sizes = size.split("x")
        width = int(sizes[0]) / len(displays)
        height = int(sizes[1])

        display.configure(height = height, width = width)
    
#########################################################################
#  Interface class
#########################################################################

class EntvClientInterface(generic_client.GenericClientInterface):

    def __init__(self, args=sys.argv, user_mode=1):
        self.verbose = 0
        generic_client.GenericClientInterface.__init__(self, args=args,
                                                       user_mode=user_mode)

    def valid_dictionaries(self):
        return (self.help_options, self.entv_options)
    
    # parse the options like normal but make sure we have other args
    def parse_options(self):
        self.capture_timeout = 120  #seconds for capture.
        self.dont_show = ""
        self.verbose = 0
        self.display = os.environ.get("DISPLAY", ":0.0")
        self.generate_messages_file = 0
        self.movers_file = ""
        self.messages_file = ""
        self.profile = 0
        self.version = 0
        generic_client.GenericClientInterface.parse_options(self)
        
        #Setup the necessary cache global variables.
        #self.csc = get_system(self)

        #Setup trace levels.
        Trace.init("ENTV")
        for x in xrange(0, self.verbose + 1):
            Trace.do_print(x)
        if self.generate_messages_file:
            Trace.do_message(10)
    
    entv_options = {
        option.CAPTURE_TIMEOUT:{option.HELP_STRING:"Duration (in seconds) that"
                                " --generate-messages-file should display"
                                " new transfers. (default 120 seconds.)",
                                option.VALUE_USAGE:option.REQUIRED,
                                option.VALUE_TYPE:option.INTEGER,
                                option.USER_LEVEL:option.USER,},
        option.DISPLAY:{option.HELP_STRING:"Specify the screen entv should"
                                           " display to.",
                        option.VALUE_USAGE:option.REQUIRED,
                        option.VALUE_TYPE:option.STRING,
                        option.USER_LEVEL:option.USER,},
        option.DONT_SHOW:{option.HELP_STRING:"Don't display the movers that"
                          " belong to the specified library manager(s).",
                          option.VALUE_USAGE:option.REQUIRED,
                          option.VALUE_TYPE:option.STRING,
                          option.VALUE_LABEL:"LM short name,...",
                          option.USER_LEVEL:option.USER,},
        option.GENERATE_MESSAGES_FILE:{option.HELP_STRING:
                                     "Output to standard output the sequence"
                                     " of messages that create the display."
                                     "  This is done in a visually clean way.",
                                     option.VALUE_USAGE:option.IGNORED,
                                     option.USER_LEVEL:option.ADMIN,},
        option.MESSAGES_FILE:{option.HELP_STRING:
                              "Use 'canned' version of entv.",
                              option.VALUE_USAGE:option.REQUIRED,
                              option.VALUE_TYPE:option.STRING,
                              option.VALUE_LABEL:"messages_file",
                              option.USER_LEVEL:option.ADMIN,},
        option.MOVERS_FILE:{option.HELP_STRING:"Use 'canned' version of entv.",
                            option.VALUE_USAGE:option.REQUIRED,
                            option.VALUE_TYPE:option.STRING,
                            option.VALUE_LABEL:"movers_file",
                            option.USER_LEVEL:option.ADMIN,},
        option.PROFILE:{option.HELP_STRING:"Display profile info on exit.",
                            option.VALUE_USAGE:option.IGNORED,
                            option.USER_LEVEL:option.ADMIN,},
        option.VERBOSE:{option.HELP_STRING:"Print out information.",
                        option.VALUE_USAGE:option.REQUIRED,
                        option.VALUE_TYPE:option.INTEGER,
                        option.USER_LEVEL:option.USER,},
        option.VERSION:{option.HELP_STRING:
                        "Display entv version information.",
                        option.DEFAULT_TYPE:option.INTEGER,
                        option.DEFAULT_VALUE:1,
                        option.USER_LEVEL:option.USER,},
        }

#########################################################################
#  main
#########################################################################

def main(intf):
    global stop_now
    global displays

    if intf.movers_file or intf.messages_file:
        csc = None

        system_name = DEFAULT_SYSTEM_NAME
        title_name = DEFAULT_SYSTEM_NAME

        cscs_info = {}
        cscs = [None]
        mover_list = []
    else:
        # get a configuration server
        default_config_host = enstore_functions2.default_host()
        default_config_port = enstore_functions2.default_port()
        try:
            csc = configuration_client.ConfigurationClient(
                (default_config_host, default_config_port))
        except (socket.error,), msg:
            sys.stderr.write("Error contacting configuration server: %s\n" %
                             msg.args[1])
            sys.exit(1)
        rtn_tkt = csc.dump_and_save(timeout = 2, retry = 2)
        if not e_errors.is_ok(rtn_tkt):
            sys.stderr.write("Unable to contact configuration server: %s\n" %
                             str(rtn_tkt['status']))
            sys.exit(1)
        csc.new_config_obj.enable_caching()

        #cscs_info contains the known_config_servers section of the
        # configuration with all unspecified systems removed.
        cscs_info = get_all_systems(csc, intf)
        if not cscs_info:
            sys.stderr.write("Unable to find configuration server.\n")
            sys.exit(1)
        
        #Get the short name for the enstore system specified.
        system_name = get_system_name(intf, cscs_info)

        cscs = {}
        for system_name, address in cscs_info.items():
            cscs[system_name] = configuration_client.ConfigurationClient(address)
            try:
                cscs[system_name].dump_and_save()

                # Once, the enable_caching() function is called the
                # csc get() function is okay to use.
                cscs[system_name].new_config_obj.enable_caching()
            except:
                pass

        if len(cscs_info) == 1:
            title_name = cscs_info.keys()[0]
        else:
            title_name = "%s: %s" % ("Enstore", cscs_info.keys())

    #Get the main window.
    master = Tkinter.Tk(screenName = intf.display)
    master.withdraw()
    master.title(title_name)
    master.bind('<Configure>', resize)
    menu_defaults = {'animate' : enstore_display.STILL,
                     'connection_color' : enstore_display.CLIENT_COLOR }
    create_menubar(menu_defaults, master)
    
    continue_working = 1
    restart_entv = False
    display = None
    mover_display = None
    mem_in_use = -1 #Make this -1 to distinguish from None cases.

    while continue_working:

        #Set this to not stop.
        stop_now = 0
        
        #Get the entvrc file information
        if intf.movers_file:
            entvrc_dict = {}
            #entvrc_dict['title'] = "Enstore"
        else:
            entvrc_dict = get_entvrc(csc, intf)
            #entvrc_dict['title'] = system_name #For simplicity put this here.

        #Set the size of the window.
        set_geometry(master, entvrc_dict)

        """
        if intf.messages_file:
            system_names = "Enstore"
            csc_addrs = [None]
        else:
            system_names = cscs_info.keys()
            csc_addrs = cscs_info.values()
        """

        size = entvrc_dict['geometry'].split("+")[0]
        sizes = size.split("x")
        width = int(sizes[0]) / len(cscs_info)
        height = int(sizes[1])
        
        displays = []
        for system_name, csc_addr in cscs_info.items():
            display = enstore_display.Display(entvrc_dict, system_name,
                                              master = master,
                                              width = width,
                                              height = height,
                                              mover_display = mover_display,
                             background = entvrc_dict.get('background', None))

            display.pack(side = Tkinter.LEFT, fill = Tkinter.BOTH,
                         expand = Tkinter.YES)
            #This function can be called for 'canned' entv too.
            # This is because a None value is inserted into the
            # cscs list.
            display.handle_command("csc %s %s" % csc_addr)

            if not intf.messages_file:
                #Obtain the list of all movers.
                mover_list = get_mover_list(intf, cscs[system_name], 0, 1)
                mover_list = ["movers"] + mover_list
                movers_command = string.join(mover_list, " ")
                
                #Inform the display the names of all the movers.
                display.handle_command(movers_command)
            
            displays.append(display)

        #If we want a clean commands file, we need to set the inital movers
        # state to idle.
        if intf.generate_messages_file:
            for mover in display.movers.keys():
                idle_command = string.join(["state", mover, "IDLE"], " ")
                display.handle_command(idle_command)

        Trace.trace(1, "starting threads")
        
        #On average collecting the status of all the movers takes 10-15
        # seconds.  We don't want to wait that long.  This can be done
        # in parallel to displaying live data.

        #First acquire the startup lock.  This will delay the other threads
        # from consuming resources that would be better spent on this
        # thread at the moment.  This lock is released inside of
        # enstore_display.mainloop().
        enstore_display.startup_lock.acquire()
        
        if intf.messages_file:
            #Read from file the event relay messages to process.
            start_messages_thread(None, "Enstore", intf)
        else:
            #Start a thread for each event relay we should contact.
            for system_name, csc in cscs.items():
                start_messages_thread((csc.server_address[0],
                                       csc.server_address[1]),
                                      system_name, intf)

        #Let the other startup threads go.
        enstore_display.startup_lock.release()

        #for dis in displays:
        #    dis.startup()

        master.deiconify()
        master.lift()
        #master.update()

        # This would be a good time to cleanup before things get hairy.
        gc.collect()
        del gc.garbage[:]

        #Loop until user says don't.
        master.mainloop()

        #Tell other thread(s) to stop.
        stop_now = 1

        #Cleanup the display.
        continue_working = 0
        for display in displays:
            #Determine if this is a reinitialization (True) or not (False).
            continue_working = continue_working + display.attempt_reinit()

            display.cleanup_display()
            display.shutdown()
            del display.mover_display
            #Forces cleanup of objects that would not happen otherwise.  As
            # part of the destroy call Tkinter does generate a Destroy event
            # that results in window_killed() being called.  Skipping a
            # destroy() function call would result in a huge memory leak.
            try:
                display.destroy()
            except Tkinter.TclError:
                #It might already be destroyed (i.e. window closed by user).
                pass

        #Set the geometry of the .entvrc file (if necessary).
        address = cscs_info[displays[0].system_name][0]
        set_entvrc(displays[0], address)

        #Wait for the other threads to finish.
        Trace.trace(1, "waiting for threads to stop")
        stop_messages_threads()
        Trace.trace(1, "message thread finished")

        #Reclaim all of display's resources now.
        del displays[:]
        del entvrc_dict
        del mover_list[:]
        #Don't move the following into threads in enstore_display functions.
        # There are wierd references that prevent them from being reclaimed
        # by the garbage collector.
        enstore_display.message_queue.clear_queues()
        enstore_display.request_queue.clear_queues()

        #Force reclaimation of memory (and other resources) and also
        # report if leaks are occuring.
        restart_entv = cleanup_objects()

        if mem_in_use == -1:
            #Only do this the first time through.
            mem_in_use = memory_in_use()
        elif mem_in_use != None and mem_in_use != -1:
            #Do this check for all loops after the first.
            current_mem_used = memory_in_use()
            if mem_in_use != None and current_mem_used != None \
               and current_mem_used > 2 * mem_in_use:
                #If the memory size is twice what it was, start a new
                # process so we don't take up to much memory.
                restart_entv = True

        if continue_working and restart_entv:
            #At this point a lot of objects have been unable to be freed.
            # Thus, we should re-exec() the entv process.
            Trace.trace(0, "Starting new entv process.")
            os.execv(sys.argv[0], sys.argv)

        if continue_working:
            master.update()
            
            #As long as we are reinitializing, make sure we pick up any
            # new configuration changes.  It is possible that the
            # reinialization is happening because a NEWCONFIGFILE message
            # was received; among other reasons.
            if csc:
                csc.dump_and_save()


        
if __name__ == "__main__":

    delete_at_exit.setup_signal_handling()

    intf = EntvClientInterface(user_mode=0)

    if intf.profile:
        #If the user wants to see the profile of entv...
        import profile
        import pstats
        profile.run("main(intf)", "/tmp/entv_profile")
        p = pstats.Stats("/tmp/entv_profile")
        p.sort_stats('cumulative').print_stats(100)
    elif intf.version:
        #Just print the version of entv.
        print entv_client_version()
    else:
        main(intf)
