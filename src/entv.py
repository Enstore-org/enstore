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
#import pprint
import threading
import re
import rexec
#import signal
import errno
import gc
import types


#enstore imports
import enstore_display
import configuration_client
import enstore_functions2
import enstore_constants
import enstore_erc_functions
import e_errors
import mover_client
import event_relay_messages
import event_relay_client
#import udp_client
#import setpath
import Trace
import generic_client
import option
import delete_at_exit
import host_config

#4-30-2002: For reasons unknown to me the order of the imports matters a lot.
# If the wrong order is done, then the dashed lines are not drawn.
#3-5-2003: Update to previous comment.  Tkinter must be imported after
# importing enstore_display.  I still don't know why this is.
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

status_thread = None
messages_thread = None

#Should we need to stop (ie. cntl-C) this is the global flag.
stop_now = 0

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
        sys.stderr.write("Unknown error.  Aborting\n")
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
                del config_servers[name]

    #Special section for test systems that are not in their own
    # config file's 'known_config_servers' section.
    if not config_servers:
        ip = socket.gethostbyname(config_host)
        addr_info = socket.gethostbyaddr(ip)
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

            if socket.getfqdn(words[0]) == socket.getfqdn(address):
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
                break
        else:
            #If it wasn't found raise this to set the defaults.
            if entvrc_data and len(words):
                raise IndexError(words[0])
            else:
                raise IndexError("Unknown")
    except (IOError, IndexError):
        geometry = "1200x1600+0+0"
        background = DEFAULT_BG_COLOR
        animate = 1

    library_colors = {}
    client_colors = {}

    #Pass through the file looking for library color lines.
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

    rtn_dict = {}
    rtn_dict['geometry'] = geometry
    rtn_dict['background'] = background
    rtn_dict['animate'] = animate
    rtn_dict['library_colors'] = library_colors
    rtn_dict['client_colors'] = client_colors

    return rtn_dict

def set_entvrc(display, csc, intf):
    
    #If there isn't geometry don't do anything.
    master_geometry = getattr(display, "master_geometry", None)
    if master_geometry == None:
        return

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
        for line in data:
            #Split the line into its individual words.
            words = line.split()

            #If the line is empty, write an empty line and continue.
            if not words:
                tmp_file.write("\n")   #Skip empty lines.
                continue

            #If this is the correct line to update; update it.
            if socket.getfqdn(words[0]) == csc_server_name:
                #We can't assume a user that puts together there own
                # .entvrc file will do it correctly.
                try:
                    background = words[2]
                except IndexError:
                    background = DEFAULT_BG_COLOR
                try:
                    if display.animate.get():
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
                tmp_file.write(line + "\n")

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
        except TypeError:
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

def handle_status(mover, status):
    state = status.get('state','Unknown')
    time_in_state = status.get('time_in_state', '0')
    mover_state = "state %s %s %s" % (mover, state, time_in_state)
    volume = status.get('current_volume', None)
    client = status.get('client', "Unknown")
    connect = "connect %s %s" % (mover, client)
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

    return [mover_state]

#########################################################################
# The following functions run in their own thread.
#########################################################################

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

#handle_messages() reads event relay messages from the specified event
# relay.  It is called within a new thread (one for each event relay).
#
#display: is an instance of the enstore_display.Display class.
#event_relay_addr is a 2-tuple of ip/hostname and port number.
#intf is an instance of the EntvInterface class.
def handle_messages(display, csc, intf):
    global stop_now

    #This is a time hack to get a clean output file.
    timeout_time = time.time() + intf.capture_timeout
    
    # we will get all of the info from the event relay.
    if intf.messages_file:
        messages_file = open(intf.messages_file, "r")
        
        enstore_system = DEFAULT_SYSTEM_NAME
    else:
        enstore_system = None
        while enstore_system == None:
            enstore_system = csc.get_enstore_system(3, 3)

            #If the user said it needs to die, then die.  Don't wait for all of
            # the movers to be contacted.  If there is a known problem then
            # this could possibly take a while to time out with each of the
            # movers.
            if stop_now or display.stopped:
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
            if stop_now or display.stopped:
                return

            if er_dict == None or e_errors.is_timedout(er_dict):
                continue

            if not e_errors.is_ok(er_dict):
                time.sleep(60)
                display.queue_command("reinit")
                return

            er_addr = (er_dict.get('hostip', None), er_dict.get('port', None))
            erc = event_relay_client.EventRelayClient(
                event_relay_host = er_addr[0], event_relay_port = er_addr[1])
            erc.start([event_relay_messages.ALL])
        
        #If the client fails to initialize then wait a minute and start over.
        # The largest known error to occur is that socket.socket() fails
        # to return a file descriptor because to many files are open.
        if stop_now or display.stopped:
            return

    start = time.time()
    count = 0

    while not display.stopped and not stop_now:

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
            except (OSError, IOError, TypeError, ValueError,
                    KeyError, IndexError):
                messages_file.seek(0, 0) #Position at beginning of file.
                continue

            #Don't overwhelm the display thread.
            time.sleep(0.03)
            while len(display.command_queue) > 50:
                time.sleep(0.01)
        else:
            #test whether there is a command ready to read, timeout in
            # 1 second.
            try:
                readable, unused, unused = select.select([erc.sock], [], [], 1)
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

            #for fd in readable:
            msg = enstore_erc_functions.read_erc(erc)

            if msg and not getattr(msg, "status", None):
                #Take the message from event relay.
                command = "%s %s" % (msg.type, msg.extra_info)
            
            ##If read_erc is valid it is a EventRelayMessage instance. If
            # it gets here it is a dictionary with a status field error.
            elif getattr(msg, "status", None):
                Trace.trace(1, msg["status"])
                continue
            elif msg == None:
                continue

        #Those commands that use mover names need to have the system name
        # appended to the name.
        words = command.split(" ")
        if words[0] in ("connect", "disconnect", "loaded", "loading", "state",
                        "unload", "transfer"):
            if len(words[1].split("@")) == 1:
                #If the name already has the enstore_system appended to the
                # end (from messages_file) then don't do this step.
                command = "%s %s %s" % (words[0],
                                        words[1] + "@" + enstore_system,
                                        string.join(words[2:], " "))

        ############# HACK ###############################################
        #When writing the messages file, special attention needs to be
        # given to make the "ending" clean.
        if intf.generate_messages_file:
            words = command.split(" ")
            if words[0] in ("connect", "disconnect", "loaded", "loading",
                            "state", "unload", "transfer"):
                try:
                    if time.time() > timeout_time \
                           and display.movers[words[1]].state == "HAVE_BOUND":
                        words2 = ["state", words[1], "DISMOUNT_WAIT"]
                        display.queue_command(string.join(words2, " "))
                        #Use a dummny name for the volume.  Obtaining the
                        # real name can cause a race condition leading to
                        # a traceback.  enstore_display.unload_command()
                        # does not even look at the volume name anyway.
                        words2 = ["unload", words[1], "dummy"]
                        display.queue_command(string.join(words2, " "))
                        words2 = ["state", words[1], "IDLE"]
                        display.queue_command(string.join(words2, " "))
                    elif words[0] == "state" \
                       and words[2] in \
                       ("MOUNT_WAIT", "DISMOUNT_WAIT", "HAVE_BOUND",
                        "FINISH_WRITE") \
                       and display.movers[words[1]].state == "IDLE":
                        pass
                    elif words[0] in ("loaded", "loading", "transfer") \
                         and display.movers[words[1]].state == "IDLE":
                        pass
                    elif time.time() < timeout_time:
                        display.queue_command(command)
                    elif display.movers[words[1]].state != "IDLE":
                        display.queue_command(command)
                except KeyError:
                    #A KeyError can occur if the main window has been closed
                    # and the main thread has already cleared the
                    # display.movers list.
                    pass
            else:
                display.queue_command(command)
        ##################################################################
        else:
            #For normal use put everything into the queue.
            display.queue_command(command)
            
        #If necessary, handle resubscribing.
        if not intf.messages_file:
            now = time.time()
            if now - start > TEN_MINUTES:
                # resubscribe
                erc.subscribe()
                start = now

    #End nicely.
    if not intf.messages_file:
        erc.unsubscribe()

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
        window_width = int(re.search("^[0-9]+", geometry).group(0))
        window_height = re.search("[x][0-9]+", geometry).group(0)
        window_height = int(window_height.replace("x", " "))
        x_position = re.search("[+][-]{0,1}[0-9]+[+]", geometry).group(0)
        x_position = int(x_position.replace("+", ""))
        y_position = re.search("[+][-]{0,1}[0-9]+$", geometry).group(0)
        y_position = int(y_position.replace("+", ""))

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

    global status_thread, messages_thread
    global stop_now

    if intf.movers_file or intf.messages_file:
        csc = None

        system_name = DEFAULT_SYSTEM_NAME
    else:
        # get a configuration server
        default_config_host = enstore_functions2.default_host()
        default_config_port = enstore_functions2.default_port()
        csc = configuration_client.ConfigurationClient((default_config_host,
                                                        default_config_port))
        csc.dump_and_save()
        csc.new_config_obj.enable_caching()

        #cscs_info contains the known_config_servers section of the
        # configuration with all unspecified systems removed.
        cscs_info = get_all_systems(csc, intf)
        if not cscs_info:
            sys.stderr.write("Unable to find configuration server.\n")
            sys.exit(1)

        #Get the short name for the enstore system specified.
        system_name = get_system_name(intf, cscs_info)

    #Get the main window.
    master = Tkinter.Tk()
    master.withdraw()
    
    continue_working = 1

    while continue_working:
        #Get the entvrc file information
        if intf.movers_file:
            entvrc_dict = {}
            #entvrc_dict['title'] = "Enstore"
        else:
            entvrc_dict = get_entvrc(csc, intf)
            entvrc_dict['title'] = system_name #For simplicity put this here.

        #Set the size of the window.
        set_geometry(master, entvrc_dict)
        
        display = enstore_display.Display(entvrc_dict, master = master,
                              background = entvrc_dict.get('background', None))

        #Inform the display the config server to use.  Don't do
        # this if running 'canned' entv.  Make sure this is run
        # before the movers_command is.
        if intf.movers_file or intf.messages_file:
            mover_list = get_mover_list(intf, None, 0, 1)

            cscs = [None]
        else:
            cscs = []
            mover_list = []
            for address in cscs_info.values():
                cscs.append(configuration_client.ConfigurationClient(address))

                try:
                    #Inform the display the config server to use.  Don't do
                    # this if running 'canned' entv.  Make sure this is run
                    # before the movers_command is.
                    display.handle_command(
                        "csc %s %s" % (cscs[-1].server_address[0],
                                       cscs[-1].server_address[1]))
                except:
                    pass

                try:
                    cscs[-1].dump_and_save()

                    # Once, the enable_caching() function is called the
                    # csc get() function is okay to use.
                    cscs[-1].new_config_obj.enable_caching()

                    #Append the new movers to the end of the list.
                    mover_list = mover_list + get_mover_list(intf, cscs[-1],
                                                             0, 1)
                except:
                    pass

        #Inform the display the names of all the movers.
        movers_command = "movers" + " " + string.join(mover_list, " ")
        display.handle_command(movers_command)

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
        
        #Start a thread for each event relay we should contact.
        status_threads = []
        messages_threads = []
        
        for i in range(len(cscs)):
            if not intf.generate_messages_file and not intf.messages_file:
                #If we are in normal running mode start the treads that
                # will get the initial status of the movers.
                status_threads.append(threading.Thread(group=None,
                                                  target=request_mover_status,
                                                       name='',
                                                       args=(display,
                                                             cscs[i], intf),
                                                       kwargs={}))
                status_threads[-1].start()

            messages_threads.append(threading.Thread(group=None,
                                                     target=handle_messages,
                                                     name='',
                                                     args=(display,
                                                           cscs[i], intf),
                                                     kwargs={}))
            messages_threads[-1].start()

        #Loop until user says don't.
        display.mainloop()
        
        #Set the geometry of the file (if necessary).
        set_entvrc(display, csc, intf)

        #Wait for the other threads to finish.
        Trace.trace(1, "waiting for threads to stop")
        for i in range(len(status_threads)):
            status_threads[i].join()
        Trace.trace(1, "status thread finished")
        for i in range(len(messages_threads)):
            messages_threads[i].join()
        Trace.trace(1, "message thread finished")

        #Determin if this is a reinitialization (True) or not (False).
        continue_working = ( not display.stopped or display.attempt_reinit() )\
                           and not stop_now

        try:
            display.destroy()
        except Tkinter.TclError:
            pass #If the window is already destroyed (i.e. user closed it)
                 # then this error will occur.
        del display

        #Force garbage collection while the display is off while awaiting
        # initialization.
        gc.collect()
        uncollectable_count = len(gc.garbage)
        del gc.garbage[:]
        if uncollectable_count > 0:
            Trace.trace(0, "UNCOLLECTABLE COUNT: %s" % uncollectable_count)

        if continue_working:
            #As long as we are reinitializing, make sure we pick up any
            # new configuration changes.  It is possible that the
            # reinialization is happening because a NEWCONFIGFILE message
            # was received; among other reasons.
            if csc:
                csc.dump_and_save()

    #Perform the following two deletes explicitly to avoid obnoxious
    # tkinter warning messages printed to the terminal when using
    # python 2.2.
    try:
        del enstore_display._font_cache
    except:
        exc, msg = sys.exc_info()[:2]
        Trace.trace(1, "ERROR: %s: %s" % (str(exc), str(msg)))
    try:
        del enstore_display._image_cache
    except:
        exc, msg = sys.exc_info()[:2]
        Trace.trace(1, "ERROR: %s: %s" % (str(exc), str(msg)))

        
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
