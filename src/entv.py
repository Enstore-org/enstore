#!/usr/bin/env python

# $Id$

#4-30-2002: For reasons unknown to me the order of the imports matters a lot.
# If the wrong order is done, then the dashed lines are not drawn.
import os
import sys
import socket
import select
import string
import time
import threading
import enstore_display
import configuration_client
import enstore_functions2
import enstore_erc_functions
import e_errors
import mover_client
import pprint
import event_relay_messages
import event_relay_client
import udp_client
import setpath
import rexec
import signal
import errno
import Tkinter
import Trace

#########################################################################
# Globals
#########################################################################

_rexec = rexec.RExec()
def eval(stuff):
    return _rexec.r_eval(stuff)

TEN_MINUTES=600   #600seconds = 10minutes
DEFAULT_BG_COLOR = '#add8e6'   #light blue

status_thread = None
messages_thread = None

#debug=1

_csc = None
_config_cache = None

#Should we need to stop (ie. cntl-C) this is the global flag.
stop_now = 0

#A lock to allow only one thread at a time access the display class instance.
#display_lock = threading.Lock()

#########################################################################
# common support functions
#########################################################################

def endswith(s1,s2):
    return s1[-len(s2):] == s2

def signal_handler(sig, frame):
    global status_thread, messages_thread
    global stop_now

    try:
        if sig != signal.SIGTERM and sig != signal.SIGINT:
            sys.stderr.write("Signal caught at: ", frame.f_code.co_filename,
                             frame.f_lineno);
            sys.stderr.flush()
    except:
        pass
    
    try:
        sys.stderr.write("Caught signal %s, exiting\n" % (sig,))
        sys.stderr.flush()
    except:
        pass

    #flag the threads to stop.
    stop_now = 1
    
    thread = threading.currentThread()
    if thread != status_thread:
        status_thread.join()
    if thread != messages_thread:
        messages_thread.join()
    sys.exit(0)

def dict_eval(data):
    ##This is like "eval" but it assumes the input is a
    ## dictionary; any trailing junk will be ignored.
    last_brace = string.rindex(data, '}')
    try:
        d = eval(data[:last_brace+1])
    except:
        print "Error", data,
        d = {}
    return d

def get_csc():
    global _csc
    if _csc:  #used cached version.
        return _csc

    config_port = enstore_functions2.default_port()
    if len(sys.argv) > 1:
        config_host = sys.argv[1]
    else:
        config_host = enstore_functions2.default_host()
        #config_host = os.environ.get("ENSTORE_CONFIG_HOST")

    default_config_host = enstore_functions2.default_host()
    default_config_port = enstore_functions2.default_port()

    # get a configuration server
    csc = configuration_client.ConfigurationClient((default_config_host,
                                                    default_config_port))
    #Get the list of all config servers and remove the 'status' element.
    config_servers = csc.get('known_config_servers', {})
    if config_servers['status'][0] == e_errors.OK:
        del config_servers['status']

    #Based on the config file determine which config server was specified.
    for name in config_servers.keys():
        if len(config_host) >= len(name) and \
           config_host[:len(name)] == name:
            config_host = config_servers[name][0]

    _csc = configuration_client.ConfigurationClient((config_host, config_port))

    return _csc

def get_config():
    global _config_cache
    if _config_cache:
        return _config_cache
    csc = get_csc()
    _config_cache = csc.dump()
    return _config_cache

def get_system_name():
    csc = get_csc()
    try:
        hostinfo = socket.gethostbyaddr(csc.server_address[0])[0]
    except socket.error, msg:
        sys.stderr.write(str(msg) + "/n")
        sys.exit(1)
    event_relay_host = hostinfo
    system_name = hostinfo

    config_servers = csc.get('known_config_servers', {})
    if config_servers['status'][0] == e_errors.OK:
        del config_servers['status']

    #Based on the config file determine which config server was specified.
    for name in config_servers.keys():
        if config_servers[name][0] == event_relay_host:
            system_name = name

    event_relay_port = 55510
    os.environ['ENSTORE_CONFIG_HOST'] = event_relay_host
    event_relay_addr = (event_relay_host, event_relay_port)

    #return event_relay_addr, system_name
    return system_name

#########################################################################
# .entrc file related functions
#########################################################################

def get_entvrc_filename():
    return os.environ["HOME"] + "/.entvrc"

def get_entvrc():
        #Variables and files to look for.
    f = open(get_entvrc_filename())
    lines = []
    for line in f.readlines():
        lines.append(line.strip())

    ###Don't remove blank lines and command lines from the output.  This
    ### output is used by the set_entvrc function which will use these
    ### extraneous lines.

    return lines

def get_geometry():
    try:
        csc = get_csc()
        for line in get_entvrc():
            if line[0] == "#": #Skip comment lines.
                continue
            words = line.split()
            if socket.getfqdn(words[0])==socket.getfqdn(csc.server_address[0]):
                geometry = words[1]
                try:
                    background = words[2]
                except IndexError:
                    background = DEFAULT_BG_COLOR
                break
        else:
            #If it wasn't found raise this to set the defaults.
            raise IndexError(words[0])
    except (IOError, IndexError):
        geometry = "700x1600+0+0"
        background = DEFAULT_BG_COLOR

    return geometry, background

def set_geometry(geometry):
    try:
        csc = get_csc()
        #Do this now to save the time to do the conversion for every line.
        csc_server_name = socket.getfqdn(csc.server_address[0])

        #Get the current .entvrc file data if possible.
        try:
            data = get_entvrc()
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
        tmp_filename = get_entvrc_filename() + ".tmp"
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

                #Write the new geometry to the .entvrc file.
                tmp_file.write("%-25s %-20s %-10s\n" %
                               (csc_server_name, geometry, background))

                new_line_written = 1
            else:
                tmp_file.write(line + "\n")

        #If the enstore system entv display is not found, add it at the end.
        if not new_line_written:
            tmp_file.write("%-25s %-20s %-10s\n" %
                           (csc_server_name, geometry, DEFAULT_BG_COLOR))
            
        tmp_file.close()

        entv_file = open(get_entvrc_filename(), "a")
        os.unlink(get_entvrc_filename())
        os.link(tmp_filename, get_entvrc_filename())
        os.unlink(tmp_filename)
                  
    except (IOError, IndexError, OSError), msg:
        Trace.trace(1, str(msg))
        pass #If the line isn't there to begin with don't change anything.

#########################################################################
# entv functions
#########################################################################

def get_mover_list(fullnames=None):
    movers = []
    csc = get_csc()

    lm_dict = csc.get_library_managers({})
    for lm in lm_dict.keys():
        try:
            mover_list = csc.get_movers(lm_dict[lm]['name'])
        except TypeError:
            print lm_dict[lm]
            exc, msg, tb = sys.exc_info()
            raise exc, msg, tb
        try:
            for mover in mover_list:
                if not fullnames:
                    movers = movers + [mover['mover'][:-6]]
                else:
                    movers = movers + [mover['mover']]
        except:
            exc, msg, tb = sys.exc_info()
            Trace.trace(1, "No movers found: %s" % str(msg))
    movers.sort()

    return movers

def subscribe(event_relay_addr):
    erc = event_relay_client.EventRelayClient(
        event_relay_host=event_relay_addr[0],
        event_relay_port=event_relay_addr[1])
    erc.start([event_relay_messages.ALL,])
    erc.subscribe()

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

def request_mover_status(display):
    global stop_now

    csc = get_csc()
    config = get_config()
    movers = get_mover_list(1)

    for mover in movers:
        #Get the mover client and the mover status.
        mov = mover_client.MoverClient(csc, mover)
        status = mov.status(rcv_timeout=5, tries=1)

        #If the user said it needs to die, then die.  Don't wait for all of
        # the movers to be contacted.  If there is a known problem then this
        # could possibly take a while to time out with each of the movers.
        if stop_now or display.stopped:
            return

        #Process the commands.
        commands = handle_status(mover[:-6], status)
        if not commands:
            continue
        for command in commands:
            #Queue the command.
            display.queue_command(command)

def handle_messages(display):
    global stop_now

    # we will get all of the info from the event relay.
    erc = event_relay_client.EventRelayClient()
    erc.start([event_relay_messages.ALL,])
    erc.subscribe()
    
    start = time.time()
    count = 0

    while not display.stopped and not stop_now:

        #test whether there is a command ready to read, timeout in
        # 1 second.
        try:
            readable, junk, junk = select.select([erc.sock], [], [], 1)
        except select.error:
            exc, msg, tb = sys.exc_info()
            if msg.args[0] == errno.EINTR:
                erc.unsubscribe()
                erc.sock.close()
                sys.stderr.write("Exiting early.\n")
                sys.exit(1)

        #If nothing received for 60 seconds, resubscribe.
        if count > 60:
            erc.subscribe()
            count = 0
        count = count + 1            
        if not readable:
            continue

        now = time.time()
        for fd in readable:
            msg = enstore_erc_functions.read_erc(erc)
            if msg and not getattr(msg, "status", None):
                command="%s %s" % (msg.type, msg.extra_info)
                Trace.trace(1, command)
                display.queue_command(command)

            ##If read_erc is valid it is a EventRelayMessage instance. If
            # it gets here it is a dictionary with a status field error.
            elif getattr(msg, "status", None):
                    Trace.trace(1, msg["status"])

        if now - start > TEN_MINUTES:
            # resubscribe
            erc.subscribe()
            start = now

    #End nicely.
    erc.unsubscribe()
    erc.sock.close()

#########################################################################
#  main
#########################################################################

def main():
    global status_thread, messages_thread
    global stop_now

    for sig in range(1, signal.NSIG):
        if sig not in (signal.SIGTSTP, signal.SIGCONT,
                       signal.SIGCHLD, signal.SIGWINCH):
            try:
                signal.signal(sig, signal_handler)
            except:
                sys.stderr.write("Setting signal %s to %s failed.\n" %
                                 (sig, signal_handler))
    
    system_name = get_system_name()

    geometry, background = get_geometry()

    display = enstore_display.Display(title=system_name,
                                      geometry=geometry, background=background)

    while ( not display.stopped or display.attempt_reinit() ) and not stop_now:

        display.reinit()

        #initalize the movers.
        movers = get_mover_list(0)
        #movers_command = "movers"
        #for mover in movers:
        #    movers_command = movers_command + " " + mover[:-6]
        movers_command = "movers " + string.join(movers, " ")
            
        #Inform the display the names of all the movers.
        display.handle_command(movers_command)

        #On average collecting the status of all the movers takes 10-15
        # seconds.  We don't want to wait that long.  This can be done
        # sychronously to displaying live data.
        status_thread = threading.Thread(group=None,
                                         target=request_mover_status,
                                         name='', args=(display,), kwargs={})
        status_thread.start() #wait for movers to sends status seperately.

        messages_thread=threading.Thread(group=None,
                                         target=handle_messages,
                                         name='', args=(display,), kwargs={})
        messages_thread.start()

        #Loop until user says don't.
        display.mainloop()

        if hasattr(display, "geometry") and display.geometry != None:
            set_geometry(display.geometry)

        Trace.trace(1, "waiting for threads to stop")
        status_thread.join()
        Trace.trace(1, "status thread finished")
        messages_thread.join()
        Trace.trace(1, "message thread finished")

if __name__ == "__main__":

    if "--debug" in sys.argv or "-d" in sys.argv:
        Trace.init("ENTV")
        for x in xrange(0, 10):
            Trace.do_print(x)
        main()
    elif "--profile" in sys.argv or "-p" in sys.argv:
            import profile
            import pstats
            profile.run("main()", "/tmp/entv_profile")
            p=pstats.Stats("/tmp/entv_profile")
            p.sort_stats('cumulative').print_stats(100)
            
    else:
        main()

