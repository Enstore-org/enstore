#!/usr/bin/env python

# $Id$

import os
import sys
import socket
import select
import string
import time
import threading
import enstore_display
import configuration_client
import enstore_functions
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

_rexec = rexec.RExec()
def eval(stuff):
    return _rexec.r_eval(stuff)

TEN_MINUTES=600   #600seconds = 10minutes

status_thread = None
messages_thread = None
periodic_thread = None

debug=1

_csc = None
_config_cache = None

#Should we need to stop (ie. cntl-C) this is the global flag.
stop_now = 0

#A lock to allow only one thread at a time access the display class instance.
display_lock = threading.Lock()

def endswith(s1,s2):
    return s1[-len(s2):] == s2

def signal_handler(sig, frame):
    global status_thread, periodic_thread, messages_thread
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
    status_thread.join()
    periodic_thread.join()
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

    config_port = enstore_functions.default_port()
    if len(sys.argv) > 1:
        config_host = sys.argv[1]
    else:
        config_host = enstore_functions.default_host()
        #config_host = os.environ.get("ENSTORE_CONFIG_HOST")

    default_config_host = enstore_functions.default_host()
    default_config_port = enstore_functions.default_port()

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

def get_system_name():
    csc = get_csc()
    hostinfo = socket.gethostbyaddr(csc.server_address[0])[0]
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

def get_config():
    global _config_cache
    if _config_cache:
        return _config_cache
    csc = get_csc()
    _config_cache = csc.dump()
    return _config_cache

def get_mover_list():
    movers = []
    csc = get_csc()

    lm_dict = csc.get_library_managers({})
    for lm in lm_dict.keys():
        mover_list = csc.get_movers(lm_dict[lm]['name'])
        for mover in mover_list:
            movers = movers + [mover['mover']]
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
        return [mover_state, loaded, connect]
    if state in ['HAVE_BOUND']:
        loaded = "loaded %s %s" % (mover, volume)
        return [mover_state, loaded]
    if state in ['MOUNT_WAIT']:
        loading = "loading %s %s" %(mover, volume)
        return [mover_state, loading, connect]

    return [mover_state]

###
### The following functions run in there own thread.
###

def request_mover_status(display):
    global stop_now

    csc = get_csc()
    config = get_config()
    movers = get_mover_list()

    for mover in movers:

        mov = mover_client.MoverClient(csc, mover)
        status = mov.status(rcv_timeout=5, tries=1)
        commands = handle_status(mover[:-6], status)
        if not commands:
            continue
        for command in commands:
            display_lock.acquire()
            if display.stopped or stop_now:
                display_lock.release()
                return
            try:
                display.handle_command(command)
            except Tkinter.TclError:
                pass
            display_lock.release()

def handle_periodic_actions(display):
    global stop_now

    while not display.stopped and not stop_now:

        display_lock.acquire()
        if display.stopped or stop_now:
            display_lock.release()
            return
        #Animate the connection lines.
        try:
            display.connection_animation()
        except Tkinter.TclError:
                pass
        display_lock.release()

        display_lock.acquire()
        if display.stopped or stop_now:
            display_lock.release()
            return
        #Remove unactive clients from the display.
        try:
            display.disconnect_clients()
        except Tkinter.TclError:
                pass
        display_lock.release()
        
        #display_lock.acquire()
        #What does this do???  For whatever reason, if it is commented in
        # then the connection lines stop moving.
        #display.handle_titling()
        #display_lock.release()
        
        time.sleep(0.03) #Without this sleep, the thread uses a lot of CPU.

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
            readable, junk, junk = select.select([erc.sock, 0], [], [], 1)
        except select.error:
            exc, msg, tb = sys.exc_info()
            if msg.args[0] == errno.EINTR:
                erc.unsubscribe()
                erc.sock.close()
                print "Exiting early"
                return

        #If nothing received for 60 seconds, resubscribe.
        if count > 60:
            erc.subscribe()
            count = 0
        count = count + 1            
        if not readable:
            continue

        now = time.time()
        for fd in readable:
            if fd == 0:
                # move along, no more to see here
                erc.unsubscribe()
                erc.sock.close()
                print "Exiting early"
                return
            else:
                msg = enstore_functions.read_erc(erc)
                if msg:
                    print time.ctime(now), msg.type, msg.extra_info
                    command="%s %s" % (msg.type, msg.extra_info)
                    display_lock.acquire()
                    if display.stopped or stop_now:
                        display_lock.release()
                        return
                    try:
                        display.handle_command(command)
                    except Tkinter.TclError:
                        pass
                    display_lock.release()
        if now - start > TEN_MINUTES:
            # resubscribe
            erc.subscribe()
            start = now

    #End nicely.
    erc.unsubscribe()
    erc.sock.close()

###
###  main
###
def main():
    global status_thread, periodic_thread, messages_thread
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

    display = enstore_display.Display(master=None, title=system_name,
                                      window_width=700, window_height=1600,
                                      canvas_width=1000, canvas_height=2000,
                                      background='#add8e6')

    while ( not display.stopped or display.attempt_reinit() ) and not stop_now:

        display.reinit()

        #initalize the movers.
        movers = get_mover_list()
        movers_command = "movers"
        for mover in movers:
            movers_command = movers_command + " " + mover[:-6]

        #Inform the display the names of all the movers.
        display.handle_command(movers_command)

        #On average collecting the status of all the movers takes 10-15
        # seconds.  We don't want to wait that long.  This can be done
        # sychronously to displaying live data.
        status_thread = threading.Thread(group=None,
                                         target=request_mover_status,
                                         name='', args=(display,), kwargs={})
        status_thread.start() #wait for movers to sends status seperately.

        periodic_thread=threading.Thread(group=None,
                                         target=handle_periodic_actions,
                                         name='', args=(display,), kwargs={})
        periodic_thread.start()

        messages_thread=threading.Thread(group=None,
                                         target=handle_messages,
                                         name='', args=(display,), kwargs={})
        messages_thread.start()

        #Loop until user says don't.
        display.mainloop()

        print "waiting for threads to stop"
        status_thread.join()
        print "status thread finished"
        periodic_thread.join()
        print "periodic thread finished"
        messages_thread.join()
        print "message thread finished"


if __name__ == "__main__":
    main()
