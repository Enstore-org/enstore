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

_rexec = rexec.RExec()
def eval(stuff):
    return _rexec.r_eval(stuff)

debug=1

_csc = None
_config_cache = None

def endswith(s1,s2):
    return s1[-len(s2):] == s2

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

def request_mover_status(display):

    csc = get_csc()
    config = get_config()
    movers = get_mover_list()

    for mover in movers:
        mov = mover_client.MoverClient(csc, mover)
        status = mov.status()
        commands = handle_status(mover[:-6], status)
        if not commands:
            continue
        print commands
        for command in commands:
            display.handle_command(command)

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
    if not volume:
        return
    if state in ['ACTIVE', 'SEEK', 'HAVE_BOUND']:
        loaded = "loaded %s %s" % (mover, volume)
        return [mover_state, loaded]
    if state in ['MOUNT_WAIT']:
        loading = "loading %s %s" %(mover, volume)
        return [mover_state, loading]
    if state in ['ACTIVE', 'SEEK']: #we are connected to a client
        client = ''
        files = status['files']
        if files[0] and files[0][0]=='/':
            client = files[1]
        else:
            client = files[0]
        colon = string.find(client, ':')
        if colon>=0:
            client = client[:colon]
        if client:
            connect = "connect %s %s" % (mover, client)
            return [mover_state, connect]

    return [mover_state]

def handle_periodic_actions(display):

    while not display.stopped:

        ## Here is where we handle periodic things
        now = time.time()
        #### Update all mover timers
        #This checks to see if the timer has changed at all.  If it has,
        # it resets the timer for new state.
        for mover in display.movers.values():
            seconds = int(now - mover.timer_started)
            if seconds != mover.timer_seconds:
                mover.update_timer(seconds)     #We must advance the timer
            if mover.connection:
                mover.connection.animate(now)

        #### Check for unconnected clients
        for client_name, client in display.clients.items():
            if (client.n_connections > 0 or client.waiting == 1):
                continue
            if now - client.last_activity_time > 5: # grace period
                print "It's been longer than 5 seconds, ",
                print client_name," client must be deleted"
                del display.clients[client_name]
                client.undraw()

        #### Handle titling
        if display.title_animation:
            if now > display.title_animation.stop_time:
                display.title_animation = None
            else:
                display.title_animation.animate(now)

        ####force the display to refresh
        display.update()
            
def main():
    system_name = get_system_name()

    entv_is_on = -1 #-1 means (re)init.  Otherwise, exit.

    display = enstore_display.Display(master=None, title=system_name,
                                      window_width=700, window_height=1600,
                                      canvas_width=1000, canvas_height=2000,
                                      background='#add8e6')

    while entv_is_on < 0:

        #initalize the movers.
        movers = get_mover_list()
        movers_command = "movers"
        for mover in movers:
            movers_command = movers_command + " " + mover[:-6]

        #Inform the display the names of all the movers.
        display.handle_command(movers_command)

        display.reposition_canvas()

        display.update()

        #On average collecting the status of all the movers takes 10-15
        # seconds.  We don't want to wait that long.  This can be done
        # sychronously to displaying live data.
        status_tread = threading.Thread(group=None,
                                        target=request_mover_status,
                                        name='', args=(display,), kwargs={})
        status_tread.start() #wait for movers to sends status seperately.

        periodic_tread=threading.Thread(group=None,
                                        target=handle_periodic_actions,
                                        name='', args=(display,), kwargs={})
        periodic_tread.start()

        #Loop until user says don't.
        display.mainloop()
        display.cleanup()
        entv_is_on = display.stopped
        display.stopped = 0
        while threading.activeCount() > 1:
            pass #wait


if __name__ == "__main__":
    main()
    
