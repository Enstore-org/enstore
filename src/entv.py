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

import rexec

_rexec = rexec.RExec()


def eval(stuff):
    return _rexec.r_eval(stuff)

import setpath
import udp_client

debug=1

def endswith(s1,s2):
    return s1[-len(s2):] == s2

_config_cache = None

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


def get_config():
    global _config_cache
    if _config_cache:
        return _config_cache
    p=os.popen("enstore config --show", 'r')
    _config_cache=dict_eval(p.read())
    p.close()
    return _config_cache

def get_movers(config=None):
    movers = []
    if not config:
        config = get_config()
    for item, value in config.items():
        if endswith(item, '.mover') and string.find(item, 'null')<0:
            mover = item[:-6]
            movers.append(mover)
        movers.sort()
    return movers


s = None
dst = None

#This function sends a string to the enstore_display, as
# well as printing it for debugging purposes
def send(msg):
    if debug:
        print "sending",   msg
    s.sendto(msg, dst)

def handle_status(mover, status):
    state = status.get('state','Unknown')
    time_in_state = status.get('time_in_state', '0')
    send("state %s %s %s" % (mover, state, time_in_state))
    volume = status.get('current_volume', None)
    if not volume:
        return
    if state in ['ACTIVE', 'SEEK', 'HAVE_BOUND']:
        send("loaded %s %s" % (mover, volume))
    if state in ['MOUNT_WAIT']:
        send("loading %s %s" %(mover, volume))
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
            send("connect %s %s" % (mover, client))
            
def main():
    global s, dst

    if len(sys.argv) > 1:
        event_relay_host = sys.argv[1]
    else:
        event_relay_host = os.environ.get("ENSTORE_CONFIG_HOST")
        system_name = event_relay_host
    if event_relay_host[:2]=='d0':
        event_relay_host = 'd0ensrv2.fnal.gov'
        system_name = 'd0en'
    elif event_relay_host[:3]=='stk':
        event_relay_host = 'stkensrv2.fnal.gov'
        system_name = 'stken'
        
    event_relay_port = 55510
    os.environ['ENSTORE_CONFIG_HOST'] = event_relay_host

    display = enstore_display.Display(master=None, title=system_name,
                                      window_width=700, window_height=1600,
                                      canvas_width=1000, canvas_height=2000,
                                      background='#add8e6')
    display_thread = threading.Thread(group=None, target=display.mainloop,
                                      name='', args=(), kwargs={})
    display_thread.start()


    dst = display.addr
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    #Tell enstore_display what the movers are
    movers = get_movers()
    send("movers "+string.join(movers))
    
    #give it a little time to draw the movers
    time.sleep(3)

    config = get_config()

    u = udp_client.UDPClient()
    tsd = u.get_tsd()
    sock =  tsd.socket.socket
    reqs = {}
    ticket = {"work" : "status"}
        
    for mover in movers:
        mover_config = config[mover+'.mover']
        mover_addr = (mover_config['hostip'], mover_config['port'])
        u.send_no_wait(ticket, mover_addr)
        reqs[tsd.txn_counter] = mover
        
    # Subscribe to the event notifier
    if debug:
        print "subscribe"
    s.sendto("notify %s %s" % (dst),
             (event_relay_host, event_relay_port))

    #Tell the event_relay that we want to hear about Enstore
    #events.
    #This gets us 15 minutes worth of update messages, so re-subscribe
    # every 10 minutes
    last_sub = 0
    while 1:
        r, w, x = select.select([sock], [], [], 600)
        
        if sock in r: #getting responses to our mover status queries
            try:
                msg = sock.recv(16384)
            except socket.error, detail:
                print detail
                
            try:
                msg_id, status, timestamp = eval(msg)
                mover = reqs[msg_id]
                handle_status(mover, status)
            except:
                print "Error", msg
            
        now = time.time()
        if now - last_sub >= 600:
            s.sendto("notify %s %s" % (dst,),
                     (event_relay_host, event_relay_port))
            last_sub = now
        
if __name__ == "__main__":
    main()
    
