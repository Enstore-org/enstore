#!/usr/bin/env python

# $Id$

import os
import sys
import socket
import select
import string
import time
import popen2

debug=1

def endswith(s1,s2):
    return s1[-len(s2):] == s2

def get_config():
    p=os.popen("enstore config --show", 'r')
    dict=dict_eval(p.read())
    p.close()
    return dict

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

def get_mover_status(mover):
    status_dict = {}
    try:
        file="enstore mover --status %s.mover" % mover
        p = os.popen(file,'r')
        status_dict = dict_eval(p.read())
        p.close()
    except:
        pass
    return status_dict


# This function requests status for all the movers at once
#   and returns a dictionary {mover_name: status }
# If movers take longer than "timeout" to reply, they are ignored

def get_mover_status_parallel(movers, timeout=15):
    ret = {}
    start_time = time.time()
    end_time = start_time + timeout
    pipe_dict = {} #Key is a file descriptor, value is a Popen object
    waiting = [] #These are the movers we're still waiting for

    for mover in movers:
        cmd = "enstore mover --status %s.mover " % mover
        try:
            p = popen2.Popen3(cmd)
            p.mover = mover
            pipe_dict[p.fromchild] = p
            waiting.append(mover)
        except:
            pass

    while waiting:
        time_remaining = end_time - time.time()
        if time_remaining<0:
            break

        r, w, x = select.select(pipe_dict.keys(), [], [], time_remaining)
        for p in r:
            try:
                pipe = pipe_dict[p]
                mover = pipe.mover
                del pipe_dict[p]
                data = p.read()
                p.close()
                ret[mover] = dict_eval(data)
                waiting.remove(mover)
            except:
                pass
    
    for p in pipe_dict.values():
        os.kill(p.pid, 9)
    return ret
            
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

    pipe = os.popen("python -u ./enstore_display.py %s"%(system_name,), 'r')
    msg = pipe.readline()
    words = string.split(msg)
    if words[0]!='addr=':
        print "Error", msg
        sys.exit(-1)
    
    target_ip = words[1]
    target_port = int(words[2])
    dst = (target_ip, target_port)
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    #Tell enstore_display what the movers are
    movers = get_movers()
    send("movers "+string.join(movers))
    
    #give it a little time to draw the movers
    time.sleep(3)

    
    #Get the state of each mover before continuing

    mover_status = get_mover_status_parallel(movers, timeout=15)
    for mover, status in mover_status.items():
        state = status.get('state','Unknown')
        time_in_state = status.get('time_in_state', '0')
        send("state %s %s %s" % (mover, state, time_in_state))
        volume = status.get('current_volume', None)
        if not volume:
            continue
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

    
    # Subscribe to the event notifier
    if debug:
        print "subscribe"
    s.sendto("notify %s %s" % (target_ip, target_port),
             (event_relay_host, event_relay_port))

            
    #Tell the event_relay that we want to hear about Enstore
    #events.
    #This gets us 15 minutes worth of update messages, so re-subscribe
    # every 10 minutes
    last_sub = 0
    while 1:
        r, w, x = select.select([pipe], [], [], 600)
        if r:
            l = pipe.readline()
            if not l:
                break
            print l,
        now = time.time()
        if now - last_sub >= 600:
            try:
                s.sendto("notify %s %s" % (target_ip, target_port),
                         (event_relay_host, event_relay_port))
                last_sub = now
            except:
                pass
        
if __name__ == "__main__":
    main()
    
