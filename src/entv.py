#!/usr/bin/env python

# $Id$


#This file needs a lot of work!!

import os
import sys
import socket
import string
import time

event_relay_host = os.environ.get("ENSTORE_CONFIG_HOST")
event_relay_port = 55510

def endswith(s1,s2):
    return s1[-len(s2):] == s2

def get_config():
    pipe = os.popen("enstore config --show", 'r') #run the command and give us
                                                                           ## a way to access to its output
    data = pipe.read() #get the output of the command (will be a string)
    configdict = eval(data) # Turn the string back into a Python object


## TODO: write "get_mover_status" which works the same way, running
    ## enstore mover --status <whatever>.mover
    
def get_movers():
    movers = []
    configdict = get_config()
    for item, value in configdict.items():
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
    print "sending",   msg
    s.sendto(msg, dst)

DEFAULTPORT = 60126 #same as enstore_display.py
    
def main():
    global s, dst
    if len(sys.argv) == 1:
        target_ip = os.uname()[1]
        target_port = DEFAULTPORT
    elif len(sys.argv) != 3:
        print "Usage: %s host port" % (sys.argv[0],)
        print "  host and port refer to the host and port enstore_display is running on"
        sys.exit(1)
    else:
        target_ip, target_port = sys.argv[1:]
    target_port = int(target_port)
    dst = (target_ip, target_port)
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    #Tell enstore_display what the movers are
    movers = get_movers()
    send("movers "+string.join(movers))
    
    #give it a little time to draw the movers
    time.sleep(3)

    # XXX Now, before we register with the event_relay, it would
    # be really nice to use "enstore mover --status" to get current
    # status of all the Movers!
    
    #Tell the event_relay that we want to hear about Enstore
    #events.
    #This gets us 15 minutes worth of update messages
    s.sendto("notify %s %s" % (target_ip, target_port),
             (event_relay_host, event_relay_port))

if __name__ == "__main__":
    main()
    
    
