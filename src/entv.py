#!/usr/bin/env python

# $Id$

import os
import sys
import socket
import string
import time

event_relay_host = os.environ.get("ENSTORE_CONFIG_HOST")
event_relay_port = 55510

def endswith(s1,s2):
    return s1[-len(s2):] == s2

configdict = eval (os.popen("enstore config --show", 'r').read())

movers = []

for item, value in configdict.items():
    if endswith(item, '.mover') and string.find(item, 'null')<0:
        mover = item[:-6]
        movers.append(mover)
    movers.sort()


s = None
dst = None
def send(msg):
    print "sending",   msg
    s.sendto(msg, dst)

def main():
    global s, dst
    
    if len(sys.argv) != 3:
        print "Usage: %s host port" % (sys.argv[0],)
        print "  host and port refer to the host and port enstore_display is running on"
        sys.exit(1)
        
    target_ip, target_port = sys.argv[1:]
    target_port = int(target_port)
    dst = (target_ip, target_port)
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    send("movers "+string.join(movers))
    #give it a little time to draw the movers
    time.sleep(3)

    #this gets us 15 minutes worth of update messages
    s.sendto("notify %s %s" % (target_ip, target_port), (event_relay_host, event_relay_port))

if __name__ == "__main__":
    main()
    
    
