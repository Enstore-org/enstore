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
    
target_ip, target_port = sys.argv[1:]
target_port = int(target_port)
dst = (target_ip, target_port)
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

def send(msg):
    print msg, dst
    s.sendto(msg, dst)

send("movers "+string.join(movers))
time.sleep(3)

#this gets us 15 minutes worth of update messages
s.sendto("notify %s %s" % (target_ip, target_port), (event_relay_host, event_relay_port))
