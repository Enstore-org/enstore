#!/usr/bin/env python

import os
import socket
import select
import time
import string
import traceback

import enstore_constants

DEFAULT_PORT = enstore_constants.EVENT_RELAY_PORT
heartbeat_interval = enstore_constants.EVENT_RELAY_HEARTBEAT
my_name = enstore_constants.EVENT_RELAY
my_ip = socket.gethostbyaddr(socket.gethostname())[2][0]

# event relay message types
ALL = "all"
NOTIFY = "notify"
UNSUBSCRIBE = "unsubscribe"

def get_message_filter_dict(msg_tok):
    filter_d = {}
    # first see if there is a message type list 
    if len(msg_tok) > 3:
        # yes there is
        for tok in msg_tok[3:]:
            if tok == ALL:
                # client wants all messages
                filter_d = None
                break
            else:
                # the value of the dictionary element does not matter
                filter_d[tok] = 1
    else:
        # the client wants all of the messages
        filter_d = None
                
    return filter_d
        
class Relay:

    client_timeout = 15*60 #clients recieve messages for this long

    def __init__(self, my_port=DEFAULT_PORT):
        self.clients = {} # key is (host,port), value is time connected
        self.listen_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.send_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        my_addr = ("", my_port)
        self.listen_socket.bind(my_addr)
        self.alive_msg = 'alive %s %s %s' % (my_ip, my_port, my_name)
            
    def mainloop(self):
        last_heartbeat = 0
        while 1:
            readable, junk, junk = select.select([self.listen_socket], [], [], 15)
            now = time.time()
            if now - last_heartbeat > heartbeat_interval:
                self.send_message(self.alive_msg, 'alive', now)
		last_heartbeat = now
            if not readable:
                continue
            msg = self.listen_socket.recv(1024)

            if not msg:
                continue
            tok = string.split(msg)
            if not tok:
                continue
            if tok[0]==NOTIFY:
                try:
                    ip = tok[1]
                    port = int(tok[2])
                    # the rest of the message is the list of message types the
                    # client is interested in.  if there is no list, the client
                    # wants all message types
                    filter_d = get_message_filter_dict(tok)
                    self.clients[(ip, port)] = (now, filter_d)
                except:
                    print "cannot handle request", msg
		    traceback.print_exc()

            elif tok[0] == UNSUBSCRIBE:
                try:
                    ip = tok[1]
                    port = int(tok[2])
                    del self.clients[(ip, port)]
                except:
                    print "cannot handle request", msg
            else:
                self.send_message(msg, tok[0], now)
        
    def send_message(self, msg, msg_type, now):
        """Send the message to all clients who care about it"""
        for addr, (t0, filter_d) in self.clients.items():
            if now - t0 > self.client_timeout:
                del self.clients[addr]
            else:
                # client wants the message if there is no filter or if
                # the filter contains the message type in its dict.
                if (not filter_d) or filter_d.has_key(msg_type):
                    try:
                        self.send_socket.sendto(msg, addr)
                    except:
                        print "send failed", addr
			traceback.print_exc()
                
if __name__ == '__main__':
    R = Relay()
    R.mainloop()
