#!/usr/bin/env python

import os
import socket
import select
import time
import string

DEFAULT_PORT = 55510
MINTOKLEN = 3

# event relay message types
ALL = "all"
NOTIFY = "notify"
UNSUBSCRIBE = "unsubscribe"

def get_message_filter_dict(msg_tok):
    filter_d = {}
    # first see if there is a message type list 
    if len(msg_tok) > MINTOKLEN:
	# yes there is
	for tok in msg_tok[MINTOKLEN:]:
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
    
    def __init__(self, myport=DEFAULT_PORT):
        self.clients = {} # key is (host,port), value is time connected
        self.listen_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.send_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        myaddr = ("", myport)
        self.listen_socket.bind(myaddr)

    def mainloop(self):
        while 1:
            readable, junk, junk = select.select([self.listen_socket], [], [], 15)
            if not readable:
                continue
            msg = self.listen_socket.recv(1024)
            now = time.time()
            if not msg:
                continue
            tok = string.split(msg)
            if not tok:
                continue
	    print msg
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

	    elif tok[0] == UNSUBSCRIBE:
		try:
		    ip = tok[1]
		    port = int(tok[2])
		    del self.clients[(ip, port)]
		except:
		    print "cannot handle request", msg
            else:
                for addr, (t0, filter_d) in self.clients.items():
                    if now - t0 > self.client_timeout:
                        del self.clients[addr]
                    else:
			# client wants the message if there is no filter or if
			# the filter contains the message type in its dict.
			if (not filter_d) or filter_d.has_key(tok[0]):
			    try:
				self.send_socket.sendto(msg, addr)
			    except:
				print "send failed", addr
                            
                
if __name__ == '__main__':
    R = Relay()
    R.mainloop()
