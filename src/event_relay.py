#!/usr/bin/env python

import os
import socket
import select
import time
import string
import sys

import enstore_constants
import Trace
import log_client
import option
import e_errors

DEFAULT_PORT = enstore_constants.EVENT_RELAY_PORT
heartbeat_interval = enstore_constants.EVENT_RELAY_HEARTBEAT
my_name = enstore_constants.EVENT_RELAY
my_ip = socket.gethostbyaddr(socket.gethostname())[2][0]

# event relay message types
ALL = "all"
NOTIFY = "notify"
UNSUBSCRIBE = "unsubscribe"
DUMP = "dump"
MAX_TIMEOUTS = 20
LOG_NAME = "EVRLY"

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
	self.timeouts = {} # key is (host,port), value is num times error in send
        self.listen_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.send_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        my_addr = ("", my_port)
        self.listen_socket.bind(my_addr)
        self.alive_msg = 'alive %s %s %s' % (my_ip, my_port, my_name)
	### debugger messages
	csc = (option.default_host(), option.default_port())
	self.logc = log_client.LoggerClient(csc, LOG_NAME, 'log_server')
	Trace.init(LOG_NAME)
            
    def dump(self):
	# dump our brains
	print "%s"%(time.strftime("%s%s%s"%("%Y-%b-%d", " ", 
					    "%H:%M:%S"), time.localtime(time.time())),)
	print "Subscribed clients : %s"%(self.clients,)
	print "Timeouts : %s"%(self.timeouts,)

    def cleanup(self, key):
	if self.clients.has_key(key):
	    del self.clients[key]
	if self.timeouts.has_key(key):
	    del self.timeouts[key]

    def mainloop(self):
        last_heartbeat = 0
	try:
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
			msg = "Subscribe request for %s, (port: %s) for %s."%(ip, port,
									      filter_d)
			Trace.log(e_errors.INFO, msg, Trace.MSG_EVENT_RELAY)
		    except:
			self.dump()
                        Trace.handle_error(msg_type=Trace.MSG_EVENT_RELAY)
			msg = "cannot handle request %s"%(msg,)
			Trace.log(e_errors.INFO, msg, Trace.MSG_EVENT_RELAY)

		elif tok[0] == UNSUBSCRIBE:
		    try:
			ip = tok[1]
			port = int(tok[2])
			if not self.clients.has_key((ip, port)):
			    msg = "no client subscribed %s"%(msg,)
			else:
			    self.cleanup((ip, port))
			    msg = "Unsubscribe request for %s, (port: %s)"%(ip, port)
			Trace.log(e_errors.INFO, msg, Trace.MSG_EVENT_RELAY)
		    except:
			self.dump()
                        Trace.handle_error(msg_type=Trace.MSG_EVENT_RELAY)
			msg = "cannot handle request %s"%(msg,)
			Trace.log(e_errors.INFO, msg, Trace.MSG_EVENT_RELAY)

		elif tok[0] == DUMP:
		    self.dump()
		else:
		    self.send_message(msg, tok[0], now)
	except:
	    self.dump()
	    Trace.handle_error(msg_type=Trace.MSG_EVENT_RELAY)

    def send_message(self, msg, msg_type, now):
        """Send the message to all clients who care about it"""
        for addr, (t0, filter_d) in self.clients.items():
            if now - t0 > self.client_timeout:
		self.cleanup(addr)
            else:
                # client wants the message if there is no filter or if
                # the filter contains the message type in its dict.
                if (not filter_d) or filter_d.has_key(msg_type):
                    try:
                        self.send_socket.sendto(msg, addr)
                    except:
			###self.dump()
                        ###Trace.handle_error(msg_type=Trace.MSG_EVENT_RELAY)
			msg = "send failed %s"%(addr,)
			Trace.log(e_errors.ERROR, msg, Trace.MSG_EVENT_RELAY)

			### figure out if we should stop sending to this client
			self.timeouts[addr] = self.timeouts.get(addr, 0) + 1
			if self.timeouts[addr] > MAX_TIMEOUTS:
			    self.cleanup(addr)
                
if __name__ == '__main__':
    R = Relay()
    R.mainloop()
