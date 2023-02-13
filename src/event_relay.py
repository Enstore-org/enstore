#!/usr/bin/env python

#import os
import socket
import select
import time
import string
import sys

import enstore_constants
import Trace
import log_client
#import option
import e_errors
import cleanUDP
import enstore_functions2

DEFAULT_PORT = enstore_constants.EVENT_RELAY_PORT
heartbeat_interval = enstore_constants.EVENT_RELAY_HEARTBEAT
my_name = enstore_constants.EVENT_RELAY
my_ip = socket.getaddrinfo(socket.gethostname(), None)[0][4][0]
my_address_family = socket.getaddrinfo(socket.gethostname(), None)[0][0]

# event relay message types
ALL = "all"
NOTIFY = "notify"
UNSUBSCRIBE = "unsubscribe"
HEARTBEAT = "heartbeat"
DUMP = "dump"
QUIT = "quit"
DO_PRINT = "do_print"
DONT_PRINT = "dont_print"
MAX_TIMEOUTS = 20
LOG_NAME = "EVRLY"
YES = 1

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
        ##self.listen_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        ##self.send_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	#self.listen_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	#self.send_socket = cleanUDP.cleanUDP(socket.AF_INET, socket.SOCK_DGRAM)
	self.listen_socket = socket.socket(my_address_family, socket.SOCK_DGRAM)
	self.send_socket = cleanUDP.cleanUDP(my_address_family, socket.SOCK_DGRAM)
        my_addr = ("", my_port)
        self.listen_socket.bind(my_addr)
        self.alive_msg = 'alive %s %s %s' % (my_ip, my_port, my_name)
	### debugger messages
	csc = (enstore_functions2.default_host(),
               enstore_functions2.default_port())
	self.logc = log_client.LoggerClient(csc, LOG_NAME, 'log_server')
	self.do_print = 0
	Trace.init(LOG_NAME)
            
    def ev_print(self, msg):
	if self.do_print:
	    print msg

    def dump(self):
	# dump our brains
	print "%s"%(time.strftime("%s%s%s"%("%Y-%b-%d", " ", 
					    "%H:%M:%S"), time.localtime(time.time())),)
	print "Subscribed clients : %s"%(self.clients,)
	print "Timeouts : %s"%(self.timeouts,)

    def doQuit(self):
        sys.exit(0)

    def cleanup(self, key, log=0):
	if self.clients.has_key(key):
	    if log:
		msg = "Cleaning up %s from clients"%(key,)
		Trace.log(e_errors.INFO, msg, Trace.MSG_EVENT_RELAY)
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
			key = (ip, port)
			# get rid of old info first
			self.cleanup(key)
			self.clients[key] = (now, filter_d)
			#msg = "Subscribe request for %s, (port: %s) for %s."%(ip, port,
			#						      filter_d)
			#Trace.log(e_errors.INFO, msg, Trace.MSG_EVENT_RELAY)
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
			    #msg = "no client subscribed %s"%(msg,)
                            pass
			else:
			    self.cleanup((ip, port))
			    #msg = "Unsubscribe request for %s, (port: %s)"%(ip, port)
			#Trace.log(e_errors.INFO, msg, Trace.MSG_EVENT_RELAY)
		    except:
			self.dump()
                        Trace.handle_error(msg_type=Trace.MSG_EVENT_RELAY)
			msg = "cannot handle request %s"%(msg,)
			Trace.log(e_errors.INFO, msg, Trace.MSG_EVENT_RELAY)

		elif tok[0] == HEARTBEAT:
		    self.send_message(self.alive_msg, 'alive', now)
		elif tok[0] == DUMP:
		    self.dump()
                elif tok[0] == QUIT:
                    self.doQuit()
		elif tok[0] == DO_PRINT:
		    self.do_print = YES
		elif tok[0] == DONT_PRINT:
		    self.do_print = 0
		else:
		    self.send_message(msg, tok[0], now)
        except SystemExit:
            pass
	except:
	    self.dump()
	    Trace.handle_error(msg_type=Trace.MSG_EVENT_RELAY)

    def handle_error(self, addr, msg, extra=""):
	error_msg = "send failed %s (%s) (%s)"%(addr, msg, extra)
	Trace.log(e_errors.ERROR, error_msg, Trace.MSG_EVENT_RELAY)

	### figure out if we should stop sending to this client
	self.timeouts[addr] = self.timeouts.get(addr, 0) + 1
	if self.timeouts[addr] > MAX_TIMEOUTS:
	    self.cleanup(addr, YES)

    def send_message(self, msg, msg_type, now):
        """Send the message to all clients who care about it"""
	self.ev_print("%s %s"%(time.ctime(now), msg_type))
        for addr, (t0, filter_d) in self.clients.items():
	    self.ev_print("    %s %s"%(addr, filter_d))
            if now - t0 > self.client_timeout:
		self.ev_print("    cleaning up %s"%(addr,))
		self.cleanup(addr, YES)
            else:
                # client wants the message if there is no filter or if
                # the filter contains the message type in its dict.
                if (not filter_d) or filter_d.has_key(msg_type):
                    try:
			self.ev_print("    sending '%s' to %s"%(msg, addr,))
                        l = self.send_socket.sendto(msg, addr)
			self.ev_print("    sendto return = %s"%(l,))
		    except socket.error, detail:
			extra = "%s"%(detail,)
			self.ev_print("    ERROR: %s"%(detail,))
			self.handle_error(addr, msg, extra)
                    except:
			self.ev_print("    ERROR: unknown")
			self.handle_error(addr, msg)
if __name__ == "__main__":   # pragma: no cover
    R = Relay()
    #R._do_print({'levels':range(5, 400)})
    R.mainloop()
