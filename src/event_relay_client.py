#!/usr/bin/env python

###############################################################################
#
# $Id$
#
# This class supports messages from the event relay process.  Methods are
# provided to read the message.
###############################################################################

import socket
import os
import sys
import select
#import string

import enstore_erc_functions
import event_relay_messages
import enstore_constants
import e_errors
import host_config

DEFAULT_PORT = 55510

DEFAULT_TIMEOUT = 3
DEFAULT_TRIES = 1

def get_event_relay_host(csc):
    ticket = csc.get(enstore_constants.EVENT_RELAY, DEFAULT_TIMEOUT,
                     DEFAULT_TRIES)
    if ticket['status'][0] == e_errors.OK:
        host = ticket.get('host', "")
    else:
        host = ""
    return host

def get_event_relay_port(csc):
    ticket = csc.get(enstore_constants.EVENT_RELAY, DEFAULT_TIMEOUT,
                     DEFAULT_TRIES)
    if ticket['status'][0] == e_errors.OK:
        port = ticket.get('port', 0)
    else:
        port = 0
    return port

def get_event_relay_addr(csc):
    ticket = csc.get(enstore_constants.EVENT_RELAY, DEFAULT_TIMEOUT,
                     DEFAULT_TRIES)
    if ticket['status'][0] == e_errors.OK:
        host = ticket.get('host', "")
        port = ticket.get('port', 0)
    else:
        host = ""
        port = 0
    return (host, port)

def set_max_recv_buffersize(sock):

    TWO_MB = 2097152
    
    try:
        max_buffer_size = os.fpathconf(sock.fileno(),
                                       os.pathconf_names['PC_SOCK_MAXBUF'])

        #Note: In at least one case (Linux LTS) the value returned by
        # fpathconf() is -1.
    except (ValueError, KeyError):
        #The string 'PC_SOCK_MAXBUF' is not recognized by this system.
        max_buffer_size = -1
    except OSError:
        #The system knows about the symbol 'PC_SOCK_MAXBUF', but the
        # required functionality is not implimented.
        max_buffer_size = -1

    if max_buffer_size > TWO_MB:
        #The maximum socket buffer size of a socket on SGI could be 1GB.
        # This is way to large... Bump it down to 2MB.
        max_buffer_size = TWO_MB

    if max_buffer_size > 0:
        #This implimentaion knew about PC_SOCK_MAXBUF if we get here.
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, max_buffer_size)
        return

    #The maximum buffer sizes for the various supported fermi platforms:
    # Linux(2.4): cat /proc/sys/net/core/rmem_max
    #             65535 = 64K (in reality it is 131070; the kernel doubles it)
    # SunOS: ndd /dev/tcp tcp_max_buf
    #        1048576 = 1MB
    # IRIX: cat /var/sysgen/mtune/bsd | grep -E "(tcp|udp)_(send|recv)space"
    #       1073741824 = 1GB (Also returns default and minimum values.)
    # OSF1: /sbin/sysconfig -q socket | grep sb_max
    #       1048576 = 1MB
    #This information came from:
    # http://www.psc.edu/networking/perf_tune.html
    current_size = TWO_MB  #2MB

    #Start at a large number (2MB) and divide by two to find a number to
    # set the socket buffer size to.  This algorithm came from "UNIX
    # Network Programming" by Stevens et. all. 3rd edition page 209. 
    while current_size > 4096:
        try:
            #Keep looping starting at a large number.
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, current_size)

            #Stop at the first success.
            break
        except socket.error:
            pass

        current_size = int(current_size / 2.0)


class EventRelayClient:

    SUCCESS = 1
    ERROR = 0

    def setup(self, sock=None):
        self.invalid = 0
        self.error_msg = ""
        if sock:
            self.sock = sock
        else:
            self.sock = None
            try:
                default_ip = host_config.get_default_interface_ip()
                address_family = socket.getaddrinfo(default_ip, None)[0][0]
                self.sock = socket.socket(address_family, socket.SOCK_DGRAM)
                set_max_recv_buffersize(self.sock)
                #default_ip = host_config.get_default_interface_ip()
                self.sock.bind((default_ip, 0))    # let the system pick a port
            except socket.error, msg:
                # this can happen rarely, it can mean too many open files
                self.invalid = 1
                self.error_msg = msg
                if self.sock:
                    self.sock.close()
                return self.ERROR

        self.addr = self.sock.getsockname()
        self.host = self.addr[0]
        self.port = self.addr[1]
        self.subscribe_time = 0
        self.notify_msg = None
        self.unsubscribe_msg = None

        # get the address of the event relay process.
        import configuration_client
        if not self.event_relay_host:
            self.csc = configuration_client.ConfigurationClient()
            self.event_relay_host = get_event_relay_host(self.csc)
            if not self.event_relay_host:
                self.event_relay_host = os.environ.get(
                                                     "ENSTORE_CONFIG_HOST","")
        if not self.event_relay_port:
            # try to get it from the config file
            self.event_relay_port = DEFAULT_PORT
        self.event_relay_addr = (self.event_relay_host, self.event_relay_port)
        self.invalid = 0
        return self.SUCCESS

    def unsetup(self):
        self.invalid = 1
        self.error_msg = ""
        self.addr = None
        self.host = None
        self.port = None
        if self.sock:
            self.sock.close()
        return self.SUCCESS

    def __init__(self, server=None, function=None, event_relay_host=None, 
                 event_relay_port=None, sock=None):
        # get a socket on which to talk to the event relay process
        # we do this setup in a subroutine, so if it doesn't work
        # we can try again later.
	config = host_config.get_config()
	if config and config.get("hostip", None):
	    self.hostname = config['hostip']
	else:
            self.hostname = host_config.get_default_interface_ip()
        self.server = server
        self.function = function
        self.event_relay_host = event_relay_host
        self.event_relay_port = event_relay_port
        self.do_interval = 1
        if sock:
            self.do_select_fd = 0
        else:
            self.do_select_fd = 1
        self.setup(sock)

    # return fileno for socket.select() processing.
    def fileno(self):
        return self.sock.fileno()

    # set value to not register interval functions with dispatching worker
    def no_interval(self):
        self.do_interval = 0

    # set value to not register fds with dispatching worker
    def no_select_fd(self):
        self.do_select_fd = 0

    # this method must be called if we want to have the event relay forward 
    # messages to us.
    def start(self, subscribe_msgs=None, resubscribe_rate=600, sock=None):
        if self.invalid:
            # we could not bind to a socket, try again.
            self.setup(sock)
            if self.invalid:
                # nope, didn't work
                return self.ERROR
        self.subscribe_msgs = subscribe_msgs
        self.resubscribe_rate = resubscribe_rate

        # subscribe here for the first time, then let the interval timer
        # (which we set below) redo it automatically for us
        retval = self.subscribe()
        if retval == self.ERROR:
            return self.ERROR

        # add this socket to the select sockets upon which we wait
	if self.server is not None:
            if self.do_select_fd:
                self.server.add_select_fd(self.sock, 0, self.function)
        
	    # resubscribe ourselves to the event relay every 10 minutes
            if self.do_interval:
                self.server.add_interval_func(self.subscribe, 
                                              self.resubscribe_rate)
        return self.SUCCESS

    def stop(self):
        if not self.invalid:
            if self.server is not None:
                try:
                    if self.do_interval:
                        self.server.remove_interval_func(self.subscribe)
                    if self.do_select_fd:
                        self.server.remove_select_fd(self.sock)
                except:
                    pass
                
        self.unsubscribe()
        self.unsetup()

    # send the message to the event relay
    def send(self, msg):
        try:
            if not self.invalid:
                
                msg.send(self.sock, self.event_relay_addr)
                return self.SUCCESS
            else:
                return self.ERROR
        except:
            # this has to be lightweight and foolproof
            return self.ERROR

    # read a message from the socket
    def read(self, fd=None):
        if self.do_select_fd == 0:

            decoded_msg = event_relay_messages.decode(self.error_msg)
            self.error_msg = ""
            if decoded_msg:
                return decoded_msg
        if not self.invalid:
            if not fd:
                fd = self.sock
            # note: this may raise a socket.error exception which needs to be
            # caught by the calling routine. (enstore_functions.read_erc does it)
            msg = fd.recv(1024)
            # now decode the message based on the message type, which is always 
            # the first word in the text message
            return event_relay_messages.decode(msg)
        else:
            return self.ERROR
        
    # subscribe ourselves to the event relay server
    def subscribe(self):
        if not self.notify_msg:
            self.notify_msg = event_relay_messages.EventRelayNotifyMsg(self.host,
                                                                     self.port)
            self.notify_msg.encode(self.subscribe_msgs)
        return self.send(self.notify_msg)

    # unsubscribe ourselves to the event relay server
    def unsubscribe(self):
        if not self.unsubscribe_msg:
            self.unsubscribe_msg = event_relay_messages.EventRelayUnsubscribeMsg(self.host,
								    self.port)
            self.unsubscribe_msg.encode()
        return self.send(self.unsubscribe_msg)

    # send the heartbeat to the event relay
    def heartbeat(self):
	opt_string = ""
	if self.function is not None:
	    opt_string = self.function()
        self.heartbeat_msg.encode(self.name, opt_string)
        return self.send(self.heartbeat_msg)

    def start_heartbeat(self, name, heartbeat_interval, function=None):
        print name, heartbeat_interval, function
        # we will set up a heartbeat to be sent periodically to the event
        # relay process
        if not self.invalid:
            self.heartbeat_interval = heartbeat_interval
            self.function = function
            self.name = name
            self.heartbeat_msg = event_relay_messages.EventRelayAliveMsg(self.host, 
                                                                        self.port)
            if self.do_interval:
                self.server.add_interval_func(self.heartbeat, self.heartbeat_interval)
            return self.SUCCESS
        else:
            return self.ERROR

    def stop_heartbeat(self):
        if self.do_interval:
             self.server.remove_interval_func(self.heartbeat)
             return self.SUCCESS
        else:
            return self.ERROR

    def send_one_heartbeat(self, name, function=None):
        # send one heartbeat to the event relay
	self.function = function
	self.name = name
        self.heartbeat_msg = event_relay_messages.EventRelayAliveMsg(self.host, 
                                                                    self.port)
	return self.heartbeat()

    def dump(self):
	# send the dump request to the event relay
	self.dump_msg = event_relay_messages.EventRelayDumpMsg(self.host, self.port)
	self.dump_msg.encode()
	return self.send(self.dump_msg)

    def quit(self):
        # send the quit message to the event relay
        self.quit_msg = event_relay_messages.EventRelayQuitMsg(self.host, self.port)
	self.quit_msg.encode()
	return self.send(self.quit_msg)

    def do_print(self):
	# send the do_print request to the event relay
	self.doprint_msg = event_relay_messages.EventRelayDoPrintMsg(self.host, self.port)
	self.doprint_msg.encode()
	return self.send(self.doprint_msg)

    def dont_print(self):
	# send the dont_print request to the event relay
	self.dontprint_msg = event_relay_messages.EventRelayDontPrintMsg(self.host, self.port)
	self.dontprint_msg.encode()
	return self.send(self.dontprint_msg)

    def event_relay_heartbeat(self):
	# send the heartbeat request to the event relay
	self.heartbeat_msg = event_relay_messages.EventRelayHeartbeatMsg(self.host, self.port)
	self.heartbeat_msg.encode()
	return self.send(self.heartbeat_msg)

    def alive(self):
        # check if the event_relay is alive.  do this by subscribing
        # for alive messages and then asking the event relay to send
        # its own heartbeat.
        if self.start([event_relay_messages.ALIVE,]) == self.SUCCESS:
            self.event_relay_heartbeat()
            # wait for events
            readable, junk, junk = select.select([self.sock], [], [], DEFAULT_TIMEOUT)
            if readable:
                for fd in readable:
                    msg = enstore_erc_functions.read_erc(self)
                    if msg:
                        # we got one, we are done
                        self.unsubscribe()
                        #self.sock.close()
                        return 0
            # we did not get a message, assume the event relay is not alive
            return 1
        else:
            return 1

class EventRelayClientInterface:


    # since we do not use the Interface class, the 2 args are place
    # holders only.
    def __init__(self, args=None, user_mode=None):
        # Since we do not use the Interface class, the 2 args are place
        # holders only.
        __pychecker__ = "no-argsused"
        
        if "--%s"%(event_relay_messages.DUMP,) in sys.argv:
            self.dump = 1
        else:
            self.dump = 0
        if "--%s"%(event_relay_messages.ALIVE,) in sys.argv:
            self.alive = 1
        else:
            self.alive = 0
        if "--%s"%(event_relay_messages.QUIT,) in sys.argv:
            self.quit = 1
        else:
            self.quit = 0
        if "--%s"%(event_relay_messages.DOPRINT,) in sys.argv:
            self.doprint = 1
        else:
            self.doprint = 0
        if "--%s"%(event_relay_messages.DONTPRINT,) in sys.argv:
            self.dontprint = 1
        else:
            self.dontprint = 0

    def print_help(self):
        print "[--%s] [--%s] [--%s] [--%s] [--%s]"% \
            (event_relay_messages.ALIVE,
             event_relay_messages.DUMP,
             event_relay_messages.DOPRINT,
             event_relay_messages.DONTPRINT,
             event_relay_messages.QUIT)

def do_work(intf):
    # now get an event relay client
    erc = EventRelayClient()
    rtn = 0
    if intf.alive:
        rtn = erc.alive()

    elif intf.dump:
        erc.dump()

    elif intf.quit:
        erc.quit()
    elif intf.doprint:
        erc.do_print()
    elif intf.dontprint:
        erc.dont_print()

    else:
        intf.print_help()
        sys.exit(0)

    return rtn

if __name__ == "__main__":
 
    intf = EventRelayClientInterface()

    sys.exit(do_work(intf))
