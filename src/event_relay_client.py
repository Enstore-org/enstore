
import socket
import os

import event_relay_messages

"""
This class supports messages from the event relay process.  Methods are provided to read
the message.
"""

DEFAULT_PORT = 55510


class EventRelayClient:

    def __init__(self, server=None, function=None, event_relay_host=None, 
                 event_relay_port=None):
        # get a socket on which to talk to the event relay process
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        hostname = os.uname()[1]
        self.sock.bind((hostname, 0))         # let the system pick a port
        self.addr = self.sock.getsockname()
        self.host = self.addr[0]
        self.port = self.addr[1]
        self.server = server                  # server we are part of
        self.subscribe_time = 0
        self.function = function              # callback on socket read
        self.notify_msg = None

        # get the address of the event relay process
        if not event_relay_host:
            event_relay_host = os.environ.get("ENSTORE_CONFIG_HOST","")
        if not event_relay_port:
            event_relay_port = DEFAULT_PORT
        self.event_relay_addr = (event_relay_host, event_relay_port)


    # this method must be called if we want to have the event relay forward messages
    # to us.
    def start(self, subscribe_msgs=None, resubscribe_rate=600):
        self.subscribe_msgs = subscribe_msgs
        self.resubscribe_rate = resubscribe_rate

        # subscribe here for the first time, then let the interval timer
        # (which we set below) redo it automatically for us
        self.subscribe()

        # add this socket to the select sockets upon which we wait
        self.server.add_select_fd(self.sock, 0, self.function)
        
        # resubscribe ourselves to the event relay every 10 minutes
        self.server.add_interval_func(self.subscribe, self.resubscribe_rate)

    # send the message to the event relay
    def send(self, msg):
        try:
            msg.send(self.sock, self.event_relay_addr)
        except:
            # this has to be lightweight and foolproof
            pass

    # read a message from the socket
    def read(self, fd=None):
        import sys
        import traceback
        import timeofday
        import e_errors
        import Trace
        if not fd:
            fd = self.sock
        try:
            msg = fd.recv(1024)
        except socket.error, detail:
            return None
        # now decode the message based on the message type, which is always the first
        # word in the text message
        return event_relay_messages.decode(msg)
        
    # subscribe ourselves to the event relay server
    def subscribe(self):
        if not self.notify_msg:
            self.notify_msg = event_relay_messages.EventRelayNotifyMsg(self.host,
                                                                      self.port)
            self.notify_msg.encode(self.subscribe_msgs)
        self.send(self.notify_msg)

    # send the heartbeat to the event realy
    def heartbeat(self):
        self.send(self.heartbeat_msg)

    def start_heartbeat(self, name, heartbeat_interval):
        # we will set up a heartbeat to be sent periodically to the event relay
        # process
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_msg = event_relay_messages.EventRelayAliveMsg(self.host, 
                                                                    self.port)
        self.heartbeat_msg.encode(name)
        self.server.add_interval_func(self.heartbeat, self.heartbeat_interval)


