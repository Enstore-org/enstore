#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

import string
import types

# event relay message types, add new ones to the list at the
# bottom too
ALL = "all"
NOTIFY = "notify"
UNSUBSCRIBE = "unsubscribe"
ALIVE = "alive"
NEWCONFIGFILE = "newconfigfile"
CLIENT = "client"
STATE = "state"
TRANSFER = "transfer"
DISCONNECT = "disconnect"
CONNECT = "connect"
UNLOAD = "unload"
LOADED = "loaded"
ENCPXFER = "encp_xfer"
DUMP = "dump"
QUIT = "quit"
DOPRINT = "do_print"
DONTPRINT = "dont_print"
HEARTBEAT = "heartbeat"

MSG_FIELD_SEPARATOR = " "


def decode_type(msg):
    return string.split(msg, MSG_FIELD_SEPARATOR, 1)


# There is a lot of overriding of the encode() function in each of the
# sub-classes of EventRelayMsg.  Thus, turn off this check for this module.
__pychecker__ = "no-override"


class EventRelayMsg:

    def __init__(self, host="", port=-1):
        self.type = ""
        self.extra_info = ""
        self.host = host
        if isinstance(port, bytes):
            self.port = int(port)
        else:
            self.port = port

    def message(self):
        return("%s%s%s" % (self.type, MSG_FIELD_SEPARATOR, self.extra_info))

    def send(self, sock, event_relay_addr):
        sock.sendto(self.message(), (event_relay_addr))

    def encode_addr(self):
        return "%s %s" % (self.host, self.port)

    def encode(self):
        pass

    def encode_self(self):
        self.encode()

# Message format :  notify host port msg_type1 msg_type1 ...


class EventRelayNotifyMsg(EventRelayMsg):

    def encode(self, msg_type_l):
        self.type = NOTIFY
        self.extra_info = self.encode_addr()
        for msg_type in msg_type_l:
            self.extra_info = "%s%s%s" % (
                self.extra_info, MSG_FIELD_SEPARATOR, msg_type)

    def decode(self, msg):
        self.type, self.extra_info = decode_type(msg)
        self.host, self.port, self.msg_types = string.split(self.extra_info,
                                                            MSG_FIELD_SEPARATOR, 2)
        self.msg_type_l = string.split(self.msg_types, MSG_FIELD_SEPARATOR)

    def encode_self(self):
        self.encode(self.msg_type_l)

# Message format :  unsubscribe host port


class EventRelayUnsubscribeMsg(EventRelayMsg):

    def encode(self):
        self.type = UNSUBSCRIBE
        self.extra_info = self.encode_addr()

    def decode(self, msg):
        self.type, self.extra_info = decode_type(msg)
        self.host, self.port = string.split(
            self.extra_info, MSG_FIELD_SEPARATOR, 1)

# Message format:   alive host port server_name


class EventRelayAliveMsg(EventRelayMsg):

    def decode(self, msg):
        self.type, self.extra_info = decode_type(msg)
        params = string.split(self.extra_info, MSG_FIELD_SEPARATOR, 3)
        self.host = params[0]
        self.port = params[1]
        self.server = params[2]
        if len(params) == 4:
            self.opt_string = params[3]
        else:
            self.opt_string = ""

    def encode(self, server, opt_string=""):
        self.type = ALIVE
        self.extra_info = self.encode_addr()
        self.extra_info = "%s%s%s%s%s" % (self.extra_info, MSG_FIELD_SEPARATOR,
                                          server, MSG_FIELD_SEPARATOR, opt_string)

    def encode_self(self):
        self.encode(self.server)

# Message format:  newconfigfile


class EventRelayNewConfigFileMsg(EventRelayMsg):

    def decode(self, msg):
        dec_msg = decode_type(msg)
        self.type = dec_msg[0]
        self.extra_info = ""

    def encode(self):
        self.type = NEWCONFIGFILE
        self.extra_info = ""

# Message format:  client host work file_family more_info


class EventRelayClientMsg(EventRelayMsg):

    def decode(self, msg):
        self.type, self.extra_info = decode_type(msg)
        self.host, self.work, self.file_family, self.more = string.split(self.extra_info,
                                                                         MSG_FIELD_SEPARATOR, 3)

    def encode(self, work, file_family, more_info):
        self.type = CLIENT
        self.extra_info = "%s %s %s %s" % (self.host, work,
                                           file_family, more_info)

    def encode_self(self):
        self.encode(self.work, self.file_family,
                    self.extra_info)

# Message format:  state short_name state_name


class EventRelayStateMsg(EventRelayMsg):

    def __init__(self, short_name=""):
        EventRelayMsg.__init__(self)
        self.short_name = short_name

    def decode(self, msg):
        self.type, self.extra_info = decode_type(msg)
        self.short_name, self.state_name = string.split(self.extra_info,
                                                        MSG_FIELD_SEPARATOR, 1)

    def encode(self, state_name):
        self.type = STATE
        self.extra_info = "%s %s" % (self.short_name, state_name)

    def encode_self(self):
        self.encode(self.state_name)

# Message format:  transfer short_name bytes_read bytes_to_read


class EventRelayTransferMsg(EventRelayMsg):

    def __init__(self, short_name=""):
        EventRelayMsg.__init__(self)
        self.short_name = short_name

    def decode(self, msg):
        self.type, self.extra_info = decode_type(msg)
        self.short_name, self.bytes_read, \
            self.bytes_to_read = string.split(self.extra_info,
                                              MSG_FIELD_SEPARATOR, 2)

    def encode(self, bytes_read, bytes_to_read):
        self.type = TRANSFER
        self.extra_info = "%s %s %s" % (
            self.short_name, bytes_read, bytes_to_read)

    def encode_self(self):
        self.encode(self.bytes_read, self.bytes_to_read)

# Message format:  disconnect short_name client_hostname


class EventRelayDisconnectMsg(EventRelayMsg):

    def __init__(self, short_name=""):
        EventRelayMsg.__init__(self)
        self.short_name = short_name

    def decode(self, msg):
        self.type, self.extra_info = decode_type(msg)
        self.short_name, self.client_hostname = string.split(self.extra_info,
                                                             MSG_FIELD_SEPARATOR, 1)

    def encode(self, client_hostname):
        self.type = DISCONNECT
        self.extra_info = "%s %s" % (self.short_name, client_hostname)

    def encode_self(self):
        self.encode(self.client_hostname)

# Message format:  connect short_name client_hostname


class EventRelayConnectMsg(EventRelayMsg):

    def __init__(self, short_name=""):
        EventRelayMsg.__init__(self)
        self.short_name = short_name

    def decode(self, msg):
        self.type, self.extra_info = decode_type(msg)
        self.short_name, self.client_hostname = string.split(self.extra_info,
                                                             MSG_FIELD_SEPARATOR, 1)

    def encode(self, client_hostname):
        self.type = CONNECT
        self.extra_info = "%s %s" % (self.short_name, client_hostname)

    def encode_self(self):
        self.encode(self.client_hostname)

# Message format:  unload short_name volume


class EventRelayUnloadMsg(EventRelayMsg):

    def __init__(self, short_name=""):
        EventRelayMsg.__init__(self)
        self.short_name = short_name

    def decode(self, msg):
        self.type, self.extra_info = decode_type(msg)
        self.short_name, self.volume = string.split(self.extra_info,
                                                    MSG_FIELD_SEPARATOR, 1)

    def encode(self, volume):
        self.type = UNLOAD
        self.extra_info = "%s %s" % (self.short_name, volume)

    def encode_self(self):
        self.encode(self.volume)

# Message format:  loaded short_name volume


class EventRelayLoadedMsg(EventRelayMsg):

    def __init__(self, short_name=""):
        EventRelayMsg.__init__(self)
        self.short_name = short_name

    def decode(self, msg):
        self.type, self.extra_info = decode_type(msg)
        self.short_name, self.volume = string.split(self.extra_info,
                                                    MSG_FIELD_SEPARATOR, 1)

    def encode(self, volume):
        self.type = LOADED
        self.extra_info = "%s %s" % (self.short_name, volume)

    def encode_self(self):
        self.encode(self.volume)

# Message format:  encp_xfer


class EventRelayEncpXferMsg(EventRelayMsg):

    def decode(self, msg):
        dec_msg = decode_type(msg)
        self.type = dec_msg[0]
        self.extra_info = ""

    def encode(self):
        self.type = ENCPXFER
        self.extra_info = ""


# Message format:  dump
class EventRelayDumpMsg(EventRelayMsg):

    def decode(self, msg):
        dec_msg = decode_type(msg)
        self.type = dec_msg[0]
        self.extra_info = ""

    def encode(self):
        self.type = DUMP
        self.extra_info = ""


# Message format:  quit
class EventRelayQuitMsg(EventRelayMsg):

    def decode(self, msg):
        dec_msg = decode_type(msg)
        self.type = dec_msg[0]
        self.extra_info = ""

    def encode(self):
        self.type = QUIT
        self.extra_info = ""


# Message format:  do_print
class EventRelayDoPrintMsg(EventRelayMsg):

    def decode(self, msg):
        dec_msg = decode_type(msg)
        self.type = dec_msg[0]
        self.extra_info = ""

    def encode(self):
        self.type = DOPRINT
        self.extra_info = ""


# Message format:  dont_print
class EventRelayDontPrintMsg(EventRelayMsg):

    def decode(self, msg):
        dec_msg = decode_type(msg)
        self.type = dec_msg[0]
        self.extra_info = ""

    def encode(self):
        self.type = DONTPRINT
        self.extra_info = ""

# Message format:  heartbeat


class EventRelayHeartbeatMsg(EventRelayMsg):

    def decode(self, msg):
        dec_msg = decode_type(msg)
        self.type = dec_msg[0]
        self.extra_info = ""

    def encode(self):
        self.type = HEARTBEAT
        self.extra_info = ""


# list of supported messages
SUPPORTED_MESSAGES = {NOTIFY: EventRelayNotifyMsg,
                      UNSUBSCRIBE: EventRelayUnsubscribeMsg,
                      ALIVE: EventRelayAliveMsg,
                      NEWCONFIGFILE: EventRelayNewConfigFileMsg,
                      CLIENT: EventRelayClientMsg,
                      STATE: EventRelayStateMsg,
                      TRANSFER: EventRelayTransferMsg,
                      DISCONNECT: EventRelayDisconnectMsg,
                      CONNECT: EventRelayConnectMsg,
                      UNLOAD: EventRelayUnloadMsg,
                      LOADED: EventRelayLoadedMsg,
                      ENCPXFER: EventRelayEncpXferMsg,
                      DUMP: EventRelayDumpMsg,
                      QUIT: EventRelayQuitMsg,
                      DOPRINT: EventRelayDoPrintMsg,
                      DONTPRINT: EventRelayDontPrintMsg,
                      HEARTBEAT: EventRelayHeartbeatMsg,
                      }


def decode(msg):
    decoded_msg = None
    if msg:
        dec_msg = decode_type(msg)
        msg_class = SUPPORTED_MESSAGES.get(dec_msg[0], None)
        if msg_class:
            decoded_msg = msg_class()
            decoded_msg.decode(msg)
    return decoded_msg
