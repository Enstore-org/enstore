import string
import types

# event relay message types, add new ones to the list below too
ALL = "all"
NOTIFY = "notify"
ALIVE = "alive"
NEWCONFIGFILE = "newconfigfile"
MSG_FIELD_SEPARATOR = " "

def decode_type(msg):
    return string.split(msg, MSG_FIELD_SEPARATOR, 1)


class EventRelayMsg:

    def __init__(self, host="", port=-1):
	self.type = ""
	self.extra_info = ""
	self.host = host
	if type(port) == types.StringType:
	    self.port = string.atoi(port)
	else:
	    self.port = port

    def message(self):
	return("%s%s%s"%(self.type, MSG_FIELD_SEPARATOR, self.extra_info))

    def send(self, sock, event_relay_addr):
	sock.sendto(self.message(), (event_relay_addr))

    def encode_addr(self):
	return "%s %s"%(self.host, self.port)

"""
Message format :  notify (host, port) msg_type1 msg_type1 ...

(this message is sent to the event relay)
"""
class EventRelayNotifyMsg(EventRelayMsg):

    def encode(self, msg_type_l):
	self.type = NOTIFY
	self.extra_info = self.encode_addr()
	for msg_type in msg_type_l:
	    self.extra_info = "%s%s%s"%(self.extra_info, MSG_FIELD_SEPARATOR, msg_type)

    def decode(self, msg):
	self.type, self.extra_info = decode_type(msg)
	self.host, self.port, self.msg_types = string.split(self.extra_info, 
							    MSG_FIELD_SEPARATOR, 2)
	
"""
Message format:   alive (host, port) server_name 

(this message is sent to and received from the event relay)
"""
class EventRelayAliveMsg(EventRelayMsg):

    def decode(self, msg):
	self.type, self.extra_info = decode_type(msg)
	self.host, self.port, self.server = string.split(self.extra_info, 
							 MSG_FIELD_SEPARATOR, 2)

    def encode(self, name):
	self.type = ALIVE
	self.extra_info = self.encode_addr()
	self.extra_info = "%s%s%s"%(self.extra_info, MSG_FIELD_SEPARATOR, name)

"""
Message format:  newconfigfile (host, port)

(this message is sent to and received from the event relay)
"""
class EventRelayNewConfigFileMsg(EventRelayMsg):

    def decode(self, msg):
	self.type, self.extra_info = decode_type(msg)
	self.host, self.port = string.split(self.extra_info, 
					    MSG_FIELD_SEPARATOR, 1)

    def encode(self):
	self.type = NEWCONFIGFILE
	self.extra_info = self.encode_addr()

# list of supported messages
SUPPORTED_MESSAGES = {NOTIFY : EventRelayNotifyMsg,
		      ALIVE :  EventRelayAliveMsg,
		      NEWCONFIGFILE :EventRelayNewConfigFileMsg }

def decode(msg):
    type, extra_info = decode_type(msg)
    msg_class = SUPPORTED_MESSAGES.get(type, None)
    if msg_class:
	decoded_msg = msg_class()
	decoded_msg.decode(msg)
    else:
	decoded_msg = None
    return decoded_msg


