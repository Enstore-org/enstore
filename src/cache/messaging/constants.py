'''
Created on Sep 7, 2010

define constants
'''

class Constant:
    def __init__(self, name, value=None):
        self.name = name
        self.value = value

    def __repr__(self):
        return self.name

# qpid ports to be used by enstore amqp exchange.
# use standard qpid ports for now
EN_AMQP_PORT  = 5672
EN_AMQPS_PORT = 5671

CLIENT_ACKNOWLEDGE, AUTO_ACKNOWLEDGE = range(2)

#EN_AMQP_PORT = Constant("EN_AMQP_PORT", 5672)
#EN_AMQPS_PORT = Constant("EN_AMQPS_PORT", 5671)
