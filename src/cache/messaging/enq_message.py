#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

# qpid / amqp
import qpid.messaging

import uuid

# enstore imports
import e_errors
from cache.messaging.messages import MSG_TYPES as mt

#import cache.messaging.messages as mt

class EnqMessage():
    """ Base class for enstore cache messages
    """
    def __init__(self, type = None, content = None, subject = None ):

        if type is None :
            raise e_errors.EnstoreError(None, "message type undefined", e_errors.WRONGPARAMETER)

        self.msg = qpid.messaging.Message(content=content)

        self.msg.id = 0                 # @todo
        self.msg.reply_to = None        # @todo
        self.msg.correlation_id = uuid.uuid4() # make a random UUID @todo for now
        self.msg.subject = subject

        self.msg.properties = {}
        self.msg.properties["type"] = type
        self.msg.properties["version"] = (0,1) # message version: (major,minor) 

