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
# from cache.messaging.messages import MSG_TYPES as mt

class EnqMessage(qpid.messaging.Message):
    """ Base class for enstore cache messages
    """
    def __init__(self, type = None, *args, **kwargs):

        if type is None :
            raise e_errors.EnstoreError(None, "message type undefined", e_errors.WRONGPARAMETER)

        qpid.messaging.Message.__init__(self, *args, **kwargs) 

        # self.correlation_id can be set in base class Message through **kwargs.
        # set correlation_id here if it has not been set in Message constructor
        if self.correlation_id is None:
            self.correlation_id = str(uuid.uuid4()) # make a random UUID

#     enstore message protocol version: 
#       major is placed into properties (messaging protocol compatibility)
#       minor is not in the header (all messages with the same major a compatible)
        self.properties["version"] = 1         
        if kwargs.has_key("reply_to"):
            self.reply_to = kwargs['reply_to']
        self.properties["en_type"] = type

