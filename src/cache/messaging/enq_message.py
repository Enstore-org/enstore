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

#        self.id = 0                 # @todo
#        self.reply_to = None        # @todo

        # @todo : when self.correlation_id is set, send() fails (both for uuid() and 0)
        if self.correlation_id is None:
            self.correlation_id = str(uuid.uuid4()) # make a random UUID

        self.properties["type"] = type
        self.properties["en_type"] = type
#@todo       self.properties["version"] = (0,1) # message version: (major,minor) 

