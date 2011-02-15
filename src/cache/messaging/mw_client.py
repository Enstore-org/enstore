#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

# qpid / amqp
import qpid.messaging

# enstore imports
import e_errors
from cache.messaging.messages import MSG_TYPES as mt
from cache.messaging.enq_message import EnqMessage

#===============================
# Migration Worker Commands
#===============================
class MWCommand(EnqMessage):
    """ Message: Migration Dispatcher generic Command
    """
    def __init__(self, type=None, content = None ):
        if [type, content].count(None) != 0:
            raise e_errors.EnstoreError(None, "type or file list is undefined", e_errors.WRONGPARAMETER)
        super(EnqMessage,self).__init__(type=type, content=content)

class MWCPurge(MWCommand):
    """ Message: Migration Dispatcher Purge Command
    """
    def __init__(self, file_list = None ):
        super(MWCommand,self).__init__(type=mt.MWC_PURGE, content=file_list)

class MWCArchive(MWCommand):
    """ Message: Migration Dispatcher Archive Command
    """
    def __init__(self, file_list = None ):
        super(MWCommand,self).__init__(type=mt.MWC_ARCHIVE, content=file_list)

class MWCStage(MWCommand):
    """ Message: Migration Dispatcher Stage Command
    """
    def __init__(self, file_list = None ):
        super(MWCommand,self).__init__(type=mt.MWC_STAGE, content=file_list)

class MWStatus(MWCommand):
    """ Message: Migration Dispatcher Status Command
    """
    def __init__(self, request_id = None ):
        if request_id is None :
            raise e_errors.EnstoreError(None, "request_id undefined", e_errors.WRONGPARAMETER)

        super(MWCommand,self).__init__(type=mt.MWC_STATUS, content=request_id)

#=======================================
# Reply to Migration Worker Commands
#=======================================
class MWReply(EnqMessage):
    """ Message: Generic Reply sent by Migration Worker in reply to command
    """
    def __init__(self, type=None, orig_msg = None, content = None ):
        if [type, orig_msg, content].count(None) != 0:
            raise e_errors.EnstoreError(None, "type, original message or content is undefined", e_errors.WRONGPARAMETER)
       
        super(EnqMessage,self).__init__(type=type, content=content)
        self.msg.correlation_id = orig_msg.correlation_id # reset correlation id

class MWRArchived(MWReply):
    """ Message: Reply to Migration Worker Archive Command
    """
    def __init__(self, content = None ):
        super(MWReply,self).__init__(type=mt.MWR_ARCHIVED, content=content)

class MWRPurged(MWReply):
    """ Message: Reply to Migration Worker Purge Command
    """
    def __init__(self, content = None ):
        super(MWReply,self).__init__(type=mt.MWR_PURGED, content=content)

class MWRStaged(MWReply):
    """ Message: Reply to Migration Worker Stage Command
    """
    def __init__(self, content = None ):
        super(MWReply,self).__init__(type=mt.MWR_STAGED, content=content)
        
class MWRStatus(MWReply):
    """ Message: Reply to Migration Worker Status Command
    """
    def __init__(self, content = None ):
        super(MWReply,self).__init__(type=mt.MWR_STATUS, content=content)
