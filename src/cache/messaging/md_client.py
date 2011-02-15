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
# Migration Dispatcher Commands
#===============================
class MDCommand(EnqMessage):
    """ Message: Base class for Migration Dispatcher Commands
    """
    def __init__(self, type=None, content = None ):
        if [type, content].count(None) != 0:
            raise e_errors.EnstoreError(None, "type or file list is undefined", e_errors.WRONGPARAMETER)
        super(EnqMessage,self).__init__(type=type, content=content)

class MDCPurge(MDCommand):
    """ Message: Migration Dispatcher Purge Command
    """
    def __init__(self, file_list = None ):
        super(MDCommand,self).__init__(type=mt.MDC_PURGE, content=file_list)

class MDCArchive(MDCommand):
    """ Message: Migration Dispatcher Archive Command
    """
    def __init__(self, file_list = None ):
        super(MDCommand,self).__init__(type=mt.MDC_ARCHIVE, content=file_list)

class MDCStage(MDCommand):
    """ Message: Migration Dispatcher Stage Command
    """
    def __init__(self, file_list = None ):
        super(MDCommand,self).__init__(type=mt.MDC_STAGE, content=file_list)

#=======================================
# Reply to Migration Dispatcher Command
#=======================================
class MDReply(EnqMessage):
    """ Message: Base class for replies sent by Migration Dispatcher
    """
    def __init__(self, type=None, orig_msg = None, content = None ):
        if [type, orig_msg, content].count(None) != 0:
            raise e_errors.EnstoreError(None, "type, original message or content is undefined", e_errors.WRONGPARAMETER)
       
        super(EnqMessage,self).__init__(type=type, content=content)
        if orig_msg.correlation_id is not None:
            self.msg.correlation_id = orig_msg.correlation_id # reset correlation id

class MDRArchived(MDReply):
    """ Message: Reply to Migration Dispatcher Archive Command
    """
    def __init__(self, content = None ):
        super(MDReply,self).__init__(type=mt.MDR_ARCHIVED, content=content)

class MDRPurged(MDReply):
    """ Message: Reply to Migration Dispatcher Purge Command
    """
    def __init__(self, content = None ):
        super(MDReply,self).__init__(type=mt.MDR_PURGED, content=content)

class MDRStaged(MDReply):
    """ Message: Reply to Migration Dispatcher Stage Command
    """
    def __init__(self, content = None ):
        super(MDReply,self).__init__(type=mt.MDR_STAGED, content=content)
