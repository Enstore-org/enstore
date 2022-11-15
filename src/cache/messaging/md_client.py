#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

# qpid / amqp
#import qpid.messaging

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
        EnqMessage.__init__(self, type=type, content=content)

class MDCPurge(MDCommand):
    """ Message: Migration Dispatcher Purge Command
    """
    def __init__(self, file_list = None ):
        MDCommand.__init__(self,type=mt.MDC_PURGE, content=file_list)

class MDCArchive(MDCommand):
    """ Message: Migration Dispatcher Archive Command
    """
    def __init__(self, file_list = None ):
        MDCommand.__init__(self,type=mt.MDC_ARCHIVE, content=file_list)

class MDCStage(MDCommand):
    """ Message: Migration Dispatcher Stage Command
    """
    def __init__(self, file_list = None ):
        MDCommand.__init__(self,type=mt.MDC_STAGE, content=file_list)

#=======================================
# Reply on Migration Dispatcher Command
#=======================================
class MDReply(EnqMessage):
    """ Message: Base class for replies sent by Migration Dispatcher
    """
    def __init__(self, type=None, orig_msg = None, content = None ):
        if [type, orig_msg, content].count(None) != 0:
            raise e_errors.EnstoreError(None, "type, original message or content is undefined", e_errors.WRONGPARAMETER)

        EnqMessage.__init__(self, type=type, content=content)       
        # @todo: fix, set correlation_id in args to constructor
        if orig_msg.correlation_id is not None:
            self.correlation_id = orig_msg.correlation_id # reset correlation id

class MDRArchived(MDReply):
    """ Message: Reply to Migration Dispatcher Archive Command
    """
    def __init__(self, orig_msg = None, content = None ):
        MDReply.__init__(self, type=mt.MDR_ARCHIVED, orig_msg = orig_msg, content=content)

class MDRPurged(MDReply):
    """ Message: Reply to Migration Dispatcher Purge Command
    """
    def __init__(self, orig_msg = None, content = None ):
        MDReply.__init__(self, type=mt.MDR_PURGED, orig_msg = orig_msg, content=content)

class MDRStaged(MDReply):
    """ Message: Reply to Migration Dispatcher Stage Command
    """
    def __init__(self, orig_msg = None, content = None ):
        MDReply.__init__(self, type=mt.MDR_STAGED, orig_msg = orig_msg, content=content)

if __name__ == "__main__":   # pragma: no cover
    l = ["a","b"]
    
    # Commands:
    ma = MDCArchive( l )
    print "MDCArchive: %s" % (ma,)
    
    mp = MDCPurge( l )
    print "MDCPurge: %s" % (mp,)

    ms = MDCStage( l )
    print "MDStage: %s" % (ms,)
    
    # Replies
    # reply to original message 'ma' with list 'l'
    ra1 = MDRArchived(ma,l)
    print "MDRArchived: %s" % (ra1,)
    
    ra2= MDRArchived(orig_msg=ma,content=l)
    print "MDRArchived: %s" % (ra2,)
    
    rp = MDRPurged(orig_msg=mp, content=l)
    print "MDRPurged: %s" % (rp,)
    
    rs = MDRStaged(orig_msg=ms, content=l)
    print "MDRStaged: %s" % (rs,)
