#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

# enstore imports
import e_errors
from cache.messaging.messages import MSG_TYPES as mt
from cache.messaging.enq_message import EnqMessage

#===============================
# Migration Worker Commands
#===============================
class MWCommand(EnqMessage):
    """ Message: Base class for Migration Worker Commands
    """
    def __init__(self, type=None, content = None, **kwargs):
        if [type, content].count(None) != 0:
            raise e_errors.EnstoreError(None, "type or file list is undefined", e_errors.WRONGPARAMETER)
        EnqMessage.__init__(self, type=type, content=content, **kwargs)

class MWCPurge(MWCommand):
    """ Message: Migration Worker Purge Command
    """
    def __init__(self, file_list = None, **kwargs):
        c = {"file_list":file_list}
        MWCommand.__init__(self,type=mt.MWC_PURGE, content=c, **kwargs)

class MWCArchive(MWCommand):
    """ Message: Migration Worker Archive Command
    """
    # TODO: assert file list items are type of FileListItemWithCRC
    def __init__(self, file_list = None, **kwargs):
        c = {"file_list":file_list}
        MWCommand.__init__(self,type=mt.MWC_ARCHIVE, content=c, **kwargs)

class MWCStage(MWCommand):
    """ Message: Migration Worker Stage Command
    """
    # TODO: assert file list items are type of FileListItemWithCRC
    def __init__(self, file_list = None, **kwargs):
        c = {"file_list":file_list}
        MWCommand.__init__(self,type=mt.MWC_STAGE, content=c, **kwargs)

class MWCStatus(MWCommand):
    """ Message: Migration Worker Status Command
    """
    # Hmm, request_id supposed to be correlation_id of message we are trying to track ...
    def __init__(self, request_id = None, **kwargs):
        if request_id is None :
            raise e_errors.EnstoreError(None, "missing 'request_id' argument to MWCStatus() constructor", e_errors.WRONGPARAMETER)
        c = {"request_id":request_id}
        MWCommand.__init__(self,type=mt.MWC_STATUS, content=c, **kwargs)

#=======================================
# Reply on Migration Worker Command
#=======================================
class MWReply(EnqMessage):
    """ Message: Base class for replies sent by Migration Worker
    """
#    def __init__(self, type=None, orig_msg = None, content = None, **kwargs ):
    def __init__(self, type=None, orig_msg = None, **kwargs ):
#        print "DEBUG %s" % (orig_msg,)
#        print "DEBUG %s" % (content,)
                
        if [type, orig_msg].count(None) != 0:
            raise e_errors.EnstoreError(None, "missing 'type' or 'orig_msg' argument to MWReply() constructor", 
                                        e_errors.WRONGPARAMETER)

        #if not kwargs.has_key("content") and type is not mt.MWR_CONFIRMATION : # logical equivalent
        if not (kwargs.has_key("content") or type is mt.MWR_CONFIRMATION) :
            raise e_errors.EnstoreError(None, "missing 'content' argument to MWReply() constructor", 
                                        e_errors.WRONGPARAMETER)
        
#        # excuse message types where content is not required
#        if type is not mt.MWR_CONFIRMATION and content is None:
#            raise e_errors.EnstoreError(None, "missing 'content' argument to MWReply() constructor", 
#                                        e_errors.WRONGPARAMETER)

        EnqMessage.__init__(self, type=type, **kwargs)       
#        EnqMessage.__init__(self, type=type, content=content, **kwargs)  
        # @todo: fix, set correlation_id in args to constructor
        try:   
            self.correlation_id = orig_msg.correlation_id # reset correlation id
        except:
            pass

        #print "DEBUG corr Id orig  %s" % (orig_msg.correlation_id,)
        #print "DEBUG corr Id reply %s" % (self.correlation_id,)

class MWRArchived(MWReply):
    """ Message: Reply to Migration Worker Archive Command
    """
    def __init__(self, orig_msg = None, file_list = None ):
        c = {"file_list":file_list}
        MWReply.__init__(self, type=mt.MWR_ARCHIVED, orig_msg = orig_msg, content=c)

class MWRPurged(MWReply):
    """ Message: Reply to Migration Worker Purge Command
    """
    def __init__(self, orig_msg = None, file_list = None ):
        c = {"file_list":file_list}
        MWReply.__init__(self, type=mt.MWR_PURGED, orig_msg = orig_msg, content=c)

class MWRStaged(MWReply):
    """ Message: Reply to Migration Worker Stage Command
    """
    def __init__(self, orig_msg = None, file_list = None ):
        c = {"file_list":file_list}
        MWReply.__init__(self, type=mt.MWR_STAGED, orig_msg = orig_msg, content=c)

# @todo Hmm, correlation id is for the correlation id for command message;
# we need to put the correlation of the first id into the reply.
# @todo specify format of content

class MWRStatus(MWReply):
    """ Message: Reply to Migration Worker Status Command
    
        @param content: must be dictionary type
    """
    def __init__(self, orig_msg = None, content = None ):
        MWReply.__init__(self, type=mt.MWR_STATUS, orig_msg = orig_msg, content=content)

class MWRConfirmation(MWReply):
    """ Message: Reply to Confirm receipt of request
    
        @param kwargs: if content is present, it must be a dictionary
    """
    def __init__(self, orig_msg = None, **kwargs):
        MWReply.__init__(self, type=mt.MWR_CONFIRMATION, orig_msg = orig_msg, **kwargs)

if __name__ == "__main__":   # pragma: no cover
    l = ["a","b","c","d"]
    l2= ["x","y"]
    
    # Commands:
    ma = MWCArchive( l )
    print "MWCArchive: %s" % (ma,)
    
    mp = MWCPurge( l )
    print "MWCPurge: %s" % (mp,)

    ms = MWCStage( l )
    print "MWCStage: %s" % (ms,)
    
    mstat = MWCStatus(request_id = 777 )
    print "MWStatus: %s" % (mstat,)
    
    # Replies
    # reply to original message 'ma' with list 'l'
    ra1 = MWRArchived(ma,l)
    print "MWRArchived: %s" % (ra1,)
    
    ra2= MWRArchived(orig_msg=ma,file_list=l2)
    print "MWRArchived: %s" % (ra2,)
    
    rp = MWRPurged(orig_msg=mp, file_list=l2)
    #rp2 = MWRPurged(orig_msg=mp) #ERROR
    print "MWRPurged: %s" % (rp,)
    
    rs = MWRStaged(orig_msg=ms, file_list=l2)
    print "MWRStaged: %s" % (rs,)
    
    rc = MWRConfirmation(orig_msg=ms)
    print "MWRConfirmation: %s" % (rc,)
    
    rconf = MWRConfirmation(orig_msg=ms, content={"status":(e_errors.OK,"work received")}, reply_to="myaddr")
    print "MWRConfirmation: %s" % (rconf,)
    
    rstat = MWRStatus(orig_msg=mstat, content={"status":(e_errors.OK,None)})
    print "MWRStatus: %s" % (rstat,)

    ra2= MWCArchive(l, correlation_id=123245)
    print "MWRArchived: %s" % (ra2,)
