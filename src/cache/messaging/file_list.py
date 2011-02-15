#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

#from uuid import UUID
import uuid

# enstore imports
import e_errors
#import cache.messaging.messages.MSG_TYPES as mt
from cache.messaging.messages import MSG_TYPES as mt

class FileListItem():
    def __init__(self, bfid = None, nsid = None, path  = None, libraries = None ):

        if [bfid, nsid, path, libraries].count(None) != 0:
            raise e_errors.EnstoreError(None, "need bfid, nsid, path, libraries", e_errors.WRONGPARAMETER)

        self.bfid = bfid
        self.nsid = nsid
        self.path = path
        # @todo: check type, libraries is a list
        self.libraries = libraries

# @todo: - implemetation

class FileListOp():
    """ File List Operations 
    """
    valid_ops = [mt.ARCHIVE, mt.STAGE, mt.PURGE]

class FileList(list):
    """ File List 
    """

    def __init__(self, **options ):
        pass
        
 # set random UUID if not given in opts

    def get_dict(self):
        d={}
        d['id'] = self.id
        d['flist'] = self.list
        return d
    