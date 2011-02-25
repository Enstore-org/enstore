#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

import uuid

# enstore imports
import e_errors

from cache.messaging.messages import MSG_TYPES as mt

class FileListItem(dict):
    def __init__(self, bfid = None, nsid = None, path  = None, libraries = None ):

        if [bfid, nsid, path, libraries].count(None) != 0:
            raise e_errors.EnstoreError(None, "need bfid, nsid, path, libraries", e_errors.WRONGPARAMETER)

        self["bfid"] = bfid
        self["nsid"] = nsid
        self["path"] = path
        # @todo: check type, libraries is a list
        self["libraries"] = libraries

# @todo: - implementation

class FileListOp():
    """ File List Operations 
    """
    valid_ops = [mt.ARCHIVE, mt.STAGE, mt.PURGE]

class FileList(list):
    """ File List 
    """

    def __init__(self, id=None, *options ):
        # set random UUID if not given in opts
        if id is None:
            id = uuid.uuid4()
        self.id = id

    # @todo enforce list item types as FileListItem
