#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################
import types

# enstore imports
import e_errors

#from cache.messaging.messages import MSG_TYPES as mt
#valid_operations = [mt.ARCHIVE, mt.STAGE, mt.PURGE]
class FileListItem:
    def __init__(self, bfid = None, nsid = None, path  = None, libraries = None ):

        if [bfid, nsid, path, libraries].count(None) != 0:
            raise e_errors.EnstoreError(None, "need bfid, nsid, path, libraries", e_errors.WRONGPARAMETER)

        if type(libraries) != types.ListType:
            raise e_errors.EnstoreError(None, "libraries must be a list", e_errors.WRONGPARAMETER)
        self.d = {"bfid" : bfid,
                  "nsid" : nsid,
                  "path" : path,
                  "libraries" : libraries,
                  }
       
class FileList:
    """
    File List.
    This is a wrapper class which checks if the appending item has a correct format.
    """
    def __init__(self):
        self.file_list = []
        
    def append(self, item):
        if isinstance(item, FileListItem):
           self.file_list.append(item.d)
        else:
            raise e_errors.EnstoreError(None, "item must be FileListItem", e_errors.WRONGPARAMETER)

    def remove(self, item):
        self.file_list.remove(item.d)
        
    def __str__(self):
        return "%s"%(self.file_list)

if __name__ == "__main__":
    l =  FileList()
    i = FileListItem(bfid = "CDMS0001", nsid = "0DAF0001", path = "/diska/aaa/f1", libraries= ["lib1", "lib2"])
    l.append(i)
    print "L",l
    
             

    l.remove(i)
    print l
    
