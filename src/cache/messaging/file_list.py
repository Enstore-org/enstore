#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################
import types
import time

# enstore imports
import e_errors

#from cache.messaging.messages import MSG_TYPES as mt
#valid_operations = [mt.ARCHIVE, mt.STAGE, mt.PURGE]
class FileListItem:
    def __init__(self, bfid=None, nsid=None, path=None, libraries=None ):

        if [bfid, nsid, path, libraries].count(None) != 0:
            raise e_errors.EnstoreError(None, "need bfid, nsid, path, libraries", e_errors.WRONGPARAMETER)

        if type(libraries) != types.ListType:
            raise e_errors.EnstoreError(None, "libraries must be a list", e_errors.WRONGPARAMETER)
        self.d = {"bfid" : bfid,
                  "nsid" : nsid,
                  "path" : path,
                  "libraries" : libraries,
                  }

class FileListItemWithCRC:
    def __init__(self, bfid=None, nsid=None, path=None, libraries=None, crc=None ):

        if [bfid, nsid, path, libraries, crc].count(None) != 0:
            raise e_errors.EnstoreError(None, "need bfid, nsid, path, libraries, crc", e_errors.WRONGPARAMETER)

        if type(libraries) != types.ListType:
            raise e_errors.EnstoreError(None, "libraries must be a list", e_errors.WRONGPARAMETER)
        self.d = {"bfid" : bfid,
                  "nsid" : nsid,
                  "path" : path,
                  "libraries" : libraries,
                  "complete_crc" : crc,
                  }

#file list states
FILLING = "FILLING" # when file list is be ing filled
FILLED = "FILLED"   # file is is ready to get sent to migrator
ACTIVE = "ACTIVE"   # when file list has been send to migrator queue
class FileList:
    """
    File List.
    
    This is a wrapper class which checks if the appending item has a correct format.
    """
    def __init__(self,
                 id = None,
                 list_type = None,
                 list_name = "",
                 minimal_data_size = 1000000,
                 maximal_file_count = 100,
                 max_time_in_list = 300,
                 disk_library = None):
        self.list_type = list_type
        self.list_id = id # unique list id
        self.list_name = list_name
        self.file_list = []
        self.creation_time = time.time()
        self.minimal_data_size = minimal_data_size
        self.maximal_file_count = maximal_file_count
        self.max_time_in_list = max_time_in_list
        self.disk_library = disk_library
        self.data_size = 0L
        self.full = False
        self.state = None

    def _append(self, item, data_size=0L):
        self.state = FILLING
        if not item.d in self.file_list:
            self.file_list.append(item.d)
            self.data_size = self.data_size + data_size
                                  
            if (self.data_size > self.minimal_data_size or
                len(self.file_list) > self.maximal_file_count or
                (time.time() - self.creation_time) > self.max_time_in_list):
                self.full = True

    def list_expired(self):
        if (time.time() - self.creation_time) > self.max_time_in_list:
           self.full = True
        return self.full

    def append(self, item, data_size=0L):
        if isinstance(item, FileListItem):
            self._append(item, data_size)
        else:
            raise e_errors.EnstoreError(None, "item must be FileListItem", e_errors.WRONGPARAMETER)

    def remove(self, item):
        self.file_list.remove(item.d)

    def __repr__(self):
        return "id=%s name=%s created %s type=%s disk_library=%s, content %s" % \
               (self.list_id, self.list_name,
                time.ctime(self.creation_time),
                self.list_type, self.disk_library, self.file_list)

class FileListWithCRC(FileList):
    """
    File List With CRC.

    This is a wrapper class which checks if the appending item has a correct format.
    """
    def append(self, item, data_size=0L):
        if isinstance(item, FileListItemWithCRC):
            self._append(item, data_size)
        else:
            raise e_errors.EnstoreError(None, "item must be FileListItemWithCRC", e_errors.WRONGPARAMETER)

if __name__ == "__main__":
    l =  FileList()
    i = FileListItem(bfid = "CDMS0001", nsid = "0DAF0001", path = "/diska/aaa/f1", libraries= ["lib1", "lib2"])
    l.append(i)
    print "L", l
    l.remove(i)
    print "L after remove:", l

    lc =  FileListWithCRC(disk_library='DISK_LIBRARY')
    ic = FileListItemWithCRC(bfid = "CDMS0001", nsid = "0DAF0001", path = "/diska/aaa/f1",
                             libraries= ["lib1", "lib2"], crc=3020422051L)
    try:
        print "expected to fail"
        ic2 = FileListItemWithCRC(bfid = "CDMS0001", nsid = "0DAF0001", path = "/diska/aaa/f1",
                                  libraries= ["lib1", "lib2"])
    except Exception, e:
        print "LC constructor failed", e
        pass

    lc.append(ic)
    print "LC", lc
    print "DL", lc.disk_library
    lc.remove(ic)
    print "LC after remove:", lc
