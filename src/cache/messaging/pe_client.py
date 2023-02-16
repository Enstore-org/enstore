#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

import os.path

# enstore imports
import e_errors
from cache.messaging.messages import MSG_TYPES as mt
from cache.messaging.enq_message import EnqMessage

#===================================
# Policy Engine Events and Commands
#===================================
class PEEvent(EnqMessage):
    """ Message: Base class for Policy Engine Events
    """
    def __init__(self, type=None, content = None ):
        if [type, content].count(None) != 0:
            raise e_errors.EnstoreError(None, "type or content is undefined", e_errors.WRONGPARAMETER)
        durable = True
        EnqMessage.__init__(self, type=type, content=content, durable=durable)

#===================================

class EvtFileDeleted(PEEvent):
    """ Message: File Deleted Event

        File deleted in Namespace
    """
    # message content - see ticket description in messaging HLD, 
    #  or 'tmin' ticket in the test unit for the minimum set of required keys  
    def __init__(self, content = None):
        PEEvent.__init__(self, type=mt.FILE_DELETED, content=content )

class EvtCacheMissed(PEEvent):
    """ Message: Cache Missed Event

        Client attempted to read file from cache and file not found in cache.
    """
    # message content - see ticket description in messaging HLD, 
    #  or 'tmin' ticket in the test unit for the minimum set of required keys  
    def __init__(self, content = None ):
        PEEvent.__init__(self, type=mt.CACHE_MISSED, content=content)

class EvtCachePurged(PEEvent):
    """ Message: Cache Purged Event

        File replica removed from cache (Migrator)
    """
    # message content - see ticket description in messaging HLD, 
    #  or 'tmin' ticket in the test unit for the minimum set of required keys  
    def __init__(self, content = None ):
        PEEvent.__init__(self, type=mt.CACHE_PURGED, content=content)

class EvtCacheWritten(PEEvent):
    """ Message: Cache Written Event

        File replica written to cache by client (enstore Disk Mover)
    """
    # message content - see ticket description in messaging HLD, 
    #  or 'tmin' ticket in the test unit for the minimum set of required keys  
    def __init__(self, content = None ):
        PEEvent.__init__(self, type=mt.CACHE_WRITTEN, content=content)

class EvtCacheStaged(PEEvent):
    """ Message: Cache Staged Event

        File staged from tape to cache (Migrator)
    """
    # message content - see ticket description in messaging HLD, 
    #  or 'tmin' ticket in the test unit for the minimum set of required keys  
    def __init__(self, content = None ):
        PEEvent.__init__(self, type=mt.CACHE_STAGED, content=content)

#======================================================================
# Helper Functions
#======================================================================

def _get_proto(ticket, vc_keys=None, fc_keys=None):
    """
    helper function to create common part of the event
    """
    # @todo: try/catch KeyError - guard against missing key in enstore ticket

    proto = {
        'cache': {                   # -- File Cache specific fields
            'arch': {                       # archive (tape backend)
                'id':     None,             #   reserved. @todo: set archive name, e.g. 'cdfen'
                'type':   'enstore'         #   archive type
                },
            'ns' : {                        # user namespace (frontend)
                'id':     None,             #   reserved. @todo: set namespace identification, e.g. "cdf"
                'type':   'pnfs',           #   user namespace description
                'mnt':    '/pnfs/fnal.gov'  #   mount point in global namespace
                }
            },
        'file': {}
    }

    if vc_keys is not None:
        vc_t = ticket['vc']
        proto['enstore'] = {}
        proto['enstore']['vc'] = {}
        dest= proto['enstore']['vc']
        for k in vc_keys:
            dest[k] = vc_t[k]

    fc_t = ticket['fc']
    proto['file']['name'] = fc_t['pnfs_name0']
    proto['file']['id']   = fc_t['pnfsid']
    proto['file']['size'] = fc_t['size']

    if fc_keys is not None:
        if 'enstore' not in proto:
            proto['enstore'] = {}
        # copy directly to 'enstore' part of the ticket, flattening nested 'fc' dictionary
        for k in fc_keys:
            proto['enstore'][k] = fc_t[k]

    return proto

def _set_cache_en(t):
    """ Fill in dict['cache']['en'] part of message dictionary using enstore ticket
    """
    en = {}
    drive = t["fc"].get("drive",None)
    location_cookie = t["fc"].get("location_cookie",None)
    id = t["fc"].get("pnfsid",None)

    if drive is not None:
        (node,mount,ignore) = drive.split(":")
    else:
        node,mount = ("none",None)
    if location_cookie is not None:
        (path,name)  = os.path.split(location_cookie)

    fsfn="%s:%s"%(node, location_cookie)
    
    en['node']  = node
    en['mount'] = mount
    en['path']  = path
    en['name']  = name
    en['id']    = id
    en['fsfn']  = fsfn
    
    return en

#======================================================================
# Functions to create message from enstore ticket
#======================================================================


def evt_cache_written_fc(encp_ticket,fc_record):
    """ create event EvtCacheWritten from encp_ticket received by File Clerk ticket and FC DB record
    """
    # TODO this need to be streamlined to avoid double copy
    
    fc_ticket={}
    fc_ticket["vc"] = {}
    for key in ("original_library","file_family_width"):
        fc_ticket["vc"][key]=encp_ticket["fc"].get(key,None)

    for key in ("file_family","external_label","storage_group","wrapper"):
        fc_ticket["vc"][key]=fc_record.get(key,None)

    fc_ticket["vc"]["volume_family"]=fc_record.get("storage_group","none")+"."+\
                                      fc_record.get("file_family","none")+"."+\
                                      fc_record.get("wrapper","none")

    fc_ticket["fc"]=fc_record.copy()
 
    fc_ticket["vc"]["library"] = encp_ticket["fc"].get("original_library",None)
    return evt_cache_written_t(fc_ticket)

def evt_cache_written_t(fc_ticket):
    """ create event EvtCacheWritten from File Clerk ticket fc_ticket
    """
    vc_keys = ['library','storage_group', 'file_family','file_family_width',"wrapper",
               "volume_family" ]
    fc_keys = ['bfid','location_cookie', 'deleted' ]

    ev = _get_proto(fc_ticket, vc_keys = vc_keys, fc_keys = fc_keys )
    ev['cache']['en'] = _set_cache_en(fc_ticket)
    ev['file']['complete_crc'] = fc_ticket['fc']['complete_crc']

    return EvtCacheWritten(ev)

def evt_cache_miss_fc(encp_ticket,fc_record):
    #
    # encp_ticket contains single field "bfid"
    #
    fc_ticket={}
    fc_ticket["vc"] = {}
    for key in ("library","storage_group","file_family","wrapper",
                "external_label","volume_family"):
        fc_ticket["vc"][key]=fc_record.get(key,None)
    #
    # this info is not present
    #
    fc_ticket["vc"]["original_library"]=None
    fc_ticket["vc"]["file_family_width"]=1
    fc_ticket["fc"]=fc_record.copy()
    return evt_cache_miss_t(fc_ticket)

def evt_cache_miss_t(fc_ticket):
    """ create event EvtCacheMissed from File Clerk ticket fc_ticket
    """
    vc_keys = ['library','storage_group', 'file_family','file_family_width',
               'external_label','volume_family']
    fc_keys = ['bfid','location_cookie', 'deleted', 'disk_library']

    ev = _get_proto(fc_ticket, vc_keys = vc_keys, fc_keys = fc_keys )
    ev['file']['complete_crc'] = fc_ticket['fc']['complete_crc']
    return EvtCacheMissed(ev)

def evt_cache_purged_t(ticket):
    """ create event EvtCachePurged from ticket ticket
    """
    ev = _get_proto(ticket)
    ev['cache']['en'] = _set_cache_en(ticket)

    return EvtCachePurged(ev)

def evt_cache_staged_t(ticket):
    """ create event EvtCacheStaged from ticket ticket
    """
    fc_keys = ['bfid' ]

    ev = _get_proto(ticket, fc_keys = fc_keys)
    ev['cache']['en'] = _set_cache_en(ticket)

    return EvtCacheStaged(ev)

#======================================================================
# Test
#======================================================================

if __name__ == "__main__":   # pragma: no cover
    t = {"k":"v"}

    # Events:
    efd = EvtFileDeleted(t)
    print "EvtFileDeleted: %s" % (efd,)

    ecm = EvtCacheMissed(t)
    print "EvtCacheMissed: %s" % (ecm,)

    ecp = EvtCachePurged(t)
    print "EvtCachePurged: %s" % (ecp,)

    ecw = EvtCacheWritten(t)
    print "EvtCacheWritten: %s" % (ecw,)

    ecs = EvtCacheStaged(t)
    print "EvtCacheStaged: %s" % (ecs,)

#---
    vct = {
           'library':'TEST_library',
           'storage_group':'TEST_storage_group',
           'file_family':'TEST_file_family',
           'file_family_width':2,
           'wrapper':'TEST_wripper',
           'external_label':'TEST_external_label', # MISS
           'volume_family':'TEST_volume_family' # MISS
           }
    fct = {
           'bfid':'GCMS129683447700000', # or shall it be taken from ticket = {'bfid':'GCMS129683447700000',}
           'location_cookie':'0000_000000000_0000001',
           'pnfs_name0': '/pnfs/fs/usr/data/moibenko/d1/mover.py',
           'pnfsid': '0001000000000000000011F8',
           'size': 1234567890L,
           'complete_crc': 3020422051L, # only in evt_cache_written_t, evt_cache_missed_t
           'deleted': 'no',
           }

    # "minimalistic" encp ticket sent by encp to fc: we need some vc fields here.
    tmin = {
         'vc': vct,
         'fc': fct
         }
#---
#  as of 11/04/2011 :

    encp_tk = {'status': ('ok', None), 
          'r_a': (('131.225.13.37', 42424), 1L, '131.225.13.37-42424-1320439860.181562-31985-47728652978352'), 
          'work': 'set_pnfsid', 
          'fc': {'size': 5189L, 'pnfsvid': '', 'pnfsid': '0001000000000000000031F0', 
                 'pnfs_name0': '/pnfs/fs/usr/data/moibenko/d2/LTO3/LTO3GS/d5.py', 'address': ('131.225.13.32', 7501), 
                 'external_label': 'common:ANM.FF1.cpio_odc:2011-11-04T15:51:08Z', 
                 'drive': 'dmsen06:/data/cache:0', 'library': 'diskSF', 'gid': 6209, 'pnfs_mapname': '', 
                 'complete_crc': 2338790700L, 'original_library': 'LTO3GS', 'mover_type': 'DiskMover', 
                 'file_family_width': 20, 'bfid': 'GCMS132043986900000', 'uid': 5744, 'sanity_cookie': (5189L, 2338790700L), 
                 'location_cookie': '/data/cache/496/3/0001000000000000000031F0'}}

    fc_record = {'storage_group': 'ANM', 'pnfsvid': '', 'uid': 5744, 
                 'pnfs_name0': '/pnfs/fs/usr/data/moibenko/d2/LTO3/LTO3GS/d5.py', 
                 'deleted': 'no', 'archive_status': None, 
                 'cache_mod_time': '2011-11-04 15:51:09', 'update': '2011-11-04 15:51:09.086832', 
                 'library': 'diskSF', 'package_id': None, 'file_family': 'FF1', 
                 'location_cookie': '/data/cache/496/3/0001000000000000000031F0', 'complete_crc': 2338790700L, 
                 'bfid': 'GCMS132043986900000', 
                 'sanity_cookie': (5189L, 2338790700L), 'size': 5189L, 
                 'external_label': 'common:ANM.FF1.cpio_odc:2011-11-04T15:51:08Z', 
                 'drive': 'dmsen06:/data/cache:0', 'wrapper': 'null', 'package_files_count': 0, 
                 'active_package_files_count': -1, 'gid': 6209, 
                 'pnfsid': '0001000000000000000031F0', 'archive_mod_time': None, 
                 'cache_status': 'CACHED'}
#---
    ew = evt_cache_written_t(tmin)
    print "evt_cache_written_t(): %s" % (ew,)

    em = evt_cache_miss_t(tmin)
    print "evt_cache_missed_t(): %s" % (em,)

    ep = evt_cache_purged_t(tmin)
    print "evt_cache_purged_t(): %s" % (ep,)

    es = evt_cache_staged_t(tmin)
    print "evt_cache_staged_t(): %s" % (es,)
#---    
    ew_fc = evt_cache_written_fc(encp_tk,fc_record)
    print "evt_cache_written_fc(): %s" % (ew_fc,)

    em_fc = evt_cache_miss_fc(encp_tk,fc_record)
    print "evt_cache_miss_fc(): %s" % (em_fc,)
