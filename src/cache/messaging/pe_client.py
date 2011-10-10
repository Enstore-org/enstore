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
    # @todo: specify content - see ticket description in messaging HLD
    def __init__(self, content = None):
        PEEvent.__init__(self, type=mt.FILE_DELETED, content=content )

class EvtCacheMissed(PEEvent):
    """ Message: Cache Missed Event
    
        Client attempted to read file from cache and file not found in cache.
    """
    # @todo: specify content
    def __init__(self, content = None ):
        PEEvent.__init__(self, type=mt.CACHE_MISSED, content=content)

class EvtCachePurged(PEEvent):
    """ Message: Cache Purged Event
    
        File replica removed from cache (Migrator)
    """
    # @todo: specify content
    def __init__(self, content = None ):
        PEEvent.__init__(self, type=mt.CACHE_PURGED, content=content)

class EvtCacheWritten(PEEvent):
    """ Message: Cache Written Event
    
        File replica written to cache by client (enstore Disk Mover)
    """
    # @todo: specify content
    def __init__(self, content = None ):
        PEEvent.__init__(self, type=mt.CACHE_WRITTEN, content=content)

class EvtCacheStaged(PEEvent):
    """ Message: Cache Staged Event
    
        File staged from tape to cache (Migrator)
    """
    # @todo: specify content
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
                'id':     'TODO__CACHE_ARCH_ID', #   reserved. @todo: set archive name, e.g. 'cdfen' 
                'type':   'enstore'         #   archive type 
                },
            'ns' : {                        # user namespace (frontend)
                'id':     'TODO__NS_ID',      #   reserved. @todo: set namespace identification, e.g. "cdf"
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
    # input: enstore ticket
    # @todo: set meaningful values in ['cache']['en'], set some stub for now
    en = {}    
    en['node'] = 'node01'
    en['mount'] = '/mnt/cache'
    en['path'] = '000/001/002/003/004/005' # path in cache, here mangled pnfsid
    en['name'] = '0FFF0000123456789ABCDEF0' # file name in cache, here it is same as pnfsid
    en['id'] =   '0FFF0000123456789ABCDEF0'
    en['fsfn'] = '%s:%s/%s/%s' %(en['node'],en['mount'],en['path'],en['name'])
    return en

#======================================================================
# Functions to create message from enstore ticket
#======================================================================

def evt_cache_written_t(fc_ticket):
    """ create event EvtCacheWritten from File Clerk ticket fc_ticket
    """
    vc_keys = ['library','storage_group', 'file_family','file_family_width' ]

    ev = _get_proto(fc_ticket, vc_keys = vc_keys ) 
    ev['cache']['en'] = _set_cache_en(fc_ticket)
    ev['file']['complete_crc'] = fc_ticket['fc']['complete_crc']
       
    return EvtCacheWritten(ev)

def evt_cache_miss_t(fc_ticket):
    """ create event EvtCacheMissed from File Clerk ticket fc_ticket
    """
    vc_keys = ['library','storage_group', 'file_family','file_family_width',
               'external_label','volume_family']
    fc_keys = ['bfid','location_cookie', 'deleted' ]    
        
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

if __name__ == "__main__":
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

#
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

    ew = evt_cache_written_t(tmin)
    print "evt_cache_written_t(): %s" % (ew,)
    
    em = evt_cache_miss_t(tmin)
    print "evt_cache_missed_t(): %s" % (em,)
    
    ep = evt_cache_purged_t(tmin)
    print "evt_cache_purged_t(): %s" % (ep,)

    es = evt_cache_staged_t(tmin)
    print "evt_cache_staged_t(): %s" % (es,)
