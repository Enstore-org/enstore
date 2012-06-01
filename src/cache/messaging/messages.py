#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################
import file_cache_status
class MSG_TYPES():
    """
    Enstore File Cache Events and Message Types 
    """
    
    # Define substrings
    #
    NS_EVENT = "NS_" # Namespace Event (e.g. pnfs Event)
    FCH_EVENT = "CACHE_" # File Cache Event
    
    MD_COMMAND = "MDC_" # Command to Migration Dispatcher
    MD_REPLY   = "MDR_" # Reply sent by Migration Dispatcher   
    
    MW_COMMAND = "MWC_" # Command to Migration Worker (Migrator)
    MW_REPLY   = "MWR_" # Reply sent by Migration Worker (Migrator)
    
    # Operations
    ARCHIVE = "ARCHIVE"  # pack and archive to tape
    DELETE = "DELETE"    # delete in namespace
    PACK = "PACK"        # pack files into container
    PURGE = "PURGE"      # remove replica rom cache
    STAGE = "STAGE"      # stage from tape and unpack
    UNPACK = "UNPACK"    # unpack container
    
    # Completion report
    ARCHIVED = file_cache_status.ArchiveStatus.ARCHIVED
    DELETED = "DELETED"
    PACKED = "PACKED"
    PURGED = file_cache_status.CacheStatus.PURGED
    MISSED = "MISSED"    # [file replica] not found [in cache]
    STAGED = "STAGED"
    CACHED = file_cache_status.CacheStatus.CACHED
    WRITTEN = "WRITTEN"  # [file replica] written [into cache,] close() on write.
    CREATED = file_cache_status.CacheStatus.CREATED  # [file replica] created [in cache, creat().] Follows file creation in NS, followed by replica is 'written'
    UNPACKED = "UNPACKED"
    FAILED="FAILED"
    
    CONFIRMATION = "CONFIRMATION"
    
    # Intermediate report
    PURGING = file_cache_status.CacheStatus.PURGING
    STAGING = file_cache_status.CacheStatus.STAGING
    STAGING_REQUESTED = file_cache_status.CacheStatus.STAGING_REQUESTED
    PURGING_REQUESTED = file_cache_status.CacheStatus.PURGING_REQUESTED
    ARCHIVING = file_cache_status.ArchiveStatus.ARCHIVING
    
    # Operation and reply
    STATUS = "STATUS"
    
    ##########################
    # Commands and Replies
    ##########################
    
    # Events Processed by Policy Engine
    FILE_DELETED    = NS_EVENT + DELETED  # file was deleted in Namespace
    #
    CACHE_MISSED    = FCH_EVENT + MISSED   # client attempts to read file from cache and file not found in cache.
    CACHE_PURGED    = FCH_EVENT + PURGED  # file copy removed from cache (Migrator)
    CACHE_WRITTEN   = FCH_EVENT + WRITTEN   # file replica written to cache by client (enstore Disk Mover)
    CACHE_STAGED    = FCH_EVENT + STAGED  # file restored from tape to cache (Migrator)
    
    # Commands processed by Migration Dispatcher
    MDC_PURGE      =  MD_COMMAND + PURGE   # Purge cache entry
    MDC_ARCHIVE    =  MD_COMMAND + ARCHIVE    # Package and Write to tape
    MDC_STAGE      =  MD_COMMAND + STAGE    # Read from tape and Unpack
    # Replies sent by Migration Dispatcher
    MDR_PURGED     =  MD_REPLY + PURGED
    MDR_ARCHIVED   =  MD_REPLY + ARCHIVED
    MDR_STAGED     =  MD_REPLY + STAGED
    MDR_CONFIRMATION =  MD_REPLY + CONFIRMATION
    
    # Commands processed by Migrator (Worker)
    MWC_PURGE      =  MW_COMMAND + PURGE    # Purge cache entry
    MWC_ARCHIVE    =  MW_COMMAND + ARCHIVE    # Package and Write to tape
    MWC_STAGE      =  MW_COMMAND + STAGE    # Read from tape and Unpack
    MWC_STATUS     =  MW_COMMAND + STATUS   # query worker status and transfer progress
    # Replies sent by Migrator (Worker)
    MWR_PURGED     =  MW_REPLY + PURGED
    MWR_ARCHIVED   =  MW_REPLY + ARCHIVED
    MWR_STAGED     =  MW_REPLY + STAGED
    MWR_STATUS     =  MW_REPLY + STATUS # Message reporting progress of work in reply to command MWC_STATUS
    MWR_CONFIRMATION =  MW_REPLY + CONFIRMATION # Confirm receipt of work request and send back reply_to
