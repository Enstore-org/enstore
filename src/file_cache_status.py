#!/usr/bin/env python

class CacheStatus:
    CREATED="CREATED"
    PURGING="PURGING"
    PURGED="PURGED"
    STAGING="STAGING"
    CACHED="CACHED"
    STAGING_REQUESTED="STAGING_REQUESTED"
    FAILED="FAILED"

class ArchiveStatus:
    ARCHIVING="ARCHIVING"
    ARCHIVED="ARCHIVED"
    FAILED="FAILED"

if __name__ == "__main__":
    print CacheStatus.CREATED
    print ArchiveStatus.ARCHIVED
    
