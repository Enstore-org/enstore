#!/usr/bin/env python

class CacheStatus:
    CREATED="CREATED"
    PURGING="PURGING"
    PURGED="PURGED"
    STAGING="STAGING"
    CACHED="CACHED"

class ArchiveStatus:
    ARCHIVING="ARCHIVING"
    ARCHIVED="ARCHIVED"

if __name__ == "__main__":
    print CacheStatus.CREATED
    print ArchiveStatus.ARCHIVED
    
