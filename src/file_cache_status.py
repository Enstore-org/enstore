#!/usr/bin/env python

class CacheStatus:
    CREATED="CREATED"
    PURGING="PURGING"
    PURGED="PURGED"
    CACHED="CACHED"

if __name__ == "__main__":
    print CacheStatus.CREATED
