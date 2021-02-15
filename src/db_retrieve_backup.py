#!/usr/bin/env python

# db_retrieve_backup.py -- retrieve database files from backup

from __future__ import print_function
import restore
import sys
import time

if __name__ == '__main__':

    intf = restore.Interface()
    dbHome, jouHome, bckHost, bckHome = restore.getHomes(intf)

    print(" dbHome =", dbHome)
    print("jouHome =", jouHome)
    print("bckHost =", bckHost)
    print("bckHome =", bckHome)

    bckTime = restore.parse_time(intf.args)
    print("Retrieving from", end=' ')
    if bckTime == -1:
        print("last backup?", end=' ')
    else:
        print(time.ctime(bckTime) + '?', end=' ')
    print("(y/n) ", end=' ')
    ans = sys.stdin.readline()
    if ans[0] != 'y' and ans[0] != 'Y':
        sys.exit(0)

    restore.retrieveBackup(dbHome, jouHome, bckHost, bckHome, bckTime)
