#!/usr/bin/env python

# $Id$

"""os.access isn't supported in Python versions before 1.5.2, so this module provides a portable implementation"""

import os

F_OK, X_OK, W_OK, R_OK = 0, 1, 2, 4

if hasattr(os,"access"):
    access = os.access
else:
    import stat
    stat_masks={
        X_OK: (stat.S_IXOTH, stat.S_IXGRP, stat.S_IXUSR),
        W_OK: (stat.S_IWOTH, stat.S_IWGRP, stat.S_IWUSR),
        R_OK: (stat.S_IROTH, stat.S_IRGRP, stat.S_IRUSR)
        }
    
    def access(filename, my_mode):
        if not os.path.exists(filename):
            return 0
        try:
            s=os.stat(filename)
        except: # if we can't stat it, all bets are off
            return 0

        if my_mode == F_OK: #check for existence only
            return 1
            
        my_uid=os.getuid() # not geteuid, as per POSIX def'n of "access"
        my_gid=os.getgid()
        file_uid=s[stat.ST_UID]
        file_gid=s[stat.ST_GID]
        file_mode=s[stat.ST_MODE]

        for test in X_OK, W_OK, R_OK:
            if test & my_mode == 0:
                continue
            other_mask, group_mask, user_mask = stat_masks[test]
            if file_uid==my_uid:
                if not (file_mode & user_mask):
                    return 0
            elif file_gid==my_gid:
                if not (file_mode & group_mask):
                    return 0
            else:
                if not (file_mode & other_mask):
                    return 0
        #all tests passed
        return 1

if __name__ == '__main__':
    testname = {F_OK: 'present',
                X_OK: 'executable',
                W_OK: 'writable',
                R_OK: 'readable'
                }
    for file in '/tmp', '/nonesuch', '/usr', '/bin/sh':
        for test in range(8):
            print "Testing if", file, "is",
            if test==0: print "present",
            for mask in F_OK,X_OK,W_OK,R_OK:
                if mask & test: print testname[mask],
            print "...", access(file, test) and "yes" or "no"

            
            
            
