#!/usr/bin/env python

import sys
import time
import db

def verify_db(dbname):
    count=0
    try:
        d = db.DbTable(dbname,'.','.',[])
        c = d.newCursor()
        t0 = time.time()
        k,v = c.first()

        while k:
            count=count+1
            v=d[k]
            k,v = c.next()

        delta = time.time()-t0

        c.close()
        if not count:
            print "DATABASE",dbname,"IS CORRUPT. NO KEYS FOUND"
            return 1

        print "%d keys checked ok in database %s. Rate was %.1f keys/S" %(count,dbname,count/delta)
        return 0

    except:
            exc, msg, tb = sys.exc_info()
            print "DATABASE",dbname,"IS CORRUPT. Current count is",count
            print exc, msg
            return 1

if __name__ == "__main__":
    sys.exit(verify_db(sys.argv[1]))
