#!/usr/bin/env python

import sys
import time
import db

def verify_db(dbname):
    try:
        d = db.DbTable(dbname,'.','.',[])
        d.cursor('open')
        t0 = time.time()
        k,v = d.cursor('first')
        count=0

        while k:
            count=count+1
            v=d[k]
            k,v=d.cursor('next')

        delta = time.time()-t0

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
