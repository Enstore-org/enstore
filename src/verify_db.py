#!/usr/bin/env python

import sys
import time
import db

def verify_db(dbname):
    count=0
    try:
        d = db.DbTable(dbname,'.','.',[])
        c = d.newCursor().C	# use low level cursor
        t0 = time.time()
        k,v = c.first()

        while k:
            count=count+1
            # v=d[k]
            try:
                k,v = c.next()
            except KeyError:
                kl,vl = c.last()
                if kl == k and vl == v:
                    k = None
                else:
                    raise EOFError, 'Current: '+`k`+':'+`v`+'\n'+'   Last: '+`kl`+':'+`vl`
            if count % 1000 == 0:
                print count

        # If it gets so far, c must point to the last record
        # check if the cursor is currupted (beyond the last record)

        notok = 0
        try:
            k,v = c.next()
            notok = 1
        except:
            pass

        if notok:
            raise IndexError, "cursor advanced beyond the last record"

        delta = time.time()-t0

        c.close()
        d.close()

        if not count:
            print "Backup of DATABASE",dbname,"IS CORRUPT. NO KEYS FOUND"
            return 1

        print "%d keys checked ok in database %s. Rate was %.1f keys/S" %(count,dbname,count/delta)
        return 0

    except:
            c.close()
            d.close()
            exc, msg, tb = sys.exc_info()
            print "Backup of DATABASE",dbname,"IS CORRUPT. Current count is",count
            print exc
            print msg
            return 1

if __name__ == "__main__":
    sys.exit(verify_db(sys.argv[1]))
