#!/usr/bin/env python

#Clean out the "bfids" records from a volume clerk database.
# Shut down the system before doing this!!!

import sys
import bfid_db
import db

def main(argv):
    if len(argv)!=3:
        print "Usage: %s dbHome jouHome"%argv[0]
        sys.exit(-1)
    dbHome, jouHome=sys.argv[1:]
    

    d = db.DbTable("volume", dbHome, jouHome, ['library', 'file_family'])
    bdb=bfid_db.BfidDb(dbHome)

    d.cursor('open')
    vol,data = d.cursor('first')

    while vol:
        data=d[vol]
        if data.has_key('bfids'):
            print "clobbering bfids for volume", vol
            bfids=data['bfids']
            del data['bfids']
            #update the database
            d[vol]=data
            bdb.init_dbfile(vol)
            for bfid in bfids:
                bdb.add_bfid(vol,bfid)
        vol,data=d.cursor('next')

if __name__=='__main__':
    main(sys.argv)
    
