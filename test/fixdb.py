#!/usr/bin/env python

#Clean out the "bfids" records from a volume clerk database.
# Shut down the system before doing this!!!

import sys
import bfid_db
import db

def main(argv):
    if len(argv)!=3:
        print "Usage: %s dbHome jouHome"
        sys.exit(-1)
    dbHome, jouHome=sys.argv[1:]
    

    db = db.DbTable("volume", dbHome, jouHome, ['library', 'file_family'])
    bfid_db=bfid_db.BfidDb(dbHome)

    db.cursor('open')
    vol,data = d.cursor('first')

    while vol:
        data=db[vol]
        if data.has_key('bfids'):
            print "clobbering bfids for volume", vol
            bfids=data['bfids']
            del data['bfids']
            #update the database
            db[vol]=data
            bfid_db.init_dbfile(vol)
            for bfid in bfids:
                bfid_db.add_bfid(vol,bfid)
        vol,data=db.cursor('next')

if __name__=='__main__':
    main(sys.argv)
    
