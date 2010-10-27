#!/usr/bin/env python

from utils import *
import pg
import random
import os
import volume_assert
import delete_at_exit

Q="select v.label from volume v where v.library='%s' and v.file_family='%s' order by v.label limit 1"

def full_pass(i, job_config):
    enstoredb = job_config.get("database")
    db = pg.DB(host  = enstoredb.get('db_host', "localhost"),
               dbname= enstoredb.get('dbname', "enstoredb"),
               port  = enstoredb.get('db_port', 5432),
               user  = enstoredb.get('dbuser_reader', "enstore_reader"))
    res=db.query(Q%(job_config.get('library'),job_config.get('hostname')))
    if res.ntuples() == 0 :
        print_error("library %s, file_family %s, There are no files to read"%(job_config.get('library'),
                                                                              job_config.get('hostname')))
        return 1
    volume=res.getresult()[0][0]
    volume='TEST54'
    number_of_full_passes = job_config.get("number_of_full_passes")
    for i in range(number_of_full_passes):
        print_message("Starting pass %d of %d"%(i,number_of_full_passes,))
        intf = volume_assert.VolumeAssertInterface(user_mode=0)
        intf._mode = "admin"
        intf.volume=volume
        intf.crc_check=True
        rc=volume_assert.do_work(intf)
        if rc:
            print_error("volume assert of %s failed, pass %d of %d"%(volume,i,number_of_full_passes))

if __name__ == "__main__":
    main(full_pass,number_of_threads=1)
