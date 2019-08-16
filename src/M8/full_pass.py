#!/usr/bin/env python

from utils import *
import pg
import random
import os
import volume_assert
import delete_at_exit
import time

#Q="select v.label from volume v where v.library='%s' and v.file_family='%s' order by v.label"
Q="select v.label from volume v where v.library='%s' and v.storage_group='%s' and system_inhibit_0 != 'none' and order by v.label limit 1"

def full_pass(i, job_config):
    """
    enstoredb = job_config.get("database")
    db = pg.DB(host  = enstoredb.get('db_host', "localhost"),
               dbname= enstoredb.get('dbname', "enstoredb"),
               port  = enstoredb.get('db_port', 5432),
               user  = enstoredb.get('dbuser_reader', "enstore_reader"))
    #res=db.query(Q%(job_config.get('library'),job_config.get('hostname')))
    res=db.query(Q%(job_config.get('library'),job_config.get('storage_group')))
    if res.ntuples() == 0 :
        print_error("library %s, file_family %s, storage_group %s. There are no files to read"%(job_config.get('library'),
                                                                                                job_config.get('hostname'),
                                                                                                job_config.get('storage_group')))
        db.close()
        return 1
    """
    volumes=job_config.get("vols_for_full_pass_test")
    print "V", volumes
    """
    for row in res.getresult():
        volumes.append(row[0])
    db.close()
    """
    number_of_full_passes = job_config.get("number_of_full_passes")
    time_to_sleep_between_passes = int(job_config.get('mover').get('dismount_delay',30))*3
    for i in range(number_of_full_passes):
        for volume in volumes:
            if os.path.exists(STOP_FILE): return 0
            print_message("Starting pass %d of %d on volume %s"%(i+1,number_of_full_passes,volume))
            intf = volume_assert.VolumeAssertInterface(user_mode=0)
            intf._mode = "admin"
            intf.volume=volume
            intf.crc_check=True
            rc=volume_assert.do_work(intf)
            print "RC", rc
            if rc:
                print_error("volume assert of %s failed, pass %d of %d"%(volume,i,number_of_full_passes))
                return 1
        print_message("Completed full pass %d of %d on %d volumes, sleeping %d seconds"%(i+1,number_of_full_passes,len(volumes),time_to_sleep_between_passes,))
        time.sleep(time_to_sleep_between_passes)
    return 0

if __name__ == "__main__":
    main(full_pass,number_of_threads=1)
