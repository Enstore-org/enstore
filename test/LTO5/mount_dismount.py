#!/usr/bin/env python

from utils import *
import pg
import random
import os
import volume_assert
import media_changer_client
import volume_clerk_client
import e_errors

#Q="select v.label from volume v where v.library='%s' and v.file_family='%s' order by v.label limit 1"
Q="select v.label from volume v where v.library='%s' order by v.label limit 1"

def mount_dismount(i, job_config):
    enstoredb = job_config.get("database")
    db = pg.DB(host  = enstoredb.get('db_host', "localhost"),
               dbname= enstoredb.get('dbname', "enstoredb"),
               port  = enstoredb.get('db_port', 5432),
               user  = enstoredb.get('dbuser_reader', "enstore_reader"))
    #res=db.query(Q%(job_config.get('library'),job_config.get('hostname')))
    res=db.query(Q%(job_config.get('library'),))
    if res.ntuples() == 0 :
        print_error("library %s, file_family %s, There are no files to read"%(job_config.get('library'),
                                                                              job_config.get('hostname')))
        return 1
    volume=res.getresult()[0][0]
    db.close()
    number_of_mounts_dismounts = job_config.get("number_of_mounts")
    mc_device = job_config.get('mover').get('mc_device')
    media_changer = job_config.get('mover').get('media_changer')
    mcc = media_changer_client.MediaChangerClient((enstore_functions2.default_host(),
                                                   enstore_functions2.default_port()),
                                                  media_changer)

    vcc = volume_clerk_client.VolumeClerkClient(mcc.csc)
    vol_ticket = vcc.inquire_vol(volume)

    print_message("Starting mount/dismount tape test for volume %s "%(volume,))

    for i in range(number_of_mounts_dismounts):
        if os.path.exists(STOP_FILE): break
        print_message("Starting mount %d of %d"%(i,number_of_mounts_dismounts,))

        ticket = mcc.loadvol(vol_ticket, mc_device, mc_device)
        if ticket['status'][0] != e_errors.OK:
            print_error("Failed to mount tape %s, %d of %d %s"%(volume,i,number_of_mounts_dismounts,str(ticket['status'])))
            return 1

        print_message("Starting dismount %d of %d"%(i,number_of_mounts_dismounts,))
        ticket = mcc.unloadvol(vol_ticket, mc_device, mc_device)
        if ticket['status'][0] != e_errors.OK:
            print_error("Failed to dismount tape %s, %d of %d %s"%(volume,i,number_of_mounts_dismounts,str(ticket['status'])))
            return 1
    return 0

if __name__ == "__main__":
    main(mount_dismount,number_of_threads=1)
