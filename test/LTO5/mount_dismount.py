#!/usr/bin/env python
##############################################################################
#
# This test performs mount/dismounts on a tape from a given library
#
##############################################################################
from utils import *
import pg
import random
import os
import volume_assert
import media_changer_client
import volume_clerk_client
import e_errors
import mover_client
import enstore_constants

#Q="select v.label from volume v where v.library='%s' and v.file_family='%s' order by v.label limit 1"
Q="select v.label from volume v where v.library='%s' and v.storage_group='%s' order by v.label limit 1"

def encp_random_file(input_list, done_list):
    input_list_length=len(input_list)
    file_position=random.randint(0,input_list_length-1)
    file_name=input_list.pop(file_position)
    done_list.append(file_name)
    output=file_name.split("/")[-1]
    if os.path.exists(output):
        os.unlink(output)
    output="/dev/null"
    print_message("Read random file %s"%(file_name,))
    cmd="encp %s %s"%(file_name,output,)
    rc=execute_command(cmd)
    if rc:
        os.system("touch %s"%(STOP_FILE))
        return rc
    return 0

def mount_dismount(i, job_config):
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

        return 1
    volume=res.getresult()[0][0]
    #
    # get list of files from this volume
    #
    Q1="select f.pnfs_path  from file f, volume v where f.volume=v.id and v.label='%s' and f.deleted='n' order by f.location_cookie"%(volume,)
    res=db.query(Q1)
    file_list=[]
    done_files=[]
    for row in res.getresult():
        file_list.append(row[0])
    db.close()

    number_of_mounts_dismounts = job_config.get("number_of_mounts")
    mc_device                  = job_config.get('mover').get('mc_device')
    media_changer              = job_config.get('mover').get('media_changer')
    mcc = media_changer_client.MediaChangerClient((enstore_functions2.default_host(),
                                                   enstore_functions2.default_port()),
                                                  media_changer)
    mover_name = job_config.get('mover').get('mover')
    moverClient               = mover_client.MoverClient(job_config.get('csc'),
                                                         mover_name,
                                                         enstore_constants.NO_LOG | enstore_constants.NO_ALARM,
                                                         3, 2)

    vcc = volume_clerk_client.VolumeClerkClient(mcc.csc)
    vol_ticket = vcc.inquire_vol(volume)

    print_message("Starting mount/dismount tape test for volume %s "%(volume,))

    for i in range(number_of_mounts_dismounts):
        if check_stop_file():
            return 0
        print_message("Starting mount %d of %d"%(i,number_of_mounts_dismounts,))

        if len(file_list) == 0 :
            file_list=done_files[:]
            del done_files[:]

        rc=encp_random_file(file_list,done_files)

        if rc:
            return 1
        print_message("Starting dismount %d of %d"%(i,number_of_mounts_dismounts,))

        mover_state = ""
        status = moverClient.status(3, 2)
        if e_errors.is_ok(status['status'][0]):
            mover_state = status['state']
        else:
            print_error("Failed to get mover status %s "%(status,))
            return 1

        max_tries  = 20
        tries = 0

        while mover_state != 'IDLE' and tries < max_tries :
            if check_stop_file():
                return 0
            tries += 1
            print_message("Mover is in state '%s'. Sleeping 40 seconds before querying its status again"%(mover_state,))
            time.sleep(40)
            status = moverClient.status(3, 2)
            if e_errors.is_ok(status['status'][0]):
                mover_state = status['state']
            else:
                print_error("Failed to get mover status %s "%(status,))
                return 1

        if mover_state != 'IDLE' :
            ticket = mcc.unloadvol(vol_ticket, mc_device, mc_device)
            while ticket['status'][0] in [e_errors.MC_QUEUE_FULL]:
                print_error("Failed to dismount tape %s, %d of %d %s"%(volume,i,number_of_mounts_dismounts,str(ticket['status'])))
                print_error("Retrying in 30 seconds ....")
                time.sleep(30)
                ticket = mcc.unloadvol(vol_ticket, mc_device, mc_device)
                if check_stop_file():
                    return 0
                if ticket['status'][0] != e_errors.OK and ticket['status'][0] not in [e_errors.MC_QUEUE_FULL ]:
                    print_error("Failed to dismount tape %s, %d of %d %s"%(volume,i,number_of_mounts_dismounts,str(ticket['status'])))
                    return 1

    return 0

if __name__ == "__main__":
    main(mount_dismount,number_of_threads=1)
