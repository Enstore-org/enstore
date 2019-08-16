#!/usr/bin/env python

from utils import *
import pg
import random
import os
import string
import time

#Q="select f.pnfs_path  from file f where f.volume in (select id from volume v where v.file_family in ('%s') and v.system_inhibit_1='full' and v.label not in ('%s') order by v.label) and f.deleted='n' order by f.location_cookie"
#Q1="select v.label from volume v where v.system_inhibit_1='full' and v.file_family='%s' order by v.label limit 1"

#Q="select f.pnfs_path  from file f where f.volume in (select id from volume v where v.library='%s' and v.file_family='%s' order by v.label limit 1) and f.deleted='n' order by f.location_cookie"

Q="select f.pnfs_path  from file f where f.volume in (select id from volume v where v.library='%s' and v.storage_group='%s' and v.system_inhibit_0='none' order by v.label limit 1000000) and f.deleted='n' order by f.location_cookie"

t0=time.time()

def random_loop(n,input_list,done_list):
    delta = int((time.time()-t0)/3600./24.)
    print_message("Doing %d"%(delta,))
    if delta >= n : return 0
    counter=0
    input_list_length=len(input_list)
    start=time.time()
    while len(input_list)>0:
        if os.path.exists(STOP_FILE):
            return 0
        number_of_files=len(input_list)
        file_position=random.randint(0,number_of_files-1)
        file_name=input_list.pop(file_position)
        done_list.append(file_name)
        #output=file_name.split("/")[-1]
        output="/dev/null"
        cmd="encp --threaded %s %s"%(file_name,output,)
        rc=execute_command(cmd)
        if os.path.exists(output) and output != "/dev/null":
            os.unlink(output)
        if rc:
            os.system("touch %s"%(STOP_FILE))
            return rc
        counter = counter + 1
        if not counter % 100 :
            print_message("Completed %d transfers (%4.2f%%)"%(counter, float(counter)/float(input_list_length)*100.))
    print_message("Completed full pass on the tape %d transfers (100%%), took %d seconds"%(input_list_length,int(time.time()-start)))
    time.sleep(120)
    random_loop(n,done_list,input_list)

def read(i,job_config):
    #
    # create list of files
    #
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
    file_list =  []
    done_files = []
    for row in res.getresult():
        file_list.append(row[0])
    db.close()
    rc=random_loop(job_config.get('number_of_days'),file_list,done_files)
    return rc

if __name__ == "__main__":
    main(read,number_of_threads=10)
