#!/usr/bin/env python

from utils import *
import pg
import random
import os
import string

Q="select f.pnfs_path  from file f where f.volume in (select id from volume v where v.file_family in ('%s') and v.system_inhibit_1='full' and v.label not in ('%s') order by v.label) and f.deleted='n' order by f.location_cookie"
Q1="select v.label from volume v where v.system_inhibit_1='full' and v.file_family='%s' order by v.label limit 1"

def random_loop(n,input_list,done_list):
    if n<=0 : return
    print_message("Doing %d"%(n,))
    n = n - 1
    counter=0
    while len(input_list)>0:
        number_of_files=len(input_list)
        file_position=random.randint(0,number_of_files-1)
        file_name=input_list.pop(file_position)
        done_list.append(file_name)
        output=file_name.split("/")[-1]
        if os.path.exists(output):
            os.unlink(output)
        cmd="encp %s %s"%(file_name,output,)
        rc=execute_command(cmd)
        if rc:
            os.system("touch %s"%(STOP_FILE))
            return rc
        counter = counter + 1
        if counter % 100 :
            print_message("Completed %4.2f%%"%(float(counter)/float(number_of_files)*100.))
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
    #
    # volumes to exclude
    #
    exclude_volumes=[]
    for mover in job_config['mount_movers']:
        q=Q1%(mover,)
        for row in db.query(q).getresult() :
            exclude_volumes.append(row[0])
    file_families = {}
    for i in job_config['mount_movers'] + job_config['read_movers']:
        file_families[i] = 0
    q=Q%(string.join(file_families.keys(),"','"),string.join(exclude_volumes,"','"),)
    res=db.query(q)
    if res.ntuples() == 0 :
        print_error("library %s, file_family %s, There are no files to read"%(job_config.get('library'),
                                                                              job_config.get('hostname')))
        return 1
    file_list = []
    done_files = []
    for row in res.getresult():
        file_list.append(row[0])
    db.close()
    random_loop(job_config.get('number_of_read_passes'),file_list,done_files)
    return 0

if __name__ == "__main__":
    main(read,number_of_threads=1)
