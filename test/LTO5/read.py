#!/usr/bin/env python

from utils import *
import pg
import random
import os

Q="select f.pnfs_path  from file f, volume v where f.volume=v.id and v.library='%s' and v.file_family='%s' and f.deleted='n' and v.system_inhibit_1='full' order by v.label, f.location_cookie limit 1"
def random_loop(n,input_list,done_list):
    if n<=0 : return
    print_message("Doing %d"%(n,))
    n = n - 1
    number_of_files=len(input_list)
    counter=0
    while len(input_list)>0:
        file_position=random.randint(0,number_of_files-1)
        file_name=input_list.pop(file_position)
        done_list.append(file_name)
        output="dummy"
        if os.path.exists(output):
            os.unlink(output)
        cmd="encp %s %s"%(file_name,output,)
        rc=execute_command(cmd)
        if rc:
            os.system("touch %s"%(STOP_FILE))
            return rc
        counter = counter + 1
        if counter % 10 :
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
    res=db.query(Q%(job_config.get('library'),job_config.get('hostname')))
    if res.ntuples() == 0 :
        print_error("library %s, file_family %s, There are no files to read"%(job_config.get('library'),
                                                                              job_config.get('hostname')))
        return 1
    file_list = []
    done_files = []
    for row in res.getresult():
        file_list.append(row[0])
    db.close()
    random_loop(200,file_list,done_files)
    return 0

if __name__ == "__main__":
    main(read,number_of_threads=1)
