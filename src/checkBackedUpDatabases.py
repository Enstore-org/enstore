#!/usr/bin/env python

import os
import sys
import string
import stat
import regex
import time

import e_errors
import timeofday
import hostaddr
import configuration_client
import verify_db

config_port = string.atoi(os.environ.get('ENSTORE_CONFIG_PORT', 7500))
config_host = os.environ.get('ENSTORE_CONFIG_HOST', "localhost")
config=(config_host,config_port)

timeout=15
tries=3

print timeofday.tod(), 'Checking Enstore on',config,'with timeout of',timeout,'and tries of',tries

def check_ticket(server, ticket,exit_if_bad=1):
    if not 'status' in ticket.keys():
        print timeofday.tod(),server,' NOT RESPONDING'
        if exit_if_bad:
            sys.exit(1)
        else:
            return 1
    if ticket['status'][0] == e_errors.OK:
        print timeofday.tod(), server, ' ok'
        return 0
    else:
        print timeofday.tod(), server, ' BAD STATUS',ticket['status']
        if exit_if_bad:
            sys.exit(1)
        else:
            return 1

def check_existance(the_file,plain_file):
    try:
        the_stat = os.stat(the_file)
    except:
        print timeofday.tod(),'ERROR',the_file,'not found'
        sys.exit(1)
    if not plain_file:
        if not stat.S_ISDIR(the_stat[stat.ST_MODE]):
            print timeofday.tod(),'ERROR',the_file,'is not a directory'
            sys.exit(1)
    else:
        if not stat.S_ISREG(the_stat[stat.ST_MODE]):
            print timeofday.tod(),'ERROR',the_file,'is not a regular file'
            sys.exit(1)
    return the_stat

def remove_files(files,dir):
    for f in files:
        ff = dir+'/'+f
        try:
            os.remove(ff)
        except:
            print timeofday.tod(),"ERROR Failed to remove",ff
            sys.exit(1)


csc = configuration_client.ConfigurationClient(config)
backup = csc.get('backup',timeout,tries)
check_ticket('Configuration Server',backup)

backup_node = backup.get('hostip','MISSING')
thisnode = hostaddr.gethostinfo(1)
mynode = thisnode[2][0]
if mynode != backup_node:
    print timeofday.tod(),"ERROR Backups are stored",backup_node,' you are on',mynode,' - database check is not possible'
    sys.exit(1)

backup_dir = backup.get('dir','MISSING')
check_existance(backup_dir,0)

# backups are saved in separate files - get the most recent one
bdirs = os.listdir(backup_dir)
bdirs.sort()
current_backup_dir = bdirs[-1:][0]

container = backup_dir+'/'+current_backup_dir+'/dbase.tar.gz'
con_stat=check_existance(container,1)
mod_time=con_stat[stat.ST_MTIME]
if mod_time+60*5 > time.time():
    print timeofday.tod(),"ERROR: Too new - still being modified? Backup container",container,'last modified on',time.ctime(mod_time)
    sys.exit(1)

check_dir=backup_dir+'/../check-db-tmp' # this is horrible, bu Don would say it is a blemish.
check_existance(check_dir,0)

old_files = os.listdir(check_dir)
remove_files(old_files,check_dir)

here = os.getcwd()
os.chdir(check_dir)
print timeofday.tod(),"Extracting database files from  backup container",container,'last modified on',time.ctime(mod_time)
os.system("/bin/tar -xzf %s"%(container,))

db_files = os.listdir(check_dir)

# check the main files, volume and file, and also any other index files we generage
check_these = ['volume','file']
delete_these = []
for f in db_files:
    if regex.search("\.index$",f)!=-1:
        check_these.append(f)
    else:
        delete_these.append(f)
for f in 'file','volume':
    delete_these.remove(f)
remove_files(delete_these,check_dir)

print timeofday.tod(),'Checking these databases:',check_these
for the_db in check_these:
    print timeofday.tod(),"Checking",the_db
    istat = verify_db.verify_db(the_db)
    if istat != 0:
        os.chdir(here)
        sys.exit(istat)

#remove_files(check_these,check_dir)
os.chdir(here)
