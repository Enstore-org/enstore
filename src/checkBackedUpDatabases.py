#!/usr/bin/env python

import os
import sys
import string
import stat
import regex
import time
import shutil
import traceback
import socket

import e_errors
import timeofday
import hostaddr
import configuration_client
import verify_db


def print_usage():
    print "Usage:", sys.argv[0], "[--help]"
    print "  --help   print this help message"
    print "See configuration dictionary entry \"backup\" for defaults."


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
        traceback.print_exc()
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
            ffstat=os.stat(ff)
            if stat.S_ISDIR(ffstat[stat.ST_MODE]): 
                shutil.rmtree(ff)
            elif stat.S_ISREG(ffstat[stat.ST_MODE]):
                os.remove(ff)
            else:
                print timeofday.tod(),"ERROR Failed to remove",ff,' not a directory or regular file'
                sys.exit(1)
        except:
            traceback.print_exc()
            print timeofday.tod(),"ERROR Failed to remove",ff
            sys.exit(1)



def configure(configuration = None):
    csc = configuration_client.ConfigurationClient()
    backup = csc.get('backup',timeout=15,retry=3)
    check_ticket('Configuration Server',backup)

    #Check both the FQDN and the ip.  On multihomed hosts only checking on
    # my lead to failed operations that would otherwise succed.
    backup_node = backup.get('hostip','MISSING')
    thisnode = hostaddr.gethostinfo(1)
    mynode = thisnode[2][0]
    backup_fqdn = socket.gethostbyaddr(backup_node)[0]
    mynode_fqdn = socket.gethostbyaddr(mynode)[0]
    if mynode != backup_node and backup_fqdn != mynode_fqdn:
        print timeofday.tod(),"ERROR Backups are stored", backup_node,
        print ' you are on',mynode,' - database check is not possible'
        sys.exit(1)
        
    backup_dir = backup.get('dir','MISSING')
    if backup_dir == 'MISSING':
        print timeofday.tod(), "ERROR Backup directory not determined."
        sys.exit(1)
    if configuration:
        check_existance(backup_dir,0)

    check_dir = backup.get('extract_dir','MISSING')
    if check_dir == 'MISSING':
        print timeofday.tod(), "ERROR Extraction directory not determined."
        sys.exit(1)
    if configuration:
        check_existance(check_dir,0)
        old_files = os.listdir(check_dir)
        remove_files(old_files,check_dir)
    
    current_dir = os.getcwd() #Remember the original directory

    print timeofday.tod(), 'Checking Enstore on', csc.get_address()[0], \
          'with timeout of', csc.get_timeout(), 'and retries of', \
          csc.get_retry()

    #Return the directory the backup file is in and the directory the backup
    # file will be ungzipped and untared to, respectively.  Lastly, return
    # the current directory.
    return backup_dir, check_dir, current_dir, backup_node



def check_backup(backup_dir, backup_node):
    # backups are saved in separate files - get the most recent one
    bdirs = os.listdir(backup_dir)
    if len(bdirs) == 0:
        print timeofday.tod(),"ERROR NO Backups found on ", backup_node,
        print " directory", backup_dir, " Are the backups running?"
        sys.exit(1)
    bdirs.sort()
    current_backup_dir = bdirs[-1:][0]

    container = backup_dir+'/'+current_backup_dir+'/dbase.tar.gz'
    file_journal = os.path.join(backup_dir, current_backup_dir, 'file.tar.gz')
    print "container:", container
    con_stat=check_existance(container,1)
    mod_time=con_stat[stat.ST_MTIME]
    if not os.access(file_journal, os.F_OK):
        print timeofday.tod(),"ERROR: Too new - still being modified? (or corrupted?)",
        print "Backup container",container,'last modified on',
        print time.ctime(mod_time)
        sys.exit(1)
    else:
        print timeofday.tod(), "Checking container", container,
        print "last modified on", time.ctime(mod_time)

    return container
    
def extract_backup(check_dir, container):
    print timeofday.tod(), "Extracting database files from backup container",
    print container
    
    os.chdir(check_dir)
    os.system("/bin/tar -xzf %s"%(container,))
    os.system("db_recover -h %s -v"%(check_dir,))

    



def check_files(check_dir):
    db_files = os.listdir(check_dir)

    # check the main files, volume and file, and also any other index
    # files we generate
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
#            os.chdir(current_dir)
            sys.exit(istat)

def clean_up(current_dir, check_dir = None):
    if check_dir:
        check_these = os.listdir(check_dir)
        remove_files(check_these,check_dir)
        os.rmdir(check_dir)
    
    os.chdir(current_dir)





if __name__ == "__main__":
    if "--help" in sys.argv:
        print_usage()
        sys.exit(0)
    
    (backup_dir, check_dir, current_dir, backup_node) = configure(1) #non-None argument!
    extract_backup(check_dir, check_backup(backup_dir, backup_node))
    check_files(check_dir)
    clean_up(current_dir)
    
