#!/usr/bin/env python
###############################################################################
#
# $Author$
# $Date$
# $Id$
#
# script that dumps and restores any enstore db
# based on setup parameters from backup or from local file
#
# Author: Dmitry Litvintsev (litvinse@fnal.gov) 01/09
#
###############################################################################

import pg
import sys
import os
import time
from optparse import OptionParser

import configuration_client
import enstore_constants

psql="psql"
pg_restore="pg_restore"
pg_ctl="pg_ctl"
pg_dump="pg_dump"
dropdb="dropdb"
createdb="createdb"
accounting_backup_subdirectory="ACC-DST"
restore_tmp="DBRESTORE"

name_to_servermap = {
    "accounting" : enstore_constants.ACCOUNTING_SERVER,
    "drivestat"  : enstore_constants.DRIVESTAT_SERVER,
    "operation"  : "operation_db",
    "enstore-db" : "database",
    }

def usage(cmd):
    print "Usage: %prog [options] argv1 "


def print_message(text):
    sys.stdout.write(text+"\n")
    sys.stdout.flush()

def get_command_output(command):
    child = os.popen(command)
    data  = child.read()
    err   = child.close()
    if err:
	raise RuntimeError, '%s failed w/ exit code %d' % (command, err)
    return data[:-1] # (skip '\n' at the end)

def print_error(text):
    sys.stderr.write(text+"\n")
    sys.stderr.flush()


def drop_database(dbname,user,port):
    cmd = dropdb + " -p %d -U %s %s"%(port,user,dbname)
    if os.system(cmd) == 0 :
        print_message("Successfully dropped database %s"%(dbname))
    else:
        print_error("Failed to drop databse %s"%(dbname))
        return 1
    return 0

def create_database(dbname,user,port):
    cmd = createdb + " -p %d -U %s %s"%(port,user,dbname)
    if os.system(cmd) == 0 :
        print_message("Successfully created database %s"%(dbname))
    else:
        print_error("Failed to create databse %s"%(dbname))
        return 1
    return 0

def dump_database(dbhost,dbname,dbuser,dbport,file):
    cmd = pg_dump + " -p %d -h %s -F c -U %s -f %s %s"%(dbport,
                                                        dbhost,
                                                        dbuser,
                                                        file,
                                                        dbname)
    if os.system(cmd) == 0 :
        print_message("Successfully executed command %s"%(cmd))
    else:
        print_error("Failed to execute command  %s"%(cmd))
        return 1
    return 0 

def get_server_name(name):
    server=name_to_servermap.get(name,None)
    if not server:
        txt="Unknown database name is specified: %s\n"%(name)
        txt=txt+"Known databases : [" 
        for db in name_to_servermap.keys():
            txt=txt+db+", "
        txt=txt[:-2]
        txt=txt+"]"
        print_error(txt)
    return server

def get_backup(backup_host, backup_dir,  backup_name=None):
    cmd=""
    if not backup_name:
        cmd='rsh %s "ls -dt %s/dbase.* | head -1"'%(backup_host, backup_dir)
    else:
        cmd='rsh %s  "ls -t %s/%s.*|head -1 "'%(backup_host, backup_dir,  backup_name)
    return get_command_output(cmd)

def restore_database(dbname,dbuser,dbport,backup_file):
    cmd=pg_restore+" -p %d -U %s  -d %s %s"%(dbport,dbuser,dbname,backup_file)
    return os.system(cmd)

if __name__ == "__main__" :
    txt = "usage %prog [options] database_name ["
    for db in name_to_servermap.keys():
        txt=txt+db+", "
    txt=txt[:-2]
    txt=txt+"]"
    usage=txt
    parser = OptionParser(usage=usage)
    parser.add_option("-b", "--backup",
                      action="store_true", dest="backup", default=False,
                      help="extract database from backup. Location of backup is defined in configuration")
    parser.add_option("-r", "--restore",
                      action="store_true", dest="restore", default=False,
                      help="perform pg_restore on specified datbase using local file or backup")
    parser.add_option("-d", "--dump",
                      action="store_true", dest="dump", default=False,
                      help="run pg_dump on specified database")
    parser.add_option("-p", "--port",
                      metavar="PORT",type=int,
                      help="database port if it should be different from setup file")
    parser.add_option("-f", "--filename",type=str,
                      metavar="FILE", help="output or sourvce datbase backup"),
    (options, args) = parser.parse_args()

    if len(args) != 1:
        parser.print_help()
        sys.exit(1)

    dbname=args[0]
    filename=dbname+".dump"
    if options.filename:
        filename=options.filename
    current_dir = os.getcwd() 
    #
    # Need to extract data defining database
    #
    server_name=get_server_name(dbname)
    if not server_name : sys.exit(1)
    csc=configuration_client.get_config_dict()
    server=csc.get(server_name,None)
    if not server : sys.exit(1)
    #extract all what we need for the database connection
    dbport = server.get("dbport",5432)
    dbname = server.get("dbname",dbname)
    dbuser = server.get("dbuser","enstore")
    dbuser_reader = server.get("dbuser_reader","enstore_reader")
    dbarea=server.get("dbarea",None)
    dbserverowner = server.get("dbserverowner","products")
    dbhost=server.get("dbhost")
    if options.port:
        dbport=options.port
    if not dbarea :
        print_error("Failed to extract dbarea for database %s"%(dbname))
    if options.dump==True:
        if dump_database(dbhost,dbname,dbuser,dbport,filename)!=0: sys.exit(1)
    if options.restore==True:
        backup_file=filename
        if options.backup==True:
            # restore from backup
            backup=csc.get("backup",None)
            if not backup:
                print_error("Requested to restore from backup, but cannot find backup dictionary in configuration")
                sys.exit(1)
            backup_host=backup.get("host",None)
            if not backup_host:
                print_error("Requested to restore from backup, but cannot determine backup host for database %s"%(dbname))
                sys.exit(1)
            backup_dir=backup.get("dir",None)
            if not backup_dir :
                print_error("Requested to restore from backup, but cannot find backup directory")
                sys.exit(1)
            if dbname != "enstore-db" :
                backup_dir=os.path.join(backup_dir,accounting_backup_subdirectory)
                backup_file=get_backup(backup_host, backup_dir,dbname)
            else:
                backup_file=get_backup(backup_host, backup_dir)
            # copy backup_file here:
            if not os.path.exists(restore_tmp):
                os.makedirs(restore_tmp)
            os.chdir(restore_tmp)
            cmd = "/usr/bin/rsync -e rsh  %s:%s . "%(backup_host, backup_file)
            print 'copying: %s'% (cmd,)
            err=os.system(cmd)
            if err:
                print_error("%s failed w/ exit code %d" % (cmd, err))
                sys.exit(1)
            backup_file=os.path.basename(backup_file)
        print "Got backup file "+backup_file
        # drop database
        drop_database(dbname,dbuser,dbport)
        if create_database(dbname,dbuser,dbport)!=0:
            print_error("Failed to create database %s"%(dbname))
            sys.exit(1)
        print_message("Restoring database %s"%(dbname))
        if restore_database(dbname,dbuser,dbport,backup_file)!=0:
            print_error("Failed to restore database %s"%(dbname))
            sys.exit(1)
        os.unlink(backup_file)
        os.chdir(current_dir)
        os.rmdir(restore_tmp)
    sys.exit(0)
    

    os.system("dropdb -p 8800 -U enstore accounting")
    if os.system("createdb -p 8800 -U enstore accounting") != 0 : sys.exit(1)
    os.system("pg_restore -p 8800 -U enstore -d accounting -i --schema-only -v accounting.dump")
    db = pg.DB(host  = "localhost",
               dbname= "accounting",
               port  = 8800,
               user  = "enstore")
    
    tables = db.get_tables()
    db.close()

    for t in tables:
        if t[:3] == "sql" :
            continue
        start = time.time()
        txt="Starting to restore table %s %f "%(t,start,)
        sys.stdout.write(txt)
        sys.stdout.flush()
        os.system("pg_restore -U enstore -p 8800 -d accounting -i --data-only --table=%s -v accounting.dump >> acc_dump.log 2>&1"%(t,))
        end   = time.time()
        txt="Took %f seconds to restore table %s\n"%(end-start, t)
        sys.stdout.write(txt)
        sys.stdout.flush()
    sys.exit(0)
#time pg_dump -F c -U enstore -f accounting.dump accounting
