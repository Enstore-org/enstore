#!/usr/bin/env python
###############################################################################
# $Author$
# $Date$
# $Id$
#
#  Utility that tests database backup: creates database from backup and
#  runs file listing on it
#
###############################################################################

import os
import shutil
import signal
import socket
import stat
import string
import sys
import time
import traceback

import configuration_client
import e_errors
import file_utils
import hostaddr
import timeofday

import psycopg2
import psycopg2.extras
from DBUtils.PooledDB import PooledDB

DURATION = 72 #hours
PREFIX = 'RECENT_FILES_ON_TAPE'

EXCLUDED_STORAGE_GROUP = ['backups', 'CLEAN', 'null', 'test', 'none']

def print_usage():
    print "Usage:", sys.argv[0], "[--help]"
    print "  --help   print this help message"
    print "See configuration dictionary entry \"backup\" for defaults."

# check the status of a return ticket
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

# check if a directory or a file exists
def check_existance(the_file, plain_file):
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

# get the configuration
def configure(configuration = None):
    csc = configuration_client.ConfigurationClient()
    backup = csc.get('backup',timeout=15,retry=3)
    check_ticket('Configuration Server',backup)
    inventory = csc.get('inventory',timeout=15,retry=3)
    check_ticket('Configuration Server',inventory)

    #Check both the FQDN and the ip.  On multihomed hosts only checking on
    # my lead to failed operations that would otherwise succed.
    backup_node = backup.get('hostip', 'MISSING')
    thisnode = hostaddr.gethostinfo(1)
    mynode = thisnode[2][0]
    backup_fqdn = socket.gethostbyaddr(backup_node)[0]
    mynode_fqdn = socket.gethostbyaddr(mynode)[0]
    if mynode != backup_node and backup_fqdn != mynode_fqdn:
        print timeofday.tod(),"ERROR Backups are stored", backup_node,
        print ' you are on',mynode,' - database check is not possible'
        sys.exit(1)

    backup_dir = backup.get('dir')
    if not backup_dir:
        print timeofday.tod(), "ERROR Backup directory is not determined"
        sys.exit(1)
    if configuration:
        check_existance(backup_dir,0)

    check_dir = backup.get('extract_dir')
    if not check_dir:
        print timeofday.tod(), "ERROR Extraction directory is not determined"
        sys.exit(1)

    dest_path = inventory.get('inventory_rcp_dir')
    if not dest_path:
        print timeofday.tod(), "ERROR rcp directory is not determined"

    db_path = backup.get('check_db_area')
    if not db_path:
        # to be backward compatible
        db_path = '/diskc/check-database'

    current_dir = os.getcwd() #Remember the original directory

    print timeofday.tod(), 'Checking Enstore on', csc.get_address()[0], \
          'with timeout of', csc.get_timeout(), 'and retries of', \
          csc.get_retry()
    """
    Return the directory the backup file is in and the directory the backup
    file will be ungzipped and untared to, respectively.  Lastly, return
    the current directory.
    """
    return backup_dir, check_dir, current_dir, backup_node, dest_path, db_path

# get the most recent backup file
def check_backup(backup_dir, backup_node):
    # backups are saved in separate files - get the most recent one
    bdirs = os.listdir(backup_dir)
    if len(bdirs) == 0:
        print timeofday.tod(),"ERROR NO Backups found on ", backup_node,
        print " directory", backup_dir, " Are the backups running?"
        sys.exit(1)
    bdirs.sort()
    current_backup_dir = bdirs[-1:][0]

    container = os.path.join(backup_dir, current_backup_dir, 'enstoredb.dmp')

    print "container:", container
    con_stat=check_existance(container,1)
    mod_time=con_stat[stat.ST_MTIME]
    for i in range(100):
        if time.time() - os.stat(container)[stat.ST_MTIME] < 60:
            print timeofday.tod(),"Too new - wait for a 15 seconds"
            time.sleep(15)
        else:
            break
    if time.time() - os.stat(container)[stat.ST_MTIME] < 60:
        print timeofday.tod(), "ERROR: Backup is too new. Give up after retries."
        print "Backup container",container,'last modified on', time.ctime(os.stat(container)[stat.ST_MTIME])
        sys.exit(1)
    else:
        print timeofday.tod(), "Checking container", container,
        print "last modified on", time.ctime(os.stat(container)[stat.ST_MTIME])

    return container

# start postmaster
def start_postmaster(db_path):
    cmd = "ps axw| grep postmaster | grep %s | grep -v grep | awk '{print $1}'"%(db_path)
    pid = os.popen(cmd).readline()
    pid = string.strip(pid)
    if pid:
        print timeofday.tod(), "postmaster has already been started -- pid =", pid
        sys.exit(1)
    else:
        print timeofday.tod(), "Starting postmaster ..."
        # take care of left over pid info, if any
        pid_file = os.path.join(db_path, "postmaster.pid")
        if os.access(pid_file, os.F_OK):
            os.unlink(pid_file)

        # starting postmaster
        cmd = "postmaster -D %s &"%(db_path)
        os.system(cmd)
        time.sleep(15)

#stop postmaster
def stop_postmaster(db_path):
    pid = os.popen("ps axw| grep postmaster | grep %s | grep -v grep | awk '{print $1}'"%(db_path)).readline()
    pid = string.strip(pid)
    if pid:
        print timeofday.tod(), "Stopping postmaster"
        os.kill(int(pid), signal.SIGKILL)
    else:
        print timeofday.tod(), "postmaster is not running"

def extract_backup(check_dir, container):
    print timeofday.tod(), "Extracting database files from backup container",
    print container

    os.chdir(check_dir)
    os.system("dropdb backup")
    os.system("createdb backup")
    # os.system("pg_restore -d backup -v "+container)
    #os.system("pg_restore -d backup -v -s -x -O "+container)
    os.system("psql backup -c 'create sequence volume_seq;'")
    os.system("psql backup -c 'create sequence state_type_seq;'")
    os.system("pg_restore -d backup -v -s -t bad_file "+container)
    os.system("pg_restore -d backup -v -s -t cache_statuses "+container)
    os.system("pg_restore -d backup -v -s -t archive_statuses "+container)
    os.system("pg_restore -d backup -v -s -t files_in_transition "+container)
    os.system("pg_restore -d backup -v -s -t cached_files "+container)
    os.system("pg_restore -d backup -v -s -t active_file_copying "+container)
    os.system("pg_restore -d backup -v -s -t file "+container)
    os.system("pg_restore -d backup -v -s -t media_capacity "+container)
    os.system("pg_restore -d backup -v -s -t migration "+container)
    os.system("pg_restore -d backup -v -s -t migration_history "+container)
    os.system("pg_restore -d backup -v -s -t no_flipping_storage_group "+container)
    os.system("pg_restore -d backup -v -s -t option "+container)
    os.system("pg_restore -d backup -v -s -t qa "+container)
    os.system("pg_restore -d backup -v -s -t quota "+container)
    os.system("pg_restore -d backup -v -s -t sg_count "+container)
    os.system("pg_restore -d backup -v -s -t state "+container)
    os.system("pg_restore -d backup -v -s -t state_type "+container)
    os.system("pg_restore -d backup -v -s -t tc "+container)
    os.system("pg_restore -d backup -v -s -t volume "+container)
    os.system("pg_restore -d backup -v -s -t no_flipping_file_family "+container)
    os.system("pg_restore -d backup -v -s -t file_copies_map "+container)
    os.system("pg_restore -d backup -v -a "+container)
    os.system("psql backup -c 'alter table only volume add constraint volume_pkey primary key (id);'")
    os.system("psql backup -c 'create index volume_storage_group_idx on volume(storage_group);'")
    os.system("psql backup -c 'create index volume_system_inhibit_0_idx on volume(system_inhibit_0);'")
    os.system("psql backup -c 'alter table only file add constraint file_pkey primary key (bfid);'")
    os.system("psql backup -c 'CREATE INDEX package_id_fk_idx ON file USING btree (package_id);'")
    os.system("psql backup -c 'CREATE INDEX file_volume_idx ON file USING btree (volume);'")

LISTING_FILE = "COMPLETE_FILE_LISTING"

# the way to check it is to run file listing on all
def check_db(check_dir):

    query = """
        SELECT coalesce(to_char(f.archive_mod_time, 'YYYY-MM-DD HH24:MI:SS'),to_char(f.update,'YYYY-MM-DD HH24:MI:SS')),
              v.storage_group,
              v.file_family,
               CASE
                   WHEN f.package_id <> f.bfid
                        AND f.package_id IS NOT NULL THEN
                          (SELECT label
                           FROM file,
                                volume
                           WHERE volume.id=file.volume
                             AND file.bfid=f.package_id)
                   ELSE v.label
               END AS volume,
               CASE
                   WHEN f.package_id <> f.bfid
                        AND f.package_id IS NOT NULL THEN
                          (SELECT location_cookie
                           FROM file
                           WHERE bfid=f.package_id)
                   ELSE f.location_cookie
               END AS location_cookie,
               f.bfid,
               f.size,
               f.crc,
               f.pnfs_id,
               f.pnfs_path,
               coalesce(f.archive_status,'None') as archive_status,
               CASE
                   WHEN f.package_id <> f.bfid
                        AND f.package_id IS NOT NULL THEN
                          (SELECT pnfs_id
                           FROM file
                           WHERE bfid=f.package_id)
                   ELSE 'None'
               END AS package_pnfsid
        FROM file f,
             volume v
        WHERE f.volume=v.id
          AND v.system_inhibit_0 != 'DELETED'
          AND f.deleted = 'n'
        ORDER BY v.file_family,
                 volume,
                 location_cookie
    """

    db = PooledDB(psycopg2,
                  maxconnections=1,
                  maxcached=1,
                  blocking=True,
                  host="localhost",
                  port=5432,
                  user="enstore",
                  database="backup")

    connection = db.connection()
    cursor = connection.cursor('cursor_for_complete_file_listing')
    cursor.execute(query)
    colnames="storage_group file_family volume location_cookie bfid size crc pnfs_id pnfs_path archive_status package_pnfsid"

    output_files = {}
    recent_output_files = {}
    pnfs_dict_file = open(os.path.join(check_dir, 'PNFS.XREF'),"w")
    pnfs_dict_file.write("pnfs_id bfid size pnfs_path\n")

    complete_file_listing_header = """
-- Listed at {}
--
-- STORAGE GROUP: {}
--
    """
    recent_files_listing_header = """
Date this listing was generated: {}
Brought to You by: {}

"""


    complete_file_listing_file = os.path.join(check_dir, LISTING_FILE)
    complete_file_listing_fd  = open(complete_file_listing_file, 'w')
    time_stamp = time.ctime(time.time())
    complete_file_listing_fd.write("Listed at %s\n\n"%(time_stamp))
    complete_file_listing_fd.write("%s\n"%(colnames,))


    start_time = time.time()
    while True:
        res = cursor.fetchmany(100000)
        if len(res) == 0:
            break
        for row in res:
            sg = row[1]
            if sg in EXCLUDED_STORAGE_GROUP:
                continue
            complete_file_listing_fd.write("%s\n"%(string.join([str(i) for i in row[1:]]," ")))
            if sg not in output_files:
                out_file = os.path.join(check_dir, LISTING_FILE + "_" + sg)
                output_files[sg]=open(out_file,"w")
                output_files[sg].write(complete_file_listing_header.format(time_stamp,sg))
                output_files[sg].write("%s\n"%(colnames,))
            f = output_files[sg]
            f.write("%s\n"%(string.join([str(i) for i in row[1:]]," ")))

            if sg not in recent_output_files:
                out_file = os.path.join(check_dir, PREFIX + "_" + sg)
                recent_output_files[sg]=open(out_file,"w")
                recent_output_files[sg].write(recent_files_listing_header.format(time_stamp,os.path.basename(sys.argv[0])))
                head = "Recent (packed and package) written to tape in the last {} hours for storage group {}".format(DURATION,sg)
                recent_output_files[sg].write("\n{}\n\n".format(head))
                recent_output_files[sg].write("update_time storage_group file_family volume location_cookie bfid size crc pnfs_id pnfs_path archive_status package_pnfsid\n")

            t = row[0]
            try:
                update_time = time.mktime(time.strptime(t,"%Y-%m-%d %H:%M:%S"))
                if update_time  > start_time - DURATION * 3600 :
                    f = recent_output_files[sg]
                    f.write("%s\n"%(string.join([str(i) for i in row]," ")))
            except Exception, msg:
                #print msg, row
                continue
            pnfs_dict_file.write("{} {} {} {}\n".format(row[8],row[5],row[6],row[9]))

    cursor.close()
    complete_file_listing_fd.close()
    map(lambda x : x.close(), output_files.values())
    map(lambda x : x.close(), recent_output_files.values())
    pnfs_dict_file.close()

    cursor.close()
    connection.close()

if __name__ == "__main__":   # pragma: no cover
    if "--help" in sys.argv:
        print_usage()
        sys.exit(0)

    (backup_dir, check_dir, current_dir, backup_node, dest_path, db_path) = configure(1) #non-None argument!
    #stop postmaster if it was running already
    stop_postmaster(db_path)
    # checking for the directories
    if not os.access(check_dir, os.F_OK):
        os.makedirs(check_dir)
    check_existance(check_dir, 0)
    if os.access(db_path, os.F_OK):
        print db_path, "exists, removing it..."
        file_utils.rmdir(db_path)
    os.makedirs(db_path)
    # create database area
    cmd = "initdb -D %s"%(db_path)
    if (os.system(cmd)):
        print "Failed to create database area ",db_path
        sys.exit(1)
    #starting postmaster
    start_postmaster(db_path)
    extract_backup(check_dir, check_backup(backup_dir, backup_node))
    check_db(check_dir)
    stop_postmaster(db_path)
    # moving COMPLETE_FILE_LISTING to dest_path
    for cmd in ("enrcp %s %s"%(os.path.join(check_dir, "COMPLETE_FILE_LISTING*"), dest_path),
                "enrcp %s %s"%(os.path.join(check_dir, "RECENT_FILES_ON_TAPE*"), dest_path),
                "enrcp %s %s"%(os.path.join(check_dir, "PNFS.XREF"), dest_path)):
        print timeofday.tod(), cmd
        os.system(cmd)
    if os.access(db_path, os.F_OK):
        #
        # clean up after ourselves so next
        #
        file_utils.rmdir(db_path)



