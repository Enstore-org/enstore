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
import grp
import pwd
import string

from optparse import OptionParser

import configuration_client
import enstore_constants
import dump_restore_database

USERID = 1342
GROUPID = 4525
ENSTORE_USERID = 5744
ENSTORE_GROUPID = 6209
creategroup = "/usr/sbin/groupadd"
createuser = "/usr/sbin/useradd"
dbserver_cmd = "postmaster"
pid_file = dbserver_cmd + ".pid"

name_to_schema_map = {
    "accounting": "databases/schemas/accounting.schema",
    "drivestat": "databases/schemas/drivestat.schema",
    "enstoredb": "databases/schemas/enstoredb.schema",
}


def copy_file(src, dst):
    try:
        f1 = open(src, "r")
        f2 = open(dst, "w")
        f2.writelines(f1.readlines())
        f1.close()
        f2.close()
    except (OSError, IOError):
        return 1
    return 0


def find_user(username):
    for data in pwd.getpwall():
        if data[0] == username:
            return True
    return False


def find_user_id(user_id):
    for data in pwd.getpwall():
        if data[2] == user_id:
            return True
    return False


def find_group(group):
    for data in grp.getgrall():
        if data[0] == group:
            return True
    return False


def find_group_id(group_id):
    for data in grp.getgrall():
        if data[2] == group_id:
            return True
    return False


def get_user_info(username):
    for data in pwd.getpwall():
        if data[0] == username:
            return data
    return None


def create_user_and_group(groupname,
                          group_id,
                          username,
                          user_id):
    cmd = ""
    if not find_group(groupname):
        if not find_group_id(group_id):
            cmd = creategroup + " -g %d  %s" % (group_id, groupname)
        else:
            cmd = creategroup + " %s" % (group_id, groupname)
        if os.system(cmd) != 0:
            dump_restore_database.print_error(
                "Failed to execute command %s" % (cmd))
            return 1
    if not find_user(username):
        if not find_user_id(user_id):
            cmd = createuser + \
                " -u %d -g %s %s " % (user_id, groupname, username)
        else:
            cmd = createuser + " -g %s %s " % (groupname, username)
        if os.system(cmd) != 0:
            dump_restore_database.print_error(
                "Failed to execute command %s" % (cmd))
            return 1
    return 0


def move_directory(src, dst):
    try:
        dst = dst + \
            ".%s" % (time.strftime("%Y-%m-%d.%H:%M:%S", time.localtime()))
        os.rename(src, dst)
    except OSError as msg:
        dump_restore_database.print_error(
            "Failed to move %s to %s" %
            (src, dst))
        dump_restore_database.print_error(msg)
        return 1
    return 0


def find_pid(cmd, arg):
    res = os.popen('ps axw').readlines()
    for i in res:
        if i.find(cmd) >= 0 and i.find(str(arg)) >= 0:
            return(int(i.split()[0]))
    return 0


def modify_pg_hba_file(filename, dbname, dbuser, dbuser_reader):
    cmd = "/sbin/ifconfig | grep \"inet addr:\" | grep -v 127.0.0.1 | awk \'{print $2}\' | cut -d\":\" -f 2"
    fp = open(filename, "a")
    for ip in os.popen(cmd).readlines():
        fp.write("host %s %s %s/24 trust \n" % (dbname, dbuser, ip.strip()))
        fp.write(
            "host %s %s %s/24 trust \n" %
            (dbname, dbuser_reader, ip.strip()))
    fp.close()


def start_database(dbarea, dbport):
    return os.system("postmaster -D %s -p %d -i &" % (dbarea, dbport))


def check_database(dbport, dbserverowner):
    cmd = 'psql -p %d template1 -U %s -c "select now();"' % (dbport, dbserverowner)
    for i in range(5):
        time.sleep(5)
        if os.system(cmd) == 0:
            dump_restore_database.print_message("database is ready")
            return 0
        else:
            dump_restore_database.print_error(
                "database is not ready, trying %d" % (i))
    return 1


def create_database_user(dbport, dbuser):
    return os.system(
        "psql template1 -p %d -c \"create user %s with superuser password \'enstore_user\' createdb;\"" % (dbport, dbuser))


def create_database_read_user(dbport, dbuser):
    return os.system(
        "psql template1 -p %d -c \"create user %s password \'enstore_user\';\"" % (dbport, dbuser))


def create_database(dbport, dbname):
    return os.system("createdb -p %d %s" % (dbport, dbname))


def init_database(dbarea):
    return os.system("initdb -D %s" % (dbarea))


def create_schema(dbport, dbname, schema_file):
    return os.system("psql -p %d %s -f %s" % (dbport, dbname, schema_file))


if __name__ == "__main__":
    if pwd.getpwuid(os.getuid())[0] != "root":
        dump_restore_database.print_error("Must be 'root' to run this program")
        sys.exit(1)
    enstore_dir = os.getenv("ENSTORE_DIR")
    if not enstore_dir:
        dump_restore_database.print_error(
            "ENSTORE_DIR variable is not defined, please setup enstore first")
        sys.exit(1)
    ourhost = string.split(os.uname()[1], '.')[0]
    parser = OptionParser(usage=dump_restore_database.help())
    (options, args) = parser.parse_args()
    if len(args) != 1:
        parser.print_help()
        sys.exit(1)
    dbname = args[0]
    #
    # Need to extract data defining database
    #
    server_name = dump_restore_database.get_server_name(dbname)
    if not server_name:
        sys.exit(1)
    csc = configuration_client.get_config_dict()
    server = csc.get(server_name, None)
    if not server:
        sys.exit(1)
    dbport = server.get("dbport", 5432)
    dbname = server.get("dbname", dbname)
    dbuser = server.get("dbuser", "enstore")
    dbuser_reader = server.get("dbuser_reader", "enstore_reader")
    dbarea = server.get("dbarea", None)
    dbserverowner = server.get("dbserverowner", "products")
    dbhost = server.get("dbhost", None)
    if create_user_and_group(dbserverowner, GROUPID,
                             dbserverowner, USERID) != 0:
        sys.exit(1)
    user_data = get_user_info(dbserverowner)
    if not user_data:
        dump_restore_database.print_error(
            "Failed to find user data for user %s" %
            (dbserverowner))
        sys.exit(1)
    uid = user_data[2]
    gid = user_data[3]
    if create_user_and_group(dbuser, ENSTORE_GROUPID,
                             dbuser, ENSTORE_USERID) != 0:
        sys.exit(1)
    enstore_user_data = get_user_info(dbuser)
    if not enstore_user_data:
        dump_restore_database.print_error(
            "Failed to find user data for user %s" %
            (dbuser))
        sys.exit(1)
    enstore_uid = enstore_user_data[2]
    enstore_gid = enstore_user_data[3]
    if not dbarea:
        dump_restore_database.print_error(
            "Failed to extract database are for database %s" %
            (dbname))
        sys.exit(1)
    schema_file = name_to_schema_map.get(dbname, None)
    if not schema_file:
        dump_restore_database.print_error(
            "Failed to find schema file for database %s (%s)" %
            (dbname, schema_file))
        sys.exit(1)
    schema_file = os.path.join(enstore_dir, schema_file)
    if not os.path.exists(schema_file):
        dump_restore_database.print_error(
            "Schema file %s does not exist" %
            (schema_file))
        sys.exit(1)
    pid = find_pid(dbserver_cmd, dbarea)
    if pid:
        dump_restore_database.print_error(
            "ERROR: database server is still running.")
        dump_restore_database.print_error("%d" % (pid))
        dump_restore_database.print_error(
            "ERROR: database %s creation failed!" %
            (dbname))
        dump_restore_database.print_error(
            "ERROR: stop above database server first!")
        sys.exit(1)
    if os.path.exists(dbarea):
        dump_restore_database.print_message(
            "Database area %s already exists" % (dbarea))
        dump_restore_database.print_message("Moving it on the side")
        if move_directory(dbarea, dbarea) != 0:
            sys.exit(1)
    try:
        dump_restore_database.print_message(
            "Creating database area %s" % (dbarea))
        os.makedirs(dbarea, 0o777)
    except BaseException:
        dump_restore_database.print_error(
            "Failed to create database area %s" %
            (dbarea))
        sys.exit(1)
    try:
        dump_restore_database.print_message(
            "chown to uid=%d gid=%d" %
            (uid, gid))
        os.chown(dbarea, uid, gid)
    except BaseException:
        dump_restore_database.print_error(
            "Failed to chown to uid=%d gid=%d" %
            (uid, gid))
        sys.exit(1)
    if dbname == "enstoredb":
        for d in ["db_dir", "jou_dir"]:
            value = server.get(d, None)
            if not value:
                dump_restore_database.print_error(
                    "%s directory is not defined " % (d))
                sys.exit(1)
            else:
                if not os.path.exists(value):
                    try:
                        os.makedirs(value, 0o777)
                    except BaseException:
                        dump_restore_database.print_error(
                            "Failed to create %s directory %s" %
                            (d, value))
                        sys.exit(1)
                else:
                    dump_restore_database.print_message(
                        "Directory %s %s already exists" %
                        (d, value))
                try:
                    os.chown(value, enstore_uid, enstore_gid)
                except BaseException:
                    dump_restore_database.print_error(
                        "Failed to chown %s directory %s to uid=%d, gid=%d" %
                        (d, value, enstore_uid, enstore_gid))
                    sys.exit(1)
    pg_hba = os.path.join(enstore_dir, "databases/control_files/pg_hba.conf")
    #
    # take care of Fermi specific setups
    #
    if ourhost.startswith("cdfen"):
        pg_hba = os.path.join(
            enstore_dir,
            "databases/control_files/pg_hba.conf-stken-%s" %
            (dbname))
    elif ourhost.startswith("d0en"):
        pg_hba = os.path.join(
            enstore_dir,
            "databases/control_files/pg_hba.conf-d0en-%s" %
            (dbname))
    elif ourhost.startswith("cdfen"):
        pg_hba = os.path.join(
            enstore_dir,
            "databases/control_files/pg_hba.conf-cdfen-%s" %
            (dbname))
    else:
        pg_hba = os.path.join(enstore_dir,
                              "databases/control_files/pg_hba.conf")
    if not os.path.exists(pg_hba):
        dump_restore_database.print_error(
            "pg_hba %s does not exist" % (pg_hba))
        sys.exit(1)
    #
    # become user we want to be
    #
    os.setgid(gid)
    os.setuid(uid)
    #
    # run initdb
    #
    if init_database(dbarea) != 0:
        dump_restore_database.print_error("Failed to initdb %s" % (dbarea))
        sys.exit(1)
    #
    # copy pg_hba in place
    #
    dst = os.path.join(dbarea, os.path.basename(pg_hba))
    if copy_file(pg_hba, dst) != 0:
        sys.exit(1)
    #
    # modify pg_hba in place
    #
    pg_hba = dst
    modify_pg_hba_file(pg_hba, dbname, dbuser, dbuser_reader)
    start_database(dbarea, dbport)
    if check_database(dbport, dbserverowner) != 0:
        sys.exit(1)
    if create_database_user(dbport, dbuser) != 0:
        dump_restore_database.print_error(
            "Failed to create database user %s" %
            (dbuser))
        sys.exit(1)
    if create_database_read_user(dbport, dbuser_reader) != 0:
        dump_restore_database.print_error(
            "Failed to create database read user %s" %
            (dbuser_reader))
        sys.exit(1)
    if create_database(dbport, dbname) != 0:
        dump_restore_database.print_error(
            "Failed to create database %s" % (dbname))
        sys.exit(1)
    if create_schema(dbport, dbname, schema_file) != 0:
        dump_restore_database.print_error(
            "Failed to create database schema %s" %
            (dbname))
        sys.exit(1)
    sys.exit(0)
