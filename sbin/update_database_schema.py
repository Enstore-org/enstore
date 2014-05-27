#!/usr/bin/env python
##############################################################################
#
# This script creates DDL that can be run to update enstore database schema
#
##############################################################################

import os
import sys
import time

import configuration_client
import enstore_constants
from optparse import OptionParser

LOCATION_OF_XML_FILES="databases/schemas/xml"
LOCATION_OF_DDL_FILES="databases/schemas/ddl"

def get_command_output(command):
    print_message("Executing command %s"%(cmd,))
    child = os.popen(command)
    data  = child.read()
    err   = child.close()
    if err:
	raise RuntimeError, '%s failed w/ exit code %d' % (command, err)
    return data

def print_message(text):
    sys.stdout.write(time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))+" : " +text+"\n")
    sys.stdout.flush()

def print_error(text):
    sys.stderr.write(time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))+" : " +text+"\n")
    sys.stderr.flush()

def help():
    txt = "usage %prog [options] database_name ["
    for db in name_to_servermap.keys():
        txt=txt+db+", "
    txt=txt[:-2]
    txt=txt+"]"
    txt=txt+"\n"
    txt=txt+"This script attempts to dowload database schema, compare it with schema \n"
    txt=txt+"stored in CVS and produce a difference DDL file that needs to be applied to \n"
    txt=txt+"database."


    return txt

name_to_servermap = {
    "accounting" : enstore_constants.ACCOUNTING_SERVER,
    "drivestat"  : enstore_constants.DRIVESTAT_SERVER,
    "operation"  : "operation_db",
    "enstoredb"  : "database",
    }

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

if __name__ == "__main__":
    ENSTORE_DIR=os.getenv("ENSTORE_DIR")
    if not ENSTORE_DIR :
        os.stderr.write("ENSTORE_DIR is not defined, bailing out");
        sys.exit(1)

    parser = OptionParser(usage=help())
    parser.add_option("-p", "--port",
                      metavar="PORT",type=int,
                      help="database port if it should be different from setup file")
    parser.add_option("-H", "--host",
                      metavar="HOST",type=str,
                      help="database host if it should be different from setup file")
    (options, args) = parser.parse_args()
    if len(args) != 1:
        parser.print_help()
        sys.exit(1)
    dbname=args[0]
    filename=dbname+".xml"
    schema_file=os.path.join(ENSTORE_DIR,
                             LOCATION_OF_XML_FILES,
                             filename)
    if not os.access(schema_file, os.F_OK):
        print_error("schema file (%s) is not found "%(schema_file,))
        sys.exit(1)
    ddl_directory_path=os.path.join(ENSTORE_DIR,
                                    LOCATION_OF_DDL_FILES,
                                    dbname)
    if not os.access(ddl_directory_path, os.F_OK):
        print_error("ddl directory (%s)is not found "%(ddl_directory_path,))
        sys.exit(1)
    server_name=get_server_name(dbname)
    if not server_name : sys.exit(1)
    csc=configuration_client.get_config_dict()
    server=csc.get(server_name,None)
    if not server : sys.exit(1)
    dbport = server.get("dbport",5432)
    dbname = server.get("dbname",dbname)
    dbuser = server.get("dbuser","enstore")
    dbhost = server.get("dbhost","localhost")
    if options.host:
        dbhost=options.host
    if options.port:
        dbport=options.port
    # get the schema
    diff_file="%s_diff.sql"%(dbname,)
    diff_file_tmp="%s_diff_tmp.sql"%(dbname,)
    f=open("schema.xml","w")
    cmd="downloadXml --dbms=postgres --host=%s --dbname=%s --port=%d --user=%s"%(dbhost,dbname,dbport,dbuser,)
    f.write(get_command_output(cmd))
    f.close()
    #
    # produce diff file
    #
    cmd="diffxml2ddl --dbms=postgres %s schema.xml > %s "%(schema_file,diff_file_tmp,)
    print cmd
    if (os.system(cmd)):
        print_error("failed to execute %s"%(cmd))
        sys.exit(1)
    #
    # move ALTER at the end
    #

    create_no_index = "%s_diff_1.sql"%(dbname,)
    create_index    = "%s_diff_2.sql"%(dbname,)
    alter_file      = "%s_diff_3.sql"%(dbname,)

    cmd="cat %s | grep -v ALTER | grep -v \"CREATE INDEX\" > %s"%(diff_file_tmp,create_no_index,)
    os.system(cmd)

    cmd="cat %s | grep -v ALTER | grep \"CREATE INDEX\" > %s"%(diff_file_tmp,create_index,)
    os.system(cmd)

    cmd="cat %s | grep ALTER > %s"%(diff_file_tmp,alter_file,)
    os.system(cmd)

    cmd="cat %s %s %s > %s"%(create_no_index, alter_file, create_index, diff_file,)
    if (os.system(cmd)):
        print_error("failed to execute %s"%(cmd))
        sys.exit(1)
    update_file="%s_update.sql"%(dbname,)
    # take care of sequences, types, triggers and functions
    cmd="cat %s/%s_header.sql  %s/%s_types.sql %s/%s_sequences.sql %s %s/%s_functions.sql %s/%s_triggers.sql > %s "%(ddl_directory_path,
                                                                                                                     dbname,
                                                                                                                     ddl_directory_path,
                                                                                                                     dbname,
                                                                                                                     ddl_directory_path,
                                                                                                                     dbname,
                                                                                                                     diff_file,
                                                                                                                     ddl_directory_path,
                                                                                                                     dbname,
                                                                                                                     ddl_directory_path,
                                                                                                                     dbname,
                                                                                                                     update_file,)
    os.system(cmd)
    os.unlink(alter_file)
    os.unlink(create_no_index)
    os.unlink(create_index)
    os.unlink(diff_file_tmp)
    os.unlink("schema.xml")
    os.unlink(diff_file)
    print_message("successfully created diff DDL file: %s"%(update_file,))
    print_message("examine the content of this file")
    print_message("after that apply to database like so:")
    print_message("psql -h %s -p %d -U %s %s -f %s"%(dbhost,dbport,dbuser,dbname,update_file,))
