#!/usr/bin/env python

###############################################################################
#
# $Id$
#
# Purpose of this script is to recover deleted volume from enstoredb using
# information from backup
#
###############################################################################

import pg
import sys
import optparse

import configuration_client

def help():
    return "Usage: %prog [options] label(s) "


if __name__ == "__main__" :
    parser = optparse.OptionParser(usage=help())

    parser.add_option("-p", "--port",
                      metavar="PORT",type=int, default=8888,
                      help="database port of backup database")

    parser.add_option("-u", "--user",
                      metavar="USER",type=str,default="enstore_reader",
                      help="database user of backup database")

    parser.add_option("-d", "--dbname",
                      metavar="DBNAME",type=str,default="enstoredb",
                      help="database name of backup database")

    parser.add_option("-H", "--host",default="localhost",
                      metavar="HOST",type=str,
                      help="database host of backup database")

    (options, args) = parser.parse_args()

    if len(args) < 1:
        parser.print_help()
        sys.exit(1)


    csc=configuration_client.get_config_dict()
    db_config=csc.get('database',None)

    if not db_config:
        sys.stderr.write("Failed to extract enstoredb parameters from configuration\n")
        sys.stderr.flush()
        sys.exit(1)

    source = pg.DB(host   = options.host,
                   dbname = options.dbname,
                   port   = options.port,
                   user   = options.user);

    destination = pg.DB(host   = db_config.get("dbhost","localhost"),
                        dbname = db_config.get("dbname","enstoredb"),
                        port   = db_config.get("dbport",8888),
                        user   = db_config.get("dbuser"))               # no default user name

    for label in args:
        vol_dictionaries = source.query("select * from volume where label='%s'"%(label,)).dictresult()
        for volume in vol_dictionaries:
            volume['deleted_files']=0
            volume['deleted_bytes']=0
            destination.insert("volume",volume)
            q="select * from state where  volume=%d"%(volume.get('id',))
            states=source.query(q).dictresult()
            for state in states:
                destination.insert("state",state)
            q="select * from file where volume=%d"%(volume.get('id',))
            file_dictionaries = source.query(q).dictresult()
            for file in file_dictionaries:
                destination.insert("file",file)
                q="select * from migration where dst_bfid='%s'"%(file.get('bfid'),)
                migration_dictionaries=source.query(q).dictresult()
                for migration in migration_dictionaries:
                    destination.insert("migration",migration)

    source.close()
    destination.close()
    sys.exit(0)



