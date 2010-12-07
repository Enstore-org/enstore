#!/usr/bin/env python

###############################################################################
#
# $Id$
#
# Equivalent of delfile for chimera
# relies on configuration entry that looks like this:
#
#configdict['namespace']    = {
#    'cms' : {'dbname' : 'chimera',
#             'dbhost' : 'dmsdca03',
#             'dbport' : 5432,
#             'dbuser' : 'enstore' },
#    }
#
#  The key in dictionary ('cms') is just a name
#
# how it works:
# loops over all chimera databases and extract bfid and pnfsid from
# t_locationinfo_trash table. Loop over each entry and mark file as
# deleted in file table, then remove entry from  t_locationinfo_trash
# if error occurs tries to rollback deletion and change delete flag
# in file table
#
###############################################################################


# system imports
import os
import string
import sys
import traceback

# enstore modules
import option
import file_clerk_client
import volume_clerk_client
import e_errors
import Trace
import urlparse

from   DBUtils import PooledDB
import  psycopg2
import psycopg2.extras

def main(intf):
    success = True
    vols = []

    if os.geteuid() != 0:
        sys.stderr.write("Must be user root.\n")
        return False

    fcc = file_clerk_client.FileClient((intf.config_host, intf.config_port))
    vcc = volume_clerk_client.VolumeClerkClient(fcc.csc)

    namespaceDictionary = fcc.csc.get('namespace',None)

    if not namespaceDictionary:
        sys.stderr.write("No namespace dictionary in configuration root.\n")
        return False

    if not e_errors.is_ok(namespaceDictionary):
        sys.stderr.write("Got errror retrieving namespace dictionary from config %s\n"%(str(namespaceDictionary['status'])))
        return False

    del namespaceDictionary['status']

    for key, value in namespaceDictionary.iteritems():
        connectionPool = None
        cursor = None
        db     = None
        try:
            connectionPool = PooledDB.PooledDB(psycopg2,
                                              maxconnections=1,
                                              blocking=True,
                                              host=value.get('dbhost','localhost'),
                                              port=value.get('dbport',5432),
                                              user=value.get('dbuser','enstore'),
                                              database=value.get('dbname','chimera'))
            db = connectionPool.connection()
            cursor = db.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cursor.execute("select * from t_locationinfo_trash where itype=0")
            res=cursor.fetchall()

            for row in res:
                pnfsid=row.get('ipnfsid')
                #
                # ilocation field is URI like and looks:
                #   enstore://enstore/?volume=NUL002&location_cookie=0000_000000000_0021154&size=1816&file_family=testers&original_name=/pnfs/fnal.gov/data/testers/NULL/enstore/fstab.16&map_file=&pnfsid_file=0000D05E3A18BB7F4378BB460A066B66B850&pnfsid_map=&bfid=B0MS129054868400000&origdrive=dmsdca03:/dev/null:0&crc=0
                #
                url_dict = urlparse.parse_qs(row.get('ilocation'))
                bfid     = url_dict.get('bfid')[0]
                volume   = url_dict.get('enstore://enstore/?volume')[0]
                fcc.bfid = bfid
                print 'deleting', bfid, '...',
                result = fcc.set_deleted('yes')
                if result['status'][0] != e_errors.OK:
                    print bfid, result['status'][1]
                    success = False
                    continue
                else:
                    if not volume in vols:
                        vols.append(volume)
                    #
                    # delete record from trash
                    #
                    delete_cursor=None
                    try:
                        delete_cursor = db.cursor()
                        delete_cursor.execute("delete from t_locationinfo_trash where ipnfsid='%s'"%(pnfsid,))
                        db.commit()
                    except psycopg2.Error, msg:
                        try:
                            if db:
                                db.rollback()
                                result = fcc.set_deleted('no')
                                if result['status'][0] != e_errors.OK:
                                    sys.stderr.write("Succeeded to rollback delete of pnfsid %s from t_localinfo_trash, database %s, but failed to unmark deleted in enstoredb database\n")
                                    sys.stderr.write("%s %s\n"%(bfid,result['status'][1],))
                                    success = False
                                    continue
                        except psycopg2.Error, msg:
                            sys.stderr.write("Failed to rollback delete of pnfsid %s from t_localinfo_trash, database %s, %s \n"%(pnfsid,
                                                                                                                                  str(value),
                                                                                                                                  str(msg)))
                            pass
                    finally:
                        if delete_cursor:
                            delete_cursor.close()

        finally:
            for item in [cursor, db, connectionPool]:
                if item :
                    item.close()

    for i in vols:
        print 'touching', i, '...',
        result = vcc.touch(i)
        if result['status'][0] == e_errors.OK:
            print 'done'
        else:
            print 'failed'
            success = False

    if not success:
        return 1

    return 0

def do_work(intf):

    Trace.init("DELFILE")

    try:
        exit_status = main(intf)
    except (SystemExit, KeyboardInterrupt), msg:
        Trace.log(e_errors.ERROR, "delfile aborted from: %s" % str(msg))
        sys.exit(1)
    except:
        #Get the uncaught exception.
        exc, msg, tb = sys.exc_info()
        #Print it to terminal.
        traceback.print_exception( exc, msg, tb )
        #Also, send it to the log file.
        Trace.handle_error(exc, msg, tb)

        del tb #No cyclic references.
        sys.exit(1)

    sys.exit(exit_status)

class DelfileInterface(option.Interface):
    def valid_dictionaries(self):
        return (self.help_options,)

if __name__ == '__main__':
    intf_of_delfile = DelfileInterface()

    do_work(intf_of_delfile)