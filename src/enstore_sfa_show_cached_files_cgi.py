#!/usr/bin/env python
###############################################################################
#
# $Id$
#
# this file generated listing of cached files in SFA
#
###############################################################################


import os
import sys
import string
import time 
import enstore_functions2
import enstore_functions3
import configuration_client
import info_client
import e_errors
import file_utils
import statvfs
import dispatcher_client 
from   DBUtils import PooledDB
import  psycopg2
import psycopg2.extras



# Obtain the correct values for ENSTORE_CONFIG_HOST and ENSTORE_CONFIG_PORT
# if they are not already available.
def get_environment():
    #This is a very messy way to determine the host and port of the
    # configuration server.  But at least it is in only one place now.
    if os.environ.get('ENSTORE_CONFIG_HOST', None) == None:
        cmd = ". /usr/local/etc/setups.sh; setup enstore; echo $ENSTORE_CONFIG_HOST; echo $ENSTORE_CONFIG_PORT"
        pfile = os.popen(cmd)
        config_host = pfile.readline().strip()
        if config_host:
            os.environ['ENSTORE_CONFIG_HOST'] = config_host
        else:
            print "Unable to determine ENSTORE_CONFIG_HOST."
            sys.exit(1)
        config_port = pfile.readline().strip()
        if config_port:
            os.environ['ENSTORE_CONFIG_PORT'] = config_port
        else:
            print "Unable to determine ENSTORE_CONFIG_PORT."
            sys.exit(1)
        pfile.close()
        
def select(value,query):
    connectionPool = None
    cursor = None
    db     = None
    try:
        connectionPool = PooledDB.PooledDB(psycopg2,
                                           maxconnections=1,
                                           blocking=True,
                                           host=value.get('dbhost','localhost'),
                                           port=value.get('dbport',5432),
                                           user=value.get('dbuser',"enstore"),
                                           database=value.get('dbname',"enstoredb"))
        db = connectionPool.connection()
        cursor = db.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute(query)
        res=cursor.fetchall()
        return res
    except:
        exc_type, exc_value = sys.exc_info()[:2]
        sys.stderr.write(time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))+" : " + str(exc_type)+' '+str(exc_value)+'\n')
        sys.stderr.flush()
        raise
    finally:
        for item in [cursor, db, connectionPool]:
            if item :
                item.close()
    
def main():
    # Obtain the correct values for ENSTORE_CONFIG_HOST and ENSTORE_CONFIG_PORT
    # if they are not already available.
    get_environment()

    print "Content-type: text/html"
    print
    print '<html>'
    print '<head>'
    print '<title> Cached files </title>'
    print '</head>'
    print '<body bgcolor="#ffffd0">'

    print "<div align=\"center\">"
    print '<hr>'
    print '<font size=7 color="#ff0000"> Cached files</font>'
    print '<hr>'
    print "</div>"


    #Get the configuration server client.
    config_host = enstore_functions2.default_host()
    config_port = enstore_functions2.default_port()
    csc = configuration_client.ConfigurationClient((config_host, config_port))
    dbinfo = csc.get("database")
    res=select(dbinfo,"select f.cache_mod_time,v.storage_group,v.file_family,v.label as volume,f.location_cookie,f.bfid,f.size, \
                    f.crc,f.pnfs_id,f.pnfs_path,f.archive_status from file f, volume v where v.id=f.volume and f.cache_status='CACHED' and f.location_cookie!=f.cache_location")

    if len(res)> 0 :
        print "<pre>"
        keys = res[0].keys()
        for k in keys:
            print k,
        print
        for r in res:
            for k in keys:
                print r[k],
            print 
        print "</pre>"
    else:
         print "<h2>Nothing yet<h2>"
    
    print '</body>'
    print '</html>'
   

if __name__ == "__main__":   # pragma: no cover

    main()
