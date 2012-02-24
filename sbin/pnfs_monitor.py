#!/usr/bin/env python

# system imports
import os
import string
import sys
import traceback

# enstore modules
import option
import enstore_functions2
import configuration_client
import e_errors
import Trace
import urlparse
import namespace
from   DBUtils import PooledDB
import  psycopg2
import psycopg2.extras
import time


QUERY="select t_inodes.ipnfsid, t_inodes.imtime, l1.ipnfsid as layer1, encode(l2.ifiledata,'escape') as layer2, l4.ipnfsid as layer4 "+\
       "from t_inodes  left outer join t_level_4 l4 on (l4.ipnfsid=t_inodes.ipnfsid) "+\
       "left outer join t_level_1 l1 on (l1.ipnfsid=t_inodes.ipnfsid) "+\
       "left outer join  t_level_2 l2 on (l2.ipnfsid=t_inodes.ipnfsid) "+\
       "where t_inodes.itype=32768 and t_inodes.iio=0  and "+\
       "t_inodes.imtime > CURRENT_TIMESTAMP - INTERVAL '49 hours' and "+\
       "t_inodes.imtime < CURRENT_TIMESTAMP - INTERVAL '24 hours'"

def print_yes_no(value):
    if not value:
        return 'n'
    else:
        return 'y'

FORMAT = "%20s | %36s | %6s | %6s | %6s | %s \n"

def print_header(f):
     f.write(FORMAT%("date", "pnfsid", "layer1", "layer2", "layer4", "path"))

if __name__ == "__main__":
    
    csc   = configuration_client.get_config_dict()
    web_server_dict = csc.get("web_server")
    web_server_name = web_server_dict.get("ServerName","localhost").split(".")[0]
    output_file = "/tmp/%s_pnfs_monitor"%(web_server_name,)
    html_dir = csc.get("crons").get("html_dir",None)

    if not html_dir :
        sys.stderr.write(time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))+" : html_dir is not found \n")
        sys.stderr.flush()
        sys.exit(1)

    f = open(output_file,"w")
    f.write("Files with missing layers in pnfs : %s \n"%(time.ctime()))
    f.write("Brought to you by pnfs_monitor\n\n")
    

    connectionPool = None
    cursor = None
    db     = None
    value={}
    print_header(f)
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
        cursor.execute(QUERY)
        res=cursor.fetchall()
        isVolatile=False
        for row in res:
            pnfsid = row['ipnfsid']
            date   = row['imtime']
            layer2 = row['layer2']
            layer1 = row['layer1']
            layer4 = row['layer4']
            sfs = namespace.StorageFS(pnfsid)
            if not layer2 and not layer4 and not layer1 :
                f.write(FORMAT%(date.strftime("%Y-%m-%d %H:%M:%S"),
                                pnfsid, print_yes_no(layer1),
                                print_yes_no(layer2), 
                                print_yes_no(layer4),
                                sfs.filepath))
                continue
            if layer2 :
                for part in layer2.split('\n'):
                    if not part:
                        continue
                    pieces=part.split(';')
                    for p in pieces:
                        c=p.strip(':')
                        if c == "h=no":
                            isVolatile=True
            if isVolatile : continue
            if not layer1 or not layer4 :
                f.write(FORMAT%(date.strftime("%Y-%m-%d %H:%M:%S"),
                                pnfsid, print_yes_no(layer1),
                                print_yes_no(layer2), 
                                print_yes_no(layer4),
                                sfs.filepath))

            
    except:
        exc_type, exc_value = sys.exc_info()[:2]
        sys.stderr.write(time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))+" : " + str(exc_type)+' '+str(exc_value)+'\n')
        sys.stderr.flush()
    finally:
        for item in [cursor, db, connectionPool]:
            if item :
                item.close()
    f.close()
    cmd="$ENSTORE_DIR/sbin/enrcp %s %s:%s"%(f.name,web_server_name,html_dir)
    rc = os.system(cmd)
    if rc : 
        txt = "Failed to execute command %s\n.\n"%(cmd,)
        sys.stderr.write(time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))+" : "+txt+"\n")
        sys.stderr.flush()
        sys.exit(1)
    sys.exit(0)
