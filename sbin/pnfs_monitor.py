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
import enstore_constants
from   DBUtils import PooledDB
import  psycopg2
import psycopg2.extras
import time

# 
# create table files_with_no_layers (ipnfsid character(36) PRIMARY KEY );
#
QUERY="select t_inodes.ipnfsid, inode2path(t_inodes.ipnfsid) as path, t_inodes.imtime, l1.ipnfsid as layer1, encode(l2.ifiledata,'escape') as layer2, l4.ipnfsid as layer4 "+\
       "from t_inodes  left outer join t_level_4 l4 on (l4.ipnfsid=t_inodes.ipnfsid) "+\
       "left outer join t_level_1 l1 on (l1.ipnfsid=t_inodes.ipnfsid) "+\
       "left outer join  t_level_2 l2 on (l2.ipnfsid=t_inodes.ipnfsid) "+\
       "where t_inodes.itype=32768 and t_inodes.iio=0  and "+\
       "((t_inodes.imtime > CURRENT_TIMESTAMP - INTERVAL '49 hours' and "+\
       "t_inodes.imtime < CURRENT_TIMESTAMP - INTERVAL '24 hours') or t_inodes.ipnfsid in('%s'))"

def print_yes_no(value):
    if not value:
        return 'n'
    else:
        return 'y'

FORMAT = "%20s | %36s | %6s | %6s | %6s | %s \n"

def print_header(f):
     f.write(FORMAT%("date", "pnfsid", "layer1", "layer2", "layer4", "path"))

def select(dbname,dbuser,query):
    connectionPool = None
    cursor = None
    db     = None
    value={}
    try:
        connectionPool = PooledDB.PooledDB(psycopg2,
                                           maxconnections=1,
                                           blocking=True,
                                           host=value.get('dbhost','localhost'),
                                           port=value.get('dbport',5432),
                                           user=value.get('dbuser',dbuser),
                                           database=value.get('dbname',dbname))
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

def insert(dbname,dbuser,query):
    connectionPool = None
    cursor = None
    db     = None
    value={}
    try:
        connectionPool = PooledDB.PooledDB(psycopg2,
                                           maxconnections=1,
                                           blocking=True,
                                           host=value.get('dbhost','localhost'),
                                           port=value.get('dbport',5432),
                                           user=value.get('dbuser',dbuser),
                                           database=value.get('dbname',dbname))
        db = connectionPool.connection()
        cursor=db.cursor()
        cursor.execute(query)
        db.commit()
    except:
        exc_type, exc_value = sys.exc_info()[:2]
        sys.stderr.write(time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))+" : " + str(exc_type)+' '+str(exc_value)+'\n')
        sys.stderr.flush()
        try:
            if db : db.rollback()
        except:
            pass
        raise
    finally:
        for item in [cursor, db, connectionPool]:
            if item :
                item.close()

if __name__ == "__main__":

    csc   = configuration_client.get_config_dict()
    web_server_dict = csc.get("web_server")
    web_server_name = web_server_dict.get("ServerName","localhost").split(".")[0]
    output_file = "/tmp/%s_pnfs_monitor"%(web_server_name,)
    html_dir = csc.get("crons").get("html_dir",None)
    inq_d = csc.get(enstore_constants.INQUISITOR, {})
    html_host=inq_d.get("host","localhost")
    if not html_dir :
        sys.stderr.write(time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))+" : html_dir is not found \n")
        sys.stderr.flush()
        sys.exit(1)

    f = open(output_file,"w")
    failed = False
    try:
        f.write("Files with missing layers in pnfs : %s \n"%(time.ctime()))
        f.write("Brought to you by pnfs_monitor\n\n")
        print_header(f)
        res = select("monitor","enstore","select ipnfsid from files_with_no_layers")
        pnfsids=[]
        if len(res) > 0:
            for row in res:
                pnfsids.append(row['ipnfsid'])

        res = select("chimera","enstore",QUERY%string.join(pnfsids,"','"))
        isVolatile=False
        for row in res:
            pnfsid = row['ipnfsid']
            date   = row['imtime']
            layer2 = row['layer2']
            layer1 = row['layer1']
            layer4 = row['layer4']
            path   = row['path']
            if layer1 and layer2 and layer4 :
                if pnfsid in pnfsids:
                    insert("monitor","enstore","delete from files_with_no_layers where ipnfsid='%s'"%(pnfsid,))
                continue
            if not layer2 and not layer4 and not layer1 :
                f.write(FORMAT%(date.strftime("%Y-%m-%d %H:%M:%S"),
                                pnfsid, print_yes_no(layer1),
                                print_yes_no(layer2),
                                print_yes_no(layer4),
                                path))
                if pnfsid not in pnfsids:
                    insert("monitor","enstore","insert into files_with_no_layers values ('%s')"%(pnfsid,))
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
                                path))
                if pnfsid not in pnfsids:
                    insert("monitor","enstore","insert into files_with_no_layers values ('%s')"%(pnfsid,))
    except:
        exc_type, exc_value = sys.exc_info()[:2]
        sys.stderr.write(time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))+" : " + str(exc_type)+' '+str(exc_value)+'\n')
        sys.stderr.flush()
        failed=True
        pass
        
    f.close()
    if not failed:
        cmd="$ENSTORE_DIR/sbin/enrcp %s %s:%s"%(f.name,html_host,html_dir)
        rc = os.system(cmd)
        rc=0
        if rc :
            txt = "Failed to execute command %s\n.\n"%(cmd,)
            sys.stderr.write(time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))+" : "+txt+"\n")
            sys.stderr.flush()
            sys.exit(1)
    else:
        sys.exit(1)
    sys.exit(0)


