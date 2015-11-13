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
# create table files_with_no_layers (ipnfsid varchar(36) PRIMARY KEY );
#
QUERY="select t_inodes.ipnfsid, t_inodes.isize, inode2path(t_inodes.ipnfsid) as path, t_inodes.imtime, l1.ipnfsid as layer1, encode(l2.ifiledata,'escape') as layer2, l4.ipnfsid as layer4 "+\
       "from t_inodes  left outer join t_level_4 l4 on (l4.ipnfsid=t_inodes.ipnfsid) "+\
       "left outer join t_level_1 l1 on (l1.ipnfsid=t_inodes.ipnfsid) "+\
       "left outer join  t_level_2 l2 on (l2.ipnfsid=t_inodes.ipnfsid) "+\
       "where t_inodes.itype=32768 and t_inodes.iio=0  and "+\
       "((t_inodes.imtime > CURRENT_TIMESTAMP - INTERVAL '49 hours' and "+\
       "t_inodes.imtime < CURRENT_TIMESTAMP - INTERVAL '24 hours') or t_inodes.ipnfsid in('%s')) order by t_inodes.imtime"

QUERY1="select t_inodes.ipnfsid, t_inodes.isize, inode2path(t_inodes.ipnfsid) as path, t_inodes.imtime, l1.ipnfsid as layer1, encode(l2.ifiledata,'escape') as layer2, l4.ipnfsid as layer4 "+\
       "from t_inodes  left outer join t_level_4 l4 on (l4.ipnfsid=t_inodes.ipnfsid) "+\
       "left outer join t_level_1 l1 on (l1.ipnfsid=t_inodes.ipnfsid) "+\
       "left outer join  t_level_2 l2 on (l2.ipnfsid=t_inodes.ipnfsid) "+\
       "where t_inodes.itype=32768 and t_inodes.iio=0  and "+\
       "(t_inodes.imtime > CURRENT_TIMESTAMP - INTERVAL '49 hours' and "+\
       "t_inodes.imtime < CURRENT_TIMESTAMP - INTERVAL '24 hours') order by t_inodes.imtime"

def print_yes_no(value):
    if not value:
        return 'n'
    else:
        return 'y'

#FORMAT = "%20s | %36s | %6s | %6s | %6s | %s \n"
HEADER_FORMAT="{0:^20} | {1:^36} | {2:^6} | {3:^6} | {4:^6} | {5:^48} | {6:^14} | {7:} \n"
FORMAT ="{0:<20} | {1:<36} | {2:^6} | {3:^6} | {4:^6} | {5:^48} | {6:>14} | {7:<} \n"

Trace.init("PNFS_MONITOR")



def print_header(f):
     #f.write(FORMAT%("date", "pnfsid", "layer1", "layer2", "layer4", "path"))
     f.write(HEADER_FORMAT.format("date", "pnfsid", "layer1", "layer2", "layer4", "pools","size [bytes]","path"))

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
    f.write("Files with missing layers in pnfs : %s \n"%(time.ctime()))
    f.write("Brought to you by pnfs_monitor\n\n")
    print_header(f)
    failed = False
    try:
        #
        # first query previously known files w/o layers
        #
        res = select("monitor","enstore","select ipnfsid from files_with_no_layers")
        pnfsids=[]
        if len(res) > 0:

            for row in res:
                pnfsids.append(row['ipnfsid'])
            #
            # check that the pnfsids stored in files_with_no_layer are still in t_inodes
            #
            res = select("chimera","enstore","select ipnfsid from t_inodes where ipnfsid in ('%s')"%string.join(pnfsids,"','"))
            chimera_pnfsids = []
            if len(res) > 0:
                for row in res:
                    chimera_pnfsids.append(row["ipnfsid"].strip())
                diff = set(pnfsids) - set(chimera_pnfsids)
                intersection = set(pnfsids) & set(chimera_pnfsids)
                if len(diff) > 0:
                    #
                    # delete from files_with_no_layers pnfsids that are no longer there
                    #
                    insert("monitor","enstore","delete from files_with_no_layers where ipnfsid in ('%s')"%(string.join(diff,"','")))
                if len(intersection) > 0:
                    res = select("chimera","enstore",QUERY%string.join(intersection,"','"))
                else:
                    res = select("chimera","enstore",QUERY1)
            else:
                #
                # none of the pnfsids from files_with_no_layers exist in t_inodes
                #
                insert("monitor","enstore","delete from files_with_no_layers where ipnfsid in ('%s')"%(string.join(pnfsids,"','")))
                res = select("chimera","enstore",QUERY1)
        else:
            res = select("chimera","enstore",QUERY1)

        for row in res:
            isVolatile=False
            pnfsid = row['ipnfsid']
            date   = row['imtime']
            layer2 = row['layer2']
            layer1 = row['layer1']
            layer4 = row['layer4']
            isize  = row['isize']
            path   = row['path']
            if layer1 and layer2 and layer4 :
                if pnfsid in pnfsids:
                    insert("monitor","enstore","delete from files_with_no_layers where ipnfsid='%s'"%(pnfsid,))
                continue
            if not layer2 and not layer4 and not layer1 :
                pres = select("chimera","enstore","select ilocation from t_locationinfo where ipnfsid='%s' and itype=1"%(pnfsid,))
                pools=""
                if len(pres) > 0:
                    for p in pres:
                        pools += p["ilocation"] + ","
                    if pools != "" :
                        pools = pools[:-1]
                    else:
                        pools = "N/A"
                f.write(FORMAT.format(date.strftime("%Y-%m-%d %H:%M:%S"),
                                      pnfsid, print_yes_no(layer1),
                                      print_yes_no(layer2),
                                      print_yes_no(layer4),
                                      pools,
                                      isize,
                                      path))
                if isize == 0 :
                    try:
                        os.unlink(path)
                    except Exception as e:
                        Trace.log(e_errors.ERROR, "Failed to remove file {}, {} ".format(path,str(e)))
                        pass
                else:
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
                pres = select("chimera","enstore","select ilocation from t_locationinfo where ipnfsid='%s' and itype=1"%(pnfsid,))
                pools=""
                if len(pres) > 0:
                    for p in pres:
                        pools += p["ilocation"] + ","
                    if pools != "" :
                        pools = pools[:-1]
                    else:
                        pools = "N/A"
                f.write(FORMAT.format(date.strftime("%Y-%m-%d %H:%M:%S"),
                                      pnfsid, print_yes_no(layer1),
                                      print_yes_no(layer2),
                                      print_yes_no(layer4),
                                      pools,
                                      isize,
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
        cmd="$ENSTORE_DIR/sbin/enrcp {} {}:{}".format(f.name,html_host,html_dir)
        rc = os.system(cmd)
        if rc :
            txt = "Failed to execute command %s\n.\n"%(cmd,)
            sys.stderr.write(time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))+" : "+txt+"\n")
            sys.stderr.flush()
            sys.exit(1)
        cmd="$ENSTORE_DIR/sbin/enrcp {} {}:{}/{}_{}".format(f.name,html_host,html_dir,os.path.basename(f.name),time.strftime("%Y-%m-%d",time.localtime(time.time())))
        rc = os.system(cmd)
        if rc :
            txt = "Failed to execute command %s\n.\n"%(cmd,)
            sys.stderr.write(time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))+" : "+txt+"\n")
            sys.stderr.flush()
            sys.exit(1)
    else:
        sys.exit(1)
    sys.exit(0)


