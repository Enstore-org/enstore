#!/usr/bin/env python

"""
 This script should run daily on the host running chimera DB.
 It checks all files created between 48 and 24 ours ago for layers.
 If files, that were supposed to go to tape, but still have layer1
 and layer4 missing, are found they are stored in database called
 "monitor" in a table files_with_no_layers and reported in a text
 file which is copied to enstore web server. Subsequent runs of
 this script check files from that table first.

"""

# system imports
import os
import string
import sys

# external dependencies
import DBUtils.PooledDB as PooledDB
import psycopg2
import psycopg2.extras

# enstore modules
import configuration_client
import enstore_constants
import Trace
import time
#
# create table files_with_no_layers (ipnfsid varchar(36) PRIMARY KEY );
#
QUERY = """
SELECT t_inodes.ipnfsid,
       t_inodes.inumber,
       t_inodes.isize,
       t_inodes.imtime,
       encode(l1.ifiledata,'escape') AS layer1,
       encode(l2.ifiledata,'escape') AS layer2,
       encode(l4.ifiledata,'escape') AS layer4,
       t_inodes.iretention_policy as retention_policy
FROM t_inodes
LEFT OUTER JOIN t_level_4 l4 ON (l4.inumber=t_inodes.inumber)
LEFT OUTER JOIN t_level_1 l1 ON (l1.inumber=t_inodes.inumber)
LEFT OUTER JOIN t_level_2 l2 ON (l2.inumber=t_inodes.inumber)
WHERE t_inodes.ipnfsid IN('{}')
  OR (t_inodes.itype=32768
  AND t_inodes.isize>0
  AND t_inodes.iaccess_latency = 0
  AND t_inodes.iretention_policy = 0
  AND (l4 is NULL OR l1 is NULL OR l2 is NULL)
  AND (t_inodes.imtime > CURRENT_TIMESTAMP - INTERVAL '49 hours'
        AND t_inodes.imtime < CURRENT_TIMESTAMP - INTERVAL '24 hours'))
ORDER BY t_inodes.imtime
"""

QUERY1 = """
SELECT t_inodes.ipnfsid,
       t_inodes.inumber,
       t_inodes.isize,
       t_inodes.imtime,
       encode(l1.ifiledata,'escape') AS layer1,
       encode(l2.ifiledata,'escape') AS layer2,
       encode(l4.ifiledata,'escape') AS layer4,
       t_inodes.iretention_policy as retention_policy
FROM t_inodes
LEFT OUTER JOIN t_level_4 l4 ON (l4.inumber=t_inodes.inumber)
LEFT OUTER JOIN t_level_1 l1 ON (l1.inumber=t_inodes.inumber)
LEFT OUTER JOIN t_level_2 l2 ON (l2.inumber=t_inodes.inumber)
WHERE t_inodes.itype=32768
  AND t_inodes.isize>0
  AND t_inodes.iaccess_latency = 0
  AND t_inodes.iretention_policy = 0
  AND (l4 is NULL OR l1 is NULL OR l2 is NULL)
  AND (t_inodes.imtime > CURRENT_TIMESTAMP - INTERVAL '49 hours'
                        AND t_inodes.imtime < CURRENT_TIMESTAMP - INTERVAL '24 hours')
ORDER BY t_inodes.imtime
"""


def print_yes_no(value):
    if not value:
        return 'n'
    else:
        return 'y'

HEADER_FORMAT = "{0:^20} | {1:^36} | {2:^6} | {3:^6} | {4:^6} | {5:^48} | {6:^14} | {7:} \n"
FORMAT = "{0:<20} | {1:<36} | {2:^6} | {3:^6} | {4:^6} | {5:^48} | {6:>14} | {7:<} \n"
Trace.init("PNFS_MONITOR")


def print_header(fh):
    fh.write(HEADER_FORMAT.format("date", "pnfsid", "layer1", "layer2", "layer4", "pools", "size [bytes]", "path"))


def select(db, query):
    cursor = None
    try:
        cursor = db.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except:
        exc_type, exc_value = sys.exc_info()[:2]
        sys.stderr.write(time.strftime("%Y-%m-%d %H:%M:%S",
                                       time.localtime(time.time()))+" : " + str(exc_type)+' '+str(exc_value)+'\n')
        sys.stderr.flush()
        raise
    finally:
        if cursor:
            cursor.close()


def insert(db, query):
    cursor = None
    try:
        cursor = db.cursor()
        cursor.execute(query)
        db.commit()
    except:
        exc_type, exc_value = sys.exc_info()[:2]
        sys.stderr.write(time.strftime("%Y-%m-%d %H:%M:%S",
                                       time.localtime(time.time()))+" : " + str(exc_type)+' '+str(exc_value)+'\n')
        sys.stderr.flush()
        try:
            if db:
                db.rollback()
        except:
            pass
        raise
    finally:
        if cursor:
            cursor.close()

if __name__ == "__main__":

    csc = configuration_client.get_config_dict()
    web_server_dict = csc.get("web_server")
    web_server_name = web_server_dict.get("ServerName", "localhost").split(".")[0]
    output_file = "/tmp/%s_pnfs_monitor" % (web_server_name,)
    html_dir = csc.get("crons").get("html_dir", None)
    inq_d = csc.get(enstore_constants.INQUISITOR, {})
    html_host = inq_d.get("host", "localhost")

    if not html_dir:
        sys.stderr.write(time.strftime("%Y-%m-%d %H:%M:%S",
                                       time.localtime(time.time()))+" : html_dir is not found \n")
        sys.stderr.flush()
        sys.exit(1)

    chimeraPool = PooledDB.PooledDB(psycopg2,
                                    maxconnections=1,
                                    blocking=True,
                                    host="localhost",
                                    user="enstore_reader",
                                    database="chimera")
    chimeradb = chimeraPool.connection()

    monitorPool = PooledDB.PooledDB(psycopg2,
                                    maxconnections=1,
                                    blocking=True,
                                    host="localhost",
                                    user="enstore",
                                    database="monitor")

    monitordb = monitorPool.connection()

    with open(output_file, "w") as f:
        f.write("Files with missing layers in pnfs : %s \n" % (time.ctime()))
        f.write("Brought to you by pnfs_monitor\n\n")
        print_header(f)
        failed = False
        try:
            #
            # first query previously known files w/o layers
            #
            res = select(monitordb,
                         "select ipnfsid from files_with_no_layers")
            pnfsids = []
            if len(res) > 0:
                pnfsids = [row['ipnfsid'] for row in res]
                #
                # check that the pnfsids stored in files_with_no_layer are still in t_inodes
                #
                res = select(chimeradb,
                             "select ipnfsid from t_inodes where ipnfsid in ('%s')" % string.join(pnfsids, "','"))
                chimera_pnfsids = []
                if len(res) > 0:
                    chimera_pnfsids = [row['ipnfsid'] for row in res]
                    diff = set(pnfsids) - set(chimera_pnfsids)
                    intersection = set(pnfsids) & set(chimera_pnfsids)
                    if len(diff) > 0:
                        #
                        # delete from files_with_no_layers pnfsids that are no longer there
                        #
                        insert(monitordb,
                               "delete from files_with_no_layers where ipnfsid in ('%s')" % (string.join(diff, "','")))
                    if len(intersection) > 0:
                        res = select(chimeradb,
                                     QUERY.format(string.join(intersection, "','")))
                    else:
                        res = select(chimeradb, QUERY1)
                else:
                    #
                    # none of the pnfsids from files_with_no_layers exist in t_inodes
                    #
                    insert(monitordb,
                           "delete from files_with_no_layers where ipnfsid in ('%s')" % (string.join(pnfsids, "','")))
                    res = select(chimeradb, QUERY1)
            else:
                res = select(chimeradb, QUERY1)

            for row in res:
                    isVolatile = False
                    pnfsid = row['ipnfsid']
                    inumber = row['inumber']
                    date = row['imtime']
                    layer2 = row['layer2']
                    layer1 = row['layer1']
                    layer4 = row['layer4']
                    isize = row['isize']
                    rp = row['retention_policy']
                    #
                    # empty layer is as "good" as non-existsnt
                    #
                    if layer1 and layer1.strip() == "":
                        layer1 = None
                    if layer4 and layer4.strip() == "":
                        layer4 = None
                    if (layer1 and layer4) or isize == 0:
                        if pnfsid in pnfsids:
                            insert(monitordb,
                                   "delete from files_with_no_layers where ipnfsid='%s'" % (pnfsid, ))
                        continue
                    if layer2:
                        for part in layer2.split('\n'):
                            if not part:
                                continue
                            pieces = part.split(';')
                            for p in pieces:
                                c = p.strip(':')
                                if c == "h=no":
                                    isVolatile = True
                    if isVolatile or (rp is None and rp != 0):
                        if pnfsid in pnfsids:
                            insert(monitordb,
                                   "delete from files_with_no_layers where ipnfsid='%s'" % (pnfsid, ))
                        continue
                    pres = select(chimeradb,
                                  "select ilocation from t_locationinfo where inumber = %s and itype=1" % (inumber, ))
                    pools = ""
                    if len(pres) > 0:
                        for p in pres:
                            pools += p["ilocation"] + ","
                        if pools != "":
                            pools = pools[:-1]
                        else:
                            pools = "N/A"
                    if pools.startswith("v"):
                        continue
                    rpath = select(chimeradb,
                                   "select inode2path('{}') AS path".format(pnfsid))
                    path = rpath[0]["path"]
                    if not path:
                        continue
                    f.write(FORMAT.format(date.strftime("%Y-%m-%d %H:%M:%S"),
                                          pnfsid, print_yes_no(layer1),
                                          print_yes_no(layer2),
                                          print_yes_no(layer4),
                                          pools,
                                          isize,
                                          path))
                    if pnfsid not in pnfsids:
                        insert(monitordb,
                               "insert into files_with_no_layers values ('%s')" % (pnfsid, ))
        except:
            exc_type, exc_value = sys.exc_info()[:2]
            sys.stderr.write(time.strftime("%Y-%m-%d %H:%M:%S",
                                           time.localtime(time.time()))+" : " + str(exc_type)+' '+str(exc_value)+'\n')
            sys.stderr.flush()
            failed = True
            pass
        finally:
            for item in [chimeradb, chimeraPool, monitordb, monitorPool]:
                if item:
                    item.close()

    if not failed:
        cmd = "$ENSTORE_DIR/sbin/enrcp {} {}:{}".format(f.name, html_host, html_dir)
        rc = os.system(cmd)
        if rc:
            txt = "Failed to execute command %s\n.\n"%(cmd,)
            sys.stderr.write(time.strftime("%Y-%m-%d %H:%M:%S",
                                           time.localtime(time.time()))+" : "+txt+"\n")
            sys.stderr.flush()
            sys.exit(1)
        cmd = "$ENSTORE_DIR/sbin/enrcp {} {}:{}/{}_{}".format(f.name,
                                                              html_host,
                                                              html_dir,
                                                              os.path.basename(f.name),
                                                              time.strftime("%Y-%m-%d",
                                                                            time.localtime(time.time())))
        rc = os.system(cmd)
        if rc:
            txt = "Failed to execute command %s\n.\n" % (cmd, )
            sys.stderr.write(time.strftime("%Y-%m-%d %H:%M:%S",
                                           time.localtime(time.time()))+" : "+txt+"\n")
            sys.stderr.flush()
            sys.exit(1)
    else:
        sys.exit(1)
    sys.exit(0)
