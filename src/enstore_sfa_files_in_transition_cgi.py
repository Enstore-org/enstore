#!/usr/bin/env python

import os
import sys
import string
import time
import errno
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



KB=1<<10

def bfid2time(bfid):
    if bfid[-1] == "L":
        e = -6
    else:
        e = -5

    if bfid[0].isdigit():
        i = 0
    elif bfid[3].isdigit():
        i = 3
    else:
        i = 4

    t = int(bfid[i:e])
    if t > 1500000000:
        t = t - 619318800

    return t


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

def parse_mtab(volumes):
    volume_map={}
    for mtab_file in ["/etc/mtab", "/etc/mnttab"]:
        try:
            fp = file_utils.open(mtab_file, "r")
            try:
                for line in fp:
                    if not line :
                        continue
                    parts=line.split()
                    mps=parts[0].split(":")
                    if len(mps)<2:
                        continue
                    mp=mps[1]
                    for v in volumes:
                        if mp == v :
                            volume_map[v] = parts[1]
            finally:
               fp.close()
            break
        except (KeyboardInterrupt, SystemExit):
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except (OSError, IOError), msg:
            if msg.args[0] in [errno.ENOENT]:
                continue
            else:
                raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
    return volume_map


def main():
    # Obtain the correct values for ENSTORE_CONFIG_HOST and ENSTORE_CONFIG_PORT
    # if they are not already available.
    get_environment()

    print "Content-type: text/html"
    print
    print '<html>'
    print '<head>'
    print '<title> Enstore SFA HUD </title>'
    print '</head>'
    print '<body bgcolor="#ffffd0">'

    print "<div align=\"center\">"
    print '<hr>'
    print '<font size=7 color="#ff0000"> FILES IN TRANSITION</font>'
    print '<hr>'


    #Get the configuration server client.
    config_host = enstore_functions2.default_host()
    config_port = enstore_functions2.default_port()
    csc = configuration_client.ConfigurationClient((config_host, config_port))
    dc = dispatcher_client.DispatcherClient((config_host, config_port),
                                            rcv_timeout=2,
                                            rcv_tries=2)

    result = dc.show_policy()
    print "<TABLE border=\"1\" >"
    if result.has_key('status') and result['status'][0] == e_errors.OK:
        for k,v in result["dump"].iteritems():
            lm = k.split(".")[0]
            for i,p in v.iteritems():
                print "<TR>"
                policy = lm +"."+p["rule"]["storage_group"]+"."+\
                         p["rule"]["file_family"]+"."+\
                         p["rule"]["wrapper"]
                print "<TD><a href=\"#"+policy+"\">"+policy+"</a></TD>"
                for key in p:
                    print "<TD>"
                    if key == 'minimal_file_size' :
                        p[key] /= KB
                        print key,"=",p[key],"(kB)"
                    else:
                        print key,"=",p[key]
                    print "</TD>"
                print "</TR>"
    print "</TABLE>"
    print "</div>"


    files={}
    files_pool_policy = {}
    times = {}
    result = dc.show_queue()
    if result.has_key('status') and result['status'][0] == e_errors.OK:
        keys =result['pools'].keys()
        keys.sort()
        for pool in keys: # pools are keys of the dictionary
            if pool == 'migration_pool' and len(result['pools'][pool].keys()) != 0:
                for k in result['pools'][pool].keys():
                    policy=result['pools'][pool][k]['policy'].strip(".")
                    t=result['pools'][pool][k]['type']
                    time_qd = result['pools'][pool][k]['time_qd']
                    time_qd = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time_qd))
                    times[k] = time_qd
                    if t != "CACHE_WRITTEN" :
                        continue
                    if not files_pool_policy.has_key(policy):
                        files_pool_policy[policy] = {}
                    if not files_pool_policy[policy].has_key(pool):
                        files_pool_policy[policy][pool]=[]
                    if not files.has_key(policy):
                        #files[policy] = {"migration_pool" : [] , "cache_written" : [] }
                        files[policy] = { }
                    if not files[policy].has_key(pool) :
                        files[policy][pool] = {}
                    if not files[policy][pool].has_key(k):
                        files[policy][pool][k] = []
                    l = result['pools'][pool][k]['list']
                    for item in l:
                        files_pool_policy[policy][pool].append(item["bfid"])
                        files[policy][pool][k].append(item["bfid"])
            elif pool == 'cache_written' :
                for k,value in result['pools'][pool].iteritems():
                    policy=k.strip(".")
                    t=value.list_type
                    if t != "CACHE_WRITTEN" :
                        continue
                    if not files_pool_policy.has_key(policy):
                        files_pool_policy[policy] = {}
                    if not files_pool_policy[policy].has_key(pool):
                        files_pool_policy[policy][pool]=[]
                    if not files.has_key(policy):
                        #files[policy] = {"migration_pool" : [] , "cache_written" : [] }
                        files[policy] = { }
                    if not files[policy].has_key(pool) :
                        files[policy][pool] = {}
                    if not files[policy][pool].has_key(value.list_id):
                        files[policy][pool][value.list_id] = []
                    times[value.list_id] = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(value.creation_time))

                    for item in value.file_list:
                        files_pool_policy[policy][pool].append(item["bfid"])
                        files[policy][pool][value.list_id].append(item["bfid"])

    if not files :
        print "<h2>Nothing yet<h2>"
    else:
        print "<table>"
        print "<tr>"
        print '<td class="layout"><div><div class="layout_precious" style="width: 0.0%"></div><div class="layout_rest" style="width: 0.1%"></div><div class="layout_used" style="width: 99.7%"></div><div class="layout_free" style="width: 0.2%"></div></div></td>'
        print "</tr>"
        print "</table>"
        dbinfo = csc.get("database")
        for policy, poollists in  files.iteritems():
            print "<h2><policy=<a id=\"%s\">%s</a></h2>"%(policy,policy,)
            for pool, list in poollists.iteritems():
                files_in_pool_policies = files_pool_policy[policy][pool]
                Q1 = "select count(*), sum(size)/1024. as total from file where bfid in ('%s')"%(string.join(files_in_pool_policies,"','"))
                res = select(dbinfo,Q1)
                count = int(res[0]["count"])
                size  = long(res[0]["total"])

                print "<h2>pool=%s, total=%d, size=%d (kB), #of lists=%d</h2>"%(pool,count,size,len(list))
                for key, value in list.iteritems():
                    Q1 = "select count(*), sum(size)/1024. as total from file where bfid in ('%s')"%(string.join(value,"','"))
                    res = select(dbinfo,Q1)
                    count = int(res[0]["count"])
                    size  = long(res[0]["total"])
                    print "list id=%s, total=%d,size=%d (kB), time_qd=%s</h2>"%(key,count,size,times[key])
                    print "<pre>"
                    print "cache_mod_time storage_group file_family volume location_cookie bfid size crc pnfs_id pnfs_path archive_status"
                    nfiles = len(value)
                    start = 0
                    end = start
                    while (end < nfiles) :

                        if end+100 > nfiles:
                            end = nfiles
                        else:
                            end += 100

                        Q = "select f.cache_mod_time,v.storage_group,v.file_family,v.label as volume,f.location_cookie,f.bfid,f.size, \
                        f.crc,f.pnfs_id,f.pnfs_path,f.archive_status from file f, volume v where v.id=f.volume and f.bfid in ('%s')"%(string.join(value[start:end],"','"))
                        res=select(dbinfo,Q)
                        for row in res:
                            for k in ("cache_mod_time",
                                      "storage_group",
                                      "file_family",
                                      "volume",
                                      "location_cookie",
                                      "bfid",
                                      "size",
                                      "crc",
                                      "pnfs_id",
                                      "pnfs_path",
                                      "archive_status"):
                                print row[k],
                            print ""
                        start=end
                    print "</pre>"
    print '</body>'
    print '</html>'


if __name__ == "__main__":   # pragma: no cover

    main()
