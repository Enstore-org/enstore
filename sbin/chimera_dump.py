#!/usr/bin/env python

"""
Script that generates dump of chimera database as
a flatfile provided name of top level directory(ies).
No argument means all directories
"""


# system imports
from optparse import OptionParser
import os
import string
import sys
import traceback

import configuration_client
import dbaccess
import enstore_functions2

QUERY_TOP_DIRECTORIES = """
WITH RECURSIVE paths(ino, pnfsid, path, type, depth)
AS (VALUES(pnfsid2inumber('000000000000000000000000000000000000'),'','',16384,0)
UNION SELECT i.inumber, i.ipnfsid, path||'/'||d.iname,i.itype, p.depth+1
   FROM t_dirs d,t_inodes i, paths p
   WHERE p.type=16384 AND d.iparent=p.ino AND p.depth<4 AND
         d.iname != '.' AND d.iname != '..' AND i.inumber=d.ichild)
         SELECT p.path, p.pnfsid  FROM paths p
         WHERE p.type=16384 and p.depth>3 and p.path ~ '/pnfs/fs/usr'
"""

QUERY_TOP_DIRECTORY = """
WITH RECURSIVE paths(ino, pnfsid, path, type, depth)
AS (VALUES(pnfsid2inumber('000000000000000000000000000000000000'),'','',16384,0)
UNION SELECT i.inumber, i.ipnfsid, path||'/'||d.iname,i.itype, p.depth+1
   FROM t_dirs d,t_inodes i, paths p
   WHERE p.type=16384 AND d.iparent=p.ino AND p.depth<4 AND
         d.iname != '.' AND d.iname != '..' AND i.inumber=d.ichild)
         SELECT p.path, p.pnfsid FROM paths p
         WHERE p.type=16384 and p.depth>3 and p.path = '/pnfs/fs/usr/{}'
"""

DUMP_DIRECTORY = """
WITH RECURSIVE paths(ino, pnfsid, path, type, size, uid, gid, ctime, atime, mtime)
AS (VALUES(pnfsid2inumber('{}'),'','',16384,0::BIGINT,0,0,now(),now(),now())
   UNION SELECT i.inumber, i.ipnfsid, path||'/'||d.iname,
      i.itype,i.isize,i.iuid,i.igid,i.ictime,i.iatime,i.imtime
   FROM t_dirs d,t_inodes i, paths p
   WHERE p.type=16384 AND d.iparent=p.ino AND
         d.iname != '.' AND d.iname != '..' AND i.inumber=d.ichild)
	  SELECT p.pnfsid, encode(l1.ifiledata,'escape') as bfid, '{}'||p.path as path,
          p.size,p.uid,p.gid,
          extract (epoch from p.ctime)::BIGINT,
          extract (epoch from p.atime)::BIGINT,
          extract (epoch from p.mtime)::BIGINT
	   FROM paths p, t_level_1 l1
	    WHERE p.type=32768 AND
	           l1.inumber=p.ino;
"""


def help():
    HELP = """
    usage %prog [SG ...]

    Generates chimera dump for a list of storage groups.
    If none is provided, all storage groups are dumped.

    """
    return HELP


def dump_directory(path, pnfsid, dbhost, dbname, dbuser, dbport):
    q = DUMP_DIRECTORY.format(pnfsid, path)
    outfile = os.path.join(
        "/tmp",
        "CHIMERA_DUMP_%s" %
        (os.path.basename(path)))
    cmd = "psql -U  {}   -A -F ' ' -c \"{}\" -o {} -h {} -p {} {}".format(
        dbuser, q, outfile, dbhost, dbport, dbname)
    os.system(cmd)


if __name__ == "__main__":
    parser = OptionParser(usage=help())
    (options, args) = parser.parse_args()
    csc = configuration_client.ConfigurationClient((enstore_functions2.default_host(),
                                                    enstore_functions2.default_port()))
    inventory = csc.get('inventory')
    dest_path = inventory.get('inventory_rcp_dir')
    namespaceDictionary = csc.get('namespace')

    if not namespaceDictionary:
        sys.stderr.write("No namespace dictionary in configuration root.\n")
        sys.exit(1)

    """
    Kludge to skip cms namespace which we do not want to query
    """
    for key in ("cms", "status",):
        try:
            del namespaceDictionary[key]
        except BaseException:
            pass

    for key, value in namespaceDictionary.iteritems():
        dbcon = value
        if "replica" in value:
            dbcon = value.get("replica")
        if not dbcon and "master" in value:
            dbcon = value.get("master")
        db = None
        try:
            db = dbaccess.DatabaseAccess(maxconnections=1,
                                         host=dbcon.get("dbhost", "localhost"),
                                         database=dbcon.get(
                                             "dbname", "chimera"),
                                         user=dbcon.get(
                                             "dbuser", "enstore_reader"),
                                         port=dbcon.get("dbport", 5432))
            storage_groups = []
            if len(args) > 0:
                for arg in set(args):
                    q = QUERY_TOP_DIRECTORY.format(arg)
                    res = db.query(q)
                    for row in res:
                        path = row[0]
                        storage_groups.append(os.path.basename(path))
                        dump_directory(row[0],
                                       row[1],
                                       dbcon.get("dbhost", "localhost"),
                                       dbcon.get("dbname", "chimera"),
                                       dbcon.get("dbuser", "enstore_reader"),
                                       dbcon.get("dbport", 5432))
            else:
                q = QUERY_TOP_DIRECTORIES
                res = db.query(q)

                for row in res:
                    path = row[0]
                    storage_groups.append(os.path.basename(path))
                    dump_directory(row[0],
                                   row[1],
                                   dbcon.get("dbhost", "localhost"),
                                   dbcon.get("dbname", "chimera"),
                                   dbcon.get("dbuser", "enstore_reader"),
                                   dbcon.get("dbport", 5432))
            for sg in storage_groups:
                cmd = "enrcp %s %s" % (os.path.join(
                    "/tmp", "CHIMERA_DUMP_%s" % (sg,)), dest_path)
                os.system(cmd)
        except Exception as e:
            sys.stderr.write("Failed to query database {} \n".format(str(e)))
            sys.exit(1)
        finally:
            if db:
                db.close()
