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

QUERY_TOP_DIRECTORIES="""
WITH RECURSIVE paths(ino, pnfsid, path, type, depth)
AS (VALUES(pnfsid2inumber('000000000000000000000000000000000000'),'','',16384,0)
UNION SELECT i.inumber, i.ipnfsid, path||'/'||d.iname,i.itype, p.depth+1
   FROM t_dirs d,t_inodes i, paths p
   WHERE p.type=16384 AND d.iparent=p.ino AND p.depth<4 AND
         d.iname != '.' AND d.iname != '..' AND i.inumber=d.ichild)
         SELECT p.path, p.pnfsid  FROM paths p
         WHERE p.type=16384 and p.depth>3 and p.path ~ '/pnfs/fs/usr'
"""

QUERY_TOP_DIRECTORY="""
WITH RECURSIVE paths(ino, pnfsid, path, type, depth)
AS (VALUES(pnfsid2inumber('000000000000000000000000000000000000'),'','',16384,0)
UNION SELECT i.inumber, i.ipnfsid, path||'/'||d.iname,i.itype, p.depth+1
   FROM t_dirs d,t_inodes i, paths p
   WHERE p.type=16384 AND d.iparent=p.ino AND p.depth<4 AND
         d.iname != '.' AND d.iname != '..' AND i.inumber=d.ichild)
         SELECT p.path, p.pnfsid FROM paths p
         WHERE p.type=16384 and p.depth>3 and p.path = '/pnfs/fs/usr/{}'
"""

DUMP_DIRECTORY="""
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
    HELP="""
    usage %prog [SG ...]

    Generates chimera dump for a list of storage groups.
    If none is provided, all storage groups are dumped.

    """
    return HELP

def dump_directory(path,pnfsid):
    q=DUMP_DIRECTORY.format(pnfsid,path)
    outfile=os.path.join("/tmp","CHIMERA_DUMP_%s"%(os.path.basename(path)))
    cmd = "psql -U enstore_reader  -A -F ' ' -c \"%s\" -o %s -h stkensrv1n chimera"%(q,outfile,)
    os.system(cmd)

if __name__ == "__main__":
    parser = OptionParser(usage=help())
    (options, args) = parser.parse_args()

    db  = dbaccess.DatabaseAccess(maxconnections=1,
                                  host     = "stkensrv1n",
                                  database = "chimera",
                                  user     = "enstore_reader")
    storage_groups = []
    if len(args) > 0:
        for arg in set(args):
            q=QUERY_TOP_DIRECTORY.format(arg)
            res=db.query(q)
            for row in res:
                path=row[0]
                storage_groups.append(os.path.basename(path))
                dump_directory(row[0],row[1])

    else:
        q=QUERY_TOP_DIRECTORIES
        res=db.query(q)
        for row in res:
            path=row[0]
            storage_groups.append(os.path.basename(path))
            dump_directory(row[0],row[1])

    db.close()

    csc = configuration_client.ConfigurationClient((enstore_functions2.default_host(),
                                                    enstore_functions2.default_port()))
    inventory = csc.get('inventory')
    dest_path = inventory.get('inventory_rcp_dir')

    for sg in storage_groups:
        cmd = "enrcp %s %s"%(os.path.join("/tmp","CHIMERA_DUMP_%s"%(sg,)),dest_path)
        os.system(cmd)

