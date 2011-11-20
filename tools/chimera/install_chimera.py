#!/bin/env python
###############################################################################
# $Author$
# $Date$
# $Id$
#
#  Script that installs chimera
#  This must be run on pnfs host. pnfs must be running when this script
#  is executed.
#  contains hardcoded database user
#
###############################################################################

import os
import sys
import time

PNFS_DUMP = "/opt/pnfs/tools/pnfsDump -e -r %s  -vv -d0 -o chimera chimera_%s.sql -2 -p %s -o verify verify_%s.md5 -r -o files files_%s.lst -f > pnfsdump_%s.log 2>&1"

def print_message(text):
    sys.stdout.write(time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))+" : " +text+"\n")
    sys.stdout.flush()

def print_error(text):
    sys.stderr.write(time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))+" : " +text+"\n")
    sys.stderr.flush()


def get_command_output(command):
    child = os.popen(command)
    data  = child.read()
    err   = child.close()
    if err:
	raise RuntimeError, '%s failed w/ exit code %d' % (command, err)
    return data[:-1] # (skip '\n' at the end)

if __name__ == "__main__":

    for c in ("createdb -U enstore chimera",
              "psql -U enstore chimera -c 'CREATE LANGUAGE plpgsql'",
              "psql -U enstore chimera < /opt/d-cache/libexec/chimera/sql/create.sql",
              "psql -U enstore chimera < /opt/d-cache/libexec/chimera/sql/pgsql-procedures.sql",
              "psql -U enstore chimera -f /opt/pnfs/share/sql/prep-chimera-for-migration.sql"):
        rc = os.system(c)
        if rc != 0 :
            sys.stderr.write("failed to execute command %s \n"%(c,))
            sys.exit(1)


    old_dir=os.getcwd()

    os.chdir("/pnfs/fs/usr")
    name_to_pnfsid={}
    for direntry in os.listdir(os.path.abspath(os.getcwd())):
        f=open(".(id)(%s)"%(direntry,),"r")
        for l in f:
            name_to_pnfsid[direntry] = l.strip('\n')
        f.close()

    os.chdir(old_dir)

    print "got ids", name_to_pnfsid

    for c in ("/sbin/service pnfs stop",
              "umount -l /pnfs/fs",
              "/sbin/service chimera-nfs-run.sh start",
              "mount localhost:/ /mnt",
              "mkdir /mnt/admin/etc/config/dCache",
              "touch /mnt/admin/etc/config/dCache/dcache.conf",
              "touch /mnt/admin/etc/config/dCache/'.(fset)(dcache.conf)(io)(on)'"
              "mkdir -p /mnt/pnfs/fs/usr",
              "umount -l /mnt",
              "mount localhost:/pnfs /pnfs"):
        rc = os.system(c)
        if rc != 0 :
            sys.stderr.write("failed to execute command %s \n"%(c,))
            sys.exit(1)

    os.chdir("/pnfs/fs/usr")

    name_to_chimeraid={}

    for c in ("echo \"chimera\" > \".(tag)(sGroup)\"",
              "echo \"StoreName sql\" > \".(tag)(OSMTemplate)\""):
        get_command_output(c)


    for dir in name_to_pnfsid.keys():
        os.mkdir(dir)
        f=open(".(id)(%s)"%(dir,),"r")
        for l in f:
            name_to_chimeraid[dir] = l.strip('\n')
        f.close()

    print "got ids", name_to_chimeraid

    os.chdir(old_dir)

    for c in ("umount -l /pnfs",
              "/sbin/service chimera-nfs-run.sh stop",
              "/sbin/service pnfs start"):
        rc = os.system(c)
        if rc != 0 :
            sys.stderr.write("failed to execute command %s \n"%(c,))
            sys.exit(1)

    print_message("STARTING PNFSDUMP")

    for dir in  name_to_pnfsid.keys():
        pnfsid=name_to_pnfsid[dir]
        chimeraid=name_to_chimeraid[dir]
        c=PNFS_DUMP%(pnfsid,dir,chimeraid,dir,dir,dir,)
        print c
        t0=time.time()
        rc=os.system(c)
        if rc !=0 :
            sys.stderr.write("failed to execute command %s \n"%(c,))
            sys.exit(1)
        t=time.time()
        print "Took %f seconds to do pnfsDump of %s "%(t-t0,dir,)

    for dir in  name_to_pnfsid.keys():
        c="psql -U enstore chimera -f chimera_%s.sql > chimera_%s.log 2>&1"%(dir,dir)
        t0=time.time()
        rc=os.system(c)
        if rc !=0 :
            sys.stderr.write("failed to execute command %s \n"%(c,))
            sys.exit(1)
        t=time.time()
        print "Took %f seconds to create %s "%(t-t0,dir,)

    print_message("FINISHED PNFSDUMP, STARTING INJECTING SQL")

    for c in ("psql -U enstore chimera -f enstore.sql",
              "psql -U enstore chimera -f enstore2chimera.sql",
              "echo \"select enstore2chimera();\" | psql -U enstore  chimera"):
        t0=time.time()
        rc=os.system(c)
        if rc !=0 :
            sys.stderr.write("failed to execute command %s \n"%(c,))
            sys.exit(1)
        t=time.time()
        print "Took %f seconds to execute %s "%(t-t0,c,)

    print_message("FINISHED INJECTING SQL, STARTING COMPANION IMPORT")


    for c in ("pg_dump -U enstore -t cacheinfo companion | psql -U enstore chimera",
              "psql -U enstore -f companion2chimera.sql chimera",
              "psql -U enstore chimera -c \"select companion2chimera()\"",
              "psql -U enstore chimera -c \"drop table cacheinfo\""):
        t0=time.time()
        rc=os.system(c)
        if rc !=0 :
            sys.stderr.write("failed to execute command %s \n"%(c,))
            sys.exit(1)
        t=time.time()
        print "Took %f seconds to execute %s "%(t-t0,c,)

    print_message("FINISHED COMPANION IMPORT, DONE!")


