#!/bin/env python
###############################################################################
# $Author$
# $Date$
# $Id$
#
#  Script that runs md5sum on result of pnfsDump
#  java must be in the path
#  should be run from directory containing pnfsDump output
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
    old_dir=os.getcwd()

    os.chdir("/pnfs/fs/usr")
    name_to_pnfsid={}
    for direntry in os.listdir(os.path.abspath(os.getcwd())):
        f=open(".(id)(%s)"%(direntry,),"r")
        for l in f:
            name_to_pnfsid[direntry] = l.strip('\n')
        f.close()

    os.chdir(old_dir)

    print_message("STARTING MD5SUM")

    for dir in name_to_pnfsid.keys():
        verify_file = os.path.join(old_dir,"verify_%s.md5"%(dir,))
        verify_log  = os.path.join(old_dir,"verify_%s.md5.log"%(dir,))
        os.chdir("/pnfs/fs/usr/%s"%(dir,))
        c="md5sum -c %s | grep -v \":\ OK$\" > %s 2>&1"%(verify_file,verify_log,)
        t0=time.time()
        rc=os.system(c)
        t=time.time()
        print "Took %f seconds to verify %s "%(t-t0,dir,)

    print_message("FINISHED MD5SUM, STARTING StorageInfo check")

    os.chdir(old_dir)


    for dir in name_to_pnfsid.keys():
        file_list     = os.path.join(old_dir,"files_%s.lst"%(dir,))
        file_log      = os.path.join(old_dir,"files_%s.lst.log"%(dir,))
        file_errors   = os.path.join(old_dir,"files_%s.lst.errors"%(dir,))
        os.chdir("/pnfs/fs/usr/%s"%(dir,))
        c="chmod +x /opt/d-cache/libexec/migration-check.sh"
        rc=os.system(c)
        c="/opt/d-cache/libexec/migration-check.sh -k %s 1> %s 2> %s"%(file_list,file_log,file_errors)
        t0=time.time()
        rc=os.system(c)
        if rc !=0 :
            sys.stderr.write("failed to execute command %s \n"%(c,))
            sys.exit(1)
        t=time.time()
        print "Took %f seconds to check StorageInfo on %s "%(t-t0,dir,)

    print_message("FINISHED StorageInfo check")
