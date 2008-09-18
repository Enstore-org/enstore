#!/usr/bin/env python

import os
import sys
import getopt
import time


if __name__ == '__main__':
    # defaults
    pnfs_path = ""
    user=os.getlogin()
    data="/scratch_dcache/cdfcaf"
    node_list="/tmp/%s/node_list"%(user,)
    works = 1
    ff_width = 3
    make_copy = 1

    opts, args = getopt.getopt(sys.argv[1:], "p:u:d:c:w:f:n",
                               ["--pnfs_path", "--user", "--data_path", "--node_list", "--works", "--ff_width","--no-copy" ])
    for o,a in opts:
        print o,a
        if o in ["-p", "--pnfs_path"]:
            pnfs_path = a
        elif o in ["-u", "--user"]:
            user = a
        elif o in ["-d", "--data_path"]:
            data = a
        elif o in ["-c", "--node_list"]:
            node_list = a
        elif o in ["-w", "--works"]:
            works = int(a)
        elif o in ["-f", "--ff_width"]:
            ff_width = int(a)
        elif o in ["-n", "--no-copy"]:
            make_copy = 0
    nodes = []
    f = open(node_list,'r')
    while 1:
        l = f.readline()
        if l:
            l = l[:-1]
            if l[0] == "#":
                continue
            if not l in nodes:
                nodes.append(l)
        else:
            break
    print nodes
    pid = os.getpid()
    if make_copy:
        for node in nodes:
            print "sending","scp ~/.bashrc %s@%s:~/"%(user, node) 
            os.system("scp ~/.bashrc %s@%s:~/"%(user, node))
            print "sending","scp ~/enstore/test/torture/write_test.sh %s@%s:~/bin"%(user, node) 
            os.system("scp ~/enstore/test/torture/write_test.sh %s@%s:~/bin"%(user, node))
            print "sending", 'ssh %s@%s "if [ ! -d $%s ]; then mkdir -p $%s; fi;"'%(user, node, data, data)
            os.system('ssh %s@%s "if [ ! -d $%s ]; then mkdir -p $%s; fi;"'%(user, node, data, data))
    for i in range(works):
        for node in nodes:
            print "sending", 'ssh %s@%s "cd %s; rm -f write_%s.out; ~/bin/write_test.sh -p %s -d %s -f %s > write_%s.out 2>&1&"'%(user,node,data,pid,pnfs_path,data,ff_width,pid)
            os.system('ssh %s@%s "cd %s; rm -f write_%s.out; ~/bin/write_test.sh -p %s -d %s -f %s > write_%s.out 2>&1&"'%(user,node,data,pid,pnfs_path,data,ff_width,pid))
            time.sleep(1)

        
        
        
        
        
            
        
