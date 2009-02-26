#!/usr/bin/env python

import os
import sys
import getopt
import time


if __name__ == '__main__':
    # defaults
    pnfs_path = ""
    user=os.getlogin()
    node_list="/tmp/%s/node_list"%(user,)
    works = 1
    ff_width = 3
    make_copy = 1
    count = 50

    opts, args = getopt.getopt(sys.argv[1:], "p:u:c:b:a:w:n",
                               ["--bfid_list", "--user", "--node_list", "--works", "--no-copy", "--count" ])
    for o,a in opts:
        print o,a
        if o in ["-u", "--user"]:
            user = a
        elif o in ["-c", "--node_list"]:
            node_list = a
        elif o in ["-b", "--bfid_list"]:
            bfid_list = a
        elif o in ["-w", "--works"]:
            works = int(a)
        elif o in ["-n", "--no-copy"]:
            make_copy = 0
        elif o in ["-a", "--count"]:
            count = int(a)
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
            print "sending","scp ~/enstore/test/torture/read_test.sh %s@%s:~/bin"%(user, node) 
            os.system("scp ~/enstore/test/torture/read_test.sh %s@%s:~/bin"%(user, node))
            print "sending","scp  %s %s@%s:~/bin"%(bfid_list, user, node) 
            os.system("scp %s %s@%s:~/bin"%(bfid_list, user, node))
    b_list = os.path.join("~/bin", os.path.basename(bfid_list))
    for node in nodes:
        print "sending", 'ssh %s@%s "cd ~/bin; ~/bin/read_test.sh -b %s -c %s -w %s > read_%s.out 2>&1&"'%(user,node,b_list,count,works, pid)
        os.system('ssh %s@%s "cd ~/bin; . /usr/local/etc/setups.sh; ~/bin/read_test.sh -b %s -c %s -w %s > read_%s.out 2>&1&"'%(user,node,b_list,count, works, pid))
        time.sleep(1)

        
        
        
        
        
            
        
