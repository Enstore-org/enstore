#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################
import sys
import time
import popen2 
import library_manager_client



def usage(arg):
    print "usage: %s lm_name"%(arg,)
    

if len(sys.argv) != 2:
    usage(sys.argv[0])
    sys.exit(1)
intf = library_manager_client.LibraryManagerClientInterface(user_mode=0)
lmc = library_manager_client.LibraryManagerClient((intf.config_host, intf.config_port), sys.argv[1])
rqs1=0.
t1=time.time()
first = True
while 1:
    a = lmc.get_pending_queue_length()
    try:
        ql = a['queue_length']
        #rqs = a['rqs']
    except KeyError:
        ql = 0
        rqs = 0
        print 'KeyError', a

    cmd = 'netstat -npl | grep 7511'
    pipeObj = popen2.Popen3(cmd, 0, 0)
    stat = pipeObj.wait()
    result = pipeObj.fromchild.readlines()

    for l in result:
        l.strip()
        if l.find('udp') != -1:
            a=l.split(' ')
            c = 0
            for i in a:
                if i == '':
                    c = c + 1
            for i in range(c):
                a.remove('')
            r_queue = a[1]
    cmd = 'netstat -npl | grep 7712'
    pipeObj = popen2.Popen3(cmd, 0, 0)
    stat = pipeObj.wait()
    result = pipeObj.fromchild.readlines()

    for l in result:
        l.strip()
        if l.find('udp') != -1:
            a=l.split(' ')
            c = 0
            for i in a:
                if i == '':
                    c = c + 1
            for i in range(c):
                a.remove('')
            m_queue = a[1]
    cmd = 'netstat -npl | grep 7713'
    pipeObj = popen2.Popen3(cmd, 0, 0)
    stat = pipeObj.wait()
    result = pipeObj.fromchild.readlines()

    for l in result:
        l.strip()
        if l.find('udp') != -1:
            a=l.split(' ')
            c = 0
            for i in a:
                if i == '':
                    c = c + 1
            for i in range(c):
                a.remove('')
            e_queue = a[1]
    cmd = 'netstat -s | grep "packet receive errors"'
    pipeObj = popen2.Popen3(cmd, 0, 0)
    stat = pipeObj.wait()
    result = pipeObj.fromchild.readlines()
    for l in result:
        l.strip(' ')
        if l.find('errors') != -1:
            r_err = long(l.split(' ')[4])
    t=time.time()
    if first:
        first = False
        error_rate = 0
        t1 = t
        r_err0 = r_err 
    else:
        error_rate = (r_err-r_err0)/(t-t1)
        t1 = t
        r_err0 = r_err 
        
        
    t=time.time()
    #print rqs
    #print rqs-rqs1
    #print t
    #print t1
    print time.ctime(time.time()), ql, e_queue, r_queue, m_queue, r_err, error_rate
    time.sleep(10)
    
