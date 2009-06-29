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
import configuration_client



def usage(arg):
    print "usage: %s lm_name"%(arg,)
    

if len(sys.argv) != 2:
    usage(sys.argv[0])
    sys.exit(1)
intf = library_manager_client.LibraryManagerClientInterface(user_mode=0)
lmc = library_manager_client.LibraryManagerClient((intf.config_host, intf.config_port), sys.argv[1])
csc = configuration_client.ConfigurationClient()
lmconf = csc.get(sys.argv[1])
print lmconf
r_port = lmconf.get('port', None)
e_port = lmconf.get('encp_port', None)
m_port = lmconf.get('mover_port', None)

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

    if r_port:
        cmd = 'netstat -npl | grep %s'%(r_port,)
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

    if m_port:
        cmd = 'netstat -npl | grep %s'%(m_port,)
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

    if e_port:
        cmd = 'netstat -npl | grep %s'%(e_port,)
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
    msg= '%s %s'%(time.ctime(time.time()), ql)
    if e_port:
        msg = '%s %s'%(msg, e_queue)
    if r_port:
        msg = '%s %s'%(msg, r_queue)
    if m_port:
        msg = '%s %s'%(msg, m_queue)
    msg = '%s %s %s'%(msg, r_err, error_rate)
    print msg
    time.sleep(10)
    
