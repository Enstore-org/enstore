#!/usr/bin/env python
import time
import os
import sys
import pprint


if len(sys.argv) == 4:
    y,m,d=sys.argv[1],sys.argv[2],sys.argv[3]
elif len(sys.argv) == 1:
    tm = time.localtime()
    y,m,d=tm[0],'%02d'%(tm[1],),'%02d'%(tm[2],)
else:
    print 'Usage: %s [YYYY MM DD]'%(sys.argv[0],)

cmd='grep "Client host" /diska/enstore-log/LOG-%s-%s-%s > /tmp/bh-%s-%s-%s'%(y,m,d,y,m,d)
os.system(cmd)

f=open('/tmp/bh-%s-%s-%s'%(y,m,d),'r')
h={}
while 1:
    l = f.readline()[:-1]
    if l:
       if l.find("ALARM") != -1 and l.find("ALMSRV") != -1: 
           la = l.split(' ')
           print la

           while 1:
               la.pop(0)
               if la[0] == 'state':
                   break
           la.pop(0)
           state = la[0]
           for e in la:
               if e.find('fnal') != -1 and e.find('mvr') == -1:
                   if not h.has_key(e):
                       h[e] = {}
                   if h[e].has_key(state):
                       h[e][state] = h[e][state]+1
                   else:
                       h[e][state]=1

    else:
       break
pprint.pprint(h)
print len(h)

    
    
    
        
