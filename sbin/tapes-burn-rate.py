#!/usr/bin/env python

import os
import string

#&columns=Mb_User_Write%2C%20Tape_Volser%2C%20time_stamp
#&orders=Tape_Volser%20Asc%0D%0A

cmd = 'rm *.ps *.jpg *.data *.gnuplot *.tapes *.volumes drivestat.html'
print cmd
os.system(cmd)


href="http://miscomp.fnal.gov/misweb/cgi/misweb.pl\
?owner=SYS\
&dbname=procprd1\
&tables=DRIVESTAT_LOG\
&columns=time_stamp%2C%20Tape_Volser%2C%20Mb_User_Write\
&wheres=%20%20Time_Stamp%20%3E%3D%20to_date%28%2701-JAN-02%27%2C%27DD-MON-YY%27%29%20%20AND%20Operation%20%3D%20upper%28%27ABSOLUTE%27%29%20AND%20Mb_User_Write%20%3E%200\
&output_type=application/xls\
&orders=time_stamp%20Asc%0D%0A\
&drill_wheres=Yes\
&pagerows=500000\
&maxrows=500000"

for cmd in 'wget -O drivestat.html "%s"' % (href,), \
           'wget -O cdfen.volumes "http://cdfensrv2.fnal.gov/enstore/tape_inventory/VOLUMES_DEFINED"', \
           'wget -O d0en.volumes  "http://d0ensrv2.fnal.gov/enstore/tape_inventory/VOLUMES_DEFINED"', \
           'wget -O stken.volumes "http://stkensrv2.fnal.gov/enstore/tape_inventory/VOLUMES_DEFINED"':
    print cmd
    os.system(cmd)

TAPES = {}

for thefile in 'cdfen','d0en','stken':
    print 'processing',thefile,'volumes'
    f = open(thefile+".volumes","r")
    f.readline()
    f.readline()
    while 1:
        line = f.readline()
        if not line: break
        (v,a,s1,s2,u1,u2,l,vf) = line.split()
        if string.find(v,'NUL')>=0:
            continue
        sg = vf.split('.')[0]
        if TAPES.has_key(v):
            print 'duplicate tape',v
        else:
            TAPES[v]=l+'.'+sg
    f.close()

group_fd = {}
little = open('CD-bought.tapes','w')
group_fd['CD-bought.tapes'] = little

print 'sorting drivestat into storage group and library'
f = open('drivestat.html',"r")
f.readline()
f.readline()
f.readline()

while 1:
    line = f.readline()
    if not line: break
    (d,v,mb) = line.split()
    if not TAPES.has_key(v):
        #print "Can not find",v
        g = 'UNKNOWN.UNKNOWN'
    else:
        g = TAPES[v]
    l,sg = g.split('.')
    if group_fd.has_key(g):
        o = group_fd[g]
    else:
        print 'New group found:',g
        o = open(g+'.tapes','w')
        group_fd[g] = o
    o.write('%s' % (line,))
    if l in ['mezsilo','cdf','samlto'] or sg in ['cms']:
        pass
    else:
        little.write('%s' % (line,))

for g in group_fd.keys():
    o = group_fd[g]
    o.close()

d1='01-JAN-02'
d2='01-APR-02'
for g in group_fd.keys():
    cmd = "./tapes-plot-sg.py %s %s %s" % (g,d1,d2)
    print cmd
    os.system(cmd)

cmd = 'rcp *.ps *.jpg stkensrv2:/fnal/ups/prd/www_pages/enstore/burn-rate/'
print cmd
os.system(cmd)
