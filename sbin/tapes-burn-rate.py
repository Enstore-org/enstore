#!/usr/bin/env python

import os
import string
import sys

#&columns=Mb_User_Write%2C%20Tape_Volser%2C%20time_stamp
#&orders=Tape_Volser%20Asc%0D%0A

cmd = 'rm *.ps *.jpg *.data *.gnuplot *.tapes *.volumes drivestat.html'
print cmd
os.system(cmd)

#d1='01-NOV-01'
#d2='01-MAY-02'

d1 = None
d2 = None
for when in 'date --date "4 months ago"  +"%b-%y"','date --date "34 days"  +"%b-%y"':
    d = os.popen(when,'r')
    dat=d.readlines()
    d.close()
    if d1 == None:
       d1 = '01-'+dat[0][:-1]
       d1 = string.upper(d1)
    else:
       d2 = '01-'+dat[0][:-1]
       d2 = string.upper(d2)
print 'Generating burn-rate plots from', d1, ' to ',d2


href="http://miscomp.fnal.gov/misweb/cgi/misweb.pl\
?owner=SYS\
&dbname=procprd1\
&tables=DRIVESTAT_LOG\
&columns=time_stamp%2C%20Tape_Volser%2C%20Mb_User_Write\
&wheres=%20%20Time_Stamp%20%3E%3D%20to_date%28%27"+d1+"%27%2C%27DD-MON-YY%27%29%20%20AND%20Operation%20%3D%20upper%28%27ABSOLUTE%27%29%20AND%20Mb_User_Write%20%3E%200\
&output_type=application/xls\
&orders=time_stamp%20Asc%0D%0A\
&drill_wheres=Yes\
&pagerows=500000\
&maxrows=500000"


for cmd in \
           '$ENSTORE_DIR/bin/Linux/wget -O drivestat.html "%s"' % (href,),\
           '$ENSTORE_DIR/bin/Linux/wget -O cdfen.volumes "http://cdfensrv2.fnal.gov/enstore/tape_inventory/VOLUMES_DEFINED"', \
           '$ENSTORE_DIR/bin/Linux/wget -O d0en.volumes  "http://d0ensrv2.fnal.gov/enstore/tape_inventory/VOLUMES_DEFINED"', \
           '$ENSTORE_DIR/bin/Linux/wget -O stken.volumes "http://stkensrv2.fnal.gov/enstore/tape_inventory/VOLUMES_DEFINED"', \
           '$ENSTORE_DIR/bin/Linux/wget -O cdfen.quotas  "http://cdfensrv2.fnal.gov/enstore/tape_inventory/VOLUME_QUOTAS"', \
           '$ENSTORE_DIR/bin/Linux/wget -O d0en.quotas   "http://d0ensrv2.fnal.gov/enstore/tape_inventory/VOLUME_QUOTAS"', \
           '$ENSTORE_DIR/bin/Linux/wget -O stken.quotas  "http://stkensrv2.fnal.gov/enstore/tape_inventory/VOLUME_QUOTAS"':
    print cmd
    os.system(cmd)

QUOTAS = {}
for thefile in 'cdfen','d0en','stken':
    print 'processing',thefile,'quotas'
    f = open(thefile+".quotas","r")
    while 1:
        line = f.readline()
        if not line: break
        if len(line) <= 1:
            continue
        if string.find(line,'----------') >= 0:
            break
        if string.find(line,'----------') >= 0 or string.find(line,'Date this') >= 0 or string.find(line,'Storage Group') >= 0:
            continue
        if string.find(line,'null') >= 0 or string.find(line,'NULL') >=0:
            continue
        if string.find(line,'emergency') >= 0:
            try:
                (c,l,sg,e,ra,aa,q,a,bv,wv,dv,su,af,df,uf) = line.split()
            except:
                print 'can not parse', line,len(line)
                continue
        else:

            try:
                (c,l,sg,ra,aa,q,a,bv,wv,dv,su,af,df,uf) = line.split()
            except:
                print 'can not parse', line,len(line)
                continue

        QUOTAS[l+'.'+sg] = (wv,bv,su)
    f.close()

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
eagle = open('CD-9840.tapes','w')
group_fd['CD-9840'] = eagle
beagle = open('CD-9940.tapes','w')
group_fd['CD-9940'] = beagle

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
        print "Can not find",v
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
    elif l == 'eagle':
        eagle.write('%s' % (line,))
    elif l == '9940':
        beagle.write('%s' % (line,))
    else:
        print 'What is it, not cdf,samlto,cms,eagle,9940 CD tape?',line


for g in group_fd.keys():
    o = group_fd[g]
    o.close()

#import pprint
#pprint.pprint(QUOTAS)

for g in group_fd.keys():
    if QUOTAS.has_key(g):
        (wv,bv,su) = QUOTAS[g]
    elif g == "CD-9840":
        (wv1,bv1,su1) = QUOTAS['blank-9840.none']
        (wv2,bv2,su2) = QUOTAS['eagle.none:']
        wv = string.atoi(wv1)+string.atoi(wv2)
        bv = string.atoi(bv1)+string.atoi(bv2)
        su = '0.0GB'
    elif g == "CD-9940":
        (wv1,bv1,su1) = QUOTAS['blank-9940.none']
        (wv2,bv2,su2) = QUOTAS['9940.none:']
        wv = string.atoi(wv1)+string.atoi(wv2)
        bv = string.atoi(bv1)+string.atoi(bv2)
        su = '0.0GB'
    else:
        print 'What group is this',g
        (wv,bv,su) = ('?','?','?')
    cmd = "$ENSTORE_DIR/sbin/tapes-plot-sg.py %s %s %s %s %s %s" % (g,d1,d2,wv,bv,su)
    print cmd
    os.system(cmd)
    print


cmd = 'source /home/enstore/gettkt; $ENSTORE_DIR/sbin/enrcp *.ps *.jpg stkensrv2:/fnal/ups/prd/www_pages/enstore/burn-rate/'
print cmd
os.system(cmd)

