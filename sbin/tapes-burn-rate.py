#!/usr/bin/env python

import os
import string
import sys
import popen2
import time

#&columns=Mb_User_Write%2C%20Tape_Volser%2C%20time_stamp
#&orders=Tape_Volser%20Asc%0D%0A

cmd = 'rm *.ps *.jpg *.data *.gnuplot *.tapes *.volumes drivestat.* dstat.*'
print cmd
os.system(cmd)

#d1='01-NOV-01'
#d2='01-MAY-02'

d1 = None
d2 = None
hosts = ("d0ensrv0.fnal.gov","stkensrv0.fnal.gov","cdfensrv0.fnal.gov")

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

query_cmd='psql -h %s -p 8076 -o "drivestat.%s.txt" -c "select time,tape_volser,mb_user_write from status where date(time) between date(%s%s%s) and date(%s%s%s) and mb_user_write != 0;" drivestat'

for host in hosts:
    pipeObj = popen2.Popen3(query_cmd%(host, host, "'", d1, "'", "'", d2, "'"), 0, 0)
    if pipeObj is None:
        sys.exit(1) 
    stat = pipeObj.wait()
    result = pipeObj.fromchild.readlines()  # result has returned string

for host in hosts:
    os.system("cat drivestat.%s.txt >> dstat.txt"%(host,))

    

for cmd in \
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
        ll = line.split()
        if len(ll) < 8: # to be paranoid
            continue
        if line[0] == '<': # skip html tags
            continue
        (v,a,s1,s2,u1,u2,l) = ll[:7]
        vf = ll[-1]
        # (v,a,s1,s2,u1,u2,l,vf) = line.split()
        if string.find(v,'NUL')>=0:
            continue
        sg = vf.split('.')[0]
        if TAPES.has_key(v):
            print 'duplicate tape',v
        else:
            TAPES[v]=l+'.'+sg
    f.close()

group_fd = {}
# copy all "old" tapes files
#os.system("cp ../burn-rate/*.tapes .")
#eagle = open('CD-9840.tapes','w')
eagle = open('CD-9840.tapes','a')
group_fd['CD-9840'] = eagle
#beagle = open('CD-9940.tapes','w')
beagle = open('CD-9940.tapes','a')
group_fd['CD-9940'] = beagle

print 'sorting drivestat into storage group and library'
f = open('dstat.txt',"r")
while 1:
    line = f.readline()
    if not line: break
    # skip the line if it begins not with digit
    i = 0
    if line[0].isspace():
        if len(line) > 1:
            i = 1
    if not line[i].isdigit():
        #print "L",line
        continue
    (d,t,junk,v,junk1,mb) = line.split()
    #print "qqqqqqqqq",d,junk,v,junk1,mb
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
        #o = open(g+'.tapes','w')
        o = open(g+'.tapes','a')
        group_fd[g] = o
    # convert date
    ti = time.mktime(time.strptime(d,"%Y-%m-%d"))
    do = time.strftime("%d-%b-%y",time.localtime(ti))
    
    ol = string.join((do.upper(),v,mb),'\t')
    o.write('%s\n' % (ol,))
    #if g=='cdf.cdf':
        #print "WWW",ol
    if l in ['mezsilo','cdf','samlto'] or sg in ['cms']:
        pass
    elif l == 'eagle':
        eagle.write('%s\n' % (ol,))
    elif l == '9940':
        beagle.write('%s\n' % (ol,))
    else:
        pass
        #print 'What is it, not cdf,samlto,cms,eagle,9940 CD tape?',line

#sys.exit()
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

cmd = 'source /home/enstore/gettkt; $ENSTORE_DIR/sbin/enrcp *.ps *.jpg stkensrv2:/fnal/ups/prd/www_pages/enstore/burn-rate/'
print cmd
os.system(cmd)

