#!/usr/bin/env python

import os
import string
import sys
import popen2
import time
import pprint


# tape capacity in GB
CAP_9840=20
CAP_9940=60
CAP_9940B=200
CAP_LTO=100


def sort_the_file(infile):
    fi = open(infile,'r')
    fo = open('%s.tmp'%(infile,),'w')
    while 1:
        line = fi.readline()
        if not line: break
        d,v,mb=line.split()
        t=time.mktime(time.strptime(d,"%d-%b-%y"))
        do=time.strftime("%Y-%m-%d",time.localtime(t))
        ol = string.join((do,v,mb),'\t')
        fo.write('%s\n' % (ol,))
    fi.close()
    fo.close()
    os.system("sort %s.tmp > /tmp/%s"%(infile,infile))
    fi = open('/tmp/%s'%(infile,),'r')
    fo = open(infile,'w')
    while 1:
        line = fi.readline()
        if not line: break
        d,v,mb=line.split()
        t=time.mktime(time.strptime(d,"%Y-%m-%d"))
        do=time.strftime("%d-%b-%y",time.localtime(t))
        ol = string.join((do.upper(),v,mb),'\t')
        fo.write('%s\n' % (ol,))
    fi.close()
    fo.close()
    
    
    
#&columns=Mb_User_Write%2C%20Tape_Volser%2C%20time_stamp
#&orders=Tape_Volser%20Asc%0D%0A

cmd = 'rm *.ps *.jpg *.data *.gnuplot *.tapes *.volumes drivestat.* dstat.*'
print cmd
os.system(cmd)

#d1='01-NOV-01'
#d2='01-MAY-02'

d1 = None
d2 = None
hosts = ("d0ensrv6.fnal.gov","stkensrv6.fnal.gov","cdfensrv6.fnal.gov")

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

        QUOTAS[l+'.'+sg] = (wv,bv,su,l)
    f.close()

print "QUOTAS"
pprint.pprint(QUOTAS)
#sys.exit()
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
ALL_9940 = open('ALL_9940.tapes','a')
ALL_9940B = open('ALL_9940B.tapes', 'a')
CD-9940B = open('CD-9940B.tapes', 'a')
group_fd['ALL_9940'] = ALL_9940
group_fd['ALL_9940B'] = ALL_9940B
group_fd['CD-9940B'] = CD-9940B


print 'sorting drivestat into storage group and library'
f = open('dstat.txt',"r")
eagle_mb = 0L
beagle_mb = 0L
eagle_v={}
beagle_v={}
all_9940_mb = 0L
all_9940b_mb = 0L
cd_9940b_mb = 0L
all_9940_v = {}
all_9940b_v = {}
cd_9940b_v = {}
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
    if not g in ['ALL_9940B', 'ALL_9940']: 
        o.write('%s\n' % (ol,))
    #if g=='cdf.cdf':
        #print "WWW",ol
    if l in ['mezsilo', 'cdf', '9940']:
       all_9940_mb = all_9940_mb + long(mb)
       all_9940_v[v] = 1
       ALL_9940.write('%s\n' % (ol,))
       if l == '9940':
           beagle_mb = beagle_mb + long(mb)
           beagle_v[v] = 1
           beagle.write('%s\n' % (ol,))
    elif l in ['D0-9940B', 'CDF-9940B', 'CD-9940B']:
       #all_9940b_mb = all_9940_mb + long(mb)
       all_9940b_v[v] = 1
       ALL_9940B.write('%s\n' % (ol,))
       if l == 'CD-9940B':
           cd_9940b_v[v] = 1
           CD-9940B.write('%s\n' % (ol,))
         
    elif l in ['samlto'] or sg in ['cms']:
        pass
    elif l == 'eagle':
        eagle_mb = eagle_mb + long(mb)
        eagle_v[v]=1
        eagle.write('%s\n' % (ol,))
    #elif l == '9940':
    #    beagle_mb = beagle_mb + long(mb)
    #    beagle_v[v] = 1
    #    beagle.write('%s\n' % (ol,))
    else:
        pass
        #print 'What is it, not cdf,samlto,cms,eagle,9940 CD tape?',line

#sys.exit()
for g in group_fd.keys():
    o = group_fd[g]
    o.close()

#import pprint
#pprint.pprint(QUOTAS)
_9940_wv = _9940_bv = 0
_9940_su = 0.
_9940b_wv = _9940b_bv = 0
_9940b_su = 0.

cd_9940b_wv = cd_9940b_bv = 0
cd_9940b_su = 0.

rpt=open('report','w')
for g in group_fd.keys():
    print "make plot for %s"%(g,)
    if g == 'ALL_9940':
        pass
        
        #wv = len(all_9940_v)
        #su="%.2f%s"%(all_9940_mb / 1024.,"GB")
    elif g == 'ALL_9940B':
        pass
        #wv = len(all_9940b_v)
        #su="%.2f%s"%(all_9940b_mb / 1024.,"GB")
    elif g == 'CD-9940B':
        pass
        #wv = len(all_9940b_v)
        #su="%.2f%s"%(all_9940b_mb / 1024.,"GB")
    if QUOTAS.has_key(g):
        (wv,bv,su, l) = QUOTAS[g]
        
        if l in ['D0-9940B', 'CDF-9940B', 'CD-9940B']:
          _9940b_wv = _9940b_wv + int(wv)
          _9940b_bv = _9940b_bv + int(bv)
          su = float(su.split("G")[0])
          _9940b_su = _9940b_su + su
          if l == 'CD-9940B':
              cd_9940b_wv = cd_9940b_wv + int(wv)
              cd_9940b_bv = cd_9940b_bv + int(bv)
              cd_9940b_su = cd_9940b_su + su
             
        elif l in ['mezsilo', 'cdf', '9940']:
          su = float(su.split("G")[0])
          rpt.write("GROUP %s WR %s BL %s GB %s\n"%(g, wv,bv,su)) 
          _9940_wv = _9940_wv + int(wv)
          _9940_bv = _9940_bv + int(bv)
          _9940_su = _9940_su + su
          cap = CAP_9940
        
            
    elif g == "CD-9840":
        (wv1,bv1,su1,l) = QUOTAS.get('blank-9840.none',('-1','-1','-1','-1'))
        (wv2,bv2,su2,l) = QUOTAS.get('eagle.none:',('-1','-1','-1','-1'))
        #wv = string.atoi(wv1)+string.atoi(wv2)
        wv = len(eagle_v)
        bv = string.atoi(bv1)+string.atoi(bv2)
        #su = '0.0GB'
        su="%.2f%s"%(eagle_mb / 1024.,"GB")
        cap = CAP_9840
    elif g == "CD-9940":
        (wv1,bv1,su1,l) = QUOTAS.get('blank-9940.none',('-1','-1','-1','-1'))
        (wv2,bv2,su2,l) = QUOTAS.get('9940.none:',('-1','-1','-1','-1'))
        #wv = string.atoi(wv1)+string.atoi(wv2)
        wv = len(beagle_v)
        bv = string.atoi(bv1)+string.atoi(bv2)
        #su = '0.0GB'
        su="%.2f%s"%(beagle_mb / 1024.,"GB")
        cap = CAP_9940
    elif g == 'ALL_9940':
        pass
        
        #wv = len(all_9940_v)
        #su="%.2f%s"%(all_9940_mb / 1024.,"GB")
    elif g == 'ALL_9940B':
        pass
        #wv = len(all_9940b_v)
        #su="%.2f%s"%(all_9940b_mb / 1024.,"GB")
    else:
        print 'What group is this',g
        (wv,bv,su) = ('?','?','?')
    if g in ['ALL_9940', 'ALL_9940B', 'CD-9940B']:
        pass
    else:
        cmd = "$ENSTORE_DIR/sbin/tapes-plot-sg.py %s %s %s %s %s %s %s" % (g,d1,d2,wv,bv,su, cap)
        print cmd
        os.system(cmd)
sort_the_file('ALL_9940.tapes')
cmd = "$ENSTORE_DIR/sbin/tapes-plot-sg.py %s %s %s %s %s %s %s" % ('ALL_9940',d1,d2,_9940_wv,_9940_bv,_9940_su, 60)
print cmd
os.system(cmd)
sort_the_file('ALL_9940B.tapes')
cmd = "$ENSTORE_DIR/sbin/tapes-plot-sg.py %s %s %s %s %s %s %s" % ('ALL_9940B',d1,d2,_9940b_wv,_9940b_bv,_9940b_su, 200)
print cmd
os.system(cmd)
cmd = "$ENSTORE_DIR/sbin/tapes-plot-sg.py %s %s %s %s %s %s %s" % ('CD-9940B',d1,d2,cd_9940b_wv,cd_9940b_bv,cd_9940b_su, 200)
print cmd
os.system(cmd)


cmd = 'source /home/enstore/gettkt; $ENSTORE_DIR/sbin/enrcp *.ps *.jpg stkensrv2:/fnal/ups/prd/www_pages/enstore/burn-rate/'
print cmd
os.system(cmd)

