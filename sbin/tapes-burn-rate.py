#!/usr/bin/env python

import os
import string
import sys
import popen2
import time
import pprint
import configuration_client

import configuration_client
import enstore_constants
import pg

# tape capacity in GB
CAP_9840=20
CAP_9940=60
CAP_9940B=200
CAP_LTO=100

systems=[]
#systems=['cdfen','d0en','stken']
QUOTAS = {}
libraries = {}
#for system in systems:
#    libraries[system] = {}
#
def sort_the_file(infile):
    fi = open(infile,'r')
    fo = open('%s.tmp'%(infile,),'w')
    while 1:
        line = fi.readline()
        if not line: break
        try:
            d,v,mb=line.split()
            t=time.mktime(time.strptime(d,"%d-%b-%y"))
            do=time.strftime("%Y-%m-%d",time.localtime(t))
            ol = string.join((do,v,mb),'\t')
            fo.write('%s\n' % (ol,))
        except:
            print "Error parsing line ",line, infile
            break
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

def get_capacity(volume, system):
    rtn = os.popen("grep %s %s.vol_sizes"%(volume, system)).readlines()[0]
    if rtn:
        list = rtn.split()
        try:
            ret = int(float(list[4][:-2]))
        except:
            ret = 0
    return ret

#&columns=Mb_User_Write%2C%20Tape_Volser%2C%20time_stamp
#&orders=Tape_Volser%20Asc%0D%0A

cmd = 'rm *.ps *.jpg *.data *.gnuplot *.tapes *.volumes drivestat.* dstat.*'
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
intf  = configuration_client.ConfigurationClientInterface(user_mode=0)
csc   = configuration_client.ConfigurationClient((intf.config_host, intf.config_port))
inq_d = csc.get(enstore_constants.INQUISITOR, {})
html_dir = inq_d["html_file"]
this_web_host = csc.get('web_server').get('ServerHost','stkensrv2')

servers=[]
servers=csc.get('known_config_servers')
query_cmd='psql -h %s -p %d -U %s -o "drivestat.%s.txt" -c "select time,tape_volser,mb_user_write from status where date(time) between date(%s%s%s) and date(%s%s%s) and mb_user_write != 0;" drivestat'


for server in servers:
    server_name,server_port = servers.get(server)
    if ( server_port != None ):
        config_server_client   = configuration_client.ConfigurationClient((server_name, server_port))
        acc = config_server_client.get(enstore_constants.DRIVESTAT_SERVER)
        db_server_name = acc.get('dbhost','localhost')
        db_name        = acc.get('dbname','drivestat')
        db_user        = acc.get('dbuser','enstore')
        db_port        = acc.get('dbport',5432)
        name           = db_server_name.split('.')[0]
        pipeObj = popen2.Popen3(query_cmd%(db_server_name,db_port,db_user,db_server_name,"'", d1, "'", "'", d2, "'"), 0, 0)
        if pipeObj is None:
            sys.exit(1)
        stat = pipeObj.wait()
        result = pipeObj.fromchild.readlines()  # result has returned string
        os.system("cat drivestat.%s.txt >> dstat.txt"%(db_server_name,))
        web_host=config_server_client.get('web_server').get('ServerHost',server_name)
        systems.append(web_host)

for system in systems:
    cmd='wget -O %s.volumes "http://%s/enstore/tape_inventory/VOLUMES_DEFINED"'%(system,system)
    os.system(cmd)

    cmd='wget -O  %s.vol_sizes "http://%s/enstore/tape_inventory/VOLUME_SIZE"'%(system,system)
    os.system(cmd)

    cmd='wget -O  %s.quotas "http://%s/enstore/tape_inventory/VOLUME_QUOTAS"'%(system,system)
    os.system(cmd)

for system in systems:
    libraries[system] = {}


def lib_capacity(g):
    l, sg = g.split('.')
    for system in libraries.keys():
        if libraries[system].has_key(l) and sg in libraries[system][l]['storage_groups']:
            cap = libraries[system][l]['volume_capacity']
            break
    else:
        cap = 0
    return cap

for thefile in systems:
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
                (c,l,sg,e,ra,aa,q,a,bv,wv,dv,su,af,df,uf, rf, mf) = line.split()
                if bv.find('/') != -1:
                    bv = bv.split('/')[1]
            except:
                print 'can not parse', line,len(line)
                continue
        else:

            try:
                (c,l,sg,ra,aa,q,a,bv,wv,dv,su,af,df,uf, rf, mf) = line.split()
                if bv.find('/') != -1:
                    bv = bv.split('/')[1]
            except:
                print 'can not parse', line,len(line)
                continue

        s= sg.split(':')
        if len(s) > 1:
            sg = s[0]
        QUOTAS[l+'.'+sg] = (wv,bv,su,l)
        if not l in  libraries[thefile].keys():
            libraries[thefile][l]={'volume_capacity':None,'storage_groups':[]}
        if not sg in libraries[thefile][l]['storage_groups']:
            libraries[thefile][l]['storage_groups'].append(sg)
    f.close()

TAPES = {}
#for thefile in 'cdfen','d0en','stken':
for thefile in systems:
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
eagle = open('CD-9840.tapes','a')
group_fd['CD-9840'] = eagle
beagle = open('CD-9940.tapes','a')
group_fd['CD-9940'] = beagle
ALL_9940 = open('ALL_9940.tapes','a')
ALL_9940B = open('ALL_9940B.tapes', 'a')
ALL_LTO3 = open('ALL_LTO3.tapes', 'a')
ALL_LTO4 = open('ALL_LTO4.tapes', 'a')
CD_9940B = open('CD-9940B.tapes', 'a')
CD_LTO3 = open('CD-LTO3.tapes', 'a')
group_fd['ALL_9940'] = ALL_9940
group_fd['ALL_9940B'] = ALL_9940B
group_fd['CD-9940B'] = CD_9940B
group_fd['ALL_LTO3'] = ALL_LTO3
group_fd['ALL_LTO4'] = ALL_LTO4
group_fd['CD-LTO3'] = CD_LTO3


f = open('dstat.txt',"r")
eagle_mb = 0L
beagle_mb = 0L
eagle_v={}
beagle_v={}
all_lto3_mb = 0L
all_lto4_mb = 0L
all_9940_mb = 0L
all_9940b_mb = 0L
cd_9940b_mb = 0L
cd_lto3_mb = 0L
all_lto3_v = {}
all_lto4_v = {}
all_9940_v = {}
all_9940b_v = {}
cd_9940b_v = {}
cd_9940b_v = {}
cd_lto3_v = {}
while 1:
    line = f.readline()
    if not line: break
    # skip the line if it begins not with digit
    i = 0
    if line[0].isspace():
        if len(line) > 1:
            i = 1
    if not line[i].isdigit():
        continue
    (d,t,junk,v,junk1,mb) = line.split()
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

    #define the tape capacity
    for system in libraries.keys():
        if libraries[system].has_key(l) and sg in libraries[system][l]['storage_groups']:
            if not libraries[system][l]['volume_capacity']:
                cap = get_capacity(v, system)
                if cap == 0:
                    print "DON'T KNOW WHAT IS CAPACITY  FOR %s"%(g,)
                    cap = 100
                libraries[system][l]['volume_capacity'] = cap
            break

    # convert date
    ti = time.mktime(time.strptime(d,"%Y-%m-%d"))
    do = time.strftime("%d-%b-%y",time.localtime(ti))

    ol = string.join((do.upper(),v,mb),'\t')
    if not g in ['ALL_9940B', 'ALL_9940']:
        o.write('%s\n' % (ol,))
    if l in ['CD-LTO3', 'CDF-LTO3', 'D0-LTO3']:
        if l == 'CD-LTO3' :
            cd_lto3_mb = cd_lto3_mb + long(mb)
            CD_LTO3.write('%s\n' % (ol,))
            cd_lto3_v[v] = 1
        all_lto3_mb = all_lto3_mb +  long(mb)
        all_lto3_v[v] = 1
        ALL_LTO3.write('%s\n' % (ol,))
    if l in ['CD-LTO4', 'CDF-LTO4', 'D0-LTO4', 'CD-LTO4G1', 'D0-LTO4G1']:
        all_lto4_mb = all_lto4_mb +  long(mb)
        all_lto4_v[v] = 1
        ALL_LTO4.write('%s\n' % (ol,))
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
           CD_9940B.write('%s\n' % (ol,))

    elif l in ['samlto', 'samlto2'] or sg in ['cms']:
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
        #pass
        print 'What is it, not cdf,samlto,cms,eagle,9940 CD tape?',l,sg

#sys.exit()
for g in group_fd.keys():
    o = group_fd[g]
    o.close()

_9940_wv = _9940_bv = 0
_9940_su = 0.
_9940b_wv = _9940b_bv = 0
_9940b_su = 0.

cd_9940b_wv = cd_9940b_bv = 0
cd_9940b_su = 0.

all_lto3_wv = all_lto3_bv = 0
all_lto3_su = 0.

all_lto4_wv = all_lto4_bv = 0
all_lto4_su = 0.

cd_lto3_wv = cd_lto3_bv = 0
cd_lto3_su = 0.

rpt=open('report','w')
for g in group_fd.keys():
    print "make plot for %s"%(g,)
    if g == 'ALL_9940':
        pass
    elif g == 'ALL_9940B':
        pass
    elif g == 'ALL_LTO3':
        pass
    elif g == 'ALL_LTO4':
        pass
    elif g == 'CD-9940B':
        pass
    elif g == 'CD-LTO3':
        pass
    if QUOTAS.has_key(g):
        (wv,bv,su, l) = QUOTAS[g]
        cap = lib_capacity(g)
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


        elif l in ['CD-LTO3', 'CDF-LTO3', 'D0-LTO3']:
          su = float(su.split("G")[0])
          all_lto3_wv = all_lto3_wv + int(wv)
          all_lto3_bv = all_lto3_bv + int(bv)
          all_lto3_su = all_lto3_su + su
          if l == 'CD-LTO3' :
              temp_su=0.0
              try:
                  temp_su = float(su.split("G")[0])
              except:
                  temp_su = float(su)
              cd_lto3_wv = cd_lto3_wv + int(wv)
              cd_lto3_bv = cd_lto3_bv + int(bv)
              cd_lto3_su = cd_lto3_su + temp_su

        elif l in ['CD-LTO4', 'CDF-LTO4', 'D0-LTO4', 'CD-LTO4G1', 'CDF-LTO4G1', 'D0-LTO4G1' ]:
          su = float(su.split("G")[0])
          all_lto4_wv = all_lto4_wv + int(wv)
          all_lto4_bv = all_lto4_bv + int(bv)
          all_lto4_su = all_lto4_su + su


    elif g == "CD-9840":
        (wv1,bv1,su1,l) = QUOTAS.get('blank-9840.none',('-1','-1','-1','-1'))
        (wv2,bv2,su2,l) = QUOTAS.get('eagle.none:',('-1','-1','-1','-1'))
        #wv = string.atoi(wv1)+string.atoi(wv2)
        wv = len(eagle_v)
        bv = string.atoi(bv1)+string.atoi(bv2)
        #su = '0.0GB'
        su="%.2f%s"%(eagle_mb / 1024.,"GB")
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
    elif g == 'ALL_LTO3':
        pass
    elif g == 'ALL_LTO4':
        pass
    elif g == 'ALL_9940B':
        pass
    else:
        print 'What group is this',g
        (wv,bv,su) = ('?','?','?')
    if g in ['ALL_9940', 'ALL_9940B', 'CD-9940B', 'ALL_LTO3', 'ALL_LTO4', 'CD-LTO3']:
        pass
    else:
        sort_the_file('%s.tapes'%(g,))
        cmd = "$ENSTORE_DIR/sbin/tapes-plot-sg.py %s %s %s %s %s %s %s" % (g,d1,d2,wv,bv,su, cap)
        print cmd
        os.system(cmd)

sort_the_file('CD-LTO3.tapes')
cmd = "$ENSTORE_DIR/sbin/tapes-plot-sg.py %s %s %s %s %s %s %s" % ('CD-LTO3',d1,d2,cd_lto3_wv,cd_lto3_bv,cd_lto3_su, 400)
os.system(cmd)

sort_the_file('ALL_LTO3.tapes')
cmd = "$ENSTORE_DIR/sbin/tapes-plot-sg.py %s %s %s %s %s %s %s" % ('ALL_LTO3',d1,d2,all_lto3_wv,all_lto3_bv,all_lto3_su, 400)
print cmd
os.system(cmd)

sort_the_file('ALL_LTO4.tapes')
cmd = "$ENSTORE_DIR/sbin/tapes-plot-sg.py %s %s %s %s %s %s %s" % ('ALL_LTO4',d1,d2,all_lto4_wv,all_lto4_bv,all_lto4_su, 800)
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

sort_the_file('CD-9940B.tapes')
cmd = "$ENSTORE_DIR/sbin/tapes-plot-sg.py %s %s %s %s %s %s %s" % ('CD-9940B',d1,d2,cd_9940b_wv,cd_9940b_bv,cd_9940b_su, 200)
print cmd
os.system(cmd)


cmd = '$ENSTORE_DIR/sbin/enrcp *.ps *.jpg %s:%s/burn-rate'%(this_web_host,html_dir)
print cmd
os.system(cmd)
