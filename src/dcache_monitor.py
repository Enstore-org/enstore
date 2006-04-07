#!/usr/bin/env python

###############################################################################
#
# $Id$ 0000 0000 0000 0000 00010000
#
###############################################################################

###############################################################################
# 
# This script monitors files in dcache
#
###############################################################################


# system imports
import sys
import string
# import time
import errno
import socket
# import select
import pprint
# import rexec
import stat

# enstore imports
#import setpath
import hostaddr
import option
import generic_client
import backup_client
#import udp_client
import Trace
import e_errors
# import cPickle
import info_client
import enstore_constants
import pg
import string
import time
import pnfs
import os
import re
import thread
import popen2


exitmutexes=[]

def check_layer_1(l):
    if (len(l) == 0 ) : return False
    bfid=l[0].strip()
    if  len(bfid) < 8 :
        return False
    return True

def check_layer_2(l):
    if (len(l) == 0 ) : return False
    size_match = re.compile("l=[0-9]+")
    line2 = l[1].strip()
    size = long(size_match.search(line2).group().split("=")[1])
    if ( size == 0L ) :
        return False
    return True

def check_layer_4(l):
    if (len(l) == 0 ) : return False
    return True
    
def check_volatile_files(db_name):
    #
    # extract entries from volatile files
    #
    db = pg.DB(db_name);
    sql_txt = "select pnfsid_string from volatile_files order by date"
    res=db.query(sql_txt)
    pnfsids = []
    for row in res.getresult():
        if not row:
            continue
        pnfsid_string=row[0]


#        f=ope[n(os.path.join("/pnfs/fs/usr/%s"%(db_name,), ".(showid)(%s)"%(pnfsid_string,)));
#        is_file=0
#        for line in f.readlines():
#            data = string.split(line[:-1],":")
#            if ( is_file == 0 and data[0].strip(" ") == "Type" and data[1].strip(" ") == "--I---r----" ) :
#                is_file=1
#        f.close()
#        if ( is_file == 1 ) :
#                
        pnfsids.append(pnfsid_string)

    for pnfsid in pnfsids:
        try:
            p=pnfs.Pnfs(pnfsid,"/pnfs/fs/usr/%s"%(db_name,));
            l1=p.readlayer(1)
            l2=p.readlayer(2)
            l4=p.readlayer(4)
            if (check_layer_1(l1) == True and check_layer_4(l4) == True ) :
                sql_txt = "delete from volatile_files where pnfsid_string='%s'"%(pnfsid,)
                r=db.query(sql_txt)
            else:
                l1_str="y";
                l2_str="y";
                l4_str="y";
                if (check_layer_1(l1) == False ) :
                    l1_str="n"
                if (check_layer_2(l2) == False ) :
                    l2_str="n"
                if (check_layer_4(l4) == False ) :
                    l4_str="n"
                sql_txt = "update volatile_files set layer1='%s',layer2='%s',layer4='%s' where pnfsid_string='%s'"%(l1_str,l2_str,l4_str,pnfsid)
                r=db.query(sql_txt)
        except (OSError, IOError, AttributeError, ValueError):
            sql_txt = "delete from volatile_files where pnfsid_string='%s'"%(pnfsid,)
            r=db.query(sql_txt)
    db.close()

def insert_into_volatile_files(db_name):
    db = pg.DB(db_name);
    #
    # establish time boundaries
    #
    now_time       = time.time()
    start_time     = now_time-3600*25 # one hour is for safety
    str_now_time   = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(now_time))
    str_start_time = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(start_time))
    
    sql_txt = "select to_char(date,'YYYY-MM-DD HH24:MI:SS'),encode(pnfsid,'hex'),pnfsid from pnfs "+\
              " where "+\
              " (date>'%s' "%(str_start_time,)+\
              " and date<'%s') "%(str_now_time)+\
              " and pnfsid not in (select pnfsid from volatile_files where date>'%s' and date<'%s') "%(str_start_time,str_now_time,)+\
              "  order by date "
    res=db.query(sql_txt)
    for row in res.getresult():
        if not row:
            continue
        d = row[0];
        p = str(row[1]);
        pnfsid_string=""
        for i in [0,4,8,12]:
            l=i+2
            h=l+2
            l1=i
            h1=l1+2
            pnfsid_string=pnfsid_string+p[l:h]+p[l1:h1]
        for i in [20,16]:
            l=i+2
            h=l+2
            l1=i
            h1=l1+2
            pnfsid_string=pnfsid_string+p[l:h]+p[l1:h1]
        pnfsid_string=string.upper(pnfsid_string)
        f=open(os.path.join("/pnfs/fs/usr/%s"%(db_name,), ".(showid)(%s)"%(pnfsid_string,)));
        is_file=0
        dbnum=0
        for line in f.readlines():
            data = string.split(line[:-1],":")
            if ( is_file == 0 and data[0].strip(" ") == "Type" and data[1].strip(" ") == "--I---r----" ) :
                is_file=1
        f.close()
        if ( is_file == 1 ) :
            try: 
                p=pnfs.Pnfs(pnfsid_string,"/pnfs/fs/usr/%s"%(db_name,));
                #print pnfsid_string,p.file_size,p.pnfsFilename, p.filename, p.directory
                l1=p.readlayer(1)
                l2=p.readlayer(2)
                l4=p.readlayer(4)
                if (check_layer_1(l1) == False or check_layer_4(l4) == False ) :
                    l1_str="y";
                    l2_str="y";
                    l4_str="y";
                    if (check_layer_1(l1) == False ) :
                        l1_str="n"
                    if (check_layer_2(l2) == False ) :
                        l2_str="n"
                    if (check_layer_4(l4) == False ) :
                        l4_str="n"
                    
                    insert_query_txt="insert into volatile_files (date,unix_date,pnfsid_string,pnfsid,pnfs_path,layer1,layer2,layer4) "+\
                                      "values ('"+str(row[0])+"',"+\
                                      str(int(time.mktime(time.strptime(row[0],'%Y-%m-%d %H:%M:%S'))))+","+\
                                      "'"+pnfsid_string+"',"+\
                                      "decode('"+row[1]+"','hex')"+\
                                      ",'"+p.pnfsFilename+"','"+l1_str+"','"+l2_str+"','"+l4_str+"')"
                    r=db.query(insert_query_txt)
            except (OSError, IOError, AttributeError, ValueError):
                continue
    db.close()

def prepare_html(db_name):
    fname="%s.txt"%(db_name,)
    sql_txt = "select date, pnfsid_string, layer1, layer2, layer4, pnfs_path from volatile_files order by date asc"
    cmd = "psql  %s  -o %s -c \"%s;\""%(db_name,fname,sql_txt)
    os.system(cmd)
    cmd = "source /home/enstore/gettkt; $ENSTORE_DIR/sbin/enrcp %s  stkensrv2.fnal.gov:/diska/www_pages/dcache_monitor/"%(fname,)
    os.system(cmd)

def do_work(i,db_name) :
    check_volatile_files(db_name)
    insert_into_volatile_files(db_name)
    prepare_html(db_name)
    exitmutexes[i]=1

if __name__ == '__main__':
    i=0
    cmd="mdb status | awk '{print $2}' | egrep -v 'Name|admin|NULL'"
    inp,out = os.popen2 (cmd, 'r')
    inp.write (cmd)
    inp.close ()
    dbs=[]
    for line in out.readlines() :
        if line.isspace():
            continue
        dbs.append(line[:-1])
    out.close()

    for db_name in dbs:
        thread.start_new(do_work, (i,db_name))
        exitmutexes.append(0)
        i=i+1
    while 0 in exitmutexes: pass


#    for db_name in ['eagle', 'exp-db']:


