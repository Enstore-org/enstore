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


import sys
import string
import pg
import time
import pnfs
import os
import re
import pnfsidparser

import configuration_client

exitmutexes=[]

def check_layer_1(l):
    if (len(l) == 0 ) : return False
    bfid=l[0].strip()
    if  len(bfid) < 8 :
        return False
    return True

def check_layer_2(l):
    if (len(l) == 0 ) : return False
#    size_match = re.compile("l=[0-9]+")
#    line2 = l[1].strip()
#    size = long(size_match.search(line2).group().split("=")[1])
#    if ( size == 0L ) :
#        return False
    return True

def is_volatile(l):
    line2 = l[1].strip()
    try :
        h = string.split(string.split(line2,("="))[1],';')[0]
        if ( h == "yes" ) :
            return False
    except IndexError:
        return False
    return True


def check_layer_4(l):
    if (len(l) == 0 ) : return False
    return True

def check_volatile_files(db_name):
    #
    # extract entries from volatile files
    #
    db = pg.DB(db_name,user="enstore");
    sql_txt = "select pnfsid_string from volatile_files order by date"
    res=db.query(sql_txt)
    pnfsids = []
    for row in res.getresult():
        if not row:
            continue
        pnfsid_string=row[0]
        pnfsids.append(pnfsid_string)

    for pnfsid in pnfsids:
        try:
            p=pnfs.Pnfs(pnfsid,"/pnfs/fs/usr/%s"%(db_name,));
            l1=p.readlayer(1)
            l2=p.readlayer(2)
            l4=p.readlayer(4)
            if ( check_layer_2(l2) ) :
                if ( is_volatile(l2) ) :
                    sql_txt = "delete from volatile_files where pnfsid_string='%s'"%(pnfsid,)
                    r=db.query(sql_txt)
                    continue
            if (check_layer_1(l1) and check_layer_4(l4) ) :
                sql_txt = "delete from volatile_files where pnfsid_string='%s'"%(pnfsid,)
                r=db.query(sql_txt)
            else:
                l1_str="y";
                l2_str="y";
                l4_str="y";
                if not check_layer_1(l1) :
                    l1_str="n"
                if not check_layer_2(l2) :
                    l2_str="n"
                if not check_layer_4(l4) :
                    l4_str="n"
                sql_txt = "update volatile_files set layer1='%s',layer2='%s',layer4='%s',pnfs_path='%s' where pnfsid_string='%s'"%(l1_str,l2_str,l4_str,p.pnfsFilename,pnfsid,)
                r=db.query(sql_txt)
        except (OSError, IOError, AttributeError, ValueError):
            sql_txt = "delete from volatile_files where pnfsid_string='%s'"%(pnfsid,)
            r=db.query(sql_txt)
    db.close()

def insert_into_volatile_files(db_name):
    db = pg.DB(db_name,user="enstore");
    #
    # establish time boundaries
    #
    now_time       = time.time()-60*30
    start_time     = now_time-3600*25 # one hour is for safety
    if ( db_name == "minos" ) :
        start_time=now_time-3600*26
    str_now_time   = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(now_time))
    str_start_time = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(start_time))

    sql_txt = "select to_char(date,'YYYY-MM-DD HH24:MI:SS'),encode(pnfsid,'hex'),pnfsid from pnfs "+\
              " where "+\
              " (date>'%s' "%(str_start_time,)+\
              " and date<'%s') "%(str_now_time)+\
              " and pnfsid not in (select pnfsid from volatile_files) "+\
              "  order by date "
#              " and pnfsid not in (select pnfsid from volatile_files where date>'%s' and date<'%s') "%(str_start_time,str_now_time,)+\
    res=db.query(sql_txt)
    for row in res.getresult():
        if not row:
            continue
        d = row[0];
        p = str(row[1]);
        pnfsid_string=pnfsidparser.parse_id(p)
        is_file=0
        try:
            f=open(os.path.join("/pnfs/fs/usr/%s"%(db_name,), ".(showid)(%s)"%(pnfsid_string,)));
        except IOError:
            print 'cannot open', os.path.join("/pnfs/fs/usr/%s"%(db_name,), ".(showid)(%s)"%(pnfsid_string,))
        else:
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
                if not check_layer_1(l1)  or not check_layer_4(l4) :
                    l1_str="y";
                    l2_str="y";
                    l4_str="y";
                    if not check_layer_1(l1) :
                        l1_str="n"
                    if not check_layer_2(l2) :
                        l2_str="n"
                    if not check_layer_4(l4) :
                        l4_str="n"
                    if ( check_layer_2(l2) ) :
                        if ( is_volatile(l2) ) :
                            continue
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
    db = pg.DB(db_name,user="enstore");
    #
    # check for any bad files
    #
    now_time       = time.time()
    stmt="select count(*) from volatile_files where layer2='n' and unix_date<%d"%(int(now_time-3600))
    if ( db_name == "netflow" ) :
        stmt="select count(*) from volatile_files where layer2='n' and unix_date<%d"%(int(now_time-12*3600))
    res=db.query(stmt)
    count=0
    for row in res.getresult():
        if not row:
            continue
        count=int(row[0])
    if ( count != 0 ) :
        fname="%s_bad.txt"%(db_name,)
        sql_txt = "select date, pnfsid_string, layer1, layer2, layer4, pnfs_path from volatile_files where layer2='n' and unix_date<%d order by date asc"%(int(now_time-3600))
        os.system("rm -f %s"%fname);
        cmd = "psql  %s  -U enstore -o %s -c \"%s;\""%(db_name,fname,sql_txt)
        os.system(cmd)
        cmd = "su  enstore -c \'/usr/local/etc/setups.sh 1>>/dev/null 2>&1; cd /tmp; $ENSTORE_DIR/sbin/enrcp %s %s/\'"%(fname, destination)
        os.system(cmd)
    delta_time     = 25*3600
    if ( db_name == "minos" ) :
        delta_time = 26*3600
    # now_time       = time.time()
    res=db.query("select count(*) from volatile_files where layer2='y' and unix_date<%d"%(int(now_time-delta_time),))
    count1=0
    for row in res.getresult():
        if not row:
            continue
        count1=int(row[0])
    if ( count1!=0 ) :
        fname="%s.txt"%(db_name,)
        sql_txt = "select date, pnfsid_string, layer1, layer2, layer4, pnfs_path from volatile_files where layer2='y' and unix_date<%d  order by date asc"%(int(now_time-delta_time),)
        cmd = "psql  %s -U enstore -o %s -c \"%s;\""%(db_name,fname,sql_txt)
        os.system(cmd)
        cmd = "su  enstore -c \'/usr/local/etc/setups.sh 1>>/dev/null 2>&1; cd /tmp; $ENSTORE_DIR/sbin/enrcp %s %s/\'"%(fname, destination)
        os.system(cmd)
    db.close()
    if (count !=0 or count1 !=0):
        return True
    else:
        return False

def do_work(i,db_name) :
    rc=False
    try:
        check_volatile_files(db_name)
        print "checked volatile ",db_name
        try:
            insert_into_volatile_files(db_name)
        except:
            print "Excepted in the insert_volatile"
            print "Unexpected error:", sys.exc_info()[0]
        print "inserted into volatile ",db_name
        rc=prepare_html(db_name)
        print "prepared html  volatile ",db_name
    except (pg.ProgrammingError,OSError, IOError):
        pass
    exitmutexes[i]=1
    return rc

def do_mail(db_name) :
    db = pg.DB(db_name,user="enstore");
    #
    # check for any bad files
    #
    now_time       = int(time.time());
    then_time      = int(now_time-24*3600)

    stmt="select count(*) from volatile_files where layer2='n' and (unix_date<%d and unix_date>%d)"%(int(now_time-3600),then_time)
    if ( db_name == "netflow" ) :
        stmt="select count(*) from volatile_files where layer2='n' and (unix_date<%d and unix_date>%d)"%(int(now_time-12*3600),then_time)
    res=db.query(stmt)
    count=0
    for row in res.getresult():
        if not row:
            continue
        count=int(row[0])
    if ( count != 0 ) :
        fname="%s_bad.txt"%(db_name,)
        sql_txt = "select date, pnfsid_string, layer1, layer2, layer4, pnfs_path from volatile_files where layer2='n' and (unix_date<%d and unix_date>%d) order by date asc"%(int(now_time-3600),then_time)
        if ( db_name == "netflow" ) :
            sql_txt = "select date, pnfsid_string, layer1, layer2, layer4, pnfs_path from volatile_files where layer2='n' and (unix_date<%d and unix_date>%d) order by date asc"%(int(now_time-12*3600),then_time)
        os.system("rm -f %s"%fname);
        cmd = "psql  %s  -U enstore -o %s -c \"%s;\""%(db_name,fname,sql_txt)
        os.system(cmd)
        cmd = "su  enstore -c \'/usr/local/etc/setups.sh 1>>/dev/null 2>&1; cd /tmp; $ENSTORE_DIR/sbin/enrcp %s %s/\'"%(fname, destination)
        os.system(cmd)
    now_time=int(now_time-24*3600)
    then_time=int(now_time-24*3600)
    if (db_name=="minos") :
        now_time=int(now_time-26*3600)
        then_time=int(now_time-26*3600)

    res=db.query("select count(*) from volatile_files where layer2='y' and (unix_date<%d and unix_date>%d)"%(now_time,then_time))
    count1=0
    for row in res.getresult():
        if not row:
            continue
        count1=int(row[0])

    if ( count1!=0 ) :
        fname="%s.txt"%(db_name,)
        sql_txt = "select date, pnfsid_string, layer1, layer2, layer4, pnfs_path from volatile_files where layer2='y' and (unix_date<%d and unix_date>%d) order by date asc"%(now_time,then_time)
        cmd = "psql  %s -U enstore -o %s -c \"%s;\""%(db_name,fname,sql_txt)
        os.system(cmd)
        cmd = "su  enstore -c \'/usr/local/etc/setups.sh 1>>/dev/null 2>&1; cd /tmp; $ENSTORE_DIR/sbin/enrcp %s %s/\'"%(fname, destination)
        os.system(cmd)
    db.close()
    if (count !=0 or count1 !=0):
        return True
    else:
        return False

if __name__ == "__main__":   # pragma: no cover
    i=0
    cmd="mdb status | awk '{print $2}' | egrep -v 'Name|admin|NULL|test'"
    inp,out = os.popen2 (cmd, 'r')
    inp.write (cmd)
    inp.close ()
    dbs=[]
    for line in out.readlines() :
        if line.isspace():
            continue
        dbs.append(line[:-1])
    out.close()

    config = configuration_client.ConfigurationClient()
    crons_config = config.get('crons')
    destination_host = crons_config['web_node']
    destination_dir = os.path.join(crons_config['html_dir'], 'dcache_monitor')
    destination = ':'.join((destination_host, destination_dir))

    cmd = "su  enstore -c \'/usr/local/etc/setups.sh 1>>/dev/null 2>&1; cd /tmp; $ENSTORE_DIR/sbin/enrsh {} \"rm {}/*.txt\"\'".format(destination_host, destination_dir)
    os.system(cmd)
    yes_mail=False
#    for db_name in ['minos']:
    for db_name in dbs:
        exitmutexes.append(0)
        do_work(i,db_name)
#       thread.start_new(do_work, (i,db_name))
#       exitmutexes.append(0)
        i=i+1
#    while 0 in exitmutexes: pass

    os.system("rm -f *.txt")

    for db_name in dbs:
        rc = False
        try:
            rc = do_mail(db_name);
            if ( rc ) :
                yes_mail = True
        except:
            print "Failed in do_mail for database ",db_name
            pass

    if ( yes_mail ) :
        os.system("cat *.txt > mail.txt");
        os.system('mail dcache-auto@fnal.gov -s "There are files with missing layers older than 24 hours" < mail.txt')
        os.system("rm -f mail.txt")


#    for db_name in ['eagle', 'exp-db']:
