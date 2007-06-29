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
import getopt
import histogram

def usage(cmd):
    print "Usage: %s -s [--sleep=] "%(cmd,)
    print "\t --sleep : sampling interval in seconds"

    
def do_work(db_name,username):
    #
    # extract entries from volatile files
    #
    db = pg.DB(db_name,user=username)
    sql_txt = "select xact_commit from pg_stat_database"
    res=db.query(sql_txt)

    sn=0
    inter=interval

    for row in res.getresult():
        if not row:
            continue
        sn=sn+row[0]
    db.close()
    return sn


if __name__ == '__main__':
    
    interval=10
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hs:ss:", ["help","sleep="])
    except getopt.GetoptError:
        print "Failed to process arguments"
        usage(sys.argv[0])
        sys.exit(2)
    for o, a in opts:
        if o in ("-h", "--help"):
            usage(sys.argv[0])
            sys.exit(1)
        if o in ("-s", "--sleep"):
            interval = a

    so=0
    sn=0
    now_time    = time.time()

    ntuple = histogram.Ntuple("transactions","transactions")
    ntuple.set_time_axis()
    try:
        while 1:
            so=sn
            sn=do_work("template1","enstore")
            sd=(sn-so+interval/2)/interval
            if so != 0:
                print time.ctime(), sd," trans/sec"
                ntuple.get_data_file().write("%s %f\n"%(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime()),sd))
            time.sleep(interval)
    except (KeyboardInterrupt, SystemExit):
        ntuple.get_data_file().close()
        ntuple.set_line_color(2)
        ntuple.set_line_width(5)
        ntuple.set_marker_type("points pt 1 ps 10 ")        
        ntuple.plot("1:3")
        sys.exit(0)
            
        


