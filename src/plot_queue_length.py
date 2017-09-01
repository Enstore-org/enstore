#!/usr/bin/env python
import sys
import os
import time
import datetime

def usage(my_name):
    print "usage: %s <library manger> <start date> <stop date>"%(my_name,)
    print "\t enter data as YYYY-MM-DD"

def days(start_date, stop_date):
    c = start_date
    d = datetime.timedelta(days = 1)
    while c <= stop_date:
        yield c
        c += d

def get_data(day, lm):
    #cmd = "fgrep NAN /srv2/enstore/enstore-log/DEBUGLOG-%s | grep %s |grep pending_queue_length | awk '{print $1,$20}' | cut -c10- | awk -F 'L' '{print $1}' > tmp_q_l_%s"%(day, lm, day)
    cmd = "fgrep NAN /srv2/enstore/enstore-log/DEBUGLOG-%s | grep %s |grep pending_queue_length | awk '{print $1,$20}' > tmp_q_l_%s"%(day, lm, day)
    #print "CMD", cmd
    os.system(cmd)
    cmd = "sed 's/^/%s:/' tmp_q_l_%s | awk -F 'L' '{print $1}' >> %s"%(day, day, lm)
    #print "CMD", cmd
    os.system(cmd)

def plot(lm):
    cmd_file = open("gnuplod.cmd", "w")
    cmd = """
set title "{} queue length" \n
set xdata time \n
set style data lines \n
set terminal postscript color solid \n
set timefmt "%Y-%m-%d:%H:%M:%S" \n
set format x "%m-%d %H:%M" \n
set size 1.5,1 \n
set grid \n
set xlabel "Time" \n
set ylabel "Queue length" \n
set autoscale y \n
set output "{}.ps" \n
plot "{}" using 1:2 with impulses linewidth 1 \n
"""
    cmd = cmd.format(lm, lm, lm)
    cmd_file.write(cmd)
    cmd_file.close()
    os.system("gnuplot %s"%("gnuplod.cmd",))
    os.system("convert -flatten -rotate 90 %s.ps %s.jpj"%(lm, lm))
    
    
if len(sys.argv) != 4:
    usage(sys.argv[0])
    sys.exit(-1)
for r in days(datetime.date(int(sys.argv[2].split("-")[0]),
                            int(sys.argv[2].split("-")[1]),
                            int(sys.argv[2].split("-")[2])),
              datetime.date(int(sys.argv[3].split("-")[0]),
                            int(sys.argv[3].split("-")[1]),
                            int(sys.argv[3].split("-")[2]))
              ):
    get_data(r, sys.argv[1])
plot(sys.argv[1])



              
    
