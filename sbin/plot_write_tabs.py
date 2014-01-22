#!/usr/bin/env python
# $Id$
import sys
import popen2
import os
import string
import time

def generate_date():
    d1 = None
    for when in 'date --date "1 month ago"  +"%b-%d-%y"','date +"%b-%d-%y"':
        d = os.popen(when,'r')
        dat=d.readlines()
        d.close()
        if d1 == None:
            d1 = dat[0][:-1]
            d1.upper()
        else:
            d2 = dat[0][:-1]
            d2.upper()
    print 'Generating burn-rate plots from', d1, ' to ',d2
    return d1, d2


def get_stats(host, port, start_date=None, finish_date=None):

    date_fmt = "'YY-MM-DD HH24:MI'"
    if start_date and finish_date:
        query_cmd='psql -h %s -p %s -o "write_tabs_%s.report" -c \
        "select to_char(date, %s),total, should, not_yet, done from write_protect_summary\
        where date(time) between date(%s%s%s) and date(%s%s%s) \
        and mb_user_write != 0 order by date desc;" enstore'
        pipeObj = popen2.Popen3(query_cmd%(host, port, host, date_fmt, "'", start_date, "'", "'", finish_date, "'"), 0, 0)
    else:
        query_cmd='psql -h %s -p %s -o "write_tabs_%s.report" -c \
        "select to_char(date, %s),total, should, not_yet, done from write_protect_summary\
        order by date desc;" enstore'
        pipeObj = popen2.Popen3(query_cmd%(host, port, host, date_fmt), 0, 0)


    if pipeObj is None:
        sys.exit(1)
    stat = pipeObj.wait()
    result = pipeObj.fromchild.readlines()  # result has returned string

def make_plot_file(host):
    f=open("write_tabs_%s.report"%(host,),'r')
    of=open("write_tabs_%s"%(host,),'w')
    l = f.readline()
    first = 1
    while 1:
        l = f.readline()
        if l:
            a = l.split('|')
            if len(a) == 5:
                if first:
                    s,n,d = int(a[2]), int(a[3]), int(a[4])
                    first = 0
                a[4] = str(int(a[4][:-1])+int(a[3]))
                lo = string.join(a)
                of.write("%s\n"%(lo,))
        else:
            break
    return s,n,d

def make_plot(host, should, not_done, done):
   t = time.ctime(time.time())
   f = open("write_tabs_%s.gnuplot"%(host,),'w')
   f.write('set terminal postscript color solid\n')
   f.write('set output "write_tabs_%s.ps"\n' % (host,))
   f.write('set title "Write Tabs States for %s."\n'%(host,))
   f.write('set xlabel "Date (year-month-day)"\n')
   f.write('set timefmt "%Y-%m-%d"\n')
   f.write('set xdata time\n')
   #f.write('set size 1.5,1\n')
   f.write('set xrange [ : ]\n')
   f.write('set grid\n')
   f.write('set yrange [0: ]\n')
   f.write('set format x "%y-%m-%d"\n')
   f.write('set ylabel "# tapes that should have write tabs ON"\n')
   f.write('set label "Plotted %s " at graph .99,0 rotate font "Helvetica,10"\n' % (t,))

   f.write('set label "Should %s, Done %s(%3.1f%%), Not Done %s." at graph .05,.90\n' % (should, done, done*100./should, not_done))

   #f.write('plot "write_tabs_%s" using 1:10 t "ON" w impulses lw %s 3 1 using 1:8 t "OFF" w impulses lw %s 1 1\n'%
   #        (host, 20, 20))
   f.write('plot "write_tabs_%s" using 1:6 t "ON" w impulses lw %s lt 2, "write_tabs_%s" using 1:5 t "OFF" w impulses lw %s lt 1\n'%
           (host, 20, host, 20))
   #f.write('plot "write_tabs_%s" using 1:10 t "ON" w impulses lw %s 3 1\n'%(host, 20))

   f.close()
   for cmd in '/usr/bin/gnuplot write_tabs_%s.gnuplot' % (host,),\
           '/usr/X11R6/bin/convert -rotate 90 -modulate 80 write_tabs_%s.ps write_tabs_%s.jpg' % (host,host),\
           '/usr/X11R6/bin/convert -rotate 90 -geometry 120x120 -modulate 80 write_tabs_%s.ps write_tabs_%s_stamp.jpg' % (host,host):
    os.system(cmd)
    cmd = '$ENSTORE_DIR/sbin/enrcp *.ps *.jpg stkensrv2:/fnal/ups/prd/www_pages/enstore/write_tabs'
    os.system(cmd)


#d1, d2 =  generate_date()
for h in "stkensrv6", "d0ensrv6", "cdfensrv6":
    get_stats(h, 5432)
    should, not_done, done = make_plot_file(h)
    make_plot(h,should, not_done, done)
