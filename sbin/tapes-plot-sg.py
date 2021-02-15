#!/usr/bin/env python

from __future__ import print_function
import os
import string
import sys
import time


now = time.strftime("%c", time.localtime(time.time()))
# the next line causes the program to traceback and exit if it is not
# called properly
thefile = sys.argv[1]
suffix = string.find(thefile, '.tapes')
if suffix > 0:
    thefile = thefile[0:suffix]

# ok, this stinks, and I never ANY arguments at all & it kept growing!
try:
    day1 = sys.argv[2]
except BaseException:
    day1 = None
try:
    day2 = sys.argv[3]
except BaseException:
    day2 = None
try:
    wv = sys.argv[4]  # written volumes
except BaseException:
    wv = ""
try:
    bv = sys.argv[5]  # blank volumes
except BaseException:
    bv = ""
try:
    su = sys.argv[6]  # MB written
except BaseException:
    su = ""
try:
    capacity = int(sys.argv[7])  # capacity of the tape in GB
except BaseException:
    capacity = 200  # GB

f = open(thefile + ".tapes", "r")
o = open(thefile + ".data", "w")

when = t_when = None
start = 0
tot = 0
gigs = 0
tapes_written = {}

while True:
    line = f.readline()
    if not line:
        break
    (d, t, mb) = line.split()
    mb = int(mb)
    gb = mb / 1024.

    if d != when:
        if start == 0:
            start = 1
        else:
            o.write("%s %f %f\n" % (when, gigs, tot))
            tapes_written[t_when] = {
                'date': when,
                'tapes': gigs / capacity,
                'GB': gigs,
                'total': tot}
        when = d
        t_when = long(time.mktime(time.strptime(when, "%d-%b-%y")))
        gigs = 0
    gigs = gigs + gb
    tot = tot + gb
tapes_written[t_when] = {
    'date': when,
    'tapes': gigs / capacity,
    'GB': gigs,
    'total': tot}
o.write("%s %f %f\n" % (when, gigs, tot))
f.close()
o.close()


# what is tape usage per lats month?
# t=time.localtime(time.time())
#today = long(time.mktime((t[0],t[1],t[2],0,0,0,t[6],t[7],t[8])))
keys = sorted(tapes_written.keys())
last_time_written = keys[len(keys) - 1]
a_month_ago = last_time_written - 30 * 24 * 3600
a_week_ago = last_time_written - 7 * 24 * 3600
keys = tapes_written.keys()
if not a_month_ago in keys:
    for i in range(len(keys)):
        if keys[i] >= a_month_ago:
            a_month_ago = keys[i]
            break
if not a_week_ago in keys:
    for i in range(len(keys)):
        if keys[i] >= a_week_ago:
            a_week_ago = keys[i]
            break

tapes_written_last_month = 0
tot_m_ago = tapes_written[a_month_ago]['total']
tot_now = tapes_written[last_time_written]['total']
f1 = open(thefile + ".1.data", "w")
f1.write(
    "%s %f\n" %
    (tapes_written[a_month_ago]['date'],
     tapes_written[a_month_ago]['total']))
f1.write(
    "%s %f\n" %
    (tapes_written[last_time_written]['date'],
     tapes_written[last_time_written]['total']))
f1.close()
for key in tapes_written.keys():
    if key >= a_month_ago and key < last_time_written:
        tapes_written_last_month = tapes_written_last_month + \
            tapes_written[key]['tapes']

tapes_written_last_week = 0
for key in tapes_written.keys():
    if key >= a_week_ago and key < last_time_written:
        tapes_written_last_week = tapes_written_last_week + \
            tapes_written[key]['tapes']


g = open(thefile + ".gnuplot", "w")
g.write('set terminal postscript color solid\n')
g.write('set output "%s.ps"\n' % (thefile,))
g.write(
    'set title "%s   TotTapesUsed=%s (%s)   TapesBlank=%s" font "TimesRomanBold,16"\n' %
    (thefile, wv, su, bv))
g.write('set ylabel "Gigabytes Written"\n')
g.write('set xdata time\n')
g.write('set timefmt "%d-%b-%y"\n')
g.write('set format x "%m-%d-%y"\n')
g.write('set grid\n')
g.write('set nokey\n')
g.write(
    'set label "Plotted %s " at graph .99,0 rotate font "Helvetica,10"\n' %
    (now,))
g.write(
    'set label "%s tapes written last month" at graph .05,.90\n' %
    (int(tapes_written_last_month),))
g.write(
    'set label "%s tapes written last week" at graph .05,.85\n' %
    (int(tapes_written_last_week),))
if day1 is not None and day2 is not None:
    g.write('set xrange["%s":"%s"]\n' % (day1, day2))
elif day1 is not None:
    g.write('set xrange["%s":]\n' % (day1,))
#g.write('plot "%s.data" using 1:2 with impulses linewidth 10 \n' % (thefile))
#g.write('plot "%s.data" using 1:2 with impulses linewidth 10, "%s.data" using 1:3 with steps\n' % (thefile,thefile))
#g.write('set log y\n')
# g.write('replot\n')
g.write(
    'plot "%s.data" using 1:3 with impulses linewidth 10, "%s.data" using 1:2 with impulses linewidth 10, "%s.data" using 1:3 with lines, "%s.1.data" using 1:2 w lp lt 3 lw 5 pt 5\n' %
    (thefile,
     thefile,
     thefile,
     thefile))
g.close()

#           'gv %s.ps' % (thefile,),
#           'convert -flatten -background lightgray -rotate 90 -geometry 120x120 -modulate -20 %s.ps %s_stamp.jpg' % (thefile,thefile)
for cmd in '/usr/bin/gnuplot %s.gnuplot' % (thefile,),\
           '/usr/X11R6/bin/convert -flatten -background lightgray -rotate 90 -modulate 80 %s.ps %s.jpg' % (thefile, thefile),\
           '/usr/X11R6/bin/convert -flatten -background lightgray -rotate 90 -geometry 120x120 -modulate 80 %s.ps %s_stamp.jpg' % (thefile, thefile):
    print(cmd)
    os.system(cmd)
