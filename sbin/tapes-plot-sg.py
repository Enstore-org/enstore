#!/usr/bin/env python

import os
import string
import sys
import time

# the next line causes the program to traceback and exit if it is not called properly
thefile = sys.argv[1]
suffix = string.find(thefile,'.tapes')
if suffix > 0:
    thefile = thefile[0:suffix]

try:
    day1 = sys.argv[2]
    try:
        day2 = sys.argv[3]
    except:
        day2 = None
except:
    day1 = None
    day2 = None

f = open(thefile+".tapes","r")
o = open(thefile+".data", "w")

when = None
start = 0
tot = 0
gigs = 0

while 1:
    line = f.readline()
    if not line: break
    (d,t,mb) = line.split()
    mb = string.atoi(mb)
    gb = mb/1024.

    if d != when:
        if start == 0:
            start=1
        else:
            o.write("%s %f %f\n" % (when,gigs ,tot))
        when = d
        gigs = 0
    gigs = gigs + gb
    tot =  tot  + gb
o.write("%s %f %f\n" % (when,gigs ,tot))
f.close()
o.close()

g = open(thefile+".gnuplot", "w")
g.write('set terminal postscript color solid\n')
g.write('set output "%s.ps"\n' % (thefile,))
g.write('set title "%s"\n' % (thefile,))
g.write('set ylabel "Gigabytes Written"\n')
g.write('set xdata time\n')
g.write('set timefmt "%d-%b-%y"\n')
g.write('set format x "%m-%d-%y"\n')
g.write('set grid\n')
g.write('set nokey\n')
if day1 != None and day2 != None:
    g.write('set xrange["%s":"%s"]\n' % (day1,day2))
elif day1 != None:
    g.write('set xrange["%s":]\n' % (day1,))
#g.write('plot "%s.data" using 1:2 with impulses linewidth 10 \n' % (thefile))
#g.write('plot "%s.data" using 1:2 with impulses linewidth 10, "%s.data" using 1:3 with steps\n' % (thefile,thefile))
g.write('set log y\n')
#g.write('replot\n')
g.write('plot "%s.data" using 1:2 with impulses linewidth 10, "%s.data" using 1:3 with steps\n' % (thefile,thefile))
g.close()

#           'gv %s.ps' % (thefile,),
#           'convert -rotate 90 -geometry 120x120 -modulate -20 %s.ps %s_stamp.jpg' % (thefile,thefile)
for cmd in 'gnuplot %s.gnuplot' % (thefile,),\
           'convert -rotate 90 %s.ps %s.jpg' % (thefile,thefile),\
           'convert -rotate 90 -geometry 120x120  %s.ps %s_stamp.jpg' % (thefile,thefile):
    print cmd
    os.system(cmd)
