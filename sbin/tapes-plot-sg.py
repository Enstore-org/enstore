#!/usr/bin/env python

import os
import string
import sys
import time

now = time.strftime("%c",time.localtime(time.time()))
# the next line causes the program to traceback and exit if it is not called properly
thefile = sys.argv[1]
suffix = string.find(thefile,'.tapes')
if suffix > 0:
    thefile = thefile[0:suffix]

# ok, this stinks, and I never ANY arguments at all & it kept growing!
try:
    day1 = sys.argv[2]
    try:
        day2 = sys.argv[3]
        try:
            wv = sys.argv[4]
            try:
                bv = sys.argv[5]
                try:
                    su = sys.argv[6]
                except:
                    su = ""
            except:
                bv = ""
                su = ""
        except:
            wv = ""
            bv = ""
            su = ""
    except:
        day2 = None
        wv = ""
        bv = ""
        su = ""
except:
    day1 = None
    day2 = None
    wv = ""
    bv = ""
    su = ""

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
g.write('set title "%s   TotTapesUsed=%s (%s)   TapesBlank=%s  "\n' % (thefile,wv,su,bv))
g.write('set ylabel "Gigabytes Written"\n')
g.write('set xdata time\n')
g.write('set timefmt "%d-%b-%y"\n')
g.write('set format x "%m-%d-%y"\n')
g.write('set grid\n')
g.write('set nokey\n')
g.write('set label "Plotted %s " at graph .99,0 rotate font "Helvetica,10"\n' % (now,))
if day1 != None and day2 != None:
    g.write('set xrange["%s":"%s"]\n' % (day1,day2))
elif day1 != None:
    g.write('set xrange["%s":]\n' % (day1,))
#g.write('plot "%s.data" using 1:2 with impulses linewidth 10 \n' % (thefile))
#g.write('plot "%s.data" using 1:2 with impulses linewidth 10, "%s.data" using 1:3 with steps\n' % (thefile,thefile))
#g.write('set log y\n')
#g.write('replot\n')
g.write('plot "%s.data" using 1:3 with impulses linewidth 10, "%s.data" using 1:2 with impulses linewidth 10, "%s.data" using 1:3 with lines\n' % (thefile,thefile,thefile))
g.close()

#           'gv %s.ps' % (thefile,),
#           'convert -rotate 90 -geometry 120x120 -modulate -20 %s.ps %s_stamp.jpg' % (thefile,thefile)
for cmd in '/usr/bin/gnuplot %s.gnuplot' % (thefile,),\
           '/usr/X11R6/bin/convert -rotate 90 -modulate 80 %s.ps %s.jpg' % (thefile,thefile),\
           '/usr/X11R6/bin/convert -rotate 90 -geometry 120x120 -modulate 80 %s.ps %s_stamp.jpg' % (thefile,thefile):
    print cmd
    os.system(cmd)
