#!/usr/bin/env python

import sys,os
import select
import time
import random

import setpath
import ftt

KB=1024
MB=KB*KB
GB=KB*MB


device = sys.argv[1]

f = ftt.open(device, ftt.RDWR)
mode = f.get_mode()
print mode
print f.set_mode(mode[1], 0 , 0)
print f.get_mode()
print 'rewind'
print f.rewind()

fd = f.open_dev()

print "open dev", fd

verbose = 0
doplot = 0


size=100*KB


data = []
for i in xrange(10):
    if verbose: print i,
    data.append(open('/dev/urandom', 'r').read(size))
    
if verbose: print "prepared random data"

def ready():
    r,w,x = select.select([], [fd], [], 0)
    return w

def fsleep(fsecs):
    return select.select([], [], [], fsecs)

histo={}

starttime = time.time()
bytes_written = 0L

for x in xrange(5000):
    now = time.time()
    w = f.write(random.choice(data), size)
    bytes_written = bytes_written + w
    elapsed = time.time() - now
    print elapsed
##    rate = w/elapsed/MB
##    bucket = int(rate)
##    histo[bucket] = histo.get(bucket, 0) + 1
##    if verbose: print "%.3g MB/sec" % (rate),",",
##    if verbose: print "overall: %.3g MB/sec" % (bytes_written/(time.time()-starttime)/MB)
##    if rate < 10:
##        if verbose: print "sleep"
##        fsleep(0.50)
##    if ready():
##        if verbose: print 'ready for more'
##    else:
##        if verbose: print 'not ready'
        
    
if verbose: print "rate: %.3g MB/sec" % (bytes_written/(time.time()-starttime)/MB)
if verbose: print "closing device"
f.close()
if verbose: print "overall rate: %.3g MB/sec" % (bytes_written/(time.time()-starttime)/MB)

if doplot:

    keys = histo.keys()
    keys.sort()

    dfile=open('/tmp/rate_hist.plot', 'w')

    for x in range(keys[0], keys[-1]+1):
        dfile.write('%s %s\n' % (x, histo.get(x,0)))

    #dfile.close()
    #p=os.popen('gnuplot', 'w')
    #p.write('plot "/tmp/rate_hist.plot" with candlesticks\n')
    #p.close()




