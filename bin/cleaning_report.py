#!/usr/bin/env python

# $Id$

import os
import string

cleaned = []
for line in open(string.strip(os.popen(
    'enstore log --get-last-logfile-name', 'r').read()),'r').readlines():
    
    if string.find(line, "automatic cleaning") > 0:
        cleaned.append(string.split(line)[5])

cleaned.sort()

remaining = {}

for line in os.popen('enstore vol --vols','r').readlines():
    if string.find(line, "CleanTape")>0:
        vol, count = string.split(line)[:2]
        info = os.popen('enstore vol --vol %s'%(vol,),'r').readlines()
        for iline in info:
            if string.find(iline,'remaining_bytes')>0:
                n=string.split(iline,":")[1]
                count=int(string.split(n,"L")[0])
                remaining[vol]=count

vols = remaining.keys()
vols.sort()

used_up = []
good = []
for vol in vols:
    if not remaining[vol]:
        used_up.append(vol)
    else:
        good.append(vol)

print "The following drives were cleaned yesterday:"
if cleaned:
    print '  ',string.join(cleaned, ' ')
else:
    print '  ', '<no drives>'
print

print "The following cleaning tapes are used up:"
if used_up:
    print '  ', string.join(used_up, ' ')
else:
    print '  ', '<no tapes>'
print

total = 0
for vol in good:
    total = total + remaining[vol]

print "There are %s cleaning tapes left, with a total capacity of %s cleanings" % (
    len(good), total)

