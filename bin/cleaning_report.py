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
        try:
            vol,count = string.split(line)[0:2]
        except:
            print line
            continue
        info = os.popen('enstore vol --vol %s'%(vol,),'r').readlines()
        library='unknown'
        for iline in info:
            if string.find(iline,'library')>0:
                l=string.split(iline,":")[1]
                library =string.split(l,"'")[1]
            elif string.find(iline,'remaining_bytes')>0:
                n=string.split(iline,":")[1]
                count=int(string.split(n,"L")[0])
                remaining[vol+'/'+library]=count
                break

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

total = {}
total_count = {}
for vol in good:
    v,library = string.split(vol,'/')
    if total.has_key(library):
        total[library] = total[library] + remaining[vol]
        total_count[library] = total_count[library] + 1
    else:
        total[library] = remaining[vol]
        total_count[library] = 1

for lib in total.keys():
    print "There are %s %s cleaning tapes left, with a total capacity of %s cleanings" % (
    total_count[lib], lib, total[lib])

