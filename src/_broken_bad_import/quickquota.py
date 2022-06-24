#!/usr/bin/env python

import popen2
import string
import sys
import time
import volume_clerk_client

#icky routiune to gget the storage group. this is the
#first of a triple of dot-delimited fields,
#except for a kluges to get the converted V1 data bases.
def get_storage_info(vol_family):
    # storage_group, ff, wrapper_type = string.split(vol_family, ".")
    ss = string.split(vol_family, ".")
    storage_group = ss[0]
    ff = ss[1]
    if len(ss) > 2:
        wrapper_type = ss[2]
    else:
        wrapper_type = 'none'

    # here we kludge up the V1 stuff.
    if storage_group == "EnsV1" :
        if ff[0:6] == "theory": storage_group = "theory"
        if ff[0:4] == "opdb"  : storage_group = "sdss"
    return storage_group, ff

detailed = 0
if len(sys.argv)>2:
    if sys.argv[1] in ('detailed', 'detail','d'):
        detailed = 1
        detailed_output=sys.argv[2]

r, w = popen2.popen2 ("enstore info --vols")

total_vols_for_entity={}
unused_vols_for_entity={}

w.close()
data = r.read()
r.close()
lines=string.split(data,"\n")
lines = lines[1:]  #strip readable header
for line in lines:
    try:
        if len(line):
            vv = volume_clerk_client.extract_volume(line)
            volser = vv['label']
            cap = vv['avail']
            lib = vv['library']
            vol_family = vv['volume_family']

            # volser, cap, x1, x2, x3, x3, lib, vol_family =  string.split(line)
            storage_group, ff  = get_storage_info(vol_family)
            total_vols_for_entity[storage_group] = total_vols_for_entity.get(storage_group,0) +1
            if ff == 'none' :
                unused_vols_for_entity[storage_group] = unused_vols_for_entity.get( storage_group,0) + 1

    except ValueError:
        print "ERROR parsing", line
        continue

total_vols=0
total_unused_vols=0
now= time.asctime(time.localtime(time.time()))
print "Enstore Volume Usage Report: %s" % now
print "%-15s %5s %5s" %("StorageGroup","Nvol","Unused")
for k in total_vols_for_entity.keys():
    v = total_vols_for_entity[k]
    uv = unused_vols_for_entity.get(k, 0)
    total_vols = total_vols + v
    total_unused_vols = total_unused_vols + uv
    print "%-15s %5d %5d" % (k, v, uv)
    if detailed:
        dfile = open (detailed_output+'/'+k,'a')
        dfile.write("%s\t%s\t%d\t%d\n" % (now, k, v, uv))
        dfile.close()
print "Total volumes: %d total unused: %d" % (total_vols, total_unused_vols)


