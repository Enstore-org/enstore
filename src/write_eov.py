#!/usr/bin/env python

# $Id#

"""Script to write EOV1 headers on a tape.  Arguments:  volume_label tape_device [eod_cookie]
      If eod_cookie is not supplied it is gotten from the volume database"""


import sys
import os
import string

if len(sys.argv) not in (3,4):
    print "Usage: %s volume_label tape_device [eod_cookie]" % (sys.argv[0],)
    sys.exit(-1)

vol = sys.argv[1]
dev = sys.argv[2]

if len(sys.argv)==4:
    eod=sys.argv[3]
else:
    eod=None

def strip_quotes(s):
    if s and s[0] in '"\'':
        s = s[1:]
    if s and s[-1] in '"\'':
        s = s[:-1]
    return s

if eod is None:
    p = os.popen("enstore volume --vol=%s" % (vol,))
    lines = p.readlines()
    for line in lines:
        words = string.split(line,':')
        if len(words)==2:
            key, val = words
            key = string.strip(key)
            val = string.strip(val)
            if val[-1]==",":
                val = val[:-1]
            if strip_quotes(key)=='eod_cookie':
                eod = strip_quotes(val)
                break
    else:
        print "no EOD cookie for volume", vol
        sys.exit(-1)

        
print "Writing eod cookie %s on volume %s using device %s" % (eod,vol,dev)

if eod == "none":
   os.system("mt -f %s rewind" % (dev,))
   fd=os.open(dev,1)
   hdr = "VOL1"+vol
   hdr = hdr+ (79-len(hdr))*' ' + '0'
   os.write(fd,hdr)
   os.close(fd)
   file=1
else:
    part, block, file = string.split(eod,'_')
    file = int(file)
os.system("mt -f %s rewind" % (dev,))
os.system("mt -f %s fsf %d" % (dev, file))
#os.system("mt -f %s weof 1" % (dev,))
fd=os.open(dev,1)
hdr = "EOV1"+vol+" "+eod
hdr = hdr+ (79-len(hdr))*' ' + '0'
os.write(fd,hdr)
os.close(fd)


        
        
        



