#!/usr/bin/env python
import exceptions
import sys
import os
import time
import e_errors 
import ftt

KB=1024L
MB=KB*KB
GB=MB*KB

BLOCK_SIZE = 128*KB
class Error(exceptions.Exception):
    def __init__(self, arg):
        exceptions.Exception.__init__(self,arg)


import ftt_driver
device=sys.argv[1]
p=os.popen("mt -f %s rewind 2>&1" % (device),'r')
r=p.read()
s=p.close()
ftt = __import__("ftt")
tape_driver = ftt_driver.FTTDriver()
rc = tape_driver.open(device, 0, retry_count=3)
print "open returned", rc
tape_driver.set_mode(compression = 0, blocksize = 0)
rc  = tape_driver.ftt.get_mode()
print "get_mode returned", rc
stats = tape_driver.get_stats()
print "BOT", stats[ftt.BOT]
print "AEOT", stats[ftt.AEOT]
print "PEOT", stats[ftt.PEOT]
print "DENSITY", hex(int(stats[ftt.DENSITY]))
print "MEDIA TYPE", stats[ftt.MEDIA_TYPE]
try:
    rc = tape_driver.verify_label(None)
except Exception as e:
    print "Exception:", e
print "verify_label returned", rc
stats = tape_driver.get_stats()
print "BOT", stats[ftt.BOT]
print "AEOT", stats[ftt.AEOT]
print "PEOT", stats[ftt.PEOT]
print "DENSITY", hex(int(stats[ftt.DENSITY]))
print "MEDIA TYPE", stats[ftt.MEDIA_TYPE]


tape_driver.close()

