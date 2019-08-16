#!/usr/bin/env python
import exceptions
import sys
import os
import time

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

tape_driver.writefm()
tape_driver.writefm()
tape_driver.close()

