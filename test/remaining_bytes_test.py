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
def write(tape_driver, f_size, block):
    tot_written = 0L
    for i in range(f_size/BLOCK_SIZE):
        try:
            bytes_written = tape_driver.write(block, 0, BLOCK_SIZE)
            if bytes_written == BLOCK_SIZE:
                tot_written = tot_written + bytes_written
            else:
                raise Error("bytes to write %s, bytes written %s total %s"%(BLOCK_SIZE, bytes_written, tot_written))
        except:
            exc, msg = sys.exc_info()[:2]
            print "Exception while doing write. Total written %s"%(tot_written,)
            raise exc, msg
            
    tape_driver.writefm()
    return tot_written
    

device="/dev/rmt/tps2d0n"
p=os.popen("mt -f %s rewind 2>&1" % (device),'r')
r=p.read()
s=p.close()
ftt = __import__("ftt")
tape_driver = ftt_driver.FTTDriver()
rc = tape_driver.open(device, 0, retry_count=3)
#print "open returned", rc

tape_driver.writefm()
tape_driver.writefm()
tape_driver.close()
p=os.popen("mt -f %s rewind 2>&1" % (device),'r')
r=p.read()
s=p.close()
tape_driver = ftt_driver.FTTDriver()
rc = tape_driver.open(device, 1, retry_count=3)
tape_driver.set_mode(compression = 0, blocksize = 0)
stats = tape_driver.get_stats()
#remain = (long(stats[ftt.REMAIN_TAPE])/1000) * MB
remain = long(stats[ftt.REMAIN_TAPE])*1024L
capacity = long(stats[ftt.BLOCK_TOTAL]) * 1024L
print "REMAINED", remain
print "CAPACITY", capacity
to_write=4*GB
block = "A"*BLOCK_SIZE
wr_cnt = 0L
start = time.time()
total=0.
while remain > to_write:
    try:
        written = write(tape_driver, to_write, block)
        total = total + written
    except:
        exc, msg, tb = sys.exc_info()
        print "Exception", exc, msg
        print "written", written
        break
    #time.sleep(2)
    wr_cnt = wr_cnt+1
    expected = remain - to_write
    stats = tape_driver.get_stats()
    #remain = (long(stats[ftt.REMAIN_TAPE])/1000) * MB
    remain = long(stats[ftt.REMAIN_TAPE]) * 1024L
    read_errors = long(stats[ftt.READ_ERRORS])
    write_errors = long(stats[ftt.WRITE_ERRORS])
    print "written %s write_count %s reported_remain %s expected_remain %s diff %s write_errors %s read_errors %s"%(written, wr_cnt, remain, expected, remain - expected, write_errors, read_errors)
stop = time.time()
print "total written %sMB in %s seconds average rate %s MB/s"%(total/MB, stop-start, total/MB/(stop-start)) 

