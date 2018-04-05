#!/usr/bin/env python
import sys
import ftt
import ftt_driver
KB=1024L
MB=KB*KB
GB=MB*KB

BLOCK_SIZE = 1*MB

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

    return tot_written

driver = ftt_driver.FTTDriver()
print "Openinig device. This may take a minute if tape is not mounted"
try:
    driver.open(sys.argv[1], mode=1, retry_count=3)
except Exception, detail:
    print "Exception", detail
stats = driver.get_stats()
print "Drive Type", stats[ftt.PRODUCT_ID]
ftt._ftt.ftt_set_last_operation(driver.ftt.d, 0)
block = "A"*BLOCK_SIZE
bytes_written = write(driver, BLOCK_SIZE, block)
print "WRITTEN", bytes_written
cdb_write_fm = [0x10,             # request sense command
                0x01,
                0x00,
                0x00,
                0x01,
                0x00
                ]
data = [0,0,0,0,0,0,0,0,0,0]
print "WRITING FM"
try:
    res = driver.ftt.do_write_scsi_command("write filemark",
                                           cdb_write_fm,
                                           6,
                                           data,
                                           10,
                                           10) # timeout
except Exception, detail:
    print "Exception", detail
finally:
    print "RES %0x"%res
    print 'data', data

