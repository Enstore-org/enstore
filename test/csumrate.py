#!/usr/bin/env python

import time
import setpath
import checksum

s = 128*1024*' '
l = len(s)
crc = 0L
t0 = time.time()
bytes = 0L
for count in xrange(10000):
    crc = checksum.adler32_o(crc, s, 0, l)
    bytes = bytes + l

now = time.time()
elapsed = now - t0

print "CRC=%s  checksummed %s bytes in %.02g seconds, rate = %.02g" % (crc, bytes, elapsed,
                                                                       bytes/elapsed)
