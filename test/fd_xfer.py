#! /usr/bin/env python
#   This file (fd_xfer.py) was created by Ron Rechenmacher <ron@fnal.gov> on
#   May 14, 1999. "TERMS AND CONDITIONS" governing this file are in the README
#   or COPYING file. If you do not have such a file, one can be obtained by
#   contacting Ron or Fermi Lab in Batavia IL, 60510, phone: 630-840-3000.
#   $RCSfile$
#   $Revision$
#   $Date$


# usage: fd_xfer.py src_spec dst_spec eod

import sys				# sys.argv[1:]
import driver
import os				# os.stat
import stat				# stat.ST_SIZE
import time				# time.time (xfer rate)
import pprint				# for stats
import FTT				# to get stats directly
import string				# support for the pprint mission

def tt():
    return time.strftime("%a %b %d %H:%M:%S %Z %Y",time.localtime(time.time()))

infile     = sys.argv[1]
tapedev    = sys.argv[2]
eod_cookie = sys.argv[3]

stdout_sav = sys.stdout
sys.stdout = sys.stderr

print "%s, %s, %s, %s"%(sys.argv[0],infile,tapedev,eod_cookie)

external_label = "test"

# first check tapedev (just to make sure it exists) we will get an exception
# if it does not (ftt coredumps with no-existant files).
statinfo = os.stat( tapedev )

statinfo = os.stat( infile )


bytes = statinfo[stat.ST_SIZE]

fo = open( infile, 'r' )

hsm_driver = driver.FTTDriver()
#hsm_driver = driver.NullDriver()

print "%s:         sw_mount... "%tt(),          #; sys.stdout.flush()
hsm_driver.sw_mount( tapedev, 102400, 30520749568L, external_label, eod_cookie )


#######################################################
do = hsm_driver.open( tapedev, 'a+' )
if do.is_bot(do.tell()) and do.is_bot(eod_cookie):
    # write an ANSI label and update the eod_cookie
    print '\n%s:         label...'%tt(),
    ll = do.format_label( external_label )
    do.write( ll )
    do.writefm()
    eod_cookie = do.tell()
    remaining_bytes = do.get_stats()['remaining_bytes']
    pass
do.close()
#######################################################


do = hsm_driver.open( tapedev, 'a+' )


#################################################
print '\n%s:         seek to %s... '%(tt(),eod_cookie),
do.seek( eod_cookie )
##################################


print '\n%s:         do.fd_xfer( fo.fileno(), %s, 0,0 )'%(tt(),bytes),
t0 = time.time()
crc = do.fd_xfer( fo.fileno(), bytes, 0,0 )
t1 = time.time()


#######################################
print '\n%s:         writefm... '%tt(),
do.writefm()
eod_cookie = do.tell()
print '\n%s:         get_stats... '%tt(),
if isinstance( hsm_driver, driver.FTTDriver ): stats = FTT.get_stats()
else:                                          stats = do.get_stats()
xx = tt()
for ll in string.split( pprint.pformat(stats), '\012' ): print '\n%s:         %s'%(xx,ll),

##########################################

do.close()			# b/c of fm above, this is purely sw.

fo.close()

print '\n%s:         crc is %s  %d bytes in %s seconds (%s bytes/sec)'%(tt(),
									crc,
									bytes,
									t1-t0,
									bytes/(t1-t0))

sys.stdout = stdout_sav
print eod_cookie
