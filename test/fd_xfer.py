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


infile     = sys.argv[1]
tapedev    = sys.argv[2]
eod_cookie = sys.argv[3]

os.system( "echo %s, %s, %s, %s >&2"%(sys.argv[0],infile,tapedev,eod_cookie) )

external_label = "test"

# first check tapedev (just to make sure it exists) we will get an exception
# if it does not (ftt coredumps with no-existant files).
statinfo = os.stat( tapedev )

statinfo = os.stat( infile )


bytes = statinfo[stat.ST_SIZE]

fo = open( infile, 'r' )

hsm_driver = driver.FTTDriver()

os.system( "echo -n sw_mount... >&2" ); sys.stderr.flush()
hsm_driver.sw_mount( tapedev, 102400, 30520749568L, external_label, eod_cookie )


#######################################################
do = hsm_driver.open( tapedev, 'a+' )
if do.is_bot(do.tell()) and do.is_bot(eod_cookie):
    # write an ANSI label and update the eod_cookie
    os.system( "echo >&2;echo -n label... >&2" ); sys.stderr.flush()
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
os.system( "echo >&2;echo -n seek to %s... >&2"%eod_cookie ); sys.stderr.flush()
do.seek( eod_cookie )
##################################


os.system( "echo >&2;echo -n 'do.fd_xfer( fo.fileno(), %s, 0,0 )' >&2"%bytes ); sys.stderr.flush()
t0 = time.time()
crc = do.fd_xfer( fo.fileno(), bytes, 0,0 )
t1 = time.time()


#######################################
os.system( "echo >&2;echo -n writefm... >&2" ); sys.stderr.flush()
do.writefm()
eod_cookie = do.tell()
stats = do.get_stats()
##########################################

do.close()			# b/c of fm above, this is purely sw.

fo.close()

os.system( "echo >&2; echo 'crc is %s  %d bytes in %s seconds (%s bytes/sec)' >&2"%(crc,bytes,t1-t0,
							  bytes/(t1-t0)) )

print eod_cookie
