#! /usr/bin/env python
#   This file (fd_xfer.py) was created by Ron Rechenmacher <ron@fnal.gov> on
#   May 14, 1999. "TERMS AND CONDITIONS" governing this file are in the README
#   or COPYING file. If you do not have such a file, one can be obtained by
#   contacting Ron or Fermi Lab in Batavia IL, 60510, phone: 630-840-3000.
#   $RCSfile$
#   $Revision$
#   $Date$


usage = "fd_xfer.py src_spec dst_spec eod [loops [drvtype drvdesig volume dasnod]]"

import sys				# sys.argv[1:]
import driver
import os				# os.stat
import stat				# stat.ST_SIZE
import time				# time.time (xfer rate)
import pprint				# for stats
import FTT				# to get stats directly
import string				# support for the pprint mission

stdout_sav = sys.stdout
sys.stdout = sys.stderr


def tt():
    return time.strftime("%a %b %d %H:%M:%S %Z %Y",time.localtime(time.time()))

if not len(sys.argv) in (4,5,9): print usage; sys.exit()

infile     = sys.argv[1]
tapedev    = sys.argv[2]
eod_cookie = sys.argv[3]

loops = 1
if len(sys.argv) > 4: loops      = string.atoi( sys.argv[4] )
if len(sys.argv) == 9:
    drvtype    = sys.argv[5]
    drvdesig   = sys.argv[6]
    volume     = sys.argv[7]
    dasnod     = sys.argv[8]
    do_robot = 1
else: do_robot = 0

external_label = "test"

# first check tapedev (just to make sure it exists) we will get an exception
# if it does not (ftt coredumps with no-existant files).
statinfo = os.stat( tapedev )

statinfo = os.stat( infile )

# initialize variables incase we comment out (during the course of testing)
# the lines that set them 
crc = None
t0 = time.time()
t1 = time.time()

bytes = statinfo[stat.ST_SIZE]

hsm_driver = driver.FTTDriver()
#hsm_driver = driver.NullDriver()


# REMEMBER x=3 will only do 2 tries!
def rsh_das( node, das_cmd ):
    cmd = "rsh %s '. /usr/local/etc/setups.sh;setup enstore;x=3;\
while x=`expr $x - 1`; do \
    if dasadmin %s;then break;\
    else sleep 5;fi;\
done'"%(node,das_cmd)
    os.system( cmd )



loop = 1
while loop <= loops:
    print '%s: start loop %s'%(tt(),loop)

    if do_robot:
	print "%s:         ejecting... "%tt(),;sys.stdout.flush()
        hsm_driver.offline( tapedev )
	print "\n%s:         dismounting... "%tt(),;sys.stdout.flush()
	rsh_das( dasnod, 'dismount -d %s'%drvdesig )
	print "%s:         mounting... "%tt(),;sys.stdout.flush()
	rsh_das( dasnod, 'mount -t %s %s %s'%(drvtype,volume,drvdesig) )
    else:
	print "%s:         rewinding... "%tt(),;sys.stdout.flush()
	os.system( "mt -f %s rewind"%tapedev )
	print

    fo = open( infile, 'r' )

    #########################################################################
    print "%s:         sw_mount... "%tt(),; sys.stdout.flush()
    hsm_driver.sw_mount( tapedev, 102400, 30520749568L, external_label, eod_cookie )
    driver_obj = hsm_driver.open( tapedev, 'a+' )
    if driver_obj.is_bot(driver_obj.tell()) and driver_obj.is_bot(eod_cookie):
	# write an ANSI label and update the eod_cookie
	print '\n%s:         label...'%tt(),; sys.stdout.flush()
	ll = driver_obj.format_label( external_label )
	driver_obj.write( ll )
	driver_obj.writefm()
	eod_cookie = driver_obj.tell()
	remaining_bytes = driver_obj.get_stats()['remaining_bytes']
	pass
    driver_obj.close()

    # THE XFER ##################################################################
    driver_obj = hsm_driver.open( tapedev, 'a+' )
    print '\n%s:         seek to %s... '%(tt(),eod_cookie),; sys.stdout.flush()
    driver_obj.seek( eod_cookie )
    print '\n%s:         driver_obj.fd_xfer( fo.fileno(), %s, 0,0 )'%(tt(),bytes),; sys.stdout.flush()
    t0 = time.time()
    crc = driver_obj.fd_xfer( fo.fileno(), bytes, 0,0 )
    t1 = time.time()
    print '\n%s:         writefm... '%tt(),; sys.stdout.flush()
    driver_obj.writefm()
    eod_cookie = driver_obj.tell()
    print '\n%s:         get_stats... '%tt(),; sys.stdout.flush()
    if isinstance( hsm_driver, driver.FTTDriver ): stats = FTT.get_stats()
    else:                                          stats = driver_obj.get_stats()
    xx = tt()
    for ll in string.split( pprint.pformat(stats), '\012' ): print '\n%s:         %s'%(xx,ll),; sys.stdout.flush()
    driver_obj.close()			# b/c of fm above, this is purely sw.


    print '\n%s:         crc is %s  %d bytes in %s seconds (%s bytes/sec)'%(tt(),
									    crc,
									    bytes,
									    t1-t0,
									    bytes/(t1-t0))
    fo.close()

    print_resume = 0
    try:
	statinfo = os.stat( "pause" )
	print_resume = 1
	print 'paused...',; sys.stdout.flush()
	while 1: time.sleep( 1 ); statinfo = os.stat( "pause" )
	pass
    except:
	if print_resume: print ' resuming'
	pass


    loop = loop + 1
    pass


sys.stdout = stdout_sav
print eod_cookie
