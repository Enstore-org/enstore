#! /usr/bin/env python
#   This file (ftt_regression.py) was created by Ron Rechenmacher <ron@fnal.gov> on
#   Jul 12, 1999. "TERMS AND CONDITIONS" governing this file are in the README
#   or COPYING file. If you do not have such a file, one can be obtained by
#   contacting Ron or Fermi Lab in Batavia IL, 60510, phone: 630-840-3000.
#   $RCSfile$
#   $Revision$
#   $Date$


import sys				# argv, stdin and exit
import getopt				# getopt - standard option parsing
import traceback			# format_exception - when unecpected error occurs
import pprint				# pprint
import os				# system (system commands)
import re				# matching and extracting groups
import string				# split

USAGE = '\
usage: %s [options] <tape_device>\n\
       tape_device must be a valid FTT tape device in the /dev/rmt directory.\n\
       "/dev/rmt" is optional.\n\
       example: %s /dev/rmt/tps3d1\n'%(sys.argv[0],sys.argv[0])

opts,args = getopt.getopt( sys.argv[1:], '', '' )

if len(args) != 1:
    print USAGE
    sys.exit( 1 );
    pass
tape_dev = args[0]
mm = re.match( '(.*)tps([0-9]+)d.*', tape_dev )
if not mm:
    print 'device does not match "tps" format'
    print USAGE
    sys.exit( 1 )
    pass
if mm.group(1) == '': tape_dev = "/dev/rmt/" + tape_dev
tape_ctlr = mm.group( 2 )
#if not os.path.exists(tape_dev):
#    print tape_dev,"does not exists"
#    print USAGE
#    sys.exit( 1 )
#    pass

print 'Record configuration (uname -a, driver, etc)'
uname = os.uname()
print 'uname:', uname

version = []
tape_drv = ''
if uname[0] == 'Linux':
    ### what about the st and sg versions???
    ### mt version from "mt -v"
    import glob
    proc_path = glob.glob( '/proc/scsi/*/%s'%tape_ctlr )
    pipe = os.popen( 'cat %s'%proc_path[0], 'r' )
    ctlr_txt = pipe.readlines()
    sts = pipe.close()
    if sts:
	print 'unexpected error reading Linux scsi controller %s info'%tape_ctlr
	sys.exit( 1 )
	pass

    tape_drv = string.split(proc_path[0],'/')[-2]
    if   tape_drv == 'aic7xxx':
	mm = re.search( 'version: (\S+)', ctlr_txt[0] )
	if not mm:
	    print 'no version match for aic7xxx driver'
	    sys.exit( 1 )
	version.append( mm.group(1) )
	pass
    elif tape_drv == 'BusLogic':
	rx = re.compile( 'Version:* ([^ ,]+)' )
	for line in ctlr_txt:
	    mm = rx.search( line )
	    if mm: version.append( mm.group(1) )
	    pass
	pass
    elif tape_drv == 'ncr53c8xx':
	pass
    else:
	pass
    pass
elif uname[0] == 'IRIX64':
    tape_drv = 'IRIX64'
    pass
else:
    pass
print 'tape driver:', tape_drv
print 'driver versions:', version


import FTT

print 'FTT modules was compiled with ftt version:',FTT.version()


"""
-- initial check --
   FTT.open  (basic - need to open before we can do anything else)
   FTT.status (basic - can we determine if a tape is in driver?)

-- tape in driver tests ----------
   test FTT.get_stats
   test open for write
   test write
   test close
   test open for read
   test locate
   test read
   test filemark

-- transition from tape-in-drive to no-tape-in-drive
   test unload
   test status

-- transition from no-tape-in-drive to tape-in-drive

"""



###############################################################################
def initial_check( tape_dev ):
    print 'Initial check'

    print '    open'
    FTT.open( tape_dev, 'r' )

    print '    status'
    ret = FTT.status( 3 )

    print '    get_statsAll'
    sts = FTT.get_statsAll()
    print 'vendor_id:',sts['vendor_id']
    print 'product_id:',sts['product_id']
    print 'serial_num:',sts['serial_num']
    #print 'get_stats returned:'
    #pprint.pprint( sts )

    print '    close'
    FTT.close()
    return ret


###############################################################################
def online_test( tape_dev ):
    # Can I do a non-destructive test? I.e. put the tape back the way
    # it was found.

    print '    open'
    FTT.open( tape_dev, 'r' )
    #sts = FTT.get_statsAll()
    #print 'get_stats returned:'
    #pprint.pprint( sts )

    # test locate to invalid block
    # test locate to valid block
    print '    locate'
    FTT.locate( 0 )
    # check locate
    print '    get_stats'
    sts = FTT.get_stats()
    if sts['bloc_loc'] != '0': raise 'unexpected bloc_loc'

    print '    get_mode'
    mode = FTT.get_mode()
    print 'FTT.get_mode() returned:',mode
    if mode['blocksize'] != 0: raise 'unexpected blocksize'
##     # jon thinks this may have clear the "blocksize/hang problem"
##     # on rip1.
##     dev_name = FTT.set_mode( mode['density'],
## 			     mode['compression'],
## 			     mode['blocksize'] )
##     print 'FTT.set_mode(%s,%s,%s) returned: %s'%(mode['density'],
## 						mode['compression'],
## 						mode['blocksize'],
## 						dev_name)

    print '    close'
    FTT.close()

    if uname[0] == 'Linux':
	no_except_blksz = (512,65536,102400)
	except_blksz = (262144,)
    else:
	no_except_blksz = (512,65536,262144,102400)
	except_blksz = ()
	pass
	
    for blksiz in no_except_blksz:
	print 'checking write/read of 80 bytes with blksize %s'%blksiz
	FTT.open( tape_dev, 'a+' )
	FTT.set_blocksize( blksiz )
	FTT.write( ' '*80 )
	FTT.writefm()			# need to do this?
	FTT.rewind()
	FTT.close()
	FTT.open( tape_dev, 'r' )
	FTT.set_blocksize( blksiz )
	ss = FTT.read( 80 )
	FTT.rewind()
	FTT.close()
	if ss != ' '*80:
	    print 'read of 80 bytes returned %s bytes (data not checked)'%len(ss)
	    raise 'unexpected FTT.read return value'
	pass
    for blksiz in except_blksz:
	print 'checking write/read of 80 bytes with blksize %s'%blksiz
	FTT.open( tape_dev, 'a+' )
	FTT.set_blocksize( blksiz )
	FTT.write( ' '*80 )
	FTT.writefm()			# need to do this?
	FTT.rewind()
	FTT.close()
	FTT.open( tape_dev, 'r' )
	FTT.set_blocksize( blksiz )
	try:
	    ss = FTT.read( 80 )
	    print '*UNANTICIPATED*: no exception occurred on this particular platform'
	except:
	    print 'an *anticipated* exception has occurred'
	    pass
	FTT.rewind()
	FTT.close()
	if ss != ' '*80:
	    print 'read of 80 bytes returned %s bytes (data not checked)'%len(ss)
	    raise 'unexpected FTT.read return value'
	pass

    print 'Online test complete.'
    return				# from online_test


###############################################################################
def offline_test( tape_dev ):
    print '    open'
    FTT.open( tape_dev, 'r' )

    print '    status'
    sts = FTT.status( 3 )
    #print 'status returned:'
    #pprint.pprint( sts )

    try:
	print '    unload'
	sts = FTT.unload()
	print '*UNANTICIPATED*: no exception occurred on this particular platform'
    except FTT.error, value:
	print
	print 'an *anticipated* exception has occurred'
	if tape_drv == 'ncr53c8xx' and value.args[4] == FTT.FTT_EBLANK:
	    print 'ftt return EBLANK when running no top of the ncr53c8xx driver'
	    pass
	else:
	    print 'ftt return %s when running no top of the %s driver'%(value.args[4],tape_drv)
	    pass
	print
	pass

    print '    get_stats'
    sts = FTT.get_stats()
    
    print '    close'
    FTT.close()
    print 'Offline tests complete.'
    return				# from offline_test


###############################################################################
def off_to_online_test( tape_dev ):
    print 'Transition to online tests (loading a tape).'
    sts = {'ONLINE':0}
    while sts['ONLINE'] == 0:
	FTT.open( tape_dev, 'r' )
	sts = FTT.status( 3 )
	FTT.close()
	pass
    if sts['ONLINE'] != 1: raise "unexpected 'ONLINE' value %s"%sts['ONLINE']
    print 'Status now indicates the tape is online.'
    return				# from off_to_online_test


###############################################################################
def on_to_offline_test( tape_dev ):
    print 'Transition to offline tests (unloading tape).'

    print '    open'
    FTT.open( tape_dev, 'r' )

    print '    unload'
    FTT.unload()

    print '    close'
    FTT.close()

    print 'It is a known problem that FTT.status will not change unless the'
    print 'device is closed and re-opened.'
    sts = {'ONLINE':1}
    while sts['ONLINE'] == 1:
	FTT.open( tape_dev, 'r' )
	sts = FTT.status( 3 )
	FTT.close()
	pass
    if sts['ONLINE'] != 0: raise "unexpected 'ONLINE' value %s"%sts['ONLINE']
    print 'Status now indicates the tape is offline.'
    return				# from on_to_offline_test


###############################################################################
# main
print '\
This procedure will check if a tape is in the tape drive and if so\n\
it will proceed with "online" tests, which will write the 1st 80 bytes\n\
of the tape. The program should eventually eject the tape and ask that\n\
it be reinserted.\n\
If there is no tape in the drive initially, you will be asked to insert\n\
one. The program will then write the 1st 80 bytes of the tape.'
print 'Press enter to continue...'
sys.stdin.readline()

try:
    ret = initial_check( tape_dev )
    if ret['ONLINE'] == 1:
	print 'Initial check indicates a tape is in the drive.'
	print 'Preceeding with "online" test first.'
	online_test( tape_dev )
	on_to_offline_test( tape_dev )
	print 'Proceeding with "offline" tests.'
	offline_test( tape_dev )
	print
	print 'Please insert a tape into the tape drive.'
	print 'This can be done, for example, by:'
	print '    dasadmin dismount -d DE01'
	print '    dasadmin mount -t DECDLT CA2502 DE01'
	off_to_online_test( tape_dev )
    elif ret['ONLINE'] == 0:
	print 'Initial check indicates no tape is in the drive.'
	print 'Preceeding with "offline" test first.'
	offline_test( tape_dev )
	print
	print 'Please insert a tape into the tape drive.'
	print 'This can be done, for example, by:'
	print '    dasadmin dismount -d DE01'
	print '    dasadmin mount -t DECDLT CA2502 DE01'
	off_to_online_test( tape_dev )
	print 'Proceeding with "online" tests.'
	online_test( tape_dev )
	on_to_offline_test( tape_dev )
    else:
	print "ERROR - FTT.status['ONLINE'] should be 1 or 0"
	exit_status = 1
	pass
    print
    print 'Testing completed OK.'
    pass
    exit_status = 0
except:
    print
    print 'an *UNANTICIPATED* exception has occurred - the exception information follows:'
    print
    exc, value, tb = sys.exc_info()
    for l in traceback.format_exception( exc, value, tb ):
	print l[0:len(l)-1]
	pass
    exit_status = 1
    FTT.close()				# OK if already closed
    pass

sys.exit( exit_status )
