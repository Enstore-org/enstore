##############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
import os				# temporary - .system for mt commands
import types				# see if eod_str is string
import string				# atoi use on loc_cookie

import ETape
import FTT
import EXfer
import IPC
import time				# sleep used in FTT sw_mount
import generic_cs			# enprint


class GenericDriver:

    # the file location should be a string that will produce an ordered
    # list of the files on the device
    LOC_SPEC = '%012d'		# bytes offset (arbitary width)

    def __init__( self ):
	# Note, I could pass "device" here save it, but I want to pass it to
	#       open (to make open like python builtin open) so I might as
	# well pass it to sw_mount and offline also.

	self.shm = IPC.shmget( IPC.IPC_PRIVATE, 0x400000, IPC.IPC_CREAT|0x1ff )
	self.shm.offset( 0, 0x400000 )
	self.shm.offset( 1, self.shm.id )

	# volume info
        self.remaining_bytes = 0
	self.blocksize = 0		# for the volume - from vc

	# driver info
	self.no_xfers = 0		# number of xfers (opens)
	self.mode = 'r'			# aids in remaining_bytes calculations
	self.cur_loc_cookie = None	# different for each driver

	# per read/write info (clear on open for write/read)
	self.file_marks = 0
	self.bytes_xferred = 0		# for possible bytes-remaining
        self.wr_err = 0			# counts
        self.rd_err = 0			# counts
        self.wr_access = 0		# counts
        self.rd_access = 0		# counts

	# special
	self.fo = None			# used by RawDisk.fd_xfer
	self.vol_label = ''		# used by RawDisk.open -- *could* also
	# -- use to check for success binds of same vol - could be -
        # used for validate location info???
	return None

    #-----------------
    def sw_mount( self, device, blocksize, remaining_bytes, vol_label ):
	# gets/verifies cur_loc
        self.remaining_bytes = remaining_bytes
	self.blocksize = blocksize
	self.cur_loc_cookie = self.LOC_SPEC % 0
	self.vol_label = vol_label	# only need RawDiskDriver
	return None

    def offline( self, device ):	# eject tape
	return None

    def get_stats( self ) :
	# return error count and # of files accesses since???
	return {'remaining_bytes':self.remaining_bytes,
		'wr_err':self.wr_err,
		'rd_err':self.rd_err,
		'wr_access':self.wr_access,
		'rd_access':self.rd_access}

    def fd_xfer( self, fd, siz_bytes, crc_fun=None, crc=0 ):
	# returns (crc); throws exception if wrong no_bytes xferred
	# no crc if crc_fun is 0
	# For disk, blocksize is used, but there are no partial blocks
	# recreating the sem and msg insures that they are cleared
	self.sem = IPC.semget( IPC.IPC_PRIVATE, 1, IPC.IPC_CREAT|0x1ff )
	self.msg = IPC.msgget( IPC.IPC_PRIVATE, IPC.IPC_CREAT|0x1ff )
	self.shm.offset( 2, self.sem.id )
	self.shm.offset( 3, self.msg.id )
	self.shm.offset( 6, 0 )		# abort flg
	self.fo.flush()			# Important - sync fp and fd
	if self.mode == 'r':		# relative to this driver = "from hsm"
	    crc = EXfer.fd_xfer( self.fo.fileno(), fd, siz_bytes, 
				 self.blocksize, crc_fun, crc, self.shm )
	else:
	    crc = EXfer.fd_xfer( fd, self.fo.fileno(), siz_bytes,
				 self.blocksize, crc_fun, crc, self.shm )
	    self.remaining_bytes = self.remaining_bytes - siz_bytes
	    pass
	del self.sem,self.msg		# sys.exit??? forking???
	return crc

    def rd_bytes_get( self ):
	return self.shm.offget( 4 )
    def wr_bytes_get( self ):
	return self.shm.offget( 5 )

    def writefm( self ):			# wrapper may need this
	self.file_marks = self.file_marks + 1
	return None
    #-----------------

    #-----------------
    # emulate "system" methods as closely as possible???

    def open( self, device, mode ):
	# clear per read/write stats
	# if write, calling program is expected to save eod_cookie.
	# Note: I can not add a method to a file object???
	self.no_xfers = self.no_xfers + 1
	self.shm.offset( 4, 0 )		# clear read bytes
	self.shm.offset( 5, 0 )		# clear write bytes
	self.file_marks = 0
	self.bytes_xferred = 0		# for possible bytes-remaining
        self.wr_err = 0			# counts
        self.rd_err = 0			# counts
        self.wr_access = 0		# counts
        self.rd_access = 0		# counts
	self.mode = mode[0]		# used in fd_xfer
	self.fo = open( device+'.'+self.vol_label, mode )	# used in fd_xfer
	self.cur_loc_cookie = self.LOC_SPEC % self.fo.tell()
	return self			# for .read, write, seek, tell, close

    def seek( self, loc_cookie ):	# for write, eod_cookie
	# Linux is a unix where 'a' always write to end-of-file (appends).
	# ref. Python Library Reference (Release 1.5.2) p. 15.
	# Seek operations to locations before eof are only applicable to
	# reads.  Thus it would be a big hassel to allow writes in the
	# middle of a tape.
	if loc_cookie==None or loc_cookie=='None' or loc_cookie=='none':
	      loc = 0
	else: loc = string.atoi( loc_cookie )
	if self.mode == 'a': self.fo.truncate( loc )
	self.fo.seek( loc )
	return None

    def tell( self ):
	return self.LOC_SPEC % self.fo.tell()

    def read( self, size_bytes ):
	return self.fo.read( size_bytes )

    def write( self, buffer ):
	self.fo.write( buffer )
	self.remaining_bytes = self.remaining_bytes - len( buffer )
	return None

    def close( self ):
	return self.fo.close()

    def flush( self ):
	return self.fo.flush()

    #-----------------
    pass


class  FTTDriver(GenericDriver) :
    LOC_SPEC = '%04d_%09d_%07d'		# partition, blk offset, filemarks (arbitrary field widths
    """
     A Fermi Tape Tools driver
    """
    def sw_mount( self, device, blocksize, remaining_bytes, vol_label ):
	# Get the position from the drive.
	# Should the driver keep the volume label to connect "position"
	# information to??? So, if a particular volume is unbound and then
	# binded again as the next volume, the position could be checked to
	# see if a rewind already occurred.
        self.remaining_bytes = remaining_bytes
	self.blocksize = blocksize	# save blocksize so we do not need as param to open
	# make cur_loc_cookie such that an ordered list can be produced (for pnfs)
	self.cur_loc_cookie = int2loc( self, (0,0,0) )# partition, blk offset, filemarks
	FTT.open( device, 'r' )
	x = 120				# for now, after ??? ftt_rewind will
					# raise exception
	while x:
	    try:
		status = FTT.status( 3 )
		if status['ONLINE']: break
	    except FTT.error:
		pass
	    time.sleep( 1 )
	    x = x -1
	    pass
	if x == 0:
	    generic_cs.enprint( "sw_mount error" )
	    raise "sw_mount error"
	FTT.rewind()
	FTT.close()
	return None

    def offline( self, device ):
	os.system( 'mt -t ' + device + ' offline')
	return None

    def get_stats( self ) :
	ss = FTT.get_stats()
	return {'remaining_bytes':self.remaining_bytes,
		'wr_err':self.wr_err,
		'rd_err':self.rd_err,
		'wr_access':self.wr_access,
		'rd_access':self.rd_access}

    def fd_xfer( self, fd, siz_bytes, crc_fun=None, crc=0 ):
	# returns (crc); throws exception if wrong no_bytes xferred
	# no crc if crc_fun is 0
	# FTT knows direction and handles partial blocks
	# recreating the sem and msg insures that they are cleared
	self.sem = IPC.semget( IPC.IPC_PRIVATE, 1, IPC.IPC_CREAT|0x1ff )
	self.msg = IPC.msgget( IPC.IPC_PRIVATE, IPC.IPC_CREAT|0x1ff )
	self.shm.offset( 2, self.sem.id )
	self.shm.offset( 3, self.msg.id )
	self.shm.offset( 6, 0 )		# abort flg
	crc = FTT.fd_xfer( fd, siz_bytes, crc_fun, crc, self.shm )
	if self.mode != 'r':
	    self.remaining_bytes = self.remaining_bytes - siz_bytes
	    pass
	del self.sem,self.msg		# sys.exit??? forking???
	return crc

    def writefm( self ):
	# I think for write only; kind of like a close
	FTT.writefm()
	self.file_marks = self.file_marks + 1
	pp,bb,ff = loc2int( self, self.cur_loc_cookie )
	self.cur_loc_cookie = int2loc( self, (pp,bb,ff+1) )
	return None

    def open( self, device, mode ):
	self.no_xfers = self.no_xfers + 1
	self.shm.offset( 4, 0 )		# clear read bytes
	self.shm.offset( 5, 0 )		# clear write bytes
	self.file_marks = 0
	self.bytes_xferred = 0		# for possible bytes-remaining
        self.wr_err = 0			# counts
        self.rd_err = 0			# counts
        self.wr_access = 0		# counts
        self.rd_access = 0		# counts
	self.mode = mode[0]		# used in fd_xfer
	FTT.open( device, mode )
	# Note: self.blocksize is set in sw_mount which is done in mover
	# forked process AND will not be done for all read/write
	# operations. self.blocksize is passed back to parent (via code in
	# mover).
	FTT.set_blocksize( self.blocksize )
	# verifiy location via FTT.stats???
	return self

    def seek( self, loc_cookie ):
	if loc_cookie==None or loc_cookie=='None' or loc_cookie=='none':
	    part, block_loc, filenum = 0,0,0
	else:
	    part, block_loc, filenum = loc2int( self, loc_cookie )
	if block_loc:
	    FTT.locate( block_loc )
	elif filenum != loc2int(self,self.cur_loc_cookie)[2]:
	    # NOTE: when skipping file marks, there must be a file mark to
	    #       skip over. This is why we can not "skip" to the beginning
	    #       of a tape; we must "rewind" to get to BOT
	    if filenum == 0: FTT.locate( 0 )
	    #if filenum == 0: FTT.rewind()
	    else:
		skip = filenum - loc2int(self,self.cur_loc_cookie)[2]
		if skip < 0: skip = skip - 1# if neg, make more neg
		FTT.skip_fm( skip )
		if skip < 0: FTT.skip_fm( 1 )
		pass
	    pass
	self.cur_loc_cookie = int2loc( self, (part,block_loc,filenum) )
	return None

    def tell( self ):
	return self.cur_loc_cookie

    def read( self, size_bytes ):
	return FTT.read( size_bytes )

    def write( self, buffer ):
	FTT.write( buffer )
	self.remaining_bytes = self.remaining_bytes - len( buffer )
	return None

    def close( self ):
	return FTT.close()

    def flush( self ):
	return FTT.flush()

    pass


import re

def loc2int( self, loc ):
    xx = re.split( '_', loc )
    part, block_loc, filenum = ( string.atoi(xx[0]),
				 string.atoi(xx[1]),
				 string.atoi(xx[2]) )
    return [part, block_loc, filenum]

def int2loc( self, ii ):
    return self.LOC_SPEC % ii
    


class  RawDiskDriver(GenericDriver) :
    """
    A driver for testing with disk files
    """	
	

class  DelayDriver(RawDiskDriver) :
    """
    A specialized RawDisk Driver for testing with disk files, but with 
    crude delays modeled on no particular tape drive.

    """
    import time

    def sw_mount( self, device, blocksize, remaining_bytes, vol_label ):
        self.time.sleep( 10 )                   # sw_mount time 10 seconds
	RawDiskDriver.sw_mount( self, device, blocksize, remaining_bytes,
				vol_label )
	return None

    def offline( self, device ):
	loc = string.atoi( self.cur_loc_cookie )
	self.time.sleep(loc/20E6)   # rewind time @ 20MB/sec
	self.time.sleep(10)			  # offline time -- 10 seconds
        RawDiskDriver.offline( self, device )
	return None

    def seek( self, loc_cookie ):
	if loc_cookie==None or loc_cookie=='None' or loc_cookie=='none':
	      loc = 0
	else: loc = string.atoi( loc_cookie )
	bytesskipped = abs( string.atoi(self.cur_loc_cookie) - loc );
	self.time.sleep( bytesskipped / 20E6 )# skip at 20MB/sec
	RawDiskDriver.seek( self, loc_cookie )
	return None

    pass


if __name__ == "__main__" :
    import sys				# exit
    import getopt			# getopt
    import os				# unlink, stat
    import stat				# stat.ST_SIZE
    import ECRC				# ECRC.ECRC (to do crc)

    Usage = "Usage: python driver.py [--in=file] [--disk_out=file] [--tape_out=dev]"

    #--------------------
    # default, get, and process args
    opt_disk_out = ''; opt_tape_out = ''; opt_in = ''
    optlist,args = getopt.getopt( sys.argv[1:], '',
				  ['in=','disk_out=','tape_out='] )
    for opt,val in optlist:
	exec( 'opt_'+opt[2:]+'="'+val+'"' )
	pass
    #--------------------

    if opt_disk_out=='' and opt_tape_out=='' or args != []:
	print Usage; sys.exit( 1 )
	pass

    cleanup_in = 'no'
    if opt_in == '':
	cleanup_in = 'yes'
	opt_in = 'driver.test.in'
	fo = open( opt_in, 'w' ); fo.write( '12345' ); fo.close()
	pass
    statinfo = os.stat( opt_in )
    fsize = statinfo[stat.ST_SIZE]
    print 'size of in file is', fsize

    # write buf1 and buf2, read back buf1
    # write buf3, read back buf2 and buf3
    buf0 = 'hello'
    buf1 = 'hi'*2048
    buf2 = 'end'

    for driver,dev in ((DelayDriver,opt_disk_out),
		       (RawDiskDriver,opt_disk_out),
		       (FTTDriver,opt_tape_out)):
	if dev == '': continue

	# simulate new volume
	eod_cookie = eval('None')

	print "\n driver is",driver,'dev is',dev

	print ' instanceating driver object'
	hsm_driver = driver()

	if driver == FTTDriver:
	    print ' setting ftt_debug to 1'
	    #FTT.set_debug( 1 )
	    pass

	print ' invoking sw_mount method'
	hsm_driver.sw_mount( dev, 1024, 50000000, 'test_vol' )
	
	for dir,buf in (('w',0), ('w',1), ('r',0), ('w',2), ('r',1), ('r',2)):
	    if dir == 'w':
		print '  write invoking open method'
		do = hsm_driver.open( dev, "a+" )# do = "driver object"

		print '     seek to eod_cookie',eod_cookie
		do.seek( eod_cookie )

		exec( "buf%d_loc=%s"%(buf,repr(do.tell())) )
		print '     buf',buf,'loc is',eval("buf%d_loc"%buf)
		
		print "     stats - %s"%str(do.get_stats())
		print '     write buf', buf, "pos b4 write is",do.tell()
		do.write( eval('buf%d'%buf) )

		fo = open( opt_in, 'r' )
		crc = do.fd_xfer( fo.fileno(), fsize, ECRC.ECRC )
		fo.close()
		print '     the crc is',crc
		print "     stats - %s"%str(do.get_stats())
	    
		do.writefm()

		# AFTER A WRITE, tell() always reports eof, even if I wrote a
		# few bytes at the beginning of an existing big file. But
		# I should only be writing at end-of-file/tape anyway
		eod_cookie = do.tell()
		print '     after write, eod_cookie is',eod_cookie
	    else:
		print '  read invoking open method'
		do = hsm_driver.open( dev, "r" )# do = "driver object"

		print '     seek to buf',buf,'location',eval("buf%d_loc"%buf)
		do.seek( eval("buf%d_loc"%buf) )
	    
		print "     stats - %s"%str(do.get_stats())
		print '     read buf', buf, "pos b4 read is",do.tell()
		xx = do.read( len(eval('buf%d'%buf)) )

		print '     pos after read is',do.tell()
		print "     stats - %s"%str(do.get_stats())

		if xx != eval('buf%d'%buf):
		    print '      XX  error reading buf',buf,'xx is',xx
		    sys.exit( 1 )
		    pass

		statinfo = os.stat( opt_in )
		fo = open( opt_in+str(buf), 'w' )
		crc = do.fd_xfer( fo.fileno(), fsize, ECRC.ECRC )
		fo.close()
		print '     the crc is',crc
	    
		pass
	    print '     invoking close method'
	    print '     close method returns:',do.close()
	    pass
	pass

    if cleanup_in == 'yes': os.unlink( opt_in )
    sys.exit( 0 )
