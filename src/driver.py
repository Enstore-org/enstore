#!/usr/bin/env python
##############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
import os				# temporary - .system for mt commands
import types				# see if eod_str is string
import string				# atoi use on loc_cookie

import FTT
import EXfer
import IPC
import time				# sleep used in FTT sw_mount
import e_errors
import Trace

SEEK_SET =       0   
SEEK_CUR =       1   
SEEK_END =       2
#lseek(fd,offset,whence)

def mode_string_to_int(s, d={'r':os.O_RDONLY, 'r+':os.O_RDWR,
                             'w':os.O_WRONLY|os.O_CREAT,
                             'w+':os.O_RDWR|os.O_CREAT,
                             'a':os.O_WRONLY|os.O_CREAT|os.O_APPEND,
                             'a+':os.O_RDWR|os.O_CREAT|os.O_APPEND}):
    return d[s]



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
	self.shm.offset( 6, 0 )		# user state location (for the record)
	self.shm.offset( 7, 0 )		# for possible tcp port info
	self.shm.offset( 8, 0 )		# for possible sub-sub process pid info
	self.shm.offset( 9, 0 )		# for possible other (future) info

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
	self.fd = None			# used by RawDisk.fd_xfer
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
	# Note: remaining_bytes is updated in write and
	# fd_xfer (when opened for write)
	return {'remaining_bytes':self.remaining_bytes,
		'wr_err'         :self.wr_err,
		'rd_err'         :self.rd_err,
		'wr_access'      :self.wr_access,
		'rd_access'      :self.rd_access,
		'serial_num'     :'1234555'}

    def fd_xfer( self, fd, siz_bytes, crc_flag=None, crc=0 ):
	# returns (crc); throws exception if wrong no_bytes xferred
	# no crc if crc_flag is 0
	# For disk, blocksize is used, but there are no partial blocks
	# recreating the sem and msg insures that they are cleared
	self.sem = IPC.semget( IPC.IPC_PRIVATE, 1, IPC.IPC_CREAT|0x1ff )
	self.msg = IPC.msgget( IPC.IPC_PRIVATE, IPC.IPC_CREAT|0x1ff )
	self.shm.offset( 2, self.sem.id )
	self.shm.offset( 3, self.msg.id )

	try:
	    if self.mode == 'r':		# relative to this driver = "from hsm"
		crc = EXfer.fd_xfer( self.fd, fd, siz_bytes, 
				     self.blocksize, crc_flag, crc, self.shm )
	    else:
                
		crc = EXfer.fd_xfer( fd, self.fd, siz_bytes,
				     self.blocksize, crc_flag, crc, self.shm )
		self.remaining_bytes = self.remaining_bytes - siz_bytes
                pass
            pass
        finally:
	    del self.sem,self.msg		# sys.exit??? forking???
	    pass
	return crc

    def rd_bytes_get( self ):
	return self.shm.offget( 4 )
    def wr_bytes_get( self ):
	return self.shm.offget( 5 )
    def _bytes_clear( self ):
	self.shm.offset( 4, 0 )
	self.shm.offset( 5, 0 )
	return None
    def user_state_set( self, ii ):
	self.shm.offset( 6, ii )
	return None
    def user_state_get( self ):
	return self.shm.offget( 6 )

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
	self.fd = os.open( device+'.'+self.vol_label,
                         mode_string_to_int(mode )) # used in fd_xfer
	self.cur_loc_cookie = self.LOC_SPEC % os.lseek(self.fd,0,SEEK_CUR)
	return self			# for .read, write, seek, tell, close

    def seek( self, loc_cookie ):	# for write, eod_cookie
	# Linux is a unix where 'a' always write to end-of-file (appends).
	# ref. Python Library Reference (Release 1.5.2) p. 15.
	# Seek operations to locations before eof are only applicable to
	# reads.  Thus it would be a big hassle to allow writes in the
	# middle of a tape.
	if loc_cookie==None or loc_cookie=='None' or loc_cookie=='none':
            loc = 0
	elif type(loc_cookie)==type(""):
            loc = string.atoi( loc_cookie )
        elif type(loc_cookie)==type(0):
            loc=loc_cookie
	if self.mode == 'a': os.ftruncate(self.fd, loc)
	os.lseek(self.fd, loc, SEEK_SET)
        return None
    
    def tell( self ):
	return self.LOC_SPEC % os.lseek(self.fd,0,SEEK_CUR)

    def read( self, size_bytes ):
	return os.read( self.fd, size_bytes )

    def write( self, buffer ):
	os.write(self.fd, buffer)
	self.remaining_bytes = self.remaining_bytes - len( buffer )
	return None

    def close( self ):
        return os.close(self.fd)

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
                #print "try ",x," status ",status, " ",device
	    except FTT.error:
		pass
            # it appears that one needs to close the device and reopen it to get the status
            # to change. This doesn't make any sense, but does work for ftt v2_3.
            # if you don't close/reopen, the status never changes, but a check outside of enstore
            # using the same python calls succeeds right away after enstore reports failure - bakken 3/3/99
            FTT.close()
	    #time.sleep( 1 )
            FTT.open( device, 'r' )
	    x = x -1
	    pass
	if x == 0:
	    Trace.log( e_errors.INFO, "sw_mount error" )
	    raise "sw_mount error"
	FTT.rewind()
	FTT.close()
	return None

    def offline( self, device ):
	os.system( 'sh -c "mt -t ' + device + ' offline 2>/dev/null"')
	return None

    def get_stats( self ) :
	# Note: remaining_bytes is updated in write and
	# fd_xfer (when opened for write)
	ss = FTT.get_stats()
	if ss['remain_tape'] == None: rb = self.remaining_bytes
	else:                         rb = string.atoi(ss['remain_tape'])*1024L
	return { 'remaining_bytes':rb,
		 'wr_err'         :self.wr_err,
		 'rd_err'         :self.rd_err,
		 'wr_access'      :self.wr_access,
		 'rd_access'      :self.rd_access,
		 'serial_num'     :ss['serial_num'] }

    def fd_xfer( self, fd, siz_bytes, crc_flag=None, crc=0 ):
	# returns (crc); throws exception if wrong no_bytes xferred
	# no crc if crc_flag is 0
	# FTT knows direction and handles partial blocks
	# recreating the sem and msg insures that they are cleared
	self.sem = IPC.semget( IPC.IPC_PRIVATE, 1, IPC.IPC_CREAT|0x1ff )
	self.msg = IPC.msgget( IPC.IPC_PRIVATE, IPC.IPC_CREAT|0x1ff )
	self.shm.offset( 2, self.sem.id )
	self.shm.offset( 3, self.msg.id )
	try:
	    crc = FTT.fd_xfer( fd, siz_bytes, crc_flag, crc, self.shm )
	    if self.mode != 'r':
		self.remaining_bytes = self.remaining_bytes - siz_bytes
		pass
	    pass
	finally:
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
	if self.mode == 'r':
	    FTT.skip_fm( 1 )
	    # this is making an assumption and should be changed to use
	    # FTT.get_stats() (which is broken as of 2-25-99) and setting
	    # the block_loc (in addition to the file location)
	    pp,bb,ff = loc2int( self, self.cur_loc_cookie )
	    self.cur_loc_cookie = int2loc( self, (pp,bb,ff+1) )
	    pass
	return FTT.close()

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


class NullDriver( GenericDriver):

    def fd_xfer( self, fd, siz_bytes, crc_fun=None, crc=0 ):
	# returns (crc); throws exception if wrong no_bytes xferred
	# no crc if crc_fun is 0
	# For disk, blocksize is used, but there are no partial blocks
	# recreating the sem and msg insures that they are cleared
	self.sem = IPC.semget( IPC.IPC_PRIVATE, 1, IPC.IPC_CREAT|0x1ff )
	self.msg = IPC.msgget( IPC.IPC_PRIVATE, IPC.IPC_CREAT|0x1ff )
	self.shm.offset( 2, self.sem.id )
	self.shm.offset( 3, self.msg.id )

	try:
	    if self.mode == 'r':		# relative to this driver = "from hsm"
		__fd__ = os.open( '/dev/zero', mode_string_to_int(self.mode) )
		crc = EXfer.fd_xfer( __fd__, fd, siz_bytes, 
				     self.blocksize, None, 0, self.shm )
	    else:
                __fd__ = os.open( '/dev/null', mode_string_to_int(self.mode) )
		crc = EXfer.fd_xfer( fd, __fd__, siz_bytes,
				     self.blocksize, None, 0, self.shm )
		self.remaining_bytes = self.remaining_bytes - siz_bytes
                pass
            pass
        finally:
	    os.close( __fd__ )
	    del self.sem,self.msg		# sys.exit??? forking???
	    pass
	return crc

    pass


class safeDict:
    def __setitem__(self,key,val):
        self.__dict__[key]=val
    def __getitem__(self,key):
       return self.__dict__.get(key)

if __name__ == "__main__" :
    import sys				# exit
    import getopt			# getopt
    import os				# unlink, stat
    import stat				# stat.ST_SIZE

    Usage = "Usage: %s [--in=file] [--disk_out=file] [--tape_out=dev]" % sys.argv[0]

    #--------------------
    # default, get, and process args
    opt = safeDict()
    optlist,args = getopt.getopt( sys.argv[1:], '',
				  ['in=','disk_out=','tape_out='] )
    for option,val in optlist:
        optname = option[2:]
        opt[optname]=val

    #--------------------

    if (not opt['disk_out']) and (not opt['tape_out']) or args != []:
	print Usage; sys.exit( 1 )
        sys.exit(-1)
                
    cleanup_in = 'no'
    if not opt['in']:
	cleanup_in = 'yes'
	opt['in'] = 'driver.test.in'
	fo = open( opt['in'], 'w' ); fo.write( '12345' ); fo.close()
	pass
    statinfo = os.stat( opt['in'] )
    fsize = statinfo[stat.ST_SIZE]
    print 'size of in file is', fsize

    # write buf1 and buf2, read back buf1
    # write buf3, read back buf2 and buf3
    buf_str = {0:'hello',
               1:'hi'*2048,
               2:'end'}

    buf_loc={}
    
    for driver,dev in ((DelayDriver,opt['disk_out']),
		       (RawDiskDriver,opt['disk_out']),
		       (FTTDriver,opt['tape_out'])):
	if dev == '': continue

	# simulate new volume
	eod_cookie = None

	print "\n driver is",driver,'dev is',dev

	print ' instantiating driver object'
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

                buf_loc[buf]=do.tell()
		print '     buf',buf,'loc is',buf_loc[buf]
		
		print "     stats - %s"%do.get_stats()
		print '     write buf', buf, "pos b4 write is",do.tell()
		do.write( buf_str[buf])

		fd = os.open( opt['in'], os.O_RDONLY )
		crc = do.fd_xfer( fd, fsize, 1)
		os.close(fd)
		print '     the crc is',crc
		print "     stats - %s"%do.get_stats()
	    
		do.writefm()

		# AFTER A WRITE, tell() always reports eof, even if I wrote a
		# few bytes at the beginning of an existing big file. But
		# I should only be writing at end-of-file/tape anyway
		eod_cookie = do.tell()
		print '     after write, eod_cookie is',eod_cookie
	    else:
		print '  read invoking open method'
		do = hsm_driver.open( dev, "r" )# do = "driver object"

		print '     seek to buf',buf,'location',buf_loc[buf]
		do.seek( buf_loc[buf])
	    
		print "     stats - %s"%do.get_stats()
		print '     read buf', buf, "pos b4 read is",do.tell()
		xx = do.read( len(buf_str[buf]))

		print '     pos after read is',do.tell()
		print "     stats - %s"%do.get_stats()

                if xx != buf_str[buf]:
		    print '      XX  error reading buf',buf,'xx is',xx
		    sys.exit( 1 )
		    pass

		statinfo = os.stat( opt['in'] )
		fd = os.open( opt['in']+str(buf), os.O_WRONLY )
		crc = do.fd_xfer( fd, fsize, 1 )
		os.close(fd)
		print '     the crc is',crc
	    
		pass
	    print '     invoking close method'
	    print '     close method returns:',do.close()
	    pass
	pass

    if cleanup_in == 'yes': os.unlink( opt['in'] )
    sys.exit( 0 )


    
