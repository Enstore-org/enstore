#!/usr/bin/env python
##############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
import os				# temporary - .system for mt commands
import types				# see if eod_str is string
import string				# atoi use on loc_cookie
import time
import re

##Enstore imports
import FTT
import EXfer
import IPC
import time				# sleep used in FTT sw_mount
import e_errors
import Trace
import hostaddr

# define exception(s)
SeekError = "seek error"

SEEK_SET =       0   
SEEK_CUR =       1   
SEEK_END =       2
#lseek(fd,offset,whence)

SeekError = "seek error"
SWMountError = "sw_mount error"
InvalidLocationError = "invalid location error"

def mode_string_to_int(s, d={'r':os.O_RDONLY, 'r+':os.O_RDWR,
                             'w':os.O_WRONLY|os.O_CREAT,
                             'w+':os.O_RDWR|os.O_CREAT,
                             'a':os.O_WRONLY|os.O_CREAT|os.O_APPEND,
                             'a+':os.O_RDWR|os.O_CREAT|os.O_APPEND}):
    return d[s]




#setting this to 1 turns on printouts related to "paranoid" checking of VOL1 headers.
#once this is all working, the printout code should be stripped out
debug_paranoia=0
if os.environ.get('DEBUG_PARANOIA'):
    debug_paranoia=1

class GenericDriver:

    # the file location should be a string that will produce an ordered
    # list of the files on the device
    LOC_SPEC = '%012d'		# bytes offset (arbitary width)


    def loc2int( self, loc ):
        if loc==None or loc=='None' or loc=='none':
            part, block_loc, filenum = 0,0,0
        else:
            xx = re.split( '_', loc )
            part, block_loc, filenum = ( string.atoi(xx[0]),
                                         string.atoi(xx[1]),
                                         string.atoi(xx[2]) )
        return [part, block_loc, filenum]

    def int2loc( self, ii ):
        return self.LOC_SPEC % ii

    
    def __init__( self, sm_size ):
	# Note, I could pass "device" here save it, but I want to pass it to
	#       open (to make open like python builtin open) so I might as
	# well pass it to sw_mount and offline also.

	self.shm = IPC.shmget( IPC.IPC_PRIVATE, sm_size, IPC.IPC_CREAT|0x1ff )
	self.shm.offset( 0, sm_size )
	self.shm.offset( 1, self.shm.id )
	self.shm.offset( 6, 0 )		# user state location (for the record)
	self.shm.offset( 7, 0 )		# for possible tcp port info
	self.shm.offset( 8, 0 )		# for possible sub-sub process pid info
	self.shm.offset( 9, 0 )		# for possible other (future) info

        # statistics info
        self.statisticsOpen = {}
        self.statisticsClose = {}
	
	# volume info
        self.remaining_bytes = 0
	self.blocksize = 0		# for the volume - from vc

	# driver info
	self.mode = ''			# Aids in remaining_bytes calculations
					# and indicates open/close status
	self.cur_loc_cookie = None	# Different for each driver

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
    def sw_mount( self, device, blocksize, remaining_bytes, vol_label,
		  eod_cookie ):
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

    def rewind(self):
        self.cur_loc_cookie=self.LOC_SPEC%0

    def skip_fm(self, skip):
        #XXX what to do for non-FTT driver?
        return
        
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

    def is_bot( self, loc_cookie ):
	if loc_cookie==None or loc_cookie=='None' or loc_cookie=='none':
                                         loc = 0
	elif type(loc_cookie)==type(""): loc = string.atoi( loc_cookie )
        elif type(loc_cookie)==type(0):  loc = loc_cookie
	if loc == 0: return 1
	else:        return 0

    def format_vol1_header( self, label ):
        if debug_paranoia:  print "driver.format_vol1_header",label
        r = "VOL1"+label
        r = r+ (79-len(r))*' ' + '0'
        if debug_paranoia:  print "return",r,"len", len(r)
        return r

    def format_eov1_header( self, label, cookie=None ):
        if debug_paranoia:  print "driver.format_eov1_header",label
        r = "EOV1"+label
        if cookie: r=r+' '+cookie
        r = r+ (79-len(r))*' ' + '0'
        if debug_paranoia:  print "return",r,"len", len(r)
        return r

    
        
    def check_header( self ):
        extra=None
        if debug_paranoia:  print "check_header"
        try:
            label=self.read(80)
            if debug_paranoia:  print "label=",label,"len(label)=", len(label)
            if len(label)!=80:
                typ,val="INVALID","INVALID"
            else:
                typ=label[:4]
                val=string.split(label[4:])[0]
        except:
            typ,val = None,None
        if debug_paranoia:  print "check_header: return",typ,val,extra
        return typ, val, extra
        
    def read( self, size_bytes ):
	return os.read( self.fd, size_bytes )

    def write( self, buffer ):
	os.write(self.fd, buffer)
	self.remaining_bytes = self.remaining_bytes - len( buffer )
	return None

    def close( self, skip=0 ):
        return os.close(self.fd)

    def loc_compare( self, loc1, loc2 ):
	if loc1==None or loc1=='None' or loc1=='none': loc1 = 0
	if loc2==None or loc2=='None' or loc2=='none': loc2 = 0
	if   loc1 == loc2: rr = 0
	elif loc1 <  loc2: rr = -1
	else: rr = 1
	return rr

    pass


class  FTTDriver(GenericDriver) :
    LOC_SPEC = '%04d_%09d_%07d'		# partition, blk offset, filemarks (arbitrary field widths
    """
     A Fermi Tape Tools driver
    """

    def sw_mount( self, device, blocksize, remaining_bytes, vol_label,
		  eod_cookie ):
	# Get the position from the drive.
	# Should the driver keep the volume label to connect "position"
	# information to??? So, if a particular volume is unbound and then
	# binded again as the next volume, the position could be checked to
	# see if a rewind already occurred.
        self.remaining_bytes = remaining_bytes
	self.blocksize = blocksize	# save blocksize so we do not need as param to open
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
	    time.sleep( 1 )
            FTT.open( device, 'r' )
	    x = x -1
	    pass
	if x == 0:
	    Trace.log( e_errors.INFO, "sw_mount error" )
	    raise SWMountError

	part, block_loc, filenum = self.loc2int(eod_cookie )

        self.statisticsOpen = self.get_allStats(device)

	# make cur_loc_cookie such that an ordered list can be produced
	# (for pnfs) i.e. partition, blk offset, filemarks
	ss =  FTT.get_stats()
	if filenum and block_loc and ss['bloc_loc'] != None:
	    # go to the closest location (that we will know the filenum)
	    cur_bloc = string.atoi( ss['bloc_loc'] )
	    if abs(cur_bloc-block_loc) < cur_bloc:
		# closer to eod (could already be there
		Trace.trace( 19, 'sw_mount going to eod bloc (%s)'%(block_loc,) )
		FTT.locate( block_loc )
		self.cur_loc_cookie = self.int2loc( (part,block_loc,filenum) )
	    else:
		FTT.locate( 0 )
		self.cur_loc_cookie = self.int2loc((0,0,0) )
		pass
	else:
            self.rewind()
	    pass
	FTT.close()
	return None

    def check_header( self ):
        ## Screwing around with the blocksize like this really sucks.
        blocksize=FTT.get_blocksize()
        FTT.set_blocksize(80)
        if debug_paranoia:  print "check_header"
        extra=0
        try:
            label=self.read(80)
            if debug_paranoia:  print "label=",label,"len(label)=", len(label)
            if len(label)!=80:
                typ,val="INVALID","INVALID"
            else:
                typ, val=label[:4],label[4:]
                if ' ' in val:
                    words=string.split(val)
                    if len(words)>2:
                        extra=words[1]
                    val=words[0]

        except: #XXX should be the specific I/O error
            ###This is very dangerous, but I don't know what to do
            ##Catching an I/O error here means that the tape is
            ## new, so we should label it.  But it also could be due to a
            ## drive error!  I don't know how to distinguish these cases.
            typ,val = None,None
        if debug_paranoia:  print "check_header: return",typ,val,extra
        FTT.set_blocksize(blocksize)
        return typ, val, extra
        
        
    def get_allStats( self, device="" ):
        statistics = FTT.get_statsAll()
	if statistics['remain_tape'] == None:
	    rb = self.remaining_bytes
	else:   
	    rb = string.atoi(statistics['remain_tape'])*1024L
        statistics['remaining_bytes'] = rb
	statistics['wr_err'] = self.wr_err
	statistics['rd_err'] = self.rd_err
	statistics['wr_access'] = self.wr_access
	statistics['rd_access'] = self.rd_access
	timeStamp = time.asctime(time.localtime(time.time()))
	statistics['reporttime'] = timeStamp
	result = hostaddr.gethostinfo()
	statistics['hostname'] = result[0]
	statistics['device'] = device
	return statistics

    def offline( self, device ):
	if self.mode == '':
	    FTT.open( device, 'r' )
	    need_cl = 1
	else: need_cl = 0
	# in the months prior to 7-7-99, when "mt offline" was used,
	# every once in a great while "Input/output error" was returned
	# and then, when the command was retried, no error was returned.
	# Now, if any exception occurs, just retry the command.
	# Note: "mt offline" was used when enstore was developed using
	# Linux machines; it did not work when IRIX development started
	# (mt unload); hence we now switch (back) to FTT.unload and
	# code for exceptions.
	x = 2;
	while x:
	    try:
                self.statisticsClose = self.get_allStats() # get statistics before unload
		FTT.unload()
		break
	    except: time.sleep( 1 )
	    x = x - 1
	    pass
	# An exception will occur if the tape is already unloaded.
	# (I could do an FTT.status and check for online 1st, but...
	# I would still have to code for the exception as outlined above)
	# Exception information:
	# the ncr53c8xx driver version 3.1d returns 'Input/output error'
	# the aic7xxx driver version 5.1.10/3.2.4 returns 'No medium found'
	# the IRIX 6.2 driver (on fndaub on 7-7-99) returned 'Resource temporarily unavailable'
	if need_cl: FTT.close()
	return None

    def rewind(self):
        self.cur_loc_cookie=self.int2loc((0,0,0) )# partition, blk offset, filemarks
        r=FTT.rewind()
        return r

    def skip_fm(self, skip):
        p,b,f = self.loc2int(self.cur_loc_cookie)
        FTT.skip_fm(skip) #if this fails, an exception is raised
        f = f+skip
        self.cur_loc_cookie = self.int2loc((p,b,f))
        
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
        #print self.msg.id, self.sem.id
	self.shm.offset( 2, self.sem.id )
	self.shm.offset( 3, self.msg.id )
	try:
            print "XXX:calling FTT.fd_xfer"
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
	p,b,f = self.loc2int(self.cur_loc_cookie )
	ss = FTT.get_stats()  # update block_loc if we can
	if ss['bloc_loc'] != None: b = string.atoi(ss['bloc_loc'])
	# NOTE: I know this makes several times that FTT.get_stats is called!!!
	self.cur_loc_cookie = self.int2loc((p,b,f+1) )
	return None

    def open( self, device, mode ):
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
        drive_mode = FTT.get_mode()
        drive_block_size = 0 # 0 is variable block mode
        dev_name = FTT.set_mode( drive_mode['density'],
	 		         drive_mode['compression'],
			         drive_block_size )
	# verifiy location via FTT.stats???
	return self

    def seek( self, loc_cookie ):
	part, block_loc, filenum = self.loc2int(loc_cookie )
        if debug_paranoia:
            print "seek: part, block, file=", part, block_loc, filenum
            print "seek: cur_loc_cookie=", self.cur_loc_cookie
	# THE "and 0" IN THE NEXT LINE IS TO TEMPORARILY DISABLE FTT.locate
	if block_loc and 0:
	    xx = 2
	    while xx:
		FTT.locate( block_loc )
		ss = FTT.get_stats()
		if string.atoi(ss['bloc_loc']) == block_loc: break
		xx = xx - 1
		pass
	    if xx == 0: raise SeekError
	    pass
	elif filenum != self.loc2int(self.cur_loc_cookie)[2]:
	    # NOTE: when skipping file marks, there must be a file mark to
	    #       skip over. This is why we can not "skip" to the beginning
	    #       of a tape; we must "rewind" to get to BOT
	    #if filenum == 0: FTT.locate( 0 ) # higher probability for problems
	    if filenum == 0:
		FTT.rewind()
		ss = FTT.get_stats()  # block_loc should be 0
		if ss['bloc_loc'] != None:
		    if ss['bloc_loc'] != "0": raise SeekError
		    pass
		else:
		    Trace.log( e_errors.ERROR, "FTT.get_stats - no block_loc" )
		    pass
		pass
	    else:
		skip = filenum - self.loc2int(self.cur_loc_cookie)[2]
		if skip < 0: skip = skip - 1# if neg, make more neg
		FTT.skip_fm( skip )
		if skip < 0:
                    FTT.skip_fm( 1 )
		ss = FTT.get_stats()  # update block_loc if we can
		if ss['bloc_loc'] != None:
		    Trace.trace( 19, 'after seek: bloc_loc=%s'%(ss['bloc_loc'],) )
		    # THE FOLLOWING 'if block_loc" CODE GOES ALONG WITH AN
		    # "and 0" INSERTED IN THE "if block_loc" STATEMENT
		    # ABOVE; IF THERE IS NO "and 0" IN THE "if block_loc"
		    # STATEMENT ABOVE, THE "if block_loc" CODE BELOW
		    # IS BOGUS.
		    if block_loc and block_loc != string.atoi(ss['bloc_loc']):
                        if debug_paranoia: print block_loc, "!=", string.atoi(ss['bloc_loc'])
			raise SeekError
		    block_loc = string.atoi(ss['bloc_loc'])
		    pass
		else:
		    Trace.log( e_errors.ERROR, "FTT.get_stats - no block_loc" )
		    pass
		pass
	    pass
	self.cur_loc_cookie = self.int2loc((part,block_loc,filenum) )
	return None

    def tell( self ):
	p,b,f = self.loc2int(self.cur_loc_cookie )
        if debug_paranoia: print "tell: part, block, file=",p,b,f
	ss = FTT.get_stats()  # update block_loc if we can
	r = ss['bloc_loc']
        if r!= None: b = string.atoi(ss['bloc_loc'])
        else: Trace.log( e_errors.ERROR, "FTT.get_stats - returned None")
	# NOTE: I know this makes several times that FTT.get_stats is called!!!
	self.cur_loc_cookie = self.int2loc((p,b,f) )
        if debug_paranoia: print "tell: return", self.cur_loc_cookie
	return self.cur_loc_cookie
        
    def is_bot( self, loc_cookie ):
        part, block_loc, filenum = self.loc2int(loc_cookie )
	if filenum == 0: return 1
	return 0

    def read( self, size_bytes ):
	return FTT.read( size_bytes )

    def write( self, buffer ):
	FTT.write( buffer )
	self.remaining_bytes = self.remaining_bytes - len( buffer )
	return None


    def close( self, skip=1 ):
        self.statisticsClose = self.get_allStats()
	if self.mode == 'r':
            if skip:
                FTT.skip_fm( 1 )
                # this is making an assumption and should be changed to use
                # FTT.get_stats() (which is broken as of 2-25-99) and setting
                # the block_loc (in addition to the file location)
                p,b,f = self.loc2int(self.cur_loc_cookie )
                self.cur_loc_cookie = self.int2loc( (p,b,f+1) )
                pass
	    pass
	self.mode = ''			# indicate the the device is closed
	return FTT.close()

    def loc_compare( self, loc1, loc2 ):
	# if loc1 greater than loc2 --> return 1
	# if same                   --> return 0
        # otherwise                 --> return -1   (loc1 less than loc2)
	part1, block_loc1, filenum1 = self.loc2int(loc1 )
	part2, block_loc2, filenum2 = self.loc2int(loc2 )
	if (part1,block_loc1,filenum1) == (part2,block_loc2,filenum2): rr = 0
	elif part1 < part2: rr = -1
	elif part1 > part2: rr =  1
	elif block_loc1<=block_loc2 and filenum1<=filenum2: rr = -1
	elif block_loc1>=block_loc2 and filenum1>=filenum2: rr =  1
	else: raise InvalidLocationError
	return rr

    pass




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

    def sw_mount( self, device, blocksize, remaining_bytes, vol_label,
		  eod_cookie ):
        self.time.sleep( 10 )                   # sw_mount time 10 seconds
	RawDiskDriver.sw_mount( self, device, blocksize, remaining_bytes,
				vol_label, eod_cookie )
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

    def fd_xfer( self, fd, siz_bytes, crc_flag=0, crc=0 ):
	# returns (crc); throws exception if wrong no_bytes xferred
	# no crc if crc_flag is 0; adler32 if crc_flag==1
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
				     self.blocksize, crc_flag, crc, self.shm )
	    else:
                __fd__ = os.open( '/dev/null', mode_string_to_int(self.mode) )
		crc = EXfer.fd_xfer( fd, __fd__, siz_bytes,
				     self.blocksize, crc_flag, crc, self.shm )
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

    Usage = "Usage: %s [--in=file] [--disk_out=file] [--tape_out=dev]" % (sys.argv[0],)

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
	fo = open( opt['in'], 'w' )
        fo.write( '12345' )
        fo.close()
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
	hsm_driver.sw_mount( dev, 1024, 50000000, 'test_vol', None )
	
	for dir,buf in (('w',0), ('w',1), ('r',0), ('w',2), ('r',1), ('r',2)):
	    if dir == 'w':
		print '  write invoking open method'
		do = hsm_driver.open( dev, "a+" )# do = "driver object"

		print '     seek to eod_cookie',eod_cookie
		do.seek( eod_cookie )

                buf_loc[buf]=do.tell()
		print '     buf',buf,'loc is',buf_loc[buf]
		
		print "     stats - %s"%(do.get_stats(),)
		print '     write buf', buf, "pos b4 write is",do.tell()
		do.write( buf_str[buf])

		fd = os.open( opt['in'], os.O_RDONLY )
		crc = do.fd_xfer( fd, fsize, 1)
		os.close(fd)
		print '     the crc is',crc
		print "     stats - %s"%(do.get_stats(),)
	    
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
	    
		print "     stats - %s"%(do.get_stats(),)
		print '     read buf', buf, "pos b4 read is",do.tell()
		xx = do.read( len(buf_str[buf]))

		print '     pos after read is',do.tell()
		print "     stats - %s"%(do.get_stats(),)

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


    
