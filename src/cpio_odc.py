###############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
import os
import sys
import stat
import errno
import string
import time
import traceback

# enstore imports
import generic_cs
import EXfer
import Trace
import ECRC

"""

cpio odc format

Offet Field Name   Length in Bytes Notes
0     c_magic      6               070707
6     dev          6
12    c_ino        6
18    c_mode       6
24    c_uid        6
30    c_gid        6
36    c_nlink      6
42    rdev         6
48    c_mtime      11
59    c_namesize   6               count includes terminating NUL in pathname
65    c_filesize   11               must be 0 for FIFOs and directories

76    filename \0
      long word padding

To make cpio archives on unix:
       echo "pnfs_enstore_airedale_o1
             pnfs_enstore_airedale_o1.encrc" |cpio -ov -H newc > archive

To list them: cpio -tv < archive
To extract:   cpio -idmv < archive

"""
class Wrapper :

    def sw_mount( self, driver, info ):
	return

    # generate an enstore cpio archive: devices must be open and ready
    def write_pre_data( self, driver, info ):
        inode        = info['inode']
        mode         = info['mode']
        uid          = info['uid']
        gid          = info['gid']
        mtime        = info['mtime']
        self.filesize     = info['size_bytes']
        major        = info['major']
        minor        = info['minor']
        rmajor       = info['rmajor']
        rminor       = info['rminor']
        filename     = info['pnfsFilename']
        sanity_bytes = info['sanity_size']

        # generate the headers for the archive and write out 1st one
        nlink = 1
        header, self.trailer = headers(inode, mode, uid, gid, nlink, mtime,
				       self.filesize, major, minor, rmajor, rminor,
				       filename)
	self.header_size = len( header )
	driver.write( header )
	return


    def write_post_data( self, driver, crc ):

	try:
	    blocksize = self.blocksize
	except:
	    blocksize = 512
	size = self.header_size + self.filesize

        driver.write( trailers(blocksize, size,self.trailer) )
        return


    def read_pre_data( self, driver, info ):
	# The pre data is one cpio header (including the pad).

	# 1st read the constant length part
	header = driver.read(76)

	# determine/save info
	self.magic = header[0:6]
	
	self.filename_size = string.atoi( header[59:65], 8 )
	self.file_size   = string.atoi( header[65:76], 8 )

	self.header_size = 76+self.filename_size

	# now just index to first part of real data
	buffer = driver.read( self.filename_size )
	return


    def read_post_data( self, driver, info ):
	trailer = driver.read(76)
	magic = trailer[0:6]
	if magic != self.magic:
	    raise IOError, "header magic number " + repr(self.magic) +\
		  "does not match trailer magic number " + repr(magic)
	trailer_name_size = string.atoi( trailer[59:65], 8 )
	trailer_size   = string.atoi( trailer[65:76], 8 )
	if trailer_name_size != 11:
	    raise IOError, "Wrong trailer name size " + repr(trailer_name_size)
	trailer_name = driver.read(trailer_name_size)
	if trailer_name != "TRAILER!!!\0":
	   raise IOError, "Wrong trailer name " + repr(trailer_name) + \
		 "Must be null terminated TRAILER!!!"

	return
	

###############################################################################
# cpio support functions
#

# create device from major and minor
def makedev(major, minor):
    return (((major) << 8) | (minor))

# extract major number
def extract_major(device):
    return (((device) >> 8) & 0xff)

# extract minor number
def extract_minor(device):
    return ((device) & 0xff)

# create header
def create_header(inode, mode, uid, gid, nlink, mtime, filesize,
             major, minor, rmajor, rminor, filename):
    
    # files greater than 2  GB are just not allowed right now
    max = 2**30-1+2**30
    if filesize > max :
	raise errno.errorcode[errno.EOVERFLOW],"Files are limited to "\
	      +repr(max) + " bytes and your "+filename+" has "\
	      +repr(filesize)+" bytes"
    fname = filename
    fsize = filesize
    # set this dang mode to something that works on all machines!
    if ((mode & 0777000) != 0100000) & (filename != "TRAILER!!!"):
	jonmode = 0100664
	generic_cs.enprint("Mode "+repr(mode)+ " is invalid, setting to "+\
			   repr(jonmode)+" so cpio valid")
    else :
	jonmode = mode

    # make all filenames relative - strip off leading slash
    if fname[0] == "/" :
	fname = fname[1:]
    dev = makedev(major, minor)
    rdev = makedev(rmajor, rminor)
    header = \
	   "070707" +\
	   "%06o"   % (dev & 0xffff) +\
	   "%06lo"  % (inode & 0xffff) +\
	   "%06lo"  % (jonmode & 0xffff) +\
	   "%06lo"  % (uid & 0xffff) +\
	   "%06lo"  % (gid & 0xffff) +\
	   "%06lo"  % (nlink & 0xffff) +\
	   "%06o"   % (rdev & 0xffff) +\
	   "%011lo" % mtime +\
	   "%06lo"  % ((int(len(fname)+1)) & 0xffff) +\
	   "%011lo" % fsize +\
	   "%s\0" % fname
    return header


# create 2 headers (1 for data file and 1 for crc file) + 1 trailer
def headers( inode, mode, uid, gid, nlink, mtime, filesize,
             major, minor, rmajor, rminor, filename):
	dev = makedev(major, minor)
	rdev = makedev(rmajor, rminor)
	head = create_header(inode, mode, uid, gid, nlink, mtime, filesize,
             major, minor, rmajor, rminor, filename)

        # create the trailer as well
	trailer = create_header(0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, "TRAILER!!!")

        return head, trailer

# generate the enstore cpio "trailers"
def trailers(blocksize, siz, trailer):
        size = siz

        size = size + len(trailer)
        padt = (blocksize-(siz%blocksize)) % blocksize

        # ok, send it back to so he can write it out
        return(trailer + "\0"*padt )


###############################################################################


# shamelessly stolen from python's posixfile.py
class DiskDriver:
    states = ['open', 'closed']

    # Internal routine
    def __repr__(self):
        file = self._file_
        return "<%s DiskDriver '%s', mode '%s' at %s>" % \
               (self.states[file.closed], file.name, file.mode,
                hex(id(self))[2:])

    # Internal routine
    def __del__(self):
        self._file_.close()

    # Initialization routines
    def open(self, name, mode='r', bufsize=-1):
        import __builtin__
        return self.fileopen(__builtin__.open(name, mode, bufsize))

    # Initialization routines
    def fileopen(self, file):
        if repr(type(file)) != "<type 'file'>":
            raise TypeError, 'DiskDriver.fileopen() arg must be file object'
        self._file_  = file
        # Copy basic file methods
        for method in file.__methods__:
            setattr(self, method, getattr(file, method))
        return self

    #
    # New methods
    #

    # this is the name of the function that the wrapper uses to read
    def read_block(self):
        blocksize = 2**16
        return self.read(blocksize)

    # this is the name fo the funciton that the wrapper uses to write
    def write_block(self,buffer):
        return self.write(buffer)

# Public routine to obtain a diskdriver object
def diskdriver_open(name, mode='r', bufsize=-1):
    return DiskDriver().open(name, mode, bufsize)



if __name__ == "__main__" :
    import sys
    import Devcodes
    Trace.init("Cpio")

    fin  = diskdriver_open(sys.argv[1],"r")
    fout = diskdriver_open(sys.argv[2],"w")

    statb = os.fstat(fin.fileno())
    if not stat.S_ISREG(statb[stat.ST_MODE]) :
        raise errno.errorcode[errno.EINVAL],\
              "Invalid input file: can only handle regular files"

    fast_write = 0 # needed for testing
    wrapper = Wrapper()

    dev_dict = Devcodes.MajMin(fin._file_.name)
    major = dev_dict["Major"]
    minor = dev_dict["Minor"]
    rmajor = 0
    rminor = 0
    sanity_bytes = 0

    ticket = {'wrapper':{},'unifo':{}}
    ticket['wrapper']['inode']       = statb[stat.ST_INO]
    ticket['wrapper']['mode']        = statb[stat.ST_MODE]
    ticket['wrapper']['uid']         = statb[stat.ST_UID]
    ticket['wrapper']['gid']         = statb[stat.ST_GID]
    ticket['wrapper']['mtime']       = statb[stat.ST_MTIME]
    ticket['wrapper']['size_bytes']  = statb[stat.ST_SIZE]
    ticket['wrapper']['major']       = major
    ticket['wrapper']['minor']       = minor
    ticket['wrapper']['rmajor']      = rmajor
    ticket['wrapper']['rminor']      = rminor
    ticket['wrapper']['pnfsFilename']= fin._file_.name
    ticket["wrapper"]["sanity_size"] = sanity_bytes
    (size,crc,sanity_cookie) = wrapper.write( ticket )
    generic_cs.enprint("Cpio.write returned: size: "+repr(size)+" crc: "+\
	               repr(crc)+" sanity_cookie: "+repr(sanity_cookie))

    fin.close()
    fout.close()

    if size != statb[stat.ST_SIZE] :
        raise IOError,"Size ERROR: Wrote "+repr(size)+" bytes, file was "\
              +repr(statb[stat.ST_SIZE])+" bytes long"




    fin  = diskdriver_open(sys.argv[2],"r")
    fout = diskdriver_open(sys.argv[1]+".copy","w")

    wrapper1 = Wrapper()
    (read_size, read_crc) = wrapper1.read(sanity_cookie)
    generic_cs.enprint("cpio.read returned: size: "+repr(read_size)+" crc: "+\
	               repr(read_crc))

    fin.close()
    fout.close()

    if read_size != size :
        raise IOError,"Size ERROR: Read "+repr(read_size)+" bytes, wrote "\
              +repr(size)+" bytes"









