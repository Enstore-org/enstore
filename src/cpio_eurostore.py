###############################################################################
#
#
# system imports
import os
import sys
import stat
import errno
import string
import time
import traceback
import types

# enstore imports
import EXfer
import Trace
import checksum
import e_errors

"""
cpio euroctore format.
This format is essentially the same as for cpio odc. Differences:
1. Use pnfs id as file name
2. Put basename of the real filename as the name of the 2nd archive
3. Set c_nlink to 2 in the 1st archive

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

To make cpio archives on unix:
       echo "pnfs_enstore_airedale_o1
             pnfs_enstore_airedale_o1.encrc" |cpio -ov -H newc > archive

To list them: cpio -tv < archive
To extract:   cpio -idmv < archive

"""


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
	Trace.log(e_errors.INFO,
                  "Mode "+repr(mode)+ " is invalid, setting to "+\
                  repr(jonmode)+" so cpio valid")
    else :
	jonmode = mode

    # make all filenames relative - strip off leading slash
    if fname[0] == "/" :
	fname = fname[1:]
    dev = makedev(major, minor)
    rdev = makedev(rmajor, rminor)

    header = "070707%06o%06lo%06lo%06lo%06lo%06lo%06o%011lo%06lo%011lo%s\0" % \
             ( dev & 0xffff, inode & 0xffff, jonmode & 0xffff,
               uid & 0xffff, gid & 0xffff, nlink & 0xffff,
               rdev & 0xffff, mtime,(len(fname)+1)&0xffff,
               fsize, fname)
    return header


# generate the enstore cpio "trailers"
def trailers(blocksize, siz, header2, trailer):
        size = siz

        size = size + len(header2)
	size = size + len(trailer)
        pad = (blocksize-(size%blocksize)) % blocksize


        # ok, send it back to so he can write it out

        return(header2 + trailer + "\0"*int(pad) )


###############################################################################

             

class Wrapper :

    def sw_mount( self, driver, info ):
	return

    # generate an enstore cpio archive: devices must be open and read
    def write_pre_data( self, driver, info ):
	file_id      = 0  # set it to 0 as does not have any meaning to us
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
	id = "0"
        header1 = create_header(inode, mode, uid, gid, 2, mtime, self.filesize,
				major, minor, rmajor, rminor, "0")
	self.header2 = create_header(inode, mode, uid, gid, 2, mtime, 
				     0, major, minor, rmajor, 
				     rminor, filename)
	self.trailer = create_header(0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 
				     "TRAILER!!!")
	self.header_size = len(header1)
	driver.write(header1)

	return


    def write_post_data( self, driver, crc ):

	try:
	    blocksize = self.blocksize
	except:
	    blocksize = 512
	size = self.header_size + self.filesize

        driver.write( trailers(blocksize, size, self.header2, self.trailer) )
        return


    def read_pre_data( self, driver, info ):
	# The pre data is one cpio header (including the pad).

	# 1st read the constant length part
	header = driver.read(76)

	# determine/save info
	self.magic = header[0:6]
	
	self.pnfs_filename_size = string.atoi( header[59:65], 8 )
	self.file_size   = string.atoi( header[65:76], 8 )

	self.header_size = 76+self.pnfs_filename_size

	# now just index to first part of real data
	buffer = driver.read( self.pnfs_filename_size )
	return


    def read_post_data( self, driver, info ):
	self.header2 = driver.read(76)
	self.filename_size = string.atoi( self.header2[59:65], 8 )
	self.filename = driver.read(self.filename_size)[:-1] # kill trailing
                                                             ##NUL byte
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
	

# shamelessly stolen from python's posixfile.py
class DiskDriver:
    states = ['open', 'closed']

    # Internal routine
    def __del__(self):
        self._file_.close()

    # Initialization routines
    def open(self, name, mode='r', bufsize=-1):
        import __builtin__
        return self.fileopen(__builtin__.open(name, mode, bufsize))

    # Initialization routines
    def fileopen(self, file):
        if type(file) != types.FileType:
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
    def read(self,size):
        return self._file_.read(size)

    # this is the name fo the funciton that the wrapper uses to write
    def write(self,buffer):
        return self._file_.write(buffer)

if __name__ == "__main__" :
    import sys
    import getopt
    import Devcodes

    options = ["create","extract"]
    optlist,args=getopt.getopt(sys.argv[1:], '', options)
    (opt,val) = optlist[0]
    if not optlist:
	print "usage: cpio_eurostore" + " <"+repr(options)+"> infile outfile"
	sys.exit(1)

    if not (opt == "--create" or opt == "--extract"):
	print "usage: cpio_eurostore" + " <"+repr(options)+"> infile outfile"
	sys.exit(1)

    fin = DiskDriver()
    fin.open(args[0],"r")
    fout = DiskDriver()
    fout.open(args[1],"w")

    wrapper = Wrapper()
	
    if opt == "--create":
	statb = os.fstat(fin.fileno())
	if not stat.S_ISREG(statb[stat.ST_MODE]) :
	    raise errno.errorcode[errno.EINVAL],\
		  "Invalid input file: can only handle regular files"
	fast_write = 0 # needed for testing
	dev_dict = Devcodes.MajMin(fin._file_.name)
	major = dev_dict["Major"]
	minor = dev_dict["Minor"]
	rmajor = 0
	rminor = 0
	sanity_bytes = 0

	info = {'inode'       : statb[stat.ST_INO],
		'mode'        : statb[stat.ST_MODE],
		'uid'         : statb[stat.ST_UID],
		'gid'         : statb[stat.ST_GID],
		'mtime'       : statb[stat.ST_MTIME],
		'size_bytes'  :  statb[stat.ST_SIZE],
		'major'       : major,
		'minor'       : minor,
		'rmajor'      : rmajor,
		'rminor'      : rminor,
		'pnfsFilename': fin._file_.name,
		'sanity_size' : sanity_bytes
		}
	wrapper.write_pre_data(fout, info)
	buf = fin.read()
	fout.write(buf)
	wrapper.write_post_data(fout, 0)

    elif opt == "--extract":
	wrapper.read_pre_data(fin, None)
	print "FILE SIZE", wrapper.file_size
	if  wrapper.file_size > 0:
	    buf = fin.read(wrapper.file_size)
	    wrapper.read_post_data(fin,None)
	    print "file_name", wrapper.filename
	    fout.write(buf)

    fin.close()
    fout.close()









