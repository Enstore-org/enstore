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
import types

# enstore imports
import EXfer
import Trace
import checksum
import e_errors

"""

CPIO is not ideal for our purposes because it cannot handle files bigger than
2 GB. However, it is nice to conform to something, and CPIO is a good deal
simpler than tar, _and_ handles arbitarily long file names.

We would have to, by convention, store larger files as a number of files
withing an archvie, I think that this is not a bad idea, and propose the
following convention:

    If a file is less than 2 GB, store as if we had run CPIO.

    If a file is greater than 2 GB, the first cpio file has a name dictated
       by convention -- "...,Appended", body of file says how many extents
       follow. Segments are 2**31-1  bytes, with the last segment
       being fractional.

       File name recorded with the segment is original name, followed by ,n
       with n begin decimal and starting with 0.  User can use dd to paste
       these things together when recovering from a raw tape.

Before the trailer, we always add an 8 byte file that has the crc value (in
ascii hex) of the data stored in the arvhive. The crc is cumulative over all
files if it is bigger than 2 GB.

Portable and CRC cpio formats:

   Each file has a 110 byte header,
   a variable length, NUL terminated filename,
   and variable length file data.
   A header for a filename "TRAILER!!!" indicates the end of the archive.

   All the fields in the header are ISO 646 (approximately ASCII) strings
   of hexadecimal numbers, left padded, not NUL terminated.

Offet Field Name   Length in Bytes Notes
0     c_magic      6               070701 for new portable format
                                   070702 for CRC format
6     c_ino        8
14    c_mode       8
22    c_uid        8
30    c_gid        8
38    c_nlink      8
46    c_mtime      8
54    c_filesize   8               must be 0 for FIFOs and directories
62    c_maj        8
70    c_min        8
78    c_rmaj       8               only valid for chr and blk special files
86    c_rmin       8               only valid for chr and blk special files
94    c_namesize   8               count includes terminating NUL in pathname
102   c_chksum     8               0 for new portable format; for CRC format
                                   the sum of all the bytes in the file
110   filename \0
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
        self.filesize= info['size_bytes']
        major        = info['major']
        minor        = info['minor']
        rmajor       = info['rmajor']
        rminor       = info['rminor']
        filename     = info['pnfsFilename']
        sanity_bytes = info['sanity_size']

        # generate the headers for the archive and write out 1st one
        format = "new"
        nlink = 1
        header,self.crc_header,self.trailer = headers( format, inode, mode,
						       uid, gid, nlink, mtime,
						       self.filesize, major,
						       minor, rmajor, rminor,
						       filename, 0 )
	self.header_size = len( header )
	driver.write( header )
	return


    def write_post_data( self, driver, crc ):

	size = self.header_size + self.filesize

        driver.write( trailers(size,self.crc_header,crc,self.trailer) )
        return


    def read_pre_data( self, driver, info ):
	# The pre data is one cpio header (including the pad).

	# 1st read the constant length part
	header = driver.read( 110 )

	# determine/save info
	self.file_size   = string.atoi( header[54:62], 16 )

	self.filename_size = string.atoi( header[94:102], 16 )
	header_pad = (4-((110+self.filename_size)%4)) % 4
	self.header_size = 110+self.filename_size+header_pad

	# now just index to first part of real data
	buffer = driver.read( self.filename_size+header_pad )
	return


    def read_post_data( self, driver, info ):
	dat_crc = info['data_crc']
	data_size = self.header_size + self.file_size
	pad_data = (4-(data_size%4)) % 4
	pad_crc  = (4-((110+self.filename_size+4+8)%4)) % 4
	# read the rest (padd), (crc_trailer) and trailer
	length = pad_data + (110+self.filename_size+3+8+pad_crc) + (110+0xb)
	trailer = driver.read( length )

	recorded_crc = encrc( trailer[pad_data:] )
        if recorded_crc != dat_crc :
            raise IOError, "CRC Mismatch, read "+repr(dat_crc)+\
                  " but was expecting "+repr(recorded_crc)
	return
	

###############################################################################
# cpio support functions
#

# convert int or long int to 8byte (zero-padded) hex string

def hex8(x):
    s=hex(x)[2:]
    if type(x)==type(1L): s=s[:-1]
    l = len(s)
    if l>8:
        raise "Overflow Error", x
    return '0'*(8-l)+s
    

# create 2 headers (1 for data file and 1 for crc file) + 1 trailer
def headers( format,            # either "new" or "CRC"
             inode, mode, uid, gid, nlink, mtime, filesize,
             major, minor, rmajor, rminor, filename, crc ):
        # only 2 cpio formats allowed
        if format == "new" :
            magic = "070701"
        elif format == "CRC"  :
            magic = "070702"
        else :
            raise errno.errorcode[errno.EINVAL],"Invalid format: "+ \
                  repr(format)+" only \"new\" and \"CRC\" are valid formats"

        # files greater than 2  GB are just not allowed right now
        max = 2**30-1+2**30
        if filesize > max :
            raise errno.errorcode[errno.EOVERFLOW],"Files are limited to "\
                  +repr(max) + " bytes and your "+filename+" has "\
                  +repr(filesize)+" bytes"

        # create the header for the data file and a header for a crc file
        heads = []
        for h in [(filename,filesize), (filename+".encrc",8)] :
            fname = h[0]
            fsize = h[1]
            # set this dang mode to something that works on all machines!
            if (mode & 0777000) != 0100000 :
                jonmode = 0100664
                Trace.log(e_errors.INFO, "Mode is invalid, setting to "+\
                          repr(jonmode)+" so cpio valid")
            else :
                jonmode = mode
            # make all filenames relative - strip off leading slash
            if fname[0] == "/" :
                fname = fname[1:]
            head = "070701" + string.join(map(hex8,
                                              [inode,jonmode,uid,gid,nlink,mtime,
                                               fsize,major,minor,rmajor,rminor,
                                               len(fname)+1, crc]), '') + "%s\0" % fname

            pad = (4-(len(head)%4)) %4
            heads.append(head + "\0"*int(pad))

        # create the trailer as well
        heads.append("070701"   
                     "00000000" 
                     "00000000" 
                     "00000000" 
                     "00000000" 
                     "00000001" 
                     "00000000" 
                     "00000000" 
                     "00000000" 
                     "00000000" 
                     "00000000" 
                     "00000000" 
                     "0000000b" 
                     "00000000" 
                     "TRAILER!!!\0")

        return heads


# generate the enstore cpio "trailers"
def trailers( siz, head_crc, data_crc, trailer ):
        size = siz

        # first need to pad data
        padd = (4-(size%4)) %4
        size = size + padd

        # next is header for crc file, 8 bytes of crc info, and padding
        size = size + len(head_crc) + 8
        padc = (4-(size%4)) %4
        size = size+padc

        # finally we have the trailer and the overall cpio padding
        size = size + len(trailer)
        padt = (512-(size%512)) % 512

        # ok, send it back to so he can write it out
        return("\0"*int(padd) +
               head_crc + hex8(data_crc) + "\0"*int(padc) +
               trailer + "\0"*int(padt) )


# given a buffer pointing to beginning of header, return crc
def encrc( buffer ):
        magic = buffer[0:6]
        if magic == "070701" or  magic == "070702" :
            pass
        else :
            raise errno.errorcode[errno.EINVAL],"Invalid format: "+ \
                  repr(magic)+ " only \"070701\" and \"070702\" "+\
                  "are valid formats"

        filename_size = string.atoi(buffer[94:102],16)
        data_offset = 110+filename_size
        data_offset =data_offset + (4-(data_offset%4))%4
        # We have switched to 32 bit crcs.
        # We are using Python long integers to represent them so they don't go negative
        crc = string.atol(buffer[data_offset:data_offset+8],16)
        return crc

###############################################################################


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
	    wrapper.read_post_data(fin,{'data_crc':0})
	    fout.write(buf)

    fin.close()
    fout.close()
