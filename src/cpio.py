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
class Cpio :

    # read  object: needs a method read_block that will read the data, it
    #               has no arguments
    # write object: needs a method write_block that will write the data, it
    #               has 1 argument - the data to be written
    # crc_function: crc's the data, 2 arguments: 1=buffer, 2=initial_crc
    # fast_write:   1 means use EXfer, 0 means use slower python writes
    def __init__(self,read_object, write_object, crc_fun, fast_write=1) :
        self.read_driver = read_object
        self.write_driver = write_object
        self.crc_fun = crc_fun
        self.fast_write = fast_write
	pass

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
	

    # generate an enstore cpio archive: devices must be open and ready
    def write( self, ticket ):
        inode        = ticket['wrapper']['inode']
        mode         = ticket['wrapper']['mode']
        uid          = ticket['wrapper']['uid']
        gid          = ticket['wrapper']['gid']
        mtime        = ticket['wrapper']['mtime']
        filesize     = ticket['wrapper']['size_bytes']
        major        = ticket['wrapper']['major']
        minor        = ticket['wrapper']['minor']
        rmajor       = ticket['wrapper']['rmajor']
        rminor       = ticket['wrapper']['rminor']
        filename     = ticket['wrapper']['pnfsFilename']
        sanity_bytes = ticket["wrapper"]["sanity_size"]

        # generate the headers for the archive and write out 1st one
        format = "new"
        nlink = 1
        header,crc_header,trailer = headers(format, inode, mode, uid,
                                               gid, nlink, mtime, filesize,
                                               major, minor, rmajor, rminor,
                                               filename,0)
        size = len(header)

	# it is assumed that the data size will be greater than sanity_bytes
	# the header is passed thru ETape
	(dat_bytes,dat_crc,san_crc) = EXfer.to_HSM( self.read_driver, self.write_driver,
						    self.crc_fun, sanity_bytes, header )
	# sanity_bytes will be dat_bytes when dat_bytes is less than
	# sanity_bytes.
	if dat_bytes < sanity_bytes:
	    san_bytes = dat_bytes
	    san_crc = dat_crc
	else:
	    san_bytes = sanity_bytes
	    pass
	size = size + dat_bytes

	# need to subtract off these bytes from remaining count if disk driver
	# ftt driver has method that just returns since the byte count is
	#        updated in hardware at end transfer
	self.write_driver.xferred_bytes(size)

	# partial tape block will be in ETape buffer????


        # write out the trailers
        self.write_driver.write_block( trailers(size,crc_header,dat_crc,trailer) )
        sanity_cookie = (san_bytes,san_crc)
        return (dat_bytes, dat_crc, repr(sanity_cookie))


    # read an enstore archive: devices must be ready and open
    def read(self, sanity_cookie="(0,0)") :
        # setup counters
        sanity_bytes, sanity_crc = eval(sanity_cookie)
        dat_crc = 0;  san_crc = 0       # "in progress" (shorter 3-character names) crc's,
        dat_bytes = 0; san_bytes = 0    #          data bytes and sanity bytes read.

        # read the 1st block - assume cpio header is always within (completely) 1st block
        buffer = self.read_driver.read_block()
        if len(buffer) == 0:
            raise errno.errorcode[errno.EINVAL],"Invalid format of cpio "+\
                  "format  Expecting 1st block bytes, but only read 0 bytes"

        # decode the cpio header block
        try:
            data_offset, data_bytes, data_name = decode( buffer )
            # take care of files where sanity size > data size
            if sanity_bytes > data_bytes:
                self.enprint("ERROR: sanity_bytes="+repr(sanity_bytes)+ \
	                     " and data_bytes="+repr(data_bytes)+ \
                             " setting sanity_bytes=data_bytes")
                sanity_bytes = data_bytes
        except errno.errorcode[errno.EINVAL]:
            # for now, just send the data back to the user, as read
            bad = str(sys.exc_info()[1]); self.enprint(bad)
            while 1:
                self.write_driver.write_block( buffer )
                buffer = self.read_driver.read_block()
                if len(buffer) == 0: return (-1,-1,-1,bad)

        buffer = buffer[data_offset:]   # just dealing with data now
        buffer_len = len( buffer )

        # now continue with writing/reading
        while 1:
            # we need to crc the data
            if buffer_len < data_bytes - dat_bytes:
                dat_bytes = dat_bytes + buffer_len
                dat_crc = self.crc_fun( buffer, dat_crc )
                self.write_driver.write_block( buffer )
            else:
                dat_end   = data_bytes - dat_bytes
                dat_bytes = data_bytes  # read all the data (finished), but still may be doing sanity
                dat_crc = self.crc_fun(        buffer[:dat_end], dat_crc )  # so don't break here
                self.write_driver.write_block( buffer[:dat_end] )
                # setup for trailer, but do not index past padd now as we may
                # be at the edge of the buffer
                padd = (4-(dat_bytes%4)) %4
                trailer = buffer[dat_end:] # may be null (if right at edge)

            # look at first part of file to make sure it is right file
            if san_bytes < sanity_bytes:
                # logic is same as data crc
                if buffer_len < sanity_bytes - san_bytes:
                    san_bytes = san_bytes + buffer_len
                    san_crc = self.crc_fun(buffer,san_crc)
                else:
                    san_end = sanity_bytes - san_bytes
                    san_bytes = sanity_bytes # done with sanity check
                    san_crc = self.crc_fun( buffer[:san_end], san_crc )
                    if san_crc != sanity_crc:
                        raise IOError, "Sanity Mismatch, read"+repr(san_crc)+\
                              " but was expecting"+repr(sanity_crc)
                # now check the case where we were told more sanity_bytes than data_bytes
                if san_bytes > dat_bytes:
                    self.enprint(san_bytes+" "+repr(sanity_bytes))
                    self.enprint(dat_bytes+" "+repr(data_bytes))
                    self.enprint(buffer_len)
                    raise "TILT - coding error!"

            if dat_bytes == data_bytes: break

            # continue with next read
            buffer = self.read_driver.read_block()
            buffer_len = len(buffer)
            if buffer_len == 0:
                raise errno.errorcode[errno.EINVAL],"Invalid format of cpio "+\
                      "format  Expecting "+ repr(data_size)+" bytes, but "+\
                      "only read"+repr(size)+" bytes"

        # now read the crc file - just read to end of data and then decode
        while 1  :
            buffer = self.read_driver.read_block()
            buffer_len = len(buffer)
            if buffer_len == 0: break
            trailer =  trailer + buffer

        recorded_crc = encrc( trailer[padd:] )
        if recorded_crc != dat_crc :
            raise IOError, "CRC Mismatch, read "+repr(dat_crc)+\
                  " but was expecting "+repr(recorded_crc)

        return (dat_bytes, dat_crc)


###############################################################################
# cpio support functions
#

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
                generic_cs.enprint("Mode is invalid, setting to "+\
	                           repr(jonmode)+" so cpio valid")
            else :
                jonmode = mode
            # make all filenames relative - strip off leading slash
            if fname[0] == "/" :
                fname = fname[1:]
            head = \
                 "070701" +\
                 "%08x" % inode +\
                 "%08x" % jonmode +\
                 "%08x" % uid +\
                 "%08x" % gid +\
                 "%08x" % nlink +\
                 "%08x" % mtime +\
                 "%08x" % fsize +\
                 "%08x" % major +\
                 "%08x" % minor +\
                 "%08x" % rmajor +\
                 "%08x" % rminor +\
                 "%08x" % int(len(fname)+1) +\
                 "%08x" % crc +\
                 "%s\0" % fname
            pad = (4-(len(head)%4)) %4
            heads.append(head + "\0"*pad)

        # create the trailer as well
        heads.append("070701"   +\
                     "00000000" +\
                     "00000000" +\
                     "00000000" +\
                     "00000000" +\
                     "00000001" +\
                     "00000000" +\
                     "00000000" +\
                     "00000000" +\
                     "00000000" +\
                     "00000000" +\
                     "00000000" +\
                     "0000000b" +\
                     "00000000" +\
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
        return("\0"*padd +
               head_crc + "%08x" % data_crc + "\0"*padc +
               trailer + "\0"*padt )


# given a buffer pointing to beginning of header, return:
#    offset to real data, data size, filename,
def decode( buffer ):
        # only 2 cpio formats allowed
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
        data_size = string.atoi(buffer[54:62],16)
        filename = buffer[110:110+filename_size-1]
        return (data_offset, data_size, filename)

# given a buffer pointing to beginning of header, return crc
def encrc( buffer ):
        offset,size,name = decode(buffer)
        # We have switched to 32 bit crcs.  This means that string.atoi now
        # causes an overflow when the crc value has the sign bit set.  The most
        # obvious solution of going to atol doesn't work.  The problem is that
        # python converts the number to a python-long, which can more than 32 bits
        # wide.  This effectively means we are using unsigned multi-precision numbers.
        # Unfortunately, the crc routines calculate their value to a 32 bit unsigned
        # number --> this causes the atol value and the crc calculated value to be
        # thought to be different when in fact they are the same.  I had 2 choices -
        # either use long casts everywhere the crc is calculated or force this crc
        # to be a 32 bit number.  The exec line causes the crc to be a 32 bit number,
        # exactly as it was stored.
        #return string.atol(buffer[offset:offset+8],16)
        exec("crc=0x"+buffer[offset:offset+8])
        return crc

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
    wrapper = Cpio(fin,fout,ECRC.ECRC,fast_write)

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

    wrapper = Cpio(fin,fout,ECRC.ECRC)
    (read_size, read_crc) = wrapper.read(sanity_cookie)
    generic_cs.enprint("cpio.read returned: size: "+repr(read_size)+" crc: "+\
	               repr(read_crc))

    fin.close()
    fout.close()

    if read_size != size :
        raise IOError,"Size ERROR: Read "+repr(read_size)+" bytes, wrote "\
              +repr(size)+" bytes"
