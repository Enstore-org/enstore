import os
import sys
import stat
import errno
import binascii
import string

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
"""
class cpio :

    # read  object: needs a method read_block that will read the data, it
    #               has no arguments
    # write object: needs a method write_block that will write the data, it
    #               has 1 argument - the data to be written
    # crc_function: crc's the data, 2 arguments: 1=buffer, 2=initial_crc
    def __init__(self,read_object, write_object, crc_fun) :
        self.read_driver = read_object
        self.write_driver = write_object
        self.crc_fun = crc_fun

    # create 2 headers (1 for data file and 1 for crc file) + 1 trailer
    def headers(self,format,            # either "new" or "CRC"
                  inode, mode, uid, gid, nlink, mtime, filesize,
                  major, minor, rmajor, rminor, filename, crc) :
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
                print "Mode is invalid, setting to",jonmode, "so cpio valid"
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
    def trailers(self, siz, head_crc, data_crc, trailer) :
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
    def decode(self,buffer):
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
    def encrc(self,buffer):
        offset,size,name = self.decode(buffer)
        return string.atoi(buffer[offset:offset+8],16)



    # generate an enstore cpio archive: devices must be open and ready
    def write(self, inode, mode, uid, gid, mtime, filesize,
              major, minor, rmajor, rminor, filename, sanity_bytes=0) :

        # generate the headers for the archive and write out 1st one
        format = "new"
        nlink = 1
        header,head_crc,trailer = self.headers(format, inode, mode, uid,
                                               gid, nlink, mtime, filesize,
                                               major, minor, rmajor, rminor,
                                               filename,0)
        size = len(header)
        apply(self.write_driver.write_block,(header,))

        # now read input and write it out
        sanity_crc = 0
        sanity_size = 0
        data_crc = 0
        data_size = 0
        while 1:
            b = apply(self.read_driver.read_block,())
            length = len(b)
            if length == 0 :
                break
            size = size + length
            data_size = data_size + length
            # we need a complete crc of the data in the file
            data_crc = apply(self.crc_fun,(b,data_crc))

            # we also need a "sanity" crc of 1st sanity_bytes of data in file
            # so, we crc the 1st portion of the data twice (should be ok)
            if sanity_size < sanity_bytes :
                if sanity_size + length <= sanity_bytes :
                    sanity_end = length
                    sanity_size = sanity_size+length
                else :
                    sanity_end = sanity_bytes - sanity_size
                    sanity_size = sanity_bytes
                sanity_crc = apply(self.crc_fun,(b[0:sanity_end],sanity_crc))

            apply(self.write_driver.write_block,(b,))

        # write out the trailers
        apply(self.write_driver.write_block,
              (self.trailers(size,head_crc,data_crc,trailer),))
        sanity_cookie = (sanity_bytes,sanity_crc)
        return (data_size, data_crc, repr(sanity_cookie))


    # read an enstore archive: devices must be ready and open
    def read(self, sanity_cookie="(0,0)") :

        # setup counters/flags
        sanity_bytes,sanity_crc=eval(sanity_cookie)
        san_crc = 0
        data_crc = 0
        data_size = 0
        sanity_size = 0
        first = 1
        size = 0
        parse_header = 1

        # now read input file and write it out
        while size < data_size or first:
            first = 0
            offset = 0
            buffer = apply(self.read_driver.read_block,())
            length = len(buffer)
            if length == 0 :
                raise errno.errorcode[errno.EINVAL],"Invalid format of cpio "+\
                      "format  Expecting "+ repr(data_size)+" bytes, but "+\
                      "only read"+repr(size)+" bytes"

            # decode the cpio header block
            if parse_header :
                try:
                    data_offset, data_size, data_name = self.decode(buffer)
                except errno.errorcode[errno.EINVAL]:
                    # for now, just send the data back to the user, as read
                    bad = str(sys.exc_info()[1])
                    print bad
                    while 1:
                        apply(self.write_driver.write_block,(buffer,))
                        buffer = apply(self.read_driver.read_block,())
                        length = len(buffer)
                        if length == 0 :
                            return (-1,-1,-1,bad)
                parse_header = 0
                offset = data_offset

            # we need to crc the data
            if size + length - offset <= data_size :
                data_end = length
                size = size + length - offset
            else :
                data_end = data_size - size + offset
                size = data_size
                next = data_offset + size
                padd =  (4-(next%4)) %4
                trailer = buffer[data_end+padd:]
            data_crc = apply(self.crc_fun,(buffer[offset:data_end],data_crc))

            # look at first part of file to make sure it is right file
            if sanity_size < sanity_bytes and size!=data_size:
                if sanity_size + length - offset <= sanity_bytes :
                    sanity_end = length
                    sanity_size = sanity_size+length-offset
                else :
                    sanity_end = sanity_bytes - sanity_size + offset
                    sanity_size = sanity_bytes
                    san_crc = apply(self.crc_fun,(buffer[offset:sanity_end]\
                                                     ,san_crc))
                if sanity_size == sanity_bytes :
                    if sanity_crc != sanity_crc:
                        raise IOError, "Sanity Mismatch, read"+repr(san_crc)+\
                              " but was expecting"+repr(sanity_crc)
                elif sanity_size > sanity_bytes :
                    # need sanity_size, or less,  bytes in sanity_crc
                    print sanity_size,sanity_bytes
                    print size,data_size
                    print length,offset
                    raise "TILT - coding error!"

            # write the data
            apply(self.write_driver.write_block,(buffer[offset:data_end],))

        # now read the crc file - just read to end of data and then decode
        while 1  :
            buffer = apply(self.read_driver.read_block,())
            length = len(buffer)
            if length == 0 :
                break
            trailer =  trailer + buffer

        recorded_crc = self.encrc(trailer)
        if recorded_crc != data_crc :
            raise IOError, "CRC Mismatch, read"+repr(san_crc)+\
                  " but was expecting"+repr(sanity_crc)

        return (data_size, data_crc)

# shamelessly stolen from python's posixfile.py
class diskdriver:
    states = ['open', 'closed']

    # Internal routine
    def __repr__(self):
        file = self._file_
        return "<%s diskdriver '%s', mode '%s' at %s>" % \
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
            raise TypeError, 'diskdriver.fileopen() arg must be file object'
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
    return diskdriver().open(name, mode, bufsize)



if __name__ == "__main__" :

    import sys
    import Devcodes

    fin  = diskdriver_open(sys.argv[1],"r")
    fout = diskdriver_open(sys.argv[2],"w")

    statb = os.fstat(fin.fileno())
    if not stat.S_ISREG(statb[stat.ST_MODE]) :
        raise errno.errorcode[errno.EINVAL],\
              "Invalid input file: can only handle regular files"

    wrapper = cpio(fin,fout,binascii.crc_hqx)

    dev_dict = Devcodes.MajMin(fin._file_.name)
    major = dev_dict["Major"]
    minor = dev_dict["Minor"]
    rmajor = 0
    rminor = 0
    sanity_bytes = 0

    (size,crc,sanity_cookie) = wrapper.write(statb[stat.ST_INO],
                                             statb[stat.ST_MODE],
                                             statb[stat.ST_UID],
                                             statb[stat.ST_GID],
                                             statb[stat.ST_MTIME],
                                             statb[stat.ST_SIZE],
                                             major, minor, rmajor, rminor,
                                             fin._file_.name, sanity_bytes)
    print "cpio.write returned: size:",size,"crc:",crc,\
          "sanity_cookie:",sanity_cookie

    fin.close()
    fout.close()

    if size != statb[stat.ST_SIZE] :
        raise IOError,"Size ERROR: Wrote "+repr(size)+" bytes, file was "\
              +repr(statb[stat.ST_SIZE])+" bytes long"




    fin  = diskdriver_open(sys.argv[2],"r")
    fout = diskdriver_open(sys.argv[1]+".copy","w")

    wrapper = cpio(fin,fout,binascii.crc_hqx)
    (read_size, read_crc) = wrapper.read(sanity_cookie)
    print "cpio.read  returned: size:",read_size,"crc:",read_crc

    fin.close()
    fout.close()

    if read_size != size :
        raise IOError,"Size ERROR: Read "+repr(read_size)+" bytes, wrote "\
              +repr(size)+" bytes"
