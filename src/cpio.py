import os
import stat
from errno import *
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

# create 2 headers (1 for data file and 1 for crc file) + 1 trailer
def cpio_hdrs(format,            # either "new" or "CRC"
              inode, mode, uid, gid, nlink, mtime, filesize,
              major, minor, rmajor, rminor, filename, crc) :
    # only 2 cpio formats allowed
    if format == "new" :
        magic = "070701"
    elif format == "CRC"  :
        magic = "070702"
    else :
        raise errorcode[EINVAL],"Invalid format: "+ repr(format)+\
              " only \"new\" and \"CRC\" are valid formats"

    # files greater than 2  GB are just not allowed right now
    max = 2**30-1+2**30
    if filesize > max :
        raise errorcode[EOVERFLOW],"Files are limited to "+repr(max) +\
              " bytes and your "+filename+" has "+repr(filesize)+" bytes"

    # create the header for the data file and a header for a crc file
    heads = []
    for h in [(filename,filesize), (filename+".encrc",8)] :
        head = \
             "070701" +\
             "%08x" % inode +\
             "%08x" % mode +\
             "%08x" % uid +\
             "%08x" % gid +\
             "%08x" % nlink +\
             "%08x" % mtime +\
             "%08x" % h[1] +\
             "%08x" % major +\
             "%08x" % minor +\
             "%08x" % rmajor +\
             "%08x" % rminor +\
             "%08x" % int(len(h[0])+1) +\
             "%08x" % crc +\
             "%s\0" % h[0]
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


# generate the cpio "trailers"
def cpio_trls(siz, head_crc, file_crc, trailer) :
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
    return("\0"*padd + \
           head_crc + "%08x" % file_crc + "\0"*padc + \
           trailer + "\0"*padt )


# given a buffer pointing to beginning of header, return:
#    offset to real data, data size, filename,
def cpio_decode(buffer):
    # only 2 cpio formats allowed
    magic = buffer[0:6]
    if magic == "070701" or  magic == "070702" :
        pass
    else :
        raise errorcode[EINVAL],"Invalid format: "+ repr(magic)+\
              " only \"070701\" and \"070702\" are valid formats"

    filename_size = string.atoi(buffer[94:102],16)
    data_offset = 110+filename_size
    data_offset =data_offset + (4-(data_offset%4))%4
    data_size = string.atoi(buffer[54:62],16)
    filename = buffer[110:110+filename_size-1]
    return (data_offset, data_size, filename)


# given a buffer pointing to beginning of header, return crc
def cpio_encrc(buffer):
    offset,size,name = cpio_decode(buffer)
    return string.atoi(buffer[offset:offset+8],16)


# generate a cpio archive, given the input and output file names
def cpio_write(input,output) :

    # open the file - we crash if they can't be opened
    fin  = open(input,"r")
    fout = open(output,"w")

    # get the input file's stat
    statb = os.fstat(fin.fileno())
    if not stat.S_ISREG(statb[stat.ST_MODE]) :
        raise errorcode[EINVAL],\
              "Invalid input file: can only handle regular files"

    format = "new"
    nlink = 1
    major = 0
    minor = 0
    rmajor = 0
    rminor = 0

    # generate the headers for the archive and write out 1st one
    header, head_crc, trailer = \
            cpio_hdrs(format, statb[stat.ST_INO], statb[stat.ST_MODE], \
                      statb[stat.ST_UID], statb[stat.ST_GID], \
                      nlink, statb[stat.ST_MTIME], statb[stat.ST_SIZE], \
                      major, minor, rmajor, rminor, fin.name,0)
    size = len(header)
    fout.write(header)

    # now read input file and write it out
    file_crc = 0
    while 1:
        b = fin.read()
        length = len(b)
        if length == 0 :
            break
        size = size + length
        file_crc = binascii.crc_hqx(b,file_crc)
        fout.write(b)

    # write out the trailers
    fout.write(cpio_trls(size, head_crc, file_crc, trailer))

    # close em - it works better than letting python decide when to do it
    fin.close()
    fout.close()

def cpio_read(input) :

    # open the file - we crash if they can't be opened
    fin  = open(input,"r")

    # setup counters/flags
    file_crc = 0
    size = 0
    parse_header = 1
    data_size = 1

    # now read input file and write it out
    while size < data_size:
        offset = 0
        buffer = fin.read()
        length = len(buffer)
        if length == 0 :
            raise "busted"
        # decode the cpio header block
        if parse_header :
            data_offset, data_size, data_name = cpio_decode(buffer)
            parse_header = 0
            fout = open(data_name+".copy",'w')
            offset = data_offset
        if size + length - offset <= data_size :
            size = size + length - offset
            data_end = length
        else :
            data_end = data_size - size + offset
            size = data_size
            next = data_offset + size
            padd =  (4-(next%4)) %4
            trailer = buffer[data_end+padd:]
        file_crc = binascii.crc_hqx(buffer[offset:data_end],file_crc)
        fout.write(buffer[offset:data_end])

    # now read the crc file - just read to end of data and then decode
    while 1  :
        buffer = fin.read()
        length = len(buffer)
        if length == 0 :
            break
        trailer =  trailer + buffer

    recorded_crc = cpio_encrc(trailer)
    print recorded_crc, file_crc

    # close em - it works better than letting python decide when to do it
    fin.close()
    fout.close()


if __name__ == "__main__" :
        import sys

        cpio_write(sys.argv[1],sys.argv[2])
        cpio_read(sys.argv[2])



