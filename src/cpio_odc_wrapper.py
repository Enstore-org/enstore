###############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
import errno
import string
import e_errors
import Trace

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
             pnfs_enstore_airedale_o1.encrc" |cpio -ov -H odc > archive

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

    # files greater than 8GB are just not allowed right now
    max = long(2**30) * 8 - 1
    if filesize > max:
        msg = "Files are limited to %s bytes and your %s has %s bytes" % (
            max, filename, filesize)
        raise e_errors.EnstoreError(errno.EOVERFLOW, msg, "")
    fname = filename
    fsize = filesize
    # set this dang mode to something that works on all machines!
    if ((mode & 0o777000) != 0o100000) & (filename != "TRAILER!!!"):
        mode = 0o100664

    # make all filenames relative - strip off leading slash
    if fname[0] == "/":
        fname = fname[1:]
    dev = makedev(major, minor)
    rdev = makedev(rmajor, rminor)
    header = "070707%06o%06lo%06lo%06lo%06lo%06lo%06o%011lo%06lo%011lo%s\0" % \
        (dev & 0xffff, inode & 0xffff, mode & 0xffff,
         uid & 0xffff, gid & 0xffff, nlink & 0xffff,
         rdev & 0xfff, mtime, (len(fname) + 1) & 0xffff,
         fsize, fname)
    return header


# create  header + trailer
def headers(ticket):

    inode = ticket.get('inode', 0)
    if inode is None:
        Trace.log(
            e_errors.ERROR, "wrong inode in wrapper: %s. Will set to 0" %
            (inode,))
        inode = 0
    mode = ticket.get('mode', 0)
    uid = ticket.get('uid', 0)
    gid = ticket.get('gid', 0)
    nlink = ticket.get('nlink', 1)
    mtime = ticket.get('mtime', 0)
    filesize = ticket.get('size_bytes', 0)
    major = ticket.get('major', 0)
    minor = ticket.get('minor', 0)
    rmajor = ticket.get('rmajor', 0)
    rminor = ticket.get('rminor', 0)
    filename = ticket.get('pnfsFilename', '???')

    header = create_header(inode, mode, uid, gid, nlink, mtime, filesize,
                           major, minor, rmajor, rminor, filename)

    # create the trailer as well
    trailer = create_header(0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, "TRAILER!!!")
    # Trailers must be rounded to 512 byte blocks.
    # Note: a 2GB file (2147483647 bytes) on a intel linux system would
    # fail since the max of a signed integer is 2147483647 and adding
    # just a header of one would trip an overflow without converting to longs.
    pad = (long(len(header)) + long(len(trailer)) + long(filesize)) % 512
    if pad:
        pad = int(512 - pad)  # Note: python 1.5 doesn't allow string*long
        trailer = trailer + '\0' * pad
    return header, trailer

# the cpio_odc wrapper does not provide a hdr_label or an eof_label


def hdr_labels(dummy):
    return ""


def eof_labels(dummy):
    return ""


def vol_label_length():
    return 80


def vol_labels(volume_label, ticket={}, own_id=""):
    vol1_label = 'VOL1' + volume_label[0:6]
    return vol1_label + (79 - len(vol1_label)) * ' ' + '0'


min_header_size = 76


def header_size(header_start):
    # Note: this can raise TypeError as well as ValueError, if the
    # string contains NULL bytes
    filename_size = string.atoi(header_start[59:65], 8)
    header_size = 76 + filename_size
    return header_size


def create_wrapper_dict(ticket):
    # the wrapper section already contains all of the information that we need
    wrapper_d = ticket['wrapper']
    return wrapper_d
