import sys
from errno import *
import pprint

SEEK_SET = 0

NoSpace = "no space for write"
DeviceError = "bad device"
MediaError = "Something wrong with the media"

STATE_UNLOAD = 0
STATE_LOAD = 1
STATE_OPEN_READ = 2
STATE_OPEN_WRITE = 3
STATE_CLOSED = 4

# all drivers should inherit from this
class GenericDriver:

    def __init__(self, device, eod_cookie, remaining_bytes):
        # remember, this is just a guess. It is minimum number (no compression)
        self.remaining_bytes = remaining_bytes
        self.device = device
        self.df = open(device, "a+")
        self.state = STATE_UNLOAD

    def load(self):
        self.state = STATE_LOAD
        pass

    def unload(self):
        self.state = STATE_UNLOAD
        pass

    # callers are to try to send buffers of iomax,
    #    except for  the last buffer, which may be exact.
    def get_iomax(self) :
        return self.iomax


class  RawDiskDriver(GenericDriver) :
    """
    I use this driver to enhance development -- I test using floppys, no
    guarantee of good or general support....

    This driver implements the eod_cookie as an ascii number which represents
    the byte to seek to when appending to a disk file.

    This driver implements the file_cookie as the python ascification of a
    two-number list. -- The byte to seek to before doing a read, and a byte
    just past the last byte written when we are finished
    """

    def __init__(self, device, eod_cookie, remaining_bytes):
        GenericDriver.__init__(self, device, eod_cookie, remaining_bytes)
        self.set_eod(eod_cookie)
        self.blocksize = 4096
        self.iomax = self.blocksize * 16

    def set_eod(self, eod_cookie) :
        # When a volume is ceated, the system sets EOD cookie to "none"
        if eod_cookie == "none" :
            self.eod = 0
        else:
            self.eod = eval(eod_cookie)

    # read file -- use the "cookie" to not walk off the end, since we have
    # no "file marks" on a disk
    def open_file_read(self, file_location_cookie) :
        self.firstbyte, self.pastbyte = eval(file_location_cookie)
        self.df.seek(self.firstbyte, 0)
        self.left_to_read = self.pastbyte - self.firstbyte
        self.state = STATE_OPEN_READ

    def close_file_read(self) :
        self.state = STATE_CLOSED
        pass

    def read_block(self):
                # no file marks on a disk, so use the information
                # in the cookie to bound the file.
                n_to_read = min(self.iomax , self.left_to_read)
                if n_to_read == 0 : return ""
                buf = self.df.read(n_to_read)
                self.left_to_read = self.left_to_read - len(buf)
                if self.left_to_read < 0:
                        raise "assert error"
                return buf

    def open_file_write(self):
        # we cannot auto sense a floppy, so we must trust the user
        self.df.seek(self.eod, 0)
        self.first_write_block = 1
        self.state = STATE_OPEN_WRITE

    def close_file_write(self):
        first_byte = self.eod
        last_byte = self.df.tell()

        self.eod = last_byte
        # we don't fill each byte - the next starting place is at the
        # beginning of the next block
        self.eod = self.eod + (self.blocksize - (self.eod % self.blocksize))

        # If the data is being written to a file on a hard drive, the
        # file has to be blanked filled to the next blocksize.
        # Otherwise, the next open_write doesn't seek to end
        empty = self.eod-last_byte-1      # number of empty bytes to next block
        self.write_block("J"*empty)       # fill it out

        self.state = STATE_CLOSED
        return `(first_byte, last_byte)`  # cookie describing the file

    def get_eod_cookie(self):
        return repr(self.eod)

    def get_eod_remaining_bytes(self):
        return self.remaining_bytes

    # write a block of data to already open file: user has to handle exceptions
    def write_block(self, data):
        if len(data) > self.remaining_bytes :
            raise errorcode[ENOSPC], NoSpace
        self.remaining_bytes = (self.remaining_bytes-len(data))
        self.df.write(data)
        self.df.flush()
        if self.first_write_block :
            self.first_write_block = 0
            self.eod = self.df.tell() - len(data)

if __name__ == "__main__" :
    import getopt
    import socket
    import string

    size = 760000
    device = "./rdd-testfile.fake"
    eod_cookie = "0"
    list = 0

    # see what the user has specified. bomb out if wrong options specified
    options = ["size=","device=","eod_cookie=","list","help"]
    optlist,args=getopt.getopt(sys.argv[1:],'',options)
    for (opt,value) in optlist :
        if opt == "--size" :
            size = string.atoi(value)
        elif opt == "--device" :
            device = value
        elif opt == "--eod_cookie" :
            eod_cookie = value
        elif opt == "--list" :
            list = 1
        elif opt == "--help" :
            print "python ",sys.argv[0], options
            print "   do not forget the '--' in front of each option"
            sys.exit(0)


    if list:
        print "Creating RawDiskDriver device",device, "with",size,"bytes"
    rdd = RawDiskDriver (device,eod_cookie,size)
    rdd.load()

    cookie = {}

    try:
        if list:
            print "writing 1 0's"
        rdd.open_file_write()
        rdd.write_block("0"*1)
        cookie[0] = rdd.close_file_write()
        if list:
            print "   ok",cookie[0]

        if list:
            print "writing 10 1's"
        rdd.open_file_write()
        rdd.write_block("1"*10)
        cookie[1] = rdd.close_file_write()
        if list:
            print "   ok",cookie[1]

        if list:
            print "writing 100 2's"
        rdd.open_file_write()
        rdd.write_block("2"*100)
        cookie[2] = rdd.close_file_write()
        if list:
            print "   ok",cookie[2]

        if list:
            print "writing 1,000 3's"
        rdd.open_file_write()
        rdd.write_block("3"*1000)
        cookie[3] = rdd.close_file_write()
        if list:
            print "   ok",cookie[3]

        if list:
            print "writing 10,000 4's"
        rdd.open_file_write()
        rdd.write_block("4"*10000)
        cookie[4] = rdd.close_file_write()
        if list:
            print "   ok",cookie[4]

        if list:
            print "writing 100,000 5's"
        rdd.open_file_write()
        rdd.write_block("5"*100000)
        cookie[5] = rdd.close_file_write()
        if list:
            print "   ok",cookie[5]

        if list:
            print "writing 1,000,000 6's"
        rdd.open_file_write()
        rdd.write_block("6"*1000000)
        cookie[6] = rdd.close_file_write()
        if list:
            print "   ok",cookie[6]

        if list:
            print "writing 1,000,000 7's"
        rdd.open_file_write()
        rdd.write_block("7"*1000000)
        cookie[7] = rdd.close_file_write()
        print "   ok",cookie[7]

    except:
        if list:
            print "ok, processed exception:"\
                  ,sys.exc_info()[0],sys.exc_info()[1]

    if list:
        print "EOD cookie:",rdd.get_eod_cookie()
        print "lower bound on bytes available:", rdd.get_eod_remaining_bytes()
        pprint.pprint(rdd)

    for k in cookie.keys() :
        rdd.open_file_read(cookie[k])
        readback = rdd.read_block()
        if list:
            print "cookie=",k," readback[0]=",readback[0]\
                  ,"readback[end]=",readback[len(readback)-1]
        rdd.close_file_read()

    rdd.unload()
