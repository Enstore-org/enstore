###############################################################################
# src/$RCSfile$   $ $
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
import Trace
import checksum
import e_errors

"""
tape format:
VOL record (80 chars)
HDR1 record (80 chars)
HDR2 record (80 chars)
HDR3 record (80 chars)
HDR4 record (80 chars)
tapedata1
EOF1 record (80 chars)
EOF2 record (80 chars)
EOF3 record (80 chars)
EOF4 record (80 chars)
HDR1 record (80 chars)
HDR2 record (80 chars)
HDR3 record (80 chars)
HDR4 record (80 chars)
tapedata2
EOF1 record (80 chars)
EOF2 record (80 chars)
EOF3 record (80 chars)
EOF4 record (80 chars)
...etc...

tapedata commonly is of zero length

"""
class Wrapper :

    def sw_mount( self, driver, info ):
	return

    def read_pre_data( self, driver, info ):
        fileNumber = int(string.split(info,"_")[2])
        if fileNumber == 1:
            self.read_volume_header(driver, info)
	self.read_ds1_header(driver, info)
	self.read_ds2_header(driver, info)
	self.read_ds3_header(driver, info)
	self.read_ds4_header(driver, info)
	return


    def read_post_data( self, driver, info ):
	self.read_ds1_header(driver, info)
	self.read_ds2_header(driver, info)
	self.read_ds3_header(driver, info)
	self.read_ds4_header(driver, info)
	return
	
    def read_volume_header ( self, driver, info ):
        header = driver.read(80)
	vol_label = header[0:4]
	vol_serial = header[4:10]
        return
	
    def read_ds1_header ( self, driver, info ):
        header = driver.read(80)
	label = header[0:4]
	if label == "HDR1":  # can be HDR1, EOV1 or EOF1
	    data_set_ID = header[4:21]
        return
	
    def read_ds2_header ( self, driver, info ):
        header = driver.read(80)
	label = header[0:4]
	if label == "HDR2":
	    block_length = header[5:10]
        return
	
    def read_ds3_header ( self, driver, info ): # VMS
        header = driver.read(80)
	label = header[0:4]
	if label == "HDR3":
	    block_length = header[4:8]
        return
	
    def read_ds4_header ( self, driver, info ): # VMS
        header = driver.read(80)
	label = header[0:4]
	if label == "HDR4":
	    file_id = header[5:67]
        return
	

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

    options = ["extract"]
    optlist,args=getopt.getopt(sys.argv[1:], '', options)
    (opt,val) = optlist[0]
    if not optlist:
	print "usage: ansix327" + " <"+repr(options)+"> infile outfile"
	sys.exit(1)

    if not (opt == "--extract"):
	print "usage: ansix327" + " <"+repr(options)+"> infile outfile"
	sys.exit(1)

    fin = DiskDriver()
    fin.open(args[0],"r")
    fout = DiskDriver()
    fout.open(args[1],"w")

    wrapper = Wrapper()
	
    if opt == "--extract":
	wrapper.read_pre_data(fin, "0000_0000000_0001")
	wrapper.file_size = -1
	while wrapper.file_size < 0 :
	    wrapper.file_size = int(raw_input('Input file size in bytes:'))
	print "FILE SIZE", wrapper.file_size
	if  wrapper.file_size > 0:
	    buf = fin.read(wrapper.file_size)
	    print "buf=",buf
	    fout.write(buf)
	wrapper.read_post_data(fin,{'data_crc':0})

    fin.close()
    fout.close()

