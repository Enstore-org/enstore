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

    recordLength = 80

    def sw_mount( self, driver, info ):
	return

    def read_pre_data( self, driver, ticket ):
        if type(ticket) == types.DictType:
            fileInfo = ticket['hsm_driver']['cur_loc_cookie']
            fileNumber = int(string.split(fileInfo,"_")[2])
	elif type(ticket) == types.IntType:
	    fileNumber = ticket
	else:
	    raise IOError, "bad file number input " + repr(ticket)	    
        if fileNumber == 0:
            header = driver.read(self.recordLength)
	header = driver.read(self.recordLength)
	header = driver.read(self.recordLength)
	header = driver.read(self.recordLength)
	header = driver.read(self.recordLength)
	return


    def read_post_data( self, driver, info ):
	header = driver.read(self.recordLength)
	header = driver.read(self.recordLength)
	header = driver.read(self.recordLength)
	header = driver.read(self.recordLength)
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
	print "usage: run1" + " <"+repr(options)+"> infile outfile infilenumber"
	sys.exit(1)

    if not (opt == "--extract"):
	print "usage: run1" + " <"+repr(options)+"> infile outfile infilenumber"
	sys.exit(1)

    fin = DiskDriver()
    fin.open(args[0],"r")
    fout = DiskDriver()
    fout.open(args[1],"w")

    wrapper = Wrapper()
	
    if opt == "--extract":
	wrapper.read_pre_data(fin, int(args[2]))
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

