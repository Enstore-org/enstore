###############################################################################
# src/$RCSfile$   $Revision$
#
#########################################################################
#                                                                       #
# write tape and drive statisics to a file                              #
#                                                                       #
#########################################################################

#system imports
import os
import sys
import string
import types

#enstore imports
import FTT
import Trace
import e_errors
import interface

import generic_client
import udp_client

MY_NAME = ".WS"

class WriteStats:
    def __init__(self, name=""):
        self.log_name = "C_"+string.upper(string.replace(name,
                                           ".write_stats", MY_NAME))

    def writem(self, device, fd, param=[]):
        #FTT.initFTT()
	FTT.open( device, 'r' )
        ss = FTT.get_stats()  
        print ss
	for skey in ss.keys():
	    buf = repr(skey)+' '+repr(ss[skey])+'\n'
            fd.write(buf)
	FTT.close( )

    def writeAll(self, device, fd, param=[]):
        #FTT.initFTT()
	FTT.open( device, 'r' )
        ss = FTT.get_statsAll()  
        print ss
	for skey in ss.keys():
	    buf = repr(skey)+' '+repr(ss[skey])+'\n'
            fd.write(buf)
	FTT.close( )

    def dumpem(self, device, fn, param=[]):
        #FTT.initFTT()
	FTT.open( device, 'r' )
	print fn
	fd = open(fn,'w')
        stat = FTT.dump_stats(fd)  
        print stat
	fd.close()
	FTT.close( )

        
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

    # this is the name of the function that the wrapper uses to write
    def write(self,buffer):
        return self._file_.write(buffer)


if __name__ == "__main__" :
    import sys
    import getopt
    import Devcodes

    Trace.init("WRITE_STATS")
    Trace.trace(6,"WriteStats called with args "+repr(sys.argv))


    options = ["fttGetStats", "fttDump", "fttGetAllStats"]
    try:
        optlist,args=getopt.getopt(sys.argv[1:], '', options)
    except getopt.error:
	print "usage: write_stats" + " <"+repr(options)+"> device outfile"
	sys.exit(1)    
    (opt,val) = optlist[0]
    w_stats = WriteStats()
	
    if opt == "--fttGetStats":
        fout = DiskDriver()
        fout.open(args[1],"w")
        w_stats.writem(args[0], fout)
        fout.close()

    elif opt == "--fttDump":
        w_stats.dumpem(args[0], args[1])

    elif opt == "--fttGetAllStats":
        fout = DiskDriver()
        fout.open(args[1],"w")
        w_stats.writeAll(args[0], fout)
        fout.close()



