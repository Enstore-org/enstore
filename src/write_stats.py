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
import string

#enstore imports
import FTT
import Trace
import e_errors
import interface
import hostaddr

import generic_client
import udp_client

MY_NAME = ".WS"

class WriteStats:
    def __init__(self, name=""):
        self.log_name = "C_"+string.upper(string.replace(name,
                                           ".write_stats", MY_NAME))

    def writem(self, device, fn, param=[]):
	FTT.open( device, 'r' )
        ss = FTT.get_stats()  
	fd = open(fn,'w')
	fd.write("device = "+device+'\n')
	result = hostaddr.gethostinfo()
	fd.write("hostname = "+result[0]+'\n')
	for skey in ss.keys():
	    buf = repr(skey)+' = '+repr(ss[skey])+'\n'
            fd.write(buf)
	fd.close()
	FTT.close( )

    def writeAll(self, device, fn, param=[]):
	FTT.open( device, 'r' )
        ss = FTT.get_statsAll()  
	fd = open(fn,'w')
	fd.write("device = "+device+'\n')
	result = hostaddr.gethostinfo()
	fd.write("hostname = "+result[0]+'\n')
	for skey in ss.keys():
	    s1 = string.replace(repr(skey),"'","")
	    s2 = string.replace(repr(ss[skey]),"'","")
	    buf = s1+' = '+s2+'\n'
            fd.write(buf)
	fd.close()
	FTT.close( )

    def dumpem(self, device, fn, param=[]):
	FTT.open( device, 'r' )
	fd = open(fn,'w')
        stat = FTT.dump_stats(fd)  
	fd.close()
	FTT.close( )

        
###############################################################################

if __name__ == "__main__" :
    import sys
    import getopt

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
        w_stats.writem(args[0], args[1])

    elif opt == "--fttDump":
        w_stats.dumpem(args[0], args[1])

    elif opt == "--fttGetAllStats":
        w_stats.writeAll(args[0], args[1])


