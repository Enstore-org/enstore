
"""

plenty of Idunnos, but works extensive testing against readable regular files
in the /usr directory trees.  

CPIO is not ideal difficult  for our purposes because it cannot
handle files bigger than 2GB, However, it is nice to conform to something, 
and CPIO is a good deal simpler than tar, _and_ handles arbitarily long file 
names.  

We would have to, by convention, store larger files as a number of 
"files" withing an archvie, I think that this is not a bad
idea, and propose the following convention:

If a file is less than 2GB, store as if we had run CPIO.
If a file is greater than 2GB, the first cpio file 
	has a name dictated by convention -- "...,Appended", 
		body of file says how many extents follow.
		Segments are 2,000,000,000 bytes, with the
		last segment being fractional.

	file name recrded with the segment is origianl name, followed
	by ,n  n begin decimal and starting with 0 
        User can use dd to paste these things together when recovering
	from a raw tape.
		
Last file before !!!Trailer is a file named ",all" which, if present has CRC of
all data in that archive.

No feature to detect alot of 0's and record them as an extent in the 
header, though this is not critical for HEP.

"""
import os
import stat

def padfour (f) :
	length = f.tell()
	padsz = (4 - (length % 4)) % 4
	return "\0" * padsz

def wrap(fin, fout) :

	statb = os.fstat(fin.fileno())
	if not stat.S_ISREG(statb[stat.ST_MODE]) :
		raise "I am only meant to handle a regular file"
	fout.write("070701")  #magic number -- ASCII headers with no crc
	fout.write("%08x" % statb[stat.ST_INO]) 
	fout.write("%08x" % statb[stat.ST_MODE]) 
	fout.write("%08x" % statb[stat.ST_UID]) 
	fout.write("%08x" % statb[stat.ST_GID]) 
	fout.write("00000001") # nlink
	fout.write("%08x" % statb[stat.ST_MTIME]) 
	fout.write("%08x" % statb[stat.ST_SIZE]) #mbz for dirs and fifos 
	fout.write("00000003") #dev  major, per gnu cpio (value from lptop)  
	fout.write("00000002") #dev  minor, per gnu cpio (valure form lptop)
	fout.write("00000000") #rdev major, per gnu cpio, not valid 4 reg files
	fout.write("00000000") #rdev minor, per gnu cpio, not valid 4 reg files
	fout.write("%08x" % int(len(fin.name)+1))  
	fout.write("00000000") # if we did use the crc version of the 
			       # header, it would be here, not clear if crc of
			       # header or just the data.anyway, wrong place 
			       # for crc of multi-GB files.
        fout.write(fin.name + "\0") # name
	fout.write(padfour(fout))
	while 1:
		b = fin.read()
		if len(b) == 0 : break
		fout.write(b)
	fout.write(padfour(fout))

	#trailer
	fout.write("070701")  #magic number -- ASCII headers with no crc
	fout.write("00000000") #hex inode
	fout.write("00000000") #hex mode
        fout.write("00000000") #hex uid
	fout.write("00000000") #hex gid
	fout.write("00000001") #
	fout.write("00000000") #
	fout.write("00000000") #
	fout.write("00000000") 
	fout.write("00000000") 
	fout.write("00000000") 
	fout.write("00000000") 
	fout.write("0000000b") #length, include term null of name 
	fout.write("00000000") 
	fout.write("TRAILER!!!\0")
	where = fout.tell()
	npad = (512 - (where % 512)) % 512
	fout.write("\0"*npad)


if __name__ == "__main__" :
	
	import sys
	fin  = open(sys.argv[1],"r")
	fout = open(sys.argv[2],"w")
	wrap(fin, fout)





