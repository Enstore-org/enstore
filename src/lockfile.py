###############################################################################
# src/$RCSfile$   $Revision$
#
# This file was originally in the python distribution - it disappeared between
# releases 1.5 and 1.51.   I (Jon) just added it to the enstore src area.

# system imports
import struct, fcntl, FCNTL

# enstore imports
import Trace

def _lock(f, op):
	Trace.trace(21,'{_lock')
	dummy = fcntl.fcntl(f.fileno(), FCNTL.F_SETLKW,
			    struct.pack('2h8l', op,
					0, 0, 0, 0, 0, 0, 0, 0, 0))
	Trace.trace(21,'}_lock '+repr(dummy))
	
def writelock(f):
	Trace.trace(20,'{}writelock')
	_lock(f, FCNTL.F_WRLCK)

def readlock(f):
	Trace.trace(20,'{}readlock')
	_lock(f, FCNTL.F_RDLCK)

def unlock(f):
	Trace.trace(20,'{}unlock')
	_lock(f, FCNTL.F_UNLCK)

