###############################################################################
# src/$RCSfile$   $Revision$
#
#system imports
import pprint

#enstore imports
import Trace

NONE = -1

# define the bits used in the verbose mask
D0SAM            =     0x1000
PRETTY_PRINT_BIT = 0x80000000
ALL              = 0xFFFFFFFF

class GenericCS:

    def enprint(self, msg, msg_bit=0, verbosity=-1):
	# send the message to STDOUT.
	# do not print if the verbosity level does not have a bit set for
	# this msg.
	if verbosity != NONE:
	    # check that this message should be printed for this verbosity
	    if verbosity and msg_bit:
	        if msg_bit and PRETTY_PRINT_BIT:
	            try:
	                pprint.pprint(msg)
	            except:
	                pass
	        else:
	            try:
	                print msg
	            except:
	                pass
	else:
	    # no verbosity was entered, try to print the message
	    try:
	        print msg
	    except:
	        pass
