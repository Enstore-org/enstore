###############################################################################
# src/$RCSfile$   $Revision$
#
#system imports
import pprint

#enstore imports
import Trace

NONE = -1
NO_LOGGER = 0

# define the bits used in the verbose mask
SERVER       = 000000000001     # 1
CLIENT       = 000000000002     # 2
CONNECTING   = 000000000004     # 4
D0SAM        = 000000000010     # 8
SOCKET_ERROR = 000000000020     # 16
ALIVE        = 000000000040     # 32
DEBUG        = 010000000000     # 1073741824
ALL_SERVER   = SERVER | ALIVE | CONNECTING | SOCKET_ERROR
ALL_CLIENT   = CLIENT | ALIVE | CONNECTING | SOCKET_ERROR
PRETTY_PRINT = 020000000000
ALL          = 037777777777

def enprint(msg, logger=NO_LOGGER, msg_bit=0, id="", verbosity=NONE):
    # add id on to the front if we have one
    if id == "":
	nmsg = msg
    else:
	nmsg = id+": "+repr(msg)

    # send the message to STDOUT.
    # do not print if the verbosity level does not have a bit set for this msg.
    if verbosity != NONE:
	# check that this message should be printed for this verbosity
	if verbosity & msg_bit:
	    if msg_bit & PRETTY_PRINT:
	        try:
	            pprint.pprint(nmsg)
	        except:
	            pass
	    else:
	        try:
	            print nmsg
	        except:
	            pass
	    # also send to the logger
	    if logger != 0:
	        print "sending to logger\n"
	        logger.send(log_client.WARNING, 1, nmsg)
    else:
	# no verbosity was entered, try to print the message
	if msg_bit & PRETTY_PRINT:
	    try:
	        pprint.pprint(nmsg)
	    except:
	        pass
	else:
	    try:
	        print nmsg
	    except:
	        pass

class GenericCS:

    def enprint(self, msg, logger=NO_LOGGER, msg_bit=0, id="", verbosity=NONE):
	enprint(msg, logger, msg_bit, id, verbosity)
