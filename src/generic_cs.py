###############################################################################
# src/$RCSfile$   $Revision$
#
#system imports
import pprint

#enstore imports
import Trace

ENNONE = 0
ENNONE_V = -1

# define the bits used in the verbose mask
SERVER       = 000000000001     # 1
CLIENT       = 000000000002     # 2
CONNECTING   = 000000000004     # 4
D0SAM        = 000000000010     # 8
SOCKET_ERROR = 000000000020     # 16
ALIVE        = 000000000040     # 32
PNFS         = 000000000100     # 64
INTERFACE    = 000000000200     # 128
DEBUG        = 010000000000     # 1073741824
ALL_SERVER   = SERVER | ALIVE | CONNECTING | SOCKET_ERROR
ALL_CLIENT   = CLIENT | ALIVE | CONNECTING | SOCKET_ERROR
PRETTY_PRINT = 020000000000
ALL          = 037777777777
global_print_id = ""

def add_id(id, msg):
    global global_print_id

    # add id on to the front if we have one
    if id == "":
	# now look to see if we have a global one
	if global_print_id == "":
	    nmsg = msg
	else:
	    nmsg = global_print_id+": "+repr(msg)
    else:
	nmsg = id+": "+repr(msg)
    return nmsg

def enprint(msg, msg_bit=ENNONE, verbosity=ENNONE_V, logger=ENNONE, \
	    log_severity=ENNONE, id=""):
    global global_print_id

    # send the message to STDOUT.
    # do not print if the verbosity level does not have a bit set for this msg.
    if verbosity != ENNONE_V:
	# check that this message should be printed for this verbosity
	if verbosity & msg_bit:
	    nmsg = add_id(id, msg)
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
	    if logger != ENNONE:
	        logger.send(log_severity, 1, nmsg)
    else:
	# no verbosity was entered, try to print the message
	nmsg = add_id(id, msg)
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
	if logger != ENNONE:
	    logger.send(log_severity, 1, nmsg)

    # reset the following so if the next time we are called generically,
    # we do not retain the old value.
    global_print_id = ""

class GenericCS:

    def enprint(self, msg, msg_bit=ENNONE, verbosity=ENNONE_V, logger=ENNONE, \
 	        log_severity=ENNONE):
	global global_print_id
	try:
	    global_print_id = self.print_id
	except:
	    global_print_id = ""

	enprint(msg, msg_bit, verbosity, logger, log_severity)

