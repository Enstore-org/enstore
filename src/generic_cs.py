###############################################################################
# src/$RCSfile$   $Revision$
#
# generic_cs = generic client/server code

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
global_logger = ENNONE
global_severity = ENNONE

def add_id(id, msg):
    Trace.trace(25,"{add_id "+repr(id))
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
    Trace.trace(25,"}add_id ")
    return nmsg

# keep a logger
def add_logger(logger, log_severity=ENNONE):
    global global_logger
    global global_severity

    global_logger = logger
    global_severity = log_severity

# send the message to the logger
def send_to_logger(logger, log_severity, msg):
    Trace.trace(25,"{send_to_logger "+repr(logger))
    if logger != ENNONE:
        l_logger = logger
    else:
        l_logger = global_logger

    if log_severity != ENNONE:
        l_log_severity = log_severity
    else:
        l_log_severity = global_severity

    if l_logger != ENNONE:
        l_logger.send(l_log_severity, 1, msg)
    Trace.trace(25,"}send_to_logger "+repr(l_logger))

def enprint(msg, msg_bit=ENNONE, verbosity=ENNONE_V, logger=ENNONE, \
	    log_severity=ENNONE, id=""):
    Trace.trace(24,"{enprint "+repr(msg))
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
	    send_to_logger(logger, log_severity, nmsg)
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
	send_to_logger(logger, log_severity, nmsg)

    # reset the following so if the next time we are called generically,
    # we do not retain the old value.
    global_print_id = ""
    Trace.trace(24,"}enprint ")

class GenericCS:

    def enprint(self, msg, msg_bit=ENNONE, verbosity=ENNONE_V, logger=ENNONE, \
 	        log_severity=ENNONE):
	Trace.trace(23,"{self.enprint ")
	global global_print_id

	# use an object data member as a prefix to the message if available
	try:
	    global_print_id = self.print_id
	except:
	    global_print_id = ""

	# if no logger entered see if the object has one
	if logger == ENNONE:
	    try:
	        l_logger = self.logc
	    except:
	        l_logger = logger
	else:
	    l_logger = logger
	    if l_logger == "":
	        l_logger = ENNONE

	enprint(msg, msg_bit, verbosity, l_logger, log_severity)
	Trace.trace(23,"}self.enprint ")

    def add_logger(self, logger, log_severity=ENNONE):
	Trace.trace(23,"{self.add_logger ")
	add_logger(logger, log_severity)
	Trace.trace(23,"}self.add_logger ")

