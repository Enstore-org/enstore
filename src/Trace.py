#  This file (Trace.py) was created by Ron Rechenmacher <ron@fnal.gov> on
#  Mar 23, 1999. "TERMS AND CONDITIONS" governing this file are in the README
#  or COPYING file. If you do not have such a file, one can be obtained by
#  contacting Ron or Fermi Lab in Batavia IL, 60510, phone: 630-840-3000.
#  $RCSfile$
#  $Revision$
#  $Date$

import sys				# setprofile
import e_errors				# required for default logging, ???
import os				# required for default logging, ???
import pwd				# required for default logging, ???
import Ptrace				# basis for this work    
import base64                           # to send pickled dictionary as  string
import cPickle                          # to preserve dictionaries and lists
import cStringIO		# to make freeze happy
import copy_reg			# to make freeze happy
import types
import string

# message types.  a message type will be appended to every message so that
# identifying which message is which will be easier.  messages logged without
# a message type will have MSG_DEFAULT appended.
MSG_DICT_DFLT = {}
MSG_DICT = "MSG_DICT:"
MSG_TYPE_DFLT = ""
MSG_TYPE = "MSG_TYPE="

MSG_ENCP_XFER = "%sENCP_XFER "%MSG_TYPE
MSG_MC_LOAD_REQ = "%sMC_LOAD_REQ "%MSG_TYPE
MSG_MC_LOAD_DONE = "%sMC_LOAD_DONE "%MSG_TYPE

# define some short-cuts, for efficiency.  (I may wish to use
# "from Ptrace import *)
trace  = Ptrace.trace
init   = Ptrace.init
on     = Ptrace.on
off    = Ptrace.off
mode   = Ptrace.mode



# USER FUNCTIONS
def log( severity, msg, msg_dict = MSG_DICT_DFLT, msg_type = MSG_TYPE_DFLT ):
    # CHECK TO SEE IF THERE IS A VALID DICTIONARY. IF THERE IS,
    # BASE64(CPICKLE) IT AND ATTACH TO END OF MESSAGE.
    if len(msg_dict) > 0:
        tmp_dict = base64.encodestring(cPickle.dumps(msg_dict))
        tmp_dict = string.split(tmp_dict, "\n")
        tmp_dict = string.joinfields(tmp_dict, "")
        msg_dict = "%s%s" % (MSG_DICT, tmp_dict)
    else:
        msg_dict = ""

    # SEE IF USER ENTERED HIS OWN 'MSG_TYPE' MESSAGE. IF HE DID,
    # ATTACH TO END OF MESSAGE.
    if len(msg_type) > 0:
        msg_type = "%s%s" % (MSG_TYPE, msg_type)

    msg = "%s %s %s" % (msg, msg_dict, msg_type)
    trace( severity, msg)
    return None

def alarm( severity, root_error, rest={} ):
    # make sure it is a valid severity
    if type(severity) == types.StringType:
	skeys = e_errors.sevdict.keys()
	for skey in skeys:
	    if severity == e_errors.sevdict[skey]:
		rest['severity'] = severity
		break
	else:
	    rest['severity'] = e_errors.sevdict[e_errors.MISC]
    else:
	# severity was an int
	rest['severity'] = e_errors.sevdict.get(severity, 
						e_errors.sevdict[e_errors.MISC])
    rest['root_error'] = root_error
    trace( e_errors.ALARM, "%s"%rest, rest )
    return None

def set_alarm_func( func ):
    Ptrace.func1_set( func )
    return None
def set_log_func( func ):
    Ptrace.func2_set( func )
    return None

##############################################################################
##############################################################################
# defaults (templates) -- called from trace
#

def default_alarm_func( time, pid, name, args ):
    lvl = args[0]
    msg = args[1]
    print "default alarm_func", args
    return None
set_alarm_func( default_alarm_func )

pid = os.getpid()
try:
    usr = pwd.getpwuid(os.getuid())[0]
except:
    usr = "unknown"
def default_log_func( time, pid, name, args ):
    severity = args[0]
    msg = args[1]
    if severity > e_errors.MISC: severity = e_errors.MISC
    print '%.6d %.8s %s %s  %s' % (pid,usr,e_errors.sevdict[severity],name,msg)
    return None

set_log_func( default_log_func )


# let user turn this on manully...sys.setprofile(Ptrace.profile)

