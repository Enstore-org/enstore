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

# message types.  a message type will be appended to every message so that
# identifying which message is which will be easier.  messages logged without
# a message type will have MSG_DEFAULT appended.
MSG_TYPE = "MSG_TYPE="
MSG_DEFAULT = ""
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
def log( severity, msg, msg_type=MSG_DEFAULT ):
    trace( severity, msg_type+msg )
    return None

def alarm( severity, root_error, rest={} ):
    rest['severity'] = severity
    rest['root_error'] = root_error
    trace( e_errors.ALARM, "root_error:%s"%rest['root_error'], rest )
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
usr = pwd.getpwuid(os.getuid())[0]
def default_log_func( time, pid, name, args ):
    severity = args[0]
    msg = args[1]
    if severity > e_errors.MISC: severity = e_errors.MISC
    print '%.6d %.8s %s %s  %s' % (pid,usr,e_errors.sevdict[severity],name,msg)
    return None

set_log_func( default_log_func )


#sys.setprofile(Ptrace.profile)
