# $Id$

# Alternate implementation of Trace, which does not use shared memory,
# semaphores, circular queues, or sys.setprofile.  Less powerful but more
# foolproof.

#Implements the functions:
# trace, init, on,  mode, log, alarm,
# set_alarm_func, set_log_func


import sys				
import e_errors				
import os				
import pwd				
import time

if __name__== '__main__':
    print "No unit test, sorry"
    sys.exit(-1)


# message types.  a message type will be appended to every message so that
# identifying which message is which will be easier.  messages logged without
# a message type will have MSG_DEFAULT appended.
MSG_TYPE = "MSG_TYPE="
MSG_DEFAULT = ""
MSG_ENCP_XFER = "%sENCP_XFER "%MSG_TYPE
MSG_MC_LOAD_REQ = "%sMC_LOAD_REQ "%MSG_TYPE
MSG_MC_LOAD_DONE = "%sMC_LOAD_DONE "%MSG_TYPE

logname = ""
alarm_func = None
log_func = None

def not_implemented(*args):
    pass #feature not implemented in Trace-Lite

trace=mode=on=off=not_implemented

def init(name):
    global logname
    logname=name


def log( severity, msg, msg_type=MSG_DEFAULT ):
    if  log_func:
        log_func( time.time(), os.getpid(), logname, (severity,msg_type,msg))
    
        
def alarm( severity, root_error, rest={} ):
    rest['severity'] = severity
    rest['root_error'] = root_error
    if alarm_func:
        alarm_func(
            time.time, os.getpid(), logname, ("root_error:%s"%rest['root_error'], rest ))
    log(severity, root_error)
        
def set_alarm_func( func ):
    global alarm_func
    alarm_func=func

def set_log_func( func ):
    global log_func
    log_func = func

# defaults (templates) -- called from trace

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

import Trace_lite
sys.modules['Trace']=Trace_lite

