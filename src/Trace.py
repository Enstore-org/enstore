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
import socket
import types

import event_relay_messages
import event_relay_client
import enstore_constants

if __name__== '__main__':
    print "No unit test, sorry"
    sys.exit(-1)

# message types.  a message type will be appended to every message so that
# identifying which message is which will be easier.  messages logged without
# a message type will have MSG_DEFAULT appended.
MSG_TYPE = "MSG_TYPE="
MSG_DEFAULT = ""
MSG_ALARM = "%sALARM "%(MSG_TYPE,)
MSG_ENCP_XFER = "%sENCP_XFER "%(MSG_TYPE,)
MSG_MC_LOAD_REQ = "%sMC_LOAD_REQ "%(MSG_TYPE,)
MSG_MC_LOAD_DONE = "%sMC_LOAD_DONE "%(MSG_TYPE,)

logname = ""
alarm_func = None
log_func = None

print_levels = {}
log_levels = {}
alarm_levels = {}

# stuff added by efb for new event_relay_client
erc = event_relay_client.EventRelayClient()

def notify(msg):
    if type(msg) == types.StringType:
	# we must convert the message into a message instance
	msg = event_relay_messages.decode(msg)
    erc.send(msg)

# end of stuff added by efb

def trunc(x):
    if type(x) != type(""):
        x = str(x)
    if len(x)>=4096:
        x=x[:4080] + "(truncated)"
    return x

def do_print(levels):
    if type(levels) != type([]):
        levels = [levels]
    for level in levels:
        print_levels[level]=1

def dont_print(levels):
    if type(levels) != type([]):
        levels = [levels]
    for level in levels:
        if print_levels.has_key(level):
            del print_levels[level]

def do_log(levels):
    if type(levels) != type([]):
        levels = [levels]
    for level in levels:
        log_levels[level]=1

def dont_log(levels):
    if type(levels) != type([]):
        levels = [levels]
    for level in levels:
        if level<5:
            raise "Not allowed"
        if log_levels.has_key(level):
            del log_levels[level]

def do_alarm(levels):
    if type(levels) != type([]):
        levels = [levels]
    for level in levels:
        alarm_levels[level]=1
    
def dont_alarm(levels):
    if type(levels) != type([]):
        levels = [levels]
    for level in levels:
        if level==0:
            raise "Not allowed"
        if alarm_levels.has_key(level):
            del alarm_levels[level]

def init(name):
    global logname
    logname=name

def log(severity, msg, msg_type=MSG_DEFAULT, doprint=1):
    msg = trunc(msg)
    if  log_func:
        try:
	    # build up message
            if  msg_type != MSG_DEFAULT:
                new_msg = "%s %s" % (msg_type, msg)
            else:
                new_msg = msg
            log_func(time.time(), os.getpid(), logname, (severity, new_msg))
        except:
            exc, detail, tb = sys.exc_info()
            print "Failure writing message to log", msg, detail
        
    if doprint and print_levels.has_key(severity):
        try:
            print msg
            sys.stdout.flush()
        except:
            pass
        
def alarm(severity, root_error, rest={}):
    rest['severity'] = severity
    rest[enstore_constants.ROOT_ERROR] = root_error
    #log(severity, root_error, MSG_ALARM)
    if alarm_func:
        alarm_func(
            time.time, os.getpid(), logname, root_error,
	    ("%s:%s"%(enstore_constants.ROOT_ERROR,
		      rest[enstore_constants.ROOT_ERROR],), rest ))
    if print_levels.has_key(severity):
        try:
            print root_error
            sys.stdout.flush()
        except:
            pass

def trace(severity, msg):
    msg = trunc(msg)
    if print_levels.has_key(severity):
        try:
            print severity, time.ctime(time.time()), msg
	    # the following line will output the memory usage of the process
	    #os.system("a=`ps -ef |grep '/inq'|grep -v grep|xargs echo|cut -f2 -d' '`;ps -el|grep $a|grep python")
	    #print "================================="  # a usefull divider
            sys.stdout.flush()
        except:
            pass
    if log_levels.has_key(severity):
        log(severity, msg, doprint=0)
    if alarm_levels.has_key(severity):
        alarm(severity, msg)
        
def set_alarm_func(func):
    global alarm_func
    alarm_func=func

def set_log_func(func):
    global log_func
    log_func = func

# defaults (templates) -- called from trace

def default_alarm_func(time, pid, name, root_error, args):
    lvl = args[0]
    msg = args[1]
    print "default alarm_func", args
    return None
set_alarm_func(default_alarm_func)


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

set_log_func(default_log_func)


