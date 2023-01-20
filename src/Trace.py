#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

# Alternate implementation of Trace, which does not use shared memory,
# semaphores, circular queues, or sys.setprofile.  Less powerful but more
# foolproof.

#Implements the functions:
# trace, init, on,  mode, log, alarm,
# set_alarm_func, set_log_func

# system imports
import sys
import traceback
import os
import pwd
import time
#import socket
import types
import threading
try:
    import multiprocessing
    have_multiprocessing = True
except ImportError:
    have_multiprocessing = False

# enstore modules
#import enstore_constants
import event_relay_messages
import event_relay_client
import e_errors

if __name__ == '__main__':
    sys.exit("No unit test, sorry\n")

"""
The use of print_lock has been commented out at the request of Sasha Moibenko.
"""
if have_multiprocessing:
    print_lock = multiprocessing.Lock()
    thread_lock = multiprocessing.Lock()
else:
    print_lock = threading.Lock()
    thread_lock = threading.Lock()

# message types.  a message type will be appended to every message so that
# identifying which message is which will be easier.  messages logged without
# a message type will have MSG_DEFAULT appended.
MSG_TYPE = "MSG_TYPE="
MSG_DEFAULT = ""
MSG_ALARM = "%sALARM " % (MSG_TYPE,)
MSG_ENCP_XFER = "%sENCP_XFER " % (MSG_TYPE,)
MSG_MC_LOAD_REQ = "%sMC_LOAD_REQ " % (MSG_TYPE,)
MSG_MC_LOAD_DONE = "%sMC_LOAD_DONE " % (MSG_TYPE,)
MSG_ADD_TO_LMQ = "%sADD_TO_LMQ " % (MSG_TYPE,)
MSG_EVENT_RELAY = "%sEVENT_RELAY " % (MSG_TYPE,)

max_message_size = 10000  # maximum allowed message size

#List of severities that should use standard error instead of standard out.
STDERR_SEVERITIES = [e_errors.EMAIL,
                     e_errors.ALARM,
                     e_errors.ERROR,
                     e_errors.USER_ERROR, ]

#Provide a way to get the logname in a thread safe way.
logname_data = threading.local()
default_logname = "UNKNOWN"


def get_logname():
    """Get the name of the logfile
    
    Gets the name of the logfile from the logname_data module. If the logname_data module does not exist,
    or does not contain the logname attribute, then the default_logname is used.
    
    Args:
        None
    
    Returns:
        str: The name of the logfile
    """
    return getattr(logname_data, "logname", default_logname)


def set_logname(new_logname):
    """    Set the name of the log file
    
    Args:
        new_logname (str): New log file name
    
    Returns:
        None
    """
    logname_data.logname = str(new_logname)


def set_max_message_size(maximum_message_size):
    """Set the maximum message size
    
    Sets the maximum message size for the current session.
    
    Args:
        maximum_message_size (int): Maximum message size (optional)
    
    Returns:
        None
    """
    global max_message_size
    max_message_size = maximum_message_size


#Provide a way to get the threadname in a thread safe way.
include_threadname = None


def get_threadname():
    """Get the name of the current thread
    
    Returns:
        str: Name of the current thread
    """
    global include_threadname
    #thread_lock.acquire()
    if include_threadname:
        thread = threading.current_thread()
        th_name = thread.getName()
    else:
        th_name = ""
    #thread_lock.release()
    return th_name


def log_thread(threadname_flag):
    """Set logging threadname flag
    
    Args:
        threadname_flag (bool): True to include thread name in log messages
    
    Returns:
        None
    """
    global include_threadname
    #thread_lock.acquire()
    include_threadname = bool(threadname_flag)
    #thread_lock.release()


alarm_func = None
log_func = None

print_levels = {}
log_levels = {}
alarm_levels = {}
message_levels = {}

# stuff added by efb for new event_relay_client
erc = None


def notify(msg):
    """Send a message to the event relay
    
    Sends a message to the event relay.
    
    Args:
        msg (str): Message to send (required)
    
    Returns:
        None
    """
    global erc
    if not erc:
        erc = event_relay_client.EventRelayClient()
    if type(msg) == types.StringType:
        # we must convert the message into a message instance
        msg = event_relay_messages.decode(msg)
    erc.send(msg)

# end of stuff added by efb


def trunc(x):
    """Truncate a string to a maximum length
    
    If the string is longer than the maximum length, truncate it and
    add a message indicating that it was truncated.
    
    Args:
        x (str): String to truncate
    
    Returns:
        str: Truncated string
    """
    if type(x) != type(""):
        x = str(x)
    if len(x) > max_message_size:
        x = x[:max_message_size - 20] + "(trunc. %s)" % (len(x),)
    return x

#Initialize the log and thread values.  This is done for the current
# thread and for the default of any future threads.


def init(name, include_thread_name=''):
    """Initialize logging
    
    Initialize logging, setting the default logname, and optionally including the thread name in the log.
    
    Args:
        name (str): Name of the log file (required)
        include_thread_name (str): Include the thread name in the log (optional)
    
    Returns:
        None
    """
    global default_logname

    set_logname(name)
    default_logname = logname_data.logname

    log_thread(include_thread_name)

###############################################################################

#message is a string to send to stdout or stderr.
#out_fp is sys.stdout, sys.stderr or file pointer


def write_trace_message(message, out_fp, append_newline=True):
    """
    Write a message to stdout, stderr, or a file
    
    Writes a message to stdout, stderr, or a file.
    
    Args:
        message (str): Message to write
        out_fp (file pointer): File pointer to write to
        append_newline (bool): Append newline to message (optional)
    
    Returns:
        None
    """

    print_lock.acquire()
    try:
        if append_newline:
            out_fp.write("%s\n" % (message,))
        else:
            out_fp.write("%s" % (message,))
        out_fp.flush()
    except (KeyboardInterrupt, SystemExit):
        print_lock.release()
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
    except:
        pass
    print_lock.release()

#out_fp is sys.stdout, sys.stderr or file pointer


def flush_and_sync(out_fp):
    """Flush and sync output file
    
    Flush and sync output file.
    
    Args:
        out_fp (file): Output file pointer
    
    Returns:
        None
    """
    print_lock.acquire()
    try:
        out_fp.flush()
        if out_fp not in [sys.stdout, sys.stderr]:
            #standard out and error don't fsysnc().
            os.fsync(out_fp.fileno())
    except (KeyboardInterrupt, SystemExit):
        print_lock.release()
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
    except:
        pass
    print_lock.release()

###############################################################################


def do_print(levels):
    """Set print level(s)
    
    Sets the print level(s) to print.
    
    Args:
        levels (list): List of print levels to print
    
    Returns:
        None
    """
    if type(levels) != type([]):
        levels = [levels]
    for level in levels:
        print_levels[level] = 1


def dont_print(levels):
    """Remove a level from the list of levels to print
    
    Args:
        levels (str): Level to remove from the list of levels to print
    
    Returns:
        None
    """
    if type(levels) != type([]):
        levels = [levels]
    for level in levels:
        if print_levels.has_key(level):
            del print_levels[level]


def do_log(levels):
    """Set logging level
    
    Sets the logging level to one or more of the following:
        critical
        error
        warning
        info
        debug
    
    Args:
        levels (list): List of logging levels to set
    
    Returns:
        None
    """
    if type(levels) != type([]):
        levels = [levels]
    for level in levels:
        log_levels[level] = 1


def dont_log(levels):
    """
    Turn off log levels
    
    Args:
        levels (list): log levels to turn off
    
    Returns:
        None
    """
    if type(levels) != type([]):
        levels = [levels]
    for level in levels:
        if level < 5:
            pass
        if log_levels.has_key(level):
            del log_levels[level]


def do_alarm(levels):
    """Set alarm levels
    
    Sets the alarm levels to the requested levels.
    
    Args:
        levels (list): List of alarm levels (optional)
    
    Returns:
        None
    """
    if type(levels) != type([]):
        levels = [levels]
    for level in levels:
        alarm_levels[level] = 1


def dont_alarm(levels):
    """Turn off alarm for a given level
    
    Args:
        levels (list): Alarm levels to turn off
    
    Returns:
        None
    """
    if type(levels) != type([]):
        levels = [levels]
    for level in levels:
        if level == 0:
            raise ValueError("alarm level 0 can not be turned off")
        if alarm_levels.has_key(level):
            del alarm_levels[level]


def do_message(levels):
    """Set message level
    
    Sets the message level to one or more of the following:
        "info", "warning", "error", "fatal"
    
    Args:
        levels (list): List of message levels to set
    
    Returns:
        None
    """
    if type(levels) != type([]):
        levels = [levels]
    for level in levels:
        message_levels[level] = 1


def dont_message(levels):
    """Turn off message for a given level
    
    Args:
        levels (list): message levels to turn off
    
    Returns:
        None
    """
    if type(levels) != type([]):
        levels = [levels]
    for level in levels:
        if level == 0:
            raise ValueError("message level 0 can not be turned off")
        if message_levels.has_key(level):
            del message_levels[level]

###############################################################################

#Take the original message string and add the log message format header.


def format_log_message(raw_message, msg_type=MSG_DEFAULT):
    """
  
     Format log message
  
     Takes the original message string and add the stderr/stdout message
     format header.
     
     Args:
         raw_message (str): Original message string (required)
         msg_type (str): Message type (optional)
     
     Returns:
         str: Formatted message string
     """

    # build up message
    if msg_type != MSG_DEFAULT:
        new_msg = "%s %s" % (msg_type, raw_message)
    else:
        new_msg = raw_message

    th_name = get_threadname()

    if th_name:
        new_msg = "%s Thread %s" % (new_msg, th_name)

    #Make sure log message will be sendable.
    message_truncated = trunc(new_msg)

    return message_truncated

#Take the original message string and add the stderr/stdout message
# format header.


def format_trace_message(severity, raw_message):
    """Format a trace message for display, adding
       the stdout/stderr message format header

    Args:
        severity (str): Severity of message (required)
        raw_message (str): Message to be formatted (required)

    Returns:
        str: Formatted message
    """
    global include_threadname

    t = time.time()
    dp = ("%3.2f" % (t - int(t),)).split('.')[1]
    a = time.ctime(t).split(" ")
    b = "."
    c = b.join((a[4], dp))
    a[4] = c
    b = " "
    tm = b.join(a)
    new_msg = raw_message
    if include_threadname:
        thread = threading.currentThread()
        if thread:
            th_name = thread.getName()
        else:
            th_name = ''
    else:
        th_name = ''

    if th_name:
        new_msg = "%s Thread %s" % (new_msg, th_name)

    #Note: we don't need to truncate messages going to stdout or stderr.

    new_message = b.join((str(severity), tm, new_msg))

    return new_message

###############################################################################


def log(severity, message, msg_type=MSG_DEFAULT, doprint=1, force_print=False):
    """
    For log(), alarm(), trace() and message():
    severity: This is an integer.  Typically it is one of e_errors.ALARM
    e_errors.ERROR, e_errors.USER_ERROR, e_errors.EMAIL,
    e_errors.WARNING, e_errors.INFO or e_errors.MISC.
    root_error, message: The orignal string to format for output.
    Send the message to the log server.
    message: message to send
    msg_type: One of the MSG_* constants from earlier in this file.
    doprint: If true, consider calling trace() too.
    force_print: if True do not use previouosly set log function, but rathe use default one which outputs to stdout
    """

    if force_print:
        local_log_func = default_log_func
    else:
        local_log_func = log_func
    if local_log_func:
        try:
            #Format the log text to include some standard information.
            new_msg = format_log_message(message, msg_type)
        except (KeyboardInterrupt, SystemExit):
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except:
            exc, detail = sys.exc_info()[:2]
            write_trace_message("Failed to make log message %s: %s\n" %
                                (message, str(detail)), sys.stderr)
            return
        try:
            #Send the log string to the log server.
            local_log_func(time.time(), os.getpid(), get_logname(), (severity, new_msg))
        except (KeyboardInterrupt, SystemExit):
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except:
            exc, detail = sys.exc_info()[:2]
            write_trace_message("Failure writing message to log %s: %s\n" %
                                (message, str(detail)), sys.stderr)

    if doprint and print_levels.has_key(severity):
        if severity in STDERR_SEVERITIES:
            use_out = sys.stderr
        else:
            use_out = sys.stdout

        trace(severity, message, dolog=0, doalarm=0, out_fp=use_out)

#Send the message to the log server.
#  rest: A dictionary with extra information for the adminsitrators.
#  condition:
#  remedy_type:
#  doprint: If true, consider calling trace() too.


def alarm(severity, root_error, rest={},
          condition=None, remedy_type=None, doprint=1):
    """Send alarm message to standard out (the default) or standard error.
        If severity is high enough, send the message to the log server.
    
    Args:
        severity (int): Severity of the message (optional)
        message (str): Message to send (required)
        dolog (bool): If true, consider sending the message to the log server too (optional)
        doalarm (bool): If true, consider sending the message to the alarm server too (optional)
        out_fp (file): File to send the message to (optional)
        append_newline (bool): If true, append a newline to the message (optional)
        force_print (bool): If true, do not use previously set log function, but rather use default one which outputs to stdout (optional)
    
    Returns:
        None
    """
    #log(severity, root_error, MSG_ALARM)
    if alarm_func:
        alarm_func(time.time(), os.getpid(), get_logname(), root_error,
                   severity, condition, remedy_type, rest)

    if doprint and print_levels.has_key(severity):
        if severity in STDERR_SEVERITIES:
            use_out = sys.stderr
        else:
            use_out = sys.stdout

        trace(severity, root_error, dolog=0, doalarm=0, out_fp=use_out)

#Send the message to the standard out (the default) or standard error.
#  dolog: If true, consider sending the message to the log server too.
#  doalarm: If true, consider sending the message to the alarm server too.


def trace(severity, message, dolog=1, doalarm=1, out_fp=sys.stdout,
          append_newline=True, force_print=False):
    """Send the message to the standard out (the default) or standard error.
    
    Args:
        severity (int): Severity of the message (optional)
        message (str): Message to send (required)
        dolog (bool): If true, consider sending the message to the log server too (optional)
        doalarm (bool): If true, consider sending the message to the alarm server too (optional)
        out_fp (file): File to send the message to (optional)
        append_newline (bool): If true, append a newline to the message (optional)
        force_print (bool): If true, do not use previously set log function, but rather use default one which outputs to stdout (optional)
    
    Returns:
        None
    """

    ## There is no need to waste time on creating a message, if it will not
    ## be sent.  Truncate all messages sent over the network, but not the
    ## messages to be printed to standard out.
    if (log_levels.has_key(severity) or alarm_levels.has_key(severity)):
        msg_truncated = trunc(message)
    else:
        if not print_levels.has_key(severity):
            return
    if print_levels.has_key(severity):
        #if out_fp not in [sys.stderr, sys.stdout]:
        #    write_trace_message("Neither stdout or stderr given.\n",
        #                        sys.stderr)
        #    return

        try:
            #Format the trace text to include the standard information.
            new_msg = format_trace_message(severity, message)
        except (KeyboardInterrupt, SystemExit):
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except:
            exc, detail = sys.exc_info()[:2]
            write_trace_message("Failed to make trace message %s: %s\n" %
                                (message, str(detail)), sys.stderr)
            return

        #Send the trace string to standard out or standard error.
        write_trace_message(new_msg, out_fp, append_newline=append_newline)

        """
        #print_lock.acquire()
        try:
	    # the following line will output the memory usage of the process
	    #os.system("a=`ps -ef |grep '/inq'|grep -v grep|xargs echo|cut -f2 -d' '`;ps -el|grep $a|grep python")
	    #print "================================="  # a usefull divider
            #sys.stdout.flush()
        except (KeyboardInterrupt, SystemExit):
            #print_lock.release()
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except:
            pass
        #print_lock.release()
        """

    if dolog and log_levels.has_key(severity):
        log(severity, msg_truncated, doprint=0, force_print=force_print)
    if doalarm and alarm_levels.has_key(severity):
        alarm(severity, msg_truncated, doprint=0)

#Send the message to the standard out (the default) or standard error.


def message(severity, message, out_fp=sys.stdout, append_newline=True):
    """Print a message to the requested output file
    
    Prints a message to the requested output file.
    
    Args:
        severity (str): Severity of the message (optional)
        message (str): Message to print (required)
        out_fp (file): File to print to (optional)
        append_newline (bool): Append a newline to the message (optional)
    
    Returns:
        None
    """
    new_msg = message
    if message_levels.has_key(severity):
        #if out_fp not in [sys.stderr, sys.stdout]:
        #    write_trace_message("Neither stdout or stderr given.\n",
        #                        sys.stderr)
        #    return
        write_trace_message(new_msg, out_fp, append_newline=append_newline)

###############################################################################


def set_alarm_func(func):
    """Set the alarm function

    Sets the alarm function to the function passed in.

    Args:
        func (function): Function to set as the alarm function

    Returns:
        None
    """
    global alarm_func
    alarm_func = func


def set_log_func(func):
    global log_func
    log_func = func

# defaults (templates) -- called from trace


def default_alarm_func(timestamp, pid, name, root_error, severity,
                       condition, remedy_type, args):
    """Default alarm function
    
    This is the default alarm function. It prints the arguments to the console.
    
    Args:
        timestamp (int): Timestamp of alarm (required)
        pid (int): Process ID (required)
        name (str): Name of process (required)
        root_error (str): Root error (required)
        severity (str): Severity of alarm (required)
        condition (str): Condition of alarm (required)
        remedy_type (str): Remedy type of alarm (required)
        args (list): List of arguments (required)
    
    Returns:
        None
    """
    __pychecker__ = "unusednames=timestamp,pid,name,root_error,severity," \
                    "condition,remedy_type,args"
    print "default_alarm_func:", args
    #lvl = args[0]
    #msg = args[1]
    return None


set_alarm_func(default_alarm_func)

#pid = os.getpid()
try:
    usr = pwd.getpwuid(os.getuid())[0]
except:
    usr = "unknown"


def default_log_func(timestamp, pid, name, args):
    """Default alarm function
    
    This is the default log function. 
    It formats and prints the arguments to the console.
    
    Args:
        timestamp (int): Timestamp of alarm (required)
        pid (int): Process ID (required)
        name (str): Name of process (required)
        args (list): List of arguments (required)
    
    Returns:
        None
    """
    #Even though this implimentation of log_func() does not use the time
    # parameter, others will.
    __pychecker__ = "unusednames=time"

    severity = args[0]
    msg = args[1]
    if severity > e_errors.MISC: severity = e_errors.MISC
    print '%s %.6d %.8s %s %s  %s' % (time.ctime(timestamp), pid, usr, e_errors.sevdict[severity], name, msg)
    return None


set_log_func(default_log_func)

###############################################################################

# log traceback info


def handle_error(exc=None, value=None, tb=None, severity=e_errors.ERROR,
                 msg_type=MSG_DEFAULT, force_print=False):
    """Log traceback info
    
    Logs traceback info.
    
    Args:
        exc (str): Exception (optional)
        value (str): Value (optional)
        tb (str): Traceback (optional)
        severity (str): Severity (optional)
        msg_type (str): Message type (optional)
        force_print (bool): Force print (optional)
    
    Returns:
        exc, value, tb
    """
    # store traceback info
    locally_obtained = False
    if not exc:
        locally_obtained = True
        exc, value, tb = sys.exc_info()

    # log it
    for l in traceback.format_exception(exc, value, tb):
        log(severity, l, msg_type, "TRACEBACK", force_print=force_print)

    if not locally_obtained:
        return exc, value, tb
    else:
        #Avoid a cyclic reference.
        del tb
        return sys.exc_info()

#log the current stack trace
# Normally, severity is e_errors.INFO, e_errors.ERROR, et. al.  Here, we
# jst want it to go to the DEBUGLOG, not the normal log; so we use 99 as
# the default.


def log_stack_trace(severity=99, msg_type=MSG_DEFAULT):
    """Log the current stack trace

    Logs the current stack trace at the given severity.
    Normally, severity is e_errors.INFO, e_errors.ERROR, et. al.  Here, we
    just want it to go to the DEBUGLOG, not the normal log; so we use 99 as
    the default.

    Args:
        severity (int): Severity of the message (optional)
        msg_type (int): Type of the message (optional)
        force_print (bool): Force printing the message (optional)

    Returns:
        None
    """
    # log it
    for l in traceback.format_stack():
        log(severity, l, msg_type, "STACKTRACE")
