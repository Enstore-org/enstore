#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

import time
import string
import os
import exceptions
import tempfile
import types
import pwd
import signal
import stat
import re
import errno
import socket

import enstore_constants

###########################################################################
## conversion function for permissions
##
## symbolic_to_bits(): converts "ug=rw" into 432.
## numeric_to_bits(): converts "0660" into 432.
## bits_to_numeric(): converts 432 into "0660".
## bits_to_rwx(): converts 432 into '-rw-rw----'.
###########################################################################

RMODE = 4
WMODE = 2
XMODE = 1

def _get_mode(pmode, read_bit, write_bit, execute_bit):
    mode = 0
    if pmode & execute_bit:
        mode = XMODE
    if pmode & write_bit:
        mode = mode | WMODE
    if pmode & read_bit:
        mode = mode | RMODE
    return mode


#Convert the permission bits returned from os.stat() into the numeric
# (ie. "0777") chmod permissions.
def bits_to_numeric(pmode):
    # other mode bits
    omode = _get_mode(pmode, stat.S_IROTH, stat.S_IWOTH, stat.S_IXOTH)
    # group mode bits
    gmode = _get_mode(pmode, stat.S_IRGRP, stat.S_IWGRP, stat.S_IXGRP)
    # user mode bits
    umode = _get_mode(pmode, stat.S_IRUSR, stat.S_IWUSR, stat.S_IXUSR)
    # now the ISUID and ISGID bits
    smode = _get_mode(pmode, stat.S_ISUID, stat.S_ISGID, stat.S_ISVTX)
    return "%s%s%s%s"%(smode, umode, gmode, omode)

#legacy name:
format_mode = bits_to_numeric

def _get_rwx(pmode, read_bit, write_bit, execute_bit):
    ls_mode = ""
    if pmode & read_bit: #Handle the specified read bit.
        ls_mode = ls_mode + "r"
    else:
        ls_mode = ls_mode + "-"
    if pmode & write_bit: #Handle the specified write bit.
        ls_mode = ls_mode + "w"
    else:
        ls_mode = ls_mode + "-"
    #Handle the specifed execute bit... there are some special cases.
    if (pmode & execute_bit) and (execute_bit == stat.S_IXUSR) and \
       (pmode & stat.S_ISUID):
        ls_mode = ls_mode + "s"
    elif (pmode & execute_bit) and (execute_bit == stat.S_IXGRP) and \
         (pmode & stat.S_ISGID):
        ls_mode = ls_mode + "s"
    elif pmode & execute_bit:
        ls_mode = ls_mode + "x"
    else:
        ls_mode = ls_mode + "-"

    return ls_mode

#Convert the permission bits returned from os.stat() into the human readable
# format of the ls(1) command.
def bits_to_rwx(pmode, first_position = "-"):
    ls_mode = str(first_position)  #Only care about permissions here.

    umode = _get_rwx(pmode, stat.S_IRUSR, stat.S_IWUSR, stat.S_IXUSR)
    gmode = _get_rwx(pmode, stat.S_IRGRP, stat.S_IWGRP, stat.S_IXGRP)
    omode = _get_rwx(pmode, stat.S_IROTH, stat.S_IWOTH, stat.S_IXOTH)

    ls_mode = ls_mode + umode + gmode + omode

    return ls_mode

def _get_bits(pmode, read_bit, write_bit, execute_bit):
    mode = 0
    bits = int(pmode)
    if bits >= RMODE:
        mode = mode | read_bit
        bits = bits - RMODE
    if bits >= WMODE:
        mode = mode | write_bit
        bits = bits - WMODE        
    if bits >= XMODE:
        mode = mode | execute_bit
    return mode

#Convert the numeric (ie. "0777") chmod permisssions into the permission bits
# returned from os.stat().
def numeric_to_bits(numeric_mode):
    #Make sure that the correct type was passed in.
    if type(numeric_mode) != type("") and type(numeric_mode) != type(1):
        raise TypeError("Expected octal string or integer instead of %s." %
                        (type(numeric_mode),))
    
    #If the mode isn't consisting of octal digits this will raise an error.
    numeric_pattern = re.compile("^[01234567]{3,4}$")
    try:
        num_mod = numeric_pattern.search(str(numeric_mode)).group(0)
    except AttributeError:
        raise ValueError("%s: Invalid permission field" %
                         (os.strerror(errno.EINVAL),))

    #If only three values specified, insert a leading zero.
    if len(num_mod) == 3:
        num_mod = "0" + num_mod

    #Determine the bits to turn on.
    sbits = _get_bits(num_mod[0], stat.S_ISUID, stat.S_ISGID, stat.S_ISVTX)
    ubits = _get_bits(num_mod[1], stat.S_IRUSR, stat.S_IWUSR, stat.S_IXUSR)
    gbits = _get_bits(num_mod[2], stat.S_IRGRP, stat.S_IWGRP, stat.S_IXGRP)
    obits = _get_bits(num_mod[3], stat.S_IROTH, stat.S_IWOTH, stat.S_IXOTH)
                     
    return sbits | ubits | gbits | obits
           

#Convert the symbolic (ie. "u+wr") chmod permissions into the permission bits
# returned from os.stat().
def symbolic_to_bits(symbolic_mode, st_mode=0):
    #Make sure that the correct type was passed in.
    if type(symbolic_mode) != type(""):
        raise TypeError("Expected string, recieved %s instead." %
                        (type(symbolic_mode),))
    
    #Split the string to support the [ugoa][+-=][rwxs] way.
    users_pattern = re.compile("^[ugoa]+")
    chmod_pattern = re.compile("[-+=]")
    modes_pattern = re.compile("[rwxs]+$")

    try:
        user_mod = users_pattern.search(str(symbolic_mode)).group(0)
        type_mod = chmod_pattern.search(str(symbolic_mode)).group(0)
        mode_mod = modes_pattern.search(str(symbolic_mode)).group(0)
    except AttributeError:
        raise ValueError("%s: Invalid permission field" %
                         (os.strerror(errno.EINVAL),))
        #return 1

    #Test to make sure that the string was legal.
    if (user_mod + type_mod + mode_mod) != str(symbolic_mode):
        raise ValueError("%s: Invalid permission field" %
                         (os.strerror(errno.EINVAL),))
        return 1

    #Translate the all - "a" - possible user entry with ugo.
    user_mod = string.replace(user_mod, "a", "ugo")

    set_mode = 0
    for user in user_mod:
        #Translate users to names pieces.
        if user == "o":
            user_name = "OTH"
        elif user == "g":
            user_name = "GRP"
        elif user == "u":
            user_name = "USR"
        else:
            raise ValueError("%s: Error parsing mode" %
                             (os.strerror(errno.EINVAL),))
            #return 1

        change_mode = 0
        for mode in mode_mod:
            #handle sticky/set-id bit differently
            if mode == "s" and user == "u":
                name = "S_ISUID"
            elif mode == "s" and user == "g":
                name = "S_ISGID"
            elif mode == "s" and user == "o":
                continue
            elif mode == "t":
                name = "S_ISVTX"
            else:
                name = "S_I" + string.upper(mode) + user_name

            #This needs to be ored not added.
            change_mode = change_mode | getattr(stat, name)
        
        #Handle combining the existing and new modes correctly.
        if type_mod == "+":
            set_mode = set_mode | (change_mode | st_mode)
        elif type_mod == "-":
            temp = ((change_mode | st_mode) ^ change_mode)
            set_mode = set_mode | temp
        else:
            set_mode = set_mode | change_mode

    return set_mode

###########################################################################
##
###########################################################################

# return both the user associated with the uid and the euid.
def get_user():
    uid = os.getuid()
    euid = os.geteuid()
    username = pwd.getpwuid(uid)[0]
    eusername = pwd.getpwuid(euid)[0]

# return a string version of a list
def print_list(aList, sep=" "):
    str = ""
    for item in aList:
	str = "%s%s%s"%(str, item, sep)
    else:
	# remove the last separator
	str = str[0:-1]
    return str

# format the mail
def format_mail(goal, question, metric): 
    return "\n\n  GOAL: %s\n\n  QUESTION: %s\n\n  METRIC: %s"%(goal, question,
							       metric)

# send mail
def send_mail(server, message, subject, destination="$ENSTORE_MAIL"):
    mail_file = tempfile.mktemp()  
    os.system("date >> %s"%(mail_file,))
    os.system('echo "\n\tFrom: %s\n" >> %s' % (server, mail_file))
    os.system('echo "\t%s" >> %s' % (message, mail_file))
    os.system("/usr/bin/Mail -s \"%s\" %s < %s"%(subject, destination, mail_file,))
    os.system("rm %s"%(mail_file,))

def get_mover_status_filename():
    return "enstore_movers.html"

def override_to_status(override):
    # translate the override value to a real status
    if type(override) == types.ListType:
	# this is the new format
	override = override[0]
    index = enstore_constants.SAAG_STATUS.index(override)
    return enstore_constants.REAL_STATUS[index]

def get_days_ago(date, days_ago):
    # return the date that is days_ago before date
    seconds_ago = float(days_ago*86400)
    return date - seconds_ago

def ping(node):
    # ping the node to see if it is up.
    times_to_ping = 4
    # the timeout parameter does not work on d0ensrv2.
    timeout = 5
    #cmd = "ping -c %s -w %s %s"%(times_to_ping, timeout, node)
    cmd = "ping -c %s %s"%(times_to_ping, node)
    p = os.popen(cmd, 'r').readlines()
    for line in p:
        if not string.find(line, "transmitted") == -1:
            # this is the statistics line
            stats = string.split(line)
            if stats[0] == stats[3]:
                # transmitted packets = received packets
                return enstore_constants.IS_ALIVE
            else:
                return enstore_constants.IS_DEAD
    else:
        # we did not find the stat line
        return enstore_constants.IS_DEAD

def get_remote_file(node, file, newfile):
    # we have to make sure that the rcp does not hang in case the remote node is goofy
    pid = os.fork()
    if pid == 0:
	# this is the child
	rtn = os.system("enrcp %s:%s %s"%(node, file, newfile))
	os._exit(0)
    else:
	# this is the parent, allow a total of 30 seconds for the child
	for i in [0, 1, 2, 3, 4, 5]:
	    rtn = os.waitpid(pid, os.WNOHANG)
	    if rtn[0] == pid:
		return rtn[1] >> 8   # pick out the top 8 bits as the return code
	    time.sleep(5)
	else:
	    # the child has not finished, be brutal. it may be hung
	    print "killing the rcp - %s"%(pid,)
	    os.kill(pid, signal.SIGKILL)
	    return 1

# translate time.time output to a person readable format.
# strip off the day and reorganize things a little
YEARFMT = "%Y-%b-%d"
TIMEFMT = "%H:%M:%S"
def format_time(theTime, sep=" "):
    return time.strftime("%s%s%s"%(YEARFMT, sep, TIMEFMT), time.localtime(theTime))

PLOTYEARFMT = "%Y-%m-%d"
def format_plot_time(theTime):
    return time.strftime("%s"%(PLOTYEARFMT,), time.localtime(theTime))

def unformat_time(strTime, sep=" "):
    time_t = time.strptime(strTime,"%s%s%s"%(YEARFMT, sep, TIMEFMT))
    return time.mktime(time_t)

# return the directory
def get_dir(str):
    if os.path.isdir(str):
	return str
    else:
	# strip off the last set of chars after the last /
	file_spec = os.path.split(str)
	return file_spec[0]

# strip off anything before the '/'
def strip_file_dir(str):
    ind = string.rfind(str, "/")
    if not ind == -1:
        str2 = str[(ind+1):]
    else:
        str2 = str
    return str2

# remove the string .fnal.gov if it is in the input string
def strip_node(str):
    if type(str) == types.StringType:
	return string.replace(str, ".fnal.gov", "")
    else:
	return str

def is_this(server, suffix):
    stype = string.split(server, ".")
    if stype[len(stype)-1] == suffix:
        return 1
    return 0

# return true if the passed server name ends in "library_manager"
def is_library_manager(server):
    return is_this(server, enstore_constants.LIBRARY_MANAGER)

# return true if the passed server name ends in "mover"
def is_mover(server):
    return is_this(server, enstore_constants.MOVER)

# return true if the passed server name ends in "media_changer"
def is_media_changer(server):
    return is_this(server, enstore_constants.MEDIA_CHANGER)

def get_name(server):
    return string.split(server, ".")[0]

def get_bpd_subdir(dir):
    new_dir = "%s/%s"%(dir, enstore_constants.BPD_SUBDIR)
    if not os.path.exists(new_dir):
	# doesn't exist, use the old one
	new_dir = dir
    return new_dir

# return true if the passed server name is one of the following -
#   file_clerk, volume_clerk, alarm_server, inquisitor, log_server, config
#   server, event_relay
def is_generic_server(server):
    if server in enstore_constants.GENERIC_SERVERS:
        return 1
    return 0

def get_status(dict):
    status = dict.get('status', None)
    if status is None or type(status) != type(()):
        return None
    else:
        return status[0]

DEFAULT_HOST = 'localhost'
DEFAULT_PORT = 7500

def default_host():
    val = os.environ.get('ENSTORE_CONFIG_HOST')
    if val:
        return val
    else:
        return DEFAULT_HOST

def default_port():
    val = os.environ.get('ENSTORE_CONFIG_PORT')
    if val:
        return int(val)
    else:
        return DEFAULT_PORT


# generate the full path name to the file
def fullpath(filename):
    if not filename:
        return None, None, None, None
    elif type(filename) != types.StringType:
        return None, None, None, None

    hostname=socket.gethostname()
    hostinfo=socket.gethostbyaddr(hostname)
    machine = hostinfo[0]  #hostaddr.gethostinfo()[0]

    #Expand the path to the complete absolute path.
    filepath = os.path.expandvars(filename)
    filepath = os.path.expanduser(filepath)
    filepath = os.path.abspath(filepath)
    filepath = os.path.abspath(filepath)

    #These functions will remove a tailing "/", put it back.
    if filename[-1] == "/":
        filepath = filepath + "/"
        
    dirname, basename = os.path.split(filepath)

    return machine, filepath, dirname, basename
