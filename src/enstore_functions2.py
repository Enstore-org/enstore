import time
import string
import os
import exceptions
import tempfile
import types
import pwd
import signal
import stat

import enstore_constants

RMODE = 4
WMODE = 2
XMODE = 1

def get_mode(pmode, read_bit, write_bit, execute_bit):
    mode = 0
    if pmode & execute_bit:
        mode = XMODE
    if pmode & write_bit:
        mode = mode | WMODE
    if pmode & read_bit:
        mode = mode | RMODE
    return mode


# format the mode from the pnfs format to the traditional 3 chars and a
# leading zero.
def format_mode(pmode):
    # other mode bits
    omode = get_mode(pmode, stat.S_IROTH, stat.S_IWOTH, stat.S_IXOTH)
    # group mode bits
    gmode = get_mode(pmode, stat.S_IRGRP, stat.S_IWGRP, stat.S_IXGRP)
    # user mode bits
    umode = get_mode(pmode, stat.S_IRUSR, stat.S_IWUSR, stat.S_IXUSR)
    # now the ISUID and ISGID bits
    smode = get_mode(pmode, stat.S_ISUID, stat.S_ISGID, stat.S_ISVTX)
    return "%s%s%s%s"%(smode, umode, gmode, omode)

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
def send_mail(server, message, subject):
    mail_file = tempfile.mktemp()  
    os.system("date >> %s"%(mail_file,))
    os.system('echo "\n\tFrom: %s\n" >> %s' % (server, mail_file))
    os.system('echo "\t%s" >> %s' % (message, mail_file))
    os.system("/usr/bin/Mail -s \"%s\" $ENSTORE_MAIL < %s"%(subject, mail_file,))
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
                return enstore_constants.ALIVE
            else:
                return enstore_constants.DEAD
    else:
        # we did not find the stat line
        return enstore_constants.DEAD

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
