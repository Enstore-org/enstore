#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

# system imports
import sys
import time
import string
import os
#import exceptions
#import tempfile
import types
#import pwd
import signal
import stat
import re
import errno
import socket
import pwd
import subprocess

# enstore imports
### enstore_constants should be the only enstore import in this module!
import enstore_constants
import Interfaces


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
#def get_user():
#    uid = os.getuid()
#    euid = os.geteuid()
#    username = pwd.getpwuid(uid)[0]
#    eusername = pwd.getpwuid(euid)[0]

# return a string version of a list
def print_list(aList, sep=" "):
    the_str = ""
    for item in aList:
	the_str = "%s%s%s"%(the_str, item, sep)
    else:
	# remove the last separator
	the_str = the_str[0:-1]
    return the_str

def get_mover_status_filename():
    return "enstore_movers.html"

def get_migrator_status_filename():
    return "enstore_migrators.html"

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

def ping(node, IPv=4):
    # ping the node to see if it is up.
    times_to_ping = 4
    if IPv == 4:
        cmd = "ping -c %s %s" % (times_to_ping, node)
    elif IPv == 6:
        cmd = "ping6 -c %s %s" % (times_to_ping, node)
    else:
        raise ValueError("%s: IPv%s does not exist" %(__file__,IPv))
    p = os.popen(cmd, 'r').readlines()
    for line in p:
        if not string.find(line, "transmitted") == -1:
            # this is the statistics line
            stats = string.split(line)
            if stats[0] == stats[3] \
               and stats[0].isdigit() \
               and int(stats[0]) > 0:
                # transmitted packets = received packets
                return enstore_constants.IS_ALIVE
            else:
                return enstore_constants.IS_DEAD
    else:
        # we did not find the stat line
        return enstore_constants.IS_DEAD


def get_remote_file(node, remote_file, newfile):
    __pychecker__ = "unusednames=i"

    # we have to make sure that the rcp does not hang in case the remote node is goofy
    pid = os.fork()
    if pid == 0:
        # this is the child
        rtn = subprocess.call("enrcp %s:%s %s" % (node, remote_file, newfile),
                              shell=True)
        os._exit(rtn)
    else:
        # this is the parent, allow a total of 30 seconds for the child
        for i in [0, 1, 2, 3, 4, 5]:
            rtn = os.waitpid(pid, os.WNOHANG)
            if rtn[0] == pid:
                # pick out the top 8 bits as the return code
                return rtn[1] >> 8
            time.sleep(5)
        else:
            # the child has not finished, be brutal. it may be hung
            print "killing the rcp - %s" % (pid,)
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

# return true if the passed server name ends in "udp_proxy_server"
def is_udp_proxy_server(server):
    return is_this(server, enstore_constants.UDP_PROXY_SERVER)

# return true if the passed server name ends in "mover"
def is_mover(server):
    return is_this(server, enstore_constants.MOVER)

# return true if the passed server name ends in "migrator"
def is_migrator(server):
    return is_this(server, enstore_constants.MIGRATOR)

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

# execute shell command
# replaces popen2.popen3
# diffrerence: popen2.popen3 returns file objects
# subprocess.Popen returns stdout and stderror as a single lines
# to make a list of lines do the following with returned
# result: 
# stdout_lines = result[0].split("\n")
# stderr_lines = result[1].split("\n")
# returns (stdout, stderr)
# if there was no stdout or stderr None is returned in corresponding fields
def shell_command(command):
    pipeObj = subprocess.Popen(command,
                               stdin=subprocess.PIPE,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE,
                               shell=True,
                               close_fds=True)
    if pipeObj == None:
        return None
    # get stdout and stderr
    result = pipeObj.communicate()
    del(pipeObj)
    return result 

# same as shell command, but
# returns
# (command return code,
# stdout,
# stderr)
def shell_command2(command):
    pipeObj = subprocess.Popen(command,
                               stdin=subprocess.PIPE,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE,
                               shell=True,
                               close_fds=True)
    if pipeObj == None:
        return None
    # get stdout and stderr
    result = pipeObj.communicate()
    rc = [pipeObj.returncode]
    del(pipeObj)
    for r in result:
        rc.append(r)
    return tuple(rc) 

###########################################################################
##
###########################################################################

def __get_wormhole(lname):
    #if lname not in ["ENSTORE_CONFIG_HOST", "ENSTORE_CONFIG_PORT",
    #                 "ENSTORE_CONFIG_FILE"]:
    #    raise ValueError("Expected ENSTORE_CONFIG_HOST, ENSTORE_CONFIG_PORT"
    #                     " or ENSTORE_CONFIG_FILE")

    #Read in the /etc/mtab file.
    for mtab_file in ["/etc/mtab", "/etc/mnttab"]:
        try:
            fp = open(mtab_file, "r")
            mtab_data = fp.readlines()
            fp.close()
            break
        except (OSError, IOError), msg:
            if msg.args[0] in [errno.ENOENT]:
                continue
            else:
                raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
    else:
        #Should this raise an error?
        mtab_data = []

    list_of_enstore_files = []
    for line in mtab_data:
        #The 2nd and 3rd items in the list are important to us here.
        data = line[:-1].split()
        mp = data[1]
        fs_type = data[2]

        #If the filesystem is not an NFS filesystem, skip it.
        if fs_type != "nfs":
            continue

        en_info_filename = os.path.join(mp, ".(config)(enstore)", "enstorerc")
        list_of_enstore_files.append(en_info_filename)


    remember_en_data = []
    #Loop over these files looking for the config info.
    for filename in list_of_enstore_files:
        result = __read_enstore_conf(filename, lname)
        if result and result not in remember_en_data:
            #We need to keep track of how many different systems we find.
            remember_en_data.append(result)

    if len(remember_en_data) == 1:
        return remember_en_data[0]
    elif len(remember_en_data) > 1:
        #We have already looked at environmental variables and the .enstorerc
        # file.  If we get here, then we don't know which 
        raise ValueError("%s value not set or not unique" % lname)

    return None

#Copied find_config_file() from host_config.py.
def __find_config_file():
    config_host = os.environ.get("ENSTORE_CONFIG_HOST", None)
    if config_host:
        filename = '/etc/'+config_host+'.enstore.conf'
    	if not os.path.exists(filename):
            filename = '/etc/enstore.conf'
    else:
        filename = '/etc/enstore.conf'
    filename = os.environ.get("ENSTORE_CONF", filename)
    #Make sure that the specified file exists and is a regular file.
    #Note: If the $ENSTORE_CONF environmental variable specifies a file
    # that exists and is not an actual enstore.conf file, there will be
    # some serious problems later on.
    if os.path.exists(filename) and os.path.isfile(filename):
        return filename
    return None

def __get_enstorerc(lname):
    #if lname not in ["ENSTORE_CONFIG_HOST", "ENSTORE_CONFIG_PORT",
    #                 "ENSTORE_CONFIG_FILE"]:
    #    raise ValueError("Expected ENSTORE_CONFIG_HOST, ENSTORE_CONFIG_PORT"
    #                     " or ENSTORE_CONFIG_FILE")

    list_of_enstore_files = []

    #
    #First obtain the location of the home area for this user.
    #
    try:
        user_login_info = pwd.getpwuid(os.geteuid())
        enstorerc_filename = os.path.join(user_login_info[5], ".enstorerc")
        #Append the file to the list of places to look at.
        list_of_enstore_files.append(enstorerc_filename)
    except KeyError:
        pass
    except (OSError, IOError):
        pass

    #
    #Second obtain the location of the enstore.conf file.
    #
    enstore_conf_filename = __find_config_file()
    if enstore_conf_filename:
        list_of_enstore_files.append(enstore_conf_filename)

    #Loop over these files looking for the config info.
    for filename in list_of_enstore_files:
        result = __read_enstore_conf(filename, lname)
        if result:
            return result

    return None

def __read_enstore_conf(filename, line_target):
    #Next read in (if present) the next enstore config file.
    try:
        enstore_conf_file = open(filename, "r")
        enstore_conf_data = enstore_conf_file.readlines()
        enstore_conf_file.close()
    except (OSError, IOError):
        return None

    #Look for lines beginning with the string in lname.
    for line in enstore_conf_data:
        line = line.strip()
        if line[:len(line_target)] == line_target:
            #If there happens
            equals_index = line[len(line_target):].find("=")
            if equals_index == -1:
                rtn_val = line[len(line_target):].strip()
            else:
                #If we found an equals sign, we need to skip 1 past it.
                rtn_val = line[len(line_target):][equals_index + 1:].strip()
            return rtn_val

    return None
            
#DEFAULT_HOST = 'localhost'
#DEFAULT_PORT = 7500

used_default_config_host = None
used_default_config_port = None
used_default_config_file = None

#First look for the ENSTORE_CONFIG_HOST/PORT/FILE values in environmental
# variables.  Failing that try looking for a ~/.enstorerc file.  If that
# still doesn't work try looking for a wormhole file in PNFS.  Lastly,
# just return the default constants.
def _get_value(requested_val, default_val):
    val = os.environ.get(requested_val)
    if val:
        return val, False
    else:
        val2 = __get_enstorerc(requested_val)
        if val2:
            return val2, False
        else:
            val3 = __get_wormhole(requested_val)
            if val3:
                return val3, False
            else:
                return default_val, True

    return None, False  #Impossible to get here.

def default_value(requested_val):
    return _get_value(requested_val, None)[0]

def used_default_host():
    global used_default_config_host
    if used_default_config_host == None:
        #If we haven't called default_host() yet, we need to call _get_env()
        # directly to return the correct info.
        unsued, used_default_config_host = _get_value(
            "ENSTORE_CONFIG_HOST", enstore_constants.DEFAULT_CONF_HOST)
        return used_default_config_host
    else:
        return used_default_config_host

def default_host():
    global used_default_config_host
    #Besides returning the ip/hostname, set used_default_config_host if
    # to false if this value was configured or true if we are using
    # enstore_constants.DEFAULT_HOST.
    rtn_val, used_default_config_host = _get_value(
        "ENSTORE_CONFIG_HOST", enstore_constants.DEFAULT_CONF_HOST)
    return rtn_val

def used_default_port():
    global used_default_config_port
    if used_default_config_port == None:
        #If we haven't called default_port() yet, we need to call _get_env()
        # directly to return the correct info.
        unsued, used_default_config_port = _get_value(
            "ENSTORE_CONFIG_PORT", enstore_constants.DEFAULT_CONF_PORT)
        return used_default_config_port
    else:
        return used_default_config_port

def default_port():
    global used_default_config_port
    #Besides returning the ip/hostname, set used_default_config_host if
    # to false if this value was configured or true if we are using
    # enstore_constants.DEFAULT_HOST.
    rtn_val, used_default_config_port = _get_value(
        "ENSTORE_CONFIG_PORT", enstore_constants.DEFAULT_CONF_PORT)
    return int(rtn_val)

def used_default_file():
    global used_default_config_file
    if used_default_config_file == None:
        #If we haven't called default_FILE() yet, we need to call _get_env()
        # directly to return the correct info.
        unsued, used_default_config_file = _get_value(
            "ENSTORE_CONFIG_FILE", enstore_constants.DEFAULT_CONF_FILE)
        return used_default_config_file
    else:
        return used_default_config_file

def default_file():
    global used_default_config_file
    #Besides returning the ip/hostname, set used_default_config_host if
    # to false if this value was configured or true if we are using
    # enstore_constants.DEFAULT_HOST.
    rtn_val, used_default_config_file = _get_value(
        "ENSTORE_CONFIG_FILE", enstore_constants.DEFAULT_CONF_FILE)
    return rtn_val

###########################################################################
##
###########################################################################

def expand_path(filename):
    #Expand the path to the complete absolute path.
    filepath = os.path.expandvars(filename)
    filepath = os.path.expanduser(filepath)
    if filepath[0] != "/":
        # It would be nice to just do the following:
        #   fullfilepath = os.path.abspath(filepath)
        # But it has been found that when the current workng directory
        # is in PNFS, that on rare occasions a different path is returned
        # from os.getcwd().  Specifically, paths with .(access)() as
        # path components replace their normal directory name.
        #
        # It is possible to cd to one of these .(access)() paths.  So, we
        # need to accept them if this is true and stop after a while.
        #
        # This has only started to be seen on SLF5.
        basedir = os.getcwd()
        i = 0
        while basedir.find(".(access)") != -1 and i < 3:
            i = i + 1
            time.sleep(1)
            basedir = os.getcwd()
        fullfilepath = os.path.join(basedir, filepath)
    else:
        fullfilepath = filepath

    
    #If the target was a directory, handle it slightly differently.
    #if filename[-1] == "/":
    #    fullfilepath = fullfilepath + "/"

    return fullfilepath

# generate the full path name to the file
def fullpath(filename):
    if not filename:
        return None, None, None, None
    elif type(filename) != types.StringType:
        return None, None, None, None

    #Detemine if a host and port have been specifed on the command line.
    host_and_port = re.search("^[a-z0-9.]*:[0-9]*/", filename)
    if host_and_port != None:
        filename = filename[len(host_and_port.group()):]

    try_count = 0
    while try_count < 60:
        try:
            machine = socket.getfqdn(socket.gethostname())
            break
        except (socket.error), msg:
            #One known way to get here is to run out of file
            # descriptors.  I'm sure there are others.
            this_errno = msg.args[0]
            if this_errno == errno.EAGAIN or this_errno == errno.EINTR:
                try_count = try_count + 1
                time.sleep(1)
            else:
                machine = None
                break
        except (socket.gaierror, socket.herror), msg:
            this_herrno = msg.args[0]
            if this_herrno == socket.EAI_AGAIN:
                try_count = try_count + 1
                time.sleep(1)
            else:
                machine = None
                break
    else:
        machine = None

    #Expand the path to the complete absolute path.
    filepath = expand_path(filename)

    #If the target was a directory, handle it slightly differently.
    if filename[-1] == "/" or os.path.isdir(filename):
        dirname, basename = (filepath, "")
    else:
        dirname, basename = os.path.split(filepath)

    return machine, filepath, dirname, basename

# generate the full path name to the file
#If no_split is true, the last two paramaters for directory and basename
# are returned as None, to avoid calling stat() (via os.path.isdir()).
def fullpath2(filename, no_split = None):
    if not filename:
        return None, None, None, None, None, None
    elif type(filename) != types.StringType:
        return None, None, None, None, None, None

    #Split off a protocol.
    #en_protocol = re.search("^[a-zA-Z-0-9]+://", filename)
    #if en_protocol:
    #    protocol = en_protocol.group()[:-3]
    #
    #    filename = filename[len(en_protocol.group()):]
    #else:
    protocol = ""

    #Detemine if a host and port have been specifed on the command line.
    host_and_port = re.search("^[a-z0-9.]*:[0-9]*/", filename)
    if host_and_port != None:
        hostname = host_and_port.group().split(":")[0]
        try:
            portnumber = int(host_and_port.group()[:-1].split(":")[1])
        except ValueError:
            #Trying to convert an empty string to int gives this error.
            portnumber = None

        filename = filename[len(host_and_port.group()):]
    else:
        hostname = socket.gethostname()
        portnumber = None

    #Fill in some values if they are empty.
    if not hostname:
        hostname = socket.gethostname()
    if not portnumber:
        portnumber = None

    try_count = 0
    while try_count < 60:
        try:
            machine = socket.getfqdn(hostname)
            break
        except (socket.error), msg:
            this_errno = msg.args[0]
            if this_errno == errno.EAGAIN or this_errno == errno.EINTR:
                try_count = try_count + 1
                time.sleep(1)
            else:
                raise socket.error, msg, sys.exc_info()[2]
        except (socket.gaierror, socket.herror): #, msg:
            msg = sys.exc_info()[1] #msg set here to appease pychecker.
            this_herrno = msg.args[0]
            if this_herrno == socket.EAI_AGAIN:
                try_count = try_count + 1
                time.sleep(1)
            else:
                raise sys.exc_info()[0], msg, sys.exc_info()[2]
    else:
        #If finding the full name fails, go with what we know.
        machine = hostname

    #Expand the path to the complete absolute path.
    filepath = expand_path(filename)

    #If the user doesn't want the path split into directories and the filename.
    if no_split:
        dirname, basename = (None, None)
    #If the target was a directory, handle it slightly differently.
    elif filename[-1] == "/" or os.path.isdir(filename):
        dirname, basename = (filepath, "")
    else:
        dirname, basename = os.path.split(filepath)

    return protocol, machine, portnumber, filepath, dirname, basename

###########################################################################
##
###########################################################################

#global cache to avoid looking up the same ip address over and over for the
# machine it is running on when starting or stoping multiple Enstore servers.
# See function this_host().  Starting a servers worth of Enstore server
# processes shouldn't take that long; not long enough to need to worry
# about the hostname and ip address list changing.
host_names_and_ips = None

#Return all IP address and hostnames for this node/host/machine.
def this_host():
    global host_names_and_ips  #global cache variable
    
    if host_names_and_ips == None:
        try:
            #rtn = socket.gethostbyname_ex(socket.getfqdn())
            hostname = socket.getfqdn()
            rtn = socket.getaddrinfo(hostname, None)
        except (socket.error, socket.herror, socket.gaierror), msg:
            try:
                message = "unable to obtain hostname information: %s\n" \
                          % (str(msg),)
                sys.stderr.write(message)
                sys.stderr.flush()
            except IOError:
                pass
            sys.exit(1)
        rtn_formated = [hostname, hostname.split('.')[0], rtn[0][4][0]]

        interfaces_list = Interfaces.interfacesGet()
        for interface in interfaces_list.keys():
            ip = interfaces_list[interface]['ip']
            if ip == "127.0.0.1":
                continue
            try:
                rc = socket.gethostbyaddr(ip)
            except (socket.error, socket.herror, socket.gaierror), msg:
                try:
                    message = "unable to obtain hostname information: %s\n" \
                              % (str(msg),)
                    sys.stderr.write(message)
                    sys.stderr.flush()
                except IOError:
                    pass
                sys.exit(1)
            rc_formated = [rc[0]] + rc[1] + rc[2]

            rtn_formated = rtn_formated + rc_formated

        host_names_and_ips = rtn_formated

    return host_names_and_ips

def is_on_host(host):
    if host in this_host():
        return 1

    return 0

###########################################################################
### Here are some migration related functions that check for certain
### states in the metadata. 
###########################################################################

def is_readonly_state(state):
    if str(state) in ['full', 'readonly',
                      'migrated', 'duplicated', 'cloned',
                      'migrating', 'duplicating', 'cloning']:
        return 1

    return 0

def is_readable_state(state):
    if str(state) in ['none', 'full', 'readonly',
                      'migrated', 'duplicated', 'cloned',
                      'migrating', 'duplicating', 'cloning']:
        return 1

    return 0

def is_migration_state(state):
    if str(state) in ['migrated', 'duplicated', 'cloned',
                      'migrating', 'duplicating', 'cloning']:
        return 1

    return 0

def is_migrated_state(state):
    if str(state) in ['migrated', 'duplicated', 'cloned']:
        return 1

    return 0

def is_migrating_state(state):
    if str(state) in ['migrating', 'duplicating', 'cloning']:
        return 1

    return 0

migration_compile = re.compile(".*-MIGRATION.*")
def is_migration_file_family(ff):
    if migration_compile.match(ff):
        return True

    return False

duplication_compile = re.compile(".*_copy_[1-9]*.*")
def is_duplication_file_family(ff):
    if duplication_compile.match(ff):
        return True

    return False

#This one tests for the file family being either for migration or duplication.
def is_migration_related_file_family(ff):
    rtn = is_migration_file_family(ff)
    if rtn:
        return rtn

    return is_duplication_file_family(ff)

###########################################################################
### Function, convert_version(), and helper functions to convert an encp
### version string into compoenent parts for >, < and == comparision.
###########################################################################

#  This one splits a string using spaces and punctuation as seperators.
re_split_components = re.compile("[a-zA-Z0-9]+")
#   This one splits groups of letters and numbers.
re_split_version = re.compile("[a-zA-Z]+|[0-9]+")

#If the value is all digits, convert the string to an int.
#Don't consider list types internally.
def __int_convert(value):
    if value.isdigit():
        return int(value)  #Convert strings of only digits.
    else:
        return value  #Leave everything else alone.

#Return the version broken up into groups of letters and numbers.
# "v3_10_1a" becomes [['v', 3], [10], [1, 'a']]
# The python <, > and = operators can be used to compare two return values.
def convert_version(version):
    #Use regular experssion to break the version string into its
    # component parts, then seperate each component part into letter and
    # number parts.  Digits are converted to integers for correct
    # comparisions; use the fact that there are only two levels deep
    # possible after the second regular expression return to skip uncecessary
    # checks.
    return [map(__int_convert, item) for item in map(re_split_version.findall,
                                                     re_split_components.findall(version))]
