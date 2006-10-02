#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

#############################################################################
# Environmental variables that effect encp.
#
# $ENSTORE_CONFIG_HOST = The hostname/ip of the configuration server.
# $ENSTORE_CONFIG_PORT = The port number of the configuration server.
# $ENSTORE_SPECIAL_LIB = Override the library manager to use.  Use with care.
#                        Its original purpose was to use a migration LM
#                        for the 9940A to 9940B conversion.  (reads only)
# $ENCP_DAQ = <Its a mover thing.>
# $ENCP_CANONICAL_DOMAINNAME = Encp will attempt on reads to try the three
#                              paths to a file: /pnfs/xyz, /pnfs/fs/usr/xyz and
#                              /pnfs/fnal.gov/usr/xyz.  Most on site machines
#                              whoose domain matches the second component of
#                              the last pathname do not have to worry.  One
#                              example where this override would be needed
#                              are machines whoose domain name looks like:
#                              dhcp.fnal.gov instead of simply fnal.gov.
#                              Another example would be nodes in a different
#                              domain/site all together (i.e. sudan.org).
# $ENSTORE_CONF (host_config.py): Override default enstore.conf file name.
# $ENSTORE_RANDOM_LB = Lower limit on the randomly choosen size when reading
#                      from /dev/zero, /dev/random or /dev/urandon.
# $ENSTORE_RANDOM_UB = Upper limit on the randomly choosen size when reading
#                      from /dev/zero, /dev/random or /dev/urandon.
############################################################################

# system imports
import sys
import os
import stat
import time
import errno
import pprint
import pwd
import grp
import socket
#import pdb
import string
#import traceback
import select
import fcntl
#if sys.version_info < (2, 2, 0):
#    import FCNTL #FCNTL is depricated in python 2.2 and later.
#    fcntl.F_GETFL = FCNTL.F_GETFL
#    fcntl.F_SETFL = FCNTL.F_SETFL
#import exceptions
import re
import statvfs
import types
import gc
import copy
import random

# enstore modules
import Trace
import e_errors
import option
import pnfs
import callback
import udp_server
import configuration_client
import log_client
import alarm_client
import volume_clerk_client
import file_clerk_client
import accounting_client
import library_manager_client
import EXfer
import hostaddr
import host_config
import atomic
import delete_at_exit
import charset
import volume_family
import enstore_constants
import enstore_functions2
import pnfs_agent_client


#Add these if missing.
if not hasattr(socket, "IPTOS_LOWDELAY"):
    socket.IPTOS_LOWDELAY = 0x10                 #16
if not hasattr(socket, "IPTOS_THROUGHPUT"):
    socket.IPTOS_THROUGHPUT = 0x08               #8
if not hasattr(socket, "IPTOS_RELIABILITY"):
    socket.IPTOS_RELIABILITY = 0x04              #4
if not hasattr(socket, "IPTOS_MINCOST"):
    socket.IPTOS_IPTOS_MINCOST = 0x02            #2
if not hasattr(socket, "SHUT_RD"):
    socket.SHUT_RD = 0
if not hasattr(socket, "SHUT_WR"):
    socket.SHUT_WR = 1
if not hasattr(socket, "SHUT_RDRW"):
    socket.SHUT_RDWR = 2

# Forward declaration.  It is assigned in get_clerks().
__acc = None
__csc = None
__fcc = None
__vcc = None
__logc = None
__alarmc = None
__pac = None

#Constants for the max file size.  Currently this assumes the max for the
# cpio_odc wrapper format.  The -1s are necessary since that is the size
# that fits in signed integer variables.
ONE_G = 1024 * 1024 * 1024
TWO_G = 2 * long(ONE_G) - 1     #Used in int32()
MAX_FILE_SIZE = long(ONE_G) * 2 - 1    # don't get overflow

MAX_VERSION_LENGTH = 48  #Length of the version string.

UNIXFILE = "unixfile"
HSMFILE = "hsmfile"
RHSMFILE = "rhsmfile"

#############################################################################
# verbose: Roughly, 10 verbose levels are used.
# 0: Print nothing except fatal errors.
# 1: Print message for complete success.
# 2: Print non-fatal error messages.
# 4: Print (short) info about the read/write status.
# 6: Print (short) information on the number of files left to transfer.
# 7: Print additional information about the current state of the transfer.
# 8: Print info about system config.
# 9: Print timing information.
# 10: Print (long) info about everthing.
#############################################################################
DONE_LEVEL     = 1
ERROR_LEVEL    = 2
TRANSFER_LEVEL = 4
TO_GO_LEVEL    = 6
INFO_LEVEL     = 7
CONFIG_LEVEL   = 8
TIME_LEVEL     = 9
TICKET_LEVEL   = 10
TICKET_1_LEVEL = 11

#This is the global used by print_data_access_layer_format().  It uses it to
# determine whether standard out or error is used.
#  1 - Means always print this output; regardless of success or failure.
#  0 - Means only print this output on fatal error.
# -1 - Means never print this output.
data_access_layer_requested = False
#This is the global used by Pnfs().  It used to force use of the pnfs agent.
pnfs_agent_client_requested = False
pnfs_agent_client_allowed = False

#Initial seed for generate_unique_id().
_counter = 0
#Initial seed for generate_unique_msg_id().
_msg_counter = 0

#This is the largest 16 bit prime number.  It is used for converting the
# 1 seeded dcache CRCs with the 0 seeded enstore CRCs.
BASE = 65521

#Completion status field values.
SUCCESS = "SUCCESS"
FAILURE = "FAILURE"

# int32(v) -- if v > 2^31-1, make it long
#
# a quick fix for those 64 bit machine that consider int is 64 bit ...

def int32(v):
    if v > TWO_G:
        return long(v)
    else:
        return v

##############################################################################

def __is_pnfs_local_path(filename, check_name_only = None):
    if os.uname()[0] == "SunOS":
        mtab_filename = "/etc/mntab"
    else:
        mtab_filename = "/etc/mtab"

    #Search throught the mtab file looking for a matching pnfs filesystem.
    try:
        fp = open(mtab_filename, "r")
        for line in fp.readlines():
            #The 2nd and 3rd items in the list are important to us here.
            data = line[:-1].split()
            mp = data[1]
            fs_type = data[2]

            if fs_type != "nfs":
                # We can exclude non-nfs filesystems.
                continue
            elif not os.path.exists(os.path.join(mp, ".(get)(cursor)")):
                # We also can exclude real nfs filesystems.
                continue

            #Compare the beginning of the pathname with the mountpoint.
            # If they match, then we are good.  We need to ignore "/"
            # because it would match with everything.
            if mp != "/" and filename[:len(mp)] == mp:
                rtn = True
                break
        else:
            rtn = False
        fp.close()
    except (KeyboardInterrupt, SystemExit):
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
    except:
        pass

    if check_name_only:
        #If we get here we only want to determine if the filesystem is
        # valid pnfs filesystem.  Not whether the target actually exists.
        return rtn

    if rtn:
        return os.path.exists(filename)

    return rtn  #Always False

def __is_pnfs_remote_path(filename, check_name_only = None):
    if pnfs_agent_client_requested or pnfs_agent_client_allowed:
        pac = get_pac()
        rtn = pac.is_pnfs_path(filename, check_name_only = check_name_only)

        if check_name_only:
            #If we get here we only want to determine if the filesystem is
            # valid pnfs filesystem.  Not whether the target actually exists.
            return rtn

        return pac.e_access(filename, os.F_OK)

    return False

############################################################################

class EncpError(Exception):
    def __init__(self, e_errno, e_message, e_type, e_ticket={}):

        Exception.__init__(self)

        #Handle the errno (if a valid one passed in).
        if e_errno in errno.errorcode.keys():
            self.errno = e_errno
        else:
            self.errno = None

        #Handel the message if not given.
        if e_message == None:
            if e_errno: #By now this is None or a valid errno.
                self.message = os.strerror(self.errno)
            else:
                self.message = None
        elif type(e_message) == types.StringType:
            self.message = e_message #There was a string message passed.
        else:
            self.message = None

        #Type should be from e_errors.py.  If not specified, use errno code.
        if not e_type:
            try:
                self.type = errno.errorcode[self.errno]
            except KeyError:
                self.type = e_errors.UNKNOWN
        else:
            self.type = e_type

        self.args = (self.errno, self.message, self.type)

        #If no usefull information was passed in (overriding the default
        # empty dictionary) then set the ticket to being {}.
        if e_ticket == None:
            self.ticket = {}
        else:
            self.ticket = e_ticket

        #Generate the string that stringifying this obeject will give.
        self._string()

        #Is this duplicated from calling Exception.__init__(self)?
        #self.args = [self.errno, self.message, self.type]

        #Do this after calling self._string().  Otherwise, self.strerror
        # will not be defined yet.
        if type(self.ticket) == types.DictType:
            if not self.ticket.has_key('status'):
                self.ticket['status'] = (self.type, self.strerror)
            elif e_errors.is_ok(self.ticket):
                self.ticket['status'] = (self.type, self.strerror)

    def __str__(self):
        self._string()
        return self.strerror

    def __repr__(self):
        return "EncpError"

    def _string(self):
        if self.errno in errno.errorcode.keys():
            errno_name = errno.errorcode[self.errno]
            errno_description = os.strerror(self.errno)
            self.strerror = "%s: [ ERRNO %s ] %s: %s" % (errno_name,
                                                        self.errno,
                                                        errno_description,
                                                        self.message)
        else:
            self.strerror = self.message

        return self.strerror

#Set this for just class Pnfs.  It suppresses warnings that class Pnfs
# uses the functions from the underlying self.p.  The warnings are from
# pychecker not able to find these attributes.
__pychecker__ = "no-objattrs" 
class Pnfs:

    def __init__(self, pnfsFilename="", mount_point="", shortcut=None,
                 use_pnfs_agent = None):
        global pnfs_agent_client_requested
        
        ###Do we still need mount_point and shortcut here?
        __pychecker__ = "unusednames=mount_point,shortcut"

        if use_pnfs_agent or pnfs_agent_client_requested:
            self.use_pnfs_agent = 1
            self.p = get_pac()
        elif pnfsFilename:
            if pnfs.is_pnfs_path(pnfsFilename):
                self.use_pnfs_agent = 0
                self.p = pnfs.Pnfs()
            elif is_pnfs_path(pnfsFilename):
                self.use_pnfs_agent = 1
                self.p = get_pac()
            else:
                self.use_pnfs_agent = 0
                self.p = pnfs.Pnfs()
        else:
            self.use_pnfs_agent = 0
            self.p = pnfs.Pnfs()

            #pnfsFilename="", mount_point="", shortcut=None)

    def __getattr__(self, attr):
        return getattr(self.p, attr)

__pychecker__ = ""  #Reset __pychecker__ after class Pnfs.

############################################################################

def encp_client_version():
    ##this gets changed automatically in {enstore,encp}Cut
    ##You can edit it manually, but do not change the syntax
    version_string = "v3_6d  CVS $Revision$ "
    encp_file = globals().get('__file__', "")
    if encp_file: version_string = version_string + os.path.basename(encp_file)
    #If we end up longer than the current version length supported by the
    # accounting server; truncate the string.
    if len(version_string) > MAX_VERSION_LENGTH:
	version_string = version_string[:MAX_VERSION_LENGTH]
    return version_string

#def quit(exit_code=1):
#    delete_at_exit.quit(exit_code)
#    #delete_at_exit.delete()
#    #os._exit(exit_code)

def print_error(errcode,errmsg):
    format = str(errcode)+" "+str(errmsg) + '\n'
    format = "ERROR: "+format
    sys.stderr.write(format)
    sys.stderr.flush()

def generate_unique_msg_id():
    global _msg_counter
    _msg_counter = _msg_counter + 1
    return _msg_counter

def generate_unique_id():
    global _counter
    thishost = hostaddr.gethostinfo()[0]
    ret = "%s-%d-%d-%d" % (thishost, int(time.time()), os.getpid(), _counter)
    _counter = _counter + 1
    return ret

def generate_location_cookie(number):
    return "0000_000000000_%07d" % int(number)

def convert_0_adler32_to_1_adler32(crc, filesize):
    #Convert to long ingeter types, and determine other values.
    crc = long(crc)
    filesize = long(filesize)
    #Modulo the size with the largest 16 bit prime number.
    size = int(filesize % BASE)
    #Extract existing s1 and s2 from the 0 seeded adler32 crc.
    s1 = (crc & 0xffff)
    s2 = ((crc >> 16) &  0xffff)
    #Modify to reflect the corrected crc.
    s1 = (s1 + 1) % BASE
    s2 = (size + s2) % BASE
    #Return the 1 seeded adler32 crc.
    return (s2 << 16) + s1

#Used to turn off logging when the --check option is enabled.
def check_log_func(dummy_self, time, pid, name, args):
    pass
#Used to turn off alarming when the --check option is enabled.
def check_alarm_func(dummy_self, time, pid, name, root_error, 
                     severity, condition, remedy_type, args):
    pass

__elapsed = os.times()[4]
def elapsed():
    return os.times()[4] - __elapsed

def elapsed_string():
    return "  elapsed=%.3fsec" % (elapsed(),)

############################################################################

def is_bfid(bfid):

    if type(bfid) == types.StringType:

        #Older files that do not have bfid brands should only be digits.
        result = re.search("^[0-9]{13,15}$", bfid)
        if result != None:
            return 1
        
        #The only part of the bfid that is of constant form is that the last
        # n characters are all digits.  There are no restrictions on what
        # is in a brand or how long it can be (though there should be).
        # Since, the bfid is based on its creation time, as time passes the
        # number of digits in a bfid will grow.  (Assume 14 as minumum).
        result = re.search("^[a-zA-Z0-9]*[0-9]{13,15}$", bfid)
        if result != None:
            return 1

        #Allow for bfids of file copies.
        #result = re.search("^[a-zA-Z0-9]*[0-9]{13,15}_copy_[0-9]+$", bfid)
        #if result != None:
        #    return 1

        #Some older files (year 2000) have a long() "L" appended to
        # the bfid.  This seems to be consistant between the file
        # database and layers one & four.  So, return true in these cases.
        result = re.search("^[0-9]{13,15}L{1}$", bfid)
        if result != None:
            return 1

    return 0

#def is_copy_bfid(bfid):
#    if type(bfid) == types.StringType:
#        result = re.search("^[a-zA-Z0-9]*[0-9]{13,15}_copy_[0-9]+$", bfid)
#        if result != None:
#            return 1
#
#    return 0

def is_volume(volume):
    #The format for ANSI labeled volumes should be 6 characters long:
    # characters 1 & 2: uppercase letters
    # characters 3 & 4: uppercase letters or digits
    # characters 5 & 6: digits
    #LTO tapes also require an L1, L2, etc. appended to the label.
    #Note: Not all (test/devel) tapes are stricly conforming to the pattern.
    #
    #The last type of volume tested for are disk volumes.  These are
    # colon seperated values consiting of the library, volume_family
    # and a unique number assigned by the disk mover.
    
    if type(volume) == types.StringType:
        if re.search("^[A-Z0-9]{6}$", volume):
            return 1   #If passed a volume.
        elif re.search("^[A-Z0-9]{6}(L)[0-9]{1}$", volume):
            return 1   #If passed a volume.
        elif re.search("^[%s]+[:]{1}[%s]+[.]{1}[%s]+[.]{1}[%s]+[:]{1}[0-9]+$"
                      % (charset.charset, charset.charset,
                         charset.charset, charset.charset), volume):
            return 1   #If pass a disk volume.
    return 0

def is_location_cookie_tape(lc):
    #For tapes and null volumes.
    tape_regex = re.compile("^[0-9]{4}(_)[0-9]{9}(_)[0-9]{7}$")

    if (len(lc) == 22 and \
        tape_regex.match(lc)):
        return 1

    return 0

#Alias this for completeness.
is_location_cookie_null = is_location_cookie_tape

def is_location_cookie_disk(lc):
    #For disk volumes.
    disk_regex = re.compile("^[/0-9A-Za-z_]*(//)[/0-9A-Za-z_]*(:)[0-9]*$")

    if disk_regex.match(lc):
            return 1
    return 0

def is_location_cookie(lc):
    if type(lc) == types.StringType:
        if is_location_cookie_tape(lc):
            return 1
        elif is_location_cookie_disk(lc):
            return 1
        
    return 0

############################################################################

def extract_brand(bfid):

    if type(bfid) != types.StringType:
        return None

    #Older files that do not have bfid brands should only be digits.
    #
    #Some older files (year 2000) have a long() "L" appended to
    # the bfid.  This seems to be consistant between the file
    # database and layers one & four.  So, return true in these cases.
    result = re.search("^[0-9]{13,15}L{0,1}$", bfid)
    if result != None:
        return ""

    #The only part of the bfid that is of constant form is that the last
    # n characters are all digits.  There are no restrictions on what
    # is in a brand or how long it can be (though there should be).
    # Since, the bfid is based on its creation time, as time passes the
    # number of digits in a bfid will grow.  (Assume 14 as minumum).
    result = re.search("[0-9]{13,15}$", bfid)
    if result != None:
        brand = bfid[:-(len(result.group()))]
        if brand.isalnum():
            return brand

    return None

def extract_file_number(location_cookie):

    if type(location_cookie) != types.StringType:
        return None

    if is_location_cookie(location_cookie):
        try:
            #Return just third integer portion of the string.
            return int(string.split(location_cookie, "_")[2])
        except (KeyboardInterrupt, SystemExit):
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except:
            return None
        
    return None

def combine_dict(*dictionaries):
    new = {}
    for i in range(0, len(dictionaries)):
        if type(dictionaries[i]) != types.DictionaryType:
            raise TypeError, "Dictionary required, not %s." % \
                  type(dictionaries[i])
        for key in dictionaries[i].keys():
            #If both items in the dictionary are themselves dictionaries, then
            # do this recursivly.
            if new.get(key, None) and \
               type(dictionaries[i][key]) == types.DictionaryType and \
               type(new[key]) == types.DictionaryType:
                         new[key] = combine_dict(new[key],
                                                 dictionaries[i][key])
                         
            #Just set the value if not a dictionary.
            elif not new.has_key(key):
                new[key] = dictionaries[i][key]

    return new

def is_access_name(filepath):
    #Determine if it is an ".(access)()" name.
    access_match = re.compile("/\.\(access\)\([0-9A-Fa-f]+\)")
    if re.search(access_match, filepath):
        return True

    return False

def get_directory_name(filepath):
    if type(filepath) != types.StringType:
        return None

    #If we already have a directory...
    #if os.path.isdir(filepath):
    #    return filepath

    #Determine if it is an ".(access)()" name.
    if is_access_name(filepath):
        #Since, we have the .(access)() name we need to split off the id.
        dirname, filename = os.path.split(filepath)
        pnfsid = filename[10:-1]  #len(".(access)(") == 10 and len ")" == 1

        #Create the filename to obtain the parent id.
        parent_id_name = os.path.join(dirname, ".(parent)(%s)" % pnfsid)

        #Read the parent id.
        try:
            f = open(parent_id_name)
            parent_id = f.readlines()[0].strip()
            f.close()
        except OSError, msg:
            if msg.args[0] == errno.ENOENT and \
                   (pnfs_agent_client_requested or pnfs_agent_client_allowed):
                pac = get_pac()
                parent_id = pac.get_parent_id(pnfsid)
                if not parent_id: #Does this work to catch errors?
                    raise OSError, msg
            else:
                raise OSError, msg

        #Build the .(access)() filename of the parent directory.
        directory_name = os.path.join(dirname, ".(access)(%s)" % parent_id)
    else:
        directory_name = os.path.dirname(filepath)
   
    return directory_name

#Make this shortcut so there is less to type.
fullpath = enstore_functions2.fullpath

#Close all desriptors, but handle different types correctly.
def close_descriptors(*fds):

    routes_to_close = {}
    
    #For each file descriptor that is passed in, close it.  The fds can
    # contain integers or class instances with a "close" function attribute.
    for fd in fds:

        try:
            #Obtain a list of unique ips to close the routes to.
            routes_to_close[fd.getpeername()[0]] = 1
        except (OSError, IOError, AttributeError, socket.error), msg:
            pass
            #Trace.log(e_errors.WARNING,
            #          "Unable to cleanup routing table: %s" % (str(msg),))
            
        if hasattr(fd, "close"):
	    try:
		fd.close()
	    except (OSError, IOError), msg:
		sys.stderr.write(
                    "Unable to close file object: %s\n" % str(msg))
                Trace.log(e_errors.ERROR,
                          "Unable to close file object: %s\n" % str(msg))
        else:
            try:
                os.close(fd)
	    except TypeError:
		#The passed in object was not a valid socket descriptor.
		pass
            except (OSError, IOError), msg:
                sys.stderr.write(
                    "Unable to close fd %s: %s\n" % (fd, str(msg)))
                Trace.log(e_errors.ERROR,
                          "Unable to close fd %s: %s\n" % (fd, str(msg)))

    for route in routes_to_close.keys():
        try:
	    #Cleanup the tcp static routes to the mover nodes.
            # (udp is handled in the udp_client module)
            #The addition and deletion of routes can be done without fear of
            # deleting the routes used by other encps... with some care.
            # All tcp traffic goes to dedicated mover nodes, with
            # one ip per media device (tape drive/disk/cdrom), a static route
            # is not shared with any other encp.
            host_config.unset_route(route)
	    pass
        except (AttributeError, KeyError), msg:
            pass
        except (OSError, IOError, socket.error), msg:
            Trace.log(e_errors.WARNING,
                      "Unable to cleanup routing table: %s" % (str(msg),))

def format_class_for_print(object, name):
    #formulate e values output
    formated_string = "%s=" % name
    pad = 0
    for var in dir(object):
        if pad:
            formated_string = formated_string + "\n"
        ret = getattr(object, var)
        if type(ret) == types.StringType: #if a string, make it look like one.
            ret = "'" + ret + "'"
        formated_string = formated_string + " " * pad + var \
                          + ": " + str(ret)
        pad = len(name) + 1 #length of string plus the = character
    return formated_string

"""
def get_file_size(file):

    try:
        statinfo = get_stat(file)  #statinfo = os.stat(file)
        filesize = statinfo[stat.ST_SIZE]
    except (OSError, IOError):
        filesize = None #0

    #If the file has a size of one and is a pnfs file, we need to take
    # extra steps to determine if it is a large file.
    # The function is_pnfs_path() only checks the name of the file for
    # performance reasons.  If it does not exist, then the stat() call
    # above would have already failed and filesize would not equal 1.
    if filesize == 1 and pnfs.is_pnfs_path(file, check_name_only = 1):
        #Get the local pnfs filesize.
        try:
            pin = pnfs.Pnfs(file)
	    pin.pstatinfo()
            pin.get_file_size()
            filesize = pin.file_size
        except (OSError, IOError):
            filesize = None #0
    #It may be a remote pnfs file.
    elif filesize == 1 and is_pnfs_path(file, check_name_only = 1):
        #Get the remote pnfs filesize.
        try:
            pac = get_pac()
            filesize = pac.get_file_size(file)
        except (KeyboardInterrupt, SystemExit):
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except:
            filesize = None

    #Return None for failures.
    if filesize != None:
        #Always return the long version to avoid 32bit vs 64bit problems.    
        filesize = long(filesize)
    
    return filesize
"""

#Return the number of requests in the list that have NOT had a non-retriable
# error or have already finished.
def get_queue_size(request_list):
    queue_size=0
    for req in request_list:
        if not req.get('finished_state', 0):
            queue_size = queue_size + 1

    return queue_size

def update_times(input_path, output_path):
    time_now = time.time()
    try:
        #Update the last access time; set last modified time to existing value.
        os.utime(input_path, (time_now, os.stat(input_path)[stat.ST_MTIME]))
    except OSError:
        pass

    try:
        #Update the last modified time; set last access time to existing value.
        os.utime(output_path, (os.stat(input_path)[stat.ST_ATIME], time_now))
    except OSError:
        pass #This one will fail if the output file is /dev/null.

def bin(integer):
    if type(integer) != types.IntType:
        print

    temp = integer
    bool_list = []
        
    for i in range(32):
        bool_list.append(temp % 2)
        temp = (temp >> 1)

    bool_list.reverse()

    temp = ""
    for i in bool_list:
        temp = temp + ("%s" % i)

    return temp

#Take as parameter the interface class instance or a request ticket.  Determine
# if the transfer(s) is/are a read or not.
# Dmitry (litvintsev@fnal.gov) added "rhsmfile" checks
#
def is_read(ticket_or_interface):
    #If the type is an interface class...
    if type(ticket_or_interface) == types.InstanceType:
        intype = getattr(ticket_or_interface, 'intype', "")
        outtype = getattr(ticket_or_interface, 'outtype', "")
        if intype == "hsmfile" and outtype == "unixfile"  :
            return 1
        elif intype == "rhsmfile" and outtype == "unixfile"  :
            return 1
        elif intype == "unixfile" and outtype == "hsmfile"  :
            return 0
        elif intype == "unixfile" and outtype == "rhsmfile"  :
            return 0
        else:
            infile = getattr(ticket_or_interface, 'infile', "")
            outfile = getattr(ticket_or_interface, 'outfile', "")
            Trace.log(e_errors.ERROR,
                      "Inconsistent file types:" + str({'infile' : infile,
                                                        'outfile' : outfile}))
            raise EncpError(errno.EINVAL, "Inconsistent file types.",
                            e_errors.BROKEN,
                            {'infile' : infile, 'outfile' : outfile})
	
    #If the type is a dictionary...
    elif type(ticket_or_interface) == types.DictionaryType:
        work = ticket_or_interface.get('work', "")
        #First attepmt this by looking at the 'work' sub field.
        if work in ["read_from_hsm",]:
            return 1
        elif work  in ["write_to_hsm",]:
            return 0

        infile = ticket_or_interface.get('infile', "")
        outfile = ticket_or_interface.get('outfile', "")

        #If that failed attempt to look at the file names.
        if not infile or not outfile:
            Trace.log(e_errors.ERROR,
                      "Inconsistent file types:" + str(ticket_or_interface))
            raise EncpError(errno.EINVAL, "Inconsistent file types.",
                            e_errors.BROKEN, ticket_or_interface)
        elif pnfs.is_pnfs_path(infile) and not pnfs.is_pnfs_path(outfile):
            return 1
        elif not pnfs.is_pnfs_path(infile) and pnfs.is_pnfs_path(outfile):
            return 0
        else:
            Trace.log(e_errors.ERROR,
                      "Inconsistent file types:" + str(ticket_or_interface))
            raise EncpError(errno.EINVAL, "Inconsistent file types.",
                            e_errors.BROKEN, ticket_or_interface)
    #Have no idea what was passed in.
    else:
        raise EncpError(errno.EINVAL, "Expected ticket or interface.",
                        e_errors.WRONGPARAMETER,
                        {'is_read() argument' : ticket_or_interface})

#Take as parameter the interface class instance or a request ticket.  Determine
# if the transfer(s) is/are a write or not.
def is_write(ticket_or_interface):
    return not is_read(ticket_or_interface)

def collect_garbage():
    collect_garbage_start_time = time.time()
    
    #Force garbage collection while there is a lull in the action.  This
    # has nothing to do with opening the data tcp socket; just an attempt
    # at optimizing performance.
    gc.collect()
    #This seems more accurate than the return from gc.collect().
    uncollectable_count = len(gc.garbage)
    #NEVER FORGET THIS.  Otherwise gc.garbage still contains references.
    del gc.garbage[:]
    if uncollectable_count > 0:
        Trace.message(1, "UNCOLLECTABLE COUNT: %s" % uncollectable_count)

    Trace.message(TIME_LEVEL, "Time to collect garbage: %s sec." %
                  (time.time() - collect_garbage_start_time,))

############################################################################

def get_enstore_pnfs_path(filepath):
    #Make sure this is a string.
    if type(filepath) != types.StringType:
        raise EncpError(None, "Expected string filename.",
                        e_errors.WRONGPARAMETER)

    #Make absolute path.
    unused, filename, dirname, unused = fullpath(filepath)

    #Determine the canonical path base.  (i.e /pnfs/fnal.gov/usr/)
    canonical_name = string.join(socket.getfqdn().split(".")[1:], ".")
    canonical_pathbase = os.path.join("/pnfs", canonical_name, "usr") + "/"

    #Return an error if the file is not a pnfs filename.
    #if not pnfs.is_pnfs_path(dirname, check_name_only = 1):
    #    raise EncpError(None, "Not a pnfs filename.", e_errors.WRONGPARAMETER)

    if dirname[:13] == "/pnfs/fs/usr/":
        return os.path.join("/pnfs/", filename[13:])
    elif dirname[:len(canonical_pathbase)] == canonical_pathbase:
        return os.path.join("/pnfs/", filename[19:])
    elif dirname[:6] == "/pnfs/":
        return filename
    else:
        raise EncpError(None, "Unable to return enstore pnfs pathname.",
                        e_errors.WRONGPARAMETER)

def get_enstore_fs_path(filepath):
    #Make sure this is a string.
    if type(filepath) != types.StringType:
        raise EncpError(None, "Expected string filename.",
                        e_errors.WRONGPARAMETER)

    #Make absolute path.
    unused, filename, dirname, unused = fullpath(filepath)

    #Determine the canonical path base.
    canonical_name = string.join(socket.getfqdn().split(".")[1:], ".")
    canonical_pathbase = os.path.join("/pnfs", canonical_name, "usr") + "/"
    
    #Return an error if the file is not a pnfs filename.
    #if not pnfs.is_pnfs_path(dirname, check_name_only = 1):
    #    raise EncpError(None, "Not a pnfs filename.", e_errors.WRONGPARAMETER)

    if dirname[:13] == "/pnfs/fs/usr/":
        return filename
    elif dirname[:len(canonical_pathbase)] == canonical_pathbase:
        #i.e. "/pnfs/fnal.gov/usr/"
        return os.path.join("/pnfs/fs/usr/", filename[19:])
    elif dirname[:6] == "/pnfs/":
        return os.path.join("/pnfs/fs/usr/", filename[6:])
    else:
        raise EncpError(None, "Unable to return enstore pnfs admin pathname.",
                        e_errors.WRONGPARAMETER)

def get_enstore_canonical_path(filepath):
    #Make sure this is a string.
    if type(filepath) != types.StringType:
        raise EncpError(None, "Expected string filename.",
                        e_errors.WRONGPARAMETER)

    #Make absolute path.
    unused, filename, dirname, unused = fullpath(filepath)

    #Determine the canonical path base.  If the ENCP_CANONICAL_DOMAINNAME
    # overriding environmental variable is set, use that.
    if os.environ.get('ENCP_CANONICAL_DOMAINNAME', None):
        canonical_name = os.environ['ENCP_CANONICAL_DOMAINNAME']
    else:
        canonical_name = string.join(socket.getfqdn().split(".")[1:], ".")
    #Use the canonical_name to determine the canonical pathname base.
    canonical_pathbase = os.path.join("/pnfs", canonical_name, "usr") + "/"

    #Return an error if the file is not a pnfs filename.
    #if not pnfs.is_pnfs_path(dirname, check_name_only = 1):
    #    raise EncpEr0ror(None, "Not a pnfs filename.", e_errors.WRONGPARAMETER)

    if dirname[:len(canonical_pathbase)] == canonical_pathbase:
        #i.e. "/pnfs/fnal.gov/usr/"
        return filename
    elif dirname[:13] == "/pnfs/fs/usr/":
        return os.path.join(canonical_pathbase, filename[13:])
    elif dirname[:6] == "/pnfs/":
        return os.path.join(canonical_pathbase, filename[6:])
    else:
        raise EncpError(None,
                        "Unable to return enstore pnfs canonical pathname.",
                        e_errors.WRONGPARAMETER)
    
############################################################################

#The os.access() and the access(2) C library routine use the real id when
# testing for access.  This function does the same thing but for the
# effective ID.
def e_access(path, mode):

    #Test for existance.
    try:
        file_stats = os.stat(path)
        stat_mode = file_stats[stat.ST_MODE]
    except OSError:
        return 0

    #Make sure a valid mode was passed in.
    if mode & (os.F_OK | os.R_OK | os.W_OK | os.X_OK) != mode:
        return 0

    # Need to check for each type of access permission.

    if mode == os.F_OK:
        # In order to get this far, the file must exist.
        return 1

    if mode & os.R_OK:  #Check for read permissions.
        #If the user is user root.
        if os.geteuid() == 0:
            #return 1
            pass
        #Anyone can read this file.
        elif (stat_mode & stat.S_IROTH):
            #return 1
            pass
        #This is the files owner.
        elif (stat_mode & stat.S_IRUSR) and \
           file_stats[stat.ST_UID] == os.geteuid():
            #return 1
            pass
        #The user has group access.
        elif (stat_mode & stat.S_IRGRP) and \
           (file_stats[stat.ST_GID] == os.geteuid() or
            file_stats[stat.ST_GID] in os.getgroups()):
            #return 1
            pass
        else:
            return 0

    if mode & os.W_OK:  #Check for write permissions.
        #If the user is user root.
        if os.geteuid() == 0:
            #return 1
            pass
        #Anyone can write this file.
        elif (stat_mode & stat.S_IWOTH):
            #return 1
            pass
        #This is the files owner.
        elif (stat_mode & stat.S_IWUSR) and \
           file_stats[stat.ST_UID] == os.geteuid():
            #return 1
            pass
        #The user has group access.
        elif (stat_mode & stat.S_IWGRP) and \
           (file_stats[stat.ST_GID] == os.geteuid() or
            file_stats[stat.ST_GID] in os.getgroups()):
            #return 1
            pass
        else:
            return 0
    
    if mode & os.X_OK:  #Check for execute permissions.
        #If the user is user root.
        if os.geteuid() == 0:
            #return 1
            pass
        #Anyone can execute this file.
        elif (stat_mode & stat.S_IXOTH):
            #return 1
            pass
        #This is the files owner.
        elif (stat_mode & stat.S_IXUSR) and \
           file_stats[stat.ST_UID] == os.geteuid():
            #return 1
            pass
        #The user has group access.
        elif (stat_mode & stat.S_IXGRP) and \
           (file_stats[stat.ST_GID] == os.geteuid() or
            file_stats[stat.ST_GID] in os.getgroups()):
            #return 1
            pass
        else:
            return 0

    return 1

############################################################################

#Return True if the file passed in is a pnfs file with either layer 1 or
# or layer 4 set.  False otherwise.  OSError, and IOError exceptions may
# be thrown for other errors.
def do_layers_exist(pnfs_filename):

    if not pnfs_filename:
        return False

    p = Pnfs(pnfs_filename)

    if p.readlayer(1, pnfs_filename) or p.readlayer(4, pnfs_filename):
        #Layers found for the file!
        return True

    #The pnfs files does not exist, or it does exist and does not have
    # any layer information.
    return False

#As the name implies remove layers 1 and 4 for the indicated file.
def clear_layers_1_and_4(work_ticket):

    pnfs_filename = work_ticket.get('wrapper', {}).get('pnfsFilename', "")
    
    if not pnfs_filename:
        return False

    if not is_pnfs_path(pnfs_filename):
        return False

    try:
        p = Pnfs(pnfs_filename)

        Trace.log(e_errors.INFO,
                  "Clearing layers 1 and 4 for file %s (%s)." %
                  (pnfs_filename, work_ticket.get('unique_id', None)))
        
        p.writelayer(1, " ", pnfs_filename)
        p.writelayer(4, " ", pnfs_filename)
    except (IOError, OSError):
        return False

    return True

############################################################################

def get_original_request(request_list, index_of_copy):
    oui = request_list[index_of_copy].get('original_unique_id', None)
    if oui:  #oui == Original Unique Id
        for j in range(len(request_list)):
            if request_list[j].get('completion_status', None) == SUCCESS \
               and oui == request_list[j].get('unique_id', None):
                return request_list[j]

    return None

def did_original_succeed(request_list, index_of_copy):
    oui = request_list[index_of_copy].get('original_unique_id', None)
    if oui:  #oui == Original Unique Id
        for j in range(len(request_list)):
            if request_list[j].get('completion_status', None) == SUCCESS \
               and oui == request_list[j].get('unique_id', None):
                return True #The original succeeded.
        else:
            return False #Original not done yet or failed.
            
    return True #Is an original.

#Return the number of files in the list left to transfer.
def requests_outstanding(request_list):

    files_left = 0

    for i in range(len(request_list)):
        completion_status = request_list[i].get('completion_status', None)
        if completion_status == None:
            if did_original_succeed(request_list, i):
                #Don't worry about copies when the original failed.
                files_left = files_left + 1

    return files_left

#Return the next uncompleted transfer.
def get_next_request(request_list):

    for i in range(len(request_list)):
        completion_status = request_list[i].get('completion_status', None)
        if completion_status == None:
            #Don't worry about copies when the original failed.
            orig_request = get_original_request(request_list, i)
            if orig_request:
                if orig_request.get('completion_status', None) == SUCCESS:
                    #Store the original bfid into the copy ticket so the
                    # mover can mangle it.
                    request_list[i]['fc']['original_bfid'] = \
                                    orig_request['fc']['bfid']
                elif orig_request.get('completion_status', None) == FAILURE:
                    # We should skip copy transfers when the original failed.
                    continue

            #If we have an original transfer, we jump right here and skip the
            # special processing for copy transfers.
            return request_list[i], i, request_list[i].get('copy', 0)

    return None, 0, 0

#Return the index that the specified request refers to.
def get_request_index(request_list, request):

    unique_id = request.get('unique_id', None)
    if not unique_id:
        return None, None

    for i in range(len(request_list)):
        if unique_id == request_list[i]['unique_id']:
            return i, request_list[i].get('copy', 0)  #file number, copy number

    return None, None
    
############################################################################

def _get_csc_from_volume(volume): #Should only be called from get_csc().
    global __csc
    global __vcc

    #There is no volume.
    if not volume:
        #If not already cached, get the default.
        if __csc == None:
            # Get the configuration server.
            config_host = enstore_functions2.default_host()
            config_port = enstore_functions2.default_port()
            csc = configuration_client.ConfigurationClient((config_host,
                                                            config_port))
            __csc = csc

        #Regardless, return the csc.
        return __csc

    #First check that the cached version knows about the volume.
    if __vcc != None:
        volume_info = __vcc.inquire_vol(volume, 5, 20)
        if e_errors.is_ok(volume_info):
            return __csc
        elif volume_info['status'][0] == e_errors.NO_VOLUME:
            Trace.log(e_errors.WARNING,
                      "Volume clerk (%s) knows nothing about %s.\n"
                      % (__vcc.server_address, volume))
        else:
            Trace.log(e_errors.WARNING,
                      "Failure communicating with volume clerk (%s) about"
                      " volume %s: %s"
                      % (__vcc.server_address, volume,
                         str(volume_info['status'])))

    #Check the default vcc for performance reasons.
    if __csc != None:
        test_vcc = volume_clerk_client.VolumeClerkClient(
            __csc, logc = __logc, alarmc = __alarmc,
            rcv_timeout = 5, rcv_tries = 20)
        if test_vcc.server_address == None:
            Trace.log(e_errors.WARNING,
                      "Locating cached volume clerk failed.\n")
        else:
            volume_info = test_vcc.inquire_vol(volume, 5, 20)
            if e_errors.is_ok(volume_info):
                __vcc = test_vcc
                return __csc
            elif volume_info['status'][0] == e_errors.NO_VOLUME:
                Trace.log(e_errors.WARNING,
                          "Volume clerk (%s) knows nothing about %s.\n"
                          % (test_vcc.server_address, volume))
            else:
                Trace.log(e_errors.WARNING,
                          "Failure communicating with volume clerk (%s) about"
                          " volume %s: %s"
                          % (test_vcc.server_address, volume,
                             str(volume_info['status'])))

            
    # get a configuration server
    config_host = enstore_functions2.default_host()
    config_port = enstore_functions2.default_port()
    csc = configuration_client.ConfigurationClient((config_host,config_port))
    vcc = volume_clerk_client.VolumeClerkClient(csc, logc = __logc,
                                                alarmc = __alarmc)
    if vcc.server_address == None:
        Trace.log(e_errors.WARNING, "Locating default volume clerk failed.\n")
    #Before checking other systems, check the current system.
    elif e_errors.is_ok(vcc.inquire_vol(volume)):
        return csc

    #Get the list of all config servers and remove the 'status' element.
    config_servers = csc.get('known_config_servers', {})
    if e_errors.is_ok(config_servers['status']):
        del config_servers['status']
    else:
        return csc

    #Loop through systems for the tape that matches the one we're looking for.
    for server in config_servers.keys():
        try:
            #Get the next configuration client.
            csc_test = configuration_client.ConfigurationClient(
                config_servers[server])

            #Get the next volume clerk client and volume inquiry.
            vcc_test = volume_clerk_client.VolumeClerkClient(
                csc_test, logc = __logc, alarmc = __alarmc,
                rcv_timeout=5, rcv_tries=2)

            if vcc_test.server_address != None:
		#If the fcc has been initialized correctly; use it.

		if e_errors.is_ok(vcc_test.inquire_vol(volume, 5, 2)):
		    msg = "Using %s based on volume %s." % \
			  (vcc_test.server_address, volume)
		    Trace.log(e_errors.INFO, msg)

		    __csc = csc_test  #Set global for performance reasons.
		    return __csc

        except (KeyboardInterrupt, SystemExit):
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except:
            exc, msg = sys.exc_info()[:2]
            Trace.log(e_errors.WARNING, str((str(exc), str(msg))))

    return csc

def _get_csc_from_bfid(bfid): #Should only be called from get_csc().
    global __csc
    global __fcc

    #There is no brand, since the file is too old.
    if not bfid:
        #If not already cached, get the default. 
        if __csc == None:
            # Get the configuration server.
            config_host = enstore_functions2.default_host()
            config_port = enstore_functions2.default_port()
            csc = configuration_client.ConfigurationClient((config_host,
                                                            config_port))
            __csc = csc

        #Regardless, return the csc.
        return __csc

    #Check the default fcc for performance reasons.
    if __csc != None:
        test_fcc = file_clerk_client.FileClient(
            __csc, logc = __logc, alarmc = __alarmc,
            rcv_timeout = 5, rcv_tries = 20)
        if test_fcc.server_address == None:
            Trace.log(e_errors.WARNING, "Locating cached file clerk failed.\n")
        else:
            file_info = test_fcc.bfid_info(bfid, 5, 3)
            if e_errors.is_ok(file_info):
                __fcc = test_fcc
                return __csc

    #Before checking other systems, check the current system.
    # Get a configuration server.
    config_host = enstore_functions2.default_host()
    config_port = enstore_functions2.default_port()
    csc = configuration_client.ConfigurationClient((config_host, config_port))
    fcc = file_clerk_client.FileClient(
        csc, logc = __logc, alarmc = __alarmc, rcv_timeout = 5, rcv_tries = 20)
    if fcc.server_address == None:
        Trace.log(e_errors.WARNING, "Locating default file clerk failed.\n")
    else:
        file_info = fcc.bfid_info(bfid, 5, 3)
        if e_errors.is_ok(file_info):
            __fcc = fcc
            return __csc
        
    #Get the list of all config servers and remove the 'status' element.
    config_servers = csc.get('known_config_servers', {})
    if e_errors.is_ok(config_servers['status']):
        del config_servers['status']
    else:
        __csc = csc
        return __csc

    #Loop through systems for the brand that matches the one we're looking for.
    for server in config_servers.keys():
        try:
            #Get the next configuration client.
            csc_test = configuration_client.ConfigurationClient(
                config_servers[server])

            #Get the next file clerk client and its brand.
            fcc_test = file_clerk_client.FileClient(
                csc_test, logc = __logc, alarmc = __alarmc,
                rcv_timeout=5, rcv_tries=2)
            if fcc_test.server_address != None:
		#If the fcc has been initialized correctly; use it.
                file_info = fcc.bfid_info(bfid, 5, 3)
                if e_errors.is_ok(file_info):
                    __csc = csc_test  #Set global for performance reasons.
                    __fcc = fcc_test
                    return __csc

        except (KeyboardInterrupt, SystemExit):
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except:
            exc, msg = sys.exc_info()[:2]
            Trace.log(e_errors.WARNING, str((str(exc), str(msg))))

    __csc = csc
    return __csc

#Return the correct configuration server client based on the 'brand' (if
# present) of the bfid or the volume name.
#
#If called during a write, we go with the default csc.  On reads, it
# is a little more complicated.  The exes are situations where the listed
# function calls call get_csc().  All the reads with exes should be called
# with a bfid passed in (seperated to lower group).  These function also tend
# to be called during writes, but there should not be a bfid in the ticket,
# yet anyway (these writes shown with asterisks).  Normal writes with exes
# should not be a problem since wites assume default configuration host.
# Dashes indicate that the function should not be called for those situations,
# event though the calling function does use it (for the other read/write).
# Pluses indicate the function is called before any great initialization
# occurs.
#
# get_csc()                     read              write
# -----------------------------------------------------
# set_pnfs_settings()                               x
# max_attempts()                  x                 x
# clients()                       +                 +
# wrappersize_check()             -                 x
# librarysize_check()             -                 x
#
# get_clerks()  (bfid)            x                 *
# submit_one_request()  (dict)    x                 *
# handle_retries()  (dict)        x                 *
#
#
# parameter: can be a dictionary containg a 'bfid' item or a bfid string,
#  or a volume name string.
def __get_csc(parameter=None):
    global __csc  #For remembering.
    global __acc

    #Set some default values.
    bfid = None
    volume = None
    address = None
    
    #Due to branding we need to figure out which system is the correct one.
    # Should only matter for reads.  On a success, the variable 'brand' should
    # contain the brand of the bfid to be used in the rest of the function.
    # On error return the default configuration server client (csc).
    if type(parameter) == types.DictType: #If passed a ticket with bfid.
        bfid = parameter.get('fc', {}).get("bfid",
                                           parameter.get("bfid", None))
        volume = parameter.get('fc', {}).get('external_label',
                                             parameter.get('volume', None))

    elif is_bfid(parameter):  #If passed a bfid.
        bfid = parameter

    elif is_volume(parameter):  #If passed a volume.
        volume = parameter

    elif type(parameter) == types.TupleType and len(parameter) == 2 \
             and type(parameter[0]) == types.StringType \
             and type(parameter[1]) == types.IntType:
        address = parameter

    #Remember this for comparisons later.
    #old_csc = __csc

    #Call the correct version of the underlying get_csc functions.    
    if bfid:  #If passed a bfid.
        #brand = extract_brand(bfid)
        csc = _get_csc_from_bfid(bfid)
    
    elif volume:  #If passed a volume.
        csc = _get_csc_from_volume(volume)

    elif address: #If passed the address of the correct config server.
        csc = configuration_client.ConfigurationClient(address)

    else:
        if __csc != None:
            #Use the cached instance, if possible.
            csc = __csc
        else:
            config_host = enstore_functions2.default_host()
            config_port = enstore_functions2.default_port()
            csc = configuration_client.ConfigurationClient((config_host,
                                                            config_port))

    #These are some things that only should be done if this is a new
    # conifguration client.
    if __csc != csc:
        #Snag the entire configuration.
        config = csc.dump_and_save(10, 10)

        if e_errors.is_timedout(config):
            raise EncpError(errno.ETIMEDOUT,
                            "Unable to obtain configuration information" \
                            " from configuration server.",
                            e_errors.CONFIGDEAD)
        elif not e_errors.is_ok(config):
            raise EncpError(None, str(config['status'][1]),
                            config['status'][0])
            
        #Don't use the csc get() function to retrieve the
        # event_relay information; doing so will clobber
        # the dump-and-saved configuration.  Once, the
        # enable_caching() function is called the csc get()
        # function is okay to use.
        #er_info = config.get(enstore_constants.EVENT_RELAY)
        #er_addr = (er_info['hostip'], er_info['port'])
        csc.new_config_obj.enable_caching()   #er_addr)

        __csc = csc
        if __acc != None:
            get_acc()
            #__acc = accounting_client.accClient(__csc, logname = 'ENCP',
            #                                    logc = __logc,
            #                                    alarmc = __alarmc)
    else:
        __csc.dump_and_save(10, 10)

    if __csc.have_complete_config:
        #Nothing valid, just return the default csc.
        return __csc, __csc.saved_dict
    else:
        return __csc, None

def get_csc(parameter=None):

    return __get_csc(parameter)[0]

# parameter: can be a dictionary containg a 'bfid' item or a bfid string
def __get_fcc(parameter = None):
    global __fcc
    global __csc
    global __acc

    if not parameter:
        bfid = None

    elif type(parameter) == types.DictType: #If passed a ticket with bfid.
        bfid = parameter.get('fc', {}).get("bfid",
                                           parameter.get("bfid", None))

    elif is_bfid(parameter):  #If passed a bfid.
        bfid = parameter
        
    else:
        raise EncpError(None,
                        "Invalid bfid (%s) specified." % parameter,
                        e_errors.WRONGPARAMETER, {})

        #Set default value.
        #bfid = None

    if bfid == None:
        if __fcc != None: #No bfid, but have cached fcc.
            return __fcc, None
        else:
            #Get the csc to use.
            if __csc == None:
                config_host = enstore_functions2.default_host()
                config_port = enstore_functions2.default_port()
                csc = configuration_client.ConfigurationClient((config_host,
                                                                config_port))
            else:
                csc = __csc

            #Now that we have the csc, we can get the fcc.
            __fcc = file_clerk_client.FileClient(
                csc, logc = __logc, alarmc = __alarmc,
                rcv_timeout=5, rcv_tries=12)
            return __fcc, None
    
    #First check that the cached version matches the bfid brand.
    if __fcc != None:
        file_info = __fcc.bfid_info(bfid, 5, 12)
        if e_errors.is_ok(file_info):
            return __fcc, file_info

    #Next check the fcc associated with the cached csc.
    if __csc != None:
        fcc = file_clerk_client.FileClient(
            __csc, logc = __logc, alarmc = __alarmc,
            rcv_timeout=5, rcv_tries=12)
        if fcc.server_address == None:
            Trace.log(e_errors.WARNING, "Locating cached file clerk failed.\n")
        else:
            file_info = fcc.bfid_info(bfid, 5, 12)
            if e_errors.is_ok(file_info):
                __fcc = fcc
                return __fcc, file_info

    #Before checking other systems, check the default system.
    config_host = enstore_functions2.default_host()
    config_port = enstore_functions2.default_port()
    csc = configuration_client.ConfigurationClient((config_host, config_port))
    fcc = file_clerk_client.FileClient(
        csc, logc = __logc, alarmc = __alarmc, rcv_timeout=5, rcv_tries=12)
    if fcc.server_address == None:
        Trace.log(e_errors.WARNING, "Locating default file clerk failed.\n")
    else:
        file_info = fcc.bfid_info(bfid, 5, 12)
        if e_errors.is_ok(file_info):
            __csc = csc
            __fcc = fcc
            if __acc != None:
                __acc = accounting_client.accClient(__csc, logname = 'ENCP',
                                                    logc = __logc,
                                                    alarmc = __alarmc)
            return __fcc, file_info

    #Get the list of all config servers and remove the 'status' element.
    config_servers = csc.get('known_config_servers', {})
    if e_errors.is_ok(config_servers['status']):
        del config_servers['status']
    else:
        __fcc = file_clerk_client.FileClient(
            csc, logc = __logc, alarmc = __alarmc, rcv_timeout=5, rcv_tries=12)
        if bfid:
            file_info = __fcc.bfid_info(bfid, 5, 12)
            if e_errors.is_ok(file_info):
                return __fcc, file_info
        else:
            return __fcc, None

    #Loop through systems for the brand that matches the one we're looking for.
    for server in config_servers.keys():
        try:
            #Get the next configuration client.
            csc_test = configuration_client.ConfigurationClient(
                config_servers[server])

            #Get the next file clerk client and its brand.
            fcc_test = file_clerk_client.FileClient(
                csc_test, logc = __logc, alarmc = __alarmc,
                rcv_timeout=5, rcv_tries=3)

            if fcc_test.server_address != None:
		#If the fcc has been initialized correctly; use it.
                file_info = fcc_test.bfid_info(bfid, 5, 3)
                if e_errors.is_ok(file_info):
                    __csc = csc_test
                    __fcc = fcc
                    if __acc != None:
                        __acc = accounting_client.accClient(
                            __csc, logname = 'ENCP',
                            logc = __logc, alarmc = __alarmc)
                    return __fcc, file_info
        
        except (KeyboardInterrupt, SystemExit):
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except:
            exc, msg = sys.exc_info()[:2]
            Trace.log(e_errors.WARNING, str((str(exc), str(msg))))

    __fcc = fcc
    if __fcc.server_address != None and bfid != None:
        #If the fcc has been initialized correctly; use it.
        file_info = __fcc.bfid_info(bfid, 5, 3)
    else:
        file_info = None

    #In theory the only spot that an error response can be returned
    # from this function is at the very end.
    if file_info and not e_errors.is_ok(file_info):
        #Stuff this back in.  Some message building code looks for this.
        # On errors the file clerk does not put it back.
        file_info['bfid'] = bfid
        
        message = "Failure communicating with file clerk (%s) about" \
                  " bfid %s: %s" \
                  % (__fcc.server_address, bfid,
                     str(file_info['status'][0]))

        Trace.log(e_errors.WARNING, message)
        
    return __fcc, file_info

def get_fcc(bfid = None):

    return __get_fcc(bfid)[0]
    
def __get_vcc(parameter = None):
    global __vcc
    global __csc
    global __acc

    if not parameter:
        volume = None

    elif type(parameter) == types.DictType: #If passed a ticket with volume.
        volume = parameter.get('fc', {}).get('external_label',
                                             parameter.get('volume', None))

    elif is_volume(parameter):  #If passed a volume.
        volume = parameter

    else:
        raise EncpError(None,
                        "Invalid volume (%s) specified." % parameter,
                        e_errors.WRONGPARAMETER, {})
     
        #Set default value.
        #volume = None

    if volume == None: 
        if __vcc != None: #No volume, but have cached vcc.
            return __vcc, None
        else:
            #Get the csc to use.
            if __csc == None:
                config_host = enstore_functions2.default_host()
                config_port = enstore_functions2.default_port()
                csc = configuration_client.ConfigurationClient((config_host,
                                                                config_port))
            else:
                csc = __csc

            #Now that we have the csc, we can get the vcc.
            __vcc = volume_clerk_client.VolumeClerkClient(
                csc, logc = __logc, alarmc = __alarmc,
                rcv_timeout=5, rcv_tries=12)
            return __vcc, None

    #First check that the cached version knows about the volume.
    if __vcc != None:
        volume_info = __vcc.inquire_vol(volume, 5, 12)
        if e_errors.is_ok(volume_info):
            return __vcc, volume_info

    #Next check the vcc associated with the cached csc.
    if __csc != None:
        test_vcc = volume_clerk_client.VolumeClerkClient(
            __csc, logc = __logc, alarmc = __alarmc,
            rcv_timeout = 5, rcv_tries = 12)
        if test_vcc.server_address == None:
            Trace.log(e_errors.WARNING,
                      "Locating cached volume clerk failed.\n")
        else:
            volume_info = test_vcc.inquire_vol(volume, 5, 12)
            if e_errors.is_ok(volume_info):
                __vcc = test_vcc
                return __vcc, volume_info

    # get a configuration server
    config_host = enstore_functions2.default_host()
    config_port = enstore_functions2.default_port()
    csc = configuration_client.ConfigurationClient((config_host,config_port))
    vcc = volume_clerk_client.VolumeClerkClient(csc, logc = __logc,
                                                alarmc = __alarmc)
    if vcc.server_address == None:
        Trace.log(e_errors.WARNING, "Locating default volume clerk failed.\n")
    #Before checking other systems, check the current system.
    else:
        volume_info = vcc.inquire_vol(volume, 5, 12)
        if e_errors.is_ok(volume_info):
            __csc = csc
            __vcc = vcc
            if __acc != None:
                __acc = accounting_client.accClient(__csc, logname = 'ENCP',
                                                    logc = __logc,
                                                    alarmc = __alarmc)
            return __vcc, volume_info

    #Get the list of all config servers and remove the 'status' element.
    config_servers = csc.get('known_config_servers', {})
    if e_errors.is_ok(config_servers['status']):
        del config_servers['status']
    else:
        __vcc = vcc
        if volume:
            volume_info = __vcc.inquire_vol(volume, 5, 12)
            return __vcc, volume_info
        else:
            return __vcc, None

    #Loop through systems for the tape that matches the one we're looking for.
    vcc_test = None
    for server in config_servers.keys():
        try:
            #Get the next configuration client.
            csc_test = configuration_client.ConfigurationClient(
                config_servers[server])

            #Get the next volume clerk client and volume inquiry.
            vcc_test = volume_clerk_client.VolumeClerkClient(
                csc_test, logc = __logc, alarmc = __alarmc,
                rcv_timeout=5, rcv_tries=3)
            if vcc_test.server_address == None:
                #If we failed to find this volume clerk, move on to the
                # next one.
                continue
		#pass

            volume_info = vcc_test.inquire_vol(volume, 5, 3)
            if e_errors.is_ok(volume_info):
                msg = "Using %s based on volume %s." % \
                      (vcc_test.server_address, volume)
                Trace.log(e_errors.INFO, msg)

                __csc = csc_test  #Set global for performance reasons.
                __vcc = vcc_test
                if __acc != None:
                    __acc = accounting_client.accClient(__csc,
                                                        logname = 'ENCP',
                                                        logc = __logc,
                                                        alarmc = __alarmc)
                return __vcc, volume_info
        except (KeyboardInterrupt, SystemExit):
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except:
            exc, msg = sys.exc_info()[:2]
            Trace.log(e_errors.WARNING, str((str(exc), str(msg))))

    __vcc = vcc
    #if vcc_test and vcc_test.server_address != None and volume != None:
    if __vcc and __vcc.server_address != None and volume != None:
        #If the vcc has been initialized correctly; use it.
        volume_info = __vcc.inquire_vol(volume, 5, 20)
    else:
        volume_info = None

    #In theory the only spot that an error response can be returned
    # from this function is at the very end.
    if volume_info and not e_errors.is_ok(volume_info):
        #Stuff this back in.  Some message building code looks for this.
        # On errors the volume clerk does not put it back.
        volume_info['external_label'] = volume
        
        message = "Failure communicating with volume clerk (%s) about" \
                  " volume %s: %s" \
                  % (__vcc.server_address, volume,
                     str(volume_info['status']))
            
        Trace.log(e_errors.WARNING, message)
                  
    return __vcc, volume_info
 
def get_vcc(volume = None):

    return __get_vcc(volume)[0]

def get_acc():
    global __acc
    global __csc

    #If we don't have the log client or alarm client by now, there is
    # likely something quite wrong.  So, don't try.  If __logc and __alarmc
    # are set when passed in these flags have no effect.
    flags = enstore_constants.NO_LOG | enstore_constants.NO_ALARM

    if __acc:
        return __acc
    elif __csc:
        __acc = accounting_client.accClient(__csc, logname = 'ENCP',
                                            flags = flags,
                                            logc = __logc, alarmc = __alarmc)
        return __acc
    else:
        try:
            csc, config = __get_csc()
        except EncpError:
            csc, config = None, None

        acc_addr = None  #Default.
        if config:
            #Find the accounting server information.
            acc_info = config.get(enstore_constants.ACCOUNTING_SERVER, {})
            if acc_info:
                acc_addr = (acc_info.get('hostip', None),
                            acc_info.get('port', None))

        __acc = accounting_client.accClient(csc, logname = 'ENCP',
                                            flags = flags,
                                            logc = __logc, alarmc = __alarmc,
                                            server_address = acc_addr)
        return __acc

def get_pac():
    global __pac
    global __csc

    #If we don't have the log client or alarm client by now, there is
    # likely something quite wrong.  So, don't try.  If __logc and __alarmc
    # are set when passed in these flags have no effect.
    flags = enstore_constants.NO_LOG | enstore_constants.NO_ALARM

    if __pac:
        return __pac
    elif __csc:
        __pac = pnfs_agent_client.PnfsAgentClient(__csc, flags = flags,
                                                  logc = __logc,
                                                  alarmc = __alarmc)
        return __pac
    else:
        try:
            csc, config = __get_csc()
        except EncpError:
            csc, config = None, None

        pac_addr = None  #Default.
        if config:
            #Find the pnfs agent information.
            pac_info = config.get(enstore_constants.PNFS_AGENT, {})
            if pac_info:
                pac_addr = (pac_info.get('hostip', None),
                            pac_info.get('port', None))

        __pac = pnfs_agent_client.PnfsAgentClient(csc, flags = flags,
                                                  logc = __logc,
                                                  alarmc = __alarmc,
                                                  server_address = pac_addr)
        return __pac

############################################################################
            
def max_attempts(csc, library, encp_intf):

    resend = {}
    resend['retry'] = 0
    resend['resubmit'] = 0

    #Determine how many times a transfer can be retried from failures.
    #Also, determine how many times encp resends the request to the lm
    # and the mover fails to call back.

    #if encp_intf.use_max_retry and encp_intf.use_max_resubmit:
    #    #The user overrode both values, no need to continue.
    #    return

    #If the shortname was supplied, make it the longname.
    if library[-16:] == ".library_manager":
        lib = library
    else:
        lib = library + ".library_manager"

    # get a configuration server
    #csc = get_csc()
    lm = csc.get(lib, 5, 5)

    #Due to the possibility of branding, check other systems.
    if lm['status'][0] == e_errors.KEYERROR:
        #If we get here, then check the other enstore config servers.
        # be prepared to find the correct system if necessary.
        kcs = csc.get("known_config_servers", 5, 5)
        #new_key = csc.get_enstore_system()
        kcs['default'] = (enstore_functions2.default_host(),
                          enstore_functions2.default_port())
        if e_errors.is_ok(kcs):
            del kcs['status']
        else:
            return resend # Give up on error.  Return what we know.

        for item in kcs.values():
            _csc = configuration_client.ConfigurationClient(address = item)
            lm = _csc.get(lib, 5, 5)
            if e_errors.is_ok(lm):
                break
        else:
            #If we didn't find a match just return what we know.
            return resend

    #If the library does not have the following entries (or the library name
    # was not found in config file(s)... very unlikely) then go with
    # the defaults.

    if encp_intf.use_max_retry:
        resend['max_retry'] = encp_intf.max_retry
    else:
        resend['max_retry'] = lm.get('max_encp_retries',
                                     enstore_constants.DEFAULT_ENCP_RETRIES)
        
    if encp_intf.use_max_resubmit:
        resend['max_resubmits'] = encp_intf.max_resubmit
    else:
        resend['max_resubmits'] = lm.get('max_encp_resubmits',
                                 enstore_constants.DEFAULT_ENCP_RESUBMISSIONS)

    return resend

def check_library(library, e):
    #Check if the library is accepting requests.

    #If the shortname was supplied, make it the longname.
    if library[-16:] == ".library_manager":
        lib = library
    else:
        lib = library + ".library_manager"

    # get a configuration server
    csc = get_csc()

    try:
        lmc = library_manager_client.LibraryManagerClient(
            csc, lib, logc = __logc, alarmc = __alarmc,
            rcv_timeout = 5, rcv_tries = 20)

        if lmc.server_address == None:
            status = (e_errors.KEYERROR, "No LM %s found." % lib)
            if e.check:
                status_ticket = {'status' : status, 'exit_status' : 2}
            else:
                status_ticket = {'status' : status}

            return status_ticket
        
    except SystemExit:
        #On error the library manager client calls sys.exit().  This
        # should catch that so we can handle it.
        status = (e_errors.TIMEDOUT,
                  "No response from configuration server for location"
                  " of %s." % lib)
        if e.check:
            status_ticket = {'status' : status, 'exit_status' : 2}
        else:
            status_ticket = {'status' : status}

        Trace.message(1, "LM status: %s" % status_ticket)

        return status_ticket

    
    status_ticket = lmc.get_lm_state(timeout=5, tries=5)

    if e_errors.is_ok(status_ticket):
        state = status_ticket.get("state", e_errors.UNKNOWN)

        if state == "locked":
            status_ticket['status'] = (e_errors.LOCKED,
                                       "%s is locked." % lib)
        #if state == "ignore":
        #    status_ticket['status'] = (e_errors.IGNORE,
        #                               "%s is ignoring requests." % lib)
        #if state == "pause":
        #    status_ticket['status'] = (e_errors.PAUSE,
        #                               "%s is paused." % lib)
        if state == "noread" and is_read(e):
            status_ticket['status'] = (e_errors.NOREAD,
                                    "%s is ignoring read requests." % lib)
        if state == "nowrite" and is_write(e):
            status_ticket['status'] = (e_errors.NOREAD,
                                    "%s is ignoring write requests." % lib)

        if state == e_errors.UNKNOWN:
            status_ticket['status'] = (e_errors.UNKNOWN,
                                       "Unable to determine %s state." % lib)

    if e.check and not e_errors.is_ok(status_ticket):
        #If one of these temporary library states is true and the
        # user is only checking, don't give a normal error.
        status_ticket['exit_status'] = 2

    Trace.message(1, "LM status: %s" % status_ticket)

    return status_ticket

############################################################################

def print_data_access_layer_format(inputfile, outputfile, filesize, ticket):

    if data_access_layer_requested < 0:
        return
    
    # check if all fields in ticket present

    #Check the file and volume clerk sub-tickets.
    fc_ticket = ticket.get('fc', {})
    vc_ticket = ticket.get('vc', {})
    if type(fc_ticket) != types.DictType:
        Trace.log(e_errors.WARNING,
                  "Did not excpect 'fc_ticket' value -- %s -- as type %s."
                  % (str(fc_ticket), str(type(fc_ticket))))
        fc_ticket = {}
    if type(vc_ticket) != types.DictType:
        
        Trace.log(e_errors.WARNING,
                  "Did not excpect 'vc_ticket' value -- %s -- as type %s."
                  % (str(vc_ticket), str(type(vc_ticket))))
        vc_ticket = {}
    external_label = fc_ticket.get('external_label',
                                   vc_ticket.get('external_label',
                                                 ticket.get('volume', "")))
    location_cookie = fc_ticket.get('location_cookie', "")
    storage_group = vc_ticket.get('storage_group', "")
    if not storage_group:
        storage_group = volume_family.extract_storage_group(
            vc_ticket.get('vc', {}).get('volume_family', ""))
    file_family = vc_ticket.get('file_family', "")
    if not file_family:
        file_family = volume_family.extract_file_family(
            vc_ticket.get('vc', {}).get('volume_family', ""))
    wrapper = vc_ticket.get('storage_group', "")
    if not wrapper:
        wrapper = volume_family.extract_wrapper(
            vc_ticket.get('vc', {}).get('volume_family', ""))

    #Check the mover sub-ticket.
    mover_ticket = ticket.get('mover', {})
    if type(mover_ticket) != types.DictType:
        Trace.log(e_errors.WARNING,
                  "Did not excpect 'mover_ticket' value -- %s -- as type %s."
                  % (str(mover_ticket), str(type(mover_ticket))))
        mover_ticket = {}
    device = mover_ticket.get('device', '')
    mover_name = mover_ticket.get('name','')
    product_id = mover_ticket.get('product_id','')
    device_sn = mover_ticket.get('serial_num','')

    #Check the time sub-ticket.
    time_ticket = ticket.get('times', {})
    if type(time_ticket) != types.DictType:
        Trace.log(e_errors.WARNING,
                  "Did not excpect 'time_ticket' value -- %s -- as type %s."
                  % (str(time_ticket), str(type(time_ticket))))
        time_ticket = {}
    transfer_time = time_ticket.get('transfer_time', 0)
    seek_time = time_ticket.get('seek_time', 0)
    mount_time = time_ticket.get('mount_time', 0)
    in_queue = time_ticket.get('in_queue', 0)
    now = time.time()
    total = now - time_ticket.get('encp_start_time', now)

    #Check miscellaneous field(s).
    unique_id = ticket.get('unique_id', "")
    hostname = ticket.get('encp_ip',
                          ticket.get('wrapper', {}).get('machine',
                                                     ("", "", "", "", "")))[1]
    if hostname:
        try:
            hostname = socket.gethostbyname(hostname)
        except (socket.error, socket.herror, socket.gaierror):
            pass

    #Check status field.
    sts =  ticket.get('status', ('Unknown', None))
    status = sts[0]
    msg = sts[1:]
    if len(msg)==1:
        msg=msg[0]

    #Check paranoid fields.
    paranoid_crc = ticket.get('ecrc', None)
    encp_crc = ticket.get('exfer', {}).get('encp_crc', None)
    mover_crc = ticket.get('fc', {}).get('complete_crc', None)
    if paranoid_crc != None:
        crc = paranoid_crc
    elif encp_crc != None:
        crc = encp_crc
    elif mover_crc != None:
        crc = mover_crc
    else:
        crc = ""

    if type(outputfile) == types.ListType and len(outputfile) == 1:
        outputfile = outputfile[0]
    if type(inputfile) == types.ListType and len(inputfile) == 1:
        inputfile = inputfile[0]

    #Secondary information source to use.
    if not inputfile and ticket.get('infile', None):
        inputfile = ticket['infile']
    if not outputfile and ticket.get('outfile', None):
        outputfile = ticket['outfile']
    if filesize == None and ticket.get('filesize', None):
        filesize = ticket['filesize']
    #These three values work out better if they are empty strings rather than
    # None when the information is not known.
    if inputfile == None:
        inputfile = ""
    if outputfile == None:
        outputfile = ""
    if filesize == None:
        filesize = ""

    # Use an 'r' or 'w' to signify read or write in the accounting db.
    try:
        if is_read(ticket):  #['work'] == "read_from_hsm":
            rw = 'r'
        elif is_write(ticket):
            rw = 'w'
        else:
            rw = "u"
    except EncpError:
        rw = "u"
        
    if not data_access_layer_requested and status != e_errors.OK:
        out=sys.stderr
    else:
        out=sys.stdout

    ###String used to print data access layer.
    data_access_layer_format = """INFILE=%s
OUTFILE=%s
FILESIZE=%s
LABEL=%s
LOCATION=%s
DRIVE=%s
DRIVE_SN=%s
TRANSFER_TIME=%.02f
SEEK_TIME=%.02f
MOUNT_TIME=%.02f
QWAIT_TIME=%.02f
TIME2NOW=%.02f
CRC=%s
STATUS=%s\n"""  #TIME2NOW is TOTAL_TIME, QWAIT_TIME is QUEUE_WAIT_TIME.

    out.write(data_access_layer_format % (inputfile, outputfile, filesize,
                                          external_label,location_cookie,
                                          device, device_sn,
                                          transfer_time, seek_time,
                                          mount_time, in_queue,
                                          total, crc, status))

    out.write('\n')
    out.flush()
    if msg:
        msg=str(msg)
        sys.stderr.write(msg+'\n')
        sys.stderr.flush()

    try:
        format = "INFILE=%s OUTFILE=%s FILESIZE=%s LABEL=%s LOCATION=%s " +\
                 "DRIVE=%s DRIVE_SN=%s TRANSFER_TIME=%.02f "+ \
                 "SEEK_TIME=%.02f MOUNT_TIME=%.02f QWAIT_TIME=%.02f " + \
                 "TIME2NOW=%.02f CRC=%s STATUS=%s"
        msg_type=e_errors.ERROR
        if status == e_errors.OK:
            msg_type = e_errors.INFO
        errmsg=format%(inputfile, outputfile, filesize, 
                       external_label, location_cookie,
                       device,device_sn,
                       transfer_time, seek_time, mount_time,
                       in_queue, total,
                       crc, status)

        if msg:
            #Attach the data access layer info to the msg, but first remove
            # the newline and tab used for readability on the terminal.
            errmsg = errmsg + " " + string.replace(msg, "\n\t", "  ")
        Trace.log(msg_type, errmsg)
    except OSError:
        exc, msg = sys.exc_info()[:2]
        sys.stderr.write("cannot log error message %s\n" % (errmsg,))
        sys.stderr.write("internal error %s %s\n" % (str(exc), str(msg)))

    if not e_errors.is_ok(status):
        if not filesize:
            use_file_size = 0
        else:
            use_file_size = filesize
        #We need to filter out the situations where status is OK.
        # On OK status printed out with the error data_access_layer format
        # can occur with the use of --data-access-layer.  However, such
        # success should not go into the encp_error table.
        acc = get_acc()
        acc.log_encp_error(inputfile, outputfile, use_file_size, storage_group,
                           unique_id, encp_client_version(),
                           status, msg, hostname, time.time(),
                           file_family, wrapper, mover_name,
                           product_id, device_sn, rw, external_label)

#######################################################################

def check_server(csc, server_name):

    # send out an alive request - if config not working, give up
    rcv_timeout = 10
    alive_retries = 360
    
    try:
        stati = csc.alive(server_name, rcv_timeout, alive_retries)
    except (KeyboardInterrupt, SystemExit):
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
    except:
        exc, msg = sys.exc_info()[:2]
        stati={}
        stati["status"] = (e_errors.BROKEN,
                           "Unexpected error (%s, %s) while attempting to "
                           "contact %s." % (str(exc), str(msg), server_name))

    #Quick translation for TIMEDOUT error.  The description part of the
    # error is None by default, this puts something there.
    if stati['status'][0] == e_errors.TIMEDOUT:
        stati['status'] = (e_errors.TIMEDOUT,
                           "Unable to contact %s." % server_name)

    return stati

# get the configuration client and udp client and logger client
# return some information about who we are so it can be used in the ticket

def clients(intf):
    global __logc
    global __alarmc

    # get a configuration server client
    try:
        csc, config = __get_csc((intf.enstore_config_host,
                                 intf.enstore_config_port))
    except EncpError, msg:
        return {'status' : (msg.type, str(msg))}

    #Report on the success of getting the csc and logc.
    #Trace.message(CONFIG_LEVEL, format_class_for_print(client['csc'],'csc'))
    #Trace.message(CONFIG_LEVEL, format_class_for_print(client['logc'],'logc'))

    #If we are only performing a check if a transfer will succeed (at least
    # start) we should turn off logging and alarming.
    if intf.check:
        log_client.LoggerClient.log_func = check_log_func
        alarm_client.AlarmClient.alarm_func = check_alarm_func

    #For performance reasons attempt to obtain the log server address
    # and alarm server address without contacting the configuration
    # server again.
    if type(config) == types.DictType:
        log_server_config = config.get('log_server', {})
        alarm_server_config = config.get('alarm_server', {})

        log_server_ip = log_server_config.get('hostip', None)
        log_server_port = log_server_config.get('port', None)
        alarm_server_ip = alarm_server_config.get('hostip', None)
        alarm_server_port = alarm_server_config.get('port', None)
        
        if log_server_ip and log_server_port:
            log_server_address = (log_server_ip, log_server_port)
        else:
            log_server_address = None
        if alarm_server_ip and alarm_server_port:
            alarm_server_address = (alarm_server_ip, alarm_server_port)
        else:
            alarm_server_address = None
    else:
        log_server_address = None
        alarm_server_address = None
    
    #Get a logger client, this will set the global log client Trace module
    # variable.  If this is not done here, it would get done while
    # creating the client classes for the csc, vc, fc, etc.  This however
    # is to late for the first message to be logged (the one with the
    # command line).  The same applies for the alarm client.
    try:
        __logc = log_client.LoggerClient(
            csc, 'ENCP', flags = enstore_constants.NO_ALARM,
            server_address = log_server_address)
    except SystemExit:
        pass
    try:
        __alarmc = alarm_client.AlarmClient(
            csc, 'ENCP', flags = enstore_constants.NO_LOG,
            server_address = alarm_server_address)
    except SystemExit:
        pass

    #return client
    return {'status' : (e_errors.OK, None)}

##############################################################################

def get_callback_addr(ip = None):  #encp_intf, ip=None):
    # get a port to talk on and listen for connections
    try:
        (host, port, listen_socket) = callback.get_callback(ip)
        listen_socket.listen(4)
    except socket.error:
        #Most likely, there are no more sockets/files left to open.
        host = ""
        port = 0
        listen_socket = None

    callback_addr = (host, port)

    Trace.message(CONFIG_LEVEL,
                  "Listening for mover(s) to call back on (%s, %s)." %
                  callback_addr)

    return callback_addr, listen_socket

def get_udp_callback_addr(encp_intf, udps=None):
    # get a port to talk on and listen for connections
    if udps == None:
        udps = udp_server.UDPServer(None,
                                    receive_timeout=encp_intf.resubmit_timeout)
    else:
	addr = udps.server_address
	udps.__del__()  #Close file descriptors and such.
        udps.__init__(addr, receive_timeout=encp_intf.resubmit_timeout)
        #In the unlikely event that the port is taken by some other process
        # between the two functions above, obtain a new port.  This can
        # cause some timeout errors, but that is life.
        if udps.server_socket == None:
            udps.__init__(None, receive_timeout=encp_intf.resubmit_timeout)

    if udps.server_socket:
        #This servers two purposes.  First, should the route change
        # while the socket is still in use and antispoofing is turned on
        # at the routers, the packets will continue using the original route.
        # Second, we don't want to assume that everything will want this
        # functionality; thus it is not set in udp_server itself.
        #Reliability was choosen because this is UDP and any boost in
        # delivering the packets is good.
        udps.server_socket.setsockopt(socket.IPPROTO_IP, socket.IP_TOS,
                                      socket.IPTOS_RELIABILITY)

    udp_callback_addr = (udps.server_address[0], udps.server_address[1])
    
    Trace.message(CONFIG_LEVEL,
                  "Listening for mover(s) to send route back on (%s, %s)." %
                  udp_callback_addr)

    return udp_callback_addr, udps

##############################################################################

def _get_stat(pathname):
    __pychecker__="unusednames=i"
     
    for i in [0, 1, 2, 3, 4]:
        try:
            statinfo = os.stat(pathname)
            return statinfo
        except (OSError, IOError), msg:
            #Historically all systems have returned ENOENT falsely when
            # a timeout occured and the file really did exist.  This also,
            # happens a lot if pnfs is automounted. One node, flxi04,
            # appears to be throwing EIO instead for these cases.
            if msg.args[0] in [errno.EIO, errno.ENOENT]:
                time.sleep(1)
                continue
            else:
                raise msg

    raise msg

def get_stat(filename):
    global pnfs_is_automounted
    
    pathname = os.path.abspath(filename)

    try:
        #This is intentionally left as an os.stat().  For the case of
        # using the pnfs_agent it doesn't make sense to fail N times
        # here.  Thus, we fail after one time, fall into the exception
        # handling were we either retry the stat (because pnfs is not
        # robust) or we need to ask the pnfs_agent.
        if pnfs_agent_client_requested:
            raise OSError(errno.ENOENT, "Force use of pnfs_agent.")
        else:
            statinfo = os.stat(pathname)
        return statinfo
    except (OSError, IOError), msg:
        if getattr(msg, "errno", None) in [errno.ENOENT, errno.EIO]:
            if pnfs_is_automounted or \
                     __is_pnfs_local_path(pathname, check_name_only = 1):
                #Sometimes when using pnfs mounted locally the NFS client times
                # out and gives the application the error ENOENT.  When in
                # reality the file is fine when asked some time later.
                # Automounting pnfs can cause timeout problems too.
                statinfo = _get_stat(pathname)
                return statinfo
            elif __is_pnfs_remote_path(pathname, check_name_only = 1):
                #Also, when using the pnfs_agent, we will get ENOENT becuase
                # locally the file will not exist.
                pac = get_pac()
                statinfo_ticket = pac.get_stat(pathname)
                if e_errors.is_ok(statinfo_ticket):
                    statinfo = statinfo_ticket['statinfo']
                    return statinfo
                elif statinfo_ticket['status'][0] == e_errors.OSERROR:
                    raise OSError(statinfo_ticket['status'][1])
                elif statinfo_ticket['status'][0] == e_errors.IOERROR:
                    raise IOError(statinfo_ticket['status'][1])
                else:
                    raise EncpError(None, str(statinfo_ticket['status'][1]),
                                    statinfo_ticket['status'][0])

        #If this is a local file that we got an error on, raise it back to
        # the calling function.
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]

#Get the user and group names from the stat member.  Also, get the
# device code information (in octal and decimal).
def stat_decode(statinfo):

    if type(statinfo) != types.TupleType:
        raise TypeError("Expected tuple, not %s." % type(statinfo))

    UNKNOWN = "unknown"
    ERROR = -1

    uid = ERROR
    uname = UNKNOWN
    gid = ERROR
    gname = UNKNOWN
    mode = 0
    mode_octal = 0
    #file_size = ERROR
    inode = 0
    #What these do, I do not know.  MWZ
    rmajor, rminor = (0, 0)
    major, minor = (0, 0)

    #Get the user id of the file's owner.
    try:
        uid = statinfo[stat.ST_UID]
    except (KeyboardInterrupt, SystemExit):
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
    except:
        pass

    #Get the user name of the file's owner.
    try:
        uname = pwd.getpwuid(uid)[0]
    except (KeyboardInterrupt, SystemExit):
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
    except:
        pass

    #Get the group id of the file's owner.
    try:
        gid = statinfo[stat.ST_GID]
    except (KeyboardInterrupt, SystemExit):
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
    except:
        pass

    #Get the group name of the file's owner.
    try:
        gname = grp.getgrgid(gid)[0]
    except (KeyboardInterrupt, SystemExit):
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
    except:
        pass

    #Get the file mode.
    try:
        # always return mode as if it were a file, not directory, so
        #  it can use used in enstore cpio creation  (we will be
        #  creating a file in this directory)
        # real mode is available in self.stat for people who need it
        mode = (statinfo[stat.ST_MODE] % 0777) | 0100000
        mode_octal = str(oct(mode))
    except (KeyboardInterrupt, SystemExit):
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
    except:
        mode = 0
        mode_octal = 0

    #if os.path.exists(self.filepath):
    if stat.S_ISREG(statinfo[stat.ST_MODE]):
        real_file = 1
    else:
        real_file = 0  #Should be the parent directory.

    #Get the file size.
    """
    try:
        if real_file:    #os.path.exists(self.filepath):
            file_size = self.statinfo[stat.ST_SIZE]
            if file_size == 1L:
                file_size = long(self.get_xreference()[2]) #[2] = size
        else:
            try:
                del self.file_size
            except AttributeError:
                pass  #Was not present.
    except (KeyboardInterrupt, SystemExit):
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
    except:
        pass
    """

    #Get the file inode.
    try:
        if real_file:   #os.path.exists(self.filepath):
            inode = statinfo[stat.ST_INO]
        else:
            inode = 0L
    except (KeyboardInterrupt, SystemExit):
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
    except:
        pass

    #Get the major and minor device codes for the device the file
    # resides on.
    try:
        #code_dict = Devcodes.MajMin(self.pnfsFilename)
        #self.major = code_dict["Major"]
        #self.minor = code_dict["Minor"]

        #The following math logic was taken from
        # $ENSTORE_DIR/modules/Devcodes.c.  For performance reasons,
        # this was done in python.  It turns out to be slower to wait
        # for another stat() call in the C implimentation of Devcodes
        # than using the existing stat info implemented in python.
        # This is largly due to pnfs response delays.
        major = int(((statinfo[stat.ST_DEV]) >> 8) & 0xff)
        minor = int((statinfo[stat.ST_DEV]) & 0xff)
    except (KeyboardInterrupt, SystemExit):
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
    except:
        pass

    rtn_dict = {}

    rtn_dict['uid'] = uid
    rtn_dict['uname'] = uname
    rtn_dict['gid'] = gid
    rtn_dict['gname'] = gname
    rtn_dict['mode'] = mode
    rtn_dict['mode_octal'] = mode_octal
    # rtn_dict['file_size'] = ERROR
    rtn_dict['inode'] = inode
    #What these do, I do not know.  MWZ
    rtn_dict['rmajor'] = rmajor
    rtn_dict['rminor'] = rminor
    rtn_dict['major'] = major
    rtn_dict['minor'] = minor

    return rtn_dict


def is_pnfs_path(filename, check_name_only = None):

    pathname = os.path.abspath(filename)

    rtn = __is_pnfs_local_path(filename)
    if not rtn:
        #If we get here we did not find a matching locally mounted
        # pnfs filesystem.  Ask the pnfs agent.
        pac = get_pac()
        rtn = pac.is_pnfs_path(pathname, check_name_only = check_name_only)
    return rtn

# for automounted pnfs

pnfs_is_automounted = 0

# access_check(path, mode) -- a wrapper for os.access() that retries for
#                             automatically mounted file system

def access_check(path, mode):

    #If pnfs is not auto mounted, simply call e_access.  We assume that
    # if the user specifies this option that they know that the pnfs
    # filesystem is locally mounted.
    if pnfs_is_automounted:
        # automatically retry 6 times, one second delay each
        i = 0
        while i < 6:
            if e_access(path, mode):
                return 1
            time.sleep(1)
            i = i + 1

        #use the effective ids and not the reals used by os.access().
        return e_access(path, mode)
    else:
        rtn = e_access(path, mode)
        if rtn:
            return rtn

    #Before giving up that this is a pnfs file, ask the pnfs_agent.
    # Is there a more performance efficent way?
    if pnfs_agent_client_requested or pnfs_agent_client_allowed:
        pac = get_pac()
        return  pac.e_access(path, mode)

    return False

#Make sure that the filename is valid.
def filename_check(filename):
    #pnfs (v3.1.7) only supports filepaths segments that are
    # less than 200 characters.
    for directory in filename.split("/"):
        if len(directory) > 199:
            raise EncpError(errno.ENAMETOOLONG,
                            "%s: %s" % (os.strerror(errno.ENAMETOOLONG),
                                        directory),
                            e_errors.USERERROR)

    #limit the usable characters for a filename.
    if not charset.is_in_filenamecharset(filename):
        st = ""
        for ch in filename: #grab all illegal characters.
            if ch not in charset.filenamecharset:
                st = st + ch
        raise EncpError(errno.EILSEQ,
                        'Filepath uses non-printable characters: %s' % (st,),
                        e_errors.USERERROR)

#Make sure that the filesystem can handle the filesize.
def filesystem_check(work_ticket):

    #Get the target from the ticket.
    target_file = work_ticket['outfile']
    if target_file in ["/dev/null", "/dev/zero",
                       "/dev/random", "/dev/urandom"]:
        #These are special cases.
        return
    target_filesystem = os.path.split(target_file)[0]

    #Get the file size from the ticket.
    size = work_ticket['file_size']
        
    #Not all operating systems support this POSIX limit yet (ie OSF1).
    try:
        #get the maximum filesize the local filesystem allows.
        bits = os.pathconf(target_filesystem,
                           os.pathconf_names['PC_FILESIZEBITS'])
    except KeyError:
        return
    except (OSError, IOError):
        msg = sys.exc_info()[1]
        msg2 = "System error obtaining maximum file size for " \
               "filesystem %s." % (target_filesystem,)
        Trace.log(e_errors.ERROR, str(msg) + ": " + msg2)
        if getattr(msg, "errno", None) == errno.EINVAL:
            sys.stderr.write("WARNING: %s  Continuing.\n" % (msg2,))
            return  #Nothing to test, user needs to be carefull.
        else:
            raise EncpError(getattr(msg, "errno", None), msg2,
                            e_errors.OSERROR, work_ticket)

    #Need to convert the bits into the maximum file size allowed.
    filesystem_max = 2L**(bits - 1) - 1

    #Normally, encp would find this an error, but "Get" may not.  Raise
    # an exception and let the caller decide.
    if size == None:
        raise EncpError(None, "Filesize is not known.", e_errors.OSERROR,
                        work_ticket)

    #Compare the max sizes.
    elif size > filesystem_max:
        raise EncpError(errno.EFBIG,
                        "Filesize (%s) larger than filesystem allows (%s)." \
                        % (size, filesystem_max),
                        e_errors.USERERROR, work_ticket)

#Make sure that the wrapper can handle the filesize.
def wrappersize_check(work_ticket):

    #Get the wrapper from the ticket.
    vc_ticket = work_ticket.get('vc', {})
    wrapper = vc_ticket.get('wrapper', None)

    #Get the file size from the ticket.
    size = work_ticket['file_size']

    try:
        # get a configuration server and the max filesize the wrappers allow.
        csc = get_csc()
        wrappersizes = csc.get('wrappersizes', {})
        wrapper_max = wrappersizes.get(wrapper, MAX_FILE_SIZE)
    except (OSError, IOError):
        msg = sys.exc_info()[1]
        raise EncpError(getattr(msg,"errno",None), str(msg), e_errors.OSERROR,
                        work_ticket)

    if size > wrapper_max:
        raise EncpError(errno.EFBIG,
                        "Filesize (%s) larger than wrapper (%s) allows (%s)." \
                        % (size, wrapper, wrapper_max),
                        e_errors.USERERROR, work_ticket)
    
#Make sure that the library can handle the filesize.
#def librarysize_check(target_filepath, inputfile):
def librarysize_check(work_ticket):

    #Get the library from the ticket.
    vc_ticket = work_ticket.get('vc', {})
    library = vc_ticket.get('library', None)
    if library[-16:] == ".library_manager":
        use_lm = library
    else:
        use_lm = library + ".library_manager"

    #Get the file size from the ticket.
    size = work_ticket['file_size']

    try:
        #Determine the max allowable size for the given library.
        csc = get_csc()
        lib_info = csc.get(use_lm, {})
        library_max = lib_info.get('max_file_size', MAX_FILE_SIZE)
    except (OSError, IOError):
        msg = sys.exc_info()[1]
        raise EncpError(getattr(msg,"errno",None), str(msg), e_errors.OSERROR,
                        work_ticket)

    #Compare the max sizes allowed for these various conditions.
    if size > library_max:
        raise EncpError(errno.EFBIG,
                        "Filesize (%s) larger than library (%s) allows (%s)." \
                        % (size, library, library_max),
                        e_errors.USERERROR, work_ticket)

# check the input file list for consistency
def inputfile_check(input_files, e):
    # create internal list of input unix files even if just 1 file passed in
    if type(input_files)==types.ListType:
        inputlist=input_files
    else:
        inputlist = [input_files]

    #Get the correct type of pnfs interface to use.
    p = Pnfs()

    # check the input unix file. if files don't exits, we bomb out to the user
    for i in range(0, len(inputlist)):
        #If we already know for the tape read (--volume) that the file
        # is foobar, then skip this test and move to the next.
        if is_location_cookie(inputlist[i]) and e.volume:
            continue
            
        try:
            #check to make sure that the filename string doesn't have any
            # wackiness to it.
            filename_check(inputlist[i])

            """
            # input files must exist - also handle automounting and pnfs agent.
            if not access_check(inputlist[i], os.F_OK):

                #We don't want to fail immediatly.  On reads it is ok for
                # encp to check all three paths to the experiment's file:
                # /pnfs/xyz, /pnfs/fs/usr/xyz and /pnfs/fnal.gov/xyz.

                if not pnfs.is_pnfs_path(inputlist[i]):
                    raise EncpError(errno.ENOENT, inputlist[i],
                                    e_errors.USERERROR,
                                    {'infile' : inputlist[i]})
                elif access_check(get_enstore_pnfs_path(inputlist[i]),
                                  os.F_OK):
                    inputlist[i] = get_enstore_pnfs_path(inputlist[i])
                elif access_check(get_enstore_fs_path(inputlist[i]),
                                  os.F_OK):
                    inputlist[i] = get_enstore_fs_path(inputlist[i])
                elif access_check(get_enstore_canonical_path(inputlist[i]),
                                  os.F_OK):
                    inputlist[i] = get_enstore_canonical_path(inputlist[i])
                else:
                    #There is a real problem with the file.  Fail the transfer.
                    raise EncpError(errno.ENOENT, inputlist[i],
                                    e_errors.USERERROR,
                                    {'infile' : inputlist[i]})

            # input files must have read permissions.
            if not access_check(inputlist[i], os.R_OK):
                raise EncpError(errno.EACCES, inputlist[i],
                                e_errors.USERERROR, {'infile' : inputlist[i]})
            """

            if not e.override_deleted:
                    #and request['fc']['deleted'] != 'no'):
            
                #Since the file exists, we can get its stats.
                #     statinfo = os.stat(inputlist[i])
                statinfo = get_stat(inputlist[i])

                # input files can't be directories
                if not stat.S_ISREG(statinfo[stat.ST_MODE]):
                    raise EncpError(errno.EISDIR, inputlist[i],
                                    e_errors.USERERROR,
                                    {'infile' : inputlist[i]})

                ###
                ### We should have permission checks here, based on the
                ### stat info.
                ### 

                #For Reads make sure the filesystem size and the pnfs size
                # match.  If the PNFS filesystem and layer 4 sizes are
                # different, calling this function raises OSError exception.
                #if p:
                if is_pnfs_path(inputlist[i]):
                    p.get_file_size(inputlist[i])
                
            # we cannot allow 2 input files to be the same
            # this will cause the 2nd to just overwrite the 1st
            try:
                match_index = inputlist[:i].index(inputlist[i])
                
                raise EncpError(None,
                                'Duplicate entry %s'%(inputlist[match_index],),
                                e_errors.USERERROR,
                                {'infile' : inputlist[i]})
            except ValueError:
                pass  #There is no error.

        #except KeyboardInterrupt:
        #    raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except EncpError:
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
            #msg = sys.exc_info()[1]
            #size = get_file_size(inputlist[i])
            #print_data_access_layer_format(inputlist[i], "", size,
            #                               {'status':(msg.type, msg.strerror)})
            #quit()
        except (OSError, IOError):
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
            #msg = sys.exc_info()[1]
            #size = get_file_size(inputlist[i])
            #error = errno.errorcode.get(getattr(msg, "errno", None),
            #                            errno.errorcode[errno.ENODATA])
            #print_data_access_layer_format(
            #    inputlist[i], "", size, {'status':(error, str(msg))})
            #quit()

    return

# check the output file list for consistency
# generate names based on input list if required
#"inputlist" is the list of input files.  "output" is a list with one element.

def outputfile_check(inputlist, outputlist, e):
    dcache = e.put_cache #being lazy

    # create internal list of input unix files even if just 1 file passed in
    if type(inputlist)==types.ListType:
        pass  #inputlist = inputlist
    else:
        inputlist = [inputlist]

    # create internal list of input unix files even if just 1 file passed in
    if type(outputlist)==types.ListType:
        pass  #outputlist = outputlist
    else:
        outputlist = [outputlist]

    #Get the correct type of pnfs interface to use.
    p = Pnfs()

    # Make sure we can open the files. If we can't, we bomb out to user
    for i in range(len(inputlist)):

            #If output location is /dev/null, skip the checks.
            if outputlist[i] in ["/dev/null", "/dev/zero",
                                 "/dev/random", "/dev/urandom"]:
                continue

        #try:

            #check to make sure that the filename string doesn't have any
            # wackiness to it.
            filename_check(outputlist[i])

            #There are four (4) possible senerios for the following test(s).
            #The two conditions are:
            # 1) If dache involked encp.
            # 2) If the file does not exist or not.

            #Test case when used by a user and the file does not exist (as is
            # should be).
            if not access_check(outputlist[i], os.F_OK) and not dcache:
                directory = get_directory_name(outputlist[i])
                
                #Check for existance and write permissions to the directory.
                if not access_check(directory, os.F_OK):
                    raise EncpError(errno.ENOENT, directory,
                                    e_errors.USERERROR,
                                    {'outfile' : outputlist[i]})

                if not os.path.isdir(directory):
                    raise EncpError(errno.ENOTDIR, directory,
                                    e_errors.USERERROR,
                                    {'outfile' : outputlist[i]})
                                        
                if not access_check(directory, os.W_OK):
                    raise EncpError(errno.EACCES, directory,
                                    e_errors.USERERROR,
                                    {'outfile' : outputlist[i]})

                #Looks like the file is good.
                outputlist.append(outputlist[i])
                
            #File exists when run by a normal user.
            elif access_check(outputlist[i], os.F_OK) and not dcache:
                raise EncpError(errno.EEXIST, outputlist[i],
                                e_errors.USERERROR,
                                {'outfile' : outputlist[i]})

            #The file does not already exits and it is a dcache transfer.
            elif not access_check(outputlist[i], os.F_OK) and dcache:
                #Check if the filesystem is corrupted.  This entails looking
                # for directory entries without valid inodes.
                directory_listing=os.listdir(get_directory_name(outputlist[i]))
                if os.path.basename(outputlist[i]) in directory_listing:
                    #If the platform supports EFSCORRUPTED use it.
                    # Otherwise use the generic EIO.
                    if hasattr(errno, 'EFSCORRUPTED'):
                        error = errno['EFSCORRUPTED'] #Not widely supported.
                    else:
                        error = errno['EIO']
                    raise EncpError(error, "Filesystem is corrupt.",
                                    e_errors.FILESYSTEM_CORRUPT,
                                    {'outfile' : outputlist[i]})
                else:
                    raise EncpError(errno.ENOENT, outputlist[i],
                                    e_errors.USERERROR,
                                    {'outfile' : outputlist[i]})

            #The file exits, as it should, for a dache transfer.
            elif access_check(outputlist[i], os.F_OK) and dcache:
                #Before continuing lets check to see if layers 1 and 4 are
                # empty first.  This check is being added because it appears
                # that the dcache can (and has) written multiple copies of
                # the same file to Enstore.  The filesize being zero trick
                # doesn't work with dcache, since the dcache sets the filesize.
                try:
                    #These to test read access.  They also allow us to
                    # determine if there is already a file written to enstore
                    # with this same filename.
                    layer1 = p.readlayer(1, outputlist[i])
                    layer4 = p.readlayer(4, outputlist[i])
                    
                    #Test if the layers are empty.
                    if layer1 != [] or layer4 != []:
                        #The layers are not empty.
                        raise EncpError(errno.EEXIST,
                                        "Layer 1 and layer 4 are already set.",
                                        e_errors.PNFS_ERROR,
                                        {'outfile' : outputlist[i]})
                    else:
                        #The layers are empty.
                        outputlist.append(outputlist[i])

                    #Try to write an empty string to layer 1.  If this fails,
                    # it will most likely fail becuase of:
                    # 1) a lack of permission access to the file (EACCES)
                    # 2) the database is read-only (EPERM)
                    # 3) the user writing (usually root) is not the
                    #    owner of the file and the node is not in the trusted
                    #    list for pnfs (EPERM)
                    # 4) user root is modifying something outside of the
                    #    /pnfs/fs/usr/xyz/ filesystem (EPERM).
                    p.writelayer(1, "", outputlist[i])

                    #Get the outfile size.
                    try:
                        ofilesize = long(os.stat(outputlist[i])[stat.ST_SIZE])
                    except OSError, msg:
                        raise EncpError(msg.errno,
                                        "Unable to get file size for file %s."
                                        % (outputlist[i]),
                                        e_errors.OSERROR,
                                        {'outfile' : outputlist[i]})
                    #Get the infile size.
                    try:
                        ifilesize = long(os.stat(inputlist[i])[stat.ST_SIZE])
                    except OSError, msg:
                        raise EncpError(msg.errno,
                                        "Unable to get file size for file %s."
                                        % (inputlist[i]),
                                        e_errors.OSERROR,
                                        {'outfile' : outputlist[i]})

                    if ofilesize == 1 and ifilesize > TWO_G:
                        #If the file is large, there is nothing to compare.
                        # Drop in here to skip the following elif statement.
                        pass
                    #Test if the output file size matches the input file size.
                    elif ifilesize != ofilesize:
                        raise EncpError(None,
                                        "Expected local file size (%s) to "
                                        "equal remote file size (%s)." %
                                        (ifilesize, ofilesize),
                                        e_errors.FILE_MODIFIED,
                                        {'outfile' : outputlist[i]})
                except (OSError, IOError), msg:
                    #Some other non-foreseen error has occured.
                    error = getattr(msg, "errno", None)
                    if error == errno.EPERM or error == errno.EACCES:
                        enstore_error = e_errors.USERERROR
                    else:
                        enstore_error = e_errors.PNFS_ERROR
                    raise EncpError(error, msg.filename, enstore_error)
                except EncpError, msg:
                    raise msg

            else:
                raise EncpError(None,
                             "Failed outputfile check for: %s" % outputlist[i],
                                e_errors.UNKNOWN,
                                {'outfile' : outputlist[i]})

            # we cannot allow 2 output files to be the same
            # this will cause the 2nd to just overwrite the 1st
            # In principle, this is already taken care of in the
            # inputfile_check, but do it again just to make sure in case
            # someone changes protocol
            try:
                match_index = inputlist[:i].index(inputlist[i])
                raise EncpError(None,
                                'Duplicate entry %s'%(inputlist[match_index],),
                                e_errors.USERERROR,
                                {'outfile' : outputlist[i]})
            except ValueError:
                pass  #There is no error.

#######################################################################

#create_zero_length_pnfs_files() and create_zero_length_local_files()
# raise an OSError exception on error.

def create_zero_length_pnfs_files(filenames, e = None):
    if type(filenames) != types.ListType:
        filenames = [filenames]

    if e and is_write(e) and e.outtype == RHSMFILE:
        remote_create = True
        pac = get_pac()
    else:
        remote_create = False

    #now try to atomically create each file
    for f in filenames:
        if type(f) == types.DictType:
            fname = f['wrapper']['pnfsFilename']
        else:
            fname = f

        delete_at_exit.register(fname)
        if remote_create: # Use pnfs_agent to access pnfs file.
            try:
                pac.creat(fname, mode=0666)

                if type(f) == types.DictType:
                    f['wrapper']['inode'] = pac.get_stat(fname)[stat.ST_INO]
                    f['fc']['pnfsid'] = pac.get_id(fname)
            except OSError, msg:
                raise OSError, msg

        else: # locally mounted filesystem.
            local_errno = -1
            while local_errno == -1 or local_errno == errno.EAGAIN:
                try:
                    #raises OSError on error.
                    fd = atomic.open(fname, mode=0666)

                    if type(f) == types.DictType:
                        #The inode is used later on to determine if
                        # another process has deleted or removed the
                        # remote pnfs file.
                        f['wrapper']['inode'] = os.fstat(fd)[stat.ST_INO]
                        #The pnfs id is used to track down the new paths
                        # to files that were moved before encp completes.
                        f['fc']['pnfsid'] = pnfs.Pnfs(fname).get_id()

                    os.close(fd)
                    local_errno = 0
                except OSError, msg:
                    if msg.args[0] == errno.EAGAIN:
                        #If we got here then we just created a 'ghost' file
                        # with the temporary lock filename.  Lets wait
                        # a moment before trying again.
                        time.sleep(2)
                        continue
                    
                    raise OSError, msg

def create_zero_length_local_files(filenames):
    if type(filenames) != types.ListType:
        filenames = [filenames]

    #now try to atomically create each file
    for f in filenames:
        if type(f) == types.DictType:
            fname = f['wrapper']['fullname']

            if fname in ["/dev/null", "/dev/zero",
                         "/dev/random", "/dev/urandom"]:
                #If this raises an error, there are massive problems going on.
                f['local_inode'] = os.stat(fname)[stat.ST_INO]
                return
        else:
            if f in ["/dev/null", "/dev/zero",
                     "/dev/random", "/dev/urandom"]:
                return
            else:
                fname = f
            
        #try:
        delete_at_exit.register(fname)
        fd = atomic.open(fname, mode=0666) #raises OSError on error.
        
        if type(f) == types.DictType:
            #The inode is used later on to determine if another process
            # has deleted or removed the local file.
            f['local_inode'] = os.fstat(fd)[stat.ST_INO]
            
        os.close(fd)


#def create_zero_length_files(filenames, raise_error = None):
#    if type(filenames) != types.ListType:
#        filenames = [filenames]
#
#    #now try to atomically create each file
#    for f in filenames:
#        if f == "/dev/null":
#            return
#            
#        try:
#            fd = atomic.open(f, mode=0666) #raises OSError on error.
#
#            if type(f) == types.DictType:
#                f['output_inode'] = os.fstat(fd)[stat.ST_INO]
#            
#            os.close(fd)
#
#        except OSError:
#            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
#            #if raise_error:
#            #    #Originally needed for "Get", don't abort.
#            #    exc, msg = sys.exc_info()[:2]
#            #    raise exc, msg
#            #else:
#            #    #Keep the default action the same.
#            #    exc, msg = sys.exc_info()[:2]
#            #    error = errno.errorcode.get(getattr(msg, "errno", None),
#            #                                errno.errorcode[errno.ENODATA])
#            #    print_data_access_layer_format('', f, 0,
#            #                                   {'status': (error, str(msg))})
#            #    
#            #    quit()

#######################################################################

#def file_check(e):
#    # check the output pnfs files(s) names
#    # bomb out if they exist already
#    inputlist, file_size = inputfile_check(e.input, e)
#    n_input = len(inputlist)
#
#    Trace.message(TICKET_LEVEL, "file count=" + str(n_input))
#    Trace.message(TICKET_LEVEL, "inputlist=" + str(inputlist))
#    Trace.message(TICKET_LEVEL, "file_size=" + str(file_size))
#
#    # check (and generate) the output pnfs files(s) names
#    # bomb out if they exist already
#    outputlist = outputfile_check(e.input, e.output, e)
#
#    Trace.message(TICKET_LEVEL, "outputlist=" + str(outputlist))
#
#    return n_input, file_size, inputlist, outputlist

#######################################################################

#Only one of bfid and volume should be specified at one time.
#Note: Even though encp no longer uses this function; 'get' still does.
def get_clerks(bfid_or_volume=None):
    global __acc

    #if is_volume(bfid_or_volume):
    #    volume = bfid_or_volume
    #    bfid = None
    #elif is_bfid(bfid_or_volume):
    #    volume = None
    #    bfid = bfid_or_volume
    #else:
    #    volume = None
    #    bfid = None
        
    #If a bfid was passed in, we must obtain the fcc first.  This will
    # find the correct __csc.  The same is true with the vcc if a volume
    # name is passed in.
    if is_volume(bfid_or_volume):
        #Set the volume.
        volume = bfid_or_volume
        bfid = None
        #Get the clerk clients.
        vcc = get_vcc(volume)  #Make sure vcc is done before fcc.
        fcc = get_fcc(None)
    elif is_bfid(bfid_or_volume):
        #Set the bfid.
        volume = None
        bfid = bfid_or_volume
        #Get the clerk clients.
        fcc = get_fcc(bfid)  #Make sure fcc is done before vcc.
        vcc = get_vcc(None)
    else: #Go with the defaults.
        volume = None
        bfid = None
        fcc = get_fcc(None)
        vcc = get_vcc(None)

    e_ticket = {}
    if volume:
        e_ticket = {'volume' : volume}

    if not fcc or fcc.server_address == None:
        raise EncpError(errno.ENOPROTOOPT,
                        "File clerk not available",
                        e_errors.NET_ERROR, e_ticket)
    
    if not vcc or vcc.server_address == None:
        raise EncpError(errno.ENOPROTOOPT,
                        "Volume clerk not available",
                        e_errors.NET_ERROR, e_ticket)

    #Snag the configuration server client for the system that contains the
    # file clerk where the file was stored.  This is determined based on
    # the bfid's brand.  By this time the csc is set correctly, thus
    # don't pass it any parameters and we will get the cached csc.
    __acc = get_acc()
    #csc = get_csc()   #(bfid_or_volume)
    # we only have the correct crc now (reads)
    #__acc = accounting_client.accClient(csc, logname = 'ENCP',
    #                                    logc = __logc, alarmc = __alarmc)

    return vcc, fcc

############################################################################
############################################################################

def get_dinfo():
    #If the environmental variable exists, send it to the lm.
    try:
        encp_daq = os.environ['ENCP_DAQ']
    except KeyError:
        encp_daq = None

    return encp_daq

#Some stat fields need to be extracted and modified.
def get_minfo(statinfo):

    
    rtn = {}

    if not statinfo:
        #We should only get here only if reading a deleted file using
        # --get-bfid and --override-deleted.
        rtn['inode'] = 0L
        rtn['major'] = 0
        rtn['minor'] = 0
        rtn['mode'] = 0
        return rtn

    st_dec_dict = stat_decode(statinfo)
    
    rtn['uid'] = st_dec_dict['uid']
    rtn['uname'] = st_dec_dict['uname']
    rtn['gid'] = st_dec_dict['gid']
    rtn['gname'] = st_dec_dict['gname']
    rtn['major'] = st_dec_dict['major']
    rtn['minor'] = st_dec_dict['minor']
    rtn['rmajor'] = st_dec_dict['rmajor']
    rtn['rminor'] = st_dec_dict['rminor']
    rtn['mode'] = st_dec_dict['mode']
    rtn['mode_octal'] = st_dec_dict['mode_octal']
    rtn['inode'] = st_dec_dict['inode']
    rtn['pstat'] = statinfo

    return rtn

def get_uinfo(e = None):
    uinfo = {}
    uinfo['uid'] = os.geteuid()
    uinfo['gid'] = os.getegid()

    if uinfo['uid'] == 0 and uinfo['gid'] == 0 and \
       e != None and e.put_cache:
        # For the case of dcache writes into enstore; we should use the
        # ownership of the zero length pnfs file (obtained from get_pinfo())
        # created by dCache.
        return {}
    
    if uinfo['uid'] == 0 and uinfo['gid'] == 0 \
       and e != None and e.put_cache:
        # For the case of dcache writes into enstore; we should use the
        # ownership of the zero length pnfs file created by dCache.
        return {}
    
    try:
        uinfo['gname'] = grp.getgrgid(uinfo['gid'])[0]
    except (ValueError, AttributeError, TypeError, IndexError, KeyError):
        uinfo['gname'] = 'unknown'
    try:
        uinfo['uname'] = pwd.getpwuid(uinfo['uid'])[0]
    except (ValueError, AttributeError, TypeError, IndexError, KeyError):
        uinfo['uname'] = 'unknown'
    uinfo['machine'] = os.uname()

    return uinfo

def get_finfo(inputfile, outputfile, e):
    finfo = {}

    if is_write(e):
        pnfs_file = outputfile
        local_file = inputfile
    else: #reads
        local_file = outputfile
        pnfs_file = inputfile

    #These exist for reads and writes.
    finfo['fullname'] = local_file
    finfo['pnfsFilename'] = pnfs_file
    finfo['sanity_size'] = 65536

    #Append these for writes.
    if is_write(e):
        finfo['mode'] = os.stat(local_file)[stat.ST_MODE]
        finfo['mtime'] = int(time.time())
    
    return finfo

#This function takes as parameters...
def get_winfo(pinfo, uinfo, finfo):

    # create the wrapper subticket - copy all the pnfs info 
    # into it for starters
    wrapper = {}

    # store the pnfs information info into the wrapper
    for key in pinfo.keys():
        wrapper[key] = pinfo[key]

    # store the file information into the wrapper - finfo['mode'] should
    # override the pinfo['mode'] key when present for writes.
    for key in finfo.keys():
        wrapper[key] = finfo[key]

    # the user keys takes precedence over the pnfs keys
    for key in uinfo.keys():
        wrapper[key] = uinfo[key]
        
    return wrapper

def get_einfo(e):
    encp_el = {}
    encp_el["basepri"] = e.priority
    encp_el["adminpri"] = e.admpri
    encp_el["delpri"] = e.delpri
    encp_el["agetime"] = e.age_time
    encp_el["delayed_dismount"] = e.delayed_dismount

    return encp_el

#Return the corrected full filenames of the inputfile and output file.
def get_ininfo(inputfile):

    # Add the name if necessary.
    if inputfile in ["/dev/zero", "/dev/random", "/dev/urandom"]:
        return inputfile
    
    if access_check(inputfile, os.F_OK):
        return inputfile
    
    # get fully qualified name
    unused, ifullname, unused, ibasename = fullpath(inputfile)

    for fname in [get_enstore_pnfs_path(inputfile),
                  get_enstore_fs_path(inputfile),
                  get_enstore_canonical_path(inputfile)]:
        if access_check(fname, os.F_OK):
            return fname

    #There is a real problem with the file.  Fail the transfer.
    raise EncpError(errno.ENOENT, inputfile,
                    e_errors.USERERROR,
                    {'infile' : inputfile})
    
def get_oninfo(inputfile, outputfile, e):
    unused, ofullname, unused, unused = fullpath(outputfile) #e.output[0])

    if ofullname in ["/dev/null", "/dev/zero",
                     "/dev/random", "/dev/urandom"]:
        #if /dev/null is target, skip elifs.
        return ofullname

    munge_name = False
    if (len(e.input) > 1):
        munge_name = True
    elif (len(e.input) == 1 and e.outtype == HSMFILE and \
          os.path.isdir(ofullname)):
        munge_name = True
    elif (len(e.input) == 1 and e.outtype == UNIXFILE and \
          os.path.isdir(ofullname)):
        munge_name = True
    elif (len(e.input) == 1 and e.outtype == RHSMFILE and \
          get_pac().isdir(ofullname)):
        munge_name = True

    if munge_name:
        ibasename = os.path.basename(inputfile)
        ofullname = os.path.join(ofullname, ibasename)
        unused, ofullname, unused, unused = fullpath(ofullname)

    return ofullname

############################################################################
############################################################################

#The following function is the depricated functionality of
# open_routing_socket().  Since "get" still uses this socket, this
# functionality must remain somewhere.
def open_udp_socket(udp_server, unique_id_list, encp_intf):

    time_to_open_udp_socket = time.time()

    Trace.message(INFO_LEVEL, "Waiting for udp callback at address: %s" %
                  str(udp_server.server_socket.getsockname()))
    Trace.log(e_errors.INFO, "Waiting for udp callback at address: %s" %
                  str(udp_server.server_socket.getsockname()))

    udp_ticket = None

    if not udp_server:
        return udp_ticket

    start_time = time.time()

    while(time.time() - start_time < encp_intf.resubmit_timeout):
        try:
            #Get the udp ticket.
            udp_ticket = udp_server.process_request()
        except socket.error, msg:
            Trace.log(e_errors.ERROR, str(msg))
            raise EncpError(msg.args[0], str(msg),
                            e_errors.NET_ERROR)

        #If udp_server.process_request() fails it returns None.
        if type(udp_ticket) != types.DictionaryType:
            continue
        #Something really bad happened.
        elif e_errors.is_non_retriable(udp_ticket.get('status', None)):
            break   #Process the error.
        #If udp_server.process_request() returns correct value.
        elif udp_ticket.has_key('unique_id') and \
                 udp_ticket['unique_id'] in unique_id_list:
            break   #Return the response.
        #It is not what we were looking for.
        else:
            continue
    else:
        raise EncpError(errno.ETIMEDOUT,
                        "Mover did not call udp back.", e_errors.TIMEDOUT)

    udp_server.reply_to_caller(udp_ticket)

    Trace.message(TIME_LEVEL, "Time to open udp socket: %s sec." %
                  (time.time() - time_to_open_udp_socket,))

    #It will most likely be a while, so this would be a good time to
    # perform this maintenance.
    collect_garbage()

    return udp_ticket

##############################################################################

def open_routing_socket(mover_ip, encp_intf):

    time_to_open_routing_socket = time.time()

    #If requested print a message.
    Trace.message(INFO_LEVEL, "Setting up routing table.")

    #Determine if reading or writing.  This only has importance on
    # mulithomed machines were an interface needs to be choosen based
    # on reading and writing usages/rates of the interfaces.
    if getattr(encp_intf, "output", "") == "hsmfile":
        mode = 1 #write
    else:
        mode = 0 #read
    #Force a reload of the enstore.conf file.  This updates the global
    # cached version of the enstore.conf file information.
    host_config.update_cached_config()
    #set up any special network load-balancing voodoo
    interface=host_config.check_load_balance(mode=mode)
    #load balencing...
    if interface:
        #Record which interface was choosen.
        if_name = interface.get('interface', "unknown")
        Trace.message(4, "Choosing interface: %s" % if_name)
        Trace.log(e_errors.INFO, "Choosing interface: %s" % if_name)

        ip = interface.get('ip')
	if ip and mover_ip:   #route_ticket.get('mover_ip', None):
	    #With this loop, give another encp 2 seconds to delete the route
	    # it is using.  After this time, it will be assumed that the encp
	    # died before it deleted the route.
	    start_time = time.time()
	    while(time.time() - start_time < 2):

                try:
                    host_config.update_cached_routes()
                    route_list = host_config.get_routes()
                except (OSError, IOError), msg:
                    Trace.log(e_errors.ERROR, str(msg))
                    raise EncpError(getattr(msg,"errno",None),
                                    str(msg), e_errors.OSERROR)
                if not route_list:
                    Trace.log(e_errors.ERROR, str(msg))
                    raise EncpError(None, "netstat output is empty",
                                    e_errors.OSERROR)
                    
		for route in route_list:
		    #if route_ticket['mover_ip'] == route['Destination']:
		    if mover_ip == route['Destination']:
			break
		else:
		    break

		time.sleep(1)
	    
            try:
                #This is were the interface selection magic occurs.
                #host_config.setup_interface(route_ticket['mover_ip'], ip)
		host_config.setup_interface(mover_ip, ip)
            except (OSError, IOError), msg:
                Trace.log(e_errors.ERROR, str(msg))
                raise EncpError(getattr(msg,"errno",None),
                                str(msg), e_errors.OSERROR)
            except socket.error, msg:
                Trace.log(e_errors.ERROR, str(msg))
                raise EncpError(msg.args[0],
                                str(msg), e_errors.OSERROR)
	    
	    #Return the ip of the local interface the data socket needs
	    # to bind to in order for the antispoofing problem to be avoided.
	    return ip

    Trace.message(TIME_LEVEL, "Time to open routing socket: %s sec." %
                  (time.time() - time_to_open_routing_socket,))

    #No route was set.
    return None

##############################################################################

def open_control_socket(listen_socket, mover_timeout):

    time_to_open_control_socket = time.time()

    message = "Waiting for mover to connect control socket at address: %s" \
              % str(listen_socket.getsockname()) + elapsed_string()
    Trace.message(INFO_LEVEL, message)
    Trace.log(e_errors.INFO, message)

    start_wait_time = time.time()
    wait_left_time = mover_timeout
    while wait_left_time:
        
        read_fds, unused, unused = select.select([listen_socket], [], [],
                                                 wait_left_time)
        
        #If there are no successful connected sockets, then select timedout.
        if not read_fds:
            raise EncpError(errno.ETIMEDOUT,
                            "Mover did not call control back.",
                            e_errors.TIMEDOUT)

        control_socket, address = listen_socket.accept()

        if not hostaddr.allow(address):
            try:
                control_socket.close()
            except socket.error:
                pass
            #raise EncpError(errno.EPERM, "Host %s not allowed." % address[0],
            #                e_errors.NOT_ALWD_EXCEPTION)
            wait_left_time = start_wait_time + mover_timeout - time.time()
            wait_left_time = max(wait_left_time, 0)
            continue

        #wait_left_time = start_wait_time + mover_timeout - time.time()
        #wait_left_time = max(wait_left_time, 0)

        try:
            #This should help the connection.  It also seems to allow the
            # connection to survive the antispoofing/routing problem for
            # properly configured (enstore.conf & enroute2) multihomed nodes.
            control_socket.setsockopt(socket.IPPROTO_IP, socket.IP_TOS,
                                     socket.IPTOS_LOWDELAY)
        except socket.error, msg:
            sys.stderr.write("Socket error setting IPTOS_LOWDELAY option: %s\n"
                             % str(msg))

        try:
            ticket = callback.read_tcp_obj(control_socket)
        except e_errors.TCP_EXCEPTION:
            try:
                control_socket.close()
            except socket.error:
                pass
            #raise EncpError(errno.EPROTO, "Unable to obtain mover response",
            #                e_errors.TCP_EXCEPTION)
            wait_left_time = start_wait_time + mover_timeout - time.time()
            wait_left_time = max(wait_left_time, 0)
            continue

        if not e_errors.is_ok(ticket):
            #If the mover already returned an error, don't bother checking if
            # it is closed already...
            return control_socket, address, ticket

        break

    message = "Control socket %s is connected to %s for %s." % \
              (control_socket.getsockname(),
               control_socket.getpeername(),
               ticket.get('unique_id', "Unknown")) + elapsed_string()
    Trace.message(INFO_LEVEL, message)
    Trace.log(e_errors.INFO, message)

    Trace.message(TIME_LEVEL, "Time to open control socket: %s sec." %
                  (time.time() - time_to_open_control_socket,))

    #Perform one last test of the socket.  This should never fail, but
    # on occasion does...
    fds, unused, unused = select.select([control_socket], [], [], 0.0)
    try:
        if fds:
            ticket = callback.read_tcp_obj(control_socket)

            Trace.log(e_errors.INFO,
                      "Received second ticket in open_control_socket: %s" %
                      (str(ticket),))
    except e_errors.TCP_EXCEPTION:
        raise EncpError(errno.ENOTCONN,
                        "Control socket no longer usable after initalization.",
                        e_errors.TCP_EXCEPTION, ticket)

    return control_socket, address, ticket
    
##############################################################################

def open_data_socket(mover_addr, interface_ip = None):

    time_to_open_data_socket = time.time()
    
    if interface_ip:
	message = "Connecting data socket to mover (%s) with interface %s." \
		  % (mover_addr, interface_ip) + elapsed_string()
    else:
	message = "Connecting data socket to mover (%s)." % \
                  (mover_addr,) + elapsed_string()
    
    Trace.message(INFO_LEVEL, message)
    Trace.log(e_errors.INFO, message)

    try:
        #Create the socket.
        data_path_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    except socket.error, msg:
        raise socket.error, msg

    #Put the socket into non-blocking mode.
    flags = fcntl.fcntl(data_path_socket.fileno(), fcntl.F_GETFL)
    fcntl.fcntl(data_path_socket.fileno(), fcntl.F_SETFL,
                flags | os.O_NONBLOCK)

    try:
	if interface_ip:
	    data_path_socket.bind((interface_ip, 0))
    except socket.error, msg:
        close_descriptors(data_path_socket)
        raise socket.error, msg

    try:
        data_path_socket.connect(mover_addr)
        #error = 0 #MWZ: pychecker questioned this line.  
    except socket.error, msg:
        #We have seen that on IRIX, when the three way handshake is in
        # progress, we get an EISCONN error.
        if hasattr(errno, 'EISCONN') and msg[0] == errno.EISCONN:
            pass
        #The TCP handshake is in progress.
        elif msg[0] == errno.EINPROGRESS:
            pass
        #A real or fatal error has occured.  Handle accordingly.
        else:
            message = "Connecting data socket failed immediatly."
            Trace.log(e_errors.ERROR, message)
            Trace.trace(12, message)
            raise socket.error, msg

    #Check if the socket is open for reading and/or writing.
    r, w, unused = select.select([data_path_socket],[data_path_socket],[],30)

    if r or w:
        #Get the socket error condition...
        rtn = data_path_socket.getsockopt(socket.SOL_SOCKET,
                                          socket.SO_ERROR)
        #error = 0 #MWZ: pychecker questioned this line.  
    #If the select didn't return sockets ready for read or write, then the
    # connection timed out.
    else:
        raise socket.error(errno.ETIMEDOUT, os.strerror(errno.ETIMEDOUT))

    #If the return value is zero then success, otherwise it failed.
    if rtn != 0:
        message = "Connecting data socket failed later."
        Trace.log(e_errors.ERROR, message)
        Trace.trace(12, message)
        raise socket.error(rtn, os.strerror(rtn))

    #Restore flag values to blocking mode.
    fcntl.fcntl(data_path_socket.fileno(), fcntl.F_SETFL, flags)

    sockname = data_path_socket.getsockname() #This might raise socket.error.
    peername = data_path_socket.getpeername()
    message = "Data socket %s is connected to %s." % \
              (sockname, peername) + elapsed_string()
    Trace.message(INFO_LEVEL, message)
    Trace.log(e_errors.INFO, message)

    try:
	data_path_socket.setsockopt(socket.IPPROTO_IP, socket.IP_TOS,
				    socket.IPTOS_THROUGHPUT)
    except socket.error, msg:
	sys.stderr.write("Socket error setting IPTOS_THROUGHPUT option: %s\n" %
                         str(msg))

    Trace.message(TIME_LEVEL, "Time to open data socket: %s sec." %
                  (time.time() - time_to_open_data_socket,))

    return data_path_socket

############################################################################

#Initiats the handshake with the mover.
#Args:
# listen_socket - The listen socket returned from a callback.get_callback().
# work_tickets - List of dictionaries, where each dictionary represents
#                a transfer.
# mover_timeout - number of seconds to wait for mover to make connection.
# max_retry - the allowable number of times to retry a specific request
#             before giving up.
# verbose - the verbose level of output.
#Returns:
# (control_socket, 

def mover_handshake(listen_socket, work_tickets, encp_intf):
    use_listen_socket = listen_socket
    ticket = {}
    #config = host_config.get_config()
    unique_id_list = []
    for work_ticket in work_tickets:
        unique_id_list.append(work_ticket['unique_id'])

    start_time = time.time()
    while time.time() < start_time + encp_intf.resubmit_timeout:
        #Attempt to get the control socket connected with the mover.
        duration = max(start_time + encp_intf.resubmit_timeout - time.time(),0)
        try:
		control_socket, mover_address, ticket = open_control_socket(
		    use_listen_socket, duration)
        except (socket.error, select.error, EncpError), msg:
            exc, msg, tb = sys.exc_info()
            try:
                #3_21_2006: In theory this should never occur due to a
                # fix in EncpError.__init__() to create self.args.
                msg.args[0]
            except IndexError:
                Trace.log(e_errors.INFO,"Covering IndexError from real error.")
                Trace.handle_error(exc, msg, tb)
                Trace.log(e_errors.INFO, "Encp crash about to occur...")
            del tb
            
            if msg.args[0] == errno.EINTR or msg.args[0] == errno.EAGAIN:
                #If a select (or other call) was interupted,
                # this is not an error, but should continue.
                continue
            elif msg.args[0] == errno.ETIMEDOUT:
                # Setting the error to RESUBMITTING is important.  If this is
                # not done, then it would be returned as ETIMEDOUT.
                # ETIMEDOUT is a retriable error; meaning it would retry
                # the request to the LM, but it will fail since the ticket only
                # contains the 'status' field (as set below).  When
                # handle_retries() is called after mover_handshake() by
                # having the error be RESUBMITTING, encp will resubmit all
                # pending requests (instead of failing on retrying one
                # request).
                ticket['status'] = (e_errors.RESUBMITTING, None)
            elif hasattr(msg, "type"):
                ticket['status'] = (msg.type, str(msg))
            else:
                ticket['status'] = (e_errors.NET_ERROR, str(msg))

            #Combine the dictionaries (if possible).
            if getattr(msg, 'ticket', None) != None:
                #Do the initial munge.
                ticket = combine_dict(ticket, msg.ticket)
                #Attempt to match this ticket with the real ticket in the list.
                # If this step wasn't done, then the code fails on the retry.
                if ticket.get("unique_id", None):
                    for i in range(0, len(work_tickets)):
                        if work_tickets[i]['unique_id'] == ticket['unique_id']:
                            #Make ticket and work_ticket[i] reference the same
                            # info.  Otherwise error handling will be
                            # inconsistant.
                            work_tickets[i] = combine_dict(ticket,
                                                           work_tickets[i])
                            ticket = work_tickets[i]
                            break #Success, control socket opened!
            
            #Since an error occured, just return it.
            return None, None, ticket

        Trace.message(TICKET_LEVEL, "MOVER HANDSHAKE (CONTROL)")
        Trace.message(TICKET_LEVEL, pprint.pformat(ticket))
        #Recored the receiving of the first control socket message.
        message = "Received callback ticket from mover %s for transfer %s." % \
                  (ticket.get('mover', {}).get('name', "Unknown"),
                   ticket.get('unique_id', "Unknown")) + elapsed_string()
        Trace.message(INFO_LEVEL, message)
        Trace.log(e_errors.INFO, message)
 
        #verify that the id is one that we are excpeting and not one that got
        # lost in the ether.
        for i in range(0, len(work_tickets)):
            if work_tickets[i]['unique_id'] == ticket['unique_id']:
                #Make ticket and work_ticket[i] reference the same info.
                # Otherwise error handling will be inconsistant.
                work_tickets[i] = combine_dict(ticket, work_tickets[i])
                ticket = work_tickets[i]
                break #Success, control socket opened!
            
        else: #Didn't find matching id.
            close_descriptors(control_socket)

            list_of_ids = []
            for j in range(0, len(work_tickets)):
                list_of_ids.append(work_tickets[j]['unique_id'])

            Trace.log(e_errors.INFO,
                      "mover handshake: mover impostor called back"
                      "   mover address: %s   got id: %s   expected: %s"
                      "   ticket=%s" %
                      (mover_address,ticket['unique_id'], list_of_ids, ticket))
            
            continue

        # ok, we've been called back with a matched id - how's the status?
        if not e_errors.is_ok(ticket['status']):
            return None, None, ticket

        try:
            mover_addr = ticket['mover']['callback_addr']
        except KeyError:
            msg = sys.exc_info()[1]
            sys.stderr.write("Sub ticket 'mover' not found.\n")
            sys.stderr.write("%s: %s\n" % (e_errors.KEYERROR, str(msg)))
            sys.stderr.write(pprint.pformat(ticket)+"\n")
            if e_errors.is_ok(ticket.get('status', (None, None))):
                ticket['status'] = (e_errors.KEYERROR, str(msg))
            return None, None, ticket

	try:
            #There is no need to do this on a non-multihomed machine.
	    config = host_config.get_config()
            if config and config.get('interface', None):
		local_intf_ip = open_routing_socket(mover_addr[0], encp_intf)
	    else:
		local_intf_ip = work_ticket['callback_addr'][0]
        except (EncpError,), msg:
	    ticket['status'] = (e_errors.EPROTO, str(msg))
	    return None, None, ticket

        #Attempt to get the data socket connected with the mover.
        try:
            Trace.log(e_errors.INFO,
                      "Atempting to connect data socket to mover at %s." \
                      % (mover_addr,))
            data_path_socket = open_data_socket(mover_addr, local_intf_ip)

            if not data_path_socket:
                raise socket.error,(errno.ENOTCONN,os.strerror(errno.ENOTCONN))

        except (socket.error,), msg:
            #exc, msg = sys.exc_info()[:2]
            ticket['status'] = (e_errors.NET_ERROR, str(msg))
            #Trace.log(e_errors.INFO, str(msg))
            close_descriptors(control_socket)
            #Since an error occured, just return it.
            return None, None, ticket

        #We need to specifiy which interface was used on the encp side.
        #ticket['encp_ip'] =  use_listen_socket.getsockname()[0]
	#ticket['encp_ip'] = local_intf_ip
	ticket['encp_ip'] = data_path_socket.getsockname()[0]
        #If we got here then the status is OK.
        ticket['status'] = (e_errors.OK, None)
        #Include new info from mover.
        work_tickets[i] = ticket

        #The following three lines are usefull for testing error handling.
        #control_socket.close()
        #data_path_socket.close()
        #ticket['status'] = (e_errors.NET_ERROR, "because I closed them")

        return control_socket, data_path_socket, ticket

    ticket['status'] = (e_errors.TIMEDOUT, "Mover did not call back.")
    return None, None, ticket
    
############################################################################
############################################################################

def submit_one_request(ticket, encp_intf):

    submit_one_request_start_time = time.time()

    #Before resending, there are some fields that the library
    # manager and mover don't expect to receive from encp,
    # these should be removed.
    item_remove_list = ['mover']
    for item in (item_remove_list):
        try:
            del ticket[item]
        except KeyError:
            pass

    #These two lines of code are for get retries to work properly.
    if ticket.get('method', None) != None:
        ticket['method'] = "read_tape_start"
    
    ##start of resubmit block

    #On a retry or resubmit, print the number of the retry.
    retries = ticket.get('resend', {}).get('retry', 0)
    if retries:
        Trace.message(TO_GO_LEVEL, "RETRY COUNT:" + str(retries))
    resubmits = ticket.get('resend', {}).get('resubmits', 0)
    if resubmits:
        Trace.message(TO_GO_LEVEL, "RESUBMITS COUNT:"+str(resubmits))

    #We need to recheck the file family width.
    if is_write(encp_intf) and (retries or resubmits):
        #First check if the user specified the value on the command line.
        # If so, skip getting a new value.
        if not encp_intf.output_file_family_width:
            try:
                dname = get_directory_name(ticket['wrapper']['pnfsFilename'])
                if encp_intf.outtype == RHSMFILE:
                    t = get_pac()
                else:
                    t = pnfs.Tag()
                file_family_width = t.get_file_family_width(dname)
                ticket['vc']['file_family_width'] = file_family_width
            except (OSError, IOError), msg:
                if msg.args[0] in [errno.ENOENT]:
                    ticket['status'] = (e_errors.USERERROR, str(msg))
                else:
                    ticket['status'] = (e_errors.PNFS_ERROR, str(msg))
                return ticket

    #Determine the type of transfer.
    try:
        if is_write(ticket):
            transfer_type = "write"
            filename = ticket['outfile']
        else:
            transfer_type = "read"
            filename = ticket['infile']

        if filename == "":
            filename = "unknown filename"
    except EncpError, msg:
        transfer_type = "unknown"
        filename = "unknown filename"
        Trace.log(e_errors.ERROR,
                  "Failed to determine the type of transfer: %s" % str(ticket))

    #Put in the log file a message connecting filenames to unique_ids.
    msg = "Sending %s %s request to LM: uninque_id: %s inputfile: %s " \
          "outputfile: %s" \
          % (filename, transfer_type, ticket['unique_id'],
             ticket['infile'], ticket['outfile'])
    Trace.log(e_errors.INFO, msg)
    #Tell the user what the current state of the transfer is.
    Trace.message(TRANSFER_LEVEL,
                  "Submitting %s %s request to LM.%s" % \
                  (filename, transfer_type, elapsed_string()))
    Trace.message(TICKET_LEVEL, "SUBMITTING TICKET: ")
    Trace.message(TICKET_LEVEL, pprint.pformat(ticket))

    #Send work ticket to LM.  As long as a single encp process is restricted
    # to working with one enstore system, not passing get_csc() the ticket
    # as parameter will not cause a problem.
    csc = get_csc()   #ticket)
    #Get the library manager info information.  This also forces an update
    # if the cached configuration information is old.
    library = ticket['vc']['library'] + ".library_manager"
    try:
        lmc = library_manager_client.LibraryManagerClient(
            csc, library, logc = __logc, alarmc = __alarmc,
            rcv_timeout = 5, rcv_tries = 20)
    except SystemExit:
        #On error the library manager client calls sys.exit().  This
        # should catch that so we can handle it.
        ticket['status'] = (e_errors.USERERROR,
              "Unable to locate %s.library_manager." % ticket['vc']['library'])
        return ticket

    #If the lmc is not in a valid state, return an error.
    if lmc.server_address == None:
        ticket['status'] = (e_errors.USERERROR,
              "Unable to locate %s.library_manager." % ticket['vc']['library'])
        return ticket

    if is_read(ticket):
        response_ticket = lmc.read_from_hsm(ticket)
    else:
        response_ticket = lmc.write_to_hsm(ticket)

    if not e_errors.is_ok(response_ticket['status']):
        #If the error is that the tape is NOACCESS or NOTALLOWED then we
        # should handle setting the status to be a little more useful.
        # If we don't do this then we won't see the full test in the
        # encpHistory web page.  The text for the message is used elsewhere
        # in encp.py.
        if not response_ticket['status'][1]:
            if response_ticket['status'][0] == e_errors.NOACCESS or \
                   response_ticket['status'][0] == e_errors.NOTALLOWED:
                response_ticket['status'] = (response_ticket['status'][0],
                                             "Volume %s is marked %s." %
                                             (ticket['vc']['external_label'],
                                              response_ticket['status'][0]))
        
        if e_errors.is_non_retriable(response_ticket['status']):
            Trace.log(e_errors.ERROR,
                      "submit_one_request: Ticket submit failed for %s: %s" %
                      (ticket['infile'], response_ticket['status']))
            Trace.message(ERROR_LEVEL, "Submission to LM failed: " \
                          + str(response_ticket['status']))
        else:
            Trace.log(e_errors.ERROR,
                      "submit_one_request: Ticket submit failed for %s"
                      " - retrying" % ticket['infile'])
            Trace.message(ERROR_LEVEL, "Submission to LM failed: - retrying" \
                          + str(response_ticket['status']))

        #If the ticket was malformed, then we want to see what was sent
        # to the LM.
        if response_ticket['status'][0] == e_errors.MALFORMED:
            Trace.log(e_errors.ERROR,
                      "submit_one_request: %s: %s" % (e_errors.MALFORMED,
                                                      str(ticket)))
            
    Trace.message(TIME_LEVEL, "Time to submit one request: %s sec." %
                  (time.time() - submit_one_request_start_time,))

    #It will most likely be a while, so this would be a good time to
    # perform this maintenance.
    collect_garbage()
    
    return response_ticket

############################################################################

#mode should only contain two values, "read", "write".
#def open_local_file(filename, e):
def open_local_file(work_ticket, e):
    
    open_local_file_start_time = time.time()
    
    if is_write(e):
        flags = os.O_RDONLY
    else: #reads
        #Setting the local fd to read/write access on reads from enstore
        # (writes to local file) should be okay.  The file is initially
        # created with 0644 permissions and is not set to original file
        # permissions until after everything else is set.  The read
        # permissions might be needed later (i.e. --ecrc).
        flags = os.O_RDWR

    filename = work_ticket['wrapper']['fullname']

    #Try to open the local file for read/write.
    try:
        local_fd = os.open(filename, flags)
    except OSError, detail:
        if getattr(detail, "errno", None) in \
               [errno.EACCES, errno.EFBIG, errno.ENOENT, errno.EPERM]:
            done_ticket = {'status':(e_errors.FILE_MODIFIED, str(detail))}
        else:
            done_ticket = {'status':(e_errors.OSERROR, str(detail))}
        return done_ticket

    #Try to grab the os stats of the file.
    try:
        stats = os.fstat(local_fd)
    except OSError, detail:
        if getattr(detail, "errno", None) in \
               [errno.EACCES, errno.EFBIG, errno.ENOENT, errno.EPERM]:
            done_ticket = {'status':(e_errors.FILE_MODIFIED, str(detail))}
        else:
            done_ticket = {'status':(e_errors.OSERROR, str(detail))}
        return done_ticket

    if filename in ["/dev/zero", "/dev/random", "/dev/urandom"]:
        #Handle these character devices a little differently.
        current_size = work_ticket['file_size']
    else:
        current_size = stats[stat.ST_SIZE]

    #Compare the file sizes.
    if is_read(e) and current_size != 0L:
        #When reading from enstore the local file being written to should
        # be zero at this point; even if this is a retry the file should have
        # been clobbered thereby setting the size back to zero.
        done_ticket = {'status':(e_errors.FILE_MODIFIED,
                                 "Local file size has changed.")}
        return done_ticket
    if is_write(e) and current_size != work_ticket['file_size']:
        #When writing to enstore the local file being read from should
        # be the real file size.
        done_ticket = {'status':(e_errors.FILE_MODIFIED,
                                 "Local file size has changed.")}
        return done_ticket

    #Attempt to catch changes to the local file made externally to the
    # current encp process.
    if work_ticket.get("local_inode", 0) != 0:
        if stats[stat.ST_INO] != work_ticket['local_inode']:
            done_ticket = {'status':(e_errors.FILE_MODIFIED,
                                     "Local file inode has changed.")}
            return done_ticket

    done_ticket = {'status':(e_errors.OK, None), 'fd':local_fd}

    #Record this.
    Trace.message(TIME_LEVEL, "Time to open local file: %s sec." %
                  (time.time() - open_local_file_start_time,))
    
    return done_ticket

############################################################################

def receive_final_dialog(control_socket):

    receive_final_dialog_start_time = time.time()

    # File has been transfered - wait for final dialog with mover.
    Trace.message(TRANSFER_LEVEL,
                  "Waiting for final mover dialog." + elapsed_string())

    #import resource
    #print "GETRUSAGE:", resource.getrusage(resource.RUSAGE_SELF)
    #print "TIMES:", os.times()
    #print "ELAPSED:", elapsed_string()
    
    # File has been sent - wait for final dialog with mover. 
    # We know the file has hit some sort of media.... 
    
    try:
        done_ticket = callback.read_tcp_obj(control_socket)

        #Output the info.
        if done_ticket.has_key("method"): #get
            message = "Received final dialog (1)." + elapsed_string()
            Trace.log(e_errors.INFO, message)
            Trace.message(TRANSFER_LEVEL, message)
        else: #encp
            message = "Received final dialog for %s." % \
                     done_ticket.get('unique_id', "Unknown") + elapsed_string()
            Trace.log(e_errors.INFO, message)
            Trace.message(TRANSFER_LEVEL, message)
        #Output these two regardless of get or encp.
        Trace.message(TICKET_LEVEL, "FINAL DIALOG:")
        Trace.message(TICKET_LEVEL, pprint.pformat(done_ticket))
    except e_errors.TCP_EXCEPTION, msg:
        done_ticket = {'status':(e_errors.TCP_EXCEPTION, str(msg))}

    Trace.message(TIME_LEVEL, "Time to receive final dialog: %s sec." %
                  (time.time() - receive_final_dialog_start_time,))
    
    return done_ticket
        
############################################################################

#Returns two-tuple.  First is dictionary with 'status' element.  The next
# is an integer of the crc value.  On error returns 0.
#def transfer_file(input_fd, output_fd, contropl_socket, request, tinfo, e):
def transfer_file(input_file_obj, output_file_obj, control_socket,
                  request, tinfo, e):

    transfer_start_time = time.time() # Start time of file transfer.

    Trace.message(TRANSFER_LEVEL, "Starting %s transfer.  elapsed=%s" %
                  (request['infile'],
                  time.time() - tinfo['encp_start_time'],))

    #Read/Write in/out the data to/from the mover and write/read it out to
    # file.  Also, total up the crc value for comparison with what was
    # sent from the mover.
    try:
        if e.chk_crc != 0:
            crc_flag = 1
        else:
            crc_flag = 0

	################################################
	"""
	mover_ip = request['mover']['callback_addr'][0]

	#Determine if reading or writing.  This only has importance on
	# mulithomed machines were an interface needs to be choosen based
	# on reading and writing usages/rates of the interfaces.
	if getattr(e, "output", "") == "hsmfile":
	    mode = 1 #write
	else:
	    mode = 0 #read
        #Force a reload of the enstore.conf file.  This updates the global
	# cached version of the enstore.conf file information.
	host_config.update_cached_config()
        #set up any special network load-balancing voodoo
	interface=host_config.check_load_balance(mode=mode)
        #load balencing...
	if interface:
	    ip = interface.get('ip')
	    if ip and mover_ip:   #route_ticket.get('mover_ip', None):

		try:
                    #This is were the interface selection magic occurs.
		    host_config.update_route(mover_ip, ip)
		except (OSError, IOError, socket.error), msg:
		    print "SOCKET ERROR:", str(msg)
		    Trace.log(e_errors.ERROR, str(msg))
		    #raise EncpError(getattr(msg,"errno",None),
		    #		    str(msg), e_errors.OSERROR)
        """
	#####################################################

        if hasattr(input_file_obj, "fileno"):
            input_fd = input_file_obj.fileno()
        else:
            input_fd = input_file_obj
        if hasattr(output_file_obj, "fileno"):
            output_fd = output_file_obj.fileno()
        else:
            output_fd = output_file_obj

	EXfer_rtn = EXfer.fd_xfer(input_fd, output_fd, request['file_size'],
                                  crc_flag, e.mover_timeout,
				  e.buffer_size, e.array_size, e.mmap_size,
				  e.direct_io, e.mmap_io, e.threaded_exfer, 0)

        #Exfer_rtn is a tuple.
        # [0] exit_status (1 or 0)
	# [1] crc
        #The following should never be needed.
	# [2] bytes_left_untransfered (should be zero)
        # [3] errno
        # [4] msg
        #The read time and write time are only useful if EXfer is threaded.
        # [5] read time
        # [6] write time
        #More error information.
        # [7] filename,
        # [8] line number
        #encp_crc = EXfer_rtn[1]
        EXfer_ticket = {'status':(e_errors.OK, None)}
        EXfer_ticket['encp_crc'] = EXfer_rtn[1]
        EXfer_ticket['bytes_transfered'] = request['file_size'] - EXfer_rtn[2]
        EXfer_ticket['bytes_not_transfered'] = EXfer_rtn[2]
        EXfer_ticket['read_time'] = EXfer_rtn[5]
        EXfer_ticket['write_time'] = EXfer_rtn[6]
    except EXfer.error, msg:
        #The exception raised can have two forms.  Both share the same values
        # for the first four positions in the msg.args tuple.
        # [0] text message
        # [1] errno
        # [2] strerror
        # [3[ pid
        #It is also possible to have the following extra elements:
        # [4] bytes left untransfered (should be non-zero)
        # [5] read time
        # [6] write time
        # [7] filename of error
        # [8] line number that the error occured on
        if msg.args[1] == errno.ENOSPC \
               or msg.args[1] == errno.EDQUOT: #These should be non-retriable.
            error_type = e_errors.NOSPACE
        elif msg.args[1] == errno.EBUSY: #This should be non-retriable.
            error_type = e_errors.DEVICE_ERROR
        else:
            error_type = e_errors.IOERROR
            
        EXfer_ticket = {'status': (error_type,
                         "[ Error %d ] %s: %s" % (msg.args[1], msg.args[2],
                                                  msg.args[0]))}
        #If this is the longer form, add these values to the ticket.
        if len(msg.args) >= 7:
            EXfer_ticket['bytes_transfered'] = request['file_size'] - \
                                               msg.args[4]
            EXfer_ticket['bytes_not_transfered'] = msg.args[4]
            EXfer_ticket['read_time'] = msg.args[5]
            EXfer_ticket['write_time'] = msg.args[6]
            EXfer_ticket['filename'] = msg.args[7]
            EXfer_ticket['line_number'] = msg.args[8]

        #This should only be done here on the data socket.  Otherwise,
        # the mover may continue to wait on the data socket while encp
        # is waiting on the control socket.
        if is_read(e):
            close_descriptors(input_file_obj)
        else:
            close_descriptors(output_file_obj)

        Trace.log(e_errors.WARNING, "EXfer file transfer error: %s" %
                  (str(msg),))
        Trace.message(TRANSFER_LEVEL,
                     "EXfer file transfer error: [Error %s] %s: %s  elapsed=%s"
                      % (msg.args[1], msg.args[2], msg.args[0],
                         time.time() - tinfo['encp_start_time'],))

    transfer_stop_time = time.time()

    if e_errors.is_ok(EXfer_ticket):
        # Print a sucess message.
        Trace.message(TRANSFER_LEVEL, "File %s transfered.  elapsed=%s" %
                      (request['infile'],
                       time.time()-tinfo['encp_start_time']))

        Trace.log(e_errors.INFO, "File %s transfered for %s in %s seconds." %
                  (request['infile'], request['unique_id'],
                   time.time() - transfer_start_time))

        # Print an additional timming value.
        Trace.message(TIME_LEVEL, "Time to transfer file: %s sec." %
                      (transfer_stop_time - transfer_start_time,))

    else:
        Trace.log(e_errors.WARNING,
                  "File %s transfer attempt done for %s after %s seconds." %
                  (request['infile'], request['unique_id'],
                   time.time() - transfer_start_time))

    #Even though the functionality is there for this to be done in
    # handle requests, this should be received outside since there must
    # be one... not only receiving one on error.
    done_ticket = receive_final_dialog(control_socket)

    #Are these necessary???  Yes.  For these two conditions, the error
    # is very much local.  Don't blame the mover.

    if EXfer_ticket['status'][0] == e_errors.NOSPACE:
        done_ticket = {'status':(e_errors.OK, None)}
    elif EXfer_ticket['status'][0] == e_errors.DEVICE_ERROR:
        done_ticket = {'status':(e_errors.OK, None)}

    if not e_errors.is_retriable(done_ticket) and \
       not e_errors.is_ok(done_ticket):
        rtn_ticket = combine_dict(done_ticket, {'exfer':EXfer_ticket}, request)
    elif not e_errors.is_retriable(EXfer_ticket) and \
         not e_errors.is_ok(EXfer_ticket):
        rtn_ticket = combine_dict({'exfer':EXfer_ticket}, done_ticket, request)
        rtn_ticket['status'] = EXfer_ticket['status'] #Set this seperately
    elif not e_errors.is_ok(done_ticket['status']):
        rtn_ticket = combine_dict(done_ticket, {'exfer':EXfer_ticket}, request)
    elif not e_errors.is_ok(EXfer_ticket['status']):
        rtn_ticket = combine_dict({'exfer':EXfer_ticket}, done_ticket, request)
        rtn_ticket['status'] = EXfer_ticket['status'] #Set this seperately
    else:
        #If we get here then the transfer was a success.
        rtn_ticket = combine_dict({'exfer':EXfer_ticket}, done_ticket, request)

    return rtn_ticket

############################################################################

def check_crc(done_ticket, encp_intf, fd=None):

    check_crc_start_time = time.time()

    #Make these more accessable.
    mover_crc = done_ticket['fc'].get('complete_crc', None)
    encp_crc = done_ticket['exfer'].get('encp_crc', None)

    Trace.log(e_errors.INFO,
              "Mover CRC: %s  ENCP CRC: %s" % (mover_crc, encp_crc))
    
    #Check this just to be safe.
    if mover_crc == None:
        msg =   "warning: mover did not return CRC; skipping CRC check\n"
        sys.stderr.write(msg)
        #done_ticket['status'] = (e_errors.NO_CRC_RETURNED, msg)
        return
    if encp_intf.chk_crc and encp_crc == None:
        msg =   "warning: encp failed to calculate CRC; skipping CRC check\n"
        sys.stderr.write(msg)
        #done_ticket['status'] = (e_errors.NO_CRC_RETURNED, msg)
        return

    # Check the CRC
    if encp_intf.chk_crc:
        if mover_crc != encp_crc:
            msg = "CRC mismatch: %d mover != %d encp" % (mover_crc, encp_crc)
            done_ticket['status'] = (e_errors.CRC_ENCP_ERROR, msg)
            return

    #If the user wants a crc readback check of the new output file (reads
    # only) calculate it and compare.
    if encp_intf.ecrc:
        #If passed a file descriptor, make sure it is to a regular file.
        if fd and (type(fd) == types.IntType) and \
           stat.S_ISREG(os.fstat(fd)[stat.ST_MODE]):
            try:
                readback_crc = EXfer.ecrc(fd)
            except EXfer.error, msg:
                done_ticket['status'] = (e_errors.CRC_ECRC_ERROR, str(msg))
                return

            #Put the ecrc value into the ticket.
            done_ticket['ecrc'] = readback_crc

            #If we have a valid crc value returned, compare it.
            if readback_crc != mover_crc:
                msg = "CRC readback mismatch: %d mover != %d encp" % \
                      (mover_crc, readback_crc)
                done_ticket['status'] = (e_errors.CRC_ECRC_ERROR, msg)
                return

    # Check the CRC in pnfs layer 2 (set by dcache).
    if encp_intf.chk_crc:
        try:
            # Get the pnfs layer 2 for this file.
            p = Pnfs(done_ticket['wrapper']['pnfsFilename'])
            data = p.readlayer(2, done_ticket['wrapper']['pnfsFilename'])
        except (IOError, OSError, TypeError, AttributeError):
            #There is no layer 2 to check.  Skip the rest of the check.
            #If there are ever any later checks added, this return is bad.
            #return
            data = []
    
        # Define the match/search once before the loop.
        crc_match = re.compile("c=[1-9]+:[a-zA-Z0-9]{8}")

        # Loop over every line in the output looking for the crc.
        for line in data:
            result = crc_match.search(line)
            if result != None:
                #First convert the 0 seeded adler32 crc used by enstore
                # to the 1 seeded adler32 crc used by dcache.
                fixed_encp_crc = convert_0_adler32_to_1_adler32(encp_crc,
                                                     done_ticket['file_size'])
                #Get the hex strings of the two CRCs.
                hex_dcache_string = "0x" + result.group().split(":")[1]
                hex_encp_string = hex(fixed_encp_crc)
                #Convert to long integers for safety.  (padding of zero's)
                dcache_crc = long(hex_dcache_string, 16)
                encp_crc = long(hex_encp_string, 16)
                #Test to make sure they are the same.
                if dcache_crc != encp_crc:
                    msg = "CRC dcache mismatch: %s (%s) != %s (%s)" % \
                          (dcache_crc, hex(dcache_crc),
                           encp_crc, hex(encp_crc))
                    done_ticket['status'] = (e_errors.CRC_DCACHE_ERROR, msg)
                    return

    Trace.message(TIME_LEVEL, "Time to check CRC: %s sec." %
                  (time.time() - check_crc_start_time,))

############################################################################

#Double check that all of the filesizes agree.  As a performance bonus,
# double check the inodes since the performace pentalty of the stat()s has
# already occured.
def verify_file_size(ticket, encp_intf = None):

    verify_file_size_start_time = time.time()
    
    #Don't worry about checking when outfile is /dev/null.
    if ticket['outfile'] in ['/dev/null', "/dev/zero",
                             "/dev/random", "/dev/urandom"]:
        return

    #Get the stat info for each file.
    try:
        full_stat = os.stat(ticket['wrapper'].get('fullname', None))
        full_inode = full_stat[stat.ST_INO]
        if ticket['infile'] in [ "/dev/zero", "/dev/random", "/dev/urandom"]:
            full_filesize = ticket['file_size']
        else:
            full_filesize = full_stat[stat.ST_SIZE]
    except (OSError, IOError), detail:
        ticket['status'] = (e_errors.OSERROR, str(detail))
        return

    try:
        p = Pnfs(ticket['wrapper'].get('pnfsFilename', None))
        pnfs_stat = p.get_stat(ticket['wrapper'].get('pnfsFilename', None))
        pnfs_filesize = pnfs_stat[stat.ST_SIZE]
        pnfs_inode = pnfs_stat[stat.ST_INO]
        
        """
        #Dmitry
        if (os.path.exists(ticket['wrapper'].get('pnfsFilename', None))):
            pnfs_stat = os.stat(ticket['wrapper'].get('pnfsFilename', None))
            pnfs_filesize = pnfs_stat[stat.ST_SIZE]
            pnfs_inode = pnfs_stat[stat.ST_INO]
        else:
            pnfs_filesize = ticket['wrapper']['pstat'][stat.ST_SIZE]
            pnfs_inode = ticket['wrapper']['pstat'][stat.ST_INO]
        """
    except (TypeError), detail:
        ticket['status'] = (e_errors.OK, "No files sizes to verify.")
        return
    except (OSError, IOError), detail:
        if encp_intf and encp_intf.override_deleted:
            #When reading we need to be able to bypass obtaining the value
            # from the pnfs file that has been deleted.
            pnfs_filesize = ticket['fc']['size']
            pnfs_inode = None
        else:
            ticket['status'] = (e_errors.OSERROR, str(detail))
            return

    #try:
    #    in_stat = os.stat(ticket['infile'])
    #    in_filesize = in_stat[stat.ST_SIZE]
    #except (OSError, IOError), detail:
    #    ticket['status'] = (e_errors.OSERROR, str(detail))
    #    return

    #try:
    #    out_stat = os.stat(ticket['outfile'])
    #    out_filesize = out_stat[stat.ST_SIZE]
    #except (OSError, IOError), detail:
    #    ticket['status'] = (e_errors.OSERROR, str(detail))
    #    return

    #Handle making sure the local inode did not change during the transfer.
    local_inode = ticket.get("local_inode", None)
    if local_inode:
        if local_inode != full_inode:
            ticket['status'] = (e_errors.USERERROR,
                                "Local inode changed during transfer.")
            return

    #Handle making sure the pnfs file inode not change during the transfer.
    remote_inode = ticket['wrapper'].get("inode", None)
    if remote_inode and remote_inode != 0:
        if remote_inode != pnfs_inode:
            ticket['status'] = (e_errors.USERERROR,
                                "Pnfs inode changed during transfer.")
            return

    #Handle large files.
    if pnfs_filesize == 1:
        #Until pnfs supports NFS version 3 (for large file support) make
        # sure we are using the correct file_size for the pnfs side.
        try:
            p = Pnfs(ticket['wrapper']['pnfsFilename'])
            pnfs_real_size = p.get_file_size(ticket['wrapper']['pnfsFilename'])
        except (OSError, IOError), detail:
            ticket['status'] = (e_errors.OSERROR, str(detail))
            return

        if full_filesize != pnfs_real_size:
            msg = "Expected local file size (%s) to equal remote file " \
                  "size (%s) for large file %s." \
                  % (full_filesize, pnfs_real_size, ticket['outfile'])
            ticket['status'] = (e_errors.FILE_MODIFIED, msg)
    #Test if the sizes are correct.
    elif ticket['file_size'] != full_filesize:
        msg = "Expected file size (%s) to equal actuall file size " \
              "(%s) for file %s." % \
              (ticket['file_size'], full_filesize, ticket['outfile'])
        ticket['status'] = (e_errors.FILE_MODIFIED, msg)
    elif full_filesize != pnfs_filesize:
        msg = "Expected local file size (%s) to equal remote file " \
              "size (%s) for file %s." \
              % (full_filesize, pnfs_filesize, ticket['outfile'])
        ticket['status'] = (e_errors.FILE_MODIFIED, msg)

    Trace.message(TIME_LEVEL, "Time to verify file size: %s sec." %
                  (time.time() - verify_file_size_start_time,))

def verify_inode(ticket):

    verify_inode_start_time = time.time()
    
    #Don't worry about checking when outfile is /dev/null.
    if ticket['outfile'] == '/dev/null':
        return

    #Get the stat info for each file.
    try:
        full_stat = os.stat(ticket['wrapper'].get('fullname', None))
        full_inode = full_stat[stat.ST_INO]
    except (OSError, IOError), detail:
        ticket['status'] = (e_errors.OSERROR, str(detail))
        return
    
    try:
        p = Pnfs(ticket['wrapper'].get('pnfsFilename', None))
        pnfs_stat = p.get_stat(ticket['wrapper'].get('pnfsFilename', None))
        #pnfs_stat = os.stat(ticket['wrapper'].get('pnfsFilename', None))
        pnfs_inode = pnfs_stat[stat.ST_INO]
    except (TypeError), detail:
        ticket['status'] = (e_errors.OK, "No inodes to verify.")
        return
    except (OSError, IOError), detail:
        ticket['status'] = (e_errors.OSERROR, str(detail))
        return

    #Handle making sure the local inode did not change during the transfer.
    local_inode = ticket.get("local_inode", None)
    if local_inode:
        if local_inode != full_inode:
            ticket['status'] = (e_errors.USERERROR,
                                "Local inode changed during transfer.")
            return

    #Handle making sure the pnfs file inode not change during the transfer.
    remote_inode = ticket['wrapper'].get("inode", None)
    if remote_inode and remote_inode != 0:
        if remote_inode != pnfs_inode:
            ticket['status'] = (e_errors.USERERROR,
                                "Pnfs inode changed during transfer.")
            return

    Trace.message(TIME_LEVEL, "Time to verify inode: %s sec." %
                  (time.time() - verify_inode_start_time,))

############################################################################

def set_outfile_permissions(ticket):

    if not ticket.get('copy', None):  #Don't set permissions if copy.
        set_outfile_permissions_start_time = time.time()

        #Attempt to get the input files permissions and set the output file to
        # match them.
        if ticket['outfile'] not in ["/dev/null", "/dev/zero",
                                     "/dev/random", "/dev/urandom"]:
            try:
                #handle remote file case
                perms = None
                p = Pnfs(ticket['wrapper']['pnfsFilename'])
                if is_write(ticket):
                    perms = os.stat(ticket['infile'])[stat.ST_MODE]
                    p.chmod(perms, ticket['outfile'])
                else:
                    perms = p.get_stat(ticket['infile'])[stat.ST_MODE]
                    os.chmod(ticket['outfile'], perms)
                ticket['status'] = (e_errors.OK, None)
            except OSError, msg:
                Trace.log(e_errors.INFO, "chmod %s failed: %s" % \
                          (ticket['outfile'], msg))
                ticket['status'] = (e_errors.USERERROR,
                                    "Unable to set permissions.")

        Trace.message(TIME_LEVEL, "Time to set_outfile_permissions: %s sec." %
                      (time.time() - set_outfile_permissions_start_time,))
    
############################################################################

#This function prototype looking thing is here so that there is a defined
# handle_retries() before internal_handle_retries() is defined.  While
# python handles this without the pre-definition, mylint.py does not.
#8-23-2004: Modified mylint.py to ignore this error.
#def handle_retries(*args):
#    pass

#This internal version of handle retries should only be called from inside
# of handle_retries().
def internal_handle_retries(request_list, request_dictionary, error_dictionary,
                            encp_intf, listen_socket = None,
                            udp_server = None, control_socket = None):
    #Set the encp_intf to internal test values to two.  This means
    # there is only one check made on internal problems.
    remember_retries = encp_intf.max_retry
    remember_resubmits = encp_intf.max_resubmit
    encp_intf.max_retry = 2
    encp_intf.max_resubmit = 2
    
    internal_result_dict = handle_retries(request_list, request_dictionary,
                                          error_dictionary, encp_intf,
                                          listen_socket = listen_socket,
                                          udp_server = udp_server,
                                          control_socket = control_socket)

    #Set the max resend parameters to original values.
    encp_intf.max_retry = remember_retries
    encp_intf.max_resubmit = remember_resubmits

    return internal_result_dict

def handle_retries(request_list, request_dictionary, error_dictionary,
                   encp_intf, listen_socket = None, udp_server = None,
                   control_socket = None, local_filename = None,
                   external_label = None, pnfs_filename = None):
    #Extract for readability.
    max_retries = encp_intf.max_retry
    max_submits = encp_intf.max_resubmit
    #verbose = encp_intf.verbose
    
    #Before resubmitting, there are some fields that the library
    # manager and mover don't expect to receive from encp,
    # these should be removed.
    #9-30-2003: Removed 'mover' from the list of items to remove.
    # This was done so that "GET" could keep the 'mover' item when
    # using this function.  It should not hurt anything, since
    # movers and/or LMs have removed this field themselves for a
    # while now.
    item_remove_list = [] #['mover']

    #error_dictionary must have 'status':(e_errors.XXX, "explanation").
    dict_status = error_dictionary.get('status', (e_errors.OK, None))

    #These fields need to be retrieved with possible defaults.  If the transfer
    # failed before encp could determine which transfer failed (aka failed
    # opening/reading the/from contol socket) then only the 'status' field
    # of both the request_dictionary and error_dictionary are guarenteed to
    # exist (although some situations will add others).
    infile = request_dictionary.get('infile', '')
    outfile = request_dictionary.get('outfile', '')
    file_size = request_dictionary.get('file_size', 0)
    retry = request_dictionary.get('resend', {}).get('retry', 0)
    resubmits = request_dictionary.get('resend', {}).get('resubmits', 0)

    #Get volume info from the volume clerk.
    #Need to check if the volume has been marked NOACCESS since it
    # was checked last.  This should only apply to reads.
    #if request_dictionary.get('fc', {}).has_key('external_label'):
    if external_label and external_label == \
           request_dictionary.get('fc', {}).get('external_label', None):
        try:
            vc_reply = get_volume_clerk_info(request_dictionary)
            vc_status = vc_reply['status']
        except EncpError, msg:
            vc_status = (getattr(msg, 'type', e_errors.UNKNOWN), str(msg))
    else:
        vc_status = (e_errors.OK, None)

    #If there is a control socket open and there is data to read, then read it.
    socket_status = (e_errors.OK, None)
    socket_dict = {'status':socket_status}
    if control_socket:
        socket_error = control_socket.getsockopt(socket.SOL_SOCKET,
                                                 socket.SO_ERROR)
        if socket_error:
            socket_status = (e_errors.NET_ERROR, os.strerror(socket_error))
            socket_dict = {'status':socket_status}
            request_dictionary = combine_dict(socket_dict, request_dictionary)
        else:
            #This loop handles select being interupted by a signal.
            while 1:
                #Determine if the control socket has some error to report.
                try:
                    read_fd, unused, unused = select.select([control_socket],
                                                            [], [], 0.0)
                    break  #No exception raised; success.
                except select.error, msg:
                    if msg.args[0] == errno.EINTR or \
                           msg.args[0] == errno.EAGAIN:
                        #If the call was interupted, continue.
                        continue
                    else:
                        #If an error occured, deal with it.
                        read_fd = []
                        break
                        
            #Check control socket for error.
            if read_fd:
                #socket_dict = receive_final_dialog(control_socket)
                #socket_status = socket_dict.get('status', (e_errors.OK ,None))
                #request_dictionary = combine_dict(socket_dict,
                #                                  request_dictionary)

                #Don't read the message.  Just determine if the socket is
                # still open.  Reading the message with short files can
                # read the final dialog early and give a false error.
                if len(control_socket.recv(1, socket.MSG_PEEK)) == 0:
                    socket_status = (e_errors.NET_ERROR,
                                     "Control socket error: %s" %
                                     os.strerror(errno.ENOTCONN))
                    socket_dict = {'status':socket_status}
                    Trace.log(e_errors.WARNING,
                              "Control socket status: %s" % (socket_status,))

    #Just to be paranoid check the listening socket.  Check the current
    # socket status to avoid wiping out an error.
    if e_errors.is_ok(socket_dict) and listen_socket:
        socket_error = listen_socket.getsockopt(socket.SOL_SOCKET,
                                                socket.SO_ERROR)
        if socket_error:
            socket_status = (e_errors.NET_ERROR, os.strerror(socket_error))
            socket_dict = {'status':socket_status}
            request_dictionary = combine_dict(socket_dict, request_dictionary)

    lf_status = (e_errors.OK, None)
    if local_filename:
        try:
            #First determine if stat-ing the file produces any errors.
            stats = os.stat(local_filename)

            #Check to make sure that the inode has not changed either.
            # Note: This may not catch every user causing problems.  It is
            # possible to delete a file, then recreate it (quickly) and
            # the new version may have the same inode as the original.
            original_inode = request_dictionary.get('local_inode', None)
            current_inode = stats[stat.ST_INO]
            if original_inode != None and \
                   long(original_inode) != long(current_inode):
                lf_status = (e_errors.FILE_MODIFIED,
                             "Noticed the local file inode changed from "
                             "%s to %s for file %s."
                             % (original_inode, current_inode,
                                local_filename))

            #Check to make sure that the size has not changed too.
            if is_read(request_dictionary): #read
                original_size = 0
                current_size = stats[stat.ST_SIZE]
            else:  #write
                original_size = request_dictionary.get('file_size', None)
                if infile in ["/dev/zero", "/dev/random", "/dev/urandom"]:
                    #With these special files, do with the original
                    current_size = original_size
                else:
                    current_size = stats[stat.ST_SIZE]
            if long(current_size) != long(original_size):
                lf_status = (e_errors.FILE_MODIFIED,
                             "Noticed the local file size changed from "
                             "%s to %s for file %s."
                             % (original_size, current_size,
                                local_filename))

        except OSError, msg:
            #These four error are likely due to an outside process/user.
            if getattr(msg, "errno", None) in [errno.EACCES, errno.ENOENT,
                                               errno.EFBIG, errno.EPERM]:
                #Local File status.
                lf_status = (e_errors.FILE_MODIFIED,
                             "Noticed the local file changed: " + str(msg))
            else:
                #Local File status.
                lf_status = (e_errors.OSERROR,
                             "Noticed error checking local file:" + str(msg))

    pf_status = (e_errors.OK, None)
    skip_layer_cleanup = False
    if is_write(encp_intf) and type(pnfs_filename) == types.StringType \
           and pnfs_filename and not request_dictionary.get('copy', None):
        #If the user wants us to specifically check if another encp has
        # written (layers 1 or 4) to this pnfs file; now is the time to check.
        try:
            if do_layers_exist(pnfs_filename):
                #The do_layers_exist() function should never give an
                # exist error for the 'real' file.  Any EEXIST errors
                # from this section should only occur if layer 1 or 4 is
                # already set.
                raise EncpError(errno.EEXIST,
                                "Layer 1 and layer 4 are already set.",
                                e_errors.FILE_MODIFIED,
                                request_dictionary)
        except (OSError, IOError), msg:
            pf_status = (e_errors.PNFS_ERROR, str(msg))
        except EncpError, msg:
            pf_status = (msg.type, str(msg))
            if getattr(msg, "errno", None) == errno.EEXIST:
                #We don't want to detete the file that has been written
                # by another encp.  In theory this can only happen with
                # dCache encp transfers.  Normal encps should be immune
                # do to the way that the output is created via the temporary
                # filename linking game (However, make sure that this
                # doesn't delete the entry regardless).
                try:
                    delete_at_exit.unregister(pnfs_filename)
                except ValueError:
                    #The name is already not in the list of files to remove.
                    pass
                #We don't want to do this because this will clobber what
                # the previous encp has already set.
                skip_layer_cleanup = True

        #Add this for debugging.
        Trace.log(e_errors.INFO,
                  "pf_status = %s  skip_layer_cleanup = %s" %
                  (pf_status, skip_layer_cleanup))
                                
    #The volume clerk set the volume NOACCESS.
    if not e_errors.is_ok(vc_status):
        status = vc_status
    #Set status if there was an error recieved from control socket.
    elif not e_errors.is_ok(socket_status):
        status = socket_status
    #Set the status if the local file could not be stat-ed.
    elif not e_errors.is_ok(lf_status):
        status = lf_status
    #Set the status if the pnfs file had layers already set.
    elif not e_errors.is_ok(pf_status):
        status = pf_status
    #Use the ticket status.
    else:
        status = dict_status
        
    #If there is no error, then don't do anything
    if e_errors.is_ok(status):
        result_dict = {'status':(e_errors.OK, None), 'retry':retry,
                       'resubmits':resubmits,
                       'queue_size':len(request_list)}
        result_dict = combine_dict(result_dict, socket_dict)
        return result_dict
    #At this point it is known there is an error.  If the transfer is a read,
    # then if the encp is killed before completing delete_at_exit.quit() could
    # leave non-zero non-correct files.  If this is the case truncate them.
    elif is_read(encp_intf):
        try:
            fd = os.open(outfile, os.O_WRONLY | os.O_TRUNC)
            os.close(fd)
        except (IOError, OSError):
            #Something is very wrong, deal with it later.
            pass
    #If the transfer is a write from dcache, we need to clear any information
    # that resides in layer 1 and/or layer 4.
    """
    elif is_write(encp_intf) and encp_intf.put_cache and not encp_intf.copies:
        #If another encp set layer 1 and/or 4 while this encp was waiting
        # in the queue, the layer test above will set skip_layer_cleanup
        # to true and thus at this point be skipping the deletion of the
        # metadata set by the other encp process.
        if not skip_layer_cleanup:
            try:
                Trace.log(e_errors.INFO,
                          "Clearing layers 1 and 4 for file %s (%s): %s" %
                          (outfile, request_dictionary.get('unique_id', None),
                           str(pf_status)))
                p = Pnfs(outfile)
                p.writelayer(1, "", outfile)
                p.writelayer(4, "", outfile)
            except (IOError, OSError):
                #Something is very wrong, deal with it later.
                pass
    """

    #If the mover doesn't call back after max_submits number of times, give up.
    # If the error is already non-retriable, skip this step.
    if max_submits != None and resubmits >= max_submits \
       and status[0] == e_errors.RESUBMITTING:
        Trace.message(ERROR_LEVEL,
                      "To many resubmissions for %s -> %s."%(infile,outfile))
        status = (e_errors.TOO_MANY_RESUBMITS, status)

    #If the transfer has failed too many times, remove it from the queue.
    # Since TOO_MANY_RETRIES is non-retriable, set this here.
    # If the error is already non-retriable, skip this step.
    if max_retries != None and retry >= max_retries \
       and e_errors.is_retriable(status) \
       and status[0] != e_errors.RESUBMITTING:
        Trace.message(ERROR_LEVEL,
                      "To many retries for %s -> %s."%(infile,outfile))
        status = (e_errors.TOO_MANY_RETRIES, status)

    #If we can an error (for example TCP_EXCEPTION), that does not result
    # in request_dictionary containing a request, we need to handle this
    # situation like the RESUBMITTING case to make sure all files get
    # Resent to the LM.  More importantly so we avoid a recursive error
    # loop resending an empty request.
    if request_dictionary.get('unique_id', None) == None \
             and e_errors.is_retriable(status) \
             and status[0] != e_errors.RESUBMITTING:
        Trace.message(ERROR_LEVEL,
                      "Treating error like resubmit for %s -> %s."%(infile,outfile))
        status = (e_errors.RESUBMITTING, status)

    #For reads only when a media error occurs.
    retry_non_retriable_media_error = False
    #Make sure that it is a media error first.  If the error is a media
    # error, then the request_dictionary will have a full request
    # dictionary to pass to is_read().  If request_dictionary is not a
    # full request dictionary is_read() will through an EncpError that
    # becomes an 'UNCAUGHT EXCEPTION'.  One know case of a non-full
    # request dictionary is for a (RESUBMITTING, None) 'error'.
    if e_errors.is_media(status) and is_read(request_dictionary):
        #Note: request_dictionary['bfid'] should always carry the
        # original bfid string while (request_dictionary['fc']['bfid']
        # should contain the bfid of the copy (original included) that
        # was just tried.
        
        fcc = get_fcc()
        vcc = get_vcc()

        copy_list_dict = fcc.find_copies(request_dictionary['bfid'])
        if e_errors.is_ok(copy_list_dict):
            copy_list = copy_list_dict['copies']
        else:
            copy_list = []

        while copy_list:
            copy_index = random.randint(0, len(copy_list) - 1)
            next_bfid = copy_list[copy_index]

            fc_info = fcc.bfid_info(next_bfid, 5, 20)
            if e_errors.is_ok(fc_info):
                vc_info = vcc.inquire_vol(fc_info['external_label'], 5, 20)
                if e_errors.is_ok(vc_info):
                    request_dictionary['fc'] = fc_info
                    request_dictionary['vc'] = vc_info
                    retry_non_retriable_media_error = True
                    break

            del copy_list[copy_index]
        
    #If the error is not retriable, remove it from the request queue.  There
    # are two types of non retriable errors.  Those that cause the transfer
    # to be aborted, and those that in addition to abborting the transfer
    # tell the operator that someone needs to investigate what happend.
    #
    #If the error is a media error AND there is another copy of the file
    # then perform the retry for the next copy.
    if e_errors.is_non_retriable(status[0]) and \
           not retry_non_retriable_media_error:
        #Print error to stdout in data_access_layer format. However, only
        # do so if the dictionary is full (aka. the error occured after
        # the control socket was successfully opened).  Control socket
        # errors are printed elsewere (for reads only).
        error_dictionary['status'] = status
        #By checking those that aren't of significant length, we only
        # print these out on writes.
        if len(request_dictionary) > 3:
            print_data_access_layer_format(infile, outfile, file_size,
                                           error_dictionary)

        if e_errors.is_emailable(status[0]):
            Trace.alarm(e_errors.EMAIL, status[0], {
                'infile':infile, 'outfile':outfile, 'status':status[1]})
        elif e_errors.is_alarmable(status[0]):
            Trace.alarm(e_errors.ERROR, status[0], {
                'infile':infile, 'outfile':outfile, 'status':status[1]})

        #try:
        #    #Try to delete the request.  In the event that the connection
        #    # didn't let us determine which request failed, don't worry.
        #    del request_list[request_list.index(request_dictionary)]
        #    queue_size = len(request_list)
        #except (KeyError, ValueError):
        #    queue_size = len(request_list) - 1
        queue_size = 0
        for req in request_list:
            if not req.get('completion_status', None):
                queue_size = queue_size + 1

        #This is needed on reads to send back to the calling function
        # that ths error means that there should be none left in the queue.
        # On writes, if it gets this far it should already be 0 and doesn't
        # effect anything.
        if status[0] == e_errors.TOO_MANY_RESUBMITS:
            queue_size = 0
        
        result_dict = {'status':status, 'retry':retry,
                       'resubmits':resubmits,
                       'queue_size':queue_size}

    #When nothing was recieved from the mover and the 15min has passed,
    # resubmit all entries in the queue.  Leave the unique id the same.
    # Even though for writes there is only one entry in the active request
    # list at a time, submitting like this will still work.
    elif status[0] == e_errors.RESUBMITTING:
        #Since, we are going to continue, we want to receive future event
        # relay NEWCONFIGFILE messages.
        #Note: The duration for receiving event relay messages and waiting
        # for a mover are both 15 minutes.  If the later were to become
        # greater than the former, a potential time window of missed messages
        # could exist.
        #try:
        #    csc = get_csc()
        #    if csc.new_config_obj.is_caching_enabled():
        #        csc.new_config_obj.erc.subscribe()
        #except EncpError:
        #    pass
        
        ###Is the work done here duplicated in the next commented code line???
        # 1-21-2004 MWZ: By testing for a non-empty request_list this code
        # should never get called.  This was duplicating the resubmit
        # increase later on.
        if not request_list:
            request_dictionary['resend']['resubmits'] = resubmits + 1

        #Update the tickets callback fields.  The actual sockets
        # are updated becuase they are passed in by reference.  There
        # are some cases (most notably when internal_handle_retries()
        # is used) that there isn't a socket passed in to change.
        if request_list[0].get('route_selection', None) and udp_server:
            udp_callback_addr = get_udp_callback_addr(
                encp_intf, udp_server)[0] #Ignore the returned socket ref.
        else:
            udp_callback_addr = None

        for req in request_list:
            try:
                #Increase the resubmit count.
                req['resend']['resubmits'] = req.get('resubmits', 0) + 1

                #Before resubmitting, there are some fields that the library
                # manager and mover don't expect to receive from encp,
                # these should be removed.
                for item in (item_remove_list):
                    try:
                        del req[item]
                    except KeyError:
                        pass

                #Update the ticket before sending it to library manager.
                if udp_callback_addr:
                    #The ticket item of 'routing_callback_addr' is a
                    # legacy name.
                    req['routing_callback_addr'] = udp_callback_addr

                #Send this to log file.
                Trace.log(e_errors.WARNING, (e_errors.RESUBMITTING,
                                             req.get('unique_id', None)))

                #Since a retriable error occured, resubmit the ticket.
                lm_response = submit_one_request(req, encp_intf)

            except (KeyboardInterrupt, SystemExit):
                raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
            except:
                exc, msg = sys.exc_info()[:2]
                #sys.stderr.write("%s: %s\n" % (str(exc), str(msg)))
                Trace.log(e_errors.ERROR,
                          "Resubmission error: %s: %s" % (str(exc), str(msg)))

                #I'm not sure what should happen with lm_response here.
                # Currently set it to all clear with the assumption that
                # it will RESUMBIT again in 15 (by default) minutes.  MWZ
                lm_response = {'status' : (e_errors.OK, None)}

            #Now it get checked.  But watch out for the recursion!!!
            internal_result_dict = internal_handle_retries([req], req,
                                                           lm_response,
                                                           encp_intf)

            #If an unrecoverable error occured while resubmitting to LM.
            if e_errors.is_non_retriable(internal_result_dict['status'][0]):
                result_dict = {'status':internal_result_dict['status'],
                               'retry':request_dictionary.get('retry', 0),
                               'resubmits':request_dictionary.get('resubmits',
                                                                  0),
                               'queue_size':0}
                Trace.log(e_errors.ERROR, str(result_dict))
            #If a retriable error occured while resubmitting to LM.
            elif not e_errors.is_ok(internal_result_dict['status'][0]):
                result_dict = {'status':internal_result_dict['status'],
                               'retry':request_dictionary.get('retry', 0),
                               'resubmits':request_dictionary.get('resubmits',
                                                                  0),
                               'queue_size':len(request_list)}
                Trace.log(e_errors.ERROR, str(result_dict))
            #If no error occured while resubmitting to LM.
            else:
                result_dict = {'status':(e_errors.RESUBMITTING,
                                         req.get('unique_id', None)),
                               'retry':request_dictionary.get('retry', 0),
                               'resubmits':request_dictionary.get('resubmits',
                                                                  0),
                               'queue_size':len(request_list)}

    #Change the unique id so the library manager won't remove the retry
    # request when it removes the old one.  Do this only when there was an
    # actuall error, not just a timeout.  Also, increase the retry count by 1.
    else:
        
        #Log the intermidiate error as a warning instead as a full error.
        Trace.log(e_errors.WARNING, "Retriable error: %s" % str(status))

        #Get a new unique id for the transfer request since the last attempt
        # ended in error.
        request_dictionary['unique_id'] = generate_unique_id()

        #Keep retrying this file.
        try:
            #Increase the retry count.
            request_dictionary['resend']['retry'] = retry + 1
            
            #Before resending, there are some fields that the library
            # manager and mover don't expect to receive from encp,
            # these should be removed.
            for item in (item_remove_list):
                try:
                    del request_dictionary[item]
                except KeyError:
                    pass

            #Send this to log file.
            Trace.log(e_errors.WARNING, (e_errors.RETRY,
                                         request_dictionary['unique_id']))
        except KeyError:
            Trace.log(e_errors.ERROR, (e_errors.BROKEN,
                                "Unable to update LM request while retrying."))
            Trace.log(e_errors.ERROR,
                      "request_dictionary: " + str(request_dictionary))
            exc, msg, tb = sys.exc_info()
            Trace.handle_error(exc, msg, tb)
            del tb

        try:
            #Since a retriable error occured, resubmit the ticket.
            lm_response = submit_one_request(request_dictionary, encp_intf)

        except KeyError, msg:
            lm_response = {'status':(e_errors.NET_ERROR,
                            "Unable to obtain response from library manager.")}
            sys.stderr.write("Error processing retry of %s.\n" %
                             (request_dictionary['unique_id']))
            sys.stderr.write(pprint.pformat(request_dictionary)+"\n")
            
        #Now it get checked.  But watch out for the recursion!!!
        internal_result_dict = internal_handle_retries([request_dictionary],
                                                       request_dictionary,
                                                       lm_response, encp_intf)

        
        #If an unrecoverable error occured while retrying to LM.
        if e_errors.is_non_retriable(internal_result_dict['status'][0]):
            result_dict = {'status':internal_result_dict['status'],
                           'retry':request_dictionary.get('retry', 0),
                           'resubmits':request_dictionary.get('resubmits', 0),
                           'queue_size':len(request_list) - 1}
            Trace.log(e_errors.ERROR, str(result_dict))
        elif not e_errors.is_ok(internal_result_dict['status'][0]):
            result_dict = {'status':internal_result_dict['status'],
                           'retry':request_dictionary.get('retry', 0),
                           'resubmits':request_dictionary.get('resubmits', 0),
                           'queue_size':len(request_list)}
            Trace.log(e_errors.ERROR, str(result_dict))            
        else:
            result_dict = {'status':(e_errors.RETRY,
                                     request_dictionary['unique_id']),
                           'retry':request_dictionary.get('retry', 0),
                           'resubmits':request_dictionary.get('resubmits', 0),
                           'queue_size':len(request_list)}
            

    #If we get here, then some type of error occured.  This means one of
    # three things happend.
    #1) The transfer was abborted.
    #    a) A non-retriable error occured once.
    #    b) A retriable error occured too many times.
    #2) The request(s) was/were resubmited.
    #    a) No mover called back before the listen socket timed out.
    #3) The request(s) was/were retried.
    #    a) A retriable error occured.
    return result_dict

############################################################################

def calculate_rate(done_ticket, tinfo):

    calculate_rate_start_time = time.time()
    
    # calculate some kind of rate - time from beginning to wait for
    # mover to respond until now. This doesn't include the overheads
    # before this, so it isn't a correct rate. I'm assuming that the
    # overheads I've neglected are small so the quoted rate is close
    # to the right one.  In any event, I calculate an overall rate at
    # the end of all transfers
    
    Trace.message(TICKET_1_LEVEL, "CALCULATING RATE FROM:")
    Trace.message(TICKET_1_LEVEL, pprint.pformat(done_ticket))
    Trace.message(TICKET_1_LEVEL, pprint.pformat(tinfo))
    
    #calculate MB relatated stats
    bytes_per_MB = float(1024 * 1024)
    MB_transfered = float(done_ticket['file_size']) / bytes_per_MB

    #For readablilty...
    u_id = done_ticket['unique_id']

    #Make these variables easier to use.
    transfer_time = tinfo.get('%s_transfer_time' % (u_id,), 0)
    overall_time = tinfo.get('%s_overall_time' % (u_id,), 0)
    drive_time = done_ticket['times'].get('drive_transfer_time', 0)
    #These are newer more accurate time measurements and may not always
    # be present.
    read_time = done_ticket.get('exfer', {}).get('read_time', None)
    write_time = done_ticket.get('exfer', {}).get('write_time', None)

    if done_ticket['work'] == "read_from_hsm":
        preposition = "from"
        if read_time != None:
            network_time = read_time
        else:
            network_time = transfer_time
        if write_time != None:
            disk_time = write_time
        else:
            disk_time = transfer_time
    else: #write "to"
        preposition = "to"
        if write_time != None:
            network_time = write_time
        else:
            network_time = transfer_time
        if read_time != None:
            disk_time = read_time
        else:
            disk_time = transfer_time

    """
    #Note MWZ 9-19-2002: These lines are hacks.  They are evil.  Fix EXfer.c
    # write time calculation bug.
    if done_ticket['work'] == "read_from_hsm":
        nsa = 0
        dsa = 0 #intf_encp.buffer_size
    else:
        nsa = 0 #intf_encp.buffer_size
        dsa = 0
    """
    
    if e_errors.is_ok(done_ticket['status'][0]):

        if transfer_time != 0:
            tinfo['%s_transfer_rate'%(u_id,)] = MB_transfered / transfer_time
        else:
            tinfo['%s_transfer_rate'%(u_id,)] = 0.0
        if overall_time != 0:
            tinfo['%s_overall_rate'%(u_id,)] = MB_transfered / overall_time
        else:
            tinfo['%s_overall_rate'%(u_id,)] = 0.0
        if network_time != 0:
            tinfo['%s_network_rate'%(u_id,)] = MB_transfered / network_time
        else:
            tinfo['%s_network_rate'%(u_id,)] = 0.0            
        if drive_time != 0:
            tinfo['%s_drive_rate'%(u_id,)] = MB_transfered / drive_time
        else:
            tinfo['%s_drive_rate'%(u_id,)] = 0.0
        if disk_time != 0:
            tinfo['%s_disk_rate'%(u_id,)] = MB_transfered / disk_time
        else:
            tinfo['%s_disk_rate'%(u_id,)] = 0.0
            
        sg = done_ticket.get('vc', {}).get('storage_group', "")
        ff = done_ticket.get('vc', {}).get('file_family', "")
        ffw = done_ticket.get('vc', {}).get('file_family_wraper', "")
        if not sg:
            sg = volume_family.extract_storage_group(
                done_ticket.get('vc', {}).get('volume_family', ""))
        if not ff:
            ff = volume_family.extract_file_family(
                done_ticket.get('vc', {}).get('volume_family', ""))
        if not ffw:
            ffw = volume_family.extract_wrapper(
                done_ticket.get('vc', {}).get('volume_family', ""))
        
        print_format = "Transfer %s -> %s:\n" \
                 "\t%d bytes copied %s %s at %.3g MB/S\n " \
                 "\t(%.3g MB/S network) (%.3g MB/S drive) (%.3g MB/S disk)\n" \
                 "\t(%.3g MB/S overall) (%.3g MB/S transfer)\n" \
                 "\tdrive_id=%s drive_sn=%s drive_vendor=%s\n" \
                 "\tmover=%s media_changer=%s   elapsed=%.02f"

        log_format = "  %s %s -> %s: " \
                     "%d bytes copied %s %s at %.3g MB/S " \
                     "(%.3g MB/S network) (%.3g MB/S drive) (%.3g MB/S disk) "\
                     "mover=%s drive_id=%s drive_sn=%s drive_vendor=%s " \
                     "elapsed=%.05g %s"

                     #"{'media_changer' : '%s', 'mover_interface' : '%s', " \
                     #"'driver' : '%s', 'storage_group':'%s', " \
                     #"'encp_ip': '%s', 'unique_id': '%s'}"

        print_values = (done_ticket['infile'],
                        done_ticket['outfile'],
                        done_ticket['file_size'],
                        preposition,
                        done_ticket["fc"]["external_label"],
                        tinfo["%s_transfer_rate"%(u_id,)],
                        tinfo['%s_network_rate'%(u_id,)],
                        tinfo["%s_drive_rate"%(u_id,)],
                        tinfo["%s_disk_rate"%(u_id,)],
                        tinfo["%s_overall_rate"%(u_id,)],
                        tinfo["%s_transfer_rate"%(u_id,)],
                        done_ticket["mover"]["product_id"],
                        done_ticket["mover"]["serial_num"],
                        done_ticket["mover"]["vendor_id"],
                        done_ticket["mover"]["name"],
                        done_ticket["mover"].get("media_changer",
                                                 e_errors.UNKNOWN),
                        time.time() - tinfo["encp_start_time"])

        log_dictionary = {
            'media_changer' : done_ticket["mover"].get("media_changer",
                                                       e_errors.UNKNOWN),
            'mover_interface' : done_ticket["mover"].get('data_ip',
                                                 done_ticket["mover"]['host']),
            'driver' : done_ticket["mover"]["driver"],
            'storage_group' : sg,
            'encp_ip' : done_ticket["encp_ip"],
            'unique_id' : done_ticket['unique_id'],
            'network_rate' : tinfo["%s_transfer_rate"%(u_id,)],
            'drive_rate' : tinfo["%s_drive_rate"%(u_id,)],
            'disk_rate' : tinfo["%s_disk_rate"%(u_id,)],
            'overall_rate' : tinfo["%s_overall_rate"%(u_id,)],
            'transfer_rate' : tinfo["%s_transfer_rate"%(u_id,)],
            'encp_crc' : done_ticket.get('ecrc', 
                                   done_ticket['exfer'].get('encp_crc', None)),
            }

        log_values = (done_ticket['work'],
                      done_ticket['infile'],
                      done_ticket['outfile'],
                      done_ticket['file_size'],
                      preposition,
                      done_ticket["fc"]["external_label"],
                      tinfo["%s_transfer_rate"%(u_id,)],
                      tinfo["%s_network_rate"%(u_id,)],
                      tinfo['%s_drive_rate'%(u_id,)],
                      tinfo["%s_disk_rate"%(u_id,)],
                      done_ticket["mover"]["name"],
                      done_ticket["mover"]["product_id"],
                      done_ticket["mover"]["serial_num"],
                      done_ticket["mover"]["vendor_id"],
                      time.time() - tinfo["encp_start_time"],
                      log_dictionary)
                      #done_ticket["mover"].get("media_changer",
                      #                         e_errors.UNKNOWN),
		      #done_ticket["mover"].get('data_ip',
                      #                         done_ticket["mover"]['host']),
                      #done_ticket["mover"]["driver"],
                      #sg,
                      #done_ticket["encp_ip"],
                      #done_ticket['unique_id'])
        
        Trace.message(DONE_LEVEL, print_format % print_values)

        Trace.log(e_errors.INFO, log_format % log_values, Trace.MSG_ENCP_XFER )

        # Use an 'r' or 'w' to signify read or write in the accounting db.
        if is_read(done_ticket):  #['work'] == "read_from_hsm":
		rw = 'r'
	else:
		rw = 'w'

        # Avoid overflow and division by zero problems.
        try:
            if network_time == 0.0:
                acc_network_rate = int(0.0);
            else:
                acc_network_rate = int(done_ticket['file_size'] / network_time)
        except OverflowError:
            acc_network_rate = -1
        try:
            if drive_time == 0.0:
                acc_drive_rate = int(0.0)
            else:
                acc_drive_rate = int(done_ticket['file_size'] / drive_time)
        except OverflowError:
            acc_drive_rate = -1
        try:
            if disk_time == 0.0:
                acc_disk_rate = int(0.0)
            else:
                acc_disk_rate = int(done_ticket['file_size'] / disk_time)
        except OverflowError:
            acc_disk_rate = -1
        try:
            if overall_time == 0.0:
                acc_overall_rate = int(0.0)
            else:
                acc_overall_rate = int(done_ticket['file_size'] / overall_time)
        except OverflowError:
            acc_overall_rate = -1
        try:
            if transfer_time == 0.0:
                acc_transfer_rate = int(0.0)
            else:
                acc_transfer_rate = int(done_ticket['file_size'] / transfer_time)
        except OverflowError:
            acc_transfer_rate = -1

        acc = get_acc()
	acc.log_encp_xfer(None,
                          done_ticket['infile'],
                          done_ticket['outfile'],
                          done_ticket['file_size'],
                          done_ticket["fc"]["external_label"],
                          #The accounting db expects the rates in bytes
                          # per second; not MB per second.
                          acc_network_rate,
                          acc_drive_rate,
                          acc_disk_rate,
                          acc_overall_rate,
                          acc_transfer_rate,
                          done_ticket["mover"]["name"],
                          done_ticket["mover"]["product_id"],
                          done_ticket["mover"]["serial_num"],
                          time.time() - tinfo["encp_start_time"],
                          done_ticket["mover"].get("media_changer",
                                                   e_errors.UNKNOWN),
                          done_ticket["mover"].get('data_ip',
                                               done_ticket["mover"]['host']),
                          done_ticket["mover"]["driver"],
                          sg,
                          done_ticket["encp_ip"],
                          done_ticket['unique_id'],
                          rw,
                          encp_client_version(),
                          ff,
                          ffw)

    Trace.message(TIME_LEVEL, "Time to calculate and record rate: %s sec." %
                  (time.time() - calculate_rate_start_time,))

############################################################################

def calculate_final_statistics(bytes, number_of_files, exit_status, tinfo):

    calculate_final_statistics_start_time = time.time()
    
    #Determine the average of each time (overall, transfer, network,
    # tape and disk) of all transfers done for the encp.  If only one file
    # was transfered, then these rates should equal the files rates.

    statistics = {}
    
    #Calculate total running time from the begining.
    now = time.time()
    tinfo['total'] = now - tinfo['encp_start_time']

    #calculate MB relatated stats
    #bytes_per_MB = float(1024 * 1024)
    #MB_transfered = float(bytes) / bytes_per_MB

    #get all the overall rates from the dictionary.
    overall_rate  = 0L
    count = 0
    for value in tinfo.keys():
        if string.find(value, "overall_rate") != -1:
            count = count + 1
            overall_rate = overall_rate + tinfo[value]
    if count:
        statistics['MB_per_S_overall'] = overall_rate / count
    else:
        statistics['MB_per_S_overall'] = 0.0

    #get all the transfer rates from the dictionary.
    transfer_rate  = 0L
    count = 0
    for value in tinfo.keys():
        if string.find(value, "transfer_rate") != -1:
            count = count + 1
            transfer_rate = transfer_rate + tinfo[value]
    if count:
        statistics['MB_per_S_transfer'] = transfer_rate / count
    else:
        statistics['MB_per_S_transfer'] = 0.0

    #get all the drive rates from the dictionary.
    drive_rate  = 0L
    count = 0
    for value in tinfo.keys():
        if string.find(value, "drive_rate") != -1:
            count = count + 1
            drive_rate = drive_rate + tinfo[value]
    if count:
        statistics['MB_per_S_drive'] = drive_rate / count
    else:
        statistics['MB_per_S_drive'] = 0.0

    #get all the network rates from the dictionary.
    network_rate  = 0L
    count = 0
    for value in tinfo.keys():
        if string.find(value, "network_rate") != -1:
            count = count + 1
            network_rate = network_rate + tinfo[value]
    if count:
        statistics['MB_per_S_network'] = network_rate / count
    else:
        statistics['MB_per_S_network'] = 0.0
        
    #get all the disk rates from the dictionary.
    disk_rate  = 0L
    count = 0
    for value in tinfo.keys():
        if string.find(value, "disk_rate") != -1:
            count = count + 1
            disk_rate = disk_rate + tinfo[value]
    if count:
        statistics['MB_per_S_disk'] = disk_rate / count
    else:
        statistics['MB_per_S_disk'] = 0.0
    
    msg = "%s transferring %s bytes in %s files in %s sec.\n" \
          "\tOverall rate = %.3g MB/sec.  Transfer rate = %.3g MB/sec.\n" \
          "\tNetwork rate = %.3g MB/sec.  Drive rate = %.3g MB/sec.\n" \
          "\tDisk rate = %.3g MB/sec.  Exit status = %s."
    
    if exit_status:
        msg = msg % ("Error after", bytes, number_of_files, tinfo['total'],
                     statistics["MB_per_S_overall"],
                     statistics["MB_per_S_transfer"],
                     statistics['MB_per_S_network'],
                     statistics['MB_per_S_drive'],
                     statistics["MB_per_S_disk"],
                     exit_status)
    else:
        msg = msg % ("Completed", bytes, number_of_files, tinfo['total'],
                     statistics["MB_per_S_overall"],
                     statistics["MB_per_S_transfer"],
                     statistics['MB_per_S_network'],
                     statistics['MB_per_S_drive'],
                     statistics["MB_per_S_disk"],
                     exit_status)

    done_ticket = {}
    done_ticket['statistics'] = statistics
    #set the final status values
    done_ticket['exit_status'] = exit_status
    done_ticket['status'] = (e_errors.OK, msg)

    Trace.message(TIME_LEVEL, "Time to calculate final statistics: %s sec." %
                  (time.time() - calculate_final_statistics_start_time,))
    
    return done_ticket

############################################################################
#Support functions for writes.
############################################################################

#Verifies that the state of the files, like existance and permissions,
# are accurate.
#def verify_write_file_consistancy(request_list, e):
#
#    for request in request_list:
#
#        #Verify that everything with the files (existance, permissions,
#        # etc) is good to go.
#        inputfile_check(request['infile'], e)
#        outputfile_check(request['infile'], request['outfile'], e)


#Args:
# Takes a list of request tickets.
#Returns:
#None
#Verifies that various information in the tickets are correct, valid, spelled
# correctly, etc.
def verify_write_request_consistancy(request_list, e):
    
    outputfile_dict = {}

    for request in request_list:

        if request['infile'] not in ["/dev/zero",
                                     "/dev/random", "/dev/urandom"]:
            inputfile_check(request['infile'], e)
            
        if request['outfile'] not in ["/dev/null", "/dev/zero",
                                      "/dev/random", "/dev/urandom"]:
            if not request['wrapper']['inode']:
                #Only test this before the output file is created.
                outputfile_check(request['infile'], request['outfile'], e)

        #This block of code makes sure the the user is not moving
        # two files with the same basename in different directories
        # into the same destination directory.
        result = outputfile_dict.get(request['outfile'], None)
        if result and not request.get('copy', None):
            #If the file is already in the list (and not a copy), give error.
            raise EncpError(None,
                            'Duplicate file entry: %s' % (result,),
                            e_errors.USERERROR,
                            {'infile' : request['infile'],
                             'outfile' : request['outfile']})
        else:
            #Put into one place all of the output names.  This is to check
            # that two file to not have the same output name.
            outputfile_dict[request['outfile']] = request['infile']
            
        #Consistancy check for valid pnfs tag values.  These values are
        # placed inside the 'vc' sub-ticket.
        tags = ["file_family", "wrapper", "file_family_width",
                "storage_group", "library"]
        for key in tags:
            if request.get('copy', None):
                #If this is a copy request (via --copies), skip this check,
                # since checking the original is good enough.  Otherwise,
                # the lack of 'file_family' (becuase on copies it is
                # original_file_family) would fail all copies.
                break
            
            try:
                #check for values that contain letters, digits and _.
                if not charset.is_in_charset(str(request['vc'][key])):
                    raise EncpError(None,
                                    "Pnfs tag, %s, contains invalid "
                                    " characters." % (key,),
                                    e_errors.PNFS_ERROR)
            except (ValueError, AttributeError, TypeError,
                    IndexError, KeyError), msg:
                msg = "Error checking tag %s: %s" % (key, str(msg))
                raise EncpError(None, str(msg), e_errors.USERERROR)

        #Verify that the file family width is in fact a non-
        # negitive integer.
        try:
            expression = int(request['vc']['file_family_width']) < 0
            if expression:
                raise ValueError,(e_errors.USERERROR,
                                  request['vc']['file_family_width'])
        except ValueError:
            msg="Pnfs tag, %s, requires a non-negitive integer value."\
                 % ("file_family_width",)
            raise EncpError(None, str(msg), e_errors.USERERROR)

        #Verify that the library and wrappers are valid.
        librarysize_check(request)
        wrappersize_check(request)

############################################################################

def set_pnfs_settings(ticket, intf_encp):

    # create a new pnfs object pointing to current output file
    Trace.message(INFO_LEVEL,
            "Updating %s file metadata." % ticket['wrapper']['pnfsFilename'])

    location_start_time = time.time() # Start time of verifying pnfs file.

    #Make sure the file is still there.  This check is done with
    # access_check() because it will loop incase pnfs is automounted.
    # If the return is zero, then it wasn't found.
    #Note: There is a possible race condition here if the file is
    #      (re)moved after it is determined to remain in the original
    #      location, but before the metadata is set.
    
    if access_check(ticket['wrapper']['pnfsFilename'], os.F_OK) == 0:
        #With the remembered pnfsid, try to find out where it was moved to
        # if it was in fact moved.
        pnfsid = ticket['fc'].get('pnfsid', None)
        if pnfsid:
            try:
                if intf_encp.outtype == RHSMFILE \
                       or intf_encp.intype == RHSMFILE: #intype for "get".
                    p = get_pac()
                    path = p.get_path(pnfsid,
                     get_directory_name(ticket['wrapper']['pnfsFilename']),
                                      shortcut = intf_encp.shortcut)
                else:   # HSMFILE
                    p = pnfs.Pnfs(pnfsid,
                     get_directory_name(ticket['wrapper']['pnfsFilename']),
                                  shortcut = intf_encp.shortcut)
                    path = p.get_path()  #Find the new path.
                Trace.log(e_errors.INFO,
                          "File %s was moved to %s." %
                          (ticket['wrapper']['pnfsFilename'], path))
                ticket['wrapper']['pnfsFilename'] = path  #Remember new path.
                if is_write(ticket):
                    ticket['outfile'] = path
                else:  #is_read() for "get".
                    ticket['infile'] = path
            except (OSError, IOError, AttributeError, ValueError):
                ticket['status'] = (e_errors.USERERROR,
                                    "PNFS file %s has been removed." %
                                    ticket['wrapper']['pnfsFilename'])
                Trace.log(e_errors.ERROR,
                          "Trouble with pnfs: %s %s." % ticket['status'])
                return
        else:
            ticket['status'] = (e_errors.USERERROR,
                                "PNFS file %s has been removed." %
                                ticket['wrapper']['pnfsFilename'])
            Trace.log(e_errors.ERROR,
                      "Trouble with pnfs: %s %s." % ticket['status'])
            return
    else:
        #Check to make sure that the inodes are still the same.
        verify_inode(ticket)
        if not e_errors.is_ok(ticket):
            #Ticket is already set.
            return

        try:
            p = Pnfs(ticket['wrapper']['pnfsFilename'])
        except (KeyboardInterrupt, SystemExit):
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except:
            #Get the exception.
            msg = sys.exc_info()[1]
            #If it is a user access/permitted problem handle accordingly.
            if hasattr(msg, "errno") and \
                   (msg.errno == errno.EACCES or msg.errno == errno.EPERM):
                ticket['status'] = (e_errors.USERERROR, str(msg))
            #Handle all other errors.
            else:
                ticket['status'] = (e_errors.PNFS_ERROR, str(msg))
            #Log the problem.
            Trace.log(e_errors.INFO,
                      "Trouble with pnfs: %s %s." % ticket['status'])
            return
    
    Trace.message(TIME_LEVEL, "Time to veify pnfs file location: %s sec." %
                  (time.time() - location_start_time,))

    if not ticket.get('copy', None):  #Don't set layer 1 if copy.
        layer1_start_time = time.time() # Start time of setting pnfs layer 1.

        #The first piece of metadata to set is the bit file id which is placed
        # into layer 1.
        Trace.message(INFO_LEVEL, "Setting layer 1: %s" %
                      ticket["fc"]["bfid"])
        try:
            # save the bfid
            p.set_bit_file_id(ticket["fc"]["bfid"],
                              ticket['wrapper']['pnfsFilename'])
        except (KeyboardInterrupt, SystemExit):
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except:
            #Get the exception.
            msg = sys.exc_info()[1]
            #If it is a user access/permitted problem handle accordingly.
            if hasattr(msg, "errno") and \
               (msg.errno == errno.EACCES or msg.errno == errno.EPERM):
                ticket['status'] = (e_errors.USERERROR, str(msg))
            #Handle all other errors.
            else:
                ticket['status'] = (e_errors.PNFS_ERROR, str(msg))
            #Log the problem.
            Trace.log(e_errors.INFO,
                      "Trouble with pnfs: %s %s." % ticket['status'])
            return

        Trace.message(TIME_LEVEL, "Time to set pnfs layer 1: %s sec." %
                      (time.time() - layer1_start_time,))

    #Format some tape drive output.
    mover_ticket = ticket.get('mover', {})
    drive = "%s:%s" % (mover_ticket.get('device', 'Unknown'),
                       mover_ticket.get('serial_num','Unknown'))
    #For writes to null movers, make the crc zero.
    if mover_ticket['driver'] == "NullDriver":
        crc = 0
    else:
        crc = ticket['fc']['complete_crc']

    try:
        #Perform the following get functions; even if it is a copy.  These,
        # calls set values in the object that are used to also update
        # file db entires for copies.
        pnfs_id = p.get_id(ticket['wrapper']['pnfsFilename'])
    except (KeyboardInterrupt, SystemExit):
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
    except:
        #Get the exception.
        msg = sys.exc_info()[1]
        #If it is a user access/permitted problem handle accordingly.
        if hasattr(msg, "errno") and \
               (msg.errno == errno.EACCES or msg.errno == errno.EPERM):
            ticket['status'] = (e_errors.USERERROR, str(msg))
        #Handle all other errors.
        else:
            ticket['status'] = (e_errors.PNFS_ERROR, str(msg))
        #Log the problem.
        Trace.log(e_errors.INFO,
                  "Trouble with pnfs: %s %s." % ticket['status'])
        
        exc, msg = sys.exc_info()[:2]
        Trace.log(e_errors.INFO, "Trouble with pnfs.set_xreference %s %s."
                  % (str(exc), str(msg)))
        ticket['status'] = (str(exc), str(msg))
        return

    if not ticket.get('copy', None):  #Don't set layer 4 if copy.
        layer4_start_time = time.time() # Start time of setting pnfs layer 4.

        #Store the cross reference data into layer 4.
        
        #Write to the metadata layer 4 "file".
        Trace.message(INFO_LEVEL, "Setting layer 4.")
        try:
            p.set_xreference(
                ticket["fc"]["external_label"],
                ticket["fc"]["location_cookie"],
                ticket["fc"]["size"],
                volume_family.extract_file_family(ticket["vc"]["volume_family"]),
                ticket['wrapper']['pnfsFilename'],
                "", #p.volume_filepath,
                pnfs_id,
                "", #p.volume_fileP.id,
                ticket["fc"]["bfid"],
                drive,
                crc,
                filepath=ticket['wrapper']['pnfsFilename'])
        except (KeyboardInterrupt, SystemExit):
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except:
            #Get the exception.
            msg = sys.exc_info()[1]
            #If it is a user access/permitted problem handle accordingly.
            if hasattr(msg, "errno") and \
               (msg.errno == errno.EACCES or msg.errno == errno.EPERM):
                ticket['status'] = (e_errors.USERERROR, str(msg))
            #Handle all other errors.
            else:
                ticket['status'] = (e_errors.PNFS_ERROR, str(msg))
            #Log the problem.
            Trace.log(e_errors.INFO,
                      "Trouble with pnfs: %s %s." % ticket['status'])

            exc, msg = sys.exc_info()[:2]
            Trace.log(e_errors.INFO, "Trouble with pnfs.set_xreference %s %s."
                      % (str(exc), str(msg)))
            ticket['status'] = (str(exc), str(msg))
            return

        Trace.message(TIME_LEVEL, "Time to set pnfs layer 4: %s sec." %
                      (time.time() - layer4_start_time,))

    filedb_start_time = time.time() # Start time of updating file database.

    #Update the file database with the transfer info.
    Trace.message(INFO_LEVEL, "Setting file db pnfs fields.")
    try:
        # add the pnfs ids and filenames to the file clerk ticket and store it
        fc_ticket = {}
        fc_ticket["fc"] = ticket['fc'].copy()
        fc_ticket["fc"]["pnfsid"] = pnfs_id
        fc_ticket["fc"]["pnfsvid"] = "" #p.volume_fileP.id
        fc_ticket["fc"]["pnfs_name0"] = ticket['wrapper']['pnfsFilename']
        fc_ticket["fc"]["pnfs_mapname"] = "" #p.mapfile
        fc_ticket["fc"]["drive"] = drive
        fc_ticket["fc"]['uid'] = ticket['wrapper']['uid']
        fc_ticket["fc"]['gid'] = ticket['wrapper']['gid']

        #As long as encp is restricted to working with one enstore system
        # at a time, passing get_fcc() the bfid info is not necessary.
        fcc = get_fcc()   #ticket["fc"]["bfid"]
        fc_reply = fcc.set_pnfsid(fc_ticket)

        if not e_errors.is_ok(fc_reply['status'][0]):
            Trace.alarm(e_errors.ERROR, fc_reply['status'][0], fc_reply)
            ticket['status'] = fc_reply['status']
            return

        Trace.message(TICKET_LEVEL, "FILE DB PNFS FIELDS SET")
        Trace.message(TICKET_LEVEL, pprint.pformat(fc_reply))

        #ticket['status'] = fc_reply['status']
    except (KeyboardInterrupt, SystemExit):
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
    except:
        exc, msg = sys.exc_info()[:2]
        Trace.log(e_errors.INFO, "Unable to send info. to file clerk. %s %s."
                  % (str(exc), str(msg)))
        ticket['status'] = (str(exc), str(msg))
        return

    Trace.message(TIME_LEVEL, "Time to set file database: %s sec." %
                  (time.time() - filedb_start_time,))

    if not ticket.get('copy', None):  #Don't set size if copy.
        filesize_start_time = time.time() # Start time of setting the filesize.

        # file size needs to be the LAST metadata to be recorded
        Trace.message(INFO_LEVEL, "Setting filesize.")
        try:
            #The dcache sets the file size.  If encp tries to set it again,
            # pnfs sets the size to zero.  Thus, only do this for normal
            # transfers.
            if not intf_encp.put_cache:
                #If the size is already set don't set it again.  Doing so
                # would set the filesize back to zero.
                pstat = p.get_stat(ticket['wrapper']['pnfsFilename'])
                size = pstat[stat.ST_SIZE]
                if long(size) == long(ticket['file_size']) or long(size) == 1L:
                    Trace.log(e_errors.INFO,
                              "Filesize (%s) for file %s already set." %
                              (ticket['file_size'],
                               ticket['wrapper']['pnfsFilename']))
                else:
                    # set the file size
                    p.set_file_size(ticket['file_size'],
                                    ticket['wrapper']['pnfsFilename'])
        except (KeyboardInterrupt, SystemExit):
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except IOError, msg:
            ticket['status'] = (e_errors.IOERROR, str(msg))
            return
        except OSError, msg:
            ticket['status'] = (e_errors.OSERROR, str(msg))
            return
        except:
            exc, msg = sys.exc_info()[:2]
            ticket['status'] = (str(exc), str(msg))
            return

        Trace.message(TIME_LEVEL, "Time to set filesize: %s sec." %
                  (time.time() - filesize_start_time,))

    #This functions write errors/warnings to the log file and put an
    # error status in the ticket.
    verify_file_size(ticket) #Verify size is the same.

############################################################################
#Functions for writes.
############################################################################

def create_write_requests(callback_addr, udp_callback_addr, e, tinfo):

    request_list = []

    #Initialize these, so that they can be set only once.
    csc = vcc = fcc = None
    file_family = file_family_width = file_family_wrapper = None
    library = storage_group = None
    if e.outtype == RHSMFILE:
        t = p = Pnfs(use_pnfs_agent = True)
    else:
        p = Pnfs(use_pnfs_agent = False)
        t=pnfs.Tag()  #get_directory_name(ofullname))

    # create internal list of input unix files even if just 1 file passed in
    if type(e.input) == types.ListType:
        pass  #e.input = e.input
    else:
        e.input = [e.input]

    if not e.put_cache and len(e.output) > 1:
        raise EncpError(None,
                        'Cannot have multiple output files',
                        e_errors.USERERROR)

    # check the input unix file. if files don't exits, we bomb out to the user
    for i in range(len(e.input)):

        if e.put_cache:

            if e.shortcut and e.override_path:
                #If the user specified a pathname (with --override-path)
                # on the command line use that name.  Otherwise if just
                # --shortcut is used, the filename name will be recored as
                # a /.(access)(<pnfsid>) style filename.
                ofullname = e.override_path
            else:
                ofullname = p.get_path(e.put_cache, e.pnfs_mount_point,
                                       shortcut=e.shortcut)

            unused, ifullname, unused, unused = fullpath(e.input[0])
            istatinfo = os.stat(ifullname)
        else: #The output file was given as a normal filename.
            #ifullname, ofullname = get_ninfo(e.input[i], e.output[0], e)

            try:
                istatinfo = os.stat(e.input[i])
                ifullname = e.input[i]
            except (KeyboardInterrupt, SystemExit):
                raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
            except (OSError, IOError):
                ifullname = get_ininfo(e.input[i])
                istatinfo = os.stat(ifullname)

            #inputfile_check(ifullname, e)

            ofullname = get_oninfo(e.input[i], e.output[0], e)

            #The existence rules behind the output file are more
            # complicated than those of the input file.  We always need
            # to call outputfile_check.  It still should go in
            # some verify function though.
            #
            #Fundamentally this belongs in verify_read_request_consistancy(),
            # but information needed about the input file requires this check.
            #outputfile_check(ifullname, ofullname, e)

        odirname = get_directory_name(ofullname)

        #ostatinfo = p.get_pnfsstat(ofullname) #same as p.get_stat(odirname)
        ostatinfo = p.get_stat(odirname)
        
        #Get these two pieces of information about the local input file.
        if ifullname in ["/dev/zero", "/dev/random", "/dev/urandom"]:
            bound_limits = [long(os.environ.get('ENSTORE_RANDOM_LB', 0L)),
                            long(os.environ.get('ENSTORE_RANDOM_UB',
                                                2147483648L))]
            bound_limits.sort()
            file_size = long(random.uniform(bound_limits[0], bound_limits[1]))
        else:
            file_size = long(istatinfo[stat.ST_SIZE])
        file_inode = istatinfo[stat.ST_INO]
        
        #There is no sense to get these values every time.  Only get them
        # on the first pass.
        if e.output_library:
            #Only take the first item of a possible comma seperated list.
            all_libraries = e.output_library.split(",")
            library = all_libraries[0]
            use_copies = len(all_libraries[1:])
        if not library:
            #If library is still empty, use the default
            all_libraries = t.get_library(odirname).split(",")
            library = all_libraries[0]
            use_copies = len(all_libraries[1:])
        #The pnfs file family may be overridden with the options
        # --ephemeral or --file-family.
        if not file_family:
            if e.output_file_family:
                file_family = e.output_file_family
            else:
                file_family = t.get_file_family(odirname)
        if not file_family_width:
            if e.output_file_family_width:
                file_family_width = e.output_file_family_width
            else:
                file_family_width = t.get_file_family_width(odirname)
        if not file_family_wrapper:
            if e.output_file_family_wrapper:
                file_family_wrapper = e.output_file_family_wrapper
            else:
                file_family_wrapper = t.get_file_family_wrapper(odirname)
        if not storage_group:
            if e.output_storage_group:
                storage_group = e.output_storage_group
            else:
                storage_group = t.get_storage_group(odirname)

        #Get the data aquisition information.
        encp_daq = get_dinfo()

        #Snag the three pieces of information needed for the wrapper.
        uinfo = get_uinfo()
        finfo = get_finfo(ifullname, ofullname, e)
        pinfo = get_minfo(ostatinfo)
        finfo['size_bytes'] = file_size
        pinfo['type'] = file_family_wrapper

        #Combine the data into the wrapper sub-ticket.
        wrapper = get_winfo(pinfo, uinfo, finfo)
        
        #Create the sub-ticket of the command line argument information.
        encp_el = get_einfo(e)

        # If this is not the last transfer in the list, force the delayed
        # dismount to be 'long.'  The last transfer should continue to use
        # the default setting.
        if i < (len(e.input) - 1):
            #In minutes.
            encp_el['delayed_dismount'] = max(3, encp_el['delayed_dismount'])

        #only do this the first time.
        if not vcc or not fcc:
            csc = get_csc() #Get csc once for max_attempts().
            vcc, fcc = get_clerks()
                
        #Get the information needed to contact the file clerk, volume clerk and
        # the library manager.
        volume_clerk = {"address"            : vcc.server_address,
                        "library"            : library,
                        "file_family"        : file_family,#might be overridden
                        # technically width does not belong here,
                        # but it associated with the volume
                        "file_family_width"  : file_family_width,
                        "wrapper"            : file_family_wrapper,
                        "storage_group"      : storage_group,}
        file_clerk = {'address' : fcc.server_address}

        #config = host_config.get_config()
        #if config and config.get('interface', None):
        #    route_selection = 1
        #else:
        #    route_selection = 0
        #if udp_callback_addr:
        #    route_selection = 1   #1 to use udp_server, 0 for no.
        #else:
        #    route_selection = 0

        #Determine the max resend values for this transfer.
        resend = max_attempts(csc, volume_clerk['library'], e)

        #We cannot use --shortcut without --override-path when
        # using NULL movers and tapes.  The mover insists that
        # "NULL" appear in the pnfs pathname, which conflicts with
        # the task of --shortcut.  So, we need to breakdown and do
        # this full name lookup.
        if e.shortcut and not e.override_path and \
               file_family_wrapper == "null":
            
            #Grab current configuration information.
            if not csc:
                csc = get_csc()
            if not csc.have_complete_config:
                csc.save_and_dump()

            # Now we need check all null movers.
            for dictkey, value in csc.saved_dict.items():
                if dictkey[-len(".mover"):] == ".mover" \
                   and type(value) == types.DictType \
                   and value.get('driver', None) == "NullDriver":
                    
                    if type(value['library']) == types.ListType:
                        library_list = value['library']
                    else:
                        library_list = [value['library']]

                    for lib in library_list:
                        if library == lib.split(".")[0]:
                            #If we find a null mover with a library that
                            # matches that of the library we are using; we
                            # know the following is true (this is a recap):
                            # 1) --shortcut was used.
                            # 2) --override-path was not used.
                            # 3) The library we are using uses null movers.
                            #
                            #To avoid an error with the mover, perform a full
                            # pnfs pathname lookup.
                            ofullname = p.get_path(e.put_cache,
                                                   e.pnfs_mount_point,
                                                   shortcut=False)
                            wrapper['pnfsFilename'] = ofullname
                            break

                    else:
                        continue

                    break  #stop processing the outter loop.

        work_ticket = {}
        work_ticket['callback_addr'] = callback_addr
        work_ticket['client_crc'] = e.chk_crc
        work_ticket['encp'] = encp_el
        work_ticket['encp_daq'] = encp_daq
        work_ticket['fc'] = file_clerk
        work_ticket['file_size'] = file_size
        work_ticket['ignore_fair_share'] = e.ignore_fair_share
        work_ticket['infile'] = ifullname
        work_ticket['local_inode'] = file_inode
        work_ticket['outfile'] = ofullname
        work_ticket['override_ro_mount'] = e.override_ro_mount
        work_ticket['resend'] = resend
        work_ticket['retry'] = None #LM legacy requirement.
        if udp_callback_addr: #For "get" only.
            work_ticket['routing_callback_addr'] = udp_callback_addr
            work_ticket['route_selection'] = 1
        work_ticket['times'] = tinfo.copy() #Only info now in tinfo needed.
        work_ticket['unique_id'] = generate_unique_id()
        work_ticket['vc'] = volume_clerk
        work_ticket['version'] = encp_client_version()
        work_ticket['work'] = "write_to_hsm"
        work_ticket['wrapper'] = wrapper

        request_list.append(work_ticket)

    #If the user overrides the copy count, use that instead.
    if e.copies != None:
        use_copies = e.copies

    #Make dictionaries for the copies of the data.
    request_copy_list = []
    if use_copies > 0:

        parmameter_libraries = e.output_library.split(",")
        tag_libraries = t.get_library(odirname).split(",")
        
        for n_copy in range(1, use_copies + 1):

            #Determine the library manager to use.  First, try to see if
            # the command line has the information.  Otherwise, check
            # the library tag.  In both cases, the library should be a
            # comma seperated list of library manager short names.
            try:
                current_library = parmameter_libraries[n_copy]
            except IndexError:
                try:
                    current_library = tag_libraries[n_copy]
                except IndexError:
                    #We get here if n copies were requested, but less than
                    # that number of libraries were found.
                    #use_copies = n_copy - 1
                    #break
                    raise EncpError(None,
                                    "Too many copies requested for the "
                                    "number of configured copy libraries.",
                                    e_errors.USERERROR)
            
            for work_ticket in request_list:
                copy_ticket = copy.deepcopy(work_ticket)
                #Specify the copy number; this is the copy number relative to
                # this encp.
                copy_ticket['copy'] = n_copy
                #Make the transfer id unique, but also keep the original around.
                copy_ticket['original_unique_id'] = copy_ticket['unique_id']
                del copy_ticket['unique_id']
                copy_ticket['unique_id'] = generate_unique_id()
                #Move the file_family to original_file_family; this is similar
                # to how the original_bfid is sent too.
                copy_ticket['vc']['original_file_family'] = \
                                             copy_ticket['vc']['file_family']
                del copy_ticket['vc']['file_family']
                #Set the new library.
                copy_ticket['vc']['library'] = current_library


                #Store the copy ticket.
                request_copy_list.append(copy_ticket)

    return request_list + request_copy_list

############################################################################

def submit_write_request(work_ticket, encp_intf):

    #Trace.message(TRANSFER_LEVEL, 
    #              "Submitting %s write request.  elapsed=%s" % \
    #              (work_ticket['outfile'],
    #               time.time() - tinfo['encp_start_time']))

    # send the work ticket to the library manager
    while encp_intf.max_retry == None or \
         work_ticket.get('resend', {}).get('retry', 0) <= encp_intf.max_retry:
        
        ##start of resubmit block
        Trace.trace(17,"write_to_hsm q'ing: %s"%(work_ticket,))

        ticket = submit_one_request(work_ticket, encp_intf)

        Trace.message(TICKET_LEVEL, "LIBRARY MANAGER")
        Trace.message(TICKET_LEVEL, pprint.pformat(ticket))

        result_dict = handle_retries([work_ticket], work_ticket, ticket,
                                     encp_intf)

        if e_errors.is_ok(result_dict['status'][0]):
	    ticket['status'] = result_dict['status']
            return ticket
        #elif result_dict['status'][0] == e_errors.RETRY or \
        #   e_errors.is_retriable(result_dict['status'][0]):
        #    continue
        elif e_errors.is_retriable(result_dict['status'][0]):
            continue
        else:
            ticket['status'] = result_dict['status']
            return ticket
	
    ticket['status'] = (e_errors.TOO_MANY_RETRIES, ticket['status'])
    return ticket

############################################################################

def stall_write_transfer(data_path_socket, e):
    #Stall starting the count until the first byte is ready for writing.
    duration = e.mover_timeout
    while 1:
        start_time = time.time()
        try:
            write_fd = select.select([], [data_path_socket], [],
                                     duration)[1]
            break
        except (select.error, socket.error), msg:
            if msg.args[0] == errno.EINTR or msg.args[0] == errno.EAGAIN:
                #If the select was interupted by a signal, keep going.
                duration = duration - (time.time() - start_time)
                continue
            else:
                write_fd = []
                break

    if data_path_socket not in write_fd:
        status_ticket = {'status' : (e_errors.UNKNOWN,
                                     "No data written to mover.")}

    #To achive more accurate rates on writes to enstore when a tape
    # needs to be mounted, wait until the mover has sent a byte as
    # a signal to encp that it is ready to read data from its socket.
    # Otherwise, 64K bytes just sit in the movers recv queue and the clock
    # ticks by making the write rate worse.  When the mover finally
    # mounts and positions the tape, the damage to the rate is already
    # done.
    duration = e.mover_timeout
    while 1:
        start_time = time.time()
        try:
            read_fd = select.select([data_path_socket], [], [],
                                     duration)[0]
            break
        except (select.error, socket.error), msg:
            if msg.args[0] == errno.EINTR or msg.args[0] == errno.EAGAIN:
                #If the select was interupted by a signal, keep going.
                duration = duration - (time.time() - start_time)
                continue
            else:
                read_fd = []
                break

    if data_path_socket not in read_fd:
        status_ticket = {'status' : (e_errors.UNKNOWN,
                                     "No data written to mover.")}
    #If there is no data waiting in the buffer, we have an error.
    elif len(data_path_socket.recv(1, socket.MSG_PEEK)) == 0:
        status_ticket = {'status' : (e_errors.UNKNOWN,
                                     "No data written to mover.")}
    else:
        status_ticket = {'status' : (e_errors.OK, None)}

    return status_ticket

############################################################################


def write_hsm_file(listen_socket, work_ticket, tinfo, e):

    Trace.message(TICKET_LEVEL, "WORK_TICKET")
    Trace.message(TICKET_LEVEL, pprint.pformat(work_ticket))

    #Trace.message(TRANSFER_LEVEL,
    #              "Sending ticket to library manager,  elapsed=%s" %
    #              (time.time() - tinfo['encp_start_time'],))
        
    #Send the request to write the file to the library manager.
    done_ticket = submit_write_request(work_ticket, e)

    Trace.message(TICKET_LEVEL, "LM RESPONSE TICKET")
    Trace.message(TICKET_LEVEL, pprint.pformat(done_ticket))
    
    work_ticket = combine_dict(done_ticket, work_ticket)

    #handle_retries() is not required here since submit_write_request()
    # handles its own retrying when an error occurs.
    if not e_errors.is_ok(work_ticket):
        return work_ticket

    Trace.message(TRANSFER_LEVEL,
               "File queued: %s library: %s family: %s bytes: %d elapsed=%s" %
                  (work_ticket['infile'], work_ticket['vc']['library'],
                   work_ticket['vc']['file_family'],
                   long(work_ticket['file_size']),
                   time.time() - tinfo['encp_start_time']))
    
    #Loop around in case the file transfer needs to be retried.
    while e.max_retry == None or \
              work_ticket['resend'].get('retry', 0) <= e.max_retry:

        #Wait for the mover to establish the control socket.  See if the
        # id matches one the the tickets we submitted.  Establish data socket
        # connection with the mover.
        control_socket, data_path_socket, ticket = mover_handshake(
            listen_socket, [work_ticket], e)

        overall_start = time.time() #----------------------------Overall Start

        #Handle any possible errors that occured so far.
        local_filename = work_ticket.get('wrapper', {}).get('fullname', None)
        external_label = work_ticket.get('fc', {}).get('external_label', None)
        result_dict = handle_retries([work_ticket], work_ticket, ticket, e,
                                     listen_socket = listen_socket,
                                     local_filename = local_filename,
                                     external_label = external_label)

        if e_errors.is_resendable(result_dict['status'][0]):
            continue
        elif e_errors.is_non_retriable(result_dict['status'][0]):
            ticket = combine_dict(result_dict, ticket, work_ticket)
            return ticket

        #Be paranoid.  Check this the ticket again.
        try:
            verify_write_request_consistancy([ticket], e)
        except EncpError, msg:
            msg.ticket['status'] = (msg.type, msg.strerror)
            return msg.ticket

        #This should be redundant error check.
        if not control_socket or not data_path_socket:
	    ticket = combine_dict({'status':(e_errors.NET_ERROR, "No socket")},
				   work_ticket)
            return ticket #This file failed.

        Trace.message(TRANSFER_LEVEL, "Mover called back.  elapsed=%s" %
                      (time.time() - tinfo['encp_start_time'],))
        
        Trace.message(TICKET_LEVEL, "WORK TICKET:")
        Trace.message(TICKET_LEVEL, pprint.pformat(ticket))

        #maybe this isn't a good idea...
        work_ticket = combine_dict(ticket, work_ticket)

        done_ticket = open_local_file(work_ticket, e)

        result_dict = handle_retries([work_ticket], work_ticket,
                                     done_ticket, e,
                                     pnfs_filename = work_ticket['outfile'])
        
        if e_errors.is_retriable(result_dict['status'][0]):
            close_descriptors(control_socket, data_path_socket)
            continue
        elif e_errors.is_non_retriable(result_dict['status'][0]):
            close_descriptors(control_socket, data_path_socket)
            return combine_dict(result_dict, work_ticket)
        else:
            in_fd = done_ticket['fd']

        Trace.message(TRANSFER_LEVEL, "Input file %s opened.   elapsed=%s" % 
                      (work_ticket['infile'],
                       time.time()-tinfo['encp_start_time']))

        #We need to stall the transfer until the mover is ready.
        done_ticket = stall_write_transfer(data_path_socket, e)
        
        if not e_errors.is_ok(done_ticket):
            #Make one last check of everything before entering transfer_file().
            # Only test control_socket if a known problem exists.  Otherwise,
            # for small files it is possible that a successful final dialog
            # message gets 'eaten' up.
            external_label = work_ticket.get('fc',{}).get('external_label',
                                                          None)
            result_dict = handle_retries([work_ticket], work_ticket,
                                         done_ticket, e,
                                         listen_socket = listen_socket,
                                         control_socket = control_socket,
                                         external_label = external_label)

            if e_errors.is_retriable(result_dict['status'][0]):
                close_descriptors(control_socket, data_path_socket, in_fd)
                continue
            elif e_errors.is_non_retriable(result_dict['status'][0]):
                close_descriptors(control_socket, data_path_socket, in_fd)
                return combine_dict(result_dict, work_ticket)

        lap_time = time.time() #------------------------------------------Start

        done_ticket = transfer_file(in_fd, data_path_socket,
                                    control_socket, work_ticket,
                                    tinfo, e)
        
        tstring = '%s_transfer_time' % work_ticket['unique_id']
        tinfo[tstring] = time.time() - lap_time #--------------------------End

        try:
            delete_at_exit.register_bfid(done_ticket['fc']['bfid'])
        except (IndexError, KeyError):
            pass
            #Trace.log(e_errors.WARNING, "unable to register bfid")
        
        Trace.message(TRANSFER_LEVEL, "Verifying %s transfer.  elapsed=%s" %
                      (work_ticket['outfile'],
                       time.time()-tinfo['encp_start_time']))

        #Don't need these anymore.
        close_descriptors(control_socket, data_path_socket, in_fd)

        #Verify that everything is ok on the mover side of the transfer.
        result_dict = handle_retries([work_ticket], work_ticket,
                                     done_ticket, e)
        
        if e_errors.is_retriable(result_dict['status'][0]):
            continue
        elif e_errors.is_non_retriable(result_dict['status'][0]):
            return done_ticket

        #Trace.message(TRANSFER_LEVEL, "File %s transfered.  elapsed=%s" %
        #              (done_ticket['outfile'],
        #               time.time()-tinfo['encp_start_time']))

        #Trace.message(TICKET_LEVEL, "FINAL DIALOG")
        #Trace.message(TICKET_LEVEL, pprint.pformat(done_ticket))

        #This function writes errors/warnings to the log file and puts an
        # error status in the ticket.
        check_crc(done_ticket, e) #Check the CRC.

        #Verify that the file transfered in tacted.
        result_dict = handle_retries([work_ticket], work_ticket,
                                     done_ticket, e)
        
        if e_errors.is_retriable(result_dict['status'][0]):
            continue
        elif e_errors.is_non_retriable(result_dict['status'][0]):
            return combine_dict(result_dict, work_ticket)

        #Update the last access and modification times respecively.
        update_times(done_ticket['infile'], done_ticket['outfile'])

        #Verify that setting times has been done successfully.
        #
        #Also verify, before calling set_pnfs_settings(), that another
        # encp has not yet set layer 1 or 4 (very small chance of race
        # condition that another encp would set them between this check
        # and them being set).
        result_dict = handle_retries([work_ticket], work_ticket,
                                     done_ticket, e,
                                     pnfs_filename = done_ticket['outfile'])

        if e_errors.is_retriable(result_dict['status'][0]):
            continue
        elif e_errors.is_non_retriable(result_dict['status'][0]):
            return combine_dict(result_dict, work_ticket)
        
        #We know the file has hit some sort of media. When this occurs
        # create a file in pnfs namespace with information about transfer.
        set_pnfs_settings(done_ticket, e)

        #Verify that the pnfs info was set correctly.
        result_dict = handle_retries([work_ticket], work_ticket,
                                     done_ticket, e)
        
        if e_errors.is_retriable(result_dict['status'][0]):
            clear_layers_1_and_4(done_ticket) #Reset this.
            continue
        elif e_errors.is_non_retriable(result_dict['status'][0]):
            clear_layers_1_and_4(done_ticket) #Reset this.
            return combine_dict(result_dict, work_ticket)

        #Set the UNIX file permissions.
        #Writes errors to log file.
        #The last peice of metadata that should be set is the filesize.  This
        # is done last inside of set_pnfs_settings().  Unfortunatly, write
        # permissions are needed to set the filesize.  If setting the
        # permissions goes first and write permissions are not included
        # in the values from the input file then the transer will fail.  Thus
        # setting the outfile permissions is done after setting the filesize,
        # however, if setting the permissions fails the file is left alone
        # but it is still treated like a failed transfer.  Worst case senerio
        # on a failure is that the file is left with full permissions.
        set_outfile_permissions(done_ticket)

        ###What kind of check should be done here?
        #This error should result in the file being left where it is, but it
        # is still considered a failed transfer (aka. exit code = 1 and
        # data access layer is still printed).
        if not e_errors.is_ok(done_ticket.get('status', (e_errors.OK,None))):
            print_data_access_layer_format(done_ticket['infile'],
                                           done_ticket['outfile'],
                                           done_ticket['file_size'],
                                           done_ticket)
            #We want to set this here, just in case the error isn't technically
            # non-retriable.
            done_ticket['completion_status'] = FAILURE

        tstring = '%s_overall_time' % done_ticket['unique_id']
        tinfo[tstring] = time.time() - overall_start #-------------Overall End

        #Remove the new file from the list of those to be deleted should
        # encp stop suddenly.  (ie. crash or control-C).
        try:
            delete_at_exit.unregister_bfid(done_ticket['fc']['bfid'])
        except (IndexError, KeyError):
            Trace.log(e_errors.INFO, "unable to unregister bfid")
        try:
            delete_at_exit.unregister(done_ticket['outfile']) #pnfsname
        except (IndexError, KeyError):
             Trace.log(e_errors.INFO, "unable to unregister file")
             
        Trace.message(TRANSFER_LEVEL,
                      "File status after verification: %s   elapsed=%s" %
                      (done_ticket['status'],
                       time.time()-tinfo['encp_start_time']))

        return done_ticket

    #If we get out of the while loop, then return error.
    msg = "Failed to write file %s." % work_ticket['outfile']
    done_ticket = {'status':(e_errors.TOO_MANY_RETRIES, msg)}
    return done_ticket

############################################################################

def write_to_hsm(e, tinfo):

    Trace.trace(16,"write_to_hsm input_files=%s  output=%s  verbose=%s  "
                "chk_crc=%s t0=%s" %
                (e.input, e.output, e.verbose, e.chk_crc,
                 tinfo['encp_start_time']))

    # initialize
    bytes = 0L #Sum of bytes all transfered (when transfering multiple files).
    exit_status = 0 #Used to determine the final message text.

    # get a port to talk on and listen for connections
    callback_addr, listen_socket = get_callback_addr()
    
    #If the sockets do not exist, do not continue.
    if listen_socket == None:
        done_ticket = {'status':(e_errors.NET_ERROR,
                                 "Unable to obtain control socket.")}
        return done_ticket, None

    #Build the dictionary, work_ticket, that will be sent to the
    # library manager.
    try:
        request_list = create_write_requests(callback_addr, None, e, tinfo)
    except (OSError, IOError, AttributeError, ValueError, EncpError), msg:
        if isinstance(msg, EncpError):
            e_ticket = msg.ticket
            if e_ticket.get('status', None) == None:
                e_ticket['status'] = (msg.type, str(msg))
        elif isinstance(msg, OSError):
            if msg.args[0] == getattr(errno, str("EFSCORRUPTED"), None) \
               or msg.args[0] == -1:
                e_ticket = {'status' : (e_errors.FILESYSTEM_CORRUPT, str(msg))}
            else:
                e_ticket = {'status' : (e_errors.OSERROR, str(msg))}
        elif isinstance(msg, IOError):
            e_ticket = {'status' : (e_errors.IOERROR, str(msg))}
        else:
            e_ticket = {'status' : (e_errors.WRONGPARAMETER, str(msg))}

        return e_ticket, None

    #If this is the case, don't worry about anything.
    if len(request_list) == 0:
        done_ticket = {'status' : (e_errors.NO_FILES, "No files to transfer.")}
        return done_ticket, request_list

    #This will halt the program if everything isn't consistant.

    try:
        verify_write_request_consistancy(request_list, e)
    except EncpError, msg:
        msg.ticket['status'] = (msg.type, msg.strerror)
        return msg.ticket, request_list

    #Determine the name of the library.
    check_lib = request_list[0]['vc']['library'] + ".library_manager"

    #If we are only going to check if we can succeed, then the last
    # thing to do is see if the LM is up and accepting requests.
    if e.check:
        try:
            return check_library(check_lib, e), request_list
        except EncpError, msg:
            return_ticket = { 'status'      : (msg.type, str(msg)),
                              'exit_status' : 2 }
            return return_ticket, request_list

    for i in range(len(request_list)):
        if e.put_cache or request_list[i].get('copy', None) != None:
            # We still need to get the inode.
            try:
                #Yet another os.stat() call.  In the future, need to work on
                # getting rid of as many of these as possible.
                pstat = os.stat(request_list[i]['wrapper']['pnfsFilename'])
                request_list[i]['wrapper']['inode'] = long(pstat[stat.ST_INO])
            except OSError:
                request_list[i]['wrapper']['inode'] = None
                
        else:
            #Create the zero length file entry and grab the inode.
            try:
                create_zero_length_pnfs_files(request_list[i], e)
            except OSError, msg:
                if msg.args[0] == getattr(errno, str("EFSCORRUPTED"), None) \
                       or (msg.args[0] == errno.EIO and \
                           msg.args[1].find("corrupt") != -1):
                    request_list[i]['status'] = \
                                   (e_errors.FILESYSTEM_CORRUPT, str(msg))
                else:
                    request_list[i]['status'] = \
                                   (e_errors.OSERROR, msg.strerror)
                return request_list[i], request_list

    work_ticket, index, copy = get_next_request(request_list)

    # loop on all input files sequentially
    while requests_outstanding(request_list):

        Trace.message(TO_GO_LEVEL,
                      "FILES LEFT: %s" % requests_outstanding(request_list))

        #Send (write) the file to the mover.
        done_ticket = write_hsm_file(listen_socket, work_ticket, tinfo, e)

        Trace.message(TICKET_LEVEL, "DONE WRITTING TICKET")
        Trace.message(TICKET_LEVEL, pprint.pformat(done_ticket))

        #Set the value of bytes to the number of bytes transfered before the
        # error occured.
        exfer_ticket = done_ticket.get('exfer', {'bytes_transfered' : 0L})
        bytes = bytes + exfer_ticket.get('bytes_transfered', 0L)

        #Store the combined tickets back into the master list.
        work_ticket = combine_dict(done_ticket, work_ticket)
        request_list[index] = work_ticket

        #handle_retries() is not required here since write_hsm_file()
        # handles its own retrying when an error occurs.
        if e_errors.is_ok(done_ticket):
            #Set completion status to successful.
            work_ticket['completion_status'] = SUCCESS
            #Store these changes back into the master list.
            request_list[index] = work_ticket
            
            #Pick up the next file.
            work_ticket, index, copy = get_next_request(request_list)

        elif e_errors.is_non_retriable(done_ticket):
            #Set completion status to successful.
            work_ticket['completion_status'] = FAILURE
            #Store these changes back into the master list.
            request_list[index] = work_ticket
            
            exit_status = 1

            #Pick up the next file.
            work_ticket, index, copy = get_next_request(request_list)
            
        if not e_errors.is_ok(done_ticket):
            continue

        # calculate some kind of rate - time from beginning 
        # to wait for mover to respond until now. This doesn't 
        # include the overheads before this, so it isn't a 
        # correct rate. I'm assuming that the overheads I've 
        # neglected are small so the quoted rate is close
        # to the right one.  In any event, I calculate an 
        # overall rate at the end of all transfers.
        calculate_rate(done_ticket, tinfo)

    # we are done transferring - close out the listen socket
    close_descriptors(listen_socket)

    #Print to screen the exit status.
    Trace.message(TO_GO_LEVEL, "EXIT STATUS: %d" % exit_status)

    #Finishing up with a few of these things.
    calc_ticket = calculate_final_statistics(bytes, len(request_list),
                                             exit_status, tinfo)

    #If applicable print new file family.
    if e.output_file_family and e_errors.is_ok(done_ticket):
        ff = string.split(done_ticket["vc"]["file_family"], ".")
        Trace.message(DONE_LEVEL, "New File Family Created: %s" % ff)

    #List one ticket is the last request that was processed.
    if e.data_access_layer:
        list_done_ticket = combine_dict(calc_ticket, done_ticket)
    else:
        list_done_ticket = combine_dict(calc_ticket, {})

    Trace.message(TICKET_LEVEL, "LIST DONE TICKET")
    Trace.message(TICKET_LEVEL, pprint.pformat(list_done_ticket))

    return list_done_ticket, request_list

#######################################################################
#Support function for reads.
#######################################################################

#######################################################################

# same_cookie(c1, c2) -- to see if c1 and c2 are the same

def same_cookie(c1, c2):
    lc_re = re.compile("[0-9]{4}_[0-9]{9,9}_[0-9]{7,7}")
    match1=lc_re.search(c1)
    match2=lc_re.search(c2)
    if match1 and match2:
        #The location cookie resembles that of null and tape cookies.
        #  Only the last section of the cookie is important.
        try: # just to be paranoid
            return string.split(c1, '_')[-1] == string.split(c2, '_')[-1]
        except (ValueError, AttributeError, TypeError, IndexError):
            return 0
    else:
        #The location cookie is a disk cookie.
        return c1 == c2

#Returns -1, 0 or 1 if request 1s cookie is less than, equal to or greater
# than request 2s cookie.
def sort_cookie(r1, r2):
    c1 = r1.get('fc', {}).get('location_cookie', "")
    c2 = r2.get('fc', {}).get('location_cookie', "")
    lc_re = re.compile("[0-9]{4}_[0-9]{9,9}_[0-9]{7,7}")
    match1=lc_re.search(c1)
    match2=lc_re.search(c2)
    if match1 and match2:
        #The location cookie resembles that of null and tape cookies.
        #  Only the last section of the cookie is important.
        try:
            #print cmp(int(string.split(c1, '_')[-1]),
            #           int(string.split(c2, '_')[-1])),
            #print int(string.split(c1, '_')[-1]), int(string.split(c2, '_')[-1])
            return cmp(int(string.split(c1, '_')[-1]),
                       int(string.split(c2, '_')[-1]))
        except (ValueError, AttributeError, TypeError, IndexError):
            return 0
    else:
        #The location cookie is a disk cookie.
        return 0

#Verifies that the state of the files, like existance and permissions,
# are accurate.
#def verify_read_file_consistancy(requests_per_vol, e):
#    vols = requests_per_vol.keys()
#    vols.sort()
#    for vol in vols:
#        request_list = requests_per_vol[vol]
#        for request in request_list:
#            inputfile_check(request['infile'], e)
#            outputfile_check(request['infile'], request['outfile'], e)
 
    
#Args:
# Takes in a dictionary of lists of transfer requests sorted by volume.
#Rerturns:
# None
#Verifies that various information in the tickets are correct, valid, spelled
# correctly, etc.
def verify_read_request_consistancy(requests_per_vol, e):

    bfid_brand = None
    sum_size = 0L
    sum_files = 0L
    outputfile_dict = {}
    p = Pnfs()
    
    vols = requests_per_vol.keys()
    vols.sort()
    for vol in vols:
        request_list = requests_per_vol[vol]

        try:
           #Only aquire the first loop.  This might be a performance hit for
           # a large number of requests otherwise.
           if not bfid_brand:
               bfid_brand = requests_per_vol[vol][0]['fc']['bfid']
        except (ValueError, AttributeError, TypeError,
                IndexError, KeyError), msg:
            pprint.pprint(msg)
            msg = "Error insuring consistancy with request list for " \
                  "volume %s." % (vol,)
            status = (e_errors.CONFLICT, msg)
            raise EncpError(None, str(msg), e_errors.CONFLICT,
                            {'status':status})
            #print_data_access_layer_format("", "", 0, {'status':status})
            #quit() #Harsh, but necessary.
            
        for request in request_list:

            #If the file was requested by BFID, then check if it is an
            # original or copy.
            is_copy = False
            if e.get_bfid:
                fcc = get_fcc()
                #Reminder: request['fc']['bfid'] would contains the copy bfid
                # if this is a copy, request['bfid'] always contain
                # the original bfid requested (which maybe original or copy).
                fcc_response = fcc.find_original(request['fc']['bfid'],
                                                 timeout=5, retry=2)
                if e_errors.is_ok(fcc_response):
                    if fcc_response['original'] != None:
                        is_copy = True

            if request['infile'] not in ["/dev/zero",
                                         "/dev/random", "/dev/urandom"]:
                try:
                    inputfile_check(request['infile'], e)
                except IOError, msg:
                    raise EncpError(msg.args, str(msg), e_errors.IOERROR,
                                    {'infile' : request['infile'],
                                     'outfile' : request['outfile']})
                except OSError, msg:
                    raise EncpError(msg.args, str(msg), e_errors.OSERROR,
                                    {'infile' : request['infile'],
                                     'outfile' : request['outfile']})
                
            if request['outfile'] not in ["/dev/null", "/dev/zero",
                                          "/dev/random", "/dev/urandom"]:
                if not request.get('local_inode', None):
                    try:
                        outputfile_check(request['infile'],
                                         request['outfile'], e)
                    except IOError, msg:
                        raise EncpError(msg.args, str(msg), e_errors.IOERROR,
                                        {'infile' : request['infile'],
                                         'outfile' : request['outfile']})
                    except OSError, msg:
                        raise EncpError(msg.args, str(msg), e_errors.OSERROR,
                                        {'infile' : request['infile'],
                                         'outfile' : request['outfile']})
                
                #This block of code makes sure the the user is not moving
                # two files with the same basename in different directories
                # into the same destination directory.
                result = outputfile_dict.get(request['outfile'], None)
                if result: 
                    #If the file is already in the list, give error.
                    raise EncpError(None,
                                    'Duplicate file entry: %s' % (result,),
                                    e_errors.USERERROR,
                                    {'infile' : request['infile'],
                                     'outfile' : request['outfile']})
                else:
                    #Put into one place all of the output names.  This is to
                    # check that two file to not have the same output name.
                    outputfile_dict[request['outfile']] = request['infile']

            try:
                #Verify that file clerk and volume clerk returned the same
                # external label.
                if request['vc']['external_label'] != \
                   request['fc']['external_label']:
                    raise EncpError(None,
                                    "Volume and file clerks returned " \
                                    "conflicting volumes. VC_V=%s  FC_V=%s" % \
                                    (request['vc']['external_label'],
                                     request['fc']['external_label']),
                                    e_errors.CONFLICT, request)
            except (ValueError, AttributeError, TypeError,
                    IndexError, KeyError), msg:
                raise EncpError(None,
                                "Unrecoverable read list consistancy error " \
                                "for volume %s on external_label check." %
                                (vol,), e_errors.KEYERROR, request)

            #First check if the file is deleted and the override deleted
            # switch/flag has been specified by the user.
            if not (e.override_deleted
                    and request['fc']['deleted'] != 'no'):
                
                #If no layer 4 is present, then report the error, raise an
                # alarm, but continue with the transfer.
                try:
                
                    #In case pnfs is automounted, first try this access_check()
                    # call to wait for the filesystem to mount.
                    access_check(request['wrapper']['pnfsFilename'], os.F_OK)
                    (pnfs_volume,
                     pnfs_location_cookie,
                     pnfs_size,
                     pnfs_origff,
                     pnfs_origname,
                     unused, #Mapfile is obsolete.
                     pnfsid_file,
                     unused, #Mapfile pnfsid is obsolete.
                     pnfs_bfid,
                     unused, #Drive is ignored by encp.
                     pnfs_crc) \
                    = p.get_xreference(request['wrapper']['pnfsFilename'])
                except (OSError, IOError), msg:
                    raise EncpError(getattr(msg, "errno", errno.EIO),
                                    str(msg), e_errors.PNFS_ERROR, request)

                #Raise an EncpError if the (input)file is larger than the
                # output filesystem supports.
                if not e.bypass_filesystem_max_filesize_check:
                    filesystem_check(request)

            #Determine if the pnfs layers and the file data are consistant.
            rest = {}
                
            #First check if the file is deleted and the override deleted
            # switch/flag has been specified by the user.
            if not (e.override_deleted
                    and request['fc']['deleted'] != 'no'):

                #Start by getting the pnfs layer 4 information.
                try:
                    #pnfs_volume = p.volume
                    if pnfs_volume == pnfs.UNKNOWN:
                        rest['pnfs_volume'] = p.volume
                except AttributeError:
                    pnfs_volume = None
                    rest['pnfs_volume'] = pnfs.UNKNOWN
                try:
                    #pnfs_location_cookie = p.location_cookie
                    if pnfs_location_cookie == pnfs.UNKNOWN:
                        rest['pnfs_location_cookie'] = p.location_cookie
                except AttributeError:
                    pnfs_location_cookie = None
                    rest['pnfs_location_cookie'] = pnfs.UNKNOWN
                try:
                    #pnfs_size = p.size
                    if pnfs_size == pnfs.UNKNOWN:
                        rest['pnfs_size'] = p.size
                    try:
                        pnfs_size = long(pnfs_size)
                    except TypeError:
                        rest['pnfs_size_type'] = \
                                        "pnfs_size contains wrong type %s." \
                                        % type(pnfs_size)
                except AttributeError:
                    pnfs_size = None
                    rest['pnfs_size'] = pnfs.UNKNOWN
                try:
                    #pnfs_origff = p.origff
                    if pnfs_origff == pnfs.UNKNOWN:
                        rest['pnfs_origff'] = p.origff
                except AttributeError:
                    pnfs_origff = None
                    rest['pnfs_origff'] = pnfs.UNKNOWN
                try:
                    #pnfs_origname = p.origname
                    if pnfs_origname == pnfs.UNKNOWN:
                        rest['pnfs_origname'] = p.origname
                except AttributeError:
                    pnfs_origname = None
                    rest['pnfs_origname'] = pnfs.UNKNOWN
                #Mapfile no longer used.
                try:
                    #pnfsid_file = p.pnfsid_file
                    if pnfsid_file == pnfs.UNKNOWN:
                        rest['pnfsid_file'] = p.pnfsid_file
                except AttributeError:
                    pnfsid_file = None
                    rest['pnfsid_file'] = pnfs.UNKNOWN
                #Volume map file id no longer used.
                try:
                    #pnfs_bfid = p.bfid
                    if pnfs_bfid == pnfs.UNKNOWN:
                        rest['pnfs_bfid'] = p.bfid
                except AttributeError:
                    pnfs_bfid = None
                    rest['pnfs_bfid'] = pnfs.UNKNOWN
                #Origdrive has not always been recored.
                try:
                    #pnfs_crc = p.crc
                    #CRC has not always been recored.
                    try:
                        if pnfs_crc != pnfs.UNKNOWN:
                            pnfs_crc = long(pnfs_crc)
                    except TypeError:
                        rest['pnfs_crc_type'] = \
                                          "pnfs_crc contains wrong type %s." \
                                          % type(pnfs_crc)
                except AttributeError:
                    pnfs_crc = None
                    rest['pnfs_crc'] = pnfs.UNKNOWN

            #Next get the database information.
            try:
                db_volume = request['fc']['external_label']
            except (ValueError, TypeError, IndexError, KeyError):
                db_volume = None
                rest['db_volume'] = pnfs.UNKNOWN
            try:
                db_location_cookie = request['fc']['location_cookie']
            except (ValueError, TypeError, IndexError, KeyError):
                db_location_cookie = None
                rest['db_location_cookie'] = pnfs.UNKNOWN
            try:
                db_size = request['fc']['size']
                try:
                    db_size = long(db_size)
                except TypeError:
                    rest['db_size_type'] = "db_size contains wrong type %s." \
                                           % type(db_size)
            except (ValueError, TypeError, IndexError, KeyError):
                db_size = None
                rest['db_size'] = pnfs.UNKNOWN
            try:
                db_volume_family = request['vc']['volume_family']
                try:
                    db_file_family = volume_family.extract_file_family(
                        db_volume_family)
                except TypeError:
                    rest['db_file_family_type'] = \
                               "db_file_family contains wrong type %s." % \
                               type(db_file_family)
            except (ValueError, TypeError, IndexError, KeyError):
                db_file_family = None
                rest['db_file_family'] = pnfs.UNKNOWN
            try:
                db_pnfs_name0 = request['fc']['pnfs_name0']
            except (ValueError, TypeError, IndexError, KeyError):
                db_pnfs_name0 = None
                rest['db_pnfs_name0'] = pnfs.UNKNOWN
            try:
                db_pnfsid = request['fc']['pnfsid']
            except (ValueError, TypeError, IndexError, KeyError):
                db_pnfsid = None
                rest['db_pnfsid'] = pnfs.UNKNOWN
            try:
                db_bfid = request['fc']['bfid']
            except (ValueError, TypeError, IndexError, KeyError):
                db_bfid = None
                rest['db_bfid'] = pnfs.UNKNOWN
            try:
                db_crc = request['fc']['complete_crc']
                #Some files do not have a crc recored.
                try:
                    if db_crc != None:
                        db_crc = long(db_crc)
                except TypeError:
                    rest['db_crc_type'] = "db_crc contains wrong type %s." \
                                          % type(db_crc)
            except (ValueError, TypeError, IndexError, KeyError):
                db_crc = None
                rest['db_crc'] = pnfs.UNKNOWN

            #If there is missing information, 
            if len(rest.keys()) > 0:
                conflict_ticket = {}
                conflict_ticket['infile'] = request['infile']
                conflict_ticket['outfile'] = request['outfile']
                conflict_ticket['bfid'] = request['bfid']
                conflict_ticket['conflict'] = rest

                Trace.alarm(e_errors.ERROR, e_errors.CONFLICT,
                            conflict_ticket)
                raise EncpError(None,
                           "Missing metadata information: %s" % str(rest),
                                e_errors.CONFLICT, request)

            #First check if the file is deleted and the override deleted
            # switch/flag has been specified by the user.
            #Not all fields get compared when reading a copy.
            if not (e.override_deleted
                    and request['fc']['deleted'] != 'no'):
                #For only those conflicting items, include them in
                # the dictionary.
                if db_volume != pnfs_volume and not is_copy:
                    rest['db_volume'] = db_volume
                    rest['pnfs_volume'] = pnfs_volume
                    rest['volume'] = "db_volume differs from pnfs_volume"
                if not same_cookie(db_location_cookie, pnfs_location_cookie) \
                       and not is_copy:
                    rest['db_location_cookie'] = db_location_cookie
                    rest['pnfs_location_cookie'] = pnfs_location_cookie
                    rest['location_cookie'] = "db_location_cookie differs " \
                                              "from pnfs_location_cookie"
                if db_size != pnfs_size:
                    rest['db_size'] = db_size
                    rest['pnfs_size'] = pnfs_size
                    rest['size'] = "db_size differs from pnfs_size"
                ##The file family check was removed for the migration project.
                #if db_file_family != pnfs_origff and not is_copy:
                #    rest['db_file_family'] = db_file_family
                #    rest['pnfs_origff'] = pnfs_origff
                #    rest['file_family'] = "db_file_family differs from " \
                #                          "pnfs_origff"
                if db_pnfs_name0 != pnfs_origname:
                    rest['db_pnfs_name0'] = db_pnfs_name0
                    rest['pnfs_origname'] = pnfs_origname
                    rest['filename'] = "db_pnfs_name0 differs from " \
                                       "pnfs_origname"
                #Mapfile no longer used.
                if db_pnfsid != pnfsid_file:
                    rest['db_pnfsid'] = db_pnfsid
                    rest['pnfsid_file'] = pnfsid_file
                    rest['pnfsid'] = "db_pnfsid differs from pnfsid_file"
                #Volume map file id no longer used.
                if db_bfid != pnfs_bfid and not is_copy:
                    rest['db_bfid'] = db_bfid
                    rest['pnfs_bfid'] = pnfs_bfid
                    rest['bfid'] = "db_bfid differs from pnfs_bfid"
                #Origdrive has not always been recored.
                if (pnfs_crc != pnfs.UNKNOWN) and (db_crc != None) \
                       and (db_crc != pnfs_crc):
                    #If present in layer 4 and file db compare the CRC too.
                    rest['db_crc'] = db_crc
                    rest['pnfs_crc'] = pnfs_crc
                    rest['crc'] = "db_crc differs from pnfs_crc"

                #If there is incorrect information.
                if len(rest.keys()) > 0:
                    conflict_ticket = {}
                    conflict_ticket['infile'] = request['infile']
                    conflict_ticket['outfile'] = request['outfile']
                    conflict_ticket['bfid'] = request['bfid']
                    conflict_ticket['conflict'] = rest

                    Trace.alarm(e_errors.ERROR, e_errors.CONFLICT,
                                conflict_ticket)
                    raise EncpError(None,
                        "Probable database conflict with pnfs: %s" % str(rest),
                                    e_errors.CONFLICT, request)

            #Test to verify that all the brands are the same.  If not exit.
            # If so, then the system will function.  If this was not true,
            # then a lot of file clerk key errors could occur.
            if extract_brand(db_bfid) != extract_brand(bfid_brand):
                msg = "All bfids must have the same brand."
                raise EncpError(None, str(msg), e_errors.USERERROR, request)

            #sum up the size to verify there is sufficent disk space.
            try:
                sum_size = sum_size + request['file_size']
            except TypeError:
                pass #If the size is not known (aka None) move on.

            #sum up the file count to verify there is sufficent disk space.
            sum_files = sum_files + 1L

        if request['outfile'] not in ["/dev/null", "/dev/zero",
                                      "/dev/random", "/dev/urandom"]:
            fs_stats = os.statvfs(get_directory_name(request['outfile']))

            #Make sure we won't exeed the maximum size of the filesystem.
            bytes_free = long(fs_stats[statvfs.F_BAVAIL]) * \
                         long(fs_stats[statvfs.F_FRSIZE])
            if  bytes_free < sum_size:
                message = \
                     "Disk is full.  %d bytes available for %d requested." % \
                     (bytes_free, sum_size)
                raise EncpError(None, str(message),
                                e_errors.USERERROR, request)

            #Make sure we won't exeed any quotas.
            fs_quotas = EXfer.quotas(request['outfile'])
            for quota in fs_quotas:
                #Transfer bytes into blocks first.
                sum_blocks = (sum_size / long(fs_stats[statvfs.F_BSIZE])) + 1

                #Test if we will exeed disk usage quota.
                if quota[EXfer.BLOCK_HARD_LIMIT] > 0L \
                       and quota[EXfer.CURRENT_BLOCKS] + sum_blocks > \
                       quota[EXfer.BLOCK_HARD_LIMIT]:
                    #User will exeed their quota.
                    message = "Quota (%d) would be exeeded (%d).  " % \
                              (quota[EXfer.BLOCK_HARD_LIMIT],
                               quota[EXfer.CURRENT_BLOCKS] + sum_blocks)
                    raise EncpError(None, str(message),
                                    e_errors.USERERROR, request)

                #Test if we will exeed file count quota.
                if quota[EXfer.FILE_HARD_LIMIT] > 0L \
                       and quota[EXfer.CURRENT_FILES] + sum_files > \
                       quota[EXfer.FILE_HARD_LIMIT]:
                    #User will exeed their file quota.
                    message = \
                          "File count quota (%d) would be exeeded (%d).  " % \
                          (quota[EXfer.FILE_HARD_LIMIT],
                           quota[EXfer.CURRENT_FILES] + sum_files)
                    raise EncpError(None, str(message),
                                    e_errors.USERERROR, request)

                #Test if we are near disk usage quota.
                if quota[EXfer.BLOCK_SOFT_LIMIT] \
                       and quota[EXfer.CURRENT_BLOCKS] + sum_blocks > \
                       quota[EXfer.BLOCK_SOFT_LIMIT]:
                    message = "WARNING: Transfer will exeed soft quota limit."
                    sys.stderr.write(message + "\n")

                #Test if we are near file count quota.
                if quota[EXfer.FILE_SOFT_LIMIT] \
                       and quota[EXfer.CURRENT_FILES] + sum_files > \
                       quota[EXfer.FILE_SOFT_LIMIT]:
                    message = "WARNING: Transfer will exeed soft quota limit."
                    sys.stderr.write(message + "\n")

#######################################################################

def get_file_clerk_info(bfid_or_ticket, encp_intf=None):
    #While __get_fcc() can accept None as the parameter value,
    # we expect that it will not be, since the purpose of
    # get_file_clerk_info() is to return the information about a bfid.

    #Get the clerk info.
    fcc, fc_ticket = __get_fcc(bfid_or_ticket)

    # Determine if the information returned is complete.
    if fc_ticket == None or not e_errors.is_ok(fc_ticket):
        fc_status = fc_ticket.get('status', (e_errors.EPROTO, None))
        fc_error_ticket = {'fc' : fc_ticket}

        if encp_intf != None and encp_intf.check \
           and e_errors.is_retriable(fc_ticket):
            #Should this only be for TIMEDOUT or all unknown errors?

            #We did not get the hard answer back that the bfid was not
            # found.  So, for these errors when --check is used only
            # also send back an exit status of 2.
            fc_error_ticket['exit_status'] = 2

        raise EncpError(None, fc_status[1], fc_status[0], fc_error_ticket)

    #We should fail the transfer if it has been marked for deletion.  The
    # exception remains that if the override flag has been specifed we
    # should ignore this test.
    if fc_ticket["deleted"] != "no" and not encp_intf.override_deleted:
        raise EncpError(None,
                        "File %s is marked %s." % (fc_ticket.get('pnfs_name0',
                                                                 "Unknown"),
                                                  e_errors.DELETED),
                        e_errors.DELETED, {'fc' : fc_ticket})

    #Include the server address in the returned info.
    fc_ticket['address'] = fcc.server_address

    return fc_ticket

def get_volume_clerk_info(volume_or_ticket, encp_intf=None):
    #While __get_vcc() can accept None as the parameter value,
    # we expect that it will not be, since the purpose of
    # get_volume_clerk_info() is to return the information about a bfid.
    
    #Get the clerk info.
    vcc, vc_ticket = __get_vcc(volume_or_ticket)

    # Determine if the information returned is complete.
    
    if vc_ticket == None or not e_errors.is_ok(vc_ticket):
        vc_status = vc_ticket.get('status', (e_errors.EPROTO, None))
        vc_error_ticket = {'vc' : vc_ticket}

        if encp_intf != None and encp_intf.check \
               and e_errors.is_retriable(vc_ticket):
            #Should this only be for TIMEDOUT or all unknown errors?

            #We did not get the hard answer back that the volume was not
            # found.  So, for these errors when --check is used only
            # also send back an exit status of 2.
            vc_error_ticket['exit_status'] = 2

        raise EncpError(None, vc_status[1], vc_status[0], vc_error_ticket)
    if not vc_ticket.get('system_inhibit', None):
        raise EncpError(None,
                        "Volume %s did not contain system_inhibit information."
                        % vc_ticket['external_label'],
                        e_errors.EPROTO, {'vc' : vc_ticket})
    if not vc_ticket.get('user_inhibit', None):
        raise EncpError(None,
                        "Volume %s did not contain user_inhibit information."
                        % vc_ticket['external_label'],
                        e_errors.EPROTO, {'vc' : vc_ticket})

    #Include the server address in the returned info.
    vc_ticket['address'] = vcc.server_address

    #Include this information in an easier to access location.
    try:
        vf = vc_ticket['volume_family']
        vc_ticket['storage_group']=volume_family.extract_storage_group(vf)
        vc_ticket['file_family'] = volume_family.extract_file_family(vf)
        vc_ticket['wrapper'] = volume_family.extract_wrapper(vf)
    except (ValueError, AttributeError, TypeError,
            IndexError, KeyError), msg:
        raise EncpError(None, str(msg), e_errors.KEYERROR, {'vc' : vc_ticket})

    # Determine if either the NOACCESS or NOTALLOWED inhibits are set for
    # the volume.  This is done after the above information is included
    # for the situation where the option to override a NOACCESS or
    # NOTALLOWED inhibit is set.
    
    inhibit = vc_ticket['system_inhibit'][0]
    if inhibit in (e_errors.NOACCESS, e_errors.NOTALLOWED):
        if encp_intf != None and encp_intf.check:
            vc_error_ticket = {'vc' : vc_ticket, 'exit_status' : 2}
        else:
            vc_error_ticket = {'vc' : vc_ticket}

        raise EncpError(None,
            "Volume %s is marked %s." % (vc_ticket['external_label'], inhibit),
                        inhibit, vc_error_ticket)

    inhibit = vc_ticket['user_inhibit'][0]
    if inhibit in (e_errors.NOACCESS, e_errors.NOTALLOWED):
        if encp_intf != None and encp_intf.check:
            vc_error_ticket = {'vc' : vc_ticket, 'exit_status' : 2}
        else:
            vc_error_ticket = {'vc' : vc_ticket}

        raise EncpError(None,
            "Volume %s is marked %s." % (vc_ticket['external_label'], inhibit),
                        inhibit, vc_error_ticket)

    return vc_ticket

def get_clerks_info(bfid, e):

    #Get the clerk info.  These functions raise EncpError on error.

    #For the file clerk.
    fc_ticket = get_file_clerk_info(bfid, encp_intf=e)

    #The volume clerk is much more complicated.  In some situations we
    # may wish to override the NOACCESS and NOTALLOWED system inhibts.
    try:
        vc_ticket = get_volume_clerk_info(fc_ticket['external_label'],
                                          encp_intf=e)
    except EncpError, msg:
        if msg.type in [e_errors.NOACCESS, e_errors.NOTALLOWED] \
               and e.override_noaccess:
            #If we get here, then we wish to override the NOACCESS or
            # NOTALLOWED inhibit.
            vc_ticket = msg.ticket
            vc_ticket['status'] = (e_errors.OK, None)
        else:
            #Otherwise re-raise the exception.
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
    
    #Return the information.
    return vc_ticket, fc_ticket


def create_read_requests(callback_addr, udp_callback_addr, tinfo, e):

    nfiles = 0
    requests_per_vol = {}
    csc = None
    p = Pnfs()

    # create internal list of input unix files even if just 1 file passed in
    if type(e.input)==types.ListType:
        pass #e.input = e.input
    else:
        e.input = [e.input]

    # create internal list of input unix files even if just 1 file passed in
    if type(e.output)==types.ListType:
        pass #e.output = e.output
    else:
        e.output = [e.output]

    # Paranoid check to make sure that the output has only one element.
    if len(e.output)>1:
        sys.stderr.write("%s %s\n" % e.output, type(e.output))
        raise EncpError(None,
                        'Cannot have multiple output files',
                        e_errors.USERERROR)

    if e.volume: #For reading a VOLUME (--volume).

        # get_clerks() can determine which it is and return the volume_clerk
        # and file clerk that it corresponds to.
        vcc, fcc = get_clerks(e.volume)

        #Just be sure that the output target is a directory.
        if not os.path.exists(e.output[0]):
            rest = {'volume':e.volume}
            raise EncpError(errno.ENOENT, e.output[0],
                            e_errors.USERERROR, rest)

        elif e.output[0] not in ["/dev/null", "/dev/zero",
                                 "/dev/random", "/dev/urandom"] \
                                 and not os.path.isdir(e.output[0]):
            rest = {'volume':e.volume}
            raise EncpError(errno.ENOTDIR, e.output[0],
                            e_errors.USERERROR, rest)                          

        #Make sure there is a valid volume clerk inquiry.
        vc_reply = get_volume_clerk_info(e.volume)

        #Make sure that the volume exists.
        if vc_reply['status'][0] == e_errors.NO_VOLUME:
            rest = {'volume':e.volume}
            raise EncpError(None, e.volume,
                            e_errors.NO_VOLUME, rest)
        #Address any other error.
        elif not e_errors.is_ok(vc_reply):
            if e.check:
                vc_reply['exit_status'] = 2
                
            raise EncpError(None, e.volume,
                            vc_reply['status'][0], vc_reply)

        Trace.message(TRANSFER_LEVEL, "Obtaining tape metadata.")
        sys.stdout.flush()
        
        #Obtain the complete listing of the volume.  It is best to do
        # this now as opposed to each iteration in the large while
        # loop below.
        try:
            tape_ticket = fcc.tape_list(e.volume)
        except (e_errors.TCP_EXCEPTION, select.error, socket.error), msg:
            tape_ticket = {'status' : (e_errors.NET_ERROR, str(msg))}
        
        #First check for errors.
        if not e_errors.is_ok(tape_ticket):
            rest = {'volume':e.volume}
            status = tape_ticket.get('status', (e_errors.BROKEN, "failed"))
            message = "Error obtaining tape listing: %s" % status[1]
            if e.check:
                rest['exit_status'] = 2
            raise EncpError(None, message, status[0], rest)

        #Set these here.  ("Get" with --list.)
        if e.list:
            #If a list was supplied, determine how many files are in the list.
            try:
                f = open(e.list, "r")
                list_of_files = f.readlines()
                number_of_files = len(list_of_files)
                f.close()
            except (IOError, OSError), msg:
                rest = {'volume':e.volume}
                raise EncpError(getattr(msg, "errno", errno.EIO),
                                "List file not accessable.",
                                e_errors.OSERROR, rest) 

        else:        
            number_of_files = len(tape_ticket['tape_list'])
            #Always say one (for the ticket to send to the LM) when the
            # number of files is unknown.
            if number_of_files == 0:
                number_of_files = 1
    else: # Normal read, --get-dcache, --put-cache, --get-bfid.
        number_of_files = len(e.input)

    # check the input unix file. if files don't exits, we bomb out to the user
    for i in range(number_of_files):

        # The following if...elif...else... statement needs to declare a set
        # of variables withing each branch to be used in constructing the
        # ticket later on.  They are:
        #
        # 1) ifullname - The full path of the input file.
        # 2) ofullname - The full path of the output file.
        # 3) file_size - The amount of bytes expected to transfer.  None if
        #                the number is unkown for some volume transfers (get).
        # 4) read_work
        # 5) fc_reply - The file data base entry for the file.  For some
        #               volume reads (get) fields maybe empty.
        # 6) vc_reply - Exception: Done before the loop on all volume reads.
        # 7) p - pnfs.Pnfs class instance
        # 8) bfid - The bfid of the transfer.

        #### VOLUME #######################################################
        #If the user specified a volume to read.
        if e.volume:

            # Check the file number on the tape to make sure everything is
            # correct.  This if...else... statement determines the next
            # fc_relply value from the tape listing.

            #If the list of files was passed in via --list, find the
            # correct file and move on.
            if getattr(e, "list", None):
                #Get the number and filename for the next line on the file.
                number, filename = list_of_files[i].split()[:2]
                #Massage the file posistion (aka file number) of the current
                # file on the tape and the name it will have.
                #These two variables should only be used withing the e.volume
                # if statement.  After that their use would break usability
                # with the other read method type branches.
                number = int(number)
                filename = os.path.basename(filename)
            
                Trace.message(TRANSFER_LEVEL,
                              "Preparing file number %s (%s) for transfer."
                              % (number, filename))

                #If everything is okay, search the listing for the location
                # of the file requested.  tape_ticket is used for performance
                # reasons over fcc.bfid_info().
                tape_list = tape_ticket.get("tape_list", [])
                for i in range(len(tape_list)):
                    #For each file number on the tape, compare it with
                    # a location cookie in the list of tapes.
                    if number == \
                       extract_file_number(tape_list[i]['location_cookie']):
                        #Make a copy so the following "del tape_list[i]" will
                        # not cause reference problems.
                        fc_reply = tape_list[i].copy()
                        #Include the server address in the returned info.
                        # Normally, get_file_clerk_info() would do this for us,
                        # but since we are using tape_ticket instead (for
                        # performance reasons) we need to set the address
                        # explicitly.
                        fc_reply['address'] = fcc.server_address
                        #Shrink the tape_ticket list for performance reasons.
                        del tape_list[i]
                        break
                else:
                    fc_reply = {
                        'address' : fcc.server_address,
                        'bfid' : None,
                        'complete_crc' : None,
                        'deleted' : None,
                        'drive' : None,
                        'external_label' : e.volume,
                        'location_cookie':generate_location_cookie(number),
                        'pnfs_mapname': None,
                        'pnfs_name0': os.path.join(e.input[0],
                                            generate_location_cookie(number)),
                        'pnfsid': None,
                        'pnfsvid': None,
                        'sanity_cookie': None,
                        'size': None,
                        'status' : (e_errors.OK, None)
                        }

            else: #No e.list from "get"'s --list switch.
                
                try:
                    fc_reply = tape_ticket.get("tape_list", [])[i]
                    #Include the server address in the returned info.
                    # Normally, get_file_clerk_info() would do this for us,
                    # but since we are using tape_ticket instead (for
                    # performance reasons) we need to set the address
                    # explicitly.
                    fc_reply['address'] = fcc.server_address
                except IndexError:
                    #We get here if bfids_list[] is empty.  It is empty when
                    # trying to read a volume with no known metadata (thus the
                    # IndexError when reading the zeroth element).
                    fc_reply = {'address' : fcc.server_address,
                                'bfid' : None,
                                'complete_crc' : None,
                                'deleted' : None,
                                'drive' : None,
                                'external_label' : e.volume,
                                'location_cookie':generate_location_cookie(1),
                                'pnfs_mapname' : None,
                                'pnfs_name0' : os.path.join(e.input[0],
                                                 generate_location_cookie(1)),
                                'pnfsid' : None,
                                'pnfsvid' : None,
                                'sanity_cookie' : None,
                                'size' : None,
                                'status' : (e_errors.OK, None)
                                }

                #Get the file posistion (aka file number) of the current
                # file on the tape and the name it will have.
                #These two variables should only be used withing the e.volume
                # if statement.  After that their use would break usability
                # with the other read method type branches.
                if is_location_cookie_disk(fc_reply['location_cookie']):
                    #i starts with 0.  To make the 1st file at location 1
                    # we add 1 to the locations.
                    number = i + 1
                else:
                    number = int(extract_file_number(fc_reply['location_cookie']))
                filename = os.path.basename(fc_reply['pnfs_name0'])

                Trace.message(TRANSFER_LEVEL,
                              "Preparing file number %s (%s) for transfer." %
                              (number, filename))

            #The database/file_clerk seems to return the stringified
            # None values rathur than an actual None value.  Fix them.
            if fc_reply.get('pnfs_name0', None) == "None":
                fc_reply['pnfs_name0'] = None
            if fc_reply.get('pnfsid', None) == "None":
                fc_reply['pnfs_id'] = None

            #Check to make sure that this file is not marked
            # as deleted.  If so, print error and exit.
            deleted_status = fc_reply.get('deleted', None)
            if deleted_status != None and deleted_status != "no":
                #If the user has specified the --skip-deleted-files switch
                # then ignore this file and move onto the next.
                if e.skip_deleted_files:
                    sys.stdout.write("Skipping deleted file (%s) %s.\n" %
                                     (number, filename))
                    continue
                else:
                    status = (e_errors.USERERROR,
                              "Requesting file (%s) that has been deleted."
                              % (generate_location_cookie(number),))
                    raise EncpError(None, status[1], status[0],
                                    {'volume' : e.volume})

            #These lines should NEVER give an error.
            bfid = fc_reply['bfid']
            lc = fc_reply['location_cookie']
            pnfs_name0 = fc_reply.get('pnfs_name0', None)
            pnfsid = fc_reply.get('pnfsid', None)

            #By this point pnfs_name0 should always have a non-None value.
            # Assuming this would make coding easier, but there is the case
            # were set_pnfs_metadata fails leaving the pnfs_name0 (and pnfsid)
            # field None while other values have been updated.

            ifullname = None
            if pnfs_name0 != None:
                stat_info = p.get_stat(pnfs_name0)
                if stat.S_ISREG(stat_info[stat.ST_MODE]):
                    ifullname = pnfs_name0

            if ifullname == None and pnfsid != None:
                try:
                    #Using the original directory as a starting point, try
                    # and determine the new file name/path/location.
                    orignal_directory = get_directory_name(pnfs_name0)
                    #Try to obtain the file name and path that the
                    # file currently has.
                    ifullname = p.get_path(pnfsid, orignal_directory)
                except (OSError, KeyError, AttributeError, ValueError):
                    sys.stdout.write("Location %s is active, but the "
                                     "file has been deleted.\n" % lc)

                    #Determine the inupt filename.
                    ifullname = pnfs_name0 #os.path.join(e.input[0], filename)

                    #Get the pnfs interface class instance.
                    #p = pnfs.Pnfs(ifullname)

            #If ifullname is still None, then the file does not exist
            # anywhere.  Use the correct name.
            if ifullname == None:
                ifullname = os.path.join(e.input[0], filename)

                #Get the pnfs interface class instance.
                #p = pnfs.Pnfs(ifullname)

            #Grab the stat info.
            istatinfo = p.get_stat(ifullname)
                
            #This is an attempt to deal with data that is incomplete
            # from a failed previous transfer.
            if fc_reply.get('pnfs_name0', None) == None:
                fc_reply['pnfs_name0'] = ifullname

            """
            #Determine the filesize.  None if non-existant.
            #file_size = get_file_size(ifullname)

            try:
                #We should call stat here and remember all the information.
                file_size = p.get_file_size()

                #Don't use p.get_file_size() to avoid a slow os.stat() call.
                #p.pstatinfo(update=0)
                #file_size = p.file_size
            except AttributeError:
                file_size = None
            """

            #Determine the output filename.
            if e.output[0] in ["/dev/null", "/dev/null",
                               "/dev/random", "/dev/urandom"]:
                ofullname = e.output[0]
            elif getattr(e, "sequential_filenames", None):
                #The user overrode "get" to use numbered filenames.
                ofullname = os.path.join(e.output[0], lc)
            else:
                ofullname = os.path.join(e.output[0], filename)

            read_work = 'read_from_hsm' #'read_tape'

        #### BFID #######################################################
        #If the user specified a bfid for the file to read.
        elif e.get_bfid:

            #Get the system information from the clerks.  In this case
            # e.input[i] doesn't contain the filename, but the bfid.

            vc_reply, fc_reply = get_clerks_info(e.get_bfid, e)

            pnfsid = fc_reply.get("pnfsid", None)
            if not pnfsid:
                #In the case that this is an overridding read of a deleted
                # file don't give the error.
                if not (e.override_deleted
                        and fc_reply['deleted'] != 'no'):
                    raise EncpError(None,
                                    "Unable to obtain pnfsid from file clerk.",
                                    e_errors.CONFLICT,
                                    {'fc' : fc_reply, 'vc' : vc_reply})

            if e.shortcut and e.override_path:
                #If the user specified a pathname (with --override-path)
                # on the command line use that name.  Otherwise if just
                # --shortcut is used, the filename name will be recored as
                # a /.(access)(<pnfsid>) style filename.
                ifullname = e.override_path
            elif e.override_deleted and fc_reply['deleted'] != "no":
                #Handle reading deleted files differently.
                ifullname = fc_reply['pnfs_name0']
            else:
                if e.pnfs_mount_point:
                    use_mount_point = e.pnfs_mount_point
                else:
                    use_mount_point = os.path.dirname(fc_reply['pnfs_name0'])
                ifullname = p.get_path(pnfsid, use_mount_point,
                                       shortcut = e.shortcut)

            if e.output[0] in ["/dev/null", "/dev/zero",
                               "/dev/random", "/dev/urandom"]:
                ofullname = e.output[0]
            else:
                unused, ofullname, unused, unused = fullpath(e.output[0])

            #file_size = long(fc_reply['size'])
            #Grab the stat info.
            if e.override_deleted and fc_reply['deleted'] != 'no' \
                   and not ifullname:
                #This if protects us in the case where we are reading
                # a deleted file using the bfid.
                istatinfo = None
            else:
                istatinfo = p.get_stat(ifullname)

            bfid = e.get_bfid

            read_work = 'read_from_hsm'

        #### PNFS ID ###################################################
        elif e.get_cache:
            
            if e.shortcut and e.override_path:
                #If the user specified a pathname (with --override-path)
                # on the command line use that name.  Otherwise if just
                # --shortcut is used, the filename name will be recored as
                # a /.(access)(<pnfsid>) style filename.
                ifullname = e.override_path
            else:
                ifullname = p.get_path(e.get_cache, e.pnfs_mount_point,
                                       shortcut=e.shortcut)
            
            #file_size = get_file_size(ifullname)
            #Grab the stat info.
            istatinfo = p.get_stat(ifullname)
            
            bfid = p.get_bit_file_id(ifullname)

            vc_reply, fc_reply = get_clerks_info(bfid, e)

            if e.output[0] in ["/dev/null", "/dev/zero",
                               "/dev/random", "/dev/urandom"]:
                ofullname = e.output[0]
            else:
                unused, ofullname, unused, unused = fullpath(e.output[0])

            read_work = 'read_from_hsm'
            
        #### FILENAME #################################################
        #else the filename was given to encp.
        else:

            #Fundamentally this belongs in verify_read_request_consistancy(),
            # but information needed about the input file requires this check.
            try:
                istatinfo = p.get_stat(e.input[i])
                ifullname = e.input[i]
            except (KeyboardInterrupt, SystemExit):
                raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
            except (OSError, IOError):
                ifullname = get_ininfo(e.input[i])
                istatinfo = p.get_stat(ifullname)

            #inputfile_check(ifullname, e)

            ofullname = get_oninfo(e.input[i], e.output[0], e)
            #if ofullname not in ["/dev/null", "/dev/zero",
            #                     "/dev/random", "/dev/urandom"]:
                #The existence rules behind the output file are more
                # complicated than those of the input file.  We always need
                # to call outputfile_check.  It still should go in
                # some verify function though.
            #Fundamentally this belongs in verify_read_request_consistancy(),
            # but information needed about the input file requires this check.
            #    outputfile_check(ifullname, ofullname, e)

            #file_size = long(istatinfo[stat.ST_SIZE])
            #if file_size == 1L:
            #    #If this is likely a large file, we need to do this the
            #    # hard way.
            #    file_size = get_file_size(ifullname)

            #if e.intype == RHSMFILE:
            #    p = get_pac()
            #    bfid = p.get_bit_file_id(ifullname)
            #else:  #e.intype == HSMFILE
            #    p = pnfs.Pnfs(ifullname)
            #    bfid = p.get_bit_file_id()

            try:
                bfid = p.get_bit_file_id(ifullname)
            except IOError, msg:
                #If this didn't exist, we should return the correct name.
                if msg.args[0] == errno.ENOENT:
                    raise IOError(errno.ENOENT, "%s: %s" % \
                                  (os.strerror(errno.ENOENT), ifullname))
            vc_reply, fc_reply = get_clerks_info(bfid, e)

            read_work = 'read_from_hsm'

        ##################################################################

        #Get these two pieces of information about the local input file.
        file_size = fc_reply.get('size', None)
        if type(file_size) != types.NoneType:
            file_size = long(file_size)

        #Print out the replies from the cerks.
        Trace.message(TICKET_LEVEL, "FILE CLERK:")
        Trace.message(TICKET_LEVEL, pprint.pformat(fc_reply))
        Trace.message(TICKET_LEVEL, "VOLUME CLERK:")
        Trace.message(TICKET_LEVEL, pprint.pformat(vc_reply))

        try:
            label = fc_reply['external_label'] #short cut for readablility
        except (KeyError, ValueError, TypeError, AttributeError, IndexError):
            raise EncpError(None,
                            "File clerk resonce did not contain an " \
                            "external label.",
                            e_errors.KEYERROR,
                            {'fc' : fc_reply, 'vc' : vc_reply,
                             'infile' : ifullname, 'outfile' : ofullname,
                             'file_size' : file_size})

        try:
            # comment this out not to confuse the users
            #if fc_reply.has_key("fc") or fc_reply.has_key("vc"):
            #    sys.stderr.write("Old file clerk format detected.\n")
            del fc_reply['fc'] #Speed up debugging by removing these.
            del fc_reply['vc']
        except (KeyboardInterrupt, SystemExit):
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except:
            pass

        #Get the data aquisition information.
        encp_daq = get_dinfo()

        #Snag the three pieces of information needed for the wrapper.
        uinfo = get_uinfo()
        finfo = get_finfo(ifullname, ofullname, e)
        pinfo = get_minfo(istatinfo)
        finfo['size_bytes'] = file_size

        #When the file has been deleted, we need to make up some values
        # in order to read the file.  Otherwise the LM will reject the
        # transfer request.
        if e.override_deleted and fc_reply.get('deleted', None) != "no":
            pinfo['pnfsFilename'] = fc_reply.get('pnfs_name0', None)
            #pinfo['major'] = 0
            #pinfo['minor'] = 0
            #pinfo['mode'] = 0
        
        #Combine the data into the wrapper sub-ticket.
        wrapper = get_winfo(pinfo, uinfo, finfo)

        #Create the sub-ticket of the command line argument information.
        encp_el = get_einfo(e)

        #There is no need to deal with routing on non-multihomed machines.
        #config = host_config.get_config()
        #if config and config.get('interface', None):
        #    route_selection = 0  #1
        #else:
        #    route_selection = 0
        #route_selection = 0  #1 to use udp_server, 0 for no.

        # allow library manager selection based on the environment variable
        lm = os.environ.get('ENSTORE_SPECIAL_LIB')
        if lm != None:
            vc_reply['library'] = lm

        if not csc:
            csc = get_csc() #Get csc once for max_attempts().
        #Determine the max resend values for this transfer.
        resend = max_attempts(csc, vc_reply['library'], e)

        request = {}
        request['bfid'] = bfid
        request['callback_addr'] = callback_addr
        request['client_crc'] = e.chk_crc
        request['encp'] = encp_el
        request['encp_daq'] = encp_daq
        request['fc'] = fc_reply
        request['file_size'] = file_size
        request['infile'] = ifullname
        request['outfile'] = ofullname
        #request['override_noaccess'] = e.override_noaccess #no to this
        request['override_ro_mount'] = e.override_ro_mount
        request['resend'] = resend
        #request['retry'] = 0
        if udp_callback_addr: #For "get" only.
            request['routing_callback_addr'] = udp_callback_addr
            request['route_selection'] = 1
        request['times'] = tinfo.copy() #Only info now in tinfo needed.
        request['unique_id'] = generate_unique_id()
        request['vc'] = vc_reply
        request['version'] = encp_client_version()
        request['volume'] = label
        request['work'] = read_work #'read_from_hsm' or 'read_tape'
        request['wrapper'] = wrapper

        requests_per_vol[label] = requests_per_vol.get(label,[]) + [request]
        nfiles = nfiles+1

        #When output is redirected to a file, sometimes it needs a push
        # to get there.
        sys.stdout.flush()
        sys.stderr.flush()
    
    return requests_per_vol

#######################################################################

# submit read_from_hsm requests
def submit_read_requests(requests, encp_intf):
    submitted = 0

    #Sort the requests by location cookie.
    requests.sort(sort_cookie)

    for req in requests:
        #After enough failures handle_retries() will eventually abort
        # the transfer.
        
        while req.get('completion_status', None) == None:
            #Trace.message(TRANSFER_LEVEL, 
            #         "Submitting %s read request.  elapsed=%s" % \
            #         (req['outfile'], time.time() - tinfo['encp_start_time']))

            Trace.trace(18, "submit_read_requests queueing:%s"%(req,))
            
            ticket = submit_one_request(req, encp_intf)
            
            Trace.message(TICKET_LEVEL, "LIBRARY MANAGER")
            Trace.message(TICKET_LEVEL, pprint.pformat(ticket))
            
            result_dict = handle_retries(requests, req, ticket, encp_intf)
            
            if e_errors.is_retriable(result_dict):
                continue
            elif e_errors.is_non_retriable(result_dict):
                req['completion_status'] = FAILURE
                return submitted, ticket
            else:
                submitted = submitted + 1
                break

    Trace.message(TO_GO_LEVEL, "SUBMITED: %s" % submitted)
    Trace.message(TICKET_LEVEL, pprint.pformat(requests))

    Trace.message(TRANSFER_LEVEL, "Files queued." + elapsed_string())

    return submitted, ticket

#######################################################################

def stall_read_transfer(data_path_socket, work_ticket, e):
    #Stall starting the count until the first byte is ready for reading.
    duration = e.mover_timeout
    while 1:
        start_time = time.time()
        try:
            read_fd, unused, unused = select.select([data_path_socket],
                                                    [], [], duration)
            break
        except (select.error, socket.error), msg:
            if msg.args[0] == errno.EINTR or msg.args[0] == errno.EAGAIN:
                #If the select was interupted by a signal, keep going.
                duration = duration - (time.time() - start_time)
                continue
            else:
                read_fd = []
                break

    if data_path_socket not in read_fd:
        status_ticket = {'status' : (e_errors.UNKNOWN,
                                     "No data read from mover.")}
        return status_ticket

    if work_ticket['file_size'] == 0:
        #In the odd case that the file is of zero length, we would expect
        # that the socket buffer be empty.
        target_size = 0
    elif work_ticket['file_size'] == None:
        #This case happens with get transfers without known metadata.
        target_size = None
    else:
        target_size = 1

    #If there is no data waiting in the buffer, we have an error.
    if target_size == None:
        status_ticket = {'status' : (e_errors.OK, None)}
    elif len(data_path_socket.recv(1, socket.MSG_PEEK)) != target_size:
        status_ticket = {'status' : (e_errors.UNKNOWN,
                                     "No data read from mover.")}
    else:
        status_ticket = {'status' : (e_errors.OK, None)}

    return status_ticket

#############################################################################

# read hsm files in the loop after read requests have been submitted
#Args:
# listen_socket - The socket to listen on returned from callback.get_callback()
# submittted - The number of elements of the list requests thatwere
#  successfully transfered.
# requests - A list of dictionaries.  Each dictionary represents on transfer.
# tinfo - Dictionary of timing info.
# e - class instance of the interface.
#Rerturns:
# (requests, bytes) - requests returned only contains those requests that
#   did not succeed.  bytes is the total running sum of bytes transfered
#   for this encp.

def read_hsm_file(listen_socket, request_list, tinfo, e):

    for rq in request_list: 
        Trace.trace(17,"read_hsm_file: %s"%(rq['infile'],))

    #Wait for the mover to establish the control socket.  See if the
    # id matches one of the tickets we submitted.  Establish data socket
    # connection with the mover.
    control_socket, data_path_socket, request_ticket = mover_handshake(
        listen_socket, request_list, e)

    overall_start = time.time() #----------------------------Overall Start

    done_ticket = request_ticket #Make sure this exists by this point.
    local_filename = request_ticket.get('wrapper',{}).get('fullname', None)
    external_label = request_ticket.get('fc',{}).get('external_label',None)
    result_dict = handle_retries(request_list, request_ticket,
                                 request_ticket, e,
                                 listen_socket = listen_socket,
                                 local_filename = local_filename,
                                 external_label = external_label)

    if not e_errors.is_ok(result_dict):
        return combine_dict(result_dict, request_ticket)


    #Be paranoid.  Check this the ticket again.
    #Dmitry is not paranoid
    #r_encp = os.environ.get('REMOTE_ENCP')
    r_encp = None #Dmitry should know better than to not be paranoid.
    if r_encp == None:
        try:
            if not e.volume: #Skip this test for volume transfers.
                verify_read_request_consistancy(
                    {request_ticket.get("external_label","label"):[request_ticket]}, e)
        except EncpError, msg:
            msg.ticket['status'] = (msg.type, msg.strerror)
            result_dict = handle_retries(request_list, msg.ticket,
                                     msg.ticket, e)

            if not e_errors.is_ok(result_dict):
                close_descriptors(control_socket, data_path_socket)
                return combine_dict(result_dict, request_ticket)
            
        
        
    Trace.message(TRANSFER_LEVEL, "Mover called back.  elapsed=%s" %
                  (time.time() - tinfo['encp_start_time'],))
    Trace.message(TICKET_LEVEL, "REQUEST:")
    Trace.message(TICKET_LEVEL, pprint.pformat(request_ticket))

    #Open the output file.
    done_ticket = open_local_file(request_ticket, e)

    result_dict = handle_retries(request_list, request_ticket,
                                 done_ticket, e)
                                 #listen_socket = listen_socket,
                                 #udp_server = route_server)

    if not e_errors.is_ok(result_dict):
        close_descriptors(control_socket, data_path_socket)
        return combine_dict(result_dict, request_ticket)
    else:
        out_fd = done_ticket['fd']
    
    Trace.message(TRANSFER_LEVEL, "Output file %s opened.  elapsed=%s" %
              (request_ticket['outfile'],
               time.time() - tinfo['encp_start_time']))

    #We need to stall the transfer until the mover is ready.
    done_ticket = stall_read_transfer(data_path_socket, request_ticket, e)

    if not e_errors.is_ok(done_ticket):
        #Make one last check of everything before entering transfer_file().
        # Only test control_socket if a known problem exists.  Otherwise,
        # for small files it is possible that a successful final dialog
        # message gets 'eaten' up.
        
        external_label = request_ticket.get('fc',{}).get('external_label',
                                                         None)
        result_dict = handle_retries([request_ticket], request_ticket,
                                     done_ticket, e,
                                     #listen_socket = listen_socket,
                                     #udp_server = route_server,
                                     control_socket = control_socket,
                                     external_label = external_label)

        if not e_errors.is_ok(result_dict):
            close_descriptors(control_socket, data_path_socket, out_fd)
            return combine_dict(result_dict, request_ticket)
        
    lap_start = time.time() #----------------------------------------Start

    done_ticket = transfer_file(data_path_socket, out_fd,
                                control_socket, request_ticket,
                                tinfo, e)
    
    lap_end = time.time()  #-----------------------------------------End
    tstring = "%s_transfer_time" % request_ticket['unique_id']
    tinfo[tstring] = lap_end - lap_start

    Trace.message(TRANSFER_LEVEL, "Verifying %s transfer.  elapsed=%s" %
                  (request_ticket['infile'],
                   time.time() - tinfo['encp_start_time']))

    #Verify that everything went ok with the transfer.
    result_dict = handle_retries(request_list, request_ticket,
                                 done_ticket, e)
                                 #listen_socket = listen_socket,

    if not e_errors.is_ok(result_dict):
        #Close these before they are forgotten.
        close_descriptors(control_socket, data_path_socket, out_fd)
        return combine_dict(result_dict, request_ticket)
    
    #These functions write errors/warnings to the log file and put an
    # error status in the ticket.

    #Verify size is the same.
    verify_file_size(done_ticket, e)

    result_dict = handle_retries(request_list, request_ticket,
                                 done_ticket, e)

    if not e_errors.is_ok(result_dict):
        #Close these before they are forgotten.
        close_descriptors(control_socket, data_path_socket, out_fd)
        return combine_dict(result_dict, request_ticket)

    #Check the CRC.
    check_crc(done_ticket, e, out_fd)

    result_dict = handle_retries(request_list, request_ticket,
                                 done_ticket, e)

    if not e_errors.is_ok(result_dict):
        #Close these before they are sforgotten.
        close_descriptors(control_socket, data_path_socket, out_fd)
        return combine_dict(result_dict, request_ticket)

    #If no error occured, this is safe to close now.
    close_descriptors(control_socket, data_path_socket, out_fd)

    #Update the last access and modification times respecively.
    update_times(done_ticket['infile'], done_ticket['outfile'])

    #If this is a read of a deleted file, leave the outfile permissions
    # to the defaults.
    if not (e.override_deleted and done_ticket['fc']['deleted'] != 'no'):
        #This function writes errors/warnings to the log file and puts an
        # error status in the ticket.
        set_outfile_permissions(done_ticket) #Writes errors to log file.
    ###What kind of check should be done here?
    #This error should result in the file being left where it is, but it
    # is still considered a failed transfer (aka. exit code = 1 and
    # data access layer is still printed).
    if not e_errors.is_ok(done_ticket.get('status', (e_errors.OK,None))):
        print_data_access_layer_format(done_ticket['infile'],
                                       done_ticket['outfile'],
                                       done_ticket['file_size'],
                                       done_ticket)
        #We want to set this here, just in case the error isn't technically
        # non-retriable.
        done_ticket['completion_status'] = FAILURE

    #Remove the new file from the list of those to be deleted should
    # encp stop suddenly.  (ie. crash or control-C).
    delete_at_exit.unregister(done_ticket['outfile']) #localname

    # remove file requests if transfer completed succesfuly.
    #del(request_ticket)
    #del request_list[request_list.index(request_ticket)]
    #if files_left > 0:
    #    files_left = files_left - 1

    tstring = "%s_overall_time" % done_ticket['unique_id']
    tinfo[tstring] = time.time() - overall_start #-------------Overall End

    Trace.message(TRANSFER_LEVEL,
                  "File status after verification: %s   elapsed=%s" %
                  (done_ticket["status"],
                   time.time() - tinfo['encp_start_time']))

    # calculate some kind of rate - time from beginning to wait for
    # mover to respond until now. This doesn't include the overheads
    # before this, so it isn't a correct rate. I'm assuming that the
    # overheads I've neglected are small so the quoted rate is close
    # to the right one.  In any event, I calculate an overall rate at
    # the end of all transfers.
    calculate_rate(done_ticket, tinfo)

    #With the transfer a success, we can now add the ticket to the list
    # of succeses.
    #succeeded_requests.append(done_ticket)

    Trace.message(TICKET_LEVEL, "DONE TICKET")
    Trace.message(TICKET_LEVEL, pprint.pformat(done_ticket))

    #Pull out the failed transfers that occured while trying to open the
    # control socket.
    #unknown_failed_transfers = []
    #for transfer in failed_requests:
    #    if len(transfer) < 3:
    #        unknown_failed_transfers.append(transfer)

    #Extract the unique ids for the two lists.
    #succeeded_ids = []
    #failed_ids = []
    #for req in succeeded_requests:
    #    try:
    #        succeeded_ids.append(req['unique_id'])
    #    except KeyError:
    #        sys.stderr.write("Error obtaining unique id list of successes.\n")
    #        sys.stderr.write(pprint.pformat(req) + "\n")
    #for req in failed_requests:
    #    try:
    #        failed_ids.append(req['unique_id'])
    #    except KeyError:
    #        sys.stderr.write("Error obtaining unique id list of failures.\n")
    #        sys.stderr.write(pprint.pformat(req) + "\n")

    #For each transfer that failed without even succeeding to open a control
    # socket, print out their data access layer.
    #for transfer in request_list:
    #    if transfer['unique_id'] not in succeeded_ids and \
    #       transfer['unique_id'] not in failed_ids:
    #        try:
    #            transfer = combine_dict(unknown_failed_transfers[0], transfer)
    #            del unknown_failed_transfers[0] #shorten this list.
    #
    #            print_data_access_layer_format(transfer['infile'],
    #                                           transfer['outfile'],
    #                                           transfer['file_size'],
    #                                           transfer)
    #        except IndexError, msg:
    #            #msg = "Unable to print data access layer.\n"
    #            sys.stderr.write(str(msg)+"\n")

    #return failed_requests, bytes, done_ticket
    return done_ticket

#######################################################################

def read_from_hsm(e, tinfo):

    Trace.trace(16,"read_from_hsm input_files=%s  output=%s  verbose=%s  "
                "chk_crc=%s t0=%s" % (e.input, e.output, e.verbose,
                                      e.chk_crc, tinfo['encp_start_time']))
    
    # initialize
    bytes = 0L #Sum of bytes all transfered (when transfering multiple files).
    exit_status = 0 #Used to determine the final message text.
    number_of_files = 0 #Total number of files where a transfer was attempted.

    # get a port to talk on and listen for connections
    callback_addr, listen_socket = get_callback_addr()  #e)
    #Get an ip and port to listen for the mover address for routing purposes.
    #udp_callback_addr, udp_server = get_udp_callback_addr(e)

    #If the sockets do not exist, do not continue.
    if listen_socket == None:
        done_ticket = {'status':(e_errors.NET_ERROR,
                                 "Unable to obtain control socket.")}
        return done_ticket, None
    #if udp_server.server_socket == None:
    #    done_ticket = {'status':(e_errors.NET_ERROR,
    #                             "Unable to obtain udp socket.")}
    #    return done_ticket
    
    #Create all of the request dictionaries.
    try:
        #requests_per_vol = create_read_requests(callback_addr,
        #                                        udp_callback_addr, tinfo, e)
        requests_per_vol = create_read_requests(callback_addr, None, tinfo, e)
    except (OSError, IOError, AttributeError, ValueError, EncpError), msg:
        if isinstance(msg, EncpError):
            e_ticket = msg.ticket
            e_ticket['status'] = (msg.type, str(msg))
        elif isinstance(msg, OSError):
            if msg.args[0] == getattr(errno, str("EFSCORRUPTED"), None) \
                   or (msg.args[0] == errno.EIO and \
                       msg.args[1].find("corrupt") != -1):
                e_ticket = {'status' : (e_errors.FILESYSTEM_CORRUPT, str(msg))}
            else:
                e_ticket = {'status' : (e_errors.OSERROR, str(msg))}
        elif isinstance(msg, IOError):
            e_ticket = {'status' : (e_errors.IOERROR, str(msg))}
        else:
            e_ticket = {'status' : (e_errors.WRONGPARAMETER, str(msg))}

        return e_ticket, None

    #If this is the case, don't worry about anything.
    if (len(requests_per_vol) == 0):
        done_ticket = {'status' : (e_errors.NO_FILES, "No files to transfer.")}
        return done_ticket, requests_per_vol

    #This will halt the program if everything isn't consistant.
    try:
        if not e.volume: #Skip these tests for volume transfers.
            verify_read_request_consistancy(requests_per_vol, e)
    except EncpError, msg:

        if not msg.ticket.get('status', None):
            msg.ticket['status'] = (msg.type, msg.strerror)
        return msg.ticket, requests_per_vol

    #Determine the name of the library.
    check_lib = requests_per_vol.values()[0][0]['vc']['library'] + \
                ".library_manager"

    #If we are only going to check if we can succeed, then the last
    # thing to do is see if the LM is up and accepting requests.
    if e.check:
        try:
            return check_library(check_lib, e), requests_per_vol
        except EncpError, msg:
            return_ticket = { 'status'      : (msg.type, str(msg)),
                              'exit_status' : 2 }
            return return_ticket, requests_per_vol

    #Create the zero length file entry.
    for vol in requests_per_vol.keys():
        #for request in requests_per_vol[vol]:
        for i in range(len(requests_per_vol[vol])):
            try:
               create_zero_length_local_files(requests_per_vol[vol][i])
            except OSError, msg:
                if msg.args[0] == getattr(errno, str("EFSCORRUPTED"), None) \
                       or (msg.args[0] == errno.EIO and \
                           msg.args[1].find("corrupt") != -1):
                    requests_per_vol[vol][i]['status'] = \
                                 (e_errors.FILESYSTEM_CORRUPT, str(msg))
                else:
                    requests_per_vol[vol][i]['status'] = \
                                 (e_errors.OSERROR, str(msg))
                
                return requests_per_vol[vol][i], requests_per_vol
    
    # loop over all volumes that are needed and submit all requests for
    # that volume. Read files from each volume before submitting requests
    # for different volumes.
    
    vols = requests_per_vol.keys()
    vols.sort()
    for vol in vols:
        request_list = requests_per_vol[vol]
        number_of_files = number_of_files + len(request_list)

        #The return value is the number of files successfully submitted.
        # This value may be different from len(request_list).  The value
        # of request_list is not changed by this function.
        submitted, reply_ticket = submit_read_requests(request_list, e)

        #If at least one submission succeeded, follow through with it.
        if submitted > 0:
            while requests_outstanding(request_list):
                #Since request_list contains all of the entires, submitted must
                # also be passed so read_hsm_files knows how many elements of
                # request_list are valid.
                done_ticket = read_hsm_file(
                    listen_socket, request_list, tinfo, e)

                #Sum up the total amount of bytes transfered.
                exfer_ticket = done_ticket.get('exfer',
                                               {'bytes_transfered' : 0L})
                bytes = bytes + exfer_ticket.get('bytes_transfered', 0L)

                index, unused = get_request_index(request_list, done_ticket)

                #handle_retries() is not required here since write_hsm_file()
                # handles its own retrying when an error occurs.
                if e_errors.is_ok(done_ticket):
                    if index == None:
                        #How can we succed at a transfer, that is not in the
                        # request list?
                        message = "Successfully transfered a file that " \
                                  "is not in the file transfer list."
                        sys.stderr.write(message + "\n")
                        Trace.log(e_errors.ERROR,
                                  message + "  " + str(done_ticket))
                        
                    else:
                        #Combine the dictionaries.
                        work_ticket = combine_dict(done_ticket,
                                                   request_list[index])
                        #Set completion status to successful.
                        work_ticket['completion_status'] = SUCCESS
                        #Store these changes back into the master list.
                        request_list[index] = work_ticket

                elif e_errors.is_non_retriable(done_ticket):
                    #Regardless if index is None or not, make sure that
                    # exit_status gets set to failure.
                    exit_status = 1

                    if index == None:
                        message = "Unknown transfer failed."
                        sys.stderr.write(message + "\n")
                        Trace.log(e_errors.ERROR,
                                  message + "  " + str(done_ticket))
                        
                    else:
                        #Combine the dictionaries.
                        work_ticket = combine_dict(done_ticket,
                                                   request_list[index])
                        #Set completion status to successful.
                        work_ticket['completion_status'] = FAILURE
                        #Store these changes back into the master list.
                        request_list[index] = work_ticket

                if not e_errors.is_ok(done_ticket):
                    continue

        else:
            exit_status = 1
            #If all submits fail (i.e using an old encp), this avoids crashing.
            if not e_errors.is_ok(reply_ticket['status'][0]):
                done_ticket = reply_ticket
            else:
                done_ticket = {}

        Trace.message(TRANSFER_LEVEL,
                      "Files read for volume %s   elapsed=%s" %
                      (vol, time.time() - tinfo['encp_start_time']))


    Trace.message(TRANSFER_LEVEL, "Files read for all volumes.   elapsed=%s" %
                  (time.time() - tinfo['encp_start_time'],))

    # we are done transferring - close out the listen socket
    close_descriptors(listen_socket)

    #Print to screen the exit status.
    Trace.message(TO_GO_LEVEL, "EXIT STATUS: %d" % exit_status)

    #Finishing up with a few of these things.
    calc_ticket = calculate_final_statistics(bytes, number_of_files,
                                             exit_status, tinfo)

    #Volume one ticket is the last request that was processed.
    if e.data_access_layer:
        list_done_ticket = combine_dict(calc_ticket, done_ticket)
    else:
        list_done_ticket = combine_dict(calc_ticket, {})

    Trace.message(TICKET_LEVEL, "LIST DONE TICKET")
    Trace.message(TICKET_LEVEL, pprint.pformat(list_done_ticket))

    return list_done_ticket, requests_per_vol

##############################################################################
##############################################################################

class EncpInterface(option.Interface):
    def __init__(self, args=sys.argv, user_mode=0):

        #This is flag is accessed via a global variable.
        global pnfs_is_automounted
        global pnfs_agent_client_allowed
        global pnfs_agent_client_requested

        #priority options
        self.chk_crc = 1           # we will check the crc unless told not to
        self.ecrc = 0              # do a paranoid check if told to
        self.priority = 1          # lowest priority
        self.delpri = 0            # priority doesn't change
        self.admpri = -1           # quick fix to check HiPri functionality
        self.age_time = 0          # priority doesn't age
        self.delayed_dismount = None # minutes to wait before dismounting
        self.check = 0             # check if transfer attempt will occur
        self.ignore_fair_share = None # tells LM not count this write transfer
                                      # against the storage groups limit.
        
        #messages for user options
        self.data_access_layer = 0 # no special listings
        self.verbose = 0           # higher the number the more is output
        self.version = 0           # print out the encp version
        self.copies = None         # number of copies to write to tape

        #EXfer optimimazation options
        self.buffer_size = 262144  # 256K: the buffer size
	self.array_size = 3        # the number of 'bins' for threaded trans.
        self.mmap_size = 100663296 # 96M: the mmap offset size
	self.direct_io = 0         # true if direct i/o should be used
	self.mmap_io = 0           # true if memory mapped i/o should be used
	self.threaded_exfer = 0    # true if EXfer should run multithreaded
        
        #options effecting encp retries and resubmits
        self.max_retry = enstore_constants.DEFAULT_ENCP_RETRIES
        self.max_resubmit = enstore_constants.DEFAULT_ENCP_RESUBMISSIONS
        self.use_max_retry = 0     # If --max-retry was used.
        self.use_max_resubmit = 0  # If --max-resubmit was used.
        self.mover_timeout = 15*60 # seconds to wait for mover to call back,
                                   # before resubmitting req. to lib. mgr.
                                   # 15 minutes
        self.resubmit_timeout = 15*60 # seconds to wait for the transfer
                                   # before giving up on it.
                                   # 15 minutes

        #Options for overriding the pnfs tags.
        self.output_file_family = "" # initial set for use with --ephemeral or
                                     # or --file-family
        self.output_file_family_wrapper = ""
        self.output_file_family_width = ""
        self.output_library = ""
        self.output_storage_group = ""

        #misc.
        #self.bytes = None          #obsolete???
        #self.test_mode = 0         #obsolete???
        self.pnfs_is_automounted = 0 # true if pnfs is automounted.
        self.override_ro_mount = 0 # Override a tape marked read only to be
                                   # mounted read/write.
        self.override_noaccess = 0 # Override reading/writing to a tape
                                   # marked NOACCESS or NOTALLOWED.
        self.override_path = ""    # If --put-cache and --shortcut are used
                                   # this switch will use the specified
                                   # string as the filename.
        self.override_deleted = 0  # If the file has been deleted, this flag
                                   # will ignore the deleted status when
                                   # reading the file back.

        #Special options for operation with a disk cache layer.
        #self.dcache = 0            #obsolete???
        self.put_cache = 0         # true if dcache is writing by pnfs_id
        self.get_cache = 0         # true if dcache is reading by pnfs_id
        self.get_bfid = None       # true if dcache is reading by bfid
        self.pnfs_mount_point = "" # For dcache, used to specify which pnfs
                                   # mount point to use.  Naively, one can
                                   # not try them all.  If the wrong mount
                                   # point is tried it could hang encp.
        self.shortcut = 0          # If true, don't extrapolate full file path.
        self.storage_info = None   # Not used yet.
        self.volume = None         # True if it is to read an entire volume.
        self.list = None           # Used for "get" only.
        self.skip_deleted_files = None # Used for "get" only.

        #Values for specifying which enstore system to contact.
        self.enstore_config_host = enstore_functions2.default_host()
        self.enstore_config_port = enstore_functions2.default_port()

        #Sometimes the kernel lies about the max size of files supported
        # by the filesystem; skip the test if that is needed.
        self.bypass_filesystem_max_filesize_check = 0

        option.Interface.__init__(self, args=args, user_mode=user_mode)

        # parse the options
        #self.parse_options()

        # This is accessed globally...
        pnfs_is_automounted = self.pnfs_is_automounted

        r_encp = os.environ.get('REMOTE_ENCP')
        if r_encp != None:
            pnfs_agent_client_allowed = True
        if r_encp == "only_pnfs_agent":
            pnfs_agent_client_requested = True


    def __str__(self):
        str_rep = ""

        #Sort the list into alphabetical order.
        the_list = self.encp_options.items()
        the_list.sort()
        
        for name, info in the_list:
            #Get the correct name to use.  Use the defualt if a developer
            # specified one should be used instead.
            if info.has_key(option.VALUE_NAME):
                use_name = info[option.VALUE_NAME]
            else:
                use_name = name

            #Translate the name's dashes to underscores (since python
            # variables have underscores).
            use_name = string.replace(use_name, "-", "_")
            #Obtain the name and type cast it to a string.
            value = str(getattr(self, use_name, None))
            #Append the current item to the current string.
            str_rep = str_rep + "  " + name + ": " + value + "\n"

        return str_rep

    def valid_dictionaries(self):
        return (self.help_options, self.encp_options)
    
    #  define our specific parameters
    parameters = ["<source file> <destination file>",
                 "<source file> [source file [...]] <destination directory>"]

    def print_version(self):
        print encp_client_version()
        sys.exit(0)

    encp_options = {
        option.AGE_TIME:{option.HELP_STRING:"Affects the current job priority."
                         "  Only knowledgeable users should set this.",
                         option.VALUE_USAGE:option.REQUIRED,
                         option.VALUE_TYPE:option.INTEGER,
                         option.USER_LEVEL:option.USER,},
        option.ARRAY_SIZE:{option.HELP_STRING:
                           "The number of 'bins' to use for when --threaded "
                           "is used. (default = 3)",
                           option.VALUE_USAGE:option.REQUIRED,
                           option.VALUE_TYPE:option.INTEGER,
                           option.USER_LEVEL:option.USER,},
        option.BUFFER_SIZE:{option.HELP_STRING:
                            "The amount of data to transfer at one time in "
                            "bytes. (default = 256k)",
                            option.VALUE_USAGE:option.REQUIRED,
                            option.VALUE_TYPE:option.INTEGER,
                            option.USER_LEVEL:option.USER,},
        option.BYPASS_FILESYSTEM_MAX_FILESIZE_CHECK:{option.HELP_STRING:
                            "Skip the check for the max filesize a file"
                            " system supports.",
                            option.VALUE_USAGE:option.IGNORED,
                            option.DEFAULT_TYPE:option.INTEGER,
                            option.USER_LEVEL:option.USER,},
        option.CHECK:{option.HELP_STRING:"Only check if the transfer would "
                      "succeed, but do not actully perform the transfer.",
                      option.VALUE_USAGE:option.IGNORED,
                      option.DEFAULT_TYPE:option.INTEGER,
                      option.USER_LEVEL:option.USER,},
        option.COPIES:{option.HELP_STRING:"Write N copies of the file.",
                       option.VALUE_USAGE:option.REQUIRED,
                       option.VALUE_TYPE:option.INTEGER,
                       option.USER_LEVEL:option.USER,},
        option.DATA_ACCESS_LAYER:{option.HELP_STRING:
                                  "Format all final output for SAM.",
                                  option.DEFAULT_TYPE:option.INTEGER,
                                  option.DEFAULT_VALUE:1,
                                  option.USER_LEVEL:option.USER,},
        option.DELAYED_DISMOUNT:{option.HELP_STRING:
                                 "Specifies time to wait (in minutes) that "
                                 "the mover should wait before dismounting "
                                 "the tape in the event that another request "
                                 "will arrive for that tape.",
                                 option.VALUE_USAGE:option.REQUIRED,
                                 option.VALUE_TYPE:option.INTEGER,
                                 option.USER_LEVEL:option.USER,},
        option.DELPRI:{option.HELP_STRING:"Affects the current job priority."
                       "  Only knowledgeable users should set this.",
                       option.VALUE_USAGE:option.REQUIRED,
                       option.VALUE_TYPE:option.INTEGER,
                       option.USER_LEVEL:option.USER,},
        option.DIRECT_IO:{option.HELP_STRING:
                          "Use direct i/o for disk access on supporting "
                          "filesystems.",
                          option.DEFAULT_TYPE:option.INTEGER,
                          option.DEFAULT_VALUE:1,
                          option.USER_LEVEL:option.USER,},
        option.ECRC:{option.HELP_STRING:
                          "Perform paranoid ecrc check after read operation.",
                          option.DEFAULT_TYPE:option.INTEGER,
                          option.DEFAULT_VALUE:1,
                          option.USER_LEVEL:option.USER,},
        option.EPHEMERAL:{option.HELP_STRING:
                          "Use the ephemeral file family (writes only).",
                          option.DEFAULT_TYPE:option.STRING,
                          option.DEFAULT_VALUE:"ephemeral",
                          option.DEFAULT_NAME:"output_file_family",
                          option.USER_LEVEL:option.USER,},
        option.FILE_FAMILY:{option.HELP_STRING:
                            "Specify an alternative file family to override "
                            "the pnfs file family tag (writes only).",
                            option.VALUE_USAGE:option.REQUIRED,
                            option.VALUE_TYPE:option.STRING,
                            option.VALUE_NAME:"output_file_family",
                            option.USER_LEVEL:option.USER,},
        option.FILE_FAMILY_WRAPPER:{option.HELP_STRING:
                                    "Specify an alternative file family "
                                    "wrapper to override the pnfs file family "
                                    "wrapper tag (writes only).",
                                    option.VALUE_USAGE:option.REQUIRED,
                                    option.VALUE_TYPE:option.STRING,
                               option.VALUE_NAME:"output_file_family_wrapper",
                                    option.USER_LEVEL:option.ADMIN,},
        option.FILE_FAMILY_WIDTH:{option.HELP_STRING:
                                  "Specify an alternative file family "
                                  "width to override the pnfs file family "
                                  "width tag (writes only).",
                                  option.VALUE_USAGE:option.REQUIRED,
                                  option.VALUE_TYPE:option.STRING,
                                 option.VALUE_NAME:"output_file_family_width",
                                  option.USER_LEVEL:option.ADMIN,},
        option.GET_BFID:{option.HELP_STRING:
                         "Specifies that dcache requested the file and that "
                         "the first 'filename' is really the file's bfid.",
                         option.VALUE_TYPE:option.STRING,
                         option.VALUE_USAGE:option.REQUIRED,
                         option.USER_LEVEL:option.ADMIN,},
        option.GET_CACHE:{option.HELP_STRING:
                          "Specifies that dcache requested the file and that "
                          "the first 'filename' is really the file's pnfs id.",
                          option.VALUE_TYPE:option.STRING,
                          option.VALUE_USAGE:option.REQUIRED,
                          option.USER_LEVEL:option.USER2,},
        option.IGNORE_FAIR_SHARE:{option.HELP_STRING:
                             "Do not count transfer against fairshare limit.",
                                  option.DEFAULT_TYPE:option.INTEGER,
                                  option.DEFAULT_VALUE:1,
                                  option.VALUE_USAGE:option.IGNORED,
                                  option.USER_LEVEL:option.ADMIN,
                          #This will set an addition value.  It is weird
                          # that DEFAULT_TYPE is used with VALUE_NAME,
                          # but that is what will make it work.
                          #option.EXTRA_VALUES:[{option.DEFAULT_VALUE:0,
                          #                option.DEFAULT_TYPE:option.INTEGER,
                          #                option.VALUE_NAME:option.PRIORITY,
                          #                option.VALUE_USAGE:option.IGNORED,
                          #                      }]
                                  },
        option.LIBRARY:{option.HELP_STRING:
                            "Specify an alternativelibrary to override "
                            "the pnfs library tag (writes only).",
                            option.VALUE_USAGE:option.REQUIRED,
                            option.VALUE_TYPE:option.STRING,
                            option.VALUE_NAME:"output_library",
                            option.USER_LEVEL:option.ADMIN,},
        option.MAX_RETRY:{option.HELP_STRING:
                          "Specifies number of non-fatal errors that can "
                          "occur before encp gives up.  Use None "
                          "to specify never. (default = 3)",
                          option.DEFAULT_NAME:'use_max_retry',
                          option.DEFAULT_VALUE:option.DEFAULT,
                          option.DEFAULT_TYPE:option.INTEGER,
                          option.VALUE_USAGE:option.REQUIRED,
                          #option.VALUE_TYPE:option.INTEGER, #Int or None.
                          option.USER_LEVEL:option.ADMIN,
                          option.FORCE_SET_DEFAULT:option.FORCE},
        option.MAX_RESUBMIT:{option.HELP_STRING:
                             "Specifies number of resubmissions encp makes "
                             "when mover does not callback.  Use None "
                             "to specify never. (default = never)",
                             option.DEFAULT_NAME:'use_max_resubmit',
                             option.DEFAULT_VALUE:option.DEFAULT,
                             option.DEFAULT_TYPE:option.INTEGER,
                             option.VALUE_USAGE:option.REQUIRED,
                             #option.VALUE_TYPE:option.INTEGER, #Int or None.
                             option.USER_LEVEL:option.ADMIN,
                             option.FORCE_SET_DEFAULT:option.FORCE},
        option.MMAP_IO:{option.HELP_STRING:
                        "Use memory mapped i/o for disk access on supporting "
                        "filesystems.",
                        option.DEFAULT_TYPE:option.INTEGER,
                        option.DEFAULT_VALUE:1,
                        option.USER_LEVEL:option.USER,},
        option.MMAP_SIZE:{option.HELP_STRING:
                          "The amount of data to transfer at one time in "
                          "bytes. (default = 96M)",
                          option.VALUE_USAGE:option.REQUIRED,
                          option.VALUE_TYPE:option.INTEGER,
                          option.USER_LEVEL:option.USER,},
        option.MOVER_TIMEOUT:{option.HELP_STRING:
                              "Number of seconds to wait for the mover "
                              "before giving up on the transfer. "
                              "(default = 15min).",
                              option.VALUE_NAME:'mover_timeout',
                              option.VALUE_TYPE:option.INTEGER,
                              option.VALUE_USAGE:option.REQUIRED,
                              option.USER_LEVEL:option.ADMIN,
                              },
        option.NO_CRC:{option.HELP_STRING:"Do not perform CRC check.",
                       option.DEFAULT_NAME:'chk_crc',
                       option.DEFAULT_TYPE:option.INTEGER,
                       option.DEFAULT_VALUE:0,
                       option.USER_LEVEL:option.USER,},
        option.OVERRIDE_DELETED:{option.HELP_STRING:
                                 "When --get-bfid is used "
                                 "this will tell encp that it can read the "
                                 "file despite being deleted.",
                                 option.VALUE_USAGE:option.IGNORED,
                                 option.VALUE_TYPE:option.INTEGER,
                                 option.DEFAULT_VALUE:1,
                                 option.USER_LEVEL:option.ADMIN,},
        #option.OVERRIDE_NOACCESS:{option.HELP_STRING:
        #                          "Override NOACCESS inhibit for read/write.",
        #                          option.DEFAULT_TYPE:option.INTEGER,
        #                          option.DEFAULT_VALUE:1,
        #                          option.USER_LEVEL:option.ADMIN,},
        option.OVERRIDE_PATH:{option.HELP_STRING:
                              "When --put-cache and --shortcut are both used "
                              "this will tell encp what the filename "
                              "should be.",
                              option.VALUE_USAGE:option.REQUIRED,
                              option.VALUE_TYPE:option.STRING,
                              option.USER_LEVEL:option.USER2,},
        option.OVERRIDE_RO_MOUNT:{option.HELP_STRING:
                                  "Override read only tape for read/write.",
                                  option.DEFAULT_TYPE:option.INTEGER,
                                  option.DEFAULT_VALUE:1,
                                  option.USER_LEVEL:option.ADMIN,},
        option.PNFS_IS_AUTOMOUNTED:{option.HELP_STRING:
                                    "Set this when the pnfs filesystem is "
                                    "auto-mounted.",
                                    option.DEFAULT_TYPE:option.INTEGER,
                                    option.DEFAULT_VALUE:1,
                                    option.USER_LEVEL:option.USER,},
        option.PNFS_MOUNT_POINT:{option.HELP_STRING:"Tells encp which pnfs "
                                 "mount point to use. (dcache only)",
                                 option.VALUE_USAGE:option.REQUIRED,
                                 option.VALUE_TYPE:option.STRING,
                                 option.USER_LEVEL:option.USER2,},
        option.PRIORITY:{option.HELP_STRING:"Sets the initial job priority."
                         "  Only knowledgeable users should set this.",
                         option.VALUE_USAGE:option.REQUIRED,
                         option.VALUE_TYPE:option.INTEGER,
                         option.USER_LEVEL:option.USER,},
        option.PUT_CACHE:{option.HELP_STRING:
                          "Specifies that dcache requested the file and that "
                          "the first 'filename' is really the file's pnfs id.",
                          option.VALUE_TYPE:option.STRING,
                          option.VALUE_USAGE:option.REQUIRED,
                          option.USER_LEVEL:option.USER2,},
        option.RESUBMIT_TIMEOUT:{option.HELP_STRING:
                                 "Number of seconds to wait for the mover "
                                 "before resubmiting the request "
                                 "(default = 15min).",
                                 option.VALUE_NAME:'resubmit_timeout',
                                 option.VALUE_TYPE:option.INTEGER,
                                 option.VALUE_USAGE:option.REQUIRED,
                                 option.USER_LEVEL:option.ADMIN,
                                 },
        option.SHORTCUT:{option.HELP_STRING:
                         "Used with dcache transfers to avoid pathname "
                         "lookups of pnfs ids.",
                         option.DEFAULT_TYPE:option.INTEGER,
                         option.DEFAULT_VALUE:1,
                         option.USER_LEVEL:option.USER2,},
        option.STORAGE_GROUP:{option.HELP_STRING:
                               "Specify an alternative storage group to "
                               "override the pnfs strorage group tag "
                               "(writes only).",
                               option.VALUE_USAGE:option.REQUIRED,
                               option.VALUE_TYPE:option.STRING,
                               option.VALUE_NAME:"output_storage_group",
                               option.USER_LEVEL:option.ADMIN,},
        option.THREADED:{option.HELP_STRING:
                         "Multithread the actual data transfer.",
                         option.DEFAULT_TYPE:option.INTEGER,
                         option.DEFAULT_NAME:"threaded_exfer",
                         option.DEFAULT_VALUE:1,
                         option.USER_LEVEL:option.USER,},
        option.VERBOSE:{option.HELP_STRING:"Print out information.",
                        option.VALUE_USAGE:option.REQUIRED,
                        option.VALUE_TYPE:option.INTEGER,
                        option.USER_LEVEL:option.USER,},
        option.VERSION:{option.HELP_STRING:
                        "Display encp version information.",
                        option.DEFAULT_TYPE:option.INTEGER,
                        option.DEFAULT_VALUE:1,
                        option.USER_LEVEL:option.USER,},
        option.VOLUME:{option.HELP_STRING:
                       "Read a volumes worth of files.",
                       #option.DEFAULT_TYPE:option.INTEGER,
                       #option.DEFAULT_VALUE:1,
                       option.VALUE_USAGE:option.REQUIRED,
                       option.VALUE_TYPE:option.STRING,
                       option.USER_LEVEL:option.ADMIN,},
        }

    ##########################################################################
    # parse the options from the command line
    def parse_options(self):
        global pnfs_agent_client_requested
        global pnfs_agent_client_allowed
        
        # normal parsing of options
        option.Interface.parse_options(self)

        #Process these at the beginning.
        if hasattr(self, "help") and self.help:
            self.print_help()
        if hasattr(self, "usage") and self.usage:
            self.print_usage()
        if hasattr(self, "version") and self.version:
            self.print_version()

        #The values for --max-retry and --max-resubmit need special processing.
        # This is so that the special non integer value of 'None' gets
        # processed correctly.
        if type(self.max_retry) != types.StringType:
            pass  #Skip the int and None cases.  Only consider string cases.
        elif self.max_retry.upper() == "NONE":
            self.max_retry = None
        elif self.max_retry.isdigit():
            self.max_retry = max(int(self.max_retry), 0)
        else:
            self.print_usage("Argument for --max-retry must be a "
                             "positive integer or None.")
        if type(self.max_resubmit) != types.StringType:
            pass  #Skip the int and None cases.  Only consider string cases.
        elif self.max_resubmit.upper() == "NONE":
            self.max_resubmit = None
        elif self.max_resubmit.isdigit():
            self.max_resubmit = max(int(self.max_resubmit), 0)
        else:
            self.print_usage("Argument for --max-resubmit must be a "
                             "positive integer or None.")

        if self.copies != None and self.copies < 0:
            self.print_usage("Argument for --copy must be a non-negative integer.")

        # bomb out if we don't have an input/output if a special command
        # line was given.  (--volume, --get-cache, --put-cache, --bfid)
        self.arglen = len(self.args)
        if self.arglen < 1 :
            self.print_usage("%s: not enough arguments specified" %
                             e_errors.USERERROR)
            sys.exit(1)

        if self.volume:
            self.input = None   #[self.args[0]]
            self.output = self.argv[-1]  #[self.args[self.arglen-1]]
            self.intype = "hsmfile"   #"hsmfile"
            self.outtype = "unixfile"
            return #Don't continue.

        if self.get_bfid:
            self.input = None #[self.args[0]]
            self.output = self.argv[-1] #[self.args[self.arglen-1]]
            self.intype = "hsmfile"  #What should this bee?
            self.outtype = "unixfile"
            return #Don't continue.
        
        if self.get_cache:
            self.input = None  #[self.args[0]]
            self.output = self.argv[-1]  #[self.args[self.arglen-1]]
            self.intype = "hsmfile"   #What should this bee?
            self.outtype = "unixfile"
            return #Don't continue.
        
        if self.put_cache:
            self.input = self.argv[-1]  #[self.args[0]]
            self.output = None  #[self.args[self.arglen-1]]
            self.intype = "unixfile"
            self.outtype = "hsmfile"  #What should this bee?
            return #Don't continue.


        # bomb out if we don't have an input and an output
        self.arglen = len(self.args)
        if self.arglen < 2 :
            self.print_usage("%s: not enough arguments specified" %
                             e_errors.USERERROR)
            sys.exit(1)

        #Determine whether the files are in /pnfs or not.
        p = []  #p stands for is_pnfs_file?
        m = []  #m stands for which_machine?
        e = []  #e stand for does this exist on local host
        for i in range(0, self.arglen):
            #Get fullpaths to the files.
            #machine, fullname, unused, unused = fullpath(self.args[i])
            protocol, host, port, fullname, dirname, basename = \
                      enstore_functions2.fullpath2(self.args[i])
            #Store the name into this list.
            self.args[i] = fullname

            #if ( os.path.exists(dirname) ) :
            #    e.append(1)
            #else:
            #    e.append(0)

            #We need to make sure that all the files are to/from the same place.
            # Store it here for processing later.
            m.append((host, port))
            
            #If the file is a pnfs file, store a 1 in the list, if not store
            # a zero.  All files on the hsm system have /pnfs/ in there name.
            # Most have /pnfs/ at the very beginning, but automounted pnfs
            # filesystems don't. Scan input files for /pnfs/ - all have to
            # be the same.  Pass check_name_only a python true value to skip
            # file existance/permission tests at this time.
            #
            #Check the three possible pnfs paths.  If all three fail, it
            # is likely that this is a non-pnfs filename.  Reasons that
            # a pnfs filename would fail to be detected include:
            # 1) Pnfs is mounted wrong or not at all.
            # 2) The user misspelled the path before the pnfs mount point
            #    in the absolute filename.

            if pnfs_agent_client_requested:
                try:
                    #Pnfs Agent.
                    pac = get_pac()
                    result = pac.is_pnfs_path(fullname,
                                              check_name_only = 1)
                except EncpError:
                    result = 0

                if result:
                    e.append(0)
                    #p.append(result)
                    p.append(2)
                    continue
                else:
                    e.append(os.path.exists(dirname))
                    p.append(0) #Assume local file.
                    #Do not even try checking if pnfs is locally mounted.
                    # The user has specifically requested that encp
                    # only access pnfs through the pnfs_agent.  Just go
                    # on and check the next file.
                    continue

            try:
                #Original full path.  (Best choice if possible)
                result = pnfs.is_pnfs_path(fullname,
                                           check_name_only = 1)
            except EncpError:
                result = 0
                
            if result:
                e.append(1)
                p.append(result)
                continue

            try:
                #Traditional encp path.
                pnfs_path = get_enstore_pnfs_path(fullname)
                result = pnfs.is_pnfs_path(pnfs_path,
                                           check_name_only = 1)
            except EncpError:
                result = 0

            if result:
                e.append(1)
                p.append(result)
                self.args[i] = pnfs_path
                continue

            try:
                #Traditional admin path.
                admin_path = get_enstore_fs_path(fullname)
                result = pnfs.is_pnfs_path(admin_path,
                                           check_name_only = 1)
            except EncpError:
                result = 0

            if result:
                e.append(1)
                p.append(result)
                self.args[i] = admin_path
                continue

            try:
                #Traditional grid path.
                grid_path = get_enstore_canonical_path(fullname)
                result = pnfs.is_pnfs_path(grid_path,
                                           check_name_only = 1)
            except EncpError:
                result = 0

            if result:
                e.append(1)
                p.append(result)
                self.args[i] = grid_path
                continue

            if pnfs_agent_client_allowed:
                try:
                    #Pnfs Agent.
                    pac = get_pac()
                    result = pac.is_pnfs_path(fullname,
                                              check_name_only = 1)
                except EncpError:
                    result = 0

                if result:
                    e.append(0)
                    #p.append(result)
                    p.append(2)
                    continue

            e.append(os.path.exists(dirname))
            p.append(0) #Assume local file.
            
        #Initialize some important values.
        

        #The p# variables are used as holders for testing if all input files
        # are unixfiles or hsmfiles (aka pnfs files).
        #    2 = pnfs agent, 1 = pnfs & 0 = local
        p1 = p[0]
        p2 = p[self.arglen - 1]
        #The m# variables are used to determine if all of the input files
        # are on the same node (enstore system).
        m1 = m[0]
        m2 = m[self.arglen - 1]
        #
        # Dmitry's hacking
        # e1 = 
        #       0 - non-existing path, 1 - existing path
        ###e1 = e[0]
        ###e2 = e[self.arglen - 1]

        #Also, build two new lists of input and output files.  The output
        # list should always be 1 in length.  A simple, assignment is not
        # performed because that only returns a reference to the original,
        # it does not create a distinct copy.
        self.input = [self.args[0]]
        self.output = [self.args[self.arglen-1]]

        #Loop through all the input files.  Compare against the first input
        # file for similarity in being a pnfs or unix file.  This check only
        # makes sure that all input files are either unix or pnfs files.
        # The check to prevent unix to unix or pnfs to pnfs copies is done
        # later on in the code.
        #It is also checked to make sure that no duplicate files are listed.
        #
        #Also, check that all the files match in terms of which node they
        # are_on/should_go_to.
        for i in range(1, len(self.args) - 1):
            if p[i] != p1:
                msg = "Not all input files are %s files"
                if p2:
                    print_error(e_errors.USERERROR, msg % "unix")
                else:
                    print_error(e_errors.USERERROR, msg % "/pnfs/...")
                delete_at_exit.quit()
            elif self.args[i] in self.input:
                msg = "Duplicate filenames is not allowed: %s"
                print_error(e_errors.USERERROR, msg % self.args[i])
                delete_at_exit.quit()
            else:
                self.input.append(self.args[i]) #Do this way for a copy.

            if m[i][0] != m1[0]:
                msg = "Not all input files are on node %s."
                print_error(e_errors.USERERROR, msg % m1[0])
                delete_at_exit.quit()

        #We need to check to make sure that only one enstore system has
        # been specified.

        #Determine all aliases and ip addresses for this node name.
        for i in [1, 2, 3]:
            try:
                this_host = socket.gethostbyname_ex(socket.getfqdn())
            except (socket.error, socket.herror, socket.gaierror):
                time.sleep()
        else:
            try:
                this_host = [socket.getfqdn(), [], []]
            except (socket.error, socket.herror, socket.gaierror):
                this_host = ["localhost", [], []]  #Is this the best to do?
        #Flatten node name info this into one list.
        this_host_list = [this_host[0]] + this_host[1] + this_host[2]
        #If we are writing to enstore and don't use the default destination.
        if m1[0] in this_host_list and m2[0] not in this_host_list:
            self.enstore_config_host = m2[0]
            if m2[1] != None:
                self.enstore_config_port = int(m2[1])
        #If we are reading from enstore and don't use the default source.
        if m1[0] not in this_host_list and m2[0] in this_host_list:
            self.enstore_config_host = m1[0]
            if m1[1] != None:
                self.enstore_config_port = int(m1[1])
        #If two remote address are given, this is an error.
        if m1[0] not in this_host_list and m2[0] not in this_host_list:
            msg = "Not able to perform remote site to remote site transfer."
            print_error(e_errors.USERERROR, msg)
            delete_at_exit.quit()

        #Dmitry:
        #      logic is this, if a file is pnfs file and it does not
        #      exist locally we asume that it is on remote pnfs server
        #      we create special type to handle this case "rhsm" aka "remote hsm"

        if pnfs_agent_client_requested:
            if p1 == 1 or p1 == 2:
                self.intype = RHSMFILE
            else:
                self.intype = UNIXFILE
            if p2 == 1 or p2 == 2:
                self.outtype = RHSMFILE
            else:
                self.outtype = UNIXFILE

        elif pnfs_agent_client_allowed:
            if p1 == 2:
                self.intype = RHSMFILE
            elif p1 == 1:
                self.intype = HSMFILE
                pnfs_agent_client_allowed = False
            else:
                self.intype = UNIXFILE
            if p2 == 2:
                self.outtype = RHSMFILE
            elif p1 == 1:
                self.outtype = HSMFILE
                pnfs_agent_client_allowed = False
            else:
                self.outtype = UNIXFILE
                
        else:
            #Assign the collection of types to these variables.
            if p1 == 1:
                self.intype = HSMFILE
            else:
                self.intype = UNIXFILE
            if p2 == 1:
                self.outtype = HSMFILE
            else:
                self.outtype = UNIXFILE

##############################################################################
##############################################################################
            
def log_encp_start(tinfo, intf):        

    #If verbosity is turned on get the user name(s).
    try:
        user_name = pwd.getpwuid(os.geteuid())[0]
    except (OSError, KeyError):
        user_name = os.geteuid()
    try:
        real_name = pwd.getpwuid(os.getuid())[0]
    except (OSError, KeyError):
        real_name = os.getuid()

    #If verbosity is turned on get the group name(s).
    try:
        user_group = grp.getgrgid(os.getegid())[0]
    except (OSError, KeyError):
        user_group = os.getegid()
    try:
        real_group = grp.getgrgid(os.getgid())[0]
    except (OSError, KeyError):
        real_group = os.getgid()

    #If verbosity is turned on and the transfer is a write to enstore,
    # output the tag information.
    try:
        if intf.put_cache:
            p = Pnfs()

            shortcut_name = p.get_path(intf.put_cache,
                                       intf.pnfs_mount_point,
                                       shortcut = True)
            shortcut_dname = get_directory_name(shortcut_name)
            if intf.outtype == RHSMFILE:
                t = p
            else:
                t = pnfs.Tag()
        elif not intf.output:
            t = None
            shortcut_dname = None
        elif os.path.isdir(intf.output[0]):
            t = pnfs.Tag(intf.output[0])
            shortcut_dname = intf.output[0]
        else:
            shortcut_dname = get_directory_name(intf.output[0])
            t = pnfs.Tag(shortcut_dname)
    except (OSError, IOError):
        t = None
        
    try:
        if intf.output_library:
            library = intf.output_library
        else:
            library = t.get_library(shortcut_dname)
    except (OSError, IOError, KeyError, TypeError, AttributeError):
        library = "Unknown"
    try:
        if intf.output_storage_group:
            storage_group = intf.output_storage_group
        else:
            storage_group = t.get_storage_group(shortcut_dname)
    except (OSError, IOError, KeyError, TypeError, AttributeError):
        storage_group = "Unknown"
    try:
        if intf.output_file_family:
            file_family = intf.output_file_family
        else:
            file_family = t.get_file_family(shortcut_dname)
    except (OSError, IOError, KeyError, TypeError, AttributeError):
        file_family = "Unknown"
    try:
        if intf.output_file_family_wrapper:
            file_family_wrapper = intf.output_file_family_wrapper
        else:
            file_family_wrapper = t.get_file_family_wrapper(shortcut_dname)
    except (OSError, IOError, KeyError, TypeError, AttributeError):
        file_family_wrapper = "Unknown"
    try:
        if intf.output_file_family_width:
            file_family_width = intf.output_file_family_width
        else:
            file_family_width = t.get_file_family_width(shortcut_dname)
    except (OSError, IOError, KeyError, TypeError, AttributeError):
        file_family_width = "Unknown"

    #Get the current working directory.  If the cwd isn't valid (i.e. the
    # directory is deleted), handle it so encp doesn't crash.
    try:
        cwd = os.getcwd()
    except OSError:
        cwd = "invalid_cwd"
    try:
        hostname = socket.getfqdn(socket.gethostname())
    except (OSError, socket.error, socket.herror, socket.gaierror):
        hostname = "invalid_hostname"

    try:
        os_info = os.uname()
    except OSError:
        os_info = ('', '', '', '', '')

    try:
        release_data = []
        for fname in os.listdir("/etc"):
            if fname[-7:] == "release":
                fp = open("/etc/" + fname, "r")
                info = fp.readline().strip() #Only care about first line?
                fp.close()
                if info not in release_data:
                    release_data.append(info)
    except OSError:
        release_data = []
    release_info = ""
    for info in release_data:
        release_info = release_info + " " + info
    
    #Other strings for the log file.
    start_line = "Start time: %s" % time.ctime(tinfo['encp_start_time'])
    command_line = "Command line: %s" % (string.join(intf.argv),)
    version_line = "Version: %s" % (encp_client_version().strip(),)
    os_line = "OS: %s %s %s Release: %s" % \
              (os_info[0], os_info[2], os_info[4], str(release_info))
    id_line = "User: %s(%d)  Group: %s(%d)  Euser: %s(%d)  Egroup: %s(%d)" %\
              (real_name, os.getuid(), real_group, os.getgid(),
               user_name, os.geteuid(), user_group, os.getegid())
    tag_line = "Library: %s  Storage Group: %s  File Family: %s  " \
               "FF Wrapper: %s  FF Width: %s" % \
               (library, storage_group,
                file_family,file_family_wrapper, file_family_width)
    cwd_line = "Current working directory: %s:%s" % (hostname, cwd)

    #Print this information to make debugging easier.
    Trace.message(DONE_LEVEL, start_line)
    Trace.message(DONE_LEVEL, id_line)
    Trace.message(DONE_LEVEL, command_line)
    Trace.message(DONE_LEVEL, version_line)
    Trace.message(DONE_LEVEL, os_line)
    if intf.outtype == HSMFILE or intf.outtype == RHSMFILE:
        Trace.message(DONE_LEVEL, tag_line)
    Trace.message(DONE_LEVEL, cwd_line)

    #Print out the information from the command line.
    #Trace.message(CONFIG_LEVEL, format_class_for_print(intf, "intf"))
    Trace.message(CONFIG_LEVEL, "intf:\n" + str(intf))

    #Convenient, but maybe not correct place, to hack in log message
    # that shows how encp was called.
    if intf.outtype == "hsmfile":  #write
        Trace.log(e_errors.INFO, "%s  %s %s  %s  %s  %s" %
                  (version_line, os_line, id_line, tag_line, cwd_line, command_line))
    else:                       #read
        Trace.log(e_errors.INFO, "%s  %s, %s  %s  %s" %
                  (version_line, os_line, id_line, cwd_line, command_line))



def final_say(intf, done_ticket):
    try:
        #Log the message that tells us that we are done.
        status = done_ticket.get('status', (e_errors.UNKNOWN,e_errors.UNKNOWN))
        exit_status = done_ticket.get('exit_status',
                                      not e_errors.is_ok(status))
        #Determine the filename(s).
        ifilename = done_ticket.get("infile", "")
        ofilename = done_ticket.get("outfile", "")
        if not ifilename and not ofilename:
            ifilename = intf.input
            ofilename = intf.output

        #Perform any necessary string formating.
        if status[1] == None:
            msg_str = None
        elif type(status[1]) == types.StringType:
            #Log messages should not have newlines in them.
            msg_str = string.replace(status[1], "\n\t", "  ")
        else:
            msg_str = str(status[1])
            
        Trace.log(e_errors.INFO, msg_str)
        
        if e_errors.is_alarmable(status):
            Trace.alarm(e_errors.ERROR, status[0],
                        {'infile':ifilename, 'outfile':ofilename,
                         'status':status[1]})

        ifilename = done_ticket.get("infile", "")
        ofilename = done_ticket.get("outfile", "")
        if not ifilename and not ofilename:
            ifilename = intf.input
            ofilename = intf.output

        if intf.data_access_layer or not e_errors.is_ok(status):            
            #We only want to print the data access layer if there was an error
            # or the user explicitly requested it.  In the cases where it will
            # be printed here, encp never got to the point of submitting
            # requests to the library manager.
            print_data_access_layer_format(ifilename, ofilename,
                                           done_ticket.get('file_size', None),
                                           done_ticket)
        else:
            #Explaination for the case where the status at this point
            # is OK and the exit_status is non-zero:
            #   The code got far enough to call calculate_final_statistics()
            #   which Okays the status (but still puts the final message in
            #   status[1]) and sets the exit_status to error in this
            #   final ticket.  In this case, the data_access_layer is
            #   printed out in handle_retries(), write_hsm_file() or
            #   read_hsm_files().

            #If the second part of the status is not empty, print it.
            # There will be non-None status[1] values paired with OK
            # status[0] values.  This is to pack the final message regardless
            # of errors (or no errors) occuring.
            if status[1] != None:
                #If There was an error print the message.
                Trace.message(DONE_LEVEL, str(status[1]))

    except ValueError:
        exc, msg = sys.exc_info()[:2]
        sys.stderr.write("Error (main): %s: %s\n" % (str(exc), str(msg)))
        sys.stderr.write("Exit status: %s\n", exit_status)
        #delete_at_exit.quit(1)
        return exit_status

    Trace.trace(20,"encp finished at %s"%(time.ctime(time.time()),))
    return exit_status
    
    #Quit safely by Removing any zero length file for transfers that failed.
    #delete_at_exit.quit(exit_status)


#The main function takes the interface class instance as parameter and
# returns an exit status.
def main(intf):
    #Snag the start time.  t0 is needed by the mover, but its name conveys
    # less meaning.
    encp_start_time = time.time()
    tinfo = {'encp_start_time':encp_start_time,
             't0':int(encp_start_time)}

    #Initialize the Trace module.
    Trace.init("ENCP")
    for x in xrange(6, intf.verbose + 1):
        Trace.do_print(x)
    for x in xrange(1, intf.verbose + 1):
        Trace.do_message(x)

    #Some globals are expected to exists for normal operation (i.e. a logger
    # client).  Create them.
    status_ticket = clients(intf)
    if not e_errors.is_ok(status_ticket):
        return final_say(intf, status_ticket)

    #Log/print the starting encp information.  This depends on the log
    # from the clients() call, thus it should always be after clients().
    # This function should never give a fatal error.
    log_encp_start(tinfo, intf)

    if intf.data_access_layer:
        global data_access_layer_requested
        data_access_layer_requested = intf.data_access_layer
        #data_access_layer_requested.set()
    elif intf.verbose < 0:
        #Turn off all output to stdout and stderr.
        data_access_layer_requested = -1
        intf.verbose = 0

    #Special handling for use with dcache.
    if intf.get_cache:
        done_ticket, work_list = read_from_hsm(intf, tinfo)

    #Special handling for use with dcache.
    elif intf.put_cache:
        done_ticket, work_list = write_to_hsm(intf, tinfo)
        
    ## have we been called "encp unixfile hsmfile" ?
    elif intf.intype == UNIXFILE and intf.outtype == HSMFILE :
        done_ticket, work_list = write_to_hsm(intf, tinfo)
        
    ## have we been called "encp hsmfile unixfile" ?
    elif intf.intype == HSMFILE and intf.outtype == UNIXFILE :
        done_ticket, work_list = read_from_hsm(intf, tinfo)

    ## have we been called "encp rshmfile unixfile" ?
    elif intf.intype == RHSMFILE and intf.outtype == UNIXFILE :
        done_ticket, work_list = read_from_hsm(intf, tinfo)

    ## have we been called "encp unixfile rhsmfile" ?
    elif intf.intype == UNIXFILE and intf.outtype == RHSMFILE :
#        emsg = "encp unix to remote hsm  is not implemented."
#        done_ticket = {'status':("USERERROR", emsg)}
        done_ticket, work_list = write_to_hsm(intf, tinfo)

    ## have we been called "encp unixfile unixfile" ?
    elif intf.intype == UNIXFILE and intf.outtype == UNIXFILE :
        emsg = "encp copies to/from tape.  It is not involved in copying " \
               "%s to %s." % (intf.intype, intf.outtype)
        done_ticket = {'status':("USERERROR", emsg)}

    ## have we been called "encp hsmfile hsmfile?
    elif intf.intype == HSMFILE and intf.outtype == HSMFILE \
         or intf.intype == RHSMFILE and intf.outtype == RHSMFILE:
        emsg = "encp tape to tape is not implemented. Copy file to local " \
               "disk and then back to tape."
        done_ticket = {'status':("USERERROR", emsg)}

    else:
        emsg = "ERROR: Can not process arguments %s" % (intf.args,)
        done_ticket = {'status':("USERERROR", emsg)}


    return final_say(intf, done_ticket)



def do_work(intf):

    try:
        exit_status = main(intf)
        delete_at_exit.quit(exit_status)
    except (SystemExit, KeyboardInterrupt), msg:
        Trace.log(e_errors.ERROR, "encp aborted from: %s" % str(msg))
        delete_at_exit.quit(1)
    except:
        #Get the uncaught exception.
        exc, msg, tb = sys.exc_info()
        ticket = {'status' : (e_errors.UNCAUGHT_EXCEPTION,
                              "%s: %s" % (str(exc), str(msg)))}

        #Print the data access layer and send the information to the
        # accounting server (if possible).
        print_data_access_layer_format(None, None, None, ticket)
        #Send to the log server the traceback dump.  If unsuccessful,
        # print the traceback to standard error.
        Trace.handle_error(exc, msg, tb)
        del tb #No cyclic references.
        #Remove any zero-length files left haning around.  Also, return
        # a non-zero exit status to the calling program/shell.
        delete_at_exit.quit(1)

        
if __name__ == '__main__':
    delete_at_exit.setup_signal_handling()

    intf_of_encp = EncpInterface(sys.argv, 0) # zero means admin

    do_work(intf_of_encp)
