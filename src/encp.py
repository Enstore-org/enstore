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
# $REMOTE_ENCP = If this evaluates to anything python true, then if a local
#                pnfs mount is not found, encp will try the pnfs_agent.
#                If this is set to "only_pnfs_agent", then only the
#                will be tried.
# $FAIL_1ST_DATA_SOCKET_LATER = Not useful to users.  When set to boolean
#                               true, will cause encp to simulate a network
#                               error connecting the data socket.
#
#                               The motivation for adding this is that
#                               a subtle error was found only for writes,
#                               that this functionality was required to
#                               duplicate.  (unique_id was not updated for
#                               the correct object reference, like reads did,
#                               for some errors.)
############################################################################

"""
There are two main ways through the code.  They start with calls to
read_from_hsm() or write_to_hsm().  Use of encp in migration starts at
do_work() instead of start().

The functions marked with a one (1) indicate only one can be executed.
The functions marked with a capital ell (L) indicate they are called in a loop.
The functions listed with an asterisk (*) are optional.
The functions listed with a plus sign (+) are started in a new thread.

start()
  |
  v
do_work()
  |
  v
main() -> 1 read_from_hsm()
       |    |
       |    -> prepare_read_from_hsm()
       |    |  |
       |    |  -> create_read_requests() -> L create_read_request()
       |    |  |
       |    |  -> verify_read_request_consistancy() -> L inputfile_check()
       |    |                                       -> L inputfile_check_pnfs()
       |    |                                       -> L outputfile_check()
       |    -> submit_read_requests()
       |    |
       |    -> L wait_for_message() -> mover_handshake()
       |    |
       |    -> L read_hsm_file()
       |    |    |
       |    |    -> open_local_file()
       |    |    |
       |    |    -> stall_read_transfer()
       |    |    |
       |    |    -> transfer_file()
       |    |    |
       |    |    -> read_post_transfer_update()
       |    |    |  |
       |    |    |  -> verify_file_size()
       |    |    |  |
       |    |    |  -> check_crc()
       |    |    |  |
       |    |    |  -> update_modification_time()
       |    |    |  |
       |    |    |  -> set_outfile_permissions()
       |    |    |  |
       |    |    |  -> update_last_access_time()
       |    |    |
       |    |    -> calculate_rate()
       |    |
       |    -> L finish_request()
       |    |
       |    -> calculate_final_statistics()
       |
       -> 1 write_from_hsm()
            |
            -> prepare_write_from_hsm()
            |  |
            |  -> create_write_requests() -> L create_write_request()
            |  |
            |  -> verify_write_request_consistancy() -> L inputfile_check()
            |                                        -> L outputfile_check()
            -> submit_write_requests()
            |
            -> L wait_for_message() -> mover_handshake()
            |
            -> L write_hsm_file()
            |     |
            |     -> open_local_file()
            |     |
            |     -> stall_write_transfer()
            |     |
            |     -> transfer_file()
            |     |
            |     -> write_post_transfer_update()
            |     |  |
            |     |  -> check_crc()
            |     |  |
            |     |  -> update_modification_time()
            |     |  |
            |     |  -> set_sfs_settings()
            |     |  |
            |     |  -> set_outfile_permissions()
            |     |  |
            |     |  -> update_last_access_time()
            |     |
            |     -> calculate_rate()
            |
            -> L finish_request()
            |
            -> calculate_final_statistics()
"""

"""
TO DO:
1) Use the Fs class from src/fs.py for the local files.
   a) Simplify the paramaterization to handle either (Fs, StorageFS) or
      (StorageFS, Fs) for reads and writes, respectively.
   b) Stop passing local paths around as strings.
   c) Stop passing PNFS/Chimera paths as sometimes strings and sometimes
      as StorageFS objects.
2) Don't assume StorageFS objects are PNFS, Chimera or pnfs_agent.
   Consider lustre as an example.
"""

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
import threading
import thread
try:
    import multiprocessing
    multiprocessing_available = True
except ImportError:
    multiprocessing_available = False

# enstore modules
import Trace
import bfid_util
import e_errors
import option
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
#import atomic
import delete_at_exit
import charset
import volume_family
import enstore_constants
import enstore_functions2
import pnfs_agent_client
import checksum
import enstore_functions3
import find_pnfs_file
import udp_client
import file_utils
import cleanUDP
import namespace
import library_manager_director_client

### The following constants:
###     USE_NEW_EVENT_LOOP
###     USE_LMC_CACHE
###     USE_LM_TIMEOUT
###     USE_FIRST_REQUEST
### are not meant to be modified by the rest of the code.  They are only
### intended to be used by a developer/tester to intentionally "break"
### encp for a particular test.

#Disabling USE_NEW_EVENT_LOOP wil cause encp to wait for an LM response,
# before waiting for a mover.  With this set to true, it will wait for
# both the LM response and the mover control connection simultaniously.
#False matches the default behavior prior to 1.927.  True, is the new
# desired value so that encp can handle multi-threaded library managers.
USE_NEW_EVENT_LOOP = True
#Revision 1.909 was the first with library manager client caching.
USE_LMC_CACHE = True
#Prior to 1.927, if the library manager failed to reply, encp treated it like
# an error.  Starting with 1.927 and when USE_NEW_EVENT_LOOP is true; the
# library manager not responding was treated like RESUBMITTING and not an
# error.
USE_LM_TIMEOUT = True
#If a timeout error occurs before receiving a request, assign the blame
# for the failure to the first uncompleted request in the work list.
USE_FIRST_REQUEST = True
#Allow for the FAIL_1ST_DATA_SOCKET_LATER environmental variable to allow
# the user/tester/developer to intentionally simulate an error.  This
# particular one should only occur once; retries should not repeat it.
FAIL_1ST_DATA_SOCKET_LATER = bool(os.environ.get('FAIL_1ST_DATA_SOCKET_LATER',
                                                 False))
fail_1st_data_socket_later_state = False #Set to True after error is simulated.

#Hack for migration to report an error, instead of having to go to the log
# file for every error.
err_msg = {}
try:
    err_msg_lock = multiprocessing.Lock()
except NameError:
    err_msg_lock = threading.Lock()

#This lock prevents multiple encps from submitting requests out of order
# when run inside of migration.
try:
    start_lock = multiprocessing.Lock()
except NameError:
    start_lock = threading.Lock()

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
__lmc = None

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

#Return values to know if encp should stop or keep going.
CONTINUE_FROM_BEGINNING = 2
CONTINUE = 1
STOP = 0

#Make this shortcut so there is less to type.
fullpath = enstore_functions2.fullpath

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
TRANS_ID_LEVEL = 13

#This is the global used by print_data_access_layer_format().  It uses it to
# determine whether standard out or error is used.
#  1 - Means always print this output; regardless of success or failure.
#  0 - Means only print this output on fatal error.
# -1 - Means never print this output.
data_access_layer_requested = False

#Initial seed for generate_unique_id().
_counter = 0
#Initial seed for generate_unique_msg_id().
###_msg_counter = 0

#This is the largest 16 bit prime number.  It is used for converting the
# 1 seeded dcache CRCs with the 0 seeded enstore CRCs.
BASE = 65521

#Completion status field values.
SUCCESS = "SUCCESS"
FAILURE = "FAILURE"

############################################################################
##
## Error class used in Encp.
##
############################################################################

class EncpError(Exception):
    def __init__(self, e_errno, e_message, e_type, e_ticket={}):

        Exception.__init__(self)

        #Handle the errno (if a valid one passed in).
        if e_errno in errno.errorcode.keys():
            self.errno = e_errno
        else:
            self.errno = None

        #In python 2.6 python throws warnings for using Exception.message.
        if sys.version_info[:2] >= (2, 6):
            self.message_attribute_name = "e_message"
        else: # python 2.5 and less
            self.message_attribute_name = "message"

        #Handel the message if not given.
        if e_message == None:
            if e_errno: #By now this is None or a valid errno.
                setattr(self, self.message_attribute_name,
                        os.strerror(self.errno))
            else:
                setattr(self, self.message_attribute_name, None)
        elif type(e_message) == types.StringType:
            #There was a string message passed.
            setattr(self, self.message_attribute_name, e_message)
        else:
            setattr(self, self.message_attribute_name, None)

        #Type should be from e_errors.py.  If not specified, use errno code.
        if not e_type:
            try:
                self.type = errno.errorcode[self.errno]
            except KeyError:
                self.type = e_errors.UNKNOWN
        else:
            self.type = e_type

        self.args = (self.errno,
                     getattr(self, self.message_attribute_name),
                     self.type)

        #If no usefull information was passed in (overriding the default
        # empty dictionary) then set the ticket to being {}.
        if e_ticket == None:
            self.ticket = {}
        else:
            self.ticket = e_ticket

        #Generate the string that stringifying this obeject will give.
        self._string()

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
            self.strerror = "%s: [ ERRNO %s ] %s: %s" \
                            % (errno_name,
                               self.errno,
                               errno_description,
                               getattr(self, self.message_attribute_name))
        else:
            self.strerror = getattr(self, self.message_attribute_name)

        return self.strerror

############################################################################
##
## Misc. functions.
##
############################################################################

"""
# int32(v) -- if v > 2^31-1, make it long
#
# a quick fix for those 64 bit machine that consider int is 64 bit ...

def int32(v):
    if v > TWO_G:
        return long(v)
    else:
        return v
"""

def encp_client_version():
    ##this gets changed automatically in {enstore,encp}Cut
    ##You can edit it manually, but do not change the syntax
    version_string = "v3_11d CVS"
    encp_file = globals().get('__file__', "")
    if encp_file:
        version_string = version_string + " $Revision$ "+ os.path.basename(encp_file)
    else:
        version_string = version_string + " $Revision: 1.1008 $ <frozen>"
    #If we end up longer than the current version length supported by the
    # accounting server; truncate the string.
    if len(version_string) > MAX_VERSION_LENGTH:
	version_string = version_string[:MAX_VERSION_LENGTH]
    return version_string

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
        Trace.message(DONE_LEVEL,
                      "UNCOLLECTABLE COUNT: %s" % uncollectable_count)

    message = "[1] Time to collect garbage: %s sec." % \
              (time.time() - collect_garbage_start_time,)
    Trace.message(TIME_LEVEL, message)
    Trace.log(TIME_LEVEL, message)

#def quit(exit_code=1):
#    delete_at_exit.quit(exit_code)
#    #delete_at_exit.delete()
#    #os._exit(exit_code)

def print_error(errcode,errmsg):
    message = str(errcode) + " " + str(errmsg) + '\n'
    message = "ERROR: " + message
    try:
        sys.stderr.write(message)
        sys.stderr.flush()
    except IOError:
        pass

#def generate_unique_msg_id():
#    global _msg_counter
#    _msg_counter = _msg_counter + 1
#    return _msg_counter

unique_id_lock = threading.Lock() #protect encps within migration.
def generate_unique_id():
    global _counter
    thishost = hostaddr.gethostinfo()[0]
    unique_id_lock.acquire()
    ret = "%s-%d-%d-%d" % (thishost, int(time.time()), os.getpid(), _counter)
    _counter = _counter + 1
    unique_id_lock.release()
    return ret

def generate_location_cookie(number):
    return "0000_000000000_%07d" % int(number)

"""
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
"""

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
#Return the number of requests in the list that have NOT had a non-retriable
# error or have already finished.
def get_queue_size(request_list):
    queue_size=0
    for req in request_list:
        if not req.get('finished_state', 0):
            queue_size = queue_size + 1

    return queue_size
"""

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

"""
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
"""

############################################################################
##
## The functions in this section assume special functionality involving
## PNFS or Chimera.
##
## To Do: These dependencies should be removed for future use with Lustre.
##
############################################################################

def is_layer_access_name(filepath):
    #Determine if it is an ".(access)(pnfsid/chimeraid)(1-8)" name.
    access_match = re.compile("\.\(access\)\([0-9A-Fa-f]+\)\([1-8]\)")
    if re.search(access_match, os.path.basename(filepath)):
        return True
    return False

def is_access_name(filepath):
    #Determine if it is an ".(access)()" name.
    access_match = re.compile("\.\(access\)\([0-9A-Fa-f]+\)")
    if re.search(access_match, os.path.basename(filepath)):
        return True

    return False

def get_directory_name(filepath):
    if type(filepath) != types.StringType:
        return None

    #If we already have a directory...
    #if file_utils.wrapper(os.path.isdir, (filepath,)):
    #    return filepath

    #Determine if it is an ".(access)()" name.
    if is_access_name(filepath):
        #Since, we have the .(access)() name we need to split off the id.
        dirname, filename = os.path.split(filepath)
        pnfsid = filename[10:-1]  #len(".(access)(") == 10 and len ")" == 1

        #Create the filename to obtain the parent id.
        parent_id_name = os.path.join(dirname, ".(parent)(%s)" % pnfsid)

        #Read the parent id.
        if namespace.pnfs_agent_client_requested:
            pac = get_pac()
            #get_parent_id() will raise an exception on error.
            parent_id = pac.get_parent_id(pnfsid, rcv_timeout=5, tries=6)
            parent_name = pac.get_nameof(parent_id, rcv_timeout=5, tries=6)
        else:
            try:
                f = file_utils.open(parent_id_name, unstable_filesystem=True)
                parent_id = file_utils.readline(f,
                                           unstable_filesystem=True).strip()
                f.close()
                f = file_utils.open(os.path.join(dirname,".(nameof)(%s)"%parent_id))
                parent_name = file_utils.readline(f,unstable_filesystem=True).strip()
                f.close()
            except (OSError, IOError), msg:
                #We only need to worry about pnfs_agent_client_allowed here,
                # pnfs_agent_client_requested is addressed a few lines earlier.
                if msg.args[0] == errno.ENOENT and \
                       namespace.pnfs_agent_client_allowed:
                    pac = get_pac()
                    parent_id = pac.get_parent_id(pnfsid, rcv_timeout=5,
                                                  tries=6)
                    parent_name = pac.get_nameof(parent_id,  rcv_timeout=5, tries=6)
                    if not parent_id: #Does this work to catch errors?
                        raise OSError, msg
                else:
                    raise sys.exc_info()[0], sys.exc_info()[1], \
                          sys.exc_info()[2]
        #
        # this avoids "[Errno 40] Too many levels of symbolic links"
        # when resulting directory_name would have looked like
        # /pnfs/fs/usr/data/.(access)(PNFSID) where PNFSID is
        # pnfsid of directory "data"
        #
        if parent_name == os.path.basename(dirname) :
            dirname = os.path.dirname(dirname)
        directory_name = os.path.join(dirname, ".(access)(%s)" % parent_id)
    else:
        directory_name = os.path.dirname(filepath)

    return directory_name

#Return True if the file passed in is a pnfs file with either layer 1 or
# or layer 4 set.  False otherwise.  OSError, and IOError exceptions may
# be thrown for other errors.
def do_layers_exist(pnfs_filename, encp_intf=None):

    if not pnfs_filename:
        return False

    try:
        sfs = namespace.StorageFS(pnfs_filename)

        layer_1_filename = sfs.layer_file(pnfs_filename, 1)
        layer_4_filename = sfs.layer_file(pnfs_filename, 4)
        if _get_stat(layer_1_filename, sfs.get_stat, encp_intf)[stat.ST_SIZE] or \
               _get_stat(layer_4_filename, sfs.get_stat, encp_intf)[stat.ST_SIZE] :
            #Layers found for the file!
            return True

    except (OSError, IOError):
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]

    #The pnfs files does not exist, or it does exist and does not have
    # any layer information.
    return False

#As the name implies remove layers 1 and 4 for the indicated file.

def clear_layers_1_and_4(work_ticket):
    if is_read(work_ticket):
        use_pnfs_filename = work_ticket['infile']
        report_pnfs_filename = work_ticket['infilepath']
    else: #write
        use_pnfs_filename = work_ticket['outfile']
        report_pnfs_filename = work_ticket['outfilepath']

    if not use_pnfs_filename:
        return False

    if not namespace.is_storage_path(use_pnfs_filename):
        return False

    try:
        sfs = namespace.StorageFS(use_pnfs_filename)

        Trace.log(e_errors.INFO,
                  "Clearing layers 1 and 4 for file %s (%s)." %
                  (report_pnfs_filename, work_ticket.get('unique_id', None)))

        sfs.writelayer(1, "", use_pnfs_filename)
        sfs.writelayer(4, "", use_pnfs_filename)
    except (IOError, OSError):
        return False

    return True

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
    #if not namespace.is_storage_path(dirname, check_name_only = 1):
    #    raise EncpError(None, "Not a pnfs filename.", e_errors.WRONGPARAMETER)

    if dirname[:13] == "/pnfs/fs/usr/":
        return os.path.join("/pnfs/", filename[13:])
    elif dirname[:len(canonical_pathbase)] == canonical_pathbase:
        return os.path.join("/pnfs/", filename[19:])
    elif dirname[:6] == "/pnfs/":
        return filename
    else:
        raise EncpError(None,
                   "Unable to return enstore pnfs pathname: %s" % (filepath,),
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
    #if not namespace.is_storage_path(dirname, check_name_only = 1):
    #    raise EncpError(None, "Not a pnfs filename.", e_errors.WRONGPARAMETER)

    if dirname[:13] == "/pnfs/fs/usr/":
        return filename
    elif dirname[:len(canonical_pathbase)] == canonical_pathbase:
        #i.e. "/pnfs/fnal.gov/usr/"
        return os.path.join("/pnfs/fs/usr/", filename[19:])
    elif dirname[:6] == "/pnfs/":
        return os.path.join("/pnfs/fs/usr/", filename[6:])
    else:
        raise EncpError(None,
              "Unable to return enstore pnfs admin pathname: %s" % (filepath,),
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
    #if not namespace.is_storage_path(dirname, check_name_only = 1):
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
          "Unable to return enstore pnfs canonical pathname: %s" % (filepath,),
                        e_errors.WRONGPARAMETER)

def _get_stat(pathname, func=file_utils.get_stat, e=None):
    __pychecker__="unusednames=i"

    retries = 5
    if e and e.outtype == HSMFILE:
        # write request
        # output file does not exist
        # no need to retry
        retries = 1
    for i in range(retries):
        try:
            statinfo = func(pathname)
            return statinfo
        except (OSError, IOError), msg:
            #Historically all systems have returned ENOENT falsely when
            # a timeout occured and the file really did exist.  This also,
            # happens a lot if pnfs is automounted. One node, flxi04,
            # appears to be throwing EIO instead for these cases.
            if msg.args[0] in [errno.EIO, errno.ENOENT]:
                if retries == 1:
                    break
                time.sleep(1)
                continue
            else:
                raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]

    raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]

def get_stat(filename, e=None):
    global pnfs_is_automounted
    pathname = os.path.abspath(filename)

    try:
        #This is intentionally left as an os.stat().  For the case of
        # using the pnfs_agent it doesn't make sense to fail N times
        # here.  Thus, we fail after one time, fall into the exception
        # handling were we either retry the stat (because pnfs is not
        # robust) or we need to ask the pnfs_agent.
        if pathname.find("pnfs") != -1 and \
               namespace.pnfs_agent_client_requested:
            #We need the find() of the substring "pnfs" to quickly (as
            # compared to stat()s over (P)NFS) determine if the file is
            # likely a pnfs file.  This test should exclude most local
            # files.  There is nothing that prevents the user from having
            # the string "pnfs" in their (local) file and directory names.
            # These rare cases are handled with the is_local_path() in
            # test below.
            raise OSError(errno.ENOENT, "Force use of pnfs_agent.")
        else:
            statinfo = file_utils.get_stat(pathname)

        return statinfo
    except (OSError, IOError), msg:
        if getattr(msg, "errno", None) in [errno.ENOENT, errno.EIO]:
            if namespace.is_storage_remote_path(pathname, check_name_only = 1):
                #Also, when using the pnfs_agent, we will get ENOENT because
                # locally the file will not exist.
                try:
                    pac = get_pac()
                    statinfo = pac.get_stat(pathname)
                except (OSError, IOError), msg:
                    raise sys.exc_info()[0], sys.exc_info()[1], \
                          sys.exc_info()[2]
                return statinfo
            elif is_layer_access_name(pathname):
                # In case of layer 1 or layer 4 and error
                # call get stat from chimera/pnfs
                sfs = namespace.StorageFS(pathname)
                rc = _get_stat(pathname, sfs.get_stat, e)
                return rc
            elif pnfs_is_automounted or \
                     namespace.is_storage_local_path(pathname, check_name_only = 1):
                #Sometimes when using pnfs mounted locally the NFS client times
                # out and gives the application the error ENOENT.  When in
                # reality the file is fine when asked some time later.
                # Automounting pnfs can cause timeout problems too.
                statinfo = _get_stat(pathname, file_utils.get_stat, e)
                return statinfo
            elif is_local_path(pathname, check_name_only = 1):
                #You can only get here by choosing to name your files poorly.
                # By poorly, this means having the string "pnfs" in your
                # local (aka not in pnfs) path.  The only penalty is this
                # is a little slower, because a greater number of os.stat()
                # calls are needed to sort out the situation.
                statinfo = file_utils.get_stat(pathname)
                return statinfo

        #If this is a local file that we got an error on, raise it back to
        # the calling function.
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]


def is_local_path(filename, check_name_only = None):

    pathname = os.path.abspath(filename)

    try:
        fstats = file_utils.get_stat(pathname)
        if stat.S_ISREG(fstats[stat.ST_MODE]):
            dname = os.path.dirname(pathname)
        elif stat.S_ISDIR(fstats[stat.ST_MODE]):
            dname = pathname
        else:
            dname = pathname  #Can this happen?
    except (OSError, IOError):
        fstats = None
        dname = os.path.dirname(pathname)

    dname = os.path.dirname(pathname)
    const_name = os.path.join(dname, ".(get)(const)")

    try:
        file_utils.get_stat(const_name)
        return False
    except (OSError, IOError):
        pass

    if check_name_only:
        return True
    elif fstats:
        return True

    return False

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
            if file_utils.e_access(path, mode):
                return 1
            time.sleep(1)
            i = i + 1

        #use the effective ids and not the reals used by os.access().
        return file_utils.e_access(path, mode)
    else:
        rtn = file_utils.e_access(path, mode)
        if rtn:
            return rtn

    #Before giving up that this is a pnfs file, ask the pnfs_agent.
    # Is there a more performance efficent way?
    if namespace.pnfs_agent_client_requested or \
           namespace.pnfs_agent_client_allowed:
        pac = get_pac()
        rtn = pac.e_access(path, mode)
        return rtn

    return False

#Take as input the raw output of Pnfs.readlayer().
def parse_layer_2(data):
    # Define the match/search once before the loop.  Enstore knows only how
    # to deal with c=1 (aka adler32) checksums.
    crc_match = re.compile("[:;]c=1:[a-zA-Z0-9]{8}")
    size_match = re.compile("[:;]l=[0-9]*")

    dcache_crc_long = None
    dcache_size_long = None

    # Loop over every line in the output looking for the crc.
    for line in data:
        result = crc_match.search(line)
        if result != None:
            hex_dcache_string = "0x" + result.group().split(":")[-1]
            dcache_crc_long = long(hex_dcache_string, 16)
        result = size_match.search(line)
        if result != None:
            dcache_string = result.group().split("=")[1]
            try:
                dcache_size_long = long(dcache_string)
            except ValueError:
                dcache_size_long = None
                #We can't trust that the CRC is correct either.
                #dcache_crc_long = None

    return (dcache_crc_long, dcache_size_long)

############################################################################

##
## The functions in this section, depend upon get_stat().
##

def isdir(pathname):
    stat_info = get_stat(pathname)
    if stat_info != None:
        return stat.S_ISDIR(stat_info[stat.ST_MODE])

    #If using the pnfs_agent, None gets returned.  This isn't useful for
    # givin useful error messages.
    return None

def isfile(pathname):
    stat_info = get_stat(pathname)
    if stat_info != None:
        return stat.S_ISREG(stat_info[stat.ST_MODE])

    #If using the pnfs_agent, None gets returned.  This isn't useful for
    # givin useful error messages.
    return None

def islink(pathname):
    stat_info = get_stat(pathname)
    if stat_info != None:
        return stat.S_ISLNK(stat_info[stat.ST_MODE])

    #If using the pnfs_agent, None gets returned.  This isn't useful for
    # givin useful error messages.
    return None

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

##############################################################################
##
## These functions are used to update the time fields in the file system.
##
##############################################################################

def update_times(input_path, output_path):
    time_now = time.time()

    update_last_access_time(input_path, time_now)
    update_modification_time(output_path, time_now)

def update_modification_time(output_path, time_now = None):

    update_modification_time = time.time()

    if time_now == None:
        time_now = update_modification_time

    try:
        #Update the last modified time; set last access time to existing value.
        file_utils.utime(output_path,
                 (file_utils.get_stat(output_path)[stat.ST_ATIME], time_now),
                         unstable_filesystem=True)
    except OSError:
        return #This one will fail if the output file is /dev/null.

    message = "[1] Time to update modification time: %s sec." % \
              (time.time() - update_modification_time,)
    Trace.message(TIME_LEVEL, message)
    Trace.log(TIME_LEVEL, message)

def update_last_access_time(input_path, time_now = None):

    update_last_access_start_time = time.time()

    if time_now == None:
        time_now = update_last_access_start_time

    try:
        #Update the last access time; set last modified time to existing value.
        file_utils.utime(input_path,
                 (time_now, file_utils.get_stat(input_path)[stat.ST_MTIME]),
                         unstable_filesystem=True)
    except OSError:
        return

    message = "[1] Time to update last_access_time: %s sec." % \
              (time.time() - update_last_access_start_time,)
    Trace.message(TIME_LEVEL, message)
    Trace.log(TIME_LEVEL, message)

##############################################################################
##############################################################################

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
                            {'infilepath' : infile, 'outfilepath' : outfile})

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
        elif namespace.is_storage_path(infile) \
                 and not namespace.is_storage_path(outfile):
            return 1
        elif not namespace.is_storage_path(infile) \
                 and namespace.is_storage_path(outfile):
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

##############################################################################
##
## All the functions in this section deal with the selection of the next
## request to work on.
##
##############################################################################

def get_original_request(request_list, index_of_copy):
    oui = request_list[index_of_copy].get('original_unique_id', None)
    if oui:  #oui == Original Unique Id
        for j in range(len(request_list)):
            if oui == request_list[j].get('unique_id', None):
                return request_list[j]
            elif oui in request_list[j].get('retried_unique_ids', []):
                return request_list[j]

    return None

def did_original_succeed(request_list, index_of_copy):
    oui = request_list[index_of_copy].get('original_unique_id', None)
    if oui:  #oui == Original Unique Id
        for j in range(len(request_list)):
            if request_list[j].get('completion_status', None) == SUCCESS:
                if oui == request_list[j].get('unique_id', None):
                    return True #The original succeeded.
                elif oui in request_list[j].get('retried_unique_ids', []):
                    return True #The original succeeded.
        else:
            return False #Original not done yet or failed.

    return True #Is an original.

#Return the number of files in the list left to transfer.
def requests_outstanding(request_list):

    files_left = 0

    for i in range(len(request_list)):
        if request_list[i] == None:
            #Ignore the item if None is in the list.  This will likely happen
            # for the inner loop of write_to_hsm() all non-multiple copy
            # writes.
            #
            # Multiple copy writes will have two tickets in the list
            # for the inner loop of write_to_hsm(); one is the current request
            # being worked on, the other is the original that needed to succeed
            # in order for the multiple copies to be considered for writing.
            continue
        completion_status = request_list[i].get('completion_status', None)
        if completion_status == None:
            if request_list[i].get('copy', None) != None:
                if did_original_succeed(request_list, i):
                    #Don't worry about copies when the original failed.
                    files_left = files_left + 1
            else:
                #This is an original copy.
                files_left = files_left + 1

    return files_left

#Return the number of files in the list left to transfer for all volumes.
def all_requests_outstanding(requests):

    if type(requests) == types.ListType:
        return requests_outstanding(requests)
    elif type(requests) == types.DictType:
        files_left = 0
        for request_list in requests.values():
            files_left = files_left + requests_outstanding(request_list)
    else:
        raise TypeError("Expected List or Dictionary")

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
                    #Store the sfs id too.  This is needed for DiskMovers
                    # that hash the file location based on this ID.
                    request_list[i]['fc']['pnfsid'] = \
                                    orig_request['fc']['pnfsid']
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

#Helper function for get_success_request_count.
def __get_successes(request_list):
    successes = 0

    for i in range(len(request_list)):
        if request_list[i].get('completion_status', None) == SUCCESS:
            successes = successes + 1

    return successes

#Given a bunch of requests, return the number of them that are successes.
def get_success_request_count(requests):
    if type(requests) == types.ListType:
        return __get_successes(requests)
    elif type(requests) == types.DictType:
        successes = 0
        for request_list in requests.values():
            successes = successes + __get_successes(request_list)
    else:
        raise TypeError("Expected List or Dictionary")

    return successes

#Helper function for get_failed_request_count.
def __get_failures(request_list):
    failures = 0

    for i in range(len(request_list)):
        if request_list[i].get('completion_status', None) == FAILURE:
            failures = failures + 1

    return failures

#Given a bunch of requests, return the number of them that are successes.
def get_failures_request_count(requests):
    if type(requests) == types.ListType:
        return __get_failures(requests)
    elif type(requests) == types.DictType:
        failures = 0
        for request_list in requests.values():
            failures = failures + __get_successes(request_list)
    else:
        raise TypeError("Expected List or Dictionary")

    return failures

############################################################################
##
## All of these functions choose and cache a default client object.
##
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
    elif e_errors.is_ok(vcc.inquire_vol(volume)):
        __vcc = vcc
        __csc = csc
        return csc

    __csc = csc
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
            __csc = csc
            return csc

    __csc = csc
    return csc

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
# set_sfs_settings()                                x
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

    elif bfid_util.is_bfid(parameter):  #If passed a bfid.
        bfid = parameter

    elif enstore_functions3.is_volume(parameter):  #If passed a volume.
        volume = parameter

    elif type(parameter) == types.TupleType and len(parameter) == 2 \
             and type(parameter[0]) == types.StringType \
             and type(parameter[1]) == types.IntType:
        address = parameter

    #Remember this for comparisons later.
    #old_csc = __csc

    #Call the correct version of the underlying get_csc functions.
    if bfid:  #If passed a bfid.
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

    elif bfid_util.is_bfid(parameter):  #If passed a bfid.
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
    if __fcc != None and match_fc(__fcc.server_address):
        file_info = __fcc.bfid_info(bfid, 5, 12)
        if e_errors.is_ok(file_info):
            return __fcc, file_info
        else:
            Trace.log(e_errors.WARNING,
                      "File inquiry to cached file_clerk failed: %s" \
                      % (file_info['status'],))

    #Next check the fcc associated with the cached csc.
    if __csc != None:
        fcc = file_clerk_client.FileClient(
            __csc, logc = __logc, alarmc = __alarmc,
            rcv_timeout=5, rcv_tries=12)
        if fcc.server_address == None:
            Trace.log(e_errors.WARNING, "Locating cached file_clerk failed.")
        else:
            file_info = fcc.bfid_info(bfid, 5, 12)
            if e_errors.is_ok(file_info):
                __fcc = fcc
                return __fcc, file_info
            else:
                Trace.log(e_errors.WARNING,
                          "File inquiry to default file_clerk failed: %s" \
                          % (file_info['status'],))

    #Before checking other systems, check the default system.
    config_host = enstore_functions2.default_host()
    config_port = enstore_functions2.default_port()
    csc = configuration_client.ConfigurationClient((config_host, config_port))
    fcc = file_clerk_client.FileClient(
        csc, logc = __logc, alarmc = __alarmc, rcv_timeout=5, rcv_tries=12)
    if fcc.server_address == None:
        Trace.log(e_errors.WARNING, "Locating default file_clerk failed.")
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
        else:
            Trace.log(e_errors.WARNING,
                      "File inquiry to default file_clerk failed: %s" \
                      % (file_info['status'],))

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
                    message = "Using file_clerk at %s based on bfid %s." % \
                          (fcc_test.server_address, bfid)
                    Trace.log(e_errors.INFO, message)

                    __csc = csc_test
                    __fcc = fcc_test
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

#match_fcc() - a helper function for __get_vcc().  It makes sure the file_clerk
# address passed in matches that of the cached configuration.
def match_fc(match_fc_addr):
    if __csc != None and __fcc != None and match_fc_addr != None:
        conf_fc = __csc.get('file_clerk')
        if e_errors.is_ok(conf_fc):
            fc_addr = (conf_fc['hostip'], conf_fc['port'])
        else:
            fc_addr = None

        if fc_addr == match_fc_addr:
            return True

    return False

#match_fcc() - a helper function for __get_vcc().  It makes sure the file_clerk
# address passed in matches that of the cached configuration.
def match_vc(match_vc_addr):
    if __csc != None and __fcc != None and match_vc_addr != None:
        conf_vc = __csc.get('volume_clerk')
        if e_errors.is_ok(conf_vc):
            vc_addr = (conf_vc['hostip'], conf_vc['port'])
        else:
            vc_addr = None

        if vc_addr == match_vc_addr:
            return True

    return False


def __get_vcc(parameter = None):
    global __vcc
    global __csc
    global __acc

    ## match_fc_addr - This value is necessary to obtain the file_clerk
    ## address that the particular file was found at.  This matters if
    ## the same tape is located in two different Enstore instances; most
    ## likely a NULL tape, like NULL01.  If we don't make sure that the
    ## vcc we proceed with matches the fcc we might already have, we will
    ## have a problem.  The use of match_vc() and match_fc() are used to aid
    ## in this process.
    ##
    ## This check is only done if a ticket is passed in.  The ticket must
    ## be from get_file_clerk_info() or a work_ticket from
    ## create_read_request() or create__write_request().

    if not parameter:
        volume = None
        match_fc_addr = None

    elif type(parameter) == types.DictType: #If passed a ticket with volume.
        if parameter.has_key('fc'):
            volume = parameter.get('fc', {}).get('external_label',
                                                 parameter.get('volume', None))
            match_fc_addr = parameter.get('fc', {}).get('address', (None, None))
        elif parameter.has_key('volume'):
            volume = parameter.get('volume', None)
            match_fc_addr = None
        elif parameter.has_key('external_label'): #fc_ticket
            volume = parameter.get('external_label', None)
            match_fc_addr = parameter.get('address', (None, None))
        else:
            volume = None
            match_fc_addr = None

    elif enstore_functions3.is_volume(parameter):  #If passed a volume.
        volume = parameter
        match_fc_addr = None

    else:
        raise EncpError(None,
                        "Invalid volume (%s) specified." % parameter,
                        e_errors.WRONGPARAMETER, {})

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

    #First check that the cached version knows about the volume.  Be sure
    # to make sure the file_clerk address matches the curent configuration
    # and that the current configuration matches that of the cached
    # volume_clerk.
    if __vcc != None and \
           ((match_fc_addr and match_fc(match_fc_addr)) or not match_fc_addr) \
           and match_vc(__vcc.server_address):

        volume_info = __vcc.inquire_vol(volume, 5, 12)
        if e_errors.is_ok(volume_info):
            return __vcc, volume_info
        else:
            Trace.log(e_errors.WARNING,
                      "Volume inquiry to cached volume_clerk failed: %s" \
                      % (volume_info['status'],))
            if match_fc(match_fc_addr):
                #If the address matches the Enstore system we want,
                # return it anyway.
                return __vcc, volume_info

    #Next check the vcc associated with the cached csc.  Be sure
    # to make sure the file_clerk address matches the curent configuration.
    if __csc != None and \
           ((match_fc_addr and match_fc(match_fc_addr)) or not match_fc_addr):

        test_vcc = volume_clerk_client.VolumeClerkClient(
            __csc, logc = __logc, alarmc = __alarmc,
            rcv_timeout = 5, rcv_tries = 12)
        if test_vcc.server_address == None:
            Trace.log(e_errors.WARNING,
                      "Locating cached volume_clerk failed.")
        else:
            volume_info = test_vcc.inquire_vol(volume, 5, 12)
            if e_errors.is_ok(volume_info):
                __vcc = test_vcc
                return __vcc, volume_info
            else:
                Trace.log(e_errors.WARNING,
                          "Volume inquiry to default volume_clerk failed [1]: %s" \
                          % (volume_info['status'],))
                if match_fc(match_fc_addr):
                    #If the address matches the Enstore system we want,
                    # return it anyway.
                    return __vcc, volume_info

    # get a configuration server
    config_host = enstore_functions2.default_host()
    config_port = enstore_functions2.default_port()
    csc = configuration_client.ConfigurationClient((config_host,config_port))
    vcc = volume_clerk_client.VolumeClerkClient(csc, logc = __logc,
                                                alarmc = __alarmc)
    if vcc.server_address == None:
        Trace.log(e_errors.WARNING, "Locating default volume_clerk failed.")
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
        else:
            Trace.log(e_errors.WARNING,
                      "Volume inquiry to default volume_clerk failed [2]: %s" \
                      % (volume_info['status'],))

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
                message = "Using volume_clerk at %s based on volume %s." % \
                      (vcc_test.server_address, volume)
                Trace.log(e_errors.INFO, message)

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

def get_lmc(library, use_lmc_cache = True):
    global __lmc

    #If the shortname was supplied, make it the longname.
    if library[-16:] != ".library_manager":
        lib = library + ".library_manager"
    else:
        lib = library

    #If we disable the use of using the cached library manager, we can
    # simulate encp prior to revision 1.909.  In wait_for_message(), if
    # USE_LM_TIMEOUT is true and transaction_id_list is not empty,
    # __lmc is reset there only with the error occuring.  This allows us
    # to avoid having to figure out here if the cached lmc is still valid
    # or if the (IP, port) might have changed.
    if USE_LMC_CACHE and use_lmc_cache:
        if __lmc and __lmc.server_name == lib:
            return __lmc

    csc = get_csc()

    #Determine which IP and port to use.  By default it will use the standard
    # 'port' value from the configuration file.  However, if the configuration
    # key 'encp_port' exists then this port will be used.
    library_dict = csc.get(lib, 3, 3)
    server_address = None
    if e_errors.is_ok(library_dict):
        server_port = library_dict.get('encp_port',
                                       library_dict.get('port', None))
        if server_port:
            server_host = library_dict.get('hostip',
                                           library_dict.get('host', None))
            server_address = (server_host, server_port)

            __lmc = library_manager_client.LibraryManagerClient(
                csc, lib, logc = __logc, alarmc = __alarmc,
                rcv_timeout = 5, rcv_tries = 20, server_address = server_address)

    return __lmc

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
    if enstore_functions3.is_volume(bfid_or_volume):
        #Set the volume.
        volume = bfid_or_volume
        bfid = None
        #Get the clerk clients.
        vcc = get_vcc(volume)  #Make sure vcc is done before fcc.
        fcc = get_fcc(None)
    elif bfid_util.is_bfid(bfid_or_volume):
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

"""
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
"""

# get the configuration client and udp client and logger client
# return some information about who we are so it can be used in the ticket

def clients(intf):
    global __logc
    global __alarmc
    global __pac

    # get a configuration server client
    csc_addr = (getattr(intf, "enstore_config_host",
                        enstore_functions2.default_host()),
                getattr(intf, "enstore_config_port",
                        enstore_functions2.default_port()))
    try:
        csc, config = __get_csc(csc_addr)
    except EncpError, msg:
        return {'status' : (msg.type, str(msg))}
    except (socket.error, select.error):
        #On 11-12-2009, tracebacks were found from migration encps that
        # started failing because there were too many open files while
        # trying to instantiate client classes.  The socket.error should
        # have been caught here.  So now it is.
        return {'status' : (e_errors.NET_ERROR, str(msg))}

    #Report on the success of getting the csc and logc.
    #Trace.message(CONFIG_LEVEL, format_class_for_print(client['csc'],'csc'))
    #Trace.message(CONFIG_LEVEL, format_class_for_print(client['logc'],'logc'))

    #If we are only performing a check if a transfer will succeed (at least
    # start) we should turn off logging and alarming.
    if getattr(intf, "check", None):
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

        if namespace.pnfs_agent_client_requested or \
               namespace.pnfs_agent_client_allowed:
            pnfs_agent_config = config.get('pnfs_agent', {})

            pnfs_agent_ip = pnfs_agent_config.get('hostip', None)
            pnfs_agent_port = pnfs_agent_config.get('port', None)

            if pnfs_agent_ip and pnfs_agent_port:
                pnfs_agent_address = (pnfs_agent_ip, pnfs_agent_port)
                try:
                    __pac = pnfs_agent_client.PnfsAgentClient(
                        csc, 'ENCP', server_address=pnfs_agent_address)
                except (KeyboardInterrupt, SystemExit):
                    raise sys.exc_info()[0], sys.exc_info()[1], \
                          sys.exc_info()[2]
                except:
                    pass
            elif namespace.pnfs_agent_client_requested:
                return {'status' : (e_errors.PNFS_ERROR,
                                    "PNFS agent not found in configuration.")}
            else:
                pnfs_agent_address = None
    else:
        log_server_address = None
        alarm_server_address = None
        pnfs_agent_address = None

    #Get a logger client, this will set the global log client Trace module
    # variable.  If this is not done here, it would get done while
    # creating the client classes for the csc, vc, fc, etc.  This however
    # is to late for the first message to be logged (the one with the
    # command line).  The same applies for the alarm client.
    try:
        __logc = log_client.LoggerClient(
            csc, 'ENCP', flags = enstore_constants.NO_ALARM,
            server_address = log_server_address)
    except (KeyboardInterrupt, SystemExit):
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
    except:
        pass
    try:
        __alarmc = alarm_client.AlarmClient(
            csc, 'ENCP', flags = enstore_constants.NO_LOG,
            server_address = alarm_server_address)
    except (KeyboardInterrupt, SystemExit):
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
    except:
        pass

    #return client
    return {'status' : (e_errors.OK, None)}

############################################################################
############################################################################

#Return True if the media we are going to use is null media for null movers.
# False otherwise.
def is_null_media_type(volume_clerk_ticket):
    if volume_clerk_ticket['wrapper'] == "null":

        #Grab current configuration information.
        csc = get_csc()
        if not csc.have_complete_config:
            csc.save_and_dump()

        # Now we need to check all null movers.
        for dictkey, value in csc.saved_dict.items():
            if dictkey[-len(".mover"):] == ".mover" \
               and type(value) == types.DictType \
               and value.get('driver', None) == "NullDriver":

                if type(value['library']) == types.ListType:
                    library_list = value['library']
                else:
                    library_list = [value['library']]

                for lib in library_list:
                    if volume_clerk_ticket['library'] == lib.split(".")[0]:
                        #If we find a null mover with a library that
                        # matches that of the library we are using; we
                        # know the following is true (this is a recap):
                        # 1) --shortcut was used.
                        # 2) --override-path was not used.
                        # 3) The library we are using uses null movers.
                        #
                        return True

                else:
                    continue

                break  #stop processing the outter loop.

    return False

def max_attempts(csc, library, encp_intf):

    resend = {}
    resend['retry'] = 0
    resend['resubmits'] = 0

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

    try:
        lmc = get_lmc(lib)

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

        Trace.message(DONE_LEVEL, "LM status: %s" % status_ticket)

        return status_ticket
    except (socket.error, select.error), msg:
        #On 11-12-2009, tracebacks were found from migration encps that
        # started failing because there were too many open files while
        # trying to instantiate client classes.  The socket.error should
        # have been caught here.  So now it is.
        status = (e_errors.NET_ERROR, str(msg))
        if e.check:
            status_ticket = {'status' : status, 'exit_status' : 2}
        else:
            status_ticket = {'status' : status}
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
            status_ticket['status'] = (e_errors.NOWRITE,
                                    "%s is ignoring write requests." % lib)

        if state == e_errors.UNKNOWN:
            status_ticket['status'] = (e_errors.UNKNOWN,
                                       "Unable to determine %s state." % lib)

    if e.check and not e_errors.is_ok(status_ticket):
        #If one of these temporary library states is true and the
        # user is only checking, don't give a normal error.
        status_ticket['exit_status'] = 2

    Trace.message(DONE_LEVEL, "LM status: %s" % status_ticket)

    return status_ticket

############################################################################
##############################################################################

def print_data_access_layer_format(inputfile, outputfile, filesize, ticket):
    if data_access_layer_requested < 0:
        return

    if ticket.get('data_access_layer_printed', None) == 1:
        #Allready printed this error.  See last line of this function.
        # Also, a little hack in handle_retries....
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
    library = vc_ticket.get('library', "")
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
                                                     ("", "", "", "", ""))[1])
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
    if not inputfile and ticket.get('infilepath', None):
        inputfile = ticket['infilepath']
    if not outputfile and ticket.get('outfilepath', None):
        outputfile = ticket['outfilepath']
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
        try:
            sys.stderr.write(msg+'\n')
            sys.stderr.flush()
        except IOError:
            pass

    try:
        #dalf = Data Access Layer Format
        dalf = "INFILE=%s OUTFILE=%s FILESIZE=%s LABEL=%s LOCATION=%s " +\
               "DRIVE=%s DRIVE_SN=%s TRANSFER_TIME=%.02f "+ \
               "SEEK_TIME=%.02f MOUNT_TIME=%.02f QWAIT_TIME=%.02f " + \
               "TIME2NOW=%.02f CRC=%s STATUS=%s"
        msg_type=e_errors.ERROR
        if status == e_errors.OK:
            msg_type = e_errors.INFO
        errmsg=dalf % (inputfile, outputfile, filesize,
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
        try:
            sys.stderr.write("cannot log error message %s\n" % (errmsg,))
            sys.stderr.write("internal error %s %s\n" % (str(exc), str(msg)))
            sys.stderr.flush()
        except IOError:
            pass

    if not e_errors.is_ok(status):
        if not filesize:
            use_file_size = 0
        else:
            use_file_size = filesize
        #We need to filter out the situations where status is OK.
        # On OK status printed out with the error data_access_layer format
        # can occur with the use of --data-access-layer.  However, such
        # success should not go into the encp_error table.
        try:
            acc = get_acc()
            acc.log_encp_error(inputfile, outputfile, use_file_size,
                               storage_group, unique_id, encp_client_version(),
                               status, msg, hostname, time.time(),
                               file_family, wrapper, mover_name,
                               product_id, device_sn, rw, external_label,
                               library)
        except (KeyboardInterrupt, SystemExit):
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except:
            exc, msg = sys.exc_info()[:2]
            Trace.log(e_errors.ERROR,
                      "Unable to update accounting DB with error: (%s, %s)" % \
                      (str(exc), str(msg)))

    #Set this so that final_say() can skip printing this info.
    ticket['data_access_layer_printed'] = 1

##############################################################################
##
## These function obtain IP addresses, port numbers and listen sockets
## (if applicable) for encp to be contacted by the mover.
##
##############################################################################

def get_callback_addresses(encp_intf):

    get_callback_addresses_start_time = time.time()

    done_ticket = {'status': (e_errors.OK, None)}

    # get a port to talk on and listen for connections
    callback_addr, listen_socket = get_callback_addr()
    #If the socket does not exist, do not continue.
    if listen_socket == None:
        done_ticket = {'status':(e_errors.NET_ERROR,
                                 "Unable to obtain control socket.")}
        return done_ticket, listen_socket, callback_addr, None, None

    #If put or get is there, then do this for the "get" or "put" request.
    if hasattr(encp_intf, 'put') or hasattr(encp_intf, 'get'):
        #Get an ip and port to listen for the mover address for routing purposes.
        udp_callback_addr, udp_serv = get_udp_callback_addr(encp_intf)
        #If the socket does not exist, do not continue.
        if udp_serv.server_socket == None:
            done_ticket = {'status':(e_errors.NET_ERROR,
                                     "Unable to obtain udp socket.")}
            return done_ticket, listen_socket, callback_addr, \
                   udp_serv, udp_callback_addr
    else:
        udp_serv = None
        udp_callback_addr = None

    message = "[1] Time to get callback addresses: %s sec." % \
              (time.time() - get_callback_addresses_start_time,)
    Trace.message(TIME_LEVEL, message)
    Trace.log(TIME_LEVEL, message)

    return done_ticket, listen_socket, callback_addr, \
           udp_serv, udp_callback_addr

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
##
## The following functions check various aspects of the metadata for
## better system integrity.
##
##############################################################################

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
    if len(filename) > 0 and not charset.is_in_filenamecharset(filename):
        st = ""
        for ch in filename: #grab all illegal characters.
            if ch not in charset.filenamecharset:
                st = st + ch
        raise EncpError(errno.EILSEQ,
                        'Filepath uses non-printable characters: %s' % (st,),
                        e_errors.USERERROR)

#Make sure that the filesystem can handle the filesize.
def filesystem_check(work_ticket):

    verify_file_system_consistancy_start_time = time.time()

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
            try:
                sys.stderr.write("WARNING: %s  Continuing.\n" % (msg2,))
                sys.stderr.flush()
            except IOError:
                    pass
            return  #Nothing to test, user needs to be carefull.
        else:
            raise EncpError(getattr(msg, "errno", None), msg2,
                            e_errors.OSERROR, work_ticket)

    if bits < 0:
        #On Solaris 10 it is known that /tmp is a swap partition.  The
        # return from os.pathconf() above is -1 instead of the nuber
        # of bits supported for filesize length.  Go with 32 here.
        # if that isn't enough use --bypass-filesystem-max-filesize-check.
        bits = 32

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

    message = "[2] Time to verify file system consistancy: %s sec." % \
              (time.time() - verify_file_system_consistancy_start_time,)
    Trace.message(TIME_LEVEL, message)
    Trace.log(TIME_LEVEL, message)

#Make sure that the wrapper can handle the filesize.
def wrappersize_check(work_ticket):

    verify_wrapper_size_consistancy_start_time = time.time()

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

    message = "[2] Time to verify wrapper size consistancy: %s sec." % \
              (time.time() - verify_wrapper_size_consistancy_start_time,)
    Trace.message(TIME_LEVEL, message)
    Trace.log(TIME_LEVEL, message)

#Make sure that the library can handle the filesize.
#def librarysize_check(target_filepath, inputfile):
def librarysize_check(work_ticket):

    verify_library_size_consistancy_start_time = time.time()

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
        #First determine if the library does exist.
        csc = get_csc()
        lib_info = csc.get(use_lm, {})
        if not e_errors.is_ok(lib_info):
            raise EncpError(None, lib_info['status'][1],
                            lib_info['status'][0], work_ticket)

        #Extract the max allowable size for the given library.
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

    message = "[2] Time to verify library size consistancy: %s sec." % \
              (time.time() - verify_library_size_consistancy_start_time,)
    Trace.message(TIME_LEVEL, message)
    Trace.log(TIME_LEVEL, message)

#Make sure that the tags contain sane values for writes.  Raises and exception
# on error.
def tag_check(work_ticket):

    verify_tags_consistancy_start_time = time.time()

    #Consistancy check for valid pnfs tag values.  These values are
    # placed inside the 'vc' sub-ticket.
    tags = ["file_family", "wrapper", "file_family_width",
            "storage_group", "library"]
    for key in tags:
        if work_ticket.get('copy', None):
            #If this is a copy request (via --copies), skip this check,
            # since checking the original is good enough.  Otherwise,
            # the lack of 'file_family' (because on copies it is
            # original_file_family) would fail all copies.
            break

        try:
            #check for values that contain letters, digits and _.
            if not charset.is_in_charset(str(work_ticket['vc'][key])):
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
        expression = int(work_ticket['vc']['file_family_width']) < 0
        if expression:
            raise ValueError,(e_errors.USERERROR,
                              work_ticket['vc']['file_family_width'])
    except ValueError:
        msg="Pnfs tag, %s, requires a non-negitive integer value."\
             % ("file_family_width",)
        raise EncpError(None, str(msg), e_errors.USERERROR)

    message = "[2] Time to verify tags consistancy: %s sec." % \
              (time.time() - verify_tags_consistancy_start_time,)
    Trace.message(TIME_LEVEL, message)
    Trace.log(TIME_LEVEL, message)

#Prevent null mover requests from proceeding without NULL in the directory
# path.
def null_mover_check(work_ticket, e):
    __pychecker__="unusednames=e"
    csc = get_csc(work_ticket)
    library_name = work_ticket['vc']['library'] + ".library_manager"
    mover_list = csc.get_movers2(library_name)

    #There is a chance that the configuration has a mover in the wrong
    # library, but we'll assume that the configuration is corrent for the
    # first one in the list.
    if len(mover_list) > 0 and mover_list[0]['driver'] == "NullDriver":
        pnfsFilename = work_ticket['wrapper']['pnfsFilename']
        if pnfsFilename.find("/NULL") == -1:
            message = "NULL not in PNFS path: %s" % \
               (work_ticket['wrapper']['pnfsFilename'],)
            raise EncpError(None, message, e_errors.USERERROR)

# check the input file list for consistency
def inputfile_check(work_list, e):

    verify_input_file_consistancy_start_time = time.time()

    inputlist = []

    """
    # create internal list of input unix files even if just 1 file passed in
    if type(input_files)==types.ListType:
        inputlist=input_files
    else:
        inputlist = [input_files]
    """

    # create internal list of work tickets even if just 1 is passed in
    if type(work_list)==types.ListType:
        pass  #work_list = work_list
    else:
        work_list = [work_list]


    #Get the correct type of pnfs interface to use.
    #p = namespace.StorageFS()

    # Make sure we can open the files. If we can't, we bomb out to user
    for i in range(len(work_list)):
        work_ticket = work_list[i]

        #shortcuts.
        #outputfile_use = work_ticket['outfile']
        #outputfile_print = work_ticket['outfilepath']
        inputfile_use = work_ticket['infile']
        inputfile_print = work_ticket['infilepath']

        #If output location is /dev/null, skip the checks.
        if inputfile_use in ["/dev/zero",
                              "/dev/random", "/dev/urandom"]:
            continue

        #If we already know for the tape read (--volume) that the inputlist[i]
        # filename is foobar, then skip this test and move to the next.
        if enstore_functions3.is_location_cookie(inputfile_use) and e.volume:
            continue

        try:
            #check to make sure that the filename string doesn't have any
            # wackiness to it.
            filename_check(inputfile_print)

            # On writes, work_ticket['fc']['deleted'] won't exist yet,
            # so we handle it.
            if not e.override_deleted \
               and work_ticket['fc'].get('deleted', None) not in ['no', None]:

                #Since the file exists, we can get its stats.
                #     statinfo = os.stat(inputlist[i])
                statinfo = get_stat(inputfile_use)

                # input files can't be directories
                if stat.S_ISDIR(statinfo[stat.ST_MODE]):
                    raise EncpError(errno.EISDIR, inputfile_print,
                                    e_errors.USERERROR,
                                    {'infilepath' : inputfile_print})

                ###
                ### We should have permission checks here, based on the
                ### stat info.
                ###

                #For Reads make sure the filesystem size and the pnfs size
                # match.  If the PNFS filesystem and layer 4 sizes are
                # different, calling this function raises OSError exception.
                #if is_read(e):
                #    p.get_file_size(inputlist[i])

            inputlist.append(inputfile_print)

            # we cannot allow 2 input files to be the same
            # this will cause the 2nd to just overwrite the 1st
            try:
                match_index = inputlist[:i].index(inputlist[i])

                raise EncpError(None,
                                'Duplicate entry %s'%(inputlist[match_index],),
                                e_errors.USERERROR,
                                {'infilepath' : inputfile_print})
            except ValueError:
                pass  #There is no error.

        except EncpError:
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except (OSError, IOError):
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]

    message = "[2] Time to verify input file metadata (local): %s sec." % \
              (time.time() - verify_input_file_consistancy_start_time,)
    Trace.message(TIME_LEVEL, message)
    Trace.log(TIME_LEVEL, message)

    return

def inputfile_check_pnfs(request_list, bfid_brand, e):

    verify_input_file_consistancy_start_time = time.time()

    # create internal list of requests even if just 1 request passed in
    if type(request_list) != types.ListType:
        request_list = [request_list]


    # check the input unix file. if files don't exits, we bomb out to the user
    for i in range(0, len(request_list)):

        request = request_list[i]

        #If we already know for the tape read (--volume) that the inputlist[i]
        # filename is foobar, then skip this test and move to the next.
        if enstore_functions3.is_location_cookie(request_list[i]) and e.volume:
            continue

        try:

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
            #If copy N was specifically requested, then this is a copy.
            elif e.copy:
                is_copy = True

            #Holds the "rest" of the error strings.
            rest = {}

            #First check if the file is deleted and the override deleted
            # switch/flag has been specified by the user.
            #
            # If the user has elected to skip pnfs access (with --get-bfid
            # only) we skip this part too.
            if not (e.override_deleted and
                    request_list[i]['fc']['deleted'] != 'no') and \
                    not e.skip_pnfs:

                #Get the correct type of pnfs interface to use.
                sfs = namespace.StorageFS(request_list[i]['infile'])

                #For Reads make sure the filesystem size and the pnfs size
                # match.  If the PNFS filesystem and layer 4 sizes are
                # different, calling this function raises OSError exception.
                if is_read(e):
                    sfs.get_file_size(request_list[i]['infile'])

                #If no layer 4 is present, then report the error, raise an
                # alarm, but continue with the transfer.
                try:
                    (sfs_volume,
                     sfs_location_cookie,
                     sfs_size,
                     sfs_origff,
                     sfs_origname,
                     unused, #Mapfile is obsolete.
                     sfs_sfsid,
                     unused, #Mapfile pnfsid is obsolete.
                     sfs_bfid,
                     unused, #Drive is ignored by encp.
                     sfs_crc) \
                    = sfs.get_xreference(request_list[i]['infile'])
                except (OSError, IOError), msg:
                    raise EncpError(getattr(msg, "errno", errno.EIO),
                                    str(msg), e_errors.PNFS_ERROR, request_list[i])

                #If no layer 2 is present continue with the transfer.
                try:
                    data = sfs.readlayer(2, request_list[i]['infile'])
                    dcache_crc, dcache_size = parse_layer_2(data)
                except (OSError, IOError), msg:
                    dcache_crc, dcache_size = (None, None)

                #Get the string to name the errors to.
                sfs_name = sfs.print_id.lower()

                #Determine if the pnfs layers and the file data are consistant.

                #Start by getting the pnfs layer 4 information.
                try:
                    if sfs_volume == namespace.UNKNOWN:
                        rest['%s_volume' % (sfs_name,)] = sfs_volume
                except AttributeError:
                    sfs_volume = None
                    rest['%s_volume' % (sfs_name,)] = namespace.UNKNOWN
                try:
                    if sfs_location_cookie == namespace.UNKNOWN:
                        rest['%s_location_cookie' % (sfs_name,)] = \
                                                  sfs_location_cookie
                except AttributeError:
                    sfs_location_cookie = None
                    rest['%s_location_cookie' % (sfs_name,)] = \
                                              namespace.UNKNOWN
                try:
                    if sfs_size == namespace.UNKNOWN:
                        rest['%s_size' % (sfs_name,)] = sfs_size
                    try:
                        sfs_size = long(sfs_size)
                    except TypeError:
                        rest['%s_size_type' % (sfs_name,)] = \
                                        "%s_size contains wrong type %s." \
                                        % (sfs_name, type(sfs_size))
                except AttributeError:
                    sfs_size = None
                    rest['%s_size' % (sfs_name,)] = namespace.UNKNOWN
                try:
                    if sfs_origff == namespace.UNKNOWN:
                        rest['%s_origff' % (sfs_name,)] = sfs_origff
                except AttributeError:
                    sfs_origff = None
                    rest['%s_origff' % (sfs_name,)] = namespace.UNKNOWN
                try:
                    if sfs_origname == namespace.UNKNOWN:
                        rest['%s_origname' % (sfs_name,)] = sfs_origname
                except AttributeError:
                    sfs_origname = None
                    rest['%s_origname' % (sfs_name,)] = namespace.UNKNOWN
                #Mapfile no longer used.
                try:
                    if sfs_sfsid == namespace.UNKNOWN:
                        rest['%s_%sid' % (sfs_name, sfs_name)] = sfs_sfsid
                except AttributeError:
                    sfs_sfsid = None
                    rest['%s_%sid' % (sfs_name, sfs_name)] = namespace.UNKNOWN
                #Volume map file id no longer used.
                try:
                    if sfs_bfid == namespace.UNKNOWN:
                        rest['%s_bfid' % (sfs_name,)] = sfs_bfid
                except AttributeError:
                    sfs_bfid = None
                    rest['%s_bfid' % (sfs_name,)] = namespace.UNKNOWN
                #Origdrive has not always been recored.
                try:
                    #CRC has not always been recored.
                    try:
                        if sfs_crc != namespace.UNKNOWN:
                            sfs_crc = long(sfs_crc)
                    except TypeError:
                        rest['%s_crc_type' % (sfs_name,)] = \
                                          "%s_crc contains wrong type %s." \
                                          % (sfs_name, type(sfs_crc))
                except AttributeError:
                    sfs_crc = None
                    rest['%s_crc' % (sfs_name,)] = namespace.UNKNOWN

            #Next get the database information.
            try:
                db_volume = request['fc']['external_label']
            except (ValueError, TypeError, IndexError, KeyError):
                db_volume = None
                rest['db_volume'] = namespace.UNKNOWN
            try:
                db_location_cookie = request['fc']['location_cookie']
            except (ValueError, TypeError, IndexError, KeyError):
                db_location_cookie = None
                rest['db_location_cookie'] = namespace.UNKNOWN
            try:
                db_size = request['fc']['size']
                try:
                    db_size = long(db_size)
                except TypeError:
                    rest['db_size_type'] = "db_size contains wrong type %s." \
                                           % type(db_size)
            except (ValueError, TypeError, IndexError, KeyError):
                db_size = None
                rest['db_size'] = namespace.UNKNOWN
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
                rest['db_file_family'] = namespace.UNKNOWN
            try:
                db_sfs_name0 = request['fc']['pnfs_name0']
            except (ValueError, TypeError, IndexError, KeyError):
                db_sfs_name0 = None
                rest['db_%s_name0' % (sfs_name,)] = namespace.UNKNOWN
            try:
                db_sfsid = request['fc']['pnfsid']
            except (ValueError, TypeError, IndexError, KeyError):
                db_sfsid = None
                rest['db_%sid' % (sfs_name,)] = namespace.UNKNOWN
            try:
                db_bfid = request['fc']['bfid']
            except (ValueError, TypeError, IndexError, KeyError):
                db_bfid = None
                rest['db_bfid'] = namespace.UNKNOWN
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
                rest['db_crc'] = namespace.UNKNOWN

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
            #
            # If the user has elected to skip PNFS/Chimera access (with
            # --get-bfid only) we skip this part too.
            #
            #Not all fields get compared when reading a copy.
            if not (e.override_deleted and request['fc']['deleted'] != 'no') \
                   and not e.skip_pnfs:
                #For only those conflicting items, include them in
                # the dictionary.
                if db_volume != sfs_volume and not is_copy:
                    rest['db_volume'] = db_volume
                    rest['%s_volume' % (sfs_name,)] = sfs_volume
                    rest['volume'] = "db_volume differs from %s_volume" \
                                     % (sfs_name,)
                if not same_cookie(db_location_cookie, sfs_location_cookie) \
                       and not is_copy:
                    rest['db_location_cookie'] = db_location_cookie
                    rest['%s_location_cookie' % (sfs_name,)] = \
                                              sfs_location_cookie
                    rest['location_cookie'] = "db_location_cookie differs " \
                                              "from %s_location_cookie" \
                                              % (sfs_name,)
                if db_size != sfs_size:
                    rest['db_size'] = db_size
                    rest['%s_size' % (sfs_name,)] = sfs_size
                    rest['size'] = "db_size differs from %s_size" \
                                   % (sfs_name,)
                ##The file family check was removed for the migration project.
                #if db_file_family != pnfs_origff and not is_copy:
                #    rest['db_file_family'] = db_file_family
                #    rest['%s_origff' % (sfs_name,)] = sfs_origff
                #    rest['file_family'] = "db_file_family differs from " \
                #                          "%s_origff" % (sys_name,)
                if db_sfs_name0 != sfs_origname:
                    rest['db_pnfs_name0'] = db_sfs_name0
                    rest['%s_origname' % (sfs_name,)] = sfs_origname
                    rest['filename'] = "db_pnfs_name0 differs from " \
                                       "%s_origname" % (sfs_name,)
                #Mapfile no longer used.
                if db_sfsid != sfs_sfsid:
                    rest['db_%sid' % (sfs_name,)] = db_sfsid
                    rest['%sid' % (sfs_name)] = sfs_sfsid
                    rest['%sid' % (sfs_name,)] = \
                                "db_%sid differs from %sid" \
                                % (sfs_name, sfs_name)
                #Volume map file id no longer used.
                if db_bfid != sfs_bfid and not is_copy:
                    rest['db_bfid'] = db_bfid
                    rest['%s_bfid' % (sfs_name,)] = sfs_bfid
                    rest['bfid'] = "db_bfid differs from %s_bfid" \
                                   % (sfs_name,)
                #Origdrive has not always been recored.
                if (sfs_crc != namespace.UNKNOWN) and (db_crc != None) \
                       and (db_crc != sfs_crc):
                    #If present in layer 4 and file db compare the CRC too.
                    rest['db_crc'] = db_crc
                    rest['%s_crc' % (sfs_name,)] = sfs_crc
                    rest['crc'] = "db_crc differs from %s_crc" % (sfs_name,)

                #Verify if the dcache information in layer 2 matches.
                crc_1_seeded = checksum.convert_0_adler32_to_1_adler32(
                    db_crc, db_size)
                if (dcache_crc != None and \
                    (dcache_crc != crc_1_seeded and dcache_crc != db_crc)):
                    #At this point we know there was a CRC mismatch.  Get
                    # the current CRC value.  If the seed is set to 1,
                    # only report that CRC; if it is zero, report 0 and
                    # 1 seeded.
                    encp_dict = get_csc().get('encp')
                    if e_errors.is_ok(encp_dict) and \
                           encp_dict.get('crc_seed') == 1:
                        rest['db_crc'] = db_crc
                    else:
                        rest['db_crc_1'] = crc_1_seeded
                        rest['db_crc_0'] = db_crc
                    rest['dcache_crc'] = dcache_crc
                    rest['layer_2_crc'] = "db_crc differs from dcache_crc"
                if (dcache_size != None and dcache_size != db_size):
                    rest['db_size'] = db_size
                    rest['dcache_size'] = dcache_size
                    rest['layer_2_size'] = "db_size differs from dcache_size"

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
            if bfid_util.extract_brand(db_bfid) != bfid_brand:
                msg = "All bfids must have the same brand."
                raise EncpError(None, str(msg), e_errors.USERERROR, request)

            #Raise an EncpError if the (input)file is larger than the
            # output filesystem supports.
            if not e.bypass_filesystem_max_filesize_check:
                filesystem_check(request)

            #If using null movers, make sure of a few things.
            try:
                null_mover_check(request, e)
            except:
                Trace.handle_error(severity=99)
                raise

        except EncpError:
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except (OSError, IOError):
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]

    message = "[2] Time to verify input file metadata (pnfs): %s sec." % \
              (time.time() - verify_input_file_consistancy_start_time,)
    Trace.message(TIME_LEVEL, message)
    Trace.log(TIME_LEVEL, message)

    return


# check the output file list for consistency
# generate names based on input list if required
def outputfile_check(work_list, e):

    verify_output_file_consistancy_start_time = time.time()

    dcache = e.put_cache #being lazy

    outputlist = []

    """
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
    """

    # create internal list of work tickets even if just 1 is passed in
    if type(work_list)==types.ListType:
        pass  #work_list = work_list
    else:
        work_list = [work_list]

    # Make sure we can open the files. If we can't, we bomb out to user

    for i in range(len(work_list)):
            work_ticket = work_list[i]

            #shortcuts.
            outputfile_use = work_ticket['outfile']
            outputfile_print = work_ticket['outfilepath']
            #inputfile_use = work_ticket['infile']
            #inputfile_print = work_ticket['infilepath']

            #If output location is /dev/null, skip the checks.
            if outputfile_use in ["/dev/null", "/dev/zero",
                                  "/dev/random", "/dev/urandom"]:
                continue

            #Get the correct type of pnfs interface to use.
            sfs = namespace.StorageFS(outputfile_use)

            #check to make sure that the filename string doesn't have any
            # wackiness to it.
            filename_check(outputfile_print)

            #Grab this stat() once for all the checks about to be run.
            try:
                fstatinfo = get_stat(outputfile_use, e)
            except (OSError, IOError):
                fstatinfo = None

            #There are four (4) possible senerios for the following test(s).
            #The two conditions are:
            # 1) If dache involked encp.
            # 2) If the file does not exist or not.

            #Test case when used by a user and the file does not exist (as is
            # should be).

            #if not access_check(outputlist[i], os.F_OK) and not dcache:
            if not fstatinfo and not dcache:
                directory = get_directory_name(outputfile_use)

                try: #Grab the stat once for all of the following tests.
                    dstatinfo = get_stat(directory)
                except (OSError, IOError):
                    dstatinfo = None

                #Check for existance and write permissions to the directory.
                #if not access_check(directory, os.F_OK):
                if not dstatinfo:
                    raise EncpError(errno.ENOENT, directory,
                                    e_errors.USERERROR,
                                    {'outfilepath' : outputfile_print})

                #if not isdir(directory):
                if not stat.S_ISDIR(dstatinfo[stat.ST_MODE]):
                    raise EncpError(errno.ENOTDIR, directory,
                                    e_errors.USERERROR,
                                    {'outfilepath' : outputfile_print})

                #if not access_check(directory, os.W_OK):
                if not file_utils.e_access_cmp(dstatinfo, os.W_OK):
                    raise EncpError(errno.EACCES, directory,
                                    e_errors.USERERROR,
                                    {'outfilepath' : outputfile_print})

                #Looks like the file is good.
                outputlist.append(outputfile_print)

            #File exists when run by a normal user.
            #elif access_check(outputlist[i], os.F_OK) and not dcache:
            elif fstatinfo and not dcache:
                raise EncpError(errno.EEXIST, outputfile_print,
                                e_errors.USERERROR,
                                {'outfilepath' : outputfile_print})

            #The file does not already exits and it is a dcache transfer.
            #elif not access_check(outputlist[i], os.F_OK) and dcache:
            elif not fstatinfo and dcache:
                #Check if the filesystem is corrupted.  This entails looking
                # for directory entries without valid inodes.
                directory_listing=os.listdir(get_directory_name(outputfile_use[i]))
                if os.path.basename(outputfile_print) in directory_listing:
                    #If the platform supports EFSCORRUPTED use it.
                    # Otherwise use the generic EIO.
                    error = getattr(errno, 'EFSCORRUPTED', errno.EIO)
                    raise EncpError(error, "Filesystem is corrupt.",
                                    e_errors.FILESYSTEM_CORRUPT,
                                    {'outfilepath' : outputfile_print})
                else:
                    raise EncpError(errno.ENOENT, outputfile_print,
                                    e_errors.USERERROR,
                                    {'outfilepath' : outputfile_print})

            #The file exits, as it should, for a dache transfer.
            #elif access_check(outputlist[i], os.F_OK) and dcache:
            elif fstatinfo and dcache:
                #Do we have the ability to set the metadata after the file
                # written to tape?
                if not file_utils.e_access_cmp(fstatinfo, os.W_OK):
                    raise EncpError(errno.EACCES, outputfile_print,
                                    e_errors.USERERROR,
                                    {'outfilepath' : outputfile_print})

                #Before continuing lets check to see if layers 1 and 4 are
                # empty first.  This check is being added because it appears
                # that the dcache can (and has) written multiple copies of
                # the same file to Enstore.  The filesize being zero trick
                # doesn't work with dcache, since the dcache sets the filesize.
                try:
                    #These two test read access.  They also allow us to
                    # determine if there is already a file written to enstore
                    # with this same filename.
                    #For chimera if the layer is not present it raises no
                    # such file or directory error.  We need to ignore it
                    # if stat() on the file returns success
                    get_stat(outputfile_use)

                    layer1 = ''
                    layer4 = ''
                    try:
                        layer1 = sfs.readlayer(1, outputfile_use)
                    except:
                        # If we reach here it means layer 0 is present, but
                        # layer 1 is missing and therefore it must be chimera.
                        pass

                    try :
                        layer4 = sfs.readlayer(4, outputfile_use)
                    except:
                        # If we reach here it means layer 0 is present, but
                        # layer 4 is missing and therefore it must be chimera.
                        pass

                    #This block of code will log information.  We should
                    # never see this message, but I'm adding it because
                    # it looks like there are cases we do.  The information
                    # logged lists which layers contain only whitespace.
                    if (len(layer1) > 0 and not layer1[0].strip()) or \
                           (len(layer4) > 0 and not layer4[0].strip()):
                        message = ""
                        for layer in (1, 2, 3, 4, 5, 6, 7):
                            try:
                                #So what if we end up reading layers 1 and
                                # 4 twice.
                                layer_data = sfs.readlayer(layer,
                                                           outputfile_use)
                            except:
                                continue

                            if layer_data and \
                                   not layer_data[0].strip():
                                message = "layer%s = %s  %s" % (layer,
                                                                layer_data,
                                                                message)
                        Trace.log(e_errors.INFO,
                                  "Detected whitspace in layers: %s" % message)

                    layer1 = map(string.strip, layer1)
                    layer4 = map(string.strip, layer4)
                    #Test if the layers are empty.
                    if layer1 != [] or layer4 != []:
                        try:
                            l1_bfid = layer1[0]
                        except IndexError:
                            l1_bfid = None
                        try:
                            l4_bfid = layer4[8]
                        except IndexError:
                            l4_bfid = None
                        try:
                            l4_line1 = layer4[0]
                        except IndexError:
                            l4_line1 = None
                        l1_is_bfid = bfid_util.is_bfid(l1_bfid)
                        l4_is_bfid = bfid_util.is_bfid(l4_bfid)
                        #Log this for debugging, because users don't care.
                        if layer1:
                            Trace.log(99, "Detected layer 1: %s" % (layer1,))
                        if layer4:
                            Trace.log(99, "Detected layer 4: %s" % (layer4,))
                        #No, give the user with a correct error message
                        # without confusing them with to many details.
                        if l1_is_bfid or l4_is_bfid:
                            if l1_is_bfid and l4_is_bfid:
                                message = "Layer 1 and layer 4 are already set: %s" \
                                          % (outputfile_print,),
                            elif l1_is_bfid:
                                message = "Layer 1 is already set: %s" \
                                          % (outputfile_print,)
                            elif l4_is_bfid:
                                message = "Layer 4 is already set: %s" \
                                          % (outputfile_print,)
                            else:
                                #Impossible to get here, but handle it anyway.
                                message = "Layer 1 or layer 4 are already set: %s" \
                                          % (outputfile_print,),
                            #The layer 4 is already set.
                            raise EncpError(errno.EEXIST, message,
                                            e_errors.PNFS_ERROR,
                                            {'outfilepath' : outputfile_print})

                        elif l1_bfid or l4_line1:
                            if l1_bfid and l4_line1:
                                message = "Layer 1 and layer 4 are corrupted: %s" \
                                          % (outputfile_print,)
                            elif l1_bfid:
                                message = "Layer 1 is corrupted: %s" \
                                          % (outputfile_print,)
                            elif l4_line1:
                                message = "Layer 4 is corrupted: %s" \
                                          % (outputfile_print,)
                            else:
                                #Impossible to get here, but handle it anyway.
                                message = "Layer 1 or layer 4 are corrupted: %s" \
                                          % (outputfile_print,)

                            #The layers are corrupted.
                            raise EncpError(errno.EEXIST, message,
                                            e_errors.PNFS_ERROR,
                                            {'outfilepath' : outputfile_print})
                        else:
                            #We are ignoring the whitespace.
                            outputlist.append(outputfile_print)
                            pass
                    else:
                        #The layers are empty.
                        outputlist.append(outputfile_print)
                        pass


                    #Match the effective IDs of the file.
###                    file_utils.match_euid_egid(outputfile_use)

                    #Try to write an empty string to layer 1.  If this fails,
                    # it will most likely fail because of:
                    # 1) a lack of permission access to the file (EACCES)
                    # 2) the database is read-only (EPERM)
                    # 3) the user writing (usually root) is not the
                    #    owner of the file and the node is not in the trusted
                    #    list for pnfs (EPERM)
                    # 4) user root is modifying something outside of the
                    #    /pnfs/fs/usr/xyz/ filesystem (EPERM).
                    y = "%s(1)" % (outputfile_use,)
                    try:
                        fp=open(y, "w")
                        fp.write("")
                        fp.close()
                    except:
                        print "OPEN failed", sys.exc_info()[1]
                    try:
                        #print os.stat(outputfile_use)
                        os.system("stat '%s'" % (y,))
                    except:
                        print "STAT failed"
                    try:

                        sfs.writelayer(1, "", outputfile_use)
                        #Release the lock.
###                        file_utils.end_euid_egid(reset_ids_back = True)
                    except:
                        #Release the lock.
###                        file_utils.end_euid_egid(reset_ids_back = True)
                        raise sys.exc_info()[0], sys.exc_info()[1], \
                              sys.exc_info()[2]

                    #Get the outfile size.
                    ofilesize = long(fstatinfo[stat.ST_SIZE])

                    #Get the infile size.
                    ### There should be a way to eliminate this stat() call,
                    ### but that will take a bit of refactoring.
                    ifilesize = long(work_ticket['file_size'])

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
                                        {'outfilepath' : outputfile_print})
                except (OSError, IOError), msg:
                    #Some other non-foreseen error has occured.
                    error = getattr(msg, "errno", None)
                    if error == errno.EPERM or error == errno.EACCES:
                        enstore_error = e_errors.USERERROR
                    else:
                        enstore_error = e_errors.PNFS_ERROR
                    raise EncpError(error,
                        "Unable to verify output file: %s" % (msg.filename),
                        enstore_error)

            else:
                raise EncpError(None,
                         "Failed outputfile check for: %s" % outputfile_print,
                                e_errors.UNKNOWN,
                                {'outfilepath' : outputfile_print})

            # we cannot allow 2 output files to be the same
            # this will cause the 2nd to just overwrite the 1st
            # In principle, this is already taken care of in the
            # inputfile_check, but do it again just to make sure in case
            # someone changes protocol
            try:
                match_index = outputlist[:i].index(outputlist[i])
                raise EncpError(None,
                            'Duplicate entry %s' % (outputlist[match_index],),
                                e_errors.USERERROR,
                                {'outfilepath' : outputfile_print})
            except ValueError:
                pass  #There is no error.

    message = "[2] Time to verify output file metadata: %s sec." % \
              (time.time() - verify_output_file_consistancy_start_time,)
    Trace.message(TIME_LEVEL, message)
    Trace.log(TIME_LEVEL, message)

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

##############################################################################
##
## These functions create the zero length output files.
##
##############################################################################

## create_zero_length_pnfs_files() and create_zero_length_local_files()
## raise an OSError exception on error.

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
            fname = f['outfile']
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
                    fd = file_utils.open_fd(fname,
                                            os.O_CREAT|os.O_EXCL|os.O_RDWR,
                                            mode = 0666,
                                            unstable_filesystem=True)

                    if type(f) == types.DictType:
                        #The inode is used later on to determine if
                        # another process has deleted or removed the
                        # remote pnfs file.
                        f['wrapper']['inode'] = os.fstat(fd)[stat.ST_INO]
                        #The pnfs id is used to track down the new paths
                        # to files that were moved before encp completes.
                        sfs = namespace.StorageFS(fname)
                        sfs_id = sfs.get_id()
                        f['fc']['pnfsid'] = sfs_id
                        #The access name will allow for more efficent
                        # error handling.  Then do a switcheroo with the
                        # access name versus the normal name for cleanup.
                        f['outfile'] = sfs.access_file(os.path.dirname(fname),
                                                       sfs_id)
                        delete_at_exit.register(f['outfile'])
                        delete_at_exit.unregister(fname)

                    os.close(fd)
                    local_errno = 0
                except (OSError, IOError), msg:
                    #TO DO
                    Trace.handle_error()
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
                f['local_inode'] = file_utils.get_stat(fname)[stat.ST_INO]
                return
        else:
            if f in ["/dev/null", "/dev/zero",
                     "/dev/random", "/dev/urandom"]:
                return
            else:
                fname = f

        delete_at_exit.register(fname)

        fd = file_utils.open_fd(fname, os.O_CREAT|os.O_EXCL|os.O_RDWR,
                                mode=0666) #raises OSError on error.

        if type(f) == types.DictType:
            #The inode is used later on to determine if another process
            # has deleted or removed the local file.
            f['local_inode'] = os.fstat(fd)[stat.ST_INO]

        os.close(fd)


############################################################################
##
## The following functions return information that is used to fill in the
## 'wrapper' field in the request tickets.
##
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
       e != None and (e.put_cache or e.migration_or_duplication):
        # For the case of dcache writes into enstore; we should use the
        # ownership of the zero length pnfs file (obtained from get_pinfo())
        # created by dCache.
        #
        # For migration purposes, we need to do the same.
        return {}

    if uinfo['uid'] == 0 and uinfo['gid'] == 0 \
       and e != None and (e.put_cache or e.migration_or_duplication):
        # For the case of dcache writes into enstore; we should use the
        # ownership of the zero length pnfs file created by dCache.
        #
        # For migration purposes, we need to do the same.
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
        finfo['mode'] = file_utils.get_stat(local_file)[stat.ST_MODE]
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

    dir_list = []
    try:
        dir_list.append(get_enstore_pnfs_path(inputfile))
    except EncpError:
        pass
    try:
        dir_list.append(get_enstore_fs_path(inputfile))
    except EncpError:
        pass
    try:
        dir_list.append(get_enstore_canonical_path(inputfile))
    except EncpError:
        pass

    for fname in dir_list:
        if access_check(fname, os.F_OK):
            return fname

    #There is a real problem with the file.  Fail the transfer.
    raise EncpError(errno.ENOENT, inputfile,
                    e_errors.USERERROR,
                    {'infilepath' : inputfile})

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
          file_utils.wrapper(os.path.isdir, (ofullname,))):
        munge_name = True
    elif (len(e.input) == 1 and e.outtype == UNIXFILE and \
          file_utils.wrapper(os.path.isdir, (ofullname,))):
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
##
## The following functions initiate communication with the Mover.
##
############################################################################

#The following function is the depricated functionality of
# open_routing_socket().  Since "get" still uses this socket, this
# functionality must remain somewhere.
def open_udp_server(udp_serv, unique_id_list, encp_intf):

    time_to_open_udp_server = time.time()

    Trace.message(INFO_LEVEL, "Waiting for udp callback at address: %s" %
                  str(udp_serv.server_socket.getsockname()))
    Trace.log(e_errors.INFO, "Waiting for udp callback at address: %s" %
                  str(udp_serv.server_socket.getsockname()))

    udp_ticket = None

    if not udp_serv:
        return udp_ticket

    start_time = time.time()

    while(time.time() - start_time < encp_intf.resubmit_timeout):
        try:
            #Get the udp ticket.
            udp_ticket = udp_serv.do_request()
        except socket.error, msg:
            Trace.log(e_errors.ERROR, str(msg))
            raise EncpError(msg.args[0], str(msg),
                            e_errors.NET_ERROR)

        #If udp_serv.process_request() fails it returns None.
        if type(udp_ticket) != types.DictionaryType:
            continue
        #Something really bad happened.
        elif e_errors.is_non_retriable(udp_ticket.get('status', None)):
            break   #Process the error.
        #If udp_serv.process_request() returns correct value.
        elif udp_ticket.has_key('unique_id') and \
                 udp_ticket['unique_id'] in unique_id_list:
            break   #Return the response.
        #It is not what we were looking for.
        else:
            continue
    else:
        raise EncpError(errno.ETIMEDOUT,
                        "Mover did not call udp back.", e_errors.TIMEDOUT)

    udp_serv.reply_to_caller(udp_ticket)

    message = "[1] Time to open udp socket: %s sec." % \
              (time.time() - time_to_open_udp_server,)
    Trace.message(TIME_LEVEL, message)
    Trace.log(TIME_LEVEL, message)

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
        Trace.message(TRANSFER_LEVEL, "Choosing interface: %s" % if_name)
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

    message = "[1] Time to open routing socket: %s sec." % \
              (time.time() - time_to_open_routing_socket,)
    Trace.message(TIME_LEVEL, message)
    Trace.log(TIME_LEVEL, message)

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

        time_to_select_control_socket = time.time()

        read_fds, unused, unused = select.select([listen_socket], [], [],
                                                 wait_left_time)

        message = "[1] Time to select control socket: %s sec." % \
                  (time.time() - time_to_select_control_socket,)
        Trace.message(TIME_LEVEL, message)
        Trace.log(TIME_LEVEL, message)

        #If there are no successful connected sockets, then select timedout.
        if not read_fds:
            raise EncpError(errno.ETIMEDOUT,
                            "Mover did not call control back.",
                            e_errors.TIMEDOUT)

        time_to_accept_control_socket = time.time()

        control_socket, address = listen_socket.accept()

        message = "[1] Time to accept control socket: %s sec." % \
                  (time.time() - time_to_accept_control_socket,)
        Trace.message(TIME_LEVEL, message)
        Trace.log(TIME_LEVEL, message)

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

        time_to_setsockopt_control_socket = time.time()

        try:
            #This should help the connection.  It also seems to allow the
            # connection to survive the antispoofing/routing problem for
            # properly configured (enstore.conf & enroute2) multihomed nodes.
            control_socket.setsockopt(socket.IPPROTO_IP, socket.IP_TOS,
                                     socket.IPTOS_LOWDELAY)
        except socket.error, msg:
            try:
                sys.stderr.write("Socket error setting IPTOS_LOWDELAY option: %s\n"
                                 % str(msg))
                sys.stderr.flush()
            except IOError:
                pass

        message = "[1] Time to setsockopt control socket: %s sec." % \
                  (time.time() - time_to_setsockopt_control_socket,)
        Trace.message(TIME_LEVEL, message)
        Trace.log(TIME_LEVEL, message)

        time_to_read_control_socket = time.time()

        try:
            #Set the timeout for this part to 30 seconds.  On 12-3-2009,
            # it was observed that movers where unable to establish
            # control connections and send the control ticket because the
            # encps where sitting in read_tcp_obj() waiting on a bogus
            # security scan connection for a control ticket that
            # never arives.
            ticket = callback.read_tcp_obj(control_socket, timeout = 30)
        except (select.error, socket.error, e_errors.EnstoreError):
            try:
                control_socket.close()
            except socket.error:
                pass
            wait_left_time = start_wait_time + mover_timeout - time.time()
            wait_left_time = max(wait_left_time, 0)
            continue
        except e_errors.TCP_EXCEPTION:
            try:
                control_socket.close()
            except socket.error:
                pass
            wait_left_time = start_wait_time + mover_timeout - time.time()
            wait_left_time = max(wait_left_time, 0)
            continue

        message = "[1] Time to read control socket: %s sec." % \
                  (time.time() - time_to_read_control_socket,)
        Trace.message(TIME_LEVEL, message)
        Trace.log(TIME_LEVEL, message)

        if not e_errors.is_ok(ticket):
            #If the mover already returned an error, don't bother checking if
            # it is closed already...
            return control_socket, address, ticket

        break
    else:
        #We left the loop, and got here, because we ran out of time.
        raise EncpError(errno.ETIMEDOUT,
                        "Mover did not call control back.",
                        e_errors.TIMEDOUT)

    message = "Control socket %s is connected to %s for %s." % \
              (control_socket.getsockname(),
               control_socket.getpeername(),
               ticket.get('unique_id', "Unknown")) + elapsed_string()
    Trace.message(INFO_LEVEL, message)
    Trace.log(e_errors.INFO, message)

    message = "[1] Time to open control socket: %s sec." % \
              (time.time() - time_to_open_control_socket,)
    Trace.message(TIME_LEVEL, message)
    Trace.log(TIME_LEVEL, message)

    #Perform one last test of the socket.  This should never fail, but
    # on occasion does...
    fds, unused, unused = select.select([control_socket], [], [], 0.0)
    if fds:
        try:
            bytes_queued = callback.get_socket_read_queue_length(control_socket)
            if bytes_queued == 0:
                Trace.log(e_errors.INFO, "No socket read queued count.")

        except AttributeError:
            #FIONREAD not known on this system.
            bytes_queued = 0
        except IOError, msg:
            Trace.log(e_errors.ERROR,
                      "timeout_recv(): ioctl(FIONREAD): %s" % (str(msg),))
            bytes_queued = 0

        if bytes_queued > 0:
            try:
                ticket = callback.read_tcp_obj(control_socket)

                Trace.log(e_errors.INFO,
                          "Received second ticket in open_control_socket: %s" %
                          (str(ticket),))
            except (select.error, socket.error, e_errors.EnstoreError), msg:
                raise EncpError(msg.errno, str(msg), e_errors.NET_ERROR, ticket)
            except e_errors.TCP_EXCEPTION:
                raise EncpError(errno.ENOTCONN,
                                "Control socket no longer usable after initalization.",
                                e_errors.TCP_EXCEPTION, ticket)

    #Check for additional connection attempts we aren't ready for.
    read_fds, unused, unused = select.select([listen_socket], [], [], 0)
    if read_fds:
        message = "Detected additional connection attempt that is not expected."
        Trace.log(e_errors.INFO, message)

    return control_socket, address, ticket

##############################################################################

def open_data_socket(mover_addr, interface_ip = None):
    global fail_1st_data_socket_later_state #For error simulating.

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
            message = "Connecting data socket failed immediatly: %s" \
                      % (str(msg),)
            Trace.log(e_errors.ERROR, message)
            Trace.trace(12, message)
            raise socket.error, msg

    #Check if the socket is open for reading and/or writing.
    while 1:
        try:
            r, w, unused = select.select([data_path_socket],
                                         [data_path_socket],[],30)
            break
        except (socket.error, select.error), msg:
            if msg.args[0] in [errno.EINTR, errno.EAGAIN]:
                #Screen out interuptions from signals.
                continue
            else:
                raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]


    if r or w:
        #Get the socket error condition...
        rtn = data_path_socket.getsockopt(socket.SOL_SOCKET,
                                          socket.SO_ERROR)

        #If the environmental variable FAIL_1ST_DATA_SOCKET_LATER is boolean
        # true, give an error here, but only for the first try.  All retries
        # should succeed.
        if FAIL_1ST_DATA_SOCKET_LATER and not fail_1st_data_socket_later_state:
            rtn = 111  #111 is "Connection refused"
            fail_1st_data_socket_later_state = True  #Don't do this again.

            message = "Simulating FAIL_1ST_DATA_SOCKET_LATER error."
            Trace.log(e_errors.INFO, message)
            Trace.message(ERROR_LEVEL, message, out_fp=sys.stderr)

    #If the select didn't return sockets ready for read or write, then the
    # connection timed out.
    else:
        raise socket.error(errno.ETIMEDOUT, os.strerror(errno.ETIMEDOUT))

    #If the return value is zero then success, otherwise it failed.
    if rtn != 0:
        message = "Connecting data socket failed later: %s" \
                  % (os.strerror(rtn),)
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
        try:
            sys.stderr.write("Socket error setting IPTOS_THROUGHPUT option: %s\n" %
                             str(msg))
            sys.stderr.flush()
        except IOError:
            pass

    message = "[1] Time to open data socket: %s sec." % \
              (time.time() - time_to_open_data_socket,)
    Trace.message(TIME_LEVEL, message)
    Trace.log(TIME_LEVEL, message)

    return data_path_socket

############################################################################

#Initiats the handshake with the mover.
#Args:
# listen_socket - The listen socket returned from a callback.get_callback().
# udp_serv - An instance of a UDPServer or None.
# work_tickets - List of dictionaries, where each dictionary represents
#                a transfer.
# encp_intf - Interface class with options specified on the command line.
#Returns:
# (control_socket, data_socket, ticket)

def mover_handshake(listen_socket, udp_serv, work_tickets, encp_intf):
    control_socket, ticket = mover_handshake_part1(listen_socket, udp_serv,
                                                   work_tickets, encp_intf)

    if not e_errors.is_ok(ticket):
        return None, None, ticket

    data_path_socket, ticket = mover_handshake_part2(ticket, encp_intf)

    if not e_errors.is_ok(ticket):
        close_descriptors(control_socket)

    #The following three lines are useful for testing error handling.
    #control_socket.close()
    #data_path_socket.close()
    #ticket['status'] = (e_errors.NET_ERROR, "because I closed them")

    return control_socket, data_path_socket, ticket

def mover_handshake_part1(listen_socket, udp_serv, work_tickets, encp_intf):
    use_listen_socket = listen_socket
    ticket = {}
    #config = host_config.get_config()
    unique_id_list = []
    for work_ticket in work_tickets:
        unique_id_list.append(work_ticket['unique_id'])

    ##################################################################
    #This udp_server code is depricated.
    ##################################################################

    if udp_serv and work_tickets[0].get('route_selection', None):
        do_udp_server = True
    else:
        do_udp_server = False

    #Open the udp_serv.
    if do_udp_server:
        #Grab a new clean udp_server.
        ###udp_callback_addr, unused = get_udp_callback_addr(encp_intf, udp_serv)
        #The ticket item of 'routing_callback_addr' is a legacy name.
        ###request['routing_callback_addr'] = udp_callback_addr

        try:
            message = "Waiting for udp message at: %s." % \
		      str(udp_serv.server_socket.getsockname())
	    Trace.message(TRANSFER_LEVEL, message)
	    Trace.log(e_errors.INFO, message)

            #Keep looping until one of these two messages arives.
            # Ignore any other that my be received.
            uticket = open_udp_server(udp_serv, unique_id_list,
                                      encp_intf)

            #If requested output the raw message.
            Trace.message(TICKET_LEVEL, "RTICKET MESSAGE:")
            Trace.message(TICKET_LEVEL, pprint.pformat(uticket))

	    if not e_errors.is_ok(uticket):
		#Log the error.
		Trace.log(e_errors.ERROR,
			  "Unable to connect udp socket: %s" %
			  (str(uticket['status'])))
		return None, uticket

	    Trace.message(TRANSFER_LEVEL, "Opened udp socket.")
	    Trace.log(e_errors.INFO, "Opened udp socket.")
        except (EncpError,), detail:
            if getattr(detail, "errno", None) == errno.ETIMEDOUT:
	        #Handle retries needs to be called to update various values
                # and to perfrom the resubmition itself.
                ticket['status'] = (e_errors.RESUBMITTING, None)
            else:
	        #Handle retries needs to be called to update various values
                # and to perfrom the resubmition itself.
                ticket['status'] = (detail.type, str(detail))

	    #Log the error.
            Trace.log(e_errors.ERROR, "Unable to connect udp socket: %s" %
                      (str(ticket['status']),))
            return None, ticket

        #Print out the final ticket.
        Trace.message(TICKET_LEVEL, "UDP TICKET:")
        Trace.message(TICKET_LEVEL, pprint.pformat(uticket))
    ##################################################################
    #End of depricated udp_server code.
    ##################################################################

    message = "Listening for control socket at: %s" \
              % str(listen_socket.getsockname())
    Trace.message(INFO_LEVEL, message)
    Trace.log(e_errors.INFO, message)

    start_time = time.time()
    while time.time() < start_time + encp_intf.resubmit_timeout:
        duration = max(start_time + encp_intf.resubmit_timeout - time.time(),0)
        if do_udp_server:  #DEPRICATED
            duration = min(duration, 5)

        #Attempt to get the control socket connected with the mover.
        try:
		control_socket, mover_address, ticket = open_control_socket(
		    use_listen_socket, duration)
        except (socket.error, select.error, EncpError), msg:
            if msg.args[0] in [errno.EINTR, errno.EAGAIN]:
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
                if do_udp_server and duration > 0:  #DEPRICATED
                    #For the udp_serv situation, we need to send the reply
                    # again and go back to waiting for the control socket
                    # to connect.
                    udp_serv.reply_to_caller(uticket)
                else:
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
            return None, ticket

        Trace.message(TICKET_LEVEL, "MOVER HANDSHAKE (CONTROL):")
        Trace.message(TICKET_LEVEL, pprint.pformat(ticket))
        #Recored the receiving of the first control socket message.
        message = "Received callback ticket from mover %s for transfer %s." % \
                  (ticket.get('mover', {}).get('name', "Unknown"),
                   ticket.get('unique_id', "Unknown")) + elapsed_string()
        Trace.message(INFO_LEVEL, message)
        Trace.log(e_errors.INFO, message)


        #Verify that the id is one that we are excpeting and not one that got
        # lost in the ether.
        #
        # Check this before checking the status of the ticket.  It would be
        # bad if we were to fail a transfer, because an impostor sent an error.
        if ticket.get('unique_id', None) not in unique_id_list:
            Trace.log(e_errors.INFO,
                      "mover handshake: mover impostor called back"
                      "   mover address: %s   got id: %s   expected: %s"
                      "   ticket=%s" %
                      (mover_address, ticket.get('unique_id', None),
                       unique_id_list, ticket))
            close_descriptors(control_socket)
            continue

        # We have a reply that we are looking for, how's the status?
        if not e_errors.is_ok(ticket['status']):
            close_descriptors(control_socket)
            return None, ticket

        #Success!
        return control_socket, ticket

    ticket['status'] = (e_errors.TIMEDOUT, "Mover did not call back.")
    return None, ticket

def mover_handshake_part2(ticket, encp_intf):
    start_time = time.time()
    while time.time() < start_time + encp_intf.resubmit_timeout:
        try:
            mover_addr = ticket['mover']['callback_addr']
        except KeyError:
            msg = sys.exc_info()[1]
            try:
                sys.stderr.write("Sub ticket 'mover' not found.\n")
                sys.stderr.write("%s: %s\n" % (e_errors.KEYERROR, str(msg)))
                sys.stderr.write(pprint.pformat(ticket)+"\n")
                sys.stderr.flush()
            except IOError:
                pass
            if e_errors.is_ok(ticket.get('status', (None, None))):
                ticket['status'] = (e_errors.KEYERROR, str(msg))
            return None, ticket

	try:
            #There is no need to do this on a non-multihomed machine.
	    config = host_config.get_config()
            if config and config.get('interface', None):
		local_intf_ip = open_routing_socket(mover_addr[0], encp_intf)
	    else:
		local_intf_ip = ticket['callback_addr'][0]
        except (EncpError,), msg:
	    ticket['status'] = (e_errors.EPROTO, str(msg))
	    return None, ticket

        #Attempt to get the data socket connected with the mover.
        try:
            Trace.log(e_errors.INFO,
                      "Attempting to connect data socket to mover at %s." \
                      % (mover_addr,))
            data_path_socket = open_data_socket(mover_addr, local_intf_ip)

            if not data_path_socket:
                raise socket.error,(errno.ENOTCONN,os.strerror(errno.ENOTCONN))

        except (socket.error,), msg:
            #exc, msg = sys.exc_info()[:2]
            ticket['status'] = (e_errors.NET_ERROR, str(msg))
            #Trace.log(e_errors.INFO, str(msg))
            #close_descriptors(control_socket)
            #Since an error occured, just return it.
            return None, ticket

        #We need to specifiy which interface was used on the encp side.
        #ticket['encp_ip'] =  use_listen_socket.getsockname()[0]
	#ticket['encp_ip'] = local_intf_ip
	ticket['encp_ip'] = data_path_socket.getsockname()[0]
        #If we got here then the status is OK.
        ticket['status'] = (e_errors.OK, None)
        #Include new info from mover.
        ### Should be done indirectly through references.
        #work_tickets[i] = ticket


        return data_path_socket, ticket

    ticket['status'] = (e_errors.TIMEDOUT, "Mover did not call back.")
    return None, ticket

#Wait for the LM to reply that it received the resubmission and also wait for
# the mover to connect the control socket.  We need to be able to wait for
# both with the new multi-threaded library manager.
#
# Allow get/put to specify their own mover_handshake() function.
def wait_for_message(listen_socket, lmc, work_list,
                     transaction_id_list, e, udp_serv = None,
                     mover_handshake = mover_handshake):

    lmc_socket = lmc.u.get_tsd().socket

    if isinstance(udp_serv, udp_server.UDPServer):
        #This is for get and put.
        udp_socket = udp_serv.server_socket
        select_list = [listen_socket, udp_socket]
    else:
        udp_socket = None
        select_list = [listen_socket]
    #Only include lmc_socket if we have something in the transaction_id_list.
    # It appears that messages can arrive unsolicated from the LM; this check
    # is needed to protect against waiting forever inside
    # udp_client.__received_deferred().
    if len(transaction_id_list) > 0:
        select_list.append(lmc_socket)

    #Wait for a message or socket connection.
    Trace.log(99, "transaction_id_list: %s" % (transaction_id_list,))
    if USE_NEW_EVENT_LOOP and transaction_id_list:
        #Send to the debug log information about the current ids we are
        # waiting for and those that we have queued up.  This should
        # never grow to a list larger than one; if it does then there
        # likely is a bug somewhere.
        Trace.log(TRANS_ID_LEVEL,
                  "waiting for: %s or connections to %s" % \
                  (transaction_id_list, listen_socket.getsockname()))
        queued_sent_ids = getattr(lmc.u.get_tsd(), "send_queue", {}).keys()
        queued_recv_ids = getattr(lmc.u.get_tsd(), "recv_queue", {}).keys()
        Trace.log(TRANS_ID_LEVEL, "queued sent ids: %s  recv ids: %s" % \
                  (queued_sent_ids, queued_recv_ids))

        #If we are using the new event loop and we have pending answers for UDP
        # replies that could arrive at any time, we need to execute this
        # nasty loop to be able to resend messages that have not received a
        # response.

        loop_start_time = time.time()
        exp = 0
        base_timeout = 5 #seconds
        while loop_start_time + e.resubmit_timeout > time.time():
            #We need to do this geometric timeout ourselves.  udp_client can't
            # because we need to wait for multiple things.
            timeout = base_timeout * (pow(2, exp))
            if exp < udp_client.MAX_EXPONENT:
                exp = exp + 1
            #Limit the timeout to what is left of the entire resubmit timeout.
            upper_limit = max(0,
                          loop_start_time + e.resubmit_timeout - time.time())
            timeout = min(timeout, upper_limit)

            #Listen for the control socket to connect or the library
            # manager's repsonse to the request resubmission.
            try:
                r, unused, unused, unused = cleanUDP.Select(select_list,
                                                            [], [], timeout)
            except (socket.error, select.error), msg:
                response_ticket = {'status' : (e_errors.RESUBMITTING,
                                               str(msg))}
                return response_ticket, None, None

            #If we got nothing back, keep looping.
            if r == []:
                if transaction_id_list:
                    #Since we are still waiting for a response, resend
                    # the original message.
                    #A better mechanism will be needed if more than just
                    # the lmc will do things like this.
#                    print "RESENDING:", transaction_id_list
                    Trace.log(TRANS_ID_LEVEL,
                              "repeating ids: %s" % (transaction_id_list,))
                    lmc.u.repeat_deferred(transaction_id_list)

                continue

            break
        else:
            r = [] #We don't have a message or connection.  Define this if
                   # the user specified "--resubmit_timeout 0" on the command
                   # line.

            if transaction_id_list:
                #We've timedout.
                #A better mechanism will be needed if more than just
                # the lmc will do things like this.
                #                print "DROPPING:", transaction_id_list
                Trace.log(TRANS_ID_LEVEL,
                          "dropping ids: %s" % (transaction_id_list,))
                lmc.u.drop_deferred(transaction_id_list)

    else:
        #There are no UDP messages to worry about resending while waiting.

        #Listen for the control socket to connect or the library
        # manager's repsonse to the request resubmission.
        try:
            r, unused, unused, timeout = cleanUDP.Select(select_list, [], [],
                                                         e.resubmit_timeout)
        except (socket.error, select.error), msg:
            response_ticket = {'status' : (e_errors.RESUBMITTING, str(msg))}
            return response_ticket, None, None


    #Get the packets specified one at a time.
    Trace.log(99, "lmc_socket: %s   r: %s" % (lmc_socket, r))
    if lmc_socket in r:
        Trace.log(TRANS_ID_LEVEL, "getting LM message")
        response_ticket, transaction_id = submit_all_request_recv(
            transaction_id_list, work_list, lmc, e)
        #We need to change the timedout errors to resumbitting ones.
        #  RESUBMITTING is handled differently by handle_retries().
        if e_errors.is_timedout(response_ticket):
            response_ticket['status'] = (e_errors.RESUBMITTING, None)

        control_socket = None
        data_path_socket = None

        #Encasing the transaction ID in a list is done only to make the
        # output string for it look like the others, which really are
        # lists.
        Trace.log(TRANS_ID_LEVEL,
                  "received id: %s" % ([transaction_id],))
    elif listen_socket in r or (udp_socket and udp_socket in r):
        Trace.log(TRANS_ID_LEVEL, "starting mover handshake")
        #Wait for the mover to establish the control socket.  See
        # if the id matches one of the tickets we submitted.
        # Establish data socket connection with the mover.
        mover_handshake_response = mover_handshake(listen_socket, udp_serv,
                                                   work_list, e)
        if len(mover_handshake_response) == 2:
            #For get and put.
            control_socket = mover_handshake_response[0]
            data_path_socket = None
            response_ticket = mover_handshake_response[1]
        elif len(mover_handshake_response) >= 3:
            #For encp.
            control_socket = mover_handshake_response[0]
            data_path_socket = mover_handshake_response[1]
            response_ticket = mover_handshake_response[2]
    elif USE_LM_TIMEOUT and transaction_id_list: #We got nothing.
        #We did not get any response back from the library manager.  Allow
        # this situation to be an error.
        message = "never received response from %s" % (lmc.server_name,)
        Trace.log(e_errors.INFO, message)

        response_ticket = {'status' : (e_errors.TIMEDOUT, lmc.server_name)}

        control_socket = None
        data_path_socket = None

        #If the library manager moved to a new host or port, we need to
        # update the cached library mananger client.
        get_lmc(lmc.server_name, use_lmc_cache = False)
    else:  #We got nothing.
        response_ticket = {'status' : (e_errors.RESUBMITTING, None)}

        control_socket = None
        data_path_socket = None

    return response_ticket, control_socket, data_path_socket

############################################################################
##
## These functions handle the end of the Mover communication.
##
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

    last_percentage_done = 0
    while True:
        try:
            done_ticket = callback.read_tcp_obj(control_socket)
            (status, percentage) =  done_ticket["status"]
            if status == e_errors.MOVER_BUSY:
                percentage_done = int(percentage)
                if percentage_done == last_percentage_done :
                    done_ticket = {'status' : (e_errors.MOVER_STUCK, "stuck calculating selective CRC")}
                    break
                last_percentage_done = percentage_done
                continue
            else:
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
                break
        except (select.error, socket.error, e_errors.EnstoreError), msg:
            done_ticket = {'status' : (e_errors.NET_ERROR, str(msg))}
            break
        except e_errors.TCP_EXCEPTION:
            done_ticket = {'status' : (e_errors.NET_ERROR, e_errors.TCP_EXCEPTION)}
            break

    message = "[1] Time to receive final dialog: %s sec." % \
              (time.time() - receive_final_dialog_start_time,)
    Trace.message(TIME_LEVEL, message)
    Trace.log(TIME_LEVEL, message)

    return done_ticket

def receive_final_dialog_2(udp_serv, e):

    final_dialog_2_start_time = time.time()

    Trace.message(TRANSFER_LEVEL, "Waiting for final dialog (2).")

    #Keep the udp socket queues clear.
    while time.time() < final_dialog_2_start_time + e.mover_timeout:
        mover_udp_done_ticket = udp_serv.do_request()

        #If requested output the raw
        Trace.trace(11, "UDP MOVER MESSAGE:")
        Trace.trace(11, pprint.pformat(mover_udp_done_ticket))

        #Make sure the messages are what we expect.
        if mover_udp_done_ticket == None: #Something happened, keep trying.
            continue
        #Keep looping until one of these two messages arives.  Ignore
        # any other that my be received.
        elif mover_udp_done_ticket['work'] != 'mover_bound_volume' and \
           mover_udp_done_ticket['work'] != 'mover_error':
            continue
        else:
            break

    Trace.message(TRANSFER_LEVEL, "Received final dialog (2).")
    Trace.message(TICKET_LEVEL, "FINAL DIALOG (udp):")
    Trace.message(TICKET_LEVEL, pprint.pformat(mover_udp_done_ticket))
    Trace.log(e_errors.INFO, "Received final dialog (2).")

    message = "[1] Time to receive final dialog (2): %s sec." % \
                  (time.time() - final_dialog_2_start_time,)
    Trace.message(TIME_LEVEL, message)
    Trace.log(TIME_LEVEL, message)

    return mover_udp_done_ticket

def wait_for_final_dialog(control_socket, udp_serv, e):
    #Get the final success/failure message from the mover.  If this side
    # has an error, don't wait for the mover in case the mover is waiting
    # for "get" or "put" to do something.
    if control_socket != None:
        mover_done_ticket = receive_final_dialog(control_socket)
    else:
        mover_done_ticket = {'status' : (e_errors.OK, None)}

    if udp_serv != None:
        mover_udp_done_ticket = receive_final_dialog_2(udp_serv, e)
    else:
        mover_udp_done_ticket = {'status' : (e_errors.OK, None)}

    if not e_errors.is_ok(mover_udp_done_ticket):
        Trace.log(e_errors.ERROR, "MOVER_UDP_DONE_TICKET")
        Trace.log(e_errors.ERROR, str(mover_udp_done_ticket))

    return mover_done_ticket

############################################################################

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
                try:
                    sys.stderr.write(
                        "Unable to close file object: %s\n" % str(msg))
                except IOError:
                    pass
                Trace.log(e_errors.ERROR,
                          "Unable to close file object: %s\n" % str(msg))
        else:
            try:
                os.close(fd)
	    except TypeError:
		#The passed in object was not a valid socket descriptor.
		pass
            except (OSError, IOError), msg:
                try:
                    sys.stderr.write(
                        "Unable to close fd %s: %s\n" % (fd, str(msg)))
                except IOError:
                    pass
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

############################################################################
##
## The following functions handle communication with the Library Manager.
##
############################################################################

def submit_one_request_send(ticket, encp_intf):
    submit_one_request_send_start_time = time.time()

    #Before resending, there are some fields that the library
    # manager and mover don't expect to receive from encp,
    # these should be removed.
    item_remove_list = ['mover']
    for item in (item_remove_list):
        try:
            del ticket[item]
        except KeyError:
            pass

    #Determine the type of transfer.
    try:
        if is_write(encp_intf):
            transfer_type = "write"
            filename = ticket['outfilepath']
        else:
            transfer_type = "read"
            #Since, this is just for show use the fullpath name, not
            # any .(access)() shortcut names.
            filename = ticket['infilepath']

        if filename == "":
            filename = "unknown filename"
    except EncpError, msg:
        transfer_type = "unknown"
        filename = "unknown filename"
        Trace.log(e_errors.ERROR,
                  "Failed to determine the type of transfer: %s" % str(msg))

    #Get the answer from the library manager director.
    orig_library = ticket['vc']['library']
    csc = get_csc()
    lm_config = csc.get(orig_library+ ".library_manager", 3, 3)
    if e_errors.is_ok(lm_config) and \
           encp_intf.enable_redirection == 1 and \
           ticket['work'] == "write_to_hsm":
        lmd_name = lm_config.get('use_LMD', None)
        if lmd_name:
            lmd = library_manager_director_client.LibraryManagerDirectorClient(csc, lmd_name)
            Trace.message(TICKET_1_LEVEL, "LMD SUBMISSION TICKET\n%s:"%(pprint.pformat(ticket),))

            t = lmd.get_library_manager(copy.deepcopy(ticket))

            ticket.update(dict(filter(lambda i : i[0] not in ("work"),t.iteritems())))

            Trace.message(TICKET_1_LEVEL, "LMD REPLY TICKET\n%s:"%(pprint.pformat(ticket),))

            if not e_errors.is_ok(ticket):
                ticket['status'] = (e_errors.USERERROR,
                                    "Unable to access library manager director: %s" % \
                                    (ticket['status'],))
                return ticket, None, None

    if orig_library !=  ticket['vc']['library']:
        encp_intf.redirected = True

    #Send work ticket to LM.  As long as a single encp process is restricted
    # to working with one enstore system, not passing get_csc() the ticket
    # as parameter will not cause a problem.
    #csc = get_csc()   #ticket)
    #Get the name.
    library = ticket['vc']['library'] + ".library_manager"

    #Get the library manager info information.  This also forces an update
    # if the cached configuration information is old.
    try:
        lmc = get_lmc(library)
    except SystemExit:
        #On error the library manager client calls sys.exit().  This
        # should catch that so we can handle it.
        ticket['status'] = (e_errors.USERERROR,
              "Unable to locate %s." % (library,))
        return ticket, None, None
    except (socket.error, select.error), msg:
        #On 11-12-2009, tracebacks were found from migration encps that
        # started failing because there were too many open files.  That
        # socket.error should have been caught here.  So now it is.
        ticket['status'] = (e_errors.NET_ERROR, str(msg))
        return ticket, None, None
    #If the lmc is not in a valid state, return an error.
    if lmc.server_address == None:
        ticket['status'] = (e_errors.USERERROR,
              "Unable to locate %s." % (library,))
        return ticket, None, None

    #Put in the log file a message connecting filenames to unique_ids.
    message = "Sending %s %s request to LM: unique_id: %s inputfile: %s " \
          "outputfile: %s" \
          % (filename, transfer_type, ticket['unique_id'],
             ticket['infile'], ticket['outfile'])
    Trace.log(e_errors.INFO, message)
    Trace.log(TICKET_1_LEVEL, "Sending ticket: %s" % (str(ticket),))
    #Tell the user what the current state of the transfer is.
    message = "Submitting %s %s request to LM.%s" % \
                  (filename, transfer_type, elapsed_string())
    Trace.message(TRANSFER_LEVEL, message)

    Trace.message(TICKET_1_LEVEL, "LM SUBMISSION TICKET:")
    Trace.message(TICKET_1_LEVEL, pprint.pformat(ticket))

    #Send the request to the library manager.
    #
    #We used to use lmc.read_from_hsm() and lmc.write_to_hsm(), but they did
    # not allow for the control over sending and receiving asyncronously udp
    # messages.
    #
    #Using lmc.send() will work as long as 'work' (and 'method' for get/put)
    # are already in the ticket.  This shouldn't be a problem since encp
    # has always set those ticket values.  This is noted, since other
    # client functions do set their own 'work' values in the ticket.
    #response_ticket = lmc.send(ticket)
    try:
        transaction_id = lmc.u.send_deferred(ticket, lmc.server_address)
    except (KeyboardInterrupt, SystemExit):
        #This is used to keep multiple encps in a migration in sync.
        # Otherwise, it should have no effect.
        try:
            #Trace.log(98, "<<< start_lock.release(), TP #2, except")
            start_lock.release()
        except (KeyboardInterrupt, SystemExit):
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except:
            pass
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
    except:  #Un-anticipated errrors.
        #This is used to keep multiple encps in a migration in sync.
        # Otherwise, it should have no effect.
        try:
            #Trace.log(98, "<<< start_lock.release(), TP #3, except")
            start_lock.release()
        except (KeyboardInterrupt, SystemExit):
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except:
            pass
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]

    #This is used to keep multiple encps in a migration in sync.
    # Otherwise, it should have no effect.
    try:
        #Trace.log(98, "<<< start_lock.release(), TP #4, after send_deferred() - going to release")
        start_lock.release()
        #Trace.log(98, "<<< start_lock.release(), TP #4, after send_deferred(), released")
    except (KeyboardInterrupt, SystemExit):
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
    except:
        #Already unlocked.  Older pythons raised AssertionError, RuntimeError
        # is what is documented with python 2.6, but ValueError appears to be
        # what is actually raised.
        pass

    message = "[1] Time to submit one request: %s sec." % \
              (time.time() - submit_one_request_send_start_time,)
    Trace.message(TIME_LEVEL, message)
    Trace.log(TIME_LEVEL, message)

    ticket['status'] = (e_errors.OK, None)

    return ticket, transaction_id, lmc

#Wait for any response in the transaction_ids list to arive.
def submit_all_request_recv(transaction_ids, work_list, lmc, encp_intf):
    __pychecker__ = "unusednames=work_list"

    submit_all_request_recv_start_time = time.time()

    #Try not to loop forever.
    response_ticket = None
    #count = 0
    while not response_ticket:
        try:
            Trace.log(99, "in submit_all_request_recv() before recv_deferred2()")
            response_ticket, transaction_id = lmc.u.recv_deferred2(
                transaction_ids, encp_intf.resubmit_timeout)
            Trace.log(99, "in submit_all_request_recv() after recv_deferred2()")
        except (socket.error, select.error, e_errors.EnstoreError), msg:
            transaction_id = None
            if msg.errno in [errno.ETIMEDOUT]:
                response_ticket = {'status' : (e_errors.TIMEDOUT,
                                               lmc.server_name)}
            else:
                response_ticket = {'status' : (e_errors.NET_ERROR, str(msg))}
            """
            if msg.errno in [errno.ETIMEDOUT]:
                transaction_id = lmc.u.send_deferred(ticket, lmc.server_address)
                count = count + 1
                if count <= 360:
                    continue
            """
            if not response_ticket:
                Trace.log(e_errors.ERROR, "about to hang in submit_all_request_recv: %s" % (response_ticket,))
            #raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]

    message = "[1] Time to receive one request: %s sec." % \
              (time.time() - submit_all_request_recv_start_time,)
    Trace.message(TIME_LEVEL, message)
    Trace.log(TIME_LEVEL, message)

    return __submit_request_recv(response_ticket), transaction_id

#Wait for the one response for transaction_id.  Resend the original request
# every 10 seconds until the response arives.
def submit_one_request_recv(transaction_id, work_ticket, lmc, encp_intf):

    submit_one_request_recv_start_time = time.time()

    #Try not to loop forever.
    response_ticket = None
    RESEND_TIMEOUT = 10 #seconds
    count = 0
    max_count = max(encp_intf.resubmit_timeout / RESEND_TIMEOUT, 2)
    while not response_ticket:
        try:
            response_ticket, transaction_id = lmc.u.recv_deferred2(
                transaction_id, RESEND_TIMEOUT)
        except (socket.error, select.error, e_errors.EnstoreError), msg:
            if msg.errno in [errno.ETIMEDOUT]:
                transaction_id = lmc.u.send_deferred(work_ticket, lmc.server_address)
                count = count + 1
                if count <= max_count:
                    continue

            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except errno.errorcode[errno.ETIMEDOUT]:
            #Handle this string exception until udp_client is fixed.
            transaction_id = lmc.u.send_deferred(work_ticket, lmc.server_address)
            count = count + 1
            if count <= 360:
                continue

            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]

    message = "[1] Time to receive first request: %s sec." % \
              (time.time() - submit_one_request_recv_start_time,)
    Trace.message(TIME_LEVEL, message)
    Trace.log(TIME_LEVEL, message)

    return __submit_request_recv(response_ticket), transaction_id

def __submit_request_recv(response_ticket, ticket = {}):

    volume = ticket.get('vc', {}).get('external_label', None)
    infilepath = ticket.get('infilepath', None)
    if not volume:
        volume = response_ticket.get('vc', {}).get('external_label', "Unknown")
    if not infilepath:
        infilepath = response_ticket.get('infilepath', "Unknown")

    Trace.message(TICKET_1_LEVEL, "LM RESPONCE TICKET:")
    Trace.message(TICKET_1_LEVEL, pprint.pformat(response_ticket))

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
                                             (volume,
                                              response_ticket['status'][0]))

        if e_errors.is_timedout(response_ticket['status']):
            #This is not an error condition.  However, the request is
            # sent again.
            pass
        elif e_errors.is_non_retriable(response_ticket['status']):
            Trace.log(e_errors.ERROR,
                      "Ticket receive failed for %s: %s" %
                      (infilepath, response_ticket['status']))
            Trace.message(ERROR_LEVEL, "Submission to LM failed: " \
                          + str(response_ticket['status']), out_fp=sys.stderr)
        else:
            Trace.log(e_errors.ERROR,
                      "Ticket receive failed for %s - retrying: %s" %
                      (infilepath, response_ticket['status']))
            Trace.message(ERROR_LEVEL, "Submission to LM failed - retrying:" \
                          + str(response_ticket['status']), out_fp=sys.stderr)

        #If the ticket was malformed, then we want to see what was sent
        # to the LM.
        if response_ticket['status'][0] == e_errors.MALFORMED:
            Trace.log(e_errors.ERROR,
                      "Ticket receive failed: %s: %s" % (e_errors.MALFORMED,
                                                         str(response_ticket)))

    #It will most likely be a while, so this would be a good time to
    # perform this maintenance.
    collect_garbage()

    return response_ticket

#This function is used to send all the initial requests to the LM.
def submit_one_request(ticket, encp_intf):
    #transaction_id, lmc = submit_one_request_send(ticket, encp_intf)
    #return submit_one_request_recv(transaction_id, ticket, lmc, encp_intf), lmc
    rticket, transaction_id, lmc = submit_one_request_send(ticket, encp_intf)
    if not e_errors.is_ok(rticket):
        return combine_dict(rticket, ticket), lmc
    response_ticket, transaction_id = submit_one_request_recv(
        transaction_id, ticket, lmc, encp_intf)
    return response_ticket, lmc

#This is modifies the ticket between retries and resubmits.
def adjust_resubmit_request(ticket, encp_intf):
    ##start of resubmit block

    resubmission_update_start_time = time.time()

    #These two lines of code are for get retries to work properly.
    if is_read(ticket) and ticket.get('method', None) != None:
        ticket['method'] = "read_tape_start"
    #These two lines of code are for put retries to work properly.
    if is_write(ticket) and ticket.get('method', None) != None:
        ticket['method'] = "write_tape_start"


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
                dname = get_directory_name(ticket['outfile']) #only for writes.
                if encp_intf.outtype == RHSMFILE:
                    t = get_pac()
                    file_family_width = t.get_file_family_width(
                        dname, rcv_timeout=5, tries=6)
                else:
                    t = namespace.Tag(dname)
                    file_family_width = t.get_file_family_width(dname)
                ticket['vc']['file_family_width'] = file_family_width
            except (OSError, IOError), msg:
                if msg.args[0] in [errno.ENOENT]:
                    ticket['status'] = (e_errors.USERERROR, str(msg))
                else:
                    ticket['status'] = (e_errors.PNFS_ERROR, str(msg))
                return ticket

    #Blast this before resubmitting.
    try:
        del ticket['transaction_id_list']
    except KeyError:
        pass

    message = "[1] Time to update request for resubmission: %s sec." % \
              (time.time() - resubmission_update_start_time,)
    Trace.message(TIME_LEVEL, message)
    Trace.log(TIME_LEVEL, message)

    #Now do what we would do for any initial submition.
    return ticket

def resubmit_one_request(ticket, encp_intf):
    ticket = adjust_resubmit_request(ticket, encp_intf)
    return submit_one_request(ticket, encp_intf)

def resubmit_one_request_send(ticket, encp_intf):
    ticket = adjust_resubmit_request(ticket, encp_intf)
    rticket, transaction_id, lmc = submit_one_request_send(ticket, encp_intf)
    return transaction_id, lmc


############################################################################
############################################################################

#Open the local disk file for reading or writing (as appropriate).
def open_local_file(work_ticket, tinfo, e):

    open_local_file_start_time = time.time()

    Trace.message(5, "Opening local file.")
    Trace.log(e_errors.INFO, "Opening local file.")

    if is_write(e):
        flags = os.O_RDONLY
    else: #reads
        #Setting the local fd to read/write access on reads from enstore
        # (writes to local file) should be okay.  The file is initially
        # created with 0644 permissions and is not set to original file
        # permissions until after everything else is set.  The read
        # permissions might be needed later (i.e. --ecrc).
        flags = os.O_RDWR

    #We don't need to worry about using the .(access)() vs true filename.
    #  This will always be an non-pnfs file.
    filename = work_ticket['wrapper']['fullname']

    #Try to open the local file for read/write.
    try:
        local_fd = file_utils.open_fd(filename, flags)
    except OSError, detail:
        if getattr(detail, "errno", None) in \
               [errno.EACCES, errno.EFBIG, errno.ENOENT, errno.EPERM]:
            done_ticket = {'status':(e_errors.FILE_MODIFIED, str(detail))}
        else:
            done_ticket = {'status':(e_errors.OSERROR, str(detail))}
        return done_ticket

    #Try to grab the os stats of the file.
    try:
        stats = file_utils.get_stat(local_fd)
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

    Trace.message(TRANSFER_LEVEL, "Local file %s opened.  elapsed=%s" %
                  (filename,
                   time.time() - tinfo['encp_start_time']))

    #Record this.
    message = "[1] Time to open local file: %s sec." % \
              (time.time() - open_local_file_start_time,)
    Trace.message(TIME_LEVEL, message)
    Trace.log(TIME_LEVEL, message)

    return done_ticket

############################################################################
############################################################################

#Returns two-tuple.  First is dictionary with 'status' element.  The next
# is an integer of the crc value.  On error returns 0.
#def transfer_file(input_fd, output_fd, contropl_socket, request, tinfo, e):
def transfer_file(input_file_obj, output_file_obj, control_socket,
                  request, tinfo, e, udp_serv = None):

    transfer_start_time = time.time() # Start time of file transfer.

    Trace.message(TRANSFER_LEVEL, "Starting %s transfer.  elapsed=%s" %
                  (request['infilepath'],
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
		    sys.stderr.write("SOCKET ERROR: %s\n" % (str(msg,))
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

        #Pulling out args like this keeps pychecker happy.
        msg_args = getattr(msg, "args", (e_errors.IOERROR, errno.EIO,
                                         os.strerror(errno.EIO), os.getpid))

        if msg_args[1] == errno.ENOSPC \
               or msg_args[1] == errno.EDQUOT: #These should be non-retriable.
            error_type = e_errors.NOSPACE
        elif msg_args[1] == errno.EBUSY: #This should be non-retriable.
            error_type = e_errors.DEVICE_ERROR
        else:
            error_type = e_errors.IOERROR

        EXfer_ticket = {'status': (error_type,
                         "[ Error %d ] %s: %s" % (msg_args[1], msg_args[2],
                                                  msg_args[0]))}
        #If this is the longer form, add these values to the ticket.
        if len(msg_args) >= 7:
            EXfer_ticket['bytes_transfered'] = request['file_size'] - \
                                               msg_args[4]
            EXfer_ticket['bytes_not_transfered'] = msg_args[4]
            EXfer_ticket['read_time'] = msg_args[5]
            EXfer_ticket['write_time'] = msg_args[6]
            EXfer_ticket['filename'] = msg_args[7]
            EXfer_ticket['line_number'] = msg_args[8]

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
                      % (msg_args[1], msg_args[2], msg_args[0],
                         time.time() - tinfo['encp_start_time'],))

    transfer_stop_time = time.time()

    if e_errors.is_ok(EXfer_ticket):
        # Print a sucess message.
        Trace.message(TRANSFER_LEVEL, "File %s transfered.  elapsed=%s" %
                      (request['infilepath'],
                       time.time()-tinfo['encp_start_time']))

        Trace.log(e_errors.INFO, "File %s transfered for %s in %s seconds." %
                  (request['infilepath'], request['unique_id'],
                   time.time() - transfer_start_time))

        # Print an additional timming value.
        message = "[1] Time to transfer file: %s sec." % \
                  (transfer_stop_time - transfer_start_time,)
        Trace.message(TIME_LEVEL, message)
        Trace.log(TIME_LEVEL, message)

    else:
        Trace.log(e_errors.WARNING,
                  "File %s transfer attempt done for %s after %s seconds." %
                  (request['infilepath'], request['unique_id'],
                   time.time() - transfer_start_time))

    #Even though the functionality is there for this to be done in
    # handle requests, this should be received outside since there must
    # be one... not only receiving one on error.
    #done_ticket = receive_final_dialog(control_socket)
    done_ticket = wait_for_final_dialog(control_socket, udp_serv, e)

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

    Trace.log(e_errors.INFO, "leaving transfer_file().")
    return rtn_ticket

############################################################################
##
## The following functions do CRC checks.
##
############################################################################


def check_crc(done_ticket, encp_intf, fd=None):

    check_crc_start_time = time.time()

    #Make these more accessable.
    #mover_crc = done_ticket['fc'].get('complete_crc', None)
    encp_crc = done_ticket['exfer'].get('encp_crc', None)

    if encp_intf.chk_crc:
        check_for_crcs(done_ticket)
        if not e_errors.is_ok(done_ticket):
            #We don't want to fail for this reason, just to skip testing more.
            done_ticket['status'] = (e_errors.OK, None)
            return

    #Enstore at FNAL historically uses a zero seeded adler32.  The adler32
    # standard says that it should be seed with 1 not 0.  Convert the zero
    # seeded value to a one seeded value for comparison, if necessary.
    #
    #The variable encp_crc_1_seeded is what is used to check the CRC in
    # layer 2, this always needs to be checked as a 1 seeded adler32.
    encp_crc_1_seeded = checksum.convert_0_adler32_to_1_adler32(
            encp_crc, done_ticket['file_size'])

    # Check the CRC
    if encp_intf.chk_crc:
        compare_crc(done_ticket, encp_crc_1_seeded)
        if not e_errors.is_ok(done_ticket):
            return

    #If the user wants a crc readback check of the new output file (reads
    # only) calculate it and compare.
    if encp_intf.ecrc:
        check_crc_readback(done_ticket, fd)
        if not e_errors.is_ok(done_ticket):
            return

    # Check the CRC in pnfs layer 2 (set by dcache).
    if encp_intf.chk_crc and (encp_intf.get_cache or encp_intf.put_cache):
        check_crc_layer2(done_ticket, encp_crc_1_seeded)
        if not e_errors.is_ok(done_ticket):
            return
    message = "[1] Time to check CRC: %s sec." % \
              (time.time() - check_crc_start_time,)
    Trace.message(TIME_LEVEL, message)
    Trace.log(TIME_LEVEL, message)

    return

def check_for_crcs(done_ticket):
    #Make these more accessable.
    mover_crc = done_ticket['fc'].get('complete_crc', None)
    encp_crc = done_ticket['exfer'].get('encp_crc', None)

    #Trace.log(e_errors.INFO,
    #          "Mover CRC: %s  ENCP CRC: %s" % (mover_crc, encp_crc))
    #print "Mover CRC: %s  ENCP CRC: %s" % (mover_crc, encp_crc)

    #Check this just to be safe.
    if mover_crc == None:
        msg =   "warning: mover did not return CRC; skipping CRC check\n"
        try:
            sys.stderr.write(msg)
            sys.stderr.flush()
        except IOError:
            pass
        done_ticket['status'] = (e_errors.NO_CRC_RETURNED, msg)
        return
    #if encp_intf.chk_crc and encp_crc == None:
    if encp_crc == None:
        msg =   "warning: encp failed to calculate CRC; skipping CRC check\n"
        try:
            sys.stderr.write(msg)
            sys.stderr.flush()
        except IOError:
            pass
        done_ticket['status'] = (e_errors.NO_CRC_RETURNED, msg)
        return

    done_ticket['status'] = (e_errors.OK, None)
    return

def compare_crc(done_ticket, encp_crc_1_seeded):
    #Make these more accessable.
    mover_crc = done_ticket['fc'].get('complete_crc', None)
    encp_crc = done_ticket['exfer'].get('encp_crc', None)

    compare_crc_start_time = time.time()
    if mover_crc != encp_crc and mover_crc != encp_crc_1_seeded:
	    msg = "CRC mismatch: %d mover != %d (or %s) encp" % \
		  (mover_crc, encp_crc_1_seeded, encp_crc)
	    done_ticket['status'] = (e_errors.CRC_ENCP_ERROR, msg)
	    return
    message = "[1] Time to compare CRC: %s sec." % \
	      (time.time() - compare_crc_start_time,)
    Trace.message(TIME_LEVEL, message)
    Trace.log(TIME_LEVEL, message)

    done_ticket['status'] = (e_errors.OK, None)
    return

def check_crc_readback(done_ticket, fd):
    #Make these more accessable.
    mover_crc = done_ticket['fc'].get('complete_crc', None)

    readback_crc_start_time = time.time()
    #If passed a file descriptor, make sure it is to a regular file.
    if fd and (type(fd) == types.IntType) and \
        stat.S_ISREG(os.fstat(fd)[stat.ST_MODE]):
        try:
	    readback_crc = EXfer.ecrc(fd)
        except EXfer.error, msg:
	    done_ticket['status'] = (e_errors.CRC_ECRC_ERROR, str(msg))
	    return

	#Convert this to a one seeded alder32 CRC.
	readback_crc_1_seeded = checksum.convert_0_adler32_to_1_adler32(
		readback_crc, done_ticket['file_size'])
	#Put the ecrc value into the ticket.
	done_ticket['ecrc'] = readback_crc

	#If we have a valid crc value returned, compare it.
	if readback_crc != mover_crc and \
	       readback_crc_1_seeded != mover_crc:
	    msg = "CRC readback mismatch: %d mover != %d (or %s) encp" % \
		  (mover_crc, readback_crc_1_seeded, readback_crc)
	    done_ticket['status'] = (e_errors.CRC_ECRC_ERROR, msg)
	    return
    message = "[1] Time to check readback CRC: %s sec." % \
	      (time.time() - readback_crc_start_time,)
    Trace.message(TIME_LEVEL, message)
    Trace.log(TIME_LEVEL, message)

    done_ticket['status'] = (e_errors.OK, None)
    return

def check_crc_layer2(done_ticket, encp_crc_1_seeded):
    layer2_crc_start_time = time.time()

    if is_read(done_ticket):
        pnfs_filename = done_ticket['infile']
    else: #write
        pnfs_filename = done_ticket['outfile']

    try:
        # Get the pnfs layer 2 for this file.
	sfs = namespace.StorageFS(pnfs_filename)
	data = sfs.readlayer(2, pnfs_filename)
    except (IOError, OSError, TypeError, AttributeError):
        #There is no layer 2 to check.  Skip the rest of the check.
	#If there are ever any later checks added, this return is bad.
	#return
	data = []

    encp_crc_long = long(encp_crc_1_seeded)
    encp_size_long = long(done_ticket['file_size'])
    dcache_crc_long, dcache_size_long = parse_layer_2(data)

    if dcache_crc_long != None and dcache_crc_long != encp_crc_1_seeded:
        msg = "CRC dcache mismatch: %s (%s) != %s (%s)" % \
	      (dcache_crc_long, hex(dcache_crc_long),
	       encp_crc_long, hex(encp_crc_long))
	done_ticket['status'] = (e_errors.CRC_DCACHE_ERROR, msg)
	return

    if dcache_size_long != None and dcache_size_long != encp_size_long:
        msg = "Size dcache mismatch: %s (%s) != %s (%s)" % \
	      (dcache_size_long, hex(dcache_size_long),
	       encp_size_long, hex(encp_size_long))
	done_ticket['status'] = (e_errors.CRC_DCACHE_ERROR, msg)
	return

    message = "[1] Time to check dCache CRC: %s sec." % \
	      (time.time() - layer2_crc_start_time,)
    Trace.message(TIME_LEVEL, message)
    Trace.log(TIME_LEVEL, message)

    done_ticket['status'] = (e_errors.OK, None)
    return

############################################################################
##
## The following functions to post transfer integrity checks.
##
############################################################################

#Double check that all of the filesizes agree.  As a performance bonus,
# double check the inodes since the performace pentalty of the stat()s has
# already occured.
def verify_file_size(ticket, encp_intf = None):

    verify_file_size_start_time = time.time()

    #Before continueing, double check the number bytes said to be
    # moved by the mover and encp.
    encp_size = ticket.get('exfer', {}).get('bytes_transfered', None)
    mover_size = ticket.get('fc', {}).get('size', None)
    if encp_size == None:
        ticket['status'] = (e_errors.UNKNOWN,
                                 "Client does not know number of bytes"
                                 " transfered.")
        return ticket
    elif mover_size == None:
        ticket['status'] = (e_errors.UNKNOWN,
                                 "Mover did not report how many bytes"
                                 " were transfered.")
        return ticket
    elif long(encp_size) != long(mover_size):
        #We get here if the two sizes do not match.  This is a very bad
        # thing to occur.
        msg = (e_errors.CONFLICT,
               "Get bytes read (%s) do not match the mover "
               "bytes written (%s)." % (encp_size, mover_size))
        ticket['status'] = msg
        return ticket
    else:
        if ticket['file_size'] == None:
            #If the number of bytes transfered is consistant with Get and
            # the mover, then set this value.  This is only necessary
            # when no file information is available.
            ticket['file_size'] = long(encp_size)



    #Don't worry about checking when outfile is /dev/null.
    if ticket['outfile'] in ['/dev/null', "/dev/zero",
                             "/dev/random", "/dev/urandom"]:
        return

    #Get the stat info for each file.
    try:
        full_stat = file_utils.get_stat(ticket['wrapper'].get('fullname',None))
        full_inode = full_stat[stat.ST_INO]
        if ticket['infile'] in [ "/dev/zero", "/dev/random", "/dev/urandom"]:
            full_filesize = ticket['file_size']
        else:
            full_filesize = full_stat[stat.ST_SIZE]
    except (OSError, IOError), detail:
        ticket['status'] = (e_errors.OSERROR, str(detail))
        return

    #If we used ticket['wrapper']['pnfsFilename'] here we would run into
    # the problem where the file could be moved in the namespace before
    # the file is on tape.  For PNFS and Chimera, use the .(access)() paths.
    try:
        if is_read(ticket):
            pnfs_filename = ticket['infile']
        else: #write
            pnfs_filename = ticket['outfile']
    except (TypeError), detail:
        ticket['status'] = (e_errors.OK, "No files sizes to verify.")
        return

    try:
        if ticket.get('fc', {}).get('deleted', None) == "yes" \
               and encp_intf and encp_intf.override_deleted:
            #If the file is deleted, just assign the input size to what
            # the database has on file.
            pnfs_filesize = ticket['file_size']
            pnfs_real_size = pnfs_filesize
        elif encp_intf and (encp_intf.get_bfid or encp_intf.get_bfids) \
                 and encp_intf.skip_pnfs:
            #Don't access PNFS/Chimera if told to ignore it.
            pnfs_filesize = ticket['file_size']
            pnfs_real_size = pnfs_filesize
        else:
            # this needs a better solution
            # if --get-bfid and --skip-pnfs we DO not USE any sfs information!!
            if encp_intf and encp_intf.get_bfid and encp_intf.skip_pnfs:
                pnfs_real_size = ticket['file_size']
            else:
                #We need to obtain the size in PNFS.
                sfs = namespace.StorageFS(pnfs_filename)
                pnfs_stat = sfs.get_stat(pnfs_filename)
                pnfs_filesize = pnfs_stat[stat.ST_SIZE]
                pnfs_inode = pnfs_stat[stat.ST_INO]

                if pnfs_filesize == 1 and ticket['file_size'] > 1:
                    #For files larger than 2GB, we need to look at the size in
                    # layer 4.  PNFS can't handle filesizes larger than that, so
                    # as a workaround encp sets large files to have a PNFS size
                    # of 1 and puts the real size in layer 4.
                    pnfs_real_size = sfs.get_file_size(pnfs_filename)
                else:
                    pnfs_real_size = pnfs_filesize
    except (TypeError), detail:
        ticket['status'] = (e_errors.OK, "No files sizes to verify.")
        return
    except (OSError, IOError), detail:
        if getattr(detail, 'errno', detail.args[0]) == errno.ENOENT \
               and encp_intf and encp_intf.override_deleted:
            #When reading we need to be able to bypass obtaining the value
            # from the pnfs file that has been deleted.
            pnfs_filesize = ticket['fc']['size']
            pnfs_inode = None
        else:
            ticket['status'] = (e_errors.OSERROR, str(detail))
            return
    except: #Un-anticipated errors.
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]

    try:
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
    except: #Un-anticipated errors.
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]

    try:
        #Test if the sizes are correct.
        if ticket['file_size'] != full_filesize:
            msg = "Expected file size (%s) to equal actuall file size " \
                  "(%s) for file %s." % \
                  (ticket['file_size'], full_filesize, ticket['outfile'])
            ticket['status'] = (e_errors.FILE_MODIFIED, msg)
        elif full_filesize != pnfs_real_size:
            msg = "Expected local file size (%s) to equal remote file " \
                  "size (%s) for file %s." \
                  % (full_filesize, pnfs_filesize, ticket['outfile'])
            ticket['status'] = (e_errors.FILE_MODIFIED, msg)
    except: #Un-anticipated errors.
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]

    message = "[1] Time to verify file size: %s sec." % \
              (time.time() - verify_file_size_start_time,)
    Trace.message(TIME_LEVEL, message)
    Trace.log(TIME_LEVEL, message)

def verify_inode(ticket):

    verify_inode_start_time = time.time()

    #Don't worry about checking when outfile is /dev/null.
    if ticket['outfile'] == '/dev/null':
        return

    #Get the stat info for each file.
    try:
        full_stat = file_utils.get_stat(ticket['wrapper'].get('fullname',None))
        full_inode = full_stat[stat.ST_INO]
    except (OSError, IOError), detail:
        ticket['status'] = (e_errors.OSERROR, str(detail))
        return

    try:
        if is_read(ticket):
            pnfs_filename = ticket['infile']
        else: #write
            pnfs_filename = ticket['outfile']

        sfs = namespace.StorageFS(pnfs_filename)
        pnfs_stat = sfs.get_stat(pnfs_filename)
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

    message = "[1] Time to verify inode: %s sec." % \
              (time.time() - verify_inode_start_time,)
    Trace.message(TIME_LEVEL, message)
    Trace.log(TIME_LEVEL, message)

############################################################################
##
## These functions modify the metadata after a transfer is complete for
## for both read and writes.  (Others maybe involved for just reads or writes.)
##
############################################################################

def set_outfile_permissions(ticket, encp_intf):

    if not ticket.get('copy', None):  #Don't set permissions if copy.
        set_outfile_permissions_start_time = time.time()

        #Attempt to get the input files permissions and set the output file to
        # match them.
        if ticket['outfile'] not in ["/dev/null", "/dev/zero",
                                     "/dev/random", "/dev/urandom"]:
            try:
                if is_write(ticket):
                    in_stat_info = file_utils.get_stat(ticket['infile'])
                else:
                    if (encp_intf.get_bfid or encp_intf.get_bfids) and \
                       encp_intf.skip_pnfs:
                        #Note, most of these values are made up, but using
                        # them for consistancy is good for error checking.
                        dev = ticket['wrapper']['major'] << 8 + \
                              ticket['wrapper']['minor']
                        update_t = ticket['fc']['update'].split(".")[0] # this is needed in case if time is appended by ms
                        fake_time = time.mktime(time.strptime(
                            update_t, "%Y-%m-%d %H:%M:%S"))
                        in_stat_info = (ticket['wrapper']['mode'],
                                        ticket['wrapper']['inode'],
                                        dev,  #Reconstructed.
                                        1,  #number of links
                                        ticket['wrapper']['uid'],
                                        ticket['wrapper']['gid'],
                                        ticket.get('filesize',ticket.get('file_size',0L)), # when merging head as SFA notices incosistency between filesize and file_size, this should take care of it
                                        fake_time,  #atime
                                        fake_time,  #mtime
                                        fake_time,  #ctime
                                        )
                    else:
                        try:
                            in_sfs = namespace.StorageFS(ticket['infile'])
                        except (OSError, IOError), msg2:
                            Trace.handle_error(severity=99)
                            message = "StorageFS failed: %s" % (str(msg2),)
                            Trace.log(e_errors.INFO, message)
                            ticket['status'] = (e_errors.OSERROR, message)
                            return
                        in_stat_info = in_sfs.get_stat(ticket['infile'])
            except (OSError, IOError), msg:
                Trace.handle_error(severity=99)
                Trace.log(e_errors.INFO, "stat failed: %s: %s" % \
                          (str(msg), ticket['infile']))
                message = "Unable to stat() file: %s: %s" % \
                          (str(msg), ticket['infilepath'])
                ticket['status'] = (e_errors.USERERROR, message)
                return

            try:
                perms = in_stat_info[stat.ST_MODE]
                #handle remote file case
                if is_write(ticket):
                    if not encp_intf.put_cache:
                        sfs = namespace.StorageFS(ticket['outfile'])
                        sfs.chmod(perms, ticket['outfile'])
                else:
                    file_utils.chmod(ticket['outfile'], perms)

                ticket['status'] = (e_errors.OK, None)
            except (OSError, IOError), msg:
                Trace.log(e_errors.INFO, "chmod %s failed: %s" % \
                          (ticket['outfile'], msg))
                ticket['status'] = (e_errors.USERERROR,
                               "Unable to set permissions: %s" % (str(msg),))

            #For root only, if an error hasn't already occured.
            if (os.getuid() == 0 or os.getgid() == 0) and \
                   e_errors.is_ok(ticket) and not encp_intf.put_cache:
                try:
                    uid = in_stat_info[stat.ST_UID]
                    gid = in_stat_info[stat.ST_GID]

                    #handle remote file case
                    if is_write(ticket):
#                        sfs = namespace.StorageFS(ticket['outfile'])
                        sfs.chown(uid, gid, ticket['outfile'])
                    else:
                        file_utils.chown(ticket['outfile'], uid, gid)
                    ticket['status'] = (e_errors.OK, None)
                except (OSError, IOError), msg:
                    message = "chown(%s, %s, %s) failed: (uid %s, gid %s, " \
                              "euid %s, egid %s): %s" % \
                              (ticket['outfile'], uid, gid, os.getuid(),
                               os.getgid(), os.geteuid(), os.getegid(),
                               str(msg))
                    Trace.log(e_errors.INFO, message)
                    ticket['status'] = (e_errors.USERERROR, message)

        message = "[1] Time to set_outfile_permissions: %s sec." % \
                      (time.time() - set_outfile_permissions_start_time,)
        Trace.message(TIME_LEVEL, message)
        Trace.log(TIME_LEVEL, message)

############################################################################

def finish_request(done_ticket, request_list, index, encp_intf):
    #Everything is fine.
    if e_errors.is_ok(done_ticket):
        if index == None:
            #How can we succed at a transfer, that is not in the
            # request list?
            message = "Successfully transfered a file that " \
                      "is not in the file transfer list."
            try:
                sys.stderr.write(message + "\n")
                sys.stderr.flush()
            except IOError:
                pass
            Trace.log(e_errors.ERROR,
                      message + "  " + str(done_ticket))

        else:
            #Tell the user what happend.
            message = "File %s copied successfully." % \
                      (done_ticket['outfilepath'],)
            Trace.message(e_errors.INFO, message)
            Trace.log(e_errors.INFO, message)

            #Set completion status to successful.
            done_ticket['completion_status'] = SUCCESS
            done_ticket['exit_status'] = 0

            #Store these changes back into the master list.
            request_list[index] = done_ticket

        return CONTINUE

    #Give up.
    elif e_errors.is_non_retriable(done_ticket):
        if index == None:
            message = "Unknown transfer failed."
            try:
                sys.stderr.write(message + "\n")
                sys.stderr.flush()
            except IOError:
                pass
            Trace.log(e_errors.ERROR,
                      message + "  " + str(done_ticket))

        else:
            #Tell the user what happend.
            message = "File %s transfer failed: %s" % \
                      (done_ticket['outfilepath'], done_ticket['status'])
            Trace.message(DONE_LEVEL, message)
            Trace.log(e_errors.ERROR, message)

            #Set completion status to failure.
            done_ticket['completion_status'] = FAILURE
            #done_ticket['exit_status'] = 1
            #Encp returns 1 on error, "get" and "put" will return 1 or 2
            # depending on the situation.
            if hasattr(encp_intf, 'put') or hasattr(encp_intf, 'get'):
                if e_errors.is_resendable(done_ticket):
                    done_ticket['exit_status'] = 1
                else:
                    done_ticket['exit_status'] = 2
            else:
                done_ticket['exit_status'] = 1

        return STOP

    #Keep trying.
    elif e_errors.is_retriable(done_ticket):
        #On retriable error go back and resubmit what is left
        # to the LM.

        #Record the intermidiate error.
        message = "File %s transfer failed: %s" % \
                  (done_ticket['outfile'], done_ticket['status'])
        Trace.log(e_errors.WARNING, message)

        return CONTINUE_FROM_BEGINNING

    ### Should never get here!!!
    return None

############################################################################
##
## The following functions handle error detection, error recover and request
## retries.
##
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
                            udp_serv = None, control_socket = None):
    #Set the encp_intf to internal test values to two.  This means
    # there is only one check made on internal problems.
    remember_retries = encp_intf.max_retry
    remember_resubmits = encp_intf.max_resubmit
    encp_intf.max_retry = 2
    encp_intf.max_resubmit = 2

    internal_result_dict = handle_retries(request_list, request_dictionary,
                                          error_dictionary, encp_intf,
                                          listen_socket = listen_socket,
                                          udp_serv = udp_serv,
                                          control_socket = control_socket)

    #Set the max resend parameters to original values.
    encp_intf.max_retry = remember_retries
    encp_intf.max_resubmit = remember_resubmits

    return internal_result_dict

def handle_retries(request_list, request_dictionary, error_dictionary,
                   encp_intf, listen_socket = None, udp_serv = None,
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

    #If we did not get far enough in the processing for this to get the
    # next request from the library manager, use the first one in the list
    # that hasn't already been done to assign the blame to it.
    if USE_FIRST_REQUEST and not request_dictionary.has_key('infile') and \
           not request_dictionary.has_key('outfile'):
        request_dictionary, unused, unused = get_next_request(request_list)
        message = "using request %s instead for error processing" \
                  % (request_dictionary['unique_id'],)
        Trace.log(e_errors.INFO, message)

    #These fields need to be retrieved with possible defaults.  If the transfer
    # failed before encp could determine which transfer failed (aka failed
    # opening/reading the/from contol socket) then only the 'status' field
    # of both the request_dictionary and error_dictionary are guarenteed to
    # exist (although some situations will add others).
    infile = request_dictionary.get('infile', '')
    outfile = request_dictionary.get('outfile', '')
    infile_print = request_dictionary.get('infilepath', '')
    outfile_print = request_dictionary.get('outfilepath', '')
    file_size = request_dictionary.get('file_size', 0)
    try:
        use_resend = request_dictionary['resend']
    except KeyError:
        #We should only get here on reads.  Writes are submited one at a time
        # so the current file being attempted is always known.
        use_resend = get_next_request(request_list)[0].get('resend', {})
    retry = use_resend.get('retry', 0)
    resubmits = use_resend.get('resubmits', 0)

    #Get volume info from the volume clerk.
    #Need to check if the volume has been marked NOACCESS since it
    # was checked last.  This should only apply to reads.
    #if request_dictionary.get('fc', {}).has_key('external_label'):
    if external_label and external_label == \
           request_dictionary.get('fc', {}).get('external_label', None):
        check_external_label_start_time = time.time()

        try:
            vc_reply = get_volume_clerk_info(request_dictionary)
            vc_status = vc_reply['status']
        except EncpError, msg:
            vc_status = (getattr(msg, 'type', e_errors.UNKNOWN), str(msg))

        message = "[1] Time to check external label: %s sec." % \
              (time.time() - check_external_label_start_time,)
        Trace.message(TIME_LEVEL, message)
        Trace.log(TIME_LEVEL, message)
    else:
        vc_status = (e_errors.OK, None)

    #If there is a control socket open and there is data to read, then read it.
    socket_status = (e_errors.OK, None)
    socket_dict = {'status':socket_status}
    if control_socket:
        check_control_socket_start_time = time.time()

        try:
            socket_error = control_socket.getsockopt(socket.SOL_SOCKET,
                                                     socket.SO_ERROR)
        except socket.error, msg:
            socket_error = msg.args[0]

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
                    if msg.args[0] in [errno.EINTR, errno.EAGAIN]:
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

        message = "[1] Time to check control socket: %s sec." % \
              (time.time() - check_control_socket_start_time,)
        Trace.message(TIME_LEVEL, message)
        Trace.log(TIME_LEVEL, message)

    #Just to be paranoid check the listening socket.  Check the current
    # socket status to avoid wiping out an error.
    if e_errors.is_ok(socket_dict) and listen_socket:
        check_listen_socket_start_time = time.time()

        socket_error = listen_socket.getsockopt(socket.SOL_SOCKET,
                                                socket.SO_ERROR)
        if socket_error:
            socket_status = (e_errors.NET_ERROR, os.strerror(socket_error))
            socket_dict = {'status':socket_status}
            request_dictionary = combine_dict(socket_dict, request_dictionary)

        message = "[1] Time to check listen socket: %s sec." % \
              (time.time() - check_listen_socket_start_time,)
        Trace.message(TIME_LEVEL, message)
        Trace.log(TIME_LEVEL, message)

    lf_status = (e_errors.OK, None)
    if local_filename:
        check_local_filename_start_time = time.time()

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
                #For debugging.
                Trace.log(40, "euid = %s  egid = %s  uid = %s  gid = %s" % \
                          (os.geteuid(), os.getegid(), os.getuid(), os.getgid()))
                #Local File status.
                lf_status = (e_errors.FILE_MODIFIED,
                             "Noticed the local file changed: " + str(msg))
            else:
                #Local File status.
                lf_status = (e_errors.OSERROR,
                             "Noticed error checking local file:" + str(msg))

        message = "[1] Time to check local filename: %s sec." % \
              (time.time() - check_local_filename_start_time,)
        Trace.message(TIME_LEVEL, message)
        Trace.log(TIME_LEVEL, message)

    pf_status = (e_errors.OK, None)
    skip_layer_cleanup = False
    if is_write(encp_intf) and type(pnfs_filename) == types.StringType \
           and pnfs_filename and not request_dictionary.get('copy', None):
        check_pnfs_filename_start_time = time.time()

        #If the user wants us to specifically check if another encp has
        # written (layers 1 or 4) to this pnfs file; now is the time to check.
        try:
            if do_layers_exist(pnfs_filename, encp_intf):
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
        message = "[1] Time to check pnfs filename: %s sec." % \
                  (time.time() - check_pnfs_filename_start_time,)
        Trace.message(TIME_LEVEL, message)
        Trace.log(TIME_LEVEL, message)


    ## First check for non-retriable errors.

    #The volume clerk set the volume NOACCESS.
    if e_errors.is_non_retriable(vc_status):
        status = vc_status
    #Set status if there was an error recieved from control socket.
    elif e_errors.is_non_retriable(socket_status):
        status = socket_status
    #Set the status if the local file could not be stat-ed.
    elif e_errors.is_non_retriable(lf_status):
        status = lf_status
    #Set the status if the pnfs file had layers already set.
    elif e_errors.is_non_retriable(pf_status):
        status = pf_status
    #Set the status if the ticket status contained a non-retriable error.
    elif e_errors.is_non_retriable(dict_status):
        status = dict_status

    ## If there were no non-retriable errors, look for any error.

    #The volume clerk set the volume NOACCESS.
    elif not e_errors.is_ok(vc_status):
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
        result_dict = {'status':(e_errors.OK, None),
                       'resend':{'retry':retry, 'resubmits':resubmits},
                       'queue_size':len(request_list)}
        result_dict = combine_dict(result_dict, socket_dict)
        return result_dict
    #At this point it is known there is an error.  If the transfer is a read,
    # then if the encp is killed before completing delete_at_exit.quit() could
    # leave non-zero non-correct files.  If this is the case truncate them.
    #
    #We leave the file alone if told to skip this step.
    elif is_read(encp_intf) and not error_dictionary.get('skip_retry', None):
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
                          (outfile_print, request_dictionary.get('unique_id', None),
                           str(pf_status)))
                sfs = namespace.StorageFS(outfile)
                sfs.writelayer(1, "", outfile)
                sfs.writelayer(4, "", outfile)
            except (IOError, OSError):
                #Something is very wrong, deal with it later.
                pass
    """
    #If the mover doesn't call back after max_submits number of times, give up.
    # If the error is already non-retriable, skip this step.
    if max_submits != None and resubmits >= max_submits \
       and status[0] == e_errors.RESUBMITTING:
        Trace.message(ERROR_LEVEL,
                      "To many resubmissions for %s -> %s." % (infile_print,
                                                               outfile_print),
                      out_fp=sys.stderr)
        status = (e_errors.TOO_MANY_RESUBMITS, status)

    #If the transfer has failed too many times, remove it from the queue.
    # Since TOO_MANY_RETRIES is non-retriable, set this here.
    # If the error is already non-retriable, skip this step.
    if max_retries != None and retry >= max_retries \
       and e_errors.is_retriable(status) \
       and status[0] != e_errors.RESUBMITTING:
        Trace.message(ERROR_LEVEL,
                      "To many retries for %s -> %s." % (infile_print,
                                                         outfile_print),
                      out_fp=sys.stderr)
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
                      "Treating error like resubmit for %s -> %s." % \
                      (infile_print, outfile_print), out_fp=sys.stderr)
        status = (e_errors.RESUBMITTING, status)

    #For reads only when a media error occurs.
    retry_non_retriable_media_error = False
    #Make sure that it is a media error first.  If the error is a media
    # error, then the request_dictionary will have a full request
    # dictionary to pass to is_read().  If request_dictionary is not a
    # full request dictionary is_read() will through an EncpError that
    # becomes an 'UNCAUGHT EXCEPTION'.  One know case of a non-full
    # request dictionary is for a (RESUBMITTING, None) 'error'.
    #
    #This only makes sense if the user specified the filename.  If they
    # specified the bfid or volume specifically, then we should not do this.
    if (not encp_intf.get_bfid and not encp_intf.volume) and \
           e_errors.is_media(status) and is_read(request_dictionary):
        #Note: request_dictionary['bfid'] should always carry the
        # original bfid string while (request_dictionary['fc']['bfid']
        # should contain the bfid of the copy (original included) that
        # was just tried.

        fcc = get_fcc()
        #vcc = get_vcc()

        copy_list_dict = fcc.find_copies(request_dictionary['bfid'])
        if e_errors.is_ok(copy_list_dict):
            copy_list = copy_list_dict['copies']
        else:
            copy_list = []

        while copy_list:
            copy_index = random.randint(0, len(copy_list) - 1)
            next_bfid = copy_list[copy_index]

            #Use the values from get_clerks_info() instead of fcc.bfid_info()
            # and vcc.inquire_vol() directly.  The reason is that this
            # function inserts the address field into the dictionary, while
            # bfid_info() and inquire_vol() do not.
            vc_ticket, fc_ticket = get_clerks_info(next_bfid, encp_intf)
            if e_errors.is_ok(vc_ticket) and e_errors.is_ok(fc_ticket):
                request_dictionary['fc'] = fc_ticket
                request_dictionary['vc'] = vc_ticket
                retry_non_retriable_media_error = True
                message = "Trying alternate BFID %s after media error.  %s" \
                          % (next_bfid, elapsed_string())
                Trace.message(ERROR_LEVEL, message, out_fp=sys.stderr)
                Trace.log(e_errors.WARNING, message)
                break
            #elif not e_errors.is_ok(vc_ticket):
            #    print "Not using %s: %s" % (next_bfid, vc_ticket['status'])
            #elif not e_errors.is_ok(fc_ticket):
            #    print "Not using %s: %s" % (next_bfid, fc_ticket['status'])

            del copy_list[copy_index]

    #If the error is not retriable, remove it from the request queue.  There
    # are two types of non retriable errors.  Those that cause the transfer
    # to be aborted, and those that in addition to abborting the transfer
    # tell the operator that someone needs to investigate what happend.
    #
    #If the error is a media error AND there is another copy of the file
    # then perform the retry for the next copy.
    #
    #Another possibility is that skip_retry is true.  Then we treat this
    # error like a non-retriable error.
    if (e_errors.is_non_retriable(status[0]) and \
           not retry_non_retriable_media_error) \
           or error_dictionary.get('skip_retry', None):
        #Print error to stdout in data_access_layer format. However, only
        # do so if the dictionary is full (aka. the error occured after
        # the control socket was successfully opened).  Control socket
        # errors are printed elsewere (for reads only).
        error_dictionary['status'] = status
        #Flag this request as failed.  This may get thrown away if the
        # request_ticket is empty (by empty I mean it likely only has a
        # 'status' field).
        request_dictionary['completion_status'] = FAILURE
        #By checking those that aren't of significant length, we only
        # print these out on writes.
        if 1: #len(request_dictionary) > 3:
            print_data_access_layer_format(infile_print, outfile_print,
                                           file_size, error_dictionary)
            #If error_dictionary is the same dictionary as request_dictionary
            # then the following line is redundant as
            # print_data_access_layer_format() has already set it.
            # However, if they are different we need to set this here, so
            # that we don't need to increase usage of combine_dict any
            # more than currently used.
            request_dictionary['data_access_layer_printed'] = 1

        alarm_dict = {'infile':infile_print, 'outfile':outfile_print,
                      'status':status[1]}
        if e_errors.is_emailable(status[0]):
            storage_group = \
                          request_dictionary.get('vc', {}).get('storage_group',
                                                               None)
            if storage_group:
                #If we put the storage group into the ticket, then we
                # should be able to have the e-mail sent to the user.
                alarm_dict['patterns'] = {'sg' : storage_group,
                                          'node' : socket.gethostname(),
                                         }
            Trace.alarm(e_errors.EMAIL, status[0], alarm_dict)
        elif e_errors.is_alarmable(status[0]):
            Trace.alarm(e_errors.ERROR, status[0], alarm_dict)

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

        result_dict = {'status':status,
                       'resend':{'resubmits':resubmits, 'retry':retry},
                       'queue_size':queue_size}

    #When nothing was recieved from the mover and the 15min has passed,
    # resubmit all entries in the queue.  Leave the unique id the same.
    # Even though for writes there is only one entry in the active request
    # list at a time, submitting like this will still work.
    elif status[0] == e_errors.RESUBMITTING:

        ###Is the work done here duplicated in the next commented code line???
        # 1-21-2004 MWZ: By testing for a non-empty request_list this code
        # should never get called.  This was duplicating the resubmit
        # increase later on.
        if not request_list:
            request_dictionary['resend']['resubmits'] = resubmits + 1

        #Update the tickets callback fields.  The actual sockets
        # are updated because they are passed in by reference.  There
        # are some cases (most notably when internal_handle_retries()
        # is used) that there isn't a socket passed in to change.
        if request_list[0].get('route_selection', None) and udp_serv:
            udp_callback_addr = get_udp_callback_addr(
                encp_intf, udp_serv)[0] #Ignore the returned socket ref.
        else:
            udp_callback_addr = None

        #keep a list of transactions to wait for later.
        transaction_id_list = []

        for i in range(len(request_list)):
            try:
                #Increase the resubmit count.
                request_list[i]['resend']['resubmits'] = \
                          request_list[i]['resend'].get('resubmits', 0) + 1

                #Before resubmitting, there are some fields that the library
                # manager and mover don't expect to receive from encp,
                # these should be removed.
                for item in (item_remove_list):
                    try:
                        del request_list[i][item]
                    except KeyError:
                        pass

                #Update the ticket before sending it to library manager.
                if udp_callback_addr:
                    #The ticket item of 'routing_callback_addr' is a
                    # legacy name.
                    request_list[i]['routing_callback_addr'] = udp_callback_addr

                #Send this to log file.
                Trace.log(e_errors.WARNING, (e_errors.RESUBMITTING,
                                             request_list[i].get('unique_id',
                                                                 None)))

                #Since a retriable error occured, resubmit the ticket.
                if USE_NEW_EVENT_LOOP:
                    transaction_id, unused = resubmit_one_request_send(
                        request_list[i], encp_intf)
                    transaction_id_list.append(transaction_id)
                    lm_response = {'status' : (e_errors.OK, None)}
                else:
                    lm_response, unused = resubmit_one_request(request_list[i],
                                                               encp_intf)
            except (KeyboardInterrupt, SystemExit):
                raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
            except:
                exc, msg = sys.exc_info()[:2]
                #try:
                #    sys.stderr.write("%s: %s\n" % (str(exc), str(msg)))
                #    sys.stderr.flush()
                #except IOError:
                #    pass
                Trace.log(e_errors.ERROR,
                          "Resubmission error: %s: %s" % (str(exc), str(msg)))

                #I'm not sure what should happen with lm_response here.
                # Currently set it to all clear with the assumption that
                # it will RESUMBIT again in 15 (by default) minutes.  MWZ
                lm_response = {'status' : (e_errors.OK, None)}

            #Now it get checked.  But watch out for the recursion!!!
            internal_result_dict = internal_handle_retries([request_list[i]],
                                                           request_list[i],
                                                           lm_response,
                                                           encp_intf)

            #If an unrecoverable error occured while resubmitting to LM.
            if e_errors.is_non_retriable(internal_result_dict['status'][0]):
                result_dict = {'status':internal_result_dict['status'],
                               #'resend':{'retry':request_dictionary.get('resend', {}).get('retry', 0),
                               #          'resubmits':request_dictionary.get('resend', {}).get('resubmits', 0)},
                               'resend':{'retry':retry,
                                         'resubmits':resubmits + 1},
                               'queue_size':0,
                               'transaction_id_list':transaction_id_list,
                               }
                Trace.log(e_errors.ERROR, str(result_dict))
                #Since, we know which request resend gave this error, we
                # should include it in the result_dict so that it can
                # have its retry count incremented correctly.
                result_dict = combine_dict(result_dict, request_list[i])
                break
            #If a retriable error occured while resubmitting to LM.
            elif not e_errors.is_ok(internal_result_dict['status'][0]):
                result_dict = {'status':internal_result_dict['status'],
                               #'resend':{'retry':request_dictionary.get('resend', {}).get('retry', 0),
                               #          'resubmits':request_dictionary.get('resend', {}).get('resubmits', 0)},
                               'resend':{'retry':retry,
                                         'resubmits':resubmits + 1},
                               'queue_size':len(request_list),
                               'transaction_id_list':transaction_id_list,
                               }
                Trace.log(e_errors.ERROR, str(result_dict))
                #Since, we know which request resend gave this error, we
                # should include it in the result_dict so that it can
                # have its retry count incremented correctly.
                result_dict = combine_dict(result_dict, request_list[i])
                break
            #If no error occured while resubmitting to LM.
            else:
                result_dict = {'status':(e_errors.RESUBMITTING,
                                         request_list[i].get('unique_id',
                                                             None)),
                               #'resend':{'retry':request_dictionary.get('resend', {}).get('retry', 0),
                               #          'resubmits':request_dictionary.get('resend', {}).get('resubmits',  0)},
                               'resend':{'retry':retry,
                                         'resubmits':resubmits + 1},
                               'queue_size':len(request_list),
                               'transaction_id_list':transaction_id_list,
                               }

    #Change the unique id so the library manager won't remove the retry
    # request when it removes the old one.  Do this only when there was an
    # actuall error, not just a timeout.  Also, increase the retry count by 1.
    else:

        #Log the intermidiate error as a warning instead as a full error.
        Trace.log(e_errors.WARNING, "Retriable error: %s" % str(status))

        #Before getting a new unique id, remember the current one.  This is
        # so that multiple copy writes have a list of previous ids to check
        # against to determine if its original succeeded.
        if not request_dictionary.has_key('retried_unique_ids'):
            request_dictionary['retried_unique_ids'] = []
        old_unique_id = request_dictionary['unique_id']
        request_dictionary['retried_unique_ids'].append(old_unique_id)

        #Get a new unique id for the transfer request since the last attempt
        # ended in error.
        if status[0] != e_errors.TIMEDOUT:
            request_dictionary['unique_id'] = generate_unique_id()

        #keep a list of transactions to wait for later.
        transaction_id_list = []

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
            if USE_NEW_EVENT_LOOP:
                transaction_id, unused = resubmit_one_request_send(
                    request_dictionary, encp_intf)
                transaction_id_list.append(transaction_id)
                lm_response = {'status' : (e_errors.OK, None)}
            else:
                lm_response = resubmit_one_request(request_dictionary,
                                                   encp_intf)
        except KeyError, msg:
            lm_response = {'status':(e_errors.NET_ERROR,
                            "Unable to obtain response from library manager.")}
            try:
                sys.stderr.write("Error processing retry of %s.\n" %
                                 (request_dictionary['unique_id']))
                sys.stderr.write(pprint.pformat(request_dictionary)+"\n")
                sys.stderr.flush()
            except IOError:
                pass

        #Now it get checked.  But watch out for the recursion!!!
        internal_result_dict = internal_handle_retries([request_dictionary],
                                                       request_dictionary,
                                                       lm_response, encp_intf)


        #If an unrecoverable error occured while retrying to LM.
        if e_errors.is_non_retriable(internal_result_dict['status'][0]):
            result_dict = {'status':internal_result_dict['status'],
                           'resend':{'retry':request_dictionary.get('resend', {}).get('retry', 0),
                                     'resubmits':request_dictionary.get('resend', {}).get('resubmits', 0)},
                           'queue_size':len(request_list) - 1,
                           'transaction_id_list':transaction_id_list,
                           }
            Trace.log(e_errors.ERROR, str(result_dict))
        elif not e_errors.is_ok(internal_result_dict['status'][0]):
            result_dict = {'status':internal_result_dict['status'],
                           'resend':{'retry':request_dictionary.get('resend', {}).get('retry', 0),
                                     'resubmits':request_dictionary.get('resend', {}).get('resubmits', 0)},
                           'queue_size':len(request_list),
                           'transaction_id_list':transaction_id_list,
                           }
            Trace.log(e_errors.ERROR, str(result_dict))
        else:
            result_dict = {'status':(e_errors.RETRY,
                                     request_dictionary['unique_id']),
                           'resend':{'retry':request_dictionary.get('resend', {}).get('retry', 0),
                                     'resubmits':request_dictionary.get('resend', {}).get('resubmits', 0)},
                           'queue_size':len(request_list),
                           'transaction_id_list':transaction_id_list,
                           }


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
##
## The following functions are involved in calculating statistics for the
## transfer(s).
##
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

    if e_errors.is_ok(done_ticket['status'][0]):

        tinfo['%s_transfer_size'%(u_id,)] = MB_transfered
        tinfo['%s_network_time'%(u_id,)] = network_time
        tinfo['%s_drive_time'%(u_id,)] = drive_time
        tinfo['%s_disk_time'%(u_id,)] = disk_time

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

        library = done_ticket.get('vc', {}).get('library', "")
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

        print_values = (done_ticket['infilepath'],
                        done_ticket['outfilepath'],
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
                      done_ticket['infilepath'],
                      done_ticket['outfilepath'],
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
                          done_ticket['infilepath'],
                          done_ticket['outfilepath'],
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
                          ffw,
                          library,
                          )

    message = "[1] Time to calculate and record rate: %s sec." % \
              (time.time() - calculate_rate_start_time,)
    Trace.message(TIME_LEVEL, message)
    Trace.log(TIME_LEVEL, message)

############################################################################

def calculate_final_statistics(bytes, number_of_files, exit_status, tinfo):


    calculate_final_statistics_start_time = time.time()
    statistics = {}
    now = time.time()
    tinfo['total'] = now - tinfo['encp_start_time']

    for key,value in tinfo.iteritems():
        index = string.find(key,'_transfer_size')
        if index != -1:
            id = key[:index]
            statistics['transfer_size'] = statistics.get('transfer_size',0) + value
            for k in ('overall_time',
                      'transfer_time',
                      'network_time',
                      'drive_time',
                      'disk_time'):
                time_key = "%s_%s"%(id,k,)
                statistics[k] = statistics.get(k,0) + tinfo[time_key]

    statistics['MB_per_S_overall'] = statistics['transfer_size']/statistics['overall_time'] if statistics.get('overall_time') else 0
    statistics['MB_per_S_transfer'] = statistics['transfer_size']/statistics['transfer_time'] if statistics.get('transfer_time') else 0
    statistics['MB_per_S_network'] = statistics['transfer_size']/statistics['network_time'] if statistics.get('network_time') else 0
    statistics['MB_per_S_drive'] = statistics['transfer_size']/statistics['drive_time'] if statistics.get('drive_time') else 0
    statistics['MB_per_S_disk'] = statistics['transfer_size']/statistics['disk_time'] if statistics.get('disk_time') else 0


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

    message = "[1] Time to calculate final statistics: %s sec." % \
              (time.time() - calculate_final_statistics_start_time,)
    Trace.message(TIME_LEVEL, message)
    Trace.log(TIME_LEVEL, message)

    return done_ticket

############################################################################
#Support functions for writes.
############################################################################

#Args:
# Takes a list of request tickets.
#Returns:
#None
#Verifies that various information in the tickets are correct, valid, spelled
# correctly, etc.
def verify_write_request_consistancy(request_list, e):

    verify_write_request_consistancy_start_time = time.time()

    outputfile_dict = {}

    for request in request_list:

        if request['infile'] not in ["/dev/zero",
                                     "/dev/random", "/dev/urandom"]:
            try:
                inputfile_check(request, e)
            except IOError, msg:
                Trace.handle_error(severity=99)
                raise EncpError(msg.args, str(msg), e_errors.IOERROR,
                                {'infilepath' : request['infilepath'],
                                 'outfilepath' : request['outfilepath']})
            except OSError, msg:
                Trace.handle_error(severity=99)
                raise EncpError(msg.args, str(msg), e_errors.OSERROR,
                                {'infilepath' : request['infilepath'],
                                 'outfilepath' : request['outfilepath']})
            except (socket.error, select.error), msg:
                Trace.handle_error(severity=99)
                #On 11-12-2009, tracebacks were found from migration encps that
                # started failing because there were too many open files while
                # trying to instantiate client classes.  The socket.error
                # should have been caught here.  So now it is.
                raise EncpError(msg.ars, str(msg), e_errors.NET_ERROR,
                                {'infilepath' : request['infilepath'],
                                 'outfilepath' : request['outfilepath']})

        if request['outfile'] not in ["/dev/null", "/dev/zero",
                                      "/dev/random", "/dev/urandom"]:
            if not request['wrapper']['inode']:
                try:
                    #Only test this before the output file is created.
                    outputfile_check(request, e)
                except IOError, msg:
                    Trace.handle_error(severity=99)
                    raise EncpError(msg.args, str(msg), e_errors.IOERROR,
                                    {'infilepath' : request['infilepath'],
                                     'outfilepath' : request['outfilepath']})
                except OSError, msg:
                    Trace.handle_error(severity=99)
                    raise EncpError(msg.args, str(msg), e_errors.OSERROR,
                                    {'infilepath' : request['infilepath'],
                                     'outfilepath' : request['outfilepath']})
                except (socket.error, select.error), msg:
                    Trace.handle_error(severity=99)
                    #On 11-12-2009, tracebacks were found from migration encps
                    # that started failing because there were too many open
                    # files while trying to instantiate client classes.  The
                    # socket.error should have been caught here.  So now it is.
                    raise EncpError(msg.ars, str(msg), e_errors.NET_ERROR,
                                    {'infilepath' : request['infilepath'],
                                     'outfilepath' : request['outfilepath']})
            else:
                #We should only get here when called from read_hsm_file()
                # or write_hsm_file().  In any case, the file should still
                # exist by this point.  As a simple test, make sure it
                # still does.
                try:
                    unused = get_stat(request['outfile'])
                except IOError, msg:
                    Trace.handle_error(severity=99)
                    raise EncpError(msg.args, str(msg), e_errors.IOERROR,
                                    {'infilepath' : request['infilepath'],
                                     'outfilepath' : request['outfilepath']})
                except OSError, msg:
                    Trace.handle_error(severity=99)
                    raise EncpError(msg.args, str(msg), e_errors.OSERROR,
                                    {'infilepath' : request['infilepath'],
                                     'outfilepath' : request['outfilepath']})
                except (socket.error, select.error), msg:
                    Trace.handle_error(severity=99)
                    #On 11-12-2009, tracebacks were found from migration encps
                    # that started failing because there were too many open
                    # files while trying to instantiate client classes.  The
                    # socket.error should have been caught here.  So now it is.
                    raise EncpError(msg.ars, str(msg), e_errors.NET_ERROR,
                                    {'infilepath' : request['infilepath'],
                                     'outfilepath' : request['outfilepath']})

        #This block of code makes sure the the user is not moving
        # two files with the same basename in different directories
        # into the same destination directory.
        result = outputfile_dict.get(request['outfilepath'], None)
        if result and not request.get('copy', None):
            #If the file is already in the list (and not a copy), give error.
            raise EncpError(None,
                            'Duplicate file entry: %s' % (result,),
                            e_errors.USERERROR,
                            {'infilepath' : request['infilepath'],
                             'outfilepath' : request['outfilepath']})
        else:
            #Put into one place all of the output names.  This is to check
            # that two file to not have the same output name.
            outputfile_dict[request['outfile']] = request['infilepath']

        #Verify that the tags contain 'sane' characters.
        tag_check(request)

        #Verify that the library and wrappers are valid.
        librarysize_check(request)
        wrappersize_check(request)
        #If using null movers, make sure of a few things.
        try:
            null_mover_check(request, e)
        except:
            Trace.handle_error(severity=99)
            raise

    message = "[1] Time to verify write request consistancy: %s sec." % \
              (time.time() - verify_write_request_consistancy_start_time,)
    Trace.message(TIME_LEVEL, message)
    Trace.log(TIME_LEVEL, message)

############################################################################

def set_sfs_settings(ticket, intf_encp):

    set_metadata_start_time = time.time()  #Start time of all set operations.

    # create a new pnfs object pointing to current output file
    Trace.message(INFO_LEVEL,
            "Updating %s file metadata." % ticket['wrapper']['pnfsFilename'])

    location_start_time = time.time() # Start time of verifying pnfs file.

    if is_read(ticket):
        pnfs_filename = ticket['infile']
    else: #write
        pnfs_filename = ticket['outfile']

    #Make sure the file is still there.  This check is done with
    # access_check() because it will loop incase pnfs is automounted.
    # If the return is zero, then it wasn't found.
    #Note: There is a possible race condition here if the file is
    #      (re)moved after it is determined to remain in the original
    #      location, but before the metadata is set.
    #Note2: Now that pnfs_filename should only contain the .(access)()
    # name at this point, most of the detection of where the file located
    # now is largely obsolete.

    if access_check(pnfs_filename, os.F_OK) == 0:
        #With the remembered PNFS/Chimera id, try to find out where it was
        # moved to if it was in fact moved.
        sfsid = ticket['fc'].get('pnfsid', None)
        if sfsid:
            try:
                if intf_encp.outtype == RHSMFILE \
                       or intf_encp.intype == RHSMFILE: #intype for "get".
                    sfs = get_pac()
                    path = sfs.get_path(sfsid,
                                        get_directory_name(pnfs_filename),
                                        shortcut = intf_encp.shortcut)[0]
                else:   # HSMFILE
                    sfs = namespace.StorageFS(sfsid)
                    path = sfs.get_path()[0]  #Find the new path.
                Trace.log(e_errors.INFO,
                          "File %s was moved to %s." %
                          (ticket['wrapper']['pnfsFilename'], path))
                ticket['wrapper']['pnfsFilename'] = path  #Remember new path.
                if is_write(ticket):
                    ticket['outfile'] = path
                else:  #is_read() for "get".
                    ticket['infile'] = path
            except (OSError, IOError, AttributeError, ValueError):
                #Get the exception.
                exc, msg = sys.exc_info()[:2]
                ticket['status'] = (e_errors.USERERROR,
                                    "SFS file %s has been removed." %
                                    ticket['wrapper']['pnfsFilename'])
                #Log the problem.
                Trace.log(e_errors.ERROR,
                          "Trouble with sfs: %s %s %s." %
                          (str(ticket['status']), str(exc), str(msg)))
                return
        else:
            ticket['status'] = (e_errors.USERERROR,
                                "SFS file %s has been removed." %
                                ticket['wrapper']['pnfsFilename'])
            #Log the problem.
            Trace.log(e_errors.ERROR,
                      "Trouble with sfs: %s" % (ticket['status'],))
            return
    else:
        #Check to make sure that the inodes are still the same.
        verify_inode(ticket)
        if not e_errors.is_ok(ticket):
            #Ticket is already set.
            return

        try:
            sfs = namespace.StorageFS(pnfs_filename)
        except (KeyboardInterrupt, SystemExit):
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except:
            #Get the exception.
            exc, msg = sys.exc_info()[:2]
            #If it is a user access/permitted problem handle accordingly.
            if hasattr(msg, "errno") and \
                   msg.errno in [errno.EACCES, errno.EPERM]:
                ticket['status'] = (e_errors.USERERROR, str(msg))
            #Handle all other errors.
            else:
                ticket['status'] = (e_errors.PNFS_ERROR, str(msg))
            #Log the problem.
            Trace.log(e_errors.INFO,
                      "Trouble with sfs (StorageFS): %s %s %s." %
                      (ticket['status'][0], str(exc), str(msg)))
            return

    message = "[2] Time to veify pnfs file location: %s sec." % \
              (time.time() - location_start_time,)
    Trace.message(TIME_LEVEL, message)
    Trace.log(TIME_LEVEL, message)

    if not ticket.get('copy', None):  #Don't set layer 1 if copy.
        layer1_start_time = time.time() # Start time of setting pnfs layer 1.

        #The first piece of metadata to set is the bit file id which is placed
        # into layer 1.
        Trace.message(INFO_LEVEL, "Setting layer 1: %s" %
                      ticket["fc"]["bfid"])
        try:
            # save the bfid
            sfs.set_bit_file_id(ticket["fc"]["bfid"], pnfs_filename)
        except (KeyboardInterrupt, SystemExit):
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except:
            #Get the exception.
            exc, msg = sys.exc_info()[:2]
            #If it is a user access/permitted problem handle accordingly.
            if hasattr(msg, "errno") and \
                   msg.errno in [errno.EACCES, errno.EPERM]:
                ticket['status'] = (e_errors.USERERROR, str(msg))
            #Handle all other errors.
            else:
                ticket['status'] = (e_errors.PNFS_ERROR, str(msg))
            #Log the problem.
            Trace.log(e_errors.ERROR,
                      "Trouble with pnfs (set_bit_file_id): %s %s %s." %
                      (ticket['status'][0], str(exc), str(msg)))
            return

        message = "[2] Time to set pnfs layer 1: %s sec." % \
                  (time.time() - layer1_start_time,)
        Trace.message(TIME_LEVEL, message)
        Trace.log(TIME_LEVEL, message)

    #Format some tape drive output.
    mover_ticket = ticket.get('mover', {})
    drive = "%s:%s" % (mover_ticket.get('device', 'Unknown'),
                       mover_ticket.get('serial_num','Unknown'))
    #For writes to null movers, make the crc zero.
    if mover_ticket['driver'] == "NullDriver":
        crc = 0
    else:
        #This crc should come from the mover, so it should match whichever
        # crc seed encp told it to use.
        crc = ticket['fc']['complete_crc']

    try:
        #Perform the following get functions; even if it is a copy.  These,
        # calls set values in the object that are used to also update
        # file db entires for copies.
        sfs_id = sfs.get_id(pnfs_filename)
    except (KeyboardInterrupt, SystemExit):
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
    except:
        #Get the exception.
        exc, msg = sys.exc_info()[:2]
        #If it is a user access/permitted problem handle accordingly.
        if hasattr(msg, "errno") and \
               msg.errno in [errno.EACCES, errno.EPERM]:
            ticket['status'] = (e_errors.USERERROR, str(msg))
        #Handle all other errors.
        else:
            ticket['status'] = (e_errors.PNFS_ERROR, str(msg))
        #Log the problem.
        Trace.log(e_errors.ERROR,
                      "Trouble with pnfs (get_id): %s %s %s." %
                      (ticket['status'][0], str(exc), str(msg)))
        return

    if not ticket.get('copy', None):  #Don't set layer 4 if copy.
        layer4_start_time = time.time() # Start time of setting pnfs layer 4.

        #Store the cross reference data into layer 4.

        #Write to the metadata layer 4 "file".
        Trace.message(INFO_LEVEL, "Setting layer 4.")
        try:
            sfs.set_xreference(
                ticket["fc"]["external_label"],
                ticket["fc"]["location_cookie"],
                ticket["fc"]["size"],
                volume_family.extract_file_family(ticket["vc"]["volume_family"]),
                ticket['wrapper']['pnfsFilename'],  #Normal path.
                "", #p.volume_filepath,
                sfs_id,
                "", #p.volume_fileP.id,
                ticket["fc"]["bfid"],
                drive,
                crc,
                filepath=pnfs_filename)             #.(access)() path
        except (KeyboardInterrupt, SystemExit):
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except:
            #Get the exception.
            exc, msg = sys.exc_info()[:2]
            #If it is a user access/permitted problem handle accordingly.
            if hasattr(msg, "errno") and \
                   msg.errno in [errno.EACCES, errno.EPERM]:
                ticket['status'] = (e_errors.USERERROR, str(msg))
            #Handle all other errors.
            else:
                ticket['status'] = (e_errors.PNFS_ERROR, str(msg))

            #Log the problem.
            Trace.log(e_errors.ERROR,
                      "Trouble with pnfs (set_xreference): %s %s %s." %
                      (ticket['status'][0], str(exc), str(msg)))
            return

        message = "[2] Time to set pnfs layer 4: %s sec." % \
                  (time.time() - layer4_start_time,)
        Trace.message(TIME_LEVEL, message)
        Trace.log(TIME_LEVEL, message)

    filedb_start_time = time.time() # Start time of updating file database.

    #Update the file database with the transfer info.
    Trace.message(INFO_LEVEL, "Setting file db pnfs fields.")
    try:
        # add the pnfs ids and filenames to the file clerk ticket and store it
        fc_ticket = {}
        fc_ticket["fc"] = ticket['fc'].copy()
        fc_ticket["fc"]["pnfsid"] = sfs_id
        fc_ticket["fc"]["pnfsvid"] = "" #p.volume_fileP.id
        fc_ticket["fc"]["pnfs_name0"] = ticket['wrapper']['pnfsFilename']
        fc_ticket["fc"]["pnfs_mapname"] = "" #p.mapfile
        fc_ticket["fc"]["drive"] = drive
        fc_ticket["fc"]['uid'] = ticket['wrapper']['uid']
        fc_ticket["fc"]['gid'] = ticket['wrapper']['gid']
        fc_ticket["fc"]['library'] = ticket['vc'].get('library', None)
        fc_ticket["fc"]['original_library'] = ticket.get('original_library', None)
        fc_ticket["fc"]['file_family_width'] = ticket['vc'].get('file_family_width', 1)


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

    message = "[2] Time to set file database: %s sec." % \
              (time.time() - filedb_start_time,)
    Trace.message(TIME_LEVEL, message)
    Trace.log(TIME_LEVEL, message)

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
                pstat = sfs.get_stat(pnfs_filename)
                size = pstat[stat.ST_SIZE]
                if long(size) == long(ticket['file_size']) or long(size) == 1L:
                    Trace.log(e_errors.INFO,
                              "Filesize (%s) for file %s already set." %
                              (ticket['file_size'],
                               ticket['wrapper']['pnfsFilename']))
                else:
                    # set the file size
                    sfs.set_file_size(ticket['file_size'], pnfs_filename)
        except (KeyboardInterrupt, SystemExit):
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except IOError, msg:
            ticket['status'] = (e_errors.IOERROR, str(msg))
            return
        except OSError, msg:
            ticket['status'] = (e_errors.OSERROR, str(msg))
            return
        except:
            Trace.handle_error()
            exc, msg = sys.exc_info()[:2]
            ticket['status'] = (str(exc), str(msg))
            return

        message = "[2] Time to set filesize: %s sec." % \
                  (time.time() - filesize_start_time,)
        Trace.message(TIME_LEVEL, message)
        Trace.log(TIME_LEVEL, message)

    #This function writes errors/warnings to the log file and put an
    # error status in the ticket.
    verify_file_size(ticket) #Verify size is the same.

    message = "[1] Time to set metadata: %s sec." % \
              (time.time() - set_metadata_start_time,)
    Trace.message(TIME_LEVEL, message)
    Trace.log(TIME_LEVEL, message)

############################################################################
#Functions for writes.
############################################################################


def create_write_requests(callback_addr, udp_callback_addr, e, tinfo):

    create_write_requests_start_time = time.time()
    request_list = []

    #Initialize these, so that they can be set only once.
    use_copies = 0

    # create internal list of input unix files even if just 1 file passed in
    if type(e.input) == types.ListType:
        pass  #e.input = e.input
    else:
        e.input = [e.input]

    if not e.put_cache and len(e.output) > 1:
        raise EncpError(None,
                        'Cannot have multiple output files',
                        e_errors.USERERROR)

    output_file = ''
    if e.put_cache:
        output_file = e.put_cache
    else:
        output_file = e.output[0]
    if e.shortcut and e.override_path:
        use_mount_point = os.path.dirname(e.override_path)
    else:
        use_mount_point = e.pnfs_mount_point
    if e.outtype == RHSMFILE:
        t = sfs = namespace.StorageFS(output_file, use_pnfs_agent = True,
                                      mount_point = use_mount_point,
                                      shortcut = e.shortcut)
    else:
        sfs = namespace.StorageFS(output_file, use_pnfs_agent = False,
                                  mount_point = use_mount_point,
                                  shortcut = e.shortcut)
        t = namespace.Tag(output_file)

    # check the input unix file. if files don't exists, we bomb out to the user
    tags = None

    for i in range(len(e.input)):
        work_ticket = {}
        if tags:
            #Have create_write_request() get these only once.
            work_ticket['vc'] = tags
        if e.put_cache:
            use_infile = e.put_cache #used for error reporting
        else:
            work_ticket['infile'] = e.input[i]
            use_infile = work_ticket['infile'] #used for error reporting

        try:
            work_ticket, tags = create_write_request(work_ticket, i,
                                                     callback_addr,
                                                     udp_callback_addr,
                                                     sfs, t, e, tinfo)
        except (KeyboardInterrupt, SystemExit):
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except (OSError, IOError), msg:
            Trace.handle_error(severity=99)
            if isinstance(msg, OSError):
                e_type = e_errors.OSERROR
            else:
                e_type = e_errors.IOERROR

            if getattr(msg, "filename", None):
                use_error_filename = msg.filename
            else:
                use_error_filename = use_infile
            raise EncpError(msg.args[0], "%s: %s" % (str(msg),
                                                     use_error_filename),
                            e_type, {'infilepath' : use_infile})

        if work_ticket == None:
            #This is a rare possibility.
            continue

        request_list.append(work_ticket)

    #If the user overrides the copy count, use that instead.
    use_copies = len(tags['all_libraries'][1:])
    if e.copies != None:
        use_copies = e.copies

    #Make dictionaries for the copies of the data.
    request_copy_list = []
    if use_copies > 0:

        parmameter_libraries = e.output_library.split(",")
        tag_libraries = t.get_library(os.path.dirname(work_ticket['outfile'])).split(",")

        #Determine the starting copy number.  This will only happen if
        # --file-family is used to specify a "_copy_#" file family.  Only
        # exceptional cases like duplication should do that.
        mul_copy = re.search("_copy_[0-9]*", work_ticket['vc']['file_family'])
        if mul_copy:
            start_copy_number = int(mul_copy.group().replace("_copy_", ""))
        else:
            start_copy_number = None

        #Create use_copies number of multiple copy requests and put them
        # into the request list.
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
                    raise EncpError(None,
                                    "Too many copies requested for the "
                                    "number of configured copy libraries.",
                                    e_errors.USERERROR)

            for work_ticket in request_list:
                copy_ticket = copy.deepcopy(work_ticket)
                #Specify the copy number; this is the copy number relative to
                # this encp.
                if start_copy_number != None:
                    #Subtract one from the total, since we want to start at
                    # start_copy_number, but n_copy starts at one.  Only
                    # exceptional cases like duplication should do this.
                    copy_ticket['copy'] = start_copy_number + n_copy - 1
                else:
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


        ##We need to update the intent of this original to include
        ## the number of copies we are going to make.
        work_ticket['fc']['copies'] = use_copies

    message = "[1] Time to create write requests: %s sec." % \
              (time.time() - create_write_requests_start_time,)
    Trace.message(TIME_LEVEL, message)
    Trace.log(TIME_LEVEL, message)

    return request_list + request_copy_list

def create_write_request(work_ticket, file_number,
                         callback_addr, udp_callback_addr,
                         sfs, t, e, tinfo):

        if e.put_cache:

            if e.shortcut and e.override_path:
                #If the user specified a pathname (with --override-path)
                # on the command line use that name.  Otherwise if just
                # --shortcut is used, the filename name will be recored as
                # a /.(access)(<PNFS/Chimera_id>) style filename.
                ofullname = e.override_path
            else:
                ofullname_list = sfs.get_path(e.put_cache, e.pnfs_mount_point,
                                            shortcut=e.shortcut)

                if len(ofullname_list) == 1:
                    ofullname = ofullname_list[0]
                else:
                    EncpError(errno.ENOENT,
                              "Unable to find correct PNFS file.",
                              e_errors.PNFS_ERROR,
                              {'onfilepath' : ofullname_list})

            #Determine the access path name.
            #oaccessname = namespace.StorageFS(ofullname).access_file(
            oaccessname = sfs.access_file(get_directory_name(ofullname),
                                          e.put_cache)

            unused, ifullname, unused, unused = fullpath(e.input[0])
            istatinfo = file_utils.get_stat(ifullname)
        else: #The output file was given as a normal filename.
            #ifullname, ofullname = get_ninfo(e.input[i], e.output[0], e)

            try:
                istatinfo = file_utils.get_stat(work_ticket['infile'])
                ifullname = work_ticket['infile']
            except (KeyboardInterrupt, SystemExit):
                raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
            except (OSError, IOError):
                ifullname = get_ininfo(work_ticket['infile'])
                istatinfo = file_utils.get_stat(ifullname)

            #inputfile_check(ifullname, e)

            ofullname = get_oninfo(ifullname, e.output[0], e)

            #Determine the access path name.  (There isn't one yet, since
            # the file has not been created.)
            oaccessname = ofullname

            #The existence rules behind the output file are more
            # complicated than those of the input file.  We always need
            # to call outputfile_check.  It still should go in
            # some verify function though.
            #
            #Fundamentally this belongs in verify_read_request_consistancy(),
            # but information needed about the input file requires this check.
            #outputfile_check(ifullname, ofullname, e)

        odirname = get_directory_name(ofullname)
        ostatinfo = sfs.get_stat(odirname)

        #See if these are already known.
        try:
            tags = work_ticket['vc'].copy()
        except KeyError:
            tags = {}
        all_libraries = tags.get('all_libraries', None)
        library = tags.get('library', None)
        file_family = tags.get('file_family', None)
        file_family_width = tags.get('file_family_width', None)
        file_family_wrapper = tags.get('file_family_wrapper', None)
        storage_group = tags.get('storage_group', None)
        try:
            #Only need this after the last loop (when we don't get here).
            del tags['all_libraries']
        except KeyError:
            pass

        #There is no sense to get these values every time.  Only get them
        # on the first pass.
        if not library:
            if e.output_library:
                #Only take the first item of a possible comma seperated list.
                all_libraries = e.output_library.split(",")
                library = all_libraries[0]
                #use_copies = len(all_libraries[1:])
            if not library:
                #If library is still empty, use the default
                all_libraries = t.get_library(odirname).split(",")
                library = all_libraries[0]
                #use_copies = len(all_libraries[1:])
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
        if e.put_cache:
            #For non-dcache writes this information is inserted shortly
            # after the output file is created.  In this case the file
            # already exists, so we just include it here.
            file_clerk['pnfsid'] = e.put_cache

        #Determine the max resend values for this transfer.
        csc = get_csc()

        resend = max_attempts(csc, volume_clerk['library'], e)

        #Get these two pieces of information about the local input file.
        if ifullname in ["/dev/zero", "/dev/random", "/dev/urandom"]:
            bound_limits = [long(os.environ.get('ENSTORE_RANDOM_LB', 0L)),
                            long(os.environ.get('ENSTORE_RANDOM_UB',
                                                2147483648L))]
            bound_limits.sort()
            file_size = long(random.uniform(bound_limits[0], bound_limits[1]))
        #If we are reading from standard in don't set the size.
        elif ifullname.startswith("/dev/fd"):
            file_size = None
        else:
            file_size = long(istatinfo[stat.ST_SIZE])
        file_inode = istatinfo[stat.ST_INO]

        #Get the crc seed to use.
        encp_dict = csc.get('encp', 5, 5)
        crc_seed = encp_dict.get('crc_seed', 1)

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
        if file_number < (len(e.input) - 1):
            #In minutes.
            encp_el['delayed_dismount'] = max(3, encp_el['delayed_dismount'])

        #config = host_config.get_config()
        #if config and config.get('interface', None):
        #    route_selection = 1
        #else:
        #    route_selection = 0
        #if udp_callback_addr:
        #    route_selection = 1   #1 to use udp_serv, 0 for no.
        #else:
        #    route_selection = 0

        #We cannot use --shortcut without --override-path when
        # using NULL movers and tapes.  The mover insists that
        # "NULL" appear in the pnfs pathname, which conflicts with
        # the task of --shortcut.  So, we need to breakdown and do
        # this full name lookup.
        Trace.log(99, "shortcut: %s  override_path: %s  wrapper: %s" % \
                  (e.shortcut, e.override_path, volume_clerk['wrapper']))
        if e.shortcut and not e.override_path:
            if is_null_media_type(volume_clerk):
                #To avoid an error with the mover, perform a full
                # pnfs pathname lookup.
                ofullname_list = sfs.get_path(e.put_cache,
                                              e.pnfs_mount_point,
                                              shortcut=False)
                ofullname = ofullname_list[0]
                wrapper['pnfsFilename'] = ofullname

        #We need to stop writing /pnfs/fs/usr/Migration/ paths in the wrappers
        # for every migrated to tape.  Correct the
        # work_ticket['wrapper']['pnfsFilename'] value.
        if e.migration_or_duplication and e.override_path:
            wrapper['pnfsFilename'] = e.override_path

        #work_ticket = {}
        work_ticket['callback_addr'] = callback_addr
        work_ticket['client_crc'] = e.chk_crc
        work_ticket['crc_seed'] = crc_seed
        work_ticket['encp'] = encp_el
        work_ticket['encp_daq'] = encp_daq
        work_ticket['fc'] = file_clerk
        work_ticket['file_size'] = file_size
        work_ticket['ignore_fair_share'] = e.ignore_fair_share
        work_ticket['infile'] = ifullname
        work_ticket['infilepath'] = ifullname
        work_ticket['local_inode'] = file_inode
        if udp_callback_addr: #For "put" only.
            work_ticket['method'] = "write_next"
        work_ticket['outfile'] = oaccessname
        work_ticket['outfilepath'] = ofullname
        work_ticket['override_ro_mount'] = e.override_ro_mount
        work_ticket['resend'] = resend
        work_ticket['retry'] = None #LM legacy requirement.
        if udp_callback_addr: #For "put" only.
            work_ticket['routing_callback_addr'] = udp_callback_addr
            work_ticket['route_selection'] = 1
        work_ticket['times'] = tinfo.copy() #Only info now in tinfo needed.
        work_ticket['unique_id'] = generate_unique_id()
        work_ticket['user_level'] = e.user_level
        work_ticket['vc'] = volume_clerk
        work_ticket['version'] = encp_client_version()
        work_ticket['work'] = "write_to_hsm"
        work_ticket['wrapper'] = wrapper

        tags = {}
        tags['library'] = library
        tags['all_libraries'] = all_libraries
        tags['file_family'] = file_family
        tags['file_family_width'] = file_family_width
        tags['file_family_wrapper'] = file_family_wrapper
        tags['storage_group'] = storage_group

        return work_ticket, tags

############################################################################

def submit_write_request(work_ticket, encp_intf):

    submit_write_request_start_time = time.time()

    # send the work ticket to the library manager
    while encp_intf.max_retry == None or \
          work_ticket.get('resend', {}).get('retry', 0) <= encp_intf.max_retry:
        try:
            ticket, lmc = submit_one_request(work_ticket, encp_intf)
        except (socket.error, select.error, e_errors.EnstoreError), msg:
            lmc = None
            if msg.errno == errno.ETIMEDOUT:
                ticket = {'status' : (e_errors.TIMEDOUT,
                            work_ticket.get('vc', {}).get('library', None))}
            else:
                ticket = {'status' : (e_errors.NET_ERROR, str(msg))}

        result_dict = handle_retries([work_ticket], work_ticket, ticket,
                                     encp_intf)

        if e_errors.is_ok(result_dict['status'][0]):
	    ticket['status'] = result_dict['status']
            #return ticket
            break
        elif e_errors.is_retriable(result_dict['status'][0]):
            continue
        else:
            ticket['status'] = result_dict['status']
            #return ticket
            break
    else:
	ticket['status'] = (e_errors.TOO_MANY_RETRIES, ticket['status'])

    #Some KeyErrors are occuring because 'file_family' doesn't exist.  Need
    # to log this for debugging.
    #
    #One case is now understood.  Some errors returned by the library
    # manager contain a 'status' value and the original request, converted
    # to a string, under the key 'request'.
    try:
        if ticket['vc']['file_family']:
            pass
        if ticket['infile']:
            pass
        if ticket['vc']['library']:
            pass
        if ticket['file_size']:
            pass
    except KeyError:
        message = "UNEXPECTED TICKET FORMAT DETECTED: %s" % ticket
        Trace.log(e_errors.INFO, message)

    Trace.message(TO_GO_LEVEL, "SUBMITED: %s" % (1,))
    Trace.message(TRANSFER_LEVEL,
                  "File queued: %s library: %s family: %s bytes: %s %s" %
                  (ticket.get('infile', "UNKNOWN"),
                   ticket.get('vc', {}).get('library' "UNKNOWN"),
                   ticket.get('vc', {}).get('file_family', "UNKNOWN"),
                   ticket.get('file_size', -1),
                   elapsed_string()))

    message = "[1] Time to submit %d write request: %s sec." % \
              (1, time.time() - submit_write_request_start_time,)
    Trace.message(TIME_LEVEL, message)
    Trace.log(TIME_LEVEL, message)

    return ticket, lmc

############################################################################

def stall_write_transfer(data_path_socket, control_socket, e):

    stall_write_transfer_start_time = time.time()

    Trace.log(INFO_LEVEL,
              "stalling write transfer until signal bytes arives: %s sec" % \
              (e.mover_timeout,))

    #Stall starting the count until the first byte is ready for writing.
    duration = e.mover_timeout
    while 1:
        start_time = time.time()
        try:
            write_fd = select.select([], [data_path_socket], [],
                                     duration)[1]
            break
        except (select.error, socket.error), msg:
            if msg.args[0] in [errno.EINTR, errno.EAGAIN]:
                #If the select was interupted by a signal, keep going.
                duration = duration - (time.time() - start_time)
                continue
            else:
                write_fd = []
                break

    if data_path_socket not in write_fd:
        Trace.log(INFO_LEVEL,
                  "confirming control_socket still okay: %s sec" % (10,))

        try:
            read_control_fd, unused, unused = select.select([control_socket],
                                                            [], [], 10)
            if control_socket in read_control_fd:
                status_ticket = callback.read_tcp_obj(control_socket)
                return status_ticket
            else:
                status_ticket = {'status' : (e_errors.UNKNOWN,
                                             "No data written to mover.")}
        except (select.error, socket.error, e_errors.EnstoreError), msg:
            status_ticket = {'status' : (e_errors.NET_ERROR,
                                         "%s: %s" % (str(msg),
                                               "No data read from mover."))}
        except e_errors.TCP_EXCEPTION:
            status_ticket = {'status' : (e_errors.NET_ERROR,
                                         e_errors.TCP_EXCEPTION)}

        return status_ticket

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
            if msg.args[0] in [errno.EINTR, errno.EAGAIN]:
                #If the select was interupted by a signal, keep going.
                duration = duration - (time.time() - start_time)
                continue
            else:
                read_fd = []
                break

    #If there is no data waiting in the buffer, we have an error.
    if (data_path_socket not in read_fd) \
           or (len(data_path_socket.recv(1, socket.MSG_PEEK)) == 0):
        try:
            read_control_fd, unused, unused = select.select([control_socket],
                                                            [], [], 10)
            if control_socket in read_control_fd:
                status_ticket = callback.read_tcp_obj(control_socket)
            else:
                status_ticket = {'status' : (e_errors.UNKNOWN,
                                             "No data written to mover.")}
        except (select.error, socket.error, e_errors.EnstoreError), msg:
            status_ticket = {'status' : (e_errors.NET_ERROR,
                                         "%s: %s" % (str(msg),
                                               "No data read from mover."))}
        except e_errors.TCP_EXCEPTION:
            status_ticket = {'status' : (e_errors.NET_ERROR,
                                         e_errors.TCP_EXCEPTION)}
    else:
        status_ticket = {'status' : (e_errors.OK, None)}

    message = "[1] Time to stall %d write transfer: %s sec." % \
              (1, time.time() - stall_write_transfer_start_time,)
    Trace.message(TIME_LEVEL, message)
    Trace.log(TIME_LEVEL, message)

    return status_ticket

############################################################################

def write_post_transfer_update(done_ticket, e):
    #This function writes errors/warnings to the log file and puts an
    # error status in the ticket.
    check_crc(done_ticket, e) #Check the CRC.

    #Verify that the file transfered in tacted.
    if not e_errors.is_ok(done_ticket):
        return done_ticket

    if not e.put_cache:
        #Update the modification time only for direct encp files
        update_modification_time(done_ticket['outfile'])

    #We know the file has hit some sort of media. When this occurs
    # create a file in the storage namespace with information about transfer.
    set_sfs_settings(done_ticket, e)

    #Verify that the pnfs info was set correctly.
    if not e_errors.is_ok(done_ticket):
        clear_layers_1_and_4(done_ticket) #Reset this.
        return done_ticket


    #Set the UNIX file permissions.
    #Writes errors to log file.
    #The last peice of metadata that should be set is the filesize.  This
    # is done last inside of set_sfs_settings().  Unfortunatly, write
    # permissions are needed to set the filesize.  If setting the
    # permissions goes first and write permissions are not included
    # in the values from the input file then the transer will fail.  Thus
    # setting the outfile permissions is done after setting the filesize,
    # however, if setting the permissions fails the file is left alone
    # but it is still treated like a failed transfer.  Worst case senerio
    # on a failure is that the file is left with full permissions.
    set_outfile_permissions(done_ticket, e)

    if not e_errors.is_ok(done_ticket):
        done_ticket['skip_retry'] = 1
        return done_ticket

    #Update the source files times.  We need to do this after we
    # are done with the locking around the euid for the output file.
    update_last_access_time(done_ticket['infile'])

    return done_ticket

############################################################################

def write_hsm_file(work_ticket, control_socket, data_path_socket,
                   tinfo, e, udp_serv = None):

        overall_start = time.time() #----------------------------Overall Start


        ### 8-22-2008: Commented out the verify_write_request_consistancy()
        ### call here.  It does a lot of checks that don't need to be done
        ### that also put a lot of strain on PNFS.  The handle_retries()
        ### pnfs_filename check just after open_local_file should be
        ### sufficent.
        """
        #Be paranoid.  Check this the ticket again.
        try:
            verify_write_request_consistancy([work_ticket], e)
        except EncpError, msg:
            msg.ticket['status'] = (msg.type, msg.strerror)
            return msg.ticket
        """

        #This should be redundant error check.
        if not control_socket or not data_path_socket:
	    work_ticket = combine_dict({'status':(e_errors.NET_ERROR, "No socket")},
				   work_ticket)
            return work_ticket #This file failed.

        Trace.message(TRANSFER_LEVEL, "Mover called back.  elapsed=%s" %
                      (time.time() - tinfo['encp_start_time'],))

        Trace.message(TICKET_LEVEL, "WORK TICKET:")
        Trace.message(TICKET_LEVEL, pprint.pformat(work_ticket))

        #maybe this isn't a good idea...
        #work_ticket = combine_dict(ticket, work_ticket)

        #Open the local file for reading.
        done_ticket = open_local_file(work_ticket, tinfo, e)

        #By adding the pnfs_filename check, we will know if another process
        # has modified this file.
        result_dict = handle_retries([work_ticket], work_ticket,
                                     done_ticket, e,
                                     pnfs_filename = work_ticket['outfile'])

        if e_errors.is_retriable(result_dict['status'][0]):
            close_descriptors(control_socket, data_path_socket)
            return combine_dict(result_dict, work_ticket)
        elif e_errors.is_non_retriable(result_dict['status'][0]):
            close_descriptors(control_socket, data_path_socket)
            return combine_dict(result_dict, work_ticket)
        else:
            in_fd = done_ticket['fd']

        Trace.message(TRANSFER_LEVEL, "Input file %s opened.   elapsed=%s" %
                      (work_ticket['infile'],
                       time.time()-tinfo['encp_start_time']))

        #We need to stall the transfer until the mover is ready.
        done_ticket = stall_write_transfer(data_path_socket, control_socket, e)

        if not e_errors.is_ok(done_ticket):
            #Make one last check of everything before entering transfer_file().
            # Only test control_socket if a known problem exists.  Otherwise,
            # for small files it is possible that a successful final dialog
            # message gets 'eaten' up.
            external_label = work_ticket.get('fc',{}).get('external_label',
                                                          None)
            result_dict = handle_retries([work_ticket], work_ticket,
                                         done_ticket, e,
                                         #listen_socket = listen_socket,
                                         #udp_serv = route_server,
                                         control_socket = control_socket,
                                         external_label = external_label)

            if e_errors.is_retriable(result_dict['status'][0]):
                close_descriptors(control_socket, data_path_socket, in_fd)
                return combine_dict(result_dict, work_ticket)
            elif e_errors.is_non_retriable(result_dict['status'][0]):
                close_descriptors(control_socket, data_path_socket, in_fd)
                return combine_dict(result_dict, work_ticket)

        lap_time = time.time() #------------------------------------------Start

        done_ticket = transfer_file(in_fd, data_path_socket,
                                    control_socket, work_ticket,
                                    tinfo, e, udp_serv = udp_serv)

        tstring = '%s_transfer_time' % work_ticket['unique_id']
        tinfo[tstring] = time.time() - lap_time #--------------------------End

        try:
            delete_at_exit.register_bfid(done_ticket['fc']['bfid'])
        except (IndexError, KeyError):
            pass
            #Trace.log(e_errors.WARNING, "unable to register bfid")

        Trace.message(TRANSFER_LEVEL, "Verifying %s transfer.  elapsed=%s" %
                      (work_ticket['outfilepath'],
                       time.time()-tinfo['encp_start_time']))

        #Don't need these anymore.
        #close_descriptors(control_socket, data_path_socket, in_fd)
        close_descriptors(in_fd)

        #Verify that everything is ok on the mover side of the transfer.
        result_dict = handle_retries([work_ticket], work_ticket,
                                     done_ticket, e)

        if e_errors.is_retriable(result_dict):
            return combine_dict(result_dict, work_ticket)
        elif e_errors.is_non_retriable(result_dict):
            return combine_dict(result_dict, work_ticket)

        #Make sure the exfer sub-ticket gets stored into request_ticket.
        work_ticket = combine_dict(done_ticket, work_ticket)

        try:
            #This function write errors/warnings to the log file and put an
            # error status in the ticket.
            done_ticket = write_post_transfer_update(done_ticket, e)
        except (SystemExit, KeyboardInterrupt):
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except:
            Trace.handle_error()
            done_ticket['status'] = (e_errors.UNKNOWN, sys.exc_info()[1])

        result_dict = handle_retries([work_ticket], work_ticket,
                                     done_ticket, e)

        if e_errors.is_non_retriable(done_ticket.get('status', (e_errors.OK,None))):
            return combine_dict(result_dict, work_ticket)
        if not e_errors.is_ok(done_ticket.get('status', (e_errors.OK, None))):
            return combine_dict(result_dict, work_ticket)

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

        # calculate some kind of rate - time from beginning
        # to wait for mover to respond until now. This doesn't
        # include the overheads before this, so it isn't a
        # correct rate. I'm assuming that the overheads I've
        # neglected are small so the quoted rate is close
        # to the right one.  In any event, I calculate an
        # overall rate at the end of all transfers.
        calculate_rate(done_ticket, tinfo)

        Trace.message(TRANSFER_LEVEL,
                      "File status after verification: %s   elapsed=%s" %
                      (done_ticket['status'],
                       time.time()-tinfo['encp_start_time']))

        return done_ticket

############################################################################

def prepare_write_to_hsm(tinfo, e):
    done_ticket, listen_socket, callback_addr, \
                 udp_serv, udp_callback_addr = get_callback_addresses(e)
    if not e_errors.is_ok(done_ticket):
        return done_ticket, listen_socket, udp_serv, None

    #Build the dictionary, work_ticket, that will be sent to the
    # library manager.
    try:
        request_list = create_write_requests(callback_addr, None, e, tinfo)
    except (SystemExit, KeyboardInterrupt):
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
    except (OSError, IOError, AttributeError, ValueError, EncpError,
            socket.error, select.error), msg:
        Trace.handle_error(severity=99)
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
        elif isinstance(msg, socket.error) or isinstance(msg, select.error):
            #On 11-12-2009, tracebacks were found from migration encps that
            # started failing because there were too many open files while
            # trying to instantiate client classes.  The socket.error should
            # have been caught here.  So now it is.
            e_ticket = {'status' : (e_errors.NET_ERROR, str(msg))}
        else:
            #ValueError???
            e_ticket = {'status' : (e_errors.WRONGPARAMETER, str(msg))}

        return e_ticket, listen_socket, udp_serv, []
    except:
        e_ticket = {'status' : (e_errors.UNKNOWN,
                                "%s: %s" % (str(sys.exc_info()[0]),
                                            str(sys.exc_info()[1])))}
        try:
            Trace.handle_error()
        except (SystemExit, KeyboardInterrupt):
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except:
            #We'll go with the original error here.
            pass

        return e_ticket, listen_socket, udp_serv, []

    #If this is the case, don't worry about anything.
    if len(request_list) == 0:
        done_ticket = {'status' : (e_errors.NO_FILES, "No files to transfer.")}
        return done_ticket, listen_socket, udp_serv, request_list

    #This will halt the program if everything isn't consistant.
    try:
        #Skip this test for volume transfers and migration/duplication
        # transfers for performane reasons.
        if not e.volume and not e.migration_or_duplication:
            verify_write_request_consistancy(request_list, e)
    except EncpError, msg:
        msg.ticket['status'] = (msg.type, msg.strerror)
        return msg.ticket, listen_socket, udp_serv, request_list
    except (socket.error, select.error), msg:
        #On 11-12-2009, tracebacks were found from migration encps that
        # started failing because there were too many open files while
        # trying to instantiate client classes.  The socket.error should
        # have been caught here.  So now it is.
        return {'status' : (e_errors.NET_ERROR, str(msg))}, listen_socket, \
               udp_serv, request_list

    #If we are only going to check if we can succeed, then the last
    # thing to do is see if the LM is up and accepting requests.
    if e.check:
        #Determine the name of the library.
        check_lib = request_list[0]['vc']['library'] + ".library_manager"
        try:
            return check_library(check_lib, e), listen_socket, \
                   udp_serv, request_list
        except EncpError, msg:
            return_ticket = { 'status'      : (msg.type, str(msg)),
                              'exit_status' : 2 }
            return return_ticket, listen_socket, udp_serv, request_list

    #Create the zero length file entries.
    message = "Creating zero length output files."
    Trace.message(TRANSFER_LEVEL, message)
    Trace.log(e_errors.INFO, message)
    for i in range(len(request_list)):
        if e.put_cache or request_list[i].get('copy', None) != None:
            # We still need to get the inode.
            try:
                #Yet another os.stat() call.  In the future, need to work on
                # getting rid of as many of these as possible.
                pstat = get_stat(request_list[i]['outfile'])
                request_list[i]['wrapper']['inode'] = long(pstat[stat.ST_INO])
            except OSError:
                request_list[i]['wrapper']['inode'] = None

            #Don't forget the pnfsid.  New disk movers depend on this value.
            if e.put_cache:
                request_list[i]['fc']['pnfsid'] = e.put_cache
            else:
                original_ticket = get_original_request(request_list, i)
                request_list[i]['fc']['pnfsid'] = original_ticket['fc']['pnfsid']
        else:
            #Create the zero length file entry and grab the inode.
            try:
                create_zero_length_pnfs_files(request_list[i], e)
            except (OSError, IOError, EncpError), msg:
                if msg.args[0] == getattr(errno, str("EFSCORRUPTED"), None) \
                       or (msg.args[0] == errno.EIO and \
                           msg.args[1].find("corrupt") != -1):
                    request_list[i]['status'] = \
                                   (e_errors.FILESYSTEM_CORRUPT, str(msg))
                else:
                    request_list[i]['status'] = \
                                   (e_errors.OSERROR, str(msg))
                return request_list[i], listen_socket, udp_serv, request_list
            except: #Un-anticipated errors.
                raise sys.exc_info()[0], sys.exc_info()[1],  sys.exc_info()[2]

    return_ticket = { 'status' : (e_errors.OK, None)}
    return return_ticket, listen_socket, udp_serv, request_list

def write_to_hsm(e, tinfo):
    global err_msg

    Trace.trace(16,"write_to_hsm input_files=%s  output=%s  verbose=%s  "
                "chk_crc=%s t0=%s" %
                (e.input, e.output, e.verbose, e.chk_crc,
                 tinfo['encp_start_time']))

    # initialize
    byte_sum = 0L #Sum of bytes transfered (when transfering multiple files).
    exit_status = 0 #Used to determine the final message text.

    done_ticket, listen_socket, unused, request_list = \
                 prepare_write_to_hsm(tinfo, e)
    if not e_errors.is_ok(done_ticket) or e.check:
        return done_ticket

    #If USE_NEW_EVENT_LOOP is true, we need this cleared.
    transaction_id_list = []

    # loop on all input files sequentially
    while requests_outstanding(request_list):

        #Report how many files are still to go.
        Trace.message(TO_GO_LEVEL,
                      "FILES LEFT: %s" % requests_outstanding(request_list))

        work_ticket, index, copy = get_next_request(request_list)

        # Check if this is a multiple copy request
        if copy != 0:
            # This is a copy request.
            # We do not make copies for files written to cache.
            # Copies are done when these files migrate to tape.
            if e.enable_redirection == 1 and e.redirected:
                # The original request (copy 0) was redirected.
                # Skip the copy request.
                del(request_list[index])
                work_ticket['status'] = (e_errors.OK, None)
                return work_ticket

        #Send the request to write the file to the library manager.
        done_ticket, lmc = submit_write_request(work_ticket, e)

        work_ticket = combine_dict(done_ticket, work_ticket)
        #handle_retries() is not required here since submit_write_request()
        # handles its own retrying when an error occurs.
        if not e_errors.is_ok(work_ticket):
            return work_ticket

        #We could just set original_ticket to whatever get_original_request()
        # returns.  It is None in the same cases as the else clause.
        # However, this short cuircuts looping over the entire list of files.
        if work_ticket.get('copy', None):
            original_ticket = get_original_request(request_list, index)
        else:
            original_ticket = None

        while requests_outstanding([work_ticket, original_ticket]):

            #Wait for all possible messages or connections.
            request_ticket, control_socket, data_path_socket = \
                                    wait_for_message(listen_socket, lmc,
                                                     [work_ticket],
                                                     transaction_id_list, e)

            if control_socket:
                #If we connected with the mover, add these two handle_retries()
                # checks.
                local_filename = request_ticket.get(
                    'wrapper', {}).get('fullname', None)
                external_label = request_ticket.get(
                    'fc', {}).get('external_label', None)
            else:
                # Skip these two handle_retries() checks if we only heard from
                # the library manager.
                local_filename = None
                external_label = None

            result_dict = handle_retries([work_ticket], work_ticket,
                                         request_ticket, e,
                                         listen_socket = listen_socket,
                                         local_filename = local_filename,
                                         external_label = external_label)


            #If USE_NEW_EVENT_LOOP is true, we need these ids.
            transaction_id_list = result_dict.get('transaction_id_list', [])

            #Make sure done_ticket exists by this point.
            #
            #For LM submission errors (i.e. tape went NOACCESS), use
            # any request information in result_dict to identify which
            # request gave an error.
            #
            #Writes included work_ticket in this combine.  That doesn't
            # work for reads because we don't know which read request
            # will get picked first.
            #
            #It is important to note that the write version works because,
            # work_ticket is passed as the work ticket to handle_retries()
            # and has the unique_id field updated on errors.  Reads pass
            # the request_ticket as both the error ticket and work ticket,
            # thus request_ticket has this info already when an error
            # happens.
            done_ticket = combine_dict(result_dict, work_ticket, request_ticket)

            if e_errors.is_non_retriable(result_dict):

                #Regardless if index is None or not, make sure that
                # exit_status gets set to failure.
                if e.migration_or_duplication:
                    #Handle migration cases a little differently.

                    # Make sure the migration knows not to try this
                    # file again.
                    exit_status = 2
                else:
                    exit_status = 1
                # Make sure the correct error is reported for migration.
                # If we don't set this here, then we end up with the
                # non-useful ("OK", "Error after transfering... )
                # error messages from calculate_final_statistics.
                err_msg[thread.get_ident()] = str(result_dict['status'])

                if index == None:
                    message = "Unknown transfer failed."
                    try:
                        sys.stderr.write(message + "\n")
                        sys.stderr.flush()
                    except IOError:
                        pass
                    Trace.log(e_errors.ERROR,
                              message + "  " + str(done_ticket))

                else:
                    #Combine the dictionaries.
                    work_ticket = combine_dict(done_ticket,
                                               request_list[index])
                    #Set completion status to failed.
                    work_ticket['completion_status'] = FAILURE
                    #Set the exit status value.
                    work_ticket['exit_status'] = exit_status
                    #Store these changes back into the master list.
                    request_list[index] = work_ticket
                    #Make sure done_ticket points to the new info too.
                    done_ticket = work_ticket

                return done_ticket

            if not e_errors.is_ok(result_dict):
                #Combine the dictionaries.
                work_ticket = combine_dict(done_ticket, request_list[index])
                #Store these changes back into the master list.
                request_list[index] = work_ticket
                continue

            if not control_socket:
                #We only got a response from the LM, we did not connect
                # with the mover yet.
                continue

            work_ticket = combine_dict(request_ticket, work_ticket)

            ############################################################
            #In this function call is where most of the work in transfering
            # a single file is done.
            #
            #Send (write) the file to the mover.
            done_ticket = write_hsm_file(work_ticket, control_socket,
                                         data_path_socket, tinfo, e)
            ############################################################

            # Close these descriptors before they are forgotten about.
            close_descriptors(control_socket, data_path_socket)

            #Set the value of bytes to the number of bytes transfered before
            # the error occured.
            exfer_ticket = done_ticket.get('exfer', {'bytes_transfered' : 0L})
            byte_sum = byte_sum + exfer_ticket.get('bytes_transfered', 0L)

            #Store the combined tickets back into the master list.
            work_ticket = combine_dict(done_ticket, work_ticket)
            request_list[index] = work_ticket

            #The completion_status is modified in the request ticket.
            # what_to_do = 0 for stop
            #            = 1 for continue
            #            = 2 for continue after retry
            what_to_do = finish_request(work_ticket, request_list, index, e)

            #If on non-success exit status was returned from
            # finish_request(), keep it around for later.
            if request_ticket.get('exit_status', None):
                #We get here only on an error.  If the value is 1, then
                # the error should be transient.  If the value is 2, then
                # the error will likely require human intervention to
                # resolve.
                exit_status = request_ticket['exit_status']
            # Do what finish_request() says to do.
            if what_to_do == STOP:
                #We get here only on a non-retriable error.
                if not exit_status:
                    #Just in case this got missed somehow.
                    exit_status = 1
                    break
            elif what_to_do == CONTINUE_FROM_BEGINNING:
                #We get here only on a retriable error.
                continue

    # we are done transferring - close out the listen socket
    close_descriptors(listen_socket)

    #Print to screen the exit status.
    Trace.message(TO_GO_LEVEL, "EXIT STATUS: %d" % exit_status)

    #Finishing up with a few of these things.
    calc_ticket = calculate_final_statistics(byte_sum, len(request_list),
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

    return list_done_ticket  #, request_list

#######################################################################
#Support function for reads.
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

#Args:
# Takes in a dictionary of lists of transfer requests sorted by volume.
#Rerturns:
# None
#Verifies that various information in the tickets are correct, valid, spelled
# correctly, etc.
def verify_read_request_consistancy(requests_per_vol, e):

    verify_read_request_consistancy_start_time = time.time()

    bfid_brand = None
    sum_size = 0L
    sum_files = 0L
    outputfile_dict = {}
    #p = namespace.StorageFS()

    #Get the bfid brand for the first file.  This will be compared with
    # the brand of each file to make sure they all belong to one Enstore
    # system.
    try:
        vol = requests_per_vol.keys()[0]
        bfid_brand = bfid_util.extract_brand(
            requests_per_vol[vol][0]['fc']['bfid'])
    except (ValueError, AttributeError, TypeError,
            IndexError, KeyError), msg:
        msg = "Error insuring consistancy with request list for " \
              "volume %s." % (vol,)
        status = (e_errors.CONFLICT, msg)
        raise EncpError(None, str(msg), e_errors.CONFLICT,
                        {'status':status})


    outfile_name = requests_per_vol[vol][0]['outfile']
    if outfile_name not in \
           ["/dev/null", "/dev/zero", "/dev/random", "/dev/urandom"]:
        #Obtain the maximum amount of free space remaining on the file_system.
        # Do this once for all files, instead of once per file for performance
        # reasons.
        fs_stats = os.statvfs(get_directory_name(outfile_name))
        bytes_free = long(fs_stats[statvfs.F_BAVAIL]) * \
                     long(fs_stats[statvfs.F_FRSIZE])

        #Obtain the quota limits for the target directory.
        fs_quotas = EXfer.quotas(outfile_name)

    vols = requests_per_vol.keys()
    vols.sort()
    for vol in vols:
        request_list = requests_per_vol[vol]

        for request in request_list:

            if request['infile'] not in ["/dev/zero",
                                         "/dev/random", "/dev/urandom"]:

                #In case pnfs is automounted, first try this access_check()
                # call to wait for the filesystem to mount.
                if e.pnfs_is_automounted:
                    access_check(request['infile'], os.F_OK)

                try:
                    inputfile_check(request, e)
                except IOError, msg:
                    Trace.handle_error(severity=99)
                    raise EncpError(msg.args, str(msg), e_errors.IOERROR,
                                    {'infilepath' : request['infilepath'],
                                     'outfilepath' : request['outfilepath']})
                except OSError, msg:
                    Trace.handle_error(severity=99)
                    raise EncpError(msg.args, str(msg), e_errors.OSERROR,
                                    {'infilepath' : request['infilepath'],
                                     'outfilepath' : request['outfilepath']})
                except (socket.error, select.error), msg:
                    Trace.handle_error(severity=99)
                    #On 11-12-2009, tracebacks were found from migration encps
                    # that started failing because there were too many open
                    # files while trying to instantiate client classes.  The
                    # socket.error should have been caught here.  So now it is.
                    raise EncpError(msg.ars, str(msg), e_errors.NET_ERROR,
                                    {'infilepath' : request['infilepath'],
                                     'outfilepath' : request['outfilepath']})

                try:
                    inputfile_check_pnfs(request, bfid_brand, e)
                except IOError, msg:
                    Trace.handle_error(severity=99)
                    raise EncpError(msg.args, str(msg), e_errors.IOERROR,
                                    {'infilepath' : request['infilepath'],
                                     'outfilepath' : request['outfilepath']})
                except OSError, msg:
                    Trace.handle_error(severity=99)
                    raise EncpError(msg.args, str(msg), e_errors.OSERROR,
                                    {'infilepath' : request['infilepath'],
                                     'outfilepath' : request['outfilepath']})
                except (socket.error, select.error), msg:
                    Trace.handle_error(severity=99)
                    #On 11-12-2009, tracebacks were found from migration encps
                    # that started failing because there were too many open
                    # files while trying to instantiate client classes.  The
                    # socket.error should have been caught here.  So now it is.
                    raise EncpError(msg.ars, str(msg), e_errors.NET_ERROR,
                                    {'infilepath' : request['infilepath'],
                                     'outfilepath' : request['outfilepath']})

            if request['outfile'] not in ["/dev/null", "/dev/zero",
                                          "/dev/random", "/dev/urandom"]:
                if not request.get('local_inode', None):
                    try:
                        #outputfile_check(request['infile'],
                        #                 request['outfile'], e)
                        outputfile_check(request, e)
                    except IOError, msg:
                        Trace.handle_error(severity=99)
                        raise EncpError(msg.args, str(msg), e_errors.IOERROR,
                                        {'infilepath' : request['infilepath'],
                                         'outfilepath' : request['outfilepath']})
                    except OSError, msg:
                        Trace.handle_error(severity=99)
                        raise EncpError(msg.args, str(msg), e_errors.OSERROR,
                                        {'infilepath' : request['infilepath'],
                                         'outfilepath' : request['outfilepath']})
                    except (socket.error, select.error), msg:
                        Trace.handle_error(severity=99)
                        #On 11-12-2009, tracebacks were found from migration
                        # encps that started failing because there were too
                        # many open files while trying to instantiate client
                        # classes.  The socket.error should have been caught
                        # here.  So now it is.
                        raise EncpError(msg.ars, str(msg), e_errors.NET_ERROR,
                                        {'infilepath' : request['infilepath'],
                                         'outfilepath' : request['outfilepath']})

                #This block of code makes sure the the user is not moving
                # two files with the same basename in different directories
                # into the same destination directory.
                result = outputfile_dict.get(request['outfilepath'], None)
                if result:
                    #If the file is already in the list, give error.
                    raise EncpError(None,
                                    'Duplicate file entry: %s' % (result,),
                                    e_errors.USERERROR,
                                    {'infilepath' : request['infilepath'],
                                     'outfilepath' : request['outfilepath']})
                else:
                    #Put into one place all of the output names.  This is to
                    # check that two file to not have the same output name.
                    outputfile_dict[request['outfilepath']] = \
                                                    request['infilepath']

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

            #sum up the size to verify there is sufficent disk space.
            try:
                sum_size = sum_size + request['file_size']
            except TypeError:
                pass #If the size is not known (aka None) move on.

            #sum up the file count to verify there is sufficent disk space.
            sum_files = sum_files + 1L

        if request['outfile'] not in ["/dev/null", "/dev/zero",
                                      "/dev/random", "/dev/urandom"]:

            ## We need to determine if transfering the files will fail
            ## because of insufficent space.

            #Test if the disk would become full while transfering the file.
            if bytes_free < sum_size:
                message = \
                     "Disk is full.  %d bytes available for %d requested." % \
                     (bytes_free, sum_size)
                raise EncpError(None, str(message),
                                e_errors.USERERROR, request)

            #Make sure we won't exeed any quotas.
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
                    try:
                        sys.stderr.write(message + "\n")
                        sys.stderr.flush()
                    except IOError:
                        pass

                #Test if we are near file count quota.
                if quota[EXfer.FILE_SOFT_LIMIT] \
                       and quota[EXfer.CURRENT_FILES] + sum_files > \
                       quota[EXfer.FILE_SOFT_LIMIT]:
                    message = "WARNING: Transfer will exeed soft quota limit."
                    try:
                        sys.stderr.write(message + "\n")
                        sys.stderr.flush()
                    except IOError:
                        pass

    message = "[1] Time to verify read request consistancy: %s sec." % \
              (time.time() - verify_read_request_consistancy_start_time,)
    Trace.message(TIME_LEVEL, message)
    Trace.log(TIME_LEVEL, message)

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

    if vc_ticket == None:
        if type(volume_or_ticket) == types.StringType:
            volume_name = volume_or_ticket
        elif  type(volume_or_ticket) == types.DictType:
            volume_name = volume_or_ticket.get('volume', "Unknown")
        else:
            volume_name = "UNKNOWN"
        raise EncpError(None, "Unable to obtain volume information for %s" %
                        volume_name, e_errors.NOVOLUME)
    if not e_errors.is_ok(vc_ticket):
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
        vc_ticket = get_volume_clerk_info(fc_ticket, encp_intf=e)
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

############################################################################
#Functions for reads.
############################################################################

def create_read_requests(callback_addr, udp_callback_addr, tinfo, e):

    create_read_requests_start_time = time.time()

    nfiles = 0
    requests_per_vol = {}
    #csc = get_csc() #Get csc once for max_attempts().
    fcc = None
    tape_ticket = None #Only used if e.volume is set.
    vc_reply = None

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
        try:
            sys.stderr.write("%s %s\n" % e.output, type(e.output))
            sys.stderr.flush()
        except IOError:
            pass
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
                                 and not file_utils.wrapper(os.path.isdir,
                                                            (e.output[0],)):
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
        except (select.error, socket.error, e_errors.EnstoreError), msg:
            if msg.errno == errno.ETIMEDOUT:
                tape_ticket = {'status' : (e_errors.TIMEDOUT, "file_clerk")}
            else:
                tape_ticket = {'status' : (e_errors.NET_ERROR, str(msg))}
        except e_errors.TCP_EXCEPTION:
            tape_ticket = {'status' : (e_errors.NET_ERROR,
                                       e_errors.TCP_EXCEPTION)}

        #First check for errors.
        if not e_errors.is_ok(tape_ticket):
            rest = {'volume':e.volume}
            status = tape_ticket.get('status', (e_errors.BROKEN, "failed"))
            message = "Error obtaining tape listing: %s" % status[1]
            if e.check:
                rest['exit_status'] = 2
            raise EncpError(None, message, status[0], rest)

        #Shortcut for accessability.
        tape_list = tape_ticket.get("tape_list", [])

        #Set these here.  ("Get" with --list.)
        if hasattr(e, "get") and e.list:
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
            list_of_files = tape_ticket['tape_list']
            #Always say one (for the ticket to send to the LM) when the
            # number of files is unknown.
            if number_of_files == 0:
                number_of_files = 1
    else: # Normal read, --get-dcache, --put-cache, --get-bfid.
        number_of_files = len(e.input)
        list_of_files = e.input

    # check the input unix file. if files don't exits, we bomb out to the user
    for i in range(number_of_files):

        #We need to put the minimal items into this dictionary to pass
        # to create_read_request() that will fill in the rest.
        request = {}
        if e.volume and hasattr(e, "get") and e.list:
            number, filename = list_of_files[i].split()[:2]
            number = int(number)
            request['infile'] = os.path.join(e.input[0],
                                             os.path.basename(filename))
            request['vc'] = vc_reply.copy()
            #If everything is okay, search the listing for the location
            # of the file requested.  tape_ticket is used for performance
            # reasons over fcc.bfid_info().
            for i in range(len(tape_list)):
                #For each file number on the tape, compare it with
                # a location cookie in the list of tapes.
                if number == \
                   enstore_functions3.extract_file_number(
                    tape_list[i]['location_cookie']):
                    #Make a copy so the following "del tape_list[i]" will
                    # not cause reference problems.
                    request['fc'] = tape_list[i].copy()
                    #Include the server address in the returned info.
                    # Normally, get_file_clerk_info() would do this for us,
                    # but since we are using tape_ticket instead (for
                    # performance reasons) we need to set the address
                    # explicitly.
                    request['fc']['address'] = fcc.server_address
                    #Shrink the tape_ticket list for performance reasons.
                    del tape_list[i]
                    break
            else:
                request['fc'] = {
                    'address' : fcc.server_address,
                    'bfid' : None,
                    'complete_crc' : None,
                    'deleted' : None,
                    'drive' : None,
                    'external_label' : e.volume,
                    'location_cookie':generate_location_cookie(number),
                    'pnfs_name0':os.path.join(e.input[0],
                                              os.path.basename(filename)),
                    'pnfsid': None,
                    'sanity_cookie': None,
                    'size': None,
                    'status' : (e_errors.OK, None)
                    }
            use_infile = request['fc']['pnfs_name0']  #used for error reporting
        elif e.volume:
            try:
                request['fc'] = tape_ticket['tape_list'][i].copy()
            except IndexError:
                if i == 0 and number_of_files == 1:
                    number = 1
                else:
                    number = i
                location = generate_location_cookie(number)
                request['fc'] = {
                    'address' : fcc.server_address,
                    'bfid' : None,
                    'complete_crc' : None,
                    'deleted' : None,
                    'drive' : None,
                    'external_label' : e.volume,
                    'location_cookie':location,
                    'pnfs_name0':os.path.join(e.input[0], location),
                    'pnfsid': None,
                    'sanity_cookie': None,
                    'size': None,
                    'status' : (e_errors.OK, None)
                    }
            request['fc']['address'] = fcc.server_address
            request['vc'] = vc_reply.copy()
            use_infile = request['fc']['pnfs_name0']  #used for error reporting
        elif e.get_cache:
            request['fc'] = {}
            request['fc']['pnfsid'] = e.get_cache
            use_infile = e.get_cache  #used for error reporting
        elif e.get_bfid:
            request['bfid'] = e.get_bfid
            request['fc'] = {}
            request['fc']['bfid'] = e.get_bfid
            use_infile = e.get_bfid  #used for error reporting
        elif e.get_bfids:
            request['bfid'] = list_of_files[i]
            request['fc'] = {}
            request['fc']['bfid'] = list_of_files[i]
            use_infile = list_of_files[i]  #used for error reporting
        else:
            request['infile'] = list_of_files[i]
            use_infile = request['infile']  #used for error reporting

        try:
            #Moved this inside the loop to pass in the filename to Pnfs/Chimera
            request = create_read_request(request, i, callback_addr,
                                          udp_callback_addr, tinfo, e)
        except (KeyboardInterrupt, SystemExit):
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except (OSError, IOError), msg:
            Trace.handle_error(severity=99)
            if isinstance(msg, OSError):
                e_type = e_errors.OSERROR
            else:
                e_type = e_errors.IOERROR

            raise EncpError(msg.args[0], use_infile, e_type, request)

        if request == None:
            #This is a rare possibility.  Can occur when --volume and
            # --skip-deleted-files are both used.
            continue

        label = request['volume']
        requests_per_vol[label] = requests_per_vol.get(label, []) + [request]
        nfiles = nfiles+1

        #When output is redirected to a file, sometimes it needs a push
        # to get there.
        sys.stdout.flush()
        sys.stderr.flush()

    message = "[1] Time to create read requests: %s sec." % \
              (time.time() - create_read_requests_start_time,)
    Trace.message(TIME_LEVEL, message)
    Trace.log(TIME_LEVEL, message)
    return requests_per_vol


def create_read_request(request, file_number,
                        callback_addr, udp_callback_addr, tinfo, e):

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
        # 7) p - namespace.StorageFS class instance
        # 8) bfid - The bfid of the transfer.

        #### VOLUME #######################################################
        #If the user specified a volume to read.
        if e.volume:


            #If these fail what could we do?
            fc_reply = request['fc']
            vc_reply = request['vc']

            #Get the file position (aka file number) of the current
            # file on the tape and the name it will have.
            #These two variables should only be used withing the e.volume
            # if statement.  After that their use would break usability
            # with the other read method type branches.
            if enstore_functions3.is_location_cookie_disk(
                fc_reply['location_cookie']):
                #file_number starts with 0.  To make the 1st file at location 1
                # we add 1 to the locations.
                number = file_number + 1
            else:
                number = int(enstore_functions3.extract_file_number(
                    fc_reply['location_cookie']))
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
                if getattr(e, "skip_deleted_files", None):
                    sys.stdout.write("Skipping deleted file (%s) %s.\n" %
                                     (number, filename))
                    #continue
                    return None
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
            sfsid = fc_reply.get('pnfsid', None)

            #By this point pnfs_name0 should always have a non-None value.
            # Assuming this would make coding easier, but there is the case
            # were set_sfs_settings() fails leaving the pnfs_name0 (and pnfsid)
            # field None while other values have been updated.

            ifullname = None
            if pnfs_name0 != None:
                sfs = namespace.StorageFS(pnfs_name0)
                if e.intype == RHSMFILE:
                    #Using pnfs_agent.
                    found_name = sfs.find_id_path(
                        sfsid, bfid, file_record = fc_reply,
                        likely_path = pnfs_name0)
                else:
                    #Using local mounted storage file system.
                    found_name = find_pnfs_file.find_id_path(
                        sfsid, bfid, file_record = fc_reply,
                        likely_path = pnfs_name0)

                stat_info = sfs.get_stat(found_name)
                if stat.S_ISREG(stat_info[stat.ST_MODE]):
                    ifullname = found_name
            else:
                #We'll, hopefully, try getting the StorageFS() class again
                # shortly.
                sfs = None

            if ifullname == None and sfsid != None:
                try:
                    #Using the original directory as a starting point, try
                    # and determine the new file name/path/location.
                    orignal_directory = get_directory_name(pnfs_name0)
                    #Try to obtain the file name and path that the
                    # file currently has.
                    if not sfs:
                        sfs = namespace.StorageFS(sfsid,
                                           mount_point=orignal_directory)
                    ifullname_list = sfs.get_path(sfsid, orignal_directory)
                    for cur_fname in ifullname_list:
                        if sfs.get_bit_file_id(cur_fname) == e.get_bfid:
                            ifullname = cur_fname
                            break
                    else:
                        EncpError(errno.ENOENT,
                                  "Unable to find correct PNFS file.",
                                  e_errors.PNFS_ERROR,
                                  {'infilepath' : ifullname_list})
                except (OSError, KeyError, AttributeError, ValueError):
                    sys.stdout.write("Location %s is active, but the "
                                     "file has been deleted.\n" % lc)

                    #Determine the inupt filename.
                    ifullname = pnfs_name0


            #If ifullname is still None, then the file does not exist
            # anywhere.  Use the correct name.
            if ifullname == None:
                ifullname = os.path.join(e.input[0], filename)

            #Determine the access path name.
            if not sfs:
                sfs = namespace.StorageFS(ifullname)
            iaccessname = sfs.access_file(get_directory_name(ifullname),
                                          sfsid)

            #Grab the stat info.
            istatinfo = sfs.get_stat(iaccessname)

            #This is an attempt to deal with data that is incomplete
            # from a failed previous transfer.
            if fc_reply.get('pnfs_name0', None) == None:
                fc_reply['pnfs_name0'] = ifullname

            #Determine the output filename.
            if e.output[0] in ["/dev/null", "/dev/null",
                               "/dev/random", "/dev/urandom"]:
                ofullname = e.output[0]
            elif getattr(e, "sequential_filenames", None):
                #The user overrode "get" to use numbered filenames.
                ofullname = os.path.join(e.output[0], "%s:%s" (e.volume, lc))
            else:
                ofullname = os.path.join(e.output[0], filename)

            read_work = 'read_from_hsm' #'read_tape'

        #### BFID #######################################################
        #If the user specified a bfid for the file to read.
        elif e.get_bfid or e.get_bfids:

            #Get the system information from the clerks.  In this case
            # e.input[i] doesn't contain the filename, but the bfid.

            vc_reply, fc_reply = get_clerks_info(request['bfid'], e)

            sfsid = fc_reply.get("pnfsid", None)
            if not sfsid:
                #In the case that this is an overridding read of a deleted
                # file don't give the error.
                if not (e.override_deleted
                        and fc_reply['deleted'] != 'no'):
                    raise EncpError(None,
                                    "Unable to obtain sfs id from file clerk.",
                                    e_errors.CONFLICT,
                                    {'fc' : fc_reply, 'vc' : vc_reply})

            if e.shortcut and e.override_path:
                #If the user specified a pathname (with --override-path)
                # on the command line use that name.  Otherwise if just
                # --shortcut is used, the filename name will be recored as
                # a /.(access)(<PNFS/Chimera_id>) style filename.
                ifullname = e.override_path
                use_dir = get_directory_name(ifullname)
                sfs = None
            elif e.override_deleted and fc_reply['deleted'] != "no":
                #Handle reading deleted files differently.
                ifullname = fc_reply['pnfs_name0']
                use_dir = ""
                sfs = None
            elif e.skip_pnfs:
                # When told to skip PNFS, we should avoid all PNFS information.
                ifullname = fc_reply['pnfs_name0']
                use_dir = ""
                sfs = None
            else:
                if e.pnfs_mount_point:
                    use_mount_point = e.pnfs_mount_point
                else:
                    use_mount_point = os.path.dirname(fc_reply['pnfs_name0'])
                try:
                    sfs = namespace.StorageFS(sfsid,
                                            mount_point=use_mount_point,
                                            shortcut = e.shortcut)
                    ifullname_list = sfs.get_path(sfsid, use_mount_point,
                                                shortcut = e.shortcut)
                except OSError, msg:
                    ifullname_list = getattr(msg, "filename", [])
                    if msg.args[0] not in [errno.ENODEV] \
                           or type(ifullname_list) != types.ListType \
                           or len(ifullname_list) <= 1:
                        #We did not find to many matching files to the
                        # sfsid.  Pass the error back up.
                        raise EncpError(msg.args[0],
                                        msg.args[1],
                                        e_errors.OSERROR,
                                        {'infilepath' : ifullname_list})

                #If we did find too many matching files to the sfsid,
                # we need to check the file bfids in layer 1 to determine
                # which one we are looking for.
                for cur_fname in ifullname_list:
                    if sfs.get_bit_file_id(cur_fname) == request['bfid']:
                        ifullname = cur_fname
                        use_dir = get_directory_name(ifullname)
                        break
                    elif get_fcc().find_original(request['bfid']).get("original",
                                                                 None) == \
                                                sfs.get_bit_file_id(cur_fname):
                        ifullname = cur_fname
                        use_dir = get_directory_name(ifullname)
                        break
                else:
                    raise EncpError(errno.ENOENT,
                                    "Unable to find correct PNFS file.",
                                    e_errors.PNFS_ERROR,
                                    {'infilepath' : ifullname_list})

            if e.override_deleted and fc_reply['deleted'] != 'no':
                iaccessname = sfsid  #Something, anything.
            elif e.skip_pnfs:
                iaccessname = sfsid  #Something, anything.
            else:
                if not sfs:
                    #All the cases that get here are ones where we are not
                    # supposed to worry, much, about what is in the storage
                    # file system.  (--shortcut without --override-path)
                    sfs = namespace.StorageFS(sfsid, shortcut=e.shortcut,
                                              mount_point=e.pnfs_mount_point)
                #Determine the access path name.
                iaccessname = sfs.access_file(use_dir, sfsid)

            if e.output[0] in ["/dev/null", "/dev/zero",
                               "/dev/random", "/dev/urandom"]:
                ofullname = e.output[0]
            elif getattr(e, "sequential_filenames", None):
                #The user overrode "get" to use numbered filenames.
                ofullname = os.path.join(e.output[0],
                                         "%s:%s" % (fc_reply['external_label'],
                                                 fc_reply['location_cookie']))
            else:
                unused, ofullname, unused, unused = fullpath(e.output[0])

            #Grab the stat info.
            if e.override_deleted and fc_reply['deleted'] != 'no':
                # This protects us in case we are reading
                # a deleted file using the bfid.
                if ifullname and sfs:
                    try:
                        istatinfo = sfs.get_stat(iaccessname)
                    except (OSError, IOError), msg:
                        if msg.args[0] == errno.ENOENT:
                            istatinfo = None
                        else:
                            raise sys.exc_info()[0], sys.exc_info()[1], \
                                  sys.exc_info()[2]

                else:
                    istatinfo = None
            elif e.skip_pnfs:
                istatinfo = None
            else:
                istatinfo = sfs.get_stat(iaccessname)

            bfid = request['bfid']

            read_work = 'read_from_hsm'

        #### PNFS ID ###################################################
        elif e.get_cache:

            if e.shortcut and e.override_path:
                #If the user specified a pathname (with --override-path)
                # on the command line use that name.  Otherwise if just
                # --shortcut is used, the filename name will be recored as
                # a /.(access)(<PNFS/Chimera_id>) style filename.
                ifullname = e.override_path

                #We still need to get a StorageFS object.
                use_mount_point = os.path.dirname(e.override_path)
                sfs = namespace.StorageFS(e.get_cache,
                                          mount_point=use_mount_point)
            else:
                sfs = namespace.StorageFS(e.get_cache,
                                   mount_point=e.pnfs_mount_point,
                                   shortcut=e.shortcut)
                ifullname_list = sfs.get_path(e.get_cache, e.pnfs_mount_point,
                                            shortcut = e.shortcut)
                if len(ifullname_list) == 1:
                    ifullname = ifullname_list[0]
                else:
                    raise EncpError(errno.ENOENT,
                                    "Unable to find correct PNFS file.",
                                    e_errors.PNFS_ERROR,
                                    {'infilepath' : ifullname_list})

            #Determine the access path name.
            iaccessname = sfs.access_file(get_directory_name(ifullname),
                                          e.get_cache)

            #Grab the stat info.
            istatinfo = sfs.get_stat(iaccessname)

            bfid = sfs.get_bit_file_id(iaccessname)

            vc_reply, fc_reply = get_clerks_info(bfid, e)

            if (not e.shortcut or not e.override_path) and \
                   vc_reply['media_type'] == "null":
                #We need to get the full path anyway to keep the mover from
                # failing the transfer because NULL is not in the path.
                ifullname_list = sfs.get_path(e.get_cache, e.pnfs_mount_point,
                                              shortcut = False)
                if len(ifullname_list) == 1:
                    ifullname = ifullname_list[0]
                else:
                    raise EncpError(errno.ENOENT,
                                    "Unable to find correct PNFS file.",
                                    e_errors.PNFS_ERROR,
                                    {'infilepath' : ifullname_list})

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
            ifullname = None
            try:
                sfs = namespace.StorageFS(request['infile'])
                istatinfo = sfs.get_stat(request['infile'])
                ifullname = request['infile']
            except (KeyboardInterrupt, SystemExit):
                raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
            except (OSError, IOError), msg:
                ifullname = get_ininfo(request['infile'])
                sfs = namespace.StorageFS(ifullname)
                istatinfo = sfs.get_stat(ifullname)

            #Need to verify the input file is a file and not a directory.
            # There should be a cleaner way to do this, but this will
            # work for now.
            #inputfile_check(ifullname, e)
            if stat.S_ISDIR(istatinfo[stat.ST_MODE]):
                raise EncpError(errno.EISDIR, ifullname,
                                e_errors.USERERROR,
                                {'infilepath' : ifullname})
            elif not stat.S_ISREG(istatinfo[stat.ST_MODE]):
                raise EncpError(errno.EINVAL, ifullname,
                                e_errors.USERERROR,
                                {'infilepath' : ifullname})

            original_bfid = sfs.get_bit_file_id(ifullname)
            if e.copy == 0:
                bfid = original_bfid
            else:
                fcc = get_fcc(original_bfid)
                copy_info = fcc.find_copies(original_bfid)
                if e_errors.is_ok(copy_info):
                    copy_list = copy_info['copies']
                    try:
                        #We need to subtract one here, becuause e.copy
                        # considers 0 to be the origianal, but this
                        # list considers the first copy to be at index 0.
                        bfid = copy_list[e.copy - 1]
                    except IndexError:
                        raise EncpError(errno.EINVAL,
                                 "File does not contain copy %s." % e.copy,
                                        e_errors.USERERROR,
                            {'infilepath' : ifullname,
                             })

            vc_reply, fc_reply = get_clerks_info(bfid, e)

            if getattr(e, "sequential_filenames", None):
                #The user overrode "get" to use numbered filenames.
                ofullname = os.path.join(e.output[0],
                                         "%s:%s" % (fc_reply['external_label'],
                                                 fc_reply['location_cookie']))
            else:
                ofullname = get_oninfo(ifullname, e.output[0], e)


            #Determine the access path name.
            use_dir = get_directory_name(ifullname)
            iaccessname = sfs.access_file(use_dir, fc_reply['pnfsid'])

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
                            "File clerk responce did not contain an " \
                            "external label.",
                            e_errors.KEYERROR,
                            {'fc' : fc_reply, 'vc' : vc_reply,
                             'infilepath' : ifullname,
                             'outfilepath' : ofullname,
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
        #    route_selection = 1   #1 to use udp_serv, 0 for no.
        #else:
        #    route_selection = 0   #1 to use udp_serv, 0 for no.

        # allow library manager selection based on the environment variable
        lm = os.environ.get('ENSTORE_SPECIAL_LIB')
        if lm != None:
            vc_reply['library'] = lm

        #Determine the max resend values for this transfer.
        csc = get_csc()
        resend = max_attempts(csc, vc_reply['library'], e)

        #Get the crc seed to use.
        #encp_dict = csc.get('encp', 5, 5)
        #crc_seed = encp_dict.get('crc_seed', 1)

        #We cannot use --shortcut when using NULL movers and tapes.
        # The mover insists that "NULL" appear in the pnfs pathname,
        # which conflicts with the task of --shortcut.  So, we need to
        # breakdown and do this full name lookup.
        if e.shortcut:
            if is_null_media_type(vc_reply):
                if e.get_cache:
                    use_id = e.get_cache
                else:
                    use_id = fc_reply['pnfsid']

                #To avoid an error with the mover, perform a full
                # pnfs pathname lookup.
                ifullname_list = sfs.get_path(use_id,
                                              e.pnfs_mount_point,
                                              shortcut=False)

                ifullname = ifullname_list[0]
                wrapper['pnfsFilename'] = ifullname

        #request = {}
        request['bfid'] = bfid
        request['callback_addr'] = callback_addr
        request['client_crc'] = e.chk_crc
        #request['crc_seed'] = crc_seed #Only for writes?
        request['encp'] = encp_el
        request['encp_daq'] = encp_daq
        request['fc'] = fc_reply
        request['file_size'] = file_size
        request['infile'] = iaccessname   #For file access.
        request['infilepath'] = ifullname #For reporting.
        if udp_callback_addr: #For "get" only.
            request['method'] = "read_next"
        request['outfile'] = ofullname    #For file access.
        request['outfilepath'] = ofullname  #For reporting.
        #request['override_noaccess'] = e.override_noaccess #no to this
        request['override_ro_mount'] = e.override_ro_mount
        request['resend'] = resend
        #request['retry'] = 0
        if udp_callback_addr: #For "get" only.
            request['routing_callback_addr'] = udp_callback_addr
            request['route_selection'] = 1
        request['times'] = tinfo.copy() #Only info now in tinfo needed.
        request['unique_id'] = generate_unique_id()
        request['user_level'] = e.user_level
        request['vc'] = vc_reply
        request['version'] = encp_client_version()
        request['volume'] = label
        request['work'] = read_work #'read_from_hsm' or 'read_tape'
        request['wrapper'] = wrapper

        return request

#######################################################################

# submit read_from_hsm requests
def submit_read_requests(requests, encp_intf):

    submit_read_requests_start_time = time.time()

    submitted = 0

    #Sort the requests by location cookie.
    requests.sort(sort_cookie)

    for req in requests:
        #After enough failures handle_retries() will eventually abort
        # the transfer.

        while req.get('completion_status', None) == None:
            try:
                ticket, lmc = submit_one_request(req, encp_intf)
            except (socket.error, select.error, e_errors.EnstoreError), msg:
                lmc = None
                if msg.args[0] == errno.ETIMEDOUT:
                    ticket = {'status' : (e_errors.TIMEDOUT,
                                      req.get('vc', {}).get('library', None))}
                else:
                    ticket = {'status' : (e_errors.NET_ERROR, str(msg))}

            result_dict = handle_retries(requests, req, ticket, encp_intf)

            if e_errors.is_retriable(result_dict):
                continue
            elif e_errors.is_non_retriable(result_dict):
                req['completion_status'] = FAILURE
                return submitted, ticket, lmc
            else:
                submitted = submitted + 1
                break

    Trace.message(TO_GO_LEVEL, "SUBMITED: %s" % submitted)
    Trace.message(TRANSFER_LEVEL, "Files queued." + elapsed_string())

    message = "[1] Time to submit %d read requests: %s sec." % \
              (submitted, time.time() - submit_read_requests_start_time,)
    Trace.message(TIME_LEVEL, message)
    Trace.log(TIME_LEVEL, message)

    return submitted, ticket, lmc

#######################################################################

def stall_read_transfer(data_path_socket, control_socket, work_ticket, e):

    stall_read_transfer_start_time = time.time()

    Trace.log(INFO_LEVEL,
              "stalling read transfer until data arives: %s sec" % \
              (e.mover_timeout,))

    #Stall starting the count until the first byte is ready for reading.
    duration = e.mover_timeout
    while 1:
        start_time = time.time()
        try:
            read_fd, unused, unused = select.select([data_path_socket],
                                                    [], [], duration)
            break
        except (select.error, socket.error), msg:
            if msg.args[0] in [errno.EINTR, errno.EAGAIN]:
                #If the select was interupted by a signal, keep going.
                duration = duration - (time.time() - start_time)
                continue
            else:
                read_fd = []
                break

    if data_path_socket not in read_fd:
        Trace.log(INFO_LEVEL,
                  "confirming control_socket still okay: %s sec" % (10,))

        try:
            read_control_fd, unused, unused = select.select([control_socket],
                                                            [], [], 10)
            if control_socket in read_control_fd:
                status_ticket = callback.read_tcp_obj(control_socket)
            else:
                status_ticket = {'status' : (e_errors.UNKNOWN,
                                             "No data read from mover.")}
        except (select.error, socket.error, e_errors.EnstoreError), msg:
            status_ticket = {'status' : (e_errors.NET_ERROR,
                                         "%s: %s" % (str(msg),
                                               "No data read from mover."))}
        except e_errors.TCP_EXCEPTION:
            status_ticket = {'status' : (e_errors.NET_ERROR,
                                         e_errors.TCP_EXCEPTION)}

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
        try:
            read_control_fd, unused, unused = select.select([control_socket],
                                                            [], [], 10)
            if control_socket in read_control_fd:
                status_ticket = callback.read_tcp_obj(control_socket)
            else:
                status_ticket = {'status' : (e_errors.UNKNOWN,
                                             "No data read from mover.")}
        except (select.error, socket.error, e_errors.EnstoreError), msg:
            status_ticket = {'status' : (e_errors.NET_ERROR,
                                         "%s: %s" % (str(msg),
                                               "No data read from mover."))}
        except e_errors.TCP_EXCEPTION:
            status_ticket = {'status' : (e_errors.NET_ERROR,
                                         e_errors.TCP_EXCEPTION)}
    else:
        status_ticket = {'status' : (e_errors.OK, None)}

    message = "[1] Time to stall %d read transfer: %s sec." % \
              (1, time.time() - stall_read_transfer_start_time,)
    Trace.message(TIME_LEVEL, message)
    Trace.log(TIME_LEVEL, message)

    return status_ticket

#############################################################################

def read_post_transfer_update(done_ticket, out_fd, e):
    #Verify size is the same.
    try:
        verify_file_size(done_ticket, e)
    except AttributeError:
        Trace.handle_error(severity=99)
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]

    if not e_errors.is_ok(done_ticket):
        return done_ticket

    #Check the CRC.
    check_crc(done_ticket, e, out_fd)

    if not e_errors.is_ok(done_ticket):
        return done_ticket

    update_modification_time(done_ticket['outfile'])

    #If this is a read of a deleted file, leave the outfile permissions
    # to the defaults.
    if not (e.override_deleted and done_ticket['fc']['deleted'] != 'no'):
        #This function writes errors/warnings to the log file and puts an
        # error status in the ticket.
        set_outfile_permissions(done_ticket, e) #Writes errors to log file.
        if not e_errors.is_ok(done_ticket):
            #Special way of saying that this error should be returned as an
            # error without retrying and that the output file should be left
            # alone.
            done_ticket['skip_retry'] = 1

    #Update the source files times.
    if done_ticket['fc']['deleted'] == "no":
        update_last_access_time(done_ticket['infile'])


    return done_ticket

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

def read_hsm_file(request_ticket, control_socket, data_path_socket,
                  request_list, tinfo, e, udp_serv = None):

    for rq in request_list:
        Trace.trace(17,"read_hsm_file: %s"%(rq['infile'],))

    read_hsm_file_start_time = time.time()

    overall_start = time.time() #----------------------------Overall Start

    ### 8-22-2008: Commented out the verify_read_request_consistancy()
    ### call here.  It does a lot of checks that don't need to be done
    ### that also put a lot of strain on PNFS.  The handle_retries()
    ### pnfs_filename check just after open_local_file should be
    ### sufficent.
    """
    #Be paranoid.  Check this the ticket again.
    #Dmitry is not paranoid
    #r_encp = os.environ.get('REMOTE_ENCP')
    r_encp = None #Dmitry should know better than to not be paranoid.
    if r_encp == None:
        try:
            #Skip this test for volume transfers and migration/duplication
            # transfers for performance reasons.
            if not e.volume and not e.migration_or_duplication:
                verify_read_request_consistancy(
                    {request_ticket.get("external_label","label"):[request_ticket]}, e)
        except EncpError, msg:
            msg.ticket['status'] = (msg.type, msg.strerror)
            result_dict = handle_retries(request_list, msg.ticket,
                                     msg.ticket, e)

            if not e_errors.is_ok(result_dict):
                #close_descriptors(control_socket, data_path_socket)
                return combine_dict(result_dict, request_ticket)
    """

    Trace.message(TRANSFER_LEVEL, "Mover called back.  elapsed=%s" %
                  (time.time() - tinfo['encp_start_time'],))
    Trace.message(TICKET_LEVEL, "REQUEST:")
    Trace.message(TICKET_LEVEL, pprint.pformat(request_ticket))

    #Open the output file for writing.
    done_ticket = open_local_file(request_ticket, tinfo, e)

    #By adding the pnfs_filename check, we will know if another process
    # has modified this file.
    Trace.log(99, "after open_local_file: %s" % (done_ticket['status'],))
    result_dict = handle_retries(request_list, request_ticket,
                                 done_ticket, e,
                                 pnfs_filename = request_ticket['infile'])

    if not e_errors.is_ok(result_dict):
        #close_descriptors(control_socket, data_path_socket)
        return combine_dict(result_dict, request_ticket)
    else:
        out_fd = done_ticket['fd']

    #We need to stall the transfer until the mover is ready.
    done_ticket = stall_read_transfer(data_path_socket, control_socket,
                                      request_ticket, e)

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
                                     #udp_serv = route_server,
                                     control_socket = control_socket,
                                     external_label = external_label)
        if not e_errors.is_ok(result_dict):
            #close_descriptors(control_socket, data_path_socket, out_fd)
            close_descriptors(out_fd)
            return combine_dict(result_dict, request_ticket)

    lap_start = time.time() #----------------------------------------Start

    try:
        done_ticket = transfer_file(data_path_socket, out_fd,
                                    control_socket, request_ticket,
                                    tinfo, e, udp_serv = udp_serv)
    except:
        exc, msg = sys.exc_info()[:2]
        done_ticket = request_ticket
        done_ticket['status'] = (e_errors.UNKNOWN, "transfer_file(): (%s, %s)" % (str(exc), str(msg)))

    lap_end = time.time()  #-----------------------------------------End
    tstring = "%s_transfer_time" % request_ticket['unique_id']
    tinfo[tstring] = lap_end - lap_start

    Trace.message(TRANSFER_LEVEL, "Verifying %s transfer.  elapsed=%s" %
                  (request_ticket['infilepath'],
                   time.time() - tinfo['encp_start_time']))

    #Verify that everything went ok with the transfer.
    result_dict = handle_retries(request_list, request_ticket,
                                 done_ticket, e)

    if not e_errors.is_ok(result_dict):
        #Close these before they are forgotten.
        close_descriptors(out_fd)
        return combine_dict(result_dict, request_ticket)

    #Make sure the exfer sub-ticket gets stored into request_ticket.
    request_ticket = combine_dict(done_ticket, request_ticket)

    try:
        #This function write errors/warnings to the log file and put an
        # error status in the ticket.
        done_ticket = read_post_transfer_update(done_ticket, out_fd, e)
    except (SystemExit, KeyboardInterrupt):
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
    except:
        Trace.handle_error(severity=99)
        done_ticket['status'] = (e_errors.UNKNOWN, sys.exc_info()[1])

    result_dict = handle_retries(request_list, request_ticket,
                                 done_ticket, e)

    if e_errors.is_non_retriable(done_ticket.get('status', (e_errors.OK,None))):
        #Close these before they are forgotten.
        close_descriptors(out_fd)
        return combine_dict(result_dict, request_ticket)
    if not e_errors.is_ok(done_ticket.get('status', (e_errors.OK,None))):
        #Close these before they are forgotten.
        close_descriptors(out_fd)
        return combine_dict(result_dict, request_ticket)

    #If no error occured, this is safe to close now.
    close_descriptors(out_fd)

    #Remove the new file from the list of those to be deleted should
    # encp stop suddenly.  (ie. crash or control-C).
    delete_at_exit.unregister(done_ticket['outfile']) #localname

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

    Trace.message(TICKET_LEVEL, "DONE TICKET (read_hsm_file)")
    Trace.message(TICKET_LEVEL, pprint.pformat(done_ticket))

    message = "[1] Time to read hsm file: %s sec." % \
              (time.time() - read_hsm_file_start_time,)
    Trace.message(TIME_LEVEL, message)
    Trace.log(TIME_LEVEL, message)

    return done_ticket

#######################################################################

def prepare_read_from_hsm(tinfo, e):
    done_ticket, listen_socket, callback_addr, \
                 udp_serv, udp_callback_addr = get_callback_addresses(e)
    if not e_errors.is_ok(done_ticket):
        return done_ticket, listen_socket, udp_serv, None

    #Create all of the request dictionaries.
    try:
        requests_per_vol = create_read_requests(callback_addr,
                                                udp_callback_addr, tinfo, e)

    except (SystemExit, KeyboardInterrupt):
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
    except (OSError, IOError, AttributeError, ValueError, EncpError), msg:
        Trace.handle_error(severity=99)
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
        elif isinstance(msg, socket.error) or isinstance(msg, select.error):
            #On 11-12-2009, tracebacks were found from migration encps that
            # started failing because there were too many open files while
            # trying to instantiate client classes.  The socket.error should
            # have been caught here.  So now it is.
            e_ticket = {'status' : (e_errors.NET_ERROR, str(msg))}
        else:
            e_ticket = {'status' : (e_errors.WRONGPARAMETER, str(msg))}

        return e_ticket, listen_socket, udp_serv, {}
    except:
        e_ticket = {'status' : (e_errors.UNKNOWN,
                                "%s: %s" % (str(sys.exc_info()[0]),
                                                str(sys.exc_info()[1])))}
        try:
            Trace.handle_error()
        except (SystemExit, KeyboardInterrupt):
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except:
            #We'll go with the original error here.
            pass

        return e_ticket, listen_socket, udp_serv, []

    #If this is the case, don't worry about anything.
    if (len(requests_per_vol) == 0):
        done_ticket = {'status' : (e_errors.NO_FILES, "No files to transfer.")}
        return done_ticket, listen_socket, udp_serv, requests_per_vol

    #Sort the requests in increasing order.
    for vol in requests_per_vol.keys():
        if enstore_functions3.is_volume_tape(vol):
            requests_per_vol[vol].sort(
                lambda x, y: cmp(x['fc']['location_cookie'],
                                 y['fc']['location_cookie']))

    #This will halt the program if everything isn't consistant.
    try:
        #Skip this test for volume transfers and migration/duplication
        # transfers for performane reasons.
        if not e.volume and not e.migration_or_duplication:
            verify_read_request_consistancy(requests_per_vol, e)
    except EncpError, msg:
        if not msg.ticket.get('status', None):
            msg.ticket['status'] = (msg.type, msg.strerror)
        return msg.ticket, listen_socket, udp_serv, requests_per_vol
    except OSError, msg:
        if msg.errno in [errno.ENOENT, errno.EPERM, errno.EACCES]:
            return_ticket = {'status' : (e_errors.USERERROR, str(msg))}
        else:
            return_ticket = {'status' : (e_errors.OSERROR, str(msg))}
        return return_ticket, listen_socket, udp_serv, requests_per_vol
    except (socket.error, select.error):
        #On 11-12-2009, tracebacks were found from migration encps that
        # started failing because there were too many open files while
        # trying to instantiate client classes.  The socket.error should
        # have been caught here.  So now it is.
        return_ticket = {'status' : (e_errors.NET_ERROR, str(msg))}
        return return_ticket, listen_socket, udp_serv, requests_per_vol

    #If we are only going to check if we can succeed, then the last
    # thing to do is see if the LM is up and accepting requests.
    if e.check:
        #Determine the name of the library.
        check_lib = requests_per_vol.values()[0][0]['vc']['library'] + \
                    ".library_manager"
        try:
            return check_library(check_lib, e), listen_socket, \
                   udp_serv, requests_per_vol
        except EncpError, msg:
            return_ticket = { 'status'      : (msg.type, str(msg)),
                              'exit_status' : 2 }
            return return_ticket, listen_socket, udp_serv, requests_per_vol

    #Create the zero length file entries.
    message = "Creating zero length output files."
    Trace.message(TRANSFER_LEVEL, message)
    Trace.log(e_errors.INFO, message)
    for vol in requests_per_vol.keys():
        for i in range(len(requests_per_vol[vol])):

            if requests_per_vol[vol][i]['outfile'] in ["/dev/null"]:
                continue

            should_skip_deleted = e.override_deleted and \
                             requests_per_vol[vol][i]['fc']['deleted'] != "no"


            try:
                create_zero_length_local_files(requests_per_vol[vol][i])

                if not should_skip_deleted:
                    #If the file is not deleted, we should set the ownership
                    # to that of the owner of the original file.

                    ### Note to self: work on removing this stat().


                    #Only try this when the real user is root.
                    if os.getuid() == 0:
                        # if --get-bfid and --skip-pnfs is specified do not use
                        # a convoluted get_stat
                        if e.get_bfid and e.skip_pnfs:
                            uid = requests_per_vol[vol][i]['fc']['uid']
                            gid = requests_per_vol[vol][i]['fc']['gid']
                        else:
                            in_file_stats = get_stat(requests_per_vol[vol][i]['infile'])
                            uid = in_file_stats[stat.ST_UID]
                            gid = in_file_stats[stat.ST_GID]
                        file_utils.chown(requests_per_vol[vol][i]['outfile'],
                                         uid,
                                         gid,
                                         unstable_filesystem=True)
            except (OSError, IOError, EncpError), msg:
                #if not should_skip_deleted:
                #    file_utils.end_euid_egid() #Release the lock.

                if isinstance(msg, EncpError):
                    requests_per_vol[vol][i]['status'] = (msg.type, str(msg))
                elif isinstance(msg, OSError):
                    if msg.args[0] == getattr(errno, "EFSCORRUPTED", None) \
                           or (msg.args[0] == errno.EIO and \
                               msg.args[1].find("corrupt") != -1):
                        requests_per_vol[vol][i]['status'] = \
                                     (e_errors.FILESYSTEM_CORRUPT, str(msg))
                    else:
                        requests_per_vol[vol][i]['status'] = \
                                     (e_errors.OSERROR, str(msg))
                elif isinstance(msg, IOError):
                    requests_per_vol[vol][i]['status'] = \
                                     (e_errors.IOERROR, str(msg))
                else:
                    requests_per_vol[vol][i]['status'] = \
                                     (e_errors.UNKNOWN, str(msg))

                requests_per_vol[vol][i]['completion_status'] = FAILURE

                return requests_per_vol[vol][i], listen_socket, udp_serv, requests_per_vol
            except:
                #if not should_skip_deleted:
                #    file_utils.end_euid_egid() #Release the lock.

                raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]

            #if not should_skip_deleted:
            #        file_utils.end_euid_egid() #Release the lock.

    return_ticket = { 'status' : (e_errors.OK, None)}
    return return_ticket, listen_socket, udp_serv, requests_per_vol

def read_from_hsm(e, tinfo):
    global err_msg

    Trace.trace(16,"read_from_hsm input_files=%s  output=%s  verbose=%s  "
                "chk_crc=%s t0=%s" % (e.input, e.output, e.verbose,
                                      e.chk_crc, tinfo['encp_start_time']))

    # initialize
    byte_sum = 0L #Sum of bytes transfered (when transfering multiple files).
    exit_status = 0 #Used to determine the final message text.
    number_of_files = 0 #Total number of files where a transfer was attempted.

    # Get the list of files to read.
    done_ticket, listen_socket, unused, requests_per_vol = \
                   prepare_read_from_hsm(tinfo, e)

    if not e_errors.is_ok(done_ticket) or e.check:
        return done_ticket   #, requests_per_vol

    ######################################################################
    # loop over all volumes that are needed and submit all requests for
    # that volume. Read files from each volume before submitting requests
    # for different volumes.
    ######################################################################

    vols = requests_per_vol.keys()
    vols.sort()
    for vol in vols:

        #Report how many files are still to go.
        Trace.message(TO_GO_LEVEL, "FILES LEFT: %s" % \
                      all_requests_outstanding(requests_per_vol))

        request_list = requests_per_vol[vol]
        number_of_files = number_of_files + len(request_list)

        #The return value is the number of files successfully submitted.
        # This value may be different from len(request_list).  The value
        # of request_list is not changed by this function.
        submitted, reply_ticket, lmc = submit_read_requests(request_list, e)

        #If USE_NEW_EVENT_LOOP is true, we need this cleared.
        transaction_id_list = []

        #If at least one submission succeeded, follow through with it.
        if submitted > 0:
            while requests_outstanding(request_list):

                #Wait for all possible messages or connections.
                request_ticket, control_socket, data_path_socket = \
                                wait_for_message(listen_socket, lmc,
                                                 request_list,
                                                 transaction_id_list, e)

                #If we connected with the mover, add these two checks.
                # Skip them if we only heard from the LM.
                if control_socket:
                    local_filename = request_ticket.get(
                        'wrapper', {}).get('fullname', None)
                    external_label = request_ticket.get(
                        'fc', {}).get('external_label', None)
                else:
                    local_filename = None
                    external_label = None

                #Obtain the index of the returned request.  This needs to be
                # done before handle_retries, becuase if an error occured
                # request_list items, like unique_id, are modified by
                # handle_retries().  get_request_index() searches based on
                # unique_id, which needs to be found based on the unique_id
                # before it is updated.
                index, unused = get_request_index(request_list, request_ticket)
                if index == None:
                    #If the error happend before we knew the next request
                    # (i.e. a timeout occured waiting), then pick the next
                    # request so the retry/resubmits counts can be attributed
                    # somewhere.
                    unused, index, unused = get_next_request(request_list)

                result_dict = handle_retries(request_list, request_ticket,
                                             request_ticket, e,
                                             listen_socket = listen_socket,
                                             local_filename = local_filename,
                                             external_label = external_label)

                #If USE_NEW_EVENT_LOOP is true, we need these ids.
                transaction_id_list = result_dict.get('transaction_id_list',[])

                #Make sure done_ticket exists by this point.
                #
                #For LM submission errors (i.e. tape went NOACCESS), use
                # any request information in result_dict to identify which
                # request gave an error.
                #
                #Writes included work_ticket in this combine.  That doesn't
                # work for reads because we don't know which read request
                # will get picked first.
                #It is important to note that the write version works because,
                # work_ticket is passed as the work ticket to handle_retries()
                # and has the unique_id field updated on errors.  Reads pass
                # the request_ticket as both the error ticket and work ticket,
                # thus request_ticket has this info already when an error
                # happened.
                done_ticket = combine_dict(result_dict, request_ticket)


                if e_errors.is_non_retriable(result_dict):

                    #Regardless if index is None or not, make sure that
                    # exit_status gets set to failure.
                    if e.migration_or_duplication:
                        #Handle migration cases a little differently.

                        # Make sure the migration knows not to try this
                        # file again.
                        exit_status = 2
                    else:
                        exit_status = 1
                    # Make sure the correct error is reported for migration.
                    # If we don't set this here, then we end up with the
                    # non-useful ("OK", "Error after transfering... )
                    # error messages from calculate_final_statistics.
                    err_msg[thread.get_ident()] = str(result_dict['status'])

                    if index == None:
                        message = "Unknown transfer failed."
                        try:
                            sys.stderr.write(message + "\n")
                            sys.stderr.flush()
                        except IOError:
                            pass
                        Trace.log(e_errors.ERROR,
                                  message + "  " + str(done_ticket))

                    else:
                        #Combine the dictionaries.
                        work_ticket = combine_dict(done_ticket,
                                                   request_list[index])
                        #Set completion status to failed.
                        work_ticket['completion_status'] = FAILURE
                        #Set the exit status value.
                        work_ticket['exit_status'] = exit_status
                        #Store these changes back into the master list.
                        request_list[index] = work_ticket
                        #Make sure done_ticket points to the new info too.
                        done_ticket = work_ticket

                    return done_ticket

                if not e_errors.is_ok(result_dict):
                    #Combine the dictionaries.
                    work_ticket = combine_dict(done_ticket,
                                               request_list[index])
                    #Store these changes back into the master list.
                    request_list[index] = work_ticket
                    continue

                if not control_socket:
                    #We only got a response from the LM, we did not connect
                    # with the mover yet.
                    continue

                ############################################################
                #In this function call is where most of the work in transfering
                # a single file is done.
                #
                done_ticket = read_hsm_file(request_ticket,
                    control_socket, data_path_socket, request_list, tinfo, e)
                ############################################################

                # Close these descriptors before they are forgotten about.
                close_descriptors(control_socket, data_path_socket)

                #Sum up the total amount of bytes transfered.
                exfer_ticket = done_ticket.get('exfer',
                                               {'bytes_transfered' : 0L})
                byte_sum = byte_sum + exfer_ticket.get('bytes_transfered', 0L)

                #Combine the tickets.
                request_ticket = combine_dict(done_ticket, request_ticket)
                #Store these changes back into the master list.
                requests_per_vol[vol][index] = request_ticket

                #The completion_status is modified in the request ticket.
                # what_to_do = 0 for stop
                #            = 1 for continue
                #            = 2 for continue after retry
                what_to_do = finish_request(request_ticket,
                                            requests_per_vol[vol],
                                            index, e)

                #If on non-success exit status was returned from
                # finish_request(), keep it around for later.
                if request_ticket.get('exit_status', None):
                    #We get here only on an error.  If the value is 1, then
                    # the error should be transient.  If the value is 2, then
                    # the error will likely require human intervention to
                    # resolve.
                    exit_status = request_ticket['exit_status']
                # Do what finish_request() says to do.
                if what_to_do == STOP:
                    #We get here only on a non-retriable error.
                    if not exit_status:
                        #Just in case this got missed somehow.
                        exit_status = 1
                    break
                elif what_to_do == CONTINUE_FROM_BEGINNING:
                    #We get here only on a retriable error.
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
    calc_ticket = calculate_final_statistics(byte_sum, number_of_files,
                                             exit_status, tinfo)

    #Volume one ticket is the last request that was processed.
    if e.data_access_layer:
        list_done_ticket = combine_dict(calc_ticket, done_ticket)
    else:
        list_done_ticket = combine_dict(calc_ticket, {})

    Trace.message(TICKET_LEVEL, "LIST DONE TICKET")
    Trace.message(TICKET_LEVEL, pprint.pformat(list_done_ticket))

    return list_done_ticket  #, requests_per_vol

##############################################################################
##############################################################################

class EncpInterface(option.Interface):

    #  define our specific parameters
    user_parameters = ["<source file> <destination file>",
                       "<source file> [source file [...]] <destination directory>"]
    admin_parameters = user_parameters +  \
                       ["--get-bfid <bfid> <destination file>",
                        "--get-cache <pnfs|chimera id> <destination file>",
                        "--put-cache <pnfs|chimera id> <source file>"]
    parameters = user_parameters #gets overridden in __init__().



    def __init__(self, args=sys.argv, user_mode=0):
        #This is flag is accessed via a global variable.
        global pnfs_is_automounted

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
        self.copy = 0              # copy number to read from tape
                                   # (0 = original)

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
        self.skip_deleted_files = None #skip files marked deleted
        self.skip_pnfs = None      #Ignore pnfs when --get-bfid is used.
        self.migration_or_duplication = None #Set true if the transfer is
                                   # called from migrate.py or duplicate.py.

        #Special options for operation with a disk cache layer.
        #self.dcache = 0            #obsolete???
        self.put_cache = 0         # true if dcache is writing by pnfs_id
        self.get_cache = 0         # true if dcache is reading by pnfs_id
        self.get_bfid = None       # true if dcache is reading by bfid
        self.get_bfids = None      # true if dcache is reading by bfids
        self.pnfs_mount_point = "" # For dcache, used to specify which pnfs
                                   # mount point to use.  Naively, one can
                                   # not try them all.  If the wrong mount
                                   # point is tried it could hang encp.
        self.shortcut = 0          # If true, don't extrapolate full file path.
        self.storage_info = None   # Not used yet.
        self.volume = None         # True if it is to read an entire volume.

        #Values for specifying which enstore system to contact.
        self.enstore_config_host = enstore_functions2.default_host()
        self.enstore_config_port = enstore_functions2.default_port()

        #Sometimes the kernel lies about the max size of files supported
        # by the filesystem; skip the test if that is needed.
        self.bypass_filesystem_max_filesize_check = 0

        #If the user wants the pnfs_agent instead of locally mount pnfs,
        # let them have it.  This needs to be set BEFORE
        # option.Interface.__init__() since it uses this value.
        r_encp = os.environ.get('REMOTE_ENCP')
        if r_encp != None:
            namespace.pnfs_agent_client_allowed = True
        if r_encp == "only_pnfs_agent":
            namespace.pnfs_agent_client_requested = True

        #Override the default paramater list for admin mode and user2/dcache
        # mode only.
        if user_mode in [0, 2] and \
               not (hasattr(self, 'put') or hasattr(self, 'get')):
            self.parameters = self.admin_parameters

        # Disable redirection of encp to another library manager
        self.enable_redirection = 0

        # Flag which is set if redirection occured
        self.redirected = False

        # parse the options
        option.Interface.__init__(self, args=args, user_mode=user_mode)

        # This is accessed globally...
        pnfs_is_automounted = self.pnfs_is_automounted

    def __str__(self):
        str_rep = ""

        #Sort the list into alphabetical order.
        the_list = self.encp_options.items()
        the_list = the_list + [('input', {}), ('output', {}),
                               ('intype', {}), ('outtype', {}),
                               ]
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
        option.COPY:{option.HELP_STRING:
                     "Read copy N of the file.  (0 = primary)",
                     option.VALUE_USAGE:option.REQUIRED,
                     option.VALUE_TYPE:option.INTEGER,
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
        #option.DISABLE_REDIRECTION:{option.HELP_STRING:
        #                  "Disable redirection of request to another library."
        #                  " Do not use Library Manager Director" ,
        #                  option.DEFAULT_TYPE:option.INTEGER,
        #                  option.DEFAULT_VALUE:1,
        #                  option.USER_LEVEL:option.ADMIN,},
        option.ENABLE_REDIRECTION:{option.HELP_STRING:
                          "Enable redirection of request to another library."
                          " Use Library Manager Director" ,
                          option.DEFAULT_TYPE:option.INTEGER,
                          option.DEFAULT_VALUE:1,
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
                                    option.USER_LEVEL:option.USER2,},
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
                         option.USER_LEVEL:option.USER2,},
        option.GET_BFIDS:{option.HELP_STRING:
                         "Specifies that dcache requested the file and that "
                         "the first 'filename' is really the file's bfid.",
                         option.VALUE_TYPE:option.INTEGER,
                         option.VALUE_USAGE:option.IGNORED,
                         option.USER_LEVEL:option.USER2,},
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
        option.SKIP_DELETED_FILES:{option.HELP_STRING:
                                   "Skip over deleted files.  "
                                   "Used with --volume",
                                   option.VALUE_USAGE:option.IGNORED,
                                   option.VALUE_TYPE:option.INTEGER,
                                   option.USER_LEVEL:option.USER,},
        option.SKIP_PNFS:{option.HELP_STRING:
                                   "Skip checking with PNFS.  "
                                   "Used with --get-bfid",
                                   option.VALUE_USAGE:option.IGNORED,
                                   option.VALUE_TYPE:option.INTEGER,
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
                       option.USER_LEVEL:option.USER,},
        }

    ##########################################################################
    # parse the options from the command line
    def parse_options(self):
        # normal parsing of options
        option.Interface.parse_options(self)

        #Process these at the beginning.
        if hasattr(self, "help") and self.help:
            self.print_help()
        if hasattr(self, "usage") and self.usage:
            self.print_usage()
        if hasattr(self, "version") and self.version:
            self.print_version()

        #Sanity check.  Only allow skip_pnfs to work if --get-bfid was used.
        if not self.get_bfid and not self.get_bfids:
            self.skip_pnfs = None

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
            self.print_usage("Argument for --copy must be a "
                             "non-negative integer.")

        # bomb out if we don't have an input/output if a special command
        # line was given.  (--volume, --get-cache, --put-cache, --bfid)
        self.arglen = len(self.args)
        if self.arglen < 1 :
            self.print_usage("%s: not enough arguments specified" %
                             e_errors.USERERROR)
            sys.exit(1)

        if self.volume:
            self.input = self.argv[:-1]
            self.output = self.argv[-1]

        if self.get_bfid:
            self.input = None
            self.output = self.argv[-1]

        if self.get_bfids:
            self.input = self.args[:-1]
            self.output = self.argv[-1]

        if self.get_cache:
            self.input = None
            self.output = self.argv[-1]

        self.outtype = UNIXFILE
        if self.volume or self.get_bfid or self.get_bfids or self.get_cache:
            if namespace.pnfs_agent_client_requested:
                self.intype = RHSMFILE
            elif namespace.pnfs_agent_client_allowed:
                self.intype = HSMFILE #Is this correct?
            else:
                self.intype = HSMFILE
            return

        if self.put_cache:
            self.input = self.argv[-1]
            self.output = None
            self.intype = UNIXFILE
            if namespace.pnfs_agent_client_requested:
                self.outtype = RHSMFILE
            elif namespace.pnfs_agent_client_allowed:
                self.outtype = HSMFILE #Is this correct?
            else:
                self.outtype = HSMFILE
            return #Don't continue.

        #If just the output file was specified and standard input is a
        # FIFO, then set the input file to be read from standard in.
        # We can't just test for FIFOs here.  In the cron environment,
        # processes are started with standard in/out/err as pipes
        # (a.k.a. FIFOs).
        file_stat=os.fstat(sys.stdin.fileno())
        if len(self.args) == 1 and stat.S_ISFIFO(file_stat[stat.ST_MODE]):
            self.input = ["/dev/fd/%d" % (sys.stdin.fileno(),)]
            self.output = [self.argv[-1]]
            self.intype = UNIXFILE
            self.outtype = HSMFILE  #What should this be?
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
            if namespace.pnfs_agent_client_requested:
                protocol, host, port, fullname, dirname, basename = \
                          enstore_functions2.fullpath2(self.args[i],
                                                       no_split = True)
                #Pnfs Agent.
                pac = get_pac()
                result = pac.is_pnfs_path(fullname, check_name_only = 1)
                if result and pac.isdir(fullname):
                    dirname = fullname
                    basename = ""
                else:
                    dirname, basename = os.path.split(fullname)
            else:
                protocol, host, port, fullname, dirname, basename = \
                          enstore_functions2.fullpath2(self.args[i])

            #Store the name into this list.
            self.args[i] = fullname

            #We need to make sure that all the files are to/from the same
            # place.  Store it here for processing later.
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

            if namespace.pnfs_agent_client_requested:
                #try:
                #    #Pnfs Agent.
                #    pac = get_pac()
                #    result = pac.is_pnfs_path(fullname,
                #                              check_name_only = 1)
                #except EncpError, msg:
                #    result = 0

                if result:
                    e.append(0)
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
                result = namespace.is_storage_local_path(fullname,
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
                result = namespace.is_storage_local_path(pnfs_path,
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
                result = namespace.is_storage_local_path(admin_path,
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
                result = namespace.is_storage_local_path(grid_path,
                                                         check_name_only = 1)
            except EncpError:
                result = 0

            if result:
                e.append(1)
                p.append(result)
                self.args[i] = grid_path
                continue

            if namespace.pnfs_agent_client_allowed:
                try:
                    #Pnfs Agent.
                    pac = get_pac()
                    result = pac.is_pnfs_path(fullname,
                                              check_name_only = 1)
                except EncpError:
                    result = 0

                if result:
                    e.append(0)
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

        if namespace.pnfs_agent_client_requested:
            if p1 == 1 or p1 == 2:
                self.intype = RHSMFILE
            else:
                self.intype = UNIXFILE
            if p2 == 1 or p2 == 2:
                self.outtype = RHSMFILE
            else:
                self.outtype = UNIXFILE

        elif namespace.pnfs_agent_client_allowed:
            if p1 == 2:
                self.intype = RHSMFILE
            elif p1 == 1:
                self.intype = HSMFILE
                namespace.pnfs_agent_client_allowed = False
            else:
                self.intype = UNIXFILE
            if p2 == 2:
                self.outtype = RHSMFILE
            elif p1 == 1:
                self.outtype = HSMFILE
                namespace.pnfs_agent_client_allowed = False
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
    global err_msg #hack for migration to report any error.

    err_msg_lock.acquire()
    err_msg[thread.get_ident()] = ""
    err_msg_lock.release()

    log_encp_start_time = time.time()

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

    if intf.outtype == HSMFILE or intf.outtype == RHSMFILE:
        #If verbosity is turned on and the transfer is a write to enstore,
        # output the tag information.
        try:
            if intf.put_cache:
                sfs = namespace.StorageFS(intf.put_cache,
                                          mount_point=intf.pnfs_mount_point)
                shortcut_name = sfs.get_path(intf.put_cache,
                                             intf.pnfs_mount_point,
                                             shortcut = True)[0]
                shortcut_dname = get_directory_name(shortcut_name)
                if intf.outtype == RHSMFILE:
                    t = sfs
                else:
                    t = namespace.Tag(intf.put_cache)
            elif not intf.output:
                t = None
                shortcut_dname = None
            elif file_utils.wrapper(os.path.isdir, (intf.output[0],)):
                t = namespace.Tag(intf.output[0])
                shortcut_dname = intf.output[0]
            else:
                shortcut_dname = get_directory_name(intf.output[0])
                t = namespace.Tag(intf.output[0])
        except (OSError, IOError):
            t = None

        try:
            if intf.output_library:
                library = intf.output_library
            else:
                library = t.get_library(shortcut_dname)
        except (OSError, IOError, KeyError, TypeError, AttributeError, ValueError):
            library = "Unknown"
        try:
            if intf.output_storage_group:
                storage_group = intf.output_storage_group
            else:
                storage_group = t.get_storage_group(shortcut_dname)
        except (OSError, IOError, KeyError, TypeError, AttributeError, ValueError):
            storage_group = "Unknown"
        try:
            if intf.output_file_family:
                file_family = intf.output_file_family
            else:
                file_family = t.get_file_family(shortcut_dname)
        except (OSError, IOError, KeyError, TypeError, AttributeError, ValueError):
            file_family = "Unknown"
        try:
            if intf.output_file_family_wrapper:
                file_family_wrapper = intf.output_file_family_wrapper
            else:
                file_family_wrapper = t.get_file_family_wrapper(shortcut_dname)
        except (OSError, IOError, KeyError, TypeError, AttributeError, ValueError):
            file_family_wrapper = "Unknown"
        try:
            if intf.output_file_family_width:
                file_family_width = intf.output_file_family_width
            else:
                file_family_width = t.get_file_family_width(shortcut_dname)
        except (OSError, IOError, KeyError, TypeError, AttributeError, ValueError):
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
    if intf.outtype == HSMFILE or intf.outtype == RHSMFILE:
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

    message = "[1] Time to log encp start: %s sec." % \
              (time.time() - log_encp_start_time,)
    Trace.message(TIME_LEVEL, message)
    Trace.log(TIME_LEVEL, message)


def final_say(intf, done_ticket):
    global err_msg

    try:
        #Log the message that tells us that we are done.
        status = done_ticket.get('status', (e_errors.UNKNOWN,e_errors.UNKNOWN))
        exit_status = done_ticket.get('exit_status',
                                      int(not e_errors.is_ok(status)))
        #Catch an impossible (hopefully) situation where status contains
        # and error, but exit_status says success.
        if not e_errors.is_ok(status) and not exit_status:
            Trace.log(e_errors.INFO, "Traceback line for check_for_traceback.")
            Trace.log(e_errors.INFO,
                      " done_ticket['exit_status'] = %s  exit_status = %s  status = %s" % (done_ticket.get('exit_status', "NotSet"), exit_status, status))
            if intf.migration_or_duplication and \
                   e_errors.is_non_retriable(status):
                #For fatal errors, tell migration not to retry!
                exit_status = 2
            else:
                exit_status = 1

        #Setting this global, will enable the migration to report the
        # errors directly.  This might keep the admins from having to
        # keep constantly looking through the log file.
        if e_errors.is_ok(status) and err_msg[thread.get_ident()]:
            #Don't set this migration specific string if it is already set
            # and when the "error" status has is "OK".
            #
            # The status[0] value can/will be "OK" if it is set by
            # calculate_final_statistics().  Even if there already was a
            # failed transfer.
            pass
        else:
            err_msg_lock.acquire()
            err_msg[thread.get_ident()] = str(status)
            err_msg_lock.release()

        #Determine the filename(s).
        ifilename = done_ticket.get("infilepath", "")
        ofilename = done_ticket.get("outfilepath", "")
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
        try:
            sys.stderr.write("Error (main): %s: %s\n" % (str(exc), str(msg)))
            sys.stderr.write("Exit status: %s\n", exit_status)
            sys.stderr.flush()
        except IOError:
            pass

        #Setting this global, will enable the migration to report the
        # errors directly.  This might keep the admins from having to
        # constantly looking through the log file.
        err_msg_lock.acquire()
        err_msg[thread.get_ident()] = str(("UNKNOWN", "UNKNOWN"))
        err_msg_lock.release()

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

    #Initialize the Trace module.  #We need to include the thread name for
    # the migration/duplication encps sharing a single process ID.
    Trace.init("ENCP", include_thread_name = intf.include_thread_name)
    #for x in xrange(6, intf.verbose + 1):
    #    Trace.do_print(x)
    for x in xrange(1, intf.verbose + 1):
        Trace.do_message(x)

    if intf.get_bfid or intf.get_bfids :
        intf.skip_pnfs = True
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
        #done_ticket, work_list = read_from_hsm(intf, tinfo)
        done_ticket = read_from_hsm(intf, tinfo)

    #Special handling for use with dcache.
    elif intf.put_cache:
        #done_ticket, work_list = write_to_hsm(intf, tinfo)
        done_ticket = write_to_hsm(intf, tinfo)

    ## have we been called "encp unixfile hsmfile" ?
    elif intf.intype == UNIXFILE and intf.outtype == HSMFILE :
        #done_ticket, work_list = write_to_hsm(intf, tinfo)
        done_ticket = write_to_hsm(intf, tinfo)

    ## have we been called "encp hsmfile unixfile" ?
    elif intf.intype == HSMFILE and intf.outtype == UNIXFILE :
        #done_ticket, work_list = read_from_hsm(intf, tinfo)
        done_ticket = read_from_hsm(intf, tinfo)

    ## have we been called "encp rshmfile unixfile" ?
    elif intf.intype == RHSMFILE and intf.outtype == UNIXFILE :
        #done_ticket, work_list = read_from_hsm(intf, tinfo)
        done_ticket = read_from_hsm(intf, tinfo)

    ## have we been called "encp unixfile rhsmfile" ?
    elif intf.intype == UNIXFILE and intf.outtype == RHSMFILE :
#        emsg = "encp unix to remote hsm  is not implemented."
#        done_ticket = {'status':("USERERROR", emsg)}
        #done_ticket, work_list = write_to_hsm(intf, tinfo)
        done_ticket = write_to_hsm(intf, tinfo)

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



def do_work(intf, main=main):

    #Keep multiple encps within a migration in sync.
    #Trace.log(98, "--- start_lock.acquire(), TP #0, before")
    start_lock.acquire()
    #Trace.log(98, ">>> start_lock.acquire(), TP #0, got lock")

    try:
        exit_status = main(intf)
    except (SystemExit, KeyboardInterrupt):
        exc, msg = sys.exc_info()[:2]
        message = "encp aborted from: %s: %s" % (str(exc), str(msg))
        Trace.log(e_errors.ERROR, message)

        exit_status = 1

        err_msg_lock.acquire()
        err_msg[thread.get_ident()] = message
        err_msg_lock.release()
    except:
        #Get the uncaught exception.
        exc, msg, tb = sys.exc_info()
        ticket = {'status' : (e_errors.UNCAUGHT_EXCEPTION,
                              "%s: %s" % (str(exc), str(msg)))}
        message = str(ticket['status'])

        #Print the data access layer and send the information to the
        # accounting server (if possible).
        print_data_access_layer_format(None, None, None, ticket)
        #Send to the log server the traceback dump.  If unsuccessful,
        # print the traceback to standard error.
        Trace.handle_error(exc, msg, tb)
        del tb #No cyclic references.

        exit_status = 1

        err_msg_lock.acquire()
        err_msg[thread.get_ident()] = message
        err_msg_lock.release()

    #Keep multiple encps within a migration in sync.
    try:
        #Trace.log(98, "<<< start_lock.release(), TP #1 main")
        start_lock.release()
    except:
        pass

    file_utils.acquire_lock_euid_egid()

    #Remove any zero-length files left haning around.
    delete_at_exit.delete()

    #The only thing that would be effected by not setting this back would be
    if os.getuid() == 0 and os.geteuid() != 0:
        try:
            os.seteuid(0)
            os.setegid(0)
        except:
            pass

    file_utils.release_lock_euid_egid()

    return exit_status

#If mode = 0 it means admin, 1 means user, 2 means dcache.
#
def start(mode, do_work=do_work, main=main, Interface=EncpInterface):
    if mode not in [0, 1, 2]:
        #Some ways of running encp (or get) allow for encp to return 2 which
        # indicates a non-retriable error.  Even for the normal encp return
        # 2 since this should never happen.
        return 2

    delete_at_exit.setup_signal_handling()

    try:
        intf = Interface(user_mode=mode)
    except (KeyboardInterrupt, SystemExit):
        return 1
    except:
        try:
            Trace.handle_error()
        except:
            pass
        #Some ways of running encp (or get) allow for encp to return 2 which
        # indicates a non-retriable error.  Having the interface class fail
        # should never happen.
        return 2

    try:
        return do_work(intf, main)
    except (KeyboardInterrupt, SystemExit):
        return 1
    except:
        try:
            Trace.handle_error()
        except:
            pass
        #Some ways of running encp (or get) allow for encp to return 2 which
        # indicates a non-retriable error.  Catching an exception outside of
        # do_work() should never happen.
        return 2

if __name__ == '__main__':
    delete_at_exit.quit(start(0))  #0 means admin
