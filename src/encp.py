#!/usr/bin/env python
#
# $Id$
#

#############################################################################
# Environmental variables that effect encp.
#
# $ENSTORE_CONFIG_HOST = The hostname/ip of the configuration server.
# $ENSTORE_CONFIG_PORT = The port number of the configuration server.
# $ENSTORE_SPECIAL_LIB = Override the library manager to use.  Use with care.
#                        Its original purpose was to use a migration LM
#                        for the 9940A to 9940B conversion.
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
import pdb
import string
import traceback
import select
import signal
import random
import fcntl
if sys.version_info < (2, 2, 0):
    import FCNTL #FCNTL is depricated in python 2.2 and later.
    fcntl.F_GETFL = FCNTL.F_GETFL
    fcntl.F_SETFL = FCNTL.F_SETFL
import math
import exceptions
import re
import statvfs
import types

# enstore modules
import setpath 
import Trace
import pnfs
import callback
import log_client
import alarm_client
import configuration_client
import udp_server
import EXfer
import interface
import option
import e_errors
import hostaddr
import host_config
import atomic
import library_manager_client
import delete_at_exit
import runon
import enroute
import charset
import volume_family
import volume_clerk_client
import file_clerk_client
import enstore_constants
import enstore_functions2
import accounting_client

# forward declaration. It is assigned in get_clerks()
acc = None
__csc = None

#Constants for the max file size.  Currently this assumes the max for the
# cpio_odc wrapper format.  The -1s are necessary since that is the size
# that fits in signed integer variables.
ONE_G = 1024 * 1024 * 1024
TWO_G = 2 * long(ONE_G) - 1     #Used in int32()
MAX_FILE_SIZE = long(ONE_G) * 2 - 1    # don't get overflow

#############################################################################
# verbose: Roughly, five verbose levels are used.
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

#This is the global used by print_data_access_layer_format().  It uses it to
# determine whether standard out or error is used.
data_access_layer_requested = 0

#Initial seed for generate_unique_id().
_counter = 0
#Initial seed for generate_unique_msg_id().
_msg_counter = 0

# int32(v) -- if v > 2^31-1, make it long
#
# a quick fix for those 64 bit machine that consider int is 64 bit ...

def int32(v):
    if v > TWO_G:
        return long(v)
    else:
        return v
    
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

        self.ticket = e_ticket

        #Generate the string that stringifying this obeject will give.
        self._string()

        #Is this duplicated from calling Exception.__init__(self)?
        #self.args = [self.errno, self.message, self.type]

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
    
############################################################################

def encp_client_version():
    ##this gets changed automatically in {enstore,encp}Cut
    ##You can edit it manually, but do not change the syntax
    version_string = "v3_1  CVS $Revision$ "
    file = globals().get('__file__', "")
    if file: version_string = version_string + file
    return version_string

def quit(exit_code=1):
    delete_at_exit.quit(exit_code)
    #delete_at_exit.delete()
    #os._exit(exit_code)

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

############################################################################

def is_brand(brand):
    if type(brand) == types.StringType:
        if len(brand) == 4 and brand.isalnum():
            return 1

    return 0

def is_bfid(bfid):

    if type(bfid) == types.StringType:
        #The only part of the bfid that is of constant form is that the last
        # n characters are all digits.  There are no restrictions on what
        # is in a brand or how long it can be (though there should be).
        # Since, the bfid is based on its creation time, as time passes the
        # number of digits in a bfid will grow.  (Assume 14 as minumum).
        #
        #Encp can only handle these problems if the first four characters
        # are the brand and are only alphanumeric.  The remaining characters
        # must only be digits.
        #
        #Older files that do not have bfid brands should only be digits.
        
        if len(bfid) >= 18 and is_brand(bfid[:4]) and bfid[4:].isdigit():
            return 1
        elif len(bfid) >= 14 and bfid.isdigit():
            return 1
            
    return 0

def is_volume(volume):
    if type(volume) == types.StringType:
        if (len(volume) == 6 and \
            re.compile("^[A-Z]{2}[A-Z0-9]{2}[0-9]{2}")):
            return 1   #If passed a volume.
        elif (len(volume) == 8 and \
            re.compile("^[A-Z]{2}[A-Z0-9]{2}[0-9]{2}(L)[12]")):
            return 1   #If passed a volume.

    return 0

def is_location_cookie(lc):
    if type(lc) == types.StringType:
        #For tapes and null volumes.
        tape_regex = re.compile("^[0-9]{4}(_)[0-9]{9}(_)[0-9]{7}$")
        disk_regex = re.compile("^[/0-9A-Za-z_]*(//)[/0-9A-Za-z_]*(:)[0-9]*$")
        
        if (len(lc) == 22 and \
            tape_regex.match(lc)):
            return 1
        elif disk_regex.match(lc):
            return 1
    return 0

############################################################################

def extract_brand(bfid):

    if type(bfid) != types.StringType:
        return None
        
    #Newer files have brands.  See is_bfid() for comments.
    if len(bfid) >= 18:
        if is_brand(bfid[:4]) and bfid[4:].isdigit():
            return bfid[:4]
        else:
            return None
    #Older files may not have a brand.
    elif len(bfid) >= 14:
        if bfid.isdigit():
            return ""

    return None

def extract_file_number(location_cookie):

    if type(location_cookie) != types.StringType:
        return None

    if is_location_cookie(location_cookie):
        try:
            #Return just third integer portion of the string.
            return int(string.split(location_cookie, "_")[2])
        except KeyboardInterrupt:
            raise sys.exc_info()
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
		apply(getattr(fd, "close"))
	    except (OSError, IOError):
		pass
        else:
            try:
                os.close(fd)
            except OSError:
                sys.stderr.write("Unable to close fd %s.\n" % fd)

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

def get_file_size(file):
    if file[:6] == "/pnfs/":
        #Get the remote pnfs filesize.
        try:
            pin = pnfs.Pnfs(file)
            pin.get_file_size()
            filesize = pin.file_size
        except (OSError, IOError), detail:
            filesize = None #0
    else:
        try:
            statinfo = os.stat(file)
            filesize = statinfo[stat.ST_SIZE]
        except (OSError, IOError), detail:
            filesize = None #0

    #Return None for failures.
    if filesize != None:
        #Always return the long version to avoid 32bit vs 64bit problems.    
        filesize = long(filesize)
    
    return filesize

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
        os.utime(input_path, (time_now, os.stat(input_path)[stat.ST_MTIME]))
    except OSError:
        pass

    try:
        os.utime(output_path, (os.stat(input_path)[stat.ST_ATIME], time_now))
    except OSError:
        pass #This one will fail if the output file is /dev/null.

def bin(integer):
    if type(integer) != types.IntType:
        print

    temp = integer
    list = []
        
    for i in range(32):
        list.append(temp % 2)
        temp = (temp >> 1)

    list.reverse()

    temp = ""
    for i in list:
        temp = temp + ("%s" % i)

    return temp

#Take as parameter the interface class instance or a request ticket.  Determine
# if the transfer(s) is/are a read or not.
def is_read(ticket_or_interface):
    #If the type is a dictionary...
    if type(ticket_or_interface) == types.DictionaryType:
        infile = ticket_or_interface.get('infile', "")
        outfile = ticket_or_interface.get('outfile', "")
        if infile[:6] == "/pnfs/" and outfile[:6] != "/pnfs/":
            return 1
        elif infile[:6] != "/pnfs/" and outfile[:6] == "/pnfs/":
            return 0
        else:
            raise EncpError(errno.EINVAL, "Inconsistant file types.",
                            e_errors.BROKEN)
    #If the type is an interface class...
    elif type(ticket_or_interface) == types.InstanceType:
        intype = getattr(ticket_or_interface, 'intype', "")
        outtype = getattr(ticket_or_interface, 'outtype', "")
        if intype == "hsmfile" and outtype == "unixfile":
            return 1
        elif intype == "unixfile" and outtype == "hsmfile":
            return 0
        else:
            raise EncpError(errno.EINVAL, "Inconsistant file types.",
                            e_errors.BROKEN)
    #Have no idea what was passed in.
    else:
        raise EncpError(errno.EINVAL, "Expected ticket or interface.",
                        e_errors.WRONGPARAMETER)

#Take as parameter the interface class instance or a request ticket.  Determine
# if the transfer(s) is/are a write or not.
def is_write(ticket_or_interface):
    return not is_read(ticket_or_interface)

############################################################################

def get_enstore_pnfs_path(filepath):
    #Make sure this is a string.
    if type(filepath) != types.StringType:
        raise EncpError(None, "Expected string filename.",
                        e_errors.WRONGPARAMETER)

    #Make absolute path.
    machine, filename, dirname, basename = fullpath(filepath)

    #Determine the canonical path base.
    canonical_name = string.join(socket.getfqdn().split(".")[1:], ".")
    canonical_pathbase = os.path.join("/pnfs", canonical_name, "usr")

    #Return an error if the file is not a pnfs filename.
    if dirname[:6] != "/pnfs/":
        raise EncpError(None, "Not a pnfs filename.", e_errors.WRONGPARAMETER)

    if dirname[:13] == "/pnfs/fs/usr/":
        return "/pnfs/" + filename[13:]
    elif dirname[:19] == canonical_pathbase:
        return "/pnfs/" + filename[19:]
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
    machine, filename, dirname, basename = fullpath(filepath)

    #Determine the canonical path base.
    canonical_name = string.join(socket.getfqdn().split(".")[1:], ".")
    canonical_pathbase = os.path.join("/pnfs", canonical_name, "usr")
    
    #Return an error if the file is not a pnfs filename.
    if dirname[:6] != "/pnfs/":
        raise EncpError(None, "Not a pnfs filename.", e_errors.WRONGPARAMETER)

    if dirname[:13] == "/pnfs/fs/usr/":
        return filename
    elif dirname[:19] == canonical_pathbase:  #i.e. "/pnfs/fnal.gov/usr/"
        return "/pnfs/fs/usr/" + filename[19:]
    elif dirname[:6] == "/pnfs/":
        return "/pnfs/fs/usr/" + filename[6:]
    else:
        raise EncpError(None, "Unable to return enstore pnfs pathname.",
                        e_errors.WRONGPARAMETER)

def get_enstore_canonical_path(filepath):
    #Make sure this is a string.
    if type(filepath) != types.StringType:
        raise EncpError(None, "Expected string filename.",
                        e_errors.WRONGPARAMETER)

    #Make absolute path.
    machine, filename, dirname, basename = fullpath(filepath)

    #Determine the canonical path base.  If the ENCP_CANONICAL_DOMAINNAME
    # overriding environmental variable is set, use that.
    if os.environ.get('ENCP_CANONICAL_DOMAINNAME', None):
        canonical_name = os.environ['ENCP_CANONICAL_DOMAINNAME']
    else:
        canonical_name = string.join(socket.getfqdn().split(".")[1:], ".")
    #Use the canonical_name to determine the canonical pathname base.
    canonical_pathbase = os.path.join("/pnfs", canonical_name, "usr")

    #Return an error if the file is not a pnfs filename.
    if dirname[:6] != "/pnfs/":
        raise EncpError(None, "Not a pnfs filename.", e_errors.WRONGPARAMETER)

    if dirname[:19] == canonical_pathbase: #i.e. "/pnfs/fnal.gov/usr/"
        return filename
    elif dirname[:13] == "/pnfs/fs/usr/":
        return os.path.join(canonical_pathbase, filename[13:])
    elif dirname[:6] == "/pnfs/":
        return os.path.join(canonical_pathbase, filename[6:])
    else:
        raise EncpError(None, "Unable to return enstore pnfs pathname.",
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
    except OSError, detail:
        return 0

    # Need to check for each type of access permission.

    if mode == os.F_OK:
        # In order to get this far, the file must exist.
        return 1

    elif mode == os.R_OK:  #Check for read permissions.
        #If the user is user root.
        if os.geteuid() == 0:
            return 1
        #Anyone can read this file.
        elif (stat_mode & stat.S_IROTH):
            return 1
        #This is the files owner.
        elif (stat_mode & stat.S_IRUSR) and \
           file_stats[stat.ST_UID] == os.geteuid():
            return 1
        #The user has group access.
        elif (stat_mode & stat.S_IRGRP) and \
           (file_stats[stat.ST_GID] == os.geteuid() or
            file_stats[stat.ST_GID] in os.getgroups()):
            return 1
        else:
            return 0

    elif mode == os.W_OK:  #Check for write permissions.
        #If the user is user root.
        if os.geteuid() == 0:
            return 1
        #Anyone can write this file.
        elif (stat_mode & stat.S_IWOTH):
            return 1
        #This is the files owner.
        elif (stat_mode & stat.S_IWUSR) and \
           file_stats[stat.ST_UID] == os.geteuid():
            return 1
        #The user has group access.
        elif (stat_mode & stat.S_IWGRP) and \
           (file_stats[stat.ST_GID] == os.geteuid() or
            file_stats[stat.ST_GID] in os.getgroups()):
            return 1
        else:
            return 0
    
    elif mode == os.X_OK:  #Check for execute permissions.
        #If the user is user root.
        if os.geteuid() == 0:
            return 1
        #Anyone can execute this file.
        elif (stat_mode & stat.S_IXOTH):
            return 1
        #This is the files owner.
        elif (stat_mode & stat.S_IXUSR) and \
           file_stats[stat.ST_UID] == os.geteuid():
            return 1
        #The user has group access.
        elif (stat_mode & stat.S_IXGRP) and \
           (file_stats[stat.ST_GID] == os.geteuid() or
            file_stats[stat.ST_GID] in os.getgroups()):
            return 1
        else:
            return 0
        
    else:
        return 0
    
############################################################################

def _get_csc_from_volume(volume): #Should only be called from get_csc().

    # get a configuration server
    config_host = enstore_functions2.default_host()
    config_port = enstore_functions2.default_port()
    csc = configuration_client.ConfigurationClient((config_host,config_port))
    vcc = volume_clerk_client.VolumeClerkClient(csc)

    #Before checking other systems, check the current system.
    if e_errors.is_ok(vcc.inquire_vol(volume)):
        return csc
    
    print "Multi-systems reads not implimented yet for volume reads."
    sys.exit(0)
    return None #Make pychecker quiet.

#The string brand could be either a bfid or brand.  This is because a brand
# can be of arbitrary length making it impossible to know how much of it is
# the numerical part (which can change size as time grows) or the brand part.
def _get_csc_from_brand(brand): #Should only be called from get_csc().
    global __csc

    # get a configuration server
    config_host = enstore_functions2.default_host()
    config_port = enstore_functions2.default_port()
    csc = configuration_client.ConfigurationClient((config_host,config_port))
    fcc = file_clerk_client.FileClient(csc)

    #There is no brand, since the file is too old.
    if not brand:
	return csc
    #Before checking other systems, check the current system.
    fcc_brand = fcc.get_brand()
    if not is_brand(fcc_brand):
        Trace.log(e_errors.WARNING,
                  "File clerk (%s) returned invalid brand: %s\n"
                  % (fcc.server_address, fcc_brand))
    elif brand[:len(fcc_brand)] == fcc_brand:
        return csc

    #Get the list of all config servers and remove the 'status' element.
    config_servers = csc.get('known_config_servers', {})
    if e_errors.is_ok(config_servers['status']):
        del config_servers['status']
    else:
        return csc

    #Check the last used non-default brand for performance reasons.
    if __csc != None:
        test_fcc = file_clerk_client.FileClient(__csc)
        test_brand = test_fcc.get_brand()
        if not is_brand(test_brand):
            Trace.log(e_errors.WARNING,
                      "File clerk (%s) returned invalid brand: %s\n"
                      % (test_fcc.server_address, test_brand))
        if brand[:len(test_brand)] == test_brand:
            return __csc
    
    #Loop through systems for the brand that matches the one we're looking for.
    for server in config_servers.keys():
        try:
            #Get the next configuration client.
            csc_test = configuration_client.ConfigurationClient(
                config_servers[server])

            #Get the next file clerk client and its brand.
            fcc_test = file_clerk_client.FileClient(csc_test,
                                                    rcv_timeout=5, rcv_tries=2)

            system_brand = fcc_test.get_brand()
            if not is_brand(system_brand):
                Trace.log(e_errors.WARNING,
                          "File clerk (%s) returned invalid brand: %s\n"
                          % (fcc_test.server_address, system_brand))
            #If things match then use this system.
            if brand[:len(system_brand)] == system_brand:
                if fcc.get_brand() != system_brand:
                    msg = "Using %s based on brand %s." % (system_brand, brand)
                    Trace.log(e_errors.INFO, msg)

                __csc = csc_test  #Set global for performance reasons.
                return __csc
        except KeyboardInterrupt:
            raise sys.exc_info()
        except:
            exc, msg = sys.exc_info()[:2]
            Trace.log(e_errors.WARNING, str((str(exc), str(msg))))

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
def get_csc(parameter=None):
    global __csc  #For remembering.

    #Set some default values.
    bfid = None
    volume = None
    
    # get a configuration server
    config_host = enstore_functions2.default_host()
    config_port = enstore_functions2.default_port()
    csc = configuration_client.ConfigurationClient((config_host,config_port))
    #fcc = file_clerk_client.FileClient(csc)
    
    #Due to branding we need to figure out which system is the correct one.
    # Should only matter for reads.  On a success, the variable 'brand' should
    # contain the brand of the bfid to be used in the rest of the function.
    # On error return the default configuration server client (csc).
    if not parameter:
        return csc
    
    elif type(parameter) == types.DictType: #If passed a ticket with bfid.
        bfid = parameter.get('fc', {}).get("bfid", "")
        volume = parameter.get('external_label', "")

    elif is_bfid(parameter):  #If passed a bfid.
        bfid = parameter

    elif is_volume(parameter):  #If passed a volume.
        volume = parameter

    #Call the correct version of the underlying get_csc functions.    
    if bfid:  #If passed a bfid.
        brand = extract_brand(bfid)
        return _get_csc_from_brand(brand)
    
    elif volume:  #If passed a volume.
        return _get_csc_from_volume(volume)

    return csc  #Nothing valid, just return the default csc.

############################################################################
            
def max_attempts(library, encp_intf):
    #Determine how many times a transfer can be retried from failures.
    #Also, determine how many times encp resends the request to the lm
    # and the mover fails to call back.
    if library[-17:] == ".library_manager":
        lib = library
    else:
        lib = library + ".library_manager"

    # get a configuration server
    csc = get_csc()
    lm = csc.get(lib, 5, 5)

    #Due to the possibility of branding, check other systems.

    if lm['status'][0] == e_errors.KEYERROR:
        #If we get here, then check the other enstore config servers.
        # be prepared to find the correct system if necessary.
        kcs = csc.get("known_config_servers", 5, 5)
        kcs[socket.gethostname()] = (socket.getfqdn(),
                              int(os.environ.get('ENSTORE_CONFIG_PORT', 7500)))
        for item in kcs.values():
            _csc = configuration_client.ConfigurationClient(address = item)
            lm = _csc.get(lib, 5, 5)
            if e_errors.is_ok(lm):
                break

    #If the library does not have the following entries (or the library name
    # was not found in config file(s)... very unlikely) then go with
    # the defaults.

    if encp_intf.max_retry == None:
        encp_intf.max_retry = lm.get('max_encp_retries',
                                     enstore_constants.DEFAULT_ENCP_RETRIES)
    if encp_intf.max_resubmit == None:
        encp_intf.max_resubmit = lm.get('max_encp_resubmits',
                                enstore_constants.DEFAULT_ENCP_RESUBMITIONS)

############################################################################

def print_data_access_layer_format(inputfile, outputfile, filesize, ticket):
    # check if all fields in ticket present
    fc_ticket = ticket.get('fc', {})
    external_label = fc_ticket.get('external_label', '')
    location_cookie = fc_ticket.get('location_cookie', ticket.get('volume',''))
    mover_ticket = ticket.get('mover', {})
    device = mover_ticket.get('device', '')
    device_sn = mover_ticket.get('serial_num','')
    time_ticket = ticket.get('times', {})
    transfer_time = time_ticket.get('transfer_time', 0)
    seek_time = time_ticket.get('seek_time', 0)
    mount_time = time_ticket.get('mount_time', 0)
    in_queue = time_ticket.get('in_queue', 0)
    now = time.time()
    total = now - time_ticket.get('encp_start_time', now)
    sts =  ticket.get('status', ('Unknown', None))
    status = sts[0]
    msg = sts[1:]
    if len(msg)==1:
        msg=msg[0]
    paranoid_crc = ticket.get('ecrc', None)
    encp_crc = ticket.get('exfer', {}).get('encp_crc', None)
    mover_crc = ticket.get('fc', {}).get('complet_crc', None)
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
    if not filesize and ticket.get('filesize', None):
        filesize = ticket['filesize']
        
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

#######################################################################

def check_server(csc, server_name):

    # send out an alive request - if config not working, give up
    rcv_timeout = 10
    alive_retries = 360
    
    try:
        stati = csc.alive(server_name, rcv_timeout, alive_retries)
    except KeyboardInterrupt:
        raise sys.exc_info()
    except:
        exc, msg = sys.exc_info()[:2]
        stati={}
        stati["status"] = (e_errors.BROKEN,
                           "Unexpected error (%s, %s) while attempting to"
                           "contact %s." % (str(exc), str(msg), server_name))

    #Quick translation for TIMEDOUT error.  The description part of the
    # error is None by default, this puts something there.
    if stati['status'][0] == e_errors.TIMEDOUT:
        stati['status'] = (e_errors.TIMEDOUT,
                           "Unable to contact %s." % server_name)

    return stati

# get the configuration client and udp client and logger client
# return some information about who we are so it can be used in the ticket

def clients():

    # get a configuration server client
    csc = get_csc()

    #This group of servers must be running to allow the transfer to
    # succed.  The library manager is not checked (now anyway) because we
    # don't know which one it is.
    for server in [configuration_client.MY_SERVER,
                   volume_clerk_client.MY_SERVER,
                   file_clerk_client.MY_SERVER]:
        Trace.message(CONFIG_LEVEL, "Contacting %s." % server)

        ticket = check_server(csc, server)

        #Handle the fatal error.
        if not e_errors.is_ok(ticket['status']):
            Trace.alarm(e_errors.ERROR, ticket['status'][0], ticket)
            print_data_access_layer_format("", "", 0, ticket)
            quit()

        Trace.message(CONFIG_LEVEL, "Server %s found at %s." %
                      (server, ticket.get('address', "Unknown")))
    
    # get a logger client
    logc = log_client.LoggerClient(csc, 'ENCP', 'log_server')

    #global client #should not do this
    client = {}
    client['csc']=csc
    client['logc']=logc
    
    return client

##############################################################################

def get_callback_addr(encp_intf, ip=None):
    # get a port to talk on and listen for connections
    (host, port, listen_socket) = callback.get_callback(
        verbose=encp_intf.verbose, ip=ip)
    callback_addr = (host, port)
    listen_socket.listen(4)

    Trace.message(CONFIG_LEVEL,
                  "Listening for mover(s) to call back on (%s, %s)." %
                  callback_addr)

    return callback_addr, listen_socket

def get_routing_callback_addr(encp_intf, udps=None):
    # get a port to talk on and listen for connections
    if udps == None:
        udps = udp_server.UDPServer(None,
                                    receive_timeout=encp_intf.mover_timeout)
    else:
	addr = udps.server_address
	udps.__del__()  #Close file descriptors and such.
        udps.__init__(addr, receive_timeout=encp_intf.mover_timeout)
        #In the unlikely event that the port is taken by some other process
        # between the two functions above, obtain a new port.  This can
        # cause some timeout errors, but that is life.
        if udps.server_socket == None:
            udps.__init__(None, receive_timeout=encp_intf.mover_timeout)
                          
    route_callback_addr = (udps.server_address[0], udps.server_address[1])
    
    Trace.message(CONFIG_LEVEL,
                  "Listening for mover(s) to send route back on (%s, %s)." %
                  route_callback_addr)

    return route_callback_addr, udps

##############################################################################

# for automounted pnfs

pnfs_is_automounted = 0

# access_check(path, mode) -- a wrapper for os.access() that retries for
#                             automatically mounted file system

def access_check(path, mode):

    # if pnfs is not auto mounted, simply call os.access
    if not pnfs_is_automounted:
        #use the effective ids and not the reals used by os.access().
        return e_access(path, mode)

    # automaticall retry 6 times, one second delay each
    for i in range(5):
        if e_access(path, mode):
            return 1
        time.sleep(1)
    return e_access(path, mode)

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
def filesystem_check(target_filesystem, inputfile):

    #Get filesize
    try:
        size = get_file_size(inputfile)
    except KeyboardInterrupt:
        raise sys.exc_info()
    except (OSError, IOError):
        exc, msg = sys.exc_info()[:2]
        raise EncpError(getattr(msg,"errno",None), str(msg), e_errors.OSERROR)
        
    #os.pathconf likes its targets to exist.  If the target is not a directory,
    # use the parent directory.
    if not os.path.isdir(target_filesystem):
        target_filesystem = os.path.split(target_filesystem)[0]

    #Not all operating systems support this POSIX limit yet (ie OSF1).
    try:
        #get the maximum filesize the local filesystem allows.
        bits = os.pathconf(target_filesystem,
                           os.pathconf_names['PC_FILESIZEBITS'])
    except KeyboardInterrupt:
        raise sys.exc_info()
    except KeyError, detail:
        return
    except (OSError, IOError):
        exc, msg = sys.exc_info()[:2]
        msg2 = "System error obtaining maximum file size for " \
               "filesystem %s." % (target_filesystem,)
        Trace.log(e_errors.ERROR, str(msg) + ": " + msg2)
        if getattr(msg, "errno", None) == errno.EINVAL:
            sys.stderr.write("WARNING: %s  Continuing." % (msg2,))
            return  #Nothing to test, user needs to be carefull.
        else:
            raise EncpError(getattr(msg,"errno",None), msg2, e_errors.OSERROR)

    filesystem_max = 2L**(bits - 1) - 1

    #Normally, encp would find this an error, but "Get" may not.  Raise
    # an exception and let the caller decide.
    if size == None:
        raise EncpError(None, "Filesize is not known.", e_errors.OSERROR)
    #Compare the max sizes.
    elif size > filesystem_max:
        raise EncpError(errno.EFBIG,
                        "Filesize (%s) larger than filesystem allows (%s)." \
                        % (size, filesystem_max),
                        e_errors.USERERROR)

#Make sure that the wrapper can handle the filesize.
def wrappersize_check(target_filepath, inputfile):

    #Get filesize
    try:
        size = get_file_size(inputfile)

        #Get the remote pnfs wrapper.  If the maximum size of the
        # wrapper isn't in the configuration file, assume 2GB-1.
        pout = pnfs.Tag((os.path.dirname(target_filepath)))
        pout.get_file_family_wrapper()
        # get a configuration server and the max filesize the wrappers allow.
        csc = get_csc()
        wrappersizes = csc.get('wrappersizes', {})
        wrapper_max = wrappersizes.get(pout.file_family_wrapper,
                                       MAX_FILE_SIZE)
    except KeyboardInterrupt:
        raise sys.exc_info()
    except (OSError, IOError):
        exc, msg = sys.exc_info()[:2]
        raise EncpError(getattr(msg,"errno",None), str(msg), e_errors.OSERROR)

    if size > wrapper_max:
        raise EncpError(errno.EFBIG,
                        "Filesize (%s) larger than wrapper (%s) allows (%s)." \
                        % (size, pout.file_family_wrapper, wrapper_max),
                        e_errors.USERERROR)
    
#Make sure that the library can handle the filesize.
def librarysize_check(target_filepath, inputfile):

    #Get filesize
    try:
        size = get_file_size(inputfile)

        #Determine the max allowable size for the given library.
        pout = pnfs.Tag(os.path.dirname(target_filepath))
        pout.get_library()
        csc = get_csc()
        library = csc.get(pout.library + ".library_manager", {})
        library_max = library.get('max_file_size', MAX_FILE_SIZE)
    except KeyboardInterrupt:
        raise sys.exc_info()
    except (OSError, IOError):
        exc, msg = sys.exc_info()[:2]
        raise EncpError(getattr(msg,"errno",None), str(msg), e_errors.OSERROR)

    #Compare the max sizes allowed for these various conditions.
    if size > library_max:
        raise EncpError(errno.EFBIG,
                        "Filesize (%s) larger than library (%s) allows (%s)." \
                        % (size, pout.library, library_max),
                        e_errors.USERERROR)

# check the input file list for consistency
def inputfile_check(input_files, e):
    # create internal list of input unix files even if just 1 file passed in
    if type(input_files)==types.ListType:
        inputlist=input_files
    else:
        inputlist = [input_files]

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
            
            # input files must exist - also handle automounting.
            if not access_check(inputlist[i], os.F_OK):

                #We don't want to fail immediatly.  On reads it is ok for
                # encp to check all three paths to the experiment's file:
                # /pnfs/xyz, /pnfs/fs/usr/xyz and /pnfs/fnal.gov/xyz.

                if not pnfs.is_pnfs_path(inputlist[i]): #Excludes writes first.
                    raise EncpError(errno.ENOENT, inputlist[i],
                                    e_errors.USERERROR)
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
                                    e_errors.USERERROR)

            # input files must have read permissions.
            if not access_check(inputlist[i], os.R_OK):
                raise EncpError(errno.EACCES, inputlist[i], e_errors.USERERROR)

            #Since, the file exists, we can get its stats.
            statinfo = os.stat(inputlist[i])

            # input files can't be directories
            if not stat.S_ISREG(statinfo[stat.ST_MODE]):
                raise EncpError(errno.EISDIR, inputlist[i], e_errors.USERERROR)

            #For Reads make sure the filesystem size and the pnfs size match.
            if e.intype == "hsmfile":  #inputlist[i][:6] == "/pnfs/":
                p = pnfs.Pnfs(inputlist[i])
                #If sizes are different, raises OSError exception.
                p.get_file_size()

            # we cannot allow 2 input files to be the same
            # this will cause the 2nd to just overwrite the 1st
            try:
                match_index = inputlist[:i].index(inputlist[i])
                
                raise EncpError(None,
                                'Duplicate entry %s'%(inputlist[match_index],),
                                e_errors.USERERROR)
            except ValueError:
                pass  #There is no error.

        except KeyboardInterrupt:
            raise sys.exc_info()
        except EncpError, detail:
            exc, msg = sys.exc_info()[:2]
            size = get_file_size(inputlist[i])
            print_data_access_layer_format(inputlist[i], "", size,
                                           {'status':(msg.type, msg.strerror)})
            quit()
        except (OSError, IOError), detail:
            exc, msg = sys.exc_info()[:2]
            size = get_file_size(inputlist[i])
            error = errno.errorcode.get(getattr(msg, "errno", None),
                                        errno.errorcode[errno.ENODATA])
            print_data_access_layer_format(
                inputlist[i], "", size, {'status':(error, str(msg))})
            quit()

    return

# check the output file list for consistency
# generate names based on input list if required
#"inputlist" is the list of input files.  "output" is a list with one element.

def outputfile_check(inputlist, outputlist, e):

    dcache = e.put_cache #being lazy

    # create internal list of input unix files even if just 1 file passed in
    if type(inputlist)==types.ListType:
        inputlist = inputlist
    else:
        inputlist = [inputlist]

    # create internal list of input unix files even if just 1 file passed in
    if type(outputlist)==types.ListType:
        outputlist = outputlist
    else:
        outputlist = [outputlist]

    # Make sure we can open the files. If we can't, we bomb out to user
    for i in range(len(inputlist)):

        #If output location is /dev/null, skip the checks.
        if outputlist[i] == "/dev/null":
            continue

        try:

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

                directory = os.path.dirname(outputlist[i])
                
                #Check for existance and write permissions to the directory.
                if not access_check(directory, os.F_OK):
                    raise EncpError(errno.ENOENT, directory,
                                    e_errors.USERERROR)

                if not os.path.isdir(directory):
                    raise EncpError(errno.ENOTDIR, directory,
                                    e_errors.USERERROR)
                                        
                if not access_check(directory, os.W_OK):
                    raise EncpError(errno.EACCES, directory,
                                    e_errors.USERERROR)

                #Looks like the file is good.
                outputlist.append(outputlist[i])
                
            #File exists when run by a normal user.
            elif access_check(outputlist[i], os.F_OK) and not dcache:
                raise EncpError(errno.EEXIST, outputlist[i],e_errors.USERERROR)

            #The file does not already exits and it is a dcache transfer.
            elif not access_check(outputlist[i], os.F_OK) and dcache:
                #Check if the filesystem is corrupted.  This entails looking
                # for directory entries without valid inodes.
                directory_listing = os.listdir(os.path.dirname(outputlist[i]))
                if os.path.basename(outputlist[i]) in directory_listing:
                    raise EncpError(
                        getattr(errno, 'EFSCORRUPTED', getattr(errno, "EIO")),
                                    "Filesystem is corrupt.", e_errors.OSERROR)
                else:
                    raise EncpError(errno.ENOENT, outputlist[i],
                                    e_errors.USERERROR)

            #The file exits, as it should, for a dache transfer.
            elif access_check(outputlist[i], os.F_OK) and dcache:
                #Before continuing lets check to see if layers 1 and 4 are
                # empty first.  This check is being added because it appears
                # that the dcache can (and has) written multiple copies of
                # the same file to Enstore.  The filesize being zero trick
                # doesn't work with dcache, since the dcache sets the filesize.
                p = pnfs.Pnfs(outputlist[i])
                try:
                    layer1 = p.readlayer(1)
                    layer4 = p.readlayer(4)

                    #Test if the layers are empty.
                    if layer1 != [] or layer4 != []:
                        #The layers are not empty.
                        raise EncpError(errno.EEXIST,
                                        "Layer 1 and layer 4 are already set.",
                                        e_errors.PNFS_ERROR)
                    else:
                        #The layers are empty.
                        outputlist.append(outputlist[i])
                except (OSError, IOError), msg:
                    #Some other non-foreseen error has occured.
                    raise EncpError(msg.errno, str(msg), e_errors.PNFS_ERROR)
                except EncpError, msg:
                    raise msg

            else:
                raise EncpError(None,
                             "Failed outputfile check for: %s" % outputlist[i],
                                e_errors.UNKNOWN)

            #Make sure the output file system can handle a file as big as
            # the input file.  Also, make sure that the maximum size that
            # the wrapper and library use are greater than the filesize.
            # These function will raise an EncpError on an error.
            if e.intype == "hsmfile": #READS
                try:
                    filesystem_check(outputlist[i], inputlist[i])
                except EncpError, msg:
                    #If the volume is being read in, the filesize may not
                    # be known yet.  Don't fail these but continue to fail
                    # such errors if they are of this type for normal reads.
                    if msg.errno == None and msg.type == e_errors.OSERROR \
                       and e.volume:
                        pass  #Get ingest.
                    else:
                        raise msg
            else: #WRITES
                librarysize_check(outputlist[i], inputlist[i])
                wrappersize_check(outputlist[i], inputlist[i])
                #filesystem_check(outputlist[i], inputlist[i])

            # we cannot allow 2 output files to be the same
            # this will cause the 2nd to just overwrite the 1st
            # In principle, this is already taken care of in the
            # inputfile_check, but do it again just to make sure in case
            # someone changes protocol
            try:
                match_index = inputlist[:i].index(inputlist[i])
                raise EncpError(None,
                                'Duplicate entry %s'%(inputlist[match_index],),
                                e_errors.USERERROR)
            except ValueError:
                pass  #There is no error.

        except EncpError:
            exc, msg = sys.exc_info()[:2]
            size = get_file_size(inputlist[i])
            print_data_access_layer_format('', outputlist[i], size,
                                           {'status':(msg.type, msg.strerror)})
            quit()

#######################################################################

def create_zero_length_files(filenames):
    if type(filenames) != types.ListType:
        filenames = [filenames]

    #now try to atomically create each file
    for f in filenames:
        if f == "/dev/null":
            return
        try:
            fd = atomic.open(f, mode=0666) #raises OSError on error.

            os.close(fd)

        except OSError:
            exc, msg = sys.exc_info()[:2]
            error = errno.errorcode.get(getattr(msg, "errno", None),
                                        errno.errorcode[errno.ENODATA])
            print_data_access_layer_format('', f, 0,
                                           {'status': (error, str(msg))})

            quit()

#######################################################################

def file_check(e):
    # check the output pnfs files(s) names
    # bomb out if they exist already
    inputlist, file_size = inputfile_check(e.input, e)
    n_input = len(inputlist)

    Trace.message(TICKET_LEVEL, "file count=" + str(n_input))
    Trace.message(TICKET_LEVEL, "inputlist=" + str(inputlist))
    Trace.message(TICKET_LEVEL, "file_size=" + str(file_size))

    # check (and generate) the output pnfs files(s) names
    # bomb out if they exist already
    outputlist = outputfile_check(e.input, e.output, e)

    Trace.message(TICKET_LEVEL, "outputlist=" + str(outputlist))

    return n_input, file_size, inputlist, outputlist

#######################################################################

#Only one of bfid and volume should be specified at one time.
def get_clerks(bfid_or_volume=None):

    global acc

    #Snag the configuration server client for the system that contains the
    # file clerk where the file was stored.  This is determined based on
    # the bfid's brand.
    csc = get_csc(bfid_or_volume)

    #Don't pass a volume to the file clerk client.
    if is_volume(bfid_or_volume):
        bfid = None
    else:
        bfid = bfid_or_volume

    #Get the file clerk information.
    fcc = file_clerk_client.FileClient(csc, bfid)
    if not fcc:
        raise EncpError(errno.ENOPROTOOPT,
                        "File clerk not available",
                        e_errors.NET_ERROR)
        
    #Get the volume clerk information.
    vcc = volume_clerk_client.VolumeClerkClient(csc)
    if not vcc:
        raise EncpError(errno.ENOPROTOOPT,
                        "File clerk not available",
                        e_errors.NET_ERROR)

    # we only have the correct crc now (reads)
    acc = accounting_client.accClient(csc, logname = 'ENCP')

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

def get_pinfo(p):
    try:
        p.pstatinfo()
    except AttributeError:
        #This error can occur for volume reads with incomplete
        # metadata available.
        pinf = {"inode" : 0,
                 #"gid" : None,
                 #"gname" : None,
                 #"uid" : None,
                 #"uname" : None,
                 "major" : None,
                 "minor" : None,
                 "rmajor" : None,
                 "rminor" : None,
                 "mode" : None,
                 "pnfsFilename" : None,
                 }
        return pinf
    
    try:
        # get some debugging info for the ticket
        pinf = {}
        for k in [ 'pnfsFilename','gid', 'gname','uid', 'uname',
                   'major','minor','rmajor','rminor',
                   'mode','pstat', 'inode' ] :
            try:
                pinf[k]=getattr(p,k)
            except AttributeError:
                pinf[k]="None"
        return pinf

    except (OSError, IOError), msg:
        error = getattr(msg, "error", errno.EIO)
        raise EncpError(error, None, errno.errorcode[error])
    except (IndexError,), detail:
        raise EncpError(None, "Unable to obtain bfid.", e_errors.OSERROR)

def get_uinfo():
    uinfo = {}
    uinfo['uid'] = os.getuid()
    uinfo['gid'] = os.getgid()
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

    if e.outtype == "hsmfile": #writes
        remote_file = outputfile
        local_file = inputfile
    else: #reads
        local_file = outputfile
        remote_file = inputfile

    #These exist for reads and writes.
    finfo["fullname"] = local_file
    finfo["sanity_size"] = 65536
    finfo["size_bytes"] = get_file_size(inputfile)

    #Append these for writes.
    if e.outtype == "hsmfile": #writes
        t = pnfs.Tag(os.path.dirname(remote_file))
        finfo["type"] = t.get_file_family_wrapper()
        finfo['mode'] = os.stat(local_file)[stat.ST_MODE]
        finfo['mtime'] = int(time.time())
        
    return finfo

#This function takes as parameters...
def get_winfo(pinfo, uinfo, finfo):

    # create the wrapper subticket - copy all the user info 
    # into for starters
    wrapper = {}

    #store the file information in the the wrapper
    for key in finfo.keys():
        wrapper[key] = finfo[key]
    
    # store the pnfs information info into the wrapper
    for key in pinfo.keys():
        wrapper[key] = pinfo[key]

    # the user key takes precedence over the pnfs key
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
def get_ninfo(inputfile, outputfile, e):
    
    # get fully qualified name
    imachine, ifullname, idir, ibasename = fullpath(inputfile) #e.input[i])
    omachine, ofullname, odir, obasename = fullpath(outputfile) #e.output[0])
    # Add the name if necessary.
    if ofullname == "/dev/null": #if /dev/null is target, skip elifs.
        pass
    elif ifullname == "/dev/zero":
        pass
    elif (len(e.input) > 1) or \
         (len(e.input) == 1 and os.path.isdir(ofullname)):
        ofullname = os.path.join(ofullname, ibasename)
        omachine, ofullname, odir, obasename = fullpath(ofullname)

    return ifullname, ofullname

############################################################################
############################################################################

def open_routing_socket(route_server, unique_id_list, encp_intf):

    Trace.message(e_errors.INFO, "Waiting for routing callback.")

    route_ticket = None

    if not route_server:
        return None, None

    start_time = time.time()

    while(time.time() - start_time < encp_intf.mover_timeout):
        try:
            route_ticket = route_server.process_request()
        except socket.error, msg:
            Trace.log(e_errors.ERROR, str(msg))
            raise EncpError(msg.args[0], str(msg),
                            e_errors.NET_ERROR)

        #If route_server.process_request() fails it returns None.
        if not route_ticket:
            continue
        #If route_server.process_request() returns incorrect value.
        elif route_ticket == types.DictionaryType and \
             hasattr(route_ticket, 'unique_id') and \
             route_ticket['unique_id'] not in unique_id_list:
            continue
        #It is what we were looking for.
        else:
            break
    else:
        raise EncpError(errno.ETIMEDOUT,
                        "Mover did not call back.", e_errors.TIMEDOUT)

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
        ip = interface.get('ip')
        if ip and route_ticket.get('mover_ip', None):
	    #With this loop, give another encp 10 seconds to delete the route
	    # it is using.  After this time, it will be assumed that the encp
	    # died before it deleted the route.
	    start_time = time.time()
	    while(time.time() - start_time < 10):

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
		    if route_ticket['mover_ip'] == route['Destination']:
			break
		else:
		    break

		time.sleep(1)
	    
            try:
                #This is were the interface selection magic occurs.
                host_config.setup_interface(route_ticket['mover_ip'], ip)
            except (OSError, IOError, socket.error), msg:
                Trace.log(e_errors.ERROR, str(msg))
                raise EncpError(getattr(msg,"errno",None),
                                str(msg), e_errors.OSERROR)

    (route_ticket['callback_addr'], listen_socket) = \
				    get_callback_addr(encp_intf, ip=ip)
    route_server.reply_to_caller_using_interface_ip(route_ticket, ip)

    return route_ticket, listen_socket

##############################################################################

def open_control_socket(listen_socket, mover_timeout):

    Trace.message(INFO_LEVEL, "Waiting for mover to connect control socket")

    read_fds,write_fds,exc_fds=select.select([listen_socket], [], [],
                                             mover_timeout)

    #If there are no successful connected sockets, then select timedout.
    if not read_fds:
        raise EncpError(errno.ETIMEDOUT,
                        "Mover did not call back.", e_errors.TIMEDOUT)
    
    control_socket, address = listen_socket.accept()

    if not hostaddr.allow(address):
        control_socket.close()
        raise EncpError(errno.EPERM, "host %s not allowed" % address[0],
                        e_errors.NOT_ALWD_EXCEPTION)

    try:
        ticket = callback.read_tcp_obj(control_socket)
    except e_errors.TCP_EXCEPTION:
        raise EncpError(errno.EPROTO, "Unable to obtain mover responce",
                        e_errors.TCP_EXCEPTION)

    if not e_errors.is_ok(ticket):
        #If the mover already returned an error, don't bother checking if
        # it is closed already...
        return control_socket, address, ticket

    fds, junk, junk = select.select([control_socket], [], [], 5)
    try:
        if fds:
            ticket = callback.read_tcp_obj(control_socket)
    except e_errors.TCP_EXCEPTION:
        raise EncpError(errno.ENOTCONN,
                        "Control socket no longer usable after initalization.",
                        e_errors.TCP_EXCEPTION, ticket)

    return control_socket, address, ticket
    
##############################################################################

def open_data_socket(mover_addr, interface_ip):
    
    Trace.message(INFO_LEVEL, "Connecting data socket.")

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
        data_path_socket.bind((interface_ip, 0))
    except socket.error, msg:
        raise socket.error, msg

    try:
        data_path_socket.connect(mover_addr)
        error = 0
    except socket.error, msg:
        #We have seen that on IRIX, when the connection succeds, we
        # get an EISCONN error.
        if hasattr(errno, 'EISCONN') and msg[0] == errno.EISCONN:
            pass
        #The TCP handshake is in progress.
        elif msg[0] == errno.EINPROGRESS:
            pass
        #A real or fatal error has occured.  Handle accordingly.
        else:
            raise socket.error, msg

    #Check if the socket is open for reading and/or writing.
    r, w, ex = select.select([data_path_socket], [data_path_socket], [], 30)

    if r or w:
        #Get the socket error condition...
        rtn = data_path_socket.getsockopt(socket.SOL_SOCKET,
                                          socket.SO_ERROR)
        error = 0
    #If the select didn't return sockets ready for read or write, then the
    # connection timed out.
    else:
        raise socket.error, (errno.ETIMEDOUT, os.strerror(errno.ETIMEDOUT))

    #If the return value is zero then success, otherwise it failed.
    if rtn != 0:
        raise socket.error, (rtn, os.strerror(rtn))

    #Restore flag values to blocking mode.
    fcntl.fcntl(data_path_socket.fileno(), fcntl.F_SETFL, flags)

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

def mover_handshake(listen_socket, route_server, work_tickets, encp_intf):
    use_listen_socket = listen_socket
    ticket = {}
    config = host_config.get_config()
    unique_id_list = []
    for work_ticket in work_tickets:
        unique_id_list.append(work_ticket['unique_id'])
        
    ##19990723:  resubmit request after 15minute timeout.  Since the
    # unique_id is unchanged the library manager should not get
    # confused by duplicate requests.
    #timedout=0
    while 1:  ###not (timedout or reply_read):

        #Attempt to get the routing socket before opening the others
        try:
            #There is no need to do this on a non-multihomed machine.
            if config and config.get('interface', None):
                ticket, use_listen_socket = open_routing_socket(
		    route_server, unique_id_list, encp_intf)
        except (EncpError,), detail:
            exc, msg = sys.exc_info()[:2]
            #Return the entire ticket.  Make sure it has valid data, otherwise
            # just having the 'status' field could cause problems in
            # handle_retries().
            ticket = work_ticket.copy()
            if getattr(msg, "errno", None) == errno.ETIMEDOUT:
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
                
            #Since an error occured, just return it.
            return None, None, ticket

        if config and config.get('interface', None):
            Trace.message(TICKET_LEVEL, "MOVER HANDSHAKE (ROUTING)")
            Trace.message(TICKET_LEVEL, pprint.pformat(ticket))
            Trace.log(e_errors.INFO,
                      "Received routing ticket from %s for transfer %s." % \
                      (ticket.get('mover', {}).get('name', "Unknown"),
                       ticket.get('unique_id', "Unknown")))

        #Attempt to get the control socket connected with the mover.
        try:
	    #The listen socket used depends on if route selection is
	    # enabled or disabled.  If enabled, then the listening socket
	    # returned from open_routing_socket is used.  Otherwise, the
	    # original routing socket opened and the beginning is used.
	    #If the routes were changed, then only wait 10 sec. before
	    # initiating the retry.  If no routing was done, wait for the
	    # mover to callback as originally done.
	    if config and config.get('interface', None):
		for i in range(int(encp_intf.mover_timeout/15)):
		    try:
			control_socket, mover_address, ticket = \
					open_control_socket(
					    use_listen_socket, 15)
			break
		    except (socket.error, EncpError), msg:
			#If the error was timeout, resend the reply
			# Since, there was an exception, "ticket" is still
			# the ticket returned from the routing call.
			if getattr(msg, "errno", None) == errno.ETIMEDOUT:
			    route_server.reply_to_caller_using_interface_ip(
				ticket, use_listen_socket.getsockname()[0])
			else:
			    raise msg
		else:
		    #If we get here then we had encp_intf.max_retry timeouts
		    # occur.  Giving up.
		    raise msg
	    else:
		control_socket, mover_address, ticket = open_control_socket(
		    use_listen_socket, encp_intf.mover_timeout)
        except (socket.error, EncpError):
            exc, msg = sys.exc_info()[:2]
            if getattr(msg, "errno", None) == errno.ETIMEDOUT:
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
        Trace.log(e_errors.INFO,
                  "Received callback ticket from mover %s for transfer %s." % \
                  (ticket.get('mover', {}).get('name', "Unknown"),
                   ticket.get('unique_id', "Unknown")))
        Trace.log(e_errors.INFO,
                  "Control socket %s is connected to %s for %s. " %
                  (control_socket.getsockname(),
                   control_socket.getpeername(),
                   ticket.get('unique_id', "Unknown")))

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
            exc, msg = sys.exc_info()[:2]
            sys.stderr.write("Sub ticket 'mover' not found.\n")
            sys.stderr.write("%s: %s\n" % (str(exc), str(msg)))
            sys.stderr.write(pprint.pformat(ticket)+"\n")
            if e_errors.is_ok(ticket.get('status', (None, None))):
                ticket['status'] = (str(exc), str(msg))
            return None, None, ticket

        #Attempt to get the data socket connected with the mover.
        try:
            Trace.log(e_errors.INFO,
                      "Atempting to connect data socket to mover at %s." \
                      % (mover_addr,))
            data_path_socket = open_data_socket(mover_addr,
                                                ticket['callback_addr'][0])

            if not data_path_socket:
                raise socket.error,(errno.ENOTCONN,os.strerror(errno.ENOTCONN))

        except (socket.error,), detail:
            exc, msg = sys.exc_info()[:2]
            ticket['status'] = (e_errors.NET_ERROR, str(msg))
            #Trace.log(e_errors.INFO, str(msg))
            close_descriptors(control_socket)
            #Since an error occured, just return it.
            return None, None, ticket

        Trace.log(e_errors.INFO,
                  "Data socket %s is connected to %s for %s. " %
                  (data_path_socket.getsockname(),
                   data_path_socket.getpeername(),
                   ticket.get('unique_id', "Unknown")))

        #We need to specifiy which interface was used on the encp side.
        ticket['encp_ip'] =  use_listen_socket.getsockname()[0]
        #If we got here then the status is OK.
        ticket['status'] = (e_errors.OK, None)
        #Include new info from mover.
        work_tickets[i] = ticket

        #The following three lines are usefull for testing error handling.
        #control_socket.close()
        #data_path_socket.close()
        #ticket['status'] = (e_errors.NET_ERROR, "because I closed them")

        return control_socket, data_path_socket, ticket
    
############################################################################
############################################################################

def submit_one_request(ticket):

    #These two lines of code are for get retries to work properly.
    if ticket.get('method', None) != None:
        ticket['method'] = "read_tape_start"
    
    ##start of resubmit block
    Trace.trace(17,"submiting: %s"%(ticket,))

    #On a retry or resubmit, print the number of the retry.
    if ticket.get('retry', None):
        Trace.message(TO_GO_LEVEL, "RETRY COUNT:" + str(ticket['retry']))
    if ticket.get('resubmits', None):
        Trace.message(TO_GO_LEVEL, "RESUBMITS COUNT:"+str(ticket['resubmits']))

    #Put in the log file a message connecting filenames to unique_ids.
    msg = "Sending request to LM: uninque_id: %s inputfile: %s outputfile: %s"\
          % (ticket['unique_id'], ticket['infile'], ticket['outfile'])
    Trace.log(e_errors.INFO, msg)

    csc = get_csc(ticket)
    Trace.message(CONFIG_LEVEL, format_class_for_print(csc, "csc"))

    #Send work ticket to LM
    #Get the library manager info information.
    lmc = library_manager_client.LibraryManagerClient(
        csc, ticket['vc']['library'] + ".library_manager")
    if ticket['infile'][:5] == "/pnfs":
        responce_ticket = lmc.read_from_hsm(ticket)
    else:
        responce_ticket = lmc.write_to_hsm(ticket)

    if not e_errors.is_ok(responce_ticket['status']):
        Trace.message(ERROR_LEVEL, "Submission to LM failed: " + \
                      str(responce_ticket['status']))
        Trace.log(e_errors.ERROR,
                  "submit_one_request: Ticket submit failed for %s"
                  " - retrying" % ticket['infile'])

    return responce_ticket

############################################################################

#mode should only contain two values, "read", "write".
def open_local_file(filename, e):
    if e.mmap_io:
        #If the file descriptor will be memory mapped, we need both read and
        # write permissions on the descriptor.
        #If (for reads only) the file is to have the crc rechecked, we
        # need both read and write permissions.
        flags = os.O_RDWR
    else:
        if e.outtype == "hsmfile": #writes
            flags = os.O_RDONLY
        else: #reads
            #Setting the local fd to read/write access on reads from enstore
            # (writes to local file) should be okay.  The file is initially
            # created with 0644 permissions and is not set to original file
            # permissions until after everything else is set.  The read
            # permissions might be needed later (i.e. --ecrc).
            flags = os.O_RDWR
    """
    #On systems where O_DIRECT does exist we must set the value directly.
    # This must be done when the file is opened.  It doesn't work if it
    # is set by fcntl() later on.
    sysname = os.uname()[0]
    if sysname == "Linux" and e.direct_io:
        #On linix this is generally ignored.  XFS on linux seems to use it,
        # since the rates on the terabyte filesystems are improved with its
        # use.  Ext2 on linux uses it.
        O_DIRECT = 16384     #O_DIRECT
    #Setting this on IRIX using a filesystem that doesn't support O_DIRECT 
    # will result in EINVAL error.  (ie. XFS would work, but NFS would fail.)
    elif sysname == "IRIX64" and e.direct_io:
        O_DIRECT = 32768     #O_DIRECT
    else:
	O_DIRECT = 0
    """
    O_DIRECT = 0
    #Try to open the local file for read/write.
    try:
        local_fd = os.open(filename, flags | O_DIRECT, 0666)
    except OSError, detail:
	if flags & O_DIRECT and detail.errno == errno.EINVAL:
	    #If we get here then it is likely that the local filesystem
	    # does not support direct i/o.  Try without it.
	    try:
		local_fd = os.open(filename, flags, 0666)
		e.direct_io = 0  #Direct io failed, using posix io.
		sys.stderr.write(
                    "Direct io failed (%s), using posix io.\n",
                    os.strerror(detail.errno))
	    except OSError, detail:
		#USERERROR is on the list of non-retriable errors.  Because of
		# this the return from handle_retries will remove this request
		# from the list.  Thus avoiding issues with the continue and
		# range(submitted).
		done_ticket = {'status':(e_errors.USERERROR, str(detail))}
		return done_ticket
        if e.mmap_io and detail.errno == errno.EACCES:
            #If we get here then check if we can open the file with only
            # specified file permission.

            #Determine the minimum permissions needed for posix I/O.
            if e.outtype == "hsmfile": #writes
                flags = os.O_WRONLY
            else: #reads
                flags = os.O_RDONLY

            #Try again with posix I/O.
            try:
                local_fd = os.open(filename, flags, 0666)
		e.mmap_io = 0  #Direct io failed, using posix io.
		sys.stderr.write(
                    "Memory mapped io failed (%s), using posix io.\n",
                    os.strerror(detail.errno))
            except OSError, detail:
                #USERERROR is on the list of non-retriable errors.  Because of
		# this the return from handle_retries will remove this request
		# from the list.  Thus avoiding issues with the continue and
		# range(submitted).
		done_ticket = {'status':(e_errors.USERERROR, str(detail))}
		return done_ticket
	else:
            #USERERROR is on the list of non-retriable errors.  Because of
	    # this the return from handle_retries will remove this request
	    # from the list.  Thus avoiding issues with the continue and
	    # range(submitted).
	    done_ticket = {'status':(e_errors.USERERROR, str(detail))}
	    return done_ticket

    done_ticket = {'status':(e_errors.OK, None), 'fd':local_fd}
    return done_ticket

############################################################################

def receive_final_dialog(control_socket):
    # File has been sent - wait for final dialog with mover. 
    # We know the file has hit some sort of media.... 
    
    try:
        done_ticket = callback.read_tcp_obj(control_socket)
        Trace.log(e_errors.INFO, "Received final dialog for %s." %
                  done_ticket.get('unique_id', "Unknown"))
    except e_errors.TCP_EXCEPTION, msg:
        done_ticket = {'status':(e_errors.TCP_EXCEPTION,
                                 msg)}
        
    return done_ticket
        
############################################################################

#Returns two-tuple.  First is dictionary with 'status' element.  The next
# is an integer of the crc value.  On error returns 0.
def transfer_file(input_fd, output_fd, control_socket, request, tinfo, e):

    encp_crc = 0

    transfer_start_time = time.time() # Start time of file transfer.

    #Read/Write in/out the data to/from the mover and write/read it out to
    # file.  Also, total up the crc value for comparison with what was
    # sent from the mover.
    try:
        if e.chk_crc != 0:
            crc_flag = 1
        else:
            crc_flag = 0

        #EXfer_rtn = EXfer.fd_xfer(input_fd, output_fd, request['file_size'],
        #                          e.buffer_size, crc_flag, e.mover_timeout, 0)

	EXfer_rtn = EXfer.fd_xfer(input_fd, output_fd, request['file_size'],
                                  crc_flag, e.mover_timeout,
				  e.buffer_size, e.array_size, e.mmap_size,
				  e.direct_io, e.mmap_io, e.threaded_exfer, 0)

        #Exfer_rtn is a tuple.
        # [0] exit_status (1 or 0)
	# [1] crc
        #The following should never be needed.
	# [2] bytes_left_untransfered
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
        # [4] bytes left untransfered
        # [5] read time
        # [6] write time
        # [7] filename of error
        # [8] line number that the error occured on
        if msg.args[1] == errno.ENOSPC: #This should be non-retriable.
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
            EXfer_ticket['bytes_not_transfered'] = msg.args[4]
            EXfer_ticket['read_time'] = msg.args[5]
            EXfer_ticket['write_time'] = msg.args[6]
            EXfer_ticket['filename'] = msg.args[7]
            EXfer_ticket['line_number'] = msg.args[8]
            
        Trace.log(e_errors.WARNING, "EXfer file transfer error: %s" %
                  (str(msg),))
        Trace.message(TRANSFER_LEVEL,
                     "EXfer file transfer error: [Error %s] %s: %s  elapsed=%s"
                      % (msg.args[1], msg.args[2], msg.args[0],
                         time.time() - tinfo['encp_start_time'],))

    transfer_stop_time = time.time()

    # Print an additional timming value.
    Trace.message(TIME_LEVEL, "Time to transfer file: %s sec." %
                  (transfer_stop_time - transfer_start_time,))
    # File has been read - wait for final dialog with mover.
    Trace.message(TRANSFER_LEVEL,"Waiting for final mover dialog.  elapsed=%s"
                  % (transfer_stop_time - tinfo['encp_start_time'],))
    
    #Even though the functionality is there for this to be done in
    # handle requests, this should be received outside since there must
    # be one... not only receiving one on error.
    if EXfer_ticket['status'][0] == e_errors.NOSPACE:
        done_ticket = {'status':(e_errors.OK, None)}
    elif EXfer_ticket['status'][0] == e_errors.DEVICE_ERROR:
        done_ticket = {'status':(e_errors.OK, None)}
    else:
        final_dialog_start_time = time.time() # Start time of file transfer.
        
        done_ticket = receive_final_dialog(control_socket)

        # Print an additional timming value.
        Trace.message(TIME_LEVEL, "Time to receive final dialog: %s sec." %
                      (time.time() - final_dialog_start_time,))

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

    #Make these more accessable.
    mover_crc = done_ticket['fc'].get('complete_crc', None)
    encp_crc = done_ticket['exfer'].get('encp_crc', None)
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
            msg = "CRC mismatch: %d != %d" % (mover_crc, encp_crc)
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
                msg = "CRC readback mismatch: %d != %d" % (mover_crc,
                                                           readback_crc)
                done_ticket['status'] = (e_errors.CRC_ECRC_ERROR, msg)
                return
                

############################################################################
            
def verify_file_size(ticket):
    #Don't worry about checking when outfile is /dev/null.
    if ticket['outfile'] == '/dev/null':
        return

    #Get the stat info for each file.
    try:
        full_stat = os.stat(ticket['wrapper'].get('fullname', None))
        full_filesize = full_stat[stat.ST_SIZE]
    except (OSError, IOError), detail:
        ticket['status'] = (e_errors.OSERROR, str(detail))
        return
    
    try:
        pnfs_stat = os.stat(ticket['wrapper'].get('pnfsFilename', None))
        pnfs_filesize = pnfs_stat[stat.ST_SIZE]
    except (TypeError), detail:
        ticket['status'] = (e_errors.OK, "No files sizes to verify.")
        return
    except (OSError, IOError), detail:
        ticket['status'] = (e_errors.OSERROR, str(detail))
        return

    try:
        in_stat = os.stat(ticket['infile'])
        in_filesize = in_stat[stat.ST_SIZE]
    except (OSError, IOError), detail:
        ticket['status'] = (e_errors.OSERROR, str(detail))
        return
    
    try:
        out_stat = os.stat(ticket['outfile'])
        out_filesize = out_stat[stat.ST_SIZE]
    except (OSError, IOError), detail:
        ticket['status'] = (e_errors.OSERROR, str(detail))
        return
    
    #Until pnfs supports NFS version 3 (for large file support) make sure
    # we are using the correct file_size for the pnfs side.
    try:
        p = pnfs.Pnfs(ticket['wrapper']['pnfsFilename'])
        p.get_file_size()
        pnfs_real_size = p.file_size
    except (OSError, IOError), detail:
        ticket['status'] = (e_errors.OSERROR, str(detail))
        return

    #Handle large files.
    if pnfs_filesize == 1:
        if full_filesize != pnfs_real_size:
            msg = "Expected local file size (%s) to equal remote file " \
                  " size (%s) for file %s." \
                  % (full_filesize, pnfs_real_size, ticket['outfile'])
            ticket['status'] = (e_errors.EPROTO, msg)
    #Test if the sizes are correct.
    elif ticket['file_size'] != out_filesize:
        msg = "Expected file size (%s) equal to actuall file size " \
              "(%s) for file %s." % \
              (ticket['file_size'], out_filesize, ticket['outfile'])
        ticket['status'] = (e_errors.EPROTO, msg)
    elif full_filesize != pnfs_filesize:
        msg = "Expected local file size (%s) to equal remote file " \
              " size (%s) for file %s." \
              % (full_filesize, pnfs_filesize, ticket['outfile'])
        ticket['status'] = (e_errors.EPROTO, msg)
    
############################################################################

def set_outfile_permissions(ticket):
    #Attempt to get the input files permissions and set the output file to
    # match them.
    if ticket['outfile'] != "/dev/null":
        try:
            perms = os.stat(ticket['infile'])[stat.ST_MODE]
            os.chmod(ticket['outfile'], perms)
            ticket['status'] = (e_errors.OK, None)
        except OSError, msg:
            Trace.log(e_errors.INFO, "chmod %s failed: %s" % \
                      (ticket['outfile'], msg))
            ticket['status'] = (e_errors.USERERROR,
                                "Unable to set permissions.")

############################################################################

#This function prototype looking thing is here so that there is a defined
# handle_retries() before internal_handle_retries() is defined.  While
# python handles this without the pre-definition, mylint.py does not.
def handle_retries(*args):
    pass

#This internal version of handle retries should only be called from inside
# of handle_retries().
def internal_handle_retries(request_list, request_dictionary, error_dictionary,
                            listen_socket, route_server, control_socket,
                            encp_intf):
    #Set the encp_intf to internal test values to two.  This means
    # there is only one check made on internal problems.
    remember_retries = encp_intf.max_retry
    remember_resubmits = encp_intf.max_resubmit
    encp_intf.max_retry = 2
    encp_intf.max_resubmit = 2
    
    internal_result_dict = handle_retries(request_list, request_dictionary,
                                          error_dictionary, listen_socket,
                                          route_server, control_socket,
                                          encp_intf)

    #Set the max resend parameters to original values.
    encp_intf.max_retry = remember_retries
    encp_intf.max_resubmit = remember_resubmits

    return internal_result_dict

def handle_retries(request_list, request_dictionary, error_dictionary,
                   listen_socket, route_server, control_socket, encp_intf):
    #Extract for readability.
    max_retries = encp_intf.max_retry
    max_submits = encp_intf.max_resubmit
    verbose = encp_intf.verbose
    
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
    retry = request_dictionary.get('retry', 0)
    resubmits = request_dictionary.get('resubmits', 0)

    #Get volume info from the volume clerk.
    #Need to check if the volume has been marked NOACCESS since it
    # was checked last.  This should only apply to reads.
    if request_dictionary.get('fc', {}).has_key('external_label'):
        # get a configuration server
        csc = get_csc(request_dictionary)

        # get the volume clerk responce.
        vcc = volume_clerk_client.VolumeClerkClient(csc)
        vc_reply = vcc.inquire_vol(request_dictionary['fc']['external_label'])
        vc_status = vc_reply['status']
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
            #Determine if the control socket has some errror to report.
            read_fd, write_fd, exc_fd = select.select([control_socket],
                                                      [], [], 15)
            #check control socket for error.
            if read_fd:
                socket_dict = receive_final_dialog(control_socket)
                socket_status = socket_dict.get('status', (e_errors.OK , None))
                request_dictionary = combine_dict(socket_dict,
                                                  request_dictionary)

    #Just to be paranoid check the listening socket.  Check the current
    # socket status to avoid wiping out an error.
    if e_errors.is_ok(socket_dict) and listen_socket:
        socket_error = listen_socket.getsockopt(socket.SOL_SOCKET,
                                                socket.SO_ERROR)
        if socket_error:
            socket_status = (e_errors.NET_ERROR, os.strerror(socket_error))
            socket_dict = {'status':socket_status}
            request_dictionary = combine_dict(socket_dict, request_dictionary)

    #The volume clerk set the volume NOACCESS.
    if not e_errors.is_ok(vc_status):
        status = vc_status
    #Set status if there was an error recieved from control socket.
    elif not e_errors.is_ok(socket_status):
        status = socket_status
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
    # then if the encp is killed before completing quit() could leave
    # non-zero non-correct files.  If this is the case truncate them.
    elif is_read(encp_intf):
        try:
            fd = os.open(outfile, os.O_WRONLY | os.O_TRUNC)
            os.close(fd)
        except (IOError, OSError):
            #Something is very wrong, deal with it later.
            pass

    #If the mover doesn't call back after max_submits number of times, give up.
    if max_submits != None and resubmits >= max_submits:
        Trace.message(ERROR_LEVEL,
                      "To many resubmissions for %s -> %s."%(infile,outfile))
        status = (e_errors.TOO_MANY_RESUBMITS, status)

    #If the transfer has failed to many times, remove it from the queue.
    # Since TOO_MANY_RETRIES is non-retriable, set this here.
    if max_retries != None and retry >= max_retries:
        Trace.message(ERROR_LEVEL,
                      "To many retries for %s -> %s."%(infile,outfile))
        status = (e_errors.TOO_MANY_RETRIES, status)

    #If the error is not retriable, remove it from the request queue.  There
    # are two types of non retriable errors.  Those that cause the transfer
    # to be aborted, and those that in addition to abborting the transfer
    # tell the operator that someone needs to investigate what happend.
    if not e_errors.is_retriable(status[0]):
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
        if e_errors.is_alarmable(status[0]):
            Trace.alarm(e_errors.ERROR, status[0], {
                'infile':infile, 'outfile':outfile, 'status':status[1]})

        try:
            #Try to delete the request.  In the event that the connection
            # didn't let us determine which request failed, don't worry.
            del request_list[request_list.index(request_dictionary)]
            queue_size = len(request_list)
        except KeyboardInterrupt:
            raise sys.exc_info()
        except (KeyError, ValueError):
            queue_size = len(request_list) - 1

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
        ###Is the work done here duplicated in the next commented code line???
        request_dictionary['resubmits'] = resubmits + 1

        #Update the tickets callback fields.  The actual sockets
        # are updated becuase they are passed in by reference.  There
        # are some cases (most notably when internal_handle_retries()
        # is used) that there isn't a socket passed in to change.
        if request_list[0].get('route_selection', None) and route_server:
            routing_addr, route_sever = get_routing_callback_addr(
                encp_intf, route_server)
        else:
            routing_addr = None

        for req in request_list:
            try:
                #Increase the resubmit count.
                req['resubmits'] = req.get('resubmits', 0) + 1

                #Before resubmitting, there are some fields that the library
                # manager and mover don't expect to receive from encp,
                # these should be removed.
                for item in (item_remove_list):
                    try:
                        del req[item]
                    except KeyError:
                        pass

                #Update the ticket before sending it to library manager.
                if routing_addr:
                    req['routing_callback_addr'] = routing_addr

                #Send this to log file.
                Trace.log(e_errors.WARNING, (e_errors.RESUBMITTING,
                                             req.get('unique_id', None)))

                #Since a retriable error occured, resubmit the ticket.
                lm_responce = submit_one_request(req)

            except KeyboardInterrupt:
                raise sys.exc_info()
            except:
                exc, msg = sys.exc_info()[:2]
                sys.stderr.write("%s: %s\n" % (str(exc), str(msg)))

            #Now it get checked.  But watch out for the recursion!!!
            internal_result_dict = internal_handle_retries([req], req,
                                                           lm_responce,
                                                           None, None,
                                                           None, encp_intf)

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
        Trace.log(e_errors.WARNING, status)

        #Get a new unique id for the transfer request since the last attempt
        # ended in error.
        request_dictionary['unique_id'] = generate_unique_id()

        #Keep retrying this file.
        try:
            #Increase the retry count.
            request_dictionary['retry'] = retry + 1
            
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

            #Since a retriable error occured, resubmit the ticket.
            lm_responce = submit_one_request(request_dictionary)

        except KeyError, msg:
            lm_responce = {'status':(e_errors.NET_ERROR,
                            "Unable to obtain responce from library manager.")}
            sys.stderr.write("Error processing retry of %s.\n" %
                             (request_dictionary['unique_id']))
            sys.stderr.write(pprint.pformat(request_dictionary)+"\n")
            
        #Now it get checked.  But watch out for the recursion!!!
        internal_result_dict = internal_handle_retries([request_dictionary],
                                                       request_dictionary,
                                                       lm_responce,
                                                       None, None,
                                                       None, encp_intf)

        
        #If an unrecoverable error occured while retrying to LM.
        if e_errors.is_non_retriable(internal_result_dict['status'][0]):
            result_dict = {'status':internal_result_dict['status'],
                           'retry':request_dictionary.get('retry', 0),
                           'resubmits':request_dictionary.get('resubmits',
                                                              0),
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
    # calculate some kind of rate - time from beginning to wait for
    # mover to respond until now. This doesn't include the overheads
    # before this, so it isn't a correct rate. I'm assuming that the
    # overheads I've neglected are small so the quoted rate is close
    # to the right one.  In any event, I calculate an overall rate at
    # the end of all transfers

    #calculate MB relatated stats
    bytes_per_MB = 1024 * 1024
    MB_transfered = float(done_ticket['file_size']) / float(bytes_per_MB)

    #For readablilty...
    id = done_ticket['unique_id']

    #Make these variables easier to use.
    transfer_time = tinfo.get('%s_transfer_time' % (id,), 0)
    overall_time = tinfo.get('%s_overall_time' % (id,), 0)
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
            tinfo['%s_transfer_rate'%(id,)] = MB_transfered / transfer_time
        else:
            tinfo['%s_transfer_rate'%(id,)] = 0.0
        if overall_time != 0:
            tinfo['%s_overall_rate'%(id,)] = MB_transfered / overall_time
        else:
            tinfo['%s_overall_rate'%(id,)] = 0.0
        if network_time != 0:
            tinfo['%s_network_rate'%(id,)] = MB_transfered / network_time
        else:
            tinfo['%s_network_rate'%(id,)] = 0.0            
        if drive_time != 0:
            tinfo['%s_drive_rate'%(id,)] = MB_transfered / drive_time
        else:
            tinfo['%s_drive_rate'%(id,)] = 0.0
        if disk_time != 0:
            tinfo['%s_disk_rate'%(id,)] = MB_transfered / disk_time
        else:
            tinfo['%s_disk_rate'%(id,)] = 0.0
            
        sg = done_ticket.get('fc', {}).get('storage_group', "")
        if not sg:
            sg = volume_family.extract_storage_group(
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
                        tinfo["%s_transfer_rate"%(id,)],
                        tinfo['%s_network_rate'%(id,)],
                        tinfo["%s_drive_rate"%(id,)],
                        tinfo["%s_disk_rate"%(id,)],
                        tinfo["%s_overall_rate"%(id,)],
                        tinfo["%s_transfer_rate"%(id,)],
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
            'network_rate' : tinfo["%s_transfer_rate"%(id,)],
            'drive_rate' : tinfo["%s_drive_rate"%(id,)],
            'disk_rate' : tinfo["%s_disk_rate"%(id,)],
            'overall_rate' : tinfo["%s_overall_rate"%(id,)],
            'transfer_rate' : tinfo["%s_transfer_rate"%(id,)],
            }

        log_values = (done_ticket['work'],
                      done_ticket['infile'],
                      done_ticket['outfile'],
                      done_ticket['file_size'],
                      preposition,
                      done_ticket["fc"]["external_label"],
                      tinfo["%s_transfer_rate"%(id,)],
                      tinfo["%s_network_rate"%(id,)],
                      tinfo['%s_drive_rate'%(id,)],
                      tinfo["%s_disk_rate"%(id,)],
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
	if done_ticket['work'] == "read_from_hsm":
		rw = 'r'
	else:
		rw = 'w'

        # Avoid division by zero problems.
        if network_time == 0.0:
            acc_network_rate = int(0.0);
        else:
            acc_network_rate = int(done_ticket['file_size'] / network_time)
        if drive_time == 0.0:
            acc_drive_rate = int(0.0)
        else:
            acc_drive_rate = int(done_ticket['file_size'] / drive_time)
        if disk_time == 0.0:
            acc_disk_rate = int(0.0)
        else:
            acc_disk_rate = int(done_ticket['file_size'] / disk_time)
        if overall_time == 0.0:
            acc_overall_rate = int(0.0)
        else:
            acc_overall_rate = int(done_ticket['file_size'] / overall_time)
        if transfer_time == 0.0:
            acc_transfer_rate = int(0.0)
        else:
            acc_transfer_rate = int(done_ticket['file_size'] / transfer_time)
        
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
                          rw)
			

############################################################################

def calculate_final_statistics(bytes, number_of_files, exit_status, tinfo):
    #Determine the average of each time (overall, transfer, network,
    # tape and disk) of all transfers done for the encp.  If only one file
    # was transfered, then these rates should equal the files rates.

    statistics = {}
    
    #Calculate total running time from the begining.
    now = time.time()
    tinfo['total'] = now - tinfo['encp_start_time']

    #calculate MB relatated stats
    bytes_per_MB = 1024 * 1024
    MB_transfered = float(bytes) / float(bytes_per_MB)

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
    return done_ticket

############################################################################
#Support functions for writes.
############################################################################

#Verifies that the state of the files, like existance and permissions,
# are accurate.
def verify_write_file_consistancy(request_list, e):

    for request in request_list:

        #Verify that everything with the files (existance, permissions,
        # etc) is good to go.
        inputfile_check(request['infile'], e)
        outputfile_check(request['infile'], request['outfile'], e)


#Args:
# Takes a list of request tickets.
#Returns:
#None
#Verifies that various information in the tickets are correct, valid, spelled
# correctly, etc.
def verify_write_request_consistancy(request_list):

    for request in request_list:

        #Consistancy check for valid pnfs tag values.  These values are
        # placed inside the 'vc' sub-ticket.
        tags = ["file_family", "wrapper", "file_family_width",
                "storage_group", "library"]
        for key in tags:
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

############################################################################

def set_pnfs_settings(ticket, intf_encp):

    # create a new pnfs object pointing to current output file
    Trace.message(INFO_LEVEL,
            "Updating %s file metadata." % ticket['wrapper']['pnfsFilename'])

    layer1_start_time = time.time() # Start time of setting pnfs layer 1.

    #The first piece of metadata to set is the bit file id which is placed
    # into layer 1.
    try:
        #p=pnfs.Pnfs(ticket['outfile'])
        #t=pnfs.Tag(os.path.dirname(ticket['outfile']))
        p=pnfs.Pnfs(ticket['wrapper']['pnfsFilename'])
        t=pnfs.Tag(os.path.dirname(ticket['wrapper']['pnfsFilename']))
        # save the bfid
        p.set_bit_file_id(ticket["fc"]["bfid"])
    except KeyboardInterrupt:
        raise sys.exc_info()
    except:
        exc, msg = sys.exc_info()[:2]
        Trace.log(e_errors.INFO, "Trouble with pnfs: %s %s."
                  % (str(exc), str(msg)))
        ticket['status'] = (str(exc), str(msg))
        return

    Trace.message(TIME_LEVEL, "Time to set pnfs layer 1: %s sec." %
                  (time.time() - layer1_start_time,))

    layer4_start_time = time.time() # Start time of setting pnfs layer 4.
        
    #Store the cross reference data into layer 4.

    #Format some tape drive output.
    mover_ticket = ticket.get('mover', {})
    drive = "%s:%s" % (mover_ticket.get('device', 'Unknown'),
                       mover_ticket.get('serial_num','Unknown'))
    #For writes to null movers, make the crc zero.
    if mover_ticket['driver'] == "NullDriver":
        crc = 0
    else:
        crc = ticket['fc']['complete_crc']
    #Write to the metadata layer 4 "file".
    try:
        #t.get_file_family()
        p.get_bit_file_id()
        p.get_id()
        p.set_xreference(ticket["fc"]["external_label"],
                         ticket["fc"]["location_cookie"],
                         ticket["fc"]["size"],
                         ticket["vc"]["file_family"],
                         p.pnfsFilename,
                         "", #p.volume_filepath,
                         p.id,
                         "", #p.volume_fileP.id,
                         p.bit_file_id,
                         drive,
                         crc)
    except KeyboardInterrupt:
        raise sys.exc_info()
    except:
        exc, msg = sys.exc_info()[:2]
        Trace.log(e_errors.INFO, "Trouble with pnfs.set_xreference %s %s."
                  % (str(exc), str(msg)))
        ticket['status'] = (str(exc), str(msg))
        return

    Trace.message(TIME_LEVEL, "Time to set pnfs layer 4: %s sec." %
                  (time.time() - layer4_start_time,))

    filedb_start_time = time.time() # Start time of updating file database.

    #Update the file database with the transfer info.
    try:
        # add the pnfs ids and filenames to the file clerk ticket and store it
        fc_ticket = {}
        fc_ticket["fc"] = ticket['fc'].copy()
        fc_ticket["fc"]["pnfsid"] = p.id
        fc_ticket["fc"]["pnfsvid"] = "" #p.volume_fileP.id
        fc_ticket["fc"]["pnfs_name0"] = p.pnfsFilename
        fc_ticket["fc"]["pnfs_mapname"] = "" #p.mapfile
        fc_ticket["fc"]["drive"] = drive

        csc = get_csc()
        fcc = file_clerk_client.FileClient(csc, ticket["fc"]["bfid"])
        fc_reply = fcc.set_pnfsid(fc_ticket)

        if not e_errors.is_ok(fc_reply['status'][0]):
            Trace.alarm(e_errors.ERROR, fc_reply['status'][0], fc_reply)

        Trace.message(TICKET_LEVEL, "PNFS SET")
        Trace.message(TICKET_LEVEL, pprint.pformat(fc_reply))

        ticket['status'] = fc_reply['status']
    except KeyboardInterrupt:
        raise sys.exc_info()
    except:
        exc, msg = sys.exc_info()[:2]
        Trace.log(e_errors.INFO, "Unable to send info. to file clerk. %s %s."
                  % (str(exc), str(msg)))
        ticket['status'] = (str(exc), str(msg))
        return

    Trace.message(TIME_LEVEL, "Time to set file database: %s sec." %
                  (time.time() - filedb_start_time,))

    filesize_start_time = time.time() # Start time of setting the filesize.

    # file size needs to be the LAST metadata to be recorded
    try:
        #The dcache sets the file size.  If encp tries to set it again, pnfs
        # sets the size to zero.  Thus, only do this for normal transfers.
        if not intf_encp.put_cache:
            #If the size is already set don't set it again.  Doing so
            # would set the filesize back to zero.
            size = os.stat(ticket['wrapper']['pnfsFilename'])[stat.ST_SIZE]
            if long(size) == long(ticket['file_size']) or long(size) == 1L:
                Trace.log(e_errors.INFO,
                          "Filesize (%s) for file %s already set." %
                          (ticket['file_size'],
                           ticket['wrapper']['pnfsFilename']))
            else:
                # set the file size
                p.set_file_size(ticket['file_size'])
    except KeyboardInterrupt:
        raise sys.exc_info()
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

def create_write_requests(callback_addr, routing_addr, e, tinfo):

    request_list = []

    #Initialize these, so that they can be set only once.
    vcc = fcc = None
    file_family = file_family_width = file_family_wrapper = None
    library = storage_group = None

    # create internal list of input unix files even if just 1 file passed in
    if type(e.input) == types.ListType:
        e.input = e.input
    else:
        e.input = [e.input]

    if not e.put_cache and len(e.output) > 1:
        raise EncpError(None,
                        'Cannot have multiple output files',
                        e_errors.USERERROR)

    # check the input unix file. if files don't exits, we bomb out to the user
    for i in range(len(e.input)):

        if e.put_cache:
            p = pnfs.Pnfs(e.put_cache, mount_point = e.pnfs_mount_point)

            ofullname = p.get_path()

            imachine, ifullname, idir, ibasename = fullpath(e.input[0])

        else: #The output file was given as a normal filename.

            ifullname, ofullname = get_ninfo(e.input[i], e.output[0], e)

        #Fundamentally this belongs in veriry_read_request_consistancy(),
        # but information needed about the input file requires this check.
        inputfile_check(ifullname, e)
        
        #Fundamentally this belongs in veriry_write_request_consistancy(),
        # but information needed about the output file requires this check.
        outputfile_check(ifullname, ofullname, e)

        # get fully qualified name
        #imachine, ifullname, idir, ibasename = fullpath(e.input[i])
        #omachine, ofullname, odir, obasename = fullpath(e.output[0])
        # Add the name if necessary.
        #if len(e.input) > 1:
        #    ofullname = os.path.join(ofullname, ibasename)
        #    omachine, ofullname, odir, obasename = fullpath(ofullname)
        #elif len(e.input) == 1 and os.path.isdir(ofullname):
        #    ofullname = os.path.join(ofullname, ibasename)
        #    omachine, ofullname, odir, obasename = fullpath(ofullname)

        file_size = get_file_size(ifullname)

        #Obtain the pnfs tag information.
        try:
            #t=pnfs.Tag(odir)
            t=pnfs.Tag(os.path.dirname(ofullname))

            #There is no sense to get these values every time.  Only get them
            # on the first pass.
            if not library:
                library = t.get_library()
            #The pnfs file family may be overridden with the options
            # --ephemeral or --file-family.
            if not file_family:
                if e.output_file_family:
                    file_family = e.output_file_family
                else:
                    file_family = t.get_file_family()
            if not file_family_width:
                file_family_width = t.get_file_family_width()
            if not file_family_wrapper:
                file_family_wrapper = t.get_file_family_wrapper()
            if not storage_group:
                storage_group = t.get_storage_group()
        except (OSError, IOError), msg:
            print_data_access_layer_format(
                '', '', 0, {'status':
                            (errno.errorcode[getattr(msg, "errno", errno.EIO)],
                             str(msg))})
            quit()

        #Get the data aquisition information.
        encp_daq = get_dinfo()

        p = pnfs.Pnfs(ofullname)

        try:
            #Snag the three pieces of information needed for the wrapper.
            pinfo = get_pinfo(p)
            uinfo = get_uinfo()
            finfo = get_finfo(ifullname, ofullname, e)

            #Combine the data into the wrapper sub-ticket.
            wrapper = get_winfo(pinfo, uinfo, finfo)
            
            #Create the sub-ticket of the command line argument information.
            encp_el = get_einfo(e)
        except EncpError, detail:
            print_data_access_layer_format(
                ifullname, ofullname, file_size,
                {'status':(detail.type, detail.strerror)})
            quit()

        # If this is not the last transfer in the list, force the delayed
        # dismount to be 'long.'  The last transfer should continue to use
        # the default setting.
        if i < (len(e.input) - 1):
            #In minutes.
            encp_el['delayed_dismount'] = max(3, encp_el['delayed_dismount'])

        #only do this the first time.
        if not vcc or not fcc:
            try:
                vcc, fcc = get_clerks()
            except EncpError, detail:
                print_data_access_layer_format(
                    ifullname, ofullname, file_size,
                    {'status':(detail.type, detail.strerror)})
                quit()
                
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
        file_clerk = {"address" : fcc.server_address}

        config = host_config.get_config()
        if config and config.get('interface', None):
            route_selection = 1
        else:
            route_selection = 0

        work_ticket = {}
        work_ticket['callback_addr'] = callback_addr
        work_ticket['client_crc'] = e.chk_crc
        work_ticket['encp'] = encp_el
        work_ticket['encp_daq'] = encp_daq
        work_ticket['fc'] = file_clerk
        work_ticket['file_size'] = file_size
        work_ticket['infile'] = ifullname
        work_ticket['outfile'] = ofullname
        work_ticket['override_ro_mount'] = e.override_ro_mount
        work_ticket['retry'] = 0 #retry,
        work_ticket['routing_callback_addr'] = routing_addr
        work_ticket['route_selection'] = route_selection
        work_ticket['times'] = tinfo.copy() #Only info now in tinfo needed.
        work_ticket['unique_id'] = generate_unique_id()
        work_ticket['vc'] = volume_clerk
        work_ticket['version'] = encp_client_version()
        work_ticket['work'] = "write_to_hsm"
        work_ticket['wrapper'] = wrapper

        request_list.append(work_ticket)

    return request_list

############################################################################

def submit_write_request(work_ticket, tinfo, encp_intf):

    Trace.message(TRANSFER_LEVEL, 
                  "Submitting %s write request.  elapsed=%s" % \
                  (work_ticket['outfile'],
                   time.time() - tinfo['encp_start_time']))

    # send the work ticket to the library manager
    while work_ticket['retry'] <= encp_intf.max_retry:

        ##start of resubmit block
        Trace.trace(17,"write_to_hsm q'ing: %s"%(work_ticket,))

        ticket = submit_one_request(work_ticket)
        
        Trace.message(TICKET_LEVEL, "LIBRARY MANAGER")
        Trace.message(TICKET_LEVEL, pprint.pformat(ticket))

        result_dict = handle_retries([work_ticket], work_ticket, ticket,
                                     None, None, None, encp_intf)
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


def write_hsm_file(listen_socket, route_server, work_ticket, tinfo, e):

    #Loop around in case the file transfer needs to be retried.
    while work_ticket.get('retry', 0) <= e.max_retry:
        
        encp_crc = 0 #In case there is a problem, make sure this exists.

        Trace.message(TRANSFER_LEVEL,
                      "Waiting for mover to call back.   elapsed=%s" % \
                      (time.time() - tinfo['encp_start_time'],))

        #Open the control and mover sockets.
        control_socket, data_path_socket, ticket = mover_handshake(
            listen_socket, route_server, [work_ticket], e)

        overall_start = time.time() #----------------------------Overall Start

        #Handle any possible errors that occured so far.
        result_dict = handle_retries([work_ticket], work_ticket, ticket,
                                     listen_socket, route_server, None, e)

        if e_errors.is_resendable(result_dict['status'][0]):
            continue
        elif e_errors.is_non_retriable(result_dict['status'][0]):
            ticket = combine_dict(result_dict, ticket, work_ticket)
            return ticket

        #Be paranoid.  Check this the ticket again.
        try:
            verify_write_request_consistancy([ticket])
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

        done_ticket = open_local_file(work_ticket['infile'], e)

        result_dict = handle_retries([work_ticket], work_ticket,
                                     done_ticket, listen_socket, route_server,
                                     None, e)
        
        #if result_dict['status'][0] == e_errors.RETRY:
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


        #Stall starting the count until the first byte is ready for writing.
        write_fd = select.select([], [data_path_socket], [], 15 * 60)[1]

        #To achive more accurate rates on writes to enstore when a tape
        # needs to be mounted, wait until the mover has sent a byte as
        # a signal to encp that it is ready to read data from its socket.
        # Otherwise, 64K bytes just sit in the movers recv queue and the clock
        # ticks by making the write rate worse.  When the mover finally
        # mounts and positions the tape, the damage to the rate is already
        # done.
        read_fd = select.select([data_path_socket], [], [], 15 * 60)[0]

        if not write_fd or not read_fd:
            status_ticket = {'status':(e_errors.UNKNOWN,
                                       "No data written to mover.")}
            result_dict = handle_retries([work_ticket], work_ticket,
                                         status_ticket, listen_socket,
                                         route_server, control_socket, e)
            
            close_descriptors(control_socket, data_path_socket, in_fd)
            
            if e_errors.is_retriable(result_dict['status'][0]):
                continue
            elif e_errors.is_non_retriable(result_dict['status'][0]):
                return combine_dict(result_dict, work_ticket)

        Trace.message(TRANSFER_LEVEL, "Starting transfer.  elapsed=%s" %
                  (time.time() - tinfo['encp_start_time'],))
            
        lap_time = time.time() #------------------------------------------Start

        done_ticket = transfer_file(in_fd, data_path_socket.fileno(),
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
                                     done_ticket, listen_socket,
                                     route_server, None, e)

        if e_errors.is_retriable(result_dict['status'][0]):
            continue
        elif e_errors.is_non_retriable(result_dict['status'][0]):
            return done_ticket

        Trace.message(TRANSFER_LEVEL, "File %s transfered.  elapsed=%s" %
                      (done_ticket['outfile'],
                       time.time()-tinfo['encp_start_time']))

        Trace.message(TICKET_LEVEL, "FINAL DIALOG")
        Trace.message(TICKET_LEVEL, pprint.pformat(done_ticket))

        #This function writes errors/warnings to the log file and puts an
        # error status in the ticket.
        check_crc(done_ticket, e) #Check the CRC.

        #Verify that the file transfered in tacted.
        result_dict = handle_retries([work_ticket], work_ticket,
                                     done_ticket, listen_socket,
                                     route_server, None, e)
        if e_errors.is_retriable(result_dict['status'][0]):
            continue
        elif e_errors.is_non_retriable(result_dict['status'][0]):
            return combine_dict(result_dict, work_ticket)

        #Update the last access and modification times respecively.
        update_times(done_ticket['infile'], done_ticket['outfile'])
        
        #We know the file has hit some sort of media. When this occurs
        # create a file in pnfs namespace with information about transfer.
        set_pnfs_settings(done_ticket, e)

        #Verify that the pnfs info was set correctly.
        result_dict = handle_retries([work_ticket], work_ticket,
                                     done_ticket, listen_socket,
                                     route_server, None, e)
        if e_errors.is_retriable(result_dict['status'][0]):
            continue
        elif e_errors.is_non_retriable(result_dict['status'][0]):
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

        tstring = '%s_overall_time' % done_ticket['unique_id']
        tinfo[tstring] = time.time() - overall_start #-------------Overall End

        #Remove the new file from the list of those to be deleted should
        # encp stop suddenly.  (ie. crash or control-C).
        try:
            delete_at_exit.unregister_bfid(done_ticket['fc']['bfid'])
        except (IndexError, KeyError):
            Trace.log(e_errors.INFO, "unable to unregister bfid")
        try:
            delete_at_exit.unregister(done_ticket['outfile']) #localname
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
    callback_addr, listen_socket = get_callback_addr(e)
    #Get an ip and port to listen for the mover address for routing purposes.
    routing_addr, udp_server = get_routing_callback_addr(e)

    #Build the dictionary, work_ticket, that will be sent to the
    # library manager.
    request_list = create_write_requests(callback_addr, routing_addr, e, tinfo)

    #If this is the case, don't worry about anything.
    if len(request_list) == 0:
        quit()

    #This will halt the program if everything isn't consistant.
    try:
        verify_write_file_consistancy(request_list, e)
        verify_write_request_consistancy(request_list)
    except EncpError, msg:
        msg.ticket['status'] = (msg.type, msg.strerror)
        print_data_access_layer_format("", "", 0, msg.ticket)
        quit()

    #Where does this really belong???
    if not e.put_cache: #Skip this for dcache transfers.
        for request in request_list:
            create_zero_length_files(request['outfile'])

    #Set the max attempts that can be made on a transfer.
    max_attempts(request_list[0]['vc']['library'], e)

    # loop on all input files sequentially
    for i in range(0,len(request_list)):

        Trace.message(TO_GO_LEVEL, "FILES LEFT: %s" % str(len(request_list)-i))

        work_ticket = request_list[i]
        Trace.message(TICKET_LEVEL, "WORK_TICKET")
        Trace.message(TICKET_LEVEL, pprint.pformat(work_ticket))

        Trace.message(TRANSFER_LEVEL,
                      "Sending ticket to library manager,  elapsed=%s" %
                      (time.time() - tinfo['encp_start_time'],))
        
        #Send the request to write the file to the library manager.
        done_ticket = submit_write_request(work_ticket, tinfo, e)

        Trace.message(TICKET_LEVEL, "LM RESPONCE TICKET")
        Trace.message(TICKET_LEVEL, pprint.pformat(done_ticket))

        #handle_retries() is not required here since submit_write_request()
        # handles its own retrying when an error occurs.
        if not e_errors.is_ok(done_ticket['status'][0]):
            exit_status = 1
            continue

        Trace.message(TRANSFER_LEVEL,
              "File queued: %s library: %s family: %s bytes: %d elapsed=%s" %
                      (work_ticket['infile'], work_ticket['vc']['library'],
                       work_ticket['vc']['file_family'],
                       long(work_ticket['file_size']),
                       time.time() - tinfo['encp_start_time']))

        #Send (write) the file to the mover.
        done_ticket = write_hsm_file(listen_socket, udp_server,
                                     work_ticket, tinfo, e)

        Trace.message(TICKET_LEVEL, "DONE WRITTING TICKET")
        Trace.message(TICKET_LEVEL, pprint.pformat(done_ticket))

        #Set the value of bytes to the number of bytes transfered before the
        # error occured.
        bytes = bytes + done_ticket['file_size']
        bytes = bytes - done_ticket.get('bytes_not_transfered', 0)

        #handle_retries() is not required here since write_hsm_file()
        # handles its own retrying when an error occurs.
        if not e_errors.is_ok(done_ticket['status'][0]):
            exit_status = 1
            continue

        # calculate some kind of rate - time from beginning 
        # to wait for mover to respond until now. This doesn't 
        # include the overheads before this, so it isn't a 
        # correct rate. I'm assuming that the overheads I've 
        # neglected are small so the quoted rate is close
        # to the right one.  In any event, I calculate an 
        # overall rate at the end of all transfers
        calculate_rate(done_ticket, tinfo)

    # we are done transferring - close out the listen socket
    close_descriptors(listen_socket)

    #Print to screen the exit status.
    Trace.message(TO_GO_LEVEL, "EXIT STATUS: %d" % exit_status)

    #Finishing up with a few of these things.
    calc_ticket = calculate_final_statistics(bytes, len(request_list),
                                             exit_status, tinfo)

    #If applicable print new file family.
    if e.output_file_family:
        ff = string.split(done_ticket["vc"]["file_family"], ".")
        Trace.message(DONE_LEVEL, "New File Family Created: %s" % ff)

    done_ticket = combine_dict(calc_ticket, done_ticket)

    Trace.message(TICKET_LEVEL, "DONE TICKET")
    Trace.message(TICKET_LEVEL, pprint.pformat(done_ticket))

    return done_ticket

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
def verify_read_file_consistancy(requests_per_vol, e):
    vols = requests_per_vol.keys()
    vols.sort()
    for vol in vols:
        request_list = requests_per_vol[vol]
        for request in request_list:
            inputfile_check(request['infile'], e)
            outputfile_check(request['infile'], request['outfile'], e)
 
    
#Args:
# Takes in a dictionary of lists of transfer requests sorted by volume.
#Rerturns:
# None
#Verifies that various information in the tickets are correct, valid, spelled
# correctly, etc.
def verify_read_request_consistancy(requests_per_vol):

    bfid_brand = None
    sum_size = 0L
    
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
            raise EncpError(None, msg, e_errors.CONFLICT, {'status':status})
            #print_data_access_layer_format("", "", 0, {'status':status})
            #quit() #Harsh, but necessary.
            
        for request in request_list:

            #Verify that everything with the files (existance, permissions,
            # etc) is good to go.
            #inputfile_check(request['infile'], e)
            #outputfile_check(request['infile'], request['outfile'], e)

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
            #except EncpError:
            #        request['status'] = (e_errors.USERERROR, msg)
            #        print_data_access_layer_format(request['infile'],
            #                                       request['outfile'],
            #                                       request['file_size'],
            #                                       request)
            #        quit() #Harsh, but necessary.
            except (ValueError, AttributeError, TypeError,
                    IndexError, KeyError), msg:
                raise EncpError(None,
                                "Unrecoverable read list consistancy error " \
                                "for volume %s on external_label check." %
                                (vol,), e_errors.KEYERROR, request)
                                
            #    print_data_access_layer_format("", "", 0, {'status':
            #                    "Unrecoverable read list consistancy error " \
            #                    "for volume %s on external_label check." %
            #                    (vol,)})
            #    quit() #Harsh, but necessary.

            #If no layer 4 is present, then report the error, raise an alarm,
            # but continue with the transfer.
            try:
                p = pnfs.Pnfs(request['wrapper']['pnfsFilename'])
                p.get_xreference()
            except (OSError, IOError), msg:
                raise EncpError(getattr(msg, "errno", errno.EIO),
                                str(msg), e_errors.PNFS_ERROR, request)
                #request['status'] = (errno.errorcode[
                #    getattr(msg, "errno", errno.EIO)], str(msg))
                #print_data_access_layer_format(request['infile'],
                #                               request['outfile'],
                #                               request['file_size'], request)
                #quit() #Harsh, but necessary.

            #Get the database information.
            try:
                db_volume = request['fc']['external_label']
                db_location_cookie = request['fc']['location_cookie']
                db_size = long(request['fc']['size'])
                db_file_family = volume_family.extract_file_family(
                    request['vc']['volume_family'])
                db_pnfs_name0 = request['fc']['pnfs_name0']
                db_pnfsid = request['fc']['pnfsid']
                db_bfid = request['fc']['bfid']
                db_crc = request['fc']['complete_crc']
            except (ValueError, AttributeError, TypeError,
                    IndexError, KeyError), msg:
                raise EncpError(
                    None, "Unable to obtain database information: " + str(msg),
                    e_errors.KEYERROR, request)
                #request['status'] = (e_errors.KEYERROR,
                #                     "Unable to obtain database information: "\
                #                     + str(msg))
                #print_data_access_layer_format(request['infile'],
                #                               request['outfile'],
                #                               request['file_size'], request)
                #quit() #Harsh, but necessary.
            
            if (p.volume == pnfs.UNKNOWN or
                p.location_cookie == pnfs.UNKNOWN or
                p.size == pnfs.UNKNOWN or
                p.origff == pnfs.UNKNOWN  or
                p.origname == pnfs.UNKNOWN or
                #Mapfile no longer used.
                p.pnfsid_file == pnfs.UNKNOWN or
                #Volume map file id no longer used.
                p.bfid == pnfs.UNKNOWN
                #Origdrive has not always been recored.
                #CRC has not always been recored.
                ):
                rest = {'infile':request['infile'],
                        'bfid':request['bfid'],
                        'pnfs_volume':p.volume,
                        'pnfs_location_cookie':p.location_cookie,
                        'pnfs_size':p.size,
                        'pnfs_origff':p.origff,
                        'pnfs_origname':p.origname,
                        'pnfs_id':p.pnfsid_file,
                        'pnfs_bfid':p.bfid,
                        'pnfs_origdrive':p.origdrive,
                        'status':"Missing data in pnfs layer 4."}
                sys.stderr.write(rest['status'] + "  Continuing.\n")
                Trace.alarm(e_errors.ERROR, e_errors.PNFS_ERROR, rest)

            #If there is a layer 4, but the data does not match that in
            # the file and volume clerk databases, then raise alarm and exit.
            elif (db_volume != p.volume or
                  not same_cookie(db_location_cookie, p.location_cookie) or
                  long(db_size) != long(p.size) or
                  db_file_family != p.origff or
                  db_pnfs_name0 != p.origname or
                  #Mapfile no longer used.
                  db_pnfsid != p.pnfsid_file or
                  #Volume map file id no longer used.
                  db_bfid != p.bfid or
                  #Origdrive has not always been recored.
                  (p.crc != pnfs.UNKNOWN and long(db_crc) != long(p.crc))):
                rest = {'infile':request['infile'],
                        'outfile':request['outfile'],
                        'bfid':request['bfid'],
                        'db_volume':db_volume,
                        'pnfs_volume':p.volume,
                        'db_location_cookie':db_location_cookie,
                        'pnfs_location_cookie':p.location_cookie,
                        'db_size':long(db_size),
                        'pnfs_size':long(p.size),
                        'db_file_family':db_file_family,
                        'pnfs_file_family':p.origff,
                        'db_pnfsid':db_pnfsid,
                        'pnfs_pnfsid':p.pnfsid_file,
                        'db_bfid':db_bfid,
                        'pnfs_bfid':p.bfid,
                        'db_crc':db_crc,
                        'pnfs_crc':p.crc,
                        'status':"Probable database conflict with pnfs."}
                Trace.alarm(e_errors.ERROR, e_errors.CONFLICT, rest)
                raise EncpError(None,
                                "Probable database conflict with pnfs.",
                                e_errors.CONFLICT, request)
                #request['status'] = (e_errors.CONFLICT, rest['status'])
                #print_data_access_layer_format(request['infile'],
                #                               request['outfile'],
                #                               request['file_size'], request)
                #quit() #Harsh, but necessary.

            #Test to verify that all the brands are the same.  If not exit.
            # If so, then the system will function.  If this was not true,
            # then a lot of file clerk key errors could occur.
            if extract_brand(db_bfid) != extract_brand(bfid_brand):
                msg = "All bfids must have the same brand."
                raise EncpError(None, str(msg), e_errors.USERERROR, request)
                #request['status'] = (e_errors.USERERROR, msg)
                #print_data_access_layer_format(request['infile'],
                #                               request['outfile'],
                #                               request['file_size'], request)
                #quit() #Harsh, but necessary.

            #sum up the size to verify there is sufficent disk space.
            try:
                sum_size = sum_size + request['file_size']
            except TypeError:
                pass #If the size is not known (aka None) move on.

        if request['outfile'] != "/dev/null":
            fs_stats = os.statvfs(os.path.dirname(request['outfile']))
            bytes_free = long(fs_stats[statvfs.F_BAVAIL]) * \
                         long(fs_stats[statvfs.F_FRSIZE])
            if  bytes_free < sum_size:
                msg = "Disk is full.  %d bytes available for %d requested." % \
                      (bytes_free, sum_size)
                raise EncpError(None, str(msg), e_errors.USERERROR, request)
                #request['status'] = (e_errors.USERERROR, msg)
                #print_data_access_layer_format(request['infile'],
                #                               request['outfile'],
                #                               request['file_size'], request)
                #quit() #Harsh, but necessary.

        #Create the zero length file entry.
        #for request in request_list:
        #    #Where does this really belong???
        #    create_zero_length_files(request['outfile'])

#######################################################################

def get_clerks_info(vcc, fcc, bfid): #Not used for volume transfers.

    fc_ticket = fcc.bfid_info(bfid=bfid)

    if not e_errors.is_ok(fc_ticket['status'][0]):
        raise EncpError(None,
                        "Failed to obtain information for bfid %s." % bfid,
                        e_errors.EPROTO, fc_ticket)
    if not fc_ticket.get('external_label', None):
        raise EncpError(None,
                        "Failed to obtain information for bfid %s." % bfid,
                        e_errors.EPROTO, fc_ticket)

    vc_ticket = vcc.inquire_vol(fc_ticket['external_label'])

    if not e_errors.is_ok(vc_ticket['status'][0]):
        raise EncpError(None,
                        "Failed to obtain information for external label %s." %
                        fc_ticket['external_label'],
                        e_errors.EPROTO, vc_ticket)
    if not vc_ticket.get('system_inhibit', None):
        raise EncpError(None,
                        "Volume %s did not contain system_inhibit information."
                        % fc_ticket['external_label'],
                        e_errors.EPROTO, vc_ticket)
    if not vc_ticket.get('user_inhibit', None):
        raise EncpError(None,
                        "Volume %s did not contain user_inhibit information."
                        % fc_ticket['external_label'],
                        e_errors.EPROTO, vc_ticket)
    
    inhibit = vc_ticket['system_inhibit'][0]
    if inhibit in (e_errors.NOACCESS, e_errors.NOTALLOWED):
        raise EncpError(None,
                        "Volume %s is marked %s."%(fc_ticket['external_label'],
                                                  inhibit),
                        inhibit, vc_ticket)

    inhibit = vc_ticket['user_inhibit'][0]
    if inhibit in (e_errors.NOACCESS, e_errors.NOTALLOWED):
        raise EncpError(None,
                        "Volume %s is marked %s."%(fc_ticket['external_label'],
                                                  inhibit),
                        inhibit, vc_ticket)

    if fc_ticket["deleted"] == "yes":
        raise EncpError(None,
                        "File %s is marked %s." % (fc_ticket.get('pnfs_name0',
                                                                 "Unknown"),
                                                  e_errors.DELETED),
                        e_errors.DELETED, vc_ticket)

    #Include the server address in the returned info.
    fc_ticket['address'] = fcc.server_address
    vc_ticket['address'] = vcc.server_address

    #Include this information in an easier to access location.
    try:
        vf = vc_ticket['volume_family']
        vc_ticket['storage_group']=volume_family.extract_storage_group(vf)
        vc_ticket['file_family'] = volume_family.extract_file_family(vf)
        vc_ticket['wrapper'] = volume_family.extract_wrapper(vf)
    except (ValueError, AttributeError, TypeError,
            IndexError, KeyError), msg:
        raise EncpError(None, str(msg), e_errors.KEYERROR)
        #print_data_access_layer_format(
        #    ifullname, ofullname, file_size,
        #    {'status':(e_errors.EPROTO, str(msg))})
        #quit()
        
    #Return the information.
    return vc_ticket, fc_ticket

#######################################################################
#Functions for reads.
#######################################################################

def create_read_requests(callback_addr, routing_addr, tinfo, e):

    nfiles = 0
    requests_per_vol = {}

    #vcc = fcc = None #Initialize these, so that they can be set only once.
    
    # create internal list of input unix files even if just 1 file passed in
    if type(e.input)==types.ListType:
        e.input = e.input
    else:
        e.input = [e.input]

    # create internal list of input unix files even if just 1 file passed in
    if type(e.output)==types.ListType:
        e.output = e.output
    else:
        e.output = [e.output]

    # Paranoid check to make sure that the output has only one element.
    if len(e.output)>1:
        print e.output, type(e.output)
        raise EncpError(None,
                        'Cannot have multiple output files',
                        e_errors.USERERROR)

    if e.volume: #For reading a VOLUME (--volume).

        # get_clerks() can determine which it is and return the volume_clerk
        # and file clerk that it corresponds to.
        try:
            vcc, fcc = get_clerks(e.volume)
        except EncpError, msg:
            print_data_access_layer_format(
                e.input, e.output, 0,
                {'status':(msg.type, msg.strerror)})
            quit(1)
        try:
            #Get the system information from the clerks.  In this case
            # e.input[i] doesn't contain the filename, but the volume.
            #vc_reply, fc_reply = get_clerks_info(vcc, fcc, e.input[i])
            #reply = fcc.list_active(e.input[0])
            #bfids_dict = fcc.get_bfids(e.input[0])
            bfids_dict = fcc.get_bfids(e.volume)
            bfids_list = bfids_dict['bfids']
        except EncpError, msg:
            print_data_access_layer_format(
                e.input, e.output, 0,
                {'status':(msg.type, msg.strerror)})
            quit(1)

        if not e_errors.is_ok(bfids_dict):
            print_data_access_layer_format(
                e.input, e.output, 0, {'status':bfids_dict['status']})
            quit(1)

        #Set these here.
        if e.list:
            #If a list was supplied, determine how many files are in the list.
            try:
                f = open(e.list, "r")
                list_of_files = f.readlines()
                number_of_files = len(list_of_files)
                f.close()
            except (IOError, OSError), msg:
                print_data_access_layer_format(
                    e.input, e.output, 0,
                    {'status' : ("Unable to read list file", str(msg))})
                quit(1)
        else:        
            number_of_files = len(bfids_dict['bfids'])
            #Always say one (for the ticket to send to the LM) when the
            # number of files is unkown.
            if number_of_files == 0:
                number_of_files = 1
    else: # Normal read, --get-dcache, --put-cache, --get-bfid.
        number_of_files = len(e.input)

    # check the input unix file. if files don't exits, we bomb out to the user
    for i in range(number_of_files):

        #### VOLUME from list #############################################
        if e.volume and e.list:

            #Just be sure that the output target is a directory.
            if not os.path.exists(e.output[0]):
                print_data_access_layer_format(
                    e.volume, e.output[0], 0,
                    {'status':(e_errors.USERERROR,
                               "%s: %s" % (errno.errorcode[errno.ENOENT],
                                           e.output[0]))})
                quit(1)

                raise EncpError(errno.ENOENT,
                                e.output[0],
                                e_errors.USERERROR)
            elif e.output[0] != "/dev/null" and not os.path.isdir(e.output[0]):
                print_data_access_layer_format(
                    e.volume, e.output[0], 0,
                    {'status':(e_errors.USERERROR,
                               "%s: %s" % (errno.errorcode[errno.ENOTDIR],
                                           e.output[0]))})
                quit(1)

            #Get the number and filename for the next line on the file.
            number, ifilename = list_of_files[i].split()[:2]

            ifilename = os.path.join(e.input[0], ifilename)
            imachine, ifullname, idir, ibasename = fullpath(ifilename)

            # Check the file number on the tape to make sure everything is
            # correct.  This mass of code obtains a bfid or determines
            # that it is not known.
            try:
                tape_ticket = fcc.tape_list(e.volume)

                #First check for errors.
                if not e_errors.is_ok(tape_ticket):
                    raise EncpError(None, "Error obtaining tape listing.",
                                    e_errors.BROKEN)

                #If everythin is okay, search the listing for the location
                # of the file requested.
                lc = generate_location_cookie(number)
                for item in tape_ticket.get("tape_list", {}).values():
                    #For each file number on the tape, compare it with
                    # a location cookie in the list of tapes.
                    if same_cookie(item['location_cookie'], lc):
                        status = (e_errors.USERERROR,
                                  "Requesting file (%s) that has been deleted."
                                  % (lc,))
                        #Check to make sure that this file is not marked
                        # as deleted.  If so, print error and exit.
                        if item['deleted'] == "yes":
                            print_data_access_layer_format(
                                ifullname, "", 0,
                                {'status' : status, 'volume' : e.volume})
                            quit(1)
                            
                        #If the file is already known to enstore.
                        bfid = item['bfid']
                        p = pnfs.Pnfs(ifullname)
                        break
                else:
                    #If we get here then we don't know anything about
                    # the file location requested.
                    p = pnfs.Pnfs()
                    bfid = None
            except:
                exc, msg = sys.exc_info()[:2]
                print_data_access_layer_format(
                    ifullname, "", 0,
                    {'status' : (str(exc), str(msg)), 'volume':e.volume})
                quit(1)

            file_size = None
            read_work = 'read_from_hsm'

            if e.output[0] == "/dev/null":
                ofullname = e.output[0]
            else:
                ofilename = os.path.join(e.output[0],
                                         os.path.basename(ifullname))
                omachine, ofullname, odir, obasename = fullpath(ofilename)

            
            # get_clerks() can determine which it is and return the
            # volume_clerk and file clerk that it corresponds to.
            try:
                vcc, fcc = get_clerks(e.volume)
            except EncpError:
                print_data_access_layer_format(
                    e.input, e.output, 0,
                    {'status':(msg.type, msg.strerror)})
                quit(1)

            #Contact the file clerk and get current bfid that is on the list.
            if bfid:
                try:
                    vc_reply, fc_reply = get_clerks_info(vcc, fcc, bfid)
                except EncpError, msg:
                    if msg.type == e_errors.DELETED:
                        continue
                    else:
                        print_data_access_layer_format(
                            e.volume, e.output[0], 0,
                            {'status':(msg.type, msg.strerror)})
                        quit(1)
            else:
                vc_reply = vcc.inquire_vol(e.volume)
                vc_reply['address'] = vcc.server_address
                fc_reply = {'address' : fcc.server_address,
                            'bfid' : None,
                            'complete_crc' : None,
                            'deleted' : None,
                            'drive' : None,
                            'external_label' : e.volume,
                            'location_cookie':generate_location_cookie(number),
                            'pnfs_mapname': None,
                            'pnfs_name0': None,
                            'pnfsid': None,
                            'pnfsvid': None,
                            'sanity_cookie': None,
                            'size': None,
                            'status' : (e_errors.OK, None)
                            }
            
        #### VOLUME #######################################################
        #If the user specified a volume to read.
        elif e.volume:

            #Just be sure that the output target is a directory.
            if not os.path.exists(e.output[0]):
                print_data_access_layer_format(
                    e.volume, e.output[0], 0,
                    {'status':(e_errors.USERERROR,
                               "%s: %s" % (errno.errorcode[errno.ENOENT],
                                           e.output[0]))})
                quit(1)

                raise EncpError(errno.ENOENT,
                                'e.output[0]',
                                e_errors.USERERROR)
            elif e.output[0] != "/dev/null" and not os.path.isdir(e.output[0]):
                print_data_access_layer_format(
                    e.volume, e.output[0], 0,
                    {'status':(e_errors.USERERROR,
                               "%s: %s" % (errno.errorcode[errno.ENOTDIR],
                                           e.output[0]))})
                quit(1)

            # get_clerks() can determine which it is and return the
            # volume_clerk and file clerk that it corresponds to.
            try:
                if bfids_list:
                    vcc, fcc = get_clerks(bfids_list[i])
                else:
                    vcc, fcc = get_clerks(e.volume)
            except EncpError:
                print_data_access_layer_format(
                    e.input, e.output, 0,
                    {'status':(msg.type, msg.strerror)})
                quit(1)
            
            #Contact the file clerk and get current bfid that is on the list.
            try:
                vc_reply, fc_reply = get_clerks_info(vcc, fcc, bfids_list[i])
            except EncpError, msg:
                if msg.type == e_errors.DELETED:
                    continue
                else:
                    print_data_access_layer_format(
                        #e.input[i], e.output[i], 0,
                        e.volume, e.output[0], 0,
                        {'status':(msg.type, msg.strerror)})
                    quit(1)
            except IndexError:
                #This exception is raised if the bfids_list[] is empty, but
                # number_of_files was set to one anyway to force a tape
                # to be ingested.
                vc_reply = vcc.inquire_vol(e.volume)
                vc_reply['address'] = vcc.server_address
                fc_reply = {'address' : fcc.server_address,
                            'bfid' : None,
                            'complete_crc' : None,
                            'deleted' : None,
                            'drive' : None,
                            'external_label' : e.volume,
                            'location_cookie': '0000_000000000_0000001',
                            'pnfs_mapname': None,
                            'pnfs_name0': None,
                            'pnfsid': None,
                            'pnfsvid': None,
                            'sanity_cookie': None,
                            'size': None,
                            'status' : (e_errors.OK, None)
                            }

            if not e_errors.is_ok(vc_reply):
                print_data_access_layer_format(
                        e.volume, e.output[0], 0,
                        {'status':vc_reply['status']})
                quit(1)
                            
            if e_errors.is_ok(fc_reply): # and fc_reply['deleted'] != "yes":
                #Attempt to get the location cookie.  This should NEVER
                # fail.
                try:
                    lc = fc_reply['location_cookie']
                except KeyError:
                    raise EncpError(None, 'No location cookie found.',
                                    e_errors.CONFLICT)

                #Attempt to get the pnfs ID.  If an encp failed in the
                # right way it is possible for this entry to not exist.
                try:
                    pnfsid = fc_reply['pnfsid']
                except KeyError:
                    #If we get here, then a file does not have all
                    # the information in the file database, but does
                    # have the deleted field set to 'no'.
                    pnfsid = None
                    sys.stdout.write("Location %s is active, but the "
                                     "pnfs id is unknown.\n" % lc)
                    #continue

                try:
                    #Using the original directory, try and determine
                    # the new file name.
                    orignal_directory = os.path.dirname(fc_reply['pnfs_name0'])
                    #Try to obtain the file name and path that the
                    # file currently has.
                    p = pnfs.Pnfs(pnfsid, orignal_directory)
                    ifullname = p.get_path() #pnfsid, orignal_directory)

                    if e.output[0] == "/dev/null":
                        ofullname = e.output[0]
                    else:
                        ofullname = os.path.join(e.output[0],
                                                 os.path.basename(ifullname))

                    file_size = get_file_size(ifullname)
                    
                except (OSError, KeyError, AttributeError):
                    #If we get here then there was a problem determining
                    # the name and path of the file.  Most likely, there
                    # is an entry in pnfs but the metadata is not complete.
                    ifullname = os.path.join(e.input[0], lc)

                    p = pnfs.Pnfs(ifullname)
                    
                    if e.output[0] == "/dev/null":
                        ofullname = e.output[0]
                    else:
                        ofullname = os.path.join(e.output[0], lc)
                    
                    file_size = None #Not known.

                    sys.stdout.write("Location %s is active, but the "
                                     "file has been deleted.\n" % lc)
                    #continue

                try:
                    bfid = bfids_list[i] #Set this alias...
                except IndexError:
                    #If we get here then there is no metadata for the volume
                    # for a tape read.  Set dummy values.
                    bfid = None
                    
                read_work = 'read_from_hsm' #'read_tape'

        #### BFID #######################################################
        #If the user specified a bfid for the file to read.
        elif e.get_bfid:

            # get_clerks() can determine which it is and return the
            # volume_clerk and file clerk that it corresponds to.
            try:
                vcc, fcc = get_clerks(e.get_bfid)
            except EncpError:
                print_data_access_layer_format(
                    e.input, e.output, 0,
                    {'status':(msg.type, msg.strerror)})
                quit()
                
            try:
                #Get the system information from the clerks.  In this case
                # e.input[i] doesn't contain the filename, but the bfid.
                vc_reply, fc_reply = get_clerks_info(vcc, fcc, e.get_bfid)
            except EncpError, msg:
                print_data_access_layer_format(
                    e.get_bfid, e.output[0], 0,
                    {'status':(msg.type, msg.strerror)})
                quit()

            pnfsid = fc_reply.get("pnfsid", None)
            if not pnfsid:
                print_data_access_layer_format(
                    e.get_bfid, e.output[0], 0,
                    {'status':(e_errors.KEYERROR,
                               "Unable to obtain pnfsid from file clerk.")})
                quit()

            try:
                p = pnfs.Pnfs(pnfsid, mount_point=e.pnfs_mount_point)
            except (OSError, IOError), msg:
                print_data_access_layer_format(
                    e.get_cache, e.output[0], 0,
                    {'status':(errno.errorcode[getattr(msg,"errno",errno.EIO)],
                               str(msg))})
                quit()

            if e.shortcut:
                ifullname = os.path.join(e.pnfs_mount_point,
                                     ".(access)(%s)" % pnfsid)
            else:
                ifullname = p.get_path()

            if e.output[0] == "/dev/null":
                ofullname = e.output[0]
            else:
                omachine, ofullname, odir, obasename = fullpath(e.output[0])

            file_size = get_file_size(ifullname)

            bfid = e.input[i]

            read_work = 'read_from_hsm'

        #### PNFS ID ###################################################
        elif e.get_cache:
            #pnfs_id = sys.argv[-2]
            #local_file = sys.argv[-1]
            #remote_file=os.popen("enstore pnfs --path " + pnfs_id).readlines()
            #p = pnfs.Pnfs(pnfs_id=pnfs_id, mount_point=self.pnfs_mount_point)
            try:
                #p = pnfs.Pnfs(pnfs_id, mount_point=self.pnfs_mount_point)
                p = pnfs.Pnfs(e.get_cache, mount_point=e.pnfs_mount_point)

                ifullname = p.get_path()

                file_size = get_file_size(ifullname)

                bfid = p.get_bit_file_id()
            except (OSError, IOError), msg:
                print_data_access_layer_format(
                    e.get_cache, e.output[0], 0,
                    {'status':(errno.errorcode[getattr(msg,"errno",errno.EIO)],
                               str(msg))})
                quit()

            # get_clerks() can determine which it is and return the
            # volume_clerk and file clerk that it corresponds to.
            try:
                vcc, fcc = get_clerks(bfid)
            except EncpError:
                print_data_access_layer_format(
                    e.input, e.output, 0,
                    {'status':(msg.type, msg.strerror)})
                quit()

            try:
                #Get the system information from the clerks.
                vc_reply, fc_reply = get_clerks_info(vcc, fcc, bfid)
            except EncpError, msg:
                print_data_access_layer_format(
                    e.get_cache, e.output[0], 0,
                    {'status':(msg.type, msg.strerror)})
                quit()

            if e.output[0] == "/dev/null":
                ofullname = e.output[0]
            else:
                omachine, ofullname, odir, obasename = fullpath(e.output[0])

            read_work = 'read_from_hsm'
            
        #### FILENAME #################################################
        #else the filename was given to encp.
        else:
            ifullname, ofullname = get_ninfo(e.input[i], e.output[0], e)

            #Fundamentally this belongs in veriry_read_request_consistancy(),
            # but information needed about the input file requires this check.
            inputfile_check(ifullname, e)
            
            file_size = get_file_size(ifullname)

            try:
                p = pnfs.Pnfs(ifullname)
                bfid = p.get_bit_file_id()
            except (OSError, IOError), msg:
                print_data_access_layer_format(
                    ifullname, ofullname, file_size,
                    {'status':(errno.errorcode[getattr(msg,"errno",errno.EIO)],
                               str(msg))})
                quit()

            # get_clerks() can determine which it is and return the
            # volume_clerk and file clerk that it corresponds to.
            try:
                vcc, fcc = get_clerks(bfid)
            except EncpError:
                print_data_access_layer_format(
                    e.input, e.output, 0,
                    {'status':(msg.type, msg.strerror)})
                quit()
                
            try:
                #Get the system information from the clerks.
                vc_reply, fc_reply = get_clerks_info(vcc, fcc, bfid)
            except EncpError, detail:
                print_data_access_layer_format(
                    ifullname, ofullname, file_size,
                    {'status':(detail.type, detail.strerror)})
                quit()

            read_work = 'read_from_hsm'

        ##################################################################

        #Print out the replies from the cerks.
        Trace.message(TICKET_LEVEL, "FILE CLERK:")
        Trace.message(TICKET_LEVEL, pprint.pformat(fc_reply))
        Trace.message(TICKET_LEVEL, "VOLUME CLERK:")
        Trace.message(TICKET_LEVEL, pprint.pformat(vc_reply))

        try:
            label = fc_reply['external_label'] #short cut for readablility
        except (KeyError, ValueError, TypeError, AttributeError, IndexError):
            print_data_access_layer_format(
                    ifullname, ofullname, file_size,
                    {'status':(e_errors.KEYERROR,
                               "File clerk resonce did not contain an " \
                               "external label.")})
            quit()

        try:
            # comment this out not to confuse the users
            #if fc_reply.has_key("fc") or fc_reply.has_key("vc"):
            #    sys.stderr.write("Old file clerk format detected.\n")
            del fc_reply['fc'] #Speed up debugging by removing these.
            del fc_reply['vc']
        except:
            pass

        #Get the data aquisition information.
        encp_daq = get_dinfo()

        try:
            #Snag the three pieces of information needed for the wrapper.
            uinfo = get_uinfo()
            finfo = get_finfo(ifullname, ofullname, e)
            pinfo = get_pinfo(p)

            #Combine the data into the wrapper sub-ticket.
            wrapper = get_winfo(pinfo, uinfo, finfo)
            
            #Create the sub-ticket of the command line argument information.
            encp_el = get_einfo(e)
        except EncpError, detail:
            print_data_access_layer_format(
                ifullname, ofullname, file_size,
                {'status':(detail.type, detail.strerror)})
            quit()

        #There is no need to deal with routing on non-multihomed machines.
        config = host_config.get_config()
        if config and config.get('interface', None):
            route_selection = 1
        else:
            route_selection = 0

        # allow library manager selection based on the environment variable
        lm = os.environ.get('ENSTORE_SPECIAL_LIB')
        if lm != None:
            vc_reply['library'] = lm

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
        request['override_ro_mount'] = e.override_ro_mount
        request['retry'] = 0
        request['routing_callback_addr'] = routing_addr
        request['route_selection'] = route_selection
        request['times'] = tinfo.copy() #Only info now in tinfo needed.
        request['unique_id'] = generate_unique_id()
        request['vc'] = vc_reply
        request['version'] = encp_client_version()
        request['volume'] = label
        request['work'] = read_work #'read_from_hsm' or 'read_tape'
        request['wrapper'] = wrapper

        requests_per_vol[label] = requests_per_vol.get(label,[]) + [request]
        nfiles = nfiles+1

    return requests_per_vol

#######################################################################

# submit read_from_hsm requests
def submit_read_requests(requests, tinfo, encp_intf):

    #Sort the requests by location cookie.
    requests.sort(sort_cookie)

    submitted = 0
    requests_to_submit = requests[:] #Don't change the original copy.

    for req in requests_to_submit:
        #Clear this flag such that resubmissions will enter the while loop.
        req['submitted'] = None
        while req.get("submitted", None) == None:
            Trace.message(TRANSFER_LEVEL, 
                     "Submitting %s read request.  elapsed=%s" % \
                     (req['outfile'], time.time() - tinfo['encp_start_time']))

            Trace.trace(18, "submit_read_requests queueing:%s"%(req,))
            
            ticket = submit_one_request(req)
            
            Trace.message(TICKET_LEVEL, "LIBRARY MANAGER")
            Trace.message(TICKET_LEVEL, pprint.pformat(ticket))
            
            result_dict = handle_retries(requests_to_submit, req, ticket,
                                         None, None, None, encp_intf)
            if e_errors.is_retriable(result_dict['status'][0]):
                continue
            elif e_errors.is_non_retriable(result_dict['status'][0]):
                #del requests_to_submit[requests_to_submit.index(req)]
                req['submitted'] = 0 #This in not 1 nor None.
                break
            
            #del requests_to_submit[requests_to_submit.index(req)]
            req['submitted'] = 1
            submitted = submitted + 1

    return submitted, ticket

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
#   did not succed.  bytes is the total running sum of bytes transfered
#   for this encp.

def read_hsm_files(listen_socket, route_server, submitted,
                   request_list, tinfo, e):

    for rq in request_list: 
        Trace.trace(17,"read_hsm_files: %s"%(rq['infile'],))

    files_left = submitted
    bytes = 0L
    failed_requests = []
    succeded_requests = []

    #for waiting in range(submitted):
    while files_left:
        Trace.message(TO_GO_LEVEL, "FILES LEFT: %s" % files_left)
        Trace.message(TRANSFER_LEVEL,
                      "Waiting for mover to call back.  elapsed=%s" %
                      (time.time() - tinfo['encp_start_time'],))
            
        # listen for a mover - see if id corresponds to one of the tickets
        #   we submitted for the volume
        control_socket, data_path_socket, request_ticket = mover_handshake(
            listen_socket, route_server, request_list, e)

        overall_start = time.time() #----------------------------Overall Start

        done_ticket = request_ticket #Make sure this exists by this point.
        result_dict = handle_retries(request_list, request_ticket,
                                     request_ticket, listen_socket,
                                     route_server, None, e)

        if e_errors.is_resendable(result_dict['status']):
            continue
        #if result_dict['status'][0]== e_errors.RETRY or \
        #   result_dict['status'][0]== e_errors.RESUBMITTING:
        #    continue
        elif result_dict['status'][0]== e_errors.TOO_MANY_RESUBMITS:
            for n in range(files_left):
                failed_requests.append(request_ticket)
            files_left = 0
            continue
        elif e_errors.is_non_retriable(result_dict['status'][0]):
            files_left = result_dict['queue_size']
            failed_requests.append(request_ticket)
            continue

        #Be paranoid.  Check this the ticket again.
        try:
            if not e.volume: #Skip this test for volume transfers.
                verify_read_request_consistancy(
              {request_ticket.get("external_label","label"):[request_ticket]},)
        except EncpError, msg:
            msg.ticket['status'] = (msg.type, msg.strerror)
            result_dict = handle_retries(request_list, msg.ticket,
                                         msg.ticket, None, None, None, e)

            if e_errors.is_resendable(result_dict['status']):
                continue
            #if result_dict['status'][0]== e_errors.RETRY or \
            #   result_dict['status'][0]== e_errors.RESUBMITTING:
            #    continue
            elif result_dict['status'][0]== e_errors.TOO_MANY_RESUBMITS:
                for n in range(files_left):
                    failed_requests.append(request_ticket)
                files_left = 0
                continue
            elif e_errors.is_non_retriable(result_dict['status'][0]):
                files_left = result_dict['queue_size']
                failed_requests.append(request_ticket)
                continue

        Trace.message(TRANSFER_LEVEL, "Mover called back.  elapsed=%s" %
                      (time.time() - tinfo['encp_start_time'],))
        Trace.message(TICKET_LEVEL, "REQUEST:")
        Trace.message(TICKET_LEVEL, pprint.pformat(request_ticket))

        #Open the output file.
        done_ticket = open_local_file(request_ticket['outfile'], e)

        result_dict = handle_retries(request_list, request_ticket,
                                     done_ticket, listen_socket,
                                     route_server, None, e)
        if e_errors.is_non_retriable(result_dict['status'][0]):
            files_left = result_dict['queue_size']
            failed_requests.append(request_ticket)

            close_descriptors(control_socket, data_path_socket)
            continue
        else:
            out_fd = done_ticket['fd']

        Trace.message(TRANSFER_LEVEL, "Output file %s opened.  elapsed=%s" %
                  (request_ticket['outfile'],
                   time.time() - tinfo['encp_start_time']))

        #Stall starting the count until the first byte is ready for reading.
        read_fd, write_fd, exc_fd = select.select([data_path_socket], [],
                                                  [data_path_socket], 15 * 60)

        Trace.message(TRANSFER_LEVEL, "Starting transfer.  elapsed=%s" %
                  (time.time() - tinfo['encp_start_time'],))
        
        lap_start = time.time() #----------------------------------------Start

        done_ticket = transfer_file(data_path_socket.fileno(),out_fd,
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
                                     done_ticket, listen_socket,
                                     route_server, None, e)
        
        if e_errors.is_retriable(result_dict['status'][0]):
            close_descriptors(control_socket, data_path_socket, out_fd)
            continue
        elif e_errors.is_non_retriable(result_dict['status'][0]):
            files_left = result_dict['queue_size']
            failed_requests.append(request_ticket)
            close_descriptors(control_socket, data_path_socket, out_fd)
            continue

        #For simplicity combine everything together.
        #done_ticket = combine_dict(result_dict, done_ticket)
        
        Trace.message(TRANSFER_LEVEL, "File %s transfered.  elapsed=%s" %
                      (request_ticket['infile'],
                       time.time() - tinfo['encp_start_time']))
        Trace.message(TICKET_LEVEL, "FINAL DIALOG")
        Trace.message(TICKET_LEVEL, pprint.pformat(done_ticket))

        #These functions write errors/warnings to the log file and put an
        # error status in the ticket.
        verify_file_size(done_ticket) #Verify size is the same.

        #Verfy that the file transfered in tacted.
        result_dict = handle_retries(request_list, request_ticket,
                                     done_ticket, listen_socket,
                                     route_server, None, e)
        
        if e_errors.is_retriable(result_dict['status'][0]):
            close_descriptors(control_socket, data_path_socket, out_fd)
            continue
        elif e_errors.is_non_retriable(result_dict['status'][0]):
            files_left = result_dict['queue_size']
            failed_requests.append(request_ticket)
            close_descriptors(control_socket, data_path_socket, out_fd)
            continue

        #Update the running bytes transfered count.
        bytes = bytes + done_ticket['file_size']
        bytes = bytes - done_ticket.get('bytes_not_transfered', 0)
        
        check_crc(done_ticket, e, out_fd) #Check the CRC.

        #Verfy that the file transfered in tacted.
        result_dict = handle_retries(request_list, request_ticket,
                                     done_ticket, listen_socket,
                                     route_server, None, e)
        if e_errors.is_retriable(result_dict['status'][0]):
            close_descriptors(control_socket, data_path_socket, out_fd)
            continue
        elif e_errors.is_non_retriable(result_dict['status'][0]):
            files_left = result_dict['queue_size']
            failed_requests.append(request_ticket)
            close_descriptors(control_socket, data_path_socket, out_fd)
            continue

        #If no error occured, this is safe to close now.
        close_descriptors(control_socket, data_path_socket, out_fd)

        #Update the last access and modification times respecively.
        update_times(done_ticket['infile'], done_ticket['outfile'])

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

        #Remove the new file from the list of those to be deleted should
        # encp stop suddenly.  (ie. crash or control-C).
        delete_at_exit.unregister(done_ticket['outfile']) #localname

        # remove file requests if transfer completed succesfuly.
        #del(request_ticket)
        del request_list[request_list.index(request_ticket)]
        if files_left > 0:
            files_left = files_left - 1

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
        # the end of all transfers
        calculate_rate(done_ticket, tinfo)

        #With the transfer a success, we can now add the ticket to the list
        # of succeses.
        succeded_requests.append(done_ticket)

    Trace.message(TICKET_LEVEL, "DONE TICKET")
    Trace.message(TICKET_LEVEL, pprint.pformat(done_ticket))

    #Pull out the failed transfers that occured while trying to open the
    # control socket.
    unknown_failed_transfers = []
    for transfer in failed_requests:
        if len(transfer) < 3:
            unknown_failed_transfers.append(transfer)

    #Extract the unique ids for the two lists.
    succeded_ids = []
    failed_ids = []
    for req in succeded_requests:
        try:
            succeded_ids.append(req['unique_id'])
        except KeyError:
            sys.stderr.write("Error obtaining unique id list of successes.\n")
            sys.stderr.write(pprint.pformat(req) + "\n")
    for req in failed_requests:
        try:
            failed_ids.append(req['unique_id'])
        except KeyError:
            sys.stderr.write("Error obtaining unique id list of failures.\n")
            sys.stderr.write(pprint.pformat(req) + "\n")

    #For each transfer that failed without even succeding to open a control
    # socket, print out their data access layer.
    for transfer in request_list:
        if transfer['unique_id'] not in succeded_ids and \
           transfer['unique_id'] not in failed_ids:
            try:
                transfer = combine_dict(unknown_failed_transfers[0], transfer)
                del unknown_failed_transfers[0] #shorten this list.

                print_data_access_layer_format(transfer['infile'],
                                               transfer['outfile'],
                                               transfer['file_size'],
                                               transfer)
            except IndexError, msg:
                #msg = "Unable to print data access layer.\n"
                sys.stderr.write(str(msg)+"\n")

    return failed_requests, bytes, done_ticket

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
    callback_addr, listen_socket = get_callback_addr(e)
    #Get an ip and port to listen for the mover address for routing purposes.
    routing_addr, udp_server = get_routing_callback_addr(e)
    
    #Create all of the request dictionaries.
    requests_per_vol = create_read_requests(callback_addr, routing_addr,
                                            tinfo, e)

    #If this is the case, don't worry about anything.
    if (len(requests_per_vol) == 0):
        quit()

    #This will halt the program if everything isn't consistant.
    try:
        verify_read_file_consistancy(requests_per_vol, e)
        if not e.volume: #Skip these tests for volume transfers.
            verify_read_request_consistancy(requests_per_vol)
    except EncpError, msg:
        msg.ticket['status'] = (msg.type, msg.strerror)
        print_data_access_layer_format("", "", 0, msg.ticket)
        quit()

    #Create the zero length file entry.
    for vol in requests_per_vol.keys():
        #Where does this really belong???
        for request in requests_per_vol[vol]:
            create_zero_length_files(request['outfile'])
    
    #Set the max attempts that can be made on a transfer.
    check_lib = requests_per_vol.keys()    
    max_attempts(requests_per_vol[check_lib[0]][0]['vc']['library'], e)

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
        submitted, reply_ticket = submit_read_requests(request_list, tinfo, e)

        Trace.message(TO_GO_LEVEL, "SUBMITED: %s" % submitted)
        Trace.message(TICKET_LEVEL, pprint.pformat(request_list))

        Trace.message(TRANSFER_LEVEL, "Files queued.   elapsed=%s" %
                      (time.time() - tinfo['encp_start_time']))

        #If at least one submission succeded, follow through with it.
        if submitted != 0:
            #Since request_list contains all of the entires, submitted must
            # also be passed so read_hsm_files knows how many elements of
            # request_list are valid.
            requests_failed, brcvd, data_access_layer_ticket = read_hsm_files(
                listen_socket, udp_server, submitted, request_list, tinfo, e)

            Trace.message(TRANSFER_LEVEL,
                          "Files read for volume %s   elapsed=%s" %
                          (vol, time.time() - tinfo['encp_start_time']))

            if len(requests_failed) > 0 or \
               not e_errors.is_ok(data_access_layer_ticket['status'][0]):
                exit_status = 1 #Error, when quit() called, this is passed in.
                Trace.message(ERROR_LEVEL,
                              "TRANSFERS FAILED: %s" % len(requests_failed))
                Trace.message(TICKET_LEVEL, pprint.pformat(requests_failed))
            #Sum up the total amount of bytes transfered.
            bytes = bytes + brcvd
        else:
            #If all submits fail (i.e using an old encp), this avoids crashing.
            if not e_errors.is_ok(reply_ticket['status'][0]):
                data_access_layer_ticket = reply_ticket
            else:
                data_access_layer_ticket = {}
            exit_status = 1

    Trace.message(TRANSFER_LEVEL, "Files read for all volumes.   elapsed=%s" %
                  (time.time() - tinfo['encp_start_time'],))

    # we are done transferring - close out the listen socket
    close_descriptors(listen_socket)

    #Print to screen the exit status.
    Trace.message(TO_GO_LEVEL, "EXIT STATUS: %d" % exit_status)

    #Finishing up with a few of these things.
    calc_ticket = calculate_final_statistics(bytes, number_of_files,
                                             exit_status, tinfo)

    done_ticket = combine_dict(calc_ticket, data_access_layer_ticket)

    Trace.message(TICKET_LEVEL, "DONE TICKET")
    Trace.message(TICKET_LEVEL, pprint.pformat(done_ticket))

    return done_ticket

##############################################################################
##############################################################################

class EncpInterface(option.Interface):
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
        
        #messages for user options
        self.data_access_layer = 0 # no special listings
        self.verbose = 0           # higher the number the more is output
        self.version = 0           # print out the encp version

        #EXfer optimimazation options
        self.buffer_size = 262144  # 256K: the buffer size
	self.array_size = 3        # the number of 'bins' for threaded trans.
        self.mmap_size = 100663296 # 96M: the mmap offset size
	self.direct_io = 0         # true if direct i/o should be used
	self.mmap_io = 0           # true if memory mapped i/o should be used
	self.threaded_exfer = 0    # true if EXfer should run multithreaded
        
        #options effecting encp retries and resubmits
        self.max_retry = None      # number of times to try again
        self.max_resubmit = None   # number of times to try again
        self.mover_timeout = 15*60 # seconds to wait for mover to call back,
                                   # before resubmitting req. to lib. mgr.
                                   # 15 minutes

        #misc.
        self.output_file_family = '' # initial set for use with --ephemeral or
                                     # or --file-family
        #self.bytes = None          #obsolete???
        #self.test_mode = 0         #obsolete???
        self.pnfs_is_automounted = 0 # true if pnfs is automounted.
        self.override_ro_mount = 0 # Override a tape marked read only to be
                                   # mounted read/write.

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

        option.Interface.__init__(self, args=args, user_mode=user_mode)

        # parse the options
        #self.parse_options()

        # This is accessed globally...
        pnfs_is_automounted = self.pnfs_is_automounted

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
                          option.USER_LEVEL:option.ADMIN,},
        option.LIST:{option.HELP_STRING:
                     "Is a get option only.  Do not use otherwise.",
                     option.VALUE_USAGE:option.REQUIRED,
                     option.VALUE_TYPE:option.STRING,
                     option.USER_LEVEL:option.ADMIN,},
        option.MAX_RETRY:{option.HELP_STRING:
                          "Specifies number of non-fatal errors that can "
                          "occur before encp gives up. (default = 3)",
                          option.VALUE_USAGE:option.REQUIRED,
                          option.VALUE_TYPE:option.INTEGER,
                          option.USER_LEVEL:option.ADMIN,},
        option.MAX_RESUBMIT:{option.HELP_STRING:
                             "Specifies number of resubmissions encp makes "
                             "when mover does not callback. (default = never)",
                             option.VALUE_USAGE:option.REQUIRED,
                             option.VALUE_TYPE:option.INTEGER,
                             option.USER_LEVEL:option.ADMIN,},
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
        option.NO_CRC:{option.HELP_STRING:"Do not perform CRC check.",
                       option.DEFAULT_TYPE:option.INTEGER,
                       option.DEFAULT_VALUE:0,
                       option.USER_LEVEL:option.USER,},
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
                                 option.USER_LEVEL:option.ADMIN,},
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
                          option.USER_LEVEL:option.ADMIN,},
        option.SHORTCUT:{option.HELP_STRING:
                         "Used with dcache transfers to avoid pathname "
                         "lookups of pnfs ids on reads.",
                         option.DEFAULT_TYPE:option.INTEGER,
                         option.DEFAULT_VALUE:1,
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
        # normal parsing of options
        option.Interface.parse_options(self)

        #Process these at the beginning.
        if getattr(self, "help") and self.help:
            ret = self.print_help()
        if getattr(self, "usage") and self.usage:
            ret = self.print_usage()
        if getattr(self, "version") and self.version:
            ret = self.print_version()

        # bomb out if we don't have an input/output if a special command
        # line was given.  (--volume, --get-cache, --put-cache, --bfid)
        self.arglen = len(self.args)
        if self.arglen < 1 :
            print_error(e_errors.USERERROR, "not enough arguments specified")
            self.print_usage()
            sys.exit(1)

        if self.volume:
            #local_file = sys.argv[-1]
            #remote_file = sys.argv[-2]   #Really just the tape.

            #self.args[0:2] = [remote_file, local_file]
            self.input = None   #[self.args[0]]
            self.output = sys.argv[-1]  #[self.args[self.arglen-1]]
            self.intype = "hsmfile"   #"hsmfile"
            self.outtype = "unixfile"
            return #Don't continue.

        if self.get_bfid:
            #local_file = sys.argv[-1]
            #remote_file = sys.argv[-2]
            #if local_file[:6] == "/pnfs/":
            #    print_data_access_layer_format(
            #        local_file, remote_file, 0,
            #        {'status':(e_errors.USERERROR,
            #                   "Local file cannot begin with '/pnfs/'.")})
            #    quit()
            #self.args[0:2] = [remote_file, local_file]
            self.input = None #[self.args[0]]
            self.output = sys.argv[-1] #[self.args[self.arglen-1]]
            self.intype = "hsmfile"  #What should this bee?
            self.outtype = "unixfile"
            return #Don't continue.
        
        #if self.get_cache and self.shortcut:
        #    pnfs_id = sys.argv[-2]
        #    local_file = sys.argv[-1]
        #    if local_file[:6] == "/pnfs/":
        #        print_data_access_layer_format(
        #            local_file, remote_file, 0,
        #            {'status':(e_errors.USERERROR,
        #                       "Local file cannot begin with '/pnfs/'.")})
        #        quit()
        #    remote_file = os.path.join(self.pnfs_mount_point,
        #                               ".(access)(%s)" % pnfs_id)
        #    self.args[0:2] = [remote_file, local_file]
        #    self.input = [self.args[0]]
        #    self.output = [self.args[self.arglen-1]]
        #    self.intype = "hsmfile"
        #    self.outtype = "unixfile"
        #    return #Don't continue.
        if self.get_cache:
            #pnfs_id = sys.argv[-2]
            #local_file = sys.argv[-1]
            #remote_file=os.popen("enstore pnfs --path " + pnfs_id).readlines()
            #p = pnfs.Pnfs(pnfs_id=pnfs_id, mount_point=self.pnfs_mount_point)
            #try:
            #    p = pnfs.Pnfs(pnfs_id, mount_point=self.pnfs_mount_point)
            #    p.get_path()
            #    remote_file = p.path
            #except (OSError, IOError), msg:
            #    print_data_access_layer_format(
            #        local_file, pnfs_id, 0,
            #        {'status':(errno.errorcode[getattr(msg,"errno",errno.EIO)],
            #                   str(msg))})
            #    quit()
                
            #self.args[0:2] = [remote_file[0][:-1], local_file]
            #self.args[0:2] = [remote_file, local_file]
            self.input = None  #[self.args[0]]
            self.output = sys.argv[-1]  #[self.args[self.arglen-1]]
            self.intype = "hsmfile"   #What should this bee?
            self.outtype = "unixfile"
            return #Don't continue.
        
        if self.put_cache:
            #pnfs_id = sys.argv[-2]
            #local_file = sys.argv[-1]
            #remote_file=os.popen("enstore pnfs --path " + pnfs_id).readlines()
            #p = pnfs.Pnfs(pnfs_id=pnfs_id, mount_point=self.pnfs_mount_point)
            #try:
            #    p = pnfs.Pnfs(pnfs_id, mount_point=self.pnfs_mount_point)
            #    p.get_path()
            #    remote_file = p.path
            #except (OSError, IOError), msg:
            #    print_data_access_layer_format(
            #        local_file, pnfs_id, 0,
            #        {'status':(errno.errorcode[getattr(msg,"errno",errno.EIO)],
            #                   str(msg))})
            #    quit()
            #self.args[0:2] = [local_file, remote_file[0][:-1]]
            #self.args[0:2] = [local_file, remote_file]

            self.input = sys.argv[-1]  #[self.args[0]]
            self.output = None  #[self.args[self.arglen-1]]
            self.intype = "unixfile"
            self.outtype = "hsmfile"  #What should this bee?
            return #Don't continue.


        # bomb out if we don't have an input and an output
        self.arglen = len(self.args)
        if self.arglen < 2 :
            print_error(e_errors.USERERROR, "not enough arguments specified")
            self.print_usage()
            sys.exit(1)

        #Determine whether the files are in /pnfs or not.
        p = []
        for i in range(0, self.arglen):
            #Get fullpaths to the files.
            (machine, fullname, directory, basename) = fullpath(self.args[i])
            self.args[i] = fullname  #os.path.join(dir,basename)
            #If the file is a pnfs file, store a 1 in the list, if not store
            # a zero.  All files on the hsm system have /pnfs/ as 1st part
            # of their name.  Scan input files for /pnfs/ - all have to be the
            # same.  Pass check_name_only a python true value to skip file
            # existance/permission tests at this time.
            
            p.append(pnfs.is_pnfs_path(fullname, check_name_only = 1))
            
        #Initialize some important values.

        #The p# variables are used as holders for testing if all input files
        # are unixfiles or hsmfiles (aka pnfs files).
        p1 = p[0]
        p2 = p[self.arglen - 1]

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
        for i in range(1, len(self.args) - 1):
            if p[i] != p1:
                msg = "Not all input_files are %s files"
                if p2:
                    print_error(e_errors.USERERROR, msg % "/pnfs/...")
                else:
                    print_error(e_errors.USERERROR, msg % "unix")
                quit()
            else:
                self.input.append(self.args[i]) #Do this way for a copy.

        #Assign the collection of types to these variables.
        if p1 == 1:
            self.intype="hsmfile"
        else:
            self.intype="unixfile"
        if p2 == 1:
            self.outtype="hsmfile"
        else:
            self.outtype="unixfile"

##############################################################################

def main(intf):
    #Snag the start time.  t0 is needed by the mover, but its name conveys
    # less meaning.
    encp_start_time = time.time()
    tinfo = {'encp_start_time':encp_start_time,
             't0':int(encp_start_time)}
    
    Trace.init("ENCP")

    #for opt in encp.deprecated_options:
    #    if opt in sys.argv:
    #        print "WARNING: option %s is deprecated, ignoring" % (opt,)
    #        sys.argv.remove(opt)
    
    # use class to get standard way of parsing options
    #e = encp()
    #e = EncpInterface(sys.argv, 0) # zero means admin
    #if e.test_mode:
    #    print "WARNING: running in test mode"

    for x in xrange(6, intf.verbose+1):
        Trace.do_print(x)
    for x in xrange(1, intf.verbose+1):
        Trace.do_message(x)

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
    if not intf.output:
        t=None
    elif os.path.isdir(intf.output[0]):
        t=pnfs.Tag(intf.output[0])
    else:
        t=pnfs.Tag(os.path.dirname(intf.output[0]))
    try:
        library = t.get_library()
    except (OSError, IOError, KeyError, TypeError, AttributeError):
        library = "Unknown"
    try:
        storage_group = t.get_storage_group()
    except (OSError, IOError, KeyError, TypeError, AttributeError):
        storage_group = "Unknown"
    try:
        file_family = t.get_file_family()
    except (OSError, IOError, KeyError, TypeError, AttributeError):
        file_family = "Unknown"
    try:
        file_family_wrapper = t.get_file_family_wrapper()
    except (OSError, IOError, KeyError, TypeError, AttributeError):
        file_family_wrapper = "Unknown"
    try:
        file_family_width = t.get_file_family_width()
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
    except (OSError, socket.error):
        hostname = "invalid_hostname"
        
    #Other strings for the log file.
    start_line = "Start time: %s" % time.ctime(encp_start_time)
    command_line = "Command line: %s" % (string.join(sys.argv),)
    version_line = "Version: %s" % (encp_client_version().strip(),)
    id_line = "User: %s(%d)  Group: %s(%d)  Euser: %s(%d)  Egroup: %s(%d)" %\
              (real_name, os.getuid(), real_group, os.getgid(),
               user_name, os.geteuid(), user_group, os.getegid())
    tag_line = "Library: %s  Storage Group: %s  File Family: %s  " \
               "FF Wrapper: %s  FF Width: %s" % \
               (library, storage_group,
                file_family, file_family_wrapper, file_family_width)
    cwd_line = "Current working directory: %s:%s" % (hostname, cwd)

    #Print this information to make debugging easier.
    Trace.message(DONE_LEVEL, start_line)
    Trace.message(DONE_LEVEL, id_line)
    Trace.message(DONE_LEVEL, command_line)
    Trace.message(DONE_LEVEL, version_line)
    if intf.outtype == "hsmfile":
        Trace.message(DONE_LEVEL, tag_line)
    Trace.message(DONE_LEVEL, cwd_line)

    #Print out the information from the command line.
    Trace.message(CONFIG_LEVEL, format_class_for_print(intf, "intf"))

    #Some globals are expected to exists for normal operation (i.e. a logger
    # client).  Create them.
    client = clients()
    #Report on the success of getting the csc and logc.
    Trace.message(CONFIG_LEVEL, format_class_for_print(client['csc'], 'csc'))
    Trace.message(CONFIG_LEVEL, format_class_for_print(client['logc'], 'logc'))

    # convenient, but maybe not correct place, to hack in log message
    # that shows how encp was called.
    if intf.outtype == "hsmfile":  #write
        Trace.log(e_errors.INFO, "%s  %s  %s  %s  %s" %
                  (version_line, id_line, tag_line, cwd_line, command_line))
    else:                       #read
        Trace.log(e_errors.INFO, "%s  %s  %s  %s" %
                  (version_line, id_line, cwd_line, command_line))

    if intf.data_access_layer:
        global data_access_layer_requested
        data_access_layer_requested = intf.data_access_layer
        #data_access_layer_requested.set()

    #Special handling for use with dcache - not yet enabled
    if intf.get_cache:
        #pnfs_id = sys.argv[-2]
        #local_file = sys.argv[-1]
        #print "pnfsid", pnfs_id
        #print "local file", local_file
        done_ticket = read_from_hsm(intf, tinfo)

    #Special handling for use with dcache - not yet enabled
    elif intf.put_cache:
        #pnfs_id = sys.argv[-2]
        #local_file = sys.argv[-1]
        done_ticket = write_to_hsm(intf, tinfo)
        
    ## have we been called "encp unixfile hsmfile" ?
    elif intf.intype=="unixfile" and intf.outtype=="hsmfile" :
        done_ticket = write_to_hsm(intf, tinfo)
        

    ## have we been called "encp hsmfile unixfile" ?
    elif intf.intype=="hsmfile" and intf.outtype=="unixfile" :
        done_ticket = read_from_hsm(intf, tinfo)


    ## have we been called "encp unixfile unixfile" ?
    elif intf.intype=="unixfile" and intf.outtype=="unixfile" :
        emsg="encp copies to/from tape. It is not involved in copying %s to %s" % (intf.intype, intf.outtype)
        print_error('USERERROR', emsg)
        if intf.data_access_layer:
            print_data_access_layer_format(intf.input, intf.output, 0,
                                           {'status':("USERERROR",emsg)})
        quit()

    ## have we been called "encp hsmfile hsmfile?
    elif intf.intype=="hsmfile" and intf.outtype=="hsmfile" :
        emsg=  "encp tape to tape is not implemented. Copy file to local disk and them back to tape"
        print_error('USERERROR', emsg)
        if intf.data_access_layer:
            print_data_access_layer_format(intf.input, intf.output, 0,
                                           {'status':("USERERROR",emsg)})
        quit()

    else:
        emsg = "ERROR: Can not process arguments %s"%(intf.args,)
        Trace.trace(16,emsg)
        print_data_access_layer_format("","",0,{'status':("USERERROR",emsg)})
        quit()

    exit_status = done_ticket.get('exit_status', 1)
    try:
        #Log the message that tells us that we are done.
        status = done_ticket.get('status', (e_errors.UNKNOWN,e_errors.UNKNOWN))
        Trace.log(e_errors.INFO, string.replace(status[1], "\n\t", "  "))

        if intf.data_access_layer and not exit_status:
            #If there was no error and they want the data access layer anyway,
            # print it out.
            print_data_access_layer_format(intf.input, intf.output,
                                           done_ticket.get('file_size', 0),
                                           done_ticket)
        else:
            #If There was an error print the message.
            Trace.message(DONE_LEVEL, str(status[1]))

    except ValueError:
        exc, msg = sys.exc_info()[:2]
        sys.stderr.write("Error (main): %s: %s\n" % (str(exc), str(msg)))
        sys.stderr.write("Exit status: %s\n", exit_status)
        quit(1)

    Trace.trace(20,"encp finished at %s"%(time.ctime(time.time()),))
    #Quit safely by Removing any zero length file for transfers that failed.
    quit(exit_status)


def do_work(intf):
    delete_at_exit.setup_signal_handling()

    try:
        main(intf)
        quit(0)
    except SystemExit, msg:
        quit(1)
    #except:
        #exc, msg, tb = sys.exc_info()
        #sys.stderr.write("%s\n" % (tb,))
        #sys.stderr.write("%s %s\n" % (exc, msg))
        #quit(1)
        
        
if __name__ == '__main__':

    intf = EncpInterface(sys.argv, 0) # zero means admin

    do_work(intf)
