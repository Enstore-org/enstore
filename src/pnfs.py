#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

"""
TO DO:
1) Eliminate use of the pnfs.File class.  (Same for chimera.py too.)
   Only migration in one location and the file_clerk_client --restore
   command still depend on the File.update() function.
2) Add commands similar to those added for pnfs_agent_client.py to make
   maintaining the encp regression tests easier.  (Same for "enstore sfs".)
"""

# system imports
import sys
import os
import errno
import stat
import pwd
import grp
import string
import time
import _strptime   #Make freeze work, this ticks off pychecker.
import re
import types
import socket
import threading

# enstore imports
import Trace
import e_errors
#try:
#    import Devcodes # this is a compiled enstore module
#except ImportError:
#    Trace.log(e_errors.INFO, "Devcodes unavailable")
import option
import enstore_constants
import hostaddr
import enstore_functions2
import charset
import atomic
import file_utils

#ENABLED = "enabled"
#DISABLED = "disabled"
#VALID = "valid"
#INVALID =  "invalid"
UNKNOWN = "unknown"  #Same in namespace and chimera.
#EXISTS = "file exists"
#DIREXISTS = "directory exists"
ERROR = -1

PATH_MAX = 199

##############################################################################

#This is used to print out some of the results to the terminal that are more
# than one line long and contained in a list.  The list is usually generated
# by a f.readlines() where if is a file object.  Otherwise the result is
# printed as is.
def print_results(result):
    if type(result) == types.ListType:
         for line in result:
            print line, #constains a '\012' at the end.
    else:
        print result
def print_results2(result):
    if type(result) == types.ListType:
         for line in result:
            print line #no '\012' at the end.
    else:
        print result

#Make this shortcut so there is less to type.
fullpath = enstore_functions2.fullpath

#######################################################################

def layer_file(f, n):
    pn, fn = os.path.split(f)
    if is_access_name(fn):
        return os.path.join(pn, "%s(%d)" % (fn, n))
    else:
        return os.path.join(pn, ".(use)(%d)(%s)" % (n, fn))

def id_file(f):
    #We need to be careful that a .(access)() file does not get passed here.
    pn, fn = os.path.split(f)
    return os.path.join(pn, ".(id)(%s)" % (fn, ))

def parent_file(f, pnfsid = None):
    pn, fn = os.path.split(f)
    if pnfsid:
        return os.path.join(pn, ".(parent)(%s)" % (pnfsid))
    if is_access_name(f):
        pnfsid = fn[10:-1]
        return os.path.join(pn, ".(parent)(%s)" % (pnfsid))
    else:
        fname = id_file(f)
        f = file_utils.open(fname, unstable_filesystem=True)
        try:
            pnfsid = file_utils.readline(f, unstable_filesystem=True)
        finally:
            f.close()
        return os.path.join(pn, ".(parent)(%s)" % (pnfsid))

def access_file(dn, pnfsid):
    return os.path.join(dn, ".(access)(%s)" % (pnfsid))

def database_file(directory):
    return os.path.join(directory, ".(get)(database)")

def showid_file(dn, pnfsid):
    return os.path.join(dn, ".(showid)(%s)" % (pnfsid,))

##############################################################################

def is_access_name(filepath):
    #Determine if it is an ".(access)()" name.
    access_match = re.compile("\.\(access\)\([0-9A-Fa-f]+\)")
    if re.search(access_match, os.path.basename(filepath)):
        return True

    return False

def is_access_path(filepath):
    #Determine if it is an ".(access)()" name.
    access_match = re.compile("\.\(access\)\([0-9A-Fa-f]+\)")
    if re.search(access_match, filepath):
        return True

    return False

def is_layer_access_name(filepath):
    #Determine if it is an ".(access)(pnfsid)(1-8)" name.
    access_match = re.compile("\.\(access\)\([0-9A-Fa-f]+\)\([1-8]\)")
    if re.search(access_match, os.path.basename(filepath)):
        return True

    return False

def is_nameof_name(filepath):
    #Determine if it is an ".(access)()" name.
    nameof_match = re.compile("\.\(nameof\)\([0-9A-Fa-f]+\)")
    if re.search(nameof_match, os.path.basename(filepath)):
        return True

    return False

def is_parent_name(filepath):
    #Determine if it is an ".(parent)()" name.
    nameof_match = re.compile("\.\(parent\)\([0-9A-Fa-f]+\)")
    if re.search(nameof_match, os.path.basename(filepath)):
        return True

    return False

#Remove all .(access)(), .(nameof)(), etc. paths from the specified path,
# then return that string.
#
#Using .(access)() paths for directories is dangerous.  Paths can be created
# that form graphs and not trees.  Linux assumes filesystems can only look
# like trees and the kernel can hang in a circular loop.
def remove_special_paths(filepath):
    rtn_path = filepath
    while is_access_name(rtn_path) or is_nameof_name(rtn_path) or \
          is_parent_name(rtn_path):
        rtn_path = os.path.dirname(rtn_path)

    return rtn_path

##############################################################################

def is_pnfs_path(pathname, check_name_only = None):
    if not pathname:  #Handle None and empty string.
        return False

    #Try and find a matching mount point.
    for cached_item in process_mtab():
        mount_point = cached_item[DB_MOUNT_POINTS][0]
        if mount_point and pathname.startswith(mount_point):
            break
    else:
        #Next try and find it after removing symbolic links.  We want to
        # avoid this is possible for performance.
        use_pathname = file_utils.wrapper(os.path.realpath, (pathname,),
                                          unstable_filesystem=True)
        for cached_item in process_mtab():
            mount_point = cached_item[DB_MOUNT_POINTS][0]
            if mount_point and use_pathname.startswith(mount_point):
                break
        else:
            return False

    #If the pathname existance test should be skipped, return true at
    # this time.
    if check_name_only:
        return True

    #If check_name_only is python false then we can reach this check
    # that checks to make sure that the filename exists.  Use os.stat()
    # instead of os.path.exist(), since the later was found to be returning
    # ENOENT errors that really should have been EACCES errors.
    try:
        if get_stat(pathname, unstable_filesystem=True):
            return True
    except (OSError, IOError), msg:
        if msg.args[0] == errno.ENOENT:
            pass
        else:
            return True

    #If we get here, then the path contains a directory named 'pnfs' but does
    # not point to a pnfs directory.
    return False

def is_normal_pnfs_path(pathname, check_name_only = None):
    rtn = is_pnfs_path(pathname, check_name_only)
    if rtn:
        #Additional check to make sure that this is a normal path.  Remove
        # the directory component seperator "/" from the character list.
        basename_charset = charset.filenamecharset.replace("/", "")
        if re.search("/pnfs/[%s]*/usr/" % (basename_charset), pathname):
            rtn = 0 #Admin path.

    return rtn


def is_admin_pnfs_path(pathname, check_name_only = None):
    rtn = is_pnfs_path(pathname, check_name_only)
    if rtn:
        #Additional check to make sure that this is an admin path.  Remove
        # the directory component seperator "/" from the character list.
        basename_charset = charset.filenamecharset.replace("/", "")
        if not re.search("/pnfs/[%s]*/usr/" % (basename_charset), pathname):
            rtn = 0 #Normal path.

    return rtn

def isdir(pathname):
    #Stat()ing an access path can cause cyclic paths.  For these we need to
    # get the showid information.
    if is_access_name(pathname):
        #Exctract the PNFS ID from the path.
        pnfs_id = get_pnfsid(pathname)
        #Get the showid information.
        showid_data = get_showid(os.path.dirname(pathname), pnfs_id)
        if is_showid_directory(showid_data):
            #If python true, this is a directory.
            return True

        return False

    return file_utils.wrapper(os.path.isdir, (pathname,))

def isfile(pathname):
    #Stat()ing an access path can cause cyclic paths.  For these we need to
    # get the showid information.
    if is_access_name(pathname):
        #Exctract the PNFS ID from the path.
        pnfs_id = get_pnfsid(pathname)
        #Get the showid information.
        showid_data = get_showid(os.path.dirname(pathname), pnfs_id)
        if is_showid_regular_file(showid_data):
            #If python true, this is a regular file.
            return True

        return False

    return file_utils.wrapper(os.path.isfile, (pathname,))

def islink(pathname):
    #Stat()ing an access path can cause cyclic paths.  For these we need to
    # get the showid information.
    if is_access_name(pathname):
        #Exctract the PNFS ID from the path.
        pnfs_id = get_pnfsid(pathname)
        #Get the showid information.
        showid_data = get_showid(os.path.dirname(pathname), pnfs_id)
        if is_showid_symbolic_link(showid_data):
            #If python true, this is not a symbolic link.
            return True

        return False

    return file_utils.wrapper(os.path.islink, (pathname,))

def is_pnfsid(pnfsid):
    #This is an attempt to deterime if a string is a pnfsid.
    # 1) Is it a string?
    # 2) Is it 24 characters long?
    # 3) All characters are in the capital hex character set.
    #Note: Does it need to be capital set of hex characters???
    if type(pnfsid) == types.StringType and len(pnfsid) == 24:
        allowable_characters = string.upper(string.hexdigits)
        for c in pnfsid:
            if c not in allowable_characters:
                return 0
        else: #success
            return 1
    return 0

#For is_special_path():
TAG = "TAG"
LAYER = "LAYER"

#Return a python true value if the pathname is a tag or layer file.
# False otherwise.
#For tag files:
# Returning a tuple of the base ID and tag name as a side effect for
# performance.
#For layer files:
# Returning a tuple of the base ID and layer number as a side effect for
# performance.  The base ID is the same as the parent ID.
def is_special_path(pathname, tag=True, layer=True):
    if is_access_name(pathname):

        dname, fname = os.path.split(pathname)
        pnfs_id = get_pnfsid(fname)

        showid_name = showid_file(dname, pnfs_id)
        fp = file_utils.open(showid_name, "r")
        try:
            data = fp.readlines()
        finally:
            fp.close()
        if len(data) >= 25:
            #Based on the number of lines read in we can tell if we are short
            # in the expected response.  Inherited tags have 25 lines,
            # locally set ones have 26.

            #Tags and layers have the type listed on line 9 (index 8).
            file_type_line = data[8]

            if tag and file_type_line[17:].startswith("Tag"):
                #File type line will look like this for tags:
                # Type          : Tag ( Inode )
                base_id = data[4][17:].strip()
                name = data[9][12:].strip()
                return (TAG, base_id, name)
            if layer and file_type_line[17:].startswith("Regular ( Data )"):
                #File type line will look like this for layers:
                # Type          : Regular ( Data )
                base_id = data[4][17:].strip()
                parent_id = data[5][17:].strip()
                #The parent ID and base ID need to match and not be all zeros.
                # This is to tell the difference between a layer file and
                # a regluar file with data written to it.
                if base_id == parent_id and \
                       base_id != "000000000000000000000000":
                    base_showid_name = showid_file(dname, base_id)
                    base_fp = file_utils.open(base_showid_name, "r")
                    try:
                        base_data = base_fp.readlines()
                    finally:
                        base_fp.close()
                    #Need to find the line that matches for this layer.
                    for line in base_data[24:]:  #Skip the headers.
                        if line.find(pnfs_id) != -1:
                            #Matching lines look like:
                            # Chain(1)         : 0001000000000000000DE948
                            #Also, layers are one character long 0 through 7,
                            # which makes grabbing the number easy.
                            rtn_layer = int(line[7])
                            return (LAYER, base_id, rtn_layer)

    return False

def is_tag_path(pathname):
    return is_special_path(pathname, tag=True, layer=False)

def is_layer_path(pathname):
    return is_special_path(pathname, tag=False, layer=True)

##############################################################################

#Remove the /pnfs/, /pnfs/fnal.gov/usr or /pnfs/fs/usr/ from the pnfs path.
def strip_pnfs_mountpoint(pathname):
    tmp1 = pathname[pathname.find("/pnfs/"):]
    tmp2 = tmp1[6:]

    #Determine the canonical path base.  (i.e /pnfs/fnal.gov/usr/)
    # If the ENCP_CANONICAL_DOMAINNAME overriding environmental variable
    # is set, use that.
    if os.environ.get('ENCP_CANONICAL_DOMAINNAME', None):
        canonical_name = os.environ['ENCP_CANONICAL_DOMAINNAME']
    else:
        canonical_name = string.join(socket.getfqdn().split(".")[1:], ".")
    canonical_path = os.path.join(canonical_name, "usr")

    if tmp2[:7] == "fs/usr/":
        tmp3 = tmp2[7:]
    elif tmp2[:len(canonical_path)] == canonical_path:
        tmp3 = tmp2[len(canonical_path):]
    else:
        tmp3 = tmp2
    return tmp3

#Return the directory for the requested path.  For normal paths, this is
# just the directory part.  For PNFS/Chimera .(access)() files, there is
# additional handling.
#
#Note: Do not return paths like the following:
# /pnfs/data3/.(access)(000000000000000000001080)/data3/zalokar/NULL/regression_testing/encp_test_for_zalokar/
# This path should resolve to the correct i-nodes, but for Linux kernels
# this can cause the kernel to go into an infinite loop.  See
# https://plone4.fnal.gov/P0/Enstore_and_Dcache/developers/enstore-developers/documents/encp-investigation-of-inc000000056879-pbi000000000147/
# for more information.
def get_directory_name(filepath):
    if type(filepath) != types.StringType:
        return None

    #If we already have a directory...
    #if file_utils.wrapper(os.path.isdir, (filepath,)):
    #    return filepath

    #Determine if it is an ".(access)()" name.
    if is_access_name(filepath) or is_nameof_name(filepath):
        #Since, we have the .(access)() name we need to split off the id.
        dirname, filename = os.path.split(filepath)
        pnfsid = filename[10:-1]  #len(".(access)(") == 10 and len ")" == 1
        #Remove all trailing .(access)(), .(nameof)(), .(parent)(), etc. path
        # components.
        use_dir = remove_special_paths(dirname)
        #Get the parent ID.
        return_value = is_tag_path(filepath)
        if return_value and return_value[0] == TAG:
            #We have a tag pnfs_id.  Use the base id returned in
            # return_value[1] instead.
            parent_id = return_value[1]
        else:
            #Regular files and layer files.
            parent_id = get_parent_id(use_dir, pnfsid)

        #Build the .(access)() filename of the parent directory.
        directory_name = os.path.join(use_dir, ".(access)(%s)" % parent_id)
    else:
        directory_name = os.path.dirname(filepath)

    #If the directory is "." we need to expand it.
    return enstore_functions2.expand_path(directory_name)

###############################################################################

# get the database information
# Sample answer:  admin:0:r:enabled:/diskb/pnfs/db/admin
def get_database(f):
    global database_info_cache

    db_a_dirpath = get_directory_name(f)
    database_path = database_file(db_a_dirpath)

    if database_path in database_info_cache.keys():
        database = database_info_cache[database_path]
    else:
        try:
            #Get the .(get)(database) information despite the reuse of the
            # get_layer() funciton.
            database = get_layer(database_path)[0].strip()

            if not is_access_path(database_path):
                #Don't cache indirect .(access)() path directories.

                #Update the database cache.
                db_num = int(database.split(":")[1])
                add_mtab(database, db_num, db_a_dirpath)
        except (OSError, IOError):
            database = None

    return database

def get_layer(layer_filename, max_lines = None):
    RETRY_COUNT = 2

    #Remove .(access)() paths from the directory.
    split_path = os.path.split(layer_filename)
    use_layer_filename = os.path.join(remove_special_paths(split_path[0]),
                                      split_path[1])

    i = 0
    while i < RETRY_COUNT:
        # get info from layer
        try:
            fl = file_utils.open(use_layer_filename, unstable_filesystem=False)
            try:
                if max_lines:
                    layer_info = []
                    i = 0
                    while i < max_lines:
                        layer_info.append(file_utils.readline(
                            fl, unstable_filesystem=True))
                        i = i + 1
                else:
                    layer_info = file_utils.readlines(fl,
                                                      unstable_filesystem=False)
            finally:
                fl.close()
            break
        except (KeyboardInterrupt, SystemExit):
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except (OSError, IOError), msg:
            #Increment the retry count before it is needed to determine if
            # we should sleep or not sleep.
            i = i + 1

            use_errno = getattr(msg, 'errno', msg.args[0])
            if use_errno in [errno.EACCES, errno.EPERM] and os.getuid() == 0:
                #If we get here and the real id is user root, we need to reset
                # the effective user id back to that of root ...
                try:
                    os.seteuid(0)
                    os.setegid(0)
                except OSError:
                    pass
            elif i < RETRY_COUNT:
                #If the problem wasn't permissions, lets give the system a
                # moment to catch up.
                #Skip the sleep if we are not going to try again.
                ##time.sleep(0.1)

                ##It is known that stat() can return an incorrect ENOENT
                ## if pnfs is really loaded.  Is this true for open() or
                ## readline()?  Skipping the time.sleep() makes the scan
                ## much faster.
                raise msg
    else:
        raise msg

    return layer_info

def get_layer_1(f):
    # get bfid from layer 1
    try:
        bfid = get_layer(layer_file(f, 1))
    except (OSError, IOError), detail:
        bfid = None
        if detail.errno in [errno.EACCES, errno.EPERM]:
            raise OSError(detail.errno, "no read permissions for layer 1",
                          detail.filename)
        elif detail.args[0] in [errno.ENOENT, errno.EISDIR]:
            pass
        else:
            raise OSError(errno.EIO, "corrupted layer 1 metadata",
                          detail.filename)

    try:
        bfid = bfid[0].strip()
    except:
        bfid = ""

    return bfid


def get_layer_2(f):
    # get dcache info from layer 2
    try:
        layer2 = get_layer(layer_file(f, 2))
    except (OSError, IOError), detail:
        layer2 = None
        if detail.errno in [errno.EACCES, errno.EPERM]:
            raise OSError(detail.errno, "no read permissions for layer 2",
                          detail.filename)
        elif detail.args[0] in [errno.ENOENT, errno.EISDIR]:
            pass
        else:
            raise OSError(errno.EIO, "corrupted layer 2 metadata",
                          detail.filename)

    l2 = {}
    if layer2:
        try:
            l2['line1'] = layer2[0].strip()
        except IndexError:
            l2['line1'] = None

        try:
            line2 = layer2[1].strip()
        except IndexError:
            line2 = ""

        try:
            hsm_match = re.compile("h=(no|yes)")
            l2['hsm'] = hsm_match.search(line2).group().split("=")[1]
        except AttributeError:
            l2['hsm'] = None

        try:
            crc_match = re.compile("c=[1-9]+:[a-zA-Z0-9]{8}")
            l2['crc'] = long(crc_match.search(line2).group().split(":")[1], 16)
        except AttributeError:
            l2['crc'] = None

        try:
            size_match = re.compile("l=[0-9]+")
            l2['size'] = long(size_match.search(line2).group().split("=")[1])
        except AttributeError:
            l2['size'] = None

        l2['pools'] = []
        for item in layer2[2:]:
            l2['pools'].append(item.strip())

    return l2

def get_layer_4(f, max_lines = None):
    # get xref from layer 4 (?)
    try:
        layer4 = get_layer(layer_file(f, 4), max_lines)
    except (OSError, IOError), detail:
        layer4 = None
        if detail.errno in [errno.EACCES, errno.EPERM]:
            raise OSError(detail.errno, "no read permissions for layer 4",
                          detail.filename)
        elif detail.args[0] in [errno.ENOENT, errno.EISDIR]:
            pass
        else:
            raise OSError(errno.EIO, "corrupted layer 4 metadata",
                          detail.filename)

    l4 = {}
    if layer4:
        try:
            l4['volume'] = layer4[0].strip()
        except IndexError:
            pass
        try:
            l4['location_cookie'] = layer4[1].strip()
        except IndexError:
            pass
        try:
            l4['size'] = layer4[2].strip()
        except IndexError:
            pass
        try:
            l4['file_family'] = layer4[3].strip()
        except IndexError:
            pass
        try:
            l4['original_name'] = layer4[4].strip()
        except IndexError:
            pass
        # map file no longer used
        try:
            l4['pnfsid'] = layer4[6].strip()
        except IndexError:
            pass
        # map pnfsid no longer used
        try:
            l4['bfid'] = layer4[8].strip()
        except IndexError:
            pass
        try:
            l4['drive'] = layer4[9].strip() #optionally present
        except IndexError:
            pass
        try:
            l4['crc'] = layer4[10].strip() #optionally present
        except IndexError:
            pass

    return l4

def get_pnfsid(f):
    if is_access_name(f) or is_nameof_name(f) or is_parent_name(f):
        #Convenient that these all have the same length.
        pnfsid = os.path.basename(f)[10:-1]
        return pnfsid

    #Remove .(access)() paths from the directory.
    split_path = os.path.split(f)
    use_path = os.path.join(remove_special_paths(split_path[0]),
                            split_path[1])

    #Get the id of the file or directory.
    try:
        fname = id_file(use_path)
        f = file_utils.open(fname, unstable_filesystem=True)
        try:
            pnfs_id = file_utils.readline(f, unstable_filesystem=True).strip()
        finally:
            f.close()
    except (KeyboardInterrupt, SystemExit):
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
    except(OSError, IOError), detail:
        pnfs_id = None
        use_errno = getattr(detail, 'errno', detail.args[0])
        if not use_errno == errno.ENOENT or not os.path.ismount(f):
            message = "%s: %s" % (os.strerror(use_errno),
                                  "unable to obtain pnfs id")
            raise OSError(use_errno, message, fname)

    return pnfs_id

#For future compatibility with other storage file systems.
get_id = get_pnfsid

#Get the parent ID of the pnfs_id requested.
def get_parent_id(directory, pnfs_id):
    #Remove .(access)() paths from the directory.
    use_directory = remove_special_paths(directory)

    #Create the filename to obtain the parent id.
    parent_id_name = os.path.join(use_directory, ".(parent)(%s)" % pnfs_id)

    #Read the parent id.
    f = file_utils.open(parent_id_name, unstable_filesystem=True)
    try:
        parent_id = file_utils.readline(f, unstable_filesystem=True).strip()
    finally:
        f.close()

    return parent_id

#Get the PNFS specific showid information for the pnfs_id.
def get_showid(directory, pnfs_id):
    #Remove .(access)() paths from the directory.
    use_directory = remove_special_paths(directory)

    #Create the filename to obtain the parent id.
    show_id_name = os.path.join(use_directory, ".(showid)(%s)" % pnfs_id)

    #Read the parent id.
    f = file_utils.open(show_id_name, unstable_filesystem=True)
    try:
        showid_data = file_utils.readlines(f, unstable_filesystem=True)
    finally:
        f.close()

    return showid_data

#Wrapper around file_utils.get_stat().  This version catches .(access)()
# paths and gets the stat information from showid information.
def get_stat(pathname, unstable_filesystem=False):
    if type(pathname) == types.StringType and is_access_name(pathname):
        pnfs_id = get_id(pathname)
        showid_data = get_showid(os.path.dirname(pathname), pnfs_id)

        #sbi means Stat Base Index.
        if is_showid_directory(showid_data):
            sbi = 9
        elif is_showid_regular_file(showid_data):
            sbi = 11
        elif is_showid_symbolic_link(showid_data):
            sbi = 11  #TO DO: verify this value
        elif is_showid_tag(showid_data):
            #For tags we need to really stat() it to get inherited values.
            return file_utils.get_stat(pathname,
                                       unstable_filesystem=unstable_filesystem)
        elif is_showid_layer(showid_data):
            #For layers the showid information does not contain the stat
            # information.
            return file_utils.get_stat(pathname,
                                       unstable_filesystem=unstable_filesystem)
        else:
            message = "Unknown PNFS file type: %s" % (showid_data[8][20:],)
            raise TypeError(message)

        vsi = 18 #Value Starting Index

        # mst_mode       : 100640
        st_mode = int(showid_data[sbi + 2][vsi:], 8)  #octal
        # mst_ino        : 16877600
        st_ino = int(showid_data[sbi + 1][vsi:])
        #mst_dev        : 1
        st_dev = long(showid_data[sbi][vsi:])
        # mst_nlink      : 1
        st_nlink = int(showid_data[sbi + 3][vsi:])
        # mst_uid        : 9276
        st_uid = int(showid_data[sbi + 4][vsi:])
        # mst_gid        : 1530
        st_gid = int(showid_data[sbi + 5][vsi:])
        # mst_size       : 1024
        st_size = long(showid_data[sbi + 7][vsi:])
        # mst_atime      : Thu Sep  9 10:43:05 2010
        st_atime = int(time.mktime(time.strptime(showid_data[sbi + 8][vsi:-1],
                                                 "%a %b %d %H:%M:%S %Y")))
        # mst_mtime      : Wed Nov  6 15:59:42 2002
        st_mtime = int(time.mktime(time.strptime(showid_data[sbi + 9][vsi:-1],
                                                 "%a %b %d %H:%M:%S %Y")))
        # mst_ctime      : Wed Nov  6 15:59:39 2002
        st_ctime = int(time.mktime(time.strptime(showid_data[sbi + 10][vsi:-1],
                                                 "%a %b %d %H:%M:%S %Y")))

        stat_results = (st_mode, st_ino, st_dev, st_nlink, st_uid, st_gid,
                            st_size, st_atime, st_mtime, st_ctime)
    else:
        stat_results = file_utils.get_stat(pathname, unstable_filesystem=unstable_filesystem)

    return stat_results

###############################################################################

def __is_showid(showid_data, match_string):
    if type(showid_data) == types.ListType and \
           len(showid_data) >= 9 and \
           type(showid_data[8]) == types.StringType:

        if showid_data[8].find(match_string) != -1:
            return True

    else:
        raise TypeError("Expected showid information.")

    return False

#Return True if the showid information belongs to a directory.
# False otherwise.
def is_showid_directory(showid_data):
    return __is_showid(showid_data, "Directory ( Inode )")

#Return True if the showid information belongs to a regular file.
# False otherwise.
def is_showid_regular_file(showid_data):
    return __is_showid(showid_data, "Regular ( Inode )")

#Return True if the showid information belongs to a symbolic link.
# False otherwise.
def is_showid_symbolic_link(showid_data):
    return __is_showid(showid_data, "Link ( Inode )")

#Return True if the showid information belongs to a tag.
# False otherwise.
def is_showid_tag(showid_data):
    return __is_showid(showid_data, "Tag ( Inode )")

#Return True if the showid information belongs to a layer.
# False otherwise.
def is_showid_layer(showid_data):
    return __is_showid(showid_data, "Regular ( Data )")

#Return the specials file's directory ID (aka base id) and its special value.
# For tag files, this is the tag name.  For layer files, this is the layer
# number (only if directory is given).
# None is returned if this is a regular file or directory.
def get_showid_special_file_info(showid_data, directory=None):
    if type(showid_data) == types.ListType and \
           len(showid_data) >= 9 and \
           type(showid_data[8]) == types.StringType:

        #Get the current id.
        try:
            pnfs_id = showid_data[1][16:-1].strip()
        except (TypeError, ValueError):
            raise TypeError("Expected showid information.")

        #Get the base id.  For Tags this is the directory id.  For layers
        # the parent and base ids are the same.
        try:
            base_id = showid_data[4][16:-1].strip()
        except (TypeError, ValueError):
            raise TypeError("Expected showid information.")

        if showid_data[8].find("Tag ( Inode )") != -1:
            #Tag.
            tag_name = showid_data[9][12:-1].strip()

            #Assign the special value.
            special_value = tag_name
        elif showid_data[8].find("Regular ( Data )") != -1:
            if directory:
                #The only way to determine which layer this layer file is,
                # involves looking at the showid information of its
                # parent/base id for a match.
                base_showid = get_showid(base_id, directory)
                for line in base_showid:
                    if line.find(pnfs_id) != -1:
                        layer_number = int(line[7])
                        break
                else:
                    #Dummy value.  This should never happen.
                    layer_number = -1
            else:
                #Dummy value.  Is possible if directory not given.
                layer_number = -1

            #Assign the special value.
            special_value = layer_number

        else:
            #This is not a special file PNFS ID.
            return None

        return (base_id, special_value)
    else:
        raise TypeError("Expected showid information.")

###############################################################################

#Global cache.
# Shared with Pnfs.get_database() and pnfs.get_database().
mount_points_cache = {}  #keyed by mount point.
database_info_cache = {} #Keyed by .(get)(database) content.
# Keys for global cache.
#TO DO:  Move these into a common place for pnfs and chimera.
DB_NUMBER = "db_number"
DB_INFO = "db_info"
DB_MOUNT_POINTS = "db_mount_point"
#Constant for empty mount point in cache.
EMPTY_MOUNT_POINT = {DB_INFO : "",
                     DB_NUMBER : -1,
                     DB_MOUNT_POINTS : ["",],
                     }
# Make copy, don't share reference.
last_db_tried = EMPTY_MOUNT_POINT.copy()
# Lock globals.
pnfs_global_lock = threading.Lock()

#Get currently mounted pnfs mountpoints.
def parse_mtab():
    #Different systems have different names for this file.
    # /etc/mtab: Linux, IRIX
    # /etc/mnttab: SunOS
    # MacOS doesn't have one.
    for mtab_file in ["/etc/mtab", "/etc/mnttab"]:
        try:
            fp = file_utils.open(mtab_file, "r")
            try:
                mtab_data = fp.readlines()
            finally:
                fp.close()
            break
        except (KeyboardInterrupt, SystemExit):
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except OSError, msg:
            if msg.args[0] in [errno.ENOENT]:
                continue
            else:
                raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
    else:
        #Should this raise an error?
        mtab_data = []

    found_mount_points = {}
    found_db_infos = {}
    for line in mtab_data:
        #The 2nd and 3rd items in the list are important to us here.
        data = line[:-1].split()
        mount_point = data[1]
        fs_type = data[2]

        #If the filesystem is not an NFS filesystem, skip it.
        if fs_type != "nfs":
            continue

        try:
            dataname = os.path.join(mount_point, ".(get)(database)")
            db_fp = file_utils.open(dataname, "r", unstable_filesystem=False)
            try:
                db_info = db_fp.readline().strip()
            finally:
                db_fp.close()
        except IOError:
            continue

        db_datas = db_info.split(":")
        #db_datas[0] is the database name
        #db_datas[1] is the database id
        #db_datas[2] is the database (???)
        #db_datas[3] is the database enabled or disabled status
        #db_datas[4] is the database (???)

        #If the database's id is not in the cache, add it along with the
        # mount point that goes with it.
        db_number = int(db_datas[1])

        if db_info not in found_db_infos.keys():
            #Create a new entry for this PNFS database.
            found_db_infos[db_info] = {DB_INFO : db_info,
                                       DB_NUMBER : db_number,
                                       DB_MOUNT_POINTS: [mount_point,],
                                       }
        elif mount_point not in found_db_infos[db_info][DB_MOUNT_POINTS]:
            #Add this mount point to the list for this PNFS database.
            found_db_infos[db_info][DB_MOUNT_POINTS].append(mount_point)

        if mount_point not in found_mount_points.keys():
            #Create a new entry for this PNFS mount point.
            found_mount_points[mount_point] = {DB_INFO : db_info,
                                               DB_NUMBER : db_number,
                                               DB_MOUNT_POINTS: [mount_point,],
                                               }
        #We do not have an elif here, since duplicates of db_info for a
        # mount point are handled by inserting a entry in the cache for
        # the directory that starts the a new database.

    #if not found_db_infos and not found_mount_points:
    #    add_mtab(EMPTY_MOUNT_POINT[DB_INFO], EMPTY_MOUNT_POINT[DB_NUMBER],
    #             EMPTY_MOUNT_POINT[DB_MOUNT_POINTS][0])

    return found_mount_points, found_db_infos


def set_last_db(database_values):
    global last_db_tried

    ## database_info: Should be the pnfs --database output.  Something like:
    ##   cms:9:r:enabled:/diskb/pnfs/db/cms
    ##
    ## database_number: The number of the database.  This should match the
    ## second part of the database_info line.
    ##
    ## mount_point: The current location of the mount point for this
    ## pnfs database.
    if type(database_values) == types.TupleType:
        #Old format:  (database_info, (database_number, mount_point))
        database_info = database_values[0]
        database_number = database_values[1][0]
        mount_point = database_values[1][1]
    elif type(database_values) == types.DictType:
        #New format: {DB_INFO : database_info,
        #             DB_NUMBER : database_number,
        #             DB_MOUNT_POINTS: mount_points,
        #             }
        database_info = database_values[DB_INFO]
        database_number = database_values[DB_NUMBER]
        mount_point = database_values[DB_MOUNT_POINTS]
    else:
        raise TypeError("Expected PNFS DB info, number and mount point.")

    #This should be a list.  It is possible for one database to be mounted
    # multiple times with differrent mount points.
    if type(mount_point) == types.ListType:
        mount_points = mount_point
    else:
        mount_points = [mount_point,]

    last_db_tried = {DB_INFO : database_info,
                     DB_NUMBER : database_number,
                     DB_MOUNT_POINTS: mount_points,
                     }

def get_last_db():
    global last_db_tried
    return last_db_tried

def _process_mtab():
    global mount_points_cache
    global database_info_cache

    pnfs_global_lock.acquire()
    #Grab copies for this thread in case another makes changes.
    try:
        mount_points_cache_copy = mount_points_cache.values()
        database_info_cache_keys = database_info_cache.keys()
    except:
        pnfs_global_lock.release()
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
    pnfs_global_lock.release()

    try:
        for cache_item in mount_points_cache_copy:
            db_num = cache_item[DB_NUMBER]
            mp = cache_item[DB_MOUNT_POINTS][0] #Should only be one long.
            if db_num == 0 or os.path.basename(mp) == "fs":
                #For /pnfs/fs we need to find all of the /pnfs/fs/usr/* dirs.
                use_path = os.path.join(mp, "usr")
                for dname in os.listdir(use_path):
                    tmp_name = os.path.join(use_path, dname)
                    try:
                        #The "True" for the second arguement to for get_stat(),
                        # to use os.lstat() instead of os.stat().  This allows
                        # for symbolic links to be screened out with S_ISLNK.
                        fstat = file_utils.wrapper(file_utils.get_stat,
                                                   (tmp_name, True),
                                                   unstable_filesystem=True)
                    except (OSError, IOError):
                        continue
                    if not stat.S_ISDIR(fstat[stat.ST_MODE]):
                        continue
                    if stat.S_ISLNK(fstat[stat.ST_MODE]):
                        continue
                    # We can't acquire the pnfs_global_lock, because this
                    # get_database() calls add_mtab() that grabs the lock too.
                    tmp_db_info = get_database(tmp_name).strip()
                    if tmp_db_info in database_info_cache_keys:
                        continue

                    #We don't need to worry about modifying the sequence
                    # we are looping over, since we are looping over a copy.
                    tmp_db = int(tmp_db_info.split(":")[1])
                    add_mtab(tmp_db_info, tmp_db, tmp_name)
    except:
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]

def process_mtab():
    global mount_points_cache
    global database_info_cache

    if not mount_points_cache:
        #Sets global mount_points_cache.
        (mount_points_cache, database_info_cache) = parse_mtab()

        #For /pnfs/fs we need to find all of the /pnfs/fs/usr/* dirs.
        _process_mtab()

    return [last_db_tried] + sort_mtab()

def __db_cmp(x, y):
    is_x_fs_usr = x[DB_MOUNT_POINTS][0].find("/fs/usr/") > 0
    is_y_fs_usr = y[DB_MOUNT_POINTS][0].find("/fs/usr/") > 0

    is_x_fs = (x[DB_NUMBER] == 0)
    is_y_fs = (y[DB_NUMBER] == 0)

    #Always put /pnfs/fs last.
    if is_x_fs and not is_y_fs:
        return 1
    elif not is_x_fs and is_y_fs:
        return -1

    #Always put /pnfs/xyz first.
    elif is_x_fs_usr and not is_y_fs_usr:
        return 1
    elif not is_x_fs_usr and is_y_fs_usr:
        return -1

    #The are the same type of path.  Sort by db number.
    if x[DB_NUMBER] < y[DB_NUMBER]:
        return 1
    elif x[DB_NUMBER] > y[DB_NUMBER]:
        return -1

    return 0

#Sort the list of mount points in search order.
def sort_mtab():
    global mount_points_cache

    pnfs_global_lock.acquire()

    try:
        search_list = mount_points_cache.values()  #Return a copy of info.
        #By sorting and reversing, we can leave db number 0 (/pnfs/fs) in
        # the list and it will be sorted to the end of the list.
        search_list.sort(lambda x, y: __db_cmp(x, y))
    except:
        pnfs_global_lock.release()
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]

    pnfs_global_lock.release()
    return search_list

#Add the database information provided to their respective caches.
def add_mtab(db_info, db_num, db_mp):
    global mount_points_cache  #dictionary
    global database_info_cache   #dictionary

    #Grab copies for this thread in case another makes changes.
    pnfs_global_lock.acquire()
    try:
        mount_points_cache_keys = mount_points_cache.keys()
        database_info_cache_keys = database_info_cache.keys()
    except:
        pnfs_global_lock.release()
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
    pnfs_global_lock.release()

    try:
        if db_mp not in mount_points_cache_keys or \
           db_info not in database_info_cache_keys:
            if db_mp.endswith("/pnfs/fs"):
                use_db_mp = db_mp + "/usr"
                parent_db_info = db_info
            elif db_mp.endswith("/pnfs/fs/"):
                use_db_mp = db_mp + "/usr/"
                parent_db_info = db_info
            else:
                use_db_mp = db_mp
                # We can't acquire the pnfs_global_lock, because this
                # get_database() calls add_mtab() that grabs the lock too.
                parent_db_info = get_database(use_db_mp)

            # Rule out that parent_db_info is None and we actually have the
            # starting point of a new database
            if (parent_db_info and parent_db_info == db_info) and \
                   use_db_mp == db_mp:  #eliminate /pnfs/fs/usr cases
                return

            #We don't need to worry about modifying the sequence
            # we are looping over, since we are looping over a copy.
            pnfs_global_lock.acquire()
            try:
                mount_points_cache[db_mp] = {DB_NUMBER : db_num,
                                             DB_INFO : db_info,
                                             DB_MOUNT_POINTS : [db_mp,],
                                             }
                if not database_info_cache.has_key(db_info):
                    database_info_cache[db_info] = {DB_NUMBER : db_num,
                                                    DB_INFO : db_info,
                                                    DB_MOUNT_POINTS : [db_mp,],
                                                    }
                else:
                    database_info_cache[db_info][DB_MOUNT_POINTS].append(db_mp)
            except:
                pnfs_global_lock.release()
                raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
            pnfs_global_lock.release()
    except:
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]

#Return the mount points as a dictionary keyed by .(get)(database) values,
# or just a single element if mount_point_key is set.
def get_cache_by_db_info(db_info_key = None, default = None):
    global mount_points_cache  #dictionary

    pnfs_global_lock.acquire()

    try:
        if db_info_key == None:
            # Return copy for thread safety.
            return_value = database_info_cache.copy()
        else:
            return_value = database_info_cache.get(db_info_key, default)
    except:
        pnfs_global_lock.release()
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]

    pnfs_global_lock.release()
    return return_value

#Return the .(get)(database) values as keyed by mount point.
def get_cache_by_mount_point(mount_point_key = None, default = None):
    global database_info_cache  #dictionary

    pnfs_global_lock.acquire()

    try:
        if mount_point_key == None:
            # Return copy for thread safety.
            return_value = mount_points_cache.copy()
        else:
            return_value = mount_points_cache.get(mount_point_key, default)
    except:
        pnfs_global_lock.release()
        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]

    pnfs_global_lock.release()
    return return_value

###############################################################################

#Return a list of admin (/pnfs/fs like) mount points.
def get_enstore_admin_mount_point(pnfsid = None):

    list_of_admin_mountpoints = []

    #Get the list of pnfs mountpoints currently mounted.
    mtab_results = process_mtab()

    for cached_item in mtab_results:
        db_num = cached_item[DB_NUMBER]
        mount_path = cached_item[DB_MOUNT_POINTS][0]  #Should only be one here.
        if db_num == -1:
            continue
        if db_num == 0:  #Admin db has number 0.
            if os.path.basename(mount_path) == "fs":
                mount_path = os.path.join(mount_path, "usr")

            if pnfsid == None:
                list_of_admin_mountpoints.append(mount_path)
            else:
                access_path = access_file(mount_path, pnfsid)
                try:
                    file_utils.get_stat(access_path, unstable_filesystem=True)
                except OSError, msg:
                    if msg.errno in [errno.ENOENT]:
                        continue
                    else:
                        list_of_admin_mountpoints.append(mount_path)


    return list_of_admin_mountpoints

#Return a list of non-admin (not /pnfs/fs like) mount points.
def get_enstore_mount_point(pnfsid = None):

    list_of_mountpoints = []

    #Get the list of pnfs mountpoints currently mounted.
    mtab_results = process_mtab()

    for cached_item in mtab_results:
        db_num = cached_item[DB_NUMBER]
        mount_path = cached_item[DB_MOUNT_POINTS][0]  #Should only be one here.
        if db_num == -1:
            continue
        if db_num != 0:  #Admin db has number 0.
            if pnfsid == None:
                list_of_mountpoints.append(mount_path)
            else:
                access_path = access_file(mount_path, pnfsid)
                try:
                    file_utils.stat(access_path, unstable_filesystem=True)
                except OSError, msg:
                    if msg.errno in [errno.ENOENT]:
                        continue
                    else:
                        list_of_mountpoints.append(mount_path)


    return list_of_mountpoints

###############################################################################

#filepath should refer to a pnfs path.
#replacement_path should be one of "/pnfs/", "/pnfs/fnal.gov" or "/pnfs/".
def __get_special_path(filepath, replacement_path):
    #Make sure this is a string.
    if type(filepath) != types.StringType:
        raise TypeError("Expected string filename.",
                        e_errors.WRONGPARAMETER)
    #Make sure this is a string.
    if type(replacement_path) != types.StringType:
        raise TypeError("Expected string replacement string.",
                        e_errors.WRONGPARAMETER)

    #Make absolute path.
    #Note: enstore_functions2.fullpath() does a stat() to determine if filepath
    # is a directory (it appends a / to filename if so).  We know we don't
    # need it here.  Just use expand_path here for performance gains.
    #unused, filename, dirname, unused = enstore_functions2.fullpath(filepath)
    filename = enstore_functions2.expand_path(filepath)

    #Determine the canonical path base.  (i.e /pnfs/fnal.gov/usr/)
    # If the ENCP_CANONICAL_DOMAINNAME overriding environmental variable
    # is set, use that.
    if os.environ.get('ENCP_CANONICAL_DOMAINNAME', None):
        canonical_name = os.environ['ENCP_CANONICAL_DOMAINNAME']
    else:
        canonical_name = string.join(socket.getfqdn().split(".")[1:], ".")
    canonical_name = string.join(socket.getfqdn().split(".")[1:], ".")
    canonical_pathbase = os.path.join("/pnfs", canonical_name, "usr") + "/"

    #Return an error if the file is not a pnfs filename.
    #if not pnfs.is_pnfs_path(dirname, check_name_only = 1):
    #    raise EncpError(None, "Not a pnfs filename.", e_errors.WRONGPARAMETER)

    #Build the list of patters to search for.  Start with the three we
    # know about...
    pattern_list = ["/pnfs/fs/usr/", canonical_pathbase, "/pnfs/"]

    ##However, we need to handle paths like matching /pnfs/fs/usr/dzero
    ## with /pnfs/sam/dzero (instead of the more obvious /pnfs/dzero).

    #First, remove any preceding directories before /pnfs/.
    dir_split = filepath.split("/")
    try:
        dir_split_index = dir_split.index("pnfs")
    except ValueError:
        #The file is not a pnfs file.
        raise OSError(errno.ENOENT, os.strerror(errno.ENOENT), filename)

    #Limit this check to just three directory levels after /pnfs/.  If it
    # hasn't been found by then, chances are it will not.  If necessary,
    # this could be increased.
    dir_split = dir_split[dir_split_index : dir_split_index + 3]

    #Next, start putting those directories into the pattern match list.
    current_dir_name = "/"
    for dir_name in dir_split:
        current_dir_name = os.path.join(current_dir_name, dir_name) + "/"
        pattern_list.append(current_dir_name)

    ## Check to make sure that the current pattern exists.  If so, return
    ## it.
    for pattern in pattern_list:
        filename, count = re.subn(pattern, replacement_path, filepath, 1)
        if count > 0 and is_pnfs_path(filename, check_name_only = 1):
            return filename

    #The file is not a pnfs file.
    raise OSError(errno.ENOENT, os.strerror(errno.ENOENT), filepath)

def get_enstore_pnfs_path(filepath):
    return __get_special_path(filepath, "/pnfs/")

#For future compatibility with other storage file systems.
get_enstore_path = get_enstore_pnfs_path

def get_enstore_fs_path(filepath):
    return __get_special_path(filepath, "/pnfs/fs/usr/")


def get_enstore_canonical_path(filepath):
    #Determine the canonical path base.  (i.e /pnfs/fnal.gov/usr/)
    # If the ENCP_CANONICAL_DOMAINNAME overriding environmental variable
    # is set, use that.
    if os.environ.get('ENCP_CANONICAL_DOMAINNAME', None):
        canonical_name = os.environ['ENCP_CANONICAL_DOMAINNAME']
    else:
        canonical_name = string.join(socket.getfqdn().split(".")[1:], ".")
    #Use the canonical_name to determine the canonical pathname base.
    canonical_pathbase = os.path.join("/pnfs", canonical_name, "usr") + "/"

    return __get_special_path(filepath, canonical_pathbase)

###############################################################################

class Pnfs:# pnfs_common.PnfsCommon, pnfs_admin.PnfsAdmin):
    # initialize - we will be needing all these things soon, get them now
    #
    #pnfsFilename: The filename of a file in pnfs.  This may also be the
    #              pnfs id of a file in pnfs.
    #mount_point: The mount point that the file should be under when
    #             pnfsFilename is really a pnfsid or pnfsFilename does
    #             not contain an absolute path.
    #shortcut: If passed a pnfsid and this is true, don't lookup the
    #          full filepath.  Use the .../.(access)(%s) name instead.
    def __init__(self, pnfsFilename="", mount_point="", shortcut=None):

        #self.print_id is unique in each of pnfs.Pnfs, chimera.ChimeraFS,
        # and pnfs_agent_client.PnfsAgentClient.  It is to be used for
        # the printing of messages to name the specific interface
        # being used by namespace.StorageFS.
        self.print_id = "PNFS"

        self.mount_point = mount_point
        #Make sure self.id exists.  __init__ should set it correctly
        # if necessary a little later on.
        self.id = None

        if mount_point:
            self.dir = mount_point
        else:
            try:
                #Handle the case where the cwd has been deleted.
                self.dir = os.getcwd()
            except OSError:
                self.dir = ""

        #Test if the filename passed in is really a pnfs id.
        if is_pnfsid(pnfsFilename):
            self.id = pnfsFilename
            try:
                if shortcut:
                    raise ValueError, "Applying filename shortcut"

                pnfsFilename_list = self.get_path(self.id)
                if len(pnfsFilename_list) == 1:
                    pnfsFilename = pnfsFilename_list[0]
                else:
                    sys.stderr.write("Found %d file matches instead of just 1.\n"
                                     % (len(pnfsFilename_list),))
                    sys.exit(1)

            except (OSError, IOError, AttributeError, ValueError):
                #No longer do just the following: pnfsFilename = ""
                # on an exception.  Attempt to get the ".(access)(<pnfs id>)"
                # version of the filename.
                #This was done in response to the pnfs database being
                # corrupted.  There was a directory that had fewer entries
                # than valid i-nodes that belonged in that directory.  With
                # this type of database corruption, the is_pnfs_path() test
                # still works correctly.
                try:
                    dir_list, target = self._get_mount_point2(self.id,
                                                              self.dir,
                                                              ".(nameof)(%s)")
                    self.dir = dir_list[0]
                    pnfsFilename = os.path.join(self.dir,
                                                ".(access)(%s)" % self.id)
                except OSError, msg:
                    #If we got the ENODEV errno, it means that the same
                    # pnfsid was found under two different pnfs mount points.
                    # For this error we keep going, but for all others
                    # re-raise the traceback.
                    if msg.args[0] not in [errno.ENODEV]:
                        raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]

                if not is_pnfs_path(pnfsFilename):
                    pnfsFilename = ""

        if pnfsFilename:
            (self.machine, self.filepath, self.dir, self.filename) = \
                           fullpath(pnfsFilename)

            if shortcut and self.id and self.filepath != self.dir and \
                   self.filepath:
                #This block of code determines if the use_dir path is a
                # directory or not.  The parent of a tag file is another
                # tag file.  So, we leave self.dir alone for these cases and
                # set it only when we really do have a directory.
                #
                #If self.filepath and self.dir are equal and not empty,
                # then we already know we have a directory and can skip
                # this possible tag ID handling.  For directories we need
                # to skip this because of the possibility of putting a
                # looping pathname together if the target is the top level
                # of a database.

                parent_id = self.get_parent(id = self.id, directory = self.dir)
                use_dir = os.path.join(self.dir,
                                       ".(access)(%s)" % parent_id)
                try:
                    f_stats = get_stat(use_dir, unstable_filesystem=True)
                    if stat.S_ISDIR(f_stats[stat.ST_MODE]):
                        #We have the pnfs id of a tag file.
                        self.dir = use_dir
                except (OSError, IOError), msg:
                    if msg.args[0] != errno.ENOTDIR:
                        #We have the pnfs id of a tag file.
                        self.dir = use_dir

            #self.pstatinfo()  #Comment out for performance.

        try:
            self.pnfsFilename = self.filepath
        except AttributeError:
            #sys.stderr.write("self.filepath DNE after initialization\n")
            pass

    ##########################################################################

    def layer_file(self, f, n):
        pn, fn = os.path.split(f)
        if is_access_name(fn):
            return os.path.join(pn, "%s(%d)" % (fn, n))
        else:
            return os.path.join(pn, ".(use)(%d)(%s)" % (n, fn))

    def id_file(self, f):
        pn, fn = os.path.split(f)
        if is_access_name(fn):
            #Just a note:  This is silly.  Finding out the pnfs id when the
            # id is already in the .(access)(<pnfsid>) name.  However,
            # we should be able to handle this, just in case.  The nameof
            # lookup is limited to just the parent directory and not the entire
            # path.

            #Since, we have the .(access)() name we need to split off the id.
            pnfsid = fn[10:-1]  #len(".(access)(") == 10 and len ")" == 1
            parent_id = self.get_parent(pnfsid, pn) #Get parent id
            nameof = self.get_nameof(pnfsid, pn) #Get nameof file

            #Create the filename to obtain the parent id.
            return os.path.join(pn, ".(access)(%s)" % parent_id,
                                ".(id)(%s)" % nameof)
        else:
            return os.path.join(pn, ".(id)(%s)" % (fn, ))

    def parent_file(self, f, pnfsid = None):
        pn, fn = os.path.split(f)
        if pnfsid:
            if file_utils.wrapper(os.path.isdir, (f,)):
                return os.path.join(f, ".(parent)(%s)" % (pnfsid))
            else:
                return os.path.join(pn, ".(parent)(%s)" % (pnfsid))
        else:
            fname = self.id_file(f)
            f = file_utils.open(fname, unstable_filesystem=True)
            try:
                pnfsid = file_utils.readline(f, unstable_filesystem=True)
            finally:
                f.close()
            return os.path.join(pn, ".(parent)(%s)" % (pnfsid))

    def access_file(self, pn, pnfsid):
        return os.path.join(pn, ".(access)(%s)" % pnfsid)

    def use_file(self, f, layer):
        pn, fn = os.path.split(f)
        if is_access_name(fn):
            #Use the .(access)() extension path for layers.
            return "%s(%s)" % (f, layer)
        else:
            return os.path.join(pn, '.(use)(%d)(%s)' % (layer, fn))

    def fset_file(self, f, size):
        pn, fn = os.path.split(f)
        if is_access_name(fn):
            pnfsid = fn[10:-1]  #len(".(access)(") == 10 and len ")" == 1

            directory = pn
            name = self.get_nameof(pnfsid, pn)
        else:
            directory = pn
            name = fn

        return os.path.join(directory, ".(fset)(%s)(size)(%s)" % (name, size))

    def nameof_file(self, pn, pnfsid):
        return os.path.join(pn, ".(nameof)(%s)" % (pnfsid,))

    def const_file(self, f):
        pn, fn = os.path.split(f)
        if is_access_name(fn):
            pnfsid = fn[10:-1]  #len(".(access)(") == 10 and len ")" == 1

            directory = pn
            name = self.get_nameof(pnfsid, pn)
        else:
            directory = pn
            name = fn

        return os.path.join(directory, ".(const)(%s)" % (name,))

    ##########################################################################

    #Convert a nameof, parent or showid filename to an access filename.
    def convert_to_access(self, pfn):
        dirname, fname = os.path.split(pfn)
        fname = fname.replace(".(nameof)", ".(access)", 1)
        fname = fname.replace(".(parent)", ".(access)", 1)
        fname = fname.replace(".(showid)", ".(access)", 1)
        return os.path.join(dirname, fname)

    #Convert a nameof, parent or showid filename to an access filename.
    def convert_to_showid(self, pfn):
        dirname, fname = os.path.split(pfn)
        fname = fname.replace(".(access)", ".(showid)", 1)
        fname = fname.replace(".(nameof)", ".(showid)", 1)
        fname = fname.replace(".(parent)", ".(showid)", 1)
        return os.path.join(dirname, fname)

    ##########################################################################

    # list what is in the current object
    def dump(self):
        #Trace.trace(14, repr(self.__dict__))
        print repr(self.__dict__)


    #This function is used to test for various conditions on the file.
    # The purpose of this function is to hide the hidden files associated
    # with each real file.
    def verify_existance(self, filepath=None):
        if filepath:
            fname = filepath
        else:
            fname = self.filepath

        #Perform only one stat() and do the checks here for performance
        # improvements over calling python library calls for each check.
        # get_stat() is not used here because that function may return
        # the status of the parent directory instead, which is not what we
        # want here.
        pstat = file_utils.get_stat(fname, unstable_filesystem=True)
        if not filepath:
            self.pstat = pstat

        #As long as the file exists root can read it.  What about writes?
        if os.geteuid() == 0:
            return

        #Using the stat, make sure that the "file" is readable.
        elif pstat[stat.ST_MODE] & stat.S_IROTH:
            return

        elif pstat[stat.ST_MODE] & stat.S_IRUSR and \
           pstat[stat.ST_UID] == os.geteuid():
            return

        elif pstat[stat.ST_MODE] & stat.S_IRGRP and \
           pstat[stat.ST_GID] == os.getegid():
            return

        else:
            raise OSError(errno.EACCES,
                          os.strerror(errno.EACCES) + ": " + fname)

        #if not os.path.exists(fname):
        #    raise OSError(errno.ENOENT,
        #                  os.strerror(errno.ENOENT) + ": " + fname)
        #
        #if not os.access(fname, os.R_OK):
        #    raise OSError(errno.EACCES,
        #                  os.strerror(errno.EACCES) + ": " + fname)

    ##########################################################################

    # create a new file or update its times
    def touch(self, filename=None):
        if not filename:
            use_filename = self.pnfsFilename
        else:
            use_filename = filename

        try:
            self.utime(use_filename)
        except os.error, msg:
            if msg.errno == errno.ENOENT:
                f = file_utils.open(use_filename,'w', unstable_filesystem=True)
                f.close()
            else:
                Trace.log(e_errors.INFO,
                          "problem with pnfsFilename = " + use_filename)
                raise os.error, msg

        if not filename:
            self.pstatinfo()

    # create a new file
    def creat(self, filename=None, mode = None):
        if filename:
            fname = filename
        else:
            fname = self.pnfsFilename

        if mode:
            fd = atomic.open(fname, os.O_RDWR | os.O_CREAT | os.O_EXCL,
                             mode=mode)
        else:
            fd = atomic.open(fname, os.O_RDWR | os.O_CREAT | os.O_EXCL)

	if not filename:
            self.pstatinfo()

        os.close(fd)

    # update the access and mod time of a file
    def utime(self, filename=None):
        if not filename:
            filename = self.pnfsFilename

        t = int(time.time())
        file_utils.utime(filename,(t,t), unstable_filesystem=True)

    # delete a pnfs file including its metadata
    def rm(self, filename=None):
        if not filename:
            filename = self.pnfsFilename

        # if the file name has access then we would
        # strip the pnfs id from the .(access)() file basename.
        # and then use get_nameof function to retrieve a file name.
        # Finally we will append the directory part with the filename
        # to generate the fully qualified path
        if is_access_name(filename):
            pnfsid = os.path.basename(filename)[10:-1]
            filename = os.path.join(filename[:filename.find('.(access)')],
                     self.get_nameof(pnfsid))

        # It would be better to move the file to some trash space.
        # I don't know how right now.
        file_utils.remove(filename, unstable_filesystem=True)
        if not filename:
           self.pstatinfo()

    ##########################################################################

    # write a new value to the specified file layer (1-7)
    # the file needs to exist before you call this
    def writelayer(self, layer, value, filepath=None):
        if filepath:
            use_filepath = filepath
        else:
            use_filepath = self.filepath
        if use_filepath:
            dir_path, dir_name = os.path.split(use_filepath)
            use_filepath = os.path.join(remove_special_paths(dir_path),
                                        dir_name)

        fname = self.use_file(use_filepath, layer)

        #If the value isn't a string, make it one.
        if type(value)!=types.StringType:
            value=str(value)

        f = file_utils.open(fname,'w', unstable_filesystem=True)
        file_utils.wrapper(f.write, (value,), unstable_filesystem=True)
        f.close()
        #self.utime()
        #self.pstatinfo()

    # read the value stored in the requested file layer
    def readlayer(self, layer, filepath=None):
        if filepath:
            use_filepath = filepath
        else:
            use_filepath = self.filepath
        if use_filepath:
            dir_path, dir_name = os.path.split(use_filepath)
            use_filepath = os.path.join(remove_special_paths(dir_path),
                                        dir_name)

        fname = self.use_file(use_filepath, layer)

        f = file_utils.open(fname,'r', unstable_filesystem=True)
        try:
            l = file_utils.readlines(f, unstable_filesystem=True)
        finally:
            f.close()

        return l

    ##########################################################################

    # get the const info of the file, given the filename
    def get_const(self, filepath=None):

        if filepath:
            use_filepath = filepath
        else:
            use_filepath = self.filepath

        fname = self.const_file(use_filepath)

        f=file_utils.open(fname, 'r', unstable_filesystem=True)
        try:
            const = file_utils.readlines(f, unstable_filesystem=True)
        finally:
            f.close()

        if not filepath:
            self.const = const
        return const

    # get the numeric pnfs id, given the filename
    def get_id(self, filepath=None):

        if filepath:
            (directory, name) = os.path.split(filepath)
        else:
            (directory, name) = os.path.split(self.filepath)

        if is_access_name(name) or \
               is_nameof_name(name) or \
               is_parent_name(name):
            # Conveniently, access, nameof and parent are all the same length.
            pnfs_id = name[10:-1]  #len(".(access)(") == 10 and len ")" == 1
        else:
            fname = os.path.join(directory, ".(id)(%s)" % (name,))

            f = file_utils.open(fname, 'r', unstable_filesystem=True)
            try:
                pnfs_id = file_utils.readline(f, unstable_filesystem=True).strip()
            finally:
                f.close()

#            pnfs_id = string.replace(pnfs_id[0], '\n', '')

        if not filepath:
            self.id = pnfs_id
        return pnfs_id

    ##########################################################################

    # Get the .(showid)() information.
    def get_showid(self, id=None, directory=""):
        if directory:
            use_dir = directory
        else:
            use_dir = self.dir
        #Remove all trailing .(access)(), .(nameof)(), .(parent)(), etc. path
        # components.
        use_dir = remove_special_paths(use_dir)

        if id:
            use_id = id
        else:
            use_id = self.id

        search_path, showid = self._get_mount_point2(use_id, use_dir,
                                                     ".(showid)(%s)")
        showid = showid[0]
        if not id:
            self.showid = showid
        return showid

    #A smaller faster version of get_nameof().
    def _get_nameof(self, id, directory):
        fname = self.nameof_file(directory, id)

        f = file_utils.open(fname, 'r', unstable_filesystem=True)
        try:
            nameof = file_utils.readline(f, unstable_filesystem=True)
        finally:
            f.close()

        return nameof.replace("\n", "")

    # get the nameof information, given the id
    def get_nameof(self, id=None, directory=""):
        if directory:
            use_dir = directory
        else:
            use_dir = self.dir
        #Remove all trailing .(access)(), .(nameof)(), .(parent)(), etc. path
        # components.
        use_dir = remove_special_paths(use_dir)

        if id:
            use_id = id
        else:
            use_id = self.id

        search_path, target = self._get_mount_point2(use_id, use_dir,
                                                     ".(nameof)(%s)")

        nameof = target[0].replace("\n", "")

        if not id:
            self.nameof = nameof
        return nameof

    #A smaller faster version of get_parent().
    def _get_parent(self, id, directory):
        fname = self.parent_file(directory, id)

        f = file_utils.open(fname, 'r', unstable_filesystem=True)
        try:
            parent = file_utils.readline(f, unstable_filesystem=True)
        finally:
            f.close()

        return parent.replace("\n", "")

    # get the parent information, given the id
    def get_parent(self, id=None, directory=""):
        if directory:
            use_dir = directory
        else:
            use_dir = self.dir
        #Remove all trailing .(access)(), .(nameof)(), .(parent)(), etc. path
        # components.
        use_dir = remove_special_paths(use_dir)

        if id:
            use_id = id
        else:
            use_id = self.id

        search_path, target = self._get_mount_point2(use_id, use_dir,
                                                     ".(parent)(%s)")

        parent = target[0].replace("\n", "")

        if not id:
            self.parent = parent
        return parent

    # get the total path of the id
    def get_path(self, id=None, directory="", shortcut=None):
        if directory:
            use_dir = fullpath(directory)[1]
        else:
            use_dir = self.dir
        #Remove all trailing .(access)(), .(nameof)(), .(parent)(), etc. path
        # components.
        use_dir = remove_special_paths(use_dir)

        if id != None:
            if is_pnfsid(id):
                use_id = id
            else:
                raise ValueError("The pnfs id (%s) is not valid." % id)
        elif self.id != None:
            if is_pnfsid(self.id):
                use_id = self.id
            else:
                raise ValueError("The pnfs id (%s) is not valid." % self.id)
        else:
            raise ValueError("No valid pnfs id.")

        try:
            search_paths, targets = self._get_mount_point2(use_id, use_dir,
                                                           ".(nameof)(%s)",
                                                           return_all = True)
        except OSError, msg:
            if msg.args[0] in [errno.ENODEV]:
                if msg.filename:
                    search_paths = msg.filename
                elif len(msg.args) >= 3 and msg.args[2]:
                    search_paths = msg.args[2]
                else:
                    search_paths = []
                targets = msg.args[3]
            else:
                raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]

        #TO DO;  fix this comment!!!
        #At this point:
        # 1) search_paths is a list of strings representing directory paths
        #    for mount points.
        # 2) targets represents a coresponding return result.  Both of the
        #    following types described are returned as lists with a matching
        #    index with search_paths.
        #    a) For regular files and directories the return result is
        #       the .(nameof)() response for the PNFS ID.
        #    b) For tag files and layer files - which do have their own
        #       PNFS IDs - these return values are the .(showid)() contents.
        #
        #       The first nine lines contain the same format for .(showid)()
        #       output for all types of PNFS IDs.  The 9th line contains
        #       the type of file record.  For thses special cases:
        #          tag files = "Tag ( Inode )"
        #          layer file = "Regular ( Data )"
        #       and just for completeness
        #          regular files = "Regular ( Inode )"
        #          directories = "Directory ( Inode )"

        rtn_filepaths = []
        old_base_id = None
        for i in range(len(search_paths)):
            if type(targets[i]) == types.ListType and \
               len(targets[i]) >= 9 and \
               type(targets[i][8]) == types.StringType and \
               (targets[i][8].find("Tag ( Inode )") != -1 \
                or targets[i][8].find("Regular ( Data )") != -1):
                #
                #This is a TAG or LAYER pnfsid.  Handle it.
                #
                base_id = targets[i][4][16:-1].strip()

                if base_id == old_base_id:
                    #Do not waste time in get_path() for a directory
                    # we have already checked.
                    continue

                #The __get_path() function, by default returns the directory.
                #
                # For tags, we want the directory.
                #
                # For layers, if we wanted the filename included in the
                # returned value, we would need to pass the basename instead
                # of the empty string for the third arguemnt.  However,
                # the get_showid() function wants the directory, so we
                # mimic getting the directory here, just like for tags, and
                # handle inserting the basename part of the filename below.
                base_path = self.__get_path(base_id, search_paths[i],
                                            "", shortcut)
                if base_path[0] != "/":
                    #This PNFS server knows about the PNFS ID, but it does
                    # not belong to a database for the current mount point.
                    continue

                #make the special path.
                if targets[i][8].find("Tag ( Inode )") != -1:
                    tag_name =  targets[i][9][12:-1].strip()
                    special_name = os.path.join(base_path,
                                                ".(tag)(%s)" % (tag_name,))
                elif targets[i][8].find("Regular ( Data )") != -1:
                    #Need the showid information for the file to determine
                    # which layer this is.
                    nameof = self.get_nameof(base_id, base_path)
                    base_showid = self.get_showid(base_id, base_path)
                    for line in base_showid:
                        if line.find(use_id) != -1:
                            layer_number = int(line[7])
                            break
                    else:
                        #Dummy value.  This should never happen.
                        layer_number = -1

                    special_name = os.path.join(base_path, nameof)
                    special_name = self.use_file(special_name, layer_number)
                #Append the special name to the directory.
                if special_name not in rtn_filepaths:
                    #Keep this list unique to avoid false to many mount
                    # point errors.
                    rtn_filepaths.append(special_name)

                #Remember this id to use in the repeated directory check.
                old_base_id = base_id
            elif type(targets[i]) == types.TupleType and \
                 len(targets[i]) == 3:
                #
                #This is a TAG or LAYER pnfsid.  Handle it.
                #

                #The __get_path() function, by default returns the directory.
                #
                # For tags, we want the directory.
                #
                # For layers, if we wanted the filename included in the
                # returned value, we would need to pass the basename instead
                # of the empty string for the third arguemnt.  However,
                # the get_showid() function wants the directory, so we
                # mimic getting the directory here, just like for tags, and
                # handle inserting the basename part of the filename below.
                base_id = targets[i][1]
                base_path = self.__get_path(base_id, search_paths[i],
                                            "", shortcut)

                if targets[i][0] == TAG:
                    tag_name = targets[i][2]
                    special_name = os.path.join(base_path,
                                                ".(tag)(%s)" % (tag_name,))
                elif targets[i][0] == LAYER:
                    layer_number = targets[i][2]
                    nameof = self.get_nameof(base_id, base_path)

                    special_name = os.path.join(base_path, nameof)
                    special_name = self.use_file(special_name, layer_number)
                else:
                    raise TypeError("Unexpected PNFS special file type: %s" %
                                    (targets[i][0],))

                #Append the special name to the directory.
                if special_name not in rtn_filepaths:
                    #Keep this list unique to avoid false to many mount
                    # point errors.
                    rtn_filepaths.append(special_name)

                #Remember this id to use in the repeated directory check.
                old_base_id = base_id
            else:
                #
                #This is a REGULAR FILE or DIRECTORY pnfsid.
                #

                rtn_path = self.__get_path(use_id, search_paths[i],
                                           "", shortcut)
                if rtn_path not in rtn_filepaths:
                    #Keep this list unique to avoid false to many mount
                    # point errors.
                    rtn_filepaths.append(rtn_path)

        if len(rtn_filepaths) == 1:
            return rtn_filepaths
        else:
            raise OSError(errno.ENODEV,
                          "%s: %s" % (os.strerror(errno.ENODEV),
                                      "Too many matching mount points",),
                          rtn_filepaths)

    def __get_path(self, pnfs_id, search_path, target, shortcut):
        filepath = target.replace("\n", "")

        #At this point 'filepath' contains just the basename of the file
        # with the "use_id" pnfs id.

        #If the user doesn't want the pain of going through a full name
        # lookup, return this alternate name.
        if shortcut:
            return os.path.join(search_path, ".(access)(%s)" % pnfs_id)

        pnfs_id_db_number = pnfs_id[:4]

        #A side effect of is_tag_path() is to return the base_id
        # and tag name for true.  This is for performance
        # optimazations.
        test_for_tag_path = os.path.join(search_path,
                                         ".(access)(%s)" % (pnfs_id,))
        return_value = is_tag_path(test_for_tag_path)
        if return_value:
            #We have a tag.
            (path_type, base_id, tag_name) = return_value
        else:
            #Not a tag.
            (path_type, base_id, tag_name) = (None, pnfs_id, None)

        #If the mountpoint doesn't know about our database fail now.
        use_search_path = search_path

        n = N(pnfs_id_db_number, search_path)
        current_db_info = n.get_databaseN(pnfs_id_db_number)

        cached_info = get_cache_by_db_info(current_db_info)
        if cached_info:
            #If we get here, then we already had cached the information
            # about this PNFS database.

            db_num = cached_info[DB_NUMBER]
            mount_point = cached_info[DB_MOUNT_POINTS][0] #Is this okay?
            #TO DO: correct comment
            #Remove the last directory to avoid listing it twice when this
            # and filepath are joined.
            found_search_path = mount_point

            if not target:
                filepath = self._get_nameof(base_id, found_search_path)

            name = ""  # compoent name of a directory.
            use_id = base_id
            while name != "root" or use_id != "000000000000000000001020":
                use_id = self._get_parent(use_id, found_search_path)
                name = self._get_nameof(use_id, found_search_path)
                filepath = os.path.join(name, filepath)

            if filepath.startswith("root/fs/usr/"):
                filepath = filepath[5:]

        else:
            #If we get here, we don't know about this PNFS database yet.

            found_search_path = None
            found_mount_point_id = None
            use_id = base_id
            while use_id != "000000000000000000001020":
                #
                # database number is hex based, need to use hex base to int call
                #
                use_id_db_num = int(use_id[:4],16)

                try:
                    n = N(use_id_db_num, use_search_path)
                    db_info = n.get_databaseN(use_id_db_num)
                except (OSError, IOError):
                    continue

                cached_info = get_cache_by_db_info(db_info)
                if cached_info:
                    found_search_path = cached_info[DB_MOUNT_POINTS][0] #???
                    found_mount_point_id = use_id
                    #TO DO: Need to update the cache with this mount point!
                    break

                use_id = self._get_parent(use_id, use_search_path)
            else:
                #This mount point has a matching db number, but it does
                # not match the DB info.
                message = "matching %s to mount point %s failed" % \
                          (pnfs_id, search_path)
                raise OSError(errno.ENOENT, message)

            #Loop through the pnfs ids to find each ids parent until the "root"
            # id is found.  The comparison for the use_id is to prevent some
            # random directory named 'root' in the users path from being
            # selected as the real "root" directory.  Of course this only
            # works if the while uses an 'or' and not an 'and'.  Silly
            # programmer error...  Grrrrrrr.
            if not target:
                filepath = self._get_nameof(base_id, found_search_path)
            use_id = base_id

            while use_id != found_mount_point_id:
                use_id = self._get_parent(use_id, found_search_path)
                name = self._get_nameof(use_id, found_search_path)
                filepath = os.path.join(name, filepath)

        # Munge the starting point of the PNFS database with the rest of
        # the path.
        if filepath:
            pieces=filepath.strip("/").split("/")
            ip=0
            for i,piece in enumerate(pieces):
                if not file_utils.e_access(os.path.join(found_search_path,piece),os.F_OK) :
                    continue
                else:
                    ip=i
                    break
            filepath = os.path.join(found_search_path,string.join(pieces[ip:],"/"))
        else:
            filepath = found_search_path

        if base_id != pnfs_id:
            #We have a tag file!  We just searched for the directory, now
            # we need to add the name.
            filepath = os.path.join(filepath, ".(tag)(%s)" % (tag_name,))

        return filepath

    ##########################################################################

    #Return just the mount point section of a pnfs path.  None if one was
    # not found.
    def get_mount_point(self, filepath = None):
        if filepath:
            fname = filepath
        else:
            fname = self.filepath

        #Loop over the mount points, looking for a mount point match.
        mp_dict, dbinfo_dict = parse_mtab()
        for mount_point in mp_dict.keys():
            if fname.startswith(mount_point):
                return mount_point
        return None

    #Search up the tree, looking for a directory with a different DB number.
    # Then return the path that still had the same PNFS ID DB number.
    def get_pnfs_db_directory(self, filepath = None):
        if filepath:
            fname = filepath
        else:
            fname = self.filepath

        return_value = is_tag_path(fname)
        if return_value:
            (unused, base_id, unused) = return_value
        else:
            base_id = self.get_id(fname)
        use_search_path = fname

        found_search_path = None
        for use_search_path in (fname, os.path.dirname(fname)):
            current_id = base_id
            while current_id != "000000000000000000000000":
                current_id_db_num = int(current_id[:4], 16)

                try:
                    n = N(current_id_db_num, use_search_path)
                    db_info = n.get_databaseN(current_id_db_num)
                except (OSError, IOError), msg:
                    if msg.errno == errno.ENOTDIR:
                        #Back to top of for loop.
                        break
                    else:
                        #give up.
                        raise sys.exc_info()[0], sys.exc_info()[1], \
                              sys.exc_info()[2]

                cached_info = get_cache_by_db_info(db_info)
                if cached_info:
                    found_search_path = cached_info[DB_MOUNT_POINTS][0]
                    #TO DO: Need to update the cache with this mount point!
                    break

                current_id = self._get_parent(current_id, use_search_path)
            if found_search_path:
                #Don't raise the exception in the for loops else clause,
                # if we found what we are looking for.
                break
        else:
            message = "pnfs db directory not found for %s" % (base_id,)
            raise OSError(errno.ENOENT, message)

        return found_search_path

    #Get the mountpoint for the pnfs id.
    # As a side effect also get the value of the pnfsname file.
    #
    # 'id' is the PNFS ID.
    # 'directory' is a default directory to try.
    # 'pnfsname' if specified, is a string consisting of a special PNFS file
    #            with %s as a place holder for the ID to be inserted.
    #            ".(showid)(%s)" would be an example.
    #            The default is ".(access)(%s)".
    # 'return_all' is boolean flag to alter the behavior if multiple
    #              PNFS servers are found to have the same ID.
    #              If false, the default, raise an OSError(errno.ENODEV)
    #              exception.  If true, return all matches.
    #
    #The return value is a two-tuple of lists (even if return_all is false).
    #  The first list is the list of mount points.  The second list is
    #  value of the pnfsname used.  The indexes for these lists are
    #  corresponding.
    def _get_mount_point2(self, id, directory, pnfsname=None,
                          return_all = False):

        if id != None:
            if not is_pnfsid(id):
                raise ValueError("The pnfs id (%s) is not valid." % id)
        else:
            raise ValueError("No valid pnfs id.")

        #Try and optimize things by looking for the target to begin with.
        if type(pnfsname) == types.StringType:
            use_pnfsname = pnfsname % id
        else:
            use_pnfsname = ".(access)(%s)" % id

        #Strip this down to just the mount point.
        use_directory = self.get_mount_point(directory)

        #We will need the pnfs database numbers.
        use_pnfsid_db = int(id[:4], 16)

        #Insert the default path first to try.  If that one has a hit, just
        # use it.  If not search all available mount points, hoping to find
        # just one that matches.
        search_lists = [{DB_INFO : None, DB_NUMBER : None,
                         DB_MOUNT_POINTS : [use_directory,]}], process_mtab()
        for search_list in search_lists:
            #Search all of the pnfs mountpoints that are mounted.
            mount_point_match_list = []
            pnfs_value_match_list = []
            for search_item in search_list:
                db_info = search_item[DB_INFO]
                db_num = search_item[DB_NUMBER]
                mount_point = search_item[DB_MOUNT_POINTS][0]

                if not mount_point:
                   #If the directory argument was not in PNFS,
                   # use_directory gets set to None, and we get here.
                   continue

                #If the mountpoint doesn't know about our database fail now.
                try:
                    n = N(db_num, mount_point)
                    current_db_info = n.get_databaseN(use_pnfsid_db)
                except (OSError, IOError):
                    continue

                if os.path.basename(mount_point) == "fs" and db_num == 0:
                    #For /pnfs/fs, insert /usr/.
                    pfn = os.path.join(mount_point, "usr", use_pnfsname)
                else:
                    pfn = os.path.join(mount_point, use_pnfsname)

                try:
                    afn = self.convert_to_access(pfn)
                    use_mount_point = self.get_pnfs_db_directory(afn)
                except (OSError, IOError):
                    continue

                if os.path.basename(use_mount_point) == "fs" and db_num == 0:
                    #For /pnfs/fs, insert /usr/.
                    pfn = os.path.join(use_mount_point, "usr", use_pnfsname)
                else:
                    pfn = os.path.join(use_mount_point, use_pnfsname)

                #
                # Get the requested information from PNFS.
                #
                pnfs_value = None
                try:
                    f = file_utils.open(pfn, 'r', unstable_filesystem=True)
                    try:
                        if pfn.find("showid") != -1:
                            pnfs_value = file_utils.readlines(
                                f, unstable_filesystem=True)
                        elif pfn.find("nameof") != -1:
                            # We don't want to do the extra work for PNFS...
                            pnfs_value = file_utils.readline(
                                f, unstable_filesystem=True)
                        else:
                            pnfs_value = file_utils.readline(
                                f, unstable_filesystem=False)

                    finally:
                        f.close()
                except (KeyboardInterrupt, SystemExit):
                    raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
                except (OSError, IOError), msg:
                    if msg.errno == errno.ENOTDIR:
                        return_result = is_special_path(afn)
                        if return_result:
                            pnfs_value = return_result
                            #type is either set to the constants TAG or LAYER.
                            #pnfs_value is the tag name or layer number.
                    else:
                        continue

                if current_db_info == db_info and pnfs_value:
                    if mount_point not in mount_point_match_list:
                        mount_point_match_list.append(mount_point)
                        pnfs_value_match_list.append(pnfs_value)
                elif current_db_info != db_info and pnfs_value:
                    if use_mount_point not in mount_point_match_list:
                        mount_point_match_list.append(use_mount_point)
                        pnfs_value_match_list.append(pnfs_value)
                    ###TO DO: Need to update the mount point cache.

            #These lists should always have the same length, but just in case
            # we handle it.
            count = max(len(mount_point_match_list),
                        len(pnfs_value_match_list))
            if count == 1:
                #Return the item from this search list set.
                return mount_point_match_list, pnfs_value_match_list
            elif count > 1 and not return_all:
                raise OSError(errno.ENODEV,
                              "%s: %s" % (os.strerror(errno.ENODEV),
                                          "Too many matching mount points",),
                              mount_point_match_list)
            elif count > 1 and return_all:
                #Return all items from this search list set.
                return mount_point_match_list, pnfs_value_match_list

        else:
            #Found no matches.
            raise OSError(errno.ENOENT,
                          "%s: %s" % (os.strerror(errno.ENOENT),
                                      "PNFS ID not found: %s" % (id,)))

    ##########################################################################

    # get the cursor information
    def get_cursor(self, directory=None):

        if directory:
            fname = os.path.join(directory, ".(get)(cursor)")
        else:
            fname = os.path.join(self.dir, ".(get)(cursor)")

        f = file_utils.open(fname, 'r', unstable_filesystem=True)
        try:
            cursor = file_utils.readlines(f, unstable_filesystem=True)
        finally:
            f.close()

        if not directory:
            self.cursor = cursor
        return cursor

    # get the cursor information
    def get_counters(self, directory=None):

        if directory:
            fname = os.path.join(directory, ".(get)(counters)")
        else:
            fname = os.path.join(self.dir, ".(get)(counters)")

        f=file_utils.open(fname, 'r', unstable_filesystem=True)
        try:
            counters = file_utils.readlines(f, unstable_filesystem=True)
        finally:
            f.close()

        if not directory:
            self.counters = counters
        return counters

    # get the position information
    def get_position(self, directory=None):

        if directory:
            fname = os.path.join(directory, ".(get)(postion)")
        else:
            fname = os.path.join(self.dir, ".(get)(postion)")

        f=file_utils.open(fname, 'r', unstable_filesystem=True)
        try:
            position = file_utils.readlines(f, unstable_filesystem=True)
        finally:
            f.close()

        if not directory:
            self.position = position
        return position

    # get the database information
    # Sample answer:  admin:0:r:enabled:/diskb/pnfs/db/admin
    def get_database(self, directory=None):
        global database_info_cache

        if directory:
            use_directory = directory
        else:
            use_directory = self.dir

        fname = os.path.join(use_directory, ".(get)(database)")

        if fname in database_info_cache.keys():
            database = database_info_cache[fname]
        else:
            f = file_utils.open(fname, 'r', unstable_filesystem=False)
            try:
                database = file_utils.readline(f, unstable_filesystem=False)
            finally:
                f.close()

            database = string.replace(database, "\n", "")

            #Update the database cache.
            if not is_access_path(directory) and \
                   directory not in database_info_cache.keys():
                #database_info_cache[directory] = database
                db_num = int(database.split(":")[1])
                add_mtab(database, db_num, directory)

        if not directory:
            self.database = database
        return database

    ##########################################################################

    def get_file_size(self, filepath=None):

        if filepath:
            fname = filepath
            #Get the file system size.
            os_filesize = long(file_utils.get_stat(
                fname, unstable_filesystem=True)[stat.ST_SIZE])
        else:
            fname = self.filepath
            self.verify_existance()
            self.pstatinfo(update=0) #verify_existance does the os.stat().
            #Get the file system size.
            os_filesize = long(self.file_size)

        #If there is no layer 4, make sure an error occurs.
        try:
            pnfs_filesize = long(self.get_xreference(fname)[2].strip())
        except ValueError:
            pnfs_filesize = long(-1)
            #self.file_size = os_filesize
            #return os_filesize

        #Error checking.  However first ignore large file cases.
        if os_filesize == 1 and pnfs_filesize > long(2L**31L) - 1:
            if not filepath:
                self.file_size = pnfs_filesize
            return long(pnfs_filesize)
        #Make sure they are the same.
        elif os_filesize != pnfs_filesize:
            raise OSError(errno.EBADFD,
                     "%s: filesize corruption: OS size %s != PNFS size %s" % \
                      (os.strerror(errno.EBADFD), os_filesize, pnfs_filesize))

        if not filepath:
            self.file_size = os_filesize
        return long(os_filesize)


    def set_file_size(self, filesize, filepath=None):
        #handle large files.
        if filesize > (2**31L) - 1:
            size = 1
        else:
            size = filesize

        #xref = self.get_xreference()
        #formated_size = str(filesize)
        #if formated_size[-1] == "L":
        #    formated_size = formated_size[:-1]
        #xref[2] = formated_size  #get_xreferece() always returns a 10-tuple.
        #apply(self.set_xreference, xref) #Don't untuple xref.

        #Set the filesize that the filesystem knows about.
        if filepath:
            use_filepath = filepath
        else:
            use_filepath = self.filepath

        #Don't report the hidden file to the user if there is a problem,
        # report the original file.
        self.verify_existance(use_filepath)
        fname = self.fset_file(use_filepath, size)
        try:
            f = file_utils.open(fname, 'w', unstable_filesystem=True)
            f.close()
        except (OSError, IOError), msg:
            if msg.args[0] == errno.ENAMETOOLONG:
                #If the .(fset) filename is too long for PNFS, then we need
                # to make a shorter temproary link to it and try it again.

                #The fset_file() function gives us the correct directory
                # to use, regardles of a normal filename or .(access)()
                # filename.
                try_dir = os.path.dirname(fname)
                #Determine the original path of the file with the new
                # directory.
                try_path = os.path.join(try_dir,
                                        os.path.basename(use_filepath))

                #Next create the temporary short name.
                short_tmp_name = ".%s_%s" % (os.uname()[1], os.getpid())
                link_name = os.path.join(try_dir, short_tmp_name)

                #Get the existing link count.
                link_count = file_utils.get_stat(
                    try_path, unstable_filesystem=True)[stat.ST_NLINK]

                #Make the temporary link using the sorter name.
                try:
                    os.link(try_path, link_name)
                except (OSError, IOError), msg:
                    if msg.args[0] == errno.EEXIST \
                       and file_utils.get_stat(
                        link_name, unstable_filesystem=True)[stat.ST_NLINK] == link_count + 1:
                        # If the link count increased by one, we succeded
                        # even though there was an EEXIST error.  This
                        # situation can occur over NFS V2.
                        pass
                    else:
                        raise sys.exc_info()[0], sys.exc_info()[1], \
                              sys.exc_info()[2]

                #Set the new file size.
                try:
                    fname = self.fset_file(link_name, size)
                    f = file_utils.open(fname, "w", unstable_filesystem=True)
                    f.close()
                except (OSError, IOError), msg:
                    os.unlink(link_name)
                    raise sys.exc_info()[0], sys.exc_info()[1], \
                          sys.exc_info()[2]

                #Cleanup the temporary link.
                os.unlink(link_name)

            else:
                raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]


        #Update the times.
        if filepath:
            self.utime(filepath)
        else:
            self.utime()
            self.pstatinfo()

    ##########################################################################

    # set a new mode for the existing file
    def chmod(self, mode, filepath=None):
        if filepath:
            use_filepath = filepath
        else:
            use_filepath = self.pnfsFilename

        file_utils.chmod(use_filepath, mode, unstable_filesystem=True)

        if filepath:
            self.utime(filepath)
        else:
            self.utime()
            self.pstatinfo()

    # change the ownership of the existing file
    def chown(self, uid, gid, filepath=None):
        if filepath:
            use_filepath = filepath
        else:
            use_filepath = self.pnfsFilename

        file_utils.chown(use_filepath, uid, gid, unstable_filesystem=True)

        if filepath:
            self.utime(filepath)
        else:
            self.utime()
            self.pstatinfo()

    ##########################################################################

    # store a new bit file id
    def set_bit_file_id(self,value,filepath=None):
        if filepath:
            self.writelayer(enstore_constants.BFID_LAYER, value, filepath)
            self.get_bit_file_id(filepath)
        else:
            self.writelayer(enstore_constants.BFID_LAYER, value)
            self.get_bit_file_id()

        return value

    # store the cross-referencing data
    def set_xreference(self, volume, location_cookie, size, file_family,
                       pnfsFilename, volume_filepath, id, volume_fileP,
                       bit_file_id, drive, crc, filepath=None):

        value = (11*"%s\n")%(volume,
                             location_cookie,
                             size,
                             file_family,
                             pnfsFilename,
                             volume_filepath,
                             id,
                             volume_fileP,  #.id,
                             bit_file_id,
                             drive,
                             crc)

        Trace.trace(11,'value='+value)
        if filepath:
            self.writelayer(enstore_constants.XREF_LAYER, value, filepath)
            self.get_xreference(filepath)
        else:
            self.writelayer(enstore_constants.XREF_LAYER, value)
            self.get_xreference()

        return value

    # get the bit file id
    def get_bit_file_id(self, filepath=None):

        if filepath:
            use_filepath = filepath
        else:
            use_filepath = self.filepath

        try:
            bit_file_id = self.readlayer(enstore_constants.BFID_LAYER,
                                         use_filepath)[0]
        except IndexError:
            raise IOError(errno.EIO, "%s: Layer %d is empty: %s" %
                          (os.strerror(errno.EIO),
                           enstore_constants.BFID_LAYER,
                           use_filepath))
        except (OSError, IOError), msg:
            if msg.args[0] in (errno.ENOENT,):
                #We only need to re-create the ENOENT error.  If reading
                # layer 1 gives ENOENT, then the entire file is gone
                # (which is what we want to report).  However, most
                # (all?) other errors will apply to the layer 1 file
                # and should be reported as such.
                exception = sys.exc_info()[0]
                raise exception(msg.args[0], "%s: %s" % \
                                (os.strerror(msg.args[0]), use_filepath))
            else:
                #Just pass allong all other exceptions.
                raise sys.exc_info()[0], sys.exc_info()[1], \
                      sys.exc_info()[2]

        if not filepath:
            self.bit_file_id = bit_file_id

        return bit_file_id

    # get the cross reference layer
    def get_xreference(self, filepath=None):

        if filepath:
            use_filepath = filepath
        else:
            use_filepath = self.filepath

        #Get the xref layer information.
        xinfo = self.readlayer(enstore_constants.XREF_LAYER, use_filepath)
        if len(xinfo) == 0:
            raise IOError(errno.EIO, "%s: Layer %d is empty: %s" %
                          (os.strerror(errno.EIO),
                           enstore_constants.XREF_LAYER,
                           use_filepath))

        #Strip off whitespace from each line.
        xinfo = map(string.strip, xinfo[:11])
        #Make sure there are 11 elements.  Early versions only contain 9.
        # Some contain 10.  This prevents problems.
        xinfo = xinfo + ([UNKNOWN] * (11 - len(xinfo)))

        #If the class member value was used, store the values seperatly.
        if not filepath:
            try:
                self.volume = xinfo[0]
                self.location_cookie = xinfo[1]
                self.size = xinfo[2]
                self.origff = xinfo[3]
                self.origname = xinfo[4]
                self.mapfile = xinfo[5]      #Obsolete.
                self.pnfsid_file = xinfo[6]  #Need to give generic name.
                self.pnfsid_map = xinfo[7]   #Obsolete.
                self.bfid = xinfo[8]
                self.origdrive = xinfo[9]
                self.crc = xinfo[10]
            except ValueError:
                pass

            self.xref = xinfo

        return xinfo

    ##########################################################################

    # get the stat of file/directory
    def get_stat(self, filepath=None):

        #Get the xref layer information.
        if filepath:
            fname = filepath
        else:
            fname = self.filepath

        # first the file itself
        pstat = file_utils.get_stat(fname, unstable_filesystem=True)

        pstat = tuple(pstat)

        if not filepath:
            self.pstat = pstat

        return pstat

    # get the stat of file/directory, or if non-existant, its directory
    def get_pnfsstat(self, filepath=None):

        #Get the xref layer information.
        if filepath:
            fname = filepath
        else:
            fname = self.filepath

        try:
            # first the file itself
            pstat = file_utils.get_stat(fname)
        except (OSError, IOError):
            # if that fails, try the directory
            pstat = file_utils.get_stat(get_directory_name(fname))

        pstat = tuple(pstat)

        if not filepath:
            self.pstat = pstat

        return pstat

    # get the uid from the stat member
    def pstat_decode(self):
	self.uid = ERROR
        self.uname = UNKNOWN
        self.gid = ERROR
        self.gname = UNKNOWN
        self.mode = 0
        self.mode_octal = 0
        self.file_size = ERROR
        self.inode = 0
        #What these do, I do not know.  MWZ
        self.rmajor, self.rminor = (0, 0)
        self.major, self.minor = (0, 0)

        #In case the stat hasn't been done already, do it now.
        if not hasattr(self, "pstat"):
            self.get_stat()

        #Get the user id of the file's owner.
        try:
            self.uid = self.pstat[stat.ST_UID]
        except (KeyboardInterrupt, SystemExit):
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except:
            pass

        #Get the user name of the file's owner.
        try:
            self.uname = pwd.getpwuid(self.uid)[0]
        except (KeyboardInterrupt, SystemExit):
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except:
            pass

        #Get the group id of the file's owner.
        try:
            self.gid = self.pstat[stat.ST_GID]
        except (KeyboardInterrupt, SystemExit):
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except:
            pass

        #Get the group name of the file's owner.
        try:
            self.gname = grp.getgrgid(self.gid)[0]
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
            self.mode = (self.pstat[stat.ST_MODE] % 0777) | 0100000
            self.mode_octal = str(oct(self.mode))
        except (KeyboardInterrupt, SystemExit):
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except:
            self.mode = 0
            self.mode_octal = 0

        #if os.path.exists(self.filepath):
        if stat.S_ISREG(self.pstat[stat.ST_MODE]):
            real_file = 1
        else:
            real_file = 0  #Should be the parent directory.

        #Get the file size.
        try:
            if real_file:    #os.path.exists(self.filepath):
                self.file_size = long(self.pstat[stat.ST_SIZE])
                if self.file_size == 1L:
                    self.file_size = long(self.get_xreference()[2]) #[2] = size
            else:
                try:
                    del self.file_size
                except AttributeError:
                    pass  #Was not present.
        except (KeyboardInterrupt, SystemExit):
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except:
            pass

        #Get the file inode.
        try:
            if real_file:   #os.path.exists(self.filepath):
                self.inode = self.pstat[stat.ST_INO]
            else:
                try:
                    del self.inode
                except AttributeError:
                    pass #Was not present.
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
            # This is largly due to pnfs responce delays.
            self.major = int(((self.pstat[stat.ST_DEV]) >> 8) & 0xff)
            self.minor = int((self.pstat[stat.ST_DEV]) & 0xff)
        except (KeyboardInterrupt, SystemExit):
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except:
            pass

    # update all the stat info on the file, or if non-existent, its directory
    def pstatinfo(self, update=1):
        #Get new stat() information if requested.
        if update:
            self.get_pnfsstat()

        #Set various class values.
        self.pstat_decode()

##############################################################################

    #Prints out the specified layer of the specified file.
    def player(self, intf):
        try:
            self.verify_existance()
            data = self.readlayer(intf.named_layer)
            for datum in data:
                print datum.strip()
            return 0
        except (OSError, IOError), detail:
            print str(detail)
            return 1

    #For legacy purposes.
    pcat = player

    #Snag the cross reference of the file inside self.file.
    #***LAYER 4**
    def pxref(self):  #, intf):
        names = ["volume", "location_cookie", "size", "file_family",
                 "original_name", "map_file", "pnfsid_file", "pnfsid_map",
                 "bfid", "origdrive", "crc"]
        try:
            self.verify_existance()
            data = self.get_xreference()
            #With the data stored in lists, with corresponding values
            # based on the index, then just print them out.
            for i in range(len(names)):
                print "%s: %s" % (names[i], data[i])
            return 0
        except (OSError, IOError), detail:
            print str(detail)
            return 1

    #For legacy purposes.
    pinfo = pxref

    #Prints out the bfid value for the specified file.
    #***LAYER 1***
    def pbfid(self):  #, intf):
        try:
            self.verify_existance()
            self.get_bit_file_id()
            print self.bit_file_id
            return 0
        except IndexError:
            print UNKNOWN
            return 1
        except (IOError, OSError), detail:
            print str(detail)
            return 1

    #Print out the filesize of the file from this layer.  It should only
    # be here as long as pnfs does not support NFS ver 3 and the filesize
    # is longer than 2GB.
    #***LAYER 4***
    def pfilesize(self):  #, intf):
        try:
            self.get_file_size()
            print self.file_size
            return 0
        except (OSError, IOError), detail:
            """
            try:
                # Get layer 2 when layer 4 is not available.
                data = self.readlayer(2)
                # Define the match/search once before the loop.
                size_match = re.compile("l=[0-9]+")
                #Loop over the data in layer 2 looking for the length value.
                for line in data:
                    result = size_match.search(line)
                    if result != None:

                        #Found the length value.
                        result = result.group()[2:] #Remove the "l=".
                        pnfs_filesize = long(result)

                        #Get the os size.  os.stat() should have been called
                        # in get_file_size().
                        try:
                            os_filesize = long(self.pstat[stat.ST_SIZE])
                        except (TypeError, AttributeError):
                            raise detail

                        #Handle the case where the sizes match or the file
                        # is a large file.
                        if pnfs_filesize == os_filesize or \
                               (os_filesize == 1L and
                                pnfs_filesize > long(2L**31L) - 1):
                            print pnfs_filesize
                            return 0
                        #Handle the case where the sizes do not match.
                        else:
                            print "%s: filesize corruption: " \
                                      "OS size %s != PNFS L2 size %s" % \
                                      (os.strerror(errno.EBADFD),
                                       os_filesize, pnfs_filesize)
                            return 1

                        #Should never get here.
                        break
            except (IOError, OSError, TypeError, AttributeError):
                #There is no layer 2 to check.
                pass
            """

            print str(detail)
            return 1

##############################################################################

    def pls(self, intf):
        filename = self.use_file(self.filepath, int(intf.named_layer))
        os.system("ls -alsF \"%s\"" % filename)

    def pecho(self, intf):
        try:
            self.writelayer(intf.named_layer, intf.text)
            return 0
        except (OSError, IOError), detail:
            sys.stderr.write("%s\n" % str(detail))
            return 1

    def prm(self, intf):
        try:
            self.writelayer(intf.named_layer, "")
            return 0
        except (OSError, IOError), detail:
            sys.stderr.write("%s\n" % str(detail))
            return 1

    def pcp(self, intf):
        try:
            f = file_utils.open(intf.unixfile, 'r', unstable_filesystem=True)
            try:
                data = file_utils.readlines(f, unstable_filesystem=True)
            finally:
                f.close()

            file_data_as_string = ""
            for line in data:
                file_data_as_string = file_data_as_string + line

            self.writelayer(intf.named_layer, file_data_as_string)

            return 0
        except (OSError, IOError), detail:
            sys.stderr.write("%s\n" % str(detail))
            return 1

    def psize(self, intf):
        try:
            self.set_file_size(intf.filesize)
            return 0
        except (OSError, IOError), detail:
            sys.stderr.write("%s\n" % str(detail))
            return 1

    def pio(self):
        print "Feature not yet implemented."

        #fname = "%s/.(fset)(%s)(io)(on)" % (self.dir, self.file)
        #os.system("touch" + fname)

    def pid(self):
        try:
            self.get_id()
            print_results(self.id)
            return 0
        except (OSError, IOError), detail:
            sys.stderr.write("%s\n" % str(detail))
            return 1

    def pshowid(self):
        try:
            self.get_showid()
            print_results(self.showid)
            return 0
        except (OSError, IOError), detail:
            sys.stderr.write("%s\n" % str(detail))
            return 1
        except (AttributeError, ValueError), detail:
            sys.stderr.write("A valid pnfs id was not entered.\n")
            return 1

    def pconst(self):
        try:
            self.get_const()
            print_results(self.const)
            return 0
        except (OSError, IOError), detail:
            sys.stderr.write("%s\n" % str(detail))
            return 1

    def pnameof(self):
        try:
            self.get_nameof()
            print_results(self.nameof)
            return 0
        except (OSError, IOError), detail:
            sys.stderr.write("%s\n" % str(detail))
            return 1
        except (AttributeError, ValueError), detail:
            sys.stderr.write("A valid pnfs id was not entered.\n")
            return 1

    def ppath(self):
        try:
            rtn_results = self.get_path()
            print_results2(rtn_results)
            return 0
        except (OSError, IOError), detail:
            sys.stderr.write("%s\n" % str(detail))
            if detail.args[0] in [errno.ENODEV]:
                print_results2(detail.filename)
            return 1
        except (AttributeError, ValueError), detail:
            print detail
            sys.stderr.write("A valid pnfs id was not entered.\n")
            return 1

    def pmount_point(self):
        try:
            print_results(self.get_mount_point())
            return 0
        except (OSError, IOError), detail:
            sys.stderr.write("%s\n" % str(detail))
            return 1
        except (AttributeError, ValueError), detail:
            sys.stderr.write("A valid pnfs id was not entered.\n")
            return 1

    def pparent(self):
        try:
            self.get_parent()
            print_results(self.parent)
            return 0
        except (OSError, IOError), detail:
            sys.stderr.write("%s\n" % str(detail))
            return 1
        except (AttributeError, ValueError), detail:
            sys.stderr.write("A valid pnfs id was not entered.\n")
            return 1

    def pcounters(self):
        try:
            self.get_counters()
            print_results(self.counters)
            return 0
        except (OSError, IOError), detail:
            sys.stderr.write("%s\n" % str(detail))
            return 1

    def pcursor(self):
        try:
            self.get_cursor()
            print_results(self.cursor)
            return 0
        except (OSError, IOError), detail:
            sys.stderr.write("%s\n" % str(detail))
            return 1

    def pposition(self):
        try:
            self.get_position()
            print_results(self.position)
            return 0
        except (OSError, IOError), detail:
            sys.stderr.write("%s\n" % str(detail))
            return 1

    def pdatabase(self, intf):
        try:
            print_results(self.get_database(intf.file))
            return 0
        except (OSError, IOError), detail:
            sys.stderr.write("%s\n" % str(detail))
            return 1


    def pdown(self, intf):
        if os.environ['USER'] != "root":
            print "must be root to create enstore system-down wormhole"
            return

        dname = "/pnfs/fs/admin/etc/config/flags"
        if not os.access(dname, os.F_OK | os.R_OK):
            print "/pnfs/fs is not mounted"
            return

        fname = "/pnfs/fs/admin/etc/config/flags/disabled"
        f = file_utils.open(fname, 'w', unstable_filesystem=True)
        f.write(intf.reason)
        f.close()

        os.system("touch .(fset)(disabled)(io)(on)")

    def pup(self):
        if os.environ['USER'] != "root":
            print "must be root to create enstore system-down wormhole"
            return

        dname = "/pnfs/fs/admin/etc/config/flags"
        if not os.access(dname, os.F_OK | os.R_OK):
            print "/pnfs/fs is not mounted"
            return

        os.remove("/pnfs/fs/admin/etc/config/flags/disabled")

    def pdump(self):
        self.dump()

##############################################################################

class PnfsInterface(option.Interface):

    def __init__(self, args=sys.argv, user_mode=1):
        # fill in the defaults for the possible options
        #self.test = 0
        #self.status = 0
        #self.info = 0
        #self.file = ""
        #self.restore = 0
        #These my be used, they may not.
        #self.duplicate_file = None
        option.Interface.__init__(self, args=args, user_mode=user_mode)

    pnfs_user_options = {
        option.BFID:{option.HELP_STRING:"lists the bit file id for file",
                     option.DEFAULT_VALUE:option.DEFAULT,
                     option.DEFAULT_NAME:"bfid",
                     option.DEFAULT_TYPE:option.INTEGER,
                     option.VALUE_NAME:"file",
                     option.VALUE_TYPE:option.STRING,
                     option.VALUE_USAGE:option.REQUIRED,
                     option.VALUE_LABEL:"filename",
                     option.FORCE_SET_DEFAULT:option.FORCE,
		     option.USER_LEVEL:option.USER
                     },
        option.CAT:{option.HELP_STRING:"see --layer",
                    option.DEFAULT_VALUE:option.DEFAULT,
                    option.DEFAULT_NAME:"layer",
                    option.DEFAULT_TYPE:option.INTEGER,
                    option.VALUE_NAME:"file",
                    option.VALUE_TYPE:option.STRING,
                    option.VALUE_USAGE:option.REQUIRED,
                    option.VALUE_LABEL:"filename",
                    option.FORCE_SET_DEFAULT:option.FORCE,
                    option.USER_LEVEL:option.USER,
                    option.EXTRA_VALUES:[{option.DEFAULT_VALUE:option.DEFAULT,
                                          option.DEFAULT_NAME:"named_layer",
                                          option.DEFAULT_TYPE:option.INTEGER,
                                          option.VALUE_NAME:"named_layer",
                                          option.VALUE_TYPE:option.INTEGER,
                                          option.VALUE_USAGE:option.OPTIONAL,
                                          option.VALUE_LABEL:"layer",
                                          }]
                    },
        option.DUPLICATE:{option.HELP_STRING:"gets/sets duplicate file values",
                     option.DEFAULT_VALUE:option.DEFAULT,
                     option.DEFAULT_NAME:"duplicate",
                     option.DEFAULT_TYPE:option.INTEGER,
                     option.VALUE_USAGE:option.IGNORED,
		     option.USER_LEVEL:option.ADMIN,
                     option.EXTRA_VALUES:[{option.DEFAULT_VALUE:"",
                                           option.DEFAULT_NAME:"file",
                                           option.DEFAULT_TYPE:option.STRING,
                                           option.VALUE_NAME:"file",
                                           option.VALUE_TYPE:option.STRING,
                                           option.VALUE_USAGE:option.OPTIONAL,
                                           option.VALUE_LABEL:"filename",
                                         option.FORCE_SET_DEFAULT:option.FORCE,
                                           },
                                          {option.DEFAULT_VALUE:"",
                                          option.DEFAULT_NAME:"duplicate_file",
                                           option.DEFAULT_TYPE:option.STRING,
                                           option.VALUE_NAME:"duplicat_file",
                                           option.VALUE_TYPE:option.STRING,
                                           option.VALUE_USAGE:option.OPTIONAL,
                                       option.VALUE_LABEL:"duplicate_filename",
                                         option.FORCE_SET_DEFAULT:option.FORCE,
                                           },]
                     },
        #option.ENSTORE_STATE:{option.HELP_STRING:"lists whether enstore " \
        #                                         "is still alive",
        #                 option.DEFAULT_VALUE:option.DEFAULT,
        #                 option.DEFAULT_NAME:"enstore_state",
        #                 option.DEFAULT_TYPE:option.INTEGER,
        #                 option.VALUE_NAME:"directory",
        #                 option.VALUE_TYPE:option.STRING,
        #                 option.VALUE_USAGE:option.REQUIRED,
        #                 option.USER_LEVEL:option.USER,
        #                 option.FORCE_SET_DEFAULT:option.FORCE,
        #             },
        option.FILE_FAMILY:{option.HELP_STRING: \
                            "gets file family tag, default; "
                            "sets file family tag, optional",
                            option.DEFAULT_VALUE:option.DEFAULT,
                            option.DEFAULT_NAME:"file_family",
                            option.DEFAULT_TYPE:option.INTEGER,
                            option.VALUE_TYPE:option.STRING,
                            option.USER_LEVEL:option.USER,
                            option.VALUE_USAGE:option.OPTIONAL,
                   },
        option.FILE_FAMILY_WIDTH: {option.HELP_STRING:
                                   "gets file family width tag, default; "
                                   "sets file family width tag, optional",
                                   option.DEFAULT_VALUE: option.DEFAULT,
                                   option.DEFAULT_NAME: "file_family_width",
                                   option.DEFAULT_TYPE: option.INTEGER,
                                   option.VALUE_TYPE: option.STRING,
                                   option.USER_LEVEL: option.USER,
                                   option.VALUE_USAGE: option.OPTIONAL,
                   },
        option.FILE_FAMILY_WRAPPER:{option.HELP_STRING: \
                                    "gets file family wrapper tag, default; "
                                    "sets file family wrapper tag, optional",
                                    option.DEFAULT_VALUE:option.DEFAULT,
                                    option.DEFAULT_NAME:"file_family_wrapper",
                                    option.DEFAULT_TYPE:option.INTEGER,
                                    option.VALUE_TYPE:option.STRING,
                                    option.USER_LEVEL:option.USER,
                                    option.VALUE_USAGE:option.OPTIONAL,
                   },
	option.FILESIZE:{option.HELP_STRING:"print out real filesize",
			 option.VALUE_NAME:"file",
			 option.VALUE_TYPE:option.STRING,
			 option.VALUE_LABEL:"file",
                         option.USER_LEVEL:option.USER,
			 option.VALUE_USAGE:option.REQUIRED,
			 },
        option.INFO:{option.HELP_STRING:"see --xref",
                     option.DEFAULT_VALUE:option.DEFAULT,
                     option.DEFAULT_NAME:"xref",
                     option.DEFAULT_TYPE:option.INTEGER,
                     option.VALUE_NAME:"file",
                     option.VALUE_TYPE:option.STRING,
                     option.VALUE_USAGE:option.REQUIRED,
                     option.VALUE_LABEL:"filename",
                     option.USER_LEVEL:option.USER,
                     option.FORCE_SET_DEFAULT:option.FORCE,
                },
        option.LAYER:{option.HELP_STRING:"lists the layer of the file",
                      option.DEFAULT_VALUE:option.DEFAULT,
                      option.DEFAULT_NAME:"layer",
                      option.DEFAULT_TYPE:option.INTEGER,
                      option.VALUE_NAME:"file",
                      option.VALUE_TYPE:option.STRING,
                      option.VALUE_USAGE:option.REQUIRED,
                      option.VALUE_LABEL:"filename",
                      option.FORCE_SET_DEFAULT:option.FORCE,
                      option.USER_LEVEL:option.USER,
                      option.EXTRA_VALUES:[{option.DEFAULT_VALUE:
                                                                option.DEFAULT,
                                            option.DEFAULT_NAME:"named_layer",
                                            option.DEFAULT_TYPE:option.INTEGER,
                                            option.VALUE_NAME:"named_layer",
                                            option.VALUE_TYPE:option.INTEGER,
                                            option.VALUE_USAGE:option.OPTIONAL,
                                            option.VALUE_LABEL:"layer",
                                            }]
                 },
        option.LIBRARY:{option.HELP_STRING:"gets library tag, default; " \
                                      "sets library tag, optional",
                   option.DEFAULT_VALUE:option.DEFAULT,
                   option.DEFAULT_NAME:"library",
                   option.DEFAULT_TYPE:option.INTEGER,
                   option.VALUE_TYPE:option.STRING,
                   option.USER_LEVEL:option.USER,
                   option.VALUE_USAGE:option.OPTIONAL,
                   },
        #option.PNFS_STATE:{option.HELP_STRING:"lists whether pnfs is " \
        #                                      "still alive",
        #              option.DEFAULT_VALUE:option.DEFAULT,
        #              option.DEFAULT_NAME:"pnfs_state",
        #              option.DEFAULT_TYPE:option.INTEGER,
        #              option.VALUE_NAME:"directory",
        #              option.VALUE_TYPE:option.STRING,
        #              option.VALUE_USAGE:option.REQUIRED,
        #              option.USER_LEVEL:option.USER,
        #              option.FORCE_SET_DEFAULT:option.FORCE,
        #              },
        option.STORAGE_GROUP:{option.HELP_STRING:"gets storage group tag, " \
                              "default; sets storage group tag, optional",
                         option.DEFAULT_VALUE:option.DEFAULT,
                         option.DEFAULT_NAME:"storage_group",
                         option.DEFAULT_TYPE:option.INTEGER,
                         option.VALUE_TYPE:option.STRING,
                         option.USER_LEVEL:option.ADMIN,
                         option.VALUE_USAGE:option.OPTIONAL,
                   },
        option.TAG:{option.HELP_STRING:"lists the tag of the directory",
                    option.DEFAULT_VALUE:option.DEFAULT,
                    option.DEFAULT_NAME:"tag",
                    option.DEFAULT_TYPE:option.INTEGER,
                    option.VALUE_NAME:"named_tag",
                    option.VALUE_TYPE:option.STRING,
                    option.VALUE_USAGE:option.REQUIRED,
                    option.VALUE_LABEL:"tag",
                    option.FORCE_SET_DEFAULT:1,
                    option.USER_LEVEL:option.USER,
                    option.EXTRA_VALUES:[{option.DEFAULT_VALUE:"",
                                          option.DEFAULT_NAME:"directory",
                                          option.DEFAULT_TYPE:option.STRING,
                                          option.VALUE_NAME:"directory",
                                          option.VALUE_TYPE:option.STRING,
                                          option.VALUE_USAGE:option.OPTIONAL,
                                         option.FORCE_SET_DEFAULT:option.FORCE,
                                          }]
               },
        option.TAGCHMOD:{option.HELP_STRING:"changes the permissions"
                         " for the tag; use UNIX chmod style permissions",
                         option.DEFAULT_VALUE:option.DEFAULT,
                         option.DEFAULT_NAME:"tagchmod",
                         option.DEFAULT_TYPE:option.INTEGER,
                         option.VALUE_NAME:"permissions",
                         option.VALUE_TYPE:option.STRING,
                         option.VALUE_USAGE:option.REQUIRED,
                         option.FORCE_SET_DEFAULT:option.FORCE,
                         option.USER_LEVEL:option.USER,
                         option.EXTRA_VALUES:[{option.VALUE_NAME:"named_tag",
                                            option.VALUE_TYPE:option.STRING,
                                            option.VALUE_USAGE:option.REQUIRED,
                                            option.VALUE_LABEL:"tag",
                                              },]
                         },
        option.TAGCHOWN:{option.HELP_STRING:"changes the ownership"
                         " for the tag; OWNER can be 'owner' or 'owner.group'",
                         option.DEFAULT_VALUE:option.DEFAULT,
                         option.DEFAULT_NAME:"tagchown",
                         option.DEFAULT_TYPE:option.INTEGER,
                         option.VALUE_NAME:"owner",
                         option.VALUE_TYPE:option.STRING,
                         option.VALUE_USAGE:option.REQUIRED,
                         option.FORCE_SET_DEFAULT:option.FORCE,
                         option.USER_LEVEL:option.USER,
                         option.EXTRA_VALUES:[{option.VALUE_NAME:"named_tag",
                                            option.VALUE_TYPE:option.STRING,
                                            option.VALUE_USAGE:option.REQUIRED,
                                            option.VALUE_LABEL:"tag",
                                              },]
                         },
        option.TAGS:{option.HELP_STRING:"lists tag values and permissions",
                option.DEFAULT_VALUE:option.DEFAULT,
                option.DEFAULT_NAME:"tags",
                option.DEFAULT_TYPE:option.INTEGER,
                option.VALUE_USAGE:option.IGNORED,
                option.USER_LEVEL:option.USER,
                option.EXTRA_VALUES:[{option.DEFAULT_VALUE:"",
                                      option.DEFAULT_NAME:"directory",
                                      option.DEFAULT_TYPE:option.STRING,
                                      option.VALUE_NAME:"directory",
                                      option.VALUE_TYPE:option.STRING,
                                      option.VALUE_USAGE:option.OPTIONAL,
                                      option.FORCE_SET_DEFAULT:option.FORCE,
                                      }]
                },
        option.XREF:{option.HELP_STRING:"lists the cross reference " \
                                        "data for file",
                     option.DEFAULT_VALUE:option.DEFAULT,
                     option.DEFAULT_NAME:"xref",
                     option.DEFAULT_TYPE:option.INTEGER,
                     option.VALUE_NAME:"file",
                     option.VALUE_TYPE:option.STRING,
                     option.VALUE_USAGE:option.REQUIRED,
                     option.VALUE_LABEL:"filename",
                     option.USER_LEVEL:option.USER,
                     option.FORCE_SET_DEFAULT:option.FORCE,
                },
        }

    pnfs_admin_options = {
        option.CP:{option.HELP_STRING:"echos text to named layer of the file",
                   option.DEFAULT_VALUE:option.DEFAULT,
                   option.DEFAULT_NAME:"cp",
                   option.DEFAULT_TYPE:option.INTEGER,
                   option.VALUE_NAME:"unixfile",
                   option.VALUE_TYPE:option.STRING,
                   option.VALUE_USAGE:option.REQUIRED,
                   option.FORCE_SET_DEFAULT:option.FORCE,
                   option.USER_LEVEL:option.ADMIN,
                   option.EXTRA_VALUES:[{option.VALUE_NAME:"file",
                                         option.VALUE_TYPE:option.STRING,
                                         option.VALUE_USAGE:option.REQUIRED,
                                         option.VALUE_LABEL:"filename",
                                         },
                                        {option.VALUE_NAME:"named_layer",
                                         option.VALUE_TYPE:option.INTEGER,
                                         option.VALUE_USAGE:option.REQUIRED,
                                         option.VALUE_LABEL:"layer",
                                         },]
                   },
        option.CONST:{option.HELP_STRING:"",
                      option.DEFAULT_VALUE:option.DEFAULT,
                      option.DEFAULT_NAME:"const",
                      option.DEFAULT_TYPE:option.INTEGER,
                      option.VALUE_NAME:"file",
                      option.VALUE_TYPE:option.STRING,
                      option.VALUE_USAGE:option.REQUIRED,
                      option.VALUE_LABEL:"filename",
                      option.FORCE_SET_DEFAULT:option.FORCE,
                      option.USER_LEVEL:option.ADMIN,
                      },
        option.COUNTERS:{option.HELP_STRING:"",
                         option.DEFAULT_VALUE:option.DEFAULT,
                         option.DEFAULT_NAME:"counters",
                         option.DEFAULT_TYPE:option.INTEGER,
                         option.VALUE_NAME:"file",
                         option.VALUE_TYPE:option.STRING,
                         option.VALUE_USAGE:option.REQUIRED,
                         option.VALUE_LABEL:"filename",
                         option.FORCE_SET_DEFAULT:option.FORCE,
                         option.USER_LEVEL:option.ADMIN,
                         },
        option.COUNTERSN:{option.HELP_STRING:"(must have cwd in pnfs)",
                          option.DEFAULT_VALUE:option.DEFAULT,
                          option.DEFAULT_NAME:"countersN",
                          option.DEFAULT_TYPE:option.INTEGER,
                          option.VALUE_NAME:"dbnum",
                          option.VALUE_TYPE:option.STRING,
                          option.VALUE_USAGE:option.REQUIRED,
                          option.FORCE_SET_DEFAULT:option.FORCE,
                          option.USER_LEVEL:option.ADMIN,
                          },
        option.CURSOR:{option.HELP_STRING:"",
                       option.DEFAULT_VALUE:option.DEFAULT,
                       option.DEFAULT_NAME:"cursor",
                       option.DEFAULT_TYPE:option.INTEGER,
                       option.VALUE_NAME:"file",
                       option.VALUE_TYPE:option.STRING,
                       option.VALUE_USAGE:option.REQUIRED,
                       option.VALUE_LABEL:"filename",
                       option.FORCE_SET_DEFAULT:option.FORCE,
                       option.USER_LEVEL:option.ADMIN,
                       },
        option.DATABASE:{option.HELP_STRING:"",
                         option.DEFAULT_VALUE:option.DEFAULT,
                         option.DEFAULT_NAME:"database",
                         option.DEFAULT_TYPE:option.INTEGER,
                         option.VALUE_NAME:"file",
                         option.VALUE_TYPE:option.STRING,
                         option.VALUE_USAGE:option.REQUIRED,
                         option.VALUE_LABEL:"filename",
                         option.FORCE_SET_DEFAULT:option.FORCE,
                         option.USER_LEVEL:option.ADMIN,
                         },
        option.DATABASEN:{option.HELP_STRING:"(must have cwd in pnfs)",
                          option.DEFAULT_VALUE:option.DEFAULT,
                          option.DEFAULT_NAME:"databaseN",
                          option.DEFAULT_TYPE:option.INTEGER,
                          option.VALUE_NAME:"dbnum",
                          option.VALUE_TYPE:option.STRING,
                          option.VALUE_USAGE:option.REQUIRED,
                          option.FORCE_SET_DEFAULT:option.FORCE,
                          option.USER_LEVEL:option.ADMIN,
                          },
        option.DOWN:{option.HELP_STRING:"creates enstore system-down " \
                                        "wormhole to prevent transfers",
                option.DEFAULT_VALUE:option.DEFAULT,
                option.DEFAULT_NAME:"down",
                option.DEFAULT_TYPE:option.INTEGER,
                option.VALUE_NAME:"reason",
                option.VALUE_TYPE:option.STRING,
                option.VALUE_USAGE:option.REQUIRED,
                option.FORCE_SET_DEFAULT:option.FORCE,
                option.USER_LEVEL:option.ADMIN,
                },
        option.DUMP:{option.HELP_STRING:"dumps info",
              option.DEFAULT_VALUE:option.DEFAULT,
              option.DEFAULT_NAME:"dump",
              option.DEFAULT_TYPE:option.INTEGER,
              option.VALUE_USAGE:option.IGNORED,
              option.USER_LEVEL:option.ADMIN,
              },
        option.ECHO:{option.HELP_STRING:"sets text to named layer of the file",
                     option.DEFAULT_VALUE:option.DEFAULT,
                     option.DEFAULT_NAME:"echo",
                     option.DEFAULT_TYPE:option.INTEGER,
                     option.VALUE_NAME:"text",
                     option.VALUE_TYPE:option.STRING,
                     option.VALUE_USAGE:option.REQUIRED,
                     option.FORCE_SET_DEFAULT:option.FORCE,
                     option.USER_LEVEL:option.ADMIN,
                     option.EXTRA_VALUES:[{option.VALUE_NAME:"file",
                                           option.VALUE_TYPE:option.STRING,
                                           option.VALUE_USAGE:option.REQUIRED,
                                           option.VALUE_LABEL:"filename",
                                           },
                                          {option.VALUE_NAME:"named_layer",
                                           option.VALUE_TYPE:option.INTEGER,
                                           option.VALUE_USAGE:option.REQUIRED,
                                           option.VALUE_LABEL:"layer",
                                           },]
                },
        option.ID:{option.HELP_STRING:"prints the pnfs id",
                   option.DEFAULT_VALUE:option.DEFAULT,
                   option.DEFAULT_NAME:"id",
                   option.DEFAULT_TYPE:option.INTEGER,
                   option.VALUE_NAME:"file",
                   option.VALUE_TYPE:option.STRING,
                   option.VALUE_USAGE:option.REQUIRED,
                   option.VALUE_LABEL:"filename",
                   option.FORCE_SET_DEFAULT:option.FORCE,
                   option.USER_LEVEL:option.USER2,
              },
        option.IO:{option.HELP_STRING:"sets io mode (can't clear it easily)",
                   option.DEFAULT_VALUE:option.DEFAULT,
                   option.DEFAULT_NAME:"io",
                   option.DEFAULT_TYPE:option.INTEGER,
                   option.VALUE_NAME:"file",
                   option.VALUE_TYPE:option.STRING,
                   option.VALUE_USAGE:option.REQUIRED,
                   option.VALUE_LABEL:"filename",
                   option.FORCE_SET_DEFAULT:option.FORCE,
                   option.USER_LEVEL:option.ADMIN,
                   },
        option.LS:{option.HELP_STRING:"does an ls on the named layer " \
                                      "in the file",
                   option.DEFAULT_VALUE:option.DEFAULT,
                   option.DEFAULT_NAME:"ls",
                   option.DEFAULT_TYPE:option.INTEGER,
                   option.VALUE_NAME:"file",
                   option.VALUE_TYPE:option.STRING,
                   option.VALUE_USAGE:option.REQUIRED,
                   option.VALUE_LABEL:"filename",
                   option.FORCE_SET_DEFAULT:option.FORCE,
                   option.USER_LEVEL:option.ADMIN,
                   option.EXTRA_VALUES:[{option.DEFAULT_VALUE:option.DEFAULT,
                                         option.DEFAULT_NAME:"named_layer",
                                         option.DEFAULT_TYPE:option.INTEGER,
                                         option.VALUE_NAME:"named_layer",
                                         option.VALUE_TYPE:option.STRING,
                                         option.VALUE_USAGE:option.OPTIONAL,
                                         option.VALUE_LABEL:"layer",
                                         }]
              },
        option.MOUNT_POINT:{option.HELP_STRING:"prints the mount point of " \
                            "the pnfs file or directory",
                            option.DEFAULT_VALUE:option.DEFAULT,
                            option.DEFAULT_NAME:"mount_point",
                            option.DEFAULT_TYPE:option.INTEGER,
                            option.VALUE_NAME:"file",
                            option.VALUE_TYPE:option.STRING,
                            option.VALUE_USAGE:option.REQUIRED,
                            option.VALUE_LABEL:"filename",
                            option.FORCE_SET_DEFAULT:option.FORCE,
                            option.USER_LEVEL:option.USER2,
                            },
        option.NAMEOF:{option.HELP_STRING:"prints the filename of the pnfs id"\
                       " (CWD must be under /pnfs)",
                       option.DEFAULT_VALUE:option.DEFAULT,
                       option.DEFAULT_NAME:"nameof",
                       option.DEFAULT_TYPE:option.INTEGER,
                       option.VALUE_NAME:"pnfs_id",
                       option.VALUE_TYPE:option.STRING,
                       option.VALUE_USAGE:option.REQUIRED,
                       option.FORCE_SET_DEFAULT:option.FORCE,
                       option.USER_LEVEL:option.ADMIN,
                       },
        option.PARENT:{option.HELP_STRING:"prints the pnfs id of the parent " \
                       "directory (CWD must be under /pnfs)",
                       option.DEFAULT_VALUE:option.DEFAULT,
                       option.DEFAULT_NAME:"parent",
                       option.DEFAULT_TYPE:option.INTEGER,
                       option.VALUE_NAME:"pnfs_id",
                       option.VALUE_TYPE:option.STRING,
                       option.VALUE_USAGE:option.REQUIRED,
                       option.FORCE_SET_DEFAULT:option.FORCE,
                       option.USER_LEVEL:option.ADMIN,
                       },
        option.PATH:{option.HELP_STRING:"prints the file path of the pnfs id"\
                                        " (CWD must be under /pnfs)",
                     option.DEFAULT_VALUE:option.DEFAULT,
                     option.DEFAULT_NAME:"path",
                     option.DEFAULT_TYPE:option.INTEGER,
                     option.VALUE_NAME:"pnfs_id",
                     option.VALUE_TYPE:option.STRING,
                     option.VALUE_USAGE:option.REQUIRED,
                     option.FORCE_SET_DEFAULT:option.FORCE,
                     option.USER_LEVEL:option.ADMIN,
                     },
        option.POSITION:{option.HELP_STRING:"",
                         option.DEFAULT_VALUE:option.DEFAULT,
                         option.DEFAULT_NAME:"position",
                         option.DEFAULT_TYPE:option.INTEGER,
                         option.VALUE_NAME:"file",
                         option.VALUE_TYPE:option.STRING,
                         option.VALUE_USAGE:option.REQUIRED,
                         option.VALUE_LABEL:"filename",
                         option.FORCE_SET_DEFAULT:option.FORCE,
                         option.USER_LEVEL:option.ADMIN,
                         },
        option.RM:{option.HELP_STRING:"deletes (clears) named layer of the file",
                   option.DEFAULT_VALUE:option.DEFAULT,
                   option.DEFAULT_NAME:"rm",
                   option.DEFAULT_TYPE:option.INTEGER,
                   option.VALUE_NAME:"file",
                   option.VALUE_TYPE:option.STRING,
                   option.VALUE_USAGE:option.REQUIRED,
                   option.VALUE_LABEL:"filename",
                   option.FORCE_SET_DEFAULT:option.FORCE,
                   option.USER_LEVEL:option.ADMIN,
                   option.EXTRA_VALUES:[{option.VALUE_NAME:"named_layer",
                                         option.VALUE_TYPE:option.INTEGER,
                                         option.VALUE_USAGE:option.REQUIRED,
                                         option.VALUE_LABEL:"layer",
                                         },]
                   },
        option.SHOWID:{option.HELP_STRING:"prints the pnfs id information",
                       option.DEFAULT_VALUE:option.DEFAULT,
                       option.DEFAULT_NAME:"showid",
                       option.DEFAULT_TYPE:option.INTEGER,
                       option.VALUE_NAME:"pnfs_id",
                       option.VALUE_TYPE:option.STRING,
                       option.VALUE_USAGE:option.REQUIRED,
                       option.FORCE_SET_DEFAULT:option.FORCE,
                       option.USER_LEVEL:option.ADMIN,
                       },
        option.SIZE:{option.HELP_STRING:"sets the size of the file",
                     option.DEFAULT_VALUE:option.DEFAULT,
                     option.DEFAULT_NAME:"size",
                     option.DEFAULT_TYPE:option.INTEGER,
                     option.VALUE_NAME:"file",
                     option.VALUE_TYPE:option.STRING,
                     option.VALUE_USAGE:option.REQUIRED,
                     option.VALUE_LABEL:"filename",
                     option.FORCE_SET_DEFAULT:option.FORCE,
                     option.USER_LEVEL:option.USER2,
                     option.EXTRA_VALUES:[{option.VALUE_NAME:"filesize",
                                           option.VALUE_TYPE:option.LONG,
                                           option.VALUE_USAGE:option.REQUIRED,
                                           },]
                },
        option.TAGECHO:{option.HELP_STRING:"echos text to named tag",
                        option.DEFAULT_VALUE:option.DEFAULT,
                        option.DEFAULT_NAME:"tagecho",
                        option.DEFAULT_TYPE:option.INTEGER,
                        option.VALUE_NAME:"text",
                        option.VALUE_TYPE:option.STRING,
                        option.VALUE_USAGE:option.REQUIRED,
                        option.FORCE_SET_DEFAULT:option.FORCE,
                        option.USER_LEVEL:option.ADMIN,
                        option.EXTRA_VALUES:[{option.VALUE_NAME:"named_tag",
                                            option.VALUE_TYPE:option.STRING,
                                            option.VALUE_USAGE:option.REQUIRED,
                                            option.VALUE_LABEL:"tag",
                                              },]
                   },

        option.TAGRM:{option.HELP_STRING:"removes the tag (tricky, see DESY "
                                         "documentation)",
                      option.DEFAULT_VALUE:option.DEFAULT,
                      option.DEFAULT_NAME:"tagrm",
                      option.DEFAULT_TYPE:option.INTEGER,
                      option.VALUE_NAME:"named_tag",
                      option.VALUE_TYPE:option.STRING,
                      option.VALUE_USAGE:option.REQUIRED,
                      option.VALUE_LABEL:"tag",
                      option.FORCE_SET_DEFAULT:option.FORCE,
                      option.USER_LEVEL:option.ADMIN,
                 },
        option.UP:{option.HELP_STRING:"removes enstore system-down wormhole",
                   option.DEFAULT_VALUE:option.DEFAULT,
                   option.DEFAULT_NAME:"up",
                   option.DEFAULT_TYPE:option.INTEGER,
                   option.VALUE_USAGE:option.IGNORED,
                   option.USER_LEVEL:option.ADMIN,
                   },
        }

    def valid_dictionaries(self):
        return (self.help_options, self.pnfs_user_options,
                self.pnfs_admin_options)

    # parse the options like normal but make sure we have other args
    def parse_options(self):
        self.pnfs_id = "" #Assume the command is a dir and/or file.
        self.file = ""
        self.dir = ""
        option.Interface.parse_options(self)

        if not self.option_list:
            self.print_usage("No valid options were given.")

        #No pnfs options take extra arguments beyond those specifed in the
        # option dictionaries.  If there are print message and exit.
        self.check_correct_count()

        if getattr(self, "help", None):
            self.print_help()

        if getattr(self, "usage", None):
            self.print_usage()

##############################################################################

# This is a cleaner interface to access the tags in /pnfs

class Tag:
    def __init__(self, directory = None):
        self.dir = directory

    # write a new value to the specified tag
    # the file needs to exist before you call this
    # remember, tags are a propery of the directory, not of a file
    def writetag(self, tag, value, directory=None):
        if type(value) != types.StringType:
            value=str(value)

        #Remove all trailing .(access)(), .(nameof)(), .(parent)(), etc. path
        # components.
        if directory:
            use_dir = directory
        elif self.dir:
            use_dir = self.dir
        else:
            use_dir = None
        if use_dir:
            dir_path, dir_name = os.path.split(use_dir)
            use_dir = os.path.join(remove_special_paths(dir_path), dir_name)

        if use_dir:
            fname = os.path.join(use_dir, ".(tag)(%s)" % (tag,))
        else:
            #Make absolute with expand_path, which hides retries from
            # ESTALE errors.
            fname = ".(tag)(%s)" % (tag,)

        #Make sure this is the full file path of the tag.
        fname = enstore_functions2.expand_path(fname)

        #If directory is empty indicating the current directory, prepend it.
        #if not get_directory_name(self.dir):
        #    try:
        #        fname = os.path.join(os.getcwd(), fname)
        #    expect OSError:
        #        fname = ""

        #Determine if the target directory is in pnfs namespace
        if not is_pnfs_path(get_directory_name(fname)):
            raise IOError(errno.EINVAL,
                   os.strerror(errno.EINVAL) + ": Not a valid pnfs directory")

        try:
            f = file_utils.open(fname, 'w', unstable_filesystem=True)
            f.write(value)
            f.close()
        except (OSError, IOError):
            exc, msg = sys.exc_info()[:2]
            if msg.args[0] == errno.ENOTDIR:
                #If the error is ENOTDIR, then correct the path returned
                # to be the directory and not the tag file.
                use_msg = exc(errno.ENOTDIR, os.strerror(errno.ENOTDIR),
                              os.path.dirname(fname))
            else:
                use_msg = msg
            raise exc, use_msg, sys.exc_info()[2] #Don't have tb be local!

    # read the value stored in the requested tag
    def readtag(self, tag, directory=None):
        #Remove all trailing .(access)(), .(nameof)(), .(parent)(), etc. path
        # components.
	if directory:
            use_dir = directory
        elif self.dir:
            use_dir = self.dir
        else:
            use_dir = None
        if use_dir:
            dir_path, dir_name = os.path.split(use_dir)
            use_dir = os.path.join(remove_special_paths(dir_path), dir_name)

        if use_dir:
            fname = os.path.join(use_dir, ".(tag)(%s)" % (tag,))
        else:
            #Make absolute with expand_path, which hides retries from
            # ESTALE errors.
            fname = ".(tag)(%s)" % (tag,)

        #Make sure this is the full file path of the tag.
        fname = enstore_functions2.expand_path(fname)

        #If directory is empty indicating the current directory, prepend it.
        #if not get_directory_name(self.dir):
        #    fname = os.path.join(os.getcwd(), fname)

        #Determine if the target directory is in pnfs namespace
        if not is_pnfs_path(get_directory_name(fname)):
            raise IOError(errno.EINVAL,
                   os.strerror(errno.EINVAL) + ": Not a valid pnfs directory")

        try:
            f = file_utils.open(fname, 'r', unstable_filesystem=True)
            try:
                t = file_utils.readlines(f, unstable_filesystem=True)
            finally:
                f.close()
        except (OSError, IOError):
            exc, msg = sys.exc_info()[:2]
            if msg.args[0] == errno.ENOTDIR:
                #If the error is ENOTDIR, then correct the path returned
                # to be the directory and not the tag file.
                use_msg = exc(errno.ENOTDIR, os.strerror(errno.ENOTDIR),
                              os.path.dirname(fname))
            else:
                use_msg = msg
            raise exc, use_msg, sys.exc_info()[2] #Don't have tb be local!

        return t

    ##########################################################################

    #Print out the current settings for all directory tags.
    def ptags(self, intf):

        #If the directory to use was passed in use that for the current
        # working directory.  Otherwise uses the current working directory.

        if hasattr(intf, "directory"):
            try:
                cwd = os.path.abspath(intf.directory)
            except OSError, detail:
                sys.stderr.write("%s\n" % (str(detail,)))
                return 1
        else:
            try:
                #Make sure that the current working directory is still valid.
                cwd = os.path.abspath(os.getcwd())
            except OSError:
                msg = sys.exc_info()[1]
                if msg.errno == errno.ENOENT:
                    msg_str = "%s: %s" % (os.strerror(errno.ENOENT),
                                          "No current working directory")
                    sys.stderr.write("%s\n" % msg_str)
                else:
                    sys.stderr.write("%s\n" % (str(msg),))
                return 1

        filename = os.path.join(cwd, ".(tags)(all)")

        try:
            f = file_utils.open(filename, "r", unstable_filesystem=True)
            try:
                data = file_utils.readlines(f, unstable_filesystem=True)
            finally:
                f.close()
        except IOError, detail:
            sys.stderr.write("%s\n" % (str(detail),))
            return 1

        #print the top portion of the output.  Note: the values placed into
        # line have a newline at the end of them, this is why line[:-1] is
        # used to remove it.
        for line in data:
            try:
                tag = string.split(line[7:], ")")[0]
                tag_info = self.readtag(tag, directory = cwd)
                print line[:-1], "=",  tag_info[0]
            except (OSError, IOError, IndexError), detail:
                print line[:-1], ":", str(detail)

        #Print the bottom portion of the output.
        for line in data:
            tag_file = os.path.join(cwd, line[:-1])
            os.system("ls -l \"" + tag_file + "\"")

        return 0

    def ptag(self, intf):
        try:
            if hasattr(intf, "directory") and intf.directory:
                tag = self.readtag(intf.named_tag, intf.directory)
            else:
                tag = self.readtag(intf.named_tag)
            print tag[0]
            return 0
        except (OSError, IOError, IndexError), detail:
            sys.stderr.write("%s\n" % (str(detail),))
            return 1

    def ptagecho(self, intf):
        try:
            self.writetag(intf.named_tag, intf.text)
        except (OSError, IOError), detail:
            sys.stderr.write("%s\n" % (str(detail),))
            return 1

    def ptagrm(self):
        sys.stderr.write("Feature not yet implemented.\n")
        return 1

    ##########################################################################

    def ptagchown(self, intf):
        #Determine the directory to use.
        if self.dir:
            cwd = self.dir
        else:
            try:
                cwd = os.getcwd()
            except OSError, msg:
                if msg.errno == errno.ENOENT:
                    msg_str = "%s: %s" % (os.strerror(errno.ENOENT),
                                          "No current working directory")
                    sys.stderr.write("%s\n" % (msg_str,))
                else:
                    sys.stderr.write("%s\n" % (str(msg),))
                return 1

        #Format the tag filename string.
        fname = os.path.join(cwd, ".(tag)(%s)" % (intf.named_tag,))

        #Determine if the target directory is in pnfs namespace
        if fname[:6] != "/pnfs/":
            msg_str = os.strerror(errno.EINVAL) + ": Not a valid pnfs directory"
            sys.stderr.write("%s\n" % (msg_str,))
            return 1

        #Determine if the tag file exists.
        try:
            pstat = file_utils.get_stat(fname, unstable_filesystem=True)
        except OSError, msg:
            sys.stderr.write("%s\n" % (str(msg),))
            return 1

        #Deterine the existing ownership.
        uid = pstat[stat.ST_UID]
        gid = pstat[stat.ST_GID]

        #Determine if the owner or owner.group was specified.
        owner = intf.owner.split(".")
        if len(owner) == 1:
            uid = owner[0]
        elif len(owner) == 2:
            uid = owner[0]
            gid = owner[1]
        else:
            msg_str = os.strerror(errno.EINVAL) + ": Incorrect owner field"
            sys.stderr.write("%s\n" % (msg_str,))
            return 1

        #If the user and group are ids, convert them to integers.
        try:
            uid = int(uid)
        except ValueError:
            pass
        try:
            gid = int(gid)
        except ValueError:
            pass

        if uid and type(uid) != types.IntType:
            try:
                uid = pwd.getpwnam(str(uid))[2]
            except KeyError:
                msg_str = os.strerror(errno.EINVAL) + ": Not a valid user"
                sys.stderr.write("%s\n" % (msg_str,))
                return 1

        if gid and type(gid) != types.IntType:
            try:
                gid = grp.getgrnam(str(gid))[2]
            except KeyError:
                msg_str = os.strerror(errno.EINVAL) + ": Not a valid group"
                sys.stderr.write("%s\n" % (msg_str,))
                return 1

        try:
            os.chown(fname, uid, gid)
            #os.utime(fname, None)
        except OSError, detail:
            sys.stderr.write("%s\n" % (str(detail),))
            return 1

        return 0


    def ptagchmod(self, intf):
        #Determine the directory to use.
        if self.dir:
            cwd = self.dir
        else:
            try:
                cwd = os.getcwd()
            except OSError, msg:
                if msg.errno == errno.ENOENT:
                    msg_str = "%s: %s" % (os.strerror(errno.ENOENT),
                                          "No current working directory")
                    sys.stderr.write("%s\n" % (msg_str,))
                else:
                    sys.stderr.write("%s\n" % (str(msg),))
                return 1

        #Format the tag filename string.
        fname = os.path.join(cwd, ".(tag)(%s)" % (intf.named_tag,))

        #Determine if the target directory is in pnfs namespace
        if fname[:6] != "/pnfs/":
            msg_str = os.strerror(errno.EINVAL) + ": Not a valid pnfs directory"
            sys.stderr.write("%s\n" % (msg_str,))
            return 1

        #Determine if the tag file exists.
        try:
            pstat = file_utils.get_stat(fname, unstable_filesystem=True)
        except OSError, msg:
            sys.stderr.write("%s\n" % (str(msg),))
            return 1

        #Deterine the existing ownership.
        st_mode = pstat[stat.ST_MODE]

        try:
            #If the user entered the permission numerically, this is it...
            set_mode = enstore_functions2.numeric_to_bits(intf.permissions)
        except (TypeError, ValueError):
            #...else try the symbolic way.
            try:
                set_mode = enstore_functions2.symbolic_to_bits(
                    intf.permissions, st_mode)
            except (TypeError, ValueError):
                msg_str = "%s: Invalid permission field" % \
                      (os.strerror(errno.EINVAL),)
                sys.stderr.write("%s\n" % (msg_str,))
                return 1
        try:
            os.chmod(fname, int(set_mode))
            #os.utime(fname, None)
        except OSError, detail:
            sys.stderr.write("%s\n" % (str(detail),))
            return 1

        return 0

    ##########################################################################

    INVALID_CHARACTERS = "Pnfs tag, %s, contains invalid characters."

    #Print or edit the library
    def plibrary(self, intf):
        try:
            if intf.library == 1:
                print self.get_library()
            else:
                if charset.is_string_in_character_set(intf.library,
                                                      charset.charset + ","):
                    #As of encp v3_6a allow the comma (,) character
                    # so that copies can be enabled.
                    self.set_library(intf.library)
                else:
                    msg_str = self.INVALID_CHARACTERS % ("library",)
                    sys.stderr.write("%s\n" % (msg_str,))
                    return 1
            return 0
        except (OSError, IOError), detail:
            sys.stderr.write("%s\n" % (str(detail),))
            return 1

    #Print or edit the file family.
    def pfile_family(self, intf):
        try:
            if intf.file_family == 1:
                print self.get_file_family()
            else:
                #Restrict the characters allowed in the file_family.
                if not charset.is_in_charset(intf.file_family):
                    msg_str = self.INVALID_CHARACTERS % ("file_family",)
                    sys.stderr.write("%s\n" % (msg_str,))
                    return 1
                #Don't allow users to set file_families with the
                # migration pattern.
                elif re.search(".*-MIGRATION$", intf.file_family):
                    msg_str = "File familes ending in -MIGRATION are forbidden."
                    sys.stderr.write("%s\n" % (msg_str,))
                    return 1
                #Don't allow users to set file_families with the
                # duplication pattern.
                elif re.search("_copy_[0-9]*$", intf.file_family):
                    msg_str = "File familes ending in _copy_# are forbidden."
                    sys.stderr.write("%s\n" % (msg_str,))
                    return 1
                else:
                    self.set_file_family(intf.file_family)

            return 0
        except (OSError, IOError), detail:
            sys.stderr.write("%s\n" % (str(detail),))
            return 1

    #Print or edit the file family wrapper.
    def pfile_family_wrapper(self, intf):
        try:
            if intf.file_family_wrapper == 1:
                print self.get_file_family_wrapper()
            else:
                if charset.is_in_charset(intf.file_family_wrapper):
                    self.set_file_family_wrapper(intf.file_family_wrapper)
                else:
                    msg_str = self.INVALID_CHARACTERS % ("file_family_wrapper",)
                    sys.stderr.write("%s\n" % (msg_str,))
                    return 1
            return 0
        except (OSError, IOError), detail:
            sys.stderr.write("%s\n" % (str(detail),))
            return 1

    # Print or edit the file family width.
    def pfile_family_width(self, intf):
        try:
            if not intf.file_family_width:
                print self.get_file_family_width()
                return 0
            if isinstance(intf.file_family_width, (int, long)):
                if intf.file_family_width > 0:
                    self.set_file_family_width(intf.file_family_width)
                    return 0
                else:
                    msg_str = "Pnfs tag, {}, has to be positive integer greater than 0.".format("file_family_width")
                    sys.stderr.write("%s\n" % (msg_str,))
                    return 1
            else:
                msg_str = "Pnfs tag, {}, has to be positive integer greater than 0.".format("file_family_width")
                sys.stderr.write("%s\n" % (msg_str,))
                return 1
        except (OSError, IOError), detail:
            sys.stderr.write("%s\n" % (str(detail),))
            return 1

    #Print or edit the storage group.
    def pstorage_group(self, intf):
        try:
            if intf.storage_group == 1:
                print self.get_storage_group()
            else:
                if charset.is_in_charset(intf.storage_group):
                    self.set_storage_group(intf.storage_group)
                else:
                    msg_str = self.INVALID_CHARACTERS % ("storage_group",)
                    sys.stderr.write("%s\n" % (msg_str,))
                    return 1
            return 0
        except (OSError, IOError), detail:
            sys.stderr.write("%s\n" % (str(detail),))
            return 1


    ##########################################################################

    # store a new tape library tag
    def set_library(self,value, directory=None):
        if directory:
            self.writetag("library", value, directory)
        else:
            self.writetag("library", value)
            self.get_library()

        return value

    # get the tape library
    def get_library(self, directory=None):
        try:
            if directory:
                library = self.readtag("library", directory)[0].strip()
            else:
                library = self.readtag("library")[0].strip()
                self.library = library
        except IndexError:
            #Only OSError and IOError should be raised.
            raise IOError(errno.EIO, "Library tag is empty.")

        return library

    ##########################################################################

    # store a new file family tag
    def set_file_family(self, value, directory=None):
        if directory:
            self.writetag("file_family", value, directory)
        else:
            self.writetag("file_family", value)
            self.get_file_family()

        return value

    # get the file family
    def get_file_family(self, directory=None):
        try:
            if directory:
                file_family = self.readtag("file_family", directory)[0].strip()
            else:
                file_family = self.readtag("file_family")[0].strip()
                self.file_family = file_family
        except IndexError:
            #Only OSError and IOError should be raised.
            raise IOError(errno.EIO, "File family tag is empty.")

        return file_family

    ##########################################################################

    # store a new file family wrapper tag
    def set_file_family_wrapper(self, value, directory=None):
        if directory:
            self.writetag("file_family_wrapper", value, directory)
        else:
            self.writetag("file_family_wrapper", value)
            self.get_file_family_wrapper()

        return value

    # get the file family
    def get_file_family_wrapper(self, directory=None):
        try:
            if directory:
                file_family_wrapper = self.readtag("file_family_wrapper",
                                                   directory)[0].strip()
            else:
                file_family_wrapper = self.readtag(
                    "file_family_wrapper")[0].strip()
                self.file_family_wrapper = file_family_wrapper
        except IndexError:
            #Only OSError and IOError should be raised.
            raise IOError(errno.EIO, "File family wrapper tag is empty.")

        return file_family_wrapper

    ##########################################################################

    # store a new file family width tag
    # this is the number of open files (ie simultaneous tapes) at one time
    def set_file_family_width(self, value, directory=None):
        if directory:
            self.writetag("file_family_width", value, directory)
        else:
            self.writetag("file_family_width", value)
            self.get_file_family_width()

        return value

    # get the file family width
    def get_file_family_width(self, directory=None):
        try:
            if directory:
                file_family_width = self.readtag("file_family_width",
                                                 directory)[0].strip()
            else:
                file_family_width = self.readtag(
                    "file_family_width")[0].strip()
                self.file_family_width = file_family_width
        except IndexError:
            #Only OSError and IOError should be raised.
            raise IOError(errno.EIO, "File family width tag is empty.")

        return file_family_width

    ##########################################################################

    # store a new storage group tag
    # this is group of volumes assigned to one experiment or group of users
    def set_storage_group(self, value, directory=None):
        if directory:
            self.writetag("storage_group", value, directory)
        else:
            self.writetag("storage_group", value)
            self.get_storage_group()

        return value

    # get the storage group
    def get_storage_group(self, directory=None):
        try:
            if directory:
                storage_group = self.readtag("storage_group",
                                             directory)[0].strip()
            else:
                storage_group = self.readtag("storage_group")[0].strip()
                self.storage_group = storage_group
        except IndexError:
            #Only OSError and IOError should be raised.
            raise IOError(errno.EIO, "Storage group tag is empty.")

        return storage_group

    ##########################################################################

    def penstore_state(self):
        fname = os.path.join(self.dir, ".(config)(flags)/disabled")
        if os.access(fname, os.F_OK):# | os.R_OK):
            f=file_utils.open(fname, 'r', unstable_filesystem=True)
            try:
                self.enstore_state = file_utils.readlines(f,
                                                    unstable_filesystem=True)
            finally:
                f.close()
            print "Enstore disabled:", self.enstore_state[0],
        else:
            print "Enstore enabled"

    def ppnfs_state(self):
        fname = "%s/.(config)(flags)/.(id)(pnfs_state)" % self.dir
        if os.access(fname, os.F_OK | os.R_OK):
            f=file_utils.open(fname, 'r', unstable_filesystem=True)
            try:
                self.pnfs_state = file_utils.readlines(f, unstable_filesystem=True)
            finally:
                f.close()
            print "Pnfs:", self.pnfs_state[0],
        else:
            print "Pnfs: unknown"

##############################################################################

class N:
    def __init__(self, dbnum, directory = None):
        if directory:
            self.dir = directory
        else:
            try:
                self.dir = os.getcwd()
            except OSError:
                self.dir = ""
        self.dbnum = dbnum

    # get the cursor information
    def get_countersN(self, dbnum=None):
        #Remove all trailing .(access)(), .(nameof)(), .(parent)(), etc. path
        # components.
        use_dir = remove_special_paths(self.dir)
        if dbnum != None:
            fname = os.path.join(use_dir,".(get)(counters)(%s)"%(dbnum,))
        else:
            fname = os.path.join(use_dir,".(get)(counters)(%s)"%(self.dbnum,))
        f = file_utils.open(fname, 'r', unstable_filesystem=True)
        try:
            self.countersN = file_utils.readlines(f, unstable_filesystem=True)
        finally:
            f.close()
        return self.countersN

    # get the database information
    # Sample answer:  admin:0:r:enabled:/diskb/pnfs/db/admin
    def get_databaseN(self, dbnum=None):
        #Remove all trailing .(access)(), .(nameof)(), .(parent)(), etc. path
        # components.
        use_dir = remove_special_paths(self.dir)
        if dbnum != None:
            fname = os.path.join(use_dir, ".(get)(database)(%s)"%(dbnum,))
        else:
            fname = os.path.join(use_dir, ".(get)(database)(%s)"%(self.dbnum,))

        f = file_utils.open(fname, 'r', unstable_filesystem=False)
        try:
            self.databaseN = file_utils.readline(f, unstable_filesystem=False)
        finally:
            f.close()
        if len(self.databaseN) > 0:
            self.databaseN = string.replace(self.databaseN, "\n", "")

        return self.databaseN

    def pdatabaseN(self, intf):
        try:
            self.get_databaseN(intf.dbnum)
            print_results(self.databaseN)
        except (OSError, IOError), detail:
            print str(detail)

    def pcountersN(self, intf):
        try:
            self.get_countersN(intf.dbnum)
            print_results(self.countersN)
        except (OSError, IOError), detail:
            print str(detail)

_mtab = None

# get_mtab() -- read /etc/mtab, for local/remote pnfs translation

def get_mtab():
    global _mtab
    if _mtab == None:
        _mtab = {}
        try:
            f = file_utils.open('/etc/mtab')
            l = f.readline()
            while l:
                lc = string.split(l)
                if lc[1][:5] == '/pnfs':
                    c1 = string.split(lc[0], ':')
                    if len(c1) > 1:
                        _mtab[lc[1]] = (c1[1], c1[0])
                    else:
                        _mtab[lc[1]] = (c1[0], None)
                l = f.readline()
            f.close()
        except:
            _mtab = {}
            f.close()
    return _mtab

LOCAL_PNFS_PREFIX = '/pnfs/fs/usr'

# get_local_pnfs_path(p) -- find local pnfs path

def get_local_pnfs_path(p):
    mtab = get_mtab()
    for i in mtab.keys():
        if string.find(p, i) == 0 and \
           string.split(os.uname()[1], '.')[0] == mtab[i][1]:
            p1 = os.path.join(LOCAL_PNFS_PREFIX, string.replace(p, i, mtab[i][0][1:]))
            if os.access(p1, os.F_OK):
                return p1
            else:
                return p
    return p

# get_abs_pnfs_path(p) -- find absolute pnfs path -- if /pnfs/fs is available

def get_abs_pnfs_path(p):
    mtab = get_mtab()
    for i in mtab.keys():
        if string.find(p, i) == 0:
            p1 = os.path.join(LOCAL_PNFS_PREFIX, string.replace(p, i, mtab[i][0][1:]))
            if os.access(p1, os.F_OK):
                return p1
            else:
                return p
    return p

# get_normal_pnfs_path(p)
#
# from /pnfs/fs/usr/XXX to get /pnfs/XXX

def get_normal_pnfs_path(p):
    # is it /pnfs/fs/usr*?
    if p[:12] != LOCAL_PNFS_PREFIX:
        return p

    p1= p[12:]
    mtab = get_mtab()
    for i in mtab.keys():
        if p1.find(mtab[i][0]) == 0:
            p2 = p1.replace(mtab[i][0], i)
            if os.access(p2, os.F_OK):
                return p2
            else:
                return p
    return p

# This is a cleaner interface to access the file, as well as its
# metadata, in /pnfs

class File:
	# the file could be a simple name, or a dictionary of file attributes
	def __init__(self, file):
		if type(file) == types.DictionaryType:  # a dictionary
			self.volume = file['external_label']
			self.location_cookie = file['location_cookie']
			self.size = str(file['size'])
			if file.has_key('file_family'):
				self.file_family = file['file_family']
			else:
				self.file_family = "unknown"
                        if file.has_key('pnfs_mapname'):
			    self.volmap = file['pnfs_mapname']
                        else:
                            self.volmap = ''
			self.pnfs_id = file['pnfsid']
                        if file.has_key('pnfsvid'):
			    self.pnfs_vid = file['pnfsvid']
                        else:
			    self.pnfs_vid = ''
			self.bfid = file['bfid']
			if file.has_key('drive'):
			    self.drive = file['drive']
			else:
			    self.drive = ''
			if file.has_key('pnfs_name0'):
			    self.path = file['pnfs_name0']
			else:
			    self.path = 'unknown'
			if file.has_key('complete_crc'):
			    self.complete_crc = str(file['complete_crc'])
			else:
			    self.complete_crc = ''
			self.p_path = self.path
		else:
			self.path = os.path.abspath(file)
			# does it exist?
                        try:
				f = file_utils.open(self.layer_file(4),
                                                    unstable_filesystem=True)
                                try:
                                    finfo = map(string.strip,
                                                file_utils.readlines(f,
                                                    unstable_filesystem=True))
                                finally:
                                    f.close()
				if len(finfo) == 11:
					self.volume,\
					self.location_cookie,\
					self.size, self.file_family,\
					self.p_path, self.volmap,\
					self.pnfs_id, self.pnfs_vid,\
					self.bfid, self.drive, \
					self.complete_crc = finfo
				elif len(finfo) == 10:
					self.volume,\
					self.location_cookie,\
					self.size, self.file_family,\
					self.p_path, self.volmap,\
					self.pnfs_id, self.pnfs_vid,\
					self.bfid, self.drive = finfo
					self.complete_crc = ''
				elif len(finfo) == 9:
					self.volume,\
					self.location_cookie,\
					self.size, self.file_family,\
					self.p_path, self.volmap,\
					self.pnfs_id, self.pnfs_vid,\
					self.bfid = finfo
					self.drive = "unknown:unknown"
					self.complete_crc = ''
				else:	# corrupted L4
					self.volume = "corrupted L4"
					self.location_cookie = ""
					self.size = None
					self.file_family = ""
					self.volmap = ""
					self.pnfs_id = "corrputed L4"
					self.pnfs_vid = ""
					self.bfid = "corrupted L4"
					self.drive = ""
					self.complete_crc = ''
					self.p_path = self.path

				# if self.p_path != self.path:
				#	raise 'DIFFERENT_PATH'
				#	print 'different paths'
				#	print '\t f>', self.path
				#	print '\t 4>', p_path
                        except IOError:
				self.volume = ""
				self.location_cookie = ""
				self.size = None
				self.file_family = ""
				self.volmap = ""
				self.pnfs_id = ""
				self.pnfs_vid = ""
				self.bfid = ""
				self.drive = ""
				self.complete_crc = ''
				self.p_path = self.path
			except:
				exc_type, exc_value = sys.exc_info()[:2]
				print exc_type, exc_value
		return

	# layer_file(i) -- compose the layer file name
	def layer_file(self, i):
		if self.file()[:9] == ".(access)":
			return "%s(%d)"%(self.path, i)
		else:
			return os.path.join(self.dir(),
                                    '.(use)(%d)(%s)'%(i, self.file()))

	# id_file() -- compose the id file name
	def id_file(self):
		return os.path.join(self.dir(), '.(id)(%s)'%(self.file()))

        # parent_file() -- compose the parent id file name
        def parent_file(self):
                try:
                        #Try and avoid unecessary .(id)() (P)NFS quires.
                        use_id = self.r_pnfs_id
                except AttributeError:
                        use_id = self.get_pnfs_id()

                return os.path.join(self.dir(), '.(parent)(%s)' % (use_id))

	# size_file -- compose the size file, except for the actual size
	def size_file(self):
		return os.path.join(self.dir(),
                                    '.(fset)(%s)(size)'%(self.file()))

	# dir() -- get the directory of this file
	def dir(self):
		return os.path.dirname(self.path)

	# file() -- get the basename of this file
	def file(self):
		return os.path.basename(self.path)

	# get_pnfs_id() -- get pnfs id from pnfs id file
	def get_pnfs_id(self):
		f = file_utils.open(self.id_file(), unstable_filesystem=True)
                try:
                    self.r_pnfs_id = file_utils.readline(
                        f, unstable_filesystem=True)[:-1]
                finally:
                    f.close()
		return self.r_pnfs_id

        # get_parent_id() -- get parent pnfs id from pnfs id file
        def get_parent_id(self):
                f = file_utils.open(self.parent_file(),
                                    unstable_filesystem=True)
                try:
                    self.parent_id = file_utils.readline(
                        f, unstable_filesystem=True)[:-1]
                finally:
                    f.close()
                return self.parent_id

	def show(self):
		print "           file =", self.path
		print "         volume =", self.volume
		print "location_cookie =", self.location_cookie
		print "           size =", self.size
		print "    file_family =", self.file_family
		print "         volmap =", self.volmap
		print "        pnfs_id =", self.pnfs_id
		print "       pnfs_vid =", self.pnfs_vid
		print "           bfid =", self.bfid
		print "          drive =", self.drive
		print "      meta-path =", self.p_path
		print "   complete_crc =", self.complete_crc
		return

	# set_size() -- set size in pnfs
	def set_size(self):
		if not self.exists():
			# do nothing if it doesn't exist
			return
		if long(self.size) > 2147483647L:
			size2 = 1
		else:
			size2 = long(self.size)
		real_size = file_utils.get_stat(
                    self.path, unstable_filesystem=True)[stat.ST_SIZE]
		if long(real_size) == long(size2):	# do nothing
			return
		size = str(size2)
		if size[-1] == 'L':
			size = size[:-1]
		fname = self.size_file()+'('+size+')'
		f = file_utils.open(fname, "w", unstable_filesystem=True)
		f.close()
		real_size = file_utils.get_stat(
                    self.path, unstable_filesystem=True)[stat.ST_SIZE]
		if long(real_size) != long(size2):
			# oops, have to reset it again
			f = file_utils.open(fname, "w",
                                            unstable_filesystem=True)
			f.close()
		return

	# update() -- write out to pnfs files
	def update(self, pnfsid=None):
		if not self.bfid:
			return
		if not self.consistent():
			raise ValueError('INCONSISTENT')
		if self.exists():
			# writing layer 1
			f = file_utils.open(self.layer_file(1), 'w',
                                            unstable_filesystem=True)
			f.write(self.bfid)
			f.close()
			# writing layer 4
			f = file_utils.open(self.layer_file(4), 'w',
                                            unstable_filesystem=True)
			f.write(self.volume+'\n')
			f.write(self.location_cookie+'\n')
			f.write(str(self.size)+'\n')
			f.write(self.file_family+'\n')
			f.write(self.p_path+'\n')
			f.write(self.volmap+'\n')
			if not pnfsid:
				# always use real pnfs id
				f.write(self.get_pnfs_id()+'\n')
			else:
				f.write(self.pnfs_id+'\n')
			f.write(self.pnfs_vid+'\n')
			f.write(self.bfid+'\n')
			f.write(self.drive+'\n')
			if self.complete_crc:
				f.write(str(self.complete_crc)+'\n')
			f.close()
			# set file size
			self.set_size()
		return

	# consistent() -- to see if data is consistent
	def consistent(self):
		# required field
		if not self.bfid or not self.volume \
                        or self.size == None  \
			or not self.location_cookie \
			or not self.file_family or not self.path \
			or not self.pnfs_id or not self.bfid \
			or not self.p_path:
			return 0
		return 1



	# exists() -- to see if the file exists in /pnfs area
	def exists(self):
		return os.access(self.path, os.F_OK)

	# create() -- create the file
	def create(self, pnfsid=None):
		# do not create if there is no BFID
		if not self.bfid:
			return
		if not self.exists() and self.consistent():
			f = file_utils.open(self.path, 'w',
                                            unstable_filesystem=True)
			f.close()
			self.update(pnfsid)

	# update_bfid(bfid) -- change the bfid
	def update_bfid(self, bfid):
		if bfid != self.bfid:
			self.bfid = bfid
			self.update()

	# set() -- set values
	def set(self, file):
		changed = 0
		res = None
		if file.has_key('external_label'):
			self.volume = file['external_label']
			changed = 1
		if file.has_key('location_cookie'):
			self.location_cookie = file['location_cookie']
			changed = 1
		if file.has_key('size'):
			self.size = file['size']
			changed = 1
		if file.has_key('file_family'):
			self.file_family = file['file_family']
			changed = 1
		if file.has_key('pnfs_mapname'):
			self.volmap = file['pnfs_mapname']
			changed = 1
		if file.has_key('pnfsid'):
			self.pnfs_id = file['pnfsid']
			changed = 1
		if file.has_key('pnfsvid'):
			self.pnfs_vid = file['pnfsvid']
			changed = 1
		if file.has_key('bfid'):
			self.bfid = file['bfid']
			changed = 1
		if file.has_key('drive'):
			self.drive = file['drive']
			changed = 1
		if file.has_key('pnfs_name0'):
			self.path = file['pnfs_name0']
			changed = 1
		if file.has_key('complete_crc'):
			self.complete_crc = file['complete_crc']
			changed = 1
		if changed:
			res = self.update()
		return res


##############################################################################

def do_work(intf):
    rtn = 0

    Trace.init("PNFS_CLIENT")

    try:
        if intf.file:
            p=Pnfs(intf.file)
            t=None
            n=None
        elif intf.pnfs_id:
            p=Pnfs(intf.pnfs_id, shortcut=True)
            t=None
            n=None
        elif hasattr(intf, "dbnum") and intf.dbnum:
            p=None
            t=None
            n=N(intf.dbnum)
        else:
            p=None
            if intf.dir:
                t=Tag(intf.dir)
            elif hasattr(intf, "directory") and intf.directory:
                t=Tag(intf.directory)
            else:
                t=Tag(os.getcwd())
            n=None
    except OSError, msg:
        print str(msg)
        return 1

    for arg in intf.option_list:
        if string.replace(arg, "_", "-") in intf.options.keys():
            arg = string.replace(arg, "-", "_")
            for instance in [t, p, n]:
                if getattr(instance, "p"+arg, None):
                    try:
                        #Not all functions use/need intf passed in.
                        rtn = apply(getattr(instance, "p" + arg), ())
                    except TypeError:
                        rtn = apply(getattr(instance, "p" + arg), (intf,))
                    break
            else:
                print "p%s not found" % arg
                rtn = 1

    return rtn

##############################################################################
if __name__ == "__main__":   # pragma: no cover

    intf = PnfsInterface(user_mode=0)

    intf._mode = "admin"

    sys.exit(do_work(intf))
