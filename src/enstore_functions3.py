#!/usr/bin/env python

"""
This file contains function for determining if a string is of the format
of Enstore metadata.  The number of Enstore imports should be kept to
a minimum.
"""
# system imports
import types
import re
import sys
import os
import string

# enstore modules
import charset

#######################################################################

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
        if is_volume_tape(volume):
            return 1
        elif is_volume_disk(volume):
            return 1
    return 0

def is_volume_tape(volume):
    if type(volume) == types.StringType:
        if re.search("^[A-Z0-9]{6}(.deleted){0,1}$", volume):
            return 1   #If passed a volume.
        elif re.search("^[A-Z0-9]{6}(L|M)[0-9]{1}(.deleted){0,1}$", volume):
            # LTO1,2 have L1 or L2 suffix
            return 1
        elif re.search("^[A-Z0-9]{6}(JC|JY|J)(.deleted){0,1}$", volume):
            # KIAE has 3592 tapes labeled as A00188JC. There also could be JY or just J
            return 1
    return 0

def is_volume_disk(volume):
    rc = 0
    if type(volume) == types.StringType:
        # volume name is
        # hostname:SG.FF.WRAPPER:YYYY-mm-ddTHH:MM:SSZ
        # or
        # hostname:SG.FF.WRAPPER:YYYY-mm-ddTHH:MM:SSZ.deleted
        # time format: ISO8601
        # Old naming
        if re.search("^[%s]+[:]{1}[%s]+[.]{1}[%s]+[.]{1}[%s]+[:]{1}[-TZ:0-9]+(.deleted){0,1}$"
                      % (charset.hostnamecharset, charset.charset,
                         charset.charset, charset.charset), volume):
            rc = 1   #If passed a disk volume.
        else:
            # New naming
            # hostname:SG.FF.WRAPPER
            # or
            # hostname:SG.FF.WRAPPER.deleted
            if re.search("^[%s]+[:]{1}[%s]+[.]{1}[%s]+[.]{1}[%s]+(.deleted){0,1}$"
                         % (charset.hostnamecharset, charset.charset,
                            charset.charset, charset.charset), volume):
                rc = 1   #If passed a disk volume.

    return rc

def is_location_cookie_tape(lc):
    if type(lc) == types.StringType:
        #For tapes and null volumes.
        tape_regex = re.compile("^[0-9]{4}(_)[0-9]{9}(_)[0-9]{7}$")

        if (len(lc) == 22 and \
            tape_regex.match(lc)):
            return 1

    return 0

#Alias this for completeness.
is_location_cookie_null = is_location_cookie_tape

def is_location_cookie_disk(lc):
    if type(lc) == types.StringType:
        #For new disk volumes.
        #  /vol1/scratch/000/0AF/0001000000000000000AF418
        # must start with "/" and be not less than 4 components in the path
        disk_regex = re.compile("^(?:/[/\w]+){4}")

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

from pnfs import is_pnfsid
try:
    #If chimera module is available, allow use of this function.
    from chimera import is_chimeraid
except ImportError:
    pass

def is_ip_addr(address):
    if type(address) == types.StringType:
        #For strings that are IP V4 addresses.
        ip_regex = re.compile("^[0-9]{1,3}(.)[0-9]{1,3}(.)[0-9]{1,3}(.)[0-9]{1,3}$")

        if ip_regex.match(address):
            return 1

    return 0

def extract_file_number(location_cookie):

    if is_location_cookie_tape(location_cookie):
        try:
            #Return just third integer portion of the string.
            return int(string.split(location_cookie, "_")[2])
        except (KeyboardInterrupt, SystemExit):
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except:
            return None

    return None


# generate file path from string of hexadecimal characters representing file ID
def file_id2path(root, file_id):
    # based on Alex suggestion
    # for even distrbution of files in directories
    # The file_id is presented by a string of hexadecimal characters
    # example:
    # 1. chimera ID
    # root = "/data_files"
    # file_id = "00001E9281CFB7054652B62737ED1ED3B3F6"
    # return value:
    # "/data_files/ee8/d3b/00001E9281CFB7054652B62737ED1ED3B3F6"
    # 2. pnfs ID
    # root = "/data_files"
    # file_id = "00020000000000001141B638"
    # return value:
    # " /data_files/629/41b/00020000000000001141B638"

    file_id_hex = int("0x"+file_id, 16)
    first = "%03x"%((file_id_hex & 0xFFF) ^ ( (file_id_hex >> 24) & 0xFFF),)
    second = "%03x"%((file_id_hex>>12) & 0xFFF,)
    path = os.path.join(root, first, second, file_id)

    return path


