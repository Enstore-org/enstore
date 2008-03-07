#!/usr/bin/env python

###############################################################################
#
# $Id$
#
# This file contains function for determining if a string is of the format
# of Enstore metadata.  The number of Enstore imports should be kept to
# a minimum.
#
###############################################################################

# system imports
import types
import re
import sys
import string

# enstore modules
import charset


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
        if is_volume_tape(volume):
            return 1
        elif is_volume_disk(volume):
            return 1
        
        """
        if re.search("^[A-Z0-9]{6}$", volume):
            return 1   #If passed a volume.
        elif re.search("^[A-Z0-9]{6}(L)[0-9]{1}$", volume):
            return 1   #If passed a volume.
        elif re.search("^[%s]+[:]{1}[%s]+[.]{1}[%s]+[.]{1}[%s]+[:]{1}[0-9]+$"
                      % (charset.hostnamecharset, charset.charset,
                         charset.charset, charset.charset), volume):
            return 1   #If passed a disk volume.
        """
        
    return 0

def is_volume_tape(volume):
    if type(volume) == types.StringType:
        if re.search("^[A-Z0-9]{6}$", volume):
            return 1   #If passed a volume.
        elif re.search("^[A-Z0-9]{6}(L)[0-9]{1}$", volume):
            return 1   #If passed a volume.

    return 0

def is_volume_disk(volume):
    if type(volume) == types.StringType:
        if re.search("^[%s]+[:]{1}[%s]+[.]{1}[%s]+[.]{1}[%s]+[:]{1}[0-9]+$"
                      % (charset.hostnamecharset, charset.charset,
                         charset.charset, charset.charset), volume):
            return 1   #If passed a disk volume.

    return 0

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

    if is_bfid(bfid):
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

    if is_location_cookie_tape(location_cookie):
        try:
            #Return just third integer portion of the string.
            return int(string.split(location_cookie, "_")[2])
        except (KeyboardInterrupt, SystemExit):
            raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
        except:
            return None
        
    return None

