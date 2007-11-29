#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

import string

#This is for pnfs tags.
charset=string.join((string.letters,string.digits,'-','_','/'),'')

#This is for filepaths.
filenamecharset=string.join((string.printable),'')

#This is for hostnames.
hostnamecharset=string.join((string.letters,string.digits,'.','-'),'')

def is_string_in_character_set(check_string, character_set):
    if not len(check_string): return 0
    for ch in check_string:
        if not ch in character_set:
            break
    else:
        return 1
    return 0

def is_in_charset(check_string):
    return is_string_in_character_set(check_string, charset)

def is_in_filenamecharset(check_string):
    return is_string_in_character_set(check_string, filenamecharset)

def is_in_hostnamecharset(check_string):
    return is_string_in_character_set(check_string, hostnamecharset)
