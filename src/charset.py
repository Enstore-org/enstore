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

def is_in_charset(string):
    #print "charset",charset
    if not len(string): return 0
    for ch in string:
        if not ch in charset:
            break
    else:
        return 1
    return 0

def is_in_filenamecharset(string):
    #print "charset",charset
    if not len(string): return 0
    for ch in string:
        if not ch in filenamecharset:
            break
    else:
        return 1
    return 0
