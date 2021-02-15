#!/usr/bin/env python
###############################################################################
#
# $Author$
# $Date$
# $Id$
#
# convert pnfs strings
#
#
###############################################################################
import string


def parse_id(txt):
    pnfsid_string = ""
    for i in [0, 4, 8, 12]:
        l = i + 2
        h = l + 2
        l1 = i
        h1 = l1 + 2
        pnfsid_string = pnfsid_string + txt[l:h] + txt[l1:h1]
    for i in [20, 16]:
        l = i + 2
        h = l + 2
        l1 = i
        h1 = l1 + 2
        pnfsid_string = pnfsid_string + txt[l:h] + txt[l1:h1]
    pnfsid_string = string.upper(pnfsid_string)
    return pnfsid_string


def inverse_parse_id(txt):
    return string.lower(parse_id(txt))
