######################################################################
# $Id$
#
# This module deals with argument passing to/from the aci_shadow module
#  All of the actual work is done in aci_shadow, here we are just
#  promoting the returned pointers into shadow class instances.
#  There has to be a better way!


import aci_shadow

# aci.py is a "subclass" of aci_shadow - we are just overriding a few functions
# So, pull in all of aci_shadow

from aci_shadow import *

def aci_clientstatus(clientname):
    x = aci_shadow.aci_clientstatus(clientname)
    if type(x)==type([]):
        return x[0], aci_shadow.aci_client_entry(x[1])
    else:
        return x, []
        

def aci_drivestatus(clientname):
    x = aci_shadow.aci_drivestatus(clientname)
    if type(x)==type([]):
        return x[0], map(aci_shadow.aci_drive_entry, x[1:])
    else:
        return x, []

def aci_drivestatus2(clientname):
    x = aci_shadow.aci_drivestatus2(clientname)
    if type(x)==type([]):
        return x[0], map(aci_shadow.aci_drive_entry, x[1:])
    else:
        return x, []

def aci_list(clientname):
    x = aci_shadow.aci_list(clientname)
    if type(x)==type([]):
        return x[0], map(aci_shadow.aci_req_entry, x[1:])
    else:
        return x, []

def aci_view(clientname, type):
    stat, ptr = aci_shadow.aci_view(clientname, type)
    return stat, aci_shadow.aci_vol_desc(ptr)
