######################################################################
# $Id$
#
# This module deals with argument passing to/from the aci_shadow module
#  All of the actual work is done in aci_shadow, here we are allocating space for
#  return arguments, and causing return values to be python objects rather than C pointers
#

import aci_shadow

# aci.py is a "subclass" of aci_shadow - we are just overriding a few functions
# So, pull in all of aci_shadow

from aci_shadow import *

def aci_clientstatus(clientname):
    client_entry = aci_client_entry()
    status = aci_shadow.aci_clientstatus(clientname,client_entry)
    return status, client_entry

def aci_drivestatus(clientname="NULL"):
    drive_entries = []
    for x in range(ACI_MAX_DRIVE_ENTRIES):
        drive_entries.append(aci_drive_entry())
    drive_entry_ptrs = map(lambda x:x.this, drive_entries)

    status = aci_shadow.aci_drivestatus(clientname, drive_entry_ptrs)
    return status, drive_entries


def aci_list(clientname):
    #XXX the sample code on page 3-15 of the ACI docs is wrong - where is the actual
    #storage for the req_entries getting allocated ?

    req_entries = []
    for x in range(ACI_MAX_REQ_ENTRIES):
        req_entries.append(aci_req_entry())
    req_entry_ptrs = map(lambda x:x.this, req_entries)

    status = aci_shadow.aci_list(clientname, req_entry_ptrs)
    return status, req_entries

def aci_view(clientname, type):
    vol_desc = aci_shadow.aci_vol_desc()
    status = aci_shadow.aci_view(clientname, type, vol_desc)
    return status, vol_desc


