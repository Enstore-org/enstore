######################################################################
# $Id$
#
# This module deals with argument passing to/from the aci_shadow module
#  All of the actual work is done in aci_shadow, here we are just
#  promoting the returned pointers into shadow class instances.
#  There has to be a better way!

import string

import aci_shadow

# aci.py is a "subclass" of aci_shadow - we are just overriding a few functions
# So, pull in all of aci_shadow

from aci_shadow import *

def aci_clientstatus(clientname):
    x = aci_shadow.aci_clientstatus(clientname)
    if type(x)==type([]):
        if type(x[1]) == type(""):
            #SWIG 1.1
            return x[0], aci_shadow.aci_client_entry(x[1])
        else:
            #SWIG 1.3
            return x[0], x[1:]
    else:
        return x, []
        

def aci_drivestatus(clientname):
    x = aci_shadow.aci_drivestatus(clientname)
    if type(x)==type([]):
        if type(x[1]) == type(""):
            #SWIG 1.1
            return x[0], map(aci_shadow.aci_drive_entry, x[1:])
        else:
            #SWIG 1.3
            return x[0], x[1:]
    else:
        return x, []

def aci_drivestatus2(clientname):
    x = aci_shadow.aci_drivestatus2(clientname)
    if type(x)==type([]):
        if type(x[1]) == type(""):
            #SWIG 1.1
            return x[0], map(aci_shadow.aci_drive_entry, x[1:])
        else:
            #SWIG 1.3
            return x[0], x[1:]
    else:
        return x, []

def aci_drivestatus3(clientname):
    x = aci_shadow.aci_drivestatus3(clientname)
    if type(x)==type([]):
        if type(x[1]) == type(""):
            #SWIG 1.1
            return x[0], map(aci_shadow.aci_ext_drive_entry, x[1:])
        else:
            #SWIG 1.3
            return x[0], x[1:]
    else:
        return x, []

def aci_drivestatus4(clientname, drivename):
    x = aci_shadow.aci_drivestatus4(clientname, drivename)
    if type(x)==type([]):
        if type(x[1]) == type(""):
            #SWIG 1.1
            return x[0], map(aci_shadow.aci_ext_drive_entry4, x[1:])
        else:
            #SWIG 1.3
            return x[0], x[1:]
    else:
        return x, []

def aci_list(clientname):
    x = aci_shadow.aci_list(clientname)
    if type(x)==type([]):
        if type(x[1]) == type(""):
            #SWIG 1.1
            return x[0], map(aci_shadow.aci_req_entry, x[1:])
        else:
            #SWIG 1.3
            return x[0], x[1:]
    else:
        return x, []

def aci_view(clientname, type):
    stat, ptr = aci_shadow.aci_view(clientname, type)
    return stat, aci_shadow.aci_vol_desc(ptr)

def aci_qvolsrange(start, end, max_count, clientname):
    x = aci_shadow.aci_qvolsrange(start, end, max_count, clientname)
    if type(x)==type([]):
        #x[0] should be the exit status.
        #x[1] should be the "next volume"
        #x[2:] will be a list of files.
        
        #If we did not get a list at the end, just return the error.
        if len(x) == 2:
            return x[0], x[1], []
        if type(x[2]) == type(""):
            #SWIG 1.1
            return x[0], x[1], map(aci_shadow.aci_volserinfo, x[2:])
        else:
            #SWIG 1.3
            return x[0], x[1], x[2:]
    else:
        return x, "",  []

def aci_getcellinfo(device, media_type, attrib):
    x = aci_shadow.aci_getcellinfo(device, media_type, attrib)
    if type(x)==type([]):
        if type(x[1]) == type(""):
            #SWIG 1.1
            return x[0], map(aci_shadow.aci_media_info, x[1:])
        else:
            #SWIG 1.3
            return x[0], x[1:]
    else:
        return x, []

def aci_insert(io_area):
    x = aci_shadow.aci_insert(io_area)
    if type(x)==type([]):
        if type(x[1]) == type(""):
            #SWIG 1.1
            volser_list = x[1].split(",")  #The SWIG 1.1 code here is broken.
            if volser_list[-1] in (" ", ""): #Skip last volser if empty.
                volser_list = volser_list[:-1]
            return x[0], map(string.strip, volser_list)
        else:
            #SWIG 1.3
            volser_list = x[1].split(",")
            if volser_list[-1] in (" ", ""): #Skip last volser if empty.
                volser_list = volser_list[:-1]
            return x[0], map(string.strip, volser_list)
    else:
        return x, []
