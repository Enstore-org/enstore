######################################################################
#  $Id$
#
#  Python replacement for EMASS.so built on top of the aci module
#
#

import aci
from derrno import * ## This is safe because the module contains constants only
                     ## (DAS status codes)

import types

status_table = (
    ("ok",	"request successful"),			                #0
    ("BAD",	"rpc failure"),			       	                #1
    ("BAD",	"aci parameter invalid "),	       	                #2
    ("TAPE",	"volume not found of this type"),		                #3
    ("DRIVE",   "drive not in Grau ATL "),			        #4
    ("DRIVE",   "the requested drive is in use"),		                #5
    ("TAPE",	"the robot has a physical problem with the volume"),	#6
    ("BAD",	"an internal error in the AMU "),		                #7
    ("BAD",	"the DAS was unable to communicate with the AMU"),	#8
    ("BAD",	"the robotic system is not functioning"),	                #9
    ("BAD",	"the AMU was unable to communicate with the robot"),	#10
    ("BAD",	"the DAS system is not active "),		                #11
    ("DRIVE",   "the drive did not contain an unloaded volume"),	        #12
    ("BAD",	"invalid registration"),			         	#13
    ("BAD",	"invalid hostname or ip address"),		        #14
    ("BAD",	"the area name does not exist "),		                #15
    ("BAD",	"the client is not authorized to make this request"),	        #16
    ("BAD",	"the dynamic area became full, insertion stopped"),	        #17
    ("DRIVE",  "the drive is currently available to another client"),	        #18
    ("BAD",	"the client does not exist "),			        #19
    ("BAD",	"the dynamic area does not exist"),		        #20
    ("BAD",	"no request exists with this number"),	                #21
    ("BAD",	"retry attempts exceeded"),		                #22
    ("TAPE",	"requested volser is not mounted"),		        #23
    ("TAPE",	"requested volser is in use "),			        #24
    ("BAD",	"no space available to add range"),	        	#25
    ("BAD",	"the range or object was not found"),	        	#26
    ("BAD",	"the request was cancelled by aci_cancel()"),       	#27
    ("BAD",	"internal DAS error"),     				#28
    ("BAD",	"internal ACI error"),		        		#29
    ("BAD",	"for a query more data are available"),	        	#30
    ("BAD",	"things don't match together"),		        	#31
    ("TAPE",	"volser is still in another pool"),	        	        #32
    ("DRIVE",   "drive in cleaning"),		        		#33
    ("BAD",	"The aci request timed out"),		        	#34
    ("DRIVE",   "the robot has a problem with handling the device"),	#35
    )


def mount(volume, drive, media_type):
    """mount(vol, drive, media type)"""

    status = 0
    
    media_code = aci.__dict__.get("ACI_"+media_type)

    if media_code is None:
        status = ENOVOLUME
    elif aci.aci_mount(vol,media_code,drive):  #note order of args!
        status = aci.cvar.d_errno
        if status > len(status_table):  #invalid error code
            status = EDASINT
    
    return status_table[status][0], status, status_table[status][1]    
    

def dismount(volume, drive, media_type):
    """dismount(vol, drive, media type)"""

    status = 0
    if aci.aci_force(drive):
        status=aci.cvar.d_errno
        if status > len(status_table):
            stat= EDASINT

    return status_table[status][0], status, status_table[status][1]    

def home(robot):
    """home(robot)"""

    retval = "badhome"

    if not aci.aci_robhome(robot):
        retval = "badstart"
        if not aci.aci_robstat(robot,"START"):
            retval = "ok"

    return retval

