######################################################################
#  $Id$
#
#  Python replacement for aml2.so built on top of the aci module
#
#

#system imports
import types
import string
import time
import whrandom
import popen2
import string
import sys
import aml2_log

#enstore imports
import aci
import derrno
import volume_clerk_client
import Trace
import e_errors

status_table = (
    ("ok",	"request successful"),			                #0
    ("BAD",	"rpc failure"),			       	                #1
    ("BAD",	"aci parameter invalid "),	       	                #2
    ("TAPE",	"volume not found of this type"),		        #3
    ("DRIVE",   "drive not in Grau ATL "),			        #4
    ("DRIVE",   "the requested drive is in use"),		        #5
    ("TAPE",	"the robot has a physical problem with the volume"),	#6
    ("BAD",	"an internal error in the AMU "),		        #7
    ("BAD",	"the DAS was unable to communicate with the AMU"),	#8
    ("BAD",	"the robotic system is not functioning"),	        #9
    ("BAD",	"the AMU was unable to communicate with the robot"),	#10
    ("BAD",	"the DAS system is not active "),		        #11
    ("DRIVE",   "the drive did not contain an unloaded volume"),	#12
    ("BAD",	"invalid registration"),			      	#13
    ("BAD",	"invalid hostname or ip address"),		        #14
    ("BAD",	"the area name does not exist "),		        #15
    ("BAD",	"the client is not authorized to make this request"),   #16
    ("BAD",	"the dynamic area became full, insertion stopped"),	#17
    ("DRIVE",   "the drive is currently available to another client"),  #18
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
    ("TAPE",	"volser is still in another pool"),	                #32
    ("DRIVE",   "drive in cleaning"),		        		#33
    ("BAD",	"The aci request timed out"),		        	#34
    ("DRIVE",   "the robot has a problem with handling the device"),	#35
    ("notused", "not used"),                                            #36
    # non aci errors:
    ("ERROR",   "command error"),                                       #37
    ("ERROR",   "vcc.new_library error(s)"),                            #38
    )

ACI_DRIVE_UP = aci.ACI_DRIVE_UP
ACI_DRIVE_DOWN = aci.ACI_DRIVE_DOWN

ACI_3480 = aci.ACI_3480
ACI_OD_THICK = aci.ACI_OD_THICK
ACI_OD_THIN = aci.ACI_OD_THIN
ACI_DECDLT = aci.ACI_DECDLT
ACI_8MM = aci.ACI_8MM
ACI_4MM = aci.ACI_4MM
ACI_D2 = aci.ACI_D2
ACI_VHS = aci.ACI_VHS
ACI_3590 = aci.ACI_3590
ACI_CD = aci.ACI_CD
ACI_TRAVAN = aci.ACI_TRAVAN
ACI_DTF = aci.ACI_DTF
ACI_BETACAM = aci.ACI_BETACAM
ACI_AUDIO_TAPE = aci.ACI_AUDIO_TAPE
ACI_BETACAML = aci.ACI_BETACAML
ACI_SONY_AIT = aci.ACI_SONY_AIT
ACI_LTO = aci.ACI_LTO
ACI_DVCM = aci.ACI_DVCM
ACI_DVCL = aci.ACI_DVCL
ACI_NUMOF_MEDIA = aci.ACI_NUMOF_MEDIA
ACI_MEDIA_AUTO = aci.ACI_MEDIA_AUTO

media_names = {aci.ACI_3480 : '3480',
               aci.ACI_OD_THICK : 'OD_THICK',
               aci.ACI_OD_THIN : 'OD_THIN',
               aci.ACI_DECDLT : 'DECDLT',
               aci.ACI_8MM : '8MM',
               aci.ACI_4MM : '4MM',
               aci.ACI_D2 : 'D2',
               aci.ACI_VHS : 'VHS',
               aci.ACI_3590 : '3590',
               aci.ACI_CD : 'CD',
               aci.ACI_TRAVAN : 'TRAVAN',
               aci.ACI_DTF : 'DTF',
               aci.ACI_BETACAM : 'BETACAM',
               aci.ACI_AUDIO_TAPE : 'AUDIO_TAPE',
               aci.ACI_BETACAML : 'BETACAML',
               aci.ACI_SONY_AIT : 'SONY_AIT',
               aci.ACI_LTO : 'LTO',
               aci.ACI_DVCM : 'DVCM',
               aci.ACI_DVCL : 'DVCL',
}               

drive_state_names = { 1 : 'down',
                2 : 'up',
                3 : 'force_down',
                4 : 'force_up',
                5 : 'exclusive_up',
                }

drive_names = {'1' : "Colorado_T1000",
               '2' : "6380/7480",
               '3' : "6390/7490",
               '4' : "9840_Eagle",
               '5' : "BVW_75p",
               #There is no 6.
               '7' : "3480/3580",
               '8' : "3480",
               '9' : "5480/3590E/3580/3590/3480/3490",
               'A' : "ER90/DST_310/DVR_2100",
               #There is no B.
               'C' : "8205-8mm/7208_001/Mammoth/DC_MK_13",
               #There is no D.
               'E' : "DLT_2000/4000/7000",
               'F' : "HP_DD2-1/DDS-2",
               'G' : "DLT_7000/8000",
               'H' : "HP_1300",
               #There is no I.
               'J' : "3995_Juckbox",
               'K' : "4480",
               'L' : "4490_Silerstone/9490_Timberline",
               #There is no M.
               'N' : "3591/3590_Magstar/8590",
               'O' : "RF7010E/RF7010X",
               'P' : "OD 512",
               'Q' : "3480/3490",
               'R' : "3480/3490",
               'S' : "3480",
               'T' : "5180",
               'U' : "5190",
               'V' : "RSP_2150_Mountaingate",
               'W' : "CD-ROM",
               'X' : "AKEBONO",
               #There is no Y.
               'Z' : "M8100",
               }

def convert_status(int_status):
    status = int_status
    if status > len(status_table):  #invalid error code
        #status = derrno.EDASINT
        return 'BAD', status, 'UNKNOWN CODE'
    return status_table[status][0], status, status_table[status][1]    

def view(volume, media_type):
    media_code = aci.__dict__.get("ACI_"+media_type)
    if media_code is None:
        Trace.log(e_errors.ERROR, 'Media code is None. media_type=%s'%(media_type,))
        return (-1,None)

    #aci_view() the old way of doing this.  Using aci_qvolsrange() gets
    # us a little more functionality.  Using aci_view2() would get
    # even more than that, if the time existed...
    """
    stat,volstate = aci.aci_view(volume,media_code)
    if stat!=0:
        Trace.log(e_errors.ERROR, 'aci_view returned status=%d'%(stat,))
        return stat,None
    """
    
    start = volume
    end = volume
    stat, start, volsers = aci.aci_qvolsrange(start, end, 1, "")
    if volsers == None:
        Trace.log(e_errors.ERROR, 'volume %s %s NOT found'%(volume,media_type))
        return stat,None

    if len(volsers) != 1:
        Trace.log(e_errors.ERROR, 'volume %s %s NOT found'%(volume,media_type))
        return stat,None

    volstate = volsers[0]
    
    return stat, volstate

def list_volser():
    start = ""
    end = ""
    client = ""
    all_volsers = []
    stat = 1 #aci_qvolsrange() returns 0 when the last volser is retuned.
    while stat:
        stat, start, volsers = aci.aci_qvolsrange(start, end,
                                                  aci.ACI_MAX_QUERY_VOLSRANGE,
                                                  client)
        if volsers:
            all_volsers = all_volsers + volsers

        if stat != 0 and aci.cvar.d_errno != derrno.EMOREDATA:
            Trace.log(e_errors.ERROR,
                      'aci_qvolsrange returned status=%d' % (stat,))
            return stat,None

    return stat, all_volsers

def drive_state(drive,client=""):
    stat,drives = aci.aci_drivestatus2(client)
    if stat!=0:
        Trace.log(e_errors.ERROR, 'drivestatus2 returned status=%d'%(stat,))
        return stat,None
    for d in range(0,len(drives)):
        #print d,drives[d].drive_name, drive
        if drives[d].drive_name == "":
            break
        if drives[d].drive_name == drive:
            return stat,drives[d]
    Trace.log(e_errors.ERROR, 'drive %s NOT found'%(drive,))
    return stat,None

def drives_states():
    stat, drives = aci.aci_drivestatus2("")
    if stat!=0:
        Trace.log(e_errors.ERROR, 'drivestatus2 returned status=%d'%(stat,))
        return stat, None

    return stat, drives

def drive_volume(drive):
    stat,drive=drive_state(drive)
    if stat!=0:
        return None
    if drive!=None:
        return drive.volser

def mount(volume, drive, media_type,view_first=1):
    print 'mount called', volume, drive, media_type, view_first

    media_code = aci.__dict__.get("ACI_"+media_type)
    if media_code is None:
        return 'BAD',e_errors.MC_NONE,'Media code is None. media_type= %s'%(media_type,)
    
    # check if tape is in the storage location or somewhere else
    if view_first:
        stat,volstate = view(volume,media_type)
        if stat!=0:
            return 'BAD', e_errors.MC_FAILCHKVOL, 'aci_view return code=%d'%(stat,)
        if volstate == None:
            return 'BAD', e_errors.MC_VOLNOTFOUND, 'volume %s not found = %d'%(volume,stat)
        if volstate.attrib != "O": # look for tape in tower (occupied="O")
            return 'BAD',e_errors.MC_VOLNOTHOME,'Tape %s is not in home position in tower. location=%s'%(volume,volstate.attrib,)
        
    # check if any tape is mounted in this drive
        stat,drvstate = drive_state(drive,"")
        if stat!=0:
            return 'BAD', e_errors.MC_FAILCHKDRV, 'aci_drivestatus2 return code = %d' %(stat,)
        if drvstate == None:
            return 'BAD', e_errors.MC_DRVNOTFOUND, 'drive %s not found = %d '%(drive,stat)
        if drvstate.volser != "": # look for any tape mounted in this drive
            return 'BAD',e_errors.MC_DRVNOTEMPTY,'Drive %s is not empty. Found volume %s'%(drive,drvstate.volser)

    stat = aci.aci_mount(volume,media_code,drive)
    if stat==0:
        status = aci.cvar.d_errno
        if status > len(status_table):  #invalid error code
            return 'BAD', status, 'MOUNT UNKNOWN CODE %d'%(status,)
        return status_table[status][0], status, status_table[status][1]    
    else:
        return 'BAD',stat,'MOUNT COMMAND FAILED'

    
# this is a forced dismount. get rid of whatever has been ejected from the drive   
def dismount(volume, drive, media_type,view_first=1):
    print 'dismount called', volume, drive, media_type, view_first

    # check if any tape is mounted in this drive
    if view_first:
        stat,drvstate = drive_state(drive,"")
        if stat!=0:
            return 'BAD', e_errors.MC_FAILCHKDRV, 'aci_drivestatus2 return code = %d'%(stat,)
        if drvstate == None:
            return 'BAD', e_errors.MC_DRVNOTFOUND, 'drive %s not found = %d'%(drive,stat)
        if drvstate.volser == "": # look for any tape mounted in this drive
            if volume!="Unknown":
                return 'ok',0,'Drive %s is empty. Thought volume %s was there.'%(drive,volume)  #FIXME: mover calling with tape when there is none in drive. Return OK for now
            else: #don't know the volume on startup
                status=0
                return status_table[status][0], status, status_table[status][1]    

    stat = aci.aci_force(drive)
    if stat==0:
        status=aci.cvar.d_errno
        if status > len(status_table):
            return 'BAD', status, 'FORCE DISMOUNT UNKNOWN CODE'
        return status_table[status][0], status, status_table[status][1]    
    else:
        return 'BAD',stat,'FORCE DISMOUNT COMMAND FAILED'






# home robot arm
def robotHome(arm):
    status = aci.aci_robhome(arm)
    if not status:
        status = aci.aci_robstat(arm,"start")
    return status_table[status][0], status, status_table[status][1]

# get status of robot
def robotStatus(arm):
    #status = aci.aci_robstat("\0","stat")
    status = aci.aci_robstat(arm, "stat")
    return status_table[status][0], status, status_table[status][1]

#start robot arm
def robotStart(arm):
    status = aci.aci_robstat(arm,"start")
    return status_table[status][0], status, status_table[status][1]

# home and start robot arm
def robotHomeAndRestart(ticket, classTicket):
    arm = ticket["robotArm"]
    status = 37
    if arm=='R1' or arm=='Both':
        st1,status,st2 = robotHome('R1')
        if not status:
            st1,status,st2 = robotStart('R1')
	    if status:
                Trace.trace(e_errors.INFO, 'aml2 robotHomeAndRestart Start failed on R1')
                return status_table[status][0], status, status_table[status][1]
	else:
            Trace.trace(e_errors.INFO, 'aml2 robotHomeAndRestart Home failed on R1')
            return status_table[status][0], status, status_table[status][1]
    if arm=='R2' or arm=='Both':
        st1,status,st2 = robotHome('R2')
        if not status:
            st1,status,st2 = robotStart('R2')
	    if status:
                Trace.trace(e_errors.INFO, 'aml2 robotHomeAndRestart Start failed on R2')
                return status_table[status][0], status, status_table[status][1]
	else:
            Trace.trace(e_errors.INFO, 'aml2 robotHomeAndRestart Home failed on R2')
            return status_table[status][0], status, status_table[status][1]
    return status_table[status][0], status, status_table[status][1]

	   
#sift through a list of lists
def yankList(listOfLists, listPosition, look4String):
    newRecordList = []
    for record in listOfLists:
	if string.find(record[listPosition],look4String)>-1:
	    newRecordList.append(record)
    return newRecordList    
    
def insert(ticket, classTicket):
    if classTicket.has_key("mcSelf"):
        mcSelf = classTicket["mcSelf"]
    else:
        status = 37
        mcSelf.workQueueClosed = 0
        Trace.trace(e_errors.ERROR, 'aml2 no mcSelf field found in ticket.')
        return status_table[status][0], status, 'aml2 no mcSelf field found in ticket.'
    
    mediaAssgn = ticket['medIOassign']
    Iareas = []
    for media in mediaAssgn.keys():
        for box in mediaAssgn[media]:
            Iareas.append("I"+box[1:])

    status = 0
    if ticket.has_key("IOarea_name") and len(ticket["IOarea_name"])>0:
        IOarea_input = ticket["IOarea_name"]
	for IOarea_name in IOarea_input:
	    if IOarea_name not in Iareas:
	        status = derrno.EINVALID
                mcSelf.workQueueClosed = 0
                Trace.trace(e_errors.ERROR, 'aml2 bad IOarea parameter specified.')
	        return status_table[status][0], status, status_table[status][1]
        areaList = IOarea_input
    else:
        areaList = list(Iareas)

    timeL1 = time.time()
    timeCmd = ticket["timeOfCmd"]
    time.sleep(90)  # let robot-IOarea door open and start inventory
                    # helps for clock skew between adic2 and mc host

    robot_host = "adic2.fnal.gov"
    year, month, day, hour, minute = time.localtime(timeL1)[:5]
    outfileName = "/tmp/aml2Log%02d%02d" % (day, month)

    mcSelf.workQueueClosed = 0

    while timeL1-timeCmd>1200:   # timeout in seconds
        #get amulog from adic2
        Trace.trace(e_errors.INFO, 'aml2 fetching log...')
        aml2_log.fetch_log_file(robot_host, month, day, outfileName)

	if ticket.has_key('FakeIOOpen'):
	    time.sleep(5)
            Trace.trace(e_errors.INFO, 'aml2: Fake IO Door Open')
	    break

        # examine the log - get INVT records
        ofile = open(outfileName,'r')
        recordCount = aml2_log.n_records(outfileName)
        Irecord = []
        for recordPointer in range(recordCount-1,0,-1):
            record = aml2_log.get_record(ofile, recordPointer)
	    if string.find(record[3],"INVT")>-1:
	        if timeCmd<record[0]:  #comment out 4dbug
	            Irecord.append(record)
        ofile.close()

        if len(Irecord) == 0:
            status = 37
            mcSelf.workQueueClosed = 0
            Trace.trace(e_errors.ERROR, 'aml2 no INVT records found.')
            return status_table[status][0], status, 'aml2 no INVT records found.'

	ESrecord = yankList(Irecord, 3, "INVT of E")
	EFrecord = []
	for record in ESrecord:
	    look4 = record[3][5:9]+" "+record[3][:4]
	    EFrecord = EFrecord+yankList(Irecord, 3, look4)
	if len(ESrecord) == len(EFrecord):
	    break
	timeL1 = time.time()

    # do insert command...
    bigVolList = []
    for area in areaList:
	Trace.trace(e_errors.INFO, 'aml2 InsertVol IOarea: %s' % (area,))
	result = aci.aci_insert(area)   # aci insert command
	res = result[0]
	media_code = result[-1]
	volser_ranges = result[1:-1]
	###XXX this is a little ugly... but it works.
	### we could fix up aci_typemaps to return the volser_ranges in
	### a sublist, but life is short
        Trace.trace(e_errors.INFO, 'aml2 aci_insert: %i %i' % (res,media_code))
        if res:
            status = aci.cvar.d_errno
            if status > len(status_table):  #invalid error code
                status = derrno.EDASINT
            Trace.trace(e_errors.ERROR, 'aml2 insert failed %s' % (status_table[status][1],))
	for strList in volser_ranges:
	    pieces = string.split(strList,', ')
	    for vol_label in pieces:
	        if len(vol_label)>0:
		    info = vol_label, res
	            bigVolList.append(info)

    # set library name to ticket["newlib"]
    vcc = volume_clerk_client.VolumeClerkClient(mcSelf.csc)
    for info in bigVolList:
        if not info[1]:
            ret = vcc.new_library(info[0],ticket["newlib"])
	    if ret['status'][0] != 'ok':
                Trace.log(e_errors.ERROR, 'aml2 NewLib-InsertVol failed %s' % (ret,))
                status = 38
	    else:
                Trace.trace(e_errors.INFO, 'aml2 NewLib-InsertVol sucessful %s' % (vol_label,))

    return status_table[status][0], status, status_table[status][1]

def eject(ticket, classTicket):

    if classTicket.has_key("mcSelf"):
        mcSelf = classTicket["mcSelf"]
    else:
        status = 37
        mcSelf.workQueueClosed = 0
        Trace.trace(e_errors.ERROR, 'aml2 no mcSelf field found in ticket.')
        return status_table[status][0], status, 'aml2 no mcSelf field found in ticket.'

    mediaAssgn = ticket['medIOassign']
    status = 0
    if ticket.has_key("media_type"):
        media_type = ticket["media_type"]
        media_code = aci.__dict__.get("ACI_"+media_type)
    else:
	status = derrno.EINVALID
	return status_table[status][0], status, status_table[status][1]
    if media_code is None:
        status = derrno.ENOVOLUME
        return status_table[status][0], status, status_table[status][1]
	
    if ticket.has_key("IOarea_name"):
        IOarea_name = ticket["IOarea_name"]
	if IOarea_name not in mediaAssgn["ACI_"+media_type]:
	    status = derrno.EINVALID
	    return status_table[status][0], status, status_table[status][1]
    else:
        box = whrandom.randint(0,len(mediaAssgn["ACI_"+media_type])-1)
        IOarea_name = mediaAssgn["ACI_"+media_type][box]
    
    if ticket.has_key("volList"):
        volumeList = ticket['volList']
    else:
	status = derrno.EINVALID
	return status_table[status][0], status, status_table[status][1]
    Trace.trace(e_errors.INFO, 'aml2 aci_eject: %s %i' % (IOarea_name,media_code))

    for volser_range in volumeList:
        Trace.trace(e_errors.INFO, 'aml2 aci_eject: %s' % (volser_range))
        res = aci.aci_eject(IOarea_name, volser_range, media_code)
        if res:
            Trace.trace(e_errors.ERROR, 'aml2 aci_eject: %i' % (res,))
            status = aci.cvar.d_errno
            if status > len(status_table):  #invalid error code
                status = derrno.EDASINT
            return status_table[status][0], status, status_table[status][1]
    
    return status_table[status][0], status, status_table[status][1]

def list_slots():
    total = aci.ACI_MS_ALL
    free = aci.ACI_MS_MKE | aci.ACI_MS_EJT #same as aci.ACI_MS_EMPTY
    used = aci.ACI_MS_OCC | aci.ACI_MS_MNT
    disabled = aci.ACI_MS_UNDEF
    #The only way (free + used != total) is if there are undefined
    # (aci.ACI_MS_UNDEF) slots in the robot.
    media_list = []
    for tower in range(6):
        device = "ST%02d" % tower
        stat, media_info_totals = aci.aci_getcellinfo(device, 0, total)
        if stat!=0:
            Trace.log(e_errors.ERROR,
                      'aci_getcellinfo returned status=%d' % (stat,))
            return stat,None
        if len(media_info_totals) == 0:
            continue
        
        stat, media_info_free = aci.aci_getcellinfo(device, 0, free)
        if stat!=0:
            Trace.log(e_errors.ERROR,
                      'aci_getcellinfo returned status=%d' % (stat,))
            return stat,None

        stat, media_info_used = aci.aci_getcellinfo(device, 0, used)
        if stat!=0:
            Trace.log(e_errors.ERROR,
                      'aci_getcellinfo returned status=%d' % (stat,))
            return stat,None

        stat, media_info_disabled = aci.aci_getcellinfo(device, 0, disabled)
        if stat!=0:
            Trace.log(e_errors.ERROR,
                      'aci_getcellinfo returned status=%d' % (stat,))
            return stat,None

        for item in range(len(media_info_totals)):
            media_list.append((device, media_info_totals, media_info_free,
                               media_info_used, media_info_disabled))
               
    return stat, media_list
