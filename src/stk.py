#!/usr/bin/env python

# $Id$

import os
import string
import sys


import Trace
import e_errors

OK    = e_errors.OK
DRIVE = "DRIVE"
TAPE  = "TAPE"
ERROR = "ERROR"

DEBUG = 0

RSH = "/usr/bin/rsh fntt -l acsss \"echo"
CMD_PROC="|/export/home/ACSSS/bin/cmd_proc 2>&1\" </dev/null"


# execute a shell command, but don't wait forever for it to complete
# the command has to return some output on stdout for this to work correctly
# I could not get popen3 function working - this would give me access to stderr
def timed_command(command, time=60, retries=3 ):
    bufsize=5000
    mode='t'
    # do not mess with the redirection in the next line trying to parse stderr or stdout
    # the way it is, allows you to get the command output, timeout after N seconds, and not
    # get garbage from the specious kill.
    cmd = "/bin/sh -c '( sleep %i ; kill $$ ) </dev/null >/dev/null 2>&1 & %s'" % (time,command) #'
    response = []
    tries = 0
    while response == [] and tries < retries:
        tries = tries + 1
        if tries>1 or DEBUG:
            Trace.log(e_errors.INFO,'Timed command retry %i of %i: %s' % (tries,retries,cmd))
        response = os.popen(cmd,'r').readlines()
    return response

def query(volume, media_type="", seqNo=0):

    # build the command, and what to look for in the response
    command = "query vol %s" % (volume,)
    cmd="%s %s %s" % (RSH,command,CMD_PROC)
    cmd_lookfor = "ACSSA> %s" % (command,)
    answer_lookfor = "%s " % (volume,)

    # execute the command and read the response
    response = timed_command(cmd,10,3)
    size = len(response)

    if DEBUG:
        import pprint
        pprint.pprint(response)

    # check for really bad errors
    if size <= 19:
        Trace.log(e_errors.ERROR, "QUERY 1: %s %s" % (command+" => ",response))
        return (ERROR, 1, response, '', command)
    elif string.find(response[19], cmd_lookfor, 0) != 0:
        Trace.log(e_errors.ERROR, "QUERY 2: %s %s" % (command+" => ",response))
        return (ERROR, 2,  response, '', command)
    if size <= 20:
        Trace.log(e_errors.ERROR, "QUERY 3: %s %s" % (command+" => ",response))
        return (ERROR, 3, response,'',command)

    # got something - if response is too small, got an "error code" back
    if size <= 22:
        answer = string.strip(response[20])
        Trace.log(e_errors.INFO, "QUERY 4: %s %s" % (command+" => ",answer))
        return (TAPE, 4, answer, '', command)

    # got response, parse it and put it into the standard form
    answer = string.strip(response[22])
    if string.find(answer, answer_lookfor,0) != 0:
        Trace.log(e_errors.ERROR, "QUERY 5: %s %s" % (command+" => ",answer))
        return (ERROR, 5, answer, '', command)
    elif string.find(answer,' home ') != -1:
        Trace.log(e_errors.INFO, "%s %s" % (command+" => ",answer))
        return (OK,0,answer, 'O', command) # occupied
    elif string.find(answer,' in drive ') != -1:
        Trace.log(e_errors.INFO, "%s %s" % (command+" => ",answer))
        return (OK,0,answer, 'M', command) # mounted
    elif string.find(answer,' in transit ') != -1:
        Trace.log(e_errors.INFO, "%s %s" % (command+" => ",answer))
        return (OK,0,answer, 'T', command) # transit
    else:
        Trace.log(e_errors.ERROR, "QUERY 6: %s %s" % (command+" => ",answer))
        return (TAPE, 6, answer, '', command)

def query_drive(drive):

    # build the command, and what to look for in the response
    command = "query drive %s" % (drive,)
    cmd="%s %s %s" % (RSH,command,CMD_PROC)
    cmd_lookfor = "ACSSA> %s" % (command,)
    answer_lookfor = "%s " % (drive,)

    # execute the command and read the response
    # FIXME - what if this hangs?
    response = timed_command(cmd,10,3)
    size = len(response)

    if DEBUG:
        import pprint
        pprint.pprint(response)

    # check for really bad errors
    if size <= 19:
        Trace.log(e_errors.ERROR, "QUERY_DRIVE 1: %s %s" % (command+" => ",response))
        return (ERROR, 1, response, '', command)
    elif string.find(response[19], cmd_lookfor, 0) != 0:
        Trace.log(e_errors.ERROR, "QUERY_DRIVE 2: %s %s" % (command+" => ",response))
        return (ERROR, 2,  response, '', command)
    if size <= 20:
        Trace.log(e_errors.ERROR, "QUERY_DRIVE 3: %s %s" % (command+" => ",response))
        return (ERROR, 3, response,'',command)

    # got something - if response is too small, got an "error code" back
    if size <= 22:
        answer = string.strip(response[20])
        Trace.log(e_errors.INFO, "QUERY_DRIVE 4: %s %s" % (command+" => ",answer))
        return (TAPE, 4, answer, '', command)

    # got response, parse it and put it into the standard form
    answer = string.strip(response[22])
    answer = string.replace(answer,', ',',') # easier to part drive id
    if string.find(answer, answer_lookfor,0) != 0:
        Trace.log(e_errors.ERROR, "QUERY_DRIVE 5: %s %s" % (command+" => ",answer))
        return (ERROR, 5, answer, '', command)
    elif string.find(answer,' online ') == -1:
        Trace.log(e_errors.ERROR, "%s %s" % (command+" => ",answer))
        return (ERROR,0,answer, 'O', command) # not online
    elif string.find(answer,' available ') != -1:
        Trace.log(e_errors.INFO, "%s %s" % (command+" => ",answer))
        return (OK,0,answer, '', command) # empty
    elif string.find(answer,' in use ') != -1:
        loc = string.find(answer,' in use ')
        volume = string.split(answer[loc+8:])[0]
        Trace.log(e_errors.INFO, "%s %s" % (command+" => ",answer))
        return (OK,0,answer, volume, command) # mounted and in use
    else:
        Trace.log(e_errors.ERROR, "QUERY_DRIVE DRIVE 6: %s %s" % (command+" => ",answer))
        return (TAPE, 6, answer, '', command)

def mount(volume, drive, media_type="",view_first=1):

    # build the command, and what to look for in the response
    command = "mount %s %s" % (volume,drive)
    cmd="%s %s %s" % (RSH,command,CMD_PROC)
    cmd_lookfor = "ACSSA> %s" % (command,)
    answer_lookfor = "Mount: %s mounted on " % (volume,)

    # check if tape is in the storage location or somewhere else
    if view_first:
        status,stat,response,attrib,com_sent = query(volume, media_type)

        if stat!=0:
            Trace.log(e_errors.ERROR,'MOUNT 1 %s => %s' % (command,response))
            return status, stat, response
        if attrib != "O": # look for tape in tower (occupied="O")
            Trace.log(e_errors.ERROR,'MOUNT 2 %s => Tape %s is not in home position =>  %s' % (command, volume,response,) )
            return 'BAD',9999,'%s => Tape %s is not in home position =>  %s'%(command,volume,response,)

    # check if any tape is mounted in this drive
        status,stat,response,volser,com_sent = query_drive(drive)
        if stat!=0:
            Trace.log(e_errors.ERROR,'MOUNT 3 %s => %s' % (command,response))
            return status, stat, response
        if volser != "": # look for any tape mounted in this drive
            Trace.log(e_errors.ERROR,'MOUNT 4 %s => Drive %s is not empty =>  %s' % (command, drive,response) )
            return 'BAD',9998,'%s => Drive %s is not empty:  %s'%(command, drive,response)

    # execute the command and read the response
    response = timed_command(cmd,60*15,3)
    size = len(response)

    if DEBUG:
        import pprint
        pprint.pprint(response)

    # check for really bad errors
    if size <= 19:
        Trace.log(e_errors.ERROR, "MOUNT 5: %s %s" % (command+" => ",response))
        return (ERROR, 1, "%s %s" % (command+" => ",response))
    elif string.find(response[19], cmd_lookfor, 0) != 0:
        Trace.log(e_errors.ERROR, "MOUNT 6: %s %s" % (command+" => ",response))
        return (ERROR, 2,  "%s %s" % (command+" => ",response))
    if size <= 20:
        Trace.log(e_errors.ERROR, "MOUNT 7: %s %s" % (command+" => ",response))
        return (ERROR, 3, "%s %s" % (command+" => ",response))

    # got response, parse it and put it into the standard form
    answer = string.strip(response[20])
    if string.find(answer, answer_lookfor,0) != 0:
        Trace.log(e_errors.ERROR, "MOUNT 8: %s %s" % (command+" => ",answer))
        return (ERROR, 5, "%s %s" % (command+" => ",answer))
    Trace.log(e_errors.INFO, "%s %s" % (command+" => ",answer))
    return (OK, 0, "%s %s" % (command+" => ",answer))


def dismount(volume, drive, media_type="",view_first=1):

    # build the command, and what to look for in the response
    command = "dismount VOLUME %s force" % (drive,)
    cmd="%s %s %s" % (RSH,command,CMD_PROC)
    cmd_lookfor = "ACSSA> %s" % (command,)
    answer_lookfor = "Dismount: Forced dismount of "

    # check if any tape is mounted in this drive
    if view_first:
        status,stat,response,volser,com_sent = query_drive(drive)
        if stat!=0:
            Trace.log(e_errors.ERROR,'ERROR 1 %s => %s' % (command,response))
            return status, stat, response
        if volser == "": # look for any tape mounted in this drive
            if volume!="Unknown":
                Trace.log(e_errors.ERROR,'IGNORED DISMOUNT 1 %s => Drive %s is empty. Thought %s was there =>  %s' % (command,drive,volume,response) )
                #return 'BAD',9998,'%s => Drive %s is empty. Thought %s was there => %s' % (command,drive,volume,response)
                #FIXME: mover calling with tape when there is none in drive. Return OK for now
                return      OK,         0,'%s => Drive %s is empty. Thought %s was there =>  %s' % (command,drive,volume,response)
            else: #don't know the volume on startup
                Trace.log(e_errors.ERROR,'IGNORED DISMOUNT 2 %s => %s' % (command,response) )
                return OK, 0,'%s => %s' % (command,response)

    # execute the command and read the response
    response = timed_command(cmd,60*15,3)
    size = len(response)

    if DEBUG:
        import pprint
        pprint.pprint(response)

    # check for really bad errors
    if size <= 19:
        Trace.log(e_errors.ERROR, "DISMOUNT 3: %s %s" % (command+" => ",response))
        return (ERROR, 3, "%s %s" % (command+" => ",response))
    elif string.find(response[19], cmd_lookfor, 0) != 0:
        Trace.log(e_errors.ERROR, "DISMOUNT 4: %s %s" % (command+" => ",response))
        return (ERROR, 4,  "%s %s" % (command+" => ",response))
    if size <= 20:
        Trace.log(e_errors.ERROR, "DISMOUNT 5: %s %s" % (command+" => ",response))
        return (ERROR, 5, "%s %s" % (command+" => ",response))

    # got response, parse it and put it into the standard form
    answer = string.strip(response[20])
    if string.find(answer, answer_lookfor,0) != 0:
        Trace.log(e_errors.ERROR, "DISMOUNT 6: %s %s" % (command+" => ",answer))
        return (ERROR, 6, "%s %s" % (command+" => ",answer))
    Trace.log(e_errors.INFO, "%s %s" % (command+" => ",answer))
    return (OK, 0, "%s %s" % (command+" => ",answer))


if __name__ == "__main__" :

    def usage():
        print "stk.py query drive  <drive>"
        print "stk.py query volume <volume>"
        print "stk.py mount <volume> <drive>"
        print "stk.py dismount <drive>"


    # silly little parsing

    if len(sys.argv) <= 3 :
        usage()
        sys.exit(1)

    if sys.argv[1] in ("query","q"):
        if sys.argv[2] in ("volume","vol",'v'):
            print query(sys.argv[3])
        elif sys.argv[2] in ("drive","d"):
            print query_drive(sys.argv[3])
    elif sys.argv[1] in ("mount","m"):
        print mount(sys.argv[2],sys.argv[3],"")
    elif sys.argv[1] in ("dismount","d"):
        print dismount(sys.argv[2],sys.argv[3],"")
    else:
        print usage()
        sys.exit(1)
