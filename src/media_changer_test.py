#!/usr/bin/env python

# src/$RCSfile$   $Revision$
#
#########################################################################
#                                                                       #
# Media Changer server.                                                 #
# Media Changer is an abstract object representing a physical device or #
# operator who performs mounts / dismounts of tapes on tape drives.     #
# At startup the media changer process takes its argument from the      #
# command line and cofigures based on the dictionary entry in the       #
# Configuration Server for this type of Media Changer.                  #
# It accepts then requests from its clients and performs tape mounts    #
# and dismounts                                                         #
#                                                                       #
#########################################################################

# system imports
import os
import sys
import types
import time
import popen2
import signal
import string
import socket
import hostaddr
import struct
import fcntl
if sys.version_info < (2, 2, 0):
    import FCNTL #FCNTL is depricated in python 2.2 and later.
    fcntl.F_GETFL = FCNTL.F_SETLKW
    fcntl.F_SETFL = FCNTL.F_WRLCK
    fcntl.F_SETFL = FCNTL.F_RDLCK
    fcntl.F_SETFL = FCNTL.F_UNLCK
import pprint
import re

# enstore imports
import configuration_client
import dispatching_worker
import generic_server
import monitored_server
import enstore_constants
import option
import Trace
import traceback
import e_errors
import volume_clerk_client
import timeofday
import enstore_functions2

def _lock(f, op):
        dummy = fcntl.fcntl(f.fileno(), fcntl.F_SETLKW,
                            struct.pack('2h8l', op,
                                        0, 0, 0, 0, 0, 0, 0, 0, 0))
        Trace.trace(21,'_lock '+repr(dummy))

def writelock(f):
        _lock(f, fcntl.F_WRLCK)

def readlock(f):
        _lock(f, fcntl.F_RDLCK)

def unlock(f):
        _lock(f, fcntl.F_UNLCK)




# media loader template class
class MediaLoaderMethods(dispatching_worker.DispatchingWorker,
                         generic_server.GenericServer):

    work_list = []
    work_cleaning_list = []

    def return_max_work(self):
	return self.max_work

    def __init__(self, medch, max_work, csc):
        self.logdetail = 1
        self.name = medch
        self.name_ext = "MC"
        generic_server.GenericServer.__init__(self, csc, medch)
        Trace.init(self.log_name)
        self.max_work = max_work
        self.workQueueClosed = 0
        self.insertRA = None
        #   pretend that we are the test system
        #   remember, in a system, there is only one bfs
        #   get our port and host from the name server
        #   exit if the host is not this machine
        self.mc_config = self.csc.get(medch)
        self.acls_host  =  self.mc_config.get('acls_host', 'UNKNOWN')
	self.acls_uname =  self.mc_config.get('acls_uname','UNKNOWN')

        self.alive_interval = monitored_server.get_alive_interval(self.csc, medch, self.mc_config)
        dispatching_worker.DispatchingWorker.__init__(self,
                                                      (self.mc_config['hostip'], self.mc_config['port']))
        self.idleTimeLimit = 600  # default idle time in seconds
        self.lastWorkTime = time.time()
        self.robotNotAtHome = 1
        self.set_this_error = e_errors.OK  # this is useful for testing mover errors

        self.timeInsert = time.time()
        ##start our heartbeat to the event relay process
        self.erc.start_heartbeat(self.name, self.alive_interval, self.return_max_work)

    # retry function call
    def retry_function(self,function,*args):
        return apply(function,args)


    # wrapper method for client - server communication
    def loadvol(self, ticket):
        ticket["function"] = "mount"
        Trace.trace(11, "loadvol %s"%(ticket,))
        return self.DoWork( self.load, ticket)

    # wrapper method for client - server communication
    def unloadvol(self, ticket):
        ticket["function"] = "dismount"
        return self.DoWork( self.unload, ticket)

    # wrapper method for client - server communication
    #def viewvol(self, ticket):
    #    ticket["status"] = self.view(ticket["vol_ticket"]["external_label"], \
    #              ticket["vol_ticket"]["media_type"])
    #    self.reply_to_caller(ticket)

    # wrapper method for client - server communication - replaced viewvol above tgj1
    def viewvol(self, ticket):
        ticket["function"] = "getVolState"
        return self.DoWork( self.getVolState, ticket)

    # wrapper method for client - server communication
    def insertvol(self, ticket):
        ticket["function"] = "insert"
        if not ticket.has_key("newlib"):
            ticket["status"] = (e_errors.WRONGPARAMETER, 37, "new library name not specified")
            Trace.log(e_errors.ERROR, "ERROR:insertvol new library name not specified")
            return
        return self.DoWork( self.insert, ticket)

    # wrapper method for client - server communication
    def ejectvol(self, ticket):
        ticket["function"] = "eject"
        return self.DoWork( self.eject, ticket)

    def homeAndRestartRobot(self, ticket):
        ticket["function"] = "homeAndRestart"
        return self.DoWork( self.robotHomeAndRestart, ticket)

    def doCleaningCycle(self, ticket):
        ticket["function"] = "cleanCycle"
        return self.DoWork( self.cleanCycle, ticket)

    def set_max_work(self,ticket):
        self.max_work = ticket["max_work"]
        self.reply_to_caller({'status' : (e_errors.OK, 0, None)})

    def getwork(self,ticket):
        result = []
        for i in self.work_list:
            result.append((i['function'], i['vol_ticket']['external_label'], i['drive_id']))
        self.reply_to_caller({'status' : (e_errors.OK, 0, None),
                              'max_work':self.max_work,
                              'worklist':result})

    # load volume into the drive;  default, overridden for other media changers
    def load(self,
             external_label,    # volume external label
             drive,             # drive id
             media_type):       # media type
        if 'delay' in self.mc_config.keys() and self.mc_config['delay']:
            # YES, THIS BLOCK IS FOR THE DEVELOPMENT ENVIRONMENT AND THE
            # OUTPUT OF THE PRINTS GO TO THE TERMINAL
            print "make sure tape %s in in drive %s"%(external_label,drive)
            time.sleep( self.mc_config['delay'] )
            print 'continuing with reply'
	if type(self.set_this_error) == type(1) and self.set_this_error > 9990:
	    return("ERROR",self.set_this_error, None, None, None)
	else:
	    return (self.set_this_error, 0, None)

    # unload volume from the drive;  default overridden for other media changers
    def unload(self,
               external_label,  # volume external label
               drive,           # drive id
               media_type):     # media type
        if 'delay' in self.mc_config.keys() and self.mc_config['delay']:
            Trace.log(e_errors.INFO, "remove tape "+external_label+" from drive "+drive)
            time.sleep( self.mc_config['delay'] )
	if type(self.set_this_error) == type(1) and self.set_this_error > 9990:
		return("ERROR",self.set_this_error, None, None, None)
	else:
		return (self.set_this_error, 0, None)

    # view volume in the drive;  default overridden for other media changers
    #def view(self,
    #           external_label,  # volume external label
    #       media_type):         # media type
    #    return (e_errors.OK, 0, None, 'O') # return 'O' - occupied aka unmounted

    # getVolState in the drive;  default overridden for other media changers - to replace above tgj1
    def getVolState(self, ticket):
        return (e_errors.OK, 0, None, 'O') # return 'O' - occupied aka unmounted

    # insert volume into the robot;  default overridden for other media changers
    def insert(self,ticket):
        return (e_errors.OK, 0, None, '') # return '' - no inserted volumes

    # eject volume from the robot;  default overridden for other media changers
    def eject(self,ticket):
        return (e_errors.OK, 0, None)

    def robotHomeAndRestart(self,ticket):
        pass
        return (e_errors.OK, 0, None)

    def cleanCycle(self,ticket):
        pass
        return (e_errors.OK, 0, None)

    def waitingCleanCycle(self,ticket):
        pass
        return (e_errors.OK, 0, None)

    def startTimer(self,ticket):
        pass
        return (e_errors.OK, 0, None)

    # prepare is overridden by dismount for mount; i.e. for tape drives we always dismount before mount
    def prepare(self,
               external_label,
               drive,
               media_type) :
        pass
        return (e_errors.OK, 0, None)

    def doWaitingInserts(self):
        pass
        return (e_errors.OK, 0, None)

    def doWaitingCleaningCycles(self, ticket):
        ticket["function"] = "waitingCleanCycle"
        return self.DoWork( self.waitingCleanCycle, ticket)

    def robotQuery(self,ticket):
        return (e_errors.OK, 0, None)

    def getNretry(self):
        numberOfRetries = 3
        return numberOfRetries

    # Do the forking and call the function
    def DoWork(self, function, ticket):
        if not ticket.has_key("function"):
           e = 'MISSING FUNCTION KEY'
           Trace.log(e_errors.ERROR, "%s"%(e,))
           ticket['status'] = e
           return e

        if ticket['function'] in ("mount", "dismount"):
            if not ticket.has_key("vol_ticket"):
               e = 'MISSING VOL_TICKET'
               Trace.log(e_errors.ERROR, "%s"%(e,))
               ticket['status'] = e
               return e
            if not ticket.has_key("drive_id"):
               e = 'MISSING DRIVE_ID'
               Trace.log(e_errors.ERROR, "%s"%(e,))
               ticket['status'] = e
               return e
            if not ticket['vol_ticket'].has_key("external_label"):
               print "MISSING EXTERNAL_LABEL", ticket  ### XXX What is going on?
               e = "MISSING EXTERNAL LABEL for %s %s"%(ticket["function"],ticket["drive_id"])
               Trace.log(e_errors.ERROR, "%s"%(e,))
               ticket['status'] = e
               return e
            Trace.log(e_errors.INFO, 'REQUESTED %s %s %s'%
                      (ticket['function'], ticket['vol_ticket']['external_label'],ticket['drive_id']))
            # if drive is doing a clean cycle, drop request
            for i in self.work_list:
                try:
                    if (i['function'] == "cleanCycle" and i.has_key('drive_id') and
                        i['drive_id'] == ticket['drive_id']):
                        Trace.log(e_errors.INFO,
                                  'REQUESTED %s request of %s in %s  dropped, drive in cleaning cycle.'%
                                  (ticket['function'],ticket['vol_ticket']['external_label'],
                                   ticket['drive_id']))

                        return
                except:
                    Trace.handle_error()
                    Trace.log(e_errors.ERROR, "ERROR %s"%(i,))

        else:
            Trace.log(e_errors.INFO, 'REQUESTED  %s'%(ticket['function'],))
        #put cleaningCyles on cleaning list
        if ticket['function'] == "cleanCycle":
            ticket["ra"] = (self.reply_address,self.client_number,self.current_id)
            todo = (ticket,function)
            self.work_cleaning_list.append(todo)

        #if we have max number of working children, assume client will resend
        # let work list length exceed max_work for cleaningCycle
        if ticket["function"] != "getVolState":
            if len(self.work_list) >= self.max_work:
                Trace.log(e_errors.INFO, "MC Overflow: "+ repr(self.max_work) + " " + ticket['function'])
                return
              ##elif work queue is temporarily closed, assume client will resend
            elif  self.workQueueClosed and len(self.work_list)>0:
                Trace.log(e_errors.INFO,
                          "MC Queue Closed: " + ticket['function'] + " " + repr(len(self.work_list)))
                return

        # otherwise, we can process work

        # set the reply address - note this could be a general thing in dispatching worker
        ticket["ra"] = (self.reply_address,self.client_number,self.current_id)
        # bump other requests for cleaning
        if len(self.work_cleaning_list) > 0:
            if ticket['function'] != "waitingCleanCycle":
                Trace.log(e_errors.INFO, "MC: "+ ticket['function'] + " bumped for cleaning")
            ticket = self.work_cleaning_list[0][0]
            function = self.work_cleaning_list[0][1]
            self.work_cleaning_list.remove((ticket,function))
            Trace.log(e_errors.INFO, 'REPLACEMENT '+ticket['function'])
        # if this a duplicate request, drop it
        for i in self.work_list:
            if i["ra"] == ticket["ra"]:
                Trace.log(e_errors.INFO,"duplicate request, drop it %s %s"%(repr(i["ra"]),repr(ticket["ra"])))
                return
        # if function is insert and queue not empty, close work queue
        if ticket["function"] == "insert":
            if len(self.work_list)>0:
               self.workQueueClosed = 1
               self.timeInsert = time.time()
               self.insertRA = ticket["ra"]
               Trace.log(e_errors.INFO,"RET1 %s"%( ticket["function"],))
               return
            else:
               self.workQueueClosed = 0
        # if not duplicate, fork the work
        pipe = os.pipe()
        if self.fork(ttl=None): #no time limit
            self.add_select_fd(pipe[0])
            os.close(pipe[1])
            # add entry to outstanding work
            self.work_list.append(ticket)
            Trace.trace(11, 'mcDoWork< Parent')
            return

        #  in child process
        if ticket['function'] in ("mount", "dismount"):
            msg="%s %s %s" % (ticket['function'],ticket['vol_ticket']['external_label'], ticket['drive_id'])
        else:
            msg="%s" % (ticket['function'],)
        Trace.trace(11, 'mcDoWork> child begin %s '%(msg,))
        os.close(pipe[0])
        # do the work ...
        # ... if this is a mount, dismount first
        if ticket['function'] == "mount":
            Trace.trace(11, 'mcDoWork> child prepare dismount for %s'%(msg,))
            self.logdetail = 0 # don't print a failure  (no tape mounted) message that is really a success
            sts=self.prepare( 'Unknown', ticket['drive_id'], ticket['vol_ticket']['media_type'])
            self.logdetail = 1 # back on
            Trace.trace(11,'mcDoWork> child prepare dismount for %s returned %s'%(msg,sts[2]))
        if ticket['function'] in ('insert','eject','homeAndRestart','cleanCycle','getVolState'):
            Trace.trace(11, 'mcDoWork> child doing %s'%(msg,))
            sts = function(ticket)
            Trace.trace(11,'mcDoWork> child %s returned %s'%(msg,sts))
        else:
            Trace.trace(11, 'mcDoWork> child doing %s'%(msg,))
	    # ok, this is a test only - see if we can mount readonly for 9840 and 9940 tapes
	    media_type = ticket['vol_ticket']['media_type']
	    if ticket['vol_ticket']['media_type'] in ('9840','9940','9940B') and ticket['function'] == 'mount':
		    if enstore_functions2.is_readonly_state(ticket['vol_ticket']['system_inhibit'][1]) or enstore_functions2.is_readonly_state(ticket['vol_ticket']['user_inhibit'][1]):
			    media_type=media_type+"READONLY"
	    #print ticket['function'],ticket['vol_ticket']['external_label'],ticket['vol_ticket']['system_inhibit'][1], ticket['vol_ticket']['user_inhibit'][1],media_type
            
            Trace.trace(12, 'ticket %s'%(ticket,))
            Trace.trace(11, 'mcDoWork> child doing %s. Media type %s'%(msg, media_type))
            sts = function(
                ticket['vol_ticket']['external_label'],
                ticket['drive_id'],
                media_type)
            Trace.trace(11,'mcDoWork> child %s returned %s'%(msg,sts))

        # send status back to MC parent via pipe then via dispatching_worker and WorkDone ticket
        Trace.trace(11, 'mcDoWork< sts'+repr(sts))
        ticket["work"]="WorkDone"       # so dispatching_worker calls WorkDone
        ticket["status"]=sts
        msg = repr(('0','0',ticket))
        bytecount = "%08d" % (len(msg),)
        os.write(pipe[1], bytecount)
        os.write(pipe[1], msg)
        os.close(pipe[1])
        os._exit(0)


    def WorkDone(self, ticket):
        # dispatching_worker sends "WorkDone" ticket here and we reply_to_caller
        # remove work from outstanding work list
        for i in self.work_list:
           if i["ra"] == ticket["ra"]:
              self.work_list.remove(i)
              break
        # report back to original client - probably a mover
        fstat = ticket.get('status', None)
        if fstat[0]=="ok":
            level = e_errors.INFO
        else:
            level = e_errors.ERROR
        if ticket['function'] in ("mount","dismount"):
            Trace.log(level, 'FINISHED %s %s %s  returned %s' %
                      (ticket['function'], ticket['vol_ticket']['external_label'],ticket['drive_id'],fstat))
        else:
            Trace.log(level, 'FINISHED %s returned %s'%(ticket['function'],fstat))
        # reply_with_address uses the "ra" entry in the ticket
        self.reply_with_address(ticket)
        self.robotNotAtHome = 1
        self.lastWorkTime = time.time()
        # check for cleaning jobs
        if len(self.work_list) < self.max_work and len(self.work_cleaning_list) > 0:
            sts = self.doWaitingCleaningCycles(ticket)
        # if work queue is closed and work_list is empty, do insert
        sts = self.doWaitingInserts()

# aml2 robot loader server
class AML2_MediaLoader(MediaLoaderMethods):

    def __init__(self, medch, max_work=7, csc=None):
        MediaLoaderMethods.__init__(self, medch, max_work, csc)

        # robot choices are 'R1', 'R2' or 'Both'
        if self.mc_config.has_key('RobotArm'):   # error if robot not in config
            self.robotArm = string.strip(self.mc_config['RobotArm'])
        else:
            Trace.log(e_errors.ERROR, "ERROR:mc:aml2 no robot arm key in configuration")
            self.robotArm = string.strip(self.mc_config['RobotArm']) # force the exception
            return

        if self.mc_config.has_key('IOBoxMedia'):   # error if IO box media assignments not in config
            self.mediaIOassign = self.mc_config['IOBoxMedia']
        else:
            Trace.log(e_errors.ERROR, "ERROR:mc:aml2 no IO box media assignments in configuration")
            self.mediaIOassign = self.mc_config['IOBoxMedia'] # force the exception
            return

        if self.mc_config.has_key('DriveCleanTime'):   # error if DriveCleanTime assignments not in config
            self.driveCleanTime = self.mc_config['DriveCleanTime']
        else:
            Trace.log(e_errors.ERROR, "ERROR:mc:aml2 no DriveCleanTime assignments in configuration")
            self.driveCleanTime = self.mc_config['DriveCleanTime'] # force the exception
            return

        if self.mc_config.has_key('IdleTimeHome'):
            if (type(self.mc_config['IdleTimeHome']) == types.IntType and
                self.mc_config['IdleTimeHome'] > 20):
                self.idleTimeLimit = self.mc_config['IdleTimeHome']
            else:
                Trace.log(e_errors.INFO, "mc:aml2 IdleHomeTime is not defined or too small, default used")

        self.prepare=self.unload

    # retry function call
    def retry_function(self,function,*args):
        count = self.getNretry()
        rpcErrors = 0
        sts=("",0,"")
        while count > 0 and sts[0] != e_errors.OK:
            try:
                sts=apply(function,args)
                if sts[1] != 0:
                   if self.logdetail:
                      Trace.log(e_errors.ERROR, 'retry_function: function %s %s error %s'%(repr(function),args,sts[2]))
                if sts[1] == 1 and rpcErrors < 2:  # RPC failure
                    time.sleep(10)
                    rpcErrors = rpcErrors + 1
                elif (sts[1] == 5 or         # requested drive in use
                      sts[1] == 8 or         # DAS was unable to communicate with AMU
                      sts[1] == 10 or        # AMU was unable to communicate with robot
                      #sts[1] == 34 or        # The aci request timed out
                      sts[1] == 24):         # requested volume in use
                    count = count - 1
                    time.sleep(20)
                elif (sts[1] == e_errors.MC_VOLNOTHOME): # tape not in home position
                    count = count - 1
                    time.sleep(120)
                else:
                    break
            except:
                exc,val,tb = Trace.handle_error()
                return "ERROR", 37, str(val)   #XXX very ad-hoc!
                                 ## this is "command error" in aml2.py
        return sts

    # load volume into the drive;
    def load(self,
             external_label,    # volume external label
             drive,             # drive id
             media_type):       # media type
        import aml2
        return self.retry_function(aml2.mount,external_label, drive,media_type)

    # unload volume from the drive
    def unload(self,
               external_label,  # volume external label
               drive,           # drive id
               media_type):     # media type
        import aml2
        return self.retry_function(aml2.dismount,external_label, drive,media_type)

    def robotHome(self, arm):
        import aml2
        return self.retry_function(aml2.robotHome,arm)

    def robotStatus(self):
        import aml2
        return self.retry_function(aml2.robotStatus)

    def robotStart(self, arm):
        import aml2
        return self.retry_function(aml2.robotStart, arm)

    def insert(self, ticket):
        import aml2
        self.insertRA = None
        classTicket = { 'mcSelf' : self }
        ticket['timeOfCmd'] = time.time()
        ticket['medIOassign'] = self.mediaIOassign
        return self.retry_function(aml2.insert,ticket, classTicket)

    def eject(self, ticket):
        import aml2
        classTicket = { 'mcSelf' : self }
        ticket['medIOassign'] = self.mediaIOassign
        return self.retry_function(aml2.eject,ticket, classTicket)

    def robotHomeAndRestart(self, ticket):
        import aml2
        classTicket = { 'mcSelf' : self }
        ticket['robotArm'] = self.robotArm
        return self.retry_function(aml2.robotHomeAndRestart,ticket, classTicket)

    def getVolState(self, ticket):
        import aml2
        "get current state of the tape"
        external_label = ticket['external_label']
        media_type = ticket['media_type']
        stat,volstate = aml2.view(external_label,media_type)
        state='U' # unknown
        if stat!=0:
            return 'BAD', stat, 'aci_view return code', state
        if volstate == None:
            return 'BAD', stat, 'volume %s not found'%(external_label,),state
        return (e_errors.OK, 0, "",volstate.attrib)

    def cleanCycle(self, inTicket):
        import aml2
        #do drive cleaning cycle
        Trace.log(e_errors.INFO, 'mc:aml2 ticket='+repr(inTicket))
        classTicket = { 'mcSelf' : self }
        try:
            drive = inTicket['moverConfig']['mc_device']
        except KeyError:
            Trace.log(e_errors.ERROR, 'mc:aml2 no device field found in ticket.')
            status = 37
            return e_errors.DOESNOTEXIST, status, "no device field found in ticket"

        driveType = drive[:2]  # ... need device type, not actual device
        try:
            if self.driveCleanTime:
                cleanTime = self.driveCleanTime[driveType][0]  # clean time in seconds
                driveCleanCycles = self.driveCleanTime[driveType][1]  # number of cleaning cycles
            else:
                cleanTime = 60
                driveCleanCycles = 1
        except KeyError:
            cleanTime = 60
            driveCleanCycles = 1

        vcc = volume_clerk_client.VolumeClerkClient(self.csc)
        min_remaining_bytes = 1
        vol_veto_list = []
        first_found = 0
        libraryManagers = inTicket['moverConfig']['library']
        if type(libraryManagers) == types.StringType:
            lm = libraryManagers
            library = string.split(libraryManagers,".")[0]
        elif type(libraryManagers) == types.ListType:
            lm = libraryManagers[0]
            library = string.split(libraryManagers[0],".")[0]
        else:
            Trace.log(e_errors.ERROR, 'mc:aml2 library_manager field not found in ticket.')
            status = 37
            return e_errors.DOESNOTEXIST, status, "no library_manager field found in ticket"
        lm_info = self.csc.get(lm)
        if not lm_info.has_key('CleanTapeVolumeFamily'):
            Trace.log(e_errors.ERROR, 'mc: no CleanTapeVolumeFamily field found in ticket.')
            status = 37
            return e_errors.DOESNOTEXIST, status, "no CleanTapeVolumeFamily field found in ticket"
        cleanTapeVolumeFamily = lm_info['CleanTapeVolumeFamily']
        v = vcc.next_write_volume(library,
                                  min_remaining_bytes, cleanTapeVolumeFamily,
                                  vol_veto_list, first_found, exact_match=1)  # get which volume to use
        if v["status"][0] != e_errors.OK:
            Trace.log(e_errors.ERROR,"error getting cleaning volume:%s %s"%
                      (v["status"][0],v["status"][1]))
            status = 37
            return v["status"][0], 0, v["status"][1]

        for i in range(driveCleanCycles):
            Trace.log(e_errors.INFO, "AML2 clean drive %s, vol. %s"%(drive,v['external_label']))
            rt = self.load(v['external_label'], drive, v['media_type'])
            status = rt[1]
            if status != 0:      # mount returned error
                s1,s2,s3 = self.retry_function(aml2.convert_status,status)
                return s1, s2, s3

            time.sleep(cleanTime)  # wait cleanTime seconds
            rt = self.unload(v['external_label'], drive, v['media_type'])
            status = rt[1]
            if status != 0:      # dismount returned error
                s1,s2,s3 = self.retry_function(aml2.convert_status,status)
                return s1, s2, s3
            Trace.log(e_errors.INFO,"AML2 Clean returned %s"%(rt,))

        retTicket = vcc.get_remaining_bytes(v['external_label'])
        remaining_bytes = retTicket['remaining_bytes']-1
        vcc.set_remaining_bytes(v['external_label'],remaining_bytes,'\0', None)
        return (e_errors.OK, 0, None)

    def doWaitingInserts(self):
        #do delayed insertvols
        if self.workQueueClosed and len(self.work_list)==0:
            self.workQueueClosed = 0
            ticket = { 'function'  : 'insert',
                       'timeOfCmd' : self.timeInsert,
                       'ra'        : self.insertRA }
            self.DoWork( self.insert, ticket)
        return (e_errors.OK, 0, None)

    def checkMyself(self):
        # do regularily scheduled internal checks
        if self.robotNotAtHome and (time.time()-self.lastWorkTime) > self.idleTimeLimit:
            self.robotNotAtHome = 0
            ticket = { 'function' : 'homeAndRestart', 'robotArm' : self.robotArm }
            sts = self.robotHomeAndRestart(ticket)
            self.lastWorkTime = time.time()

# STK robot loader server
class STK_MediaLoader(MediaLoaderMethods):

    def __init__(self, medch, max_work=7, csc=None):
        MediaLoaderMethods.__init__(self,medch,max_work,csc)
        self.prepare = self.unload
        self.DEBUG = 0
	self.driveCleanTime = self.mc_config.get('DriveCleanTime',{'9840':[60,1],'9940':[60,1]})
        print "STK MediaLoader initialized"

    # retry function call
    def retry_function(self,function,*args):
        count = self.getNretry()
        sts=("",0,"")
        # retry every error
        while count > 0 and sts[0] != e_errors.OK:
            try:
                sts=apply(function,args)
                if sts[1] != 0:
                   if self.logdetail:
                      Trace.log(e_errors.ERROR, 'retry_function: function %s  %s  sts[1] %s  sts[2] %s  count %s'%(repr(function),args,sts[1],sts[2],count))
                   if function==self.mount:
                       time.sleep(60)
                       fixsts=apply(self.dismount,args)
                       Trace.log(e_errors.INFO, 'Tried %s %s  status=%s %s  Desperation dismount  status %s %s'%(repr(function),args,sts[1],sts[2],fixsts[1],fixsts[2]))
                   time.sleep(60)
                   count = count - 1
                else:
                    break
            except:
                exc,val,tb = Trace.handle_error()
                return str(exc),0,""
        return sts

    # load volume into the drive;
    def load(self,
             external_label,    # volume external label
             drive,             # drive id
             media_type):       # media type
        return self.retry_function(self.mount,external_label,drive,media_type)

    # unload volume from the drive
    def unload(self,
               external_label,  # volume external label
               drive,           # drive id
               media_type):     # media type
        return self.retry_function(self.dismount,external_label,drive,media_type)

    # see how my good friend the robot is doing
    def robotQuery(self,ticket):
        # don't retry this query - want to know current status
        ticket['status'] =  self.query_server()
        self.reply_to_caller(ticket)


    #FIXME - what the devil is this?
    def getVolState(self, ticket):
        external_label = ticket['external_label']
        media_type = ticket['media_type']
        rt = self.retry_function(self.query,external_label,media_type)
        Trace.trace(11, "getVolState returned %s"%(rt,))
        if rt[3] == '\000':
            state=''
        else :
            state = rt[3]
            if not state and rt[2]:  # volumes not in the robot
                state = rt[2]
        return (rt[0], rt[1], rt[2], state)


    def cleanCycle(self, inTicket):
        #do drive cleaning cycle
        Trace.log(e_errors.INFO, 'mc:ticket='+repr(inTicket))
        classTicket = { 'mcSelf' : self }
        try:
            drive = inTicket['moverConfig']['mc_device']
        except KeyError:
            Trace.log(e_errors.ERROR, 'mc:no device field found in ticket.')
            status = 37
            return e_errors.DOESNOTEXIST, status, "no device field found in ticket"

        driveType = drive[:2]  # ... need device type, not actual device
        try:
            if self.driveCleanTime:
                cleanTime = self.driveCleanTime[driveType][0]  # clean time in seconds
                driveCleanCycles = self.driveCleanTime[driveType][1]  # number of cleaning cycles
            else:
                cleanTime = 60
                driveCleanCycles = 1
        except KeyError:
            cleanTime = 60
            driveCleanCycles = 1

        vcc = volume_clerk_client.VolumeClerkClient(self.csc)
        min_remaining_bytes = 1
        vol_veto_list = []
        first_found = 0
        libraryManagers = inTicket['moverConfig']['library']
        if type(libraryManagers) == types.StringType:
            lm = libraryManagers
            library = string.split(libraryManagers,".")[0]
        elif type(libraryManagers) == types.ListType:
            lm = libraryManagers[0]
            library = string.split(libraryManagers[0],".")[0]
        else:
            Trace.log(e_errors.ERROR, 'mc: library_manager field not found in ticket.')
            status = 37
            return e_errors.DOESNOTEXIST, status, "no library_manager field found in ticket"
        lm_info = self.csc.get(lm)
        if not lm_info.has_key('CleanTapeVolumeFamily'):
            Trace.log(e_errors.ERROR, 'mc: no CleanTapeVolumeFamily field found in ticket.')
            status = 37
            return e_errors.DOESNOTEXIST, status, "no CleanTapeVolumeFamily field found in ticket"
        cleanTapeVolumeFamily = lm_info['CleanTapeVolumeFamily']
        v = vcc.next_write_volume(library,
                                  min_remaining_bytes, cleanTapeVolumeFamily,
                                  vol_veto_list, first_found, exact_match=1)  # get which volume to use
        if v["status"][0] != e_errors.OK:
            Trace.log(e_errors.ERROR,"error getting cleaning volume:%s %s"%
                      (v["status"][0],v["status"][1]))
            status = 37
            return v["status"][0], 0, v["status"][1]

        for i in range(driveCleanCycles):
            Trace.log(e_errors.INFO, "STK clean drive %s, vol. %s"%(drive,v['external_label']))
            rt = self.load(v['external_label'], drive, v['media_type'])
            status = rt[0]
            if status != e_errors.OK:      # mount returned error
                return status, 0, None

            time.sleep(cleanTime)  # wait cleanTime seconds
            rt = self.unload(v['external_label'], drive, v['media_type'])
            status = rt[0]
	    if status != e_errors.OK:
                return status, 0, None
            Trace.log(e_errors.INFO,"STK Clean returned %s"%(rt,))

        retTicket = vcc.get_remaining_bytes(v['external_label'])
        remaining_bytes = retTicket['remaining_bytes']-1
        vcc.set_remaining_bytes(v['external_label'],remaining_bytes,'\0', None)
        return (e_errors.OK, 0, None)

    # simple elapsed timer
    def delta_t(self,begin):
            (ut, st,cut, cst,now) = os.times()
            return (now-begin, now)


    # execute a stk cmd_proc command, but don't wait forever for it to complete
    #mostly stolen from Demo/tkinter/guido/ShellWindow.py - spawn function
    def timed_command(self,cmd,min_response_length=0,timeout=60):
        now=timeofday.tod()
        p2cread, p2cwrite = os.pipe()
        c2pread, c2pwrite = os.pipe()
        command = "echo %s|/export/home/ACSSS/bin/cmd_proc 2>&1" % (cmd,)
        cmd_lookfor = "ACSSA> %s" % (cmd,)

        # can not use dispatching work fork because we are already child.
        # need to kill explictly and children can't kill
        (dum,mark) = self.delta_t(0)
        pid = os.fork()

        if pid == 0:
            # Child
            for i in 0, 1, 2:
                try:
                    os.close(i)
                except os.error:
                    pass
            if os.dup(p2cread) <> 0:
                print 'ERROR: timed_command pc2cread bad read dup'
                Trace.log(e_errors.ERROR, 'timed_command pc2cread bad read dup')
            if os.dup(c2pwrite) <> 1:
                print 'ERROR: timed_command c2pwrite bad write dup'
                Trace.log(e_errors.ERROR, 'timed_command c2pwrite bad write dup')
            if os.dup(c2pwrite) <> 2:
                print 'ERROR: timed_command c2pwrite bad error dup'
                Trace.log(e_errors.ERROR, 'timed_command c2pwrite bad error dup')
            MAXFD = 100 # Max number of file descriptors (os.getdtablesize()???)
            for i in range(3, MAXFD):
                try:
                    os.close(i)
                except:
                    pass
            try:
                #I know this is hard-coded and inflexible. That is what I want so as to
                #prevent any possible security problem.

                os.execv('/usr/bin/rsh',[self.acls_host,'-l',self.acls_uname,command])
            finally:
                exc, msg, tb = sys.exc_info()
                Trace.log(e_errors.ERROR, "timed_command execv failed:  %s %s %s"% (exc, msg, traceback.format_tb(tb)))
                os._exit(1)

        os.close(p2cread)
        os.close(c2pwrite)
        os.close(p2cwrite)


        #wait for child to complete, or kill it
        start = time.time()
        if self.DEBUG:
            print timeofday.tod(),cmd
            Trace.trace(e_errors.INFO,"%s" %(cmd,))
        active=0
        (p,r) = (0,0)
        try:
            while active<timeout:
                p,r = os.waitpid(pid,os.WNOHANG)
                if p!=0:
                    break
                time.sleep(1)
                active=time.time()-start
            else:
                msg="killing %d => %s" % (pid,cmd)
                print timeofday.tod(),msg
                Trace.trace(e_errors.INFO,msg)
                os.kill(pid,signal.SIGTERM)
                time.sleep(1)
                p,r = os.waitpid(pid,os.WNOHANG)
                if p==0:
                    msg="kill -9ing %d => %s" % (pid,cmd)
                    print timeofday.tod(),msg
                    Trace.trace(e_errors.INFO,msg)
                    os.kill(pid,signal.SIGKILL)
                    time.sleep(2)
                    p,r = os.waitpid(pid,os.WNOHANG)
        except:
            exc, msg, tb = sys.exc_info()
            Trace.log(e_errors.ERROR, "timed_command wait for child failed:  %s %s %s"% (exc, msg, traceback.format_tb(tb)))
	    os.close(c2pread)
            return -1,[], self.delta_t(mark)[0]

        if p==0:
	    os.close(c2pread)
            return -2,[], self.delta_t(mark)[0]

        # now read response from the pipe
        message = ""
        blanks=0
        nread=0
	if string.find(cmd,'mount') != -1:  # this is a mount or a dismount command
	    maxread=100  # quick response on queries
	else:
	    maxread=10000 # slow respone on mount/dismounts

	nlines=0
	ntries=0
	jonflag=0
        # async message start with a date:  2001-12-20 07:33:17     0    Drive   0, 0,10,12: Cleaned.
        # unfortunately, not just async messages start with a date.  Alas, each message has to be parsed.
        async_date=re.compile("20\d\d-\d\d-\d\d \d\d:\d\d:\d\d")  
        while nlines<19 and ntries<3:
	  ntries=ntries+1
          while blanks<2 and nread<maxread:
            msg=os.read(c2pread,200)
            message = message+msg
            nread = nread+1
            if msg == '':
                blanks = blanks+1
#	    if self.DEBUG:
#	        nl=0
#		ml=string.split(msg,'\012')
#		for l in ml:
#                   nl=nl+1
#		   print "nread=",nread, "line=",nl, l
          response = []
          resp = string.split(message,'\012')
	  nl=0
	  for l in resp:
            if async_date.match(l):
              if string.find(l,'Place cartridges in CAP') != -1 or \
                 string.find(l,'Remove cartridges from CAP') != -1 or \
                 string.find(l,'Library error, LSM offline') != -1 or \
                 string.find(l,'Library error, Transport failure') != -1 or \
                 string.find(l,'Library error, LMU failure') != -1 or \
                 string.find(l,'LMU Recovery Complete') != -1 or \
                 string.find(l,': Offline.') != -1 or \
                 string.find(l,': Online.') != -1 or \
                 string.find(l,': Enter operation ') != -1 or \
                 string.find(l,'Clean drive') != -1 or \
                 string.find(l,'Cleaned') != -1:
                if self.DEBUG:
                  print "DELETED:", l
                jonflag=1
                continue
	    if self.DEBUG:
              print    "response line =",nl, l
            response.append(l)
	    nl=nl+1
          nlines=len(response)

	  nl=0
	  if jonflag and self.DEBUG:
	       for l in response:
		  print    "parsed lines =",nl, l
		  nl=nl+1

	os.close(c2pread)
        size = len(response)
        if size <= 19:
            return -3,[], self.delta_t(mark)[0]
        status = 0
        for look in range(19,size): # 1st part of response is STK copyright information
            if string.find(response[look], cmd_lookfor, 0) == 0:
                break
        if look == size:
            status = -4
            look = 0
        else:
            if len(response[look:]) < min_response_length:
                status = -5
        if self.DEBUG:
            rightnow = timeofday.tod() # the times on fntt are not necessarily right, allows us to correlate log time
            rsp = [now,response[look:],rightnow]
            pprint.pprint(rsp)

        return status,response[look:], self.delta_t(mark)[0]

    def query(self,volume, media_type=""):

        # build the command, and what to look for in the response
        command = "query vol %s" % (volume,)
        answer_lookfor = "%s " % (volume,)

        # execute the command and read the response
        status,response, delta = self.timed_command(command,4,10)
        if status != 0:
            E=1
            msg = "QUERY %i: %s => %i,%s" % (E,command,status,response)
            Trace.log(e_errors.ERROR, msg)
            return ("ERROR", E, response, '', msg)

        # got response, parse it and put it into the standard form
        answer = string.strip(response[3])
        if string.find(answer, answer_lookfor,0) != 0:
            E=2
            msg = "QUERY %i: %s => %i,%s" % (E,command,status,response)
            Trace.log(e_errors.ERROR, msg)
            return ("ERROR", E, response, '', msg)
        elif string.find(answer,' home ') != -1:
            msg = "%s => %i,%s" % (command,status,answer)
            Trace.log(e_errors.INFO, msg)
            return (e_errors.OK,0,answer, 'O', msg) # occupied
        elif string.find(answer,' in drive ') != -1:
            msg = "%s => %i,%s" % (command,status,answer)
            Trace.log(e_errors.INFO, msg)
            return (e_errors.OK,0,answer, 'M', msg) # mounted
        elif string.find(answer,' in transit ') != -1:
            msg = "%s => %i,%s" % (command,status,answer)
            Trace.log(e_errors.INFO, msg)
            return (e_errors.OK,0,answer, 'T', msg) # transit
        else:
            E=3
            msg = "QUERY %i: %s => %i,%s" % (E,command,status,response)
            Trace.log(e_errors.ERROR, msg)
            return ("ERROR", E, answer, '', msg)

    def query_drive(self,drive):

        # build the command, and what to look for in the response
        command = "query drive %s" % (drive,)
        answer_lookfor = "%s " % (drive,)

        # execute the command and read the response
        # FIXME - what if this hangs?
        status,response, delta = self.timed_command(command,4,10)
        if status != 0:
            E=4
            msg = "QUERY_DRIVE %i: %s => %i,%s" % (E,command,status,response)
            Trace.log(e_errors.ERROR, msg)
            return ("ERROR", E, response, '', msg)


        # got response, parse it and put it into the standard form
        answer = string.strip(response[3])
        answer = string.replace(answer,', ',',') # easier to part drive id
        if string.find(answer, answer_lookfor,0) != 0:
            E=5
            msg = "QUERY_DRIVE %i: %s => %i,%s" % (E,command,status,answer)
            Trace.log(e_errors.ERROR, msg)
            return ("ERROR", E, answer, '', msg)
        elif string.find(answer,' online ') == -1:
            E=6
            msg = "QUERY_DRIVE %i: %s => %i,%s" % (E,command,status,answer)
            Trace.log(e_errors.ERROR, msg)
            return ("ERROR", E, answer, '', msg)
        elif string.find(answer,' available ') != -1:
            msg = "%s => %i,%s" % (command,status,answer)
            Trace.log(e_errors.INFO, msg)
            return (e_errors.OK,0,answer, '', msg) # empty
        elif string.find(answer,' in use ') != -1:
            loc = string.find(answer,' in use ')
            volume = string.split(answer[loc+8:])[0]
            msg = "%s => %i,%s" % (command,status,answer)
            Trace.log(e_errors.INFO, msg)
            return (e_errors.OK,0,answer, volume, msg) # mounted and in use
        else:
            E=7
            msg = "QUERY_DRIVE %i: %s => %i,%s" % (E,command,status,answer)
            Trace.log(e_errors.ERROR, msg)
            return ("ERROR", E, answer, '', msg)

    def mount(self,volume, drive, media_type="",view_first=1):

        # strip out the readonly if present
	if string.find(media_type,'READONLY')!=-1:
		media_type=string.replace(media_type,'READONLY','')
		readonly=1
	else:
		readonly=0
        # build the command, and what to look for in the response
        command = "mount %s %s" % (volume,drive)
	if readonly:
		command = command + " readonly"
        answer_lookfor = "Mount: %s mounted on " % (volume,)

        # check if tape is in the storage location or somewhere else
        if view_first:
            status,stat,response,attrib,com_sent = self.query(volume, media_type)

            if stat!=0:
                E=e_errors.MC_FAILCHKVOL
                msg = "MOUNT %i: %s => %i,%s" % (E,command,stat,response)
                Trace.log(e_errors.ERROR, msg)
                return ("ERROR", E, response, '', msg)
            if attrib != "O": # look for tape in tower (occupied="O")
                E=e_errors.MC_VOLNOTHOME
                msg = "MOUNT %i: Tape is not in home position. %s => %s,%s" % (E,command,status,response)
                Trace.log(e_errors.ERROR, msg)
                return ("ERROR", E, response, '', msg)

        # check if any tape is mounted in this drive
            status,stat,response,volser,com_sent = self.query_drive(drive)
            if stat!=0:
                E=e_errors.MC_FAILCHKDRV
                msg = "MOUNT %i: %s => %i,%s" % (E,command,stat,response)
                Trace.log(e_errors.ERROR, msg)
                return ("ERROR", E, response, '', msg)
            if volser != "": # look for any tape mounted in this drive
                E=e_errors.MC_DRVNOTEMPTY
                msg = "MOUNT %i: Drive %s is not empty =>. %s => %s,%s" % (E,drive,command,status,response)
                Trace.log(e_errors.ERROR, msg)
                return ("ERROR", E, response, '', msg)

        # execute the command and read the response
        status,response, delta = self.timed_command(command,2,60*10)
        if status != 0:
            E=12
            msg = "MOUNT %i: %s => %i,%s" % (E,command,status,response)
            Trace.log(e_errors.ERROR, msg)
            return ("ERROR", E, response, '', msg)

        # got response, parse it and put it into the standard form
        answer = string.strip(response[1])
        if string.find(answer, answer_lookfor,0) != 0:
            E=13
            msg = "MOUNT %i: %s => %i,%s" % (E,command,status,answer)
            Trace.log(e_errors.ERROR, msg)
            return ("ERROR", E, response, '', msg)
        msg = "%s => %i,%s" % (command,status,answer)
        Trace.log(e_errors.INFO, msg)
        return (e_errors.OK, 0,msg)


    def dismount(self,volume, drive, media_type="",view_first=1):

        # build the command, and what to look for in the response
        command = "dismount VOLUME %s force" % (drive,)
        answer_lookfor = "Dismount: Forced dismount of "

        # check if any tape is mounted in this drive
        if view_first:
            status,stat,response,volser,com_sent = self.query_drive(drive)
            if stat!=0:
                E=e_errors.MC_FAILCHKDRV
                msg = "DISMOUNT %i: %s => %i,%s" % (E,command,stat,response)
                Trace.log(e_errors.ERROR, msg)
                return ("ERROR", E, response, '', msg)

            if volser == "": # look for any tape mounted in this drive
                if volume!="Unknown":
                    #FIXME - this should be a real error. mover needs to know which tape it has.
                    E=14
                    msg = "Dismount %i ignored: Drive %s is empty. Thought %s was there =>. %s => %s,%s" % (E,drive,volume,command,status,response)
                    Trace.log(e_errors.INFO, msg)
                    return (e_errors.OK, 0,response, '',msg)
                else: #don't know the volume on startup
                    E=15
                    msg = "Dismount %i ignored: Drive %s is empty. Thought %s was there =>. %s => %s,%s" % (E,drive,volume,command,status,response)
                    Trace.log(e_errors.INFO, msg)
                    return (e_errors.OK, 0,response, '',msg)

        # execute the command and read the response
        status,response,delta = self.timed_command(command,2,60*10)
        if status != 0:
            E=16
            msg = "DISMOUNT %i: %s => %i,%s" % (E,command,status,response)
            Trace.log(e_errors.ERROR, msg)
            return ("ERROR", E, response, '', msg)

        # got response, parse it and put it into the standard form
        answer = string.strip(response[1])
        if string.find(answer, answer_lookfor,0) != 0:
            E=17
            msg = "DISMOUNT %i: %s => %i,%s" % (E,command,status,answer)
            Trace.log(e_errors.ERROR, msg)
            return ("ERROR", E, response, '', msg)
        msg = "%s => %i,%s" % (command,status,answer)
        Trace.log(e_errors.INFO, msg)
        return (e_errors.OK, 0,msg)


    def query_server(self):

        # build the command, and what to look for in the response
        command = "query server"
        answer_lookfor = "run"

        # execute the command and read the response
        status,response,delta = self.timed_command(command,5,10)
        if status != 0:
            E=18
            msg = "query server %i: %s => %i,%s" % (E,command,status,response)
            Trace.log(e_errors.ERROR, msg)
            return ("ERROR", E, response, '', msg)

        # got response, parse it and put it into the standard form
        answer = string.strip(response[4])
        if string.find(answer, answer_lookfor,0) != 0:
            E=19
            msg = "query_server %i: %s => %i,%s, %f" % (E,command,status,response,delta)
            Trace.log(e_errors.ERROR, msg)
            return ("ERROR", E, response, '', msg)
        msg = "%s => %i,%s, %f" % (command,status,answer[0:17],delta)
        Trace.log(e_errors.INFO, msg)
        return (e_errors.OK, 0,msg)

# manual media changer
class Manual_MediaLoader(MediaLoaderMethods):
    def __init__(self, medch, max_work=7, csc=None):
        MediaLoaderMethods.__init__(self,medch,max_work,csc)
        if self.mc_config.has_key('DriveCleanTime'):   # error if DriveCleanTime assignments not in config
            self.driveCleanTime = self.mc_config['DriveCleanTime']
        else:
            self.driveCleanTime = None

    def loadvol(self, ticket):
        if ticket['vol_ticket']['external_label']:
            rc = os.system("mc_popup_test 'Please load %s'"%(ticket['vol_ticket']['external_label'],)) >> 8
        if rc: self.set_this_error = rc + 9990
	
        else: self.set_this_error = e_errors.OK
        return MediaLoaderMethods.loadvol(self,ticket)

    def unloadvol(self, ticket):
        if ticket['vol_ticket']['external_label']:
            rc = os.system("mc_popup_test 'Please unload %s'"%(ticket['vol_ticket']['external_label']),) >> 8
        if rc: self.set_this_error = rc + 9990
        else: self.set_this_error = e_errors.OK
        return MediaLoaderMethods.unloadvol(self,ticket)

    def cleanCycle(self, inTicket):
        #do drive cleaning cycle
        Trace.log(e_errors.INFO, 'mc: ticket='+repr(inTicket))
        classTicket = { 'mcSelf' : self }
        try:
            drive = inTicket['moverConfig']['mc_device']
        except KeyError:
            Trace.log(e_errors.ERROR, 'mc: no device field found in ticket.')
            status = 37
            return e_errors.DOESNOTEXIST, status, "no device field found in ticket"

        driveType = drive[:2]  # ... need device type, not actual device
        try:
            if self.driveCleanTime:
                cleanTime = self.driveCleanTime[driveType][0]  # clean time in seconds
                driveCleanCycles = self.driveCleanTime[driveType][1]  # number of cleaning cycles
            else:
                cleanTime = 60
                driveCleanCycles = 1
        except KeyError:
            cleanTime = 60
            driveCleanCycles = 1

        vcc = volume_clerk_client.VolumeClerkClient(self.csc)
        min_remaining_bytes = 1
        vol_veto_list = []
        first_found = 0
        libraryManagers = inTicket['moverConfig']['library']
        if type(libraryManagers) == types.StringType:
            lm = libraryManagers
            library = string.split(libraryManagers,".")[0]
        elif type(libraryManagers) == types.ListType:
            lm = libraryManagers[0]
            library = string.split(libraryManagers[0],".")[0]
        else:
            Trace.log(e_errors.ERROR, 'mc: library_manager field not found in ticket.')
            status = 37
            return e_errors.DOESNOTEXIST, status, "no library_manager field found in ticket"
        lm_info = self.csc.get(lm)
        if not lm_info.has_key('CleanTapeVolumeFamily'):
            Trace.log(e_errors.ERROR, 'mc: no CleanTapeVolumeFamily field found in ticket.')
            status = 37
            return e_errors.DOESNOTEXIST, status, "no CleanTapeVolumeFamily field found in ticket"

        cleanTapeVolumeFamily = lm_info['CleanTapeVolumeFamily']
        v = vcc.next_write_volume(library,
                                  min_remaining_bytes, cleanTapeVolumeFamily,
                                  vol_veto_list, first_found, exact_match=1)  # get which volume to use
        if v["status"][0] != e_errors.OK:
            Trace.log(e_errors.ERROR,"error getting cleaning volume:%s %s"%
                      (v["status"][0],v["status"][1]))
            status = 37
            return v["status"][0], 0, v["status"][1]

        for i in range(driveCleanCycles):
            Trace.log(e_errors.INFO, "clean drive %s, vol. %s"%(drive,v['external_label']))
            t = {'vol_ticket':v,'drive_id':drive}
            rt = self.loadvol(t)
            time.sleep(cleanTime)  # wait cleanTime seconds
            rt = self.unloadvol(t)
        retTicket = vcc.get_remaining_bytes(v['external_label'])
        remaining_bytes = retTicket['remaining_bytes']-1
        vcc.set_remaining_bytes(v['external_label'],remaining_bytes,'\0', None)
        return (e_errors.OK, 0, None)


# Raw Disk and stand alone tape media server
class RDD_MediaLoader(MediaLoaderMethods):
    def __init__(self, medch, max_work=1, csc=None):
        MediaLoaderMethods.__init__(self,medch,max_work,csc)


# "Shelf" manual media server - interfaces with OCS
class Shelf_MediaLoader(MediaLoaderMethods):
    """
      Reserving tape drives for the exclusive use of the Enstore-media_changer
      can be done by establishing an OCS Authorization Group inwhich the sole
      user is the enstore_userid and tape drive list consists soley of the
      enstore reserved drives. These drives and the enstore_userid must not be
      listed in any other Authorization Group as well. Section 7.3.2 of the
      OCS Installation/Administration Guide, Version 3.1, details this
      mechanism.
    """
    status_message_dict = {
      'OK':        (e_errors.OK, "request successful"),
      'ERRCfgHst': (e_errors.NOACCESS, "mc:Shlf config OCShost incorrect"),
      'ERRNoLoHN': (e_errors.NOACCESS, "mc:Shlf local hostname not accessable"),
      'ERRPipe':   (e_errors.NOACCESS, "mc:Shlf no pipeObj"),
      'ERRHoNoRe': (e_errors.NOACCESS, "mc:Shlf remote host not responding"),
      'ERRHoCmd':  (e_errors.NOACCESS, "mc:Shlf remote host command unsuccessful"),
      'ERROCSCmd': (e_errors.NOACCESS, "mc:Shlf OCS not responding"),
      'ERRHoNamM': (e_errors.NOACCESS, "mc:Shlf remote host name match"),
      'ERRAloPip': (e_errors.MOUNTFAILED, "mc:Shlf allocate no pipeObj"),
      'ERRAloCmd': (e_errors.MOUNTFAILED, "mc:Shlf allocate failed"),
      'ERRAloDrv': (e_errors.MOUNTFAILED, "mc:Shlf allocate drive not available"),
      'ERRAloRsh': (e_errors.MOUNTFAILED, "mc:Shlf allocate rsh error"),
      'ERRReqPip': (e_errors.MOUNTFAILED, "mc:Shlf request no pipeObj"),
      'ERRReqCmd': (e_errors.MOUNTFAILED, "mc:Shlf request failed"),
      'ERRReqRsh': (e_errors.MOUNTFAILED, "mc:Shlf request rsh error"),
      'ERRDeaPip': (e_errors.DISMOUNTFAILED, "mc:Shlf deallocate no pipeObj"),
      'ERRDeaCmd': (e_errors.DISMOUNTFAILED, "mc:Shlf deallocate failed"),
      'ERRDeaRsh': (e_errors.DISMOUNTFAILED, "mc:Shlf deallocate rsh error"),
      'ERRDsmPip': (e_errors.DISMOUNTFAILED, "mc:Shlf dismount no pipeObj"),
      'ERRDsmCmd': (e_errors.DISMOUNTFAILED, "mc:Shlf dismount failed"),
      'ERRDsmRsh': (e_errors.DISMOUNTFAILED, "mc:Shlf dismount rsh error")
      }

    def status_message(self, s):
        if s in self.status_message_dict.keys():
            return self.status_message_dict[s][1]
        else:
            return s

    def status_code(self, s):
        if s in self.status_message_dict.keys():
            return self.status_message_dict[s][0]
        else:
            return e_errors.ERROR


    def __init__(self, medch, max_work=1, csc=None): #Note: max_work may need to be changed, tgj
        MediaLoaderMethods.__init__(self,medch,max_work,csc)
        self.prepare=self.unload #override prepare with dismount and deallocate

        fnstatusO = self.getOCSHost()
        fnstatus = self.getLocalHost()
        Trace.trace(e_errors.INFO,"Shelf init localHost=%s OCSHost=%s" % (self.localHost, self.ocsHost))
        if fnstatus == 'OK' and fnstatusO == 'OK' :
            index = string.find(self.localHost,self.ocsHost)
            if index > -1 :
                self.cmdPrefix = ""
                self.cmdSuffix = ""
            else :
                self.cmdPrefix = "enrsh " + self.ocsHost + " '"
                self.cmdSuffix = "'"
                fnstatus = self.checkRemoteConnection()
                if fnstatus != 'OK' :
                    Trace.log(e_errors.ERROR, "ERROR:Shelf init %s %s" %
                              (fnstatus, self.status_message(fnstatus)))
                    return
        else :
            ## XXX fnstatusR not defined at this point...
            #Trace.log(e_errors.ERROR, "ERROR:Shelf init %s %s" %
            # (fnstatusR, self.status_message(fnstatusR))
            Trace.log(e_errors.ERROR, "ERROR:Shelf init %s %s" %
                      (fnstatus, self.status_message(fnstatus)))
            return
        fnstatus = self.checkOCSalive()
        if fnstatus != 'OK' :
             Trace.log(e_errors.ERROR, "ERROR:Shelf init %s %s" %
                       (fnstatus, self.status_message(fnstatus)))
             return
        #fnstatus = self.deallocateOCSdrive("AllTheTapeDrives")
        #if fnstatus != 'OK' :
        #     Trace.log(e_errors.ERROR, "ERROR:Shelf init %s %s" %
        #                   (fnstatus, self.status_message(fnstatus)))
        #     return
        Trace.log(e_errors.INFO, "Shelf init %s %s" % (fnstatus, self.status_message(fnstatus)))
        return

    def getOCSHost(self):
        "get the hostname of the OCS machine from the config server"
        fnstatus = 'OK'
        self.ocsHost = string.strip(self.mc_config['OCSclient'])
        index = string.find(self.ocsHost,".")
        if index == 0 :
            fnstatus = 'ERRCfgHst'
        return fnstatus

    def getLocalHost(self):
        "get the hostname of the local machine"
        fnstatus = 'OK'
        result = hostaddr.gethostinfo()
        self.localHost = result[0]
        return fnstatus

    def checkRemoteConnection(self):
        "check to see if remote host is there"
        fnstatus = 'OK'
        command = self.cmdPrefix + "echo $(hostname) ; echo $?" + self.cmdSuffix
        Trace.log(e_errors.INFO, "Shelf cRC Cmd=%s" % (command, ))
        pipeObj = popen2.Popen3(command, 0, 0)
        if pipeObj is None:
            fnstatus = 'ERRPipe'
            return fnstatus
        stat = pipeObj.wait()
        result = pipeObj.fromchild.readlines()  # result has returned string
        Trace.log(e_errors.INFO, "Shelf cRC enrsh return strings=%s stat=%s" % (result, stat))
        if stat == 0:
            retval = result[len(result)-1][0]
            if retval != '0':
                fnstatus = 'ERRHoCmd'
                return fnstatus
        else :
            fnstatus = 'ERRHoNoRe'
            return fnstatus
        return fnstatus

    def checkOCSalive(self):
        "check to see if OCS is alive"
        fnstatus = 'OK'
        command = self.cmdPrefix + "ocs_left_allocated -l 0 ; echo $?"  + self.cmdSuffix
        Trace.log(e_errors.INFO, "Shelf cOa Cmd=%s" % (command,) )
        pipeObj = popen2.Popen3(command, 0, 0)
        if pipeObj is None:
            fnstatus = 'ERRPipe'
            return fnstatus
        stat = pipeObj.wait()
        result = pipeObj.fromchild.readlines()  # result has returned string
        Trace.log(e_errors.INFO, "Shelf cOa enrsh return strings=%s stat=%s" % (result, stat))
        if stat == 0:
            retval = result[len(result)-1][0]
            if retval != '0':
                fnstatus = 'ERROCSCmd'
                return fnstatus
        else :
            fnstatus = 'ERRHoNoRe'
            return fnstatus
        return fnstatus

    def allocateOCSdrive(self, drive):
        "allocate an OCS managed drive"
        fnstatus = 'OK'
        command = self.cmdPrefix + "ocs_allocate -T " + drive + " ; echo $?" + self.cmdSuffix
        Trace.log(e_errors.INFO, "Shelf aOd Cmd=%s" % (command,) )
        pipeObj = popen2.Popen3(command, 0, 0)
        if pipeObj is None:
            fnstatus = 'ERRAloPip'
            return fnstatus
        stat = pipeObj.wait()
        result = pipeObj.fromchild.readlines()  # result has returned string
        Trace.log(e_errors.INFO, "Shelf aOd enrsh return strings=%s stat=%s" % (result, stat))
        if stat == 0:
            retval = result[len(result)-1][0]
            if retval != '0':
                fnstatus = 'ERRAloCmd'
                return fnstatus
            else :   # check if OCS allocated a different drive
                retstring = result[0]
                pos=string.find(retstring," "+drive)
                if pos == -1 :  # different drive was allocated
                    fnstatus = 'ERRAloDrv'
                    pos=string.find(retstring," ")
                    if pos != -1 :
                        wrongdrive=string.strip(retstring[pos+1:])
                        Trace.log(e_errors.ERROR, "ERROR:Shelf aOd enrsh wrongdrive=%s" % (wrongdrive,) )
                    fnstatusR = self.deallocateOCSdrive(drive)
                    return fnstatus
        else :
            fnstatus = 'ERRAloRsh'
            return fnstatus
        return fnstatus

    def mountOCSdrive(self, external_label, drive):
        "request an OCS managed tape"
        fnstatus = 'OK'
        command = self.cmdPrefix + "ocs_request -t " + drive + \
                  " -v " + external_label + " ; echo $?" + self.cmdSuffix
        Trace.log(e_errors.INFO, "Shelf mOd Cmd=%s" % (command,) )
        pipeObj = popen2.Popen3(command, 0, 0)
        if pipeObj is None:
            fnstatus = 'ERRReqPip'
            fnstatusR = self.deallocateOCSdrive(drive)
            return fnstatus
        stat = pipeObj.wait()
        result = pipeObj.fromchild.readlines()  # result has returned string
        Trace.log(e_errors.INFO, "Shelf mOd enrsh return strings=%s stat=%s" % (result, stat))
        if stat == 0:
            retval = result[len(result)-1][0]
            if retval != '0':
                fnstatus = 'ERRReqCmd'
                fnstatusR = self.deallocateOCSdrive(drive)
                return fnstatus
        else :
            fnstatus = 'ERRReqRsh'
            fnstatusR = self.deallocateOCSdrive(drive)
            return fnstatus
        return fnstatus

    def deallocateOCSdrive(self, drive):
        "deallocate an OCS managed drive"
        fnstatus = 'OK'
        if "AllTheTapeDrives" == drive :
            command = self.cmdPrefix + "ocs_deallocate -a " + \
                      " ; echo $?" + self.cmdSuffix
        else :
            command = self.cmdPrefix + "ocs_deallocate -t " + drive + \
                      " ; echo $?" + self.cmdSuffix
        Trace.log(e_errors.INFO, "Shelf dOd Cmd=%s" % (command,) )
        pipeObj = popen2.Popen3(command, 0, 0)
        if pipeObj is None:
            fnstatus = 'ERRDeaPip'
            return fnstatus
        stat = pipeObj.wait()
        result = pipeObj.fromchild.readlines()  # result has returned string
        Trace.log(e_errors.INFO, "Shelf dOd enrsh return strings=%s stat=%s" % (result, stat))
        if stat == 0:
            retval = result[len(result)-1][0]
            if retval != '0': #check if drive already deallocated (not an error)
                retstring = result[0]
                pos=string.find(retstring,"drive is already deallocated")
                if pos == -1 :  # really an error
                    fnstatus = 'ERRDeaCmd'
                    return fnstatus
        else :
            fnstatus = 'ERRDeaRsh'
            return fnstatus
        return fnstatus

    def unmountOCSdrive(self, drive):
        "dismount an OCS managed tape"
        fnstatus = 'OK'
        command = self.cmdPrefix + "ocs_dismount -t " + drive + \
                  " ; echo $?" + self.cmdSuffix
        Trace.log(e_errors.INFO, "Shelf uOd Cmd=%s" % (command,) )
        pipeObj = popen2.Popen3(command, 0, 0)
        if pipeObj is None:
            fnstat = 'ERRDsmPip'
            return fnstatus
        stat = pipeObj.wait()
        result = pipeObj.fromchild.readlines()  # result has returned string
        Trace.log(e_errors.INFO, "Shelf uOd enrsh return strings=%s stat=%s" % (result, stat))
        if stat == 0:
            retval = result[len(result)-1][0]
            if retval != '0':
                fnstatus = 'ERRDsmCmd'
                return fnstatus
        else :
            fnstatus = 'ERRDsmRsh'
            return fnstatus
        return fnstatus

    def load(self, external_label, drive, media_type):
        "load a tape"
        fnstatus = self.allocateOCSdrive(drive)
        if fnstatus == 'OK' :
            fnstatus = self.mountOCSdrive(external_label, drive)
        if fnstatus == 'OK' :
            status = 0
        else :
            status = 1
            Trace.log(e_errors.ERROR, "ERROR:Shelf load exit fnst=%s %s %s" %
                      (status, fnstatus, self.status_message(fnstatus)))
        return self.status_code(fnstatus), status, self.status_message(fnstatus)

    def unload(self, external_label, drive, media_type):
        "unload a tape"
        fnstatusTmp = self.unmountOCSdrive(drive)
        fnstatus = self.deallocateOCSdrive(drive)
        Trace.log(e_errors.INFO, "Shelf unload deallocate exit fnstatus=%s" % (fnstatus,))
        if fnstatusTmp != 'OK' :
            Trace.log(e_errors.ERROR, "ERROR:Shelf unload deall exit fnst= %s %s" %
                      (fnstatus, self.status_message(fnstatus)))
            fnstatus = fnstatusTmp
        if fnstatus == 'OK' :
            status = 0
        else :
            status = 1
            Trace.log(e_errors.ERROR, "ERROR:Shelf unload exit fnst= %s %s" %
                      (fnstatus, self.status_message(fnstatus)))
        return self.status_code(fnstatus), status, self.status_message(fnstatus)

    def getNretry(self):
        numberOfRetries = 1
        return numberOfRetries


class MediaLoaderInterface(generic_server.GenericServerInterface):

    def __init__(self):
        # fill in the defaults for possible options
        self.max_work=7
        generic_server.GenericServerInterface.__init__(self)

    media_options = {
	    option.LOG:{option.HELP_STRING:"",
			option.VALUE_USAGE:option.REQUIRED,
			option.VALUE_TYPE:option.STRING,
			option.USER_LEVEL:option.ADMIN
			},
	    option.MAX_WORK:{option.HELP_STRING:"",
			     option.VALUE_USAGE:option.REQUIRED,
			     option.VALUE_TYPE:option.INTEGER,
			     option.USER_LEVEL:option.ADMIN
			     },
	    }

    def valid_dictionaries(self):
	    return (self.media_options,) + \
		 generic_server.GenericServerInterface.valid_dictionaries(self)

    # parse the options like normal but make sure we have a media changer
    def parse_options(self):
        option.Interface.parse_options(self)
        # bomb out if we don't have a media_changer
        if len(self.args) < 1 :
            self.missing_parameter(self.parameters())
            self.print_help(),
            sys.exit(1)
        else:
            self.name = self.args[0]


if __name__ == "__main__":   # pragma: no cover
    Trace.init("MEDCHANGER")
    Trace.trace(6, "media changer called with args: %s"%(sys.argv,) )

    # get an interface
    intf = MediaLoaderInterface()

    csc  = configuration_client.ConfigurationClient((intf.config_host,
                                                     intf.config_port) )
    keys = csc.get(intf.name)
    try:
        mc_type = keys['type']
    except:
        exc,msg,tb=sys.exc_info()
        Trace.log(e_errors.ERROR, "MC Error %s %s"%(exc,msg))
        sys.exit(1)

    import __main__
    constructor=getattr(__main__, mc_type)
    mc = constructor(intf.name, intf.max_work, (intf.config_host, intf.config_port))

    mc.handle_generic_commands(intf)

    while 1:
        try:
            Trace.log(e_errors.INFO, "Media Changer %s (re) starting"%(intf.name,))
            mc.serve_forever()
        except SystemExit, exit_code:
            sys.exit(exit_code)
        except:
            mc.serve_forever_error("media changer")
            continue
    Trace.log(e_errors.ERROR,"Media Changer finished (impossible)")
