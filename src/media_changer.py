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
import string
import socket
import time
import hostaddr

# enstore imports
import configuration_client
import dispatching_worker
import generic_server
import interface
import Trace
import e_errors
import timer_task
import volume_clerk_client

# media loader template class
class MediaLoaderMethods(dispatching_worker.DispatchingWorker,
	                 generic_server.GenericServer,
			 timer_task.TimerTask):
    work_list = []
    work_cleaning_list = []

    def __init__(self, medch, maxwork, csc):
        self.name = medch
        self.name_ext = "MC"
        generic_server.GenericServer.__init__(self, csc, medch)
        Trace.init(self.log_name)
        self.MaxWork = maxwork
	self.workQueueClosed = 0
        self.insertRA = None
        #   pretend that we are the test system
        #   remember, in a system, there is only one bfs
        #   get our port and host from the name server
        #   exit if the host is not this machine
        self.mc_config = self.csc.get(medch)
        dispatching_worker.DispatchingWorker.__init__(self, \
	                 (self.mc_config['hostip'], self.mc_config['port']))
        self.idleTimeLimit = 600  # default idle time in seconds
        self.lastWorkTime = time.time()
	self.robotNotAtHome = 1
        self.timeInsert = time.time()
	timer_task.TimerTask.__init__(self, 10)
	timer_task.msg_add(180, self.checkMyself) # initial check time in seconds

    def checkMyself(self):
        pass
	#timer_task.msg_add(180, self.checkMyself) # recheck time in seconds
	
    # wrapper method for client - server communication
    def loadvol(self, ticket):        
        ticket["function"] = "mount"
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
	    ticket["status"] = (e_errors.WRONGPARAMETER, 1, "new library name not specified")
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

    def maxwork(self,ticket):
        self.MaxWork = ticket["maxwork"]
        self.reply_to_caller({'status' : (e_errors.OK, 0, None)})

    def getwork(self,ticket):
        if 0: print ticket #lint fix
        result = []
        for i in self.work_list:
            result.append((i['function'], i['vol_ticket']['external_label'], i['drive_id']))
        self.reply_to_caller({'status' : (e_errors.OK, 0, None),'worklist':result})

    # load volume into the drive;  default, overridden for other media changers
    def load(self,
             external_label,    # volume external label
             drive,             # drive id
             media_type):	# media type
        if 0: print media_type #lint fix
	if 'delay' in self.mc_config.keys() and self.mc_config['delay']:
	    # YES, THIS BLOCK IS FOR THE DEVELOPMENT ENVIRONMENT AND THE
	    # OUTPUT OF THE PRINTS GO TO THE TERMINAL
	    print "make sure tape %s in in drive %s"%(external_label,drive)
	    time.sleep( self.mc_config['delay'] )
	    print 'continuing with reply'
	return (e_errors.OK, 0, None)

    # unload volume from the drive;  default overridden for other media changers
    def unload(self,
               external_label,  # volume external label
               drive,           # drive id
	       media_type):     # media type
        if 0: print media_type #lint fix
	if 'delay' in self.mc_config.keys() and self.mc_config['delay']:
            Trace.log(e_errors.INFO,
                      "remove tape "+external_label+" from drive "+drive)
	    time.sleep( self.mc_config['delay'] )
	return (e_errors.OK, 0, None)

    # view volume in the drive;  default overridden for other media changers
    #def view(self,
    #           external_label,  # volume external label
    #       media_type):         # media type
    #    if 0: print media_type #lint fix
    #    return (e_errors.OK, 0, None, 'O') # return 'O' - occupied aka unmounted

    # getVolState in the drive;  default overridden for other media changers - to replace above tgj1
    def getVolState(self, ticket):
        if 0: print media_type #lint fix
        return (e_errors.OK, 0, None, 'O') # return 'O' - occupied aka unmounted

    # insert volume into the robot;  default overridden for other media changers
    def insert(self,ticket):
        if 0: print media_type #lint fix
        return (e_errors.OK, 0, None, '') # return '' - no inserted volumes

    # eject volume from the robot;  default overridden for other media changers
    def eject(self,ticket):
        if 0: print media_type #lint fix
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
        if 0: print external_label, drive, media_type, self.keys()
        pass

    def doWaitingInserts(self):
        pass
        return (e_errors.OK, 0, None) 

    def doWaitingCleaningCycles(self):
        ticket["function"] = "waitingCleanCycle"
        return self.DoWork( self.waitingCleanCycle, ticket)

    def getNretry(self):
        numberOfRetries = 2
        return numberOfRetries

    # Do the forking and call the function
    def DoWork(self, function, ticket):
	if ticket['function'] == "mount" or ticket['function'] == "dismount":
            Trace.log(e_errors.INFO, 'REQUESTED '+ticket['function']+ ' ' + \
		   ticket['vol_ticket']['external_label']+ ' ' +ticket['drive_id'])
            # if drive is doing a clean cycle, drop request
            for i in self.work_list:
	        if i['function'] == "cleanCycle" and i['drive_id'] == ticket['drive_id']:
                    Trace.log(e_errors.INFO, 'REQUESTED '+ticket['function']+ ' request of ' + \
		            ticket['vol_ticket']['external_label']+ ' in ' +ticket['drive_id'] + \
			    ' dropped, drive in cleaning cycle.')
                    return
        else:
            Trace.log(e_errors.INFO, 'REQUESTED '+ticket['function'])
	#put cleaningCyles on cleaning list
	if ticket['function'] == "cleanCycle":
            ticket["ra"] = (self.reply_address,self.client_number,self.current_id)
	    todo = (ticket,function)
	    self.work_cleaning_list.append(todo)
	
        #if we have max number of working children, assume client will resend
	# let work list length exceed MaxWork for cleaningCycle
        if len(self.work_list) >= self.MaxWork:
            Trace.log(e_errors.INFO,
                      "MC Overflow: "+ repr(self.MaxWork) + " " +\
		      ticket['function'])
        #elif work queue is temporarily closed, assume client will resend
        elif self.workQueueClosed and len(self.work_list)>0:
            Trace.log(e_errors.INFO,
                      "MC Queue Closed: " + ticket['function'] + " " + repr(len(self.work_list)))
        # otherwise, we can process work
        else:
            #Trace.log(e_errors.INFO, "DOWORK "+repr(ticket))
            # set the reply address - note this could be a general thing in dispatching worker
            ticket["ra"] = (self.reply_address,self.client_number,self.current_id)
            # bump other requests for cleaning
	    if len(self.work_cleaning_list) > 0:
	        if ticket['function'] != "waitingCleanCycle":
	            Trace.log(e_errors.INFO,
                      "MC: "+ ticket['function'] + " bumped for cleaning")
	        ticket = self.work_cleaning_list[0][0]
	        self.work_cleaning_list.remove(ticket)
	        function = self.work_cleaning_list[0][1]
                Trace.log(e_errors.INFO, 'REPLACEMENT '+ticket['function'])
            # if this a duplicate request, drop it
            for i in self.work_list:
                if i["ra"] == ticket["ra"]:
                    return
	    # if function is insert and queue not empty, close work queue
            if ticket["function"] == "insert":
	        if len(self.work_list)>0:
	           self.workQueueClosed = 1
		   self.timeInsert = time.time()
		   self.insertRA = ticket["ra"]
		   return
		else:
		   self.workQueueClosed = 0
            # if not duplicate, fork the work
            pipe = os.pipe()
            if not self.fork():
                # if in child process
	        if ticket['function'] == "mount" or ticket['function'] == "dismount":
                    Trace.log(e_errors.INFO, 'mcDoWork>>> forked (child) '+ticket['function']+ \
		            ' ' +ticket['vol_ticket']['external_label']+ ' ' +ticket['drive_id'])
                else:
                    Trace.log(e_errors.INFO, 'mcDoWork>>> '+ticket['function'])
                os.close(pipe[0])
		# do the work ...
                # ... if this is a mount, dismount first
                if ticket['function'] == "mount":
                    Trace.trace(e_errors.INFO, 'mcDoWork>dismount for mount (prepare)')
		    sts=self.prepare(
                        ticket['vol_ticket']['external_label'],
                        ticket['drive_id'],
                        ticket['vol_ticket']['media_type'])
                count = self.getNretry()
		rpcErrors = 0
                sts=("",0,"")
		while count > 0 and sts[0] != e_errors.OK:
		    if ticket['function'] == 'insert':
			sts = function(ticket)
		    elif ticket['function'] == 'eject':
			sts = function(ticket)
		    elif ticket['function'] == 'homeAndRestart':
			sts = function(ticket)
		    elif ticket['function'] == 'cleanCycle':
			sts = function(ticket)
		    elif ticket['function'] == 'getVolState':
			sts = function(ticket)
		    else:
		        sts = function(
			    ticket['vol_ticket']['external_label'],
			    ticket['drive_id'],
			    ticket['vol_ticket']['media_type'])
		    if sts[1] == 1 and rpcErrors < 10:  # RPC failure
		        time.sleep(5)
			rpcErrors = rpcErrors + 1
                        Trace.trace(e_errors.ERROR, 'mcDoWork >>> RPC error, count= :'+repr(rpcErrors)+' '+repr(count)+' '+repr(sts[2]))
		    else:
			count = count - 1
                # send status back to MC parent via pipe then via dispatching_worker and WorkDone ticket
                Trace.trace(10, 'mcDoWork<<< sts'+repr(sts))
                ticket["work"]="WorkDone"	# so dispatching_worker calls WorkDone
                ticket["status"]=sts
                msg = repr(('0','0',ticket))
                bytecount = "%08d" % len(msg)
                os.write(pipe[1], bytecount)
                os.write(pipe[1], msg)
                os.close(pipe[1])
                os._exit(0)

            # else, this is the parent
            else:
                self.add_select_fd(pipe[0])
                os.close(pipe[1])
                # add entry to outstanding work 
                self.work_list.append(ticket)
                Trace.trace(10, 'mcDoWork<<< Parent')
    
    def WorkDone(self, ticket):
        # dispatching_worker sends "WorkDone" ticket here and we reply_to_caller
        # remove work from outstanding work list
        for i in self.work_list:
           if i["ra"] == ticket["ra"]:
              self.work_list.remove(i)
              break
        # report back to original client - probably a mover
	if ticket['function'] == "mount" or ticket['function'] == "dismount":
            Trace.log(e_errors.INFO, 'FINISHED '+ticket['function']+ ' ' +\
		   ticket['vol_ticket']['external_label']+ ' ' +ticket['drive_id'])
        else:
            Trace.log(e_errors.INFO, 'FINISHED '+ticket['function'])
        # reply_with_address uses the "ra" entry in the ticket
        self.reply_with_address(ticket)
	self.robotNotAtHome = 1
        self.lastWorkTime = time.time()
	# check for cleaning jobs
	if len(self.work_list) < self.MaxWork and len(self.work_cleaning_list) > 0:
            sts = self.doWaitingCleaningCycles()
 	# if work queue is closed and work_list is empty, do insert
	sts = self.doWaitingInserts()

# aml2 robot loader server
class AML2_MediaLoader(MediaLoaderMethods):
    def __init__(self, medch, maxwork=10, csc=None):
        MediaLoaderMethods.__init__(self, medch, maxwork, csc)

	# robot choices are 'R1', 'R2' or 'Both'
	if self.mc_config.has_key('RobotArm'):   # error if robot not in config
	    self.robotArm = string.strip(self.mc_config['RobotArm'])
	else:
            Trace.log(e_errors.ERROR, "ERROR:aml2 no robot arm key in configuration")
	    self.robotArm = string.strip(self.mc_config['RobotArm']) # force the exception          
	    return

	if self.mc_config.has_key('IOBoxMedia'):   # error if IO box media assignments not in config
	    self.mediaIOassign = self.mc_config['IOBoxMedia']
	else:
            Trace.log(e_errors.ERROR, "ERROR:aml2 no IO box media assignments in configuration")
	    self.mediaIOassign = self.mc_config['IOBoxMedia'] # force the exception
	    return

	if self.mc_config.has_key('DriveCleanTime'):   # error if DriveCleanTime assignments not in config
	    self.driveCleanTime = self.mc_config['DriveCleanTime']
	else:
            Trace.log(e_errors.ERROR, "ERROR:aml2 no DriveCleanTime assignments in configuration")
	    self.driveCleanTime = self.mc_config['DriveCleanTime'] # force the exception
	    return

	if self.mc_config.has_key('CleanTapeFileFamily'):   # error if DriveCleanTime assignments not in config
	    self.cleanTapeFileFamily = self.mc_config['CleanTapeFileFamily']  # expected format is "externalfamilyname.wrapper"
	    try:
	        self.cleanTapeFileWrapper = string.split(self.cleanTapeFileFamily,'.')[1]
            except IndexError:
	        Trace.log(e_errors.ERROR, "ERROR:aml2 bad CleanTapeFileFamily in configuration file")
	        self.cleanTapeFileWrapper = string.split(self.cleanTapeFileFamily,'.')[1] # force error
	else:
            Trace.log(e_errors.ERROR, "ERROR:aml2 no CleanTapeFileFamily assignments in configuration")
	    self.cleanTapeFileFamily = self.mc_config['CleanTapeFileFamily'] # force the exception
	    return

	if self.mc_config.has_key('IdleTimeHome'):
	    temp = self.mc_config['IdleTimeHome']
            if type(temp) == types.IntType:
	        if temp < 20:   # wait at least 20 seconds
	            self.idleTimeLimit = self.mc_config['IdleTimeHome']
                    Trace.log(e_errors.ERROR, "ERROR:aml2 IdleHomeTimeTooSmall(>20), default used")
		else:
	            self.idleTimeLimit = self.mc_config['IdleTimeHome']
	    else:
                Trace.log(e_errors.ERROR, "ERROR:aml2 IdleHomeTimeNotAnInt, default used")

	import aml2
        self.load=aml2.mount
        self.unload=aml2.dismount
        self.prepare=aml2.dismount
        self.robotHome=aml2.robotHome
        self.robotStatus=aml2.robotStatus
        self.robotStart=aml2.robotStart

    def insert(self, ticket):
        import aml2
	self.insertRA = None
        classTicket = { 'mcSelf' : self }
	ticket['timeOfCmd'] = time.time()
	ticket['medIOassign'] = self.mediaIOassign
	rt = aml2.insert(ticket, classTicket)
        return rt
	
    def eject(self, ticket):
        import aml2
        classTicket = { 'mcSelf' : self }
	ticket['medIOassign'] = self.mediaIOassign
	rt = aml2.eject(ticket, classTicket)
        return rt

    def robotHomeAndRestart(self, ticket):
        import aml2
        classTicket = { 'mcSelf' : self }
	ticket['robotArm'] = self.robotArm
	rt = aml2.robotHomeAndRestart(ticket, classTicket)
        return rt
    
    def getVolState(self, ticket):
	"get current state of the tape"
        import aml2
	external_label = ticket['external_label']
	media_type = ticket['media_type']
	rt = aml2.view(external_label, media_type)
        if 'O' == rt[5] :
          state = 'O'
        elif 'M' == rt[5] :
          state = 'M'
        else :
          state = rt[5]
        return (rt[0], rt[1], rt[2], state)
	
    def doCleaningCycle(self, inTicket):
        """ do drive cleaning cycle """
        import aml2
        classTicket = { 'mcSelf' : self }
	ticket = {}
        try:
            ticket['drive'] = inTicket['moverConfig']['device']
        except KeyError:
            Trace.log(e_errors.ERROR, 'aml2 no device field found in ticket.')
	    status = 1
            return "ERROR", status, "no device field found in ticket"
        try:
	    ticket['media_type'] = inTicket['vol_info']['media_type']
        except KeyError:
            Trace.log(e_errors.ERROR, 'aml2 no media_type field found in ticket.')
	    status = 1
            return "ERROR", status, "no media_type field found in ticket"
	
	driveType = ticket['drive'][:2]  # ... need device type, not actual device
        ticket['cleanTime'] = self.driveCleanTime[driveType][0]  # clean time in seconds	
        driveCleanCycles = self.driveCleanTime[driveType][1]  # number of cleaning cycles
	
        vcc = inTicket['vcc']
	if type(vcc) == types.StringType:
	    cleaningVolume = vcc
	else:
	    min_remaining_bytes = 1
	    wrapper = self.cleanTapeFileWrapper
	    vol_veto_list = []
	    first_found = 0
	    cleaningVolume = vcc.next_write_volume(inTicket['moverConfig']['library'],
	                      min_remaining_bytes, self.cleanTapeFileFamily, wrapper, 
			      vol_veto_list, first_found)  # get which volume to use
	ticket['volume'] = cleaningVolume
	
        Trace.log(e_errors.LOG, 'mc: ticket='+repr(ticket))
	for i in range(driveCleanCycles):
	    rt = aml2.cleanADrive(ticket, classTicket)
	if type(vcc) != types.StringType:
	    retTicket = vcc.get_remaining_bytes(cleaningVolume)
	    remaining_bytes = retTicket['remaining_bytes']-1
	    vcc.set_remaining_bytes(cleaningVolume,remaining_bytes,'\0',0,0,0,0,None)
        return (e_errors.OK, 0, None)

    def doWaitingInserts(self):
        """ do delayed insertvols"""
	if self.workQueueClosed and len(self.work_list)==0:
	    self.workQueueClosed = 0
	    ticket = { 'function'  : 'insert',
	               'timeOfCmd' : self.timeInsert,
		       'ra'        : self.insertRA }
	    self.DoWork( self.insert, ticket)
        return (e_errors.OK, 0, None) 

    def checkMyself(self):
        """ do regularily scheduled internal checks"""
	if self.robotNotAtHome and (time.time()-self.lastWorkTime) > self.idleTimeLimit:
	    self.robotNotAtHome = 0
            ticket = { 'function' : 'homeAndRestart', 'robotArm' : self.robotArm }
	    sts = self.robotHomeAndRestart(ticket)
	    self.lastWorkTime = time.time()
	timer_task.msg_add(29, self.checkMyself) # recheck time in seconds

# STK robot loader server
class STK_MediaLoader(MediaLoaderMethods):
    def __init__(self, medch, maxwork=10, csc=None):
        MediaLoaderMethods.__init__(self,medch,maxwork,csc)
        import STK
        self.load=STK.mount
        self.unload=STK.dismount
        self.prepare=STK.dismount

# manual media changer
class Manual_MediaLoader(MediaLoaderMethods):
    def __init__(self, medch, maxwork=10, csc=None):
        MediaLoaderMethods.__init__(self,medch,maxwork,csc)
    def loadvol(self, ticket):
        if ticket['vol_ticket']['external_label']:
            os.system("mc_popup 'Please load %s'"%ticket['vol_ticket']['external_label'])
        return MediaLoaderMethods.loadvol(self,ticket)
    def unloadvol(self, ticket):
        if ticket['vol_ticket']['external_label']:
            os.system("mc_popup 'Please unload %s'"%ticket['vol_ticket']['external_label'])
        return MediaLoaderMethods.unloadvol(self,ticket)

    
# Raw Disk and stand alone tape media server
class RDD_MediaLoader(MediaLoaderMethods):
    def __init__(self, medch, maxwork=1, csc=None):
        MediaLoaderMethods.__init__(self,medch,maxwork,csc)


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
    status_message = {
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

    def __init__(self, medch, maxwork=1, csc=None): #Note: maxwork may need to be changed, tgj
        MediaLoaderMethods.__init__(self,medch,maxwork,csc)
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
	        self.cmdPrefix = "rsh " + self.ocsHost + " '"
	        self.cmdSuffix = "'"
                fnstatus = self.checkRemoteConnection()
		if fnstatus != 'OK' :
                    Trace.log(e_errors.ERROR, "ERROR:Shelf init %s %s" % (fnstatus, self.status_message[fnstatus][1]) )
		    return
	else :
            Trace.log(e_errors.ERROR, "ERROR:Shelf init %s %s" % (fnstatusR, self.status_message[fnstatusR][1]) )
            Trace.log(e_errors.ERROR, "ERROR:Shelf init %s %s" % (fnstatus, self.status_message[fnstatus][1]) )
	    return
        fnstatus = self.checkOCSalive()
        if fnstatus != 'OK' :
             Trace.log(e_errors.ERROR, "ERROR:Shelf init %s %s" % (fnstatus, self.status_message[fnstatus][1]) )
             return
     	#fnstatus = self.deallocateOCSdrive("AllTheTapeDrives")
        #if fnstatus != 'OK' :
        #     Trace.log(e_errors.ERROR, "ERROR:Shelf init %s %s" % (fnstatus, self.status_message[fnstatus][1]) )
        #     return
        Trace.log(e_errors.INFO, "Shelf init %s %s" % (fnstatus, self.status_message[fnstatus][1]) )
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
        command = self.cmdPrefix + "echo $(hostname) ; echo $?" \
	          + self.cmdSuffix
        Trace.log(e_errors.INFO, "Shelf cRC Cmd=%s" % command )
        pipeObj = popen2.Popen3(command, 0, 0)
	if pipeObj is None:
	    fnstatus = 'ERRPipe'
            return fnstatus
	stat = pipeObj.wait()
	result = pipeObj.fromchild.readlines()  # result has returned string
        Trace.log(e_errors.INFO, "Shelf cRC rsh return strings=%s stat=%s" % (result, stat))
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
        command = self.cmdPrefix + "ocs_left_allocated -l 0 ; echo $?" \
	          + self.cmdSuffix
        Trace.log(e_errors.INFO, "Shelf cOa Cmd=%s" % command )
        pipeObj = popen2.Popen3(command, 0, 0)
	if pipeObj is None:
	    fnstatus = 'ERRPipe'
            return fnstatus
	stat = pipeObj.wait()
	result = pipeObj.fromchild.readlines()  # result has returned string
        Trace.log(e_errors.INFO, "Shelf cOa rsh return strings=%s stat=%s" % (result, stat))
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
	command = self.cmdPrefix + "ocs_allocate -T " + drive + \
	          " ; echo $?" + self.cmdSuffix
        Trace.log(e_errors.INFO, "Shelf aOd Cmd=%s" % command )
	pipeObj = popen2.Popen3(command, 0, 0)
	if pipeObj is None:
	    fnstatus = 'ERRAloPip'
            return fnstatus
	stat = pipeObj.wait()
	result = pipeObj.fromchild.readlines()  # result has returned string
        Trace.log(e_errors.INFO, "Shelf aOd rsh return strings=%s stat=%s" % (result, stat))
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
                        Trace.log(e_errors.ERROR, "ERROR:Shelf aOd rsh wrongdrive=" % wrongdrive )
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
        Trace.log(e_errors.INFO, "Shelf mOd Cmd=%s" % command )
	pipeObj = popen2.Popen3(command, 0, 0)
	if pipeObj is None:
	    fnstatus = 'ERRReqPip'
	    fnstatusR = self.deallocateOCSdrive(drive)
            return fnstatus
	stat = pipeObj.wait()
	result = pipeObj.fromchild.readlines()  # result has returned string
        Trace.log(e_errors.INFO, "Shelf mOd rsh return strings=%s stat=%s" % (result, stat))
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
        Trace.log(e_errors.INFO, "Shelf dOd Cmd=%s" % command )
	pipeObj = popen2.Popen3(command, 0, 0)
	if pipeObj is None:
	    fnstatus = 'ERRDeaPip'
            return fnstatus
	stat = pipeObj.wait()
	result = pipeObj.fromchild.readlines()  # result has returned string
        Trace.log(e_errors.INFO, "Shelf dOd rsh return strings=%s stat=%s" % (result, stat))
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
        Trace.log(e_errors.INFO, "Shelf uOd Cmd=%s" % command )
	pipeObj = popen2.Popen3(command, 0, 0)
	if pipeObj is None:
	    fnstat = 'ERRDsmPip'
            return fnstatus
	stat = pipeObj.wait()
	result = pipeObj.fromchild.readlines()  # result has returned string
        Trace.log(e_errors.INFO, "Shelf uOd rsh return strings=%s stat=%s" % (result, stat))
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
            Trace.log(e_errors.ERROR, "ERROR:Shelf load exit fnst=%s %s %s" % (status, fnstatus, self.status_message[fnstatus][1]) )
        return self.status_message[fnstatus][0], status, self.status_message[fnstatus][1]

    def unload(self, external_label, drive, media_type):
	"unload a tape"
	fnstatusTmp = self.unmountOCSdrive(drive)
     	fnstatus = self.deallocateOCSdrive(drive)
        Trace.log(e_errors.INFO, "Shelf unload deallocate exit fnstatus=%s" % fnstatus)
	if fnstatusTmp != 'OK' :
            Trace.log(e_errors.ERROR, "ERROR:Shelf unload deall exit fnst=%s %s %s" % (status, fnstatus, self.status_message[fnstatus][1]) )
	    fnstatus = fnstatusTmp
	if fnstatus == 'OK' :
	    status = 0
	else :
	    status = 1
            Trace.log(e_errors.ERROR, "ERROR:Shelf unload exit fnst=%s %s %s" % (status, fnstatus, self.status_message[fnstatus][1]) )
        return self.status_message[fnstatus][0], status, self.status_message[fnstatus][1]

    def getNretry(self):
        numberOfRetries = 1
        return numberOfRetries

	
class MediaLoaderInterface(generic_server.GenericServerInterface):

    def __init__(self):
        # fill in the defaults for possible options
        self.maxwork=10
        generic_server.GenericServerInterface.__init__(self)

    # define the command line options that are valid
    def options(self):
        return generic_server.GenericServerInterface.options(self)+\
	       ["log=","maxwork="]

    #  define our specific help
    def parameters(self):
        if 0: print self.keys() #lint fix
        return "media_changer"

    # parse the options like normal but make sure we have a media changer
    def parse_options(self):
        interface.Interface.parse_options(self)
        # bomb out if we don't have a media_changer
        if len(self.args) < 1 :
	    self.missing_parameter(self.parameters())
            self.print_help(),
            sys.exit(1)
        else:
            self.name = self.args[0]


if __name__ == "__main__" :
    Trace.init("MEDCHANGER")
    Trace.trace( 6, "media changer called with args: %s"%sys.argv )

    # get an interface
    intf = MediaLoaderInterface()

    csc  = configuration_client.ConfigurationClient((intf.config_host, 
                                                     intf.config_port) )
    keys = csc.get(intf.name)
    try:
	mc_type = keys['type']
    except:
	Trace.log(e_errors.ERROR,
                  "MC Error "+str(sys.exc_info()[0])+str(sys.exc_info()[1]))
	sys.exit(1)

    del(csc)
    # here we need to define what is the class of the media changer
    # and create an object of that class based on the value of args[0]
    # for now there is just one possibility

    mc = eval(mc_type+"("+repr(intf.name)+","+repr(intf.maxwork)+",("+\
              repr(intf.config_host)+","+repr(intf.config_port)+"))")
    while 1:
        try:
            #Trace.init(intf.name[0:5]+'.medc')
            Trace.log(e_errors.INFO, "Media Changer %s (re) starting"%\
                      intf.name)
            mc.serve_forever()
        except:
	    mc.serve_forever_error("media changer")
            continue
    Trace.trace(6,"Media Changer finished (impossible)")
