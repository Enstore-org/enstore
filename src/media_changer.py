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
import time				# sleep
import popen2
import string
import socket

# enstore imports
import configuration_client
import dispatching_worker
import generic_server
import generic_cs
import interface
import log_client
import Trace
import e_errors


# media loader template class
class MediaLoaderMethods(dispatching_worker.DispatchingWorker,
	                 generic_server.GenericServer) :

    work_list = []

    def __init__(self, medch, maxwork, csc=0, verbose=0,\
	         host=interface.default_host(), \
	         port=interface.default_port()):
        self.MaxWork = maxwork
	self.verbose = verbose
	self.print_id = medch
        # get the config server
        configuration_client.set_csc(self, csc, host, port, verbose)
        #   pretend that we are the test system
        #   remember, in a system, there is only one bfs
        #   get our port and host from the name server
        #   exit if the host is not this machine
        self.mc_config = self.csc.get(medch)

	try:
	    self.print_id = self.mc_config['logname']
	except:
	    pass
        Trace.init(self.mc_config["logname"])
        dispatching_worker.DispatchingWorker.__init__(self, \
	                 (self.mc_config['hostip'], self.mc_config['port']))
        # get a logger
        self.logc = log_client.LoggerClient(self.csc, \
	                                    self.mc_config["logname"], \
                                            'logserver', 0)

    # wrapper method for client - server communication
    def loadvol(self, ticket):        
        Trace.trace(10, '>mount')
        ticket["function"] = "mount"
        return self.DoWork( self.load, ticket)

    # wrapper method for client - server communication
    def unloadvol(self, ticket):
        Trace.trace(10, '>dismount')
        ticket["function"] = "dismount"
        return self.DoWork( self.unload, ticket)

    # wrapper method for client - server communication
    def viewvol(self, ticket):
        Trace.trace(10, '>view')
        ticket["status"] = self.view(ticket["vol_ticket"]["external_label"], \
	               ticket["vol_ticket"]["media_type"])
        self.reply_to_caller(ticket)

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
             media_type) :	# media type
        if 0: print media_type #lint fix
	if 'delay' in self.mc_config.keys() and self.mc_config['delay']:
	    self.enprint("make sure tape "+external_label+" is in drive "+drive)
	    time.sleep( self.mc_config['delay'] )
	    self.enprint( 'continuing with reply' )
	return (e_errors.OK, 0, None)

    # unload volume from the drive;  default overridden for other media changers
    def unload(self,
               external_label,  # volume external label
               drive,
	       media_type) :         # drive id
        if 0: print media_type #lint fix
	if 'delay' in self.mc_config.keys() and self.mc_config['delay']:
	    self.enprint("remove tape "+external_label+" from drive "+drive)
	    time.sleep( self.mc_config['delay'] )
	return (e_errors.OK, 0, None)

    # view volume in the drive;  default overridden for other media changers
    def view(self,
               external_label,  # volume external label
	       media_type) :         # drive id 
        if 0: print media_type #lint fix
        return (e_errors.OK, 0, None, 'O') # return 'O' - occupied aka unmounted
	#return (e_errors.OK, 0, None, None)

    # prepare is overridden by dismount for mount; i.e. for tape drives we always dismount before mount
    def prepare(self,
               external_label,
               drive,
               media_type) :        
        if 0: print external_label, drive, media_type, self.keys()
        pass

    # Do the forking and call the function which will be load or unload
    def DoWork(self, function, ticket):

        Trace.trace(10, '>dowork')
        self.logc.send(e_errors.INFO, 2,"REQUESTED "+ticket['function']+" "  +\
                                          ticket['vol_ticket']['external_label']+" "  +\
                                          ticket['drive_id']+" "  +\
                                          ticket['vol_ticket']['media_type'] )
        #if we have max number of working children, assume client will resend
        if len(self.work_list) >= self.MaxWork :
            self.logc.send(e_errors.INFO, 2, "MC Overflow: "+ repr(self.MaxWork) + " " +\
                      ticket['vol_ticket']['external_label'] + " " + ticket['drive_id'])
        # otherwise, we can do this
        else:
            #self.enprint( "DOWORK"+repr(ticket))
            # set the reply address - note this could be a general thing in dispatching worker
            ticket["ra"] = (self.reply_address,self.client_number,self.current_id)
            # if this a duplicate request, drop it
            for i in self.work_list:
                if i["ra"] == ticket["ra"]:
                    return
            # if not duplicate, fork the work
            pipe = os.pipe()
            if not os.fork() :
                # if in child process
                Trace.trace(10, '>forked')
                #self.enprint( "FORKED"+repr(ticket))
                os.close(pipe[0])
                # do the work, if this is a mount, dismount first
                if ticket['function'] == "mount":
                    Trace.trace(10, '>dismount for mount')
                    #self.enprint( "PREPARE"+repr(ticket))
		    sts=self.prepare(
                        ticket['vol_ticket']['external_label'],
                        ticket['drive_id'],
                        ticket['vol_ticket']['media_type'])

                Trace.trace(10, '>>> '+ticket['function'])
                #self.enprint( "MOUNT"+repr(ticket))
                count=2
                sts=("",0)
		while count > 0 and sts[0] != e_errors.OK:
		    count = count - 1
		    sts = function(
			ticket['vol_ticket']['external_label'],
			ticket['drive_id'],
			ticket['vol_ticket']['media_type'])
                # send status back to MC parent via pipe then via dispatching_worker and WorkDone ticket
                #self.enprint( "STS"+repr(ticket))
                Trace.trace(10, '<<< sts'+repr(sts))
                ticket["work"]="WorkDone"			# so dispatching_worker calls WorkDone
                ticket["status"]=sts
                os.write(pipe[1], repr(('0','0',ticket) ))
                os.close(pipe[1])
                os._exit(0)

            # else, this is the parent
            else:
                self.add_select_fd(pipe[0])
                os.close(pipe[1])
                # add entry to outstanding work 
                self.work_list.append(ticket)
                Trace.trace(10, '<<< Parent')
    
    def WorkDone(self, ticket):
        # dispatching_worker sends "WorkDone" ticket here and we reply_to_caller
        # remove work from outstanding work list
        for i in self.work_list:
           if i["ra"] == ticket["ra"]:
              self.work_list.remove(i)
              break
        self.logc.send(e_errors.INFO, 2,"FINISHED "+ticket['function']+" "  +\
                                          ticket['vol_ticket']['external_label']+" "  +\
                                          ticket['drive_id']+" "  +\
                                          repr(ticket['status']) )
        # report back to original client - probably a mover
        Trace.trace(10, '<<< WorkDone')
        # reply_with_address uses the "ra" entry in the ticket
        self.reply_with_address(ticket)

# EMASS robot loader server
class EMASS_MediaLoader(MediaLoaderMethods) :
    def __init__(self, medch, maxwork=10,csc=0, verbose=0,\
                 host=interface.default_host(), \
                 port=interface.default_port()):
        MediaLoaderMethods.__init__(self,medch,maxwork,csc,verbose,host,port)
        import EMASS
        self.load=EMASS.mount
        self.unload=EMASS.dismount
        self.prepare=EMASS.dismount

    def view(self, external_label, media_type):
	"get current state of the tape"
        import EMASS
	rt = EMASS.view(external_label, media_type)
        if 'O' == rt[5] :
          state = 'O'
        elif 'M' == rt[5] :
          state = 'M'
        else :
          state = rt[5]
        return (rt[0], rt[1], rt[2], state)


# STK robot loader server
class STK_MediaLoader(MediaLoaderMethods) :
    def __init__(self, medch, maxwork=10,csc=0, verbose=0,\
                 host=interface.default_host(), \
                 port=interface.default_port()):
        MediaLoaderMethods.__init__(self,medch,maxwork,csc,verbose,host,port)
        import STK
        self.load=STK.mount
        self.unload=STK.dismount
        self.prepare=STK.dismount

# Raw Disk and stand alone tape media server
class RDD_MediaLoader(MediaLoaderMethods) :
    def __init__(self, medch, maxwork=1, csc=0, verbose=0,\
                 host=interface.default_host(), \
                 port=interface.default_port()):
        MediaLoaderMethods.__init__(self,medch,maxwork,csc,verbose,host,port)

    def view(self, external_label, media_type):
	"get current state of the tape"
        return (e_errors.OK, 0, None, 'O') # return 'O' - occupied aka unmounted

# "Shelf" manual media server - interfaces with OCS
class Shelf_MediaLoader(MediaLoaderMethods) :
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

    def __init__(self, medch, maxwork=1, csc=0, verbose=0,\
                 host=interface.default_host(), \
                 port=interface.default_port()):
        MediaLoaderMethods.__init__(self,medch,maxwork,csc,verbose,host,port)
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
	result = socket.gethostbyaddr(socket.gethostname())
        Trace.log(e_errors.INFO, "Shelf gLH Rslt=%s" % result[0] )
	localH = result[0]
	index = string.find(localH,".")
	if index > 0 :
	    self.localHost = localH
	elif len(result[1]) > 0 :
	    for localH in result[1] :
                Trace.trace(10, "Shelf gLH Rslt[1]=%s" % localH )
	        index = string.find(localH,".")
                if index > 0 :
	            self.localHost = localH
		    break
            else :
	        fnstatus = 'ERRNoLoHN'
		return fnstatus
	else :
	    fnstatus = 'ERRNoLoHN'
	    return fnstatus
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
	
    def view(self, external_label, media_type):
	"get current state of the tape"
        return (e_errors.OK, 0, None, 'O') # return 'O' - occupied aka unmounted

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
            Trace.log(e_errors.ERROR, "ERROR:Shelf load exit fnst=%s %s" % (fnstatus, self.status_message[fnstatus][1]) )
        return self.status_message[fnstatus][0], status, self.status_message[fnstatus][1]

    def unload(self, external_label, drive, media_type):
	"unload a tape"
	fnstatusTmp = self.unmountOCSdrive(drive)
     	fnstatus = self.deallocateOCSdrive(drive)
        Trace.log(e_errors.INFO, "Shelf unload deallocate exit fnstatus=%s" % fnstatus)
	if fnstatusTmp != 'OK' :
            Trace.log(e_errors.ERROR, "ERROR:Shelf unload deall exit fnst=%s %s" % (fnstatus, self.status_message[fnstatus][1]) )
	    fnstatus = fnstatusTmp
	if fnstatus == 'OK' :
	    status = 0
	else :
	    status = 1
            Trace.log(e_errors.ERROR, "ERROR:Shelf unload exit fnst=%s %s" % (fnstatus, self.status_message[fnstatus][1]) )
        return self.status_message[fnstatus][0], status, self.status_message[fnstatus][1]
	
class MediaLoaderInterface(generic_server.GenericServerInterface):

    def __init__(self):
        Trace.trace(10,'{mlsi.__init__')
        # fill in the defaults for possible options
        self.maxwork=10
        generic_server.GenericServerInterface.__init__(self)
        Trace.trace(10,'}mlsi.__init__')

    # define the command line options that are valid
    def options(self):
        Trace.trace(16, "{}options")
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
    Trace.init("medchanger")
    Trace.trace( 6, "media changer called with args: %s"%sys.argv )

    # get an interface
    intf = MediaLoaderInterface()

    csc  = configuration_client.ConfigurationClient( intf.config_host, 
                                                 intf.config_port, 0 )
    keys = csc.get(intf.name)
    try:
	mc_type = keys['type']
    except:
	generic_cs.enprint("MC Error "+str(sys.exc_info()[0])+\
                     str(sys.exc_info()[1]), generic_cs.SERVER, 1)
	sys.exit(1)

    del(csc)
    # here we need to define what is the class of the media changer
    # and create an object of that class based on the value of args[0]
    # for now there is just one possibility

    mc = eval(mc_type+"("+repr(intf.name)+","+repr(intf.maxwork)+","+repr(0)+","+\
	      repr(intf.verbose)+","+repr(intf.config_host)+","+\
	      repr(intf.config_port)+")")
    while 1:
        try:
            #Trace.init(intf.name[0:5]+'.medc')
            mc.logc.send(e_errors.INFO, 1, "Media Changer"+intf.name+"(re) starting")
            mc.serve_forever()
        except:
	    mc.serve_forever_error("media changer", mc.logc)
            continue
    Trace.trace(1,"Media Changer finished (impossible)")
