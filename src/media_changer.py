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

# enstore imports
import configuration_client
import dispatching_worker
import generic_server
import generic_cs
import interface
import log_client
import time				# sleep
import Trace
import e_errors
import popen2
import string


# media loader template class
class MediaLoaderMethods(dispatching_worker.DispatchingWorker,
	                 generic_server.GenericServer) :

    work_list = []

    def __init__(self, medch, maxwork, csc=0, verbose=0,\
	         host=interface.default_host(), \
	         port=interface.default_port()):
        Trace.trace(10, '{__init__')
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
        Trace.trace(10, '}__init__')

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
	return (e_errors.OK, 0, None, None)

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
        return (e_errors.OK, 0, None, 'O') # return 'O' - occupied aka unmounted

# "Shelf" manual media server - interfaces with OCS
class Shelf_MediaLoader(MediaLoaderMethods) :
    status_table = (
         (e_errors.OK,	"request successful"),                              #0
         (e_errors.MOUNTFAILED, "mc: on OCS"),                              #1
         (e_errors.MOUNTFAILED, "mc: on OCS, no pipeObj"),                  #2
         (e_errors.MOUNTFAILED, "mc: on OCS, OCS command unsuccessful"),    #3
         (e_errors.MOUNTFAILED, "mc: on OCS, drive not available"),         #4
         (e_errors.MOUNTFAILED, "mc: on OCS, rsh error"),                   #5
         (e_errors.DISMOUNTFAILED, "mc: on OCS"),                           #6
         (e_errors.DISMOUNTFAILED, "mc: on OCS, no pipeObj"),               #7
         (e_errors.DISMOUNTFAILED, "mc: on OCS, OCS command unsuccessful"), #8
         (e_errors.DISMOUNTFAILED, "mc: on OCS, rsh error")                 #9
	 )
    remoteHost = "bastet"

    def __init__(self, medch, maxwork=1, csc=0, verbose=0,\
                 host=interface.default_host(), \
                 port=interface.default_port()):
        MediaLoaderMethods.__init__(self,medch,maxwork,csc,verbose,host,port)
	#self.prepare=self.unload  #  override prepare with dismount

    def view(self, external_label, media_type):
        return (e_errors.OK, 0, None, 'O') # return 'O' - occupied aka unmounted

    def allocateOCSdrive(self, remoteHost, drive):
        status = 0
	fnstatus = 0
	command = "rsh " + remoteHost + " 'ocs_allocate -T " + drive + \
	          " ; echo $?' "
        #Trace.trace(7, "media changer rsh Command=", command )
	pipeObj = popen2.Popen3(command, 0, 0)
	if pipeObj is None:
	    status = 1
	    fnstatus = 2
            return status, fnstatus
	stat = pipeObj.wait()
	result = pipeObj.fromchild.readlines()  # result has returned string
        #Trace.trace(7, "media changer rsh return strings=", result, " stat=", stat )
	if stat == 0:
	    retval = result[1][0]
	    if retval != '0':
	        status = 1
	        fnstatus = 3
                return status, fnstatus
	    else :   # check if OCS allocated a different drive
	        retstring = result[0]
		pos=string.find(retstring," "+drive)
		if pos == -1 :
		    status = 1        # different drive was allocated 
		    fnstatus = 4
         	    pos=string.find(retstring,remoteHost)
		    if pos == 0 :
		        wrongdrive=string.strip(retstring[len(remoteHost):])
                        #Trace.trace(7, "media changer rsh wrongdrive=", wrongdrive )
			status, fnstatus = self.deallocateOCSdrive(remoteHost, drive)
                        return status, fnstatus
                    else :
		        status = 1        # returned hostname malformed,
		        fnstatus = 4      # cannot deallocate the drive
                        return status, fnstatus
	else :
	    status = 1
	    fnstatus = 5
        return status, fnstatus
        
    def mountOCSdrive(self, external_label, remoteHost, drive):
        status = 0
	fnstatus = 0
	command = "rsh " + remoteHost + " 'ocs_request -t " + drive + \
	          " -v " + external_label + " ; echo $?' "
        #Trace.trace(7, "media changer rsh Command=", command )
	pipeObj = popen2.Popen3(command, 0, 0)
	if pipeObj is None:
	    status = 1
	    fnstatus = 2
	    statusR, fnstatusR = self.deallocateOCSdrive(remoteHost, drive)
            return status, fnstatus
	stat = pipeObj.wait()
	result = pipeObj.fromchild.readlines()  # result has returned string
        #Trace.trace(7, "media changer rsh return strings=", result, " stat=", stat )
	if stat == 0:
	    retval = result[1][0]
	    if retval != '0':
	        status = 1
	        fnstatus = 3
	        statusR, fnstatusR = self.deallocateOCSdrive(remoteHost, drive)
                return status, fnstatus
	else :
	    status = 1
	    fnstatus = 5
	    statusR, fnstatusR = self.deallocateOCSdrive(remoteHost, drive)
            return status, fnstatus
        return status, fnstatus

    def deallocateOCSdrive(self, remoteHost, drive):
        status = 0
	fnstatus = 0
	command = "rsh " + remoteHost + " 'ocs_deallocate -t " + drive + \
	          " ; echo $?' "
        #Trace.trace(7, "media changer rsh Command=", command )
	pipeObj = popen2.Popen3(command, 0, 0)
	if pipeObj is None:
	    status = 1
	    fnstatus = 7
            return status, fnstatus
	stat = pipeObj.wait()
	result = pipeObj.fromchild.readlines()  # result has returned string
        #Trace.trace(7, "media changer rsh return strings=", result, " stat=", stat )
	if stat == 0:
	    retval = result[1][0]
	    if retval != '0': #check if drive already deallocated (not an error)
	        retstring = result[0]
		pos=string.find(retstring,"drive is already deallocated")
		if pos == -1 :  # really an error
	            status = 1
	            fnstatus = 8
                    return status, fnstatus
	else :
	    status = 1
	    fnstatus = 9
        return status, fnstatus

    def unmountOCSdrive(self, remoteHost, drive):
        status = 0
	fnstatus = 0
	command = "rsh " + remoteHost + " 'ocs_dismount -t " + drive + \
	          " ; echo $?' "
        #Trace.trace(7, "media changer rsh Command=", command )
	pipeObj = popen2.Popen3(command, 0, 0)
	if pipeObj is None:
	    status = 1
	    fnstat = 7
            return status, fnstatus
	stat = pipeObj.wait()
	result = pipeObj.fromchild.readlines()  # result has returned string
        #Trace.trace(7, "media changer rsh return strings=", result, " stat=", stat )
	if stat == 0:
	    retval = result[1][0]
	    if retval != '0':
	        status = 1
	        fnstatus = 8
                return status, fnstatus
	else :
	    status = 1
	    fnstatus = 9
        return status, fnstatus

    def load(self, external_label, drive, media_type):
	status = 0
	fnstat = 0
	status, fnstatus = self.allocateOCSdrive(self.remoteHost, drive)
	if status == 0 :
	    status, fnstatus = self.mountOCSdrive(external_label, self.remoteHost, \
	                       drive)
        return self.status_table[status][0], status, self.status_table[status][1]

    def unload(self, external_label, drive, media_type):
	status = 0
	fnstat = 0
	statusTmp, fnstatusTmp = self.unmountOCSdrive(self.remoteHost, drive)
     	status, fnstatus = self.deallocateOCSdrive(self.remoteHost, drive)
	if statusTmp != 0 :
	    status = statusTmp
	    fnstatus = fnstatusTmp
        return self.status_table[status][0], status, self.status_table[status][1]
	
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
