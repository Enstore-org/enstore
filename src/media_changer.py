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

# enstore imports
import configuration_client
import dispatching_worker
import generic_server
import generic_cs
import interface
import log_client
import traceback
import string
import time				# sleep
import Trace
import e_errors

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

    def maxwork(self,val=-1):
        if val != -1:
           self.MaxWork = val
        return (self.MaxWork, self.work_list)        

    # load volume into the drive;  default, overridden for other media changers
    def load(self,
             external_label,    # volume external label
             drive,             # drive id
             media_type) :	# media type
	if 'delay' in self.mc_config.keys() and self.mc_config['delay']:
	    self.enprint("make sure tape "+external_label+" is in drive "+\
	                 drive)
	    time.sleep( self.mc_config['delay'] )
	    self.enprint( 'continuing with reply' )
	    pass
        self.reply_to_caller({'status' : (e_errors.OK, 0, None)})

    # unload volume from the drive;  default overridden for other media changers
    def unload(self,
               external_label,  # volume external label
               drive,
	       media_type) :         # drive id
	if 'delay' in self.mc_config.keys() and self.mc_config['delay']:
	    self.enprint("remove tape "+external_label+" from drive "+drive)
	    time.sleep( self.mc_config['delay'] )
	    pass
        self.reply_to_caller({'status' : (e_errors.OK, 0, None)})

    # prepare is overridden by dismount for mount; i.e. for tape drives we always dismount before mount
    def prepare(self,
               external_label,  # volume external label
               drive,
               media_type) :         # drive id
        pass

    # Do the forking and call the function which will be load or unload
    def DoWork(self, function, ticket):

        Trace.trace(10, '>dowork')
        self.logc.send(log_client.INFO, 2,"REQUESTED "+ticket['function']+" "  +\
                                          ticket['vol_ticket']['external_label']+" "  +\
                                          ticket['drive_id']+" "  +\
                                          ticket['vol_ticket']['media_type'] )
        #if we have max number of working children, assume client will resend
        if len(self.work_list) >= self.MaxWork :
            self.logc.send(log_client.INFO, 2, "MC Overflow: "+ repr(self.MaxWork) + " " +\
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
                # send status back to MC parent via pipe to dispatching_worker
                #self.enprint( "STS"+repr(ticket))
                Trace.trace(10, '<<< sts'+repr(sts))
                ticket["work"]="WorkDone"
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
        # dispatching_worker sends "WorkDone" ticket here
        # remove work from outstanding work list
        for i in self.work_list:
           if i["ra"] == ticket["ra"]:
              self.work_list.remove(i)
              break
        self.logc.send(log_client.INFO, 2,"FINISHED "+ticket['function']+" "  +\
                                          ticket['vol_ticket']['external_label']+" "  +\
                                          ticket['drive_id']+" "  +\
                                          repr(ticket['status']) )
        # report back ito original client - probably a mover
        Trace.trace(10, '<<< WorkDone')
        self.reply_with_address(ticket)

# EMASS robot loader server
class EMASS_MediaLoader(MediaLoaderMethods) :
    def __init__(self, medch, maxwork=5,csc=0, verbose=0,\
                 host=interface.default_host(), \
                 port=interface.default_port()):
        MediaLoaderMethods.__init__(self,medch,maxwork,csc,verbose,host,port)
        import EMASS
        self.load=EMASS.mount
        self.unload=EMASS.dismount
        self.prepare=EMASS.dismount


# STK robot loader server
class STK_MediaLoader(MediaLoaderMethods) :
    def __init__(self, medch, maxwork=5,csc=0, verbose=0,\
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

class MediaLoaderInterface(interface.Interface):

    def __init__(self):
        Trace.trace(10,'{mlsi.__init__')
        # fill in the defaults for possible options
	self.verbose = 0
        self.maxwork=5
        interface.Interface.__init__(self)

        # now parse the options
        self.parse_options()
        Trace.trace(10,'}mlsi.__init__')

    # define the command line options that are valid
    def options(self):
        Trace.trace(16, "{}options")
        return self.config_options()+["verbose=","log=","maxwork="] +\
               self.help_options()

    #  define our specific help
    def parameters(self):
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
    import sys
    Trace.init("medchanger")
    Trace.trace(1,"media changer called with args "+repr(sys.argv))

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
            Trace.init(intf.name[0:5]+'.medc')
            mc.logc.send(log_client.INFO, 1, "Media Changer"+intf.name+"(re) starting")
            mc.serve_forever()
        except:
	    mc.serve_forever_error("media changer", mc.logc)
            continue
    Trace.trace(1,"Media Changer finished (impossible)")
