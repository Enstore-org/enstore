###############################################################################
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

verbose = 0
# media loader template class
class MediaLoaderMethods(dispatching_worker.DispatchingWorker,
	                 generic_server.GenericServer) :

    def __init__(self, medch, csc=0, verbose=0,\
	         host=interface.default_host(), \
	         port=interface.default_port()):
        Trace.trace(10, '{__init__')
	self.verbose = verbose
	self.print_id = medch
        # get the config server
        configuration_client.set_csc(self, csc, host, port, verbose)
        #   pretend that we are the test system
        #   remember, in a system, there is only one bfs
        #   get our port and host from the name server
        #   exit if the host is not this machine
        self.mc_config = self.csc.get(medch)
        Trace.init(self.mc_config["logname"])
        dispatching_worker.DispatchingWorker.__init__(self, \
	                 (self.mc_config['hostip'], self.mc_config['port']))
        # get a logger
        self.logc = log_client.LoggerClient(self.csc, \
	                                    self.mc_config["logname"], \
                                            'logserver', 0)
        Trace.trace(10, '}__init__')



    # load volume into the drive
    def load(self,
             external_label,    # volume external label
             drive) :           # drive id
	if 'delay' in self.mc_config.keys() and self.mc_config['delay']:
	    self.enprint("make sure tape "+external_label+" is in drive "+\
	                 drive)
	    time.sleep( self.mc_config['delay'] )
	    pass
        self.reply_to_caller({'status' : (e_errors.OK, None)})

    # unload volume from the drive
    def unload(self,
               external_label,  # volume external label
               drive) :         # drive id
	if 'delay' in self.mc_config.keys() and self.mc_config['delay']:
	    self.enprint("remove tape "+external_label+" from drive "+drive)
	    time.sleep( self.mc_config['delay'] )
	    pass
        self.reply_to_caller({'status' : (e_errors.OK, None)})

    # wrapper method for client - server communication
    def loadvol(self,
                ticket):        # associative array containing volume and\
                                # drive id
        return self.load(ticket['external_label'], ticket['drive_id'])

    # wrapper method for client - server communication
    def unloadvol(self, ticket):
        return self.unload(ticket['external_label'], ticket['drive_id'])

# IBM3494 robot class
class IBM3494_MediaLoaderMethods(MediaLoaderMethods) :

    # load volume into the drive
    def load(self, external_label, drive) :
        self.reply_to_caller({'status' : (e_errors.OK, None)})

    # unload volume from the drive
    def unload(self, external_label, drive) :
        self.reply_to_caller({'status' : (e_errors.OK, None)})

# FTT tape drives with no robot
class FTT_MediaLoaderMethods(MediaLoaderMethods) :

    # assumes volume is in drive
    def load(self, external_label, drive) :
	self.enprint("media changer rewind")
        #os.system("mt -t " + drive + " rewind")
        self.reply_to_caller({'status' : (e_errors.OK, None)})

    # assumes volume is in drive and leave it there for testing
    def unload(self, external_label, drive) :
        os.system("mt -t " + drive + " rewind")
        self.reply_to_caller({'status' : (e_errors.OK, None)})

# EMASS tape robot
class EMASS_MediaLoaderMethods(MediaLoaderMethods) :

    # load volume is in drive
    def load(self, external_label, drive) :
      command = "dasadmin mount" + external_label + " " + tape_drive
      out_ticket = {"status" : (e_errors.MOUNTFAILED, "mount_failed")}
      count=2
      while count > 0 and out_ticket != {"status" : (e_errors.OK, None)}:
        count = count - 1
        self.logc.send(log_client.INFO, 2, "Mnt cmd:"+command)
        #    try the mount
        result = os.system(command)
        #    analyze the results
	for line in result.readlines():
           if string.find(line, "completed successfully") ! -1 :
                out_ticket = {"status" : (e_errors.OK, None)}
                self.logc.send(log_client.INFO, 4, "Mnt returned ok")
                for line in result:
                    self.logc.send(log_client.INFO, 8, "Mnt ok:"+line)
                break
        
        # log the error
        self.logc.send(log_client.ERROR, 1, "Mnt Failed:"+command)
        for line in result:
                self.logc.send(log_client.ERROR, 1, "Mnt Failed:"+line)
      # send reply to caller
      self.reply_to_caller(out_ticket)

    # unload volume from the drive;   ron said the tape will be ejected.
    def unload(self, external_label, tape_drive) :
      # form dismount command to be executed
      command = "dasadmin unload " + tape_drive

      # retry dismount once if it fails
      count=2
      out_ticket = {"status" : (e_errors.DISMOUNTFAILED, "dismount_failed")}
      while count > 0 and out_ticket != {"status" : (e_errors.OK, None)}:
        count = count - 1
        # call dismount command
        self.logc.send(log_client.INFO, 2, "UMnt cmd:"+command)
        result = os.popen(command, "r").readlines()

        # analyze the return message
        for line in result:
            if string.find(line, "dismount") != -1 :
                out_ticket = {"status" : (e_errors.OK, None)}
                self.logc.send(log_client.INFO, 4, "UMnt returned ok:")
                for line in result:
                    {self.logc.send(log_client.INFO, 8, "UMnt ok:"+line)
                break
        # log the error
        self.logc.send(log_client.ERROR, 1, "UMnt Failed:"+command)
        for line in result:
            self.logc.send(log_client.ERROR, 1, "UMnt Failed:"+line)

      self.reply_to_caller(out_ticket)

# STK robot class
class STK_MediaLoaderMethods(MediaLoaderMethods) :

    # load volume into the drive
    def load(self, external_label, tape_drive) :
      # form mount command to be executed
      command = "rsh " + self.mc_config['acls_host'] + " -l " + \
                            self.mc_config['acls_uname'] + " 'echo mount " + \
                            external_label + " " + tape_drive + \
                            " | /export/home/ACSSS/bin/cmd_proc 2>>/tmp/garb'"

      stk_query_command = "rsh " + self.mc_config['acls_host'] + " -l " + \
                            self.mc_config['acls_uname'] + " 'echo query drive " + \
                            tape_drive + \
                            " | /export/home/ACSSS/bin/cmd_proc 2>>/tmp/garb'"
      out_ticket = {"status" : (e_errors.MOUNTFAILED, "dismount_failed")}
      count=2
      while count > 0 and out_ticket != {"status" : (e_errors.OK, None)}:
        count = count - 1
        # call mount command
        self.logc.send(log_client.INFO, 2, "Mnt cmd:"+command)
        result = os.popen(command, "r").readlines()

        # analyze the return message
        for line in result:
            if string.find(line, "mounted") != -1 :
                out_ticket = {"status" : (e_errors.OK, None)}
                self.logc.send(log_client.INFO, 4, "Mnt returned ok")
                for line in result:
                    self.logc.send(log_client.INFO, 8, "Mnt ok:"+line)
                break

        # log the error
        self.logc.send(log_client.ERROR, 1, "Mnt Failed:"+command)
        for line in result:
            self.logc.send(log_client.ERROR, 1, "Mnt Failed:"+line)
        result  = os.popen(stk_query_command, "r").readlines()
        for line in result:
            self.logc.send(log_client.INFO, 1, "Mnt qry:"+line)
      # send reply to caller
      self.reply_to_caller(out_ticket)

    # unload volume from the drive - a force does an eject
    def unload(self, external_label, tape_drive) :
      # form dismount command to be executed
      command = "rsh " + self.mc_config['acls_host'] + " -l " +\
                            self.mc_config['acls_uname'] + " 'echo dismount " +\
                            external_label + " " + tape_drive + " force" +\
                            " | /export/home/ACSSS/bin/cmd_proc 2>>/tmp/garb'"

      stk_query_command = "rsh " + self.mc_config['acls_host'] + " -l " + \
                            self.mc_config['acls_uname'] + " 'echo query drive " + \
                            tape_drive + \
                            " | /export/home/ACSSS/bin/cmd_proc 2>>/tmp/garb'"

      # retry dismount once if it fails
      count=2
      out_ticket = {"status" : (e_errors.DISMOUNTFAILED, "dismount_failed")}
      while count > 0 and out_ticket != {"status" : (e_errors.OK, None)}:
        count = count - 1
        # call dismount command
        self.logc.send(log_client.INFO, 2, "UMnt cmd:"+command)
        result = os.popen(command, "r").readlines()

        # analyze the return message
        for line in result:
            if string.find(line, "dismount") != -1 :
                out_ticket = {"status" : (e_errors.OK, None)}
                self.logc.send(log_client.INFO, 4, "UMnt returned ok:")
                for line in result:
                    self.logc.send(log_client.INFO, 8, "UMnt ok:"+line)
                break
        # log the error
        self.logc.send(log_client.ERROR, 1, "UMnt Failed:"+command)
        for line in result:
            self.logc.send(log_client.ERROR, 1, "UMnt Failed:"+line)
        result  = os.popen(stk_query_command, "r").readlines()
        for line in result:
            self.logc.send(log_client.INFO, 1, "Umnt qry:"+line)
        
      self.reply_to_caller(out_ticket)

# STK media loader server
class STK_MediaLoader(STK_MediaLoaderMethods) :
    pass

# EMASS robot loader server
class EMASS_MediaLoader(EMASS_MediaLoaderMethods) :
    pass

# Raw Disk media loaded server
class RDD_MediaLoader(MediaLoaderMethods) :
    pass

# FTT tape drives with no robot loader server
class FTT_MediaLoader(FTT_MediaLoaderMethods) :
    pass

class MediaLoaderInterface(interface.Interface):

    def __init__(self):
        Trace.trace(10,'{mlsi.__init__')
        # fill in the defaults for possible options
	self.verbose = 0
        interface.Interface.__init__(self)

        # now parse the options
        self.parse_options()
        Trace.trace(10,'}mlsi.__init__')

    # define the command line options that are valid
    def options(self):
        Trace.trace(16, "{}options")
        return self.config_options()+["verbose=","log="] +\
               self.help_options()

    #  define our specific help
    def help_line(self):
        return interface.Interface.help_line(self)+" media_changer"

    # parse the options like normal but make sure we have a media changer
    def parse_options(self):
        interface.Interface.parse_options(self)
        # bomb out if we don't have a media_changer
        if len(self.args) < 1 :
            self.print_help(),
            sys.exit(1)
        else:
            self.name = self.args[0]


if __name__ == "__main__" :
    import sys
    import socket
    import time
    import timeofday
    Trace.init("medchanger")
    Trace.trace(1,"media changer called with args "+repr(sys.argv))

    # get an interface
    intf = MediaLoaderInterface()

    # here we need to define what is the class of the media changer
    # and create an object of that class based on the value of args[0]
    # for now there is just one possibility

    # THIS NEEDS TO BE FIXED -- WE CAN'T BE CHECKING FOR EACH KIND!!!
    if intf.name == 'STK.media_changer' :
        mc =  STK_MediaLoader(intf.name, 0, intf.verbose, \
	                      intf.config_host, intf.config_port)
    elif intf.name == 'EMASS.media_changer' :
        mc =  EMASS_MediaLoader(intf.name, 0, intf.verbose, \
	                      intf.config_host, intf.config_port)
    elif intf.name == 'FTT.media_changer' :
        mc =  FTT_MediaLoader(intf.name, 0, intf.verbose, \
	                      intf.config_host, intf.config_port)
    else :
        mc =  RDD_MediaLoader(intf.name, 0, intf.verbose, \
	                      intf.config_host, intf.config_port)

    while 1:
        try:
            Trace.init(intf.name[0:5]+'.medc')
            mc.logc.send(log_client.INFO, 1, "Media Changer"+intf.name+"(re) starting")
            mc.serve_forever()
        except:
	    mc.serve_forever_error("media changer", mc.logc)
            continue
    Trace.trace(1,"Media Changer finished (impossible)")

