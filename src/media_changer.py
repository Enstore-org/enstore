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
import SocketServer
import configuration_client
import dispatching_worker
import generic_server
import log_client
import traceback
import string
import time				# sleep
import Trace
import e_errors

list = 0
# media loader template class
class MediaLoaderMethods(dispatching_worker.DispatchingWorker) :

    # load volume into the drive
    def load(self,
             external_label,    # volume external label
             drive) :           # drive id
	if 'delay' in mc_config.keys() and mc_config['delay']:
	    print "make sure tape",external_label,"is in drive",drive
	    time.sleep( mc_config['delay'] )
	    pass
        self.reply_to_caller({'status' : (e_errors.OK, None)})

    # unload volume from the drive
    def unload(self,
               external_label,  # volume external label
               drive) :         # drive id
	if 'delay' in mc_config.keys() and mc_config['delay']:
	    print "remove tape",external_label,"from drive",drive
	    time.sleep( mc_config['delay'] )
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
	print "media changer rewind"
        #os.system("mt -t " + drive + " rewind")
        self.reply_to_caller({'status' : (e_errors.OK, None)})

    # assumes volume is in drive and leave it there for testing
    def unload(self, external_label, drive) :
        os.system("mt -t " + drive + " rewind")
        self.reply_to_caller({'status' : (e_errors.OK, None)})

# STK robot class
class STK_MediaLoaderMethods(MediaLoaderMethods) :

    # load volume into the drive
    def load(self, external_label, tape_drive) :
      # form mount command to be executed
      stk_mount_command = "rsh " + mc_config['acls_host'] + " -l " + \
                            mc_config['acls_uname'] + " 'echo mount " + \
                            external_label + " " + tape_drive + \
                            " | /export/home/ACSSS/bin/cmd_proc 2>>/tmp/garb'"

      stk_query_command = "rsh " + mc_config['acls_host'] + " -l " + \
                            mc_config['acls_uname'] + " 'echo query drive " + \
                            tape_drive + \
                            " | /export/home/ACSSS/bin/cmd_proc 2>>/tmp/garb'"
      count=2
      out_ticket = {"status" : (e_errors.MOUNTFAILED, "dismount_failed")}
      while count > 0 and out_ticket != {"status" : (e_errors.OK, None)}:
        count = count - 1
        # call mount command
        logc.send(log_client.INFO, 2, "Mnt cmd:"+stk_mount_command)
        returned_message = os.popen(stk_mount_command, "r").readlines()
        out_ticket = {"status" : (e_errors.MOUNTFAILED, "mount_failed")}

        # analyze the return message
        for line in returned_message:
            if string.find(line, "mounted") != -1 :
                out_ticket = {"status" : (e_errors.OK, None)}
                break

        # log the work
        if out_ticket["status"][0] != e_errors.OK:
            logc.send(log_client.ERROR, 1, "Mnt Failed:"+stk_mount_command)
            for line in returned_message:
                logc.send(log_client.ERROR, 1, "Mnt Failed:"+line)
            returned_message  = os.popen(stk_query_command, "r").readlines()
            for line in returned_message:
                logc.send(log_client.INFO, 1, "Mnt qry:"+line)
        else :
            logc.send(log_client.INFO, 4, "Mnt returned ok")
            for line in returned_message:
                logc.send(log_client.INFO, 8, "Mnt ok:"+line)
        # send reply to caller
        self.reply_to_caller(out_ticket)

    # unload volume from the drive
    def unload(self, external_label, tape_drive) :
      # form dismount command to be executed
      stk_umnt_command = "rsh " + mc_config['acls_host'] + " -l " +\
                            mc_config['acls_uname'] + " 'echo dismount " +\
                            external_label + " " + tape_drive + " force" +\
                            " | /export/home/ACSSS/bin/cmd_proc 2>>/tmp/garb'"

      stk_query_command = "rsh " + mc_config['acls_host'] + " -l " + \
                            mc_config['acls_uname'] + " 'echo query drive " + \
                            tape_drive + \
                            " | /export/home/ACSSS/bin/cmd_proc 2>>/tmp/garb'"

      # retry dismount once if it fails
      count=2
      out_ticket = {"status" : (e_errors.DISMOUNTFAILED, "dismount_failed")}
      while count > 0 and out_ticket != {"status" : (e_errors.OK, None)}:
        count = count - 1
        # call dismount command
        logc.send(log_client.INFO, 2, "UMnt cmd:"+stk_umnt_command)
        returned_message = os.popen(stk_umnt_command, "r").readlines()

        # analyze the return message
        for line in returned_message:
            if string.find(line, "dismount") != -1 :
                out_ticket = {"status" : (e_errors.OK, None)}
                break
        # log the work
        if out_ticket["status"][0] != e_errors.OK:
            logc.send(log_client.ERROR, 1, "UMnt Failed:"+stk_mount_command)
            for line in returned_message:
                logc.send(log_client.ERROR, 1, "UMnt Failed:"+line)
            returned_message  = os.popen(stk_query_command, "r").readlines()
            for line in returned_message:
                logc.send(log_client.INFO, 1, "Umnt qry:"+line)
        else:
            logc.send(log_client.INFO, 4, "UMnt returned ok:")
            for line in returned_message:
                logc.send(log_client.INFO, 8, "UMnt ok:"+line)
        
      self.reply_to_caller(out_ticket)

# STK media loader server
class STK_MediaLoader(STK_MediaLoaderMethods,
                      generic_server.GenericServer,
                      SocketServer.UDPServer) :
    pass

# Raw Disk media loaded server
class RDD_MediaLoader(MediaLoaderMethods,
                      generic_server.GenericServer,
                      SocketServer.UDPServer) :
    pass

# Raw Disk media loaded server
class FTT_MediaLoader(FTT_MediaLoaderMethods,
                      generic_server.GenericServer,
                      SocketServer.UDPServer) :
    pass

if __name__ == "__main__" :
    import sys
    import getopt
    import socket
    import time
    import timeofday
    Trace.init("medchanger")
    Trace.trace(1,"media changer called with args "+repr(sys.argv))

    # defaults
    #config_host = "localhost"
    (config_hostname,ca,ci) = socket.gethostbyaddr(socket.gethostname())
    config_host = ci[0]
    config_port = "7500"
    config_file = ""
    config_list = 0

    # see what the user has specified. bomb out if wrong options specified
    options = ["config_host=","config_port=","config_file="\
               ,"config_list","list","help"]
    optlist,args=getopt.getopt(sys.argv[1:],'',options)
    for (opt,value) in optlist :
        if opt == "--config_host" :
            config_host = value
        elif opt == "--config_port" :
            config_port = value
        elif opt == "--config_list" :
            config_list = 1
        elif opt == "--list" :
            list = 1
        elif opt == "--log" :
            list = value
        elif opt == "--help" :
            print "python ",sys.argv[0], options, "media changer"
            print "   do not forget the '--' in front of each option"
            sys.exit(0)

    # bomb out if can't translate host
    ip = socket.gethostbyname(config_host)

    # bomb out if port isn't numeric
    config_port = string.atoi(config_port)

    # bomb out if we don't have a media changer
    if len(args) < 1 :
        print "python ",sys.argv[0], options, "media changer"
        print "   do not forget the '--' in front of each option"
        sys.exit(1)

    csc = configuration_client.ConfigurationClient(config_host,config_port,\
                                                    config_list)

    mc_config = csc.get(args[0])


    # here we need to define what is the class of the media changer
    # and create an object of that class based on the value of args[0]
    # for now there is just one possibility

    # THIS NEEDS TO BE FIXED -- WE CAN'T BE CHECKING FOR EACH KIND!!!
    if args[0] == 'STK.media_changer' :
        mc =  STK_MediaLoader((mc_config['hostip'], mc_config['port']),
                               STK_MediaLoaderMethods)
    elif args[0] == 'FTT.media_changer' :
        mc =  FTT_MediaLoader((mc_config['hostip'], mc_config['port']),
                               FTT_MediaLoaderMethods)
    else :
        mc =  RDD_MediaLoader((mc_config['hostip'], mc_config['port']),
                               MediaLoaderMethods)
    mc.set_csc(csc)

    # create a log client
    logc = log_client.LoggerClient(csc, mc_config["logname"], 'logserver', 0)
    mc.set_logc(logc)

    while 1:
        try:
            Trace.init(args[0][0:5]+'.medc')
            logc.send(log_client.INFO, 1, "Media Changer"+args[0]+"(re) starting")
            mc.serve_forever()
        except:
            traceback.print_exc()
            format = timeofday.tod()+" "+\
                     str(sys.argv)+" "+\
                     str(sys.exc_info()[0])+" "+\
                     str(sys.exc_info()[1])+" "+\
                     "media changer serve_forever continuing"
            print format
            logc.send(log_client.ERROR, 1, format)
            Trace.trace(0,format)
            continue
    Trace.trace(1,"Media Changer finished (impossible)")
