#! /usr/products/IRIX/python/x1_5/bin/python

#########################################################################
#									#
# Media Changer server.							#
# Media Changer is an abstract object representing a physical device or	#
# operator who performs mounts / dismounts of tapes on tape drives.	#
# At startup the media changer process takes its argument from the	#
# command line and cofigures based on the dictionary entry in the	#
# Configuration	Server for this type of Media Changer.			#
# It accepts then requests from its clients and performs tape mounts	#
# and dismounts								#
#  $Id$								#
#########################################################################


import sys
import os
from SocketServer import *
from configuration_client import *
from callback import send_to_user_callback
from dispatching_worker import DispatchingWorker
from generic_server import GenericServer
import string

# create confuguration client object

# Conf = configuration_server_client() 


# media loader template class
class MediaLoaderMethods(DispatchingWorker) :
# start media changer for particular type of the changer
# media changer types are described in the Configuration Server Dictionary

        # load volume into the drive
	def load(self, 
		 external_label,    # volume external label
		 drive) :           # drive id
	    raise "Failed to overload method in template"

	# unload volume from the drive
	def unload(self, 
		   external_label,  # volume external label
		   drive) :         # drive id
	    raise "Failed to overload method in template"

	# wrapper method for client - server communication
	def loadvol(self, 
		    ticket):        # associative array containing volume and\
	                            # drive id
	    self.load(ticket['external_label'], ticket['drive_id'])

	# wrapper method for client - server communication
	def unloadvol(self, ticket):
	    self.unload(ticket['external_label'], ticket['drive_id'])

class IBM3494_MediaLoaderMethods(MediaLoaderMethods) :

	def load(self, external_label, drive) :
		print 'I am load function and my type is IBM3494'
		return {"status" : "ok"}

	def unload(self, external_label, drive) :
		print 'I am unload function and my type is IBM3494' 
		return {"status" : "ok"}

 
class STK_MediaLoaderMethods(MediaLoaderMethods) :
	def load(self, external_label, tape_drive) :
		print 'I am load function and my type is STK'
		# try to get the Enstore path from the system environment
		try:
			bin_dir = os.environ['ENSTORE_DIR'] + "/" + "src"
		except:
			bin_dir = "."

#		stk_mount_command = "rsh " + keys['acls_host'] + " -l " +\
#			    keys['acls_uname'] + " 'echo que mount " +\
#				    external_label + " | /export/home/ACSSS/bin/cmd_proc 2>>/tmp/garb'"
		stk_mount_command = "rsh " + keys['acls_host'] + " -l " +\
				    keys['acls_uname'] + " 'echo mount " +\
				    external_label + " " + \
				    tape_drive + " | /export/home/ACSSS/bin/cmd_proc 2>>/tmp/garb'"

		print 'command is:'
		print stk_mount_command

		returned_message = os.popen(stk_mount_command, "r").readlines()
		out_ticket = {"status" : "mount_failed"}
		for line in returned_message:
		    #print line
		    if string.find(line, "mounted") != -1 :
			print line   # TEST
			out_ticket = {"status" : "ok"}
			break
		self.reply_to_caller(out_ticket)
                print "status " + out_ticket["status"]

	def unload(self, external_label, tape_drive) :
		print 'I am unload function and my type is STK'
		# try to get the Enstore path from the system environment
		try:
			bin_dir = os.environ['ENSTORE_DIR'] + "/" + "src"
		except:
			bin_dir = "."

		stk_mount_command = "rsh " + keys['acls_host'] + " -l " +\
				    keys['acls_uname'] + " 'echo dismount " +\
				    external_label + " " + tape_drive + \
				    " force" +\
				    " | /export/home/ACSSS/bin/cmd_proc 2>>/tmp/garb'"

		print 'command is:'
		print stk_mount_command

		returned_message = os.popen(stk_mount_command, "r").readlines()
		out_ticket = {"status" : "dismount_failed"}
		for line in returned_message:
		    print line
		    if string.find(line, "dismount") != -1 :
			print line   # TEST
			out_ticket = {"status" : "ok"}
			break
		self.reply_to_caller(out_ticket)
                print "status " + out_ticket["status"]


class STK_MediaLoader(STK_MediaLoaderMethods, GenericServer, UDPServer) :
    pass

if __name__ == "__main__" :
    import getopt
    import socket

    # defaults
    config_host = "localhost"
    #(config_host,ca,ci) = socket.gethostbyaddr(socket.gethostname())
    config_port = "7500"
    config_file = ""
    config_list = 0

    # see what the user has specified. bomb out if wrong options specified
    options = ["config_host=","config_port=","config_file="\
               ,"config_list","help"]
    optlist,args=getopt.getopt(sys.argv[1:],'',options)
    for (opt,value) in optlist :
        if opt == "--config_host" :
            config_host = value
        elif opt == "--config_port" :
            config_port = value
        elif opt == "--config_list" :
            config_list = 1
        elif opt == "--help" :
            print "python ",sys.argv[0], options
            print "   do not forget the '--' in front of each option"
            sys.exit(0)

    # bomb out if can't translate host
    ip = socket.gethostbyname(config_host)

    # bomb out if port isn't numeric
    config_port = string.atoi(config_port)

    if config_list :
        print "Connecting to configuration server at ",config_host,config_port
    csc = configuration_client(config_host,config_port)

    keys = csc.get("STK.media_changer")
    print keys
    mls =  STK_MediaLoader((keys['host'], keys['port']), STK_MediaLoaderMethods)
    mls.set_csc(csc)
    #mls.loadvol('VSN001', '0,0,9,1')
    #mls.unloadvol('VSN001', '0,0,9,1')
    
    mls.serve_forever()
    








