#! /usr/products/IRIX/python/x1_5/bin/python

#########################################################################
#									#
# Media Changer client.							#
# Media Changer access methods                                  	#
#  $Id$								#
#########################################################################


import sys
import os
from configuration_client import *
from udp_client import UDPClient


class MediaLoaderClient:
    def __init__(self, configuration_client, name) :
	self.csc = configuration_client
	self.u = UDPClient()
	self.media_changer = name
	print "media loader client :" + self.media_changer

    # send the request to the Media Loader server and then send answer to user
    def send (self, ticket) :
        vticket = self.csc.get(self.media_changer)
	print 'host' + "(vticket['host'])"
	print 'port' + "(vticket['port'])"
	print 'sending ticket' #+ ticket
        return  self.u.send(ticket, (vticket['host'], vticket['port']))

    def loadvol(self, external_label, drive):
	ticket = {'work'           : 'loadvol',
		  'external_label' : external_label,
		  'drive_id'       : drive
		  }
	return self.send(ticket)

    def unloadvol(self, external_label, drive):
	ticket = {'work'           : 'unloadvol', 
		  'external_label' : external_label,
		  'drive_id'       : drive
		  }
	return self.send(ticket)


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

    print 'host=' + config_host + 'port=' + config_port

    # bomb out if can't translate host
    ip = socket.gethostbyname(config_host)

    # bomb out if port isn't numeric
    config_port = string.atoi(config_port)

    if config_list :
        print "Connecting to configuration server at ",config_host,config_port
    csc = configuration_client(config_host,config_port)

    mlc = MediaLoaderClient(csc, "STK.media_changer")
#    ticket = mlc.loadvol('000007', '0,0,9,1')
#    print 'load returned:' + ticket['status']
    ticket = mlc.unloadvol('000007', '0,0,9,1')
    print 'unload returned:' + ticket['status']
    
    













