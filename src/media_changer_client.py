#! /usr/products/IRIX/python/x1_5/bin/python

#########################################################################
#                                                                       #
# Media Changer client.                                                 #
# Media Changer access methods                                          #
#  $Id$                                                                #
#########################################################################


import sys
import os
from configuration_client import *
from udp_client import UDPClient
import pprint

class MediaLoaderClient:
    def __init__(self, configuration_client, name) :
        self.csc = configuration_client
        self.u = UDPClient()
        self.media_changer = name

    # send the request to the Media Loader server and then send answer to user
    def send (self, ticket) :
        vticket = self.csc.get(self.media_changer)
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

    # check on alive status
    def alive(self):
        return self.send({'work':'alive'})

if __name__ == "__main__" :
    import getopt
    import socket

    # defaults
    #config_host = "localhost"
    (config_host,ca,ci) = socket.gethostbyaddr(socket.gethostname())
    config_port = "7500"
    config_file = ""
    config_list = 0
    list = 0
    alive = 0

    # see what the user has specified. bomb out if wrong options specified
    options = ["config_host=","config_port=","config_file="\
               ,"config_list","list","alive","help"]
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
        elif opt == "--alive" :
            alive = 1
        elif opt == "--help" :
            print "python ",sys.argv[0], options, "media_changer volume drive"
            print "   do not forget the '--' in front of each option"
            sys.exit(0)

    # bomb out if we number of arguments is wron
    if len(args) < 3 :
        print "python ",sys.argv[0], options, "media_changer volume drive"
        print "   do not forget the '--' in front of each option"
        sys.exit(1)

    # bomb out if can't translate host
    ip = socket.gethostbyname(config_host)

    # bomb out if port isn't numeric
    config_port = string.atoi(config_port)

    if config_list :
        print "Connecting to configuration server at ",config_host,config_port
    csc = configuration_client(config_host,config_port)

    media_changer_type = args[0]
    volume = args[1]
    drive = args[2]

    if list :
        pprint.pprint(args)
    mlc = MediaLoaderClient(csc, media_changer_type)
    if alive:
        ticket = mlc.alive()

    else:
        ticket = mlc.unloadvol(volume, drive)
        print 'unload returned:' + ticket['status']

    if ticket['status'] == 'ok' :
        if list:
            pprint.pprint(ticket)
        sys.exit(0)

    else :
        print "BAD STATUS:",ticket['status']
        pprint.pprint(ticket)
        sys.exit(1)
