import sys
import os
from configuration_client import *
from udp_client import UDPClient

class VolumeClerkClient :

    def __init__(self, configuration_client) :
        self.csc = configuration_client
        self.u = UDPClient()


    # send the request to the volume clerk server and then send answer to user
    def send (self, ticket) :
        vticket = self.csc.get("volume_clerk")
        return  self.u.send(ticket, (vticket['host'], vticket['port']))


    # add a volume to the stockpile
    def addvol(self,
               library,               # name of library media is in
               file_family,           # volume family the media is in
               media_type,            # media
               external_label,        # label as known to the system
               capacity_bytes,        #
               remaining_bytes,       #
               eod_cookie  = "none",  # code for seeking to eod
               user_inhibit  = "none",# "none" | "readonly" | "all"
               error_inhibit = "none",# "none" | "readonly" | "all" | "writing"
                                      # lesser access is specified as
                                      #       we find media errors,
                                      # writing means that a mover is
                                      #       appending or that a mover
                                      #       crashed while writing
               last_access = -1,      # last accessed time
               first_access = -1,     # first accessed time
               declared = -1,         # time volume was declared to system
               sum_wr_err = 0,        # total number of write errors
               sum_rd_err = 0,        # total number of read errors
               sum_wr_mnt = 0,        # total number of write mounts
               sum_rd_mnt = 0,        # total number of read mounts
               wrapper = "cpio",      # kind of wrapper for volume
               blocksize = -1         # blocksize (-1 =  media type specifies)
               ) :
        ticket = { 'work'            : 'addvol',
                   'library'         : library,
                   'file_family'     : file_family,
                   'media_type'      : media_type,
                   'external_label'  : external_label,
                   'capacity_bytes'  : capacity_bytes,
                   'remaining_bytes' : remaining_bytes,
                   'eod_cookie'      : eod_cookie,
                   'user_inhibit'    : user_inhibit,
                   'error_inhibit'   : error_inhibit,
                   'last_access'     : last_access,
                   'first_access'    : first_access,
                   'declared'        : declared,
                   'sum_wr_err'      : sum_wr_err,
                   'sum_rd_err'      : sum_rd_err,
                   'sum_wr_mnt'      : sum_wr_mnt,
                   'sum_rd_mnt'      : sum_rd_mnt,
                   'wrapper'         : wrapper,
                   'blocksize'       : blocksize,
                   }
        return self.send(ticket)


    # delete a volume from the stockpile
    def delvol(self, external_label) :
        ticket= { 'work'           : 'delvol',
                  'external_label' : external_label }
        return  self.send(ticket)


    # get a list of all volumnes
    def get_vols(self):
        return self.send({"work" : "get_vols"} )

    # what is the current status of a specified volume?
    def inquire_vol(self, external_label) :
        ticket= { 'work'           : 'inquire_vol',
                  'external_label' : external_label }
        return  self.send(ticket)

    # we are using the volume
    def set_writing(self, external_label) :
        ticket= { 'work'           : 'set_writing',
                  'external_label' : external_label }
        return self.send(ticket)

    # we are using the volume
    def set_system_readonly(self, external_label) :
        ticket= { 'work'           : 'set_system_readonly',
                  'external_label' : external_label }
        return self.send(ticket)

    # this many bytes left
    def set_remaining_bytes(self, external_label,remaining_bytes,eod_cookie,
                            wr_err,rd_err,wr_mnt,rd_mnt) :
        ticket= { 'work'            : 'set_remaining_bytes',
                  'external_label'  : external_label,
                  'remaining_bytes' : remaining_bytes,
                  'eod_cookie'      : eod_cookie,
                  'wr_err'          : wr_err,
                  'rd_err'          : rd_err,
                  'wr_mnt'          : wr_mnt,
                  'rd_mnt'          : rd_mnt }
        return self.send(ticket)

    # which volume can we use for this library, bytes and file family and ...
    def next_write_volume (self, library, min_remaining_bytes,
                           file_family, vol_veto_list) :
        ticket = { 'work'                : 'next_write_volume',
                   'library'             : library,
                   'min_remaining_bytes' : min_remaining_bytes,
                   'file_family'         : file_family,
                   'vol_veto_list'       : `vol_veto_list` }
        return self.send(ticket)

    # check on alive status
    def alive(self):
        return self.send({'work':'alive'})



if __name__ == "__main__" :
    import getopt
    import socket

    # defaults
    config_host = "localhost"
    #(config_host,ca,ci) = socket.gethostbyaddr(socket.gethostname())
    config_port = "7500"
    config_list = 0
    vol = ""
    vols = 0
    list = 0
    addvol = 0
    delvol = 0
    alive = 0

    # see what the user has specified. bomb out if wrong options specified
    options = ["config_host=","config_port=","config_list",
               "vols","vol=","addvol","delvol","list","alive","help"]
    optlist,args=getopt.getopt(sys.argv[1:],'',options)
    for (opt,value) in optlist :
        if opt == "--config_host" :
            config_host = value
        elif opt == "--config_port" :
            config_port = value
        elif opt == "--config_list" :
            config_list = 1
        elif opt == "--vols" :
            vols = 1
        elif opt == "--vol" :
            vol = value
        elif opt == "--addvol" :
            addvol = 1
        elif opt == "--delvol" :
            delvol = 1
        elif opt == "--alive" :
            alive = 1
        elif opt == "--list" :
            list = 1
        elif opt == "--help" :
            print "python ",sys.argv[0], options
            print "   do not forget the '--' in front of each option"
            print "   addvol arguments: library file_family media_type"\
                  +", volume_name, volume_byte_capacity remaining_capacity"
            print "   delvol arguments: volume_name"
            sys.exit(0)

    # bomb out if can't translate host
    ip = socket.gethostbyname(config_host)

    # bomb out if port isn't numeric
    config_port = string.atoi(config_port)

    if config_list :
        print "Connecting to configuration server at ",config_host,config_port
    csc = configuration_client(config_host,config_port)

    vcc = VolumeClerkClient(csc)

    if alive:
        ticket = vcc.alive()
    elif vols :
        ticket = vcc.get_vols()
    elif vol :
        ticket = vcc.inquire_vol(vol)
    elif addvol:
        # bomb out if we don't have correct number of add vol arguments
        if len(args) < 6 :
            print "   addvol arguments: library file_family media_type"\
                  +", volume_name, volume_byte_capacity remaining_capacity"
            sys.exit(1)
        ticket = vcc.addvol(args[0],              # library
                            args[1],              # file family
                            args[2],              # media type
                            args[3],              # name of this volume
                            string.atol(args[4]), # cap'y of this vol (bytes)
                            string.atol(args[5])) # rem cap'y of this volume
    elif delvol:
        # bomb out if we don't have correct number of del vol arguments
        if len(args) < 1 :
            print "   delvol arguments: volume_name"
            sys.exit(1)
        ticket = vcc.delvol(args[0])              # name of this volume

    if ticket['status'] != 'ok' :
        print "Bad status:",ticket['status']
        pprint.pprint(ticket)
        sys.exit(1)
    elif list:
        pprint.pprint(ticket)
        sys.exit(0)
