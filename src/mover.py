import sys
from os import *
from time import *
from configuration_client import *
from volume_clerk_client import VolumeClerkClient
from file_clerk_client import FileClerkClient
from udp_client import UDPClient
from callback import *
from dict_to_a import *
from driver import RawDiskDriver
from media_changer_client import *
import pprint

class Mover :

    def __init__(self, config_host="localhost", config_port=7500):
        # An administrator can change out configuration on the fly.  This is
        # useful when a "library" is really one robot with multiple uses, or
        # when we need to manually load balance movers attached to virtual
        # library.  So we need to keep track of configuration server.
        self.config_host = config_host
        self.config_port = config_port
        self.csc = configuration_client(self.config_host,self.config_port)
        self.u = UDPClient()

    # primary serving loop
    def move_forever(self, name) :
        self.name = name

        # we don't have any work when we start - setup
        self.nowork({})

        while 1:
            # ok, here we go. get something to do from library manager
            ticket = self.send_library_manager()

            try:
                function = ticket["work"]
            except KeyError:
                raise "assert error Bogus stuff from vol mgr"

            # we got a job - now do it
            exec ("self." + function + "(ticket)")


    # send a message to our (current) library manager & return its answer
    def send_library_manager(self) :
        return  self.u.send(self.next_libmgr_request,
                             (self.library_manager_host,
                              self.library_manager_port) )

    # send a message to our user
    def send_user_last(self, ticket):
        self.control_socket.send(dict_to_a(ticket))
        self.control_socket.close()


    # we don't have any work. setup to see if we can get some
    def nowork(self, ticket) :
        sleep(1)

        # self.csc = configuration_client(self.config_host,self.config_port)

        # get (possibly new) info about the library manager this mover serves
        mconfig = self.csc.get(self.name)
        if mconfig["status"] != "ok" :
            raise "could not start mover up:" + mconfig["status"]
        self.library_device = mconfig["library_device"]
        self.driver_name = mconfig["driver"]
        self.device = mconfig["device"]
        self.library = mconfig["library"]
        self.media_changer = mconfig["media_changer"]

        #self.csc = configuration_client(self.config_host,self.config_port)

        # get (possibly new) info asssociated with our volume manager
        lconfig = self.csc.get_uncached(self.library)
        self.library_manager_host = lconfig["host"]
        self.library_manager_port = lconfig["port"]

        # set our ticket when we announce ourselves to the library manager
        self.idle_mover_next()


    # the library manager has told us to bind a volume so we can do some work
    def bind_volume(self, ticket) :

        # become a volume clerk client first
        vcc = VolumeClerkClient(self.csc)

        # find out what volume clerk knows about this volume
        self.external_label = ticket["external_label"]
        vticket = vcc.inquire_vol(self.external_label)
        if vticket["status"] != "ok" :
            self.unilateral_unbind_next()
            return
        self.vticket = vticket

        # now invoke the driver, which has the form:
        #       __init__(self, device, eod_cookie, remaining_bytes):
        self.driver = eval(self.driver_name + "('" +\
                           self.device + "','" +\
                           self.vticket["eod_cookie"] + "'," +\
                           repr(vticket["remaining_bytes"]) + ")")

        # the blocksize is controlled by the volume
        blocksize = self.vticket["blocksize"]
        iomax = blocksize*16      # how do I control this here?
        self.driver.set_size(blocksize,iomax)

        # need a media changer to control (mount/load...) the volume
        mlc = MediaLoaderClient(self.csc, self.media_changer)

        lmticket = mlc.loadvol(self.external_label, self.library_device)
        if lmticket["status"] != "ok" :

            # it is possible, under normal conditions, for the system to be
            # in the following race condition:
            #   library manager told another mover to unbind.
            #   other mover responds promptly,
            #   more work for library manager arrives and is given to a
            #     new mover before the old volume was given back to the library
            if lmticket["status"] == "media_in_another_device" :
                sleep (10)
            self.unilateral_unbind_next()
            return

        # we have the volume - load it
        self.driver.load()

        # create a ticket for library manager that says we have bound volume
        self.have_bound_volume_next()


    def unbind_volume(self, ticket) :

        # do any driver level rewind, unload, eject operations on the device
        self.driver.unload()

        # we will be needing a media loader to help unmount/unload...
        mlc = MediaLoaderClient(self.csc, self.media_changer)

        # now ask the media changer to unload the volume
        ticket = mlc.unloadvol(self.external_label, self.library_device)
        if ticket["status"] != "ok" :
            raise "media loader cannot unload my volume"

        # need to call the driver destructor....

        # create ticket that says we need more work
        self.idle_mover_next()


    # data transfer takes place on tcp sockets, so get ports & call user
    def get_user_sockets(self, ticket) :
        mover_host, mover_port, listen_socket = get_callback()
        listen_socket.listen(4)
        ticket["mover_callback_host"] = mover_host
        ticket["mover_callback_port"] = mover_port

        # call the user and tell him I'm your mover and here's your ticket
        self.control_socket = user_callback_socket(ticket)

        # we expect a prompt call-back here, and should protect against users
        # not getting back to us. The best protection would be to kick off if
        # the user dropped the control_socket, but I am at home and am not able
        # to find documentation on select...

        data_socket, address = listen_socket.accept()
        self.data_socket = data_socket
        listen_socket.close()


    # the library manager has asked us to write a file to the hsm
    def write_to_hsm(self, ticket) :

        # double check that we are talking about the same volume
        if ticket["external_label"] != self.external_label :
            raise "volume manager and I disagree on volume"

        # call the volume clerk and tell him we are going to append to volume
        vcc = VolumeClerkClient(self.csc)
        vticket = vcc.set_writing(self.external_label)
        if ticket["status"] != "ok" :
            raise "volume clerk forgot about this volume"

        # space to where the file will begin and save location
        # information for where future reads will have to  space the drive to.

        # call the user and announce that your her mover
        self.get_user_sockets(ticket)

        # setup values before transfer
        nb = ticket["size_bytes"]
        bytes_recvd = 0
        media_error = 0
        media_full  = 0
        drive_error = 0
        user_send_error = 0
        anything_written = 0
        bof_space_cookie = 0
        sanity_cookie = 0
        complete_crc = 0

        # open the hsm file for writing
        self.driver.open_file_write()

        # read the file from the user and write it out
        while 1:
            buff = self.data_socket.recv(self.driver.get_iomax())
            l = len(buff)
            if l == 0 :
                break
            bytes_recvd = bytes_recvd + l
            self.driver.write_block(buff)
            anything_written = 1

        # we've read the file from user, shut down data transfer socket
        self.data_socket.close()

        # close hsm file: get file/eod cookies & remaining bytes & errs & mnts
        file_cookie = self.driver.close_file_write()
        eod_cookie = self.driver.get_eod_cookie()
        remaining_bytes = self.driver.get_eod_remaining_bytes()
        wr_err,rd_err,wr_mnt,rd_mnt = self.driver.get_errors()

        vcc = VolumeClerkClient(self.csc)

        # check for errors and inform volume clerk
        if bytes_recvd != ticket["size_bytes"] :
            user_send_error = 1

        # mark database and continue on all user errors
        if user_send_error:
            self.vticket = vcc.set_remaining_bytes(ticket["external_label"],
                                                   remaining_bytes,
                                                   eod_cookie,wr_err,\
                                                   rd_err,wr_mnt,rd_mnt)
            # tell mover ready for more - can't undo user error, so continue
            self.have_bound_volume_next()
            msg = "Expected "+repr(ticket["size_bytes"])+" bytes,"\
                  " but only" +" stored"+repr(bytes_recvd)
            # tell user we're done, but there has been an error
            self.send_user_last({"status" : "Mover: Retry: user_error "+msg})
            return

        # if media full, mark volume readonly, unbind it & tell user to retry
        elif media_full :
            vcc.set_system_readonly(ticket["external_label"])
            self.unilateral_unbind_next()
            msg = "Volume "+repr(ticket["external_label"])
            self.send_user_last({"status" : "Mover: Retry: media_full "+msg})
            return

        # if media error, mark volume readonly, unbind it & tell user to retry
        elif media_error :
            vcc.set_system_readonly(ticket["external_label"])
            self.unilateral_unbind_next()
            msg = "Volume "+repr(ticket["external_label"])
            self.send_user_last({"status" : "Mover: Retru: media_error "+msg})
            return

        # drive errors are bad:  unbind volule it & tell user to retry
        elif drive_error :
            vcc.set_hung(ticket["external_label"])
            self.unilateral_unbind_next()
            msg = "Volume "+repr(ticket["external_label"])
            self.send_user_last({"status" : "Mover: Retry: drive_error "+msg})
            #since we will kill ourselves, tell the volume mgr now....
            ticket = send_library_manager()
            raise "device panic -- I want to do no harm to media"

        # All is well.

        # Tell volume server
        self.vticket = vcc.set_remaining_bytes(ticket["external_label"],\
                                               remaining_bytes,\
                                               eod_cookie,\
                                               wr_err,rd_err,wr_mnt,rd_mnt)


        # connec to file clerk and get new bit file id
        fc = FileClerkClient(self.csc)
        self.fticket = fc.new_bit_file(file_cookie,ticket["external_label"],\
                                       sanity_cookie,complete_crc)

        # only bfid is needed, but save other useful information for user too
        work = ticket["work"]
        stat = ticket["status"]
        for key in self.vticket :
            ticket[key] = self.vticket[key]
        for key in self.fticket :
            ticket[key] = self.fticket[key]
        ticket["work"] = work
        ticket["status"] = status

        pprint.pprint(self.__dict__)

        ticket["device"] = self.device
        ticket["driver_name"] = self.driver_name
        ticket["mover_name"] = self.name
        ticket["library_device"] = self.library_device

        # finish up and tell user about the transfer
        self.send_user_last(ticket)

        # go around for more
        self.have_bound_volume_next()


    def read_from_hsm(self, ticket) :
        if ticket["external_label"] != self.external_label :
            raise "volume manager and I disagree on volume"
        vcc = VolumeClerkClient(self.csc)
        vticket = vcc.inquire_vol(self.external_label)
        if ticket["status"] != "ok" :
            raise "volume clerk forgot about this volume"

        # space to where the file will begin and save location
        # information for where futrure reads will have to
        # space the drive to.

        self.get_user_sockets(ticket)
        media_error = 0
        media_full  = 0
        drive_error = 0
        user_recieve_error = 0
        bytes_sent = 0
        self.driver.open_file_read(ticket["bof_space_cookie"])
        while 1:
            buff = self.driver.read_block()
            l = len(buff)
            if l == 0 : break
            self.data_socket.send(buff)
            bytes_sent = bytes_sent + l
            anything_sent = 1
        self.data_socket.close()

        if media_error :
            vcc.set_system_readonly(ticket["external_label"])
            self.send_user_last({"status" : "Mover: retry"})
            self.unilateral_unbind_next()
            return

        elif drive_error :
            vcc.set_hung(ticket["external_label"])
            self.send_user_last({"status" : "Mover: retry"})
            self.unilateral_unbind_next()
            #since we will kill ourselves, tell the volume mgr now....
            ticket = send_library_manager()
            raise "device panic -- I want to do no harm to media"
            return

        # read has finished correctly

        # tell user
        self.send_user_last(ticket)

        # go around for more
        self.have_bound_volume_next()


    # create ticket that says we are idle
    def idle_mover_next(self):
        self.next_libmgr_request = {"work"  : "idle_mover",
                                    "mover" : self.name }


    # create ticket that says we have bound volume x
    def have_bound_volume_next(self):
        self.next_libmgr_request = {}
        self.next_libmgr_request["work"] = "have_bound_volume"
        self.next_libmgr_request["mover"] = self.name
        # copy volume information about the volume to our ticket
        for k in self.vticket.keys() :
            self.next_libmgr_request[k] = self.vticket[k]


    # create ticket that says we need to unbind volume x
    def unilateral_unbind_next(self):
        self.next_libmgr_request = {"work"           : "unilateral_unbind",
                                    "external_label" : self.external_label,
                                    "mover"          : self.name }


if __name__ == "__main__" :
    import getopt
    import socket

    # defaults
    config_host = "localhost"
    #(config_host,ca,ci) = socket.gethostbyaddr(socket.gethostname())
    config_port = "7500"
    config_list = 0

    # see what the user has specified. bomb out if wrong options specified
    options = ["config_host=","config_port=","config_list","help"]
    optlist,args=getopt.getopt(sys.argv[1:],'',options)
    for (opt,value) in optlist :
        if opt == "--config_host" :
            config_host = value
        elif opt == "--config_port" :
            config_port = value
        elif opt == "--config_list" :
            config_list = 1
        elif opt == "--help" :
            print "python ",sys.argv[0], options,"mover_device"
            print "   do not forget the '--' in front of each option"
            sys.exit(0)

    # bomb out if can't translate host
    ip = socket.gethostbyname(config_host)

    # bomb out if port isn't numeric
    config_port = string.atoi(config_port)

    # bomb out if we don't have a mover
    if len(args) < 1 :
        print "python",sys.argv[0], options, "mover_device"
        print "   do not forget the '--' in front of each option"
        sys.exit(1)

    if config_list :
        print "Connecting to configuration server at ",config_host,config_port

    while (1) :
        mv = Mover(config_host,config_port)
        mv.move_forever (args[0])
