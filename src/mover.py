import sys
import time
from configuration_client import configuration_client
from volume_clerk_client import VolumeClerkClient
from file_clerk_client import FileClerkClient
from udp_client import UDPClient
import callback
from driver import RawDiskDriver, FTTDriver
from media_changer_client import MediaLoaderClient
import cpio
import binascii

class Mover :

    def __init__(self, config_host="localhost", config_port=7500):
        # An administrator can change out configuration on the fly.  This is
        # useful when a "library" is really one robot with multiple uses, or
        # when we need to manually load balance movers attached to virtual
        # library.  So we need to keep track of configuration server.
        if config_host == "localhost" :
            (config_host,ca,ci) = socket.gethostbyaddr(socket.gethostname())
        self.config_host = config_host
        self.config_port = config_port
        self.csc = configuration_client(self.config_host,self.config_port)
        self.u = UDPClient()

    def read_block(self) :
        badsock = self.data_socket.getsockopt(socket.SOL_SOCKET,
                                              socket.SO_ERROR)
        if badsock != 0 :
            print "Mover read_block, pre-recv error:", \
                  errno.errorcode[badsock]
        block = self.data_socket.recv(self.driver.get_blocksize())
        badsock = self.data_socket.getsockopt(socket.SOL_SOCKET,
                                              socket.SO_ERROR)
        if badsock != 0 :
            print "Mover read_block, post-recv error:", \
                  errno.errorcode[badsock]
        return block

    def write_block(self,buff) :
        badsock = self.data_socket.getsockopt(socket.SOL_SOCKET,
                                              socket.SO_ERROR)
        if badsock != 0 :
            print "Mover write_block, pre-send error:", \
                  errno.errorcode[badsock]
        count = self.data_socket.send(buff)
        badsock = self.data_socket.getsockopt(socket.SOL_SOCKET,
                                              socket.SO_ERROR)
        if badsock != 0 :
            print "Mover write_block, post-send error:", \
                  errno.errorcode[badsock]
        return count

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
        callback.write_tcp_socket(self.control_socket,ticket,\
                                  "mover send_user_last")
        self.control_socket.close()


    # we don't have any work. setup to see if we can get some
    def nowork(self, ticket) :
        time.sleep(1)

        # get (possibly new) info about the library manager this mover serves
        mconfig = self.csc.get_uncached(self.name)
        if mconfig["status"] != "ok" :
            raise "could not start mover up:" + mconfig["status"]
        self.library_device = mconfig["library_device"]
        self.driver_name = mconfig["driver"]
        self.device = mconfig["device"]
        self.library = mconfig["library"]
        self.media_changer = mconfig["media_changer"]

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
                           repr(self.vticket["remaining_bytes"]) + ")")

        # the blocksize is controlled by the volume
        blocksize = self.vticket["blocksize"]
        self.driver.set_blocksize(blocksize)

        # need a media changer to control (mount/load...) the volume
        self.mlc = MediaLoaderClient(self.csc, self.media_changer)

        lmticket = self.mlc.loadvol(self.external_label, self.library_device)
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
        self.mlc = MediaLoaderClient(self.csc, self.media_changer)

        # now ask the media changer to unload the volume
        ticket = self.mlc.unloadvol(self.external_label, self.library_device)
        if ticket["status"] != "ok" :
            raise "media loader cannot unload my volume"

        # need to call the driver destructor....

        # create ticket that says we need more work
        self.idle_mover_next()


    # data transfer takes place on tcp sockets, so get ports & call user
    def get_user_sockets(self, ticket) :
        mover_host, mover_port, listen_socket = callback.get_callback()
        listen_socket.listen(4)
        ticket["mover_callback_host"] = mover_host
        ticket["mover_callback_port"] = mover_port

        # call the user and tell him I'm your mover and here's your ticket
        self.control_socket = callback.user_callback_socket(ticket)

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
        self.vticket = vcc.set_writing(self.external_label)
        if ticket["status"] != "ok" :
            raise "volume clerk forgot about this volume"

        # setup values before transfer
        nb = ticket["size_bytes"]
        wr_size = 0
        sanity_size = ticket["sanity_size"]
        media_error = 0
        media_full  = 0
        drive_error = 0
        user_send_error = 0
        bof_space_cookie = 0
        sanity_crc = 0
        complete_crc = 0
        pnfs = ticket["pnfsfile_info"]
        inode = 0

        # call the user and announce that your her mover
        self.get_user_sockets(ticket)

        # open the hsm file for writing
        self.driver.open_file_write()

        # create the wrapper instance
        self.wrapper = cpio.cpio(self, self.driver, binascii.crc_hqx)

        # now write the file
        try:
            (wr_size, complete_crc, sanity_cookie) = self.wrapper.write(
                inode, pnfs["mode"], pnfs["uid"], pnfs["gid"], ticket["mtime"],
                ticket["size_bytes"], pnfs["major"], pnfs["minor"],
                pnfs["rmajor"], pnfs["rminor"], pnfs["pnfsFilename"],
                sanity_size)
            #print "cpio.write size:",wr_size,"crc:",complete_crc, \
            #     "sanity_cookie:",sanity_cookie
        except:
            print sys.exc_info()[0],sys.exc_info()[1]
            media_error = 1 # I don't know what else to do right now

        # we've read the file from user, shut down data transfer socket
        self.data_socket.close()

        # close hsm file
        file_cookie = self.driver.close_file_write()

        #  get file/eod cookies & remaining bytes & errs & mnts
        eod_cookie = self.driver.get_eod_cookie()
        remaining_bytes = self.driver.get_eod_remaining_bytes()
        wr_err,rd_err,wr_access,rd_access = self.driver.get_errors()

        vcc = VolumeClerkClient(self.csc)

        # check for errors and inform volume clerk
        if wr_size != ticket["size_bytes"] :
            user_send_error = 1

        # update database, tell user & continue on user errors
        if user_send_error:
            self.vticket = vcc.set_remaining_bytes(ticket["external_label"],
                                                   remaining_bytes,
                                                   eod_cookie,wr_err,
                                                   wr_err,rd_err,
                                                   wr_access,rd_access)
            msg = "Expected "+repr(ticket["size_bytes"])+" bytes,"\
                  " but only" +" stored"+repr(wr_size)
            # tell user we're done, but there has been an error
            self.send_user_last({"status" : "Mover: Retry: user_error "+msg})
            # tell mover ready for more - can't undo user error, so continue
            self.have_bound_volume_next()
            return

        # if media full, mark volume readonly, unbind it & tell user to retry
        elif media_full :
            vcc.update_counts(ticket["external_label"],
                              wr_err,rd_err,wr_access,rd_access)
            vcc.set_system_readonly(ticket["external_label"])
            self.unilateral_unbind_next()
            msg = "Volume "+repr(ticket["external_label"])
            self.send_user_last({"status" : "Mover: Retry: media_full "+msg})
            return

        # if media error, mark volume readonly, unbind it & tell user to retry
        elif media_error :
            vcc.update_counts(ticket["external_label"],
                              wr_err,rd_err,wr_access,rd_access)
            vcc.set_system_readonly(ticket["external_label"])
            self.unilateral_unbind_next()
            msg = "Volume "+repr(ticket["external_label"])
            self.send_user_last({"status" : "Mover: Retry: media_error "+msg})
            return

        # drive errors are bad:  unbind volule it & tell user to retry
        elif drive_error :
            vcc.update_counts(ticket["external_label"],
                              wr_err,rd_err,wr_access,rd_access)
            vcc.set_hung(ticket["external_label"])
            self.unilateral_unbind_next()
            msg = "Volume "+repr(ticket["external_label"])
            self.send_user_last({"status" : "Mover: Retry: drive_error "+msg})
            #since we will kill ourselves, tell the library manager now....
            ticket = send_library_manager()
            raise "device panic -- I want to do no harm to media"

        # All is well - write has finished successfully.

        # Tell volume server & update database
        self.vticket = vcc.set_remaining_bytes(ticket["external_label"],
                                               remaining_bytes,
                                               eod_cookie,
                                               wr_err,rd_err,\
                                               wr_access,rd_access)

        # connect to file clerk and get new bit file id
        fc = FileClerkClient(self.csc)
        self.fticket = fc.new_bit_file(file_cookie,ticket["external_label"],
                                       sanity_cookie,complete_crc)

        # only bfid is needed, but save other useful information for user too
        ticket["bfid"] = self.fticket["bfid"]
        ticket["volume_clerk"] = self.vticket
        ticket["file_clerk"] = self.fticket
        minfo = {}
        for k in ['config_host', 'config_port', 'device', 'driver_name',
                  'library', 'library_device', 'library_manager_host',
                  'library_manager_port', 'media_changer', 'name' ] :
            exec("minfo["+repr(k)+"] = self."+k)
        ticket["mover"] = minfo
        dinfo = {}
        if 0 :
            for k in ['blocksize', 'device', 'eod', 'first_write_block',
                      'rd_err', 'rd_access', 'remaining_bytes',
                      'wr_err', 'wr_access'] :
                exec("dinfo["+repr(k)+"] = self.driver."+k)

        ticket["driver"] = dinfo

        # finish up and tell user about the transfer
        self.send_user_last(ticket)

        # go around for more
        self.have_bound_volume_next()


    # the library manager has asked us to read a file to the hsm
    def read_from_hsm(self, ticket) :

        # double check that we are talking about the same volume
        if ticket["external_label"] != self.external_label :
            raise "volume manager and I disagree on volume"

        # call the volume clerk and check on volume
        vcc = VolumeClerkClient(self.csc)
        self.vticket = vcc.inquire_vol(self.external_label)
        if self.vticket["status"] != "ok" :
            raise "volume clerk forgot about this volume"

        # space to where the file will begin and save location
        # information for where future reads will have to space the drive to.

        # setup values before transfer
        media_error = 0
        media_full  = 0
        drive_error = 0
        user_recieve_error = 0
        bytes_sent = 0
        sanity_cookie = ticket["sanity_cookie"]
        complete_crc = 0

        # call the user and announce that your her mover
        self.get_user_sockets(ticket)

        # create the wrapper instance
        self.wrapper = cpio.cpio(self.driver,self,binascii.crc_hqx)

        # open the hsm file for reading and read it
        try:
            self.driver.open_file_read(ticket["bof_space_cookie"])
            (bytes_sent, complete_crc) = self.wrapper.read(sanity_cookie)
             #print "cpio.read  size:",wr_size,"crc:",complete_crc
        except:
            print sys.exc_info()[0],sys.exc_info()[1]
            media_error = 1 # I don't know what else to do right now

        # close hsm file
        self.driver.close_file_read()

        # we've sent the hsm file to the user, shut down data transfer socket
        self.data_socket.close()

        # get the error/mount counts and update database
        wr_err,rd_err,wr_access,rd_access = self.driver.get_errors()
        vcc.update_counts(ticket["external_label"],
                          wr_err,rd_err,wr_access,rd_access)

        # if media error, mark volume readonly, unbind it & tell user to retry
        if media_error :
            vcc.set_system_readonly(ticket["external_label"])
            self.unilateral_unbind_next()
            msg = "Volume "+repr(ticket["external_label"])
            self.send_user_last({"status" : "Mover: Retry: media_error "+msg})
            return

        # drive errors are bad:  unbind volule it & tell user to retry
        elif drive_error :
            vcc.set_hung(ticket["external_label"])
            self.unilateral_unbind_next()
            msg = "Volume "+repr(ticket["external_label"])
            self.send_user_last({"status" : "Mover: Retry: drive_error "+msg})
            #since we will kill ourselves, tell the library manager now....
            ticket = send_library_manager()
            raise "device panic -- I want to do no harm to media"

        # All is well - read has finished correctly

        # add some info to user's ticket
        ticket["volume_clerk"] = self.vticket
        minfo = {}
        for k in ['config_host', 'config_port', 'device', 'driver_name',
                  'library', 'library_device', 'library_manager_host',
                  'library_manager_port', 'media_changer', 'name' ] :
            exec("minfo["+repr(k)+"] = self."+k)
        ticket["mover"] = minfo
        dinfo = {}
        if 0:
            for k in ['blocksize', 'device', 'eod', 'firstbyte',
                      'left_to_read', 'pastbyte',
                      'rd_err', 'rd_access', 'remaining_bytes',
                      'wr_err', 'wr_access'] :
                exec("dinfo["+repr(k)+"] = self.driver."+k)
        ticket["driver"] = dinfo

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
    import string
    # Import SOCKS module if it exists, else standard socket module socket
    try:
        import SOCKS; socket = SOCKS
    except ImportError:
        import socket

    # defaults
    #config_host = "localhost"
    (config_host,ca,ci) = socket.gethostbyaddr(socket.gethostname())
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

    while 1:
        try:
            mv = Mover(config_host,config_port)
            mv.move_forever (args[0])
        except:
            continue
