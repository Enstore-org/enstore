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

#csc = configuration_client()
#u = UDPClient()

class Mover :
        def __init__(self, config_host="localhost", config_port=7500):
            self.config_host = config_host
            self.config_port = config_port
            self.csc = configuration_client(self.config_host,self.config_port)
            self.u = UDPClient()

        def move_forever(self, name) :
                self.name = name
                self.nowork({})
                while 1:
                        ticket = self.send_vol_manager()
                        try:
                                function = ticket["work"]
                        except KeyError:
                                raise "assert error Bogus stuff from vol mgr"
                        exec ("self." + function + "(ticket)")


        def send_vol_manager(self) :
                ticket = self.u.send(self.next_volmgr_request,
                                (self.library_manager_host,
                                 self.library_manager_port)
                                )
                return ticket

        def send_user_last(self, ticket):
                self.control_socket.send(dict_to_a(ticket))
                self.control_socket.close()
                #when I have debugged this, blow away any socket exceptions

        def nowork(self, ticket) :
                sleep(1)
                # An adminsitrator can change out configuration on the fly.
                # this is useful when a "library" is really one robot
                # with multiple uses, or when we need to manually load balance
                # movers attacke to virtual library....
                self.csc = configuration_client(self.config_host,self.config_port)
                mconfig = self.csc.get(self.name)
                if mconfig["status"] != "ok" :
                        raise "could not start mover up:" + mconfig["status"]
                self.library_device = mconfig["library_device"]
                self.driver_name = mconfig["driver"]
                self.device = mconfig["device"]
                self.library = mconfig["library"]
                self.media_changer = mconfig["media_changer"]

                # now get info asssociated with our volume manager
                self.csc = configuration_client(self.config_host,self.config_port)
                lconfig = self.csc.get_uncached(self.library)
                self.library_manager_host = lconfig["host"]
                self.library_manager_port = lconfig["port"]

                # announce ourselves
                self.idle_mover_next()


        def bind_volume(self, ticket) :
                self.external_label = ticket["external_label"]
                ss = VolumeClerkClient(self.csc)
                vticket = ss.inquire_vol(self.external_label)
                if vticket["status"] != "ok" :
                        self.unilateral_unbind_next()
                        return
                self.vticket = vticket
                self.driver = eval(self.driver_name + "('" +
                                  self.device + "','" +
                                  vticket["eod_cookie"] + "'," +
                                  `vticket["remaining_bytes"]` +
                                        ")")

                #print "Mover's name is " + self.name
                #print "creating media loader client: " + self.media_changer
                ml = MediaLoaderClient(self.csc, self.media_changer)
                #print "external_label",self.external_label
                #print "library_device",self.library_device
                lmticket = ml.loadvol(self.external_label, self.library_device)
                if lmticket["status"] != "ok" :
                        if lmticket["status"] == "media_in_another_device" :
                        # it is possible under normal functioning of the
                        #  system to be in the following race condition
                        #    "the volume mgr told another mover to unbind.
                        #    the mover responds promptly, but more work for the
                        #    volume mgr arrives and is given to a new mover
                        #    before the old volume was given back to the
                        #    library
                                sleep (10)
                        self.unilateral_unbind_next()
                        return

                self.driver.load()

                self.have_bound_volume_next()

        def unbind_volume(self, ticket) :

                ml = MediaLoaderClient(self.csc, self.media_changer)
                #
                # do any rewind unload or eject operations on the device
                #
                self.driver.unload()
                ticket = ml.unloadvol(self.external_label,
                                                self.library_device)
                if ticket["status"] != "ok" :
                        raise "media loader cannot unload my volume"

                # need to call the driver destructor....
                self.idle_mover_next()


        def get_user_sockets(self, ticket) :

                # get a port for the data transfer
                # tell the user I'm your mover and here's your ticket

                mover_host, mover_port, listen_socket = get_callback()
                listen_socket.listen(4)
                ticket["mover_callback_host"] = mover_host
                ticket["mover_callback_port"] = mover_port
                self.control_socket = user_callback_socket(ticket)

                # we expect a prompt call-back here, and should protect
                # against users not getting back to us. The best protection
                # would be to kick off if the user dropped the control_socket,
                # but I am at home and am not able to find documentation
                # on select...

                data_socket, address = listen_socket.accept()
                self.data_socket = data_socket
                listen_socket.close()

        def write_to_hsm(self, ticket) :

                if ticket["external_label"] != self.external_label :
                        raise "volume manager and I disagree on volume"
                ss = VolumeClerkClient(self.csc)
                vticket = ss.set_writing(self.external_label)
                if ticket["status"] != "ok" :
                        raise "volume clerk forgot about this volume"

                # space to where the file will begin and save location
                # information for where futrure reads will have to
                # space the drive to.

                self.get_user_sockets(ticket)


                nb = ticket["size_bytes"]
                bytes_recvd = 0
                media_error = 0
                media_full  = 0
                drive_error = 0
                user_send_error = 0
                anything_written = 0
                bof_space_cookie = 0
                self.driver.open_file_write()
                while 1:
                        buff = self.data_socket.recv(self.driver.get_iomax())
                        l = len(buff)
                        bytes_recvd = bytes_recvd + l
                        if l == 0 : break
                        self.driver.write_block(buff)
                        anything_written = 1

                self.data_socket.close()
                file_cookie = self.driver.close_file_write()
                eod_cookie = self.driver.get_eod_cookie()
                remaining_bytes = self.driver.get_eod_remaining_bytes()
                wr_err = 0
                rd_err = 0
                wr_mnt = 0
                rd_mnt = 0

                ss = VolumeClerkClient(self.csc)

                if bytes_recvd != ticket["size_bytes"] :
                        user_send_error = 1

                if user_send_error:
                        self.vticket = ss.set_remaining_bytes(
                                ticket["external_label"],
                                remaining_bytes,
                                eod_cookie,wr_err,rd_err,wr_mnt,rd_mnt)
                        self.have_bound_volume_next()
                        self.send_user_last({"status" : "user_protocol_error"})
                        return

                elif media_full :
                        ss.set_system_readonly(ticket["external_label"])
                        self.unilateral_unbind_next()
                        self.send_user_last({"status" : "retry"})
                        return

                elif media_error :
                        ss.set_system_readonly(ticket["external_label"])
                        self.send_user_last({"status" : "retry"})
                        self.unilateral_unbind_next()
                        return

                elif drive_error :
                        ss.set_hung(ticket["external_label"])
                        self.send_user_last({"status" : "retry"})
                        self.unilateral_unbind_next()
                        #since we will kill ourselves, tell the volume mgr
                        #now....
                        ticket = send_vol_manager()
                        raise "device panic -- I want to do no harm to media"
                        return

                # All is well.
                # Tell volume server
                self.vticket = ss.set_remaining_bytes(
                                ticket["external_label"],
                                remaining_bytes,
                                eod_cookie,wr_err,rd_err,wr_mnt,rd_mnt)


                # tell file clerk server

                fc = FileClerkClient(self.csc)
                fticket = fc.new_bit_file(
                        file_cookie,
                        ticket["external_label"], 0, 0, 0)

                # really only bfid is needed, but save other useful information for user too
                ticket["bfid"] = fticket["bfid"]
                ticket["bof_space_cookie"] = fticket["bof_space_cookie"]
                ticket["complete_crc"] = fticket["complete_crc"]
                ticket["beginning_crc"] = fticket["beginning_crc"]
                ticket["sanity_cookie"] = fticket["sanity_cookie"]
                ticket["device"] = self.device
                ticket["driver_name"] = self.driver_name
                ticket["mover_name"] = self.name
                ticket["library_device"] = self.library_device
                ticket["capacity_bytes"] = self.vticket["capacity_bytes"]
                ticket["remaining_bytes"] = self.vticket["remaining_bytes"]
                ticket["media_type"] = self.vticket["media_type"]

                # tell user
                self.send_user_last(ticket)
                # go around for more
                self.have_bound_volume_next()

        def read_from_hsm(self, ticket) :
                if ticket["external_label"] != self.external_label :
                        raise "volume manager and I disagree on volume"
                ss = VolumeClerkClient(self.csc)
                vticket = ss.inquire_vol(self.external_label)
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
                        ss.set_system_readonly(ticket["external_label"])
                        self.send_user_last({"status" : "retry"})
                        self.unilateral_unbind_next()
                        return

                elif drive_error :
                        ss.set_hung(ticket["external_label"])
                        self.send_user_last({"status" : "retry"})
                        self.unilateral_unbind_next()
                        #since we will kill ourselves, tell the volume mgr
                        #now....
                        ticket = send_vol_manager()
                        raise "device panic -- I want to do no harm to media"
                        return

                # read has finished correctly
                # tell user
                self.send_user_last(ticket)
                # go around for more
                self.have_bound_volume_next()


        def idle_mover_next(self):
                self.next_volmgr_request = {"work" : "idle_mover",
                        "mover" : self.name
                        }

        def have_bound_volume_next(self):
                self.next_volmgr_request = {}
                for k in self.vticket.keys() :
                        self.next_volmgr_request[k] = self.vticket[k]
                self.next_volmgr_request["work"] = "have_bound_volume"
                self.next_volmgr_request["mover"] = self.name

        def unilateral_unbind_next(self):
                self.next_volmgr_request = {"work" : "unilateral_unbind",
                        "external_label" : self.external_label,
                        "mover" : self.name
                        }


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




