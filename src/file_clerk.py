import sys
import os
import time
import pprint
from dict_to_a import *
from SocketServer import *
from configuration_client import *
from volume_clerk_client import VolumeClerkClient
from library_manager_client import LibraryManagerClient
from dispatching_worker import DispatchingWorker
from generic_server import GenericServer
from udp_client import UDPClient
from journal import JournalDict

dict = JournalDict({}, "file_clerk.jou")

class FileClerkMethods(DispatchingWorker) :

    # we need a new bit field id for each new file in the system
    def new_bit_file(self, ticket) :
        bfid = self.unique_bit_file_id()
        dict[bfid] = ticket
        ticket["bfid"] = bfid
        ticket["status"] = "ok"
        self.reply_to_caller(ticket)

    # To read from the hsm, we need to verify that the bit file id is ok,
    # call the volume server to find the library, and copy to the work
    # ticket the salient information
    def read_from_hsm(self, ticket) :

        try :
            # look up in our dictionary the request bit field id
            finfo = dict[ticket["bfid"]]
            # copy all file information we have to user's ticket
            ticket["external_label"]   = finfo["external_label"]
            ticket["bof_space_cookie"] = finfo["bof_space_cookie"]
            ticket["external_label"]   = finfo["external_label"]
            ticket["sanity_cookie"]    = finfo["sanity_cookie"]
            ticket["complete_crc"]     = finfo["complete_crc"]
            ticket["beginning_crc"]    = finfo["beginning_crc"]

        except KeyError :
            self.reply_to_caller({"status" : "bfid not found" })
            return

        # found the bit file id, now go and find the library
        # become a client of the volume clerk server first
        vc = VolumeClerkClient(self.csc)

        # ask the volume clerk server which library has "external_label" in it
        vticket = vc.inquire_vol(ticket["external_label"])
        if vticket["status"] != "ok" :
            self.reply_to_caller(vticket)
            return
        library = vticket["library"]

        # got the library, now send it to the apropos library manager
        # we get the library manager from our configuration server, of course
        vmticket = csc.get(library)
        if vmticket["status"] != "ok" :
            self.reply_to_caller(vmticket)
            return
        u = UDPClient()

        # send to library manager and tell user
        ticket = u.send(ticket, (vmticket['host'], vmticket['port']))
        self.reply_to_caller(ticket)

    # return all the bfids in our dictionary.  Not so useful!
    def get_bfids(self,ticket) :
            self.reply_to_caller({"status" : "ok",\
                                  "bfids"  :repr(dict.keys()) })

    # return all info about a certain bfid - this does everything that the
    # read_from_hsm method does, except send the ticket to the library manager
    def bfid_info(self, ticket) :

        try :
            # look up in our dictionary the request bit field id
            finfo = dict[ticket["bfid"]]
            # copy all file information we have to user's ticket
            ticket["external_label"]   = finfo["external_label"]
            ticket["bof_space_cookie"] = finfo["bof_space_cookie"]
            ticket["external_label"]   = finfo["external_label"]
            ticket["sanity_cookie"]    = finfo["sanity_cookie"]
            ticket["complete_crc"]     = finfo["complete_crc"]
            ticket["beginning_crc"]    = finfo["beginning_crc"]

        except KeyError :
            self.reply_to_caller({"status" : "bfid not found" })
            return

        # found the bit file id, now go and find the library
        # become a client of the volume clerk server first
        vc = VolumeClerkClient(self.csc)

        # ask the volume clerk server which library has "external_label" in it
        vticket = vc.inquire_vol(ticket["external_label"])
        if vticket["status"] != "ok" :
            self.reply_to_caller(vticket)
            return

        # ok, now merge the dictionaries and return to user
        # make sure original ticket values override vticket values
        self.reply_to_caller(merge_dicts(vticket,ticket))

    # A bit file id is defined to be a 64-bit number whose most significant
    # part is based on the time, and the least significant part is a count
    # to make it unique
    def unique_bit_file_id(self) :
        bfid = time.time()
        bfid = long(bfid)*100000
        while dict.has_key(repr(bfid)) :
            bfid = bfid + 1
        return repr(bfid)


class FileClerk(FileClerkMethods, GenericServer, UDPServer) :
    pass

if __name__ == "__main__" :
    import getopt
    import socket

    # defaults
    config_host = "localhost"
    #(config_host,ca,ci) = socket.gethostbyaddr(socket.gethostname())
    config_port = "7500"
    config_list = 0

    # see what the user has specified. bomb out if wrong options specified
    options = ["config_host=","config_port="\
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

    #   pretend that we are the test system
    #   remember, in a system, there is only one bfs
    #   get our port and host from the name server
    #   exit if the host is not this machine
    ticket = csc.get("file_clerk")
    cs = FileClerk( (ticket["host"], ticket["port"]), FileClerkMethods)
    cs.set_csc(csc)
    cs.serve_forever()
