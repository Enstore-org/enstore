import sys
import time
import timeofday
import pprint
import copy
import log_client
import traceback
from configuration_client import configuration_client
from volume_clerk_client import VolumeClerkClient
from library_manager_client import LibraryManagerClient
from dispatching_worker import DispatchingWorker
from SocketServer import UDPServer, TCPServer
from generic_server import GenericServer
from udp_client import UDPClient
from db import dBTable



class FileClerkMethods(DispatchingWorker) :

    # we need a new bit field id for each new file in the system
    def new_bit_file(self, ticket) :
     try:
        # create empty record and control what goes into database
        # do not pass ticket, for example to the database!
        record = {}
        record["external_label"]   = ticket["external_label"]
        record["bof_space_cookie"] = ticket["bof_space_cookie"]
        record["sanity_cookie"]    = ticket["sanity_cookie"]
        record["complete_crc"]     = ticket["complete_crc"]

        # get a new bit fit id
        bfid = self.unique_bit_file_id()
        record["bfid"] = bfid

        # record it to the database
        dict[bfid] = copy.deepcopy(record)

        ticket["bfid"] = bfid
        ticket["status"] = "ok"
        self.reply_to_caller(ticket)
        return

     # even if there is an error - respond to caller so he can process it
     except:
         ticket["status"] = sys.exc_info()[0]+sys.exc_info()[1]
         pprint.pprint(ticket)
         self.reply_to_caller(ticket)
         return


    # To read from the hsm, we need to verify that the bit file id is ok,
    # call the volume server to find the library, and copy to the work
    # ticket the salient information
    def read_from_hsm(self, ticket) :
     try:
        # everything is based on bfid - make sure we have this
        try:
            key="bfid"
            bfid = ticket[key]
        except KeyError:
            ticket["status"] = "File Clerk: "+key+" key is missing"
            pprint.pprint(ticket)
            self.reply_to_caller(ticket)
            return

        # look up in our dictionary the request bit field id
        try :
            finfo = copy.deepcopy(dict[bfid])
        except KeyError :
            ticket["status"] = "File Clerk: bfid "+repr(bfid)+" not found"
            pprint.pprint(ticket)
            self.reply_to_caller(ticket)
            return

        # copy all file information we have to user's ticket
        for key in finfo.keys() :
            ticket[key] = finfo[key]
        ticket["file_clerk"] = finfo

        # become a client of the volume clerk to get library information
        vcc = VolumeClerkClient(self.csc)

        # everything is based on external label - make sure we have this
        try:
            key="external_label"
            external_label = ticket[key]
        except KeyError:
            ticket["status"] = "File Clerk: "+key+" key is missing"
            pprint.pprint(ticket)
            self.reply_to_caller(ticket)
            return

        # ask the volume clerk server which library has "external_label" in it
        vticket = vcc.inquire_vol(external_label)
        if vticket["status"] != "ok" :
            pprint.pprint(ticket)
            self.reply_to_caller(vticket)
            return
        library = vticket["library"]

        # get the library manager
        vmticket = csc.get(library+".library_manager")
        if vmticket["status"] != "ok" :
            pprint.pprint(ticket)
            self.reply_to_caller(vmticket)
            return

        # send to library manager and tell user
        u = UDPClient()
        ticket = u.send(ticket, (vmticket['host'], vmticket['port']))
        self.reply_to_caller(ticket)
        return

     # even if there is an error - respond to caller so he can process it
     except:
         ticket["status"] = sys.exc_info()[0]+sys.exc_info()[1]
         pprint.pprint(ticket)
         self.reply_to_caller(ticket)
         return



    # return all the bfids in our dictionary.  Not so useful!
    def get_bfids(self,ticket) :
     try:
        self.reply_to_caller({"status" : "ok",\
                              "bfids"  :repr(dict.keys()) })
        return

     # even if there is an error - respond to caller so he can process it
     except:
         ticket["status"] = sys.exc_info()[0]+sys.exc_info()[1]
         pprint.pprint(ticket)
         self.reply_to_caller(ticket)
         return


    # return all info about a certain bfid - this does everything that the
    # read_from_hsm method does, except send the ticket to the library manager
    def bfid_info(self, ticket) :
     try:
        # everything is based on bfid - make sure we have this
        try:
            key="bfid"
            bfid = ticket[key]
        except KeyError:
            ticket["status"] = "File Clerk: "+key+" key is missing"
            pprint.pprint(ticket)
            self.reply_to_caller(ticket)
            return

        # look up in our dictionary the request bit field id
        try :
            finfo = copy.deepcopy(dict[bfid])
        except KeyError :
            ticket["status"] = "File Clerk: bfid "+repr(bfid)+" not found"
            pprint.pprint(ticket)
            self.reply_to_caller(ticket)
            return

        # copy all file information we have to user's ticket
        ticket["file_clerk"] = finfo

        # become a client of the volume clerk to get library information
        vcc = VolumeClerkClient(self.csc)

        # everything is based on external label - make sure we have this
        try:
            key="external_label"
            external_label = finfo[key]
        except KeyError:
            ticket["status"] = "File Clerk: "+key+" key is missing"
            pprint.pprint(ticket)
            self.reply_to_caller(ticket)
            return

        # ask the volume clerk server which library has "external_label" in it
        vticket = vcc.inquire_vol(external_label)
        if vticket["status"] != "ok" :
            pprint.pprint(ticket)
            self.reply_to_caller(vticket)
            return
        library = vticket["library"]

        # copy all volume information we have to user's ticket
        ticket["volume_clerk"] = vticket

        ticket["status"] = "ok"
        self.reply_to_caller(ticket)
        return

     # even if there is an error - respond to caller so he can process it
     except:
         ticket["status"] = sys.exc_info()[0]+sys.exc_info()[1]
         pprint.pprint(ticket)
         self.reply_to_caller(ticket)
         return

    # A bit file id is defined to be a 64-bit number whose most significant
    # part is based on the time, and the least significant part is a count
    # to make it unique
    def unique_bit_file_id(self) :
     try:
        bfid = time.time()
        bfid = long(bfid)*100000
        while dict.has_key(repr(bfid)) :
            bfid = bfid + 1
        return repr(bfid)
     # even if there is an error - respond to caller so he can process it
     except:
         print "can not generate a bit file id!!",sys.exc_info()[0]+sys.exc_info()[1]
         sys.exit(1)
    def start_backup(self,ticket):
        dict.start_backup()
        self.reply_to_caller({"status" : "ok",\
                "start_backup"  : 'yes' })
    def stop_backup(self,ticket):
        dict.stop_backup()
        self.reply_to_caller({"status" : "ok",\
                "stop_backup"  : 'yes' })

class FileClerk(FileClerkMethods, GenericServer, UDPServer) :
    pass

if __name__ == "__main__" :
    import sys
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
    keys = csc.get("file_clerk")
    fc = FileClerk( (keys["host"], keys["port"]), FileClerkMethods)
    fc.set_csc(csc)

    # get a logger
    logc = log_client.LoggerClient(csc, keys["logname"], 'logserver', 0)
    fc.set_logc(logc)
    dict = dBTable("file",logc)
    while 1:
        try:
            logc.send(log_client.INFO, "File Clerk (re)starting")
            fc.serve_forever()
        except:
            traceback.print_exc()
            format = timeofday.tod()+" "+\
                     str(sys.argv)+" "+\
                     str(sys.exc_info()[0])+" "+\
                     str(sys.exc_info()[1])+" "+\
                     "file clerk serve_forever continuing"
            print format
            logc.send(log_client.INFO,format)
            continue
