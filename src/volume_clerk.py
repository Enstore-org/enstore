import sys
import os
from SocketServer import *
from configuration_client import *
from callback import send_to_user_callback
from dispatching_worker import DispatchingWorker
from generic_server import GenericServer
from journal import JournalDict

dict = JournalDict({},"volume_clerk.jou")

class VolumeClerkMethods(DispatchingWorker) :

    # add : some sort of hook to keep old versions of the s/w out
    # since we should like to have some control over the format of the records.
    def addvol(self, ticket):
        external_label = ticket["external_label"]
        if dict.has_key(external_label) :
            ticket["status"] = "volume already exists"
            return ticket
        dict[external_label] = ticket
        ticket["status"] = "ok"
        self.reply_to_caller(ticket)
        return


    def delvol(self, ticket):
        ticket["status"] = "ok"
        try:
            del dict[ticket["external_label"]]
        except KeyError:
            ticket["status"] = "no such volume"
        self.reply_to_caller(ticket)
        return


    # I suppose, to use volumes in the order they were declared to us.
    def next_write_volume (self, ticket) :
        exec ("vol_veto_list = " + ticket["vol_veto_list"])
        min_remaining_bytes = ticket["min_remaining_bytes"]
        library = ticket["library"]
        file_family = ticket["file_family"]

        # go through the volumes and find one we can use for this request
        for k in dict.keys() :
            v = dict[k]
            if v["library"] != library :
                continue
            if v["file_family"] != file_family :
                continue
            if v["user_inhibit"] != "none" :
                continue
            if v["error_inhibit"] != "none" :
                continue
            if v["remaining_bytes"] < min_remaining_bytes :
                continue
            vetoed = 0
            extl = v["external_label"]
            for veto in vol_veto_list :
                if extl == veto :
                    vetoed = 1
                    break
            if vetoed :
                continue
            v["status"] = "ok"
            self.reply_to_caller(v)
            return

        # nothing was available
        ticket["status"] = "no new volume"
        self.reply_to_caller(ticket)


    def set_remaining_bytes(self, ticket) :
        try:
            key = ticket["external_label"]
            record = dict[key]
            record["remaining_bytes"] = ticket["remaining_bytes"]
            record["eod_cookie"] = ticket["eod_cookie"]
            record["error_inhibit"] = "none"
            dict[key] = record # THIS WILL JOURNAL IT
            record["status"] = "ok"
        except KeyError:
            record["status"] = "no such volume"
        self.reply_to_caller(record)


    def inquire_vol(self, ticket) :
        try:
            old = dict[ticket["external_label"]]
            ticket = old
            ticket["status"] = "ok"
        except KeyError:
            ticket["status"] = "no such volume"
        self.reply_to_caller(ticket)


    def set_writing(self, ticket) :
        try:
            key = ticket["external_label"]
            record = dict[key]
            record ["error_inhibit"] = "writing"
            dict[key] = record # THIS WILL JOURNAL IT
            record["status"] = "ok"
        except KeyError:
            record["status"] = "no such volume"
        self.reply_to_caller(record)
        return record

    # return all the volumes in our dictionary.  Not so useful!
    def get_vols(self,ticket) :
            self.reply_to_caller({"status" : "ok",\
                                  "vols"  :repr(dict.keys()) })

class VolumeClerk(VolumeClerkMethods, GenericServer, UDPServer) :
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

    keys = csc.get("volume_clerk")
    vs =  VolumeClerk((keys['host'], keys['port']), VolumeClerkMethods)
    vs.set_csc(csc)
    vs.serve_forever()


