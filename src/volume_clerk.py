import sys
import os
import time
from SocketServer import *
from configuration_client import *
from callback import send_to_user_callback
from dispatching_worker import DispatchingWorker
from generic_server import GenericServer
from journal import JournalDict

dict = JournalDict({},"volume_clerk.jou")

class VolumeClerkMethods(DispatchingWorker) :

    # add : some sort of hook to keep old versions of the s/w out
    # since we should like to have some control over format of the records.
    def addvol(self, ticket):
        # mandatory keys
        for key in  ['external_label', 'media_type', 'file_family',\
                     'library', 'eod_cookie', 'remaining_bytes', \
                     'capacity_bytes' ] :
            try:
                x = ticket[key]
            except KeyError:
                ticket["status"] = key+" is missing"
                self.reply_to_caller(ticket)
                return ticket

        # can't have 2 with same label
        external_label = ticket["external_label"]
        if dict.has_key(external_label) :
            ticket["status"] = "volume already exists"
            self.reply_to_caller(ticket)
            return ticket

        # optional keys - use default values if not there
        try:
            x = ticket['last_access']
        except KeyError:
            ticket["last_access"] = -1
        try:
            x = ticket['first_access']
        except KeyError:
            ticket["first_access"] = -1
        try:
            x = ticket['declared']
            if x == -1 :
                x = ticket['force_key_error_to_get_default']
        except KeyError:
            ticket["declared"] = time.time()
        try:
            x = ticket['error_inhibit']
        except KeyError:
            ticket["error_inhibit"] = "none"
        try:
            x = ticket['user_inhibit']
        except KeyError:
            ticket["user_inhibit"] = "none"
        try:
            x = ticket['sum_wr_err']
        except KeyError:
            ticket["sum_wr_err"] = 0
        try:
            x = ticket['sum_rd_err']
        except KeyError:
            ticket["sum_rd_err"] = 0
        try:
            x = ticket['sum_wr_mnt']
        except KeyError:
            ticket["sum_wr_mnt"] = 0
        try:
            x = ticket['sum_rd_mnt']
        except KeyError:
            ticket["sum_rd_mnt"] = 0
        try:
            x = ticket['wrapper']
        except KeyError:
            ticket["wrapper"] = "cpio"
        try:
            x = ticket['blocksize']
            if x == 0:
                x = ticket['force_key_error_to_get_default']
        except KeyError:
            sizes = self.csc.get("blocksizes")
            try:
                msize = sizes[ticket['media_type']]
            except :
                ticket['status'] = "unknown media type = unknown blocksize"
                self.reply_to_caller(ticket)
                return ticket
            ticket['blocksize'] = msize

        # write the ticket out to the database
        dict[external_label] = ticket
        ticket["status"] = "ok"
        self.reply_to_caller(ticket)


    def delvol(self, ticket):
        ticket["status"] = "ok"
        try:
            del dict[ticket["external_label"]]
        except KeyError:
            ticket["status"] = "no such volume"
        self.reply_to_caller(ticket)


    # Use volumes that satisfy criteria
    def next_write_volume (self, ticket) :
        exec ("vol_veto_list = " + ticket["vol_veto_list"])
        min_remaining_bytes = ticket["min_remaining_bytes"]
        library = ticket["library"]
        file_family = ticket["file_family"]

        try:
            first_found = ticket["first_found"]
        except KeyError:
            first_found = 0

        vol = {}
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

            # supposed to return first volume found?
            if first_found:
                v["status"] = "ok"
                self.reply_to_caller(v)
                return v
            # is this an "earlier" volume that one we already found?
            if len(vol) == 0 :
                vol = v
            elif v['declared'] < vol['declared'] :
                vol = v

        # return what we found
        if len(vol) != 0:
            vol["status"] = "ok"
            self.reply_to_caller(vol)
            return vol

        # nothing was available
        ticket["status"] = "no new volume"
        self.reply_to_caller(ticket)
        return ticket

    def set_remaining_bytes(self, ticket) :
        try:
            # get the current entry for the volume
            key = ticket["external_label"]
            record = dict[key]

            # update the fields that have changed
            record["remaining_bytes"] = ticket["remaining_bytes"]
            record["eod_cookie"] = ticket["eod_cookie"]
            record["error_inhibit"] = "none"
            record["last_access"] = time.time()
            if record["first_access"] == 1 :
                record["first_access"] = record["first_access"]
            record['sum_wr_err'] = record['sum_wr_err'] + ticket['sum_wr_err']
            record['sum_rd_err'] = record['sum_rd_err'] + ticket['sum_rd_err']
            record['sum_wr_mnt'] = record['sum_wr_mnt'] + ticket['sum_wr_mnt']
            record['sum_rd_mnt'] = record['sum_rd_mnt'] + ticket['sum_rd_mnt']

            # record our changes
            dict[key] = record
            record["status"] = "ok"

        except KeyError:
            record["status"] = "no such volume - or badly formed ticket"

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


