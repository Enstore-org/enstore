import time
from SocketServer import UDPServer, TCPServer
from configuration_client import configuration_client
from dispatching_worker import DispatchingWorker
from generic_server import GenericServer
from journal import JournalDict
import pprint

dict = JournalDict({},"volume_clerk.jou")

class VolumeClerkMethods(DispatchingWorker) :

    # add : some sort of hook to keep old versions of the s/w out
    # since we should like to have some control over format of the records.
    def addvol(self, ticket):
        # create empty record and control what goes into database
        # do not pass ticket, for example to the database!
        record={}

        # everything is based on external label - make sure we have this
        try:
            key="external_label"
            external_label = ticket[key]
        except KeyError:
            ticket["status"] = "Volume Clerk: "+key+" key is missing"
            pprint.pprint(ticket)
            self.reply_to_caller(ticket)
            return

        # can't have 2 with same label
        if dict.has_key(external_label) :
            ticket["status"] = "Volume Clerk: volume "+external_label\
                               +" already exists"
            pprint.pprint(ticket)
            self.reply_to_caller(ticket)
            return

        # mandatory keys
        for key in  ['external_label','media_type', 'file_family', 'library',\
                     'eod_cookie', 'remaining_bytes', 'capacity_bytes' ] :
            try:
                record[key] = ticket[key]
            except KeyError:
                ticket["status"] = "Volume Clerk: "+key+" is missing"
                pprint.pprint(ticket)
                self.reply_to_caller(ticket)
                return

        # optional keys - use default values if not specified
        try:
            record['last_access'] = ticket['last_access']
        except KeyError:
            record["last_access"] = -1
        try:
            record['first_access'] = ticket['first_access']
        except KeyError:
            record["first_access"] = -1
        try:
            record['declared'] = ticket['declared']
            if record['declared'] == -1 :
                x = ticket['force_key_error_to_get_except']
        except KeyError:
            record["declared"] = time.time()
        try:
            record['system_inhibit'] = ticket['system_inhibit']
        except KeyError:
            record["system_inhibit"] = "none"
        try:
            record['user_inhibit'] = ticket['user_inhibit']
        except KeyError:
            record["user_inhibit"] = "none"
        try:
            record['sum_wr_err'] = ticket['sum_wr_err']
        except KeyError:
            record["sum_wr_err"] = 0
        try:
            record['sum_rd_err'] = ticket['sum_rd_err']
        except KeyError:
            record["sum_rd_err"] = 0
        try:
            record['sum_wr_access'] = ticket['sum_wr_access']
        except KeyError:
            record["sum_wr_access"] = 0
        try:
            record['sum_rd_access'] = ticket['sum_rd_access']
        except KeyError:
            record["sum_rd_access"] = 0
        try:
            record['wrapper'] = ticket['wrapper']
        except KeyError:
            record["wrapper"] = "cpio"
        try:
            record['blocksize'] = ticket['blocksize']
            if record['blocksize'] == -1:
                x = ticket['force_key_error_to_get_except']
        except KeyError:
            sizes = self.csc.get("blocksizes")
            try:
                msize = sizes[ticket['media_type']]
            except :
                ticket['status'] = "Volume Clerk: "\
                                   +"unknown media type = unknown blocksize"
                pprint.pprint(ticket)
                self.reply_to_caller(ticket)
                return
            record['blocksize'] = msize

        # write the ticket out to the database
        dict[external_label] = record
        ticket["status"] = "ok"
        self.reply_to_caller(ticket)


    # delete a volume from the database
    def delvol(self, ticket):

        # everything is based on external label - make sure we have this
        key="external_label"
        try:
            external_label = ticket[key]
        except KeyError:
            ticket["status"] = "Volume Clerk: "+key+" key is missing"
            pprint.pprint(ticket)
            self.reply_to_caller(ticket)
            return

        # delete if from the database
        try:
            del dict[external_label]
            ticket["status"] = "ok"
        except KeyError:
            ticket["status"] = "Volume Clerk: volume "+external_label\
                               +" no such volume"
            pprint.pprint(ticket)

        self.reply_to_caller(ticket)


    # Get the next volume that satisfy criteria
    def next_write_volume (self, ticket) :
        # make sure we have this vol_veto_list
        key="vol_veto_list"
        try:
            vol_veto = ticket[key]
        except KeyError:
            ticket["status"] = "Volume Clerk: "+key+" key is missing"
            pprint.pprint(ticket)
            self.reply_to_caller(ticket)
            return
        exec ("vol_veto_list = " + vol_veto)

        # get the criteria for the volume from the user's ticket
        try:
            key = "min_remaining_bytes"
            min_remaining_bytes = ticket[key]
            key = "library"
            library = ticket[key]
            key = "file_family"
            file_family = ticket[key]
            key = "first_found"
            first_found = ticket[key]
        except KeyError:
            ticket["status"] = "Volume Clerk: "+key+" is missing"
            pprint.pprint(ticket)
            self.reply_to_caller(ticket)
            return

        # go through the volumes and find one we can use for this request
        vol = {}
        for k in dict.keys() :
            v = dict[k]
            if v["library"] != library :
                continue
            if v["file_family"] != file_family :
                continue
            if v["user_inhibit"] != "none" :
                continue
            if v["system_inhibit"] != "none" :
                continue
            if v["remaining_bytes"] < min_remaining_bytes :
                # if it __ever__ happens that we can't write a file on a
                # volume, then mark volume as full.  This prevents us from
                # putting 1 byte files on old "golden" volumes and potentially
                # losing the entire tape. One could argue that a very large
                # file write could prematurely flag a volume as full, but lets
                # worry about if it is really a problem - I propose that an
                # administrator reset the system_inhibit back to none in these
                # special, and hopefully rare cases.
                v["system_inhibit"] = "full"
                left = v["remaining_bytes"]/1.
                totb = v["capacity_bytes"]/1.
                if totb != 0 :
                    waste = left/totb*100.
                print k,"is now full, bytes remaining = ",left,\
                      "wasted=",waste,"%"
                dict[k] = v
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
                return
            # if not, is this an "earlier" volume that one we already found?
            if len(vol) == 0 :
                vol = v
            elif v['declared'] < vol['declared'] :
                vol = v

        # return what we found
        if len(vol) != 0:
            vol["status"] = "ok"
            self.reply_to_caller(vol)
            return

        # nothing was available
        ticket["status"] = "Volume Clerk: no new volumes available"
        pprint.pprint(ticket)
        self.reply_to_caller(ticket)
        return


    # update the database entry for this volume
    def set_remaining_bytes(self, ticket) :

        # everything is based on external label - make sure we have this
        try:
            key="external_label"
            external_label = ticket[key]
        except KeyError:
            ticket["status"] = "Volume Clerk: "+key+" key is missing"
            pprint.pprint(ticket)
            self.reply_to_caller(ticket)
            return

        # get the current entry for the volume
        try:
            record = dict[external_label]
        except KeyError:
            ticket["status"] = "Volume Clerk: volume "+external_label\
                               +" no such volume"
            pprint.pprint(ticket)
            self.reply_to_caller(ticket)
            return

        # update the fields that have changed
        try:
            for key in ["remaining_bytes","eod_cookie"] :
                record[key] = ticket[key]
        except KeyError:
            ticket["status"] = "Volume Clerk: "+key+" key is missing"
            pprint.pprint(ticket)
            self.reply_to_caller(ticket)
            return

        record["system_inhibit"] = "none"
        record["last_access"] = time.time()
        if record["first_access"] == -1 :
            record["first_access"] = record["last_access"]

        for key in ['wr_err','rd_err','wr_access','rd_access'] :
            try:
                record['sum_'+key] = record['sum_'+key] + ticket[key]
            except KeyError:
                ticket["status"] = "Volume Clerk: "+key+" key is missing"
                pprint.pprint(ticket)
                self.reply_to_caller(ticket)
                return

        # record our changes
        dict[external_label] = record
        record["status"] = "ok"

        self.reply_to_caller(record)


    # update the database entry for this volume
    def update_counts(self, ticket) :

        # everything is based on external label - make sure we have this
        try:
            key="external_label"
            external_label = ticket[key]
        except KeyError:
            ticket["status"] = "Volume Clerk: "+key+" key is missing"
            pprint.pprint(ticket)
            self.reply_to_caller(ticket)
            return

        # get the current entry for the volume
        try:
            record = dict[external_label]
        except KeyError:
            ticket["status"] = "Volume Clerk: volume "+external_label\
                               +" no such volume"
            pprint.pprint(ticket)
            self.reply_to_caller(ticket)
            return

        # update the fields that have changed
        record["last_access"] = time.time()
        if record["first_access"] == -1 :
            record["first_access"] = record["last_access"]

        for key in ['wr_err','rd_err','wr_access','rd_access'] :
            try:
                record['sum_'+key] = record['sum_'+key] + ticket[key]
            except KeyError:
                ticket["status"] = "Volume Clerk: "+key+" key is missing"
                pprint.pprint(ticket)
                self.reply_to_caller(ticket)
                return

        # record our changes
        dict[external_label] = record
        record["status"] = "ok"

        self.reply_to_caller(record)

    # get the current database volume about a specific entry
    def inquire_vol(self, ticket) :
        # everything is based on external label - make sure we have this
        try:
            key="external_label"
            external_label = ticket[key]
        except KeyError:
            ticket["status"] = "Volume Clerk: "+key+" key is missing"
            pprint.pprint(ticket)
            self.reply_to_caller(ticket)
            return

        # get the current entry for the volume
        try:
            record = dict[external_label]
            record["status"] = "ok"
            self.reply_to_caller(record)
        except KeyError:
            ticket["status"] = "Volume Clerk: volume "+external_label\
                               +" no such volume"
            pprint.pprint(ticket)
            self.reply_to_caller(ticket)


    # flag the database that we are now writing the system
    def set_writing(self, ticket) :
        # everything is based on external label - make sure we have this
        try:
            key="external_label"
            external_label = ticket[key]
        except KeyError:
            ticket["status"] = "Volume Clerk: "+key+" key is missing"
            pprint.pprint(ticket)
            self.reply_to_caller(ticket)
            return

        # get the current entry for the volume
        try:
            record = dict[external_label]
        except KeyError:
            ticket["status"] = "Volume Clerk: volume "+external_label\
                               +" no such volume"
            pprint.pprint(ticket)
            self.reply_to_caller(ticket)
            return

        # update the fields that have changed
        record ["system_inhibit"] = "writing"
        dict[external_label] = record # THIS WILL JOURNAL IT

        record["status"] = "ok"
        self.reply_to_caller(record)

    # flag that the current volume is readonly
    def set_system_readonly(self, ticket) :
        # everything is based on external label - make sure we have this
        try:
            key="external_label"
            external_label = ticket[key]
        except KeyError:
            ticket["status"] = "Volume Clerk: "+key+" key is missing"
            pprint.pprint(ticket)
            self.reply_to_caller(ticket)
            return

        # get the current entry for the volume
        try:
            record = dict[external_label]
        except KeyError:
            ticket["status"] = "Volume Clerk: volume "+external_label\
                               +" no such volume"
            pprint.pprint(ticket)
            self.reply_to_caller(ticket)
            return

        # update the fields that have changed
        record ["system_inhibit"] = "readonly"
        dict[external_label] = record # THIS WILL JOURNAL IT

        record["status"] = "ok"
        self.reply_to_caller(record)


    # device is broken - what to do, what to do
    def set_hung(self,ticket) :
        print "set_hung",pprint.pformat(ticket)
        self.reply_to_caller({"status" : "ok"})


    # return all the volumes in our dictionary.  Not so useful!
    def get_vols(self,ticket) :
            self.reply_to_caller({"status" : "ok",\
                                  "vols"  :repr(dict.keys()) })


class VolumeClerk(VolumeClerkMethods, GenericServer, UDPServer) :
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
