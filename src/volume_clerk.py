###############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
import sys
import os
import time
import copy
import pprint

# enstore imports
import timeofday
import callback
import log_client
import traceback
import SocketServer
import configuration_client
import dispatching_worker
import generic_server
import db
import Trace

class VolumeClerkMethods(dispatching_worker.DispatchingWorker):

    # add: some sort of hook to keep old versions of the s/w out
    # since we should like to have some control over format of the records.
    def addvol(self, ticket):
     try:
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
        if dict.has_key(external_label):
            ticket["status"] = "Volume Clerk: volume "+external_label\
                               +" already exists"
            pprint.pprint(ticket)
            self.reply_to_caller(ticket)
            return

        # mandatory keys
        for key in  ['external_label','media_type', 'file_family', 'library',\
                     'eod_cookie', 'remaining_bytes', 'capacity_bytes' ]:
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
            if record['declared'] == -1:
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
            except:
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
        return

     # even if there is an error - respond to caller so he can process it
     except:
         ticket["status"] = str(sys.exc_info()[0])+str(sys.exc_info()[1])
         pprint.pprint(ticket)
         self.reply_to_caller(ticket)
         return


    # delete a volume from the database
    def delvol(self, ticket):
     try:
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
        return

     # even if there is an error - respond to caller so he can process it
     except:
         ticket["status"] = str(sys.exc_info()[0])+str(sys.exc_info()[1])
         pprint.pprint(ticket)
         self.reply_to_caller(ticket)
         return


    # Get the next volume that satisfy criteria
    def next_write_volume (self, ticket):
     try:
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
        while 1:
            label=dict.next()
            if label:
                pass
            else:
                break
            v = copy.deepcopy(dict[label])
            #pprint.pprint(v)
            if v["library"] != library:
                continue
            if v["file_family"] != file_family:
                continue
            if v["user_inhibit"] != "none":
                continue
            if v["system_inhibit"] != "none":
                continue
            if v["remaining_bytes"] < min_remaining_bytes:
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
                if totb != 0:
                    waste = left/totb*100.
                print label,"is now full, bytes remaining = ",left,\
                      "wasted=",waste,"%"
                dict[label] = copy.deepcopy(v)
                continue
            vetoed = 0
            for veto in vol_veto_list:
                if label == veto:
                    vetoed = 1
                    break
            if vetoed:
                continue

            # supposed to return first volume found?
            if first_found:
                v["status"] = "ok"
                self.reply_to_caller(v)
                return
            # if not, is this an "earlier" volume that one we already found?
            if len(vol) == 0:
                vol = copy.deepcopy(v)
            elif v['declared'] < vol['declared']:
                vol = copy.deepcopy(v)

        # return what we found
        if len(vol) != 0:
            vol["status"] = "ok"
            self.reply_to_caller(vol)
            return

        # nothing was available - see if we can assign a blank one.
        vol = {}
        while 1:
            label=dict.next()
            if label:
                pass
            else:
                break
            v = copy.deepcopy(dict[label])
            if v["library"] != library:
                continue
            if v["file_family"] != "none":
                continue
            if v["user_inhibit"] != "none":
                continue
            if v["system_inhibit"] != "none":
                continue
            if v["remaining_bytes"] < min_remaining_bytes:
                continue
            vetoed = 0
            label = v["external_label"]
            for veto in vol_veto_list:
                if label == veto:
                    vetoed = 1
                    break
            if vetoed:
                continue

            # supposed to return first blank volume found?
            if first_found:
                v["file_family"] = file_family
                logc.send(log_client.INFO,2,
                  "Assigning blank volume"+label+"to"+library+" "+file_family)
                dict[label] = copy.deepcopy(v)
                v["status"] = "ok"
                self.reply_to_caller(v)
                return
            # if not, is this an "earlier" volume that one we already found?
            if len(vol) == 0:
                vol = copy.deepcopy(v)
            elif v['declared'] < vol['declared']:
                vol = copy.deepcopy(v)

        # return blank volume we found
        if len(vol) != 0:
            label = vol['external_label']
            vol["file_family"] = file_family
            logc.send(log_client.INFO,2,
                  "Assigning blank volume"+label+"to"+library+" "+file_family)
            dict[label] = copy.deepcopy(vol)
            vol["status"] = "ok"
            self.reply_to_caller(vol)
            return

        # nothing was available at all
        ticket["status"] = "Volume Clerk: no new volumes available"
        logc.send(log_client.ERROR,1, "No blank volumes"+str(ticket) )
        self.reply_to_caller(ticket)
        return

     # even if there is an error - respond to caller so he can process it
     except:
        ticket["status"] = str(sys.exc_info()[0])+str(sys.exc_info()[1])
        logc.send(log_client.ERROR,1, str(ticket) )
        self.reply_to_caller(ticket)
        return


    # update the database entry for this volume
    def set_remaining_bytes(self, ticket):
     try:
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
            record = copy.deepcopy(dict[external_label])
        except KeyError:
            ticket["status"] = "Volume Clerk: volume "+external_label\
                               +" no such volume"
            pprint.pprint(ticket)
            self.reply_to_caller(ticket)
            return

        # update the fields that have changed
        try:
            for key in ["remaining_bytes","eod_cookie"]:
                record[key] = ticket[key]
        except KeyError:
            ticket["status"] = "Volume Clerk: "+key+" key is missing"
            pprint.pprint(ticket)
            self.reply_to_caller(ticket)
            return

        record["system_inhibit"] = "none"
        record["last_access"] = time.time()
        if record["first_access"] == -1:
            record["first_access"] = record["last_access"]

        for key in ['wr_err','rd_err','wr_access','rd_access']:
            try:
                record['sum_'+key] = record['sum_'+key] + ticket[key]
            except KeyError:
                ticket["status"] = "Volume Clerk: "+key+" key is missing"
                pprint.pprint(ticket)
                self.reply_to_caller(ticket)
                return

        # record our changes
        dict[external_label] = copy.deepcopy(record)
        record["status"] = "ok"
        self.reply_to_caller(record)
        return

     # even if there is an error - respond to caller so he can process it
     except:
         ticket["status"] = str(sys.exc_info()[0])+str(sys.exc_info()[1])
         pprint.pprint(ticket)
         self.reply_to_caller(ticket)
         return


    # update the database entry for this volume
    def update_counts(self, ticket):
     try:
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
            record = copy.deepcopy(dict[external_label])
        except KeyError:
            ticket["status"] = "Volume Clerk: volume "+external_label\
                               +" no such volume"
            pprint.pprint(ticket)
            self.reply_to_caller(ticket)
            return

        # update the fields that have changed
        record["last_access"] = time.time()
        if record["first_access"] == -1:
            record["first_access"] = record["last_access"]

        for key in ['wr_err','rd_err','wr_access','rd_access']:
            try:
                record['sum_'+key] = record['sum_'+key] + ticket[key]
            except KeyError:
                ticket["status"] = "Volume Clerk: "+key+" key is missing"
                pprint.pprint(ticket)
                self.reply_to_caller(ticket)
                return

        # record our changes
        dict[external_label] = copy.deepcopy(record)
        record["status"] = "ok"
        self.reply_to_caller(record)
        return

     # even if there is an error - respond to caller so he can process it
     except:
         ticket["status"] = str(sys.exc_info()[0])+str(sys.exc_info()[1])
         pprint.pprint(ticket)
         self.reply_to_caller(ticket)
         return


    # get the current database volume about a specific entry
    def inquire_vol(self, ticket):
     try:
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
            record = copy.deepcopy(dict[external_label])
            record["status"] = "ok"
            self.reply_to_caller(record)
            return
        except KeyError:
            ticket["status"] = "Volume Clerk: volume "+external_label\
                               +" no such volume"
            pprint.pprint(ticket)
            self.reply_to_caller(ticket)
            return

     # even if there is an error - respond to caller so he can process it
     except:
         ticket["status"] = str(sys.exc_info()[0])+str(sys.exc_info()[1])
         pprint.pprint(ticket)
         self.reply_to_caller(ticket)
         return


    # flag the database that we are now writing the system
    def clr_system_inhibit(self, ticket):
     try:
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
            record = copy.deepcopy(dict[external_label])
        except KeyError:
            ticket["status"] = "Volume Clerk: volume "+external_label\
                               +" no such volume"
            pprint.pprint(ticket)
            self.reply_to_caller(ticket)
            return

        # update the fields that have changed
        record ["system_inhibit"] = "none"
        dict[external_label] = copy.deepcopy(record) # THIS WILL JOURNAL IT
        record["status"] = "ok"
        self.reply_to_caller(record)
        return

     # even if there is an error - respond to caller so he can process it
     except:
         ticket["status"] = str(sys.exc_info()[0])+str(sys.exc_info()[1])
         pprint.pprint(ticket)
         self.reply_to_caller(ticket)
         return


    # flag the database that we are now writing the system
    def set_writing(self, ticket):
     try:
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
            record = copy.deepcopy(dict[external_label])
        except KeyError:
            ticket["status"] = "Volume Clerk: volume "+external_label\
                               +" no such volume"
            pprint.pprint(ticket)
            self.reply_to_caller(ticket)
            return

        # update the fields that have changed
        record ["system_inhibit"] = "writing"
        dict[external_label] = copy.deepcopy(record) # THIS WILL JOURNAL IT
        record["status"] = "ok"
        self.reply_to_caller(record)
        return

     # even if there is an error - respond to caller so he can process it
     except:
         ticket["status"] = str(sys.exc_info()[0])+str(sys.exc_info()[1])
         pprint.pprint(ticket)
         self.reply_to_caller(ticket)
         return


    # flag that the current volume is readonly
    def set_system_readonly(self, ticket):
     try:
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
            record = copy.deepcopy(dict[external_label])
        except KeyError:
            ticket["status"] = "Volume Clerk: volume "+external_label\
                               +" no such volume"
            pprint.pprint(ticket)
            self.reply_to_caller(ticket)
            return

        # update the fields that have changed
        record ["system_inhibit"] = "readonly"
        dict[external_label] = copy.deepcopy(record) # THIS WILL JOURNAL IT
        record["status"] = "ok"
        self.reply_to_caller(record)
        return

     # even if there is an error - respond to caller so he can process it
     except:
         ticket["status"] = str(sys.exc_info()[0])+str(sys.exc_info()[1])
         pprint.pprint(ticket)
         self.reply_to_caller(ticket)
         return


    # device is broken - what to do, what to do
    def set_hung(self,ticket):
     try:
        self.reply_to_caller({"status" : "ok"})
        return

     # even if there is an error - respond to caller so he can process it
     except:
         ticket["status"] = str(sys.exc_info()[0])+str(sys.exc_info()[1])
         pprint.pprint(ticket)
         self.reply_to_caller(ticket)
         return


    # return all the volumes in our dictionary.  Not so useful!
    def get_vols(self,ticket):
        ticket["status"] = "ok"
        try:
            self.reply_to_caller(ticket)
        # even if there is an error - respond to caller so he can process it
        except:
            ticket["status"] = str(sys.exc_info()[0])+str(sys.exc_info()[1])
            self.reply_to_caller(ticket)
            return

        # this could tie things up for awhile - fork and let child
        # send the work list (at time of fork) back to client
        if os.fork() != 0:
            return
        self.get_user_sockets(ticket)
        ticket["status"] = "ok"
        callback.write_tcp_socket(self.data_socket,ticket,
                                  "volume_clerk get_vols, controlsocket")
        msg=""
        key=dict.next()
        while key:
                msg=msg+repr(key)+","
                key=dict.next()
                #send 16K message
                if len(msg) >= 8192:
                   callback.write_tcp_buf(self.data_socket,msg,
                                  "volume_clerk get_vols, datasocket")
                   msg=""
        #send the last message
        msg=msg[:-1]
        callback.write_tcp_buf(self.data_socket,msg,
                                  "volume_clerk get_vols, datasocket")
        self.data_socket.close()
        callback.write_tcp_socket(self.control_socket,ticket,
                                  "volume_clerk get_vols, controlsocket")
        self.control_socket.close()
        os._exit(0)


    # get a port for the data transfer
    # tell the user I'm your volume clerk and here's your ticket
    def get_user_sockets(self, ticket):
        volume_clerk_host, volume_clerk_port, listen_socket =\
                           callback.get_callback()
        listen_socket.listen(4)
        ticket["volume_clerk_callback_host"] = volume_clerk_host
        ticket["volume_clerk_callback_port"] = volume_clerk_port
        self.control_socket = callback.user_callback_socket(ticket)
        data_socket, address = listen_socket.accept()
        self.data_socket = data_socket
        listen_socket.close()

    def start_backup(self,ticket):
        dict.start_backup()
        self.reply_to_caller({"status" : "ok",\
                "start_backup"  : 'yes' })
    def stop_backup(self,ticket):
        dict.stop_backup()
        self.reply_to_caller({"status" : "ok",\
                "stop_backup"  : 'yes' })

class VolumeClerk(VolumeClerkMethods,\
                  generic_server.GenericServer,\
                  SocketServer.UDPServer):
    pass

if __name__ == "__main__":
    import sys
    import getopt
    import string
    # Import SOCKS module if it exists, else standard socket module socket
    # This is a python module that works just like the socket module, but uses
    # the SOCKS protocol to make connections through a firewall machine.
    # See http://www.w3.org/People/Connolly/support/socksForPython.html or
    # goto www.python.org and search for "import SOCKS"
    try:
        import SOCKS; socket = SOCKS
    except ImportError:
        import socket
    Trace.init("Vol Clerk")
    Trace.trace(1,"Volume clerk called with args "+repr(sys.argv))

    # defaults
    #config_host = "localhost"
    (config_host,ca,ci) = socket.gethostbyaddr(socket.gethostname())
    config_port = "7500"
    config_list = 0

    # see what the user has specified. bomb out if wrong options specified
    options = ["config_host=","config_port=","config_list","help"]
    optlist,args=getopt.getopt(sys.argv[1:],'',options)
    for (opt,value) in optlist:
        if opt == "--config_host":
            config_host = value
        elif opt == "--config_port":
            config_port = value
        elif opt == "--config_list":
            config_list = 1
        elif opt == "--help":
            print "python ",sys.argv[0], options
            print "   do not forget the '--' in front of each option"
            sys.exit(0)

    # bomb out if can't translate host
    ip = socket.gethostbyname(config_host)

    # bomb out if port isn't numeric
    config_port = string.atoi(config_port)

    csc = configuration_client.configuration_client(config_host,config_port,\
                                                    config_list)

    keys = csc.get("volume_clerk")
    vc =  VolumeClerk((keys['host'], keys['port']), VolumeClerkMethods)
    vc.set_csc(csc)

    # get a logger
    logc = log_client.LoggerClient(csc, keys["logname"], 'logserver', 0)
    vc.set_logc(logc)
    indlst=['media_type','file_family','library']
    dict = db.dBTable("volume",logc,indlst)
    while 1:
        try:
            Trace.trace(1,'Volume Clerk (re)starting')
            logc.send(log_client.INFO, 1, "Volume Clerk (re)starting")
            vc.serve_forever()
        except:
            traceback.print_exc()
            format = timeofday.tod()+" "+\
                     str(sys.argv)+" "+\
                     str(sys.exc_info()[0])+" "+\
                     str(sys.exc_info()[1])+" "+\
                     "volume clerk serve_forever continuing"
            print format
            logc.send(log_client.ERROR, 1, format)
            Trace.trace(0,format)
            continue
    Trace.trace(1,"Volume Clerk finished (impossible)")
