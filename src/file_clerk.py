# system import
import sys
import time
import pprint
import copy

# enstore imports
import timeofday
import traceback
import callback
import log_client
import configuration_client
import volume_clerk_client
import dispatching_worker
import SocketServer
import generic_server
import udp_client
import db
import Trace


class FileClerkMethods(dispatching_worker.DispatchingWorker) :

    # we need a new bit field id for each new file in the system
    def new_bit_file(self, ticket) :
     Trace.trace(10,'{new_bit_file')
     # input ticket is a file clerk part of the main ticket
     try:
        # create empty record and control what goes into database
        # do not pass ticket, for example to the database!
        record = {}
        record["external_label"]   = ticket["file_clerk"]["external_label"]
        record["bof_space_cookie"] = ticket["file_clerk"]["bof_space_cookie"]
        record["sanity_cookie"]    = ticket["file_clerk"]["sanity_cookie"]
        record["complete_crc"]     = ticket["file_clerk"]["complete_crc"]

        # get a new bit file id
        bfid = self.unique_bit_file_id()
        record["bfid"] = bfid
        # record it to the database
        dict[bfid] = copy.deepcopy(record)

        ticket["file_clerk"]["bfid"] = bfid
        ticket["status"] = "ok"
        self.reply_to_caller(ticket)
        Trace.trace(10,"}new_bit_file bfid="+repr(bfid))
        return

     # even if there is an error - respond to caller so he can process it
     except:
         Trace.trace(0,"}new_bit_file "+str(sys.exc_info()[0])+\
                     str(sys.exc_info()[1]))
         pprint.pprint(ticket)
         self.reply_to_caller(ticket)
         ticket["status"] = str(sys.exc_info()[0])+str(sys.exc_info()[1])
         return


    # To read from the hsm, we need to verify that the bit file id is ok,
    # call the volume server to find the library, and copy to the work
    # ticket the salient information
    def read_from_hsm(self, ticket) :
     Trace.trace(8,"{read_from_hsm")
     try:
        # everything is based on bfid - make sure we have this
        try:
            key="bfid"
            bfid = ticket["file_clerk"][key]
        except KeyError:
            ticket["status"] = "File Clerk: "+key+" key is missing"
            pprint.pprint(ticket)
            self.reply_to_caller(ticket)
            Trace.trace(0,"read_from_hsm "+ticket["status"])
            return

        # look up in our dictionary the request bit field id
        try:
            finfo = copy.deepcopy(dict[bfid])
        except KeyError :
            ticket["status"] = "File Clerk: bfid "+repr(bfid)+" not found"
            pprint.pprint(ticket)
            # unusual error - no id, but it is there
            # what to do - let's try again and see what happens
            try:
                time.sleep(5)
                finfo = copy.deepcopy(dict[bfid])
                print "found on retry!!!!"
                Trace.trace(0,"read_from_hsm found on retry")
                pprint.pprint(finfo)
            except KeyError :
                print "not found on retry either!"
                Trace.trace(0,"read_from_hsm not found on retry either")
            self.reply_to_caller(ticket)
            Trace.trace(0,"read_from_hsm "+ticket["status"])
            return

        # copy all file information we have to user's ticket
        ticket["file_clerk"] = finfo


        # become a client of the volume clerk to get library information
        Trace.trace(10,"read_from_hsm getting volume clerk")
        vcc = volume_clerk_client.VolumeClerkClient(self.csc)
        Trace.trace(10,"read_from_hsm got volume clerk")

        # everything is based on external label - make sure we have this
        try:
            key="external_label"
            external_label = ticket["file_clerk"][key]
        except KeyError:
            ticket["status"] = "File Clerk: "+key+" key is missing"
            pprint.pprint(ticket)
            self.reply_to_caller(ticket)
            Trace.trace(0,"read_from_hsm "+ticket["status"])
            return

        # ask the volume clerk server which library has "external_label" in it
        Trace.trace(9,"read_from_hsm inquiring about volume="+\
                    repr(external_label))
        vticket = vcc.inquire_vol(external_label)
        if vticket["status"] != "ok" :
            pprint.pprint(ticket)
            self.reply_to_caller(vticket)
            Trace.trace(0,"read_from_hsm "+ticket["status"])
            return
        library = vticket["library"]
        Trace.trace(9,"read from hsm volume="+repr(external_label)+" in "+
                    "library="+repr(library))

        # get the library manager
        Trace.trace(10,"write_to_hsm calling config server to find "+\
                    library+".library_manager")
        vmticket = csc.get(library+".library_manager")
        Trace.trace(10,"write_to_hsm."+ library+".library_manager at host="+\
                    repr(vmticket["host"])+" port="+repr(vmticket["port"]))
        if vmticket["status"] != "ok" :
            pprint.pprint(ticket)
            self.reply_to_caller(vmticket)
            Trace.trace(0,"read_from_hsm "+ticket["status"])
            return

        # send to library manager and tell user
        u = udp_client.UDPClient()
        Trace.trace(7,"read_from_hsm q'ing:"+repr(ticket))
        ticket = u.send(ticket, (vmticket['host'], vmticket['port']))
        self.reply_to_caller(ticket)
        Trace.trace(7,"}read_from_hsm")
        return

     # even if there is an error - respond to caller so he can process it
     except:
         ticket["status"] = str(sys.exc_info()[0])+str(sys.exc_info()[1])
         pprint.pprint(ticket)
         self.reply_to_caller(ticket)
         Trace.trace(0,"read_from_hsm "+ticket["status"])
         return

    def get_user_sockets(self, ticket) :
        Trace.trace(16,"{get_user_sockets")
        file_clerk_host, file_clerk_port, listen_socket =\
                           callback.get_callback()
        listen_socket.listen(4)
        ticket["file_clerk_callback_host"] = file_clerk_host
        ticket["file_clerk_callback_port"] = file_clerk_port
        self.control_socket = callback.user_callback_socket(ticket)
        data_socket, address = listen_socket.accept()
        self.data_socket = data_socket
        listen_socket.close()
        Trace.trace(16,"}get_user_sockets host="+repr(file_clerk_host)+\
                    " file_clerk_port="+repr(file_clerk_port))

    # return all the bfids in our dictionary.  Not so useful!
    def get_bfids(self,ticket) :
     Trace.trace(10,"{get_bfids  R U CRAZY?")
     ticket["status"] = "ok"
     try:
        self.reply_to_caller(ticket)
     # even if there is an error - respond to caller so he can process it
     except:
        ticket["status"] = str(sys.exc_info()[0])+str(sys.exc_info()[1])
        self.reply_to_caller(ticket)
        Trace.trace(0,"get_bfids "+ticket["status"])
        return
     self.get_user_sockets(ticket)
     ticket["status"] = "ok"
     callback.write_tcp_socket(self.data_socket,ticket,
                                  "file_clerk get bfids, controlsocket")
     msg=""
     key=dict.next()
     while key :
        msg=msg+repr(key)+","
        key=dict.next()
        if len(msg) >= 16384:
           callback.write_tcp_buf(self.data_socket,msg,
                                  "file_clerk get bfids, datasocket")
           msg=""

     msg=msg[:-1]
     callback.write_tcp_buf(self.data_socket,msg,
                                  "file_clerk get bfids, datasocket")
     self.data_socket.close()
     callback.write_tcp_socket(self.control_socket,ticket,
                                  "file_clerk get bfids, controlsocket")
     self.control_socket.close()
     Trace.trace(10,"}get_bfids")
     return


    # return all info about a certain bfid - this does everything that the
    # read_from_hsm method does, except send the ticket to the library manager
    def bfid_info(self, ticket) :
     Trace.trace(10,'{bfid_info')
     try:
        # everything is based on bfid - make sure we have this
        try:
            key="bfid"
            bfid = ticket[key]
        except KeyError:
            ticket["status"] = "File Clerk: "+key+" key is missing"
            pprint.pprint(ticket)
            self.reply_to_caller(ticket)
            Trace.trace(0,"bfid_info "+ticket["status"])
            return

        # look up in our dictionary the request bit field id
        try:
            finfo = copy.deepcopy(dict[bfid])
        except KeyError :
            ticket["status"] = "File Clerk: bfid "+repr(bfid)+" not found"
            pprint.pprint(ticket)
            self.reply_to_caller(ticket)
            Trace.trace(0,"bfid_info "+ticket["status"])
            return

        # copy all file information we have to user's ticket
        ticket["file_clerk"] = finfo

        # become a client of the volume clerk to get library information
        Trace.trace(11,"bfid_info getting volume clerk")
        vcc = volume_clerk_client.VolumeClerkClient(self.csc)
        Trace.trace(11,"bfid_info got volume clerk")

        # everything is based on external label - make sure we have this
        try:
            key="external_label"
            external_label = finfo[key]
        except KeyError:
            ticket["status"] = "File Clerk: "+key+" key is missing"
            pprint.pprint(ticket)
            self.reply_to_caller(ticket)
            Trace.trace(0,"bfid_info "+ticket["status"])
            return

        # ask the volume clerk server which library has "external_label" in it
        Trace.trace(11,"bfid_info inquiring about volume="+\
                    repr(external_label))
        vticket = vcc.inquire_vol(external_label)
        if vticket["status"] != "ok" :
            pprint.pprint(ticket)
            self.reply_to_caller(vticket)
            Trace.trace(0,"bfid_info "+ticket["status"])
            return
        library = vticket["library"]
        Trace.trace(11,"bfid_info volume="+repr(external_label)+" in "+
                    "library="+repr(library))

        # copy all volume information we have to user's ticket
        ticket["volume_clerk"] = vticket

        ticket["status"] = "ok"
        self.reply_to_caller(ticket)
        Trace.trace(10,"}bfid_info bfid="+repr(bfid))
        return

     # even if there is an error - respond to caller so he can process it
     except:
         ticket["status"] = str(sys.exc_info()[0])+str(sys.exc_info()[1])
         pprint.pprint(ticket)
         self.reply_to_caller(ticket)
         Trace.trace(0,"bfid_info "+ticket["status"])
         return

    # A bit file id is defined to be a 64-bit number whose most significant
    # part is based on the time, and the least significant part is a count
    # to make it unique
    def unique_bit_file_id(self) :
     Trace.trace(10,'}unique_bit_file_id')
     try:
        bfid = time.time()
        bfid = long(bfid)*100000
        while dict.has_key(repr(bfid)) :
            bfid = bfid + 1
        Trace.trace(10,'}unique_bit_file_id bfid='+repr(bfid))
        return repr(bfid)
     # even if there is an error - respond to caller so he can process it
     except:
         msg = "can not generate a bit file id!!"+\
               str(sys.exc_info()[0])+str(sys.exc_info()[1])
         print msg
         Trace.trace(0,"unique_bit_file_id "+msg)
         sys.exit(1)

    def start_backup(self,ticket):
        Trace.trace(10,'{start_backup')
        dict.start_backup()
        self.reply_to_caller({"status" : "ok",\
                "start_backup"  : 'yes' })
        Trace.trace(10,'}start_backup')

    def stop_backup(self,ticket):
        Trace.trace(10,'{stop_backup')
        dict.stop_backup()
        self.reply_to_caller({"status" : "ok",\
                "stop_backup"  : 'yes' })
        Trace.trace(10,'}stop_backup')

class FileClerk(FileClerkMethods,
                generic_server.GenericServer,
                SocketServer.UDPServer) :
    pass

if __name__ == "__main__" :
    Trace.init("file clerk")
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

    # defaults
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
    csc = configuration_client.configuration_client(config_host,config_port)
    csc.connect()

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
    indlst=['external_label']
    dict = db.dBTable("file",logc,indlst)
    while 1:
        try:
            Trace.trace(1,'File Clerk (re)starting')
            logc.send(log_client.INFO, 1, "File Clerk (re)starting")
            fc.serve_forever()
        except:
            traceback.print_exc()
            format = timeofday.tod()+" "+\
                     str(sys.argv)+" "+\
                     str(sys.exc_info()[0])+" "+\
                     str(sys.exc_info()[1])+" "+\
                     "file clerk serve_forever continuing"
            print format
            logc.send(log_client.ERROR, 1, format)
            Trace.trace(0,format)
            continue
