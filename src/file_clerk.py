##############################################################################
# src/$RCSfile$   $Revision$
#
# system import
import sys
import time
import copy
import os
import regsub
import stat

# enstore imports
import timeofday
import traceback
import callback
import log_client
import configuration_client
import volume_clerk_client
import dispatching_worker
import generic_server
import generic_cs
import interface
import udp_client
import db
import Trace
import e_errors

class FileClerkMethods(dispatching_worker.DispatchingWorker):

    # we need a new bit field id for each new file in the system
    def new_bit_file(self, ticket):
     Trace.trace(10,'{new_bit_file '+repr(ticket))
     # input ticket is a file clerk part of the main ticket
     try:
        # create empty record and control what goes into database
        # do not pass ticket, for example to the database!
        record = {}
        record["external_label"]   = ticket["fc"]["external_label"]
        ###########################################################################TEMPORARY##########
        try:
            record["location_cookie"] = ticket["fc"]["location_cookie"]
            record["size"] = ticket["fc"]["size"]
        except:
            self.enprint("Old fashioned ticket: upgrade.")
            record["location_cookie"], record["size"] = eval(ticket["fc"]["bof_space_cookie"])

        #######################################################################END#TEMPORARY##########
        record["sanity_cookie"]    = ticket["fc"]["sanity_cookie"]
        record["complete_crc"]     = ticket["fc"]["complete_crc"]

        # get a new bit file id
        bfid = self.unique_bit_file_id()
        record["bfid"] = bfid
        # record it to the database
        dict[bfid] = copy.deepcopy(record)

        ticket["fc"]["bfid"] = bfid
        ###########################################################################TEMPORARY##########
        if not ticket["fc"].has_key("location_cookie"):
            self.enprint("Old fashioned ticket: upgrade.")
            ticket["fc"]["location_cookie"], ticket["fc"]["size"] = eval(record["bof_space_cookie"])
        #######################################################################END#TEMPORARY##########
        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        Trace.trace(10,'}new_bit_file bfid='+repr(bfid))
        return

     # even if there is an error - respond to caller so he can process it
     except:
         Trace.trace(0,"}new_bit_file "+str(sys.exc_info()[0])+\
                     str(sys.exc_info()[1]))
	 traceback.print_exc()
         ticket["status"] = (str(sys.exc_info()[0]), str(sys.exc_info()[1]))
         self.enprint(ticket, generic_cs.PRETTY_PRINT)
         self.reply_to_caller(ticket)
         return

    # update the database entry for this file - add the pnfs file id
    def set_pnfsid(self, ticket):
     Trace.trace(12,'{set_pnfsid '+repr(ticket))
     try:

        # everything is based on bfid - make sure we have this
        try:
            key="bfid"
            bfid = ticket["fc"][key]
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
                                "File Clerk: "+key+" key is missing")
            self.enprint(ticket, generic_cs.PRETTY_PRINT)
            self.reply_to_caller(ticket)
            Trace.trace(0,"bfid_info "+repr(ticket["status"]))
            return

        # also need new pnfsid - make sure we have this
        try:
            key2="pnfsid";       pnfsid       = ticket["fc"][key2]
            # temporary try block - sam doesn't want to update encp too often --> put back into main try in awhile
            try: 
                key2="pnfsvid";      pnfsvid      = ticket["fc"][key2]
                key2="pnfs_name0";   pnfs_name0   = ticket["fc"][key2]
                key2="pnfs_mapname"; pnfs_mapname = ticket["fc"][key2]
            except:
                pass
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, "File Clerk: "+key2+" key is missing")
            self.enprint(ticket, generic_cs.PRETTY_PRINT)
            self.reply_to_caller(ticket)
            Trace.trace(0,"bfid_info "+repr(ticket["status"]))
            return

        # look up in our dictionary the request bit field id
        try:
            record = copy.deepcopy(dict[bfid])
            ###########################################################################TEMPORARY##########
            if not record.has_key("location_cookie"):
                self.enprint("Old fashioned ticket: upgrade.")
                record["location_cookie"], record["size"] = eval(record["bof_space_cookie"])
            #######################################################################END#TEMPORARY##########
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
                                "File Clerk: bfid "+repr(bfid)+" not found")
            self.enprint(ticket, generic_cs.PRETTY_PRINT)
            self.reply_to_caller(ticket)
            Trace.trace(0,"bfid_info "+repr(ticket["status"]))
            return

        # add the pnfsid
        record["pnfsid"] = pnfsid
        # temporary try block - sam doesn't want to update encp too often --> put back into main try in awhile
        try: 
            record["pnfsvid"] = pnfsvid
            record["pnfs_name0"] = pnfs_name0
            record["pnfs_mapname"] = pnfs_mapname
            record["deleted"] = "no"
        except:
            pass

        # record our changes
        dict[bfid] = copy.deepcopy(record)
        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        Trace.trace(12,'}set_pnfsid '+repr(ticket))
        return

     # even if there is an error - respond to caller so he can process it
     except:
         ticket["status"] = (str(sys.exc_info()[0]), str(sys.exc_info()[1]))
         self.enprint(ticket, generic_cs.PRETTY_PRINT)
         self.reply_to_caller(ticket)
         Trace.trace(0,"}set_pnfsid "+repr(ticket["status"]))
         return

    def get_user_sockets(self, ticket):
        Trace.trace(16,"{get_user_sockets "+repr(ticket))
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
    def get_bfids(self,ticket):
     Trace.trace(10,"{get_bfids  R U CRAZY? "+repr(ticket))
     ticket["status"] = (e_errors.OK, None)
     try:
        self.reply_to_caller(ticket)
     # even if there is an error - respond to caller so he can process it
     except:
        ticket["status"] = (str(sys.exc_info()[0]), str(sys.exc_info()[1]))
        self.reply_to_caller(ticket)
        Trace.trace(0,"get_bfids "+repr(ticket["status"]))
        return
     self.get_user_sockets(ticket)
     ticket["status"] = (e_errors.OK, None)
     callback.write_tcp_socket(self.data_socket,ticket,
                                  "file_clerk get bfids, controlsocket")
     msg=""
     key=dict.next()
     while key:
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
    def bfid_info(self, ticket):
     Trace.trace(10,'{bfid_info '+repr(ticket))
     try:
        # everything is based on bfid - make sure we have this
        try:
            key="bfid"
            bfid = ticket[key]
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
                                "File Clerk: "+key+" key is missing")
            self.enprint(ticket, generic_cs.PRETTY_PRINT)
            self.reply_to_caller(ticket)
            Trace.trace(0,"bfid_info "+repr(ticket["status"]))
            return

        # look up in our dictionary the request bit field id
        try:
            finfo = copy.deepcopy(dict[bfid])
            ###########################################################################TEMPORARY##########
            if not finfo.has_key("location_cookie"):
                finfo["location_cookie"], finfo["size"] = eval(record["bof_space_cookie"])
                self.enprint("Old fashioned ticket: upgrade.")
            import types
            if type(finfo["location_cookie"]) == types.IntType:
                self.enprint("fixing location_cookie from int to string type:"+repr(finfo["location_cookie"]))
                finfo["location_cookie"] = "%12.12i"%finfo["location_cookie"]
                dict[bfid] = copy.deepcopy(finfo) # copy back to database
            if type(finfo["sanity_cookie"]) == types.StringType:
                self.enprint("fixing sanity_cookie from string to tuple type:"+repr(finfo["sanity_cookie"]))
                exec("x="+finfo["sanity_cookie"])
                finfo["sanity_cookie"] = x
                dict[bfid] = copy.deepcopy(finfo) # copy back to database
            if 0==1:
                filelf = os.popen("pcmd path "+finfo["pnfsid"],'r').readlines()
                file = regsub.sub("\012","",filelf[0])
                finfo["pnfsfilename"] = file
                if 1==1 and file!="NO_SUCH_FILE":
                    fstat = os.stat(file)
                    psize = fstat[stat.ST_SIZE]
                    if psize != finfo['size']:
                        self.enprint("Size mismatch between "+file+"="+repr(psize)+" and fc="+repr(finfo['size']))
                        finfo['size'] = psize
                        exec("bof="+finfo["bof_space_cookie"])
                        #finfo['bof_space_cookie'] = repr((bof[0],psize))  don't fix this for now, use as safeguard to get old size back
                        dict[bfid] = copy.deepcopy(finfo) # copy back to database
            #######################################################################END#TEMPORARY##########
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
                                "File Clerk: bfid "+repr(bfid)+" not found")
            self.enprint(ticket, generic_cs.PRETTY_PRINT)
            self.reply_to_caller(ticket)
            Trace.trace(0,"bfid_info "+repr(ticket["status"]))
            return

        # copy all file information we have to user's ticket
        ticket["fc"] = finfo

        # become a client of the volume clerk to get library information
        Trace.trace(11,"bfid_info getting volume clerk")
        vcc = volume_clerk_client.VolumeClerkClient(self.csc)
        Trace.trace(11,"bfid_info got volume clerk")

        # everything is based on external label - make sure we have this
        try:
            key="external_label"
            external_label = finfo[key]
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
                                "File Clerk: "+key+" key is missing")
            self.enprint(ticket, generic_cs.PRETTY_PRINT)
            self.reply_to_caller(ticket)
            Trace.trace(0,"bfid_info "+repr(ticket["status"]))
            return

        # ask the volume clerk server which library has "external_label" in it
        Trace.trace(11,"bfid_info inquiring about volume="+\
                    repr(external_label))
        vticket = vcc.inquire_vol(external_label)
        if vticket["status"][0] != e_errors.OK:
            self.enprint(ticket, generic_cs.PRETTY_PRINT)
            self.reply_to_caller(vticket)
            Trace.trace(0,"bfid_info "+repr(ticket["status"]))
            return
        library = vticket["library"]
        Trace.trace(11,"bfid_info volume="+repr(external_label)+" in "+
                    "library="+repr(library))

        # copy all volume information we have to user's ticket
        ticket["vc"] = vticket

        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        Trace.trace(10,"}bfid_info bfid="+repr(bfid))
        return

     # even if there is an error - respond to caller so he can process it
     except:
         ticket["status"] = (str(sys.exc_info()[0]), str(sys.exc_info()[1]))
         self.enprint(ticket, generic_cs.PRETTY_PRINT)
         self.reply_to_caller(ticket)
         Trace.trace(0,"bfid_info "+repr(ticket["status"]))
         return

    # A bit file id is defined to be a 64-bit number whose most significant
    # part is based on the time, and the least significant part is a count
    # to make it unique
    def unique_bit_file_id(self):
     Trace.trace(10,'}unique_bit_file_id')
     try:
        bfid = time.time()
        bfid = long(bfid)*100000
        while dict.has_key(repr(bfid)):
            bfid = bfid + 1
        Trace.trace(10,'}unique_bit_file_id bfid='+repr(bfid))
        return repr(bfid)
     # even if there is an error - respond to caller so he can process it
     except:
         msg = "can not generate a bit file id!!"+\
               str(sys.exc_info()[0])+str(sys.exc_info()[1])
         self.enprint(msg)
         Trace.trace(0,"unique_bit_file_id "+msg)
         sys.exit(1)

    def start_backup(self,ticket):
        Trace.trace(10,'{start_backup '+repr(ticket))
        dict.start_backup()
        self.reply_to_caller({"status" : (e_errors.OK, None),\
                "start_backup"  : 'yes' })
        Trace.trace(10,'}start_backup')

    def stop_backup(self,ticket):
        Trace.trace(10,'{stop_backup '+repr(ticket))
        dict.stop_backup()
        self.reply_to_caller({"status" : (e_errors.OK, None),\
                "stop_backup"  : 'yes' })
        Trace.trace(10,'}stop_backup')

class FileClerk(FileClerkMethods, generic_server.GenericServer):

    def __init__(self, csc=0, verbose=0, host=interface.default_host(), \
                 port=interface.default_port()):
	Trace.trace(10, '{__init__')
	self.print_id = "FCS"
	self.verbose = verbose
	# get the config server
	configuration_client.set_csc(self, csc, host, port, verbose)
	#   pretend that we are the test system
	#   remember, in a system, there is only one bfs
	#   get our port and host from the name server
	#   exit if the host is not this machine
	keys = self.csc.get("file_clerk")
        try:
            self.print_id = keys['logname']
        except:
            pass
        Trace.init(keys["logname"])
	dispatching_worker.DispatchingWorker.__init__(self, (keys['hostip'], \
	                                              keys['port']))
        # get a logger
        self.logc = log_client.LoggerClient(self.csc, keys["logname"], \
                                            'logserver', 0)
	Trace.trace(10, '}__init__')


class FileClerkInterface(generic_server.GenericServerInterface):
    pass

if __name__ == "__main__":
    import sys
    Trace.init("file clerk")
    Trace.trace(1,"file clerk called with args "+repr(sys.argv))

    # get the interface
    intf = FileClerkInterface()

    # get a file clerk
    fc = FileClerk(0, intf.verbose, intf.config_host, intf.config_port)

    indlst=['external_label']
    dict = db.DbTable("file",fc.logc,indlst)
    while 1:
        try:
            Trace.trace(1,'File Clerk (re)starting')
            fc.logc.send(log_client.INFO, 1, "File Clerk (re)starting")
            fc.serve_forever()
        except:
	    fc.serve_forever_error("file clerk", fc.logc)
            continue
    Trace.trace(1,"File Clerk finished (impossible)")
