##############################################################################
# src/$RCSfile$   $Revision$
#
# system import
import sys
import time
import copy
import string

# enstore imports
import traceback
import callback
import volume_clerk_client
import dispatching_worker
import generic_server
import db
import Trace
import e_errors

dict="" # quiet lint

MY_NAME = "file_clerk"

class FileClerkMethods(dispatching_worker.DispatchingWorker):

    # we need a new bit field id for each new file in the system
    def new_bit_file(self, ticket):
     # input ticket is a file clerk part of the main ticket
     try:
        # create empty record and control what goes into database
        # do not pass ticket, for example to the database!
        record = {}
        record["external_label"]   = ticket["fc"]["external_label"]
        record["location_cookie"]  = ticket["fc"]["location_cookie"]
        record["size"]             = ticket["fc"]["size"]
        record["sanity_cookie"]    = ticket["fc"]["sanity_cookie"]
        record["complete_crc"]     = ticket["fc"]["complete_crc"]

        # get a new bit file id
        bfid = self.unique_bit_file_id()
        record["bfid"] = bfid
        # record it to the database
        dict[bfid] = copy.deepcopy(record)

        ticket["fc"]["bfid"] = bfid
        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        Trace.trace(10,'new_bit_file bfid='+repr(bfid))
        return

     # even if there is an error - respond to caller so he can process it
     except:
         Trace.trace(10,"new_bit_file "+str(sys.exc_info()[0])+\
                     str(sys.exc_info()[1]))
	 traceback.print_exc()
         ticket["status"] = (str(sys.exc_info()[0]), str(sys.exc_info()[1]))
         Trace.log(e_errors.INFO, repr(ticket))
         self.reply_to_caller(ticket)
         return

    # update the database entry for this file - add the pnfs file id
    def set_pnfsid(self, ticket):
     try:

        # everything is based on bfid - make sure we have this
        try:
            key="bfid"
            bfid = ticket["fc"][key]
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
                                "File Clerk: "+key+" key is missing")
            Trace.log(e_errors.INFO, repr(ticket))
            self.reply_to_caller(ticket)
            Trace.trace(10,"set_pnfsid "+repr(ticket["status"]))
            return

        # also need new pnfsid - make sure we have this
        try:
            key2="pnfsid";
            pnfsid = ticket["fc"][key2]
            # temporary try block - sam doesn't want to update encp too often --> put back into main try in awhile
            try:
                key2="pnfsvid";      pnfsvid      = ticket["fc"][key2]
                key2="pnfs_name0";   pnfs_name0   = ticket["fc"][key2]
                key2="pnfs_mapname"; pnfs_mapname = ticket["fc"][key2]
            except:
                pass
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, "File Clerk: "+key2+" key is missing")
            Trace.log(e_errors.INFO, repr(ticket))
            self.reply_to_caller(ticket)
            Trace.trace(10,"set_pnfsid "+repr(ticket["status"]))
            return

        # look up in our dictionary the request bit field id
        try:
            record = copy.deepcopy(dict[bfid])
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
                                "File Clerk: bfid "+repr(bfid)+" not found")
            Trace.log(e_errors.INFO, repr(ticket))
            self.reply_to_caller(ticket)
            Trace.trace(10,"bfid_info "+repr(ticket["status"]))
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
        Trace.trace(12,'set_pnfsid '+repr(ticket))
        return

     # even if there is an error - respond to caller so he can process it
     except:
         ticket["status"] = (str(sys.exc_info()[0]), str(sys.exc_info()[1]))
         Trace.log(e_errors.INFO, repr(ticket))
         self.reply_to_caller(ticket)
         Trace.trace(10,"set_pnfsid "+repr(ticket["status"]))
         return

    # change the delete state element in the dictionary
    def set_deleted(self, ticket):
     try:

        # everything is based on bfid - make sure we have this
        try:
            key="bfid"
            bfid = ticket[key]
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
                                "File Clerk: "+key+" key is missing")
            Trace.log(e_errors.INFO, repr(ticket))
            self.reply_to_caller(ticket)
            Trace.trace(10,"set_deleted "+repr(ticket["status"]))
            return

        # also need new value of the delete element- make sure we have this
        try:
            key2="deleted"
            deleted = ticket[key2]
            if string.find(string.lower(deleted),'y') !=-1 or string.find(string.lower(deleted),'Y') !=-1:
                deleted = "yes"
                decr_count = 1
            else:
                self.deleted = "no"
                decr_count = -1
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, "File Clerk: "+key2+" key is missing")
            Trace.log(e_errors.INFO, repr(ticket))
            self.reply_to_caller(ticket)
            Trace.trace(10,"set_deleted "+repr(ticket["status"]))
            return

        # look up in our dictionary the request bit field id
        try:
            record = copy.deepcopy(dict[bfid])
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
                                "File Clerk: bfid "+repr(bfid)+" not found")
            Trace.log(e_errors.INFO, repr(ticket))
            self.reply_to_caller(ticket)
            Trace.trace(10,"set_deleted "+repr(ticket["status"]))
            return
        
        if record["deleted"] == deleted:
            ticket["status"] = (e_errors.USER_ERROR,
                                "%s = %s deleted flag already set to %s - no change." % (bfid,record["pnfs_name0"],record["deleted"]))
            Trace.log(e_errors.USER_ERROR, 
            "%s = %s deleted flag already set to %s - no change." % (bfid,record["pnfs_name0"],record["deleted"]))
            self.reply_to_caller(ticket)
            Trace.trace(12,'set_deleted '+repr(ticket))
            return
            
        # mod the delete state
        record["deleted"] = deleted

	if deleted == "no":
	    # restore pnfs entry
	    import pnfs
	    map = pnfs.Pnfs(record["pnfs_mapname"])
	    map.restore_from_volmap()
	    del map
        # become a client of the volume clerk and decrement the non-del files on the volume
        vcc = volume_clerk_client.VolumeClerkClient(self.csc)
        vticket = vcc.decr_file_count(record['external_label'],decr_count)

        # record our changes
        dict[bfid] = copy.deepcopy(record)

        Trace.log(e_errors.INFO,
                  "%s = %s flagged as deleted:%s  volume=%s(%d)  mapfile=%s" %
                  (bfid,record["pnfs_name0"],record["deleted"],record["external_label"],vticket["non_del_files"],record["pnfs_mapname"]))

        # and return to the caller
        ticket["status"] = (e_errors.OK, None)
        ticket["fc"] = record
        ticket["vc"] = vticket
        self.reply_to_caller(ticket)
        Trace.trace(12,'set_deleted '+repr(ticket))
        return

     # even if there is an error - respond to caller so he can process it
     except:
         ticket["status"] = (str(sys.exc_info()[0]), str(sys.exc_info()[1]))
         Trace.log(e_errors.INFO, repr(ticket))
         self.reply_to_caller(ticket)
         Trace.trace(10,"set_deleted "+repr(ticket["status"]))
         return

    def get_user_sockets(self, ticket):
        file_clerk_host, file_clerk_port, listen_socket =\
                           callback.get_callback()
        listen_socket.listen(4)
        ticket["file_clerk_callback_host"] = file_clerk_host
        ticket["file_clerk_callback_port"] = file_clerk_port
        self.control_socket = callback.user_callback_socket(ticket)
        data_socket, address = listen_socket.accept()
        if 0: print address # quiet lint
        self.data_socket = data_socket
        listen_socket.close()
        Trace.trace(16,"get_user_sockets host="+repr(file_clerk_host)+\
                    " file_clerk_port="+repr(file_clerk_port))

    # return all the bfids in our dictionary.  Not so useful!
    def get_bfids(self,ticket):
     ticket["status"] = (e_errors.OK, None)
     try:
        self.reply_to_caller(ticket)
     # even if there is an error - respond to caller so he can process it
     except:
        ticket["status"] = (str(sys.exc_info()[0]), str(sys.exc_info()[1]))
        self.reply_to_caller(ticket)
        Trace.trace(10,"get_bfids "+repr(ticket["status"]))
        return
     self.get_user_sockets(ticket)
     ticket["status"] = (e_errors.OK, None)
     callback.write_tcp_socket(self.data_socket,ticket,
                                  "file_clerk get bfids, controlsocket")
     msg=""
     dict.cursor("open")
     key,value=dict.cursor("first")
     while key:
        msg=msg+repr(key)+","
        if len(msg) >= 16384:
           callback.write_tcp_buf(self.data_socket,msg,
                                  "file_clerk get bfids, datasocket")
           msg=""
	print "KEY", key
	print "VALUE", value
        key,value=dict.cursor("next")
     dict.cursor("close")
     msg=msg[:-1]
     callback.write_tcp_buf(self.data_socket,msg,
                                  "file_clerk get bfids, datasocket")
     self.data_socket.close()
     callback.write_tcp_socket(self.control_socket,ticket,
                                  "file_clerk get bfids, controlsocket")
     self.control_socket.close()
     return


    # return all info about a certain bfid - this does everything that the
    # read_from_hsm method does, except send the ticket to the library manager
    def bfid_info(self, ticket):
     try:
        # everything is based on bfid - make sure we have this
        try:
            key="bfid"
            bfid = ticket[key]
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
                                "File Clerk: "+key+" key is missing")
            Trace.log(e_errors.INFO, repr(ticket))
            self.reply_to_caller(ticket)
            Trace.trace(10,"bfid_info "+repr(ticket["status"]))
            return

        # look up in our dictionary the request bit field id
        try:
            finfo = copy.deepcopy(dict[bfid])
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
                                "File Clerk: bfid "+repr(bfid)+" not found")
            Trace.log(e_errors.INFO, repr(ticket))
            self.reply_to_caller(ticket)
            Trace.trace(10,"bfid_info "+repr(ticket["status"]))
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
            Trace.log(e_errors.INFO, repr(ticket))
            self.reply_to_caller(ticket)
            Trace.trace(10,"bfid_info "+repr(ticket["status"]))
            return

        # ask the volume clerk server which library has "external_label" in it
        Trace.trace(11,"bfid_info inquiring about volume="+\
                    repr(external_label))
        vticket = vcc.inquire_vol(external_label)
        if vticket["status"][0] != e_errors.OK:
            Trace.log(e_errors.INFO, repr(ticket))
            self.reply_to_caller(vticket)
            Trace.trace(10,"bfid_info "+repr(ticket["status"]))
            return
        library = vticket["library"]
        Trace.trace(11,"bfid_info volume="+repr(external_label)+" in "+
                    "library="+repr(library))

        # copy all volume information we have to user's ticket
        ticket["vc"] = vticket

        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        Trace.trace(10,"bfid_info bfid="+repr(bfid))
        return

     # even if there is an error - respond to caller so he can process it
     except:
         ticket["status"] = (str(sys.exc_info()[0]), str(sys.exc_info()[1]))
         Trace.log(e_errors.INFO, repr(ticket))
         self.reply_to_caller(ticket)
         Trace.trace(10,"bfid_info "+repr(ticket["status"]))
         return

    # return volume map name for given bfid
    def get_volmap_name(self, ticket):
     try:
        # everything is based on bfid - make sure we have this
        try:
            key="bfid"
            bfid = ticket[key]
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
                                "File Clerk: "+key+" key is missing")
            Trace.log(e_errors.INFO, repr(ticket))
            self.reply_to_caller(ticket)
            Trace.trace(10,"bfid_info "+repr(ticket["status"]))
            return

        # look up in our dictionary the request bit field id
        try:
            finfo = copy.deepcopy(dict[bfid])
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
                                "File Clerk: bfid "+repr(bfid)+" not found")
            Trace.log(e_errors.INFO, repr(ticket))
            self.reply_to_caller(ticket)
            Trace.trace(10,"bfid_info "+repr(ticket["status"]))
            return

        # copy all file information we have to user's ticket
        ticket["pnfs_mapname"] = finfo["pnfs_mapname"]
	ticket["status"] = (e_errors.OK, None)

	self.reply_to_caller(ticket)
	Trace.trace(10,"get_volmap_name "+repr(ticket["status"]))
	return

     # even if there is an error - respond to caller so he can process it
     except:
         ticket["status"] = (str(sys.exc_info()[0]), str(sys.exc_info()[1]))
         Trace.log(e_errors.INFO, repr(ticket))
         self.reply_to_caller(ticket)
         Trace.trace(10,"bfid_info "+repr(ticket["status"]))
         return

    # change the delete state element in the dictionary
    def del_bfid(self, ticket):
     try:

        # everything is based on bfid - make sure we have this
        try:
            key="bfid"
            bfid = ticket[key]
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
                                "File Clerk: "+key+" key is missing")
            Trace.log(e_errors.INFO, repr(ticket))
            self.reply_to_caller(ticket)
            Trace.trace(10,"del_bfid "+repr(ticket["status"]))
            return

        # now just delete the bfid
        del dict[bfid]

        # and return to the caller
        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        Trace.trace(12,'del_bfid '+repr(ticket))
        return

     # even if there is an error - respond to caller so he can process it
     except:
         ticket["status"] = (str(sys.exc_info()[0]), str(sys.exc_info()[1]))
         Trace.log(e_errors.INFO, repr(ticket))
         self.reply_to_caller(ticket)
         Trace.trace(10,"bfid_info "+repr(ticket["status"]))
         return

    # rename volume and volume map
    def rename_volume(self, ticket):
     try:
        # everything is based on bfid - make sure we have this
        try:
            key="bfid"
            bfid = ticket[key]
	    key = "external_label"
	    label = ticket[key]
	    key = "set_deleted"
	    set_deleted = ticket[key]
	    key = "restore"
	    restore_volmap = ticket[key]
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
                                "File Clerk: "+key+" key is missing")
            Trace.log(e_errors.INFO, repr(ticket))
            self.reply_to_caller(ticket)
            Trace.trace(10,"rename_volume "+repr(ticket["status"]))
            return

	record = copy.deepcopy(dict[bfid])
	# replace old volume name with new one
	record["pnfs_mapname"] = string.replace(record["pnfs_mapname"], 
						record["external_label"], 
						label)
	ticket["pnfs_mapname"] = record["pnfs_mapname"]
	record["external_label"] = label
	record["deleted"] = set_deleted
	if record["deleted"] == "no" and restore_volmap == "yes":
	    # restore pnfs entry
	    import pnfs
	    map = pnfs.Pnfs(record["pnfs_mapname"])
	    map.restore_from_volmap()
	    del map
	dict[bfid] = copy.deepcopy(record)
 
        # and return to the caller
        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        Trace.trace(12,'rename_volume '+repr(ticket))
        return

     # even if there is an error - respond to caller so he can process it
     except:
         ticket["status"] = (str(sys.exc_info()[0]), str(sys.exc_info()[1]))
         Trace.log(e_errors.INFO, repr(ticket))
         self.reply_to_caller(ticket)
         Trace.trace(10,"bfid_info "+repr(ticket["status"]))
         return

    # A bit file id is defined to be a 64-bit number whose most significant
    # part is based on the time, and the least significant part is a count
    # to make it unique
    def unique_bit_file_id(self):
     try:
        bfid = time.time()
        bfid = long(bfid)*100000
        while dict.has_key(repr(bfid)):
            bfid = bfid + 1
        return repr(bfid)
     # even if there is an error - respond to caller so he can process it
     except:
         msg = "can not generate a bit file id!!"+\
               str(sys.exc_info()[0])+str(sys.exc_info()[1])
         Trace.log(e_errors.INFO, repr(msg))
         Trace.trace(10,"unique_bit_file_id "+msg)
         sys.exit(1)

    def tape_list(self,ticket):
     # everything is based on external_label - make sure we have this
     try:
         key="external_label"
         external_label = ticket[key]
         ticket["status"] = (e_errors.OK, None)
         self.reply_to_caller(ticket)
     except KeyError:
         ticket["status"] = (e_errors.KEYERROR,"File Clerk: "+key+" key is missing")
         Trace.log(e_errors.INFO, repr(ticket))
         self.reply_to_caller(ticket)
         Trace.trace(10,"tape_list "+repr(ticket["status"]))
         return

     if self.fork() != 0:
         return
     # get a user callback
     self.get_user_sockets(ticket)
     callback.write_tcp_socket(self.data_socket,ticket,"file_clerk get bfids, controlsocket")
     msg="     label            bfid       size        location_cookie delflag original_name\n"

     # now get a cursor so we can loop on the database quickly:
     dict.cursor("open")
     key,value=dict.cursor("first")
     while key:
         if value['external_label'] == external_label:
             if value.has_key('deleted'):
                 if value['deleted']=="yes":
                     deleted = "deleted"
                 else:
                     deleted = " active"
             else:
                 deleted = "unknown"
             if not value.has_key('pnfs_name0'):
                 value['pnfs_name0'] = "unknown"
             msg=msg+ "%10s %s %10i %22s %7s %s\n" % (external_label, value['bfid'],
                                                      value['size'],value['location_cookie'],
                                                      deleted,value['pnfs_name0'])
             if len(msg) >= 16384:
                 #print "sending len(msg)"
                 callback.write_tcp_buf(self.data_socket,msg, "file_clerk tape_list, datasocket")
                 msg=""
         key,value=dict.cursor("next")
     dict.cursor("close")
     callback.write_tcp_buf(self.data_socket,msg, "file_clerk tape_list(2), datasocket")
     self.data_socket.close()
     callback.write_tcp_socket(self.control_socket,ticket, "file_clerk tape_list, controlsocket")
     self.control_socket.close()
     return

    def start_backup(self,ticket):
        dict.start_backup()
        self.reply_to_caller({"status" : (e_errors.OK, None),\
                "start_backup"  : 'yes' })

    def stop_backup(self,ticket):
        dict.stop_backup()
        self.reply_to_caller({"status" : (e_errors.OK, None),\
                "stop_backup"  : 'yes' })

class FileClerk(FileClerkMethods, generic_server.GenericServer):

    def __init__(self, csc):
        generic_server.GenericServer.__init__(self, csc, MY_NAME)
        Trace.init(self.log_name)
	#   pretend that we are the test system
	#   remember, in a system, there is only one bfs
	#   get our port and host from the name server
	#   exit if the host is not this machine
	keys = self.csc.get(MY_NAME)
	dispatching_worker.DispatchingWorker.__init__(self, (keys['hostip'], \
	                                              keys['port']))


class FileClerkInterface(generic_server.GenericServerInterface):
    pass

if __name__ == "__main__":
    Trace.init(string.upper(MY_NAME))

    # get the interface
    intf = FileClerkInterface()

    # get a file clerk
    fc = FileClerk((intf.config_host, intf.config_port))
    Trace.log(e_errors.INFO, '%s' % sys.argv)

    Trace.log(e_errors.INFO,"opening file database using DbTable")
    dict = db.DbTable("file", [])
    Trace.log(e_errors.INFO,"hurrah, file database is open")

    while 1:
        try:
            Trace.log(e_errors.INFO, "File Clerk (re)starting")
            fc.serve_forever()
        except:
	    fc.serve_forever_error(fc.log_name)
            continue
    Trace.trace(e_errors.ERROR,"File Clerk finished (impossible)")
