##############################################################################
# src/$RCSfile$   $Revision$
#
# system import
import sys
import time
import string
import os

# enstore imports
import traceback
import callback
import volume_clerk_client
import dispatching_worker
import generic_server
import db
import Trace
import e_errors
import configuration_client


MY_NAME = "file_clerk"

class FileClerkMethods(dispatching_worker.DispatchingWorker):

    # we need a new bit field id for each new file in the system
    def new_bit_file(self, ticket):
     # input ticket is a file clerk part of the main ticket
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
     self.dict[bfid] = record ## was deepcopy
     
     ticket["fc"]["bfid"] = bfid
     ticket["status"] = (e_errors.OK, None)
     self.reply_to_caller(ticket)
     Trace.trace(10,'new_bit_file bfid=%s'%(bfid,))
     return

    # update the database entry for this file - add the pnfs file id
    def set_pnfsid(self, ticket):
        # everything is based on bfid - make sure we have this
        try:
            key="bfid"
            bfid = ticket["fc"][key]
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, 
                                "File Clerk: "+key+" key is missing")
            Trace.log(e_errors.INFO, "%s"%(ticket,))
            self.reply_to_caller(ticket)
            Trace.trace(10,"set_pnfsid %s"%(ticket["status"],))
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
            Trace.log(e_errors.INFO, "%s"%(ticket,))
            self.reply_to_caller(ticket)
            Trace.trace(10,"set_pnfsid %s"%(ticket["status"],))
            return

        # look up in our dictionary the request bit field id
        try:
            record = self.dict[bfid] ## was deepcopy
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR,"File Clerk: bfid %s not found"%(bfid,))
            Trace.log(e_errors.INFO, "%s"%(ticket,))
            self.reply_to_caller(ticket)
            Trace.trace(10,"bfid_info %s"%(ticket["status"],))
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
        self.dict[bfid] = record ## was deepcopy
        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        Trace.trace(12,'set_pnfsid %s'%(ticket,))
        return

    # change the delete state element in the dictionary
    # this method is for private use only
    def set_deleted_priv(self, bfid, deleted, restore_dir="no"):
     try:
        # look up in our dictionary the request bit field id
        try:
            record = self.dict[bfid] ## was deepcopy
        except KeyError:
            status = (e_errors.KEYERROR, 
		      "File Clerk: bfid %s not found"%(bfid,))
            Trace.log(e_errors.INFO, "%s"%(status,))
            Trace.trace(10,"set_deleted %s"%(status,))
            return status, None, None
        

	if string.find(string.lower(deleted),'y') !=-1 or \
	   string.find(string.lower(deleted),'Y') !=-1:
	    deleted = "yes"
	    decr_count = 1
	else:
	    self.deleted = "no"
	    decr_count = -1
        if record["deleted"] == deleted:
            # don't return a status error to the user - she needs a 0 status in order to delete
            # the file in the trashcan.  Otherwise we are in a hopeless loop & it makes no sense
            # to try and keep deleting the already deleted file over and over again
            #status = (e_errors.USER_ERROR,
            #                    "%s = %s deleted flag already set to %s - no change." % (bfid,record["pnfs_name0"],record["deleted"]))
            status = (e_errors.OK, None)
            Trace.log(e_errors.USER_ERROR, 
            "%s = %s deleted flag already set to %s - no change." % (bfid,record["pnfs_name0"],record["deleted"]))
            Trace.trace(12,'set_deleted_priv %s'%(status,))
            return status, None, None
            
	if deleted == "no":
	    # restore pnfs entry
	    import pnfs
	    map = pnfs.Pnfs(record["pnfs_mapname"])
	    status = map.restore_from_volmap(restore_dir)
	    del map
	    if status[0] != e_errors.OK:
		Trace.log(e_errors.ERROR, "restore_from_volmap failed. Status: %s"%(status,))
		return status, None, None 

        # mod the delete state
        record["deleted"] = deleted

        # become a client of the volume clerk and decrement the non-del files on the volume
        vcc = volume_clerk_client.VolumeClerkClient(self.csc)
        vticket = vcc.decr_file_count(record['external_label'],decr_count)
	status = vticket["status"]
	if status[0] != e_errors.OK: 
	    Trace.log(e_errors.ERROR, "decr_file_count failed. Status: %s"%(status,))
	    return status, None, None 

        # record our changes
        self.dict[bfid] = record ## was deepcopy

        Trace.log(e_errors.INFO,
                  "%s = %s flagged as deleted:%s  volume=%s(%d)  mapfile=%s" %
                  (bfid,record["pnfs_name0"],record["deleted"],record["external_label"],vticket["non_del_files"],record["pnfs_mapname"]))

        # and return to the caller
        status = (e_errors.OK, None)
        fc = record
        vc = vticket
        Trace.trace(12,'set_deleted_priv status %s'%(status,))
        return status, fc, vc

     # if there is an error - log and return it
     except:
	 exc, val, tb = e_errors.handle_error()
         status = (str(exc), str(val))

    # change the delete state element in the dictionary
    def set_deleted(self, ticket):
        # everything is based on bfid - make sure we have this
        try:
            key="bfid"
            bfid = ticket[key]
            key="deleted"
            deleted = ticket[key]
	    key="restore_dir"
	    restore_dir = ticket[key]
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, 
                                "File Clerk: "+key+" key is missing")
            Trace.log(e_errors.INFO, "%s"%(ticket,))
            self.reply_to_caller(ticket)
            Trace.trace(10,"set_deleted %s"%(ticket["status"],))
            return

        # also need new value of the delete element- make sure we have this
        try:
            key2="deleted"
            deleted = ticket[key2]
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, "File Clerk: "+key2+" key is missing")
            Trace.log(e_errors.INFO, "%s"%(ticket,))
            self.reply_to_caller(ticket)
            Trace.trace(10,"set_deleted status %s"%(ticket["status"],))
            return

	status, fc, vc = self.set_deleted_priv(bfid, deleted, restore_dir)
	ticket["status"] = status
	if fc: ticket["fc"] = fc
	if vc: ticket["vc"] = fc
        # look up in our dictionary the request bit field id
        self.reply_to_caller(ticket)
        Trace.trace(12,'set_deleted %s'%(ticket,))
        return

    # restore specified file
    def restore_file(self, ticket):
        # everything is based on bfid - make sure we have this
        try:
            key="file_name"
            fname = ticket[key]
            key = "restore_dir"
	    restore_dir = ticket[key]
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, 
                                "File Clerk: "+key+" key is missing")
            Trace.log(e_errors.INFO, "%s"%(ticket,))
            self.reply_to_caller(ticket)
            Trace.trace(10,"restore_file %s"%(ticket["status"],))
            return
	# find the file in db
	bfid = None
	self.dict.cursor("open")
	key,value=self.dict.cursor("first")
	while key:
	    if value["pnfs_name0"] == fname:
		bfid = value["bfid"]
		break
	    key,value=self.dict.cursor("next")
	self.dict.cursor("close")
	# file not found
	if not bfid:
	    ticket["status"] = "ENOENT", "File %s not found"%(fname,)
            Trace.log(e_errors.INFO, "%s"%(ticket,))
            self.reply_to_caller(ticket)
            Trace.trace(10,"restore_file %s"%(ticket["status"],))
            return

	if string.find(value["external_label"],'deleted') !=-1:
	    ticket["status"] = "EACCES", "volume %s is deleted"%(value["external_label"],)
            Trace.log(e_errors.INFO, "%s"%(ticket,))
            self.reply_to_caller(ticket)
            Trace.trace(10,"restore_file %s"%(ticket["status"],))

	status, fc, vc = self.set_deleted_priv(bfid, "no", restore_dir)
	ticket["status"] = status
	if fc: ticket["fc"] = fc
	if vc: ticket["vc"] = fc

        self.reply_to_caller(ticket)
        Trace.trace(12,'restore_file %s'%(ticket,))
        return

    def get_user_sockets(self, ticket):
        file_clerk_host, file_clerk_port, listen_socket =\
                           callback.get_callback()
        listen_socket.listen(4)
        ticket["file_clerk_callback_host"] = file_clerk_host
        ticket["file_clerk_callback_port"] = file_clerk_port
        self.control_socket = callback.user_callback_socket(ticket)
        data_socket, address = listen_socket.accept()
        self.data_socket = data_socket
        listen_socket.close()
        Trace.trace(16,"get_user_sockets host=%s, file_clerk_port=%s"%
                    (file_clerk_host,file_clerk_port))

    # return all the bfids in our dictionary.  Not so useful!
    def get_bfids(self,ticket):
        ticket["status"] = (e_errors.OK, None)
        try:
            self.reply_to_caller(ticket)
        # even if there is an error - respond to caller so he can process it
        except:
            exc,msg,tb=sys.exc_info()
            ticket["status"] = str(exc),str(msg)
            self.reply_to_caller(ticket)
            Trace.trace(10,"get_bfids %s"%(ticket["status"],))
            return
        self.get_user_sockets(ticket)
        ticket["status"] = (e_errors.OK, None)
        callback.write_tcp_obj(self.data_socket,ticket)
        self.dict.cursor("open")
        key,value=self.dict.cursor("first")
        while key:
            callback.write_tcp_raw(self.data_socket,repr(key))
            key,value=self.dict.cursor("next")
        callback.write_tcp_raw(self.data_socket,"")
        self.dict.cursor("close")
        callback.write_tcp_raw(self.data_socket,"")
        self.data_socket.close()
        callback.write_tcp_obj(self.control_socket,ticket)
        self.control_socket.close()
        return


    # return all info about a certain bfid - this does everything that the
    # read_from_hsm method does, except send the ticket to the library manager
    def bfid_info(self, ticket):
        # everything is based on bfid - make sure we have this
        try:
            key="bfid"
            bfid = ticket[key]
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, 
                                "File Clerk: %s key is missing"%(key,))
            Trace.log(e_errors.INFO, "%s"%(ticket,))
            self.reply_to_caller(ticket)
            Trace.trace(10,"bfid_info %s"%(ticket["status"],))
            return

        # look up in our dictionary the request bit field id
        try:
            finfo = self.dict[bfid] ## was deepcopy
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, 
                                "File Clerk: bfid %s not found"%(bfid,))
            Trace.log(e_errors.INFO, "%s"%(ticket,))
            self.reply_to_caller(ticket)
            Trace.trace(10,"bfid_info %s"%(ticket["status"],))
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
            ticket["status"] = (e_errors.KEYERROR, 
                                "File Clerk: "+key+" key is missing")
            Trace.log(e_errors.INFO, "%s"%(ticket,))
            self.reply_to_caller(ticket)
            Trace.trace(10,"bfid_info %s"%(ticket["status"],))
            return

        # ask the volume clerk server which library has "external_label" in it
        Trace.trace(11,"bfid_info inquiring about volume=%s"%(external_label,))
        vticket = vcc.inquire_vol(external_label)
        if vticket["status"][0] != e_errors.OK:
            Trace.log(e_errors.INFO, "%s"%(ticket,))
            self.reply_to_caller(vticket)
            Trace.trace(10,"bfid_info %s"%(ticket["status"],))
            return
        library = vticket["library"]
        Trace.trace(11,"bfid_info volume=%s in library %s"%
                    (external_label,library))

        # copy all volume information we have to user's ticket
        ticket["vc"] = vticket

        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        Trace.trace(10,"bfid_info bfid=%s"%(bfid,))
        return

    # return volume map name for given bfid
    def get_volmap_name(self, ticket):
        # everything is based on bfid - make sure we have this
        try:
            key="bfid"
            bfid = ticket[key]
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, 
                                "File Clerk: "+key+" key is missing")
            Trace.log(e_errors.INFO, "%s"%(ticket,))
            self.reply_to_caller(ticket)
            Trace.trace(10,"bfid_info %s"%(ticket["status"],))
            return

        # look up in our dictionary the request bit field id
        try:
            finfo = self.dict[bfid] ## was deepcopy
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, 
                                "File Clerk: bfid %s not found"%(bfid,))
            Trace.log(e_errors.INFO, "%s"%(ticket,))
            self.reply_to_caller(ticket)
            Trace.trace(10,"bfid_info %s"%(ticket["status"],))
            return

        # copy all file information we have to user's ticket
        ticket["pnfs_mapname"] = finfo["pnfs_mapname"]
	ticket["status"] = (e_errors.OK, None)

	self.reply_to_caller(ticket)
	Trace.trace(10,"get_volmap_name %s"%(ticket["status"],))
	return

    # change the delete state element in the dictionary
    def del_bfid(self, ticket):
        # everything is based on bfid - make sure we have this
        try:
            key="bfid"
            bfid = ticket[key]
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, 
                                "File Clerk: "+key+" key is missing")
            Trace.log(e_errors.INFO, "%s"%(ticket,))
            self.reply_to_caller(ticket)
            Trace.trace(10,"del_bfid: status %s"%(ticket["status"],))
            return

        # now just delete the bfid
        del self.dict[bfid]
        Trace.log(e_errors.INFO, "bfid %s has been removed from DB"%(bfid,))

        # and return to the caller
        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        Trace.trace(12,'del_bfid %s'%(ticket,))
        return

    # rename volume and volume map
    def rename_volume(self, ticket):
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
	    key = "restore_dir"
	    restore_dir = ticket[key]

        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, 
                                "File Clerk: "+key+" key is missing")
            Trace.log(e_errors.INFO, "%s"%(ticket,))
            self.reply_to_caller(ticket)
            Trace.trace(10,"rename_volume %s"%(ticket["status"],))
            return

	record = self.dict[bfid] ## was deepcopy
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
	    status = map.restore_from_volmap(restore_dir)
	    del map
	    if status[0] != e_errors.OK:
		ticket["status"] = status
		self.reply_to_caller(ticket)
		Trace.log(e_errors.ERROR,'rename_volume failed %s'%(ticket,))
		return
	self.dict[bfid] = record ## was deepcopy
 
        # and return to the caller
        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        Trace.log(e_errors.INFO,'volume renamed for %s'%(ticket,))
        return

    # A bit file id is defined to be a 64-bit number whose most significant
    # part is based on the time, and the least significant part is a count
    # to make it unique
    def unique_bit_file_id(self):
        bfid = time.time()
        bfid = long(bfid)*100000
        while self.dict.has_key(str(bfid)):
            bfid = bfid + 1
        return str(bfid)

    def tape_list(self,ticket):
     # everything is based on external_label - make sure we have this
     try:
         key="external_label"
         external_label = ticket[key]
         ticket["status"] = (e_errors.OK, None)
         self.reply_to_caller(ticket)
     except KeyError:
         ticket["status"] = (e_errors.KEYERROR,"File Clerk: %s key is missing"%(key,))
         Trace.log(e_errors.INFO, "%s"%(ticket,))
         self.reply_to_caller(ticket)
         Trace.trace(10,"tape_list %s"%(ticket["status"],))
         return

     if self.fork() != 0:
         return
     # get a user callback
     self.get_user_sockets(ticket)
     callback.write_tcp_obj(self.data_socket,ticket)
     msg="     label            bfid       size        location_cookie delflag original_name\n"
     callback.write_tcp_raw(self.data_socket, msg)
     # now get a cursor so we can loop on the database quickly:
     self.dict.cursor("open")
     key,value=self.dict.cursor("first")
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
             msg= "%10s %s %10i %22s %7s %s\n" % (external_label, value['bfid'],
                                                      value['size'],value['location_cookie'],
                                                      deleted,value['pnfs_name0'])
             callback.write_tcp_raw(self.data_socket, msg)
         key,value=self.dict.cursor("next")
     self.dict.cursor("close")
     callback.write_tcp_raw(self.data_socket, "")
     self.data_socket.close()
     callback.write_tcp_obj(self.control_socket,ticket)
     self.control_socket.close()
     return

#    def start_backup(self,ticket):
#        dict.start_backup()
#        self.reply_to_caller({"status" : (e_errors.OK, None),
#                "start_backup"  : 'yes' })
#
#    def stop_backup(self,ticket):
#        dict.stop_backup()
#        self.reply_to_caller({"status" : (e_errors.OK, None),
#                "stop_backup"  : 'yes' })

    def start_backup(self,ticket):
        try:
            Trace.log(e_errors.INFO,"start_backup")
            self.dict.start_backup()
            self.reply_to_caller({"status"        : (e_errors.OK, None),
                                  "start_backup"  : 'yes' })
        # catch any error and keep going. server needs to be robust
        except:
            exc, msg, tb = sys.exc_info()
            status = str(exc), str(msg)
            Trace.log(e_errors.ERROR,"start_backup %s"%(status,))
            self.reply_to_caller({"status"       : status,
                                  "start_backup" : 'no' })


    def stop_backup(self,ticket):
        try:
            Trace.log(e_errors.INFO,"stop_backup")
            self.dict.stop_backup()
            self.reply_to_caller({"status"       : (e_errors.OK, None),
                                  "stop_backup"  : 'yes' })
        # catch any error and keep going. server needs to be robust
        except:
            exc, msg, tb = sys.exc_info()
            status = str(exc), str(msg)
            Trace.log(e_errors.ERROR,"stop_backup %s"%(status,))
            self.reply_to_caller({"status"       : status,
                                  "stop_backup"  : 'no' })

    def backup(self,ticket):
        try:
            Trace.log(e_errors.INFO,"backup")
            self.dict.backup()
            self.reply_to_caller({"status"       : (e_errors.OK, None),
                                  "backup"  : 'yes' })
        # catch any error and keep going. server needs to be robust
        except:
            exc, msg, tb = sys.exc_info()
            status = str(exc), str(msg)
            Trace.log(e_errors.ERROR,"backup %s"%(status,))
            self.reply_to_caller({"status"       : status,
                                  "backup"  : 'no' })


class FileClerk(FileClerkMethods, generic_server.GenericServer):

    def __init__(self, csc):
        generic_server.GenericServer.__init__(self, csc, MY_NAME)
        Trace.init(self.log_name)
	#   pretend that we are the test system
	#   remember, in a system, there is only one bfs
	#   get our port and host from the name server
	#   exit if the host is not this machine
	keys = self.csc.get(MY_NAME)
	dispatching_worker.DispatchingWorker.__init__(self, (keys['hostip'], 
	                                              keys['port']))


class FileClerkInterface(generic_server.GenericServerInterface):
    pass

if __name__ == "__main__":
    Trace.init(string.upper(MY_NAME))

    # get the interface
    intf = FileClerkInterface()

    # get a file clerk
    fc = FileClerk((intf.config_host, intf.config_port))
    Trace.log(e_errors.INFO, '%s' % (sys.argv,))

    Trace.log(e_errors.INFO,"determine dbHome and jouHome")
    try:
        dbInfo = configuration_client.ConfigurationClient(
		(intf.config_host, intf.config_port)).get('database')
        dbHome = dbInfo['db_dir']
        try:  # backward compatible
            jouHome = dbInfo['jou_dir']
        except:
            jouHome = dbHome
    except:
        dbHome = os.environ['ENSTORE_DIR']
        jouHome = dbHome

    Trace.log(e_errors.INFO,"opening file database using DbTable")
    fc.dict = db.DbTable("file", dbHome, jouHome, [])
    Trace.log(e_errors.INFO,"hurrah, file database is open")

    while 1:
        try:
            Trace.log(e_errors.INFO, "File Clerk (re)starting")
            fc.serve_forever()
        except:
	    fc.serve_forever_error(fc.log_name)
            continue
    Trace.trace(e_errors.ERROR,"File Clerk finished (impossible)")
