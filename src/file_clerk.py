#!/usr/bin/env python

##############################################################################
# src/$RCSfile$   $Revision$
#
# system import
import sys
import os
import time
import string
import socket
import select
import pprint

# enstore imports
import setpath
import traceback
import callback
import volume_clerk_client
import dispatching_worker
import generic_server
import event_relay_client
import monitored_server
import enstore_constants
import db
import bfid_db
import Trace
import e_errors
import configuration_client
import hostaddr

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
        self.dict[bfid] = record 
        
        ticket["fc"]["bfid"] = bfid
        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        Trace.trace(10,'new_bit_file bfid=%s'%(bfid,))
        return

    # update the database entry for this file - add the pnfs file id
    def set_pnfsid(self, ticket):
        try:
            bfid = ticket["fc"]["bfid"]
        except KeyError, detail:
            msg =  "File Clerk: key %s is missing"  % (detail,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        # also need new pnfsid - make sure we have this
        try:
            pnfsid = ticket["fc"]["pnfsid"]
        except KeyError, detail:
            msg =  "File Clerk: key %s is missing"  % (detail,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        # temporary workaround - sam doesn't want to update encp too often
        pnfsvid = ticket["fc"].get("pnfsvid")
        pnfs_name0 = ticket["fc"].get("pnfs_name0")
        pnfs_mapname = ticket["fc"].get("pnfs_mapname")

        # start (10/18/00) adding which drive we used to write the file
        drive = ticket["fc"].get("drive","unknown:unknown")

        # look up in our dictionary the request bit field id
        try:
            record = self.dict[bfid] 
        except KeyError, detail:
            "File Clerk: bfid %s not found"%(detail,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        # add the pnfsid
        record["pnfsid"] = pnfsid
        record["drive"] = drive
        # temporary workaround - see above
        if pnfsvid != None:
            record["pnfsvid"] = pnfsvid
        if pnfs_name0 != None:
            record["pnfs_name0"] = pnfs_name0
        if pnfs_mapname != None:
            record["pnfs_mapname"] = pnfs_mapname
        record["deleted"] = "no"

        # record our changes
        self.dict[bfid] = record 
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
                record = self.dict[bfid] 
            except KeyError:
                status = (e_errors.KEYERROR, 
                          "File Clerk: bfid %s not found"%(bfid,))
                Trace.log(e_errors.INFO, "%s"%(status,))
                Trace.trace(10,"set_deleted %s"%(status,))
                return status, None, None


            if 'y' in string.lower(deleted):
                deleted = "yes"
                decr_count = 1
            else:
                deleted = "no"
                decr_count = -1
            # the foolowing fixes a problem with lost 'deleted' entry'
            fix_deleted = 0
            if not record.has_key('deleted'):
                fix_deleted = 1
                record["deleted"] = deleted
                
            if record["deleted"] == deleted:
                # don't return a status error to the user - she needs a 0 status in order to delete
                # the file in the trashcan.  Otherwise we are in a hopeless loop & it makes no sense
                # to try and keep deleting the already deleted file over and over again
                #status = (e_errors.USER_ERROR,
                #                    "%s = %s deleted flag already set to %s - no change." % (bfid,record["pnfs_name0"],record["deleted"]))
                status = (e_errors.OK, None)
                fname=record.get('pnfs_name0','pnfs_name0 is lost')
                Trace.log(e_errors.USER_ERROR, 
                "%s = %s deleted flag already set to %s - no change." % (bfid, fname, record["deleted"]))
                Trace.trace(12,'set_deleted_priv %s'%(status,))
                if fix_deleted:
                    self.dict[bfid] = record
                    Trace.log(e_errors.INFO, 'added missing "deleted" key for bfid %s' % (bfid,))
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
            self.dict[bfid] = record 

            Trace.log(e_errors.INFO,
                      "%s = %s flagged as deleted:%s  volume=%s(%d)  mapfile=%s" %
                      (bfid,record["pnfs_name0"],record["deleted"],
                       record["external_label"],vticket["non_del_files"],record["pnfs_mapname"]))

            # and return to the caller
            status = (e_errors.OK, None)
            fc = record
            vc = vticket
            Trace.trace(12,'set_deleted_priv status %s'%(status,))
            return status, fc, vc

        # if there is an error - log and return it
        except:
            exc, val, tb = Trace.handle_error()
            status = (str(exc), str(val))

    def get_crcs(self, ticket):
        try:
            bfid  =ticket["bfid"]
            record=self.dict[bfid]
            complete_crc=record["complete_crc"]
            sanity_cookie=record["sanity_cookie"]
        except KeyError, detail:
            msg =  "File Clerk: key %s is missing" % (detail,)
            ticket["status"]=(e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return
        ticket["status"]=(e_errors.OK, None)
        ticket["complete_crc"]=complete_crc
        ticket["sanity_cookie"]=sanity_cookie
        self.reply_to_caller(ticket)

    def set_crcs(self, ticket):
        try:
            bfid  =ticket["bfid"]
            complete_crc=ticket["complete_crc"]
            sanity_cookie=ticket["sanity_cookie"]
            record=self.dict[bfid]
        except KeyError, detail:
            msg = "File Clerk: key %s is missing"%(detail,)
            ticket["status"]=(e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return
        record["complete_crc"]=complete_crc
        record["sanity_cookie"]=sanity_cookie
        #record our changes to the database
        self.dict[bfid] = record
        ticket["status"]=(e_errors.OK, None)
        #reply to caller with updated database values
        record=self.dict[bfid]
        ticket["complete_crc"]=record["complete_crc"]
        ticket["sanity_cookie"]=record["sanity_cookie"]
        self.reply_to_caller(ticket)


        
    # change the delete state element in the dictionary
    def set_deleted(self, ticket):
        try:
            bfid = ticket["bfid"]
            deleted = ticket["deleted"]
            restore_dir = ticket["restore_dir"]
        except KeyError, detail:
            msg =  "File Clerk: key %s is missing" % (detail,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        # also need new value of the delete element- make sure we have this
        try:
            deleted = ticket["deleted"]
        except KeyError, detail:
            msg = "File Clerk: key %s is missing" % (detail,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
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
        try:
            fname = ticket["file_name"]
            restore_dir = ticket["restore_dir"]
        except KeyError, detail:
            msg =  "File Clerk: key %s is missing" % (detail,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
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
        file_clerk_host, file_clerk_port, listen_socket = callback.get_callback()
        listen_socket.listen(4)
        ticket["file_clerk_callback_addr"] = (file_clerk_host, file_clerk_port)

        self.control_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.control_socket.connect(ticket['callback_addr'])
        callback.write_tcp_obj(self.control_socket, ticket)
        r, w, x = select.select([listen_socket], [], [], 15)
        if not r:
            listen_socket.close()
            return 0
        data_socket, address = listen_socket.accept()
        if not hostaddr.allow(address):
            data_socket.close()
            listen_socket.close()
            return 0
        self.data_socket = data_socket
        listen_socket.close()
        return 1

    # return all info about a certain bfid - this does everything that the
    # read_from_hsm method does, except send the ticket to the library manager
    def bfid_info(self, ticket):
        try:
            bfid = ticket["bfid"]
        except KeyError, detail:
            msg = "File Clerk: key %s is missing"%(detail,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        # look up in our dictionary the request bit field id
        try:
            finfo = self.dict[bfid] 
        except KeyError, detail:
            ticket["status"] = (e_errors.KEYERROR, 
                                "File Clerk: bfid %s not found"%(bfid,))
            Trace.log(e_errors.INFO, "%s"%(ticket,))
            self.reply_to_caller(ticket)
            Trace.trace(10,"bfid_info %s"%(ticket["status"],))
            return

        # chek if finfo has deleted key and if not fix the record
        if not finfo.has_key('deleted'):
            finfo['deleted'] = 'no'
            Trace.log(e_errors.INFO, 'added missing "deleted" key for bfid %s' % (bfid,))
            self.dict[bfid] = finfo

        #Copy all file information we have to user's ticket.  Copy the info
        # one key at a time to avoid cyclic dictionary references.
        for key in finfo.keys():
            ticket[key] = finfo[key]

        #####################################################################
        #The folllowing is included for backward compatiblity with old encps.
        #####################################################################
        ticket["fc"] = finfo

        # become a client of the volume clerk to get library information
        Trace.trace(11,"bfid_info getting volume clerk")
        vcc = volume_clerk_client.VolumeClerkClient(self.csc)
        Trace.trace(11,"bfid_info got volume clerk")

        try:
            external_label = finfo["external_label"]
        except KeyError, detail:
            msg =  "File Clerk: key %s is missing" % (detail,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        # ask the volume clerk server which library has "external_label" in it
        Trace.trace(11,"bfid_info inquiring about volume=%s"%(external_label,))
        vticket = vcc.inquire_vol(external_label)
        if vticket["status"][0] != e_errors.OK:
            Trace.log(e_errors.INFO, "%s"%(ticket,))
            self.reply_to_caller(vticket)
            return
        library = vticket["library"]
        Trace.trace(11,"bfid_info volume=%s in library %s"%
                    (external_label,library))

        # copy all volume information we have to user's ticket
        ticket["vc"] = vticket
        #####################################################################
        #The previous is included for backward compatiblity with old encps.
        #####################################################################

        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        Trace.trace(10,"bfid_info bfid=%s"%(bfid,))
        return

    # return volume map name for given bfid
    def get_volmap_name(self, ticket):
        try:
            bfid = ticket["bfid"]
        except KeyError, detail:
            msg  = "File Clerk: key %s is missing" % (detail,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.INFO, msg)
            self.reply_to_caller(ticket)
            return

        # look up in our dictionary the request bit field id
        try:
            finfo = self.dict[bfid] 
        except KeyError, detail:
            msg = "File Clerk: bfid %s not found"%(detail,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        # copy all file information we have to user's ticket
        try:
            ticket["pnfs_mapname"] = finfo["pnfs_mapname"]
            ticket["status"] = (e_errors.OK, None)
        except KeyError:
            ticket['status'] = (e_errors.KEYERROR, None)

        self.reply_to_caller(ticket)
        Trace.trace(10,"get_volmap_name %s"%(ticket["status"],))
        return

    # change the delete state element in the dictionary
    def del_bfid(self, ticket):
        try:
            bfid = ticket["bfid"]
        except KeyError, detail:
            msg = "File Clerk: key %s is missing" % (detail,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
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
        try:
            bfid = ticket["bfid"]
            label = ticket["external_label"]
            set_deleted = ticket[ "set_deleted"]
            restore_volmap = ticket["restore"]
            restore_dir = ticket["restore_dir"]
        except KeyError, detail:
            msg = "File Clerk: key %s is missing" % (detail,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        record = self.dict[bfid] 
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
        self.dict[bfid] = record 
 
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

    # get_bfids(self, ticket) -- get bfids of a certain volume
    #        This is almost the same as tape_list() yet it does not
    #        retrieve any information from primary file database

    def get_bfids(self, ticket):
        try:
            external_label = ticket["external_label"]
            ticket["status"] = (e_errors.OK, None)
            self.reply_to_caller(ticket)
        except KeyError, detail:
            msg = "File Clerk: key %s is missing"%(detail,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        # get a user callback
        if not self.get_user_sockets(ticket):
            return
        callback.write_tcp_obj(self.data_socket,ticket)

        # see if index file exists

        if self.dict.inx.has_key('external_label'):
            # now get a cursor so we can loop on the database quickly:
            c = self.dict.inx['external_label'].cursor()
            key, pkey = c.set(external_label)
            while key:
                callback.write_tcp_raw(self.data_socket, pkey+'\n')
                key, pkey = c.nextDup()
            c.close()
        else:  # use bfid_db
            try:
                bfid_list = self.bfid_db.get_all_bfids(external_label)
            except:
                msg = "File Clerk: no entry for volume %s" % external_label
                ticket["status"] = (e_errors.KEYERROR, msg)
                Trace.log(e_errors.ERROR, msg)
                bfid_list = []
            for bfid in bfid_list:
                callback.write_tcp_raw(self.data_socket, bfid+'\n')
        # finishing up
        callback.write_tcp_raw(self.data_socket, "")
        self.data_socket.close()
        callback.write_tcp_obj(self.control_socket,ticket)
        self.control_socket.close()
        return

    def tape_list(self,ticket):
        try:
            external_label = ticket["external_label"]
            ticket["status"] = (e_errors.OK, None)
            self.reply_to_caller(ticket)
        except KeyError, detail:
            msg = "File Clerk: key %s is missing"%(detail,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            ####XXX client hangs waiting for TCP reply
            return

        # fork as it may take quite a while to get the list
        # if self.fork() != 0:
        #    return

        # get a user callback
        if not self.get_user_sockets(ticket):
            return
        callback.write_tcp_obj(self.data_socket,ticket)
        msg="     label            bfid       size        location_cookie delflag original_name\n"
        callback.write_tcp_raw(self.data_socket, msg)

        # if index is available, use index, otherwise use bfid_db to be
        # backward compatible

        if self.dict.inx.has_key('external_label'):  # use index
            print "tape_list(): using index"
            # now get a cursor so we can loop on the database quickly:
            c = self.dict.inx['external_label'].cursor()
            key, pkey = c.set(external_label)
            while key:
                value = self.dict[pkey]
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
                key,pkey = c.nextDup()
            c.close()
        else:  # use bfid_db
            print "tape_list(): using bfid_db"
            try:
                bfid_list = self.bfid_db.get_all_bfids(external_label)
            except:
                msg = "File Clerk: no entry for volume %s" % external_label
                ticket["status"] = (e_errors.KEYERROR, msg)
                Trace.log(e_errors.ERROR, msg)
                bfid_list = []
            for bfid in bfid_list:
                value = self.dict[bfid]
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

        # finishing up

        callback.write_tcp_raw(self.data_socket, "")
        self.data_socket.close()
        callback.write_tcp_obj(self.control_socket,ticket)
        self.control_socket.close()
        return

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
        self.alive_interval = monitored_server.get_alive_interval(self.csc,
                                                                  MY_NAME,
                                                                  keys)
        dispatching_worker.DispatchingWorker.__init__(self, (keys['hostip'], 
                                                      keys['port']))
        # start our heartbeat to the event relay process
        self.erc.start_heartbeat(enstore_constants.FILE_CLERK, 
                                 self.alive_interval)


class FileClerkInterface(generic_server.GenericServerInterface):
    pass

if __name__ == "__main__":
    Trace.init(string.upper(MY_NAME))

    # get the interface
    intf = FileClerkInterface()

    # get a file clerk
    fc = FileClerk((intf.config_host, intf.config_port))
    fc.handle_generic_commands(intf)
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
    # see if there is an index file
    if os.access(os.path.join(dbHome, 'file.external_label.index'), os.F_OK):
        print "open with index"
        fc.dict = db.DbTable("file", dbHome, jouHome, ['external_label']) 
    else:
        print "open with no index"
        fc.dict = db.DbTable("file", dbHome, jouHome) 
    fc.bfid_db = bfid_db.BfidDb(dbHome)
    Trace.log(e_errors.INFO,"hurrah, file database is open")
    
    while 1:
        try:
            Trace.log(e_errors.INFO, "File Clerk (re)starting")
            fc.serve_forever()
        except SystemExit, exit_code:
            fc.dict.close()
            sys.exit(exit_code)
        except:
            fc.serve_forever_error(fc.log_name)
            continue
    Trace.trace(e_errors.ERROR,"File Clerk finished (impossible)")
