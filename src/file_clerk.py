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
import dispatching_worker
import generic_server
import event_relay_client
import monitored_server
import enstore_constants
import edb
import Trace
import e_errors
import configuration_client
import hostaddr
import pnfs
import volume_family

MY_NAME = "file_clerk"

class FileClerkMethods(dispatching_worker.DispatchingWorker):

    def __init__(self, csc):
        dispatching_worker.DispatchingWorker.__init__(self, csc)
        self.dict = None
        return

    # set_brand(brand) -- set brand

    def set_brand(self, brand):
        self.brand = brand
        return

    def get_brand(self, ticket):
        ticket['brand'] = self.brand
        ticket['status'] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        return

    #### DONE
    # we need a new bit field id for each new file in the system
    def new_bit_file(self, ticket):
        # input ticket is a file clerk part of the main ticket
        # create empty record and control what goes into database
        # do not pass ticket, for example to the database!
        record = {'pnfsid':'','drive':'','pnfs_name0':'','deleted':'unknown'}

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

    #### DONE
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

        # start (10/18/00) adding which drive we used to write the file
        drive = ticket["fc"].get("drive","unknown:unknown")

        # look up in our dictionary the request bit field id
        record = self.dict[bfid] 
        if not record:
            msg = "File Clerk: bfid %s not found"%(bfid,)
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
        record["deleted"] = "no"

        # record our changes
        self.dict[bfid] = record 
        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        Trace.trace(12,'set_pnfsid %s'%(ticket,))
        return

    #### DONE
    def get_crcs(self, ticket):
        try:
            bfid  =ticket["bfid"]
        except KeyError, detail:
            msg =  "File Clerk: key %s is missing" % (detail,)
            ticket["status"]=(e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        record=self.dict[bfid]
        if not record:
            msg = "File Clerk: no such bfid %s"%(bfid)
            ticket["status"]=(e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        complete_crc=record["complete_crc"]
        sanity_cookie=record["sanity_cookie"]
        ticket["status"]=(e_errors.OK, None)
        ticket["complete_crc"]=complete_crc
        ticket["sanity_cookie"]=sanity_cookie
        self.reply_to_caller(ticket)

    #### DONE
    def set_crcs(self, ticket):
        try:
            bfid  =ticket["bfid"]
            complete_crc=ticket["complete_crc"]
            sanity_cookie=ticket["sanity_cookie"]
        except KeyError, detail:
            msg = "File Clerk: key %s is missing"%(detail,)
            ticket["status"]=(e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        record=self.dict[bfid]
        if not record:
            msg = "File Clerk: no such bfid %s"%(bfid)
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


    #### DONE        
    # change the delete state element in the dictionary
    def set_deleted(self, ticket):
        try:
            bfid = ticket["bfid"]
            deleted = ticket["deleted"]
        except KeyError, detail:
            msg =  "File Clerk: key %s is missing" % (detail,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        record = self.dict[bfid]
        if not record:
            msg = "File Clerk: no such bfid %s"%(bfid)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        if record["deleted"] != deleted:
            record["deleted"] = deleted
            self.dict[bfid] = record
        ticket["status"] = (e_errors.OK, None)
        # look up in our dictionary the request bit field id
        self.reply_to_caller(ticket)
        Trace.trace(12,'set_deleted %s'%(ticket,))
        return

    #### DONE
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

    # DONE
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
        finfo = self.dict[bfid] 
        if not finfo:
            ticket["status"] = (e_errors.KEYERROR, 
                                "File Clerk: bfid %s not found"%(bfid,))
            Trace.log(e_errors.INFO, "%s"%(ticket,))
            self.reply_to_caller(ticket)
            Trace.trace(10,"bfid_info %s"%(ticket["status"],))
            return

        #Copy all file information we have to user's ticket.  Copy the info
        # one key at a time to avoid cyclic dictionary references.
        for key in finfo.keys():
            ticket[key] = finfo[key]

        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        Trace.trace(10,"bfid_info bfid=%s"%(bfid,))
        return

    #### DONE
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

        # This is a restricted service
        status = self.restricted_access()
        if status:
            msg = "attempt to delete file %s from %s"%(bfid, self.reply_address[0])
            Trace.log(e_errors.ERROR, msg)
            ticket['status'] = status
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

    #### DONE
    # __erase_volume(self, vol) -- delete all files belonging to vol
    #
    # This is only the file clerk portion of erasing a volume.
    # A complete erasing needs to erase volume information too.
    #
    # This involves removing volmap directory in /pnfs.
    # If it fails, nothing would be done further.

    def __erase_volume(self, vol):
        Trace.log(e_errors.INFO, 'erasing files of volume %s'%(vol))
        try:
            bfids = self.get_all_bfids(vol)
        except:
            exc_type, exc_value = sys.exc_info()[:2]
            msg = "__erase_volume(): can not get bfids for '%s' %s %s"%(vol, str(exc_type), str(exc_value))
            Trace.log(e_errors.ERROR, msg)
            return e_errors.ERROR, msg

        # remove file record
        for bfid in bfids:
            try:
                del self.dict[bfid]
            except:
                exc_type, exc_value = sys.exc_info()[:2]
                msg = "__erase_volume(): failed to remove record '%s' %s %s"%(bfid, str(exc_type), str(exc_value))
                Trace.log(e_errors.ERROR, msg)
                return e_errors.ERROR, msg

        Trace.log(e_errors.INFO, 'files of volume %s are erased'%(vol))
        return e_errors.OK, None

    # erase_volume -- server service
    #### DONE
    def erase_volume(self, ticket):
        try:
            vol = ticket["external_label"]
        except KeyError, detail:
            msg = "File Clerk: key %s is missing" % (detail,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        ticket["status"] = (e_errors.OK, None)
        # catch any failure
        try:
            ticket['status'] = self.__erase_volume(vol)
        except:
            exc_type, exc_value = sys.exc_info()[:2]
            msg = 'erase failed due to: '+str(exc_type)+' '+str(exc_value)
            Trace.log(e_errors.ERROR, msg)
            ticket["status"] = (e_errors.ERROR, msg)
        # and return to the caller
        self.reply_to_caller(ticket)
        return

    # __delete_volume(self, vol) -- mark all files belonging to vol as deleted
    #
    # Note: this is NOT the counter part of __delete_volume() in
    #       volume clerk, which is simply a renaming to *.deleted
    #### DONE
    def __delete_volume(self, vol):
        Trace.log(e_errors.INFO, 'marking files of volume %s as deleted'%(vol))
        bfids = self.get_all_bfids(vol)
        for bfid in bfids:
            record = self.dict[bfid]
            if record['deleted'] != 'yes':
                record['deleted'] = 'yes'
                self.dict[bfid] = record
        Trace.log(e_errors.INFO, 'all files of volume %s are marked deleted'%(vol))
        return

    #### DONE
    # delete_volume -- server service

    def delete_volume(self, ticket):
        try:
            vol = ticket["external_label"]
        except KeyError, detail:
            msg = "File Clerk: key %s is missing" % (detail,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        ticket["status"] = (e_errors.OK, None)
        # catch any failure
        try:
            self.__delete_volume(vol)
        except:
            ticket["status"] = (e_errors.ERROR, "delete failed")
        # and return to the caller
        self.reply_to_caller(ticket)
        return

    #### DONE
    # __has_undeleted_file(self, vol) -- check if all files are deleted

    def __has_undeleted_file(self, vol):
        Trace.log(e_errors.INFO, 'checking if files of volume %s are deleted'%(vol))
        q = "select bfid from file, volume \
             where volume.label = '%s' and \
                   file.volume = volume.id and \
                   file.deleted = 'n';"%(vol)
        res = self.dict.db.query(q)
        return res.ntuples()

    #### DONE
    # has_undeleted_file -- server service

    def has_undeleted_file(self, ticket):
        try:
            vol = ticket["external_label"]
        except KeyError, detail:
            msg = "File Clerk: key %s is missing" % (detail,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        ticket["status"] = (e_errors.OK, None)
        # catch any failure
        try:
            result = self.__has_undeleted_file(vol)
            ticket["status"] = (e_errors.OK, result)
        except:
            ticket["status"] = (e_errors.ERROR, "inquire failed")
        # and return to the caller
        self.reply_to_caller(ticket)
        return

    #### DONE
    # exist_bfids -- check if a, or a list of, bfid(s) exists/exist

    def exist_bfids(self, ticket):
        try:
            bfids = ticket['bfids']
        except KeyError, detail:
            msg = "File Clerk: key %s is missing" % (detail,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        if type(bfids) == type([]):    # a list
            result = []
            for i in bfids:
                rec = self.dict[i]
                if rec:
                    result.append(1)
                else:
                    result.append(0)
        else:
            rec = self.dict[bfids]
            if rec:
                result = 1
            else:
                result = 0

        ticket['result'] = result
        ticket['status'] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        return

    #### DONE
    # __restore_volume(self, vol) -- restore according to volmap

    def __restore_volume(self, vol):
        Trace.log(e_errors.INFO, 'restoring files for vol %s'%(vol))
        bfids = self.get_all_bfids(vol)
        msg = ""
        for bfid in bfids:
            status = self.__restore_file(bfid)
            if status[1]:
                msg = msg + '\n' + status[1]
        if msg:
            return e_errors.ERROR, msg
        else:
            return e_errors.OK, None

    #### DONE
    # restore_volume -- server service

    def restore_volume(self, ticket):
        try:
            vol = ticket["external_label"]
        except KeyError, detail:
            msg = "File Clerk: key %s is missing" % (detail,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        ticket["status"] = (e_errors.OK, None)
        # catch any failure
        try:
            ticket['status'] = self.__restore_volume(vol)
        except:
            ticket["status"] = (e_errors.ERROR, "restore failed")
        # and return to the caller
        self.reply_to_caller(ticket)
        return

    #### DONE
    # A bit file id is defined to be a 64-bit number whose most significant
    # part is based on the time, and the least significant part is a count
    # to make it unique
    def unique_bit_file_id(self):
        bfid = time.time()
        bfid = long(bfid)*100000
        while self.dict.has_key(self.brand+str(bfid)):
            bfid = bfid + 1
        return self.brand+str(bfid)

    #### DONE
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

        bfids = self.get_all_bfids(external_label)
        callback.write_tcp_obj_new(self.data_socket, bfids)
        self.data_socket.close()
        callback.write_tcp_obj(self.control_socket,ticket)
        self.control_socket.close()
        return

    #### DONE
    # get_all_bfids(external_label) -- get all bfids of a particular volume

    def get_all_bfids(self, external_label):
        q = "select bfid from file, volume\
             where volume.label = '%s' and \
                   file.volume = volume.id;"%(external_label)
        res = self.dict.db.query(q).getresult()
        bfids = []
        for i in res:
            bfids.append(i[0])
        return bfids

    #### DONE
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

        # get a user callback
        if not self.get_user_sockets(ticket):
            return
        callback.write_tcp_obj(self.data_socket,ticket)

        # log the activity
        Trace.log(e_errors.INFO, "start listing "+external_label)
        
        q = "select bfid, crc, deleted, drive, volume.label, \
                    location_cookie, pnfs_path, pnfs_id, \
                    sanity_size, sanity_crc, size \
             from file, volume \
             where \
                 file.volume = volume.id and volume.label = '%s' \
             order by location_cookie;"%(external_label)

        res = self.dict.db.query(q).dictresult()

        vol = []

        for ff in res:
            value = self.dict.export_format(ff)
            if not value.has_key('pnfs_name0'):
                value['pnfs_name0'] = "unknown"
            vol.append(value)

        # finishing up

        callback.write_tcp_obj_new(self.data_socket, vol)
        self.data_socket.close()
        callback.write_tcp_obj(self.control_socket,ticket)
        self.control_socket.close()
        Trace.log(e_errors.INFO, "finish listing "+external_label)
        return

    #### DONE
    # list_active(self, ticket) -- list the active files on a volume
    #     only the /pnfs path is listed
    #     the purpose is to generate a list for deletion before the
    #     deletion of a volume

    def list_active(self,ticket):
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

        # get a user callback
        if not self.get_user_sockets(ticket):
            return
        callback.write_tcp_obj(self.data_socket,ticket)

        q = "select bfid, crc, deleted, drive, volume.label, \
                    location_cookie, pnfs_path, pnfs_id, \
                    sanity_size, sanity_crc, size \
             from file, volume \
             where \
                 file.volume = volume.id and volume.label = '%s' \
             order by location_cookie;"%(external_label)

        res = self.dict.db.query(q).dictresult()

        alist = []

        for ff in res:
            value = self.dict.export_format(ff)
            if not value.has_key('deleted') or value['deleted'] != "yes":
                if value.has_key('pnfs_name0') and value['pnfs_name0']:
                    alist.append(value['pnfs_name0'])

        # finishing up

        callback.write_tcp_obj_new(self.data_socket, alist)
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
            exc, msg = sys.exc_info()[:2]
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
            exc, msg = sys.exc_info()[:2]
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
            exc, msg = sys.exc_info()[:2]
            status = str(exc), str(msg)
            Trace.log(e_errors.ERROR,"backup %s"%(status,))
            self.reply_to_caller({"status"       : status,
                                  "backup"  : 'no' })

    #### DONE
    # add_file_record() -- create a file record
    #
    # This is very dangerous!
    #
    # The bfid must be None or one that has not already existed

    def add_file_record(self, ticket):

        if ticket.has_key('bfid'):
            bfid = ticket['bfid']
            # to see if the bfid has already been used
            record = self.dict[bfid]
            if record:
                msg = 'bfid "%s" has already been used'%(bfid)
                Trace.log(e_errors.ERROR, msg)
                ticket['status'] = (e_errors.ERROR, msg)
                self.reply_to_caller(ticket)
                return
        else:
            bfid = self.unique_bit_file_id()
            ticket['bfid'] = bfid

        # handle branding

        if bfid[0] in string.letters:
            brand = bfid[:4]
            sequence = long(bfid[4:]+'L')
            while self.dict.has_key(self.brand+str(sequence)):
                sequence = sequence + 1
            bfid = self.brand+str(sequence)

        # extracting the values 
        try:
            complete_crc = ticket['complete_crc']
            deleted = ticket['deleted']
            drive = ticket['drive']
            external_label = ticket['external_label']
            location_cookie = ticket['location_cookie']
            pnfs_name0 = ticket['pnfs_name0']
            pnfsid = ticket['pnfsid']
            pnfsvid = ticket.get('pnfsvid')
            sanity_cookie = ticket['sanity_cookie']
            size = ticket['size']
        except KeyError, detail:
            msg =  "File Clerk: add_file_record() -- key %s is missing" % (detail,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        record = {}
        record['bfid'] = bfid
        record['complete_crc'] = complete_crc
        record['deleted'] = deleted
        record['drive'] = drive
        record['external_label'] = external_label
        record['location_cookie'] = location_cookie
        record['pnfs_name0'] = pnfs_name0
        record['pnfsid'] = pnfsid
        if pnfsvid:
            record['pnfsvid'] = pnfsvid
        record['sanity_cookie'] = sanity_cookie
        record['size'] = size

        # assigning it to database
        self.dict[bfid] = record
        Trace.log(e_errors.INFO, 'assigned: '+`record`)
        ticket['status'] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        return

    #### DONE
    # modify_file_record() -- modify file record
    #
    # This is very dangerous!
    #
    # bfid must exist

    def modify_file_record(self, ticket):

        if ticket.has_key('bfid'):
            bfid = ticket['bfid']
            # to see if the bfid exists
            record = self.dict[bfid]
            if not record:
                msg = 'modify_file_record(): bfid "%s" does not exist'%(bfid)
                Trace.log(e_errors.ERROR, msg)
                ticket['status'] = (e_errors.ERROR, msg)
                self.reply_to_caller(ticket)
                return
        else:
            msg = 'modify_file_record(): no bfid specified'
            ticket['status'] = (e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        # better log this
        Trace.log(e_errors.INFO, "start modifying "+`record`)

        # modify the values
        for k in ticket.keys():
            # can not change bfid!
            if k != 'bfid' and record.has_key(k):
                record[k] = ticket[k]

        # assigning it to database
        self.dict[bfid] = record
        Trace.log(e_errors.INFO, 'modified to '+`record`)
        ticket['status'] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        return

    def mark_bad(self, ticket):
        try:
            bfid = ticket['bfid']
            path = ticket['path']
        except KeyError, detail:
            msg = "File Clerk: key %s is missing" % (detail,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        # does this file exist?
        record = self.dict[bfid]
        if not record:
            msg = "file %s does not exist in DB"%(bfid)
            ticket["status"] = (e_errors.KEYERROR, msg)
            self.reply_to_caller(ticket)
            return

        # check if this file has already been marked bad
        q = "select * from bad_file where bfid = '%s';"%(bfid)
        res = self.dict.db.query(q).dictresult()
        if res:
            msg = "file %s has already been marked bad"%(bfid)
            ticket["status"] = (e_errors.KEYERROR, msg)
            self.reply_to_caller(ticket)
            return

        # insert into database
        q = "insert into bad_file (bfid, path) values('%s', '%s');"%(
            bfid, path)
        try:
            res = self.dict.db.query(q)
            ticket['status'] = (e_errors.OK, None)
        except:
            exc_type, exc_value = sys.exc_info()[:2]
            msg = "failed to mark %s bad due to "%(bfid)+str(exc_type)+' '+str(exc_value)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.KEYERROR, msg)

        self.reply_to_caller(ticket)
        return

    def unmark_bad(self, ticket):
        try:
            bfid = ticket['bfid']
        except KeyError, detail:
            msg = "File Clerk: key %s is missing" % (detail,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        q = "delete from bad_file where bfid = '%s';" %(bfid)
        try:
            res = self.dict.db.query(q)
            ticket['status'] = (e_errors.OK, None)
        except:
            exc_type, exc_value = sys.exc_info()[:2]
            msg = "failed to mark %s bad due to "%(bfid)+str(exc_type)+' '+str(exc_value)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.KEYERROR, msg)

        self.reply_to_caller(ticket)
        return

    def show_bad(self, ticket):
        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)

        # get a user callback
        if not self.get_user_sockets(ticket):
            return
        callback.write_tcp_obj(self.data_socket,ticket)

        q = "select label, bad_file.bfid, size, path \
             from bad_file, file, volume \
             where \
                 bad_file.bfid = file.bfid and \
                 file.volume = volume.id;"
        res = self.dict.db.query(q).dictresult()

        # finishing up

        callback.write_tcp_obj_new(self.data_socket, res)
        self.data_socket.close()
        callback.write_tcp_obj(self.control_socket,ticket)
        self.control_socket.close()
        return

    def quit(self, ticket):
	# self.dict.close()
	dispatching_worker.DispatchingWorker.quit(self, ticket)


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
        FileClerkMethods.__init__(self, (keys['hostip'], keys['port']))
        # start our heartbeat to the event relay process
        self.erc.start_heartbeat(enstore_constants.FILE_CLERK, 
                                 self.alive_interval)
        self.brand = ""


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

    # find the brand

    Trace.log(e_errors.INFO,"find the brand")
    try:
        brand = configuration_client.ConfigurationClient(
                (intf.config_host, intf.config_port)).get('file_clerk')['brand']
        Trace.log(e_errors.INFO,"The brand is %s"%(brand))
    except:
        brand = string.upper(string.split(os.uname()[1], ".")[0][:2])+'MS'
        Trace.log(e_errors.INFO,"No brand is found, using '%s'"%(brand))

    fc.set_brand(brand)

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

    db_host = dbInfo['db_host']
    db_port = dbInfo['db_port']

    Trace.log(e_errors.INFO,"opening file database using edb.FileDB")
    try:
        fc.dict = edb.FileDB(host=db_host, port=db_port, jou=jouHome)
    except:
        exc_type, exc_value = sys.exc_info()[:2]
        msg = str(exc_type)+' '+str(exc_value)+' IS POSTMASTER RUNNING?'
        Trace.log(e_errors.ERROR,msg)
        Trace.alarm(e_errors.ERROR,msg, {})
        Trace.log(e_errors.ERROR, "CAN NOT ESTABLISH DATABASE CONNECTION ... QUIT!")
        sys.exit(1) 
    
    while 1:
        try:
            Trace.log(e_errors.INFO, "File Clerk (re)starting")
            fc.serve_forever()
        except SystemExit, exit_code:
            # fc.dict.close()
            sys.exit(exit_code)
        except:
            fc.serve_forever_error(fc.log_name)
            continue
    Trace.trace(e_errors.ERROR,"File Clerk finished (impossible)")
