#!/usr/bin/env python

###############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
import sys
import os
import time
import errno
import string
import socket
import select

# enstore imports
import setpath
import hostaddr
import callback
import dispatching_worker
import generic_server
import db
import Trace
import e_errors
import configuration_client
import bfid_db
import volume_family
import sg_db
import enstore_constants
import monitored_server

def hack_match(a,b): #XXX clean this up
    a = string.split(a, '.')
    b = string.split(b, '.')
    if len(a) != len(b):
        min_len = min(len(a), len(b))
        a = a[:min_len]
        b = b[:min_len]
    return a==b

# conditional comparison
def mycmp(cond, a, b):
    # condition may be None or some other
    if not cond: return a==b        # if cond is not None use ==
    else: return a!=b               # else use !=

# require 5% more space on a tape than the file size,
#    this accounts for the wrapper overhead and "some" tape rewrites

KB=1024
MB=KB*KB
GB=MB*KB

SAFETY_FACTOR=1.05
#MIN_LEFT=long(300*MB)
MIN_LEFT=long(0) # for now, this is disabled.


MY_NAME = "volume_clerk"

# This is a bfid_db replacement. A prototype for now.

class newBfidDB:
	def __init__(self, dbHome):
		self.db = db.Index(None, dbHome, 'file', 'external_label')

	def get_all_bfids(self, external_label):
		return self.db[external_label]

	def rename_volume(self, old_label, new_label):
		# do onthing
		return

	def delete_all_bfids(self, external_label):
		# do nothing
		return

	def init_dbfile(self, external_label):
		# do nothing
		return

	def add_bfid(self, external_label, bfid):
		# do nothing
		return

class VolumeClerkMethods(dispatching_worker.DispatchingWorker):

    # check if volume is full
    def is_volume_full(self, v, min_remaining_bytes):
        external_label = v['external_label']
        ret = ""
        left = v["remaining_bytes"] 
        if left < long(min_remaining_bytes*SAFETY_FACTOR) or left < MIN_LEFT:
            # if it __ever__ happens that we can't write a file on a
            # volume, then mark volume as full.  This prevents us from
            # putting 1 byte files on old "golden" volumes and potentially
            # losing the entire tape. One could argue that a very large
            # file write could prematurely flag a volume as full, but lets
            # worry about if it is really a problem - I propose that an
            # administrator reset the system_inhibit back to none in these
            # special, and hopefully rare cases.

            if v["system_inhibit"][1] != "full":
                # detect a transition
                ret = e_errors.VOL_SET_TO_FULL
                v["system_inhibit"][1] = "full"
                left = v["remaining_bytes"]/1.
                totb = v["capacity_bytes"]/1.
                if totb != 0:
                    waste = left/totb*100.
                else:
                    waste = 0.
                Trace.log(e_errors.INFO,
                          "%s is now full, bytes remaining = %d, %.2f %%" %
                          (external_label,
                           v["remaining_bytes"],waste))
                
                if self.dict.cursor_open:
                    Trace.log(e_errors.ERROR, "Old style cursor is opened...")

                t = self.dict.db.txn()
                self.dict.db[(external_label,t)] = v
                t.commit()
            else: ret = e_errors.NOSPACE
        return ret

    # rename deleted volume
    def rename_volume(self, old_label, new_label, restore="no"):
     try:
         cur_rec = self.dict[old_label]
         # should not happen
         if self.dict.has_key(new_label):
             rec = self.dict[new_label]
             if rec['system_inhibit'][0] != e_errors.RECYCLE: 
                 return 'EEXIST', "Volume Clerk: volume "+new_label+" already exists"
         # rename volume names in the FC database
         if string.find(new_label, ".deleted") != -1:
             cur_rec["system_inhibit"][0] = e_errors.DELETED
             set_deleted = "yes"
             restore_dir = "no"
         else:
             cur_rec["system_inhibit"][0] = "none"
             if restore == "yes":
                 set_deleted = "no"
                 restore_dir = "yes"
             else:
                 set_deleted = "yes"
                 restore_dir = "yes"

             
         import file_clerk_client
         fcc = file_clerk_client.FileClient(self.csc)
         # get volume map name
         bfid_list = self.bfid_db.get_all_bfids(old_label)
         if bfid_list:
             fcc.bfid = bfid_list[0]
             vm_ticket = fcc.get_volmap_name()
             if vm_ticket.has_key('pnfs_mapname'):
                 old_vol_map_name = vm_ticket["pnfs_mapname"]
                 (old_vm_dir,file) = os.path.split(old_vol_map_name)
                 new_vm_dir = string.replace(old_vm_dir, old_label, new_label)
                 # rename map files
                 Trace.log(e_errors.INFO, "trying volume map directory renamed %s->%s"%
                           (old_vm_dir, new_vm_dir))
                 os.rename(old_vm_dir, new_vm_dir)
                 Trace.log(e_errors.INFO, "volume map directory renamed %s->%s"%
                       (old_vm_dir, new_vm_dir))
         # replace file clerk database entries
         for bfid in bfid_list:
             ret = fcc.rename_volume(bfid, new_label, 
                                     set_deleted, restore, restore_dir)
             if ret["status"][0] != e_errors.OK:
                 Trace.log(e_errors.ERROR, "rename_volume failed: "+repr(ret))
                 
         # create new record in the database
         cur_rec['declared'] = time.time()
         self.dict[new_label] = cur_rec
         # remove current record from the database
         del self.dict[old_label]
         # update the bitfile id database too
         self.bfid_db.rename_volume(old_label,new_label)
         Trace.log(e_errors.INFO, "volume renamed %s->%s"%(old_label,
                                                           new_label))
         return e_errors.OK, None
     # even if there is an error - respond to caller so he can process it
     except:
         exc, val, tb = Trace.handle_error()
         return str(exc), str(val)
     
    # remove deleted volume and all information about it
    def remove_deleted_volume(self, external_label):
     try:
         cur_rec = self.dict[external_label]
         # if volume is not marked as deleted it is an error
         if cur_rec["system_inhibit"][0] != e_errors.DELETED:
             return cur_rec["system_inhibit"][0], "Volume Clerk: volume "+external_label+" is not marked as deleted"
         
         ## if volume is marked as deleted copy its record into DB
         ## and delete original
         else:
             # remove all bfids for this volume
             import file_clerk_client
             fcc = file_clerk_client.FileClient(self.csc)
             bfid_list = self.bfid_db.get_all_bfids(external_label)
             vm_dir = ''
             for bfid in bfid_list:
                 fcc.bfid = bfid
                 vm_ticket = fcc.get_volmap_name()
                 if vm_ticket.has_key("pnfs_mapname"):
                     vol_map_name = vm_ticket["pnfs_mapname"]
                     (vm_dir,file) = os.path.split(vol_map_name)
                     ret = fcc.del_bfid()
                     os.remove(vol_map_name)
                 else:
                     Trace.log(e_errors.WARNING, "no pnfs_mapname entry for bfid %s"%
                               (fcc.bfid,))
             if vm_dir:
                 os.rmdir(vm_dir)
             # remove current record from the database
             del self.dict[external_label]
             # update the bfid database too
             self.bfid_db.delete_all_bfids(external_label)
             new_label = string.replace(external_label, '.deleted','')
             rec = self.dict[new_label]
             rec['system_inhibit'] = ['none', 'none']
             self.dict[new_label] = rec
             Trace.log(e_errors.INFO, "volume removed %s"%(external_label,))

             return e_errors.OK, None
     # even if there is an error - respond to caller so he can process it
     except:
         exc, val, tb = Trace.handle_error()
         return str(exc), str(val)
         
    # remove deleted volume(s)
    # this method is called externally
    def remove_deleted_vols(self, ticket):
        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        # this could tie things up for awhile - fork and let child
        # send the work list (at time of fork) back to client
        #if self.fork() != 0:
        #    return
        vols = []
        try:
            if not self.get_user_sockets(ticket):
                return
            ticket["status"] = (e_errors.OK, None)
            callback.write_tcp_obj(self.data_socket, ticket)
            if not ticket.has_key("external_label"):
                # fill in the list of volumes to delete
                # REMOVE ME LATER
                # The following commented out old style cursor usage
                #   should be removed when the new style cursor usage is
                #   proven to be stable
                # self.dict.cursor("open")
                # key,value=self.dict.cursor("first")
                c = self.dict.newCursor()
                key,value = c.first()
                while key:
                    if value["system_inhibit"][0] == e_errors.DELETED:
                        vols.append(key)
                    key,value = c.next()
                c.close()
            else:
                if self.dict.has_key(ticket["external_label"]):
                    record = self.dict[ticket["external_label"]]
                    if record["system_inhibit"][0] == e_errors.DELETED:
                        vols.append(ticket["external_label"])
            Trace.log(e_errors.INFO,"remove_deleted_vols: vols %s" % (vols,)) 
            for vol in vols:
                ret = self.remove_deleted_volume(vol)
                msg="VOLUME "+vol
                if ret[0] == e_errors.OK: msg = msg+" removed"
                else: msg = msg + " " + repr(ret)
                callback.write_tcp_raw(self.data_socket,msg)                
            self.data_socket.close()
            callback.write_tcp_obj(self.control_socket, ticket)
            self.control_socket.close()
        except:
            c.close()
            Trace.handle_error()
        return
            

    # add: some sort of hook to keep old versions of the s/w out
    # since we should like to have some control over format of the records.
    def addvol(self, ticket):
        # create empty record and control what goes into database
        # do not pass ticket, for example to the database!
        record={}

        try:
            external_label = ticket["external_label"]
        except KeyError, detail:
            msg= "Volume Clerk: key %s is missing" % (detail,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        # can't have 2 with same external_label
        if self.dict.has_key(external_label):
            msg="Volume Clerk: volume %s already exists" % (external_label,)
            ticket["status"] = (errno.errorcode[errno.EEXIST], msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        inc_counter = 0
        # first check quota
        if ticket.has_key('library') and ticket.has_key('storage_group'):
            library = ticket['library']
            sg = ticket['storage_group']
            if sg != 'none':
                # check if quota is enabled
                q_dict = self.quota_enabled(library, sg)
                if q_dict:
                    if self.check_quota(q_dict, library, sg):
                        inc_counter = 1
                    else:
                        msg="Volume Clerk: Quota exceeded, contact enstore admin."
                        ticket["status"] = (e_errors.QUOTAEXCEEDED, msg)
                        Trace.log(e_errors.ERROR,msg)
                        self.reply_to_caller(ticket)
                        return
        else:
            msg= "Volume Clerk: key %s is missing" % (detail,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return
            
        # mandatory keys
        for key in  ['external_label','media_type', 'library',
                     'eod_cookie', 'capacity_bytes']:
            try:
                record[key] = ticket[key]
            except KeyError, detail:
                msg="Volume Clerk: key %s is missing" % (detail,)
                ticket["status"] = (e_errors.KEYERROR, msg)
                Trace.log(e_errors.ERROR,msg)
                self.reply_to_caller(ticket)
                return
        # set remaining bytes
        record['remaining_bytes'] = record['capacity_bytes']
        # check if library key is valid library manager name
        llm = self.csc.get_library_managers(ticket)

        # "shelf" library is a special case
        if ticket['library']!='shelf' and not llm.has_key(ticket['library']):
            Trace.log(e_errors.INFO,
                      " vc.addvol: Library Manager does not exist: %s " 
                      % (ticket['library'],))

        # form a volume family
        try:
            record['volume_family'] = volume_family.make_volume_family(ticket['storage_group'],
                                                                       ticket['file_family'],
                                                                       ticket['wrapper'])
        except KeyError, detail:
            msg="Volume Clerk: key %s is missing" % (detail,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR,msg)
            self.reply_to_caller(ticket)
            return
        # optional keys - use default values if not specified
        record['last_access'] = ticket.get('last_access', -1)
        record['first_access'] = ticket.get('first_access', -1)
        record['declared'] = ticket.get('declared',-1)
        if record['declared'] == -1:
            record["declared"] = time.time()
        record['system_inhibit'] = ticket.get('system_inhibit', ["none", "none"])
        record['user_inhibit'] = ticket.get('user_inhibit', ["none", "none"])
        record['sum_wr_err'] = ticket.get('sum_wr_err', 0)
        record['sum_rd_err'] = ticket.get('sum_rd_err', 0)
        record['sum_wr_access'] = ticket.get('sum_wr_access', 0)
        record['sum_rd_access'] = ticket.get('sum_rd_access', 0)
        record['non_del_files'] = ticket.get('non_del_files', 0)
        record['blocksize'] = ticket.get('blocksize', -1)
        if record['blocksize'] == -1:
            sizes = self.csc.get("blocksizes")
            try:
                msize = sizes[ticket['media_type']]
            except:
                msg= "Volume Clerk:  unknown media type = unknown blocksize"
                ticket['status'] = (e_errors.UNKNOWNMEDIA,msg)
                Trace.log(e_errors.ERROR, msg)
                self.reply_to_caller(ticket)
                return
            record['blocksize'] = msize

        # write the ticket out to the database
        self.dict[external_label] = record
        if inc_counter: self.sgdb.inc_sg_counter(library, sg)
        # initialize the bfid database for this volume
        self.bfid_db.init_dbfile(external_label)
        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        return

    # modify:
    def modifyvol(self, ticket):
        # create empty record and control what goes into database
        # do not pass ticket, for example to the database!
        record={}
        try:
            external_label = ticket["external_label"]
        except KeyError, detail:
            msg= "Volume Clerk: key %s key is missing" % (detail,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        # make sure it exists
        if not self.dict.has_key(external_label):
            msg="Volume Clerk: volume %s does not exist" % (external_label,)
            ticket["status"] = (errno.errorcode[errno.EEXIST], msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        record = self.dict[external_label]
        
        for key in record.keys():
            if ticket.has_key(key):
                record[key]=ticket[key]

        sizes = self.csc.get("blocksizes")
        try:
            msize = sizes[record['media_type']]
        except:
            msg= "Volume Clerk:  unknown media type = unknown blocksize"
            ticket['status'] = (e_errors.UNKNOWNMEDIA,msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return
        record['blocksize'] = msize
        if record['media_type']=='null':
            record['wrapper']='null'
        # write the ticket out to the database
        self.dict[external_label] = record
        Trace.log(e_errors.INFO, "volume has been modifyed %s" % (record,))
        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        return

    # delete a volume entry from the database
    # This is meant to be used only by trained professional ...
    # It removes the volume entry in the database ...
    # However, it does NOT coordinate with file_clerk nor pnfs ...
    # Its purpose is simply to clean up some portion of the database ...
    # Once an entry is removed, it is gone forever!

    def rmvolent(self, ticket):
        try:
            external_label = ticket["external_label"]
        except KeyError, detail:
            msg= "Volume Clerk: key %s is missing" % (detail,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return
        
        # get the current entry for the volume
        try:
            record = self.dict[external_label]
        except KeyError, detail:
            msg="Volume Clerk: no such volume %s" % (detail,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        del self.dict[external_label]
        #clear the bfid database file too
        self.bfid_db.delete_all_bfids(external_label)
        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        return

    # delete a volume from the database
    def delvol(self, ticket):
        try:
            external_label = ticket["external_label"]
        except KeyError, detail:
            msg= "Volume Clerk: key %s is missing" % (detail,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return
        
        # get the current entry for the volume
        try:
            record = self.dict[external_label]
        except KeyError, detail:
            msg="Volume Clerk: no such volume %s" % (detail,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        force = ticket.get('force',0)
        # get the volume state as seen by media changer
        ret = self.get_media_changer_state(record["library"],
                                            record["external_label"],
                                            record["media_type"])
        # the following code is robot type dependent!!!!!
        if not force and ret != 'unmounted' and ret != '' and ret != 'E':
           ticket["status"] = (e_errors.CONFLICT,"volume state must be unmounted or '' or 'E'. state %s" %
                               (ret,))
           self.reply_to_caller(ticket)
           return
        if record.has_key('non_del_files'):
            if record['non_del_files']>0:
                msg= "Volume Clerk: volume %s has %s active files"%(
                    external_label,record['non_del_files'])
                ticket["status"] = (e_errors.CONFLICT,msg)
                Trace.log(e_errors.INFO, msg)
                self.reply_to_caller(ticket)
                return
        else:
            Trace.log(e_errors.ERROR,"non_del_files not found in volume ticket - old version of table")

        if record['system_inhibit'][0] == e_errors.RECYCLE:
            # volume has been deleted but still can be recovered
            # to delete this volume it must be destroyed by admin.
            ticket['status'] = (e_errors.RECYCLE, "volume must be deleted by administrator")
            self.reply_to_caller(ticket)
            return
        if record['system_inhibit'][0] == e_errors.DELETED:
            # volume is already deleted
            ticket['status'] = (e_errors.DELETED, "volume is deleted")
            self.reply_to_caller(ticket)
            return
        # if volume has not been written delete it
        if record['sum_wr_access'] == 0:
            # see what is the current counter
            library = record['library']
            sg = volume_family.extract_storage_group(record['volume_family'])
            if sg != 'none' and self.quota_enabled(library, sg):
                vol_count = self.sgdb.get_sg_counter(library, sg) - 1
                Trace.trace(21, "delvol: volume_counter %s" % (vol_count,))
                if vol_count >= 0: self.sgdb.inc_sg_counter(library, sg, increment=-1)
                if vol_count == 0: self.sgdb.delete_sg_counter(library, sg)
            del self.dict[external_label]
            #clear the bfid database file too
            self.bfid_db.delete_all_bfids(external_label)
            ticket["status"] = (e_errors.OK, None)
        else:
            record["system_inhibit"][0] = e_errors.DELETED
            cur_rec = self.dict[external_label]
            self.dict[external_label] = record
            # try to remove deleted volume and mark the current one as deleted
            if self.dict.has_key(external_label+".deleted"):
                # remove deleted volume
                status = self.remove_deleted_volume(external_label+".deleted")
                #if status[0] == e_errors.OK:
            # rename current volume
            status = self.rename_volume(external_label, 
                                        external_label+".deleted")

            ticket["status"] = status
            if status[0] == e_errors.OK:
                # return volume to its pool
             
                new_label = string.replace(cur_rec['external_label'], '.deleted', '')
                if not self.dict.has_key(new_label):
                    cur_rec['external_label'] = new_label
                    # form a volume family
                    sg = volume_family.extract_storage_group(cur_rec['volume_family'])
                    cur_rec['volume_family'] = volume_family.make_volume_family(sg,'none', 'none')
                    cur_rec['remaining_bytes'] = cur_rec['capacity_bytes']
                    cur_rec['eod_cookie'] = '0000_000000000_0000001'
                    cur_rec['last_access'] = -1
                    cur_rec['first_access'] = -1
                    #cur_rec['declared'] = time.time()
                    cur_rec['system_inhibit'] = [e_errors.RECYCLE, "none"]
                    cur_rec['user_inhibit'] = ["none", "none"]
                    cur_rec['sum_wr_err'] = 0
                    cur_rec['sum_rd_err'] = 0
                    cur_rec['sum_wr_access'] = 0
                    cur_rec['sum_rd_access'] = 0
                    cur_rec['non_del_files'] = 0
                    # write the new record out to the database
                    self.dict[new_label] = cur_rec
                    # initialize the bfid database for this volume
                    self.bfid_db.init_dbfile(new_label)
            
                    Trace.log(e_errors.INFO,"Volume %s is deleted"%(external_label,))

        self.reply_to_caller(ticket)
        return

    # restore a volume
    def restorevol(self, ticket):
        try:
            external_label = ticket["external_label"]
            restore_vm = ticket["restore"]
        except KeyError, detail:
            msg= "Volume Clerk: %s key is missing"%(detail,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        # get the current entry for the volume
        cl = external_label+".deleted"
        try:
            record = self.dict[cl]
        except KeyError, detail:
            msg="Volume Clerk: no such volume %s"%(detail,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        status = self.rename_volume(cl, external_label, restore_vm)
        ticket["status"] = status
        if status[0] == e_errors.OK:
            record = self.dict[external_label]
            bfid_list = self.bfid_db.get_all_bfids(external_label)
            record["system_inhibit"] = ["none","none"]
            if restore_vm == "yes":
                record["non_del_files"] = len(bfid_list)
            self.dict[external_label] = record
            Trace.log(e_errors.INFO,"Volume %s is restored"%(external_label,))

        self.reply_to_caller(ticket)
        return

    # Check if volume is available
    def is_vol_available(self, ticket):
        work = ticket["action"]
        label = ticket["external_label"]
        # get the current entry for the volume
        try:
            record = self.dict[label]  
        except KeyError, detail:
            msg="Volume Clerk: no such volume %s" % (detail,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return
        if self.lm_is_paused(record['library']):
            ticket['status'] = (e_errors.BROKEN,'Too many volumes set to NOACCESS')
            self.reply_to_caller(ticket)
            return
            
                             
        ret_stat = (e_errors.OK,None)
        Trace.trace(35, "is_vol_available system_inhibit = %s user_inhibit = %s ticket = %s" %
                    (record['system_inhibit'],
                     record['user_inhibit'],
                     ticket))
        if record["system_inhibit"][0] == e_errors.DELETED:
            ret_stat = (record["system_inhibit"][0],None)
        else:
            if work == 'read_from_hsm':
                Trace.trace(35, "is_vol_available: reading")
                # if system_inhibit is NOT in one of the following 
                # states it is NOT available for reading
                if record['system_inhibit'][0] != 'none':
                    ret_stat = (record['system_inhibit'][0], None)
                elif (record['system_inhibit'][1] != 'none' and
                      record['system_inhibit'][1] != 'readonly' and
                      record['system_inhibit'][1] != 'full'):
                    ret_stat = (record['system_inhibit'][1], None)
                # if user_inhibit is NOT in one of the following 
                # states it is NOT available for reading
                elif record['user_inhibit'][0] != 'none':
                    ret_stat = (record['user_inhibit'][0], None)
                elif (record['user_inhibit'][1] != 'none' and
                      record['user_inhibit'][1] != 'readonly' and
                      record['user_inhibit'][1] != 'full'):
                    ret_stat = (record['user_inhibit'][1], None)
                else:
                    ret_stat = (e_errors.OK,None)
            elif work == 'write_to_hsm':
                Trace.trace(35, "is_vol_available: writing")
                if record['system_inhibit'][0] != 'none':
                    ret_stat = (record['system_inhibit'][0], None)
                elif (record['system_inhibit'][1] == 'readonly' or
                      record['system_inhibit'][1] == 'full'):
                    ret_stat = (record['system_inhibit'][1], None)
                elif record['user_inhibit'][0] != 'none':
                    ret_stat = (record['user_inhibit'], None)
                elif (record['user_inhibit'][1] == 'readonly' or
                      record['user_inhibit'][1] == 'full'):
                    ret_stat = (record['user_inhibit'][1], None)
                else:
                    ff = volume_family.extract_file_family(ticket['volume_family'])
                    Trace.trace(35, "is_vol_available: ticket %s, record %s" %
                                (ticket['volume_family'],record['volume_family']))

                    #XXX deal with 2-tuple vs 3-tuple...
                    if (hack_match(ticket['volume_family'],record['volume_family']) or
                        ff == 'ephemeral'):
                        ret = self.is_volume_full(record,ticket['file_size'])
                        if not ret:
                            ret_stat = (e_errors.OK,None)
                        else:
                            ret_stat = (ret, None)
                    else: ret_stat = (e_errors.NOACCESS,None)
            else:
                ret_stat = (e_errors.UNKNOWN,None)
        ticket['status'] = ret_stat
        Trace.trace(35, "is_volume_available: returning %s " %(ret_stat,))
        self.reply_to_caller(ticket)

    # find volume that matches given volume family
    def find_matching_volume(self, library, vol_fam, pool,
                             wrapper, vol_veto_list, first_found,
                             min_remaining_bytes, exact_match=1):

        # go through the volumes and find one we can use for this request
        vol = {}

        Trace.trace(20,  "volume family %s pool %s wrapper %s veto %s exact %s" %
                    (vol_fam, pool,wrapper, vol_veto_list, exact_match))

        lc = self.dict.inx['library'].cursor()          # read only
        vc = self.dict.inx['volume_family'].cursor()
        label, v = lc.set(library)
        label, v = vc.set(pool)
        c = db.join(self.dict, [lc, vc])
        while 1:
            label,v = c.next()
            Trace.trace(30, "label,v = %s, %s" % (label, v))
            if not label:
                break
            if v["user_inhibit"] != ["none",  "none"]:
                Trace.trace(30, "user inhibit = %s" % (v['user_inhibit'],))
                continue
            if v["system_inhibit"] != ["none", "none"]:
                Trace.trace(30, "system inhibit = %s" % (v['system_inhibit'],))
                continue

            # equal treatment for blank volume
            if exact_match:
                if self.is_volume_full(v,min_remaining_bytes):
                    Trace.trace(30, "full")
                    continue
            else:
                if v["remaining_bytes"] < long(min_remaining_bytes*SAFETY_FACTOR):
                    Trace.trace(30, "almost full")
                    continue
                
            vetoed = 0
            for veto in vol_veto_list:
                if label == veto:
                    Trace.trace(30, "vetoed")
                    vetoed = 1
                    break
            if vetoed:
                continue

            # supposed to return first volume found?
            # do not return blank volume at this point yet
            if first_found:
                Trace.trace(30,"first found")
                v["status"] = (e_errors.OK, None)
                c.close()
                return v
            # if not, is there an "earlier" volume that we have already found?
            if len(vol) == 0:
                Trace.trace(30,"vol %s"%(v,))
                vol = v
            elif v['declared'] < vol['declared']:
                vol = v  
        c.close()
        return vol
        
    
    # check if quota is enabled in the configuration
    def quota_enabled(self, library, storage_group):
        q_dict = self.csc.get('quotas')
        if q_dict['status'][0] == e_errors.KEYERROR:
            # no quota defined in the configuration
            return None
        enabled = q_dict.get('enabled',None)
        if not enabled:
            # enabled key does not exist. Wrong cofig.
            return None
        if 'y' not in string.lower(enabled):
            # do not use quota
            return None
        else:
            return q_dict
        
    # check quota
    def check_quota(self, quotas, library, storage_group):
        if not quotas.has_key('libraries'):
            Trace.log(e_errors.ERROR, "Wrong quota config")
            return 0
            
        if quotas['libraries'].has_key(library):
            vol_count = self.sgdb.get_sg_counter(library, storage_group)
            quota = quotas['libraries'][library].get(storage_group, 0)
            Trace.trace(21, "storage group %s, vol counter %s, quota %s" % (storage_group, vol_count, quota)) 
            if quota == 0 or (vol_count >= quota):
                return 0
            else: return 1
        else:
            Trace.log(e_errors.ERROR, "no library %s defined in the quota configuration" % (library))
            return 0
        return 0
            
    
    # Get the next volume that satisfy criteria
    def next_write_volume (self, ticket):
        Trace.trace(20, "next_write_volume %s" % (ticket,))
            
        vol_veto = ticket["vol_veto_list"]
        vol_veto_list = self.r_eval(vol_veto)

        # get the criteria for the volume from the user's ticket
        min_remaining_bytes = ticket["min_remaining_bytes"]
        library = ticket["library"]
        if self.lm_is_paused(library):
            ticket['status'] = (e_errors.BROKEN,'Too many volumes set to NOACCESS')
            self.reply_to_caller(ticket)
            return
        
        vol_fam = ticket['volume_family']
        first_found = ticket["first_found"]
        wrapper_type = volume_family.extract_wrapper(vol_fam)
        use_exact_match = ticket['use_exact_match']

        # go through the volumes and find one we can use for this request
        # first use exact match
        sg = volume_family.extract_storage_group(vol_fam)
        ff = volume_family.extract_file_family(vol_fam)
        Trace.trace(20, "next_write_volume %s %s" % (vol_fam, vol_fam))

        pool = vol_fam
        vol = self.find_matching_volume(library, vol_fam, pool,
                                        wrapper_type, vol_veto_list,
                                        first_found, min_remaining_bytes,exact_match=1)
        Trace.trace(20, "find matching volume returned %s" % (vol,))

        if use_exact_match:
            if not vol or len(vol) == 0:
                # nothing was available at all
                msg="Volume Clerk: no new volumes available [%s, %s]"%(library,
								       vol_fam)
                ticket["status"] = (e_errors.NOVOLUME, msg)
                Trace.alarm(e_errors.ERROR,msg)
                self.reply_to_caller(ticket)
                return

        if not vol or len(vol) == 0:
            # nothing was available - see if we can assign a blank from a
            # given storage group and file family.
            pool = volume_family.make_volume_family(sg, ff, 'none')
        
            Trace.trace(20, "next_write_volume %s %s" % (vol_fam, pool))
            vol = self.find_matching_volume(library, vol_fam, pool, wrapper_type,
                                            vol_veto_list, first_found,
                                            min_remaining_bytes,exact_match=0)
        
            Trace.trace(20, "find matching volume returned %s" % (vol,))

        if not vol or len(vol) == 0:
            # nothing was available - see if we can assign a blank from a
            # given storage group
            pool = volume_family.make_volume_family(sg, 'none', 'none')
        
            Trace.trace(20, "next_write_volume %s %s" % (vol_fam, pool))
            vol = self.find_matching_volume(library, vol_fam, pool, wrapper_type,
                                            vol_veto_list, first_found,
                                            min_remaining_bytes,exact_match=0)
        
            Trace.trace(20, "find matching volume returned %s" % (vol,))

        inc_count = 0
        if not vol or len(vol) == 0:
            # nothing was available - see if we can assign a blank from a
            # common pool
            pool = 'none.none.none'
            Trace.trace(20, "next_write_volume %s %s" % (vol_fam, pool))
            vol = self.find_matching_volume(library, vol_fam, pool, wrapper_type,
                                            vol_veto_list, first_found,
                                            min_remaining_bytes, exact_match=0)
            Trace.trace(20, "find matching volume returned %s" % (vol,))

            if vol and len(vol) != 0:
                # check if quota is enabled
                q_dict = self.quota_enabled(library, sg)
                if q_dict:
                    if self.check_quota(q_dict, library, sg):
                        inc_counter = 1
                    else:
                        msg="Volume Clerk: Quota exceeded, contact enstore admin."
                        ticket["status"] = (e_errors.QUOTAEXCEEDED, msg)
                        Trace.alarm(e_errors.ERROR,msg)
                        self.reply_to_caller(ticket)
                        return

        # return blank volume we found
        if vol and len(vol) != 0:
            label = vol['external_label']
            if ff == 'ephemeral':
                vol_fam = volume_family.make_volume_family(sg, label, wrapper_type)
            vol['volume_family'] = vol_fam
            vol['wrapper'] = wrapper_type
            if vol['sum_wr_access'] != 0:
                msg = ''
            else:
                msg = 'blank'
            Trace.log(e_errors.INFO, "Assigning %s volume %s from storage group %s to library %s, volume family %s"
                      % (msg, label, pool, library, vol_fam))
            if inc_count: self.sgdb.inc_sg_counter(library, sg)

            self.dict[label] = vol  
            vol["status"] = (e_errors.OK, None)
            self.reply_to_caller(vol)
            return

        # nothing was available at all
        msg="Volume Clerk: no new volumes available [%s, %s]"%(library,
							       vol_fam)
        ticket["status"] = (e_errors.NOVOLUME, msg)
        Trace.alarm(e_errors.ERROR,msg)
        self.reply_to_caller(ticket)
        return


    # check if specific volume can be used for write
    def can_write_volume (self, ticket):
     # get the criteria for the volume from the user's ticket
     try:
         min_remaining_bytes = ticket["min_remaining_bytes"]
         library = ticket["library"]
         vol_fam = ticket["volume_family"]
         external_label = ticket["external_label"]
     except KeyError, detail:
         msg="Volume Clerk: key %s is missing"%(detail,)
         ticket["status"] = (e_errors.KEYERROR, msg)
         Trace.log(e_errors.ERROR, msg)
         self.reply_to_caller(ticket)
         return

     # get the current entry for the volume
     try:
         v = self.dict[external_label]  

         ticket["status"] = (e_errors.OK,'None')
         if (v["library"] == library and
             (v["volume_family"] == vol_fam) and
             v["user_inhibit"][0] == "none" and
             v["user_inhibit"][1] == "none" and
             v["system_inhibit"][0] == "none" and
             v["system_inhibit"][1] == "none"):
             ##
             ##ret_st = self.is_volume_full(v,min_remaining_bytes)
             ##if ret_st:
             ##    ticket["status"] = (ret_st,None)
             ##

             if v["remaining_bytes"] < long(min_remaining_bytes*SAFETY_FACTOR):
                 ticket["status"] = (e_errors.WRITE_EOT, "file too big")
             self.reply_to_caller(ticket)
             return
         else:
             ticket["status"] = (e_errors.NOACCESS, 'None')
             self.reply_to_caller(ticket)
             return
     except KeyError, detail:
         msg="Volume Clerk: no such volume %s" % (detail,)
         ticket["status"] = (e_errors.KEYERROR, msg)
         Trace.log(e_errors.ERROR, msg)
         self.reply_to_caller(ticket)
         return


    # update the database entry for this volume
    def get_remaining_bytes(self, ticket):
        ticket['status'] = (e_errors.OK, None)
        try:
            external_label = ticket["external_label"]
        except KeyError, detail:
            msg="Volume Clerk: key %s is missing" % (detail,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        # get the current entry for the volume
        try:
            record = self.dict[external_label]  
        except KeyError, detail:
            msg="Volume Clerk: no such volume %s" % (detail,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return


        # access the remaining_byte field
        try:
            ticket["remaining_bytes"] = record["remaining_bytes"]
        except KeyError, detail:
            msg="Volume Clerk: key %s is missing" % (detail,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            
        self.reply_to_caller(ticket)
        return

    ##This should really be renamed, it does more than set_remaining_bytes
    # update the database entry for this volume
    def set_remaining_bytes(self, ticket):
        try:
            external_label = ticket["external_label"]
        except KeyError, detail:
            msg= "Volume Clerk: key %s is missing"%(detail,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        # get the current entry for the volume
        try:
            record = self.dict[external_label]  
        except KeyError, detail:
            msg="Volume Clerk: no such volume %s"%(detail,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        # update the fields that have changed
        try:
            for key in ["remaining_bytes","eod_cookie"]:
                record[key] = ticket[key]

        except KeyError, detail:
            msg="Volume Clerk: key %s is missing"%(detail,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        if record["remaining_bytes"] == 0:
            record["system_inhibit"][1] = "full"
        else:
            record["system_inhibit"][0] = "none"
            
        record["last_access"] = time.time()
        if record["first_access"] == -1:
            record["first_access"] = record["last_access"]
            
        non_del_files = record['non_del_files']

        # update the non-deleted file count if we wrote a new file to the tape
        bfid = ticket.get("bfid") #will be present when a new file is added
        record["status"] = (e_errors.OK, None)
        if bfid:
            # exception may occur in the bfid_db
            # it is either generated by bfid_db(BfidDbError)
            # or IOError.
            # This exception is not crucial for the operation of the
            # system, but must be recorded. We set the alarm and log the event
            try:
                self.bfid_db.add_bfid(external_label, bfid)
            except (bfid_db.BfidDbError, IOError), detail:
                Trace.alarm(e_errors.ERROR, str(detail), record)
                # this exception does not cause any major problem, hence
                # returned status is still OK 
            record['non_del_files'] = record['non_del_files'] + 1
            
        # record our changes
        self.dict[external_label] = record  
        self.reply_to_caller(record)
        return


    def add_bfid(self, ticket):
        try:
            external_label = ticket["external_label"]
            bfid = ticket['bfid']
        except KeyError, detail:
            msg= "Volume Clerk: key %s is missing"%(detail,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        status = (e_errors.OK, None)
        try:
            self.bfid_db.add_bfid(external_label, bfid)
        except (bfid_db.BfidDbError, IOError), detail:
            status =  (e_errors.KEYERROR, str(detail))
        self.reply_to_caller({'status':status})
        return
        
    # decrement the file count on the volume
    def decr_file_count(self, ticket):
        try:
            external_label = ticket["external_label"]
        except KeyError, detail:
            msg="Volume Clerk: key %s is missing" % (detail,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        # get the current entry for the volume
        try:
            record = self.dict[external_label]  
        except KeyError, detail:
            msg="Volume Clerk: no such volume %s" % (detail,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        # assume the count is 1 unless specified
        count = ticket.get("count",1)

        # decrement the number of non-deleted files on the tape
        record ["non_del_files"] = record["non_del_files"] - count
        self.dict[external_label] = record   # THIS WILL JOURNAL IT
        record["status"] = (e_errors.OK, None)
        self.reply_to_caller(record)
        return

    # update the database entry for this volume
    def update_counts(self, ticket):
        try:
            external_label = ticket["external_label"]
        except KeyError, detail:
            msg="Volume Clerk: key %s is missing" % (detail,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR,msg)
            self.reply_to_caller(ticket)
            return

        # get the current entry for the volume
        try:
            record = self.dict[external_label]  
        except KeyError, detail:
            msg="Volume Clerk: no such volume %s" % (detail,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        # update the fields that have changed
        record["last_access"] = time.time()
        if record["first_access"] == -1:
            record["first_access"] = record["last_access"]

        for key in ['wr_err','rd_err','wr_access','rd_access']:
            try:
                record['sum_'+key] = record['sum_'+key] + ticket[key]
            except KeyError, detail:
                msg= "Volume Clerk: key %s is missing" % (detail,)
                ticket["status"] = (e_errors.KEYERROR, msg)
                Trace.log(e_errors.ERROR, msg)
                self.reply_to_caller(ticket)
                return

        # record our changes
        self.dict[external_label] = record  
        record["status"] = (e_errors.OK, None)
        self.reply_to_caller(record)
        return

    # get the current database volume about a specific entry
    def inquire_vol(self, ticket):
        try:
            external_label = ticket["external_label"]
        except KeyError, detail:
            msg="Volume Clerk: key %s is missing" % (detail,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        # get the current entry for the volume
        try:
            record = self.dict[external_label]  
            record["status"] = e_errors.OK, None
            self.reply_to_caller(record)
            return
        except KeyError, detail:
            msg="Volume Clerk: no such volume %s" % (detail,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

    # flag the database that we are now writing the system
    def clr_system_inhibit(self, ticket):
        try:
            external_label = ticket["external_label"]
            inhibit = ticket["inhibit"]
            if not inhibit:
                inhibit = "system_inhibit" # set default field 
            position = ticket["position"]
        except KeyError, detail:
            msg="Volume Clerk: key %s is missing" % (detail,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        # get the current entry for the volume
        try:
            record = self.dict[external_label]  
        except KeyError, detail:
            msg="Volume Clerk: no such volume %s" % (detail,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        if (inhibit == "system_inhibit" and position == 0):
            if record [inhibit][position] == e_errors.DELETED:
                # if volume is deleted no data can be changed
                record["status"] = (e_errors.DELETED, 
                                    "Cannot perform action on deleted volume")
            else:    
                # update the fields that have changed
                record[inhibit][position] = "none"
                self.dict[external_label] = record   # THIS WILL JOURNAL IT
                record["status"] = (e_errors.OK, None)
        else:
            # if it is not record["system_inhibit"][0] just set it to none
            record[inhibit][position] = "none"
            self.dict[external_label] = record   # THIS WILL JOURNAL IT
            record["status"] = (e_errors.OK, None)
        if record["status"][0] == e_errors.OK:
            Trace.log(e_errors.INFO, "system inhibit cleared for %s" % (external_label, ))
        self.reply_to_caller(record)
        return

    # get the actual state of the media changer
    def get_media_changer_state(self, lib, volume, m_type):
        m_changer = self.csc.get_media_changer(lib + ".library_manager")
        if not m_changer:
            Trace.log(e_errors.ERROR,
                      " vc.get_media_changer_state: ERROR: no media changer found %s" % (volume,))
            return 'unknown'
            
        import media_changer_client
        mcc = media_changer_client.MediaChangerClient(self.csc, m_changer )
        stat = mcc.viewvol(volume, m_type)["status"][3]
        # the following code is robot type dependant!!!!!
        if stat == 'O':
            state = 'unmounted'
        elif stat == 'M':
            state = 'mounted'
        else :
            state = stat

        return state

    # move a volume to a new library
    def new_library(self, ticket):
        external_label = ticket["external_label"]
        new_library = ticket["new_library"]

        # get the current entry for the volume
        # get the current entry for the volume
        try:
            record = self.dict[external_label]  
        except KeyError, detail:
            msg="Volume Clerk: no such volume %s" % (detail,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return
        
        # update the library field with the new library
        record ["library"] = new_library
        self.dict[external_label] = record   # THIS WILL JOURNAL IT
        record["status"] = (e_errors.OK, None)
        self.reply_to_caller(record)
        return

    # set system_inhibit flag
    def set_system_inhibit(self, ticket, flag, index=0):
        external_label = ticket["external_label"]
        # get the current entry for the volume
        try:
            record = self.dict[external_label]  
        except KeyError, detail:
            msg="Volume Clerk: no such volume %s" % (detail,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return ticket["status"]

        # update the fields that have changed
        if flag is "readonly":
            # check if volume is blank
            if record['non_del_files'] == 0:
                record['status'] = (e_errors.CONFLICT, "volume is blank")
                self.reply_to_caller(record)
                return record["status"]
        record["system_inhibit"][index] = flag

        self.dict[external_label] = record   # THIS WILL JOURNAL IT
        record["status"] = (e_errors.OK, None)
        Trace.log(e_errors.INFO,external_label+" system inhibit set to "+flag)
        self.reply_to_caller(record)
        return record["status"]

    # set system_inhibit flag, flag the database that we are now writing the system
    def set_writing(self, ticket):
        return self.set_system_inhibit(ticket, "writing")

    # set system_inhibit flag to none
    def set_system_none(self, ticket):
        return self.set_system_inhibit(ticket, "none")

    # flag that the current volume is readonly
    def set_system_readonly(self, ticket):
        return self.set_system_inhibit(ticket, "readonly", 1)

    # set pause flag for the all Library Managers corresponding to
    # certain Media Changer
    def pause_lm(self, external_label):
        # get the current entry for the volume
        try:
            record = self.dict[external_label]  
        except KeyError, detail:
            msg="Volume Clerk: no such volume %s" % (detail,)
            Trace.log(e_errors.ERROR, msg)
            return
        # find the media changer for this volume
        m_changer = self.csc.get_media_changer(record['library'] + ".library_manager")
        if m_changer:
            if not self.paused_lms.has_key(m_changer):
                self.paused_lms[m_changer] = {'paused':0,
                                              'noaccess_cnt': 0,
                                              'noaccess_time':time.time(),
                                              }
            now = time.time()
            if self.paused_lms[m_changer]['noaccess_cnt'] == 0:
                self.paused_lms[m_changer]['noaccess_time'] = now
            if now - self.paused_lms[m_changer]['noaccess_time'] <= self.noaccess_to:
                self.paused_lms[m_changer]['noaccess_cnt'] = self.paused_lms[m_changer]['noaccess_cnt'] + 1
            else:
                self.paused_lms[m_changer]['noaccess_cnt'] = 1
            if ((self.paused_lms[m_changer]['noaccess_cnt'] >= self.max_noaccess_cnt) and
                self.paused_lms[m_changer]['paused'] == 0):
                self.paused_lms[m_changer]['paused'] = 1
                Trace.log(e_errors.INFO,'pause library_managers for %s media_changerare paused due to too many volumes set to NOACCESS' % (m_changer,))
                
    # check if Library Manager is paused
    def lm_is_paused(self, library):
        m_changer = self.csc.get_media_changer(library + ".library_manager")
        if m_changer:
            if (self.paused_lms.has_key(m_changer) and
                self.paused_lms[m_changer]['paused'] != 0):
                ret_code = 1
                Trace.log(e_errors.ERROR,'library_managers for %s media_changerare paused due to too many volumes set to NOACCESS' % (m_changer,))
            else:
                ret_code = 0
        else:
            ret_code = 0
        return ret_code
            
            
    # flag that the current volume is marked as noaccess
    def set_system_noaccess(self, ticket):
        Trace.alarm(e_errors.WARNING, e_errors.NOACCESS,{"label":ticket["external_label"]})
        rc = self.set_system_inhibit(ticket, e_errors.NOACCESS)
        if rc[0] == e_errors.OK:
            self.pause_lm(ticket["external_label"])
        return rc

    # flag that the current volume is marked as not allowed
    def set_system_notallowed(self, ticket):
        Trace.alarm(e_errors.WARNING, e_errors.NOTALLOWED,{"label":ticket["external_label"]}) 
        return self.set_system_inhibit(ticket, e_errors.NOTALLOWED)

    # device is broken - what to do, what to do ===================================FIXME======================================
    def set_hung(self,ticket):
        self.reply_to_caller({"status" : (e_errors.OK, None)})
        return

    # return all the volumes in our dictionary.  Not so useful!
    def get_vols(self,ticket):
        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)

        # this could tie things up for awhile - fork and let child
        # send the work list (at time of fork) back to client
        #if self.fork() != 0:
        #    return
        try:
            if not self.get_user_sockets(ticket):
                return
            ticket["status"] = (e_errors.OK, None)
            callback.write_tcp_obj(self.data_socket, ticket)
            # REMOVE ME LATER
            # The following commented out old style cursor usage should
            #   removed once the new style usage is proven stable
            # self.dict.cursor("open")
            # key,value=self.dict.cursor("first")
            c = self.dict.newCursor()
            key,value = c.first()
            msg={}
            while key:
                if ticket.has_key("not"): cond = ticket["not"]
                if ticket.has_key("in_state") and ticket["in_state"] != None:
                    if ticket.has_key("key") and ticket["key"] != None:
                        if value.has_key(ticket["key"]):
                            loc_val = value[ticket["key"]]
                            if mycmp(cond,loc_val,ticket["in_state"]):
                                if msg:
                                    dict = {"volume":key}
                                    msg["volumes"].append(dict)
                                else:
                                    msg["volumes"]= []
                                    dict = {"volume":key}
                                    msg["volumes"].append(dict)
                            else:
                                pass
                    else:
                        if (ticket["in_state"] == "full" or
                            ticket["in_state"] == "readonly"):
                            index = 1
                        else: index = 0
                        if value["system_inhibit"][index] == ticket["in_state"]:
                            if msg:
                                dict = {"volume":key}
                                msg["volumes"].append(dict)
                            else:
                                msg["volumes"]= []
                                dict = {"volume":key}
                                msg["volumes"].append(dict)
                else:
                    dict = {"volume"         : key}
                    for k in ["capacity_bytes","remaining_bytes", "system_inhibit",
                              "user_inhibit", "library", "volume_family", "non_del_files"]:
                        dict[k]=value[k]
                    if msg:
                        msg["volumes"].append(dict)
                    else:
                        msg["header"] = "FULL"
                        msg["volumes"]= []
                        msg["volumes"].append(dict)

                key,value = c.next()
            callback.write_tcp_obj_new(self.data_socket, msg)
            c.close()
            self.data_socket.close()

            callback.write_tcp_obj(self.control_socket, ticket)
            self.control_socket.close()
        except:
            exc, msg, tb = sys.exc_info()
            Trace.handle_error(exc,msg,tb)
        return

    # get a port for the data transfer
    # tell the user I'm your volume clerk and here's your ticket
    def get_user_sockets(self, ticket):
        try:
            addr = ticket['callback_addr']
            if not hostaddr.allow(addr):
                return 0
            volume_clerk_host, volume_clerk_port, listen_socket = callback.get_callback()
            listen_socket.listen(4)
            ticket["volume_clerk_callback_addr"] = (volume_clerk_host, volume_clerk_port)
            self.control_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.control_socket.connect(addr)
            callback.write_tcp_obj(self.control_socket, ticket)

            r,w,x = select.select([listen_socket], [], [], 15)
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
        # catch any error and keep going. server needs to be robust
        except:
            exc, msg, tb = sys.exc_info()
            Trace.handle_error(exc,msg,tb)
        return 1
    
    def start_backup(self,ticket):
        try:
            self.dict.start_backup()
            self.reply_to_caller({"status"        : (e_errors.OK, None),
                                  "start_backup"  : 'yes' })
        # catch any error and keep going. server needs to be robust
        except:
            exc, msg, tb = sys.exc_info()
            Trace.handle_error(exc,msg,tb)
            status = str(exc), str(msg)
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
            exc,msg,tb=sys.exc_info()
            Trace.handle_error(exc,msg,tb)
            status = str(exc), str(msg)
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
            Trace.handle_error(exc,msg,tb)
            status = str(exc), str(msg)
            self.reply_to_caller({"status"       : status,
                                  "backup"  : 'no' })

    def clear_lm_pause(self, ticket):
        m_changer = self.csc.get_media_changer(ticket['library'] + ".library_manager")
        if m_changer:
            if self.paused_lms.has_key(m_changer):
                Trace.log(e_errors.INFO, "Cleared BROKEN flag for all LMs related to media changer %s" % (m_changer,))
                self.paused_lms[m_changer] = {'paused':0,
                                              'noaccess_cnt': 0,
                                              'noaccess_time':time.time(),
                                              }
        self.max_noaccess_cnt = self.keys.get('max_noaccess_cnt', 2)
        self.noaccess_to = self.keys.get('noaccess_to', 300.)
        self.reply_to_caller({"status" : (e_errors.OK, None)})
         

class VolumeClerk(VolumeClerkMethods, generic_server.GenericServer):
    def __init__(self, csc):
        generic_server.GenericServer.__init__(self, csc, MY_NAME)
        Trace.init(self.log_name)
        self.keys = self.csc.get(MY_NAME)
	self.alive_interval = monitored_server.get_alive_interval(self.csc,
								  MY_NAME,
								  self.keys)

        dispatching_worker.DispatchingWorker.__init__(self, (self.keys['hostip'],
                                                             self.keys['port']))

        Trace.log(e_errors.INFO,"determine dbHome and jouHome")
        try:
            dbInfo = configuration_client.ConfigurationClient(csc).get('database')
            dbHome = dbInfo['db_dir']
            try:  # backward compatible
                jouHome = dbInfo['jou_dir']
            except:
                jouHome = dbHome
        except:
            dbHome = os.environ['ENSTORE_DIR']
            jouHome = dbHome

        Trace.log(e_errors.INFO,"opening volume database using DbTable")
        self.dict = db.DbTable("volume", dbHome, jouHome, ['library', 'volume_family'])
        Trace.log(e_errors.INFO,"hurrah, volume database is open")
        self.bfid_db=bfid_db.BfidDb(dbHome)
        # self.bfid_db=newBfidDB(dbHome)
        self.sgdb = sg_db.SGDb(dbHome)

        self.noaccess_cnt = 0
        self.max_noaccess_cnt = self.keys.get('max_noaccess_cnt', 2)
        self.noaccess_to = self.keys.get('noaccess_to', 300.)
        self.paused_lms = {}
        self.noaccess_time = time.time()
        
	# start our heartbeat to the event relay process
	self.erc.start_heartbeat(enstore_constants.VOLUME_CLERK, 
				 self.alive_interval)


class VolumeClerkInterface(generic_server.GenericServerInterface):
        pass

if __name__ == "__main__":
    Trace.init(string.upper(MY_NAME))

    # get the interface
    intf = VolumeClerkInterface()
    vc = VolumeClerk((intf.config_host, intf.config_port))
    vc.handle_generic_commands(intf)
    
    Trace.log(e_errors.INFO, '%s' % (sys.argv,))

    while 1:
        try:
            Trace.log(e_errors.INFO,'Volume Clerk (re)starting')
            vc.serve_forever()
        except SystemExit, exit_code:
            vc.dict.close()
            sys.exit(exit_code)
        except:
            vc.serve_forever_error(vc.log_name)
            continue
    Trace.log(e_errors.ERROR,"Volume Clerk finished (impossible)")
