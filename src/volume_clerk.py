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
import volume_family
import sgdb
import enstore_constants
import monitored_server
import file_clerk_client
import inquisitor_client
import cPickle
import event_relay_client
import event_relay_messages

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

# make pychecker happy

if GB:
    pass

SAFETY_FACTOR=1.05
#MIN_LEFT=long(300*MB)
MIN_LEFT=long(0) # for now, this is disabled.


MY_NAME = "volume_clerk"

class VolumeClerkMethods(dispatching_worker.DispatchingWorker, generic_server.GenericServer):

    def __init__(self, csc):
        # basically, to make pychecker happy
        generic_server.GenericServer.__init__(self, csc, MY_NAME, self.handle_er_msg)
        self.keys = self.csc.get(MY_NAME)
        dispatching_worker.DispatchingWorker.__init__(self, (self.keys['hostip'], self.keys['port']))
        self.dict = None
        self.bfid_db = None
        self.sgdb = None
        self.paused_lms = {}
        self.ignored_sg_file = None
        return

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

    # __rename_volume(old, new): rename a volume from old to new
    #
    # renaming a volume involves:
    # [1] renaming the records of the files in it, done by file clerk
    #     [a] in each file record, 'external_label' and 'pnfs_mapname'
    #         are changed according
    # [2] physically renaming the volmap path in /pnfs, done by file clerk
    # [3] renaming volume record by changing its 'external_label'
    #
    # after renaming, the original volume does not exist any more

    def __rename_volume(self, old, new):
         try:
             record = self.dict[old]
         except:
             return 'EACCESS', "volume %s does not exist"%(old)

         if self.dict.has_key(new):
             return 'EEXIST', "volume %s already exists"%(new)

         fcc = file_clerk_client.FileClient(self.csc)

         # have file clerk to rename the volume information for the files

         r = fcc.rename_volume(old, new)
         if r['status'][0] == e_errors.OK:
             # modify the volume record
             # should we update other infromation?
             record['external_label'] = new
             self.dict[new] = record
             del self.dict[old]
         else:
             Trace.log(e_errors.ERROR, "failed to rename %s to %s"%(old, new))
             return r['status']

         Trace.log(e_errors.INFO, "volume renamed %s->%s"%(old, new))
         return e_errors.OK, None

    # rename_volume() -- server version of __rename_volume()

    def rename_volume(self, ticket):
        try:
            old = ticket['old']
            new = ticket['new']
        except KeyError, detail:
            msg =  "Volume Clerk: key %s is missing"  % (detail)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        # This is a restricted service
        status = self.restricted_access()
        if status:
            msg = "attempt to rename volume %s to %s from %s"%(old, new, self.reply_address[0])
            Trace.log(e_errors.ERROR, msg)
            ticket['status'] = status
            self.reply_to_caller(ticket)
            return

        ticket['status'] = self.__rename_volume(old, new)
        self.reply_to_caller(ticket)
        return

    # __erase_volume(vol) -- erase vol forever
    # This one is very dangerous
    #
    # erasing a volume wipe out the meta information about it as if
    # it never exists.
    #
    # * only deleted volume can be erased.
    #
    # erasing a volume involves:
    # [1] erasing all file records associated with this volume -- done
    #     by file clerk
    # [2] erasing volume record

    def __erase_volume(self, vol):

        # only allow deleted volume to be erased
        if vol[-8:] != '.deleted':
            error_msg = 'trying to erase a undeleted volume %s'%(vol)
            Trace.log(e_errors.ERROR, error_msg)
            return e_errors.ERROR, error_msg

        fcc = file_clerk_client.FileClient(self.csc)

        # erase file record
        status = fcc.erase_volume(vol)['status']
        del fcc
        if status[0] != e_errors.OK:
            Trace.log(e_errors.ERROR, 'erasing volume "%s" failed'%(vol))
            return status
        # erase volume record
        del self.dict[vol]
        Trace.log(e_errors.INFO, 'volume "%s" has been erased'%(vol))
        return e_errors.OK, None

    # erase_volume(vol) -- server version of __erase_volume()

    def erase_volume(self, ticket):
        try:
            vol = ticket['external_label']
        except KeyError, detail:
            msg =  "Volume Clerk: key %s is missing"  % (detail)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        # This is a restricted service
        status = self.restricted_access()
        if status:
            msg = "attempt to erase volume %s from %s"%(vol, self.reply_address[0])
            Trace.log(e_errors.ERROR, msg)
            ticket['status'] = status
            self.reply_to_caller(ticket)
            return

        ticket['status'] = self.__erase_volume(vol)
        self.reply_to_caller(ticket)
        return

    # has_undeleted_file(vol) -- check if vol has undeleted file
    # this is served by file_clerk

    def has_undeleted_file(self, vol):
        fcc = file_clerk_client.FileClient(self.csc)
        r = fcc.has_undeleted_file(vol)
        del fcc
        if r['status'][1]:
            return 1
        return 0

    # __delete_volume(vol) -- delete a volume
    #
    # * only a volume that contains no active files can be deleted
    #
    # deleting a volume, vol, is simply renaming it to vol.deleted
    #
    # if recycle flag is set, vol will be redeclared as a new volume

    def __delete_volume(self, vol, recycle = 0):
        # check existence of the volume
        try:
            record = self.dict[vol]
        except KeyError, detail:
            msg = "Volume Clerk: no such volume %s" % (detail)
            Trace.log(e_errors.ERROR, msg)
            return e_errors.ERROR, msg

        # check if it has been deleted
        if vol[-8:] == '.deleted' or record['external_label'][-8:] == '.deleted':
            return e_errors.OK, 'volume %s has been deleted already'%(vol)

        # check if all files are deleted
        if self.has_undeleted_file(vol):
            msg = 'can not delete non-empty volume %s'%(vol)
            Trace.log(e_errors.ERROR, msg)
            return e_errors.ERROR, msg

        # check its state
        ret = self.get_media_changer_state(record["library"],
                                           record["external_label"],
                                           record["media_type"])

        if ret != 'unmounted' and ret != 'no_mc' and ret != '' and ret != 'E' and ret != 'U':
            return e_errors.CONFLICT,"volume state must be unmounted or '' or 'E' or 'U'. state %s"%(ret)

        # delete the volume
        # check if <vol>.deleted exists, if so, erase it.

        if self.dict.has_key(vol+'.deleted'):
            # erase it
            status = self.__erase_volume(vol+'.deleted')
            if status[0] != e_errors.OK:
                return status

        # check if it is never written, if so, erase it
        if record['sum_wr_access']:
	    status = self.__rename_volume(vol, vol+'.deleted')
            if status[0] == e_errors.OK:
                record = self.dict[vol+'.deleted']
                record['system_inhibit'][0] = e_errors.DELETED
                self.dict[vol+'.deleted'] = record
                Trace.log(e_errors.INFO, 'volume "%s" has been deleted'%(vol))
            else: # don't do anything further
                return status
        else:    # never written
            del self.dict[vol]
            status = e_errors.OK, None
            Trace.log(e_errors.INFO, 'Empty volume "%s" has been deleted'%(vol))

        # recycling it?

        if recycle:
            record['external_label'] = vol
            record['remaining_bytes'] = record['capacity_bytes']
            record['declared'] = time.time()
            record['eod_cookie'] = '0000_000000000_0000001'
            record['last_access'] = -1
            record['first_access'] = -1
            record['system_inhibit'] = ["none", "none"]
            record['user_inhibit'] = ["none", "none"]
            record['sum_rd_access'] = 0
            record['sum_wr_access'] = 0
            record['sum_wr_err'] = 0
            record['sum_rd_err'] = 0
            record['non_del_files'] = 0
            # reseting volume family
            sg = string.split(record['volume_family'], '.')[0]
            record['volume_family'] = sg+'.none.none'
            # check for obsolete fields
            for ek in ['at_mover', 'file_family', 'status']:
                if record.has_key(ek):
                    del record[ek]
            self.dict[vol] = record
            Trace.log(e_errors.INFO, 'volume "%s" has been recycled'%(vol))
        else:

            # get storage group and take care of quota

            library = record['library']
            sg = volume_family.extract_storage_group(record['volume_family'])
            self.sgdb.inc_sg_counter(library, sg, increment=-1)

        return status

    # delete_volume(vol) -- server version of __delete_volume()

    def delete_volume(self, ticket):

        try:
            vol = ticket['external_label']
        except KeyError, detail:
            msg =  "Volume Clerk: key %s is missing"  % (detail)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        # This is a restricted service
        status = self.restricted_access()
        if status:
            msg = "attempt to delete volume %s from %s"%(vol, self.reply_address[0])
            Trace.log(e_errors.ERROR, msg)
            ticket['status'] = status
            self.reply_to_caller(ticket)
            return

        ticket['status'] = self.__delete_volume(vol)
        self.reply_to_caller(ticket)
        return

    # recycle_volume(vol) -- server version of __delete_volume(vol, 1)

    def recycle_volume(self, ticket):

        try:
            vol = ticket['external_label']
        except KeyError, detail:
            msg =  "Volume Clerk: key %s is missing"  % (detail)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        # This is a restricted service
        status = self.restricted_access()
        if status:
            msg = "attempt to recycle volume %s from %s"%(vol, self.reply_address[0])
            Trace.log(e_errors.ERROR, msg)
            ticket['status'] = status
            self.reply_to_caller(ticket)
            return

        ticket['status'] = self.__delete_volume(vol, 1)
        self.reply_to_caller(ticket)
        return

    # __restore_volume(vol) -- restore a deleted volume
    #
    # Only a deleted volume can be restored, i.e., vol must be of the
    # form <vol>.deleted
    #
    # if <vol> exists:
    #     if <vol> has not been written:
    #         erase <vol>
    #     if <vol> has benn written:
    #         signal this as an error
    #
    # a volume is restored to the state when it was deleted, i.e.,
    # containing only deleted files.
    #
    # restoring a volume is, if all critera are satisfied, simply
    # renaming a volume from <vol>.deleted to <vol>

    def __restore_volume(self, vol):

        # only allow deleted volume to be restored
        if vol[-8:] != '.deleted':
            error_msg = 'trying to restore a undeleted volume %s'%(vol)
            Trace.log(e_errors.ERROR, error_msg)
            return e_errors.ERROR, error_msg

        # check if another vol exists
        vol = vol[:-8]
        if self.dict.has_key(vol):
            # there is vol, check if it has been written
            record = self.dict[vol]
            if record['sum_wr_access']:
                error_msg = 'volume %s already exists, can not restore %s.deleted'%(vol, vol)
                Trace.log(e_errors.ERROR, error_msg)
                return e_errors.ERROR, error_msg
            else:    # never written, just erase it
                del self.dict[vol]
                Trace.log(e_errors.INFO, 'Empty volume "%s" is erased due to restoration of a previous version'%(vol))

        status = self.__rename_volume(vol+'.deleted', vol)
        if status[0] == e_errors.OK:
            # take care of system inhibit[0]
            record = self.dict[vol]
            record['system_inhibit'][0] = 'none'
            self.dict[vol] = record
            Trace.log(e_errors.INFO, 'volume "%s" has been restored'%(vol))
        return status

    # restore_volume(vol) -- server version of __restore_volume()

    def restore_volume(self, ticket):
        try:
            vol = ticket['external_label']
        except KeyError, detail:
            msg =  "Volume Clerk: key %s is missing"  % (detail)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        # This is a restricted service
        status = self.restricted_access()
        if status:
            msg = "attempt to restore volume %s from %s"%(vol, self.reply_address[0])
            Trace.log(e_errors.ERROR, msg)
            ticket['status'] = status
            self.reply_to_caller(ticket)
            return

        ticket['status'] = self.__restore_volume(vol)
        self.reply_to_caller(ticket)
        return

    # reassign_sg(self, ticket) -- reassign storage group
    #    only the volumes with initial storage 'none' can be reassigned

    def reassign_sg(self, ticket):
        try:
            vol = ticket['external_label']
            storage_group = ticket['storage_group']
        except KeyError, detail:
            msg =  "Volume Clerk: key %s is missing"  % (detail)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        if storage_group == 'none':
            msg = "Can not assign to storage group 'none'"
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        try:
	    record = self.dict[vol]
        except:
            msg = "trying to reassign sg for non-existing volume %s"%(vol)
            Trace.log(e_errors.ERROR, msg)
            ticket['status'] = (e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        sg, ff, wp = string.split(record['volume_family'], '.')
        if sg != 'none': # can not do it
            msg = "can not reassign from existing storage group %s"%(sg)
            Trace.log(e_errors.ERROR, msg)
            ticket['status'] = (e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        # deal with quota

        library = record['library']
        q_dict = self.quota_enabled(library, storage_group)
        if q_dict:
            if not self.check_quota(q_dict, library, storage_group):
                msg="%s Quota exceeded when reassiging blank volume to it. Contact enstore admin."%(storage_group)
                Trace.log(e_errors.ERROR,msg)
                ticket["status"] = (e_errors.QUOTAEXCEEDED, msg)
                self.reply_to_caller(ticket)
                return
            
        record['volume_family'] = string.join((storage_group, ff, wp), '.')

        self.dict[vol] = record
        self.sgdb.inc_sg_counter(library, storage_group)
        ticket['status'] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        return

    # set_comment() -- set comment to a volume record

    def set_comment(self, ticket):
        try:
            vol = ticket['vol']
            comment = ticket['comment']
        except KeyError, detail:
            msg =  "Volume Clerk: key %s is missing"  % (detail)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        # comment will be truncked at 80 characters
        if len(comment) > 80:
            comment = comment[:80]
        try:
	    record = self.dict[vol]
        except:
            msg = "trying to set comment for non-existing volume %s"%(vol)
            Trace.log(e_errors.ERROR, msg)
            ticket['status'] = (e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        if comment:
            record['comment'] = comment
        elif record.has_key('comment'):
            del record['comment']
        self.dict[vol] = record
        ticket['status'] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        return

    # show_quota() -- set comment to a volume record

    def show_quota(self, ticket):
	ticket['quota'] = self.quota_enabled(None, None)
	ticket['status'] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
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

        # This is a restricted service
        # but not for a disk media type
        media = ticket.get('media_type', None)
        if media and media == 'disk':
            status = None
        else:
            status = self.restricted_access()
        if status:
            msg = "attempt to add volume %s from %s"%(external_label, self.reply_address[0])
            Trace.log(e_errors.ERROR, msg)
            ticket['status'] = status
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
                    if not self.check_quota(q_dict, library, sg):
                        msg="Volume Clerk: %s quota exceeded while adding %s. Contact enstore admin."%(sg, external_label)
                        ticket["status"] = (e_errors.QUOTAEXCEEDED, msg)
                        Trace.log(e_errors.ERROR,msg)
                        self.reply_to_caller(ticket)
                        return
                inc_counter = 1
        else:
            msg= "Volume Clerk: key %s or %s is missing" % ('library', 'storage_group')
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
        record['remaining_bytes'] = ticket.get('remaining_bytes', record['capacity_bytes'])
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
        record['wrapper'] = ticket.get('wrapper', None)
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
        record['mounts'] = 0
        # write the ticket out to the database
        self.dict[external_label] = record
        if inc_counter: self.sgdb.inc_sg_counter(library, sg)
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
        
        # This is a restricted service
        status = self.restricted_access()
        if status:
            msg = "attempt to remove volume entry %s from %s"%(external_label, self.reply_address[0])
            Trace.log(e_errors.ERROR, msg)
            ticket['status'] = status
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
        ticket["status"] = (e_errors.OK, None)
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
                      record['system_inhibit'][1] != 'full' and
                      record['system_inhibit'][1] != 'migrated'):
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
                elif (record['system_inhibit'][1] == 'migrated'):
                    # treated as readonly
                    ret_stat = ('readonly', None)
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
                             min_remaining_bytes, exact_match=1,
                             mover={}):

        # go through the volumes and find one we can use for this request
        vol = {}
        type_of_mover = mover.get('mover_type','Mover')
        if type_of_mover == 'DiskMover':
          exact_match=1  
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
            if label in vol_veto_list:
                continue
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
                if type_of_mover == 'DiskMover':
                    mover_ip_map = mover.get('ip_map', '')
                    Trace.trace(30, "ip_map %s v %s"%(mover_ip_map,string.split(v['external_label'],':')[0])) 
                    if mover_ip_map and mover_ip_map is string.split(v['external_label'],':')[0]:
                        break
            else:
                if v["remaining_bytes"] < long(min_remaining_bytes*SAFETY_FACTOR):
                    Trace.trace(30, "almost full")
                    continue
                
            # vetoed = 0
            # for veto in vol_veto_list:
            #    if label == veto:
            #        Trace.trace(30, "vetoed")
            #        vetoed = 1
            #        break
            # if vetoed:
            #    continue

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
            Trace.log(e_errors.INFO, "storage group %s, vol counter %s, quota %s" % (storage_group, vol_count, quota)) 
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
        # To be backward compatible
        if not ticket.has_key('mover'):
            ticket['mover'] = {}
        mover_type = ticket['mover'].get('mover_type','Mover')
        if mover_type == 'DiskMover':
           use_exact_match = 1
           first_found = 1
        vol = self.find_matching_volume(library, vol_fam, pool,
                                        wrapper_type, vol_veto_list,
                                        first_found, min_remaining_bytes,exact_match=1,
                                        mover=ticket['mover'])
        Trace.trace(20, "find matching volume returned %s" % (vol,))

        if use_exact_match:
            if not vol or len(vol) == 0:
                # nothing was available at all
                if mover_type == 'DiskMover':
                    vol['external_label'] = None
                    vol['volume_family'] = vol_fam
                    vol['wrapper'] = wrapper_type
                    Trace.log(e_errors.INFO, "Assigning fake volume %s from storage group %s to library %s, volume family %s"
                      % (vol['external_label'], pool, library, vol_fam))
                    vol["status"] = (e_errors.OK, None)
                    self.reply_to_caller(vol)
                else:
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

        inc_counter = 0
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
                inc_counter = 1
                if q_dict:
                    if self.check_quota(q_dict, library, sg):
                        # this should not happen, do it but let someone know that
                        # more volumes need to be assigned to the storage group.
                        Trace.alarm(e_errors.ERROR,
                          "Volume Clerk: Selecting volume from common pool, add more volumes for %s"%(vol_fam,))
                        # this is important so turn the enstore ball red
                        # check if it is ignored
                        if not library+'.'+sg in self.ignored_sg:
			    ic = inquisitor_client.Inquisitor(self.csc)
                            ic.override(enstore_constants.ENSTORE, enstore_constants.RED)
                            # release ic
                            del ic
                    else:
                        msg="Volume Clerk: %s quota exceeded while drawing from common pool. Contact enstore admin."%(sg)
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
            if inc_counter: self.sgdb.inc_sg_counter(library, sg)
            self.dict[label] = vol  
            vol["status"] = (e_errors.OK, None)
            self.reply_to_caller(vol)
            return

        # nothing was available at all
        msg="Volume Clerk: no new volumes available [%s, %s]"%(library,
							       vol_fam)
        ticket["status"] = (e_errors.NOVOLUME, msg)
        # ignore NULL
        if volume_family.extract_wrapper(vol_fam) != 'null' and \
           library[:4] != 'null' and library[-4:] != 'null':
            Trace.alarm(e_errors.ERROR,msg)
            # this is important so turn the enstore ball red
            if not library+'.'+sg in self.ignored_sg:
                ic = inquisitor_client.Inquisitor(self.csc)
                ic.override(enstore_constants.ENSTORE, enstore_constants.RED)
                # release ic
                del ic
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
        if bfid:
            record['non_del_files'] = record['non_del_files'] + 1
            
        # record our changes
        self.dict[external_label] = record  
        record["status"] = (e_errors.OK, None)
        self.reply_to_caller(record)
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

        if external_label == None:
            external_label = '<None>'

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

        for key in ['wr_err','rd_err','wr_access','rd_access','mounts']:
            try:
                record['sum_'+key] = record['sum_'+key] + ticket[key]
            except KeyError, detail:
                if key == 'mounts':
                    # FIX ME LATER!!!
                    if ticket.has_key('mounts'):
                        # make a new dictionary entry for the old tape records
                        record['sum_mounts'] = ticket[key]
                else:
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

        # guarded against external_label == None
        if external_label:
            # get the current entry for the volume
            try:
                record = self.dict[external_label]  
                record["status"] = e_errors.OK, None
                self.reply_to_caller(record)
            except KeyError, detail:
                msg="Volume Clerk: no such volume %s" % (detail,)
                ticket["status"] = (e_errors.KEYERROR, msg)
                Trace.log(e_errors.ERROR, msg)
                self.reply_to_caller(ticket)
            return
        else:
            msg = "Volume Clerk::inquire_vol(): external_label == None"
            ticket["status"] = (e_errors.ERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

    # touch(self, ticket) -- update last_access time
    def touch(self, ticket):
        try:
            external_label = ticket["external_label"]
        except KeyError, detail:
            msg="touch(): key %s is missing" % (detail,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        # get the current entry for the volume
        try:
            record = self.dict[external_label]
            record['last_access'] = time.time()
            if record['first_access'] == -1:
                record['first_access'] = record['last_access']
            self.dict[external_label] = record
            ticket["last_access"] = record['last_access']
            ticket["status"] = (e_errors.OK, None)
            self.reply_to_caller(ticket)
            return
        except KeyError, detail:
            msg="touch(): no such volume %s" % (detail,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

    # check_record(self, ticket) -- trim obsolete fileds
    def check_record(self, ticket):
        try:
            external_label = ticket["external_label"]
        except KeyError, detail:
            msg="check_record(): key %s is missing" % (detail,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        # get the current entry for the volume
        try:
            record = self.dict[external_label]
            changed = 0
            for i in ['at_mover', 'status', 'file_family']:
                if record.has_key(i):
                    del record[i]
                    changed = 1
            if changed:
                self.dict[external_label] = record
            ticket["status"] = (e_errors.OK, None)
            self.reply_to_caller(ticket)
            return
        except KeyError, detail:
            msg="check_record(): no such volume %s" % (detail,)
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
                # set time stamp
                if not record.has_key('si_time'):
                    record['si_time'] = [0, 0]
                record['si_time'][position] = time.time()
                self.dict[external_label] = record   # THIS WILL JOURNAL IT
                record["status"] = (e_errors.OK, None)
        else:
            # if it is not record["system_inhibit"][0] just set it to none
            record[inhibit][position] = "none"
            if inhibit == "system_inhibit":
                # set time stamp
                if not record.has_key('si_time'):
                    record['si_time'] = [0, 0]
                record['si_time'][position] = time.time()
            self.dict[external_label] = record   # THIS WILL JOURNAL IT
            record["status"] = (e_errors.OK, None)
        if record["status"][0] == e_errors.OK:
            Trace.log(e_errors.INFO, "system inhibit cleared for %s" % (external_label, ))
        self.reply_to_caller(record)
        return

    # get the actual state of the media changer
    def get_media_changer_state(self, lib, volume, m_type):
        # m_changer = self.csc.get_media_changer(lib + ".library_manager")

        # a short cut for non-existing library, such as blank
        if not string.split(lib, '.')[0] in self.csc.get_library_managers({}).keys():
            return ""

        if len(lib) < 16 or lib[-16:] != '.library_manager':
            lib = lib + '.library_manager'
        m_changer = self.csc.get_media_changer(lib)
        if not m_changer:
            Trace.log(e_errors.ERROR,
                      " vc.get_media_changer_state: ERROR: no media changer found (lib = %s) %s" % (lib, volume))
            return 'no_mc'
            
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

        # ??? why does it matter? ???
        # if flag is "readonly":
        #     # check if volume is blank
        #     if record['non_del_files'] == 0:
        #         record['status'] = (e_errors.CONFLICT, "volume is blank")
        #         self.reply_to_caller(record)
        #         return record["status"]
        record["system_inhibit"][index] = flag
        # record time
        if not record.has_key("si_time"):
            record["si_time"] = [0,0]
        record["si_time"][index] = time.time()
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

    # flag that the current volume is migrated
    def set_system_migrated(self, ticket):
        return self.set_system_inhibit(ticket, "migrated", 1)

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
        # m_changer = self.csc.get_media_changer(record['library'] + ".library_manager")
        lib = record['library']
        if len(lib) < 16 or lib[-16:] != '.library_manager':
            lib = lib + '.library_manager'
        m_changer = self.csc.get_media_changer(lib)
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
        # m_changer = self.csc.get_media_changer(library + ".library_manager")
        if len(library) < 16 or library[-16:] != '.library_manager':
            library = library + '.library_manager'
        m_changer = self.csc.get_media_changer(library)
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
        # Trace.alarm(e_errors.WARNING, e_errors.NOTALLOWED,{"label":ticket["external_label"]}) 
        Trace.log(e_errors.INFO, "volume %s is set to NOTALLOWED"%(ticket['external_label']))
        return self.set_system_inhibit(ticket, e_errors.NOTALLOWED)

    # device is broken - what to do, what to do ===================================FIXME======================================
    def set_hung(self,ticket):
        self.reply_to_caller({"status" : (e_errors.OK, None)})
        return

    # return all the volumes in our dictionary.  Not so useful!
    def get_vols(self,ticket):
        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)

	# log it
        Trace.log(e_errors.INFO, "start listing all volumes")
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
                            ticket["in_state"] == "readonly" or
                            ticket["in_state"] == "migrated"):
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
                    if value.has_key('si_time'):
                        dict['si_time'] = value['si_time']
                    if value.has_key('comment'):
                        dict['comment'] = value['comment']
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
        Trace.log(e_errors.INFO, "stop listing all volumes")
        return

    # The following are for the sgdb
    def __rebuild_sg_count(self):
        self.sgdb.clear()
        c = self.dict.newCursor()
        k, v = c.first()
        while k:
            try:
                sg = string.split(v['volume_family'], '.')[0]
                self.sgdb.inc_sg_counter(v['library'], sg)
            except:
                pass
            k, v = c.next()
        c.close()

    def rebuild_sg_count(self, ticket):
        self.__rebuild_sg_count()
        ticket['status'] = (e_errors.OK, None)
        self.reply_to_caller(ticket)

    def set_sg_count(self, ticket):
        try:
            lib = ticket['library']
            sg = ticket['storage_group']
            count = ticket['count']
        except KeyError, detail:
            msg= "Volume Clerk: key %s is missing"%(detail,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return
	ticket['count'] = self.sgdb.set_sg_counter(lib, sg, count)
        if ticket['count'] == -1:
            ticket['status'] = (e_errors.ERROR, "failed to set %s.%s"%(lib,sg))
        else:
            ticket['status'] = (e_errors.OK, None)
        self.reply_to_caller(ticket)

    def get_sg_count(self, ticket):
        try:
            lib = ticket['library']
            sg = ticket['storage_group']
        except KeyError, detail:
            msg= "Volume Clerk: key %s is missing"%(detail,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return
	ticket['count'] = self.sgdb.get_sg_counter(lib, sg)
        if ticket['count'] == -1:
            ticket['status'] = (e_errors.ERROR, "failed to get %s.%s"%(lib,sg))
        else:
            ticket['status'] = (e_errors.OK, None)
        self.reply_to_caller(ticket)

    def list_sg_count(self, ticket):
        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)

        c = self.sgdb.dict.newCursor()
        sgcnt = {}
        k, v = c.first()
        while k:
            sgcnt[k] = v
            k, v = c.next()
        c.close()

        try:
            if not self.get_user_sockets(ticket):
                return
            ticket["status"] = (e_errors.OK, None)
            callback.write_tcp_obj(self.data_socket, ticket)
            callback.write_tcp_obj_new(self.data_socket, sgcnt)
            self.data_socket.close()
            callback.write_tcp_obj(self.control_socket, ticket)
            self.control_socket.close()
        except:
            exc, msg, tb = sys.exc_info()
            Trace.handle_error(exc,msg,tb)
        return

    def __get_vol_list(self):
        return self.dict.keys()

    # return a list of all the volumes
    def get_vol_list(self,ticket):
        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)

        try:
            if not self.get_user_sockets(ticket):
                return
            ticket["status"] = (e_errors.OK, None)
            callback.write_tcp_obj(self.data_socket, ticket)
            vols = self.__get_vol_list()
            callback.write_tcp_obj_new(self.data_socket, vols)
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
        # m_changer = self.csc.get_media_changer(ticket['library'] + ".library_manager")
        lib = ticket['library']
        if len(lib) < 16 or lib[-16:] != '.library_manager':
            lib = lib + '.library_manager'
        m_changer = self.csc.get_media_changer(lib)
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

    # The following is for temporarily surpress raising the red ball
    # when new tape is drawn from the common pool. The operator may
    # use the following methods to set or clear a library.storage_group
    # in an ignored group list. This list is presistent across the
    # sessions

    def set_ignored_sg(self, ticket):
        try:
            sg = ticket['sg']
        except KeyError, detail:
            msg= "Volume Clerk: key %s is missing"%(detail,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        # check syntax

        if len(string.split(sg, '.')) != 2:
            msg = 'wrong format. It has to be "library.storage_group"'
            ticket["status"] = (e_errors.ERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        if not sg in self.ignored_sg:
            self.ignored_sg.append(sg)
            # dump it to file
            try:
                f = open(self.ignored_sg_file, 'w')
                cPickle.dump(self.ignored_sg, f)
                f.close()
                Trace.log(e_errors.INFO, 'storage group "%s" has been ignored'%(sg))
            except:
                msg = 'Volume Clerk: failed to ignore storage group "%s"'%(sg)
                ticket['status'] = (e_errors.ERROR, msg)
                Trace.log(e_errors.ERROR, msg)
                self.reply_to_caller(ticket)
                return

        ticket['status'] = (e_errors.OK, self.ignored_sg)
        self.reply_to_caller(ticket)
        return

    def clear_ignored_sg(self, ticket):
        try:
            sg = ticket['sg']
        except KeyError, detail:
            msg= "Volume Clerk: key %s is missing"%(detail,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        if sg in self.ignored_sg:
            self.ignored_sg.remove(sg)
            # dump it to file
            try:
                f = open(self.ignored_sg_file, 'w')
                cPickle.dump(self.ignored_sg, f)
                f.close()
                Trace.log(e_errors.INFO, 'ignored storage group "%s" has been cleared'%(sg))
            except:
                msg = 'Volume Clerk: failed to clear ignored storage group "%s"'%(sg)
                ticket['status'] = (e_errors.ERROR, msg)
                Trace.log(e_errors.ERROR, msg)
                self.reply_to_caller(ticket)
                return
        else:
            msg = '"%s" is not in ignored storage group list'%(sg)
            ticket['status'] = (e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        ticket['status'] = (e_errors.OK, self.ignored_sg)
        self.reply_to_caller(ticket)
        return

    def clear_all_ignored_sg(self, ticket):
        try:
            self.ignored_sg = []
            f = open(self.ignored_sg_file, 'w')
            cPickle.dump(self.ignored_sg, f)
            f.close()
            Trace.log(e_errors.INFO, 'all ignored storage groups has been cleared')
        except:
            msg = 'Volume Clerk: failed to clear all ignored storage groups'
            ticket['status'] = (e_errors.ERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        ticket['status'] = (e_errors.OK, self.ignored_sg)
        self.reply_to_caller(ticket)
        return

    def list_ignored_sg(self, ticket):
        ticket['status'] = (e_errors.OK, self.ignored_sg)
        self.reply_to_caller(ticket)

class VolumeClerk(VolumeClerkMethods):
    def __init__(self, csc):
        VolumeClerkMethods.__init__(self, csc)
        Trace.init(self.log_name)
	self.alive_interval = monitored_server.get_alive_interval(self.csc,
								  MY_NAME,
								  self.keys)

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

        self.sgdb = sgdb.SGDb(dbHome)
        # rebuild it if it was not loaded
        if len(self.sgdb.dict) == 0:
            c = self.dict.newCursor()
            k, v = c.first()
            while k:
                try:
                    sg = string.split(v['volume_family'], '.')[0]
                    self.sgdb.inc_sg_counter(v['library'], sg)
                except:
                    pass
                k, v = c.next()
            c.close()
        self.noaccess_cnt = 0
        self.max_noaccess_cnt = self.keys.get('max_noaccess_cnt', 2)
        self.noaccess_to = self.keys.get('noaccess_to', 300.)
        self.paused_lms = {}
        self.noaccess_time = time.time()
        # load ignored sg
        self.ignored_sg_file = os.path.join(dbHome, 'IGNORED_SG')
        try:
            f = open(self.ignored_sg_file)
            self.ignored_sg = cPickle.load(f)
            f.close()
        except:
            self.ignored_sg = []

        # setup the communications with the event relay task
        self.resubscribe_rate = 300
        self.erc.start([event_relay_messages.NEWCONFIGFILE], self.resubscribe_rate)

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
