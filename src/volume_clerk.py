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
import edb
import Trace
import e_errors
import configuration_client
import volume_family
import esgdb
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

    # change_state(type, value) -- change a state
    def change_state(self, volume, type, value):
        q = "insert into state (volume, type, value) values (\
             lookup_vol('%s'), lookup_stype('%s'), '%s');" % \
             (volume, type, value)
        try:
	    res = self.dict.db.query(q)
        except:
            exc_type, exc_value = sys.exc_info()[:2]
            msg = "change_state(): "+exc_type+' '+exc_value+' query: '+q
            Trace.log(e_errors.ERROR, msg)


    # check if volume is full #### DONE
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
                v["si_time"][1] = time.time()
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

		# update it
		self.dict[external_label] = v
                self.change_state(external_label, 'system_inhibit_1', "full")
                Trace.log(e_errors.INFO, 'volume %s is set to "full" by is_volume_full()'%(external_label))
            else: ret = e_errors.NOSPACE
        return ret

    # __history(vol) -- show state change history of vol
    def __history(self, vol):
        q = "select time, label, state_type.name as type, state.value \
             from state, state_type, volume \
             where \
                label = '%s' and \
                state.volume = volume.id and \
                state.type = state_type.id \
             order by time desc;"%(vol)
        try:
            res = self.dict.db.query(q).dictresult()
        except:
            exc_type, exc_value = sys.exc_info()[:2]
            msg = '__history(): '+exc_type+' '+exc_value+' query: '+q
            Trace.log(e_errors.ERROR, msg)
            res = []
        return res

    # history(ticket) -- server version of __history()
    def history(self, ticket):
        try:
            vol = ticket['external_label']
            ticket["status"] = (e_errors.OK, None)
            self.reply_to_caller(ticket)
        except KeyError, detail:
            msg =  "Volume Clerk: key %s is missing"  % (detail)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        # get a user callback
        if not self.get_user_sockets(ticket):
            return
        callback.write_tcp_obj(self.data_socket,ticket)
        res = self.__history(vol)
        callback.write_tcp_obj_new(self.data_socket, res)
        self.data_socket.close()
        callback.write_tcp_obj(self.control_socket,ticket)
        self.control_socket.close() 
        return

    # set_write_protect
    def write_protect_on(self, ticket):
        try:
            vol = ticket['external_label']
        except KeyError, detail:
            msg =  "Volume Clerk: key %s is missing"  % (detail)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        try:
            res = self.change_state(vol, 'write_protect', 'ON')
            ticket['status'] = (e_errors.OK, None)
        except:
            exc_type, exc_value = sys.exc_info()[:2]
            msg = exc_type+' '+exc_value
            Trace.log(e_errors.ERROR, msg)
            ticket["status"] = (e_errors.ERROR, msg)
        self.reply_to_caller(ticket)
        return

    # set_write_protect
    def write_protect_off(self, ticket):
        try:
            vol = ticket['external_label']
        except KeyError, detail:
            msg =  "Volume Clerk: key %s is missing"  % (detail)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        try:
            res = self.change_state(vol, 'write_protect', 'OFF')
            ticket['status'] = (e_errors.OK, None)
        except:
            exc_type, exc_value = sys.exc_info()[:2]
            msg = exc_type+' '+exc_value
            Trace.log(e_errors.ERROR, msg)
            ticket["status"] = (e_errors.ERROR, msg)
        self.reply_to_caller(ticket)
        return

    # write_protect_status(self, ticket):
    def write_protect_status(self, ticket):
        try:
            vol = ticket['external_label']
        except KeyError, detail:
            msg =  "Volume Clerk: key %s is missing"  % (detail)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        q = "select time, value from state, state_type, volume \
             where \
                 state.type = state_type.id and \
                 state_type.name = 'write_protect' and \
                 state.volume = volume.id and \
                 volume.label = '%s' \
             order by time desc limit 1;"%(vol)

        try:
            res = self.dict.db.query(q).dictresult()
            if not res:
                status = "UNKNOWN"
            else:
                status = res[0]['value']
            ticket['status'] = (e_errors.OK, status)
        except:
            exc_type, exc_value = sys.exc_info()[:2]
            msg = 'write_protect_status(): '+exc_type+' '+exc_value+' query: '+q
            Trace.log(e_errors.ERROR, msg)
            ticket["status"] = (e_errors.ERROR, msg)
        self.reply_to_caller(ticket)
        return
        
    #### DONE
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
        record = self.dict[old]
        if not record:
            return 'EACCESS', "volume %s does not exist"%(old)

        if self.dict.has_key(new):
            return 'EEXIST', "volume %s already exists"%(new)

        try:
            record['external_label'] = new
            self.dict[old] = record
        except:
            Trace.log(e_errors.ERROR, "failed to rename %s to %s"%(old, new))
            return e_errors.ERROR, None

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

    # __erase_volume(vol) -- erase vol forever #### DONE
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

    # erase_volume(vol) -- server version of __erase_volume() #### DONE

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

    def has_undeleted_file(self, vol):
        q = "select * from file, volume where volume.label = '%s' and volume.id = file.volume and file.deleted <> 'y';"%(vol)
        res = self.dict.db.query(q)
        return res.ntuples()

    # __delete_volume(vol) -- delete a volume #### DONE
    #
    # * only a volume that contains no active files can be deleted
    #
    # deleting a volume, vol, is simply renaming it to vol.deleted
    #
    # if recycle flag is set, vol will be redeclared as a new volume

    def __delete_volume(self, vol, recycle = 0):
        # check existence of the volume
        record = self.dict[vol]
        if not record:
            msg = "Volume Clerk: no such volume %s" % (vol)
            Trace.log(e_errors.ERROR, msg)
            return e_errors.ERROR, msg

        # check if it has been deleted
        if vol[-8:] == '.deleted' or record['external_label'][-8:] == '.deleted':
            return e_errors.OK, 'volume %s has been deleted already'%(vol)

        # check if all files are deleted
        try:
            if self.has_undeleted_file(vol):
                msg = 'can not delete non-empty volume %s'%(vol)
                Trace.log(e_errors.ERROR, msg)
                return e_errors.ERROR, msg
        except:
            exc_type, exc_value = sys.exc_info()[:2]
            msg = 'has_undeleted_file(): '+exc_type+' '+exc_value
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
		self.change_state(vol+'.deleted', 'system_inhibit_0', e_errors.DELETED)
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
            self.change_state(vol, 'other', "RECYCLED");
            Trace.log(e_errors.INFO, 'volume "%s" has been recycled'%(vol))
        else:

            # get storage group and take care of quota

            library = record['library']
            sg = volume_family.extract_storage_group(record['volume_family'])
            self.sgdb.inc_sg_counter(library, sg, increment=-1)

        return status

    # delete_volume(vol) -- server version of __delete_volume() #### DONE

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

    #### DONE
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

    #### DONE
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
            self.change_state(vol, 'system_inhibit_0', 'none')
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

    #### DONE
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

	record = self.dict[vol]
        if not record:
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
        Trace.log(e_errors.INFO, "volume %s is assigned to storage group %s"%(vol, storage_group))
        self.reply_to_caller(ticket)
        return

    # set_comment() -- set comment to a volume record #### DONE

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

        record = self.dict[vol]
        if not record:
            msg = "trying to set comment for non-existing volume %s"%(vol)
            Trace.log(e_errors.ERROR, msg)
            ticket['status'] = (e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        if comment:
            record['comment'] = comment
        self.dict[vol] = record
        ticket['status'] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        return

    # show_quota() -- set comment to a volume record #### DONE

    def show_quota(self, ticket):
	ticket['quota'] = self.quota_enabled(None, None)
	ticket['status'] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        return

    #### DONE
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
        record['sum_mounts'] = ticket.get('sum_mounts', 0)
        record['non_del_files'] = ticket.get('non_del_files', 0)
        record['wrapper'] = ticket.get('wrapper', None)
        record['blocksize'] = ticket.get('blocksize', -1)
	record['si_time'] = [0.0, 0.0]
	record['comment'] = ""
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
        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        return

    #### DONE
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

        mdr = {}
        for key in record.keys():
            if ticket.has_key(key):
                record[key]=ticket[key]
                mdr[key]=ticket[key] # keep a record

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
        mdr['blocksize'] = msize
        if record['media_type']=='null':
            record['wrapper']='null'
            mdr['wrapper'] = 'null'
        # write the ticket out to the database
        self.dict[external_label] = record
        Trace.log(e_errors.INFO, "volume has been modifyed %s" % (record,))
        # to make SQL happy
        mdr2 = string.replace(`mdr`, "'", '"')
        self.change_state(external_label, 'modified', mdr2)
        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        return

    #### DONE
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
        record = self.dict[external_label]
        if not record:
            msg="Volume Clerk: no such volume %s" % (external_label)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        Trace.log(e_errors.INFO, 'removing volume %s from database. %s'%(external_label, `record`))
        del self.dict[external_label]
        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        return

    #### DONE
    # Check if volume is available
    def is_vol_available(self, ticket):
        work = ticket["action"]
        label = ticket["external_label"]
        # get the current entry for the volume
        record = self.dict[label]  
        if not record:
            msg="Volume Clerk: no such volume %s" % (label)
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

    # find volume that matches given volume family #### DONE
    def find_matching_volume(self, library, vol_fam, pool,
                             wrapper, vol_veto_list, first_found,
                             min_remaining_bytes, exact_match=1,
                             mover={}):

        # decomposit storage_group, file_family and wrapper
        storage_group, file_family, wrapper = string.split(pool, '.')

        # figure out minimal space needed
        required_bytes = max(long(min_remaining_bytes*SAFETY_FACTOR), MIN_LEFT)

        # build vito list into where clause
        vito_q = ""
        for i in vol_veto_list:
            vito_q = vito_q+" and label != '%s'"%(i)

        type_of_mover = mover.get('mover_type','Mover')

        # To be backward comparible
        if type_of_mover == 'DiskMover':
            exact_match = 1

        Trace.trace(20,  "volume family %s pool %s wrapper %s veto %s exact %s" %
                    (vol_fam, pool,wrapper, vol_veto_list, exact_match))

        # special treatment for Disk Mover
        if type_of_mover == 'DiskMover':
            mover_ip_map = mover.get('ip_map', '')

            q = "select * from volume \
                where \
                    label like '%s:%%' and \
                    library = '%s' and \
                    storage_group = '%s' and \
                    file_family = '%s' and \
                    wrapper = '%s' and \
                    system_inhibit_0 = 'none' and \
                    system_inhibit_1 = 'none' and \
                    user_inhibit_0 = 'none' and \
                    user_inhibit_1 = 'none' \
                    %s\
                order by declared ;"%(mover_ip_map, library,
                    storage_group, file_family, wrapper, vito_q)
        else: # normal case
            q = "select * from volume \
                where \
                    library = '%s' and \
                    storage_group = '%s' and \
                    file_family = '%s' and \
                    wrapper = '%s' and \
                    system_inhibit_0 = 'none' and \
                    system_inhibit_1 = 'none' and \
                    user_inhibit_0 = 'none' and \
                    user_inhibit_1 = 'none' \
                    %s\
                order by declared ;"%(library, storage_group,
                    file_family, wrapper, vito_q)
        Trace.trace(20, "start query: %s"%(q))
        try:
            res = self.dict.db.query(q).dictresult()
        except:
            exc_type, exc_value = sys.exc_info()[:2]
            msg = 'find_matching_volume(): '+exc_type+' '+exc_value+' query: '+q
            Trace.log(e_errors.ERROR, msg)
            res = []
        Trace.trace(20, "finish query: found %d exact_match=%d"%(len(res), exact_match))
        if len(res):
            if exact_match:
                for v in res:
                    v1 = self.dict.export_format(v)
                    if self.is_volume_full(v1,min_remaining_bytes):
                        Trace.trace(20, "set %s to full"%(v1['external_label']))
                    else:
                        return v1
                return {}
            else:
                return self.dict.export_format(res[0])
        else:
            return {}

    # check if quota is enabled in the configuration #### DONE
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

    # check quota #### DONE
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
            
    
    # Get the next volume that satisfy criteria #### DONE
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


    # check if specific volume can be used for write #### DONE
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
        v = self.dict[external_label]
        if not v:
            msg="Volume Clerk: no such volume %s" % (external_label)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

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

    # update the database entry for this volume #### DONE
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
        record = self.dict[external_label]  
        if not record:
            msg="Volume Clerk: no such volume %s" % (external_label,)
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

    #### DONE
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
        record = self.dict[external_label]  
        if not record:
            msg="Volume Clerk: no such volume %s"%(external_label,)
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

        if record["remaining_bytes"] == 0 and \
            record["system_inhibit"][1] == "none":
            record["system_inhibit"][1] = "full"
            self.change_state(external_label, 'system_inhibit_1', "full")
            record["si_time"][1] = time.time()

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

    # decrement the file count on the volume #### DONE
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
        record = self.dict[external_label]  
        if not record:
            msg="Volume Clerk: no such volume %s" % (external_label,)
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

    # update the database entry for this volume #### DONE
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
        record = self.dict[external_label]  
        if not record:
            msg="Volume Clerk: no such volume %s" % (external_label,)
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

    # get the current database volume about a specific entry #### DONE
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
            record = self.dict[external_label]
            if not record:
                msg="Volume Clerk: no such volume %s" % (external_label,)
                ticket["status"] = (e_errors.KEYERROR, msg)
                Trace.log(e_errors.ERROR, msg)
                self.reply_to_caller(ticket)
                return
            record["status"] = (e_errors.OK, None)
            self.reply_to_caller(record)
            return
        else:
            msg = "Volume Clerk::inquire_vol(): external_label == None"
            ticket["status"] = (e_errors.ERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

    # touch(self, ticket) -- update last_access time #### DONE
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
        record = self.dict[external_label]
        if not record:
            msg="touch(): no such volume %s" % (external_label,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        record['last_access'] = time.time()
        if record['first_access'] == -1:
            record['first_access'] = record['last_access']
        self.dict[external_label] = record
        ticket["last_access"] = record['last_access']
        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        return

    #### Should have nothing to trim!!!
    # check_record(self, ticket) -- trim obsolete fileds #### DONE
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
        record = self.dict[external_label]
        if not record:
            msg="check_record(): no such volume %s" % (external_label,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return
        changed = 0
        for i in ['at_mover', 'status', 'mounts']:
            if record.has_key(i):
                del record[i]
                changed = 1
        if changed:
            self.dict[external_label] = record
        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        return

    # flag the database that we are now writing the system #### DONE
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
        record = self.dict[external_label]
        if not record:
            msg="Volume Clerk: no such volume %s" % (external_label,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        # check the range of position
        if position != 0 and position != 1:
            msg="Volume Clerk: clr_system_inhibit(%s, %d), no such position %d"%(inhibit, position, position)
            ticket["status"] = (e_errors.ERROR, msg)
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
                record['si_time'][position] = time.time()
                self.dict[external_label] = record   # THIS WILL JOURNAL IT
                record["status"] = (e_errors.OK, None)
        else:
            # if it is not record["system_inhibit"][0] just set it to none
            record[inhibit][position] = "none"
            if inhibit == "system_inhibit":
                # set time stamp
                record['si_time'][position] = time.time()
            self.dict[external_label] = record   # THIS WILL JOURNAL IT
            record["status"] = (e_errors.OK, None)
        if record["status"][0] == e_errors.OK:
            type = inhibit+'_'+`position`
            self.change_state(external_label, type, "none")
            Trace.log(e_errors.INFO, "system inhibit %d cleared for %s" % (position, external_label))
        self.reply_to_caller(record)
        return

    # get the actual state of the media changer #### DONE
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

    # move a volume to a new library #### DONE
    def new_library(self, ticket):
        external_label = ticket["external_label"]
        new_library = ticket["new_library"]

        # get the current entry for the volume
        # get the current entry for the volume
        record = self.dict[external_label]  
        if not record:
            msg="Volume Clerk: no such volume %s" % (external_label,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return
        
        # update the library field with the new library
        old_library = record ["library"]
        record ["library"] = new_library
        self.dict[external_label] = record   # THIS WILL JOURNAL IT
        record["status"] = (e_errors.OK, None)
        Trace.log(e_errors.INFO, 'volume %s is assigned from library %s to library %s'%(external_label, old_library, new_library))
        self.reply_to_caller(record)
        return

    # set system_inhibit flag #### DONE
    def set_system_inhibit(self, ticket, flag, index=0):
        external_label = ticket["external_label"]
        # get the current entry for the volume
        record = self.dict[external_label]  
        if not record:
            msg="Volume Clerk: no such volume %s" % (external_label,)
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
        record["si_time"][index] = time.time()
        self.dict[external_label] = record   # THIS WILL JOURNAL IT
        self.change_state(external_label, 'system_inhibit_'+`index`, flag)
        record["status"] = (e_errors.OK, None)
        Trace.log(e_errors.INFO,external_label+" system inhibit set to "+flag)
        self.reply_to_caller(record)
        return record["status"]

    #### DONE
    # set system_inhibit flag, flag the database that we are now writing the system
    def set_writing(self, ticket):
        return self.set_system_inhibit(ticket, "writing")

    # set system_inhibit flag to none #### DONE
    def set_system_none(self, ticket):
        return self.set_system_inhibit(ticket, "none")

    # flag that the current volume is readonly #### DONE
    def set_system_readonly(self, ticket):
        return self.set_system_inhibit(ticket, "readonly", 1)

    # flag that the current volume is migrated #### DONE
    def set_system_migrated(self, ticket):
        return self.set_system_inhibit(ticket, "migrated", 1)

    #### DONE
    # set pause flag for the all Library Managers corresponding to
    # certain Media Changer
    def pause_lm(self, external_label):
        # get the current entry for the volume
        record = self.dict[external_label]
        if not record:
            msg="Volume Clerk: no such volume %s" % (external_label,)
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
                
    # check if Library Manager is paused #### DONE
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
            
            
    # flag that the current volume is marked as noaccess #### DONE
    def set_system_noaccess(self, ticket):
        Trace.alarm(e_errors.WARNING, e_errors.NOACCESS,{"label":ticket["external_label"]})
        rc = self.set_system_inhibit(ticket, e_errors.NOACCESS)
        if rc[0] == e_errors.OK:
            self.pause_lm(ticket["external_label"])
        return rc

    # flag that the current volume is marked as not allowed #### DONE
    def set_system_notallowed(self, ticket):
        # Trace.alarm(e_errors.WARNING, e_errors.NOTALLOWED,{"label":ticket["external_label"]}) 
        Trace.log(e_errors.INFO, "volume %s is set to NOTALLOWED"%(ticket['external_label']))
        return self.set_system_inhibit(ticket, e_errors.NOTALLOWED)

    #### DONE
    # device is broken - what to do, what to do ===================================FIXME======================================
    def set_hung(self,ticket):
        self.reply_to_caller({"status" : (e_errors.OK, None)})
        return

    #### DONE, probably not completely
    # return all the volumes in our dictionary.  Not so useful!
    def get_vols(self,ticket):
        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)

	# log it
        Trace.log(e_errors.INFO, "start listing all volumes")

        if not self.get_user_sockets(ticket):
            return
        try:
            callback.write_tcp_obj(self.data_socket, ticket)
        except:
            Trace.log(e_errors.ERROR, "get_vols(): client bailed out 1")
            return

        msg = {}
        q = "select * from volume "
        if ticket.has_key('in_state'):
            state = ticket['in_state']
        else:
            state = None
        if ticket.has_key('not'):
            cond = ticket['not']
        else:
            cond = None
        if ticket.has_key('key'):
            key = ticket['key']
        else:
            key = None

        if key and state:
            if key == 'volume_family':
                sg, ff, wp = string.split(state, '.')
                if cond == None:
                    q = q + "where storage_group = '%s' and file_family = '%s' and wrapper = '%s'"%(sg, ff, wp)
                else:
                    q = q + "where not (storage_group = '%s' and file_family = '%s' and wrapper = '%s')"%(sg, ff, wp)

            else:
                if key in ['blocksize', 'capacity_bytes',
                    'non_del_files', 'remaining_bytes', 'sum_mounts',
                    'sum_rd_access', 'sum_rd_err', 'sum_wr_access',
                    'sum_wr_err']:
                    val = "%d"%(state)
                elif key in ['eod_cookie', 'external_label', 'library',
                    'media_type', 'volume_family', 'wrapper',
                    'storage_group', 'file_family', 'wrapper']:
                    val = "'%s'"%(state)
                elif key in ['first_access', 'last_access', 'declared',
                    'si_time_0', 'si_time_1', 'system_inhibit_0',
                    'system_inhibit_1', 'user_inhibit_0',
                    'user_inhibit_1']:
                    val = "'%s'"%(edb.time2timestamp(state))
                else:
                    val = state

                if key == 'external_label':
                    key = 'label'

                if cond == None:
                    q = q + "where %s = %s"%(key, val)
                else:
                    q = q + "where %s %s %s"%(key, cond, val)
        elif state:
            if state in ['full', 'read_only', 'migrated']:
                q = q + "where system_inhibit_1 = '%s'"%(state)
            else:
                q = q + "where system_inhibit_0 = '%s'"%(state)
        else:
            msg['header'] = 'FULL'

        q = q + ' order by label;'

        try:
            res = self.dict.db.query(q).dictresult()
        except:
            exc_type, exc_value = sys.exc_info()[:2]
            mesg = 'get_vols(): '+exc_type+' '+exc_value+' query: '+q
            Trace.log(e_errors.ERROR, mesg)
            res = []

        msg['volumes'] = []
        for v2 in res:
            vol2 = {'volume': v2['label']}
            for k in ["capacity_bytes","remaining_bytes", "library",
                "non_del_files"]:
                vol2[k] = v2[k]
            vol2['volume_family'] = v2['storage_group']+'.'+v2['file_family']+'.'+v2['wrapper']
            vol2['system_inhibit'] = (v2['system_inhibit_0'], v2['system_inhibit_1'])
            vol2['user_inhibit'] = (v2['user_inhibit_0'], v2['user_inhibit_1'])
            vol2['si_time'] = (edb.timestamp2time(v2['si_time_0']),
                edb.timestamp2time(v2['si_time_1']))
            if len(v2['comment']):
                vol2['comment'] = v2['comment']
            msg['volumes'].append(vol2)

        try:
            callback.write_tcp_obj_new(self.data_socket, msg)
        except:
            Trace.log(e_errors.ERROR, "get_vols(): client bailed out 2")
            # clean up
            self.data_socket.close()
            return
        self.data_socket.close()
        try:
            callback.write_tcp_obj(self.control_socket, ticket)
        except:
            Trace.log(e_errors.ERROR, "get_vols(): client bailed out 3")
            # clean up
            self.control_socket.close()
            return
        self.control_socket.close()

        Trace.log(e_errors.INFO, "stop listing all volumes")
        return

    # The followings are for the sgdb

    #### DONE
    def rebuild_sg_count(self, ticket):
        self.sgdb.rebuild_sg_count()
        ticket['status'] = (e_errors.OK, None)
        self.reply_to_caller(ticket)

    #### DONE
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

    #### DONE
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

    #### DONE
    def list_sg_count(self, ticket):
        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)

        sgcnt = self.sgdb.list_sg_count()

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
            exc, msg = sys.exc_info()[:2]
            Trace.handle_error(exc,msg)
        return

    #### DONE
    def __get_vol_list(self):
        q = "select label from volume order by label;"
        try:
            res2 = self.dict.db.query(q).getresult()
        except:
            exc_type, exc_value = sys.exc_info()[:2]
            msg = '__get_vol_list(): '+exc_type+' '+exc_value+' query: '+q
            Trace.log(e_errors.ERROR, msg)
            return []
        res = []
        for i in res2:
            res.append(i[0])
        return res

    #### DONE        
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
            exc, msg = sys.exc_info()[:2]
            Trace.handle_error(exc,msg)
        return

    #### DONE
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
            exc, msg = sys.exc_info()[:2]
            Trace.handle_error(exc,msg)
        return 1

    #### DONE
    def start_backup(self,ticket):
        try:
            self.dict.start_backup()
            self.reply_to_caller({"status"        : (e_errors.OK, None),
                                  "start_backup"  : 'yes' })
        # catch any error and keep going. server needs to be robust
        except:
            exc, msg = sys.exc_info()[:2]
            Trace.handle_error(exc,msg)
            status = str(exc), str(msg)
            self.reply_to_caller({"status"       : status,
                                  "start_backup" : 'no' })

    #### DONE
    def stop_backup(self,ticket):
        try:
            Trace.log(e_errors.INFO,"stop_backup")
            self.dict.stop_backup()
            self.reply_to_caller({"status"       : (e_errors.OK, None),
                                  "stop_backup"  : 'yes' })
        # catch any error and keep going. server needs to be robust
        except:
            exc,msg=sys.exc_info()[:2]
            Trace.handle_error(exc,msg)
            status = str(exc), str(msg)
            self.reply_to_caller({"status"       : status,
                                  "stop_backup"  : 'no' })

    #### DONE
    def backup(self,ticket):
        try:
            Trace.log(e_errors.INFO,"backup")
            self.dict.backup()
            self.reply_to_caller({"status"       : (e_errors.OK, None),
                                  "backup"  : 'yes' })
        # catch any error and keep going. server needs to be robust
        except:
            exc, msg = sys.exc_info()[:2]
            Trace.handle_error(exc,msg)
            status = str(exc), str(msg)
            self.reply_to_caller({"status"       : status,
                                  "backup"  : 'no' })

    #### DONE
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

    #### DONE
    # The following is for temporarily surpress raising the red ball
    # when new tape is drawn from the common pool. The operator may
    # use the following methods to set or clear a library.storage_group
    # in an ignored group list. This list is presistent across the
    # sessions

    #### DONE
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

    #### DONE
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

    #### DONE
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

    #### DONE
    def list_ignored_sg(self, ticket):
        ticket['status'] = (e_errors.OK, self.ignored_sg)
        self.reply_to_caller(ticket)

    #### DONE
    def quit(self, ticket):
	self.dict.close()
	dispatching_worker.DispatchingWorker.quit(self, ticket)

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

        db_host = dbInfo['db_host']
        db_port = dbInfo['db_port']

        Trace.log(e_errors.INFO,"opening volume database using edb.VolumeDB")
        try:
            self.dict = edb.VolumeDB(host=db_host, port=db_port, jou=jouHome)
            self.sgdb = esgdb.SGDb(self.dict.db)
        except:
            exc_type, exc_value = sys.exc_info()[:2]
            msg = exc_type+' '+exc_value+' IS POSTMASTER RUNNING?'
            Trace.log(e_errors.ERROR,msg)
            Trace.alarm(e_errors.ERROR,msg, {})
            Trace.log(e_errors.ERROR, "CAN NOT ESTABLISH DATABASE CONNECTION ... QUIT!")
            sys.exit(1)

        # rebuild it if it was not loaded
        if len(self.sgdb) == 0:
            self.sgdb.rebuild_sg_count()
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
