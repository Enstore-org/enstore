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

# enstore imports
import callback
import dispatching_worker
import generic_server
import db
import Trace
import e_errors
import configuration_client
import bfid_db

##def p(*args):
##    lev = args[0]
##    if lev<20:
##        print args[1:]
##Trace.trace=p

# conditional comparison
def mycmp(cond, a, b):
    # condition may be None or some other
    if not cond: return a==b        # if cond is not None use ==
    else: return a!=b               # else use !=

# require 5% more space on a tape than the file size,
#    this accounts for the wrapper overhead and "some" tape rewrites

SAFETY_FACTOR=1.05

MY_NAME = "volume_clerk"

class VolumeClerkMethods(dispatching_worker.DispatchingWorker):

    # check if volume is full and if is set it to full
    def is_volume_full(self, v, min_remaining_bytes):
        external_label = v['external_label']
        ret = ""
        if v["remaining_bytes"] < long(min_remaining_bytes*SAFETY_FACTOR):
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
	 fcc.bfid = bfid_list[0]
	 vm_ticket = fcc.get_volmap_name()
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
	 exc, val, tb = e_errors.handle_error()
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
	     for bfid in bfid_list:
		 fcc.bfid = bfid
		 vm_ticket = fcc.get_volmap_name()
		 vol_map_name = vm_ticket["pnfs_mapname"]
		 (vm_dir,file) = os.path.split(vol_map_name)
		 ret = fcc.del_bfid()
		 os.remove(vol_map_name)
	     os.rmdir(vm_dir)
	     # remove current record from the database
	     del self.dict[external_label]
             # update the bfid database too
             self.bfid_db.delete_all_bfids(external_label)
	     Trace.log(e_errors.INFO, "volume removed %s"%(external_label,))
	     return e_errors.OK, None
     # even if there is an error - respond to caller so he can process it
     except:
	 exc, val, tb = e_errors.handle_error()
         return str(exc), str(val)
	 
    # remove deleted volume(s)
    # this method is called externally
    def remove_deleted_vols(self, ticket):
        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        # this could tie things up for awhile - fork and let child
        # send the work list (at time of fork) back to client
        if self.fork() != 0:
            return
        vols = []
        try:
            self.get_user_sockets(ticket)
            ticket["status"] = (e_errors.OK, None)
            callback.write_tcp_obj(self.data_socket, ticket)
            if not ticket.has_key("external_label"):
                # fill in the list of volumes to delete
                self.dict.cursor("open")
                key,value=self.dict.cursor("first")
                while key:
                    if value["system_inhibit"][0] == e_errors.DELETED:
                        vols.append(key)
                    key,value=self.dict.cursor("next")
                self.dict.cursor("close")
            else:
                if self.dict.has_key(ticket["external_label"]):
                    record = self.dict[ticket["external_label"]]
                    if record["system_inhibit"][0] == e_errors.DELETED:
                        vols.append(ticket["external_label"])
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
            print "EXCEPTION"
            e_errors.handle_error()
        os._exit(0)
            

    # add: some sort of hook to keep old versions of the s/w out
    # since we should like to have some control over format of the records.
    def addvol(self, ticket):
        # create empty record and control what goes into database
        # do not pass ticket, for example to the database!
        record={}

        # everything is based on external label - make sure we have this
        try:
            key="external_label"
            external_label = ticket[key]
        except KeyError:
            msg= "Volume Clerk: "+key+" key is missing"
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        # can't have 2 with same external_label
        if self.dict.has_key(external_label):
            msg="Volume Clerk: volume "+external_label+" already exists"
            ticket["status"] = (errno.errorcode[errno.EEXIST], msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        # mandatory keys
        for key in  ['external_label','media_type', 'library',
                     'eod_cookie', 'remaining_bytes', 'capacity_bytes']:
            try:
                record[key] = ticket[key]
            except KeyError:
                msg="Volume Clerk: "+key+" is missing"
                ticket["status"] = (e_errors.KEYERROR, msg)
                Trace.log(e_errors.ERROR,msg)
                self.reply_to_caller(ticket)
                return

        # check if library key is valid library manager name
        llm = self.csc.get_library_managers(ticket)

        # "shelf" library is a special case
        if ticket['library']!='shelf' and not llm.has_key(ticket['library']):
            Trace.log(e_errors.INFO,
                      " vc.addvol: Library Manager does not exist: %s " 
                      % (ticket['library'],))

        # form a volume family
        # if file family has a dotted notation then wrapper is
        # already defined and follws a "."
        record['volume_family'] = string.join((ticket['storage_group'],
                                               ticket['file_family']), '.')
        # optional keys - use default values if not specified
        record['last_access'] = ticket.get('last_access', -1)
        record['first_access'] = ticket.get('first_access', -1)
        record['declared'] = ticket.get('declared',-1)
        if record['declared'] == -1:
            record["declared"] = time.time()
        record['system_inhibit'] = ticket.get('system_inhibit', ["none", "none"])
        record['at_mover'] = ticket.get('at_mover',  ("unmounted", "none") )
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

        # everything is based on external label - make sure we have this
        try:
            key="external_label"
            external_label = ticket[key]
        except KeyError:
            msg= "Volume Clerk: "+key+" key is missing"
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return
        external_label=ticket['external_label']
        # make sure it exists
        if not self.dict.has_key(external_label):
            msg="Volume Clerk: volume "+external_label+" does not exist"
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
        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        return

        
    # delete a volume from the database
    def delvol(self, ticket):
        # everything is based on external label - make sure we have this
        key="external_label"
        try:
            external_label = ticket[key]
        except KeyError:
            msg= "Volume Clerk: "+key+" key is missing"
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        # get the current entry for the volume
        try:
            record = self.dict[external_label]
        except KeyError:
            msg="Volume Clerk: volume "+external_label+" no such volume"
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        if record['at_mover'][0] != 'unmounted' and record['at_mover'][0] != '' and record['at_mover'][0] != 'E':
           ticket["status"] = (e_errors.CONFLICT,"volume state must be unmounted or '' or 'E'")
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

	# if volume has not been written delete it
	if record['sum_wr_access'] == 0:
	    del self.dict[external_label]
            #clear the bfid database file too
            self.bfid_db.delete_all_bfids(external_label)
	    ticket["status"] = (e_errors.OK, None)
	else:
	    record["system_inhibit"][0] = e_errors.DELETED
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
		Trace.log(e_errors.INFO,"Volume %s is deleted"%(external_label,))

        self.reply_to_caller(ticket)
        return

    # restore a volume
    def restorevol(self, ticket):
        try:
	    # everything is based on external label - make sure we have this
	    key="external_label"
            external_label = ticket[key]
	    key="restore"
	    restore_vm = ticket[key]
        except KeyError:
            msg= "Volume Clerk: %s key is missing"%(key,)
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        # get the current entry for the volume
	cl = external_label+".deleted"
        try:
            record = self.dict[cl]
        except KeyError:
            msg="Volume Clerk: volume %s: no such volume"%(cl,)
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
	record = self.dict[label]  
        ret_stat = (e_errors.OK,None)
	if record["system_inhibit"][0] == e_errors.DELETED:
	    ret_stat = (record["system_inhibit"][0],None)
	else:
	    if work == 'read_from_hsm':
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
                    vf = string.split(ticket['volume_family'],'.')
		    if (ticket['volume_family'] == record['volume_family'] or
                        vf[1] == 'ephemeral'):
                        ret = self.is_volume_full(record,ticket['file_size'])
                        if not ret:
                            ret_stat = (e_errors.OK,None)
                        else:
                            ret_stat = (ret, None)
                    else: ret_stat = (e_errors.NOACCESS,None)
	    else:
		ret_stat = (e_errors.UNKNOWN,None)
	ticket['status'] = ret_stat
	self.reply_to_caller(ticket)

    # find volume that matches given volume family
    def find_matching_volume(self, library, volume_family, pool,
                             wrapper, vol_veto_list, first_found,
                             min_remaining_bytes, exact_match=1):

        # go through the volumes and find one we can use for this request
        vol = {}
        lc = self.dict.inx['library'].cursor()		# read only
        vc = self.dict.inx['volume_family'].cursor()
        label, v = lc.set(library)
        label, v = vc.set(pool)
        c = db.join(self.dict, [lc, vc])
        while 1:
            label,v = c.next()
            if not label:
                break
            if v["user_inhibit"] != ("none",  "none"):
                continue
            if v["system_inhibit"] != ("none", "none"):
                continue
            at_mover = v.get('at_mover',('unmounted', '')) # for backward compatibility for at_mover field
            if v['at_mover'][0] != "unmounted" and  v['at_mover'][0] != None: 
                continue

            # equal treatment for blank volume
            if exact_match:
                if self.is_volume_full(v,min_remaining_bytes): continue
            else:
                if v["remaining_bytes"] < long(min_remaining_bytes*SAFETY_FACTOR):
                    continue
                
            vetoed = 0
            for veto in vol_veto_list:
                if label == veto:
                    vetoed = 1
                    break
            if vetoed:
                continue

            # supposed to return first volume found?
            # do not return blank volume at this point yet
            if first_found:
                if exact_match:
                    v["status"] = (e_errors.OK, None)
                    self.reply_to_caller(v)
                    c.close()
                    return None
                else:
                    # volume family may contain storage group+file family
                    # or none.none
                    vf =string.split(volume_family, '.')
                    if vf[1] == 'ephemeral':
                        volume_family = string.join((vf[0], label, wrapper), '.')
                    v['volume_family'] = volume_family
                    Trace.log(e_errors.INFO, "Assigning blank volume "+label+" to "+library+" "+volume_family)
                    c.close()
                    self.dict[label] = v  
                    v["status"] = (e_errors.OK, None)
                    self.reply_to_caller(v)
                    return None
                    
            # if not, is there an "earlier" volume that we have already found?
            if len(vol) == 0:
                vol = v  
            elif v['declared'] < vol['declared']:
                vol = v  
        c.close()
        return vol
        
    
    # Get the next volume that satisfy criteria
    def next_write_volume (self, ticket):
        vol_veto = ticket["vol_veto_list"]
        vol_veto_list = eval(vol_veto)

        # get the criteria for the volume from the user's ticket
        min_remaining_bytes = ticket["min_remaining_bytes"]
        library = ticket["library"]
        volume_family = ticket['volume_family']
        first_found = ticket["first_found"]
        wrapper_type = ticket["wrapper"]
        ##print "CGW1: lib='%s' fam='%s' wrap='%s'"%(library,file_family,wrapper_type)
        # go through the volumes and find one we can use for this request
        # first use exact match
        vol = self.find_matching_volume(library, volume_family, volume_family,
                                        wrapper_type, vol_veto_list,
                                        first_found, min_remaining_bytes,1)

        # return what we found
        if vol and len(vol) != 0:
            vol["status"] = (e_errors.OK, None)
            self.reply_to_caller(vol)
            return

        # nothing was available - see if we can assign a blank from a
        # given storage group.
        vf = string.split(volume_family,'.')
        pool = string.join((vf[0],'none'), '.')
        vol = self.find_matching_volume(library, volume_family, pool, wrapper_type,
                                        vol_veto_list, first_found, min_remaining_bytes,0)

        # return blank volume we found
        if vol and len(vol) != 0:
            label = vol['external_label']

            vf =string.split(volume_family, '.')
            if vf[1] == 'ephemeral':
                volume_family = string.join((vf[0], label, wrapper_type), '.')
            vol['volume_family'] = volume_family
            Trace.log(e_errors.INFO, "Assigning blank volume %s from storage group %s to libray %s, volume family %s"
                      % (label, pool, library, volume_family))
            self.dict[label] = vol  
            vol["status"] = (e_errors.OK, None)
            self.reply_to_caller(vol)
            return
        # nothing was available - see if we can assign a blank one.
        pool = 'none.none'
        vol = self.find_matching_volume(library, volume_family, pool, wrapper_type,
                                        vol_veto_list, first_found, min_remaining_bytes, 0)

        # return blank volume we found
        if vol and len(vol) != 0:
            label = vol['external_label']

            vf =string.split(volume_family, '.')
            if vf[1] == 'ephemeral':
                volume_family = string.join((vf[0], label, wrapper_type), '.')
            vol['volume_family'] = volume_family
            Trace.log(e_errors.INFO,
                      "Assigning blank volume "+label+" to "+library+" "+volume_family)
            self.dict[label] = vol  
            vol["status"] = (e_errors.OK, None)
            self.reply_to_caller(vol)
            return

        # nothing was available at all
        msg="Volume Clerk: no new volumes available"
        ticket["status"] = (e_errors.NOVOLUME, msg)
        Trace.log(e_errors.ERROR,msg)
        self.reply_to_caller(ticket)
        return


    # check if specific volume can be used for write
    def can_write_volume (self, ticket):
     # get the criteria for the volume from the user's ticket
     try:
         key = "min_remaining_bytes"
         min_remaining_bytes = ticket[key]
         key = "library"
         library = ticket[key]
         key = "volume_family"
         volume_family = ticket[key]
         key = "external_label"
         external_label = ticket[key]
     except KeyError:
         msg="Volume Clerk: "+key+" is missing"
         ticket["status"] = (e_errors.KEYERROR, msg)
         Trace.log(e_errors.ERROR, msg)
         self.reply_to_caller(ticket)
         return

     # get the current entry for the volume
     try:
         v = self.dict[external_label]  
         # for backward compatibility for at_mover field
         try:
             at_mover = v['at_mover']
         except KeyError:
             v['at_mover'] = ('unmounted', '')

         ticket["status"] = (e_errors.OK,'None')
         if (v["library"] == library and
             (v["volume_family"] == volume_family) and
             v["user_inhibit"][0] == "none" and
             v["user_inhibit"][1] == "none" and
             v["system_inhibit"][0] == "none" and
             v["system_inhibit"][1] == "none" and
             v['at_mover'][0] == "mounted"):
             ##
             ##ret_st = self.is_volume_full(v,min_remaining_bytes)
             ##if ret_st:
             ##    ticket["status"] = (ret_st,None)
             ##

             if v["remaining_bytes"] < long(min_remaining_bytes*SAFETY_FACTOR):
                 ticket["status"] = (e_errors.WRITE_EOT,"file too big")
             self.reply_to_caller(ticket)
             return
         else:
             ticket["status"] = (e_errors.NOACCESS,'None')
             self.reply_to_caller(ticket)
             return
     except KeyError:
         msg="Volume Clerk: volume "+external_label+" no such volume"
         ticket["status"] = (e_errors.KEYERROR, msg)
         Trace.log(e_errors.ERROR, msg)
         self.reply_to_caller(ticket)
         return


    # update the database entry for this volume
    def get_remaining_bytes(self, ticket):
        ticket['status'] = (e_errors.OK, None)
        # everything is based on external label - make sure we have this
        try:
            key="external_label"
            external_label = ticket[key]
        except KeyError:
            msg="Volume Clerk: "+key+" key is missing"
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        # get the current entry for the volume
        try:
            record = self.dict[external_label]  
        except KeyError:
            msg="Volume Clerk: volume "+external_label+" no such volume"
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return


        # access the remaining_byte field
        try:
            ticket["remaining_bytes"] = record["remaining_bytes"]
        except KeyError:
            msg="Volume Clerk: "+key+" key is missing"
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            
        self.reply_to_caller(ticket)
        return


    # update the database entry for this volume
    def set_remaining_bytes(self, ticket):
        # everything is based on external label - make sure we have this
        try:
            key="external_label"
            external_label = ticket[key]
        except KeyError:
            msg= "Volume Clerk: "+key+" key is missing"
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        # get the current entry for the volume
        try:
            record = self.dict[external_label]  
        except KeyError:
            msg="Volume Clerk: volume "+external_label+" no such volume"
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        # update the fields that have changed
        try:
            for key in ["remaining_bytes","eod_cookie"]:
                record[key] = ticket[key]

        except KeyError:
            msg="Volume Clerk: "+key+" key is missing"
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        record["system_inhibit"][0] = "none"
        record["last_access"] = time.time()
        if record["first_access"] == -1:
            record["first_access"] = record["last_access"]

        for key in ['wr_err','rd_err','wr_access','rd_access']:
            try:
                record['sum_'+key] = record['sum_'+key] + ticket[key]

            except KeyError:
                msg="Volume Clerk: "+key+" key is missing"
                ticket["status"] = (e_errors.KEYERROR, msg)
                Trace.log(e_errors.ERROR, msg)
                self.reply_to_caller(ticket)
                return

        #TEMPORARY TRY BLOCK - all new volumes should already have the non_del_files key
        try:
            non_del_files = record['non_del_files']
        except KeyError:
            record['non_del_files'] = record['sum_wr_access']-record['sum_wr_err']

        # update the non-deleted file count if we wrote to the tape
        # this key gets decremented when we delete files
        if not ticket.get('wr_err',0):
            record['non_del_files'] = record['non_del_files'] + \
                                      ticket['wr_access']

	bfid = ticket.get("bfid")
	if bfid:
            self.bfid_db.add_bfid(external_label, bfid)
        # record our changes
        self.dict[external_label] = record  
        record["status"] = (e_errors.OK, None)
        self.reply_to_caller(record)
        return

    # decrement the file count on the volume
    def decr_file_count(self, ticket):
        # everything is based on external label - make sure we have this
        try:
            key="external_label"
            external_label = ticket[key]
        except KeyError:
            msg="Volume Clerk: "+key+" key is missing"
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        # get the current entry for the volume
        try:
            record = self.dict[external_label]  
        except KeyError:
            msg="Volume Clerk: volume "+external_label+" no such volume"
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
        # everything is based on external label - make sure we have this
        try:
            key="external_label"
            external_label = ticket[key]
        except KeyError:
            msg="Volume Clerk: "+key+" key is missing"
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR,msg)
            self.reply_to_caller(ticket)
            return

        # get the current entry for the volume
        try:
            record = self.dict[external_label]  
        except KeyError:
            msg="Volume Clerk: volume "+external_label+" no such volume"
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
            except KeyError:
                msg= "Volume Clerk: "+key+" key is missing"
                ticket["status"] = (e_errors.KEYERROR, msg)
                Trace.log(e_errors.ERROR, msg)
                self.reply_to_caller(ticket)
                return

        #TEMPORARY TRY BLOCK - all new volumes should already have the non_del_files key
        try:
            non_del_files = record['non_del_files']
        except KeyError:
            record['non_del_files'] = record['sum_wr_access']-record['sum_wr_err']

        # update the non-deleted file count if we wrote to the tape
        # this key gets decremented when we delete files
        if not ticket.get('wr_err',0):
            record['non_del_files'] = record['non_del_files'] + \
                                      ticket['wr_access']
        # record our changes
        self.dict[external_label] = record  
        record["status"] = (e_errors.OK, None)
        self.reply_to_caller(record)
        return

    # get the current database volume about a specific entry
    def inquire_vol(self, ticket):
        # everything is based on external label - make sure we have this
        try:
            key="external_label"
            external_label = ticket[key]
        except KeyError:
            msg="Volume Clerk: "+key+" key is missing"
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
        except KeyError:
            msg="Volume Clerk: volume "+external_label +" no such volume"
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

    # update dB to reflect actual state of volume
    def update_mc_state(self, ticket):
        # everything is based on external label - make sure we have this
        if 'external_label' not in ticket.keys():
            msg= "Volume Clerk: external label is missing"
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR,msg)
            self.reply_to_caller(ticket)
            return
        external_label = ticket['external_label']
        # get the current entry for the volume
        try:
            record = self.dict[external_label]  
        except KeyError:
            msg="Volume Clerk: volume "+external_label +" no such volume"
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        # update the fields that have changed
        ll = list(record['at_mover'])
        ret = self.get_media_changer_state(record["library"],
					    record["external_label"],
                                            record["media_type"])
        # when MC call fails return value will be ""
        if not ret:
            Trace.log(e_errors.ERROR, "call to media changer failed")
        else:
            ll[0] = ret
        record['at_mover']=tuple(ll)
        # if volume is unmounted system_inhibit cannot be writing
        if (record['at_mover'][0] == 'unmounted' and
            record['system_inhibit'][0] == 'writing'):
            record['system_inhibit'][0] = "none"
        self.dict[external_label] = record   # THIS WILL JOURNAL IT
        record["status"] = (e_errors.OK, None)
        self.reply_to_caller(record)
        return

    # flag the database that we are now writing the system
    def clr_system_inhibit(self, ticket):
        # everything is based on external label - make sure we have this
        try:
            key="external_label"
            external_label = ticket[key]
            key = "inhibit"
            inhibit = ticket[key]
            if not inhibit: inhibit = "system_inhibit" # set default field 
            key = "position"
            position = ticket[key]
        except KeyError:
            msg="Volume Clerk: "+key+" key is missing"
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        # get the current entry for the volume
        try:
            record = self.dict[external_label]  
        except KeyError:
            msg="Volume Clerk: volume "+external_label+" no such volume"
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
                ll = list(record['at_mover'])
                ret = self.get_media_changer_state(record["library"],
                                                    record["external_label"], 
                                                    record["media_type"])

                # when MC call fails return value will be ''
                if not ret:
                    Trace.log(e_errors.ERROR, "call to media changer failed")
                else:
                    ll[0] = ret
                
                record['at_mover']=tuple(ll)
                self.dict[external_label] = record   # THIS WILL JOURNAL IT
                record["status"] = (e_errors.OK, None)
        else:
            # if it is not record["system_inhibit"][0] just set it to none
            record[inhibit][position] = "none"
	    self.dict[external_label] = record   # THIS WILL JOURNAL IT
	    record["status"] = (e_errors.OK, None)
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
        if stat == 'O':
            state = 'unmounted'
        elif stat == 'M':
            state = 'mounted'
        else :
            state = stat

        return state


    # for the backward compatibility D0_TEMP
    # flag the database that we are now writing the system
    def add_at_mover(self, ticket):
        # everything is based on external label - make sure we have this
        try:
            key="external_label"
            external_label = ticket[key]
        except KeyError:
            msg="Volume Clerk: "+key+" key is missing"
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.INFO, msg)
            self.reply_to_caller(ticket)
            return

        # get the current entry for the volume
        try:
            record = self.dict[external_label]  
        except KeyError:
            msg="Volume Clerk: volume "+external_label+" no such volume"
            ticket["status"] = (e_errors.KEYERROR, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return

        # add fields
        try:
            at_mover = record['at_mover']
        except KeyError:
            record['at_mover'] = ('unmounted', 'none')
        try:
            non_del_files = record['non_del_files']
        except KeyError:
            record['non_del_files'] = record['sum_wr_access']

        self.dict[external_label] = record   # THIS WILL JOURNAL IT
        record["status"] = (e_errors.OK, None)
        self.reply_to_caller(record)
        return

    # move a volume to a new library
    def new_library(self, ticket):
	external_label = ticket["external_label"]
	new_library = ticket["new_library"]

	# get the current entry for the volume
	record = self.dict[external_label]  
	
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
	record = self.dict[external_label]  

	# update the fields that have changed
	record["system_inhibit"][index] = flag

	self.dict[external_label] = record   # THIS WILL JOURNAL IT
	record["status"] = (e_errors.OK, None)
	Trace.log(e_errors.INFO,external_label+" system inhibit set to "+flag)
	self.reply_to_caller(record)
	return

    # set system_inhibit flag, flag the database that we are now writing the system
    def set_writing(self, ticket):
        return self.set_system_inhibit(ticket, "writing")

    # set system_inhibit flag to none
    def set_system_none(self, ticket):
        return self.set_system_inhibit(ticket, "none")

    # flag that the current volume is readonly
    def set_system_readonly(self, ticket):
        return self.set_system_inhibit(ticket, "readonly", 1)

    # flag that the current volume is marked as noaccess
    def set_system_noaccess(self, ticket):
        Trace.alarm(e_errors.WARNING, e_errors.NOACCESS,{"label":ticket["external_label"]}) 
        #        return self.set_system_inhibit(ticket, e_errors.NOACCESS)
        # setting volume to NOACCESS has proven to be disastrous
        # it is better to let 1 volume fail than give out all remaining tapes
        # in cases like media changer failure
        return self.set_system_inhibit(ticket, "none")

    # flag that the current volume is marked as not allowed
    def set_system_notallowed(self, ticket):
        Trace.alarm(e_errors.WARNING, e_errors.NOTALLOWED,{"label":ticket["external_label"]}) 
        return self.set_system_inhibit(ticket, e_errors.NOTALLOWED)

    # device is broken - what to do, what to do ===================================FIXME======================================
    def set_hung(self,ticket):
	self.reply_to_caller({"status" : (e_errors.OK, None)})
	return

    # set at_mover flag
    def set_at_mover(self, ticket):
	external_label = ticket["external_label"]
	record = self.dict[external_label]  
	at_mover = record.get('at_mover',('unmounted','none'))
	
	# update the fields that have changed
	if ticket['force']:
	    wrong_state = 0
	else:
	    wrong_state = 1
	    if (ticket['at_mover'][0] == 'mounted' and
		record['at_mover'][0] != 'unmounted'):
                pass
	    elif (ticket['at_mover'][0] == 'unmounted' and
		  record['at_mover'][0] != 'mounted'):
                pass
	    else:
		wrong_state = 0

	if wrong_state:
	    record["status"] = (e_errors.CONFLICT, "volume "+
				repr(external_label)+ " state "+
				repr(record['at_mover'][0])+" req. state "+
				repr(ticket['at_mover'][0]))
	else:
	    record ['at_mover'] = ticket['at_mover']

            # Take care of the impossible "unmounted and writing" state
            if (record['at_mover'][0] == "unmounted" and
                record['system_inhibit'][0] == "writing"):
                record['system_inhibit'][0] = "none"

	    self.dict[external_label] = record   # THIS WILL JOURNAL IT
	    record["status"] = (e_errors.OK, None)
	self.reply_to_caller(record)
	return

    # return all the volumes in our dictionary.  Not so useful!
    def get_vols(self,ticket):
        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)

        # this could tie things up for awhile - fork and let child
        # send the work list (at time of fork) back to client
        if self.fork() != 0:
            return
        try:
            import cPickle
            self.get_user_sockets(ticket)
            ticket["status"] = (e_errors.OK, None)
            callback.write_tcp_obj(self.data_socket, ticket)
            self.dict.cursor("open")
            key,value=self.dict.cursor("first")
            msg={}
            while key:
                if ticket.has_key("not"): cond = ticket["not"]
                if ticket.has_key("in_state") and ticket["in_state"] != None:
                    if ticket.has_key("key") and ticket["key"] != None:
                        if value.has_key(ticket["key"]):
                            if ticket["key"] == "at_mover":
                                loc_val = value[ticket["key"]][0]
                            else:
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
                    for k in ["remaining_bytes", "at_mover",   "system_inhibit",
                              "user_inhibit", "library", "volume_family"]:
                        dict[k]=value[k]
                    if msg:
                        msg["volumes"].append(dict)
                    else:
                        msg["header"] = "FULL"
                        msg["volumes"]= []
                        msg["volumes"].append(dict)

                key,value=self.dict.cursor("next")
            to_send = cPickle.dumps(msg)
            callback.write_tcp_raw(self.data_socket, to_send)
            self.dict.cursor("close")
            self.data_socket.close()

            callback.write_tcp_obj(self.control_socket, ticket)
            self.control_socket.close()
        except:
            exc, msg, tb = sys.exc_info()
            e_errors.handle_error(exc,msg,tb)
        os._exit(0)


    # get a port for the data transfer
    # tell the user I'm your volume clerk and here's your ticket
    def get_user_sockets(self, ticket):
        try:
            volume_clerk_host, volume_clerk_port, listen_socket = callback.get_callback()
            listen_socket.listen(4)
            ticket["volume_clerk_callback_host"] = volume_clerk_host
            ticket["volume_clerk_callback_port"] = volume_clerk_port
            self.control_socket = callback.user_callback_socket(ticket)
            data_socket, address = listen_socket.accept()
            self.data_socket = data_socket
            listen_socket.close()
        # catch any error and keep going. server needs to be robust
        except:
            exc, msg, tb = sys.exc_info()
            e_errors.handle_error(exc,msg,tb)

    def start_backup(self,ticket):
        try:
            self.dict.start_backup()
            self.reply_to_caller({"status"        : (e_errors.OK, None),
                                  "start_backup"  : 'yes' })
        # catch any error and keep going. server needs to be robust
        except:
            exc, msg, tb = sys.exc_info()
            e_errors.handle_error(exc,msg,tb)
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
            e_errors.handle_error(exc,msg,tb)
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
            e_errors.handle_error(exc,msg,tb)
            status = str(exc), str(msg)
            self.reply_to_caller({"status"       : status,
                                  "backup"  : 'no' })


class VolumeClerk(VolumeClerkMethods, generic_server.GenericServer):
    def __init__(self, csc):
        generic_server.GenericServer.__init__(self, csc, MY_NAME)
        Trace.init(self.log_name)
        keys = self.csc.get(MY_NAME)

        dispatching_worker.DispatchingWorker.__init__(self, (keys['hostip'],
                                                             keys['port']))

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
        
class VolumeClerkInterface(generic_server.GenericServerInterface):
        pass

if __name__ == "__main__":
    Trace.init(string.upper(MY_NAME))

    # get the interface
    intf = VolumeClerkInterface()
    vc = VolumeClerk((intf.config_host, intf.config_port))
    Trace.log(e_errors.INFO, '%s' % (sys.argv,))

    while 1:
        try:
            Trace.log(e_errors.INFO,'Volume Clerk (re)starting')
            vc.serve_forever()
	except SystemExit, exit_code:
	    sys.exit(exit_code)
        except:
            vc.serve_forever_error(vc.log_name)
            continue
    Trace.log(e_errors.ERROR,"Volume Clerk finished (impossible)")
