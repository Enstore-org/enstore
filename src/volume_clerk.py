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


    ## Avoid sending back a ticket which contains the "bfids" key, which may
    ## be arbitrarily large and hence not fit in a UDP packet.  This is a hack which
    ## should probably be replaced with a more general notion of public vs. private
    ## (or external vs. internal) elements of tickets
    def reply_to_caller(self, ticket,
                        replyfunc=dispatching_worker.DispatchingWorker.reply_to_caller):
        has_bfids = ticket.has_key("bfids")
        if has_bfids:
            bfids=ticket["bfids"]
            del ticket["bfids"]
        replyfunc(self,ticket)
        if has_bfids:
            ticket["bfids"]=bfids
            
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
	 fcc.bfid = cur_rec['bfids'][0]
	 vm_ticket = fcc.get_volmap_name()
	 old_vol_map_name = vm_ticket["pnfs_mapname"]
	 (old_vm_dir,file) = os.path.split(old_vol_map_name)
	 new_vm_dir = string.replace(old_vm_dir, old_label, new_label)
	 # rename map files
	 Trace.log(e_errors.INFO, "trying volume map directory renamed %s->%s"%\
		   (old_vm_dir, new_vm_dir))
	 os.rename(old_vm_dir, new_vm_dir)
	 Trace.log(e_errors.INFO, "volume map directory renamed %s->%s"%\
		   (old_vm_dir, new_vm_dir))
	 # replace file clerk database entries
	 for bfid in cur_rec['bfids']:
	     ret = fcc.rename_volume(bfid, new_label, 
				     set_deleted, restore, restore_dir)
	     if ret["status"][0] != e_errors.OK:
		 Trace.log(e_errors.ERROR, "rename_volume failed: "+repr(ret))
		 
	 # create new record in the database
	 self.dict[new_label] = cur_rec
	 # remove current record from the database
	 del self.dict[old_label]
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
	     
	     for bfid in cur_rec['bfids']:
		 fcc.bfid = bfid
		 vm_ticket = fcc.get_volmap_name()
		 vol_map_name = vm_ticket["pnfs_mapname"]
		 (vm_dir,file) = os.path.split(vol_map_name)
		 ret = fcc.del_bfid()
		 os.remove(vol_map_name)
	     os.rmdir(vm_dir)
	     # remove current record from the database
	     del self.dict[external_label]
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
            Trace.trace(17,'remove_deleted_vols forked parent - returning')
            return
        vols = []
        try:
            Trace.init("REM_VOLS")
            Trace.trace(17,"remove_deleted_vols child processing")
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
            Trace.trace(17,'remove_deleted_vols child exitting')
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
            ticket["status"] = (e_errors.KEYERROR, \
                                "Volume Clerk: "+key+" key is missing")
            Trace.log(e_errors.WARNING, repr(ticket))
            self.reply_to_caller(ticket)
            Trace.trace(8,"addvol "+repr(ticket["status"]))
            return

        # can't have 2 with same external_label
        if self.dict.has_key(external_label):
            ticket["status"] = (errno.errorcode[errno.EEXIST], \
                                "Volume Clerk: volume "+external_label\
                               +" already exists")
            Trace.log(e_errors.INFO, repr(ticket))
            self.reply_to_caller(ticket)
            Trace.trace(8,"addvol "+repr(ticket["status"]))
            return

        # mandatory keys
        for key in  ['external_label','media_type', 'file_family', 'library',\
                     'eod_cookie', 'remaining_bytes', 'capacity_bytes' ]:
            try:
                record[key] = ticket[key]
            except KeyError:
                ticket["status"] = (e_errors.KEYERROR, \
                                    "Volume Clerk: "+key+" is missing")
                Trace.log(e_errors.INFO, repr(ticket))
                self.reply_to_caller(ticket)
                Trace.trace(8,"addvol "+repr(ticket["status"]))
                return

        # check if library key is valid library manager name
        llm = self.csc.get_library_managers(ticket)

        # "shelf" library is a special case
        if ticket['library']!='shelf' and not llm.has_key(ticket['library']):
            Trace.log(e_errors.INFO,
                      " vc.addvol: Library Manager does not exist: %s " 
                      % (ticket['library'],))

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
        record['wrapper'] = ticket.get('wrapper', "cpio_odc")
        record['non_del_files'] = ticket.get('non_del_files', 0)
        record['blocksize'] = ticket.get('blocksize', -1)
	record['bfids'] = []
        if record['blocksize'] == -1:
            sizes = self.csc.get("blocksizes")
            try:
                msize = sizes[ticket['media_type']]
            except:
                ticket['status'] = (e_errors.UNKNOWNMEDIA,
                                    "Volume Clerk: "+
                                    "unknown media type = unknown blocksize")
                Trace.log(e_errors.INFO, repr(ticket))
                self.reply_to_caller(ticket)
                Trace.trace(8,"addvol "+repr(ticket["status"]))
                return
            record['blocksize'] = msize

        # write the ticket out to the database
        self.dict[external_label] = record
        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        Trace.trace(10,'addvol ok '+repr(external_label)+" "+repr(record))
        return

    # delete a volume from the database
    def delvol(self, ticket):
        # everything is based on external label - make sure we have this
        key="external_label"
        try:
            external_label = ticket[key]
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
                                "Volume Clerk: "+key+" key is missing")
            Trace.log(e_errors.INFO, repr(ticket))
            self.reply_to_caller(ticket)
            Trace.trace(8,"delvol "+repr(ticket["status"]))
            return

        # get the current entry for the volume
        try:
            record = self.dict[external_label]
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
                                "Volume Clerk: volume "+external_label\
                               +" no such volume")
            Trace.log(e_errors.INFO, repr(ticket))
            self.reply_to_caller(ticket)
            Trace.trace(8,"delvol "+repr(ticket["status"]))
            return

        if record['at_mover'][0] != 'unmounted':
           ticket["status"] = (e_errors.CONFLICT,"volume must be unmounted")
           self.reply_to_caller(ticket)
           return
        if record.has_key('non_del_files'):
            if record['non_del_files']>0:
                ticket["status"] = (e_errors.CONFLICT,
                                    "Volume Clerk: volume "+external_label
                                    +" has "+repr(record['non_del_files'])+" active files")
                Trace.log(e_errors.INFO,
                          "Volume Clerk: volume "+external_label+" has "\
                          +repr(record['non_del_files'])+" active files")
                #Trace.log(e_errors.INFO, repr(ticket))
                self.reply_to_caller(ticket)
                Trace.trace(8,"delvol "+repr(ticket["status"]))
                return
        else:
            Trace.log(e_errors.INFO,"non_del_files not found in volume ticket - old version of table")

	# if volume has not been written delete it
	if record['sum_wr_access'] == 0:
	    del self.dict[external_label]
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
            ticket["status"] = (e_errors.KEYERROR, 
                                "Volume Clerk: %s key is missing"%(key,))
            Trace.log(e_errors.INFO, repr(ticket))
            self.reply_to_caller(ticket)
            return

        # get the current entry for the volume
	cl = external_label+".deleted"
        try:
            record = self.dict[cl]
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, 
                                "Volume Clerk: volume %s: no such volume"%(cl,))

            Trace.log(e_errors.INFO, repr(ticket))
            self.reply_to_caller(ticket)
            return

	status = self.rename_volume(cl, external_label, restore_vm)
	ticket["status"] = status
	if status[0] == e_errors.OK:
	    record = self.dict[external_label]
	    record["system_inhibit"] = ["none","none"]
	    if restore_vm == "yes":
		record["non_del_files"] = len(record["bfids"])
	    self.dict[external_label] = record

	    Trace.log(e_errors.INFO,"Volume %s is restored"%(external_label,))

        self.reply_to_caller(ticket)
        return

    # Check if volume is available
    def is_vol_available(self, ticket):
	work = ticket["action"]
	label = ticket["external_label"]
	record = self.dict[label]  ## was deepcopy
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
		    if (ticket['file_family'] == record['file_family'] and
			ticket['file_size'] <= record['remaining_bytes']):
			ret_stat = (e_errors.OK,None)
		    else:
			ret_stat = (e_errors.NOACCESS,None)
	    else:
		ret_stat = (e_errors.UNKNOWN,None)
	ticket['status'] = ret_stat
	self.reply_to_caller(ticket)

    # Get the next volume that satisfy criteria
    def next_write_volume (self, ticket):
        vol_veto = ticket["vol_veto_list"]
        vol_veto_list = eval(vol_veto)

        # get the criteria for the volume from the user's ticket
        min_remaining_bytes = ticket["min_remaining_bytes"]
        library = ticket["library"]
        file_family = ticket["file_family"]
        first_found = ticket["first_found"]
        wrapper_type = ticket["wrapper"]

        # go through the volumes and find one we can use for this request
        vol = {}
        self.dict.cursor("open")
        while 1:
            label,v = self.dict.cursor("next")
            if label:
                Trace.trace(17,'nwv '+label)
                pass
            else:
                break

            if v["library"] != library:
                Trace.trace(17,label+" rejected library "+v["library"]+' '+library)
                continue
            if v["file_family"] != file_family+"."+wrapper_type:
                Trace.trace(17,label+" rejected file_family "+v["file_family"]+' '+file_family+"."+wrapper_type)
                continue
            #if v["wrapper"] != wrapper_type:
            #    Trace.trace(17,label+" rejected wrapper "+v["wrapper"]+' '+wrapper_type)
            #    continue
            if v["user_inhibit"][0] != "none":
                Trace.trace(17,label+" rejected user_inhibit "+v["user_inhibit"][0])
                continue
            if v["user_inhibit"][1] != "none":
                Trace.trace(17,label+" rejected user_inhibit "+v["user_inhibit"][1])
                continue
            if v["system_inhibit"][0] != "none":
                Trace.trace(17,label+" rejected system_inhibit "+v["system_inhibit"][0])
                continue
            if v["system_inhibit"][1] != "none":
                Trace.trace(17,label+" rejected system_inhibit "+v["system_inhibit"][1])
                continue
            at_mover = v.get('at_mover',('unmounted', '')) # for backward compatibility for at_mover field
            if v['at_mover'][0] != "unmounted" and  v['at_mover'][0] != None: 
                Trace.trace(17,label+" rejected at_mover "+v['at_mover'][0])
                continue
            if v["remaining_bytes"] < long(min_remaining_bytes*SAFETY_FACTOR):
                # if it __ever__ happens that we can't write a file on a
                # volume, then mark volume as full.  This prevents us from
                # putting 1 byte files on old "golden" volumes and potentially
                # losing the entire tape. One could argue that a very large
                # file write could prematurely flag a volume as full, but lets
                # worry about if it is really a problem - I propose that an
                # administrator reset the system_inhibit back to none in these
                # special, and hopefully rare cases.
                Trace.trace(17,label+" rejected remaining_bytes"+str(v["remaining_bytes"]))
                v["system_inhibit"][1] = "full"
                left = v["remaining_bytes"]/1.
                totb = v["capacity_bytes"]/1.
                if totb != 0:
                    waste = left/totb*100.
                else:
                    waste = 0.
                Trace.log(e_errors.INFO,
                          "%s is now full, bytes remaining = %d, %.2f %%" % (label, v["remaining_bytes"],waste))
                self.dict.cursor("update",v)
                continue
            vetoed = 0
            for veto in vol_veto_list:
                if label == veto:
                    vetoed = 1
                    break
            if vetoed:
                Trace.trace(17,label+"rejected - in veto list")
                continue

            # supposed to return first volume found?
            if first_found:
                v["status"] = (e_errors.OK, None)
                Trace.trace(16,'next_write_vol label = '+ v['external_label'])
                self.reply_to_caller(v)
                self.dict.cursor("close")
                return
            # if not, is this an "earlier" volume that one we already found?
            if len(vol) == 0:
                Trace.trace(17,label+" ok")
                vol = v  ## was deepcopy
            elif v['declared'] < vol['declared']:
                Trace.trace(17,label+' ok')
                vol = v  ## was deepcopy
            else:
                Trace.trace(17,label+" rejected "+vol['external_label']+' declared eariler')
        self.dict.cursor("close")

        # return what we found
        if len(vol) != 0:
            vol["status"] = (e_errors.OK, None)
            Trace.trace(16,'next_write_vol label = '+ vol['external_label'])
            self.reply_to_caller(vol)
            return

        #========================================FIXME--- why go thru the list twice, this is slow ====================
        # nothing was available - see if we can assign a blank one.
        Trace.trace(16,'next_write_vol no vols available, checking for blanks')
        vol = {}
        self.dict.cursor("open")
        while 1:
            label,v = self.dict.cursor("next")
            if label:
                Trace.trace(17,'nwv '+label)
                pass
            else:
                break

            if v["library"] != library:
               Trace.trace(17,label+" rejected library "+v["library"]+' '+library)
               continue
            if v["file_family"] != "none":
                Trace.trace(17,label+" rejected file_family "+v["file_family"])
                continue
            if v["user_inhibit"][0] != "none":
                Trace.trace(17,label+" rejected user_inhibit "+v["user_inhibit"][0])
                continue
            if v["user_inhibit"][1] != "none":
                Trace.trace(17,label+" rejected user_inhibit "+v["user_inhibit"][1])
                continue
            if v["system_inhibit"][0] != "none":
                Trace.trace(17,label+" rejected system_inhibit "+v["system_inhibit"][0])
                continue
            if v["system_inhibit"][1] != "none":
                Trace.trace(17,label+" rejected system_inhibit "+v["system_inhibit"][1])
                continue
            at_mover = v.get('at_mover',('unmounted', '')) # for backward compatibility for at_mover field
            if v['at_mover'][0] != "unmounted" and  v['at_mover'][0] != None: 
                Trace.trace(17,label+" rejected at_mover "+v['at_mover'][0])
                continue
            if v["remaining_bytes"] < long(min_remaining_bytes*SAFETY_FACTOR):
                Trace.trace(17,label+" rejected remaining_bytes"+str(v["remaining_bytes"]))
                continue
            vetoed = 0
            for veto in vol_veto_list:
                if label == veto:
                    vetoed = 1
                    break
            if vetoed:
                Trace.trace(17,label+"rejected - in veto list")
                continue

            # supposed to return first blank volume found?
            if first_found:
                if file_family == "ephemeral":
                    file_family = label
                v["file_family"] = file_family+"."+wrapper_type
                v["wrapper"] = wrapper_type
                Trace.log(e_errors.INFO, "Assigning blank volume "+label+" to "+library+" "+file_family)
                self.dict[label] = v  ## was deepcopy
                v["status"] = (e_errors.OK, None)
                self.reply_to_caller(v)
                self.dict.cursor("close")
                return
            # if not, is this an "earlier" volume that one we already found?
            if len(vol) == 0:
                Trace.trace(17,label+" ok")
                vol = v  ## was deepcopy
            elif v['declared'] < vol['declared']:
                Trace.trace(17,label+" ok")
                vol = v  ## was deepcopy
            else:
                Trace.trace(17,label+" rejected "+vol['external_label']+' declared eariler')
        self.dict.cursor("close")

        # return blank volume we found
        if len(vol) != 0:
            label = vol['external_label']
            if file_family == "ephemeral":
                file_family = label
            vol["file_family"] = file_family+"."+wrapper_type
            vol["wrapper"] = wrapper_type
            Trace.log(e_errors.INFO,
                      "Assigning blank volume "+label+" to "+library+" "+file_family)
            self.dict[label] = vol  ## was deepcopy
            vol["status"] = (e_errors.OK, None)
            self.reply_to_caller(vol)
            return

        # nothing was available at all
        ticket["status"] = (e_errors.NOVOLUME, \
                            "Volume Clerk: no new volumes available")
        Trace.log(e_errors.ERROR,"No blank volumes "+str(ticket) )
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
         key = "file_family"
         file_family = ticket[key]
         key = "wrapper"
         wrapper_type = ticket[key]
         key = "external_label"
         external_label = ticket[key]
     except KeyError:
         ticket["status"] = (e_errors.KEYERROR, \
                             "Volume Clerk: "+key+" is missing")
         Trace.log(e_errors.INFO, repr(ticket))
         self.reply_to_caller(ticket)
         Trace.trace(8,"can_write_volume "+repr(ticket["status"]))
         return

     # get the current entry for the volume
     try:
         v = self.dict[external_label]  ## was deepcopy
         # for backward compatibility for at_mover field
         try:
             at_mover = v['at_mover']
         except KeyError:
             v['at_mover'] = ('unmounted', '')

         ticket["status"] = (e_errors.OK,'None')
         if (v["library"] == library and
             (v["file_family"] == file_family+"."+wrapper_type) and
             v["wrapper"] == wrapper_type and
             v["user_inhibit"][0] == "none" and
             v["user_inhibit"][1] == "none" and
             v["system_inhibit"][0] == "none" and
             v["system_inhibit"][1] == "none" and
             v['at_mover'][0] == "mounted"):
             if v["remaining_bytes"] < long(min_remaining_bytes*SAFETY_FACTOR):
                 # if it __ever__ happens that we can't write a file on a
                 # volume, then mark volume as full.  This prevents us from
                 # putting 1 byte files on old "golden" volumes and potentially
                 # losing the entire tape. One could argue that a very large
                 # file write could prematurely flag a volume as full, but lets
                 # worry about if it is really a problem - I propose that an
                 # administrator reset the system_inhibit back to none in these
                 # special, and hopefully rare cases.
                 v["system_inhibit"][1] = "full"
                 left = v["remaining_bytes"]/1.
                 totb = v["capacity_bytes"]/1.
                 if totb != 0:
                     waste = left/totb*100.
                 Trace.log(e_errors.INFO,
                           "%s is now full, bytes remaining = %d, %.2f %%" %
                           (external_label, v["remaining_bytes"],waste))
                 self.dict[external_label] = v  ## was deepcopy
                 ticket["status"] = (e_errors.WRITE_EOT, \
                                     "Volume Clerk: "+key+" is missing")
             self.reply_to_caller(ticket)
             Trace.trace(8,"can_write_volume "+repr(ticket["status"]))
             return
         else:
             ticket["status"] = (e_errors.NOACCESS,'None')
             self.reply_to_caller(ticket)
             Trace.trace(8,"can_write_volume "+repr(ticket["status"]))
             return
     except KeyError:
         ticket["status"] = (e_errors.KEYERROR, \
                             "Volume Clerk: volume "+external_label\
                             +" no such volume")
         Trace.log(e_errors.INFO, repr(ticket))
         self.reply_to_caller(ticket)
         Trace.trace(8,"can_write_volume "+repr(ticket["status"]))
         return


    # update the database entry for this volume
    def get_remaining_bytes(self, ticket):
        ticket['status'] = (e_errors.OK, None)
        # everything is based on external label - make sure we have this
        try:
            key="external_label"
            external_label = ticket[key]
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
                                "Volume Clerk: "+key+" key is missing")
            Trace.log(e_errors.INFO, repr(ticket))
            self.reply_to_caller(ticket)
            Trace.trace(8,"get_remaining_bytes "+repr(ticket["status"]))
            return

        # get the current entry for the volume
        try:
            record = self.dict[external_label]  ## was deepcopy
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
                                "Volume Clerk: volume "+external_label\
                               +" no such volume")
            Trace.log(e_errors.INFO, repr(ticket))
            self.reply_to_caller(ticket)
            Trace.trace(8,"get_remaining_bytes "+repr(ticket["status"]))
            return


        # access the remaining_byte field
        try:
            ticket["remaining_bytes"] = record["remaining_bytes"]
            Trace.trace(12,'get_remaining bytes '+key+'='+\
                        repr(record[key]))
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
                                "Volume Clerk: "+key+" key is missing")
            Trace.log(e_errors.INFO, repr(ticket))
            
        self.reply_to_caller(ticket)
        Trace.trace(8,"get_remaining_bytes "+repr(ticket["status"]))
        return


    # update the database entry for this volume
    def set_remaining_bytes(self, ticket):
        # everything is based on external label - make sure we have this
        try:
            key="external_label"
            external_label = ticket[key]
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
                                "Volume Clerk: "+key+" key is missing")
            Trace.log(e_errors.INFO, repr(ticket))
            self.reply_to_caller(ticket)
            Trace.trace(8,"set_remaining_bytes "+repr(ticket["status"]))
            return

        # get the current entry for the volume
        try:
            record = self.dict[external_label]  ## was deepcopy
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
                                "Volume Clerk: volume "+external_label\
                               +" no such volume")
            Trace.log(e_errors.INFO, repr(ticket))
            self.reply_to_caller(ticket)
            Trace.trace(8,"set_remaining_bytes "+repr(ticket["status"]))
            return

        # update the fields that have changed
        try:
            for key in ["remaining_bytes","eod_cookie"]:
                record[key] = ticket[key]
                Trace.trace(12,'set_remaining bytes '+key+'='+\
                            repr(record[key]))
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
                                "Volume Clerk: "+key+" key is missing")
            Trace.log(e_errors.INFO, repr(ticket))
            self.reply_to_caller(ticket)
            Trace.trace(8,"set_remaining_bytes "+repr(ticket["status"]))
            return

        record["system_inhibit"][0] = "none"
        record["last_access"] = time.time()
        if record["first_access"] == -1:
            record["first_access"] = record["last_access"]

        for key in ['wr_err','rd_err','wr_access','rd_access']:
            try:
                record['sum_'+key] = record['sum_'+key] + ticket[key]
                Trace.trace(12,'set_remaining_bytes '+key+'='+\
                            repr(ticket[key]))
            except KeyError:
                ticket["status"] = (e_errors.KEYERROR, \
                                    "Volume Clerk: "+key+" key is missing")
                Trace.log(e_errors.INFO, repr(ticket))
                self.reply_to_caller(ticket)
                Trace.trace(8,"set_remaining_bytes "+repr(ticket["status"]))
                return

        #TEMPORARY TRY BLOCK - all new volumes should already have the non_del_files key
        try:
            non_del_files = record['non_del_files']
        except KeyError:
            record['non_del_files'] = record['sum_wr_access']

        # update the non-deleted file count if we wrote to the tape
        # this key gets decremented when we delete files
        if not ticket.get('wr_err',0):
            record['non_del_files'] = record['non_del_files'] + \
                                      ticket['wr_access']

	bfid = ticket.get("bfid")
	if bfid: 
	    if not record.has_key("bfids"):
	    	record["bfids"] = []
	    record["bfids"].append(bfid)
        # record our changes
        self.dict[external_label] = record  ## was deepcopy
        record["status"] = (e_errors.OK, None)
        self.reply_to_caller(record)
        Trace.trace(12,'set_remaining_bytes '+repr(record))
        return

    # decrement the file count on the volume
    def decr_file_count(self, ticket):
        # everything is based on external label - make sure we have this
        try:
            key="external_label"
            external_label = ticket[key]
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
                                "Volume Clerk: "+key+" key is missing")
            Trace.log(e_errors.INFO, repr(ticket))
            self.reply_to_caller(ticket)
            Trace.trace(8,"decr_file_count "+repr(ticket["status"]))
            return

        # get the current entry for the volume
        try:
            record = self.dict[external_label]  ## was deepcopy
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
                                "Volume Clerk: volume "+external_label\
                               +" no such volume")
            Trace.log(e_errors.INFO, repr(ticket))
            self.reply_to_caller(ticket)
            Trace.trace(8,"decr_file_count "+repr(ticket["status"]))
            return

        # assume the count is 1 unless specified
        count = ticket.get("count",1)

        # decrement the number of non-deleted files on the tape
        record ["non_del_files"] = record["non_del_files"] - count
        self.dict[external_label] = record  ## was deepcopy # THIS WILL JOURNAL IT
        record["status"] = (e_errors.OK, None)
        self.reply_to_caller(record)
        Trace.trace(10,'decr_file_count '+repr(record))
        return

    # update the database entry for this volume
    def update_counts(self, ticket):
        # everything is based on external label - make sure we have this
        try:
            key="external_label"
            external_label = ticket[key]
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
                                "Volume Clerk: "+key+" key is missing")
            Trace.log(e_errors.INFO, repr(ticket))
            self.reply_to_caller(ticket)
            Trace.trace(8,"update_counts "+repr(ticket["status"]))
            return

        # get the current entry for the volume
        try:
            record = self.dict[external_label]  ## was deepcopy
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
                                "Volume Clerk: volume "+external_label\
                               +" no such volume")
            Trace.log(e_errors.INFO, repr(ticket))
            self.reply_to_caller(ticket)
            Trace.trace(8,"update_counts "+repr(ticket["status"]))
            return

        # update the fields that have changed
        record["last_access"] = time.time()
        if record["first_access"] == -1:
            record["first_access"] = record["last_access"]

        for key in ['wr_err','rd_err','wr_access','rd_access']:
            try:
                record['sum_'+key] = record['sum_'+key] + ticket[key]
                Trace.trace(12,'update_counts '+key+'='+\
                            repr(ticket[key]))
            except KeyError:
                ticket["status"] = (e_errors.KEYERROR, \
                                    "Volume Clerk: "+key+" key is missing")
                Trace.log(e_errors.INFO, repr(ticket))
                self.reply_to_caller(ticket)
                Trace.trace(8,"update_counts "+repr(ticket["status"]))
                return

        #TEMPORARY TRY BLOCK - all new volumes should already have the non_del_files key
        try:
            non_del_files = record['non_del_files']
        except KeyError:
            record['non_del_files'] = record['sum_wr_access']

        # update the non-deleted file count if we wrote to the tape
        # this key gets decremented when we delete files
        record['non_del_files'] = record['non_del_files'] + ticket['wr_access']

        # record our changes
        self.dict[external_label] = record  ## was deepcopy
        record["status"] = (e_errors.OK, None)
        self.reply_to_caller(record)
        Trace.trace(12,'update_counts ok '+repr(record))
        return

    # get the current database volume about a specific entry
    def inquire_vol(self, ticket):
        # everything is based on external label - make sure we have this
        try:
            key="external_label"
            external_label = ticket[key]
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
                                "Volume Clerk: "+key+" key is missing")
            Trace.log(e_errors.INFO, repr(ticket))
            self.reply_to_caller(ticket)
            Trace.trace(8,"inquire_vol "+repr(ticket["status"]))
            return

        # get the current entry for the volume
        try:
            record = self.dict[external_label]  ## was deepcopy
            record["status"] = e_errors.OK, None
            self.reply_to_caller(record)
            Trace.trace(12,'inquire_vol '+repr(record))
            return
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
                                "Volume Clerk: volume "+external_label\
                               +" no such volume")
            Trace.log(e_errors.INFO, repr(ticket))
            self.reply_to_caller(ticket)
            Trace.trace(8,"inquire_vol "+repr(ticket["status"]))
            return

    # update dB to reflect actual state of volume
    def update_mc_state(self, ticket):
        # everything is based on external label - make sure we have this
        if 'external_label' not in ticket.keys():
            ticket["status"] = (e_errors.KEYERROR, 
                                "Volume Clerk: external label is missing")
            Trace.log(e_errors.INFO, repr(ticket))
            self.reply_to_caller(ticket)
            Trace.trace(8,"vc.update_mc_state "+repr(ticket["status"]))
            return
        external_label = ticket['external_label']
        # get the current entry for the volume
        try:
            record = self.dict[external_label]  ## was deepcopy
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, 
                                "Volume Clerk: volume "+external_label
                                +" no such volume")
            Trace.log(e_errors.INFO, repr(ticket))
            self.reply_to_caller(ticket)
            Trace.trace(8,"vc.update_mc_state "+repr(ticket["status"]))
            return

        # update the fields that have changed
        ll = list(record['at_mover'])
        ll[0]= self.get_media_changer_state(record["library"],
					    record["external_label"],
                                            record["media_type"])

        record['at_mover']=tuple(ll)
        # if volume is unmounted system_inhibit cannot be writing
        if (record['at_mover'][0] == 'unmounted' and
            record['system_inhibit'][0] == 'writing'):
            record['system_inhibit'][0] = "none"
        self.dict[external_label] = record  ## was deepcopy # THIS WILL JOURNAL IT
        record["status"] = (e_errors.OK, None)
        self.reply_to_caller(record)
        Trace.trace(10,'vc.update_mc_state '+repr(record))
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
            ticket["status"] = (e_errors.KEYERROR, \
                                "Volume Clerk: "+key+" key is missing")
            Trace.log(e_errors.INFO, repr(ticket))
            self.reply_to_caller(ticket)
            Trace.trace(8,"vc.clr_system_inhibit "+repr(ticket["status"]))
            return

        # get the current entry for the volume
        try:
            record = self.dict[external_label]  ## was deepcopy
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
                                "Volume Clerk: volume "+external_label\
                               +" no such volume")
            Trace.log(e_errors.INFO, repr(ticket))
            self.reply_to_caller(ticket)
            Trace.trace(8,"vc.clr_system_inhibit "+repr(ticket["status"]))
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
                ll[0]= self.get_media_changer_state(record["library"],
                                                    record["external_label"], 
                                                    record["media_type"])
                record['at_mover']=tuple(ll)
                self.dict[external_label] = record  ## was deepcopy # THIS WILL JOURNAL IT
                record["status"] = (e_errors.OK, None)
        else:
            # if it is not record["system_inhibit"][0] just set it to none
            record[inhibit][position] = "none"
	    self.dict[external_label] = record  ## was deepcopy # THIS WILL JOURNAL IT
	    record["status"] = (e_errors.OK, None)
        self.reply_to_caller(record)
        Trace.trace(10,'vc.clr_system_inhibit '+repr(record))
        return

    # get the actual state of the media changer
    def get_media_changer_state(self, lib, volume, m_type):
        m_changer = self.csc.get_media_changer(lib + ".library_manager")
        if not m_changer:
            Trace.trace(8," vc.get_media_changer_state: ERROR: no media changer found %s" % (volume,))
            return 'unknown'
            
        import media_changer_client
        mcc = media_changer_client.MediaChangerClient(self.csc, m_changer )
        stat = mcc.viewvol(volume, m_type)["status"][3]

        if 'O' == stat :
            state = 'unmounted'
        elif 'M' == stat :
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
            ticket["status"] = (e_errors.KEYERROR, \
                                "Volume Clerk: "+key+" key is missing")
            Trace.log(e_errors.INFO, repr(ticket))
            self.reply_to_caller(ticket)
            Trace.trace(8,"add_at_mover "+repr(ticket["status"]))
            return

        # get the current entry for the volume
        try:
            record = self.dict[external_label]  ## was deepcopy
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
                                "Volume Clerk: volume "+external_label\
                               +" no such volume")
            Trace.log(e_errors.INFO, repr(ticket))
            self.reply_to_caller(ticket)
            Trace.trace(8,"add_at_mover "+repr(ticket["status"]))
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

        self.dict[external_label] = record  ## was deepcopy # THIS WILL JOURNAL IT
        record["status"] = (e_errors.OK, None)
        self.reply_to_caller(record)
        Trace.trace(10,'add_at_mover '+repr(record))
        return

    # move a volume to a new library
    def new_library(self, ticket):
	external_label = ticket["external_label"]
	new_library = ticket["new_library"]

	# get the current entry for the volume
	record = self.dict[external_label]  ## was deepcopy
	
	# update the library field with the new library
	record ["library"] = new_library
	self.dict[external_label] = record  ## was deepcopy # THIS WILL JOURNAL IT
	record["status"] = (e_errors.OK, None)
	self.reply_to_caller(record)
	Trace.trace(16,external_label+" moved to library "+new_library)
	return

    # set system_inhibit flag
    def set_system_inhibit(self, ticket, flag, index=0):
	external_label = ticket["external_label"]
	# get the current entry for the volume
	record = self.dict[external_label]  ## was deepcopy

	# update the fields that have changed
	record["system_inhibit"][index] = flag

	self.dict[external_label] = record  ## was deepcopy # THIS WILL JOURNAL IT
	record["status"] = (e_errors.OK, None)
	Trace.log(e_errors.INFO,external_label+" system inhibit set to "+flag)
	self.reply_to_caller(record)
	return

    # set system_inhibit flag, flag the database that we are now writing the system
    def set_writing(self, ticket):
        return self.set_system_inhibit(ticket, "writing")

    # flag that the current volume is readonly
    def set_system_readonly(self, ticket):
        return self.set_system_inhibit(ticket, "readonly", 1)

    # flag that the current volume is marked as noaccess
    def set_system_noaccess(self, ticket):
        Trace.alarm(e_errors.WARNING, e_errors.NOACCESS,{"label":ticket["external_label"]}) 
        return self.set_system_inhibit(ticket, e_errors.NOACCESS)

    # device is broken - what to do, what to do ===================================FIXME======================================
    def set_hung(self,ticket):
	Trace.trace(16,'set_hung')
	self.reply_to_caller({"status" : (e_errors.OK, None)})
	return

    # set at_mover flag
    def set_at_mover(self, ticket):
	external_label = ticket["external_label"]
	record = self.dict[external_label]  ## was deepcopy
	at_mover = record.get('at_mover',('unmounted','none'))
	
	# update the fields that have changed
	if ticket['force']:
	    wrong_state = 0
	else:
	    wrong_state = 1
	    if (ticket['at_mover'][0] == 'mounting' and
		record['at_mover'][0] != 'unmounted'):
		pass
	    elif (ticket['at_mover'][0] == 'mounted' and
		  record['at_mover'][0] != 'mounting'):
		pass
	    elif (ticket['at_mover'][0] == 'unmounting' and
		  record['at_mover'][0] != 'mounted'):
		pass
	    elif (ticket['at_mover'][0] == 'unmounted' and
		  record['at_mover'][0] != 'unmounting'):
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
	    self.dict[external_label] = record  ## was deepcopy # THIS WILL JOURNAL IT
	    record["status"] = (e_errors.OK, None)
	self.reply_to_caller(record)
	Trace.trace(16,external_label+" state now "+str(ticket['at_mover']))
	return

    # return all the volumes in our dictionary.  Not so useful!
    def get_vols(self,ticket):
        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)

        # this could tie things up for awhile - fork and let child
        # send the work list (at time of fork) back to client
        if self.fork() != 0:
            Trace.trace(17,'get_vols forked parent - returning')
            return
        try:
            import cPickle
            Trace.init("GET_VOLS")
            Trace.trace(17,"get_vols child processing")
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
                    bytes_left = value['remaining_bytes']*1./1024./1024./1024.
                    dict = {"volume"         : key,
                            "bytes_left"     : bytes_left,
                            "at_mover"       : value['at_mover'],
                            "system_inhibit" : value['system_inhibit'],
                            "user_inhibit"   : value['user_inhibit'],
                            "library"        : value['library'],
                            "file_family"    : value['file_family']
                            }
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
            Trace.trace(17,'get_vols child exitting')
        except:
            exc, msg, tb = sys.exc_info()
            print exc, msg
            status = str(exc), str(msg)
            Trace.log(e_errors.ERROR,"get_vols child %s"%(status,))
        os._exit(0)


    # get a port for the data transfer
    # tell the user I'm your volume clerk and here's your ticket
    def get_user_sockets(self, ticket):
        try:
            volume_clerk_host, volume_clerk_port, listen_socket = callback.get_callback()
            Trace.trace(16,'get_user_sockets= %s %s'%(volume_clerk_host,volume_clerk_port))
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
            status = str(exc), str(msg)
            Trace.log(e_errors.ERROR,"get_user_sockets %s"%(status,))

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
            exc,msg,tb=sys.exc_info()
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
        self.dict = db.DbTable("volume", dbHome, jouHome, [])
        Trace.log(e_errors.INFO,"hurrah, volume database is open")
        
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
        except:
            vc.serve_forever_error(vc.log_name)
            continue
    Trace.log(e_errors.ERROR,"Volume Clerk finished (impossible)")
