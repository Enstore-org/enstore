###############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
import sys
import os
import time
import copy
import errno
import string

# enstore imports
import callback
import dispatching_worker
import generic_server
import db
import Trace
import e_errors

# require 5% more space on a tape than the file size,
#    this accounts for the wrapper overhead and "some" tape rewrites
SAFETY_FACTOR=1.05

MY_NAME = "volume_clerk"

class VolumeClerkMethods(dispatching_worker.DispatchingWorker):

    # add: some sort of hook to keep old versions of the s/w out
    # since we should like to have some control over format of the records.
    def addvol(self, ticket):
     try:
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
        if dict.has_key(external_label):
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
                      " vc.addvol: Library Manager does not exist: %s " \
                      % ticket['library'])

        # optional keys - use default values if not specified
        record['last_access'] = ticket.get('last_access', -1)
        record['first_access'] = ticket.get('first_access', -1)
        record['declared'] = ticket.get('declared',-1)
        if record['declared'] == -1:
            record["declared"] = time.time()
        record['system_inhibit'] = ticket.get('system_inhibit', "none")
        record['at_mover'] = ticket.get('at_mover',  ("unmounted", "none") )
        record['user_inhibit'] = ticket.get('user_inhibit', "none")
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
        dict[external_label] = record
        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        Trace.trace(10,'addvol ok '+repr(external_label)+" "+repr(record))
        return

     # even if there is an error - respond to caller so he can process it
     except:
         ticket["status"] = (str(sys.exc_info()[0]), str(sys.exc_info()[1]))
         Trace.log(e_errors.INFO, repr(ticket))
         self.reply_to_caller(ticket)
         return


    # delete a volume from the database
    def delvol(self, ticket):
     try:
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
            record = copy.deepcopy(dict[external_label])
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
                                "Volume Clerk: volume "+external_label\
                               +" no such volume")
            Trace.log(e_errors.INFO, repr(ticket))
            self.reply_to_caller(ticket)
            Trace.trace(8,"delvol "+repr(ticket["status"]))
            return

        if record.has_key('non_del_files'):
            force = ticket.get("force",0)
            if record['non_del_files']>0 and not force:
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


        # delete if from the database
	del dict[external_label]
	ticket["status"] = (e_errors.OK, None)
	Trace.trace(10,'delvol ok '+repr(external_label))

        self.reply_to_caller(ticket)
        return

     # even if there is an error - respond to caller so he can process it
     except:
         Trace.trace(8,"delvol "+str(sys.exc_info()[0])+\
                     str(sys.exc_info()[1]))
         ticket["status"] = (str(sys.exc_info()[0]), str(sys.exc_info()[1]))
         Trace.log(e_errors.INFO, repr(ticket))
         self.reply_to_caller(ticket)
         return


    # Check if volume is available
    def is_vol_available(self, ticket):
     try:
	 work = ticket["action"]
	 label = ticket["external_label"]
	 record = copy.deepcopy(dict[label])
	 if record["system_inhibit"] == e_errors.DELETED:
	    ret_stat = (record["system_inhibit"],None)
	 else:
	     if work == 'read_from_hsm':
		 # if system_inhibit is NOT in one of the following 
		 # states it is NOT available for reading
		 if (record['system_inhibit'] != 'none' and 
		     record['system_inhibit'] != 'readonly' and
		     record['system_inhibit'] != 'full'):
		     ret_stat = (record['system_inhibit'], None)
		 # if user_inhibit is NOT in one of the following 
		 # states it is NOT available for reading
		 elif (record['user_inhibit'] != 'none' and
		       record['user_inhibit'] != 'readonly' and
		       record['user_inhibit'] != 'full'):
		     ret_stat = (record['system_inhibit'], None)
		     ticket['status'] = (e_errors.OK,None)
		 else:
		     ret_stat = (e_errors.OK,None)
	     elif work == 'write_to_hsm':

		 if record['system_inhibit'] != 'none':
		     ret_stat = (record['system_inhibit'], None)
		 elif record['user_inhibit'] != 'none':
		     ret_stat = (record['user_inhibit'], None)
		 else:
		     if (ticket['file_family'] == record['file_family'] and
			 ticket['file_size'] <= record['remaining_bytes']):
			 ret_stat = (e_errors.OK,None)
		     else:
			 ret_stat = (e_errors.NOACCESS,None)
	     else:
		 ret_stat = (e_errors.UNKNOWN,None)
     except:
         ret_stat = (str(sys.exc_info()[0]), str(sys.exc_info()[1]))
         Trace.log(e_errors.ERROR,"is_vol_available "+repr(ret_stat))
     ticket['status'] = ret_stat
     self.reply_to_caller(ticket)
		


    # Get the next volume that satisfy criteria
    def next_write_volume (self, ticket):
     try:
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
        dict.cursor("open")
        while 1:
            label,v = dict.cursor("next")
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
            if v["user_inhibit"] != "none":
                Trace.trace(17,label+" rejected user_inhibit "+v["user_inhibit"])
                continue
            if v["system_inhibit"] != "none":
                Trace.trace(17,label+" rejected system_inhibit "+v["system_inhibit"])
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
                v["system_inhibit"] = "full"
                left = v["remaining_bytes"]/1.
                totb = v["capacity_bytes"]/1.
                if totb != 0:
                    waste = left/totb*100.
                else:
                    waste = 0.
                Trace.log(e_errors.INFO,
                          "%s is now full, bytes remaining = %d, %.2f %%" % (label, v["remaining_bytes"],waste))
                dict.cursor("update",v)
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
                dict.cursor("close")
                return
            # if not, is this an "earlier" volume that one we already found?
            if len(vol) == 0:
                Trace.trace(17,label+" ok")
                vol = copy.deepcopy(v)
            elif v['declared'] < vol['declared']:
                Trace.trace(17,label+' ok')
                vol = copy.deepcopy(v)
            else:
                Trace.trace(17,label+" rejected "+vol['external_label']+' declared eariler')
        dict.cursor("close")

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
        dict.cursor("open")
        while 1:
            label,v = dict.cursor("next")
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
            if v["user_inhibit"] != "none":
                Trace.trace(17,label+" rejected user_inhibit "+v["user_inhibit"])
                continue
            if v["system_inhibit"] != "none":
                Trace.trace(17,label+" rejected system_inhibit "+v["system_inhibit"])
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
                dict[label] = copy.deepcopy(v)
                v["status"] = (e_errors.OK, None)
                self.reply_to_caller(v)
                dict.cursor("close")
                return
            # if not, is this an "earlier" volume that one we already found?
            if len(vol) == 0:
                Trace.trace(17,label+" ok")
                vol = copy.deepcopy(v)
            elif v['declared'] < vol['declared']:
                Trace.trace(17,label+" ok")
                vol = copy.deepcopy(v)
            else:
                Trace.trace(17,label+" rejected "+vol['external_label']+' declared eariler')
        dict.cursor("close")

        # return blank volume we found
        if len(vol) != 0:
            label = vol['external_label']
            if file_family == "ephemeral":
                file_family = label
            vol["file_family"] = file_family+"."+wrapper_type
            vol["wrapper"] = wrapper_type
            Trace.log(e_errors.INFO,
                      "Assigning blank volume "+label+" to "+library+" "+file_family)
            dict[label] = copy.deepcopy(vol)
            vol["status"] = (e_errors.OK, None)
            self.reply_to_caller(vol)
            return

        # nothing was available at all
        ticket["status"] = (e_errors.NOVOLUME, \
                            "Volume Clerk: no new volumes available")
        Trace.log(e_errors.ERROR,"No blank volumes "+str(ticket) )
        self.reply_to_caller(ticket)
        return

     # even if there is an error - respond to caller so he can process it
     except:
         ticket["status"] = (str(sys.exc_info()[0]), str(sys.exc_info()[1]))
         Trace.log(e_errors.ERROR,"next_write_vol "+repr(ticket["status"]))
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
         v = copy.deepcopy(dict[external_label])
         # for backward compatibility for at_mover field
         try:
             at_mover = v['at_mover']
         except KeyError:
             v['at_mover'] = ('unmounted', '')

         ticket["status"] = (e_errors.OK,'None')
         if (v["library"] == library and
             (v["file_family"] == file_family+"."+wrapper_type) and
             v["wrapper"] == wrapper_type and
             v["user_inhibit"] == "none" and
             v["system_inhibit"] == "none" and
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
                 v["system_inhibit"] = "full"
                 left = v["remaining_bytes"]/1.
                 totb = v["capacity_bytes"]/1.
                 if totb != 0:
                     waste = left/totb*100.
                 Trace.log(e_errors.INFO,
                           "%s is now full, bytes remaining = %d, %.2f %%" %
                           (external_label, v["remaining_bytes"],waste))
                 dict[external_label] = copy.deepcopy(v)
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
    def set_remaining_bytes(self, ticket):
     try:
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
            record = copy.deepcopy(dict[external_label])
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

        record["system_inhibit"] = "none"
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
        record['non_del_files'] = record['non_del_files'] + ticket['wr_access']

	bfid = ticket.get("bfid")
	if bfid: record["bfids"].append(bfid)
        # record our changes
        dict[external_label] = copy.deepcopy(record)
        record["status"] = (e_errors.OK, None)
        self.reply_to_caller(record)
        Trace.trace(12,'set_remaining_bytes '+repr(record))
        return

     # even if there is an error - respond to caller so he can process it
     except:
         ticket["status"] = (str(sys.exc_info()[0]), str(sys.exc_info()[1]))
         Trace.log(e_errors.INFO, repr(ticket))
         self.reply_to_caller(ticket)
         Trace.trace(8,"set_remaining_bytes "+repr(ticket["status"]))
         return


    # decrement the file count on the volume
    def decr_file_count(self, ticket):
     try:
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
            record = copy.deepcopy(dict[external_label])
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
	cc=record ["non_del_files"]
        record ["non_del_files"] = record["non_del_files"] - count
	# if file count is 0 declare it deleted
	if record ["non_del_files"] == 0:
	    record["system_inhibit"] = e_errors.DELETED
	# if count was 0 and then went up reset system inhibit
	elif cc == 0 and record ["non_del_files"] == 1:
	    record["system_inhibit"] = "none"
        dict[external_label] = copy.deepcopy(record) # THIS WILL JOURNAL IT
        record["status"] = (e_errors.OK, None)
        self.reply_to_caller(record)
        Trace.trace(10,'decr_file_count '+repr(record))
        return

     # even if there is an error - respond to caller so he can process it
     except:
         ticket["status"] = (str(sys.exc_info()[0]), str(sys.exc_info()[1]))
         Trace.log(e_errors.INFO, repr(ticket))
         self.reply_to_caller(ticket)
         Trace.trace(8,"decr_file_count "+repr(ticket["status"]))
         return


    # update the database entry for this volume
    def update_counts(self, ticket):
     try:
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
            record = copy.deepcopy(dict[external_label])
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
        dict[external_label] = copy.deepcopy(record)
        record["status"] = (e_errors.OK, None)
        self.reply_to_caller(record)
        Trace.trace(12,'update_counts ok '+repr(record))
        return

     # even if there is an error - respond to caller so he can process it
     except:
         ticket["status"] = (str(sys.exc_info()[0]), str(sys.exc_info()[1]))
         Trace.log(e_errors.INFO, repr(ticket))
         self.reply_to_caller(ticket)
         Trace.trace(8,"update_counts "+repr(ticket["status"]))
         return


    # get the current database volume about a specific entry
    def inquire_vol(self, ticket):
     try:
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
            record = copy.deepcopy(dict[external_label])
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

     # even if there is an error - respond to caller so he can process it
     except:
         ticket["status"] = (str(sys.exc_info()[0]), str(sys.exc_info()[1]))
         Trace.log(e_errors.INFO, repr(ticket))
         self.reply_to_caller(ticket)
         Trace.trace(8,"inquire_vol "+repr(ticket["status"]))
         return


    # flag the database that we are now writing the system
    def update_mc_state(self, ticket):
     try:
        # everything is based on external label - make sure we have this
        try:
            key="external_label"
            external_label = ticket[key]
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
                                "Volume Clerk: "+key+" key is missing")
            Trace.log(e_errors.INFO, repr(ticket))
            self.reply_to_caller(ticket)
            Trace.trace(8,"vc.update_mc_state "+repr(ticket["status"]))
            return

        # get the current entry for the volume
        try:
            record = copy.deepcopy(dict[external_label])
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
                                "Volume Clerk: volume "+external_label\
                               +" no such volume")
            Trace.log(e_errors.INFO, repr(ticket))
            self.reply_to_caller(ticket)
            Trace.trace(8,"vc.update_mc_state "+repr(ticket["status"]))
            return

        # update the fields that have changed
        ll = list(record['at_mover'])
        ll[0]= self.get_media_changer_state(record["library"],
                                  record["external_label"], record["media_type"])
        record['at_mover']=tuple(ll)
        dict[external_label] = copy.deepcopy(record) # THIS WILL JOURNAL IT
        record["status"] = (e_errors.OK, None)
        self.reply_to_caller(record)
        Trace.trace(10,'vc.update_mc_state '+repr(record))
        return

     # even if there is an error - respond to caller so he can process it
     except:
         ticket["status"] = (str(sys.exc_info()[0]), str(sys.exc_info()[1]))
         Trace.log(e_errors.INFO, repr(ticket))
         self.reply_to_caller(ticket)
         Trace.trace(8,"vc.update_mc_state "+repr(ticket["status"]))
         return


    # flag the database that we are now writing the system
    def clr_system_inhibit(self, ticket):
     try:
        # everything is based on external label - make sure we have this
        try:
            key="external_label"
            external_label = ticket[key]
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
                                "Volume Clerk: "+key+" key is missing")
            Trace.log(e_errors.INFO, repr(ticket))
            self.reply_to_caller(ticket)
            Trace.trace(8,"vc.clr_system_inhibit "+repr(ticket["status"]))
            return

        # get the current entry for the volume
        try:
            record = copy.deepcopy(dict[external_label])
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
                                "Volume Clerk: volume "+external_label\
                               +" no such volume")
            Trace.log(e_errors.INFO, repr(ticket))
            self.reply_to_caller(ticket)
            Trace.trace(8,"vc.clr_system_inhibit "+repr(ticket["status"]))
            return

        # update the fields that have changed
        record ["system_inhibit"] = "none"
        ll = list(record['at_mover'])
        ll[0]= self.get_media_changer_state(record["library"],
                                  record["external_label"], record["media_type"])
        record['at_mover']=tuple(ll)
        dict[external_label] = copy.deepcopy(record) # THIS WILL JOURNAL IT
        record["status"] = (e_errors.OK, None)
        self.reply_to_caller(record)
        Trace.trace(10,'vc.clr_system_inhibit '+repr(record))
        return

     # even if there is an error - respond to caller so he can process it
     except:
         ticket["status"] = (str(sys.exc_info()[0]), str(sys.exc_info()[1]))
         Trace.log(e_errors.INFO, repr(ticket))
         self.reply_to_caller(ticket)
         Trace.trace(8,"vc.clr_system_inhibit "+repr(ticket["status"]))
         return


    # get the actual state of the media changer
    def get_media_changer_state(self, libMgr, volume, m_type):
     import library_manager_client
     lmc = library_manager_client.LibraryManagerClient(self.csc,
                                                     libMgr+".library_manager")
     mchgr = lmc.get_mc()      # return media changer
     del lmc
     if None != mchgr :
         import media_changer_client
         mcc = media_changer_client.MediaChangerClient(self.csc, mchgr )
         del mchgr
         vol_ticket = {'external_label' : volume,
                       'media_type' : m_type
                      }
         mc_ticket = {'work' : 'viewvol',
                      'vol_ticket' : vol_ticket
                     }
         stat = mcc.viewvol(mc_ticket)["status"][3]
         del mcc
         if 'O' == stat :
           state = 'unmounted'
         elif 'M' == stat :
           state = 'mounted'
         else :
           state = stat
     else :
         #print "vc.get_media_changer_state: ERROR: no media changer found" \
         #       +repr(volume)
         Trace.trace(8," vc.get_media_changer_state: ERROR: no media changer found "
                     +repr(volume))
         return 'unknown'
     return state


    # for the backward compatibility D0_TEMP
    # flag the database that we are now writing the system
    def add_at_mover(self, ticket):
     try:
        # everything is based on external label - make sure we have this
        try:
            key="external_label"
            external_label = ticket[key]
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
                                "Volume Clerk: "+key+" key is missing")
            Trace.log(e_errors.INFO, repr(ticket))
            self.reply_to_caller(ticket)
            Trace.trace(8,"cadd_at_mover "+repr(ticket["status"]))
            return

        # get the current entry for the volume
        try:
            record = copy.deepcopy(dict[external_label])
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

        dict[external_label] = copy.deepcopy(record) # THIS WILL JOURNAL IT
        record["status"] = (e_errors.OK, None)
        self.reply_to_caller(record)
        Trace.trace(10,'add_at_mover '+repr(record))
        return

     # even if there is an error - respond to caller so he can process it
     except:
         ticket["status"] = (str(sys.exc_info()[0]), str(sys.exc_info()[1]))
         Trace.log(e_errors.INFO, repr(ticket))
         self.reply_to_caller(ticket)
         Trace.trace(8,"add_at_mover "+repr(ticket["status"]))
         return
    # END D0_TEMP


    # move a volume to a new library
    def new_library(self, ticket):
        try:
            external_label = ticket["external_label"]
            new_library = ticket["new_library"]

            # get the current entry for the volume
            record = copy.deepcopy(dict[external_label])

            # update the library field with the new library
            record ["library"] = new_library
            dict[external_label] = copy.deepcopy(record) # THIS WILL JOURNAL IT
            record["status"] = (e_errors.OK, None)
            self.reply_to_caller(record)
            Trace.trace(16,external_label+" moved to library "+new_library)
            return

        # even if there is an error - respond to caller so he can process it
        except:
            ticket["status"] = (str(sys.exc_info()[0]), str(sys.exc_info()[1]))
            Trace.log(e_errors.ERROR,"new_library "+repr(ticket["status"]))
            self.reply_to_caller(ticket)
            return


    # set system_inhibit flag
    def set_system_inhibit(self, ticket, flag):
        try:
            external_label = ticket["external_label"]

            # get the current entry for the volume
            record = copy.deepcopy(dict[external_label])

            # update the fields that have changed
            record ["system_inhibit"] = flag
            dict[external_label] = copy.deepcopy(record) # THIS WILL JOURNAL IT
            record["status"] = (e_errors.OK, None)
            Trace.trace(16,external_label+" system inhibit set to "+flag)
            self.reply_to_caller(record)
            return

        # even if there is an error - respond to caller so he can process it
        except:
            ticket["status"] = (str(sys.exc_info()[0]), str(sys.exc_info()[1]))
            Trace.log(e_errors.ERROR,"set_system_inhibit "+repr(ticket["status"]))
            self.reply_to_caller(ticket)
            return

    # set system_inhibit flag, flag the database that we are now writing the system
    def set_writing(self, ticket):
        return self.set_system_inhibit(ticket, "writing")

    # flag that the current volume is readonly
    def set_system_readonly(self, ticket):
        return self.set_system_inhibit(ticket, "readonly")

    # flag that the current volume is marked as noaccess
    def set_system_noaccess(self, ticket):
        return self.set_system_inhibit(ticket, e_errors.NOACCESS)

    # device is broken - what to do, what to do ===================================FIXME======================================
    def set_hung(self,ticket):
        try:
            Trace.trace(16,'set_hung')
            self.reply_to_caller({"status" : (e_errors.OK, None)})
            return

        # even if there is an error - respond to caller so he can process it
        except:
            ticket["status"] = (str(sys.exc_info()[0]), str(sys.exc_info()[1]))
            Trace.log(e_errors.ERROR,"set_system_readonly "+repr(ticket["status"]))
            self.reply_to_caller(ticket)
            return


    # set at_mover flag
    def set_at_mover(self, ticket):
        try:
            external_label = ticket["external_label"]
            record = copy.deepcopy(dict[external_label])
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
                dict[external_label] = copy.deepcopy(record) # THIS WILL JOURNAL IT
                record["status"] = (e_errors.OK, None)
            self.reply_to_caller(record)
            Trace.trace(16,external_label+" state now "+str(ticket['at_mover']))
            return

        # even if there is an error - respond to caller so he can process it
        except:
            ticket["status"] = (str(sys.exc_info()[0]), str(sys.exc_info()[1]))
            Trace.log(e_errors.ERROR,"set_at_mover "+repr(ticket["status"]))
            self.reply_to_caller(ticket)
            return

    # return all the volumes in our dictionary.  Not so useful!
    def get_vols(self,ticket):
        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)

        # this could tie things up for awhile - fork and let child
        # send the work list (at time of fork) back to client
        if os.fork() != 0:
            Trace.trace(17,'get_vols forked parent - returning')
            return
        try:
            Trace.init("GET_VOLS")
            Trace.trace(17,"get_vols child processing")
            self.get_user_sockets(ticket)
            ticket["status"] = (e_errors.OK, None)
            callback.write_tcp_obj(self.data_socket, ticket)
            dict.cursor("open")
            key,value=dict.cursor("first")
            while key:
                callback.write_tcp_raw(self.data_socket,repr(key))
                key,value=dict.cursor("next")
            callback.write_tcp_raw(self.data_socket,"")
            dict.cursor("close")
            self.data_socket.close()

            callback.write_tcp_obj(self.control_socket, ticket)
            self.control_socket.close()
            Trace.trace(17,'get_vols child exitting')
        except:
            print "EXCEPTION"
            status = (str(sys.exc_info()[0]), str(sys.exc_info()[1]))
            Trace.log(e_errors.ERROR,"get_vols child "+repr(status))
        os._exit(0)


    # get a port for the data transfer
    # tell the user I'm your volume clerk and here's your ticket
    def get_user_sockets(self, ticket):
        try:
            volume_clerk_host, volume_clerk_port, listen_socket = callback.get_callback()
            Trace.trace(16,'get_user_sockets='+repr((volume_clerk_host,volume_clerk_port)))
            listen_socket.listen(4)
            ticket["volume_clerk_callback_host"] = volume_clerk_host
            ticket["volume_clerk_callback_port"] = volume_clerk_port
            self.control_socket = callback.user_callback_socket(ticket)
            data_socket, address = listen_socket.accept()
            self.data_socket = data_socket
            listen_socket.close()
        # catch any error and keep going. server needs to be robust
        except:
            status = (str(sys.exc_info()[0]), str(sys.exc_info()[1]))
            Trace.log(e_errors.ERROR,"get_user_sockets "+repr(status))


    def start_backup(self,ticket):
        try:
            Trace.log(e_errors.INFO,"start_backup")
            dict.start_backup()
            self.reply_to_caller({"status"        : (e_errors.OK, None),
                                  "start_backup"  : 'yes' })
        # catch any error and keep going. server needs to be robust
        except:
            status = (str(sys.exc_info()[0]), str(sys.exc_info()[1]))
            Trace.log(e_errors.ERROR,"start_backup "+repr(status))
            self.reply_to_caller({"status"       : status,
                                  "start_backup" : 'no' })


    def stop_backup(self,ticket):
        try:
            Trace.log(e_errors.INFO,"stop_backup")
            dict.stop_backup()
            self.reply_to_caller({"status"       : (e_errors.OK, None),
                                  "stop_backup"  : 'yes' })
        # catch any error and keep going. server needs to be robust
        except:
            status = (str(sys.exc_info()[0]), str(sys.exc_info()[1]))
            Trace.log(e_errors.ERROR,"stop_backup "+repr(status))
            self.reply_to_caller({"status"       : status,
                                  "stop_backup"  : 'no' })


class VolumeClerk(VolumeClerkMethods,\
                  generic_server.GenericServer):
    def __init__(self, csc):
        generic_server.GenericServer.__init__(self, csc, MY_NAME)
        Trace.init(self.log_name)
        keys = self.csc.get(MY_NAME)

        dispatching_worker.DispatchingWorker.__init__(self, (keys['hostip'],
                                                             keys['port']))

class VolumeClerkInterface(generic_server.GenericServerInterface):
        pass

if __name__ == "__main__":
    Trace.init(string.upper(MY_NAME))

    # get the interface
    intf = VolumeClerkInterface()

    # get a volume clerk
    vc = VolumeClerk((intf.config_host, intf.config_port))
    Trace.log(e_errors.INFO, '%s' % sys.argv)

    Trace.log(e_errors.INFO,"opening volume database using DbTable")
    dict = db.DbTable("volume",[])
    Trace.log(e_errors.INFO,"hurrah, volume database is open")

    while 1:
        try:
            Trace.log(e_errors.INFO,'Volume Clerk (re)starting')
            vc.serve_forever()
        except:
            vc.serve_forever_error(vc.log_name)
            continue
    Trace.log(e_errors.ERROR,"Volume Clerk finished (impossible)")
