import Trace
Trace.init("Vol Clerk")
Trace.trace(1,"GO")
###############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
import sys
import os
import time
import copy
import errno

# enstore imports
import callback
import log_client
import traceback
import configuration_client
import dispatching_worker
import generic_server
import generic_cs
import interface
import db
import Trace
import e_errors

class VolumeClerkMethods(dispatching_worker.DispatchingWorker):

    # add: some sort of hook to keep old versions of the s/w out
    # since we should like to have some control over format of the records.
    def addvol(self, ticket):
     Trace.trace(10,'{addvol '+repr(ticket))
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
	    self.enprint(ticket, generic_cs.PRETTY_PRINT)
            self.reply_to_caller(ticket)
            Trace.trace(0,"}addvol "+repr(ticket["status"]))
            return

        # can't have 2 with same label
        if dict.has_key(external_label):
            ticket["status"] = (errno.errorcode[errno.EEXIST], \
				"Volume Clerk: volume "+external_label\
                               +" already exists")
	    self.enprint(ticket, generic_cs.PRETTY_PRINT)
            self.reply_to_caller(ticket)
            Trace.trace(0,"}addvol "+repr(ticket["status"]))
            return

        # mandatory keys
        for key in  ['external_label','media_type', 'file_family', 'library',\
                     'eod_cookie', 'remaining_bytes', 'capacity_bytes' ]:
            try:
                record[key] = ticket[key]
            except KeyError:
                ticket["status"] = (e_errors.KEYERROR, \
				    "Volume Clerk: "+key+" is missing")
	        self.enprint(ticket, generic_cs.PRETTY_PRINT)
                self.reply_to_caller(ticket)
                Trace.trace(0,"}addvol "+repr(ticket["status"]))
                return

        # optional keys - use default values if not specified
        try:
            record['last_access'] = ticket['last_access']
        except KeyError:
            record["last_access"] = -1
        try:
            record['first_access'] = ticket['first_access']
        except KeyError:
            record["first_access"] = -1
        try:
            record['declared'] = ticket['declared']
            if record['declared'] == -1:
                x = ticket['force_key_error_to_get_except']
        except KeyError:
            record["declared"] = time.time()
        try:
            record['system_inhibit'] = ticket['system_inhibit']
        except KeyError:
            record["system_inhibit"] = "none"
        try:
            record['at_mover'] = ticket['at_mover']
        except KeyError:
            record["at_mover"] = ("unmounted", "none")
        try:
            record['user_inhibit'] = ticket['user_inhibit']
        except KeyError:
            record["user_inhibit"] = "none"
        try:
            record['sum_wr_err'] = ticket['sum_wr_err']
        except KeyError:
            record["sum_wr_err"] = 0
        try:
            record['sum_rd_err'] = ticket['sum_rd_err']
        except KeyError:
            record["sum_rd_err"] = 0
        try:
            record['sum_wr_access'] = ticket['sum_wr_access']
        except KeyError:
            record["sum_wr_access"] = 0
        try:
            record['sum_rd_access'] = ticket['sum_rd_access']
        except KeyError:
            record["sum_rd_access"] = 0
        try:
            record['wrapper'] = ticket['wrapper']
        except KeyError:
            record["wrapper"] = "cpio_custom"
        try:
            record['blocksize'] = ticket['blocksize']
            if record['blocksize'] == -1:
                x = ticket['force_key_error_to_get_except']
        except KeyError:
            sizes = self.csc.get("blocksizes")
            try:
                msize = sizes[ticket['media_type']]
            except:
                ticket['status'] = (e_errors.UNKNOWNMEDIA, \
				    "Volume Clerk: "\
                                   +"unknown media type = unknown blocksize")
	        self.enprint(ticket, generic_cs.PRETTY_PRINT)
                self.reply_to_caller(ticket)
                Trace.trace(0,"}addvol "+repr(ticket["status"]))
                return
            record['blocksize'] = msize

        # write the ticket out to the database
        dict[external_label] = record
        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        Trace.trace(10,'}addvol ok '+repr(external_label)+" "+repr(record))
        return

     # even if there is an error - respond to caller so he can process it
     except:
         Trace.trace(0,"}addvol "+str(sys.exc_info()[0])+\
                     str(sys.exc_info()[1]))
         ticket["status"] = (str(sys.exc_info()[0]), str(sys.exc_info()[1]))
	 self.enprint(ticket, generic_cs.PRETTY_PRINT)
         self.reply_to_caller(ticket)
         return


    # delete a volume from the database
    def delvol(self, ticket):
     Trace.trace(10,'{delvol '+repr(ticket))
     try:
        # everything is based on external label - make sure we have this
        key="external_label"
        try:
            external_label = ticket[key]
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
				"Volume Clerk: "+key+" key is missing")
	    self.enprint(ticket, generic_cs.PRETTY_PRINT)
            self.reply_to_caller(ticket)
            Trace.trace(0,"}delvol "+repr(ticket["status"]))
            return

        # delete if from the database
        try:
            del dict[external_label]
            ticket["status"] = (e_errors.OK, None)
            Trace.trace(10,'}delvol ok '+repr(external_label))
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
				"Volume Clerk: volume "+external_label\
                               +" no such volume")
	    self.enprint(ticket, generic_cs.PRETTY_PRINT)
            Trace.trace(0,"}delvol "+repr(ticket["status"]))

        self.reply_to_caller(ticket)
        return

     # even if there is an error - respond to caller so he can process it
     except:
         Trace.trace(0,"}delvol "+str(sys.exc_info()[0])+\
                     str(sys.exc_info()[1]))
         ticket["status"] = (str(sys.exc_info()[0]), str(sys.exc_info()[1]))
	 self.enprint(ticket, generic_cs.PRETTY_PRINT)
         self.reply_to_caller(ticket)
         return


    # Get the next volume that satisfy criteria
    def next_write_volume (self, ticket):
     Trace.trace(12,'{next_write_volume '+repr(ticket))
     try:
        # make sure we have this vol_veto_list
        key="vol_veto_list"
        try:
            vol_veto = ticket[key]
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
				"Volume Clerk: "+key+" key is missing")
	    self.enprint(ticket, generic_cs.PRETTY_PRINT)
            self.reply_to_caller(ticket)
            Trace.trace(0,"}next_write_volume "+repr(ticket["status"]))
            return
        #exec ("vol_veto_list = " + vol_veto)
        vol_veto_list = eval(vol_veto)

        # get the criteria for the volume from the user's ticket
        try:
            key = "min_remaining_bytes"
            min_remaining_bytes = ticket[key]
            key = "library"
            library = ticket[key]
            key = "file_family"
            file_family = ticket[key]
            key = "first_found"
            first_found = ticket[key]
	    key = "wrapper"
	    wrapper_type = ticket[key]
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
				"Volume Clerk: "+key+" is missing")
	    self.enprint(ticket, generic_cs.PRETTY_PRINT)
            self.reply_to_caller(ticket)
            Trace.trace(0,"}next_write_volume "+repr(ticket["status"]))
            return

        # go through the volumes and find one we can use for this request
        vol = {}
        while 1:
            label=dict.next()
            if label:
                pass
            else:
                break
            v = copy.deepcopy(dict[label])
	    # for backward compatibility for at_mover field
	    try:
		at_mover = v['at_mover']
	    except KeyError:
		v['at_mover'] = ('unmounted', '')
	    
	    #self.enprint(v, generic_cs.PRETTY_PRINT)
            if v["library"] != library:
                continue
            if v["file_family"] != file_family+"."+wrapper_type:
                continue
            if v["wrapper"] != wrapper_type:
                continue
            if v["user_inhibit"] != "none":
                continue
            if v["system_inhibit"] != "none":
                continue
	    if v['at_mover'][0] != "unmounted":
		continue
            if v["remaining_bytes"] < min_remaining_bytes:
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
	        self.enprint(label+" is now full, bytes remaining = "+\
	                     repr(left)+" wasted = "+repr(waste)+"%")
                dict[label] = copy.deepcopy(v)
                continue
            vetoed = 0
            for veto in vol_veto_list:
                if label == veto:
                    vetoed = 1
                    break
            if vetoed:
                continue

            Trace.trace(12,'next_write_vol found '+repr(v['external_label']))
            # supposed to return first volume found?
            if first_found:
                v["status"] = (e_errors.OK, None)
                self.reply_to_caller(v)
                Trace.trace(12,'}next_write_vol label = '+\
                            repr(v['external_label'])+" "+repr(v))
                return
            # if not, is this an "earlier" volume that one we already found?
            if len(vol) == 0:
                vol = copy.deepcopy(v)
            elif v['declared'] < vol['declared']:
                vol = copy.deepcopy(v)

        # return what we found
        if len(vol) != 0:
            vol["status"] = (e_errors.OK, None)
            self.reply_to_caller(vol)
            Trace.trace(12,'}next_write_vol label = '+\
                        repr(v['external_label'])+" "+repr(v))
            return

        # nothing was available - see if we can assign a blank one.
        Trace.trace(12,'}next_write_vol no vols available, checking for blanks')
        vol = {}
        while 1:
            label=dict.next()
            if label:
                pass
            else:
                break
            v = copy.deepcopy(dict[label])
	    # for backward compatibility for at_mover field
	    try:
		at_mover = v['at_mover']
	    except KeyError:
		v['at_mover'] = ('unmounted', '')

            if v["library"] != library:
                continue
            if v["file_family"] != "none":
                continue
            if v["user_inhibit"] != "none":
                continue
            if v["system_inhibit"] != "none":
                continue
	    if v['at_mover'][0] != "unmounted":
		continue
            if v["remaining_bytes"] < min_remaining_bytes:
                continue
            vetoed = 0
            label = v["external_label"]
            for veto in vol_veto_list:
                if label == veto:
                    vetoed = 1
                    break
            if vetoed:
                continue

            # supposed to return first blank volume found?
            if first_found:
		if file_family == "ephemeral":
		    file_family = label
                v["file_family"] = file_family+"."+wrapper_type
		v["wrapper"] = wrapper_type
                self.logc.send(e_errors.INFO,2,
                  "Assigning blank volume"+label+"to"+library+" "+file_family)
                dict[label] = copy.deepcopy(v)
                v["status"] = (e_errors.OK, None)
                self.reply_to_caller(v)
                Trace.trace(12,'}next_write_vol label = '+\
                            repr(v['external_label'])+" "+repr(v))
                return
            # if not, is this an "earlier" volume that one we already found?
            Trace.trace(12,'next_write_vol blank found '+\
                        repr(v['external_label']))
            if len(vol) == 0:
                vol = copy.deepcopy(v)
            elif v['declared'] < vol['declared']:
                vol = copy.deepcopy(v)

        # return blank volume we found
        if len(vol) != 0:
            label = vol['external_label']
	    if file_family == "ephemeral":
		file_family = label
            vol["file_family"] = file_family+"."+wrapper_type
	    vol["wrapper"] = wrapper_type
            self.logc.send(e_errors.INFO,2,
                  "Assigning blank volume"+label+"to"+library+" "+file_family)
            dict[label] = copy.deepcopy(vol)
            vol["status"] = (e_errors.OK, None)
            self.reply_to_caller(vol)
            Trace.trace(12,'}next_write_vol label = '+\
                        repr(v['external_label'])+" "+repr(v))
            return

        # nothing was available at all
        ticket["status"] = (e_errors.NOVOLUME, \
			    "Volume Clerk: no new volumes available")
        self.logc.send(e_errors.ERROR,1, "No blank volumes"+str(ticket) )
        self.reply_to_caller(ticket)
        Trace.trace(0,"}delvol "+repr(ticket["status"]))
        return

     # even if there is an error - respond to caller so he can process it
     except:
         Trace.trace(0,"}next_write_volume "+str(sys.exc_info()[0])+\
                     str(sys.exc_info()[1]))
	 traceback.print_exc()
         ticket["status"] = (str(sys.exc_info()[0]), str(sys.exc_info()[1]))
         self.logc.send(e_errors.ERROR,1, str(ticket) )
         self.reply_to_caller(ticket)
         return

    # check if specific volume can be used for write
    def can_write_volume (self, ticket):
     Trace.trace(12,'{can_write_volume '+repr(ticket))
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
	 self.enprint(ticket, generic_cs.PRETTY_PRINT)
	 self.reply_to_caller(ticket)
	 Trace.trace(0,"}can_write_volume "+repr(ticket["status"]))
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
	     if v["remaining_bytes"] < min_remaining_bytes:
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
		 self.enprint(external_label+" is now full, bytes remaining = "+\
			      repr(left)+" wasted = "+repr(waste)+"%")
		 dict[external_label] = copy.deepcopy(v)
		 ticket["status"] = (e_errors.WRITE_EOT, \
				     "Volume Clerk: "+key+" is missing")
	     self.reply_to_caller(ticket)
	     Trace.trace(0,"}can_write_volume "+repr(ticket["status"]))
	     return
	 else:
	     ticket["status"] = (e_errors.NOACCESS,'None')
	     self.reply_to_caller(ticket)
	     Trace.trace(0,"}can_write_volume "+repr(ticket["status"]))
	     return
     except KeyError:
	 ticket["status"] = (e_errors.KEYERROR, \
			     "Volume Clerk: volume "+external_label\
			     +" no such volume")
	 self.enprint(ticket, generic_cs.PRETTY_PRINT)
	 self.reply_to_caller(ticket)
	 Trace.trace(0,"}can_write_volume "+repr(ticket["status"]))
	 return


    # update the database entry for this volume
    def set_remaining_bytes(self, ticket):
     Trace.trace(12,'{set_remaining_bytes '+repr(ticket))
     try:
        # everything is based on external label - make sure we have this
        try:
            key="external_label"
            external_label = ticket[key]
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
				"Volume Clerk: "+key+" key is missing")
	    self.enprint(ticket, generic_cs.PRETTY_PRINT)
            self.reply_to_caller(ticket)
            Trace.trace(0,"}set_remaining_bytes "+repr(ticket["status"]))
            return

        # get the current entry for the volume
        try:
            record = copy.deepcopy(dict[external_label])
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
				"Volume Clerk: volume "+external_label\
                               +" no such volume")
	    self.enprint(ticket, generic_cs.PRETTY_PRINT)
            self.reply_to_caller(ticket)
            Trace.trace(0,"}set_remaining_bytes "+repr(ticket["status"]))
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
	    self.enprint(ticket, generic_cs.PRETTY_PRINT)
            self.reply_to_caller(ticket)
            Trace.trace(0,"}set_remaining_bytes "+repr(ticket["status"]))
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
	        self.enprint(ticket, generic_cs.PRETTY_PRINT)
                self.reply_to_caller(ticket)
                Trace.trace(0,"}set_remaining_bytes "+repr(ticket["status"]))
                return

        # record our changes
        dict[external_label] = copy.deepcopy(record)
        record["status"] = (e_errors.OK, None)
        self.reply_to_caller(record)
        Trace.trace(12,'}set_remaining_bytes '+repr(record))
        return

     # even if there is an error - respond to caller so he can process it
     except:
         ticket["status"] = (str(sys.exc_info()[0]), str(sys.exc_info()[1]))
	 self.enprint(ticket, generic_cs.PRETTY_PRINT)
         self.reply_to_caller(ticket)
         Trace.trace(0,"}set_remaining_bytes "+repr(ticket["status"]))
         return


    # update the database entry for this volume
    def update_counts(self, ticket):
     Trace.trace(12,'{update_counts '+repr(ticket))
     try:
        # everything is based on external label - make sure we have this
        try:
            key="external_label"
            external_label = ticket[key]
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
				"Volume Clerk: "+key+" key is missing")
	    self.enprint(ticket, generic_cs.PRETTY_PRINT)
            self.reply_to_caller(ticket)
            Trace.trace(0,"}update_counts "+repr(ticket["status"]))
            return

        # get the current entry for the volume
        try:
            record = copy.deepcopy(dict[external_label])
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
				"Volume Clerk: volume "+external_label\
                               +" no such volume")
	    self.enprint(ticket, generic_cs.PRETTY_PRINT)
            self.reply_to_caller(ticket)
            Trace.trace(0,"}update_counts "+repr(ticket["status"]))
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
	        self.enprint(ticket, generic_cs.PRETTY_PRINT)
                self.reply_to_caller(ticket)
                Trace.trace(0,"update_counts "+repr(ticket["status"]))
                return

        # record our changes
        dict[external_label] = copy.deepcopy(record)
        record["status"] = (e_errors.OK, None)
        self.reply_to_caller(record)
        Trace.trace(12,'}update_counts ok '+repr(record))
        return

     # even if there is an error - respond to caller so he can process it
     except:
         ticket["status"] = (str(sys.exc_info()[0]), str(sys.exc_info()[1]))
	 self.enprint(ticket, generic_cs.PRETTY_PRINT)
         self.reply_to_caller(ticket)
         Trace.trace(0,"}update_counts "+repr(ticket["status"]))
         return


    # get the current database volume about a specific entry
    def inquire_vol(self, ticket):
     Trace.trace(12,'{inquire_vol '+repr(ticket))
     try:
        # everything is based on external label - make sure we have this
        try:
            key="external_label"
            external_label = ticket[key]
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
				"Volume Clerk: "+key+" key is missing")
	    self.enprint(ticket, generic_cs.PRETTY_PRINT)
            self.reply_to_caller(ticket)
            Trace.trace(0,"}inquire_vol "+repr(ticket["status"]))
            return

        # get the current entry for the volume
        try:
            record = copy.deepcopy(dict[external_label])
            record["status"] = e_errors.OK, None
            self.reply_to_caller(record)
            Trace.trace(12,'}inquire_vol '+repr(record))
            return
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
				"Volume Clerk: volume "+external_label\
                               +" no such volume")
	    self.enprint(ticket, generic_cs.PRETTY_PRINT)
            self.reply_to_caller(ticket)
            Trace.trace(0,"}inquire_vol "+repr(ticket["status"]))
            return

     # even if there is an error - respond to caller so he can process it
     except:
         ticket["status"] = (str(sys.exc_info()[0]), str(sys.exc_info()[1]))
	 self.enprint(ticket, generic_cs.PRETTY_PRINT)
         self.reply_to_caller(ticket)
         Trace.trace(0,"}inquire_vol "+repr(ticket["status"]))
         return


    # flag the database that we are now writing the system
    def update_mc_state(self, ticket):
     Trace.trace(10,'{vc.update_mc_state '+repr(ticket))
     try:
        # everything is based on external label - make sure we have this
        try:
            key="external_label"
            external_label = ticket[key]
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
				"Volume Clerk: "+key+" key is missing")
	    self.enprint(ticket, generic_cs.PRETTY_PRINT)
            self.reply_to_caller(ticket)
            Trace.trace(0,"}vc.update_mc_state "+repr(ticket["status"]))
            return

        # get the current entry for the volume
        try:
            record = copy.deepcopy(dict[external_label])
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
				"Volume Clerk: volume "+external_label\
                               +" no such volume")
	    self.enprint(ticket, generic_cs.PRETTY_PRINT)
            self.reply_to_caller(ticket)
            Trace.trace(0,"}vc.update_mc_state "+repr(ticket["status"]))
            return

        # update the fields that have changed
	ll = list(record['at_mover'])
	ll[0]= self.get_media_changer_state(record["library"],
	                          record["external_label"], record["media_type"])
	record['at_mover']=tuple(ll)
        dict[external_label] = copy.deepcopy(record) # THIS WILL JOURNAL IT
        record["status"] = (e_errors.OK, None)
        self.reply_to_caller(record)
        Trace.trace(10,'}vc.update_mc_state '+repr(record))
        return

     # even if there is an error - respond to caller so he can process it
     except:
         ticket["status"] = (str(sys.exc_info()[0]), str(sys.exc_info()[1]))
	 self.enprint(ticket, generic_cs.PRETTY_PRINT)
         self.reply_to_caller(ticket)
         Trace.trace(0,"}vc.update_mc_state "+repr(ticket["status"]))
         return


    # flag the database that we are now writing the system
    def clr_system_inhibit(self, ticket):
     Trace.trace(10,'{vc.clr_system_inhibit '+repr(ticket))
     try:
        # everything is based on external label - make sure we have this
        try:
            key="external_label"
            external_label = ticket[key]
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
				"Volume Clerk: "+key+" key is missing")
	    self.enprint(ticket, generic_cs.PRETTY_PRINT)
            self.reply_to_caller(ticket)
            Trace.trace(0,"}vc.clr_system_inhibit "+repr(ticket["status"]))
            return

        # get the current entry for the volume
        try:
            record = copy.deepcopy(dict[external_label])
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
				"Volume Clerk: volume "+external_label\
                               +" no such volume")
	    self.enprint(ticket, generic_cs.PRETTY_PRINT)
            self.reply_to_caller(ticket)
            Trace.trace(0,"}vc.clr_system_inhibit "+repr(ticket["status"]))
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
        Trace.trace(10,'}vc.clr_system_inhibit '+repr(record))
        return

     # even if there is an error - respond to caller so he can process it
     except:
         ticket["status"] = (str(sys.exc_info()[0]), str(sys.exc_info()[1]))
	 self.enprint(ticket, generic_cs.PRETTY_PRINT)
         self.reply_to_caller(ticket)
         Trace.trace(0,"}vc.clr_system_inhibit "+repr(ticket["status"]))
         return

	 
    # get the actual state of the media changer
    def get_media_changer_state(self, libMgr, volume, m_type):
     Trace.trace(11,'{vc.get_media_changer_state '+repr(volume))
     import library_manager_client
     lmc = library_manager_client.LibraryManagerClient(self.csc, 0,
        	           libMgr+".library_manager", 0, 0)
     mchgr = lmc.get_mc()      # return media changer
     del lmc
     if None != mchgr :
         import media_changer_client
         mcc = media_changer_client.MediaChangerClient(self.csc, 0, mchgr )
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
         Trace.trace(0," }vc.get_media_changer_state: ERROR: no media changer found "
	             +repr(volume))
         return 'unknown'
     return state


    # for the backward compatibility D0_TEMP
    # flag the database that we are now writing the system
    def add_at_mover(self, ticket):
     Trace.trace(10,'{add_at_mover '+repr(ticket))
     try:
        # everything is based on external label - make sure we have this
        try:
            key="external_label"
            external_label = ticket[key]
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
				"Volume Clerk: "+key+" key is missing")
	    self.enprint(ticket, generic_cs.PRETTY_PRINT)
            self.reply_to_caller(ticket)
            Trace.trace(0,"}cadd_at_mover "+repr(ticket["status"]))
            return

        # get the current entry for the volume
        try:
            record = copy.deepcopy(dict[external_label])
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
				"Volume Clerk: volume "+external_label\
                               +" no such volume")
	    self.enprint(ticket, generic_cs.PRETTY_PRINT)
            self.reply_to_caller(ticket)
            Trace.trace(0,"}add_at_mover "+repr(ticket["status"]))
            return

        # add fields
	try:
	    at_mover = record['at_mover']
	except KeyError:
	    record['at_mover'] = ('unmounted', 'none')
        dict[external_label] = copy.deepcopy(record) # THIS WILL JOURNAL IT
        record["status"] = (e_errors.OK, None)
        self.reply_to_caller(record)
        Trace.trace(10,'}add_at_mover '+repr(record))
        return

     # even if there is an error - respond to caller so he can process it
     except:
         ticket["status"] = (str(sys.exc_info()[0]), str(sys.exc_info()[1]))
	 self.enprint(ticket, generic_cs.PRETTY_PRINT)
         self.reply_to_caller(ticket)
         Trace.trace(0,"}add_at_mover "+repr(ticket["status"]))
         return
    # END D0_TEMP


    # move a volume to a new library
    def new_library(self, ticket):
     Trace.trace(16,'{new_library '+repr(ticket))
     try:
        # everything is based on external label - make sure we have this
        try:
            key="external_label"
            external_label = ticket[key]
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
				"Volume Clerk: "+key+" key is missing")
	    self.enprint(ticket, generic_cs.PRETTY_PRINT)
            self.reply_to_caller(ticket)
            Trace.trace(0,"}new_library "+repr(ticket["status"]))
            return

        # make sure the user has specified a new library
        try:
            key="new_library"
            new_library = ticket[key]
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
				"Volume Clerk: "+key+" key is missing")
	    self.enprint(ticket, generic_cs.PRETTY_PRINT)
            self.reply_to_caller(ticket)
            Trace.trace(0,"}new_library "+repr(ticket["status"]))
            return

        # get the current entry for the volume
        try:
            record = copy.deepcopy(dict[external_label])
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
				"Volume Clerk: volume "+external_label\
                               +" no such volume")
	    self.enprint(ticket, generic_cs.PRETTY_PRINT)
            self.reply_to_caller(ticket)
            Trace.trace(0,"}new_library "+repr(ticket["status"]))
            return

        # update the library field with the new library
        record ["library"] = new_library
        dict[external_label] = copy.deepcopy(record) # THIS WILL JOURNAL IT
        record["status"] = (e_errors.OK, None)
        self.reply_to_caller(record)
        Trace.trace(16,'}new_library '+repr(record))
        return

     # even if there is an error - respond to caller so he can process it
     except:
         ticket["status"] = (str(sys.exc_info()[0]), str(sys.exc_info()[1]))
	 self.enprint(ticket, generic_cs.PRETTY_PRINT)
         self.reply_to_caller(ticket)
         Trace.trace(0,"}new_library "+repr(ticket["status"]))
         return


    # set system_inhibit flag
    # flag the database that we are now writing the system
    def set_writing(self, ticket):
     Trace.trace(16,'{set_writing '+repr(ticket))
     try:
        # everything is based on external label - make sure we have this
        try:
            key="external_label"
            external_label = ticket[key]
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
				"Volume Clerk: "+key+" key is missing")
	    self.enprint(ticket, generic_cs.PRETTY_PRINT)
            self.reply_to_caller(ticket)
            Trace.trace(0,"}set_writing "+repr(ticket["status"]))
            return

        # get the current entry for the volume
        try:
            record = copy.deepcopy(dict[external_label])
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
				"Volume Clerk: volume "+external_label\
                               +" no such volume")
	    self.enprint(ticket, generic_cs.PRETTY_PRINT)
            self.reply_to_caller(ticket)
            Trace.trace(0,"}set_writing "+repr(ticket["status"]))
            return

        # update the fields that have changed
        record ["system_inhibit"] = "writing"
        dict[external_label] = copy.deepcopy(record) # THIS WILL JOURNAL IT
        record["status"] = (e_errors.OK, None)
        self.reply_to_caller(record)
        Trace.trace(16,'}set_writing '+repr(record))
        return

     # even if there is an error - respond to caller so he can process it
     except:
         ticket["status"] = (str(sys.exc_info()[0]), str(sys.exc_info()[1]))
	 self.enprint(ticket, generic_cs.PRETTY_PRINT)
         self.reply_to_caller(ticket)
         Trace.trace(0,"}set_writing "+repr(ticket["status"]))
         return


    # set system_inhibit flag
    def set_system_inhibit(self, ticket, flag):
     Trace.trace(11,'{set_system_inhibit '+repr(ticket))
     try:
        # everything is based on external label - make sure we have this
        try:
            key="external_label"
            external_label = ticket[key]
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
				"Volume Clerk: "+key+" key is missing")
	    self.enprint(ticket, generic_cs.PRETTY_PRINT)
            self.reply_to_caller(ticket)
            Trace.trace(0,"}set_system_inhibit "+repr(ticket["status"]))
            return

        # get the current entry for the volume
        try:
            record = copy.deepcopy(dict[external_label])
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
				"Volume Clerk: volume "+external_label\
                               +" no such volume")
	    self.enprint(ticket, generic_cs.PRETTY_PRINT)
            self.reply_to_caller(ticket)
            Trace.trace(0,"}set_system_inhibit "+repr(ticket["status"]))
            return

        # update the fields that have changed
        record ["system_inhibit"] = flag
        dict[external_label] = copy.deepcopy(record) # THIS WILL JOURNAL IT
        record["status"] = (e_errors.OK, None)
        self.reply_to_caller(record)
        Trace.trace(11,"}set_system_inhibit "+repr(record))
        return

     # even if there is an error - respond to caller so he can process it
     except:
         ticket["status"] = (str(sys.exc_info()[0]), str(sys.exc_info()[1]))
	 self.enprint(ticket, generic_cs.PRETTY_PRINT)
         self.reply_to_caller(ticket)
         Trace.trace(0,"}set_system_inhibit "+repr(ticket["status"]))
         return

    # flag that the current volume is readonly
    def set_system_readonly(self, ticket):
     Trace.trace(10,'{set_system_readonly '+repr(ticket))
     self.set_system_inhibit(ticket, "readonly")
     Trace.trace(10,"}set_system_readonly ")

    # flag that the current volume is marked as noaccess
    def set_system_noaccess(self, ticket):
     Trace.trace(10,'{set_system_noaccess '+repr(ticket))
     self.set_system_inhibit(ticket, e_errors.NOACCESS)
     Trace.trace(10,"}set_system_noaccess ")

    # device is broken - what to do, what to do
    def set_hung(self,ticket):
     Trace.trace(10,'{set_hung '+repr(ticket))
     try:
        self.reply_to_caller({"status" : (e_errors.OK, None)})
        Trace.trace(10,'}set_hung')
        return

     # even if there is an error - respond to caller so he can process it
     except:
         ticket["status"] = (str(sys.exc_info()[0]), str(sys.exc_info()[1]))
	 self.enprint(ticket, generic_cs.PRETTY_PRINT)
         self.reply_to_caller(ticket)
         Trace.trace(0,"}set_system_readonly "+repr(ticket["status"]))
         return


    # set at_mover flag
    def set_at_mover(self, ticket):
     Trace.trace(11,'{set_at_mover '+repr(ticket))
     try:
        # everything is based on external label - make sure we have this
        try:
            key="external_label"
            external_label = ticket[key]
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
				"Volume Clerk: "+key+" key is missing")
	    self.enprint(ticket, generic_cs.PRETTY_PRINT)
            self.reply_to_caller(ticket)
            Trace.trace(0,"}set_at_mover "+repr(ticket["status"]))
            return

        # get the current entry for the volume
        try:
            record = copy.deepcopy(dict[external_label])
        except KeyError:
            ticket["status"] = (e_errors.KEYERROR, \
				"Volume Clerk: volume "+external_label\
                               +" no such volume")
	    self.enprint(ticket, generic_cs.PRETTY_PRINT)
            self.reply_to_caller(ticket)
            Trace.trace(0,"}set_at_mover "+repr(ticket["status"]))
            return

	# see if record has key at_mover
	try:
	    at_mover = record['at_mover']
	except KeyError:
	    record['at_mover'] = ('unmounted','none')
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
	    record["status"] = (e_errors.CONFLICT, "volume "+\
				repr(external_label)+ " state "+\
				repr(record['at_mover'][0])+" req. state "+\
				     repr(ticket['at_mover'][0]))
	else:      
	    record ['at_mover'] = ticket['at_mover']
	    dict[external_label] = copy.deepcopy(record) # THIS WILL JOURNAL IT
	    record["status"] = (e_errors.OK, None)
        self.reply_to_caller(record)
        Trace.trace(11,"}set_at_mover "+repr(record))
        return

     # even if there is an error - respond to caller so he can process it
     except:
         ticket["status"] = (str(sys.exc_info()[0]), str(sys.exc_info()[1]))
	 self.enprint(ticket, generic_cs.PRETTY_PRINT)
         self.reply_to_caller(ticket)
         Trace.trace(0,"}set_at_mover "+repr(ticket["status"]))
         return

    # return all the volumes in our dictionary.  Not so useful!
    def get_vols(self,ticket):
        Trace.trace(20,'get_vols R U CRAZY? '+repr(ticket))
        ticket["status"] = (e_errors.OK, None)
        try:
            self.reply_to_caller(ticket)
        # even if there is an error - respond to caller so he can process it
        except:
            ticket["status"] = (str(sys.exc_info()[0]), str(sys.exc_info()[1]))
            self.reply_to_caller(ticket)
            Trace.trace(0,"}get_vols "+repr(ticket["status"]))
            return

        # this could tie things up for awhile - fork and let child
        # send the work list (at time of fork) back to client
        if os.fork() != 0:
            Trace.trace(20,'}get_vols forked parent - returning')
            return
        Trace.init("get_vols")
        self.get_user_sockets(ticket)
        ticket["status"] = (e_errors.OK, None)
        callback.write_tcp_socket(self.data_socket,ticket,
                                  "volume_clerk get_vols, controlsocket")
        msg=""
        key=dict.next()
        while key:
                msg=msg+repr(key)+","
                key=dict.next()
                #send 16K message
                if len(msg) >= 8192:
                   callback.write_tcp_buf(self.data_socket,msg,
                                  "volume_clerk get_vols, datasocket")
                   msg=""
        #send the last message
        msg=msg[:-1]
        callback.write_tcp_buf(self.data_socket,msg,
                                  "volume_clerk get_vols, datasocket")
        self.data_socket.close()
        callback.write_tcp_socket(self.control_socket,ticket,
                                  "volume_clerk get_vols, controlsocket")
        self.control_socket.close()
        Trace.trace(20,'}get_vols child exitting')
        os._exit(0)


    # get a port for the data transfer
    # tell the user I'm your volume clerk and here's your ticket
    def get_user_sockets(self, ticket):
        Trace.trace(16,'{get_user_sockets '+repr(ticket))
        volume_clerk_host, volume_clerk_port, listen_socket =\
                           callback.get_callback()
        listen_socket.listen(4)
        ticket["volume_clerk_callback_host"] = volume_clerk_host
        ticket["volume_clerk_callback_port"] = volume_clerk_port
        self.control_socket = callback.user_callback_socket(ticket)
        data_socket, address = listen_socket.accept()
        self.data_socket = data_socket
        listen_socket.close()
        Trace.trace(16,'}get_user_sockets ='+repr((volume_clerk_host,\
                                                   volume_clerk_port)))

    def start_backup(self,ticket):
        Trace.trace(5,'{start_backup '+repr(ticket))
        dict.start_backup()
        self.reply_to_caller({"status" : (e_errors.OK, None),\
                "start_backup"  : 'yes' })
        Trace.trace(5,'}start_backup')

    def stop_backup(self,ticket):
        Trace.trace(5,'{stop_backup '+repr(ticket))
        dict.stop_backup()
        self.reply_to_caller({"status" : (e_errors.OK, None),\
                "stop_backup"  : 'yes' })
        Trace.trace(5,'}stop_backup')

class VolumeClerk(VolumeClerkMethods,\
                  generic_server.GenericServer):
    def __init__(self, csc=0, verbose=0, host=interface.default_host(), \
                 port=interface.default_port()):
        Trace.trace(10, '{__init__')
	self.print_id = "VOLCS"
	self.verbose = verbose
        # get the config server
        configuration_client.set_csc(self, csc, host, port, verbose)
        #   pretend that we are the test system
        #   remember, in a system, there is only one bfs
        #   get our port and host from the name server
        #   exit if the host is not this machine
        keys = self.csc.get("volume_clerk")
        try:
            self.print_id = keys['logname']
        except:
            pass
        Trace.init(keys["logname"])
        dispatching_worker.DispatchingWorker.__init__(self, (keys['hostip'],
	                                              keys['port']))
        # get a logger
        self.logc = log_client.LoggerClient(self.csc, keys["logname"], \
                                            'logserver', 0)
        Trace.trace(10, '}__init__')

class VolumeClerkInterface(generic_server.GenericServerInterface):
	pass

if __name__ == "__main__":
    import sys
    Trace.init("Vol Clerk")
    Trace.trace(1,"Volume clerk called with args "+repr(sys.argv))

    # get the interface
    intf = VolumeClerkInterface()

    # get a volume clerk
    vc = VolumeClerk(0, intf.verbose, intf.config_host, intf.config_port)

    indlst=['media_type','file_family','library']
    dict = db.DbTable("volume",vc.logc,indlst)
    while 1:
        try:
            Trace.trace(1,'Volume Clerk (re)starting')
            vc.logc.send(e_errors.INFO, 1, "Volume Clerk (re)starting")
            vc.serve_forever()
        except:
	    vc.serve_forever_error("volume clerk", vc.logc)
            continue
    Trace.trace(1,"Volume Clerk finished (impossible)")
