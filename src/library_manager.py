#!/usr/bin/env python

###############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
import os
import time
import traceback
import sys
import string

# enstore imports
import volume_clerk_client
import callback
import dispatching_worker
import generic_server
import interface
import Trace
import udp_client

import manage_queue
import e_errors
import lm_list
import volume_family

def p(*args):
    print args

Trace.trace = p

######################################################################
# The following routines are for test only
# I need them until new mover code is available

movers = []    # list of movers belonging to this LM

# find mover in the list
def find_mover(mover, mover_list):
    for mv in mover_list:
	if (mover['address'] == mv['address']):
	    break
    else:
	# mover is not in the list
	return {}
    return mv

# add mover to the movers list
def add_mover(name, address):
    if not find_mover({"address":address}, movers):
	mover = {'mover'   : name,             # mover name
		 'address' : address,          # mover address
		 'state'   : 'idle_mover',     # mover state
		 'last_checked' : time.time(), # last time the mover has been updated
		 'summon_try_cnt' : 0,         # number of summon attempts till succeeded
		 'tr_error' : 'ok',            # transmission error
		 'file_family':''              # last file family the mover had worked with
		 }
	movers.append(mover)
	Trace.log(e_errors.INFO, "Mover added to mover list. Mover:%s. "%(mover,))
	return mover
    return {}

# get list of assigned movers from the configuration list
def get_movers(config_client, lm_name):
    movers_list = config_client.get_movers(lm_name)
    for item in movers_list:
	if (item.has_key('mover') and
	    item.has_key('address')):
	    add_mover(item['mover'], item['address'])
    if movers:
	Trace.log(e_errors.INFO, "get_movers %s"%(movers,))
    else:
	Trace.log(e_errors.ERROR, "get_movers: no movers defined in the configuration for this LM")

# find mover by name in the list
def find_mover_by_name(mover):
    for mv in movers:
	if (mover == mv['mover']):
	    break
    else:
	# mover is not in the list
	return {}
    return mv

        
## Trace.trace for additional debugging info uses bits >= 11
 

##############################################################
class SG_FF:
    def __init__(self):
        self.sg = {}
        self.vf = {}

    def put(self, mover, volume, sg, vf):
        if not self.sg.has_key(sg):
            self.sg[sg] = []
        if not self.vf.has_key(vf):
            self.vf[vf] = []
        if not ((mover, volume) in self.sg[sg]):
            self.sg[sg].append((mover,volume))
        if not ((mover, volume) in self.vf[vf]):
            self.vf[vf].append((mover,volume))

    def delete(self, mover, volume, sg, vf):
        if self.sg.has_key(sg) and (mover, volume) in self.sg[sg]:
            self.sg[sg].remove((mover, volume))
            if len(self.sg[sg]) == 0:
                del(self.sg[sg])
        if self.vf.has_key(vf) and (mover, volume) in self.vf[vf]:
            self.vf[vf].remove((mover, volume))
            if len(self.vf[vf]) == 0:
                del(self.vf[vf])

    def __repr__(self):
        return "<storage groups %s volume_families %s >" % (self.sg, self.vf)
        
##############################################################
class AtMovers:
    def __init__(self):
        self.at_movers = {}
        self.sg_vf = SG_FF()
            
    def put(self, mover_info):
        # mover_info contains:
        # mover
        # volume
        # volume_family
        # work (read/write)
        # current location
        Trace.trace(11,"put: %s" % (mover_info,))
        if not mover_info['volume_family']: return
        if not mover_info['mover']: return
        storage_group = volume_family.extract_storage_group(mover_info['volume_family'])
        vol_family = mover_info['volume_family']
        mover = mover_info['mover']
        self.at_movers[mover] = mover_info
        self.sg_vf.put(mover, mover_info['external_label'], storage_group, vol_family)
        Trace.trace(11,"AtMovers put: at_movers: %s sg_vf: %s" % (self.at_movers, self.sg_vf))

    def delete(self, mover_info):
        Trace.trace(11, "AtMovers delete. before: %s" % (self.at_movers,))
        mover = mover_info['mover']
        if self.at_movers.has_key(mover):
            Trace.log(11, "MOVER %s" % (self.at_movers[mover],))
            storage_group = volume_family.extract_storage_group(self.at_movers[mover]['volume_family'])
            vol_family = self.at_movers[mover]['volume_family']
            self.sg_vf.delete(mover, self.at_movers[mover]['external_label'], storage_group, vol_family) 
            del(self.at_movers[mover])
        Trace.trace(11,"AtMovers delete: at_movers: %s sg_vf: %s" % (self.at_movers, self.sg_vf))

   # return a list of busy volumes for a given volume family
    def busy_volumes (self, volume_family_name):
        Trace.trace(12,"busy_volumes: family=%s"%(volume_family_name,))
        vols = []
        write_enabled = 0
        if not  self.sg_vf.vf.has_key(volume_family_name):
            return vols, write_enabled
        # look in the list of work_at_movers
        for rec in self.sg_vf.vf[volume_family_name]:
            vols.append(rec[1])
            if (self.at_movers.has_key(rec[0]) and self.at_movers[rec[0]]['volume_status'][0][1]) == 'none':  # system inhibit
                # if volume can be potentially written increase number
                # of write enabled volumes that are currently at work
                # further comparison of this number with file family width
                # tells if write work can be given out
                write_enabled = write_enabled + 1
        return vols, write_enabled

    # return active volumes for a given storage class for
    # a fair share distribution
    def active_volumes_in_storage_group(self, storage_group):
        if self.sg_vf.sg.has_key(storage_group):
            sg = self.sg_vf.sg[storage_group]
        else: sg = []
        return sg
    
    # check if a particular volume with given label is busy
    # for read requests
    def is_vol_busy(self, external_label):
        rc = 0
        # see if this volume is in voulemes_at movers list
        keys = self.at_movers.keys()
        for key in keys:
            if external_label == self.at_movers[key]['external_label']:
                Trace.log(e_errors.INFO, "volume %s is active. Mover=%s"%\
                          (external_label, key))
                rc = 1
                break
        return rc
        
##############################################################
       

class LibraryManagerMethods:
    def __init__(self, csc, sg_limits):
	# instantiate volume clerk client
	self.vcc = volume_clerk_client.VolumeClerkClient(self.csc)
        self.sg_limits = {'use_default' : 1,
                          'default' : 0,
                          'limits' : {}
                          }
        if sg_limits:
            self.sg_limits['use_default'] = 0
            self.sg_limits['limits'] = sg_limits
        self.work_at_movers = lm_list.LMList()      ## remove this when work on volumes_at_movers is finished
        self.volumes_at_movers = AtMovers() # to keep information about what volumes are mounted at which movers
        self.suspect_volumes = lm_list.LMList()
        self.pending_work = manage_queue.Request_Queue()
        

    # get storage grop limit
    def get_sg_limit(self, storage_group):
        if self.sg_limits['use_default']:
            return self.sg_limits['default']
        else:
            return self.sg_limits['limits'].get(storage_group, self.sg_limits['default'])
        
    # send a regret
    def send_regret(self, ticket):
        # fork off the regret sender
        ret = self.fork()
        if ret == 0:
            try:
                Trace.trace(13,"send_regret "+repr(ticket))
                callback.send_to_user_callback(ticket)
                Trace.trace(13,"send_regret ")
            except:
                exc,msg,tb=sys.exc_info()
                Trace.log(1,"send_regret %s %s %s"%(exc,msg,ticket))
            os._exit(0)
        else:
            Trace.trace(12, "CHILD ID= %s"%(ret,))


    # find mover in the list
    def find_mover(mover, mover_list):
        for mv in mover_list:
            if (mover['address'] == mv['address']):
                break
        else:
            # mover is not in the list
            return {}
        return mv

    # find mover by name in the list
    def find_mover_by_name(mover, mover_list):
        for mv in mover_list:
            if (mover == mv['mover']):
                break
        else:
            # mover is not in the list
            return {}
        return mv


    # remove all pending works
    def flush_pending_jobs(self, status, external_label=None, jobtype=None):
        Trace.trace(12,"flush_pending_jobs: %s"%(external_label,))
        w = self.pending_work.get(external_label)
        rm_list = []
        while w:
            Trace.trace(12,"flush_pending_jobs:work %s"%(w.ticket,))
            self.send_regret(w.ticket)
            self.pending_work.delete(w)
            w = self.pending_work.get(external_label)
        # this is just for test

    # return a list of busy volumes for a given file family
    def busy_vols_in_family (self, family_name):
        Trace.trace(12,"busy_vols_in_family family: %s"%(family_name,))
        vols = []
        write_enabled = 0
        # look in the list of work_at_movers
        for w in self.work_at_movers.list:
            Trace.trace(12,"busy_vols_in_family work family: %s"%(w["vc"]["file_family"],))
            if w["vc"]["file_family"] == family_name:
                vols.append(w["fc"]["external_label"])
                vol_info = self.vcc.inquire_vol(w["fc"]["external_label"])
                Trace.trace(12,"busy_vols_in_family: system inhibit: %s"%(vol_info['system_inhibit'][1],))
                if vol_info['system_inhibit'][1] == 'none':
                    # if volume can be potentially written increase number
                    # of write enabled volumes that are currently at work
                    # further comparison of this number with file family width
                    # tells if write work can be given out
                    write_enabled = write_enabled + 1

        return vols, write_enabled

    # check if a particular volume with given label is busy
    # for read requests
    def is_volume_busy(self, external_label, requestor):
        rc = 0
        recover_vol_state = 0
        check_after_recovery = 0
        # check volume state
        vol_info = self.vcc.inquire_vol(external_label)
        if vol_info['at_mover'][0] == 'mounted':
            if requestor == vol_info['at_mover'][1]:
                # volume can not be mounted on idle mover : recover its state
                recover_vol_state = 1
                # check volume after recovery
                check_after_recovery = 1
            else:
                # volumes is being reported as mounted on snother mover
                # it is not available for this mover
                rc = 1
                
        elif vol_info['at_mover'][0] == 'unmounted':
            # check if volume is in work at movers queue
            for w in self.work_at_movers.list:
                if w["fc"]["external_label"] == external_label:
                    # for work at movers volume cannot be in unmounted state
                    Trace.log(e_errors.ERROR, "volume %s is in work_at_movers. Mover=%s,requestor=%s"%\
                              (external_label,vol_info['at_mover'][1],requestor))
                    rc = 1
                    recover_vol_state = 1
                    break
            if rc:
                self.work_at_movers.remove(w)
                Trace.log(e_errors.INFO, "removed work_at_movers entry %s"%(w,))
        else:
            # volume can be only in mounted or unmounted state
            # perhaps it is ejected: try to recover its state
            recover_vol_state = 1
            rc = 1
        if recover_vol_state:
            # restore volume state
            mcstate =  self.vcc.update_mc_state(external_label)
            format = "vol:%s state recovered to %s"
            Trace.log(e_errors.INFO,format%(external_label,mcstate["at_mover"][0]))
            if check_after_recovery:
                if mcstate["at_mover"][0] != 'unmounted': rc = 1
                    
        return rc


    # return ticket if given labeled volume is in mover queue
    # only one work can be in the work_at_movers for
    # a given volume. That's why external label is used
    # to identify the work
    def get_work_at_movers(self, external_label):
        rc = {}
        for w in self.work_at_movers.list:
            if w["fc"]["external_label"] == external_label:
                rc = w
                break
        return rc


    def init_request_selection(self):
        self.write_vf_list = {}
        self.tmp_rq = None   # need this to temporarily store selcted request
        self.checked_keys = [] # list of checked tag keys
        self.continue_scan = 0

    def fair_share(self, rq):
        # fair share
        # see how many active volumes are in this storage group
        if rq.ticket['work'] == 'read_from_hsm':
            storage_group = volume_family.extract_storage_group(rq.ticket['vc']['volume_family'])
            check_key = rq.ticket["fc"]["external_label"]
        else:
            # write request
            storage_group = rq.ticket["vc"]["storage_group"]
            check_key = rq.ticket["vc"]["volume_family"]
            
        if not check_key in self.checked_keys:
            self.checked_keys.append(check_key)
        active_volumes = self.volumes_at_movers.active_volumes_in_storage_group(storage_group)
        if len(active_volumes) >= self.get_sg_limit(storage_group):
            rq.ticket["reject_reason"] = ("LIMIT_REACHED",None)
            Trace.trace(11, "fair_share: active work limit exceeded for %s" % (storage_group,))
            if rq.adminpri > -1:
                self.continue_scan = 1
                return None
            # we have saturated system with requests from the same storage group
            # see if there are pending requests for different storage group
            tags = self.pending_work.get_tags()
            if len(tags) > 1:
                for key in tags:
                    if not key in self.checked_keys:
                        self.checked_keys.append(key) 
                        if key != check_key:
                            return key
        return None


    def process_read_request(self, request, requestor):
        self.continue_scan = 0
        rq = request
        if self.volumes_at_movers.is_vol_busy(rq.ticket["fc"]["external_label"]):
            rq.ticket["reject_reason"] = ("VOL_BUSY",rq.ticket["fc"]["external_label"])
            self.continue_scan = 1
            return rq, None
        # otherwise we have found a volume that has read work pending
        Trace.trace(11,"process_read_request %s"%(rq.ticket,))
        # ok passed criteria. Get request by file location
        if rq.ticket['encp']['adminpri'] < 0:
            rq = self.pending_work.get(rq.ticket["fc"]["external_label"])

        ########################################################
        ### from old idle_mover
        # check if the volume for this work had failed on this mover
        Trace.trace(13,"SUSPECT_VOLS %s"%(self.suspect_volumes,))
        suspect_v,suspect_mv = self.is_mover_suspect(requestor, rq.ticket['fc']['external_label'])
        if suspect_mv:
            # determine if this volume had failed on the maximal
            # allowed number of movers and, if yes, set volume 
            # as having no access and send a regret: noaccess.
            self.bad_volume(suspect_v, rq.ticket)
            self.continue_scan = 1
            return rq, None
        ############################################################
        
        # Check the presence of current_location field
        if not rq.ticket["vc"].has_key('current_location'):
            rq.ticket["vc"]['current_location'] = rq.ticket['fc']['location_cookie']

        # request has passed about all the criterias
        # check if it passes the fair share criteria
        # temprorarily store selected request to use it in case
        # when other request(s) based on fair share criteria
        # for some other reason(s) do not get selected
        self.tmp_rq = rq
        key_to_check = self.fair_share(rq)
        if key_to_check:
            self.continue_scan = 1
        return rq, key_to_check

    def process_write_request(self, request):
        self.continue_scan = 0
        rq = request
        vol_family = rq.ticket["vc"]["volume_family"]
        if not self.write_vf_list.has_key(vol_family):
            vol_veto_list, wr_en = self.volumes_at_movers.busy_volumes(vol_family)
            #vol_veto_list, wr_en = self.busy_vols_in_family(key)
            Trace.trace(11,"process_write_request vol veto list:%s, width:%d"%\
                        (vol_veto_list, wr_en))
            self.write_vf_list[vol_family] = {'vol_veto_list':vol_veto_list, 'wr_en': wr_en}
        else:
            vol_veto_list =  self.write_vf_list[vol_family]['vol_veto_list']
            wr_en = self.write_vf_list[vol_family]['wr_en']
        # only so many volumes can be written to at one time
        if wr_en >= rq.ticket["vc"]["file_family_width"]:
            rq.ticket["reject_reason"] = ("VOLS_IN_WORK","")
            self.continue_scan = 1
            return rq, None
        Trace.trace(11,"process_write_request: request next write volume for %s" % (vol_family,))

        # width not exceeded, ask volume clerk for a new volume.
        first_found = 0
        v = self.vcc.next_write_volume (rq.ticket["vc"]["library"],
                                        rq.ticket["wrapper"]["size_bytes"],
                                        rq.ticket["vc"]["volume_family"], 
                                        rq.ticket["vc"]["wrapper"],
                                        vol_veto_list,
                                        first_found)
        # volume clerk returned error
        Trace.trace(11,"process_write_request: next write volume returned %s" % (v,))
        if v["status"][0] != e_errors.OK:
            rq.ticket["status"] = v["status"]
            rq.ticket["reject_reason"] = (v["status"][0],v["status"][1])
            self.continue_scan = 1
            return rq, None
		
        # found a volume that has write work pending - return it
        rq.ticket["fc"]["external_label"] = v["external_label"]
        rq.ticket["fc"]["size"] = rq.ticket["wrapper"]["size_bytes"]
        rq.ticket['vc']['at_mover'] = v['at_mover']

        # request has passed about all the criterias
        # check if it passes the fair share criteria
        # temprorarily store selected request to use it in case
        # when other request(s) based on fair share criteria
        # for some other reason(s) do not get selected
        self.tmp_rq = rq
        
        key_to_check = self.fair_share(rq)
        if key_to_check:
            self.continue_scan = 1
        return rq, key_to_check

    # is there any work for any volume?
    def next_work_any_volume(self, requestor):
        Trace.trace(11, "next_work_any_volume")
        self.init_request_selection()
        # look in pending work queue for reading or writing work
        rq=self.pending_work.get()
        while rq:
            if rq.work == "read_from_hsm":
                rq, key = self.process_read_request(rq, requestor)
                if self.continue_scan:
                    if key:
                        rq = self.pending_work.get(key)
                    else:
                        rq = self.pending_work.get(next=1) # get next request
                    continue
                break
            elif rq.work == "write_to_hsm":
                rq, key = self.process_write_request(rq) 
                if self.continue_scan:
                    if key:
                        rq = self.pending_work.get(key)
                    else:
                        rq = self.pending_work.get(next=1) # get next request
                    continue
                break

            # alas, all I know about is reading and writing
            else:
                Trace.log(e_errors.ERROR,
                          "next_work_any_volume assertion error in next_work_any_volume %s"%(rq.ticket,))
                raise AssertionError
            rq = self.pending_work.get(next=1)

        if not rq:
            # see if there is a temporary stored request
            Trace.trace(11,"next_work_any_volume: using exceeded mover limit request") 
            rq = self.tmp_rq
        # check if this volume is ok to work with
        if rq:
            w = rq.ticket
            Trace.trace(11,"check volume %s " % (w['fc']['external_label'],))
            if w["status"][0] == e_errors.OK:
                ret = self.vcc.is_vol_available(rq.work,
                                                w['fc']['external_label'],
                                                w["vc"]["volume_family"],
                                                w["wrapper"]["size_bytes"])
                if ret['status'][0] != e_errors.OK:
                    Trace.trace(11,"work can not be done at this volume %s"%(ret,))
                    w['status'] = ret['status']
                    self.pending_work.delete(rq)
                    self.send_regret(w)
                    Trace.log(e_errors.ERROR,
                              "next_work_any_volume: cannot do the work for %s status:%s" % 
                              (rq.ticket['fc']['external_label'], rq.ticket['status'][0]))
                    return None, (e_errors.NOWORK, None)
            return rq, rq.ticket['status']
        return None, (e_errors.NOWORK, None)


    # what is next on our list of work?
    def schedule(self, mover):
        while 1:
            rq, status = self.next_work_any_volume(mover)
            if status[0] == e_errors.OK or \
               status[0] == e_errors.NOWORK:
                return rq, status
            # some sort of error, like write work and no volume available
            # so bounce. status is already bad...
            self.pending_work.delete(rq)
            self.send_regret(rq.ticket)
            Trace.trace(11,"schedule: Error detected %s" % (rq.ticket,))

    def check_write_request(self, external_label, rq):
        vol_veto_list, wr_en = self.volumes_at_movers.busy_volumes(rq.ticket['vc']['volume_family'])
        if wr_en >= rq.ticket["vc"]["file_family_width"]:
            if not external_label in vol_veto_list:
                rq.ticket["reject_reason"] = ("VOLS_IN_WORK","")
                Trace.trace(12, "check_write_request: request for volume %s rejected %s"%
                                (external_label, rq.ticket["reject_reason"]))
                rq.ticket['status'] = e_errors.INPROGRESS
                return rq, rq.ticket['status'] 
            
        ret = self.vcc.is_vol_available(rq.work,  external_label,
                                        rq.ticket['vc']['volume_family'],
                                        rq.ticket["wrapper"]["size_bytes"])
        # this work can be done on this volume
        if ret['status'][0] == e_errors.OK:
            rq.ticket['vc']['external_label'] = external_label
            rq.ticket['status'] = ret['status']
            rq.ticket["fc"]["size"] = rq.ticket["wrapper"]["size_bytes"]
            rq.ticket['fc']['external_label'] = external_label
            return rq, ret['status'] 
        else:
            rq.ticket['reject_reason'] = (ret['status'][0], ret['status'][1])
            # if work is write_to_hsm and volume has just been set to full
            # return this status for the immediate dismount
            if (rq.work == "write_to_hsm" and
                ret['status'][0] == e_errors.VOL_SET_TO_FULL):
                return None, ret['status']


    # is there any work for this volume??  v is a volume info
    # last_work is a last work for this volume
    # corrent location is a current position of the volume
    def next_work_this_volume(self, v, last_work, requestor, current_location):
        Trace.trace(11, "next_work_this_volume for %s" % (v,)) 
        self.init_request_selection()
        #self.pending_work.wprint()
        # first see if there are any HiPri requests
        rq =self.pending_work.get_admin_request()
        while rq:
            if rq.work == 'read_from_hsm':
                rq, key = self.process_read_request(rq, requestor)
                if self.continue_scan:
                    # before continuing check if it is a request
                    # for v['external_label']
                    if rq.ticket['fc']['external_label'] == v['external_label']: break
                    rq = self.pending_work.get_admin_request(next=1) # get next request
                    continue
                break
            elif rq.work == 'write_to_hsm':
                rq, key = self.process_write_request(rq) 
                if self.continue_scan:
                    rq, status = self.check_write_request(self, v['external_label'], rq)
                    if rq and status[0] == e_errors.OK: break
                    rq = self.pending_work.get_admin_request(next=1) # get next request
                    continue
                break
        
        if not rq:
            rq = self.tmp_rq
        if rq:
            if rq.work == 'read_from_hsm':
                # return work
                rq.ticket['status'] = (e_errors.OK, None)
                return rq, rq.ticket['status']
            elif rq.work == 'write_to_hsm':
                rq, status = self.check_write_request(self, v['external_label'], rq)
                if rq and status[0] == e_errors.OK:
                    return rq, status
                
        # no HIPri requests: look in pending work queue for reading or writing work
        self.init_request_selection()
        # for tape positioning optimization check what was
        # a last work for this volume
        if last_work == 'read_from_hsm':
            # see if there is another work for this volume
            # rq may be request for another volume in case
            # if it is an administration priority request
            # which may override all regular requests for this volume
            rq = self.pending_work.get(v["external_label"], current_location, use_admin_queue=0)
            if not rq:
               rq = self.pending_work.get(v['volume_family'], use_admin_queue=0) 
        elif last_work == 'write_to_hsm':
            # see if there is another work for this volume family
            # rq may be request for another volume in case
            # if it is an administration priority request
            # which may override all regular requests for this volume
            rq = self.pending_work.get(v['volume_family'], use_admin_queue=0)
            if not rq:
               rq = self.pending_work.get(v["external_label"], current_location, use_admin_queue=0) 

        if rq:
            # fair share
            storage_group = volume_family.extract_storage_group(v['volume_family'])
            active_volumes = self.volumes_at_movers.active_volumes_in_storage_group(storage_group)
            if len(active_volumes) > self.get_sg_limit(storage_group):
                rq.ticket["reject_reason"] = ("LIMIT_REACHED",None)
                Trace.trace(11, "next_work_this_volume: active work limit exceeded for %s" %
                            (storage_group,))
                return None, (e_errors.NOWORK, None)
            if rq.work == 'write_to_hsm':
                while rq:
                    rq, status = self.check_write_request(v['external_label'], rq)
                    if rq and status[0] == e_errors.OK:
                        return rq, status
                    self.pending_work.get(v['volume_family'],next=1, use_admin_queue=0)
            # return read work
            rq.ticket['status'] = (e_errors.OK, None)
            return rq, rq.ticket['status'] 
        
        # try from the beginning
        rq = self.pending_work.get(v["external_label"])
        if rq:
            # return work
            rq.ticket['status'] = (e_errors.OK, None)
            return rq, rq.ticket['status'] 
        return None, (e_errors.NOWORK, None)
                
                    
    # check if volume is in the suspect volume list
    def is_volume_suspect(self, external_label):
        for vol in self.suspect_volumes.list:
            if external_label == vol['external_label']:
                return vol
        return None

    # check if mover is in the suspect volume list
    # return tuple (suspect_volume, suspect_mover)
    def is_mover_suspect(self, mover, external_label):
        vol = self.is_volume_suspect(external_label)
        if vol:
            for mov in vol['movers']:
                if mover == mov:
                    break
            else: return vol,None
            return vol,mov
        else:
            return None,None

    # determine if this volume had failed on the maximal
    # allowed number of movers and, if yes, set volume 
    # as having no access and send a regret: noaccess.
    def bad_volume(self, suspect_volume, ticket):
	ret_val = 0
	if ticket['fc'].has_key('external_label'):
	    label = ticket['fc']['external_label']
	else: label = None
	if len(suspect_volume['movers']) >= self.max_suspect_movers:
	    ticket['status'] = (e_errors.NOACCESS, None)
	    Trace.trace(13,"Number of movers for suspect volume %d" %
			(len(suspect_volume['movers']),))
				
	    # set volume as noaccess
	    v = self.vcc.set_system_noaccess(label)

	    #remove entry from suspect volume list
	    self.suspect_volumes.remove(suspect_volume)
	    # delete the job
            rq, err = self.pending_work.find(ticket)
            Trace.trace(13,"bad volume: find returned %s %s"%(rq, err))
            if rq:
                self.pending_work.delete_job(rq)
                self.send_regret(ticket)
	    Trace.trace(13,"bad_volume: failed on more than %s for %s"%\
			(self.max_suspect_movers,suspect_volume))
	    ret_val = 1
	return ret_val

    # update suspect volumer list
    def update_suspect_vol_list(self, external_label, mover):
	# update list of suspected volumes
	Trace.trace(14,"SUSPECT VOLUME LIST BEFORE %s"%(self.suspect_volumes,))
	vol_found = 0
	for vol in self.suspect_volumes:
	    if external_label == vol['external_label']:
		vol_found = 1
		break
	if not vol_found:
	    vol = {'external_label' : external_label,
		   'movers' : []
		   }
	for mv in vol['movers']:
	    if mover == mv:
		break
	else:
	    vol['movers'].append(mover)
	if not vol_found:
	    self.suspect_volumes.append(vol)
            # send alarm if number of suspect volumes is above a threshold
            if len(self.suspect_volumes) >= self.max_suspect_volumes:
                Trace.alarm(e_errors.WARNING, e_errors.ABOVE_THRESHOLD,
                            {"volumes":"Number of suspect volumes is above threshold"}) 
            
	Trace.trace(14, "SUSPECT VOLUME LIST AFTER %s" % (self.suspect_volumes,))
	return vol


class LibraryManager(dispatching_worker.DispatchingWorker,
		     generic_server.GenericServer,
		     LibraryManagerMethods):

    suspect_volumes = [] # list of suspected volumes
    max_suspect_movers = 2 # maximal number of movers in the suspect volume
    max_suspect_volumes = 100 # maximal number of suspected volumes for alarm
                              # generation
    def __init__(self, libman, csc):
        self.name_ext = "LM"
        generic_server.GenericServer.__init__(self, csc, libman)
	self.name = libman
        #   pretend that we are the test system
        #   remember, in a system, there is only one bfs
        #   get our port and host from the name server
        #   exit if the host is not this machine
        self.keys = self.csc.get(libman)
	self.open_db()

        # setup a start up delay
        # this delay is needed to update state of the movers
        if self.keys.has_key('startup_delay'):
            self.startup_delay = self.keys['startup_delay']
        else:
            self.startup_delay = 32  # set it to 32 sec.
        self.time_started = time.time()
        self.startup_flag = 1   # this flag means that LM is in the startup state

        dispatching_worker.DispatchingWorker.__init__(self, (self.keys['hostip'], \
                                                      self.keys['port']))
        sg_limits = None
        if self.keys.has_key('storage_group_limits'):
            sg_limits = self.keys['storage_group_limits']
        LibraryManagerMethods.__init__(self, csc, sg_limits)
	self.set_udp_client()

    # check startup flag
    def is_starting(self):
        if self.startup_flag:
            if time.time() - self.time_started > self.startup_delay:
               self.startup_flag = 0
        return self.startup_flag
    
    # get lock from a lock file
    def get_lock(self):
        if self.keys.has_key('lock'):
            # get starting state from configuration
            # it can be: unlocked, locked, ignore, pause
            # the meaning of these states:
            # unlocked -- no comments
            # locked -- reject encp requests, give out works in the pending queue to movers
            # ignore -- do not put encp requests into pending queue, but return ok to encp,
            #           and give out works in the pending queue to movers
            # pause -- same as ignore, but also do not give out works in the pending
            #          queue to movers
            if self.keys['lock'] in ('locked', 'unlocked', 'ignore', 'pause'): 
                return self.keys['lock']
        try:
            lock_file = open(os.path.join(self.db_dir, 'lm_lock'), 'r')
            lock_state = lock_file.read()
            lock_file.close()
        except IOError:
            lock_state = None
        return lock_state
        
    # set lock in a lock file
    def set_lock(self, lock):
        lock_file = open(os.path.join(self.db_dir, 'lm_lock'), 'w')
        lock_file.write(lock)
        lock_file.close()
        
    # open all dbs that keep LM data
    def open_db(self):
        import string
	# if database directory is specified in configuration - get it
	if self.keys.has_key('database'):
	    self.db_dir = self.keys['database']
	else:
            Trace.log(e_errors.ERROR,"LM database is not defined in config file for %s"%(self.name,))
            sys.exit(1)
        # if directory does not exist, create it
        try:	
            if os.path.exists(self.db_dir) == 0:
                os.makedirs(self.db_dir)
        except:
	  exc, val, tb = e_errors.handle_error()
	  sys.exit(1)
       
	#self.work_at_movers = lm_list.LMList(self.db_dir, 
	#				     "work_at_movers",
	#				     "unique_id")
	#self.work_at_movers.restore()
        self.lm_lock = self.get_lock()
        if not self.lm_lock:
            self.lm_lock = 'unlocked'
            self.set_lock(self.lm_lock)
        Trace.log(e_errors.INFO,"Library manager started in state:%s"%(self.lm_lock,))
	
    def set_udp_client(self):
	self.udpc = udp_client.UDPClient()
	self.rcv_timeout = 10 # set receive timeout

    def write_to_hsm(self, ticket):
        #if self.lm_lock == 'locked' or self.lm_lock == 'ignore':
        if self.lm_lock in ('locked', 'ignore', 'pause'):
            if self.lm_lock == 'locked':
                ticket["status"] = (e_errors.NOMOVERS, "Library manager is locked for external access")
            else:
                ticket["status"] = (e_errors.OK, None)
            self.reply_to_caller(ticket)
            return
	    
        ticket["status"] = (e_errors.OK, None)
        # create a file_family
        if ticket["vc"]["file_family"] != 'ephemeral':
            ticket["vc"]["file_family"]+"."+ticket["vc"]["wrapper"]

        # check if work is in the at mover list before inserting it
	for wt in self.work_at_movers.list:
            # 2 requests cannot have the same output file names
            if (wt["wrapper"]['pnfsFilename'] == ticket["wrapper"]["pnfsFilename"]) \
	       and wt['retry'] == ticket['retry']:
                ticket['status'] = (e_errors.INPROGRESS,"Operation in progress")
                break
	    elif wt["unique_id"] == ticket["unique_id"]:
		break
        else:
            if not ticket.has_key('lm'):
                ticket['lm'] = {'address':self.server_address }
            # put ticket into request queue
            rq, status = self.pending_work.put(ticket)
            if status == e_errors.INPROGRESS:
                ticket['status'] = (e_errors.INPROGRESS,"Operation in progress")
            else: ticket['status'] = (status, None)
                
        self.reply_to_caller(ticket) # reply now to avoid deadlocks
        if ticket['status'][0] == e_errors.INPROGRESS:
            # we did not put request
            format = "write NOT Q'd %s -> %s : library=%s family=%s requester:%s"
            Trace.log(e_errors.INFO, format%(ticket["wrapper"]["fullname"],
                                             ticket["wrapper"]["pnfsFilename"],
                                             ticket["vc"]["library"],
                                             ticket["vc"]["file_family"],
                                             ticket["wrapper"]["uname"]))
            return

        if status == e_errors.OK:
            if not rq:
                format = "write rq. is already in the queue %s -> %s : library=%s family=%s requester:%s"
            else:
                format = "write Q'd %s -> %s : library=%s family=%s requester:%s"
            Trace.log(e_errors.INFO, format%(ticket["wrapper"]["fullname"],
                                             ticket["wrapper"]["pnfsFilename"],
                                             ticket["vc"]["library"],
                                             ticket["vc"]["file_family"],
                                             ticket["wrapper"]["uname"]))


    def read_from_hsm(self, ticket):
        #if self.lm_lock == 'locked' or self.lm_lock == 'ignore':
        if self.lm_lock in ('locked', 'ignore', 'pause'):
            if self.lm_lock == 'locked':
                ticket["status"] = (e_errors.NOMOVERS, "Library manager is locked for external access")
            else:
                ticket["status"] = (e_errors.OK, None)
            self.reply_to_caller(ticket)
            return
	# check if this volume is OK
	v = self.vcc.inquire_vol(ticket['fc']['external_label'])
	if (v['system_inhibit'][0] == e_errors.NOACCESS or
            v['system_inhibit'][0] == e_errors.NOTALLOWED):
	    # tape cannot be accessed, report back to caller and do not
	    # put ticket in the queue
	    ticket["status"] = (v['system_inhibit'][0], None)
	    self.reply_to_caller(ticket)
	    format = "read request discarded for unique_id=%s : volume %s is marked as %s"
	    Trace.log(e_errors.ERROR, format%(ticket['unique_id'],
					      ticket['fc']['external_label'],
					      ticket["status"][0]))
	    Trace.trace(11,"read_from_hsm: volume has no access")
	    return

	if not ticket.has_key('lm'):
	    ticket['lm'] = {'address' : self.server_address}

        # check if work is in the at mover list before inserting it
	for wt in self.work_at_movers.list:
	    if wt["unique_id"] == ticket["unique_id"]:
                status = e_errors.OK
                ticket['status'] = (status, None)
                rq = None
		break
        else:
            # put ticket into request queue
            rq, status = self.pending_work.put(ticket)
            if status == e_errors.INPROGRESS:
                ticket['status'] = (e_errors.INPROGRESS,"Operation in progress")
            else: ticket['status'] = (status, None)
        self.reply_to_caller(ticket) # reply now to avoid deadlocks
        if status == e_errors.OK:
            if not rq:
                format = "read rq. is already in the queue %s -> %s : library=%s family=%s requester:%s"
            else:
                format = "read Q'd %s -> %s : library=%s family=%s requester:%s"
            Trace.log(e_errors.INFO, format%(ticket["wrapper"]["fullname"],
                                             ticket["wrapper"]["pnfsFilename"],
                                             ticket["vc"]["library"],
                                             ticket["vc"]["volume_family"],
                                             ticket["wrapper"]["uname"]))

    # mover is idle - see what we can do
    def mover_idle(self, mticket):
        Trace.trace(11,"IDLE RQ %s"%(mticket,))
        
        # mover is idle remove it from volumes_at_movers
        self.volumes_at_movers.delete(mticket)
        if self.is_starting():
            # LM needs a certain startup delay before it
            # starts processing mover requests to update
            # its volumes at movers table
            return
        
        if self.lm_lock == 'pause':
            Trace.trace(11,"LM state is %s no mover request processing" % (self.lm_lock,))
            return
            
        self.requestor = mticket['mover']

	# check if there is a work for this mover in work_at_movers list
	# it should not happen in a normal operations but it may when for 
	# instance mover detects that encp is gone and returns idle or
	# mover crashes and then restarts
	
	# find mover in the work_at_movers
	found = 0
	for wt in self.work_at_movers.list:
	    if wt['mover'] == self.requestor:
		found = 1     # must do this. Construct. for...else will not
                              # do better 
		break
	if found:
	    self.work_at_movers.remove(wt)
	    format = "Removing work from work at movers queue for idle mover. Work:%s mover:%s"
	    Trace.log(e_errors.INFO, format%(wt,mticket))
	    # tape must be in unmounted state
	    vol_info = self.vcc.inquire_vol(wt['fc']['external_label'])
            # recover volume state if not unmounted
	    if (vol_info['at_mover'][0] != 'unmounted' and
                vol_info['at_mover'][1] == self.requestor):
                mcstate =  self.vcc.update_mc_state(wt['fc']['external_label'])
		format = "vol:%s state recovered to %s. mover:%s"
		Trace.log(e_errors.INFO, format%(wt['fc']['external_label'],
						 mcstate["at_mover"][0], 
						 wt['mover']))
        rq, status = self.schedule(mticket['mover'])
        Trace.trace(11,"SCHEDULE RETURNED %s %s"%(rq, status))
        # no work means we're done
        if status[0] == e_errors.NOWORK:
            return

        if status[0] != e_errors.OK:
	    Trace.log(1,"mover_idle: assertion error w=%s ticket=%"%
                      (rq, mticket))
            raise AssertionError
            
        # ok, we have some work - try to bind the volume
        w = rq.ticket
        # reply now to avoid deadlocks
        format = "%s work on vol=%s mover=%s requester:%s"
        Trace.log(e_errors.INFO, format%\
                       (w["work"],
                       w["fc"]["external_label"],
                       mticket["mover"],
                       w["wrapper"]["uname"]))
        if w.has_key('reject_reason'): del(w['reject_reason'])
        self.pending_work.delete(rq)
        w['times']['lm_dequeued'] = time.time()
        # set the correct volume family for write request
        if w['work'] == 'write_to_hsm' and w['vc']['file_family'] == 'ephemeral':
            w['vc']['volume_family'] = string.join((w['vc']['storage_group'],
                                                    w['fc']['external_label'],
                                                    w['vc']['wrapper']), '.')
        w['vc']['file_family'] = volume_family.extract_file_family(w['vc']['volume_family'])
        Trace.log(e_errors.INFO,"IDLE:sending %s to mover"%(w,))
        self.udpc.send_no_wait(w, mticket['address'])

        w['mover'] = mticket['mover']
        Trace.trace(11, "File Family = %s" % (w['vc']['file_family']))
        self.work_at_movers.append(w)
        work = string.split(w['work'],'_')[0]

        ### XXX are these all needed?
        mticket['external_label'] = w["fc"]["external_label"]
        mticket['current_location'] = None
        mticket['volume_family'] =  w['vc']['volume_family']
        mticket['status'] =  (e_errors.OK, None)
        mticket['volume_status'] = ((None,None),(None,None))

        Trace.trace(11,"MT %s" % (mticket,))


        self.volumes_at_movers.put(mticket)
            
        

    # mover is busy - update volumes_at_movers
    def mover_busy(self, mticket):
        Trace.trace(11,"BUSY RQ %s"%(mticket,))
        self.volumes_at_movers.put(mticket)
        
    # we have a volume already bound - any more work??
    def mover_bound_volume(self, mticket):
	Trace.trace(11, "mover_bound_volume: request: %s"%(mticket,))
        last_work = mticket['last_work']
        # put volume information
        # if this mover is already in volumes_at_movers
        # it will not get updated
        self.volumes_at_movers.put(mticket)
        if self.is_starting():
            # LM needs a certain startup delay before it
            # starts processing mover requests to update
            # its volumes at movers table
            return
        if self.lm_lock == 'pause':
            Trace.trace(11,"LM state is %s no mover request processing" % (self.lm_lock,))
            return
        # just did some work, delete it from queue
        w = self.get_work_at_movers(mticket['vc']['external_label'])
        if w:
            Trace.trace(13,"removing %s  from the queue"%(w,))
            # file family may be changed by VC during the volume
            # assignment. Set file family to what VC has returned
            if mticket['vc']['external_label']:
                vol_info = self.vcc.inquire_vol(mticket['vc']['external_label'])
                w['vc']['volume_family'] = vol_info['volume_family']
                w['vc']['file_family'] = volume_family.extract_file_family(vol_info['volume_family'])
                Trace.trace(11, "FILE_FAMILY=%s" % (w['vc']['file_family'],))  # REMOVE
	    self.work_at_movers.remove(w)

        # see if this volume will do for any other work pending
        rq, status = self.next_work_this_volume(mticket["vc"], last_work, mticket['mover'], mticket['vc']['current_location'])
        Trace.trace(11, "mover_bound_volume: next_work_this_volume returned: %s %s"%(rq,status))
        if status[0] == e_errors.OK:
            w = rq.ticket
            format = "%s next work on vol=%s mover=%s requester:%s"
            Trace.log(e_errors.INFO, format%(w["work"],
					     w["vc"]["external_label"],
					     mticket["mover"],
					     w["wrapper"]["uname"]))
	    w['times']['lm_dequeued'] = time.time()
            if w.has_key('reject_reason'): del(w['reject_reason'])
            Trace.log(e_errors.INFO,"HAVE_BOUND:sending %s to mover"%(w,))
            self.udpc.send_no_wait(w, mticket['address']) 
            self.pending_work.delete(rq)
	    state = 'work_at_mover'
            w['mover'] = mticket['mover']
            self.work_at_movers.append(w)
            # if new work volume is different from mounted
            # which may happen in case of high pri. work
            # update volumes_at_movers
            if w["vc"]["external_label"] != mt['external_label']:
                self.volumes_at_movers.delete(mt)
            # create new mover_info
            work = string.split(w['work'],'_')[0]
            mt = {'mover': mticket['mover'],
                  'external_label' : w["vc"]["external_label"],
                  'current_location' : mticket['vc']['current_location'],
                  'state' : work,
                  'status' : (e_errors.OK, None),
                  'volume_family': w['vc']['volume_family'],
                  'volume_status':((None,None),(None,None))
                  }
            Trace.trace(11,"MT %s" % (mt,))
        
            self.volumes_at_movers.put(mt)
            


        # if the pending work queue is empty, then we're done
        elif  (status[0] == e_errors.NOWORK or
               status[0] == e_errors.VOL_SET_TO_FULL):

            # do not dismount
            return

        # alas
        else:
	    Trace.log(1,"mover_bound_volume: assertion error %s %s"%(w,mticket))
            raise AssertionError


    # if the work is on the awaiting bind list, it is the library manager's
    #  responsibility to retry
    # THE LIBRARY COULD NOT MOUNT THE TAPE IN THE DRIVE AND IF THE MOVER
    # THOUGHT THE VOLUME WAS POISONED, IT WOULD TELL THE VOLUME CLERK.
    # this will be raplaced with error handlers!!!!!!!!!!!!!!!!
    def mover_error(self, ticket):
        Trace.trace(11,"UNILATERAL UNBIND RQ %s"%(ticket,))
        # get the work ticket for the volume
        w = self.get_work_at_movers(ticket["external_label"])
        if w:
            Trace.trace(13,"unilateral_unbind: work_at_movers %s"%(w,))
            self.work_at_movers.remove(w)

    # what is going on
    def getwork(self,ticket):
        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket) # reply now to avoid deadlocks
        # this could tie things up for awhile - fork and let child
        # send the work list (at time of fork) back to client
        if self.fork() != 0:
            return
        self.get_user_sockets(ticket)
        rticket = {}
        rticket["status"] = (e_errors.OK, None)
        rticket["at movers"] = self.work_at_movers.list
        rticket["pending_work"] = self.pending_work.get_queue()
        callback.write_tcp_obj(self.data_socket,rticket)
        self.data_socket.close()
        callback.write_tcp_obj(self.control_socket,ticket)
        self.control_socket.close()
        os._exit(0)

    # get list of suspected volumes 
    def get_suspect_volumes(self,ticket):
        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket) # reply now to avoid deadlocks
        # this could tie things up for awhile - fork and let child
        # send the work list (at time of fork) back to client
        if self.fork() != 0:
            return
        self.get_user_sockets(ticket)
        rticket = {}
        rticket["status"] = (e_errors.OK, None)
        rticket["suspect_volumes"] = self.suspect_volumes.list
        callback.write_tcp_obj(self.data_socket,rticket)
        self.data_socket.close()
        callback.write_tcp_obj(self.control_socket,ticket)
        self.control_socket.close()
	Trace.trace(13,"get_suspect_volumes ")
        os._exit(0)


    # get a port for the data transfer
    # tell the user I'm your library manager and here's your ticket
    def get_user_sockets(self, ticket):
        library_manager_host, library_manager_port, listen_socket =\
                              callback.get_callback()
        listen_socket.listen(4)
        ticket["library_manager_callback_host"] = library_manager_host
        ticket["library_manager_callback_port"] = library_manager_port
        self.control_socket = callback.user_callback_socket(ticket)
        data_socket, address = listen_socket.accept()
        self.data_socket = data_socket
        listen_socket.close()

    # remove work from list of pending works
    def remove_work(self, ticket):
	id = ticket["unique_id"]
	rq = self.pending_work.find(id)
	if not rq:
	    self.reply_to_caller({"status" : (e_errors.NOWORK,"No such work")})
	else:
	    self.pending_work.delete(rq)
	    format = "Request:%s deleted. Complete request:%s"
	    Trace.log(e_errors.INFO, format % (rq.unique_id, rq))
	    self.reply_to_caller({"status" : (e_errors.OK, "Work deleted")})
					 
    # change priority
    def change_priority(self, ticket):
	rq = self.pending_work.find(ticket["unique_id"])
	if not rq:
	    self.reply_to_caller({"status" : (e_errors.NOWORK,"No such work")})
            return
	ret = self.pending_work.change_pri(rq, ticket["priority"])
	if not ret:
	    self.reply_to_caller({"status" : (e_errors.NOWORK, "Attempt to set wrong priority")})
	else:
	    format = "Changed priority to:%s Complete request:%s"
	    Trace.log(e_errors.INFO, format % (ret.pri, ret.ticket))
	    self.reply_to_caller({"status" :(e_errors.OK, "Priority changed")})

    # change state of the library manager
    def change_lm_state(self, ticket):
        if ticket.has_key('state'):
            if ticket['state'] in ('locked', 'ignore', 'unlocked', 'pause'):
                self.lm_lock = ticket['state']
                self.set_lock(ticket['state'])
                ticket["status"] = (e_errors.OK, None)
                Trace.log(e_errors.INFO,"Library manager state is changed to:%s"%(self.lm_lock,))
            else:
                ticket["status"] = (e_errors.WRONGPARAMETER, None)
        else:
            ticket["status"] = (e_errors.KEYERROR,None)
	self.reply_to_caller(ticket)

    # get state of the library manager
    def get_lm_state(self, ticket):
        ticket['state'] = self.lm_lock
        ticket["status"] = (e_errors.OK, None)
	self.reply_to_caller(ticket)


    # This method is needed only for test when new mover code is unavailable
    # REMOVE IT
    def summon(self, ticket):
        if ticket["mover"] != None:
            mv = find_mover_by_name(ticket["mover"])
            if mv:
                # summon this mover
                summon_mover(self, mv, {})
                reply = {"status" :(e_errors.OK, "will summon")}
            else: reply = {"status" :(e_errors.UNKNOWN, "mover is not found")}
        else:
            reply = {"status" :(e_errors.WRONGPARAMETER, "mover must be specified")}
        self.reply_to_caller(reply)

class LibraryManagerInterface(generic_server.GenericServerInterface):

    def __init__(self):
        # fill in the defaults for possible options
	self.summon = 1
        generic_server.GenericServerInterface.__init__(self)

    # define the command line options that are valid
    def options(self):
        return generic_server.GenericServerInterface.options(self)+\
               ["debug", "nosummon"]

    #  define our specific help
    def parameters(self):
        return "library_manager"

    # parse the options like normal but make sure we have a library manager
    def parse_options(self):
        interface.Interface.parse_options(self)
        # bomb out if we don't have a library manager
        if len(self.args) < 1 :
	    self.missing_parameter(self.parameters())
            self.print_help(),
            sys.exit(1)
        else:
            self.name = self.args[0]


if __name__ == "__main__":
    import sys
    Trace.init("LIBMAN")
    Trace.trace(6, "libman called with args "+repr(sys.argv) )

    # get an interface
    intf = LibraryManagerInterface()
    summon = intf.summon

    # get a library manager
    lm = LibraryManager(intf.name, (intf.config_host, intf.config_port))


    get_movers(lm.csc, intf.name) ## REMOVE: this is only for test. REMOVE

    while 1:
        try:
            #Trace.init(intf.name[0:5]+'.libm')
            Trace.init(lm.log_name)
            Trace.log(e_errors.INFO, "Library Manager %s (re)starting"%(intf.name,))
            lm.serve_forever()
	except SystemExit, exit_code:
	    sys.exit(exit_code)
        except:
	    traceback.print_exc()
	    lm.serve_forever_error("library manager")
	    continue
    Trace.trace(1,"Library Manager finished (impossible)")
