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
import setpath

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

#Trace.trace = p

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
        Trace.trace(13,"AtMovers put: at_movers: %s sg_vf: %s" % (self.at_movers, self.sg_vf))

    def delete(self, mover_info):
        Trace.trace(13, "AtMovers delete. before: %s" % (self.at_movers,))
        mover = mover_info['mover']
        if self.at_movers.has_key(mover):
            Trace.log(11, "MOVER %s" % (self.at_movers[mover],))
            storage_group = volume_family.extract_storage_group(self.at_movers[mover]['volume_family'])
            vol_family = self.at_movers[mover]['volume_family']
            self.sg_vf.delete(mover, self.at_movers[mover]['external_label'], storage_group, vol_family) 
            del(self.at_movers[mover])
        Trace.trace(13,"AtMovers delete: at_movers: %s sg_vf: %s" % (self.at_movers, self.sg_vf))

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
            if self.at_movers.has_key(rec[0]): ### DBG: REMOVE
                Trace.trace(12,"busy_volumes: rec %s" % (self.at_movers[rec[0]]['volume_status'][0][1],))
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

    def get_active_movers(self):
        list = []
        for key in self.at_movers.keys():
            list.append(self.at_movers[key])
        return list
    
    # check if a particular volume with given label is busy
    # for read requests
    def is_vol_busy(self, external_label):
        rc = 0
        # see if this volume is in voulemes_at movers list
        for key in self.at_movers.keys():
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
                Trace.trace(11,"send_regret %s" % (ticket,))
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
        self.process_for_bound_vol = 0 # if not 0 volume is bound

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
            Trace.trace(11,"VOL BUSY %s" % (rq.ticket["fc"]["external_label"],))  ## REMOVE!!!
            self.continue_scan = 1
            return rq, None
        # otherwise we have found a volume that has read work pending
        Trace.trace(11,"process_read_request %s"%(rq.ticket,))
        # ok passed criteria. Get request by file location
        if rq.ticket['encp']['adminpri'] < 0: # not a HiPri request
            rq = self.pending_work.get(rq.ticket["fc"]["external_label"])

        ########################################################
        ### from old idle_mover
        # check if the volume for this work had failed on this mover
        Trace.trace(13,"SUSPECT_VOLS %s"%(self.suspect_volumes,))
        suspect_v,suspect_mv = self.is_mover_suspect(requestor, rq.ticket['fc']['external_label'])
        if suspect_mv:
            Trace.trace(11,"suspect mv %s %s" % (suspect_v,suspect_mv)) ## REMOVE!!
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

        # in any case if request SG limit is 0 and temporarily stored rq. SG limit is not,
        # do not update temporarily store rq.


        #############################################
        # REMOVE WHEN V1 IS GONE
        #
        if not rq.ticket['vc'].has_key('volume_family'):
            rq.ticket['vc']['volume_family'] = rq.ticket['vc']['file_family']
        ##############################################
        rq_sg = volume_family.extract_storage_group(rq.ticket['vc']['volume_family'])
        sg_limit = self.get_sg_limit(rq_sg)
        if self.tmp_rq:
            tmp_rq_sg = volume_family.extract_storage_group(self.tmp_rq.ticket['vc']['volume_family'])
            tmp_sg_limit = self.get_sg_limit(tmp_rq_sg)
            if sg_limit != 0:     # replace tmp_rq if rq SG limit is not 0
                # replace tmp_rq based on priority
                if rq.pri > self.tmp_rq.pri:
                    self.tmp_rq = rq
        else: self.tmp_rq = rq
        key_to_check = self.fair_share(rq)
        Trace.trace(11, "key to check %s" % (key_to_check,))  ## REMOVE !!!
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
        #################################################
        # REMOVE WHEN OLD SYSTEM IS GONE
        # backward compatibility
        # cut the wrapper info off volume family
        sg_tmp = volume_family.extract_storage_group(vol_family)
        if sg_tmp == 'unknown':
            vf = string.join((sg_tmp, string.split(vol_family,'.')[1]), '.')
        else: vf = vol_family
        ##################################################
        v = self.vcc.next_write_volume (rq.ticket["vc"]["library"],
                                        rq.ticket["wrapper"]["size_bytes"],
                                        vf, 
                                        rq.ticket["vc"]["wrapper"],
                                        vol_veto_list,
                                        first_found)
        # volume clerk returned error
        Trace.trace(11,"process_write_request: next write volume returned %s" % (v,))
        if v["status"][0] != e_errors.OK:
            if v["status"][0] == e_errors.NOVOLUME:
                if not self.process_for_bound_vol:
                    if wr_en > rq.ticket["vc"]["file_family_width"]:
                        # remove this request and send regret to the client
                        rq.ticket['status'] = v['status']
                        self.send_regret(rq.ticket)
                        self.pending_work.delete(rq)
                    rq = None
            else:
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

        # in any case if request SG limit is 0 and temporarily stored rq. SG limit is not,
        # do not update temporarily store rq.
        rq_sg = volume_family.extract_storage_group(vol_family)
        sg_limit = self.get_sg_limit(rq_sg)
        if self.tmp_rq:
            tmp_rq_sg = volume_family.extract_storage_group(self.tmp_rq.ticket['vc']['volume_family'])
            tmp_sg_limit = self.get_sg_limit(tmp_rq_sg)
            if sg_limit != 0:     # replace tmp_rq if rq SG limit is not 0
                # replace tmp_rq based on priority
                if rq.pri > self.tmp_rq.pri:
                    self.tmp_rq = rq
        else: self.tmp_rq = rq
        
        key_to_check = self.fair_share(rq)
        if key_to_check:
            self.continue_scan = 1
        return rq, key_to_check

    # is there any work for any volume?
    def next_work_any_volume(self, requestor, bound=0):
        Trace.trace(11, "next_work_any_volume")
        self.init_request_selection()
        self.process_for_bound_vol = bound

        # look in pending work queue for reading or writing work
        rq=self.pending_work.get()
        while rq:
            if rq.work == "read_from_hsm":
                rq, key = self.process_read_request(rq, requestor)
                Trace.trace(11,"process_read_request returned %s %s %s" % (rq, key,self.continue_scan))
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
            return (rq, rq.ticket['status'])
        return (None, (e_errors.NOWORK, None))


    # what is next on our list of work?
    def schedule(self, mover, bound=0):
        while 1:
            rq, status = self.next_work_any_volume(mover, bound)
            if (status[0] == e_errors.OK or 
                status[0] == e_errors.NOWORK):
                return rq, status
            # some sort of error, like write work and no volume available
            # so bounce. status is already bad...
            self.pending_work.delete(rq)
            self.send_regret(rq.ticket)
            Trace.trace(11,"schedule: Error detected %s" % (rq.ticket,))

    def check_write_request(self, external_label, rq):
        Trace.trace(11, "check_write_request %s %s"%(external_label, rq.ticket))
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
                (ret['status'][0] == e_errors.VOL_SET_TO_FULL or
                 ret['status'][0] == 'full')):
                return None, ret['status']
        return rq, ret['status']
            


    # is there any work for this volume??  v is a volume info
    # last_work is a last work for this volume
    # corrent location is a current position of the volume
    def next_work_this_volume(self, external_label, vol_family, last_work, requestor, current_location):
        Trace.trace(11, "next_work_this_volume for %s" % (external_label,))
        status = None
        self.init_request_selection()
        self.process_for_bound_vol = 1
        #self.pending_work.wprint()
        # first see if there are any HiPri requests
        rq =self.pending_work.get_admin_request()
        while rq:
            if rq.work == 'read_from_hsm':
                rq, key = self.process_read_request(rq, requestor)
                if self.continue_scan:
                    # before continuing check if it is a request
                    # for v['external_label']
                    if rq.ticket['fc']['external_label'] == external_label: break
                    rq = self.pending_work.get_admin_request(next=1) # get next request
                    continue
                break
            elif rq.work == 'write_to_hsm':
                rq, key = self.process_write_request(rq) 
                if self.continue_scan:
                    rq, status = self.check_write_request(external_label, rq)
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
                rq, status = self.check_write_request(self, external_label, rq)
                if rq and status[0] == e_errors.OK:
                    return rq, status
                
        # no HIPri requests: look in pending work queue for reading or writing work
        self.init_request_selection()
        self.process_for_bound_vol = 1
        # for tape positioning optimization check what was
        # a last work for this volume
        if last_work == 'WRITE':
            # see if there is another work for this volume family
            # disable retrival of HiPri requests as they were
            # already treated above
            rq = self.pending_work.get(vol_family, use_admin_queue=0)
            if not rq:
               rq = self.pending_work.get(external_label, current_location, use_admin_queue=0) 

        else:
            # see if there is another work for this volume
            # disable retrival of HiPri requests as they were
            # already treated above
            rq = self.pending_work.get(external_label, current_location, use_admin_queue=0)
            if not rq:
               rq = self.pending_work.get(vol_family, use_admin_queue=0) 

        exc_limit_rq = None
        if rq:
            # fair share
            storage_group = volume_family.extract_storage_group(vol_family)
            active_volumes = self.volumes_at_movers.active_volumes_in_storage_group(storage_group)
            if len(active_volumes) > self.get_sg_limit(storage_group):
                rq.ticket['reject_reason'] = ('LIMIT_REACHED',None)
                Trace.trace(11, "next_work_this_volume: active work limit exceeded for %s" %
                            (storage_group,))
                # temporarily store this request
                exc_limit_rq = rq
            if exc_limit_rq:
                # if storage group limit for this volume has been exceeded
                # try to get any work
                rq, status = self.schedule(requestor, bound=1)
                Trace.trace(11,"SCHEDULE RETURNED %s %s"%(rq, status))
                # no work means: use what we have
                if status[0] == e_errors.NOWORK:
                    rq = exc_limit_rq
                elif status[0] != e_errors.OK:
                    Trace.log(1,"next_work_this_volume: assertion error w=%s "%
                              (rq.ticket,))
                    raise AssertionError
                # check if it is the same request
                # or request for the same volume
                if exc_limit_rq is not rq:
                    if rq.work == 'write_to_hsm':
                        if rq.ticket['vc']['volume_family'] == vol_family:
                            # same volume family
                            rq = exc_limit_rq
                    else:
                        # read request
                        if rq.ticket['fc']['external_label'] == external_label:
                            rq = exc_limit_rq
                        elif (rq.ticket.has_key('reject_reason') and
                              rq.ticket['reject_reason'][0] == 'LIMIT_REACHED'):
                            rq = exc_limit_rq                            
                
            if rq.work == 'write_to_hsm':
                while rq:
                    Trace.trace(11,"LABEL %s RQQQQQQQ %s" % (external_label, rq))
                    rq, status = self.check_write_request(external_label, rq)
                    Trace.trace(11,"RQ1 %s STAT %s" %(rq,status))
                    if rq: Trace.trace(11,"TICK %s" %(rq.ticket,))
                    if rq and status[0] == e_errors.OK:
                        return rq, status
                    if not rq: break
                    rq = self.pending_work.get(vol_family, next=1, use_admin_queue=0)
            # return read work
            if rq:
                rq.ticket['status'] = (e_errors.OK, None)
                return rq, rq.ticket['status'] 
        
        # try from the beginning
        rq = self.pending_work.get(external_label)
        if rq:
            # return work
            rq.ticket['status'] = (e_errors.OK, None)
            return (rq, rq.ticket['status'])
        if status:
            return (None, status)
        return (None, (e_errors.NOWORK, None))
                
                    
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

            #############################################
            # REMOVE WHEN V1 IS GONE
            #
            if not ticket['vc'].has_key('volume_family'):
                ticket['vc']['volume_family'] = ticket['vc']['file_family']
            ##############################################
                
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
        #work = string.split(w['work'],'_')[0]

        ### XXX are these all needed?
        mticket['external_label'] = w["fc"]["external_label"]
        mticket['current_location'] = None
        mticket['volume_family'] =  w['vc']['volume_family']
        mticket['status'] =  (e_errors.OK, None)
        # update volume status
        # get it directly from volume clerk as mover
        # in the idle state does not have it
        vol_info = self.vcc.inquire_vol(mticket['external_label'])
        mticket['volume_status'] = (vol_info.get('system_inhibit',['Unknown', 'Unknown']),
                                    vol_info.get('user_inhibit',['Unknown', 'Unknown']))
        
         #mticket['operation'] = work

        Trace.trace(11,"MT %s" % (mticket,))
        self.volumes_at_movers.put(mticket)

    # mover is busy - update volumes_at_movers
    def mover_busy(self, mticket):
        Trace.trace(11,"BUSY RQ %s"%(mticket,))
        self.volumes_at_movers.put(mticket)
        
    # we have a volume already bound - any more work??
    def mover_bound_volume(self, mticket):
	Trace.trace(11, "mover_bound_volume: request: %s"%(mticket,))
        last_work = mticket['operation']
        if not mticket['volume_family']:
            # mover restarted with bound volume and it has not
            # all the volume info
            # so go get it
            vol_info = self.vcc.inquire_vol(mticket['external_label'])
            if vol_info['status'][0] == e_errors.OK:
                mticket['volume_family'] = vol_info['volume_family']
                mticket['volume_status'] = (vol_info.get('system_inhibit',['none', 'none']),
                                            vol_info.get('user_inhibit',['none', 'none']))
            
                Trace.trace(11, "mover_bound_volume: updated mover ticket: %s"%(mticket,))
            else:
               Trace.trace(11, "mover_bound_volume: can't update volume info, status:%s"%
                           (vol_info['status'],))
               return
                           
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
        w = self.get_work_at_movers(mticket['external_label'])
        if w:
            Trace.trace(13,"removing %s  from the queue"%(w,))
            # file family may be changed by VC during the volume
            # assignment. Set file family to what VC has returned
            if mticket['external_label']:
                w['vc']['volume_family'] = mticket['volume_family']
                Trace.trace(11, "FILE_FAMILY=%s" % (w['vc']['volume_family'],))  # REMOVE
	    self.work_at_movers.remove(w)

        # see if this volume will do for any other work pending
        rq, status = self.next_work_this_volume(mticket['external_label'], mticket['volume_family'],
                                                last_work, mticket['mover'],
                                                mticket['current_location'])
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
            Trace.log(e_errors.INFO,"HAVE_BOUND:sending %s %s to mover %s %s DEL_DISM %s"%
                      (w['work'],w['wrapper']['pnfsFilename'], mticket['mover'], mticket['address'], w['encp']['delayed_dismount']))
            self.pending_work.delete(rq)
            w['times']['lm_dequeued'] = time.time()
            w['mover'] = mticket['mover']
            self.work_at_movers.append(w)
            self.udpc.send_no_wait(w, mticket['address']) 
            # if new work volume is different from mounted
            # which may happen in case of high pri. work
            # update volumes_at_movers
            if w["vc"]["external_label"] != mticket['external_label']:
                self.volumes_at_movers.delete(mticket)
                mticket['external_label'] = w["vc"]["external_label"]
                # update volume status
                # get it directly from volume clerk as mover
                # in the idle state does not have it
                vol_info = self.vcc.inquire_vol(mticket['external_label'])
                mticket['volume_status'] = (vol_info.get('system_inhibit',['Unknown', 'Unknown']),
                                            vol_info.get('user_inhibit',['Unknown', 'Unknown']))
            # create new mover_info
            mticket['status'] = (e_errors.OK, None)

            #############################################
            # REMOVE WHEN V1 IS GONE
            #
            if not w['vc'].has_key('volume_family'):
                w['vc']['volume_family'] = w['vc']['file_family']
            ##############################################
                
            mticket['volume_family'] = w['vc']['volume_family']
            Trace.trace(11,"mover %s label %s vol_fam %s" % (mticket['mover'], mticket['external_label'],
                                                                  mticket['volume_family']))
        
            self.volumes_at_movers.put(mticket)

        # if the pending work queue is empty, then we're done
        elif  (status[0] == e_errors.NOWORK or
               status[0] == e_errors.VOL_SET_TO_FULL or
               status[0] == 'full'):
            # do not dismount
            return
        elif (w['work'] == 'write_to_hsm' and status[0] == 'full'):
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
    def mover_error(self, mticket):
        Trace.trace(11,"MOVER ERROR RQ %s"%(mticket,))
        self.volumes_at_movers.delete(mticket)
        # get the work ticket for the volume
        w = self.get_work_at_movers(mticket["external_label"])
        if w:
            Trace.trace(13,"mover_error: work_at_movers %s"%(w,))
            self.work_at_movers.remove(w)
        # update suspected volume list
	vol = self.update_suspect_vol_list(mticket['external_label'], 
				mticket['mover'])
        Trace.log(e_errors.INFO,"mover_error updated suspect volume list for %s"%(repr(w),))
	if len(vol['movers']) >= self.max_suspect_movers:
	    w['status'] = (e_errors.NOACCESS, None)

	    # set volume as noaccess
	    v = self.vcc.set_system_noaccess(w['fc']['external_label'])
	    # set volume as read only
	    #v = self.vcc.set_system_readonly(w['fc']['external_label'])
	    label = w['fc']['external_label']

	    #remove entry from suspect volume list
	    self.suspect_volumes.remove(vol)
	    Trace.trace(13,"removed from suspect volume list %s"%(vol,))

	    self.send_regret(w)
	    # send regret to all clients requested this volume and remove
	    # requests from a queue
	    self.flush_pending_jobs(e_errors.NOACCESS, label)
	else:
	    pass

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


    # get active volume known to LM
    def get_active_volumes(self, ticket):
        movers = self.volumes_at_movers.get_active_movers()
        ticket['movers'] = []
        for mover in movers:
            ticket['movers'].append({'mover'          : mover['mover'],
                                     'external_label' : mover['external_label'],
                                     'volume_family'  : mover['volume_family'],
                                     'operation'      : mover['operation'],
                                     'volume_status'  : mover['volume_status']
                                     })
        ticket['status'] = (e_errors.OK, None)
        self.reply_to_caller(ticket)

    # get strage groups
    def storage_groups(self, ticket):
        ticket['storage_groups'] = []
        ticket['storage_groups'] = self.sg_limits
        self.reply_to_caller(ticket)
        
        
class LibraryManagerInterface(generic_server.GenericServerInterface):

    def __init__(self):
        # fill in the defaults for possible options
        generic_server.GenericServerInterface.__init__(self)

    # define the command line options that are valid
    def options(self):
        return generic_server.GenericServerInterface.options(self)+\
               ["debug"]

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

    # get a library manager
    lm = LibraryManager(intf.name, (intf.config_host, intf.config_port))


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
