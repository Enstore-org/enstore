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
import socket
import select
import errno
import fcntl
if sys.version_info < (2, 2, 0):
    import FCNTL #FCNTL is depricated in python 2.2 and later.
    fcntl.F_GETFL = FCNTL.F_GETFL
    fcntl.F_SETFL = FCNTL.F_SETFL
import re
# enstore imports
import setpath
import copy

import volume_clerk_client
import callback
import hostaddr
import dispatching_worker
import generic_server
import event_relay_client
import event_relay_messages
import monitored_server
import enstore_constants
import option
import Trace
import udp_client

import manage_queue
import e_errors
import lm_list
import volume_family
import priority_selector
import mover_constants
import charset
import discipline

KB=1L<<10
MB=1L<<20
GB=1L<<30

def convert_version(version):
    if not version:
        return version
    v1 = version.replace('-','_')
    v2 = v1.replace('_','.')
    s= v2.split('.')
    ipart = 0.
    fract = 0.
    
    for ch in s[0]:
        if ch.isdigit():
            ipart = ipart * 10. +(ord(ch)-ord('0'))*1.
    l = len(s[1])-1
    for i in range(0, len(s[1])):
        if s[1][i].isalpha():
            if l > 0: l = l-1
    for i in range(0, len(s[1])):
        if s[1][l].isdigit():
            fract = fract + (ord(s[1][l])-ord('0'))*0.1**(l+1)
            l = l-1
            if l < 0:
                break
    return ipart+fract


    
def get_storage_group(dict):
    sg = dict.get('storage_group', None)
    if not sg:
	vf = dict.get('volume_family', None)
	if vf:
	    sg = volume_family.extract_storage_group(vf)
    return sg

def log_q_message(dict, q_txt):
    sg = get_storage_group(dict)
    if sg:
	# this message is used to create a plot. do not change the format of the message.
	Trace.log(e_errors.INFO, 
		  "%s request added to %s queue for storage group : %s"%(Trace.MSG_ADD_TO_LMQ,
									 q_txt, sg))

def log_add_to_pending_queue(dict):
    log_q_message(dict, enstore_constants.PENDING)
    
def log_add_to_wam_queue(dict):
    log_q_message(dict, enstore_constants.WAM)
    
## Trace.trace for additional debugging info uses bits >= 11

##############################################################
class SG_FF:
    def __init__(self):
        self.sg = {}
        self.vf = {}

    def delete(self, mover, volume, sg, vf):
        rc = 0
        #if not (mover and volume and sg and vf): return
        Trace.trace(31,"SG:delete mover %s, volume %s, sg %s, vf %s" % (mover, volume, sg, vf))
        if self.sg.has_key(sg) and (mover, volume) in self.sg[sg]:
            self.sg[sg].remove((mover, volume))
            if len(self.sg[sg]) == 0:
                del(self.sg[sg])
        else:
            Trace.log(e_errors.INFO,'can not remove from sg %s %s' % (mover, volume))
            Trace.log(e_errors.INFO, 'SG: %s' % (self.sg,))
            rc = rc - 1
        if self.vf.has_key(vf) and (mover, volume) in self.vf[vf]:
            self.vf[vf].remove((mover, volume))
            if len(self.vf[vf]) == 0:
                del(self.vf[vf])
        else:
            Trace.log(e_errors.INFO,'can not remove from vf %s %s' % (mover, volume))
            Trace.log(e_errors.INFO,'VF: %s' % (self.vf,))
            rc = rc - 1
        return rc

    def delete_mover(self, mover):
        m,v = None, None
        # delete from sg
        for key in self.sg.keys():
            for tpl in self.sg[key]:
                if tpl[0] == mover:
                    m,v = tpl[0], tpl[1]
                    break
            if m and v:
                Trace.trace(31,"delete_mover %s from %s"%(mover,self.sg[key]))
                break
        if m and v:
            self.sg[key].remove((m,v))
            if len(self.sg[key]) == 0:
                del(self.sg[key])
        else:
            Trace.log(e_errors.INFO,'delete_mover can not remove from sg %s %s' % (m, v))
            Trace.log(e_errors.INFO, 'delete_mover SG: %s' % (self.sg,))
            
        # now delete from vf
        m,v = None, None
        for key in self.vf.keys():
            for tpl in self.vf[key]:
                if tpl[0] == mover:
                    m,v = tpl[0], tpl[1]
                    break
            if m and v:
                Trace.trace(31,"delete_mover %s from %s"%(mover,self.vf[key]))
                break
        if m and v:
            self.vf[key].remove((m,v))
            if len(self.vf[key]) == 0:
                del(self.vf[key])
        else:
            Trace.log(e_errors.INFO,'delete_mover can not remove from vf %s %s' % (m, v))
            Trace.log(e_errors.INFO,'delete_mover VF: %s' % (self.vf,))
            
    def put(self, mover, volume, sg, vf):
        self.delete(mover, volume, sg, vf) # delete entry to update content
        if not self.sg.has_key(sg):
            self.sg[sg] = []
        if not self.vf.has_key(vf):
            self.vf[vf] = []
        if not ((mover, volume) in self.sg[sg]):
            self.sg[sg].append((mover,volume))
        if not ((mover, volume) in self.vf[vf]):
            self.vf[vf].append((mover,volume))
        Trace.log(e_errors.INFO, "%s"%(self,))
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
        Trace.trace(31,"put: %s" % (mover_info,))
        if not mover_info['external_label']: return
        if not mover_info['volume_family']: return
        if not mover_info['mover']: return
        storage_group = volume_family.extract_storage_group(mover_info['volume_family'])
        vol_family = mover_info['volume_family']
        mover = mover_info['mover']
        mover_info['updated'] = time.time()
        if self.at_movers.has_key(mover):
            if self.at_movers[mover]['external_label'] != mover_info['external_label']:
                return
        self.at_movers[mover] = mover_info
        self.sg_vf.put(mover, mover_info['external_label'], storage_group, vol_family)
        Trace.trace(32,"AtMovers put: at_movers: %s" % (self.at_movers,))
        Trace.trace(31,"AtMovers put: sg_vf: %s" % (self.sg_vf,))

    def delete(self, mover_info):
        Trace.trace(32, "AtMovers delete. before: %s" % (self.at_movers,))
        Trace.trace(31, "AtMovers delete. before: sg_vf: %s" % (self.sg_vf,))
        mover = mover_info['mover']
        mover_state = mover_info.get('state', None)
        if self.at_movers.has_key(mover):
            Trace.trace(31, "MOVER %s" % (self.at_movers[mover],))
            if  mover_info.has_key('volume_family') and mover_info['volume_family']:
                vol_family = mover_info['volume_family']
            else:
                vol_family = self.at_movers[mover]['volume_family']
            
            if mover_info.has_key('external_label') and mover_info['external_label']:
                label = mover_info['external_label']
            else:
                label = self.at_movers[mover]['external_label']
                # due to the mover bug mticket['volume_family'] may not be a None
                # when mticket['external_label'] is None
                # the following fixes this
                vol_family = self.at_movers[mover]['volume_family']
            #vol_family = self.at_movers[mover]['volume_family']
            #self.sg_vf.delete(mover, self.at_movers[mover]['external_label'], storage_group, vol_family) 
            storage_group = volume_family.extract_storage_group(vol_family)
            if (self.sg_vf.delete(mover, label, storage_group, vol_family) < 0 and
                mover_state == 'IDLE'):
                # the pair (mover, volume) is wrong.
                # This usually happens when mover automatically goes to
                # IDLE after ERROR in cases when the tape mount fails
                self.sg_vf.delete_mover(mover)
                
            del(self.at_movers[mover])
        Trace.trace(32,"AtMovers delete: at_movers: %s" % (self.at_movers,))
        Trace.trace(31,"AtMovers delete: sg_vf: %s" % (self.sg_vf,))

   # return a list of busy volumes for a given volume family
    def busy_volumes (self, volume_family_name):
        Trace.trace(19,"busy_volumes: family=%s"%(volume_family_name,))
        vols = []
        write_enabled = 0
        if not  self.sg_vf.vf.has_key(volume_family_name):
            return vols, write_enabled
        # look in the list of work_at_movers
        Trace.trace(19,"busy_volumes: sg_vf %s" % (self.sg_vf,))
        for rec in self.sg_vf.vf[volume_family_name]:
            vols.append(rec[1])
            if self.at_movers.has_key(rec[0]): ### DBG: REMOVE
                Trace.trace(19,"busy_volumes: rec %s" % (self.at_movers[rec[0]],))
                Trace.trace(19,"busy_volumes: rec %s" % (self.at_movers[rec[0]]['volume_status'][0][1],))
            if self.at_movers.has_key(rec[0]):
                if self.at_movers[rec[0]]['volume_status'][0][0] in (e_errors.NOACCESS, e_errors.NOTALLOWED):
                    continue
                if self.at_movers[rec[0]]['volume_status'][0][1] == 'none':
                    # system inhibit
                    # if volume can be potentially written increase number
                    # of write enabled volumes that are currently at work
                    # further comparison of this number with file family width
                    # tells if write work can be given out
                    write_enabled = write_enabled + 1
                elif self.at_movers[rec[0]]['state'] == 'ERROR':
                    if not (self.at_movers[rec[0]]['volume_status'][0][1] in ("full", "readonly", "migrated")):
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
                Trace.trace(11, "volume %s is active. Mover=%s"%\
                          (external_label, key))
                rc = 1
                break
        return rc

    
##############################################################
       
class PostponedRequests:
    # requests that have been refused because of Limit Reached go into this "list".
    # Initally requests were already sorted by prioroty, so that only one request for a given
    # volume or volume family may be in this list.
    def __init__(self, keep_time):
        self.rq_list = {}
        self.sg_list = {}
        self.keep_time = keep_time
        self.start_time = time.time()

    def init_rq_list(self):
        self.rq_list = {}
        
    # check if Postponed request list has expired
    def list_expired(self):
        return (time.time() - self.start_time > self.keep_time)
        
    def put(self, rq):
        replace = 0
        sg = volume_family.extract_storage_group(rq.ticket['vc']['volume_family'])
        if self.rq_list.has_key(sg):
            if rq.adminpri > -1:
                if rq.adminpri > self.rq_list[sg].adminpri:
                    replace = 1
            else:
                if rq.basepri > self.rq_list[sg].basepri:
                    replace = 1
        else:
            replace = 1
        if replace:
            Trace.trace(16,"postponed_put %s" % (rq,))
            self.rq_list[sg] = rq
        if not self.sg_list.has_key(sg):
            self.sg_list[sg] = 0L

    def get(self):
        if self.rq_list:
            # find the least counter
            l = []
            remove_these = []
            for sg in self.sg_list.keys():
                if self.rq_list.has_key(sg):
                    l.append((self.sg_list[sg], sg))
                else:
                   remove_these.append(sg)
            if len(l) > 1: l.sort()
            Trace.trace(16, "sorted sg_list %s"%(l,))

            for g in remove_these:
                if self.sg_list.has_key(sg):
                    del(self.sg_list[sg])
            # get sg
            sg = l[0][1]
            #import pprint
            #pprint.pprint(self.rq_list)
            Trace.trace(16,"returning %s %s" % (self.rq_list[sg], sg)) 
            return self.rq_list[sg], sg
        return None, None

    def update(self, sg, deficiency=0):
        if self.rq_list.has_key(sg): del(self.rq_list[sg])
        if self.sg_list.has_key(sg):
            self.sg_list[sg] = self.sg_list[sg]+deficiency
            if self.sg_list[sg] < 0:
                self.sg_list[sg] = 0L
            if deficiency >= 0:
                self.sg_list[sg] = self.sg_list[sg]+1
            Trace.trace(16, "postponed update %s %s %s"%(sg, deficiency, self.sg_list[sg]))
        

class LibraryManagerMethods:
    
    def init_suspect_volumes(self):
        # make it method for the capability to reinitialze
        # suspect_volumes outside of this class
        self.suspect_volumes = lm_list.LMList()

    def init_postponed_requests(self, keep_time):
        self.postponed_requests_time = keep_time
        ## place to keep all requests that have been postponed due to
        ## storage group limit reached
        self.postponed_requests = PostponedRequests(self.postponed_requests_time)
       
    def mover_type(self, ticket):
        return ticket.get("mover_type", "Mover")

    def __init__(self, name, csc, sg_limits, min_file_size, max_suspect_movers, max_suspect_volumes):
        self.name = name
        self.min_file_size = min_file_size
        self.max_suspect_movers = max_suspect_movers
        self.max_suspect_volumes = max_suspect_volumes
        # instantiate volume clerk client
        self.csc = csc
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
        self.init_suspect_volumes()
        self.pending_work = manage_queue.Request_Queue()
        

    # get storage grop limit
    def get_sg_limit(self, storage_group):
        if self.sg_limits['use_default']:
            return self.sg_limits['default']
        else:
            return self.sg_limits['limits'].get(storage_group, self.sg_limits['default'])

    def del_udp_client(self, udp_client):
        if not udp_client: return
        # tell server we're done - this allows it to delete our unique id in
        # its dictionary - this keeps things cleaner & stops memory from growing
        try:
            pid = udp_client._os.getpid()
            tsd = udp_client.tsd.get(pid)
            if not tsd:
                return
            for server in tsd.send_done.keys() :
                try:
                    tsd.socket.close()
                except:
                    pass
        except:
            pass
            
    # send a regret
    def send_regret(self, ticket):
        # fork off the regret sender
        if self.fork() != 0:
            return
        try:
            Trace.trace(11,"send_regret %s" % (ticket,))
            control_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            flags = fcntl.fcntl(control_socket.fileno(), fcntl.F_GETFL)
            fcntl.fcntl(control_socket.fileno(), fcntl.F_SETFL, flags | os.O_NONBLOCK)
            # the following insertion is for antispoofing
            host = ticket['wrapper']['machine'][1]
            if ticket.has_key('route_selection') and ticket['route_selection']:
                ticket['mover_ip'] = host
                # bind control socket to data ip
                control_socket.bind((host, 0))
                u = udp_client.UDPClient()
                Trace.trace(10, "sending IP %s to %s" % (host, ticket['routing_callback_addr']))
                try:
                    x= u.send(ticket,ticket['routing_callback_addr'] , 15, 3, 0)
                except errno.errorcode[errno.ETIMEDOUT]:
                    Trace.log(e_errors.ERROR, "error sending to %s (%s)" %
                              (ticket['routing_callback_addr'], os.strerror(errno.ETIMEDOUT)))
                    self.del_udp_client(u)
                    return
                if x.has_key('callback_addr'): ticket['callback_addr'] = x['callback_addr']
                Trace.trace(10, "encp replied with %s"%(x,))
                self.del_udp_client(u)
            Trace.trace(10, "connecting to %s" % (ticket['callback_addr'],))
	    try:
		control_socket.connect(ticket['callback_addr'])
	    except socket.error, detail:
		Trace.log(e_errors.ERROR, "%s %s" %
			  (detail, ticket['callback_addr']))
		#We have seen that on IRIX, when the connection succeds, we
		# get an ISCONN error.
		if hasattr(errno, 'EISCONN') and detail[0] == errno.EISCONN:
		    pass
		#The TCP handshake is in progress.
		elif detail[0] == errno.EINPROGRESS:
		    pass
		else:
		    Trace.log(e_errors.ERROR, "error connecting to %s (%s)" %
			      (ticket['callback_addr'], os.strerror(detail)))
            
            callback.write_tcp_obj(control_socket,ticket)
            control_socket.close()

        except:
            exc,msg,tb=sys.exc_info()
            Trace.log(1,"send_regret %s %s %s"%(exc,msg,ticket))
        os._exit(0)


    def busy_volumes(self, volume_family_name):
        vol_veto_list, wr_en = self.volumes_at_movers.busy_volumes(volume_family_name)
        vols = []
        # look in the list of work_at_movers
        for w in self.work_at_movers.list:
            if w["vc"]["volume_family"] == volume_family_name:
                if w["fc"]["external_label"] in vol_veto_list:
                    continue       # already processed
                else:
                    vol_veto_list.append(w["fc"]["external_label"])
                    permissions = w["vc"].get("system_inhibit", None)
                    if permissions and permissions[1] == 'none':
                        wr_en = wr_en + 1
        return vol_veto_list, wr_en

    # check if a particular volume with given label is busy
    # for read requests
    def is_vol_busy(self, external_label):
        rc = self.volumes_at_movers.is_vol_busy(external_label)
        if rc: return rc
        rc = 0
        for w in self.work_at_movers.list:
            if w["fc"]["external_label"] == external_label:
                rc = 1
                break
        return rc
            
    # remove all pending works
    def flush_pending_jobs(self, status, external_label=None, jobtype=None):
        Trace.trace(12,"flush_pending_jobs: %s"%(external_label,))
        if not external_label: return
        w = self.pending_work.get(external_label)
        while w:
            w.ticket['status'] = (status, None)
            Trace.log(e_errors.INFO,"flush_pending_jobs:work %s"%(w.ticket,))
            self.send_regret(w.ticket)
            self.pending_work.delete(w)
            w = self.pending_work.get(external_label)
        # this is just for test

    # return ticket if given labeled volume is in mover queue
    # only one work can be in the work_at_movers for
    # a given volume. That's why external label is used
    # to identify the work
    def get_work_at_movers(self, external_label, mover):
        rc = {}
        if not external_label: return rc
        if not mover: return rc
        for w in self.work_at_movers.list:
            if w["fc"]["external_label"] == external_label:
                if w.has_key('mover') and w['mover'] == mover: 
                    rc = w
                    break
        return rc

    # if returned ticket has no external label use the
    # following as an alternative to get_work_at_movers
    def get_work_at_movers_m(self, mover):
        rc = {}
        if not mover: return rc
        for w in self.work_at_movers.list:
            if w.has_key('mover') and w['mover'] == mover:
                rc = w
                break
        return rc


    def is_vol_available(self, work, external_label, requestor):
        if work == 'write_to_hsm':
            return {'status':(e_errors.OK, None)}
        map = string.split(external_label,':')[0]
        Trace.trace(37, "label %s address #%s# requestor address #%s#"%(external_label, map,
                                                                    requestor['ip_map']))
        if map == requestor['ip_map']:
            return {'status':(e_errors.OK, None)}
        else:
            return {'status': (e_errors.MEDIA_IN_ANOTHER_DEVICE, None)}

    def init_request_selection(self):
        self.write_vf_list = {}
        self.tmp_rq = None   # need this to temporarily store selected request
        self.processed_admin_requests = []
        # initialize postponed requests list
        if self.postponed_requests.list_expired():
            Trace.trace(16, "postponed list expired")
            self.postponed_requests = PostponedRequests(self.postponed_requests_time)
        else: self.postponed_requests.init_rq_list()
        self.postponed_rq = 0
        self.pending_work.start_cycle()
        
        self.checked_keys = [] # list of checked tag keys
        self.continue_scan = 0
        self.process_for_bound_vol = None # if not None volume is bound

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
        Trace.trace(16, "SG LIMIT %s"%(self.get_sg_limit(storage_group),))
        if len(active_volumes) >= self.get_sg_limit(storage_group):
            rq.ticket["reject_reason"] = ("PURSUING",None)
            Trace.trace(12, "fair_share: active work limit exceeded for %s" % (storage_group,))
            if rq.adminpri > -1:
                self.continue_scan = 1
                return None
            # we have saturated system with requests from the same storage group
            # see if there are pending requests for different storage group
            Trace.trace(17,"PW1")
            tags = self.pending_work.get_tags()
            Trace.trace(16,"tags: %s"%(tags,))
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
        Trace.trace(12,"process_read_request %s"%(rq))

        key_to_check = self.fair_share(rq)
        if key_to_check:
            self.continue_scan = 1
            #return rq, key_to_check
    
        if self.is_vol_busy(rq.ticket["fc"]["external_label"]):
            rq.ticket["reject_reason"] = ("VOL_BUSY",rq.ticket["fc"]["external_label"])
            self.continue_scan = 1
            Trace.trace(12,"process_read_request VOL_BUSY %s"%(rq.ticket["fc"]["external_label"]))
            return rq, key_to_check
        # otherwise we have found a volume that has read work pending
        Trace.trace(12,"process_read_request %s"%(rq.ticket,))
        # ok passed criteria. Get request by file location
        if rq.ticket['encp']['adminpri'] < 0: # not a HiPri request
            Trace.trace(17,"PW2")
            rq = self.pending_work.get(rq.ticket["fc"]["external_label"])
            if rq.ticket['encp']['adminpri'] >= 0: # got a HIPri request
                self.continue_scan = 1
                key_to_check = self.fair_share(rq)
                return rq,key_to_check 
        ########################################################
        ### from old idle_mover
        # check if the volume for this work had failed on this mover
        Trace.trace(15,"SUSPECT_VOLS %s"%(self.suspect_volumes.list,))
        suspect_v,suspect_mv = self.is_mover_suspect(requestor['mover'], rq.ticket['fc']['external_label'])
        if suspect_mv:
            # determine if this volume had failed on the maximal
            # allowed number of movers and, if yes, set volume 
            # as having no access and send a regret: noaccess.
            self.bad_volume(suspect_v, rq.ticket)
            self.continue_scan = 1
            return rq, key_to_check
        ############################################################
        
        # Check the presence of current_location field
        if not rq.ticket["vc"].has_key('current_location'):
            try:
                rq.ticket["vc"]['current_location'] = rq.ticket['fc']['location_cookie']
            except KeyError:
                Trace.log(e_errors.ERROR,"process_read_request loc cookie missing %s" %
                          (rq.ticket,))
                raise KeyError
            

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
        self.postponed_requests.put(rq)
        if self.tmp_rq:
            #tmp_rq_sg = volume_family.extract_storage_group(self.tmp_rq.ticket['vc']['volume_family'])
            #tmp_sg_limit = self.get_sg_limit(tmp_rq_sg)
            if sg_limit != 0:     # replace tmp_rq if rq SG limit is not 0
                # replace tmp_rq based on priority
                if rq.pri > self.tmp_rq.pri:
                    self.tmp_rq = rq
        else: self.tmp_rq = rq
        return rq, key_to_check

    def process_write_request(self, request, requestor):
        self.continue_scan = 0
        rq = request
        key_to_check = self.fair_share(rq)
        Trace.trace(16,"process_write_request: key %s"%(key_to_check,))
        if key_to_check:
            self.continue_scan = 1
            #return rq, key_to_check
        vol_family = rq.ticket["vc"]["volume_family"]
        if rq.adminpri > -1:
            if vol_family in self.processed_admin_requests:
                Trace.trace(16,"return here")
                self.continue_scan = 1
                return None,key_to_check
            else:
               self.processed_admin_requests.append(vol_family) 
        if not self.write_vf_list.has_key(vol_family):
            vol_veto_list, wr_en = self.busy_volumes(vol_family)
            Trace.trace(19,"process_write_request vol veto list:%s, width:%d"%\
                        (vol_veto_list, wr_en))
            self.write_vf_list[vol_family] = {'vol_veto_list':vol_veto_list, 'wr_en': wr_en}
        else:
            vol_veto_list =  self.write_vf_list[vol_family]['vol_veto_list']
            wr_en = self.write_vf_list[vol_family]['wr_en']
        # only so many volumes can be written to at one time
        if wr_en >= rq.ticket["vc"]["file_family_width"]:
            rq.ticket["reject_reason"] = ("VOLS_IN_WORK","")
            self.continue_scan = 1
            return rq, key_to_check
        #Trace.trace(12,"process_write_request: request next write volume for %s" % (vol_family,))

        # before assigning volume check if it is bound for the current family
        if self.process_for_bound_vol not in vol_veto_list:
            # width not exceeded, ask volume clerk for a new volume.
            Trace.trace(9,"process_write_request for %s" % (rq.ticket,))
            v = self.vcc.next_write_volume(rq.ticket["vc"]["library"],
                                           rq.ticket["wrapper"]["size_bytes"]+self.min_file_size,
                                           vol_family, 
                                           vol_veto_list,
                                           first_found=0,
                                           mover=requestor)
            # volume clerk returned error
            Trace.trace(9,"process_write_request: next write volume returned %s" % (v,))
            if v["status"][0] != e_errors.OK:
                rq.ticket["reject_reason"] = (v["status"][0],v["status"][1])
                if v['status'][0] == e_errors.BROKEN:
                    # temporarily save last state:
                    tmp_lock = self.lm_lock
                    self.lm_lock = e_errors.BROKEN
                    if tmp_lock != self.lm_lock:
                        Trace.alarm(e_errors.ERROR,"LM %s goes to %s state" %
                                    (self.name, self.lm_lock))
                    return None, None
                     
                if v["status"][0] == e_errors.NOVOLUME or v["status"][0] == e_errors.QUOTAEXCEEDED:
                    if not self.process_for_bound_vol:
                        #if wr_en > rq.ticket["vc"]["file_family_width"]:
                        
                        # if volume veto list is not empty then work can be done later after
                        # the tape is available again
                        if not vol_veto_list or v["status"][0] == e_errors.QUOTAEXCEEDED:
                            # remove this request and send regret to the client
                            rq.ticket['status'] = v['status']
                            self.send_regret(rq.ticket)
                            self.pending_work.delete(rq)
                        rq = None
                else:
                    rq.ticket["status"] = v["status"]
                    #rq.ticket["reject_reason"] = (v["status"][0],v["status"][1])
                self.continue_scan = 1
                return rq, key_to_check
            else:
                suspect_v,suspect_mv = self.is_mover_suspect(requestor['mover'], v['external_label'])
                if suspect_mv:
                    Trace.log(e_errors.INFO,"mover %s is suspect for %s cannot assign a write work"%
                              (requestor['mover'], v["external_label"]))
                    self.continue_scan = 1
                    return rq, key_to_check
                rq.ticket["status"] = v["status"]
                external_label = v["external_label"]
        else:
           external_label = self.process_for_bound_vol 
                
        # found a volume that has write work pending - return it
        rq.ticket["fc"]["external_label"] = external_label
        rq.ticket["fc"]["size"] = rq.ticket["wrapper"]["size_bytes"]

        # request has passed about all the criterias
        # check if it passes the fair share criteria
        # temprorarily store selected request to use it in case
        # when other request(s) based on fair share criteria
        # for some other reason(s) do not get selected

        # in any case if request SG limit is 0 and temporarily stored rq. SG limit is not,
        # do not update temporarily stored rq.
        rq_sg = volume_family.extract_storage_group(vol_family)
        sg_limit = self.get_sg_limit(rq_sg)
        self.postponed_requests.put(rq)
        if self.tmp_rq:
            #tmp_rq_sg = volume_family.extract_storage_group(self.tmp_rq.ticket['vc']['volume_family'])
            #tmp_sg_limit = self.get_sg_limit(tmp_rq_sg)
            if sg_limit != 0:     # replace tmp_rq if rq SG limit is not 0
                # replace tmp_rq based on priority
                if rq.pri > self.tmp_rq.pri:
                    self.tmp_rq = rq
        else: self.tmp_rq = rq
        
        return rq, key_to_check

    # is there any work for any volume?
    def next_work_any_volume(self, requestor, bound=None):
        Trace.trace(11, "next_work_any_volume")
        self.init_request_selection()
        self.process_for_bound_vol = bound

        # look in pending work queue for reading or writing work
        Trace.trace(17,"PW3")
        rq=self.pending_work.get()
        while rq:
            rej_reason = None
            if rq.ticket.has_key('reject_reason'):
                rej_reason = rq.ticket['reject_reason'][0]
                del(rq.ticket['reject_reason'])
            ## check if there are any additional restrictions
            rc, fun, args, action = self.restrictor.match_found(rq.ticket)
            if rc and fun and action:
                rq.ticket["status"] = (e_errors.OK, None)
                if fun == 'restrict_host_access':
                    callback = rq.ticket.get('callback_addr', None)
                    if callback:
                        host_from_ticket = hostaddr.address_to_name(callback[0])
                    else:
                        host_from_ticket = rq.ticket['wrapper']['machine'][1]
                    
                    args.append(host_from_ticket)
                    ret = apply(getattr(self,fun), args)
                    #Trace.trace(16, "restrict_host_access returned %s"%(ret,))
                    if ret and (action in (e_errors.LOCKED, 'ignore', 'pause', 'reject')):
                        if not (rej_reason == "RESTRICTED_ACCESS"):
                            format = "access delayed for %s : library=%s family=%s requester:%s"
                            Trace.log(e_errors.INFO, format%(rq.ticket['wrapper']['pnfsFilename'],
                                                             rq.ticket["vc"]["library"],
                                                             rq.ticket["vc"]["volume_family"],
                                                             rq.ticket["wrapper"]["uname"]))
                        rq.ticket["reject_reason"] = ("RESTRICTED_ACCESS",None)
                        Trace.trace(17,"PW4")
                        rq = self.pending_work.get(next=1) # get next request
                        continue
            if rq.work == "read_from_hsm":
                rq, key = self.process_read_request(rq, requestor)
                if rq: t = rq.ticket
                else: t = rq
                Trace.trace(16,"process_read_request returned %s %s %s" % (t, key,self.continue_scan))
                    
                if self.continue_scan:
                    if key:
                        Trace.trace(17,"PW5")
                        rq = self.pending_work.get(key)
                    else:
                        Trace.trace(17,"PW6")
                        rq = self.pending_work.get(next=1) # get next request
                    continue
                break
            elif rq.work == "write_to_hsm":
                rq, key = self.process_write_request(rq, requestor)
                if rq: t = rq.ticket
                else: t = rq
                Trace.trace(16,"process_write_request returned %s %s %s" % (t, key,self.continue_scan))
                if self.continue_scan:
                    if key:
                        Trace.trace(17,"PW7")
                        rq = self.pending_work.get(key)
                    else:
                        Trace.trace(17,"PW8")
                        rq = self.pending_work.get(next=1) # get next request
                    continue
                break

            # alas, all I know about is reading and writing
            else:
                Trace.log(e_errors.ERROR,
                          "next_work_any_volume assertion error in next_work_any_volume %s"%(rq.ticket,))
                raise AssertionError
            Trace.trace(17,"PW9")
            rq = self.pending_work.get(next=1)

        if not rq or (rq.ticket.has_key('reject_reason') and rq.ticket['reject_reason'][0] == 'PURSUING'):
            # see if there is a temporary stored request
            Trace.trace(11,"next_work_any_volume: using exceeded mover limit request") 
            #rq = self.tmp_rq
            rq, self.postponed_sg = self.postponed_requests.get()
            Trace.trace(16,"next_work_any_volume: get from postponed %s"%(rq,)) 
            self.postponed_rq = 1 # request comes from postponed requests list
        # check if this volume is ok to work with
        if rq:
            ##if self.tmp_rq:
            ##    Trace.trace(16,"next_work_any_volume: rq.pri %s, tmp_rq.pri %s"%(rq.pri, self.tmp_rq.pri))
            ##    sg_limit = self.get_sg_limit(volume_family.extract_storage_group(rq.ticket["vc"]["volume_family"]))
            ##    if sg_limit == 0:
            ##        if rq.pri < self.tmp_rq.pri:
            ##            rq = self.tmp_rq
            w = rq.ticket
            Trace.trace(11,"check volume %s " % (w['fc']['external_label'],))
            if w["status"][0] == e_errors.OK:
                if self.mover_type(requestor) == 'DiskMover':
                    ret = self.is_vol_available(rq.work,w['fc']['external_label'], requestor)
                else:
                    fsize = w['wrapper'].get('size_bytes', 0L)
                    method = w.get('method', None)
                    if method and method != "read_tape_start":
                        # size has a meaning only for general rq
                        fsize = fsize+self.min_file_size
                    ret = self.vcc.is_vol_available(rq.work,
                                                    w['fc']['external_label'],
                                                    w["vc"]["volume_family"],
                                                    fsize)
                if ret['status'][0] != e_errors.OK:
                    if ret['status'][0] == e_errors.BROKEN:
                        # temporarily save last state:
                        tmp_lock = self.lm_lock
                        self.lm_lock = e_errors.BROKEN
                        if tmp_lock != self.lm_lock:
                            Trace.alarm(e_errors.ERROR,"LM %s goes to %s state" %
                                        (self.name, self.lm_lock))
                        return  None, (e_errors.NOWORK, None)
                    Trace.trace(11,"work can not be done at this volume %s"%(ret,))
                    #w['status'] = ret['status']
                    if not (ret['status'][0] == e_errors.VOL_SET_TO_FULL or
                            ret['status'][0] == 'full' or
                            ret['status'][0] == e_errors.MEDIA_IN_ANOTHER_DEVICE):
                        w['status'] = ret['status']
                        self.pending_work.delete(rq)
                        self.send_regret(w)
                    Trace.log(e_errors.ERROR,
                              "next_work_any_volume: cannot do the work for %s status:%s" % 
                              (rq.ticket['fc']['external_label'], rq.ticket['status'][0]))
                    return None, (e_errors.NOWORK, None)
            else:
                if (w['work'] == 'write_to_hsm' and
                    (w['status'][0] == e_errors.VOL_SET_TO_FULL or
                     w['status'][0] == 'full')):
                    return None, (e_errors.NOWORK, None)
            return (rq, rq.ticket['status'])
        return (None, (e_errors.NOWORK, None))


    # what is next on our list of work?
    def schedule(self, mover, bound=None):
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

    def check_write_request(self, external_label, rq, requestor):
        vol_veto_list, wr_en = self.busy_volumes(rq.ticket['vc']['volume_family'])
        Trace.trace(19, "check_write_request: vet_list %s wr_en %s"%(vol_veto_list, wr_en))
        label = rq.ticket['fc'].get('external_label', external_label)
        if label != external_label:
            # this is a case with admin pri
            # process it carefuly
            # check if tape is already mounted somewhere
            if label in vol_veto_list:
                rq.ticket["reject_reason"] = ("VOLS_IN_WORK","")
                Trace.trace(19, "check_write_request: request for volume %s rejected %s Mounted somwhere else"%
                                (external_label, rq.ticket["reject_reason"]))
                rq.ticket['status'] = ("VOLS_IN_WORK",None)
                return rq, rq.ticket['status']
        external_label = label
        Trace.trace(19, "check_write_request %s %s"%(external_label, rq.ticket))
        if wr_en >= rq.ticket["vc"]["file_family_width"]:
            if not external_label in vol_veto_list:
                #if rq.adminpri < 0: # This allows request with admin pri to go even it exceeds its limit
                rq.ticket["reject_reason"] = ("VOLS_IN_WORK","")
                Trace.trace(19, "check_write_request: request for volume %s rejected %s"%
                            (external_label, rq.ticket["reject_reason"]))
                rq.ticket['status'] = ("VOLS_IN_WORK",None)
                return rq, rq.ticket['status']
                
            
        
        if self.mover_type(requestor) == 'DiskMover':
            ret = self.is_vol_available(rq.work, external_label, requestor)
        else:
            fsize = rq.ticket['wrapper'].get('size_bytes', 0L)
            method = rq.ticket.get('method', None)
            if method and method != "read_tape_start":
                # size has a meaning only for general rq
                fsize = fsize+self.min_file_size
            ret = self.vcc.is_vol_available(rq.work,  external_label,
                                            rq.ticket['vc']['volume_family'],
                                            fsize)
        # this work can be done on this volume
        if ret['status'][0] == e_errors.OK:
            rq.ticket['vc']['external_label'] = external_label
            rq.ticket['status'] = ret['status']
            rq.ticket["fc"]["size"] = rq.ticket["wrapper"]["size_bytes"]
            rq.ticket['fc']['external_label'] = external_label
            return rq, ret['status'] 
        else:
            rq.ticket['reject_reason'] = (ret['status'][0], ret['status'][1])
            if ret['status'][0] == e_errors.BROKEN:
                # temporarily save last state:
                tmp_lock = self.lm_lock
                self.lm_lock = e_errors.BROKEN
                if tmp_lock != self.lm_lock:
                    Trace.alarm(e_errors.ERROR,"LM %s goes to %s state" %
                                (self.name, self.lm_lock))
                return  None,ret['status'] 
            # if work is write_to_hsm and volume has just been set to full
            # return this status for the immediate dismount
            if (rq.work == "write_to_hsm" and
                (ret['status'][0] == e_errors.VOL_SET_TO_FULL or
                 ret['status'][0] == 'full')):
                return None, ret['status']
        return rq, ret['status']
            
    def check_read_request(self, external_label, rq, requestor):
        Trace.trace(11,"check_read_request %s %s %s"%(rq.work,external_label, requestor))
        if self.mover_type(requestor) == 'DiskMover':
            ret = self.is_vol_available(rq.work,external_label, requestor)
        else:
            fsize = rq.ticket['wrapper'].get('size_bytes', 0L)
            ret = self.vcc.is_vol_available(rq.work,  external_label,
                                            rq.ticket['vc']['volume_family'],
                                            fsize)
        Trace.trace(11,"check_read_request: ret %s" % (ret,))
        if ret['status'][0] != e_errors.OK:
            if ret['status'][0] == e_errors.BROKEN:
                # temporarily save last state:
                tmp_lock = self.lm_lock
                self.lm_lock = e_errors.BROKEN
                if tmp_lock != self.lm_lock:
                    Trace.alarm(e_errors.ERROR,"LM %s goes to %s state" %
                                (self.name, self.lm_lock))
                return  None,ret['status'] 
            Trace.trace(11,"work can not be done at this volume %s"%(ret,))
            rq.ticket['status'] = ret['status']
            self.pending_work.delete(rq)
            self.send_regret(rq.ticket)
            Trace.log(e_errors.ERROR,
                      "check_read_request: cannot do the work for %s status:%s" % 
                                  (rq.ticket['fc']['external_label'], rq.ticket['status'][0]))
        else:
            rq.ticket['status'] = (e_errors.OK, None)
        return rq, rq.ticket['status']
        

    # is there any work for this volume??  v is a volume info
    # last_work is a last work for this volume
    # corrent location is a current position of the volume
    def next_work_this_volume(self, external_label, vol_family, last_work, requestor, current_location):
        Trace.trace(11, "next_work_this_volume for %s" % (external_label,))
        status = None
        self.init_request_selection()
        self.process_for_bound_vol = external_label
        #self.pending_work.wprint()
        # first see if there are any HiPri requests
        rq =self.pending_work.get_admin_request()
        while rq:
            # skip over tape read requests they are processed only in the idle state
            method = rq.ticket.get("method", None)
            if method and method == "read_tape_start":
                rq = self.pending_work.get_admin_request(next=1) # get next request
                continue

            rej_reason = None
            if rq.ticket.has_key('reject_reason'):
                rej_reason = rq.ticket['reject_reason'][0]
                del(rq.ticket['reject_reason'])
            ## check if there are any additional restrictions
            rc, fun, args, action = self.restrictor.match_found(rq.ticket)
            if rc and fun and action:
                rq.ticket["status"] = (e_errors.OK, None)
                if fun == 'restrict_host_access':
                    callback = rq.ticket.get('callback_addr', None)
                    if callback:
                        host_from_ticket = hostaddr.address_to_name(callback[0])
                    else:
                        host_from_ticket = rq.ticket['wrapper']['machine'][1]
                    args.append(host_from_ticket)
                    ret = apply(getattr(self,fun), args)
                    if ret and (action in (e_errors.LOCKED, 'ignore', 'pause', 'reject')):
                        if not (rej_reason == "RESTRICTED_ACCESS"):
                            format = "bound:access delayed for %s : library=%s family=%s requester:%s"
                            Trace.log(e_errors.INFO, format%(rq.ticket['wrapper']['pnfsFilename'],
                                                             rq.ticket["vc"]["library"],
                                                             rq.ticket["vc"]["volume_family"],
                                                             rq.ticket["wrapper"]["uname"]))
                        rq.ticket["reject_reason"] = ("RESTRICTED_ACCESS",None)
                        rq = self.pending_work.get_admin_request(next=1) # get next request
                        continue
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
                rq, key = self.process_write_request(rq, requestor) 
                if self.continue_scan:
                    if rq:
                        rq, status = self.check_write_request(external_label, rq, requestor)
                        if rq and status[0] == e_errors.OK: break
                    rq = self.pending_work.get_admin_request(next=1) # get next request
                    continue
                break
        
        if not rq:
            rq = self.tmp_rq
        if rq:
            Trace.trace(14, "s1 rq %s" % (rq.ticket,))
            if rq.work == 'read_from_hsm':
                rq, status = self.check_read_request(external_label, rq, requestor)
                if rq and status[0] == e_errors.OK:
                    return rq, status
            elif rq.work == 'write_to_hsm':
                rq, status = self.check_write_request(external_label, rq, requestor)
                if rq and status[0] == e_errors.OK:
                    return rq, status
                
        # no HIPri requests: look in pending work queue for reading or writing work
        self.init_request_selection()
        self.process_for_bound_vol = external_label
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
        loop = 1
        while rq:
            # skip over tape read requests they are processed only in the idle state
            method = rq.ticket.get("method", None)
            if method and method == "read_tape_start":
                rq = self.pending_work.get_admin_request(next=1) # get next request
                continue
            rej_reason = None
            if rq.ticket.has_key('reject_reason'):
                rej_reason = rq.ticket['reject_reason'][0]
                del(rq.ticket['reject_reason'])
            ## check if there are any additional restrictions
            rc, fun, args, action = self.restrictor.match_found(rq.ticket)
            if rc and fun and action:
                rq.ticket["status"] = (e_errors.OK, None)
                if fun == 'restrict_host_access':
                    callback = rq.ticket.get('callback_addr', None)
                    if callback:
                        host_from_ticket = hostaddr.address_to_name(callback[0])
                    else:
                        host_from_ticket = rq.ticket['wrapper']['machine'][1]
                    args.append(host_from_ticket)
                    ret = apply(getattr(self,fun), args)
                    if ret and (action in (e_errors.LOCKED, 'ignore', 'pause', 'reject')):
                        if not (rej_reason == "RESTRICTED_ACCESS"):
                            format = "bound:access delayed for %s : library=%s family=%s requester:%s"
                            Trace.log(e_errors.INFO, format%(rq.ticket['wrapper']['pnfsFilename'],
                                                             rq.ticket["vc"]["library"],
                                                             rq.ticket["vc"]["volume_family"],
                                                             rq.ticket["wrapper"]["uname"]))
                        rq.ticket["reject_reason"] = ("RESTRICTED_ACCESS",None)
                        rq = self.pending_work.get(external_label,  next=1, use_admin_queue=0)
                        if not rq:
                            rq = self.pending_work.get(vol_family,  next=1, use_admin_queue=0)
                        continue
                    else: break
                else: break
            else: break
            
        if rq:
            Trace.trace(14, "s2 rq %s" % (rq.ticket,))
            # fair share
            storage_group = volume_family.extract_storage_group(vol_family)
            active_volumes = self.volumes_at_movers.active_volumes_in_storage_group(storage_group)
            if len(active_volumes) > self.get_sg_limit(storage_group):
                rq.ticket['reject_reason'] = ('PURSUING',None)
                Trace.trace(11, "next_work_this_volume: active work limit exceeded for %s" %
                            (storage_group,))
                # temporarily store this request
                exc_limit_rq = rq
            if exc_limit_rq:
                # if storage group limit for this volume has been exceeded
                # try to get any work with online priority
                rq, status = self.schedule(requestor, bound=external_label)
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
                    if (rq.ticket.has_key('reject_reason') and
                        rq.ticket['reject_reason'][0] == 'PURSUING'):
                        rq = exc_limit_rq
                        # !!!!!!!!! should I check here if rq has a higher priority than exc_limit_rq??????????
                    elif rq.work == 'write_to_hsm':
                        if rq.ticket['vc']['volume_family'] == vol_family:
                            # same volume family
                            rq = exc_limit_rq
                    else:
                        # read request
                        if rq.ticket['fc']['external_label'] == external_label:
                            rq = exc_limit_rq
                        #elif (rq.ticket.has_key('reject_reason') and
                        #      rq.ticket['reject_reason'][0] == 'PURSUING'):
                        #    rq = exc_limit_rq
                        else:
                            if rq.ticket['encp']['adminpri'] <0:
                                rq = exc_limit_rq
                Trace.trace(14, "s3 rq %s" % (rq.ticket,))
            
            if rq.work == 'write_to_hsm':
                while rq:
                    Trace.trace(14,"LABEL %s RQQQQQQQ %s" % (external_label, rq))
                    rq, status = self.check_write_request(external_label, rq, requestor)
                    Trace.trace(14,"RQ1 %s STAT %s" %(rq,status))
                    if rq: Trace.trace(14,"TICK %s" %(rq.ticket,))
                    if rq and status[0] == e_errors.OK:
                        return rq, status
                    if not rq: break
                    rq = self.pending_work.get(vol_family, next=1, use_admin_queue=0)
            # return read work
            if rq:
                Trace.trace(14, "s4 rq %s" % (rq.ticket,))
                
                rq, status = self.check_read_request(external_label, rq, requestor)
                return rq, status
        
        # try from the beginning
        Trace.trace(14,"try from the beginning")
        rq = self.pending_work.get(external_label, use_admin_queue=0)
        if rq:
            rej_reason = None
            if rq.ticket.has_key('reject_reason'):
                rej_reason = rq.ticket['reject_reason'][0]
                del(rq.ticket['reject_reason'])
            ## check if there are any additional restrictions
            rc, fun, args, action = self.restrictor.match_found(rq.ticket)
            if rc and fun and action:
                rq.ticket["status"] = (e_errors.OK, None)
                if fun == 'restrict_host_access':
                    callback = rq.ticket.get('callback_addr', None)
                    if callback:
                        host_from_ticket = hostaddr.address_to_name(callback[0])
                    else:
                        host_from_ticket = rq.ticket['wrapper']['machine'][1]
                    args.append(host_from_ticket)
                    ret = apply(getattr(self,fun), args)
                    if ret and (action in (e_errors.LOCKED, 'ignore', 'pause', 'reject')):
                        if not (rej_reason == "RESTRICTED_ACCESS"):
                            format = "bound:access delayed for %s : library=%s family=%s requester:%s"
                            Trace.log(e_errors.INFO, format%(rq.ticket['wrapper']['pnfsFilename'],
                                                             rq.ticket["vc"]["library"],
                                                             rq.ticket["vc"]["volume_family"],
                                                             rq.ticket["wrapper"]["uname"]))
                        rq.ticket["reject_reason"] = ("RESTRICTED_ACCESS",None)
                        return (None, (e_errors.NOWORK, None))
            if rq.work == 'read_from_hsm':
                rq, status = self.check_read_request(external_label, rq, requestor)
            else:
                status = (e_errors.OK, None)
                rq.ticket['status'] = status
            # return work
            return (rq, status)
        if status:
            return (None, status)
        return (None, (e_errors.NOWORK, None))
                
                    
    # check if volume is in the suspect volume list
    def is_volume_suspect(self, external_label):
        # remove volumes time in the queue for wich has expired
        if self.suspect_vol_expiration_to:
            # create a list of expired volumes
            now = time.time()
            expired_vols = []
            for vol in self.suspect_volumes.list:
                if now - vol['time'] >= self.suspect_vol_expiration_to:
                    expired_vols.append(vol)
            # cleanup suspect volume list
            if expired_vols:
                for vol in expired_vols:
                    Trace.log(e_errors.INFO,"%s has been removed from suspect volume list due to TO expiration"%
                              (vol['external_label'],))
                    self.suspect_volumes.remove(vol)
                   
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
        # do nothing. All that is being done here must be and is
        # done in mover_error
        return
        ret_val = 0
        if ticket['fc'].has_key('external_label'):
            label = ticket['fc']['external_label']
        else: label = None
        if len(suspect_volume['movers']) >= self.max_suspect_movers:
            ticket['status'] = (e_errors.NOACCESS, None)
            Trace.trace(15,"Number of movers for suspect volume %d" %
                        (len(suspect_volume['movers']),))
                                
            # set volume as noaccess
            v = self.vcc.set_system_noaccess(label)
	    Trace.alarm(e_errors.ERROR, 
			"Volume %s failed on maximal allowed movers (%s)"%(label, 
							     self.max_suspect_movers))
            #remove entry from suspect volume list
            # do not remove anything as this should have been done in the
            # error porcessing
            Trace.alarm(e_errors.ERROR, "Attn. developer. ", rest=suspect_volume)
            return
            self.suspect_volumes.remove(suspect_volume)
            # delete the job
            ## !!! To catch a bug use try
            rq,err = None, None
            try:
                rq, err = self.pending_work.find(ticket)
            except TypeError, detail:
                Trace.log(e_errors.ERROR, "TypeError %s %s"%(detail,ticket))
                if rq:
                    Trace.log(e_errors.ERROR, "Request: %s"%(rq,))
                if err:
                    Trace.log(e_errors.ERROR, "Error: %s"%(err,))
                    
            Trace.trace(15,"bad volume: find returned %s %s"%(rq, err))
            if rq:
                self.pending_work.delete_job(rq)
                self.send_regret(ticket)
            Trace.trace(15,"bad_volume: failed on more than %s for %s"%\
                        (self.max_suspect_movers,suspect_volume))
            ret_val = 1
        return ret_val

    # update suspect volumer list
    def update_suspect_vol_list(self, external_label, mover):
        # update list of suspected volumes
        Trace.trace(15,"SUSPECT VOLUME LIST BEFORE %s"%(self.suspect_volumes.list,))
        if not external_label: return None
        vol_found = 0
        for vol in self.suspect_volumes.list:
            if external_label == vol['external_label']:
                vol_found = 1
                break
        if not vol_found:
            vol = {'external_label' : external_label,
                   'movers' : [],
                   'time':time.time()
                   }
        for mv in vol['movers']:
            if mover == mv:
                break
        else:
            vol['movers'].append(mover)
        if not vol_found:
            self.suspect_volumes.append(vol)
            # send alarm if number of suspect volumes is above a threshold
            if len(self.suspect_volumes.list) >= self.max_suspect_volumes:
                Trace.alarm(e_errors.WARNING, e_errors.ABOVE_THRESHOLD,
                            {"volumes":"Number of suspect volumes is above threshold"}) 
            
        Trace.trace(15, "SUSPECT VOLUME LIST AFTER %s" % (self.suspect_volumes,))
        return vol

class LibraryManager(dispatching_worker.DispatchingWorker,
                     generic_server.GenericServer,
                     LibraryManagerMethods):

    def __init__(self, libman, csc):
        self.name_ext = "LM"
        self.csc = csc
        generic_server.GenericServer.__init__(self, self.csc, libman,
					      self.handle_er_msg)
        self.name = libman
        #   pretend that we are the test system
        #   remember, in a system, there is only one bfs
        #   get our port and host from the name server
        #   exit if the host is not this machine
        self.keys = self.csc.get(libman)
        self.alive_interval = monitored_server.get_alive_interval(self.csc,
                                                                  libman,
                                                                  self.keys)
        enstore_dir = os.environ.get('ENSTORE_DIR','')
        self.pri_sel = priority_selector.PriSelector(self.csc, self.name)

        self.lm_lock = self.get_lock()
        if not self.lm_lock:
            self.lm_lock = 'unlocked'
            self.set_lock(self.lm_lock)
        Trace.log(e_errors.INFO,"Library manager started in state:%s"%(self.lm_lock,))

        # setup a start up delay
        # this delay is needed to update state of the movers
        self.startup_delay = self.keys.get('startup_delay', 32)
        # add this to file size when requesting
        # a tape for writes to avoid FTT_ENOSPC at the end of the tape
        # due to inaccurate REMAINING_BYTES
        min_file_size = self.keys.get('min_file_size',0L)
        # maximal file size
        self.max_file_size = self.keys.get('max_file_size', 2*GB - 2*KB)
        self.max_suspect_movers = self.keys.get('max_suspect_movers',3) # maximal number of movers in the suspect volume list
        self.max_suspect_volumes = self.keys.get('max_suspect_volumes', 100) # maximal number of suspected volumes for alarm generation
        self.blank_error_increment = self.keys.get('blank_error_increment', 5) # this + max_suspect_movers shuold not be more than total number of movers 
        self.time_started = time.time()
        self.startup_flag = 1   # this flag means that LM is in the startup state

        dispatching_worker.DispatchingWorker.__init__(self, (self.keys['hostip'],
                                                             self.keys['port']))
        # setup the communications with the event relay task
        self.resubscribe_rate = 300
        self.erc.start([event_relay_messages.NEWCONFIGFILE], self.resubscribe_rate)

        # start our heartbeat to the event relay process
        self.erc.start_heartbeat(self.name, self.alive_interval, 
                                 self.return_state)

        sg_limits = None
        if self.keys.has_key('storage_group_limits'):
            sg_limits = self.keys['storage_group_limits']
        v = self.keys.get('legal_encp_version','')
        self.legal_encp_version = (v, convert_version(v))
        self.suspect_vol_expiration_to = self.keys.get('suspect_volume_expiration_time',None)
        
        LibraryManagerMethods.__init__(self, self.name,
                                       self.csc,
                                       sg_limits,
                                       min_file_size,
                                       self.max_suspect_movers,
                                       self.max_suspect_volumes)
        self.init_postponed_requests(self.keys.get('rq_wait_time',3600))
        self.restrictor = discipline.Restrictor(self.csc, self.name)
        self.set_udp_client()

        self.volume_assert_list = []

    # check startup flag
    def is_starting(self):
        if self.startup_flag:
            if time.time() - self.time_started > self.startup_delay:
               self.startup_flag = 0
        return self.startup_flag


    def lockfile_name(self):
        d=os.environ.get("ENSTORE_TMP","/tmp")
        return os.path.join(d, "%s_lock"%(self.name,))
    
        
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
            # nowrite -- locked for write requests
            # noread -- locked for read requests

            if self.keys['lock'] in (e_errors.LOCKED, 'unlocked', 'ignore', 'pause', e_errors.NOWRITE, e_errors.NOREAD): 
                return self.keys['lock']
        try:
            lock_file = open(self.lockfile_name(), 'r')
            lock_state = lock_file.read()
            lock_file.close()
        except IOError:
            lock_state = None
        return lock_state
        
    # set lock in a lock file
    def set_lock(self, lock):
        if not os.path.exists(self.lockfile_name()):
           dirname, basename = os.path.split(self.lockfile_name())
           if not os.path.exists(dirname):
               os.makedirs(dirname)
        lock_file = open(self.lockfile_name(), 'w')
        lock_file.write(lock)
        lock_file.close()
        
        
    def set_udp_client(self):
        self.udpc = udp_client.UDPClient()
        self.rcv_timeout = 10 # set receive timeout

    def restrict_host_access(self, storage_group, host, max_permitted, rq_host=None):
        active = 0
        Trace.trace(30, "restrict_host_access(%s,%s,%s %s)"%
                    (storage_group, host, max_permitted, rq_host))
        for w in self.work_at_movers.list:
            callback = w.get('callback_addr', None)
            if callback:
                host_from_ticket = hostaddr.address_to_name(callback[0])
            else:
                host_from_ticket = w['wrapper']['machine'][1]
            
            try:
                if (w['vc']['storage_group'] == storage_group and
                    re.search(host, host_from_ticket)):
                    if rq_host:
                        if  host_from_ticket == rq_host:
                            active = active + 1
                    else:
                        active = active + 1
            except KeyError,detail:
                Trace.log(e_errors.ERROR,"restrict_host_access:%s....%s"%(detail, w))
        Trace.trace(30, "restrict_host_access(%s,%s)"%
                    (active, max_permitted))
        return active >= max_permitted

    def restrict_version_access(self, storage_group, legal_version, ticket):
        Trace.trace(30, "restrict_version_access %s %s %s"%(storage_group,
                                                            legal_version,
                                                            ticket))
        if storage_group == ticket['vc']['storage_group']:
            if ticket.has_key('version'):
                version=ticket['version'].split()[0]
            else:
                version = ''
            if legal_version > version:
                ticket['status'] = (e_errors.VERSION_MISMATCH,
                                    "encp version too old: %s. Must be not older than %s"%(version, legal_version,))
                return 1
        return 0
                
        
        
    def write_to_hsm(self, ticket):
        key = encp_ticket.write_request_ok(w_ticket)
        if key:
            ticket['status'] = (e_errors.MALFORMED,
                                "ticket does not have a mandatory key %s"%(key,))
            self.reply_to_caller(ticket)
        return
            
        if ticket.has_key('vc') and ticket['vc'].has_key('file_family_width'):
            ticket['vc']['file_family_width'] = int(ticket['vc']['file_family_width']) # ff width must be an integer
        if ticket.has_key('version'):
            version=ticket['version'].split()[0]
        else:
            version = ''
        if self.legal_encp_version[0]:
            if self.legal_encp_version[0] > version:
                ticket['status'] = (e_errors.VERSION_MISMATCH,
                                    "encp version too old: %s. Must be not older than %s"%(version, self.legal_encp_version[0],))
                self.reply_to_caller(ticket)
                return
        fsize = ticket['wrapper'].get('size_bytes',0L)
        if fsize > self.max_file_size:
                ticket['status'] = (e_errors.USERERROR,
                                    "file size %s more than max. %s"%(fsize, self.max_file_size))
                self.reply_to_caller(ticket)
                return
            
        ########################
        # The following is a hack to not let sdss use encp
        # older than 2_14
        fn = ticket['outfile'].split('/')
        if 'sdss' in fn:
            if version <= 'v2_14':
                ticket['status'] = (e_errors.VERSION_MISMATCH,
                                    "encp version too old: %s. Must be not older than %s"%(version, 'v2_14',))
                self.reply_to_caller(ticket)
                return
            
         # end of hack
        ##########################
            
        if ticket.has_key('mover'):
            Trace.log(e_errors.WARNING,'input ticket has key mover in it %s'%(ticket,))
            del(ticket['mover'])
        if ticket['vc'].has_key('external_label'):
            del(ticket['vc']['external_label'])
        if ticket['fc'].has_key('external_label'):
            del(ticket['fc']['external_label'])
            
        # data for Trace.notify
        host = ticket['wrapper']['machine'][1]
        work = 'write'
        ff = ticket['vc']['file_family']
        #if self.lm_lock == 'locked' or self.lm_lock == 'ignore':
        if self.lm_lock in (e_errors.LOCKED, 'ignore', 'pause', e_errors.NOWRITE, e_errors.BROKEN):
            if self.lm_lock in  (e_errors.LOCKED, e_errors.NOWRITE):
                ticket["status"] = (self.lm_lock, "Library manager is locked for external access")
            else:
                ticket["status"] = (e_errors.OK, None)
            self.reply_to_caller(ticket)
            Trace.notify("client %s %s %s %s" % (host, work, ff, self.lm_lock))
            return


        # check file family width
        ff_width = ticket["vc"].get("file_family_width", 0)
        if ff_width <= 0:
            ticket["status"] = (e_errors.USERERROR, "wrong file family width %s" % (ff_width,))
            self.reply_to_caller(ticket)
            Trace.notify("client %s %s %s %s" % (host, work, ff, 'rejected'))
            return
            
        ticket["status"] = (e_errors.OK, None)

        for item in ('storage_group', 'file_family', 'wrapper'):
            if ticket['vc'].has_key(item):
                if not charset.is_in_charset(ticket['vc'][item]):
                    ticket['status'] = (e_errors.USERERROR,
                                        "%s contains illegal character"%(item,))
                    self.reply_to_caller(ticket)
                    return

        ## check if there are any additional restrictions
        rc, fun, args, action = self.restrictor.match_found(ticket)
        Trace.trace(30,"match returned %s %s %s %s"% (rc, fun, args, action))
        if fun == 'restrict_host_access':
            action = None   # do nothing here
        if rc and fun and action:
            ticket["status"] = (e_errors.OK, None)
            if fun == 'restrict_version_access':
                #replace last argument with ticket
                #args.remove({})
                # for some reason discipline has begun to return a complete ticket as a
                # last argument on 05/10/2002 after update
                # that's why I excplicitely remove a 3rd argument
                del(args[2])
                args.append(ticket)
            elif fun == 'restrict_host_access':
                callback = ticket.get('callback_addr', None)
                if callback:
                    host_from_ticket = hostaddr.address_to_name(callback[0])
                else:
                    host_from_ticket = ticket['wrapper']['machine'][1]
                args.append(host_from_ticket)
            
            ret = apply(getattr(self,fun), args)
            if ret and (action in (e_errors.LOCKED, 'ignore', 'pause', e_errors.NOWRITE, 'reject')):
                format = "access restricted for %s : library=%s family=%s requester:%s "
                Trace.log(e_errors.INFO, format%(ticket["wrapper"]["fullname"],
                                                 ticket["vc"]["library"],
                                                 ticket["vc"]["file_family"],
                                                 ticket["wrapper"]["uname"]))
                if action in (e_errors.LOCKED, e_errors.NOWRITE):
                    ticket["status"] = (action, "Library manager is locked for external access")
                self.reply_to_caller(ticket)
                Trace.notify("client %s %s %s %s" % (host, work, ff, action))
                return
                

        # check if work is in the at mover list before inserting it
##        for wt in self.work_at_movers.list:
##            # 2 requests cannot have the same output file names
##            if ((wt["wrapper"]['pnfsFilename'] == ticket["wrapper"]["pnfsFilename"]) and
##                (wt['retry'] == ticket['retry'])):
##                ticket['status'] = (e_errors.INPROGRESS,"Operation in progress")
##                break
##            elif wt["unique_id"] == ticket["unique_id"]:
##                break
##        else:
        # check if work is in the at mover list before inserting it
        for wt in self.work_at_movers.list:
            # 2 requests cannot have the same output file names
            if ((wt["wrapper"]['pnfsFilename'] == ticket["wrapper"]["pnfsFilename"]) and
                (wt["unique_id"] == ticket["unique_id"])):
                ticket['status'] = (e_errors.OK,"Operation in progress")
                self.reply_to_caller(ticket)
                format = "write rq. is already in the at mover queue %s (%s) -> %s : library=%s family=%s requester:%s sg:%s"
                Trace.log(e_errors.INFO, format%(ticket["wrapper"]["fullname"],
                                                 ticket["unique_id"],
                                                 ticket["wrapper"]["pnfsFilename"],
                                                 ticket["vc"]["library"],
                                                 ticket["vc"]["file_family"],
                                                 ticket["wrapper"]["uname"],
                                                 ticket['vc']["storage_group"]))
                return
            
        if not ticket.has_key('lm'):
            ticket['lm'] = {'address':self.server_address }
        # set up priorities
        ticket['encp']['basepri'],ticket['encp']['adminpri'] = self.pri_sel.priority(ticket)
	log_add_to_pending_queue(ticket['vc'])
	# put ticket into request queue
        rq, status = self.pending_work.put(ticket)
        ticket['status'] = (status, None)
                
        self.reply_to_caller(ticket) # reply now to avoid deadlocks

        if status == e_errors.OK:
            if not rq:
                format = "write rq. is already in the queue %s (%s) -> %s : library=%s family=%s requester:%s volume_family:%s"
            else:
                format = "write Q'd %s (%s) -> %s : library=%s family=%s requester:%s volume_family:%s"
            Trace.log(e_errors.INFO, format%(ticket["wrapper"]["fullname"],
                                             ticket["unique_id"],
                                             ticket["wrapper"]["pnfsFilename"],
                                             ticket["vc"]["library"],
                                             ticket["vc"]["file_family"],
                                             ticket["wrapper"]["uname"],
                                             ticket['vc']["volume_family"]))
            Trace.notify("client %s %s %s %s" % (host, work, ff, 'queued'))
            

    def read_from_hsm(self, ticket):
        key = encp_ticket.read_request_ok(w_ticket)
        if key:
            ticket['status'] = (e_errors.MALFORMED,
                                "ticket does not have a mandatory key %s"%(key,))
            self.reply_to_caller(ticket)
        return
        method = ticket.get('method', None)
        if method and method == 'read_next': # this request must go directly to mover
            ticket['status'] = (e_errors.USERERROR, "Wrong method used %s"%(method,))
            self.reply_to_caller(ticket)
            return
        if ticket.has_key('vc') and ticket['vc'].has_key('file_family_width'):
            ticket['vc']['file_family_width'] = int(ticket['vc']['file_family_width']) # ff width must be an integer
        if ticket.has_key('version'):
            version=ticket['version'].split()[0]
        else:
            version = ''
        if self.legal_encp_version[0]:
            if self.legal_encp_version[0] > version:
                ticket['status'] = (e_errors.VERSION_MISMATCH,
                                    "encp version too old: %s. Must be not older than %s"%(version, self.legal_encp_version[0],))
                self.reply_to_caller(ticket)
                return
        ########################
        # The following is a hack to not let sdss use encp
        # older than 2_14
        fn = ticket['infile'].split('/')
        if 'sdss' in fn:
            if version <= 'v2_14':
                ticket['status'] = (e_errors.VERSION_MISMATCH,
                                    "encp version too old: %s. Must be not older than %s"%(version, 'v2_14',))
                self.reply_to_caller(ticket)
                return
        # end of hack
        ##########################
        if ticket.has_key('mover'):
            Trace.log(e_errors.WARNING,'input ticket has key mover in it %s'%(ticket,))
            del(ticket['mover'])
        # data for Trace.notify
        host = ticket['wrapper']['machine'][1]
        work = 'read'
        vol = ticket['fc']['external_label']
        #if self.lm_lock == 'locked' or self.lm_lock == 'ignore':
        if self.lm_lock in (e_errors.LOCKED, 'ignore', 'pause', e_errors.NOREAD, e_errors.BROKEN):
            if self.lm_lock in (e_errors.LOCKED, e_errors.NOREAD):
                ticket["status"] = (self.lm_lock, "Library manager is locked for external access")
            else:
                ticket["status"] = (e_errors.OK, None)
            self.reply_to_caller(ticket)
            Trace.notify("client %s %s %s %s" % (host, work, vol, self.lm_lock))
            return
        ## check if there are any additional restrictions
        rc, fun, args, action = self.restrictor.match_found(ticket)
        Trace.trace(30,"match returned %s %s %s %s"% (rc, fun, args, action))
        if fun == 'restrict_host_access':
            action = None    # do nothing here
        if rc and fun and action:
            ticket["status"] = (e_errors.OK, None)
            if fun == 'restrict_version_access':
                #replace last argument with ticket
                #args.remove({})
                # for some reason discipline has begun to return a complete ticket as a
                # last argument on 05/10/2002 after update
                # that's why I excplicitely remove a 3rd argument
                del(args[2])
                args.append(ticket)
            elif fun == 'restrict_host_access':
                callback = ticket.get('callback_addr', None)
                if callback:
                    host_from_ticket = hostaddr.address_to_name(callback[0])
                else:
                    host_from_ticket = ticket['wrapper']['machine'][1]
                args.append(host_from_ticket)
            ret = apply(getattr(self,fun), args)
            if ret and (action in (e_errors.LOCKED, 'ignore', 'pause', e_errors.NOREAD, 'reject')):
                format = "access restricted for %s : library=%s family=%s requester:%s"
                Trace.log(e_errors.INFO, format%(ticket['wrapper']['pnfsFilename'],
                                                 ticket["vc"]["library"],
                                                 ticket["vc"]["volume_family"],
                                                 ticket["wrapper"]["uname"]))
                if action in (e_errors.LOCKED, e_errors.NOREAD):
                    ticket["status"] = (action, "Library manager is locked for external access")
                self.reply_to_caller(ticket)
                Trace.notify("client %s %s %s %s" % (host, work, vol, action))
                return

        # check if this volume is OK
        v = self.vcc.inquire_vol(ticket['fc']['external_label'])
        if v['status'][0] != e_errors.OK:
            Trace.log(e_errors.ERROR, "read_from_hsm: can't update vloume info %s"%
                      (v['status'][0],))
            return
            
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
            Trace.notify("client %s %s %s %s" % (host, work, vol, 'rejected'))
            return

        if not ticket.has_key('lm'):
            ticket['lm'] = {'address' : self.server_address}

        # check if work is in the at mover list before inserting it
        format = None
        for wt in self.work_at_movers.list:
            if wt["unique_id"] == ticket["unique_id"]:
                status = e_errors.OK
                ticket['status'] = (status, None)
                rq = None
                format = "read rq. is already in the at mover queue %s (%s) -> %s : library=%s family=%s requester:%s"
                
                break
        else:
            # set up priorities
            ticket['encp']['basepri'],ticket['encp']['adminpri'] = self.pri_sel.priority(ticket)
	    log_add_to_pending_queue(ticket['vc'])
            # put ticket into request queue
            rq, status = self.pending_work.put(ticket)
            ticket['status'] = (status, None)
        self.reply_to_caller(ticket) # reply now to avoid deadlocks
        if status == e_errors.OK:
            if not rq:
                if not format:
                    format = "read rq. is already in the queue %s (%s) -> %s : library=%s family=%s requester:%s"
                file1 = ticket['wrapper']['fullname']
                file2 = ticket['wrapper']['pnfsFilename']
            else:
                format = "read Q'd %s (%s) -> %s : library=%s family=%s requester:%s"
                file1 = ticket['wrapper']['pnfsFilename']
                file2 = ticket['wrapper']['fullname']

            #############################################
            # REMOVE WHEN V1 IS GONE
            #
            if not ticket['vc'].has_key('volume_family'):
                ticket['vc']['volume_family'] = ticket['vc']['file_family']
            ##############################################
                
            Trace.log(e_errors.INFO, format%(file1, ticket['unique_id'], file2,
                                             ticket["vc"]["library"],
                                             ticket["vc"]["volume_family"],
                                             ticket["wrapper"]["uname"]))
            Trace.notify("client %s %s %s %s" % (host, work, vol, 'queued'))

    # mover is idle - see what we can do
    def mover_idle(self, mticket):
        Trace.trace(11,"IDLE RQ %s"%(mticket,))
        
        # mover is idle remove it from volumes_at_movers
        self.volumes_at_movers.delete(mticket)
        if self.is_starting():
            # LM needs a certain startup delay before it
            # starts processing mover requests to update
            # its volumes at movers table
            self.reply_to_caller({'work': 'no_work'})
            return
        
        if self.lm_lock in ('pause', e_errors.BROKEN):
            Trace.trace(11,"LM state is %s no mover request processing" % (self.lm_lock,))
            self.reply_to_caller({'work': 'no_work'})
            return
            
        self.requestor = mticket

        # check if there is a work for this mover in work_at_movers list
        # it should not happen in a normal operations but it may when for 
        # instance mover detects that encp is gone and returns idle or
        # mover crashes and then restarts
        
        # find mover in the work_at_movers
        found = 0
        Trace.trace(11, "work_at_movers: %s" % (self.work_at_movers.list,))
        for wt in self.work_at_movers.list:
            Trace.trace(11, "work_at_movers: mover %s id %s" % (wt['mover'], wt['unique_id']))
            if wt['mover'] == self.requestor['mover']:
                found = 1     # must do this. Construct. for...else will not
                              # do better 
                break
        if found:
            mover_rq_unique_id = mticket.get('unique_id',None)
            work_at_mover_unique_id = wt.get('unique_id', None)
            method = wt.get('method', None)
            if method == 'read_tape_start':
                # this was a tape copy request
                # it requires a special way of comparing request ids
                if mover_rq_unique_id:
                    s1 = mover_rq_unique_id.split("-")
                    mover_rq_unique_id = string.join(s1[:-1], "-")
                s1 = work_at_mover_unique_id.split("-")
                work_at_mover_unique_id = string.join(s1[:-1], "-")
                Trace.trace(11, "m_id %s w_id %s"%(mover_rq_unique_id,work_at_mover_unique_id))  
            # check if it is a backed up request
            if mover_rq_unique_id and mover_rq_unique_id != work_at_mover_unique_id:
                Trace.trace(15,"found backed up mover %s" % (mticket['mover'],))
                self.reply_to_caller({'work': 'no_work'})
                return
            self.work_at_movers.remove(wt)
            format = "Removing work from work at movers queue for idle mover. Work:%s mover:%s"
            Trace.log(e_errors.INFO, format%(wt,mticket))
        rq, status = self.schedule(mticket)
        Trace.trace(11,"SCHEDULE RETURNED %s %s"%(rq, status))

        # no work means we're done
        if status[0] == e_errors.NOWORK:
            ##Before actually saying we are done, if any volume assert
            # requests are pending, handle them.
            if self.volume_assert_list:
                for i in range(len(self.volume_assert_list)):
                    external_label = \
                         self.volume_assert_list[i]['vc'].get('external_label',
                                                              None)
                    if external_label:
                        if not self.volumes_at_movers.is_vol_busy(
                            external_label):
                            #Add some items to the dictionary.
                            self.volume_assert_list[i]['mover'] = \
                                                              mticket['mover']
                            #Update the dequeued time.
                            self.volume_assert_list[i]['times']['lm_dequeued'] = \
                                time.time()
                            #Add the volume and mover to the list of currently
                            # busy volumes and movers.
                            self.work_at_movers.append(
                                self.volume_assert_list[i])
                            self.volumes_at_movers.put(mticket)
                            #Record this action in log file.
                            Trace.log(e_errors.INFO,
                                      "IDLE:sending %s to mover" %
                                      (self.volume_assert_list[i],))

                            #Tell the mover it has something to do.
                            self.reply_to_caller(self.volume_assert_list[i])
                            #Remove the job from the list of volumes to check.
                            del self.volume_assert_list[i]
                            break
                        else:
                            self.reply_to_caller({'work': 'no_work'})
                    else:
                        self.reply_to_caller({'work': 'no_work'})
            else:
                self.reply_to_caller({'work': 'no_work'})
            return

        if status[0] != e_errors.OK:
            self.reply_to_caller({'work': 'no_work'})
            Trace.log(e_errors.ERROR,"mover_idle: assertion error w=%s ticket=%s"%(rq, mticket))
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
            w['vc']['volume_family'] = volume_family.make_volume_family(w['vc']['storage_group'],
                                                    w['fc']['external_label'],
                                                    w['vc']['wrapper'])
        w['vc']['file_family'] = volume_family.extract_file_family(w['vc']['volume_family'])

        w['mover'] = mticket['mover']
        Trace.trace(11, "File Family = %s" % (w['vc']['file_family']))
	log_add_to_wam_queue(w['vc'])
        self.work_at_movers.append(w)
        #work = string.split(w['work'],'_')[0]
        Trace.log(e_errors.INFO,"IDLE:sending %s to mover"%(w,))
        self.reply_to_caller(w)
        #self.udpc.send_no_wait(w, mticket['address'])

        ### XXX are these all needed?
        mticket['external_label'] = w["fc"]["external_label"]
        mticket['current_location'] = None
        mticket['volume_family'] =  w['vc']['volume_family']
        mticket['unique_id'] =  w['unique_id']
        mticket['status'] =  (e_errors.OK, None)
        # update volume status
        # get it directly from volume clerk as mover
        # in the idle state does not have it
        if self.mover_type(mticket) is 'DiskMover':
            mticket['volume_status'] = (['none', 'none'], ['none', 'none'])
        else:
            Trace.trace(29,"inquire_vol")
            vol_info = self.vcc.inquire_vol(mticket['external_label'])
            mticket['volume_status'] = (vol_info.get('system_inhibit',['Unknown', 'Unknown']),
                                        vol_info.get('user_inhibit',['Unknown', 'Unknown']))
        
         #mticket['operation'] = work

        Trace.trace(11,"MT %s" % (mticket,))
        self.volumes_at_movers.put(mticket)
        Trace.trace(16,"IDLE:postponed%s %s"%(self.postponed_requests.sg_list,self.postponed_requests.rq_list))
        if self.postponed_rq:
            #v_count = len(self.volumes_at_movers.active_volumes_in_storage_group(self.postponed_sg))
            self.postponed_requests.update(self.postponed_sg, 1)

    # mover is busy - update volumes_at_movers
    def mover_busy(self, mticket):
        Trace.trace(11,"BUSY RQ %s"%(mticket,))
        library = mticket.get('library', None)
        if library and library != self.name.split(".")[0]:
            # this mover is currently assigned to another library
            # remove this mover from volumes_at_movers
            movers = self.volumes_at_movers.get_active_movers()
            movers_to_delete = []
            for mv in movers:
                if mticket['mover'] == mv['mover']:
                    movers_to_delete.append(mv)
            if movers_to_delete:
                for mv in movers_to_delete:
                    Trace.trace(11, "mover_busy removing from at movers %s"%(mv,))
                    self.volumes_at_movers.delete(mv)
            return
        state = mticket.get('state', None)
        if state is 'IDLE':
            # mover dismounted a volume on a request to mount another one
            self.volumes_at_movers.delete(mticket)
        else: self.volumes_at_movers.put(mticket)
        # do not reply to mover as it does not 
        
    # we have a volume already bound - any more work??
    def mover_bound_volume(self, mticket):
        Trace.trace(11, "mover_bound_volume: request: %s"%(mticket,))
        library = mticket.get('library', None)
        if library and library != self.name.split(".")[0]:
            self.reply_to_caller({'work': 'no_work'})
            return
            
        if not mticket['external_label']:
            Trace.log(e_errors.ERROR,"mover_bound_volume: mover request with suspicious volume label %s" %
                      (mticket['external_label'],))
            self.reply_to_caller({'work': 'no_work'})
            return
        last_work = mticket['operation']
        if not mticket['volume_family']:
            # mover restarted with bound volume and it has not
            # all the volume info
            # so go get it
            if self.mover_type(mticket) is 'DiskMover':
                mticket['volume_status'] = (['none', 'none'], ['none', 'none'])
            else:
                vol_info = self.vcc.inquire_vol(mticket['external_label'])
                if vol_info['status'][0] == e_errors.OK:
                    mticket['volume_family'] = vol_info['volume_family']
                    mticket['volume_status'] = (vol_info.get('system_inhibit',['none', 'none']),
                                                vol_info.get('user_inhibit',['none', 'none']))

                    Trace.trace(11, "mover_bound_volume: updated mover ticket: %s"%(mticket,))
                else:
                   Trace.log(e_errors.ERROR, "mover_bound_volume: can't update volume info, status:%s"%
                               (vol_info['status'],))
                   self.reply_to_caller({'work': 'no_work'})
                   return
                           
        # put volume information
        # if this mover is already in volumes_at_movers
        # it will not get updated
        self.volumes_at_movers.put(mticket)
        transfer_deficiency = mticket.get('transfer_deficiency', 1)
        sg = volume_family.extract_storage_group(mticket['volume_family'])
        self.postponed_requests.update(sg, 1)
        if self.is_starting():
            # LM needs a certain startup delay before it
            # starts processing mover requests to update
            # its volumes at movers table
            self.reply_to_caller({'work': 'no_work'})
            return
        # just did some work, delete it from queue
        w = self.get_work_at_movers(mticket['external_label'], mticket['mover'])
        if w:
            # check if it is a backed up request
            if mticket['unique_id'] and mticket['unique_id'] != w['unique_id']:
                Trace.trace(15,"found backed up mover %s " % (mticket['mover'], ))
                self.reply_to_caller({'work': 'no_work'})
                return
            Trace.trace(18,"removing %s  from the queue"%(w,))
            # file family may be changed by VC during the volume
            # assignment. Set file family to what VC has returned
            if mticket['external_label']:
                w['vc']['volume_family'] = mticket['volume_family']
                Trace.trace(18, "FILE_FAMILY=%s" % (w['vc']['volume_family'],))  # REMOVE
            self.work_at_movers.remove(w)

        if self.lm_lock in ('pause', e_errors.BROKEN):
            Trace.trace(18,"LM state is %s no mover request processing" % (self.lm_lock,))
            self.reply_to_caller({'work': 'no_work'})
            return
        # see if this volume will do for any other work pending
        rq, status = self.next_work_this_volume(mticket['external_label'], mticket['volume_family'],
                                                last_work, mticket,
                                                mticket['current_location'])
        Trace.trace(18, "mover_bound_volume: next_work_this_volume returned: %s %s"%(rq,status))
        if status[0] == e_errors.OK:
            w = rq.ticket

            format = "%s next work on vol=%s mover=%s requester:%s"
            try:
                Trace.log(e_errors.INFO, format%(w["work"],
                                                 w["vc"]["external_label"],
                                                 mticket["mover"],
                                                 w["wrapper"]["uname"]))
            except KeyError:
                Trace.log(e_errors.ERROR, "mover_bound_volume: Bad ticket: %s"%(w,))
                self.reply_to_caller({'work': 'no_work'})
                return
            w['times']['lm_dequeued'] = time.time()
            if w.has_key('reject_reason'): del(w['reject_reason'])
            Trace.log(e_errors.INFO,"HAVE_BOUND:sending %s %s to mover %s %s DEL_DISM %s"%
                      (w['work'],w['wrapper']['pnfsFilename'], mticket['mover'],
                       mticket['address'], w['encp']['delayed_dismount']))
            Trace.trace(18, "HAVE_BOUND: Ticket %s"%(w,))
            self.pending_work.delete(rq)
            w['times']['lm_dequeued'] = time.time()
            w['mover'] = mticket['mover']
	    log_add_to_wam_queue(w['vc'])
            self.work_at_movers.append(w)
            Trace.log(e_errors.INFO,"HAVE_BOUND:sending %s to mover"%(w,))
            self.reply_to_caller(w)
            #self.udpc.send_no_wait(w, mticket['address']) 
            # if new work volume is different from mounted
            # which may happen in case of high pri. work
            # update volumes_at_movers
            if w["vc"]["external_label"] != mticket['external_label']:
                # update volumes_at_movers
                # perhaps this is a hipri request forcing a tape replacement
                self.volumes_at_movers.delete(mticket)
                mticket['external_label'] = w["vc"]["external_label"]
                # update volume status
                # get it directly from volume clerk as mover
                # in the idle state does not have it
                if self.mover_type(mticket) is 'DiskMover':
                    mticket['volume_status'] = (['none', 'none'], ['none', 'none'])
                else:
                    vol_info = self.vcc.inquire_vol(mticket['external_label'])
                    if vol_info['status'][0] != e_errors.OK:
                       Trace.log(e_errors.ERROR, "mover_bound_volume 2: can't update volume info, status:%s"%
                                   (vol_info['status'],))
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
        
            #sg = volume_family.extract_storage_group(mticket['volume_family'])
            #self.postponed_requests.update(sg, transfer_deficiency)
            self.volumes_at_movers.put(mticket)

        # if the pending work queue is empty, then we're done
        elif  (status[0] == e_errors.NOWORK or
               status[0] == e_errors.VOL_SET_TO_FULL or
               status[0] == 'full'):
            ret_ticket = {'work': 'no_work'}
            if (status[0] == e_errors.VOL_SET_TO_FULL or
                status[0] == 'full'):
                # update at_movers information
                vol_stat = mticket['volume_status']
                s0 = [vol_stat[0][0],status[0]]
                s1 = vol_stat[1]
                mticket['volume_status'] = (s0, s1)
                Trace.trace(11, "mover_bound_volume: update at_movers %s"%(mticket['volume_status'],))
                self.volumes_at_movers.put(mticket)
                ret_ticket = {'work':'update_volume_info',
                              'external_label': mticket['external_label']
                              }
            # do not dismount
            self.reply_to_caller(ret_ticket)
            return
        else:
            Trace.log(e_errors.ERROR,"HAVE_BOUND: .next_work_this_volume returned %s:"%(status,))
            self.reply_to_caller({'work': 'no_work'})
            return


    # if the work is on the awaiting bind list, it is the library manager's
    #  responsibility to retry
    # THE LIBRARY COULD NOT MOUNT THE TAPE IN THE DRIVE AND IF THE MOVER
    # THOUGHT THE VOLUME WAS POISONED, IT WOULD TELL THE VOLUME CLERK.
    # this will be raplaced with error handlers!!!!!!!!!!!!!!!!
    def mover_error(self, mticket):
        Trace.log(e_errors.ERROR,"MOVER ERROR RQ %s"%(mticket,))
        library = mticket.get('library', None)
        if library and library != self.name.split(".")[0]:
            return
        self.volumes_at_movers.put(mticket)
        #if mticket.has_key(['volume_family']):
        #    sg = volume_family.extract_storage_group(mticket['volume_family'])
        #    self.postponed_requests.update(sg, -1)
        # get the work ticket for the volume
        if mticket['external_label']:
            w = self.get_work_at_movers(mticket['external_label'], mticket['mover'])
        else:
            # use alternative fuction
            w = self.get_work_at_movers_m(mticket['mover'])
        if w:
            Trace.trace(11,"mover_error: work_at_movers %s"%(w,))
            self.work_at_movers.remove(w)
        if ((mticket['state'] == mover_constants.OFFLINE) or # mover finished request and went offline
            ((mticket['status'][0] == e_errors.ENCP_GONE) and mticket['state'] != 'HAVE_BOUND')):
            self.volumes_at_movers.delete(mticket)
            return
        if mticket.has_key('returned_work') and mticket['returned_work']:
            # put this ticket back into the pending queue
            # if work is write_to_hsm remove currently assigned volume label
            # because later it may be different
            d= mticket['returned_work']
            if d['work'] == 'write_to_hsm':
                if d['fc'].has_key('external_label'): del(d['fc']['external_label'])
                if d['vc'].has_key('external_label'): del(d['vc']['external_label'])
                
            del(mticket['returned_work']['mover'])
            Trace.trace(11, "mover_error put returned work back to pending queue %s"%
                        (mticket['returned_work'],))
            rq, status = self.pending_work.put(mticket['returned_work'])
            # return
        
        # update suspected volume list if error source is ROBOT or TAPE
        error_source = mticket.get('error_source', 'none')
        if error_source in ("ROBOT", "TAPE"):
            vol = self.update_suspect_vol_list(mticket['external_label'], 
                                    mticket['mover'])
            Trace.log(e_errors.INFO,"mover_error updated suspect volume list for %s"%(mticket['external_label'],))
            if vol:
                # need a special processing for FTT_EBLANK. For 9940A tape drives
                # it is mainly a firmware bug, but we need to make tape to not
                # go NOACCESS in such a case.
                ftt_eblank_error = (mticket['status'][0] == e_errors.READ_ERROR
                                    and mticket['status'][1] and
                                    ((mticket['status'][1] == 'FTT_EBLANK') or mticket['status'][1] == 'FTT_SUCCESS'))
                if ((len(vol['movers']) >= self.max_suspect_movers and not ftt_eblank_error) or
                    (len(vol['movers']) >= self.max_suspect_movers + self.blank_error_increment and ftt_eblank_error)):

                    if w:
                        w['status'] = (e_errors.NOACCESS, None)

                    # set volume as noaccess
                    v = self.vcc.set_system_noaccess(mticket['external_label'])
                    Trace.alarm(e_errors.ERROR, 
                                "Mover error (%s) caused volume %s to go NOACCESS"%(mticket['mover'],
                                                                               mticket['external_label']))
                    # set volume as read only
                    #v = self.vcc.set_system_readonly(w['fc']['external_label'])
                    label = mticket['external_label']

                    #remove entry from suspect volume list
                    self.suspect_volumes.remove(vol)
                    Trace.trace(15,"removed from suspect volume list %s"%(vol,))

                    #self.send_regret(w)
                    # send regret to all clients requested this volume and remove
                    # requests from a queue
                    self.flush_pending_jobs(e_errors.NOACCESS, label)
            else:
                pass

        #self.reply_to_caller({'work': 'no_work'})

    # what is going on
    def getwork(self,ticket):
        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket) # reply now to avoid deadlocks
        # this could tie things up for awhile - fork and let child
        # send the work list (at time of fork) back to client
        if self.fork() != 0:
            return
        try:
            if not self.get_user_sockets(ticket):
                return
            rticket = {}
            rticket["status"] = (e_errors.OK, None)
            rticket["at movers"] = self.work_at_movers.list
            adm_queue, write_queue, read_queue = self.pending_work.get_queue()
            rticket["pending_work"] = adm_queue + write_queue + read_queue
            callback.write_tcp_obj_new(self.data_socket,rticket)
            self.data_socket.close()
            callback.write_tcp_obj_new(self.control_socket,ticket)
            self.control_socket.close()
        except:
            pass #XXX
        os._exit(0)
    # what is going on

    # return sorted queus as they appear in the pendung queue +
    # work at movers
    def getworks_sorted(self,ticket):
        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket) # reply now to avoid deadlocks
        # this could tie things up for awhile - fork and let child
        # send the work list (at time of fork) back to client
        if self.fork() != 0:
            return
        try:
            if not self.get_user_sockets(ticket):
                return
            rticket = {}
            rticket["status"] = (e_errors.OK, None)
            rticket["at movers"] = self.work_at_movers.list
            adm_queue, write_queue, read_queue = self.pending_work.get_queue()
            rticket["pending_works"] = {'admin_queue': adm_queue,
                                        'write_queue': write_queue,
                                        'read_queue':  read_queue
                                        }
            callback.write_tcp_obj_new(self.data_socket,rticket)
            self.data_socket.close()
            callback.write_tcp_obj_new(self.control_socket,ticket)
            self.control_socket.close()
        except:
            pass #XXX
        os._exit(0)

    #Return sorted volume assert list of volumes.
    def get_asserts(self, ticket):
        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket) # reply now to avoid deadlocks
        # this could tie things up for awhile - fork and let child
        # send the work list (at time of fork) back to client
        if self.fork() != 0:
            return
        try:
            if not self.get_user_sockets(ticket):
                return
            rticket = {}
            rticket["status"] = (e_errors.OK, None)
            rticket['pending_asserts'] = self.volume_assert_list
            callback.write_tcp_obj_new(self.data_socket,rticket)
            self.data_socket.close()
            callback.write_tcp_obj_new(self.control_socket,ticket)
            self.control_socket.close()
        except:
            pass #XXX
        os._exit(0)

            
    # get list of suspected volumes 
    def get_suspect_volumes(self,ticket):
        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket) # reply now to avoid deadlocks
        # this could tie things up for awhile - fork and let child
        # send the work list (at time of fork) back to client
        if self.fork() != 0:
            return
        try:
            if not self.get_user_sockets(ticket):
                return
            rticket = {}
            rticket["status"] = (e_errors.OK, None)
            rticket["suspect_volumes"] = self.suspect_volumes.list
            callback.write_tcp_obj_new(self.data_socket,rticket)
            self.data_socket.close()
            callback.write_tcp_obj_new(self.control_socket,ticket)
            self.control_socket.close()
            Trace.trace(15,"get_suspect_volumes ")
        except:
            pass
        os._exit(0)


    # get a port for the data transfer
    # tell the user I'm your library manager and here's your ticket
    def get_user_sockets(self, ticket):
        library_manager_host, library_manager_port, listen_socket =\
                              callback.get_callback()
        listen_socket.listen(4)
        ticket["library_manager_callback_addr"] = (library_manager_host, library_manager_port)
        self.control_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.control_socket.connect(ticket['callback_addr'])
        callback.write_tcp_obj_new(self.control_socket, ticket)
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
        return 1
    
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
            if ticket['state'] in (e_errors.LOCKED, 'ignore', 'unlocked', 'pause', e_errors.NOREAD, e_errors.NOWRITE):
                self.lm_lock = ticket['state']
                self.set_lock(ticket['state'])
                ticket["status"] = (e_errors.OK, None)
                Trace.log(e_errors.INFO,"Library manager state is changed to:%s"%(self.lm_lock,))
            else:
                ticket["status"] = (e_errors.WRONGPARAMETER, None)
        else:
            ticket["status"] = (e_errors.KEYERROR,None)
        self.reply_to_caller(ticket)

    # this is used to include the state information in with the heartbeat
    def return_state(self):
        return self.lm_lock

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
                                     'volume_status'  : mover['volume_status'],
                                     'state'   : mover['state'],
                                     'updated' : mover['updated'],
                                     'time_in_state' : int(mover.get('time_in_state', 0)),
                                     })
        ticket['status'] = (e_errors.OK, None)
        self.reply_to_caller(ticket)

    def remove_active_volume(self, ticket):
        # find the mover to which volume is assigned
        movers = self.volumes_at_movers.get_active_movers()
        for mover in movers:
            if mover['external_label'] == ticket['external_label']:
                break
        else:
            ticket['status'] = (e_errors.DOESNOTEXIST, "Volume not found")
            self.reply_to_caller(ticket)
            return
        Trace.log(e_errors.INFO, "removing active volume %s , mover %s" %
                  (mover['external_label'],mover['mover'])) 
        self.volumes_at_movers.delete({'mover': mover['mover']})
        ticket['status'] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        
            
        
    # get storage groups
    def storage_groups(self, ticket):
        ticket['storage_groups'] = []
        ticket['storage_groups'] = self.sg_limits
        self.reply_to_caller(ticket)

    # reply to the vol_assert client
    def volume_assert(self, ticket):
        self.volume_assert_list.append(ticket)
        ticket['status'] = (e_errors.OK, None)
        self.reply_to_caller(ticket) # reply now to avoid deadlock

    # remove volume from suspect volume list
    def remove_suspect_volume(self, ticket):
        if ticket['volume'] == 'all': # magic word
            self.init_suspect_volumes()
            ticket['status'] = (e_errors.OK, None)
            Trace.log(e_errors.INFO, "suspect volume list cleaned")
        else:
            found = 0
            for vol in self.suspect_volumes.list:
                if ticket['volume'] ==  vol['external_label']:
                    ticket['status'] = (e_errors.OK, None)
                    found = 1
                    break
            else:
                ticket['status'] = (e_errors.NOVOLUME, "No such volume %s"%(ticket['volume']))
            if found:
                self.suspect_volumes.remove(vol)
                Trace.log(e_errors.INFO, "%s removed from suspect volume list"%(vol,))
        self.reply_to_caller(ticket)
            

class LibraryManagerInterface(generic_server.GenericServerInterface):

    def __init__(self):
        # fill in the defaults for possible options
        generic_server.GenericServerInterface.__init__(self)


    library_options = {}

    # define the command line options that are valid
    def valid_dictionaries(self):
        return generic_server.GenericServerInterface.valid_dictionaries(self) \
               + (self.library_options,)

    paramaters = ["library_name"]

    # parse the options like normal but make sure we have a library manager
    def parse_options(self):
        option.Interface.parse_options(self)
        # bomb out if we don't have a library manager
        if len(self.args) < 1 :
            self.missing_parameter(self.parameters())
            self.print_help(),
            sys.exit(1)
        else:
            self.name = self.args[0]



if __name__ == "__main__":

    Trace.init("LIBMAN")
    Trace.trace(6, "libman called with args "+repr(sys.argv) )

    # get an interface
    intf = LibraryManagerInterface()

    # get a library manager
    lm = LibraryManager(intf.name, (intf.config_host, intf.config_port))
    lm.handle_generic_commands(intf)

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
