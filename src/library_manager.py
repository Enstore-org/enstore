###############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
import os
import time
import traceback
import sys

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
import timer_task
import lm_list

## Trace.trace for additional debugging info uses bits >= 11
 
##############################################################
movers = []    # list of movers belonging to this LM
mover_cnt = 0  # number of movers in the queue

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


# summon mover
def summon_mover(self, mover, ticket):
    # update mover info
    mover['last_checked'] = time.time()
    mover['state'] = 'summoned'
    mover['summon_try_cnt'] = mover['summon_try_cnt'] + 1
    # find the mover in summon queue
    mv = find_mover(mover, self.summon_queue)
    Trace.trace(15,"MV=%s"%(mv,))
    if not mv:
	# add it to the summon queue
	self.summon_queue.append(mover)
    mover['work_ticket'] = {}
    mover['work_ticket'].update(ticket)

    summon_rq = {'work': 'summon',
		 'address': self.server_address }
    
    Trace.trace(15,"summon_rq %s will be sent to %s" % (summon_rq, mover['mover']))
    # send summon request
    mover['tr_error'] = self.udpc.send_no_wait(summon_rq, mover['address'])
    Trace.trace(15,"summon_queue %s" % (self.summon_queue,))


# summon mover timer function
def summon_mover_d(self, mover, ticket):
    mvr = find_mover(mover,self.del_dismount_list.list)
    Trace.trace(16,"summon_mover_d %s" % (mvr,))
    if mvr:
	if mvr.has_key("del_dism"):
	    del mvr["del_dism"]
    summon_mover(self, mover, ticket)


# add mover to the movers list
def add_mover(name, address):
    global mover_cnt
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
	mover_cnt = mover_cnt + 1
	Trace.log(e_errors.INFO, "Mover added to mover list. Mover:%s. Total movers:%s"%(mover, mover_cnt))
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
	Trace.trace(7, "get_movers %s"%(movers,))
    else:
	Trace.trace(7, "get_movers: no movers defined in the configuration for this LM")

    
# update mover list
def update_mover_list(mover, state):
    # find mover in the list of known movers
    mv = find_mover(mover, movers)
    if not mv:
	# mover was not in the list, add it
	mv = add_mover(mover['mover'], mover['address'])
	
    # change mover state
    if mv['mover'] != mover['mover']:
	# mover name has changed: report and modify it's name
	format = "Mover name changed from %s to %s"
	Trace.log(e_errors.INFO, format%(mv['mover'], mover['mover']))
	mv['mover'] = mover['mover']
    # change the state of the mover
    mv['state'] = state
    mv['last_checked'] = time.time()
    return mv

# remove mover from summon list and update the mover state
def remove_from_summon_list(self, mover, state):
    update_mover_list(mover, state)
    mv = find_mover(mover, self.summon_queue)
    if mv:
	mv['tr_error'] = 'ok'
	mv['summon_try_cnt'] = 0
    
	self.summon_queue.remove(mv)
    return mv
	
# remove all pending works
def flush_pending_jobs(self, status, external_label=None, jobtype=None):
    w = self.pending_work.get_init()
    while w:
	delete_this_job = 0
	if external_label:
	    if (w["fc"].has_key("external_label") and
		w["fc"]["external_label"] != external_label):
		# if external label specified and it does not match
		# work (w) external label skip it over
		continue
	if jobtype:
	    # try to match the jobtype with work
	    if w['work'] == jobtype:
		delete_this_job = 1
	else:
	    # delete no matter what work it is
	   delete_this_job = 1
	if delete_this_job:
	    w['status'] = status
	    send_regret(self, w)
	    self.pending_work.delete_job(w)
	    w = self.pending_work.get_next()
    
##############################################################

# return a list of busy volumes for a given file family
def busy_vols_in_family (self, vc, family_name):
    vols = []
    # look in the list of work_at_movers
    for w in self.work_at_movers.list:
        fn = w["vc"]["file_family"]+"."+w["vc"]["wrapper"]
	if fn == family_name:
	    vols.append(w["fc"]["external_label"])

    # now check if any volume in this family is still mounted
    work_movers = []
    for mv in movers:
	if mv["file_family"] == family_name:
	    vol_info = vc.inquire_vol(mv["external_label"])
            Trace.trace(11,"busy_vols_in_family vol %s"%(vol_info,))
	    if vol_info["status"][0] != e_errors.OK: continue
	    if vol_info['at_mover'][0] != 'unmounted':
		# volume is potentially available if not unmounted
		for vol in vols:
		    if vol == mv["external_label"]:
			# volume is already in the volume veto list
			break
		else:
		    # work for this volume has been completed, hence
		    # it is not in the work_at_movers list
		    # but the volume has is still mounted and must
		    # go into the volume veto list
		    vols.append(mv["external_label"])
            # check if this mover can do the work
	    if (vol_info['at_mover'][0] == 'mounted' and 
		mv['state'] == 'idle_mover'):
		work_movers.append(mv)
    Trace.trace(11,"busy_vols_in_family. vols %s. movers %s"%\
                (repr(vols), repr(work_movers)))
    return vols, work_movers

# check if a particular volume with given label is busy
# for read requests
def is_volume_busy(self, external_label):
    
    rc = 0
    check_vol_state = 0
    at_mov = 0
    del_dism = 0
    # check if volume is in work at movers queue
    for w in self.work_at_movers.list:
        if w["fc"]["external_label"] == external_label:
	    # check if volume is in the 'unmounted' state
	    vol_info = self.vcc.inquire_vol(external_label)
	    if vol_info['at_mover'][0] == 'unmounted':
		# for work at movers volume cannot be in unmounted state
		check_vol_state = 1
		at_mov = 1
                Trace.log(e_errors.ERROR, "volume %s is in work_at_movers. Mover=%s,requestor=%s"%\
                          (external_label,vol_info['at_mover'][1],self.requestor))
            rc = 1
            break
    else: # for
	# check if volume is in delayed dismount list
	# this function is called when LM gets "idle" request and
	# if volume which is in the idle request is also in delayed
	# dismount list, this is wrong and may be a consequence of
	# mover crash and restart
	for w in self.del_dismount_list.list:
	    if (w.has_key("external_label") and 
		w["external_label"] == external_label):
		vol_info = self.vcc.inquire_vol(external_label)
		if vol_info['at_mover'][0] == 'unmounted':
		    # for delayed dismounts volume cannot be in unmounted state
		    check_vol_state = 1
		    del_dism = 1
		    rc = 1
		    Trace.log(e_errors.ERROR, "volume %s is in delayed_dismount"%(external_label,))
		    break
	else: # for
	    # check if volume is in the intemediate 'unmounting state'
	    vol_info = self.vcc.inquire_vol(external_label)
	    if vol_info['at_mover'][0] == 'unmounting':
		# volume is in unmounting state: can't give it out
                Trace.log(e_errors.ERROR, "volume %s is in unmounting state. Mover=%s"%\
                          (external_label,vol_info['at_mover'][1]))
		rc = 1
            # check if volume is in the intemediate 'mounting state'
            elif vol_info['at_mover'][0] == 'mounting':
                # volume is in the mounting state
                # if it is for current mover check it's "real" state
                if vol_info['at_mover'][1] == self.requestor:
                    # restore volume state
                    mcstate =  self.vcc.update_mc_state(external_label)
                    format = "vol:%s state recovered to %s"
                    Trace.log(e_errors.INFO,format%(external_label,
                                                    mcstate["at_mover"][0]))
                    
                else:
                    # if it is not this mover, summon it
                    Trace.trace(11,"volume %s mounting, trying to summon %s"%\
                            (external_label,vol_info['at_mover'][1])) 
                    mv = find_mover_by_name(vol_info['at_mover'][1], movers)
                    if mv:
                        summon_mover(self, mv, {})

                rc = 1
    Trace.trace(7,"is_volume_busy:vol=%s,return code=%s"%(external_label,repr(rc)))
    if check_vol_state:
	# restore volume state
	mcstate =  self.vcc.update_mc_state(external_label)
	format = "vol:%s state recovered to %s"
	Trace.log(e_errors.INFO,format%(external_label,mcstate["at_mover"][0]))
	if at_mov:
	    self.work_at_movers.remove(w)
	    Trace.log(e_errors.INFO, "removed work_at_movers entry %s"%(w,))
	elif del_dism:
	    self.del_dismount_list.remove(w)
	    Trace.log(e_errors.INFO, "removed delayed_dismount entry %s"%(w,))
	    
    return rc


# return ticket if given labelled volume in mover queue
def get_work_at_movers(self, external_label):
    rc = {}
    for w in self.work_at_movers.list:
        if w["fc"]["external_label"] == external_label:
	    rc = w
	    break
    return rc

##############################################################
# is there any work for any volume?
def next_work_any_volume(self):
    Trace.trace(11, "next_work_any_volume")
    # look in pending work queue for reading or writing work
    w=self.pending_work.get_init()
    while w:
        # if we need to read and volume is busy, check later
        if w["work"] == "read_from_hsm":
            if is_volume_busy(self, w["fc"]["external_label"]) :
                w["reject_reason"] = ("VOL_BUSY",w["fc"]["external_label"])
                w=self.pending_work.get_next()
                continue
            # otherwise we have found a volume that has read work pending
	    Trace.trace(11,"next_work_any_volume %s"%(w,))
            # ok passed criteria
	    # sort requests according file locations
	    self.pending_work.get_init_by_location()
	    # Check the presence of current_location field
	    if not w["vc"].has_key('current_location'):
		w["vc"]['current_location'] = w['fc']['location_cookie']
	    w = self.pending_work.get_next_for_this_volume(w['vc'] )
	    # return read work ticket
	    break

        # if we need to write: ask the volume clerk for a volume, but first go
        # find volumes we _dont_ want to hear about -- that is volumes in the
        # apropriate family which are currently at movers.
        elif w["work"] == "write_to_hsm":
            vol_veto_list, work_movers = busy_vols_in_family(self, self.vcc, 
							    w["vc"]["file_family"]+\
                                                             "."+w["vc"]["wrapper"])
            # only so many volumes can be written to at one time
            if len(vol_veto_list) >= w["vc"]["file_family_width"]:
                w["reject_reason"] = ("VOLS_IN_WORK","")
                w=self.pending_work.get_next()
                continue

	    # check if mover that already has mounted volume can do the
	    # work and, if yes, summon it 
	    for mov in work_movers:
		# found mover that can do the work: check if we can
		# write to the volume belonging to this mover
		v_info = self.vcc.can_write_volume (w["vc"]["library"],
					      w["wrapper"]["size_bytes"],
					      w["vc"]["file_family"],
					      w["vc"]["wrapper"],
					      mov["external_label"])
	    
		if v_info['status'][0] == e_errors.OK:
		    Trace.trace(11,"next_work_any_volume MV TO SUMMON %s"%(mov,))
		    # summon this mover
		    summon_mover(self, mov, w)
		    # and return no work to the idle requester mover
		    return {"status" : (e_errors.NOWORK, None)}
		else:
                    w["reject_reason"] = (v_info['status'],"")
		    Trace.trace(11,"next_work_any_volume:can_write_volume returned %s" %
                                (v_info['status'],))

		
            # width not exceeded, ask volume clerk for a new volume.
            first_found = 0
            v = self.vcc.next_write_volume (w["vc"]["library"],
                                      w["wrapper"]["size_bytes"],
                                      w["vc"]["file_family"], 
				      w["vc"]["wrapper"],
				      vol_veto_list,
                                      first_found)
            # If the volume clerk returned error - return
	    if v["status"][0] != e_errors.OK:
		w["status"] = v["status"]
                w["reject_reason"] = (v["status"],"")
		return w
		
            # found a volume that has write work pending - return it
	    w["fc"]["external_label"] = v["external_label"]
	    w["fc"]["size"] = w["wrapper"]["size_bytes"]
	    break

        # alas, all I know about is reading and writing
        else:
	    Trace.log(e_errors.ERROR,
                      "next_work_any_volume assertion error in next_work_any_volume w=%"%(w,))
            raise AssertionError
        w=self.pending_work.get_next()

	
    # check if this volume is ok to work with
    if w:
	Trace.trace(11,"check volume %s " % (w['fc']['external_label'],))
	if w["status"][0] == e_errors.OK:
	    file_family = w["vc"]["file_family"]
	    if w["work"] == "write_to_hsm":
		file_family = file_family+"."+w["vc"]["wrapper"]
	    ret = self.vcc.is_vol_available(w['work'],
					    w['fc']['external_label'],
					    file_family,
					    w["wrapper"]["size_bytes"])
	    if ret['status'][0] != e_errors.OK:
		Trace.trace(11,"work can not be done at this volume %s"%(ret,))
		w['status'] = ret['status']
		self.pending_work.delete_job(w)
		send_regret(self, w)
		Trace.log(e_errors.ERROR,
                          "next_work_any_volume: cannot do the work for %s status:%s" % 
			  (w['fc']['external_label'], w['status'][0]))
		return {"status" : (e_errors.NOWORK, None)}
	return w
    return {"status" : (e_errors.NOWORK, None)}


# is there any work for this volume??  v is a work ticket with info
def next_work_this_volume(self, v):
    # look in pending work queue for reading or writing work
    w=self.pending_work.get_init()
    while w:
	file_family = w["vc"]["file_family"]
	if w["work"] == "write_to_hsm":
	    file_family = file_family+"."+w["vc"]["wrapper"]
	ret = self.vcc.is_vol_available(w['work'],  v["external_label"],
					file_family,
					w["wrapper"]["size_bytes"])
	if ret['status'][0] == e_errors.OK:
	    w['status'] = ret['status']
	    # writing to this volume?
	    if w["work"] == "write_to_hsm":
		w["fc"]["external_label"] = v["external_label"]
		w["fc"]["size"] = w["wrapper"]["size_bytes"]
		# ok passed criteria, return write work ticket
		return w

	    # reading from this volume?
	    elif w["work"] == "read_from_hsm":
		# if previous read for this file failed and volume
		# is mounted have_bound_volume request will not
		# contain current_location field.
		# Check the presence of current_location field
		if not v.has_key('current_location'):
		    v['current_location'] = w['fc']['location_cookie']

		# ok passed criteria
		# pick up request according to file locations
		w = self.pending_work.get_init_by_location()
		w = self.pending_work.get_next_for_this_volume(v)
		if not w: return {"status" : (e_errors.NOWORK, None)}
		# return read work ticket
		return w
        else:
            w['reject_reason'] = (ret['status'], "")
	w=self.pending_work.get_next()
    return {"status" : (e_errors.NOWORK, None)}

# check if volume is in the suspect volume list
def is_volume_suspect(self, external_label):
    for vol in self.suspect_volumes:
	if external_label == vol['external_label']:
	    return vol
    return None

# check if mover is in the suspect volume list
# return tuple (suspect_volume, suspect_mover)
def is_mover_suspect(self, mover, external_label):
    vol = is_volume_suspect(self, external_label)
    if vol:
	for mov in vol['movers']:
	    if mover == mov:
		break
	else: return vol,mov
	return vol,None
    else:
	return None,None

# find the next idle mover
def idle_mover_next(self, external_label):
    global mover_cnt
    idle_mover_found = 0
    j = self.mover_index
    for i in range(0, mover_cnt):
	if movers[j]['state'] == 'idle_mover':
	    mv_suspect = None
	    if external_label != None:
		# check if this mover is in the list of suspect volumes
		(vol,mv_suspect) = is_mover_suspect(self, movers[j]['mover'], 
						    external_label) 
	    if not mv_suspect:
		idle_mover_found = 1
		break
	j = j+1
	if j == mover_cnt:
	    j = 0
    if idle_mover_found:
	mv = movers[j]
	j = j+1
	if j == mover_cnt:
	    j = 0
	self.mover_index = j
    else:
	mv = None
    # return next idle mover
    return mv


class LibraryManager(dispatching_worker.DispatchingWorker,
		     generic_server.GenericServer,
		     timer_task.TimerTask):

    summon_queue = []   # list of movers being summoned
    max_summon_attempts = 3
    suspect_volumes = [] # list of suspected volumes
    max_suspect_movers = 2 # maximal number of movers in the suspect volume
    max_suspect_volumes = 100 # maximal number of suspected volumes for alarm
                              # generation
    min_mv_num = 1 # minimal number of movers allowed
    mover_index = 0  # index if current mover in the queue

    def __init__(self, libman, csc):
        self.name_ext = "LM"
        generic_server.GenericServer.__init__(self, csc, libman)
	self.name = libman
        #   pretend that we are the test system
        #   remember, in a system, there is only one bfs
        #   get our port and host from the name server
        #   exit if the host is not this machine
        self.keys = self.csc.get(libman)
        if self.keys.has_key("movers_threshold"): self.min_mv_num = self.keys["movers_threshold"]
        if self.keys.has_key("volumes_threshold"): self.max_suspect_volumes = self.keys["volumes_threshold"]
	# open DB and restore internal data
	self.open_db()
	# instantiate volume clerk client
	self.vcc = volume_clerk_client.VolumeClerkClient(self.csc)

        dispatching_worker.DispatchingWorker.__init__(self, (self.keys['hostip'], \
                                                      self.keys['port']))
	timer_task.TimerTask.__init__( self, 10 )
	self.set_udp_client()

    # open all dbs that keep LM data
    def open_db(self):
        import string
	# if database directory is specified in configuration - get it
	if self.keys.has_key('database'):
	    self.db_dir = self.keys['database']
	else:
	    # if not - use default 
	    self.db_dir = os.environ['ENSTORE_LM_DB']
        # if directory does not exist, create it
        try:	
            if os.path.exists(self.db_dir) == 0:
                dir = ""
                dir_elements = string.split(self.db_dir,'/')
                for element in dir_elements:
                    dir=dir+'/'+element
                    if os.path.exists(dir) == 0:
                        # try to make the directory - just bomb out if we fail
                        #   since we probably require user intervention to fix
                        dir = string.replace(dir,"//","/")
                        Trace.trace(11,'dir='+repr(dir)+" path="+repr(self.db_dir))
                        os.mkdir(dir)
                        os.chmod(dir,0777)
                        break
        except:
	  exc, val, tb = e_errors.handle_error()
	  sys.exit(1)
       
	# list of read or write work tickets    
	self.pending_work = manage_queue.LM_Queue(self.db_dir)
	# restore pending work
	self.pending_work.restore_queue()

	self.work_at_movers = lm_list.LMList(self.db_dir, 
					     "work_at_movers",
					     "unique_id")
	self.work_at_movers.restore()

	self.del_dismount_list = lm_list.LMList(self.db_dir, 
					     "delayed_dismounts",
					     "mover")
	self.del_dismount_list.restore()

        self.lock_file = open(os.path.join(self.db_dir, 'lm_lock'), 'w')
        self.lm_lock = self.lock_file.read()
        if not self.lm_lock: self.lm_lock = 'unlocked'
        Trace.log(e_errors.INFO,"Library manager started in state:%s"%self.lm_lock)
	
    def set_udp_client(self):
	self.udpc = udp_client.UDPClient()
	self.rcv_timeout = 10 # set receive timeout

    # overrides timeout handler from DispatchingWorker
    def handle_timeout(self):
	global mover_cnt
	t = time.time()
	
	# if mover state did not change from being summoned then
	# it means that it did not "respond" during timeout period.
	# If number of attempts is less than max_summon_attempts,
	# summon this mover again. Else remove this mover from the 
	# list of known movers as well as from the list of movers 
	# being summoned

				
	if mover_cnt <= 0:
	    # no movers
	    # check if there are any pending works and remove them
	    flush_pending_jobs(self, (e_errors.NOMOVERS, None))
	    # also clear summon queue
	    self.summon_queue = []
	    
        Trace.trace(15,"movers queue after processing TO %s\nmover count %s"%
	               (movers,mover_cnt))
        Trace.trace(15,"summon queue after processing TO %s"%(self.summon_queue,))

	for mv in self.summon_queue:
	    if mv['state'] == 'summoned':
		if (t - mv['last_checked']) > self.rcv_timeout:
		    # timeout has expired
		    if mv['summon_try_cnt'] < self.max_summon_attempts:
			# retry summon
			Trace.trace(15,"handle_timeout retrying " + repr(mv))
			summon_mover(self, mv, mv["work_ticket"])
		    else:
			# mover is dead. Remove it from all lists
			Trace.log(e_errors.ERROR,"mover %s is dead" % (mv,))
			movers.remove(mv)
			self.summon_queue.remove(mv)
			# decrement mover counter
			mover_cnt = mover_cnt - 1
                        # send alarm if number of movers is below a threshold
                        if mover_cnt < self.min_mv_num:
                            Trace.alarm(e_errors.WARNING, e_errors.BELOW_THRESHOLD,
                                        {"movers":"Number of movers is below threshold"}) 

			# mover index must be not more than mover counter
			# and cannot be negative
			if (self.mover_index >= mover_cnt and 
			    self.mover_index > 0):
			    self.mover_index = mover_cnt - 1
			if mover_cnt == 0:
			    # no movers left send regrets to clients
			    Trace.log(e_errors.INFO,
				      "handle_timeout: no movers left")
			    mv["work_ticket"]['status'] = (e_errors.NOMOVERS, None)
			    self.pending_work.delete_job(mv["work_ticket"])
			    send_regret(self, mv["work_ticket"])

			    # flush pending jobs
			    flush_pending_jobs(self, 
					       (e_errors.NOMOVERS, None))
			    return
				
			# try another mover
			try:
			    next_mover = idle_mover_next(self, 
							 mv["work_ticket"]["fc"]["external_label"])
			except:
			    # there was no external label: must be write
			    next_mover = idle_mover_next(self, None)
			Trace.trace(15,"current mover %snext mover %s"%\
				    (mv, next_mover))
			if next_mover:
			    summon_mover(self,next_mover,mv["work_ticket"])
			    break
			    
	
    def write_to_hsm(self, ticket):
        if self.lm_lock == 'locked':
            ticket["status"] = (e_errors.NOMOVERS, "Library manager is locked for external access")
            self.reply_to_caller(ticket)
            return
	#call handle_timeout to avoid the situation when due to
	# requests from another encp clients TO did not work even if
	# movers being summoned did not "respond"

	self.handle_timeout()
	if movers:
	    ticket["status"] = (e_errors.OK, None)
	else:
	    ticket["status"] = (e_errors.NOMOVERS, None)
	    
        # check if work is in the at mover list before inserting it
	for wt in self.work_at_movers.list:
            # 2 requests cannot have the same output file names
	    #print 'wt["unique_id"]=%s   ticket["unique_id"]=%s'%(wt["unique_id"],ticket["unique_id"])
            if     wt["wrapper"]['pnfsFilename'] == ticket["wrapper"]["pnfsFilename"] \
	       and wt['retry']                   == ticket['retry']:
                ticket['status'] = (e_errors.INPROGRESS,"Operation in progress")
                break
	    elif wt["unique_id"] == ticket["unique_id"]:
		break
        else:
            status = self.pending_work.insert_job(ticket)
            if status:
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
	if not movers:
	    Trace.trace(11,"write_to_hsm: No movers available")
	    return

        format = "write Q'd %s -> %s : library=%s family=%s requester:%s"
	Trace.log(e_errors.INFO, format%(ticket["wrapper"]["fullname"],
					 ticket["wrapper"]["pnfsFilename"],
					 ticket["vc"]["library"],
					 ticket["vc"]["file_family"],
					 ticket["wrapper"]["uname"]))
	if not ticket.has_key('lm'):
	    ticket['lm'] = {'address':self.server_address }

	# find the next idle mover
	if ticket["fc"].has_key("external_label"):
	   label = ticket["fc"]["external_label"] # must be retry
	else:
	    label = None
	mv = idle_mover_next(self, label)
	if mv:
	    # summon this mover
	    summon_mover(self, mv, ticket)

    def read_from_hsm(self, ticket):
        if self.lm_lock == 'locked':
            ticket["status"] = (e_errors.NOMOVERS, "Library manager is locked for external access")
            self.reply_to_caller(ticket)
            return
	# check if this volume is OK
	v = self.vcc.inquire_vol(ticket['fc']['external_label'])
	if v['system_inhibit'][0] == e_errors.NOACCESS:
	    # tape cannot be accessed, report back to caller and do not
	    # put ticket in the queue
	    ticket["status"] = (e_errors.NOACCESS, None)
	    self.reply_to_caller(ticket)
	    format = "read request discarded for unique_id=%s : volume %s is marked as %s"
	    Trace.log(e_errors.ERROR, format%(ticket['unique_id'],
					      ticket['fc']['external_label'],
					      ticket["status"][0]))
	    Trace.trace(11,"read_from_hsm: volume has no access")
	    return
	# if volume mouted on idle mover summon it
	mv_found = 0
	if v['at_mover'][0] == 'mounted':
	    # set summon flag for this mover
	    for mv in movers:
		if mv['mover'] == v['at_mover'][1]:
		    # !!! stronger condition is search by address but
		    # so far there is no address field in at_mover
		    # it must be added there
		    mv_found = 1
		    break

	# call handle_timeout to avoid the situation when due to
	#requests from another encp clients TO did not work even if
	#movers being summoned did not respond

	self.handle_timeout()
	if movers:
	    ticket["status"] = (e_errors.OK, None)
	else:
	    ticket["status"] = (e_errors.NOMOVERS, "No movers")
	self.reply_to_caller(ticket) # reply now to avoid deadlocks
	if not movers:
	    Trace.trace(11,"read_from_hsm: No movers available")
	    return

        format = "read Q'd %s -> %s : vol=%s bfid=%s requester:%s"
	Trace.log(e_errors.INFO, format%(ticket["wrapper"]["pnfsFilename"],
					 ticket["wrapper"]["fullname"],
					 ticket["fc"]["external_label"],
					 ticket["fc"]["bfid"],
					 ticket["wrapper"]["uname"]))

	if not ticket.has_key('lm'):
	    ticket['lm'] = {'address' : self.server_address}

        # check if work is in the at mover list before inserting it
	for wt in self.work_at_movers.list:
	    if wt["unique_id"] == ticket["unique_id"]:
		break
        else: self.pending_work.insert_job(ticket)

	# check if requested volume is busy
	if  not is_volume_busy(self, ticket["fc"]["external_label"]):
	    Trace.trace(14,"VOLUME %s IS AVAILABLE" % (ticket["fc"]["external_label"],))
	    if not mv_found:
		# find the next idle mover
		mv = idle_mover_next(self, ticket["fc"]["external_label"])
	    else: 
		pass
	    if mv:
		# summon this mover
		Trace.trace(14,"read_from_hsm will summon mover %s"% (mv,))
		summon_mover(self, mv, ticket)
        else:
            ticket["reject_reason"] = ("VOL_BUSY",ticket["fc"]["external_label"])

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
	    self.pending_work.delete_job(ticket)
	    send_regret(self, ticket)
	    Trace.trace(13,"idle_mover: failed on more than %s for %s"%\
			(self.max_suspect_movers,suspect_volume))
	    ret_val = 1
	elif mover_cnt == 1:
	    Trace.trace(13,"There is only one mover in the conf.")
	    self.pending_work.delete_job(ticket)
	    ticket['status'] = (e_errors.NOMOVERS, 'Read failed') # set it to something more specific
	    send_regret(self, ticket)
	    # check if there are any pending works and remove them
	    flush_pending_jobs(self, (e_errors.NOMOVERS, "Read failed"))
             #remove volume from suspect volume list
	    self.suspect_volumes.remove(suspect_volume)
	    ret_val = 1
	else:
	    next_mover = idle_mover_next(self, label)
	    if ticket.has_key('mover'): mover = ticket['mover']
	    else: mover = None
	    Trace.trace(14,"current mover %s next mover %s"%
			(mover, next_mover))
	    if next_mover:
		Trace.trace(14, "will summon mover %s"%(next_mover,))
		summon_mover(self, next_mover, ticket)
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

    # mover is idle - see what we can do
    def idle_mover(self, mticket):
	global mover_cnt
        Trace.trace(11,"IDLE RQ %s"%(mticket,))
	# remove the mover from the list of movers being summoned
	mv = remove_from_summon_list(self, mticket, mticket['work'])
        self.requestor = mticket['mover']
        # mover can be idle in the draining state
        # if state of the mover sending idle RQ is not idle,
        #do not process this request 
	if mticket['state'] != 'idle':
            Trace.trace(14,"idle_mover state:%s"%(mticket['state'],))
            if mticket['state'] == "draining":
                movers.remove(mv)
                Trace.log(e_errors.ERROR,"mover %s is in drainig state and removed" % (mv,))
            self.reply_to_caller({"work" : "nowork"})
            return
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
	    if (vol_info['at_mover'][0] != 'unmounted' and
                vol_info['at_mover'][1] == self.requestor):
                mcstate =  self.vcc.update_mc_state(wt['fc']['external_label'])
		format = "vol:%s state recovered to %s. mover:%s"
		Trace.log(e_errors.INFO, format%(wt['fc']['external_label'],
						 mcstate["at_mover"][0], 
						 wt['mover']))
        w = self.schedule()
        Trace.trace(11,"SCHEDULE RETURNED %s"%(w,))
        # no work means we're done
        if w["status"][0] == e_errors.NOWORK:
            self.reply_to_caller({"work" : "nowork"})

        # ok, we have some work - try to bind the volume
	elif w["status"][0] == e_errors.OK:
	    #self.reply_to_caller({"work" : "nowork"})
	    #return
	    # check if the volume for this work had failed on this mover
            Trace.trace(13,"SUSPECT_VOLS %s"%(self.suspect_volumes,))
	    suspect_v,suspect_mv = is_mover_suspect(self, mticket['mover'], 
						    w['fc']['external_label'])
	    if suspect_mv:
		# skip this mover
		self.reply_to_caller({"work" : "nowork"})
		Trace.trace(13,"idle_mover: skipping %s"%(suspect_mv,))

		# determine if this volume had failed on the maximal
		# allowed number of movers and, if yes, set volume 
		# as having no access and send a regret: noaccess.
		self.bad_volume(suspect_v, w)
		return

	    # check the volume state and try to lock it
	    vol_info = self.vcc.inquire_vol(w['fc']['external_label'])
	    if vol_info['at_mover'][0] == 'unmounted':
		# set volume to mounting
		v = self.vcc.set_at_mover(w['fc']['external_label'], 
					  'mounting', mticket["mover"])
		if v['status'][0] != e_errors.OK:
                    state,mover=v.get('at_mover')
		    format = "cannot change to 'mounting' vol=%s mover=%s state=%s"
		    Trace.log(e_errors.ERROR, format%\
				   (w["fc"]["external_label"],
				    mover, 
				    state))
		    self.reply_to_caller({"work" : "nowork"})
		    return
		else:
		   w['vc']['at_mover'] = v['at_mover']
            elif vol_info['at_mover'][0] == 'mounted':
                # no work for this mover
                self.reply_to_caller({"work" : "nowork"})
                # volume is mounted on a different mover,
                # summon it
                Trace.trace(11,"volume %s mounted, trying to summon %s"%\
                            (w['fc']['external_label'],vol_info['at_mover'][1])) 
                mv = find_mover_by_name(vol_info['at_mover'][1], movers)
                if mv:
                    summon_mover(self, mv, w)
                return

	    else:
                Trace.log(e_errors.INFO,
                          "Cannot satisfy request. Vol %s is %s"%\
                          (w['fc']['external_label'],vol_info['at_mover'][0]))
                if (vol_info['at_mover'][0] != "mounting" and
                    vol_info['at_mover'][0] != "unmounting"):
                    # volume is not in any known state
                    w['status'] = (e_errors.NOACCESS, "volume state:"+vol_info['at_mover'][0])
                    self.pending_work.delete_job(w)
                    send_regret(self, w)
		self.reply_to_caller({"work" : "nowork"})
		return
		
            # reply now to avoid deadlocks
            format = "%s work on vol=%s state=%smover=%s requester:%s"
            Trace.log(e_errors.INFO, format%\
			   (w["work"],
			   w["fc"]["external_label"],
			   w['vc']['at_mover'],
			   mticket["mover"],
			   w["wrapper"]["uname"]))
	    self.pending_work.delete_job(w)
            if w.has_key('reject_reason'): del(w['reject_reason'])
            Trace.log(e_errors.INFO,"IDLE:sending %s to mover"%(w,))
            self.reply_to_caller(w) # reply now to avoid deadlocks
            w['mover'] = mticket['mover']
            self.work_at_movers.append(w)
	    mv = update_mover_list(mticket, 'work_at_mover')
	    mv['external_label'] = w['fc']['external_label']
            file_family = w["vc"]["file_family"]
            if w["work"] == "write_to_hsm":
                file_family = file_family+"."+w["vc"]["wrapper"]
	    mv["file_family"] = file_family
	    #sys.exit(0)
            return

        # alas
        else:
	    Trace.log(1,"idle_mover: assertion error w=%s ticket=%"%
                      (w, mticket))
            raise AssertionError

    # we have a volume already bound - any more work??
    def have_bound_volume(self, mticket):
	Trace.trace(11, "have_bound_volume: request: %s"%(mticket,))
	# update mover list. If mover is in the list - update its state
	if mticket['state'] == 'idle':
	    state = 'idle_mover'  # to make names consistent
	else:
	    state = mticket['state']

	# remove the mover from the list of movers being summoned
	mv = remove_from_summon_list(self, mticket, state)
	# check if mover can accept another request
	if state != 'idle_mover':
            Trace.trace(14,"have_bound_volume state:%s"%(state,))
            if state == "draining":
                movers.remove(mv)
                Trace.log(e_errors.ERROR,"mover %s is in drainig state and removed" % (mv,))
	    self.reply_to_caller({'work': 'nowork'})
	    return

        # just did some work, delete it from queue
        w = get_work_at_movers (self, mticket['vc']["external_label"])
        if w:
            Trace.trace(13,"removing %s  from the queue"%(w,))
	    delayed_dismount = w['encp']['delayed_dismount']
	    self.work_at_movers.remove(w)
	    mv = find_mover(mticket, movers)
	    if mv and  mv.has_key("work_ticket"):
		del(mv["work_ticket"])

	else: delayed_dismount = 0
        # otherwise, see if this volume will do for any other work pending
        w = next_work_this_volume(self, mticket["vc"])
        if w["status"][0] == e_errors.OK:
            format = "%s next work on vol=%s mover=%s requester:%s"
            Trace.log(e_errors.INFO, format%(w["work"],
					     w["fc"]["external_label"],
					     mticket["mover"],
					     w["wrapper"]["uname"]))
	    w['times']['lm_dequeued'] = time.time()
            self.pending_work.delete_job(w)
            if w.has_key('reject_reason'): del(w['reject_reason'])
            Trace.log(e_errors.INFO,"HAVE_BOUND:sending %s to mover"%(w,))
            self.reply_to_caller(w) # reply now to avoid deadlocks
	    delayed_dismount = w['encp']['delayed_dismount']
	    state = 'work_at_mover'
	    update_mover_list(mticket, state)
            w['mover'] = mticket['mover']
            self.work_at_movers.append(w)
            # find mover in the delayed dismount list
            mvr = find_mover(mticket,self.del_dismount_list.list)
            if mvr:
                # there is summon request pending for this mover
                # from the previous have_bound request
                # cancel it
                timer_task.msg_cancel_tr(summon_mover_d, 
                                         self, mvr['mover'])                
                self.del_dismount_list.remove(mvr)
            return

        # if the pending work queue is empty, then we're done
        elif  w["status"][0] == e_errors.NOWORK:
	    mv = find_mover(mticket, movers)
	    # check if delayed_dismount is set
	    mvr_found = 0
	    dismount_vol = 0
	    if len(self.del_dismount_list.list) != 0:
		# find mover in the delayed dismount list
		mvr = find_mover(mticket,self.del_dismount_list.list)
		if mvr:
		    mvr_found = 1
	    try:
		if delayed_dismount:
		    if not mvr_found:
			# add mover to delayed dismount list
			mv["del_dism"] = 1
			self.del_dismount_list.append(mv)
		    else:
			# it was already there, cancel timer func. for
			# the previous ticket
			timer_task.msg_cancel_tr(summon_mover_d, 
						 self, mvr['mover'])
		    # add timer func. for this ticket
		    mv["delay"] = delayed_dismount
		    timer_task.msg_add(delayed_dismount*60, 
				       summon_mover_d, self, mv, w)

		    # do not dismount, rather send no work
		    self.reply_to_caller({'work': 'nowork'})
		    Trace.trace(16,"have_bound_volume delayed dismount %s"%(w,))
		    return
		else:
		    # check if dismount delay had expired
                    if mvr_found:
                        if not mvr.has_key("del_dism"):
                            # no delayed dismount: flag dismount
                            dismount_vol = 1
                        else:
                            # do not dismount, rather send no work
                            self.reply_to_caller({'work': 'nowork'})
                            Trace.trace(16,"have_bound_volume delayed dismount %s"%(w,))
                            return
                    else: dismount_vol = 1
	    except:
                e_errors.handle_error()
		# no delayed dismount: flag dismount
		dismount_vol = 1 

	    if dismount_vol:
		# unbind volume
		timer_task.msg_cancel_tr(summon_mover_d, self, mv['mover'])
		Trace.trace(12, "del_dismount_list %s"%(self.del_dismount_list.list,))
		if mv in self.del_dismount_list.list:
		    Trace.log(e_errors.INFO, "have_bound removed delayed_dismount entry %s"%(mv,))
		    self.del_dismount_list.remove(mv)
		v = self.vcc.set_at_mover(mticket['vc']['external_label'], 
				    'unmounting', 
				    mticket["mover"])
		if v['status'][0] != e_errors.OK:
                    state,mover=v.get('at_mover')
		    format = "cannot change to 'unmounting' vol=%s mover=%s state=%s"
		    Trace.log(e_errors.INFO, format%\
			      (mticket['vc']['external_label'],
			       mover, 
			       state))
		
		format = "unbind vol %s mover=%s"
		Trace.log(e_errors.INFO, format %\
			  (mticket['vc']["external_label"],
			   mticket["mover"]))
		mv['state'] = 'unbind_sent'
		self.reply_to_caller({"work" : "unbind_volume"})

        # alas
        else:
	    Trace.log(1,"have_bound_volume: assertion error %s %s"%(w,mticket))
            raise AssertionError


    # if the work is on the awaiting bind list, it is the library manager's
    #  responsibility to retry
    # THE LIBRARY COULD NOT MOUNT THE TAPE IN THE DRIVE AND IF THE MOVER
    # THOUGHT THE VOLUME WAS POISONED, IT WOULD TELL THE VOLUME CLERK.
    def unilateral_unbind(self, ticket):
        Trace.trace(11,"UNILATERAL UNBIND RQ %s"%(ticket,))
        # get the work ticket for the volume
        w = get_work_at_movers(self, ticket["external_label"])

	# remove the mover from the list of movers being summoned
	mv = remove_from_summon_list(self, ticket, 'idle_mover')

        if w:
            Trace.trace(13,"unilateral_unbind: work_at_movers %s"%(w,))
            self.work_at_movers.remove(w)
	    mv = find_mover(ticket, movers)
	    if mv and mv.has_key("work_ticket"):
		del(mv["work_ticket"])

	    if ticket['state'] != 'offline':
		# change volume state to unmounting and send unmount request
		v = self.vcc.set_at_mover(ticket['external_label'], 
				    'unmounting', 
				    ticket["mover"])
		if v['status'][0] != e_errors.OK:
                    state,mover=v.get('at_mover')
		    format = "cannot change to 'unmounting' vol=%s mover=%s state=%s"
		    Trace.log(e_errors.INFO, format %\
			      (ticket['external_label'],
			      mover, 
			      state))
                    self.reply_to_caller({"work" : "nowork"})
		else:
		    timer_task.msg_cancel_tr(summon_mover, 
					     self, mv['mover'])
		    format = "unbind vol %s mover=%s"
		    Trace.log(e_errors.ERROR, format %\
			       (ticket['external_label'],
			       ticket["mover"]))
		    self.reply_to_caller({"work" : "unbind_volume"})
	    else:
                if ticket['state'] == "draining":
                    movers.remove(mv)
                    Trace.log(e_errors.ERROR,"mover %s is in drainig state and removed" % (mv,))
                Trace.log(e_errors.INFO,"unilateral_unbind: sending nowork")
		self.reply_to_caller({"work" : "nowork"})

	# determine if all the movers are in suspect volume list and if
	# yes set volume as having no access and send a regret: noaccess.

	# update list of suspected volumes
	vol = self.update_suspect_vol_list(ticket['external_label'], 
				ticket['mover'])
	if len(vol['movers']) >= self.max_suspect_movers:
	    w['status'] = (e_errors.NOACCESS, None)

	    # set volume as noaccess
	    v = self.vcc.set_system_noaccess(w['fc']['external_label'])
	    label = w['fc']['external_label']
	    Trace.trace(13,"set_system_noaccess returned %s"%(v,))

	    #remove entry from suspect volume list
	    self.suspect_volumes.remove(vol)
	    Trace.trace(13,"removed from suspect volume list %s"%(vol,))

	    send_regret(self, w)
	    # send regret to all clients requested this volume and remove
	    # requests from a queue
	    flush_pending_jobs(self, e_errors.NOACCESS, label)
	else:
	    pass

    # go through entire pending work queue and summon movers
    def force_summon(self):
        # save mover_index
        mv_index = self.mover_index
	vols = []
	# create list of volumes summon requst for which has been already sent
	for mv in movers:
	    if mv.has_key('work_ticket') and mv['state'] == 'summoned':
		if mv['work_ticket'].has_key("external_label"):
		    if not (mv['work_ticket']["external_label"] in vols):
			vols.append(mv['work_ticket']["external_label"])

	work = self.pending_work.get_init_by_location()
	pend_req = len(self.pending_work.queue)
	for i in range(0, pend_req):
	    if work:
		# see if mover for this work has been already summoned
		for rq in self.summon_queue:
                    Trace.trace(13,"force_summon.summon_q: %s........work:%s"\
                                %(rq, work))
                    if (rq.has_key("work_ticket") and
                        rq["work_ticket"].has_key("unique_id")):
                        if rq["work_ticket"]["unique_id"] == work["unique_id"]:
                            break
		else:
		    # find the next idle mover
		    if work["fc"].has_key("external_label"):
			label = work["fc"]["external_label"] 
		    else:
			label = None
		    # see if summon for the volume has been already done
		    if label:
			if label in vols:
			    work = self.pending_work.get_next()
			    continue
		    mv = idle_mover_next(self, label)
		    if mv:
			# summon this mover
			summon_mover(self, mv, work)

		work = self.pending_work.get_next()
	    else:
		break
        if mv_index != self.mover_index: self.mover_index = mv_index 
	
    # what is next on our list of work?
    def schedule(self):
        while 1:
            w = next_work_any_volume(self)
            if w["status"][0] == e_errors.OK or \
	       w["status"][0] == e_errors.NOWORK:
                self.force_summon()
                return w
            # some sort of error, like write work and no volume available
            # so bounce. status is already bad...
            self.pending_work.delete_job(w)
	    send_regret(self, w)
            self.force_summon()
	    Trace.trace(14,"schedule: Error detected %s" % (w,))

    # load mover list form the configuration server
    def load_mover_list(self, ticket):
	get_movers(self.csc, self.name)
	ticket['movers'] = movers
	ticket["status"] = (e_errors.OK, None)
	self.reply_to_caller(ticket)
	
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

    # get list of assigned movers 
    def getmoverlist(self,ticket):
        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket) # reply now to avoid deadlocks
        # this could tie things up for awhile - fork and let child
        # send the work list (at time of fork) back to client
        if self.fork() != 0:
            return
        self.get_user_sockets(ticket)
        rticket = {}
        rticket["status"] = (e_errors.OK, None)
        rticket["moverlist"] = []
	for mover in movers:
	    m = {'mover'          : mover['mover'],
		 'address'        : mover['address'],
		 'state'          : mover['state'],
		 'last_checked'   : mover['last_checked'],
		 'summon_try_cnt' : mover['summon_try_cnt'],
		 'tr_error'       : mover['tr_error']
		 }
	    rticket["moverlist"].append(m)
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
        rticket["suspect_volumes"] = self.suspect_volumes
        callback.write_tcp_obj(self.data_socket,rticket)
        self.data_socket.close()
        callback.write_tcp_obj(self.control_socket,ticket)
        self.control_socket.close()
	Trace.trace(13,"get_suspect_volumes ")
        os._exit(0)

    # get list of delayed dismounts 
    def get_delayed_dismounts(self,ticket):
        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket) # reply now to avoid deadlocks
        # this could tie things up for awhile - fork and let child
        # send the work list (at time of fork) back to client
        if self.fork() != 0:
            return
        self.get_user_sockets(ticket)
	rticket = {"status":            (e_errors.OK, None),
		   "delayed_dismounts": self.del_dismount_list.list
		   }
        callback.write_tcp_obj(self.data_socket,rticket)
        self.data_socket.close()
        callback.write_tcp_obj(self.control_socket,ticket)
        self.control_socket.close()
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
	w = self.pending_work.find_job(id)
	if w == None:
	    self.reply_to_caller({"status" : (e_errors.NOWORK,"No such work")})
	else:
	    self.pending_work.delete_job(w)
	    format = "Request:%s deleted. Complete request:%s"
	    Trace.log(e_errors.INFO, format % (w["unique_id"], w))
	    self.reply_to_caller({"status" : (e_errors.OK, "Work deleted")})
					 
    # change priority
    def change_priority(self, ticket):
	w = self.pending_work.change_pri(ticket["unique_id"], ticket["priority"])
	if w == None:
	    self.reply_to_caller({"status" : (e_errors.NOWORK, "No such work or attempt to set wrong priority")})
	else:
	    format = "Changed priority to:%s Complete request:%s"
	    Trace.log(e_errors.INFO, format % (w["encp"]["curpri"], w))
	    self.reply_to_caller({"status" :(e_errors.OK, "Priority changed")})

    def kickoff_movers(self):
	# if there are any works in work_at_mover or delayed_dismount lists
	# summon corresponding movers
	for w in self.work_at_movers.list:
	    mv = find_mover_by_name(w['mover'], movers)
	    if mv:
		summon_mover(self, mv, w)
	for w in self.del_dismount_list.list:
            if w.has_key("del_dism"):
                del w["del_dism"]
	    mv = find_mover_by_name(w['mover'], movers)
	    if mv:
                if mv.has_key("del_dism"):
                    del mv["del_dism"]
		summon_mover(self, mv, w) 

	# try to kick off the mover
	ticket = self.pending_work.get_init()
	# find the next idle mover
	if ticket: 
	    if ticket["fc"].has_key("external_label"):
		label = ticket["fc"]["external_label"] # must be retry
	    else:
		label = None
	    mv = idle_mover_next(self, label)
	    if mv:
		# summon this mover
		summon_mover(self, mv, ticket)
	
    def summon(self, ticket):
        if ticket["mover"] != None:
            mv = find_mover_by_name(ticket["mover"], movers)
            if mv:
                # summon this mover
                summon_mover(self, mv, {})
                reply = {"status" :(e_errors.OK, "will summon")}
            else: reply = {"status" :(e_errors.UNKNOWN, "mover is not found")}
        else:
            reply = {"status" :(e_errors.OK, "will summon")}
            self.kickoff_movers()
        self.reply_to_caller(reply)

    def poll(self, ticket):
	mv = idle_mover_next(self, None)
	self.reply_to_caller({"status" :(e_errors.OK, "will poll movers")})
	while mv:
	   summon_mover(self, mv, {})
	   mv = idle_mover_next(self, None)

    # change state of the library manager
    def change_lm_state(self, ticket):
        if ticket.has_key('state'):
            if (ticket['state'] == 'locked' or
                ticket['state'] == 'unlocked'):
                self.lm_lock = ticket['state']                      
                self.lock_file.write(ticket['state'])
                ticket["status"] = (e_errors.OK, None)
                Trace.log(e_errors.INFO,"Library manager state is changed to:%s"%self.lm_lock)
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

    # get initial list of movers potentially belonging to this
    # library manager from the configuration server

    get_movers(lm.csc, intf.name)

    # check if there is something pending 
    #if (len(lm.pending_work.queue) != 0 or 
    #  len(lm.work_at_movers.list) != 0 or
    #  len(lm.del_dismount_list.list) != 0):
    # try to kick off movers
    lm.kickoff_movers()
    while 1:
        try:
            #Trace.init(intf.name[0:5]+'.libm')
            Trace.init(lm.log_name)
            Trace.log(e_errors.INFO, "Library Manager %s (re)starting"%(intf.name,))
            lm.serve_forever()
        except:
	    traceback.print_exc()
	    if SystemExit:
		sys.exit(0)
	    else:
	        lm.serve_forever_error("library manager", lm.logc)
		continue
    Trace.trace(1,"Library Manager finished (impossible)")
