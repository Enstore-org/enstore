###############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
import os
import time
import traceback
import sys

# enstore imports
import log_client
import configuration_client
import volume_clerk_client
import callback
import dispatching_worker
import generic_server
import generic_cs
import interface
import Trace
import udp_client

import manage_queue
import e_errors
import timer_task

pending_work = manage_queue.LM_Queue()    # list of read or write work tickets

## Trace.trace for additional debugging info uses bits >= 11
 
##############################################################
movers = []    # list of movers belonging to this LM
mover_cnt = 0  # number of movers in the queue
mover_index = 0  # index if current mover in the queue

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
	Trace.trace(7, "}get_movers %s"%movers)
    else:
	Trace.trace(7, "}get_movers: no movers defined in the configuration for this LM")

    
# update mover list
def update_mover_list(self, mover, state):
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
    update_mover_list(self, mover, state)
    mv = find_mover(mover, self.summon_queue)
    if mv:
	mv['tr_error'] = 'ok'
	mv['summon_try_cnt'] = 0
    
	self.summon_queue.remove(mv)
    return mv
	
# remove all pending works
def flush_pending_jobs(self, status, external_label=None, jobtype=None):
    w = pending_work.get_init()
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
	    pending_work.delete_job(w)
	    w = pending_work.get_next()
    
##############################################################

work_at_movers = []

# return a list of busy volumes for a given file family
def busy_vols_in_family (vc, family_name):
    vols = []
    # look in the list of work_at_movers
    for w in work_at_movers:
	if w["vc"]["file_family"] == family_name:
	    vols.append(w["fc"]["external_label"])

    # now check if any volume in this family is still mounted
    work_movers = []
    for mv in movers:
	if mv["file_family"] == family_name:
	    vol_info = vc.inquire_vol(mv["external_label"])
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
    return vols, work_movers


# check if a particular volume with given label is busy
def is_volume_busy(self, external_label):
    rc = 0
    for w in work_at_movers:
        if w["fc"]["external_label"] == external_label:
	    rc = 1
	    break
    # check if volume is in the intemediate 'unmounting state'
    vol_info = self.vcc.inquire_vol(external_label)
    if vol_info['at_mover'][0] == 'unmounting':
	# volume is in unmounting state: can't give it out
	rc = 1
    return rc


# return ticket if given labelled volume in mover queue
def get_work_at_movers(external_label):
    rc = {}
    for w in work_at_movers:
        if w["fc"]["external_label"] == external_label:
	    rc = w
	    break
    return rc

##############################################################
# is there any work for any volume?
def next_work_any_volume(self, csc):

    # look in pending work queue for reading or writing work
    w=pending_work.get_init()
    while w:
        # if we need to read and volume is busy, check later
        if w["work"] == "read_from_hsm":
            if is_volume_busy(self, w["fc"]["external_label"]) :
                w=pending_work.get_next()
                continue
            # otherwise we have found a volume that has read work pending
	    Trace.trace(11,"}next_work_any_volume %s"%w)
            # ok passed criteria
	    # sort requests according file locations
	    w = pending_work.get_init_by_location()
	    # return read work ticket
	    break

        # if we need to write: ask the volume clerk for a volume, but first go
        # find volumes we _dont_ want to hear about -- that is volumes in the
        # apropriate family which are currently at movers.
        elif w["work"] == "write_to_hsm":
            vol_veto_list, work_movers = busy_vols_in_family(self.vcc, 
							    w["vc"]["file_family"])
            # only so many volumes can be written to at one time
            if len(vol_veto_list) >= w["vc"]["file_family_width"]:
                w=pending_work.get_next()
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
		    Trace.trace(11,"next_work_any_volume MV TO SUMMON %s"%mov)
		    # summon this mover
		    summon_mover(self, mov, w)
		    # and return no work to the idle requester mover
		    return {"status" : (e_errors.NOWORK, None)}
		else:
		    Trace.trace(11,"next_work_any_volume:can_write_volume returned %s" % v_info['status'])

		
            # width not exceeded, ask volume clerk for a new volume.
            first_found = 0
            t1 = time.time()
            v = self.vcc.next_write_volume (w["vc"]["library"],
                                      w["wrapper"]["size_bytes"],
                                      w["vc"]["file_family"], 
				      w["vc"]["wrapper"],
				      vol_veto_list,
                                      first_found)
            t2 = time.time()-t1

            # If the volume clerk returned error - return
	    if v["status"][0] != e_errors.OK:
		w["status"] = v["status"]
		return w
		
            # found a volume that has write work pending - return it
	    w["fc"]["external_label"] = v["external_label"]
	    w["fc"]["size"] = w["wrapper"]["size_bytes"]
	    break

        # alas, all I know about is reading and writing
        else:
	    Trace.log(e_errors.ERROR,"next_work_any_volume \
	    assertion error in next_work_any_volume w=%"%w)
            raise "assertion error"
        w=pending_work.get_next()

    # check if this volume is ok to work with
    if w:
	Trace.trace(11,"check volume %s " % w['fc']['external_label'])
	if w["status"][0] == e_errors.OK:
	    vol_info = self.vcc.inquire_vol(w['fc']['external_label'])
	    if (vol_info['system_inhibit'] == e_errors.NOACCESS or
		(vol_info['system_inhibit'] != 'none' and 
		 w['work'] == 'write_to_hsm') or
		((vol_info['system_inhibit'] != 'none' and
		  vol_info['system_inhibit'] != 'readonly' and
		  vol_info['system_inhibit'] != 'full') and 
		 w['work'] == 'read_from_hsm')):
		Trace.trace("work can not be done at this volume %s"%vol_info)
		w['status'] = (e_errors.NOACCESS,None)
		pending_work.delete_job(w)
		send_regret(self, w)
		Trace.log(e_errors.ERROR,"next_work_any_volume: cannot do"
			  "the work for %s status:%s" % 
			  (w['fc']['external_label'], 
			   vol_info['system_inhibit']))
		return {"status" : (e_errors.NOWORK, None)}
	return w
    return {"status" : (e_errors.NOWORK, None)}


# is there any work for this volume??  v is a work ticket with info
def next_work_this_volume(v):
    # look in pending work queue for reading or writing work
    w=pending_work.get_init()
    while w:
        # writing to this volume?
        if (w["work"]                == "write_to_hsm"   and
            (w["vc"]["file_family"]+"."+w["vc"]["wrapper"]) == v['vc']["file_family"] and
            v["vc"]["user_inhibit"]        == "none"           and
            v["vc"]["system_inhibit"]      == "none"           and
            w["wrapper"]["size_bytes"] <= v['vc']["remaining_bytes"]):
            w["fc"]["external_label"] = v['vc']["external_label"]
            w["fc"]["size"] = w["wrapper"]["size_bytes"]
            # ok passed criteria, return write work ticket
	    Trace.trace(13,"}next_work_this_volume " + repr(w))
            return w

        # reading from this volume?
        elif (w["work"]           == "read_from_hsm" and
              w["fc"]["external_label"] == v['vc']["external_label"] and
	      v["vc"]["system_inhibit"] != e_errors.NOACCESS):

	    # if previous read for this file failed and volume
	    # is mounted have_bound_volume request will not
	    # contain current_location field.
	    # Check the presence of current_location field
	    if not v['vc'].has_key('current_location'):
		v['vc']['current_location'] = w['fc']['location_cookie']

            # ok passed criteria
	    # pick up request according to file locations
	    w = pending_work.get_init_by_location()
	    w = pending_work.get_next_for_this_volume(v)

	    # return read work ticket
            return w
        w=pending_work.get_next()
    return {"status" : (e_errors.NOWORK, None)}

##############################################################

# summon mover
def summon_mover(self, mover, ticket):
    if not summon: return
    # update mover info
    mover['last_checked'] = time.time()
    mover['state'] = 'summoned'
    mover['summon_try_cnt'] = mover['summon_try_cnt'] + 1
    # find the mover in summon queue
    mv = find_mover(mover, self.summon_queue)
    Trace.trace(14,"MV=%s"%mv)
    if not mv:
	# add it to the summon queue
	self.summon_queue.append(mover)
    mover['work_ticket'] = {}
    mover['work_ticket'].update(ticket)

    summon_rq = {'work': 'summon',
		 'address': self.server_address }
    
    Trace.trace(14,"summon_rq %s" % summon_rq)
    # send summon request
    mover['tr_error'] = self.udpc.send_no_wait(summon_rq, mover['address'])
    Trace.trace(15,"summon_queue %s" % self.summon_queue)

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
    global mover_index
    Trace.trace(13,"{idle_mover_next ")
    idle_mover_found = 0
    j = mover_index
    for i in range(0, mover_cnt):
	if movers[j]['state'] == 'idle_mover':
	    mv_suspect = None
	    if external_label != None:
		# check if this mover is in the list of suspect volumes
		(vol,mv_suspect) = is_mover_suspect(self, movers[j]['mover'], 
						    external_label) 
	    if not mv_suspect:
		idle_mover_found = 1
		self.summon_queue_index = i
		break
	j = j+1
	if j == mover_cnt:
	    j = 0
    if idle_mover_found:
	mv = movers[j]
	j = j+1
	if j == mover_cnt:
	    j = 0
	mover_index = j
    else:
	mv = None
    # return next idle mover
    return mv

# send a regret
def send_regret(self, ticket):
    # fork off the regret sender
    Trace.trace(12,"FORKING REGRET SENDER")
    ret = self.fork()
    if ret == 0:
	try:
	    Trace.trace(13,"{send_regret "+repr(ticket))
	    callback.send_to_user_callback(ticket)
	    Trace.trace(13,"}send_regret ")
	except:
	    Trace.trace(1,"send_regret "+str(sys.exc_info()[0])+\
			str(sys.exc_info()[1])+repr(ticket))

	os._exit(0)
    else:
        Trace.trace(12, "CHILD ID= %s"%ret)


class LibraryManager(dispatching_worker.DispatchingWorker,
		     generic_server.GenericServer,
		     timer_task.TimerTask):

    summon_queue = []   # list of movers being summoned
    max_summon_attempts = 3
    summon_queue_index = 0
    suspect_volumes = [] # list of suspected volumes
    del_dismount_list = []
    max_suspect_movers = 2 # maximal number of movers in the suspect volume

    def __init__(self, libman, csc=0, verbose=0, \
                 host=interface.default_host(), port=interface.default_port()):
        Trace.trace(10, '{__init__')
	self.verbose = verbose
	self.print_id = libman
	self.name = libman
        # get the config server
        configuration_client.set_csc(self, csc, host, port, verbose)
        #   pretend that we are the test system
        #   remember, in a system, there is only one bfs
        #   get our port and host from the name server
        #   exit if the host is not this machine
        self.keys = self.csc.get(libman)

	# instantiate volume clerk client
	self.vcc = volume_clerk_client.VolumeClerkClient(self.csc)

	try:
	    self.print_id = self.keys['logname']
	except:
	    pass
        dispatching_worker.DispatchingWorker.__init__(self, (self.keys['hostip'], \
                                                      self.keys['port']))
	timer_task.TimerTask.__init__( self, 10 )
        # get a logger
        self.logc = log_client.LoggerClient(self.csc, self.keys["logname"], \
                                            'logserver', 0)
	self.set_udp_client()
        Trace.trace(10, '}__init__')

    def set_udp_client(self):
	self.udpc = udp_client.UDPClient()
	self.rcv_timeout = 10 # set receive timeout

    # overrides timeout handler from DispatchingWorker
    def handle_timeout(self):
	global mover_cnt
	global mover_index
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
	    
        Trace.trace(14,"movers queue after processing TO %s\nmover count %s"%
	               (movers,mover_cnt))
        Trace.trace(14,"summon queue after processing TO %s"%self.summon_queue)

	for mv in self.summon_queue:
	    if mv['state'] == 'summoned':
		if (t - mv['last_checked']) > self.rcv_timeout:
		    # timeout has expired
		    if mv['summon_try_cnt'] < self.max_summon_attempts:
			# retry summon
			Trace.trace(13,"handle_timeout retrying " + repr(mv))
			summon_mover(self, mv, mv["work_ticket"])
		    else:
			# mover is dead. Remove it from all lists
			Trace.log(e_errors.ERROR,"mover %s is dead" % mv)
			movers.remove(mv)
			self.summon_queue.remove(mv)
			# decrement mover counter
			mover_cnt = mover_cnt - 1
			# mover index must be not more than mover counter
			# and cannot be negative
			if (mover_index >= mover_cnt and 
			    mover_index > 0):
			    mover_index = mover_cnt - 1
			if mover_cnt == 0:
			    # no movers left send regrets to clients
			    Trace.log(e_errors.INFO,
				      "handle_timeout: no movers left")
			    mv["work_ticket"]['status'] = (e_errors.NOMOVERS, None)
			    pending_work.delete_job(mv["work_ticket"])
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
			Trace.trace(14,"current mover %snext mover %s"%
				    (mv, next_mover))
			if next_mover:
			    summon_mover(self,next_mover,mv["work_ticket"])
			    break
			else:
			    # no movers left
			    Trace.trace(13,"handle_timeout: no movers left")
			    mv["work_ticket"]['status'] = (e_errors.NOMOVERS, 
							   None)
			    # to catch rare key error
			    format = "NO Movers:%s "
			    pending_work.delete_job(mv["work_ticket"])
			    Trace.log(e_errors.ERROR, "NO Movers:%s "%mv)
			    send_regret(self, mv["work_ticket"])
			    # flush pending jobs
			    flush_pending_jobs(self, (e_errors.NOMOVERS, None))
			    return
			    
	
    def write_to_hsm(self, ticket):
	#call handle_timeout to avoid the situation when due to
	# requests from another encp clients TO did not work even if
	# movers being summoned did not "respond"

	self.handle_timeout()
	if movers:
	    ticket["status"] = (e_errors.OK, None)
	else:
	    ticket["status"] = (e_errors.NOMOVERS, None)
	    
        self.reply_to_caller(ticket) # reply now to avoid deadlocks
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

        pending_work.insert_job(ticket)

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
	# check if this volume is OK
	v = self.vcc.inquire_vol(ticket['fc']['external_label'])
	if v['system_inhibit'] == e_errors.NOACCESS:
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

        pending_work.insert_job(ticket)

	# check if requested volume is busy
	if  not is_volume_busy(self, ticket["fc"]["external_label"]):
	    Trace.trace(14,"VOLUME %s IS AVAILABLE" % ticket["fc"]["external_label"])
	    if not mv_found:
		# find the next idle mover
		mv = idle_mover_next(self, ticket["fc"]["external_label"])
	    else: 
		pass
	    if mv:
		# summon this mover
		Trace.trace(14,"read_from_hsm will summon mover %s"% mv)
		summon_mover(self, mv, ticket)


    # determine if this volume had failed on the maximal
    # allowed number of movers and, if yes, set volume 
    # as having no access and send a regret: noaccess.
    def bad_volume(self, suspect_volume, ticket):
	ret_val = 0
	label = ticket['fc']['external_label']
	if len(suspect_volume['movers']) >= self.max_suspect_movers:
	    ticket['status'] = (e_errors.NOACCESS, None)
	    Trace.trace(13,"Number of movers for suspect volume %" %
			len(suspect_v['movers']))
				
	    # set volume as noaccess
	    v = self.vcc.set_system_noaccess(label)

	    #remove entry from suspect volume list
	    self.suspect_volumes.remove(suspect_volume)
	    # delete the job 
	    pending_work.delete_job(ticket)
	    send_regret(self, ticket)
	    Trace.trace(13,"idle_mover: failed on more than %s for %s"%
			(self.max_suspect_movers,suspect_volume))
	    ret_val = 1
	elif mover_cnt == 1:
	    Trace.trace(13,"There is only one mover in the conf.")
	    pending_work.delete_job(ticket)
	    ticket['status'] = (e_errors.NOMOVERS, 'Read failed') # set it to something more specific
	    send_regret(self, ticket)
	    # check if there are any pending works and remove them
	    flush_pending_jobs(self, (e_errors.NOMOVERS, "Read failed"))
             #remove volume from suspect volume list
	    self.suspect_volumes.remove(suspect_volume)
	    ret_val = 1
	else:
	    try:
		next_mover = idle_mover_next(self, label)
	    except:
		# there was no external label: must be write
		next_mover = idle_mover_next(self, None)
	    Trace.trace(14,"current mover %snext mover %s"%
			(mticket['mover'], next_mover))
	    if next_mover:
		Trace.trace(14, "will summon mover %s"%next_mover)
		summon_mover(self,next_mover,ticket)
		ret_val = 1

	return ret_val

    # update suspect volumer list
    def update_suspect_vol_list(self, external_label, mover):
	# update list of suspected volumes
	Trace.trace(14,"SUSPECT VOLUME LIST BEFORE %s"%self.suspect_volumes)
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
	Trace.trace(14, "SUSPECT VOLUME LIST AFTER %s" % self.suspect_volumes)
	return vol

    # mover is idle - see what we can do
    def idle_mover(self, mticket):
	global mover_cnt
	# remove the mover from the list of movers being summoned
	mv = remove_from_summon_list(self, mticket, mticket['work'])

	# check if there is a work for this mover in work_at_movers list
	# it should not happen in a normal operations but it may when for 
	# instance mover detects that encp is gone and returns idle or
	# mover crashes and then restarts
	
	# find mover in the work_at_movers
	found = 0
	for wt in work_at_movers:
	    if wt['mover'] == mticket['mover']:
		found = 1     # must do this. Construct. for...else will not
                              # do better 
		break
	if found:
	    work_at_movers.remove(wt)
	    format = "Removing work from work at movers queue for idle mover. Work:%s mover:%s"
	    Trace.log(e_errors.INFO, format%(wt,mticket))
	    # check if tape is stuck in in the mounting state
	    vol_info = self.vcc.inquire_vol(wt['fc']['external_label'])
	    if vol_info['at_mover'][0] == 'mounting':
		format = "FORCING  vol:%s to %s. mover:%s"
		Trace.log(e_errors.INFO, format%(wt['fc']['external_label'],
						 'unmounted', wt['mover']))
		# force set volume to unmounted
		v = self.vcc.set_at_mover(wt['fc']['external_label'],
					  'unmounted', wt["mover"], 1)
        w = self.schedule()
        Trace.trace(11,"SCHEDULE RETURNED %s"%w)
        # no work means we're done
        if w["status"][0] == e_errors.NOWORK:
            self.reply_to_caller({"work" : "nowork"})

        # ok, we have some work - try to bind the volume
	elif w["status"][0] == e_errors.OK:
	    # check if the volume for this work had failed on this mover
            Trace.trace(13,"SUSPECT_VOLS %s"%self.suspect_volumes)
	    suspect_v,suspect_mv = is_mover_suspect(self, mticket['mover'], 
						    w['fc']['external_label'])
	    if suspect_mv:
		# skip this mover
		self.reply_to_caller({"work" : "nowork"})
		Trace.trace(13,"idle_mover: skipping %s"%suspect_mv)

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
		    format = "cannot change to 'mounting' vol=%s mover=%s state=%s"
		    Trace.log(e_errors.INFO, format%
				   (w["fc"]["external_label"],
				    v['at_mover'][1], 
				    v['at_mover'][0]))
		    self.reply_to_caller({"work" : "nowork"})
		    return
		else:
		   w['vc']['at_mover'] = v['at_mover'] 
	    else:
		self.reply_to_caller({"work" : "nowork"})
		return
		
            # reply now to avoid deadlocks
            format = "%s work on vol=%s state=%smover=%s requester:%s"
            Trace.log(e_errors.INFO, format%
			   (w["work"],
			   w["fc"]["external_label"],
			   w['vc']['at_mover'],
			   mticket["mover"],
			   w["wrapper"]["uname"]))
	    pending_work.delete_job(w)
            self.reply_to_caller(w) # reply now to avoid deadlocks
            w['mover'] = mticket['mover']
            work_at_movers.append(w)
            Trace.trace(13,"MOVER WORK appended:%s"%w)
	    mv = update_mover_list(self, mticket, 'work_at_mover')
	    mv['external_label'] = w['fc']['external_label']
	    mv["file_family"] = w["vc"]["file_family"]
            return

        # alas
        else:
	    Trace.trace(0,"idle_mover: assertion error w=%s ticket=%"%
			(w, mticket))
            raise "assertion error"

    # we have a volume already bound - any more work??
    def have_bound_volume(self, mticket):
	# update mover list. If mover is in the list - update its state
	if mticket['state'] == 'idle':
	    state = 'idle_mover'  # to make names consistent
	else:
	    state = mticket['state']

	# remove the mover from the list of movers being summoned
	mv = remove_from_summon_list(self, mticket, state)

        # just did some work, delete it from queue
        w = get_work_at_movers (mticket['vc']["external_label"])
        if w:
            Trace.trace(13,"removing %s  from the queue"%w)
	    delayed_dismount = w['encp']['delayed_dismount']
	    work_at_movers.remove(w)
	    mv = find_mover(mticket, movers)
	    if mv and  mv.has_key("work_ticket"):
		del(mv["work_ticket"])

	else: delayed_dismount = 0
	# check if mover can accept another request
	if state != 'idle_mover':
            Trace.trace(14,"have_bound_volume state:%s"%state)
	    self.reply_to_caller({'work': 'nowork'})
	    return
        # otherwise, see if this volume will do for any other work pending
        w = next_work_this_volume(mticket)
        if w["status"][0] == e_errors.OK:
            format = "%s next work on vol=%s mover=%s requester:%s"
            Trace.log(e_errors.INFO, format%(w["work"],
					     w["fc"]["external_label"],
					     mticket["mover"],
					     w["wrapper"]["uname"]))
	    w['times']['lm_dequeued'] = time.time()
            pending_work.delete_job(w)
            Trace.trace(13,"sending %s to mover"%w)
            self.reply_to_caller(w) # reply now to avoid deadlocks
	    delayed_dismount = w['encp']['delayed_dismount']
	    state = 'work_at_mover'
	    update_mover_list(self, mticket, state)
            w['mover'] = mticket['mover']
            work_at_movers.append(w)
            return

        # if the pending work queue is empty, then we're done
        elif  w["status"][0] == e_errors.NOWORK:
	    mv = find_mover(mticket, movers)
	    # check if delayed_dismount is set
	    mvr_found = 0
	    if len(self.del_dismount_list) != 0:
		# find mover in the delayed dismount list
		mvr = find_mover(mticket,self.del_dismount_list)
		if mvr: 
		    mvr_found = 1
	    try:
		if delayed_dismount:
		    if not mvr_found:
			# add mover to delayed dismount list
			self.del_dismount_list.append(mv)
		    else:
			# it was already there, cancel timer func. for
			# the previous ticket
			timer_task.msg_cancel_tr(summon_mover, 
						 self, mvr['mover'])
		    # add timer func. for this tisket
		    timer_task.msg_add(delayed_dismount*60, 
				       summon_mover, self, mv, w)
		    # do not dismount, rather send no work
		    self.reply_to_caller({'work': 'nowork'})
		    Trace.trace(13,"have_bound_volume delayed dismount %s"%w)
		    return
		else:
		    # no delayed dismount: flag dismount
		    mvr_found = 1
	    except:
		# no delayed dismount: flag dismount
		mvr_found = 1 

	    if mvr_found:
		# unbind volume
		timer_task.msg_cancel_tr(summon_mover, self, mv['mover'])
		if mv in self.del_dismount_list:
		    self.del_dismount_list.remove(mv)
		v = self.vcc.set_at_mover(mticket['vc']['external_label'], 
				    'unmounting', 
				    mticket["mover"])
		if v['status'][0] != e_errors.OK:
		    format = "cannot change to 'unmounting' vol=%s mover=%s state=%s"
		    Trace.log(e_errors.INFO, format%
			      (mticket['vc']['external_label'],
			       v['at_mover'][1], 
			       v['at_mover'][0]))
		
		format = "unbind vol %s mover=%s"
		Trace.log(e_errors.INFO, format %
			  (mticket['vc']["external_label"],
			   mticket["mover"]))
		mv['state'] = 'unbind_sent'
		self.reply_to_caller({"work" : "unbind_volume"})

        # alas
        else:
	    Trace.trace(0,"have_bound_volume: assertion error %s %s"%(w,mticket))
            raise "assertion error"


    # if the work is on the awaiting bind list, it is the library manager's
    #  responsibility to retry
    # THE LIBRARY COULD NOT MOUNT THE TAPE IN THE DRIVE AND IF THE MOVER
    # THOUGHT THE VOLUME WAS POISONED, IT WOULD TELL THE VOLUME CLERK.
    def unilateral_unbind(self, ticket):
        # get the work ticket for the volume
        w = get_work_at_movers(ticket["external_label"])

	# remove the mover from the list of movers being summoned
	mv = remove_from_summon_list(self, ticket, 'idle_mover')

        if w:
            Trace.trace(13,"unilateral_unbind: work_at_movers %s"%w)
            work_at_movers.remove(w)
	    mv = find_mover(ticket, movers)
	    if mv and mv.has_key("work_ticket"):
		del(mv["work_ticket"])

	    if ticket['state'] != 'offline':
		# change volume state to unmounting and send unmount request
		v = self.vcc.set_at_mover(ticket['external_label'], 
				    'unmounting', 
				    ticket["mover"])
		if v['status'][0] != e_errors.OK:
		    format = "cannot change to 'unmounting' vol=%s mover=%s state=%s"
		    Trace.log(e_errors.INFO, format %
			      ticket['external_label'],
			      v['at_mover'][1], 
			      v['at_mover'][0])
		else:
		    timer_task.msg_cancel_tr(summon_mover, 
					     self, mv['mover'])
		    format = "unbind vol %s mover=%s"
		    Trace.log(e_errors.ERROR, format %
			       (ticket['external_label'],
			       ticket["mover"]))
		    self.reply_to_caller({"work" : "unbind_volume"})
	    else:
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
	    Trace.trace(13,"set_system_noaccess returned %s"%v)

	    #remove entry from suspect volume list
	    self.suspect_volumes.remove(vol)
	    Trace.trace(13,"removed from suspect volume list %s"%vol)

	    send_regret(self, w)
	    # send regret to all clients requested this volume and remove
	    # requests from a queue
	    flush_pending_jobs(self, e_errors.NOACCESS, label)
	else:
	    pass

    # what is next on our list of work?
    def schedule(self):
	Trace.trace(13,"{schedule ")
        while 1:
            w = next_work_any_volume(self, self.csc)
            if w["status"][0] == e_errors.OK or \
	       w["status"][0] == e_errors.NOWORK:
		Trace.trace(13,"}schedule " + repr(w))
                return w
            # some sort of error, like write work and no volume available
            # so bounce. status is already bad...
            pending_work.delete_job(w)
	    send_regret(self, w)
	    Trace.trace(14,"schedule: Error detected %s" % w)

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
        rticket["at movers"] = work_at_movers
        rticket["pending_work"] = pending_work.get_queue()
        callback.write_tcp_socket(self.data_socket,rticket,
                                  "library_manager getwork, datasocket")
        self.data_socket.close()
        callback.write_tcp_socket(self.control_socket,ticket,
                                  "library_manager getwork, controlsocket")
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
        callback.write_tcp_socket(self.data_socket,rticket,
                                  "library_manager getmoverlist, datasocket")
        self.data_socket.close()
        callback.write_tcp_socket(self.control_socket,ticket,
                                  "library_manager getmoverlist, \
				  controlsocket")
        self.control_socket.close()
        os._exit(0)

    # get Media Changer serving this LM
    def get_mc(self, ticket):
	mticket = self.csc.get(movers[0]["mover"])
	if mticket.has_key('media_changer'):
	    return_ticket = {'mc': mticket['media_changer']}
	else:
	    return_ticket = {}
	
	self.reply_to_caller(return_ticket)
	

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
        callback.write_tcp_socket(self.data_socket,rticket,
                                  "library_manager get_suspect_volumes, datasocket")
        self.data_socket.close()
        callback.write_tcp_socket(self.control_socket,ticket,
                                  "library_manager get_suspect_volumes, \
				  controlsocket")
        self.control_socket.close()
	Trace.trace(13,"}get_suspect_volumes ")
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
		   "delayed_dismounts": self.del_dismount_list
		   }
        callback.write_tcp_socket(self.data_socket,rticket,
                                  "library_manager get_suspect_volumes, datasocket")
        self.data_socket.close()
        callback.write_tcp_socket(self.control_socket,ticket,
                                  "library_manager get_delayed_dismounts, \
				  controlsocket")
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
	w = pending_work.find_job(id)
	if w == None:
	    self.reply_to_caller({"status" : (e_errors.NOWORK,"No such work")})
	else:
	    pending_work.delete_job(w)
	    format = "Request:%s deleted. Complete request:%s"
	    Trace.log(e_errors.INFO, format % (w["unique_id"], w))
	    self.reply_to_caller({"status" : (e_errors.OK, "Work deleted")})
					 
    # change priority
    def change_priority(self, ticket):
	w = pending_work.change_pri(id, pri)
	if w == None:
	    self.reply_to_caller({"status" : (e_errors.NOWORK, "No such work or attempt to set wrong priority")})
	else:
	    format = "Changed priority to:%s Complete request:%s"
	    Trace.log(e_errors.INFO, format % (w["encp"]["curpri"], w))
	    self.reply_to_caller({"status" :(e_errors.OK, "Priority changed")})

class LibraryManagerInterface(generic_server.GenericServerInterface):

    def __init__(self):
        Trace.trace(10,'{lmsi.__init__')
        # fill in the defaults for possible options
	self.summon = 1
        generic_server.GenericServerInterface.__init__(self)
        Trace.trace(10,'}lmsi.__init__')

    # define the command line options that are valid
    def options(self):
        Trace.trace(16, "{}options")
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
    import string
    Trace.init("libman")
    Trace.trace(6, "libman called with args "+repr(sys.argv) )

    # get an interface
    intf = LibraryManagerInterface()
    summon = intf.summon

    # get a library manager
    lm = LibraryManager(intf.name, 0, intf.verbose, intf.config_host, \
	                intf.config_port)

    # get initial list of movers potentially belonging to this
    # library manager from the configuration server

    get_movers(lm.csc, intf.name)

    while 1:
        try:
            #Trace.init(intf.name[0:5]+'.libm')
            Trace.init(lm.keys["logname"])
            Trace.log(e_errors.INFO, "Library Manager %s (re)starting"%intf.name)
            lm.serve_forever()
        except:
	    traceback.print_exc()
	    if SystemExit:
		sys.exit(0)
	    else:
	        lm.serve_forever_error("library manager", lm.logc)
		continue
    Trace.trace(1,"Library Manager finished (impossible)")
