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

pending_work = manage_queue.Queue()       # list of read or write work tickets

##############################################################
movers = []    # list of movers belonging to this LM
mover_cnt = 0  # number of movers in the queue
mover_index = 0  # index if current mover in the queue

# add mover to the movers list
def add_mover(name, address):
    global mover_cnt
    Trace.trace(4, "{add_mover " + repr(name) + " " + repr(address))
    found = 0
    for mv in movers:
	# check if mover is already in the list
	if (address == mv['address']):
	    found = 1
	    break
    if not found:
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
	Trace.trace(4, "}add_mover " + repr(mover) + "mover count=" + \
		    repr(mover_cnt))
    

# get list of assigned movers from the configuration list
def get_movers(config_client, lm_name, verbose=0):
    Trace.trace(3, "{get_movers for " + repr(lm_name))
    generic_cs.enprint("get_movers", generic_cs.SERVER, verbose,\
	               generic_cs.ENNONE, generic_cs.ENNONE, lm_name)
    movers_list = config_client.get_movers(lm_name)
    generic_cs.enprint(movers_list, \
	               generic_cs.SERVER|generic_cs.PRETTY_PRINT, verbose,\
	               generic_cs.ENNONE, generic_cs.ENNONE, lm_name)
    if movers_list:
	for item in movers_list:
	    if (item.has_key('mover') and
		item.has_key('address')):
		add_mover(item['mover'], item['address'])
    Trace.trace(3, "}get_movers " + repr(movers))
    if movers:
	generic_cs.enprint(movers, generic_cs.SERVER, verbose,\
	                   generic_cs.ENNONE, generic_cs.ENNONE, lm_name)
    else:
	generic_cs.enprint("no movers defined in the configuration for this LM",\
	                  generic_cs.SERVER, verbose, generic_cs.ENNONE,\
	                  lm_name)

# find mover in the list
def find_mover(mover, mover_list, verbose=0):
    Trace.trace(4,"{find_mover " + repr(mover) + "in " + repr(mover_list))
    found = 0
    try:
	for mv in mover_list:
	    if (mover['address'] == mv['address']):
		found = 1
		break
	if not found:
	    mv = {}
	Trace.trace(4,"}find_mover "+repr(mv))
	return mv
    except KeyError:
	Trace.trace(0,"}find_mover "+str(sys.exc_info()[0])+\
                     str(sys.exc_info()[1])+repr(mover))

	generic_cs.enprint("keyerror", generic_cs.SERVER, verbose)
	generic_cs.enprint(mover, \
	                generic_cs.SERVER|generic_cs.PRETTY_PRINT, verbose)
	generic_cs.enprint("find_mover "+repr(mover), generic_cs.SERVER, \
	                   verbose)
    
# update mover list
def update_mover_list(self, mover, state):
    Trace.trace(3,"{update_mover_list " + repr(mover))
    mv = find_mover(mover, movers, self.verbose)
    if mv == None:
	# there is no such mover: return
	return mv
    if not mv:
	# list was empty: add mover
	add_mover(mover['mover'], mover['address'])
	# to get the mover call find_mover
	mv = find_mover(mover, movers, self.verbose)
	
    # change mover state
    generic_cs.enprint("changing mover state", generic_cs.SERVER, self.verbose)
    if mv['mover'] != mover['mover']:
	# mover name has changed: report and modify it's name
	format = "Mover name changed from %s to %s"
        self.logc.send(e_errors.INFO, 2, format,
		       mv['mover'], mover['mover'])
	mv['mover'] = mover['mover']
    # change the state of the mover
    mv['state'] = state
    mv['last_checked'] = time.time()
    Trace.trace(3,"}update_mover_list " + repr(mv))
    generic_cs.enprint("MOVER_LIST"+repr(movers), generic_cs.SERVER, \
	               self.verbose)
    """
    for i in movers:
        generic_cs.enprint(i, generic_cs.SERVER, self.verbose)
    """
    return mv

# remove mover from summon list and update the mover state
def remove_from_summon_list(self, ticket, state):
    update_mover_list(self, ticket, state)
    mv = find_mover(ticket, self.summon_queue, self.verbose)
    if mv:
	mv['tr_error'] = 'ok'
	mv['summon_try_cnt'] = 0
    
	self.summon_queue.remove(mv)
    return mv
	
# remove all pending works
def flush_pending_jobs(self, status, *jobtype):
    Trace.trace(3,"{flush_pending_jobs: status "+repr(status))
    generic_cs.enprint("flush_pending_jobs: status"+repr(status),
		       generic_cs.DEBUG, self.verbose) 
    w = pending_work.get_init()
    while w:
	delete_this_job = 0
	if jobtype:
	    # try to match the jobtype with work
	    if w['work'] == jobtype:
		delete_this_job = 1
	else:
	    # delete no matter what work it is
	   delete_this_job = 1
	if delete_this_job:
	    w['status'] = status
	    send_regret(self, w, self.verbose)
	    pending_work.delete_job(w)
	    w1 = pending_work.get_next()
	    w = w1
    Trace.trace(3,"}flush_pending_jobs ")
    
##############################################################

work_at_movers = []

# return a list of busy volumes for a given file family
def busy_vols_in_family (vc, family_name, verbose):
    Trace.trace(4,"{busy_vols_in_family " + repr(family_name))
    vols = []
    # look in the list of work_at_movers
    for w in work_at_movers:
     try:
	 if w["vc"]["file_family"] == family_name:
            vols.append(w["fc"]["external_label"])
     except:
	 Trace.trace(0,"}busy_vols_in_family "+str(sys.exc_info()[0])+\
                     str(sys.exc_info()[1]))
	 generic_cs.enprint(repr(w)+"\n"+repr(work_at_movers), \
			    generic_cs.DEBUG|generic_cs.PRETTY_PRINT, verbose)
	 os._exit(222)

    # now check if any volume in this family is still mounted
    work_movers = []
    for mv in movers:
     try:
	 if mv["file_family"] == family_name:
	     vol_info = vc.inquire_vol(mv["external_label"])
	     if vol_info['at_mover'][0] != 'unmounted':
		 # volume is potentially available if not unmounted
		 vol_found = 0
		 for vol in vols:
		     if vol == mv["external_label"]:
			 # volume is already in the volume veto list
			 vol_found = 1
			 break
		 if not vol_found:
		     # add volume to the volume veto list
		     vols.append(mv["external_label"])
	     # check if this mover can do the work
	     if vol_info['at_mover'][0] == 'mounted':
		 if mv['state'] == 'idle_mover':
		     work_movers.append(mv)
     except:
	 Trace.trace(0,"}busy_vols_in_family "+str(sys.exc_info()[0])+\
                     str(sys.exc_info()[1]))
	 generic_cs.enprint(repr(mv)+"\n"+repr(work_at_movers), \
			    generic_cs.DEBUG|generic_cs.PRETTY_PRINT, verbose)
	 os._exit(222)
     Trace.trace(4,"}busy_vols_in_family ")
    return vols, work_movers


# check if a particular volume with given label is busy
def is_volume_busy(self, external_label):
    Trace.trace(4,"{is_volume_busy " + repr(external_label))
    rc = 0
    for w in work_at_movers:
        if w["fc"]["external_label"] == external_label:
	    rc = 1
	    break
    # check if volume is in the intemediate 'unmounting state'
    vc = volume_clerk_client.VolumeClerkClient(self.csc)
    vol_info = vc.inquire_vol(external_label)
    if vol_info['at_mover'][0] == 'unmounting':
	# volume is in unmounting state: can't give it out
	rc = 1

    Trace.trace(4,"}is_volume_busy " + repr(rc))
    return rc


# return ticket if given labelled volume in mover queue
def get_work_at_movers(external_label):
    rc = {}
    Trace.trace(4,"{get_work_at_movers " + repr(external_label))
    for w in work_at_movers:
        if w["fc"]["external_label"] == external_label:
	    rc = w
	    break
    Trace.trace(4,"}get_work_at_movers " + repr(rc))
    return rc

##############################################################
# is there any work for any volume?
def next_work_any_volume(self, csc, verbose):
    Trace.trace(3,"{next_work_any_volume "+repr(csc))

    # instantiate volume clerk client
    vc = volume_clerk_client.VolumeClerkClient(csc)
    # look in pending work queue for reading or writing work
    w=pending_work.get_init()
    while w:
        # if we need to read and volume is busy, check later
        if w["work"] == "read_from_hsm":
            if is_volume_busy(self, w["fc"]["external_label"]) :
                w=pending_work.get_next()
                continue
            # otherwise we have found a volume that has read work pending
	    Trace.trace(3,"}next_work_any_volume "+ repr(w))
            # ok passed criteria
	    # sort requests according file locations
	    w = pending_work.get_init_by_location()
	    # return read work ticket
	    break

        # if we need to write: ask the volume clerk for a volume, but first go
        # find volumes we _dont_ want to hear about -- that is volumes in the
        # apropriate family which are currently at movers.
        elif w["work"] == "write_to_hsm":
            vol_veto_list, work_movers = busy_vols_in_family(vc, 
							    w["vc"]["file_family"],
							    verbose)
	    for mov in work_movers:
		# found mover that can do the work: check if we can
		# write to the volume belonging to this mover
		v_info = vc.can_write_volume (w["vc"]["library"],
					      w["wrapper"]["size_bytes"],
					      w["vc"]["file_family"],
					      w["vc"]["wrapper"],
					      mov["external_label"])
		if v_info['status'][0] == e_errors.OK:
		    Trace.trace(3,"{next_work_any_volume MV TO SUMMON"+\
				 repr(mov))
		    # summon this mover
		    summon_mover(self, mov, w)
		    # and return no work to the idle requester mover
		    return {"status" : (e_errors.NOWORK, None)}
		else:
		    Trace.trace(3,"{next_work_any_volume:can_write_volume returned"+repr(v_info['status']))

            # only so many volumes can be written to at one time
            if len(vol_veto_list) >= w["vc"]["file_family_width"]:
                w=pending_work.get_next()
                continue
		
            # width not exceeded, ask volume clerk for a new volume.
            first_found = 0
            t1 = time.time()
            v = vc.next_write_volume (w["vc"]["library"],
                                      w["wrapper"]["size_bytes"],
                                      w["vc"]["file_family"], 
				      w["vc"]["wrapper"],
				      vol_veto_list,
                                      first_found)
            t2 = time.time()-t1
            #generic_cs.enprint("  next_write_volume dt= "+repr(t2))

            # If the volume clerk has no volumes and our veto list was empty,
            # then we have run out of space for this file family == error
	    if v["status"][0] == e_errors.NOVOLUME:
		generic_cs.enprint("no volume. Status:"+repr(v["status"]),
				   generic_cs.DEBUG, verbose)
		w["status"] = v["status"]
		return w
		
            if (len(vol_veto_list) < w["vc"]["file_family_width"] and 
		v["status"][0] != e_errors.OK):
                w["status"] = v["status"]
		Trace.trace(0,"next_work_any_volume "+ repr(w))
                return w
            # found a volume that has write work pending - return it
	    try:
		w["fc"]["external_label"] = v["external_label"]
		w["fc"]["size"] = w["wrapper"]["size_bytes"]
	    except KeyError:
		Trace.trace(3,"}next_work_any_volume:keyerror "+\
			    str(sys.exc_info()[0])+\
			    str(sys.exc_info()[1])+ repr(v))
		raise "key error"+str(sys.exc_info()[0])+\
		      str(sys.exc_info()[1])+ repr(v)
	    Trace.trace(3,"}next_work_any_volume "+ repr(w))
            #return w
	    break

        # alas, all I know about is reading and writing
        else:
	    Trace.trace(0,"}next_work_any_volume \
	    assertion error in next_work_any_volume w="+ repr(w))
            generic_cs.enprint("assertion error in next_work_any_volume w="+\
			       repr(w), \
	                       generic_cs.DEBUG, verbose)
            generic_cs.enprint(w, generic_cs.DEBUG|generic_cs.PRETTY_PRINT, \
	                       verbose)
            raise "assertion error"
        w=pending_work.get_next()

    # check if this volume is ok to work with
    if w:
	self.enprint("check volume "+repr(w['fc']['external_label']),\
		     generic_cs.DEBUG, self.verbose)
	if w["status"][0] == e_errors.OK:
	    vol_info = vc.inquire_vol(w['fc']['external_label'])
	    if (vol_info['system_inhibit'] == e_errors.NOACCESS or
		(vol_info['system_inhibit'] != 'none' and 
		 w['work'] == 'write_to_hsm') or
		((vol_info['system_inhibit'] != 'none' and
		  vol_info['system_inhibit'] != 'readonly' and
		  vol_info['system_inhibit'] != 'full') and 
		 w['work'] == 'read_from_hsm')):
		self.enprint("work can not be done at this volume"+repr(vol_info),\
			     generic_cs.DEBUG, self.verbose)
		w['status'] = (e_errors.NOACCESS,None)
		pending_work.delete_job(w)
		send_regret(self, w, self.verbose)
		Trace.trace(0,"next_work_any_volume: cannot do the work for "+\
			    repr(w['fc']['external_label'])+ " status:" + \
			    repr(vol_info['system_inhibit']))
		return {"status" : (e_errors.NOWORK, None)}
	return w
    # if the pending work queue is empty, then we're done
    Trace.trace(3,"}next_work_any_volume: pending work queue is empty ")
    return {"status" : (e_errors.NOWORK, None)}


# is there any work for this volume??  v is a work ticket with info
def next_work_this_volume(v,verbose):
    Trace.trace(3,"{next_work_this_volume "+repr(v))
    generic_cs.enprint("next_work_this_volume vol="+repr(v), \
	                       generic_cs.DEBUG, verbose)
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
	    Trace.trace(3,"}next_work_this_volume " + repr(w))
            return w

        # reading from this volume?
        elif (w["work"]           == "read_from_hsm" and
              w["fc"]["external_label"] == v['vc']["external_label"] and
	      v["vc"]["system_inhibit"] != e_errors.NOACCESS):
	    try:
		# if previous read for this file failed and volume
		# is mounted have_bound_volume request will not
		# contain current_location field.
		# Check the presence of current_location field
		cur_loc = v['vc']['current_location']
	    except KeyError:
		v['vc']['current_location'] = w['fc']['location_cookie']

            # ok passed criteria
	    # pick up request according to file locations
	    w = pending_work.get_init_by_location()
	    w = pending_work.get_next_for_this_volume(v)

	    #print "LOCATION COOKIE:", w["fc"]["location_cookie"]
	    # return read work ticket
	    Trace.trace(3,"}next_work_this_volume " + repr(w))

            return w
        w=pending_work.get_next()
    # if the pending work queue for this volume is empty, then we're done
    Trace.trace(3,"}next_work_this_volume: pending work queue for this volume\
    is empty")
    return {"status" : (e_errors.NOWORK, None)}

##############################################################

# summon mover
def summon_mover(self, mover, ticket):
    if not summon: return
    self.enprint("SUMMON "+repr(mover), generic_cs.DEBUG, self.verbose)
    self.enprint("SUMMON TICKET"+repr(ticket), generic_cs.DEBUG, self.verbose)
    Trace.trace(3,"{summon_mover " + repr(mover))
    # update mover info
    mover['last_checked'] = time.time()
    mover['state'] = 'summoned'
    mover['summon_try_cnt'] = mover['summon_try_cnt'] + 1
    # find the mover in summon queue
    mv = find_mover(mover, self.summon_queue, self.verbose)
    self.enprint("MV= "+repr(mv), generic_cs.DEBUG, self.verbose)
    if not mv:
	# add it to the summon queue
	self.summon_queue.append(mover)
    mover['work_ticket'] = {}
    mover['work_ticket'].update(ticket)

	    
    summon_rq = {'work': 'summon',
		 'address': self.server_address }
    
    self.enprint("summon_rq "+repr(summon_rq), generic_cs.DEBUG, self.verbose)
    # send summon request
    mover['tr_error'] = self.udpc.send_no_wait(summon_rq, mover['address'])
    self.enprint("summon_queue", generic_cs.DEBUG, self.verbose)
    self.enprint(self.summon_queue, generic_cs.DEBUG|generic_cs.PRETTY_PRINT, \
	         self.verbose)
    Trace.trace(3,"}summon_mover " + repr(mover))


# find the next idle mover
def idle_mover_next(self,external_label):
    global mover_cnt
    global mover_index
    Trace.trace(3,"{idle_mover_next ")
    idle_mover_found = 0
    j = mover_index
    for i in range(0, mover_cnt):
	if movers[j]['state'] == 'idle_mover':
	    mv_suspect = 0
	    if external_label != None:
		# check if this mover is not in the list of suspect volumes
		vol_suspect = 0
		for vol in self.suspect_volumes:
		    if external_label == vol['external_label']:
			vol_suspect = 1
			break
		mv_suspect = 0
		if vol_suspect:
		    for mov in vol['movers']:
			if movers[j]['mover'] == mov:
			    mv_suspect = 1
			    break
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
    Trace.trace(3,"}idle_mover_next " + repr(mv))
    # return next idle mover
    return mv

# check if this volume is in suspect volume list
def is_volume_suspect(self, external_label):
    for vol in self.suspect_volumes:
	if external_label == vol['external_label']:
	    return vol
    return

# send a regret
def send_regret(self, ticket, verbose):
    Trace.trace(3,"}send_regret " + repr(ticket['status']))
    # fork off the regret sender
    generic_cs.enprint("FORKING REGRET SENDER",generic_cs.DEBUG, verbose)
    ret = self.fork()
    if ret == 0:
	try:
	    generic_cs.enprint("SENDING REGRET "+repr(ticket), generic_cs.DEBUG, \
	                   verbose)
	    Trace.trace(3,"{send_regret "+repr(ticket))
	    callback.send_to_user_callback(ticket)
	    Trace.trace(3,"}send_regret ")
	    generic_cs.enprint("REGRET SENDER EXITS", generic_cs.DEBUG, verbose)
	except:
	    generic_cs.enprint("send_regret "+str(sys.exc_info()[0])+\
			       str(sys.exc_info()[1])+repr(ticket), 
			       generic_cs.DEBUG, verbose)
	    Trace.trace(0,"send_regret "+str(sys.exc_info()[0])+\
			str(sys.exc_info()[1])+repr(ticket))

	os._exit(0)
    else:
        generic_cs.enprint("CHILD ID= "+repr(ret), generic_cs.DEBUG, verbose)


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
	Trace.trace(3,"{set_udp_client")
	self.udpc = udp_client.UDPClient()
	self.rcv_timeout = 10 # set receive timeout
	Trace.trace(3,"}set_udp_client")

    # overrides timeout handler from DispatchingWorker
    def handle_timeout(self):
	Trace.trace(3,"{handle_timeout")
	global mover_cnt
	global mover_index
	generic_cs.enprint("PROCESSING TO\nsummon queue", 
			   generic_cs.DEBUG, \
	                    self.verbose)
        generic_cs.enprint(self.summon_queue, \
	                    generic_cs.DEBUG,
			   self.verbose)
	t = time.time()
	"""
	if mover state did not change from being summoned then
	it means that it did not "respond" during timeout period.
	If number of attempts is less than max_summon_attempts,
	summon this mover again. Else remove this mover from the 
	list of known movers as well as from the list of movers 
	being summoned
	"""
	if mover_cnt != 0:
	    for mv in self.summon_queue:
		if mv['state'] == 'summoned':
		    if (t - mv['last_checked']) > self.rcv_timeout:
			if mv['summon_try_cnt'] < self.max_summon_attempts:
			    # retry summon
			    Trace.trace(3,"handle_timeout retrying " + repr(mv))
			    summon_mover(self, mv, mv["work_ticket"])
			else:
                            generic_cs.enprint("mover "+repr(mv)+\
					       " is dead", \
	                                       generic_cs.DEBUG, 
					       self.verbose)
			    # mover is dead. Remove it from all lists
			    Trace.trace(3,"handle_timeout: mover " +\
					repr(mv) + " is dead")
			    movers.remove(mv)
			    self.summon_queue.remove(mv)
			    if mover_cnt > 0:
				mover_cnt = mover_cnt - 1
				if mover_index >= mover_cnt:
				    mover_index = mover_cnt - 1
			    if mover_cnt == 0:
				# no movers left send regrets to clients
				generic_cs.enprint("handle_timeout: no movers left",
	                                       generic_cs.DEBUG, 
					       self.verbose)
				Trace.trace(3,"handle_timeout: no movers left")
				mv["work_ticket"]['status'] = (e_errors.NOMOVERS, None)
				pending_work.delete_job(mv["work_ticket"])
				send_regret(self, mv["work_ticket"], self.verbose)

				# flush pending jobs
				flush_pending_jobs(self, 
						   (e_errors.NOMOVERS, None))
				return
				
			    # try another mover
			    next_mover_found = 0
			    for i in range(0, mover_cnt):
				try:
				    next_mover = idle_mover_next(self, 
								 mv["work_ticket"]["fc"]["external_label"])
				except:
				    # there was no external label: must be write
				    next_mover = idle_mover_next(self, None)
				self.enprint("current mover "+repr(mv)+\
					     " next mover "+repr(next_mover),
					     generic_cs.DEBUG, self.verbose)
				if (next_mover) and \
				   (next_mover['mover'] != mv['mover']):
				    next_mover_found = 1
				    break

			    if next_mover_found:
				self.enprint("TO proc. will summon mover "+\
					     repr(next_mover),generic_cs.DEBUG,
					     self.verbose)
				summon_mover(self,next_mover,mv["work_ticket"])
				break
			    else:
				# no movers left
				generic_cs.enprint("handle_timeout: no movers left",
	                                       generic_cs.DEBUG, 
					       self.verbose)
				Trace.trace(3,"handle_timeout: no movers left")
				mv["work_ticket"]['status'] = (e_errors.NOMOVERS, None)
				pending_work.delete_job(mv["work_ticket"])
				send_regret(self, mv["work_ticket"], self.verbose)
				# flush pending jobs
				flush_pending_jobs(self,
						   (e_errors.NOMOVERS, None))
				return
			    
				
	else:
	    # no movers
	    # check if there are any pending works and remove them
	    flush_pending_jobs(self, (e_errors.NOMOVERS, None))
	    # also clear summon queue
	    self.summon_queue = []
	    
        self.enprint("movers queue after processing TO\nmover count "+\
	               repr(mover_cnt), generic_cs.SERVER, self.verbose)
        self.enprint(movers, 
		     generic_cs.SERVER|generic_cs.PRETTY_PRINT,
	              self.verbose)
        self.enprint("summon queue after processing TO", \
	              generic_cs.SERVER, self.verbose)
        self.enprint(self.summon_queue, \
	             generic_cs.SERVER|generic_cs.PRETTY_PRINT, 
		     self.verbose)
	Trace.trace(3,"}handle_timeout")
    
	
    def write_to_hsm(self, ticket):
	"""
	call handle_timeout to avoid the situation when due to
	requests from another encp clients TO did not work even if
	movers being summoned did not "respond"
	"""
        Trace.trace(3,"{write_to_hsm " + repr(ticket))
	self.handle_timeout()
	if movers:
	    ticket["status"] = (e_errors.OK, None)
	else:
	    ticket["status"] = (e_errors.NOMOVERS, None)
	    
        self.reply_to_caller(ticket) # reply now to avoid deadlocks
	if not movers:
	    Trace.trace(3,"}write_to_hsm: No movers available")
	    return

        format = "write Q'd %s -> %s : library=%s family=%s requester:%s"
        self.logc.send(e_errors.INFO, 2, format,
		       repr(ticket["wrapper"]["fullname"]),
		       ticket["wrapper"]["pnfsFilename"],
		       ticket["vc"]["library"],
		       ticket["vc"]["file_family"],
		       ticket["wrapper"]["uname"])
	if not ticket.has_key('lm'):
	    ticket['lm'] = {'address':self.server_address }

        pending_work.insert_job(ticket)
        self.enprint(movers, generic_cs.SERVER|generic_cs.PRETTY_PRINT, \
	             self.verbose)

	# find the next idle mover
	try:
	    label = ticket["fc"]["external_label"] # must be retry
	except:
	    label = None
	mv = idle_mover_next(self, label)
	if mv:
	    # summon this mover
	    summon_mover(self, mv, ticket)
	Trace.trace(3,"}write_to_hsm")

    def read_from_hsm(self, ticket):
	Trace.trace(3,"{read_from_hsm " + repr(ticket))
	self.enprint("read_from_hsm "+repr(ticket), generic_cs.DEBUG, \
	             self.verbose)
	# check if this volume is OK
	vc = volume_clerk_client.VolumeClerkClient(self.csc)
	v = vc.inquire_vol(ticket['fc']['external_label'])
	#self.enprint("VC "+repr(v))
	if v['system_inhibit'] == e_errors.NOACCESS:
	    # tape cannot be accessed, report back to caller and do not
	    # put ticket in the queue
	    ticket["status"] = (e_errors.NOACCESS, None)
	    self.reply_to_caller(ticket)
	    format = "read request discarded for unique_id=%s : \
	    volume %s is marked as %s"
	    self.logc.send(e_errors.ERROR, 1, format,
			   ticket['unique_id'],
			   ticket['fc']['external_label'],
			   ticket["status"][0])
	    Trace.trace(3,"}read_from_hsm: volume has no access")
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
	    """
	    if mv_found:
		self.enprint("found mover:state= "+repr(mv['state']), generic_cs.DEBUG, self.verbose)
		if mv['state'] == 'idle_mover':
		     self.enprint("found mounted vol= "+repr(ticket['fc']['external_label'])+ " at mover=" + repr(mv), generic_cs.DEBUG, self.verbose)
		     ticket["status"] = (e_errors.CONFLICT, 
					 "volume mounted on the idle mover")
		     format = "volume %s mounted on the idle mover %s"
		     logticket = self.logc.send(e_errors.ERROR, 1, format,
						repr(ticket['fc']['external_label']),
						repr(mv['mover']))
		     self.reply_to_caller(ticket)
		     return
	    """


	"""
	call handle_timeout to avoid the situation when due to
	requests from another encp clients TO did not work even if
	movers being summoned did not respond
	"""
        #self.enprint("read_from_hsm "+repr(ticket), generic_cs.DEBUG, \
	#             self.verbose)
	Trace.trace(3,"{read_from_hsm " + repr(ticket))
	self.handle_timeout()
	#self.enprint("MOVERS "+repr(movers), generic_cs.DEBUG, self.verbose)
	if movers:
	    ticket["status"] = (e_errors.OK, None)
	else:
	    ticket["status"] = (e_errors.NOMOVERS, "No movers")
	self.reply_to_caller(ticket) # reply now to avoid deadlocks
	if not movers:
	    Trace.trace(3,"}read_from_hsm: No movers available")
	    return

        format = "read Q'd %s -> %s : vol=%s bfid=%s requester:%s"
        self.logc.send(e_errors.INFO, 2, format,
		       ticket["wrapper"]["pnfsFilename"],
		       repr(ticket["wrapper"]["fullname"]),
		       ticket["fc"]["external_label"],
		       ticket["fc"]["bfid"],
		       ticket["wrapper"]["uname"])
	if not ticket.has_key('lm'):
	    ticket['lm'] = {'address' : self.server_address}

        pending_work.insert_job(ticket)

	# check if requested volume is busy
	if  not is_volume_busy(self, ticket["fc"]["external_label"]):
	    self.enprint("VOLUME IS AVAILABLE", generic_cs.DEBUG, self.verbose)
	    if not mv_found:
		# find the next idle mover
		mv = idle_mover_next(self, ticket["fc"]["external_label"])
	    else: 
		pass
		# print "mover found"
	    if mv:
		# summon this mover
		self.enprint("read_from_hsm will summon mover "+repr(mv), \
			     generic_cs.DEBUG, self.verbose)
		summon_mover(self, mv, ticket)

	Trace.trace(3,"}read_from_hsm ")
	

    # mover is idle - see what we can do
    def idle_mover(self, mticket):
	global mover_cnt
	
	Trace.trace(3,"{idle_mover " + repr(mticket))
        self.enprint("IDLE MOVER "+repr(mticket), generic_cs.DEBUG, \
	             self.verbose)

	vc = volume_clerk_client.VolumeClerkClient(self.csc)
	# remove the mover from the list of movers being summoned
	mv = remove_from_summon_list(self, mticket, mticket['work'])

	# check if there is a work for this mover in work_at_movers list
	# it should not happen in a normal operations but it may when for 
	# instance mover detects that encp is gone and returns idle or
	# mover crashes and then restarts
	
	# if mover has been removed from the summon list check it
	if not mv:
	    # print "LOOK FOR MOVER IN MOVER QUEUE" 
	    mv = find_mover(mticket, movers, self.verbose)
	if mv:
	    try:
		# try to remove work from work_at_movers list
		# print "REMOVING",mv 
		work_at_movers.remove(mv['work_ticket'])
		format = "Removing work from work at movers queue for idle mover. Work:%s mover:%s"
		self.logc.send(e_errors.INFO, 2, format,
			       repr(mv['work_ticket']),
			       repr(mv))
		# check if tape is stuck in in the mounting state
		vol_info = vc.inquire_vol(mv['work_ticket']['fc']['external_label'])
		if vol_info['at_mover'][0] == 'mounting':
		    # force set volume to unmounted
		    # print "FORCING", vol_info['at_mover']
		    v = vc.set_at_mover(mv['work_ticket']['fc']['external_label'], 'unmounted', 
					mticket["mover"], 1)
		
		    #work_at_movers.remove(mv['work_ticket'])
		    del(mv["work_ticket"])
	    except (KeyError, ValueError):
		# print "NOT REMOVED",str(sys.exc_info()[0]), str(sys.exc_info()[1])
		pass
	    except:
		traceback.print_exc()
	
        w = self.schedule()

        self.enprint("SCHEDULE RETURNED "+repr(w), generic_cs.DEBUG, \
	             self.verbose)

        # no work means we're done
        self.enprint("status "+repr(w['status'])+"\nreply address "+\
	             repr(self.reply_address), generic_cs.SERVER, self.verbose)
        if w["status"][0] == e_errors.NOWORK:
            self.reply_to_caller({"work" : "nowork"})

        # ok, we have some work - try to bind the volume
	elif w["status"][0] == e_errors.OK:
	    # check if the volume for this work had failed on this mover
            self.enprint("SUSPECT_VOLS "+repr(self.suspect_volumes), \
	                 generic_cs.DEBUG, self.verbose)
	    for item in self.suspect_volumes:
		if (w['fc']['external_label'] == item['external_label']):
                    self.enprint("FOUND volume "+item['external_label'], \
	                         generic_cs.DEBUG, self.verbose)
		    mov_is_suspect = 0
		    for i in item['movers']:
			if i == mticket['mover']:
                            self.enprint("FOUND mover "+repr(i), \
	                                 generic_cs.SERVER, self.verbose)
			    # skip this mover
			    self.reply_to_caller({"work" : "nowork"})
			    Trace.trace(3,"}idle_mover: skipping " + \
					repr(item))
			    mov_is_suspect = 1
			    break
		    if not mov_is_suspect:
			continue
	            self.enprint("MOVERS= "+repr(mover_cnt), \
                                 generic_cs.DEBUG, self.verbose)

		    # determine if all the movers are in suspect volume list 
		    # and if yes set volume as having no access and send 
		    # a regret: noaccess.

		    if len(item['movers']) >= self.max_suspect_movers:
			w['status'] = (e_errors.NOACCESS, None)

                        self.enprint("Number of movers for suspect volume "+\
	                         repr(len(item['movers'])), generic_cs.DEBUG, \
	                         self.verbose)

			# set volume as noaccess
			#vc = volume_clerk_client.VolumeClerkClient(self.csc)
			v = vc.set_system_noaccess(w['fc']['external_label'])

			#remove entry from suspect volume list
			self.suspect_volumes.remove(item)

			pending_work.delete_job(w)
			send_regret(self, w, self.verbose)
			Trace.trace(3,"}idle_mover: failed on more than "\
			 + repr(self.max_suspect_movers)+ " for " + repr(item))
			return
		    elif mover_cnt == 1:
                        self.enprint("There is only one mover in the conf.",\
	                             generic_cs.DEBUG, self.verbose)
			pending_work.delete_job(w)
			w['status'] = (e_errors.NOMOVERS, 'Read failed') # set it to something more specific
			send_regret(self, w, self.verbose)
			# check if there are any pending works and remove them
			flush_pending_jobs(self, 
					   (e_errors.NOMOVERS, 'Read failed'))
			#remove volume from suspect volume list
			self.suspect_volumes.remove(item)
			Trace.trace(3,"}idle_mover: only one mover in config." \
				    + repr(item))
		    else:
			# summon mover that can do the work
			next_mover_found = 0
			for i in range(0, mover_cnt):
			    next_mover = idle_mover_next(self, w['fc']['external_label'])
			    self.enprint("current mover "+repr(mticket['mover'])+\
					 " next mover "+ repr(next_mover), \
					 generic_cs.DEBUG, self.verbose)
			    if (next_mover) and \
			       (next_mover['mover'] != mticket['mover']):
				next_mover_found = 1
				break

			if next_mover_found:
			    self.enprint("idle will summon mover "+ \
					 repr(next_mover), generic_cs.DEBUG, self.verbose)
			    summon_mover(self, next_mover, w)
			
			return

	    # check the volume state and try to lock it
	    #vc = volume_clerk_client.VolumeClerkClient(self.csc)
	    vol_info = vc.inquire_vol(w['fc']['external_label'])
	    if vol_info['at_mover'][0] == 'unmounted':
		# set volume to mounting
		v = vc.set_at_mover(w['fc']['external_label'], 'mounting', 
				    mticket["mover"])
		if v['status'][0] != e_errors.OK:
		    format = "cannot change to 'mounting' vol=%s mover=%s state=%s"
		    self.logc.send(e_errors.INFO, 2, format,
				   w["fc"]["external_label"],
				   v['at_mover'][1], v['at_mover'][0])
		
		    Trace.trace(3,"}idle_mover: cannot change to 'mounting'"+repr(v['at_mover']))
		    self.reply_to_caller({"work" : "nowork"})
		    return
		else:
		   w['vc']['at_mover'] = v['at_mover'] 
	    else:
		self.reply_to_caller({"work" : "nowork"})
		return
		
		
            # reply now to avoid deadlocks
            format = "%s work on vol=%s state=%smover=%s requester:%s"
            self.logc.send(e_errors.INFO, 2, format,
			   w["work"],
			   w["fc"]["external_label"],
			   repr(w['vc']['at_mover']),
			   mticket["mover"],
			   w["wrapper"]["uname"])
	    pending_work.delete_job(w)
            self.reply_to_caller(w) # reply now to avoid deadlocks
            self.enprint("MOVER WORK:"+repr(w), generic_cs.DEBUG, 
			 self.verbose)
            w['mover'] = mticket['mover']
            work_at_movers.append(w)
	    mv = update_mover_list(self, mticket, 'work_at_mover')
	    mv['external_label'] = w['fc']['external_label']
	    mv["file_family"] = w["vc"]["file_family"]
            self.enprint("Work at movers appended", generic_cs.SERVER, \
	                 self.verbose)
            self.enprint(w, generic_cs.SERVER|generic_cs.PRETTY_PRINT, \
	                 self.verbose)
	    Trace.trace(3,"}idle_mover " + repr(w))
            return

        # alas
        else:
	    Trace.trace(0,"}idle_mover: assertion error " + repr(w) + " " \
			+ repr(mticket))
            self.enprint("assertion error in idle_mover w= mticket= ", \
	                 generic_cs.SERVER, self.verbose)
            self.enprint(w, generic_cs.SERVER|generic_cs.PRETTY_PRINT, \
	                 self.verbose)
            self.enprint(mticket, generic_cs.SERVER|generic_cs.PRETTY_PRINT, \
	                 self.verbose)
            raise "assertion error"

    # we have a volume already bound - any more work??
    def have_bound_volume(self, mticket):
	#print "HAVE_BOUND", mticket
	Trace.trace(3,"{have_bound_volume " + repr(mticket))
        self.enprint("LM:have_bound_volume "+repr(mticket), generic_cs.DEBUG, self.verbose)
        self.enprint(mticket, generic_cs.SERVER|generic_cs.PRETTY_PRINT, \
	             self.verbose)
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
            self.enprint("removing "+repr(w)+" from the queue", \
	                 generic_cs.SERVER, self.verbose)
	    delayed_dismount = w['encp']['delayed_dismount']
	    work_at_movers.remove(w)
	    mv = find_mover(mticket, movers, self.verbose)
	    if mv:
		try:
		    del(mv["work_ticket"])
		except KeyError:
		    pass

	else: delayed_dismount = 0
	# check if mover can accept another request
	if state != 'idle_mover':
            self.enprint("have_bound_volume "+repr(state), generic_cs.DEBUG, \
	                 self.verbose)
	    self.reply_to_caller({'work': 'nowork'})
	    return

        # otherwise, see if this volume will do for any other work pending
        w = next_work_this_volume(mticket, self.verbose)
        self.enprint("next_work_this_volume "+repr(w), generic_cs.SERVER, \
	             self.verbose)
        if w["status"][0] == e_errors.OK:
            format = "%s next work on vol=%s mover=%s requester:%s"
            self.logc.send(e_errors.INFO, 2, format,
			   w["work"],
			   w["fc"]["external_label"],
			   mticket["mover"],
			   w["wrapper"]["uname"])
	    w['times']['lm_dequeued'] = time.time()
            self.enprint("sending "+repr(w)+" to mover", generic_cs.DEBUG, \
	                 self.verbose)
            pending_work.delete_job(w)
            self.reply_to_caller(w) # reply now to avoid deadlocks
	    delayed_dismount = w['encp']['delayed_dismount']
	    state = 'work_at_mover'
	    update_mover_list(self, mticket, state)
            w['mover'] = mticket['mover']
            work_at_movers.append(w)
	    
            self.enprint("Pending Work", generic_cs.SERVER, self.verbose)
            self.enprint(w, generic_cs.SERVER|generic_cs.PRETTY_PRINT, \
	                 self.verbose)
	    Trace.trace(3,"}have_bound_volume " + repr(w))
            return

        # if the pending work queue is empty, then we're done
        elif  w["status"][0] == e_errors.NOWORK:
	    mv = find_mover(mticket, movers, self.verbose)
	    # check if delayed_dismount is set
	    mvr_found = 0
	    if len(self.del_dismount_list) != 0:
		# find mover in the delayed dismount list
		mvr = find_mover(mticket,self.del_dismount_list,self.verbose)
		if mvr: 
		    mvr_found = 1
	    try:
		if (delayed_dismount):
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
		    Trace.trace(3,"}have_bound_volume delayed dismount"+\
				repr(w))
		    return
		else:
		    # no delayed dismount: flag dismount
		    mvr_found = 1
	    except:
		# no delayed dismount: flag dismount
		mvr_found = 1 

	    if mvr_found:
		# unbind volume
		try:
		    self.del_dismount_list.remove(mv)
		except:
		    pass
		format = "unbind vol %s mover=%s"
		self.logc.send(e_errors.INFO, 2, format,
			       mticket['vc']["external_label"],
			       mticket["mover"])
		mv['state'] = 'unbind_sent'
		self.enprint("unbind volume for "+repr(mv), generic_cs.SERVER, \
			     self.verbose)
		#print "Unbind", mv
		# set volume to unmounting
		vc = volume_clerk_client.VolumeClerkClient(self.csc)
		v = vc.set_at_mover(mticket['vc']['external_label'], 
				    'unmounting', 
				    mticket["mover"])
		if v['status'][0] != e_errors.OK:
		    format = "cannot change to 'unmounting' vol=%s mover=%s state=%s"
		    self.logc.send(e_errors.INFO, 2, format,
				   mticket['vc']['external_label'],
				   v['at_mover'][1], 
				   v['at_mover'][0])
		
		    Trace.trace(3,"have_bound_volume: cannot change to 'mounting'"+repr(v['at_mover']))
		self.reply_to_caller({"work" : "unbind_volume"})
		Trace.trace(3,"}have_bound_volume: No work, sending unbind "+\
			    repr(mv))
	    

        # alas
        else:
	    Trace.trace(0,"}have_bound_volume: assertion error " \
			+ repr(w) + " " + repr(mticket))
            self.enprint("assertion error in have_bound_volume w=, mticket=", \
	                 generic_cs.SERVER, self.verbose)
            self.enprint(w, generic_cs.SERVER|generic_cs.PRETTY_PRINT, \
	                 self.verbose)
            self.enprint(mticket, generic_cs.SERVER|generic_cs.PRETTY_PRINT, \
	                 self.verbose)
            raise "assertion error"


    # if the work is on the awaiting bind list, it is the library manager's
    #  responsibility to retry
    # THE LIBRARY COULD NOT MOUNT THE TAPE IN THE DRIVE AND IF THE MOVER
    # THOUGHT THE VOLUME WAS POISONED, IT WOULD TELL THE VOLUME CLERK.
    def unilateral_unbind(self, ticket):
	Trace.trace(3,"{unilateral_unbind " + repr(ticket))
	vc = volume_clerk_client.VolumeClerkClient(self.csc)

        # get the work ticket for the volume
        self.enprint("unilateral_unbind "+repr(ticket), generic_cs.DEBUG, \
	             self.verbose)
        self.enprint(ticket, generic_cs.SERVER|generic_cs.PRETTY_PRINT, \
	             self.verbose)
        w = get_work_at_movers(ticket["external_label"])

	# remove the mover from the list of movers being summoned
	mv = remove_from_summon_list(self, ticket, 'idle_mover')

	# update list of suspected volumes
        self.enprint("SUSPECT VOLUME LIST BEFORE", generic_cs.SERVER, \
	             self.verbose)
        self.enprint(self.suspect_volumes, \
	             generic_cs.SERVER|generic_cs.PRETTY_PRINT, self.verbose)
	vol_found = 0
	for vol in self.suspect_volumes:
	    if ticket['external_label'] == vol['external_label']:
		vol_found = 1
		break
	if not vol_found:
	    vol = {'external_label' : ticket['external_label'],
		    'movers' : []
		    }
	mv_found = 0
	for mv in vol['movers']:
	    if ticket['mover'] == mv:
		mv_found = 1
	if not mv_found:
	    vol['movers'].append(ticket['mover'])
	if not vol_found:
	    self.suspect_volumes.append(vol)
        self.enprint("SUSPECT VOLUME LIST AFTER", generic_cs.DEBUG, \
	             self.verbose)
        self.enprint(self.suspect_volumes, \
	             generic_cs.DEBUG|generic_cs.PRETTY_PRINT, self.verbose)

        if w:
            self.enprint("unilateral_unbind: work_at_movers", generic_cs.DEBUG, \
	                 self.verbose)
            self.enprint(w, generic_cs.DEBUG|generic_cs.PRETTY_PRINT, \
	                 self.verbose)
            work_at_movers.remove(w)
	    mv = find_mover(ticket, movers, self.verbose)
	    if mv:
		try:
		    del(mv["work_ticket"])
		except KeyError:
		    pass


	    if ticket['state'] != 'offline':
		# change volume state to unmounting and send unmount request
		v = vc.set_at_mover(ticket['external_label'], 
				    'unmounting', 
				    ticket["mover"])
		if v['status'][0] != e_errors.OK:
		    format = "cannot change to 'unmounting' vol=%s mover=%s state=%s"
		    self.logc.send(e_errors.INFO, 2, format,
				   ticket['external_label'],
				   v['at_mover'][1], 
				   v['at_mover'][0])
		else:
		    self.enprint("unilateral_unbind: sending unbind", 
				 generic_cs.DEBUG,
				 self.verbose)
		    self.reply_to_caller({"work" : "unbind_volume"})
	    else:
		self.enprint("unilateral_unbind: sending nowork", 
			     generic_cs.DEBUG,
			     self.verbose)
		self.reply_to_caller({"work" : "nowork"})

	# determine if all the movers are in suspect volume list and if
	# yes set volume as having no access and send a regret: noaccess.
	if len(vol['movers']) >= self.max_suspect_movers:
	    w['status'] = (e_errors.NOACCESS, None)

	    # set volume as noaccess
	    v = vc.set_system_noaccess(w['fc']['external_label'])
	    label = w['fc']['external_label']
	    self.enprint("set_system_noaccess returned "+repr(v), 
			 generic_cs.DEBUG, self.verbose)

	    #remove entry from suspect volume list
	    self.suspect_volumes.remove(vol)
	    self.enprint("removed from suspect volume list "+repr(vol)+\
	                 "\nSUSPECT VOLUME LIST AFTER", generic_cs.DEBUG, self.verbose)
	    self.enprint(self.suspect_volumes, generic_cs.DEBUG, self.verbose)

	    # 01/22 do not send a regret as the mover had already
	    # sent a status to encp
	    call_back_addr = w['callback_addr']
	    send_regret(self, w, self.verbose)
	    # send regret to all clients requested this volume and remove
	    # requests from a queue
	    w = pending_work.get_init()
	    while w:
		try:
		    if w['fc']['external_label'] == label:
			w['status'] = (e_errors.NOACCESS, None)
			#if w['callback_addr'] != call_back_addr:
			print "SENDING REGRET"
			send_regret(self, w, self.verbose)
			w1 = pending_work.get_next()
			pending_work.delete_job(w)
			w = w1
		except KeyError:
		    pass
	else:
	    pass
	    # 01/22 do not send a regret as the mover had already
	    # sent a status to encp
	    # send_regret(self, w, self.verbose)
	    
	    # check if there are any pending works and remove them
	    #flush_pending_jobs(self, (e_errors.NOMOVERS, None))
	Trace.trace(3,"}unilateral_unbind ")

    # what is next on our list of work?
    def schedule(self):
	Trace.trace(3,"{schedule ")
        while 1:
            w = next_work_any_volume(self, self.csc, self.verbose)
            if w["status"][0] == e_errors.OK or \
	       w["status"][0] == e_errors.NOWORK:
		Trace.trace(3,"}schedule " + repr(w))
                return w
            # some sort of error, like write
            # work and no volume available
            # so bounce. status is already bad...
            pending_work.delete_job(w)
	    send_regret(self, w, self.verbose)
	    Trace.trace(3,"}schedule: Error detected " + repr(w))
            #callback.send_to_user_callback(w)

    # load mover list form the configurarion server
    def load_mover_list(self, ticket):
	Trace.trace(3, "{load_mover_list for " + repr(self.name))
	get_movers(self.csc, self.name, self.verbose)
	ticket['movers'] = movers
	ticket["status"] = (e_errors.OK, None)
	self.reply_to_caller(ticket)
	Trace.trace(3, "}load_mover_list for " + repr(movers))
	
    # what is going on
    def getwork(self,ticket):
        self.enprint("getwork "+ repr(ticket), generic_cs.SERVER, self.verbose)
	Trace.trace(3,"{getwork ")
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
	Trace.trace(3,"}getwork ")
        os._exit(0)

    # get list of assigned movers 
    def getmoverlist(self,ticket):
        self.enprint("getmoverlist "+ repr(ticket), generic_cs.SERVER, \
	             self.verbose)
	Trace.trace(3,"{getmoverlist ")
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
	    m = {'mover'          : mover['mover'],\
		 'address'        : mover['address'],\
		 'state'          : mover['state'],\
		 'last_checked'   : mover['last_checked'],\
		 'summon_try_cnt' : mover['summon_try_cnt'],\
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
	Trace.trace(3,"}getmoverlist ")
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
	Trace.trace(3,"{get_suspect_volumes ")
        self.enprint("get_suspect_volumes "+ repr(ticket), generic_cs.SERVER, \
	             self.verbose)
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
	Trace.trace(3,"}get_suspect_volumes ")
        os._exit(0)

    # get list of delayed dismounts 
    def get_delayed_dismounts(self,ticket):
	Trace.trace(3,"{get_delayed_dismounts ")
        self.enprint("get_delayed_dismounts "+repr(ticket),generic_cs.SERVER,
	             self.verbose)
        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket) # reply now to avoid deadlocks
        # this could tie things up for awhile - fork and let child
        # send the work list (at time of fork) back to client
        if self.fork() != 0:
            return
        self.get_user_sockets(ticket)
        rticket = {}
        rticket["status"] = (e_errors.OK, None)
        rticket["delayed_dismounts"] = self.del_dismount_list
        callback.write_tcp_socket(self.data_socket,rticket,
                                  "library_manager get_suspect_volumes, datasocket")
        self.data_socket.close()
        callback.write_tcp_socket(self.control_socket,ticket,
                                  "library_manager get_delayed_dismounts, \
				  controlsocket")
        self.control_socket.close()
	Trace.trace(3,"}get_delayed_dismounts ")
        os._exit(0)


    # get a port for the data transfer
    # tell the user I'm your library manager and here's your ticket
    def get_user_sockets(self, ticket):
	Trace.trace(3,"{get_user_sockets " + repr(ticket))
        library_manager_host, library_manager_port, listen_socket =\
                              callback.get_callback()
        listen_socket.listen(4)
        ticket["library_manager_callback_host"] = library_manager_host
        ticket["library_manager_callback_port"] = library_manager_port
        self.control_socket = callback.user_callback_socket(ticket)
        data_socket, address = listen_socket.accept()
        self.data_socket = data_socket
        listen_socket.close()
	Trace.trace(3,"}get_user_sockets " + repr(ticket))
    
    #pass

    # remove work from list of pending works
    def remove_work(self, ticket):
	try:
	    id = ticket["unique_id"]
	    Trace.trace(3,"{remove_work "+repr(id))
	except:
	    return {"status" : (e_errors.KEYERROR, "Key 'unique_id' does not exist")}
	try:
	    w = pending_work.find_job(id)
	    if w == None:
		self.reply_to_caller({"status" : (e_errors.NOWORK, "No such work")})

		return
	    else:
		pending_work.delete_job(w)
		format = "Request:%s deleted. Complete request:%s"
		self.logc.send(e_errors.INFO, 2, format,
			       repr(w["unique_id"]), repr(w))
		Trace.trace(3,"{remove_work ")
		self.reply_to_caller({"status" : (e_errors.OK, "Work deleted")})
		return
	except:
	    traceback.print_exc()

	    Trace.trace(0,"}remove_work "+str(sys.exc_info()[0])+\
                     str(sys.exc_info()[1]))
	    self.reply_to_caller({"status" : (e_errors.WRONGPARAMETER, "Request failed"+str(sys.exc_info()[0])+str(sys.exc_info()[1]))})
	    return
					 
    # change priority
    def change_priority(self, ticket):
	try:
	    id = ticket["unique_id"]
	    pri = ticket["priority"]
	    Trace.trace(3,"{change_priority "+repr(id)+repr(pri))
	except:
	    return {"status" : (e_errors.KEYERROR, "Key 'unique_id' does not exist")}
	try:
	    w = pending_work.change_pri(id, pri)
	    if w == None:
		self.reply_to_caller({"status" : (e_errors.NOWORK, "No such work or attempt to set wrong priority")})
		return
	    else:
		format = "Changed priority to:%s Complete request:%s"
		self.logc.send(e_errors.INFO, 2, format,
			       repr(w["encp"]["curpri"]), repr(w))
		Trace.trace(3,"}change_priority ")
		self.reply_to_caller({"status" : (e_errors.OK, "Priority changed")})
		return
	except:
	    
	    Trace.trace(0,"}change_priority "+str(sys.exc_info()[0])+\
                     str(sys.exc_info()[1]))
	    self.reply_to_caller({"status" : (e_errors.WRONGPARAMETER, "Request failed"+str(sys.exc_info()[0])+str(sys.exc_info()[1]))})
	    return

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
    Trace.trace(1,"libman called with args "+repr(sys.argv))

    # get an interface
    intf = LibraryManagerInterface()
    summon = intf.summon

    # get a library manager
    lm = LibraryManager(intf.name, 0, intf.verbose, intf.config_host, \
	                intf.config_port)

    """ get initial list of movers potentially belonging to this
    library manager from the configuration server
    """
    get_movers(lm.csc, intf.name, intf.verbose)

    while 1:
        try:
            #Trace.init(intf.name[0:5]+'.libm')
            Trace.init(lm.keys["logname"])
            lm.logc.send(e_errors.INFO, 1, "Library Manager "+intf.name+"(re)starting")
            lm.serve_forever()
        except:
	    traceback.print_exc()
	    if SystemExit:
		sys.exit(0)
	    else:
	        lm.serve_forever_error("library manager", lm.logc)
		continue
    Trace.trace(1,"Library Manager finished (impossible)")
