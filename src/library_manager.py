###############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
import os
import time
import timeofday
import traceback
import sys
import errno

# enstore imports
import log_client
import SocketServer
import configuration_client
import volume_clerk_client
import callback
import dispatching_worker
import generic_server
import Trace
import udp_client

import pprint

import manage_queue
import e_errors

pending_work = manage_queue.Queue()       # list of read or write work tickets

##############################################################
movers = []    # list of movers belonging to this LM
mover_cnt = 0  # number of movers in the queue

# add mover to the movers list
def add_mover(name, address):
    global mover_cnt
    Trace.trace(4, "{add_mover " + repr(name) + " " + repr(address))
    mover = {'mover'   : name,
	     'address' : address,
	     'state'   : 'idle_mover',
	     'last_checked' : time.time(),
	     'summon_try_cnt' : 0
	     }
    movers.append(mover)
    mover_cnt = mover_cnt + 1
    Trace.trace(4, "}add_mover " + repr(mover) + "mover count=" + \
		repr(mover_cnt))
    

# get list of assigned movers from the configuration list
def get_movers(config_client, lm_name):
    Trace.trace(3, "{get_movers for " + repr(lm_name))
    if list: print "get_movers"
    movers_list = config_client.get_movers(lm_name)
    if list: pprint.pprint(movers_list)
    if movers_list:
	if (movers_list.has_key('mover') and
	    movers_list.has_key('address')):
	    add_mover(movers_list['mover'], movers_list['address'])
    Trace.trace(3, "}get_movers " + repr(movers))
    if list:
	if movers:
	    pprint.pprint(movers)
	else:
	    print "no movers defined in the cofiguration for this LM"

# find mover in the list
def find_mover(mover, mover_list):
    Trace.trace(4,"{find_mover " + repr(mover) + "in " + repr(mover_list))
    found = 0
    try:
	for mv in mover_list:
	    if ((mover['mover'] == mv['mover']) and
		(mover['address'] == mv['address'])):
		found = 1
		break
	if not found:
	    mv = {}
	Trace.trace(4,"}find_mover "+repr(mv))
	return mv
    except KeyError:
	Trace.trace(0,"}find_mover "+str(sys.exc_info()[0])+\
                     str(sys.exc_info()[1])+repr(mover))
	if list: 
	    print "keyerror"
	    pprint.pprint(mover)
	    print "find_mover "+repr(mover)
    
# update mover list
def update_mover_list(mover):
    Trace.trace(3,"{update_mover_list " + repr(mover))
    mv = find_mover(mover, movers)
    if mv == None:
	return
    if not mv:
	add_mover(mover['mover'], mover['address'])
    else:
	if mover.has_key('work'):
	    # change mover state
	    if list: print "changing mover state"
	    mv['state'] = mover['work']
	    mv['last_checked'] = time.time()
    Trace.trace(3,"}update_mover_list ")
    if list: 
	print "MOVER_LIST"
	pprint.pprint(movers)

# remove mover from list
def remove_mover(mover, mover_list):
    Trace.trace(3,"{remove_mover " + repr(mover) + "from " + repr(mover_list))
    mv = find_mover(mover, mover_list)
    if mv == None:
	return
    if mv:
	mover_list.remove(mv)
	Trace.trace(3,"}remove_mover " + repr(mv))
	
	

##############################################################

work_at_movers = []

# return a list of busy volumes for a given file family
def busy_vols_in_family (family_name):
    Trace.trace(4,"{busy_vols_in_family " + repr(family_name))
    vols = []
    for w in work_at_movers:
     try:
        if w["vc"]["file_family"] == family_name:
            vols.append(w["fc"]["external_label"])
     except:
	Trace.trace(0,"}busy_vols_in_family "+str(sys.exc_info()[0])+\
                     str(sys.exc_info()[1])) 
        pprint.pprint(w)
        pprint.pprint(work_at_movers)
        os._exit(222)
    Trace.trace(4,"}busy_vols_in_family ")
    return vols


# check if a particular volume with given label is busy
def is_volume_busy(external_label):
    Trace.trace(4,"{is_volume_busy " + repr(external_label))
    rc = 0
    for w in work_at_movers:
        if w["fc"]["external_label"] == external_label:
	    rc = 1
	    break
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
def next_work_any_volume(csc):
    Trace.trace(3,"{next_work_any_volume "+repr(csc))

    # look in pending work queue for reading or writing work
    w=pending_work.get_init()
    while w:
        # if we need to read and volume is busy, check later
        if w["work"] == "read_from_hsm":
            if is_volume_busy(w["fc"]["external_label"]) :
                w=pending_work.get_next()
                continue
            # otherwise we have found a volume that has read work pending
	    Trace.trace(3,"}next_work_any_volume "+ repr(w))
            return w

        # if we need to write: ask the volume clerk for a volume, but first go
        # find volumes we _dont_ want to hear about -- that is volumes in the
        # apropriate family which are currently at movers.
        elif w["work"] == "write_to_hsm":
            vol_veto_list = busy_vols_in_family(w["vc"]["file_family"])
            # only so many volumes can be written to at one time
            if len(vol_veto_list) >= w["vc"]["file_family_width"]:
                w=pending_work.get_next()
                continue
            # width not exceeded, ask volume clerk for a new volume.
            vc = volume_clerk_client.VolumeClerkClient(csc)
            first_found = 0
            t1 = time.time()
            v = vc.next_write_volume (w["vc"]["library"],
                                      w["wrapper"]["size_bytes"],\
                                      w["vc"]["file_family"], vol_veto_list,\
                                      first_found)
            t2 = time.time()-t1
            #print "  next_write_volume dt=",t2

            # If the volume clerk has no volumes and our veto list was empty,
            # then we have run out of space for this file family == error
            if (len(vol_veto_list) == 0 and v["status"][0] != e_errors.OK):
                w["status"] = v["status"]
		Trace.trace(0,"next_work_any_volume "+ repr(w))
                return w
            # found a volume that has write work pending - return it
            w["fc"] = {} # clear old info or create new subticket
            w["fc"]["external_label"] = v["external_label"]
	    Trace.trace(3,"}next_work_any_volume "+ repr(w))
            return w

        # alas, all I know about is reading and writing
        else:
	    Trace.trace(0,"}next_work_any_volume \
	    assertion error in next_work_any_volume w="+ repr(w))
            #import pprint
            print "assertion error in next_work_any_volume w="
            pprint.pprint(w)
            raise "assertion error"
        w=pending_work.get_next()
    # if the pending work queue is empty, then we're done
    Trace.trace(3,"}next_work_any_volume: pending work queue is empty ")
    return {"status" : (e_errors.NOWORK, None)}


# is there any work for this volume??  v is a work ticket with info
def next_work_this_volume(v):
    Trace.trace(3,"{next_work_this_volume "+repr(v))
    # look in pending work queue for reading or writing work
    w=pending_work.get_init()
    while w:

        # writing to this volume?
        if (w["work"]                == "write_to_hsm"   and
            w["vc"]["file_family"]   == v["file_family"] and
            v["user_inhibit"]        == "none"           and
            v["system_inhibit"]      == "none"           and
            w["wrapper"]["size_bytes"] <= v["remaining_bytes"]):
            w["fc"] = {} # clear old info or create new subticket
            w["fc"]["external_label"] = v["external_label"]
            # ok passed criteria, return write work ticket
	    Trace.trace(3,"}next_work_this_volume " + repr(w))
            return w

        # reading from this volume?
        elif (w["work"]           == "read_from_hsm" and
              w["fc"]["external_label"] == v["external_label"] ):
            # ok passed criteria, return read work ticket
	    Trace.trace(3,"}next_work_this_volume " + repr(w))
            return w
        w=pending_work.get_next()
    # if the pending work queue for this volume is empty, then we're done
    Trace.trace(3,"}next_work_this_volume: pending work queue for this volume\
    is empty")
    return {"status" : (e_errors.NOWORK, None)}

##############################################################

def summon_mover(self, mover):
    if not summon: return
    Trace.trace(3,"{summon_mover " + repr(mover))
    mover['last_checked'] = time.time()
    mover['state'] = 'summoned'
    mover['summon_try_cnt'] = mover['summon_try_cnt'] + 1
    mv = find_mover(mover, self.summon_queue)
    if list: print "MV=", mv
    if not mv:
	self.summon_queue.append(mover)
	    
    summon_rq = {'work': 'summon',
		 'address': self.server_address }
    if list: print 'summon_rq', summon_rq
    mover['tr_error'] = self.udpc.send_no_wait(summon_rq, mover['address'])
    if list: 
	print "summon_queue"
	pprint.pprint(self.summon_queue)
    Trace.trace(3,"}summon_mover " + repr(mover))


# find the next idle mover
def idle_mover_next(self):
    Trace.trace(3,"{idle_mover_next ")
    idle_mover_found = 0
    for i in range(self.summon_queue_index, mover_cnt):
	if movers[i]['state'] == 'idle_mover':
	    idle_mover_found = 1
	    self.summon_queue_index = i
	    break
	else:
	    continue
    if idle_mover_found:
	mv = movers[i]
    else:
	mv = None
    Trace.trace(3,"}idle_mover_next " + repr(mv))
    return mv

# send a regret
def send_regret(ticket):
    # fork off the regret sender
    if list:
	print "FORKING REGRET SENDER"
    ret = os.fork()
    if ret == 0:
	Trace.trace(3,"{send_regret "+repr(ticket))
	callback.send_to_user_callback(ticket)
	Trace.trace(3,"}send_regret ")
	os._exit(0)
    else:
	print "CHILD ID=", ret


class LibraryManager(dispatching_worker.DispatchingWorker,
		     generic_server.GenericServer,
                     SocketServer.UDPServer):

    summon_queue = []   # list of movers being summoned
    max_summon_attempts = 3
    summon_queue_index = 0
    suspect_volumes = [] # list of suspected volumes

    def set_udp_client(self):
	Trace.trace(3,"{set_udp_client")
	self.udpc = udp_client.UDPClient()
	self.rcv_timeout = 10 # set receive timeout
	Trace.trace(3,"}set_udp_client")

    # overrides timeout handler from SocketServer
    def handle_timeout(self):
	Trace.trace(3,"{handle_timeout")
	global mover_cnt
	if list: 
	    print "PROCESSING TO"
	    print "summon queue"
	    pprint.pprint(self.summon_queue)
	t = time.time()
	"""
	if mover state did not change from being summoned then
	it means that it did not "respond" during timeout period.
	If number of attempts is less than max_summon_attempts,
	summon this mover again. Else remove this mover from the 
	list of known movers as well as from the list of movers 
	being summoned
	"""
	for mv in self.summon_queue:
	    if mv['state'] == 'summoned':
		if (t - mv['last_checked']) > self.rcv_timeout:

		    if mv['summon_try_cnt'] < self.max_summon_attempts:
			# retry summon
			Trace.trace(3,"handle_timeout retrying " + repr(mv))
			self.summon_mover(mv)
		    else:
			# mover is dead. Remove it from all lists
			Trace.trace(3,"handle_timeout: mover " + repr(mv) \
				    + " is dead")
			movers.remove(mv)
			self.summon_queue.remove(mv)
			if mover_cnt > 0:
			    mover_cnt = mover_cnt - 1

	if list: 
	    print "movers queue after processing TO"
	    pprint.pprint(movers)
	    print "summon queue after processing TO"
	    pprint.pprint(self.summon_queue)
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

        format = "write Q'd %s -> %s : library=%s family=%s requestor:%s"
        logticket = self.logc.send(log_client.INFO, 2, format,
                                   repr(ticket["wrapper"]["fullname"]),
                                   ticket["wrapper"]["pnfsFilename"],
                                   ticket["vc"]["library"],
                                   ticket["vc"]["file_family"],
                                   ticket["wrapper"]["uname"])
	if not ticket.has_key('lm'):
	    ticket['lm'] = {'address':self.server_address }

        pending_work.insert_job(ticket)
	if list: pprint.pprint(movers)

	# find the next idle mover
	mv = idle_mover_next(self)
	if mv != None:
	    # summon this mover
	    summon_mover(self, mv)
	Trace.trace(3,"}write_to_hsm")

    def read_from_hsm(self, ticket):
	"""
	call handle_timeout to avoid the situation when due to
	requests from another encp clients TO did not work even if
	movers being summoned did not respond
	"""
	Trace.trace(3,"{read_from_hsm " + repr(ticket))
	self.handle_timeout()
	if movers:
	    ticket["status"] = (e_errors.OK, None)
	else:
	    ticket["status"] = (e_errors.NOMOVERS, "No movers")
        self.reply_to_caller(ticket) # reply now to avoid deadlocks
	if not movers:
	    Trace.trace(3,"}read_from_hsm: No movers available")
	    return

        format = "read Q'd %s -> %s : vol=%s bfid=%s requestor:%s"
        logticket = self.logc.send(log_client.INFO, 2, format,
                                   ticket["wrapper"]["pnfsFilename"],
                                   repr(ticket["wrapper"]["fullname"]),
                                   ticket["fc"]["external_label"],
                                   ticket["fc"]["bfid"],
                                   ticket["wrapper"]["uname"])
	if not ticket.has_key('lm'):
	    ticket['lm'] = {'address' : self.server_address}

        pending_work.insert_job(ticket)

	# find the next idle mover
	mv = idle_mover_next(self)
	if mv != None:
	    # summon this mover
	    summon_mover(self, mv)
	Trace.trace(3,"}read_from_hsm ")
	

    # mover is idle - see what we can do
    def idle_mover(self, mticket):
	global mover_cnt
	
	Trace.trace(3,"{idle_mover " + repr(mticket))
	if list: print "IDLE MOVER"
	update_mover_list(mticket)
	# remove the mover from the list of movers being summoned
	mv = find_mover(mticket, self.summon_queue)
	if ((mv != None) and mv):
	    mv['tr_error'] = 'ok'
	    mv['summon_try_cnt'] = 0
	    self.summon_queue.remove(mv)

        w = self.schedule()
	if list: 
	    print "SHEDULE RETURNED"
	    pprint.pprint(w)

        # no work means we're done
        if w["status"][0] == e_errors.NOWORK:
            self.reply_to_caller({"work" : "nowork"})

        # ok, we have some work - bind the volume
	elif w["status"][0] == e_errors.OK:
	    # check if the volume for this work had failed on this mover
	    for item in self.suspect_volumes:
		if (w['fc']['external_label'] == item['external_label']):
		    if list: print "FOUND volume ", item['external_label']
		    for i in item['movers']:
			if i == mticket['mover']:
			    if list: print "FOUND mover ", i
			    # skip this mover
			    self.reply_to_caller({"work" : "nowork"})
			    Trace.trace(3,"}idle_mover: skipping " + \
					repr(item))
			    return

		    if len(item['movers']) > 1:
			if list: 
			    print "Number of movers for suspect volume", \
				  len(item['movers'])
			pending_work.delete_job(w)
			w['status'] = (e_errors.READERROR, 'Read failed')
			send_regret(w)
			#remove volume from suspect volume list
			self.suspect_volumes.remove(item)
			Trace.trace(3,"}idle_mover: failed on more than \
			 1 mover " + repr(item))
			return
		    elif mover_cnt == 1:
			if list:
			    print "There is only one mover in the \
			    configuration"
			pending_work.delete_job(w)
			w['status'] = (e_errors.READERROR, 'Read failed') # set it to something more specific
			send_regret(w)
			#remove volume from suspect volume list
			self.suspect_volumes.remove(item)
			Trace.trace(3,"}idle_mover: only one mover in config." \
				    + repr(item))
			return

            # reply now to avoid deadlocks
            format = "%s work on vol=%s mover=%s requestor:%s"
            logticket = self.logc.send(log_client.INFO, 2, format,
                                       w["work"],
                                       w["fc"]["external_label"],
                                       mticket["mover"],
                                       w["wrapper"]["uname"])
	    w['times']['lm_dequeued'] = time.time()
            self.reply_to_caller(w) # reply now to avoid deadlocks
	    if list:
		print "MOVER WORK:"
		pprint.pprint(w)
	    pending_work.delete_job(w)
            w['mover'] = mticket['mover']
            work_at_movers.append(w)
	    if list: 
		print "Work awaiting bind"
		pprint.pprint(w)
	    Trace.trace(3,"}idle_mover " + repr(w))
            return

        # alas
        else:
	    Trace.trace(0,"}idle_mover: assertion error " + repr(w) + " " \
			+ repr(mticket))
	    if list: 
		print "assertion error in idle_mover w=, mticket="
		pprint.pprint(w)
		pprint.pprint(mticket)
            raise "assertion error"

    # we have a volume already bound - any more work??
    def have_bound_volume(self, mticket):
	Trace.trace(3,"{have_bound_volume " + repr(mticket))
	if list: 
	    print "LM:have_bound_volume"
	    pprint.pprint(mticket)
	# update mover list. If mover is in the list - update its state
	update_mover_list(mticket)
	# remove the mover from the list of movers being summoned
	mv = find_mover(mticket, self.summon_queue)
	if ((mv != None) and mv):
	    mv['tr_error'] = 'ok'
	    mv['summon_try_cnt'] = 0
	    self.summon_queue.remove(mv)

        # just did some work, delete it from queue
        w = get_work_at_movers (mticket['vc']["external_label"])
        if w:
            work_at_movers.remove(w)

        # otherwise, see if this volume will do for any other work pending
        w = next_work_this_volume(mticket)
        if w["status"][0] == e_errors.OK:
            format = "%s next work on vol=%s mover=%s requestor:%s"
            logticket = self.logc.send(log_client.INFO, 2, format,
                                       w["work"],
                                       w["fc"]["external_label"],
                                       mticket["mover"],
                                       w["wrapper"]["uname"])
	    w['times']['lm_dequeued'] = time.time()
            self.reply_to_caller(w) # reply now to avoid deadlocks
            pending_work.delete_job(w)
            w['mover'] = mticket['mover']
            work_at_movers.append(w)
	    if list: 
		print "Pending Work"
		pprint.pprint(w)
	    Trace.trace(3,"}have_bound_volume " + repr(w))
            return


        # if the pending work queue is empty, then we're done
        elif  w["status"][0] == e_errors.NOWORK:
            format = "unbind vol %s mover=%s"
            logticket = self.logc.send(log_client.INFO, 2, format,
                                       mticket['vc']["external_label"],
                                       mticket["mover"])
            self.reply_to_caller({"work" : "unbind_volume"})
	    Trace.trace(3,"}have_bound_volume: No work, sending unbind ")

        # alas
        else:
	    Trace.trace(0,"}have_bound_volume: assertion error " \
			+ repr(w) + " " + repr(mticket))
	    if list: 
		print "assertion error in have_bound_volume w=, mticket="
		pprint.pprint(w)
		pprint.pprint(mticket)
            raise "assertion error"


    # if the work is on the awaiting bind list, it is the library manager's
    #  responsibility to retry
    # THE LIBRARY COULD NOT MOUNT THE TAPE IN THE DRIVE AND IF THE MOVER
    # THOUGHT THE VOLUME WAS POISONED, IT WOULD TELL THE VOLUME CLERK.
    def unilateral_unbind(self, ticket):
	Trace.trace(3,"{unilateral_unbind " + repr(ticket))
        # get the work ticket for the volume
	if list: 
	    print "unilateral_unbind"
	    pprint.pprint(ticket)
        w = get_work_at_movers(ticket["external_label"])

	# update mover list. If mover is in the list - update its state
	update_mover_list(ticket)

	# remove the mover from the list of movers being summoned
	mv = find_mover(ticket, self.summon_queue)
	if ((mv != None) and mv):
	    mv['tr_error'] = 'ok'
	    mv['summon_try_cnt'] = 0
	    self.summon_queue.remove(mv)

	# update list of suspected volumes
	if list: 
	    print "SUSPECT VOLUME LIST BEFORE"
	    pprint.pprint(self.suspect_volumes)
	vol_found = 0
	for item in self.suspect_volumes:
	    if ticket['fc']['external_label'] == item['external_label']:
		vol_found = 1
		break
	if not vol_found:
	    item = {'external_label' : ticket['external_label'],
		    'movers' : []
		    }
	mv_found = 0
	for mv in item['movers']:
	    if ticket['mover'] == mv:
		mv_found = 1
	if not mv_found:
	    item['movers'].append(ticket['mover'])
	if not vol_found:
	    self.suspect_volumes.append(item)
	if list: 
	    print "SUSPECT VOLUME LIST AFTER"
	    pprint.pprint(self.suspect_volumes)

        if w:
	    if list: 
		print "unbind: work_at_movers" 
		pprint.pprint(w)
            work_at_movers.remove(w)

        self.reply_to_caller({"work" : "nowork"})
	Trace.trace(3,"}unilateral_unbind ")

    # what is next on our list of work?
    def schedule(self):
	Trace.trace(3,"{schedule ")
        while 1:
            w = next_work_any_volume(self.csc)
            if w["status"][0] == e_errors.OK or \
	       w["status"][0] == e_errors.NOWORK:
		Trace.trace(3,"}schedule " + repr(w))
                return w
            # some sort of error, like write
            # work and no volume available
            # so bounce. status is already bad...
            pending_work.delete_job(w)
	    send_regret(w)
	    Trace.trace(3,"}schedule: Error detected " + repr(w))
            #callback.send_to_user_callback(w)


    # what is going on
    def getwork(self,ticket):
	Trace.trace(3,"{getwork ")
        ticket["status"] = (e_errors.OK, None)
        self.reply_to_caller(ticket) # reply now to avoid deadlocks
        # this could tie things up for awhile - fork and let child
        # send the work list (at time of fork) back to client
        if os.fork() != 0:
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

if __name__ == "__main__":
    import sys
    import getopt
    import string
    # Import SOCKS module if it exists, else standard socket module socket
    # This is a python module that works just like the socket module, but uses
    # the SOCKS protocol to make connections through a firewall machine.
    # See http://www.w3.org/People/Connolly/support/socksForPython.html or
    # goto www.python.org and search for "import SOCKS"
    try:
        import SOCKS; socket = SOCKS
    except ImportError:
        import socket
    Trace.init("libman")
    Trace.trace(1,"libman called with args "+repr(sys.argv))

    # defaults
    #config_host = "localhost"
    (config_hostname,ca,ci) = socket.gethostbyaddr(socket.gethostname())
    config_host = ci[0]
    config_port = "7500"
    config_list = 0
    list = 0
    summon = 0

    # see what the user has specified. bomb out if wrong options specified
    options = ["config_host=","config_port=","config_list","list","summon","help"]
    optlist,args=getopt.getopt(sys.argv[1:],'',options)
    for (opt,value) in optlist:
        if opt == "--config_host":
            config_host = value
        elif opt == "--config_port":
            config_port = value
        elif opt == "--config_list":
            config_list = 1
        elif opt == "--list":
	    print "setting list"
            list = 1
        elif opt == "--summon":
	    print "setting summon"
            list = 1
        elif opt == "--help":
            print "python ",sys.argv[0], options, "library"
            print "   do not forget the '--' in front of each option"
            sys.exit(0)

    # bomb out if can't translate host
    ip = socket.gethostbyname(config_host)

    # bomb out if port isn't numeric
    config_port = string.atoi(config_port)

    # bomb out if we don't have a library
    if len(args) < 1:
        print "python",sys.argv[0], options, "library"
        print "   do not forget the '--' in front of each option"
        sys.exit(1)

    csc = configuration_client.ConfigurationClient(config_host,config_port,\
                                                    config_list)

    keys = csc.get(args[0])


    #  set ourself up on that port and start serving
    #methods =  LibraryManagerMethods()
    #lm =  LibraryManager( (keys['hostip'], keys['port']), methods)
    lm =  LibraryManager( (keys['hostip'], keys['port']), 'unused param')
    lm.set_csc(csc)
    
    """ get initial list of movers potentially belonging to this
    library manager from the configuration server
    """
    get_movers(lm.csc, args[0])

    # get a logger
    logc = log_client.LoggerClient(csc, keys["logname"],  'logserver', 0)
    lm.set_logc(logc)
    lm.set_udp_client()

    while 1:
        try:
            Trace.init(args[0][0:5]+'.libm')
            logc.send(log_client.INFO, 1, "Library Manager"+args[0]+"(re)starting")
            lm.serve_forever()
        except:
	    if SystemExit:
		sys.exit(0)
	    else:
		traceback.print_exc()
		format = timeofday.tod()+" "+\
			 str(sys.argv)+" "+\
			 str(sys.exc_info()[0])+" "+\
			 str(sys.exc_info()[1])+" "+\
			 "library manager serve_forever continuing"
		logc.send(log_client.ERROR, 1, format)
		Trace.trace(0,format)
		continue
    Trace.trace(1,"Library Manager finished (impossible)")
