#!/usr/bin/python

import sys
import os
from SocketServer import *
from configuration_server_client import *
from volume_clerk_client import VolumeClerkClient
from callback import send_to_user_callback
from dispatching_worker import DispatchingWorker
from generic_server import GenericServer
from udp_client import UDPClient

# Read write work lists
# set up and manipulate the list of requests to read or write
# along with their priorities

pending_work = []   	# list of read write work tickets

def priority(ticket) :
	if ticket["work"] == "write_to_hsm" :
		return 10
	return 1

def queue_pending_work(ticket) :
	ticket["priority"] = priority(ticket)
	i = 0
	tryp = ticket["priority"]
	for item in pending_work :
		if tryp > item["priority"] :
			break
		i = i + 1
	pending_work.insert(i, ticket)
	return

##############################################################

work_at_movers = []
work_awaiting_bind = []

def busy_vols_in_family (family_name):
	vols = []	
	for w in work_at_movers + work_awaiting_bind :
		if w["file_family"] == family_name :
			vols.append(w["external_label"])
	return vols

def is_volume_busy(external_label) :
	for w in work_at_movers + work_awaiting_bind :
		if w["external_label"] == external_label :
			return 1
	return 0

def get_awaiting_work(external_label) :
	for w in work_awaiting_bind :
		if w["external_label"] == external_label :
			return w
	return {}

def get_work_at_movers(external_label) :
	for w in work_at_movers :
		if w["external_label"] == external_label :
			return w
	return {}


##############################################################

	
def next_work_any_volume(csc) :
	for w in pending_work:
		if w["work"] == "read_from_hsm" :
			if is_volume_busy(w["external_label"])  :
				continue
			return w
		elif w["work"] == "write_to_hsm" :  
			# ask the volume clerk for a volume,
			# but first go find volumes we _dont_ want to hear
			# about -- that is volumes in the apropriate family
			# which are currently at movers.
			vol_veto_list = busy_vols_in_family(w["file_family"])
			if len(vol_veto_list) >= w["file_family_width"] :
				continue
			# width not exceeded, ask ss for a new volume.
			vc = VolumeClerkClient(csc)
			v = vc.next_write_volume (
				w["library"], 
				w["size_bytes"], 
				w["file_family"],
				vol_veto_list) 

			if (len(vol_veto_list) == 0 and 
			      not v["status"] == "ok") :
			# If the volume clerk has no volumes and 
			# our veto list was empty, then we have run 
			# out of space for this file family. so
			# mark as an error
				w["status"] = v["status"]
				return w
			w["external_label"] = v["external_label"]
			return w
		else :
			raise "assertion error"
	return {"status" : "nowork"}

def next_work_this_volume(v) :
	for w in pending_work:
		if (w["work"] == "write_to_hsm" and 
			w["file_family"] == v["file_family"]  and
			w["size_bytes"] <= v["remaining_bytes"]) :
			w["external_label"] = v["external_label"] 
			return w
		elif (w["work"] == "read_from_hsm" and 
			w["external_label"] == v["external_label"] ) :
			return w
		else:
			print w
			raise "assertion error"
	return {"status" : "nowork"}

##############################################################

def printwork() :
	for w in work_at_movers :
		print ("at movers", w)
	for w in work_awaiting_bind :
		print ("awaiting a volume bind", w)
	for w in pending_work :
		print ("pending", w)

class LibraryManagerMethods(DispatchingWorker) :	

	def write_to_hsm(self, ticket):
		ticket["status"]="ok"
		self.reply_to_caller(ticket)
		queue_pending_work(ticket)
		return

	def read_from_hsm(self, ticket):
		ticket["status"]="ok"
		self.reply_to_caller(ticket)
		queue_pending_work(ticket)
		return

	def idle_mover(self, mticket) :
		w = self.schedule()
		if w["status"] == "nowork":
			self.reply_to_caller({"work" : "nowork"})
		elif w["status"] == "ok" :
			self.reply_to_caller({
				"work" : "bind_volume",
				"external_label" : w["external_label"] })
			work_awaiting_bind.append(w)
			pending_work.remove(w)
		else :
			raise "assert error"
		return

	def have_bound_volume(self, mticket) :
		# if we had work on the work_at_mover list, delete it
		w = get_work_at_movers (mticket["external_label"])
		if w:
			work_at_movers.remove(w)

		# if we have work awaiting the bind, pass that work
		#  and delete it from the list, return
		w = get_awaiting_work(mticket["external_label"])
		if w :
			self.reply_to_caller(w)
			work_awaiting_bind.remove(w)
			work_at_movers.append(w)
			return

		# otherwise, see if this volume will do for any 
		# other work pending
		w = next_work_this_volume(mticket)

		if w["status"] == "ok" :
			self.reply_to_caller(w)
			pending_work.remove(w)
			work_at_movers.append(w)
	
		elif  w["status"] == "nowork" :
			self.reply_to_caller({"work" : "unbind_volume"})

		else:
			raise "assertion error"

	def unilateral_unbind(self, ticket) :
		# if the work is on the awaitng bind list, it is
		# the library manager's responsibility to retry
		# THE LIBRARY COULD NOT MOUNT THE TAPE IN THE DRIVE
		# AND IF THE MOVER THOUGHT THE VOLUME WAS POISONED, IT
		# WOULD TELL THE VOLUME CLERK.
		w = get_awaiting_work(ticket["external_label"])
		if w:
			work_awaiting_bind.remove(w)
			queue_pending_work(w)
				
		# else, it is the user's responsibility to retry
		w = get_work_at_movers (ticket["external_label"])
		if w:
			work_awaiting_movers.remove(w)

		self.reply_to_caller({"work" : "nowork"})
		
	def schedule(self) :
		while 1 :
			w = next_work_any_volume(self.csc)
			if w["status"] == "ok" or w["status"] == "nowork" :
				return w
			# some sort of error, like write
			# work and no volume available
			# so bounce. status is already bad...
			pending_work.remove(w)	
			send_to_user_callback(w)

class LibraryManager(LibraryManagerMethods, GenericServer, UDPServer) : pass

class LibraryManagerClient :
	def __init__(self, configuration_server_client) :
		self.u = UDPClient()
		self.csc = configuration_server_client

	def write_to_hsm(self, ticket) :
		return self.u.send(ticket)

	def read_from_hsm(self, ticket) :
		return self.u.send(ticket)
			
if __name__ == "__main__" :
	#
        # find the volume server
        # in order to find our node name and port.
        #
	csc = configuration_server_client()
	keys = csc.get(sys.argv[1] + ".library_manager")
	#
	#  set ourself up on that port and start serving
        #
	methods =  LibraryManagerMethods()
	vs =  LibraryManager( (keys['host'], keys['port']), methods)
	vs.set_csc(csc)
	vs.serve_forever()

