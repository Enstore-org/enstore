#!/usr/bin/python

import sys
import os
from SocketServer import *
from configuration_server_client import *
from callback import send_to_user_callback
from dispatching_worker import DispatchingWorker
from generic_server import GenericServer
from journal import JournalDict

dict = JournalDict({},"volume_clerk.jou")

class VolumeClerkMethods(DispatchingWorker) :
	
	def addvol(self, ticket):
		# add : except out to error if name exists
		# add : some sort of hook to keep old versions of the s/w out
		#        since we should like to have some control 
		#        over the format of the records.
		external_label = ticket["external_label"]
                if dict.has_key(external_label) :
			ticket["status"] = "volume already exists"
			return ticket
		dict[external_label] = ticket
		ticket["status"] = "ok"
		self.reply_to_caller(ticket)
		return

	def delvol(self, ticket):
		ticket["status"] = "ok"
		try:
			del dict[ticket["external_label"]]
		except KeyError:
			ticket["status"] = "no such volume"
		self.reply_to_caller(ticket)
		return

	def next_write_volume (self, ticket) :
		#  this looks awful, but I tried a smaller test on 16000
		#  entries, so we have time to fix it... It would be 
		#  better, I suppose, to use volumes in the order they
		#  were declared to us.
		exec ("vol_veto_list = " + ticket["vol_veto_list"])
		min_remaining_bytes = ticket["min_remaining_bytes"]
		library = ticket["library"]
		for k in dict.keys() :
			v = dict[k]
			if not v["library"] == library :
				continue
			if not v["user_inhibit"] == "none" :
				continue
			if not v["error_inhibit"] == "none" :
				continue
			if v["remaining_bytes"] < min_remaining_bytes :
				continue
			vetoed = 0
			extl = v["external_label"]
			for veto in vol_veto_list :
				if extl == veto :
					vetoed = 1
			if vetoed : 
				continue
			v["status"] = "ok"
			self.reply_to_caller(v)
			return
		# default case.
		ticket["status"] = "no new volume"
		self.reply_to_caller(ticket)
		return
				
	def set_remaining_bytes(self, ticket) :
		try:
			key = ticket["external_label"]
			record = dict[key]
			record["remaining_bytes"] = ticket["remaining_bytes"]
			record["eod_cookie"] = ticket["eod_cookie"]
			record["error_inhibit"] = "none"
			dict[key] = record # THIS WILL JOURNAL IT
			record["status"] = "ok"
		except KeyError:
			record["status"] = "no such volume"
		self.reply_to_caller(record)
		return

	def inquire_vol(self, ticket) :
		try:
			old = dict[ticket["external_label"]]
			ticket = old
			ticket["status"] = "ok"
		except KeyError:
			ticket["status"] = "no such volume"
		self.reply_to_caller(ticket) 
		return

	def set_writing(self, ticket) :
		try:
			key = ticket["external_label"]
			record = dict[key]
			record ["error_inhibit"] = "writing"
			dict[key] = record # THIS WILL JOURNAL IT
			record["status"] = "ok"
		except KeyError:
			record["status"] = "no such volume"
		self.reply_to_caller(record)
		return record

class VolumeClerk(VolumeClerkMethods, GenericServer, UDPServer) : pass

if __name__ == "__main__" :
	#
        # find our node name and port.
        #
	csc = configuration_server_client()
	keys = csc.get("volume_clerk")
	vs =  VolumeClerk((keys['host'], keys['port']), VolumeClerkMethods)
	vs.set_csc(csc)
	vs.serve_forever()
