#!/usr/bin/python

import sys
import os
import time
from SocketServer import *
from configuration_server_client import *
from volume_clerk_client import VolumeClerkClient
from library_manager import LibraryManagerClient
from dispatching_worker import DispatchingWorker
from generic_server import GenericServer
from udp_client import UDPClient
from journal import JournalDict


dict = JournalDict({}, "file_clerk.jou")

class FileClerkMethods(DispatchingWorker) :

	def new_bit_file(self, ticket) :
		bfid = self.unique_bit_file_id()
		dict[bfid] = ticket
		ticket["bfid"] = bfid
		ticket["status"] = "ok"
		self.reply_to_caller(ticket)
		return 

	def read_from_hsm(self, ticket) :
		# verify that the bit file id is o.k.
		# call the volume server to find the library
		# copy to the work ticket the salient information
		try :
			finfo = dict[ticket["bfid"]]
			ticket["external_label"]   = finfo["external_label"]
			ticket["bof_space_cookie"] = finfo["bof_space_cookie"]
			ticket["external_label"]   = finfo["external_label"]
			ticket["sanity_cookie"]    = finfo["sanity_cookie"]
			ticket["complete_crc"]     = finfo["complete_crc"]
		except KeyError :
			self.reply_to_caller({"status" : `dict.keys()`})
			return

		# found the bit file i.d, now go and find the library
		vc = VolumeClerkClient(self.csc)
		vticket = vc.inquire_vol(ticket["external_label"])
		if not vticket["status"] == "ok" :
			self.reply_to_caller(vticket)
			return
		library = vticket["library"]

		# got the library, now send it to the apropos vol mgr
		vmticket = csc.get(library + ".library_manager")
		if not vmticket["status"] == "ok" :
			self.reply_to_caller(vmticket)
			return
		u = UDPClient()
		# send to volulme mgr and tell user what ever....
		ticket = u.send(ticket, (vmticket['host'], vmticket['port']))
		self.reply_to_caller(ticket)
		return 

	def unique_bit_file_id(self) :
	        # make a 64-bit number whose most significant
	        # part is based on the time, and the least significant
	        # part is a count to make it unique
		bfid = time.time()	        
 		bfid = long(bfid)*100000
		while dict.has_key(`bfid`) :
			bfid = bfid + 1
		return `bfid`

class FileClerk(FileClerkMethods, GenericServer, UDPServer) : pass

if __name__ == "__main__" :
	#
	#   pretend that we are the test system
	#   remember, in a system, there is only one bfs
 	#   get our port and host from the name server
	#   exit if the host is not this machine
	#
	csc = configuration_server_client() 
	ticket = csc.get("file_clerk")
	cs = FileClerk( (ticket["host"], ticket["port"]), 
						FileClerkMethods)
	cs.set_csc(csc)
	cs.serve_forever()












