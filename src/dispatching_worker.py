#!/usr/bin/python

from SocketServer import *

dict = {}

#
# Generic requestr response server class, for multiple connections
#
import socket

class DispatchingWorker:

	def process_request(self, request, client_address) :
		exec ( "id, number, ticket = " + request)
		self.reply_address = client_address
		self.client_number = number
		self.current_id = id
		try :
			exec ("list = " + dict[id])
			if list[0] == number :
				self.reply_with_list(list)
				return
			elif list[0] < number :
				pass # new request, fall through
			else:
				return #old news, timing race....
		except KeyError:
			pass # first request, fall through 
		try :
			function = ticket["work"]
		except KeyError:
			ticket = {'status' : 
			"cannot find requested function"}
			self.reply_to_caller(ticket)
			return
		exec ("self." + function + "(ticket)")
		return

	def reply_to_caller(self, ticket) :
	 	reply = (self.client_number, ticket)
		self.reply_with_list(reply) 

	def reply_with_list(self, list) :
		dict[id] = list
		self.socket.sendto(`list`, self.reply_address)
		return





