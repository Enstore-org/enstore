
import sys
from configuration_server_client import *
from udp_client import UDPClient
from pnfs_surrogate import *

class FileClerkClient :
	def __init__(self, configuration_server_client) :
		self.csc = configuration_server_client
		self.u = UDPClient()

	def send (self, ticket) :
		vticket = self.csc.get("file_clerk")
		ticket = self.u.send(ticket,
					(vticket['host'], vticket['port'])
				     )
		return ticket
 
	def read_from_hsm(self, bitfileid) :
		ticket = self.send(ticket)
		return ticket

	def new_bit_file(self, bof_space_cookie, 
				external_label, 
				sanity_cookie,
				complete_crc ) :
		ticket = {"work" : "new_bit_file",
			  "bof_space_cookie" : bof_space_cookie,
			  "external_label" : external_label,
			  "sanity_cookie" : sanity_cookie,
			  "complete_crc" : complete_crc
			 }
		ticket = self.send(ticket)
		return ticket
