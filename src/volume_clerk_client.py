import sys
import os
from configuration_client import *
from udp_client import UDPClient

class VolumeClerkClient :

	def __init__(self, configuration_client) :
		self.csc = configuration_client
		self.u = UDPClient()

	def send (self, ticket) :
		vticket = self.csc.get("volume_clerk")
		ticket = self.u.send(
			ticket,
			(vticket['host'], vticket['port'])
			)
		return ticket

	def addvol(self,
			library,		# name of library media is in
			file_family,		# volume family the media is in
			media_type,	        # media  
			external_label,         # label as known to the system
			capacity_bytes,	        #
			remaining_bytes,        # 
			eod_cookie  = "none",   # code for seeking to eod
			user_inhibit  = "none", # "none" | "readonly" | "all"
			error_inhibit = "none"  # "none" | "readonly" | "all" |
						#   "writing"
					        # lesser access is specified as
					        # we find media errors, writing
						# means that a mover is
						#appending or that a mover 
						# crashed while writing
			) :
		ticket = {
			'work' : 'addvol', 
			'library' : library, 
			'file_family' : file_family, 
			'media_type' : media_type, 
			'capacity_bytes' : capacity_bytes, 
			'remaining_bytes' : remaining_bytes, 
			'eod_cookie' : eod_cookie, 
			'external_label'    : external_label,
			'user_inhibit' : user_inhibit,
			'error_inhibit'  : error_inhibit
			}
		ticket = self.send(ticket)
		return ticket


	def delvol(self, external_label) :
		ticket= {
			'work' : 'delvol', 
			'external_label'    : external_label,
			}
		ticket = self.send(ticket)
		return ticket

	def inquire_vol(self, external_label) :
		ticket= {
			'work' : 'inquire_vol', 
			'external_label'    : external_label,
			}
		ticket = self.send(ticket)
		return ticket

	def set_writing(self, external_label) :
		ticket= {
			'work' : 'set_writing', 
			'external_label'    : external_label
			}
		ticket = self.send(ticket)
		return ticket

	def set_remaining_bytes(self, external_label, 
			remaining_bytes, eod_cookie) :
		ticket= {
			'work' : 'set_remaining_bytes', 
			'external_label'    : external_label,
			'remaining_bytes'    : remaining_bytes,
			'eod_cookie'    : eod_cookie
			}
		ticket = self.send(ticket)
		return ticket

	def next_write_volume (self, library, min_remaining_bytes, 
				    file_family, vol_veto_list) :
		ticket = {
			  'work' : 'next_write_volume',
			  'library' : library,
			  'min_remaining_bytes' : min_remaining_bytes,
			  'file_family' : file_family,
			  'vol_veto_list': `vol_veto_list`
                          }
		ticket = self.send(ticket)
		return ticket
