#!/usr/bin/python

import sys
import posixfile
from SocketServer import *
from dict_to_a import *
from dispatching_worker import DispatchingWorker
from generic_server import GenericServer

#
# Put temporary associate array stuff here
# later implemention should look to file system
# maybe do away with this server in a wormhole
#
dict = {}
dict["file_clerk"]  = {'host' : 'localhost', 
				'port' : 7501, 'status' : 'ok'}
dict["volume_clerk"] = {'host' : 'localhost', 'port' : 7502, 'status' : 'ok'}
dict["activelibrary.library_manager"] = {'host' : 'localhost', 'port' : 7503, 
		'status' : 'ok'}
dict["shelf.library_manager"] = {'host' : 'localhost', 'port' : 7504, 
		'status' : 'ok'}
dict["fd0.mover"] = {  'status'  : 'ok', 
		       'device' : './file.fake', 
		       'library' : 'activelibrary',
			'driver' : 'RawDiskDriver',
			'library_device' : '1'}

class ConfigurationDict(DispatchingWorker) :
	
	def lookup(self, ticket) :

		out_ticket = {"status" : "nosuchname"}
		try :
			out_ticket = dict[ticket["lookup"]]
		except KeyError:
			pass    # send the previously set up error
		self.reply_to_caller(out_ticket)

class ConfigurationServer(ConfigurationDict, GenericServer, 
			UDPServer) : pass 
	


if __name__ == "__main__" :
	cd =  ConfigurationDict()
	cs =  ConfigurationServer( ("localhost", 7500), cd)
	cs.serve_forever()











