#
# Generic server class for the hsm
#
import socket
import dict_to_a

class GenericServer:

	def set_csc(self, configuration_server_client) :
		self.csc = configuration_server_client

	def server_bind(self) :
		self.request_queue_size = 20
		self.socket.setsockopt(socket.SOL_SOCKET, 
				       socket.SO_REUSEADDR, 1)
		self.socket.bind(self.server_address)
