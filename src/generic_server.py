# Generic server class for enstore

import socket

class GenericServer:

    # we need to know where the configuration server is - keep track of it
    def set_csc(self, configuration_client) :
        self.csc = configuration_client


    # this overrides the server_bind in TCPServer for the hsm system
    def server_bind(self) :
        self.request_queue_size = 20
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(self.server_address)
