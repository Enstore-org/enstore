# Generic server class for enstore

# Import SOCKS module if it exists, else standard socket module socket
# This is a python module that works just like the socket module, but uses
# the SOCKS protocol to make connections through a firewall machine.
# See http://www.w3.org/People/Connolly/support/socksForPython.html or
# goto www.python.org and search for "import SOCKS"
try:
    import SOCKS; socket = SOCKS
except ImportError:
    import socket

class GenericServer:

    # we need to know where the configuration server is - keep track of it
    def set_csc(self, configuration_client) :
        self.csc = configuration_client

    # we need to know where the logger is - keep track of it
    def set_logc(self, log_client) :
        self.logc = log_client

    # this overrides the server_bind in TCPServer for the hsm system
    def server_bind(self) :
        self.request_queue_size = 20
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(self.server_address)
