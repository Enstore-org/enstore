###############################################################################
# src/$RCSfile$   $Revision$
#
# Generic server class for enstore

#system imports

# Import SOCKS module if it exists, else standard socket module socket
# This is a python module that works just like the socket module, but uses
# the SOCKS protocol to make connections through a firewall machine.
# See http://www.w3.org/People/Connolly/support/socksForPython.html or
# goto www.python.org and search for "import SOCKS"
try:
    import SOCKS; socket = SOCKS
except ImportError:
    import socket

# enstore imports
import Trace
import generic_cs
import traceback

class GenericServer(generic_cs.GenericCS):

    # we must call the init of our inherited class
    def __init__(self):
	generic_cs.GenericCS.__init__(self)

    # this overrides the server_bind in TCPServer for the hsm system
    def server_bind(self):
        Trace.trace(10,'{server_bind')
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(self.server_address)
        Trace.trace(10,'}server_bind')

    # we got an uncaught error while in serve_forever
    def serve_forever_error(self, str, logger=0):
        traceback.print_exc()
        format = timeofday.tod()+" "+str(sys.argv)+" "+\
                 str(sys.exc_info()[0])+" "+str(sys.exc_info()[1])+" "+\
                 str+" serve_forever continuing"
        self.enprint(format)
	if logger:
            logger.send(log_client.ERROR, 1, format)
