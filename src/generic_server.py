###############################################################################
# src/$RCSfile$   $Revision$
#
# Generic server class for enstore

#system imports
import sys

# Import SOCKS module if it exists, else standard socket module socket
# This is a python module that works just like the socket module, but uses
# the SOCKS protocol to make connections through a firewall machine.
# See http://www.w3.org/People/Connolly/support/socksForPython.html or
# goto www.python.org and search for "import SOCKS"
try:
    import SOCKS
    socket = SOCKS
except ImportError:
    import socket

# enstore imports
import Trace
import generic_cs
import traceback
import timeofday
import log_client
import e_errors
import interface

class GenericServerInterface(interface.Interface):

    def __init__(self):
	self.verbose = 0
	interface.Interface.__init__(self)

    def options(self):
        Trace.trace(16, "{}options")
	return self.config_options() + ["verbose="]+\
	       self.help_options()


class GenericServer(generic_cs.GenericCS):

    # this overrides the server_bind in TCPServer for the hsm system
    def server_bind(self):
        Trace.trace(10,'{server_bind')
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(self.server_address)
        Trace.trace(10,'}server_bind')

    # we got an uncaught error while in serve_forever
    def serve_forever_error(self, id, logger=0):
        traceback.print_exc()
        format = timeofday.tod()+" "+str(sys.argv)+" "+\
                 str(sys.exc_info()[0])+" "+str(sys.exc_info()[1])+" "+\
                 id+" serve_forever continuing"
        self.enprint(format)
	if logger:
            logger.send(e_errors.ERROR, 1, format)

    # reset the verbosity
    def set_verbose(self, ticket):
        Trace.trace(10,'{set_verbose')
        ticket["status"] = (e_errors.OK, None)
	id = "verbose"
	if self.__dict__.has_key(id):
	    # set both just in case
	    self.verbose = ticket[id]
	    verbose = ticket[id]
            if 0: print verbose #quiet lint
	    ticket["variable"] = "self.verbose, verbose"
	else:
	    verbose = ticket[id]
	    ticket["variable"] = "verbose"
	self.send_reply(ticket)
        Trace.trace(10,'}set_verbose')
	
    # send back our response
    def send_reply(self, t):
	Trace.trace(11,"{send_reply "+repr(t))
	self.enprint(t, generic_cs.SERVER, self.verbose)
        try:
           self.reply_to_caller(t)
        # even if there is an error - respond to caller so he can process it
        except:
           t["status"] = (str(sys.exc_info()[0]), str(sys.exc_info()[1]))
           self.reply_to_caller(t)
           Trace.trace(0,"}send_reply "+repr(t))
           return
	Trace.trace(11,"}send_reply")
