###############################################################################
# src/$RCSfile$   $Revision$
#
# Generic server class for enstore

#system imports
import sys
import string

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
import traceback
import timeofday
import e_errors
import interface
import generic_client

class GenericServerInterface(interface.Interface):

    def __init__(self):
	interface.Interface.__init__(self)

    def options(self):
	return self.config_options() + self.help_options()

DOT = '.'

class GenericServer(generic_client.GenericClient):

   def __init__(self, csc, name):
      # do this in order to centralize getting a log, alarm and configuration
      # client. and to record the fact that we only want to do it once.
      generic_client.GenericClient.__init__(self, csc, name)

   # given a server name, return the name mutated into a name appropriate for
   # identification in log and trace. this means, upcase the name and possibly
   # shorten it.  so if the name can be split into 'part1.part2', shorten
   # part1 to only be 8 characters.  also if there is an alternate part2
   # specified in the instance, use it.
   def get_log_name(self, name):
      parts = string.split(name, DOT)
      if len(parts) == 2:
         new_name = "%s.%s"%(string.upper(parts[0][0:8]),
                             string.upper(self.__dict__.get("name_ext",
                                                            parts[1])))
      else:
         new_name = string.upper(name)
      return new_name

   # return the server name
   def get_name(self, name):
      configD = self.csc.get(name)
      if not configD['status'][0] == 'KEYERROR':
         name = configD.get('logname', self.get_log_name(name))
      else:
         name = self.get_log_name(name)
      return name

   # this overrides the server_bind in TCPServer for the hsm system
   def server_bind(self):
      self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
      self.socket.bind(self.server_address)

   # we got an uncaught error while in serve_forever
   def serve_forever_error(self, id):
       exc,msg,tb=sys.exc_info()
       traceback.print_exc()
       format = "%s %s %s %s %s: serve_forever continuing" % (
           timeofday.tod(),sys.argv,exc,msg,id)
       Trace.log(e_errors.ERROR, repr(format))

   # send back our response
   def send_reply(self, t):
      try:
         self.reply_to_caller(t)
      except:
         # even if there is an error - respond to caller so he can process it
         exc,msg,tb=sys.exc_info()
         t["status"] = (str(exc),str(msg))
         self.reply_to_caller(t)
         Trace.trace(7,"send_reply %s"%t)
         return
