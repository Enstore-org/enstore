import errno
import time
import timeofday
import os
import sys
from SocketServer import UDPServer, TCPServer

# Import SOCKS module if it exists, else standard socket module socket
# This is a python module that works just like the socket module, but uses
# the SOCKS protocol to make connections through a firewall machine.
# See http://www.w3.org/People/Connolly/support/socksForPython.html or
# goto www.python.org and search for "import SOCKS"
try:
    import SOCKS; socket = SOCKS
except ImportError:
    import socket

dict = {}
#
# Purge entries older than 600 seconds. Dict is a dictionary
#    The first entry, dict[0], is the key
#    The second entry, dict[1], is the message, client number, ticket, and time
#        which becomes list[0-2]
def purge_stale_entries(dict) :
     stale_time = time.time() - 600
     for entry in dict.items() :
         list = entry[1]
         if  list[2] < stale_time :
             del dict[entry[0]]

import pdb
def dodebug(a,b):
    pdb.set_trace()

import signal
signal.signal(3,dodebug)

# check for any children that have exitted (zombies) and collect them
def collect_children() :
    try :
	pid, status = os.waitpid(0, os.WNOHANG)
	if (pid!=0) :
	    #print "Child reaped: pid=",pid," status=",status
	    pass
    except os.error:
	if sys.exc_info()[1][0] != errno.ECHILD :
	    raise sys.exc_info()[0],sys.exc_info()[1]

# Generic request response server class, for multiple connections
# This method overrides the process_request function in SocketServer.py
# Note that the UDPServer.get_request actually read the data from the socket

class DispatchingWorker:

    # Process the  request that was (generally) sent from UDPClient.send
    def process_request(self, request, client_address) :

        # the real info and work is in the ticket - get that
        exec ( "idn, number, ticket = " + request)
        self.reply_address = client_address
        self.client_number = number
        self.current_id = idn

        try :

            # UDPClient resends messages if it doesn't get a response from us
            # see it we've already handled this request earlier. We've
            # handled it if we have a record of it in our dict
            exec ("list = " + repr(dict[idn]))
            if list[0] == number :
                self.reply_with_list(list)
                return

            # if the request number is larger, then this request is new
            # and we need to process it
            elif list[0] < number :
                pass # new request, fall through

            # if the request number is smaller, then there has been a timing
            # race and we've already handled this as much as we are going to.
            else:
                return #old news, timing race....

        # on the very 1st request, we don't have anything to compare to
        except KeyError:
            pass # first request or request purged by purge_stale_entries, fall through

        # look in the ticket and figure out what work user wants
        try :
            function = ticket["work"]
        except KeyError:
            ticket = {'status' : "cannot find requested function"}
            self.reply_to_caller(ticket)
            return

        if len(dict) > 200:
             purge_stale_entries(dict)

        # call the user function
        exec ("self." + function + "(ticket)")

	# check for any zombie children and get rid of them
	collect_children()

    # nothing like a heartbeat to let someone know we're alive
    def alive(self,ticket):
	ticket['address'] = self.server_address
        ticket['status'] = "ok"
        self.reply_to_caller(ticket)

    # reply to sender with her number and ticket (which has status)
    # generally, the requested user function will send its response through
    # this function - this keeps the request numbers straight
    def reply_to_caller(self, ticket) :
        reply = (self.client_number, ticket, time.time())
        self.reply_with_list(reply)

    # keep a copy of request to check for later udp retries of same
    # request and then send to the user
    def reply_with_list(self, list) :
        dict[self.current_id] = list
        badsock = self.socket.getsockopt(socket.SOL_SOCKET,socket.SO_ERROR)
        if badsock != 0 :
            print "dispatching_worker reply_with_list, pre-sendto error:",\
                  errno.errorcode[badsock]
        sent = 0
        while sent == 0:
            try:
                self.socket.sendto(repr(list), self.reply_address)
                sent = 1
            except socket.error:
                print timeofday.tod(),\
                      "dispatching_worker: Nameserver not responding\n",\
                      message,"\n",address,"\n",\
                      sys.exc_info()[0],"\n", sys.exc_info()[1]
                time.sleep(10)
        badsock = self.socket.getsockopt(socket.SOL_SOCKET,socket.SO_ERROR)
        if badsock != 0 :
            print "dispatching_worker reply_with_list, post-sendto error:",\
                  errno.errorcode[badsock]

