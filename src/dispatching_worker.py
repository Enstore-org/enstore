###############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
import errno
import time
import os
import sys
# Import SOCKS module if it exists, else standard socket module socket
# This is a python module that works just like the socket module, but uses
# the SOCKS protocol to make connections through a firewall machine.
# See http://www.w3.org/People/Connolly/support/socksForPython.html or
# goto www.python.org and search for "import SOCKS"
try:
    import SOCKS; socket = SOCKS
except ImportError:
    import socket

#enstore imports
import timeofday
import Trace
import e_errors

request_dict = {}
#
# Purge entries older than 600 seconds. Dict is a dictionary
#    The first entry, dict[0], is the key
#    The second entry, dict[1], is the message, client number, ticket, and time
#        which becomes list[0-2]
def purge_stale_entries(request_dict):
    Trace.trace(20,"{purge_stale_entries")
    stale_time = time.time() - 600
    count = 0
    for entry in request_dict.items():
        list = entry[1]
        if  list[2] < stale_time:
            del request_dict[entry[0]]
            count = count+1
    Trace.trace(20,"}purge_stale_entries count=%d",count)

import pdb
def dodebug(a,b):
    pdb.set_trace()

import signal
signal.signal(3,dodebug)

# check for any children that have exitted (zombies) and collect them
def collect_children():
    Trace.trace(20,"{collect_children")
    count = 0
    try:
        pid, status = os.waitpid(0, os.WNOHANG)
        if (pid!=0):
            #print "Child reaped: pid=",pid," status=",status
            count = count+1
            Trace.trace(21,"collect_children reaped pid="+repr(pid))
    except os.error:
        if sys.exc_info()[1][0] != errno.ECHILD:
            Trace.trace(0,"collect_children "+str(sys.exc_info()[0])+\
                        str(sys.exc_info()[1]))
            raise sys.exc_info()[0],sys.exc_info()[1]
    Trace.trace(20,"}collect_children count=%d",count)

# Generic request response server class, for multiple connections
# This method overrides the process_request function in SocketServer.py
# Note that the UDPServer.get_request actually read the data from the socket

class DispatchingWorker:

    # Process the  request that was (generally) sent from UDPClient.send
    def process_request(self, request, client_address):
        # the real info and work is in the ticket - get that
        Trace.trace(5,"{process_request add="+repr(client_address))

	# ref udp_client.py (i.e. we may wish to have a udp_client method
	# to get this information)
        exec ( "idn, number, ticket = " + request)
        self.reply_address = client_address
        self.client_number = number
        self.current_id = idn

        Trace.trace(6,"process_request idn="+repr(idn)+" number"+repr(number)+\
                    " req="+repr(request))

        try:

            # UDPClient resends messages if it doesn't get a response from us
            # see it we've already handled this request earlier. We've
            # handled it if we have a record of it in our dict
            exec ("list = " + repr(request_dict[idn]))
            if list[0] == number:
                Trace.trace(5,"}process_request "+repr(idn)+" already handled")
                self.reply_with_list(list)
                return

            # if the request number is larger, then this request is new
            # and we need to process it
            elif list[0] < number:
                pass # new request, fall through

            # if the request number is smaller, then there has been a timing
            # race and we've already handled this as much as we are going to.
            else:
                Trace.trace(5,"}process_request "+repr(idn)+" old news")
                return #old news, timing race....

        # on the very 1st request, we don't have anything to compare to
        except KeyError:
            pass # first request or request purged by purge_stale_entries, fall through

        # look in the ticket and figure out what work user wants
        try:
            function = ticket["work"]
        except KeyError:
            ticket = {'status' : (e_errors.KEYERROR, \
				  "cannot find requested function")}
            Trace.trace(0,"process_request "+repr(ticket)+repr(function))
            self.reply_to_caller(ticket)
            return

        if len(request_dict) > 200:
            purge_stale_entries(request_dict)

        # call the user function
        Trace.trace(6,"process_request function="+repr(function))
        exec ("self." + function + "(ticket)")

        # check for any zombie children and get rid of them
        collect_children()
        Trace.trace(5,"}process_request idn="+repr(idn))

    def handle_error(self, request, client_address):
	"""OVERRIDING SocketServer.handle_error
	"""
	Trace.trace(0,"{handle_error request="+repr(request)+" add="+\
		    repr(client_address))
	exc, value, tb = sys.exc_type, sys.exc_value, sys.exc_traceback
	print '-'*40
	print 'Exception happened during processing of request from',
	print client_address
	import traceback
	traceback.print_exception(exc, value, tb)
	print '-'*40
	self.reply_to_caller( {'status':(str(sys.exc_info()[0]), \
					    str(sys.exc_info()[1]),'error'), \
			       'request':request, \
			       'exc_type':repr(exc), \
			       'exc_value':repr(value)} )
	Trace.trace(0,"}handle_error "+str(sys.exc_info()[0])+\
		    str(sys.exc_info()[1]))

    # nothing like a heartbeat to let someone know we're alive
    def alive(self,ticket):
        Trace.trace(10,"{alive address="+repr(self.server_address))
        ticket['address'] = self.server_address
        ticket['status'] = (e_errors.OK, None)
        self.reply_to_caller(ticket)
        Trace.trace(10,"}alive")

    # cleanup if we are done with this unique id
    def done_cleanup(self,ticket):
        try:
            Trace.trace(20,"{done_cleanup id="+repr(self.current_id))
            del request_dict[self.current_id]
        except KeyError:
            pass
        Trace.trace(20,"}done_cleanup")

    # reply to sender with her number and ticket (which has status)
    # generally, the requested user function will send its response through
    # this function - this keeps the request numbers straight
    def reply_to_caller(self, ticket):
        Trace.trace(18,"{reply_to_caller number="+repr(self.client_number)+\
                    " id ="+repr(self.current_id))
        reply = (self.client_number, ticket, time.time())
        self.reply_with_list(reply)
        Trace.trace(18,"}reply_to_caller number="+repr(self.client_number))

    # keep a copy of request to check for later udp retries of same
    # request and then send to the user
    def reply_with_list(self, list):
        Trace.trace(19,"{reply_with_list number="+repr(self.client_number)+\
                    " id ="+repr(self.current_id))
        request_dict[self.current_id] = list
        badsock = self.socket.getsockopt(socket.SOL_SOCKET,socket.SO_ERROR)
        if badsock != 0:
            Trace.trace(0,"reply_with_list pre-send error "+\
                        repr(errno.errorcode[badsock]))
            print "dispatching_worker reply_with_list, pre-sendto error:",\
                  errno.errorcode[badsock]
        sent = 0
        while sent == 0:
            try:
                self.socket.sendto(repr(list), self.reply_address)
                sent = 1
            except socket.error:
                Trace.trace(0,"reply_with_list Nameserver not responding "+\
                            "add="+repr(address)+\
                            str(sys.exc_info()[0])+str(sys.exc_info()[1]))
                print timeofday.tod(),\
                      "dispatching_worker: Nameserver not responding\n",\
                      address,"\n", sys.exc_info()[0],"\n", sys.exc_info()[1]
                time.sleep(10)
        badsock = self.socket.getsockopt(socket.SOL_SOCKET,socket.SO_ERROR)
        if badsock != 0:
            Trace.trace(0,"reply_with_list post-send error "+\
                        repr(errno.errorcode[badsock]))
            print "dispatching_worker reply_with_list, post-sendto error:",\
                  errno.errorcode[badsock]
        Trace.trace(19,"}reply_with_list number="+repr(self.client_number))
