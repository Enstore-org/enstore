#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

"""
        The purpose of this module is to provide a clean datagram
        interface for Enstore. By "clean" we mean that we try to
	provide a uniform interface on all platforms by masking specific
        errors.

        Specific errors that are masked:

        1) Linux ipv4 -- returning an error on the next UDP send or recieve
        when an ICMP port unreachable message is recieved. The socket 
        implementation will return, then automatically clear ECONNREFUSED.
        To handle this, we transparently retry self.retry_max times 

        cleanUDP.select() must be used instead of select.select()

"""

# system imports
import socket
import Trace
import errno
import time
import select

# enstore imports
import e_errors

def Select (R, W, X, timeout) :

## we have an error under linux where we get an error, and
## r and x are set, but there is no data. If the error is a spurious error, 
## we must delete the object from all lists.
##
        cleaned_r = []
	t0 = time.time()
	timeout = max(0.0, timeout)
        while 1 :
		r, w, x = select.select(R, W, X, timeout)
		timeout = timeout - (time.time() - t0)
		timeout = max(0.0, timeout)

                if r == cleaned_r :
			#If the timeout specified hasn't run out and
			# we don't have a ready socket keep trying.
			if r == w == x == [] and timeout > 0.0:
				continue
			
                        # all except FD's as the same as not scrubbed
                        # previously.
                        return r, w, x, timeout
                cleaned_r = []
                for obj in r :
                        try:
                            if obj.scrub() :
                                cleaned_r.append(obj)           
                        except: 
                            #Trace.trace( 6, "non clean UDP object" )
                            cleaned_r.append(obj)


class cleanUDP :

        retry_max = 10
        previous_sendto_address="N/A"
        this_sendto_address="N/A"

        def __init__(self, protocol, kind) :
                if kind != socket.SOCK_DGRAM :
                        raise e_errors.CLEANUDP_EXCEPTION
                self.socket = socket.socket(protocol, kind)
                return

	def __del__(self):
	        self.socket.close()
        
        def scrub(self) :
                self.socket.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
                r, w, x = select.select([self], [], [self], 0)
                if r :
                        return 1 # it's clean - there really is a read to do
		# it went away - just the icmp message under A. Cox's
		# linux implementation
                return 0  

        def __getattr__(self, attr):
                return getattr(self.socket, attr)

        # Mitigate case 1 -- ECONNREFUSED from previous sendto
        def recvfrom(self, bufsize, rcv_timeout=10) :
                data = ("", ("", 0))
                for n in range(self.retry_max) :
                        try:
                                r, junk, junk = select.select([self.socket],
							      [], [],
							      rcv_timeout)
                                if r:
                                        data=self.socket.recvfrom(bufsize)
                                        return data
                        except socket.error:
                                self.logerror("recvfrom", n)
                return data

        # Mitigate case 1 -- ECONNREFUSED from previous sendto
        def sendto(self, data, address) : 
                self.previous_sendto_address = self.this_sendto_address
                self.this_sendto_address = address

                for n in range(self.retry_max) :
                        try:
                                return self.socket.sendto(data, address)
                        except socket.error:
                                self.logerror("sendto", n)
                return self.socket.sendto(data, address)
                

        def logerror(self, sendto_or_recvfrom, try_number) :
                badsockerrno = self.socket.getsockopt(
                                socket.SOL_SOCKET,socket.SO_ERROR)
                try :
                        badsocktext = repr(errno.errorcode[badsockerrno])
                except:
                        badsocktext = repr(badsockerrno)
                etext = "cleanUDP %s try %d %s failed on %s last %s" % (
                          sendto_or_recvfrom, try_number,
                          badsocktext, self.this_sendto_address,
                          self.previous_sendto_address)

                Trace.log(e_errors.ERROR, etext )

if __name__ == "__main__" :
        sout = cleanUDP(socket.AF_INET, socket.SOCK_DGRAM)
        sout.bind(('localhost',303030))
        # on linux, should see one retry from the following.

        sout.sendto("all dogs have fleas", ('localhost', 303031))       
        r, w, x = select.select([sout],[sout],[sout], 1.0)
        if not x and not r and w:
                print "expected select.select behavoir on non-linux " \
		      "and post 2.4 linux kernel"
        elif x and r and w:
                print "expected select.select behavior on linux, " \
		      "pre 2.2 kernel"
        elif not x and r and w:
                print "expected select.select behavior on linux, " \
		      "post 2.2 kernel"
        else:
                print "***unexpected  behavior on _any_ platform"
        r, w, x, remaining_time = Select([sout],[sout],[sout], 1.0)

        if not r and not x :
                print "expected behavior"
        else :
                print "***unexpected behavior"

        sout.sendto("all dogs have fleas", ('localhost', 303031))
        sin = cleanUDP(socket.AF_INET, socket.SOCK_DGRAM)
        sin.bind(('localhost',303031))
        sout.sendto("Expected behavior", ('localhost', 303031))
        print sin.recvfrom(1000)
