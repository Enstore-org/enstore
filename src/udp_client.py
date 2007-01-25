#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################

# system imports
import socket
import time
import os
import errno
import exceptions
import sys
try:
    import threading
    import thread
    thread_support=1
except ImportError:
    thread_support=0

import rexec
import types

# enstore imports
import Trace
#import setpath
import e_errors
import checksum
import cleanUDP
import udp_common
#import hostaddr
import host_config

#UDPError = "UDP Error"
class UDPError(socket.error):
    def __init__(self, e_errno, e_message = None):

        socket.error.__init__(self)

        #If the only a message is present, it is in the e_errno spot.
        if e_message == None:
            if type(e_errno) == types.IntType:
                self.errno = e_errno
                self.message = None
            elif type(e_errno) == types.StringType:
                self.errno = None
                self.message = e_errno
            else:
                self.errno = None
                self.message = "Unknown error"
        #If both are there then we have both to use.
        else:
            self.errno = e_errno
            self.message = e_message

        #Generate the string that stringifying this obeject will give.
        self._string()

    def __str__(self):
        self._string()
        return self.strerror

    def __repr__(self):
        return "UDPError"

    def _string(self):
        if self.errno in errno.errorcode.keys():
            errno_name = errno.errorcode[self.errno]
            errno_description = os.strerror(self.errno)
            self.strerror = "%s: [ ERRNO %s ] %s: %s" % (errno_name,
                                                        self.errno,
                                                        errno_description,
                                                        self.message)
        else:
            self.strerror = self.message

        return self.strerror


TRANSFER_MAX=16384

def wait_rsp( sock, address, rcv_timeout ):

    reply,server=None,None

    r, w, x, rcv_timeout = cleanUDP.Select( [sock], [], [sock], rcv_timeout)
    if r:
        reply , server = sock.recvfrom( TRANSFER_MAX, rcv_timeout)
    elif x or w :
        exc, msg = sys.exc_info()[:2]
        Trace.log(e_errors.INFO,
              "UDPClient.send: exception on select after send to %s %s: %s %s"%
                  (address, x, exc, msg))
        raise UDPError, "impossible to get these set w/out [r]"
    return reply, server, rcv_timeout

class Container:
    pass

_rexec = rexec.RExec()
def r_eval(stuff):
    return _rexec.r_eval(stuff)

class UDPClient:

    def __init__(self):
        self.tsd = {} #Thread-specific data
        self._os = os

        self.reinit()

    def reinit(self):
        #Obtain necessary values.
        pid = self._os.getpid()
        host, port, socket = udp_common.get_default_callback()
        if thread_support:
            tid = thread.get_ident() #Obtain unique identifier.
        else:
            tid = 1
        #Build thread specific data.
        tsd = Container()
        tsd.host = host
        tsd.port = port
        tsd.socket = socket
        tsd.txn_counter = 0L
        tsd.reply_queue = {}
        tsd.ident = self._mkident(host, port, pid)
        tsd.send_done = {}
        tsd.tid = tid
        if thread_support:
            #There is no good way to store which thread this tsd was
            # create for.  It used to do the following.
            #     tsd.thread = threading.currentThread()
            # But this turns out to be a resource leak by creating a
            # cyclic reference.  Thus, this hack was devised to track
            # them from the other direction; namely knowing the thread
            # identify the tsd in the self.tsd dict that it relates to.
            threading.currentThread().tid = tid
            
        #Cache the tsd and return.
        self.tsd[tid] = tsd
        return tsd

    def cleanup_tsd(self):
        if thread_support:
            for tid, tsd in self.tsd.items():
                #Clean up resources of exited threads.
                try:
                    #Loop though all of the active threads searching for
                    # the thread specific data (tsd) that it relates to.
                    for a_thread in threading.enumerate():
                        if not hasattr(a_thread, "tid"):
                            #If there is no tid attribute, it hasn't used
                            # this udp_client and thus we don't care.
                            continue
                        if a_thread.tid == tid:
                            #If the thread is still active, don't cleanup.
                            break
                    else:
                        #After testing all the active threads this thread
                        # was found to be gone.
                        for server in self.tsd[tid].send_done.keys() :
                            try:
                                self.send_no_wait({"work":"done_cleanup"},
                                                  server)
                            except:
                                pass

                        #Cleanup system resources now.
                        self.tsd[tid].socket.close()
                        del self.tsd[tid]
                except KeyError, msg:
                    pass # another thread could have done the cleanup...
                except (KeyboardInterrupt, SystemExit):
                    raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]
                except:
                    exc, msg = sys.exc_info()[:2]
                    try:
                        sys.stderr.write("%s: %s\n" % (str(exc), str(msg)))
                        sys.stderr.flush()
                    except IOError:
                        pass
                    pass
             

    def get_tsd(self):
        if thread_support:
            tid = thread.get_ident() #Obtain unique identifier.
        else:
            tid = 1
        tsd = self.tsd.get(tid)
        if not tsd:
            #Cleanup unused sockets and TSDs.
            self.cleanup_tsd()
            #Get the new socket and TSD.
            tsd = self.reinit()
        return tsd
    
    def _mkident(self, host, port, pid):
        return "%s-%d-%f-%d" % (host, port, time.time(), pid )
        
    def __del__(self):
        # tell server we're done - this allows it to delete our unique id in
        # its dictionary - this keeps things cleaner & stops memory from
        # growing
        
        try:
            #Cleanup all of the other thread specific data related to threads
            # that should no longer exist.
            self.cleanup_tsd()

            #Since, this UDPClient object should go away (in the last
            # surviving thread) we also need to cleanup this tsd too, even
            # though the thread is still alive.
            #Note: Don't use get_tsd(), to return the tsd for this thread.
            #      If this thread hasn't used this UDPClient object before
            #      then there is nothing to clean up.  get_tsd() will
            #      unecessarily create a UDPClient object to destroy.
            if thread_support:
                tid = thread.get_ident() #Obtain unique identifier.
            else:
                tid = 1
            tsd = self.tsd.get(tid)
            if tsd:
                #Send any final messages for this threads socket.
                for server in tsd.send_done.keys():
                    try:
                        self.send_no_wait({"work" : "done_cleanup"}, server)
                    except:
                        pass

                #Cleanup system resources now.
                self.tsd[tid].socket.close()
                del self.tsd[tid]
        except Exception, msg:
            exc, msg = sys.exc_info()[:2]
            print exc, msg
            pass
        
    def _eval_reply(self, reply): #private to send
        try:
            number, out, t = r_eval(reply) #XXX
            # catch any error and keep going. server needs to be robust
        except:
            exc, msg = sys.exc_info()[:2]
            logmsg="udpClient.eval_reply %s %s"%(exc, msg)
            if exc == exceptions.SyntaxError: #msg size> max datagram size?
                logmsg=logmsg+"Truncated message?"
            elif exc == exceptions.TypeError:
                logmsg = logmsg + ": " + reply
            Trace.log(e_errors.ERROR, logmsg)
            raise sys.exc_info()
        return number, out, t


    def protocolize( self, text ):
        tsd = self.get_tsd()

        tsd.txn_counter = tsd.txn_counter + 1

        # CRC text
        body = `(tsd.ident, tsd.txn_counter, text)`
        crc = checksum.adler32(0L, body, len(body))

        # stringify message and check if it is too long
        message = `(body, crc)`

        if len(message) > TRANSFER_MAX:
            errmsg="send:message too big, size=%d, max=%d" %(len(message),TRANSFER_MAX)
            Trace.log(e_errors.ERROR, errmsg)
            raise errno.errorcode[errno.EMSGSIZE],errmsg
            #raise UDPError(errno.EMSGSIZE, errmsg)

        return message, tsd.txn_counter

        
    def send(self, data, dst, rcv_timeout=0, max_send=0,send_done=1):
        """send msg to dst address, up to `max_send` times, each time
        waiting `rcv_timeout' seconds for reply
        A value of 0 for max_send means retry forever"""

        tsd = self.get_tsd()
            
        if rcv_timeout:
            if max_send==0:
                max_send = 1 # protect from nonsense inputs XXX should we do this?
        else:
            rcv_timeout = 10   

        msg, txn_id = self.protocolize(data)
        # keep track of whom we need to send a "done_cleanup" to
        tsd.send_done[dst] = 1

        #If the ip to send from is localhost there is something wrong.
        if tsd.host == "127.0.0.1":
            return {'status':(e_errors.NET_ERROR,
                    "Default ip address is localhost.")}

        #set up the static route before sending.
	# outgoing interface_ip is tsg.host and destination is dst[0].
        if not host_config.is_route_in_table(dst[0]):
            host_config.setup_interface(dst[0], tsd.host)

        n_sent = 0
        while max_send==0 or n_sent<max_send:
            tsd.socket.sendto( msg, dst )
            n_sent=n_sent+1
            rcvd_txn_id=None
            timeout=rcv_timeout
            while rcvd_txn_id != txn_id: #look for reply while rejecting "stale" responses
                reply, server, timeout = wait_rsp( tsd.socket, dst, timeout)
                if not reply: # receive timed out
                    break #resend
                try:
                    rcvd_txn_id, out, t = self._eval_reply(reply)
                    if type(out) == type({}) and out.has_key('status') \
                       and out['status'][0] == e_errors.MALFORMED:
                        return out
                except TypeError:
                    #If a this error occurs, keep retrying.  Most likely it is
                    # an "expected string without null bytes".
                    exc, msg = sys.exc_info()[:2]
                    try:
                        message = "%s: %s: From server %s:%s" % \
                                  (exc, msg, server, reply[:100])
                    except IndexError:
                        message = "%s: %s: From server %s: %s" % \
                                  (exc, msg, server, reply)

                    Trace.log(e_errors.INFO, message)
                    rcvd_txn_id=None
            else: # we got a good reply
                ##Trace.log(e_errors.INFO,"done cleanup %s"%(dst,))
                if send_done:
                    self.send_no_wait({"work":"done_cleanup"}, dst)
                try:
                    del tsd.send_done[dst]
                except KeyError:
                    #If the send_done entry for this key is already gone,
                    # is this an error?  How does it get empty in the
                    # first place?
                    pass
                return out
	    
        #If we got here, it's because we didn't receive a response to the
	# message we sent.
        raise errno.errorcode[errno.ETIMEDOUT]
        #raise UDPError(errno.ETIMEDOUT)
        
    # send message without waiting for reply and resend
    def send_no_wait(self, data, address) :
        tsd = self.get_tsd()
        message, txn_id = self.protocolize( data )
        
        #set up the static route before sending.
	# outgoing interface_ip is tsg.host and destination is address[0].
        if not host_config.is_route_in_table(address[0]):
            host_config.setup_interface(address[0], tsd.host)

        reply = tsd.socket.sendto( message, address )
	
	return reply

    # send message, return an ID that can be used in the recv_deferred function
    def send_deferred(self, data, address) :
        tsd = self.get_tsd()
        tsd.send_done[address] = 1
        message, txn_id = self.protocolize( data )
        
        #set up the static route before sending.
	# outgoing interface_ip is tsg.host and destination is address[0].
        if not host_config.is_route_in_table(address[0]):
            host_config.setup_interface(address[0], tsd.host)
        
        bytes_sent = tsd.socket.sendto( message, address )
	
        if bytes_sent < 0:
            return -1
        return txn_id

    # Recieve a reply, timeout has the same meaning as in select
    def recv_deferred(self, txn_id, timeout):
        tsd = self.get_tsd()
        if tsd.reply_queue.has_key(txn_id):
            reply = tsd.reply_queue[txn_id]
            del  tsd.reply_queue[txn_id]
            return reply
        else:
            rcvd_txn_id=None
            while rcvd_txn_id != txn_id: #look for reply
                reply = None
                r, w, x, timeout = cleanUDP.Select( [tsd.socket], [], [], timeout)
                if r:
                    reply, server = tsd.socket.recvfrom(TRANSFER_MAX, timeout)
                if not reply: # receive or select timed out
                    break
                try:
                    rcvd_txn_id, out, t = self._eval_reply(reply)
                    if type(out) == type({}) and out.has_key('status') \
                       and out['status'][0] == e_errors.MALFORMED:
                        return out
                except TypeError:
                    #If a this error occurs, keep retrying.  Most likely it is
                    # an "expected string without null bytes".
                    exc, msg = sys.exc_info()[:2]
                    try:
                        message = "%s: %s: From server %s:%s" % \
                                  (exc, msg, server, reply[:100])
                    except IndexError:
                        message = "%s: %s: From server %s: %s" % \
                                  (exc, msg, server, reply)
                    Trace.log(e_errors.INFO, message)

                    #Set this to none.  Since it is invalid, don't add it
                    # to the queue and instead skip the following if and
                    # go right back to the top of the loop.
                    rcvd_txn_id=None
                    continue
                
                if rcvd_txn_id != txn_id: #Queue it up, somebody else wants it
                    tsd.reply_queue[rcvd_txn_id] = out
            else: # we got a good reply
                return out

        ##If we got here, it's because we didn't receive a response to the
        ## message we sent.
        raise errno.errorcode[errno.ETIMEDOUT]
        #raise UDPError(errno.ETIMEDOUT)
        
    
        
if __name__ == "__main__" :

    #This test can be run in a number of ways.  The usage of this test
    # will look like:
    #   python $ENSTORE_DIR/SRC/udp_client.py
    #   python $ENSTORE_DIR/SRC/udp_client.py deferred
    #   python $ENSTORE_DIR/SRC/udp_client.py nowait
    #
    #Before running this test, start a udp_server:
    #   python $ENSTORE_DIR/SRC/udp_server.py
    #
    #A sample session should look like this:
    # UDPServer:
    #   $ python udp_server.py
    # <Note: Nothing happens until the message arives.>
    #   Message {'message': 'TEST MESSAGE'}
    #   finished
    #
    # UDPClient:
    #   $ python udp_client.py
    #   Sending message {'message': 'TEST MESSAGE'} to ('localhost', 7700)
    #   using callback ('131.225.84.1', 60853).
    #   Recieved message {'message': 'TEST MESSAGE'}.

    status = 0

    # get a UDP client
    u = UDPClient()
    tsd = u.get_tsd()
    msg = {'message' : "TEST MESSAGE"}
    address = ("localhost", 7700)
    
    print "Sending message %s to %s using callback %s." \
          % (msg, address, (tsd.host, tsd.port))

    try:
        if "deferred" in  sys.argv:
            txn_id = u.send_deferred(msg, address)
            print "Sleeping for 5 sec."
            time.sleep(5)
            back = u.recv_deferred(txn_id, 5)
            print "Recieved message %s." % (back)

        elif "nowait" in sys.argv:
            back = u.send_no_wait(msg, address)
            print "Sent message."

        else:
            back = u.send(msg, address, rcv_timeout = 10)
            print "Recieved message %s." % (back)

    except:
        exc, msg = sys.exc_info()[:2]
        print "Unable to complete test: %s: %s" % (str(exc), str(msg))
        status = 1

    del u

    sys.exit(status)
    
