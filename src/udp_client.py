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
import sys
try:
    import threading
    import thread
    thread_support=1
except ImportError:
    thread_support=0
import types

# enstore imports
import Trace
import e_errors
import checksum
import cleanUDP
import udp_common
import host_config


MAX_EXPONENT=6 # do not increase reeive TO in send beyond this

"""
#UDPError = "UDP Error"
class UDPError(socket.error):

    def __init__(self, e_errno, e_message = None):

        socket.error.__init__(self)

        #If the only a message is present, it is in the e_errno spot.
        if e_message == None:
            if type(e_errno) == types.IntType:
                self.errno = e_errno
                self.e_message = None
            elif type(e_errno) == types.StringType:
                self.errno = None
                self.e_message = e_errno
            else:
                self.errno = None
                self.e_message = "Unknown error"
        #If both are there then we have both to use.
        else:
            self.errno = e_errno
            self.e_message = e_message

        #Generate the string that stringifying this obeject will give.
        self.strerror = "" #Define this to make pychecker happy.
        self._string()

        self.args = (self.errno, self.e_message)

    def __str__(self):
        self._string()
        return self.strerror

    def __repr__(self):
        return "UDPError"  #String value.

    def _string(self):
        if self.errno in errno.errorcode.keys():
            errno_name = errno.errorcode[self.errno]
            errno_description = os.strerror(self.errno)
            self.strerror = "%s: [ ERRNO %s ] %s: %s" % (errno_name,
                                                        self.errno,
                                                        errno_description,
                                                        self.e_message)
        else:
            self.strerror = self.e_message

        return self.strerror
"""

TRANSFER_MAX=16384

def wait_rsp( sock, address, rcv_timeout ):

    reply,server=None,None

    r, w, x, rcv_timeout = cleanUDP.Select( [sock], [], [sock], rcv_timeout )
    if r:
        reply , server = sock.recvfrom( TRANSFER_MAX, rcv_timeout )
    elif x or w :
        exc, msg = sys.exc_info()[:2]
        Trace.log(e_errors.INFO,
              "UDPClient.send: exception on select after send to %s %s: %s %s"%
                  (address, x, exc, msg))
        raise e_errors.EnstoreError(None,
                                    "impossible to get these set w/out [r]",
                                    e_errors.NET_ERROR)
    
    return reply, server, rcv_timeout

class Container:
    pass

"""
_rexec = rexec.RExec()
def r_eval(stuff):
    return _rexec.r_eval(stuff)
"""

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
        tsd.send_queue = {}  #For deferred messages.
        tsd.reply_queue = {}
        tsd.tid = tid
        tsd.pid = pid
        tsd.ident = self._mkident(tsd.host, tsd.port, tsd.pid, tsd.tid)
        tsd.send_done = {}

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

    def get_address(self):
        if thread_support:
            tid = thread.get_ident() #Obtain unique identifier.
        else:
            tid = 1
        return self.tsd[tid].socket.getsockname()

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
                        '''
                        for server in self.tsd[tid].send_done.keys() :
                            try:
                                self.send_no_wait({"work":"done_cleanup"},
                                                  server)
                            except:
                                pass
                        '''

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
    
    def _mkident(self, host, port, pid, tid):
        return "%s-%d-%f-%d-%d" % (host, port, time.time(), pid, abs(tid))
        
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
                '''
                #Send any final messages for this threads socket.
                for server in tsd.send_done.keys():
                    try:
                        self.send_no_wait({"work" : "done_cleanup"}, server)
                    except:
                        pass
                '''

                #Cleanup system resources now.
                self.tsd[tid].socket.close()
                del self.tsd[tid]
        except Exception, msg:
            exc, msg = sys.exc_info()[:2]
            print exc, msg
            pass

    """
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
    """


    def protocolize( self, text ):
        tsd = self.get_tsd()

        tsd.txn_counter = tsd.txn_counter + 1

        # CRC text
        #body = `(tsd.ident, tsd.txn_counter, text)`
        #ident = self._mkident(tsd.host, tsd.port, tsd.pid, tsd.tid)
        #body = udp_common.r_repr((ident, tsd.txn_counter, text))
        body = udp_common.r_repr((tsd.ident, tsd.txn_counter, text))
        crc = checksum.adler32(0L, body, len(body))

        # stringify message and check if it is too long
        #message = `(body, crc)`
        message = udp_common.r_repr((body, crc))

        if len(message) > TRANSFER_MAX:
            errmsg="send:message too big, size=%d, max=%d" %(len(message),TRANSFER_MAX)
            Trace.log(e_errors.ERROR, errmsg)
            #raise errno.errorcode[errno.EMSGSIZE],errmsg
            raise e_errors.EnstoreError(errno.EMSGSIZE, errmsg,
                                        e_errors.NET_ERROR)

        return message, tsd.txn_counter

        
    def send(self, data, dst, rcv_timeout=0, max_send=0, send_done=1):
        """send msg to dst address, up to `max_send` times, each time
        waiting `rcv_timeout' seconds for reply
        A value of 0 for max_send means retry forever"""

        tsd = self.get_tsd()
            
        if rcv_timeout:
            if max_send==0:
                max_send = 1 # protect from nonsense inputs XXX should we do this?
            # if rcv_timeout is specified
            # do not grow tiemout exponentially
            max_exponent = 0
        else:
            rcv_timeout = 10
            # if rcv_timeout is not specified or is 0 (try forever)
            # grow tiemout exponentially
            max_exponent = MAX_EXPONENT
             

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
        exp = 0
        timeout=rcv_timeout
        while max_send==0 or n_sent<max_send:
            #print "SENDING", time.time(), msg, dst
            tsd.socket.sendto( msg, dst )
            timeout = timeout*(pow(2,exp))
            if exp < max_exponent:
                exp = exp + 1
            n_sent=n_sent+1
            rcvd_txn_id=None

            while rcvd_txn_id != txn_id: #look for reply while rejecting "stale" responses
                reply, server_addr, timeout_1 = \
                       wait_rsp( tsd.socket, dst, timeout)
                
                if not reply: # receive timed out
                    #print "TIMEOUT", time.time(), msg
                    break #resend
                #print "GOT REPLY",reply 
                try:
                    rcvd_txn_id, out, t = udp_common.r_eval(reply)
                    if type(out) == type({}) and out.has_key('status') \
                       and out['status'][0] == e_errors.MALFORMED:
                        return out
                except (SyntaxError, TypeError):
                    #If TypeError occurs, keep retrying.  Most likely it is
                    # an "expected string without null bytes".
                    #If SyntaxError occurs, also keep trying, most likely
                    # it is from and empty UDP datagram.
                    exc, msg = sys.exc_info()[:2]
                    try:
                        message = "%s: %s: From server %s:%s" % \
                                  (exc, msg, server_addr, reply[:100])
                    except IndexError:
                        message = "%s: %s: From server %s: %s" % \
                                  (exc, msg, server_addr, reply)
                    Trace.log(10, message)

                    #Set this to something.
                    rcvd_txn_id=None
            else: # we got a good reply
                ##Trace.log(e_errors.INFO,"done cleanup %s"%(dst,))
                '''
                if send_done:
                    self.send_no_wait({"work":"done_cleanup"}, dst)
                '''
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
        try:
            del tsd.send_done[dst]
        except KeyError:
            #If the send_done entry for this key is already gone,
            # is this an error?  How does it get empty in the
            # first place?
            pass
        
        #raise errno.errorcode[errno.ETIMEDOUT]
        raise e_errors.EnstoreError(errno.ETIMEDOUT, "", e_errors.TIMEDOUT)
        
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

    ### send_deferred()
    ### repeat_deferred()
    ### recv_deferred()
    ### recv_deferred_with_repeat_send()
    ### drop_deferred()
    ###
    ### The *_deferred() functions allow for asymetric processing of messages.
    ###
    ### There are two recv_deferred() functions.  recv_deferred() does
    ### what it has always been done; only wait for the perscribed time
    ### without any automatic resending of messages where a response has not
    ### returned after too long of a time period.  This means that the caller
    ### of the recv_deferred() function needs to handle their own retrying
    ### with geometric timeout backoff.
    ### 
    ### The other recv_deffered function, recv_deferred_with_repeat_send(),
    ### performs automatic resending/retrying of messages.  This function
    ### does gemotric backoff of retransmitted messages.

    # send message, return an ID that can be used in the recv_deferred function
    def send_deferred(self, data, address):
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

        #Remember the message in case we need to repeat it.
        tsd.send_queue[txn_id] = (message, address, time.time())

        return txn_id

    # Resend any messages that have not yet yielded a reply.
    def repeat_deferred(self, txn_ids):
        #Make the target a list of txn_id to consider.
        if type(txn_ids) != types.ListType:
            txn_ids = [txn_ids]
        
        tsd = self.get_tsd()

        TEN_MINUTES = 10 * 60  #Time limit.
        
        for txn_id in txn_ids:
            message, address, timestamp = tsd.send_queue[txn_id]

            #After 10 minutes, assume we are not going to get a response.
            # Remove the item from the queue to prevent it from growing
            # without bound.
            if timestamp < time.time() - TEN_MINUTES:
                del tsd.send_queue[txn_id]
                continue

            tsd.socket.sendto( message, address )

    # Recieve a reply, timeout has the same meaning as in select
    def recv_deferred(self, txn_ids, timeout):
        #Make the target a list of txn_id to consider.
        if type(txn_ids) != types.ListType:
            txn_ids = [txn_ids]
            
        tsd = self.get_tsd()
        for txn_id in txn_ids:
            if tsd.reply_queue.has_key(txn_id):
                reply = tsd.reply_queue[txn_id]
                del tsd.reply_queue[txn_id]
                del tsd.send_queue[txn_id]
                return reply
        else:
            rcvd_txn_id=None
            while rcvd_txn_id not in txn_ids: #look for reply
                reply = None
                r, w, x, timeout = cleanUDP.Select( [tsd.socket], [], [], timeout)
                if r:
                    reply, server_addr = \
                           tsd.socket.recvfrom(TRANSFER_MAX, timeout)
                if not reply: # receive or select timed out
                    break
                try:
                    rcvd_txn_id, out, t = udp_common.r_eval(reply)
                    if type(out) == type({}) and out.has_key('status') \
                       and out['status'][0] == e_errors.MALFORMED:
                        del tsd.send_queue[rcvd_txn_id]
                        return out
                except (SyntaxError, TypeError):
                    #If a this error occurs, keep retrying.  Most likely it is
                    # an "expected string without null bytes".
                    #If SyntaxError occurs, also keep trying, most likely
                    # it is from and empty UDP datagram.
                    exc, msg = sys.exc_info()[:2]
                    try:
                        message = "%s: %s: From server %s:%s" % \
                                  (exc, msg, server_addr, reply[:100])
                    except IndexError:
                        message = "%s: %s: From server %s: %s" % \
                                  (exc, msg, server_addr, reply)
                    Trace.log(10, message)

                    #Set this to none.  Since it is invalid, don't add it
                    # to the queue and instead skip the following if and
                    # go right back to the top of the loop.
                    rcvd_txn_id=None
                    continue
                
                if rcvd_txn_id not in txn_ids:
                    #Queue it up, somebody else wants it
                    tsd.reply_queue[rcvd_txn_id] = out
            else: # we got a good reply
                del tsd.send_queue[rcvd_txn_id]
                return out

        ##If we got here, it's because we didn't receive a response to the
        ## message we sent.
        raise e_errors.EnstoreError(errno.ETIMEDOUT, "", e_errors.TIMEDOUT)

    # Recieve a reply, timeout has the same meaning as in select.  This
    # version is different from recv_deferred(); entire_timeout refers to the
    # entire timeout period just like recv_deferred(), but a geometric backoff
    # of resending the original message is performed up to entire_timeout
    # number of seconds.
    #
    #Note: This function not fully verified to work yet.
    def recv_deferred_with_repeat_send(self, txn_ids, entire_timeout):
        #Make the target a list of txn_id to consider.
        if type(txn_ids) != types.ListType:
            txn_ids = [txn_ids]
            
        tsd = self.get_tsd()
        for txn_id in txn_ids:
            if tsd.reply_queue.has_key(txn_id):
                reply = tsd.reply_queue[txn_id]
                del tsd.reply_queue[txn_id]
                del tsd.send_queue[txn_id]
                return reply
        else:
            loop_start_time = time.time()
            exp = 0
            base_timeout = 5 #seconds
            while loop_start_time + entire_timeout > time.time():
                #We need to do this geometric timeout ourselves.
                timeout = base_timeout * (pow(2, exp))
                if exp < MAX_EXPONENT:
                    exp = exp + 1
                #Limit the timeout to what is left of the entire resubmit
                # timeout.
                upper_limit = max(0,
                           loop_start_time + entire_timeout - time.time())
                timeout = min(timeout, upper_limit)
                
                try:
                    return self.recv_deferred(txn_ids, timeout)
                except (socket.error, e_errors.EnstoreError), msg:
                    if msg.errno == errno.ETIMEDOUT:
                        #Since we are still waiting for a response, resend
                        # the original message.
                        self.repeat_deferred(txn_ids)
                        continue
                    else:
                        raise sys.exc_info()[0], sys.exc_info()[1], \
                              sys.exc_info()[2]
                    
    #If we are giving up on a reponse, we can remove it from the send and 
    # receive lists explicitly.
    def drop_deferred(self, txn_ids):
        #Make the target a list of txn_id to consider.
        if type(txn_ids) != types.ListType:
            txn_ids = [txn_ids]

        tsd = self.get_tsd()
        for txn_id in txn_ids:
            try:
                del tsd.reply_queue[txn_id]
            except KeyError:
                pass
            try:
                del tsd.send_queue[txn_id]
            except KeyError:
                pass
    
        
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
            back = u.send(msg, address, rcv_timeout = 10, max_send=3)
            print "Recieved message %s." % (back)

    except:
        exc, msg = sys.exc_info()[:2]
        print "Unable to complete test: %s: %s" % (str(exc), str(msg))
        status = 1

    del u

    sys.exit(status)
    
