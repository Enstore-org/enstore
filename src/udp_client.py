#!/usr/bin/env python
"""
Enstore UDP client is used to communicate with Enstore UDP server.
It is thread safe.
"""

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
import enstore_constants


MAX_EXPONENT=6 # do not increase reeive TO in send beyond this
TRANSFER_MAX=enstore_constants.MAX_UDP_PACKET_SIZE #Max size of UDP datagram.


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


class UDPClient:

    def __init__(self):
        self.thread_specific_data = threading.local() #Thread-specific data

        self.reinit()

    def reinit(self):
        #Obtain necessary values.
        pid = os.getpid()
        host, port, socket = udp_common.get_default_callback()
        if thread_support:
            tid = thread.get_ident() #Obtain unique identifier.
        else:
            tid = 1

        #Build thread specific data.
        tsd = self.thread_specific_data  #local shortcut
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

        return tsd

    #Return this thread's local data.  If it hasn't been initialized yet,
    # call reinit() to do so.
    def get_tsd(self):

        if not hasattr(self.thread_specific_data, 'pid'):
            self.reinit()

        return self.thread_specific_data

    #Return the IP address for the socket.
    def get_address(self):
        tsd = self.get_tsd()

        return tsd.socket.getsockname()

    def _mkident(self, host, port, pid, tid):
        return "%s-%d-%f-%d-%d" % (host, port, time.time(), pid, abs(tid))


    def fileno(self):
        return self.get_tsd().socket.fileno()

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


    def send(self, data, dst, rcv_timeout=0, max_send=0, send_done=1, exponential_to=False):
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
            if exponential_to:
                max_exponent = MAX_EXPONENT
            else:
                max_exponent = 0


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
            Trace.trace(5, "sending %s %s"%(msg, dst))
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
                    Trace.trace(5, "TIMEOUT sending %s"%(msg,))
                    break #resend
                #print "GOT REPLY",reply
                Trace.trace(5, "GOT REPLY %s"%(reply,))
                try:
                    rcvd_txn_id, out, t = udp_common.r_eval(reply)
                    if type(out) == type({}) and out.has_key('status') \
                       and out['status'][0] == e_errors.MALFORMED:
                        return out
                except (SyntaxError, TypeError, ValueError):
                    #If TypeError occurs, keep retrying.  Most likely it is
                    # an "expected string without null bytes".
                    #If SyntaxError occurs, also keep trying, most likely
                    # it is from and empty UDP datagram.
                    #A ValueError can happen if the eval-ed reply does
                    # not contain a triple, but some other number of elements.
                    exc, msg, tb = sys.exc_info()
                    try:
                        message = "%s: %s: From server %s:%s" % \
                                  (exc, msg, server_addr, reply[:100])
                    except IndexError:
                        message = "%s: %s: From server %s: %s" % \
                                  (exc, msg, server_addr, reply)
                    Trace.log(10, message)
                    #Trace.handle_error(exc, msg, tb, severity=10)
                    del tb  #Avoid resource leak.

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
    def send_no_wait(self, data, address, unique_id=False) :
        tsd = self.get_tsd()
        if unique_id:
            # Create unique id for each message.
            tsd.ident = self._mkident(tsd.host, tsd.port, tsd.pid, tsd.tid)

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
    ### what it has always been done; only wait for the prescribed time
    ### without any automatic resending of messages where a response has not
    ### returned after too long of a time period.  This means that the caller
    ### of the recv_deferred() function needs to handle their own retrying
    ### with geometric timeout backoff.
    ###
    ### The other recv_deffered function, recv_deferred_with_repeat_send(),
    ### performs automatic resending/retrying of messages.  This function
    ### does geometric backoff of retransmitted messages.

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
            try:
                message, address, timestamp = tsd.send_queue[txn_id]
            except KeyError:
                continue

            #After 10 minutes, assume we are not going to get a response.
            # Remove the item from the queue to prevent it from growing
            # without bound.
            if timestamp < time.time() - TEN_MINUTES:
                del tsd.send_queue[txn_id]
                continue

            tsd.socket.sendto( message, address )

    # Recieve a reply, timeout has the same meaning as in select
    def __recv_deferred(self, txn_ids, timeout):
        #Make the target a list of txn_id to consider.
        if type(txn_ids) != types.ListType:
            txn_ids = [txn_ids]

        tsd = self.get_tsd()
        for txn_id in txn_ids:
            if tsd.reply_queue.has_key(txn_id):
                reply = tsd.reply_queue[txn_id]
                del tsd.reply_queue[txn_id]
                try:
                    del tsd.send_queue[txn_id]
                except KeyError:
                    #Apparently it is possible to get here when Enstore has
                    # very high load.  How exactly does this happen?
                    pass
                return reply, txn_id
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
                        return out, rcvd_txn_id
                except (SyntaxError, TypeError):
                    #If a this error occurs, keep retrying.  Most likely it is
                    # an "expected string without null bytes".
                    #If SyntaxError occurs, also keep trying, most likely
                    # it is from and empty UDP datagram.
                    exc, msg, tb = sys.exc_info()
                    try:
                        message = "%s: %s: From server %s:%s" % \
                                  (exc, msg, server_addr, reply[:100])
                    except IndexError:
                        message = "%s: %s: From server %s: %s" % \
                                  (exc, msg, server_addr, reply)
                    Trace.log(10, message)
                    #Trace.handle_error(exc, msg, tb, severity=10)
                    del tb  #Avoid resource leak.

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
                return out, rcvd_txn_id

        ##If we got here, it's because we didn't receive a response to the
        ## message we sent.
        raise e_errors.EnstoreError(errno.ETIMEDOUT, "", e_errors.TIMEDOUT)

    # Recieve a reply, timeout has the same meaning as in select
    #This version returns the message.
    def recv_deferred(self, txn_ids, timeout):
        return self.__recv_deferred(txn_ids, timeout)[0]

    # Recieve a reply, timeout has the same meaning as in select
    #This version returns a tuple of the message and transaction id.
    def recv_deferred2(self, txn_ids, timeout):
        return self.__recv_deferred(txn_ids, timeout)

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
                        raise sys.exc_info()

    #If we are giving up on a response, we can remove it from the send and
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

    status = 0
    def send_test(msg, address, udp_c):
        global status

        tsd = udp_c.get_tsd()

        print "Sending message %s to %s in %s thread using callback %s." \
              % (msg, address, threading.current_thread().getName(), (tsd.host, tsd.port))

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

        return status

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

    # get a UDP client
    u = UDPClient()
    print "Default client callback for %s thread: %s" % (threading.current_thread().getName(), (u.get_tsd().host, u.get_tsd().port))
    message = {'message' : "TEST MESSAGE"}
    # To test big data transfers consider the following:
    # data = open("big_text_file", "r").readlines() where big_text_file size is < enstore_constants.MAX_UDP_PACKET_SIZE,
    ## and > 16KB
    # message = {'message':data}
    address = ("localhost", 7700)

    test_thread = threading.Thread(target = send_test,
                                   args = (message, address, u),
                                   name = "test_thread")
    test_thread.start()
    test_thread.join()

    del u

    sys.exit(status)  #Note: status is global.

