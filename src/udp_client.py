###############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
import socket
import time
import os
import errno
import exceptions
import sys
try:
    import threading
    thread_support=1
except ImportError:
    thread_support=0

import rexec

# enstore imports
import Trace
import setpath
import e_errors
import interface
import checksum
import cleanUDP
import hostaddr
import host_config

UDPError = "UDP Error"

TRANSFER_MAX=16384


# try to get a port from a range of possibilities
def get_client() :
    #(hostname,ha,hi) = hostaddr.gethostinfo()
    #host = hi[0]
    #Pick an interface based on the current load of the system.
    #host = host_config.check_load_balance()['ip']
    #host = host_config.choose_interface()['ip']
    #Pick an interface.
    host = host_config.get_default_interface_ip()

    sock = cleanUDP.cleanUDP(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((host, 0))
    host, port = sock.getsockname()
    return host, port, sock


def wait_rsp( sock, address, rcv_timeout ):

    reply,server=None,None

    r, w, x, rcv_timeout = cleanUDP.Select( [sock], [], [sock], rcv_timeout)
    if r:
        reply , server = sock.recvfrom( TRANSFER_MAX, rcv_timeout)

    elif x or w :
        exc,msg,tb=sys.exc_info()
        Trace.log(e_errors.INFO, "UDPClient.send: exception on select after send to %s %s: %s %s"%
                  (address,x,exc,msg))
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
        pid = self._os.getpid()
        tsd = Container()
        self.tsd[pid] = tsd
        tsd.host, tsd.port, tsd.socket = get_client()
        tsd.txn_counter = 0L
        tsd.reply_queue = {}
        tsd.ident = self._mkident(tsd.host, tsd.port, pid)
        tsd.send_done = {}
        if thread_support:
            tsd.thread = threading.currentThread() 
        return tsd

    def get_tsd(self):
        pid = self._os.getpid()
        tsd = self.tsd.get(pid)
        if not tsd:
            if thread_support:
                for key, value in self.tsd.items():
                    #Clean up resources of exited threads
                    try:
                        if not value.thread.isAlive():
                            del self.tsd[key]
                    except:
                        pass # another thread could have done the cleanup...
            tsd = self.reinit()
        return tsd
    
    def _mkident(self, host, port, pid):
        return "%s-%d-%f-%d" % (host, port, time.time(), pid )
        
    def __del__(self):
        # tell server we're done - this allows it to delete our unique id in
        # its dictionary - this keeps things cleaner & stops memory from growing
        try:
            pid = self._os.getpid()
            tsd = self.tsd.get(pid)
            if not tsd:
                return
            for server in tsd.send_done.keys() :
                try:
                    self.send_no_wait({"work":"done_cleanup"}, server)
                except:
                    pass
                try:
                    tsd.socket.close()
                except:
                    pass
        except:
            pass
        
    def _eval_reply(self, reply): #private to send
        try:
            number, out, t = r_eval(reply) #XXX
            # catch any error and keep going. server needs to be robust
        except:
            exc,msg,tb=sys.exc_info()
            logmsg="udpClient.eval_reply %s %s"%(exc, msg)
            if exc == exceptions.SyntaxError: #msg size> max datagram size?
                logmsg=logmsg+"Truncated message?"
            elif exc == exceptions.TypeError:
                logmsg = logmsg + ": " + reply
            Trace.log(e_errors.ERROR, logmsg)
            raise exc, msg, tb
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
            Trace.log(e_errors.ERROR,errmsg)
            raise errno.errorcode[errno.EMSGSIZE],errmsg

        return message, tsd.txn_counter

        
    def send(self, data, dst, rcv_timeout=0, max_send=0):
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

        #set up the static route before sending.
        host_config.set_route(host_config.get_default_interface_ip(), dst[0])

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
                    exc, msg, tb = sys.exc_info()
                    try:
                        message = "%s: %s: From server %s:%s" % \
                                  (exc, msg, server, reply[:100])
                    except IndexError:
                        message = "%s: %s: From server %s: %s" % \
                                  (exc, msg, server, reply)

                    Trace.log(e_errors.INFO, message)
                    rcvd_txn_id=None
            else: # we got a good reply
                return out

        ##if we got here, it's because we didn't receive a response to the message we sent.
        raise errno.errorcode[errno.ETIMEDOUT]
        
    # send message without waiting for reply and resend
    def send_no_wait(self, data, address) :
        tsd = self.get_tsd()
        message, txn_id = self.protocolize( data )
        #set up the static route before sending.
        host_config.set_route(host_config.get_default_interface_ip(), address[0])
        return tsd.socket.sendto( message, address )

    # send message, return an ID that can be used in the recv_deferred function
    def send_deferred(self, data, address) :
        tsd = self.get_tsd()
        tsd.send_done[address] = 1
        message, txn_id = self.protocolize( data )
        #set up the static route before sending.
        host_config.set_route(host_config.get_default_interface_ip(), address[0])
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
                    exc, msg, tb = sys.exec_info()
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

        ##if we got here, it's because we didn't receive a response to the message we sent.
        raise errno.errorcode[errno.ETIMEDOUT]

        
    
        
if __name__ == "__main__" :

    status = 0

    # get a UDP client
    u = UDPClient()

    msg="TEST MESSAGE"
    print "Sending message", msg, "to", u.host, " with callback on port ", u.port

    back = u.send_no_wait(msg, (u.host, u.port))
    print "sleeping for 5 sec"
    time.sleep(5)
    del u
    print "sleeping for 5 sec"
    time.sleep(5)
    print "back"

    sys.exit(status)
    
