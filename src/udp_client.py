###############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
import socket
import time
import os
import errno
import exceptions
import errno
import sys

# enstore imports
import e_errors
import interface
import Trace
import checksum
import cleanUDP
import hostaddr

UDPError = "UDP Error"

TRANSFER_MAX=16384

# see if we can allocate a specific port on a specific host
def try_a_port(host, port) :
    Trace.trace(20,"try_a_port: trying udp port %s %s" % (host, port))
    try:
	sock = cleanUDP.cleanUDP(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind((host, port))
    except:
	try:
            sock.close()
	except:
	    pass
        return (0, 0) # failure
    Trace.trace(20,'try_a_port success')
    return (1, sock)     # success


# try to get a port from a range of possibilities
def get_client() :
    (hostname,ha,hi) = hostaddr.gethostinfo()
    host = hi[0]
    port1 = 7000
    port2 = 8000
    while  1:
        for port in range (port1, port2) : 
            success, sockt = try_a_port (host, port)
            if success :
                return host, port, sockt
        Trace.log(e_errors.INFO,'get_client sleeping for 10 - all ports from %s to %s used '%
                  (port1, port2))
        time.sleep(10) # tried all ports, try later.


def wait_rsp( sock, address, rcv_timeout ):
    # init return vals
    reply,server=None,None

    r, w, x, rcv_timeout = cleanUDP.Select( [sock], [], [sock], rcv_timeout)
    if r:
	reply , server = sock.recvfrom( TRANSFER_MAX )
    elif x or w :
        exc,msg,tb=sys.exc_info()
	Trace.log(e_errors.INFO, "UDPClient.send: exception on select after send to %s %s: %s %s"%
                  (address,x,exc,msg))
	raise UDPError, "impossible to get these set w/out [r]"
    return reply, server, rcv_timeout



class UDPClient:

    def __init__(self, host=None, port=0, socket=0):
        if not host:
            self.host, self.port, self.socket = get_client()
        else:
            self.socket = socket
        self.txn_counter = 0L
        self.pid = None
        self.sendport = 7
        self.where_sent = {}

    def ident(self):
        pid=os.getpid()  
        if pid != self.pid:  #recompute ident each time we fork
            self.pid = pid
            self._ident = "%s-%d-%f-%d" % (self.host, self.port, time.time(), self.pid )
        return self._ident 
        
    def __del__(self):
        # tell file clerk we're done - this allows it to delete our unique id in
        # its dictionary - this keeps things cleaner & stops memory from growing
        for server in self.where_sent.items() :
            #Trace.log(e_errors.INFO, "clearing "+server[0]+" "+ server[1])
            try:
                self.send_no_wait({"work":"done_cleanup"}, server[0])
            except:
                pass

    def _eval_reply(self, reply): #private to send
        try:
            number,  out, t  = eval(reply)   ##XXX
            # catch any error and keep going. server needs to be robust
        except:
            exc,msg,tb=sys.exc_info()
            logmsg="udpClient.eval_reply %s %s"%(exc, msg)
            if exc == exceptions.SyntaxError: #msg size> max datagram size?
                logmsg=logmsg+"Truncated message?"
            Trace.log(e_errors.ERROR, logmsg)
            raise exc, msg
        return number, out, t


    def protocolize( self, text ):

        self.txn_counter = self.txn_counter + 1

        # CRC text
        body = `(self.ident(), self.txn_counter, text)`
        crc = checksum.adler32(0L, body, len(body))

        # stringify message and check if it is too long
        message = `(body, crc)`

        if len(message) > TRANSFER_MAX:
            errmsg="send:message too big, size=%d, max=%d" %(len(message),TRANSFER_MAX)
            Trace.log(e_errors.ERROR,errmsg)
            raise errno.errorcode[errno.EMSGSIZE],errmsg

        return message, self.txn_counter


        
    def send(self, data, dst, rcv_timeout=0, max_send=0):
        """send msg to dst address, up to `max_send` times, each time
        waiting `rcv_timeout' seconds for reply
        A value of 0 for max_send means retry forever"""

	if rcv_timeout:
            if max_send==0:
                max_send = 1 # protect from nonsense inputs XXX should we do this?
	else:
	    rcv_timeout = 10   

        msg, txn_id = self.protocolize(data)
        # keep track of where we are sending things so we can clean up later
        self.where_sent[dst] = (self.ident,msg)
        
        n_sent = 0
        while max_send==0 or n_sent<max_send:
            self.socket.sendto( msg, dst )
            n_sent=n_sent+1
            rcvd_txn_id=None
            timeout=rcv_timeout
            while rcvd_txn_id != txn_id: #look for reply while rejecting "stale" responses
                reply, server, timeout = wait_rsp( self.socket, dst, timeout)
                if not reply:  # receive timed out
                    break #resend
                rcvd_txn_id, out, t = self._eval_reply(reply)
            else: # we got a good reply
                return out

        ##if we got here, it's because we didn't receive a response to the message we sent.
        raise errno.errorcode[errno.ETIMEDOUT]

        
    # send message without waiting for reply and resend
    def send_no_wait(self, data, address) :
	message, txn_id = self.protocolize( data )
	return self.socket.sendto( message, address )

        
class UDPClientInterface(interface.Interface):

    def __init__(self):
        self.msg = "All dogs have fleas"
	self.sendhost="localhost"
	self.sendport=9998
        self.host, self.port, self.socket = get_client()
        interface.Interface.__init__(self)

        # parse the options
        self.parse_options()

    # define the command line options that are valid
    def options(self):
        return ["msg=","host=","port="] +\
               self.help_options()

if __name__ == "__main__" :

    status = 0

    # fill in the interface
    intf = UDPClientInterface()

    # get a UDP client
    u = UDPClient(intf.host, intf.port, intf.socket)

    print "Sending message", intf.msg, "to", intf.sendhost, " with callback on ", intf.port

    back = u.send(intf.msg, (intf.sendhost, intf.sendport))

    if back != intf.msg :
        print "Error: sent:",intf.msg+"but read:",back
        status = 1

    else:
	print "Read back:", back

    sys.exit(status)
    
