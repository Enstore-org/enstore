###############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
import socket
import time
import select
import os
import errno
import exceptions
import errno
import sys

# enstore imports
import interface
import timeofday
import Trace
import ECRC
import generic_cs

TRANSFER_MAX=16384

# see if we can allocate a specific port on a specific host
def try_a_port(host, port) :
    Trace.trace(20,'{try_a_port '+repr((host,port)))
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(host, port)
    except:
        sock.close()
        Trace.trace(20,'}try_a_port failure')
        return (0, sock) # failure
    Trace.trace(20,'}try_a_port success')
    return (1, sock)     # success


# try to get a port from a range of possibilities
def get_client() :
    Trace.trace(20,'{get_client')
    (hostname,ha,hi) = socket.gethostbyaddr(socket.gethostname())
    host = hi[0]
    port1 = 7600
    port2 = 8000
    while  1:
        for port in range (port1, port2) : # range (7600, 7600) has 0 members...
            success, sockt = try_a_port (host, port)
            if success :
                Trace.trace(20,'{get_client '+repr((host,port))+" "+repr(sockt))
                return host, port, sockt
        Trace.trace(0,'{get_client sleeping for 10 - all ports used '+\
                    repr((port1, port2)))
        time.sleep(10) # tried all ports, try later.

def empty_socket( sock ):
    xcount = 0         # number of exceptions on select
    rcount = 0         # number of reads
    xcountmax = 2      # retry count if select exception
    rcountalert = 10   # complain if we read too much
    while xcount < xcountmax:
        try:
            f = sock.fileno()
            r, w, x = select.select([f],[],[f],0)
            #generic_cs.enprint("empty socket "+repr(sock))
	    #generic_cs.enprint(sock.__dict__, generic_cs.PRETTY_PRINT)
	    #generic_cs.enprint(repr(f)+" "+repr(xcount)+" "+repr(r)+" "+\
	    #                    repr(w)+" "+repr(x))

            # exception mean trouble
            if x:
                xcount = xcount+1
                Trace.trace(0, "empty_socket: exception on select to " +repr(f))
                Trace.trace(0,"empty_socket"+str(sys.exc_info()[0])+str(sys.exc_info()[1]))
                #generic_cs.enprint("empty socket exception "+repr(xcount))
	        #generic_cs.enprint(sock.__dict__, generic_cs.PRETTY_PRINT)
	        #generic_cs.enprint(repr(r)+" "+repr(w)+" "+repr(x)+" "+\
	        #                    str(sys.exc_info()[0])+\
	        #                    " "+str(sys.exc_info()[1]))

            elif r:
                rcount = rcount+1
                if rcount%rcountalert == 0:
                    Trace.trace(4,"empty_socket: r from select - count="+repr(rcount))
                badsock = sock.getsockopt(socket.SOL_SOCKET,socket.SO_ERROR)
                if badsock==errno.ECONNREFUSED:
                    Trace.trace(3,"ECONNREFUSED...retrying (empty_socket r:)")
                    badsock = sock.getsockopt(socket.SOL_SOCKET,socket.SO_ERROR)
                if badsock != 0:
                    Trace.trace(0,"empty pre recv, clearout error "+\
                                repr(errno.errorcode[badsock]))
                reply , server = sock.recvfrom(TRANSFER_MAX)
                badsock = sock.getsockopt(socket.SOL_SOCKET,socket.SO_ERROR)
                if badsock==errno.ECONNREFUSED:
                    Trace.trace(0,"ECONNREFUSED: Redoing recvfrom. POSSIBLE ERROR empty_socket")
                    self.enprint("ECONNREFUSED: Redoing recvfrom. POSSIBLE ERROR empty_socket")
                    reply , server = sock.recvfrom(TRANSFER_MAX)
                    badsock = sock.getsockopt(socket.SOL_SOCKET,socket.SO_ERROR)
                if badsock != 0:
                    Trace.trace(0,"empty post recv, clearout error"+\
                                repr(errno.errorcode[badsock]))
            else:
                xcount = xcountmax # nothing to read - no more retries

        except:
            Trace.trace(0,'empty_socket clearout err'+str(sys.exc_info()[0])+str(sys.exc_info()[1]))
            xcount = xcountmax # no more retries on unhandled exception

    return
def send_socket( sock, message, address ):
    badsock = sock.getsockopt( socket.SOL_SOCKET, socket.SO_ERROR )
    if badsock==errno.ECONNREFUSED:
        Trace.trace(3,"ECONNREFUSED...retrying (get_request r:)")
        badsock = sock.getsockopt( socket.SOL_SOCKET, socket.SO_ERROR )
    if badsock != 0:
	Trace.trace( 0, "send_socket pre send"+repr(errno.errorcode[badsock]) )
	generic_cs.enprint("udp_client send_socket, pre-sendto error: "+\
	      repr(errno.errorcode[badsock]))
    sent = 0
    while sent == 0:
	try:
	    sock.sendto( message, address )
            badsock = sock.getsockopt(socket.SOL_SOCKET,socket.SO_ERROR)
            if badsock==errno.ECONNREFUSED:
                Trace.trace(0,"ECONNREFUSED: Redoing sendto. POSSIBLE ERROR send_socket")
		self.enprint("ECONNREFUSED: Redoing sendto. POSSIBLE ERROR send_socket")
                sock.sendto( message, address )
                badsock = sock.getsockopt(socket.SOL_SOCKET,socket.SO_ERROR)
            if badsock != 0:
                Trace.trace( 0,"send_socket post send"+repr(errno.errorcode[badsock]) )
                generic_cs.enprint("udp_client send_socket, post-sendto "+\
                                   repr(address)+" error: "+ \
                                   repr(errno.errorcode[badsock]))
            else:
                sent = 1
	except socket.error:
	    Trace.trace(  0
			, 'send_socket: Nameserver not responding add='
			  +repr(address)+str(sys.exc_info()[0])
			  +str(sys.exc_info()[1]) )
	    generic_cs.enprint(repr(timeofday.tod())+ \
		  "udp_client.send_socket: Nameserver not responding\n"+\
		  message+"\n"+repr(address)+"\n"+\
		  str(sys.exc_info()[0])+"\n"+str(sys.exc_info()[1]))
	    time.sleep(3)		# arbitrary time - don't beat on NS
    return

def wait_rsp( sock, address, rcv_timeout ):
    # init return vals
    reply=''
    server=''

    f = sock.fileno()
    r, w, x = select.select( [f], [], [f], rcv_timeout )

    # exception mean trouble
    if x:
	Trace.trace( 0, "send: exception on select after send to "
		        +repr(address)+" "+repr(x) )
	Trace.trace(0,"send"+str(sys.exc_info()[0])+str(sys.exc_info()[1]))
	generic_cs.enprint("UDPClient.send: exception on select after send to "+\
	      repr(address)+" "+repr(x)+" "+str(sys.exc_info()[0])+" "+\
	      str(sys.exc_info()[1]))

    # something there - read it and see if we have response that
    # matches the number we sent out
    elif r:
	badsock = sock.getsockopt( socket.SOL_SOCKET, socket.SO_ERROR )
        if badsock==errno.ECONNREFUSED:
            Trace.trace(3,"ECONNREFUSED...retrying (wait_rsp r:)")
            badsock = sock.getsockopt( socket.SOL_SOCKET, socket.SO_ERROR )
	if badsock != 0:
	    Trace.trace( 0,"send pre recv"+repr(errno.errorcode[badsock]) )
	    generic_cs.enprint("udp_client send, pre-recv error: "+\
	                       repr(errno.errorcode[badsock]))
	reply , server = sock.recvfrom( TRANSFER_MAX )
	badsock = sock.getsockopt( socket.SOL_SOCKET, socket.SO_ERROR )
        if badsock==errno.ECONNREFUSED:
            Trace.trace(0,"ECONNREFUSED: Redoing recvfrom. POSSIBLE ERROR wait_rsp")
            self.enprint("ECONNREFUSED: Redoing recvfrom. POSSIBLE ERROR wait_rsp")
            reply , server = sock.recvfrom( TRANSFER_MAX )
            badsock = sock.getsockopt( socket.SOL_SOCKET, socket.SO_ERROR )
	if badsock != 0:
	    Trace.trace( 0,"send post recv"+repr(errno.errorcode[badsock]))
	    generic_cs.enprint("udp_client send, post-recv error: "+ \
	                       repr(errno.errorcode[badsock]))

    return reply, server

def protocolize( self, text ):
    # make a new message number - response needs to match this number
    # We use to increment by one, but that does not work with forking
    # lcl_number = self.number + 1
    # note: do str here; elsewhere causes problems (precision is 1ms -
    # this should be OK)
    lcl_number = "%.6f"%time.time()

    # CRC text
    body = `(self.ident, lcl_number, text)`
    crc = ECRC.ECRC(body, 0)

    # stringify message and check if it is too long
    message = `(body, crc)`

    if len(message) > TRANSFER_MAX:
	Trace.trace(0,"send:message "+\
		    "too big. Size = "+repr(len(message))+" Max = "+\
		    repr(TRANSFER_MAX)+" "+repr(message))
	raise errno.errorcode[errno.EMSGSIZE],"udp_client.check_len:message "+\
	      "too big. Size = "+repr(len(message))+" Max = "+\
	      repr(TRANSFER_MAX)+" "+repr(message)

    return message, lcl_number



class UDPClient:

    def __init__(self, host="", port=0, socket=0):
        Trace.trace(10,'__init__ udpclient '+repr((host,port,socket)))
        if host == "":
            host, port, self.socket = get_client()
        else:
            self.socket = socket
        self.number = "0"
        self.ident = "%s-%d-%f-%d" \
                     % (host, port, time.time(), os.getpid() )
        self.sendport = 7
        self.where_sent = {}
	try:
	    x = os.environ['ENSTORE_UDP_PP']
	    self.pp = 1
	except:
	    self.pp = 0
        Trace.trace(10,'{__init__ udpclient '+repr(self.ident))

    def __del__(self):
        # tell file clerk we're done - this allows it to delete our unique id in
        # its dictionary - this keeps things cleaner & stops memory from growing
        Trace.trace(10,'__del__ udpclient '+repr(self.ident))
        for server in self.where_sent.items() :
            #generic_cs.enprint("clearing "+server[0]+" "+ server[1])
            try:
                self.send_no_wait({"work":"done_cleanup"}, server[0])
            except:
                pass
        Trace.trace(10,'__del__ udpclient')

    # this (generally) is received/processed by dispatching worker
    def send( self, text, address, rcv_timeout=0, tries=0 ):
        Trace.trace( 20, 'send add='+repr(address)+' text='+repr(text) )
	if self.pp and text['work']!='idle_mover':
	    x=sys.stdout;sys.stdout=sys.stderr
	    generic_cs.enprint("\nreq/cmd to address: "+repr(address)+\
	                       " from: "+ repr(self.ident))
	    generic_cs.enprint(text, generic_cs.PRETTY_PRINT)
	    sys.stdout=x

	if rcv_timeout:
            if tries==0:
                tries = 1      # if timeout!=0 and tries=0, then try just once
	else:
	    rcv_timeout = 10   # default timeout - also no adjusting of tries

	message, self.number = protocolize( self, text )

        # keep track of where we are sending things so we can clean up later
        self.where_sent[address] = (self.ident,text)

        # make sure the socket is empty before we start
	empty_socket( self.socket )

        # send the udp message until we get a response that it was sent
        number = "0"  # impossible "number"
        ntries = 0  
        while number != self.number:
	    send_socket( self.socket, message, address )
            ntries = ntries+1
            
            # check for a response
	    reply , server = wait_rsp( self.socket, address, rcv_timeout )

	    if reply != "":
		# OK, we have received something -- "try" it
		try:
		    exec ("number,  out, time  = "  + reply)
		# did we read entire message (bigger than TRANSFER_MAX?)
	        except exceptions.SyntaxError :
		    Trace.trace(0,"send disaster: didn't read entire message"+\
				"server="+repr(server)+" "+\
				str(sys.exc_info()[0])+str(sys.exc_info()[1]))
		    generic_cs.enprint("disaster: didn't read entire message")
		    generic_cs.enprint("reply: "+repr(reply))
		    generic_cs.enprint("server: "+repr(server))
		    raise sys.exc_info()[0],sys.exc_info()[1]
		# goofy test feature - need for client being echo service only
		except exceptions.ValueError :
		    Trace.trace(0,'send GOOFY TEST FEATURE')
		    exec ("ident, number,  out, time  = "  + reply)

		# now (after receive), check...
		if number != self.number :
                    generic_cs.enprint(repr(type(number))+" "+\
	                               repr(type(self.number)))
		    msg="UDPClient.send: stale_number=%s number=%s" %\
			 (number,self.number)
		    Trace.trace(21,'send stale='+repr(number)+' want='+\
				repr(self.number))
		    generic_cs.enprint(msg+" resending to "+repr(address)+\
	                               message)

	    elif tries!=0 and ntries>=tries:  # no reply after requested tries
                Trace.trace(0,"send quiting,no reply after tries="+repr(ntries))
		raise errno.errorcode[errno.ETIMEDOUT]
            else:
                Trace.trace(0,"send no reply after tries="+repr(ntries))

	if self.pp and (not 'work' in out.keys() or out['work']!='nowork'):
	    x=sys.stdout;sys.stdout=sys.stderr
	    generic_cs.enprint("\nrsp - sent to: "+repr(self.ident))
	    generic_cs.enprint(out, generic_cs.PRETTY_PRINT)
	    sys.stdout=x
	Trace.trace(20,"}send "+repr(out))
        return out

    # send message without waiting for reply and resend
    def send_no_wait(self, text, address) :
        Trace.trace(20,'send_no_wait add='+repr(address)+' text='+repr(text))
	if self.pp:
	    x=sys.stdout;sys.stdout=sys.stderr
	    generic_cs.enprint("\nmsg/cmd to address: "+repr(address)+\
	                       " from: "+repr(self.ident))
	    generic_cs.enprint(text, generic_cs.PRETTY_PRINT)
	    sys.stdout=x

	message, self.number = protocolize( self, text )

        # send the udp message
	send_socket( self.socket, message, address )

        Trace.trace( 20, '}send_no_wait' )

class UDPClientInterface(interface.Interface):

    def __init__(self):
        Trace.trace(10,'__init__ ci')
        self.msg = "All dogs have fleas, but cats make you sick!"
        self.host, self.port, self.socket = get_client()
        interface.Interface.__init__(self)

        # parse the options
        self.parse_options()
        Trace.trace(10,'__init__ ci '+repr((self.host,self.port,self.socket)))

    # define the command line options that are valid
    def options(self):
        Trace.trace(20,'{}options')
        return ["verbose=", "msg=","host=","port="] +\
               self.help_options()

if __name__ == "__main__" :

    status = 0

    # fill in the interface
    intf = UDPClientInterface()

    # get a UDP client
    u = UDPClient(intf.host, intf.port, intf.socket)

    #generic_cs.enprint(u.__dict__, generic_cs.PRETTY_PRINT)

    generic_cs.enprint("Sending:\n"+intf.msg+"\nto"+repr(intf.sendhost)+" "+\
	               repr(intf.sendport)+" with calback on "+\
	      	       repr(intf.port), generic_cs.CONNECTING, intf.verbose)
    back = u.send(intf.msg, (intf.sendhost, intf.sendport))

    if back != intf.msg :
        generic_cs.enprint("Error: sent:\n"+intf.msg+"\nbut read:\n"+back)
        status = status|1

    else:
	generic_cs.enprint("Read back:\n"+back, generic_cs.CONNECTING,\
	                   intf.verbose)

    sys.exit(status)
