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
import ECRC
import generic_cs
import cleanUDP

TRANSFER_MAX=16384

# see if we can allocate a specific port on a specific host
def try_a_port(host, port) :
    Trace.trace(20,'{try_a_port '+repr((host,port)))
    try:
	sock = cleanUDP.cleanUDP(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind((host, port))
    except:
	try:
            sock.close()
	except:
	    pass
        Trace.trace(20,'}try_a_port failure')
        return (0, 0) # failure
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
                Trace.trace(20,'{get_client '+repr((host,port))+" "+repr(sockt)+" "+repr((hostname,ha,hi)))
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
        r, w, x = cleanUDP.Select([sock],[],[sock],0)
        Trace.trace(20,'empty_socket select r,w,x='+repr(r)+' '+repr(w)+' '+repr(x))
        if r:
            rcount = rcount+1
            if rcount%rcountalert == 0:
                Trace.trace(4,"empty_socket: r from select - count="+repr(rcount))
                reply , server = sock.recvfrom(TRANSFER_MAX)
                Trace.trace(10,"empty_socket read from "+repr(server)+":"+repr(reply))
	elif x or w :
	    raise "imposible to get these set w/out [r]"
        else:
            xcount = xcountmax # nothing to read - no more retries
    return

def send_socket( sock, message, address ):
    Trace.trace(20,'{send_socket')
    return sock.sendto( message, address )

def wait_rsp( sock, address, rcv_timeout ):
    # init return vals
    reply=''
    server=''

    r, w, x = cleanUDP.Select( [sock], [], [sock], rcv_timeout )
    Trace.trace(20,'wait_rsp select r,w,x='+repr(r)+' '+repr(w)+' '+repr(x))
    if r:
	reply , server = sock.recvfrom( TRANSFER_MAX )
    elif x or w :
	Trace.trace( 0, "send: exception on select after send to "
		        +repr(address)+" "+repr(x) )
	Trace.trace(0,"send"+str(sys.exc_info()[0])+str(sys.exc_info()[1]))
	generic_cs.enprint("UDPClient.send: exception on select after send to "+\
	      repr(address)+" "+repr(x)+" "+str(sys.exc_info()[0])+" "+\
	      str(sys.exc_info()[1]))
	raise "impossible to get these set w/out [r]"
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
        Trace.trace(10,'{__init__ udpclient '+repr((host,port,socket)))
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
            Trace.trace(20,'ENSTORE_UDP_PP found in environment'+repr(x))
	    self.pp = 1
	except:
	    self.pp = 0
        Trace.trace(10,'}__init__ udpclient '+repr(self.ident))

    def __del__(self):
        # tell file clerk we're done - this allows it to delete our unique id in
        # its dictionary - this keeps things cleaner & stops memory from growing
        for server in self.where_sent.items() :
            #generic_cs.enprint("clearing "+server[0]+" "+ server[1])
            try:
                self.send_no_wait({"work":"done_cleanup"}, server[0])
            except:
                pass

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
		    number,  out, time  = eval(reply)   ##XXX
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
		    ident, number,  out, time  = eval(reply)
                    Trace.trace(20,'goofy test:'+repr((ident,number,out,time)))
                # catch any error and keep going. server needs to be robust
                except:
                    Trace.log(e_errors.ERROR,"unexpected exception in udp_client:send "+
                              str(sys.exc_info()[0])+" "+str(sys.exc_info()[1]))
		    raise sys.exc_info()[0],sys.exc_info()[1]

		# now (after receive), check...
		if number != self.number :
                    #generic_cs.enprint(repr(type(number))+" "+repr(type(self.number)))
		    msg="UDPClient.send: stale_number=%s number=%s" %\
			 (number,self.number)
		    Trace.trace(21,'send stale='+repr(number)+' want='+repr(self.number))
                    if 0:
                        generic_cs.enprint(msg+" resending to "+repr(address)+message)

	    elif tries!=0 and ntries>=tries:  # no reply after requested tries
                Trace.trace(10,"send quiting,no reply after tries="+repr(ntries))
		raise errno.errorcode[errno.ETIMEDOUT]
            else:
                Trace.trace(6,"send no reply after tries="+repr(ntries))

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
        self.verbose = 0           # no output yet
	self.sendhost="localhost"
	self.sendport=9998
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
