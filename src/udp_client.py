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
import binascii

# enstore imports
import interface
import timeofday
import Trace

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
    (host,ha,hi) = socket.gethostbyaddr(socket.gethostname())
    port1 = 7600
    port2 = 7700
    while  1:
        for port in range (port1, port2) : # range (7600, 7600) has 0 members...
            success, sockt = try_a_port (host, port)
            if success :
                Trace.trace(20,'{get_client '+repr((host,port))+" "+repr(sockt))
                return host, port, sockt
        Trace.trace(0,'{get_client sleeping for 10 - all ports used '+\
                    repr((port1, port2)))
        time.sleep(10) # tried all ports, try later.



class UDPClient:

    def __init__(self, host="", port=0, socket=0):
        Trace.trace(10,'__init__ udpclient '+repr((host,port,socket)))
        if host == "":
            host, port, self.socket = get_client()
        else:
            self.socket = socket
        self.number = 0
        self.ident = "%s-%d-%f-%d" \
                     % (host, port, time.time(), os.getpid() )
        self.sendport = 7
        self.where_sent = {}
        Trace.trace(10,'{__init__ udpclient '+repr(self.ident))

    def __del__(self):
        # tell file clerk we're done - this allows it to delete our unique id in
        # its dictionary - this keeps things cleaner & stops memory from growing
        Trace.trace(10,'__del__ udpclient '+repr(self.ident))
        for server in self.where_sent.items() :
            #print "clearing ",server[0], server[1]
            try:
                self.send_no_wait({"work":"done_cleanup"}, server[0])
            except:
                pass
        Trace.trace(10,'__del__ udpclient')

    # this (generally) is received/processed by dispatching worker
    def send(self, text, address) :
        Trace.trace(20,'send add='+repr(address)+' text='+repr(text))
        # make a new message number - response needs to match this number
        self.number = self.number + 1

        # keep track of where we are sending things so we can clean up later
        self.where_sent[address] = (self.ident,text)

        # CRC text
        body = `(self.ident, self.number, text)`
        crc = binascii.crc_hqx(body, 0)
        # stringify message and check if it is too long
        message = `(body, crc)`
        #print message
        if len(message) > TRANSFER_MAX :
            Trace.trace(0,"send:message "+\
                        "too big. Size = "+repr(len(message))+" Max = "+\
                        repr(TRANSFER_MAX)+" "+repr(message))
            raise errno.errorcode[errno.EMSGSIZE],"UDPClient.send:message "+\
                  "too big. Size = "+repr(len(message))+" Max = "+\
                  repr(TRANSFER_MAX)+" "+repr(message)

        # make sure the socket is empty before we start
        try:
            f = self.socket.fileno()
            r, w, x = select.select([f],[],[f],0)
            if r:
                badsock = self.socket.getsockopt(socket.SOL_SOCKET,
                                                 socket.SO_ERROR)
                if badsock != 0 :
                    Trace.trace(0,"send pre recv, clearout error "+\
                                repr(errno.errorcode[badsock]))
                    print "udp_client pre recv, clearout error:",\
                          errno.errorcode[badsock]
                reply , server = self.socket.recvfrom(TRANSFER_MAX)
                badsock = self.socket.getsockopt(socket.SOL_SOCKET,
                                                 socket.SO_ERROR)
                if badsock != 0 :
                    Trace.trace(0,"send post recv, clearout error"+\
                                repr(errno.errorcode[badsock]))
                    print "udp_client post recv, clearout error:",\
                          errno.errorcode[badsock]
        except:
            Trace.trace(0,'send clearout err'+str(sys.exc_info()[0])+\
                        str(sys.exc_info()[1]))
            print "clearout",sys.exc_info()[0],sys.exc_info()[1]

        # send the udp message until we get a response that it was sent
        number = 0  # impossible number
        while number != self.number:
            badsock = self.socket.getsockopt(socket.SOL_SOCKET,socket.SO_ERROR)
            if badsock != 0 :
                Trace.trace(0,"send pre send"+ \
                            repr(errno.errorcode[badsock]))
                print "udp_client send, pre-sendto error:", \
                      errno.errorcode[badsock]
            sent = 0
            while sent == 0:
                try:
                    self.socket.sendto(message, address)
                    sent = 1
                except socket.error:
                    Trace.trace(0,'send: Nameserver not responding add='+\
                                repr(address)+\
                                str(sys.exc_info()[0])+str(sys.exc_info()[1]))
                    print timeofday.tod(),\
                          "udp_client: Nameserver not responding\n",\
                          message,"\n",address,"\n",\
                          sys.exc_info()[0],"\n", sys.exc_info()[1]
                    time.sleep(10)
            badsock = self.socket.getsockopt(socket.SOL_SOCKET,socket.SO_ERROR)
            if badsock != 0 :
                Trace.trace(0,"send post send"+ \
                            repr(errno.errorcode[badsock]))
                print "udp_client send, post-sendto error:", \
                  errno.errorcode[badsock]

            # check for a response
            f = self.socket.fileno()
            r, w, x = select.select([f],[],[f],10)

            # exception mean trouble
            if x :
                Trace.trace(0,"send: exception on select after send to "+\
                      repr(address)+" "+repr(x))
                Trace.trace(0,"send"+str(sys.exc_info()[0])+\
                            str(sys.exc_info()[1]))
                print "UDPClient.send: exception on select after send to "+\
                      repr(address)+" "+repr(x)
                print sys.exc_info()[0],sys.exc_info()[1]

            # something there - read it and see if we have response that
            # matches the number we sent out
            if r :
                badsock = self.socket.getsockopt(socket.SOL_SOCKET,
                                                 socket.SO_ERROR)
                if badsock != 0 :
                    Trace.trace(0,"send pre recv"+ \
                                repr(errno.errorcode[badsock]))
                    print "udp_client send, pre-recv error:",\
                          errno.errorcode[badsock]
                reply , server = self.socket.recvfrom(TRANSFER_MAX)
                badsock = self.socket.getsockopt(socket.SOL_SOCKET,
                                                 socket.SO_ERROR)
                if badsock != 0 :
                    Trace.trace(0,"send pre recv"+ \
                                repr(errno.errorcode[badsock]))
                    print "udp_client send, post-recv error:",\
                          errno.errorcode[badsock]
                try:
                    exec ("number,  out, time  = "  + reply)
                # did we read entire message (bigger than TRANSFER_MAX?)
                except exceptions.SyntaxError :
                    Trace.trace(0,"send disaster: didn't read entire message"+\
                                "server="+repr(server)+" "+\
                                str(sys.exc_info()[0])+str(sys.exc_info()[1]))
                    print "disaster: didn't read entire message"
                    print "reply:",reply
                    print "server:",server
                    raise sys.exc_info()[0],sys.exc_info()[1]
                # goofy test feature - need for client being echo service only
                except exceptions.ValueError :
                    Trace.trace(0,'send GOOFY TEST FEATURE')
                    exec ("ident, number,  out, time  = "  + reply)
                if number != self.number :
                    Trace.trace(21,'send stale='+repr(number)+' want='+\
                                repr(self.number))
                    print "UDPClient.send: stale_number=",number, "number=", \
                          self.number,"resending to ", address, message
            else :
                #print "UDPClient.send: resending to ", address, message
                pass
        Trace.trace(20,"}send "+repr(out))
        return out

    # send message without waiting for reply and resend
    def send_no_wait(self, text, address) :
        Trace.trace(20,'send_no_wait add='+repr(address)+' text='+repr(text))
        # make a new message number - response needs to match this number
        self.number = self.number + 1

        # CRC text
        body = `(self.ident, self.number, text)`
        crc = binascii.crc_hqx(body, 0)
        # stringify message and check if it is too long
        message = `(body, crc)`

        if len(message) > TRANSFER_MAX :
            Trace.trace(0,"send_no_wait:message "+\
                        "too big. Size = "+repr(len(message))+" Max = "+\
                        repr(TRANSFER_MAX)+" "+repr(message))
            raise errorcode[EMSGSIZE],"UDPCl.send_n_w:message too big.Size = "\
                  +repr(len(message))+" Max = "+repr(TRANSFER_MAX)+" ",message

        # send the udp message
        badsock = self.socket.getsockopt(socket.SOL_SOCKET,socket.SO_ERROR)
        if badsock != 0 :
            Trace.trace(0,"send_no_wait pre sendto"+ \
                        repr(errno.errorcode[badsock]))
            print "udp_client send_no_wait, pre-sendto error:", \
                  errno.errorcode[badsock]
        sent = 0
        while sent == 0:
            try:
                self.socket.sendto(message, address)
                sent = 1
            except socket.error:
                Trace.trace(0,'send: Nameserver not responding add='+\
                            repr(address)+\
                            str(sys.exc_info()[0])+str(sys.exc_info()[1]))
                print timeofday.tod(),\
                      "udp_client (no wait): Nameserver not responding\n",\
                      message,"\n",address,"\n",\
                      sys.exc_info()[0],"\n", sys.exc_info()[1]
                time.sleep(1)
        badsock = self.socket.getsockopt(socket.SOL_SOCKET,socket.SO_ERROR)
        if badsock != 0 :
            Trace.trace(0,"send_no_wait post send"+ \
                        repr(errno.errorcode[badsock]))
            print "udp_client send_no_wait, post-sendto error:", \
                  errno.errorcode[badsock]
        Trace.trace(20,'}send_no_wait')

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
        return self.list_options() +\
               ["msg=","host=","port="] +\
               self.help_options()

if __name__ == "__main__" :
    import pprint

    status = 0

    # fill in the interface
    intf = UDPClientInterface()

    # get a UDP client
    u = UDPClient(intf.host, intf.port, intf.socket)

    #pprint.pprint(u.__dict__)

    if intf.list:
        print "Sending:\n",intf.msg,"\nto",intf.sendhost,intf.sendport,"with calback on",intf.port
    back = u.send(intf.msg, (intf.sendhost, intf.sendport))

    if back != intf.msg :
        print "Error: sent:\n",intf.msg,"\nbut read:\n",back
        status = status|1

    elif intf.list:
        print "Read back:\n",back

    sys.exit(status)
