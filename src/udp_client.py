import socket
import time
import timeofday
import select
import os
import errno
import exceptions
import errno
import sys

TRANSFER_MAX=16384

# see if we can allocate a specific port on a specific host
def try_a_port(host, port) :
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(host, port)
    except:
        sock.close()
        return (0, sock) # failure
    return (1, sock)     # success


# try to get a port from a range of possibilities
def get_client() :
    #host = 'localhost'
    (host,ha,hi) = socket.gethostbyaddr(socket.gethostname())
    while  1:
        for port in range (7600, 7700) : # range (7600, 7600) has 0 members...
            success, sockt = try_a_port (host, port)
            if success :
                return host, port, sockt
        time.sleep(10) # tried all ports, try later.



class UDPClient:

    def __init__(self):
        self.number = 0
        self.host, self.port, self.socket = get_client()
        self.ident = "%s-%d-%f-%d" \
                     % (self.host, self.port, time.time(), os.getpid() )
        self.where_sent = {}


    def __del__(self):
        # tell file clerk we are done - this allows it to delete our unique id in
        # its dictionary - this keeps things cleaner and stops memory from growing
        for server in self.where_sent.items() :
            #print "clearing ",server[0], server[1]
	    try:
		self.send_no_wait({"work":"done_cleanup"}, server[0])
	    except:
		pass

    # this (generally) is received/processed by dispatching worker
    def send(self, text, address) :
        # make a new message number - response needs to match this number
        self.number = self.number + 1

        # keep track of where we are sending things so we can clean up later
        self.where_sent[address] = (self.ident,text)

        # stringify message and check if it is too long
        message = `(self.ident, self.number, text)`
        ##print message
        if len(message) > TRANSFER_MAX :
            raise errno.errorcode[errno.EMSGSIZE],"UDPClient.send:message "+\
                  "too big. Size = ",+repr(len(message))+" Max = "+\
                  repr(TRANSFER_MAX)+" "+repr(message)

        # make sure the socket is empty before we start
        try:
            f = self.socket.fileno()
            r, w, x = select.select([f],[],[f],0)
            if r:
                badsock = self.socket.getsockopt(socket.SOL_SOCKET,
                                                 socket.SO_ERROR)
                if badsock != 0 :
                    print "udp_client send, clearout error:",\
                          errno.errorcode[badsock]
                reply , server = self.socket.recvfrom(TRANSFER_MAX)
                print "udp_client.send: read old info:",reply,server
                badsock = self.socket.getsockopt(socket.SOL_SOCKET,
                                                 socket.SO_ERROR)
                if badsock != 0 :
                    print "udp_client send, clearout error:",\
                          errno.errorcode[badsock]
        except:
            print "clearout",sys.exc_info()[0],sys.exc_info()[1]

        # send the udp message until we get a response that it was sent
        number = 0  # impossible number
        while number != self.number:
            badsock = self.socket.getsockopt(socket.SOL_SOCKET,socket.SO_ERROR)
            if badsock != 0 :
                print "udp_client send, pre-sendto error:", \
                      errno.errorcode[badsock]
            sent = 0
            while sent == 0:
                try:
                    self.socket.sendto(message, address)
                    sent = 1
                except socket.error:
                    print timeofday.tod(),\
                          "udp_client: Nameserver not responding\n",\
                          message,"\n",address,"\n",\
                          sys.exc_info()[0],"\n", sys.exc_info()[1]
                    time.sleep(10)
            badsock = self.socket.getsockopt(socket.SOL_SOCKET,socket.SO_ERROR)
            if badsock != 0 :
                print "udp_client send, post-sendto error:", \
                  errno.errorcode[badsock]

            # check for a response
            f = self.socket.fileno()
            r, w, x = select.select([f],[],[f],10)

            # exception mean trouble
            if x :
                print "UDPClient.send: exception on select after send to "+\
                      repr(address)+" "+repr(x)
                print sys.exc_info()[0],sys.exc_info()[1]

            # something there - read it and see if we have response that
            # matches the number we sent out
            if r :
                badsock = self.socket.getsockopt(socket.SOL_SOCKET,
                                                 socket.SO_ERROR)
                if badsock != 0 :
                    print "udp_client send, pre-recv error:",\
                          errno.errorcode[badsock]
                reply , server = self.socket.recvfrom(TRANSFER_MAX)
                badsock = self.socket.getsockopt(socket.SOL_SOCKET,
                                                 socket.SO_ERROR)
                if badsock != 0 :
                    print "udp_client send, post-recv error:",\
                          errno.errorcode[badsock]
                try :
                    exec ("number,  out, time  = "  + reply)
                # did we read entire message (bigger than TRANSFER_MAX?)
                except exceptions.SyntaxError :
                    print "disaster: didn't read entire message"
                    print "reply:",reply
                    print "server:",server
                    raise sys.exc_info()[0],sys.exc_info()[1]
                # goofy test feature - need for client being echo service only
                except exceptions.ValueError :
                    exec ("ident, number,  out, time  = "  + reply)
                if number != self.number :
                    print "UDPClient.send: stale_number=",number, "number=", \
                          self.number,"resending to ", address, message
            else :
                #print "UDPClient.send: resending to ", address, message
                pass
        return out

    # send message without waiting for reply and resend
    def send_no_wait(self, text, address) :
        # make a new message number - response needs to match this number
        self.number = self.number + 1

        # stringify message and check if it is too long
        message = `(self.ident, self.number, text)`
        if len(message) > TRANSFER_MAX :
            raise errorcode[EMSGSIZE],"UDPCl.send_n_w:message too big.Size = "\
                  +repr(len(message))+" Max = "+repr(TRANSFER_MAX)+" ",message

        # send the udp message
        badsock = self.socket.getsockopt(socket.SOL_SOCKET,socket.SO_ERROR)
        if badsock != 0 :
            print "udp_client send_no_wait, pre-sendto error:", \
                  errno.errorcode[badsock]
        sent = 0
        while sent == 0:
            try:
                self.socket.sendto(message, address)
                sent = 1
            except socket.error:
                print timeofday.tod(),\
                      "udp_client (no wait): Nameserver not responding\n",\
                      message,"\n",address,"\n",\
                      sys.exc_info()[0],"\n", sys.exc_info()[1]
                time.sleep(1)
        badsock = self.socket.getsockopt(socket.SOL_SOCKET,socket.SO_ERROR)
        if badsock != 0 :
            print "udp_client send_no_wait, post-sendto error:", \
                  errno.errorcode[badsock]

if __name__ == "__main__" :
    import getopt
    import socket
    import string
    import pprint

    status = 0

    # defaults
    msg = "All dogs have fleas, but cats make you sick!"
    #host = "localhost"
    (host,ca,ci) = socket.gethostbyaddr(socket.gethostname())
    port = 7
    list = 0

    # see what the user has specified. bomb out if wrong options specified
    options = ["msg=","host=","port=","list","verbose","help"]
    optlist,args=getopt.getopt(sys.argv[1:],'',options)
    for (opt,value) in optlist :
        if opt == "--msg" :
            msg = value
        elif opt == "--host" :
            host = value
        elif opt == "--port" :
            port = string.atoi(value)
        elif opt == "--list" or opt == "--verbose":
            list = 1
        elif opt == "--help" :
            print "python ",sys.argv[0], options
            print "   do not forget the '--' in front of each option"
            sys.exit(0)

    u = UDPClient()
    #pprint.pprint(u.__dict__)

    if list:
        print "Sending:\n",msg,"\nto",host,port,"with calback on",u.port
    back = u.send(msg, (host, port))

    if back != msg :
        print "Error: sent:\n",msg,"\nbut read:\n",back
        status = status|1

    elif list:
        print "Read back:\n",back

    sys.exit(status)
