import socket
import time
import select
import os
import errno
import exceptions
from errno import *
import sys

TRANSFER_MAX=1024

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
    host = 'localhost'
    #(host,ha,hi) = socket.gethostbyaddr(socket.gethostname())
    while  1:
        for port in range (7600, 7700) : # range (7600, 7600) has 0 members...
            success, sockt = try_a_port (host, port)
            if success :
                return host, port, sockt
        sleep (10) # tried all ports, try later.



class UDPClient:

    def __init__(self):
        self.number = 0
        self.host, self.port, self.socket = get_client()
        self.ident = "%s-%d-%f-%d" \
                     % (self.host, self.port, time.time(), os.getpid() )

    # this (generally) is received/processed by dispatching worker
    def send(self, text, address) :
        # make a new message number - response needs to match this number
        self.number = self.number + 1

        # stringify message and check if it is too long
        message = `(self.ident, self.number, text)`
        if len(message) > TRANSFER_MAX :
            raise errorcode[EMSGSIZE],"UDPClient.send:message too big.Size = "\
                  +repr(len(message))+" Max = "+repr(TRANSFER_MAX)+" ",message

        # send the udp message until we get a response that it was sent
        number = 0  # impossible number
        while number != self.number:
            self.socket.sendto (message, address)

            # check for a response
            f  = self.socket.fileno()
            r, w, x = select.select([f],[],[f],10)

            # exception mean trouble
            if x :
                raise errorcode[ESHUTDOWN],"UDPClient.send: exception on "\
                      +"select after send to "+repr(address)+" peer exitted"

            # something there - read it and see if we have response that
            # matches the number we sent out
            if r :
                reply , server = self.socket.recvfrom(TRANSFER_MAX)
                try :
                    exec ("number,  out  = "  + reply)
                # did we read entire message (bigger than TRANSFER_MAX?)
                except exceptions.SyntaxError :
                    print "disaster: probably didn't read entire message"
                    print "reply:",reply
                    print "server:",server
                    raise sys.exc_info()[0],sys.exc_info()[1]
                # goofy test feature - need for client being echo service only
                except exceptions.ValueError :
                    exec ("ident, number,  out  = "  + reply)
                if number != self.number :
                    print "UDPClient.send: stale_number=",number, "number=", \
                          self.number,"resending to ", address, message
            else :
                print "UDPClient.send: resending to ", address, message
        return out


if __name__ == "__main__" :
    import getopt
    import socket
    import string
    import pprint

    status = 0

    # defaults
    msg = "All dogs have fleas, but cats make you sick!"
    host = "localhost"
    port = 7
    list = 0

    # see what the user has specified. bomb out if wrong options specified
    options = ["msg=","host=","port=","list","help"]
    optlist,args=getopt.getopt(sys.argv[1:],'',options)
    for (opt,value) in optlist :
        if opt == "--msg" :
            msg = value
        elif opt == "--host" :
            host = value
        elif opt == "--port" :
            port = string.atoi(value)
        elif opt == "--list" :
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
