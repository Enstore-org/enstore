import socket
import time
import select
from errno import *

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
    #host = socket.gethostname()
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
        self.ident = "%s-%d-%d" % (self.host, self.port, long(time.time()) )

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
        while not number == self.number:
            self.socket.sendto (message, address)

            # check for a response
            f  = self.socket.fileno()
            r, w, x = select.select([f],[],[f],10)

            # exception mean trouble
            if x :
                raise errorcode[ESHUTDOWN],"UDPClient.send: exception on select "\
                      +"after send to "+repr(address)+" peer exitted"

            # something there - read it and see if we have response that matches
            # the number we sent out
            if r :
                reply , server = self.socket.recvfrom(TRANSFER_MAX)
                exec ("number,  out  = "  + reply)

        return out


if __name__ == "__main__" :
    u = UDPClient()
    print u.send("all dogs have fleas", ('localhost', 7550))
    print u.send("all dogs have fleas", ('localhost', 7550))
