#!/usr/bin/env python

###############################################################################
#
# $Id$
# This module uses processes and requires python 2.6 and better
###############################################################################
import sys
import multiprocessing
import fcntl
import socket
import cleanUDP
import udp_common
import os

MAX_PACKET_SIZE = 16384

class RawUDP:
    
    def __init__(self, receive_timeout=60.):
        print "I am rawUDP_p", os.getpid()
        self.max_packet_size = MAX_PACKET_SIZE
        self.rcv_timeout = receive_timeout   # timeout for get_request in sec.
        self._lock = multiprocessing.Lock()
        self.arrived = multiprocessing.Event()
        self.queue_size = multiprocessing.Value("l",0L)
        self.buffer = multiprocessing.Queue()
        print "QUEUE SIZE", self.buffer.qsize()
        
    def init_port(self, port):
        self.socket_type = socket.SOCK_DGRAM
        self.address_family = socket.AF_INET
        ip, port, self.server_socket = udp_common.get_callback('', port)
        self.server_address = (ip, port)
        #print "addr %s sock %s"%(self.server_address, self.server_socket)
        
        # set this socket to be closed in case of an exec
        if self.server_socket != None:
            fcntl.fcntl(self.server_socket.fileno(), fcntl.F_SETFD, fcntl.FD_CLOEXEC)

    def init_socket(self, socket):
        self.server_socket = socket
        

    # get message from FIFO buffer
    # return values:
    # client_ip
    # client port
    # request
    # number of requests in the buffer
    def get(self):
        if self.queue_size.value == 0:
            self.arrived.wait()
            #print "ARRIVED"
            self.arrived.clear()
            #print "QUEUE SIZE", self.queue_size.value
        if self.queue_size.value > 0:
              self._lock.acquire()
              ret = self.buffer.get()
              self.queue_size.value = self.queue_size.value - 1 
              self._lock.release()
              return ret[0], ret[1], ret[2], self.queue_size.value
        else:
            return None
        

    def receiver(self):
        #thread = threading.Thread(group=None, target='_receiver', args=(), kwargs={})
        proc = multiprocessing.Process(target=_receiver, args = (self,))
        try:
            proc.start()
            #proc.join()
        except:
            exc, detail, tb = sys.exc_info()
            print detail
        
def put(lock, event, buffer, queue_size, message):
        req = message[2]
        request=None
        try:
            request, inCRC = udp_common.r_eval(req)
        except (SyntaxError, TypeError):
            #If TypeError occurs, keep retrying.  Most likely it is
            # an "expected string without null bytes".
            #If SyntaxError occurs, also keep trying, most likely
            # it is from and empty UDP datagram.
            exc, msg = sys.exc_info()[:2]
            try:
                message = "%s: %s: From client %s:%s" % \
                          (exc, msg, client_addr, request[:100])
            except IndexError:
                message = "%s: %s: From client %s: %s" % \
                          (exc, msg, client_addr, request)
            Trace.log(5, message)

            #Set these to something.
            request, inCRC = (None, None)

        if request == None:
            return (request, client_addr)
        # calculate CRC
        crc = checksum.adler32(0L, request, len(request))
        if (crc != inCRC) :
            Trace.log(e_errors.INFO,
                      "BAD CRC request: %s " % (request,))
            Trace.log(e_errors.INFO,
                      "CRC: %s calculated CRC: %s" %
                      (repr(inCRC), repr(crc)))

            request=None
        if not request:
            return

    buffer.put((message[0], message[1], request))
    lock.acquire()
    queue_size.value = queue_size.value + 1
    lock.release()
    event.set()
        

def _receiver(self):
    print "RECEIVER PROCESS", os.getpid()
    rcv_timeout = self.rcv_timeout
    while 1:

        #r = [self.server_socket]

        #print "wait"
        r, w, x, remaining_time = cleanUDP.Select([self.server_socket], [], [], rcv_timeout)
        #print "got it", r, w, self.server_socket
        #print "got it", self.queue_size.value

        if r:
            for fd in r:
                if fd == self.server_socket:
                    req, client_addr = self.server_socket.recvfrom(
                        self.max_packet_size, self.rcv_timeout)
                   # print "REQ", req

                    if req:
                        message = (client_addr[0], client_addr[1], req)
                        put(self._lock, self.arrived, self.buffer, self.queue_size, message)
        else:
            # time out
            # set event to allow get to proceed
            # this can be used in dispatching worker to run interva functions
            self.arrived.set()

if __name__ == "__main__":
    rs = RawUDP(7700)
    
    rs.receiver()
