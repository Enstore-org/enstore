#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################
import sys
import threading
import fcntl
import socket
import cleanUDP
import udp_common


class RawUDP:
    
    def __init__(self, port, receive_timeout=60.):
        self.socket_type = socket.SOCK_DGRAM
        self.max_packet_size = 16384
        self.rcv_timeout = receive_timeout   # timeout for get_request in sec.
        self.address_family = socket.AF_INET
        self._lock = threading.Lock()
        self.arrived = threading.Event()
        self.queue_size = 0L

        ip, port, self.server_socket = udp_common.get_callback('', port)
        self.server_address = (ip, port)
        #print "addr %s sock %s"%(self.server_address, self.server_socket)
        
        # set this socket to be closed in case of an exec
        if self.server_socket != None:
            fcntl.fcntl(self.server_socket.fileno(), fcntl.F_SETFD, fcntl.FD_CLOEXEC)

        self.buffer = []

    def put(self, message):
        self._lock.acquire()
        self.buffer.append(message)
        self.queue_size = self.queue_size + 1
        self._lock.release()
        self.arrived.set()
        
    # get message from FIFO buffer
    # return values:
    # client_ip
    # client port
    # request
    # number of requests in the buffer
    def get(self):
        if self.queue_size == 0:
            self.arrived.wait()
            self.arrived.clear()
        if self.queue_size > 0:
              self._lock.acquire()
              #self.arrived.clear()
              ret = self.buffer.pop(0)
              self.queue_size = self.queue_size - 1 
              self._lock.release()
              return ret[0], ret[1], ret[2], self.queue_size
        else:
            return None
        
    
    def _receiver(self):
        rcv_timeout = self.rcv_timeout
        while 1:
            
            #r = [self.server_socket]

            #print "wait"
            r, w, x, remaining_time = cleanUDP.Select([self.server_socket], [], [], rcv_timeout)
            #print "got it", r, w, self.server_socket
            #print "got it", self.queue_size

            if r:
                for fd in r:
                    if fd == self.server_socket:
                        req, client_addr = self.server_socket.recvfrom(
                            self.max_packet_size, self.rcv_timeout)
                       # print "REQ", req

                        if req:
                            message = (client_addr[0], client_addr[1], req)
                            self.put(message)
            else:
                # time out
                # set event to allow get to proceed
                # this can be used in dispatching worker to run interva functions
                self.arrived.set()


    def receiver(self):
        #thread = threading.Thread(group=None, target='_receiver', args=(), kwargs={})
        thread = threading.Thread(group=None, target=self._receiver)
        try:
            thread.start()
        except:
            exc, detail, tb = sys.exc_info()
            print detail
        
if __name__ == "__main__":
    rs = RawUDP(7700)
    
    rs.receiver()
