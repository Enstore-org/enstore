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
import Trace
import checksum 

class RawUDP:
    
    def __init__(self, receive_timeout=60.):
        self.max_packet_size = 16384
        self.rcv_timeout = receive_timeout   # timeout for get_request in sec.
        self._lock = threading.Lock()
        self.arrived = threading.Event()
        self.queue_size = 0L
        self.buffer = []
        self.movers = {}
        
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
        

    def is_mover_request(self, request):
        if type(request) == type(""):
            start_index =  request.find("'mover':")
            if start_index != -1:
                # extract mover name
                end_index =request.find(",", start_index)
                if end_index > start_index:
                    substr = request[start_index:end_index]
                    return substr.split(":")[1]
        return None
        
    def put(self, message):
        req = message[2]
        client_addr = (message[0], message[1])
        request=None
        try:
            request, inCRC = udp_common.r_eval(req)
            # calculate CRC
            crc = checksum.adler32(0L, request, len(request))
            if (crc != inCRC) :
                Trace.log(e_errors.INFO,
                          "BAD CRC request: %s " % (request,))
                Trace.log(e_errors.INFO,
                          "CRC: %s calculated CRC: %s" %
                          (repr(inCRC), repr(crc)))
            
                request=None
        except ValueError, detail:
            # must be an event relay message
            # it has a different format
            try:
                request = udp_common.r_eval(req)
            except:
                exc, msg = sys.exc_info()[:2]
                # reraise exception
                raise exc, msg
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

        if not request:
            return

        mover_request = self.is_mover_request(request)
            
        self._lock.acquire()
        if mover_request:
            # put the latest request into the queue
            if self.movers.has_key(mover_request) and self.movers[mover_request] !="":
                self.buffer.remove(self.movers[mover_request])
                self.movers[mover_request] = (message[0], message[1], request)
                
        self.queue_size = self.queue_size + 1
        self.buffer.append((message[0], message[1], request))
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
              mover_request = self.is_mover_request(ret[2])
              if self.movers.has_key(mover_request) and self.movers[mover_request] !="":
                  self.movers[mover_request] = ""
             
              self._lock.release()
              return ret[0], ret[1], ret[2]
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
                        #print "rawUDP:REQ", req

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
