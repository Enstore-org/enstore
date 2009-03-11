#!/usr/bin/env python

###############################################################################
#
# $Id$
# This module uses processes and requires python 2.6 and better
###############################################################################
import sys
import multiprocessing
import ctypes
import fcntl
import socket
import os

import cleanUDP
import udp_common
import os
import checksum
import Trace
import time

MAX_PACKET_SIZE = 16384

def get_id(request):
    rarr = request.split("'")
    try:
        return rarr[1]
    except:
        return None

def _print(f, msg):
    f.write("%s\n"%(msg,))
    f.flush()
    
class RawUDP:
    
    def __init__(self, receive_timeout=60.):
        #print "I am rawUDP_p", os.getpid()
        self.max_packet_size = MAX_PACKET_SIZE
        self.rcv_timeout = receive_timeout   # timeout for get_request in sec.
        self.manager = multiprocessing.Manager()
        #self._lock = multiprocessing.Lock()
        self._lock = self.manager.Lock()
        self.arrived = self.manager.Event()
        self.buffer = self.manager.list()
        self.requests = self.manager.dict()
        self.queue_size_p = self.manager.Value("l",0L)
        self.queue_size = 0L

        #print "QUEUE SIZE", self.buffer.qsize()
        
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
        rc = None
        if self.queue_size_p.value == 0:
            self.arrived.wait()
            #print "ARRIVED"
            self.arrived.clear()
            #print "QUEUE SIZE", self.queue_size_p.value
        if self.queue_size_p.value > 0:
            #print "AAAA"
            t0 = time.time()
            self._lock.acquire()
            try:
                ret = self.buffer.pop(0)
                request_id = get_id(ret[2])
                self.queue_size_p.value = self.queue_size_p.value - 1
                self.queue_size = self.queue_size_p.value
                #print "REQ_ID", request_id
                if self.requests.has_key(request_id):
                    #print "DELETE ID",request_id 
                    del(self.requests[request_id])
            except IndexError, detail:
                print "IndexError", detail
            except:
                pass
            rc = ret[0], ret[1], ret[2]
            self._lock.release()
            #print "GET", time.time(), request_id, time.time()-t0
        return rc
        

    def receiver(self):
        #thread = threading.Thread(group=None, target='_receiver', args=(), kwargs={})
        proc = multiprocessing.Process(target=_receiver, args = (self,))
        try:
            proc.start()
            #proc.join()
        except:
            exc, detail, tb = sys.exc_info()
            print detail
        
def put(lock, event, buffer, queue_size, message, requests):
    #print "REQUESTS", requests
    req = message[2]
    client_addr = (message[0], message[1])
    request=None
    #print "MESSAGE", type(req), message
    try:
        request, inCRC = udp_common.r_eval(req, check=True)
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
            request = udp_common.r_eval(req, check=True)
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
        request = None

    if not request:
        return

    request_id = get_id(request)
    lock.acquire()
    t0 = time.time()
    request_id = get_id(request)
    do_put = True
    if request_id:
        # put the latest request into the queue
        if requests.has_key(request_id) and requests[request_id] !="":
            print "DUPLICATE",time.time(), request_id 
            do_put = False # duplicate request, do not put into the queue
        else:
            requests[request_id] = (message[0], message[1], request)
    
    if do_put:
        t0 = time.time()
        
        buffer.append((message[0], message[1], request))
        queue_size.value = queue_size.value + 1
        event.set()
        #print "PUT", time.time(), request_id, time.time() - t0
    lock.release()

        

def _receiver(self):
    #print "RECEIVER PROCESS", os.getpid()
    rcv_timeout = self.rcv_timeout
    while 1:

        #r = [self.server_socket]

        #print "wait"
        r, w, x, remaining_time = cleanUDP.Select([self.server_socket], [], [], rcv_timeout)
        #print "got it", r, w, self.server_socket
        #print "got it", self.queue_size_p.value

        if r:
            for fd in r:
                if fd == self.server_socket:
                    req, client_addr = self.server_socket.recvfrom(
                        self.max_packet_size, self.rcv_timeout)
                    #print "RAW REQ", req

                    if req:
                        message = (client_addr[0], client_addr[1], req)
                        #print "MESSAGE", message
                        put(self._lock, self.arrived, self.buffer, self.queue_size_p, message, self.requests)
        else:
            # time out
            # set event to allow get to proceed
            # this can be used in dispatching worker to run interva functions
            self.arrived.set()

if __name__ == "__main__":
    rs = RawUDP(7700)
    
    rs.receiver()
