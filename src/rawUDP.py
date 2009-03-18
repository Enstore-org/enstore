#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################
import sys
import os
import threading
import fcntl
import socket
import cleanUDP
import udp_common
import Trace
import checksum 
import time

DEBUG = False
MAX_PACKET_SIZE = 16384

def get_id(request):
    rarr = request.split("'")
    try:
        return rarr[1]
    except:
        return None
'''
def get_id(request):
    rarr = request.split(",")
    id=(rarr[0].strip(),rarr[1].strip()) 
    try:
        return id
    except:
        return None
'''

def _print(f, msg):
    if DEBUG:
        f.write("%s %s\n"%(time.time(),msg))
        f.flush()


class RawUDP:
    
    def __init__(self, receive_timeout=60.):
        self.max_packet_size = MAX_PACKET_SIZE
        self.rcv_timeout = receive_timeout   # timeout for get_request in sec.
        self._lock = threading.Lock()
        self.arrived = threading.Event()
        self.queue_size = 0L
        self.buffer = []
        self.requests = {} # this is to avoid multiple requests with the same id
        
        
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


    def put(self, message):
        req = message[2]
        client_addr = (message[0], message[1])
        request=None
        try:
            request, inCRC = udp_common.r_eval(req, check=True)
            # calculate CRC
            crc = checksum.adler32(0L, request, len(request))
            if (crc != inCRC) :
                print "BAD CRC request: %s " % (request,)
                print "CRC: %s calculated CRC: %s" % (repr(inCRC), repr(crc))
            
                request=None
        except ValueError, detail:
            # must be an event relay message
            # it has a different format
            try:
                request = udp_common.r_eval(req,check=True)
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

        request_id = get_id(request)

        self._lock.acquire()
        do_put = True
        print "PUT ENTERED", self.thread_name
        if request_id:
            # put the latest request into the queue
            if self.requests.has_key(request_id) and self.requests[request_id] !="":
                print "DUPLICATE",request 

                # "retry" message put it closer to the beginnig of the queue
                index = self.buffer.index(self.requests[request_id])
                new_index = index / (((self.queue_size + 1)/10)+1) + index % 10
                if new_index < index:
                    print "FOUND at %s reinserting at %s queue size %s"%(index, new_index, self.queue_size)
                    self.buffer.remove(self.requests[request_id])
                    self.buffer.insert(new_index, (message[0], message[1], request))
                do_put = False # duplicate request, do not put into the queue

            else:
                self.requests[request_id] = (message[0], message[1], request)
                
        if do_put:
            t0 = time.time()
            self.queue_size = self.queue_size + 1
            self.buffer.append((message[0], message[1], request))
            self.arrived.set()
            #print "PUT", time.time() - t0
        print "PUT EXITING", self.thread_name
        self._lock.release()

        
    # get message from FIFO buffer
    # return values:
    # client_ip
    # client port
    # request
    # number of requests in the buffer
    def get(self):
        rc = None
        if self.queue_size == 0:
            self.arrived.wait()
            self.arrived.clear()
        if self.queue_size > 0:
            t0 = time.time()
            self._lock.acquire()
            #self.arrived.clear()
            try:
                ret = self.buffer.pop(0)
                request_id = get_id(ret[2])
                self.queue_size = self.queue_size - 1
            
                if self.requests.has_key(request_id):
                    #print "DELETE ID",request_id 
                    del(self.requests[request_id])
            except IndexError, detail:
                print "IndexError", detail
            except:
                pass

            rc = ret[0], ret[1], ret[2]
            self._lock.release()
            #print "GET", time.time()-t0

        return rc
        
    
    def _receiver(self):
        rcv_timeout = self.rcv_timeout
        while 1:
            
            #r = [self.server_socket]

            print "wait"
            r, w, x, remaining_time = cleanUDP.Select([self.server_socket], [], [], rcv_timeout)
            #print "got it", r, w, self.server_socket
            print "got it", self.queue_size

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

    def set_out_file(self):
        thread = threading.currentThread()
        self.thread_name = thread.getName()
        os.getenv("ENSTORE_OUT", "")
        self.f1 = open(os.path.join(os.environ.get("ENSTORE_OUT", ""),"gp_%s_%s"%(os.getpid(),self.thread_name)), "w")

        
if __name__ == "__main__":
    rs = RawUDP(7700)
    
    rs.receiver()
