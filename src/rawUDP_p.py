#!/usr/bin/env python

###############################################################################
#
# $Id$
# This module uses processes and requires python 2.6 and latter
###############################################################################
import sys
import multiprocessing
import fcntl
import socket
import os
import pwd
import threading
import time

import cleanUDP
import udp_common
import checksum
import Trace



DEBUG = False
#DEBUG = True
MAX_PACKET_SIZE = 16384
MAX_QUEUE_SIZE = 200000

# get request id
def get_id(request):
    # request is a string like
    # ('131.225.13.187-42240-1240336133.248870-25246-134719808', 13L, {'work': 'alive'})
    rarr = request.split("'")
    try:
        # the fist element is a first part of request id
        # the second: sequential request number
        # we do not need it
        return rarr[1]
    except:
        # wrong message format
        return None

def _print(f, msg):
    if DEBUG and f:
        f.write("%s %s\n"%(time.time(),msg))
        f.flush()
    
class RawUDP:
    
    def __init__(self, receive_timeout=60.):
        self.max_packet_size = MAX_PACKET_SIZE
        self.rcv_timeout = receive_timeout   # timeout for get_request in sec.
        self.manager = multiprocessing.Manager()
        self._lock = self.manager.Lock()
        self.arrived = self.manager.Event()
        self.buffer = self.manager.list()
        self.requests = self.manager.dict()
        self.queue_size_p = self.manager.Value("l",0L)
        self.queue_size = 0L

        
    def init_port(self, port):
        self.socket_type = socket.SOCK_DGRAM
        self.address_family = socket.AF_INET
        ip, port, self.server_socket = udp_common.get_callback('', port)
        self.server_address = (ip, port)
        
        # set this socket to be closed in case of an exec
        if self.server_socket != None:
            fcntl.fcntl(self.server_socket.fileno(), fcntl.F_SETFD, fcntl.FD_CLOEXEC)

    def init_socket(self, socket):
        self.server_socket = socket
        

    # get message from FIFO buffer
    # return value:
    # (request, client_address)
    # where client_address = (client_ip, client_port) 
    def get(self):
        _print(self.d_o, "GET")
        rc = None
        if self.queue_size_p.value == 0:
            self.arrived.wait()
            self.arrived.clear()
        if self.queue_size_p.value > 0:
            self._lock.acquire()
            try:
                ret = self.buffer.pop(0)
                request_id = get_id(ret[0])
                self.queue_size_p.value = self.queue_size_p.value - 1
                self.queue_size = self.queue_size_p.value
                if self.requests.has_key(request_id):
                    del(self.requests[request_id])
            except IndexError, detail:
                print "IndexError", detail
            except:
                pass
            rc = ret
            self._lock.release()
            _print(self.d_o, "GET %s %s"%(self.queue_size_p.value, request_id,))
        return rc
        

    def receiver(self):
        print "STARTING RECEIVER", os.getpid()
        proc = multiprocessing.Process(target=_receiver, args = (self,))
        try:
            proc.start()
            #proc.join()
        except:
            exc, detail, tb = sys.exc_info()
            print detail

    def set_out_file(self):
        if DEBUG:
            thread = threading.currentThread()
            thread_name = thread.getName()
            dirpath = os.path.join(os.environ.get("ENSTORE_OUT", ""),"tmp/%s"%(pwd.getpwuid(os.geteuid())[0],))
            if not os.path.exists(dirpath):
                os.makedirs(dirpath)
            self.d_o = open(os.path.join(dirpath, "gp_%s_%s"%(os.getpid(), thread_name)), "w")
        else:
            self.d_o = None

        
def put(lock, event, buffer, queue_size, message, requests, f):
    _print (f, "QUEUE SIZE %s msg %s"%(queue_size.value, message))
    if queue_size.value > MAX_QUEUE_SIZE:
        # drop incoming message
        return
    # message structure:
    #(request, cliient_address)
    req = message[0]
    client_addr = (message[1])
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
            request = udp_common.r_eval(req, check=True)
        except:
            exc, msg = sys.exc_info()[:2]
            print "PUT: exc %s %s" % (exc, msg)
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
    #_print (f, "REQUEST %s"% (request,)) 
    lock.acquire()
    try:
        request_id = get_id(request)
        do_put = True
        if request_id:
            # put the latest request into the queue
            if requests.has_key(request_id) and requests[request_id] !="":
                # most like this is a retry
                _print (f, "DUPLICATE %s %s" % (time.time(), request_id))
                try:
                    # "retry" message put it closer to the beginnig of the queue
                    index = buffer.index(requests[request_id])
                    # new index is in 10% of top messages
                    new_index = index / (((queue_size.value + 1)/10)+1) + index % 10
                    if new_index >= index:
                        new_index = index
                    _print(f, "FOUND at %s reinserting at %s queue size %s"%(index, new_index, queue_size.value))
                    buffer.remove(requests[request_id])
                    buffer.insert(new_index, (request, message[1]))
                    do_put = False # duplicate request, do not put into the queue
                except ValueError, detail:
                    _print(f,"put: Exception:ValueError %s removing %s"%(detail,request_id))  

            else:
                requests[request_id] = (request, message[1])

        if do_put:
            t0 = time.time()

            buffer.append((request, message[1]))
            queue_size.value = queue_size.value + 1
            event.set()
            _print (f, "PUT %s %s %s"% (time.time(), request_id, time.time() - t0))
    except:
        exc, detail, tb = sys.exc_info()
        print exc, detail
    lock.release()

        
# receiver runs in a separate process
def _receiver(self):
    print "I am rawUDP_p", os.getpid()
    if DEBUG:
        thread = threading.currentThread()
        thread_name = thread.getName()
        dirpath = os.path.join(os.environ.get("ENSTORE_OUT", ""),"tmp/%s"%(pwd.getpwuid(os.geteuid())[0],))
        if not os.path.exists(dirpath):
            os.makedirs(dirpath)
        self.f = open(os.path.join(dirpath, "p_%s_%s"%(os.getpid(), thread_name)), "w")
    else:
        self.f = None
    
    print"RECEIVER %s STARTS on %s"%(os.getpid(), self.server_socket.getsockname(),)
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
                        message = (req, client_addr)
                        #print "MESSAGE", time.time(), message
                        put(self._lock, self.arrived, self.buffer, self.queue_size_p, message, self.requests, self.f)
        else:
            # time out
            # set event to allow get to proceed
            # this can be used in dispatching worker to run interva functions
            self.arrived.set()

if __name__ == "__main__":
    rs = RawUDP(7700)
    
    rs.receiver()
