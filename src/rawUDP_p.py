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
import enstore_constants

DEBUG = False
#DEBUG = True
MAX_PACKET_SIZE = enstore_constants.MAX_UDP_PACKET_SIZE
MAX_QUEUE_SIZE = 200000
SHUFFLE_THRESHOLD = 1000 # reinsert at the lower index beginning with this queue size

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

# get a keyword from message
# message is a str(dictionary)
def get_keyword(request, keyword):
    rarr = request.split("'")
    try:
        if keyword in rarr:
            index = rarr.index(keyword)
            return(rarr[index+2])
        else:
            return None
    except:
        exc, detail, tb = sys.exc_info()
        print "Exception getting keyword %s"%(detail,)
        
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
        self.print_queue = False
        self.enable_reinsert = True

        # if self.replace_keyword is specified
        # replace a message with this keyword in the buffer
        # this is needed for processing
        # mover requests require different aprroaach in their processing
        # they need to be aligned on the order they came and no duplicate
        # request is allowed.
        # Thus the old mover request gets replaced by the newer request
        # at the same place in the queue
        self.replace_keyword = None 

        
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
        

    def set_keyword(self, keyword):
        self.replace_keyword = keyword
        

    # disable reshuffling of duplicate requests
    # this can be beneficial for mover requests
    # but may hurt encp requests
    def disable_reshuffle(self):
        self.enable_reinsert = False
        
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
            ret = None
            request_id = None
            self._lock.acquire()
            try:
                ret = self.buffer.pop(0)
                request_id = get_id(ret[0])
                self.queue_size_p.value = self.queue_size_p.value - 1
                self.queue_size = self.queue_size_p.value
                if self.requests.has_key(request_id):
                    del(self.requests[request_id])
                keyword_to_replace = get_keyword(ret[0], self.replace_keyword)
                if self.requests.has_key(keyword_to_replace):
                    del(self.requests[keyword_to_replace])
                
            except IndexError, detail:
                print "IndexError", detail
            except:
                exc, detail, tb = sys.exc_info()
                Trace.trace(5, "GET:Exception %s"%(detail,))
                
                pass
            rc = ret
            self._lock.release()
            _print(self.d_o, "GET %s %s"%(self.queue_size_p.value, request_id,))
            self.last_get = time.time()

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

# put runs in a separate process (_receiver)
# this is why it is outside of RawUDP class
        
def put(lock, event, buffer, queue_size, message, requests, f, enable_reinsert, replace_keyword):
    _print (f, "QUEUE SIZE %s msg %s"%(queue_size.value, message))
    _print (f, "enable reinsert %s replace_keyword %s"%(enable_reinsert, replace_keyword))
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
            msg = "%s: %s: From client %s:%s" % \
                      (exc, msg, client_addr, request[:100])
        except IndexError:
            msg = "%s: %s: From client %s: %s" % \
                      (exc, msg, client_addr, request)
        # There were cases when request could not get formatted as a string.
        # This apparently happened during port scan
        except:
            try:
                exc, msg = sys.exc_info()[:2]
                msg = "%s: %s: From client %s" % \
                      (exc, msg, client_addr)
            except:
                exc, msg = sys.exc_info()[:2]
                msg = "%s: %s: " % \
                      (exc, msg)
                
            
        Trace.log(5, msg)

        #Set these to something.
        request = None

    if not request:
        return

    lock.acquire()
    try:
        request_id = get_id(request)
        do_put = True
        # if self.replace_keyword is specified
        # replace a message with this keyword in the buffer
        # this is needed for processing
        # mover requests require different aprroaach in their processing
        # they need to be aligned on the order they came and no duplicate
        # request is allowed.
        # Thus the old mover request gets replaced by the newer request
        # at the same place in the queue
        if replace_keyword:
            keyword_to_replace = get_keyword(request, replace_keyword)
            if keyword_to_replace:
                if requests.has_key(keyword_to_replace):
                    try:
                       do_put = False # duplicate request, do not put into the queue
                       index = buffer.index(requests[keyword_to_replace])
                       del buffer[index]
                       buffer.insert(index, (request, client_addr))
                           
                    except ValueError:
                       # request is not in buffer yet
                       pass
                _print(f, "ADD TO REQUESTS %s"%(request,))
                requests[keyword_to_replace] = (request, client_addr)
        else:
            if request_id:
                # put the latest request into the queue
                if requests.has_key(request_id) and requests[request_id] !="":
                    # most like this is a retry
                    _print(f, "DUPLICATE %s %s" % (time.time(), request_id))
                    try:
                        # "retry" message put it closer to the beginnig of the queue
                        index = buffer.index(requests[request_id])
                        if enable_reinsert and queue_size.value > SHUFFLE_THRESHOLD:
                            # new index is in 10% of top messages
                            new_index = index / (((queue_size.value + 1)/10)+1) + index % 10
                            if new_index >= index:
                                new_index = index
                        else:
                            new_index = index
                        _print(f, "FOUND at %s reinserting at %s queue size %s"%(index, new_index, queue_size.value))
                        #if enable_reinsert and queue_size.value > SHUFFLE_THRESHOLD:
                        #buffer.remove(requests[request_id])
                        del buffer[index]
                        buffer.insert(new_index, (request, client_addr))
                        do_put = False # duplicate request, do not put into the queue
                    except ValueError, detail:
                        _print(f,"put: Exception:ValueError %s removing %s"%(detail,request_id))  

                    requests[request_id] = (request, client_addr)
                else:
                    if queue_size.value > MAX_QUEUE_SIZE:
                        # drop incoming message
                        do_put = False
                    else:
                        requests[request_id] = (request, client_addr)

        if do_put:
            t0 = time.time()

            buffer.append((request, client_addr))
            queue_size.value = queue_size.value + 1
            event.set()
            _print (f, "PUT %s %s %s"% (time.time(), request_id, time.time() - t0))
    except:
        exc, detail, tb = sys.exc_info()
        print "put: %s %s"%(exc, detail)
    lock.release()

        
# receiver runs in a separate process
# this is why it is outside of RawUDP class
def _receiver(RawUDP_obj):
    print "I am rawUDP_p", os.getpid(), RawUDP_obj.replace_keyword
    if DEBUG:
        thread = threading.currentThread()
        thread_name = thread.getName()
        dirpath = os.path.join(os.environ.get("ENSTORE_OUT", ""),"tmp/%s"%(pwd.getpwuid(os.geteuid())[0],))
        if not os.path.exists(dirpath):
            os.makedirs(dirpath)
        RawUDP_obj.f = open(os.path.join(dirpath, "p_%s_%s"%(os.getpid(), thread_name)), "w")
    else:
        RawUDP_obj.f = None
    
    print"RECEIVER %s STARTS on %s"%(os.getpid(), RawUDP_obj.server_socket.getsockname(),)
    rcv_timeout = RawUDP_obj.rcv_timeout
    while 1:

        r, w, x, remaining_time = cleanUDP.Select([RawUDP_obj.server_socket], [], [], rcv_timeout)

        if r:
            for fd in r:
                if fd == RawUDP_obj.server_socket:
                    req, client_addr = RawUDP_obj.server_socket.recvfrom(
                        RawUDP_obj.max_packet_size, RawUDP_obj.rcv_timeout)

                    if req:
                        # Reminder about request structure
                        # request is a string
                        # it has the following structure
                        # str(str((request_id, request_counter, body)), check_sum)
                        # where body is a dictionary
                        message = (req, client_addr)
                        #_print (RawUDP_obj.f, "MESSAGE %s %s"%(time.time(), message))
                        put(RawUDP_obj._lock, RawUDP_obj.arrived, RawUDP_obj.buffer,
                            RawUDP_obj.queue_size_p, message, RawUDP_obj.requests,
                            RawUDP_obj.f, RawUDP_obj.enable_reinsert, RawUDP_obj.replace_keyword)
        else:
            # time out
            # set event to allow get to proceed
            # this can be used in dispatching worker to run interva functions
            RawUDP_obj.arrived.set()

if __name__ == "__main__":
    rs = RawUDP(7700)
    
    rs.receiver()
