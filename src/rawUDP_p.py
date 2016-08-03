#!/usr/bin/env python
###############################################################################
#
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
import select
import Queue

import cleanUDP
import udp_common
import checksum
import Trace
import enstore_constants
import hostaddr

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
        self.f = None
        self.max_queue_size = MAX_QUEUE_SIZE
        self.use_queue = False # use queue directly, not as intermediate place for incoming messages
        self.queue = multiprocessing.Queue(self.max_queue_size) # intermediate place for incoming messages

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
        #self.address_family = socket.AF_INET
        #hostip = socket.gethostbyname(socket.gethostname())
	hostip = hostaddr.name_to_address(socket.gethostname())
        ip, port, self.server_socket = udp_common.get_callback(hostip, port)
        self.address_family = socket.getaddrinfo(ip, None)[0][0]
        self.server_address = (ip, port)

        # set this socket to be closed in case of an exec
        if self.server_socket != None:
            fcntl.fcntl(self.server_socket.fileno(), fcntl.F_SETFD, fcntl.FD_CLOEXEC)

    def init_socket(self, socket):
        self.server_socket = socket


    def set_keyword(self, keyword):
        self.replace_keyword = keyword

    def set_max_queue_size(self, queue_size):
        self.max_queue_size = queue_size
        del(self.queue)
        self.queue = multiprocessing.Queue(self.max_queue_size)

    def set_use_queue(self):
        self.use_queue = True

    def set_caller_name(self, caller_name):
        self.caller_name = caller_name


    # disable reshuffling of duplicate requests
    # this can be beneficial for mover requests
    # but may hurt encp requests
    def disable_reshuffle(self):
        self.enable_reinsert = False


    def process_enstore_message(self, message):
        rc = None
        req = message[0]
        client_addr = (message[1])
        request=None
        try:
            request, inCRC = udp_common.r_eval(req, check=True)

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

            print msg
        if request:
            rc = request, client_addr
        return rc


    # get message from FIFO buffer
    # return value:
    # (request, client_address)
    # where client_address = (client_ip, client_port)
    def get(self):
        _print(self.d_o, "GET")
        rc = None
        Trace.trace(6, "GET Queue size %s"%(self.queue_size_p.value,))
        if self.use_queue:
            message = self.queue.get(True, self.rcv_timeout)
            if message:
                rc = self.process_enstore_message(message)
            else:
                if self.arrived.is_set():
                    self.arrived.clear()
            return rc

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
        try:
            multiprocessing.Process(target=_receiver, args = (self,)).start()
            if not self.use_queue:
                multiprocessing.Process(target=fetch, args = (self,)).start()
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
def put(RawUDP_obj, message):
    #_print (RawUDP_obj.f, "QUEUE SIZE %s msg %s"%(queue_size.value, message))

    #_print (RawUDP_obj.f, "enable reinsert %s replace_keyword %s"%(enable_reinsert, replace_keyword))
    # message structure:
    # (request, client_address)
    rc = RawUDP_obj.process_enstore_message(message)
    client_addr = (message[1])

    if not rc:
        return
    request, client_addr = rc
    RawUDP_obj._lock.acquire()
    try:
        request_id = get_id(request)
        do_put = True
        # if self.replace_keyword is specified
        # replace a message with this keyword in the buffer
        # this is needed for processing
        # mover requests requiring different aprroach in their processing
        # they need to be aligned on the order they came and no duplicate
        # request is allowed.
        # Thus the old mover request gets replaced by the newer request
        # at the same place in the queue
        if RawUDP_obj.replace_keyword:
            keyword_to_replace = get_keyword(request, RawUDP_obj.replace_keyword)
            if keyword_to_replace and keyword_to_replace in RawUDP_obj.requests:
                try:
                   do_put = False # duplicate request, do not put into the queue
                   index = RawUDP_obj.buffer.index(RawUDP_obj.requests[keyword_to_replace])
                   del RawUDP_obj.buffer[index]
                   RawUDP_obj.buffer.insert(index, (request, client_addr))

                except ValueError:
                   # request is not in buffer yet
                   pass
                RawUDP_obj.requests[keyword_to_replace] = (request, client_addr)
        else:
            if request_id:
                # put the latest request into the queue
                if request_id in RawUDP_obj.requests and RawUDP_obj.requests[request_id] !="":
                    # most likely this is a retry
                    #_print(RawUDP_obj.f, "DUPLICATE %s %s" % (time.time(), request_id))
                    try:
                        # "retry" message put it closer to the beginning of the queue
                        index = RawUDP_obj.buffer.index(RawUDP_obj.requests[request_id])
                        if RawUDP_obj.enable_reinsert and RawUDP_obj.queue_size_p.value > SHUFFLE_THRESHOLD:
                            # new index is in 10% of top messages
                            new_index = index / (((RawUDP_obj.queue_size_p.value + 1)/10)+1) + index % 10
                            if new_index >= index:
                                new_index = index
                        else:
                            new_index = index
                        _print(RawUDP_obj.f, "FOUND at %s reinserting at %s queue size %s"%(index, new_index, RawUDP_obj.queue_size_p.value))
                        #if enable_reinsert and queue_size.value > SHUFFLE_THRESHOLD:
                        #buffer.remove(requests[request_id])
                        del RawUDP_obj.buffer[index]
                        RawUDP_obj.buffer.insert(new_index, (request, client_addr))
                        do_put = False # duplicate request, do not put into the queue
                    except ValueError, detail:
                        _print(RawUDP_obj.f,"put: Exception:ValueError %s removing %s"%(detail,request_id))

                    RawUDP_obj.requests[request_id] = (request, client_addr)
                else:
                    if RawUDP_obj.queue_size_p.value > RawUDP_obj.max_queue_size:
                        # drop incoming message
                        do_put = False
                    else:
                        RawUDP_obj.requests[request_id] = (request, client_addr)

        if do_put:
            #t0 = time.time()

            RawUDP_obj.buffer.append((request, client_addr))
            RawUDP_obj.queue_size_p.value += 1
            RawUDP_obj.arrived.set()
            #_print (RawUDP_obj.f, "PUT %s %s %s"% (time.time(), request_id, time.time() - t0))
    except:
        exc, detail, tb = sys.exc_info()
        print "put: %s %s"%(exc, detail)
    RawUDP_obj._lock.release()


def fetch(RawUDP_obj):
    while 1:
        message = RawUDP_obj.queue.get()
        put(RawUDP_obj, message)

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
        print "REC OUT FILE", RawUDP_obj.f
    else:
        RawUDP_obj.f = None

    print "RECEIVER %s STARTS on %s"%(os.getpid(), RawUDP_obj.server_socket.getsockname(),)
    rcv_timeout = RawUDP_obj.rcv_timeout
    while 1:
        r, w, x = select.select([RawUDP_obj.server_socket], [], [], rcv_timeout)

        if r and r[0] == RawUDP_obj.server_socket:
            message = RawUDP_obj.server_socket.recvfrom(
                RawUDP_obj.max_packet_size, RawUDP_obj.rcv_timeout)

            if message[0]: # request is not empty
                # Reminder about request structure
                # request is a string
                # it has the following structure
                # str(str((request_id, request_counter, body)), check_sum)
                # where body is a dictionary
                # message is (req, client_addr)
                #_print (RawUDP_obj.f, "MESSAGE %s %s"%(time.time(), message))

                try:
                    RawUDP_obj.queue.put_nowait(message)
                except Queue.Full:
                    m = str(message)
                    if RawUDP_obj.caller_name == "log_server":
                        if not " ENCP " in m:
                            # Send to stdout as enstore log service may not be available.
                            msg = " ".join((time.strftime("%Y-%m-%d %H:%M:%S"), m))
                            print "Intermediate queue is full", msg
                except Exception, detail:
                    print "Exception putting into queue:", detail

        else:
            RawUDP_obj.arrived.set()

def create_server(port):
    #hostip = socket.gethostbyname(socket.gethostname())
    hostip = hostaddr.name_to_address(socket.gethostname())
    address_family = socket.getaddrinfo(hostip[0], None)[0][0]
    server_socket = cleanUDP.cleanUDP(address_family, socket.SOCK_DGRAM)
    try:
        server_socket.socket.bind((hostip, port))
    except socket.error, msg:
        print msg
        sys.exit(1)
    raw_requests = RawUDP(receive_timeout=60)
    raw_requests.disable_reshuffle()
    raw_requests.init_socket(server_socket)
    return raw_requests

if __name__ == "__main__":

    srv = create_server(7700)
    srv.receiver()

    while 1:
        rc = srv.get()
        if rc:
           print "REQ %s %s"%(rc[1], srv.queue_size)
