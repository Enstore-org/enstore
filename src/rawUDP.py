#!/usr/bin/env python

###############################################################################
#
# $Id$
#
###############################################################################
from __future__ import print_function
from future.utils import raise_
import sys
import os
import pwd
import threading
import fcntl
import socket
import cleanUDP
import udp_common
import Trace
import checksum
import time
import enstore_constants

DEBUG = False
MAX_PACKET_SIZE = enstore_constants.MAX_UDP_PACKET_SIZE
MAX_QUEUE_SIZE = 200000
SHUFFLE_THRESHOLD = 1000  # reinsert at the lower index beginning with this queue size

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
    except BaseException:
        # wrong message format
        return None

# get a keyword from message
# message is a str(dictionary)


def get_keyword(request, keyword):
    rarr = request.split("'")
    try:
        if keyword in rarr:
            index = rarr.index(keyword)
            return(rarr[index + 2])
        else:
            return None
    except BaseException:
        exc, detail, tb = sys.exc_info()
        print("Exception getting keyword %s" % (detail,))

        return None


def _print(f, msg):
    if DEBUG and f:
        f.write("%s %s\n" % (time.time(), msg))
        f.flush()


class RawUDP:

    def __init__(self, receive_timeout=60.):
        self.max_packet_size = MAX_PACKET_SIZE
        self.rcv_timeout = receive_timeout   # timeout for get_request in sec.
        self._lock = threading.Lock()
        self.arrived = threading.Event()
        self.queue_size = 0
        self.buffer = []
        self.requests = {}  # this is to avoid multiple requests with the same id
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
        #print "addr %s sock %s"%(self.server_address, self.server_socket)

        # set this socket to be closed in case of an exec
        if self.server_socket is not None:
            fcntl.fcntl(
                self.server_socket.fileno(),
                fcntl.F_SETFD,
                fcntl.FD_CLOEXEC)

    def init_socket(self, socket):
        self.server_socket = socket

    def set_keyword(self, keyword):
        self.replace_keyword = keyword

    # disable reshuffling of duplicate requests
    # this can be beneficial for mover requests
    # but may hurt encp requests
    def disable_reshuffle(self):
        self.enable_reinsert = False

    def put(self, message):
        # message structure:
        #(request, cliient_address)
        req = message[0]
        client_addr = (message[1])
        request = None
        try:
            request, inCRC = udp_common.r_eval(req, check=True)
            # calculate CRC
            crc = checksum.adler32(0, request, len(request))
            if (crc != inCRC):
                print("BAD CRC request: %s " % (request,))
                print("CRC: %s calculated CRC: %s" % (repr(inCRC), repr(crc)))

                request = None
        except ValueError as detail:
            # must be an event relay message
            # it has a different format
            try:
                request = udp_common.r_eval(req, check=True)
            except BaseException:
                exc, msg = sys.exc_info()[:2]
                raise_(exc, msg)
        except (SyntaxError, TypeError):
            # If TypeError occurs, keep retrying.  Most likely it is
            # an "expected string without null bytes".
            # If SyntaxError occurs, also keep trying, most likely
            # it is from and empty UDP datagram.
            exc, msg = sys.exc_info()[:2]
            try:
                message = "%s: %s: From client %s:%s" % \
                          (exc, msg, client_addr, request[:100])
            except IndexError:
                message = "%s: %s: From client %s: %s" % \
                          (exc, msg, client_addr, request)
            # There were cases when request could not get formatted as a string.
            # This apparently happened during port scan
            except BaseException:
                try:
                    exc, msg = sys.exc_info()[:2]
                    msg = "%s: %s: From client %s" % \
                        (exc, msg, client_addr)
                except BaseException:
                    exc, msg = sys.exc_info()[:2]
                    msg = "%s: %s: " % \
                        (exc, msg)

            Trace.log(5, message)

            # Set these to something.
            request, inCRC = (None, None)

        if not request:
            return

        request_id = get_id(request)

        self._lock.acquire()
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
            if self.replace_keyword:
                keyword_to_replace = get_keyword(request, self.replace_keyword)
                if keyword_to_replace:
                    if keyword_to_replace in self.requests:
                        try:
                            do_put = False  # duplicate request, do not put into the queue
                            index = self.buffer.index(
                                self.requests[keyword_to_replace])
                            del self.buffer[index]
                            self.buffer.insert(index, (request, client_addr))

                        except ValueError:
                            # request is not in buffer yet
                            pass
                    _print(self.f, "ADD TO REQUESTS %s" % (request,))
                    self.requests[keyword_to_replace] = (request, client_addr)
            else:
                if request_id:
                    # put the latest request into the queue
                    if request_id in self.requests and self.requests[request_id] != "":
                        _print(self.f, "DUPLICATE %s" % (request,))
                        try:
                            # "retry" message put it closer to the beginnig of the queue
                            index = self.buffer.index(
                                self.requests[request_id])

                            if self.enable_reinsert and self.queue_size > SHUFFLE_THRESHOLD:
                                # new index is in 10% of top messages
                                new_index = index / \
                                    (((self.queue_size + 1) / 10) + 1) + index % 10
                                if new_index >= index:
                                    new_index = index
                            else:
                                new_index = index
                            _print(
                                self.f, "FOUND at %s reinserting at %s queue size %s" %
                                (index, new_index, self.queue_size))
                            self.buffer.remove(self.requests[request_id])
                            self.buffer.insert(
                                new_index, (request, message[1]))
                            do_put = False  # duplicate request, do not put into the queue
                        except ValueError as detail:
                            _print(
                                self.f, "put: Exception:ValueError %s removing %s" %
                                (detail, request_id))

                    else:
                        if self.queue_size > MAX_QUEUE_SIZE:
                            # drop incoming message
                            do_put = False
                        else:
                            self.requests[request_id] = (request, message[1])

            if do_put:
                t0 = time.time()
                self.buffer.append((request, message[1]))
                self.queue_size = self.queue_size + 1
                self.arrived.set()
                _print(self.f, "PUT %s" % (time.time() - t0, ))
        except BaseException:
            exc, detail, tb = sys.exc_info()
            print(exc, detail)

        self._lock.release()

    # get message from FIFO buffer
    # return values:
    # client_ip
    # client port
    # request
    # number of requests in the buffer

    def get(self):
        # this is to measure time in get
        if hasattr(self, "last_get"):
            t = time.time() - self.last_get
        else:
            t = 0
        _print(self.d_o, "GET")
        Trace.trace(5, "GET %s" % (t,))
        rc = None
        if self.queue_size == 0:
            self.arrived.wait()
            self.arrived.clear()
        if self.queue_size > 0:
            t0 = time.time()
            ret = None
            request_id = None
            self._lock.acquire()
            # self.arrived.clear()
            try:
                ret = self.buffer.pop(0)
                request_id = get_id(ret[0])
                self.queue_size = self.queue_size - 1

                if request_id in self.requests:
                    #print "DELETE ID",request_id
                    del(self.requests[request_id])
                keyword_to_replace = get_keyword(ret[0], self.replace_keyword)
                if keyword_to_replace in self.requests:
                    del(self.requests[keyword_to_replace])

            except IndexError as detail:
                print("IndexError", detail)
            except BaseException:
                pass

            rc = ret
            self._lock.release()
            _print(self.d_o, "GET %s" % (time.time() - t0,))
            self.last_get = time.time()

        return rc

    def _receiver(self):
        if DEBUG:
            thread = threading.currentThread()
            thread_name = thread.getName()
            dirpath = os.path.join(
                os.environ.get(
                    "ENSTORE_OUT", ""), "tmp/%s" %
                (pwd.getpwuid(
                    os.geteuid())[0],))
            if not os.path.exists(dirpath):
                os.makedirs(dirpath)
            self.f = open(
                os.path.join(
                    dirpath, "p_%s_%s" %
                    (os.getpid(), thread_name)), "w")
        else:
            self.f = None
        rcv_timeout = self.rcv_timeout
        while True:

            #r = [self.server_socket]

            #print "wait"
            r, w, x, remaining_time = cleanUDP.Select(
                [self.server_socket], [], [], rcv_timeout)
            #print "got it", r, w, self.server_socket
            #print "got it", self.queue_size

            if r:
                for fd in r:
                    if fd == self.server_socket:
                        req, client_addr = self.server_socket.recvfrom(
                            self.max_packet_size, self.rcv_timeout)
                        #print "rawUDP:REQ", req
                        # Reminder about request structure
                        # request is a string
                        # it has the following structure
                        # str(str((request_id, request_counter, body)), check_sum)
                        # where body is a dictionary

                        if req:
                            message = (req, client_addr)
                            self.put(message)
            else:
                # time out
                # set event to allow get to proceed
                # this can be used in dispatching worker to run interva
                # functions
                self.arrived.set()

    def receiver(self):
        #thread = threading.Thread(group=None, target='_receiver', args=(), kwargs={})
        thread = threading.Thread(group=None, target=self._receiver)
        try:
            thread.start()
        except BaseException:
            exc, detail, tb = sys.exc_info()
            print(detail)

    def set_out_file(self):
        if DEBUG:
            thread = threading.currentThread()
            thread_name = thread.getName()
            dirpath = os.path.join(
                os.environ.get(
                    "ENSTORE_OUT", ""), "tmp/%s" %
                (pwd.getpwuid(
                    os.geteuid())[0],))
            if not os.path.exists(dirpath):
                os.makedirs(dirpath)
            self.d_o = open(
                os.path.join(
                    dirpath, "gp_%s_%s" %
                    (os.getpid(), thread_name)), "w")
        else:
            self.d_o = None


if __name__ == "__main__":
    rs = RawUDP(7700)

    rs.receiver()
