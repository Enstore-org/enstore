#!/usr/bin/env python

# $Id$

from future.utils import raise_
import select
import socket
import time
import errno

import setpath
import generic_driver
import e_errors
import strbuffer
import Trace


class NetDriver(generic_driver.Driver):

    def __init__(self):
        self.sock = None
        self._bytes_transferred = 0
        self._start_time = None
        self._rate = self._last_rate = 0

    def fdopen(self, sock):
        self.sock = sock
        #size = 128 * 1024

        # This is disabled because it seems to trigger a problem
        # when recieving data from a gigabit ethernet interface
        # on IRIX
# for opt in  (socket.SO_RCVBUF, socket.SO_SNDBUF):
# try:
##                sock.setsockopt(socket.SOL_SOCKET, opt, size)
# Trace.trace(10, "tcp buffer size %s %s" % (
##                    {socket.SO_RCVBUF:"SO_RCVBUF", socket.SO_SNDBUF:"SO_SNDBUF"}[opt],
# sock.getsockopt(socket.SOL_SOCKET, opt)))
# except socket.error, msg:
# Trace.log(e_errors.ERROR, "setting tcp buffer size:  %s %s %s" % (
# opt, size, msg))

        self._last_rate = 0
        self._rate = 0
        self._bytes_transferred = 0
        return self.sock

    def fileno(self):
        return self.sock.fileno()

    def close(self):
        r = 0
        #Trace.log(e_errors.INFO,"NetDriver:close %s"%(self.sock,))
        if self.sock is not None:
            r = self.sock.close()
            if r:
                Trace.log(
                    e_errors.ERROR, "NetDriver:error closing socket %s" %
                    (r,))
        self.sock = None
        return r

    def read(self, buf, offset, nbytes):
        t0 = time.time()
        # Give up if the client does not send us data for 1 minute
        ready, junk, junk = select.select([self.fileno()], [], [], 1 * 60)
        if ready:
            try:
                r = strbuffer.buf_recv(self.fileno(), buf, offset, nbytes)
            except MemoryError:
                Trace.log(e_errors.ERROR, "Memory error calling buf_recv with fileno %s offset %s nbytes %s" %
                          (self.fileno(), offset, nbytes))
                raise MemoryError
        else:
            r = 0

        if r < 0:
            if strbuffer.cvar.errno in (errno.EAGAIN, errno.EINTR):
                r = 0
            else:
                msg = "net_driver: read(%d) returns %d, errno=%d" % (
                    nbytes, r, strbuffer.cvar.errno)
                Trace.log(e_errors.ERROR, msg)
                raise_(generic_driver.DriverError, msg)
        if r > 0:
            now = time.time()
            t = now - t0
            if self._bytes_transferred == 0:
                self._start_time = t0
            self._bytes_transferred = self._bytes_transferred + r
            if t != 0:
                self._last_rate = r / t
                self._rate = self._bytes_transferred / (now - self._start_time)
        return r

    def write(self, buf, offset, nbytes):
        t0 = time.time()
        r = strbuffer.buf_send(self.fileno(), buf, offset, nbytes)
        if r < 0:
            if strbuffer.cvar.errno in (errno.EAGAIN, errno.EINTR):
                r = 0
            else:
                msg = "net_driver: write(%d) returns %d, errno=%d" % (
                    nbytes, r, strbuffer.cvar.errno)
                Trace.log(e_errors.ERROR, msg)
                raise_(generic_driver.DriverError, msg)
        if r > 0:
            now = time.time()
            t = now - t0
            self._last_rate = r / (now - t0)
            if self._bytes_transferred == 0:
                self._start_time = t0
            self._bytes_transferred = self._bytes_transferred + r
            if t != 0:
                self._last_rate = r / t
                self._rate = self._bytes_transferred / (now - self._start_time)
        return r

    def rates(self):
        """returns a tuple (overall rate, instantaneous rate)"""
        return self._rate, self._last_rate
