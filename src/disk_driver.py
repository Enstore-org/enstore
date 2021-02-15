#!/usr/bin/env python

# $Id$

from future.utils import raise_
import os
import sys
import time

import setpath
import generic_driver
import strbuffer
import e_errors
import Trace


class DiskDriver(generic_driver.Driver):

    def __init__(self):
        self.fd = -1
        self._rate = 0
        self._last_rate = 0
        self_start_time = None
        self._total_time = 0
        self._bytes_transferred = 0
        self.verbose = 0
        self._active_time = 0

    def open(self, device=None, mode=None, retry_count=10):
        Trace.trace(25, "open %s" % (device,))
        if self.fd > 0:
            if mode != self.mode:
                self.close()
        if not (mode == 0 or mode == 1):
            raise ValueError("illegal mode", mode)
        self.device = device
        self.mode = mode
        self._active_time = 0  # time actually spent in read or write call
        if self.fd < 0:
            dir_path = os.path.dirname(device)
            if mode == 1:  # WRITE
                mode = mode | os.O_CREAT
                try:
                    if not os.path.exists(dir_path):
                        os.makedirs(dir_path, 0o755)
                except BaseException:
                    exc, msg, tb = sys.exc_info()
                    raise_(exc, msg, tb)
            try:
                self.fd = os.open(device, mode)
            except BaseException:
                exc, msg, tb = sys.exc_info()
                os.system("ls %s > /tmp/ls_out" % (os.path.dirname(dir_path),))
                raise_(exc, msg, tb)

        self._rate = self._last_rate = self._bytes_transferred = 0
        return 1

    def rewind(self):
        pass

    def tell(self):
        pass

    def seek(self, loc, eot_ok=None):
        pass

    def fileno(self):
        return self.fd

    def flush(self):
        pass

    def close(self):
        Trace.trace(25, "close")
        if self.fd == -1:
            return 0
        fd = self.fd
        self.fd = -1
        try:
            r = os.close(fd)
        except BaseException:
            exc, msg, tb = sys.exc_info()
            raise_(exc, msg, tb)
        return r

    def read(self, buf, offset, nbytes):
        if self.mode != 0:
            raise ValueError("file not open for reading")

        t0 = time.time()
        r = strbuffer.buf_read(self.fd, buf, offset, nbytes)
        if r > 0:
            now = time.time()
            self._last_rate = r / (now - t0)
            if self._bytes_transferred == 0:
                self._start_time = t0
            self._bytes_transferred = self._bytes_transferred + r
            self._active_time = now - self._start_time
            self._rate = self._bytes_transferred / (now - self._start_time)
        if r == -1:
            Trace.log(e_errors.ERROR, "read error on null device")
            raise IOError("read error on null device")
        return r

    def write(self, buf, offset, nbytes):
        if self.mode != 1:
            raise ValueError("file not open for writing")

        t0 = time.time()
        r = strbuffer.buf_write(self.fd, buf, offset, nbytes)

        if r > 0:
            now = time.time()
            self._last_rate = r / (now - t0)
            if self._bytes_transferred == 0:
                self._start_time = t0
            self._bytes_transferred = self._bytes_transferred + r
            self._active_time = now - self._start_time
            self._rate = self._bytes_transferred / (now - self._start_time)
        if r == -1:
            Trace.log(e_errors.ERROR, "write error on null device")
        return r

    def writefm(self):
        pass

    def skipfm(self, n):
        pass

    def eject(self):
        return

    def set_mode(self, density=None, compression=None, blocksize=None):
        pass

    def rates(self):
        return self._rate, self._last_rate

    def verify_label(self, label=None, mode=0):
        if label is None:
            # hack. "New" null volumes must appear unlabeled
            return e_errors.READ_VOL1_READ_ERR, None
        else:
            return e_errors.OK, None

    def tape_transfer_time(self):
        return self._active_time
