#!/usr/bin/env python

# $Id$

import os
import time

import setpath
import driver
import strbuffer
import Trace

class NullDriver(driver.Driver):

    def __init__(self):
        self.fd = -1
        self.loc = 0L
        self._rate = 0
        self._last_rate = 0
        self._bytes_transferred = 0
        self.verbose = 0
        
    def open(self, device=None, mode=None, retry_count=10):
        if mode==0:
            device = '/dev/zero'
        elif mode==1:
            device = '/dev/null'
        else:
            raise ValueError, ("illegal mode", mode)
        self.device = device
        self.mode = mode
        self.fd = os.open(device, mode)
        self._rate = self._last_rate = self._bytes_transferred = 0
        return self.fd

    def rewind(self):
        Trace.trace(25, "rewind")
        self.loc = 0L

    def tell(self):
        Trace.trace(25, "tell %s" % (self.loc))
        return self.loc
    
    def seek(self, loc, eot_ok=None):
        if type(loc) is type(""):
            if loc[-1]=='L':
                loc=loc[:-1] #py1.5.2 
            loc = long(loc)
        Trace.trace(25, "seek %s" % (loc,))
        self.loc = loc
        
    def fileno(self):
        return self.fd

    def flush(self):
        pass
        
    def close(self):
        Trace.trace(25, "close")
        r = os.close(self.fd)
        self.fd = -1
        return r

    def read(self, buf, offset, nbytes):
        if self.mode != 0:
            raise ValueError, "file not open for reading"

        t0 = time.time()
        r = strbuffer.buf_read(self.fd, buf, offset, nbytes)
        if r > 0:
            now = time.time()
            self._last_rate = r/(now-t0)
            if self._bytes_transferred == 0:
                self._start_time = t0
            self._bytes_transferred = self._bytes_transferred + r
            self._rate = self._bytes_transferred/(now - self._start_time)
        if r == -1:
            Trace.log(e_errors.ERROR, "read error on null device")
            raise IOError, "read error on null device"
        return r
    
    def write(self, buf, offset, nbytes):
        if self.mode != 1:
            raise ValueError, "file not open for writing"

        t0 = time.time()
        r = strbuffer.buf_write(self.fd, buf, offset, nbytes)

        if r > 0:
            now = time.time()
            self._last_rate = r/(now-t0)
            if self._bytes_transferred == 0:
                self._start_time = t0
            self._bytes_transferred = self._bytes_transferred + r
            self._rate = self._bytes_transferred/(now - self._start_time)
        if r == -1:
            Trace.log(e_errors.ERROR,  "write error on null device")
        return r
        
    def writefm(self):
        Trace.trace(25, "writefm")
        self.loc = self.loc + 1

    def eject(self):
        return
    
    def set_mode(self, density=None, compression=None, blocksize=None):
        pass

    def rates(self):
        return self._rate, self._last_rate

    def ready_to_read(self):
        return 1

    def ready_to_write(self):
        return 1
    
    
            
        
