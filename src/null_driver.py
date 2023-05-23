#!/usr/bin/env python

# $Id$

import os
import time

import setpath
import generic_driver
import strbuffer
import e_errors
import Trace

class NullDriver(generic_driver.Driver):

    def __init__(self):
        self.fd = -1
        self.loc = 0
        self._rate = 0
        self._last_rate = 0
        self_start_time = None
        self._active_time = 0
        self._total_time = 0
        self._bytes_transferred = 0
        self.verbose = 0
        
    def open(self, device=None, mode=None, retry_count=10):
        """open the device
            Args:
                device(str): device name (not used, device set by mode)
                mode(int): 0=read, 1=write, other=error (required)
                retry_count(int): number of times to retry open (optional)
            Returns:
                1 if successful
            Raises:
                ValueError if mode is not 0 or 1
        """
        Trace.trace(25, "open")
        if self.fd>0:
            if mode != self.mode:
                self.close()
        if mode==0:
            device = '/dev/zero'
        elif mode==1:
            device = '/dev/null'
        else:
            raise ValueError("illegal mode", mode)
        self.device = device
        self.mode = mode
        self._active_time = 0 #time actually spent in read or write call
        if self.fd < 0:
            self.fd = os.open(device, mode)
        self._rate = self._last_rate = self._bytes_transferred = 0
        return 1

    def rewind(self):
        """Rewind the file to the beginning
        Args:
            None
        
        Returns:
            None
        """
        Trace.trace(25, "rewind")
        self.loc = 0

    def tell(self):
        """tell the current location on device
        Args:
            None
        Returns:
            (long, long): (current location, current location)
        """
        Trace.trace(25, "tell %s" % (self.loc))
        return self.loc, self.loc
    
    def seek(self, loc, eot_ok=None): #XXX is eot_ok needed?
        """seek to a location on the device
        Args:
            loc(long): location to seek to
            eot_ok(bool): ignored
        Returns:
            None
        """
        if type(loc) is type(""):
            if loc[-1]=='L':
                loc=loc[:-1] #py1.5.2 
            loc = int(loc)
        Trace.trace(25, "seek %s" % (loc,))
        self.loc = loc
        
    def fileno(self):
        """return the file descriptor
        Args:
            None
        Returns:
            int: file descriptor
        """
        return self.fd

    def flush(self):
        """flush the device buffer
        NOT IMPLEMENTED
        Args:
            None
        Returns:
            None"""
        pass
        
    def close(self):
        """close the device
        Args:
            None
        Returns:
            int: 0 if successful, -1 if error
        """
        Trace.trace(25, "close")
        if self.fd == -1:
            return 0
        try:
            r = os.close(self.fd)
        except:
            r=-1
            Trace.handle_error()
        self.fd = -1
        return r

    def read(self, buf, offset, nbytes):
        """read data from the device

        Args:
            buf(str): buffer to read into
            offset(int): offset into buffer
            nbytes(int): number of bytes to read

        Returns:
            int: number of bytes read

        Raises:
            ValueError if file not open for reading
            IOError if read error on null device
        """
        if self.mode != 0:
            raise ValueError("file not open for reading")

        t0 = time.time()
        r = strbuffer.buf_read(self.fd, buf, offset, nbytes)
        if r > 0:
            now = time.time()
            self._last_rate = r/(now-t0)
            if self._bytes_transferred == 0:
                self._start_time = t0
            self._bytes_transferred = self._bytes_transferred + r
            self._active_time = now - self._start_time
            self._rate = self._bytes_transferred/(now - self._start_time)
        if r == -1:
            Trace.log(e_errors.ERROR, "read error on null device")
            raise IOError("read error on null device")
        return r
    
    def write(self, buf, offset, nbytes):
        """write data to the device
        
        Args:
            buf(str): buffer to write from
            offset(int): offset into buffer
            nbytes(int): number of bytes to write
        
        Returns:
            int: number of bytes written
            
        Raises:
            ValueError if file not open for writing
        """
        if self.mode != 1:
            raise ValueError("file not open for writing")
        return nbytes

        
    def writefm(self):
        """write a filemark
        Args:
            None
        Returns:
            None
        """
        Trace.trace(25, "writefm")
        self.loc = self.loc + 1

    def skipfm(self, n):
        """move device location n bytes
        Args:
            n(int): number of bytes to move
        Returns:
            None
        """
        Trace.trace(25, "skipfm")
        self.loc = self.loc + n
        
    def eject(self):
        """eject the tape
            NOT IMPLEMENTED
        Args:
            None
        Returns:
            None
        """
        return
    
    def set_mode(self, density=None, compression=None, blocksize=None):
        """set the tape mode
            NOT IMPLEMENTED
        Args:
            density(int): density code (optional)
            compression(int): compression code (optional)
            blocksize(int): blocksize code  (optional)
        Returns:
            None
        """
        pass

    def rates(self):
        """return the current and last transfer rates
        Args:
            None
        Returns:
            (float, float): (current rate, last rate)
        """
        return self._rate, self._last_rate

    def verify_label(self, label=None, mode=0):
        """verify the label on the tape
        Args:
            label(str): label to verify (optional)
            mode(int): mode to verify in (ignored)
        Returns:
            (int, obj): (enstore error code, None)
        """
        if label is None:
            #hack. "New" null volumes must appear unlabeled
            return e_errors.READ_VOL1_READ_ERR, None
        else:
            return e_errors.OK, None
    
    def tape_transfer_time(self):
        """return the time the tape has been active
        Args:
            None
        Returns:
            float: time in seconds
        """
        return self._active_time
            
        
