#!/usr/bin/env python

# $Id$

import time
import select

import Trace
import driver
import e_errors
import ftt

KB=1024
MB=KB*KB
GB=MB*KB


class FTTDriver(driver.Driver):

    def __init__(self):
        self.fd = -1
        self.ftt = None
        self._bytes_transferred = 0
        self._start_time = None
        self._rate = self._last_rate = 0
        self._fast_rate = 10*MB
        
    def open(self, device=None, mode=None, retry_count=10):
        if mode not in (0,1):
            raise ValueError, ("illegal mode", mode)
        mode = 1 #XXX HACK CGW!
        self.device = device
        if self.ftt and mode != self.mode:
            self.ftt.close()
            self.ftt = None

        if not self.ftt:
            self.ftt = ftt.open(
                self.device,
                {0:ftt.RDONLY, 1:ftt.RDWR}[mode])

        self.mode = mode
        self._last_rate = 0
        self._rate = 0
        self._bytes_transferred = 0
        Trace.trace(25, "ftt_open returns %s" % (self.ftt,))
        for retry in xrange(retry_count):
            if retry:
                Trace.trace(25, "retrying open")
            try:
                self.fd = self.ftt.open_dev()
                break
            except ftt.FTTError, detail:
                Trace.log(e_errors.ERROR, "%s %s" %(detail, detail.errno))
                if detail.errno == ftt.EBUSY:
                    time.sleep(5)
                else:
                    break
        for retry in xrange(retry_count):
            if retry:
                Trace.trace(25, "retrying status")
            status = self.ftt.status(5)
            if status & ftt.ONLINE:
                break
#        else:
#            ftt.raise_ftt()  #this is BADSWMOUNT
            
        self._rate = self._last_rate = self._bytes_transferred = 0
        Trace.trace(25, "ftt_open_dev returns %s" % (self.fd,))
        return self.fd
    
    def rewind(self):
        r = self.ftt.rewind()
        Trace.trace(25, "ftt_rewind returns %s" % (r,))
        return r

    def tell(self):
        if not self.ftt:
            Trace.log(e_errors.ERROR, "tell: no ftt descriptor")
            return None
        file, block = self.ftt.get_position()
        Trace.trace(25, "ftt_get_position returns %s %s" % (file, block))
        return file
    
    def seek(self, target, eot_ok=0):
        if type(target)==type(""):
            target = long(target)
        try:
            file, block = self.ftt.get_position()
        except ftt.FTTError, detail: 
            if detail.errno == ftt.ELOST: 
                self.rewind() #don't know tape position, must rewind
            else:
                raise ftt.FTTError, detail #some other FTT error

        file, block = self.ftt.get_position()
        if block==0 and file == target:
            return 0
        else:
            Trace.trace(25,"seek: current = %s,%s target=%s" %(file, block, target))
        current = file
        if target>current:
            try:
                self.ftt.skip_fm(target-current)
            except ftt.FTTError, detail:
                if detail.errno == ftt.EBLANK and eot_ok:
                    pass
        else:
            self.ftt.skip_fm(target-current-1)
            self.ftt.skip_fm(1)

        current = self.tell()
        Trace.trace(25,"seek2: current=%s target=%s" % (current, target))
        if current != target:
            raise "XXX Positioning error", (current, target)
        
    def fileno(self):
        return self.fd

    def flush(self):
        Trace.trace(25, "flushing %s" % (self.ftt))
        now=time.time()
        Trace.trace(25, "transferred %s bytes in %s seconds"%(
            self._bytes_transferred, now-self._start_time))
        if now>self._start_time and self._bytes_transferred:
            Trace.trace(25,  "rate: %.3g MB/sec" % (self._bytes_transferred/(now-self._start_time)/MB))

        r = self.ftt.close_dev()
        Trace.trace(25, "ftt_close_dev returns %s" % (r,))
        self.fd = -1
        return r

    def close(self):
        Trace.trace(25, "closing %s" % (self.ftt,))
        r = self.ftt.close()
        Trace.trace(25, "ftt_close returns %s" % (r,))
        self.ftt = None
        self.fd = -1
        return r

    def read(self, buf, offset, nbytes):
##        if self.mode != 0:
##            raise ValueError, "file not open for reading"
        if offset != 0:
            raise ValueError, "offset must be 0"
        t0 = time.time()
        try:
            r = self.ftt.read(buf, nbytes)
        except ftt.FTTError, detail:
            raise e_errors.READ_ERROR, detail
        if r > 0:
            now = time.time()
            self._last_rate = r/(now-t0)
            if self._bytes_transferred == 0:
                self._start_time = t0
            self._bytes_transferred = self._bytes_transferred + r
            self._rate = self._bytes_transferred/(now - self._start_time)
        return r
    
    def write(self, buf, offset, nbytes):
        if self.mode != 1:
            raise ValueError, "file not open for writing"
        if offset != 0:
            raise ValueError, "offset must be 0"
        t0 = time.time()
        try:
            r = self.ftt.write(buf, nbytes)
        except:
            raise e_errors.WRITE_ERROR, detail
        if r > 0:
            now = time.time()
            self._last_rate = r/(now - t0)
            if self._bytes_transferred == 0:
                self._start_time = t0
            self._bytes_transferred = self._bytes_transferred + r
            self._rate = self._bytes_transferred/(now - self._start_time)
        return r
        
    def writefm(self):
        r = self.ftt.writefm()
        if r==-1:
            ftt.raise_ftt()
        return r

    def eject(self):
        return self.ftt.unload()
        
    def set_mode(self, density=None, compression=None, blocksize=None):
        mode = self.ftt.get_mode()
        if density is None:
            density = mode[1]
        if compression is None:
            compression = mode[2]
        if blocksize is None:
            blocksize = mode[3]
        r = self.ftt.set_mode(density, compression, blocksize)
        Trace.trace(25, "ftt mode is %s" %  (self.ftt.get_mode(),))
        self.fd = self.ftt.open_dev()
        return r

    def set_fast_rate(self, rate):
        self._fast_rate = rate
        
    def ready_to_read(self):
        ## r,w,x = select.select([self],[],[],0)  ##aarrggh!  select doesn't work on st devices! - cgw
        if self._last_rate and self._last_rate < self._fast_rate:
            return 0
        return 1

    def ready_to_write(self):
        ### r,w,x = select.select([], [self],[],0)
        if self._last_rate and self._last_rate < self._fast_rate:
            return 0
        return 1

    def rates(self):
        """returns a tuple (overall rate, instantaneous rate)"""
        return self._rate, self._last_rate

        
if __name__ == '__main__':

    print "TEST ME!"
    

            

    
