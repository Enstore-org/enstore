#!/usr/bin/env python

# $Id$

import time
import select

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
        self.verbose = 0
        
    def open(self, device=None, mode=None):
        if mode not in (0,1):
            raise ValueError, ("illegal mode", mode)
        self.device = device
        self.mode = mode
        self.ftt = ftt.open(
            self.device,
            {0:ftt.RDONLY, 1:ftt.RDWR}[mode])
        self._last_rate = 0
        self._rate = 0
        self._bytes_transferred = 0
        if self.verbose: print "ftt_open returns", self.ftt
        for retry in xrange(10):
            if retry:
                if self.verbose: print "retrying open"
            try:
                self.fd = self.ftt.open_dev()
                break
            except ftt.FTTError, detail:
                if self.verbose: print detail, detail.errno
                if detail.errno == ftt.EBUSY:
                    time.sleep(5)
                else:
                    break
        for retry in xrange(10):
            if retry:
                if self.verbose: print "retrying status"
            status = self.ftt.status(10)
            if status & ftt.ONLINE:
                break
        else:
            ftt.raise_ftt()  #this is BADSWMOUNT
            
        self._rate = self._last_rate = self._bytes_transferred = 0
        if self.verbose: print "ftt_open_dev returns", self.fd
        return self.fd

    def reopen(self, device, mode):
        if self.verbose: print "ftt reopen"
        if not self.ftt:
            raise "XXX reopen, not open"
        if mode != self.mode:
            self.ftt.close_dev()
            return self.open(device, mode)

        else:
            for retry in xrange(10):
                if retry:
                    if self.verbose: print "retrying open"
                try:
                    self.fd = self.ftt.open_dev()
                    break
                except ftt.FTTError, detail:
                    print detail, detail.errno
                    if detail.errno == ftt.EBUSY:
                        time.sleep(5)
                    else:
                        break
        for retry in xrange(10):
            status = self.ftt.status(10)
            if status & ftt.ONLINE:
                break
        else:
            ftt.raise_ftt() #BADSWMOUNT

        self._rate = self._last_rate = self._bytes_transferred = 0
        if self.verbose: print "reopen: ftt_open_dev returns", self.fd
        return self.fd

    
    def rewind(self):
        r = self.ftt.rewind()
        if self.verbose: print "ftt_rewind returns", r

    def tell(self):
        if not self.ftt:
            if self.verbose: print "tell: no ftt descriptor"
            return None
        file, block = self.ftt.get_position()
        if self.verbose: print "ftt_get_position returns", file, block
        return file
    
    def seek(self, target):
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
            if self.verbose: print "seek: current = %s,%s target=%s" %(file, block, target)
        current = file
        if target>current:
            self.ftt.skip_fm(target-current)
        else:
            self.ftt.skip_fm(target-current-1)
            self.ftt.skip_fm(1)

        current = self.tell()
        if self.verbose: print "seek2: current=", current, "target=", target
        if current != target:
            raise "XXX Positioning error", (current, target)
        
    def fileno(self):
        return self.fd

    def flush(self):
        if self.verbose: print "flushing", self.ftt
        now=time.time()
        if self.verbose:
            print "transferred %s bytes in %s seconds"%(
                self._bytes_transferred, now-self._start_time),
            if now>self._start_time and self._bytes_transferred:
                print "rate: %.3g MB/sec" % (self._bytes_transferred/(now-self._start_time)/MB)
            else:
                print
        r = self.ftt.close_dev()
        if self.verbose: print "ftt_close_dev returns",r
        self.fd = -1
        return r

    def close(self):
        if self.verbose: print "closing", self.ftt
        r = self.ftt.close()
        if self.verbose: print "ftt_close returns",r
        self.ftt = None
        self.fd = -1
        return r

    def read(self, buf, offset, nbytes):
        if self.mode != 0:
            raise ValueError, "file not open for reading"
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
            self.print_error()
        return r

    def set_mode(self, density=None, compression=None, blocksize=None):
        mode = self.ftt.get_mode()
        if density is None:
            density = mode[1]
        if compression is None:
            compression = mode[2]
        if blocksize is None:
            blocksize = mode[3]
        r = self.ftt.set_mode(density, compression, blocksize)
        if self.verbose: print "ftt mode is", self.ftt.get_mode()
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
    

            

    
