#!/usr/bin/env python

# $Id$

import sys, os
import time
import string

import Trace
import generic_driver
import e_errors
import ftt
import ftt2 as _ftt

KB=1024
MB=KB*KB
GB=MB*KB
DEBUG_LOG = 11

class FTTDriver(generic_driver.Driver):
    mount_delay = 5
    def __init__(self):
        self.fd = -1
        self.ftt = None
        self._bytes_transferred = 0L
        self._start_time = 0
        self._active_time = 0
        self._rate = self._burst_rate = 0
        self._cleaning_bit = 0

    def open(self, device=None, mode=None, retry_count=10):
        """Open will return 1 if there's a volume, 0 if there is no volume
        but otherwise OK, -1 or exception on errors"""
        if mode is None:
            mode = 0
        if mode not in (0,1):
            Trace.log(e_errors.ERROR,"ftt_driver:read: illegal mode %s"%(mode,))
            raise ValueError, ("illegal mode", mode)

        self.device = device
        if self.ftt and mode != self.mode:
            #Trace.trace(42, "ftt.close()")
            self.ftt.close()
            #Trace.trace(42, "ftt.close() done")
            self.ftt = None

        if not self.ftt:
            #Trace.trace(42, "ftt.open(%s,%s)"%(self.device,ftt.RDWR,))
            self.ftt = ftt.open(
                self.device,
                ftt.RDWR) ##Note: always open r/w since mode-switching causes
                          ## loss of location information XXX
            ###{0:ftt.RDONLY, 1:ftt.RDWR}[mode])
            #Trace.trace(42, "%s=ftt.open() done"%(self.ftt,))
            if not self.ftt:
                Trace.log(e_errors.ERROR, "ftt_open returned None")
                return -1

        self.mode = mode
        self._burst_rate = 0
        self._active_time = 0 #time actually spent in read or write call
        self._rate = 0
        self._bytes_transferred = 0L
        Trace.trace(25, "ftt_open returns %s" % (self.ftt,))

        self._open_dev(retry_count)

        if self.fd is None:
            return -1 #or exception?

        Trace.trace(25, "ftt_open_dev returns %s" % (self.fd,))

        for retry in xrange(retry_count):
            if retry:
                Trace.trace(25, "retrying status %d" % (retry,))
                time.sleep(5)
            #Trace.trace(42, "ftt.status(5)")
            status = self.ftt.status(5)
            #Trace.trace(42, "%s=ftt.status(5) done"%(status,))
            if status & ftt.ONLINE:
                break
            Trace.trace(25, "closing ftt device to force status update")
            #Trace.trace(42, "ftt.close_dev()")
            self.ftt.close_dev()
            #Trace.trace(42, "ftt.close_dev done")
            self._open_dev(2)
        else:
            return 0 #this is BADSWMOUNT

        self._rate = self._burst_rate = self._bytes_transferred = 0L
        Trace.log(e_errors.INFO, 'ftt_open returns. The number of retries: %s'%
                  (retry,))
        return 1

    def _open_dev(self, retry_count):
        self.fd = None
        for retry in xrange(retry_count):
            if retry:
                Trace.trace(25, "retrying open %s"%(retry,))
            try:
                #Trace.trace(42, "ftt.open_dev()")
                self.fd = self.ftt.open_dev()
                #Trace.trace(42, "%s=ftt.open_dev() done"%(self.fd,))
                break
            except ftt.FTTError, detail:
                Trace.log(e_errors.ERROR, "ftt open dev: %s %s" %(detail, detail.value))
                if detail.errno == ftt.EBUSY:
                    time.sleep(5)
                    #Trace.trace(42, "ftt.close_dev()")
                    self.ftt.close_dev()  ## XXX Added by Bakken and Moibenko. Do we really need it?
                    #Trace.trace(42, "ftt.close_dev() done")
                elif detail.errno == ftt.EROFS:
                    ###XXX HACK!  Tape may have write-protect tab set.  But we really
                    ### ought to get readonly status of the tape from the volume database
                    Trace.log(e_errors.INFO, "ftt open dev: %s %s: reopening read-only" %(detail, detail.value))
                    #Trace.trace(42, "ftt.close()")
                    self.ftt.close()
                    #Trace.trace(42, "ftt.close() done")
                    #Trace.trace(42, "ftt.open(%s,%s)"%(self.device,ftt.RDONLY))
                    self.ftt = ftt.open(self.device, ftt.RDONLY)
                    self.mode = 0
                    #Trace.trace(42, "%s=ftt.open(%s,%s) done"%(self.ftt,self.device,ftt.RDONLY))
                elif detail.errno == ftt.SUCCESS: ###XXX hack - why are we getting this?
                    Trace.log(e_errors.ERROR, "CGW: got SUCCESS on open, why?")
                    try:
                        #Trace.trace(42, "ftt.close_dev()")
                        self.ftt.close_dev()
                        #Trace.trace(42, "ftt.close_dev() done")
                    except ftt.FTTError, detail:
                        Trace.log(e_errors.ERROR, "ftt close dev error: %s %s"%(detail, detail.errno))
                    except:
                        pass
                    Trace.log(e_errors.ERROR, "raising %s"%(detail,))
                    raise ftt.FTTError, detail
                    #time.sleep(5)
                else:
                    break
            except:
                Trace.handle_error()

    def rewind(self):
        try:
            #Trace.trace(42, "ftt.rewind()")
            r = self.ftt.rewind()
            #Trace.trace(42, "%s=ftt.rewind() done" % (r,))
            return r
        except ftt.FTTError, detail:
            Trace.log(e_errors.ERROR, "rewind: %s %s" % (detail, detail.value))
            return -1

    def tell(self):
        if not self.ftt:
            Trace.log(e_errors.ERROR, "tell: no ftt descriptor")
            return None, None
        try:
            #Trace.trace(42, "ftt.get_position()")
            fil, block = self.ftt.get_position()
            #Trace.trace(42, "%s,%s=ftt.get_position() done" % (fil, block,))
        except ftt.FTTError, detail:
            Trace.log(e_errors.ERROR, "tell: %s %s" % (detail, detail.value))
            raise ftt.FTTError, detail
        except:
            exc, detail, tb = sys.exc_info()
            Trace.log(e_errors.ERROR,"Unexpected exception in ftt.tell %s %s"%(exc, detail))
            try:
                Trace.handle_error(exc, detail, tb)
            except:
                exc, detail, tb = sys.exc_info()
                Trace.log(e_errors.ERROR,"Exception while handling error %s %s"%(exc, detail))
            raise ftt.FTTError,detail

            #return -1
        return fil, block

    def seek(self, target, eot_ok=0): #XXX is eot_ok needed?
        if type(target)==type(""):
            target = long(target)
        try:
            #Trace.trace(42, "ftt.get_position()")
            fil, block = self.ftt.get_position()
        except ftt.FTTError, detail:
            if detail.errno == ftt.ELOST:
                Trace.log(e_errors.INFO, "seek: lost position, rewinding")
                self.rewind() #don't know tape position, must rewind
            else:
                Trace.log(e_errors.ERROR,"ftt_driver:seek: ftt error %s %s"%(detail,detail.value))
                raise ftt.FTTError, detail #some other FTT error

        #Trace.trace(42, "ftt.get_position()")
        fil, block = self.ftt.get_position()
        Trace.log(e_errors.INFO, "tape position: target %s current %s %s" % (target, fil, block,))
        #Trace.trace(42, "%s,%s=ftt.get_position() done" % (fil, block,))
        if block==0 and fil == target:
            return
        else:
            Trace.trace(25,"seek: current = %s,%s target=%s" %(fil, block, target))
        current = fil
        if target>current:
            try:
               self.ftt.skip_fm(target-current)
               Trace.log(e_errors.INFO, "skip_fm %s"%(target-current,))

            except ftt.FTTError, detail:
                if detail.errno == ftt.EBLANK and eot_ok: ##XXX is eot_ok needed?
                    ### XXX Damn, this is unrecoverable (for AIT2, at least). What to do?
                    Trace.log(e_errors.ERROR,"ftt errors positioning tape %s" % (detail, ))
                    pass
                else:
                    Trace.log(e_errors.ERROR, "ftt_driver:seek: %s %s" % (detail, detail.value))
                    raise ftt.FTTError, detail
        else:
            try:
                self.ftt.skip_fm(target-current-1)
                Trace.log(e_errors.INFO, "skip_fm %s"%(target-current-1,))
                self.ftt.skip_fm(1)
                Trace.log(e_errors.INFO, "skip_fm %s"%(1,))
            except ftt.FTTError, detail:
                Trace.log(e_errors.ERROR, "ftt_driver:skip_fm: %s %s" % (detail, detail.value))
                raise ftt.FTTError, detail
        current, block = self.tell()
        Trace.trace(25,"seek2: current=%s target=%s" % (current, target))
        if current != target:
            Trace.log(e_errors.ERROR, "ftt_driver:seek: Positioning error %s %s" % (current, target))
            #raise e_errors.POSIT_EXCEPTION
            raise ftt.FTTError(("Lost position", ftt.ELOST, "%s %s"%((current, target))))

    def skipfm(self, n):
        return self.ftt.skip_fm(n)

    def fileno(self):
        return self.fd

    def flush(self):
        if not self.ftt:
            return 0,None
        Trace.trace(25, "flushing %s" % (self.ftt))
        now=time.time()
        Trace.trace(25, "transferred %s bytes in %s seconds"%(
            self._bytes_transferred, now-self._start_time))
        if now>self._start_time and self._bytes_transferred:
            Trace.trace(25,  "rate: %.3g MB/sec" % (self._bytes_transferred/(now-self._start_time)/MB))

        try:
            #Trace.trace(42, "ftt.close_dev()")
            r = self.ftt.close_dev()
            info = None
            #Trace.trace(42, "ftt.close_dev() done")
        except ftt.FTTError, detail:
            Trace.log(e_errors.ERROR, "close_dev %s %s" % (detail, detail.value))
            r = -1
            info = detail
        Trace.trace(25, "ftt_close_dev returns %s" % (r,))
        self.fd = -1
        return r, info

    def close(self):
        Trace.trace(25, "closing %s" % (self.ftt,))
        r = -1
        if self.ftt:
            try:
                #Trace.trace(42, "ftt.close()")
                r = self.ftt.close()
                #Trace.trace(42, "%s=ftt_close() done" % (r,))
            except ftt.FTTError, detail:
                Trace.log(e_errors.ERROR, "ftt_driver:close: %s %s" % (detail, detail.value))
                r = -1
        self.ftt = None
        self.fd = -1
        return r

    def read(self, buf, offset, nbytes):
##        if self.mode != 0:
##            raise ValueError, "file not open for reading"
        if offset != 0:
            Trace.log(e_errors.ERROR, "ftt_driver:read: offset must be 0")
            raise ValueError, "offset must be 0"
        t0 = time.time()
        try:
            #Trace.trace(42, "ftt.read(buf,%s)" % (nbytes,))
            r = self.ftt.read(buf, nbytes)
            #Trace.trace(42, "%s=ftt.read(buf,%s) done" % (r,nbytes,))
        except ftt.FTTError, detail:
            Trace.log(e_errors.ERROR, "ftt_driver:read: %s %s" % (detail, detail.value))
            # re raise ftt exception
            raise ftt.FTTError, detail
        if r > 0:
            now = time.time()
            t = (now - t0)
            if self._bytes_transferred == 0:
                self._start_time = t0
            self._bytes_transferred = self._bytes_transferred + r
            if t!=0:
                self._burst_rate = r / t
                self._active_time = self._active_time + t
                self._rate = self._bytes_transferred / self._active_time
        return r

    def write(self, buf, offset, nbytes):
        if self.mode != 1:
            Trace.log(e_errors.ERROR, "ftt_driver:write: file not open for writing")
            raise ValueError, "file not open for writing"
        if offset != 0:
            Trace.log(e_errors.ERROR, "ftt_driver:write: offset must be 0")
            raise ValueError, "offset must be 0"
        t0 = time.time()
        try:
            #Trace.trace(42, "ftt.write(buf,%s)" % (nbytes,))
            r = self.ftt.write(buf, nbytes)
            #Trace.trace(42, "%s=ftt.write(buf,%s) done" % (r,nbytes,))
        except ftt.FTTError, detail:
            Trace.log(e_errors.ERROR, "ftt_driver:write: %s %s" % (detail, detail.value))
            # re raise ftt exception
            raise ftt.FTTError, detail
            #raise e_errors.WRITE_ERROR, detail
        if r > 0:
            now = time.time()
            t = (now - t0)
            if self._bytes_transferred == 0:
                self._start_time = t0
            self._bytes_transferred = self._bytes_transferred + r

            if t!=0:
                self._burst_rate = r / t
                self._active_time = self._active_time + t
                self._rate = self._bytes_transferred / self._active_time
        return r

    def writefm(self):
        ## Write one and only one filemark.  Letting ftt close the device for us
        ## results in writing two and backspacing over one.
        r=0
        try:
            #Trace.trace(42, "ftt.writefm()")
            r = self.ftt.writefm()
            #Trace.trace(42,"%s=ftt.writefm() done"%(r,))
            #### XXX Hack! Avert your eyes, innocent ones!
            ## We don't want a subsequent "close" to write extra filemarks.
            ## ftt_close_dev is being too helpful in the case where the last operation
            ## was a writefm.  So we tell a lie to ftt...
            ftt._ftt.ftt_set_last_operation(self.ftt.d, 0)
        except ftt.FTTError, detail:
            Trace.log(e_errors.ERROR, "ftt_driver:write_fm %s %s" % (detail, detail.value))
            # re raise ftt exception
            raise ftt.FTTError, detail
            #raise e_errors.WRITE_ERROR, detail
        if r==-1:
            ftt.raise_ftt()
        return r

    def writefm_buffered(self):
        ## Write one and only one filemark.  Letting ftt close the device for us
        ## results in writing two and backspacing over one.
        r=0
        try:
            Trace.trace(42, "ftt.writefm_buffered()")
            r = self.ftt.writefm_buffered()
            Trace.trace(42,"%s=ftt.writefm_buffered() done"%(r,))
            #### XXX Hack! Avert your eyes, innocent ones!
            ## We don't want a subsequent "close" to write extra filemarks.
            ## ftt_close_dev is being too helpful in the case where the last operation
            ## was a writefm.  So we tell a lie to ftt...
            ftt._ftt.ftt_set_last_operation(self.ftt.d, 0)
        except ftt.FTTError, detail:
            Trace.log(e_errors.ERROR, "ftt_driver:writefm_buffered %s %s" % (detail, detail.value))
            raise ftt.FTTError, detail
        if r==-1:
            ftt.raise_ftt()
        return r

    def flush_data(self):
        ## Write one and only one filemark.  Letting ftt close the device for us
        ## results in writing two and backspacing over one.
        r=0
        try:
            Trace.trace(42, "ftt.flush_data()")
            r = self.ftt.flush_data()
            Trace.trace(42,"%s=ftt.flush_data done"%(r,))
            #### XXX Hack! Avert your eyes, innocent ones!
            ## We don't want a subsequent "close" to write extra filemarks.
            ## ftt_close_dev is being too helpful in the case where the last operation
            ## was a writefm.  So we tell a lie to ftt...
            ftt._ftt.ftt_set_last_operation(self.ftt.d, 0)
        except ftt.FTTError, detail:
            Trace.log(e_errors.ERROR, "ftt_driver:flush_data %s %s" % (detail, detail.value))
            # re raise ftt exception
            raise ftt.FTTError, detail
            #raise e_errors.WRITE_ERROR, detail
        if r==-1:
            ftt.raise_ftt()
        return r

    def eject(self):
        try:
            #Trace.trace(42, "ftt.close()")
            self.ftt.close()
            #Trace.trace(42, "ftt.close() done")
        except:
            Trace.log(e_errors.ERROR, "eject: ftt_close failed")
        ok = 0
        for retry in (0, 1, 2):
            if retry:
                Trace.log(e_errors.ERROR, "eject: retry %s" % (retry,))
                time.sleep(5)
            p=os.popen("mt -f %s offline 2>&1" % (self.device),'r')
            r=p.read()
            s=p.close()
            del(p)
            if not s:
                ok = 1
                break
            else:
                Trace.log(e_errors.ERROR, "eject: mt offline failed: %s" % (r,))
                if string.find(r, "Input/output error") >= 0:
                    p=os.popen("mt -f %s rewind 2>&1" % (self.device),'r')
                    r=p.read()
                    s=p.close()
                    del(p)
                    if s:
                        Trace.log(e_errors.ERROR, "eject: mt rewind failed: %s" % (r,))
                elif string.find(r, "No medium found") >= 0: # there is no media no need to offline
                    ok = 1
        if not ok:
            Trace.log(e_errors.ERROR, "eject: failed after 3 tries")
            return -1
        else:
            return 0

    def set_mode(self, density=None, compression=None, blocksize=None):
        ##HACK: this seems to trigger a core dump in ftt, and it's
        ## not clear we're really changing the mode anyhow.
        ## XXX investigate this!
        ## Be very careful with density and compression
        ## possible values for them are defined in ftt_table.c and are different
        ## for differint kinds of devices
        ## NOT RECOMMENDED to specify dencity other than None
        ## compressin must be either None (default compression), or 0, or 1
        ##

        ## to actually set mode ftt.open_dev() must be used.
        #return 0

        r = -1
        try:
            #Trace.trace(42, "ftt.get_mode()")
            mode = self.ftt.get_mode()
            Trace.trace(42, "%s=ftt.get_mode() done"%(mode,))
        except ftt.FTTError, detail:
            Trace.log(e_errors.ERROR, "get_mode %s %s" % (detail, detail.value))
            return -1

        if density is None:
            density = mode[1]
        if compression is None:
            compression = mode[2]
        elif not compression in (0,1):
            Trace.log(e_errors.WARNING, "set_mode: wrong compression value %s. Using default" % (compression,))
            compression = mode[2]
        if blocksize is None:
            blocksize = mode[3]

        try:
            #Trace.trace(42, "ftt.set_mode(%s,%s,%s)"%(density,compression,blocksize,))
            r = self.ftt.set_mode(density, compression, blocksize)
            Trace.trace(42, "%s=ftt.set_mode(%s,%s,%s) done"%(r,density,compression,blocksize,))
        except ftt.FTTError, detail:
            Trace.log(e_errors.ERROR, "set_mode %s %s" % (detail, detail.value))
            return -1

        try:
            #Trace.trace(42, "ftt.open_dev()")
            self.fd = self.ftt.open_dev()
            #Trace.trace(42, "%s=ftt.open_dev() done"%(self.fd,))
        except ftt.FTTError, detail:
            Trace.log(e_errors.ERROR, "open_dev %s %s" % (detail, detail.value))
            return -1
        Trace.trace(25, "set mode: new mode is %s" % (self.ftt.get_mode(),))
        return r

    # labels that are possible at each position in the vol header
    VOL2 = "VOL2"
    VOL3 = "VOL3"
    VOL4 = "VOL4"
    VOL5 = "VOL5"
    VOL6 = "VOL6"
    VOL7 = "VOL7"
    VOL8 = "VOL8"
    VOL9 = "VOL9"
    UVL1 = "UVL1"
    UVL2 = "UVL2"
    UVL3 = "UVL3"
    UVL4 = "UVL4"
    UVL5 = "UVL5"
    UVL6 = "UVL6"
    UVL7 = "UVL7"
    UVL8 = "UVL8"
    UVL9 = "UVL9"

    labels = {80  : [VOL2, UVL1],
              160 : [VOL3, UVL1, UVL2],
              240 : [VOL4, UVL1, UVL2, UVL3],
              320 : [VOL5, UVL1, UVL2, UVL3, UVL4],
              400 : [VOL6, UVL1, UVL2, UVL3, UVL4, UVL5],
              480 : [VOL7, UVL1, UVL2, UVL3, UVL4, UVL5, UVL6],
              560 : [VOL8, UVL1, UVL2, UVL3, UVL4, UVL5, UVL6, UVL7],
              640 : [VOL9, UVL1, UVL2, UVL3, UVL4, UVL5, UVL6, UVL7, UVL8],
              720 : [UVL1, UVL2, UVL3, UVL4, UVL5, UVL6, UVL7, UVL8, UVL9],
              800 : [UVL2, UVL3, UVL4, UVL5, UVL6, UVL7, UVL8, UVL9],
              880 : [UVL3, UVL4, UVL5, UVL6, UVL7, UVL8, UVL9],
              960 : [UVL4, UVL5, UVL6, UVL7, UVL8, UVL9],
              1040 : [UVL5, UVL6, UVL7, UVL8, UVL9],
              1120 : [UVL6, UVL7, UVL8, UVL9],
              1200 : [UVL7, UVL8, UVL9],
              1280 : [UVL8, UVL9],
              1360 : [UVL9]
              }

    def check_addtl_hdrs(self, nbytes, buf, mode):
        # check for VOL2 - VOL9 and
        #           UVL1 - UVL9
        bytes_l = self.labels.keys()
        for bytes in bytes_l:
            if nbytes > bytes:
                if buf[bytes:bytes+4] not in self.labels[bytes]:
                    # we found a label in a spot where it should not be
                    return {0:"READ_ERR-at-%s"%(bytes,),
                            1:"WRITE_ERR-at-%s"%(bytes,)}[mode]
        return None


    # the expected length is set to the absolute maximum volume header length that
    # we could possibly get (VOL1-VOL9 + UVL1-UVL9, each 80 bytes = 1440)
    def verify_label(self, volume_label=None, mode=0, expected_length=1440):

        buf=expected_length*' '
        try:
            Trace.trace(25, "rewinding tape to check volume label")
            r=self.rewind()
            Trace.trace(25, "rewind returned %s"%(r,))
            self.set_mode(compression = 0, blocksize = 0)

            if self.fd is None:
                return {0:e_errors.READ_BADSWMOUNT, 1:e_errors.WRITE_BADSWMOUNT}[mode], None
            try:
                nbytes=self.read(buf, 0, expected_length)
            except e_errors.READ_ERROR, detail:
                Trace.log(e_errors.ERROR, "verify_label returned: %s"%(detail,))
                nbytes = 0
            # the returned data should be a modulo of 80.
            if not nbytes or nbytes%80 != 0:
                Trace.trace(25, "read %s bytes checking label" % (nbytes,))
                return {0:e_errors.READ_VOL1_READ_ERR, 1:e_errors.WRITE_VOL1_READ_ERR}[mode], None
            Trace.trace(25, "verify_label: read %s" % (buf,))
            if buf[:4] != "VOL1":
                return {0:e_errors.READ_VOL1_MISSING, 1:e_errors.WRITE_VOL1_MISSING}[mode], None
            rtn = self.check_addtl_hdrs(nbytes, buf, mode)
            if rtn:
                return rtn, None
            s = string.split(buf[4:])
            if not s:
                return {0:e_errors.READ_VOL1_MISSING, 1:e_errors.WRITE_VOL1_MISSING}[mode], None
            if volume_label is None:
                return e_errors.OK, s[0]
            if s[0] != volume_label[0:6]:
                Trace.log(e_errors.ERROR, "ftt_driver:verify_label read label returned %s" % (s,))
                return {0:e_errors.READ_VOL1_WRONG, 1:e_errors.WRITE_VOL1_WRONG}[mode], s[0]

            return e_errors.OK, None
        except ftt.FTTError, detail:
             Trace.log(e_errors.ERROR, "ftt exception reading VOL1 label: %s" % (detail,))
             ftt_error = _ftt.ftt_get_error()
             Trace.log(DEBUG_LOG, 'ftt errors %s'%(ftt_error,))
             return {0:e_errors.READ_VOL1_READ_ERR, 1:e_errors.WRITE_VOL1_READ_ERR}[mode], str(detail), ftt_error
        except:
            # sometimes verify_label throws a non ftt.FTTError exception but ftt_get_error()
            # can still tell us whats going on
            exc, msg = sys.exc_info()[:2]
            Trace.log(e_errors.ERROR, "reading VOL1 label: %s %s" % (exc, msg))
            ftt_error = _ftt.ftt_get_error()
            return {0:e_errors.READ_VOL1_READ_ERR, 1:e_errors.WRITE_VOL1_READ_ERR}[mode], str(exc)+str(msg), ftt_error

    def rates(self):
        """returns a tuple (overall rate, instantaneous rate)"""
        return self._rate, self._burst_rate

    def tape_transfer_time(self):
        return self._active_time

    def get_cleaning_bit(self):
        clean = 0
        # to avoid writing filemark during get_stats operation
        # set last operation to 0
        if self.ftt:
            ftt._ftt.ftt_set_last_operation(self.ftt.d, 0)
            stats = self.ftt.get_stats()
            if stats:
                try:
                    clean = int(stats[ftt.CLEANING_BIT])
                except:
                    pass
        return clean

    def get_stats(self):
        stats = None
        # to avoid writing filemark during get_stats operation
        # set last operation to 0
        # to avoid writing filemark during get_stats operation
        # set last operation to 0
        if self.ftt:
            ftt._ftt.ftt_set_last_operation(self.ftt.d, 0)
            stats = self.ftt.get_stats()
        return stats

if __name__ == "__main__":   # pragma: no cover

    print "TEST ME!"





