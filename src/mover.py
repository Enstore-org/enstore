#!/usr/bin/env python
# $Id$

# python modules
import sys
import os
import threading
import errno
import pprint
import socket
import signal                           
import time                             
import string
import struct
import select
import exceptions
import traceback
import fcntl, FCNTL
import random
import popen2

# enstore modules

import configuration_client
import setpath
import generic_server
import event_relay_client
import monitored_server
import inquisitor_client
import enstore_functions
import enstore_functions2
import enstore_constants
import option
import dispatching_worker
import volume_clerk_client
import volume_family
import file_clerk_client                
import media_changer_client             
import callback
import checksum
import e_errors
import udp_client
import socket_ext
import hostaddr
import string_driver
import disk_driver
import accounting_client
import drivestat_client
import Trace


"""
Mover:

  Any single mount or dismount failure ==> state=BROKEN, vol=NOACCESS
        (At some point, we'll want single failures to be 2-3 failures)

  Any single eject failure ==> state=BROKEN, vol=NOACCESS, no dismount
                                  attempt, tape stays in drive.

  Two consecutive transfer failures ==> state=BROKEN
        Exclude obvious failures like encp_gone.

  Any 3 transfer failures within an hour ==> state=BROKEN


  By BROKEN, I mean the normal ERROR state plus:
        a. Alarm is raised.
        b. Nanny doesn't fix it.   (enstore sched --down xxx   stops nanny)
        c. It's sticky across mover process restarts.
        d. Admin has to investigate and take drive out of BROKEN state.

I know I'm swinging way to far to the right and I know this is more
work for everyone, especially the admins.  Could you both please
comment and maybe suggest other things. I just don't want to come in
some morning and have all the D0 tapes torn apart and have the
possibility exist that Enstore could have done more to prevent it.


Jon
"""

"""TODO:

Make a class for EOD objects to handle all conversions from loc_cookie to integer
automatically.  It's confusing in this code when `eod' is a cookie vs a plain old int.


"""



class MoverError(exceptions.Exception):
    def __init__(self, arg):
        exceptions.Exception.__init__(self,arg)


#states
IDLE, SETUP, MOUNT_WAIT, SEEK, ACTIVE, HAVE_BOUND, DISMOUNT_WAIT, DRAINING, OFFLINE, CLEANING, ERROR, FINISH_WRITE = range(12)

_state_names=['IDLE', 'SETUP', 'MOUNT_WAIT', 'SEEK', 'ACTIVE', 'HAVE_BOUND', 'DISMOUNT_WAIT',
             'DRAINING', 'OFFLINE', 'CLEANING', 'ERROR', 'FINISH_WRITE']

##assert len(_state_names)==11

def state_name(state):
    return _state_names[state]

#modes
READ, WRITE, ASSERT = range(3)

#error sources
TAPE, ROBOT, NETWORK = ['TAPE', 'ROBOT', 'NETWORK']

def mode_name(mode):
    if mode is None:
        return None
    else:
        return ['READ','WRITE','ASSERT'][mode]

#KB=1L<<10
MB=1L<<20
#GB=1L<<30

SANITY_SIZE = 65536

#used in the threshold calculation
TRANSFER_THRESHOLD = 2*1024*1024

def get_transfer_notify_threshold(bytes_to_transfer):
    if TRANSFER_THRESHOLD * 5 > bytes_to_transfer:
        threshold = bytes_to_transfer / 5
    elif TRANSFER_THRESHOLD * 100 < bytes_to_transfer:
        threshold = bytes_to_transfer / 100
    else:
        threshold = TRANSFER_THRESHOLD

    return threshold

def is_threshold_passed(bytes_transfered, bytes_notified, bytes_to_transfer):
    #If transfer is complete, indicate notify message to be sent.
    if bytes_transfered == bytes_to_transfer:
        return 1
    #If threshold passed, indicate notify message to be sent.
    elif bytes_transfered - bytes_notified > \
             get_transfer_notify_threshold(bytes_to_transfer):
        return 1
    #elif bytes_notified==0:
    #    return 1
    else:
        return 0

class Buffer:
    def __init__(self, blocksize, min_bytes = 0, max_bytes = 1*MB):
        self.blocksize = blocksize
        self.min_bytes = min_bytes
        self.max_bytes = max_bytes
        self.complete_crc = 0L
        self.sanity_crc = 0L
        self.sanity_bytes = 0L
        self.header_size = None
        self.trailer_size = 0L
        self.file_size = 0L
        self.bytes_written = 0L

        self.read_ok = threading.Event()
        self.write_ok = threading.Event()
        
        self._lock = threading.Lock()
        self._buf = []
        self._buf_bytes = 0L
        self._freelist = []
        self._reading_block = None
        self._writing_block = None
        self._read_ptr = 0
        self._write_ptr = 0
        self.wrapper = None
        self.first_block = 1
        self.bytes_for_crc = 0L
        self.trailer_pnt = 0L
        
    def set_wrapper(self, wrapper):
        self.wrapper = wrapper

    def save_settings(self):
        self.saved_buf_bytes = self._buf_bytes
        self.saved_reading_block = self._reading_block
        self.saved_writing_block = self._writing_block
        self.saved_read_ptr = self._read_ptr
        self.saved_write_ptr = self._write_ptr
        self.saved_complete_crc = self.complete_crc
        self.saved_sanity_crc = self.sanity_crc
        self.saved_sanity_bytes = self.sanity_bytes
        self.saved_header_size = self.header_size
        self.saved_trailer_size = self.trailer_size
        self.saved_file_size = self.file_size
        self.saved_bytes_written = self.bytes_written
        self.saved_sanity_cookie = self.sanity_cookie
        self.saved_wrapper = self.wrapper

    def restore_settings(self):
        self._buf_bytes = self.saved_buf_bytes
        self._reading_block = self.saved_reading_block
        self._writing_block = self.saved_writing_block
        self._read_ptr = self.saved_read_ptr
        self._write_ptr = self.saved_write_ptr
        self.complete_crc = self.saved_complete_crc
        self.sanity_crc = self.saved_sanity_crc
        self.sanity_bytes = self.saved_sanity_bytes
        self.header_size = self.saved_header_size
        self.trailer_size = self.saved_trailer_size
        self.file_size = self.saved_file_size
        self.bytes_written = self.saved_bytes_written
        self.sanity_cookie = self.saved_sanity_cookie
        self.wrapper = self.saved_wrapper
        
    def reset(self, sanity_cookie, client_crc_on):
        self._lock.acquire()
        self.read_ok.set()
        self.write_ok.clear()
        
        self._buf = []
##        self._freelist = []   keep this around to save on malloc's
        self._buf_bytes = 0
        self._reading_block = None
        self._writing_block = None
        self._read_ptr = 0
        self._write_ptr = 0
        self.complete_crc = 0L
        self.sanity_crc = 0L
        self.sanity_bytes = 0L
        self.header_size = None
        self.trailer_size = 0L
        self.file_size = 0L
        self.bytes_written = 0L
        self._lock.release()
        self.sanity_cookie = sanity_cookie
        self.client_crc_on = client_crc_on
        self.wrapper = None
        self.first_block = 1
        
    def clear(self):
        self._lock.acquire()
        l = len(self._buf)
        for i in range(l):
            self._buf.pop(0)
        l = len(self._freelist)
        for i in range(l):
            self._freelist.pop(0)
        self._lock.release()
        
        self.write_ok.clear()
    def nbytes(self):
        return self._buf_bytes
        
    def full(self):
        return self.nbytes() >= self.max_bytes
    
    def empty(self):
        ## this means that a stream write would fail - we have no data at all to send
        self._lock.acquire()
        r =  len(self._buf) == 0 and not self._writing_block
        self._lock.release()
        return r
    
    def low(self):
        ## this means that either we don't have enough data for a full block,
        ## or we're deferring writes until enough data is buffered (min_bytes)
        self._lock.acquire()
        r = len(self._buf) == 0 or self.nbytes() < self.min_bytes
        self._lock.release()
        return r
    
    def set_min_bytes(self, min_bytes):
        self.min_bytes = min_bytes
        
    def set_blocksize(self, blocksize):
        if blocksize == self.blocksize:
            return
        if self.nbytes() != 0:
            raise "Buffer error: changing blocksize of nonempty buffer"
        self._lock.acquire()
        self._freelist = []
        self.blocksize = blocksize
        self._lock.release()
    
    def push(self, data):
        self._lock.acquire()
        self._buf.append(data)
        self._buf_bytes = self._buf_bytes + len(data)
        self._lock.release()
        
    def pull(self):
        self._lock.acquire()
        data = self._buf.pop(0)
        self._buf_bytes = self._buf_bytes - len(data)
        self._lock.release()
        return data
        
    def nonzero(self):
        return self.nbytes() > 0
    
    def __repr__(self):
        return "Buffer %s  %s  %s" % (self.min_bytes, self._buf_bytes, self.max_bytes)

    def block_read(self, nbytes, driver, fill_buffer=1):

        if self.client_crc_on:
            # calculate checksum when reading from
            # tape (see comment in setup_transfer)
            do_crc = 1
        else:
            do_crc = 0
        data = None
        partial = None
        space = self._getspace()
        #Trace.trace(22,"block_read: bytes_to_read: %s"%(nbytes,))
        #bytes_read = driver.read(space, 0, nbytes)
        bytes_read = driver.read(space, 0, self.blocksize)
        #Trace.trace(22,"block_read: bytes_read: %s"%(bytes_read,))
        if bytes_read == nbytes: #normal case
            data = space
        elif bytes_read<=0: #error
            Trace.trace(25, "block_read: read %s" % (bytes_read,))
            pass #XXX or raise an exception?
        else: #partial block read
            if bytes_read > nbytes:
                bytes_read = nbytes
            Trace.trace(25, "partial block (%s/%s) read" % (bytes_read,nbytes))
            data = space[:bytes_read]
            partial = 1

        data_ptr = 0
        bytes_for_crc = bytes_read
        bytes_for_cs = bytes_read

        if self.first_block: #Handle variable-sized cpio header
            ##if len(self.buffer._buf) != 1:
            ##        Trace.log(e_errors.ERROR,
            ##                  "block_read: error skipping over cpio header, len(buf)=%s"%(len(self.buffer._buf)))
            if len(data) >= self.wrapper.min_header_size:
                try:
                    header_size = self.wrapper.header_size(data)
                except (TypeError, ValueError), msg:
                    Trace.log(e_errors.ERROR,"Invalid header %s" %(data[:self.wrapper.min_header_size]))
                    raise "WRAPPER_ERROR"
                data_ptr = header_size
                bytes_for_cs = min(bytes_read - header_size, self.bytes_for_crc)
            self.first_block = 0
        if do_crc:
            crc_error = 0
            try:
                #Trace.trace(22,"block_read: data_ptr %s, bytes_for_cs %s" % (data_ptr, bytes_for_cs))

                self.complete_crc = checksum.adler32_o(self.complete_crc,
                                                       data,
                                                       data_ptr, bytes_for_cs)
                if self.sanity_bytes < SANITY_SIZE:
                    nbytes = min(SANITY_SIZE-self.sanity_bytes, bytes_for_cs)
                    self.sanity_crc = checksum.adler32_o(self.sanity_crc,
                                                         data,
                                                         data_ptr, nbytes)
                    self.sanity_bytes = self.sanity_bytes + nbytes
                    #Trace.trace(22, "block_read: sanity cookie %s sanity_crc %s sanity_bytes %s" %
                    #            (self.sanity_cookie, self.sanity_crc,
                    #             self.sanity_bytes))
                else:
                    # compare sanity crc
                    if self.sanity_cookie and self.sanity_crc != self.sanity_cookie[1]:
                        Trace.log(e_errors.ERROR, "CRC Error: CRC sanity cookie %s, actual (%s,%s)" %
                                  (self.sanity_cookie, self.sanity_bytes, self.sanity_crc)) 
                        if self.sanity_cookie != (None, None): # special case to fix bfid db
                            crc_error = 1   
                data_ptr = data_ptr + bytes_for_cs
            except:
                Trace.log(e_errors.ERROR,"block_read: CRC_ERROR")
                Trace.handle_error()
                raise "CRC_ERROR"
            if crc_error:
                Trace.log(e_errors.ERROR,"block_read: CRC_ERROR")
                raise "CRC_ERROR"
                
##        Trace.trace(100, "block_read: len(buf)=%s"%(len(self._buf),)) #XXX remove CGW
        if data and fill_buffer:
            self.push(data)
            if partial:
                self._freespace(space)
                
        return bytes_read

    def block_write(self, nbytes, driver):
        #Trace.trace(22,"block_write: bytes %s"%(nbytes,))
        
        if self.client_crc_on:
            # calculate checksum when reading from
            # tape (see comment in setup_transfer)
            do_crc = 1
        else:
            do_crc = 0
        #Trace.trace(22,"block_write: header size %s"%(self.header_size,))
        data = self.pull() 
        if len(data)!=nbytes:
            raise ValueError, "asked to write %s bytes, buffer has %s" % (nbytes, len(data))
        bytes_written = driver.write(data, 0, nbytes)
        if bytes_written == nbytes: #normal case
            #Trace.trace(22, "block_write: bytes written %s" % (self.bytes_written,))
            number_to_skip = 0L
            if do_crc:
                data_ptr = 0  # where data for CRC starts
                bytes_for_cs = bytes_written
                if self.first_block: #Handle variable-sized cpio header
                    #skip over the header
                    data_ptr = data_ptr + self.header_size
                    number_to_skip = self.header_size
                    #bytes_for_cs = bytes_for_cs - self.header_size
                    if len(data) <= self.header_size:
                        raise "WRAPPER_ERROR"
                    self.first_block = 0
                #Trace.trace(22, "block_write: written in this shot %s" % (bytes_written,))
                
                if self.bytes_written >= self.trailer_pnt:
                    number_to_skip = bytes_written
                elif self.bytes_written+bytes_written > self.trailer_pnt:
                    number_to_skip = number_to_skip + self.bytes_written + bytes_written - self.trailer_pnt

                bytes_for_cs = bytes_for_cs - number_to_skip

                #Trace.trace(22, "nbytes %s, bytes written %s, bytes for cs %s trailer size %s"%
                #            (nbytes, bytes_written, bytes_for_cs,self.trailer_size))
                if bytes_for_cs:
                    try:
                        #Trace.trace(22,"block_write: data_ptr: %s, bytes_for_cs %s" %
                        #            (data_ptr, bytes_for_cs))
                        self.complete_crc = checksum.adler32_o(self.complete_crc,
                                                               data,
                                                               data_ptr, bytes_for_cs)

                        #if self.first_block and self.sanity_bytes < SANITY_SIZE:
                        if self.sanity_bytes < SANITY_SIZE:
                            nbytes = min(SANITY_SIZE-self.sanity_bytes, bytes_for_cs)
                            self.sanity_crc = checksum.adler32_o(self.sanity_crc,
                                                                 data,
                                                                 data_ptr, nbytes)
                            self.sanity_bytes = self.sanity_bytes + nbytes
                            #Trace.trace(22, "block_write: sanity_crc %s sanity_bytes %s" %
                            #            (self.sanity_crc, self.sanity_bytes))
                    except:
                        Trace.log(e_errors.ERROR,"block_write: CRC_ERROR")
                        raise "CRC_ERROR"
            self._freespace(data)
            self.bytes_written = self.bytes_written + bytes_written

        else: #XXX raise an exception?
            Trace.trace(22,"actually written %s" % (bytes_written,))
            self._freespace(data)
        return bytes_written

        
    def stream_read(self, nbytes, driver):
        if not self.client_crc_on:
            # calculate checksum when reading from
            # the network (see comment in setup_transfer)
            # CRC when receiving from the network if client does not CRC
            do_crc = 1
        else:
            do_crc = 0
            
        if type(driver) is type (""):
            driver = string_driver.StringDriver(driver)
        if isinstance(driver, string_driver.StringDriver):
            do_crc = 0
        if not self._reading_block:
            self._reading_block = self._getspace()
            self._read_ptr = 0
        bytes_to_read = min(self.blocksize - self._read_ptr, nbytes)
        bytes_read = driver.read(self._reading_block, self._read_ptr, bytes_to_read)
        if do_crc:
            #Trace.trace(22,"nbytes %s, bytes_to_read %s, bytes_read %s" %
            #            (nbytes, bytes_to_read, bytes_read))
            self.complete_crc = checksum.adler32_o(self.complete_crc, self._reading_block,
                                                   self._read_ptr, bytes_read)
            if self.sanity_bytes < SANITY_SIZE:
                nbytes = min(SANITY_SIZE-self.sanity_bytes, bytes_read)
                self.sanity_crc = checksum.adler32_o(self.sanity_crc, self._reading_block,
                                                     self._read_ptr, nbytes)
                self.sanity_bytes = self.sanity_bytes + nbytes
        self._read_ptr = self._read_ptr + bytes_read
        if self._read_ptr == self.blocksize: #we filled up  a block
            self.push(self._reading_block)
            self._reading_block = None
            self._read_ptr = 0
        return bytes_read

    def eof_read(self):
        Trace.trace(10, "EOF reached, %s"%(self._read_ptr,))
        if self._reading_block and self._read_ptr:
            data = self._reading_block[:self._read_ptr]
            self.push(data)
            self._reading_block = None
            self._read_ptr = None
    
    def stream_write(self, nbytes, driver):
        if not self.client_crc_on:
            # calculate checksum when writing to
            # the network (see comment in setup_transfer)
            # CRC when sending to the network if client does not CRC
            do_crc = 1
        else:
            do_crc = 0
        Trace.trace(8, "stream_write do_crc %s"%(do_crc,))
        if not self._writing_block:
            if self.empty():
                Trace.trace(10, "stream_write: buffer empty")
                return 0
            self._writing_block = self.pull()
            self._write_ptr = 0
        bytes_to_write = min(len(self._writing_block)-self._write_ptr, nbytes)
        if driver:
            bytes_written = driver.write(self._writing_block, self._write_ptr, bytes_to_write)
            if bytes_written != bytes_to_write:
                Trace.trace(e_errors.ERROR, "encp gone? bytes to write %s, bytes written %s"%
                            (bytes_to_write, bytes_written)) 
                raise e_errors.ENCP_GONE
            if do_crc:
                self.complete_crc = checksum.adler32_o(self.complete_crc,
                                                       self._writing_block,
                                                       self._write_ptr, bytes_written)
                if self.sanity_bytes < SANITY_SIZE:
                    nbytes = min(SANITY_SIZE-self.sanity_bytes, bytes_written)
                    self.sanity_crc = checksum.adler32_o(self.sanity_crc,
                                                         self._writing_block,
                                                         self._write_ptr, nbytes)
                    self.sanity_bytes = self.sanity_bytes + nbytes

                    #Trace.trace(22, "stream_write: sanity cookie %s sanity_crc %s sanity_bytes %s" %
                    #            (self.sanity_cookie, self.sanity_crc,self.sanity_bytes))
                    # compare sanity crc
                    if self.sanity_cookie and self.sanity_crc != self.sanity_cookie[1]:
                        Trace.log(e_errors.ERROR,
                                  "CRC Error: CRC sanity cookie %s, sanity CRC %s writing %s bytes. Written %s bytes" % (self.sanity_cookie[1],self.sanity_crc, bytes_to_write, nbytes)) 
                        raise "CRC_ERROR"
        else:
            bytes_written = bytes_to_write #discarding header stuff
        self._write_ptr = self._write_ptr + bytes_written
        if self._write_ptr == len(self._writing_block): #finished sending out this block
            self._freespace(self._writing_block)
            self._writing_block = None
            self._write_ptr = 0
        return bytes_written

    def _getspace(self):
        self._lock.acquire()
        if self._freelist:
            r =  self._freelist.pop(0)
        else:
            r = struct.pack("%ss" % (self.blocksize,), '')
        self._lock.release()
        return r
    
    def _freespace(self, s):
        if len(s) != self.blocksize:
            return # don't need this partial block around!
        self._lock.acquire()
        self._freelist.append(s)
        self._lock.release()
    
def cookie_to_long(cookie): # cookie is such a silly term, but I guess we're stuck with it :-(
    if type(cookie) is type(0L):
        return long(cookie)
    if type(cookie) is type(0):
        return long(cookie)
    if type(cookie) != type(''):
        raise TypeError, "expected string or integer, got %s %s" % (cookie, type(cookie))
    if '_' in cookie:
        part, block, file = string.split(cookie, '_')
    else:
        file = cookie
    if file[-1]=='L':
        file = file[:-1]
    return long(file)

def loc_to_cookie(loc):
    if type(loc) is type(""):
        loc = cookie_to_long(loc)
    if loc is None:
        loc = 0
    return '%04d_%09d_%07d' % (0, 0, loc)

_host_type = None

Linux, IRIX, Solaris, Other = range(4)

def host_type():
    global _host_type
    if _host_type:
        return _host_type
    uname = string.upper(os.uname()[0])
    _host_type = {'linux':Linux, 'irix':IRIX, 'sunos': Solaris}.get(uname, Other)
    return _host_type
    
class Mover(dispatching_worker.DispatchingWorker,
            generic_server.GenericServer):

    def __init__(self, csc_address, name):
        generic_server.GenericServer.__init__(self, csc_address, name)
        self.name = name
        self.shortname = name
        self.unique_id = None #Unique id of last transfer, whether success or failure
        self.notify_transfer_threshold = 2*1024*1024
        self.state_change_time = 0.0
        self.time_in_state = 0.0
        self.in_state_to_cnt = 0 # how many times timeot for being in the same state expired
        self.connect_to = 15  # timeout for control socket connection
        self.connect_retry = 4 # number of retries for control socket connection 
        self._state_lock = threading.Lock()
        if self.shortname[-6:]=='.mover':
            self.shortname = name[:-6]
        self.draining = 0
        self.log_mover_state = None
        self.override_ro_mount = None # if set override readonly mount MC option
        
        # self.need_lm_update is used in threads to flag LM update in
        # the main thread. First element flags update if not 0,
        # second - state
        # third -  reset timer
        # fourth - error source
        self.need_lm_update = (0, None, 0, None)
        self.asc = None
        self.send_update_cnt = 0
        
    def __setattr__(self, attr, val):
        #tricky code to catch state changes
        try:
            if attr == 'state':
                if val != getattr(self, 'state', None):
                    Trace.notify("state %s %s" % (self.shortname, state_name(val)))
                if val != SETUP:
                    self.tmp_vol = None
                    self.tmp_vf = None
                self.time_in_state = 0.0
                self.in_state_to_cnt = 0
                self.__dict__['state_change_time'] = time.time()
        except:
            pass #don't want any errors here to stop us
        self.__dict__[attr] = val

    def init_data_buffer(self):
        if self.buffer:
            self.buffer.clear()
            del(self.buffer)
        self.buffer = Buffer(0, self.min_buffer, self.max_buffer)
        if self.log_mover_state:
            cmd = "EPS | grep %s"%(self.name,)
            pipeObj = popen2.Popen3(cmd, 0, 0)
            if pipeObj is None:
                return
            stat = pipeObj.wait()
            result = pipeObj.fromchild.readlines()  # result has returned string
            Trace.log(e_errors.INFO,"Init d_b LOG: PS %s"%(result,))

        
    def return_state(self):
        return state_name(self.state)

    def log_state(self):
        if self.log_mover_state:
            cmd = "EPS | grep %s"%(self.name,)
            pipeObj = popen2.Popen3(cmd, 0, 0)
            if pipeObj is None:
                return
            stat = pipeObj.wait()
            result = pipeObj.fromchild.readlines()  # result has returned string
            Trace.log(e_errors.INFO,"LOG: PS %s"%(result,))
            thread = threading.currentThread()
            if thread:
                thread_name = thread.getName()
            else:
                thread_name = None
            Trace.log(e_errors.INFO,"LOG: CurThread %s"%(thread_name))
          
            # see what threads are running
            threads = threading.enumerate()
            for thread in threads:
                if thread.isAlive():
                    thread_name = thread.getName()
                    Trace.log(e_errors.INFO,"LOG: Thread %s is running" % (thread_name,))
                else:
                    Trace.log(e_errors.INFO,"LOG(%s): Thread is dead"%(thread_name,))
            

    def watch_syslog(self):
        if self.syslog_match:
            try:
                cmd = "$ENSTORE_DIR/src/match_syslog.py '%s'"%(self.syslog_match)
                pipeObj = popen2.Popen3(cmd, 0, 0)
                if pipeObj is None:
                    return
                stat = pipeObj.wait()
                result = pipeObj.fromchild.readlines()  # result has returned string
                if result:
                    for l in result:
                        Trace.log(e_errors.INFO,"SYSLOG Entry:[%s] %s"%(l[:-1],self.current_volume))
            except: # do not know what kind of exception it may be
                Trace.handle_error()

    def lock_state(self):
        self._state_lock.acquire()

    def unlock_state(self):
        self._state_lock.release()

    # check if we are known to be down (offline) in the outage file
    # this still uses rsh to avoid a race condition with the mover starting before the
    # inquisitor
    def check_sched_down(self):
	inq = self.csc.get('inquisitor')
	host = inq.get('host')
	dir = inq.get('html_file')
	file = enstore_constants.OUTAGEFILE
	if not host:
	    return 0
	cmd = 'enrsh -n %s cat %s/%s ' % (host, dir, file)
	p = os.popen(cmd, 'r')
	r = p.read()
        # when mover restarts by 'at' p.close generates IOerror no child process
        # causing mover failure
        # to avoid this use try .. except..
        try:
            s = p.close()
        except IOError, detail:
            Trace.log(e_errors.WARNING, "error closing pipe %s"%(detail,))
            s = None
	if s:
	    Trace.log(e_errors.ERROR, "error getting outage file (%s)"%(self.name,))
	lines = string.split(r,'\n')
	for line in lines:
	    if line[0:7] == "offline":
		if string.find(line, self.name) != -1:
		    return 1
	return 0

    # set ourselves to be known down in the outage file
    def set_sched_down(self):
	self.inq = inquisitor_client.Inquisitor(self.csc)
	ticket = self.inq.down(self.name, "set by mover", 15)
	if not enstore_functions.is_ok(ticket):
	    Trace.log(e_errors.ERROR, 
		      "error setting %s as known down in outage file : %s"%(self.name,
						 enstore_functions2.get_status(ticket)))

    # get the initial statistics
    def init_stat(self, drive, drive_name):
        self.stats_on = 0
        self.send_stats = self.config.get('send_stats',None)
        if not self.stat_file: return
        if not self.driver_type == 'FTTDriver':
            return
        # create drivestat client
        try:
            self.dsc = drivestat_client.dsClient(self.csc, drive_name)
            self.stats_on = 1
        except:
            Trace.handle_error()
            pass


    def set_volume_noaccess(self, volume):
        self.vcc.set_system_noaccess(volume)
        self.vol_info.update(self.vcc.inquire_vol(volume))

    # update statistics
    def update_stat(self):
        if self.driver_type != 'FTTDriver': return
        if self.stats_on and self.tape_driver and self.tape_driver.ftt:
            import ftt
            stats = self.tape_driver.ftt.get_stats()
            Trace.log(e_errors.INFO, "volume %s write protection %s  override_ro_mount %s"%(self.current_volume,
                                                                       stats[ftt.WRITE_PROT],
                                                                       self.override_ro_mount))
            if self.stat_file:
                if not os.path.exists(self.stat_file):
                   dirname, basename = os.path.split(self.stat_file)
                   if not os.path.exists(dirname):
                       os.makedirs(dirname)
                fd = open(self.stat_file, "w")
                fd.write("FORMAT VERSION:         %d\n"%(22,))
                fd.write("INIT FLAG:              %d\n"%(1,))
                fd.write("DRIVE SERNO:            %s\n"%(stats[ftt.SERIAL_NUM],))
                fd.write("VENDOR:                 %s\n"%(stats[ftt.VENDOR_ID],))
                fd.write("PROD TYPE:              %s\n"%(stats[ftt.PRODUCT_ID],))
                fd.write("LOGICAL NAME:           %s\n"%(self.logname,))
                fd.write("HOST:                   %s\n"%(self.config['host'],))
                fd.write("VOLSER:                 %s\n"%(self.current_volume,))
                fd.write("OPERATION               %s\n"%(self.mode,))
                fd.write("CLEANING BIT:           %s\n"%(stats[ftt.CLEANING_BIT],))
                fd.write("PWR HRS:                %s\n"%(stats[ftt.POWER_HOURS],))
                fd.write("MOT HRS:                %s\n"%(stats[ftt.MOTION_HOURS],))
                fd.write("RD ERR:                 %s\n"%(stats[ftt.READ_ERRORS],))
                fd.write("WR ERR:                 %s\n"%(stats[ftt.WRITE_ERRORS],))
                fd.write("MB UREAD:               %s\n"%(long(stats[ftt.USER_READ])/1024.,))
                fd.write("MB UWRITE:              %s\n"%(long(stats[ftt.USER_WRITE])/1024.,))
                fd.write("MB DREAD:                  %s\n"%(long(stats[ftt.READ_COUNT])/1024.,))
                fd.write("MB DWRITE:                 %s\n"%(long(stats[ftt.WRITE_COUNT])/1024.,))
                fd.write("RETRIES:                %s\n"%(stats[ftt.TRACK_RETRY],))
                fd.write("WRITEPROT:               %s\n"%(stats[ftt.WRITE_PROT],))
                fd.write("UNDERRUN:               %s\n"%(stats[ftt.UNDERRUN],))
                
            if self.send_stats:
                self.dsc.log_stat(stats[ftt.SERIAL_NUM],
                                  stats[ftt.VENDOR_ID],
                                  stats[ftt.PRODUCT_ID],
                                  self.config['host'],
                                  self.logname,
                                  "ABSOLUTE",
                                  time.time(),
                                  self.current_volume,
                                  long(stats[ftt.POWER_HOURS]),
                                  long(stats[ftt.MOTION_HOURS]),
                                  stats[ftt.CLEANING_BIT],
                                  long(stats[ftt.USER_READ])/1024.,
                                  long(stats[ftt.USER_WRITE])/1024.,
                                  long(stats[ftt.READ_COUNT])/1024.,
                                  long(stats[ftt.WRITE_COUNT])/1024.,
                                  long(stats[ftt.READ_ERRORS]),
                                  long(stats[ftt.WRITE_ERRORS]),
                                  long(stats[ftt.TRACK_RETRY]),
                                  long(stats[ftt.UNDERRUN]),
                                  0,
                                  int(stats[ftt.WRITE_PROT]))
                
    def start(self):
        name = self.name
        self.t0 = time.time()
        self.config = self.csc.get(name)
        if self.config['status'][0] != 'ok':
            raise MoverError('could not start mover %s: %s'%(name, self.config['status']))
        self.logname = self.config.get('logname', name)
        Trace.init(self.logname)
        # do not restart if some mover processes are already running
        cmd = "EPS | grep %s | grep %s"%(self.name,"mover.py")
        pipeObj = popen2.Popen3(cmd, 0, 0)
        if pipeObj:
            stat = pipeObj.wait()
            result = pipeObj.fromchild.readlines()  # result has returned string
            if len(result) > 1:
                Trace.alarm(e_errors.ERROR,"mover is already running, can not restart: %s"%(result,))
                time.sleep(2)
                sys.exit(-1)
        

        Trace.log(e_errors.INFO, "starting mover %s" % (self.name,))
        
        self.config['device'] = os.path.expandvars(self.config['device'])
        self.state = IDLE
        self.force_clean = 0
        # check if device exists
        if not os.path.exists(self.config['device']):
            Trace.alarm(e_errors.ERROR, "Cannot start. Device %s does not exist"%(self.config['device'],))
            self.state = OFFLINE
            
        #how often to send an alive heartbeat to the event relay
        self.alive_interval = monitored_server.get_alive_interval(self.csc, name, self.config)
        self.address = (self.config['hostip'], self.config['port'])
        self.lm_address = None # LM that called mover
        self.do_eject = 1
        if self.config.has_key('do_eject'):
            if self.config['do_eject'][0] in ('n','N'):
                self.do_eject = 0

        self.do_cleaning = 1
        if self.config.has_key('do_cleaning'):
            if self.config['do_cleaning'][0] in ('n','N'):
                self.do_cleaning = 0
        
        self.rem_stats = 1
        if self.config.has_key('get_remaining_from_stats'):
            if self.config['get_remaining_from_stats'][0] in ('n','N'):
                self.rem_stats = 0

        self.mover_type = self.config.get('type','')
        self.ip_map = self.config.get('ip_map','')
        self.mc_device = self.config.get('mc_device', 'UNDEFINED')
        self.media_type = self.config.get('media_type', '8MM') #XXX
        self.min_buffer = self.config.get('min_buffer', 8*MB)
        self.max_buffer = self.config.get('max_buffer', 64*MB)
        self.max_rate = self.config.get('max_rate', 11.2*MB) #XXX
        self.log_mover_state = self.config.get('log_state', None)
        self.syslog_match = self.config.get("syslog_entry",None) # pattern to match in syslog for scsi error
        self.restart_on_error = self.config.get("restart_on_error", None)
        self.connect_to = self.config.get("connect_timeout", 15)
        self.connect_retry = self.config.get("connect_retries", 4)
        self.transfer_deficiency = 1.0
        self.buffer = None
        self.udpc = udp_client.UDPClient()
        self.last_error = (e_errors.OK, None)
        if self.check_sched_down() or self.check_lockfile():
            self.state = OFFLINE
        self.current_location = 0L
        self.current_volume = None #external label of current mounted volume
        self.last_location = 0L
        self.last_volume = None
        self.last_volume_family = None
        self.mode = None # READ, WRITE or ASSERT
        self.setup_mode = None
        self.compression = None # use default
        self.bytes_to_transfer = 0L
        self.bytes_to_read = 0L
        self.bytes_to_write = 0L
        self.bytes_read = 0L
        self.bytes_read_last = 0L
        self.bytes_written = 0L
        self.bytes_written_last = 0L
        self.volume_family = None 
        self.files = ('','')
        self.transfers_completed = 0
        self.transfers_failed = 0
        self.error_times = []
        self.consecutive_failures = 0
        self.max_consecutive_failures = 2
        self.max_failures = 3
        self.failure_interval = 3600
        self.current_work_ticket = {}
        self.vol_info = {}
        self.file_info = {}
        self.dismount_time = None
        self.delay = 0
        self.fcc = None
        self.vcc = None
        self.stat_file = None
        self.media_transfer_time = 0.
        self.mcc = media_changer_client.MediaChangerClient(self.csc,
                                                           self.config['media_changer'])
        self.asc = accounting_client.accClient(self.csc, self.logname)
        mc_keys = self.csc.get(self.mcc.media_changer)
        # STK robot can eject tape by either sending command directly to drive or
        # by pushing a corresponding button
        if mc_keys.has_key('type') and mc_keys['type'] is 'STK_MediaLoader':
            self.can_force_eject = 1
        else:
            self.can_force_eject = 0
        
        self.client_hostname = None
        self.client_ip = None  #NB: a client may have multiple interfaces, this is
                                         ##the IP of the interface we're using
        
        import net_driver
        self.net_driver = net_driver.NetDriver()
        self.client_socket = None

        self.config['name']=self.name 
        self.config['product_id']='Unknown'
        self.config['serial_num']=0
        self.config['vendor_id']='Unknown'
        self.config['local_mover'] = 0 #XXX who still looks at this?
        self.driver_type = self.config['driver']

        self.max_consecutive_failures = self.config.get('max_consecutive_failures',
                                                        self.max_consecutive_failures)
        self.max_failures = self.config.get("max_failures", self.max_failures)
        self.failure_interval = self.config.get("failure_interval", self.failure_interval)
        
        self.default_dismount_delay = self.config.get('dismount_delay', 60)
        if self.default_dismount_delay < 0:
            self.default_dismount_delay = 31536000 #1 year
        self.max_dismount_delay = max(
            self.config.get('max_dismount_delay', 600),
            self.default_dismount_delay)
        
        self.libraries = []
        lib_list = self.config['library']
        if type(lib_list) != type([]):
            lib_list = [lib_list]
        for lib in lib_list:
            lib_config = self.csc.get(lib)
            self.libraries.append((lib, (lib_config['hostip'], lib_config['port'])))

        #how often to send a message to the library manager
        self.update_interval = self.config.get('update_interval', 15)

        self.single_filemark=self.config.get('single_filemark', 0)
        ##Setting this attempts to optimize filemark writing by writing only
        ## a single filemark after each file, instead of using ftt's policy of always
        ## writing two and backspacing over one.  However this results in only
        ## a single filemark at the end of the volume;  causing some drives
        ## (e.g. Mammoth-1) to have trouble spacing to end-of-media.
            
        self.check_written_file_period = self.config.get('check_written_file', 0)
        self.files_written_cnt = 0
        self.max_time_in_state = self.config.get('max_time_in_state', 600) # maximal time allowed in a certain states
        self.max_in_state_cnt = self.config.get('max_in_state_cnt', 3) 
        if self.driver_type == 'NullDriver':
            self.check_written_file_period = 0 # no file check for null mover
            self.device = None
            self.single_filemark = 1 #need this to cause EOD cookies to update.
            ##XXX should this be more encapsulated in the driver class?
            import null_driver
            self.tape_driver = null_driver.NullDriver()
        elif self.driver_type == 'FTTDriver':
            self.stat_file = self.config.get('statistics_path', None)
            Trace.log(e_errors.INFO,"statitics path %s"%(self.stat_file,))
            self.compression = self.config.get('compression', None)
            if self.compression > 1: self.compression = None
            self.device = self.config['device']
            import ftt_driver
            import ftt
            self.tape_driver = ftt_driver.FTTDriver()
            have_tape = 0
            if self.state is IDLE:
                good_label = 1
                have_tape = self.tape_driver.open(self.device, mode=0, retry_count=3)

                stats = self.tape_driver.ftt.get_stats()
                self.config['product_id'] = stats[ftt.PRODUCT_ID]
                self.config['serial_num'] = stats[ftt.SERIAL_NUM]
                self.config['vendor_id'] = stats[ftt.VENDOR_ID]

                if have_tape == 1:
                    status = self.tape_driver.verify_label(None)
                    if status[0]==e_errors.OK:
                        self.current_volume = status[1]
                        self.state = HAVE_BOUND
                        Trace.log(e_errors.INFO, "have vol %s at startup" % (self.current_volume,))
                        self.dismount_time = time.time() + self.default_dismount_delay
                        self.vcc = volume_clerk_client.VolumeClerkClient(self.csc)
                        
                    else:
                        # failed to read label eject tape
                        good_label = 0
                        Trace.alarm(e_errors.ERROR, "failed to read volume label on startup")
                        ejected = self.tape_driver.eject()
                        if ejected == -1:
                            if self.can_force_eject:
                                # try to unload tape if robot is STK. It can do this
                                Trace.log(e_errors.INFO,"Eject failed. For STK robot will try to unload anyway")
                            else:
                                Trace.alarm(e_errors.ERROR, "cannot eject tape on startup, will die")
                                time.sleep(2)
                                sys.exit(-1)
                        have_tape=0
                    self.init_stat(self.device, self.logname)
                else:
                    self.tape_driver.close()
                if not have_tape:
                    Trace.log(e_errors.INFO, "performing precautionary dismount at startup")
                    vol_ticket = { "external_label": "Unknown",
                                   "media_type":self.media_type}
                    # check if media changer is open
                    mcc_reply = self.mcc.GetWork()
                    if mcc_reply['max_work'] == 0:
                        # media changer would not accept requests. Go OFFLINE
                        Trace.alarm(e_errors.ERROR, "media changer is locked, going OFFLINE")
                        self.state = OFFLINE
                    else:
                        mcc_reply = self.mcc.unloadvol(vol_ticket, self.name, self.mc_device)
                if good_label: # to prevent mover from failure in ftt_get_stats
                    if self.maybe_clean():
                        have_tape = 0
                    
        else:
            print "Sorry, only Null and FTT driver allowed at this time"
            sys.exit(-1)

        self.mount_delay = self.config.get('mount_delay',
                                           self.tape_driver.mount_delay)
        
        if type(self.mount_delay) != type(0):
            self.mount_delay = int(self.mount_delay)
        if self.mount_delay < 0:
            self.mount_delay = 0

        dispatching_worker.DispatchingWorker.__init__(self, self.address)
        self.add_interval_func(self.update_lm, self.update_interval) #this sets the period for messages to LM.
        self.add_interval_func(self.need_update, 1) #this sets the period for checking if child thread has asked for update.
        self.set_error_handler(self.handle_mover_error)
        ##start our heartbeat to the event relay process
        self.erc.start_heartbeat(self.name, self.alive_interval, self.return_state)
        ##end of __init__

    # restart itselfs
    def restart(self):
        cmd = '/usr/bin/at now+1minute'
        ecmd = "enstore Estart %s '--just %s > /dev/null'\n"%(self.config['host'],self.name) 
        p=os.popen(cmd, 'w')
        p.write(ecmd)
        p.close()
        sys.exit(0)
        
    # device_dump(self, sendto=[], notify=['enstore-admin@fnal.gov'])
    #   -- internal device dump
    #   Initially, this is mainly for M2 drives. It can be generalized
    #   for other drives.
    #
    #   if sendto is set, it is either an email address or a list of
    #   them which the dump file will be sent to.
    #
    #   if notify is set, it is either an email address or a list of
    #   them which the notification is sent to. By default, notify is
    #   set to ['enstore-admin@fnal.gov']
    #
    # device_dump_S() is a server hook for device_dump()

    # device_dump_S(self, ticket) -- server hook for device_dump()

    def device_dump_S(self, ticket):

        """
        device_dump_S(self, ticket) -- server hook for device_dump()
        """

        if ticket.has_key('sendto'):
            sendto = ticket['sendto']
        else:
            sendto = []

        if ticket.has_key('notify'):
            notify = ticket['notify']
        else:
            notify = []

        if notify:
            res = self.device_dump(sendto, notify)
        else:
            res = self.device_dump(sendto)	# take default notify

        t = {"status":(e_errors.OK, res)}
        self.reply_to_caller(t)
	return

    def device_dump(self, sendto=None, notify=['enstore-admin@fnal.gov']):
        import m2

        """
        device_dump(self, sendto=None, notify=['enstore-admin@fnal.gov'])
            -- internal device dump. This is mainly for M2 drives.

        if sendto is set, it is either an email address or a list of
        them which the dump file will be sent to.

        if notify is set, it is either an email address or a list of
        them which the notification is sent to. By default, notify is
        set to ['enstore-admin@fnal.gov']
        """

        Trace.log(e_errors.INFO, 'device_dump('+`sendto`+', '+`notify`+')')
        # print 'device_dump('+`sendto`+', '+`notify`+')'

        # self.config['product_id'] is not reliable. Leave m2probe to
        # handle non-Mammoth2 drive situation
        #
        ## do nothing if it is not a M2 drive
        #
        # if self.config['product_id'] != "Mammoth2":
        #    return "can not dump a non-Mammoth2 drive"

        res = m2.dump_code(self.device, '/tmp', sendto, notify, 'enstore mover: '+self.name)

	if res:
            Trace.log(e_errors.INFO, res)

	return res
 
    def check_written_file(self):
        if self.check_written_file_period:
            ran = random.randrange(self.check_written_file_period,self.check_written_file_period*10,1)
            if (ran % self.check_written_file_period == 0):
                return 1
            else:
                return 0
        else:
            return 0
        
    def nowork(self, ticket):
        x =ticket # to trick pychecker
        return {}

    def handle_mover_error(self, exc, msg, tb):
        x = tb # to trick pychecker
        Trace.log(e_errors.ERROR, "handle mover error %s %s"%(exc, msg))
        Trace.trace(10, "%s %s" %(self.current_work_ticket, state_name(self.state)))
        if self.current_work_ticket:
            try:
                Trace.trace(10, "handle error: calling transfer failed, str(msg)=%s"%(str(msg),))
                self.transfer_failed(exc, msg)
            except:
                pass

    ## This is the function which is responsible for updating the LM.
    def update_lm(self, state=None, reset_timer=None, error_source=None):
        self.need_lm_update = (0, None, 0, None)
        if state is None:
            state = self.state
        
        Trace.trace(20,"update_lm: %s %s" % (state_name(state), self.unique_id))
        thread = threading.currentThread()
        if thread:
            thread_name = thread.getName()
        else:
            thread_name = None
        Trace.trace(20, "update_lm: thread %s"% (thread_name,))

        if not hasattr(self,'_last_state'):
            self._last_state = None

        now = time.time()

        if reset_timer:
            self.reset_interval_timer(self.update_lm)

        # check time in state
        if (thread_name is 'MainThread'):
            time_in_state = int(now - self.state_change_time)
            if not hasattr(self,'time_in_state'):
                self.time_in_state = 0
            Trace.trace(8, "time in state %s %s %s"%(time_in_state,self.time_in_state,self.state_change_time))
            if (((time_in_state - self.time_in_state) > self.max_time_in_state) and  
                (self.state in (SETUP, SEEK, MOUNT_WAIT, DISMOUNT_WAIT, DRAINING, ERROR, FINISH_WRITE, ACTIVE))):
                if self.state == ACTIVE:
                    self.too_long_in_state_sent = 0 # set this to avoid sending a false alarm
                    transfer_stuck = 0 
                    t_thread = getattr(self, 'tape_thread', None)
                    n_thread = getattr(self, 'net_thread', None)
                    Trace.trace(8, "bytes read last %s bytes read %s"%(self.bytes_read_last, self.bytes_read))
                    if self.bytes_read_last == self.bytes_read:
                        # Trace.trace(8, "net thread %s tape thread %s"%(n_thread.isAlive(),t_thread.isAlive())) 
                        Trace.log(e_errors.INFO, "net thread %s tape thread %s"%(n_thread.isAlive(),t_thread.isAlive())) # Remove this when problem is fixed !!!!! 
                        # see what thread is active
                        if n_thread.isAlive() and not t_thread.isAlive():
                            # we better do not drop a connection while
                            # tape thread is alive
                            # tape thread can itself detect that
                            # transfer out is slow and error out
                            # this is the case when tape thread read
                            # the data into the buffer and finished
                            # (buffer is bigger than file size)
                            transfer_stuck = 1
                            del(self.too_long_in_state_sent)
                    else:
                        return
                            
                if not hasattr(self,'too_long_in_state_sent'):
                    Trace.alarm(e_errors.WARNING, "Too long in state %s for %s" %
                                (state_name(self.state),self.current_volume))
                    self.too_long_in_state_sent = 0 # send alarm just once
                if self.state == ERROR:
                    ## restart the mover
                    ## this will clean LM active mover list and let
                    ## LM proceed
                    ## potentially this may cause drawing tapes
                    ## and setting them to NOACCESS
                    ## but this is better than holding
                    ## write requests from being executed
                    if self.restart_on_error:
                        Trace.log(e_errors.INFO,"restarting %s"% (self.name,))
                        time.sleep(5)
                        self.restart()
                    
                self.time_in_state = time_in_state
                self.in_state_to_cnt = self.in_state_to_cnt+1
                Trace.trace(8, "in state cnt %s"%(self.in_state_to_cnt,))
                    
                if ((self.in_state_to_cnt >= self.max_in_state_cnt) and
                    (self.state != ERROR) and
                    (self.state != FINISH_WRITE)):
                    if self.state != ACTIVE:
                        # mover is stuck. There is nothing to do as to
                        # offline it
                        msg = "mover is stuck in %s" % (self.return_state(),)
                        Trace.alarm(e_errors.ERROR, msg)
                        #Trace.log(e_errors.ERROR, "marking %s noaccess" % (self.current_volume,))
                        #self.vcc.set_system_noaccess(self.current_volume)
                        #self.set_volume_noaccess(self.current_volume)
                        self.transfer_failed(e_errors.MOVER_STUCK, msg, error_source=TAPE, dismount_allowed=0)
                        return
                        
                    else:
                        if transfer_stuck:
                            msg = "data transfer to or from client stuck. Breaking connection"
                            self.transfer_failed(e_errors.ENCP_STUCK, msg, error_source=NETWORK)
                            return
                        
        ticket = self.format_lm_ticket(state=state, error_source=error_source)
        # send offline less often
        send_rq = 1
        if ((self.state == self._last_state) and
            self.state == OFFLINE):
            send_rq = 0
            if self.send_update_cnt > 0:
               self.send_update_cnt = self.send_update_cnt - 1 
            if self.send_update_cnt == 0:
               send_rq = 1
               self.send_update_cnt = 10
        
        if send_rq:
            for lib, addr in self.libraries:
                if state != self._last_state:
                    Trace.trace(10, "update_lm: %s to %s" % (ticket, addr))
                self._last_state = self.state
                # only main thread is allowed to send messages to LM
                # exception is a mover_busy and mover_error works
                if ((thread_name is 'MainThread') and
                    (ticket['work'] is not 'mover_busy')
                    and (ticket['work'] is not 'mover_error')):
                    Trace.trace(20,"update_lm: send with wait %s"%(ticket['work'],))
                    ## XXX Sasha - this is an experiment - not sure this is a good idea!
                    try:
                        request_from_lm = self.udpc.send(ticket, addr)
                    except:
                        exc, msg, tb = sys.exc_info()
                        if exc == errno.errorcode[errno.ETIMEDOUT]:
                            x = {'status' : (e_errors.TIMEDOUT, msg)}
                        else:
                            x = {'status' : (str(exc), str(msg))}
                        Trace.trace(10, "update_lm: got %s" %(x,))
                        continue
                    work = request_from_lm.get('work')
                    if not work or work=='nowork':
                        continue
                    method = getattr(self, work, None)
                    if method:
                        try:
                            method(request_from_lm)
                        except:
                            exc, detail, tb = sys.exc_info()
                            Trace.handle_error(exc, detail, tb)
                            Trace.log(e_errors.ERROR,"update_lm: tried %s %s and failed"%
                                      (method,request_from_lm)) 
                # if work is mover_busy of mover_error
                # send no_wait message
                if (ticket['work'] is 'mover_busy') or (ticket['work'] is 'mover_error'):
                    Trace.trace(20,"update_lm: send with no wait %s"%(ticket['work'],))
                    self.udpc.send_no_wait(ticket, addr)
        self.check_dismount_timer()


    def need_update(self):
        if self.need_lm_update[0]:
            Trace.trace(20," need_update calling update_lm") 
            self.update_lm(state = self.need_lm_update[1],
                           reset_timer=self.need_lm_update[2],
                           error_source=self.need_lm_update[3])
            
    def _do_delayed_update_lm(self):
        for x in xrange(3):
            time.sleep(1)
            self.update_lm()
        
    def delayed_update_lm(self):
        self.run_in_thread('delayed_update_thread', self._do_delayed_update_lm)
        
    def check_dismount_timer(self):
        self.lock_state()
        ## See if the delayed dismount timer has expired
        now = time.time()
        if self.state is HAVE_BOUND and self.dismount_time and now>self.dismount_time:
            self.state = DISMOUNT_WAIT
            self.unlock_state()
            Trace.trace(10,"Dismount time expired %s"% (self.current_volume,))
            self.run_in_thread('media_thread', self.dismount_volume, after_function=self.idle)
        else:
            self.unlock_state()

    # send a single error message to LM - requestor
    # can be done from any thread
    def send_error_msg(self,error_info=(None,None),error_source=None,returned_work=None):
        if self.lm_address: # send error message only to LM that called us
            ticket = self.format_lm_ticket(state=ERROR,
                                           error_info = error_info,
                                           error_source=error_source,
                                           returned_work=returned_work)
            self.udpc.send_no_wait(ticket, self.lm_address)
                    
    def idle(self):
        if self.state == ERROR:
            return
        if not self.do_eject:
            return
        self.state = IDLE
        self.mode = None
        self.vol_info = {}
        self.file_info = {}
        self.current_volume = None
        if hasattr(self,'too_long_in_state_sent'):
            del(self.too_long_in_state_sent)

        thread = threading.currentThread()
        if thread:
            thread_name = thread.getName()
        else:
            thread_name = None
        # if running in the main thread update lm
        if thread_name is 'MainThread':
            self.update_lm() 
        else: # else just set the update flag
            self.need_lm_update = (1, None, 0, None)

    def offline(self):
        self.state = OFFLINE
        Trace.log(e_errors.INFO, "mover is set OFFLINE")
        thread = threading.currentThread()
        if thread:
            thread_name = thread.getName()
        else:
            thread_name = None
        # if running in the main thread update lm
        if thread_name is 'MainThread':
            self.update_lm() 
        else: # else just set the update flag
            self.need_lm_update = (1, None, 0, None)


    def reset(self, sanity_cookie, client_crc_on):
        self.current_work_ticket = None
        self.init_data_buffer()
        self.buffer.reset(sanity_cookie, client_crc_on)
        self.bytes_read = 0L
        self.bytes_written = 0L

    def return_work_to_lm(self,ticket):
        Trace.trace(21, "return_work_to_lm %s"%(ticket,))
        try:
            lm_address = ticket['lm']['address']
        except KeyError, msg:
            Trace.trace(21, "return_work_to_lm failed %s"%(msg,))
            self.malformed_ticket(ticket, "[lm][address]")
            return

        ticket = self.format_lm_ticket(state=ERROR,
                                       error_info=(e_errors.MOVER_BUSY, state_name(self.state)),
                                       returned_work=ticket)
        self.udpc.send_no_wait(ticket, lm_address)

        
    # read data from the network
    def read_client(self):
        Trace.trace(8, "read_client starting,  bytes_to_read=%s" % (self.bytes_to_read,))
        driver = self.net_driver
        if self.bytes_read == 0 and self.header: #splice in cpio headers, as if they came from client
            nbytes = self.buffer.header_size
            ##XXX this will fail if nbytes>block_size.
            bytes_read = self.buffer.stream_read(nbytes,self.header)

        #Initialize thresholded transfer notify messages.
        bytes_notified = 0L
        Trace.notify("transfer %s %s %s network %s %.3f" %
                     (self.shortname, self.bytes_read,
                      self.bytes_to_read, self.buffer.nbytes(), time.time()))
        
        while self.state in (ACTIVE, DRAINING) and self.bytes_read < self.bytes_to_read:
            if self.tr_failed:
                break
            self.bytes_read_last = self.bytes_read    
            if self.buffer.full():
                Trace.trace(9, "read_client: buffer full %s/%s, read %s/%s" %
                            (self.buffer.nbytes(), self.buffer.max_bytes,
                             self.bytes_read, self.bytes_to_read))
                self.buffer.read_ok.clear()
                self.buffer.read_ok.wait(1)
                continue

            nbytes = min(self.bytes_to_read - self.bytes_read, self.buffer.blocksize)
            bytes_read = 0
            try:
                bytes_read = self.buffer.stream_read(nbytes, driver)
            except:
                exc, detail, tb = sys.exc_info()
                #Trace.handle_error(exc, detail, tb)
                self.transfer_failed(e_errors.ENCP_GONE, detail, error_source=NETWORK)
                return
            Trace.trace(34, "read_client: bytes read %s"%(bytes_read,))
            if bytes_read <= 0:  #  The client went away!
                Trace.log(e_errors.ERROR, "read_client: dropped connection")
                #if self.state is not DRAINING: self.state = HAVE_BOUND
                # if state is DRAINING transfer_failed will set it to OFFLINE
                self.transfer_failed(e_errors.ENCP_GONE, error_source=NETWORK)
                return
            self.bytes_read = self.bytes_read + bytes_read

            if not self.buffer.low():
                self.buffer.write_ok.set()

            #If it is time to do so, send the notify message.
            if is_threshold_passed(self.bytes_read, bytes_notified,
                                   self.bytes_to_read):
                bytes_notified = self.bytes_read
                Trace.notify("transfer %s %s %s network %s %.3f" %
                             (self.shortname, self.bytes_read,
                              self.bytes_to_read, self.buffer.nbytes(),
                              time.time()))
                
        if self.tr_failed:
            return
        if self.bytes_read == self.bytes_to_read:
            if self.trailer:
                trailer_driver = string_driver.StringDriver(self.trailer)
                trailer_bytes_read = 0
                while trailer_bytes_read < self.buffer.trailer_size:
                    if self.tr_failed:
                        break
                    bytes_to_read = self.buffer.trailer_size - trailer_bytes_read
                    bytes_read = self.buffer.stream_read(bytes_to_read, trailer_driver)
                    trailer_bytes_read = trailer_bytes_read + bytes_read
                    Trace.trace(8, "read %s bytes of trailer" % (trailer_bytes_read,))
            if self.tr_failed:
                return
            self.buffer.eof_read() #pushes last partial block onto the fifo
            self.buffer.write_ok.set()
        self.bytes_read_last = self.bytes_read
        Trace.trace(8, "read_client exiting, read %s/%s bytes" %(self.bytes_read, self.bytes_to_read))
                        
    def write_tape(self):
        Trace.log(e_errors.INFO, "write_tape starting, bytes_to_write=%s" % (self.bytes_to_write,))
        Trace.trace(8, "bytes_to_transfer=%s" % (self.bytes_to_transfer,))
        driver = self.tape_driver
        count = 0
        defer_write = 1
        failed = 0
        self.media_transfer_time = 0.
        buffer_empty_t = time.time()   #time when buffer empty has been detected
        buffer_empty_cnt = 0 # number of times buffer was cosequtively empty
        nblocks = 0L
        # send a trigger message to the client
        bytes_written = self.net_driver.write(self.header_labels, # write anything
                                              0,
                                              1) # just 1 byte
        if self.header_labels:
            t1 = time.time()
            try:
                bytes_written = driver.write(self.header_labels, 0, len(self.header_labels))
            except:
                exc, detail, tb = sys.exc_info()
                #Trace.handle_error(exc, detail, tb)
                # bail out gracefuly

                # set volume to readlonly
                Trace.alarm(e_errors.ERROR, "Write error on %s. Volume is set readonly" %
                            (self.current_volume,))
                self.vcc.set_system_readonly(self.current_volume)
                # trick ftt_close, so that it does not attempt to write FM
                if self.driver_type == 'FTTDriver':
                    import ftt
                    ftt._ftt.ftt_set_last_operation(self.tape_driver.ftt.d, 0)
                #initiate cleaning
                self.force_clean = 1
                self.transfer_failed(e_errors.WRITE_ERROR, detail, error_source=TAPE)
                return
            if bytes_written != len(self.header_labels):
                self.transfer_failed(e_errors.WRITE_ERROR, "short write %s != %s" %
                                     (bytes_written, len(self.header_labels)), error_source=TAPE)
                return
            try:
                self.tape_driver.writefm()
            except:
                exc, detail, tb = sys.exc_info()
                self.transfer_failed(e_errors.WRITE_ERROR, detail, error_source=TAPE)
                return
            self.media_transfer_time = self.media_transfer_time + (time.time()-t1)

        #Initialize thresholded transfer notify messages.
        bytes_notified = 0L
        Trace.notify("transfer %s %s %s media %s %.3f" %
                     (self.shortname, self.bytes_written,
                      self.bytes_to_write, self.buffer.nbytes(), time.time()))

        while self.state in (ACTIVE, DRAINING) and self.bytes_written<self.bytes_to_write:
            Trace.trace(33,"total_bytes %s total_bytes_written %s"%(self.bytes_to_write, self.bytes_written))
            if self.tr_failed:
                Trace.trace(27,"write_tape: tr_failed %s"%(self.tr_failed,))
                break
            self.bytes_written_last = self.bytes_written
            empty = self.buffer.empty()
            # buffer is empty no data to write to tape
            if empty and buffer_empty_t == 0:
                # first time
                buffer_empty_t = time.time() 
                
            if (empty or
                (defer_write and (self.bytes_read < self.bytes_to_read and self.buffer.low()))):
                if empty:
                    defer_write = 1
                Trace.trace(9,"write_tape: buffer low %s/%s, wrote %s/%s, defer=%s empty=%s"%
                            (self.buffer.nbytes(), self.buffer.min_bytes,
                             self.bytes_written, self.bytes_to_write,
                             defer_write, empty))
                self.buffer.write_ok.clear()
                self.buffer.write_ok.wait(1)
                now = time.time()
                if int(now - buffer_empty_t) > self.max_time_in_state:
                    if not hasattr(self,'too_long_in_state_sent'):
                        Trace.alarm(e_errors.WARNING, "Too long in state %s for %s" %
                                    (state_name(self.state),self.current_volume))
                        Trace.trace(9, "now %s t %s max %s"%(now, buffer_empty_t,self.max_time_in_state))
                        self.too_long_in_state_sent = 0 # send alarm just once
                    buffer_empty_t = now
                    Trace.trace(9, "buf empty cnt %s max %s"%(buffer_empty_cnt, self.max_in_state_cnt))
                    if buffer_empty_cnt >= self.max_in_state_cnt:
                        msg = "data transfer from client stuck. Breaking connection"
                        self.transfer_failed(e_errors.ENCP_STUCK, msg, error_source=NETWORK)
                        return
                    buffer_empty_cnt = buffer_empty_cnt + 1
                
                if (defer_write and (self.bytes_read==self.bytes_to_read or not self.buffer.low())):
                    defer_write = 0
                continue
            else:
               buffer_empty_t = 0
               buffer_empty_cnt = 0

            count = (count + 1) % 20
            if count == 0:
                ##Dynamic setting of low-water mark
                if self.bytes_read >= self.buffer.min_bytes:
                    netrate, junk = self.net_driver.rates()
                    taperate = self.max_rate
                    if taperate > 0:
                        ratio = netrate/(taperate*1.0)
                        optimal_buf = self.bytes_to_transfer * (1-ratio)
                        optimal_buf = min(optimal_buf, 0.5 * self.max_buffer)
                        optimal_buf = max(optimal_buf, self.min_buffer)
                        optimal_buf = int(optimal_buf)
                        Trace.trace(12,"netrate = %.3g, taperate=%.3g" % (netrate, taperate))
                        if self.buffer.min_bytes != optimal_buf:
                            Trace.trace(12,"Changing buffer size from %s to %s"%
                                        (self.buffer.min_bytes, optimal_buf))
                            self.buffer.set_min_bytes(optimal_buf)

            nbytes = min(self.bytes_to_write - self.bytes_written, self.buffer.blocksize)

            bytes_written = 0
            t1 = time.time()
            try:
                bytes_written = self.buffer.block_write(nbytes, driver)
                nblocks = nblocks+1
                Trace.trace(33,"bytes_to_write %s bytes_written %s"%(nbytes,bytes_written))
            except:
                exc, detail, tb = sys.exc_info()
                #Trace.handle_error(exc, detail, tb)
                # bail out gracefuly

                # set volume to readlonly
                Trace.alarm(e_errors.ERROR, "Write error on %s. Volume is set readonly" %
                            (self.current_volume,))
                self.vcc.set_system_readonly(self.current_volume)
                # trick ftt_close, so that it does not attempt to write FM
                if self.driver_type == 'FTTDriver':
                    import ftt
                    ftt._ftt.ftt_set_last_operation(self.tape_driver.ftt.d, 0)
                #initiate cleaning
                self.force_clean = 1
                self.transfer_failed(e_errors.WRITE_ERROR, detail, error_source=TAPE)
                failed = 1
                break
            self.media_transfer_time = self.media_transfer_time + (time.time()-t1)
            if bytes_written != nbytes:
                self.transfer_failed(e_errors.WRITE_ERROR, "short write %s != %s" %
                                     (bytes_written, nbytes), error_source=TAPE)
                failed = 1
                break
            self.bytes_written = self.bytes_written + bytes_written

            #If it is time to do so, send the notify message.
            if is_threshold_passed(self.bytes_written, bytes_notified,
                                   self.bytes_to_write):
                bytes_notified = self.bytes_written
                Trace.notify("transfer %s %s %s media %s %.3f" %
                             (self.shortname, self.bytes_written,
                              self.bytes_to_write, self.buffer.nbytes(),
                              time.time()))

            if not self.buffer.full():
                self.buffer.read_ok.set()
        if self.tr_failed:
            Trace.trace(27,"write_tape: tr_failed %s"%(self.tr_failed,))
            return

        Trace.log(e_errors.INFO, "written bytes %s/%s, blocks %s header %s trailer %s" %( self.bytes_written, self.bytes_to_write, nblocks, len(self.header), len(self.trailer)))

        if failed: return
        if self.bytes_written == self.bytes_to_write:
            try:
                if self.single_filemark:
                    self.tape_driver.writefm()
                else:
                    self.tape_driver.writefm()
                    self.tape_driver.writefm()
                    self.tape_driver.skipfm(-1)
                ##We don't ever want to let ftt handle the filemarks for us, because its
                ##default behavior is to write 2 filemarks and backspace over both
                ##of them.
                Trace.trace(10, "complete CRC %s"%(self.buffer.complete_crc,))
                self.eof_labels = self.wrapper.eof_labels(self.buffer.complete_crc)
                if self.eof_labels:
                    bytes_written = driver.write(self.eof_labels, 0, len(self.eof_labels))
                    if bytes_written != len(self.eof_labels):
                        self.transfer_failed(e_errors.WRITE_ERROR, "short write %s != %s" %
                                             (bytes_written, len(self.eof_labels)), error_source=TAPE)
                        return
                    self.tape_driver.writefm()
                self.tape_driver.flush()
            except:
                exc, detail, tb = sys.exc_info()
                self.transfer_failed(e_errors.WRITE_ERROR, detail, error_source=TAPE)
                return
            
            if self.check_written_file() and self.driver_type == 'FTTDriver':
                Trace.log(e_errors.INFO, "selective CRC check after writing file")
                Trace.trace(22, "position media")
                Trace.log(e_errors.INFO, "compression %s"%(self.compression,))
                have_tape = self.tape_driver.open(self.device, self.mode, retry_count=30)
                self.tape_driver.set_mode(compression = self.compression, blocksize = 0)
                save_location = self.tape_driver.tell()
                Trace.trace(22,"save location %s" % (save_location,))
                if have_tape != 1:
                    Trace.alarm(e_errors.ERROR, "error positioning tape for selective CRC check")

                    self.transfer_failed(e_errors.WRITE_ERROR, "error positioning tape for selective CRC check", error_source=TAPE)
                    return
                try:
                    location = self.vol_info['eod_cookie']
                    if self.header_labels:
                        location = location+1
                    self.tape_driver.seek(cookie_to_long(location), 0) #XXX is eot_ok needed?
                except:
                    exc, detail, tb = sys.exc_info()
                    Trace.alarm(e_errors.ERROR, "error positioning tape for selective CRC check")
                    self.transfer_failed(e_errors.POSITIONING_ERROR, 'positioning error %s' % (detail,), error_source=TAPE)
                    return
                self.buffer.save_settings()
                bytes_read = 0L
                Trace.trace(20,"write_tape: header size %s" % (self.buffer.header_size,))
                #bytes_to_read = self.bytes_to_transfer + self.buffer.header_size
                bytes_to_read = self.bytes_to_transfer
                header_size = self.buffer.header_size
                # setup buffer for reads
                saved_wrapper = self.buffer.wrapper
                saved_sanity_bytes = self.buffer.sanity_bytes
                saved_complete_crc = self.buffer.complete_crc
                self.buffer.reset((self.buffer.sanity_bytes, self.buffer.sanity_crc), client_crc_on=1)
                self.buffer.set_wrapper(saved_wrapper)
                Trace.trace(22, "starting check after write, bytes_to_read=%s" % (bytes_to_read,))
                driver = self.tape_driver
                first_block = 1
                while bytes_read < bytes_to_read:

                    nbytes = min(bytes_to_read - bytes_read, self.buffer.blocksize)
                    self.buffer.bytes_for_crc = nbytes
                    if bytes_read == 0 and nbytes<self.buffer.blocksize: #first read, try to read a whole block
                        nbytes = self.buffer.blocksize
                    try:
                        b_read = self.buffer.block_read(nbytes, driver)

                        # clean buffer
                        #Trace.trace(22,"write_tape: clean buffer")
                        self.buffer._writing_block = self.buffer.pull()
                        if self.buffer._writing_block:
                            #Trace.trace(22,"write_tape: freeing block")
                            self.buffer._freespace(self.buffer._writing_block)
                        
                    except "CRC_ERROR":
                        exc, detail, tb = sys.exc_info()
                        #Trace.handle_error(exc, detail, tb)
                        Trace.alarm(e_errors.ERROR, "selective CRC check error",
                        {'outfile':self.current_work_ticket['outfile'],
                         'infile':self.current_work_ticket['infile'],
                         'location_cookie':self.current_work_ticket['fc']['location_cookie'],
                         'external_label':self.current_work_ticket['vc']['external_label']})
                        self.transfer_failed(e_errors.WRITE_ERROR, detail, error_source=TAPE)
                        failed = 1
                        break
                    except:
                        exc, detail, tb = sys.exc_info()
                        #Trace.handle_error(exc, detail, tb)
                        Trace.alarm(e_errors.ERROR, "selective CRC check error")
                        self.transfer_failed(e_errors.WRITE_ERROR, detail, error_source=TAPE)
                        failed = 1
                        break
                    if b_read <= 0:
                        Trace.alarm(e_errors.ERROR, "selective CRC check read error")
                        self.transfer_failed(e_errors.WRITE_ERROR, "read returns %s" % (bytes_read,),
                                             error_source=TAPE)
                        failed = 1
                        break
                    if first_block:
                        bytes_to_read = bytes_to_read + header_size
                        first_block = 0
                    bytes_read = bytes_read + b_read
                    if bytes_read > bytes_to_read: #this is OK, we read a cpio trailer or something
                        bytes_read = bytes_to_read

                Trace.trace(22,"write_tape: read CRC %s write CRC %s"%
                            (self.buffer.complete_crc, saved_complete_crc))
                if failed:
                    return
                if self.buffer.complete_crc != saved_complete_crc:
                    Trace.alarm(e_errors.ERROR, "selective CRC check error")
                    self.transfer_failed(e_errors.WRITE_ERROR, "selective CRC check error",error_source=TAPE)
                    return
                Trace.log(e_errors.INFO, "selective CRC check after writing file completed successfuly")
                self.buffer.restore_settings()
                # position to eod"
                self.tape_driver.seek(save_location, 0) #XXX is eot_ok
            if self.update_after_writing():
                self.files_written_cnt = self.files_written_cnt + 1
                self.bytes_written_last = self.bytes_written
                self.transfer_completed()
            else:
                self.transfer_failed(e_errors.EPROTO)

    # read data from the tape
    def read_tape(self):
        Trace.log(e_errors.INFO, "read_tape starting, bytes_to_read=%s" % (self.bytes_to_read,))
        if self.buffer.client_crc_on:
            # calculate checksum when reading from
            # tape (see comment in setup_transfer)
            do_crc = 1
        else:
            do_crc = 0
        driver = self.tape_driver
        failed = 0
        self.media_transfer_time = 0.
        buffer_full_t = 0   #time when buffer full has been detected
        buffer_full_cnt = 0 # number of times buffer was cosequtively full
        nblocks = 0
        #Initialize thresholded transfer notify messages.
        bytes_notified = 0L
        Trace.notify("transfer %s %s %s media %s %.3f" %
                     (self.shortname, -self.bytes_read,
                      self.bytes_to_read, self.buffer.nbytes(), time.time()))

            
        while self.state in (ACTIVE, DRAINING) and self.bytes_read < self.bytes_to_read:
            Trace.trace(33,"total_bytes_to_read %s total_bytes_read %s"%(self.bytes_to_read, self.bytes_read))
            Trace.trace(27,"read_tape: tr_failed %s"%(self.tr_failed,))
            if self.tr_failed:
                break

            self.bytes_read_last = self.bytes_read
            if self.buffer.full():
                # buffer is full no place to read data in
                if buffer_full_t == 0:
                    # first time
                    buffer_full_t = time.time()
                Trace.trace(9, "read_tape: buffer full %s/%s, read %s/%s" %
                            (self.buffer.nbytes(), self.buffer.max_bytes,
                             self.bytes_read, self.bytes_to_read))
                self.buffer.read_ok.clear()
                self.buffer.read_ok.wait(1)
                now = time.time()
                if int(now - buffer_full_t) > self.max_time_in_state:
                    if not hasattr(self,'too_long_in_state_sent'):
                        Trace.alarm(e_errors.WARNING, "Too long in state %s for %s" %
                                    (state_name(self.state),self.current_volume))
                        self.too_long_in_state_sent = 0 # send alarm just once
                    buffer_full_t = now
                    if buffer_full_cnt >= self.max_in_state_cnt:
                        msg = "data transfer to client stuck. Breaking connection"
                        self.transfer_failed(e_errors.ENCP_STUCK, msg, error_source=NETWORK)
                        return
                    buffer_full_cnt = buffer_full_cnt + 1
                continue
            else:
                buffer_full_t = 0
                buffer_full_cnt = 0
            
            nbytes = min(self.bytes_to_read - self.bytes_read, self.buffer.blocksize)
            self.buffer.bytes_for_crc = nbytes
            if self.bytes_read == 0 and nbytes<self.buffer.blocksize: #first read, try to read a whole block
                nbytes = self.buffer.blocksize

            bytes_read = 0
            try:
                t1 = time.time()
                Trace.trace(33,"bytes to read %s"%(nbytes,))
                bytes_read = self.buffer.block_read(nbytes, driver)
                Trace.trace(33,"bytes read %s"%(bytes_read,))
                nblocks = nblocks + 1
                self.media_transfer_time = self.media_transfer_time + (time.time()-t1)
            except "CRC_ERROR":
                Trace.alarm(e_errors.ERROR, "CRC error reading tape",
                            {'outfile':self.current_work_ticket['outfile'],
                             'infile':self.current_work_ticket['infile'],
                             'location_cookie':self.current_work_ticket['fc']['location_cookie'],
                             'external_label':self.current_work_ticket['vc']['external_label']})
                self.transfer_failed(e_errors.CRC_ERROR, error_source=TAPE)
                failed = 1
                break
            except:
                exc, detail, tb = sys.exc_info()
                Trace.trace(33,"Exception %s %s"%(str(exc),str(detail)))
                #Trace.handle_error(exc, detail, tb)
                self.transfer_failed(e_errors.READ_ERROR, detail, error_source=TAPE)
                failed = 1
                break
            if bytes_read <= 0:
                self.transfer_failed(e_errors.READ_ERROR, "read returns %s" % (bytes_read,),
                                     error_source=TAPE)
                failed = 1
                break
            if self.bytes_read==0: #Handle variable-sized cpio header
                if len(self.buffer._buf) != 1:
                    Trace.log(e_errors.ERROR,
                              "read_tape: error skipping over cpio header, len(buf)=%s"%(len(self.buffer._buf)))
                b0 = self.buffer._buf[0]
                if len(b0) >= self.wrapper.min_header_size:
                    try:
                        header_size = self.wrapper.header_size(b0)
                    except (TypeError, ValueError), msg:
                        Trace.log(e_errors.ERROR,"Invalid header %s" %(b0[:self.wrapper.min_header_size]))
                        self.transfer_failed(e_errors.READ_ERROR, "Invalid file header", error_source=TAPE)
                        ##XXX NB: the client won't necessarily see this message since it's still trying
                        ## to recieve data on the data socket
                        failed = 1
                        break
                    self.buffer.header_size = header_size
                    self.bytes_to_read = self.bytes_to_read + header_size
            self.bytes_read = self.bytes_read + bytes_read
            if self.bytes_read > self.bytes_to_read: #this is OK, we read a cpio trailer or something
                self.bytes_read = self.bytes_to_read

            #If it is time to do so, send the notify message.
            if is_threshold_passed(self.bytes_read, bytes_notified,
                                   self.bytes_to_read):
                bytes_notified = self.bytes_read
                Trace.notify("transfer %s %s %s media %s %.3f" %
                             (self.shortname, -self.bytes_read,
                              self.bytes_to_read, self.buffer.nbytes(),
                              time.time()))

            if not self.buffer.empty():
                self.buffer.write_ok.set()


        Trace.log(e_errors.INFO, "read bytes %s/%s, blocks %s header %s" %(self.bytes_read, self.bytes_to_read, nblocks, header_size))
        
        if self.tr_failed:
            Trace.trace(27,"read_tape: tr_failed %s"%(self.tr_failed,))
            return
        if failed: return
        if do_crc:
            if self.tr_failed: return # do not calculate CRC if net thead detected a failed transfer
            Trace.trace(22,"read_tape: calculated CRC %s File DB CRC %s"%
                        (self.buffer.complete_crc, self.file_info['complete_crc']))
            if self.buffer.complete_crc != self.file_info['complete_crc']:
                Trace.log(e_errors.ERROR,"read_tape: calculated CRC %s File DB CRC %s"%
                          (self.buffer.complete_crc, self.file_info['complete_crc']))
                # this is to fix file db
                if self.file_info['complete_crc'] == None:
                    sanity_cookie = (self.buffer.sanity_bytes,self.buffer.sanity_crc)
                    Trace.log(e_errors.WARNING, "found complete CRC set to None in file DB for %s. Changing cookie to %s and CRC to %s" %
                              (self.file_info['bfid'],sanity_cookie, self.buffer.complete_crc))
                    self.fcc.set_crcs(self.file_info['bfid'], sanity_cookie, self.buffer.complete_crc)
                else:
                    if self.tr_failed: return  # do not raise alarm if net thead detected a failed transfer
                    Trace.alarm(e_errors.ERROR, "read_tape CRC error",
                                {'outfile':self.current_work_ticket['outfile'],
                                 'infile':self.current_work_ticket['infile'],
                                 'location_cookie':self.current_work_ticket['fc']['location_cookie'],
                                 'external_label':self.current_work_ticket['vc']['external_label']})
                    self.transfer_failed(e_errors.CRC_ERROR, error_source=TAPE)
                return
        self.bytes_read_last = self.bytes_read
        # if data is tranferred slowly
        # the false "too long in state.." may be generated
        # to aviod this just make a trick with time_in_state 
        self.time_in_state = time.time()
        Trace.trace(8, "read_tape exiting, read %s/%s bytes" %
                    (self.bytes_read, self.bytes_to_read))
                
    # write data out to the network
    def write_client(self):
        Trace.trace(8, "write_client starting, bytes_to_write=%s" % (self.bytes_to_write,))
        if not self.buffer.client_crc_on:
            # calculate checksum when writing to
            # the network (see comment in setup_transfer)
            # CRC when sending to the network if client does not CRC
            do_crc = 1
        else:
            do_crc = 0
        driver = self.net_driver
        #be careful about 0-length files
        if self.bytes_to_write > 0 and self.bytes_written == 0 and self.wrapper: #Skip over cpio or other headers
            while self.buffer.header_size is None and self.state in (ACTIVE, DRAINING):
                Trace.trace(8, "write_client: waiting for read_tape to set header info")
                self.buffer.write_ok.clear()
                self.buffer.write_ok.wait(1)
            # writing to "None" will discard headers, leaving stream positioned at
            # start of data
            self.buffer.stream_write(self.buffer.header_size, None)
            Trace.trace(8, "write_client: discarded %s bytes of header info"%(self.buffer.header_size))
        failed = 0

        #Initialize thresholded transfer notify messages.
        bytes_notified = 0L
        Trace.notify("transfer %s %s %s network %s %.3f" %
                     (self.shortname, -self.bytes_written,
                      self.bytes_to_write, self.buffer.nbytes(),
                      time.time()))

        while self.state in (ACTIVE, DRAINING) and self.bytes_written < self.bytes_to_write:
            if self.tr_failed:
                break
            self.bytes_written_last = self.bytes_written
            if self.buffer.empty():
                # there is no data to transfer to the client
                Trace.trace(9, "write_client: buffer empty, wrote %s/%s" %
                            (self.bytes_written, self.bytes_to_write))
                self.buffer.write_ok.clear()
                self.buffer.write_ok.wait(1)
                continue
                
            nbytes = min(self.bytes_to_write - self.bytes_written, self.buffer.blocksize)
            bytes_written = 0
            try:
                bytes_written = self.buffer.stream_write(nbytes, driver)
            except "CRC_ERROR":
                Trace.alarm(e_errors.ERROR, "CRC error in write client",
                            {'outfile':self.current_work_ticket['outfile'],
                             'infile':self.current_work_ticket['infile'],
                             'location_cookie':self.current_work_ticket['fc']['location_cookie'],
                             'external_label':self.current_work_ticket['vc']['external_label']})
                self.transfer_failed(e_errors.CRC_ERROR, error_source=TAPE)
                failed = 1
                break
            except:
                exc, detail, tb = sys.exc_info()
                #Trace.handle_error(exc, detail, tb)
                #if self.state is not DRAINING: self.state = HAVE_BOUND
                # if state is DRAINING transfer_failed will set it to OFFLINE
                self.transfer_failed(e_errors.ENCP_GONE, detail)
                failed = 1
                break
            if bytes_written < 0:
                #if self.state is not DRAINING: self.state = HAVE_BOUND
                # if state is DRAINING transfer_failed will set it to OFFLINE
                self.transfer_failed(e_errors.ENCP_GONE, "write returns %s"%(bytes_written,))
                failed = 1
                break
            if bytes_written != nbytes:
                pass #this is not unexpected, since we send with MSG_DONTWAIT
            self.bytes_written = self.bytes_written + bytes_written

            if not self.buffer.full():
                self.buffer.read_ok.set()

            #If it is time to do so, send the notify message.
            if is_threshold_passed(self.bytes_written, bytes_notified,
                                   self.bytes_to_write):
                bytes_notified = self.bytes_written
                #negative byte-count to indicate direction
                Trace.notify("transfer %s %s %s network %s %.3f" %
                             (self.shortname, -self.bytes_written,
                              self.bytes_to_write, self.buffer.nbytes(),
                              time.time()))

        if self.tr_failed:
            return
        
        Trace.trace(8, "write_client exiting: wrote %s/%s bytes" % (self.bytes_written, self.bytes_to_write))
        if failed: return
  
        if self.bytes_written == self.bytes_to_write:
            # check crc
            if do_crc:
                Trace.trace(22,"write_client: calculated CRC %s File DB CRC %s"%
                            (self.buffer.complete_crc, self.file_info['complete_crc']))
                if self.buffer.complete_crc != self.file_info['complete_crc']:
                    Trace.alarm(e_errors.ERROR, "CRC error in write client",
                                {'outfile':self.current_work_ticket['outfile'],
                                 'infile':self.current_work_ticket['infile'],
                                 'location_cookie':self.current_work_ticket['fc']['location_cookie'],
                                 'external_label':self.current_work_ticket['vc']['external_label']})
                    self.transfer_failed(e_errors.CRC_ERROR, error_source=TAPE)
                    return
            self.bytes_written_last = self.bytes_written                
            self.transfer_completed()

        
    # the library manager has asked us to write a file to the hsm
    def write_to_hsm(self, ticket):
        Trace.log(e_errors.INFO, "WRITE_TO_HSM")
        self.setup_transfer(ticket, mode=WRITE)

    def update_volume_info(self, ticket):
        Trace.trace(20, "update_volume_info for %s. Current %s"%(ticket['external_label'],
                                                                 self.vol_info))
        if not self.vol_info:
            self.vol_info.update(self.vcc.inquire_vol(ticket['external_label']))
        else:
            if self.vol_info['external_label'] is not ticket['external_label']:
                Trace.log(e_errors.ERROR,"Library manager asked to update iformation for the wrong volume: %s, current %s" % (ticket['external_label'],self.vol_info['external_label']))
            else:
                self.vol_info.update(self.vcc.inquire_vol(ticket['external_label']))
            
            
    # the library manager has asked us to read a file from the hsm
    def read_from_hsm(self, ticket):
        Trace.log(e_errors.INFO,"READ FROM HSM")
        self.setup_transfer(ticket, mode=READ)

    def volume_assert(self, ticket):
        Trace.log(e_errors.INFO,"VOLUME ASSERT")
        self.setup_transfer(ticket, mode=ASSERT)

    def setup_transfer(self, ticket, mode):
        self.lock_state()
        self.save_state = self.state

        self.unique_id = ticket['unique_id']
        try:
            self.lm_address = ticket['lm']['address']
        except KeyError:
            self.lm_address = None
        Trace.trace(10, "setup transfer")
        self.tr_failed = 0
        self.setup_mode = mode
        ## pprint.pprint(ticket)
        if (self.save_state not in (IDLE, HAVE_BOUND) or
            self.setup_mode == ASSERT and self.save_state != IDLE):
            Trace.log(e_errors.ERROR, "Not idle %s" %(state_name(self.state),))
            self.return_work_to_lm(ticket)
            self.unlock_state()
            return 0

        self.state = SETUP
        # the following settings are needed by LM to update it's queues
        self.tmp_vol = ticket['fc'].get('external_label', None)
        self.tmp_vf = ticket['vc'].get('volume_family', None)
        self.need_lm_update = (1, self.state, 1, None)
        self.override_ro_mount = ticket.get('override_ro_mount', None)
        #prevent a delayed dismount from kicking in right now
        if self.dismount_time:
            self.dismount_time = None
        self.unlock_state()
        
        ticket['mover']={}
        ticket['mover'].update(self.config)
        ticket['mover']['device'] = "%s:%s" % (self.config['host'], self.config['device'])

        self.current_work_ticket = ticket
        self.run_in_thread('client_connect_thread', self.connect_client)

    def assert_vol(self):
        ticket = self.current_work_ticket
        self.t0 = time.time()
        self.vcc = volume_clerk_client.VolumeClerkClient(self.csc,
                                                         server_address=ticket['vc']['address'])
        vc = ticket['vc']
        self.vol_info.update(vc)
        self.volume_family=vc['volume_family']
        self.mount_volume(ticket['vc']['external_label'])
        if self.state == ERROR:
            Trace.log(e_errors.ERROR, "ASSERT failed %s" % (self.current_work_ticket['status'],))
            self.current_work_ticket['status'] = (e_errors.MOUNTFAILED, None)
            callback.write_tcp_obj(self.control_socket, ticket)
            self.control_socket.close()
            return
        #At this point the media changer claims the correct volume is loaded;
        have_tape = 0
        for retry_open in range(3):
            Trace.trace(10, "position media")
            have_tape = self.tape_driver.open(self.device, self.mode, retry_count=30)
            if have_tape == 1:
                err = None
                self.tape_driver.set_mode(compression = self.compression, blocksize = 0)
                break
            else:
                try:
                    Trace.log(e_errors.INFO, "rewind/retry")
                    r= self.tape_driver.close()
                    time.sleep(1)
                    ### XXX Yuk!! This is a total hack
                    p=os.popen("mt -f %s rewind 2>&1" % (self.device),'r')
                    r=p.read()
                    s=p.close()
                    ### r=self.tape_driver.rewind()
                    err = r
                    Trace.log(e_errors.INFO, "rewind/retry: mt rewind returns %s, status %s" % (r,s))
                    if s:
                        self.transfer_failed(e_errors.MOUNTFAILED, 'mount failure: %s' % (err,), error_source=ROBOT)
                        self.dismount_volume(after_function=self.idle)
                        return

                except:
                    exc, detail, tb = sys.exc_info()
                    err = detail
                    Trace.log(e_errors.ERROR, "rewind/retry: %s %s" % ( exc, detail))
        else:
            self.transfer_failed(e_errors.MOUNTFAILED, 'mount failure: %s' % (err,), error_source=ROBOT)
            self.dismount_volume(after_function=self.idle)
            return
        eod = self.vol_info['eod_cookie']
        status = self.tape_driver.verify_label(ticket['vc']['external_label'], READ)
        if status[0] != e_errors.OK:
            if status[0] == e_errors.READ_VOL1_READ_ERR and eod == 'none':
                self.dismount_volume(after_function=self.idle)
                return
            self.transfer_failed(status[0], status[1], error_source=TAPE)
            return

        self.dismount_volume(after_function=self.idle)

        
        
    def finish_transfer_setup(self):
        Trace.trace(10, "client connect returned: %s %s" % (self.control_socket, self.client_socket))
        ticket = self.current_work_ticket
        if not self.client_socket:
            Trace.trace(20, "finish_transfer_setup: connection to client failed")
            self.state = self.save_state
            ## Connecting to client failed
            if self.state is HAVE_BOUND:
                self.dismount_time = time.time() + self.default_dismount_delay
            self.need_lm_update = (1, self.state, 1, None)
            #self.update_lm(reset_timer=1)
            return 0

        self.t0 = time.time()

        ##all groveling around in the ticket should be done here
        fc = ticket['fc']
        vc = ticket['vc']
        self.vol_info.update(vc)
        self.file_info.update(fc)
        self.volume_family=vc['volume_family']
        delay = 0
        if ticket['work'] == 'read_from_hsm':
            sanity_cookie = ticket['fc']['sanity_cookie']
        else:
            sanity_cookie = None
        
        if ticket.has_key('client_crc'):
            client_crc_on = ticket['client_crc']
        elif self.config['driver'] == "NullDriver":
            client_crc_on = 0
        else:
            client_crc_on = 1 # assume that client does CRC

        # if client_crc is ON:
        #    write requests -- calculate CRC when writing from memory to tape
        #    read requetsts -- calculate CRC when reading from tape to memory
        # if client_crc is OFF:
        #    write requests -- calculate CRC when writing to memory
        #    read requetsts -- calculate CRC when reading memory

        self.reset(sanity_cookie, client_crc_on)
        # restore self.current_work_ticket after it gets cleaned in the reset()
        self.current_work_ticket = ticket
        if self.current_work_ticket['encp'].has_key('delayed_dismount'):
            if ((type(self.current_work_ticket['encp']['delayed_dismount']) is type(0)) or
                (type(self.current_work_ticket['encp']['delayed_dismount']) is type(0.))):
                delay = 60 * self.current_work_ticket['encp']['delayed_dismount']
            else:
                delay = self.default_dismount_delay
        if delay > 0:
            self.delay = max(delay, self.default_dismount_delay)
        elif delay < 0:
            self.delay = 31536000  # 1 year
        else:
            self.delay = 0   
        self.delay = min(self.delay, self.max_dismount_delay)
        self.fcc = file_clerk_client.FileClient(self.csc, bfid=0,
                                                server_address=fc['address'])
        self.vcc = volume_clerk_client.VolumeClerkClient(self.csc,
                                                         server_address=vc['address'])
        self.unique_id = self.current_work_ticket['unique_id']
        volume_label = fc['external_label']
        if volume_label:
            self.vol_info.update(self.vcc.inquire_vol(volume_label))
            self.current_work_ticket['vc'].update(self.vol_info)
        else:
            Trace.log(e_errors.ERROR, "setup_transfer: volume label=%s" % (volume_label,))
        if self.vol_info['status'][0] != 'ok':
            msg =  ({READ: e_errors.READ_NOTAPE, WRITE: e_errors.WRITE_NOTAPE}.get(
                self.setup_mode, e_errors.EPROTO), self.vol_info['status'][1])
            Trace.log(e_errors.ERROR, "Volume clerk reply %s" % (msg,))
            self.send_client_done(self.current_work_ticket, msg[0], msg[1])
            self.state = self.save_state
            return 0
        
        self.buffer.set_blocksize(self.vol_info['blocksize'])
        self.wrapper = None
        self.wrapper_type = volume_family.extract_wrapper(self.volume_family)

        try:
            self.wrapper = __import__(self.wrapper_type + '_wrapper')
        except:
            exc, msg, tb = sys.exc_info()
            Trace.log(e_errors.ERROR, "error importing wrapper: %s %s" %(exc,msg))

        if not self.wrapper:
            msg = e_errors.EPROTO, "Illegal wrapper type %s" % (self.wrapper_type)
            Trace.log(e_errors.ERROR,  "%s" %(msg,))
            self.send_client_done(self.current_work_ticket, msg[0], msg[1])
            self.state = self.save_state
            return 0
        
        self.buffer.set_wrapper(self.wrapper)
        client_filename = self.current_work_ticket['wrapper'].get('fullname','?')
        pnfs_filename = self.current_work_ticket['wrapper'].get('pnfsFilename', '?')

        self.mode = self.setup_mode
        self.bytes_to_transfer = long(fc['size'])
        self.bytes_to_write = self.bytes_to_transfer
        self.bytes_to_read = self.bytes_to_transfer
        self.expected_transfer_time = self.bytes_to_write*1.0 / self.max_rate
        self.real_transfer_time  = 0.
        self.transfer_deficiency = 1.

        ##NB: encp v2_5 supplies this information for writes but not reads. Somebody fix this!
        try:
            client_hostname = self.current_work_ticket['wrapper']['machine'][1]
        except KeyError:
            client_hostname = ''
        self.client_hostname = client_hostname
        if client_hostname:
            client_filename = client_hostname + ":" + client_filename
        if self.wrapper:
            self.current_work_ticket['mover']['compression'] = self.compression
            self.wrapper_ticket = self.wrapper.create_wrapper_dict(self.current_work_ticket)
        if self.mode == READ:
            self.files = (pnfs_filename, client_filename)
            self.target_location = cookie_to_long(fc['location_cookie'])
            self.buffer.header_size = None
        elif self.mode == WRITE:
            self.files = (client_filename, pnfs_filename)
            if self.wrapper:
                self.header_labels = self.wrapper.hdr_labels(self.wrapper_ticket)
                # note! eof_labels will be called when write is done and checksum is calculated
                self.header, self.trailer = self.wrapper.headers(self.wrapper_ticket)
            else:
                self.header = ''
                self.trailer = ''
            self.buffer.header_size = len(self.header)
            self.buffer.trailer_size = len(self.trailer)
            self.bytes_to_write = self.bytes_to_write + len(self.header) + len(self.trailer)
            self.buffer.file_size = self.bytes_to_write
            self.buffer.trailer_pnt = self.buffer.file_size - len(self.trailer)
            self.target_location = None        

        if volume_label == self.current_volume and self.save_state == HAVE_BOUND: #no mount needed
            self.timer('mount_time')
            self.position_media(verify_label=0)
        else:
            self.run_in_thread('media_thread', self.mount_volume, args=(volume_label,),
                               after_function=self.position_media)
        
    def error(self, msg, err=e_errors.ERROR):
        self.last_error = (str(err), str(msg))
        Trace.log(e_errors.ERROR, "error: %s message: %s state=ERROR"%(err, msg))
        self.state = ERROR

    def broken(self, msg, err=e_errors.ERROR):
        #self.set_sched_down()
        Trace.alarm(e_errors.ERROR, "%s %s"%(err, str(msg)))
        self.error(msg, err)
        
    def position_media(self, verify_label=1):
        #At this point the media changer claims the correct volume is loaded; now position it
        label_tape = 0
        have_tape = 0
        err = None
        for retry_open in range(3):
            Trace.trace(10, "position media")
            have_tape = self.tape_driver.open(self.device, self.mode, retry_count=30)
            if have_tape == 1:
                if self.mode == WRITE and self.tape_driver.mode == READ:
                    Trace.alarm(e_errors.ERROR, "tape %s is write protected, will be set read-only"%
                                (self.current_volume,))
                    self.vcc.set_system_readonly(self.current_volume)
                    self.vcc.set_comment(self.current_volume, "write-protected")
                    self.send_client_done(self.current_work_ticket, e_errors.WRITE_ERROR,
                                          "tape %s is write protected"%(self.current_volume,))
                    self.net_driver.close()
                    self.dismount_volume(after_function=self.idle)
                    return
                    
                err = None
                self.tape_driver.set_mode(compression = self.compression, blocksize = 0)
                break
            else:
                try:
                    Trace.log(e_errors.INFO, "rewind/retry")
                    r= self.tape_driver.close()
                    time.sleep(1)
                    ### XXX Yuk!! This is a total hack
                    p=os.popen("mt -f %s rewind 2>&1" % (self.device),'r')
                    r=p.read()
                    s=p.close()
                    ### r=self.tape_driver.rewind()
                    err = r
                    Trace.log(e_errors.INFO, "rewind/retry: mt rewind returns %s, status %s" % (r,s))
                    if s:
                        self.transfer_failed(e_errors.MOUNTFAILED, 'mount failure: %s' % (err,), error_source=ROBOT, dismount_allowed=0)
                        self.unload_volume(self.vol_info, after_function=self.idle)
                        return

                except:
                    exc, detail, tb = sys.exc_info()
                    err = detail
                    Trace.log(e_errors.ERROR, "rewind/retry: %s %s" % ( exc, detail))
        else:
            self.transfer_failed(e_errors.MOUNTFAILED, 'mount failure: %s' % (err,), error_source=ROBOT, dismount_allowed=0)
            self.unload_volume(self.vol_info, after_function=self.idle)
            return
        self.state = SEEK ##XXX start a timer here?
        eod = self.vol_info['eod_cookie']
        if eod=='none':
            eod = None
        volume_label = self.current_volume

        if self.mode is WRITE and eod is None:
            verify_label = 0
            label_tape = 1
        
        if self.mode is WRITE:
            if self.target_location is None:
                self.target_location = eod
            if self.target_location != eod:
                Trace.log(e_errors.ERROR, "requested write at location %s, eod=%s" %
                          (self.target_location, eod))
                return 0 # Can only write at end of tape

            if label_tape:
                ## new tape, label it
                ##  need to safeguard against relabeling here
                status = self.tape_driver.verify_label(None)
                Trace.trace(10, "verify label returns %s" % (status,))
                if status[0] == e_errors.OK:  #There is a label present!
                        msg = "volume %s already labeled %s" % (volume_label,status[1])
                        #self.vcc.set_system_noaccess(volume_label)
                        self.set_volume_noaccess(volume_label)
                        Trace.alarm(e_errors.ERROR, msg)
                        Trace.log(e_errors.ERROR, "marking %s noaccess" % (volume_label,))
                        self.transfer_failed(e_errors.WRITE_VOL1_WRONG, msg, error_source=TAPE)
                        return 0

                try:
                    Trace.trace(10,"rewind")
                    self.tape_driver.rewind()
                    if self.driver_type == 'FTTDriver':
                        import ftt
                        time.sleep(3)
                        stats = self.tape_driver.ftt.get_stats()
                        Trace.trace(10,"WRITE_PROT=%s"%(stats[ftt.WRITE_PROT],))
                        write_prot = stats[ftt.WRITE_PROT]
                        if type(write_prot) is type(''):
                            write_prot = string.atoi(write_prot)
                        if write_prot:
                            #self.vcc.set_system_noaccess(volume_label)
                            self.set_volume_noaccess(volume_label)
                            Trace.alarm(e_errors.ERROR, "attempt to label write protected tape")
                            self.transfer_failed(e_errors.WRITE_ERROR,
                                                 "attempt to label write protected tape",
                                                 error_source=TAPE)
                            return 0
                    Trace.log(e_errors.INFO, "labeling new tape %s" % (volume_label,))
                    Trace.trace(10, "ticket %s"%(self.current_work_ticket))
                    vol1_label = self.wrapper.vol_labels(volume_label, self.wrapper_ticket)
                    self.tape_driver.write(vol1_label, 0, len(vol1_label))
                    self.tape_driver.writefm()
	            # WAYNE FOO
                    ##if self.config['product_id'] == 'T9940B':
                    ##    Trace.trace(42, "WAYNE DEBUG: rewinding")
                    ##    self.tape_driver.rewind()
                    ##    Trace.trace(42, "WAYNE DEBUG: rewriting label")
                    ##    self.tape_driver.write(vol1_label, 0, 80)
                    ##    self.tape_driver.writefm()
	            # END WAYNE FOO
                
                except e_errors.WRITE_ERROR, detail:
                    self.transfer_failed(e_errors.WRITE_ERROR, detail, error_source=TAPE)
                    return 0
                except:
                    exc, detail, tb = sys.exc_info()
                    Trace.handle_error(exc, detail, tb)
                    self.transfer_failed(e_errors.WRITE_ERROR, detail, error_source=TAPE)
                    return 0
                eod = 1
                self.target_location = eod
                self.vol_info['eod_cookie'] = eod
                if self.driver_type == 'FTTDriver' and self.rem_stats:
                    import ftt
                    stats = self.tape_driver.ftt.get_stats()
                    remaining = stats[ftt.REMAIN_TAPE]
                    if remaining is not None:
                        remaining = long(remaining)
                        self.vol_info['remaining_bytes'] = remaining * 1024L
                        ##XXX keep everything in KB?
                ret = self.vcc.set_remaining_bytes(volume_label,
                                                   self.vol_info['remaining_bytes'],
                                                   self.vol_info['eod_cookie'])
                if ret['status'][0] != e_errors.OK:
                    self.transfer_failed(ret['status'][0], ret['status'][1], error_source=TAPE)
                    return 0
                    

        if verify_label:
            status = self.tape_driver.verify_label(volume_label, self.mode)
            if status[0] != e_errors.OK:
                self.transfer_failed(status[0], status[1], error_source=TAPE)
                return 0
        location = cookie_to_long(self.target_location)
        self.run_in_thread('seek_thread', self.seek_to_location,
                           args = (location, self.mode==WRITE),
                           after_function=self.start_transfer)
        
        return 1
            
    def transfer_failed(self, exc=None, msg=None, error_source=None, dismount_allowed=1):
        self.timer('transfer_time')
        after_dismount_function = None
        volume_label = self.current_volume
        ticket = self.current_work_ticket
        if not ticket.has_key('times'):
            ticket['times']={}
        if self.mode == READ:
            t = self.tape_driver.tape_transfer_time()
        else:
            t = self.media_transfer_time
        if t == 0.:
            t = ticket['times']['transfer_time']
        ticket['times']['drive_transfer_time'] = t
        self.log_state()
        if self.tr_failed:
            return          ## this function has been alredy called in the other thread
        self.tr_failed = 1
        broken = ""
        ftt_eio =0 

        Trace.log(e_errors.ERROR, "transfer failed %s %s %s volume=%s location=%s" % (
            exc, msg, error_source,self.current_volume, self.current_location))
        Trace.notify("disconnect %s %s" % (self.shortname, self.client_ip))
        if type(msg) != type(""):
            msg = str(msg)
        if exc == e_errors.WRITE_ERROR or exc == e_errors.READ_ERROR:
            if msg.find("FTT_EIO") != -1:
                # possibly a scsi error, log low level diagnostics
                # report error but go idle
                self.watch_syslog()
                ftt_eio = 1
        
        ### XXX translate this to an e_errors code?
        self.last_error = str(exc), str(msg)
        
        if self.state == ERROR:
            Trace.log(e_errors.ERROR, "Mover already in ERROR state %s, state=ERROR" % (msg,))
            self.tr_failed = 0
            return

        if exc not in (e_errors.ENCP_GONE, e_errors.ENCP_STUCK, e_errors.READ_VOL1_WRONG, e_errors.WRITE_VOL1_WRONG):
            if msg.find("FTT_EBUSY") != -1:
                # tape thread stuck in D state - offline mover
                after_dismount_function = self.offline
                Trace.alarm(e_errors.ERROR, "tape thread is possibly stuck in D state")
                self.log_state()

            self.consecutive_failures = self.consecutive_failures + 1
            if self.consecutive_failures >= self.max_consecutive_failures:
                broken =  "max_consecutive_failures (%d) reached" %(self.max_consecutive_failures)
            now = time.time()
            self.error_times.append(now)
            while self.error_times and now - self.error_times[0] > self.failure_interval:
                self.error_times.pop(0)
            if len(self.error_times) >= self.max_failures:
                if broken:
                  after_dismount_function = self.offline 
                broken =  "max_failures (%d) per failure_interval (%d) reached" % (self.max_failures,
                                                                                     self.failure_interval)
            ### network errors should not count toward rd_err, wr_err
            if self.mode == WRITE:
                self.vcc.update_counts(self.current_volume, wr_err=1, wr_access=1)
                #Heuristic: if tape is more than 90% full and we get a write error, mark it full
                try:
                    capacity = self.vol_info['capacity_bytes']
                    remaining = self.vol_info['remaining_bytes']
                    eod = self.vol_info['eod_cookie']
                    if remaining <= 0.1 * capacity:
                        Trace.log(e_errors.INFO,
                                  "heuristic: write error on vol %s, remaining=%s, capacity=%s, marking volume full"%
                                  (self.current_volume, remaining, capacity))
                        ret = self.vcc.set_remaining_bytes(self.current_volume, 0, eod, None)
                        if ret['status'][0] != e_errors.OK:
                            Trace.alarm(e_errors.ERROR, "set_remaining_bytes failed", ret)
                            broken = broken +  "set_remaining_bytes failed"
                                
                except:
                    exc, msg, tb = sys.exc_info()
                    Trace.log(e_errors.ERROR, "%s %s" % (exc, msg))
            else:
                self.vcc.update_counts(self.current_volume, rd_err=1, rd_access=1)       

            self.transfers_failed = self.transfers_failed + 1
        encp_gone = exc in (e_errors.ENCP_GONE, e_errors.ENCP_STUCK)
        self.net_driver.close()
        self.send_client_done(self.current_work_ticket, str(exc), str(msg))
        if exc == e_errors.MOVER_STUCK:
            broken = exc

        save_state = self.state
        # get the current thread
        cur_thread = threading.currentThread()
        if cur_thread:
            cur_thread_name = cur_thread.getName()
        else:
            cur_thread_name = None

        # if encp is gone there is no need to dismount a tape
        dism_allowed = not encp_gone
        dism_allowed = dism_allowed & dismount_allowed
        Trace.trace(26,"current thread %s encp_gone %s"%(cur_thread_name, encp_gone))
        if cur_thread_name:
            if cur_thread_name == 'net_thread':
                # check if tape_thread is active before allowing dismount
                Trace.trace(26,"checking thread %s"%('tape_thread',))
                thread = getattr(self, 'tape_thread', None)
                for wait in range(60):
                    if thread and thread.isAlive():
                        Trace.trace(26, "thread %s is already running, waiting %s" % ('tape_thread', wait))
                        time.sleep(1)
                        break
        Trace.trace(26,"dismount_allowed %s after_dismount %s"%(dism_allowed, after_dismount_function))
        if encp_gone:
            self.current_location = self.tape_driver.tell()
            self.dismount_time = time.time() + self.delay
            self.state = HAVE_BOUND
            if self.maybe_clean():
                Trace.trace(26,"cleaned")
                self.state = IDLE
                self.log_state()
                self.tr_failed = 0
                return
                
        self.send_error_msg(error_info = (exc, msg),error_source=error_source)
        if not ftt_eio:
            self.need_lm_update = (1, ERROR, 1, error_source)
        if exc in (e_errors.READ_VOL1_WRONG,
                   e_errors.WRITE_VOL1_WRONG,
                   e_errors.WRITE_VOL1_MISSING,
                   e_errors.READ_VOL1_MISSING,
                   e_errors.READ_VOL1_READ_ERR,
                   e_errors.WRITE_VOL1_READ_ERR):
           self.set_volume_noaccess(volume_label) 
        if dism_allowed:
            if save_state == DRAINING:
                self.dismount_volume()
                self.offline()
            else:
                if not after_dismount_function:
                    if not self.maybe_clean():
                        self.dismount_volume()
                    self.idle()
                else:
                    self.dismount_volume(after_function=after_dismount_function)
            
        if not after_dismount_function and broken:
            self.broken(broken, exc)
            self.tr_failed = 0
            return

        self.tr_failed = 0   
        
    def transfer_completed(self):
        self.consecutive_failures = 0
        self.timer('transfer_time')
        ticket = self.current_work_ticket
        if not ticket.has_key('times'):
            ticket['times']={}
        if self.mode == READ:
            t = self.tape_driver.tape_transfer_time()
        else:
            t = self.media_transfer_time
        if t == 0.:
            t = ticket['times']['transfer_time']
        ticket['times']['drive_transfer_time'] = t
        Trace.log(e_errors.INFO, "transfer complete volume=%s location=%s"%(
            self.current_volume, self.current_location))
        Trace.notify("disconnect %s %s" % (self.shortname, self.client_ip))
        if self.mode == WRITE:
            self.vcc.update_counts(self.current_volume, wr_access=1)
        else:
            self.vcc.update_counts(self.current_volume, rd_access=1)
        self.transfers_completed = self.transfers_completed + 1
        self.net_driver.close()
        self.current_location = self.tape_driver.tell()
        now = time.time()
        self.dismount_time = now + self.delay
        self.send_client_done(self.current_work_ticket, e_errors.OK)
        if hasattr(self,'too_long_in_state_sent'):
            del(self.too_long_in_state_sent)
        
        if self.state == DRAINING or (self.state == FINISH_WRITE and self.draining):
            self.dismount_volume()
            self.offline()
        else:
            self.state = HAVE_BOUND
            if self.maybe_clean():
                self.state = IDLE
        self.log_state()
        self.need_lm_update = (1, None, 1, None)
            
    def maybe_clean(self):
        Trace.log(e_errors.INFO, "maybe_clean")
        if self.force_clean:
             needs_cleaning = 1
             Trace.log(e_errors.INFO, "Force clean is set")
        else:
            needs_cleaning = self.tape_driver.get_cleaning_bit()
        self.force_clean = 0
        did_cleaning = 0
        if needs_cleaning:
            if not self.do_cleaning:
                Trace.log(e_errors.INFO, "cleaning bit set but automatic cleaning disabled")
                return 0
            Trace.log(e_errors.INFO, "initiating automatic cleaning")
            did_cleaning = 1
            save_state = self.state
            if save_state == HAVE_BOUND:
                self.dismount_volume()
                save_state = IDLE
            self.state = CLEANING
            self.mcc.doCleaningCycle(self.config)
            self.state = save_state
            Trace.log(e_errors.INFO, "cleaning complete")
        return did_cleaning
        
    def update_after_writing(self):
        previous_eod = cookie_to_long(self.vol_info['eod_cookie'])
        eod_increment = 0
        if self.header_labels:
           eod_increment = eod_increment + 1
            
        self.current_location = self.tape_driver.tell()
        if self.current_location <= previous_eod:
            Trace.log(e_errors.ERROR, " current location %s <= eod %s" %
                      (self.current_location, previous_eod))
            return 0

        r0 = self.vol_info['remaining_bytes']  #value prior to this write
        r1 = r0 - self.bytes_written           #value derived from simple subtraction
        r2 = r1                                #value reported from drive, if possible
        Trace.trace(24, "estimate remainig %s" % (r2,))
        ## XXX OO: this should be a driver method
        if self.driver_type == 'FTTDriver' and self.rem_stats:
            import ftt
            stats = None
            try:
                stats = self.tape_driver.ftt.get_stats()
                r2 = long(stats[ftt.REMAIN_TAPE]) * 1024L
                Trace.trace(24, "reported remainig %s" % (r2,))
            except ftt.FTTError, detail:
                Trace.log(e_errors.ERROR, "ftt.get_stats: FTT_ERROR %s"%
                          (detail,))
            except:
                exc, detail, tb = sys.exc_info()
                Trace.handle_error(exc, detail, tb)
                try:
                    Trace.log(e_errors.ERROR, "REMAIN_TAPE: type %s value %s"%
                              (type(stats[ftt.REMAIN_TAPE]), stats[ftt.REMAIN_TAPE]))
                except:
                    exc, detail, tb = sys.exc_info()
                    Trace.handle_error(exc, detail, tb)

        capacity = self.vol_info['capacity_bytes']
        if r1 <= 0.1 * capacity:  #do not allow remaining capacity to decrease in the "near-EOT" regime
            remaining = min(r1, r2)
        else:                     #trust what the drive tells us, as long as we are under 90% full
            remaining = r2

        self.vol_info['remaining_bytes']=remaining
        eod = loc_to_cookie(self.current_location)
        self.vol_info['eod_cookie'] = eod
        sanity_cookie = (self.buffer.sanity_bytes,self.buffer.sanity_crc)
        complete_crc = self.buffer.complete_crc
        fc_ticket = {  'location_cookie': loc_to_cookie(previous_eod+eod_increment),
                       'size': self.bytes_to_transfer,
                       'sanity_cookie': sanity_cookie,
                       'external_label': self.current_volume,
                       'complete_crc': complete_crc}
        ##  HACK:  store 0 to database if mover is NULL
        if self.config['driver']=='NullDriver':
            fc_ticket['complete_crc']=0L
            fc_ticket['sanity_cookie']=(self.buffer.sanity_bytes,0L)
        fcc_reply = self.fcc.new_bit_file({'work':"new_bit_file",
                                            'fc'  : fc_ticket
                                            })
        if fcc_reply['status'][0] != e_errors.OK:
            Trace.log(e_errors.ERROR,
                       "cannot assign new bfid")
            self.transfer_failed(e_errors.ERROR,"Cannot assign new bit file ID")
            #XXX exception?
            return 0
        ## HACK: restore crc's before replying to caller
        fc_ticket = fcc_reply['fc']
        fc_ticket['sanity_cookie'] = sanity_cookie
        fc_ticket['complete_crc'] = complete_crc 
        bfid = fc_ticket['bfid']
        self.current_work_ticket['fc'] = fc_ticket
        Trace.log(e_errors.INFO,"set remaining: %s %s %s" %(self.current_volume, remaining, eod))

        self.state = FINISH_WRITE
        finish_writing = 1
        while finish_writing:
            Trace.trace(26,"sending set_remaining_bytes")
            reply = self.vcc.set_remaining_bytes(self.current_volume, remaining, eod, bfid)
            Trace.trace(26,"set_remaining_bytes returned %s"%(reply,))
            if reply['status'][0] != e_errors.OK:
                if reply['status'][0] == e_errors.TIMEDOUT:
                    # keep trying
                    Trace.alarm(e_errors.ERROR,"Volume Clerk timeout on the final stage of file writing to %s"%(self.current_volume))
                else:
                    self.transfer_failed(reply['status'][0], reply['status'][1], error_source=TAPE)
                    finish_writing = 0
                    return 0
            else:
               finish_writing = 0 
        self.vol_info.update(reply)
        return 1

    def malformed_ticket(self, ticket, expected_keys=None):
        msg = "Missing keys "
        if expected_keys is not None:
            msg = "%s %s"(msg, expected_keys)
        msg = "%s %s"%(msg, ticket)
        Trace.log(e_errors.ERROR, msg)

    def send_client_done(self, ticket, status, error_info=None):
        if self.control_socket is None:
            return
        ticket['status'] = (status, error_info)
        Trace.trace(26, "send_client_done: %s"%(ticket))
        try:
            callback.write_tcp_obj(self.control_socket, ticket)
        except:
            exc, detail, tb = sys.exc_info()
            Trace.log(e_errors.ERROR, "error in send_client_done: %s" % (detail,))
        if self.control_socket:
            self.control_socket.close()
        self.control_socket = None
        return

    def del_udp_client(self, udp_client):
        if not udp_client: return
        # tell server we're done - this allows it to delete our unique id in
        # its dictionary - this keeps things cleaner & stops memory from growing
        try:
            pid = udp_client._os.getpid()
            tsd = udp_client.tsd.get(pid)
            if not tsd:
                return
            for server in tsd.send_done.keys() :
                try:
                    tsd.socket.close()
                except:
                    pass
        except:
            pass

    def connect_client(self):
        # run this in a thread
        try:
            ticket = self.current_work_ticket
            data_ip=self.config.get("data_ip",None)
            host, port, listen_socket = callback.get_callback(ip=data_ip)
            listen_socket.listen(1)
            ticket['mover']['callback_addr'] = (host,port) #client expects this

            control_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            flags = fcntl.fcntl(control_socket.fileno(), FCNTL.F_GETFL)
            fcntl.fcntl(control_socket.fileno(), FCNTL.F_SETFL, flags | os.O_NONBLOCK)
            # the following insertion is for antispoofing
            if ticket.has_key('route_selection') and ticket['route_selection']:
                ticket['mover_ip'] = host
                # bind control socket to data ip
                control_socket.bind((host, 0))
                u = udp_client.UDPClient()
                Trace.trace(10, "sending IP %s to %s" % (host, ticket['routing_callback_addr']))
                try:
                    x= u.send(ticket,ticket['routing_callback_addr'] , self.connect_to, self.connect_retry, 0)
                except errno.errorcode[errno.ETIMEDOUT]:
                    Trace.log(e_errors.ERROR, "error sending to %s (%s)" %
                              (ticket['routing_callback_addr'], os.strerror(errno.ETIMEDOUT)))
                    self.del_udp_client(u)
                    #del u
                    # just for a case
                    try:
                        control_socket.close()
                        listen_socket.close()
                    except:
                        pass
                    self.control_socket, self.client_socket = None, None
                    self.run_in_thread('finish_transfer_setup_thread', self.finish_transfer_setup)
                    return
                Trace.trace(10, "encp called back with %s"%(x,))
                if x.has_key('callback_addr'): ticket['callback_addr'] = x['callback_addr']
                self.del_udp_client(u)
                #del u
            Trace.trace(10, "connecting to %s" % (ticket['callback_addr'],))
	    try:
		control_socket.connect(ticket['callback_addr'])
	    except socket.error, detail:
		Trace.log(e_errors.ERROR, "%s %s" %
			  (detail, ticket['callback_addr']))
		#We have seen that on IRIX, when the connection succeds, we
		# get an ISCONN error.
		if hasattr(errno, 'EISCONN') and detail[0] == errno.EISCONN:
		    pass
		#The TCP handshake is in progress.
		elif detail[0] == errno.EINPROGRESS:
		    pass
		else:
		    Trace.log(e_errors.ERROR, "error connecting to %s (%s)" %
			      (ticket['callback_addr'], os.strerror(detail)))
                    # just for a case
                    try:
                        control_socket.close()
                        listen_socket.close()
                    except:
                        pass

		    self.control_socket, self.client_socket = None, None
                    self.run_in_thread('finish_transfer_setup_thread', self.finish_transfer_setup)
                    return
		    
	    #Check if the socket is open for reading and/or writing.
	    r, w, ex = select.select([control_socket], [control_socket], [], self.connect_to*self.connect_retry)

	    if r or w:
		#Get the socket error condition...
		rtn = control_socket.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
	    else:
                Trace.log(e_errors.ERROR, "error connecting to %s (%s)" %
                          (ticket['callback_addr'], os.strerror(errno.ETIMEDOUT)))
                # just for a case
                try:
                    control_socket.close()
                    listen_socket.close()
                except:
                    pass

                self.control_socket, self.client_socket = None, None
                self.run_in_thread('finish_transfer_setup_thread', self.finish_transfer_setup)
                return
	    #...if it is zero then success, otherwise it failed.
            if rtn != 0:
                Trace.log(e_errors.ERROR, "error connecting to %s (%s)" %
                          (ticket['callback_addr'], os.strerror(rtn)))
                # just for a case
                try:
                    control_socket.close()
                    listen_socket.close()
                except:
                    pass

                self.control_socket, self.client_socket = None, None
                self.run_in_thread('finish_transfer_setup_thread', self.finish_transfer_setup)
                return

	    # we have a connection
            fcntl.fcntl(control_socket.fileno(), FCNTL.F_SETFL, flags)
            Trace.trace(10, "connected")
            try:
                ### cgw - abstract this to a check_valid_filename method of the driver ?
                null_err = 0
                if self.config['driver'] == "NullDriver": 
                    filename = ticket['wrapper'].get("pnfsFilename",'')
                    for w in string.split(filename,'/'):
                        if w.find("NULL") != -1:
                            # found
                            break
                    else: # for
                        # not found
                        ticket['status']=(e_errors.USERERROR, "NULL not in PNFS path")
                        #self.send_client_done(ticket, e_errors.USERERROR, "NULL not in PNFS path")
                        Trace.log(e_errors.USERERROR, "NULL not in PNFS path")
                        self.state = self.save_state
                        null_err = 1
                    if not null_err:
                        wrapper_type = volume_family.extract_wrapper(self.tmp_vf)
                        if ticket['work'] == 'write_to_hsm' and wrapper_type != "null":
                            ticket['status']=(e_errors.USERERROR, 'only "null" wrapper is allowed for NULL mover')
                            #self.send_client_done(ticket, e_errors.USERERROR,
                            #                      'only "null" wrapper is allowed for NULL mover')
                            self.state = self.save_state
                            null_err = 1
                
                if self.setup_mode == ASSERT:
                    ticket['status'] = (e_errors.OK, None)
                Trace.log (e_errors.INFO,"SENDING %s"%(ticket,))
                callback.write_tcp_obj(control_socket, ticket)
                if null_err:
                    # just for a case
                    try:
                        control_socket.close()
                        listen_socket.close()
                    except:
                        pass
                    
                    self.control_socket, self.client_socket = None, None
                    self.run_in_thread('finish_transfer_setup_thread', self.finish_transfer_setup)
                    return
                    
                # for ASSERT finish here
                if self.setup_mode == ASSERT:
                    listen_socket.close()
                    self.control_socket  = control_socket
                    self.run_in_thread('volume_assert__thread', self.assert_vol)
                    return
            except:
                exc, detail, tb = sys.exc_info()
                Trace.log(e_errors.ERROR,"error in connect_client: %s" % (detail,))
                # just for a case
                try:
                    control_socket.close()
                    listen_socket.close()
                except:
                    pass
                self.control_socket, self.client_socket = None, None
                self.run_in_thread('finish_transfer_setup_thread', self.finish_transfer_setup)
                return
            # we expect a prompt call-back here
            Trace.trace(10, "select: listening for client callback")
            read_fds,write_fds,exc_fds=select.select([listen_socket],[],[],60) # one minute timeout
            Trace.trace(10, "select returned %s" % ((listen_socket in read_fds),))
            if listen_socket in read_fds:
                Trace.trace(10, "accepting client connection")
                client_socket, address = listen_socket.accept()
                if not hostaddr.allow(address):
                    # just for a case
                    try:
                        control_socket.close()
                        listen_socket.close()
                        client_socket.close()
                    except:
                        pass
		    self.control_socket, self.client_socket = None, None
                    self.run_in_thread('finish_transfer_setup_thread', self.finish_transfer_setup)
                    return
                if data_ip:
                    interface=hostaddr.interface_name(data_ip)
                    if interface:
                        status=socket_ext.bindtodev(client_socket.fileno(),interface)
                        if status:
                            Trace.log(e_errors.ERROR, "bindtodev(%s): %s"%(interface,os.strerror(status)))

                listen_socket.close()
                self.client_ip = address[0]
                Trace.notify("connect %s %s" % (self.shortname, self.client_ip))
                self.net_driver.fdopen(client_socket)
                self.control_socket, self.client_socket = control_socket, client_socket
                self.run_in_thread('finish_transfer_setup_thread', self.finish_transfer_setup)
                return
            else:
                Trace.log(e_errors.ERROR, "timeout on waiting for client connect")
                # just for a case
                try:
                    control_socket.close()
                    listen_socket.close()
                except:
                    pass
                self.control_socket, self.client_socket = None, None
                self.run_in_thread('finish_transfer_setup_thread', self.finish_transfer_setup)
                return
        except:
            exc, msg, tb = sys.exc_info()
            Trace.log(e_errors.ERROR, "connect_client:  %s %s %s"%
                      (exc, msg, traceback.format_tb(tb)))
            # just for a case
            try:
                control_socket.close()
                listen_socket.close()
            except:
                pass
            self.control_socket, self.client_socket = None, None
            self.run_in_thread('finish_transfer_setup_thread', self.finish_transfer_setup)
    
    def format_lm_ticket(self, state=None, error_info=None, returned_work=None, error_source=None):
        status = e_errors.OK, None
        work = None
        if state is None:
            state = self.state
        Trace.trace(20,"format_lm_ticket: state %s error_info %s error_source %s"%
                    (state, error_info, error_source))
        volume_label = self.current_volume
        if self.current_volume:
            volume_label = self.current_volume
            volume_family = self.volume_family
        else:
            volume_label = self.last_volume
            volume_family = self.last_volume_family

        if state is IDLE:
            work = "mover_idle"
        elif state in (HAVE_BOUND,):
            work = "mover_bound_volume"
        elif state in (ACTIVE, SETUP, SEEK, DRAINING, CLEANING, MOUNT_WAIT, DISMOUNT_WAIT, FINISH_WRITE):
            work = "mover_busy"
            if state == SETUP:
                try:
                    volume_label = self.tmp_vol
                    volume_family = self.tmp_vf
                except AttributeError:
                    pass
            if error_info:
                status = error_info
        elif state in (ERROR, OFFLINE):
            work = "mover_error"  ## XXX If I'm offline should I send mover_error? I don't think so....
            if self.setup_mode == ASSERT:
                volume_label = self.tmp_vol
                volume_family = self.tmp_vf
                
            if error_info is None:
                status = self.last_error
            else:
                status = error_info
        if work is None:
            Trace.log(e_errors.ERROR, "state: %s work: %s" % (state_name(state),work))

        if not status:
            status = e_errors.OK, None
            
        if type(status) != type(()) or len(status)!=2:
            Trace.log(e_errors.ERROR, "status should be 2-tuple, is %s" % (status,))
            if len(status) == 1:
                status = (status, None)
            else:
                status = (status[0], status[1])

        now = time.time()
        if self.unique_id and state in (IDLE, HAVE_BOUND):
            ## If we've been idle for more than 15 minutes, force the LM to clear
            ## any entry for this mover in the work_at_movers.  Yes, this is a
            ## kludge, but it keeps the system from getting completely hung up
            ## if the LM doesn't realize we've finished a transfer.
            if now - self.state_change_time > 900:
                self.unique_id = None

        Trace.trace(20, "format_lm_ticket: volume info %s"%(self.vol_info,))
        if not self.vol_info:
            volume_status = (['none', 'none'], ['none','none'])
        else:
            volume_status = (self.vol_info.get('system_inhibit',['Unknown', 'Unknown']),
                             self.vol_info.get('user_inhibit',['Unknown', 'Unknown']))
        if self.transfer_deficiency < 1.:
            self.transfer_deficiency = 1.
        ticket =  {
            "mover":  self.name,
            "address": self.address,
            "external_label":  volume_label,
            "current_location": loc_to_cookie(self.current_location),
            "read_only" : 0, ###XXX todo: multiple drives on one scsi bus, write locking
            'mover_type': self.mover_type,
            'ip_map':self.ip_map,
            "returned_work": returned_work,
            "state": state_name(self.state),
            "status": status,
            "volume_family": volume_family,
            "volume_status": volume_status,
            "operation": mode_name(self.mode),
            "error_source": error_source,
            "unique_id": self.unique_id,
            "work": work,
            "transfer_deficiency": int(self.transfer_deficiency),
            'time_in_state': now - self.state_change_time,

            }
        return ticket

    def run_in_thread(self, thread_name, function, args=(), after_function=None):
        thread = getattr(self, thread_name, None)
        for wait in range(5):
            if thread and thread.isAlive():
                Trace.trace(20, "thread %s is already running, waiting %s" % (thread_name, wait))
                time.sleep(1)
        if thread and thread.isAlive():
                Trace.log(e_errors.ERROR, "thread %s is already running" % (thread_name))
                return -1
        if after_function:
            args = args + (after_function,)
        Trace.trace(20, "create thread: target %s name %s args %s" % (function, thread_name, args))
        thread = threading.Thread(group=None, target=function,
                                  name=thread_name, args=args, kwargs={})
        setattr(self, thread_name, thread)
        Trace.trace(20, "starting thread %s"%(dir(thread,)))
        try:
            thread.start()
        except:
            exc, detail, tb = sys.exc_info()
            Trace.log(e_errors.ERROR, "starting thread %s: %s" % (thread_name, detail))
        return 0
    
    def dismount_volume(self, after_function=None):
        broken = ""
        self.dismount_time = None
        Trace.log(e_errors.INFO, "Updating stats")
        #try:
        self.update_stat()
        #except TypeError:
            #exc, msg = sys.exc_info()[:2]
            #Trace.log(e_errors.ERROR, "in update_stat: %s %s" % (exc, msg))
            # perhaps it is due to scsi error
            #self.watch_syslog()
        #except:
            # I do not know what kind of exception this can be 
        #    exc, msg = sys.exc_info()[:2]
        #    Trace.log(e_errors.ERROR, "in update_stat2: %s %s" % (exc, msg))

        if not self.do_eject:
            ### AM I do not know if this is correct but it does what it supposed to
            ### Do not eject if specified
            Trace.log(e_errors.INFO, "Do not eject specified")
            self.state = HAVE_BOUND
            if self.draining:
                #self.state = OFFLINE
                self.offline()
            return

        self.state = DISMOUNT_WAIT
        Trace.log(e_errors.INFO, "Ejecting tape")

        ejected = self.tape_driver.eject()
        if ejected == -1:
            # see what threads are running
            threads = threading.enumerate()
            for thread in threads:
                if thread.isAlive():
                    thread_name = thread.getName()
                    Trace.log(e_errors.INFO,"Thread %s is running" % (thread_name,))
                else:
                    Trace.log(e_errors.INFO,"Thread is dead")
            if self.can_force_eject:
                # try to unload tape if robot is STK. It can do this
                Trace.log(e_errors.INFO,"Eject failed. For STK robot will try to unload anyway")
                Trace.alarm(e_errors.ERROR, "Eject failed. Can be a problem with tape drive")
                after_function = self.offline
            else:
                
                broken = "Cannot eject tape"

                if self.current_volume:
                    try:
                        #self.vcc.set_system_noaccess(self.current_volume)
                        self.set_volume_noaccess(self.current_volume)
                    except:
                        exc, msg, tb = sys.exc_info()
                        broken = broken + " set_system_noaccess failed: %s %s" %(exc, msg)                

                self.broken(broken)

                return
        Trace.log(e_errors.INFO, "Tape is ejected")

        self.tape_driver.close()
        self.last_volume = self.current_volume
        self.last_volume_family = self.volume_family
        self.last_location = self.current_location

        if (self.vol_info.has_key('external_label')
            and self.vol_info['external_label']
            and self.vol_info['external_label'] != self.current_volume):
            # mover has request for a different volume (adminpi request)
            vol_info = self.vcc.inquire_vol(self.current_volume)
        else: vol_info = None
        if not self.vol_info.get('external_label'):
            if self.vcc:
                if self.current_volume:
                    v = self.vcc.inquire_vol(self.current_volume)
                    if type(v) is type({}) and v.has_key('status') and v['status'][0]==e_errors.OK:
                        self.vol_info.update(v)
                    else:
                        Trace.log(e_errors.ERROR, "dismount_volume: inquire_vol(%s)->%s" %
                                  (self.current_volume, v))
                else:
                    Trace.log(e_errors.ERROR, "dismount_volume: volume=%s" % (self.current_volume,))

        if not self.vol_info.get('external_label'):
            if self.current_volume:
                self.vol_info['external_label'] = self.current_volume
            else:
                self.vol_info['external_label'] = "Unknown"

        if not self.vol_info.get('media_type'):
            self.vol_info['media_type'] = self.media_type #kludge

        if not vol_info: vol_info = self.vol_info

        self.unload_volume(vol_info, after_function=after_function)
        
    def unload_volume(self, vol_info,after_function=None):
        broken= ''
        Trace.notify("unload %s %s" % (self.shortname, self.current_volume))
        Trace.log(e_errors.INFO, "dismounting %s" %(self.current_volume,))
        self.asc.log_start_dismount(self.current_volume,self.config['product_id'])
        mcc_reply = self.mcc.unloadvol(vol_info, self.name, self.mc_device)

        status = mcc_reply.get('status')
        if status and status[0]==e_errors.OK:
            self.asc.log_finish_dismount(self.current_volume)
            tm = time.localtime(time.time())
            time_msg = "%.2d:%.2d:%.2d" %  (tm[3], tm[4], tm[5])
            Trace.log(e_errors.INFO, "dismounted %s %s %s"%(self.current_volume,self.config['product_id'], time_msg))
            #self.current_volume = None
            if self.setup_mode == ASSERT:
                self.send_client_done(self.current_work_ticket, e_errors.OK, None)

            if self.draining:
                #self.state = OFFLINE
                self.offline()
            elif after_function:
                Trace.trace(20,"after function %s" % (after_function,))
                after_function()

        ###XXX aml-specific hack! Media changer should provide a layer of abstraction
        ### on top of media changer error returns, but it doesn't  :-(
        elif status[-1] == "the drive did not contain an unloaded volume":
            if self.draining:
                #self.state = OFFLINE
                self.offline()
            else:
                self.idle()
        else:
##            self.error(status[-1], status[0])
            
            self.asc.log_finish_dismount_err(self.current_volume)
            broken = "dismount failed: %s %s" %(status[-1], status[0])
            if self.current_volume:
                try:
                    #self.vcc.set_system_noaccess(self.current_volume)
                    self.set_volume_noaccess(self.current_volume)
                except:
                    exc, msg, tb = sys.exc_info()
                    broken = broken + " set_system_noaccess failed: %s %s" %(exc, msg)
            self.broken(broken)
            time.sleep(3)
            self.offline()
        return
    
    def mount_volume(self, volume_label, after_function=None):
        self.dismount_time = None
        if self.current_volume:
            old_volume = self.current_volume
            self.dismount_volume()
            # tell lm that previously mounted volume is dismounted
            vinfo = self.vcc.inquire_vol(old_volume)
            volume_status = (vinfo.get('system_inhibit',['Unknown', 'Unknown']),
                             vinfo.get('user_inhibit',['Unknown', 'Unknown']))

            volume_family = vinfo.get('volume_family', 'Unknown')
            ticket =  {
                "mover":  self.name,
                "address": self.address,
                "external_label":  old_volume,
                "current_location": loc_to_cookie(self.current_location),
                "read_only" : 0, ###XXX todo: multiple drives on one scsi bus, write locking
                "returned_work": None,
                "state": state_name(IDLE),
                "status": (e_errors.OK, None),
                "volume_family": volume_family,
                "volume_status": volume_status,
                "operation": mode_name(self.mode),
                "error_source": None,
                "unique_id": self.unique_id,
                "work": "mover_busy",
                }
            Trace.trace(14,"mount_volume: after dismount %s"%(ticket,)) 
            for lib, addr in self.libraries:
                self.udpc.send_no_wait(ticket, addr)


        self.state = MOUNT_WAIT
        self.current_volume = volume_label


        # XXX DEBUG Block of code to get more info on why label is missing on some mounts
        if not self.vol_info.get('external_label'):
            Trace.log(e_errors.ERROR, "mount_volume: no external label in vol_info.  volume_label=%s" % (volume_label,))
            if self.vcc:
                if self.current_volume:
                    v = self.vcc.inquire_vol(self.current_volume)
                    if type(v) is type({}) and v.has_key('status') and v['status'][0]==e_errors.OK:
                        self.vol_info.update(v)
                    else:
                        Trace.log(e_errors.ERROR, "mount_volume: inquire_vol(%s)->%s" %
                                  (self.current_volume, v))
                else:
                    Trace.log(e_errors.ERROR, "mount_volume: no self.current_volume self.current_volue=%s volume_label=%s" %
                              (self.current_volume,volume_label))
            else:
                Trace.log(e_errors.ERROR, "mount_volume: no self.vcc")

        if not self.vol_info.get('external_label'):
            if self.current_volume:
                self.vol_info['external_label'] = self.current_volume
            else:
                self.vol_info['external_label'] = "Unknown"

        if not self.vol_info.get('media_type'):
            self.vol_info['media_type'] = self.media_type #kludge
        # XXX END DEBUG Block of code to get more info on why label is missing on some mounts


        Trace.notify("loading %s %s" % (self.shortname, volume_label))
        tm = time.localtime(time.time()) # get the local time
        time_msg = "%.2d:%.2d:%.2d" %  (tm[3], tm[4], tm[5])
        Trace.log(e_errors.INFO, "mounting %s %s %s"%(volume_label, self.config['product_id'],time_msg),
                  msg_type=Trace.MSG_MC_LOAD_REQ)

        self.asc.log_start_mount(self.current_volume,self.config['product_id'])
                                 
        self.current_location = 0L
        vi = self.vol_info
        Trace.trace(12, "override_ro_mount %s"%(self.override_ro_mount,))
        if self.override_ro_mount:
            vi['system_inhibit'][1] = 'none'
            vi['user_inhibit'][1] = 'none'
        mcc_reply = self.mcc.loadvol(vi, self.name, self.mc_device)
        self.timer('mount_time')
        status = mcc_reply.get('status')
        Trace.trace(10, 'mc replies %s' % (status,))

        #Do another query volume, just to make sure its status has not changed
        self.vol_info.update(self.vcc.inquire_vol(volume_label))

        if status[0] == e_errors.OK:
            self.vcc.update_counts(self.current_volume, mounts=1)
            self.asc.log_finish_mount(self.current_volume)
            Trace.notify("loaded %s %s" % (self.shortname, volume_label))        
            self.init_stat(self.device, self.logname)
            tm = time.localtime(time.time()) # get the local time
            time_msg = "%.2d:%.2d:%.2d" %  (tm[3], tm[4], tm[5])
            Trace.log(e_errors.INFO, "mounted %s %s %s"%(volume_label,self.config['product_id'], time_msg),
                  msg_type=Trace.MSG_MC_LOAD_DONE)

            if self.mount_delay:
                Trace.trace(25, "waiting %s seconds after mount"%(self.mount_delay,))
                time.sleep(self.mount_delay)
            if after_function:
                Trace.trace(10, "mount: calling after function")
                after_function()
        else: #Mount failure, do not attempt to recover
            Trace.log(e_errors.ERROR, "mount %s: %s" % (volume_label, status))
            self.last_error = status
            self.asc.log_finish_mount_err(volume_label)
##            "I know I'm swinging way to far to the right" - Jon
##            Trace.log(e_errors.ERROR, "mount %s: %s, dismounting" % (volume_label, status))
##            self.state = DISMOUNT_WAIT
##            self.transfer_failed(e_errors.MOUNTFAILED, 'mount failure %s' % (status,), error_source=ROBOT)
##            self.dismount_volume(after_function=self.idle)
            broken = None
               
            if ((status[1] in (e_errors.MC_VOLNOTHOME, e_errors.MC_NONE,
                              e_errors.MC_FAILCHKVOL, e_errors.MC_VOLNOTFOUND,
                              e_errors.MC_DRVNOTEMPTY)) or
                (status[0] == e_errors.TIMEDOUT)):          
                # mover is all right
                # error is only tape or MC related
                # send error to LM and go into the IDLE state
                if ((status[1] in (e_errors.MC_NONE,
                                   e_errors.MC_FAILCHKVOL,
                                   e_errors.MC_DRVNOTEMPTY)) or
                    (status[0] == e_errors.TIMEDOUT)):
                    err_source = ROBOT
                else:
                    err_source = TAPE
                if status[0] == e_errors.TIMEDOUT:
                    s_status = status
                else:
                    s_status = (status[1], status[2])
                # send error message only to LM that called us
                self.send_error_msg(error_info = s_status,
                                    error_source=err_source,
                                    returned_work=None)
                self.send_client_done(self.current_work_ticket, e_errors.MOUNTFAILED, s_status[0])
                self.net_driver.close()
                if status[1] == e_errors.MC_DRVNOTEMPTY:
                    Trace.alarm(e_errors.ERROR, "mount %s failed: %s" % (volume_label, status))
                    self.offline()
                    return
                else:    
                    self.state = IDLE
                self.current_volume = None
                self.last_error = s_status
                return
            else:    
                broken = "mount %s failed: %s" % (volume_label, status)
            try:
                #self.vcc.set_system_noaccess(volume_label)
                self.set_volume_noaccess(volume_label)
            except:
                exc, msg, tb = sys.exc_info()
                broken = broken + " set_system_noaccess failed: %s %s" %(exc, msg)
            if broken:
                # error out and do not allow dismount as nothing has
                # been mounted yet
                self.transfer_failed(exc=e_errors.MOUNTFAILED, msg=broken,error_source=ROBOT, dismount_allowed=0)
                self.broken(broken) # this is to address AML2 mount failures
            
            #self.current_volume = None
            
    def seek_to_location(self, location, eot_ok=0, after_function=None): #XXX is eot_ok needed?
        Trace.trace(10, "seeking to %s, after_function=%s"%(location,after_function))
        failed=0
        try:
            self.tape_driver.seek(location, eot_ok) #XXX is eot_ok needed?
        except:
            exc, detail, tb = sys.exc_info()
            self.transfer_failed(e_errors.POSITIONING_ERROR, 'positioning error %s' % (detail,), error_source=TAPE)
            failed=1
        self.timer('seek_time')
        self.current_location = self.tape_driver.tell()
        if self.mode is WRITE:
            previous_eod = cookie_to_long(self.vol_info['eod_cookie'])
            Trace.trace(10,"seek_to_location: current location %s, eod %s"%
                        (self.current_location, previous_eod))
            # compare location reported by driver with eod cookie
            if self.current_location != previous_eod:
                Trace.log(e_errors.ERROR, " current location %s != eod %s" %
                          (self.current_location, previous_eod))
                detail = "wrong location %s, eod %s"%(self.current_location, previous_eod)
                self.transfer_failed(e_errors.WRITE_ERROR, detail, error_source=TAPE)
                failed = 1
        # If seek takes too long and main thread sets mover to ERROR state
        # fail the transfer
        if self.state == ERROR:
            Trace.trace(e_errors.WARNING, "mover went to the error state while positioning tape. Will try to reset the mover")
            if self.current_volume:
                vol = self.current_volume
                vol_info = self.vol_info
                self.dismount_volume(after_function=self.idle)
                #self.unload_volume(self.vol_info, after_function=self.idle)
            else:
                vol = self.last_volume # current_volume does not exist, perhaps was unloaded already
                if self.state == ERROR:
                    self.state = IDLE
                else:
                    # lost track of the state
                    Trace.log(e_errors.ERROR,"lost track of states. State %s"%(state_name(self.state),))
                    return
                if self.state == IDLE:
                    # successful unload
                    vol_info = (self.vcc.inquire_vol(vol))
                if vol_info['system_inhibit'][0] == e_errors.NOACCESS:
                    # clear NOACCESS
                    ret = self.vcc.clr_system_inhibit(vol,
                                                      what='system_inhibit',
                                                      pos = 0,
                                                      timeout= 10,
                                                      retry= 2)
                    if ret['status'][0] != e_errors.OK:
                        Trace.log(e_errors.ERROR,
                                  "failed to clear system inhibit for %s. Status %s"%(self.current_volume,ret['status']))
            return        
            
        if after_function and not failed:
            Trace.trace(10, "seek calling after function %s" % (after_function,))
            after_function()

    def start_transfer(self):
        Trace.trace(10, "start transfer")
        #If we've gotten this far, we've mounted, positioned, and connected to the client.
        # If seek takes too long and main thread sets mover to ERROR state
        # fail the transfer
        if self.state == ERROR:
            Trace.log(e_errors.ERROR, "State ERROR, can not proceed")
        
        self.state = ACTIVE
        if self.draining:
            self.state = DRAINING
        if self.mode is WRITE:
            self.run_in_thread('net_thread', self.read_client)
            self.run_in_thread('tape_thread', self.write_tape)
        elif self.mode is READ:
            self.run_in_thread('tape_thread', self.read_tape)
            self.run_in_thread('net_thread', self.write_client)
        else:
            self.transfer_failed(e_errors.ERROR, "invalid mode %s" % (self.mode,))
                
    def status(self, ticket):
        x = ticket # to trick pychecker
        now = time.time()
        status_info = (e_errors.OK, None)
        if self.state == ERROR:
            status_info = self.last_error
        try:
            if self.buffer:
                bytes_buffered = self.buffer.nbytes()
                buffer_min_bytes = self.buffer.min_bytes
                buffer_max_bytes = self.buffer.max_bytes
            else:
                bytes_buffered = 0
                buffer_min_bytes = 0
                buffer_max_bytes = 0
        except AttributeError:
            # try it again
            time.sleep(3)
            if self.buffer:
                bytes_buffered = self.buffer.nbytes()
                buffer_min_bytes = self.buffer.min_bytes
                buffer_max_bytes = self.buffer.max_bytes
            else:
                bytes_buffered = 0
                buffer_min_bytes = 0
                buffer_max_bytes = 0
        tick = { 'status'       : status_info,
                 'drive_sn'     : self.config['serial_num'],
                 'drive_vendor' : self.config['vendor_id'],
                 'drive_id'     : self.config['product_id'],
                 #
                 'state'        : state_name(self.state),
                 'transfers_completed'     : self.transfers_completed,
                 'transfers_failed': self.transfers_failed,
                 'bytes_read'     : self.bytes_read,
                 'bytes_read_last': self.bytes_read_last,
                 'bytes_written'     : self.bytes_written,
                 'bytes_buffered' : bytes_buffered,
                 'successful_writes': self.files_written_cnt,
                 # from "work ticket"
                 'bytes_to_transfer': self.bytes_to_transfer,
                 'files'        : self.files,
                 'last_error': self.last_error,
                 'mode'         : mode_name(self.mode),
                 'current_volume': self.current_volume,
                 'current_location': self.current_location,
                 'last_volume' : self.last_volume,
                 'last_location': self.last_location,
                 'time_stamp'   : now,
                 'time_in_state': now - self.state_change_time,
                 'buffer_min': buffer_min_bytes,
                 'buffer_max': buffer_min_bytes,
                 'rate of network': self.net_driver.rates()[0],
                 'rate of tape': self.tape_driver.rates()[0],
                 'default_dismount_delay': self.default_dismount_delay,
                 'max_dismount_delay': self.max_dismount_delay,
                 'client': self.client_ip,
                 }
        if self.state is HAVE_BOUND and self.dismount_time and self.dismount_time>now:
            tick['will dismount'] = 'in %.1f seconds' % (self.dismount_time - now)
            
        self.reply_to_caller(tick)
        return

    def timer(self, key):
        if not self.current_work_ticket:
            return
        ticket = self.current_work_ticket
        if not ticket.has_key('times'):
            ticket['times']={}
        now = time.time()
        ticket['times'][key] = now - self.t0
        self.t0 = now
    
    def lockfile_name(self):
        d=os.environ.get("ENSTORE_TMP","/tmp")
        return os.path.join(d, "mover_lock%s"%(self.name,))
        
    def create_lockfile(self):
        filename=self.lockfile_name()
        try:
            f=open(filename,'w')
            f.write('locked\n')
            f.close()
        except (OSError, IOError):
            Trace.log(e_errors.ERROR, "Cannot write %s"%(filename,))
            
    def remove_lockfile(self):
        filename=self.lockfile_name()
        try:
            os.unlink(filename)
        except (OSError, IOError):
            Trace.log(e_errors.ERROR, "Cannot unlink %s"%(filename,))

    def check_lockfile(self):
        return os.path.exists(self.lockfile_name())
        
    def start_draining(self, ticket):       # put itself into draining state
        x = ticket # to trick pychecker
        save_state = self.state
        self.draining = 1 
        if self.state in (ACTIVE, FINISH_WRITE):
            self.state = DRAINING
        elif self.state in (IDLE, ERROR):
            self.state = OFFLINE
        elif self.state is HAVE_BOUND:
            self.state = DRAINING # XXX CGW should dismount here. fix this
        self.create_lockfile()
        out_ticket = {'status':(e_errors.OK,None)}
        self.reply_to_caller(out_ticket)
        if save_state is HAVE_BOUND and self.state is DRAINING:
            self.dismount_volume()
            self.state = OFFLINE
        return

    def stop_draining(self, ticket):        # put itself into draining state
        x = ticket # to trick pychecker
        if self.state != OFFLINE:
            out_ticket = {'status':("EPROTO","Not OFFLINE")}
            self.reply_to_caller(out_ticket)
            return
        out_ticket = {'status':(e_errors.OK,None)}
        self.reply_to_caller(out_ticket)
        ## XXX here we need to check if tape is mounted
        ## if yes go to have bound, NOT idle AM
        Trace.trace(11,"check lockfile %s"%(self.check_lockfile(),))
        self.remove_lockfile()
        Trace.trace(11,"check lockfile %s"%(self.check_lockfile(),))
        Trace.log(e_errors.INFO,"restarting %s"% (self.name,))
        self.restart()
        #self.idle()

    def warm_restart(self, ticket):
        self.start_draining(ticket)
        out_ticket = {'status':(e_errors.OK,None),'state':self.state}
        self.reply_to_caller(out_ticket)
        while 1:
            if self.state == OFFLINE:
                self.stop_draining(ticket)
            elif self.state !=  ERROR:
                time.sleep(2)
                Trace.trace(11,"waiting in state %s for OFFLINE" % (self.state,))
            else:
                Trace.alarm(e_errors.ERROR, "can not restart. State: %s" % (self.state,))
        
    def clean_drive(self, ticket):
        x = ticket # to trick pychecker
        save_state = self.state
        if self.state not in (IDLE, OFFLINE):
            ret = {'status':("EPROTO", "Cleaning not allowed in %s state" % (state_name(self.state)))}
        else:
            self.state = CLEANING
            ret = self.mcc.doCleaningCycle(self.config)
            if ret['status'][0] != e_errors.OK:
                Trace.alarm(e_errors.WARNING,"clean request returned %s"%(ret['status'],))
            self.state = save_state
        self.reply_to_caller(ret)
        

class MoverInterface(generic_server.GenericServerInterface):

    def __init__(self):
        # fill in the defaults for possible options
        generic_server.GenericServerInterface.__init__(self)

    def valid_dictionaries(self):
        return ({},)
        
    #  define our specific help
    #def parameters(self):
    #    return 'mover_name'

    parameters = ["mover_name"]

    # parse the options like normal but make sure we have a mover
    def parse_options(self):
        option.Interface.parse_options(self)
        # bomb out if we don't have a mover
        if len(self.args) < 1 :
            self.missing_parameter(self.parameters())
            self.print_help(),
            os._exit(1)
        else:
            self.name = self.args[0]

class DiskMover(Mover):

    def start(self):
        name = self.name
        self.t0 = time.time()
        self.config = self.csc.get(name)
        if self.config['status'][0] != 'ok':
            raise MoverError('could not start mover %s: %s'%(name, self.config['status']))

        logname = self.config.get('logname', name)
        Trace.init(logname)
        
        self.config['device'] = os.path.expandvars(self.config['device'])
        self.state = IDLE
        # check if device exists
        if not os.path.exists(self.config['device']):
            Trace.alarm(e_errors.ERROR, "Cannot start. Device %s does not exist"%(self.config['device'],))
            self.state = OFFLINE
            
        #how often to send an alive heartbeat to the event relay
        self.alive_interval = monitored_server.get_alive_interval(self.csc, name, self.config)
        self.address = (self.config['hostip'], self.config['port'])
        self.lm_address = None # LM that called mover
        self.default_block_size = 131072
        self.mover_type = self.config.get('type','')
        self.ip_map = self.config.get('ip_map','')
        Trace.log(e_errors.INFO, "starting %s mover %s" % (self.mover_type, self.name,))
        self.media_type = self.config.get('media_type', 'disk') #XXX
        self.min_buffer = self.config.get('min_buffer', 8*MB)
        self.max_buffer = self.config.get('max_buffer', 64*MB)
        self.max_rate = self.config.get('max_rate', 11.2*MB) #XXX
        self.log_mover_state = self.config.get('log_state', None)
        self.restart_on_error = self.config.get("restart_on_error", None)
        self.transfer_deficiency = 1.0
        self.buffer = None
        self.udpc = udp_client.UDPClient()
        self.last_error = (e_errors.OK, None)
        if self.check_sched_down() or self.check_lockfile():
            self.state = OFFLINE
        self.current_volume = None #external label of current mounted volume
        self.last_volume = None
        self.last_volume_family = None

        self.mode = None # READ, WRITE or ASSERT
        self.setup_mode = None
        self.bytes_to_transfer = 0L
        self.bytes_to_read = 0L
        self.bytes_to_write = 0L
        self.bytes_read = 0L
        self.bytes_read_last = 0L
        self.bytes_written = 0L
        self.bytes_written_last = 0L
        self.volume_family = None 
        self.files = ('','')
        self.transfers_completed = 0
        self.transfers_failed = 0
        self.error_times = []
        self.consecutive_failures = 0
        self.max_consecutive_failures = 2
        self.max_failures = 3
        self.failure_interval = 3600
        self.current_work_ticket = {}
        self.vol_info = {}
        self.file_info = {}
        self.dismount_time = None
        self.delay = 0
        self.fcc = None
        self.media_transfer_time = 0.
        
        self.client_hostname = None
        self.client_ip = None  #NB: a client may have multiple interfaces, this is
                                         ##the IP of the interface we're using
        
        self.tape_driver = disk_driver.DiskDriver()
        import net_driver
        self.net_driver = net_driver.NetDriver()
        self.client_socket = None

        self.config['name']=self.name 
        self.config['product_id']='Unknown'
        self.config['serial_num']=0
        self.config['vendor_id']='Unknown'
        self.config['local_mover'] = 0 #XXX who still looks at this?
        self.driver_type = self.config['driver']

        self.max_consecutive_failures = self.config.get('max_consecutive_failures',
                                                        self.max_consecutive_failures)
        self.max_failures = self.config.get("max_failures", self.max_failures)
        self.failure_interval = self.config.get("failure_interval", self.failure_interval)
        
        self.default_dismount_delay = self.config.get('dismount_delay', 0)
        if self.default_dismount_delay < 0:
            self.default_dismount_delay = 31536000 #1 year
        self.max_dismount_delay = max(
            self.config.get('max_dismount_delay', 600),
            self.default_dismount_delay)
        
        self.libraries = []
        lib_list = self.config['library']
        if type(lib_list) != type([]):
            lib_list = [lib_list]
        for lib in lib_list:
            lib_config = self.csc.get(lib)
            self.libraries.append((lib, (lib_config['hostip'], lib_config['port'])))

        #how often to send a message to the library manager
        self.update_interval = self.config.get('update_interval', 15)

        self.check_written_file_period = self.config.get('check_written_file', 0)
        self.files_written_cnt = 0
        self.max_time_in_state = self.config.get('max_time_in_state', 600) # maximal time allowed in a certain states

        self.max_in_state_cnt = self.config.get('max_in_state_cnt', 3) 
        dispatching_worker.DispatchingWorker.__init__(self, self.address)
        self.add_interval_func(self.update_lm, self.update_interval) #this sets the period for messages to LM.
        self.add_interval_func(self.need_update, 1) #this sets the period for checking if child thread has asked for update.
        self.set_error_handler(self.handle_mover_error)
        ##start our heartbeat to the event relay process
        self.erc.start_heartbeat(self.name, self.alive_interval, self.return_state)
        ##end of __init__

    # device_dump_S(self, ticket) -- server hook for device_dump()

    def device_dump_S(self, ticket):
        x =ticket # to trick pychecker
        t = {"status":(e_errors.ERROR, "not implemented")}
        self.reply_to_caller(t)
	return

    def idle(self):
        if self.state == ERROR:
            return
        self.state = IDLE
        self.mode = None
        self.vol_info = {}
        self.file_info = {}
        thread = threading.currentThread()
        if thread:
            thread_name = thread.getName()
        else:
            thread_name = None
        # if running in the main thread update lm
        if thread_name is 'MainThread':
            self.update_lm() 
        else: # else just set the update flag
            self.need_lm_update = (1, None, 0, None)

    def write_tape(self):
        Trace.trace(8, "write_tape starting, bytes_to_write=%s" % (self.bytes_to_write,))
        Trace.trace(8, "bytes_to_transfer=%s" % (self.bytes_to_transfer,))
        driver = self.tape_driver
        count = 0
        defer_write = 1
        failed = 0
        self.media_transfer_time = 0.
        # send a trigger message to the client
        bytes_written = self.net_driver.write(bytes_notified, # write anything
                                              0,
                                              1) # just 1 byte

        #Initialize thresholded transfer notify messages.
        bytes_notified = 0L
        Trace.notify("transfer %s %s %s media %s %.3f" %
                     (self.shortname, self.bytes_written,
                      self.bytes_to_write, self.buffer.nbytes(), time.time()))

        while self.state in (ACTIVE, DRAINING) and self.bytes_written<self.bytes_to_write:
            if self.tr_failed:
                Trace.trace(27,"write_tape: tr_failed %s"%(self.tr_failed,))
                break
            empty = self.buffer.empty()
            if (empty or
                (defer_write and (self.bytes_read < self.bytes_to_read and self.buffer.low()))):
                if empty:
                    defer_write = 1
                Trace.trace(9,"write_tape: buffer low %s/%s, wrote %s/%s, defer=%s"%
                            (self.buffer.nbytes(), self.buffer.min_bytes,
                             self.bytes_written, self.bytes_to_write,
                             defer_write))
                self.buffer.write_ok.clear()
                self.buffer.write_ok.wait(1)
                if (defer_write and (self.bytes_read==self.bytes_to_read or not self.buffer.low())):
                    defer_write = 0
                continue

            count = (count + 1) % 20
            if count == 0:
                ##Dynamic setting of low-water mark
                if self.bytes_read >= self.buffer.min_bytes:
                    netrate, junk = self.net_driver.rates()
                    taperate = self.max_rate
                    if taperate > 0:
                        ratio = netrate/(taperate*1.0)
                        optimal_buf = self.bytes_to_transfer * (1-ratio)
                        optimal_buf = min(optimal_buf, 0.5 * self.max_buffer)
                        optimal_buf = max(optimal_buf, self.min_buffer)
                        optimal_buf = int(optimal_buf)
                        Trace.trace(12,"netrate = %.3g, taperate=%.3g" % (netrate, taperate))
                        if self.buffer.min_bytes != optimal_buf:
                            Trace.trace(12,"Changing buffer size from %s to %s"%
                                        (self.buffer.min_bytes, optimal_buf))
                            self.buffer.set_min_bytes(optimal_buf)

            nbytes = min(self.bytes_to_write - self.bytes_written, self.buffer.blocksize)

            bytes_written = 0
            t1 = time.time()
            try:
                bytes_written = self.buffer.block_write(nbytes, driver)
            except:
                exc, detail, tb = sys.exc_info()
                self.transfer_failed(e_errors.WRITE_ERROR, detail, error_source=TAPE)
                failed = 1
                break
            self.media_transfer_time = self.media_transfer_time + (time.time()-t1)
            if bytes_written != nbytes:
                self.transfer_failed(e_errors.WRITE_ERROR, "short write %s != %s" %
                                     (bytes_written, nbytes), error_source=TAPE)
                failed = 1
                break
            self.bytes_written = self.bytes_written + bytes_written

            #If it is time to do so, send the notify message.
            if is_threshold_passed(self.bytes_written, bytes_notified,
                                   self.bytes_to_write):
                bytes_notified = self.bytes_written
                Trace.notify("transfer %s %s %s media %s %.3f" %
                             (self.shortname, self.bytes_written,
                              self.bytes_to_write, self.buffer.nbytes(),
                              time.time()))
            
            if not self.buffer.full():
                self.buffer.read_ok.set()
        if self.tr_failed:
            Trace.trace(27,"write_tape: tr_failed %s"%(self.tr_failed,))
            return

        Trace.trace(8, "write_tape exiting, wrote %s/%s bytes" %( self.bytes_written, self.bytes_to_write))

        if failed: return
        if self.bytes_written == self.bytes_to_write:
            self.tape_driver.flush()
            if self.check_written_file():
                self.tape_driver.close()
                Trace.log(e_errors.INFO, "selective CRC check after writing file")
                have_tape = self.tape_driver.open(self.file, READ)
                if have_tape != 1:
                    Trace.alarm(e_errors.ERROR, "error positioning tape for selective CRC check")

                    self.transfer_failed(e_errors.WRITE_ERROR, "error positioning tape for selective CRC check", error_source=TAPE)
                    return
                
                self.buffer.save_settings()
                bytes_read = 0L
                Trace.trace(20,"write_tape: header size %s" % (self.buffer.header_size,))
                #bytes_to_read = self.bytes_to_transfer + self.buffer.header_size
                bytes_to_read = self.bytes_to_transfer
                header_size = self.buffer.header_size
                # setup buffer for reads
                saved_wrapper = self.buffer.wrapper
                saved_sanity_bytes = self.buffer.sanity_bytes
                saved_complete_crc = self.buffer.complete_crc
                self.buffer.reset((self.buffer.sanity_bytes, self.buffer.sanity_crc), client_crc_on=1)
                self.buffer.set_wrapper(saved_wrapper)
                Trace.trace(22, "starting check after write, bytes_to_read=%s" % (bytes_to_read,))
                driver = self.tape_driver
                first_block = 1
                while bytes_read < bytes_to_read:

                    nbytes = min(bytes_to_read - bytes_read, self.buffer.blocksize)
                    self.buffer.bytes_for_crc = nbytes
                    if bytes_read == 0 and nbytes<self.buffer.blocksize: #first read, try to read a whole block
                        nbytes = self.buffer.blocksize
                    try:
                        b_read = self.buffer.block_read(nbytes, driver)

                        # clean buffer
                        #Trace.trace(22,"write_tape: clean buffer")
                        self.buffer._writing_block = self.buffer.pull()
                        if self.buffer._writing_block:
                            #Trace.trace(22,"write_tape: freeing block")
                            self.buffer._freespace(self.buffer._writing_block)
                        
                    except "CRC_ERROR":
                        exc, detail, tb = sys.exc_info()
                        #Trace.handle_error(exc, detail, tb)
                        Trace.alarm(e_errors.ERROR, "selective CRC check error",
                                    {'outfile':self.current_work_ticket['outfile'],
                                     'infile':self.current_work_ticket['infile'],
                                     'location_cookie':self.current_work_ticket['fc']['location_cookie'],
                                     'external_label':self.current_work_ticket['vc']['external_label']})
                        self.transfer_failed(e_errors.WRITE_ERROR, detail, error_source=TAPE)
                        failed = 1
                        break
                    except:
                        exc, detail, tb = sys.exc_info()
                        #Trace.handle_error(exc, detail, tb)
                        Trace.alarm(e_errors.ERROR, "selective CRC check error")
                        self.transfer_failed(e_errors.WRITE_ERROR, detail, error_source=TAPE)
                        failed = 1
                        break
                    if b_read <= 0:
                        Trace.alarm(e_errors.ERROR, "selective CRC check read error")
                        self.transfer_failed(e_errors.WRITE_ERROR, "read returns %s" % (bytes_read,),
                                             error_source=TAPE)
                        failed = 1
                        break
                    if first_block:
                        bytes_to_read = bytes_to_read + header_size
                        first_block = 0
                    bytes_read = bytes_read + b_read
                    if bytes_read > bytes_to_read: #this is OK, we read a cpio trailer or something
                        bytes_read = bytes_to_read

                Trace.trace(22,"write_tape: read CRC %s write CRC %s"%
                            (self.buffer.complete_crc, saved_complete_crc))
                if failed:
                    return
                if self.buffer.complete_crc != saved_complete_crc:
                    Trace.alarm(e_errors.ERROR, "selective CRC check error")
                    self.transfer_failed(e_errors.WRITE_ERROR, "selective CRC check error",error_source=TAPE)
                    return
                Trace.log(e_errors.INFO, "selective CRC check after writing file completed successfuly")
                self.buffer.restore_settings()
                # position to eod"
            if self.update_after_writing():
                self.files_written_cnt = self.files_written_cnt + 1
                self.transfer_completed()
            else:
                self.transfer_failed(e_errors.EPROTO)

    def read_tape(self):
        Trace.trace(8, "read_tape starting, bytes_to_read=%s" % (self.bytes_to_read,))
        if self.buffer.client_crc_on:
            # calculate checksum when reading from
            # tape (see comment in setup_transfer)
            do_crc = 1
        else:
            do_crc = 0
        driver = self.tape_driver
        failed = 0
        self.media_transfer_time = 0.

        #Initialize thresholded transfer notify messages.
        bytes_notified = 0L
        Trace.notify("transfer %s %s %s media %s %.3f" %
                     (self.shortname, -self.bytes_read,
                      self.bytes_to_read, self.buffer.nbytes(), time.time()))

        while self.state in (ACTIVE, DRAINING) and self.bytes_read < self.bytes_to_read:
            Trace.trace(27,"read_tape: tr_failed %s"%(self.tr_failed,))
            if self.tr_failed:
                break
            if self.buffer.full():
                Trace.trace(9, "read_tape: buffer full %s/%s, read %s/%s" %
                            (self.buffer.nbytes(), self.buffer.max_bytes,
                             self.bytes_read, self.bytes_to_read))
                self.buffer.read_ok.clear()
                self.buffer.read_ok.wait(1)
                continue
            
            nbytes = min(self.bytes_to_read - self.bytes_read, self.buffer.blocksize)
            self.buffer.bytes_for_crc = nbytes
            if self.bytes_read == 0 and nbytes<self.buffer.blocksize: #first read, try to read a whole block
                nbytes = self.buffer.blocksize

            bytes_read = 0
            try:
                t1 = time.time()
                bytes_read = self.buffer.block_read(nbytes, driver)
                self.media_transfer_time = self.media_transfer_time + (time.time()-t1)
            except "CRC_ERROR":
                Trace.alarm(e_errors.ERROR, "CRC error reading tape",
                            {'outfile':self.current_work_ticket['outfile'],
                             'infile':self.current_work_ticket['infile'],
                             'location_cookie':self.current_work_ticket['fc']['location_cookie'],
                             'external_label':self.current_work_ticket['vc']['external_label']})
                self.transfer_failed(e_errors.CRC_ERROR, error_source=TAPE)
                failed = 1
                break
            except:
                exc, detail, tb = sys.exc_info()
                #Trace.handle_error(exc, detail, tb)
                self.transfer_failed(e_errors.READ_ERROR, detail, error_source=TAPE)
                failed = 1
                break
            if bytes_read <= 0:
                self.transfer_failed(e_errors.READ_ERROR, "read returns %s" % (bytes_read,),
                                     error_source=TAPE)
                failed = 1
                break
            if self.bytes_read==0: #Handle variable-sized cpio header
                if len(self.buffer._buf) != 1:
                    Trace.log(e_errors.ERROR,
                              "read_tape: error skipping over cpio header, len(buf)=%s"%(len(self.buffer._buf)))
                b0 = self.buffer._buf[0]
                if len(b0) >= self.wrapper.min_header_size:
                    try:
                        header_size = self.wrapper.header_size(b0)
                    except (TypeError, ValueError), msg:
                        Trace.log(e_errors.ERROR,"Invalid header %s" %(b0[:self.wrapper.min_header_size]))
                        self.transfer_failed(e_errors.READ_ERROR, "Invalid file header", error_source=TAPE)
                        ##XXX NB: the client won't necessarily see this message since it's still trying
                        ## to recieve data on the data socket
                        failed = 1
                        break
                    self.buffer.header_size = header_size
                    self.bytes_to_read = self.bytes_to_read + header_size
            self.bytes_read = self.bytes_read + bytes_read
            if self.bytes_read > self.bytes_to_read: #this is OK, we read a cpio trailer or something
                self.bytes_read = self.bytes_to_read

            #If it is time to do so, send the notify message.
            if is_threshold_passed(self.bytes_read, bytes_notified,
                                   self.bytes_to_read):
                bytes_notified = self.bytes_read
                Trace.notify("transfer %s %s %s media %s %.3f" %
                             (self.shortname, -self.bytes_read,
                              self.bytes_to_read, self.buffer.nbytes(),
                              time.time()))

            if not self.buffer.empty():
                self.buffer.write_ok.set()
        if self.tr_failed:
            Trace.trace(27,"read_tape: tr_failed %s"%(self.tr_failed,))
            return
        if failed: return
        if do_crc:
            if self.tr_failed: return # do not calculate CRC if net thead detected a failed transfer
            Trace.trace(22,"read_tape: calculated CRC %s File DB CRC %s"%
                        (self.buffer.complete_crc, self.file_info['complete_crc']))
            if self.buffer.complete_crc != self.file_info['complete_crc']:
                Trace.log(e_errors.ERROR,"read_tape: calculated CRC %s File DB CRC %s"%
                          (self.buffer.complete_crc, self.file_info['complete_crc']))
                # this is to fix file db
                if self.file_info['complete_crc'] == None:
                    sanity_cookie = (self.buffer.sanity_bytes,self.buffer.sanity_crc)
                    Trace.log(e_errors.WARNING, "found complete CRC set to None in file DB for %s. Changing cookie to %s and CRC to %s" %
                              (self.file_info['bfid'],sanity_cookie, self.buffer.complete_crc))
                    self.fcc.set_crcs(self.file_info['bfid'], sanity_cookie, self.buffer.complete_crc)
                else:
                    if self.tr_failed: return  # do not raise alarm if net thead detected a failed transfer
                    Trace.alarm(e_errors.ERROR, "read_tape CRC error",
                                {'outfile':self.current_work_ticket['outfile'],
                                 'infile':self.current_work_ticket['infile'],
                                 'location_cookie':self.current_work_ticket['fc']['location_cookie'],
                                 'external_label':self.current_work_ticket['vc']['external_label']})
                    self.transfer_failed(e_errors.CRC_ERROR, error_source=TAPE)
                return

        Trace.trace(8, "read_tape exiting, read %s/%s bytes" %
                    (self.bytes_read, self.bytes_to_read))
                
    def write_client(self):
        Trace.trace(8, "write_client starting, bytes_to_write=%s" % (self.bytes_to_write,))
        if not self.buffer.client_crc_on:
            # calculate checksum when writing to
            # the network (see comment in setup_transfer)
            # CRC when sending to the network if client does not CRC
            do_crc = 1
        else:
            do_crc = 0
        driver = self.net_driver
        #be careful about 0-length files
        if self.bytes_to_write > 0 and self.bytes_written == 0 and self.wrapper: #Skip over cpio or other headers
            while self.buffer.header_size is None and self.state in (ACTIVE, DRAINING):
                Trace.trace(8, "write_client: waiting for read_tape to set header info")
                self.buffer.write_ok.clear()
                self.buffer.write_ok.wait(1)
            # writing to "None" will discard headers, leaving stream positioned at
            # start of data
            self.buffer.stream_write(self.buffer.header_size, None)
            Trace.trace(8, "write_client: discarded %s bytes of header info"%(self.buffer.header_size))
        failed = 0

        #Initialize thresholded transfer notify messages.
        bytes_notified = 0L
        Trace.notify("transfer %s %s %s network %s %.3f" %
                     (self.shortname, -self.bytes_written,
                      self.bytes_to_write, self.buffer.nbytes(), time.time()))
           
        while self.state in (ACTIVE, DRAINING) and self.bytes_written < self.bytes_to_write:
            if self.tr_failed:
                break
            if self.buffer.empty():
                Trace.trace(9, "write_client: buffer empty, wrote %s/%s" %
                            (self.bytes_written, self.bytes_to_write))
                self.buffer.write_ok.clear()
                self.buffer.write_ok.wait(1)
                continue
                
            nbytes = min(self.bytes_to_write - self.bytes_written, self.buffer.blocksize)
            bytes_written = 0
            try:
                bytes_written = self.buffer.stream_write(nbytes, driver)
            except "CRC_ERROR":
                Trace.alarm(e_errors.ERROR, "CRC error in write client",
                            {'outfile':self.current_work_ticket['outfile'],
                             'infile':self.current_work_ticket['infile'],
                             'location_cookie':self.current_work_ticket['fc']['location_cookie'],
                             'external_label':self.current_work_ticket['vc']['external_label']})
                self.transfer_failed(e_errors.CRC_ERROR, None)
                failed = 1
                break
            except:
                exc, detail, tb = sys.exc_info()
                #Trace.handle_error(exc, detail, tb)
                #if self.state is not DRAINING: self.state = HAVE_BOUND
                # if state is DRAINING transfer_failed will set it to OFFLINE
                self.transfer_failed(e_errors.ENCP_GONE, detail)
                failed = 1
                break
            if bytes_written < 0:
                #if self.state is not DRAINING: self.state = HAVE_BOUND
                # if state is DRAINING transfer_failed will set it to OFFLINE
                self.transfer_failed(e_errors.ENCP_GONE, "write returns %s"%(bytes_written,))
                failed = 1
                break
            if bytes_written != nbytes:
                pass #this is not unexpected, since we send with MSG_DONTWAIT
            self.bytes_written = self.bytes_written + bytes_written

            if not self.buffer.full():
                self.buffer.read_ok.set()

            #If it is time to do so, send the notify message.
            if is_threshold_passed(self.bytes_written, bytes_notified,
                                   self.bytes_to_write):
                bytes_notified = self.bytes_written
                #negative byte-count to indicate direction
                Trace.notify("transfer %s %s %s network %s %.3f" %
                             (self.shortname, -self.bytes_written,
                              self.bytes_to_write, self.buffer.nbytes(),
                              time.time()))

        if self.tr_failed:
            return
        
        Trace.trace(8, "write_client exiting: wrote %s/%s bytes" % (self.bytes_written, self.bytes_to_write))
        if failed: return
  
        if self.bytes_written == self.bytes_to_write:
            # check crc
            if do_crc:
                Trace.trace(22,"write_client: calculated CRC %s File DB CRC %s"%
                            (self.buffer.complete_crc, self.file_info['complete_crc']))
                if self.buffer.complete_crc != self.file_info['complete_crc']:
                    Trace.alarm(e_errors.ERROR, "CRC error in write client",
                                {'outfile':self.current_work_ticket['outfile'],
                                 'infile':self.current_work_ticket['infile'],
                                 'location_cookie':self.current_work_ticket['fc']['location_cookie'],
                                 'external_label':self.current_work_ticket['vc']['external_label']})
                    self.transfer_failed(e_errors.CRC_ERROR, None)
                    return
                
            self.transfer_completed()

        
    def create_volume_name(self, ip_map, volume_family):
        return string.join((ip_map,volume_family,'%s'%(long(time.time()*1000),)),':')

    def no_work(self, ticket):
        x = ticket # to trick pychecker
        if self.state is HAVE_BOUND:
            self.dismount_volume()
        
    def setup_transfer(self, ticket, mode):
        self.lock_state()
        self.save_state = self.state

        self.unique_id = ticket['unique_id']
        try:
            self.lm_address = ticket['lm']['address']
        except KeyError:
            self.lm_address = None
        Trace.trace(10, "setup transfer")
        self.tr_failed = 0
        self.setup_mode = mode
        ## pprint.pprint(ticket)
        if self.save_state not in (IDLE, HAVE_BOUND):
            Trace.log(e_errors.ERROR, "Not idle %s" %(state_name(self.state),))
            self.return_work_to_lm(ticket)
            self.unlock_state()
            return 0

        self.state = SETUP
        # the following settings are needed by LM to update it's queues
        self.tmp_vol = ticket['fc'].get('external_label', None)
        self.tmp_vf = ticket['vc'].get('volume_family', None)
        self.need_lm_update = (1, self.state, 1, None)
        #prevent a delayed dismount from kicking in right now
        if self.dismount_time:
            self.dismount_time = None
        self.unlock_state()
        
        ticket['mover']={}
        ticket['mover'].update(self.config)
        ticket['mover']['device'] = "%s:%s" % (self.config['host'], self.config['device'])
        if ticket['fc']['external_label'] == None:
            del(ticket['fc']['external_label'])
        self.current_work_ticket = ticket
        self.run_in_thread('client_connect_thread', self.connect_client)

    def finish_transfer_setup(self):
        Trace.trace(10, "client connect returned: %s %s" % (self.control_socket, self.client_socket))
        ticket = self.current_work_ticket
        if not self.client_socket:
            Trace.trace(20, "finish_transfer_setup: connection to client failed")
            self.state = self.save_state
            ## Connecting to client failed
            if self.state is HAVE_BOUND:
                self.dismount_time = time.time() + self.default_dismount_delay
            self.need_lm_update = (1, self.state, 1, None)
            #self.update_lm(reset_timer=1)
            return 0

        self.t0 = time.time()

        ##all groveling around in the ticket should be done here
        fc = ticket['fc']
        vc = ticket['vc']
        self.file_info.update(fc)
        self.vol_info.update(vc)
        self.volume_family=vc['volume_family']
        delay = 0
        if ticket['work'] == 'read_from_hsm':
            sanity_cookie = ticket['fc']['sanity_cookie']
        else:
            sanity_cookie = None
        
        if ticket.has_key('client_crc'):
            client_crc_on = ticket['client_crc']
        client_crc_on = 1 # assume that client does CRC

        # if client_crc is ON:
        #    write requests -- calculate CRC when writing from memory to tape
        #    read requetsts -- calculate CRC when reading from tape to memory
        # if client_crc is OFF:
        #    write requests -- calculate CRC when writing to memory
        #    read requetsts -- calculate CRC when reading memory

        self.reset(sanity_cookie, client_crc_on)
        # restore self.current_work_ticket after it gets cleaned in the reset()
        self.current_work_ticket = ticket
        self.current_volume =  self.file_info.get('external_label', None)
        if self.current_work_ticket['encp'].has_key('delayed_dismount'):
            if ((type(self.current_work_ticket['encp']['delayed_dismount']) is type(0)) or
                (type(self.current_work_ticket['encp']['delayed_dismount']) is type(0.))):
                delay = 60 * self.current_work_ticket['encp']['delayed_dismount']
            else:
                delay = self.default_dismount_delay
        if delay > 0:
            self.delay = max(delay, self.default_dismount_delay)
        elif delay < 0:
            self.delay = 31536000  # 1 year
        else:
            self.delay = 0   
        self.delay = min(self.delay, self.max_dismount_delay)
        self.fcc = file_clerk_client.FileClient(self.csc, bfid=0,
                                                server_address=fc['address'])
        self.vcc = volume_clerk_client.VolumeClerkClient(self.csc,
                                                         server_address=vc['address'])
        self.unique_id = self.current_work_ticket['unique_id']
        volume_label = self.current_volume
        self.current_location = 0L
        if volume_label:
            self.vol_info.update(self.vcc.inquire_vol(volume_label))
            self.current_location = cookie_to_long(self.vol_info['eod_cookie'])
        else:
            Trace.log(e_errors.INFO, "setup_transfer: volume label=%s" % (volume_label,))
            if  self.setup_mode == WRITE:
                self.current_volume = self.create_volume_name(self.ip_map,
                                                              self.volume_family)
                self.vol_info['external_label'] = self.current_volume
                self.vol_info['status'] = (e_errors.OK, None)
                Trace.trace(e_errors.INFO, "new volume label created %s"%(self.current_volume,))
        if self.vol_info['status'][0] != e_errors.OK:
            msg =  ({READ: e_errors.READ_NOTAPE, WRITE: e_errors.WRITE_NOTAPE}.get(
                self.setup_mode, e_errors.EPROTO), self.vol_info['status'][1])
            Trace.log(e_errors.ERROR, "Volume clerk reply %s" % (msg,))
            self.send_client_done(self.current_work_ticket, msg[0], msg[1])
            self.state = self.save_state
            return 0

        self.buffer.set_blocksize(self.vol_info.get('blocksize',self.default_block_size))
        self.wrapper = None
        self.wrapper_type = volume_family.extract_wrapper(self.volume_family)

        try:
            self.wrapper = __import__(self.wrapper_type + '_wrapper')
        except:
            exc, msg, tb = sys.exc_info()
            Trace.log(e_errors.ERROR, "error importing wrapper: %s %s" %(exc,msg))

        if not self.wrapper:
            msg = e_errors.EPROTO, "Illegal wrapper type %s" % (self.wrapper_type)
            Trace.log(e_errors.ERROR,  "%s" %(msg,))
            self.send_client_done(self.current_work_ticket, msg[0], msg[1])
            self.state = self.save_state
            return 0
        
        self.buffer.set_wrapper(self.wrapper)
        client_filename = self.current_work_ticket['wrapper'].get('fullname','?')
        pnfs_filename = self.current_work_ticket['wrapper'].get('pnfsFilename', '?')

        self.mode = self.setup_mode
        self.bytes_to_transfer = long(fc['size'])
        self.bytes_to_write = self.bytes_to_transfer
        self.bytes_to_read = self.bytes_to_transfer
        self.expected_transfer_time = self.bytes_to_write*1.0 / self.max_rate
        self.real_transfer_time  = 0.
        self.transfer_deficiency = 1.

        ##NB: encp v2_5 supplies this information for writes but not reads. Somebody fix this!
        try:
            client_hostname = self.current_work_ticket['wrapper']['machine'][1]
        except KeyError:
            client_hostname = ''
        self.client_hostname = client_hostname
        if client_hostname:
            client_filename = client_hostname + ":" + client_filename
                                
        if self.mode == READ:
            self.file = fc['location_cookie']
            self.files = (pnfs_filename, client_filename)
            self.buffer.header_size = None
        elif self.mode == WRITE:
            self.files = (client_filename, pnfs_filename)
            t = "%s"%(int(time.time()),)
            import statvfs
            max_fn_len = os.statvfs(self.config['device'])[statvfs.F_NAMEMAX]
            dir,filename = os.path.split(pnfs_filename)
            if len(filename) > max_fn_len-len(t)-1: #truncate filename
                filename = filename[0:max_fn_len-len(t)-1]
            fullname = os.path.join(dir,filename)
            f = string.join((fullname,t),':')
            self.file = string.join((self.config['device'],self.config['ip_map'],f),'/')  
            if self.wrapper:
                self.header, self.trailer = self.wrapper.headers(self.current_work_ticket['wrapper'])
            else:
                self.header = ''
                self.trailer = ''
            self.buffer.header_size = len(self.header)
            self.buffer.trailer_size = len(self.trailer)
            self.bytes_to_write = self.bytes_to_write + len(self.header) + len(self.trailer)
            self.buffer.file_size = self.bytes_to_write
            self.buffer.trailer_pnt = self.buffer.file_size - len(self.trailer)

        Trace.trace(29,"FILE NAME %s"%(self.file,))
        self.position_media(self.file)
        
    def position_media(self, file):
        x = file # to trick pychecker
        have_tape = 0
        err = None
        Trace.trace(10, "position media")
        have_tape = self.tape_driver.open(self.file, self.mode, retry_count=30)
        if have_tape == 1:
            err = None
        else:
            self.transfer_failed(e_errors.MOUNTFAILED, 'mount failure: %s' % (err,), error_source=ROBOT)
            self.idle()
            return

        self.start_transfer()
        return 1
            
    def transfer_failed(self, exc=None, msg=None, error_source=None, dismount_allowed=1):
        self.timer('transfer_time')
        ticket = self.current_work_ticket
        if not ticket.has_key('times'):
            ticket['times']={}
        if self.mode == READ:
            t = self.tape_driver.tape_transfer_time()
        else:
            t = self.media_transfer_time
        if t == 0.:
            t = ticket['times']['transfer_time']
        ticket['times']['drive_transfer_time'] = t
        self.log_state()
        self.tape_driver.close()
        if self.mode == WRITE:
            try:
                os.unlink(self.file)
            except:
                pass
        if self.tr_failed:
            return          ## this function has been alredy called in the other thread
        self.tr_failed = 1
        broken = ""
        Trace.log(e_errors.ERROR, "transfer failed %s %s volume=%s location=%s" % (
            exc, msg, self.current_volume, 0))
        Trace.notify("disconnect %s %s" % (self.shortname, self.client_ip))
        
        ### XXX translate this to an e_errors code?
        self.last_error = str(exc), str(msg)
        
        if self.state == ERROR:
            Trace.log(e_errors.ERROR, "Mover already in ERROR state %s, state=ERROR" % (msg,))
            self.tr_failed = 0
            return

        if exc != e_errors.ENCP_GONE:
            self.consecutive_failures = self.consecutive_failures + 1
            if self.consecutive_failures >= self.max_consecutive_failures:
                broken =  "max_consecutive_failures (%d) reached" %(self.max_consecutive_failures)
            now = time.time()
            self.error_times.append(now)
            while self.error_times and now - self.error_times[0] > self.failure_interval:
                self.error_times.pop(0)
            if len(self.error_times) >= self.max_failures:
                broken =  "max_failures (%d) per failure_interval (%d) reached" % (self.max_failures,
                                                                                     self.failure_interval)
            self.transfers_failed = self.transfers_failed + 1

        self.send_client_done(self.current_work_ticket, str(exc), str(msg))
        self.net_driver.close()
        self.need_lm_update = (1, ERROR, 1, error_source)

        if broken:
            self.broken(broken)
            self.tr_failed = 0
            return
        
        save_state = self.state
        # get the current thread
        cur_thread = threading.currentThread()
        if cur_thread:
            cur_thread_name = cur_thread.getName()
        else:
            cur_thread_name = None

        dismount_allowed = 0
        Trace.trace(26,"current thread %s"%(cur_thread_name,))
        if cur_thread_name:
            if cur_thread_name == 'tape_thread':
                dismount_allowed = 1

            elif cur_thread_name == 'net_thread':
                # check if tape_thread is active before allowing dismount
                Trace.trace(26,"checking thread %s"%('tape_thread',))
                thread = getattr(self, 'tape_thread', None)
                for wait in range(60):
                    if thread and thread.isAlive():
                        Trace.trace(27, "thread %s is already running, waiting %s" % ('tape_thread', wait))
                        time.sleep(1)
                    else:
                        dismount_allowed = 1
                        break
                else:
                    dismount_allowed = 1
            else:
                # Main thread ?
                dismount_allowed = 1
                
        if dismount_allowed:
            self.dismount_volume()

        if save_state == DRAINING:
            self.dismount_volume()
            #self.state = OFFLINE
            self.offline()
        else:
            self.idle()
            self.current_volume = None
        
        self.tr_failed = 0   
        #self.delayed_update_lm() Why do we need delayed udpate AM 01/29/01
        #self.update_lm()
        #self.need_lm_update = (1, 0, None)    
        
    def transfer_completed(self):
        self.consecutive_failures = 0
        self.timer('transfer_time')
        ticket = self.current_work_ticket
        if not ticket.has_key('times'):
            ticket['times']={}
        if self.mode == READ:
            t = self.tape_driver.tape_transfer_time()
        else:
            t = self.media_transfer_time
        if t == 0.:
            t = ticket['times']['transfer_time']
        ticket['times']['drive_transfer_time'] = t
        Trace.log(e_errors.INFO, "transfer complete volume=%s location=%s"%(
            self.current_volume, 0))
        Trace.notify("disconnect %s %s" % (self.shortname, self.client_ip))
        if self.mode == WRITE:
            self.vcc.update_counts(self.current_volume, wr_access=1)
        else:
            self.vcc.update_counts(self.current_volume, rd_access=1)
        self.transfers_completed = self.transfers_completed + 1
        self.net_driver.close()
        self.tape_driver.close()
        now = time.time()
        self.dismount_time = now + self.delay
        self.send_client_done(self.current_work_ticket, e_errors.OK)
        if hasattr(self,'too_long_in_state_sent'):
            del(self.too_long_in_state_sent)

        ######### AM 01/30/01
        ### do not update lm as in a child thread
        # self.update_lm(reset_timer=1)
        ##########################
        
        if self.state == DRAINING:
            self.dismount_volume()
            #self.state = OFFLINE
            self.offline()
        else:
            self.state = HAVE_BOUND
        self.need_lm_update = (1, None, 1, None)
        self.log_state()

    def update_after_writing(self):
        sanity_cookie = (self.buffer.sanity_bytes,self.buffer.sanity_crc)
        complete_crc = self.buffer.complete_crc
        fc_ticket = {  'location_cookie': self.file,
                       'size': self.bytes_to_transfer,
                       'sanity_cookie': sanity_cookie,
                       'external_label': self.current_volume,
                       'complete_crc': complete_crc}
        fcc_reply = self.fcc.new_bit_file({'work':"new_bit_file",
                                            'fc'  : fc_ticket
                                            })
        if fcc_reply['status'][0] != e_errors.OK:
            Trace.log(e_errors.ERROR,
                       "cannot assign new bfid")
            self.transfer_failed(e_errors.ERROR,"Cannot assign new bit file ID")
            #XXX exception?
            return 0
        ## HACK: restore crc's before replying to caller
        fc_ticket = fcc_reply['fc']
        fc_ticket['sanity_cookie'] = sanity_cookie
        fc_ticket['complete_crc'] = complete_crc 
        bfid = fc_ticket['bfid']
        self.current_work_ticket['fc'] = fc_ticket
        Trace.trace(15,"inquire volume %s"%(self.current_volume,))
        v = self.vcc.inquire_vol(self.current_volume)
        import statvfs
        stats = os.statvfs(self.config['device'])
        r2 = long(stats[statvfs.F_BAVAIL])*stats[statvfs.F_BSIZE]
        if v['status'][0] != e_errors.OK: # volume does not exist, create it!
            r = self.vcc.add(self.vol_info['library'],
                             volume_family.extract_file_family(self.vol_info['volume_family']),
                             volume_family.extract_storage_group(self.vol_info['volume_family']),
                             'disk',
                             self.current_volume,
                             r2,
                             eod_cookie  = '0000_000000000_0000001',
                             wrapper='null',
                             blocksize = self.buffer.blocksize)
            Trace.log(e_errors.INFO,"Add volume returned %s"%(r,))
            if r['status'][0] != e_errors.OK:
                Trace.log(e_errors.ERROR,
                          "cannot assign new bfid")
                self.transfer_failed(e_errors.ERROR,"Cannot add new volume")
                return 0
            self.vol_info['remaining_bytes'] = r2
                
        r0 = self.vol_info['remaining_bytes']  #value prior to this write
        r1 = r0 - self.bytes_written           #value derived from simple subtraction
        remaining = remaining = min(r1, r2)
        self.vol_info['remaining_bytes']=remaining
        reply = self.vcc.set_remaining_bytes(self.current_volume,
                                             remaining,
                                             loc_to_cookie(self.current_location+1), bfid)
        if reply['status'][0] != e_errors.OK:
            self.transfer_failed(reply['status'][0], reply['status'][1], error_source=TAPE)
            return 0
        self.vol_info.update(reply)
        
        return 1

    
    def format_lm_ticket(self, state=None, error_info=None, returned_work=None, error_source=None):
        status = e_errors.OK, None
        work = None
        if state is None:
            state = self.state
        Trace.trace(20,"format_lm_ticket: state %s"%(state,))
        volume_label = self.current_volume
        if self.current_volume:
            volume_label = self.current_volume
            volume_family = self.volume_family
        else:
            volume_label = self.last_volume
            volume_family = self.last_volume_family
        
        if state is IDLE:
            work = "mover_idle"
        elif state in (HAVE_BOUND,):
            work = "mover_bound_volume"
        elif state in (ACTIVE, SETUP, SEEK, DRAINING, CLEANING, MOUNT_WAIT, DISMOUNT_WAIT, FINISH_WRITE):
            work = "mover_busy"
            if state == SETUP:
                try:
                    # self.tmp_vol and self.tmp_vf may be None, None
                    # because of the resynchronization of threads
                    # check again and make the best guess
                    if self.tmp_vol:
                        volume_label = self.tmp_vol
                    if self.tmp_vf:
                        volume_family = self.tmp_vf
                except AttributeError:
                    volume_label = None
                    
            if error_info:
                status = error_info
        elif state in (ERROR, OFFLINE):
            work = "mover_error"  ## XXX If I'm offline should I send mover_error? I don't think so....
            if error_info is None:
                status = self.last_error
            else:
                status = error_info
        if work is None:
            Trace.log(e_errors.ERROR, "state: %s work: %s" % (state_name(state),work))

        if not status:
            status = e_errors.OK, None
            
        if type(status) != type(()) or len(status)!=2:
            Trace.log(e_errors.ERROR, "status should be 2-tuple, is %s" % (status,))
            status = (status, None)

        now = time.time()
        if self.unique_id and state in (IDLE, HAVE_BOUND):
            ## If we've been idle for more than 15 minutes, force the LM to clear
            ## any entry for this mover in the work_at_movers.  Yes, this is a
            ## kludge, but it keeps the system from getting completely hung up
            ## if the LM doesn't realize we've finished a transfer.
            if now - self.state_change_time > 900:
                self.unique_id = None

        volume_status = (['none', 'none'], ['none','none'])
        if self.transfer_deficiency < 1.:
            self.transfer_deficiency = 1.
        ticket =  {
            "mover":  self.name,
            "address": self.address,
            "external_label":  volume_label,
            "current_location": loc_to_cookie(0),
            'mover_type': self.mover_type,
            'ip_map' : self.ip_map,
            "read_only" : 0, ###XXX todo: multiple drives on one scsi bus, write locking
            "returned_work": returned_work,
            "state": state_name(self.state),
            "status": status,
            "volume_family": volume_family,
            "volume_status": volume_status,
            "operation": mode_name(self.mode),
            "error_source": error_source,
            "unique_id": self.unique_id,
            "work": work,
            "transfer_deficiency": int(self.transfer_deficiency),
            'time_in_state': now - self.state_change_time,

            }
        return ticket

    def dismount_volume(self, after_function=None):
        broken = ""
        self.dismount_time = None
        self.state = DISMOUNT_WAIT
        self.tape_driver.close()
        self.last_volume = self.current_volume
        self.last_volume_family = self.volume_family

        Trace.notify("unload %s %s" % (self.shortname, self.current_volume))
        if self.draining:
            self.offline()
        elif after_function:
            Trace.trace(20,"after function %s" % (after_function,))
            after_function()
        else:
            self.idle()
        self.current_volume = None
        return
    
    def status(self, ticket):
        x =ticket # to trick pychecker
        now = time.time()
        status_info = (e_errors.OK, None)
        if self.state == ERROR:
            status_info = self.last_error
        try:
            if self.buffer:
                bytes_buffered = self.buffer.nbytes()
                buffer_min_bytes = self.buffer.min_bytes
                buffer_max_bytes = self.buffer.max_bytes
            else:
                bytes_buffered = 0
                buffer_min_bytes = 0
                buffer_max_bytes = 0
        except AttributeError:
            # try it again
            time.sleep(3)
            if self.buffer:
                bytes_buffered = self.buffer.nbytes()
                buffer_min_bytes = self.buffer.min_bytes
                buffer_max_bytes = self.buffer.max_bytes
            else:
                bytes_buffered = 0
                buffer_min_bytes = 0
                buffer_max_bytes = 0

        tick = { 'status'       : status_info,
                 'drive_sn'     : self.config['serial_num'],
                 'drive_vendor' : self.config['vendor_id'],
                 'drive_id'     : self.config['product_id'],
                 #
                 'state'        : state_name(self.state),
                 'transfers_completed'     : self.transfers_completed,
                 'transfers_failed': self.transfers_failed,
                 'bytes_read'     : self.bytes_read,
                 'bytes_written'     : self.bytes_written,
                 'bytes_buffered' : bytes_buffered,
                 'successful_writes': self.files_written_cnt,
                 # from "work ticket"
                 'bytes_to_transfer': self.bytes_to_transfer,
                 'files'        : self.files,
                 'last_error': self.last_error,
                 'mode'         : mode_name(self.mode),
                 'current_volume': self.current_volume,
                 'current_location': 0,
                 'last_volume' : self.last_volume,
                 'last_location': 0,
                 'time_stamp'   : now,
                 'time_in_state': now - self.state_change_time,
                 'buffer_min': buffer_min_bytes,
                 'buffer_max': buffer_max_bytes,
                 'rate of network': self.net_driver.rates()[0],
                 'rate of tape': self.tape_driver.rates()[0],
                 'default_dismount_delay': self.default_dismount_delay,
                 'max_dismount_delay': self.max_dismount_delay,
                 'client': self.client_ip,
                 }
        if self.state is HAVE_BOUND and self.dismount_time and self.dismount_time>now:
            tick['will dismount'] = 'in %.1f seconds' % (self.dismount_time - now)
            
        self.reply_to_caller(tick)
        return

if __name__ == '__main__':            

    if len(sys.argv)<2:
        sys.argv=["python", "null.mover"] #REMOVE cgw
    # get an interface, and parse the user input

    intf = MoverInterface()
    csc  = configuration_client.ConfigurationClient((intf.config_host,
                                                     intf.config_port) )
    keys = csc.get(intf.name)
    try:
        mc_type = keys['type']
    except KeyError:
        mc_type = 'Mover'
    except:
        exc,msg,tb=sys.exc_info()
        Trace.log(e_errors.ERROR, "Mover Error %s %s"%(exc,msg))
        sys.exit(1)
    
    import __main__
    constructor=getattr(__main__, mc_type)
    mover = constructor((intf.config_host, intf.config_port), intf.name)

    mover.handle_generic_commands(intf)
    mover.start()
    
    while 1:
        try:
            mover.serve_forever()
        except SystemExit:
            Trace.log(e_errors.INFO, "mover %s exiting." % (mover.name,))
            os._exit(0)
            break
        except:
            try:
                exc, msg, tb = sys.exc_info()
                full_tb = traceback.format_exception(exc,msg,tb)
                for l in full_tb:
                    Trace.log(e_errors.ERROR, l[:-1], {}, "TRACEBACK")
                Trace.log(e_errors.INFO, "restarting after exception")
                mover.start()
            except:
                pass

    Trace.log(e_errors.INFO, 'ERROR returned from serve_forever')
