#!/usr/bin/env python

"""
Enstore mover.

Delivers files from client to tape and from tape to client.
Data delivery request is picked up from the library manager the mover is assigned to.
The mover can be assigned to more than one library manager.
Mover sends tape mount / dismount requests to the media changer specifying the address (name) of
the tape and the address of the tape drive the mover is assigned to.
Data delivery reliability is provided by end-to-end checksumming.
Mover also randomly checks written files by reading them back from tape and caclulating checksum.
Volume assert functionality serves for checking write tab consistency as well as data integrity checks
by reading files from tape and calculating checksum.
"""

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
import fcntl
import random
import subprocess
import copy
import platform
import types

# enstore modules

import configuration_client
import setpath
import generic_server
import monitored_server
import inquisitor_client
#import enstore_functions
import enstore_functions2
import enstore_functions3
import enstore_constants
import option
import dispatching_worker
import volume_clerk_client
import volume_family
import file_clerk_client
import info_client
import media_changer_client
import callback
import checksum
import e_errors
import udp_client
import socket_ext
import hostaddr
import string_driver
import disk_driver
import net_driver
import null_driver
import accounting_client
import drivestat_client
import Trace
import generic_driver
import event_relay_messages
import file_cache_status
import scsi_mode_select
import set_cache_status

DEBUG_LOG=11

WRAPPER_ERROR = 'WRAPPER_ERROR'
BUF_SIZE_CH_ERR = 'Buffer error: changing blocksize of nonempty buffer'
class MoverError(exceptions.Exception):
    """
    Needed to raise mover specific exceptions.

    """
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
TAPE, ROBOT, NETWORK, DRIVE, USER, MOVER, UNKNOWN = ['TAPE', 'ROBOT', 'NETWORK', 'DRIVE', 'USER','MOVER', 'UNKNOWN']

MEDIA_VERIFY_FAILED = "media verify failed"
def mode_name(mode):
    if mode is None:
        return None
    else:
        return ['READ','WRITE','ASSERT'][mode]

KB=enstore_constants.KB
MB=enstore_constants.MB
GB=enstore_constants.GB

SANITY_SIZE = 65536
TRANSFER_THRESHOLD = 2*MB
"""Used in the threshold calculation"""

MAX_BUFFER = 2500*MB
"""Maximal allowed buffer size
for 32 bit architecture maximal process size is 4 GB,
so maximal buffer size can not be bigger than this value.
Practice shows that the safe size should be 2.5 GB.
"""

#Return total system memory or None if unable to determine.
def total_memory():
    total_mem_in_pages = os.sysconf(os.sysconf_names['SC_PHYS_PAGES'])
    if total_mem_in_pages == -1:
        return None

    page_size = os.sysconf(os.sysconf_names['SC_PAGE_SIZE'])
    if page_size == -1:
        return None

    return long(total_mem_in_pages) * long(page_size)


# set MAX_BUFFER
def set_max_buffer_limit():
    global MAX_BUFFER
    mem_total = total_memory()
    if mem_total:
        # give 1GB for kernel and code
        MAX_BUFFER = mem_total - GB
    if platform.architecture()[0].find("32") != -1:
        # 32 - bit python
        # maximal allowed buffer size
        # for 32 bit architecture maximal process size is 4 GB
        # so maximal buffer size can not be bigger than this value
        # practice shows that the safe size should be 2.5 GB
        if MAX_BUFFER > 2500*MB:
            MAX_BUFFER = 2500*MB

set_max_buffer_limit() # run it here

def get_transfer_notify_threshold(bytes_to_transfer):
    if TRANSFER_THRESHOLD * 5 > bytes_to_transfer:
        threshold = bytes_to_transfer / 5
    elif TRANSFER_THRESHOLD * 100 < bytes_to_transfer:
        threshold = bytes_to_transfer / 100
    else:
        threshold = TRANSFER_THRESHOLD

    return threshold

def is_threshold_passed(bytes_transfered, bytes_notified, bytes_to_transfer,
                        last_notify_time):
    #If transfer is complete, indicate notify message to be sent.
    if bytes_transfered == bytes_to_transfer:
        return 1
    now = time.time()
    #If threshold passed, indicate notify message to be sent.
    if (now - last_notify_time) > enstore_constants.MIN_TRANSFER_TIME and \
        bytes_transfered - bytes_notified > \
                             get_transfer_notify_threshold(bytes_to_transfer):
        return 1
    #If it has almost been 5 seconds we should send an update.
    if (now - last_notify_time) > (enstore_constants.MAX_TRANSFER_TIME - 0.25):
        return 1

    return 0

class Buffer:
    """
    Provide memory buffer for incoming and outgoing data.

    """
    def __init__(self, blocksize, min_bytes = 0, max_bytes = 1*MB, crc_seed=1L):
        """

        :type blocksize: :obj:`int`
        :arg blocksize: buffer block size. Read / write requests are done by blocks

        :type min_bytes: :obj:`int`
        :arg min_bytes: lower buffer watermark

        :type max_bytes: :obj:`int`
        :arg max_bytes: upper buffer watermark

        :type crc_seed: :obj:`long`
        :arg crc_seed: seed for adler32 checksum calculation
        """

        self.blocksize = blocksize
        self.min_bytes = min_bytes
        self.max_bytes = max_bytes
        self.crc_seed = crc_seed
        self.complete_crc = 0L
        self.sanity_crc = 0L
        if self.crc_seed == 1L:
           self.complete_crc = self.crc_seed
           self.sanity_crc = self.crc_seed
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
        self.client_crc_on = 0
        self.read_stats = [0,0,0,0,0] # read block timing stats
        self.write_stats = [0,0,0,0,0] # read block timing stats

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
        if self.crc_seed == 1L:
           self.complete_crc = self.crc_seed
           self.sanity_crc = self.crc_seed
        else:
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
        self.read_stats = [0,0,0,0,0] # read block timing stats
        self.write_stats = [0,0,0,0,0] # read block timing stats

    def clear(self):
        Trace.trace(10,"clear buffer start")
        self._lock.acquire()
        l = len(self._buf)
        for i in range(l):
            d = self._buf.pop(0)
            del(d)
        l = len(self._freelist)
        for i in range(l):
            d = self._freelist.pop(0)
            del(d)
        Trace.trace(10,"clear buffer finish")
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
            raise MoverError(BUF_SIZE_CH_ERR)
        self._lock.acquire()
        self._freelist = []
        self.blocksize = blocksize
        self._lock.release()

    def push(self, data):
        """
        Push data into the buffer.

        :type data: :obj:`str`
        :arg data: portion of the transferring data

        """
        self._lock.acquire()
        self._buf.append(data)
        self._buf_bytes = self._buf_bytes + len(data)
        self._lock.release()

    def pull(self):
        """
        Pull data out of the buffer.

        """
        self._lock.acquire()
        data = self._buf.pop(0)
        self._buf_bytes = self._buf_bytes - len(data)
        self._lock.release()
        return data

    def set_crc_seed(self, crc_seed):
        self.crc_seed = crc_seed
        if self.crc_seed == 1L:
           self.complete_crc = self.crc_seed
           self.sanity_crc = self.crc_seed
        else:
            self.complete_crc = 0L
            self.sanity_crc = 0L

    def nonzero(self):
        return self.nbytes() > 0

    def __repr__(self):
        return "Buffer %s  %s  %s" % (self.min_bytes, self._buf_bytes, self.max_bytes)

    def dump(self, f):
        """
        Dump buffer content into file for debugging purposes.

        :type f: :obj:`file`
        :arg f: file to dump into.
        """

        for name, value in self.__class__.__dict__.items( ) + self.__dict__.items( ):
            v = value
            try:
                l = len(value)
            except (TypeError, AttributeError):
                l = None
            if l:
                if l < 100:
                    v = value
                else:
                    v = ">100"

            f.write("%s = %s, len = %s\n"%(name, v, l))


    def block_read(self, nbytes, driver, fill_buffer=1):
        """
        Read data using specified driver.

        :type nbytes: :obj:`int`
        :arg nbytes: bytes to read.
        :type driver: :class:`disk_driver.DiskDriver`, :class:`ftt_driver.FTTDriver`, :class:`null_driver.NullDriver`
        :arg driver: disk, tape or null driver
        :type fill_buffer: :obj:`int`
        :arg fill_buffer: fill buffer (if != 0). (0 can be used for testing or null driver)
        :rtype: :obj:`int` number of bytes read
        """

        if self.client_crc_on:
            # calculate checksum when reading from
            # tape (see comment in setup_transfer)
            do_crc = 1
        else:
            do_crc = 0
        data = None
        partial = None
        t0 = time.time()
        space = self._getspace()
        t1 = time.time()
        #Trace.trace(222,"block_read: bytes_to_read: %s"%(nbytes,)) # COMMENT THIS
        #bytes_read = driver.read(space, 0, nbytes)
        bytes_read = driver.read(space, 0, self.blocksize)
        t2 = time.time()
        #Trace.trace(222,"block_read: bytes_read: %s in %s"%(bytes_read,t2-t1)) # COMMENT THIS
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
                    raise MoverError(WRAPPER_ERROR)
                data_ptr = header_size
                bytes_for_cs = min(bytes_read - header_size, self.bytes_for_crc)
            self.first_block = 0
        if do_crc:
            crc_error = 0
            try:
                #Trace.trace(22,"block_read: data_ptr %s, bytes_for_cs %s" % (data_ptr, bytes_for_cs)) #COMMENT THIS

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
                    crc_error = 0
                    if self.sanity_cookie and self.sanity_crc != self.sanity_cookie[1]:
                        if self.sanity_cookie != (None, None):
                            # (None, None) is a special case to fix bfid db
                            crc_error = 1
                            if self.crc_seed == 0:
                                # try 1 based crc
                                crc_1_seeded = checksum.convert_0_adler32_to_1_adler32(self.sanity_crc,
                                                                                       self.sanity_cookie[0])
                                if crc_1_seeded == self.sanity_cookie[1]:
                                    self.sanity_crc = crc_1_seeded
                                    crc_error = 0

                data_ptr = data_ptr + bytes_for_cs
            except:
                Trace.log(e_errors.ERROR,"block_read: CRC_ERROR")
                Trace.handle_error()
                raise MoverError(e_errors.CRC_ERROR)
            if crc_error:
                Trace.log(e_errors.ERROR, "CRC Error: CRC sanity cookie %s, actual (%s,%s)" %
                          (self.sanity_cookie, self.sanity_bytes, self.sanity_crc))
                Trace.log(e_errors.ERROR,"block_read: CRC_ERROR")
                raise MoverError(e_errors.CRC_ERROR)

        t3 = time.time()
        if data and fill_buffer:
            self.push(data)
            if partial:
                self._freespace(space)
        t4 = time.time()
        self.read_stats[0] = self.read_stats[0] + t1-t0   # total time in get_space
        self.read_stats[1] = self.read_stats[1] + t2-t1   # total time in read
        self.read_stats[2] = self.read_stats[2] + t3-t2   # total time in check CRC
        self.read_stats[3] = self.read_stats[3] + t4-t3   # total time in push
        self.read_stats[4] = self.read_stats[4] + t4-t0   # total time in block_read

        return bytes_read

    def block_write(self, nbytes, driver):
        """
        Write data using specified driver.

        :type nbytes: :obj:`int`
        :arg nbytes: bytes to read
        :type driver: :class:`disk_driver.DiskDriver`, :class:`ftt_driver.FTTDriver`, :class:`null_driver.NullDriver`
        :arg driver: disk, tape or null driver
        :rtype: :obj:`int` number of written bytes
        """
        #Trace.trace(22,"block_write: bytes %s"%(nbytes,))

        if self.client_crc_on:
            # calculate checksum when reading from
            # tape (see comment in setup_transfer)
            do_crc = 1
        else:
            do_crc = 0
        #Trace.trace(22,"block_write: header size %s"%(self.header_size,))
        #Trace.trace(22,"block_write: do_crc %s"%(do_crc,))
        t0 = t1 = t2 = t3 = t4 = time.time()
        data = self.pull()
        t1 = time.time()
        if len(data)!=nbytes:
            raise ValueError, "asked to write %s bytes, buffer has %s" % (nbytes, len(data))
        bytes_written = driver.write(data, 0, nbytes)
        t2 = time.time()
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
                        raise MoverError(WRAPPER_ERROR)
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

                        #Trace.trace(22,"complete crc %s"%(self.complete_crc,))
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
                        raise MoverError(e_errors.CRC_ERROR)
            t3 = time.time()
            self._freespace(data)
            t4 = time.time()
            self.bytes_written = self.bytes_written + bytes_written

        else: #XXX raise an exception?
            Trace.trace(22,"actually written %s" % (bytes_written,))
            self._freespace(data)
        self.write_stats[0] = self.write_stats[0] + t1-t0   # total time in pull
        self.write_stats[1] = self.write_stats[1] + t2-t1   # total time in write
        self.write_stats[2] = self.write_stats[2] + t3-t2   # total time in check CRC
        self.write_stats[3] = self.write_stats[3] + t4-t3   # total time in freespace
        self.write_stats[4] = self.write_stats[4] + t4-t0   # total time in block_write
        Trace.trace(122, "WS %s"%(self.write_stats,))
        return bytes_written


    def stream_read(self, nbytes, driver):
        """
        Read data from the stream using specified driver.

        :type nbytes: :obj:`int`
        :arg nbytes: bytes to read.
        :type driver: :class:`net_driver.NetDriver` or :class:`string_driver.StringDriver`
        :arg driver: network or string driver.
        :rtype: :obj:`int` number of bytes read
        """

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
        try:
            bytes_read = driver.read(self._reading_block, self._read_ptr, bytes_to_read)
        except MemoryError:
            message = "memory error calling driver.read bytes to read %s" \
                      % (bytes_to_read,)
            Trace.log(e_errors.ERROR, message)
            raise MemoryError(message)
        if do_crc:
            #Trace.trace(22,"nbytes %s, bytes_to_read %s, bytes_read %s" %
            #            (nbytes, bytes_to_read, bytes_read))
            try:
                self.complete_crc = checksum.adler32_o(self.complete_crc, self._reading_block,
                                                       self._read_ptr, bytes_read)
            except MemoryError:
                message = "memory error calling adler32_o"
                Trace.log(e_errors.ERROR, message)
                raise MemoryError(message)

            if self.sanity_bytes < SANITY_SIZE:
                nbytes = min(SANITY_SIZE-self.sanity_bytes, bytes_read)
                try:
                    self.sanity_crc = checksum.adler32_o(self.sanity_crc, self._reading_block,
                                                         self._read_ptr, nbytes)
                except MemoryError:
                    message = "memory error calling adler32_o for sanity bytes"
                    Trace.log(e_errors.ERROR, message)
                    raise MemoryError(message)

                self.sanity_bytes = self.sanity_bytes + nbytes
        self._read_ptr = self._read_ptr + bytes_read
        if self._read_ptr == self.blocksize: #we filled up  a block
            try:
                self.push(self._reading_block)
            except MemoryError:
                message = "memory error calling push, buffered bytes %s" \
                          % (self._buf_bytes,)
                Trace.log(e_errors.ERROR, message)
                raise MemoryError(message)

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
        """
        Write data to the stream using specified driver.

        :type nbytes: :obj:`int`
        :arg nbytes: bytes to read.
        :type driver: :class:`net_driver.NetDriver` or :class:`string_driver.StringDriver`
        :arg driver: network or string driver.
        :rtype: :obj:`int` number of bytes written
        """
        if not self.client_crc_on:
            # calculate checksum when writing to
            # the network (see comment in setup_transfer)
            # CRC when sending to the network if client does not CRC
            do_crc = 1
        else:
            do_crc = 0
        Trace.trace(108, "stream_write do_crc %s bytes %s"%(do_crc,nbytes))
        if not self._writing_block:
            if self.empty():
                Trace.trace(10, "stream_write: buffer empty")
                return 0
            self._writing_block = self.pull()
            self._write_ptr = 0
        bytes_to_write = min(len(self._writing_block)-self._write_ptr, nbytes)
        Trace.trace(135, "bytes_to_write %s write_ptr %s"%(bytes_to_write,self._write_ptr))

        if driver:
            bytes_written = driver.write(self._writing_block, self._write_ptr, bytes_to_write)
            Trace.trace(124, "BYTES WRITTEN %s"%(bytes_written,))
            if bytes_written != bytes_to_write:
                msg="encp gone? bytes to write %s, bytes written %s"%(bytes_to_write, bytes_written)
                Trace.log(e_errors.ERROR, msg)
                raise MoverError(e_errors.ENCP_GONE)
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
                        crc_error = 1
                        if self.crc_seed == 0:
                            # try 1 based crc
                            crc_1_seeded = checksum.convert_0_adler32_to_1_adler32(self.sanity_crc,
                                                                                   self.sanity_cookie[0])
                            if crc_1_seeded == self.sanity_cookie[1]:
                                self.sanity_crc = crc_1_seeded
                                crc_error = 0
                        if crc_error:
                            Trace.log(e_errors.ERROR,
                                      "CRC Error: CRC sanity cookie %s, sanity CRC %s writing %s bytes. Written %s bytes" %
                                      (self.sanity_cookie[1],self.sanity_crc, bytes_to_write, nbytes))
                            raise MoverError(e_errors.CRC_ERROR)
        else:
            bytes_written = bytes_to_write #discarding header stuff
        self._write_ptr = self._write_ptr + bytes_written
        Trace.trace(135, "write_ptr %s len w_b %s"%(self._write_ptr,len(self._writing_block)))
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
        part, block, filename = string.split(cookie, '_')
    else:
        filename = cookie
    if filename[-1]=='L':
        filename = filename[:-1]
    return long(filename)

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
    """
    Mover (tape, or null) class.

    Accepts tickets from library manager and does specified in it work.

    Accepts enstore commands and launches specified in it metods.
    """

    def __init__(self, csc_address, name):
        """
        :type csc_address: :obj:`tuple`
        :arg csc_address: configuration server host name :obj:`str`, configuration server port :obj:`int`
        :type name: :obj:`str`
        :arg name: mover unique name

        """
        generic_server.GenericServer.__init__(self, csc_address, name,
                                              function = self.handle_er_msg)
        self.name = name # log name
        self.shortname = name
        self.unique_id = None #Unique id of last transfer, whether success or failure
        self.state_change_time = 0.0 # time when state has changed
        self.time_in_state = 0.0
        self.in_state_to_cnt = 0 # how many times timeot for being in the same state expired
        self.connect_to = 15  # timeout for control socket connection
        self.connect_retry = 4 # number of retries for control socket connection
        self._state_lock = threading.Lock()
        if self.shortname[-6:]=='.mover':
            self.shortname = name[:-6]
        self.draining = 0  # draining flag. Draining is not 0
        self.log_mover_state = None # allow logging additional mover information (configurable)
        self.override_ro_mount = None # if set override readonly mount MC option
        self.just_mounted = 0 # to indicate that the volume was just mounted
        # self.need_lm_update is used in threads to flag LM update in
        # the main thread. First element flags update if not 0,
        # second - state
        # third -  reset timer
        # fourth - error source
        self.need_lm_update = (0, None, 0, None)
        self.asc = None # accounting server client
        self.send_update_cnt = 0 # send lm_update if this counter is 0
        self.control_socket = None
        self.lock_file_info = 0   # lock until file info is updated
        self.read_tape_done = threading.Event() # use this to synchronize read and network threads
        self.stream_w_flag = 0    # this flag is set when before stream_write is called
        self.vc_address = None  # volume clerk address. Used in LM to identify volume clerk. Needed for sharing movers and
        # LMs across systems

        self.dont_update_lm = 0   # if this flag is set do not update LM to avoid mover restart
        self.initial_crc_seed = 1L  # adler 32 default seed
        self.crc_seed = self.initial_crc_seed # crc seed to use in adler32 (0 or 1)
        self.memory_error = 0 # to flag memory error
        self.starting = 1 # to disable changing interval
        self._error = None
        self._error_source = None
        self.memory_debug = 0 # enable memory debugging output if not 0
        self.saved_mode = None
        self.will_mount = None # this is to indicate that the new volume will be mounted and the current dismounted (HIPR)
        self.last_absolute_location = 0L
        self.write_in_progress = False # set this to True before tape write, if write comletes successfuly it will be set to False
        self.read_crc_control = 0 # used to set client_crc for READ and ASSERT
        self.read_errors = 0 # number of read errors per file
        self.write_errors = 0 # number of write errors per file
        # 1: calculate CRC when reading from tape to memory
        # 0: calculate CRC when reading memory

        self.assert_ok = threading.Event() # for synchronization in ASSERT mode
        self.network_write_active = False   # to indicate network write activity



        ##############################################
        # moved from start()
        self.buffer = None # data buffer
        self.udpc = udp_client.UDPClient() # UDP client to communicate with LM(s)
        self.udp_control_address = None  ## needed for tape ingest
        self.udp_ext_control_address = None ## needed for tape ingest
        self.udp_cm_sent = 0  ## needed for tape ingest
        self.last_error = (e_errors.OK, None)
        self.current_location = 0L
        self.current_volume = None #external label of current mounted volume
        self.current_library = None
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
        self.files = ('','') # source and destination files
        self.transfers_completed = 0 # number of completed transfers
        self.transfers_failed = 0 # number of failed transfers
        self.error_times = [] # list of times when transfer failed
        self.consecutive_failures = 0 # consecutive error counter
        self.max_consecutive_failures = 2 # maximal number of consecutive errors (configurable)
        self.max_failures = 3 # maximal number of failures before going OFFLINE (configurable)
        self.failure_interval = 3600 # offline mover if number of failures>self.max_failures within this interval (configurable)
        self.current_work_ticket = {}
        # work may have a method. read_from_hsm work may have methods
        # describing how read must be done. This is used for reading
        # tapes bypassing LM. So called tape reads.
        self.method = None # read_next, read_tape_start to be used with get client
        self.vol_info = {}
        self.file_info = {}
        self.dismount_time = None
        self.delay = 0
        self.fcc = None
        self.vcc = None
        self.stat_file = None # file to write statistics to (configurable)
        self.media_transfer_time = 0.
        self.client_hostname = None
        self.client_ip = None  #NB: a client may have multiple interfaces, this is
                                         ##the IP of the interface we're using
        self.net_driver = net_driver.NetDriver()
        self.client_socket = None
        self.rewind_tape = 0
        self.t0 = time.time()
        self.tr_failed = 0 # do not proceed in transfer_failed() if not 0
        self.on_start = 1 # to debug memory problems
        self.restart_flag = 0

        self.read_count_per_mount = 0 # files successfully read per mounted tape
        self.write_count_per_mount = 0 # files successfully written per mounted tape

        # get mover configuration
        self.config = self.csc.get(self.name)
        if self.config['status'][0] != 'ok':
            raise MoverError('could not start mover %s: %s'%(self.name, self.config['status']))

    def __setattr__(self, attr, val):
        #tricky code to catch state changes
        try:
            if attr == 'state':
                if val != getattr(self, 'state', None):
                    self.time_in_state = 0.0
                    self.in_state_to_cnt = 0
                    self.__dict__['state_change_time'] = time.time()
                    Trace.notify("state %s %s" % (self.shortname, state_name(val)))
                if val != SETUP:
                    self.tmp_vol = None
                    self.tmp_vf = None
                if self.starting == 0:
                    if val == IDLE:
                        # in idle update interval for update_lm is as set
                        interval = self.update_interval
                    elif val == HAVE_BOUND:
                        # in have_bound update interval for update_lm is different
                        interval = self.update_interval_in_bound
                    else:
                        # in all other states it is 3 times +1 more
                        interval = self.update_interval*3+1
                    self.reset_interval(self.update_lm, interval)

        except:
            pass #don't want any errors here to stop us
        self.__dict__[attr] = val

    def dump(self, ticket):
        """
        Dump some internal variables.
        This method is invoked via :class:`dispatching_worker.DispatchingWorker`.
        The corresponding enstore command is::

           enstore mover --dump <mover_name>

        :type ticket: :obj:`dict`
        :arg ticket: ticket received from mover client

        """
        x = ticket
        ticket['status'] = (e_errors.OK,None)
        self.reply_to_caller(ticket)
        self.dump_vars()

    def dump_vars(self, header=None):
        """
        Dump some internal variables into the file.

        The file name is described inside of the code

        """
        d=os.environ.get("ENSTORE_OUT","/tmp")
        pid = os.getpid()
        f = open("%s/mover_dump-%s"%(d,pid,), "a")
        if header:
            f.write('%s\n'%(header,))
        f.write("%s\n"%(time.ctime(),))
        f.write("Dumping /proc/%d/status\n" % (pid,))
        stat_f = open("/proc/%d/status" % (pid,), "r")
        proc_info = stat_f.readlines()
        stat_f.close()
        f.writelines(proc_info)
        f.write("=========================================\n")
        if self.buffer:
            f.write("dumping Buffer\n")
            self.buffer.dump(f)
        for name, value in self.__class__.__dict__.items( ) + self.__dict__.items( ):
            v = value
            try:
                l = len(value)
            except (TypeError, AttributeError):
                l = None
            if l:
                if l < 100:
                    v = value
                else:
                    v = ">100"

            f.write("%s = %s, len = %s\n"%(name, v, l))
        f.write("=========================================\n")
        f.close()

    def set_new_bitfile(self, fc_ticket, vc_ticket):
        """
        Mover requests to create a new record in file table after file was written to media
        by sending ``new_bit_file`` ``work`` to file clerk.

        :type fc_ticket: :obj:`dict`
        :arg fc_ticket: file info part of ticket
        :type vc_ticket: :obj:`dict`
        :arg fc_ticket: volume info part of ticket
        :rtype: :obj:`dict` reply from file clerk (or file clerk client in case of time-out)

        """

        request = {'work':"new_bit_file",
                   'fc'  : fc_ticket,
                   'vc'  : vc_ticket,
                   }
        Trace.log(e_errors.INFO,"new bitfile request %s "%(request,))
        for i in range(2):
            fcc_reply = self.fcc.new_bit_file(request, timeout = 60, retry = 0)
            Trace.log(e_errors.INFO,"New bit file returned %s" % (fcc_reply,))
            if fcc_reply['status'][0] == e_errors.OK:
                break
            else:
                if fcc_reply['status'][0] != e_errors.TIMEDOUT:
                    Trace.log(e_errors.ERROR, "Cannot assign new bit file ID")
                    return
                else:
                    if i == 0:
                        Trace.log(e_errors.INFO,"re-trying new bitfile request %s "%(request))
                    else:
                        Trace.log(e_errors.INFO,"re-try failed %s"%(fcc_reply))
                        return
        if fcc_reply['fc']['location_cookie'] != request['fc']['location_cookie']:
            Trace.log(e_errors.ERROR,
                       "error assigning new bfid requested: %s returned %s"%(request, fcc_reply))
            return
        return fcc_reply

    def shell_command(self, command):
        """
        Execute shell command.

        :type command: :obj:`str`
        :arg command: shell command.
        """

        res = enstore_functions2.shell_command(command)
        if res:
            result = res[0] # stdout
        else:
            result = None
        return result

    def init_data_buffer(self):
        """
        Prepare data buffer for data transfer between media and client.

        """

        Trace.trace(10, "init_data_buffer started")
        if self.buffer:
            self.buffer.clear()
            del(self.buffer)
        self.buffer = Buffer(0, self.min_buffer, self.max_buffer, crc_seed=self.crc_seed)
        if self.log_mover_state:
            cmd = "EPS | grep %s"%(self.name,)
            result =  self.shell_command(cmd)
            Trace.log(e_errors.INFO,"Init d_b LOG: PS %s"%(result,))
        Trace.trace(10, "init_data_buffer finished")


    def return_state(self):
        return state_name(self.state)

    def log_state(self,logit=0):
        """
        Log current mover state into enstore log file.

        :type logit: :obj:`int`
        :arg logit: do log if not 0
        """
        if self.log_mover_state or logit:
            cmd = "EPS | grep %s"%(self.name,)
            result =  self.shell_command(cmd)
            Trace.log(e_errors.INFO,"LOG: PS %s"%(result,))
            thread = threading.currentThread()
            if thread:
                thread_name = thread.getName()
            else:
                thread_name = None
            Trace.log(e_errors.INFO,"LOG: CurThread %s"%(thread_name))
            Trace.trace(87,"LOG: PS %s"%(result,))
            #self.dump_vars()

            # see what threads are running
            threads = threading.enumerate()
            for thread in threads:
                if thread.isAlive():
                    thread_name = thread.getName()
                    Trace.log(e_errors.INFO,"LOG: Thread %s is running" % (thread_name,))
                else:
                    Trace.log(e_errors.INFO,"LOG(%s): Thread is dead"%(thread_name,))

    def log_processes(self,logit=0):
        """
        Log a snapshot of all running processes.

        This is done to check for activities during
        tape operation failure

        :type logit: :obj:`int`
        :arg logit: do log if not 0
        """

        if self.log_mover_state or logit:
            # the format of the "ps" command is such
            # that it usally fits into udp message and
            # still is detailed enough
            cmd = "ps -eo user,start,args --cols 100"
            result =  self.shell_command(cmd)
            Trace.log(e_errors.INFO,"LOG: All running processes \n%s"%(result,))
            thread = threading.currentThread()
            if thread:
                thread_name = thread.getName()
            else:
                thread_name = None
            Trace.log(e_errors.INFO,"LOG: CurThread %s"%(thread_name))


    def memory_in_use(self):
        """
        Get information about memory in use by the current mover process for debugging purposes.

        """
        if os.uname()[0] == "Linux":
            f = open("/proc/%d/status" % (os.getpid(),), "r")
            proc_info = f.readlines()
            f.close()

            for item in proc_info:
                words = item.split()
                if words[0] == "VmSize:":
                    return int(words[1])

            return None #Should never happen.
        else:
            return None

    def memory_usage(self):
        """
        Get information about memory in use on the host for debugging purposes.

        """
        self.on_start = 0
        if self.on_start:
            self.on_start=0
            return
        cmd = "EPS | grep %s"%(self.name,)
        result =  self.shell_command(cmd)
        if result == None:
            return

        # parse the line
        #result[0].split('\t')
        mem_u = 0.
        z=result[0].split(' ')
        #print z
        if z[0] == 'root':
            c = 0
            i = 1
            while 1:
                #print z[i],i,c
                if i < len(z):
                    if z[i] != '':
                        c = c + 1
                    if c == 3:
                        mem_u = float(z[i])
                        break
                    i = i + 1
                else:
                    break
        #print "MEM", mem_u

        #mm = self.memory_in_use()
        #print "MMM", mm

        if mem_u > self.max_idle_mem:
            Trace.log(e_errors.WARNING, "Memory usage %s approaches a limit %s. %s. Will restart the mover"%(mem_u, self.max_idle_mem, result,))
            #self.transfer_failed(e_errors.NOSPACE,"No memory. Mover will restart",error_source=MOVER, dismount_allowed=0)
            self.log_state(logit=1)
            self.dump_vars("High Memory")
            self.restart()
            return 1
        return 0


    def watch_syslog(self):
        """
        Get information for syslog around current time.

        Used for investigation of problems.
        """
        if self.syslog_match:
            try:
                cmd = "$ENSTORE_DIR/src/match_syslog.py '%s'"%(self.syslog_match)
                result =  self.shell_command(cmd)
                if result:
                    for l in result:
                        Trace.log(e_errors.INFO,"SYSLOG Entry:[%s] %s"%(l[:-1],self.current_volume))
            except: # do not know what kind of exception it may be
                Trace.handle_error()

    def lock_state(self):
        self._state_lock.acquire()

    def unlock_state(self):
        self._state_lock.release()

    def check_sched_down(self):
        """
        Check if we are known to be down (offline) in the outage file.

        This still uses rsh to avoid a race condition with the mover starting before the
        inquisitor.
        """

	inq = self.csc.get('inquisitor')
	host = inq.get('host')
	dirname = inq.get('html_file')
	filename = enstore_constants.OUTAGEFILE
	if not host:
	    return 0
	cmd = 'enrsh -n %s cat %s/%s ' % (host, dirname, filename)
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

    def set_sched_down(self):
        """
        Set ourselves to be known down in the outage file.

        """

	self.inq = inquisitor_client.Inquisitor(self.csc)
	ticket = self.inq.down(self.name, "set by mover", 15)
	if not e_errors.is_ok(ticket):
	    Trace.log(e_errors.ERROR,
		      "error setting %s as known down in outage file : %s"%(self.name,
						 enstore_functions2.get_status(ticket)))

    # get the initial statistics
    def init_stat(self, drive_name):
        """
        Initialize statistics reporting for tape media.

        :type drive_name: :obj:`str`
        :arg drive_name: from mover configuration.
        """

        self.stats_on = 0
        self.send_stats = self.config.get('send_stats',1)
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


    def set_volume_noaccess(self, volume, reason=None):
        """
        Wrapper method to set volume to *NOACCESS*.

        :type volume: :obj:`str`
        :arg volume: volume name
        :type reason: :obj:`str`
        :arg reason: reason for setting volume to *NOACCESS*.
        """

        Trace.alarm(e_errors.ALARM, "marking %s NOACCESS. Mover: %s. Reason: %s"%(volume,  self.shortname, reason))
        ret = self.vcc.set_system_noaccess(volume)
        if ret['status'][0] != e_errors.OK:
            Trace.alarm(e_errors.ALARM, "Serious problem can not set volume %s to NOACCESS. Reason %s. Will offline the mover with tape in it"%
                        (volume, ret))
            self.offline()
            return

        self.vol_info.update(self.vcc.inquire_vol(volume))

    def update_tape_stats(self):
        """
        Get tape stats.

        """

        try:
            self.stats = self.tape_driver.get_stats()
            self.block_n = self.stats[self.ftt.BLOCK_NUMBER]
            self.tot_blocks = self.stats[self.ftt.BLOCK_TOTAL]
            self.bloc_loc = long(self.stats[self.ftt.BLOC_LOC])
            self.block_size = self.stats[self.ftt.BLOCK_SIZE]
            self.bot = self.stats[self.ftt.BOT]
            self.read_errors = long(self.stats[self.ftt.READ_ERRORS])
            self.write_errors = long(self.stats[self.ftt.WRITE_ERRORS])

        except (self.ftt.FTTError, TypeError), detail:
            self.transfer_failed(e_errors.WRITE_ERROR, "error getting stats %s %s"%(self.ftt.FTTError, detail), error_source=DRIVE)
            return

    def update_stat(self):
        """
        Record drive statistics into the local file (see the code)
        and send it to drivestat server to enter into drivestat DB.

        """
        if self.driver_type != 'FTTDriver':
            return
        if self.tape_driver and self.tape_driver.ftt:
            self.update_tape_stats()
            if self.tr_failed:
                return

        stats = self.stats
        if self.override_ro_mount and self.tape_driver and self.tape_driver.ftt:
            Trace.log(e_errors.INFO,
                      "volume %s write protection %s  override_ro_mount %s" % \
                      (self.current_volume,
                       stats[self.ftt.WRITE_PROT],
                       self.override_ro_mount))
            if self.current_work_ticket['vc']['external_label'] != self.current_volume:
                vol_info = self.vcc.inquire_vol(self.current_volume)
            else:
                vol_info = self.current_work_ticket['vc']

            #### Added by MZ #################
            try:
                #Get the true/false value if write tab if flipped or not
                # from the drive and the volume DB.
                drive_write_tab_status = int(stats[self.ftt.WRITE_PROT])
                if vol_info['write_protected'] == 'y':
                    volume_db_write_tab_status = 1 #DB says it is flipped
                else:
                    volume_db_write_tab_status = 0 #DB says it is not flipped
                #Compare the two values.  If they don't match raise an alarm.
                if drive_write_tab_status != volume_db_write_tab_status:
                    #Raise an alarm if the DB and the tape values don't
                    # match.
                    Trace.alarm(e_errors.ALARM,
                                "%s write protection tab inconsistency: "
                                "tape %s != db %s" % \
                                (self.current_volume,
                                 stats[self.ftt.WRITE_PROT],
                                 volume_db_write_tab_status),
                                rest = self.current_work_ticket['vc'],
                               )
            except:
                exc, msg, tb = sys.exc_info()
                Trace.log(e_errors.ERROR,
                          "update_stat(): %s: %s" % (str(exc), str(msg)))
            #### Added by MZ #################

        if self.stats_on and self.tape_driver and self.tape_driver.ftt:
            try:
                user_read = long(stats[self.ftt.USER_READ])/1024.
            except TypeError, detail:
                Trace.log(e_errors.ERROR, 'Type Error getting USER_READ: %s %s'%(detail,stats[self.ftt.USER_READ]))
                user_read = -1

            try:
                user_write = long(stats[self.ftt.USER_WRITE])/1024.
            except TypeError, detail:
                Trace.log(e_errors.ERROR, 'Type Error getting USER_WRITE: %s %s'%(detail,stats[self.ftt.USER_WRITE]))
                user_write = -1

            try:
                read_count = long(stats[self.ftt.READ_COUNT])/1024.
            except TypeError, detail:
                Trace.log(e_errors.ERROR, 'Type Error getting READ_COUNT: %s %s'%(detail,stats[self.ftt.READ_COUNT]))
                read_count = -1
            try:
                write_count = long(stats[self.ftt.WRITE_COUNT])/1024.
            except TypeError, detail:
                Trace.log(e_errors.ERROR, 'Type Error getting WRITE_COUNT: %s %s'%(detail, stats[self.ftt.WRITE_COUNT]))
                write_count = -1
            try:
                read_errors = long(stats[self.ftt.READ_ERRORS])
            except TypeError, detail:
                Trace.log(e_errors.ERROR, 'Type Error getting READ_ERRORS: %s %s'%(detail,stats[self.ftt.READ_ERRORS]))
                read_errors = -1
            try:
                write_errors = long(stats[self.ftt.WRITE_ERRORS])
            except TypeError, detail:
                Trace.log(e_errors.ERROR, 'Type Error getting WRITE_ERRORS: %s %s'%(detail,stats[self.ftt.WRITE_ERRORS]))
                write_errors = -1
            try:
                write_prot = int(stats[self.ftt.WRITE_PROT])
            except TypeError, detail:
                Trace.log(e_errors.ERROR, 'Type Error getting WRITE_PROT: %s %s'%(detail,stats[self.ftt.WRITE_PROT]))
                write_prot = -1

            if self.stat_file:
                if not os.path.exists(self.stat_file):
                   dirname, basename = os.path.split(self.stat_file)
                   if not os.path.exists(dirname):
                       os.makedirs(dirname)

                fd = open(self.stat_file, "w")
                fd.write("FORMAT VERSION:         %d\n"%(22,))
                fd.write("INIT FLAG:              %d\n"%(1,))
                fd.write("DRIVE SERNO:            %s\n"%(stats[self.ftt.SERIAL_NUM],))
                fd.write("VENDOR:                 %s\n"%(stats[self.ftt.VENDOR_ID],))
                fd.write("PROD TYPE:              %s\n"%(stats[self.ftt.PRODUCT_ID],))
                fd.write("FIRMWARE:               %s\n"%(stats[self.ftt.FIRMWARE],))
                fd.write("LOGICAL NAME:           %s\n"%(self.logname,))
                fd.write("HOST:                   %s\n"%(self.config['host'],))
                fd.write("VOLSER:                 %s\n"%(self.current_volume,))
                fd.write("OPERATION               %s\n"%(self.mode,))
                fd.write("CLEANING BIT:           %s\n"%(stats[self.ftt.CLEANING_BIT],))
                fd.write("PWR HRS:                %s\n"%(stats[self.ftt.POWER_HOURS],))
                fd.write("MOT HRS:                %s\n"%(stats[self.ftt.MOTION_HOURS],))
                fd.write("RD ERR:                 %s\n"%(stats[self.ftt.READ_ERRORS],))
                fd.write("WR ERR:                 %s\n"%(stats[self.ftt.WRITE_ERRORS],))
                fd.write("MB UREAD:               %s\n"%(user_read,))
                fd.write("MB UWRITE:              %s\n"%(user_write,))
                fd.write("MB DREAD:                  %s\n"%(read_count,))
                fd.write("MB DWRITE:                 %s\n"%(write_count,))
                fd.write("RETRIES:                %s\n"%(stats[self.ftt.TRACK_RETRY],))
                fd.write("WRITEPROT:               %s\n"%(stats[self.ftt.WRITE_PROT],))
                fd.write("UNDERRUN:               %s\n"%(stats[self.ftt.UNDERRUN],))

            if self.send_stats:
                self.dsc.log_stat(stats[self.ftt.SERIAL_NUM],
                                  stats[self.ftt.VENDOR_ID],
                                  stats[self.ftt.PRODUCT_ID],
                                  stats[self.ftt.FIRMWARE],
                                  self.config['host'],
                                  self.logname,
                                  "ABSOLUTE",
                                  time.time(),
                                  self.current_volume,
                                  stats[self.ftt.POWER_HOURS],
                                  stats[self.ftt.MOTION_HOURS],
                                  stats[self.ftt.CLEANING_BIT],
                                  user_read,
                                  user_write,
                                  read_count,
                                  write_count,
                                  read_errors,
                                  write_errors,
                                  stats[self.ftt.TRACK_RETRY],
                                  stats[self.ftt.UNDERRUN],
                                  0,
                                  write_prot,
                                  self.name)

    def fetch_tape_device(self):
        """
        Tape device is /dev/rmt/tps<n>d<m>n.
        Search for such pattern

        :rtype: :obj:`str` or None - tape device
        """
        cmd = "ls /dev/rmt/tps*d*n"
        result =  enstore_functions2.shell_command2(cmd)
        if result[0] == 0:
            lines = result[1].split()
            if len(lines) == 1:
                return lines[0]
            else:
                Trace.alarm(e_errors.ERROR,"Non existing or ambiguous device in /dev/rmt: %s"%(result,))
        else:
               Trace.alarm(e_errors.ERROR,"Device search returned %s"%(result[2],))

        return None

    def get_tape_device(self):
        """
        Devices change on each reboot since SLF6.
        To map location in the SL robot with correct tape driver get wwn from media changer and lookup device
        in /dev/tape.
        This approach needs a binding between tps and nst, which is implemented outside of mover.py
        :rtype: :obj:`str` or None - tape device

        Implemented for STK only
        """

        mcc_reply = self.mcc.displaydrive(self.mc_device)
        if not e_errors.is_ok(mcc_reply):
            return self.fetch_tape_device()

        if self.mc_keys.has_key('type') and self.mc_keys['type'] == 'STK_MediaLoader':
            # drive_info example:
            # {'Wwn': '50.01.04.f0.00.a2.b4.b2', 'drive': '2,3,1,15'}
            wwn = int("0x%s"%(mcc_reply['drive_info']['Wwn'].replace('.','')),16)+1 # according to STK documentation wwn must be reported Wwn+1
        else: # here should be other MC types
            wwn = int("0x%s"%(mcc_reply['drive_info']['Wwn'].replace('.','')),16)+1  # jsut set it the same as for STK library so far

       # find the nst reference
        cmd = "ls -l /dev/tape/by-path | grep %0x  | grep nst"%(wwn,)
        result =  enstore_functions2.shell_command2(cmd)
        if result[0] == 0:
            lines = result[1].split()
            n1 = lines[-1:]
            nst_device = lines[-1:][0].split('/')[-1:][0]
            nst_num = int(nst_device[-1:])

            rmt_dir = '/dev/rmt'
            ftt_tape = '/dev/rmt/tps%sd0n'%(nst_num,)
            # make symlink
            if not os.path.exists(rmt_dir):
                try:
                    os.makedirs(rmt_dir)
                except:
                    Trace.handle_error()
                    Trace.alarm(e_errors.ERROR, "Can not create %s"%(rmt_dir,))
                    return None
            try:
                os.remove(ftt_tape)
            except:
                pass # file could be not existing, which will cause exception, but we do not care

            Trace.log(e_errors.INFO, 'symlinking: /dev/nst%s to %s'%(nst_num,ftt_tape))
            os.symlink('/dev/nst%s'%(nst_num,), ftt_tape)

            # find the sg device
            cmd = "sg_map -st | grep %s"%(nst_device,)
            result =  enstore_functions2.shell_command2(cmd)
            if result[0] == 0:
                sg_dev = result[1].split()[0]
                sc_dir = '/dev/sc'
                ftt_sc =  '/dev/sc/sc%sd0'%(nst_num,)
                # make symlink
                if not os.path.exists(sc_dir):
                    try:
                        os.makedirs(sc_dir)
                    except:
                        Trace.handle_error()
                        Trace.alarm(e_errors.ERROR, "Can not create %s"%(sc_dir,))
                        return None
                try:
                    os.remove(ftt_sc)
                except:
                    pass # file could be not existing, which will cause exception, but we do not care
                Trace.log(e_errors.INFO, 'symlinking: %s %s'%(sg_dev, ftt_sc))
                os.symlink(sg_dev, ftt_sc)
                return ftt_tape
            else:
                Trace.alarm(e_errors.ERROR,"Device search returned %s"%(result[2],))
        return None

    def start(self):
        """
        Set up the mover configuration received from configuration server and
        start serving requests.

        """

        self.logname = self.config.get('logname', self.name)
        Trace.init(self.logname, self.config.get('include_thread_name', 'yes'))
        # do not restart if some mover processes are already running
        cmd = "EPS | grep %s | grep %s | grep -v grep"%(self.name,"mover.py")
        result =  self.shell_command(cmd)
        if result:
            if len(result) > 1:
                Trace.alarm(e_errors.ERROR,"mover is already running, can not restart: %s"%(result,))
                # remove when problem is resoved
                threads = threading.enumerate()
                for thread in threads:
                    if thread.isAlive():
                        thread_name = thread.getName()
                        Trace.log(e_errors.INFO,"LOG: Thread %s is running" % (thread_name,))
                    else:
                        Trace.log(e_errors.INFO,"LOG(%s): Thread is dead"%(thread_name,))
                ########

                time.sleep(2)
                sys.exit(-1)

        self.restart_unlock()

        Trace.log(e_errors.INFO, "starting mover %s. MAX_BUFFER=%s MB" % (self.name, MAX_BUFFER/MB))

        device  =  self.config.get('device')
        if device:
            self.config['device'] = os.path.expandvars(device)

        self.state = IDLE
        self.force_clean = 0
        # get initial fcc and fcc
        try:
            self.vcc = volume_clerk_client.VolumeClerkClient(self.csc)
        except:
            self.vcc == None
        try:
            self.fcc = volume_clerk_client.VolumeClerkClient(self.csc)
        except:
            self.fcc == None


        #################################
        ### Mover type dependent settings
        #################################
        if isinstance(self, DiskMover):
            self.do_eject = 0
            self.do_cleaning = 0
            self.rem_stats = 0
            default_media_type = "disk"
            self.default_block_size = 131072
            self.tape_driver = disk_driver.DiskDriver()
            self.mover_type = self.config.get('type', enstore_constants.DISK_MOVER)
            self.max_rate = self.config.get('max_rate', 100*MB) #XXX
            self.ip_map = self.config.get('ip_map','cluster_fs')

            # check if device exists
            if self.config.get('device'):
                if not os.path.exists(self.config['device']):
                    # try to create disk data directory
                    try:
                        os.makedirs(self.config['device'], 0755)
                    except OSError, detail:
                        if detail[0] != errno.EEXIST:
                            raise OSError(detail)
            else:
                Trace.alarm(e_errors.ALARM, "Cannot start. Device %s does not exist"%(self.config['device'],))
                sys.exit(-1)

            # for cluster fs all files are available for any disk mover
            # Temporary file for write operations.
            # The data is written into this file.
            # After write completes this file is moved into real destination file.
            # If write does not complete due to mover crash or
            # other unexpected halt this file does not get removed
            # indicating a problem with file transfer.
            tmp_dir = self.config.get("tmp_dir", "%s/tmp"%(self.config['device'],))
            self.tmp_file = os.path.join(tmp_dir, self.name)
            try:
                os.makedirs(tmp_dir, 0755)
            except OSError, detail:
                if detail[0] != errno.EEXIST:
                    Trace.alarm(e_errors.ALARM, "Cannot start. OSError %s"%(str(detail),))
                    sys.exit(-1)
            # check if tmp file exists on start
            try:
                f = open(self.tmp_file, 'r')

            except IOError, detail:
                if detail[0] != errno.ENOENT:
                    Trace.alarm(e_errors.ALARM, "Cannot start. OSError %s"%(str(detail),))
                    sys.exit(-1)
        else:
            self.do_eject = 1
            self.do_cleaning = 1
            self.rem_stats = 1
            default_media_type = '8MM'
            self.mover_type = self.config.get('type','')
            self.max_rate = self.config.get('max_rate', 11.2*MB) #XXX
            self.ip_map = self.config.get('ip_map','')
            self.mcc = media_changer_client.MediaChangerClient(self.csc,
                                                               self.config['media_changer'])
            self.mc_keys = self.csc.get(self.mcc.media_changer)
            # STK robot can eject tape by either sending command directly to drive or
            # by pushing a corresponding button
            if self.mc_keys.has_key('type') and self.mc_keys['type'] == 'STK_MediaLoader':
                self.can_force_eject = 1
            else:
                self.can_force_eject = 0

        #how often to send an alive heartbeat to the event relay
        self.alive_interval = monitored_server.get_alive_interval(self.csc, self.name, self.config)
        self.address = (self.config['hostip'], self.config['port'])
        self.lm_address = ('none',0) # LM that called mover
        if self.config.has_key('do_eject'):
            if self.config['do_eject'][0] in ('n','N'):
                self.do_eject = 0

        if self.config.has_key('do_cleaning'):
            if self.config['do_cleaning'][0] in ('n','N'):
                self.do_cleaning = 0

        self.rem_stats = 1
        if self.config.has_key('get_remaining_from_stats'):
            if self.config['get_remaining_from_stats'][0] in ('n','N'):
                self.rem_stats = 0

        self.mc_device = self.config.get('mc_device', 'UNDEFINED')
        self.media_type = self.config.get('media_type', default_media_type) #XXX
        self.min_buffer = self.config.get('min_buffer', 8*MB)
        self.max_buffer = self.config.get('max_buffer', 64*MB)
        if self.max_buffer > MAX_BUFFER:
           self.max_buffer = MAX_BUFFER # python list can not be more than this number
        self.log_mover_state = self.config.get('log_state', None)
        self.syslog_match = self.config.get("syslog_entry",None) # pattern to match in syslog for scsi error
        self.restart_on_error = self.config.get("restart_on_error", None)
        self.connect_to = self.config.get("connect_timeout", 15)
        self.connect_retry = self.config.get("connect_retries", 4)
        self.stop =  self.config.get('stop_mover',None)
        self.check_first_written_enabled =self.config.get("check_first_written_file", 0)
        self.max_idle_mem = self.config.get('max_idle_mem', 10) # pecentage of memory usage in idle state

        if self.check_sched_down() or self.check_lockfile():
            self.state = OFFLINE

        self.asc = accounting_client.accClient(self.csc, self.logname)

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
            if lib_config.has_key('mover_port'):
                port = lib_config['mover_port']
            else:
                port = lib_config['port']
            self.libraries.append((lib, (lib_config['hostip'], port)))
        self.saved_libraries = self.libraries ## needed for tape ingest

        #how often to send a message to the library manager
        self.update_interval = self.config.get('update_interval', 15)
        self.update_interval_in_bound = self.config.get('update_interval_in_bound', self.update_interval)

        ##Setting this attempts to optimize filemark writing by writing only
        ## a single filemark after each file, instead of using ftt's policy of always
        ## writing two and backspacing over one.  However this results in only
        ## a single filemark at the end of the volume;  causing some drives
        ## (e.g. Mammoth-1) to have trouble spacing to end-of-media.
        self.single_filemark=self.config.get('single_filemark', 0)
        self.memory_debug = self.config.get('memory_debug', 0)
        self.exp_time_factor = self.config.get('expected_time_factor', 50)

        self.check_written_file_period = self.config.get('check_written_file', 0)
        self.files_written_cnt = 0
        self.max_time_in_state = self.config.get('max_time_in_state', 600) # maximal time allowed in a certain states
        self.max_in_state_cnt = self.config.get('max_in_state_cnt', 2)
        self.remaining_factor = self.config.get('remaining_factor', 0.01)
        crc_control = self.config.get("read_crc_control", None)
        if crc_control and crc_control in (0,1):
            self.read_crc_control = crc_control

        elif crc_control != None:
            Trace.log(e_errors.ERROR, "Ignoring invalid 'read_crc_control' value of %s found in configuration. Allowed values are 0 or 1. Using default value of 0 ('calculate CRC when reading from memory')."%(crc_control,))
        self.read_error_recovery_timeout = float(self.config.get("read_error_recovery_timeout", 20*60))

        if not self.config.get('device') :
            # try to deduce device for tape mover
            self.config['device'] = self.get_tape_device()
            if self.config['device'] :
                Trace.log(e_errors.INFO, "Dynamically picked device %s"%(self.config['device'],))
            else:
                Trace.alarm(e_errors.ALARM, "Cannot start. No device was found")
                sys.exit(-1)

        self._reinit() # pickup whatever reinit provides
        drive_rate_threshold =  self.config.get("drive_rate_threshold", None)
        if drive_rate_threshold:
            exit_flag = False
            if type(drive_rate_threshold) == types.TupleType and len(drive_rate_threshold) == 2:
                try:
                   self.drive_rate_threshold = (long(drive_rate_threshold[0]), long(drive_rate_threshold[1]))
                except:
                    exit_flag = True
            else:
                exit_flag = True
            if exit_flag:
                Trace.log(e_errors.ERROR, "Wrong format for drive_rate_threshold. Check configuration")
                sys.exit(-1)

        Trace.log(e_errors.INFO, "Starting in state %s"%(state_name(self.state),))

        self.device = self.config['device']
        if self.driver_type == 'NullDriver':
            self.check_written_file_period = 0 # no file check for null mover
            self.device = None
            self.single_filemark = 1 #need this to cause EOD cookies to update.
            ##XXX should this be more encapsulated in the driver class?
            #import null_driver
            self.tape_driver = null_driver.NullDriver()
        elif self.driver_type == 'FTTDriver':
            self.stat_file = self.config.get('statistics_path', None)
            Trace.log(e_errors.INFO,"statitics path %s"%(self.stat_file,))
            self.compression = self.config.get('compression', None)
            if self.compression > 1: self.compression = None
            import ftt_driver
            self.ftt = __import__("ftt")
            self.tape_driver = ftt_driver.FTTDriver()
            have_tape = 0
            if self.state == IDLE:
                good_label = 1
                try:
                    have_tape = self.tape_driver.open(self.device, mode=0, retry_count=3)
                except self.ftt.FTTError, detail:
                    Trace.alarm(e_errors.ERROR,"Supposedly a serious problem with tape drive: %s %s. Will terminate"%(self.ftt.FTTError, detail))
                    time.sleep(5)
                    sys.exit(-1)

                try:
                    stats = self.tape_driver.get_stats()
                    self.config['product_id'] = stats[self.ftt.PRODUCT_ID]
                    self.config['serial_num'] = stats[self.ftt.SERIAL_NUM]
                    self.config['vendor_id'] = stats[self.ftt.VENDOR_ID]
                except (self.ftt.FTTError, TypeError), detail:
                    Trace.alarm(e_errors.ALARM, "Can not start: %s"%(detail,))
                    print "Can not start: %s"%(detail,)
                    sys.exit(-1)

                if self.config['product_id'] in ("T10000C", "T10000D"):
                    # for T10000C/D set Allow Maximum Capacity (AMC)
                    disable_AMC = self.config.get('disable_AMC', False)
                    if not disable_AMC:
                        # enable AMC
                        try:
                            enabled = scsi_mode_select.t10000c_amc(self.tape_driver,  1)
                        except Exception, detail:
                            Trace.alarm(e_errors.ERROR, 'Failed to set  "Allow Maximum Capacity": %s'%(str(detail),))
                            sys.exit(-1)
                        if not enabled:
                            Trace.alarm(e_errors.ERROR, '"Allow Maximum Capacity" was not set')
                            sys.exit(-1)
                        # if AMC is set do not use remaining bytes information from tape drive
                        self.rem_stats = 0
                    # set initial compression
                    rc = scsi_mode_select.t10000_set_compression(self.tape_driver, compression = self.compression)
                    if not rc:
                        Trace.alarm(e_errors.ERROR, "Compression setting failed")
                        sys.exit(-1)

                if have_tape == 1:
                    self.init_stat(self.logname)
                    status = self.tape_driver.verify_label(None)
                    #self.write_counter = 0 # this flag is used in write_tape to verify tape position
                    if status[0]==e_errors.OK:
                        self.current_volume = status[1]
                        if self.state == OFFLINE:
                            Trace.log(e_errors.INFO, "Mover is OFFLINE. Performing dismount at startup")
                            self.dismount_volume(after_function=self.offline)
                            self.current_volume = None
                        else:
                            self.state = HAVE_BOUND
                            Trace.log(e_errors.INFO, "have vol %s at startup" % (self.current_volume,))
                            self.dismount_time = time.time() + self.default_dismount_delay

                            '''
                            Let tape get dismounted
                            if self.vcc:
                                # get volume and library information
                                v = self.vcc.inquire_vol(self.current_volume)
                                if v['status'][0] == e_errors.OK:
                                    lm_config = self.csc.get("%s.library_manager"%(v["library"],), None)
                                    if lm_config:
                                        a1 = lm_config.get('hostip')
                                        a2 = lm_config.get('port')
                                        self.lm_address = (a1, a2)
                                        self.lm_address_saved = self.lm_address
                            '''

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
                else:
                    self.tape_driver.close()
                if not have_tape:
                    Trace.log(e_errors.INFO, "performing precautionary dismount at startup")
                    vol_ticket = { "external_label": "Unknown",
                                   "media_type":self.media_type}
                    # check if media changer is open
                    retry_cnt = 10
                    for i in range(retry_cnt):
                        mcc_reply = self.mcc.GetWork()
                        if not mcc_reply.has_key('max_work'):
                            Trace.log(e_errors.INFO, "Media Changer returned %s"%(mcc_reply,))
                        else:
                            time.sleep(2)
                            break
                    else:
                        Trace.log(e_errors.ERROR, "Media Changer did not return expected reply: %s"%(mcc_reply,))
                        self.state = OFFLINE
                    if self.state != OFFLINE:
                        if mcc_reply['max_work'] == 0:
                            # media changer would not accept requests. Go OFFLINE
                            Trace.alarm(e_errors.ERROR, "media changer is locked, going OFFLINE")
                            self.state = OFFLINE
                        else:

                            while 1:
                                mcc_reply = self.mcc.unloadvol(vol_ticket, self.name, self.mc_device)
                                status = mcc_reply.get('status')
                                if status and status[0] == e_errors.MC_QUEUE_FULL:
                                    # media changer responded but could not perform the operation
                                    Trace.log(e_errors.INFO, "Media Changer returned %s"%(status[0],))
                                    # to avoid false "too long in state.."
                                    # reset self.time_in_state
                                    self.time_in_state = time.time()
                                    time.sleep(10)
                                    continue
                                else:
                                    break

                            if status and status[0] != e_errors.OK:
                                self.offline()
                                #return Do not return here as this does not comlete
                                # the initialization
                                # dispathing worker does not get instantiated and
                                # this causes a failure, causing exceptions and
                                # restarts in a loop
                if good_label: # to prevent mover from failure in ftt_get_stats
                    if self.maybe_clean():
                        have_tape = 0

        else:
            # disk mover
            pass

        self.mount_delay = int(self.config.get('mount_delay',
                                           self.tape_driver.mount_delay))

        if type(self.mount_delay) != type(0):
            self.mount_delay = int(self.mount_delay)
        if self.mount_delay < 0:
            self.mount_delay = 0

        dispatching_worker.DispatchingWorker.__init__(self, self.address)
        self.add_interval_func(self.update_lm, self.update_interval) #this sets the period for messages to LM.
        self.add_interval_func(self.need_update, 1) #this sets the period for checking if child thread has asked for update.
        self.set_error_handler(self.handle_mover_error)
        ##setup the communications with the event relay task
        self.erc.start([event_relay_messages.NEWCONFIGFILE])
        ##start our heartbeat to the event relay process
        self.erc.start_heartbeat(self.name, self.alive_interval, self.return_state)
        if self.memory_debug:
            self.dump_vars("At the start")


    def restart_lockfile_name(self):
        d=os.environ.get("ENSTORE_TMP","/tmp")
        return os.path.join(d, "restart_lock%s"%(self.name,))

    def restart_check(self):
        return os.path.exists(self.restart_lockfile_name())

    def restart_lock(self):
        filename=self.restart_lockfile_name()
        try:
            f=open(filename,'w')
            f.write('locked\n')
            f.close()
        except (OSError, IOError):
            Trace.log(e_errors.ERROR, "Cannot write %s"%(filename,))

    def restart_unlock(self):
        filename=self.restart_lockfile_name()
        try:
            os.unlink(filename)
        except (OSError, IOError), detail:
            Trace.log(e_errors.ERROR, "Cannot unlink %s %s"%(filename,detail))



    def restart(self, do_restart=1):
        """
        Restart myself.

        """
        cur_thread = threading.currentThread()
        if cur_thread:
            cur_thread_name = cur_thread.getName()
        else:
            cur_thread_name = None
        Trace.log(e_errors.INFO, "Current thread %s"%(cur_thread_name,))
        if cur_thread_name != 'MainThread':
            self.restart_flag = 1
            return
        # to avoid restart while restarting get lock from a file
        restart_allowed = 1
        Trace.log(e_errors.INFO, "Checking restart lock")
        if self.restart_check():
            restart_allowed = 0
            Trace.log(e_errors.INFO, "Waiting for a lock to restart mover")
            for wait in range(60):
               if self.restart_check():
                   time.sleep(2)
               else:
                   restart_allowed = 1
                   break
        if restart_allowed == 0:
            Trace.log(e_errors.ERROR, "Can not restart there is a lock file %s preventing restart"%(self.restart_lockfile_name(),))
            sys.exit(-1)
        Trace.log(e_errors.INFO, "Getting restart lock")
        self.restart_lock()
        if cur_thread_name:
            if cur_thread_name in ('net_thread', 'MainThread'):
                # check if tape_thread is active before allowing dismount
                thread = getattr(self, 'tape_thread', None)
                if thread and thread.isAlive():
                    Trace.log(e_errors.INFO,"waiting for tape thread to finish")

                for wait in range(60):
                    if thread and thread.isAlive():
                        time.sleep(2)
                    else:
                        break
            elif cur_thread_name == 'tape_thread':
                Trace.log(e_errors.INFO,"restart was called from tape thread")

        # release data buffer
        #Trace.log(e_errors.INFO, "releasing data buffer")
        #if self.buffer:
        #    self.buffer.clear()
        #    del(self.buffer)
        #    self.buffer = None
        if do_restart:
            cmd = '/usr/bin/at now+1minute'
            ecmd = "enstore start --just %s\n"%(self.name,)
            Trace.log(e_errors.INFO, "sending restart command: %s"%(ecmd,))
            p=os.popen(cmd, 'w')
            p.write(ecmd)
            p.close()
        sys.exit(0)
        Trace.alarm(e_errors.ALARM, "Could not exit! Sys.exit did not work")


    def _reinit(self):
        """
        Overridden from generic server
        to set variables if configuration was reloaded
        """

        encp_dict = self.csc.get('encp')
        if encp_dict:
            self.crc_seed = long(encp_dict.get("crc_seed", 1L))


    def send_error_and_restart(self, err = (None, None), do_restart=1):
        """
        Send error message and restart.

        """

        if err[0]:
            e = self.error_msg(err)
        else:
            e = self.last_error
        self.send_error_msg(e)
        time.sleep(5)
        self.dont_update_lm = 0
        self.restart()


    def device_dump_S(self, ticket):
        """
        Server hook for :meth:`device_dump`.

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

        ticket['status'] = (e_errors.OK, res)
        self.reply_to_caller(ticket)
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


    def check_drive_rate(self, bfid, file_size, drive_rate, rw, read_err, write_err, drive_stats):
        """
        Check drive rate and update suspect drive table if needed.

        :type bfid: :obj:`str`
        :arg bfid: bfid of transferred file
        :type file_size: :obj:`long`
        :arg file_size: size of last writtent or read file in bytes
        :type drive_rate: :obj:`float`
        :arg drive_rate: rate of tape drive in bytes/s.
        :type rw:  :obj:`str`
        :arg rw: read (``r``) or write (``w``)
        :type read_errors:  :obj:`int`
        :arg read_errors: tape drive read errors for this file (we have seen non 0 read errors during write operation).
        :type write_errors: :obj:`int`
        :arg write_errors: tape drive write errors for this file
        :type drive_stats: :class:`ftt.FTT`
        :arg drive_stats: information returned by :meth:`drive.get_stats`.
        """

        if hasattr(self, "drive_rate_threshold") and type(self.drive_rate_threshold) == types.TupleType:
            if file_size > self.drive_rate_threshold[0]: # self.drive_rate_threshold[0] - cut off file size
                if drive_rate < self.drive_rate_threshold[1]: # self.drive_rate_threshold[1] - cut off drive rate
                    # put information into suspect drives table
                    ticket = { "drive_id":self.logname,
                               "drive_sn":drive_stats[self.ftt.SERIAL_NUM],
                               "volume":self.current_volume,
                               "drive_rate":drive_rate,
                               "rw": rw,
                               "write_error_count": read_err,
                               "read_error_count": write_err,
                               "file_size": file_size,
                               "bfid": bfid,
                               }
                    self.asc.log_drive_info(ticket)
                    Trace.alarm(e_errors.ERROR, "Slow drive rate detected. Drive %s rate %3.2f MB/s"%(self.logname, drive_rate/MB))


    def check_written_file(self):
        """
        Define if it is time to check written file.

        """
        rc = 0
        if self.just_mounted and self.check_first_written_enabled:
            self.just_mounted = 0
            rc = 1
        if self.check_written_file_period:
            ran = random.randrange(self.check_written_file_period,self.check_written_file_period*10,1)
            if (ran % self.check_written_file_period == 0):
                rc = 1
        return rc

    def nowork(self, ticket):
        """
        No work is no work.

        """
        Trace.trace(98, "nowork")
        x =ticket # to trick pychecker
        if self.control_socket:
            try:
                self.control_socket.close()
                self.listen_socket.close()
            except:
                pass
            self.control_socket = None
            self.listen_socket = None
        if self.udp_control_address:
            if self.udp_ext_control_address in self.libraries:
                self.libraries = self.saved_libraries
                self.lm_address = self.lm_address_saved
            self.udp_control_address = None
            self.udp_ext_control_address = None
        self.method = None
        return {}

    def handle_mover_error(self, exc, msg, tb):
        """
        Log the mover error.

        """
        x = tb # to trick pychecker
        Trace.log(e_errors.ERROR, "handle mover error %s %s"%(exc, msg))
        Trace.trace(10, "%s %s" %(self.current_work_ticket, state_name(self.state)))
        if self.current_work_ticket:
            try:
                Trace.trace(10, "handle error: calling transfer failed, str(msg)=%s"%(str(msg),))
                self.transfer_failed(exc, msg)
            except:
                pass

    def update_lm(self, state=None, reset_timer=None, error_source=None):
        """
        Send updated mover state and additional information to library manager(s).

        As a rule this method is called from the main thread.

        :type state: :obj:`int`
        :arg state: mover state
        :type reset_timer: :obj:`int`
        :arg reset_timer: reset LM update interval timer (:meth:`update_lm` is called periodically or by direct call).
        :type error_source: :obj:`str`
        :arg error_source: ``TAPE``, ``ROBOT``, ``NETWORK``, ``DRIVE``, ``USER``, ``MOVER``, ``UNKNOWN``

        """

        Trace.trace(20,"update_lm: dont_update=%s" % (self.dont_update_lm,))
        if self.state == IDLE:
            # check memory usage and if bad restart
            self.memory_usage()

        if self.dont_update_lm: return
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
        if (thread_name == 'MainThread') and self.restart_flag:
            self.restart()

        if not hasattr(self,'_last_state'):
            self._last_state = None

        ## it may happen so that for some unknown yet reason
        ## the net thread hangs after connection was broken by mover
        ## as the consequence the data transfers can not be done
        ## until hung net thread is gone. This thread can hang forever
        ## to deal with this situation do the following here:
        ## check for the presence of the net and tape thread in the
        ## IDLE or HAVE_BOUND state. They should not be running.
        ## If net thread is running restart the mover.
        ## It is a radical measure, but documents do not recommend
        ## killing thread with not closed or hung socket.
        ## If tape thread is running put mover offline.
        t_thread = getattr(self, 'tape_thread', None)
        n_thread = getattr(self, 'net_thread', None)
        if self.state in (IDLE, HAVE_BOUND):
            t_in_state = int(time.time()) - int(self.state_change_time)
            if n_thread and n_thread.isAlive():
                if t_in_state <= 1:
                    ## skip sending lm _update
                    ## to allow network thread to complete
                    return
                if not hasattr(self,'restarting'):
                    Trace.alarm(e_errors.ALARM,
                                "Net thread is running in the state %s. Will restart the mover"%
                                (state_name(self.state),))
                    self.dont_update_lm = 1
                    if self.state == HAVE_BOUND:
                        self.run_in_thread('media_thread', self.dismount_volume, after_function=self.send_error_and_restart)
                        #self.dismount_volume(after_function=self.restart)
                    else:
                        self.restart()
                    self.restarting = True # set it to whatever
                return
            elif t_thread and t_thread.isAlive():
                if t_in_state <= 1:
                    ## skip sending lm _update
                    ## to allow tape thread to complete
                    return
                if self.config['product_id'].find("DLT") == -1:
                    Trace.alarm(e_errors.ALARM,
                                "Tape thread is running in the state %s. Will offline the mover"%
                                (state_name(self.state),))
                    self.watch_syslog()
                    self.log_state(logit=1)
                    Trace.log(e_errors.INFO, "Trying to dismount volume %s"%(self.current_volume,))
                    self.dismount_volume(after_function=self.offline)
                    return
                else:
                    Trace.alarm(e_errors.WARNING,"Tape thread is running in the state %s."%
                                (state_name(self.state),))


        now = time.time()
        transfer_stuck = 0
        Trace.trace(20, "reset_timer %s"%(reset_timer,))
        if reset_timer:
            self.reset_interval_timer(self.update_lm)

        # check time in state
        if (thread_name == 'MainThread'):
            time_in_state = int(now - self.state_change_time)
            if not hasattr(self,'time_in_state'):
                self.time_in_state = 0
            Trace.trace(88, "time in state %s %s %s %s state %s"%
                        (time_in_state,self.time_in_state,self.max_time_in_state, self.state_change_time, state_name(self.state)))
            if (((time_in_state - self.time_in_state) > self.max_time_in_state) and
                (self.state in (SETUP, SEEK, MOUNT_WAIT, DISMOUNT_WAIT, ERROR, FINISH_WRITE, ACTIVE))):
                send_alarm = True
                if self.state == ACTIVE:
                    Trace.trace(8, "bytes read last %s bytes read %s"%(self.bytes_read_last, self.bytes_read))
                    if self.bytes_read_last == self.bytes_read:
                        if self.mode in (WRITE, ASSERT):
                            if self.bytes_written == self.bytes_to_write:
                                # data transfer completed
                                # but the state has not yet changed
                                # return here to not raise a false alarm
                                return
                        else:
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
                                if hasattr(self,'too_long_in_state_sent'):
                                    del(self.too_long_in_state_sent)
                    else:
                        # data is being transferred
                        # do not raise alarm
                        send_alarm = False

                if not hasattr(self,'too_long_in_state_sent') and send_alarm:
                    if (self.state != ERROR and
                        self.mode != ASSERT and  #in ASSERT mode network is not used
                        not self.network_write_active): # no network activity
                        try:
                            Trace.alarm(e_errors.WARNING, "Too long in state %s for %s. Client host %s" %
                                        (state_name(self.state),self.current_volume, self.current_work_ticket['wrapper']['machine'][1]))
                        except TypeError:
                            exc, msg, tb = sys.exc_info()
                            try:
                                Trace.log(e_errors.ERROR, "error sending alarm %s %s"%(exc, msg))
                                Trace.log(e_errors.INFO, "state %s"%(self.state,))
                                Trace.log(e_errors.INFO, "volume %s"%(self.current_volume,))
                                Trace.log(e_errors.INFO, "host %s"%(self.current_work_ticket['wrapper']['machine'][1],))
                            except TypeError:
                                Trace.log(e_errors.ERROR,"wrong ticket format? %s"%(self.current_work_ticket))
                                Trace.log(e_errors.INFO,"will restart")
                                time.sleep(5)
                                self.restart()

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
                Trace.trace(8, "in_state_to_cnt %s max_in_state_cnt %s"%(self.in_state_to_cnt, self.max_in_state_cnt,))

                if ((self.in_state_to_cnt >= self.max_in_state_cnt) and
                    (self.state != ERROR) and
                    (self.state != FINISH_WRITE)):
                    if self.state != ACTIVE:
                        # mover is stuck. There is nothing to do as to
                        # offline it
                        msg = "mover is stuck in %s. Client host %s" % (self.return_state(),self.current_work_ticket['wrapper']['machine'][1])
                        Trace.alarm(e_errors.ERROR, msg)
                        if self.state == SEEK:
                            self.set_volume_noaccess(self.current_volume, "Mover stuck. Seelog for details")
                        self.transfer_failed(e_errors.MOVER_STUCK, msg, error_source=TAPE, dismount_allowed=0)
                        self.offline()
                        return

                    else:
                        if transfer_stuck:
                            msg = "data transfer to or from client stuck. Client machine %s. Breaking connection."%(self.current_work_ticket['wrapper']['machine'][1],)
                            if self.mode == READ:
                                msg1 = "Stream write flag %s. Bytes %s/%s"%(self.stream_w_flag, self.bytes_written, self.bytes_to_write)
                                msg = msg+msg1
                            self.transfer_failed(e_errors.ENCP_STUCK, msg, error_source=NETWORK)
                            return

        #ticket = self.format_lm_ticket(state=state, error_source=error_source)
        # if mover is offline or active send LM update less often
        Trace.trace(20, "BEFORE: STATE %s udp_sent %s"%(state_name(self.state), self.udp_cm_sent))
        Trace.trace(20, "send_update_cnt %s method %s"%(self.send_update_cnt, self.method))
        send_rq = 1
        use_state = 1
        if ((self.state == self._last_state) and
            (self.state in (OFFLINE, ACTIVE, MOUNT_WAIT, DISMOUNT_WAIT, SEEK))):
            send_rq = 0
            if self.send_update_cnt > 0:
               self.send_update_cnt = self.send_update_cnt - 1
            if self.send_update_cnt == 0:
               send_rq = 1
               self.send_update_cnt = 10
        if self.method and self.method == 'read_next' and self.udp_cm_sent:
            send_rq = 0
        set_cm_sent = 1 # tape ingest, initially enable
        Trace.trace(20, "send_rq %s"%(send_rq,))

        if send_rq:
            libraries = copy.deepcopy(self.libraries)
            if self.state == IDLE and len(libraries) > 1:
                # shuffle the array to get a "fair share" for the library
                random.shuffle(libraries)
            for lib, addr in libraries:
                if use_state:
                    ticket = self.format_lm_ticket(state=state, error_source=error_source)
                else:
                    ticket = self.format_lm_ticket(state=self.state, error_source=error_source)

                Trace.trace(20, "ticket %s"%(ticket,))
                if state != self._last_state:
                    Trace.trace(10, "update_lm: %s to %s" % (ticket, addr))
                Trace.trace(20, "addr %s lm_addr %s"%(addr, self.lm_address,))
                self._last_state = self.state
                # only main thread is allowed to send messages to LM
                # exception is a mover_busy and mover_error works
                if ((thread_name == 'MainThread') and
                    (ticket['work'] != 'mover_busy') and
                    (ticket['work'] != 'mover_error')):
                    ## XXX Sasha - this is an experiment - not sure this is a good idea!
                    if addr != self.lm_address and self.state == HAVE_BOUND:
                        ticket['work'] = 'mover_busy'
                    else:
                        to = 0
                        retry = 0
                        if addr == self.udp_control_address:
                            to = 10
                            retry = 1 # 1 means no retry
                        if addr == self.udp_control_address and ticket['work'] == 'mover_busy':
                            # do not send mover_busy to get
                            set_cm_sent = 0
                            pass
                        else:
                            Trace.trace(20,"update_lm: send with wait %s to %s TO %s retry %s"%
                                        (ticket['work'],addr, to, retry))
                            try:
                                t0 = time.time()

                                request_from_lm = self.udpc.send(ticket, addr, rcv_timeout=to, max_send=retry)
                                #request_from_lm = self.udpc.send(ticket, addr, rcv_timeout=30)
                                Trace.trace(41, "Request turn around time %s"%(time.time() - t0,))
                                #self.waiting_for_lm_response = 1
                            except:
                                exc, msg, tb = sys.exc_info()
                                if exc == errno.errorcode[errno.ETIMEDOUT]:
                                    x = {'status' : (e_errors.TIMEDOUT, msg)}
                                    if addr == self.udp_control_address:
                                        # break a connection with get
                                        self.nowork({})
                                        if not msg:
                                            msg = "%s sending to %s"%(e_errors.TIMEDOUT, addr)
                                        Trace.log(e_errors.INFO, "ENCP_GONE1 %s"%(msg,)) # remove after fixing ENCP_GONE
                                        self.transfer_failed(e_errors.ENCP_GONE, msg, error_source=NETWORK)
                                        if self.method and self.method == 'read_next':
                                            self.nowork({})
                                            return
                                else:
                                    x = {'status' : (str(exc), str(msg))}
                                Trace.trace(10, "update_lm: got %s" %(x,))
                                continue
                            work = request_from_lm.get('work')
                            if addr == self.udp_control_address:
                                set_cm_sent = 0
                            if not work:
                                continue
                            method = getattr(self, work, None)
                            if method:
                                use_state = 0
                                try:
                                    method(request_from_lm)
                                except:
                                    exc, detail, tb = sys.exc_info()
                                    Trace.handle_error(exc, detail, tb)
                                    Trace.log(e_errors.ERROR,"update_lm: tried %s %s and failed"%
                                              (method,request_from_lm))
                # if work is mover_busy or mover_error
                # send no_wait message
                if (ticket['work'] == 'mover_busy') or (ticket['work'] == 'mover_error'):
                    if ticket['work'] == 'mover_busy' and addr == self.udp_control_address:
                        #do not send mover_busy to get
                        set_cm_sent = 0
                        pass
                    else:
                        Trace.trace(20,"update_lm: send with no wait %s to %s"%(ticket['work'],addr))
                        self.udpc.send_no_wait(ticket, addr)
            if self.method and self.method == 'read_next' and set_cm_sent:
                self.udp_cm_sent = 1
        self.check_dismount_timer()
        Trace.trace(20, "STATE %s udp_sent %s"%(state_name(self.state), self.udp_cm_sent))


    def need_update(self):
        # self.need_lm_update is used in threads to flag LM update in
        # the main thread. First element flags update if not 0,
        # second - state
        # third -  reset timer
        # fourth - error source
        Trace.trace(20," need_update %s"%(self.need_lm_update,))
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
        """
        See if the dismount timer has expired.

        """
        self.lock_state()
        now = time.time()
        if self.state is HAVE_BOUND and self.dismount_time and now>self.dismount_time:
            self.state = DISMOUNT_WAIT
            self.unlock_state()
            Trace.trace(10,"Dismount time expired %s"% (self.current_volume,))
            self.run_in_thread('media_thread', self.dismount_volume, after_function=self.idle)
        else:
            self.unlock_state()

    def send_error_msg(self,error_info=(None,None),error_source=None,returned_work=None):
        """
        Send a single error message to LM - requestor.

        This can be done from any thread.

        :type error_info: :obj:`tuple`
        :arg error_info: error code :obj:`str`, error description :obj:`str`
        :type error_source: :obj:`str`
        :arg error_source: ``TAPE``, ``ROBOT``, ``NETWORK``, ``DRIVE``, ``USER``, ``MOVER``, ``UNKNOWN``
        :type returned_work: :obj:`dict`
        :arg returned_work: work ticket received from library manager

        """

        if self.lm_address != ('none',0): # send error message only to LM that called us
            ticket = self.format_lm_ticket(state=ERROR,
                                           error_info = error_info,
                                           error_source=error_source,
                                           returned_work=returned_work)
            self.udpc.send_no_wait(ticket, self.lm_address)

    def idle(self):
        """
        Set state to `IDLE`.
        """
        if self.state == ERROR:
            return
        if not self.do_eject:
            return
        self.state = IDLE
        self.mode = None
        self.vol_info = {}
        self.file_info = {}
        self.current_volume = None
        self.current_library = None
        self.method = None
        self._error = None
        self._error_source = None

        if hasattr(self,'too_long_in_state_sent'):
            del(self.too_long_in_state_sent)

        thread = threading.currentThread()
        if thread:
            thread_name = thread.getName()
        else:
            thread_name = None
        # if running in the main thread update lm
        if thread_name == 'MainThread':
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
        if thread_name == 'MainThread':
            self.update_lm()
        else: # else just set the update flag
            self.need_lm_update = (1, None, 0, None)


    def reset(self, sanity_cookie, client_crc_on):
        """
        Prepare for data transfer.

        :type sanity_cookie: :obj:`int`
        :arg sanity_cookie: sanity cookie
        :type client_crc_on: :obj:`int`
        :arg client_crc_on: controls crc check
        """

        Trace.trace(22, "reset: client_crc_on %s"%(client_crc_on,))
        self.current_work_ticket = None
        self.init_data_buffer()
        self.buffer.reset(sanity_cookie, client_crc_on)
        self.bytes_read = 0L
        self.bytes_written = 0L

    def return_work_to_lm(self,ticket):
        """
        Return work back to library manager.

        The work was not done.
        """
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
        """
        Read data from the network.

        """

        Trace.trace(8, "read_client starting,  bytes_to_read=%s" % (self.bytes_to_read,))
        driver = self.net_driver
        if self.bytes_read == 0 and self.header: #splice in cpio headers, as if they came from client
            nbytes = self.buffer.header_size
            ##XXX this will fail if nbytes>block_size.
            bytes_read = self.buffer.stream_read(nbytes,self.header)

        #Initialize thresholded transfer notify messages.
        bytes_notified = 0L
        last_notify_time = time.time()
        Trace.notify("transfer %s %s %s network %s %.3f %s" %
                     (self.shortname, self.bytes_read,
                      self.bytes_to_read, self.buffer.nbytes(), time.time(), self.draining))

        while self.state in (ACTIVE,) and self.bytes_read < self.bytes_to_read:
            if self.tr_failed:
                break
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
                #raise exceptions.MemoryError # to test this thread
            except MemoryError:
                exc, detail, tb = sys.exc_info()
                #Trace.handle_error(exc, detail, tb)
                self.transfer_failed(e_errors.MEMORY_ERROR, detail, error_source=NETWORK,dismount_allowed=1)
                return
            except:
                exc, detail, tb = sys.exc_info()
                msg ="ENCP_GONE(2) exc %s detail %s. bytes_read %s"%(exc, detail, bytes_read)
                #Trace.handle_error(exc, detail, tb)
                self.transfer_failed(e_errors.ENCP_GONE, msg, error_source=NETWORK)
                return
            Trace.trace(134, "read_client: bytes read %s"%(bytes_read,))

            if bytes_read <= 0:  #  The client went away!
                if bytes_read == 0:
                    reason = "read timed out"
                else:
                    reason = "dropped connection"
                message = "read_client: %s"%(reason,)
                self.transfer_failed(e_errors.ENCP_GONE, msg=message, error_source=NETWORK)
                return
            self.bytes_read_last = self.bytes_read
            self.bytes_read = self.bytes_read + bytes_read

            if not self.buffer.low():
                self.buffer.write_ok.set()

            #If it is time to do so, send the notify message.
            if is_threshold_passed(self.bytes_read, bytes_notified,
                                   self.bytes_to_read, last_notify_time):
                bytes_notified = self.bytes_read
                last_notify_time = time.time()
                Trace.notify("transfer %s %s %s network %s %.3f %s" %
                             (self.shortname, self.bytes_read,
                              self.bytes_to_read, self.buffer.nbytes(),
                              last_notify_time, self.draining))

        if self.tr_failed:
            driver.close()
            return
        if self.bytes_read == self.bytes_to_read:
            if self.trailer:
                trailer_driver = string_driver.StringDriver(self.trailer)
                trailer_bytes_read = 0
                while trailer_bytes_read < self.buffer.trailer_size:
                    if self.tr_failed:
                        driver.close()
                        break
                    bytes_to_read = self.buffer.trailer_size - trailer_bytes_read
                    bytes_read = self.buffer.stream_read(bytes_to_read, trailer_driver)
                    trailer_bytes_read = trailer_bytes_read + bytes_read
                    Trace.trace(8, "read %s bytes of trailer" % (trailer_bytes_read,))
            if self.tr_failed:
                driver.close()
                return
            self.buffer.eof_read() #pushes last partial block onto the fifo
            self.buffer.write_ok.set()
        self.bytes_read_last = self.bytes_read
        Trace.trace(8, "read_client exiting, read %s/%s bytes" %(self.bytes_read, self.bytes_to_read))


    def position_for_crc_check(self):
        """
        Position tape for crc check.

        """
        Trace.trace(22, "position media")
        Trace.log(e_errors.INFO, "compression %s"%(self.compression,))
        try:
            have_tape = self.tape_driver.open(self.device, self.mode, retry_count=30)
            self.tape_driver.set_mode(compression = self.compression, blocksize = 0)

        except self.ftt.FTTError, detail:
            Trace.alarm(e_errors.ERROR,"Supposedly a serious problem with tape drive while checking a written file: %s %s"%(self.ftt.FTTError, detail))
            self.vcc.set_system_readonly(self.current_volume)
            Trace.alarm(e_errors.ERROR, "Serious FTT error %s on %s. Volume is set readonly"%(detail, self.current_volume))
            self.transfer_failed(e_errors.WRITE_ERROR, "Serious FTT error %s"%(detail,), error_source=DRIVE)
            # also set volume to NOACCESS, so far
            # no alarm is needed here because it is send by volume clerk
            # when it sets a volume to NOACCESS
            self.set_volume_noaccess(self.current_volume, "Write error. See log for details")
            # log all running proceses
            self.log_processes(logit=1)

            return
        try:
            save_location, block = self.tape_driver.tell()
        except self.ftt.FTTError, detail:
            self.transfer_failed(e_errors.WRITE_ERROR, 'Can not get drive info %s' % (detail,),
                                 error_source=TAPE)
            return
        except:
            exc, detail, tb = sys.exc_info()
            self.transfer_failed(e_errors.WRITE_ERROR, 'Can not get drive info %s %s' % (exc, detail,),
                                 error_source=TAPE)
            return

        Trace.trace(22,"save location %s" % (save_location,))
        if have_tape != 1:
            Trace.alarm(e_errors.ERROR, "error positioning tape %s for selective CRC check. Position %s"%
                        (self.current_volume,save_location))

            self.transfer_failed(e_errors.WRITE_ERROR, "error positioning tape for selective CRC check", error_source=DRIVE)
            return
        try:
            location = cookie_to_long(self.vol_info['eod_cookie'])
            if self.header_labels:
                location = location+1
            self.tape_driver.seek(location, 0) #XXX is eot_ok needed?
        except:
            exc, detail, tb = sys.exc_info()
            self.vcc.set_system_readonly(self.current_volume)
            Trace.alarm(e_errors.ERROR, "error positioning tape %s for selective CRC check. Position %s. Volume is set readonly"%
                        (self.current_volume,save_location))
            self.transfer_failed(e_errors.POSITIONING_ERROR, 'positioning error %s' % (detail,), error_source=DRIVE)
            # also set volume to NOACCESS, so far
            # no alarm is needed here because it is send by volume clerk
            # when it sets a volume to NOACCESS
            self.set_volume_noaccess(self.current_volume, "Positioning error. See log for details")
            # log all running proceses
            self.log_processes(logit=1)
            return
        return save_location

    def client_update_enabled(self, ticket):
        """
        This method checks if client code can handle updates from mover.

        This was introduced to avoid client disconnection if crc check takes longer than the client
        wait time (15 min).

        :type ticket: :obj:`dict`
        :arg ticket: ticket received from library manager
        :rtype: :obj:`bool`

        """

        legal_encp_version = "v3_11c" # used for backward compatibility
        rc = False
        c_legal_version = enstore_functions2.convert_version(legal_encp_version)
        if "version" in ticket:
            version=ticket['version'].split()[0]
            c_version = enstore_functions2.convert_version(version)
        else:
            c_version = (0, "")
        if c_version >= c_legal_version:
                rc = True
        Trace.trace(20, "client_update_enabled: legal version %s current version %s return code %s"%
                    (c_legal_version, c_version, rc))

        return rc

    def send_client_update(self):
        """
        Send to client (encp) updated status if selective CRC check runs too long
        to avoid encp timeouts.

        """

        if not self.client_update_enabled(self.current_work_ticket):
            return
        if self.crc_check_percent_completed <= 0:
            # This can happen during rewind.
            # Encp interrupts transfer if it receives the same percentage.
            # To avoid such cases update self.crc_check_percent_completed every time
            # self.crc_check_percent_complete can not be 0. If it is, encp interrupts transfer.

            self.crc_check_percent_completed -= 1
        Trace.trace(20, "send_client_update: percent done %s"%(self.crc_check_percent_completed,))
        self.current_work_ticket['status'] = (e_errors.MOVER_BUSY,  self.crc_check_percent_completed)
        Trace.log(e_errors.INFO, "Sending update to client: %s"%(self.current_work_ticket['status'],))
        try:
            callback.write_tcp_obj(self.control_socket, self.current_work_ticket)
        except:
            exc, detail, tb = sys.exc_info()
            Trace.log(e_errors.ERROR, "error in send_client_update: %s" % (detail,))
            self.encp_gone_during_crc_check = True

    def selective_crc_check(self):
        """
        Read back a file after write, calculate its crc and compare with calculated during transfer.

        """

        failed = 0
        self.encp_gone_during_crc_check = False
        save_location = self.position_for_crc_check()
        if self.tr_failed:
            # if self.position_for_crc_check fails self.tr_failed will be set
            return

        self.buffer.save_settings()
        self.bytes_read = self.bytes_read_last = 0L
        Trace.trace(20,"selective_crc_check: header size %s" % (self.buffer.header_size,))
        bytes_to_read = self.bytes_to_transfer
        header_size = self.buffer.header_size
        # setup buffer for reads
        saved_wrapper = self.buffer.wrapper
        saved_sanity_bytes = self.buffer.sanity_bytes
        saved_complete_crc = self.buffer.complete_crc
        self.buffer.reset((self.buffer.sanity_bytes,
                           self.buffer.sanity_crc),
                          client_crc_on=1) # always calculate crc before writing to tape
        self.buffer.set_wrapper(saved_wrapper)
        Trace.trace(22, "selective_crc_check: starting check after write, bytes_to_read=%s" % (bytes_to_read,))
        driver = self.tape_driver
        first_block = 1
        block_counter = 0
        total_block_counter = 0L
        time_started = time.time()
        last_percent_done = 0
        Trace.log(e_errors.INFO, "crc check starting")

        while self.bytes_read < bytes_to_read:
            if self.encp_gone_during_crc_check:
                Trace.log(e_errors.INFO, "crc check interrupted")
                self.remove_interval_func(self.send_client_update)
                self.transfer_failed(e_errors.ENCP_GONE,  "crc check interrupted", error_source=NETWORK)
                return

            nbytes = min(bytes_to_read - self.bytes_read, self.buffer.blocksize)
            self.buffer.bytes_for_crc = nbytes
            if self.bytes_read == 0 and nbytes<self.buffer.blocksize: #first read, try to read a whole block
                nbytes = self.buffer.blocksize
            try:
                b_read = self.buffer.block_read(nbytes, driver)

                # clean buffer
                #Trace.trace(22,"write_tape: clean buffer")
                self.buffer._writing_block = self.buffer.pull()
                if self.buffer._writing_block:
                    #Trace.trace(22,"write_tape: freeing block")
                    self.buffer._freespace(self.buffer._writing_block)

            except MoverError, detail:
                detail = str(detail)
                if detail == e_errors.CRC_ERROR:
                    Trace.alarm(e_errors.ERROR, "selective CRC check error",
                                {'outfile':self.current_work_ticket['outfile'],
                                 'infile':self.current_work_ticket['infile'],
                                 'external_label':self.current_work_ticket['vc']['external_label']})
                    self.transfer_failed(e_errors.WRITE_ERROR, detail, error_source=DRIVE)
                else:
                    if detail == e_errors.ENCP_GONE:
                        err_source = NETWORK
                    else:
                        err_source = UNKNOWN
                    self.transfer_failed(detail, error_source=err_source)
                failed = 1
                break
            except:
                exc, detail, tb = sys.exc_info()
                #Trace.handle_error(exc, detail, tb)
                Trace.alarm(e_errors.ERROR, "selective CRC check error",
                            {'outfile':self.current_work_ticket['outfile'],
                             'infile':self.current_work_ticket['infile'],
                             'external_label':self.current_work_ticket['vc']['external_label']})
                self.transfer_failed(e_errors.WRITE_ERROR, detail, error_source=DRIVE)
                failed = 1
                break
            if b_read <= 0:
                Trace.alarm(e_errors.ERROR, "selective CRC check read error")
                self.transfer_failed(e_errors.WRITE_ERROR, "read returns %s" % (self.bytes_read,),
                                     error_source=DRIVE)
                failed = 1
                break
            if first_block:
                bytes_to_read = bytes_to_read + header_size
                first_block = 0
            self.bytes_read_last = self.bytes_read
            self.bytes_read = self.bytes_read + b_read
            if self.bytes_read > bytes_to_read: #this is OK, we read a cpio trailer or something
                self.bytes_read = bytes_to_read

            total_block_counter += 1
            if block_counter == 2000:
                self.crc_check_percent_completed = int(self.bytes_read*100 / self.bytes_to_write)
                if self.crc_check_percent_completed - last_percent_done >= 5:
                    Trace.notify("transfer %s %s %s media %s %.3f %s" %
                                 (self.shortname, -self.bytes_read,
                                  bytes_to_read, self.buffer.nbytes(), time.time(), self.draining))

                    last_percent_done = self.crc_check_percent_completed
                block_counter = 0
            else:
                block_counter += 1
            Trace.trace(22, "selective_crc_check: bytes_read %s bytes_read_last %s"%(self.bytes_read, self.bytes_read_last))
        # end while
        Trace.trace(22,"selective_crc_check: total blocks %s"%(total_block_counter,))

        Trace.trace(22,"write_tape: read CRC %s write CRC %s"%
                    (self.buffer.complete_crc, saved_complete_crc))
        if failed:
            self.vcc.set_system_readonly(self.current_volume)
            Trace.alarm(e_errors.ERROR, "Write error on %s. See log for details. Volume is set readonly"%(self.current_volume,))
            # also set volume to NOACCESS, so far
            # no alarm is needed here because it is send by volume clerk
            # when it sets a volume to NOACCESS
            self.set_volume_noaccess(self.current_volume, "Write error. See log for details")
            # log all running proceses
            self.log_processes(logit=1)
            return
        if self.buffer.complete_crc != saved_complete_crc:
            self.vcc.set_system_readonly(self.current_volume)
            Trace.alarm(e_errors.ERROR,
                        "selective CRC check error on %s. See log for details. Volume is set readonly"%
                        (self.current_volume,))
            self.transfer_failed(e_errors.WRITE_ERROR, "selective CRC check error",error_source=DRIVE)
            return
        Trace.log(e_errors.INFO,
                  "selective CRC check after writing file completed successfuly")
        self.buffer.restore_settings()
        # position to eod"
        try:
            self.tape_driver.seek(save_location, 0) #XXX is eot_ok
        except:
            exc, detail, tb = sys.exc_info()
            Trace.alarm(e_errors.ERROR,
                        "error positioning tape %s after selective CRC check. Position %s"%
                        (self.current_volume,save_location))
            self.vcc.set_system_readonly(self.current_volume)
            self.transfer_failed(e_errors.POSITIONING_ERROR,
                                 'positioning error %s. See log for details. Volume is set readonly' %
                                 (detail,), error_source=DRIVE)
            # also set volume to NOACCESS, so far
            # no alarm is needed here because it is send by volume clerk
            # when it sets a volume to NOACCESS
            self.set_volume_noaccess(self.current_volume, "Positioning error. See log for details")
            # log all running proceses
            self.log_processes(logit=1)
            return

        read_errors_1 = self.read_errors # save read error count returned after write
        write_errors_1 = self.write_errors # save read error count returned after write
        self.read_errors = -1
        self.write_errors = -1

        self.update_tape_stats()
        if self.tr_failed:
            return

        if self.read_errors > 0:
            self.read_errors = self.read_errors - read_errors_1 # number of read error per current file
        if self.write_errors > 0:
            self.write_errors = self.write_errors - write_errors_1 # number of read error per current file

        #if self.buffer.read_stats[4] > 0.:
        #    rates = bytes_read/self.buffer.read_stats[4]
        if self.buffer.read_stats[1] > 0.:
            rates = self.bytes_written/self.buffer.read_stats[1] # read rates are file_size / time_in_driver.read
        else:
            rates = 0.
        Trace.log(e_errors.INFO, 'drive stats after crc check. Tape %s block %s block_size %s bloc_loc %s tot_blocks %s BOT %s read_err %s write_err %s bytes %s block_read_tot %s tape_rate %s'%
                  (self.current_volume,
                   self.block_n,
                   self.block_size,
                   self.bloc_loc,
                   self.tot_blocks,
                   self.bot,
                   self.read_errors,
                   self.write_errors,
                   self.bytes_read,
                   self.buffer.read_stats[4],
                   rates))

    def write_tape(self):
        """
        Write file to tape.

        """

        Trace.log(e_errors.INFO, "write_tape starting, bytes_to_write=%s" % (self.bytes_to_write,))
        Trace.trace(8, "bytes_to_transfer=%s" % (self.bytes_to_transfer,))
        driver = self.tape_driver
        if self.config['product_id'] in ("T10000C", "T10000D") and self.compression:
            # special code for setting compression for T10000C tape drives
            rc = scsi_mode_select.t10000_set_compression(driver, compression = True)
            if not rc:
                Trace.log(e_errors.ERROR, "Compression setting failed")
        count = 0
        defer_write = 1
        failed = 0
        self.bloc_loc = 0L
        self.media_transfer_time = 0.
        idle_time = 0. # accumulative time when not writing

        if self.driver_type == 'FTTDriver':
            self.update_tape_stats()
            if self.tr_failed:
                return
            read_errors_0 = self.read_errors
            write_errors_0 = self.write_errors

        Trace.log(e_errors.INFO, 'Write starting. Tape %s absolute location in blocks %s'%(self.current_volume, self.bloc_loc))

        if self.driver_type == 'FTTDriver':
            if self.write_counter == 0: # this is a first write since tape has been mounted
                self.initial_abslute_location = self.bloc_loc
                self.current_absolute_location = self.initial_abslute_location
                self.last_absolute_location = self.current_absolute_location
                self.last_blocks_written = 0L
                if self.initial_abslute_location == 0L:
                    # tape is at BOT
                    # something wrong with positioning.
                    self.transfer_failed(e_errors.WRITE_ERROR, "Tape %s at BOT, can not write"%(self.current_volume,), error_source=TAPE)
                    self.set_volume_noaccess(self.current_volume, "Tape is at BOT, can not write")
                    return
            else:
                Trace.trace(31, "cur %s, initial %s, last %s"%(self.bloc_loc, self.initial_abslute_location, self.last_absolute_location))
                if (self.bloc_loc <= self.initial_abslute_location) or (self.bloc_loc != self.last_absolute_location):
                    Trace.alarm(e_errors.ERROR, "Write error on %s. Wrong position. See log for details"%(self.current_volume,))
                    self.log_processes(logit=1)
                    self.transfer_failed(e_errors.WRITE_ERROR,
                                         "Wrong position for %s: initial %s, last %s, current %s, last written blocks %s"%
                                         (self.current_volume,
                                          self.initial_abslute_location,
                                          self.last_absolute_location,
                                          self.bloc_loc,
                                          self.last_blocks_written),error_source=TAPE)

                    self.set_volume_noaccess(self.current_volume, "Wrong position. See log for details")
                    return

                self.current_absolute_location = self.bloc_loc


        buffer_empty_t = time.time()   #time when buffer empty has been detected
        buffer_empty_cnt = 0 # number of times buffer was consequtively empty
        nblocks = 0L
        bytes_written = 0
        # send a trigger message to the client
        try:
            bytes_written = self.net_driver.write("B", # write anything
                                                  0,
                                                  1) # just 1 byte
        except generic_driver.DriverError, detail:
            Trace.log(e_errors.INFO, "ENCP_GONE3 detail %s. bytes_written %s"%(detail, bytes_written)) # remove after fixing ENCP_GONE
            self.transfer_failed(e_errors.ENCP_GONE, detail, error_source=NETWORK)
            return
        except:
            exc, detail = sys.exc_info()[:2]
            Trace.log(e_errors.INFO, "ENCP_GONE4 exc %s detail %s. bytes_written %s"%(exc, detail, bytes_written)) # remove after fixing ENCP_GONE
            self.transfer_failed(e_errors.ENCP_GONE, detail, error_source=NETWORK)
            return

        self.write_in_progress = True # set it here
        if self.header_labels:
            t1 = time.time()

            try:
                bytes_written = driver.write(self.header_labels, 0, len(self.header_labels))
            except:
                exc, detail, tb = sys.exc_info()
                #Trace.handle_error(exc, detail, tb)
                # bail out gracefuly

                # set volume to readlonly
                self.vcc.set_system_readonly(self.current_volume)
                Trace.alarm(e_errors.ERROR, "Write error on %s detail %s exception %s. Volume is set readonly" %
                            (self.current_volume,detail, exc, ))
                # also set volume to NOACCESS, so far
                # no alarm is needed here because it is send by volume clerk
                # when it sets a volume to NOACCESS
                self.set_volume_noaccess(self.current_volume, "Write Error. See log for details")
                # log all running proceses
                self.log_processes(logit=1)
                # trick ftt_close, so that it does not attempt to write FM
                if self.driver_type == 'FTTDriver':
                    import ftt
                    self.ftt._ftt.ftt_set_last_operation(self.tape_driver.ftt.d, 0)
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
        last_notify_time = time.time()
        Trace.notify("transfer %s %s %s media %s %.3f %s" %
                     (self.shortname, self.bytes_written,
                      self.bytes_to_write, self.buffer.nbytes(), time.time(), self.draining))

        while self.state in (ACTIVE, ) and self.bytes_written<self.bytes_to_write:
            loop_start = time.time()

            Trace.trace(133,"total_bytes %s total_bytes_written %s"%(self.bytes_to_write, self.bytes_written))
            if self.tr_failed:
                self.tape_driver.flush() # to empty buffer and to release device from this thread
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
                    #buffer_empty_t = time.time()
                    defer_write = 1
                Trace.trace(9,"write_tape: buffer low %s/%s, wrote %s/%s, defer=%s empty=%s"%
                            (self.buffer.nbytes(), self.buffer.min_bytes,
                             self.bytes_written, self.bytes_to_write,
                             defer_write, empty))
                self.buffer.write_ok.clear()
                self.buffer.write_ok.wait(1)
                now = time.time()
                if (int(now - buffer_empty_t) > self.max_time_in_state) and int(buffer_empty_t) > 0:
                    if not hasattr(self,'too_long_in_state_sent'):
                        if not self.state == ERROR:
                            Trace.alarm(e_errors.WARNING, "Too long in state %s for %s. Client host %s" %
                                        (state_name(self.state),self.current_volume, self.current_work_ticket['wrapper']['machine'][1]))
                            #Trace.trace(9, "now %s t %s max %s"%(now, buffer_empty_t,self.max_time_in_state))
                            Trace.log(e_errors.INFO, "write:now %s t %s max %s empty %s defer %s br %s btr %s"%(now, buffer_empty_t,self.max_time_in_state, empty, defer_write,self.bytes_read, self.bytes_to_read)) #!!! REMOVE WHEN PROBLEM is fixed
                            self.too_long_in_state_sent = 0 # send alarm just once
                        else:
                            return
                    Trace.log(e_errors.INFO, "write:now %s t %s max %s empty %s defer %s br %s btr %s"%
                              (now, buffer_empty_t,
                               self.max_time_in_state, empty,
                               defer_write,self.bytes_read, self.bytes_to_read)) #!!! REMOVE WHEN PROBLEM is fixed
                    buffer_empty_t = now
                    Trace.trace(9, "buf empty cnt %s max %s"%(buffer_empty_cnt, self.max_in_state_cnt))
                    if buffer_empty_cnt >= self.max_in_state_cnt:
                        msg = "data transfer from client stuck. Client host %s. Breaking connection"%(self.current_work_ticket['wrapper']['machine'][1],)
                        self.tape_driver.flush() # to empty buffer and to release device from this thread
                        ## AM: DO NOT REWIND TAPE
                        ## do not rewind tape
                        ##Trace.log(e_errors.INFO, "To avoid potential data overwriting will rewind tape");
                        ##self.tape_driver.rewind()
                        ##self.current_location = 0L
                        ##self.rewind_tape = 0
                        ##------------------------------------
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
                        # if network rate < taperate
                        # increase the water mark
                        # otherwise set it to self.min_buffer
                        ratio = netrate/(taperate*1.0)
                        optimal_buf = self.bytes_to_transfer * (1-ratio)
                        optimal_buf = min(optimal_buf, 0.5 * self.max_buffer)
                        optimal_buf = max(optimal_buf, self.min_buffer)
                        optimal_buf = int(optimal_buf)
                        Trace.trace(112,"netrate = %.3g, taperate=%.3g" % (netrate, taperate))
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
                Trace.trace(133,"bytes_to_write %s bytes_written %s"%(nbytes,bytes_written))
            except:
                exc, detail, tb = sys.exc_info()
                #Trace.handle_error(exc, detail, tb)
                # bail out gracefuly
                if str(detail) == 'FTT_ENOSPC':
                    # no space left on tape
                    Trace.log(e_errors.INFO, "No sace left on %s. Setting to full"%(self.current_volume,))
                    ret = self.vcc.set_remaining_bytes(self.current_volume, 0, self.vol_info['eod_cookie'])
                    if ret['status'][0] != e_errors.OK or ret['eod_cookie'] != self.vol_info['eod_cookie']:
                        Trace.alarm(e_errors.ERROR, "set_remaining_bytes failed", ret)
                        self.set_volume_noaccess(self.current_volume, "Failed to update volume information. See log for details")
                        Trace.alarm(e_errors.ALARM, "Failed to update volume information on %s, EOD %s. May cause a data loss."%
                                    (self.current_volume, self.vol_info['eod_cookie']))
                else:
                    # set volume to readlonly
                    self.vcc.set_system_readonly(self.current_volume)
                    Trace.alarm(e_errors.ERROR, "Write error on %s. Volume is set readonly" %
                                (self.current_volume,))
                    # also set volume to NOACCESS, so far
                    # no alarm is needed here because it is send by volume clerk
                    # when it sets a volume to NOACCESS
                    self.set_volume_noaccess(self.current_volume, "Write error. See log for details")
                    # log all running proceses
                    self.log_processes(logit=1)

                # trick ftt_close, so that it does not attempt to write FM
                if self.driver_type == 'FTTDriver':
                    import ftt
                    self.ftt._ftt.ftt_set_last_operation(self.tape_driver.ftt.d, 0)
                #initiate cleaning
                self.force_clean = 1
                self.transfer_failed(e_errors.WRITE_ERROR, detail, error_source=TAPE)
                failed = 1
                break
            t2 = time.time()
            self.media_transfer_time = self.media_transfer_time + (t2-t1)
            idle_time = idle_time + t1 - loop_start
            if bytes_written != nbytes:
                self.transfer_failed(e_errors.WRITE_ERROR, "short write %s != %s" %
                                     (bytes_written, nbytes), error_source=TAPE)
                failed = 1
                break
            self.bytes_written = self.bytes_written + bytes_written

            #If it is time to do so, send the notify message.
            if is_threshold_passed(self.bytes_written, bytes_notified,
                                   self.bytes_to_write, last_notify_time):
                bytes_notified = self.bytes_written
                last_notify_time = time.time()
                Trace.notify("transfer %s %s %s media %s %.3f %s" %
                             (self.shortname, self.bytes_written,
                              self.bytes_to_write, self.buffer.nbytes(),
                              last_notify_time, self.draining))

            if not self.buffer.full():
                self.buffer.read_ok.set()
        if self.tr_failed:
            Trace.trace(27,"write_tape: write interrupted transfer failed")
            self.tape_driver.flush() # to empty buffer and to release devivice from this thread
            ## AM: DO NOT REWIND TAPE
            ##if self.rewind_tape:
            ##    Trace.log(e_errors.INFO, "To avoid potential data overwriting will rewind tape");
            ##    self.tape_driver.rewind()
            ##    self.current_location = 0L
            ##    self.rewind_tape = 0

            return

        Trace.log(e_errors.INFO, "written bytes %s/%s, blocks %s header %s trailer %s" %( self.bytes_written, self.bytes_to_write, nblocks, len(self.header), len(self.trailer)))

        if failed:
            rc = self.tape_driver.flush() # to empty buffer and to release device from this thread
            if rc[0] == -1:
               self.transfer_failed(e_errors.WRITE_ERROR, "Serious FTT error %s"%(rc[1],), error_source=DRIVE)
            return

        t1 = time.time()
        #
        if self.tr_failed:
            Trace.log(e_errors.ERROR,"Transfer failed just before writing file marks")
            self.tape_driver.flush() # to empty buffer and to release devivice from this thread
            ## AM: DO NOT REWIND TAPE
            ##if self.rewind_tape:
            ##    Trace.log(e_errors.INFO, "To avoid potential data overwriting will rewind tape");
            ##    self.tape_driver.rewind()
            ##    self.current_location = 0L
            ##    self.rewind_tape = 0
            return

        if self.bytes_written == self.bytes_to_write:
            try:
                ##We don't ever want to let ftt handle the filemarks for us, because its
                ##default behavior is to write 2 filemarks and backspace over both
                ##of them.
                self.eof_labels = self.wrapper.eof_labels(self.buffer.complete_crc)
                if self.single_filemark or self.eof_labels:
                    Trace.trace(23, "single fm %s eof labels %s"%(self.single_filemark, self.eof_labels))
                    Trace.trace(23, "write fm")
                    self.tape_driver.writefm()
                else:
                    Trace.trace(23, "write fm")
                    self.tape_driver.writefm()
                    Trace.trace(23, "write fm")
                    self.tape_driver.writefm()
                    Trace.trace(23, "skip fm -1")
                    self.tape_driver.skipfm(-1)
                    Trace.trace(23, "fm done")
                Trace.trace(10, "complete CRC %s"%(self.buffer.complete_crc,))
                self.eof_labels = self.wrapper.eof_labels(self.buffer.complete_crc)
                if self.eof_labels:
                    bytes_written = driver.write(self.eof_labels, 0, len(self.eof_labels))
                    if bytes_written != len(self.eof_labels):
                        self.vcc.set_system_readonly(self.current_volume)
                        self.transfer_failed(e_errors.WRITE_ERROR, "short write %s != %s" %
                                             (bytes_written, len(self.eof_labels)), error_source=TAPE)

                        Trace.alarm(e_errors.ERROR, "Short write on %s. Volume is set readonly. See log for details"%(self.current_volume,))
                        # also set volume to NOACCESS, so far
                        # no alarm is needed here because it is send by volume clerk
                        # when it sets a volume to NOACCESS
                        self.set_volume_noaccess(self.current_volume, "Short write. See log for details")
                        # log all running proceses
                        self.log_processes(logit=1)
                        return
                    Trace.trace(23, "write fm")
                    self.tape_driver.writefm()
                    if not self.single_filemark:
                        Trace.trace(5, "single fm %s"%(self.single_filemark,))
                        Trace.trace(23, "write fm")
                        self.tape_driver.writefm()
                        Trace.trace(23, "skip fm -1")
                        self.tape_driver.skipfm(-1)
                        Trace.trace(23, "fm done")
                # get location info before calling tape_driver.flush() as it will clear stats
                self.last_blocks_written = nblocks
                new_bloc_loc = 0L
                if self.driver_type == 'FTTDriver':
                    self.read_errors = -1
                    self.write_errors = -1
                    self.update_tape_stats()
                    if self.tr_failed:
                        return
                    new_bloc_loc = self.bloc_loc
                    if self.read_errors > 0:
                       self.read_errors = self.read_errors - read_errors_0 # number of read error per current file
                    if self.write_errors > 0:
                       self.write_errors = self.write_errors - write_errors_0 # number of read error per current file

                    Trace.log(e_errors.INFO, 'filemarks written. Tape %s absolute location in blocks %s'%(self.current_volume, new_bloc_loc,))
                    Trace.log(e_errors.INFO, "write_tape timing:pull %s write %s crc_check %s freespace %s block_write %s idle %s" %
                              (self.buffer.write_stats[0],
                               self.buffer.write_stats[1],
                               self.buffer.write_stats[2],
                               self.buffer.write_stats[3],
                               self.buffer.write_stats[4],
                               idle_time))

                    #if self.buffer.write_stats[4] > 0.:
                    #    rates = self.bytes_written/self.buffer.write_stats[4]
                    if self.buffer.write_stats[1] > 0.:
                        rates = self.bytes_written/self.buffer.write_stats[1] # write rates are file_size / time_in_driver.write
                    else:
                        rates = 0.
                    Trace.log(e_errors.INFO, 'drive stats after write. Tape %s position %s block %s block_size %s bloc_loc %s tot_blocks %s BOT %s read_err %s write_err %s bytes %s block_write_tot %s tape_rate %s'%
                              (self.current_volume,
                               self.current_location,
                               self.block_n,
                               self.block_size,
                               new_bloc_loc,
                               self.tot_blocks,
                               self.bot,
                               self.read_errors,
                               self.write_errors,
                               self.bytes_written,
                               self.buffer.write_stats[4],
                               rates))


                    self.tape_driver.flush()
                    self.media_transfer_time = self.media_transfer_time + (time.time()-t1) # include filemarks into drive time
                    Trace.trace(31, "cur %s, initial %s, last %s, blocks %s, headers %s trailers %s"%(new_bloc_loc, self.initial_abslute_location, self.current_absolute_location,self.last_blocks_written, len(self.header_labels), len(self.eof_labels)))

                if self.header_labels and self.eof_labels:
                    extra = 4
                else:
                    extra = 0
                if self.driver_type == 'FTTDriver':
                    if new_bloc_loc != self.current_absolute_location+self.last_blocks_written+1+extra:
                        self.transfer_failed(e_errors.WRITE_ERROR, "Wrong position on %s: before write %s after write %s, blocks written+1 %s headers %s trailers %s"%
                                             (self.current_volume,
                                              self.current_absolute_location,
                                              new_bloc_loc, self.last_blocks_written+1,
                                              len(self.header_labels),
                                              len(self.eof_labels)),
                                             error_source=TAPE,
                                             dismount_allowed=0)
                        Trace.alarm(e_errors.ALARM, "Wrong position on %s: before write %s after write %s, blocks written+1 %s headers %s trailers %s. Mover will be set to OFFLINE and tape to NOACCESS for the investigation"%
                                    (self.current_volume,
                                     self.current_absolute_location,
                                     new_bloc_loc, self.last_blocks_written+1,
                                     len(self.header_labels),
                                     len(self.eof_labels)))
                        self.set_volume_noaccess(self.current_volume, "Wrong position. See log for details")
                        self.offline()
                        return
                    self.last_absolute_location = new_bloc_loc


            except:
                exc, detail, tb = sys.exc_info()
                self.vcc.set_system_readonly(self.current_volume)
                Trace.alarm(e_errors.ERROR, "Write error on %s. Volume is set readonly. See log for details"%(self.current_volume,))
                Trace.handle_error(exc, detail, tb)
                self.transfer_failed(e_errors.WRITE_ERROR, detail, error_source=TAPE)
                # also set volume to NOACCESS, so far
                # no alarm is needed here because it is send by volume clerk
                # when it sets a volume to NOACCESS
                self.set_volume_noaccess(self.current_volume, "Write error. See log for details")
                # log all running proceses
                self.log_processes(logit=1)
                return


            if self.check_written_file() and self.driver_type == 'FTTDriver':
                Trace.log(e_errors.INFO, "selective CRC check after writing file")
                self.crc_check_percent_completed = 0
                self.add_interval_func(self.send_client_update,  14*60) # send more often than encp retry interval (15 min)
                self.selective_crc_check()
                self.remove_interval_func(self.send_client_update)
                if self.tr_failed:
                    return

            if self.update_after_writing():
                self.files_written_cnt = self.files_written_cnt + 1
                self.bytes_written_last = self.bytes_written
                if self.driver_type == 'FTTDriver':
                    self.check_drive_rate(self.current_work_ticket['fc'].get('bfid',None),
                                          self.bytes_written,
                                          rates, 'w',
                                          self.read_errors, self.write_errors, self.stats)

                self.transfer_completed()
                self.write_counter =  self.write_counter + 1 # successful write was done
                self.write_in_progress = False

            else:
                self.transfer_failed(e_errors.EPROTO)

    def read_tape(self):
        """
        Read file from tape.

        """
        self.read_tape_done.clear() # use this to synchronize read and network threads

        if self.driver_type == 'FTTDriver':
            self.update_tape_stats()
            if self.tr_failed:
                return
            read_errors_0 = self.read_errors
            write_errors_0 = self.write_errors

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
        buffer_full_cnt = 0 # number of times buffer was consequtively full
        nblocks = 0
        header_size = 0 # to avoit a silly exception
        #Initialize thresholded transfer notify messages.
        bytes_notified = 0L
        last_notify_time = time.time()
        Trace.notify("transfer %s %s %s media %s %.3f %s" %
                     (self.shortname, -self.bytes_read,
                      self.bytes_to_read, self.buffer.nbytes(), time.time(), self.draining))

        Trace.trace(24, "state %s read %s to_read %s"%(state_name(self.state),self.bytes_read, self.bytes_to_read))
        t_started = time.time()
        idle_time = 0. # accumulative time when not reading
        break_here = 0
        network_slow = False

        while self.state in (ACTIVE, ) and self.bytes_read < self.bytes_to_read:
            loop_start = time.time()
            Trace.trace(133,"total_bytes_to_read %s total_bytes_read %s"%(self.bytes_to_read, self.bytes_read))
            Trace.trace(127,"read_tape: tr_failed %s"%(self.tr_failed,))
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
                Trace.trace(9, "read_tape: time in state %s max %s, bf %s sent %s bf_c %s m_c %s" %
                            (int(now - buffer_full_t), self.max_time_in_state,
                             int(buffer_full_t),hasattr(self,'too_long_in_state_sent'),
                             buffer_full_cnt, self.max_in_state_cnt))

                if self.time_in_state > self.exp_time_factor * self.expected_transfer_time:
                    # expected transfer time is the file size / drive rate
                    # drive rate is comparable with network rate
                    # factor of 10 should be enough yet reasonable
                    network_slow = True

                if (((int(now - buffer_full_t) > self.max_time_in_state) and int(buffer_full_t) > 0) or
                    network_slow):
                    if not hasattr(self,'too_long_in_state_sent'):
                        if not self.state == ERROR:
                            Trace.alarm(e_errors.WARNING, "Too long in state %s for %s. Client host %s" %
                                        (state_name(self.state),self.current_volume, self.current_work_ticket['wrapper']['machine'][1]))
                            self.too_long_in_state_sent = 0 # send alarm just once
                        else:
                            return
                    buffer_full_t = now
                    if (buffer_full_cnt >= self.max_in_state_cnt) or network_slow:
                        msg = "data transfer to client stuck. Client host %s. Breaking connection"%(self.current_work_ticket['wrapper']['machine'][1],)
                        self.read_tape_done.set()

                        self.transfer_failed(e_errors.ENCP_STUCK, msg, error_source=NETWORK)
                        return
                    buffer_full_cnt = buffer_full_cnt + 1
                continue
            else:
                buffer_full_t = 0
                buffer_full_cnt = 0

            Trace.trace(124, "btr %s br %s bs %s"%(self.bytes_to_read, self.bytes_read, self.buffer.blocksize))
            nbytes = min(self.bytes_to_read - self.bytes_read, self.buffer.blocksize)
            self.buffer.bytes_for_crc = nbytes
            if self.bytes_read == 0 and nbytes<self.buffer.blocksize: #first read, try to read a whole block
                nbytes = self.buffer.blocksize

            bytes_read = 0
            try:
                t1 = time.time()
                bytes_read = self.buffer.block_read(nbytes, driver)
                nblocks = nblocks + 1
                self.media_transfer_time = self.media_transfer_time + (time.time()-t1)
                idle_time = idle_time + t1 - loop_start
                Trace.trace(133,"bytes to read %s, bytes read %s"%(nbytes, bytes_read))
            except MemoryError:
                #raise exceptions.MemoryError # to test this thread
                exc, detail, tb = sys.exc_info()
                #Trace.handle_error(exc, detail, tb)
                self.transfer_failed(e_errors.MEMORY_ERROR, detail, error_source=MOVER,dismount_allowed=1)
                return
            except MoverError, detail:
                detail = str(detail)
                if detail == e_errors.CRC_ERROR:
                    Trace.alarm(e_errors.ERROR, "CRC error reading tape",
                                {'outfile':self.current_work_ticket['outfile'],
                                 'infile':self.current_work_ticket['infile'],
                                 'location_cookie':self.current_work_ticket['fc']['location_cookie'],
                                 'external_label':self.current_work_ticket['vc']['external_label']})
                    self.transfer_failed(e_errors.CRC_ERROR, error_source=TAPE)
                else:
                    if detail == e_errors.ENCP_GONE:
                        err_source = NETWORK
                    else:
                        err_source = UNKNOWN
                    self.transfer_failed(detail, error_source=err_source)

                self.read_tape_done.set()
                failed = 1
                Trace.trace(24, "MODE %s"%(mode_name(self.mode),))
                if self.mode == ASSERT:
                    self.assert_return = e_errors.CRC_ERROR
                    return
                break
            except self.ftt.FTTError, detail:
                if type(detail) != type(""):
                    detail = str(detail)
                if self.method == 'read_next':
                    prev_loc = self.current_location
                    self.current_location, block = self.tape_driver.tell()
                    if detail == 'FTT_SUCCESS':
                        if self.current_location - prev_loc == 1:
                            break_here = 1
                            Trace.log(e_errors.INFO, "hit EOF while reading tape. Current Location %s Previous location %s"%
                                      (self.current_location, prev_loc))
                            if self.bytes_read == 0 and bytes_read == 0:
                                #End of tape
                                Trace.log(e_errors.INFO, "hit EOT while reading tape. Current Location %s Previous location %s"%
                                          (self.current_location, prev_loc))
                                #self.transfer_failed(e_errors.READ_ERROR, e_errors.READ_NODATA, error_source=TAPE)
                                #self.send_error_msg(error_info=(e_errors.READ_ERROR, e_errors.READ_NODATA), error_source=TAPE)
                                #self.current_work_ticket['status'] = (e_errors.READ_ERROR, e_errors.READ_NODATA)
                                #failed = 1
                                break
                    elif detail == 'FTT_EBLANK':
                        Trace.log(e_errors.INFO, "perhaps EOT.Current Location %s Previous location %s"%
                                  (self.current_location, prev_loc))
                        self.read_tape_done.set()
                        self.transfer_failed(e_errors.READ_ERROR, e_errors.READ_EOD, error_source=TAPE)
                        failed = 1
                        break

                    else:
                        Trace.log(e_errors.ERROR, "Read failed. Current Location %s Previous location %s"%
                                  (self.current_location, prev_loc))
                        self.read_tape_done.set()

                        self.transfer_failed(e_errors.READ_ERROR, detail, error_source=TAPE)
                        failed = 1
                        break

                else:
                    Trace.trace(33,"Exception %s %s"%(e_errors.READ_ERROR,str(detail)))
                    self.read_tape_done.set()

                    self.transfer_failed(e_errors.READ_ERROR, detail, error_source=TAPE)
                    failed = 1
                    break
            except:
                exc, detail,tb = sys.exc_info()
                Trace.trace(33,"Exception %s %s"%(str(exc),str(detail)))
                Trace.handle_error(exc, detail, tb)
                self.read_tape_done.set()
                self.transfer_failed(e_errors.READ_ERROR, detail, error_source=TAPE)
                failed = 1
                break
            if bytes_read <= 0:
                Trace.trace(98, "method %s bytes %s"%(self.method,bytes_read))
                if bytes_read == 0 and self.method == 'read_next':
                    pass
                else:
                    self.read_tape_done.set()
                    self.transfer_failed(e_errors.READ_ERROR, "read returns %s" % (bytes_read,),
                                         error_source=TAPE)
                    failed = 1
                    break
            if self.bytes_read==0: #Handle variable-sized cpio header
                if len(self.buffer._buf) != 1:
                    Trace.log(e_errors.ERROR,
                              "read_tape: error skipping over cpio header, len(buf)=%s"%(len(self.buffer._buf)))
                try:
                    b0 = self.buffer._buf[0]
                except IndexError, detail:
                    self.transfer_failed(e_errors.READ_ERROR, "%s"%(detail,), error_source=TAPE)
                    failed = 1
                    break
                if len(b0) >= self.wrapper.min_header_size:
                    try:
                        header_size = self.wrapper.header_size(b0)
                    except (TypeError, ValueError), msg:
                        Trace.log(e_errors.ERROR,"Invalid header %s" %(b0[:self.wrapper.min_header_size]))
                        self.read_tape_done.set()
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
                                   self.bytes_to_read, last_notify_time):
                bytes_notified = self.bytes_read
                last_notify_time = time.time()
                Trace.notify("transfer %s %s %s media %s %.3f %s" %
                             (self.shortname, -self.bytes_read,
                              self.bytes_to_read, self.buffer.nbytes(),
                              last_notify_time, self.draining))

            if not self.buffer.empty():
                Trace.trace(199, "write_ok_set")
                self.buffer.write_ok.set()
            if bytes_read < nbytes and self.method == 'read_next':
                # end of file?
                if bytes_read == 0:
                    try:
                        location, block = self.tape_driver.tell()
                    except self.ftt.FTTError, detail:
                        self.transfer_failed(e_errors.READ_ERROR, 'Can not get drive info %s' % (detail,),
                                             error_source=TAPE)
                        return
                    except:
                        exc, detail, tb = sys.exc_info()
                        self.transfer_failed(e_errors.READ_ERROR, 'Can not get drive info %s %s' % (exc, detail,),
                                             error_source=TAPE)
                        return

                    Trace.log(e_errors.INFO, "location %s block %s cur_loc %s"%(location, block, self.current_location))
                    break_here = 1
                    break

            if break_here:
                break


        Trace.log(e_errors.INFO, "read bytes %s/%s, blocks %s header %s" %(self.bytes_read, self.bytes_to_read, nblocks, header_size))
        try:
            location, block = self.tape_driver.tell()
        except self.ftt.FTTError, detail:
            self.transfer_failed(e_errors.READ_ERROR, 'Can not get drive info %s' % (detail,),
                                 error_source=TAPE)
            return
        except:
            exc, detail, tb = sys.exc_info()
            self.transfer_failed(e_errors.READ_ERROR, 'Can not get drive info %s %s' % (exc, detail,),
                                 error_source=TAPE)
            return

        block_n = tot_blocks = bloc_loc = block_size = bot = 0L
        if self.driver_type == 'FTTDriver':
            self.read_errors = -1
            self.write_errors = -1
            self.update_tape_stats()
            if self.tr_failed:
                return

            if self.read_errors > 0:
                self.read_errors = self.read_errors - read_errors_0 # number of read error per current file
            if self.write_errors > 0:
                self.write_errors = self.write_errors - write_errors_0 # number of read error per current file


            #if self.buffer.read_stats[4] > 0.:
            #    rates = self.bytes_read/self.buffer.read_stats[4]
            if self.buffer.read_stats[1] > 0.:
                rates = self.bytes_read/self.buffer.read_stats[1] # read rates are file_size / time_in_driver.read
            else:
                rates = 0.
            Trace.log(e_errors.INFO, 'drive stats after read. Tape %s position %s block %s block_size %s bloc_loc %s tot_blocks %s BOT %s read_err %s write_err %s bytes %s block_read_tot %s tape_rate %s'%
                      (self.current_volume,
                       location,
                       self.block_n,
                       self.block_size,
                       self.bloc_loc,
                       self.tot_blocks,
                       self.bot,
                       self.read_errors,
                       self.write_errors,
                       self.bytes_read,
                       self.buffer.read_stats[4],
                       rates))
            self.check_drive_rate(self.file_info.get('bfid',None),
                                  self.bytes_read,
                                  rates, 'r',
                                  self.read_errors, self.write_errors, self.stats)

        if break_here and self.method == 'read_next':
            self.bytes_to_write = self.bytes_read # set correct size for bytes to write
        if self.tr_failed:
            self.read_tape_done.set()
            Trace.trace(127,"read_tape: tr_failed %s"%(self.tr_failed,))
            return
        if failed:
            self.read_tape_done.set()
            return
        if do_crc:
            if self.tr_failed:
                self.read_tape_done.set()
                return # do not calculate CRC if net thead detected a failed transfer
            complete_crc = self.file_info.get('complete_crc',None)
            bfid = self.file_info.get('bfid',None)
            Trace.trace(22,"read_tape: calculated CRC %s File DB CRC %s"%
                        (self.buffer.complete_crc, complete_crc))
            if self.buffer.complete_crc != complete_crc:
                # this is to fix file db
                if complete_crc == None:

                    sanity_cookie = (self.buffer.sanity_bytes,self.buffer.sanity_crc)
                    if self.method == 'read_next' and bfid == None:
                        # must be tape ingest case
                        if self.bytes_to_write != self.bytes_read:
                            self.bytes_to_write = self.bytes_read
                        Trace.log(e_errors.WARNING, "updating file db entry for the tape ingest")
                        # create a bit file entry
                        self.lock_file_info = 1
                        self.file_info['size'] = self.bytes_read
                        self.file_info['sanity_cookie'] = sanity_cookie
                        self.file_info['complete_crc'] = self.buffer.complete_crc
                        self.file_info['drive'] = "%s:%s" % (self.current_work_ticket['mover']['device'], self.config['serial_num'])
                        self.file_info['pnfsid'] = None
                        self.file_info['pnfs_name0'] = None # it may later come in get ticket
                        self.file_info['gid'] = self.gid
                        self.file_info['uid'] = self.uid
                        self.file_info['mover_type'] = self.mover_type

                        ret = self.fcc.create_bit_file(self.file_info)
                        # update file info
                        ##Trace.trace(98, "updated file info %s"%(ret,)) Uncomment when fixed
                        Trace.log(e_errors.INFO, "updated file info %s"%(ret,)) ## Remove when fixed
                        if ret['status'][0] != e_errors.OK:
                            Trace.log(e_errors.ERROR, "cannot assign new bfid %s"%(ret,))
                            self.read_tape_done.set()
                            self.transfer_failed(e_errors.ERROR,"Cannot assign new bit file ID")
                            return
                        if ret['fc']['bfid'] == None:
                            Trace.log(e_errors.ERROR,"FC returned None for bfid %s"%(ret,))
                            self.read_tape_done.set()
                            self.transfer_failed(e_errors.ERROR,"FC returned None for bfid")
                        self.file_info.update(ret['fc'])
                        self.current_work_ticket['fc'].update(self.file_info)
                        self.current_work_ticket['bfid'] = self.file_info['bfid']
                        self.lock_file_info = 0
                        Trace.trace(98, "updated file db %s"%(self.current_work_ticket['fc'],))
                        # update volume DB and volume info
                        self.vcc.update_counts(self.current_volume, wr_access=1)
                        self.vol_info['eod_cookie'] = loc_to_cookie(self.current_location)

                        if self.driver_type == 'FTTDriver' and self.rem_stats:
                            stats = self.tape_driver.get_stats()
                            remaining = stats[self.ftt.REMAIN_TAPE]
                            if remaining is not None:
                                remaining = long(remaining)
                                self.vol_info['remaining_bytes'] = remaining * 1024L
                                ##XXX keep everything in KB?
                        ret = self.vcc.set_remaining_bytes(self.current_volume,
                                                           self.vol_info['remaining_bytes'],
                                                           self.vol_info['eod_cookie'])
                        if ret['status'][0] != e_errors.OK:
                            self.read_tape_done.set()
                            self.set_volume_noaccess(self.current_volume, "Failed to update volume information. See log for details")
                            Trace.alarm(e_errors.ALARM, "Failed to update volume information on %s, EOD %s. May cause a data loss."%
                                        (self.current_volume, self.vol_info['eod_cookie']))
                            self.transfer_failed(ret['status'][0], ret['status'][1], error_source=TAPE)
                            return
                        self.vol_info.update(self.vcc.inquire_vol(self.current_volume))
                        self.current_work_ticket['vc'].update(self.vol_info)
                        Trace.trace(98, "updated volume db %s"%(self.current_work_ticket['vc'],))

                    else:
                        Trace.log(e_errors.WARNING, "found complete CRC set to None in file DB for %s. Changing cookie to %s and CRC to %s" %
                                  (self.file_info['bfid'],sanity_cookie, self.buffer.complete_crc))
                        self.fcc.set_crcs(self.file_info['bfid'], sanity_cookie, self.buffer.complete_crc)
                else:
                    if self.tr_failed:
                        Trace.log(e_errors.ERROR,"read_tape: calculated CRC %s File DB CRC %s"%
                                  (self.buffer.complete_crc, complete_crc))
                        self.read_tape_done.set()
                        return  # do not raise alarm if net thead detected a failed transfer
                    crc_error = 1
                    # try 1 based crc
                    crc_1_seeded = checksum.convert_0_adler32_to_1_adler32(self.buffer.complete_crc,
                                                                               self.file_info['size'])
                    if crc_1_seeded == complete_crc:
                        self.buffer.complete_crc = crc_1_seeded
                        crc_error = 0
                    if crc_error:
                        # this is for crc check in ASSERT mode
                        Trace.trace(24, "MODE %s"%(mode_name(self.mode),))
                        if self.mode == ASSERT:
                            self.assert_return = e_errors.CRC_ERROR
                            return
                        ####
                        else:
                            Trace.alarm(e_errors.ERROR, "read_tape CRC error",
                                        {'outfile':self.current_work_ticket['outfile'],
                                         'infile':self.current_work_ticket['infile'],
                                         'location_cookie':self.current_work_ticket['fc']['location_cookie'],
                                         'external_label':self.current_work_ticket['vc']['external_label'],
                                         'complete_crc':complete_crc, 'buffer_crc':self.buffer.complete_crc,
                                         '_bytes_read':self.file_info['size']})
                        self.read_tape_done.set()
                        self.transfer_failed(e_errors.CRC_ERROR, error_source=TAPE)
                        return

        self.bytes_read_last = self.bytes_read
        # if data is tranferred slowly
        # the false "too long in state.." may be generated
        # to aviod this just make a trick with time_in_state
        self.time_in_state = time.time()- t_started
        Trace.log(e_errors.INFO, "read_tape exiting, read %s/%s bytes" %
                    (self.bytes_read, self.bytes_to_read))

        Trace.log(e_errors.INFO, "read_tape timing:get_space %s read %s crc_check %s push %s block_read %s idle %s" %
                  (self.buffer.read_stats[0],
                   self.buffer.read_stats[1],
                   self.buffer.read_stats[2],
                   self.buffer.read_stats[3],
                   self.buffer.read_stats[4],
                   idle_time))


        # this is for crc check in ASSERT mode
        if self.mode == ASSERT:
            self.assert_return = e_errors.OK
        #####

        self.read_tape_done.set()

    def write_client(self):
        """
        Write data out to the network.

        """

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
        if self.bytes_to_write > 0 and self.bytes_written == 0 and self.wrapper and self.wrapper.__name__ != "null_wrapper": #Skip over cpio or other headers
            while self.buffer.header_size is None and self.state in (ACTIVE, ):
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
        last_notify_time = time.time()
        Trace.notify("transfer %s %s %s network %s %.3f %s" %
                     (self.shortname, -self.bytes_written,
                      self.bytes_to_write, self.buffer.nbytes(),
                      time.time(), self.draining))
        cnt = 0
        while 1:
            Trace.trace(133, "state %s cnt %s"%(state_name(self.state),cnt))
            Trace.trace(133, "bytes_written %s bytes to write %s"%(self.bytes_written,self.bytes_to_write))
            if self.state in (ACTIVE, ) and self.bytes_written < self.bytes_to_write:
                #Trace.trace(33, "bytes_written %s bytes to write %s"%(self.bytes_written,self.bytes_to_write))
                if self.tr_failed:
                    break
                self.network_write_active = (self.bytes_written_last != self.bytes_written)
                self.bytes_written_last = self.bytes_written
                if self.buffer.empty():
                    # there is no data to transfer to the client
                    Trace.trace(9, "write_client: buffer empty, wrote %s/%s" %
                                (self.bytes_written, self.bytes_to_write))
                    self.buffer.write_ok.clear()
                    #self.buffer.write_ok.wait(1)
                    self.buffer.write_ok.wait(2)
                    continue

                nbytes = min(self.bytes_to_write - self.bytes_written, self.buffer.blocksize)
                bytes_written = 0
                try:
                    self.stream_w_flag = 1
                    bytes_written = self.buffer.stream_write(nbytes, driver)
                    self.stream_w_flag = 0
                except MoverError, detail:
                    detail = str(detail)
                    if detail == e_errors.CRC_ERROR:
                        Trace.alarm(e_errors.ERROR, "CRC error in write client",
                                    {'outfile':self.current_work_ticket['outfile'],
                                     'infile':self.current_work_ticket['infile'],
                                     'location_cookie':self.current_work_ticket['fc']['location_cookie'],
                                     'external_label':self.current_work_ticket['vc']['external_label']})
                        self.transfer_failed(detail, error_source=TAPE)
                    else:

                        if detail == e_errors.ENCP_GONE:
                            err_source = NETWORK
                        else:
                            err_source = UNKNOWN
                        self.transfer_failed(detail, error_source=err_source)
                    failed = 1
                    break
                except:
                    exc, detail, tb = sys.exc_info()
                    msg="exc %s detail %s"%(exc, detail)
                    self.transfer_failed(e_errors.ENCP_GONE, msg)
                    failed = 1
                    break
                if bytes_written < 0:
                    self.transfer_failed(e_errors.ENCP_GONE, "write returns %s"%(bytes_written,))
                    failed = 1
                    break
                if bytes_written != nbytes:
                    Trace.trace(22, "write_client: !!! bytes written %s bytes to write %s"%(bytes_written, nbytes))
                    if self.client_socket:
                        # get netstat for this socket
                        data_port = self.client_socket.getsockname()[1]
                        rc = self.shell_command("netstat -t | grep %s"%(data_port,))
                        Trace.trace(22, "write_client: netstat: %s"%(rc,))
                    pass
                self.bytes_written = self.bytes_written + bytes_written
                if not self.buffer.full():
                    self.buffer.read_ok.set()

                #If it is time to do so, send the notify message.
                if is_threshold_passed(self.bytes_written, bytes_notified,
                                       self.bytes_to_write, last_notify_time):
                    bytes_notified = self.bytes_written
                    last_notify_time = time.time()
                    #negative byte-count to indicate direction
                    Trace.notify("transfer %s %s %s network %s %.3f %s" %
                                 (self.shortname, -self.bytes_written,
                                  self.bytes_to_write, self.buffer.nbytes(),
                                  last_notify_time, self.draining))
            else:
                break

        if self.tr_failed:
            driver.close()
            return

        Trace.log(e_errors.INFO, "write_client: wrote %s/%s bytes" % (self.bytes_written, self.bytes_to_write))
        if failed or self.tr_failed:
            driver.close() # just in case when it was not closed for some reason
            return

        if self.bytes_written == self.bytes_to_write:
            # check crc
            if do_crc and self.file_info['size'] != 0: # we do not calculate crc for 0 length file.
                Trace.trace(22,"write_client: calculated CRC %s File DB CRC %s"%
                            (self.buffer.complete_crc, self.file_info['complete_crc']))
                if self.buffer.complete_crc != self.file_info['complete_crc']:
                    # try 1 based crc
                    Trace.trace(22,"write_client: trying crc 1 seeded")
                    crc_1_seeded = checksum.convert_0_adler32_to_1_adler32(self.buffer.complete_crc,
                                                                           self.file_info['size'])

                    Trace.trace(22,"write_client: calculated CRC (1 seeded) %s File DB CRC %s"%
                                (crc_1_seeded, self.file_info['complete_crc']))
                    if crc_1_seeded == self.file_info['complete_crc']:
                        self.buffer.complete_crc = crc_1_seeded
                    else:
                        Trace.alarm(e_errors.ERROR, "CRC error in write client",
                                    {'outfile':self.current_work_ticket['outfile'],
                                     'infile':self.current_work_ticket['infile'],
                                     'location_cookie':self.current_work_ticket['fc']['location_cookie'],
                                     'external_label':self.current_work_ticket['vc']['external_label']})
                        self.transfer_failed(e_errors.CRC_ERROR, error_source=TAPE)
                        return
            self.network_write_active = (self.bytes_written_last != self.bytes_written)
            self.bytes_written_last = self.bytes_written

        # This is for the cases when transfer has completed
        # but tape thread did not exit for some reason.
        # Usually this is due to read error recovery.
        # The maximal recommended by oracle timeout is 20 min.
        if self.mover_type != enstore_constants.DISK_MOVER and self.read_tape_done.wait(self.read_error_recovery_timeout):
            Trace.log(e_errors.INFO, "write_client waits for tape thread to exit it may take up to %s min"%(self.read_error_recovery_timeout/60,))
            self.read_tape_done.clear()
            Trace.log(e_errors.INFO, "write_client detected tape theard termination signal")
        else:
            Trace.log(e_errors.ERROR, "write_client exits, but tape thread termination signal was not recieved")

        # this is for crc check in ASSERT mode
        # do not call transfer_completed
        # it will be done by volume_assert
        Trace.log(e_errors.INFO, "write_client exits")
        if self.mode != ASSERT:
            self.transfer_completed()
        else:
            self.assert_ok.set()

    def write_to_hsm(self, ticket):
        """
        The library manager has asked mover to write a file to the hsm.

        :type ticket: :obj:`dict`
        :arg ticket: ticket received from library manager.
        """
        Trace.log(e_errors.INFO, "WRITE_TO_HSM. Mover state %s"%(state_name(self.state),))
        Trace.trace(10, "State %s"%(state_name(self.state),))
        if ticket.has_key('copy') and not ticket['fc'].has_key('original_bfid'):
            # this is a file copy request
            self.transfer_failed(e_errors.ERROR,"Cannot assign new bit file ID. No original_bfid key in ticket")
            return

        self.setup_transfer(ticket, mode=WRITE)

    def update_volume_info(self, ticket):
        """
        Volume information coming in the ticket from library manager may be not up to date.

        Update it with request to volume clerk

        :type ticket: :obj:`dict`
        :arg ticket: current ticket originally received from library manager
        """
        Trace.trace(20, "update_volume_info for %s. Current %s"%(ticket['external_label'],
                                                                 self.vol_info))
        if not self.vol_info:
            self.vol_info.update(self.vcc.inquire_vol(ticket['external_label']))
        else:
            if self.vol_info['external_label'] is not ticket['external_label']:
                Trace.log(e_errors.ERROR,"Library manager asked to update iformation for the wrong volume: %s, current %s" % (ticket['external_label'],self.vol_info['external_label']))
            else:
                self.vol_info.update(self.vcc.inquire_vol(ticket['external_label']))


    def read_from_hsm(self, ticket):
        """
        The library manager has asked mover to read a file from the hsm.

        :type ticket: :obj:`dict`
        :arg ticket: ticket received from library manager.
        """

        Trace.log(e_errors.INFO,"READ FROM HSM. Mover state %s"%(state_name(self.state),))
        self.method = ticket.get("method", None)
        Trace.trace(98, "read_from_hsm %s"%(ticket,))
        if self.method and self.method == "read_next":
            self.udp_cm_sent = 0
        self.setup_transfer(ticket, mode=READ)

    def volume_assert(self, ticket):
        """
        The library manager has asked mover to make volume assert -
        read specified or all files on tape and report result.

        :type ticket: :obj:`dict`
        :arg ticket: ticket received from library manager.
        """
        Trace.log(e_errors.INFO,"VOLUME ASSERT")
        self.setup_transfer(ticket, mode=ASSERT)

    def setup_transfer(self, ticket, mode):
        """
        Prepare for data transfer.

        Read specified or all files on tape and report result.

        :type ticket: :obj:`dict`
        :arg ticket: ticket received from library manager.
        :type mode: :obj:`int`
        :arg mode: ``READ``, ``WRITE``, ``ASSERT`` (see the source)
        """
        # see what threads are running
        threads = threading.enumerate()
        for thread in threads:
            if thread.isAlive():
                thread_name = thread.getName()
                Trace.trace(87,"setup_transfer: Thread %s is running" % (thread_name,))
            else:
                Trace.trace(87,"setup_transfer: Thread %s is dead"%(thread_name,))

        self.net_driver = net_driver.NetDriver()
        self._error = None
        self._error_source = None
        self.lock_state()
        self.save_state = self.state
        Trace.trace(24, "setup_transfer: save_state %s"%(state_name(self.save_state),))
        self.udp_cm_sent = 0
        self.unique_id = ticket['unique_id']
        self.uid = -1
        self.gid = -1
        self.header_labels = "" # set it here to not have problems in assert
        if ticket.has_key('wrapper'):
            self.uid = ticket['wrapper'].get('uid', -1)
            self.gid = ticket['wrapper'].get('gid', -1)
        if self.method and self.method == "read_next" and self.udp_control_address:
            self.lm_address = self.udp_control_address
            self.lm_address_saved = self.lm_address
            Trace.trace(98, "LM address %s"%(self.lm_address,))
            self.libraries = [("get", self.lm_address)]

        else:
            try:
                self.lm_address = ticket['lm']['address']
            except KeyError:
                self.lm_address = ('none',0)
        ##NB: encp v2_5 supplies this information for writes but not reads. Somebody fix this!
        try:
            client_hostname = ticket['wrapper']['machine'][1]
        except KeyError:
            client_hostname = ''
        self.client_hostname = client_hostname

        Trace.trace(10, "setup transfer1 %s"%(ticket,))
        self.tr_failed = 0
        self.current_library = ticket['vc'].get('library', None)
        if not self.current_library:
            self.transfer_failed(e_errors.EPROTO)
            return
        self.setup_mode = mode
        ## pprint.pprint(ticket)
        #if (self.save_state not in (IDLE, HAVE_BOUND) or
        #    self.setup_mode == ASSERT and self.save_state != IDLE):
        if self.save_state not in (IDLE, HAVE_BOUND):
            Trace.log(e_errors.ERROR, "Not idle %s" %(state_name(self.state),))
            self.return_work_to_lm(ticket)
            self.unlock_state()
            return

        if (self.save_state == HAVE_BOUND and self.single_filemark and self.mode == WRITE and self.setup_mode == READ
            and self.write_counter > 0): # there was at least one successful write since tape mount
            # switching from write to read write additional fm
            Trace.log(e_errors.INFO,"writing a tape mark before switching to READ")
            if self.driver_type == 'FTTDriver':
                bloc_loc = 0L
                if self.write_in_progress:
                    # Write was interrupted on the client side
                    # position the tape to the last fm
                    try:
                        self.tape_driver.seek(self.current_location, 0)
                    except:
                        exc, detail, tb = sys.exc_info()
                        self.transfer_failed(e_errors.POSITIONING_ERROR, 'positioning error %s %s' % (exc, detail,), error_source=DRIVE)
                        self.unlock_state()
                        return

                self.update_tape_stats()
                if self.tr_failed:
                    self.unlock_state()
                    return

                if self.bloc_loc != self.last_absolute_location:
                    self.transfer_failed(e_errors.WRITE_ERROR,
                                         "Wrong position for %s: last %s, current %s"%
                                         (self.current_volume, self.last_absolute_location,
                                          self.bloc_loc,),error_source=TAPE)
                    self.vcc.set_system_readonly(self.current_volume)
                    Trace.alarm(e_errors.ERROR,
                                "Wrong position for %s. See log for details. Volume is set readonly"%
                                (self.current_volume,))
                    # also set volume to NOACCESS, so far
                    # no alarm is needed here because it is send by volume clerk
                    # when it sets a volume to NOACCESS
                    self.set_volume_noaccess(self.current_volume, "Positioning error. See log for details")
                    # log all running proceses
                    self.log_processes(logit=1)
                    self.unlock_state()
                    return
                try:
                  self.tape_driver.writefm()
                  # skip back one position in case when next read fails
                  # in this case tape is in the right position for the next write
                  self.tape_driver.skipfm(-1)
                  # reset counter, which counts the number of successful writes
                  # this counter must be reset after mounting tape
                  # or switching from write to read
                  # to decide later whether the additionla tape mark needs to get written
                  # in a single file mark mode
                  self.write_counter = 0
                except:
                    Trace.handle_error()
                    self.vcc.set_system_readonly(self.current_volume)
                    Trace.alarm(e_errors.ERROR,"error writing file mark, will set volume readonly")
                    # also set volume to NOACCESS, so far
                    # no alarm is needed here because it is send by volume clerk
                    # when it sets a volume to NOACCESS
                    self.set_volume_noaccess(self.current_volume, "Write error. See log for details")
                    # log all running proceses
                    self.log_processes(logit=1)

                    self.return_work_to_lm(ticket)
                    self.unlock_state()
                    self.transfer_failed(e_errors.WRITE_ERROR, detail, error_source=TAPE)
                    return

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

    def check_connection(self):
        """
        Check connection in ``ASSERT`` mode.
        Assert can run for a long period of time.
        The client initiated assert could be gone.
        This allows to check if the client is still connected and interrupt assert if it is gone.
        """

        STOP_MEDIA_VALIDATION = 0x10
        VLBPM = 0x20 # Verify Logical Block Protection Method

        Trace.trace(40, "check_connection started")
        percent_completed = -1. # to have 1st notify sent when 0 bytes were transferred
        self.bytes_read_last = 0
        while self.mode == ASSERT:
            Trace.trace(40, "check_connection mode %s"%(mode_name(self.mode),))
            try:
                if self.control_socket:
                    r, w, ex = select.select([self.control_socket], [self.control_socket], [], 10)
                    Trace.trace(40, "check_connection1 %s %s %s"%(r, w, ex))
                    Trace.trace(40, "r= %s"%(r,))
                    if r:
                        # r - read socket appears when client connection gets closed
                        Trace.trace(40, "media_validate= %s"%(self.media_validate,))
                        if self.media_validate:
                            # Stop media validation.
                            # Effective only for T10000C and D drives
                            # self.media_validate is set only for them
                            scsi_mode_select.ftt_scsi_verify(self.tape_driver, VLBPM, STOP_MEDIA_VALIDATION)
                            Trace.log(e_errors.INFO, "The assert client is gone %s" %
                                      (self.current_work_ticket['callback_addr'],))
                            self.transfer_failed(e_errors.ENCP_GONE,
                                                 "The assert client is gone %s" %
                                                 (self.current_work_ticket['callback_addr'],),
                                                 error_source=NETWORK)

                        self.interrupt_assert = True
                        break
                    else:
                        time.sleep(10)
                else:
                    break
            except:
                pass
            if self.media_validate:
                # Check percent completed
                ret = scsi_mode_select.check_scsi_verify(self.tape_driver)
                if ret[0]:
                    Trace.log(DEBUG_LOG, "check_scsi_verify %s %s %s"%(ret[1], ret[2], percent_completed))
                    if ret[1]:
                        self.transfer_completed(e_errors.OK)
                        Trace.log(e_errors.INFO, "The assert for %s is completed" %
                                  (self.vol_info['external_label'],))
                        self.transfer_completed(e_errors.OK)
                        break
                    if percent_completed != ret[2]:
                        percent_completed = ret[2]
                        self.bytes_read = int(self.vol_info['active_bytes']/100. * percent_completed)
                        self.bytes_read_last = self.bytes_read-1 # hack to not alarm false transfer stuck

                        Trace.log(DEBUG_LOG, "active %s written %s to write %s"%
                                  (self.vol_info['active_bytes'],
                                  self.bytes_read,
                                  self.vol_info['active_bytes']-self.bytes_read))
                        Trace.notify("transfer %s %s %s network %s %.3f %s" %
                                     (self.shortname, -self.bytes_read,
                                      self.vol_info['active_bytes'], 0,
                                      time.time(), self.draining))
                    time.sleep(30)
                else:
                    self.transfer_failed(e_errors.DRIVEERROR,
                                         "%s for %s. Last reported percent done %s" %
                                         (MEDIA_VERIFY_FAILED, self.vol_info['external_label'], percent_completed),
                                         error_source=TAPE)
                    break


        Trace.trace(40, "check_connection exits %s" % (mode_name(self.mode),))



    def assert_vol(self):
        """
        Performing volume assert.

        """
        STANDARD_VERIFY = 0x04
        COMPLETE_VERIFY = 0x01
        self.net_driver = null_driver.NullDriver()
        ticket = self.current_work_ticket
        self.media_validate = False
        self.assert_ok.clear()
        self.t0 = time.time()
        self.vcc = volume_clerk_client.VolumeClerkClient(self.csc,
                                                         server_address=ticket['vc']['address'])
        vc = ticket['vc']
        self.vol_info.update(vc)
        self.volume_family=vc['volume_family']
        if not (self.save_state == HAVE_BOUND and self.current_volume == ticket['vc']['external_label']):
            self.mount_volume(ticket['vc']['external_label'])
        if self.state in  (ERROR, IDLE):
            Trace.log(e_errors.ERROR, "ASSERT failed %s" % (self.current_work_ticket['status'],))
            self.current_work_ticket['status'] = (e_errors.MOUNTFAILED, None)
            callback.write_tcp_obj(self.control_socket, ticket)
            self.control_socket.close()
            self.control_socket = None
            return
        #At this point the media changer claims the correct volume is loaded;
        have_tape = 0
        self.need_lm_update = (1, None, 0, None)
        for retry_open in range(3):
            Trace.trace(10, "position media")
            have_tape = self.tape_driver.open(self.device, READ, retry_count=30)
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

        # to not dismout volume after assert
        self.delay = self.default_dismount_delay

        if ticket.has_key('action'):
            self.current_work_ticket = ticket
            self.mode = ASSERT
            if (ticket['action'] == 'crc_check'):
                ic_conf = self.csc.get("info_server")
                info_c = info_client.infoClient(self.csc,
                                            server_address=(ic_conf['host'],
                                                            ic_conf['port']))
                file_info = {}
                fc_address = ticket['fc']['address']
                ticket['return_file_list'] = {}
                Trace.trace(24, 'ticket %s'%(ticket,))
                if ticket.has_key('parameters') and type(ticket['parameters']) == type([]) and ticket['parameters']:
                    # Initialize return list
                    for lc in ticket['parameters']:
                       rec = info_c.find_file_by_location(ticket['vc']['external_label'], lc)
                       Trace.trace(24, "find_file_by_location returned %s"%(rec,))
                       if rec['status'][0] != e_errors.OK:
                           self.transfer_failed(file_list['status'][0], file_list['status'][1])
                           return
                       if 'file_list' in rec:
                           # This happens when there is more than one entry for the same location,
                           # which, in general is wrong.
                           # Send alarm but continue
                           Trace.alarm(e_errors.WARNING,"Possibly the same location: %s %s"%
                                       (ticket['vc']['external_label'], rec['file_list']))

                           for entry in rec['file_list']:
                               ticket['return_file_list'][entry['location_cookie']] = e_errors.UNKNOWN
                               file_info[entry['location_cookie']] = entry
                       else:
                           ticket['return_file_list'][lc] = e_errors.UNKNOWN
                           file_info[lc] = rec
                else:
                    # get file list for the whole tape from file clerk
                    try:
                        Trace.log(e_errors.INFO, "calling tape_list")
                        file_list = info_c.tape_list(ticket['vc']['external_label'], all_files = False, skip_unknown = True, timeout = 300, retry = 2)

                        Trace.log(e_errors.INFO, "tape_list returned")
                        Trace.trace(24, "file List %s:: %s"%(type(file_list), file_list))
                        if file_list['status'][0] != e_errors.OK:
                            self.transfer_failed(file_list['status'][0], file_list['status'][1])
                            return
                    except:
                        exc, msg, tb = sys.exc_info()
                        self.transfer_failed(exc, msg, error_source=USER)
                        return
                    # create the list of location cookies
                    for entry in file_list['tape_list']:
                        ticket['return_file_list'][entry['location_cookie']] = e_errors.UNKNOWN
                        file_info[entry['location_cookie']] = entry
                    Trace.trace(24, "file_info %s"%(file_info,))

                Trace.trace(24, 'ticket %s'%(ticket,))

                keys = ticket['return_file_list'].keys()
                keys.sort()
                Trace.trace(24, 'keys %s'%(keys,))
                stat = e_errors.OK
                # start client network monitor to detect
                # that client is gone and interruprt assert
                self.interrupt_assert = False
                self.run_in_thread('network_monitor', self.check_connection)

                for loc_cookie in keys:
                    if self.draining or self.interrupt_assert:
                        break
                    location = cookie_to_long(loc_cookie)
                    self.file_info =  file_info[loc_cookie]
                    self.assert_return = e_errors.OK
                    self.reset(None, 1)
                    self.current_work_ticket = ticket
                    Trace.trace(24, "t1 %s"%(type(self.current_work_ticket),))
                    Trace.trace(24, "t11 %s"%(type(self.current_work_ticket['fc']),))
                    Trace.trace(24, "t2 %s"%(type(file_info),))
                    Trace.trace(24, "t21 %s"%(type(file_info.get(loc_cookie)),))
                    Trace.trace(24, "t22 %s"%(ticket,))
                    self.current_work_ticket['fc'] = file_info[loc_cookie]
                    self.current_work_ticket['fc']['address'] = fc_address
                    Trace.trace(24, "t23 %s"%(self.current_work_ticket,))
                    self.finish_transfer_setup()
                    Trace.trace(24, "t31 starting seek thread" )
                    self.run_in_thread('seek_thread', self.seek_to_location,
                                       args = (location, self.mode==WRITE),
                                       after_function=self.start_transfer)

                    #self.net_driver.open('/dev/null', WRITE)
                    Trace.trace(24, "t32 assert_ok returned" )

                    self.assert_ok.wait()
                    self.need_lm_update = (1, None, 0, None)
                    self.assert_ok.clear()
                    self.net_driver.close()
                    self.network_write_active = False # reset to indicate no network activity
                    Trace.trace(24, "assert return: %s"%(self.assert_return,))
                    ticket['return_file_list'][loc_cookie] = self.assert_return
                    if self.assert_return != e_errors.OK:
                        stat = self.assert_return
                    if self.tr_failed:
                        stat = self._error
                        ticket['return_file_list'][loc_cookie] = stat
                        Trace.trace(24, "ticket!: %s"%(ticket['return_file_list'][loc_cookie],))
                        break

                if self.interrupt_assert:
                    Trace.log(e_errors.INFO, "The assert client is gone %s" %
                              (self.current_work_ticket['callback_addr'],))
                    self.transfer_failed(e_errors.ENCP_GONE,
                                         "The assert client is gone %s" %
                                         (self.current_work_ticket['callback_addr'],),
                                         error_source=NETWORK)

                elif not self.tr_failed:
                    self.transfer_completed(stat)
                    Trace.log(e_errors.INFO, "The assert for %s is completed" %
                              (ticket['vc']['external_label'],))
            elif (ticket['action'] in ('media_validate_standard', 'media_validate_complete')):
                if not self.config['product_id'] in ("T10000C", "T10000D"):
                    self.transfer_failed(e_errors.WRONGPARAMETER,
                                         "Media validation is not supported for this drive type %s" %
                                         (self.current_work_ticket['callback_addr'],),
                                         error_source=DRIVE)
                else:
                    self.state = ACTIVE
                    verify_option = STANDARD_VERIFY
                    if ticket['action'] == 'media_validate_complete':
                        verify_option = COMPLETE_VERIFY
                    Trace.log(e_errors.INFO, "Starting media validation for %s"%
                              (ticket['vc']['external_label'],))
                    scsi_mode_select.ftt_scsi_verify(self.tape_driver,  byte2 = verify_option)
                    # the execition monitoring will be done in network_monitor
                    # start client network monitor to detect
                    # that client is gone and interruprt assert
                    self.media_validate = True
                    self.run_in_thread('network_monitor', self.check_connection)

        else:
            self.transfer_completed(e_errors.OK)
            Trace.log(e_errors.INFO, "The assert for %s is completed" %
                      (ticket['vc']['external_label'],))

        #    # read tape and
        #    self.dismount_volume(after_function=self.idle)

    def finish_transfer_setup(self):
        """
        Complete data transfer setup.

        """
        Trace.trace(10, "client connect returned: %s %s" % (self.control_socket, self.client_socket))
        Trace.trace(24, "finish_transfer_setup %s %s"%(mode_name(self.mode), mode_name(self.setup_mode)))
        ticket = self.current_work_ticket
        if not self.client_socket:
            if self.mode != ASSERT:
                Trace.trace(20, "finish_transfer_setup: connection to client failed")
                self.state = self.save_state
                ## Connecting to client failed
                if self.state == HAVE_BOUND:
                    self.dismount_time = time.time() + self.default_dismount_delay
                self.need_lm_update = (1, self.state, 1, None)
                self.send_error_msg(error_info=(e_errors.ENCP_GONE, "no client socket"), error_source=NETWORK)
                #self.update_lm(reset_timer=1)
                return

        self.t0 = time.time()
        self.crc_seed = self.initial_crc_seed
        if 'crc_seed' in ticket:
            crc_seed = int(ticket['crc_seed'])
            if crc_seed == 1 or crc_seed == 0:
                self.crc_seed = crc_seed

        ##all groveling around in the ticket should be done here
        fc = ticket['fc']
        vc = ticket['vc']
        self.vol_info = vc
        self.file_info = fc
        Trace.trace(24, "VOL_INFO %s"%(self.vol_info,))
        Trace.trace(24, "FILE_INFO %s"%(self.file_info,))
        self.volume_family=vc['volume_family']
        delay = 0
        sanity_cookie = ticket['fc'].get('sanity_cookie', None)
        # explicitely set client crc
        if self.setup_mode == READ or self.setup_mode == ASSERT:
            client_crc_on = self.read_crc_control
        else:
            client_crc_on = 1 # write
        if self.config['driver'] == "NullDriver":
            client_crc_on = 0

        # ignore client_crc,
        # the default setting for write: check crc before writing to tape in tape thread
        # the default setting for read, assert: check crc before writing to network in net thread

        #if ticket.has_key('client_crc'):
        #    client_crc_on = ticket['client_crc']

        Trace.trace(22, "crc_control %s"%(self.read_crc_control,))
        self.reset(sanity_cookie, client_crc_on)
        # restore self.current_work_ticket after it gets cleaned in the reset()
        self.current_work_ticket = ticket
        if self.current_work_ticket['encp'].has_key('delayed_dismount'):
            if ((type(self.current_work_ticket['encp']['delayed_dismount']) is type(0)) or
                (type(self.current_work_ticket['encp']['delayed_dismount']) is type(0.))):
                delay = 60 * self.current_work_ticket['encp']['delayed_dismount']
            else:
                delay = self.default_dismount_delay
        else:
            if self.mode == ASSERT:
              delay = self.default_dismount_delay
        if delay > 0:
            self.delay = max(delay, self.default_dismount_delay)
        elif delay < 0:
            self.delay = 31536000  # 1 year
        else:
            self.delay = 0
        self.delay = min(self.delay, self.max_dismount_delay)

        # If we have mixed IPV4/IPV6 configuration
        # the address coming from encp may have IPV4 if it runs on IPV4 configuration.
        # Check the address originally stored in vcc and fcc.
        # If it has IPV6 configuration do not open new vcc or fcc.
        vcc_address_family = socket.getaddrinfo(self.vcc.server_address[0], None)[0][0]
        fcc_address_family = socket.getaddrinfo(self.fcc.server_address[0], None)[0][0]
        client_reported_vcc_address_family = socket.getaddrinfo(vc['address'][0], None)[0][0]
        client_reported_fcc_address_family = socket.getaddrinfo(fc['address'][0], None)[0][0]
        if fcc_address_family == client_reported_fcc_address_family:
            self.fcc = file_clerk_client.FileClient(self.csc, bfid=0,
                                                    server_address=fc['address'])
        if vcc_address_family == client_reported_vcc_address_family:
            self.vcc = volume_clerk_client.VolumeClerkClient(self.csc,
                                                             server_address=vc['address'])
        self.vc_address = self.vcc.server_address
        self.unique_id = self.current_work_ticket['unique_id']
        volume_label = fc['external_label']
        if volume_label:
            self.vol_info.update(self.vcc.inquire_vol(volume_label))
            self.current_work_ticket['vc'].update(self.vol_info)
        else:
            Trace.log(e_errors.ERROR, "setup_transfer: volume label=%s" % (volume_label,))

        if self.vol_info['status'][0] != e_errors.OK:
            msg =  ({READ: e_errors.READ_NOTAPE, WRITE: e_errors.WRITE_NOTAPE}.get(
                self.setup_mode, e_errors.EPROTO), self.vol_info['status'][1])
            Trace.log(e_errors.ERROR, "Volume clerk reply %s %s" % (self.vol_info['status'][0], msg,))
            self.transfer_failed(msg[0], msg[1], error_source=TAPE, dismount_allowed=0)
            #self.send_client_done(self.current_work_ticket, msg[0], msg[1])
            #self.state = self.save_state
            return

        self.buffer.set_blocksize(self.vol_info['blocksize'])
        self.wrapper = None
        self.wrapper_type = volume_family.extract_wrapper(self.volume_family)

        try:
            self.wrapper = __import__(self.wrapper_type + '_wrapper')
        except:
            exc, msg, tb = sys.exc_info()
            Trace.log(e_errors.ERROR, "error importing wrapper: %s %s" %(exc,msg))

        if not self.wrapper:
            msg = e_errors.INVALID_WRAPPER, "Illegal wrapper type %s" % (self.wrapper_type)
            Trace.log(e_errors.ERROR,  "%s" %(msg,))
            self.transfer_failed(msg[0], msg[1], error_source=TAPE, dismount_allowed=0)
            #self.send_client_done(self.current_work_ticket, msg[0], msg[1])
            #self.state = self.save_state
            self.idle()
            return

        self.buffer.set_wrapper(self.wrapper)
        client_filename = self.current_work_ticket['wrapper'].get('fullname','?')
        pnfs_filename = self.current_work_ticket['wrapper'].get('pnfsFilename', '?')

        self.saved_mode = self.mode
        self.mode = self.setup_mode
        if self.mode in (READ, ASSERT):
            # for reads and asserts always set crc_seed to 0
            # crc will be automatically checked against seed 1
            # in case if seed 0 crc check fails
            self.buffer.set_crc_seed(0L)

        Trace.trace(24, "FC %s"%(fc,))
        bytes = fc.get('size', None)
        if bytes == None:
            self.bytes_to_transfer = None
            self.expected_transfer_time = 0.
            if self.method == "read_next":
                # we do not know what is the file size, so let's assume that it is big
                self.bytes_to_transfer = self.vol_info['capacity_bytes']
                self.expected_transfer_time = self.bytes_to_transfer / self.max_rate
        else:
            self.bytes_to_transfer = long(bytes)
            self.expected_transfer_time = self.bytes_to_transfer / self.max_rate
        self.bytes_to_write = self.bytes_to_transfer
        self.bytes_to_read = self.bytes_to_transfer
        Trace.trace(24, "BYTES TO READ %s"%(self.bytes_to_read,))
        self.real_transfer_time  = 0.
        if (self.bytes_to_transfer == None) or (self.bytes_to_transfer < 0L) :
            self.transfer_failed(e_errors.BAD_FILE_SIZE, "bad file size is %s"%(self.bytes_to_transfer,), error_source=USER, dismount_allowed=0)

            return

        if self.client_hostname:
            client_filename = self.client_hostname + ":" + client_filename
        if self.wrapper:
            self.current_work_ticket['mover']['compression'] = self.compression
            if self.mode != ASSERT:
                self.wrapper_ticket = self.wrapper.create_wrapper_dict(self.current_work_ticket)
        if self.mode == READ:
            self.files = (pnfs_filename, client_filename)
            self.target_location = cookie_to_long(fc['location_cookie'])
            #seed = self.crc_seed
            #self.crc_seed = fc.get('crc_seed', seed)

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

        Trace.trace(10, "finish_transfer_setup: label %s state %s"%(volume_label, state_name(self.save_state)))
        Trace.trace(10, "finish_transfer_setup: ticket %s"%(self.current_work_ticket,))
        # this is for crc check in ASSERT mode
        Trace.trace(24, "finish_transfer_setup MODE %s"%(mode_name(self.mode),))
        if self.mode == ASSERT:
            return
        #######
        Trace.trace(24, "setup_transfer: save_state %s volume %s cur %s "%(state_name(self.save_state),volume_label, self.current_volume))

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
        """
        At this point the media changer claims the correct volume is loaded, Now position it.

        :type verify_label: :obj:`int`
        :arg verify_label: verify tape label if 1
        """

        label_tape = 0
        have_tape = 0
        err = None
        for retry_open in range(3):
            Trace.trace(10, "position media")
            try:
                have_tape = self.tape_driver.open(self.device, self.mode, retry_count=30)
            except self.ftt.FTTError, detail:
                Trace.alarm(e_errors.ERROR,"Supposedly a serious problem with tape drive positioning the tape: %s %s."%(self.ftt.FTTError, detail))
                self.transfer_failed(e_errors.POSITIONING_ERROR, "Serious FTT error %s"%(detail,), error_source=DRIVE)
                return

            if have_tape == 1:
                if self.mode == WRITE and self.tape_driver.mode == READ:
                    Trace.alarm(e_errors.ERROR, "tape %s is write protected, will be set read-only"%
                                (self.current_volume,))
                    self.vcc.set_system_readonly(self.current_volume)
                    self.vcc.set_comment(self.current_volume, "write-protected")
                    self.send_client_done(self.current_work_ticket, e_errors.WRITE_ERROR,
                                          "tape %s is write protected"%(self.current_volume,))
                    self.net_driver.close()
                    self.network_write_active = False # reset to indicate no network activity

                    thread = threading.currentThread()
                    if thread:
                        thread_name = thread.getName()
                    else:
                        thread_name = None
                    if thread_name and thread_name == 'media_thread':
                        self.dismount_volume(after_function=self.idle)
                    else:
                        self.run_in_thread('media_thread', self.dismount_volume, after_function=self.idle)

                    #self.dismount_volume(after_function=self.idle)
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

        if self.mode == WRITE and eod == None:
            verify_label = 0
            label_tape = 1

        if self.mode == WRITE:
            Trace.trace(24, "target location %s EOD cookie %s"%(self.target_location, eod))
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
                        self.set_volume_noaccess(volume_label, "Volume is already labeled")
                        Trace.alarm(e_errors.ERROR, msg)
                        Trace.log(e_errors.ERROR, "marking %s noaccess" % (volume_label,))
                        self.transfer_failed(e_errors.WRITE_VOL1_WRONG, msg, error_source=TAPE)
                        return 0

                try:
                    Trace.trace(10,"rewind")
                    self.tape_driver.rewind()
                    if self.driver_type == 'FTTDriver':
                        time.sleep(3)
                        stats = self.tape_driver.get_stats()
                        Trace.trace(10,"WRITE_PROT=%s"%(stats[self.ftt.WRITE_PROT],))
                        write_prot = stats[self.ftt.WRITE_PROT]
                        if type(write_prot) is type(''):
                            write_prot = string.atoi(write_prot)
                        if write_prot:
                            #self.vcc.set_system_noaccess(volume_label)
                            self.set_volume_noaccess(volume_label, "Attempt to label write protected tape")
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
		    ##
                    ##    Trace.trace(42, "WAYNE DEBUG: rewriting label")
                    ##    self.tape_driver.write(vol1_label, 0, 80)
                    ##    self.tape_driver.writefm()
	            # END WAYNE FOO

                except self.ftt.FTTError, detail:
                    self.transfer_failed(e_errors.WRITE_ERROR, detail, error_source=TAPE)
                    return 0
                except:
                    exc, detail, tb = sys.exc_info()
                    Trace.handle_error(exc, detail, tb)
                    self.transfer_failed(e_errors.WRITE_ERROR, detail, error_source=TAPE)
                    return 0
                eod = 1
                self.target_location = eod
                self.vol_info['eod_cookie'] = loc_to_cookie(eod)
                if self.driver_type == 'FTTDriver' and self.rem_stats:
                    stats = self.tape_driver.get_stats()
                    remaining = stats[self.ftt.REMAIN_TAPE]
                    if remaining is not None:
                        remaining = long(remaining)
                        self.vol_info['remaining_bytes'] = remaining * 1024L
                        ##XXX keep everything in KB?
                ret = self.vcc.set_remaining_bytes(volume_label,
                                                   self.vol_info['remaining_bytes'],
                                                   self.vol_info['eod_cookie'])
                if ret['status'][0] != e_errors.OK:
                    self.set_volume_noaccess(self.current_volume, "Failed to update volume information. See log for details")
                    Trace.alarm(e_errors.ALARM, "Failed to update volume information on %s, EOD %s. May cause a data loss."%
                                (self.current_volume, self.vol_info['eod_cookie']))
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
        """
        Declare that transfer is failed and complete it accordingly.

        :type exc: :obj:`~exceptions.Exception`
        :arg exc: returned by exception
        :type msg: :obj:`str`
        :arg msg: additional message
        :type error_source: :obj:`int`
        :arg error_source: :obj:`int`
        :type dismount_allowed: :obj:`int`
        :arg dismount_allowed: initiate tape dismount if 1
        """

        exc = str(exc)

        if self.state == OFFLINE:
            # transfer failed should not get called in OFFLINE state
            return
        if exc == e_errors.MEMORY_ERROR:
            self.memory_error = 1
        else:
            self.memory_error = 0

        #self.init_data_buffer() # reset buffer
        self.timer('transfer_time')
        after_dismount_function = None
        volume_label = self.current_volume
        ticket = self.current_work_ticket
        if not ticket.has_key('times'):
            ticket['times']={}
        t = self.media_transfer_time
        if t == 0.:
            t = ticket['times'].get('transfer_time', 0.)
        ticket['times']['drive_transfer_time'] = t
        ticket['times']['drive_call_time'] = self.tape_driver.tape_transfer_time()
        self.log_state()
        if self.tr_failed:
            return          ## this function has been alredy called in the other thread
        self.tr_failed = 1
        broken = ""
        ftt_eio =0

        if type(msg) != type(""):
            msg = str(msg)

        # get the current thread
        cur_thread = threading.currentThread()
        if cur_thread:
            cur_thread_name = cur_thread.getName()
        else:
            cur_thread_name = None

        Trace.log(e_errors.ERROR, "transfer failed %s %s %s volume=%s location=%s thread %s" % (
            exc, msg, error_source,self.current_volume, self.current_location, cur_thread_name))
        Trace.notify("disconnect %s %s" % (self.shortname, self.client_ip))
        self._error = exc
        self._error_source = error_source
        if exc == e_errors.WRITE_ERROR or exc == e_errors.READ_ERROR or exc == e_errors.POSITIONING_ERROR:
            if (msg.find("FTT_") != -1):
                # log low level diagnostics
                self.watch_syslog()

            if (msg.find("FTT_EIO") != -1):
                ftt_eio = 1
            elif msg.find("FTT_EBLANK") != -1:
                if self.mode == WRITE:
                    self.vcc.set_system_readonly(self.current_volume)
                    Trace.alarm(e_errors.ERROR,
                                "Write error on %s. See log for details. Volume is set readonly"%
                                (self.current_volume))
                    # also set volume to NOACCESS, so far
                    # no alarm is needed here because it is send by volume clerk
                    # when it sets a volume to NOACCESS
                    self.set_volume_noaccess(self.current_volume, "Write error. See log for details")
                    # log all running proceses
                    self.log_processes(logit=1)

                if self.stop:
                    self.offline() # stop here for investigation
                    return
            elif msg.find("FTT_EUNRECOVERED") != -1:
                Trace.alarm(e_errors.ERROR, "encountered FTT_EUNRECOVERED error. Going OFFLINE. Please check the tape drive")
                self.set_volume_noaccess(volume_label, "encountered FTT_EUNRECOVERED error. See log for details")

                self.offline() # stop here for investigation
                return

        ### XXX translate this to an e_errors code?
        self.last_error = str(exc), str(msg)

        if self.state == ERROR:
            Trace.log(e_errors.ERROR, "Mover already in ERROR state %s, state=ERROR" % (msg,))
            self.tr_failed = 0
            return

        # this only can happen when initial communication with get failed
        # just return here as no tape was mounted yet
        if self.udp_control_address and self.udp_cm_sent:
            Trace.trace(98, "calling nowork")
            self.nowork({})

        if exc not in (e_errors.ENCP_GONE, e_errors.ENCP_STUCK, e_errors.READ_VOL1_WRONG, e_errors.WRITE_VOL1_WRONG, e_errors.MEMORY_ERROR):
            if msg.find("FTT_EBUSY") != -1:
                # tape thread stuck in D state - offline mover
                after_dismount_function = self.offline
                Trace.alarm(e_errors.ERROR, "tape thread is possibly stuck in D state")
                self.log_state(logit=1)
            if error_source == DRIVE:
                Trace.log(e_errors.ERROR, "MODE %s"%(self.mode,))
                Trace.alarm(e_errors.ERROR, "Possible drive failure %s %s"%(exc, msg))
                if self.mode == WRITE:
                    # offline mover only if it was writing
                    # for READ and ASSERT
                    # allow to proceed
                    # the consecutive_failures count will
                    # take care of the rest
                    after_dismount_function = self.offline

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
                # Heuristic: if tape is more than  remaining_factor*capacity full and we get a write error,
                # mark it full
                try:
                    capacity = self.vol_info['capacity_bytes']
                    remaining = self.vol_info['remaining_bytes']
                    eod = self.vol_info['eod_cookie']
                    if remaining <= self.remaining_factor * capacity:
                        Trace.log(e_errors.INFO,
                                  "heuristic: write error on vol %s, remaining=%s, capacity=%s, marking volume full"%
                                  (self.current_volume, remaining, capacity))
                        ret = self.vcc.set_remaining_bytes(self.current_volume, 0, eod, None)
                        if ret['status'][0] != e_errors.OK or ret['eod_cookie'] != eod:
                            Trace.alarm(e_errors.ERROR, "set_remaining_bytes failed", ret)
                            self.set_volume_noaccess(self.current_volume, "Failed to update volume information. See log for details")
                            Trace.alarm(e_errors.ALARM, "Failed to update volume information on %s, EOD %s. May cause a data loss."%
                                        (self.current_volume, self.vol_info['eod_cookie']))

                            broken = broken +  "set_remaining_bytes failed"

                except:
                    exc, msg, tb = sys.exc_info()
                    Trace.log(e_errors.ERROR, "%s %s" % (exc, msg))
            else:
                if self.vcc:
                    self.vcc.update_counts(self.current_volume, rd_err=1, rd_access=1)

            self.transfers_failed = self.transfers_failed + 1
        encp_gone = exc in (e_errors.ENCP_GONE, e_errors.ENCP_STUCK)

        save_state = self.state

        if (cur_thread_name == 'net_thread' or
            (cur_thread_name == 'media_thread' and exc == e_errors.DISMOUNTFAILED) or
            encp_gone):
            #For the 2nd entry in if ... If dismount fails close net_driver (data connection).
            # If there is a preemptive dismount and net_driver is not closed,
            # the client (encp) port does not get disconnected.
            #This resulted in 15 min timeout for encp retry.
            # When fixed, encp retries immediately. (bz # 767)
            self.net_driver.close()
            self.network_write_active = False # reset to indicate no network activity

        if self.mode == ASSERT:
            return_here = False
            if (any(s in msg for s in ("FTT_EBLANK", "FTT_EBUSY", "FTT_EIO", "FTT_ENODEV", "FTT_ENOTAPE", MEDIA_VERIFY_FAILED))
                or exc == e_errors.ENCP_GONE):
                # stop assert
                pass
            else:
                self.tr_failed = 0 # to let assert finish
                self.assert_return = exc
                return_here = True
                #return
            self._error = exc
            self.assert_ok.set()
            if return_here:
                return

        self.send_client_done(self.current_work_ticket, str(exc), str(msg))
        if exc == e_errors.MOVER_STUCK:
            self.log_state(logit=1)
            broken = exc

        # if encp is gone there is no need to dismount a tape
        dism_allowed = not encp_gone
        dism_allowed = dism_allowed & dismount_allowed
        Trace.trace(26,"current thread %s encp_gone %s"%(cur_thread_name, encp_gone))
        # to avoid false mover restart in state have bound
        # when transfer_failed is called from the net thread
        # use dont_update_lm flag
        self.dont_update_lm = 0
        if cur_thread_name:
            if cur_thread_name == 'net_thread':
                act_thread_name = 'net_thread'
                alt_thread_name = 'tape_thread'
            elif cur_thread_name == 'tape_thread':
                alt_thread_name = 'net_thread'
                act_thread_name = 'tape_thread'
            else:
               act_thread_name = None
            if act_thread_name:
                self.dont_update_lm = 1
                # check if tape_thread is active before allowing dismount
                Trace.trace(26,"checking thread %s"%(alt_thread_name,))
                thread = getattr(self, alt_thread_name, None)
                for wait in range(60):
                    if thread and thread.isAlive():
                        Trace.trace(26, "thread %s is already running, waiting %s" % (alt_thread_name, wait))
                        time.sleep(1)
                    else:
                        break
        Trace.trace(26,"dismount_allowed %s after_dismount %s"%(dism_allowed, after_dismount_function))
        if encp_gone:
            last_location = self.current_location
            try:
                self.current_location, block = self.tape_driver.tell()
            except self.ftt.FTTError, detail:
                Trace.alarm(e_errors.ERROR, 'Can not get drive info %s' % (detail,))
                if self.mode == WRITE:
                    self.vcc.set_system_readonly(self.current_volume)
                self.dismount_volume(after_function=self.idle)
                return

            if (self.header_labels and
                (self.current_location - last_location == 1) and
                self.mode == WRITE):
                # This case is for cern alike headers,
                # which are separated from data by fm.
                # If header was written before
                # encp was gone,
                # the current location will be 1 more than in
                # case of cpio wrapper, which is not separated from
                # data with fm
                Trace.trace(10, "for cern wrapper will set loc to %s"%(last_location,))
                self.current_location = last_location

            self.dismount_time = time.time() + self.delay
            if self.state == IDLE:
                pass
            else:
                self.state = HAVE_BOUND
                if self.maybe_clean():
                    Trace.trace(26,"cleaned")
                    self.dont_update_lm = 0
                    return

        self.send_error_msg(error_info = (exc, msg),error_source=error_source)
        if not ftt_eio:
            self.need_lm_update = (1, ERROR, 1, error_source)
        else:
            Trace.alarm(e_errors.ERROR,"FTT_EIO: tape drive or IC problem")
        if ((exc in (e_errors.READ_VOL1_WRONG,
                   e_errors.WRITE_VOL1_WRONG,
                   e_errors.WRITE_VOL1_MISSING,
                   e_errors.READ_VOL1_MISSING,
                   e_errors.READ_VOL1_READ_ERR,
                   e_errors.WRITE_VOL1_READ_ERR,
                   e_errors.MOVER_STUCK))):
            self.set_volume_noaccess(volume_label, "Error: %s"%(exc,))
        if ftt_eio and self.mode != WRITE:
            # if it was WRITE then the tape was set to readonly, which is enough
            # action for tape
            self.set_volume_noaccess(volume_label, "FTT_EIO error. See log for details")
        if dism_allowed:
            if exc == e_errors.MEMORY_ERROR:
                Trace.log(e_errors.ERROR, "Memory error, restarting mover")
                self.log_state(logit=1)
                self.dump_vars()
                self.run_in_thread('media_thread', self.dismount_volume, after_function=self.restart)
            if self.draining:
                self.run_in_thread('media_thread', self.dismount_volume, after_function=self.offline)

                #self.dismount_volume(after_function=self.offline)
            else:
                if not after_dismount_function:
                    if not self.maybe_clean():
                        if cur_thread_name and cur_thread_name == 'media_thread':
                            self.dismount_volume(after_function=self.idle)
                        else:
                            self.run_in_thread('media_thread', self.dismount_volume, after_function=self.idle)

                else:
                    if cur_thread_name and cur_thread_name == 'media_thread':
                        self.dismount_volume(after_function=self.idle)
                    else:
                        self.run_in_thread('media_thread', self.dismount_volume, after_function=after_dismount_function)

            #self.tr_failed = 0
            self.dont_update_lm = 0
            return

        if not after_dismount_function and broken:
            self.broken(broken, exc)
            self.tr_failed = 0
            self.dont_update_lm = 0
            return

        #self.tr_failed = 0
        self.dont_update_lm = 0

    def transfer_completed(self, error=None):
        """
        Complete the transfer.

        :type  error: :obj:`int`
        :arg  error: error code (see source)
        """

        self.init_data_buffer() # reset (buffer)
        # simple synchonizatin between tape and network threads.
        # without this not updated file info is transferred to get
        for i in range(10):
            if self.lock_file_info:
                time.sleep(.3)
            else:
                break
        else:
            Trace.log(e_errors.ERROR,"did not get file info lock")
        self.lock_file_info = 0
        self.consecutive_failures = 0
        self.timer('transfer_time')
        Trace.trace(24, "transfer_completed. ticket %s"%(self.current_work_ticket,))
        ticket = self.current_work_ticket
        if not ticket.has_key('times'):
            ticket['times']={}

        t = self.media_transfer_time
        if t == 0.:
            t = ticket['times'].get('transfer_time', 0.)
        ticket['times']['drive_transfer_time'] = t
        ticket['times']['drive_call_time'] = self.tape_driver.tape_transfer_time()


        Trace.log(e_errors.INFO, "transfer complete volume=%s location=%s"%(
            self.current_volume, self.current_location))
        Trace.notify("disconnect %s %s" % (self.shortname, self.client_ip))
        if self.mode == WRITE:
            self.write_count_per_mount += 1
            self.vcc.update_counts(self.current_volume, wr_access=1)
        else:
            self.read_count_per_mount += 1
            self.vcc.update_counts(self.current_volume, rd_access=1)
        self.transfers_completed = self.transfers_completed + 1
        self.net_driver.close()
        self.network_write_active = False # reset to indicate no network activity
        now = time.time()
        self.dismount_time = now + self.delay
        if error:
            ret_err = error
        else:
            ret_err = e_errors.OK
        self.send_client_done(self.current_work_ticket, ret_err)
        try:
            self.current_location, block = self.tape_driver.tell()
        except  self.ftt.FTTError, detail:
            self.transfer_failed(e_errors.DRIVEERROR, 'Can not get drive info %s' % (detail,),
                                 error_source=TAPE)
            self.dismount_volume(after_function=self.offline)
            return
        except:
            exc, detail, tb = sys.exc_info()
            self.transfer_failed(e_errors.DRIVEERROR, 'Can not get drive info %s %s' % (exc, detail,),
                                 error_source=TAPE)
            self.dismount_volume(after_function=self.offline)
            return


        if hasattr(self,'too_long_in_state_sent'):
            del(self.too_long_in_state_sent)

        if self.draining:
            self.run_in_thread('media_thread', self.dismount_volume, after_function=self.offline)
            #self.dismount_volume(after_function=self.offline)
            self.log_state()
            self.need_lm_update = (1, None, 1, None)
            return

        else:
            self.state = HAVE_BOUND
            self.maybe_clean()
        self.log_state()
        self.need_lm_update = (1, None, 1, None)

    def maybe_clean(self):
        """
        Check if tape is clean.
        """

        if self.memory_error:
            return 1
        Trace.log(e_errors.INFO, "maybe_clean")
        cur_thread = threading.currentThread()
        if cur_thread:
            cur_thread_name = cur_thread.getName()
        else:
             cur_thread_name = None
        if cur_thread_name and cur_thread_name != "tape_thread":
            # wait until tape thread finishes
            th = getattr(self, 'tape_thread', None)
            for wait in range(60):
                if th and th.isAlive():
                    Trace.trace(26, "thread %s is already running, waiting %s" % ('tape_thread', wait))
                    time.sleep(2)
                else:
                    break

        if self.force_clean:
             needs_cleaning = 1
             Trace.log(e_errors.INFO, "Force clean is set")
        else:
            try:
                needs_cleaning = self.tape_driver.get_cleaning_bit()
            except self.ftt.FTTError, detail:
                Trace.alarm(e_errors.ALARM,"Possible Drive problem %s %s"%(self.ftt.FTTError, detail))
                self.set_volume_noaccess(self.current_volume, "Possible drive problem. See log for details")
                if not self.stop: # otherwise do not dismount for further diagnostics
                    self.dismount_volume()
                self.offline()
                return 1

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
            self.state = IDLE
        Trace.log(e_errors.INFO, "returned from maybe_clean")
        return did_cleaning

    def update_after_writing(self):
        """
        Update necessary information after file was successfully written to tape.

        """
        previous_eod = cookie_to_long(self.vol_info['eod_cookie'])
        eod_increment = 0
        if self.header_labels:
           eod_increment = eod_increment + 1

        try:
            self.current_location, block = self.tape_driver.tell()
        except  self.ftt.FTTError, detail:
            self.transfer_failed(e_errors.WRITE_ERROR, 'Can not get drive info %s' % (detail,),
                                 error_source=TAPE)
            return
        except:
            exc, detail, tb = sys.exc_info()
            self.transfer_failed(e_errors.WRITE_ERROR, 'Can not get drive info %s %s' % (exc, detail,),
                                 error_source=TAPE)
            return

        if self.current_location <= previous_eod:
            Trace.log(e_errors.ERROR, " current location %s <= eod %s" %
                      (self.current_location, previous_eod))
            return 0

        r0 = self.vol_info['remaining_bytes']  #value prior to this write
        r1 = r0 - self.bytes_written           #value derived from simple subtraction
        r2 = r1                                #value reported from drive, if possible
        Trace.trace(24, "remainingbytes info in DB %s" % (r2,))
        ## XXX OO: this should be a driver method
        if self.driver_type == 'FTTDriver' and self.rem_stats:
            stats = None
            failed = 0
            try:
                stats = self.tape_driver.get_stats()
                r2 = long(stats[self.ftt.REMAIN_TAPE]) * 1024L
                Trace.trace(24, "reported remaining %s" % (r2,))
            except self.ftt.FTTError, detail:
                failed = 1
            except:
                exc, detail, tb = sys.exc_info()
                Trace.handle_error(exc, detail, tb)
                failed = 1
                try:
                    Trace.log(e_errors.ERROR, "REMAIN_TAPE: type %s value %s"%
                              (type(stats[self.ftt.REMAIN_TAPE]), stats[self.ftt.REMAIN_TAPE]))
                except:
                    exc, detail, tb = sys.exc_info()
                    Trace.handle_error(exc, detail, tb)
            if failed:
                self.vcc.set_system_readonly(self.current_volume)
                Trace.alarm(e_errors.ERROR, "ftt error on %s after write, detail %s. Volume is set readonly" %
                            (self.current_volume, detail))
                # no alarm is needed here because it is send by volume clerk
                # when it sets a volume to NOACCESS
                self.set_volume_noaccess(self.current_volume, "ftt error on %s after write. See log for details")
                # log all running proceses
                self.log_processes(logit=1)
                self.transfer_failed(e_errors.ERROR, "ftt.get_stats: FTT_ERROR %s"%(detail,), error_source=DRIVE)
                return 0

        Trace.log(e_errors.INFO,
                  "remaining bytes info in DB %s estimated %s reported from drive %s position %s" % (r0, r1, r2,self.current_location))

        capacity = self.vol_info['capacity_bytes']
        # check remaining bytes, it must be less than a previous
        # this does not work for 9940B (it reports the same remaining bytes after write as it was before)
        # for LTO-2 the spread in values is somewhat random
        '''
        if (r2 > r1):
            # set volume read only and noaccess
            self.vcc.set_system_readonly(self.current_volume)
            self.set_volume_noaccess(self.current_volume)
            self.transfer_failed(e_errors.WRITE_ERROR, 'Wrong remaining bytes count', error_source=DRIVE)
            Trace.alarm(e_errors.ALARM, 'Wrong remaining bytes count detected: prev %s current %s expected %s'%(r0, r2, r1))
            return 0
        '''
        if r1 <= self.remaining_factor * capacity:  #do not allow remaining capacity to decrease in the "near-EOT" regime
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
                       'complete_crc': complete_crc,
                       'gid': self.gid,
                       'uid': self.uid,
                       'pnfs_name0': self.current_work_ticket['outfilepath'],
                       'pnfsid':  self.file_info['pnfsid'],
                       'drive': "%s:%s" % (self.current_work_ticket['mover']['device'], self.config['serial_num']),
                       'original_library': self.current_work_ticket.get('original_library'),
                       'file_family_width': self.vol_info.get('file_family_width'),
                       'mover_type': self.mover_type,
                       'unique_id': self.current_work_ticket['unique_id'],
}
        ##  HACK:  store 0 to database if mover is NULL
        if self.config['driver']=='NullDriver':
            fc_ticket['complete_crc']=0L
            fc_ticket['sanity_cookie']=(self.buffer.sanity_bytes,0L)

        #If it is an original of multiple copies, pass this along.
        copies = self.file_info.get('copies')
        if copies:
           fc_ticket['copies'] = copies

        # if it is a copy, make sure that original_bfid is passed along
        if self.current_work_ticket.has_key('copy'):
            original_bfid = self.file_info.get('original_bfid')
            if not original_bfid:
                self.transfer_failed(e_errors.ERROR,'file clerk error: missing original bfid for copy')
                return 0
            fc_ticket['original_bfid'] = original_bfid

        fcc_reply = self.set_new_bitfile(fc_ticket, self.vol_info)
        if not fcc_reply:
            return fcc_reply
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
            if reply['status'][0] != e_errors.OK or reply['eod_cookie'] != eod:
                if reply['status'][0] == e_errors.TIMEDOUT:
                    # keep trying
                    Trace.alarm(e_errors.ERROR,"Volume Clerk timeout on the final stage of file writing to %s"%(self.current_volume))
                else:
                    self.set_volume_noaccess(self.current_volume, "Failed to update volume information. See log for details")
                    Trace.alarm(e_errors.ALARM, "Failed to update volume information on %s, EOD %s. May cause a data loss."%
                                (self.current_volume, self.vol_info['eod_cookie']))
                    self.transfer_failed(reply['status'][0], reply['status'][1], error_source=TAPE)
                    finish_writing = 0
                    return 0
            else:
               finish_writing = 0
        self.vol_info.update(reply)
        return 1

    def malformed_ticket(self, ticket, expected_keys=None):
        """
        Check if ticked received from library manager
        conforms to rules.
        """

        msg = "Missing keys "
        if expected_keys is not None:
            msg = "%s %s"(msg, expected_keys)
        msg = "%s %s"%(msg, ticket)
        Trace.log(e_errors.ERROR, msg)

    def send_client_done(self, ticket, status, error_info=None):
        """
        Send ticket to client (encp, volume_assert) the final information.

        """
        Trace.trace(13, "send_client_done %s"%(self.control_socket))
        Trace.trace(13, "send_client_done status %s error_info %s ticket %s"%(status, error_info, ticket))
        if self.control_socket == None:
            return
        ticket['status'] = (status, error_info)
        Trace.log(e_errors.INFO, "Sending done to client: %s"%(ticket))
        Trace.log(e_errors.INFO, "Sending done to client: %s"%(ticket['status'],))
        if ticket['status'][0] != e_errors.ENCP_GONE:
            try:
                callback.write_tcp_obj(self.control_socket, ticket)
            except:
                exc, detail, tb = sys.exc_info()
                Trace.log(e_errors.ERROR, "error in send_client_done: %s" % (detail,))

        if ((self.method and self.method != 'read_next') or
            (self.method == None)):
            # close sockets only for general case
            # in case of tape reads do not close them
            try:
                self.control_socket.close()
                if self.listen_socket:
                    self.listen_socket.close()
            except:
                exc, detail, tb = sys.exc_info()
                Trace.log(e_errors.ERROR, "error closing control and listen sockets: %s" % (detail,))
            self.control_socket = None
            self.listen_socket = None

        Trace.log(e_errors.INFO, "Done is sent")
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
        """
        Connect to client (encp) to setup the data transfer.

        """

        self.client_socket = None
        # run this in a thread
        self.host = None
        ticket = self.current_work_ticket
        data_ip=self.config.get("data_ip",None)
        Trace.trace(10, "data ip %s"%(data_ip,))

        # Coordinate IP protocol (IPV4 or IPV6) with client
        host_ip = None
        caller_address_family = socket.getaddrinfo(ticket['callback_addr'][0], None)[0][0]
        hostinfo = socket.getaddrinfo(socket.gethostname(), caller_address_family)
        my_address_family = hostinfo[0][0]
        if my_address_family != caller_address_family:
            # Select mover host IP according to the caller address family
            for e in hostinfo:
             if e[0] == caller_address_family:
                host_ip = e[4][0]
        try:
            if (not self.method) or self.method and self.method != 'read_next':
                try:
                    host, port, self.listen_socket = callback.get_callback(ip=data_ip if data_ip else host_ip)
                except Exception, detail:
                    exc, msg, tb = sys.exc_info()
                    Trace.log(e_errors.ERROR, "connect_client: Connection to data ip failed:  %s %s %s"%
                      (exc, msg, traceback.format_tb(tb)))
                    Trace.alarm(e_errors.ERROR, "Connection to data ip failed: %s"%(detail,))
                    self.dismount_volume(after_function=self.offline)
                    return

                self.host = host
            #self.listen_socket.listen(1)
            #if self.method and self.method == 'read_tape_start':
            #    self.udp_control_address = ticket.get('routing_callback_addr', None)
            #    if self.udp_control_address:
            #        self.reset_interval_timer(self.update_lm)
            #        self.lm_address_saved = self.lm_address
            #        self.lm_address = self.udp_control_address
            #        Trace.trace(98, "LM address %s"%(self.lm_address,))
            #        self.state = IDLE
            #        self.udp_ext_control_address =("get", self.lm_address)
            #        self.libraries = [self.udp_ext_control_address]

            #self.listen_socket.listen(1)
            if (not self.method) or (self.method and self.method != 'read_next'):
                self.listen_socket.listen(1)
                # need a control connection setup
                # otherwise: not because it must be left open
                ticket['mover']['callback_addr'] = (host,port) #client expects this
                ticket['mover']['mover_address'] = (host, self.config['port'])

                self.control_socket = socket.socket(caller_address_family, socket.SOCK_STREAM)
                flags = fcntl.fcntl(self.control_socket.fileno(), fcntl.F_GETFL)
                fcntl.fcntl(self.control_socket.fileno(), fcntl.F_SETFL, flags | os.O_NONBLOCK)
                # the following insertion is for antispoofing
                if ticket.has_key('route_selection') and ticket['route_selection']:
                    ticket['mover_ip'] = host
                    # bind control socket to data ip
                    self.control_socket.bind((host, 0))
                    u = udp_client.UDPClient()
                    Trace.trace(10, "sending IP %s to %s. whole ticket %s"%
                                (host, ticket['routing_callback_addr'], ticket))
                    Trace.trace(10, "callback socket %s" % (u.get_tsd().socket.getsockname(),))
                    try:
                        x= u.send(ticket,ticket['routing_callback_addr'] , self.connect_to, self.connect_retry, 0)
                    except (socket.error, select.error, e_errors.EnstoreError), msg:
                        Trace.log(e_errors.ERROR, "error sending to %s (%s)" %
                                  (ticket['routing_callback_addr'], str(msg)))
                        self.del_udp_client(u)
                        #del u
                        # just for a case
                        try:
                            self.control_socket.close()
                            self.listen_socket.close()
                            self.control_socket,self.listen_socket = None, None
                        except:
                            pass
                        self.client_socket = None
                        self.run_in_thread('finish_transfer_setup_thread', self.finish_transfer_setup)
                        return
                    except errno.errorcode[errno.ETIMEDOUT]:
                        Trace.log(e_errors.ERROR, "error sending to %s (%s)" %
                                  (ticket['routing_callback_addr'], os.strerror(errno.ETIMEDOUT)))
                        self.del_udp_client(u)
                        #del u
                        # just for a case
                        try:
                            self.control_socket.close()
                            self.listen_socket.close()
                            self.control_socket,self.listen_socket = None, None
                        except:
                            pass
                        self.client_socket = None
                        self.run_in_thread('finish_transfer_setup_thread', self.finish_transfer_setup)
                        return
                    Trace.trace(10, "encp called back with %s"%(x,))
                    if x.has_key('callback_addr'): ticket['callback_addr'] = x['callback_addr']
                    self.del_udp_client(u)
                    #del u
                if self.method and self.method == 'read_tape_start':
                    self.udp_control_address = ticket.get('routing_callback_addr', None)
                    if self.udp_control_address:
                        # make sure that
                        self.reset_interval_timer(self.update_lm)
                        self.lm_address_saved = self.lm_address
                        self.lm_address = self.udp_control_address
                        Trace.trace(98, "LM address %s"%(self.lm_address,))
                        self.state = IDLE
                        self.udp_ext_control_address =("get", self.lm_address)
                        self.libraries = [self.udp_ext_control_address]
                Trace.trace(10, "connecting to %s" % (ticket['callback_addr'],))
                try:
                    self.control_socket.connect(ticket['callback_addr'])
                except socket.error, detail:
                    #Trace.log(e_errors.ERROR, "%s %s" %
                    #          (detail, ticket['callback_addr']))
                    #We have seen that on IRIX, when the connection succeds, we
                    # get an ISCONN error.
                    if hasattr(errno, 'EISCONN') and detail[0] == errno.EISCONN:
                        pass
                    #The TCP handshake is in progress.
                    elif detail[0] == errno.EINPROGRESS:
                        pass
                    else:
                        Trace.log(e_errors.ERROR, "error connecting to %s (%s)" %
                                  (ticket['callback_addr'], detail))
                        # just for a case
                        try:
                            self.control_socket.close()
                            self.listen_socket.close()
                            self.control_socket,self.listen_socket = None, None
                        except:
                            pass

                        self.client_socket = None
                        self.run_in_thread('finish_transfer_setup_thread', self.finish_transfer_setup)
                        return

                #Check if the socket is open for reading and/or writing.
                r, w, ex = select.select([self.control_socket], [self.control_socket], [], self.connect_to*self.connect_retry)

                if r or w:
                    #Get the socket error condition...
                    rtn = self.control_socket.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
                else:
                    Trace.log(e_errors.ERROR, "error connecting to %s (%s)" %
                              (ticket['callback_addr'], os.strerror(errno.ETIMEDOUT)))
                    # just for a case
                    try:
                        self.control_socket.close()
                        self.listen_socket.close()
                        self.control_socket,self.listen_socket = None, None
                    except:
                        pass

                    self.client_socket = None
                    self.run_in_thread('finish_transfer_setup_thread', self.finish_transfer_setup)
                    return
                #...if it is zero then success, otherwise it failed.
                if rtn != 0:
                    Trace.log(e_errors.ERROR, "error connecting to %s (%s)" %
                              (ticket['callback_addr'], os.strerror(rtn)))
                    # just for a case
                    try:
                        self.control_socket.close()
                        self.listen_socket.close()
                        self.control_socket,self.listen_socket = None, None
                    except:
                        pass

                    self.client_socket = None
                    self.run_in_thread('finish_transfer_setup_thread', self.finish_transfer_setup)
                    return

                # we have a connection
                fcntl.fcntl(self.control_socket.fileno(), fcntl.F_SETFL, flags)
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
                            if ticket['work'] == 'write_to_hsm':
                                #self.tmp_vf is only set for writes
                                wrapper_type = volume_family.extract_wrapper(self.tmp_vf)
                                if wrapper_type != "null":
                                    ticket['status']=(e_errors.USERERROR, 'only "null" wrapper is allowed for NULL mover')
                                    #self.send_client_done(ticket, e_errors.USERERROR,
                                    #                      'only "null" wrapper is allowed for NULL mover')
                                    self.state = self.save_state
                                    null_err = 1

                    if self.setup_mode == ASSERT:
                        ticket['status'] = (e_errors.OK, None)
                    """
                    if self.method:
                        if self.method == "read_tape_start":
                            pass
                    else:
                        Trace.log (e_errors.INFO,"SENDING %s"%(ticket,))
                        callback.write_tcp_obj(self.control_socket, ticket)
                    """
                    Trace.log (e_errors.INFO,"SENDING %s"%(ticket,))
                    rtn = callback.write_tcp_obj(self.control_socket, ticket, timeout=10)
                    Trace.trace(10,"SENDING RC %s"%(rtn,))
                    if null_err:
                        # just for a case
                        try:
                            self.control_socket.close()
                            self.listen_socket.close()
                            self.control_socket,self.listen_socket = None, None
                        except:
                            pass

                        self.client_socket = None
                        self.run_in_thread('finish_transfer_setup_thread', self.finish_transfer_setup)
                        return

                    # for ASSERT finish here
                    if self.setup_mode == ASSERT:
                        self.listen_socket.close()
                        self.listen_socket = None
                        callback_addr = ticket.get('callback_addr', None)
                        if callback_addr:
                            self.client_ip = hostaddr.address_to_name(callback_addr[0])
                        else:
                            self.client_ip = ticket['wrapper']['machine'][1]
                        self.run_in_thread('volume_assert_thread', self.assert_vol)
                        return
                except:
                    exc, detail, tb = sys.exc_info()
                    Trace.log(e_errors.ERROR,"error in connect_client: %s" % (detail,))
                    # just for a case
                    try:
                        self.control_socket.close()
                        self.listen_socket.close()
                        self.control_socket,self.listen_socket = None, None
                    except:
                        pass
                    self.client_socket = None
                    self.run_in_thread('finish_transfer_setup_thread', self.finish_transfer_setup)
                    return

            if self.method == "read_tape_start":
                # do not establish data connection yet
                return
            # we expect a prompt call-back here
            # establish a data connection
            Trace.trace(10, "select: listening for client callback")
            read_fds,write_fds,exc_fds=select.select([self.listen_socket],[],[],20) # 20 s TO
            Trace.trace(10, "select returned %s" % ((self.listen_socket in read_fds),))

            if self.listen_socket in read_fds:
                Trace.trace(10, "accepting client connection")
                self.client_socket, address = self.listen_socket.accept()
                if not hostaddr.allow(address):
                    # just for a case
                    try:
                        if (not self.method) or (self.method and self.method != 'read_next'):
                            # close sockets only for general case
                            # in case of tape reads do not close them
                            self.control_socket.close()
                            self.listen_socket.close()
                            self.control_socket,self.listen_socket = None, None
                        self.client_socket.close()
                    except:
                        pass
		    self.client_socket = None
                    self.run_in_thread('finish_transfer_setup_thread', self.finish_transfer_setup)
                    return
                if data_ip:
                    # binding to socket to device if data_ip is the same as host ip
                    # results in connection reset if file is trasferred on the same machine
                    # where mover runs.
                    # this is why we bind to device only if network interfaces are different

                    host_addr = self.control_socket.getsockname()[0]
                    client_host_addr = hostaddr.name_to_address(self.client_hostname)
                    data_interface = hostaddr.interface_name(data_ip)
                    #host_interface = hostaddr.interface_name(host_addr)

                    Trace.trace(10, "client_host_addr %s host_addr %s data_ip %s"%
                                (client_host_addr, host_addr, data_ip))
                    # Bind to device only if client and server are not on the same host
                    # otherwise the connection on the same host is refused

                    if ((client_host_addr == host_addr) or # client comes from the same host
                        (client_host_addr == data_ip)): # client comes on data ip of the same host
                        Trace.trace(10, "pass")
                        pass
                    else:
                        Trace.trace(10, "bind client socket to %s"%(data_interface,))
                        status=socket_ext.bindtodev(self.client_socket.fileno(),data_interface)
                        if status:
                            Trace.log(e_errors.ERROR, "bindtodev(%s): %s"%(data_interface,os.strerror(status)))

                if (not self.method) or (self.method and self.method != 'read_next'):
                    # close sockets only for general case
                    # in case of tape reads do not close them
                    self.listen_socket.close()
                    self.listen_socket = None
                self.client_ip = address[0]
                Trace.notify("connect %s %s" % (self.shortname, self.client_ip))
                self.net_driver.fdopen(self.client_socket)
                Trace.trace(10, "control socket %s data_socket %s"%
                            (self.control_socket.getsockname(),
                             self.client_socket.getsockname()))

                self.run_in_thread('finish_transfer_setup_thread', self.finish_transfer_setup)
                return
            else:
                Trace.log(e_errors.ERROR, "timeout on waiting for client connect")
                # just for a case
                try:
                    if (not self.method) or (self.method and self.method != 'read_next'):
                        # close sockets only for general case
                        # in case of tape reads do not close them
                        self.control_socket.close()
                        self.listen_socket.close()
                        self.control_socket,self.listen_socket = None, None
                except:
                    pass
                self.client_socket = None
                self.run_in_thread('finish_transfer_setup_thread', self.finish_transfer_setup)
                return
        except:
            exc, msg, tb = sys.exc_info()
            Trace.log(e_errors.ERROR, "connect_client:  %s %s %s"%
                      (exc, msg, traceback.format_tb(tb)))
            # just for a case
            try:
                self.control_socket.close()
                self.listen_socket.close()
                self.control_socket,self.listen_socket = None, None
            except:
                pass
            self.client_socket = None
            self.run_in_thread('finish_transfer_setup_thread', self.finish_transfer_setup)

    def format_lm_ticket(self, state=None, error_info=None, returned_work=None, error_source=None):
        """
        Format ticket to library manager to be sent by :meth:`update_lm`.

        :type state: :obj:`int`
        :arg state: mover state (see source)
        :type error_info: :obj:`tuple`
        :arg error_info: error code :obj:`str`, error description :obj:`str`
        :type returned_work: :obj:`dict`
        :arg returned_work: work ticket received from library manager
        :type error_source: :obj:`str`
        :arg error_source: ``TAPE``, ``ROBOT``, ``NETWORK``, ``DRIVE``, ``USER``, ``MOVER``, ``UNKNOWN``

        """
        status = e_errors.OK, None
        work = None
        if state is None:
            state = self.state
        Trace.trace(20,"format_lm_ticket: state %s error_info %s error_source %s"%
                    (state_name(state), error_info, error_source))
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
        elif state in (ACTIVE, SETUP, SEEK, CLEANING, MOUNT_WAIT, DISMOUNT_WAIT, FINISH_WRITE):
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
        if self.current_work_ticket:
            encp = self.current_work_ticket.get('encp', None)
            if encp:
                pri = (encp.get('curpri', None), encp.get('adminpri', None))
            else:
                pri = (None, None)
        else:
            pri = (None, None)

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
            volume_family = self.vol_info.get('volume_family', volume_family)
            volume_label = self.vol_info.get('external_label', volume_label)
        state = self.state
        #if state == DRAINING:
        #    state = ACTIVE
        ticket =  {
            "mover":  self.name,
            "address": self.address,
            "external_label":  volume_label,
            "current_location": loc_to_cookie(self.current_location),
            "read_only" : 0, ###XXX todo: multiple drives on one scsi bus, write locking
            'mover_type': self.mover_type,
            "ip_map":self.ip_map,
            "returned_work": returned_work,
            "state": state_name(state),
            "status": status,
            "volume_family": volume_family,
            "volume_status": volume_status,
            "operation": mode_name(self.mode),
            "error_source": error_source,
            "unique_id": self.unique_id,
            "work": work,
            "time_in_state": now - self.state_change_time,
            "current_time" : now,
            "library": self.current_library,
            "library_list":self.libraries, # this is needed for the federation project
            'current_priority': pri,
            }
        return ticket

    def run_in_thread(self, thread_name, function, args=(), after_function=None):
        """
        Launch the thread.

        :type thread_name: :obj:`str`
        :arg thread_name: name of thread
        :type function: :obj:`callable`
        :arg function: function to run in thread
        :type args: :obj:`tuple`
        :arg args: function arguments
        :type after_function: :obj:`callable`
        :arg after_function: function to run after function in thread is completed
        :rtype: :obj:`int` -1 if thread is already running, 0 - if thread started

        """

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
        """
        Dismount tape currently mounted in the tape drive: rewind, eject and unload.

        :type after_function: :obj:`callable`
        :arg after_function: function to run after dismount is done.
        """
        if self.current_volume == None: # no volume - no need to dismount
            if after_function:
                after_function()
            return
        Trace.trace(10, "state %s"%(state_name(self.state),))
        will_mount = self.will_mount
        self.will_mount = None
        self.just_mounted = 0
        if self.state in (IDLE, OFFLINE):
            Trace.log(e_errors.INFO, "No dismount in the state %s"%(state_name(self.state),))
            return
        broken = ""
        if self.method == 'read_next':
            self.nowork({})
        self.dismount_time = None
        self.method = None
        Trace.log(e_errors.INFO, "Updating stats")
        try:
            if self.memory_error == 0:
                self.update_stat()
        except TypeError:
            exc, msg = sys.exc_info()[:2]
            Trace.log(e_errors.ERROR, "in update_stat: %s %s" % (exc, msg))
            # perhaps it is due to scsi error
            self.watch_syslog()
        except:
            # I do not know what kind of exception this can be
            exc, msg = sys.exc_info()[:2]
            Trace.log(e_errors.ERROR, "in update_stat2: %s %s" % (exc, msg))

        Trace.trace(10, 'fm %s mode %s saved_mode %s will_mount %s'%(self.single_filemark,
                                                                     self.mode,
                                                                     self.saved_mode,
                                                                     will_mount))
        Trace.trace(10, 'error %s error_source %s'%(self._error,
                                                    self._error_source)
                    )
        Trace.trace(10, 'write_in_progress %s write_counter %s'%(self.write_in_progress,
                                                    self.write_in_progress)
                    )
        if (self.single_filemark
            and self.mode == WRITE
            and self._error != e_errors.WRITE_ERROR
            and (not (self._error_source == TAPE or self._error_source == DRIVE))):

            if (self._error == e_errors.MEMORY_ERROR and self._error_source ==  NETWORK):
                # write was interrupted in the middle by memory error in the network thread
                # do not write file mark
                pass
            elif will_mount and self.saved_mode != WRITE:
                pass
            elif self.write_counter == 0:
                # first write after tape mount transfer was interrupted on client side
                # no need to write fm
                pass
            else:
                # this case is for forced tape dismount request (HIPRI)
                # and interrupted write on the client side and
                # there was at leas one successful write sinse tape mount
                Trace.log(e_errors.INFO,"writing a tape mark before dismount")
                if self.driver_type == 'FTTDriver':
                    bloc_loc = 0L
                    if self.write_in_progress:
                        # Write was interrupted on the client side
                        # position the tape to the last fm
                        try:
                            self.tape_driver.seek(self.current_location, 0)
                        except:
                            exc, detail, tb = sys.exc_info()
                            Trace.log(e_errors.ERROR, "error positioning to last fm %s %s"%(exc, detail,))
                            return
                    self.update_tape_stats()
                    if self.write_counter != 0:
                        # try to write additional file mark
                        bloc_loc = self.bloc_loc
                        if bloc_loc != self.last_absolute_location:
                            Trace.alarm(e_errors.ERROR, "Wrong position for %s: last %s, current %s. Will set readonly"%
                                      (self.current_volume, self.last_absolute_location,
                                       bloc_loc,))
                            self.vcc.set_system_readonly(self.current_volume)
                            # also set volume to NOACCESS, so far
                            # no alarm is needed here because it is send by volume clerk
                            # when it sets a volume to NOACCESS
                            self.set_volume_noaccess(self.current_volume, "Positioning error. See log for details")
                            # log all running proceses
                            self.log_processes(logit=1)

                        else:
                            try:
                                self.tape_driver.writefm()
                            except:
                                Trace.alarm(e_errors.ERROR,"error writing file mark, will set %s readonly"%(self.current_volume,))
                                Trace.handle_error()
                                self.vcc.set_system_readonly(self.current_volume)
                                # also set volume to NOACCESS, so far
                                # no alarm is needed here because it is send by volume clerk
                                # when it sets a volume to NOACCESS
                                self.set_volume_noaccess(self.current_volume, "Error writing file mark. See log for details")
                                # log all running proceses
                                self.log_processes(logit=1)


        if not self.do_eject:
            ### AM I do not know if this is correct but it does what it supposed to
            ### Do not eject if specified
            Trace.log(e_errors.INFO, "Do not eject specified")
            self.state = HAVE_BOUND
            if self.draining:
                #self.state = OFFLINE
                self.offline()
                self.nowork({})
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
                        self.set_volume_noaccess(self.current_volume, "Eject error. See log for details")
                    except:
                        exc, msg, tb = sys.exc_info()
                        broken = broken + " set_system_noaccess failed: %s %s" %(exc, msg)

                self.broken(broken)
                self.nowork({})
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
        #self.nowork({})

    def unload_volume(self, vol_info,after_function=None):
        """
        Unload tape from drive by media changer.

        :type vol_info: :obj:`dict`
        :arg vol_info: mounted volume information.
        :type after_function: :obj:`callable`
        :arg after_function: function to run after dismount is done.
        """
        broken= ''
        Trace.notify("unload %s %s" % (self.shortname, self.current_volume))
        Trace.log(e_errors.INFO, "dismounting %s" %(self.current_volume,))
        self.asc.log_start_dismount(self.current_volume,
                                    self.config['product_id'],
                                    sg=volume_family.extract_storage_group(self.vol_info['volume_family']),
                                    reads=self.read_count_per_mount,
                                    writes=self.write_count_per_mount)

        while 1:
            mcc_reply = self.mcc.unloadvol(vol_info, self.name, self.mc_device)
            status = mcc_reply.get('status')
            if status and status[0] == e_errors.MC_QUEUE_FULL:
                # media changer responded but could not perform the operation
                Trace.log(e_errors.INFO, "Media Changer returned %s"%(status[0],))
                # to avoid false "too long in state.."
                # reset self.time_in_state
                self.time_in_state = time.time()
                time.sleep(10)
                continue
            else:
                break
        self.need_lm_update = (1, None, 0, None)
        if status and status[0] == e_errors.OK:
            self.asc.log_finish_dismount(self.current_volume)
            tm = time.localtime(time.time())
            time_msg = "%.2d:%.2d:%.2d" %  (tm[3], tm[4], tm[5])
            Trace.log(e_errors.INFO, "dismounted %s %s %s"%(self.current_volume,self.config['product_id'], time_msg))
            if self.draining:
                #self.state = OFFLINE
                self.offline()
            elif after_function:
                if self.state != ERROR:
                   self.state = IDLE
                Trace.trace(20,"after function %s" % (after_function,))
                after_function()

        ###XXX aml-specific hack! Media changer should provide a layer of abstraction
        ### on top of media changer error returns, but it doesn't  :-(
        elif status[-1] == "the drive did not contain an unloaded volume":
            self.asc.log_finish_dismount_err(self.current_volume)
            if self.draining:
                #self.state = OFFLINE
                self.offline()
            else:
                self.idle()
        else:
##            self.error(status[-1], status[0])

            self.asc.log_finish_dismount_err(self.current_volume)
            Trace.log(e_errors.ERROR, "dismount %s: %s" % (self.current_volume, status))
            self.last_error = status
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
                if self.control_socket:
                    self.send_client_done(self.current_work_ticket, e_errors.DISMOUNTFAILED, s_status[0])
                    self.net_driver.close()
                    self.network_write_active = False # reset to indicate no network activity
                Trace.alarm(e_errors.ERROR, "dismount %s failed: %s" % (self.current_volume, status))
                self.last_error = s_status
            broken = "dismount %s failed: %s" % (self.current_volume, status)

            if self.current_volume:
                try:
                    self.set_volume_noaccess(self.current_volume, "Dismount error. See log for details")
                except:
                    exc, msg, tb = sys.exc_info()
                    broken = broken + " set_system_noaccess failed: %s %s" %(exc, msg)
            if broken:
                # error out and do not allow dismount as nothing has
                # been mounted yet
                # Modify this to not call transfer_failed because this method can be called from transfer_called
                self.transfer_failed(exc=e_errors.DISMOUNTFAILED, msg=broken,error_source=ROBOT, dismount_allowed=0)
                self.broken(broken, e_errors.DISMOUNTFAILED) # this is to address AML2 dismount failures
            time.sleep(3)
            self.offline()
        return

    def mount_volume(self, volume_label, after_function=None):
        """
        Mount tape.

        :type volume_label: :obj:`str`
        :arg volume_label: label of the volume to be mounted.
        :type after_function: :obj:`callable`
        :arg after_function: function to run after mount is done.
        """

        self.dismount_time = None
        self.just_mounted = 0
        if self.current_volume:
            old_volume = self.current_volume
            if volume_label != self.current_volume:
                self.will_mount = volume_label
            self.dismount_volume()
            # see if dismount completed successfully
            if self.state in (ERROR, OFFLINE):
                # return work to lm
                self.current_volume = volume_label
                self.send_error_msg(error_info = (e_errors.MOUNTFAILED,volume_label),
                                    error_source=ROBOT,
                                    returned_work=None)

                #self.return_work_to_lm(self.current_work_ticket)
                return

            # tell lm that previously mounted volume is dismounted
            vinfo = self.vcc.inquire_vol(old_volume)
            volume_status = (vinfo.get('system_inhibit',['Unknown', 'Unknown']),
                             vinfo.get('user_inhibit',['Unknown', 'Unknown']))

            vol_family = vinfo.get('volume_family', 'Unknown')
            ticket =  {
                "mover":  self.name,
                "address": self.address,
                "external_label":  old_volume,
                "current_location": loc_to_cookie(self.current_location),
                "read_only" : 0, ###XXX todo: multiple drives on one scsi bus, write locking
                "returned_work": None,
                "state": state_name(IDLE),
                "status": (e_errors.OK, None),
                "volume_family": vol_family,
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
        self.read_count_per_mount = self.write_count_per_mount = 0

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

        self.asc.log_start_mount(self.current_volume,
                                 self.config['product_id'],
                                 volume_family.extract_storage_group(self.vol_info['volume_family']))

        self.current_location = 0L
        vi = self.vol_info
        Trace.trace(12, "override_ro_mount %s"%(self.override_ro_mount,))
        if self.override_ro_mount:
            vi['system_inhibit'][1] = 'none'
            vi['user_inhibit'][1] = 'none'

        mc_queue_full = 0
        while 1:
            mcc_reply = self.mcc.loadvol(vi, self.name, self.mc_device)
            status = mcc_reply.get('status')
            if status and status[0] == e_errors.MC_QUEUE_FULL:
                # media changer responded but could not perform the operation
                Trace.log(e_errors.INFO, "Media Changer returned %s"%(status[0],))
                # to avoid false "too long in state.."
                # reset self.time_in_state
                self.time_in_state = time.time()
                time.sleep(10)
                mc_queue_full = 1
                continue
            else:
                break

        Trace.trace(10, 'mc replies %s' % (status,))

        #Do another query volume, just to make sure its status has not changed
        self.vol_info.update(self.vcc.inquire_vol(volume_label))

        if status[0] != e_errors.OK and mc_queue_full:
            # if mover has retried due to MC_QUEUE_FULL there can be a situation
            # when the tape was actually mounted
            # the next mount attempt will result then in the error message like this:
            #
            # ('ERROR',
            #  13,
            #  ['ACSSA> mount PRT982 1,1,10,12 readonly',
            #  '.',
            #  'Mount: PRT982 mounted on   1, 1,10,12',
            #  'ACSSA> logoff',
            #  ''],
            # '',
            # 'MOUNT 13: mount PRT982 1,1,10,12 readonly => 0,.'
            # )
            # We need to verify that the tape is actually in the drive
            if status[0] == e_errors.ERROR and status[1] == 13:
                tape_status=status[2][2].split(' ')
                # tape status is derived from this part:
                # 'Mount: PRT982 mounted on   1, 1,10,12'
                #
                t = tape_status[1]
                m = tape_status[2]
                d=''
                d=d.join((tape_status[-2], tape_status[-1]))
                if t == volume_label and m == 'mounted' and d == self.mc_device:
                    # the requested tape was actually mouted in this drive
                    status[0] = e_errors.OK

        if status[0] == e_errors.OK:
            self.vcc.update_counts(self.current_volume, mounts=1)
            self.asc.log_finish_mount(self.current_volume)
            Trace.notify("loaded %s %s" % (self.shortname, volume_label))
            self.init_stat(self.logname)
            tm = time.localtime(time.time()) # get the local time
            time_msg = "%.2d:%.2d:%.2d" %  (tm[3], tm[4], tm[5])
            Trace.log(e_errors.INFO, "mounted %s %s %s"%(volume_label,self.config['product_id'], time_msg),
                  msg_type=Trace.MSG_MC_LOAD_DONE)

            if self.mount_delay:
                Trace.trace(25, "waiting %s seconds after mount"%(self.mount_delay,))
                time.sleep(self.mount_delay)
            self.just_mounted = 1
            self.write_counter = 0 # this flag is used in write_tape to verify tape position
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
                self.network_write_active = False # reset to indicate no network activity
                if status[1] == e_errors.MC_VOLNOTFOUND:
                    try:
                        self.set_volume_noaccess(volume_label, "Volume not found. See log for details")
                    except:
                        exc, msg = sys.exc_info()[:2]
                        Trace.alarm(e_errors.WARNING, "set_system_noaccess failed: %s %s" %(exc, msg))

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
                self.set_volume_noaccess(volume_label, "Mount error. See log for details")
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
        """
        Seek tape to the specified location.

        :type location: :obj:`int`
        :arg location: location on a tape.
        :type eot_ok: :obj:`int`
        :arg eot_ok: if 1 - ok to seek to end of tape.
        :type after_function: :obj:`callable`
        :arg after_function: function to run after seek is done.
        """

        Trace.trace(10, "seeking to %s, after_function=%s"%(location,after_function))
        failed=0
        try:
            self.tape_driver.seek(location, eot_ok) #XXX is eot_ok needed?
        except:
            exc, detail, tb = sys.exc_info()

            ########## Zalokar: April 1, 2004 ##########################
            # If the error is FTT_EBLANK, do a similar action to that in
            # Mover.read_tape() to return (READ_ERROR, READ_EOD) instead
            # of positioning error if we believe that we reached the end
            # of the tape.
            if str(detail) == "FTT_EBLANK":
                prev_loc = self.current_location
                try:
                    self.current_location, tell = self.tape_driver.tell()
                except self.ftt.FTTError, detail:
                    self.transfer_failed(e_errors.POSITIONING_ERROR, 'Positioning error, can not get drive info %s' % (detail,),
                                     error_source=DRIVE)
                    return

                Trace.log(e_errors.INFO,
                          "Reached EOT seeking location %s.  Current "
                          "Location %s Previous location %s" %
                          (location, self.current_location, prev_loc))
                self.transfer_failed(e_errors.READ_ERROR, e_errors.READ_EOD,
                                     error_source=TAPE)
            else:
                self.transfer_failed(e_errors.POSITIONING_ERROR,
                                     'positioning error %s' % (detail,),
                                     error_source=DRIVE)
                return
            ########## Zalokar: April 1, 2004 ##########################
            failed=1
        self.timer('seek_time')
        try:
            self.current_location, block = self.tape_driver.tell()
        except self.ftt.FTTError, detail:
            self.transfer_failed(e_errors.POSITIONING_ERROR, 'Positioning error, can not get drive info %s' % (detail,),
                                 error_source=DRIVE)
            return
        except:
            exc, detail, tb = sys.exc_info()
            self.transfer_failed(e_errors.POSITIONING_ERROR, 'Positioning error, can not get drive info %s %s' % (exc, detail,),
                                 error_source=DRIVE)
            return
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
                self.run_in_thread('media_thread', self.dismount_volume, after_function=self.idle)

                #self.dismount_volume(after_function=self.idle)
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
        elif self.state == OFFLINE:
            # mover went offline while seeking
            # the possible reason is a bad MIR causing a mover to seek for too long
            return

        if after_function and not failed:
            Trace.trace(10, "seek calling after function %s" % (after_function,))
            after_function()

    def start_transfer(self):
        """
        Start data transfer.

        """
        Trace.trace(10, "start transfer")
        #If we've gotten this far, we've mounted, positioned, and connected to the client.
        # If seek takes too long and main thread sets mover to ERROR state
        # fail the transfer
        if self.state == ERROR:
            Trace.log(e_errors.ERROR, "State ERROR, can not proceed")

        self.state = ACTIVE

        if self.mode == WRITE:
            self.run_in_thread('net_thread', self.read_client)
            self.run_in_thread('tape_thread', self.write_tape)
        elif self.mode == READ or self.mode == ASSERT:
            if self.mode == ASSERT:
                self.net_driver.open('/dev/null', WRITE)
            self.run_in_thread('tape_thread', self.read_tape)
            self.run_in_thread('net_thread', self.write_client)
        else:
            self.transfer_failed(e_errors.ERROR, "invalid mode %s" % (self.mode,))

    def status(self, ticket):
        """
        Send mover status to the client.

        This method is invoked via :class:`dispatching_worker.DispatchingWorker`.
        The corresponding enstore command is::

           enstore mover --status <mover_name>

        :type ticket: :obj:`dict`
        :arg ticket: ticket received from mover client
        """

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
                buf = self.buffer
            else:
                bytes_buffered = 0
                buffer_min_bytes = 0
                buffer_max_bytes = 0
                buf = None
        except AttributeError:
            # try it again
            time.sleep(3)
            if self.buffer:
                bytes_buffered = self.buffer.nbytes()
                buffer_min_bytes = self.buffer.min_bytes
                buffer_max_bytes = self.buffer.max_bytes
                buf = self.buffer
            else:
                bytes_buffered = 0
                buffer_min_bytes = 0
                buffer_max_bytes = 0
                buf = None
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
                 'buffer_max': buffer_max_bytes,
                 'rate of network': self.net_driver.rates()[0],
                 'rate of tape': self.tape_driver.rates()[0],
                 'default_dismount_delay': self.default_dismount_delay,
                 'max_dismount_delay': self.max_dismount_delay,
                 'client': self.client_ip,
                 'buffer':'%s'%(buf,),
                 }
        if self.state is HAVE_BOUND and self.dismount_time and self.dismount_time>now:
            tick['will dismount'] = 'in %.1f seconds' % (self.dismount_time - now)
        ticket.update(tick)
        self.reply_to_caller(ticket)
        #self.log_processes(logit=1)
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
        except (OSError, IOError), detail:
            Trace.log(e_errors.ERROR, "Cannot unlink %s %s"%(filename,detail))

    def check_lockfile(self):
        return os.path.exists(self.lockfile_name())

    def start_draining(self, ticket):
        """
        Finish current work and stop accepting any new work.

        """
        x = ticket # to trick pychecker
        save_state = self.state
        self.draining = 1
        if self.state in (IDLE, ERROR):
            self.state = OFFLINE

        ##elif self.state is HAVE_BOUND:
        ##    self.state = DRAINING # XXX CGW should dismount here. fix this
        Trace.trace(e_errors.INFO, "The mover is set to state %s"%(state_name(self.state),))
        self.create_lockfile()
        out_ticket = {'status':(e_errors.OK,None),'state':state_name(self.state), 'pid': os.getpid()}
        ticket.update(out_ticket)
        self.reply_to_caller(ticket)
        if save_state is HAVE_BOUND:
            self.run_in_thread('media_thread', self.dismount_volume, after_function=self.offline)

            #self.dismount_volume()
            #self.state = OFFLINE
        return

    def stop_draining(self, ticket, do_restart=1):
        """
        Start accepting new work.
        """
        x = ticket # to trick pychecker
        if self.state != OFFLINE:
            ticket['status'] =("EPROTO","Not OFFLINE")
            self.reply_to_caller(ticket)
            return
        ticket['status'] = (e_errors.OK,None)
        self.reply_to_caller(ticket)
        ## XXX here we need to check if tape is mounted
        ## if yes go to have bound, NOT idle AM
        Trace.trace(11,"check lockfile %s"%(self.check_lockfile(),))
        self.remove_lockfile()
        Trace.trace(11,"check lockfile %s"%(self.check_lockfile(),))
        if do_restart:
            Trace.log(e_errors.INFO,"restarting %s"% (self.name,))
        self.restart(do_restart)
        #self.idle()

    def warm_restart(self, ticket):
        """
        Restart gracefully: finish current work and restart.

        """
        self.start_draining(ticket)
        out_ticket = {'status':(e_errors.OK,None),'state':self.state}
        ticket.update(out_ticket)
        self.reply_to_caller(ticket)
        while 1:
            if self.state == OFFLINE:
                self.stop_draining(ticket)
            elif self.state != ERROR:
                time.sleep(2)
                Trace.trace(11,"waiting in state %s for OFFLINE" % (self.state,))
            else:
                Trace.alarm(e_errors.ERROR, "can not restart. State: %s" % (self.state,))

    def quit(self, ticket):
        """
        Stop mover gracefully.

        This method is invoked via :class:`dispatching_worker.DispatchingWorker`.

        :type ticket: :obj:`dict`
        :arg ticket: ticket received from mover client
        """

        Trace.log(e_errors.INFO, "Mover has received a quit command")
        self.start_draining(ticket)
        ticket['status'] = (e_errors.OK,None)
        ticket['state'] = self.state

        self.reply_to_caller(ticket)
        while 1:
            if self.state == OFFLINE:
                self.stop_draining(ticket, do_restart=0)
            elif self.state != ERROR:
                time.sleep(2)
                Trace.trace(11,"waiting in state %s for OFFLINE" % (self.state,))
            else:
                Trace.alarm(e_errors.ERROR, "can not restart. State: %s" % (self.state,))

    def clean_drive(self, ticket):
        """
        Clean drive.
        This method is invoked via :class:`dispatching_worker.DispatchingWorker`.
        The corresponding enstore command is::

           enstore mover --clean-drive <mover_name>

        :type ticket: :obj:`dict`
        :arg ticket: ticket received from mover client
        """

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
        ticket.update(ret)
        self.reply_to_caller(ticket)


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
            self.print_help()
            os._exit(1)
        else:
            self.name = self.args[0]

class DiskMover(Mover):
    """
    Implements disk mover.

    """

    def device_dump_S(self, ticket):
        """
        Overriden from :meth:`Mover.device_dump_S`.

        """
        x =ticket # to trick pychecker
        ticket['status'] = (e_errors.ERROR, "not implemented")
        self.reply_to_caller(ticket)
	return

    def __idle(self):
        if self.state == ERROR:
            return
        self.state = IDLE
        self.mode = None
        self.vol_info = {}
        self.file_info = {}
        self.current_library = None

    def idle(self):
        """
        Overriden from :meth:`Mover.idle`.

        """
        self.__idle()
        thread = threading.currentThread()
        if thread:
            thread_name = thread.getName()
        else:
            thread_name = None
        # if running in the main thread update lm
        if thread_name == 'MainThread':
            self.update_lm()
        else: # else just set the update flag
            self.need_lm_update = (1, None, 0, None)

    def nowork(self):
        """
        Overriden from :meth:`Mover.nowork`.

        """
        Trace.trace(98, "NOWORK")
        Mover.nowork(self,{})
        self.__idle()
        pass

    def no_work(self, ticket):
        #x = ticket # to trick pychecker
        Trace.trace(98, "no_work %s"%(ticket,))
        # no_work is No work: do nothing.
        self.nowork()

    def write_tape(self):
        """
        Overriden from :meth:`Mover.write_tape`.

        """
        Trace.trace(8, "write_tape starting, bytes_to_write=%s" % (self.bytes_to_write,))
        Trace.trace(8, "bytes_to_transfer=%s" % (self.bytes_to_transfer,))
        driver = self.tape_driver
        count = 0
        defer_write = 1
        failed = 0
        self.media_transfer_time = 0.
        # send a trigger message to the client
        try:
            bytes_written = self.net_driver.write("B", # write anything
                                                  0,
                                                  1) # just 1 byte

        except generic_driver.DriverError, detail:
            self.transfer_failed(e_errors.ENCP_GONE, detail, error_source=NETWORK)
            return
        except:
            exc, detail = sys.exc_info()[:2]
            self.transfer_failed(e_errors.ENCP_GONE, detail, error_source=NETWORK)
            return
        #Initialize thresholded transfer notify messages.
        bytes_notified = 0L
        last_notify_time = time.time()
        Trace.notify("transfer %s %s %s media %s %.3f %s" %
                     (self.shortname, self.bytes_written,
                      self.bytes_to_write, self.buffer.nbytes(), time.time(), self.draining))

        while self.state in (ACTIVE,) and self.bytes_written<self.bytes_to_write:
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
                        # if network rate < taperate
                        # increase the water mark
                        # otherwise set it to self.min_buffer
                        ratio = netrate/(taperate*1.0)
                        optimal_buf = self.bytes_to_transfer * (1-ratio)
                        optimal_buf = min(optimal_buf, 0.5 * self.max_buffer)
                        optimal_buf = max(optimal_buf, self.min_buffer)
                        optimal_buf = int(optimal_buf)
                        Trace.trace(112,"netrate = %.3g, taperate=%.3g" % (netrate, taperate))
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
                                   self.bytes_to_write, last_notify_time):
                bytes_notified = self.bytes_written
                last_notify_time = time.time()
                Trace.notify("transfer %s %s %s media %s %.3f %s" %
                             (self.shortname, self.bytes_written,
                              self.bytes_to_write, self.buffer.nbytes(),
                              last_notify_time, self.draining))

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
                have_tape = self.tape_driver.open(self.tmp_file, READ)
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
                #saved_sanity_bytes = self.buffer.sanity_bytes
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

                    except MoverError, detail:
                        detail = str(detail)
                        if detail == e_errors.CRC_ERROR:
                            Trace.alarm(e_errors.ERROR, "selective CRC check error",
                                        {'outfile':self.current_work_ticket['outfile'],
                                         'infile':self.current_work_ticket['infile'],
                                         'location_cookie':self.current_work_ticket['fc']['location_cookie'],
                                         'external_label':self.current_work_ticket['vc']['external_label']})
                            self.transfer_failed(e_errors.WRITE_ERROR, detail, error_source=TAPE)
                        else:
                            if detail == e_errors.ENCP_GONE:
                                err_source = NETWORK
                            else:
                                err_source = UNKNOWN
                            self.transfer_failed(detail, error_source=err_source)

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
        """
        Overriden from :meth:`Mover.read_tape`.
        """
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
        last_notify_time = time.time()
        Trace.notify("transfer %s %s %s media %s %.3f %s" %
                     (self.shortname, -self.bytes_read,
                      self.bytes_to_read, self.buffer.nbytes(), time.time(), self.draining))

        while self.state in (ACTIVE,) and self.bytes_read < self.bytes_to_read:
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
            except MoverError, detail:
                detail = str(detail)
                if detail == e_errors.CRC_ERROR:
                    Trace.alarm(e_errors.ERROR, "CRC error reading tape",
                                {'outfile':self.current_work_ticket['outfile'],
                                 'infile':self.current_work_ticket['infile'],
                                 'location_cookie':self.current_work_ticket['fc']['location_cookie'],
                                 'external_label':self.current_work_ticket['vc']['external_label']})
                    self.transfer_failed(e_errors.CRC_ERROR, error_source=TAPE)
                else:
                    if detail == e_errors.ENCP_GONE:
                        err_source = NETWORK
                    else:
                        err_source = UNKNOWN
                    self.transfer_failed(detail, error_source=err_source)

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
                    failed = 1
                    self.transfer_failed(e_errors.READ_ERROR, "Invalid file header ", error_source=TAPE)
                    break
                b0 = self.buffer._buf[0]
                if len(b0) >= self.wrapper.min_header_size:
                    try:
                        header_size = self.wrapper.header_size(b0)
                    except (TypeError, ValueError), msg:
                        Trace.log(e_errors.ERROR,"Invalid header %s" %(b0[:self.wrapper.min_header_size]))
                        self.transfer_failed(e_errors.READ_ERROR, "Invalid file header %s"%(msg,), error_source=TAPE)
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
                                   self.bytes_to_read, last_notify_time):
                bytes_notified = self.bytes_read
                last_notify_time = time.time()
                Trace.notify("transfer %s %s %s media %s %.3f %s" %
                             (self.shortname, -self.bytes_read,
                              self.bytes_to_read, self.buffer.nbytes(),
                              last_notify_time, self.draining))

            if not self.buffer.empty():
                self.buffer.write_ok.set()
        if self.tr_failed:
            Trace.trace(27,"read_tape: tr_failed %s"%(self.tr_failed,))
            return
        if failed: return
        if do_crc:
            if self.tr_failed: return # do not calculate CRC if net thead detected a failed transfer
            complete_crc = self.file_info.get('complete_crc',None)
            Trace.trace(22,"read_tape: calculated CRC %s File DB CRC %s"%
                        (self.buffer.complete_crc, self.file_info['complete_crc']))
            if self.buffer.complete_crc != self.file_info['complete_crc']:
                if self.tr_failed: return # do not calculate CRC if net thead detected a failed transfer
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
                    crc_error = 1
                    # try 1 based crc
                    crc_1_seeded = checksum.convert_0_adler32_to_1_adler32(self.buffer.complete_crc,
                                                                               self.file_info['size'])
                    if crc_1_seeded == complete_crc:
                        self.buffer.complete_crc = crc_1_seeded
                        crc_error = 0
                    if crc_error:
                        Trace.alarm(e_errors.ERROR, "read_tape CRC error",
                                    {'outfile':self.current_work_ticket['outfile'],
                                     'infile':self.current_work_ticket['infile'],
                                     'location_cookie':self.current_work_ticket['fc']['location_cookie'],
                                     'external_label':self.current_work_ticket['vc']['external_label']})
                        self.transfer_failed(e_errors.CRC_ERROR, error_source=TAPE)
                        return
            #Trace.log(e_errors.INFO, "fake read CRC error")
            #self.transfer_failed(e_errors.CRC_ERROR, error_source=TAPE)
            return

        Trace.trace(8, "read_tape exiting, read %s/%s bytes" %
                    (self.bytes_read, self.bytes_to_read))

    def create_volume_name(self, ip_map, volume_family):
        """
        Combine disk volume name.

        :type ip_map: :obj:`str`
        :arg ip_map: IP mapping from mover configuration.
        :type volume_family: :obj:`str`
        :arg volume_family: volume family from work ticket
        """

        #return string.join((ip_map, volume_family,time.strftime("%Y-%m-%dT%H:%M:%SZ")),':')
        return string.join((ip_map, volume_family,),':')

    def setup_transfer(self, ticket, mode):
        """
        Overriden from :meth:`Mover.setup_transfer`.
        """
        self.lock_state()
        self.save_state = self.state

        self.unique_id = ticket['unique_id']
        self.uid = -1
        self.gid = -1
        if 'wrapper' in ticket:
            self.uid = ticket['wrapper'].get('uid', -1)
            self.gid = ticket['wrapper'].get('gid', -1)

        try:
            self.lm_address = ticket['lm']['address']
        except KeyError:
            self.lm_address = ('none',0)

        ##NB: encp v2_5 supplies this information for writes but not reads. Somebody fix this!
        try:
            client_hostname = ticket['wrapper']['machine'][1]
        except KeyError:
            client_hostname = ''
        self.client_hostname = client_hostname
        Trace.trace(10, "setup transfer1 %s"%(ticket,))
        self.tr_failed = 0
        self.current_library = ticket['vc'].get('library', None)
        if not self.current_library:
            self.transfer_failed(e_errors.EPROTO)
            return

        self.setup_mode = mode
        ## pprint.pprint(ticket)
        if self.save_state not in (IDLE, HAVE_BOUND):
            Trace.log(e_errors.ERROR, "Not idle %s" %(state_name(self.state),))
            self.return_work_to_lm(ticket)
            self.unlock_state()
            return

        self.state = SETUP
        # the following settings are needed by LM to update it's queues
        self.tmp_vol = ticket['vc'].get('external_label', None)
        self.tmp_vf = ticket['vc'].get('volume_family', None)
        self.need_lm_update = (1, self.state, 1, None)
        self.unlock_state()

        ticket['mover']={}
        ticket['mover'].update(self.config)
        ticket['mover']['device'] = "%s:%s" % (self.config['ip_map'], self.config['device'])
        self.current_package =  ticket['fc'].get('external_label', None)
        if ticket['fc']['external_label'] == None:
            del(ticket['fc']['external_label'])
        else:
            ticket['fc']['external_label'] = ticket['vc']['external_label']
        self.current_work_ticket = ticket
        Trace.log(DEBUG_LOG, "CURR TICK: %s"%(self.current_work_ticket,))
        Trace.trace(DEBUG_LOG, "CURR PACK %s"%(self.current_package,))
        self.run_in_thread('client_connect_thread', self.connect_client)

    def finish_transfer_setup(self):
        """
        Overriden from :meth:`Mover.finish_transfer_setup`.

        """
        Trace.trace(10, "client connect returned: %s %s" % (self.control_socket, self.client_socket))
        ticket = self.current_work_ticket
        if not self.client_socket:
            Trace.trace(20, "finish_transfer_setup: connection to client failed")
            self.state = self.save_state
            ## Connecting to client failed
            self.need_lm_update = (1, self.state, 1, None)
            #self.update_lm(reset_timer=1)
            return

        self.t0 = time.time()
        self.crc_seed = self.initial_crc_seed

        if 'crc_seed' in ticket:
            crc_seed = int(ticket['crc_seed'])
            if crc_seed == 1 or crc_seed == 0:
                self.crc_seed = crc_seed
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
        #    read requests -- calculate CRC when reading from tape to memory
        # if client_crc is OFF:
        #    write requests -- calculate CRC when writing to memory
        #    read requetsts -- calculate CRC when reading memory

        self.reset(sanity_cookie, client_crc_on)
        # restore self.current_work_ticket after it gets cleaned in the reset()
        self.current_work_ticket = ticket
        self.current_volume =  self.vol_info.get('external_label', None)
        self.delay = 31536000  # 1 year
        self.fcc = file_clerk_client.FileClient(self.csc, bfid=0,
                                                server_address=fc['address'])
        self.vcc = volume_clerk_client.VolumeClerkClient(self.csc,
                                                         server_address=vc['address'])
        ic_conf = self.csc.get("info_server")
        #self.infoc = info_client.infoClient(self.csc,
        #                                    server_address=(ic_conf['host'],
        #                                                    ic_conf['port']))
        self.unique_id = self.current_work_ticket['unique_id']
        volume_label = self.current_volume
        self.current_location = 0L
        if not volume_label:
            if  self.setup_mode == WRITE:
                self.current_volume = self.create_volume_name(self.ip_map,
                                                              self.volume_family)
                self.vol_info['external_label'] = self.current_volume
                self.vol_info['status'] = (e_errors.OK, None)

        if self.vol_info['status'][0] != e_errors.OK:
            msg =  ({READ: e_errors.READ_NOTAPE, WRITE: e_errors.WRITE_NOTAPE}.get(
                self.setup_mode, e_errors.EPROTO), self.vol_info['status'][1])
            Trace.log(e_errors.ERROR, "Volume clerk reply %s" % (msg,))
            self.send_client_done(self.current_work_ticket, msg[0], msg[1])
            self.state = self.save_state
            return

        self.buffer.set_blocksize(self.vol_info.get('blocksize',self.default_block_size))
        self.wrapper = None
        #self.wrapper_type = volume_family.extract_wrapper(self.volume_family)
        self.wrapper_type = 'null'

        try:
            self.wrapper = __import__(self.wrapper_type + '_wrapper')
        except:
            exc, msg, tb = sys.exc_info()
            Trace.log(e_errors.ERROR, "error importing wrapper: %s %s" %(exc,msg))

        if not self.wrapper:
            msg = e_errors.INVALID_WRAPPER, "Illegal wrapper type %s" % (self.wrapper_type)
            Trace.log(e_errors.ERROR,  "%s" %(msg,))
            self.send_client_done(self.current_work_ticket, msg[0], msg[1])
            self.state = self.save_state
            return

        self.buffer.set_wrapper(self.wrapper)
        client_filename = self.current_work_ticket['wrapper'].get('fullname','?')
        pnfs_filename = self.current_work_ticket['wrapper'].get('pnfsFilename', '?')

        self.mode = self.setup_mode
        if self.mode == READ:
            # for reads alwas set crc_seed to 0
            # crc will be automatically checked against seed 1
            # in case if seed 0 crc check fails
            self.buffer.set_crc_seed(0L)

        self.bytes_to_transfer = long(fc['size'])
        self.bytes_to_write = self.bytes_to_transfer
        self.bytes_to_read = self.bytes_to_transfer
        self.expected_transfer_time = self.bytes_to_write*1.0 / self.max_rate
        self.real_transfer_time  = 0.

        if self.client_hostname:
            client_filename = self.client_hostname + ":" + client_filename

        if self.mode == READ:
            self.file = fc['cache_location']
            self.files = (pnfs_filename, client_filename)
            self.buffer.header_size = None
            work_file = self.file
        elif self.mode == WRITE:
            if fc.has_key('pnfsid'):
                self.file = enstore_functions3.file_id2path(self.device, fc['pnfsid'])
                work_file = self.tmp_file
                # create directory for file
                dir_name = os.path.dirname(self.file)
                if not os.path.exists(dir_name):
                    try:
                        os.makedirs(dir_name, 0755)
                    except Exception, detail:
                        if detail[0] != errno.EEXIST:
                            self.transfer_failed(e_errors.OSERROR, str(detail), error_source=DRIVE)
                self.files = (client_filename, pnfs_filename)
            else:
                self.transfer_failed(e_errors.MALFORMED, 'expected Key "pnfsid". Current dictionary %s'%(fc,), error_source=USER)
                return
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
        self.position_media(work_file)

    # check connection in READ mode
    def check_connection(self):
        """
        Overriden from :meth:`Mover.check_connection`.

        """
        Trace.trace(40, "check_connection started")
        if self.mode == READ:
            Trace.trace(40, "check_connection mode %s"%(mode_name(self.mode),))
            try:
                if self.control_socket:
                    r, w, ex = select.select([self.control_socket], [self.control_socket], [], 10)
                    Trace.trace(40, "check_connection1 %s %s %s"%(r, w, ex))
                    Trace.trace(40, "r= %s"%(r,))
                    if r:
                        # r - read socket appears when client connection gets closed
                        return False
            except:
                pass
        return True


    # stage file into cache
    # returns file cache location or None
    def stage_file(self):
        """
        Stage file into cache.

        :rtype: :obj:`str` file location in cache or :obj:`None`
        """

        info = self.fcc.bfid_info(self.file_info['bfid'])
        if not e_errors.is_ok(info):
            Trace.log(e_errors.ERROR, "stage_file: file clerk returned %s"%(info,))
            return None
        cache_status = info['cache_status']
        if cache_status != file_cache_status.CacheStatus.CACHED:
            Trace.log(e_errors.INFO, "staging status at the start %s"%(cache_status,))
            # make this better!
            # When file gets staged its cache_status must change as
            # PURGED -> STAGING_REQUESTED -> STAGING -> CACHED
            if cache_status in (file_cache_status.CacheStatus.PURGING_REQUESTED,):
                # There were lots of cases when files do not get purged leaving them
                # in this intermediate state and preventing from being transferred immediately.

                # Check if file is still in cache:
                if os.path.exists(info['cache_location']):
                    Trace.log(DEBUG_LOG, "changing cache_status for %s to %s"%(self.file_info['bfid'],
                                                                               file_cache_status.CacheStatus.CACHED))
                    rc = set_cache_status.set_cache_status(self.fcc,
                                                           [{'bfid': self.file_info['bfid'],
                                                             'cache_status': file_cache_status.CacheStatus.CACHED,
                                                             'archive_status': None,
                                                             'cache_location': None}])
                    rv = info['cache_location']
                else:
                    Trace.log(DEBUG_LOG, "changing cache_status for %s to %s"%(self.file_info['bfid'],
                                                                               file_cache_status.CacheStatus.PURGED))
                    rc = set_cache_status.set_cache_status(self.fcc,
                                                           [{'bfid': self.file_info['bfid'],
                                                             'cache_status': file_cache_status.CacheStatus.PURGED,
                                                             'archive_status': None,
                                                             'cache_location': None}])
                    rv = None
                Trace.trace(10, "set_cache_status: set_cache_status 1 returned %s"%(rc,))
                if not e_errors.is_ok(rc['status']):
                    Trace.log(e_errors.ERROR, "Error setting cache status for %s: %s"%(self.file_info['bfid'],rc))
                    return None
                return rv

            file_cache_location = None
            loop_counter = 1L
            open_bitfile_sent = False
            while not hasattr(self,'too_long_in_state_sent'):
                info = self.fcc.bfid_info(self.file_info['bfid'])
                if not e_errors.is_ok(info):
                    Trace.log(e_errors.ERROR, "stage_file1: file clerk returned %s"%(info,))
                    return None
                if info['cache_status'] in (file_cache_status.CacheStatus.PURGING_REQUESTED,
                                            file_cache_status.CacheStatus.PURGING):
                    # looks as there was no request to open bitfile
                    if not open_bitfile_sent:
                        open_bitfile_sent = True
                        package_id = info.get('package_id')
                        if package_id:
                            rticket = self.fcc.open_bitfile_for_package(package_id)
                        else:
                            rticket = self.fcc.open_bitfile(info['bfid'])
                        Trace.log(DEBUG_LOG, "open_bifile returned %s"%(rticket,))
                        if rticket['status'][0] != e_errors.OK:
                            Trace.log(e_errors.ERROR, "File staging has failed for %s %s"%
                                      (info['bfid'], info['pnfs_name0']))
                            break

                Trace.log(DEBUG_LOG, "staging status %s cache_status %s"%(info['cache_status'], cache_status))

                if info['cache_status'] == file_cache_status.CacheStatus.CACHED:
                    file_cache_location = info.get('cache_location', None)
                    break
                # check if the file can be staged
                rticket = self.vcc.is_vol_available(self.current_work_ticket['work'],
                                                    self.file_info['tape_label'],
                                                    None,
                                                    self.file_info['size'],
                                                    timeout=5,
                                                    retry=2)
                if not e_errors.is_ok(rticket):
                   self.transfer_failed(rticket['status'][0], rticket['status'][1], error_source=TAPE)
                   self.idle()
                   break

                if (info['cache_status'] == file_cache_status.CacheStatus.STAGING_REQUESTED and
                    cache_status != file_cache_status.CacheStatus.STAGING_REQUESTED):
                    cache_status = info['cache_status'] # File Clerk requested staging
                elif (info['cache_status'] == file_cache_status.CacheStatus.STAGING and
                    cache_status ==  file_cache_status.CacheStatus.STAGING_REQUESTED):
                    cache_status = info['cache_status'] # Migrator started staging
                elif (info['cache_status'] == file_cache_status.CacheStatus.PURGED and
                      (cache_status in (file_cache_status.CacheStatus.STAGING,
                                        file_cache_status.CacheStatus.STAGING_REQUESTED,
                                        file_cache_status.CacheStatus.PURGING_REQUESTED))):
                    Trace.log(e_errors.ERROR, "File staging has failed for %s %s "%(info['bfid'], info['pnfs_name0']))
                    break
                time.sleep(2)
                # Staging a file may take a very long time.
                # Reset self.state_change_time
                # to avoid "mover stuck in state condition"
                self.state_change_time = time.time()
            if loop_counter % 100 == 0:
                # check if encp is still connected
                if not self.check_connection():
                    self.transfer_failed(e_errors.ENCP_GONE, "encp gone while waiting for stage", error_source=USER)
                    self.idle()
                    return None
            loop_counter = loop_counter + 1
        else:
            # check if file exists in cache
            file_cache_location = info.get('cache_location')
            Trace.log(e_errors.INFO, "check if file is in cache %s"%(file_cache_location,))
            if not os.path.exists(file_cache_location):
                Trace.log(e_errors.ERROR, "file cache status was reported as %s, but it is not in cache. Setting to %s" %
                          (file_cache_status.CacheStatus.CACHED, file_cache_status.CacheStatus.PURGED))
                file_cache_location = None
                rc = set_cache_status.set_cache_status(self.fcc,
                                                       [{'bfid': self.file_info['bfid'],
                                                         'cache_status':file_cache_status.CacheStatus.PURGED,
                                                         'archive_status': None,
                                                         'cache_location': None}
                                                        ])

        return file_cache_location

    def position_media(self, filename):
        """
        Overriden from :meth:`Mover.position_media`.

        """
        Trace.log(e_errors.INFO, "position media for %s"%(filename,))
        x = filename # to trick pychecker
        have_tape = 0
        err = None
        Trace.trace(10, "position media")
        if self.current_work_ticket['work'] == "read_from_hsm":
            if self.current_work_ticket['fc']['deleted'] != "no":
                self.transfer_failed(e_errors.DELETED, 'file %s does not exist: deleted==%s' %
                                     (self.current_work_ticket['fc']['pnfs_name0'],
                                      self.current_work_ticket['fc']['deleted']),
                                     error_source=UNKNOWN)

                self.idle()
                return

            # Check if file exists.
            # If this is a cache file it might have been purged
            #if not os.path.exists(filename):
            #    filename = self.stage_file()
            # Assume that all files have cache_status
            # and call self.stage_file() always.
            # This is needed to re-stage file on disk if it was corrupted
            filename = self.stage_file()
        if filename:
            try:
                have_tape = self.tape_driver.open(filename, self.mode, retry_count=30)
            except Exception, err:
                self.transfer_failed(e_errors.MOUNTFAILED, 'mount failure: %s' % (err,), error_source=DRIVE)
                self.idle()
                return
        else:
            if self.state != IDLE:
                self.transfer_failed(e_errors.MOUNTFAILED, 'mount failure: filename is %s' %
                                     (filename,), error_source=DRIVE)
                self.state = IDLE # to reset state ERROR
                self.idle()
            return

        if have_tape == 1:
            err = None
        else:
            self.transfer_failed(e_errors.MOUNTFAILED, 'mount failure: %s' % (err,), error_source=DRIVE)
            self.idle()
            return

        self.start_transfer()
        return 1

    def transfer_failed(self, exc=None, msg=None, error_source=None, dismount_allowed=0):
        """
        Overriden from :meth:`Mover.transfer_failed`.

        """

        if self.tr_failed:
            return          ## this function has been already called in the other thread
        self.tr_failed = 1
        Trace.trace(25, "TR FAILED")
        self.timer('transfer_time')
        ticket = self.current_work_ticket
        if not ticket.has_key('times'):
            ticket['times']={}

        t = self.media_transfer_time
        if t == 0.:
            t = ticket['times'].get('transfer_time', 0.)
        ticket['times']['drive_transfer_time'] = t
        ticket['times']['drive_call_time'] = self.tape_driver.tape_transfer_time()

        self.log_state()
        self.tape_driver.close()
        try:
            os.unlink(self.tmp_file)
        except:
            pass
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

        if exc not in (e_errors.ENCP_GONE, e_errors.DELETED, e_errors.NOACCESS,e_errors.NOTALLOWED):
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
        if exc == e_errors.CRC_ERROR and self.mode == READ:
            # do not remove cached file (leave it for investigation), but set it to PURGED
            # try:
            #     os.remove(self.file)
            # except:
            #    pass
            self.fcc.set_cache_status([{'bfid': self.file_info['bfid'],
                                        'cache_status': file_cache_status.CacheStatus.PURGED,
                                        'archive_status': None,        # we are not changing this
                                        'cache_location': self.file_info['cache_location']}])


        self.current_work_ticket['status'] = (str(exc), str(msg))
        self.current_work_ticket['fc']['external_label'] = self.current_work_ticket['vc']['external_label']
        self.send_client_done(self.current_work_ticket, str(exc), str(msg))
        self.net_driver.close()
        self.network_write_active = False # reset to indicate no network activity
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

        Trace.trace(26,"current thread %s"%(cur_thread_name,))
        if self.draining:
            self.offline()
        else:
            if self.mode == READ:
                self.state = HAVE_BOUND
                self.need_lm_update = (1, None, 1, None)
            else:
                self.idle()
                self.current_volume = None
        Trace.log(e_errors.INFO, "transfer failed state %s"%(state_name(self.state),))

        #self.tr_failed = 0

    def transfer_completed(self):
        """
        Overriden from :meth:`Mover.transfer_completed`.

        """
        self.consecutive_failures = 0
        self.timer('transfer_time')
        ticket = self.current_work_ticket
        if not ticket.has_key('times'):
            ticket['times']={}

        t = self.media_transfer_time
        if t == 0.:
            t = ticket['times'].get('transfer_time', 0.)
        ticket['times']['drive_transfer_time'] = t
        ticket['times']['drive_call_time'] = self.tape_driver.tape_transfer_time()


        Trace.log(e_errors.INFO, "transfer complete volume=%s location=%s"%(
            self.current_volume, 0))
        Trace.notify("disconnect %s %s" % (self.shortname, self.client_ip))
        self.transfers_completed = self.transfers_completed + 1
        self.net_driver.close()
        self.network_write_active = False # reset to indicate no network activity
        try:
            self.tape_driver.close()
        except OSError, detail:
            Trace.log(DEBUG_LOG, "transfer_completed error closing tape driver %s"%(str(detail),))
        if self.mode == WRITE:
            # move temporary file to destination files
            try:
                os.rename(self.tmp_file, self.file)
            except Exception, detail:
                Trace.alarm(e_errors.ALARM, "error saving file %s to %s. Detail %s"%
                            (self.tmp_file, self.file, str(detail)))
                self.transfer_failed(e_errors.OSERROR, 'transfer failure: %s' % (str(detail),), error_source=DRIVE)
                self.offline()
                return

        Trace.trace(10, "transfer complete mode %s"%(self.mode,))
        self.state = HAVE_BOUND
        Trace.log(e_errors.INFO, "transfer complete state %s"%(state_name(self.state),))
        Trace.trace(10, "transfer complete state %s"%(state_name(self.state),))

        now = time.time()
        self.dismount_time = now + self.delay
        self.current_work_ticket['fc']['external_label'] = self.current_work_ticket['vc']['external_label']
        self.send_client_done(self.current_work_ticket, e_errors.OK)
        if hasattr(self,'too_long_in_state_sent'):
            del(self.too_long_in_state_sent)

        if self.draining:
            self.offline()
        self.need_lm_update = (1, None, 1, None)
        self.log_state()


    def update_after_writing(self):
        """
        Overriden from :meth:`Mover.update_after_writing`.

        """
        sanity_cookie = (self.buffer.sanity_bytes,self.buffer.sanity_crc)
        complete_crc = self.buffer.complete_crc
        fc_ticket = {  'location_cookie': self.file,
                       'size': self.bytes_to_transfer,
                       'sanity_cookie': sanity_cookie,
                       'external_label': self.current_volume,
                       'complete_crc': complete_crc,
                       'gid': self.gid,
                       'uid': self.uid,
                       'pnfs_name0': self.current_work_ticket['outfilepath'],
                       'pnfsid':  self.file_info['pnfsid'],
                       'drive': "%s:%s" % (self.current_work_ticket['mover']['device'], self.config['serial_num']),
                       'original_library': self.current_work_ticket.get('original_library'),
                       'file_family_width': self.vol_info.get('file_family_width'),
                       'mover_type': self.mover_type,
                       'unique_id': self.current_work_ticket['unique_id'],
                       }

        copies = self.file_info.get('copies')
        if copies:
           fc_ticket['copies'] = copies

        # if it is a copy, make sure that original_bfid is passed along
        if self.current_work_ticket.has_key('copy'):
            original_bfid = self.file_info.get('original_bfid')
            if not original_bfid:
                self.transfer_failed(e_errors.ERROR,
                          'file clerk error: missing original bfid for copy')
                return 0
            fc_ticket['original_bfid'] = original_bfid

        #Get the volume information. If necessary create a new one.
        Trace.trace(15,"inquire volume %s"%(self.current_volume,))
        v = self.vcc.inquire_vol(self.current_volume)
        if v['status'][0] == e_errors.NO_VOLUME:
            # volume does not exist, create it!
            Trace.log(e_errors.INFO, "new disk volume request")
            stats = os.statvfs(self.config['device'])
            r2 = long(stats.f_bavail)*stats.f_bsize
            r = self.vcc.add(self.vol_info['library'],
                             volume_family.extract_file_family(self.vol_info['volume_family']),
                             volume_family.extract_storage_group(self.vol_info['volume_family']),
                             'disk',
                             self.current_volume,
                             r2,
                             eod_cookie = '0000_000000000_0000001',
                             wrapper = self.vol_info['wrapper'], # actually disk file will have no wrapper
                             blocksize = self.buffer.blocksize)
            Trace.log(e_errors.INFO,"Add volume returned %s"%(r,))
            if r['status'][0] != e_errors.OK:
                Trace.log(e_errors.ERROR,
                          "cannot assign new bfid")
                self.transfer_failed(e_errors.ERROR, "Cannot add new volume")
                return 0
            self.vol_info.update(r)
        elif v['status'][0] != e_errors.OK:
            Trace.log(e_errors.ERROR,
                      "cannot assign new bfid")
            self.transfer_failed(e_errors.ERROR, "Cannot obtain volume info")
            return 0

        #Request the new bit file.
        Trace.log(e_errors.INFO, "new bitfile request %s" % (fc_ticket,))

        fcc_reply = self.set_new_bitfile(fc_ticket, self.vol_info)
        if not fcc_reply:
            return fcc_reply
        ## HACK: restore crc's before replying to caller
        fc_ticket = fcc_reply['fc']
        fc_ticket['sanity_cookie'] = sanity_cookie
        fc_ticket['complete_crc'] = complete_crc
        bfid = fc_ticket['bfid']
        self.current_work_ticket['fc'] = fc_ticket

        self.current_work_ticket['vc'].update(self.vol_info)

        return 1


    def format_lm_ticket(self, state=None, error_info=None, returned_work=None, error_source=None):
        """
        Overriden from :meth:`Mover.format_lm_ticket`.

        """
        status = e_errors.OK, None
        work = None
        if state is None:
            state = self.state
        Trace.trace(20,"format_lm_ticket: state %s"%(state_name(state),))
        volume_label = self.current_volume
        Trace.trace(20,"format_lm_ticket: CV %s lv %s vf %s lvf %s"%(self.current_volume, self.last_volume, self.volume_family, self.last_volume_family))
        if self.current_volume:
            volume_label = self.current_volume
            volume_family = self.volume_family
        else:
            volume_label = self.last_volume
            volume_family = self.last_volume_family

        external_label = None
        if hasattr(self, "current_package") and self.current_package:
            external_label = volume_label
            volume_label = self.current_package

        if state is IDLE:
            work = "mover_idle"
        elif state in (HAVE_BOUND,):
            work = "mover_bound_volume"
        elif state in (ACTIVE, SETUP, SEEK, CLEANING, MOUNT_WAIT, DISMOUNT_WAIT, FINISH_WRITE):
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

        if self.current_work_ticket:
            encp = self.current_work_ticket.get('encp', None)
            if encp:
                pri = (encp.get('curpri', None), encp.get('adminpri', None))
            else:
                pri = (None, None)
        else:
            pri = (None, None)

        now = time.time()
        if self.unique_id and state in (IDLE, HAVE_BOUND):
            ## If we've been idle for more than 15 minutes, force the LM to clear
            ## any entry for this mover in the work_at_movers.  Yes, this is a
            ## kludge, but it keeps the system from getting completely hung up
            ## if the LM doesn't realize we've finished a transfer.
            if now - self.state_change_time > 900:
                self.unique_id = None

        volume_status = (['none', 'none'], ['none','none'])
        ticket =  {
            "mover":  self.name,
            "address": self.address,
            "external_label":  volume_label,
            "volume": external_label,
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
            'time_in_state': now - self.state_change_time,
            "current_time" : now,
            "library": self.current_library,
            "volume_clerk": self.vc_address,
            "library_list":self.libraries, # this is needed for the federation project
            'current_priority': pri,
            }
        return ticket
    """
    def dismount_volume(self, after_function=None):
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
    """

    def status(self, ticket):
        """
        Overriden from :meth:`Mover.status`.

        """
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

        ticket.update(tick)
        self.reply_to_caller(ticket)
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
    #mover._do_print({'levels':range(5, 100)})

    mover.start()
    mover.starting = 0
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
                mover.restart()
            except:
                pass

    Trace.log(e_errors.INFO, 'ERROR returned from serve_forever')
