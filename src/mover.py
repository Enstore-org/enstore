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
import select
import exceptions
import traceback

# enstore modules

import setpath
import generic_server
import interface
import dispatching_worker
import volume_clerk_client		
import file_clerk_client		
import media_changer_client		
import callback
import checksum
import e_errors
import udp_client
import socket_ext
import hostaddr
import string_driver

import Trace

class MoverError(exceptions.Exception):
    def __init__(self, arg):
        exceptions.Exception.__init__(self,arg)


#states
IDLE, MOUNT_WAIT, SEEK, ACTIVE, HAVE_BOUND, DISMOUNT_WAIT, DRAINING, OFFLINE, CLEANING, ERROR = range(10)

_state_names=['IDLE', 'MOUNT_WAIT', 'SEEK', 'ACTIVE', 'HAVE_BOUND', 'DISMOUNT_WAIT',
             'DRAINING', 'OFFLINE', 'CLEANING', 'ERROR']

##assert len(_state_names)==10

def state_name(state):
    return _state_names[state]

#modes
READ, WRITE = range(2)

def mode_name(mode):
    if mode is None:
        return None
    else:
        return ['READ','WRITE'][mode]

KB=1L<<10
MB=1L<<20
GB=1L<<30

SANITY_SIZE = 65536

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

    def reset(self):
        self._lock.acquire()

        #notify anybody who is waiting on this? - cgw
        self.read_ok.set()
        self.write_ok.clear()
        
        self._buf = []
        self._freelist = []
        self._buf_bytes = 0
        self._reading_block = None
        self._writing_block = None
        self._read_ptr = 0
        self._write_ptr = 0
        self.complete_crc = 0L
        self.sanity_crc = 0L
        self.sanity_bytes = 0L
        self.header_size = None
        self.trailer_size = 0
        self._lock.release()
        
    def nbytes(self): #this is only approximate..
        n = self._buf_bytes
        return n
        
    def full(self):
        return self.nbytes() >= self.max_bytes
    
    def empty(self):
        ## this means that a "pull" would fail
        return len(self._buf) == 0

    def low(self):
        return self.empty() or self._buf_bytes < self.min_bytes
    
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

    def block_read(self, nbytes, driver):
        space = self._getspace()
        bytes_read = driver.read(space, 0, nbytes)
        if bytes_read == nbytes: #normal case
            self.push(space)
        elif bytes_read<=0: #error
            pass #XXX or raise an exception?
        else: #partial block read
            partial=space[:bytes_read]
            self.push(partial)
            self._freespace(space)
        return bytes_read
        
    def block_write(self, nbytes, driver):
        data = self.pull() 
        if len(data)!=nbytes:
            raise ValueError, "asked to write %s bytes, buffer has %s" % (nbytes, len(data))
        bytes_written = driver.write(data, 0, nbytes)
        if bytes_written == nbytes: #normal case
            self._freespace(data)
        else: #XXX raise an exception?
            self._freespace(data)
        return bytes_written
        
    def stream_read(self, nbytes, driver):
        do_crc = 1
        if type(driver) is type (""):
            driver = string_driver.StringDriver(driver)
            do_crc = 0
        if not self._reading_block:
            self._reading_block = self._getspace()
            self._read_ptr = 0
        bytes_to_read = min(self.blocksize - self._read_ptr, nbytes)
        bytes_read = driver.read(self._reading_block, self._read_ptr, bytes_to_read)
        if do_crc:
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
        if self._reading_block and self._read_ptr:
            data = self._reading_block[:self._read_ptr]
            self.push(data)
            self._reading_block = None
            self._read_ptr = None
    
    def stream_write(self, nbytes, driver):
        if not self._writing_block:
            if self.empty():
                Trace.trace(10, "stream_write: buffer empty")
                return 0
            self._writing_block = self.pull()
            self._write_ptr = 0
        bytes_to_write = min(len(self._writing_block)-self._write_ptr, nbytes)
        if driver:
            bytes_written = driver.write(self._writing_block, self._write_ptr, bytes_to_write)
            self.complete_crc = checksum.adler32_o(self.complete_crc, self._writing_block,
                                                   self._write_ptr, bytes_written)
            if self.sanity_bytes < SANITY_SIZE:
                nbytes = min(SANITY_SIZE-self.sanity_bytes, bytes_written)
                self.sanity_crc = checksum.adler32_o(self.sanity_crc, self._writing_block,
                                                            self._write_ptr, nbytes)
                self.sanity_bytes = self.sanity_bytes + nbytes
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
            r = '\0' * self.blocksize
        self._lock.release()
        return r
    
    def _freespace(self, s):
        self._lock.acquire()
        self._freelist.append(s)
        self._lock.release()

def cookie_to_long(cookie): # cookie is such a silly term, but I guess we're stuck with it :-(
    if type(cookie) is type(0L):
        return cookie
    if type(cookie) is type(0):
        return long(cookie)
    if type(cookie) != type(''):
        raise TypeError, "expected string or integer"
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
        
    
class Mover(dispatching_worker.DispatchingWorker,
            generic_server.GenericServer):

    def __init__(self, csc_address, name):

        self.name = name
        self.t0 = time.time()
        
        generic_server.GenericServer.__init__(self, csc_address, name)

        self.config = self.csc.get( name )
        if self.config['status'][0] != 'ok':
            raise MoverError('could not start mover %s: %s'%(name, self.config['status']))

        logname = self.config.get('logname', name)
        Trace.init(logname)
        
        self.address = (self.config['hostip'], self.config['port'])

        self.do_eject = 1
        if self.config.has_key('do_eject'):
            if self.config['do_eject'][0] in ('n','N'):
                self.do_eject = 0

        self.default_dismount_delay = self.config.get('dismount_delay', 60)
        if self.default_dismount_delay < 0:
            self.default_dismount_delay = 31536000 #1 year

        self.mc_device = self.config.get('mc_device', 'UNDEFINED')
        self.min_buffer = self.config.get('min_buffer', 8*MB)
        self.max_buffer = self.config.get('max_buffer', 64*MB)
        self.buffer = Buffer(0, self.min_buffer, self.max_buffer)
        self.udpc =  udp_client.UDPClient()
        self.state = IDLE
        self.last_error = (e_errors.OK, None)
        if self.check_lockfile():
            self.state = OFFLINE
        self.current_location = 0L
        self.current_volume = None #external label of current mounted volume
	self.last_location = 0L
	self.last_volume = None
        self.mode = None # READ or WRITE
        self.bytes_to_transfer = 0L
        self.bytes_to_read = 0L
        self.bytes_to_write = 0L
        self.bytes_read = 0L
        self.bytes_written = 0L
        self.volume_family = None 
        self.volume_status = (['none', 'none'], ['none', 'none'])
        self.files = ('','')
        self.transfers_completed = 0
        self.transfers_failed = 0
        self.current_work_ticket = {}
        self.vol_info = {}
        self.dismount_time = None
        self.delay = 0
        self.mcc = media_changer_client.MediaChangerClient( self.csc,
                                                            self.config['media_changer'] )
        self.config['device'] = os.path.expandvars( self.config['device'] )

        import net_driver
        self.net_driver = net_driver.NetDriver()
        self.client_socket = None

        self.config['name']=self.name 
        self.config['product_id']='Unknown'
        self.config['serial_num']=0
        self.config['vendor_id']='Unknown'
        self.config['local_mover'] = 0 #XXX who still looks at this?
        self.driver_type = self.config['driver']
        if self.driver_type == 'NullDriver':
            self.device = None
            import null_driver
            self.tape_driver = null_driver.NullDriver()
        elif self.driver_type == 'FTTDriver':
            self.device = self.config['device']
            import ftt_driver
            import ftt
            self.tape_driver = ftt_driver.FTTDriver()
            have_tape = self.tape_driver.open(self.device, mode=0, retry_count=2)
            Trace.trace(10, "checking for loaded tape, open returns %s" % (have_tape,))
            stats = self.tape_driver.ftt.get_stats()
            self.config['product_id'] = stats[ftt.PRODUCT_ID]
            self.config['serial_num'] = stats[ftt.SERIAL_NUM]
            self.config['vendor_id'] = stats[ftt.VENDOR_ID]
            if have_tape == 1:
                self.tape_driver.set_mode(compression = 0, blocksize = 0)
                self.tape_driver.rewind()
                buf=80*' '
                try:
                    self.tape_driver.read(buf, 0, 80)
                    Trace.trace(10, "checking for label: read %s" % (buf,))
                except (e_errors.READ_ERROR, ftt.FTTError), detail:
                    Trace.log(e_errors.ERROR, "while checking for loaded tape: %s"%(detail,))
                tok = string.split(buf)
                if tok:
                    buf = tok[0]
                if buf[:4]=='VOL1':
                    volname=buf[4:]
                    self.current_volume = volname
                    self.state = HAVE_BOUND
                    Trace.log(e_errors.INFO, "have vol %s at startup" % (self.current_volume,))
                    self.dismount_time = time.time() + self.default_dismount_delay
            ##XXX do a dismount here - cgw
            self.tape_driver.close()
        else:
            print "Sorry, only Null and FTT driver allowed at this time"
            sys.exit(-1)
	dispatching_worker.DispatchingWorker.__init__( self, self.address)
        self.libraries = []
        lib_list = self.config['library']
        if type(lib_list) != type([]):
            lib_list = [lib_list]
        for lib in lib_list:
            lib_config = self.csc.get(lib)
            self.libraries.append((lib, (lib_config['hostip'], lib_config['port'])))
        self.set_interval_func(self.update, 5) #this sets the period for messages to LM.
        self.set_error_handler(self.handle_mover_error)
        ##end of __init__

    def nowork( self, ticket ):
	return {}

    def handle_mover_error(self, exc, msg, tb):
        Trace.log(e_errors.ERROR, "handle mover error %s %s"%(exc, msg))
        Trace.trace(10, "%s %s" %(self.current_work_ticket, state_name(self.state)))
        if self.current_work_ticket:
            try:
                Trace.trace(10, "handle error: calling transfer failed, str(msg)=%s"%(str(msg),))
                self.transfer_failed(exc, msg)
            except:
                pass
    
    def update(self, reset_timer=None):
        Trace.trace(20,"update: %s" % (state_name(self.state)))
        if not hasattr(self,'_last_state'):
            self._last_state = None
        if self.state in (CLEANING, DRAINING, OFFLINE, SEEK, MOUNT_WAIT, DISMOUNT_WAIT):
            if self.state == self._last_state:
                return
        if reset_timer:
            self.reset_interval_timer()
        ticket = self.format_lm_ticket()
        for lib, addr in self.libraries:
            if self.state is OFFLINE and self._last_state is OFFLINE:
                continue
            if self.state != self._last_state:
                Trace.trace(10, "Send %s to %s" % (ticket, addr))
            self.udpc.send_no_wait(ticket, addr)

        self._last_state = self.state
        self.check_dismount_timer()

    def check_dismount_timer(self):
        ## See if the delayed dismount timer has expired
        now = time.time()
        if self.state is HAVE_BOUND and self.dismount_time and now>self.dismount_time:
            Trace.trace(10,"Dismount time expired %s"% (self.current_volume,))
            self.run_in_thread('media_thread', self.dismount_volume, after_function=self.idle)
            
    def idle(self):
        self.state = IDLE
        self.mode = None
        self.volume_status = (['none', 'none'], ['none','none'])
        self.vol_info = {}
        self.update() 

    def offline(self):
        self.state = OFFLINE
        self.update()

    def reset(self):
        self.current_work_ticket = None
        self.buffer.reset()
        self.bytes_read = 0L
        self.bytes_written = 0L
        
    def return_work_to_lm(self,ticket):
        try:
            lm_address = ticket['lm']['address']
        except KeyError, msg:
            self.malformed_ticket(ticket, "[lm][address]")
            return
        ticket = self.format_lm_ticket(state=ERROR, error_info=(e_errors.MOVER_BUSY,
                                                                state_name(self.state)),
                                       returned_work=ticket)
        self.udpc.send_no_wait(ticket, lm_address)

        
    def read_client(self):
        Trace.trace(10, "read_client,  bytes_to_read=%s" % (self.bytes_to_read,))

        driver = self.net_driver
        if self.bytes_read == 0 and self.header: #splice in cpio headers, as if they came from client
            nbytes = self.buffer.header_size
            bytes_read = self.buffer.stream_read(nbytes,self.header)

        while self.state in (ACTIVE, DRAINING) and self.bytes_read < self.bytes_to_read:

            if self.buffer.full():
                Trace.trace(15, "read_client: buffer full %s/%s" % (self.buffer.nbytes(), self.buffer.max_bytes))
                self.buffer.read_ok.clear()
                self.buffer.read_ok.wait(1)
                continue

            nbytes = min(self.bytes_to_read - self.bytes_read, self.buffer.blocksize)
            bytes_read = 0
            try:
                bytes_read = self.buffer.stream_read(nbytes, driver)
            except exceptions.Exception, detail:
                self.transfer_failed(e_errors.READ_ERROR, detail)
                return
            if bytes_read <= 0:  #  The client went away!
                Trace.log(e_errors.ERROR, "read_client: dropped connection")
                self.transfer_failed(e_errors.READ_ERROR, None)
                return
            self.bytes_read = self.bytes_read + bytes_read

            if self.buffer.nbytes() >= self.buffer.min_bytes:
                self.buffer.write_ok.set()
                
        if self.bytes_read == self.bytes_to_read:
            if self.trailer:
                nbytes = self.buffer.trailer_size
                bytes_read = self.buffer.stream_read(nbytes, self.trailer)
                Trace.trace(10, "read %s bytes of  trailer" % bytes_read)
            self.buffer.eof_read() #pushes last partial block onto the fifo
            self.buffer.write_ok.set()

            
        Trace.trace(10, "read_client: state = %s, read %s/%s bytes" %
                    (state_name(self.state), self.bytes_read, self.bytes_to_read))
                        
    def write_tape(self):
        Trace.trace(10, "write_tape, bytes_to_write=%s" % (self.bytes_to_write,))
        driver = self.tape_driver
        count = 0
        while self.state in (ACTIVE, DRAINING) and self.bytes_written<self.bytes_to_write:

            if self.bytes_read < self.bytes_to_read and self.buffer.low():
                Trace.trace(15,"write_tape: buffer low %s/%s"%
                            ( self.buffer.nbytes(), self.buffer.min_bytes))
                self.buffer.write_ok.clear()
                self.buffer.write_ok.wait(1)
                continue

            count = (count + 1) % 20
            if count == 0:
                ##Dynamic setting of low-water mark
                if self.bytes_read >= self.buffer.min_bytes:
                    netrate, junk = self.net_driver.rates()
                    taperate, junk = self.tape_driver.rates()
                    if taperate > 0:
                        ratio = netrate/(taperate*1.0)
                        optimal_buf = self.bytes_to_transfer * (1-ratio)
                        optimal_buf = min(optimal_buf, self.max_buffer)
                        optimal_buf = max(optimal_buf, 2*self.buffer.blocksize)
                        Trace.trace(15,"netrate = %.3g, taperate=%.3g" % (netrate, taperate))
                        if self.buffer.min_bytes != optimal_buf:
                            Trace.trace(15,"Changing buffer size from %s to %s"%
                                        (self.buffer.min_bytes, optimal_buf))
                            self.buffer.set_min_bytes(optimal_buf)

            nbytes = min(self.bytes_to_write - self.bytes_written, self.buffer.blocksize)

            bytes_written = 0
            try:
                bytes_written = self.buffer.block_write(nbytes, driver)
            except exceptions.Exception, detail:
                self.transfer_failed(e_errors.WRITE_ERROR, detail)
                break
            if bytes_written != nbytes:
                self.transfer_failed(e_errors.WRITE_ERROR, None) #XXX detail?
                break
            self.bytes_written = self.bytes_written + bytes_written

            if not self.buffer.full():
                self.buffer.read_ok.set()
        
            
        Trace.trace(10, "write_tape, state = %s, wrote %s/%s bytes" %
                    (state_name(self.state), self.bytes_written, self.bytes_to_write))

        if self.bytes_written == self.bytes_to_write:
            self.tape_driver.writefm()
            self.tape_driver.flush()
            if self.update_after_writing():
                self.transfer_completed()


    def read_tape(self):
        Trace.trace(10, "read_tape, bytes_to_read=%s" % (self.bytes_to_read,))
        driver = self.tape_driver
        while self.state in (ACTIVE, DRAINING) and self.bytes_read < self.bytes_to_read:

            if self.buffer.full():
                Trace.trace(15, "read_tape: buffer full %s/%s" % (self.buffer.nbytes(), self.buffer.max_bytes))
                self.buffer.read_ok.clear()
                self.buffer.read_ok.wait(1)
                continue
            
            nbytes = min(self.bytes_to_read - self.bytes_read, self.buffer.blocksize)
            if self.bytes_read == 0 and nbytes<self.buffer.blocksize: #first read, try to read a whole block
                nbytes = self.buffer.blocksize

            bytes_read = 0
            try:
                bytes_read = self.buffer.block_read(nbytes, driver)
            except exceptions.Exception, detail:
                self.transfer_failed(e_errors.READ_ERROR, detail)
                break
            if bytes_read <= 0:
                self.transfer_failed(e_errors.READ_ERROR, None) ##XXX detail?
                break
            if self.bytes_read==0: #Handle variable-sized cpio header
                if len(self.buffer._buf) != 1:
                    Trace.log(e_errors.ERROR, "read_tape: error skipping over cpio header, len(buf)=%s"%(len(self.buffer._buf)))
                b0 = self.buffer._buf[0]
                if len(b0) >= self.wrapper.min_header_size:
                    header_size = self.wrapper.header_size(b0)
                    self.buffer.header_size = header_size
                    self.bytes_to_read = self.bytes_to_read + header_size
            self.bytes_read = self.bytes_read + bytes_read
            if self.bytes_read > self.bytes_to_read: #this is OK, we read a cpio trailer or something
                self.bytes_read = self.bytes_to_read

            if not self.buffer.empty():
                self.buffer.write_ok.set()
            
        Trace.trace(10, "read_tape: state=%s, read %s/%s bytes" %
                    (state_name(self.state), self.bytes_read, self.bytes_to_read))
                
    def write_client(self):
        Trace.trace(10, "write_client, bytes_to_write=%s" % (self.bytes_to_write,))
        driver = self.net_driver
        if self.bytes_written == 0 and self.wrapper: #Skip over cpio or other headers
            while self.buffer.header_size is None and self.state in (ACTIVE, DRAINING):
                Trace.trace(15, "write_client: waiting for read_tape to set header info")
                self.buffer.write_ok.clear()
                self.buffer.write_ok.wait(1)
                
            self.buffer.stream_write(self.buffer.header_size, None)

        while self.state in (ACTIVE, DRAINING) and self.bytes_written < self.bytes_to_write:
            if self.buffer.empty():
                Trace.trace(15, "write_client: buffer empty %s/%s" %
                            (self.buffer.nbytes(), self.buffer.min_bytes))
                self.buffer.write_ok.clear()
                self.buffer.write_ok.wait(1)

            nbytes = min(self.bytes_to_write - self.bytes_written, self.buffer.blocksize)
            bytes_written = 0
            try:
                bytes_written = self.buffer.stream_write(nbytes, driver)
            except exceptions.Exception, detail:
                self.transfer_failed(e_errors.WRITE_ERROR, detail)
                break
            if bytes_written < 0:
                self.transfer_failed(e_errors.WRITE_ERROR, None) #XXX detail?
                break
            if bytes_written != nbytes:
                pass #this is not unexpected, since we send with MSG_DONTWAIT
            self.bytes_written = self.bytes_written + bytes_written

            if not self.buffer.full():
                self.buffer.read_ok.set()
            
        Trace.trace(10, "write_client: state=%s, wrote %s/%s bytes" %
                    (state_name(self.state), self.bytes_written, self.bytes_to_write))
  
        if self.bytes_written == self.bytes_to_write:
            self.transfer_completed()

        
    # the library manager has asked us to write a file to the hsm
    def write_to_hsm( self, ticket ):
        Trace.log(e_errors.INFO, "WRITE_TO_HSM")
        self.setup_transfer(ticket, mode=WRITE)
        
    # the library manager has asked us to read a file from the hsm
    def read_from_hsm( self, ticket ):
        Trace.log(e_errors.INFO,"READ FROM HSM")
        self.setup_transfer(ticket, mode=READ)

    def setup_transfer(self, ticket, mode):
        Trace.trace(10, "setup transfer")
        if self.state not in (IDLE, HAVE_BOUND):
            Trace.log(e_errors.ERROR, "Not idle %s" %( state_name(self.state),))
            self.return_work_to_lm(ticket)
            return 0
        ### cgw - abstract this to a check_valid_filename method of the driver ?
        if self.config['driver'] == "NullDriver": 
            filename = ticket['wrapper'].get("pnfsFilename",'')
            if "NULL" not in string.split(filename,'/'):
                ticket['status']=(e_errors.USERERROR, "NULL not in PNFS path")
                self.send_client_done( ticket, e_errors.USERERROR, "NULL not in PNFS path" )
                return 0
        self.reset()

        ticket['mover']={}
        ticket['mover'].update(self.config)
        ticket['mover']['device'] = "%s:%s" % (self.config['host'], self.config['device'])

        self.current_work_ticket = ticket
        self.control_socket, self.client_socket = self.connect_client()
        Trace.trace(10,  "client connect %s %s" %( self.control_socket, self.client_socket))
        if not self.client_socket:
            ##XXX ENCP GONE
            return 0

        self.t0 = time.time()

        ##all groveling around in the ticket should be done here
        fc = ticket['fc']
        vc = ticket['vc']
        self.vol_info.update(vc)
        self.volume_family=vc['volume_family']
        delay = 0
        if ticket['encp'].has_key('delayed_dismount'):
            delay = 60 * int(ticket['encp']['delayed_dismount']) #XXX is this right? minutes?
                                                                  ##what does the flag really mean?
        self.delay = max(delay, self.default_dismount_delay)
        self.fcc = file_clerk_client.FileClient( self.csc, bfid=0,
                                                 server_address=fc['address'] )
        self.vcc = volume_clerk_client.VolumeClerkClient(self.csc,
                                                         server_address=vc['address'])
        volume_label = fc['external_label']
        self.current_work_ticket = ticket
        
        vol_info = self.query_volume_clerk(volume_label)
        if vol_info['status'][0] != 'ok':
            return 0 #XXX NOTAPE
        
        self.buffer.set_blocksize(self.vol_info['blocksize'])
        
        self.wrapper_type = self.vol_info.get('wrapper')
        if self.wrapper_type is None:
            ##XXX hack - why is wrapper not coming in on the vc ticket?
            fam = (self.vol_info.get("volume_family") or
                   self.vol_info.get("file_family"))
            self.wrapper_type = string.split(fam,'.')[-1]
        try:
            self.wrapper = __import__(self.wrapper_type + '_wrapper')
        except ImportError, detail:
            Trace.log(e_errors.ERROR, "%s"%(self.wrapper_type, detail))
            self.wrapper = None
            
        self.client_filename = ticket['wrapper'].get('fullname','?')
        self.pnfs_filename = ticket['wrapper'].get('pnfsFilename', '?')

        self.mode = mode
        self.bytes_to_transfer = long(fc['size'])
        self.bytes_to_write = self.bytes_to_transfer
        self.bytes_to_read = self.bytes_to_transfer

        if self.mode == READ:
            self.files = (self.pnfs_filename, self.client_filename)
            self.target_location = cookie_to_long(fc['location_cookie'])
            self.buffer.header_size = None
        elif self.mode == WRITE:
            self.files = (self.client_filename, self.pnfs_filename)
            if self.wrapper:
                self.header, self.trailer = self.wrapper.headers(ticket['wrapper'])
            else:
                self.header = ''
                self.trailer = ''
            self.buffer.header_size = len(self.header)
            self.buffer.trailer_size = len(self.trailer)
            self.bytes_to_write = self.bytes_to_write + len(self.header) + len(self.trailer)
            self.target_location = None        

        if volume_label == self.current_volume: #no mount needed
            self.position_media()
        else:
            self.run_in_thread('media_thread', self.mount_volume, args=(volume_label,),
                               after_function=self.position_media)



    def position_media(self):
        #At this point the correct volume is loaded; now position it
        Trace.trace(10, "position media")
        have_tape = self.tape_driver.open(self.device, self.mode, retry_count=10)
        self.tape_driver.set_mode(compression = 0, blocksize = 0)
        if have_tape != 1:
            #This is bad...
            Trace.log(e_errors.ERROR, "cannot open tape device for positioning")
            self.state = ERROR
            return
        
        self.state = SEEK

        volume_label = self.current_volume
        
        if self.mode is WRITE:
            eod = self.vol_info['eod_cookie']
            if eod in (None, "none"):
                ## new tape, label it
                ## XXX need to safeguard against relabeling here
                self.tape_driver.rewind()
                vol1_label = 'VOL1'+ volume_label
                vol1_label = vol1_label+ (79-len(vol1_label))*' ' + '0'
                Trace.log(e_errors.INFO, "labeling new tape %s" % volume_label)
                self.tape_driver.write(vol1_label, 0, 80)
                self.tape_driver.writefm()
                eod = 1
                self.vol_info['eod_cookie'] = eod
                if self.driver_type == 'FTTDriver':
                    import ftt
                    stats = self.tape_driver.ftt.get_stats()
                    rt = stats[ftt.REMAIN_TAPE]
                    if rt is not None:
                        rt = long(rt)
                        self.vol_info['remaining_bytes'] = rt * 1024L #XXX keep everything in KB?
                self.vcc.set_remaining_bytes( volume_label,
                                              self.vol_info['remaining_bytes'],
                                              self.vol_info['eod_cookie'])

            if self.target_location is None:
                self.target_location = eod
            if self.target_location != eod:
                return 0# Can only write at end of tape

        location = cookie_to_long(self.target_location)
        self.run_in_thread('seek_thread', self.seek_to_location,
                           args = (location, self.mode==WRITE and location==eod,),
                           after_function=self.start_transfer)
        
        return 1
            
    def transfer_failed(self, exc=None, msg=None):
        Trace.log(e_errors.ERROR, "transfer failed %s %s" % (str(exc), str(msg)))

        self.last_error = exc, msg
        
        if self.state == ERROR:
            Trace.log(e_errors.ERROR, "Mover already in ERROR state %s" % (msg,))
            return

        ###XXX network errors should not count toward rd_err, wr_err
        if self.mode == WRITE:
            self.vcc.update_counts(self.current_volume, wr_err=1, wr_access=1)
        else:
            self.vcc.update_counts(self.current_volume, rd_err=1, rd_access=1)       
        msgstr = str(msg) #XXX convert to appropriate Enstore error
        self.transfers_failed = self.transfers_failed + 1
        self.timer('transfer_time')

        if msg:
            self.send_client_done(self.current_work_ticket, msgstr)
        self.net_driver.close()

        if self.state == DRAINING:
            self.dismount_volume()
            self.state = OFFLINE
        else:
            self.state = HAVE_BOUND
        now = time.time()
        self.dismount_time = now + self.delay
        self.update(reset_timer=1)

        
    def transfer_completed(self):
        Trace.log(e_errors.INFO, "transfer complete, current_volume = %s, current_location = %s"%(
            self.current_volume, self.current_location))
        
        if self.mode == WRITE:
            self.vcc.update_counts(self.current_volume, wr_access=1)
        else:
            self.vcc.update_counts(self.current_volume, rd_access=1)
        self.transfers_completed = self.transfers_completed + 1
        self.timer('transfer_time')
        self.net_driver.close()
        self.current_location = self.tape_driver.tell()
        now = time.time()
        self.dismount_time = now + self.delay
        self.send_client_done(self.current_work_ticket, e_errors.OK)
        if self.state == DRAINING:
            self.dismount_volume()
            self.state = OFFLINE
        else:
            self.state = HAVE_BOUND
        self.update(reset_timer=1)

    def update_after_writing(self):
        self.current_location = self.tape_driver.tell()
        remaining = self.vol_info['remaining_bytes']-self.bytes_written
        if self.driver_type == 'FTTDriver':
            import ftt
            stats = self.tape_driver.ftt.get_stats()
            if stats[ftt.REMAIN_TAPE]:
                rt = stats[ftt.REMAIN_TAPE]
                if rt is not None:
                    rt = long(rt)
                    remaining = rt  * 1024L
        eod = loc_to_cookie(self.current_location)
        self.vol_info['eod_cookie'] = eod
        self.vol_info['remaining_bytes']=remaining
        sanity_cookie = (self.buffer.sanity_bytes,self.buffer.sanity_crc)
        complete_crc = self.buffer.complete_crc
        fc_ticket = {'location_cookie': loc_to_cookie(self.last_seek),
                     'size': self.bytes_to_transfer,
                     'sanity_cookie': sanity_cookie,
                     'external_label': self.current_volume,
                     'complete_crc': complete_crc}
        ##  HACK:  store 0 to database if mover is NULL
        if self.config['driver']=='NullDriver':
            fc_ticket['complete_crc']=0L
            fc_ticket['sanity_cookie']=(self.buffer.sanity_bytes,0L)
        fcc_reply = self.fcc.new_bit_file( {'work':"new_bit_file",
                                            'fc'  : fc_ticket
                                            } )
        if fcc_reply['status'][0] != e_errors.OK:
            Trace.log( e_errors.ERROR,
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
        Trace.log(e_errors.INFO,"set remaining: %s %s %s" %( self.current_volume, remaining, eod))
        reply = self.vcc.set_remaining_bytes( self.current_volume,
                                              remaining, eod,
                                              bfid )
        self.vol_info.update(reply)
        vol_info = self.query_volume_clerk(self.current_volume)
        self.vol_info.update(vol_info)
        self.update_volume_status(self.vol_info)
        return 1

    #XXX get rid of these silly functions
    def query_volume_clerk(self, label): ###XXX is this function needed or should we just use vcc.
        vol_info=self.vcc.inquire_vol(label)
        ##XXX side-effect, yuk
        self.vol_info.update(vol_info)
        return vol_info 
    def update_volume_status(self, vol_info):
        self.volume_status = (vol_info.get('system_inhibit',['Unknown', 'Unknown']),
                           vol_info.get('user_inhibit',['Unknown', 'Unknown']))

        
    
    def malformed_ticket(self, ticket, expected_keys=None):
        msg = "Missing keys "
        if expected_keys is not None:
            msg = "%s %s"(msg, expected_keys)
        msg = "%s %s"%(msg, ticket)
        Trace.log(e_errors.ERROR, msg)

    def send_client_done( self, ticket, status, error_info=None):
        ticket['status'] = (status, error_info)
        try:
            callback.write_tcp_obj( self.control_socket, ticket)
        except Exceptions.exception, detail:
            Trace.log(e_errors.ERROR, "error in send_client_done: %s" % (detail,))
        self.control_socket.close()
        self.control_socket = None
        return
            
    def connect_client(self):
        # cgw - Should this thread out?
        try:
            ticket = self.current_work_ticket
            data_ip=self.config.get("data_ip",None)
            host, port, listen_socket = callback.get_callback(fixed_ip=data_ip)
            listen_socket.listen(4)
            ticket['mover']['callback_addr'] = (host,port) #client expects this

            control_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            control_socket.connect(ticket['callback_addr'])
            try:
                callback.write_tcp_obj(control_socket, ticket)
            except Exceptions.exception, detail:
                Trace.log(e_errors.ERROR,"error in connect_client_done: %s" % (detail,))
            # we expect a prompt call-back here
            
            read_fds,write_fds,exc_fds=select.select(
                [listen_socket],[],[],60) # one minute timeout
            if listen_socket in read_fds:
                client_socket, address = listen_socket.accept()
                listen_socket.close()
                self.net_driver.fdopen(client_socket)
                return control_socket, client_socket
            else:
                Trace.log(e_errors.INFO, "timeout on waiting for client connect")
                return None, None
        except:
            exc, msg, tb = sys.exc_info()
            Trace.log(e_errors.ERROR, "connect_client:  %s %s %s"%(exc, msg, traceback.format_tb(tb)))
            return None, None 
    
    def format_lm_ticket(self, state=None, error_info=None, returned_work=None):
        status = e_errors.OK, None
        work = None
        if state is None:
            state = self.state
        if state is IDLE:
            work = "mover_idle"
        elif state in (HAVE_BOUND,):
            work = "mover_bound_volume"
        elif state in (ACTIVE, SEEK, CLEANING, MOUNT_WAIT, DISMOUNT_WAIT):
            work = "mover_busy"
            if error_info:
                status = error_info
        elif state in (ERROR, OFFLINE):
            work = "mover_error"  ## XXX If I'm offline should I send mover_error? I don't think so....
            if error_info is None:
                status = self.last_error
            else:
                status = error_info
        if work is None:
            Trace.log(e_errors.ERROR, "state: %s work: %s" %
                      (state_name(state),work))
        if status is None:
            status = e_errors.OK, None
            
        if type(status) != type(()) or len(status)!=2:
            Trace.log(e_errors.ERROR, "status should be 2-tuple, is %s" % (status,))
            status = (status, None)
            
        ticket =  {
            "mover":  self.name,
            "address": self.address,
            "external_label":  self.current_volume,
            "current_location": loc_to_cookie(self.current_location),
            "read_only" : 0, ###XXX todo: multiple drives on one scsi bus, write locking
            "returned_work": returned_work,
            "state": state_name(self.state),
            "status": status, 
            "volume_family": self.volume_family,
            "volume_status": self.volume_status,
            "operation": mode_name(self.mode),
            "work": work,
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
        thread = threading.Thread(group=None, target=function,
                                  name=thread_name, args=args, kwargs={})
        setattr(self, thread_name, thread)
        try:
            thread.start()
        except exceptions.Exception, detail:
            Trace.log(e_errors.ERROR, "starting thread %s: %s" % (thread_name, detail))
        return 0
    
    def dismount_volume(self, after_function=None, error_function=None):
        self.dismount_time = None
        prev_state = self.state
        self.state = DISMOUNT_WAIT
        if self.do_eject:
            have_tape = self.tape_driver.open(self.device, mode=0, retry_count=2)
            if have_tape == 1:
                ejected = self.tape_driver.eject()
                if ejected == -1:
                    Trace.log(e_errors.ERROR, "Oops, cannot eject tape")
                    self.state = ERROR
            self.tape_driver.close()
        vol_info = self.vol_info.copy()
        Trace.log(e_errors.INFO, "dismounting %s" %( self.current_volume,))
        self.last_volume = self.current_volume
        self.last_location = self.current_location

        if not vol_info.get('external_label'):
            vol_info = self.query_volume_clerk(self.current_volume)
            self.vol_info.update(vol_info)

        mcc_reply = self.mcc.unloadvol(vol_info, self.name, self.mc_device)
        status = mcc_reply.get('status')
        if status and status[0]==e_errors.OK:
            self.current_volume = None
            if after_function:
                after_function()
            else:
                self.idle()
        else:
            Trace.log(e_errors.ERROR, "dismount volume: %s" % (status,))
            self.state = ERROR
            if error_function:
                error_function(self)
                

    def mount_volume(self, volume_label, after_function=None, error_function=None):
        self.dismount_time = None
        self.state = MOUNT_WAIT
        self.current_volume = volume_label
        
        Trace.log(e_errors.INFO, "mounting %s, after_function=%s" %( volume_label,after_function))
        self.timer('mount_time')
        
        mcc_reply = self.mcc.loadvol(self.vol_info, self.name, self.mc_device)

        status = mcc_reply.get('status')
        Trace.trace(10, 'mc replies %s' % (status,))
        
        if status and status[0]==e_errors.OK:
            self.state = ACTIVE
            if after_function:
                Trace.trace(10, "mount: calling after function")
                after_function()
        else:
            Trace.log(e_errors.ERROR, "mount volume: %s" % (status,))
            #XXX robot error, deal with it
            if error_function:
                error_function()
            else:
                self.idle()
                self.current_volume = None
            return
    
    def seek_to_location(self, location, eot_ok=0, after_function=None):
        Trace.trace(10, "seeking to %s, after_function=%s"%(location,after_function))
        failed=0
        try:
            self.tape_driver.seek(location, eot_ok)
        except exceptions.Exception, detail:
            self.transfer_failed(e_errors.ERROR, 'positioning error %s' % (detail,))
            failed=1
        self.timer('seek_time')
        self.current_location = self.tape_driver.tell()
        self.last_seek = self.current_location
        if after_function and not failed:
            Trace.trace(10, "seek calling after function %s" % (after_function,))
            after_function()

    def start_transfer(self):
        Trace.trace(10, "start transfer")
        #If we've gotten this far, we've mounted, positioned, and connected to the client.
        #Just start up the work threads and watch the show...
        self.state = ACTIVE
        if self.mode is WRITE:
            self.run_in_thread('net_thread', self.read_client)
            self.run_in_thread('tape_thread', self.write_tape)
        elif self.mode is READ:
            self.run_in_thread('tape_thread', self.read_tape)
            self.run_in_thread('net_thread', self.write_client)
        else:
            self.transfer_failed(e_errors.ERROR, "invalid mode %s" % (self.mode,))
                
    def status( self, ticket ):
        now = time.time()
	tick = { 'status'       : (e_errors.OK,None),
		 'drive_sn'     : self.config['serial_num'],
                 'drive_vendor' : self.config['vendor_id'],
                 'drive_id'     : self.config['product_id'],
		 #
		 'state'        : state_name(self.state),
		 'transfers_completed'     : self.transfers_completed,
                 'transfers_failed': self.transfers_failed,
		 'bytes_read'     : self.bytes_read,
		 'bytes_written'     : self.bytes_written,
                 'bytes_buffered' : self.buffer.nbytes(),
		 # from "work ticket"
		 'bytes_to_transfer': self.bytes_to_transfer,
		 'files'        : self.files,
		 'mode'         : mode_name(self.mode),
                 'current_volume': self.current_volume,
		 'current_location': self.current_location,
		 'last_volume' : self.last_volume,
		 'last_location': self.last_location,
		 'time_stamp'   : now,
                 'buffer_min': self.buffer.min_bytes,
                 'buffer_max': self.buffer.max_bytes,
                 'rate of network': self.net_driver.rates()[0],
                 'rate of tape': self.tape_driver.rates()[0],
                 }
        if self.state is HAVE_BOUND and self.dismount_time and self.dismount_time>now:
            tick['will dismount'] = 'in %.1f seconds'%(self.dismount_time - now)
            
	self.reply_to_caller( tick )
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
        return os.path.join(d, "mover_lock")
        
    def create_lockfile(self):
        filename=self.lockfile_name()
        try:
            f=open(filename,'w')
            f.write('locked\n')
            f.close()
        except IOError:
            Trace.log(e_errors.ERROR, "Cannot write %s"%(filename,))
            
    def remove_lockfile(self):
        filename=self.lockfile_name()
        try:
            os.unlink(filename)
        except IOError:
            Trace.log(e_errors.ERROR, "Cannot unlink %s"%(filename,))

    def check_lockfile(self):
        return os.path.exists(self.lockfile_name())
        
    def start_draining(self, ticket):	    # put itself into draining state
        if self.state is ACTIVE:
            self.state = DRAINING
        elif self.state is IDLE:
            self.state = OFFLINE
        elif self.state is HAVE_BOUND:
            self.state = DRAINING # XXX CGW should dismount here. fix this
        self.create_lockfile()
	out_ticket = {'status':(e_errors.OK,None)}
	self.reply_to_caller( out_ticket )
	return

    def stop_draining(self, ticket):	    # put itself into draining state
        if self.state != OFFLINE:
            out_ticket = {'status':("EPROTO","Not in draining state")}
            self.reply_to_caller( out_ticket )
            return
        self.state = IDLE
        out_ticket = {'status':(e_errors.OK,None)}
        self.reply_to_caller( out_ticket )
        self.remove_lockfile()

    def clean_drive(self, ticket):
        save_state = self.state
        if self.state not in (IDLE, OFFLINE):
            ret = {'status':("EPROTO", "Cleaning not allowed in %s state" % (state_name(self.state)))}
        else:
            self.state = CLEANING
            ret = self.mcc.doCleaningCycle(self.config)
            self.state = save_state
        self.reply_to_caller(ret)
        
class MoverInterface(generic_server.GenericServerInterface):

    def __init__(self):
        # fill in the defaults for possible options
        generic_server.GenericServerInterface.__init__(self)

    #  define our specific help
    def parameters(self):
        return 'mover_name'

    # parse the options like normal but make sure we have a mover
    def parse_options(self):
        interface.Interface.parse_options(self)
        # bomb out if we don't have a mover
        if len(self.args) < 1 :
	    self.missing_parameter(self.parameters())
            self.print_help(),
            os._exit(1)
        else:
            self.name = self.args[0]

#############################################################################

#############################################################################

if __name__ == '__main__':            

    if len(sys.argv)<2:
        sys.argv=["python", "null.mover"] #REMOVE cgw
    # get an interface, and parse the user input

    intf = MoverInterface()
    mover =  Mover( (intf.config_host, intf.config_port), intf.name )
    mover.handle_generic_commands(intf)
    
    while 1:
        try:
            mover.serve_forever()
        except SystemExit:
            Trace.log(e_errors.INFO, "goodbye")
            os._exit(0)
            break
        except:
            try:
                exc, msg, tb = sys.exc_info()
                full_tb = traceback.format_exception(exc,msg,tb)
                for l in full_tb:
                    Trace.log(e_errors.ERROR, l[:-1], {}, "TRACEBACK")
                Trace.log(e_errors.INFO, "restarting after exception")
            except:
                pass

            
    Trace.log(e_errors.INFO, 'ERROR returned from serve_forever')
    



