#!/usr/bin/env python

# $Id$


import os


# python modules
import sys
import os
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
import generic_server
import interface
import dispatching_worker
import volume_clerk_client		
import file_clerk_client		
import media_changer_client		
import callback				
import wrapper_selector
import Trace

import e_errors
import udp_client
import socket_ext
import hostaddr

def print_args(*args):
    print args

Trace.trace = print_args

class MoverError(exceptions.Exception):
    def __init__(self, arg):
        exceptions.Exception.__init__(self,arg)

IDLE, MOUNT_WAIT, ACTIVE, HAVE_BOUND, DISMOUNT_WAIT, DRAINING, OFFLINE, CLEANING, ERROR = range(9)

_state_names=['IDLE', 'MOUNT_WAIT', 'ACTIVE', 'HAVE_BOUND', 'DISMOUNT_WAIT',
              'DRAINING', 'OFFLINE', 'CLEANING', 'ERROR']

def state_name(state):
    return _state_names(state)

READ, WRITE, CLEANING = range(3)
def  mode_name(mode):
    if mode is None:
        return None
    else:
        return ['READ','WRITE','CLEANING'][mode]

MB=1L<<20
GB=1L<<30

class Mover(  dispatching_worker.DispatchingWorker,
              generic_server.GenericServer):
    def __init__( self, csc_address, name):

        self.name = name
        self.t0 = time.time()
        
        generic_server.GenericServer.__init__(self, csc_address, name)
        Trace.init( self.log_name )

        self.config = self.csc.get( name )
        if self.config['status'][0] != 'ok':
            raise MoverError('could not start mover %s: %s'%(name, self.config['status']))

        
        self.address = (self.config['hostip'], self.config['port'])

        self.do_eject = 1
        self.max_buffer = 256*MB;
        self.min_buffer = 16*MB;

        if self.config.has_key('do_eject'):
            if self.config['do_eject'][0] in ('n','N'):
                self.do_eject = 0

        if self.config.has_key('min_buffer'):
            self.min_buffer = string.atoi(self.config['min_buffer'])
        if self.config.has_key('max_buffer'):
            self.min_buffer = string.atoi(self.config['max_buffer'])

        self.udpc =  udp_client.UDPClient()
        self.state = IDLE
        self.last_error = ()
        if self.check_lockfile():
            self.state = OFFLINE

        self.current_location = "0L"
        self.mode = None # READ or WRITE
        self.bytes_to_transfer = 0L
        self.bytes_read = 0L
        self.bytes_written = 0L
        self.current_volume = None #external label of current mounted volume
        self.next_volume = None # external label of pending (MC) volume
        self.volume_family = None 
        self.volume_status = ((None,None), (None,None))
        self.files = ('','')
        self.hsm_drive_sn = ''
        self.no_transfers = 0
        self.current_work_ticket = {}
        self.client_socket = None
        self.tape_fd = None
        
        self.default_dismount_delay = 30
        self.dismount_time = None
        self.delay = 0
        
        self.clear_buffer() #creates self.buffer and self.buffer_bytes
        
        self.driveStatistics = {'mount':{},'dismount':{}}
##        self.mcc = media_changer_client.MediaChangerClient( self.csc,
##                                             self.config['media_changer'] )


        self.read_error = [0,0]         # error this vol ([0]) and last vol ([1])
        self.crc_flag = 1
        self.config['device'] = os.path.expandvars( self.config['device'] )


        if self.config['driver'] != "NullDriver":
            print "Sorry, only NULL driver allowed at this time"
            sys.exit(-1)

##        stats = self.driver.get_stats()
        
        
##        if stats['serial_num'] != None: self.hsm_drive_sn = stats['serial_num']
##        self.config['serial_num'] = stats['serial_num']
##        self.config['product_id'] = stats['product_id']
##        self.config['vendor_id'] = stats['vendor_id']
        
        self.config['name']=self.name
        self.config['product_id']="Product ID"
        self.config['serial_num']=12345
        self.config['vendor_id']="fnal"
        self.config['local_mover'] = 0 #yuk
            
##        # check for tape in drive
##        # if no vol one labels, I can only eject. -- tape maybe left in bad
##        # state.
##        if self.do_eject == 'yes':
##            self.driver_class.offline( self.config['device'] )
##            # tell media changer to unload the vol BUT I DO NOT KNOW THE VOL
##            #mcc.unloadvol( self.vol_info, self.config['mc_device'] )
##            self.mcc.unloadvol( self.vol_info, self.name, 
##                self.config['mc_device'], None)
##            pass
##        self.driver.close( skip=0 )



	# now go on with server setup (i.e. respond to summon,status,etc.)
	dispatching_worker.DispatchingWorker.__init__( self, self.address)
                
        self.libraries = []
        lib_list = self.config['library']

        if type(lib_list) != type([]):
            lib_list = [lib_list]
            
        for lib in lib_list:
            lib_config = self.csc.get(lib)
            self.libraries.append((lib, (lib_config['hostip'], lib_config['port'])))

        self.set_interval_func(self.update, 5) #this sets the period for messages to LM.
        ##end of __init__
        
        
    

    def update(self):

        if self.state in (CLEANING, DRAINING, OFFLINE):
            ### XXX when going offline, we still need to send a message to LM
            return

        ## See if the delayed dismount timer has expired
        now = time.time()
        if self.state is HAVE_BOUND and self.dismount_time and now>self.dismount_time:
            self.dismount_volume()
            self.dismount_time = None
            self.state = IDLE
            
        ticket = self.format_lm_ticket()
        
        for lib, addr in self.libraries:
            print "Send", ticket, "to", addr
            self.udpc.send_no_wait(ticket, addr)

    def nowork( self, ticket ):
	return {}

    #I/O callbacks
    def read_tape(self, fd):
        print "read tape"
        nbytes = min(self.bytes_to_transfer - self.bytes_read, self.blocksize)
        if nbytes == 0:
            print "Nothing to do!"
            return 
        data = os.read(fd, nbytes)
        if len(data) != nbytes:
            self.transfer_aborted(e_errors.READ_ERROR)
            return
        self.buffer.append(data)
        self.buffer_bytes = self.buffer_bytes + nbytes
        self.bytes_read = self.bytes_read + nbytes
        if self.buffer_bytes:
            print "enabling write cli"
            self.add_select_fd(self.client_socket, WRITE, self.write_client)
        
    def write_client(self, sock):
        print "write client"
        if self.buffer_bytes == 0:
            self.remove_select_fd(cli)
            return
        

        nbytes = min(self.bytes_to_transfer - self.bytes_written, self.blocksize)
        data = self.buffer.pop(0)
        nbytes = len(data)
        self.buffer_bytes = self.buffer_bytes - len(data)
        status = sock.send(data)
        if status != nbytes:
            # XXX network write error, what to do?
            pass
        self.bytes_written = self.bytes_written + nbytes
        if self.bytes_written == self.bytes_to_transfer:
            self.transfer_complete()
        pass
    
    def write_tape(self, fd):
        if self.buffer_bytes == 0:
            # turn off the select fd!
            self.remove_select_fd(fd)
            return
        
        nbytes = min(self.bytes_to_transfer - self.bytes_written, self.blocksize)
        print "write tape", nbytes
        if nbytes > self.buffer_bytes:
            print "only have", self.buffer_bytes
            self.remove_select_fd(fd)
            #not enough data in buffer to keep tape streaming
            return
        data = self.buffer.pop(0)
        nbytes = len(data)
        self.buffer_bytes = self.buffer_bytes - nbytes
        status = os.write(fd, data)
        if status != nbytes:
            self.transfer_aborted(e_errors.WRITE_ERROR)
        self.bytes_written = self.bytes_written + nbytes
        if self.bytes_written == self.bytes_to_transfer:
            self.transfer_complete()
            

    def read_client(self, sock):
        print "read client"
        nbytes = min(self.bytes_to_transfer - self.bytes_read, self.blocksize)
        if nbytes == 0:
            print "nothing to do!"
            return 
        data = sock.recv(nbytes)
        print "read", len(data), "bytes"
        print data
        if not data:  #  The client went away!
            self.transfer_aborted(None)
            return
        nbytes = len(data)
        self.buffer.append(data)
        self.buffer_bytes = self.buffer_bytes + nbytes
        self.bytes_read = self.bytes_read + nbytes
        if self.buffer_bytes:
            print "enabling write tape"
            self.add_select_fd(self.tape_fd, WRITE, self.write_tape)
        print "read client exits, buffer bytes=", self.buffer_bytes

    # the library manager has asked us to write a file to the hsm
    def write_to_hsm( self, ticket ):
        print "WRITE TO HSM"
        self.current_work_ticket = ticket

        if not self.setup_transfer(ticket):
            return
        self.state = ACTIVE        

        self.tape_fd = os.open("/dev/null",1)
        print "client fd=", self.client_socket
        print "tape_fd=", self.tape_fd
        self.add_select_fd( self.client_socket, READ, self.read_client)
        self.add_select_fd( self.tape_fd, WRITE, self.write_tape)

        
    # the library manager has asked us to read a file to the hsm
    def read_from_hsm( self, ticket ):
        print "READ FROM HSM"
        self.current_work_ticket = ticket
        
        if not self.client_socket:
            self.transfer_aborted(None)

        if not self.setup_transfer(ticket):
            return
        
        self.state = ACTIVE
        

        self.tape_fd = os.open("/dev/zero",0)
        print "client fd=", self.client_socket
        print "tape_fd=", self.tape_fd
        self.add_select_fd( self.tape_fd, READ, self.read_tape)
        self.add_select_fd( self.client_socket, WRITE, self.write_client)


    def setup_transfer(self, ticket):
        if self.state not in (IDLE, HAVE_BOUND):
            print "Not idle"
            self.return_work_to_lm(ticket)
            return 0

                    
        fname = ticket['wrapper'].get("pnfsFilename",'')
        if "NULL" not in string.split(fname,'/'):
            ticket['status']=(e_errors.USERERROR, "NULL not in PNFS path")
            self.send_client_done( ticket, e_errors.USERERROR, "NULL not in PNFS path" )
            return 0

        pprint.pprint(ticket)
        self.clear_buffer()

        self.current_work_ticket = ticket
        if not ticket.has_key('mover'):
            ticket['mover']={}
        ticket['mover'].update(self.config)
        self.current_work_ticket = ticket
        
	self.fc = ticket['fc']
        self.t0 = time.time()
        
        self.bytes_to_transfer = long(self.fc['size'])
        self.blocksize = 65536 #XXX
        delay = 0
        if ticket['encp'].has_key('delayed_dismount'):
            delay = int(ticket['encp']['delayed_dismount'])
        self.delay = min(delay, self.default_dismount_delay)

        self.fcc = file_clerk_client.FileClient( self.csc, 0,
                                                 ticket['fc']['address'] )

        
        if not self.prepare_volume(self.fc['external_label']):
            return 0
        self.control_socket, self.client_socket = self.connect_client()
        print "client connect", self.control_socket, self.client_socket
        if not self.client_socket:
            return 0

        return 1

    def transfer_aborted(self, msg): #Client is gone...
        print "transfer aborted"
        self.timer('transfer_time')
        self.state = HAVE_BOUND
        self.remove_select_fd(self.client_socket)
        self.remove_select_fd(self.tape_fd)
        if msg:
            self.send_client_done(self.current_work_ticket, msg)
        self.current_work_ticket = None ##XXX let's make a self.reset method
        self.reset()

    def transfer_complete(self):
        print "transfer complete"
        self.timer('transfer_time')
        self.state = HAVE_BOUND
        now = time.time()

        self.remove_select_fd(self.client_socket)
        self.remove_select_fd(self.tape_fd)
        
        self.dismount_time = now + self.delay
        self.client_socket.close()

        fc_ticket = {'location_cookie':'%s'%(self.current_location),
                     'size': self.bytes_to_transfer,
                     'sanity_cookie': (0,0L),
                     'external_label': self.current_volume,
                     'complete_crc': 0L}
            
        fcc_reply = self.fcc.new_bit_file( {'work':"new_bit_file",
                                      'fc'  : fc_ticket
                                      } )
        
        if fcc_reply['status'][0] != e_errors.OK:
            self.transfer_aborted((e_errors.ERROR,"Cannot assign new bit file ID"))

            
            Trace.log( e_errors.ERROR,
                       "cannot assign new bfid")

        self.current_work_ticket['fc'] = fcc_reply['fc']

        self.send_client_done(self.current_work_ticket, e_errors.OK)

        self.reset()

    def reset(self):
        
        self.current_work_ticket = None
        self.clear_buffer()
        self.bytes_read = 0L
        self.bytes_written = 0L

            
        
    def clear_buffer(self):
        self.buffer = []
        self.buffer_bytes = 0L
        
    def return_work_to_lm(self,ticket):
        try:
            lm_address = ticket['lm']['address']
        except KeyError, msg:
            self.malformed_ticket(ticket, "[lm][address]")
            return
        
        ticket = self.format_lm_ticket(state=ERROR, error_info=(e_errors.MOVER_BUSY, ticket))
        self.udpc.send_no_wait(ticket, lm_addr)

    def prepare_volume(self, volume_label):
        self.mount_volume(volume_label)
        self.set_position()
        return 1
    
    def malformed_ticket(self, ticket, expected_keys=None):
        msg = "Missing keys "
        if expected_keys is not None:
            msg = "%s %s"(msg, expected_keys)
        msg = "%s %s"%(msg, ticket)
        print msg
        Trace.log(e_errrors.ERROR, msg)

    def send_client_done( self, ticket, status, error_info=None):
        ticket['status'] = (status, error_info)
        callback.write_tcp_obj( self.control_socket, ticket)
        self.control_socket.close()
        self.control_socket = None
        return
    

    def format_lm_ticket(self, state=None, error_info=None):

        status = e_errors.OK, None

        if state is None:
            state = self.state
            
        if state is IDLE:
            work = "mover_idle"
        elif state in (MOUNT_WAIT, HAVE_BOUND):
            work = "mover_bound_volume"
        elif state in (ACTIVE, DISMOUNT_WAIT):
            work = "mover_busy"
        elif state is ERROR:
            work = "mover_error"
            if ext_info is None:
                status = self.last_error
            else:
                status = error_info

        ticket =  {
            "mover":  self.name,
            "address": self.address,
            "external_label":  self.current_volume,
            "current_location": self.current_location,
            "status": status, 
            "volume_family": self.volume_family,
            "volume_status": self.volume_status,
            "operation": mode_name(self.mode),
            "work": work,
            }
        return ticket


    def dismount_volume(self):
        print "dismounting", self.current_volume
        self.current_volume = None
        return

    def mount_volume(self, volume_label):
        print "mounting", volume_label
        self.timer('mount_time')
        self.current_volume = volume_label
        return
    
    def set_position(self):
        self.timer('seek_time')
        
    # data transfer takes place on tcp sockets, so get ports & call client
    # Info is added to ticket
    def connect_client(self):
        print "connect client"
        try:
            ticket = self.current_work_ticket
            pprint.pprint(ticket)
            data_ip=self.config.get("data_ip",None)
            host, port, listen_socket = callback.get_data_callback(fixed_ip=data_ip)
            listen_socket.listen(4)
            if not ticket.has_key('mover'):
                ticket['mover']={}
            ticket['mover']['callback_addr'] = (host,port) #client expects this
            # ticket must have 'callback_addr' set for the following to work
            control_socket = callback.user_callback_socket( ticket)
            print "ctrl = ", control_socket
            # we expect a prompt call-back here
            
            read_fds,write_fds,exc_fds=select.select(
                [listen_socket],[],[],30) # half minute timeout
            if listen_socket in read_fds:
                client_socket, address = listen_socket.accept()
                listen_socket.close()
                return control_socket, client_socket
            else:
                print "timeout on waiting for client connect"
                return None, None
            
        except:
            exc, msg, tb = sys.exc_info()
            print exc, msg
            traceback.print_tb(tb)
            return None, None #XXX

    
    def status( self, ticket ):

	tick = { 'status'       : (e_errors.OK,None),
		 'drive_sn'     : self.hsm_drive_sn,
		 #
		 'crc_flag'     : str(self.crc_flag),
		 'state'        : state_names[self.state],
		 'no_transfers'     : self.no_transfers,
		 'bytes_read'     : self.bytes_read,
		 'bytes_written'     : self.bytes_written,
		 # from "work ticket"
		 'bytes_to_transfer': self.bytes_to_transfer,
		 'files'        : self.files,
		 'mode'         : self.mode_name(self.mode),
                 'current_volume': self.current_volume,
		 'time_stamp'   : time.time(),
                 }

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
        print self.current_work_ticket['times']
        
    def lockfile_name(self):
        d=os.environ.get("ENSTORE_TMP","/tmp")
        return os.path.join(d, "mover_lock")
        
    def create_lockfile(self):
        fname=self.lockfile_name()
        try:
            f=open(fname,'w')
            f.write('locked\n')
            f.close()
        except IOError:
            Trace.log(e_errors.ERROR, "Cannot write %s"%(fname,))
            
    def remove_lockfile(self):
        fname=self.lockfile_name()
        try:
            os.unlink(fname)
        except IOError:
            Trace.log(e_errors.ERROR, "Cannot unlink %s"%(fname,))

    def check_lockfile(self):
        return os.path.exists(self.lockfile_name())
        

        
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

    # get an interface, and parse the user input
    intf = MoverInterface()

    while 1:
##        try:
            mover =  Mover( (intf.config_host, intf.config_port), intf.name )
            print mover.server_address
            mover.serve_forever()
##        except:
##            print sys.exc_info(), "restarting"
            
    Trace.log(e_errors.INFO, 'ERROR returned from serve_forever')
    



