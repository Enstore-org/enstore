###############################################################################
# src/$RCSfile$   $Revision$
#

"""
MoverServer:
    knows about it's library manager(s)
    knows if it's client is busy (and from whom)

MoverClient:
    knows what kind it is -- disk or tape (DRIVER info)
        o   hsm device
	o   hsm driver
	       functions(  mount/unmount        (load/unload)
	                 , get/set_blocksize ??? - mover to driver init?
			 , get_remaining_bytes  (get_eod_remaining_bytes)
			 , get/set_eod
			 , get_errors
	                 , seek_position        (set_position)
			 , open                 (open_file{read,write})
			 , close                (close_file{read,write})
			 , read/write_block
			 , read/write_data( buf, buf_bytes, fd, fd_bytes )
			 , xferred_bytes      ??? )
        o   net device
	o   net driver
    knows about it's media changer (tape or disk)
        o   load/unload cmds
    knows about the volume it may or may not have control of
        the vol (WRAPPER) info:
	o   label
	o   type of voluem - cpio, ansi, ...
	    functions(  new_vol
	              , file_pos
		      , pre_data
                      , data             to_hsm( fd, fd, byte_cnt )                     to_hsm( read_obj, write_obj, chk_sum_obj, byte_cnt )
                      , post_data )               |    \
        o   block size                            |     \                            python                               c
        o   eod                    read(fd,buf,blk_sz)  write(fd,buf,blk_sz)
                                             \                  /                  obj.fd
    Client Ops:                               `---shared-mem---'                   obj.read_method
        write_to_hsm:				                                   obj.write_method
	    o   check user							   wrapper python's file object to allow read of certain # of bytes
	    o                                                                      buf = r.read( bytes )

"""

import generic_server
import SocketServer
import dispatching_worker
import volume_clerk_client		# -.
import file_clerk_client		#   >-- 3 significant clients
import media_changer_client		# -'
import callback				# used in send_user_done, get_user_sockets
import cpio
import Trace
import driver

import time				# .sleep
import traceback			# print_exc == stack dump
import binascii				# for crc
import pprint				# hopefully get rid of this???
import posix				# waitpid

import e_errors
# for status via exit status (initial method), set using exit_status=m_err.index(e_errors.WRITE_NOTAPE),
#                            and convert back using just ticket['status']=m_err[exit_status]
m_err = [ e_errors.OK,				# exit status of 0 (index 0) is 'ok'
          e_errors.WRITE_NOTAPE,
	  e_errors.WRITE_TAPEBUSY,
	  e_errors.WRITE_BADMOUNT,
	  e_errors.WRITE_BADSPACE,
	  e_errors.WRITE_ERROR,
	  e_errors.WRITE_EOT,		# special (not freeze_tape_in_driver, offline_drive, or freeze_tape)
	  e_errors.WRITE_UNLOAD,
	  e_errors.WRITE_UNMOUNT,
	  e_errors.WRITE_NOBLANKS,	# not handled by mover
	  e_errors.READ_NOTAPE,
	  e_errors.READ_TABEBUSY,
	  e_errors.READ_BADMOUNT,
	  e_errors.READ_BADLOCATE,
	  e_errors.READ_ERROR,
	  e_errors.READ_COMPCRC,
	  e_errors.READ_EOT,
	  e_errors.READ_EOD,
	  e_errors.READ_UNLOAD,
	  e_errors.READ_UNMOUNT,
	  e_errors.ENCP_GONE,
	  e_errors.TCP_HUNG,
	  e_errors.MOVER_CRASH ]	# obviously can not handle this one
	   

def freeze_tape( self, error_info, unload=1 ):# DO NOT UNLOAD TAPE, BUT LIBRARY MANAGER CAN RSP UNBIND??
    vcc.set_system_readonly( self.vol_info['external_label'] )

    logc.send( log_client.ERROR, 0, "MOVER SETTING VOLUME \"SYSTEM NOACCESS\""+repr(error_info)+self.vol_info )

    return unilateral_unbind_next( self, error_info )

def freeze_tape_in_drive( self, error_info ):
    vcc.set_system_readonly( self.vol_info['external_label'] )

    logc.send( log_client.ERROR, 0, "MOVER SETTING VOLUME \"SYSTEM NOACCESS\""+repr(error_info)+self.vol_info )

    return offline_drive( self, error_info )

def offline_drive( self, error_info ):	# call directly for READ_ERROR
    logc.send( log_client.ERROR, 0, "MOVER OFFLINE"+repr(error_info) )

    if error_info == e_errors.READ_ERROR:
	if self.read_error[1]:
	    next_req_to_lm = {}		# so what if library manager is left in the dark???
	    self.state = 'offline'
	else:
	    next_req_to_lm = unilateral_unbind_next( self, error_info )
    else:
	next_req_to_lm = unilateral_unbind_next( self, error_info )
    return next_req_to_lm


def fatal_enstore( self, error_info, ticket ):
    logc.send( log_client.ERROR, 0, "FATAL ERROR - MOVER - "+repr(error_info) )
    rsp = udpc.send( {'work':"unilateral_unbind",'status':error_info}, ticket['address'] )
    while 1: time.sleep( 100 )		# NEVER RETURN!?!?!?!?!?!?!?!?!?!?!?!?
    return

# MUST SEPARATE SYSTEM AND USER FUNCTIONS - I.E. ERROR MIGHT BE USERGONE
# send a message to our user (last means "done with the read or write")
def send_user_done( self, ticket, error_info ):
    ticket['status'] = (error_info,None)
    print "DONE ticket (TCP):"; pprint.pprint( ticket )
    callback.write_tcp_socket( self.control_socket, ticket,
			       "mover send_user_done" )
    self.control_socket.close()
    return

#########################################################################
# The next set of methods are ones that will be invoked via an eval of
# the 'work' key of the ticket received.
#
class MoverClient:
    def __init__( self, config ):
	self.config = config
	self.state = 'idle'
	self.pid = 0
	self.vol_info = {'external_label':''}
	self.read_error = [0,0]		# error this vol ([0]) and last vol ([1])

	if config['device'][0] == '$':
	    dev_rest=config['device'][string.find(config['device'],'/'):]
	    if dev_rest[0] != '/':
		print "device '",config['device'],"' configuration ERROR"
		sys.exit(1)
		pass
	    dev_env = config["device"][1:string.find(config['device'],'/')]
	    try:
		dev_env = os.environ[dev_env];
	    except:
		print "device '",config['device'],"' configuration ERROR"
		sys.exit(1)
		pass
	    config['device'] = dev_env + dev_rest
	    pass

	self.hsm_driver = eval( "driver."+config['driver']+"('"+
		       config['device']+"','none',0)" )
	self.net_driver = Mover()

    def nowork( self, ticket ):
	# nowork is no work
	return {}

    def unbind_volume( self, ticket ):
	# do any driver level rewind, unload, eject operations on the device
	self.hsm_driver.unload()

	# now ask the media changer to unload the volume
	print "ronDBG - asking media change"
	rr = mcc.unloadvol( self.vol_info['external_label'], self.config['mc_device'] )
	print "ronDBG - asked media change"
	if rr['status'][0] != "ok":
	    raise "media loader cannot unload my volume"

	self.vol_info['external_label'] = ''

	return {}

    # the library manager has asked us to write a file to the hsm
    def write_to_hsm( self, ticket ):
	return forked_write_to_hsm( self, ticket )
	
    # the library manager has asked us to read a file to the hsm
    def read_from_hsm( self, ticket ):
	return forked_read_from_hsm( self, ticket )

    pass

#
# End of set of methods that will be invoked via an eval of
# the 'work' key of the ticket received.
#########################################################################

# the library manager has told us to bind a volume so we can do some work
def bind_volume( self, ticket ):

    #
    # NOTE: external_label is in rsponses from both the vc and fc
    #       for a write:
    #          encp contacts lm which contacts vc and gets external_label
    #          lm puts vc-external_label in fc "sub-ticket"
    #       for a read:
    #          encp contacts fc 1st with bfid
    #          fc uses bfid to get external_label from it's database
    #          fc uses external_label to ask vc which library
    #          fc could pas vc info, but it does not as some of the info could
    #                                           get stale
    #          then fc adds it's fc-info to encp ticket and sends it to lm
    
    if self.vol_info['external_label'] == '':
	# NEW VOLUME FOR ME - find out what volume clerk knows about it
	#self.vol_info.update( vcc.inquire_vol(ticket['fc']['external_label']) )
	#if self.vol_info['status'] != "ok":
	#    return 'vol_error'
	self.vol_info['read_errors_this_mover'] = 0
	rsp = mcc.loadvol( ticket['fc']['external_label'],
			   self.config['mc_device'] )
	if rsp['status'][0] != "ok":
	    # it is possible, under normal conditions, for the system to be
	    # in the following race condition:
	    #   library manager told another mover to unbind.
	    #   other mover responds promptly,
	    #   more work for library manager arrives and is given to a
	    #     new mover before the old volume was given back to the library
	    # SHULD I RETRY????????
	    if rsp['status'][0] == "media_in_another_device": time.sleep (10)
	    return 'mc_error'
	pass
    elif ticket['fc']['external_label'] != self.vol_info['external_label']:
	fatal_enstore( self, "unbind label %s before read/write label %s"%(self.vol_info['external_label'],ticket['fc']['external_label']) )
	return 'vol_error'

    # FOR NOW - alway update info - as counts may be updated in child process
    self.vol_info.update( vcc.inquire_vol(ticket['fc']['external_label']) )
    if self.vol_info['status'][0] != "ok": return 'vol_error'

    self.hsm_driver.remaining_bytes = self.vol_info['remaining_bytes']
    self.hsm_driver.set_blocksize( self.vol_info['blocksize'] )
    sts = self.hsm_driver.load( self.vol_info['eod_cookie'] )# SOFTWARE "MOUNT"
    if repr(sts) != 0 and repr(sts) != 'None': return 'mount_error'
	
    return 'ok'

def forked_write_to_hsm( self, ticket ):
    # have to fork early b/c of early user (tcp) check
    # but how do I handle vol??? - prev_vol, this_vol???
    if mvr_config['do_fork']:
	self.pid = os.fork()
	self.state = 'busy'
    if mvr_config['do_fork'] and self.pid != 0:
        #self.net_driver.data_socket.close()# parent copy??? opened in get_user_sockets
	self.vol_info['external_label'] = ticket['fc']['external_label']# assume success
	return {}			# forked with send 
    else:
	logc.send(log_client.INFO,2,"WRITE_TO_HSM"+str(ticket))

	origin_addr = ticket['lm']['address']# who contacts me directly
    
	# First, call the user (to see if they are still there)
	sts = get_user_sockets( self, ticket )
	if sts == "error":
	    return_or_exit( self, origin_addr, e_errors.ENCP_GONE )
	    pass

	t0 = time.time()
	sts = bind_volume( self, ticket )
	ticket['times']['mount_time'] = time.time() - t0
	if   sts == 'vol_error':
	    print "ronDBG - sending WRITE_NOTAPE - after bind"
	    send_user_done( self, ticket, e_errors.WRITE_NOTAPE )
	    return_or_exit( self, origin_addr, e_errors.WRITE_NOTAPE )
	elif sts == 'mc_error':
	    send_user_done( self, ticket, e_errors.WRITE_TAPEBUSY )
	    return_or_exit( self, origin_addr, e_errors.WRITE_TAPEBUSY )
	elif sts == 'mount_error':
	    send_user_done( self, ticket, e_errors.WRITE_BADMOUNT )
	    return_or_exit( self, origin_addr, e_errors.WRITE_BADMOUNT )
	    pass
	    
	sts = vcc.set_writing( self.vol_info['external_label'] )
	if sts['status'][0] != "ok":
	    print "ronDBG - sending WRITE_NOTAPE - after set_writing"
	    send_user_done( self, ticket, e_errors.WRITE_NOTAPE )
	    return_or_exit( self, origin_addr, e_errors.WRITE_NOTAPE )
	    pass

        # setup values before transfer - incase of exception
        wr_size = 0
        media_error = 0
        complete_crc = 0

        logc.send(log_client.INFO,2,"OPEN_FILE_WRITE")
        # open the hsm file for writing
        try:
	    t0 = time.time()
            self.hsm_driver.open_file_write()
	    ticket['times']['seek_time'] = time.time() - t0

	    fast_write = 1
	    print "ronDBG - net_driver=",self.net_driver,"; hsm_driver=",self.hsm_driver

	    # create the wrapper instance (could be different for different tapes)
            wrapper = cpio.Cpio(  self.net_driver, self.hsm_driver, binascii.crc_hqx
				, fast_write )

            logc.send(log_client.INFO,2,"WRAPPER.WRITE")
	    t0 = time.time()
            (wr_size, complete_crc, sanity_cookie) = wrapper.write( ticket )
	    ticket['times']['transfer_time'] = time.time() - t0

	    # close hsm file
            file_cookie = self.hsm_driver.close_file_write()
        except:
            logc.send( log_client.ERROR, 1, "Error writing "+str(ticket) )
	    traceback.print_exc()
            media_error = 1 # I don't know what else to do right now
            wr_err,rd_err,wr_access,rd_access = (1,0,1,0)

        # we've read the file from user, shut down data transfer socket
        self.net_driver.data_socket.close()

        # check for errors and inform volume clerk
        if media_error:
            vcc.update_counts( self.vol_info['external_label'],
                               wr_err, rd_err, wr_access, rd_access )
            send_user_done( self, ticket, e_errors.WRITE_ERROR )
	    return_or_exit( self, origin_addr, e_errors.WRITE_ERROR )

        if wr_size != ticket['wrapper']['size_bytes']:
            msg = "Expected "+repr(ticket['wrapper']['size_bytes'])+\
		  " bytes  but only" +" stored"+repr(wr_size)
            logc.send( log_client.ERROR, 1, msg )
            send_user_done( self, ticket, e_errors.WRITE_ERROR )
	    return_or_exit( self, origin_addr, e_errors.WRITE_ERROR )

        # All is well - write has finished successfully.
        #  get file/eod cookies & remaining bytes & errs & mnts
        eod_cookie = self.hsm_driver.get_eod()
        remaining_bytes = self.hsm_driver.get_eod_remaining_bytes()
        wr_err,rd_err,wr_access,rd_access = self.hsm_driver.get_errors()

        # Tell volume server & update database
        ticket['vc'].update( vcc.set_remaining_bytes(self.vol_info['external_label'],
						     remaining_bytes,
						     eod_cookie,
						     wr_err,rd_err,
						     wr_access,rd_access) )
	rsp = fcc.new_bit_file( {'work':"new_bit_file",
				 'fc'  :{'bof_space_cookie':file_cookie,
					 'sanity_cookie':sanity_cookie,
					 'external_label':self.vol_info['external_label'],
					 'complete_crc':complete_crc}} )
	if rsp['status'][0] != e_errors.OK:
	    print "XXXXXXXXXXXenstore software error"
	    pass
	ticket['fc'] = rsp['fc']
	ticket['mover'] = self.config
	ticket['mover']['callback_addr']        = self.callback_addr# this was the data callback

        logc.send(log_client.INFO,2,"WRITE"+str(ticket))

	send_user_done( self, ticket, e_errors.OK )
	return_or_exit( self, origin_addr, e_errors.OK )
	pass
    pass

def forked_read_from_hsm( self, ticket ):
    # have to fork early b/c of early user (tcp) check
    # but how do I handle vol??? - prev_vol, this_vol???
    if mvr_config['do_fork']:
	self.pid = os.fork()
	self.state = 'busy'
    if mvr_config['do_fork'] and self.pid != 0:
        #self.net_driver.data_socket.close()# parent copy??? opened in get_user_sockets
	self.vol_info['external_label'] = ticket['fc']['external_label']# assume success
	print "FORKED PID ", self.pid
	return {}			# forked with send 
    else:
	logc.send(log_client.INFO,2,"READ_FROM_HSM"+str(ticket))

	origin_addr = ticket['lm']['address']# who contacts me directly

	# First, call the user (to see if they are still there)
	sts = get_user_sockets( self, ticket )
	if sts == "error":
	    return_or_exit( self, origin_addr, e_errors.ENCP_GONE )
	    pass

	t0 = time.time()
	sts = bind_volume( self, ticket )
	ticket['times']['mount_time'] = time.time() - t0

	if   sts == 'vol_error':
	    print "ronDBG - sending WRITE_NOTAPE - after bind"
	    send_user_done( self, ticket, e_errors.READ_NOTAPE )
	    return_or_exit( self, origin_addr, e_errors.READ_NOTAPE )
	elif sts == 'mc_error':
	    send_user_done( self, ticket, e_errors.READ_TAPEBUSY )
	    return_or_exit( self, origin_addr, e_errors.READ_TAPEBUSY )
	elif sts == 'mount_error':
	    send_user_done( self, ticket, e_errors.READ_BADMOUNT )
	    return_or_exit( self, origin_addr, e_errors.READ_BADMOUNT )
	    pass

        # space to where the file will begin and save location
        # information for where future reads will have to space the drive to.

        # setup values before transfer
        media_error = 0
        drive_errors = 0
        bytes_sent = 0			# reset below, BUT not used afterwards!!!!!!!!!!!!!
        complete_crc = 0		# reset below, BUT not used afterwards!!!!!!!!!!!!!

        # open the hsm file for reading and read it
        try:
	    t0 = time.time()
            self.hsm_driver.open_file_read( ticket['fc']['bof_space_cookie'] )
	    ticket['times']['seek_time'] = time.time() - t0

	    # create the wrapper instance (could be different for different tapes)
	    wrapper = cpio.Cpio( self.hsm_driver, self.net_driver, binascii.crc_hqx )

            logc.send(log_client.INFO,2,"WRAPPER.READ")
	    t0 = time.time()
            (bytes_sent, complete_crc) = wrapper.read( ticket['fc']['sanity_cookie'] )
	    ticket['times']['transfer_time'] = time.time() - t0

	    # close hsm file
            self.hsm_driver.close_file_read()
        except:
            logc.send( log_client.ERROR, 1, "Error reading "+str(ticket) )
	    traceback.print_exc()
            media_error = 1 # I don't know what else to do right now
            wr_err,rd_err,wr_access,rd_access = (0,1,0,1)

        # we've sent the hsm file to the user, shut down data transfer socket
        self.net_driver.data_socket.close()

        # get the error/mount counts and update database
        wr_err,rd_err,wr_access,rd_access = self.hsm_driver.get_errors()
        xx = vcc.update_counts( self.vol_info['external_label'],
				wr_err,rd_err,wr_access,rd_access )
	self.vol_info.update( xx )

        # if media error, mark volume readonly, unbind it & tell user to retry
        if media_error :
            vcc.set_system_readonly( self.vol_info['external_label'] )
            send_user_done( self, ticket, e_errors.READ_ERROR )
	    return_or_exit( self, origin_addr, e_errors.READ_ERROR )

        # drive errors are bad:  unbind volule it & tell user to retry
        elif drive_errors :
            vcc.set_hung( self.vol_info['external_label'] )
            send_user_done( self, {"status" : "Mover: Retry: drive_errors "+msg}, e_errors.READ_ERROR )
	    return_or_exit( self, origin_addr, e_errors.READ_ERROR )

        # All is well - read has finished correctly

        # add some info to user's ticket
        ticket['vc'] = self.vol_info
	ticket['mover'] = self.config
	ticket['mover']['callback_addr']        = self.callback_addr# this was the data callback

        logc.send(log_client.INFO,2,"READ"+str(ticket))

	send_user_done( self, ticket, e_errors.OK )
	return_or_exit( self, origin_addr, e_errors.OK )
	pass
    pass

def return_or_exit( self, origin_addr, status ):
    if mvr_config['do_fork']:
	summon_self( self, origin_addr )
	print "ronDBG - exiting with status =",status
	sys.exit( m_err.index(status) )
    else:
	return status_to_request( self, status )
    print "ronDBG - WHHHHHHHHHHHHHHHHOOOOOOOOOOOWWWWWWWWWAAAAA"


# data transfer takes place on tcp sockets, so get ports & call user
# Info is added to ticket
def get_user_sockets( self, ticket ):
    try:
	host, port, listen_socket = callback.get_data_callback()
	self.callback_addr = (host,port)
	listen_socket.listen(4)
	mover_ticket = {"callback_addr":self.callback_addr}
	ticket["mover"] = mover_ticket
	
	# call the user and tell him I'm your mover and here's your ticket
	# ticket should have index ['callback_addr']
	# and then the entire ticket is sent to the callback_addr
	# The user expects the ticket to contain the following fields:
	#           ['unique_id'] == id sent by the user
	#           ['status'] == "ok"
	#           ['mover']['callback_addr']   set above
	self.control_socket = callback.user_callback_socket( ticket )
	
	# we expect a prompt call-back here, and should protect against
	# users not getting back to us. The best protection would be to
	# kick off if the user dropped the control_socket, but I am at
	# home and am not able to find documentation on select...
	
	self.net_driver.data_socket, address = listen_socket.accept()
	listen_socket.close()
	return "ok"
    except:
	return "error"

# create ticket that says we are idle
def idle_mover_next( self ):
    return {'work'   :"idle_mover",
	    'mover'  :self.config['name'],
	    'address': (self.config['hostip'],self.config['port'])}

# create ticket that says we have bound volume x
def have_bound_volume_next( self ):
    next_req_to_lm =  { 'work'   : "have_bound_volume",
			'mover'  : self.config['name'],
			'address': (self.config['hostip'],self.config['port']),
			'state'  : "idle",
			'vc'     : self.vol_info }
    return next_req_to_lm

# create ticket that says we need to unbind volume x
def unilateral_unbind_next( self, error_info ):
    next_req_to_lm = {'work'           : "unilateral_unbind",
		      'mover'          : self.config['name'],
		      'address'        : (self.config['hostip'],self.config['port']),
		      'external_label' : self.vol_info['external_label'],
		      'status'         : (error_info,None)}
    return next_req_to_lm

def summon_self( self, origin_addr ):
    print "ronDBG - summon_self"
    udpc.send_no_wait( {'work':"summon",
			'address':origin_addr,
			'pid':os.getpid()},
		       (self.config['hostip'],self.config['port']) )
    return


class Mover:
    # read a block from the network (from the user).  This method is call
    # from the wrapper object when writing to the HSM
    def read_block( self ):
        badsock = self.data_socket.getsockopt(socket.SOL_SOCKET,
                                              socket.SO_ERROR)
        if badsock != 0:
            print "Mover read_block, pre-recv error:", \
                  errno.errorcode[badsock]
	    pass
	block = self.data_socket.recv(self.hsm_driver.get_blocksize())
        badsock = self.data_socket.getsockopt(socket.SOL_SOCKET,
                                              socket.SO_ERROR)
        if badsock != 0:
            print "Mover read_block, post-recv error:", \
                  errno.errorcode[badsock]
	    pass
	return block

    # write a block to the network (to the user).  This method is call
    # from the wrapper object when reading from the HSM
    def write_block( self, buff ):
        badsock = self.data_socket.getsockopt(socket.SOL_SOCKET,
                                              socket.SO_ERROR)
        if badsock != 0:
            print "Mover write_block, pre-send error:",\
	     	  errno.errorcode[badsock]
	    pass
	count = self.data_socket.send(buff)
        badsock = self.data_socket.getsockopt(socket.SOL_SOCKET,
                                              socket.SO_ERROR)
        if badsock != 0:
            print "Mover write_block, post-send error:", \
                  errno.errorcode[badsock]
	    pass
        return count

    pass


# Gather everything together and add to the mess
class MoverServer(  dispatching_worker.DispatchingWorker
	     	  , generic_server.GenericServer, SocketServer.UDPServer ):
    # NOTE: SocketServer.UDPServer.__init__ method is called by default.
    # Of the two parameters, (server_address, RequestHandlerClass), the
    # RequestHandlerClass becomes obsolete as the process_request method
    # is overridden by DispatchingWorker method which does not use the
    # store RequestHandlerClass
    def __init__( self, server_address, RequestHandlerClass ):
	self.client_obj_inst = MoverClient( mvr_config )
	self.next_req_to_lm = idle_mover_next( self.client_obj_inst )# a "respone" to server being summoned
	SocketServer.UDPServer.__init__( self, server_address, RequestHandlerClass )
	return

    def hello( self, ticket ):
	self.reply_to_caller( {'status':"ok",'extra status':"hi"} )
	return

    def set_timeout( self, ticket ):
	out_ticket = {'status':"ok",'old timeout':self.rcv_timeout}
	out_ticket['extra status'] = "changed"
	try:    self.rcv_timeout = ticket['timeout']
	except: out_ticket['extra status'] = "not changed"
	out_ticket['new timeout'] = self.rcv_timeout
	self.reply_to_caller( out_ticket )
	return

    def respond_with_error( self, ticket ):
	self.reply_to_caller( {'status':"error"} )
	return

    def after_checked_user( self, ticket ):
	return


    def summon( self, ticket ):
	# CHECK IF SUMMON CAME FROM ME
	if 'pid' in ticket.keys(): wait=0
	else:                      wait=posix.WNOHANG

	next_req_to_lm = get_state_build_next_lm_req( self, wait )

	do_next_req_to_lm( self, next_req_to_lm, ticket['address'] )
	    
	return

    def handle_timeout( self ):
        return

    pass

def do_next_req_to_lm( self, next_req_to_lm, address ):
    while next_req_to_lm != {}:
	print "next_req_to_lm",next_req_to_lm 
	rsp_ticket = udpc.send(  next_req_to_lm, address )
	if next_req_to_lm['work'] == "unilateral_unbind":
	    # FOR SOME ERRORS I FREEZE
	    #if next_req_to_lm['status'] in [ ]:
	 	#logc.send( log_client.ERROR, 0, "MOVER FREEZE - told busy mover to do work" )
		#while 1: time.sleep( 1 )# freeze
		#pass
	    pass
	if self.client_obj_inst.state == 'busy' and rsp_ticket['work'] != 'nowork':
	    logc.send( log_client.ERROR, 0, "FATAL ENSTORE - libman told busy mover to do work" )
	    while 1: time.sleep( 1 )	# freeze
	    pass
	# Exceptions are caught (except block) in SocketServer.py.
	# The reply is the command (i.e the network is the computer).
	client_function = rsp_ticket['work']
	method = MoverClient.__dict__[client_function]
	next_req_to_lm = method( self.client_obj_inst, rsp_ticket )
	pass
    return

def get_state_build_next_lm_req( self, wait ):
    if self.client_obj_inst.pid:
	print "ronDBG - yes I have a pid to check; wait =",wait
	pid, status = posix.waitpid( self.client_obj_inst.pid, wait )
	print "ronDBG - waitpid",self.client_obj_inst.pid," returned pid =",pid
	if pid == self.client_obj_inst.pid:
	    print "I just completed -- infrom LIBRARY MANAGER"
	    self.client_obj_inst.pid = 0
	    self.client_obj_inst.state = 'idle'
	    signal = status&0xff
	    exit_status = status>>8
	    next_req_to_lm = status_to_request( self.client_obj_inst,
						exit_status )
	else:
	    next_req_to_lm = have_bound_volume_next( self.client_obj_inst )
	    next_req_to_lm['state'] = 'busy'
	    pass
	pass
    else:
	if self.client_obj_inst.vol_info['external_label'] == '':
	    next_req_to_lm = idle_mover_next( self.client_obj_inst )
	else:
	    next_req_to_lm = have_bound_volume_next( self.client_obj_inst )
	    pass
	next_req_to_lm['state'] = 'idle'
	pass
    return next_req_to_lm

def status_to_request( client_obj_inst, exit_status ):
    next_req_to_lm = {}
    if   m_err[exit_status] == e_errors.OK:
	next_req_to_lm = have_bound_volume_next( client_obj_inst )
	next_req_to_lm['state'] = 'idle'
    elif m_err[exit_status] == e_errors.ENCP_GONE:
	next_req_to_lm = unilateral_unbind_next( client_obj_inst, m_err[exit_status] )
	next_req_to_lm['state'] = 'idle'
    elif m_err[exit_status] == e_errors.WRITE_ERROR:
	next_req_to_lm = offline_drive( client_obj_inst, m_err[exit_status] )
    elif m_err[exit_status] == e_errors.READ_ERROR:
	next_req_to_lm = offline_drive( client_obj_inst, m_err[exit_status] )
	pass
    elif m_err[exit_status] in [e_errors.WRITE_NOTAPE,
				e_errors.WRITE_TAPEBUSY,
				e_errors.READ_NOTAPE,
				e_errors.READ_TABEBUSY]:
	next_req_to_lm = freeze_tape( client_obj_inst, m_err[exit_status] )
	pass
    elif m_err[exit_status] in [e_errors.WRITE_BADMOUNT,
				e_errors.WRITE_BADSPACE,
				e_errors.WRITE_UNLOAD,
				e_errors.WRITE_UNMOUNT,
				e_errors.READ_BADMOUNT,
				e_errors.READ_BADLOCATE,
				e_errors.READ_COMPCRC,
				e_errors.READ_EOT,
				e_errors.READ_EOD,
				e_errors.READ_UNLOAD,
				e_errors.READ_UNMOUNT]:
	next_req_to_lm = freeze_tape_in_driver( client_obj_inst, m_err[exit_status] )
    else:
	# new error
	logc.send( log_client.ERROR, 0, "FATAL ERROR - MOVER - unknown transfer status - fix me now" )
	while 1: time.sleep( 100 )		# NEVER RETURN!?!?!?!?!?!?!?!?!?!?!?!?
	pass
    return next_req_to_lm

#############################################################################

#############################################################################
import sys				# sys.argv[1:]
import os				# os.environ
import getopt				# getopt.getopt
import socket				# gethostname (local host)
import string				# atoi
import types				# see if library config is list
import configuration_client
import udp_client
import log_client

# defaults
try:
    config_host = os.environ['ENSTORE_CONFIG_HOST']
except:
    (config_host,ca,ci) = socket.gethostbyaddr(socket.gethostname())
    pass
try:
    config_port = string.atoi(os.environ['ENSTORE_CONFIG_PORT'])
except:
    config_port = 7500
    pass

# see what the user has specified. bomb out if wrong options specified
options = [ "config_host=","config_port=","help" ]
optlist,args=getopt.getopt(sys.argv[1:],'',options)
for (opt,value) in optlist:
    if opt == "--config_host":   config_host = value
    elif opt == "--config_port": config_port = string.atoi(value)
    elif opt == "--help":
	print "python ",sys.argv[0], options
	print "   do not forget the '--' in front of each option"
	sys.exit(0)
	pass
    pass

# bomb out if we don't have a mover
if len(args) < 1:
    print "python",sys.argv[0], options, "mover_device"
    print "   do not forget the '--' in front of each option"
    sys.exit(1)
    pass

# get clients -- these will be (readonly) global object instances
csc  = configuration_client.ConfigurationClient( config_host, config_port, 0 )
udpc =          udp_client.UDPClient()	# for server to send (client) request
logc =          log_client.LoggerClient( csc, 'MOVER', 'logserver', 0 )
fcc  = file_clerk_client.FileClient( csc )
vcc  = volume_clerk_client.VolumeClerkClient( csc )

# get my (localhost) configuration from the configuration server
mvr_config = csc.get( args[0] )
if mvr_config['status'][0] != "ok":
    raise "could not start mover",args[0]," up:" + mvr_config['status']
# clean up the mvr_config a bit
mvr_config['name'] = args[0]
mvr_config['config_host'] = config_host; del config_host
mvr_config['config_port'] = config_port; del config_port
mvr_config['do_fork'] = 1
del mvr_config['status']

# need a media changer to control (mount/load...) the volume
mcc = media_changer_client.MediaChangerClient( csc, 0,
					       mvr_config['media_changer'] )

# now get my library manager's config ---- COULD HAVE MULTIPLE???
# get info asssociated with our volume manager
libm_config_dict = {}
if type(mvr_config['library']) == types.ListType:
    for lib in  mvr_config['library']:
	libm_config_dict[lib] = {'startup_polled':'not_yet'}
	libm_config_dict[lib].update( csc.get_uncached(lib) )
	pass
    pass
else:
    lib = mvr_config['library']
    libm_config_dict[lib] = {'startup_polled':'not_yet'}
    libm_config_dict[lib].update( csc.get_uncached(lib) )
    pass


print "\nmy address is ",(mvr_config['hostip'],mvr_config['port']),"\n\n\n"
mvr_srvr =  MoverServer( (mvr_config['hostip'],mvr_config['port']), 0 )
mvr_srvr.rcv_timeout = 15

mvr_srvr.serve_forever()

print "ERROR?"
