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
import generic_cs
import interface
import dispatching_worker
import volume_clerk_client		# -.
import file_clerk_client		#   >-- 3 significant clients
import media_changer_client		# -'
import callback				# used in send_user_done, get_user_sockets
import errno
import cpio
import Trace
import driver
import pprint
import time				# .sleep
import traceback			# print_exc == stack dump
import ECRC				# for crc
import posix				# waitpid
import sys				# exit

import e_errors
# for status via exit status (initial method), set using exit_status=m_err.index(e_errors.WRITE_NOTAPE),
#                            and convert back using just ticket['status']=m_err[exit_status]
m_err = [ e_errors.OK,				# exit status of 0 (index 0) is 'ok'
          e_errors.WRITE_NOTAPE,
	  e_errors.WRITE_TAPEBUSY,
	  e_errors.WRITE_BADMOUNT,
	  e_errors.WRITE_BADSPACE,
	  e_errors.WRITE_ERROR,
	  e_errors.WRITE_EOT,		# special (not freeze_tape_in_drive, offline_drive, or freeze_tape)
	  e_errors.WRITE_UNLOAD,
	  e_errors.WRITE_UNMOUNT,
	  e_errors.WRITE_NOBLANKS,	# not handled by mover
	  e_errors.READ_NOTAPE,
	  e_errors.READ_TAPEBUSY,
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

    logc.send( log_client.ERROR, 0, "MOVER SETTING VOLUME \"SYSTEM NOACCESS\""+str(error_info)+str(self.vol_info) )

    return unilateral_unbind_next( self, error_info )

def freeze_tape_in_drive( self, error_info ):
    vcc.set_system_readonly( self.vol_info['external_label'] )

    logc.send( log_client.ERROR, 0, "MOVER SETTING VOLUME \"SYSTEM NOACCESS\""+str(error_info)+str(self.vol_info) )

    return offline_drive( self, error_info )

def offline_drive( self, error_info ):	# call directly for READ_ERROR
    logc.send( log_client.ERROR, 0, "MOVER OFFLINE"+str(error_info) )

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
    logc.send( log_client.ERROR, 0, "FATAL ERROR - MOVER - "+str(error_info) )
    rsp = udpc.send( {'work':"unilateral_unbind",'status':error_info}, ticket['address'] )
    while 1: time.sleep( 100 )		# NEVER RETURN!?!?!?!?!?!?!?!?!?!?!?!?
    return

# MUST SEPARATE SYSTEM AND USER FUNCTIONS - I.E. ERROR MIGHT BE USERGONE
# send a message to our user (last means "done with the read or write")
def send_user_done( self, ticket, error_info ):
    ticket['status'] = (error_info,None)
    callback.write_tcp_socket( self.control_socket, ticket,
			       "mover send_user_done" )
    self.control_socket.close()
    return

#########################################################################
# The next set of methods are ones that will be invoked by calling
# the MoverClient dictionary element specified by the 'work' key of the
# ticket received.
#
class MoverClient:
    def __init__( self, config ):
	self.print_id = "MOVER"
	self.config = config
	self.state = 'idle'
	self.mode = ''			# will be either 'r' or 'w'
	self.pid = 0
	self.vol_info = {'external_label':''}
	self.read_error = [0,0]		# error this vol ([0]) and last vol ([1])

	if config['device'][0] == '$':
	    dev_rest=config['device'][string.find(config['device'],'/'):]
	    if dev_rest[0] != '/':
		self.enprint("device '"+config['device']+\
	                     "' configuration ERROR")
		sys.exit(1)
		pass
	    dev_env = config["device"][1:string.find(config['device'],'/')]
	    try:
		dev_env = os.environ[dev_env];
	    except:
		self.enprint("device '"+config['device']+\
	                     "' configuration ERROR")
		sys.exit(1)
		pass
	    config['device'] = dev_env + dev_rest
	    pass

	self.hsm_driver = eval( 'driver.'+config['driver']+'()' )
	self.net_driver = Mover()

    def nowork( self, ticket ):
	# nowork is no work
	return {}

    def unbind_volume( self, ticket ):
	# do any driver level rewind, unload, eject operations on the device
	if mvr_config['do_eject'] == 'yes':
	    self.hsm_driver.offline(mvr_config['device'])

	# now ask the media changer to unload the volume
	rr = mcc.unloadvol( self.vol_info['external_label'], self.config['mc_device'] )
	if rr['status'][0] != "ok":
	    raise "media loader cannot unload my volume"

	self.vol_info['external_label'] = ''

	return idle_mover_next( self )

    # the library manager has asked us to write a file to the hsm
    def write_to_hsm( self, ticket ):
	self.fc = ticket['fc']
	return forked_write_to_hsm( self, ticket )
	
    # the library manager has asked us to read a file to the hsm
    def read_from_hsm( self, ticket ):
	self.fc = ticket['fc']
	return forked_read_from_hsm( self, ticket )

    pass

#
# End of set of methods are ones that will be invoked by calling
# the MoverClient dictionary element specified by the 'work' key of the
# ticket received.
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
    
    tmp_vol_info = vcc.inquire_vol( ticket['fc']['external_label'] )
    if tmp_vol_info['status'][0] != "ok": return 'NOTAPE' # generic, not read or write specific

    if self.vol_info['external_label'] == '':

	# NEW VOLUME FOR ME - find out what volume clerk knows about it
	#self.vol_info.update( vcc.inquire_vol(ticket['fc']['external_label']) )
	#if self.vol_info['status'] != "ok":
	#    return 'NOTAPE' # generic, not read or write specific
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
	    return 'TAPEBUSY' # generic, not read or write specific
	sts = self.hsm_driver.sw_mount( mvr_config['device'],
					tmp_vol_info['blocksize'],
					tmp_vol_info['remaining_bytes'],
					ticket['fc']['external_label'] )
	if str(sts) != '0' and str(sts) != 'None': return 'BADMOUNT' # generic, not read or write specific
	pass
    elif ticket['fc']['external_label'] != self.vol_info['external_label']:
	fatal_enstore( self, "unbind label %s before read/write label %s"%(self.vol_info['external_label'],ticket['fc']['external_label']) )
	return 'NOTAPE' # generic, not read or write specific

    # FOR NOW - alway update info - as counts may be updated in child process
    self.vol_info.update( tmp_vol_info )

    return e_errors.OK

def forked_write_to_hsm( self, ticket ):
    # have to fork early b/c of early user (tcp) check
    # but how do I handle vol??? - prev_vol, this_vol???
    if mvr_config['do_fork']:
	self.pid = os.fork()
	self.state = 'busy'
	self.mode = 'w'			# client mode, not driver mode
    if mvr_config['do_fork'] and self.pid != 0:
        #self.net_driver.data_socket.close()# parent copy??? opened in get_user_sockets
	pass
    else:
	logc.send(log_client.INFO,2,"WRITE_TO_HSM"+str(ticket))

	origin_addr = ticket['lm']['address']# who contacts me directly
    
	# First, call the user (to see if they are still there)
	sts = get_user_sockets( self, ticket )
	if sts == "error":
	    return_or_update_and_exit( self, origin_addr, e_errors.ENCP_GONE )
	    pass

	t0 = time.time()
	sts = bind_volume( self, ticket )
	ticket['times']['mount_time'] = time.time() - t0
	if sts != e_errors.OK:
	    # add CLOSING DATA SOCKET SO ENCP DOES NOT GET 'Broken pipe'
	    self.net_driver.data_socket.close()
	    Trace.trace( 17, 'bind problem in write' )
	    # make write specific and ...
	    sts = eval( "e_errors.WRITE_"+sts )
	    send_user_done( self, ticket, sts )
	    return_or_update_and_exit( self, origin_addr, sts )
	    pass
	    
	sts = vcc.set_writing( self.vol_info['external_label'] )
	if sts['status'][0] != "ok":
	    # add CLOSING DATA SOCKET SO ENCP DOES NOT GET 'Broken pipe'
	    self.net_driver.data_socket.close()
	    Trace.trace( 17, 'vcc.set_writing problem in write' )
	    send_user_done( self, ticket, e_errors.WRITE_NOTAPE )
	    return_or_update_and_exit( self, origin_addr, e_errors.WRITE_NOTAPE )
	    pass

        logc.send(log_client.INFO,2,"OPEN_FILE_WRITE")
        # open the hsm file for writing
        try:
	    # if forked, our eod info is not correct (after previous write)
	    # WE COULD SEND EOD STATUS BACK, then we could test/check our
	    # info against the vol_clerks
            do = self.hsm_driver.open( mvr_config['device'], 'a+' )
	    t0 = time.time()
	    do.seek( self.vol_info['eod_cookie'] )
	    self.vol_info['eod_cookie'] = do.cur_loc_cookie# vol_info may be 'none'
	    ticket['times']['seek_time'] = time.time() - t0

	    fast_write = 1

	    # create the wrapper instance (could be different for different
	    # tapes) so it can save data between pre and post
            wrapper = cpio.Cpio(  self.net_driver, self.hsm_driver, ECRC.ECRC
				, fast_write )

            logc.send(log_client.INFO,2,"WRAPPER.WRITE")
	    t0 = time.time()
	    wrapper.write_pre_data( do, ticket['wrapper'] )
	    Trace.trace( 11, 'done with pre_data' )

	    file_bytes = ticket['wrapper']['size_bytes']
	    san_bytes = ticket["wrapper"]["sanity_size"]
	    if file_bytes < san_bytes: san_bytes = file_bytes
	    san_crc = do.fd_xfer( self.net_driver.fileno(),
					       san_bytes, ECRC.ECRC, 0 )
	    Trace.trace( 11, 'done with sanity' )
	    sanity_cookie = (san_bytes, san_crc)
	    if file_bytes > san_bytes:
		file_crc = do.fd_xfer( self.net_driver.fileno(),
						    file_bytes - san_bytes,
						    ECRC.ECRC, san_crc )
	    else:
		file_crc = san_crc

	    Trace.trace( 11, 'done with rest' )
	    wrapper.write_post_data( do, file_crc )
	    Trace.trace( 11, 'done with post_data' )

	    do.writefm()
	    Trace.trace( 11, 'done with fm' )
            location_cookie = self.vol_info['eod_cookie']
	    eod_cookie = do.tell()
	    stats = self.hsm_driver.get_stats()
	    do.close()
	    ticket['times']['transfer_time'] = time.time() - t0

        #except EWHATEVER_NET_ERROR:
        except:
            logc.send( log_client.ERROR, 1, "Error writing "+str(ticket) )
	    traceback.print_exc()
            wr_err,rd_err,wr_access,rd_access = (1,0,1,0)
            vcc.update_counts( self.vol_info['external_label'],
                               wr_err, rd_err, wr_access, rd_access )
	    self.net_driver.data_socket.close()
            send_user_done( self, ticket, e_errors.WRITE_ERROR )
	    return_or_update_and_exit( self, origin_addr, e_errors.WRITE_ERROR )
	    pass

        # we've read the file from user, shut down data transfer socket
        self.net_driver.data_socket.close()
	Trace.trace( 11, 'close data' )

	# Tell volume server & update database
	remaining_bytes = stats['remaining_bytes']
	wr_err = stats['wr_err']
	rd_err = stats['rd_err']
	wr_access = stats['wr_access']
	rd_access = stats['rd_access']
	ticket['vc'].update( vcc.set_remaining_bytes(self.vol_info['external_label'],
						     remaining_bytes,
						     eod_cookie,
						     wr_err,rd_err, # added to total
						     wr_access,rd_access) )
	rsp = fcc.new_bit_file( {'work':"new_bit_file",
				 'fc'  :{'location_cookie':location_cookie,
					 'size':file_bytes,
					 'sanity_cookie':sanity_cookie,
					 'external_label':self.vol_info['external_label'],
					 'complete_crc':file_crc}} )
	if rsp['status'][0] != e_errors.OK:
	    logc.send( log_client.ERROR, 0,
		       "XXXXXXXXXXXenstore software error" )
	    pass
	ticket['fc'] = rsp['fc']
	ticket['mover'] = self.config
	ticket['mover']['callback_addr']        = self.callback_addr# this was the data callback
	
	logc.send(log_client.INFO,2,"WRITE"+str(ticket))
	
	Trace.trace( 11, 'b4 send_user_done' )
	send_user_done( self, ticket, e_errors.OK )

	return_or_update_and_exit( self, origin_addr, e_errors.OK )
	pass
    return {}

def forked_read_from_hsm( self, ticket ):
    # have to fork early b/c of early user (tcp) check
    # but how do I handle vol??? - prev_vol, this_vol???
    if mvr_config['do_fork']:
	self.pid = os.fork()
	self.state = 'busy'
	self.mode = 'r'			# client mode, not driver mode
    if mvr_config['do_fork'] and self.pid != 0:
        #self.net_driver.data_socket.close()# parent copy??? opened in get_user_sockets
	pass
    else:
	logc.send(log_client.INFO,2,"READ_FROM_HSM"+str(ticket))

	origin_addr = ticket['lm']['address']# who contacts me directly

	# First, call the user (to see if they are still there)
	sts = get_user_sockets( self, ticket )
	if sts == "error":
	    return_or_update_and_exit( self, origin_addr, e_errors.ENCP_GONE )
	    pass

	t0 = time.time()
	sts = bind_volume( self, ticket )
	ticket['times']['mount_time'] = time.time() - t0
	if sts != e_errors.OK:
	    # add CLOSING DATA SOCKET SO ENCP DOES NOT GET 'Broken pipe'
	    self.net_driver.data_socket.close()
	    Trace.trace( 17, 'bind problem in read' )
	    # make read specific and ...
	    sts = eval( "e_errors.READ_"+sts )
	    send_user_done( self, ticket, sts )
	    return_or_update_and_exit( self, origin_addr, sts )
	    pass

        # space to where the file will begin and save location
        # information for where future reads will have to space the drive to.

        # setup values before transfer
        media_error = 0
        drive_errors = 0
        bytes_sent = 0			# reset below, BUT not used afterwards!!!!!!!!!!!!!
        user_file_crc = 0		# reset below, BUT not used afterwards!!!!!!!!!!!!!

        # open the hsm file for reading and read it
        try:
            do = self.hsm_driver.open( mvr_config['device'], 'r' )

	    t0 = time.time()
	    do.seek( ticket['fc']['location_cookie'] )

	    ticket['times']['seek_time'] = time.time() - t0

	    # create the wrapper instance (could be different for different tapes)
	    wrapper = cpio.Cpio( self.hsm_driver, self.net_driver, ECRC.ECRC )

            logc.send(log_client.INFO,2,"WRAPPER.READ")
	    t0 = time.time()
            wrapper.read_pre_data( do, None )
            san_crc = do.fd_xfer( self.net_driver.data_socket.fileno(),
				  ticket['fc']['sanity_cookie'][0],
				  ECRC.ECRC,
				  0 )
	    if (ticket['fc']['size']-ticket['fc']['sanity_cookie'][0]) > 0:
		user_file_crc = do.fd_xfer( self.net_driver.data_socket.fileno(),
					    ticket['fc']['size']-ticket['fc']['sanity_cookie'][0],
					    ECRC.ECRC,
					    ticket['fc']['sanity_cookie'][1] )
	    else:
		user_file_crc = san_crc
		pass
	    tt = {'data_crc':ticket['fc']['complete_crc']}
            wrapper.read_post_data( do, tt )
	    ticket['times']['transfer_time'] = time.time() - t0

	    stats = self.hsm_driver.get_stats()
	    # close hsm file
            do.close()
	    wr_err,rd_err       = stats['wr_err'],stats['rd_err']
	    wr_access,rd_access = stats['wr_access'],stats['rd_access']
        except errno.errorcode[errno.EPIPE]: # do not know why I can not use just 'EPIPE'
            logc.send( log_client.ERROR, 1, "Error writing to user"+str(ticket) )
	    traceback.print_exc()
	    self.net_driver.data_socket.close()
	    send_user_done( self, ticket, e_errors.READ_ERROR )
	    return_or_update_and_exit( self, origin_addr, e_errors.OK )
        except:
            logc.send( log_client.ERROR, 1, "Error reading "+str(ticket) )
	    traceback.print_exc()
            media_error = 1 # I don't know what else to do right now
	    # this is bogus right now
            wr_err,rd_err,wr_access,rd_access = (0,1,0,1)

        # we've sent the hsm file to the user, shut down data transfer socket
        self.net_driver.data_socket.close()

        # get the error/mount counts and update database
        xx = vcc.update_counts( self.vol_info['external_label'],
				wr_err,rd_err,wr_access,rd_access )
	self.vol_info.update( xx )

        # if media error, mark volume readonly, unbind it & tell user to retry
        if media_error :
            vcc.set_system_readonly( self.vol_info['external_label'] )
            send_user_done( self, ticket, e_errors.READ_ERROR )
	    return_or_update_and_exit( self, origin_addr, e_errors.READ_ERROR )

        # drive errors are bad:  unbind volule it & tell user to retry
        elif drive_errors :
            vcc.set_hung( self.vol_info['external_label'] )
            send_user_done( self, ticket, e_errors.READ_ERROR )
	    return_or_update_and_exit( self, origin_addr, e_errors.READ_ERROR )

        # All is well - read has finished correctly

        # add some info to user's ticket
        ticket['vc'] = self.vol_info
	ticket['mover'] = self.config
	ticket['mover']['callback_addr'] = self.callback_addr# this was the data callback

        logc.send(log_client.INFO,2,"READ DONE"+str(ticket))

	send_user_done( self, ticket, e_errors.OK )
	return_or_update_and_exit( self, origin_addr, e_errors.OK )
	pass
    return

def return_or_update_and_exit( self, origin_addr, status ):
    if mvr_config['do_fork']:
	# need to send info to update parent: vol_info and
	# hsm_driver_info (read: hsm_driver.position
	#                  write: position, eod, remaining_bytes)
	# recent (read) errors (part of vol_info???)
	udpc.send_no_wait( {'work':"update_client_info",
			    'address':origin_addr,
			    'pid':os.getpid(),
			    'hsm_driver':{'blocksize':self.hsm_driver.blocksize,
					  'remaining_bytes':self.hsm_driver.remaining_bytes,
					  'vol_label':self.hsm_driver.vol_label,
					  'cur_loc_cookie':self.hsm_driver.cur_loc_cookie,
					  'no_xfers':self.hsm_driver.no_xfers},
			    'vol_info':self.vol_info},
			   (self.config['hostip'],self.config['port']) )
	sys.exit( m_err.index(status) )
    else:
	return status_to_request( self, status )

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
		      'external_label' : self.fc['external_label'],
		      'status'         : (error_info,None)}
    return next_req_to_lm


class Mover:
    # Note: the self.data_socket member/object gets created/initialized
    #       by get_user_sockets

    # Read a block from the network (from the user).  This method is call
    # from the wrapper object when writing to the HSM
    def read_block( self ):
        badsock = self.data_socket.getsockopt(socket.SOL_SOCKET,
                                              socket.SO_ERROR)
        if badsock != 0:
            self.enprint("Mover read_block, pre-recv error: "+ \
                         repr(errno.errorcode[badsock]))
	    raise errno.errorcode[badsock]
	block = self.data_socket.recv( self.blocksize )
        badsock = self.data_socket.getsockopt(socket.SOL_SOCKET,
                                              socket.SO_ERROR)
        if badsock != 0:
            self.enprint("Mover read_block, post-recv error: "+ \
                         repr(errno.errorcode[badsock]))
	    raise errno.errorcode[badsock]
	return block

    # write a block to the network (to the user).  This method is call
    # from the wrapper object when reading from the HSM
    def write_block( self, buff ):
        badsock = self.data_socket.getsockopt(socket.SOL_SOCKET,
                                              socket.SO_ERROR)
        if badsock != 0:
            self.enprint("Mover write_block, pre-send error: "+\
	     	         repr(errno.errorcode[badsock]))
	    raise errno.errorcode[badsock]
	count = self.data_socket.send(buff)
        badsock = self.data_socket.getsockopt(socket.SOL_SOCKET,
                                              socket.SO_ERROR)
        if badsock != 0:
            self.enprint("Mover write_block, post-send error: "+ \
                         repr(errno.errorcode[badsock]))
	    raise errno.errorcode[badsock]
        return count

    def fileno( self ):
	return self.data_socket.fileno()

    pass


# Gather everything together and add to the mess
class MoverServer(  dispatching_worker.DispatchingWorker
	     	  , generic_server.GenericServer ):
    def __init__( self, server_address, verbose=0 ):
	self.client_obj_inst = MoverClient( mvr_config )
	self.verbose = verbose
	for lm in mvr_config['library']:# should be libraries
	    # a "respone" to server being summoned
	    address = (libm_config_dict[lm]['hostip'],libm_config_dict[lm]['port'])
	    next_req_to_lm = idle_mover_next( self.client_obj_inst )
	    do_next_req_to_lm( self, next_req_to_lm, address )
	    pass
	dispatching_worker.DispatchingWorker.__init__( self, server_address)
	return

    def set_timeout( self, ticket ):
	out_ticket = {'status':'ok','old timeout':self.rcv_timeout}
	out_ticket['extra status'] = 'changed'
	try:    self.rcv_timeout = ticket['timeout']
	except: out_ticket['extra status'] = 'not changed'
	out_ticket['new timeout'] = self.rcv_timeout
	self.reply_to_caller( out_ticket )
	return

    def status( self, ticket ):
	out_ticket = {'status':'ok'}
	out_ticket['state']    = self.client_obj_inst.state
	out_ticket['mode']     = self.client_obj_inst.mode
	out_ticket['no_xfers'] = self.client_obj_inst.hsm_driver.no_xfers
	out_ticket['rd_bytes'] = self.client_obj_inst.hsm_driver.rd_bytes_get()
	out_ticket['wr_bytes'] = self.client_obj_inst.hsm_driver.wr_bytes_get()
	self.reply_to_caller( out_ticket )
	return

    def update_client_info( self, ticket ):
	self.client_obj_inst.vol_info = ticket['vol_info']
	self.client_obj_inst.hsm_driver.blocksize = \
					ticket['hsm_driver']['blocksize']
	self.client_obj_inst.hsm_driver.remaining_bytes = \
					ticket['hsm_driver']['remaining_bytes']
	self.client_obj_inst.hsm_driver.vol_label = \
					ticket['hsm_driver']['vol_label']
	self.client_obj_inst.hsm_driver.cur_loc_cookie = \
					ticket['hsm_driver']['cur_loc_cookie']
	self.client_obj_inst.hsm_driver.no_xfers = \
					ticket['hsm_driver']['no_xfers']
	wait = 0
	next_req_to_lm = get_state_build_next_lm_req( self, wait )
	do_next_req_to_lm( self, next_req_to_lm, ticket['address'] )
	return

    def summon( self, ticket ):
	wait=posix.WNOHANG
	next_req_to_lm = get_state_build_next_lm_req( self, wait )
	do_next_req_to_lm( self, next_req_to_lm, ticket['address'] )
	return

    def handle_timeout( self ):
        return

    pass

def do_next_req_to_lm( self, next_req_to_lm, address ):
    while next_req_to_lm != {} and next_req_to_lm != None:
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
	# Exceptions are caught (except block) in dispatching_worker.py.
	# The reply is the command (i.e the network is the computer).
	client_function = rsp_ticket['work']
	method = MoverClient.__dict__[client_function]
	next_req_to_lm = method( self.client_obj_inst, rsp_ticket )
	pass
    return

def get_state_build_next_lm_req( self, wait ):
    if self.client_obj_inst.pid:
	pid, status = posix.waitpid( self.client_obj_inst.pid, wait )
	if pid == self.client_obj_inst.pid:
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
				e_errors.READ_TAPEBUSY]:
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
	next_req_to_lm = freeze_tape_in_drive( client_obj_inst, m_err[exit_status] )
    else:
	# new error
	logc.send( log_client.ERROR, 0, "FATAL ERROR - MOVER - unknown transfer status - fix me now" )
	while 1: time.sleep( 100 )		# NEVER RETURN!?!?!?!?!?!?!?!?!?!?!?!?
	pass
    return next_req_to_lm

class MoverInterface(interface.Interface):

    def __init__(self):
        Trace.trace(10,'{lmsi.__init__')
        # fill in the defaults for possible options
        self.summon = 1
        self.verbose = 0
        interface.Interface.__init__(self)

        # now parse the options
        self.parse_options()
        Trace.trace(10,'}lmsi.__init__')

    # define the command line options that are valid
    def options(self):
        Trace.trace(16, "{}options")
        return self.config_options()+\
               self.help_options()

    #  define our specific help
    def help_line(self):
        return interface.Interface.help_line(self)+" mover_device"

    # parse the options like normal but make sure we have a mover
    def parse_options(self):
        interface.Interface.parse_options(self)
        # bomb out if we don't have a mover
        if len(self.args) < 1 :
            self.print_help(),
            sys.exit(1)
        else:
            self.name = self.args[0]

#############################################################################

#############################################################################
import sys				# sys.argv[1:]
import os				# os.environ
import socket                           # gethostname (local host)
import string				# atoi
import types				# see if library config is list
import configuration_client
import udp_client
import log_client

# get an interface, and parse the user input
intf = MoverInterface()

# get clients -- these will be (readonly) global object instances
csc  = configuration_client.ConfigurationClient( intf.config_host, 
                                                 intf.config_port, 0 )
udpc =          udp_client.UDPClient()	# for server to send (client) request
logc =          log_client.LoggerClient( csc, 'MOVER', 'logserver', 0 )
fcc  = file_clerk_client.FileClient( csc )
vcc  = volume_clerk_client.VolumeClerkClient( csc )

# get my (localhost) configuration from the configuration server
mvr_config = csc.get( intf.name )
if mvr_config['status'][0] != "ok":
    raise "could not start mover",intf.name," up:" + mvr_config['status']
# clean up the mvr_config a bit
mvr_config['name'] = intf.name
mvr_config['config_host'] = intf.config_host
mvr_config['config_port'] = intf.config_port
del intf
mvr_config['do_fork'] = 1
if not 'do_eject' in mvr_config.keys(): mvr_config['do_eject'] = 'yes'
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
    mvr_config['library'] = [lib]	# make it a list
    libm_config_dict[lib] = {'startup_polled':'not_yet'}
    libm_config_dict[lib].update( csc.get_uncached(lib) )
    pass


mvr_srvr =  MoverServer( (mvr_config['hostip'],mvr_config['port']) )
mvr_srvr.rcv_timeout = 15
try:
    mvr_srvr.client_obj_inst.print_id = mvr_config['logname']
except:
    pass

Trace.init( mvr_config['logname'] )
Trace.on( mvr_config['logname'], 0, 31 )
mvr_srvr.serve_forever()

generic_cs.enprint("ERROR?")
