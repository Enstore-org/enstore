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
	o   type of volume - cpio, ansi, ...
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
# Note: pass statement used to help emacs formatting.
#
#       vcc stands for Volume Clerk Client
#       fcc stands for File Clerk Client
#       mcc stands for Media Changer Client
#
#    some functions ("methods") are defined outside of classes to not allow
#    method invocation via dispatching_worker.py (see dispatching_worker.py).
#


# python modules
import errno
import os				# os.environ, os.system, and possible os.error (from os.waitpid)
import pprint
import signal				# signal - to del shm on sigterm, etc
import sys				# exit
import time				# .sleep
import traceback			# print_exc == stack dump
import string				# find
import select

# enstore modules
import generic_server
import interface
import dispatching_worker
import volume_clerk_client		# -.
import file_clerk_client		#   >-- 3 significant clients
import media_changer_client		# -'
import callback				# used in send_user_done, get_usr_driver
import wrapper_selector
import Trace
import driver
import FTT				# needed for FTT.error
import EXfer				# needed for EXfer.error
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
	  e_errors.UNMOUNT,
	  e_errors.ENCP_GONE,
	  e_errors.TCP_HUNG,
          e_errors.READ_VOL1_READ_ERR,
          e_errors.WRITE_VOL1_READ_ERR,
          e_errors.READ_VOL1_MISSING,
          e_errors.WRITE_VOL1_MISSING,
          e_errors.READ_VOL1_WRONG,
          e_errors.WRITE_VOL1_WRONG,
	  e_errors.MOVER_CRASH ]	# obviously can not handle this one

forked_state = [ 'forked',
		 'encp check',
		 'bind',
		 'wrapper, pre',
		 'data',
		 'wrapper, post',
		 'send_user_done' ]
	   

#setting this to 1 turns on printouts related to "paranoid"
# checking of VOL1 and EOV1 headers.
#once this is all working, the printout code should be stripped out
debug_paranoia=0
vol1_paranoia=1 #check VOL1 headers (robot grabbed wrong tape)
eov1_paranoia=0 #write and check EOV1 headers (spacing error)
### eov1 checks are still a "work in progress", do not enable this!


def sigterm( sig, stack ):
    print '%d sigterm called'%os.getpid()
    if mvr_srvr.client_obj_inst.pid:
	print 'attempt kill of mover subprocess', mvr_srvr.client_obj_inst.pid
	os.kill( mvr_srvr.client_obj_inst.pid, signal.SIGTERM )
	pass

    # ONLY DELETE AFTER FORKED PROCESS IS KILL
    # must just try: b/c may get "AttributeError: hsm_driver" which causes
    # forked process to become server via dispatching working exception handling
    try: del mvr_srvr.client_obj_inst.hsm_driver.shm
    except: pass			# wacky things can happen with forking
    sem = 0; msg = 0
    try: sem = mvr_srvr.client_obj_inst.hsm_driver.sem
    except: pass
    try: msg = mvr_srvr.client_obj_inst.hsm_driver.msg
    except: pass
    print 'deleting sem (id=%d) and msg (id=%d)'%(sem,msg)
    try: del mvr_srvr.client_obj_inst.hsm_driver.sem; print '%d deleted sem'%os.getpid()
    except: pass			# wacky things can happen with forking
    try: del mvr_srvr.client_obj_inst.hsm_driver.msg; print '%d deleted msg'%os.getpid()
    except: pass			# wacky things can happen with forking
    #print '%d sigterm exiting'%os.getpid()
    sys.exit( 0 )   # anything other than 0 causes traceback

    # sys.exit( 0x80 | sig ) # this is the way to indicate exit b/c of a signal
    return None

def sigint( sig, stack ):
    del mvr_srvr.client_obj_inst.hsm_driver
    print 'Traceback (innermost last):'
    traceback.print_stack( stack )
    print 'KeyboardInterrupt'
    sys.exit( 1 )
    return None
    
def sigsegv( sig, stack ):
    if mvr_srvr.client_obj_inst.pid:
	os.kill( mvr_srvr.client_obj_inst.pid, signal.SIGTERM )
	time.sleep(3)
	os.waitpid( mvr_srvr.client_obj_inst.pid, os.WNOHANG )
	pass
    # kill just shm to avoid "AttributeError: hsm_driver" which causes
    # forked process to become server via dispatching working exception handling
    try: del mvr_srvr.client_obj_inst.hsm_driver.shm
    except: pass			# wacky things can happen with forking
    try: del mvr_srvr.client_obj_inst.hsm_driver.sem
    except: pass			# wacky things can happen with forking
    try: del mvr_srvr.client_obj_inst.hsm_driver.msg
    except: pass			# wacky things can happen with forking

    if mvr_srvr.client_obj_inst.pid:
	mvr_srvr.client_obj_inst.usr_driver.close()
    else:
	sys.exit( 0x80 | sig )
    return None

def sigstop( sig, stack ):
    return None
    

# The following functions are the result of the enstore error documentation...
# know it, live it, love it.
def freeze_tape( self, error_info ):# DO NOT UNLOAD TAPE  (BUT --
    # LIBRARY MANAGER CAN RSP UNBIND)
    vcc.set_system_noaccess( self.vol_info['err_external_label'] )
    Trace.log( e_errors.ERROR,
               'MOVER SETTING VOLUME "SYSTEM NOACCESS" %s %s'%(error_info,
							       self.vol_info) )
    return unilateral_unbind_next( self, error_info )

def freeze_tape_in_drive( self, error_info ):
    vcc.set_system_noaccess( self.vol_info['err_external_label'] )
    Trace.log( e_errors.ERROR,
               'MOVER SETTING VOLUME "SYSTEM NOACCESS" %s %s'%(error_info,
							       self.vol_info) )
    return offline_drive( self, error_info )

def offline_drive( self, error_info ):	# call directly for READ_ERROR
    Trace.log( e_errors.ERROR, "MOVER OFFLINE "+str(error_info) )
    self.state = 'offline'
    return unilateral_unbind_next( self, error_info )


def fatal_enstore( self, error_info ):
    Trace.log( e_errors.ERROR, "FATAL ERROR - MOVER - "+str(error_info) )
    ticket = {'work'          :"unilateral_unbind",
	      'external_label':self.vol_info['external_label'],
	      'status'        :error_info}
    rsp = udpc.send( ticket, self.lm_origin_addr )
    # Enstore design issue... it has not yet been decided what to do; so for
    # now I just...   #XXX
    while 1: time.sleep( 100 )	 # the Inquisitor will restart us
    return

# MUST SEPARATE SYSTEM AND USER FUNCTIONS - I.E. ERROR MIGHT BE USERGONE
# send a message to our user (last means "done with the read or write")
def send_user_done( self, ticket, error_info ):
    self.hsm_driver.user_state_set( forked_state.index('send_user_done') )
    ticket['status'] = (error_info,None)
    callback.write_tcp_obj( self.control_socket, ticket)
    self.control_socket.close()
    return

#########################################################################
# The next set of methods are ones that will be invoked by calling
# the MoverClient dictionary element specified by the 'work' key of the
# ticket received.
#
class MoverClient:
    def __init__( self, csc ):
        self.csc = csc
	self.state = 'idle'
	# needs to be initialized for status
	self.mode = ''			# will be either 'r' or 'w'
	self.bytes_to_xfer = 0
	self.tape = '' # like vol_info['external_label'], just for "status"
	self.files = ('','')
	self.work_ticket = {}
	self.hsm_drive_sn = ''
	self.no_xfers = 0

	self.pid = 0

        # need a media changer to control (mount/load...) the volume
        self.mcc = media_changer_client.MediaChangerClient( self.csc,
                                             mvr_config['media_changer'] )

	self.vol_info = {'external_label':'', 'media_type':''}
	self.vol_vcc = {}		# vcc associated with a particular
	# vol_label (useful when other lib man summons during delayed
	# dismount -- labels must be unique

	self.read_error = [0,0]		# error this vol ([0]) and last vol ([1])
	self.crc_flag = 1
	self.local_mover_enable = 0

        mvr_config['device'] = os.path.expandvars( mvr_config['device'] )

	# next line of code is equivalent to:
	# eval( "driver.%s()"%mvr_config['driver'] )  (we do not like to use eval)

        try: self.hsm_driver = getattr(driver, mvr_config['driver']) ()
        except AttributeError:
            Trace.log(e_errors.INFO, "No such driver: "+mvr_config['driver'])
            raise

        driver_object = self.hsm_driver.open( mvr_config['device'], 'a+' )
        ss = driver_object.get_stats()
        if ss['serial_num'] != None: self.hsm_drive_sn = ss['serial_num']

        # check for tape in drive
        # if no vol one labels, I can only eject. -- tape maybe left in bad
        # state.
        if mvr_config['do_eject'] == 'yes':
            self.hsm_driver.offline( mvr_config['device'] )
            # tell media changer to unload the vol BUT I DO NOT KNOW THE VOL
            #mcc.unloadvol( self.vol_info, mvr_config['mc_device'] )
            self.mcc.unloadvol( self.vol_info, mvr_config['name'], 
                mvr_config['mc_device'], None)
            pass
        driver_object.close()

	signal.signal( signal.SIGTERM, sigterm )# to allow cleanup of shm
	signal.signal( signal.SIGINT, sigint )# to allow cleanup of shm
	signal.signal( signal.SIGSEGV, sigsegv )# scsi reset???
	return None

    def nowork( self, ticket ):
	# nowork is no work
	return {}

    def unbind_volume( self, ticket ):
        #if last action was write, and the feature is enabled, write an EOV label before
        #unloading tape.  
        if eov1_paranoia and self.mode=='w' and mvr_config['driver']!='NullDriver':
            driver_object = self.hsm_driver.open( mvr_config['device'], 'a+')
            eod_cookie=self.vol_info['eod_cookie']
            external_label=self.vol_info['external_label']
            ll = driver_object.format_eov1_header( external_label, eod_cookie)
            if debug_paranoia: print "writing EOV1 label", ll
	    driver_object.write( ll )
	    driver_object.writefm()
            driver_object.seek(eod_cookie)
	if self.vol_info['external_label'] == '': return idle_mover_next( self )
	if mvr_config['do_fork']:
            do_fork( self, ticket, 'u' )
            if self.pid != 0: return {} #parent
            pass

	# child or single process???
	Trace.log(e_errors.INFO,'UNBIND start'+str(ticket))
	    
	# do any driver level rewind, unload, eject operations on the device
	if mvr_config['do_eject'] == 'yes':
	    Trace.log( e_errors.INFO, "Performing offline/eject of device %s"%mvr_config['device'] )
	    self.hsm_driver.offline(mvr_config['device'])
	    Trace.log( e_errors.INFO, "Completed  offline/eject of device %s"%mvr_config['device'] )
	    pass
	# now ask the media changer to unload the volume
	Trace.log(e_errors.INFO,"Requesting media changer unload")
	rr = self.mcc.unloadvol( self.vol_info, mvr_config['name'], 
                                 mvr_config['mc_device'],
                                self.vol_vcc[self.vol_info['external_label']] )
	Trace.log(e_errors.INFO,"Media changer unload status"+str(rr['status']))
	if rr['status'][0] != "ok":
	    return_or_update_and_exit( self, self.vol_info['from_lm'], e_errors.UNMOUNT )
	    pass

	del self.vol_vcc[self.vol_info['external_label']]
	self.vol_info['external_label'] = ''
	
	return_or_update_and_exit( self, self.vol_info['from_lm'], e_errors.OK )
	pass

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
def bind_volume( object, external_label ):
    #
    # NOTE: external_label is in rsponses from both the vc and fc
    #       for a write:
    #          encp contacts lm which contacts vc and gets external_label
    #          lm puts vc-external_label in fc "sub-ticket"
    #       for a read:
    #          encp contacts fc 1st with bfid
    #          fc uses bfid to get external_label from its database
    #          fc uses external_label to ask vc which library
    #          fc could pass vc info, but it does not as some of the info could
    #                                           get stale
    #          then fc adds it's fc-info to encp ticket and sends it to lm
    
    object.hsm_driver.user_state_set( forked_state.index('bind') )

    if object.vol_info['external_label'] == '':

	# NEW VOLUME FOR ME - find out what volume clerk knows about it
	object.vol_info['err_external_label'] = external_label
	tmp_vol_info = vcc.inquire_vol( external_label )
	if tmp_vol_info['status'][0] != 'ok': return 'NOTAPE' # generic, not read or write specific

        # if there is a tape in the drive, eject it (then the robot can put it away and we can continue)
	# NOTE: can we detect "cleaning in progress" or "cleaning cartridge (as
	# opposed to data cartridge) in drive?"
	# If we can detect "cleaning in progress" we can wait for it to
	# complete before ejecting.
	if mvr_config['do_eject'] == 'yes':
            Trace.log(e_errors.INFO,'Performing precautionary offline/eject of device'+str(mvr_config['device']))
	    object.hsm_driver.offline(mvr_config['device'])
            Trace.log(e_errors.INFO,'Completed  precautionary offline/eject of device'+str(mvr_config['device']))
            pass

	object.vol_info['read_errors_this_mover'] = 0
        tmp_mc = ", "+str({"media_changer":mvr_config['media_changer']})
        Trace.log(e_errors.INFO,Trace.MSG_MC_LOAD_REQ+'Requesting media changer load '+str(tmp_vol_info)+
                  " "+tmp_mc+' '+str(mvr_config['mc_device']))
	try: rsp = object.mcc.loadvol( tmp_vol_info, mvr_config['name'],
				mvr_config['mc_device'], vcc )
	except errno.errorcode[errno.ETIMEDOUT]:
            rsp = { 'status':('ETIMEDOUT',None) }
	Trace.log(e_errors.INFO,Trace.MSG_MC_LOAD_DONE+'Media changer load status '+
                  str(rsp['status'])+" "+tmp_mc)
	if rsp['status'][0] != 'ok':
	    # it is possible, under normal conditions, for the system to be
	    # in the following race condition:
	    #   library manager told another mover to unbind.
	    #   other mover responds promptly,
	    #   more work for library manager arrives and is given to a
	    #     new mover before the old volume was given back to the library
	    # SHOULD I RETRY????????
	    if rsp['status'][0] == 'media_in_another_device':
		time.sleep (10)
		return 'TAPEBUSY' # generic, not read or write specific
	    else: return 'BADMOUNT'
	try:
            Trace.log(e_errors.INFO,'Requesting software mount '+str(external_label)+' '+str(mvr_config['device']))
	    object.hsm_driver.sw_mount( mvr_config['device'],
				      tmp_vol_info['blocksize'],
				      tmp_vol_info['remaining_bytes'],
				      external_label,
				      tmp_vol_info['eod_cookie'] )
            Trace.log(e_errors.INFO,'Software mount complete '+str(external_label)+' '+str(mvr_config['device']))
	except: return 'BADMOUNT' # generic, not read or write specific

        #Paranoia:  We may have the wrong tape.  Check the VOL1 header!
        if vol1_paranoia and mvr_config['driver']!='NullDriver':
            driver_object = object.hsm_driver.open( mvr_config['device'], 'r' )
            if debug_paranoia: print "Rewinding (pre check-label)"
            r=driver_object.rewind()
            if debug_paranoia:
                print "pre check-label rewind complete, returned",r
            header_type, header_label = driver_object.check_header()
            if debug_paranoia: print header_type, header_label
            if header_type == None:  #This only happens if
                                     ##there was a read error
                return 'VOL1_READ_ERR'
            elif header_type == 'VOL1':
                if header_label != external_label:
                    vcc.set_system_noaccess( external_label )
                    if debug_paranoia:
                        print "wrong VOL1 header: got %s, expected %s" % (
                            header_label, external_label)
                    ##XXX alarm ???
                    return 'VOL1_WRONG'
            else:
                if not driver_object.is_bot(tmp_vol_info['eod_cookie']):
                    if debug_paranoia: print tmp_vol_info['eod_cookie']
                    ## This tape really should have been labelled!
                    if debug_paranoia: print "Tape should have been labeled"
                    vcc.set_system_noaccess( external_label )
                    if debug_paranoia:
                        print "no VOL1 header present for volume %s" %\
                              external_label
                    ##XXX alarm/log?
                    return 'VOL1_MISSING' 
        
            if debug_paranoia: print "Rewind (post check-label)"
            # Note:  Closing the device seems to write a
            #file mark (even though it was opened "r"!),
            # so we better close *before* rewinding.
            driver_object.close()
            driver_object = object.hsm_driver.open( mvr_config['device'], 'a+')
            r=driver_object.rewind()
            if debug_paranoia:
                print "post check-label rewind complete, returned",r
            if debug_paranoia: print "tell", driver_object.tell()
        ##end of paranoid checks    
        else:
            driver_object = object.hsm_driver.open( mvr_config['device'], 'a+')
            
        if (driver_object.is_bot(driver_object.tell()) and
            driver_object.is_bot(tmp_vol_info['eod_cookie'])):
	    # write an ANSI label and update the eod_cookie
	    ll = driver_object.format_vol1_header( external_label )
            if debug_paranoia: print "writing VOL1 label", ll
	    driver_object.write( ll )
	    driver_object.writefm()
            eod_cookie = driver_object.tell()
	    tmp_vol_info['eod_cookie'] = eod_cookie
	    tmp_vol_info['remaining_bytes'] = driver_object.get_stats()['remaining_bytes']
	    vcc.set_remaining_bytes( external_label,
				     tmp_vol_info['remaining_bytes'],
				     tmp_vol_info['eod_cookie'],
				     0,0,0,0,None )
	    Trace.trace( 18, 'wrote label, new eod/remaining_byes = %s/%s'%
			 (tmp_vol_info['eod_cookie'],
			  tmp_vol_info['remaining_bytes']) )
            if eov1_paranoia and mvr_config['driver']!='NullDriver':
                ll = driver_object.format_eov1_header  (
                    external_label, eod_cookie)
                if debug_paranoia: print "writing EOV1 label", ll
                driver_object.write( ll )
                driver_object.writefm()
                driver_object.skip_fm(-1)
	    pass
	driver_object.close()
	object.vol_info.update( tmp_vol_info )
	pass
    elif external_label != object.vol_info['external_label']:
        object.vol_info['err_external_label'] = external_label
        fatal_enstore( object,"unbind label %s before read/write label %s"%
                       (object.vol_info['external_label'],external_label) )
        return 'NOTAPE' # generic, not read or write specific

    return e_errors.OK  # bind_volume


def do_fork( self, ticket, mode ):
    global vcc, fcc
    ticket['mover'] = mvr_config
    if mode == 'w' or mode == 'r':
	# get vcc and fcc for this xfer
	fcc = file_clerk_client.FileClient( self.csc, 0,
					    ticket['fc']['address'] )
	vcc = volume_clerk_client.VolumeClerkClient( self.csc,
						     ticket['vc']['address'] )
	self.vol_vcc[ticket['fc']['external_label']] = vcc# remember for unbind
	ticket['mover']['local_mover'] = 0  # potentially changed in get_usr_driver
	self.hsm_driver._bytes_clear()
	self.prev_r_bytes = 0; self.prev_w_bytes = 0; self.init_stall_time = 1
	self.bytes_to_xfer = ticket['fc']['size']
	# save some stuff for "status"
	self.tape = ticket['fc']['external_label']
	self.files = ("%s:%s"%(ticket['wrapper']['machine'][1],
			       ticket['wrapper']['fullname']),
		      ticket['wrapper']['pnfsFilename'])
        self.work_ticket = ticket	#just save the whole thing for "status"
	pass
    self.state = 'busy'
    self.mode = mode			# client mode, not driver mode

    self.pid = os.fork()
    if self.pid == 0:
	Trace.init( self.log_name ) # update trc_pid
	# do in child only
	self.hsm_driver.user_state_set( forked_state.index('forked') )
	pass
    return None

def forked_write_to_hsm( self, ticket ):
    # have to fork early b/c of early user (tcp) check
    # but how do I handle vol??? - prev_vol, this_vol???
    check_eov=0
    if eov1_paranoia and self.mode!='w' and mvr_config['driver']!='NullDriver':
        check_eov=1
    if mvr_config['do_fork']: do_fork( self, ticket, 'w' )
    
    if mvr_config['do_fork'] and self.pid != 0:
	# parent
	pass
    else:
	# child or single process???
	Trace.log(e_errors.INFO,'WRITE_TO_HSM start'+str(ticket))

	self.lm_origin_addr = ticket['lm']['address']# who contacts me directly
    
	# First, call the user (to see if they are still there)
	sts = get_usr_driver( self, ticket )
	if sts == 'error':
	    return_or_update_and_exit( self, self.lm_origin_addr, e_errors.ENCP_GONE )
	    pass

	t0 = time.time()
	sts = bind_volume( self, ticket['fc']['external_label'] )
	ticket['times']['mount_time'] = time.time() - t0
	if sts != e_errors.OK:
	    # add CLOSING DATA SOCKET SO ENCP DOES NOT GET 'Broken pipe'
	    self.usr_driver.close()
	    # make write specific and ...
	    sts = getattr(e_errors,"WRITE_"+sts)
	    send_user_done( self, ticket, sts )
	    return_or_update_and_exit( self, self.lm_origin_addr, sts )
	    pass
	self.vol_info['from_lm'] = ticket['lm']['address'] # who gave me this volume
	    
	sts = vcc.set_writing( self.vol_info['external_label'] )
	if sts['status'][0] != "ok":
	    # add CLOSING DATA SOCKET SO ENCP DOES NOT GET 'Broken pipe'
	    self.usr_driver.close()
	    send_user_done( self, ticket, e_errors.WRITE_NOTAPE )
	    return_or_update_and_exit( self, self.lm_origin_addr, e_errors.WRITE_NOTAPE )
	    pass

        Trace.log(e_errors.INFO,"OPEN_FILE_WRITE "+str(self.vol_info['external_label'])+":"+str(self.vol_info['eod_cookie']) )
        # open the hsm file for writing
        try:
	    # if forked, our eod info is not correct (after previous write)
	    # WE COULD SEND EOD STATUS BACK, then we could test/check our
	    # info against the vol_clerks
            driver_object = self.hsm_driver.open( mvr_config['device'], 'a+' )
	    self.no_xfers = self.no_xfers + 1
	    t0 = time.time()
	    # vol_info may be 'none' - seek can handle that and
	    # tell will convert it.
	    driver_object.seek( self.vol_info['eod_cookie'] )
	    self.vol_info['eod_cookie'] = driver_object.tell()
            if check_eov:
                if debug_paranoia: print "checking EOV label"
                header_type, header_label=driver_object.check_header()
                if debug_paranoia: print header_type, header_label
                if header_type != "EOV1":
                    #XXX Error!!
                    print "bad EOV1 %s %s"%(header_type, header_label)
                driver_object.skip_fm(-1)
                              
                
	    ticket['times']['seek_time'] = time.time() - t0

	    fast_write = 1

	    # create the wrapper instance (could be different for different
	    # tapes) so it can save data between pre and post
            wrapper=wrapper_selector.select_wrapper(ticket['wrapper']['type'])
	    if wrapper == None:
		raise errno.errorcode[errno.EINVAL], "Invalid wrapper"+\
		      str(ticket['wrapper']['type'])

            Trace.log(e_errors.INFO,"WRAPPER.WRITE "+str(self.vol_info['external_label']))
	    t0 = time.time()
	    self.hsm_driver.user_state_set( forked_state.index('wrapper, pre') )
	    wrapper.blocksize = self.vol_info['blocksize']
	    wrapper.write_pre_data( driver_object, ticket['wrapper'] )
	    Trace.trace( 11, 'done with pre_data' )

	    # should not be getting this from wrapper sub-ticket
	    file_bytes = ticket['wrapper']['size_bytes']
	    san_bytes = ticket['wrapper']['sanity_size']
	    if file_bytes < san_bytes: san_bytes = file_bytes
	    self.hsm_driver.user_state_set( forked_state.index('data') )
	    san_crc = driver_object.fd_xfer( self.usr_driver.fileno(),
				  san_bytes, self.crc_flag, 0 )
	    Trace.trace( 11, 'done with sanity' )
	    sanity_cookie = (san_bytes, san_crc)
	    if file_bytes > san_bytes:
		file_crc = driver_object.fd_xfer( self.usr_driver.fileno(),
				       file_bytes-san_bytes, self.crc_flag,
				       san_crc )
	    else: file_crc = san_crc

	    Trace.log(e_errors.INFO,'done with write fd_xfers')
	    
	    Trace.trace( 11, 'done with rest of data' )
	    self.hsm_driver.user_state_set( forked_state.index('wrapper, post') )
	    wrapper.write_post_data( driver_object, file_crc )
	    Trace.trace( 11, 'done with post_data' )
	    ticket['times']['transfer_time'] = time.time() - t0
	    t0 = time.time()
	    driver_object.writefm()
	    ticket['times']['eof_time'] = time.time() - t0
	    Trace.trace( 11, 'done with fm' )

            location_cookie = self.vol_info['eod_cookie']
	    eod_cookie = driver_object.tell()
	    if driver_object.loc_compare(eod_cookie,location_cookie) != 1: raise "bad eod"
	    t0 = time.time()
	    stats = driver_object.get_stats()
	    ticket['times']['get_stats_time'] = time.time() - t0
	    driver_object.close()			# b/c of fm above, this is purely sw.

        #except EWHATEVER_NET_ERROR:
	except (FTT.error, EXfer.error), err_msg:
            Trace.log( e_errors.ERROR,
		       'FTT or Exfer exception: '+str(sys.exc_info()[0])+str(sys.exc_info()[1]) )
	    #traceback.print_exc()
	    #print 'type of err_msg is',type(err_msg),'type of sys.exc_info()[1]) is',type(sys.exc_info()[1])
	    # err_msg is <type 'instance'>
	    if err_msg.args[0] == 'fd_xfer - read EOF unexpected':
		# assume encp dissappeared
		return_or_update_and_exit( self, self.lm_origin_addr,
					   e_errors.ENCP_GONE )
		pass
	    else:
		wr_err,rd_err,wr_access,rd_access = (1,0,1,0)
		vcc.update_counts( self.vol_info['external_label'],
				   wr_err, rd_err, wr_access, rd_access )
		self.usr_driver.close()
		send_user_done( self, ticket, e_errors.WRITE_ERROR )
		return_or_update_and_exit( self, self.lm_origin_addr,
					   e_errors.WRITE_ERROR )
		pass
	    pass
	except "seek error":
	    Trace.log( e_errors.ERROR, "seek error during write" )
            wr_err,rd_err,wr_access,rd_access = (1,0,1,0)
            vcc.update_counts( self.vol_info['external_label'],
                               wr_err, rd_err, wr_access, rd_access )
	    self.usr_driver.close()
            send_user_done( self, ticket, e_errors.WRITE_ERROR )
	    return_or_update_and_exit( self, self.lm_origin_addr, e_errors.WRITE_ERROR )
	    pass
        except:
	    traceback.print_exc()
            wr_err,rd_err,wr_access,rd_access = (1,0,1,0)
            vcc.update_counts( self.vol_info['external_label'],
                               wr_err, rd_err, wr_access, rd_access )
	    self.usr_driver.close()
            send_user_done( self, ticket, e_errors.WRITE_ERROR )
	    return_or_update_and_exit( self, self.lm_origin_addr, e_errors.WRITE_ERROR )
	    pass

        # we've read the file from user, shut down data transfer socket
        self.usr_driver.close()
	Trace.trace( 11, 'close data' )

	# Tell volume server & update database
	remaining_bytes = stats['remaining_bytes']
	wr_err = stats['wr_err']
	rd_err = stats['rd_err']
	wr_access = 1
	rd_access = 0

	rsp = fcc.new_bit_file( {'work':"new_bit_file",
				 'fc'  :{'location_cookie':location_cookie,
					 'size':file_bytes,
					 'sanity_cookie':sanity_cookie,
					 'external_label':self.vol_info['external_label'],
					 'complete_crc':file_crc}} )
	if rsp['status'][0] != e_errors.OK:
	    Trace.log( e_errors.ERROR,
		       "XXXXXXXXXXXenstore software error" )
	    ticket["fc"]["bfid"] = None
	    pass
	ticket['fc'] = rsp['fc']

	ticket['vc'].update( vcc.set_remaining_bytes(self.vol_info['external_label'],
						     remaining_bytes,
						     eod_cookie,
						     wr_err,rd_err, # added to total
						     wr_access,rd_access,
						     ticket["fc"]["bfid"]) )
	self.vol_info.update( ticket['vc'] )
	

	Trace.log(e_errors.INFO,"WRITE DONE"+str(ticket))
	
	Trace.trace( 11, 'b4 send_user_done' )
	send_user_done( self, ticket, e_errors.OK )

	return_or_update_and_exit( self, self.lm_origin_addr, e_errors.OK )
	pass
    return {}

def forked_read_from_hsm( self, ticket ):

    # if we're changing state from writing to reading, and the feature is enabled,
    # put an eov label on the tape before repositioning
    write_eov=0
    if eov1_paranoia and self.mode=='w' and mvr_config['driver']!='NullDriver':
        #cgw
        write_eov=1
    
    # have to fork early b/c of early user (tcp) check
    # but how do I handle vol??? - prev_vol, this_vol???
    if mvr_config['do_fork']: do_fork( self, ticket, 'r' )
    if mvr_config['do_fork'] and self.pid != 0:
	pass
    else:
	Trace.log(e_errors.INFO,"READ_FROM_HSM start"+str(ticket))

	self.lm_origin_addr = ticket['lm']['address']# who contacts me directly

	# First, call the user (to see if they are still there)
	sts = get_usr_driver( self, ticket )
	if sts == "error":
	    return_or_update_and_exit( self, self.lm_origin_addr, e_errors.ENCP_GONE )
	    pass

	t0 = time.time()
	sts = bind_volume( self, ticket['fc']['external_label'] )
	ticket['times']['mount_time'] = time.time() - t0
	if sts != e_errors.OK:
	    # add CLOSING DATA SOCKET SO ENCP DOES NOT GET 'Broken pipe'
	    self.usr_driver.close()
	    # make read specific and ...
	    sts = getattr(e_errors, "READ_"+sts )
	    send_user_done( self, ticket, sts )
	    return_or_update_and_exit( self, self.lm_origin_addr, sts )
	    pass
	self.vol_info['from_lm'] = ticket['lm']['address'] # who gave me this volume

        # space to where the file will begin and save location
        # information for where future reads will have to space the drive to.

        # setup values before transfer
        media_error = 0
        drive_errors = 0
        bytes_sent = 0			# reset below, BUT not used afterwards!!!!!!!!!!!!!
        user_file_crc = 0		# reset below, BUT not used afterwards!!!!!!!!!!!!!

        Trace.log(e_errors.INFO,"OPEN_FILE_READ "+str(ticket['fc']['external_label'])+
                  ":"+str(ticket['fc']['location_cookie']))

        #eov1 paranoia.
        #XXX need to check returns of open, write, and close
        if write_eov:
            eod_cookie = self.vol_info['eod_cookie']
            driver_object=self.hsm_driver.open( mvr_config['device'], 'w' )
            if debug_paranoia: print "writing EOV"
            ll = driver_object.format_eov1_header( external_label, eod_cookie)
            if debug_paranoia: print "writing EOV1 label", ll
	    driver_object.write( ll )
	    driver_object.writefm()
            driver_object.close()
        # open the hsm file for reading and read it
        try:
            Trace.trace(11, 'driver_open '+mvr_config['device'])
            driver_object = self.hsm_driver.open( mvr_config['device'], 'r' )
	    self.no_xfers = self.no_xfers + 1

	    t0 = time.time()
	    driver_object.seek( ticket['fc']['location_cookie'] )

	    ticket['times']['seek_time'] = time.time() - t0

	    # create the wrapper instance (could be different for different tapes)
            wrapper=wrapper_selector.select_wrapper(self.vol_info['wrapper'])
	    if wrapper == None:
		wrapper=wrapper_selector.select_wrapper("cpio_odc")
		if wrapper == None:
		    raise errno.errorcode[errno.EINVAL], "Invalid wrapper"

            Trace.log(e_errors.INFO,"WRAPPER.READ "+str(ticket['fc']['external_label']))
	    if ticket['fc']['sanity_cookie'][1] == None:# when reading...
		crc_flag = None
	    else: crc_flag = self.crc_flag
	    t0 = time.time()
            Trace.trace(11,'calling read_pre_data')
	    self.hsm_driver.user_state_set( forked_state.index('wrapper, pre') )
            wrapper.read_pre_data( driver_object, ticket )
            Trace.trace(11,'calling fd_xfer -sanity size='+str(ticket['fc']['sanity_cookie'][0]))
	    self.hsm_driver.user_state_set( forked_state.index('data') )
            san_crc = driver_object.fd_xfer( self.usr_driver.fileno(),
				  ticket['fc']['sanity_cookie'][0],
				  self.crc_flag,
				  0 )
	    # check the san_crc!!!
	    if     self.crc_flag != None \
	       and ticket['fc']['sanity_cookie'][1] != None \
	       and san_crc != ticket['fc']['sanity_cookie'][1]:
		pass
	    if (ticket['fc']['size']-ticket['fc']['sanity_cookie'][0]) > 0:
                Trace.trace(11,'calling fd_xfer -rest size='+str(ticket['fc']['size']-ticket['fc']['sanity_cookie'][0]))
		user_file_crc = driver_object.fd_xfer( self.usr_driver.fileno(),
					    ticket['fc']['size']-ticket['fc']['sanity_cookie'][0],
					    self.crc_flag, san_crc )
	    else:
		user_file_crc = san_crc
		pass

	    Trace.log(e_errors.INFO,'done with read fd_xfers')
	    
	    if ticket['mover']['local_mover']:
		# try to give the file to the user
		# Note: IRIX fs (and nfs) allow user to give file to other
		# user but LINUX fs does not.
		try:
		    os.chown( ticket['mover']['lcl_fname'],
				 ticket['wrapper']['uid'],
				 ticket['wrapper']['gid'] )
		except: pass
		pass

	    # This if block is a place holder for the code that should
	    # be executed when CRCs don't match.
	    if     self.crc_flag != None \
	       and ticket['fc']['complete_crc'] != None \
	       and user_file_crc != ticket['fc']['complete_crc']:
		pass

	    tt = {'data_crc':ticket['fc']['complete_crc']}
            Trace.trace(11,'calling read_post_data')
	    self.hsm_driver.user_state_set( forked_state.index('wrapper, post') )
            wrapper.read_post_data( driver_object, tt )
	    ticket['times']['transfer_time'] = time.time() - t0

            Trace.trace(11,'calling get stats')
	    stats = self.hsm_driver.get_stats()
	    # close hsm file
            Trace.trace(11,'calling close')
            driver_object.close()
            Trace.trace(11,'closed')
	    wr_err,rd_err       = stats['wr_err'],stats['rd_err']
	    wr_access,rd_access = 0,1
        #except errno.errorcode[errno.EPIPE]: # do not know why I can not use just 'EPIPE'
	except (FTT.error, EXfer.error), err_msg:
            # XXX
            # Check for a broken pipe.

            if err_msg.args[2] == "Broken pipe": #XXX should compare ints rather than strings
                err = e_errors.BROKENPIPE
                ticket['status']=(e_errors.BROKENPIPE,None)
            else:
                err = e_errors.READ_ERROR
                traceback.print_exc()
            Trace.log( e_errors.ERROR,
		       'FTT or Exfer exception: '+str(sys.exc_info()[0])+str(sys.exc_info()[1]) )
	    self.usr_driver.close()
	    send_user_done( self, ticket, err)
	    return_or_update_and_exit( self, self.lm_origin_addr, e_errors.OK )
	except "seek error":
	    Trace.log( e_errors.ERROR, "seek error during read" )
            wr_err,rd_err,wr_access,rd_access = (0,1,0,1)
            vcc.update_counts( self.vol_info['external_label'],
                               wr_err, rd_err, wr_access, rd_access )
	    self.usr_driver.close()
            send_user_done( self, ticket, e_errors.READ_ERROR )
	    return_or_update_and_exit( self, self.lm_origin_addr, e_errors.READ_ERROR )
	    pass
        except:
	    # unanticipated exception: guess a cause and hope we can continue
	    traceback.print_exc()
            media_error = 1
            wr_err,rd_err,wr_access,rd_access = (0,1,0,1)

        # we've sent the hsm file to the user, shut down data transfer socket
        self.usr_driver.close()

        # get the error/mount counts and update database
        xx = vcc.update_counts( self.vol_info['external_label'],
				wr_err,rd_err,wr_access,rd_access )
	self.vol_info.update( xx )

        # if media error, mark volume readonly, unbind it & tell user to retry
        if media_error :
            vcc.set_system_readonly( self.vol_info['external_label'] )
            send_user_done( self, ticket, e_errors.READ_ERROR )
	    return_or_update_and_exit( self, self.lm_origin_addr, e_errors.READ_ERROR )

        # drive errors are bad:  unbind volume it & tell user to retry
        elif drive_errors :
            vcc.set_hung( self.vol_info['external_label'] )
            send_user_done( self, ticket, e_errors.READ_ERROR )
	    return_or_update_and_exit( self, self.lm_origin_addr, e_errors.READ_ERROR )

        # All is well - read has finished correctly

        # add some info to user's ticket
        ticket['vc'] = self.vol_info
	ticket['vc']['current_location'] = ticket['fc']['location_cookie']

        Trace.log(e_errors.INFO,'READ DONE'+str(ticket))

	send_user_done( self, ticket, e_errors.OK )
	return_or_update_and_exit( self, self.lm_origin_addr, e_errors.OK )
	pass
    return {} # forked_read_from_hsm


def return_or_update_and_exit( self, origin_addr, status ):
    if status != e_errors.OK: Trace.log( e_errors.ERROR, str(status) )
    if mvr_config['do_fork']:
	# need to send info to update parent: vol_info and
	# hsm_driver_info (read: hsm_driver.position
	#                  write: position, eod, remaining_bytes)
	# recent (read) errors (part of vol_info???)
	udpc.send_no_wait( {'work'       :'update_client_info',
			    'address'    :origin_addr,
			    'pid'        :os.getpid(),
			    'exit_status':m_err.index(status),
			    'vol_info'   :self.vol_info,
			    'no_xfers'   :self.no_xfers,
			    'hsm_driver' :{'blocksize'      :self.hsm_driver.blocksize,
					   'remaining_bytes':self.hsm_driver.remaining_bytes,
					   'vol_label'      :self.hsm_driver.vol_label,
					   'cur_loc_cookie' :self.hsm_driver.cur_loc_cookie}},
			   (mvr_config['hostip'],mvr_config['port']) )
	sys.exit( m_err.index(status) )
	pass
    return status_to_request( self, status ) # return_or_update_and_exit

# data transfer takes place on tcp sockets, so get ports & call user
# Info is added to ticket
def get_usr_driver( self, ticket ):
    self.hsm_driver.user_state_set( forked_state.index('encp check') )
    try:
	if self.local_mover_enable and ticket['wrapper']['machine']==os.uname():
	    if ticket['work'] == 'read_from_hsm':
		mode = 'w'
		fname = ticket['wrapper']['fullname']+'.'+ticket['unique_id']
		# do not worry about umask!?!
		ticket['mover']['lcl_fname'] = fname# to chown after exfer
		pass
	    else:
		mode = 'r'
		fname = ticket['wrapper']['fullname']
		pass
	    try:
		self.usr_driver = open( fname, mode )
		ticket['mover']['local_mover'] = 1
	    except: pass
	    pass

	# call the user and tell him I'm your mover and here's your ticket
	# ticket should have index ['callback_addr']
	# and then the entire ticket is sent to the callback_addr
	# The user expects the ticket to contain the following fields:
	#           ['unique_id'] == id sent by the user
	#           ['status'] == 'ok'
	#           ['mover']['callback_addr']
	if ticket['mover']['local_mover']:
	    ticket['mover']['callback_addr'] = None# to appease encp verbose
	    self.control_socket = callback.user_callback_socket( ticket )
	else:
	    host, port, listen_socket = callback.get_data_callback()
	    listen_socket.listen(4)
	    ticket['mover']['callback_addr'] = (host,port)
	    self.control_socket = callback.user_callback_socket( ticket )

	    # we expect a prompt call-back here

            read_fds,write_fds,exc_fds=select.select(
                [listen_socket],[],[],300) # 5 minute timeout
            if listen_socket in read_fds:
                self.usr_driver, address = listen_socket.accept()
                listen_socket.close()
            else:
                return "error"
	    pass
	return 'ok'
    except: return 'error'
    pass

# create ticket that says we are idle
def idle_mover_next( self ):
    return {'work'   : 'idle_mover',
	    'mover'  : mvr_config['name'],
	    'state'  : self.state,
	    'address': (mvr_config['hostip'],mvr_config['port'])}

# create ticket that says we have bound volume x
def have_bound_volume_next( self ):
    return { 'work'   : 'have_bound_volume',
	     'mover'  : mvr_config['name'],
	     'state'  : self.state,
	     'address': (mvr_config['hostip'],mvr_config['port']),
	     'vc'     : self.vol_info }

# create ticket that says we need to unbind volume x
def unilateral_unbind_next( self, error_info ):
    # This method use to automatically unbind, but now it does not because
    # there are some errors where the tape should be left in the drive. So
    # unilateral_unbind now just means that there was an error.
    # The response to this command can be either 'nowork' or 'unbind'
    return {'work'           : 'unilateral_unbind',
	    'mover'          : mvr_config['name'],
	    'state'          : self.state,
	    'address'        : (mvr_config['hostip'],mvr_config['port']),
	    'external_label' : self.fc['external_label'],
	    'state'          : self.state,
	    'status'         : (error_info,None)}


import timer_task
# Gather everything together and add to the mess

class MoverServer(  dispatching_worker.DispatchingWorker
	     	  , generic_server.GenericServer
		  , timer_task.TimerTask ):
    def __init__( self, csc_address, name):
        global mvr_config, udpc
        generic_server.GenericServer.__init__(self, csc_address, name)
        Trace.init( self.log_name )

        # get my (localhost) configuration from the configuration server
        mvr_config = self.csc.get( name )
        if mvr_config['status'][0] != 'ok':
            raise 'could not start mover',name,' up:' + mvr_config['status']
        # clean up the mvr_config a bit
        mvr_config['name'] = name
        mvr_config['do_fork'] = 1
	# for production, either add 'execution_env':'production to mover
        # config or change this default to 'production'
	if not 'execution_env' in mvr_config.keys(): mvr_config['execution_env'] = 'devel'
        if not 'do_eject' in mvr_config.keys(): mvr_config['do_eject'] = 'yes'

        del mvr_config['status']

        # get clients -- these will be (readonly) global object instances
        udpc =  udp_client.UDPClient()     # for server to send (client) request

        # now get my library manager's config ---- COULD HAVE MULTIPLE???
        # get info asssociated with our volume manager
        libm_config_dict = {}
        if type(mvr_config['library']) == types.ListType:
            for lib in  mvr_config['library']:
                libm_config_dict[lib] = {'startup_polled':'not_yet'}
                libm_config_dict[lib].update( self.csc.get_uncached(lib) )
                pass
            pass
        else:
            lib = mvr_config['library']
            mvr_config['library'] = [lib]	# make it a list
            libm_config_dict[lib] = {'startup_polled':'not_yet'}
            libm_config_dict[lib].update( self.csc.get_uncached(lib) )
            pass        

	self.client_obj_inst = MoverClient( self.csc )
	self.client_obj_inst.log_name = self.log_name # duplicate for do_fork
	self.summoned_while_busy = []
        Trace.log( e_errors.INFO, 'Mover starting - contacting libman')
	for lm in mvr_config['library']:# should be libraries
	    address = (libm_config_dict[lm]['hostip'],libm_config_dict[lm]['port'])
	    next_req_to_lm = idle_mover_next( self.client_obj_inst )
	    do_next_req_to_lm( self, next_req_to_lm, address )
	    pass
	# now go on with *server* setup (i.e. respond to summon,status,etc.)
	dispatching_worker.DispatchingWorker.__init__( self,(mvr_config['hostip'],
                                                             mvr_config['port']) )
	self.last_status_tick = {}
	#print time.time(),'ronDBG - MoverServer init timerTask rcv_timeout is',self.rcv_timeout
	#timer_task.TimerTask.__init__( self, self.rcv_timeout )
	timer_task.TimerTask.__init__( self, 5 )
	#timer_task.msg_add( 65, self.hello1 )
	#timer_task.msg_add( 25, self.hello1 )
	#timer_task.msg_add( 15, self.hello2 )
	#timer_task.msg_add( 15, self.hello3, {} )
	return None

    def hello1( self ): print time.time(),'ronDBG - hello from self.hello1';timer_task.msg_add( 15, self.hello2 )
    def hello2( self ): print time.time(),'ronDBG - hello from self.hello2'
    def hello3( self, ticket ):
	print time.time(),'ronDBG - hello from self.hello3'
	timer_task.msg_add( 15, self.hello3, {} )
	if 'work' in ticket.keys():
	    out_ticket = {'status':(e_errors.OK,None)}
	    self.reply_to_caller( out_ticket )
	    pass
	return

    def debug_status( self, ticket ):
	out_ticket = {'status':(e_errors.OK,None)}
	out_ticket['vol_info'] = self.client_obj_inst.vol_info
	self.reply_to_caller( out_ticket )
	return

    def status( self, ticket ):
	tim = time.time()
	obj_inst = self.client_obj_inst
	# try getting wr_bytes 1st to prevent writing ahead of reading
	wb  = obj_inst.hsm_driver.wr_bytes_get()
	rb  = obj_inst.hsm_driver.rd_bytes_get()
	tick = { 'status'       : (e_errors.OK,None),
		 'drive_sn'     : obj_inst.hsm_drive_sn,
		 #
		 'crc_flag'     : str(obj_inst.crc_flag),
		 'forked_state' : forked_state[
            obj_inst.hsm_driver.user_state_get()],
		 'state'        : obj_inst.state,
		 'no_xfers'     : obj_inst.no_xfers,
		 'local_mover'  : obj_inst.local_mover_enable,
		 'rd_bytes'     : rb,
		 'wr_bytes'     : wb,
		 # from "work ticket"
		 'bytes_to_xfer': obj_inst.bytes_to_xfer,
		 'files'        : obj_inst.files,
		 'mode'         : obj_inst.mode,
		 'tape'         : obj_inst.tape,
		 'time_stamp'   : tim,
		 'vol_info'     : obj_inst.vol_info,
		 # just include total "work ticket"
		 'work_ticket'  : obj_inst.work_ticket,
		 'zlast_status' : self.last_status_tick }
	self.reply_to_caller( tick )
	self.last_status_tick = tick	# remember - duplicate reference -- 
	del self.last_status_tick['zlast_status'] # must del after send
	return

    def quit(self,ticket):		# override dispatching_worker -
	#  method which does not clean up
        Trace.trace(10,"quit address="+str(self.server_address))
	del self.client_obj_inst	# clean up shm??? I should not have to!
	# Note: 11-30-98 python v1.5 does cleans-up shm upon SIGINT (2)
        ticket['address'] = self.server_address
        ticket['status'] = (e_errors.OK, None)
        ticket['pid'] = os.getpid()
        try:
            Trace.log(e_errors.INFO, "QUITTING... via os_exit python call")
        except:
            Trace.log(e_errors.INFO, "QUITTING-e... via os_exit python call")
        self.reply_to_caller(ticket)
        os._exit(0)
	return

    def start_draining(self, ticket):		# put itself into draining state
        self.client_obj_inst.state = 'draining'
	out_ticket = {'status':(e_errors.OK,None)}
	self.reply_to_caller( out_ticket )
	return

    def stop_draining(self, ticket):		# put itself into idle state
        if self.client_obj_inst.state == 'draining':
            self.client_obj_inst.state = 'idle'
	    out_ticket = {'status':(e_errors.OK,None)}
	    self.reply_to_caller( out_ticket )
	return

    def shutdown( self, ticket ):
	del self.client_obj_inst	# clean up shm??? I should not have to!
	# Note: 11-30-98 python v1.5 does cleans-up shm upon SIGINT (2)
	out_ticket = {'status':(e_errors.OK,None)}
	self.reply_to_caller( out_ticket )
	sys.exit( 0 )
	return

    def crc_on( self, ticket ):
	self.client_obj_inst.crc_flag = 1
	out_ticket = {'status':(e_errors.OK,None)}
	self.reply_to_caller( out_ticket )
	return
	
    def crc_off( self, ticket ):
	self.client_obj_inst.crc_flag = None
	out_ticket = {'status':(e_errors.OK,None)}
	self.reply_to_caller( out_ticket )
	return

    def local_mover( self, ticket ):
	try: self.client_obj_inst.local_mover_enable = ticket['enable']
	except: pass
	out_ticket = {'status':(e_errors.OK,None),
                      'enabled':self.client_obj_inst.local_mover_enable}
	self.reply_to_caller( out_ticket )
	return

    def config( self, ticket ):
	global mvr_config
	out_ticket = {'status':(e_errors.OK,None)}
	if 'config' in ticket.keys(): mvr_config.update( ticket['config'] )
	out_ticket['config'] = mvr_config
	self.reply_to_caller( out_ticket )
	return
	
    def update_client_info( self, ticket ):
	self.client_obj_inst.vol_info = ticket['vol_info']
	self.client_obj_inst.no_xfers = ticket['no_xfers']
	self.client_obj_inst.hsm_driver.blocksize = \
					ticket['hsm_driver']['blocksize']
	self.client_obj_inst.hsm_driver.remaining_bytes = \
					ticket['hsm_driver']['remaining_bytes']
	self.client_obj_inst.hsm_driver.vol_label = \
					ticket['hsm_driver']['vol_label']
	self.client_obj_inst.hsm_driver.cur_loc_cookie = \
					ticket['hsm_driver']['cur_loc_cookie']
	Trace.log( e_errors.INFO,
		   "update_client_info - pid:"+str(self.client_obj_inst.pid)+
		   "ticket['pid']:"+str(ticket['pid']) )
	wait = 0
	next_req_to_lm = get_state_build_next_lm_req( self, wait, ticket['exit_status'] )
	do_next_req_to_lm( self, next_req_to_lm, ticket['address'] )
	return

    def summon( self, ticket ):
	wait=os.WNOHANG
	next_req_to_lm = get_state_build_next_lm_req( self, wait, None )
	if next_req_to_lm['state']=='busy' and not ticket['address'] in self.summoned_while_busy:
	    self.summoned_while_busy.append(ticket['address'])
	do_next_req_to_lm( self, next_req_to_lm, ticket['address'] )
	return

    def handle_timeout( self ):
	if self.client_obj_inst.state == 'busy' and \
	   self.client_obj_inst.hsm_driver.user_state_get() ==  forked_state.index('data'):
	    if self.client_obj_inst.init_stall_time == 1:
		self.client_obj_inst.stall_time = time.time()
		self.client_obj_inst.init_stall_time = 0
	    else:
		rr = self.client_obj_inst.hsm_driver.rd_bytes_get()
		ww = self.client_obj_inst.hsm_driver.wr_bytes_get()
		bs = self.client_obj_inst.hsm_driver.blocksize
		p_rr = self.client_obj_inst.prev_r_bytes
		p_ww = self.client_obj_inst.prev_w_bytes
		if rr == p_rr and ww == p_ww:
		    # arbitrary time; to determine if a "stall" condition
		    # exists, be it network or tape.
		    if time.time()-self.client_obj_inst.stall_time > 60.0:# arbitrary number
			try:    os.system( 'traceMode 0' )
			except: pass
			if self.client_obj_inst.mode == 'w':
			    msg = 'writing mover (rr=%d ww=%d) '%(rr,ww)
			    if rr >= ww+bs: msg = msg + 'tape stall'
			    else:              msg = msg + 'network stall'
			else:
			    msg = 'reading mover (rr=%d ww=%d) '%(rr,ww)
			    if rr >= ww+bs: msg = msg + 'network stall'
			    else:              msg = msg + 'tape stall'
			    pass
			Trace.log( e_errors.ERROR,msg+' - should abort' )
			self.client_obj_inst.stall_time = time.time()
			pass
		    pass
		else:
		    self.client_obj_inst.prev_r_bytes = rr
		    self.client_obj_inst.prev_w_bytes = ww
		    self.client_obj_inst.stall_time = time.time()
		    pass
		pass
	    pass
        return

    pass

def do_next_req_to_lm( self, next_req_to_lm, address ):
    while next_req_to_lm != {} and next_req_to_lm != None:
	rsp_ticket = udpc.send(  next_req_to_lm, address )
	# STATE COULD BE 'BUSY' OR 'OFFLINE'
	if self.client_obj_inst.state != 'idle' and rsp_ticket['work'] != 'nowork':
	    # CHANGE THIS TO Trace.alarm???
	    Trace.log( e_errors.ERROR,
                       'FATAL ENSTORE - libm gave busy or offline move work %s'%\
		       rsp_ticket['work'] )
	    if mvr_config['execution_env'][0:5] == 'devel':
		Trace.log( e_errors.ERROR, 'FATAL ENSTORE in devel env. => crazed' )
		print 'FATAL ENSTORE in devel env. => crazed (check the log!)'
		self.client_obj_inst.state = 'crazed'
	    Trace.log( e_errors.ERROR, 'mover changing work %s to "nowork"'%\
		       rsp_ticket['work'] )
	    rsp_ticket['work'] = 'nowork'
	    pass
	# Exceptions are caught (except block) in dispatching_worker.py.
	# The reply is the command (i.e the network is the computer).
	try: client_function = rsp_ticket['work']
	except KeyError:
	    # CHANGE THIS TO Trace.alarm???
	    Trace.log( e_errors.ERROR,
                       'FATAL ENSTORE - invalid rsp from libm: %s'%rsp_ticket )
	    if mvr_config['execution_env'][0:5] == 'devel':
		Trace.log( e_errors.ERROR, 'FATAL ENSTORE in devel env. => crazed' )
		print 'FATAL ENSTORE in devel env. => crazed (check the log!)'
		self.client_obj_inst.state = 'crazed'
	    Trace.log( e_errors.ERROR, 'mover changing invalid rsp to "nowork"' )
	    client_function = 'nowork'
	method = MoverClient.__dict__[client_function]
	next_req_to_lm = method( self.client_obj_inst, rsp_ticket )
	# note: order of check is important to avoid KeyError exception
	if  self.summoned_while_busy and not next_req_to_lm:
	    # now check if next_req_to_lm=={} means we just started an xfer and
	    # are waiting for completion.
	    if self.client_obj_inst.state == 'idle':
		if self.client_obj_inst.vol_info['external_label'] == '':
		    next_req_to_lm = idle_mover_next( self.client_obj_inst )
		else:
		    next_req_to_lm = have_bound_volume_next( self.client_obj_inst )
		    pass
		address = self.summoned_while_busy[0]
		del self.summoned_while_busy[0]
		pass
	elif self.summoned_while_busy and next_req_to_lm['work']=='idle_mover':
	    # do not tell this lm idle as he may keep giving work
	    next_req_to_lm = idle_mover_next( self.client_obj_inst )
	    self.summoned_while_busy.append( address )
	    address = self.summoned_while_busy[0]
	    del self.summoned_while_busy[0]
	    pass
	pass
    return

def get_state_build_next_lm_req( self, wait, exit_status ):
    if self.client_obj_inst.pid:
	try: pid, status = os.waitpid( self.client_obj_inst.pid, wait )
	except:
	    if wait == 0: status = exit_status
	    else:         status = m_err.index(e_errors.OK)<<8 # assume success???
	    pid = self.client_obj_inst.pid
	    m = 'waitpid-for pid:%s wait:%s exit_status:%s exc_info:%s%s'
	    Trace.log( e_errors.WARNING,m%((self.client_obj_inst.pid,wait,
					    exit_status)+sys.exc_info()[0:2]) )
	    #traceback.print_exc()
	    #os.system( 'ps alxwww' )
	    #raise sys.exc_info()[0], sys.exc_info()[1]
	    pass
	if pid == self.client_obj_inst.pid:
	    self.client_obj_inst.pid = 0
	    if self.client_obj_inst.state != 'crazed':
                if self.client_obj_inst.state != 'draining':
		    self.client_obj_inst.state = 'idle'
	    exit_status = status>>8
	    next_req_to_lm = status_to_request( self.client_obj_inst,
						exit_status )
	else:
	    next_req_to_lm = have_bound_volume_next( self.client_obj_inst )
	    pass
	pass
    else:
	if self.client_obj_inst.vol_info['external_label'] == '':
	    next_req_to_lm = idle_mover_next( self.client_obj_inst )
	else:
	    next_req_to_lm = have_bound_volume_next( self.client_obj_inst )
	    pass
	pass
    return next_req_to_lm

def status_to_request( client_obj_inst, exit_status ):
    next_req_to_lm = {}
    if   m_err[exit_status] == e_errors.OK:
	if client_obj_inst.vol_info['external_label'] == '':
	    next_req_to_lm = idle_mover_next( client_obj_inst )
	else:
	    next_req_to_lm = have_bound_volume_next( client_obj_inst )
	    pass
        if next_req_to_lm['state'] != 'draining':
            next_req_to_lm['state'] = 'idle'
    elif m_err[exit_status] == e_errors.ENCP_GONE:
	if client_obj_inst.vol_info['external_label'] == '':
	    # This is the case where a just started mover determines ENCP_GONE
	    # before mounting the volume, so the mover does not have a volume
	    next_req_to_lm = idle_mover_next( client_obj_inst )
	else:
	    next_req_to_lm = have_bound_volume_next( client_obj_inst )
	    pass
        if next_req_to_lm['state'] != 'draining':
            next_req_to_lm['state'] = 'idle'
    elif m_err[exit_status] == e_errors.WRITE_ERROR:
	next_req_to_lm = offline_drive( client_obj_inst, m_err[exit_status] )
    elif m_err[exit_status] == e_errors.READ_ERROR:
	if client_obj_inst.read_error[1]:
            next_req_to_lm = offline_drive( client_obj_inst,m_err[exit_status])
	else:
	    next_req_to_lm = unilateral_unbind_next( client_obj_inst,
                                                     m_err[exit_status] )
	    pass
	pass
    elif m_err[exit_status] in (e_errors.WRITE_NOTAPE,
				e_errors.WRITE_TAPEBUSY,
				e_errors.READ_NOTAPE,
				e_errors.READ_TAPEBUSY):
	next_req_to_lm = freeze_tape( client_obj_inst, m_err[exit_status] )
	pass
    elif m_err[exit_status] in (e_errors.WRITE_BADMOUNT,
				e_errors.WRITE_BADSPACE,
				e_errors.WRITE_UNLOAD,
				e_errors.READ_BADMOUNT,
				e_errors.READ_BADLOCATE,
				e_errors.READ_COMPCRC,
				e_errors.READ_EOT,
				e_errors.READ_EOD,
				e_errors.READ_UNLOAD,
				e_errors.UNMOUNT):
	next_req_to_lm = freeze_tape_in_drive( client_obj_inst, m_err[exit_status] )
    elif m_err[exit_status] in (e_errors.READ_VOL1_READ_ERR,
                                e_errors.WRITE_VOL1_READ_ERR):
        # XXX jon says we have to add the volume to the suspect volume list.  Am I doing this correctly?
        next_req_to_lm = offline_drive( client_obj_inst,m_err[exit_status])
        pass
    elif m_err[exit_status] in (e_errors.WRITE_VOL1_MISSING,
                                e_errors.WRITE_VOL1_WRONG):
        # grab another volume
        next_req_to_lm = freeze_tape( client_obj_inst,
                                      m_err[exit_status] )
        pass
    elif m_err[exit_status] in (e_errors.READ_VOL1_MISSING,
                                e_errors.READ_VOL1_WRONG):
        #fatal
        next_req_to_lm = freeze_tape_in_drive( client_obj_inst,
                                               m_err[exit_status] )
        pass
    else:
	# new error
        Trace.log( e_errors.ERROR, 'FATAL ERROR - MOVER - unknown transfer status - fix me now' )
	while 1: time.sleep( 100 )		# NEVER RETURN!?!?!?!?!?!?!?!?!?!?!?!?
	pass
    return next_req_to_lm

class MoverInterface(generic_server.GenericServerInterface):

    def __init__(self):
        # fill in the defaults for possible options
        self.summon = 1
        generic_server.GenericServerInterface.__init__(self)

    #  define our specific help
    def parameters(self):
        return 'mover_device'

    # parse the options like normal but make sure we have a mover
    def parse_options(self):
        interface.Interface.parse_options(self)
        # bomb out if we don't have a mover
        if len(self.args) < 1 :
	    self.missing_parameter(self.parameters())
            self.print_help(),
            sys.exit(1)
        else:
            self.name = self.args[0]

#############################################################################

#############################################################################
import sys				# sys.argv[1:]
import socket                           # gethostname (local host)
import string				# atoi
import types				# see if library config is list
import udp_client

# get an interface, and parse the user input
intf = MoverInterface()

mvr_srvr =  MoverServer( (intf.config_host, intf.config_port), intf.name )
del intf

mvr_srvr.serve_forever()

Trace.log(e_errors.INFO, 'ERROR?')



