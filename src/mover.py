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
import types				

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
import driver
#import FTT				
#import EXfer				
import e_errors
import write_stats
import udp_client
import socket_ext
import hostaddr
import timer_task

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

READ, WRITE = range(2)
def  mode_name(mode):
    if mode is None:
        return None
    else:
        return ['READ','WRITE'][mode]

MB=1L<<20

class Mover(  dispatching_worker.DispatchingWorker,
              generic_server.GenericServer,
              timer_task.TimerTask ):
    def __init__( self, csc_address, name):

        self.name = name

        generic_server.GenericServer.__init__(self, csc_address, name)
        Trace.init( self.log_name )

        self.config = self.csc.get( name )
        if self.config['status'][0] != 'ok':
            raise MoverError('could not start mover %s: %s'%(name, self.config['status']))

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

        self.buffer = []
        self.buffer_bytes = 0L
        
        self.udpc =  udp_client.UDPClient()
        self.state = IDLE
        self.last_error = ()
        if self.check_lockfile():
            self.state = OFFLINE

        self.current_location = "0L"
        self.mode = None # READ or WRITE
        self.bytes_to_transfer = 0
        self.bytes_read = 0
        self.bytes_written = 0
        self.external_label = None
        self.volume_family = None
        self.volume_status = ((None,None), (None,None))
        self.files = ('','')
        self.work_ticket = {}
        self.hsm_drive_sn = ''
        self.no_transfers = 0
        
        self.driveStatistics = {'mount':{},'dismount':{}}
        self.mcc = media_changer_client.MediaChangerClient( self.csc,
                                             self.config['media_changer'] )

        self.vol_info = {'external_label':'', 'media_type':''}
        self.vol_vcc = {}               # vcc associated with a particular
                                        ## vol_label (useful when other lib man summons
                                        ## during delayed dismount -- labels must be unique

        self.read_error = [0,0]         # error this vol ([0]) and last vol ([1])
        self.crc_flag = 1
        self.config['device'] = os.path.expandvars( self.config['device'] )


        try: self.driver_class = getattr(driver, self.config['driver'])()
        except AttributeError:
            Trace.log(e_errors.INFO, "No such driver: "+self.config['driver'])
            exc,msg,tb=sys.exc_info()
            raise exc,msg

        self.driver = self.driver_class.open( self.config['device'], 'r' )
        stats = self.driver.get_stats()
        
        
        if stats['serial_num'] != None: self.hsm_drive_sn = stats['serial_num']
        self.config['serial_num'] = stats['serial_num']
        self.config['product_id'] = stats['product_id']
        self.config['vendor_id'] = stats['vendor_id']
        

        # check for tape in drive
        # if no vol one labels, I can only eject. -- tape maybe left in bad
        # state.
        if self.do_eject == 'yes':
            self.driver_class.offline( self.config['device'] )
            # tell media changer to unload the vol BUT I DO NOT KNOW THE VOL
            #mcc.unloadvol( self.vol_info, self.config['mc_device'] )
            self.mcc.unloadvol( self.vol_info, self.name, 
                self.config['mc_device'], None)
            pass
        self.driver.close( skip=0 )


	# now go on with server setup (i.e. respond to summon,status,etc.)
	dispatching_worker.DispatchingWorker.__init__( self,(self.config['hostip'],
                                                             self.config['port']) )

        
        self.do_collect = 0 # Don't let dispatching_worker's loop do a waitpid, we want to catch children ourself.

        self.libraries = []
        lib_list = self.config['library']

        if type(lib_list) != type([]):
            lib_list = [lib_list]
            
        for lib in lib_list:
            lib_config = self.csc.get(lib)
            self.libraries.append((lib, (lib_config['hostip'], lib_config['port'])))

        self.set_interval_func(self.update_lm, 15) #this sets the period for messages to LM.


    def update_lm(self):
        status = e_errors.OK, None
        if self.state is IDLE:
            work = "mover_idle"
        elif self.state in (MOUNT_WAIT, HAVE_BOUND):
            work = "mover_bound_volume"
        elif self.state in (ACTIVE, DISMOUNT_WAIT):
            work = "mover_busy"
        elif self.state is ERROR:
            work = "mover_error"
            status = self.last_error
        else: #cleaning, draining or offline, no message sent
            ### XXX when going offline, we need to send a message to LM
            return
           
        
        ticket =  {
            "mover":  self.name,
            "external_label":  self.external_label,
            "current_location": self.current_location,
            "status": status, 
            "volume_family": self.volume_family,
            "volume_status": self.volume_status,
            "operation": mode_name(self.mode),
            "work": work,
            }
        
        for lib, addr in self.libraries:
            print "Send", ticket, "to", addr
            self.udpc.send_no_wait(ticket, addr)

    def nowork( self, ticket ):
	return {}

    def read_tape(self, fd):
        pass

    def write_tape(self, fd):
        pass

    def read_user(self, fd):
        pass

    def write_user(self, fd):
        pass
        
    def unbind_volume( self, ticket ):

	Trace.log(e_errors.INFO,'UNBIND  %s start'%(ticket,))
	    
	# do any driver level rewind, unload, eject operations on the device
        clean_drive = 0
        if self.do_eject:
	    Trace.log( e_errors.INFO, "Performing offline/eject of device %s"%(self.config['device'],))
	    self.driver_class.offline(self.config['device'])
            self.store_statistics('dismount', self.driver_class)
            if (self.driveStatistics['dismount'].has_key('cleaning_bit') and
                self.driveStatistics['dismount']['cleaning_bit'] == '1'): clean_drive = 1
	    Trace.log( e_errors.INFO, "Completed  offline/eject of device %s"%(self.config['device'],))

	# now ask the media changer to unload the volume
	Trace.log(e_errors.INFO,"Requesting media changer unload")
	rr = self.mcc.unloadvol( self.vol_info, self.name, 
                                 self.config['mc_device'],
                                self.vol_vcc[self.vol_info['external_label']] )
	Trace.log(e_errors.INFO,"Media changer unload status %s"%(rr['status'],))
	if rr['status'][0] != "ok":
	    return e_errors.UNMOUNT, rr['status']

	del self.vol_vcc[self.vol_info['external_label']]
	self.vol_info['external_label'] = ''
        if clean_drive:
            try:
                rr = self.mcc.doCleaningCycle(self.config)
                Trace.log(e_errors.INFO,"Media changer cleaningCycle return status =%s"%(rr['status'],))
            except:
                e_errors.handle_error()
        return e_errors.OK, None


    # the library manager has asked us to write a file to the hsm
    def write_to_hsm( self, ticket ):
	self.fc = ticket['fc']
	return self.forked_write_to_hsm(ticket )
	
    # the library manager has asked us to read a file to the hsm
    def read_from_hsm( self, ticket ):
	self.fc = ticket['fc']
	return self.forked_read_from_hsm(ticket )

    # update and store driver statistics
    # action is either mount or dismount
    def store_statistics( self, action, driver ):
        if driver is None:
            Trace.log(e_errors.ERROR,"No mount statistics stored. driver is None.")
            return
        if not (action=='mount' or action =='dismount'):
            Trace.log(e_errors.ERROR,"Wrong action specified for store__statistics.")
            return
        
        
        try:
            if action == 'mount': statistics = driver.statisticsOpen
            else: statistics = driver.statisticsClose
            self.driveStatistics[action] = statistics
           
        except KeyError:
            Trace.log(e_errors.ERROR,"%s statistics malformed."%(action,))
        try:
            path = self.config['statistics_path']
        except KeyError:
            Trace.log(e_errors.ERROR,"Mover 'statistics_path' configuration missing.")
            return
        output_dict = self.driveStatistics[action]
        output_dict['DEVNAME'] = self.config['mc_device']
        output_dict['VSN'] = self.vol_info['external_label']
        
        try:
            fd = open(path,'a')
        except IOError, msg:
            Trace.log(e_errors.INFO, "IOError: "+str(msg))
            return
        try:
            fd.write("%s\n"%(action,))
            fd.write(repr(output_dict))
            fd.write('\n')
            fd.close()
        except IOError, msg:
            Trace.log(e_errors.INFO, "IOError: "+str(msg))
        return

    # The following functions are the result of the enstore error documentation...
    # know it, live it, love it.
    def freeze_tape( self, error_info ):# DO NOT UNLOAD TAPE  (BUT --
        # LIBRARY MANAGER CAN RSP UNBIND)
        self.vcc.set_system_noaccess( self.vol_info['err_external_label'] )
        Trace.log( e_errors.ERROR,
                   'MOVER SETTING VOLUME "SYSTEM NOACCESS" %s %s'%(str(error_info),
                                                                   str(self.vol_info)))
        return self.unilateral_unbind_next( error_info )

    def freeze_tape_in_drive( self, error_info ):
        self.vcc.set_system_noaccess( self.vol_info['err_external_label'] )
        Trace.log( e_errors.ERROR,
                   'MOVER SETTING VOLUME "SYSTEM NOACCESS" %s %s'%(str(error_info),
                                                                   str(self.vol_info)))
        return self.offline_drive(error_info )

    def offline_drive( self, error_info ):	# call directly for READ_ERROR
        Trace.log( e_errors.ERROR, "MOVER OFFLINE %s"%(error_info,))
        self.state = OFFLINE
        return self.unilateral_unbind_next(error_info )


    def fatal_enstore( self, error_info ):
        Trace.log( e_errors.ERROR, "FATAL ERROR - MOVER - %s"%(error_info,))
        ticket = {'work'          :"unilateral_unbind",
                  'external_label':self.vol_info['external_label'],
                  'status'        :error_info}
        rsp = self.udpc.send( ticket, self.lm_origin_addr )
        # Enstore design issue... it has not yet been decided what to do; so for
        # now I just...   #XXX
        while 1: time.sleep( 100 )	 # the Inquisitor will restart us
        return


    def send_user_done( self, ticket, error_info, extra=None ):
        ticket['status'] = (error_info,extra)
        callback.write_tcp_obj( self.control_socket, ticket)
        self.control_socket.close()
        return
    

    def do_fork( self, ticket, mode ):
        if self.state not in (IDLE, DRAINING):
            Trace.log(e_errors.ERROR, "mover: do_fork called when mover already forked")
        #grab a new udp client
        ticket['mover'] = self.config
        if mode == 'w' or mode == 'r':
            # get vcc and fcc for this transfer
            self.fcc = file_clerk_client.FileClient( self.csc, 0,
                                                ticket['fc']['address'] )
            self.vcc = volume_clerk_client.VolumeClerkClient( self.csc,
                                                         ticket['vc']['address'] )
            self.vol_vcc[ticket['fc']['external_label']] = self.vcc# remember for unbind
            self.prev_r_bytes = 0; self.prev_w_bytes = 0; self.init_stall_time = 1
            self.bytes_to_transfer = ticket['fc']['size']
            # save some stuff for "status"
            self.external_label = ticket['fc']['external_label']
            self.files = ("%s:%s"%(ticket['wrapper']['machine'][1],
                                   ticket['wrapper']['fullname']),
                          ticket['wrapper']['pnfsFilename'])
            self.work_ticket = ticket	#just save the whole thing for "status"
                                            # and for bind to get lm address!

        if self.state != 'draining': self.state = 'busy' # draining state cannot be changed
        self.mode = mode			# client mode, not driver mode

        self.pid = self.fork()
        if self.pid == 0:
            Trace.init( self.log_name ) # update trc_pid


    def bind_volume( self, external_label ):
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

        if self.vol_info['external_label'] == '':

            # NEW VOLUME FOR ME - find out what volume clerk knows about it
            self.vol_info['err_external_label'] = external_label
            tmp_vol_info = self.vcc.inquire_vol( external_label )
            if tmp_vol_info['status'][0] != 'ok': return 'NOTAPE' # generic, not read or write specific
            open_flag = "a+"
            if tmp_vol_info['system_inhibit'][1] in [ 'readonly', 'full']:
                open_flag = "r"
            # if there is a tape in the drive, eject it (then the robot can put it away and we can continue)
            # NOTE: can we detect "cleaning in progress" or "cleaning cartridge (as
            # opposed to data cartridge) in drive?"
            # If we can detect "cleaning in progress" we can wait for it to
            # complete before ejecting.
            # the media_changer ignores mount and dismount requests for a drive that
            # is in the midst of a cleaning cycle.-- tgj
            if self.do_eject:
                Trace.log(e_errors.INFO,'Performing precautionary offline/eject of device%s'%
                          (self.config['device'],))
                self.driver_class.offline(self.config['device'])
                Trace.log(e_errors.INFO,'Completed  precautionary offline/eject of device %s'%
                          (self.config['device'],))
                # before loading volume check if cleaning is needed
                # and if yes clean the drive
                self.driver_class.open(self.config['device'],"r")
                statistics = self.driver_class.get_allStats(self.config['device'])
                if debug_paranoia:
                    Trace.log(e_errors.INFO,"MANDBG prebind statistics %s"%(statistics,))
                cleaning_bit = statistics.get('cleaning_bit','')
                if cleaning_bit == '1':
                    rr = self.mcc.doCleaningCycle(self.config)
                    Trace.log(e_errors.INFO,"Media changer cleaningCycle return status =%s"%(rr['status'],))
                self.driver_class.close(skip=0)


            self.vol_info['read_errors_this_mover'] = 0	
            tmp_mc = ", "+str({"media_changer":self.config['media_changer']})
            Trace.log(e_errors.INFO,Trace.MSG_MC_LOAD_REQ+"Requesting media changer load %s %s %s"%
                       (tmp_vol_info, tmp_mc, self.config['mc_device']))           
            try:
                rsp = self.mcc.loadvol( tmp_vol_info, self.name,
                                    self.config['mc_device'], self.vcc )
            except errno.errorcode[errno.ETIMEDOUT]:
                rsp = { 'status':('ETIMEDOUT',None) }
            Trace.log(e_errors.INFO,Trace.MSG_MC_LOAD_DONE+'Media changer load status %s %s'%
                      (rsp['status'], tmp_mc))
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
                else:
                    mcstate = self.vcc.update_mc_state(external_label)
                    if mcstate["at_mover"][0] != 'unmounted':
                        # force set to unmounted
                        Trace.log(e_errors.INFO,"forcing %s to unmouted state"%
                                  (external_label,))
                        self.vcc.set_at_mover(external_label, 
                                              'unmounted', 
                                              self.name,1)
                    return 'BADMOUNT'
            try:
                Trace.log(e_errors.INFO,'Requesting software mount %s %s'%
                          (external_label,self.config['device']))
                self.driver_class.sw_mount( self.config['device'],
                                          tmp_vol_info['blocksize'],
                                          tmp_vol_info['remaining_bytes'],
                                          external_label,
                                          tmp_vol_info['eod_cookie'] )
                Trace.log(e_errors.INFO,'Software mount complete %s %s'%
                          (external_label,self.config['device']))
            except:
                e_errors.handle_error()
                self.vol_info.update(tmp_vol_info) #XXX Kludge to make unbind work
                return 'BADSWMOUNT' # generic, not read or write specific

            # at this point, we have the volume in our tape drive,
            # so if anything goes wrong from this point on, we can let whoever
            # know that the volume is in our tape drive.
            # Note that the self.vol_info is updated again if, for example
            # a label is placed on the tape.
            self.vol_info.update( tmp_vol_info )
            self.vol_info['from_lm'] = self.work_ticket['lm']['address'] # who gave me this volume
            self.store_statistics('mount', self.driver_class)
            tape_is_labelled=0
            #Paranoia:  We may have the wrong tape.  Check the VOL1 header!
            if vol1_paranoia and self.config['driver']=='FTTDriver':
                driver = self.driver_class.open( self.config['device'], 'r' )
                Trace.log(e_errors.INFO, "Rewinding tape %s to check VOL1 label"%(external_label,))
                r=driver.rewind()
                header_type, header_label, cookie = driver.check_header()
                if not (header_type == None or header_type == 'VOL1'):
                   header_type = "GARBAGE"    # needed for trace to not fail 
                   header_label = "GARBAGE"   # needed for trace to not fail 
                Trace.log(e_errors.INFO, "header_type=%s, label=%s, cookie=%s" %
                          (header_type,header_label,cookie))
                if header_type == None:
                    ##This only happens if there was a read error, which is
                    ##OK for a brand-new tape
                    if driver.is_bot(tmp_vol_info['eod_cookie']):
                        infomsg="New tape, labelling %s"%(external_label,)
                        tape_is_labelled=0
                        Trace.log(e_errors.INFO, infomsg)
                    else:  #read error on tape, but eod!=bot
                        Trace.log(e_errors.ERROR,"VOL1_READ_ERR %s"%(external_label,))
                        return 'VOL1_READ_ERR'
                elif header_type == 'VOL1':
                    if header_label != external_label:
                        self.vcc.set_system_noaccess( external_label )
                        errmsg="wrong VOL1 header: got %s, expected %s" % (
                                header_label, external_label)
                        Trace.log(e_errors.ERROR, errmsg)
                        return 'VOL1_WRONG'
                    else:
                        tape_is_labelled=1
                else:
                    self.vcc.set_system_noaccess( external_label )
                    errmsg="no VOL1 header present for volume %s: read label %s %s" %\
                            (external_label, fix_nul(header_type), fix_nul(header_label))
                    Trace.log(e_errors.ERROR,errmsg)
                    tape_is_labelled=0
                    return 'VOL1_MISSING' 
                    
                Trace.log(e_errors.INFO, "Rewinding %s after checking label" % (external_label,))
                # Note:  Closing the device seems to write a
                #file mark (even though it was opened "r"!),
                # so we better close *before* rewinding.
                driver.close(skip=0)
                driver = self.driver_class.open( self.config['device'], open_flag)
                r=driver.rewind()
                x=driver.tell()
                Trace.log(e_errors.INFO, "CGWDEBUG tell %s"%(x,))
            ##end of paranoid checks    
            else:
                driver = self.driver_class.open( self.config['device'], open_flag)

            if (self.config['driver']=='FTTDriver' and
                driver.is_bot(driver.tell()) and
                driver.is_bot(tmp_vol_info['eod_cookie'])):

                # write an ANSI label and update the eod_cookie
                label = driver.format_vol1_header( external_label )
                Trace.log(e_errors.INFO, "bind_volume: writing VOL1 label"+label)
                if tape_is_labelled:

                    Trace.log(e_errors.INFO,"bind_volume: tape %s already labelled"%(external_label,))
                    driver.skip_fm(1) #Take me past the VOL1 label
                    eod_cookie = driver.tell()
                    

                    Trace.log(e_errors.INFO, "new EOD cookie=%s"%(eod_cookie,))
                else:
                    driver.write( label )
                    driver.writefm()
                    eod_cookie = driver.tell()

                Trace.log(e_errors.INFO,"bind_volume: volume %s, eod_cookie=%s" %(external_label,eod_cookie))
                tmp_vol_info['eod_cookie'] = eod_cookie
                tmp_vol_info['remaining_bytes'] = driver.get_stats()['remaining_bytes']
                self.vcc.set_remaining_bytes( external_label,
                                         tmp_vol_info['remaining_bytes'],
                                         tmp_vol_info['eod_cookie'],
                                         0,0,0,0,None )

            driver.close()
            # update again, after all is said and done
            self.vol_info.update( tmp_vol_info )

        elif external_label != self.vol_info['external_label']:
            self.vol_info['err_external_label'] = external_label
            self.fatal_enstore("unbind label %s before read/write label %s"%
                               (self.vol_info['external_label'],external_label) )
            return 'NOTAPE' # generic, not read or write specific

        return e_errors.OK  # bind_volume


    def forked_write_to_hsm( self, ticket ):
        # child or single process???
        Trace.log(e_errors.INFO,'WRITE_TO_HSM start %s'%(ticket,))

        self.lm_origin_addr = ticket['lm']['address']# who contacts me directly

        # First, call the user (to see if they are still there)
        sts = self.get_usr_driver(ticket )
        if sts == 'error':
            return e_errors.ENCP_GONE


        ##Don't allow null movers to write to any pnfs path not containing /NULL/
        if self.config['driver']=='NullDriver':
            fname = ticket['wrapper'].get("pnfsFilename",'')
            if "NULL" not in string.split(fname,'/'):
                ticket['status']=(e_errors.USERERROR, "NULL not in destination path")
                self.send_user_done( ticket, e_errors.USERERROR, "NULL not in destination path" )
                return e_errors.USERERROR

        t0 = time.time()
        sts = self.bind_volume( ticket['fc']['external_label'] )
        ticket['times']['mount_time'] = time.time() - t0
        if sts != e_errors.OK:
            # add CLOSING DATA SOCKET SO ENCP DOES NOT GET 'Broken pipe'
            self.usr_driver.close()
            # make write specific and ...
            sts = getattr(e_errors,"WRITE_"+sts)
            self.send_user_done( ticket, sts )
            return sts



        external_label = self.vol_info['external_label']
        sts = self.vcc.set_writing(external_label)
        if sts['status'][0] != "ok":
            # add CLOSING DATA SOCKET SO ENCP DOES NOT GET 'Broken pipe'
            self.usr_driver.close()
            self.send_user_done( ticket, e_errors.WRITE_NOTAPE )
            return e_errors.WRITE_NOTAPE


        Trace.log(e_errors.INFO,"OPEN_FILE_WRITE %s: %s"%
                  (external_label,self.vol_info['eod_cookie']))
        # open the hsm file for writing
        try:
            # if forked, our eod info is not correct (after previous write)
            # WE COULD SEND EOD STATUS BACK, then we could test/check our
            # info against the vol_clerks
            driver = self.driver_class.open( self.config['device'], 'a+' )
            self.no_transfers = self.no_transfers + 1
            t0 = time.time()
            # vol_info may be 'none' - seek can handle that and
            # tell will convert it.
            driver.seek( self.vol_info['eod_cookie'] )
            self.vol_info['eod_cookie'] = driver.tell()

            ticket['times']['seek_time'] = time.time() - t0

            fast_write = 1

            # create the wrapper instance (could be different for different
            # tapes) so it can save data between pre and post
            wrapper=wrapper_selector.select_wrapper(ticket['wrapper']['type'])
            if wrapper == None:
                raise errno.errorcode[errno.EINVAL], "Invalid wrapper %s"%(ticket['wrapper']['type'],)

            Trace.log(e_errors.INFO,"WRAPPER.WRITE %s"%(self.vol_info['external_label'],))
            t0 = time.time()
            wrapper.blocksize = self.vol_info['blocksize']
            wrapper.write_pre_data( driver, ticket['wrapper'] )

            # should not be getting this from wrapper sub-ticket
            file_bytes = ticket['wrapper']['size_bytes']
            san_bytes = ticket['wrapper']['sanity_size']
            if file_bytes < san_bytes: san_bytes = file_bytes
            san_crc = driver.fd_transfer( self.usr_driver.fileno(),
                                  san_bytes, self.crc_flag, 0 )
            sanity_cookie = (san_bytes, san_crc)
            if file_bytes > san_bytes:
                file_crc = driver.fd_transfer( self.usr_driver.fileno(),
                                       file_bytes-san_bytes, self.crc_flag,
                                       san_crc )
            else: file_crc = san_crc

            Trace.log(e_errors.INFO,'done with write fd_transfers')

            wrapper.write_post_data( driver, file_crc )
            ticket['times']['transfer_time'] = time.time() - t0
            t0 = time.time()
            driver.writefm()
            ticket['times']['eof_time'] = time.time() - t0

            location_cookie = self.vol_info['eod_cookie']
            eod_cookie = driver.tell()
            if driver.loc_compare(eod_cookie,location_cookie) != 1:
                raise MoverError, "bad eod"
            t0 = time.time()
            stats = driver.get_stats()
            ticket['times']['get_stats_time'] = time.time() - t0
            driver.close()			# b/c of fm above, this is purely sw.

        #except EWHATEVER_NET_ERROR:
        except (FTT.error, ETransfer.error):
            exc,msg,tb=sys.exc_info()
            Trace.log( e_errors.ERROR,
                       'FTT or Etransfer exception: %s %s '%(exc,msg))

            if msg.args[0] == 'fd_transfer - read EOF unexpected':
                # assume encp dissappeared
                return e_errors.ENCP_GONE 

            else:
                wr_err,rd_err,wr_access,rd_access = (1,0,1,0)
                self.vcc.update_counts( self.vol_info['external_label'],
                                   wr_err, rd_err, wr_access, rd_access )
                self.usr_driver.close()
                self.send_user_done( ticket, e_errors.WRITE_ERROR )
                return e_errors.WRITE_ERROR 


        except driver.SeekError:
            Trace.log( e_errors.ERROR, "seek error during write" )
            wr_err,rd_err,wr_access,rd_access = (1,0,1,0)
            self.vcc.update_counts( self.vol_info['external_label'],
                               wr_err, rd_err, wr_access, rd_access )
            self.usr_driver.close()
            self.send_user_done(  ticket, e_errors.WRITE_ERROR )
            return  e_errors.WRITE_ERROR

        except:
            e_errors.handle_error()
            wr_err,rd_err,wr_access,rd_access = (1,0,1,0)
            self.vcc.update_counts( self.vol_info['external_label'],
                               wr_err, rd_err, wr_access, rd_access )
            self.usr_driver.close()
            self.send_user_done( ticket, e_errors.WRITE_ERROR )
            return e_errors.WRITE_ERROR 


        # we've read the file from user, shut down data transfer socket
        self.usr_driver.close()

        # Tell volume server & update database
        remaining_bytes = stats['remaining_bytes']
        wr_err = stats['wr_err']
        rd_err = stats['rd_err']
        wr_access = 1
        rd_access = 0

        fc_ticket = {'location_cookie':location_cookie,
                     'size':file_bytes,
                     'sanity_cookie':sanity_cookie,
                     'external_label':self.vol_info['external_label'],
                     'complete_crc':file_crc}


        ## XXX HACK:  store 0 to database if mover is NULL
        if self.mvr_config['driver']=='NullDriver':
            fc_ticket['complete_crc']=0L
            fc_ticket['sanity_cookie']=(sanity_cookie[0],0L)

        rsp = self.fcc.new_bit_file( {'work':"new_bit_file",
                                      'fc'  : fc_ticket
                                      } )

        if rsp['status'][0] != e_errors.OK:
            Trace.log( e_errors.ERROR,
                       "XXXXXXXXXXXenstore software error" )
            ticket["fc"]["bfid"] = None

        ticket['fc'] = rsp['fc']

        ##XXX HACK set crcs back to real values before replying to encp
        ticket['fc']['sanity_cookie'] = sanity_cookie
        ticket['fc']['complete_crc'] = file_crc




        ticket['vc'].update( self.vcc.set_remaining_bytes(self.vol_info['external_label'],
                                                     remaining_bytes,
                                                     eod_cookie,
                                                     wr_err,rd_err, # added to total
                                                     wr_access,rd_access,
                                                     ticket["fc"]["bfid"]) )
        self.vol_info.update( ticket['vc'] )


        Trace.log(e_errors.INFO,"WRITE DONE %s"%(ticket,))

        self.send_user_done( ticket, e_errors.OK )

        return  e_errors.OK 


    def forked_read_from_hsm( self, ticket ):

        Trace.log(e_errors.INFO,"READ_FROM_HSM start %s"%(ticket,))

        self.lm_origin_addr = ticket['lm']['address']# who contacts me directly

        # First, call the user (to see if they are still there)
        sts = self.get_usr_driver( ticket )
        if sts == "error":
            return e_errors.ENCP_GONE 


        t0 = time.time()
        sts = self.bind_volume( ticket['fc']['external_label'] )
        ticket['times']['mount_time'] = time.time() - t0
        if sts != e_errors.OK:
            # add CLOSING DATA SOCKET SO ENCP DOES NOT GET 'Broken pipe'
            self.usr_driver.close()
            # make read specific and ...
            sts = getattr(e_errors, "READ_"+sts )
            self.send_user_done( ticket, sts )
            return sts


        # space to where the file will begin and save location
        # information for where future reads will have to space the drive to.

        # setup values before transfer
        media_error = 0
        drive_errors = 0
        bytes_sent = 0			# reset below, BUT not used afterwards!!!!!!!!!!!!!
        user_file_crc = 0		# reset below, BUT not used afterwards!!!!!!!!!!!!!

        Trace.log(e_errors.INFO,"OPEN_FILE_READ %s:%s"%
                  (ticket['fc']['external_label'],ticket['fc']['location_cookie']))

        # open the hsm file for reading and read it
        try:
            driver = self.driver_class.open( self.config['device'], 'r' )
            self.no_transfers = self.no_transfers + 1

            t0 = time.time()
            driver.seek( ticket['fc']['location_cookie'] )
            ticket['times']['seek_time'] = time.time() - t0

            # create the wrapper instance (could be different for different tapes)
            wrapper=wrapper_selector.select_wrapper(self.vol_info['wrapper'])
            if wrapper == None:
                wrapper=wrapper_selector.select_wrapper("cpio_odc")
                if wrapper == None:
                    raise errno.errorcode[errno.EINVAL], "Invalid wrapper"

            Trace.log(e_errors.INFO,"WRAPPER.READ %s"%(ticket['fc']['external_label'],))
            if ticket['fc']['sanity_cookie'][1] == None:# when reading...
                crc_flag = None
            else: crc_flag = self.crc_flag
            t0 = time.time()
            wrapper.read_pre_data( driver, ticket )
            san_crc = driver.fd_transfer( self.usr_driver.fileno(),
                                  ticket['fc']['sanity_cookie'][0],
                                  self.crc_flag,
                                  0 )
            # check the san_crc!!!
            if self.crc_flag != None:
                sanity_cookie=ticket['fc']['sanity_cookie']
                if sanity_cookie != None:
                    if san_crc != sanity_cookie[1]:
                        Trace.log(e_errors.ERROR,
                                  'Mover CRC mismatch: (sanity CRC): file clerk has %s, we have %s'%(
                            sanity_cookie[1],san_crc))


            if (ticket['fc']['size']-ticket['fc']['sanity_cookie'][0]) > 0:
                user_file_crc = driver.fd_transfer( self.usr_driver.fileno(),
                                            ticket['fc']['size']-ticket['fc']['sanity_cookie'][0],
                                            self.crc_flag, san_crc )
            else:
                user_file_crc = san_crc


            Trace.log(e_errors.INFO,'done with read fd_transfers')



            # This if block is a place holder for the code that should
            # be executed when CRCs don't match.
            if     self.crc_flag != None:
                complete_crc=ticket['fc']['complete_crc']
                if complete_crc != None:
                    if user_file_crc != complete_crc:
                        Trace.log(e_errors.ERROR,'Mover CRC mismatch: file clerk has %s, we have %s'%(
                            user_file_crc,complete_crc))


            tt = {'data_crc':ticket['fc']['complete_crc']}
            wrapper.read_post_data( driver, tt )
            ticket['times']['transfer_time'] = time.time() - t0

            stats = self.driver_class.get_stats()
            # close hsm file
            driver.close()
            #self.store_statistics(dismount, driver)
            wr_err,rd_err       = stats['wr_err'],stats['rd_err']
            wr_access,rd_access = 0,1
        #except errno.errorcode[errno.EPIPE]: # do not know why I can not use just 'EPIPE'
        except (FTT.error, ETransfer.error, IOError):
            exc,msg,tb=sys.exc_info()
            # XXX
            # Check for a broken pipe.

            if msg.args[2] == "Broken pipe": #XXX should compare ints rather than strings
                err = e_errors.BROKENPIPE
                ticket['status']=(e_errors.BROKENPIPE,None)
            else:
                err = e_errors.READ_ERROR
            e_errors.handle_error()
            self.usr_driver.close()
            self.send_user_done( ticket, err)
            return e_errors.OK
        except driver.SeekError:
            Trace.log( e_errors.ERROR, "seek error during read" )
            wr_err,rd_err,wr_access,rd_access = (0,1,0,1)
            self.vcc.update_counts( self.vol_info['external_label'],
                               wr_err, rd_err, wr_access, rd_access )
            self.usr_driver.close()
            self.send_user_done( ticket, e_errors.READ_ERROR )
            return  e_errors.READ_ERROR 


        except:
            # unanticipated exception: guess a cause and hope we can continue
            e_errors.handle_error()
            media_error = 1
            wr_err,rd_err,wr_access,rd_access = (0,1,0,1)

        # we've sent the hsm file to the user, shut down data transfer socket
        self.usr_driver.close()

        # get the error/mount counts and update database
        xx = self.vcc.update_counts( self.vol_info['external_label'],
                                wr_err,rd_err,wr_access,rd_access )
        self.vol_info.update( xx )

        # if media error, mark volume readonly, unbind it & tell user to retry
        if media_error :
            self.vcc.set_system_readonly( self.vol_info['external_label'] )
            self.send_user_done( ticket, e_errors.READ_ERROR )
            return e_errors.READ_ERROR 

        # drive errors are bad:  unbind volume it & tell user to retry
        elif drive_errors :
            self.vcc.set_hung( self.vol_info['external_label'] )
            self.send_user_done( ticket, e_errors.READ_ERROR )
            return e_errors.READ_ERROR

        # All is well - read has finished correctly

        # add some info to user's ticket
        ticket['vc'] = self.vol_info
        ticket['vc']['current_location'] = ticket['fc']['location_cookie']

        Trace.log(e_errors.INFO,'READ DONE %s'%(ticket,))

        self.send_user_done( ticket, e_errors.OK )
        return  e_errors.OK



    # data transfer takes place on tcp sockets, so get ports & call user
    # Info is added to ticket
    def get_usr_driver( self, ticket ):
        try:
            if self.local_mover_enable and ticket['wrapper']['machine']==os.uname():
                if ticket['work'] == 'read_from_hsm':
                    mode = 'w'
                    fname = ticket['wrapper']['fullname']+'.'+ticket['unique_id']
                    # do not worry about umask!?!
                    ticket['mover']['lcl_fname'] = fname# to chown after etransfer
                else:
                    mode = 'r'
                    fname = ticket['wrapper']['fullname']
                try:
                    self.usr_driver = open( fname, mode )
                    ticket['mover']['local_mover'] = 1
                except: pass

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
                data_ip=self.config.get("data_ip",None)
                host, port, listen_socket = callback.get_data_callback(fixed_ip=data_ip)
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

            return 'ok'
        except: return 'error'


    # create ticket that says we are idle
    def idle_mover_next( self ):
        ret = {'work'   : 'idle_mover',
                'mover'  : self.name,
                'state'  : state_names[self.state],
                'address': (self.config['hostip'],self.config['port'])}
        return ret
    # create ticket that says we have bound volume x
    def have_bound_volume_next( self ):
        return { 'work'   : 'have_bound_volume',
                 'mover'  : self.name,
                 'state'  : state_names[self.state],
                 'address': (self.config['hostip'],self.config['port']),
                 'vc'     : self.vol_info }

    # create ticket that says we need to unbind volume x
    def unilateral_unbind_next( self, error_info ):
        # This method used to automatically unbind, but now it does not because
        # there are some errors where the tape should be left in the drive. So
        # unilateral_unbind now just means that there was an error.
        # The response to this command can be either 'nowork' or 'unbind'
        return {'work'           : 'unilateral_unbind',
                'mover'          : self.name,
                'state'          : state_names[self.state],
                'address'        : (self.config['hostip'],self.config['port']),
                'external_label' : self.fc['external_label'],
                'status'         : (error_info,None)}

    def debug_status( self, ticket ):
	out_ticket = {'status':(e_errors.OK,None)}
	out_ticket['vol_info'] = self.vol_info
	self.reply_to_caller( out_ticket )
	return

    # get a port for the data transfer
    def get_user_sockets(self, ticket):
        host, port, listen_socket = callback.get_callback()
        listen_socket.listen(4)
        ticket["callback_host"] = host
        ticket["callback_port"] = port
        control_socket = callback.user_callback_socket(ticket)
        data_socket, address = listen_socket.accept()
        if 0: #XXX CGW
            data_socket.setsockopt(socket.SOL_SOCKET,socket.SO_DONTROUTE,1)
        interface = hostaddr.interface_name(host)
        if interface:
            Trace.log(16,"bindtodev %s %s %s",host,address,interface)
            if 0: #XXX CGW
                status = socket_ext.bindtodev(data_socket.fileno(),interface)
            else:
                status=0
            if status and status != errno.ENOSYS:
                Trace.log(16,"bindtodev %s",os.strerror(status))
        listen_socket.close()
        return control_socket, data_socket

    def clean_drive( self, ticket ):
        ticket["status"] = (e_errors.OK, None)
	if self.config['do_fork']:
            if self.state == IDLE:
                self.do_fork( ticket, 'c' )
                if self.pid != 0:
                    ticket["status"] = (e_errors.OK, None)
                    self.reply_to_caller(ticket) # reply now to avoid deadlocks
                    return {} #parent
            else:
                self.reply_to_caller( {'status': (e_errors.INPROGRESS, "Mover is busy")} )
                return {}
	# child or single process???
	Trace.log(e_errors.INFO,'CLEAN start %s'%(ticket,))
        
        control_socket, data_socket = self.get_user_sockets(ticket)
        rt =self.mcc.doCleaningCycle(self.config)
	out_ticket = {'status':(rt['status'][0],rt['status'][2])}
        callback.write_tcp_obj(data_socket,out_ticket)
        data_socket.close()
        callback.write_tcp_obj(control_socket,ticket)
        control_socket.close()
        
        return
    
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
		 'mode'         : self.mode,
                 'external_label': self.external_label,
		 'time_stamp'   : time.time(),
                 }

	self.reply_to_caller( tick )
	return

    def start_draining(self, ticket):	    # put itself into draining state
        if (self.state == IDLE and
            (self.mode == 'u' or self.mode == '')): self.mode = 'f'
        self.state = 'draining'
        self.create_lockfile()
	out_ticket = {'status':(e_errors.OK,None)}
	self.reply_to_caller( out_ticket )
	return

    def stop_draining(self, ticket):	    # put itself into draining state
        if self.state != 'draining':
            out_ticket = {'status':("EPROTO","Not in draining state")}
            self.reply_to_caller( out_ticket )
            return
        
        out_ticket = {'status':(e_errors.OK,None)}
        self.reply_to_caller( out_ticket )
        self.remove_lockfile()
        os._exit(0) # inq. or reboot will restart us.

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
        
    def update_client_info( self, ticket ):
	Trace.log( e_errors.INFO,  "update_client_info - pid: %s, ticket['pid']=%s"%
                   (self.pid,ticket['pid']))
        self.reply_to_caller({'status':(e_errors.OK, None)})
        if self.pid != ticket['pid']:
            # assume previous "No child processes" exception
            Trace.log(e_errors.INFO, "update_client_info: self.pid=%s, ticket[pid]=%s"%(
                self.pid, ticket['pid']))
            return
        if self.mode == 'c':     # cleaning
            if self.state != 'draining': self.state = 'idle'
            # cleaning returned
            # tell all known library managers that mover is idle
            for lm in self.config['library']:# should be libraries
                address = (self.lib_config[lm]['hostip'],self.lib_config[lm]['port'])
                next_req_to_lm = self.idle_mover_next()
                self.do_next_req_to_lm(next_req_to_lm, address )
            return
        # make 'hook' for state command to identify completion of request
        # in the draining state
        if self.state == 'draining': self.mode = 'f'
	self.vol_info = ticket['vol_info']
	self.no_transfers = ticket['no_transfers']
	self.driver_class.blocksize = ticket['driver_class']['blocksize']
	self.driver_class.remaining_bytes = ticket['driver_class']['remaining_bytes']
	self.driver_class.vol_label = ticket['driver_class']['vol_label']
	self.driver_class.cur_loc_cookie = ticket['driver_class']['cur_loc_cookie']
	wait = 0
	next_req_to_lm = self.get_state_build_next_lm_req( wait, ticket['exit_status'] )
	self.do_next_req_to_lm( next_req_to_lm, ticket['address'] )




    def do_next_req_to_lm( self, next_req_to_lm, address ):
        while next_req_to_lm != {} and next_req_to_lm != None:
            rsp_ticket = self.udpc.send(  next_req_to_lm, address )
            # STATE COULD BE 'BUSY' OR 'OFFLINE' OR 'DRAINING'
            if self.state != 'idle' and self.state != 'draining' and rsp_ticket['work'] != 'nowork':
                # CHANGE THIS TO Trace.alarm???
                Trace.log( e_errors.ERROR,
                           'FATAL ENSTORE - libm gave busy or offline move work %s'%
                           (rsp_ticket['work'],) )
                if self.config['execution_env'][0:5] == 'devel':
                    Trace.log( e_errors.ERROR, 'FATAL ENSTORE in devel env. => crazed' )
                    ##print 'FATAL ENSTORE in devel env. => crazed (check the log!)'
                    self.state = 'crazed'
                Trace.log( e_errors.ERROR, 'mover changing work %s to "nowork"'%
                           (rsp_ticket['work'],) )
                rsp_ticket['work'] = 'nowork'

            # Exceptions are caught (except block) in dispatching_worker.py.
            # The reply is the command (i.e the network is the computer).
            try: client_function = rsp_ticket['work']
            except KeyError:
                # CHANGE THIS TO Trace.alarm???
                Trace.log( e_errors.ERROR,
                           'FATAL ENSTORE - invalid rsp from libm: %s'%(rsp_ticket,) )
                if self.config['execution_env'][0:5] == 'devel':
                    Trace.log( e_errors.ERROR, 'FATAL ENSTORE in devel env. => crazed' )
                    ##print 'FATAL ENSTORE in devel env. => crazed (check the log!)'
                    self.state = 'crazed'
                Trace.log( e_errors.ERROR, 'mover changing invalid rsp to "nowork"' )
                client_function = 'nowork'
            method = getattr(self,client_function,None)
            if not method:
                Trace.log( e_errors.ERROR, "mover: cannot dispatch request %s"%(client_function,))
            next_req_to_lm = method( rsp_ticket )

    def get_state_build_next_lm_req( self, wait, exit_status ):
        if self.pid:
            try: pid, status = os.waitpid( self.pid, wait )
            except:
                exc, msg, tb = sys.exc_info()
                if wait == 0: status = exit_status
                else:         status = m_err.index(e_errors.OK)<<8 # assume success???
                pid = self.pid
                format = 'waitpid-for pid:%s wait:%s exit_status:%s exc_info:%s%s'% (
                    self.pid, wait, exit_status, exc, msg)

                Trace.log( e_errors.WARNING,format)


            if pid == self.pid:
                self.pid = 0
                if self.state != 'crazed':
                    if self.state != 'draining':
                        self.state = 'idle'
                exit_status = status>>8
                next_req_to_lm = self.status_to_request( exit_status )
            else:
                next_req_to_lm = self.have_bound_volume_next()


        else:
            if self.vol_info['external_label'] == '':
                next_req_to_lm = self.idle_mover_next()
            else:
                next_req_to_lm = self.have_bound_volume_next()


        return next_req_to_lm

    def status_to_request( self, exit_status ):
        next_req_to_lm = {}
        if   m_err[exit_status] == e_errors.OK:
            if self.vol_info['external_label'] == '':
                next_req_to_lm = self.idle_mover_next()
            else:
                next_req_to_lm = self.have_bound_volume_next()

            if next_req_to_lm['state'] != 'draining':
                next_req_to_lm['state'] = 'idle'
        elif m_err[exit_status] == e_errors.ENCP_GONE:
            if self.vol_info['external_label'] == '':
                # This is the case where a just started mover determines ENCP_GONE
                # before mounting the volume, so the mover does not have a volume
                next_req_to_lm = self.idle_mover_next()
            else:
                next_req_to_lm = self.have_bound_volume_next()

            if next_req_to_lm['state'] != 'draining':
                next_req_to_lm['state'] = 'idle'
        elif m_err[exit_status] == e_errors.WRITE_ERROR:
            next_req_to_lm = self.offline_drive(m_err[exit_status] )
        elif m_err[exit_status] == e_errors.READ_ERROR:
            if self.read_error[1]:
                next_req_to_lm = self.offline_drive(m_err[exit_status])
            else:
                next_req_to_lm = self.unilateral_unbind_next( m_err[exit_status] )


        elif m_err[exit_status] in (e_errors.WRITE_NOTAPE,
                                    e_errors.WRITE_TAPEBUSY,
                                    e_errors.READ_NOTAPE,
                                    e_errors.READ_TAPEBUSY):
            next_req_to_lm = self.freeze_tape( m_err[exit_status] )

        elif m_err[exit_status] in (e_errors.WRITE_BADSPACE,
                                    e_errors.READ_BADLOCATE,
                                    e_errors.READ_COMPCRC,
                                    e_errors.READ_EOT,
                                    e_errors.READ_EOD,
                                    e_errors.UNMOUNT):
            next_req_to_lm = self.freeze_tape_in_drive( m_err[exit_status] )
        elif m_err[exit_status] in (e_errors.WRITE_BADMOUNT,
                                    e_errors.WRITE_UNLOAD,
                                    e_errors.READ_BADMOUNT,
                                    e_errors.WRITE_BADSWMOUNT,
                                    e_errors.READ_BADSWMOUNT,
                                    e_errors.READ_UNLOAD):
            next_req_to_lm = self.unilateral_unbind_next( m_err[exit_status] )
        elif m_err[exit_status] in (e_errors.READ_VOL1_READ_ERR,
                                    e_errors.WRITE_VOL1_READ_ERR):
            # we don't know if it's a tape error or a drive error.  take the drive offline.
            next_req_to_lm = self.freeze_tape_in_drive( m_err[exit_status])

        elif m_err[exit_status] in (e_errors.WRITE_VOL1_MISSING,
                                    e_errors.WRITE_VOL1_WRONG,
                                    e_errors.READ_VOL1_MISSING,
                                    e_errors.READ_VOL1_WRONG):
            next_req_to_lm = self.freeze_tape( m_err[exit_status] )

        elif m_err[exit_status] == e_errors.USERERROR:
            next_req_to_lm = self.idle_mover_next()

        else:
            # new error
            Trace.log( e_errors.ERROR, 'FATAL ERROR - MOVER - unknown transfer status - fix me now' )
            while 1: time.sleep( 100 )		# let the Inquisitor restart us

        return next_req_to_lm



        
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
    
    mover =  Mover( (intf.config_host, intf.config_port), intf.name )
    print mover.server_address
    mover.serve_forever()

    Trace.log(e_errors.INFO, 'ERROR returned from serve_forever')
    



