import Trace
Trace.init("VC client")
Trace.trace(6,"GO")
###############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
import sys
import string
import time
import errno

# enstore imports
import callback
import dict_to_a
import interface
import generic_client
import generic_cs
import backup_client
import configuration_client
import udp_client
import db
import Trace
import pdb
import e_errors

class VolumeClerkClient(generic_client.GenericClient,\
                        backup_client.BackupClient):

    def __init__( self, csc=0, verbose=0, host=interface.default_host(), \
                  port=interface.default_port(), servr_addr=None ):
        Trace.trace( 10, '{__init__ csc='+str(csc)+' verbose='+str(verbose)+\
		     ' host='+str(host)+' port='+str(port)+' servr_addr='+str(servr_addr) )
	self.print_id = "VCC"
        self.u = udp_client.UDPClient()
	self.verbose = verbose
        configuration_client.set_csc( self, csc, host, port, verbose )
        ticket = self.csc.get( "volume_clerk" )
	if servr_addr != None: self.servr_addr = servr_addr
	else:                  self.servr_addr = (ticket['hostip'],ticket['port'])
	try:    self.print_id = ticket['logname']
        except: pass
        Trace.trace(10,'}__init__ u='+str(self.u))

    # send the request to the volume clerk server and then send answer to user
    def send (self, ticket,  rcv_timeout=0, tries=0):
        Trace.trace( 16, '{send to volume clerk '+str(self.servr_addr) )
        x = self.u.send( ticket, self.servr_addr, rcv_timeout, tries )
        Trace.trace( 16, '}send '+str(x) )
        return x

    # add a volume to the stockpile
    def addvol(self,
               library,               # name of library media is in
               file_family,           # volume family the media is in
               media_type,            # media
               external_label,        # label as known to the system
               capacity_bytes,        #
               remaining_bytes,       #
               eod_cookie  = "none",  # code for seeking to eod
               user_inhibit  = "none",# "none" | "readonly" | "NOACCESS"
               error_inhibit = "none",# "none" | "readonly" | "NOACCESS" | "writing"
                                      # lesser access is specified as
                                      #       we find media errors,
                                      # writing means that a mover is
                                      #       appending or that a mover
                                      #       crashed while writing
               last_access = -1,      # last accessed time
               first_access = -1,     # first accessed time
               declared = -1,         # time volume was declared to system
               sum_wr_err = 0,        # total number of write errors
               sum_rd_err = 0,        # total number of read errors
               sum_wr_access = 0,     # total number of write mounts
               sum_rd_access = 0,     # total number of read mounts
               wrapper = "cpio_odc",  # kind of wrapper for volume
               blocksize = -1         # blocksize (-1 =  media type specifies)
               ):
        Trace.trace( 6, 'add_vol label=%s'%external_label )
        ticket = { 'work'            : 'addvol',
                   'library'         : library,
                   'file_family'     : file_family,
                   'media_type'      : media_type,
                   'external_label'  : external_label,
                   'capacity_bytes'  : capacity_bytes,
                   'remaining_bytes' : remaining_bytes,
                   'eod_cookie'      : eod_cookie,
                   'user_inhibit'    : user_inhibit,
                   'error_inhibit'   : error_inhibit,
                   'last_access'     : last_access,
                   'first_access'    : first_access,
                   'declared'        : declared,
                   'sum_wr_err'      : sum_wr_err,
                   'sum_rd_err'      : sum_rd_err,
                   'sum_wr_access'   : sum_wr_access,
                   'sum_rd_access'   : sum_rd_access,
                   'wrapper'         : wrapper,
                   'blocksize'       : blocksize }
        x = self.send(ticket)
        return x


    # delete a volume from the stockpile
    def delvol(self, external_label,force=0):
        Trace.trace( 6, 'del_vol label=%s'%external_label )
        ticket= { 'work'           : 'delvol',
                  'external_label' : external_label,
                  'force'          : force }
        x = self.send(ticket)
        return x


    # get a list of all volumes
    def get_vols(self):
        Trace.trace(20,'{get_vols R U CRAZY?')
        # get a port to talk on and listen for connections
        host, port, listen_socket = callback.get_callback()
        listen_socket.listen(4)
        ticket = {"work"         : "get_vols",
                  "callback_addr" : (host, port),
                  "unique_id"    : time.time() }
        # send the work ticket to the library manager
        ticket = self.send(ticket)
        if ticket['status'][0] != e_errors.OK:
            Trace.log( e_errors.ERROR,
		       'vcc.get_vols: sending ticket: %s'%ticket )
            raise errno.errorcode[errno.EPROTO],"vcc.get_vols: sending ticket"\
                  +str(ticket)

        # We have placed our request in the system and now we have to wait.
        # All we  need to do is wait for the system to call us back,
        # and make sure that is it calling _us_ back, and not some sort of old
        # call-back to this very same port. It is dicey to time out, as it
        # is probably legitimate to wait for hours....
        while 1:
            control_socket, address = listen_socket.accept()
            new_ticket = callback.read_tcp_socket(control_socket, "volume"+\
                                  "clerk client get_vols,  vc call back")
            if ticket["unique_id"] == new_ticket["unique_id"]:
                listen_socket.close()
                break
            else:
	        self.enprint("get_vols - imposter called us back, trying again")
                control_socket.close()
        ticket = new_ticket
        if ticket["status"][0] != e_errors.OK:
            Trace.trace(0,"vcc.get_vols: "\
                  +"1st (pre-work-read) volume clerk callback on socket "\
                  +str(address)+" failed to setup transfer: "\
                  +ticket["status"])
            raise errno.errorcode[errno.EPROTO],"vcc.get_vols: "\
                  +"1st (pre-work-read) volume clerk callback on socket "\
                  +str(address)+", failed to setup transfer: "\
                  +"ticket[\"status\"]="+ticket["status"]

        # If the system has called us back with our own  unique id, call back
        # the library manager on the library manager's port and read the
        # work queues on that port.
        data_path_socket = callback.volume_server_callback_socket(ticket)
        ticket= callback.read_tcp_socket(data_path_socket, "volume clerk"\
                  +"client get_vols, vc final dialog")

        volumes = []
        while 1:
          msg=callback.read_tcp_buf(data_path_socket,"volume clerk "+\
                                    "client get_vols, reading worklist")
          if len(msg)==0:
              break
          volumes.append(msg)
        
	generic_cs.enprint(string.join(volumes,''))
        worklist = ticket
        data_path_socket.close()


        # Work has been read - wait for final dialog with volume clerk
        done_ticket = callback.read_tcp_socket(control_socket, "volume clerk"\
                  +"client get_vols, vc final dialog")
        control_socket.close()
        if done_ticket["status"][0] != e_errors.OK:
            Trace.trace(0,"vcc.get_vols "\
                  +"2nd (post-work-read) volume clerk callback on socket "\
                  +str(address)+", failed to transfer: "\
                  +ticket["status"])
            raise errno.errorcode[errno.EPROTO],"vcc.get_vols "\
                  +"2nd (post-work-read) volume clerk callback on socket "\
                  +str(address)+", failed to transfer: "\
                  +"ticket[\"status\"]="+ticket["status"]
        Trace.trace(20,'}get_vols')
        return worklist

    # what is the current status of a specified volume?
    def inquire_vol(self, external_label):
        Trace.trace(10,'inquire_vol label='+str(external_label))
        ticket= { 'work'           : 'inquire_vol',
                  'external_label' : external_label }
        x = self.send(ticket)
        Trace.trace(10,'}inquire_vol '+str(x))
        return x

    # move a volume to a new library
    def new_library(self, external_label,new_library):
        Trace.trace(10,'new_library label='+str(external_label)+' new_library='+str(new_library))
        ticket= { 'work'           : 'new_library',
                  'external_label' : external_label,
                  'new_library'    : new_library}
        x = self.send(ticket)
        Trace.trace(10,'}new_library '+str(x))
        return x

    # we are using the volume
    def set_writing(self, external_label):
        Trace.trace(10,'set_writing label='+str(external_label))
        ticket= { 'work'           : 'set_writing',
                  'external_label' : external_label }
        x = self.send(ticket)
        Trace.trace(10,'}set_writing '+str(x))
        return x

    # we are using the volume
    def set_system_readonly(self, external_label):
        Trace.trace(10,'set_system_readonly label='+str(external_label))
        ticket= { 'work'           : 'set_system_readonly',
                  'external_label' : external_label }
        x = self.send(ticket)
        Trace.trace(10,'}set_system_readonly '+str(x))
        return x

    # mark volume as noaccess
    def set_system_noaccess(self, external_label):
        Trace.trace(10,'set_system_noaccess label='+str(external_label))
        ticket= { 'work'           : 'set_system_noaccess',
                  'external_label' : external_label }
        x = self.send(ticket)
        Trace.trace(10,'}set_system_noaccess '+str(x))
        return x

    # mark volume as noaccess
    def set_at_mover(self, external_label, flag, mover, *force):
        Trace.trace(10,'set_at_mover label='+str(external_label)+\
		    ' flag='+str(flag)+' mover='+str(mover))
	if force: f = 1
	else: f = 0
        ticket= { 'work'           : 'set_at_mover',
                  'external_label' : external_label,
		  'at_mover' : (flag, mover),
		  'force'    : f}
        x = self.send(ticket)
	"""
	generic_cs.enprint("set_at_mover:VCC returned "+\
			   str(x['at_mover'])+str(x['status']), 
			   generic_cs.DEBUG)
        """

        Trace.trace(10,'}set_at_mover '+str(x))
        return x

    # get the state of the media changer for the volume
    def update_mc_state(self,external_label):
        Trace.trace( 6, 'vcc.update_mc_state label=%s'%external_label )
        ticket= { 'work'           : 'update_mc_state',
                  'external_label' : external_label }
        x = self.send(ticket)
        return x

    # clear any inhibits on the volume
    def clr_system_inhibit(self,external_label):
        Trace.trace( 6, 'clr_system_inhibit label=%s'%external_label )
        ticket= { 'work'           : 'clr_system_inhibit',
                  'external_label' : external_label }
        x = self.send(ticket)
        return x

    # decrement the file count on a tape
    def decr_file_count(self,external_label, count=1):
        Trace.trace( 6, 'decr_file_count label=%s count=%s'%(external_label,count) )
        ticket= { 'work'           : 'decr_file_count',
                  'external_label' : external_label,
                  'count'          : count }
        x = self.send(ticket)
        return x

    # we are using the volume
    def set_hung(self, external_label):
        Trace.trace( 6, 'set_hung label=%s'%external_label )
        ticket= { 'work'           : 'set_hung',
                  'external_label' : external_label }
        x = self.send(ticket)
        return x

    # this many bytes left - update database
    def set_remaining_bytes(self, external_label,remaining_bytes,eod_cookie,
                            wr_err,rd_err,wr_access,rd_access):
        Trace.trace(10,'{set_remaining_bytes label='+str(external_label)+\
                    ' bytes='+str(remaining_bytes)+ ' wr_err='+\
                    str(wr_err)+' rd_err='+str(rd_err)+' wr_access='+\
                    str(wr_access)+' rd_access='+str(rd_access))
        ticket= { 'work'            : 'set_remaining_bytes',
                  'external_label'  : external_label,
                  'remaining_bytes' : remaining_bytes,
                  'eod_cookie'      : eod_cookie,
                  'wr_err'          : wr_err,
                  'rd_err'          : rd_err,
                  'wr_access'       : wr_access,
                  'rd_access'       : rd_access }
        x = self.send(ticket)
        return x


    # update the counts in the database
    def update_counts(self, external_label, wr_err,rd_err,wr_access,rd_access):
        Trace.trace(10,'{update_counts label='+str(external_label)+' wr_err='+\
                    str(wr_err)+' rd_err='+str(rd_err)+' wr_access='+\
                    str(wr_access)+' rd_access='+str(rd_access))
        ticket= { 'work'            : 'update_counts',
                  'external_label'  : external_label,
                  'wr_err'          : wr_err,
                  'rd_err'          : rd_err,
                  'wr_access'       : wr_access,
                  'rd_access'       : rd_access }
        x = self.send(ticket)
        Trace.trace(10,'}update_counts '+str(x))
        return x

    # which volume can we use for this library, bytes and file family and ...
    def next_write_volume (self, library, min_remaining_bytes,
                           file_family, wrapper, vol_veto_list,first_found):
        Trace.trace(10,'{next_write_volume lib='+str(library)+' bytes='+\
                    str(min_remaining_bytes)+' ff='+str(file_family)+\
                    " veto="+str(vol_veto_list)+' first_found='+\
                    str(first_found))
        ticket = { 'work'                : 'next_write_volume',
                   'library'             : library,
                   'min_remaining_bytes' : min_remaining_bytes,
                   'file_family'         : file_family,
		   'wrapper'             : wrapper,
                   'vol_veto_list'       : `vol_veto_list`,
                   'first_found'         : first_found }

        x = self.send(ticket)
        Trace.trace(10,'}next_write_volume '+str(x))
        return x

    # check if specific volume can be used for write
    def can_write_volume (self, library, min_remaining_bytes,
                           file_family, wrapper, external_label):
        Trace.trace(10,'{can_write_volume lib='+str(library)+' bytes='+\
                    str(min_remaining_bytes)+' ff='+str(file_family)+\
                    " volume="+str(external_label))
        ticket = { 'work'                : 'can_write_volume',
                   'library'             : library,
                   'min_remaining_bytes' : min_remaining_bytes,
                   'file_family'         : file_family,
		   'wrapper'             : wrapper,
                   'external_label'       : external_label }

        x = self.send(ticket)
        Trace.trace(10,'}can_write_volume '+str(x))
        return x

    # for the backward compatibility D0_TEMP
    def add_at_mover (self, external_label):
        Trace.trace(10,'{add_at_mover '+str(external_label))
        ticket = { 'work'                : 'add_at_mover',
                   'external_label'       : external_label }

        x = self.send(ticket)
        Trace.trace(10,'}add_at_mover '+str(x))
        return x
    # END D0_TEMP

class VolumeClerkClientInterface(generic_client.GenericClientInterface):

    def __init__(self):
        Trace.trace(10,'{__init__ vcci')
        self.alive_rcv_timeout = 0
        self.alive_retries = 0
        self.clrvol = 0
        self.statvol = ""
        self.backup = 0
        self.vols = 0
        self.nextvol = 0
        self.vol = ""
        self.addvol = 0
        self.delvol = 0
        self.force  = 0
        self.newlib = 0
        self.rdovol = 0
        self.noavol = 0
        self.decr_file_count = 0
	self.atmover = 0 # for the backward compatibility D0_TEMP
        generic_client.GenericClientInterface.__init__(self)
        Trace.trace(10,'}__init__ vcci')

    # define the command line options that are valid
    def options(self):
        Trace.trace(20,'{}options')
        return self.client_options()+\
               ["clrvol", "backup", "vols","nextvol","vol=","addvol","statvol=",
	        "delvol","newlib","rdovol","noavol","atmover","decr_file_count=","force"]

    # parse the options like normal but make sure we have necessary params
    def parse_options(self):
        Trace.trace(16,'{parse_options')
        interface.Interface.parse_options(self)
        if self.nextvol:
            if len(self.args) < 3:
                self.print_addvol_args()
                sys.exit(1)
        elif self.addvol:
            if len(self.args) < 6:
                self.print_addvol_args()
                sys.exit(1)
        elif self.delvol:
            if len(self.args) < 1:
                self.print_delvol_args()
                sys.exit(1)
        elif self.clrvol:
            if len(self.args) < 1:
                self.print_clr_inhibit_args()
                sys.exit(1)
        #elif self.statvol != "":
        #    if len(self.args) < 1:
	#        print "vcc.parse_options statvol self.args= ", self.args
        #        self.print_update_mc_state_args()
        #        sys.exit(1)
        elif self.rdovol:
            if len(self.args) < 1:
                self.print_set_system_readonly_args()
                sys.exit(1)
        elif self.noavol:
            if len(self.args) < 1:
                self.print_set_system_noaccess_args()
                sys.exit(1)
        elif self.newlib:
            if len(self.args) < 2:
                self.print_new_library_args()
                sys.exit(1)
        Trace.trace(16,'}parse_options')

    # print update_mc_state arguments
    def print_update_mc_state_args(self):
        Trace.trace(20,'{}vcc.print_update_mc_state_args')
        generic_cs.enprint("   update_mc_state arguments: volume_name")

    # print clr_inhibit arguments
    def print_clr_inhibit_args(self):
        Trace.trace(20,'{}print_clr_inhibit_args')
        generic_cs.enprint("   clr_inhibit arguments: volume_name")

    # print rdovol arguments
    def print_set_system_readonly_args(self):
        Trace.trace(20,'{}print_set_system_readonly_args')
        generic_cs.enprint("   rdovol arguments: volume_name")

    # print noavol arguments
    def print_set_system_noaccess_args(self):
        Trace.trace(20,'{}print_set_system_noaccess_args')
        generic_cs.enprint("   noavol arguments: volume_name")

    # print new library arguments
    def print_new_library_args(self):
        Trace.trace(20,'{}print_new_library_args')
        generic_cs.enprint("   newlib arguments: volume_name new_library_name")

    # print addvol arguments
    def print_addvol_args(self):
        Trace.trace(20,'{}print_addvol_args')
        generic_cs.enprint("   addvol arguments: library file_family media_type"\
              +", volume_name, volume_byte_capacity remaining_capacity")

    # print delvol arguments
    def print_delvol_args(self):
        Trace.trace(20,'{}print_delvol_args')
        generic_cs.enprint("   delvol arguments: volume_name")

    # print out our extended help
    def print_help(self):
        Trace.trace(20,'{print_help')
        interface.Interface.print_help(self)
        self.print_addvol_args()
        self.print_delvol_args()
        self.print_clr_inhibit_args()
        self.print_update_mc_state_args()
        self.print_set_system_readonly_args()
        self.print_set_system_noaccess_args()
        self.print_new_library_args()
        Trace.trace(16,'}print_help')


if __name__ == "__main__":
    Trace.init("VC client")
    Trace.trace( 6, 'vcc called with args: %s'%sys.argv )

    # fill in the interface
    intf = VolumeClerkClientInterface()

    # get a volume clerk client
    vcc = VolumeClerkClient(0, intf.verbose, intf.config_host,\
                            intf.config_port)
	
    if intf.alive:
        ticket = vcc.alive(intf.alive_rcv_timeout,intf.alive_retries)
	msg_id = generic_cs.ALIVE
    elif intf.got_server_verbose:
        ticket = vcc.set_verbose(intf.server_verbose, intf.alive_rcv_timeout,\
	                         intf.alive_retries)
	msg_id = generic_cs.CLIENT
    elif intf.backup:
        ticket = vcc.start_backup()
        db.do_backup("volume")
        ticket = vcc.stop_backup()
	msg_id = generic_cs.CLIENT
    elif intf.vols:
        ticket = vcc.get_vols()
	msg_id = generic_cs.CLIENT
    elif intf.nextvol:
        ticket = vcc.next_write_volume(intf.args[0], #library
                                       string.atol(intf.args[1]), #min_remaining_byte
                                       intf.args[2], #file_family
                                            [], #vol_veto_list
                                             1) #first_found
	msg_id = generic_cs.CLIENT
    elif intf.vol:
        ticket = vcc.inquire_vol(intf.vol)
	generic_cs.enprint(ticket, generic_cs.PRETTY_PRINT)
	msg_id = generic_cs.CLIENT
    elif intf.addvol:
        ticket = vcc.addvol(intf.args[0],              # library
                            intf.args[1],              # file family
                            intf.args[2],              # media type
                            intf.args[3],              # name of this volume
                            string.atol(intf.args[4]), # cap'y of vol (bytes)
                            string.atol(intf.args[5])) # rem cap'y of volume
	msg_id = generic_cs.CLIENT
    elif intf.newlib:
        ticket = vcc.new_library(intf.args[0],         # volume name
                                 intf.args[1])         # new library name
	msg_id = generic_cs.CLIENT
    elif intf.delvol:
        ticket = vcc.delvol(intf.args[0],intf.force)   # name of this volume
	msg_id = generic_cs.CLIENT
    elif intf.clrvol:
        ticket = vcc.clr_system_inhibit(intf.args[0])  # name of this volume
	msg_id = generic_cs.CLIENT
    elif intf.statvol != "":
        ticket = vcc.update_mc_state(intf.statvol)  # name of this volume
	try:
	    if intf.verbose :
	        generic_cs.enprint(ticket, generic_cs.PRETTY_PRINT)
	except:
	    pass
	msg_id = generic_cs.CLIENT
    elif intf.decr_file_count:
        ticket = vcc.decr_file_count(intf.args[0],string.atoi(intf.decr_file_count))
	msg_id = generic_cs.CLIENT
        try:
            if intf.verbose:
                generic_cs.enprint(ticket, generic_cs.PRETTY_PRINT)
        except:
            pass
    elif intf.rdovol:
        ticket = vcc.set_system_readonly(intf.args[0])  # name of this volume
	msg_id = generic_cs.CLIENT
    elif intf.noavol:
        ticket = vcc.set_system_noaccess(intf.args[0])  # name of this volume
	msg_id = generic_cs.CLIENT
    # D0_TEPM
    elif intf.atmover:
	ticket = vcc.add_at_mover (intf.args[0])
	generic_cs.enprint(ticket, generic_cs.PRETTY_PRINT)
	msg_id = generic_cs.CLIENT
    # END D0_TEMP
    else:
	intf.print_help()
        sys.exit(0)

    vcc.check_ticket(ticket, msg_id)
