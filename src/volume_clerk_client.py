import Trace
Trace.init("VC client")
Trace.trace(1,"GO")
###############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
import string
import time

# enstore imports
import callback
import dict_to_a
import interface
import generic_client
import backup_client
import configuration_client
import udp_client
import db
import Trace
import pdb
import e_errors

class VolumeClerkClient(generic_client.GenericClient,\
                        backup_client.BackupClient):

    def __init__(self, csc=0, verbose=0, host=interface.default_host(), \
                 port=interface.default_port()):
        Trace.trace(10,'{__init__ csc='+repr(csc)+' verbose='+repr(verbose)+\
                    ' host='+repr(host)+' port='+repr(port))
        configuration_client.set_csc(self, csc, host, port, verbose)
        self.u = udp_client.UDPClient()
        Trace.trace(10,'}__init__ u='+repr(self.u))

    # send the request to the volume clerk server and then send answer to user
    def send (self, ticket,  rcv_timeout=0, tries=0):
        Trace.trace(16,'{send')
        vticket = self.csc.get("volume_clerk")
        Trace.trace(16,'send to volume clerk '+\
                    repr((vticket['hostip'], vticket['port'])))
        x = self.u.send(ticket, (vticket['hostip'], vticket['port']), rcv_timeout, tries)
        Trace.trace(16,'}send '+repr(x))
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
               user_inhibit  = "none",# "none" | "readonly" | "all"
               error_inhibit = "none",# "none" | "readonly" | "all" | "writing"
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
               wrapper = "cpio",      # kind of wrapper for volume
               blocksize = -1         # blocksize (-1 =  media type specifies)
               ):
        Trace.trace(3,'add_vol label='+repr(external_label))
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
        Trace.trace(3,'}add_vol '+repr(x))
        return x


    # delete a volume from the stockpile
    def delvol(self, external_label):
        Trace.trace(3,'del_vol label='+repr(external_label))
        ticket= { 'work'           : 'delvol',
                  'external_label' : external_label }
        x = self.send(ticket)
        Trace.trace(3,'}del_vol '+repr(x))
        return x


    # get a list of all volumes
    def get_vols(self):
        Trace.trace(20,'{get_vols R U CRAZY?')
        import string
        # get a port to talk on and listen for connections
        host, port, listen_socket = callback.get_callback()
        listen_socket.listen(4)
        ticket = {"work"         : "get_vols",
                  "callback_addr" : (host, port),
                  "unique_id"    : time.time() }
        # send the work ticket to the library manager
        ticket = self.send(ticket)
        if ticket['status'][0] != e_errors.OK:
            Trace.trace(0,"vcc.get_vols: sending ticket"+repr(ticket))
            raise errno.errorcode[errno.EPROTO],"vcc.get_vols: sending ticket"\
                  +repr(ticket)

        # We have placed our request in the system and now we have to wait.
        # All we  need to do is wait for the system to call us back,
        # and make sure that is it calling _us_ back, and not some sort of old
        # call-back to this very same port. It is dicey to time out, as it
        # is probably legitimate to wait for hours....
        while 1:
            control_socket, address = listen_socket.accept()
            new_ticket = callback.read_tcp_socket(control_socket, "volume"+\
                                  "clerk client get_vols,  vc call back")
            import pprint
            if ticket["unique_id"] == new_ticket["unique_id"]:
                listen_socket.close()
                break
            else:
                print ("vcc.get_vols: imposter called us back, trying again")
                control_socket.close()
        ticket = new_ticket
        if ticket["status"][0] != e_errors.OK:
            Trace.trace(0,"vcc.get_vols: "\
                  +"1st (pre-work-read) volume clerk callback on socket "\
                  +repr(address)+" failed to setup transfer: "\
                  +ticket["status"])
            raise errno.errorcode[errno.EPROTO],"vcc.get_vols: "\
                  +"1st (pre-work-read) volume clerk callback on socket "\
                  +repr(address)+", failed to setup transfer: "\
                  +"ticket[\"status\"]="+ticket["status"]

        # If the system has called us back with our own  unique id, call back
        # the library manager on the library manager's port and read the
        # work queues on that port.
        data_path_socket = callback.volume_server_callback_socket(ticket)
        ticket= callback.read_tcp_socket(data_path_socket, "volume clerk"\
                  +"client get_vols, vc final dialog")
        while 1:
          msg=callback.read_tcp_buf(data_path_socket,"volume clerk "+\
                                    "client get_vols, reading worklist")
          if len(msg)==0:
                break
          pprint.pprint(msg)
        worklist = ticket
        data_path_socket.close()


        # Work has been read - wait for final dialog with volume clerk
        done_ticket = callback.read_tcp_socket(control_socket, "volume clerk"\
                  +"client get_vols, vc final dialog")
        control_socket.close()
        if done_ticket["status"][0] != e_errors.OK:
            Trace.trace(0,"vcc.get_vols "\
                  +"2nd (post-work-read) volume clerk callback on socket "\
                  +repr(address)+", failed to transfer: "\
                  +ticket["status"])
            raise errno.errorcode[errno.EPROTO],"vcc.get_vols "\
                  +"2nd (post-work-read) volume clerk callback on socket "\
                  +repr(address)+", failed to transfer: "\
                  +"ticket[\"status\"]="+ticket["status"]
        Trace.trace(20,'}get_vols')
        return worklist

    # what is the current status of a specified volume?
    def inquire_vol(self, external_label):
        Trace.trace(10,'inquire_vol label='+repr(external_label))
        ticket= { 'work'           : 'inquire_vol',
                  'external_label' : external_label }
        x = self.send(ticket)
        Trace.trace(10,'}inquire_vol '+repr(x))
        return x

    # we are using the volume
    def set_writing(self, external_label):
        Trace.trace(10,'set_writing label='+repr(external_label))
        ticket= { 'work'           : 'set_writing',
                  'external_label' : external_label }
        x = self.send(ticket)
        Trace.trace(10,'}set_writing '+repr(x))
        return x

    # we are using the volume
    def set_system_readonly(self, external_label):
        Trace.trace(10,'set_system_readonly label='+repr(external_label))
        ticket= { 'work'           : 'set_system_readonly',
                  'external_label' : external_label }
        x = self.send(ticket)
        Trace.trace(10,'}set_system_readonly '+repr(x))
        return x

    # clear any inhibits on the volume
    def clr_system_inhibit(self,external_label):
        Trace.trace(3,'clr_system_inhibit label='+repr(external_label))
        ticket= { 'work'           : 'clr_system_inhibit',
                  'external_label' : external_label }
        x = self.send(ticket)
        Trace.trace(3,'}clr_system_inhibit '+repr(x))
        return x

    # we are using the volume
    def set_hung(self, external_label):
        Trace.trace(3,'set_hung label='+repr(external_label))
        ticket= { 'work'           : 'set_hung',
                  'external_label' : external_label }
        x = self.send(ticket)
        Trace.trace(3,'}set_hung '+repr(x))
        return x

    # this many bytes left - update database
    def set_remaining_bytes(self, external_label,remaining_bytes,eod_cookie,
                            wr_err,rd_err,wr_access,rd_access):
        Trace.trace(10,'{set_remaining_bytes label='+repr(external_label)+\
                    ' bytes='+repr(remaining_bytes)+ ' wr_err='+\
                    repr(wr_err)+' rd_err='+repr(rd_err)+' wr_access='+\
                    repr(wr_access)+' rd_access='+repr(rd_access))
        ticket= { 'work'            : 'set_remaining_bytes',
                  'external_label'  : external_label,
                  'remaining_bytes' : remaining_bytes,
                  'eod_cookie'      : eod_cookie,
                  'wr_err'          : wr_err,
                  'rd_err'          : rd_err,
                  'wr_access'       : wr_access,
                  'rd_access'       : rd_access }
        x = self.send(ticket)
        Trace.trace(10,'}set_remaining_bytes '+repr(x))
        return x


    # update the counts in the database
    def update_counts(self, external_label, wr_err,rd_err,wr_access,rd_access):
        Trace.trace(10,'{update_counts label='+repr(external_label)+' wr_err='+\
                    repr(wr_err)+' rd_err='+repr(rd_err)+' wr_access='+\
                    repr(wr_access)+' rd_access='+repr(rd_access))
        ticket= { 'work'            : 'update_counts',
                  'external_label'  : external_label,
                  'wr_err'          : wr_err,
                  'rd_err'          : rd_err,
                  'wr_access'       : wr_access,
                  'rd_access'       : rd_access }
        x = self.send(ticket)
        Trace.trace(10,'}update_counts '+repr(x))
        return x

    # which volume can we use for this library, bytes and file family and ...
    def next_write_volume (self, library, min_remaining_bytes,
                           file_family, vol_veto_list,first_found):
        Trace.trace(10,'{next_write_volume lib='+repr(library)+' bytes='+\
                    repr(min_remaining_bytes)+' ff='+repr(file_family)+\
                    " veto="+repr(vol_veto_list)+' first_found='+\
                    repr(first_found))
        ticket = { 'work'                : 'next_write_volume',
                   'library'             : library,
                   'min_remaining_bytes' : min_remaining_bytes,
                   'file_family'         : file_family,
                   'vol_veto_list'       : `vol_veto_list`,
                   'first_found'         : first_found }

        x = self.send(ticket)
        Trace.trace(10,'}next_write_volume '+repr(x))
        return x

class VolumeClerkClientInterface(interface.Interface):

    def __init__(self):
        Trace.trace(10,'{__init__ vcci')
        self.alive = 0
        self.alive_rcv_timeout = 0
        self.alive_retries = 0
        self.clrvol = 0
        self.backup = 0
        self.vols = 0
        self.nextvol = 0
        self.vol = ""
        self.addvol = 0
        self.delvol = 0
	self.verbose = 0
        interface.Interface.__init__(self)

        # parse the options
        self.parse_options()
        Trace.trace(10,'}__init__ vcci')

    # define the command line options that are valid
    def options(self):
        Trace.trace(20,'{}options')
        return self.config_options() +\
               ["verbose=", "alive","alive_rcv_timeout=","alive_retries=","clrvol", "backup" ] +\
               ["vols","nextvol","vol=","addvol","delvol" ] +\
               self.help_options()

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
        Trace.trace(16,'}parse_options')

    # print clr_inhibit arguments
    def print_clr_inhibit_args(self):
        Trace.trace(20,'{}print_clr_inhibit_args')
        print "   clr_inhibit arguments: volume_name"

    # print addvol arguments
    def print_addvol_args(self):
        Trace.trace(20,'{}print_addvol_args')
        print "   addvol arguments: library file_family media_type"\
              +", volume_name, volume_byte_capacity remaining_capacity"

    # print delvol arguments
    def print_delvol_args(self):
        Trace.trace(20,'{}print_delvol_args')
        print "   delvol arguments: volume_name"

    # print out our extended help
    def print_help(self):
        Trace.trace(20,'{print_help')
        interface.Interface.print_help(self)
        self.print_addvol_args()
        self.print_delvol_args()
        Trace.trace(16,'}print_help')


if __name__ == "__main__":
    import sys
    import pprint
    Trace.init("VC client")
    Trace.trace(1,"vcc called with args "+repr(sys.argv))

    # fill in the interface
    intf = VolumeClerkClientInterface()

    # get a volume clerk client
    vcc = VolumeClerkClient(0, intf.verbose, intf.config_host,\
                            intf.config_port)

    if intf.alive:
        ticket = vcc.alive(intf.alive_rcv_timeout,intf.alive_retries)
    elif intf.backup:
        ticket = vcc.start_backup()
        db.do_backup("volume")
        ticket = vcc.stop_backup()
    elif intf.vols:
        ticket = vcc.get_vols()
    elif intf.nextvol:
        ticket = vcc.next_write_volume(intf.args[0], #library
                                       string.atol(intf.args[1]), #min_remaining_byte
                                       intf.args[2], #file_family
                                            [], #vol_veto_list
                                             1) #first_found
    elif intf.vol:
        ticket = vcc.inquire_vol(intf.vol)
	pprint.pprint(ticket)
    elif intf.addvol:
        ticket = vcc.addvol(intf.args[0],              # library
                            intf.args[1],              # file family
                            intf.args[2],              # media type
                            intf.args[3],              # name of this volume
                            string.atol(intf.args[4]), # cap'y of vol (bytes)
                            string.atol(intf.args[5])) # rem cap'y of volume
    elif intf.delvol:
        ticket = vcc.delvol(intf.args[0])              # name of this volume
    elif intf.clrvol:
        ticket = vcc.clr_system_inhibit(intf.args[0])  # name of this volume

    if ticket['status'][0] != e_errors.OK:
        print "Bad status:",ticket['status']
        pprint.pprint(ticket)
        Trace.trace(0,"vcc BAD STATUS - "+repr(ticket['status']))
        sys.exit(1)
    elif intf.verbose:
        pprint.pprint(ticket)

    Trace.trace(1,"vcc exit ok")
    sys.exit(0)
