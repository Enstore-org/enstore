###############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
import sys
import string
import time
import errno
import pprint

# enstore imports
import callback
import interface
import generic_client
import backup_client
import udp_client
import Trace
import e_errors

MY_NAME = "VOLUME_C_CLIENT"
MY_SERVER = "volume_clerk"


#turn byte count into a nicely formatted string
def capacity_str(x):
    x=1.0*x    ## make x floating-point
    neg=x<0    ## remember the sign of x
    x=abs(x)   ##  make x positive so that "<" comparisons work
        
    for suffix in ('B ', 'KB', 'MB', 'GB', 'TB', 'PB'):
        if x <= 1024:
            break
        x=x/1024
    if neg:    ## if x was negative coming in, restore the - sign  
        x = -x
    return "%6.2f%s"%(x,suffix)


class VolumeClerkClient(generic_client.GenericClient,
                        backup_client.BackupClient):

    def __init__( self, csc, servr_addr=None ):
        generic_client.GenericClient.__init__(self, csc, MY_NAME)
        self.u = udp_client.UDPClient()
        ticket = self.csc.get( MY_SERVER )
	if servr_addr != None: self.servr_addr = servr_addr
	else:                  self.servr_addr = (ticket['hostip'],ticket['port'])
        Trace.trace(10,'__init__ u='+str(self.u))

    # send the request to the volume clerk server and then send answer to user
    def send (self, ticket,  rcv_timeout=0, tries=0):
        Trace.trace( 16, 'send to volume clerk '+str(self.servr_addr) )
        x = self.u.send( ticket, self.servr_addr, rcv_timeout, tries )
        return x

    # add a volume to the stockpile
    def add(self,
            library,               # name of library media is in
            file_family,           # volume family the media is in
            media_type,            # media
            external_label,        # label as known to the system
            capacity_bytes,        #
            remaining_bytes,       #
            eod_cookie  = "none",  # code for seeking to eod
            user_inhibit  = ["none","none"],# 0:"none" | "readonly" | "NOACCESS"
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
            blocksize = -1,        # blocksize (-1 =  media type specifies)
            non_del_files = 0,     # non-deleted files
            system_inhibit = ["none","none"] # 0:"none" | "writing??" | "NOACCESS", "DELETED
                                             # 1:"none" | "readonly" | "full"
            ):
        Trace.trace( 6, 'add label=%s'%(external_label,))
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
                   'blocksize'       : blocksize,
                   'non_del_files'   : non_del_files,
                   'system_inhibit'  : system_inhibit
                   }
        x = self.send(ticket)
        return x

    def modify(self,ticket):
        ticket['work']='modifyvol'
        x=self.send(ticket)
        return x

    # delete a volume from the stockpile
    def delete(self, external_label,force=0):
        ticket= { 'work'           : 'delvol',
                  'external_label' : external_label,
                  'force'          : force }
        x = self.send(ticket)
        return x

    # delete a volume from the stockpile
    def restore(self, external_label, restore=0):
	if restore: restore_vm = "yes"
	else: restore_vm = "no"
        ticket= { 'work'           : 'restorevol',
                  'external_label' : external_label,
		  "restore"         : restore_vm}
        x = self.send(ticket)
        return x


    # get a list of all volumes
    def get_vols(self, key=None,state=None, not_cond=None):
        import cPickle
        # get a port to talk on and listen for connections
        host, port, listen_socket = callback.get_callback()
        listen_socket.listen(4)
        ticket = {"work"          : "get_vols",
                  "callback_addr" : (host, port),
                  "key"           : key,
                  "in_state"      : state,
                  "not"           : not_cond,
                  "unique_id"     : time.time() }
        # send the work ticket to the library manager
        ticket = self.send(ticket)
        if ticket['status'][0] != e_errors.OK:
            Trace.log( e_errors.ERROR,
		       'vcc.get_vols: sending ticket: %s'%(ticket,) )
            raise errno.errorcode[errno.EPROTO],"vcc.get_vols: sending ticket %s"%(ticket,)

        # We have placed our request in the system and now we have to wait.
        # All we  need to do is wait for the system to call us back,
        # and make sure that is it calling _us_ back, and not some sort of old
        # call-back to this very same port. It is dicey to time out, as it
        # is probably legitimate to wait for hours....
        while 1:
            control_socket, address = listen_socket.accept()
            new_ticket = callback.read_tcp_obj(control_socket)
            if ticket["unique_id"] == new_ticket["unique_id"]:
                listen_socket.close()
                break
            else:
                Trace.log(e_errors.WARNING,
                          "get_vols - imposter called us back, trying again")
                control_socket.close()
        ticket = new_ticket
        if ticket["status"][0] != e_errors.OK:
            raise errno.errorcode[errno.EPROTO],"vcc.get_vols: "\
                  +"1st (pre-work-read) volume clerk callback on socket "\
                  +str(address)+", failed to setup transfer: "\
                  +"ticket[\"status\"]="+ticket["status"]

        # If the system has called us back with our own  unique id, call back
        # the volume clerk on the volume clerk's port and read the
        # work queues on that port.
        data_path_socket = callback.volume_server_callback_socket(ticket)
        ticket= callback.read_tcp_obj(data_path_socket)
        volumes = cPickle.loads(callback.read_tcp_raw(data_path_socket))
        ##while 1:
        ##    msg=callback.read_tcp_raw(data_path_socket)
        ##    if not msg: break
        ##    if volumes: volumes = volumes+","+msg
        ##    else: volumes=msg
        #ticket['volumes'] = volumes
        data_path_socket.close()


        # Work has been read - wait for final dialog with volume clerk
        done_ticket = callback.read_tcp_obj(control_socket)
        control_socket.close()
        if done_ticket["status"][0] != e_errors.OK:
            raise errno.errorcode[errno.EPROTO],"vcc.get_vols "\
                  +"2nd (post-work-read) volume clerk callback on socket "\
                  +str(address)+", failed to transfer: "\
                  +"ticket[\"status\"]="+ticket["status"]

        if volumes.has_key("header"):        # full info
            print "%-10s  %-8s %-12s %-17s %17s %012s %-012s"%(
                "label","avail.","mount state",
                "system_inhibit","user_inhibit",
                "library","    file_family")
            for v in volumes["volumes"]:
                print "%-10s"%v['volume'],
                print capacity_str(v['remaining_bytes']),
                print " %-12s (%-08s %08s) (%-08s %08s) %-012s %012s"%(
                    v['at_mover'][0],
                    v['system_inhibit'][0],v['system_inhibit'][1],
                    v['user_inhibit'][0],v['user_inhibit'][1],
                    v['library'],v['file_family'])
        else:
            vlist = ''
            for v in volumes.get("volumes",[]):
                vlist = vlist+v['volume']+" "
            print vlist
                

        return ticket

    # remove deleted volumes
    def remove_deleted_vols(self, volume=None):
        # get a port to talk on and listen for connections
        host, port, listen_socket = callback.get_callback()
        listen_socket.listen(4)
        ticket = {"work"         : "remove_deleted_vols",
                  "callback_addr" : (host, port),
                  "unique_id"    : time.time() }
        if volume: ticket["external_label"] = volume
        # send the work ticket to the library manager
        ticket = self.send(ticket)
        if ticket['status'][0] != e_errors.OK:
            raise errno.errorcode[errno.EPROTO],"vcc.remove_deleted_vols: sending ticket %s"%(ticket,)

        # We have placed our request in the system and now we have to wait.
        # All we  need to do is wait for the system to call us back,
        # and make sure that is it calling _us_ back, and not some sort of old
        # call-back to this very same port. It is dicey to time out, as it
        # is probably legitimate to wait for hours....
        while 1:
            control_socket, address = listen_socket.accept()
            new_ticket = callback.read_tcp_obj(control_socket)
            if ticket["unique_id"] == new_ticket["unique_id"]:
                listen_socket.close()
                break
            else:
                Trace.log(e_errors.WARNING,
                          "remove_deleted_vols - imposter called us back, trying again")
                control_socket.close()
        ticket = new_ticket
        if ticket["status"][0] != e_errors.OK:
            raise errno.errorcode[errno.EPROTO],"vcc.remove_deleted_vols: "\
                  +"1st (pre-work-read) volume clerk callback on socket "\
                  +str(address)+", failed to setup transfer: "\
                  +"ticket[\"status\"]="+ticket["status"]

        # If the system has called us back with our own  unique id, call back
        # the library manager on the library manager's port and read the
        # work queues on that port.
        data_path_socket = callback.volume_server_callback_socket(ticket)
        ticket= callback.read_tcp_obj(data_path_socket)
        volumes=''
        while 1:
            msg=callback.read_tcp_raw(data_path_socket)
            if not msg: break
            if volumes: volumes = volumes+"\012"+msg
            else: volumes=msg
        ticket['volumes'] = volumes
        data_path_socket.close()


        # Work has been read - wait for final dialog with volume clerk
        done_ticket = callback.read_tcp_obj(control_socket)
        control_socket.close()
        if done_ticket["status"][0] != e_errors.OK:
            raise errno.errorcode[errno.EPROTO],"vcc.remove_deleted_vols "\
                  +"2nd (post-work-read) volume clerk callback on socket "\
                  +str(address)+", failed to transfer: "\
                  +"ticket[\"status\"]="+ticket["status"]

        return ticket

    # what is the current status of a specified volume?
    def inquire_vol(self, external_label):
        ticket= { 'work'           : 'inquire_vol',
                  'external_label' : external_label }
        x = self.send(ticket)
        return x

    # move a volume to a new library
    def new_library(self, external_label,new_library):
        ticket= { 'work'           : 'new_library',
                  'external_label' : external_label,
                  'new_library'    : new_library}
        x = self.send(ticket)
        return x

    # we are using the volume
    def set_writing(self, external_label):
        ticket= { 'work'           : 'set_writing',
                  'external_label' : external_label }
        x = self.send(ticket)
        return x

    # we are using the volume
    def set_system_readonly(self, external_label):
        ticket= { 'work'           : 'set_system_readonly',
                  'external_label' : external_label }
        x = self.send(ticket)
        return x

    # mark volume as not allowed 
    def set_system_notallowed(self, external_label):
        ticket= { 'work'           : 'set_system_notallowed',
                  'external_label' : external_label }
        x = self.send(ticket)
        return x

    # mark volume as noaccess
    def set_system_noaccess(self, external_label):
        ticket= { 'work'           : 'set_system_noaccess',
                  'external_label' : external_label }
        x = self.send(ticket)
        return x

    # set system inhibit to none
    def set_system_none(self, external_label):
        ticket= { 'work'           : 'set_system_none',
                  'external_label' : external_label }
        x = self.send(ticket)
        return x

    # mark volume as noaccess
    def set_at_mover(self, external_label, flag, mover, force=None):
	if force: f = 1
	else: f = 0
        ticket= { 'work'           : 'set_at_mover',
                  'external_label' : external_label,
		  'at_mover' : (flag, mover),
		  'force'    : f}
        x = self.send(ticket)

        return x

    # get the state of the media changer for the volume
    def update_mc_state(self, external_label):
        ticket= { 'work'           : 'update_mc_state',
                  'external_label' : external_label
                  }
        x = self.send(ticket)
        return x

    # clear any inhibits on the volume
    def clr_system_inhibit(self,external_label,what=None, pos=0):
        ticket= { 'work'           : 'clr_system_inhibit',
                  'external_label' : external_label,
                  'inhibit'        : what,
                  'position'       : pos}
        x = self.send(ticket)
        return x

    # decrement the file count on a tape
    def decr_file_count(self,external_label, count=1):
        ticket= { 'work'           : 'decr_file_count',
                  'external_label' : external_label,
                  'count'          : count }
        x = self.send(ticket)
        return x

    # we are using the volume
    def set_hung(self, external_label):
        ticket= { 'work'           : 'set_hung',
                  'external_label' : external_label }
        x = self.send(ticket)
        return x

    # this many bytes left - read the database
    def get_remaining_bytes(self, external_label):
        ticket= { 'work'            : 'get_remaining_bytes',
                  'external_label'  : external_label }
        x = self.send(ticket)
        return x

    # this many bytes left - update database
    def set_remaining_bytes(self, external_label,remaining_bytes,eod_cookie,
                            wr_err,rd_err,wr_access,rd_access, bfid):
        ticket= { 'work'            : 'set_remaining_bytes',
                  'external_label'  : external_label,
                  'remaining_bytes' : remaining_bytes,
                  'eod_cookie'      : eod_cookie,
                  'wr_err'          : wr_err,
                  'rd_err'          : rd_err,
                  'wr_access'       : wr_access,
                  'rd_access'       : rd_access,
		  'bfid'            : bfid }
        x = self.send(ticket)
        return x


    # update the counts in the database
    def update_counts(self, external_label, wr_err,rd_err,wr_access,rd_access):
        ticket= { 'work'            : 'update_counts',
                  'external_label'  : external_label,
                  'wr_err'          : wr_err,
                  'rd_err'          : rd_err,
                  'wr_access'       : wr_access,
                  'rd_access'       : rd_access }
        x = self.send(ticket)
        return x

    # Check if volume is available
    def is_vol_available(self, work, external_label, family=None, size=0):
        ticket = { 'work'                : 'is_vol_available',
		   'action'              : work,
		   'file_family'         : family,
		   'file_size'           : size,
		   'external_label'      : external_label
		   }
        x = self.send(ticket)
        return x
	
	
    # which volume can we use for this library, bytes and file family and ...
    def next_write_volume (self, library, min_remaining_bytes,
                           file_family, wrapper, vol_veto_list,first_found):
        ticket = { 'work'                : 'next_write_volume',
                   'library'             : library,
                   'min_remaining_bytes' : min_remaining_bytes,
                   'file_family'         : file_family,
		   'wrapper'             : wrapper,
                   'vol_veto_list'       : `vol_veto_list`,
                   'first_found'         : first_found }

        x = self.send(ticket)
        return x

    # check if specific volume can be used for write
    def can_write_volume (self, library, min_remaining_bytes,
                           file_family, wrapper, external_label):
        ticket = { 'work'                : 'can_write_volume',
                   'library'             : library,
                   'min_remaining_bytes' : min_remaining_bytes,
                   'file_family'         : file_family,
		   'wrapper'             : wrapper,
                   'external_label'       : external_label }

        x = self.send(ticket)
        return x

    # for the backward compatibility D0_TEMP
    def add_at_mover (self, external_label):
        ticket = { 'work'                : 'add_at_mover',
                   'external_label'       : external_label }

        x = self.send(ticket)
        return x
    # END D0_TEMP

class VolumeClerkClientInterface(generic_client.GenericClientInterface):

    def __init__(self, flag=1, opts=[]):
        self.do_parse = flag
        self.restricted_opts = opts
        self.alive_rcv_timeout = 0
        self.alive_retries = 0
        self.clear = ""
        self.update = ""
        self.backup = 0
        self.vols = 0
        self.in_state = 0
        self.next = 0
        self.vol = ""
        self.check = ""
        self.add = ""
        self.modify = ""
        self.delete = ""
        self.restore = ""
        self.all = 0
        self.force  = 0
        self.new_library = ""
        self.read_only = ""
        self.no_access = ""
        self.decr_file_count = 0
        self.rmvol = 0
	self.atmover = 0 # for the backward compatibility D0_TEMP
        generic_client.GenericClientInterface.__init__(self)

    # define the command line options that are valid
    def options(self):
        if self.restricted_opts:
            return self.restricted_opts
        else:
            return self.client_options()+\
                   ["clear=", "backup", "vols","next","vol=","check=","add=",
                    "update=", "delete=","new_library=","read_only=",
                    "no_access=", "atmover","decr_file_count=","force",
                    "restore=", "all","destroy=", "modify="]

    # parse the options like normal but make sure we have necessary params
    def parse_options(self):
        interface.Interface.parse_options(self)
        if self.next:
            if len(self.args) < 3:
                self.print_add_args()
                sys.exit(1)
        elif self.add:
            if len(self.args) < 5:
                self.print_add_args()
                sys.exit(1)
        elif self.new_library:
            if len(self.args) < 1:
                self.print_new_library_args()
                sys.exit(1)

    def print_update_mc_state_args(self):
        print "   update_mc_state arguments: volume_name"

    def print_new_library_args(self):
        print "   new_library arguments: volume_name"

    def print_add_args(self):
        print "   add arguments: volume_name library file_family media_type"\
              +" volume_byte_capacity remaining_capacity"

    def print_clear_args(self):
        print "usage: --clear volume_name"
        print "       --clear volume_name {system_inhibit|user_inhibit} position(1 or 2)"
        
        
    # print out our extended help
    def print_help(self):
        interface.Interface.print_help(self)
        self.print_add_args()
        self.print_update_mc_state_args()
        self.print_new_library_args()
        self.print_clear_args()
        
def do_work(intf):
    # get a volume clerk client
    vcc = VolumeClerkClient((intf.config_host, intf.config_port))
    Trace.init(vcc.get_name(MY_NAME))
	
    if intf.alive:
        ticket = vcc.alive(MY_SERVER, intf.alive_rcv_timeout,
                           intf.alive_retries)
    elif intf.backup:
        ticket = vcc.start_backup()
        ticket = vcc.backup()
        ticket = vcc.stop_backup()
    elif intf.vols:
        # optional argument
        nargs = len(intf.args)
        not_cond = None
        if nargs:
            if nargs == 3:
                key = intf.args[0]     
                in_state=intf.args[1]
                not_cond = intf.args[2]
            elif nargs == 2:
                key = intf.args[0]     
                in_state=intf.args[1]
            elif nargs == 1:
                key = None
                in_state=intf.args[0]
            else:
                print "Wrong number of arguments"
                print "usage: --vols"
                print "       --vols state (will match system_inhibit)"
                print "       --vols key state"
                print "       --vols key state not (not in state)"
                return
        else:
            key = None
            in_state = None 
        ticket = vcc.get_vols(key, in_state, not_cond)
    elif intf.rmvol:
        # optional argument
        if intf.rmvol == 'all': intf.rmvol = None
        ticket = vcc.remove_deleted_vols(intf.rmvol)
        print ticket['volumes']
    elif intf.next:
        ticket = vcc.next_write_volume(intf.args[0], #library
                                       string.atol(intf.args[1]), #min_remaining_byte
                                       intf.args[2], #file_family
                                       intf.args[3], # wrapper
                                            [], #vol_veto_list
                                             1) #first_found
    elif intf.vol:
        ticket = vcc.inquire_vol(intf.vol)
	pprint.pprint(ticket)
    elif intf.check:
        ticket = vcc.inquire_vol(intf.check)
        ##pprint.pprint(ticket)
        print "%-10s  %s %-12s  %s %s" % (ticket['external_label'],
                                               capacity_str(ticket['remaining_bytes']),
                                               ticket['at_mover'][0],
                                               ticket['system_inhibit'],
                                               ticket['user_inhibit'])
    elif intf.add:
        print repr(intf.args)
        library, file_family, media_type, capacity, remaining = intf.args[:5]
        capacity, remaining = string.atol(capacity), string.atol(remaining)
        if media_type == 'null': #media type
            wrapper = "null"
        else:
            wrapper = "cpio_odc"
        ticket = vcc.add(library,
                         file_family,
                         media_type,     
                         intf.add,                  # name of this volume
                         capacity,
                         remaining,
                         wrapper=wrapper)           # rem cap'y of volume
    elif intf.modify:
        d={}
        for s in intf.args:
            k,v=string.split(s,'=')
            try:
                v=eval(v) #numeric args
            except:
                pass #yuk...
            d[k]=v
        d['external_label']=intf.modify # name of this volume
        ticket = vcc.modify(d)

        
    elif intf.new_library:
        ticket = vcc.new_library(intf.args[0],         # volume name
                                 intf.new_library)     # new library name
    elif intf.delete:
        ticket = vcc.delete(intf.delete,intf.force)   # name of this volume
    elif intf.restore:
        ticket = vcc.restore(intf.restore, intf.all)  # name of volume
    elif intf.clear:
        nargs = len(intf.args)
        what = None
        pos = 0
        if nargs > 0:
            if nargs != 2:
                intf.print_clear_args()
            else:
                ipos = string.atoi(intf.args[1])-1
                if not (intf.args[0] == "system_inhibit" or intf.args[0] == "user_inhibit"):
                    intf.print_clear_args()
                    return
                elif not (ipos == 0 or ipos == 1):
                    intf.print_clear_args()
                    return
                else:
                    what = intf.args[0]
                    pos = ipos
                
        ticket = vcc.clr_system_inhibit(intf.clear, what, pos)  # name of this volume
    elif intf.update:
        ticket = vcc.update_mc_state(intf.update)  # name of this volume
        Trace.trace(12, repr(ticket))
    elif intf.decr_file_count:
        ticket = vcc.decr_file_count(intf.args[0],string.atoi(intf.decr_file_count))
        Trace.trace(12, repr(ticket))
    elif intf.read_only:
        ticket = vcc.set_system_readonly(intf.read_only)  # name of this volume
    elif intf.no_access:
        ticket = vcc.set_system_notallowed(intf.no_access)  # name of this volume
    # D0_TEPM
    elif intf.atmover:
	ticket = vcc.add_at_mover (intf.args[0])
	pprint.pprint(ticket)
    # END D0_TEMP
    else:
	intf.print_help()
        sys.exit(0)

    vcc.check_ticket(ticket)

if __name__ == "__main__":
    Trace.init(MY_NAME)
    Trace.trace( 6, 'vcc called with args: %s'%(sys.argv,) )

    # fill in the interface
    intf = VolumeClerkClientInterface()

    do_work(intf)

