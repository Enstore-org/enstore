###############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
import time
import string
import errno

# enstore imports
import generic_client
import backup_client
import udp_client
import db
import callback
import Trace
import e_errors

MY_NAME = "FILE_C_CLIENT"
MY_SERVER = "file_clerk"

class FileClient(generic_client.GenericClient, \
                      backup_client.BackupClient):

    def __init__( self, csc, bfid=0, servr_addr=None ):
        generic_client.GenericClient.__init__(self, csc, MY_NAME)
        self.u = udp_client.UDPClient()
	self.bfid = bfid
        ticket = self.csc.get( MY_SERVER )
	if servr_addr != None: self.servr_addr = servr_addr
	else:                  self.servr_addr = (ticket['hostip'],ticket['port'])

    def send (self, ticket, rcv_timeout=0, tries=0):
        Trace.trace( 12, 'send to volume clerk '+repr(self.servr_addr) )
        x = self.u.send( ticket, self.servr_addr, rcv_timeout, tries )
        return x

    def new_bit_file(self, ticket):
        r = self.send(ticket)
        return r

    def set_pnfsid(self, ticket):
        r = self.send(ticket)
        return r

    def set_delete(self, ticket):
        r = self.send(ticket)
        return r

    def get_bfids(self):
        host, port, listen_socket = callback.get_callback()
        listen_socket.listen(4)
        ticket = {"work"         : "get_bfids",
                  "callback_addr": (host, port),
                  "unique_id"    : time.time() }
        # send the work ticket to the library manager
        ticket = self.send(ticket)
        if ticket['status'][0] != e_errors.OK:
            raise errno.errorcode[errno.EPROTO],"fcc.get_bfids: sending ticket"+repr(ticket)

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
                Trace.log(e_errors.INFO,
                          "get_bfids - imposter called us back, trying again")
                control_socket.close()
        ticket = new_ticket
        if ticket["status"][0] != e_errors.OK:
            msg = "get_bfids: "\
                  +"1st (pre-work-read) file clerk callback on socket "\
                  +repr(address)+", failed to setup transfer: "\
                  +"ticket[\"status\"]="+ticket["status"]
            Trace.trace(7,msg)
            raise errno.errorcode[errno.EPROTO],msg
        # If the system has called us back with our own  unique id, call back
        # the library manager on the library manager's port and read the
        # work queues on that port.
        data_path_socket = callback.file_server_callback_socket(ticket)
        ticket= callback.read_tcp_obj(data_path_socket)
        bfids=''
        while 1:
            msg=callback.read_tcp_raw(data_path_socket)
            if not msg: break
            if bfids: bfids=bfids+','+msg
            else: bfids=msg
        ticket['bfids'] = bfids
        data_path_socket.close()

        # Work has been read - wait for final dialog with file clerk
        done_ticket = callback.read_tcp_obj(control_socket)
        control_socket.close()
        if done_ticket["status"][0] != e_errors.OK:
            msg = "get_bfids "\
                  +"2nd (post-work-read) file clerk callback on socket "\
                  +repr(address)+", failed to transfer: "\
                  +"ticket[\"status\"]="+ticket["status"]
            Trace.trace(7,msg)
            raise 'EPROTO',msg

        return ticket

    def tape_list(self,external_label):
        host, port, listen_socket = callback.get_callback()
        listen_socket.listen(4)
        ticket = {"work"          : "tape_list",
                  "callback_addr" : (host, port),
                  "external_label": external_label,
                  "unique_id"     : time.time() }
        # send the work ticket to the file clerk
        ticket = self.send(ticket)
        if ticket['status'][0] != e_errors.OK:
            raise errno.errorcode[errno.EPROTO],"fcc.tape_list: sending ticket"+repr(ticket)

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
	        Trace.log(e_errors.INFO,
                          "tape_list - imposter called us back, trying again")
                control_socket.close()
        ticket = new_ticket
        if ticket["status"][0] != e_errors.OK:
            msg = "tape_list: "\
                  +"1st (pre-work-read) file clerk callback on socket "\
                  +repr(address)+", failed to setup transfer: "\
                  +"ticket[\"status\"]="+ticket["status"]
            Trace.trace(7,msg)
            raise errno.errorcode[errno.EPROTO],msg
        # If the system has called us back with our own  unique id, call back
        # the library manager on the library manager's port and read the
        # work queues on that port.
        data_path_socket = callback.file_server_callback_socket(ticket)
        ticket= callback.read_tcp_obj(data_path_socket)
        tape_list=''
        while 1:
            msg=callback.read_tcp_raw(data_path_socket)
            if not msg: break
            if tape_list: tape_list=tape_list+msg
            else: tape_list=msg
        ticket['tape_list'] = tape_list
        data_path_socket.close()

        # Work has been read - wait for final dialog with file clerk
        done_ticket = callback.read_tcp_obj(control_socket)
        control_socket.close()
        if done_ticket["status"][0] != e_errors.OK:
            msg = "tape_list "\
                  +"2nd (post-work-read) file clerk callback on socket "\
                  +repr(address)+", failed to transfer: "\
                  +"ticket[\"status\"]="+ticket["status"]
            Trace.trace(7,msg)
            raise 'EPROTO',msg

        return ticket


    def bfid_info(self):
        r = self.send({"work" : "bfid_info",\
                       "bfid" : self.bfid } )
        return r

    def set_deleted(self, deleted, restore_dir="no"):
        r = self.send({"work"        : "set_deleted",
                       "bfid"        : self.bfid,
                       "deleted"     : deleted,
		       "restore_dir" : restore_dir } )
        return r

    # rename volume and volume map
    def rename_volume(self, bfid, external_label, 
		      set_deleted, restore_vm, restore_dir):
        r = self.send({"work"           : "rename_volume",
                       "bfid"           : bfid,
		       "external_label" : external_label,
		       "set_deleted"    : set_deleted,
		       "restore"        : restore_vm,
		       "restore_dir"    : restore_dir } )
	return r

    # rename volume and volume map
    def restore(self, file_name, restore_dir="no"):
        r = self.send({"work"           : "restore_file",
                       "file_name"      : file_name,
		       "restore_dir"    : restore_dir } )
	return r

    # get volume map name for given bfid
    def get_volmap_name(self):
        r = self.send({"work"           : "get_volmap_name",\
                       "bfid"           : self.bfid} )
	return r

    # delete bitfile
    def del_bfid(self):
        r = self.send({"work"           : "del_bfid",\
                       "bfid"           : self.bfid} )
	return r

class FileClerkClientInterface(generic_client.GenericClientInterface):

    def __init__(self):
        # fill in the defaults for the possible options
        self.bfids = 0
        self.tape_list = 0
        self.bfid = 0
        self.backup = 0
        self.deleted = 0
	self.restore = 0
        self.alive_rcv_timeout = 0
        self.alive_retries = 0
        generic_client.GenericClientInterface.__init__(self)

    # define the command line options that are valid
    def options(self):
        return self.client_options()+["bfids","bfid=","deleted=","tape_list=","backup", "restore=", "r"]


if __name__ == "__main__" :
    import sys
    Trace.init(MY_NAME)
    Trace.trace(6,"fcc called with args "+repr(sys.argv))

    # fill in interface
    intf = FileClerkClientInterface()

    # now get a file clerk client
    fcc = FileClient((intf.config_host, intf.config_port), intf.bfid)
    Trace.init(fcc.get_name(MY_NAME))

    if intf.alive:
        ticket = fcc.alive(MY_SERVER, intf.alive_rcv_timeout,
                           intf.alive_retries)

    elif intf.backup:
        ticket = fcc.start_backup()
        db.do_backup("file")
        ticket = fcc.stop_backup()

    elif intf.deleted and intf.bfid:
	try:
	    if intf.restore_dir: dir ="yes"
	except AttributeError:
	    dir = "no"
        ticket = fcc.set_deleted(intf.deleted, dir)
        Trace.trace(13, repr(ticket))

    elif intf.bfids:
        ticket = fcc.get_bfids()
        print ticket['bfids']

    elif intf.tape_list:
        ticket = fcc.tape_list(intf.tape_list)
        print ticket['tape_list']
        aticket = fcc.alive(MY_SERVER, intf.alive_rcv_timeout,
                            intf.alive_retries) #clear out any zombies from the forked file clerk

    elif intf.bfid:
        ticket = fcc.bfid_info()
	if ticket['status'][0] ==  e_errors.OK:
	    print repr(ticket['fc'])
	    print repr(ticket['vc'])
    elif intf.restore:
	try:
	    if intf.restore_dir: dir="yes"
	except AttributeError:
	    dir = "no"
	print "file",intf.file 
        ticket = fcc.restore(intf.file, dir)
    else:
	intf.print_help()
        sys.exit(0)

    fcc.check_ticket(ticket)


