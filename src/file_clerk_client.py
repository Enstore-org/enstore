###############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
import time
import string
import errno

# enstore imports
import configuration_client
import generic_client
import generic_cs
import backup_client
import udp_client
import db
import callback
import interface
import Trace
import e_errors

class FileClient(generic_client.GenericClient, \
                      backup_client.BackupClient):

    def __init__( self, csc=0, verbose=0, host=interface.default_host(),
                  port=interface.default_port(), bfid=0, servr_addr=None ):
        Trace.trace( 10, '{__init__' )
	self.print_id = "FILECC"
        self.u = udp_client.UDPClient()
	self.bfid = bfid
	self.verbose = verbose
        configuration_client.set_csc( self, csc, host, port, verbose )
        ticket = self.csc.get( "file_clerk" )
	if servr_addr != None: self.servr_addr = servr_addr
	else:                  self.servr_addr = (ticket['hostip'],ticket['port'])
	try:    self.print_id = ticket['logname']
        except: pass
        Trace.trace( 10, '}__init' )

    def send (self, ticket, rcv_timeout=0, tries=0):
        Trace.trace( 12, '{send to volume clerk '+repr(self.servr_addr) )
        x = self.u.send( ticket, self.servr_addr, rcv_timeout, tries )
        Trace.trace( 12, '}send '+repr(x) )
        return x

    def new_bit_file(self, ticket):
        Trace.trace(12,"{new_bit_file")
        r = self.send(ticket)
        Trace.trace(12,"}new_bit_file"+repr(r))
        return r

    def set_pnfsid(self, ticket):
        Trace.trace(12,"{set_pnfsid")
        r = self.send(ticket)
        Trace.trace(12,"}set_pnfsid"+repr(r))
        return r

    def set_delete(self, ticket):
        Trace.trace(12,"{set_delete")
        r = self.send(ticket)
        Trace.trace(12,"}set_delete"+repr(r))
        return r

    def get_bfids(self):
        Trace.trace(16,"{get_bfids")
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
            new_ticket = callback.read_tcp_socket(control_socket, "file"+\
                                  "clerk client get_bfids,  fc call back")
            if ticket["unique_id"] == new_ticket["unique_id"]:
                listen_socket.close()
                break
            else:
	        self.enprint("get_bfids - imposter called us back, trying again")
                control_socket.close()
        ticket = new_ticket
        if ticket["status"][0] != e_errors.OK:
            msg = "get_bfids: "\
                  +"1st (pre-work-read) file clerk callback on socket "\
                  +repr(address)+", failed to setup transfer: "\
                  +"ticket[\"status\"]="+ticket["status"]
            Trace.trace(0,msg)
            raise errno.errorcode[errno.EPROTO],msg
        # If the system has called us back with our own  unique id, call back
        # the library manager on the library manager's port and read the
        # work queues on that port.
        data_path_socket = callback.file_server_callback_socket(ticket)
        ticket= callback.read_tcp_socket(data_path_socket, "file clerk"\
                  +"client get_bfids, fc final dialog")
        while 1: ##XXX warning read_tcp_buf can over-read
          msg=callback.read_tcp_buf(data_path_socket,"file  clerk "+"client get_bfids, reading worklist")
          if len(msg)==0:
                break
	  generic_cs.enprint(msg)
        worklist = ticket
        data_path_socket.close()

        # Work has been read - wait for final dialog with file clerk
        done_ticket = callback.read_tcp_socket(control_socket, "file clerk"\
                  +"client get_bfids, fc final dialog")
        control_socket.close()
        if done_ticket["status"][0] != e_errors.OK:
            msg = "get_bfids "\
                  +"2nd (post-work-read) file clerk callback on socket "\
                  +repr(address)+", failed to transfer: "\
                  +"ticket[\"status\"]="+ticket["status"]
            Trace.trace(0,msg)
            raise errno.errorcode[errno.EPROTO],msg

        Trace.trace(16,"}get_bfids")
        return worklist

    def tape_list(self,external_label):
        Trace.trace(16,"{tape_list")
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
            new_ticket = callback.read_tcp_socket(control_socket, "file"+\
                                  "clerk client tape_list,  fc call back")
            if ticket["unique_id"] == new_ticket["unique_id"]:
                listen_socket.close()
                break
            else:
	        self.enprint("tape_list - imposter called us back, trying again")
                control_socket.close()
        ticket = new_ticket
        if ticket["status"][0] != e_errors.OK:
            msg = "tape_list: "\
                  +"1st (pre-work-read) file clerk callback on socket "\
                  +repr(address)+", failed to setup transfer: "\
                  +"ticket[\"status\"]="+ticket["status"]
            Trace.trace(0,msg)
            raise errno.errorcode[errno.EPROTO],msg
        # If the system has called us back with our own  unique id, call back
        # the library manager on the library manager's port and read the
        # work queues on that port.
        data_path_socket = callback.file_server_callback_socket(ticket)
        ticket= callback.read_tcp_socket(data_path_socket, "file clerk"\
                  +"client tape_list, fc final dialog")
        workmsg=""
        while 1: ## XXX warning read_tcp_buf can over-read
          msg=callback.read_tcp_buf(data_path_socket,"file  clerk client tape_list, reading worklist")
          #print msg
          if len(msg)==0:
              #print "break"
              break
          workmsg = workmsg+msg
        worklist = ticket
        worklist['tape_list'] = workmsg
        data_path_socket.close()

        # Work has been read - wait for final dialog with file clerk
        done_ticket = callback.read_tcp_socket(control_socket, "file clerk"\
                  +"client tape_list, fc final dialog")
        control_socket.close()
        if done_ticket["status"][0] != e_errors.OK:
            msg = "tape_list "\
                  +"2nd (post-work-read) file clerk callback on socket "\
                  +repr(address)+", failed to transfer: "\
                  +"ticket[\"status\"]="+ticket["status"]
            Trace.trace(0,msg)
            raise errno.errorcode[errno.EPROTO],msg

        Trace.trace(16,"}tape_list")
        print workmsg
        return worklist


    def bfid_info(self):
        Trace.trace(10,"{bfid_info")
        r = self.send({"work" : "bfid_info",\
                       "bfid" : self.bfid } )
        Trace.trace(10,"}bfid_info"+repr(r))
        return r

    def set_deleted(self,deleted):
        Trace.trace(10,"{set_delete")
        r = self.send({"work"   : "set_deleted",
                       "bfid"   : self.bfid,
                       "deleted": deleted} )
        Trace.trace(10,"}set_delete"+repr(r))
        return r

class FileClerkClientInterface(generic_client.GenericClientInterface):

    def __init__(self):
        Trace.trace(10,'{fci.__init__')
        # fill in the defaults for the possible options
        self.bfids = 0
        self.tape_list = 0
        self.bfid = 0
        self.backup = 0
        self.deleted = 0
        self.alive_rcv_timeout = 0
        self.alive_retries = 0
        generic_client.GenericClientInterface.__init__(self)
        Trace.trace(10,'}fci.__init')

    # define the command line options that are valid
    def options(self):
        Trace.trace(16,"{}options")
        return self.client_options()+["bfids","bfid=","deleted=","tape_list=","backup"]


if __name__ == "__main__" :
    import sys
    Trace.init("FC client")
    Trace.trace(6,"fcc called with args "+repr(sys.argv))

    # fill in interface
    intf = FileClerkClientInterface()

    # now get a file clerk client
    fcc = FileClient(0, intf.verbose, intf.config_host, \
                          intf.config_port, intf.bfid)

    if intf.alive:
        ticket = fcc.alive(intf.alive_rcv_timeout,intf.alive_retries)
	msg_id = generic_cs.ALIVE

    elif intf.got_server_verbose:
        ticket = fcc.set_verbose(intf.server_verbose, intf.alive_rcv_timeout,\
	                         intf.alive_retries)
	msg_id = generic_cs.CLIENT

    elif intf.backup:
        ticket = fcc.start_backup()
        db.do_backup("file")
        ticket = fcc.stop_backup()
	msg_id = generic_cs.CLIENT

    elif intf.deleted and intf.bfid:
        ticket = fcc.set_deleted(intf.deleted)
        try:
            if intf.verbose:
                generic_cs.enprint(ticket, generic_cs.PRETTY_PRINT)
        except:
            pass
	msg_id = generic_cs.CLIENT

    elif intf.bfids:
        ticket = fcc.get_bfids()
	msg_id = generic_cs.CLIENT

    elif intf.tape_list:
        ticket = fcc.tape_list(intf.tape_list)
	msg_id = generic_cs.CLIENT
        aticket = fcc.alive(intf.alive_rcv_timeout,intf.alive_retries) #clear out any zombies from the forked file clerk

    elif intf.bfid:
        ticket = fcc.bfid_info()
	if ticket['status'][0] ==  e_errors.OK:
	    generic_cs.enprint(ticket['fc'], generic_cs.PRETTY_PRINT)
	    generic_cs.enprint(ticket['vc'], generic_cs.PRETTY_PRINT)
	msg_id = generic_cs.CLIENT

    else:
	intf.print_help()
        sys.exit(0)

    fcc.check_ticket(ticket, msg_id)
