###############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
import time
import string

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

    def __init__(self, csc=0, verbose=0, host=interface.default_host(), \
                 port=interface.default_port(), bfid=0):
        # we always need to be talking to our configuration server
        Trace.trace(10,'{__init__')
	self.print_id = "FILECC"
        configuration_client.set_csc(self, csc, host, port, verbose)
        self.u = udp_client.UDPClient()
	self.bfid = bfid
	self.verbose = verbose
        ticket = self.csc.get("file_clerk")
	try:
            self.print_id = ticket['logname']
        except:
            pass
        Trace.trace(10,'}__init')

    def send (self, ticket, rcv_timeout=0, tries=0):
        Trace.trace(12,"{send"+repr(ticket))
        # who's our file clerk server that we should send the ticket to?
        vticket = self.csc.get("file_clerk")
        # send user ticket and return answer back
        Trace.trace(12,"send addr="+repr((vticket['hostip'], vticket['port'])))
        s = self.u.send(ticket, (vticket['hostip'], vticket['port']), rcv_timeout, tries)
        Trace.trace(12,"}send"+repr(s))
        return s

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
        workmsg=""
        while 1:
          msg=callback.read_tcp_buf(data_path_socket,"file  clerk "+"client get_bfids, reading worklist")
          if len(msg)==0:
	        generic_cs.enprint(workmsg)
                break
          workmsg = workmsg+msg
	  generic_cs.enprint(workmsg[:string.rfind( workmsg,',',0)])
          workmsg=msg[string.rfind(msg,',',0)+1:]
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


    def bfid_info(self):
        Trace.trace(10,"{bfid_info")
        r = self.send({"work" : "bfid_info",\
                       "bfid" : self.bfid } )
        Trace.trace(10,"}bfid_info"+repr(r))
        return r

class FileClerkClientInterface(interface.Interface):

    def __init__(self):
        Trace.trace(10,'{fci.__init__')
        # fill in the defaults for the possible options
        self.bfids = 0
        self.bfid = 0
        self.alive = 0
        self.backup = 0
        self.alive_rcv_timeout = 0
        self.alive_retries = 0
	self.got_server_verbose = 0
        interface.Interface.__init__(self)

        # now parse the options
        self.parse_options()
        Trace.trace(10,'}fci.__init')

    # define the command line options that are valid
    def options(self):
        Trace.trace(16,"{}options")
        return self.config_options()+self.verbose_options()+\
               ["bfids","bfid=","backup"] +\
               self.alive_options()+self.help_options()



if __name__ == "__main__" :
    import sys
    Trace.init("FC client")
    Trace.trace(1,"fcc called with args "+repr(sys.argv))

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

    elif intf.bfids:
        ticket = fcc.get_bfids()
	msg_id = generic_cs.CLIENT

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
