from configuration_client import configuration_client, set_csc
import generic_client_server
import generic_client
import backup_client
from udp_client import UDPClient
from db import do_backup
import callback
import time
import string
import Trace

class FileClerkClient(generic_client_server.GenericClientServer, \
                      generic_client.GenericClient, \
                      backup_client.BackupClient) :

    def __init__(self, csc=[], \
                 host=generic_client_server.default_host(), \
                 port=generic_client_server.default_port()) :
        # we always need to be talking to our configuration server
        self.config_list = 0
        self.doalive = 0
        self.dolist = 0
  	self.bfid = 0
    	self.bfids = 0
    	self.backup=0
        set_csc(self, csc, host, port)
        self.u = UDPClient()

    # define the command line options that are valid
    def options(self):
        return generic_client_server.GenericClientServer.config_options(self)+\
      	       generic_client_server.GenericClientServer.list_options(self)  +\
	       ["config_list","bfids","bfid=","alive","backup"] +\
	       generic_client_server.GenericClientServer.options(self)

    def send (self, ticket) :
        # who's our file clerk server that we should send the ticket to?
        vticket = self.csc.get("file_clerk")
        # send user ticket and return answer back
        return self.u.send(ticket, (vticket['host'], vticket['port']) )

    def read_from_hsm(self, ticket) :
        return self.send(ticket)
    """
    def new_bit_file(self, bof_space_cookie \
                         , external_label \
                         , sanity_cookie \
                         , complete_crc ) :
        return self.send({"work"             : "new_bit_file", \
                          "bof_space_cookie" : bof_space_cookie, \
                          "external_label"   : external_label, \
                          "sanity_cookie"    : sanity_cookie, \
                          "complete_crc"     : complete_crc })
    """
    """ To keep it consistent with approach to transferring a ticket
        send the whole ticket to file clerk
    """
    def new_bit_file(self, ticket) :
        return self.send(ticket)

    def get_bfids(self):

        host, port, listen_socket = callback.get_callback()
        listen_socket.listen(4)
        ticket = {"work"               : "get_bfids",
                  "user_callback_port" : port,
                  "user_callback_host" : host,
                  "unique_id"          : time.time() }
        # send the work ticket to the library manager
        ticket = self.send(ticket)
        if ticket['status'] != "ok" :
            raise errno.errorcode[errno.EPROTO],"fcc.get_bfids: sending ticket"+repr(ticket)

        # We have placed our request in the system and now we have to wait.
        # All we  need to do is wait for the system to call us back,
        # and make sure that is it calling _us_ back, and not some sort of old
        # call-back to this very same port. It is dicey to time out, as it
        # is probably legitimate to wait for hours....
        while 1 :
            control_socket, address = listen_socket.accept()
            new_ticket = callback.read_tcp_socket(control_socket, "file"+\
                                  "clerk client get_bfids,  fc call back")
            import pprint
            if ticket["unique_id"] == new_ticket["unique_id"] :
                listen_socket.close()
                break
            else:
                print ("fcc:get_bfids: imposter called us back, trying again")
                control_socket.close()
        ticket = new_ticket
        if ticket["status"] != "ok" :
            raise errno.errorcode[errno.EPROTO],"fcc:get_bfids: "\
                  +"1st (pre-work-read) file clerk callback on socket "\
                  +repr(address)+", failed to setup transfer: "\
                  +"ticket[\"status\"]="+ticket["status"]

        # If the system has called us back with our own  unique id, call back
        # the library manager on the library manager's port and read the
        # work queues on that port.
        data_path_socket = callback.file_clerk_callback_socket(ticket)
	ticket= callback.read_tcp_socket(data_path_socket, "file clerk"\
		  +"client get_bfids, fc final dialog")
	workmsg=""
        while 1:
	  msg=callback.read_tcp_buf(data_path_socket,"file  clerk "+"client get_bfids, reading worklist")
	  if len(msg)==0 :
		pprint.pprint(workmsg)
		break
	  workmsg = workmsg+msg
	  pprint.pprint( workmsg[:string.rfind( workmsg,',',0)])
	  workmsg=msg[string.rfind(msg,',',0)+1:]
	worklist = ticket
	data_path_socket.close()


        # Work has been read - wait for final dialog with file clerk
        done_ticket = callback.read_tcp_socket(control_socket, "file clerk"\
		  +"client get_bfids, fc final dialog")
        control_socket.close()
        if done_ticket["status"] != "ok" :
            raise errno.errorcode[errno.EPROTO],"fcc.get_bfids "\
                  +"2nd (post-work-read) file clerk callback on socket "\
                  +repr(address)+", failed to transfer: "\
                  +"ticket[\"status\"]="+ticket["status"]
        return worklist








        return self.send({"work" : "get_bfids"} )

    def bfid_info(self):
        return self.send({"work" : "bfid_info",\
                          "bfid" : self.bfid } )

if __name__ == "__main__" :
    Trace.init("FC client")
    import sys
    import pprint

    # fill in defaults
    fcc = FileClerkClient()

    # see what the user has specified. bomb out if wrong options specified
    fcc.parse_options()
    fcc.csc.connect()

    if fcc.doalive:
        ticket = fcc.alive()
    elif fcc.backup:
	ticket = fcc.start_backup()
	do_backup("file")
	ticket = fcc.stop_backup()
    elif fcc.bfids :
        ticket = fcc.get_bfids()

    elif fcc.bfid :
        ticket = fcc.bfid_info()

    if ticket['status'] == 'ok' :
        if fcc.dolist:
            pprint.pprint(ticket)
        sys.exit(0)
    else :
        print "BAD STATUS:",ticket['status']
        pprint.pprint(ticket)
        sys.exit(1)


















