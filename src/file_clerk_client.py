from configuration_client import configuration_client
from udp_client import UDPClient
from db import do_backup
import callback
import time
class FileClerkClient :

    def __init__(self, configuration_client) :
        # we always need to be talking to our configuration server
        self.csc = configuration_client
        self.u = UDPClient()

    def send (self, ticket) :
        # who's our file clerk server that we should send the ticket to?
        vticket = self.csc.get("file_clerk")
        # send user ticket and return answer back
        return self.u.send(ticket, (vticket['host'], vticket['port']) )

    def read_from_hsm(self, ticket) :
        return self.send(ticket)

    def new_bit_file(self, bof_space_cookie \
                         , external_label \
                         , sanity_cookie \
                         , complete_crc ) :
        return self.send({"work"             : "new_bit_file", \
                          "bof_space_cookie" : bof_space_cookie, \
                          "external_label"   : external_label, \
                          "sanity_cookie"    : sanity_cookie, \
                          "complete_crc"     : complete_crc })

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

    def bfid_info(self,bfid):
        return self.send({"work" : "bfid_info",\
                          "bfid" : bfid } )

    # check on alive status
    def alive(self):
        return self.send({'work':'alive'})
    def start_backup(self):
    	return self.send({'work':'start_backup'})
    def stop_backup(self):
    	return self.send({'work':'stop_backup'})	


if __name__ == "__main__" :
    import sys
    import getopt
    import string
    import pprint
    # Import SOCKS module if it exists, else standard socket module socket
    # This is a python module that works just like the socket module, but uses
    # the SOCKS protocol to make connections through a firewall machine.
    # See http://www.w3.org/People/Connolly/support/socksForPython.html or
    # goto www.python.org and search for "import SOCKS"
    try:
        import SOCKS; socket = SOCKS
    except ImportError:
        import socket

    # defaults
    #config_host = "localhost"
    (config_host,ca,ci) = socket.gethostbyaddr(socket.gethostname())
    config_port = "7500"
    config_list = 0
    bfid = 0
    bfids = 0
    list = 0
    backup=0	
    alive = 0

    # see what the user has specified. bomb out if wrong options specified
    options = ["config_host=","config_port=","config_list",
               "bfids","bfid=","list","verbose","alive","backup","help"]
    optlist,args=getopt.getopt(sys.argv[1:],'',options)
    for (opt,value) in optlist :
        if opt == "--config_host" :
            config_host = value
        elif opt == "--config_port" :
            config_port = value
        elif opt == "--config_list" :
            config_list = 1
        elif opt == "--bfids" :
            bfids = 1
        elif opt == "--bfid" :
            bfid = value
        elif opt == "--alive" :
            alive = 1
        elif opt == "--list" or opt == "--verbose":
            list = 1
	elif opt == "--backup":
	    backup = 1
        elif opt == "--help" :
            print "python ",sys.argv[0], options
            print "   do not forget the '--' in front of each option"
            sys.exit(0)

    # bomb out if can't translate host
    ip = socket.gethostbyname(config_host)

    # bomb out if port isn't numeric
    config_port = string.atoi(config_port)

    if config_list :
        print "Connecting to configuration server at ",config_host,config_port
    csc = configuration_client(config_host,config_port)

    fcc = FileClerkClient(csc)

    if alive:
        ticket = fcc.alive()
    elif backup:
	ticket = fcc.start_backup()
	do_backup("file")
	ticket = fcc.stop_backup()
    elif bfids :
        ticket = fcc.get_bfids()

    elif bfid :
        ticket = fcc.bfid_info(bfid)

    if ticket['status'] == 'ok' :
        if list:
            pprint.pprint(ticket)
        sys.exit(0)

    else :
        print "BAD STATUS:",ticket['status']
        pprint.pprint(ticket)
        sys.exit(1)





















