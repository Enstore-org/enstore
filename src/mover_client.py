###############################################################################
# src/$RCSfile$   $Revision$
#
#########################################################################
#                                                                       #
# Mover client.                                                         #
# Mover access methods                                                  #
#                                                                       #
#########################################################################

# system imports
import sys
import string
import pprint

#enstore imports
import udp_client
import interface
import generic_client
import Trace

class MoverClient(generic_client.GenericClient):
    def __init__(self, csc, name=""):
        self.mover=name
        self.log_name = "C_"+string.upper(name)
        generic_client.GenericClient.__init__(self, csc, self.log_name)
        self.u = udp_client.UDPClient()

    def status(self, rcv_timeout=0, tries=0):
	return self.send({"work" : "status"}, rcv_timeout, tries)

    def local_mover(self, enable, rcv_timeout=0, tries=0):
	return self.send({"work" : "local_mover",
                          "enable" : enable}, rcv_timeout, tries)

    def getlist(self, work,timeout=20, tries=2):
        # Import SOCKS module if it exists, else standard socket module socket
        # This is a python module that works just like the socket module, but uses the
        # SOCKS protocol to make connections through a firewall machine.
        # See http://www.w3.org/People/Connolly/support/socksForPython.html or
        # goto www.python.org and search for "import SOCKS"
        try:
            import SOCKS
            socket = SOCKS
        except ImportError:
            import socket
        import callback
        import e_errors
        import time
        # get a port to talk on and listen for connections
        host, port, listen_socket = callback.get_callback()
        listen_socket.listen(4)
        ticket = {"work"         : work,
                  "callback_addr" : (host, port),
                  "unique_id"    : time.time() }

        # send the work ticket to the library manager
        ticket = self.send(ticket, timeout,tries)
        if ticket['status'][0] != e_errors.OK:
            raise errno.errorcode[errno.EPROTO],"mvc."+work+": sending ticket"\
                  +repr(ticket)

        # We have placed our request in the system and now we have to wait.
        # All we  need to do is wait for the system to call us back,
        # and make sure that is it calling _us_ back, and not some sort of old
        # call-back to this very same port. It is dicey to time out, as it
        # is probably legitimate to wait for hours....
        while 1 :
            control_socket, address = listen_socket.accept()
            new_ticket = callback.read_tcp_obj(control_socket)
            if ticket["unique_id"] == new_ticket["unique_id"] :
                listen_socket.close()
                break
            else:
                Trace.trace(9,"mvc.%s: imposter called us back, trying again"\
                            %work)
                control_socket.close()
        ticket = new_ticket
        if ticket["status"][0] != e_errors.OK:
            raise errno.errorcode[errno.EPROTO],"mvc."+work+": "\
                  +"1st (pre-work-read) mover callback on socket "\
                  +repr(address)+", failed to setup transfer: "\
                  +"ticket[\"status\"]="+ticket["status"]

        # If the system has called us back with our own  unique id, call back
        # the library manager on the library manager's port and read the
        # work queues on that port.
        data_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        data_sock.connect(ticket['callback_host'], \
                     ticket['callback_port'])
        
        worklist = callback.read_tcp_obj(data_sock)
        data_sock.close()

        # Work has been read - wait for final dialog with library manager.
        done_ticket = callback.read_tcp_obj(control_socket)
        control_socket.close()
        if done_ticket["status"][0] != e_errors.OK:
            raise errno.errorcode[errno.EPROTO],"lmc."+work+": "\
                  +"2nd (post-work-read) mover callback on socket "\
                  +repr(address)+", failed to transfer: "\
                  +"ticket[\"status\"]="+ticket["status"]
        return worklist


    def clean_drive(self, rcv_timeout=0, tries=0):
	return self.getlist("clean_drive")

    def start_draining(self, rcv_timeout=0, tries=0):
	return self.send({"work" : "start_draining"}, rcv_timeout, tries)

    # do not add stop_draining as an option.  Once a mover is put into a start_draining
    # state, the only correct thing to do is to stop the mvoer and restart it.  Otherwise
    # the library manager gets totally confused.  The only reason to put a mover into a
    # draining state is to stop it.
    #def stop_draining(self, rcv_timeout=0, tries=0):
    #	return self.send({"work" : "stop_draining"}, rcv_timeout, tries)

    def send (self, ticket, rcv_timeout=0, tries=0) :
        vticket = self.csc.get(self.mover)
        return self.u.send(ticket, (vticket['hostip'], vticket['port']), \
	                rcv_timeout, tries)

class MoverClientInterface(generic_client.GenericClientInterface):
    def __init__(self, flag=1, opts=[]):
        self.do_parse = flag
        self.restricted_opts = opts
        self.alive_rcv_timeout = 0
        self.alive_retries = 0
        self.mover = ""
        self.local_mover = 0
        self.clean_drive = 0
        self.enable = 0
	self.status = 0
        self.start_draining = 0
        self.stop_draining = 0
        generic_client.GenericClientInterface.__init__(self)

    # define the command line options that are valid
    def options(self):
        if self.restricted_opts:
            return self.restricted_opts
        else:
            return self.client_options()+["status", "local_mover=", "clean_drive", "start_draining" ]

    #  define our specific help
    def parameters(self):
        return "mover"

    # parse the options like normal but make sure we have a mover name
    def parse_options(self):
        interface.Interface.parse_options(self)
        # bomb out if we don't have a mover
        if len(self.args) < 1 :
	    self.missing_parameter(self.parameters())
            self.print_help()
            sys.exit(1)
        else:
            self.mover = self.args[0]


def do_work(intf):
    # get a mover client
    movc = MoverClient((intf.config_host, intf.config_port), intf.mover)
    Trace.init(movc.get_name(movc.log_name))

    ticket = {}
    msg_id = None

    if intf.alive:
        ticket = movc.alive(intf.mover, intf.alive_rcv_timeout,
                            intf.alive_retries)
    elif intf.status:
        ticket = movc.status(intf.alive_rcv_timeout,intf.alive_retries)
	pprint.pprint(ticket)
    elif intf.local_mover:
        ticket = movc.local_mover(intf.enable, intf.alive_rcv_timeout,
                                  intf.alive_retries)
    elif intf.clean_drive:
        ticket = movc.clean_drive(intf.alive_rcv_timeout, intf.alive_retries)
        print ticket
    elif intf.start_draining:
        ticket = movc.start_draining(intf.alive_rcv_timeout, intf.alive_retries)
    elif intf.stop_draining:
        ticket = movc.stop_draining(intf.alive_rcv_timeout, intf.alive_retries)
    else:
	intf.print_help()
        sys.exit(0)

    del movc.csc.u
    del movc.u		# del now, otherwise get name exception (just for python v1.5???)

    movc.check_ticket(ticket)

if __name__ == "__main__" :
    Trace.init("MOVER_CLI")
    Trace.trace(6,"movc called with args "+repr(sys.argv))

    # fill in the interface
    intf = MoverClientInterface()

    do_work(intf)
