###############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
import time
import errno

#enstore imports
import configuration_client
import callback
import interface
import generic_client
import generic_cs
import udp_client
import Trace
import e_errors

def getlist(self, work):
    # get a port to talk on and listen for connections
    host, port, listen_socket = callback.get_callback()
    listen_socket.listen(4)
    ticket = {"work"         : work,
              "callback_addr" : (host, port),
              "unique_id"    : time.time() }

    # send the work ticket to the library manager
    ticket = self.send(ticket)
    if ticket['status'][0] != e_errors.OK:
        raise errno.errorcode[errno.EPROTO],"lmc."+work+": sending ticket"\
              +repr(ticket)

    # We have placed our request in the system and now we have to wait.
    # All we  need to do is wait for the system to call us back,
    # and make sure that is it calling _us_ back, and not some sort of old
    # call-back to this very same port. It is dicey to time out, as it
    # is probably legitimate to wait for hours....
    while 1 :
        control_socket, address = listen_socket.accept()
        new_ticket = callback.read_tcp_socket(control_socket, "library "+\
                                     "manager "+ work + ",  mover call back")
        if ticket["unique_id"] == new_ticket["unique_id"] :
            listen_socket.close()
            break
        else:
	    self.enprint("lmc."+work+\
                         ": imposter called us back, trying again")
            control_socket.close()
    ticket = new_ticket
    if ticket["status"][0] != e_errors.OK:
        raise errno.errorcode[errno.EPROTO],"lmc."+work+": "\
              +"1st (pre-work-read) library manager callback on socket "\
              +repr(address)+", failed to setup transfer: "\
              +"ticket[\"status\"]="+ticket["status"]

    # If the system has called us back with our own  unique id, call back
    # the library manager on the library manager's port and read the
    # work queues on that port.
    data_path_socket = callback.library_manager_callback_socket(ticket)
    worklist = callback.read_tcp_socket(data_path_socket,"library "+\
                                "manager "+work+", reading worklist")
    data_path_socket.close()

    # Work has been read - wait for final dialog with library manager.
    done_ticket = callback.read_tcp_socket(control_socket, "library "+\
                                  "manager "+work+", mover final dialog")
    control_socket.close()
    if done_ticket["status"][0] != e_errors.OK:
        raise errno.errorcode[errno.EPROTO],"lmc."+work+": "\
              +"2nd (post-work-read) library manger callback on socket "\
              +repr(address)+", failed to transfer: "\
              +"ticket[\"status\"]="+ticket["status"]
    return worklist

class LibraryManagerClient(generic_client.GenericClient) :
    def __init__(self, csc=0, verbose=0, name="", \
                 host=interface.default_host(), port=interface.default_port()):
        self.name=name
	self.verbose = verbose
	self.print_id = "LIBMANC"
        # we always need to be talking to our configuration server
        configuration_client.set_csc(self, csc, host, port, verbose)
        self.u = udp_client.UDPClient()

    def send (self, ticket, rcv_timeout=0, tries=0) :
        # who's our library manager that we should send the ticket to?
        lticket = self.csc.get(self.name)
        # send user ticket and return answer back
        return self.u.send(ticket, (lticket['hostip'], lticket['port']), rcv_timeout, tries )


    def write_to_hsm(self, ticket) :
        return self.send(ticket)

    def read_from_hsm(self, ticket) :
        return self.send(ticket)

    def getwork(self,verbose) :
	return getlist(self,"getwork")

    def getmoverlist(self):
	return getlist(self,"getmoverlist")

    def get_suspect_volumes(self):
	return getlist(self,"get_suspect_volumes")

class LibraryManagerClientInterface(interface.Interface) :
    def __init__(self) :
        self.name = ""
        self.getwork = 0
        self.alive = 0
        self.alive_rcv_timeout = 0
        self.alive_retries = 0
	self.getmoverlist = 0
        interface.Interface.__init__(self)

	# parse the options
        self.parse_options()

    # define the command line options that are valid
    def options(self):
        return self.config_options()+\
	       ["verbose=", "getwork", "getmoverlist", "get_suspect_vols"] +\
	       self.alive_options()+self.help_options()

    #  define our specific help
    def help_line(self):
        return interface.Interface.help_line(self)+" library"

    # parse the options like normal but make sure we have a library manager
    def parse_options(self):
        interface.Interface.parse_options(self)
        # bomb out if we don't have a library
        if len(self.args) < 1 :
            self.print_help(),
            sys.exit(1)
        else:
            self.name = self.args[0]

if __name__ == "__main__" :
    import sys
    Trace.init("libm cli")
    Trace.trace(1,"lmc called with args "+repr(sys.argv))

    # fill in the interface
    intf = LibraryManagerClientInterface()

    # get a library manager client
    lmc = LibraryManagerClient(0, intf.verbose, intf.name,
                               intf.config_host, intf.config_port)

    if intf.alive:
        ticket = lmc.alive(intf.alive_rcv_timeout,intf.alive_retries)
	msg_id = generic_cs.ALIVE
    elif  intf.getwork:
        ticket = lmc.getwork(intf.verbose)
	generic_cs.enprint(ticket['pending_work'], generic_cs.PRETTY_PRINT)
	generic_cs.enprint(ticket['at movers'], generic_cs.PRETTY_PRINT)
	msg_id = generic_cs.CLIENT
    elif  intf.getmoverlist:
	ticket = lmc.getmoverlist()
	generic_cs.enprint(ticket['moverlist'], generic_cs.PRETTY_PRINT)
	msg_id = generic_cs.CLIENT
    elif  intf.get_susp_vols:
	ticket = lmc.get_suspect_volumes()
	generic_cs.enprint(ticket['suspect_volumes'], generic_cs.PRETTY_PRINT)
	msg_id = generic_cs.CLIENT

    del lmc.csc.u
    del lmc.u		# del now, otherwise get name exception (just for python v1.5???)

    lmc.check_ticket(ticket, msg_id)

