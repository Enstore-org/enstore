###############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
import time
import errno
import sys
import string

#enstore imports
import callback
import interface
import generic_client
import udp_client
import Trace
import e_errors

MY_NAME = ".LM"

class LibraryManagerClient(generic_client.GenericClient) :
    def __init__(self, csc, name=""):
        self.name=name
        self.log_name = string.upper(string.replace(name, ".library_manager",
                                                    MY_NAME))
        generic_client.GenericClient.__init__(self, csc, self.log_name)
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

    def getwork(self) :
	return self.getlist("getwork")

    def getmoverlist(self):
	return self.getlist("getmoverlist")

    def get_suspect_volumes(self):
	return self.getlist("get_suspect_volumes")

    def get_delayed_dismounts(self):
	return self.getlist("get_delayed_dismounts")

    def remove_work(self, id):
	print "ID", id
	return self.send({"work":"remove_work", "unique_id": id})

    def change_priority(self, id, pri):
	return self.send({"work":"change_priority", "unique_id": id, "priority": pri})

    def load_mover_list(self):
	return self.send({"work":"load_mover_list"})

    def get_mc(self):
	mc = self.send({"work":"get_mc"})
	if mc:
	    return mc['mc']
	return None
	
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
                                                  "manager "+ work + \
                                                  ", mover call back")
            if ticket["unique_id"] == new_ticket["unique_id"] :
                listen_socket.close()
                break
            else:
                Trace.trace(9,"lmc.%s: imposter called us back, trying again"\
                            %work)
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
                                            "manager "+work+\
                                            ", reading worklist")
        data_path_socket.close()

        # Work has been read - wait for final dialog with library manager.
        done_ticket = callback.read_tcp_socket(control_socket, "library "+\
                                               "manager "+work+\
                                               ", mover final dialog")
        control_socket.close()
        if done_ticket["status"][0] != e_errors.OK:
            raise errno.errorcode[errno.EPROTO],"lmc."+work+": "\
                  +"2nd (post-work-read) library manger callback on socket "\
                  +repr(address)+", failed to transfer: "\
                  +"ticket[\"status\"]="+ticket["status"]
        return worklist

class LibraryManagerClientInterface(generic_client.GenericClientInterface) :
    def __init__(self) :
        self.name = ""
        self.getwork = 0
        self.alive_rcv_timeout = 0
        self.alive_retries = 0
	self.getmoverlist = 0
	self.get_susp_vols = 0
	self.get_del_dismounts = 0
	self.get_susp_vols = 0
	self.remove_work = 0
	self.change_priority = 0
        generic_client.GenericClientInterface.__init__(self)

    # define the command line options that are valid
    def options(self):
        return self.client_options()+\
	       ["getwork", "getmoverlist", "get_suspect_vols",
	       "get_del_dismount","del_work","change_priority","loadmovers"]

    # tell help that we need a library manager specified on the command line
    def parameters(self):
	return "library"

    # parse the options like normal but make sure we have a library manager
    def parse_options(self):
        interface.Interface.parse_options(self)
        # bomb out if we don't have a library
        if len(self.args) < 1 :
	    self.missing_parameter(self.parameters())
            self.print_help(),
            sys.exit(1)
        else:
	    if self.remove_work:
		if len(self.args) != 2:
		    self.print_remove_work_args()
		    sys.exit(1)
	    elif self.change_priority:
		if len(self.args) != 3:
		    self.print_change_priority_args()
		    sys.exit(1)
            self.name = self.args[0]

    # print remove_work arguments
    def print_remove_work_args(self):
        print "   remove_work arguments: library work_id"

    # print change_priority arguments
    def print_change_priority_args(self):
        print "   change_priority arguments: library work_id priority"

if __name__ == "__main__" :
    Trace.init("LIBM CLI")
    Trace.trace(6,"lmc called with args "+repr(sys.argv))

    # fill in the interface
    intf = LibraryManagerClientInterface()

    # get a library manager client
    lmc = LibraryManagerClient((intf.config_host, intf.config_port), intf.name)
    Trace.init(lmc.get_name(lmc.log_name))

    if intf.alive:
        ticket = lmc.alive(intf.alive_rcv_timeout,intf.alive_retries)
    elif  intf.getwork:
        ticket = lmc.getwork()
	print ticket['pending_work']
	print ticket['at movers']
    elif  intf.getmoverlist:
	ticket = lmc.getmoverlist()
	print ticket['moverlist']
    elif  intf.get_susp_vols:
	ticket = lmc.get_suspect_volumes()
	print ticket['suspect_volumes']
    elif  intf.get_del_dismounts:
	ticket = lmc.get_delayed_dismounts()
	print ticket['delayed_dismounts']
    elif intf.remove_work:
	ticket = lmc.remove_work(intf.args[1])
	print repr(ticket)
    elif intf.change_priority:
	ticket = lmc.change_priority(intf.args[1], string.atoi(intf.args[2]))
	print repr(ticket)
    elif intf.load_mover_list:
	ticket = lmc.load_mover_list()
	print repr(ticket)
	
    else:
	intf.print_help()
        sys.exit(0)

    del lmc.csc.u
    del lmc.u		# del now, otherwise get name exception (just for python v1.5???)

    lmc.check_ticket(ticket)

