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
        self.log_name = "C_"+string.upper(string.replace(name,
                                                         ".library_manager",
                                                         MY_NAME))
        generic_client.GenericClient.__init__(self, csc, self.log_name)
        self.send_to = 20
        self.send_tries = 2
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

    def get_queue(self, node=None, lm=None):
        if not lm: lmname = "library_manager"
        else: lmname = lm
        keys = self.csc.get_keys()
        for key in keys['get_keys']:
            if string.find(key, lmname) != -1:
               self.name = key
               lst = self.getwork()
               pw_list = lst["pending_work"]
               at_list = lst["at movers"]
               for i in range(0, len(pw_list)):
                   host = pw_list[i]["wrapper"]["machine"][1]
                   user = pw_list[i]["wrapper"]["uname"]
                   pnfsfn = pw_list[i]["wrapper"]["pnfsFilename"]
                   fn = pw_list[i]["wrapper"]["fullname"]
                   at_top = pw_list[i]["at_the_top"]
                   reject_reason = ""
                   if pw_list[i].has_key('reject_reason'):
                       reject_reason = repr(pw_list[i]['reject_reason'])
                   if (host == node) or (not node):
                       print "%s %s %s %s %s P %d %s" % (host,self.name,user,pnfsfn,fn, at_top, reject_reason)
               for i in range(0, len(at_list)):
                   host = at_list[i]["wrapper"]["machine"][1]
                   user = at_list[i]["wrapper"]["uname"]
                   pnfsfn = at_list[i]["wrapper"]["pnfsFilename"]
                   fn = at_list[i]["wrapper"]["fullname"]
                   if (host == node) or (not node):
                       print "%s %s %s %s %s M" % (host,self.name, user,pnfsfn,fn)
               
        return {"status" :(e_errors.OK, None)}

    def getmoverlist(self):
	return self.getlist("getmoverlist")

    def get_suspect_volumes(self):
	return self.getlist("get_suspect_volumes")

    def get_delayed_dismounts(self):
	return self.getlist("get_delayed_dismounts")

    def remove_work(self, id):
	print "ID", id
	return self.send({"work":"remove_work", "unique_id": id})

    def priority(self, id, pri):
	return self.send({"work":"change_priority", "unique_id": id, "priority": pri})

    def load_mover_list(self):
	return self.send({"work":"load_mover_list"})

    def summon_mover(self, mover):
        if mover == 'all': mover=None
	return self.send({"work":"summon", "mover":mover})

    def poll(self):
	 return self.send({"work":"poll"})

    def getlist(self, work):
        # get a port to talk on and listen for connections
        host, port, listen_socket = callback.get_callback()
        listen_socket.listen(4)
        ticket = {"work"         : work,
                  "callback_addr" : (host, port),
                  "unique_id"    : time.time() }

        # send the work ticket to the library manager
        ticket = self.send(ticket, self.send_to,self.send_tries)
        if ticket['status'][0] != e_errors.OK:
            raise errno.errorcode[errno.EPROTO],"lmc.%s sending ticket %s"%(work,ticket)

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
                Trace.trace(9,"lmc.%s: imposter called us back, trying again" %(work,))
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
        worklist = callback.read_tcp_obj(data_path_socket)
        data_path_socket.close()

        # Work has been read - wait for final dialog with library manager.
        done_ticket = callback.read_tcp_obj(control_socket)
        control_socket.close()
        if done_ticket["status"][0] != e_errors.OK:
            raise errno.errorcode[errno.EPROTO],"lmc."+work+": "\
                  +"2nd (post-work-read) library manger callback on socket "\
                  +repr(address)+", failed to transfer: "\
                  +"ticket[\"status\"]="+ticket["status"]
        return worklist

class LibraryManagerClientInterface(generic_client.GenericClientInterface) :
    def __init__(self, flag=1, opts=[]) :
        # this flag if 1, means do everything, if 0, do no option parsing
        self.do_parse = flag
        self.restricted_opts = opts
        self.name = ""
        self.get_work = 0
        self.alive_rcv_timeout = 0
        self.alive_retries = 0
	self.get_mover_list = 0
	self.get_susp_vols = 0
	self.get_delayed_dismount = 0
	self.get_susp_vols = 0
        self.delete_work = 0
	self.priority = -1
	self.load_mover_list = 0
	self.summon = 0
	self.poll = 0
        self.queue_list = 0
        self.host = 0
        generic_client.GenericClientInterface.__init__(self)

    # define the command line options that are valid
    def options(self):
        if self.restricted_opts:
            return self.restricted_opts
        else:
            return self.client_options()+\
                   ["get_work", "get_mover_list", "get_suspect_vols",
                    "get_delayed_dismount","delete_work=","priority=",
                    "load_movers", "summon=", "poll", "get_queue","host="]

    # tell help that we need a library manager specified on the command line
    def parameters(self):
	return "library"

    # parse the options like normal but make sure we have a library manager
    def parse_options(self):
        interface.Interface.parse_options(self)
        # bomb out if we don't have a library
        if (len(self.args) < 1) and (not  self.queue_list) :
	    self.missing_parameter(self.parameters())
            self.print_help(),
            sys.exit(1)
        else:
	    if self.delete_work:
		if len(self.args) != 1:
		    self.print_delete_work_args()
		    sys.exit(1)
	    elif not self.priority == -1:
		if len(self.args) != 2:
		    self.print_priority_args()
		    sys.exit(1)
            if not  self.queue_list: self.name = self.args[0]

    # print delete _work arguments
    def print_delete_work_args(self):
        print "   delete arguments: library"

    # print priority arguments
    def print_priority_args(self):
        print "   priority arguments: library work_id"

def do_work(intf):
    # get a library manager client
    lmc = LibraryManagerClient((intf.config_host, intf.config_port), intf.name)
    Trace.init(lmc.get_name(lmc.log_name))

    if intf.alive:
        ticket = lmc.alive(intf.name, intf.alive_rcv_timeout,
                           intf.alive_retries)
    elif  intf.get_work:
        ticket = lmc.getwork()
	print ticket['pending_work']
	print ticket['at movers']
    elif  intf.get_mover_list:
	ticket = lmc.getmoverlist()
	print ticket['moverlist']
    elif  intf.get_susp_vols:
	ticket = lmc.get_suspect_volumes()
	print ticket['suspect_volumes']
    elif  intf.get_delayed_dismount:
	ticket = lmc.get_delayed_dismounts()
	print ticket['delayed_dismounts']
    elif intf.delete_work:
	ticket = lmc.remove_work(intf.work_to_delete)
	print repr(ticket)
    elif not intf.priority == -1:
	ticket = lmc.priority(intf.args[1], intf.priority)
	print repr(ticket)
    elif intf.load_mover_list:
	ticket = lmc.load_mover_list()
	print repr(ticket)
    elif intf.summon:
	ticket = lmc.summon_mover(intf.summon)
	print repr(ticket)
    elif intf.poll:
	ticket = lmc.poll()
	print repr(ticket)
    elif intf.queue_list:
        if len(intf.args) >= 1:
            if intf.args[0]: 
                host = intf.args[0]
            else: host = None
            if len(intf.args) == 2:
                lm = intf.args[1]
            else: lm = None
        else:
            host = None
            lm = None
	ticket = lmc.get_queue(host, lm)
	print repr(ticket)
	
    else:
	intf.print_help()
        sys.exit(0)

    del lmc.csc.u
    del lmc.u		# del now, otherwise get name exception (just for python v1.5???)

    lmc.check_ticket(ticket)

if __name__ == "__main__" :
    Trace.init("LIBM_CLI")
    Trace.trace(6,"lmc called with args "+repr(sys.argv))

    # fill in the interface
    intf = LibraryManagerClientInterface()

    do_work(intf)
