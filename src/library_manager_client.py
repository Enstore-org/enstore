###############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
import time
import errno
import sys
import string
import socket

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
        self.server_address = self.get_server_address(self.name)

    def write_to_hsm(self, ticket) :
        return self.send(ticket)

    def read_from_hsm(self, ticket) :
        return self.send(ticket)

    def getwork(self) :
	return self.getlist("getwork")

    def get_queue(self, node=None, lm=None):
        if not lm: lmname = "library_manager"
        else: lmname = lm
        pending_read_cnt = 0
        pending_write_cnt = 0
        active_read_cnt = 0
        active_write_cnt = 0
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
                   reject_reason = ("","")
                   if pw_list[i].has_key('reject_reason'):
                       reject_reason = pw_list[i]['reject_reason']
                   if (host == node) or (not node):
                       print "%s %s %s %s %s P %d %s %s" % (host,self.name,user,pnfsfn,fn, at_top, reject_reason[0], reject_reason[1])
                       if pw_list[i]["work"] == "read_from_hsm":
                          pending_read_cnt = pending_read_cnt + 1
                       elif pw_list[i]["work"] == "write_to_hsm":
                           pending_write_cnt = pending_write_cnt + 1
                           
               for i in range(0, len(at_list)):
                   host = at_list[i]["wrapper"]["machine"][1]
                   user = at_list[i]["wrapper"]["uname"]
                   pnfsfn = at_list[i]["wrapper"]["pnfsFilename"]
                   fn = at_list[i]["wrapper"]["fullname"]
                   if at_list[i]["vc"].has_key("external_label"):
                       vol = at_list[i]["vc"]["external_label"]
                   else:
                       vol = at_list[i]["fc"]["external_label"]
                   if at_list[i].has_key("mover"):
                       mover = at_list[i]["mover"]
                   else:
                       mover = ''
                   if (host == node) or (not node):
                       print "%s %s %s %s %s M %s %s" % (host,self.name, user,pnfsfn,fn, mover, vol)
                       if at_list[i]["work"] == "read_from_hsm":
                          active_read_cnt = active_read_cnt + 1
                       elif at_list[i]["work"] == "write_to_hsm":
                           active_write_cnt = active_write_cnt + 1
        print "Pending read requests: ", pending_read_cnt
        print "Pending write requests: ", pending_write_cnt
        print "Active read requests: ", active_read_cnt
        print "Active write requests: ", active_write_cnt
                           
        return {"status" :(e_errors.OK, None)}

    def get_suspect_volumes(self):
	return self.getlist("get_suspect_volumes")

    def remove_work(self, id):
	print "ID", id
	return self.send({"work":"remove_work", "unique_id": id})

    def change_lm_state(self, state):
        return self.send({"work":"change_lm_state", "state": state})

    def get_lm_state(self, timeout=0, tries=0):
        return self.send({"work":"get_lm_state"}, timeout, tries)
        
    # remove volume from suspect volume list
    def remove_suspect_volume(self, volume):
        return self.send({"work":"remove_suspect_volume", "volume":volume})

    # remove volume from suspect volume list
    def remove_active_volume(self, volume):
        return self.send({"work":"remove_active_volume", "external_label":volume})

    def priority(self, id, pri):
	return self.send({"work":"change_priority", "unique_id": id, "priority": pri})

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
        # call-back to this very same port. 
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
        
        data_path_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        data_path_socket.connect(ticket['library_manager_callback_addr'])
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

    # get active volume known to LM
    def get_active_volumes(self):
        ticket = self.send({"work":"get_active_volumes"})
        print "%-10s  %-17s %-17s %-17s %17s %10s %11s"%(
            "label","mover","volume family",
            "system_inhibit","user_inhibit","status","updated")
        for mover in ticket['movers']:
            print "%-10s  %-17s %-17s (%-08s %08s) (%-08s %08s) %-10s %-11s" %\
            (mover['external_label'], mover['mover'],
             mover['volume_family'],
             mover['volume_status'][0][0], mover['volume_status'][0][1],
             mover['volume_status'][1][0], mover['volume_status'][1][1],
             mover['state'],
             time.ctime(mover['updated']))
        return ticket
            
    def storage_groups(self):
        ticket = self.send({"work":"storage_groups"})
        print "%-14s %-12s" % ('storage group', 'limit')
        for sg in ticket['storage_groups']['limits'].keys():
            print "%-14s %-12s" % (sg, ticket['storage_groups']['limits'][sg])
                         
        return ticket
        

class LibraryManagerClientInterface(generic_client.GenericClientInterface) :
    def __init__(self, flag=1, opts=[]) :
        # this flag if 1, means do everything, if 0, do no option parsing
        self.do_parse = flag
        self.restricted_opts = opts
        self.name = ""
        self.get_work = 0
        self.alive_rcv_timeout = 0
        self.alive_retries = 0
	self.get_susp_vols = 0
	self.get_susp_vols = 0
        self.delete_work = 0
	self.priority = -1
	self.poll = 0
        self.get_queue = None
        self.host = 0
        self.start_draining = 0
        self.stop_draining = 0
        self.status = 0
        self.vols = 0
        self.storage_groups = 0
        self.rm_suspect_vol = 0
        self.rm_active_vol = 0
        generic_client.GenericClientInterface.__init__(self)

    # define the command line options that are valid
    def options(self):
        if self.restricted_opts:
            return self.restricted_opts
        else:
            return self.client_options()+\
                   ["get_work", "get_suspect_vols",
                    "delete_work=","priority=",
                    "poll", "get_queue=","host=",
                    "start_draining=", "stop_draining", "status", "vols",
                    "storage_groups", "rm_suspect_vol=","rm_active_vol="]

    # tell help that we need a library manager specified on the command line
    def parameters(self):
	return "library"

    # parse the options like normal but make sure we have a library manager
    def parse_options(self):
        interface.Interface.parse_options(self)
        # bomb out if we don't have a library
        if len(self.args) < 1:
	    self.missing_parameter(self.parameters())
            self.print_help()
            sys.exit(1)
        else:
            self.name = self.args[0]
            
    # print delete _work arguments
    def print_delete_work_args(self):
        print "   delete arguments: library"

    # print priority arguments
    def print_priority_args(self):
        print "   priority arguments: library work_id"

def do_work(intf):
    # get a library manager client
    print "DO WORK: name=", intf.name
    lmc = LibraryManagerClient((intf.config_host, intf.config_port), intf.name)
    Trace.init(lmc.get_name(lmc.log_name))

    ticket = lmc.handle_generic_commands(intf.name, intf)
    if ticket:
        pass
    elif  intf.get_work:
        ticket = lmc.getwork()
	print ticket['pending_work']
	print ticket['at movers']
    elif  intf.get_susp_vols:
	ticket = lmc.get_suspect_volumes()
	print ticket['suspect_volumes']
    elif intf.delete_work:
	ticket = lmc.remove_work(intf.work_to_delete)
	print repr(ticket)
    elif intf.rm_suspect_vol:
        ticket = lmc.remove_suspect_volume(intf.suspect_volume)
    elif intf.rm_active_vol:
        ticket = lmc.remove_active_volume(intf.active_volume)
    elif not intf.priority == -1:
	ticket = lmc.priority(intf.args[1], intf.priority)
	print repr(ticket)
    elif intf.poll:
	ticket = lmc.poll()
	print repr(ticket)
    elif intf.vols:
	ticket = lmc.get_active_volumes()
    elif intf.storage_groups:
        ticket = lmc.storage_groups()

    elif intf.get_queue != None:
	ticket = lmc.get_queue(intf.get_queue, intf.name)
	print repr(ticket)
        
    elif (intf.start_draining or intf.stop_draining):
        if intf.start_draining:
            if intf.start_draining == 'lock': lock = 'locked'
            elif not (intf.start_draining in ('ignore', 'pause')):
                print "only 'lock', 'ignore' and 'pause' are valid for start_draining option"
                sys.exit(0)
            else: lock = intf.start_draining
        else: lock = 'unlocked'
        ticket = lmc.change_lm_state(lock)
    elif (intf.status):
        ticket = lmc.get_lm_state()
        print "LM state:%s"%(ticket['state'],)
	
    else:
	intf.print_help()
        sys.exit(0)

    lmc.check_ticket(ticket)

if __name__ == "__main__" :
    Trace.init("LIBM_CLI")
    Trace.trace(6,"lmc called with args "+repr(sys.argv))

    # fill in the interface
    intf = LibraryManagerClientInterface()

    do_work(intf)
