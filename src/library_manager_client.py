###############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
import time
import errno
import sys
import string
import socket
import select

#enstore imports
import callback
import hostaddr
import option
import generic_client
import udp_client
import Trace
import e_errors

MY_NAME = ".LM"

class LibraryManagerClient(generic_client.GenericClient) :
    def __init__(self, csc, name="", rcv_timeout = 20, rcv_tries = 3):
        self.name=name
        self.log_name = "C_"+string.upper(string.replace(name,
                                                         ".library_manager",
                                                         MY_NAME))
        generic_client.GenericClient.__init__(self, csc, self.log_name)
        self.send_to = rcv_timeout
        self.send_tries = rcv_tries
        self.server_address = self.get_server_address(self.name, self.send_to, self.send_tries)
        if not self.server_address:
            sys.stderr.write("%s does not exist\n"%(self.name,))
            sys.exit(1)

    def write_to_hsm(self, ticket) :
        return self.send(ticket)

    def read_from_hsm(self, ticket) :
        return self.send(ticket)

    def getwork(self) :
        return self.getlist("getwork")

    def getworks_sorted(self) :
        return self.getlist("getworks_sorted")

    #Print out the relavent information on the volume assert.
    def get_asserts(self):

        #Pending volume asserts get printed here.
        asserts = self.getlist("get_asserts").get("pending_asserts",[])
        if asserts:
            print "Pending assert requests"
        for assert_work in asserts:
            #Get and format the node the request came from.
            node = assert_work.get("wrapper", {}).get("machine", ("",)*6)[1]
            node = socket.getfqdn(node)
            #Get and format the library name.
            lib = assert_work.get("vc", {}).get("library", "Library_Manager")
            if lib != "Library_Manager" and lib[-16:] != ".library_manager":
                lib = lib + ".library_manager"
            #Get the username.
            user = assert_work.get("wrapper", {}).get("uname", "Unknown")
            #Get the volume name.
            volume = assert_work.get("vc", {}).get("external_label", "Unknown")

            #Print the volume information.
            print "%s %s %s %s" % (node, lib, user, volume)

        #Active volume asserts get printed here.
        lst = self.getwork()
        pw_list = lst["pending_work"]
        at_list = lst["at movers"]
        active_assert_cnt = 0
        #If at_list has items, print heading
        for work in at_list:
            if work['work'] == "volume_assert":
                print "Active assert requests"
                break
        for work in at_list:
            if work['work'] != "volume_assert":
                continue

            #Total up the active volume assert requests.
            active_assert_cnt = active_assert_cnt + 1

            #Get and format the node the request came from.
            node = work.get("wrapper", {}).get("machine", ("",)*6)[1]
            node = socket.getfqdn(node)
            #Get and format the library name.
            lib = work.get("vc", {}).get("library", "Library_Manager")
            if lib != "Library_Manager" and lib[-16:] != ".library_manager":
                lib = lib + ".library_manager"
            #Get the username.
            user = work.get("wrapper", {}).get("uname", "Unknown")
            #Get the volume name.
            volume = work.get("vc", {}).get("external_label", "Unknown")
            #Get the mover the volume is being asserted at.
            mover = work.get("mover", "Unknown")

            #Print the volume information.
            print "%s %s %s %s %s" % (node, lib, user, volume, mover)
            

        print "Pending assert requests:", len(asserts)
        print "Active assert requests:", active_assert_cnt

        return {"status" :(e_errors.OK, None)}

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
               pend_writes = []
               pend_reads = []
               for work in pw_list:
                   if work["work"] == "read_from_hsm":
                       pending_read_cnt = pending_read_cnt + 1
                       pend_reads.append(work)
                   elif work["work"] == "write_to_hsm":
                       pending_write_cnt = pending_write_cnt + 1
                       pend_writes.append(work)
               if pending_read_cnt:
                   print "Pending read requests"
                   for work in pend_reads:
                       host = work["wrapper"]["machine"][1]
                       user = work["wrapper"]["uname"]
                       pnfsfn = work["wrapper"]["pnfsFilename"]
                       fn = work["wrapper"]["fullname"]
                       at_top = work["at_the_top"]
                       reject_reason = ("","")
                       vol_msg = ''
                       if work.has_key('reject_reason'):
                           reject_reason = work['reject_reason']
                       else:
                           vol = ''
                           if work['fc'].has_key('external_label'):
                               vol = work['vc']['external_label']
                           if vol:
                               vol_msg='VOL %s' % (vol,)
                       if (host == node) or (not node):
                           print "%s %s %s %s %s P %d %s %s %s" % (host,self.name,user,pnfsfn,fn, at_top, reject_reason[0], reject_reason[1], vol_msg)
               if pending_write_cnt:
                   print "Pending write requests"
                   for work in pend_writes:
                       host = work["wrapper"]["machine"][1]
                       user = work["wrapper"]["uname"]
                       pnfsfn = work["wrapper"]["pnfsFilename"]
                       fn = work["wrapper"]["fullname"]
                       at_top = work["at_the_top"]
                       reject_reason = ("","")
                       if work.has_key('reject_reason'):
                           reject_reason = work['reject_reason']
                       vol = ''
                       if work['vc'].has_key('external_label'):
                           vol = work['vc']['external_label']
                       vol_msg = ''
                       if vol:
                           vol_msg='VOL %s' % (vol,)
                       ff_msg = ''
                       if work['vc'].has_key('file_family'):
                           ff_msg = string.join((ff_msg,"FF",work['vc']['file_family']),' ')
                       if work['vc'].has_key('file_family_width'):
                           ff_msg = string.join((ff_msg,"FF_W %s"%(work['vc']['file_family_width'],)),' ')
                       if (host == node) or (not node):
                           print "%s %s %s %s %s P %d %s %s %s %s" % (host,self.name,user,fn,pnfsfn, at_top, reject_reason[0], reject_reason[1], vol_msg, ff_msg)
               #If at_list has items, print heading
               if at_list:
                   print "Active requests"
               for work in at_list:
                   #Work at movers will contain volume_assert requests.  Skip
                   # them here; they are handled with another switch.
                   if work["work"] == "volume_assert":
                       continue
                   
                   host = work["wrapper"]["machine"][1]
                   user = work["wrapper"]["uname"]
                   pnfsfn = work["wrapper"]["pnfsFilename"]
                   fn = work["wrapper"]["fullname"]
                   if work["vc"].has_key("external_label"):
                       vol = work["vc"]["external_label"]
                   else:
                       vol = work["fc"]["external_label"]
                   if work.has_key("mover"):
                       mover = work["mover"]
                   else:
                       mover = ''
                   if (host == node) or (not node):
                       if work["work"] == "read_from_hsm":
                          active_read_cnt = active_read_cnt + 1
                          f1 = pnfsfn
                          f2 = fn
                       elif work["work"] == "write_to_hsm":
                           active_write_cnt = active_write_cnt + 1
                           f1 = fn
                           f2 = pnfsfn
                       print "%s %s %s %s %s M %s %s" % (host,self.name, user,f1,f2, mover, vol)

        print "Pending read requests: ", pending_read_cnt
        print "Pending write requests: ", pending_write_cnt
        print "Active read requests: ", active_read_cnt
        print "Active write requests: ", active_write_cnt
                           
        return {"status" :(e_errors.OK, None)}

    def get_suspect_volumes(self):
        return self.getlist("get_suspect_volumes")

    def remove_work(self, id):
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

    def getlist(self, work):
        # get a port to talk on and listen for connections
        host, port, listen_socket = callback.get_callback()
        listen_socket.listen(4)
        ticket = {"work"         : work,
                  "callback_addr" : (host, port)}

        # send the work ticket to the library manager
        ticket = self.send(ticket, self.send_to,self.send_tries)
        if ticket['status'][0] != e_errors.OK:
            return ticket

        r,w,x = select.select([listen_socket], [], [], 15)
        if not r:
            raise errno.errorcode[errno.ETIMEDOUT], "timeout waiting for library manager callback"
        
        control_socket, address = listen_socket.accept()

        if not hostaddr.allow(address):
            control_socket.close()
            listen_socket.close()
            raise errno.errorcode[errno.EPROTO], "address %s not allowed" %(address,)

        ticket = callback.read_tcp_obj_new(control_socket)
        listen_socket.close()

        if ticket["status"][0] != e_errors.OK:
            return ticket
        
        data_path_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        data_path_socket.connect(ticket['library_manager_callback_addr'])
        worklist = callback.read_tcp_obj_new(data_path_socket)
        data_path_socket.close()

        # Work has been read - wait for final dialog with library manager.
        done_ticket = callback.read_tcp_obj_new(control_socket)
        control_socket.close()
        if done_ticket["status"][0] != e_errors.OK:
            return done_ticket

        return worklist

    # get active volume known to LM
    def get_active_volumes(self, timeout=0, tries=0):
        return self.send({"work":"get_active_volumes"}, timeout, tries)
            
    def storage_groups(self, timeout=0, tries=0):
        return self.send({"work":"storage_groups"}, timeout, tries)

    def volume_assert(self, ticket, timeout=0, tries=0):
        ticket['work'] = "volume_assert"
        return self.send(ticket, timeout, tries)

class LibraryManagerClientInterface(generic_client.GenericClientInterface) :
    def __init__(self, args=sys.argv, user_mode=1) :
        # this flag if 1, means do everything, if 0, do no option parsing
        #self.do_parse = flag
        #self.restricted_opts = opts
        self.name = ""
        self.get_work = 0
        self.get_work_sorted = 0
        self.alive_retries = 0
        self.alive_rcv_timeout = 0
        self.get_susp_vols = 0
        self.get_susp_vols = 0
        self.delete_work = ''
        self.priority = -1
        self.get_asserts = None
        self.get_queue = None
        self.start_draining = 0
        self.stop_draining = 0
        self.status = 0
        self.vols = 0
        self.storage_groups = 0
        self.rm_suspect_vol = 0
        self.rm_active_vol = 0
        self.unique_id = ''
        
        generic_client.GenericClientInterface.__init__(self, args=args,
                                                       user_mode=user_mode)

    def valid_dictionaries(self):
        return (self.alive_options, self.help_options, self.trace_options,
                self.library_options)

    parameters = ["library"]
        
    def parse_options(self):
        generic_client.GenericClientInterface.parse_options(self)

        if (getattr(self, "help", 0) or getattr(self, "usage", 0)):
                pass
        elif len(self.argv) <= 1: #if only "enstore library" is specified.
            self.print_help()
        elif len(self.args) < 1: #if a valid switch doesn't have the LM.
            self.print_usage("expected library parameter")
        else:
            try:
                self.name = self.args[0]
                del self.args[0]
            except KeyError:
                self.name = ""
                
        self.name = self.complete_server_name(self.name, "library_manager")

    library_options = {
        option.DELETE_WORK:{option.HELP_STRING:
                            "delete work identified by unique id from "
                            "the pending queue",
                            option.VALUE_TYPE:option.STRING,
                            option.VALUE_USAGE:option.REQUIRED,
                            option.USER_LEVEL:option.ADMIN},
        option.GET_ASSERTS:{option.HELP_STRING:
                                "print sorted lists of pending volume asserts",
                                option.DEFAULT_TYPE:option.INTEGER,
                                option.VALUE_USAGE:option.IGNORED,
                                option.USER_LEVEL:option.USER},
        option.GET_QUEUE:{option.HELP_STRING:
                          "print queue submitted from the specified host.  "
                          "If empty string specified, print the whole queue",
                          option.VALUE_TYPE:option.STRING,
                          option.VALUE_USAGE:option.REQUIRED,
                          option.VALUE_LABEL:"host_name",
                          option.USER_LEVEL:option.USER},
        option.GET_SUSPECT_VOLS:{option.HELP_STRING:
                                 "print suspect volume list",
                                 option.DEFAULT_TYPE:option.INTEGER,
                                 option.DEFAULT_NAME:"get_susp_vols",
                                 option.VALUE_USAGE:option.IGNORED,
                                 option.USER_LEVEL:option.USER},
        option.GET_WORK_SORTED:{option.HELP_STRING:
                           "print sorted lists of pending and active requests",
                                option.DEFAULT_TYPE:option.INTEGER,
                                option.VALUE_USAGE:option.IGNORED,
                                option.USER_LEVEL:option.USER},
        option.PRIORITY:{option.HELP_STRING:
                         "change priority of the specified request",
                         option.VALUE_NAME:"unique_id",
                         option.VALUE_TYPE:option.STRING,
                         option.VALUE_USAGE:option.REQUIRED,
                         option.USER_LEVEL:option.ADMIN,
                         option.EXTRA_VALUES:[{option.VALUE_TYPE:
                                                                option.INTEGER,
                                               option.VALUE_USAGE:
                                                               option.REQUIRED,
                                               option.VALUE_NAME:"priority",
                                               },]},
        option.RM_ACTIVE_VOL:{option.HELP_STRING:
                              "remove volume from the list of active volumes",
                              option.VALUE_TYPE:option.STRING,
                              option.VALUE_USAGE:option.REQUIRED,
                              option.VALUE_LABEL:"volume",
                              option.USER_LEVEL:option.ADMIN},
        option.RM_SUSPECT_VOL:{option.HELP_STRING:
                              "remove volume from the list of suspect volumes",
                               option.VALUE_TYPE:option.STRING,
                               option.VALUE_USAGE:option.REQUIRED,
                               option.VALUE_LABEL:"volume",
                               option.USER_LEVEL:option.ADMIN},
        option.START_DRAINING:{option.HELP_STRING:
                               "start draining library manager",
                               option.VALUE_TYPE:option.STRING,
                               option.VALUE_USAGE:option.REQUIRED,
                               option.VALUE_LABEL:
                               "locked,ingore,pause,noread,nowrite",
                               option.USER_LEVEL:option.ADMIN},
        option.STOP_DRAINING:{option.HELP_STRING:
                              "stop draining library manager",
                              option.DEFAULT_TYPE:option.INTEGER,
                              option.VALUE_USAGE:option.IGNORED,
                              option.USER_LEVEL:option.ADMIN},
        option.STATUS:{option.HELP_STRING:
                       "print current status of the library manager",
                       option.DEFAULT_TYPE:option.INTEGER,
                       option.VALUE_USAGE:option.IGNORED,
                       option.USER_LEVEL:option.ADMIN},
        option.STOP_DRAINING:{option.HELP_STRING:
                              "stop draining library manager",
                              option.DEFAULT_TYPE:option.INTEGER,
                              option.VALUE_USAGE:option.IGNORED,
                              option.USER_LEVEL:option.ADMIN},
        option.VOLS:{option.HELP_STRING:
                     "get list of active volumes",
                     option.DEFAULT_TYPE:option.INTEGER,
                     option.VALUE_USAGE:option.IGNORED,
                     option.USER_LEVEL:option.ADMIN},
        
        }


def do_work(intf):
    # get a library manager client
    lmc = LibraryManagerClient((intf.config_host, intf.config_port), intf.name)
    Trace.init(lmc.get_name(lmc.log_name))

    ticket = lmc.handle_generic_commands(intf.name, intf)
    if ticket:
        pass
    elif  intf.get_work:
        ticket = lmc.getwork()
        if e_errors.is_ok(ticket):
            print ticket['pending_work']
            print ticket['at movers']
    elif intf.get_asserts:
        ticket = lmc.get_asserts()
        if e_errors.is_ok(ticket):
            print ticket
    elif  intf.get_work_sorted:
        ticket = lmc.getworks_sorted()
        if e_errors.is_ok(ticket):
            print ticket['pending_works']
            print ticket['at movers']
    elif  intf.get_susp_vols:
        ticket = lmc.get_suspect_volumes()
        if e_errors.is_ok(ticket):
            print ticket['suspect_volumes']
    elif intf.delete_work:
        ticket = lmc.remove_work(intf.delete_work)
        if e_errors.is_ok(ticket):
            print ticket
    elif intf.rm_suspect_vol:
        ticket = lmc.remove_suspect_volume(intf.rm_suspect_vol)
    elif intf.rm_active_vol:
        ticket = lmc.remove_active_volume(intf.rm_active_vol)
    elif intf.priority != -1:
        ticket = lmc.priority(intf.unique_id, intf.priority)
        if e_errors.is_ok(ticket):
            print ticket
    elif intf.vols:
        ticket = lmc.get_active_volumes()
        print "%-10s  %-17s %-17s %-17s %17s %10s %18s"%(
            "label","mover","volume family",
            "system_inhibit","user_inhibit","status","updated")
        for mover in ticket['movers']:
            print "%-10s  %-17s %-17s (%-08s %08s) (%-08s %08s) %-10s(%-05s) %-11s" %\
            (mover['external_label'], mover['mover'],
             mover['volume_family'],
             mover['volume_status'][0][0], mover['volume_status'][0][1],
             mover['volume_status'][1][0], mover['volume_status'][1][1],
             mover['state'],
              mover['time_in_state'],
             time.ctime(mover['updated']),)
    elif intf.storage_groups:
        ticket = lmc.storage_groups()
	print "%-14s %-12s" % ('storage group', 'limit')
	for sg in ticket['storage_groups']['limits'].keys():
	    print "%-14s %-12s" % (sg, ticket['storage_groups']['limits'][sg])

    elif intf.get_queue != None:
        ticket = lmc.get_queue(intf.get_queue, intf.name)
        if e_errors.is_ok(ticket):
            print ticket
        
    elif (intf.start_draining or intf.stop_draining):
        if intf.start_draining:
            if intf.start_draining == 'lock': lock = 'locked'
            elif not (intf.start_draining in ('ignore', 'pause', 'noread', 'nowrite')):
                print "only 'lock', 'ignore', 'noread', 'nowrite' and 'pause' are valid for start_draining option"
                sys.exit(0)
            else: lock = intf.start_draining
        else: lock = 'unlocked'
        ticket = lmc.change_lm_state(lock)
    elif (intf.status):
        ticket = lmc.get_lm_state()
        if e_errors.is_ok(ticket):
            print "LM state:%s"%(ticket['state'],)
    else:
        intf.print_help()
        sys.exit(0)

    lmc.check_ticket(ticket)

if __name__ == "__main__" :
    Trace.init("LIBM_CLI")
    Trace.trace(6,"lmc called with args "+repr(sys.argv))

    # fill in the interface
    intf = LibraryManagerClientInterface(user_mode=0)

    do_work(intf)
