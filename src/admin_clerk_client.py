###############################################################################
# src/$RCSfile$   $Revision$
#
# system imports
import time

# enstore imports
import callback
import dict_to_a
import configuration_client
import udp_client
import Trace
import e_errors

import interface
import generic_client
import generic_cs

class AdminClerkClient(generic_client.GenericClient) :

    def __init__(self, csc=0, verbose=0, host=interface.default_host(), \
                 port=interface.default_port()):
	self.print_id = "ADMINC"
        configuration_client.set_csc(self, csc, host, port, verbose)
        self.u = udp_client.UDPClient()
	self.verbose = verbose
        ticket = self.csc.get("admin_clerk")
	try:
            self.print_id = ticket['logname']
        except:
            pass

    # send the request to the volume clerk server and then send answer to user
    def send (self, ticket, rcv_timeout=0, tries=0) :
        aticket = self.csc.get("admin_clerk")
        return  self.u.send(ticket, (aticket['hostip'], aticket['port']), rcv_timeout, tries)

    def select(self,criteria,dbname):
        # get a port to talk on and listen for connections
        host, port, listen_socket = callback.get_callback()
        listen_socket.listen(4)
        ticket = {"work"               : "select",
                  "callback_addr" : (host, port),
                  "unique_id"          : time.time(),
		  "dbname"	       : dbname, 
		  "criteria"           : criteria}
        # send the work ticket to the library manager
        ticket = self.send(ticket)
        if ticket['status'][0] != e_errors.OK:
            raise errno.errorcode[errno.EPROTO],"select: sending ticket"\
                  +repr(ticket)
        while 1 :
            control_socket, address = listen_socket.accept()
            new_ticket = callback.read_tcp_socket(control_socket, "admin"+\
                                  "clerk client select,  ac call back")
            if ticket["unique_id"] == new_ticket["unique_id"] :
                listen_socket.close()
                break
            else:
	        self.enprint("select - imposter called us back, trying again")
                control_socket.close()
        ticket = new_ticket
        if ticket["status"][0] != e_errors.OK:
            raise errno.errorcode[errno.EPROTO],"acc.select: "\
                  +"1st (pre-work-read) admin clerk callback on socket "\
                  +repr(address)+", failed to setup transfer: "\
                  +"ticket[\"status\"]="+ticket["status"]

        # If the system has called us back with our own  unique id, call back
        # the library manager on the library manager's port and read the
        # work queues on that port.
        data_path_socket = callback.admin_server_callback_socket(ticket)
	ticket= callback.read_tcp_socket(data_path_socket, "admin clerk"\
		  +"client select, ac final dialog")
	msg=""
        while 1:
	  msg=msg+callback.read_tcp_buf(data_path_socket,"admin clerk "+"client select, reading worklist")
	  if len(msg)==0 :
		break
	  msg=self.printRec(msg)
	worklist = ticket
	data_path_socket.close()


        # Work has been read - wait for final dialog with volume clerk
        done_ticket = callback.read_tcp_socket(control_socket, "admin clerk"\
		  +"client select, ac final dialog")
        control_socket.close()
	if done_ticket["status"][0] != e_errors.OK :
            raise errno.errorcode[errno.EPROTO],"acc.select "\
                  +"2nd (post-work-read) admin clerk callback on socket "\
                  +repr(address)+", failed to transfer: "\
                  +"ticket[\"status\"]="+ticket["status"]
        return worklist

    def printRec(self,msg):
        import regex
   	index=regex.search("}",msg)
	self.enprint(msg[:index+1], generic_cs.PRETTY_PRINT)
   	if index==len(msg)-1:
	   msg=""
	else:
	   msg=msg[index+1:]
   	return msg

class AdminClerkClientInterface(interface.Interface) :

    def __init__(self):
	self.verbose=0
	self.got_server_verbose=0
        self.alive=0
        self.alive_rcv_timeout = 0
        self.alive_retries = 0
        self.set_dbname()
        self.criteria={}

        # parse the options
        self.parse_options()

    def set_dbname(self, dbname="volume"):
        self.dbname=dbname

    # define the command line options that are valid
    def options(self):
        return self.config_options() + self.verbose_options()+\
               ["dbname=", "faccess=",
                "laccess=","declared=","capacity=","rem_bytes=",] +\
               self.alive_options()+self.help_options()

    # define the single character options
    def charopts(self):
        return "v:f:l:m:w:u:s:"

    def check(self, value):
        import regex
        index=regex.search("[^0-9><=!]",value)
        if index==-1:
	    return value
	generic_cs.enprint("ERROR: Wrong syntax: "+repr(value))
        self.print_help()
        sys.exit(1)

    def strip(self, value):
        newVal=""
        for c in value:
            if c !=' ':
	        newVal=newVal+c
        return newVal 

    def print_help(self):
        interface.Interface.print_help(self)
	msg = "help             : to see this messsage and exit\n"+\
         "dbname           : table name (volume or file)\n"+\
         "faccess,laccess  : time(YYYYMMDDHHMM) of first/last access\n"+\
         "declared         : creation time(YYYYMMDDHHMM)\n"+\
         "capacity         : number of bytes on this volume\n"+\
         "rem_bytes        : number of free bytes on this volume\n"+\
         "do not forget the '-' in front of following option:\n"+\
         "  f : file family\n"+\
         "  v : volume name\n"+\
         "  m : media type\n"+\
         "  w : wraper\n"+\
         "  u : user inhibit\n"+\
         "  s : system inhibit\n"+\
         "possible operators : '==','>=','<=','!=','<','>','<>'(between)\n"+\
         "Examples:\n"+\
         "Select all volume from media1,file_family type1 and type3, with\n"+\
         "wraper cpio that were decalred before 06/10 1998 12:00 pm :\n"
	generic_cs.enprint(msg)
	generic_cs.enprint("--dbname volume -m media1 -f type1,type3 -w cpio --declared '>199806101200'\n")
	msg = "Select all files that are loacted on volume1 ,volume3 and volume5:\n"+\
         "--dbname file -v volume1,volume,volume5\n"+\
         "Warning: if you didn't specify one of the following options\n"+\
         "selection could be very slow:\n"+\
         "f(file_family),m(media_type),v(volume name for file table)\n"
	generic_cs.enprint(msg)


if __name__ == "__main__" :
    import sys
    Trace.init("admin cli")
    Trace.trace(1,"acc called with args "+repr(sys.argv))

    # get interface
    intf = AdminClerkClientInterface()

    # get an admin clerk client
    acc = AdminClerkClient(0, intf.verbose, intf.config_host,\
                           intf.config_port)

    if intf.alive:
        ticket = acc.alive(intf.alive_rcv_timeout,intf.alive_retries)
	msg_id = generic_cs.ALIVE
    elif intf.got_server_verbose:
        ticket = acc.set_verbose(intf.server_verbose, intf.alive_rcv_timeout,\
	                         intf.alive_retries)
	msg_id = generic_cs.CLIENT
    else :
	if intf.dbname=="file" and len(acc.criteria)>0:
	   for key in intf.criteria.keys():
		if key != 'external_label':
		   intf.set_dbname("volume,file")
    	ticket=acc.select(intf.criteria,intf.dbname) 
	msg_id = generic_cs.CLIENT

    del acc.csc.u
    del acc.u		# del now, otherwise get name exception (just for python v1.5???)

    acc.check_ticket(ticket, msg_id)
