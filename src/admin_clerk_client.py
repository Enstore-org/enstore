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

class AdminClerkClient(generic_client.GenericClient) :

    def __init__(self, csc=0, verbose=0, host=interface.default_host(), \
                 port=interface.default_port()):
        configuration_client.set_csc(self, csc, host, port, verbose)
        self.u = udp_client.UDPClient()

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
            import pprint
            if ticket["unique_id"] == new_ticket["unique_id"] :
                listen_socket.close()
                break
            else:
                print ("acc.select: imposter called us back, trying again")
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
   	pprint.pprint(msg[:index+1])
   	if index==len(msg)-1:
	   msg=""
	else:
	   msg=msg[index+1:]
   	return msg

class AdminClerkClientInterface(interface.Interface) :

    def __init__(self):
	self.verbose=0
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
        return self.config_options() +\
               ["verbose=", "dbname=", "faccess=",
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
        print "ERROR: Wrong syntax: ",value
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
        print "help             : to see this messsage and exit"
        print "dbname           : table name (volume or file)"
        print "faccess,laccess  : time(YYYYMMDDHHMM) of first/last access"
        print "declared         : creation time(YYYYMMDDHHMM)" 
        print "capacity         : number of bytes on this volume"
        print "rem_bytes        : number of free bytes on this volume"
        print "do not forget the '-' in front of following option:"
        print "f : file family"
        print "v : volume name"
        print "m : media type"
        print "w : wraper"
        print "u : user inhibit"
        print "s : system inhibit" 
        print "possible operators : '==','>=','<=','!=','<','>','<>'(between)"
        print "Examples:"
        print "Select all volume from media1,file_family type1 and type3, with"
        print "wraper cpio that were decalred before 06/10 1998 12:00 pm :"
        print "--dbname volume -m media1 -f type1,type3 -w cpio --declared '>199806101200'"	
        print "Select all files that are loacted on volume1 ,volume3 and volume5:"
        print "--dbname file -v volume1,volume,volume5"
        print "Warning: if you didn't specify one of the following options"
        print "selection could be very slow:"
        print "f(file_family),m(media_type),v(volume name for file table)"


if __name__ == "__main__" :
    import sys
    import pprint
    Trace.init("admin cli")
    Trace.trace(1,"acc called with args "+repr(sys.argv))

    # get interface
    intf = AdminClerkClientInterface()

    # get an admin clerk client
    acc = AdminClerkClient(0, intf.verbose, intf.config_host,\
                           intf.config_port)

    if intf.alive:
        ticket = acc.alive(intf.alive_rcv_timeout,intf.alive_retries)
    else :
	if intf.dbname=="file" and len(acc.criteria)>0:
	   for key in acc.criteria.keys():
		if key != 'external_label':
		   intf.set_dbname("volume,file")
    	ticket=acc.select(acc.criteria,intf.dbname) 

    del acc.csc.u
    del acc.u		# del now, otherwise get name exception (just for python v1.5???)

    acc.check_ticket("acc", ticket)
