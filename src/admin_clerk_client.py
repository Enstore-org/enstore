# system imports
import time

# enstore imports
import callback
import dict_to_a
import configuration_client
import udp_client
import base_defaults 
import client_defaults
import Trace

class AdminClerkClient(base_defaults.BaseDefaults,
                       client_defaults.ClientDefaults) :

    def __init__(self, csc=[],
                 host=base_defaults.default_host(),
                 port=base_defaults.default_port()) :
        self.config_list = 0
        self.criteria={}
        self.dbname="volume"
        self.doalive=0
        configuration_client.set_csc(self, csc, host, port)
        self.u = udp_client.UDPClient()

    # define the command line options that are valid
    def options(self):
        return base_defaults.BaseDefaults.config_options(self) + \
               ["config_list", "alive", "dbname=", "faccess=",
                "laccess=","declared=","capacity=","rem_bytes=",] +\
               base_defaults.BaseDefaults.options(self)

    # define the single character options
    def charopts(self):
        return "v:f:l:m:w:u:s:"

    # send the request to the volume clerk server and then send answer to user
    def send (self, ticket) :
        aticket = self.csc.get("admin_clerk")
        return  self.u.send(ticket, (aticket['host'], aticket['port']))

    def select(self,criteria,dbname):
        # get a port to talk on and listen for connections
        host, port, listen_socket = callback.get_callback()
        listen_socket.listen(4)
        ticket = {"work"               : "select",
                  "user_callback_port" : port,
                  "user_callback_host" : host,
                  "unique_id"          : time.time(),
		  "dbname"	       : dbname, 
		  "criteria"           : criteria}
        # send the work ticket to the library manager
        ticket = self.send(ticket)
        if ticket['status'] != "ok" :
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
        if ticket["status"] != "ok" :
            raise errno.errorcode[errno.EPROTO],"acc.select: "\
                  +"1st (pre-work-read) admin clerk callback on socket "\
                  +repr(address)+", failed to setup transfer: "\
                  +"ticket[\"status\"]="+ticket["status"]

        # If the system has called us back with our own  unique id, call back
        # the library manager on the library manager's port and read the
        # work queues on that port.
        data_path_socket = callback.admin_clerk_callback_socket(ticket)
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
	if done_ticket["status"] != "ok" :
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
        base_defaults.BaseDefaults.print_help(self)
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
    Trace.init("admin cli")
    import sys
    import pprint

    # fill in defaults
    acc = AdminClerkClient()

    # see what the user has specified. bomb out if wrong options specified
    acc.parse_options()
    acc.csc.connect()
	
    if acc.doalive:
        ticket = acc.alive()
    else :
	if acc.dbname=="file" and len(acc.criteria)>0:
	   for key in acc.criteria.keys():
		if key != 'external_label':
		   acc.dbname="volume,file"	
    	ticket=acc.select(acc.criteria,acc.dbname) 
    if ticket['status'] != 'ok' :
        print "Bad status:",ticket['status']
       	pprint.pprint(ticket)
        sys.exit(1)
    else :
        pprint.pprint(ticket)
