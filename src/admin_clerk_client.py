import time
import callback
import dict_to_a
from configuration_client import configuration_client
from udp_client import UDPClient
class AdminClerkClient :

    def __init__(self, configuration_client) :
        self.csc = configuration_client
        self.u = UDPClient()


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
    # check on alive status
    def alive(self):
        return self.send({'work':'alive'})
def strip(value):
    newVal=""
    for c in value:
	if c !=' ':
	   newVal=newVal+c
    return newVal 
def parseOpt(optlist):
   # defaults
   config_host = "localhost"
   config_port ="7500"
   config_list = 0
   criteria={}
   dbname="volume"
   alive=0
   for  (opt,value) in optlist :
    value=strip(value)
    if opt=="-v":
	criteria['external_label']=string.split(value,',')
    if opt=="-l" :
	criteria['library']=string.split(value,',')
    if opt=="-f" :
	criteria['file_family']=string.split(value,',')
    if opt=="-v":
	criteria['external_label']=string.split(value,',')
    if opt=="-m":
	criteria['media_type']=string.split(value,',') 
    if opt=="-w":
	criteria['wrapper']=string.split(value,',')
    if opt=="-u":
	criteria['user_inhibit']=string.split(value,',')
    if opt=="-s":
	criteria['system_inhibit']=string.split(value,',')
    if opt=="--faccess":
	criteria['first_access']=check(value)
    if opt=="--laccess":
	criteria['last_access']=check(value)
    if opt=="--declared":
	criteria['declared']=check(value)
    if opt=="--capacity":
	criteria['capacity']=check(value)
    if opt=="--rem_bytes":
	criteria['rem_bytes']=check(value)
    if opt == "--config_host" :
        config_host = value
    if opt == "--config_port" :
        config_port = value
    if opt == "--config_list" :
        config_list = 1
    if opt == "--alive" :
        alive = 1
    if opt == "--dbname":
	dbname=value
    if opt == "--help":
	help()
        sys.exit(0)
   return  criteria,config_host,config_port,config_list,alive,dbname
def check(value):
    import regex
    index=regex.search("[^0-9><=!]",value)
    if index==-1:
	return value
    print "ERROR: Wrong syntax: ",value
    help()
    sys.exit(1)
def help():
    print "python ",sys.argv[0]
    print "do not forget the '--' in front of following option:"
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


    options = ["config_host=","config_port=","config_list","alive","dbname=",
	       "faccess=","laccess=","declared=","capacity=","rem_bytes=",
	       "help"]
    try:
     	optlist,args=getopt.getopt(sys.argv[1:],'v:f:l:m:w:u:s:',options)
	if len(args)!=0 :
		raise "Wrong syntax"
    except :
	print "Error: ",sys.exc_info()[0],sys.exc_info()[1]
	print "Usage:"
	help()
	sys.exit(1)
	
    criteria,config_host,config_port,config_list,alive,dbname=parseOpt(optlist)
    (config_host,ca,ci) = socket.gethostbyaddr(config_host)

    # bomb out if can't translate host
    ip = socket.gethostbyname(config_host)

    # bomb out if port isn't numeric
    config_port = string.atoi(config_port)


    if config_list :
        print "Connecting to configuration server at ",config_host,config_port
    csc = configuration_client(config_host,config_port)    
    acc = AdminClerkClient(csc)
    if alive:
        ticket = acc.alive()
    else :
	if dbname=="file" and len(criteria)>0:
	   for key in criteria.keys():
		if key != 'external_label':
		   dbname="volume,file"	
    	ticket=acc.select(criteria,dbname) 
    if ticket['status'] != 'ok' :
        print "Bad status:",ticket['status']
       	pprint.pprint(ticket)
        sys.exit(1)
    else :
	 pprint.pprint(ticket)











