#
# system import
import sys
import time
import pprint
import copy
import errno

# enstore imports
import timeofday
import traceback
import callback
import log_client
import configuration_client
import volume_clerk_client
import file_clerk_client
import admin_clerk_client
import library_manager_client
import media_changer_client
import dispatching_worker
import SocketServer
import generic_server
import udp_client
import Trace
import e_errors

class EnstoreSystemStatusFile:

    def __init__(self, dir="/tmp", list=0):
        Trace.trace(10,'{__init__ essfile')
	if dir == "":
	    dir = "/tmp"
        self.file_name = dir + "/" + "enstore_system_status.txt"
        if list :
            print "opening " + self.file_name
        # try to open status file for append
        try:
            self.file = open(self.file_name, 'a')
            if list :
                print "opened for append"
        except:
            self.file = open(self.file_name, 'w')
            if list :
                print "opened for write"

    # output the passed alive status
    def output_alive(self, tag, status):
	stat = tag + repr(status['work']) + ", at " + repr(status['address']) + ", is " + repr(status['status']) + "\n"
	self.file.write(stat)

    # output the timeout error
    def output_etimedout(self, tag):
	stat = tag + "timed out\n"
	self.file.write(stat)

    # get the current time and output it
    def output_time(self):
        Trace.trace(12,"{output_time "+repr(self.file_name))
	tm = time.localtime(time.time())
	atm = "ENSTORE SYSTEM STATUS at %04d-%02d-%02d %02d:%02d:%02d\n" % (tm[0], tm[1], tm[2], tm[3], tm[4], tm[5])
 	self.file.write(atm)
        Trace.trace(12,"}output_time ")

    # output the library manager queues
    def output_lmqueues(self, ticket):
	self.file.write(self.format_lmc_queues(ticket))

    # format the library manager work queues for output
    def format_lmc_queues(self, ticket):
	return "      "+pprint.pformat(ticket)+"\n"

    # flush everything to the file
    def flush(self):
	self.file.flush()

class InquisitorMethods(dispatching_worker.DispatchingWorker):

    # update the enstore status file - to do this we must contact each of
    # the following and get their status.  
    #	file clerk
    #   admin clerk
    #   volume clerk
    #   log server
    #   configuration server
    #   library manager(s)
    #   media changer(s)
    #   mover(s)
    # then we write the status to the specified file

    # get the information from the configuration server
    def update_config(self):
        Trace.trace(12,"{update_config "+repr(self.essfile.file_name))
	try:
	    stat = self.csc.alive(self.alive_rcv_timeout, self.alive_retries)
	    self.essfile.output_alive("config server   : ", stat)
	except errno.errorcode[errno.ETIMEDOUT]:	
	    self.essfile.output_etimedout("config server   : ")
        Trace.trace(12,"}update_config ")

    # get the information from the library manager(s)
    def update_library_manager(self, key, list):
        Trace.trace(12,"{update_library_manager "+repr(self.essfile.file_name)+" "+repr(key))
	# get info on this library_manager
	t = self.csc.get_uncached(key)
	# get a client and then check if the server is alive
	lmc = library_manager_client.LibraryManagerClient(self.csc, 0, key, t['host'], t['port'])
	try:
	    stat = lmc.alive(self.alive_rcv_timeout, self.alive_retries)
	    self.essfile.output_alive("library manager : ", stat)
	    #self.essfile.output_name("library manager : ")
	    stat = lmc.getwork(list)
	    self.essfile.output_lmqueues(stat)
	except errno.errorcode[errno.ETIMEDOUT]:
	    self.essfile.output_etimedout("library manager : ")
        Trace.trace(12,"}update_library_manager ")

    # get the information from the movers
    def update_mover(self, key):
	pass

    # get the information from the admin clerk
    def update_admin_clerk(self):
        Trace.trace(12,"{update_admin_clerk "+repr(self.essfile.file_name))
	try:
	    stat = self.acc.alive(self.alive_rcv_timeout, self.alive_retries)
	    self.essfile.output_alive("admin clerk     : ", stat)
	except errno.errorcode[errno.ETIMEDOUT]:	
	    self.essfile.output_etimedout("admin clerk     : ")
        Trace.trace(12,"}update_admin_clerk ")

    # get the information from the file clerk
    def update_file_clerk(self):
        Trace.trace(12,"{update_file_clerk "+repr(self.essfile.file_name))
	try:
	    stat = self.fcc.alive(self.alive_rcv_timeout, self.alive_retries)
	    self.essfile.output_alive("file clerk      : ", stat)
	except errno.errorcode[errno.ETIMEDOUT]:	
	    self.essfile.output_etimedout("file clerk      : ")
        Trace.trace(12,"}update_file_clerk ")

    # get the information from the log server
    def update_log_server(self):
        Trace.trace(12,"{update_log_server "+repr(self.essfile.file_name))
	try:
	    stat = self.logc.alive(self.alive_rcv_timeout, self.alive_retries)
	    self.essfile.output_alive("log server      : ", stat)
	except errno.errorcode[errno.ETIMEDOUT]:	
	    self.essfile.output_etimedout("log server      : ")
        Trace.trace(12,"}update_log_server ")

    # get the information from the media changer(s)
    def update_media_changer(self, key):
        Trace.trace(12,"{update_media_changer "+repr(self.essfile.file_name)+" "+repr(key))
	# get info on this media changer
	t = self.csc.get_uncached(key)
	# get a client and then check if the server is alive
	mcc = media_changer_client.MediaChangerClient(self.csc, 0, key, t['host'], t['port'])
	try:
	    stat = mcc.alive(self.alive_rcv_timeout, self.alive_retries)
	    self.essfile.output_alive("media changer   : ", stat)
	except errno.errorcode[errno.ETIMEDOUT]:	
	    self.essfile.output_etimedout("media changer   : ")
        Trace.trace(12,"}update_media_changer ")

    # get the information from the volume clerk server
    def update_volume_clerk(self):
        Trace.trace(12,"{update_volume_clerk "+repr(self.essfile.file_name))
	try:
	    stat = self.vcc.alive(self.alive_rcv_timeout, self.alive_retries)
	    self.essfile.output_alive("volume clerk    : ", stat)
	except errno.errorcode[errno.ETIMEDOUT]:	
	    self.essfile.output_etimedout("volume clerk    : ")
        Trace.trace(12,"}update_volume_clerk ")

    # update the enstore system status information
    def do_update(self, list):
        Trace.trace(11,"{do_update ")
	# get local time and output it to the file
	self.essfile.output_time()
	self.update_config()
	self.update_admin_clerk()
	self.update_file_clerk()
	self.update_log_server()
	self.update_volume_clerk()

	# we want to get all the following information fresh, so only get the
	# the information from the configuration server and not from the 
	# configuration clients' cache.
	ticket = self.csc.get_keys()
	skeys = ticket['get_keys']
	for key in skeys:
	    if string.find(key, ".mover") != -1:
		self.update_mover(key)
	    elif string.find(key, ".media_changer") != -1:
	        self.update_media_changer(key)
	    elif string.find(key, ".library_manager") != -1:
	        self.update_library_manager(key, list)
	self.essfile.flush()
        Trace.trace(11,"}do_update ")

    # our client said to update the enstore system status information
    def update(self, ticket, list=0):
        Trace.trace(10,"{update "+repr(ticket))
	self.do_update(list)
        ticket["status"] = (e_errors.OK, None)
        try:
           self.reply_to_caller(ticket)
        # even if there is an error - respond to caller so he can process it
        except:
           ticket["status"] = (str(sys.exc_info()[0]), str(sys.exc_info()[1]))
           self.reply_to_caller(ticket)
           Trace.trace(0,"}update "+repr(ticket["status"]))
           return
        Trace.trace(10,"}update")
        return

    # set a new timeout value
    def set_timeout(self,ticket):
        Trace.trace(10,"{set_timeout "+repr(ticket))
        ticket["status"] = (e_errors.OK, None)
        try:
           self.reply_to_caller(ticket)
        # even if there is an error - respond to caller so he can process it
        except:
           ticket["status"] = (str(sys.exc_info()[0]), str(sys.exc_info()[1]))
           self.reply_to_caller(ticket)
           Trace.trace(0,"}set_timeout "+repr(ticket["status"]))
           return
        self.rcv_timeout = ticket["timeout"]
        Trace.trace(10,"}set_timeout")
        return

    # get the current timeout value
    def get_timeout(self,ticket):
        Trace.trace(10,"{get_timeout "+repr(ticket))
	ret_ticket = { 'timeout' : self.rcv_timeout,
	               'status'  : (e_errors.OK, None) }
        try:
           self.reply_to_caller(ret_ticket)
        # even if there is an error - respond to caller so he can process it
        except:
           ret_ticket["status"] = (str(sys.exc_info()[0]), str(sys.exc_info()[1]))
           self.reply_to_caller(ret_ticket)
           Trace.trace(0,"}get_timeout "+repr(ret_ticket["status"]))
           return
        Trace.trace(10,"}get_timeout")
        return

    # loop here forever doing what inquisitors do best (overrides UDP one)
    def serve_forever(self, timeout, list, alive_to, alive_tries) :
	Trace.trace(4,"{serve_forever "+repr(timeout))
	self.rcv_timeout = timeout

	self.alive_rcv_timeout = alive_to
	self.alive_retries = alive_tries

	# get a file clerk client, volume clerk client, admin clerk client,
	# library manager client(s), media changer client(s) and a connection
	# to the mover(s).  these will be used to get the status
	# information from the servers. we do not need to pass a host and port
	# to the class instantiators because we are giving them a configuration
	# client and they do not need to connect to the configuration server.
	self.fcc = file_clerk_client.FileClient(self.csc, list)
	self.vcc = volume_clerk_client.VolumeClerkClient(self.csc, list)
	self.acc = admin_clerk_client.AdminClerkClient(self.csc, list)

        while 1:
            self.handle_request()
	Trace.trace(4,"}serve_forever ")

    def handle_timeout(self):
	self.do_update(0)
	return



class Inquisitor(InquisitorMethods,
                generic_server.GenericServer,
                SocketServer.UDPServer):
    pass

if __name__ == "__main__":
    import sys
    import getopt
    import string
    # Import SOCKS module if it exists, else standard socket module socket
    # This is a python module that works just like the socket module, but uses
    # the SOCKS protocol to make connections through a firewall machine.
    # See http://www.w3.org/People/Connolly/support/socksForPython.html or
    # goto www.python.org and search for "import SOCKS"
    try:
        import SOCKS; socket = SOCKS
    except ImportError:
        import socket
    Trace.init("inquisitor")
    Trace.trace(1,"inquisitor called with args "+repr(sys.argv))

    # defaults
    (config_hostname,ca,ci) = socket.gethostbyaddr(socket.gethostname())
    config_host = ci[0]
    config_port = "7500"
    config_list = 0
    file_dir = ""
    timeout = 0
    list = 0

    # see what the user has specified. bomb out if wrong options specified
    options = ["config_host=","config_port="\
               ,"config_list","file_dir=","timeout=","help"]
    optlist,args=getopt.getopt(sys.argv[1:],'',options)
    for (opt,value) in optlist:
        if opt == "--config_host":
            config_host = value
        elif opt == "--config_port":
            config_port = value
        elif opt == "--config_list":
            config_list = 1
        elif opt == "--file_dir":
            file_dir = value
        elif opt == "--rcv_timeout":
            timeout = value
        elif opt == "--alive_rcv_timeout":
            alive_rcv_timeout = value
        elif opt == "--alive_retry":
            alive_retry = value
        elif opt == "--list":
            list = 1
        elif opt == "--help":
            print "python ",sys.argv[0], options
            print "   do not forget the '--' in front of each option"
            sys.exit(0)

    # bomb out if can't translate host
    ip = socket.gethostbyname(config_host)

    # bomb out if port isn't numeric
    config_port = string.atoi(config_port)

    csc = configuration_client.ConfigurationClient(config_host,config_port,\
                                                    config_list)

    #   pretend that we are the test system
    #   remember, in a system, there is only one bfs
    #   get our port and host from the name server
    #   exit if the host is not this machine
    keys = csc.get("inquisitor")
    iq = Inquisitor( (keys["hostip"], keys["port"]), InquisitorMethods)
    iq.set_csc(csc)

    # if no timeout was entered on the command line, get it from the 
    # configuration file.
    try:
        timeout = keys["timeout"]
    except:
        timeout = 120

    # if no alive timeout was entered on the command line, get it from the 
    # configuration file.
    try:
        alive_rcv_timeout = keys["alive_rcv_timeout"]
    except:
        alive_rcv_timeout = 15

    # if no alive retry # was entered on the command line, get it from the 
    # configuration file.
    try:
        alive_retries = keys["alive_retries"]
    except:
        alive_retries = 4

    # get the directory where the files we create will go.  this should
    # be in the configuration file.
    try:
        file_dir = keys["file_dir"]
    except:
        file_dir = ""

    # get a logger
    logc = log_client.LoggerClient(csc, keys["logname"], 'logserver', 0)
    iq.set_logc(logc)
    indlst=['external_label']

    # get a system status file
    iq.essfile = EnstoreSystemStatusFile(file_dir, list)

    while 1:
        try:
            Trace.trace(1,'Inquisitor (re)starting')
            logc.send(log_client.INFO, 1, "Inquisitor (re)starting")
            iq.serve_forever(timeout, list, alive_rcv_timeout, alive_retries)
        except:
            traceback.print_exc()
            format = timeofday.tod()+" "+\
                     str(sys.argv)+" "+\
                     str(sys.exc_info()[0])+" "+\
                     str(sys.exc_info()[1])+" "+\
                     "inquisitor serve_forever continuing"
            print format
            logc.send(log_client.ERROR, 1, format)
            Trace.trace(0,format)
            continue
    Trace.trace(1,"Inquisitor finished (impossible)")
