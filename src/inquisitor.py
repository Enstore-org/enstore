#
# system import
import sys
import time
import pprint
import copy

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
    def update_config(self, file):
	stat = self.csc.alive()
	self.output_alive("config server   : ", stat, file)

    # get the information from the library manager(s)
    def update_library_manager(self, file, key):
	# get info on this library_manager
	t = self.csc.get_uncached(key)
	# get a client and then check if the server is alive
	lmc = library_manager_client.LibraryManagerClient(self.csc, 0, key, t['host'], t['port'])
	stat = lmc.alive()
	self.output_alive("library manager : ", stat, file)
	pass

    # get the information from the movers
    def update_mover(self, file, key):
	pass

    # get the information from the admin server
    def update_admin_clerk(self, file):
	stat = self.acc.alive()
	self.output_alive("admin server    : ", stat, file)

    # get the information from the file clerk
    def update_file_clerk(self, file):
	stat = self.fcc.alive()
	self.output_alive("file clerk      : ", stat, file)

    # get the information from the log server
    def update_log_server(self, file):
	stat = self.logc.alive()
	self.output_alive("log server      : ", stat, file)

    # get the information from the media changer(s)
    def update_media_changer(self, file, key):
	# get info on this media changer
	t = self.csc.get_uncached(key)
	# get a client and then check if the server is alive
	mcc = media_changer_client.MediaChangerClient(self.csc, 0, key, t['host'], t['port'])
	stat = mcc.alive()
	self.output_alive("media changer   : ", stat, file)

    # get the information from the volume clerk server
    def update_volume_clerk(self, file):
	stat = self.vcc.alive()
	self.output_alive("volume clerk    : ", stat, file)

    # output the passed alive status
    def output_alive(self, tag, status, file):
	stat = tag + repr(status['work']) + ", at " + repr(status['address']) + ", is " + repr(status['status']) + "\n"
	file.write(stat)

    # get the current time and output it
    def update_time(self, file):
	tm = time.localtime(time.time())
	atm = "ENSTORE SYSTEM STATUS at %04d-%02d-%02d %02d:%02d:%02d\n" % (tm[0], tm[1], tm[2], tm[3], tm[4], tm[5])
 	file.write(atm)

    # update the enstore system status information
    def do_update(self):
        Trace.trace(11,"{do_update ")
	# get local time and output it to the file
	self.update_time(self.statusfile)
	self.update_config(self.statusfile)
	self.update_admin_clerk(self.statusfile)
	self.update_file_clerk(self.statusfile)
	self.update_log_server(self.statusfile)
	self.update_volume_clerk(self.statusfile)

	# we want to get all the following information fresh, so only get the
	# the information from the configuration server and not from the 
	# configuration clients' cache.
	ticket = self.csc.get_keys()
	skeys = ticket['get_keys']
	for key in skeys:
	    if string.find(key, ".mover") != -1:
		self.update_mover(self.statusfile, key)
	    elif string.find(key, ".media_changer") != -1:
	        self.update_media_changer(self.statusfile, key)
	    elif string.find(key, ".library_manager") != -1:
	        self.update_library_manager(self.statusfile, key)
	self.statusfile.flush()
        Trace.trace(11,"}do_update ")

    def open_statusfile(self, statusfile_name, list) :
        # try to open status file for append
        if list :
            print "opening " + statusfile_name
        try:
            self.statusfile = open(statusfile_name, 'a')
            if list :
                print "opened for append"
        except :
            self.statusfile = open(statusfile_name, 'w')
            if list :
                print "opened for write"

    # our client said to update the enstore system status information
    def update(self, ticket):
        Trace.trace(10,"{update "+repr(ticket))
	self.do_update()
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

    # loop here forever doing what inquisitors do best
    def serve_forever(self, statusfile_dir_path, timeout, list) :  # overrides 
                                                                # UDPServer one
	self.rcv_timeout = timeout
	fn = "enstore_system_status.txt"
        self.statusfile_name = statusfile_dir_path + "/" + fn
        # open status file
        self.open_statusfile(self.statusfile_name, list)

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

    def handle_timeout(self):
	self.do_update()
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

    # get the directory where the files we create will go.  this should
    # be in the configuration file.
    try:
        file_dir = keys["file_dir"]
    except:
        file_dir = "/tmp"

    # get a logger
    logc = log_client.LoggerClient(csc, keys["logname"], 'logserver', 0)
    iq.set_logc(logc)
    indlst=['external_label']
    while 1:
        try:
            Trace.trace(1,'Inquisitor (re)starting')
            logc.send(log_client.INFO, 1, "Inquisitor (re)starting")
            iq.serve_forever(file_dir, timeout, list)
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
