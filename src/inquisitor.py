#
# system import
import sys
import time
import pprint
import copy
import errno
import string

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
import interface
import SocketServer
import generic_server
import udp_client
import Trace
import e_errors
import enstore_status

def default_timeout():
    return 60

def default_alive_rcv_timeout():
    return 5

def default_alive_retries():
    return 2

def default_ascii_file():
    return "./inquisitor.txt"

def default_html_file():
    return "./inquisitor.html"

class InquisitorMethods(dispatching_worker.DispatchingWorker):

    def set_udp_client(self):
	Trace.trace(3,"{set_udp_client")
	self.udpc = udp_client.UDPClient()
	Trace.trace(3,"}set_udp_client")

    # get the information from the configuration server
    def update_config(self):
        Trace.trace(12,"{update_config "+repr(self.essfile.file_name))
	address = self.csc.get_address()
	try:
	    stat = self.csc.alive(self.alive_rcv_timeout, self.alive_retries)
	    self.essfile.output_alive(address[0], "config server   : ", stat)
	except errno.errorcode[errno.ETIMEDOUT]:	
	    self.essfile.output_etimedout(address, "config server   : ")
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
	    self.essfile.output_name(key)
	    self.essfile.output_alive(t['host'], " : ", stat)
	    stat = lmc.getmoverlist()
	    self.essfile.output_lmmoverlist(stat)
	    stat = lmc.getwork(list)
	    self.essfile.output_lmqueues(stat)
	except errno.errorcode[errno.ETIMEDOUT]:
	    self.essfile.output_etimedout((t['host'], t['port']), \
                                          "library manager : ")
        Trace.trace(12,"}update_library_manager ")

    # get the information from the movers
    def update_mover(self, key):
        Trace.trace(12,"{update_mover "+repr(self.essfile.file_name)+" "+repr(key))
	# get info on this mover
	t = self.csc.get_uncached(key)
	self.essfile.output_name(key)
#	alive_rq = {'work': 'alive',
#	            'address': (t['host'], t['port']) }
#	stat = self.udpc.send(alive_rq, alive_rq['address'])
	self.essfile.output_alive(t['host'], " : ", { 'work' : "NOT IMPL YET",\
                                          'address' : (t['host'], t['port']), \
                                          'status' : (e_errors.OK, None)})

    # get the information from the admin clerk
    def update_admin_clerk(self, key):
        Trace.trace(12,"{update_admin_clerk "+repr(self.essfile.file_name))
	ticket = self.csc.get(key)
	try:
	    stat = self.acc.alive(self.alive_rcv_timeout, self.alive_retries)
	    self.essfile.output_alive(ticket['host'], "admin clerk     : ", stat)
	except errno.errorcode[errno.ETIMEDOUT]:	
	    self.essfile.output_etimedout((ticket['host'], ticket['port']), "admin clerk     : ")
        Trace.trace(12,"}update_admin_clerk ")

    # get the information from the file clerk
    def update_file_clerk(self, key):
        Trace.trace(12,"{update_file_clerk "+repr(self.essfile.file_name))
	ticket = self.csc.get(key)
	try:
	    stat = self.fcc.alive(self.alive_rcv_timeout, self.alive_retries)
	    self.essfile.output_alive(ticket['host'], "file clerk      : ", stat)
	except errno.errorcode[errno.ETIMEDOUT]:	
	    self.essfile.output_etimedout((ticket['host'], ticket['port']), "file clerk      : ")
        Trace.trace(12,"}update_file_clerk ")

    # get the information from the log server
    def update_log_server(self, key):
        Trace.trace(12,"{update_log_server "+repr(self.essfile.file_name))
	ticket = self.csc.get(key)
	try:
	    stat = self.logc.alive(self.alive_rcv_timeout, self.alive_retries)
	    self.essfile.output_alive(ticket['host'], "log server      : ", stat)
	except errno.errorcode[errno.ETIMEDOUT]:	
	    self.essfile.output_etimedout((ticket['host'], ticket['port']), "log server      : ")
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
	    self.essfile.output_name(key)
	    self.essfile.output_alive(t['host'], " : ", stat)
	except errno.errorcode[errno.ETIMEDOUT]:	
	    self.essfile.output_etimedout((t['host'], t['port']), "media changer   : ")
        Trace.trace(12,"}update_media_changer ")

    # get the information from the inquisitor
    def update_inquisitor(self, key):
        Trace.trace(12,"{update_inquisitor "+repr(self.essfile.file_name)+" "+repr(key))
	# get info on the inquisitor
	t = self.csc.get(key)
	# just output our info, if we are doing this, we are alive.
	self.essfile.output_alive(t['host'], "inquisitor      : ",\
	                          { 'work' : "alive",\
	                            'address' : (t['host'], t['port']), \
                                    'status' : (e_errors.OK, None)})
        Trace.trace(12,"}update_inquisitor ")

    # get the information from the volume clerk server
    def update_volume_clerk(self, key):
        Trace.trace(12,"{update_volume_clerk "+repr(self.essfile.file_name))
	ticket = self.csc.get(key)
	try:
	    stat = self.vcc.alive(self.alive_rcv_timeout, self.alive_retries)
	    self.essfile.output_alive(ticket['host'], "volume clerk    : ", stat)
	except errno.errorcode[errno.ETIMEDOUT]:	
	    self.essfile.output_etimedout((ticket['host'], ticket['port']), "volume clerk    : ")
        Trace.trace(12,"}update_volume_clerk ")

    # update the enstore system status information
    def do_update(self, list):
        Trace.trace(11,"{do_update ")
	# get local time and output it to the file
	self.essfile.output_time()
	self.update_config()
	self.update_admin_clerk("admin_clerk")
	self.update_file_clerk("file_clerk")
	self.update_inquisitor("inquisitor")
	self.update_log_server("logserver")
	self.update_volume_clerk("volume_clerk")

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
    def serve_forever(self, list) :
	Trace.trace(4,"{serve_forever "+repr(self.rcv_timeout))

	# get a file clerk client, volume clerk client, admin clerk client
	# connections to library manager client(s), media changer client(s)
	# and a connection to the movers will be gotten dynamically.
	# these will be used to get the status
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
	Trace.trace(4,"{handle_timeout ")
	self.do_update(0)
	Trace.trace(4,"}handle_timeout ")
	return



class Inquisitor(InquisitorMethods,
                generic_server.GenericServer,
                SocketServer.UDPServer):

    def __init__(self, csc=0, list=0, host=interface.default_host(), \
                 port=interface.default_port(), timeout=-1, ascii_file="", \
                 html_file="", alive_rcv_to=-1, alive_retries=-1):
	# get the config server
	Trace.trace(10, '{__init__')
	configuration_client.set_csc(self, csc, host, port, list)
	#   pretend that we are the test system
	#   remember, in a system, there is only one bfs
	#   get our port and host from the name server
	#   exit if the host is not this machine
	keys = self.csc.get("inquisitor")
	SocketServer.UDPServer.__init__(self, (keys["hostip"], keys["port"]), \
	                                InquisitorMethods)

	# become a udp client to talk to the mover
	self.set_udp_client()

	# if no timeout was entered on the command line, get it from the 
	# configuration file.
	if timeout == -1:
	    try:
	        self.rcv_timeout = keys["timeout"]
	    except:
	        self.rcv_timeout = default_timeout()
	else:
	    self.rcv_timeout = timeout

	# if no alive timeout was entered on the command line, get it from the 
	# configuration file.
	if alive_rcv_to == -1:
	    try:
	        self.alive_rcv_timeout = keys["alive_rcv_timeout"]
	    except:
	        self.alive_rcv_timeout = default_alive_rcv_timeout()
	else:
	    self.alive_rcv_timeout = alive_rcv_to

	# if no alive retry # was entered on the command line, get it from the 
	# configuration file.
	if alive_retries == -1:
	    try:
	        self.alive_retries = keys["alive_retries"]
	    except:
	        self.alive_retries = default_alive_retries()
	else:
	    self.alive_retries = alive_retries

	# get the directory where the files we create will go.  this should
	# be in the configuration file.
	if ascii_file == "":
	    try:
	        ascii_file = keys["ascii_file"]
	    except:
	        ascii_file = default_ascii_file()

	# get the directory where the files we create will go.  this should
	# be in the configuration file.
	if html_file == "":
	    try:
	        html_file = keys["html_file"]
	    except:
	        html_file = default_html_file()

	# get a logger
	self.logc = log_client.LoggerClient(self.csc, keys["logname"], \
	                                    'logserver', 0)

	# get a system status file
	self.essfile = enstore_status.EnstoreStatus(ascii_file, list)

	Trace.trace(10, '}__init__')

class InquisitorInterface(interface.Interface):

    def __init__(self):
	Trace.trace(10,'{iqsi.__init__')
	# fill in the defaults for possible options
	self.config_list = 0
	self.ascii_file = ""
	self.html_file = ""
	self.alive_rcv_timeout = -1
	self.alive_retries = -1
	self.timeout = -1
	self.list = 0
	interface.Interface.__init__(self)

	# now parse the options
	self.parse_options()
	Trace.trace(10,'}iqsi.__init__')

    # define the command line options that are valid
    def options(self):
	Trace.trace(16, "{}options")
	return self.config_options()+self.list_options() +\
	       ["config_list", "ascii_file=","html_file=","timeout="] +\
	       self.alive_rcv_options()+self.help_options()

if __name__ == "__main__":
    Trace.init("inquisitor")
    Trace.trace(1,"inquisitor called with args "+repr(sys.argv))

    # get interface
    intf = InquisitorInterface()

    # get the inquisitor
    inq = Inquisitor(0, intf.config_list, intf.config_host, intf.config_port, \
                     intf.timeout, intf.ascii_file, intf.html_file,\
                     intf.alive_rcv_timeout, intf.alive_retries)

    while 1:
        try:
            Trace.trace(1,'Inquisitor (re)starting')
            inq.logc.send(log_client.INFO, 1, "Inquisitor (re)starting")
            inq.serve_forever(intf.list)
        except:
            traceback.print_exc()
            format = timeofday.tod()+" "+\
                     str(sys.argv)+" "+\
                     str(sys.exc_info()[0])+" "+\
                     str(sys.exc_info()[1])+" "+\
                     "inquisitor serve_forever continuing"
            print format
            inq.logc.send(log_client.ERROR, 1, format)
            Trace.trace(0,format)
            continue
    Trace.trace(1,"Inquisitor finished (impossible)")
