#
# system import
import sys
import time
import pprint
import copy
import errno
import regsub
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

def default_timeout():
    return 60

def default_alive_rcv_timeout():
    return 5

def default_alive_retries():
    return 2

def default_file_dir():
    return "./"

class EnstoreSystemStatusFile:

    def __init__(self, dir=default_file_dir(), list=0):
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
        Trace.trace(10,'}__init__')

    # output the passed alive status
    def output_alive(self, host, tag, status):
        Trace.trace(12,"{output_alive "+repr(tag)+" "+repr(host))
	str = tag+self.unquote(status['work'])+" at "+self.format_ip_address(host, status['address'])+" is "+self.format_status(status['status'])+"\n"
	self.file.write(str)
        Trace.trace(12,"}output_alive")

    # format the status, just use the first element
    def format_status(self, status):
        Trace.trace(12,"{format_status "+repr(status))
	return self.unquote(status[0])
        Trace.trace(12,"}format_status ")

    # format the ip address - replace the ip address with the actual host name
    def format_ip_address(self, host, address):
        Trace.trace(12,"{format_ip_address "+repr(host)+" "+repr(address))
	return "("+self.unquote(host)+", "+repr(address[1])+")"
        Trace.trace(12,"}format_ip_address ")

    # output the timeout error
    def output_etimedout(self, address, tag):
        Trace.trace(12,"{output_etimedout "+repr(tag)+" "+repr(address))
	stat = tag + "timed out at "+self.unquote(repr(address))+"\n"
	self.file.write(stat)
        Trace.trace(12,"}output_etimedout")

    # get the current time and output it
    def output_time(self):
        Trace.trace(12,"{output_time "+repr(self.file_name))
	tm = time.localtime(time.time())
	atm = "ENSTORE SYSTEM STATUS at %04d-%02d-%02d %02d:%02d:%02d\n" % (tm[0], tm[1], tm[2], tm[3], tm[4], tm[5])
 	self.file.write(atm)
        Trace.trace(12,"}output_time ")

    # output the library manager queues
    def output_lmqueues(self, ticket):
        Trace.trace(12,"{output_lmqueues "+repr(ticket))
	self.file.write(self.format_lm_queues(ticket))
        Trace.trace(12,"}output_lmqueues ")

    # output the library manager mover list
    def output_lmmoverlist(self, ticket):
        Trace.trace(12,"{output_lmmoverlist "+repr(ticket))
	self.file.write(self.format_lm_moverlist(ticket))
        Trace.trace(12,"}output_lmmoverlist ")

    # output the name of the server
    def output_name(self, name):
        Trace.trace(12,"{output_name "+repr(name))
	self.file.write(self.unquote(name))
        Trace.trace(12,"}output_name ")

    # remove all single quotes
    def unquote(self, string):
        Trace.trace(12,"{unquote "+repr(string))
	return regsub.gsub("\'", "", string)
        Trace.trace(12,"}unquote ")

    # parse the 'at movers' and 'pending_work' library manager queues
    def parse_lm_queues(self, work, spacing, string):
        Trace.trace(13,"{parse_lm_queues")
	prefix = ""
	for mover in work:
	    callback_addr = mover['callback_addr']
	    encp = mover['encp']
	    fc = mover['fc']
	    times = mover['times']
	    vc = mover['vc']
	    wrapper = mover['wrapper']
	    machine = wrapper['machine']

            # now format it all up	
	    # only exists in 'at mover' ticket
	    try:
	        string = string+prefix+mover['mover']+": "
	    except:
	        string = string+prefix
	    prefix = "            "
	    if mover['work'] == 'write_to_hsm':
	        string = string+" WRITE to "
	    else:
		string = string+"READ to "
	    string = string+wrapper['fullname']+" ("+\
	               repr(wrapper['size_bytes'])+" bytes)"
	    string = string+spacing+"on device labeled: "+fc['external_label']
	    string = string+spacing+"from "+self.unquote(machine[1])+" ("+\
	             self.unquote(machine[0])+") port "+repr(callback_addr[1])
	    string = string+spacing+"with a current priority of "+\
	             repr(encp['curpri'])+", base priority of "+\
	             repr(encp['basepri'])+","+spacing+\
	             "   delta priority of "+repr(encp['delpri'])+\
	             ", and agetime of "+repr(encp['agetime'])
	    string = string+spacing+"file family is "+vc['file_family']
	    # only exists in a write ticket
	    try:
	        string = string+" and file family width is "+\
	                 repr(vc['file_family_width'])
	    except:
	        pass
	    string = string+spacing+"job submitted at "+repr(times['t0'])
	    # only exists in a 'at movers' ticket
	    try:
	        string = string+", dequeued at "+repr(times['lm_dequeued'])
	    except:
	        pass
	    # only exists in a read ticket
	    try:
	        string = string+spacing+"remaining bytes to read: "+\
	                 repr(vc['remaining_bytes'])
	    except:
	        pass
	    # only exists in a write ticket
	    try:
	        string = string+spacing+"   and file modified at "+\
	                 repr(wrapper['mtime'])+"\n"
	    except:
	        string = string+"\n"
        Trace.trace(13,"}parse_lm_queues")
	return string

    # format the library manager work queues for output
    def format_lm_queues(self, ticket):
        Trace.trace(12,"{format_lm_queues "+repr(ticket))
	string = "    Work at "
	spacing = "\n                    "
	work = ticket['at movers']
	if len(work) != 0:
	    string = self.parse_lm_queues(work, spacing, string)
	else:
	    string = "    No work at movers\n"
	pending_work = ticket['pending_work']
	if len(pending_work) != 0:
	    string = string+"\n    Pending work: "
	    string = self.parse_lm_queues(pending_work, spacing, string)
	else:
	    string = string+"    No pending work\n"

        Trace.trace(12,"}format_lm_queues ")
	return string

    # parse the library manager moverlist ticket
    def parse_lm_moverlist(self, work, spacing, string):
        Trace.trace(13,"{parse_lm_moverlist")
	prefix = ""
	for mover in work:
	    (address, port) = mover['address']

	    # now format it all up
	    string = string+prefix+mover['mover']+" on port "+repr(port)
	    prefix = "            "
	    string = string+spacing+"state is "+mover['state']+\
	             ", last checked on "+repr(mover['last_checked'])
	    string = string+spacing+"with a try count of "+\
	             repr(mover['summon_try_cnt'])+"\n"

        Trace.trace(13,"}parse_lm_moverlist")
	return string

    # format the library manager mover list for output
    def format_lm_moverlist(self, ticket):
        Trace.trace(12,"{format_lm_moverlist "+repr(ticket))
	string = "    Known Movers: "
	spacing = "\n                    "
	work = ticket['moverlist']
	if len(work) != 0:
	    string = self.parse_lm_moverlist(work, spacing, string)
	else:
	    string = "    No moverlist\n"

        Trace.trace(12,"}format_lm_moverlist ")
	return string

    # flush everything to the file
    def flush(self):
        Trace.trace(10,'{flush')
	self.file.flush()
        Trace.trace(10,'}flush')

class InquisitorMethods(dispatching_worker.DispatchingWorker):

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
                 port=interface.default_port(), timeout=-1, file_dir="", \
                 alive_rcv_to=-1, alive_retries=-1):
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
	# if no timeout was entered on the command line, get it from the 
	# configuration file.
	if timeout == -1:
	    try:
	        self.rcv_timeout = keys["timeout"]
	    except:
	        self.rcv_timeout = inquisitor.default_timeout()
	else:
	    self.rcv_timeout = timeout

	# if no alive timeout was entered on the command line, get it from the 
	# configuration file.
	if alive_rcv_to == -1:
	    try:
	        self.alive_rcv_timeout = keys["alive_rcv_timeout"]
	    except:
	        self.alive_rcv_timeout = inquisitor.default_alive_rcv_timeout()
	else:
	    self.alive_rcv_timeout = alive_rcv_to

	# if no alive retry # was entered on the command line, get it from the 
	# configuration file.
	if alive_retries == -1:
	    try:
	        self.alive_retries = keys["alive_retries"]
	    except:
	        self.alive_retries = inquisitior.default_alive_retries()
	else:
	    self.alive_retries = alive_retries

	# get the directory where the files we create will go.  this should
	# be in the configuration file.
	if file_dir == "":
	    try:
	        file_dir = keys["file_dir"]
	    except:
	        file_dir = inquisitor.default_file_dir()

	# get a logger
	self.logc = log_client.LoggerClient(self.csc, keys["logname"], \
	                                    'logserver', 0)

	# get a system status file
	self.essfile = EnstoreSystemStatusFile(file_dir, list)

	Trace.trace(10, '}__init__')

class InquisitorInterface(interface.Interface):

    def __init__(self):
	Trace.trace(10,'{iqsi.__init__')
	# fill in the defaults for possible options
	self.config_list = 0
	self.file_dir = ""
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
	       ["config_list","file_dir=","timeout="] +\
	       self.alive_rcv_options()+self.help_options()

if __name__ == "__main__":
    Trace.init("inquisitor")
    Trace.trace(1,"inquisitor called with args "+repr(sys.argv))

    # get interface
    intf = InquisitorInterface()

    # get the inquisitor
    inq = Inquisitor(0, intf.config_list, intf.config_host, intf.config_port, \
                     intf.timeout, intf.file_dir, \
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
