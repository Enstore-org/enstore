#
# system import
import sys
import time
import pprint
import copy
import errno
import string
import regsub
import os

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
import generic_server
import udp_client
import Trace
import e_errors
import enstore_status

def default_timeout():
    return 5

def default_inq_timeout():
    return 90

def default_alive_rcv_timeout():
    return 5

def default_alive_retries():
    return 2

def default_ascii_file():
    return "./inquisitor.txt"

def default_html_file():
    return "./inquisitor.html"

trailer = " : "
suffix = ".new"
did_it = 0
timed_out = 1
server_key = 'server'

class InquisitorMethods(dispatching_worker.DispatchingWorker):

    # get the alive status of the server and output it
    def alive_status(self, client, (host, port), prefix, time, key):
        Trace.trace(13,"{alive_status "+repr(host)+" "+repr(port))
	ret = did_it
	try:
	    stat = client.alive(self.alive_rcv_timeout, self.alive_retries)
	    self.essfile.output_alive(host, prefix, stat, time, key)
	    self.htmlfile.output_alive(host, prefix, stat, time, key)
	except errno.errorcode[errno.ETIMEDOUT]:
	    self.essfile.output_etimedout((host, port), prefix, time, key)
	    self.htmlfile.output_etimedout((host, port), prefix, time, key)
	    ret = timed_out
        Trace.trace(13,"}alive_status")
	return ret

    # get the library manager work queue and output it
    def work_queue(self, lm, (host, port), key, list):
        Trace.trace(13,"{work_queue "+repr(host)+" "+repr(port))
	try:
	    stat = lm.getwork(list)
	    self.essfile.output_lmqueues(stat, key, list)
	    self.htmlfile.output_lmqueues(stat, key, list)
	except errno.errorcode[errno.ETIMEDOUT]:	
	    self.essfile.output_etimedout((host, port), "    ", key)
	    self.htmlfile.output_etimedout((host, port), "    ", key)
        Trace.trace(13,"}work_queue ")

    # get the library manager mover list and output it
    def mover_list(self, lm, (host, port), key, list):
        Trace.trace(13,"{mover_list "+repr(host)+" "+repr(port))
	try:
	    stat = lm.getmoverlist()
	    self.essfile.output_lmmoverlist(stat, key, list)
	    self.htmlfile.output_lmmoverlist(stat, key, list)
	except errno.errorcode[errno.ETIMEDOUT]:	
	    self.essfile.output_etimedout((host, port), "    ", key)
	    self.htmlfile.output_etimedout((host, port), "    ", key)
        Trace.trace(13,"}mover_list ")

    # get the information from the configuration server
    def update_config_server(self, key, time, list=0):
        Trace.trace(12,"{update_config_server "+repr(self.essfile.file_name))
	self.alive_status(self.csc, self.csc.get_address(), \
	                  "config server   : ", time, key)
        Trace.trace(12,"}update_config_server")

    # get the information from the library manager(s)
    def update_library_manager(self, key, time, list=0):
        Trace.trace(12,"{update_library_manager "+\
                         repr(self.essfile.file_name)+" "+repr(key))
	# get info on this library_manager
	try:
	    t = self.csc.get_uncached(key)
	except:
	    # this library manager does not exist in the config dict
	    Trace.trace(12,"}update_library_manager - ERROR, not in config dict")
	    return
	# get a client and then check if the server is alive
	lmc = library_manager_client.LibraryManagerClient(self.csc, 0, key, \
                                                          t['host'], t['port'])
	ret = self.alive_status(lmc, (t['host'], t['port']), key+trailer, \
	                        time, key)
	if ret == did_it:
	    self.mover_list(lmc, (t['host'], t['port']), key, list)
	    self.work_queue(lmc, (t['host'], t['port']), key, list)
        Trace.trace(12,"}update_library_manager ")

    # get the information from the movers
    def update_mover(self, key, time, list=0):
        Trace.trace(12,"{update_mover "+repr(self.essfile.file_name)+" "+\
                        repr(key))
	# get info on this mover
	try:
	    t = self.csc.get_uncached(key)
	except:
	    # this mover does not exist in the config dict
	    Trace.trace(12,"{update_mover - ERROR, not in config dict")
	    return
	self.essfile.output_alive(t['host'], key+trailer, \
                                  { 'work' : "NOT IMPL YET",\
                                  'address' : (t['host'], t['port']), \
                                  'status' : (e_errors.OK, None)}, time, key)
	self.htmlfile.output_alive(t['host'], key+trailer, \
                                  { 'work' : "NOT IMPL YET",\
                                  'address' : (t['host'], t['port']), \
                                  'status' : (e_errors.OK, None)}, time, key)
        Trace.trace(12,"{update_mover")

    # get the information from the admin clerk
    def update_admin_clerk(self, key, time, list=0):
        Trace.trace(12,"{update_admin_clerk "+repr(self.essfile.file_name))
	t = self.csc.get(key)
	self.alive_status(self.acc, (t['host'], t['port']),\
	                  "admin clerk     : ", time, key)
        Trace.trace(12,"}update_admin_clerk ")

    # get the information from the file clerk
    def update_file_clerk(self, key, time, list=0):
        Trace.trace(12,"{update_file_clerk "+repr(self.essfile.file_name))
	t = self.csc.get(key)
	self.alive_status(self.fcc, (t['host'], t['port']),\
	                  "file clerk      : ", time, key)
        Trace.trace(12,"}update_file_clerk ")

    # get the information from the log server
    def update_logserver(self, key, time, list=0):
        Trace.trace(12,"{update_log_server "+repr(self.essfile.file_name))
	t = self.csc.get(key)
	self.alive_status(self.logc, (t['host'], t['port']),\
	                  "log server      : ", time, key)
        Trace.trace(12,"}update_log_server ")

    # get the information from the media changer(s)
    def update_media_changer(self, key, time, list=0):
        Trace.trace(12,"{update_media_changer "+repr(self.essfile.file_name)+\
	                " "+repr(key))
	# get info on this media changer
	try:
	    t = self.csc.get_uncached(key)
	except:
	    # this media changer did not exist in the config dict
	    Trace.trace(12,"}update_media_changer - ERROR, not in config dict")
	    return
	# get a client and then check if the server is alive
	mcc = media_changer_client.MediaChangerClient(self.csc, 0, key, \
	                                              t['host'], t['port'])
	self.alive_status(mcc, (t['host'], t['port']), key+trailer, time, key)
        Trace.trace(12,"}update_media_changer")

    # get the information from the inquisitor
    def update_inquisitor(self, key, time, list=0):
        Trace.trace(12,"{update_inquisitor "+repr(self.essfile.file_name))
	# get info on the inquisitor
	t = self.csc.get_uncached(key)
	# just output our info, if we are doing this, we are alive.
	self.essfile.output_alive(t['host'], "inquisitor      : ",\
	                          { 'work' : "alive",\
	                            'address' : (t['host'], t['port']), \
                                    'status' : (e_errors.OK, None)}, time, key)
	self.htmlfile.output_alive(t['host'], "inquisitor      : ",\
	                          { 'work' : "alive",\
	                            'address' : (t['host'], t['port']), \
                                    'status' : (e_errors.OK, None)}, time, key)
	# we need to update the dict of servers that we are keeping track of.
	# however we cannot do it now as we may be in the middle of a loop
	# reading the keys of this dict.  so we just record the fact that this
	# needs to get done and we will do it later
	self.doupdate_server_dict = 1
	self.new_timeouts = t['timeouts']
        Trace.trace(12,"}update_inquisitor ")

    # get the information from the volume clerk server
    def update_volume_clerk(self, key, time, list=0):
        Trace.trace(12,"{update_volume_clerk "+repr(self.essfile.file_name))
	t = self.csc.get(key)
	self.alive_status(self.vcc, (t['host'], t['port']), \
                          "volume clerk    : ", time, key)
        Trace.trace(12,"}update_volume_clerk ")

    # get the keys from the inquisitor part of the config file ready for use
    def prepare_keys(self):
        Trace.trace(12,"{prepare_keys")
	self.keys = self.timeouts.keys()
	self.keys.sort()
        Trace.trace(12,"}prepare_keys")

    # fix up the server list that we are keeping track of
    def update_server_dict(self):
        Trace.trace(12,"{update_server_dict")
	self.timeouts = self.new_timeouts
	self.prepare_keys()
        Trace.trace(12,"}update_server_dict")

    # flush the files we have been writing to
    def flush_files(self):
        Trace.trace(12,"{flush_files")
	self.essfile.flush()
	self.htmlfile.flush()
        Trace.trace(12,"}flush_files")

    # update the enstore system status information
    def do_update(self, ticket, list, do_all=0):
        Trace.trace(11,"{do_update ")

	# check the ascii file and see if it has gotten too big and needs to be
	# backed up and opened fresh.
	self.essfile.timestamp()

	# open the html file and output the header to it
	self.htmlfile.open()
	self.htmlfile.write_header()

	# we will need the current time to decide which servers to poke with
	# the soft cushions
	ctime = time.time()

	# see which servers we need to get info from this time around
	did_some_work = 0
	for key in self.keys:
	    if self.last_update.has_key(key):
	        delta = ctime - self.last_update[key]
	    else:
	        # the key was not in last_update although it was read in from
	        # the configuration file.  this means we have read in the
	        # configuration file and this is a new key, we have not checked
	        # this server before, so do it now
	        delta = self.timeouts[key]
	    if do_all or (delta >= self.timeouts[key] and \
	                  self.timeouts[key] != -1):
	        # time to ping this server. some keys are of the form
	        # name.real_key, so we have to get the real key to find the
	        # function to call
	        rkeyl = string.split(key, '.')
	        rkey = rkeyl[len(rkeyl)-1]
	        exec("self.update_"+rkey+"(key, ctime, list)")
	        self.last_update[key] = ctime
	        did_some_work = 1

	# now that we are out of the above loop we can update the server dict
	# if we were asked to
	if self.doupdate_server_dict:
	    self.update_server_dict()
	    self.doupdate_server_dict = 0
	        
	# only flush the files if something was written to them this time
	if did_some_work:
	    self.flush_files()

	# now we must close the html file and move it to itself without the
	# suffix tacked on the end. i.e. the file becomes for example inq.html
	# not inq.html.new
	self.htmlfile.close()
	try:
	    os.system("mv "+self.htmlfile_orig+suffix+" "+self.htmlfile_orig)
	except:
	    traceback.print_exc()
	    format = timeofday.tod()+" "+\
	             str(sys.argv)+" "+\
	             str(sys.exc_info()[0])+" "+\
	             str(sys.exc_info()[1])+" "+\
	             "inquisitor serve_forever continuing"
	    print format
	    self.logc.send(log_client.ERROR, 1, format)
	    Trace.trace(0,format)
        Trace.trace(11,"}do_update ")

    # loop here forever doing what inquisitors do best (overrides UDP one)
    def serve_forever(self, list) :
	Trace.trace(4,"{serve_forever "+repr(self.rcv_timeout))

	# get a file clerk client, volume clerk client, admin clerk client.
	# connections to library manager client(s), media changer client(s)
	# and a connection to the movers will be gotten dynamically.
	# these will be used to get the status
	# information from the servers. we do not need to pass a host and port
	# to the class instantiators because we are giving them a configuration
	# client and they do not need to connect to the configuration server.
	self.fcc = file_clerk_client.FileClient(self.csc, list)
	self.vcc = volume_clerk_client.VolumeClerkClient(self.csc, list)
	self.acc = admin_clerk_client.AdminClerkClient(self.csc, list)

	# get all the servers we are to keep tabs on
	self.prepare_keys()

        while 1:
            self.handle_request()
	Trace.trace(4,"}serve_forever ")

    def handle_timeout(self):
	Trace.trace(4,"{handle_timeout ")
	self.do_update(0, 0)
	Trace.trace(4,"}handle_timeout ")

    # our client said to update the enstore system status information
    def update(self, ticket):
        Trace.trace(10,"{update "+repr(ticket))
	try:
	    list = ticket['list']
	except:
	    list = 0
	# if the ticket holds a server name then only update that one, else
	# update everything we know about
	if ticket.has_key(server_key):
	    if self.timeouts.has_key(ticket[server_key]):
	        # mark as needing an update when call do_update
	        self.last_update[ticket[server_key]] = 0
	        do_all = 0
	    else:
	        # we have no knowledge of this server, maybe it was a typo
	        ticket["status"] = (e_errors.DOESNOTEXIST, None)
		self.send_reply(ticket)
	        Trace.trace(10,"}update")
	        return
	else:
	    do_all = 1
	self.do_update(ticket, list, do_all)
        ticket["status"] = (e_errors.OK, None)
	self.send_reply(ticket)
        Trace.trace(10,"}update")

    # send back our response
    def send_reply(self, t):
	Trace.trace(11,"{send_reply "+repr(t))
        try:
           self.reply_to_caller(t)
        # even if there is an error - respond to caller so he can process it
        except:
           t["status"] = (str(sys.exc_info()[0]), str(sys.exc_info()[1]))
           self.reply_to_caller(t)
           Trace.trace(0,"}send_reply "+repr(t))
           return
	Trace.trace(11,"}send_reply")

    # set a new timeout value
    def set_timeout(self,ticket):
        Trace.trace(10,"{set_timeout "+repr(ticket))
        ticket["status"] = (e_errors.OK, None)
	if ticket.has_key(server_key):
	    if self.timeouts.has_key(ticket[server_key]):
	        self.timeouts[ticket[server_key]] = ticket["timeout"]
	    else:
	        ticket["status"] = (e_errors.DOESNOTEXIST, None)
	else:
            self.rcv_timeout = ticket["timeout"]
	self.send_reply(ticket)
        Trace.trace(10,"}set_timeout")

    # set a new timestamp value
    def set_maxi_size(self, ticket):
        Trace.trace(10,"{set_maxi_size "+repr(ticket))
        ticket["status"] = (e_errors.OK, None)
        self.essfile.set_max_ascii_size(ticket['max_ascii_size'])
	self.send_reply(ticket)
        Trace.trace(10,"}set_maxi_size")

    # timestamp the current ascii file, and open a new one
    def do_timestamp(self, ticket):
        Trace.trace(10,"{do_timestamp "+repr(ticket))
	ticket['status'] = (e_errors.OK, None)
	self.essfile.timestamp(enstore_status.force)
	self.send_reply(ticket)
        Trace.trace(10,"}do_timestamp")

    # get the current timeout value
    def get_timeout(self, ticket):
        Trace.trace(10,"{get_timeout "+repr(ticket))
	if ticket.has_key(server_key):
	    if self.timeouts.has_key(ticket[server_key]):
	        ret_ticket = { 'timeout' : self.timeouts[ticket[server_key]],\
	                       server_key  : ticket[server_key], \
	                       'status'  : (e_errors.OK, None) }
	    else:        
	        ret_ticket = { 'timeout' : -1,\
	                       server_key  : ticket[server_key], \
	                       'status'  : (e_errors.DOESNOTEXIST, None) }
	else:
	    ret_ticket = { 'timeout' : self.rcv_timeout,\
	                   'status'  : (e_errors.OK, None) }
	self.send_reply(ret_ticket)
        Trace.trace(10,"}get_maxi_size")

    # get the current maximum ascii file size
    def get_maxi_size(self, ticket):
        Trace.trace(10,"{get_maxi_size "+repr(ticket))
	ret_ticket = { 'max_ascii_size' : self.essfile.get_max_ascii_size(),\
	               'status'  : (e_errors.OK, None) }
	self.send_reply(ret_ticket)
        Trace.trace(10,"}get_timeout")


class Inquisitor(InquisitorMethods, generic_server.GenericServer):

    def __init__(self, csc=0, list=0, host=interface.default_host(), \
                 port=interface.default_port(), timeout=-1, ascii_file="", \
                 html_file="", alive_rcv_to=-1, alive_retries=-1, \
	         max_ascii_size=-1):
	Trace.trace(10, '{__init__')
	# get the config server
	configuration_client.set_csc(self, csc, host, port, list)
	#   pretend that we are the test system
	#   remember, in a system, there is only one bfs
	#   get our port and host from the name server
	#   exit if the host is not this machine
	keys = self.csc.get("inquisitor")
	dispatching_worker.DispatchingWorker.__init__(self, (keys['hostip'], \
	                                              keys['port']))

	# initialize
	self.doupdate_server_dict = 0

        # if no timeout was entered on the command line, get it from the 
        # configuration file.
        if timeout == -1:
            try:
                self.rcv_timeout = keys['timeout']
            except:
                self.rcv_timeout = default_timeout()
        else:
            self.rcv_timeout = timeout

	# get the current time 
	ctime = time.time()

	# get the timeout for each of the servers from the configuration file.
	self.last_update = {}
	if keys.has_key('timeouts'):
	    self.timeouts = keys['timeouts']
	    # now we will create a dictionary, initiallizing it to the current
	    # time. this array records the last time that the associated server
	    # info was updated. everytime we get a particular servers' info we
	    # will update this time  
	    for key in self.timeouts.keys():
	        self.last_update[key] = ctime
	else:
	    self.timeouts['inquisitor'] = default_inq_timeout()

	# if no alive timeout was entered on the command line, get it from the 
	# configuration file.
	if alive_rcv_to == -1:
	    try:
	        self.alive_rcv_timeout = keys['alive_rcv_timeout']
	    except:
	        self.alive_rcv_timeout = default_alive_rcv_timeout()
	else:
	    self.alive_rcv_timeout = alive_rcv_to

	# if no alive retry # was entered on the command line, get it from the 
	# configuration file.
	if alive_retries == -1:
	    try:
	        self.alive_retries = keys['alive_retries']
	    except:
	        self.alive_retries = default_alive_retries()
	else:
	    self.alive_retries = alive_retries

	# if no max file size was entered on the command line, get it from the 
	# configuration file.
	if max_ascii_size == -1:
	    try:
	        max_ascii_size = keys['max_ascii_size']
	    except:
	        pass
	else:
	    max_ascii_size = max_ascii_size

	# get the ascii output file.  this should be in the configuration file.
	if ascii_file == "":
	    try:
	        ascii_file = keys['ascii_file']
	    except:
	        ascii_file = default_ascii_file()

	# get the directory where the files we create will go.  this should
	# be in the configuration file.
	if html_file == "":
	    try:
	        html_file = keys['html_file']
	    except:
	        html_file = default_html_file()

	# get a logger
	self.logc = log_client.LoggerClient(self.csc, keys["logname"], \
	                                    'logserver', 0)

	# get an ascii system status file, and open it
	if ascii_file != "":
	    self.essfile = enstore_status.EnstoreStatus(ascii_file, \
	                                            enstore_status.ascii_file,\
	                                            "", max_ascii_size, list)
	    self.essfile.open()

	# get an html system status file
	if html_file != "":
	    # add a suffix to it because we will write to this file and 
	    # maintain another copy of the file (with the user entered name) to
	    # be displayed
	    self.htmlfile = enstore_status.EnstoreStatus(html_file+suffix,\
	                                            enstore_status.html_file,\
	                                            html_file, -1, list)
	    self.htmlfile_orig = html_file
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
	self.max_ascii_size = -1
	interface.Interface.__init__(self)

	# now parse the options
	self.parse_options()
	Trace.trace(10,'}iqsi.__init__')

    # define the command line options that are valid
    def options(self):
	Trace.trace(16, "{}options")
	return self.config_options()+self.list_options() +\
	       ["config_list", "ascii_file=","html_file=","timeout="] +\
	       ["max_ascii_size="] +\
	       self.alive_rcv_options()+self.help_options()

if __name__ == "__main__":
    Trace.init("inquisitor")
    Trace.trace(1,"inquisitor called with args "+repr(sys.argv))

    # get interface
    intf = InquisitorInterface()

    # get the inquisitor
    inq = Inquisitor(0, intf.config_list, intf.config_host, intf.config_port, \
                     intf.timeout, intf.ascii_file, intf.html_file,\
                     intf.alive_rcv_timeout, intf.alive_retries,\
	             intf.max_ascii_size)

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
