#
# Still to do to the inquisitor:
#
#  o allow it to respond to alive requests when it is in the middle of 
#	updating the servers.
#
##############################################################################
# system import
import sys
import time
import copy
import errno
import string
import regsub
import types
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
import mover_client
import dispatching_worker
import interface
import generic_server
import generic_cs
import udp_client
import Trace
import e_errors
import enstore_status

def default_timeout():
    return 5

def default_server_timeout():
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

    trailer = " : "
    suffix = ".new"
    server_keyword = 'server'
    did_it = 0
    timed_out = 1
    ac_prefix =   "admin clerk     : "
    fc_prefix =   "file clerk      : "
    logc_prefix = "log server      : "
    in_prefix =   "inquisitor      : "
    vc_prefix =   "volume clerk    : "
    bl_prefix =   "blocksizes      : "
    cfg_prefix =  "config server   : "

    # get the alive status of the server and output it
    def alive_status(self, client, (host, port), prefix, time, key):
        Trace.trace(14,"{alive_status "+repr(host)+" "+repr(port))
	try:
	    stat = client.alive(self.alive_rcv_timeout, self.alive_retries)
	    self.essfile.output_alive(host, prefix, stat, time, key)
	    self.htmlfile.output_alive(host, prefix, stat, time, key)
	except errno.errorcode[errno.ETIMEDOUT]:
	    self.essfile.output_etimedout((host, port), prefix, time, key)
	    self.htmlfile.output_etimedout((host, port), prefix, time, key)
            Trace.trace(14,"}alive_status - ERROR, alive timed out")
	    return self.timed_out
        Trace.trace(14,"}alive_status")
	return self.did_it

    # send alive to the server and handle any errors
    def do_alive_check(self, key, time, client, prefix):
        Trace.trace(13,"{do_alive_check "+prefix)
	try:
	    t = self.csc.get(key, self.alive_rcv_timeout, self.alive_retries)
	except errno.errorcode[errno.ETIMEDOUT]:
	    self.essfile.output_noconfigdict(prefix, time, key)
	    self.htmlfile.output_noconfigdict(prefix, time, key)
            Trace.trace(13,"}do_alive_check - ERROR, getting config dict timed out ")
	    return self.timed_out

        if t['status'] == (e_errors.OK, None):
	    ret = self.alive_status(client, (t['host'], t['port']),\
	                            prefix, time, key)
	elif t['status'][0] == 'KEYERROR':
	    self.remove_key(key)
        Trace.trace(13,"}do_alive_check ")
	return ret

    # get the library manager suspect volume list and output it
    def suspect_vols(self, lm, (host, port), key):
        Trace.trace(13,"{suspect_vols "+repr(host)+" "+repr(port))
	try:
	    stat = lm.get_suspect_volumes()
	    self.essfile.output_suspect_vols(stat, key, self.verbose)
	    self.htmlfile.output_suspect_vols(stat, key, self.verbose)
	except errno.errorcode[errno.ETIMEDOUT]:	
	    self.essfile.output_etimedout((host, port), "    ", key)
	    self.htmlfile.output_etimedout((host, port), "    ", key)
	    Trace.trace(13, "}suspect_vols - ERROR, timed out")
	    return
        Trace.trace(13,"}suspect_vols")

    # get the library manager work queue and output it
    def work_queue(self, lm, (host, port), key):
        Trace.trace(13,"{work_queue "+repr(host)+" "+repr(port))
	try:
	    stat = lm.getwork(self.verbose)
	    self.essfile.output_lmqueues(stat, key, self.verbose)
	    self.htmlfile.output_lmqueues(stat, key, self.verbose)
	except errno.errorcode[errno.ETIMEDOUT]:	
	    self.essfile.output_etimedout((host, port), "    ", key)
	    self.htmlfile.output_etimedout((host, port), "    ", key)
	    Trace.trace(13, "}work_queue - ERROR, timed out")
	    return
        Trace.trace(13,"}work_queue ")

    # get the library manager mover list and output it
    def mover_list(self, lm, (host, port), key):
        Trace.trace(13,"{mover_list "+repr(host)+" "+repr(port))
	try:
	    stat = lm.getmoverlist()
	    self.essfile.output_lmmoverlist(stat, key, self.verbose)
	    self.htmlfile.output_lmmoverlist(stat, key, self.verbose)
	except errno.errorcode[errno.ETIMEDOUT]:	
	    self.essfile.output_etimedout((host, port), "    ", key)
	    self.htmlfile.output_etimedout((host, port), "    ", key)
	    Trace.trace(13, "}mover_list - ERROR, timed out")
	    return
        Trace.trace(13,"}mover_list ")

    # get the movers' status
    def mover_status(self, movc, (host, port), key):
        Trace.trace(13,"{mover_status "+repr(host)+" "+repr(port))
	try:
	    stat = movc.status(self.alive_rcv_timeout, self.alive_retries)
	    self.essfile.output_moverstatus(stat, key, self.verbose)
	    self.htmlfile.output_moverstatus(stat, key, self.verbose)
	except errno.errorcode[errno.ETIMEDOUT]:	
	    self.essfile.output_etimedout((host, port), "    ", key)
	    self.htmlfile.output_etimedout((host, port), "    ", key)
	    Trace.trace(13, "}mover_list - ERROR, timed out")
	    return
        Trace.trace(13,"}mover_status")


    # get the information from the configuration server
    def update_config_server(self, key, time):
        Trace.trace(12,"{update_config_server "+repr(self.essfile.file_name))
	self.alive_status(self.csc, self.csc.get_address(), \
	                  self.cfg_prefix, time, key)
        Trace.trace(12,"}update_config_server")

    # get the information from the library manager(s)
    def update_library_manager(self, key, time):
        Trace.trace(12,"{update_library_manager "+\
                         repr(self.essfile.file_name)+" "+repr(key))
	# get info on this library_manager
	try:
	    t = self.csc.get_uncached(key, self.alive_rcv_timeout, \
	                              self.alive_retries)
	except errno.errorcode[errno.ETIMEDOUT]:
	    self.essfile.output_noconfigdict(key+self.trailer, time, key)
	    self.htmlfile.output_noconfigdict(key+self.trailer, time, key)
	    Trace.trace(12,"}update_library_manager - ERROR, getting config dict timed out")
	    return
        if t['status'] == (e_errors.OK, None):
	    # get a client and then check if the server is alive
	    lmc = library_manager_client.LibraryManagerClient(self.csc, 0,
	                                                      key, \
                                                              t['hostip'], \
	                                                      t['port'])
	    ret = self.alive_status(lmc, (t['host'], t['port']), \
	                            key+self.trailer, time, key)
	    if ret == self.did_it:
	        self.suspect_vols(lmc, (t['host'], t['port']), key)
	        self.mover_list(lmc, (t['host'], t['port']), key)
	        self.work_queue(lmc, (t['host'], t['port']), key)
	elif t['status'][0] == 'KEYERROR':
	    self.remove_key(key)
        Trace.trace(12,"}update_library_manager ")

    # get the information from the movers
    def update_mover(self, key, time):
        Trace.trace(12,"{update_mover "+repr(self.essfile.file_name)+" "+\
                        repr(key))
	# get info on this mover
	try:
	    t = self.csc.get_uncached(key, self.alive_rcv_timeout, \
	                              self.alive_retries)
	except errno.errorcode[errno.ETIMEDOUT]:
	    self.essfile.output_noconfigdict(key+self.trailer, time, key)
	    self.htmlfile.output_noconfigdict(key+self.trailer, time, key)
            Trace.trace(12,"}update_mover - ERROR, getting config dict timed out")
	    return
        if t['status'] == (e_errors.OK, None):
	    # get a client and then check if the server is alive
	    movc = mover_client.MoverClient(self.csc, 0, key, t['hostip'], \
	                                    t['port'])
	    ret = self.alive_status(movc, (t['host'], t['port']),\
	                            key+self.trailer, time, key)
	    if ret == self.did_it:
	        self.mover_status(movc, (t['host'], t['port']), key)
	elif t['status'][0] == 'KEYERROR':
	    self.remove_key(key)
        Trace.trace(12,"}update_mover")

    # get the information from the admin clerk
    def update_admin_clerk(self, key, time):
        Trace.trace(12,"{update_admin_clerk "+repr(self.essfile.file_name))
	self.do_alive_check(key, time, self.acc, self.ac_prefix)
        Trace.trace(12,"}update_admin_clerk ")

    # get the information from the file clerk
    def update_file_clerk(self, key, time):
        Trace.trace(12,"{update_file_clerk "+repr(self.essfile.file_name))
	self.do_alive_check(key, time, self.fcc, self.fc_prefix)
        Trace.trace(12,"}update_file_clerk ")

    # get the information from the log server
    def update_logserver(self, key, time):
        Trace.trace(12,"{update_log_server "+repr(self.essfile.file_name))
	self.do_alive_check(key, time, self.logc, self.logc_prefix)
        Trace.trace(12,"}update_log_server ")

    # get the information from the media changer(s)
    def update_media_changer(self, key, time):
        Trace.trace(12,"{update_media_changer "+repr(self.essfile.file_name)+\
	                " "+repr(key))
	# get info on this media changer
	try:
	    t = self.csc.get_uncached(key, self.alive_rcv_timeout, \
	                              self.alive_retries)
	except errno.errorcode[errno.ETIMEDOUT]:
	    self.essfile.output_noconfigdict(key+self.trailer, time, key)
	    self.htmlfile.output_noconfigdict(key+self.trailer, time, key)
            Trace.trace(12,"}update_media_changer - ERROR, getting config dict timed out")
	    return
        if t['status'] == (e_errors.OK, None):
	    # get a client and then check if the server is alive
	    mcc = media_changer_client.MediaChangerClient(self.csc, 0, key, \
	                                              t['hostip'], t['port'])
	    self.alive_status(mcc, (t['host'], t['port']), key+self.trailer, \
	                      time, key)
	elif t['status'][0] == 'KEYERROR':
	    self.remove_key(key)
        Trace.trace(12,"}update_media_changer")

    # get the information from the inquisitor
    def update_inquisitor(self, key, time):
        Trace.trace(12,"{update_inquisitor "+repr(self.essfile.file_name))
	# get info on the inquisitor
	try:
	    t = self.csc.get_uncached(key, self.alive_rcv_timeout, \
	                              self.alive_retries)
	except errno.errorcode[errno.ETIMEDOUT]:
	    self.essfile.output_noconfigdict(self.in_prefix, time, key)
	    self.htmlfile.output_noconfigdict(self.in_prefix, time, key)
            Trace.trace(12,"}update_inquisitor - ERROR, getting config dict timed out")
	    return

	# just output our info, if we are doing this, we are alive.
	self.essfile.output_alive(t['host'], self.in_prefix, \
	                          { 'work' : "alive",\
	                            'address' : (t['host'], t['port']), \
                                    'status' : (e_errors.OK, None)}, time, key)
	self.htmlfile.output_alive(t['host'], self.in_prefix, \
	                          { 'work' : "alive",\
	                            'address' : (t['host'], t['port']), \
                                    'status' : (e_errors.OK, None)}, time, key)
	# we need to update the dict of servers that we are keeping track of.
	# however we cannot do it now as we may be in the middle of a loop
	# reading the keys of this dict.  so we just record the fact that this
	# needs to get done and we will do it later
	self.doupdate_server_dict = 1
	self.new_timeouts = t['timeouts']
	self.set_default_server_timeout(t)
        Trace.trace(12,"}update_inquisitor ")

    # get the default server timeout, either from the inquisitor config dict
    # or from the routine
    def set_default_server_timeout(self, inq_dict={}):
	the_key = "default_server_timeout"
	if inq_dict.has_key(the_key):
	    self.default_server_timeout = inq_dict[the_key]
	else:
	    self.default_server_timeout = default_server_timeout()

    # get the information from the volume clerk server
    def update_volume_clerk(self, key, time):
        Trace.trace(12,"{update_volume_clerk "+repr(self.essfile.file_name))
	self.do_alive_check(key, time, self.vcc, self.vc_prefix)
        Trace.trace(12,"}update_volume_clerk ")

    # get the information about the blocksizes
    def update_blocksizes(self, key, time):
        Trace.trace(12,"{update_blocksizes "+repr(self.essfile.file_name))
	try:
	    t = self.csc.get(key, self.alive_rcv_timeout, self.alive_retries)
	except errno.errorcode[errno.ETIMEDOUT]:
	    self.essfile.output_noconfigdict(self.bl_prefix, time, key)
	    self.htmlfile.output_noconfigdict(self.bl_prefix, time, key)
            Trace.trace(12,"}update_blocksizes - ERROR, getting config dict timed out")
	    return
        if t['status'] == (e_errors.OK, None):
	    self.essfile.output_blocksizes(t, self.bl_prefix, key)
	    self.htmlfile.output_blocksizes(t, self.bl_prefix, key)
	elif t['status'][0] == 'KEYERROR':
	    self.remove_key(key)
        Trace.trace(12,"}update_blocksizes")

    # get the keys from the inquisitor part of the config file ready for use
    def prepare_keys(self):
        Trace.trace(12,"{prepare_keys")
	self.server_keys = self.timeouts.keys()
	self.server_keys.sort()
        Trace.trace(12,"}prepare_keys")

    # delete the key (server) from the main looping hash and from the text	   # output to the various files
    def remove_key(self, key):
	Trace.trace(12,"{remove_key")
	i = 0
	for item in self.server_keys:
	    if item == key:
	        del self.server_keys[i]
	    else:
	        i = i + 1
	self.essfile.remove_key(key)
	self.htmlfile.remove_key(key)
	Trace.trace(12,"}remove_key")

    # fix up the server list that we are keeping track of
    def update_server_dict(self):
        Trace.trace(12,"{update_server_dict")
	self.timeouts = self.new_timeouts

	# now look thru any server timeouts that may have reset by hand and
	# keep the reset value
	for key in self.reset.keys():
	    if self.timeouts.has_key(key):
	        self.timeouts[key] = self.reset[key]
	    else:
	        del self.reset[key]
	# now we must look thru the whole config file and use the default
	# server timeout for any servers that were not included in the
	# 'timeouts' dict element
	self.fill_in_default_timeouts()
	self.prepare_keys()
        Trace.trace(12,"}update_server_dict")

    # fill in a default timeout for any servers that did not have one specified
    def fill_in_default_timeouts(self, ctime=time.time()):
        Trace.trace(12,"{fill_in_default_timeouts")
	try:
	    csc_keys = self.csc.get_keys(self.alive_rcv_timeout, \
	                                 self.alive_retries)
	except errno.errorcode[errno.ETIMEDOUT]:
            Trace.trace(12,"}fill_in_default_timeouts - ERROR, getting config dict timed out")
	    return
	for a_key in csc_keys['get_keys']:
	    if not self.timeouts.has_key(a_key):
	        self.timeouts[a_key] = self.default_server_timeout
	        if not self.last_update.has_key(a_key):
	            self.last_update[a_key] = ctime
	# now get rid of any keys that are in timeouts and not in csc_keys
	# make an exception for config_server
	for a_key in self.timeouts.keys():
	    if a_key not in csc_keys['get_keys']:
	        if a_key != "config_server":
	            del self.timeouts[a_key]
	            self.essfile.remove_key(a_key)
	            self.htmlfile.remove_key(a_key)
        Trace.trace(12,"}fill_in_default_timeouts")

    # flush the files we have been writing to
    def flush_files(self):
        Trace.trace(12,"{flush_files")
	self.essfile.flush()
	self.htmlfile.flush()
        Trace.trace(12,"}flush_files")

    # output a line if an update was requested of a server that we do not have
    # a function for
    def update_nofunc(self, server):
	Trace.trace(12,"{update_nofunc "+server)
	self.essfile.output_nofunc(server)
	self.htmlfile.output_nofunc(server)
	Trace.trace(12,"}update_nofunc ")

    # update the enstore system status information
    def do_update(self, ticket, do_all=0):
        Trace.trace(11,"{do_update ")

	# check the ascii file and see if it has gotten too big and needs to be
	# backed up and opened fresh.
	self.essfile.timestamp()

	# open the html file and output the header to it
	self.htmlfile.open(self.verbose)
	self.htmlfile.write_header()

	# we will need the current time to decide which servers to poke with
	# the soft cushions
	ctime = time.time()

	# see which servers we need to get info from this time around
	did_some_work = 0
	for key in self.server_keys:
	    if self.last_update.has_key(key):
	        delta = ctime - self.last_update[key]
	    else:
	        # the key was not in last_update although it was read in from
	        # the configuration file.  this means we have read in the
	        # configuration file and this is a new key, we have not checked
	        # this server before, so do it now
	        delta = self.timeouts[key]

	    # see if we need to update the info on this server.  do not do it
	    # if the timeout was set to -1.  this 'disables' getting info on
	    # this server.  do it if either we were asked to get info on all 
	    # the servers or it has been longer than timeout since we last
	    # gathered info on this server.
	    if do_all or (delta >= self.timeouts[key] and \
	                  self.timeouts[key] != -1):
	        # time to ping this server. some keys are of the form
	        # name.real_key, so we have to get the real key to find the
	        # function to call
	        rkeyl = string.split(key, '.')
	        inq_func = "update_"+rkeyl[len(rkeyl)-1]

	        # make sure we support this type of server first
	        if InquisitorMethods.__dict__.has_key(inq_func):
	            if type(InquisitorMethods.__dict__[inq_func]) == \
	               types.FunctionType:
	                exec("self."+inq_func+"(key, ctime)")
	                self.last_update[key] = ctime
	                did_some_work = 1
	            else:
	                # it was not a function
	                self.update_nofunc(key)
	        else:
	            # apparently we do not.
	            self.update_nofunc(key)

	# now that we are out of the above loop we can update the server dict
	# if we were asked to. we did not want to do it while doing the update
	# as we might change some timeouts or servers in the list we were
	# processing
	if self.doupdate_server_dict:
	    self.update_server_dict()
	    self.doupdate_server_dict = 0
	        
	# only flush the files if something was written to them this time
	if did_some_work:
	    self.flush_files()

	# now we must close the html file and move it to itself without the
	# suffix tacked on the end. i.e. the file becomes for example inq.html
	# not inq.html.new. only mover the file if we actually did something
	self.htmlfile.close()
	try:
	    if did_some_work:
	        os.system("mv "+self.htmlfile_orig+self.suffix+" "+\
	                  self.htmlfile_orig)
	    else:
	        os.system("rm "+self.htmlfile_orig+self.suffix)
	except:
	    traceback.print_exc()
	    format = timeofday.tod()+" "+\
	             str(sys.argv)+" "+\
	             str(sys.exc_info()[0])+" "+\
	             str(sys.exc_info()[1])+" "+\
	             "inquisitor update system error"
	    self.logc.send(log_client.ERROR, 1, format)
        Trace.trace(11,"}do_update ")

    # loop here forever doing what inquisitors do best (overrides UDP one)
    def serve_forever(self) :
	Trace.trace(4,"{serve_forever "+repr(self.rcv_timeout))

	# get a file clerk client, volume clerk client, admin clerk client.
	# connections to library manager client(s), media changer client(s)
	# and a connection to the movers will be gotten dynamically.
	# these will be used to get the status
	# information from the servers. we do not need to pass a host and port
	# to the class instantiators because we are giving them a configuration
	# client and they do not need to connect to the configuration server.
	self.fcc = file_clerk_client.FileClient(self.csc, self.verbose)
	self.vcc = volume_clerk_client.VolumeClerkClient(self.csc,
	                                                 self.verbose)
	self.acc = admin_clerk_client.AdminClerkClient(self.csc, self.verbose)

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
	# if the ticket holds a server name then only update that one, else
	# update everything we know about
	if ticket.has_key(self.server_keyword):
	    if self.timeouts.has_key(ticket[self.server_keyword]):
	        # mark as needing an update when call do_update
	        self.last_update[ticket[self.server_keyword]] = 0
	        do_all = 0
	    else:
	        # we have no knowledge of this server, maybe it was a typo
	        ticket["status"] = (e_errors.DOESNOTEXIST, None)
		self.send_reply(ticket)
	        Trace.trace(10,"}update")
	        return
	else:
	    do_all = 1
	self.do_update(ticket, do_all)
        ticket["status"] = (e_errors.OK, None)
	self.send_reply(ticket)
        Trace.trace(10,"}update")

    # dump everything we have
    def dump(self, ticket):
	Trace.trace(10,"{dump "+repr(ticket))
        ticket["status"] = (e_errors.OK, None)
	self.enprint("last_update - "+repr(self.last_update))
	self.enprint("timeouts    - "+repr(self.timeouts))
	self.enprint("server_keys - "+repr(self.server_keys))
	self.enprint("reset       - "+repr(self.reset))
	self.send_reply(ticket)
	Trace.trace(10,"}dump")

    # set a new timeout value
    def set_timeout(self,ticket):
        Trace.trace(10,"{set_timeout "+repr(ticket))
        ticket["status"] = (e_errors.OK, None)
	if ticket.has_key(self.server_keyword):
	    if self.timeouts.has_key(ticket[self.server_keyword]):
	        self.timeouts[ticket[self.server_keyword]] = ticket["timeout"]
		self.reset[ticket[self.server_keyword]] = ticket["timeout"]
	    else:
	        ticket["status"] = (e_errors.DOESNOTEXIST, None)
	else:
            self.rcv_timeout = ticket["timeout"]
	self.send_reply(ticket)
        Trace.trace(10,"}set_timeout")

    # reset the timeout value to what was in the config file
    def reset_timeout(self,ticket):
        Trace.trace(10,"{reset_timeout "+repr(ticket))
        ticket["status"] = (e_errors.OK, None)
	if ticket.has_key(self.server_keyword):
	    if self.reset.has_key(ticket[self.server_keyword]):
		del self.reset[ticket[self.server_keyword]]
	    else:
	        ticket["status"] = (e_errors.DOESNOTEXIST, None)
	else:
	    t = self.csc.get("inquisitor", self.alive_rcv_timeout, \
	                     self.alive_retries)
            self.rcv_timeout = t["timeout"]
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
	if ticket.has_key(self.server_keyword):
	    if self.timeouts.has_key(ticket[self.server_keyword]):
	        ret_ticket = { \
	               'timeout' : self.timeouts[ticket[self.server_keyword]],\
	               self.server_keyword  : ticket[self.server_keyword], \
	               'status'  : (e_errors.OK, None) }
	    else:        
	        ret_ticket = { 'timeout' : -1,\
	                  self.server_keyword  : ticket[self.server_keyword], \
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

    def __init__(self, csc=0, verbose=0, host=interface.default_host(), \
                 port=interface.default_port(), timeout=-1, ascii_file="", \
                 html_file="", alive_rcv_to=-1, alive_retries=-1, \
	         max_ascii_size=-1):
	Trace.trace(10, '{__init__')
	self.print_id = "INQS"
	self.verbose = verbose
	# set a timeout and retry that we will use the first time to get the
	# inquisitor information from the config server.  we do not use the
	# passed values because they might have been defaulted and we need to
	# look them up in the config file which we have not gotten yet.
	use_once_timeout = 5
	use_once_retry = 1

	# get the config server
	configuration_client.set_csc(self, csc, host, port, verbose)
	#   pretend that we are the test system
	#   remember, in a system, there is only one bfs
	#   get our port and host from the name server
	#   exit if the host is not this machine
	keys = self.csc.get("inquisitor", use_once_timeout, use_once_retry)
        Trace.init(keys["logname"])
	try:
	    self.print_id = keys['logname']
	except:
	    pass
	dispatching_worker.DispatchingWorker.__init__(self, (keys['hostip'], \
	                                              keys['port']))

	# initialize
	self.doupdate_server_dict = 0
	self.reset = {}

        # if no timeout was entered on the command line, get it from the 
        # configuration file.
        if timeout == -1:
            try:
                self.rcv_timeout = keys['timeout']
            except:
                self.rcv_timeout = default_timeout()
        else:
            self.rcv_timeout = timeout

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
	                                           "", max_ascii_size, verbose)
	    self.essfile.open(verbose)

	# get an html system status file
	if html_file != "":
	    # add a suffix to it because we will write to this file and 
	    # maintain another copy of the file (with the user entered name) to
	    # be displayed
	    self.htmlfile = enstore_status.EnstoreStatus(\
	                                            html_file+self.suffix,\
	                                            enstore_status.html_file,\
	                                            html_file, -1, verbose)
	    self.htmlfile_orig = html_file

	# get the timeout for each of the servers from the configuration file.
	self.last_update = {}
	if keys.has_key('timeouts'):
	    self.timeouts = keys['timeouts']
	    # now we will create a dictionary, initiallizing it to the current
	    # time. this array records the last time that the associated server
	    # info was updated. everytime we get a particular servers' info we
	    # will update this time. start out at 0 so we do an update right
	    # away
	    for key in self.timeouts.keys():
	        self.last_update[key] = 0

	# now we must look thru the whole config file and use the default
	# server timeout for any servers that were not included in the
	# 'timeouts' dict element
	self.set_default_server_timeout(keys)
	self.fill_in_default_timeouts(0)

	Trace.trace(10, '}__init__')

class InquisitorInterface(interface.Interface):

    def __init__(self):
	Trace.trace(10,'{iqsi.__init__')
	# fill in the defaults for possible options
	self.ascii_file = ""
	self.html_file = ""
	self.alive_rcv_timeout = -1
	self.alive_retries = -1
	self.timeout = -1
	self.verbose = 0
	self.max_ascii_size = -1
	interface.Interface.__init__(self)

	# now parse the options
	self.parse_options()
	Trace.trace(10,'}iqsi.__init__')

    # define the command line options that are valid
    def options(self):
	Trace.trace(16, "{}options")
	return self.config_options()+\
	       ["verbose=", "ascii_file=","html_file=","timeout="] +\
	       ["max_ascii_size="] +\
	       self.alive_rcv_options()+self.help_options()

if __name__ == "__main__":
    Trace.init("inquisitor")
    Trace.trace(1,"inquisitor called with args "+repr(sys.argv))

    # get interface
    intf = InquisitorInterface()

    # get the inquisitor
    inq = Inquisitor(0, intf.verbose, intf.config_host, intf.config_port, \
                     intf.timeout, intf.ascii_file, intf.html_file,\
                     intf.alive_rcv_timeout, intf.alive_retries,\
	             intf.max_ascii_size)

    while 1:
        try:
            Trace.trace(1,'Inquisitor (re)starting')
            inq.logc.send(log_client.INFO, 1, "Inquisitor (re)starting")
            inq.serve_forever()
        except:
	    inq.serve_forever_error("inquisitor", inq.logc)
            continue
    Trace.trace(1,"Inquisitor finished (impossible)")
