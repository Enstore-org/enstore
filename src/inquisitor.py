#
##############################################################################
# system import
import sys
import time
import errno
import string
import types
import os
import stat
import signal
import threading

# enstore imports
import library_manager_client
import mover_client
import inquisitor_plots
import monitored_server
import event_relay_messages
import event_relay_client
import dispatching_worker
import generic_server
import Trace
import e_errors
import enstore_files
import enstore_functions
import enstore_constants
import www_server
import safe_dict

MY_NAME = "inquisitor"
LOGHTMLFILE_NAME = "enstore_logs.html"
TIMED_OUT_SP = "    "
DEFAULT_REFRESH = "60"
DEAD = "dead"
ALIVE = "alive"
NO_INFO_YET = "no info yet"

USER = 0
TIMEOUT=1

def default_update_interval():
    return 10

def default_alive_rcv_timeout():
    return 5

def default_alive_retries():
    return 2

def default_max_encp_lines():
    return 50

# given a directory get a list of the files and their sizes
def get_file_list(dir, prefix):
    logfiles = {}
    files = os.listdir(dir)
    # pull out the files and get their sizes
    prefix_len = len(prefix)
    for file in files:
	if file[0:prefix_len] == prefix and not file[-3:] == ".gz":
	    logfiles[file] = os.stat('%s/%s'%(dir,file))[stat.ST_SIZE]
    return logfiles

class EventRelay:

    def __init__(self, interval):
	self.last_alive = enstore_constants.NEVER_ALIVE
	self.interval = interval
	self.sent_own_alive = 0
	self.state = enstore_constants.NEVER_ALIVE
	self.start = time.time()

    def alive(self, now):
	self.last_alive = now
	self.sent_own_alive = 0
	self.state = ALIVE

    def dead(self):
	self.state = DEAD

    def is_alive(self):
	if self.state == ALIVE:
	    rtn = 1
	else:
	    rtn = 0
	return rtn

    # should the event relay process be contacted.  in other words, has it been awhile
    # since it talked to us.
    def doPing(self):
	now = time.time()
	if self.last_alive == enstore_constants.NEVER_ALIVE and \
	   now - self.start > self.interval:
	    rtn = 1
	elif now - self.last_alive > self.interval:
	    rtn = 1
	else:
	    rtn = 0
	return rtn


class InquisitorMethods(inquisitor_plots.InquisitorPlots,
			dispatching_worker.DispatchingWorker):
    
    # look for the hung_rcv_timeout value for the specified key in the inquisitor
    # config info.  if it does not exist, return the default
    def get_hung_to(self, key):
	return self.inquisitor.get("hung_rcv_timeout", {}).get(key, 
					       monitored_server.HUNG_INTERVAL_DEFAULT)

    def mark_event_relay(self, state):
	self.serverfile.output_etimedout(self.erc.event_relay_addr[0], 
					 self.erc.event_relay_addr[1], state,
					 time.time(), enstore_constants.EVENT_RELAY, 
					 self.event_relay.last_alive)

    def mark_server(self, state, server):
	if server.no_thread():
	    self.serverfile.output_etimedout(server.host, server.port, state, 
					     time.time(), server.name, 
					     server.last_alive)
	    self.new_server_status = 1

    # mark a server as not having sent a heartbeat yet.
    def mark_no_info(self, server):
	self.mark_server(NO_INFO_YET, server)

    # mark a server as having timed out, this happens if no alive message is
    # received from the event_relay for this server
    def mark_timed_out(self, server):
	self.mark_server("timed out", server)

    # mark a server as having possible hung, this happens if no alive message is
    # received from the event_relay for this server after server.hung_interval time
    def mark_hung(self, server):
	self.mark_server(DEAD, server)

    # called by the signal handling routines
    def s_update_exit(self, the_signal, frame):
	self.update_exit(0)

    # function called to attempt a restart on a 'dead' server
    def restart_function(self, server):
	prefix = "INQ_CHILD"
	alarm_info = {'server' : server.name}
	try:
	    # we should try to restart the server.  try 3X
	    i = 0
	    retries = 1
	    pid = os.getpid()
	    Trace.log(e_errors.INFO,
		      "%s: Attempting restart of %s (%s)"%(prefix, server.name, 
							   pid))
	    # we need just the node name part of the host name
	    node = string.split(server.host, ".", 1)
	    alarm_info['node'] = node
	    while i < 3:
		if server.delete:
		    # we no longer need to try to restart this server.  possibly
		    # while we were trying to restart it, it was removed from the
		    # config file.
		    Trace.log(e_errors.INFO, 
			      "%s: Aborting restart attempt of %s"%(prefix, 
								    server.name))
		    break
		# do not do the stop and start if the event relay is not alive.  wait
		# awhile and see if the event relay comes back up.
		if self.event_relay.is_alive():
		    Trace.trace(7, "%s: Server restart: try %s"%(prefix, i))
		    os.system('enstore Estop %s "--just %s"'%(node[0], server.name))
		    j = 0
		    while j < 15:
			time.sleep(1)
			j = j + 1

		    os.system('enstore Estart %s "--just %s"'%(node[0], server.name))

		# check if now alive - to do this, wait the equivalent of hung_interval
		# for this server and then see if an event relay message has arrived
		j = 0
		while j < server.hung_interval:
		    time.sleep(1)
		    j = j + 1
		if server.check_recent_alive() == monitored_server.NO_TIMEOUT:
		    Trace.log(e_errors.INFO, "%s: Restarted %s"%(prefix, server.name))
		    break
		else:
		    i = i + 1
	    else:
		# we could not restart the server, do not try again
		server.cant_restart()
		if not server.name == enstore_constants.ALARM_SERVER:
		    Trace.alarm(e_errors.ERROR, e_errors.CANTRESTART, alarm_info)
		else:
		    Trace.log(e_errors.ERROR, "%s: Can't restart %s"%(prefix, 
								      server.name))
	except:
	    # we catch any exception from the thread as we do not want it to
	    # escape from this area and start executing the main loop of the
	    # inquisitor code.  we will output an error and then exit.
	    e_errors.handle_error()
	    self.serve_forever_error(prefix)

    # check if a server is alive, if not, try to restart it.  this restarting
    # may be overridden in the config file on a per server basis.
    def attempt_restart(self, server):
	# could not communicate with the server.  create a thread that will try
	# to restart it, if we have not already done this
	if server.no_thread():
	    # if we raise an alarm we need to include the following info.
	    alarm_info = {'server' : server.name}
	    # first see if the server is supposed to be restarted.
	    if (server.norestart or server.restart_failed) and not server.did_restart_alarm:
		# do not restart, raise an alarm that the
		# server is dead.
		if not server.name == enstore_constants.ALARM_SERVER:
		    Trace.alarm(e_errors.ERROR, e_errors.SERVERDIED, alarm_info)
		else:
		    Trace.log(e_errors.ERROR,
			      "%s died and will not be restarted"%(server.name,))
		server.did_restart_alarm = 1
	    else:
		# we must keep track of the fact that we created a thread for this 
		# client so the next time we find the server dead we do not create 
		# another one.
		Trace.trace(8,"Inquisitor creating thread to restart %s"%(server.name,))
		server.restart_thread = threading.Thread(group=None,
							 target=self.restart_function,
						       name="RESTART_%s"%(server.name,),
							 args=(server,))
		server.restart_thread.setDaemon(1)
		server.restart_thread.start()

    def make_server_status_html_file(self):
	if self.new_server_status:
	    self.serverfile.open()
	    self.serverfile.write()
	    self.serverfile.close()
	    self.serverfile.install()
	    self.new_server_status = 0

    # create an html file that has a link to all of the current log files
    def make_log_html_file(self):
	log_dirs = self.inquisitor.user_log_dirs
        # add the web host to the dict of log directories if not already there
	try: 
	    for key in log_dirs.keys():
		if log_dirs[key][0:5] not in ["http:", "file:"]:
		    log_dirs[key] = "%s%s"%(self.inquisitor.www_host,log_dirs[key])
	except AttributeError:
	    # there were no log_dirs
	    pass

        # first get a list of all of the log files and their sizes
        if self.log_server.log_file_path:
	    # given a directory get a list of the files and their sizes
	    logfiles = get_file_list(self.log_server.log_file_path, 
				     enstore_constants.LOG_PREFIX)
            if logfiles:
                # create the new log listing file.
                self.logfile.open()
                self.logfile.write(self.inquisitor.http_log_file_path, logfiles,
				   log_dirs, self.inquisitor.www_host)
                self.logfile.close()
                # now we must move the new file to it's real name. do a copy
                # and then a delete as this will work across disks
		self.logfile.copy()
		self.logfile.remove()

    def add_new_mv_lm_mc(self, key, config_d, event_relay_interval):
	if enstore_functions.is_mover(key):
	    self.server_d[key] = monitored_server.MonitoredMover(config_d[key], key)
	elif enstore_functions.is_media_changer(key):
	    self.server_d[key] = monitored_server.MonitoredMediaChanger(config_d[key],
									key)
	elif enstore_functions.is_library_manager(key):
	    self.server_d[key] = monitored_server.MonitoredLibraryManager(config_d[key],
									  key)
	else:
	    # nothing to see here
	    return
	self.server_d[key].hung_interval = \
				self.inquisitor.get_hung_interval(self.server_d[key].name)
	event_relay_interval = max(event_relay_interval, self.server_d[key].hung_interval)

    def update_config_page(self, config):
        self.configfile.open()
	self.configfile.write(config)
	self.configfile.close()
	self.configfile.install()

    def update_variables_from_config(self, config):
	self.inquisitor.update_config(config.get(self.inquisitor.name, {}))
	if not self.inquisitor.config:
	    # the inquisitor information is no longer in the config file.  exit 
	    self.update_exit()
	for skey in self.server_d.keys():
	    server = self.server_d[skey]
	    new_server_config = config.get(server.name, {})
	    if new_server_config:
		server.update_config(new_server_config)
		server.hung_interval = self.inquisitor.get_hung_interval(server.name)
		self.event_relay.interval = max(self.event_relay.interval,
					       server.hung_interval)
	    else:
		# this server no longer exists in the config file, get rid of it
		# from our internal dictionary and the output html file.
		if not server.name == enstore_constants.CONFIG_SERVER:
		    # set this so if there is a thread attempting to restart this
		    # server, it will notice and abort the attempt.
		    server.delete_me()
		    del(self.server_d[server.name])
		    self.serverfile.remove_key(server.name)
	# check the new config for any new servers we need to add. only handle movers,
	# library managers and media changers for now.
	for skey in config.keys():
	    if not self.server_d.has_key(skey):
		self.add_new_mv_lm_mc(skey, config)

	self.www_server = config.get(enstore_constants.WWW_SERVER, {})
	# only update the following values if they were not set on the command line
	key = 'alive_rcv_timeout'
	self.alive_rcv_timeout = self.get_value(key, self.got_from_cmdline[key])
	key = 'alive_retries'
	self.alive_retries = self.get_value(key, self.got_from_cmdline[key])
	key = 'max_encp_lines'
	self.max_encp_lines = self.get_value(key, self.got_from_cmdline[key])

    # update the file that contains the configuration file information
    def make_config_html_file(self):
        d = {}
	# get the config dictionary
	d=self.csc.dump(self.alive_rcv_timeout, self.alive_retries)
	if enstore_functions.is_timedout(d):
            Trace.trace(12,
			"make_config_html_file - ERROR, getting config dict timed out")
	    return
	# we may not have gotten the dict so check for it first before writing it.
	new_config = d.get('dump', {})
	self.update_config_page(new_config)

	# now update all of the internal information based on the new config info.
	self.update_variables_from_config(new_config)

    # get the library manager suspect volume list and output it
    def suspect_vols(self, lm, (host, port), key, time):
	state = safe_dict.SafeDict(lm.get_suspect_volumes())
	self.serverfile.output_suspect_vols(state, key)
	if enstore_functions.is_timedout(state):
	    self.serverfile.output_etimedout(host, port, TIMED_OUT_SP, time, key)
	    Trace.trace(13, "suspect_vols - ERROR, timed out")

    # get the library manager work queue and output it
    def work_queue(self, lm, (host, port), key, time):
	state = safe_dict.SafeDict(lm.getwork())
	self.serverfile.output_lmqueues(state, key)
	if enstore_functions.is_timedout(state):
	    self.serverfile.output_etimedout(host, port, TIMED_OUT_SP, time, key)
	    Trace.trace(13, "work_queue - ERROR, timed out")

    # get the library manager state and output it
    def lm_state(self, lm, (host, port), key, time):
	state = safe_dict.SafeDict(lm.get_lm_state())
	self.serverfile.output_lmstate(state, key)
	if enstore_functions.is_timedout(state):
	    self.serverfile.output_etimedout(host, port, TIMED_OUT_SP, time, key)
	    Trace.trace(13, "lm_state - ERROR, timed out")

    # get the information from the library manager(s)
    def update_library_manager(self, lib_man):
	# get a client and then check if the server is alive
	lmc = library_manager_client.LibraryManagerClient(self.csc, lib_man.name)
	host = lib_man.host
	port = lib_man.port
	now = time.time()
	self.lm_state(lmc, (host, port), lib_man.name, now)
	self.suspect_vols(lmc, (host, port), lib_man.name, now)
	self.work_queue(lmc, (host, port), lib_man.name, now)
	self.new_server_status = 1

    # get the movers' status
    def mover_status(self, movc, (host, port), key, time):
	state = safe_dict.SafeDict(movc.status(self.alive_rcv_timeout, 
					      self.alive_retries))
	self.serverfile.output_moverstatus(state, key)
	if enstore_functions.is_timedout(state):
	    self.serverfile.output_etimedout(host, port, TIMED_OUT_SP, time, key)
	    Trace.trace(13, "mover_status - ERROR, timed out")

    # get the information from the movers
    def update_mover(self, mover):
	movc = mover_client.MoverClient(self.csc, mover.name)
	self.mover_status(movc, (mover.host, mover.port), mover.name, time.time())
	self.new_server_status = 1

    # only change the status of the inquisitor on the system status page to
    # timed out, then exit.
    def update_exit(self, exit_code):
	self.serverfile.output_alive(self.inquisitor.host, self.inquisitor.port, 
				     "exiting", time.time(), self.name)
	self.new_server_status = 1
	# the above just stored the information, now write the page out
	self.make_server_status_html_file()
	# Don't fear the reaper!!
	Trace.trace(10, "exiting inquisitor due to request")
	os._exit(exit_code)

    # update any encp information from the log files
    def make_encp_html_file(self):
        encplines = []
	encplines2 = []
	date = ''
	date2 = ''
	parsed_file = "%s%s"%(enstore_files.default_dir, "parsed")
	# look to see if the log server LOGs are accessible to us.  if so we
	# will need to parse them to get encp information.
        try:
            t = self.logc.get_logfile_name(self.alive_rcv_timeout,
						      self.alive_retries)
        except errno.errorcode[errno.ETIMEDOUT]:
            Trace.trace(8,"update_encp - ERROR, getting log file name timed out")
            return
	logfile = t.get('logfile_name', "")
        # create the file which contains the encp lines from the most recent
        # log file.
	search_text = "-e %s -e \" E ENCP \""%(Trace.MSG_ENCP_XFER,)
        if logfile and os.path.exists(logfile):
            encpfile = enstore_files.EnDataFile(logfile, parsed_file+".encp", 
						search_text, "", "|sort -r")
            encpfile.open('r')
            date, encplines = encpfile.read(self.max_encp_lines)
            encpfile.close()
	
	i = len(encplines)
	if i < self.max_encp_lines:
	    # we read in all the encps from the most recent log file. we
	    # did not read in self.max_encp_lines, so get the 2nd most recent
	    # log file and do the same.
            try:
                t = self.logc.get_last_logfile_name(self.alive_rcv_timeout,
                                                    self.alive_retries)
            except errno.errorcode[errno.ETIMEDOUT]:
                Trace.trace(8,"update_encp - ERROR, getting last log file name timed out")
                t = {}
	    logfile2 = t.get('last_logfile_name', "")
	    if (logfile2 != logfile) and logfile2 and os.path.exists(logfile2):
	        encpfile2 = enstore_files.EnDataFile(logfile2, parsed_file+".encp2",
						     search_text, "", "|sort -r")
	        encpfile2.open('r')
	        date2, encplines2 = encpfile2.read(self.max_encp_lines-i)
	        encpfile2.close()
	# now we have some info, output it
	self.encpfile.open()
	self.encpfile.write(date, encplines, date2, encplines2)
	self.encpfile.close()
	self.encpfile.install()

    def ping_event_relay(self):
	if self.event_relay.sent_own_alive == 2:
	    # we have sent several alive messages to the event relay and have gotten
	    # nothing back.  mark it as dead
	    self.mark_event_relay(DEAD)
	    self.event_relay.dead()

	# whoops have not seen anything for awhile, try to send ourselves our own 
	# alive, if this does not work, then the event relay process is not running
	self.erc.send(self.event_relay_msg)
	self.event_relay.sent_own_alive = self.event_relay.sent_own_alive + 1
	# try resubscribing too.
	self.erc.subscribe()

    # if we have not heard anything from the event relay for awhile.  maybe it is down or
    # maybe nothing else is running
    def check_event_message(self):
	now = time.time()
	if self.event_relay.doPing():
	    self.ping_event_relay()

    # examine each server we are monitoring to see if we have received an alive from
    # it recently (via the event relay)
    def check_last_alive(self):
	for skey in self.server_d.keys():
	    server = self.server_d[skey]
	    status = server.check_recent_alive()
	    if status == monitored_server.TIMEDOUT:
		self.mark_timed_out(server)
	    elif status == monitored_server.HUNG:
		self.mark_hung(server)
		self.attempt_restart(server)

    def server_is_alive(self, name):
	now = time.time()
	server = self.server_d.get(name, None)
	if server:
	    self.serverfile.output_alive(server.host, server.port, ALIVE, 
					 now, name)
	    self.new_server_status = 1
	    server.is_alive()
	return server

    # this is the routine that is called when a message arrives from the event
    # relay.  this is what this routine does:
    #     read and decode the message from the passed in descriptor
    #     update the internal information based on the info in the message
    #     if this message is from a mover - send a message to the mover for a
    #                                       more detailed status
    #     if this message is from a library_manager - send a message to the
    #                                       lib_man for it's state, suspect
    #                                       volume list and queue list.
    def process_event_message(self, fd):
	# the event relay is alive
	now = time.time()
	self.serverfile.output_alive(self.erc.event_relay_addr[0], 
				     self.erc.event_relay_addr[1],
				     ALIVE, now, enstore_constants.EVENT_RELAY)
	self.event_relay.alive(now)
	msg = self.erc.read(fd)
	if msg.type == event_relay_messages.ALIVE:
	    server = self.server_is_alive(msg.server)
	    # if server is a mover, we need to get some extra status
	    if enstore_functions.is_mover(msg.server):
		self.update_mover(server)
	    # if server is a library_manager, we need to get some extra status
	    if enstore_functions.is_library_manager(msg.server):
		self.update_library_manager(server)
	elif msg.type == event_relay_messages.NEWCONFIGFILE:
	    # a new config file was loaded into the config server, get it
	    self.make_config_html_file()

    # these are the things we do periodically, this routine is called when an
    # interval is up, from within dispatching_worker  -
    #      o generate a new log file page
    #      o generate a new encp history page
    #      o process any servers that have not reported in recently
    #      o generate a new server status web page
    def periodic_tasks(self, reason_called=TIMEOUT):
        # update the web page that lists all the current log files
        self.make_log_html_file()
	# just output the inquisitor alive info, if we are doing this, we are alive.
	self.server_is_alive(self.inquisitor.name)
	# update the encp history page
	self.make_encp_html_file()
	# see if there are any servers in cardiac arrest (no heartbeat)
	self.check_last_alive()
	# check if we have received an event realy message recently
	self.check_event_message()
	self.make_server_status_html_file()

    # our client said to update the enstore system status information
    def update(self, ticket):
	self.periodic_tasks(self, USER)
        ticket["status"] = (e_errors.OK, None)
	self.send_reply(ticket)

    def do_dump(self):
	keys = self.server_d.keys()
	keys.sort()
	for skey in keys:
	    server = self.server_d[skey]
	    print repr(server)
	if self.plot_thread and self.plot_thread.isAlive():
	    print "plot thread is alive"
	print ""
	import pprint
	pprint.pprint(self.serverfile.text)
	print ""

    # spill our guts
    def dump(self, ticket):
        ticket["status"] = (e_errors.OK, None)
	self.do_dump()
	self.send_reply(ticket)

    # set the timeout for the periodic_tasks function
    def set_update_interval(self,ticket):
        ticket["status"] = (e_errors.OK, None)
        self.update_interval = ticket["update_interval"]
	self.remove_interval_func(self.periodic_tasks)
	self.add_interval_func(self.periodic_tasks, self.update_interval)
	self.send_reply(ticket)

    def get_update_interval(self, ticket):
        ret_ticket = { 'update_interval' : self.update_interval,
                       'status'      : (e_errors.OK, None) }
	self.send_reply(ret_ticket)

    # update the inq status and exit
    def update_and_exit(self, ticket):
        ticket["status"] = (e_errors.OK, None)
	self.send_reply(ticket)
	self.update_exit(0)

    # set a new refresh value for the html files
    def set_refresh(self, ticket):
        ticket["status"] = (e_errors.OK, None)
        self.serverfile.set_refresh(ticket['refresh'])
        self.encpfile.set_refresh(ticket['refresh'])
	self.send_reply(ticket)

    # return the current refresh value
    def get_refresh(self, ticket):
        ticket["status"] = (e_errors.OK, None)
        ticket["refresh"] = self.serverfile.get_refresh()
	self.send_reply(ticket)

    # set a new max encp lines displayed value
    def set_max_encp_lines(self, ticket):
        ticket["status"] = (e_errors.OK, None)
	self.max_encp_lines = ticket['max_encp_lines']
	self.send_reply(ticket)

    # return the current number of displayed encp lines
    def get_max_encp_lines(self, ticket):
        ticket["status"] = (e_errors.OK, None)
        ticket["max_encp_lines"] = self.max_encp_lines
	self.send_reply(ticket)

    # call the default function defined at the top of the file
    def default_value(self, aKey):
	default_func = "default_%s"%(aKey)
	return eval("%s()"%(default_func))

    def get_value(self, aKey, aValue):
        if aValue == -1:         # nothing was entered on the command line 
            new_val = self.inquisitor.config.get(aKey, self.default_value(aKey))
	    self.got_from_cmdline[aKey] = aValue
        else:
	    self.got_from_cmdline[aKey] = aValue
            new_val = aValue
	return new_val


class Inquisitor(InquisitorMethods, generic_server.GenericServer):

    def __init__(self, csc, html_file="", update_interval=-1, alive_rcv_to=-1, 
		 alive_retries=-1, max_encp_lines=-1, refresh=-1):
        generic_server.GenericServer.__init__(self, csc, MY_NAME, 
					      self.process_event_message)
        Trace.init(self.log_name)
	self.name = MY_NAME
	self.startup_state = e_errors.OK
	# set an interval and retry that we will use the first time to get the
	# config information from the config server.  we do not use the
	# passed values because they might have been defaulted and we need to
	# look them up in the config file which we have not gotten yet.
	use_once_timeout = 5
	use_once_retry = 1
	config_d = self.csc.dump(use_once_timeout, use_once_retry)
	if enstore_functions.is_timedout(config_d):
	    Trace.trace(12,"inquisitor init - ERROR, getting config dict timed out")
	    self.startup_state = e_errors.TIMEDOUT
	    self.startup_text = enstore_constants.CONFIG_SERVER
	    return
	config_d = config_d['dump']
	# these are the servers we will be monitoring, always get the inquisitor first
	self.inquisitor = monitored_server.MonitoredInquisitor(\
				    config_d.get(enstore_constants.INQUISITOR, {}))
	self.server_d = {enstore_constants.INQUISITOR : self.inquisitor}
	event_relay_interval = 0
	self.alarm_server = monitored_server.MonitoredAlarmServer(\
				    config_d.get(enstore_constants.ALARM_SERVER, {}))
	self.server_d[enstore_constants.ALARM_SERVER] = self.alarm_server
	self.log_server = monitored_server.MonitoredLogServer(\
				    config_d.get(enstore_constants.LOG_SERVER, {}))
	self.server_d[enstore_constants.LOG_SERVER]  = self.log_server
	self.file_clerk = monitored_server.MonitoredFileClerk(\
				    config_d.get(enstore_constants.FILE_CLERK, {}))
	self.server_d[enstore_constants.FILE_CLERK]  = self.file_clerk
	self.volume_clerk = monitored_server.MonitoredVolumeClerk(\
				    config_d.get(enstore_constants.VOLUME_CLERK, {}))
	self.server_d[enstore_constants.VOLUME_CLERK]  = self.volume_clerk
	self.config_server = monitored_server.MonitoredConfigServer(\
				    config_d.get(enstore_constants.CONFIG_SERVER, {}))
	self.server_d[enstore_constants.CONFIG_SERVER]  = self.config_server

	for server_key in self.server_d.keys():
	    server = self.server_d[server_key]
	    server.hung_interval = self.inquisitor.get_hung_interval(server.name)
	    event_relay_interval = max(event_relay_interval, server.hung_interval)

	self.lib_man_d = {}
	self.mover_d = {}
	self.media_changer_d = {}
	for key in config_d.keys():
	    self.add_new_mv_lm_mc(key, config_d, event_relay_interval)

	dispatching_worker.DispatchingWorker.__init__(self, 
						      (self.inquisitor.hostip,
						       self.inquisitor.port))
	self.got_from_cmdline = {}
	# if no interval to do updates was entered on the command line, get it from the 
	# configuration file.
	self.update_interval = self.get_value('update_interval', update_interval)

	# if no alive timeout was entered on the command line, get it from the 
	# configuration file.
	self.alive_rcv_timeout = self.get_value('alive_rcv_timeout', alive_rcv_to)

	# if no alive retry # was entered on the command line, get it from the 
	# configuration file.
	self.alive_retries = self.get_value('alive_retries', alive_retries)

	# if no max number of encp lines was entered on the command line, get 
	# it from the configuration file.
	self.max_encp_lines = self.get_value('max_encp_lines', max_encp_lines)

	# get the keys that are associated with the web information
	self.www_server = config_d.get(enstore_constants.WWW_SERVER, {})

	# get the directory where the files we create will go.  this should
	# be in the configuration file.
	if not html_file:
	    if self.inquisitor.html_file:
	        self.html_dir = self.inquisitor.html_file
	        html_file = "%s/%s"%(self.html_dir,
				     enstore_files.status_html_file_name())
	        encp_file = "%s/%s"%(self.html_dir,
				     enstore_files.encp_html_file_name())
	        config_file = "%s/%s"%(self.html_dir,
				       enstore_files.config_html_file_name())
		plot_file = "%s/%s"%(self.html_dir,
				     enstore_files.plot_html_file_name())
	    else:
	        self.html_dir = enstore_files.default_dir
	        html_file = enstore_files.default_status_html_file()
	        encp_file = enstore_files.default_encp_html_file()
	        config_file = enstore_files.default_config_html_file()
		plot_file = enstore_files.default_plot_html_file()

        # if no html refresh was entered on the command line, get it from
        # the configuration file.
        if refresh == -1:
            refresh = self.inquisitor.refresh
	    if not refresh:
		refresh = DEFAULT_REFRESH

	self.system_tag = self.www_server.get(www_server.SYSTEM_TAG, 
					      www_server.SYSTEM_TAG_DEFAULT)

	# these are the files to which we will write, they are html files
	self.serverfile = enstore_files.HTMLStatusFile(html_file, refresh, 
						       self.system_tag)
	self.encpfile = enstore_files.HTMLEncpStatusFile(encp_file, refresh,
							 self.system_tag)
        self.logfile = enstore_files.HTMLLogFile(self.logc.log_dir,
						 LOGHTMLFILE_NAME,
						 self.html_dir, self.system_tag)
        self.configfile = enstore_files.HTMLConfigFile(config_file, 
						       self.system_tag)
	self.plotfile = enstore_files.HTMLPlotFile(plot_file, 
						   self.system_tag)

	# set up a signal handler to catch termination signals (SIGKILL) so we can
	# update our status before dying
	signal.signal(signal.SIGTERM, self.s_update_exit)

	# set up the threads we will need
	self.plot_thread = None

	# set an interval timer to periodically update the web pages
	self.add_interval_func(self.periodic_tasks, self.update_interval)

	self.event_relay_msg = event_relay_messages.EventRelayAliveMsg(self.inquisitor.host,
								       self.inquisitor.port)
	self.event_relay_msg.encode(self.inquisitor.name)

	# setup the communications with the event relay task
	self.erc.start([event_relay_messages.ALIVE,
			event_relay_messages.NEWCONFIGFILE])

	# keep track of when we receive event relay messages.  maybe we can tell if
	# the event relay process goes down.
	self.event_relay = EventRelay(event_relay_interval)

	# setup the initial system page
	for server in self.server_d.keys():
	    self.mark_no_info(self.server_d[server])
	self.mark_event_relay(NO_INFO_YET)

	# update the config page    
	self.update_config_page(config_d)


class InquisitorInterface(generic_server.GenericServerInterface):

    def __init__(self):
	# fill in the defaults for possible options
	self.html_file = ""
	self.alive_rcv_timeout = -1
	self.alive_retries = -1
	self.update_interval = -1
	self.max_encp_lines = -1
	self.refresh = -1
	generic_server.GenericServerInterface.__init__(self)

    # define the command line options that are valid
    def options(self):
	return generic_server.GenericServerInterface.options(self)+[
            "html-file=","update-interval=", "max-encp-lines=", "refresh="] + self.alive_rcv_options()

if __name__ == "__main__":
    Trace.init(string.upper(MY_NAME))
    Trace.trace(6,"inquisitor called with args "+repr(sys.argv))

    # get interface
    intf = InquisitorInterface()

    # get the inquisitor
    inq = Inquisitor((intf.config_host, intf.config_port), 
                     intf.html_file, intf.update_interval,
                     intf.alive_rcv_timeout, intf.alive_retries,
	             intf.max_encp_lines,intf.refresh)

    if inq.startup_state == e_errors.TIMEDOUT:
	Trace.trace(6, "Inquisitor TIMED OUT when contacting %s"%(inq.startup_text))
    else:
	inq.handle_generic_commands(intf)

	while 1:
	    try:
		Trace.log(e_errors.INFO, "Inquisitor (re)starting")
		inq.serve_forever()
	    except SystemExit, exit_code:
		# we need to update the inquisitor page to show that the inquisitor 
		# is not running, then exit fer sure.
		inq.update_exit(exit_code)
	    except:
		e_errors.handle_error()
		inq.serve_forever_error(inq.log_name)
		inq.do_dump()
            continue
    Trace.trace(6,"Inquisitor finished (impossible)")
