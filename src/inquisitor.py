#!/usr/bin/env python

##############################################################################
#
# $Id$
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
import socket
import re
import copy
import select
import traceback

# enstore imports
import monitored_server
import event_relay_messages
import dispatching_worker
import generic_server
import Trace
import e_errors
import enstore_files
import enstore_functions
import enstore_functions2
import enstore_mail
import enstore_erc_functions
import enstore_constants
import enstore_pg
import www_server
import volume_family
import option
import cleanUDP
import udp_server
import read_write_condition_variable

server_map = {"log_server" : enstore_constants.LOGS,
	      "alarm_server" : enstore_constants.ALARMS,
	      "configuration" : enstore_constants.CONFIGS,
	      "file_clerk" : enstore_constants.FILEC,
	      "inquisitor" : enstore_constants.INQ,
	      "info_server" : enstore_constants.INFO,
	      "volume_clerk" : enstore_constants.VOLC,
	      "ratekeeper" : enstore_constants.RATEKEEPER,
	      "enstore" : enstore_constants.ENSTORE,
	      "network" : enstore_constants.NETWORK,
	      "alarms" : enstore_constants.ANYALARMS,
              "dispatcher": enstore_constants.DISPR,
              "lm_director": enstore_constants.LMD, 
              }

server_keys = server_map.keys()

# start of global variables
UP = 1
DOWN = 2
OUTAGE = 3
NOOUTAGE = 4
OVERRIDE = 5
NOOVERRIDE = 6

MY_NAME = enstore_constants.INQUISITOR   #"inquisitor"
LOGHTMLFILE_NAME = "enstore_logs.html"
TIMED_OUT_SP = "    "
DEFAULT_REFRESH = "60"
ALIVE = "alive"
NO_INFO_YET = "no info yet"
SERVER_STATUS_THREAD_TO = 5.0
SERVER_ER_READ_THREAD_TO = 1.0

USER = 0
TIMEOUT=1
NOVALUE = -1
QUESTION = "?"

ER_UPDATE_INTERVAL = 15
ENCP_UPDATE_INTERVAL = 150
LOG_UPDATE_INTERVAL = 300
DEFAULT_OVERRIDE_INTERVAL = 86400   # (1 day) how long something needs to be
                                    #  overridden to generate mail.
OVERRIDE_UPDATE_INTERVAL = 3600     # (1 hr) how often it checks the override file

MOVER_ERROR_STATES = ['OFFLINE', 'ERROR', enstore_constants.DEAD]

DIVIDER = "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"

FF_W = "file_family_width"
NUM_IN_Q = "num_in_q"
RESTRICTED_ACCESS = "RESTRICTED_ACCESS"
#
# maximum number of threads to spawn
#
MAX_THREADS = 50
defaults = {'update_interval': 20,
            'alive_rcv_timeout': 5,
            'alive_retries': 2,
            'max_encp_lines': 50,
            'max_threads': MAX_THREADS,
	    enstore_constants.PAGE_THRESHOLDS: {enstore_constants.FILE_LIST: 200}}

# delete a key from a dictionary if it exists
def delkey(key, dict):
    if dict.has_key(key):
	del dict[key]

def make_node_d(nwc):
    node_d = {}
    keys = nwc.keys()
    for key in keys:
	for node in nwc[key][enstore_constants.NODES]:
	    node_d[node] = [nwc[key][enstore_constants.ACTION],
			    nwc[key][enstore_constants.DO_ACTION_AFTER]]
    else:
	return node_d

# given a directory get a list of the files and their sizes
def get_file_list(directory, prefix):
    logfiles = {}
    filenames = os.listdir(directory)
    # pull out the files and get their sizes
    prefix_len = len(prefix)
    for filename in filenames:
        if filename[0:prefix_len] == prefix and \
               (not filename[-3:] == ".gz") and \
               (not filename[-5:] == ".save"):
            logfiles[filename] = os.stat('%s/%s'%(directory,filename))[stat.ST_SIZE]
    return logfiles

class EventRelay:

    def __init__(self):
        self.last_alive = enstore_constants.NEVER_ALIVE
        self.state = enstore_constants.NEVER_ALIVE
        self.start = time.time()
        self.name = enstore_constants.EVENT_RELAY
        self.heartbeat = enstore_constants.EVENT_RELAY_HEARTBEAT + 15

    def __repr__(self):
        import pprint
        return "event_relay : %s\n%s"%(pprint.pformat(self.__dict__), DIVIDER)

    def alive(self, now):
        self.last_alive = now
        enstore_functions.inqTrace(enstore_constants.INQSERVERDBG,
				   "setting event relay as alive")
        self.state = ALIVE

    def dead(self):
        enstore_functions.inqTrace(enstore_constants.INQSERVERDBG,
				   "setting event relay as dead")
        self.state = enstore_constants.DEAD

    def set_state(self, now):
        enstore_functions.inqTrace(enstore_constants.INQSERVERTIMESDBG,
		 "Now: %s, Last alive: %s, Heartbeat: %s"%(time.ctime(now),
						   time.ctime(self.last_alive),
						   self.heartbeat))
        if (now - self.last_alive) > self.heartbeat:
            self.dead()

    def is_alive(self):
        self.set_state(time.time())
        if self.state == ALIVE:
            rtn = 1
        else:
            rtn = 0
        return rtn

    def is_dead(self, now):
        self.set_state(now)
        if self.state == enstore_constants.DEAD:
            rtn = 1
        else:
            rtn = 0
        return rtn


class InquisitorMethods(dispatching_worker.DispatchingWorker):

    # this whole function is here to calm down pychecker so that
    # real errors can perhaps be found.
    def __init__(self):
	self.server_d = None
        self.server_er_msg = {}
	self.inquisitor = None
	self.erc = None
	self.event_relay = None
	self.serverfile = None
	self.encpfile = None
	self.logfile = None
	self.configfile = None
	self.got_from_cmdline = None
	self.csc = None
	self.logc = None
	self.log_server = None
	self.server_status_file_event = None
	self.exit_now_event = None
        self.sent_stalled_mail = {}
	self.servers_by_name = {}
	self.override_mail_sent = {}
	self.mover_state = {}
	self.migrator_state = {}
	self.lm_queues = {}
        self.name = MY_NAME
	self.html_dir = None
        self.er_lock = threading.Lock()
        self.rw_lock = read_write_condition_variable.ReadWriteConditionVariable()
        self.max_threads=MAX_THREADS

    def get_server(self, name):
	if type(name) == types.ListType:
	    name = name[0]
	servers = self.server_d.keys()
	for server in servers:
	    if name == self.server_d[server].name:
		return self.server_d[server]
	else:
	    return None

    # look for the hung_rcv_timeout value for the specified key in the inquisitor
    # config info.  if it does not exist, return the default
    def get_hung_to(self, key):
        return self.inquisitor.get("hung_rcv_timeout", {}).get(key,
                                               monitored_server.DEFAULT_HUNG_INTERVAL)

    def mark_event_relay(self, state):
        self.serverfile.output_etimedout(self.erc.event_relay_addr[0], state,
                                         time.time(), enstore_constants.EVENT_RELAY,
                                         self.event_relay.last_alive)
	self.new_server_status = 1
        enstore_functions.inqTrace(enstore_constants.INQSERVERDBG,
				   "mark event relay as %s"%(state,))

    def mark_server(self, state, server):
        if server.no_thread():
            self.serverfile.output_etimedout(server.host, state, time.time(),
					     server.name, server.output_last_alive)
            self.new_server_status = 1
            enstore_functions.inqTrace(enstore_constants.INQSERVERDBG,
				       "mark %s as %s"%(server.name, state,))

    # mark a server as not having sent a heartbeat yet.
    def mark_no_info(self, server):
        self.mark_server(NO_INFO_YET, server)

    # mark a server as having timed out, this happens if no alive message is
    # received from the event_relay for this server
    def mark_timed_out(self, server):
        self.mark_server(enstore_constants.TIMED_OUT, server)

    # mark a server as having possible hung, this happens if no alive message is
    # received from the event_relay for this server after server.hung_interval time
    def mark_dead(self, server):
        self.mark_server(enstore_constants.DEAD, server)
	server.server_status = enstore_constants.DEAD

    # called by the signal handling routines
    def s_update_exit(self, the_signal, frame):
        __pychecker__ = "unusednames=the_signal,frame"
        self.update_exit(0)

    def ok_to_monitor(self, config_dict):
        return not config_dict.has_key("inq_ignore")

    def is_server_known_down(self, server):
        # check to see if the server is known to be down by enstore.
        (sfile, outage_d, offline_d, override_d)=(None,None,None,None)
        self.rw_lock.acquire_read()
        try:
            sfile, outage_d, offline_d, override_d = enstore_files.read_schedule_file(self.html_dir)
        finally:
            self.rw_lock.release_read()

        if offline_d and offline_d.has_key(server.name):
            # server is known to be down
            return 1
        else:
            return 0

    # function called to attempt a restart on a 'dead' server
    def restart_function(self, server):
        prefix = "INQ_CHILD"
        alarm_info = {'server' : server.name}
        try:
            # we should try to restart the server.  try 3X
            i = 0
            known_down = 0
            first_time = 1
            # we need just the node name part of the host name
            node = string.split(server.host, ".", 1)
            alarm_info['node'] = node
            while i < 3:
                enstore_functions.inqTrace(enstore_constants.INQRESTARTDBG,
				      "Restart try %s for %s"%(i, server.name))
                if server.delete:
                    # we no longer need to try to restart this server.  possibly
                    # while we were trying to restart it, it was removed from the
                    # config file.
                    Trace.log(e_errors.INFO,
                              "%s: Aborting restart attempt of %s"%(prefix,
                                                                    server.name))
                    break
                # do not do the stop and start if the event relay is not alive or if this
                # server is marked as known down.
                known_down = self.is_server_known_down(server)
                if self.event_relay.is_alive() and (not known_down):
                    if first_time:
                        Trace.log(e_errors.INFO,
                                  "%s: Attempting restart of %s"%(prefix, server.name))
                        first_time = 0
                    os.system('enstore Estop %s "--just %s"'%(node[0], server.name))
                    time.sleep(20)
                    os.system('enstore Estart %s "--just %s"'%(node[0], server.name))

                # check if now alive - to do this, wait the equivalent of hung_interval
                # for this server and then see if an event relay message has arrived
                k = 15
                j = 0
                while j < server.hung_interval:
                    time.sleep(k)
                    j = j + k
                if server.check_recent_alive(self.event_relay) == monitored_server.NO_TIMEOUT:
                    Trace.log(e_errors.INFO, "%s: Restarted %s"%(prefix, server.name))
                    break
                else:
                    i = i + 1
            else:
                # we could not restart the server, do not try again
                server.cant_restart()
                # do not send an alarm if the server is known down as we really did not
                # try to restart it
                if known_down:
                    Trace.log(e_errors.INFO,
                              "%s: %s is marked known down, no restart attempted"%(prefix,
                                                                                   server.name))
                else:
                    if not server.name == enstore_constants.ALARM_SERVER:
                        Trace.alarm(e_errors.ERROR, e_errors.CANTRESTART, alarm_info)
                    else:
                        Trace.log(e_errors.ERROR, "%s: Can't restart %s"%(prefix,
                                                                          server.name))
        except:
            # we catch any exception from the thread as we do not want it to
            # escape from this area and start executing the main loop of the
            # inquisitor code.  we will output an error and then exit.
            Trace.handle_error()
            self.serve_forever_error(prefix)

    # this restarting may be overridden in the config file on a per server basis.
    def attempt_restart(self, server):
        # could not communicate with the server.  create a thread that will try
        # to restart it, if we have not already done this
        if server.no_thread():
            # if we raise an alarm we need to include the following info.
            alarm_info = {'server' : server.name}
            # first see if the server is supposed to be restarted.
            if server.norestart:
                if not server.did_restart_alarm:
                    # do not restart, raise an alarm that the
                    # server is dead.
                    if not server.name == enstore_constants.ALARM_SERVER:
                        Trace.alarm(e_errors.ERROR, e_errors.SERVERDIED, alarm_info)
                    else:
                        Trace.log(e_errors.ERROR,
                                  "%s died and will not be restarted"%(server.name,))
                    server.did_restart_alarm = 1
		    server.do_hack_restart()
            else:
                # do not attempt to do the restart if the event relay is not alive.
                if (not server.restart_failed) and self.event_relay.is_alive():
                    # we must keep track of the fact that we created a thread for this
                    # client so the next time we find the server dead we do not create
                    # another one.
                    server.restart_thread = threading.Thread(group=None,
                                                             target=self.restart_function,
                                                             name="RESTART_%s"%(server.name,),
                                                             args=(server,))
                    enstore_functions.inqTrace(enstore_constants.INQRESTARTDBG,
			     "Inquisitor creating thread to restart %s, event_relay is %s"%(server.name,
											            self.event_relay.state))
                    server.restart_thread.setDaemon(1)
                    server.restart_thread.start()

    # do the actual write in the thread
    def do_server_status_write(self):
	enstore_functions.inqTrace(enstore_constants.INQTHREADDBG,
				   "Starting write of status files")
	if self.serverfile_new:
	    self.serverfile_new.open()
	    self.serverfile_new.write()
	    self.serverfile_new.close()
	    self.serverfile_new.install()

    # this is the file writing thread
    def make_server_status_html_file(self):
	while 1:
	    try:
		self.server_status_file_event.wait(SERVER_STATUS_THREAD_TO)
		if self.server_status_file_event.isSet() or \
		   self.exit_now_event.isSet():
		    self.do_server_status_write()
		    self.server_status_file_event.clear()
		if self.exit_now_event.isSet():
		    enstore_functions.inqTrace(enstore_constants.INQTHREADDBG,
					       "Exiting write of status files thread")
		    return
	    except:
		# this is the write thread.  catch anything, report it and
		# carry on.
                self.do_dump()
		self.server_status_file_event.clear()
		Trace.handle_error()
		self.serve_forever_error(self.log_name+"WT")

    # signal the thread that the server status file can be written
    def write_server_status_file(self):
	if not self.server_status_file_event.isSet() and self.new_server_status:
	    # there is no thread doing this, we can do it
	    # copy the information to write out
	    self.serverfile_new = copy.deepcopy(self.serverfile)
	    enstore_functions.inqTrace(enstore_constants.INQFILEDBG,
				       "Signaling thread to write out status files")
	    self.server_status_file_event.set()
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
                enstore_functions.inqTrace(enstore_constants.INQFILEDBG,
					   "make new log file")

    def add_new_server(self, key, config_d):
        if enstore_functions2.is_mover(key):
            cdict = config_d[key]
            if self.ok_to_monitor(cdict):
                self.er_lock.acquire()
                self.server_d[key] = monitored_server.MonitoredMover(cdict, key,
								     self.csc)
                self.er_lock.release()
        elif enstore_functions2.is_migrator(key):
            cdict = config_d[key]
            if self.ok_to_monitor(cdict):
                self.er_lock.acquire()
                try:
                    self.server_d[key] = monitored_server.MonitoredMigrator(cdict, key,
                                                                        self.csc)
                except:
                    pass
                self.er_lock.release()
        elif enstore_functions2.is_media_changer(key):
            cdict = config_d[key]
            if self.ok_to_monitor(cdict):
                self.er_lock.acquire()
                self.server_d[key] = monitored_server.MonitoredMediaChanger(cdict,
                                                                            key)
                self.er_lock.release()
        elif enstore_functions2.is_udp_proxy_server(key):
            cdict = config_d[key]
            if self.ok_to_monitor(cdict):
                self.er_lock.acquire()
                try:
                    self.server_d[key] = monitored_server.MonitoredUDPProxyServer(cdict,
                                                                                  key)
                except:
                    pass
                self.er_lock.release()
        elif enstore_functions2.is_library_manager(key):
            cdict = config_d[key]
            if self.ok_to_monitor(cdict):
                self.er_lock.acquire()
                self.server_d[key] = monitored_server.MonitoredLibraryManager(cdict,
									      key,
									      self.csc)
                self.er_lock.release()
        elif enstore_functions2.is_generic_server(key):
	    # this is a generic server - make sure we are now monitoring this server
            cdict = config_d[key]
	    if self.ok_to_monitor(cdict) and self.servers_by_name.has_key(key):
                self.er_lock.acquire()
		self.server_d[key] = self.servers_by_name[key]
                self.er_lock.release()
		# set the last alive time to now.
		self.server_d[key].last_alive = time.time()
	    else:
		# we do not keep this server in our list
		return
	else:
	    # nothing to see here
	    return
        if self.ok_to_monitor(cdict):
            self.server_d[key].hung_interval = \
                                    self.inquisitor.get_hung_interval(self.server_d[key].name)
        else:
            self.serverfile.dont_monitor(key, cdict.get("host", ""))

    def update_config_page(self, config):
        self.configfile.open()
        self.configfile.write(config)
        self.configfile.close()
        self.configfile.install()
        enstore_functions.inqTrace(enstore_constants.INQFILEDBG,
				   "make new html config file")

    def stop_monitoring(self, server, skey):
        self.serverfile.dont_monitor(server.name, server.host)
        self.er_lock.acquire()
        del self.server_d[skey]
        self.er_lock.release()

    def get_value(self, aKey, aValue):
	self.got_from_cmdline[aKey] = aValue
        if aValue == NOVALUE:         # nothing was entered on the command line
            new_val = self.inquisitor.config.get(aKey, defaults.get(aKey))
        else:
            new_val = aValue
        return new_val

    def update_variables_from_config(self, config):
        self.inquisitor.update_config(config.get(self.inquisitor.name, {}))
        if not self.inquisitor.config:
            # the inquisitor information is no longer in the config file.  exit
            self.update_exit(1)
        for skey in self.server_d.keys():
            server = self.server_d[skey]
            new_server_config = config.get(server.name, {})
            if new_server_config:
                if self.ok_to_monitor(new_server_config):
                    server.update_config(new_server_config)
                    server.hung_interval = self.inquisitor.get_hung_interval(server.name)
                else:
                    # we should no longer be monitoring this server
                    self.stop_monitoring(server, skey)
            else:
                # this server no longer exists in the config file, get rid of it
                # from our internal dictionary and the output html file.
                if not server.name == enstore_constants.CONFIG_SERVER:
                    self.stop_monitoring(server, skey)
                    ##del self.server_d[skey]
                    ##self.serverfile.remove_server(server.name)
        # check the new config for any new servers we need to add. only handle movers,
        # library managers and media changers for now.
        for skey in config.keys():
            if not self.server_d.has_key(skey):
                self.add_new_server(skey, config)

        self.www_server = config.get(enstore_constants.WWW_SERVER, {})
        # only update the following values if they were not set on the command line
        key = 'alive_rcv_timeout'
        self.alive_rcv_timeout = self.get_value(key, self.got_from_cmdline[key])
        key = 'alive_retries'
        self.alive_retries = self.get_value(key, self.got_from_cmdline[key])
        key = 'max_encp_lines'
        self.max_encp_lines = self.get_value(key, self.got_from_cmdline[key])
        key = 'max_threads'
        self.max_threads = self.get_value(key, self.got_from_cmdline[key])

	# update any page thresholds
	# get the thresholds which determine when we need an extra web page or two
	self.page_thresholds = self.get_value(enstore_constants.PAGE_THRESHOLDS, NOVALUE)
	self.serverfile.page_thresholds = self.page_thresholds

    # update the file that contains the configuration file information
    def make_config_html_file(self):
        d = {}
        # get the config dictionary
        d = self.csc.dump(self.alive_rcv_timeout, self.alive_retries)
        if e_errors.is_timedout(d):
            enstore_functions.inqTrace(enstore_constants.INQERRORDBG,
		"make_config_html_file - ERROR, getting config dict timed out")
            return
        # we may not have gotten the dict so check for it first before writing it.
        self.config_d = d.get('dump', {})
        self.update_config_page(self.config_d)

        # now update all of the internal information based on the new config info.
        self.update_variables_from_config(self.config_d)

    def handle_lmc_error(self, lib_man, time, state):
        status = enstore_functions2.get_status(state)
        self.serverfile.output_error(lib_man.host, status, time, lib_man.name)
        enstore_functions.inqTrace(enstore_constants.INQERRORDBG,
				   "lm client - ERROR: %s"%(status,))

    # get the library manager suspect volume list and output it
    def suspect_vols(self, lib_man, time):
        enstore_functions.inqTrace(enstore_constants.INQSERVERDBG,
		 "get new suspect vol list from %s"%(lib_man.name,))
        message=""
	try:
	    state = lib_man.client.get_suspect_volumes()
        except (socket.error, select.error, e_errors.EnstoreError), detail:
            if detail.errno == errno.ETIMEDOUT:
                message = "Timeout while getting suspect vols from %s (%s)" \
                          % (lib_man.name, str(detail))
            else:
                message = "Error while getting suspect vols from %s (%s)" \
                          % (lib_man.name, str(detail))
            Trace.log(e_errors.ERROR, message, e_errors.IOERROR)
            return None
	except e_errors.TCP_EXCEPTION, detail:
	    message = "Error while getting suspect vols from %s (%s)" \
                      % (lib_man.name, str(detail))
	    Trace.log(e_errors.ERROR, message, e_errors.IOERROR)
	    return None

	except errno.errorcode[errno.ETIMEDOUT], detail:
	    message = "Error while getting suspect vols from %s (%s)" \
                      % (lib_man.name, str(detail))
	    Trace.log(e_errors.ERROR, message, e_errors.IOERROR)
	    return None

	lib_man.check_suspect_vols(state)
        self.serverfile.output_suspect_vols(state, lib_man.name)
        if e_errors.is_timedout(state):
            self.serverfile.output_etimedout(lib_man.host,
					     enstore_constants.NO_SUSPECT_VOLS,
                                             time, lib_man.name,
					     lib_man.output_last_alive)
            enstore_functions.inqTrace(enstore_constants.INQERRORDBG,
				       "suspect_vols - ERROR, timed out")
            return None
        elif not e_errors.is_ok(state):
            self.handle_lmc_error(lib_man, time, state)
            return None
        return 1

    def default_suspect_vols(self, lib_man):
        if not self.serverfile.text[lib_man.name].has_key(enstore_constants.SUSPECT_VOLS):
            self.serverfile.text[lib_man.name][enstore_constants.SUSPECT_VOLS] = ["None"]

    check_nodes = ["d0olc",]

    def num_in_queue(self, node, queue, ff_d={}):
	if ff_d:
	    ff_dict = ff_d
	else:
	    ff_dict = {}
        enstore_functions.inqTrace(enstore_constants.INQERRORDBG,
                                   "num in queue = %s"%(len(queue),))
        # if there was no queue, then it is a safe_dict. so check if it
        # is empty first. otherwise this becomes an infinite loop
        if queue:
            for elem in queue:
                if elem['work'] == "write_to_hsm" and \
                   enstore_functions2.strip_node(elem['wrapper']['machine'][1]) == node:
                    ff = elem['vc']['file_family']
                    if ff_dict.has_key(ff):
                        if ff_dict[ff][FF_W] > elem['vc']['file_family_width']:
                            ff_dict[ff][FF_W] = elem['vc']['file_family_width']
                        ff_dict[ff][NUM_IN_Q] = ff_dict[ff][NUM_IN_Q] + 1
                    else:
                        ff_dict[ff] = {FF_W : elem['vc']['file_family_width']}
                        ff_dict[ff][NUM_IN_Q] = 1

        enstore_functions.inqTrace(enstore_constants.INQERRORDBG,
                                   "return from num_in_queue")
	return ff_dict

    def get_stalled_q_mail_key(self, server_name, node, ff):
	return "%s,%s,%s"%(server_name, ff, node)

    def split_stalled_q_mail_key(self, key):
	return string.split(key, ',', 2)

    def already_sent_mail(self, server_name, ff, node):
	key = self.get_stalled_q_mail_key(server_name, node, ff)
	if not self.sent_stalled_mail.has_key(key):
	    self.sent_stalled_mail[key] = 1
	    rtn = 0
	else:
	    rtn = 1
	return rtn

    def clear_sent_stalled_mail(self, server_name, node, ff=""):
	if ff:
	    key = self.get_stalled_q_mail_key(server_name, node, ff)
	    if self.sent_stalled_mail.has_key(key):
		del(self.sent_stalled_mail[key])
	else:
	    # clear all elements for this server, node combo
	    keys = self.sent_stalled_mail.keys()
	    for key in keys:
		name, ff, snode = self.split_stalled_q_mail_key(key)
		if name == server_name and snode == node:
		    del(self.sent_stalled_mail[key])

    # we do this when a queue seems to be stalled
    def queue_is_stalled(self, server, node, ff, pend_num, wam_num):
	# queue may be stalled, or we may be in a transitional state.  we should make
	# sure the same problem exists several times in a row.
	if self.lm.is_really_stalled(node, ff) and not self.already_sent_mail(server.name, ff, node):
	    # send mail and raise an alarm, this looks stalled.
	    txt = "Write queue stall for %s, file_family: %s, from node %s"%(server.name,
									     ff, node)
	    Trace.alarm(e_errors.ERROR, txt)
	    enstore_functions.inqTrace(enstore_constants.INQSERVERDBG, txt)
	    enstore_mail.send_mail(MY_NAME,
	      enstore_mail.format_mail("Write data using the full file_family width to enstore from %s"%(node,),
			  "Why are there %s elems in the pend queue and only %s elems in the wam queue?"%(pend_num,
													  wam_num),
					    txt), "Write Queue Stall")
	    enstore_functions.inqTrace(enstore_constants.INQSERVERDBG,
				       "get new work queue from %s"%(server.name,))

    def check_for_stalled_queue(self, lib_man):
	# get the number of writes that are being done now
	self.lm = self.server_d[lib_man.name]
	for node in self.check_nodes:
	    # we can overwrite ff_width because we are really concerned about what is in
	    # the pending queue.  if there is nothing, ff_width  = 0, but we don't care
	    wam_dict = self.num_in_queue(node, lib_man.wam_queue)
	    # the pending queue is actually 3 queues, ignore the read queue
            enstore_functions.inqTrace(enstore_constants.INQERRORDBG,
                                       "num in queue = %s"%(len(lib_man.pend_queue[enstore_constants.READ_QUEUE],),))
	    pend_dict2 = self.num_in_queue(node,
					  lib_man.pend_queue[enstore_constants.ADMIN_QUEUE])
	    pend_dict = self.num_in_queue(node,
					   lib_man.pend_queue[enstore_constants.WRITE_QUEUE],
					  pend_dict2)
	    # loop over all the file families that have an element in the pending queue.
	    # see if there are file_family_width elements in the wam queue to account for
	    # there being elements in the pending queue.
	    keys = pend_dict.keys()
	    for ff in keys:
		if wam_dict.has_key(ff):
		    # there are elements in the work at movers queue
		    if wam_dict[ff][NUM_IN_Q] < wam_dict[ff][FF_W]:
			# we should be processing more for this file_family.
			self.queue_is_stalled(lib_man, node, ff, pend_dict[ff][NUM_IN_Q],
					      wam_dict[ff][NUM_IN_Q])
		    else:
			# no stall here, move along
			self.lm.no_stall(node, ff)
			self.clear_sent_stalled_mail(lib_man.name, node, ff)
			enstore_functions.inqTrace(enstore_constants.INQSERVERDBG,
						   "%s queue not stalled"%(lib_man.name))
		else:
		    # there are no elements in the wam queue for this ff. oops.
		    self.queue_is_stalled(lib_man, node, ff, pend_dict[ff][NUM_IN_Q], 0)

	    # if the pending queue did not have any elements from this node that were stalled, clear out
	    # ff_stalled of anyhting from before, all is ok now
	    if not keys:
		self.lm.no_stall(node)
		self.clear_sent_stalled_mail(lib_man.name, node)

    # get the library manager work queue and output it
    def work_queue(self, lib_man, time):
        enstore_functions.inqTrace(enstore_constants.INQSERVERDBG,
				  "get new work queue from %s"%(lib_man.name,))
	try:
	    self.lm_queues[lib_man.name] = lib_man.client.getworks_sorted()
        except (socket.error, select.error, e_errors.EnstoreError), detail:
            if detail.errno == errno.ETIMEDOUT:
                message = "Timeout while getting sorted work queue from %s (%s)" \
                          % (lib_man.name, str(detail))
            else:
                 message = "Error while getting sorted work queue from %s (%s)" \
                           % (lib_man.name, str(detail))
	    Trace.log(e_errors.ERROR, message, e_errors.IOERROR)
	    return None
	except e_errors.TCP_EXCEPTION, detail:
	    message = "Error while getting sorted work queue from %s (%s)" \
                      % (lib_man.name, str(detail))
	    Trace.log(e_errors.ERROR, message, e_errors.IOERROR)
	    return None
	except errno.errorcode[errno.ETIMEDOUT], detail:
	    message = "Timeout while getting sorted work queue from %s (%s)" \
                      % (lib_man.name, str(detail))
	    Trace.log(e_errors.ERROR, message, e_errors.IOERROR)
	    return None

	lib_man.check_work_queue(self.lm_queues[lib_man.name])
	lib_man.wam_queue = self.lm_queues[lib_man.name][enstore_constants.ATMOVERS]
	lib_man.pend_queue = self.lm_queues[lib_man.name][enstore_constants.PENDING_WORKS]
        self.serverfile.output_lmqueues(self.lm_queues[lib_man.name], lib_man.name)
        if e_errors.is_timedout(self.lm_queues[lib_man.name]):
            self.serverfile.output_etimedout(lib_man.host,
					     enstore_constants.NO_WORK_QUEUE,
                                             time, lib_man.name,
					     lib_man.output_last_alive)
            enstore_functions.inqTrace(enstore_constants.INQERRORDBG,
				       "work_queue - ERROR, timed out")
            return None
        elif not e_errors.is_ok(self.lm_queues[lib_man.name]):
            self.handle_lmc_error(lib_man, time, self.lm_queues[lib_man.name])
            return None
        return 1

    def default_work_queue(self, lib_man):
        if not self.serverfile.text[lib_man.name].has_key(enstore_constants.WORK):
            self.serverfile.text[lib_man.name][enstore_constants.WORK] = enstore_constants.NO_WORK
        if not self.serverfile.text[lib_man.name].has_key(enstore_constants.PENDING):
            self.serverfile.text[lib_man.name][enstore_constants.PENDING] = enstore_constants.NO_PENDING
        self.serverfile.text[lib_man.name][enstore_constants.TOTALPXFERS] = QUESTION
        self.serverfile.text[lib_man.name][enstore_constants.READPXFERS] = QUESTION
        self.serverfile.text[lib_man.name][enstore_constants.WRITEPXFERS] = QUESTION
        self.serverfile.text[lib_man.name][enstore_constants.TOTALONXFERS] = QUESTION
        self.serverfile.text[lib_man.name][enstore_constants.READONXFERS] = QUESTION
        self.serverfile.text[lib_man.name][enstore_constants.WRITEONXFERS] = QUESTION

    # get the library manager active_volumes and output it
    def active_volumes(self, lib_man, time):
        enstore_functions.inqTrace(enstore_constants.INQSERVERDBG,
				  "get new active volumes from %s"%(lib_man.name,))
	try:
	    ticket = lib_man.client.get_active_volumes(self.alive_rcv_timeout,
						       self.alive_retries)
        except (socket.error, select.error, e_errors.EnstoreError), detail:
            if detail.errno == errno.ETIMEDOUT:
                message = "Timeout while getting active volumes from %s (%s)" \
                      % (lib_man.name, str(detail))
            else:
                message = "Error while getting active volumes from %s (%s)" \
                      % (lib_man.name, str(detail))
	    Trace.log(e_errors.ERROR, message, e_errors.IOERROR)
	    return None
	except e_errors.TCP_EXCEPTION, detail:
	    message = "Error while getting active volumes from %s (%s)" \
                      % (lib_man.name, str(detail))
	    Trace.log(e_errors.ERROR, message, e_errors.IOERROR)
	    return None
	except errno.errorcode[errno.ETIMEDOUT], detail:
	    message = "Timeout while getting active volumes from %s (%s)" \
                      % (lib_man.name, str(detail))
	    Trace.log(e_errors.ERROR, message, e_errors.IOERROR)
	    return None

	lib_man.check_active_vols(ticket)
	lib_man.active_volumes = ticket[enstore_constants.MOVERS]
        self.serverfile.output_lmactive_volumes(lib_man.active_volumes, lib_man.name)
        if e_errors.is_timedout(self.lm_queues[lib_man.name]):
            self.serverfile.output_etimedout(lib_man.host,
					     enstore_constants.NO_ACTIVE_VOLS,
                                             time, lib_man.name,
					     lib_man.output_last_alive)
            enstore_functions.inqTrace(enstore_constants.INQERRORDBG,
				       "active volumes - ERROR, timed out")
            return None
        elif not e_errors.is_ok(self.lm_queues[lib_man.name]):
            self.handle_lmc_error(lib_man, time, self.lm_queues[lib_man.name])
            return None
        return 1

    # there was an error getting this information, default it
    def default_active_volumes(self, lib_man):
        if not self.serverfile.text[lib_man.name].has_key(enstore_constants.ACTIVE_VOLUMES):
            self.serverfile.text[lib_man.name][enstore_constants.ACTIVE_VOLUMES] = []

    # get the library manager state and output it
    def lm_state(self, lib_man, time):
        enstore_functions.inqTrace(enstore_constants.INQSERVERDBG,
				   "get new state from %s"%(lib_man.name,))
	try:
	    state = lib_man.client.get_lm_state(self.alive_rcv_timeout,
						self.alive_retries)
        except (socket.error, select.error, e_errors.EnstoreError), detail:
            if detail.errno == errno.ETIMEDOUT:
                message = "Timeout while getting state from %s (%s)" \
                      % (lib_man.name, str(detail))
            else:
                message = "Error while getting state from %s (%s)" \
                      % (lib_man.name, str(detail))
	    Trace.log(e_errors.ERROR, message, e_errors.IOERROR)
	    return None
	except e_errors.TCP_EXCEPTION, detail:
	    message = "Error while getting state from %s (%s)" \
                      % (lib_man.name, str(detail))
	    Trace.log(e_errors.ERROR, message, e_errors.IOERROR)
	    return None
	except errno.errorcode[errno.ETIMEDOUT], detail:
	    message = "Timeout while getting state from %s (%s)" \
                      % (lib_man.name, str(detail))
	    Trace.log(e_errors.ERROR, message, e_errors.IOERROR)
	    return None

	lib_man.check_state(state)
	lib_man.server_status = state[enstore_constants.STATE]
        self.serverfile.output_lmstate(state, lib_man.name)
        if e_errors.is_timedout(state):
            self.serverfile.output_etimedout(lib_man.host,
					     enstore_constants.NO_STATE,
                                             time, lib_man.name,
					     lib_man.output_last_alive)
            enstore_functions.inqTrace(enstore_constants.INQERRORDBG,
				       "lm_state - ERROR, timed out")
            return None
        elif not e_errors.is_ok(state):
            self.handle_lmc_error(lib_man, time, state)
            return None
        return 1

    # get the information from the library manager(s)
    def update_library_manager(self, lib_man):
        rtn = 0      # assume will timeout on something
        # get a client and then check if the server is alive
        now = time.time()
        if self.lm_state(lib_man, now):
            if self.suspect_vols(lib_man, now):
                if self.work_queue(lib_man, now):
                    if self.active_volumes(lib_man, now):
                        self.check_for_stalled_queue(lib_man)
                        self.new_server_status = 1
                        rtn = 1         # success
                    else:
                        # there was an error, we do not have the active volumes  information,
                        # if we never did default it
                        self.default_active_volumes(lib_man)
                else:
                    # there was an error, we do not have the work queue information,
                    # if we never did default it
                    self.default_work_queue(lib_man)
                    # also the active_vols
                    self.default_active_volumes(lib_man)
            else:
                # there was an error, we do not have the suspect vols information,
                # if we never did default it
                self.default_suspect_vols(lib_man)
                # also the work_queue and active_vols
                self.default_work_queue(lib_man)
                self.default_active_volumes(lib_man)
        else:
            # there was an error, we do not have the lm_state information
            self.serverfile.text[lib_man.name][enstore_constants.LMSTATE] = enstore_constants.NO_STATE
            # also the suspect vols, work_queue and active_vols
            self.default_suspect_vols(lib_man)
            self.default_work_queue(lib_man)
            self.default_active_volumes(lib_man)
        return rtn

    # get the information from the mover
    def update_mover(self, mover):
        rtn = 1          # assume no timeouts
        enstore_functions.inqTrace(enstore_constants.INQSERVERDBG,
				   "get new state from %s"%(mover.name,))
        self.mover_state[mover.name] = mover.client.status(self.alive_rcv_timeout,
							   self.alive_retries)
	mover.check_status_ticket(self.mover_state[mover.name])
        self.serverfile.output_moverstatus(self.mover_state[mover.name], mover.name)
	mover.server_status = self.mover_state[mover.name][enstore_constants.STATE]
        if e_errors.is_timedout(self.mover_state[mover.name]):
            rtn = 0
            self.serverfile.output_etimedout(mover.host, enstore_constants.NO_STATE,
					     time.time(), mover.name,
					     mover.output_last_alive)
            enstore_functions.inqTrace(enstore_constants.INQERRORDBG,
				       "mover_status - ERROR, timed out")
        self.new_server_status = 1
        return rtn

    # get the information from the migrator
    def update_migrator(self, migrator):
        rtn = 1          # assume no timeouts
        enstore_functions.inqTrace(enstore_constants.INQSERVERDBG,
				   "get new state from %s"%(migrator.name,))

        self.migrator_state[migrator.name] = migrator.client.status(self.alive_rcv_timeout,
							   self.alive_retries)
        mig_state = self.migrator_state[migrator.name]
	migrator.check_status_ticket(mig_state)
        self.serverfile.output_migratorstatus(mig_state, migrator.name)
        migrator.server_status = {}
        for k in mig_state:
            if isinstance(mig_state[k], dict):
                migrator.server_status[k] = (mig_state[k]['state'], mig_state[k]['internal_state'])
	#migrator.server_status = self.migrator_state[migrator.name][enstore_constants.STATE]

        if e_errors.is_timedout(mig_state):
            rtn = 0
            self.serverfile.output_etimedout(migrator.host, enstore_constants.NO_STATE,
					     time.time(), migrator.name,
					     migrator.output_last_alive)
            enstore_functions.inqTrace(enstore_constants.INQERRORDBG,
				       "migrator_status - ERROR, timed out")
        self.new_server_status = 1
        return rtn

    # only change the status of the inquisitor on the system status page to
    # timed out, then exit.
    def update_exit(self, exit_code):
        self.serverfile.output_alive(self.inquisitor.host,
                                     "exiting", time.time(), self.name)
        # the above just stored the information, now write the page out and
	# tell the thread to exit
        enstore_functions.inqTrace(enstore_constants.INQTHREADDBG,
				   "setting exit event for threads")
	self.exit_now_event.set()
	self.write_server_status_file()
        # Don't fear the reaper!!
        enstore_functions.inqTrace(enstore_constants.INQERRORDBG,
				   "exiting inquisitor due to request")
	if self.server_status_file_thread.isAlive():
	    self.server_status_file_thread.join()
	if self.er_thread.isAlive():
	    self.er_thread.join()
        os._exit(exit_code)

    # get the encp transfer information from the accounting database
    def make_encp_html_file(self, now):
        self.last_encp_update = now
        #self.encp_xfer_but_no_update = 0
        enstore_functions.inqTrace(enstore_constants.INQERRORDBG,
                      "starting search of accounting db for %s encp history lines"%(self.max_encp_lines,))
        encplines = enstore_pg.acc_encp_lines(self.csc, self.max_encp_lines)
        encplines.sort()
        enstore_functions.inqTrace(enstore_constants.INQERRORDBG,
                                   "update_encp - found %s lines"%(len(encplines,)))
         # now we have some info, output it
        self.encpfile.open()
        self.encpfile.write(encplines)
        self.encpfile.close()
        self.encpfile.install()
        enstore_functions.inqTrace(enstore_constants.INQFILEDBG,
				   "make new html encp file")

    # examine each server we are monitoring to see if we have received an alive from
    # it recently (via the event relay).  if it was processed successfully during this
    # time through it should be marked as up
    def check_last_alive(self, msgs_rcvd):
        for skey in self.server_d.keys():
            if skey not in msgs_rcvd:
                server = self.server_d[skey]
                status = server.check_recent_alive(self.event_relay)
                if status == monitored_server.TIMEDOUT:
                    self.mark_timed_out(server)
                elif status == monitored_server.HUNG:
                    self.mark_dead(server)
                    self.attempt_restart(server)

    def check_event_relay_last_alive(self):
        if self.event_relay.is_dead(time.time()):
            self.mark_event_relay(enstore_constants.DEAD)
            if not self.sent_event_relay_alarm:
                Trace.alarm(e_errors.ERROR,
                            e_errors.TIMEDOUT, {'server' : self.event_relay.name,
                                                'host' : self.erc.host })
                self.sent_event_relay_alarm = 1

    # check if the new information identifies bad writes as specified in the
    # inquisitors' configuration file element 'node_write_check'
    def check_for_bad_writes(self, server):
	node_write_check = self.inquisitor.node_write_check
	if not node_write_check:
	    # no nodes listed to check
	    return
	# if the new information is for a mover and the mover status is not
	# bad, then we are done.
	if enstore_functions2.is_mover(server.name):
	    if server.server_status not in MOVER_ERROR_STATES:
		# there is no problem
		return
	    else:
		# make a list of the library managers that we will have to
		# check.
		lm = self.get_server(server.library)
		if lm:
		    Trace.trace(enstore_constants.INQWORKDBG,
				"CBW: bad mover %s with lm %s"%(server.name, lm.name))
		else:
		    # we do not have information on this lm yet.
		    return
	elif enstore_functions2.is_library_manager(server.name):
	    lm = server
	    Trace.trace(enstore_constants.INQWORKDBG,
			"CBW: lm %s"%(lm.name))
	# now check the library managers' queue to make sure that -
	#
	#      o the file family limit has not been reached for writes/reads
	#           coming from the specified nodes
	#
	node_d = make_node_d(node_write_check)
	node_d_keys = node_d.keys()
	# first check the state of the lib man.  if it is in a bad state, just
	# return, the enstore ball will be red already and we do not really
	# know what is happening anyway.
	if lm.server_status in [e_errors.BROKEN]:
	    Trace.trace(enstore_constants.INQWORKDBG,
			"CBW: lm in bad state (%s - %s), no checks done"%(lm.name,
									  lm.server_status))
	    return
	# now parse the lm write wam queue and pull out any queue elements
	# which have a mover in a bad state and match the criteria specified
	# in the configuration file (currently only a node list is supported)
	bad_movers = {}
	if lm.wam_queue:
	    for qelem in lm.wam_queue:
		# remove any '.fnal.gov' from the node name
		node = enstore_functions2.strip_node(qelem['wrapper']['machine'][1])
		vc = qelem['vc']
		mover = self.get_server(qelem[enstore_constants.MOVER])
		if mover and mover.server_status in MOVER_ERROR_STATES and \
		   node in node_d_keys:
		    # if this is a read, we will ignore it unless the tape could
		    # be written to, i.e. not full and not read-only
		    if qelem['work'] == 'read_from_hsm':
			if enstore_functions2.is_readonly_state(vc['system_inhibit'][1]) \
                           or enstore_functions2.is_readonly_state(vc['user_inhibit'][1]):
			    continue
		    ff = vc.get('file_family',
				volume_family.extract_file_family(vc.get('volume_family',
									 "")))
		    key = "%s-%s"%(node, ff)
		    if not bad_movers.has_key(key):
			bad_movers[key] = [[vc.get('file_family_width', None), node, ff]]
		    Trace.trace(enstore_constants.INQWORKDBG,
				"CBW: found a bad mover in check %s (%s %s)"%(mover.name,
									      node, ff))
		    bad_movers[key].append(mover)
	# now see if the number of bad node-ff combinations
	# is > the file_family_width
	bad_mover_keys = bad_movers.keys()
	for key in bad_mover_keys:
	    # subtrace 1 for the first element which is not a bad_mover but
	    # file_family information
	    n = len(bad_movers[key]) - 1
	    [ffw, node, ff] = bad_movers[key][0]
	    if n >= ffw:
		# we have exceeded the file_family width, check to see how long
		# this problem has been occurring.  if longer than the specified
		# time do the specified actions
		Trace.trace(enstore_constants.INQWORKDBG,
			    "CBW: exceeded file family width (%s) for %s on %s"%(ffw,
										 ff, node))
		time_bad = node_d[node][1]
		now = time.time()
		if lm.time_bad == 0:
		    # this is the first time things are seen to be bad
		    lm.time_bad = now
		Trace.trace(enstore_constants.INQWORKDBG,
			    "CBW: check if do actions (%s) : now - time_bad = %s"%(time_bad,
									   now - lm.time_bad))
		if now - lm.time_bad >= time_bad:
		    action_l = node_d[node][0]
		    for action in action_l:
			if action == enstore_constants.ALARM:
			    Trace.trace(enstore_constants.INQWORKDBG,
					"CBW: action is raise an alarm")
			    # raise an alarm
			    mover_info = ""
			    for mover in bad_movers[key][1:]:
				mover_info = "%s-- %s (%s) "%(mover_info, mover.name,
							      mover.server_status)
			    Trace.alarm(e_errors.ERROR,"file family %s, from node %s has exceeded the file_family_width of %s %s"%(ff, node, ffw, mover_info))
			if action == enstore_constants.RED:
			    Trace.trace(enstore_constants.INQWORKDBG,
					"CBW: action is set ball red")
			    # set the lm ball to red, this will set the enstore ball to red
			    self.update_schedule_file({"work":"override",
						       "servers":lm.name,
						       "saagStatus":"red"}, OVERRIDE)

    def server_is_alive(self, name, aTime=None):
        if not aTime:
            now = time.time()
        else:
            now = aTime
        server = self.server_d.get(name, None)
        if server and server.host:
            self.serverfile.output_alive(server.host, ALIVE, now, name)
            self.new_server_status = 1
            server.is_alive()
            server.did_restart_alarm = 0
        return server

    def get_server_info(self, servers_just_done, success_servers):
        # return a list of servers that we have not processed this time around
        self.er_lock.acquire()
        servers = self.server_er_msg.keys()
        self.er_lock.release()
        # most times delays are introduced by library managers taking longer as
        # they have alot to do.  so process any lms first so that the latest alive
        # time can be set for the other servers/movers.
        lms = []
        for aServer in servers:
            if enstore_functions2.is_library_manager(aServer):
                lms.append(aServer)
                servers.remove(aServer)
        for aServer in lms+servers:
            if aServer not in servers_just_done:
                servers_just_done.append(aServer)
            else:
                # we did this one this time through already.  do not do it again
                continue
            if self.server_d.has_key(aServer):
                self.er_lock.acquire()
                server_time = self.server_er_msg[aServer]
                self.er_lock.release()
                server = self.server_is_alive(aServer, server_time)
                success_servers.append(aServer)
                # if server is a mover, we need to get some extra status
                if enstore_functions2.is_mover(aServer):
                    rtn = self.update_mover(server)
                    self.check_for_bad_writes(server)
                    # mark the mover alive again as we just got info and it may
                    # have taken awhile to get it.  only mark it alive if there was
                    # no problem getting the status
                    if rtn:
                        server = self.server_is_alive(aServer)
                    else:
                        # do not report it as success
                        success_servers.remove(aServer)


                if enstore_functions2.is_migrator(aServer):
                    rtn = self.update_migrator(server)
                    #self.check_for_bad_writes(server)
                    # mark the mover alive again as we just got info and it may
                    # have taken awhile to get it.  only mark it alive if there was
                    # no problem getting the status
                    if rtn:
                        server = self.server_is_alive(aServer)
                    else:
                        # do not report it as success
                        success_servers.remove(aServer)




                        
                # if server is a library_manager, we need to get some extra status
                if enstore_functions2.is_library_manager(aServer):
                    rtn = self.update_library_manager(server)
                    self.check_for_bad_writes(server)
                    # mark the lm alive again as we just got info and it may
                    # have taken awhile to get it.  only mark it alive if there was
                    # no problem getting the status
                    if rtn:
                        server = self.server_is_alive(aServer)
                    else:
                        # do not report it as success
                        success_servers.remove(aServer)
        return servers_just_done, success_servers

    # this is the routine that is called when a message arrives from the event
    # relay.  this is what this routine does:
    #     read and decode the message from the passed in descriptor
    #     update the internal information based on the info in the message
    #     if this message is from a mover - send a message to the mover for a
    #                                       more detailed status
    #     if this message is from a library_manager - send a message to the
    #                                       lib_man for it's state, suspect
    #                                       volume list and queue list.
    def process_event_message(self, reason_called=TIMEOUT):
        # the event relay is alive
        now = time.time()
        #self.serverfile.output_alive(self.erc.event_relay_addr[0],
        #                             ALIVE, now, enstore_constants.EVENT_RELAY)
        # if this is the first time alive after the event relay was thought
	# to be dead then we must adjust the last alive times for all of
	# the servers,  otherwise we will think the server is dead immediately
	# and not allow any time to receive the alive message from it.
        #if not self.event_relay.is_alive():
        #    for server in self.server_d.keys():
        #        self.server_d[server].last_alive = now
        #
        # if many servers/movers time out here, this may take along time.  the first
        # servers processed will have a heartbeat time that is too long and will be
        # marked as connot update status.  return a list of server heartbeats rcvd
        # that have no error if further status is obtained.  this can be used to not
        # mark these down. so return success_servers
        self.sent_event_relay_alarm = 0
        servers_just_done = []
        success_servers = []
        # ignore messages that originated with us
        if self.er_alive_event.isSet():
            self.serverfile.output_alive(self.erc.event_relay_addr[0],
                                         ALIVE, now, enstore_constants.EVENT_RELAY)
            self.event_relay.alive(now)
            self.er_alive_event.clear()
            enstore_functions.inqTrace(enstore_constants.INQEVTMSGDBG,
                  "processing event relay message type alive")
            # check if this is one of the servers we are watching. we need to process
            # all of the messages that come in while we are getting other server
            # information too.
            start_time = 0.0
            # process all the received messages that are received during processing of
            # other messages.  we do not want to get stuck here forever.  do this at max
            # 3 times .  it would be better to put all of the getting of mover and
            # library manager information into a separate thread, but there are many
            # places to make thread safe.  this may be an easier solution.
            iterations = 0
            while (time.time() - start_time > 10) and iterations < 3:
                i = time.time() - start_time
                enstore_functions.inqTrace(enstore_constants.INQEVTMSGDBG,
                                           "periodic timeout iterations = %s, now-start= %s"%(iterations, i))
                start_time = time.time()
                servers_just_done, success_servers = self.get_server_info(servers_just_done, success_servers)
                iterations = iterations + 1
            else:
                # delete the done servers from the list of requests
                self.er_lock.acquire()
                for server in servers_just_done:
                    del self.server_er_msg[server]
                self.er_lock.release()

            # we may have taken awhile so mark the event relay as alive again
            now_after = time.time()
            self.serverfile.output_alive(self.erc.event_relay_addr[0],
                                         ALIVE, now_after, enstore_constants.EVENT_RELAY)
            self.event_relay.alive(now_after)

        if self.er_config_event.isSet():
            self.serverfile.output_alive(self.erc.event_relay_addr[0],
                                         ALIVE, now, enstore_constants.EVENT_RELAY)
            self.event_relay.alive(now)
            enstore_functions.inqTrace(enstore_constants.INQEVTMSGDBG,
                                       "processing event relay message type NEW CONFIG")
            # a new config file was loaded into the config server, get it
            self.er_config_event.clear()
            self.make_config_html_file()
        return success_servers

    # these are the things we do periodically, this routine is called when an
    # interval is up, from within dispatching_worker  -
    #      o generate a new log file page
    #      o generate a new encp history page
    #      o process any servers that have not reported in recently
    #      o generate a new server status web page
    def periodic_tasks(self, reason_called=TIMEOUT):
        enstore_functions.inqTrace(enstore_constants.INQEVTMSGDBG,
				   "periodic timeout")
        # check for all received event messages
        msgs_rcvd = self.process_event_message(reason_called)
        # just output the inquisitor alive info, if we are doing this, we are alive.
        self.server_is_alive(self.inquisitor.name)
        # see if there are any servers in cardiac arrest (no heartbeat)
        enstore_functions.inqTrace(enstore_constants.INQEVTMSGDBG,
				   "periodic timeout - check last alive")
        self.check_last_alive(msgs_rcvd)
        # check if we have received an event relay message recently
        enstore_functions.inqTrace(enstore_constants.INQEVTMSGDBG,
				   "periodic timeout - check event relay alive")
        self.check_event_relay_last_alive()
        enstore_functions.inqTrace(enstore_constants.INQEVTMSGDBG,
				   "periodic timeout - write server status file")
        self.write_server_status_file()
        enstore_functions.inqTrace(enstore_constants.INQEVTMSGDBG,
				   "periodic timeout - end")
        self.last_time_for_periodic_tasks = time.time()
        self.reset_interval_timer(self.periodic_tasks)

    def encp_periodic_tasks(self, reason_called=TIMEOUT):
        # the encp web page is updated when notice of an encp transfer is received, but
        # not more than 1/ update interval.  thus if notice of 1 transfer arrives, the page is
        # updated and then another notice comes almost immediately, the page will not have
        # been updated to include this one. so here we
        # see if there has been notice of an encp transfer which happened too soon after
        # a previous transfer to trigger the web page to be updated.
        # if so then we must hand trigger the web page update
        enstore_functions.inqTrace(enstore_constants.INQEVTMSGDBG,
				   "encp periodic timeout")
        now = time.time()
        if self.er_encp_event.isSet():
            self.serverfile.output_alive(self.erc.event_relay_addr[0],
                                         ALIVE, now, enstore_constants.EVENT_RELAY)
            self.event_relay.alive(now)
            enstore_functions.inqTrace(enstore_constants.INQEVTMSGDBG,
                                       "processing event relay message type ENCP XFER")
            self.er_encp_event.clear()
            # an encp xfer completed - update the encp history page, but only do
            # this at most once per minute
            if now - self.last_encp_update > ENCP_UPDATE_INTERVAL:
                # it has been more than update interval since the last update, so do it.
                self.make_encp_html_file(now)
                # do not clear the event unless we update the web page.  otherwise we
                # could miss doing an update.
            #else:
                # record the fact that we got an encp transfer notice, but it was too
                # soon to do another update of the html file.  we will check this
                # later to do the update if no other encp xfer comes thru to trigger
                # an update.
                #self.encp_xfer_but_no_update = now
        #if self.encp_xfer_but_no_update:
        #    now = time.time()
        #    if now - self.encp_xfer_but_no_update > ENCP_UPDATE_INTERVAL:
        #        self.make_encp_html_file(now)
        self.reset_interval_timer(self.encp_periodic_tasks)

    def log_periodic_tasks(self, reason_called=TIMEOUT):
        # update the web page that lists all the current log files
        enstore_functions.inqTrace(enstore_constants.INQEVTMSGDBG,
				   "log periodic timeout")
        self.make_log_html_file()
        self.reset_interval_timer(self.log_periodic_tasks)

    def time_for_override_mail(self, now, element):
	if not self.override_mail_sent.has_key(element):
	    # mail was never sent
	    return 1
	elif now - self.override_mail_sent[element] > DEFAULT_OVERRIDE_INTERVAL:
	    # mail was sent a long time ago, send it again
	    return 1
	else:
	    # not time to send mail yet
	    return 0

    def saag_periodic_tasks(self, reason_called=TIMEOUT):
	# see if any enstore element has been overridden for longer than specified
	# in the configuration file
        enstore_functions.inqTrace(enstore_constants.INQEVTMSGDBG,
				   "saag periodic timeout")
	interval = self.inquisitor.override_interval
	if interval is None:
	    interval = DEFAULT_OVERRIDE_INTERVAL
	now = time.time()
        (sfile, outage_d, offline_d, override_d)=(None,None,None,None)
        self.rw_lock.acquire_read()
        try:
            sfile, outage_d, offline_d, override_d = enstore_files.read_schedule_file(self.html_dir)
        finally:
            self.rw_lock.release_read()
	elements = override_d.keys()
	for element in elements:
	    elist = override_d[element]
	    if type(elist) == types.ListType:
		origin_date = elist[1]
		if now - origin_date > interval and self.time_for_override_mail(now, element):
		    subject = "%s overridden too long"%(element,)
		    msg = "The saag page element %s has been overridden to %s for %.0d seconds"%(element,
									           elist[0],
										   now - elist[1])
		    enstore_mail.send_mail(MY_NAME, msg, subject)
		    self.override_mail_sent[element] = now
        self.reset_interval_timer(self.saag_periodic_tasks)

    # our client said to update the enstore system status information
    def update(self, ticket):
        self.periodic_tasks(USER)
        ticket["status"] = (e_errors.OK, None)
        self.send_reply(ticket)
        enstore_functions.inqTrace(enstore_constants.INQWORKDBG,
				   "update work from user")

    def do_dump(self):
        keys = self.server_d.keys()
        keys.sort()
        for skey in keys:
            server = self.server_d[skey]
            print repr(server)
        print repr(self.event_relay)
        print ""
        import pprint
        pprint.pprint(self.serverfile.text)
        print ""

    # spill our guts
    def dump(self, ticket):
        ticket["status"] = (e_errors.OK, None)
        self.do_dump()
        self.send_reply(ticket)
        enstore_functions.inqTrace(enstore_constants.INQWORKDBG,
				   "dump work from user")

    # set the timeout for the periodic_tasks function
    def set_update_interval(self,ticket):
        ticket["status"] = (e_errors.OK, None)
        self.update_interval = ticket["update_interval"]
        self.remove_interval_func(self.periodic_tasks)
        self.add_interval_func(self.periodic_tasks, self.update_interval)
        self.send_reply(ticket)
        enstore_functions.inqTrace(enstore_constants.INQWORKDBG,
				   "update_interval work from user")

    def get_update_interval(self, ticket):
        __pychecker__ = "unusednames=ticket"
        ret_ticket = { 'update_interval' : self.update_interval,
                       'status'      : (e_errors.OK, None) }
        self.send_reply(ret_ticket)
        enstore_functions.inqTrace(enstore_constants.INQWORKDBG,
				   "get_update_interval work from user")

    # update the inq status and exit
    def update_and_exit(self, ticket):
        ticket["status"] = (e_errors.OK, None)
        self.send_reply(ticket)
        self.update_exit(0)
        enstore_functions.inqTrace(enstore_constants.INQWORKDBG,
				   "update_and_exit work from user")

    # set a new refresh value for the html files
    def set_refresh(self, ticket):
        ticket["status"] = (e_errors.OK, None)
        self.serverfile.set_refresh(ticket['refresh'])
        self.encpfile.set_refresh(ticket['refresh'])
        self.send_reply(ticket)
        enstore_functions.inqTrace(enstore_constants.INQWORKDBG,
				   "set_refresh work from user")

    # return the current refresh value
    def get_refresh(self, ticket):
        ticket["status"] = (e_errors.OK, None)
        ticket["refresh"] = self.serverfile.get_refresh()
        self.send_reply(ticket)
        enstore_functions.inqTrace(enstore_constants.INQWORKDBG,
				   "get_refresh work from user")

    # set a new max encp lines displayed value
    def set_max_encp_lines(self, ticket):
        ticket["status"] = (e_errors.OK, None)
        self.max_encp_lines = ticket['max_encp_lines']
        self.send_reply(ticket)
        enstore_functions.inqTrace(enstore_constants.INQWORKDBG,
				   "set_max_encp_lines work from user")

    # return the current number of displayed encp lines
    def get_max_encp_lines(self, ticket):
        ticket["status"] = (e_errors.OK, None)
        ticket["max_encp_lines"] = self.max_encp_lines
        self.send_reply(ticket)
        enstore_functions.inqTrace(enstore_constants.INQWORKDBG,
				   "get_max_encp_lines work from user")

    # resubscribe to the event relay
    def resubscribe(self, reason_called=TIMEOUT):
        self.er_subscribe_event.set()
        enstore_functions.inqTrace(enstore_constants.INQWORKDBG,
				   "event relay resubscribe work")
        self.reset_interval_timer(self.resubscribe)

    # subscribe to the event relay
    def subscribe(self, ticket):
        ticket["status"] = (e_errors.OK, None)
        self.er_subscribe_event.set()
        self.send_reply(ticket)
        enstore_functions.inqTrace(enstore_constants.INQWORKDBG,
				   "event relay subscribe work from user")

    # the following routines manage the file enstore_outage.py in the web area
    def find_server_match(self, text):
	total_matches = 0
	matched_server = None
	pattern = "^%s"%(text,)
	for server in server_keys:
	    match = re.match(pattern, server, re.I)
	    if not match is None:
		total_matches = total_matches + 1
		matched_server = server
	return (total_matches, matched_server)

    def is_valid(self, server):
	# look and see if this server is listed in the config file
	if self.config_d.has_key(server):
	    # this is a valid server
	    return 1
	else:
	    # this server was not listed in the config file
	    # see if it is one of the media tags
	    media = enstore_functions.get_media()
	    keys = media.keys()
	    for key in keys:
		if server == media[key]:
		    return 1
	    else:
                # see if this flag is in the other_saag_links section
                if self.config_d.has_key("other_saag_links") and \
                   self.config_d["other_saag_links"].has_key(server):
                    return 1
		return 0

    def update_schedule_file(self, ticket, func):
	ticket["status"] = (e_errors.OK, None)
	bad_servers = []
	server_l = string.split(ticket["servers"], ',')
        (sfile, outage_d, offline_d, override_d) = (None,None,None,None)
        self.rw_lock.acquire_read()
        try:
            sfile, outage_d, offline_d, override_d = enstore_files.read_schedule_file(self.html_dir)
        finally:
            self.rw_lock.release_read()
	if (sfile.opened != 0) or (sfile.exists() == 0):
	    for key in server_l:
		# map the entered name to the name in the outage dictionary
		num, server = self.find_server_match(key)
		if num == 1:
		    key = server_map[server]
		elif not self.is_valid(key):
		    # we could not find this server amongst the existing
                    # ones, maybe it is an old one.  allow it to be marked
                    # UP or NOOVERRIDE or NOOUTAGE as these mean removing
                    # the name from the corresponding dictionary.
		    if func not in [UP, NOOVERRIDE, NOOUTAGE]:
			key = None
			error = e_errors.DOESNOTEXIST
		    else:
			error = e_errors.DOESNOTEXISTSTILLDONE
		    bad_servers.append(key)
		    ticket["status"] = (error, bad_servers)

		if key is not None:
		    # we found a match
		    if func == UP:
			delkey(key, offline_d)
                        enstore_mail.send_mail(MY_NAME,
                                                     "REASON: %s marked up"%(key,),
                                                     "%s marked up"%(key,))
		    elif func == DOWN:
			offline_d[key] = ticket["time"]
                        enstore_mail.send_mail(MY_NAME,
                                                     "REASON: %s"%(ticket.get("time", "None"),),
                                                     "%s marked down"%(key,))
                    elif func == OUTAGE:
			outage_d[key] = ticket["time"]
		    elif func == NOOUTAGE:
			delkey(key, outage_d)
		    elif func == OVERRIDE:
			reason = ticket.get("reason", "<no given reason>")
			override_d[key] = [ticket["saagStatus"], time.time(), reason]
		    elif func == NOOVERRIDE:
			delkey(key, override_d)
            rc=0
            self.rw_lock.acquire_write()
            try:
                rc=sfile.write(outage_d, offline_d, override_d)
            finally:
                self.rw_lock.release_write()
	    if not rc:
		ticket["status"] = (e_errors.IOERROR, None)
		Trace.log(e_errors.ERROR,
			  "Could not write to file %s/%s"%(self.html_dir,
							 enstore_constants.OUTAGEFILE))

	else:
	    # file was not opened, maybe a problem with the lock file
	    ticket["status"] = (e_errors.IOERROR, None)
	    Trace.log(e_errors.ERROR,
		      "Could not read file %s/%s"%(self.html_dir,
						   enstore_constants.OUTAGEFILE))

    # return a dict of the last alive time for all requested servers.  if we are not
    # monitoring the server or have no last time, return nothing.
    def get_last_heartbeat(self, ticket):
        ticket["status"] = (e_errors.OK, None)
        # the list of servers may be a string or a list.
        if type(ticket["servers"]) == types.StringType:
            server_l = string.split(ticket["servers"], ',')
        else:
            server_l = ticket["servers"]
        last_time = {}
        for server in server_l:
            if self.server_d.has_key(server):
                heartbeat_time = self.server_d[server].last_heartbeat()
                # we may not be watching this server, do not return a time if this is so
                if heartbeat_time:
                    last_time[server] = heartbeat_time
        ticket['servers'] = last_time
        return last_time

    # return the last time the server heartbeat was received
    def get_last_alive(self, ticket):
        last_time = self.get_last_heartbeat(ticket)
        self.send_reply(ticket)
        enstore_functions.inqTrace(enstore_constants.INQWORKDBG,
				   "return last time server was alive (%s)"%(last_time,))


    def up(self, ticket):
	self.update_schedule_file(ticket, UP)
        self.send_reply(ticket)
        enstore_functions.inqTrace(enstore_constants.INQWORKDBG,
				   "mark server up work from user")

    def down(self, ticket):
	self.update_schedule_file(ticket, DOWN)
        self.send_reply(ticket)
        enstore_functions.inqTrace(enstore_constants.INQWORKDBG,
				   "mark server down work from user")

    def outage(self, ticket):
	self.update_schedule_file(ticket, OUTAGE)
        self.send_reply(ticket)
        enstore_functions.inqTrace(enstore_constants.INQWORKDBG,
				   "mark server outage work from user")

    def nooutage(self, ticket):
	self.update_schedule_file(ticket, NOOUTAGE)
        self.send_reply(ticket)
        enstore_functions.inqTrace(enstore_constants.INQWORKDBG,
				   "mark server nooutage work from user")

    def override(self, ticket):
	self.update_schedule_file(ticket, OVERRIDE)
        self.send_reply(ticket)
        Trace.trace(enstore_constants.INQWORKDBG,
		    "mark server override work from user")

    def nooverride(self, ticket):
	self.update_schedule_file(ticket, NOOVERRIDE)
        self.send_reply(ticket)
        Trace.trace(enstore_constants.INQWORKDBG,
		    "mark server nooverride work from user")

    def thread_wrapper(self, function, args=(), after_function=None):
        t = time.time()
        function(args)
        if after_function:
            after_function()
        Trace.trace(5,"process_request: function %s time %s"%(function.__name__,time.time()-t))

    #
    # spawn function in new thread of there is not alive thread with this name
    #
    def run_in_thread(self,
                      function,
                      args=(),
                      after_function=None,
                      thread_name=None):
        if thread_name :
            for thread in threading.enumerate():
                if thread.getName() == thread_name:
                    if thread.isAlive():
                        Trace.trace(e_errors.WARNING, "thread %s is already runnnig, skipping execution of %s" % (thread_name, function.__name__,))
                        return
        _args = (function,)+ args
        if after_function:
            _args = _args + (after_function,)
        enstore_functions.inqTrace(enstore_constants.INQTHREADDBG,
                                   "create thread: name %s target %s args %s" % (thread_name, function.__name__, args))
        thread = threading.Thread(group=None, target=self.thread_wrapper,
                                  args=_args, kwargs={})
        if thread_name:
            thread.setName(thread_name)
        enstore_functions.inqTrace(enstore_constants.INQTHREADDBG,
                                   "starting thread name=%s"%(thread.getName()))
        try:
            thread.start()
        except:
            exc, detail, tb = sys.exc_info()
            Trace.log(e_errors.ERROR, "starting thread: %s" % (detail))

    def do_one_request(self):
        #
        # this function overrides base class's function implementing
        # execution of interval functions in threads
        # In the future this functionality will be provided by DispatchingWorker
        # so we will no longer need to override it
        #
        request = None
        try:
            request, client_address = self.get_request()
        except:
            exc, msg = sys.exc_info()[:2]

        now=time.time()

        for func, time_data in self.interval_funcs.items():
            interval, last_called, one_shot = time_data
            if now - last_called > interval:
                if one_shot:
                    del self.interval_funcs[func]
                else: #record last call time
                    self.interval_funcs[func][1] =  now
                Trace.trace(6, "do_one_request: calling interval_function %s"%(func.__name__,))
                self.run_in_thread(func, (), after_function=self._done_cleanup, thread_name="%s.%s"%(self.__class__.__name__,func.__name__))
                #func()

        if request is None: #Invalid request sent in
            return

        if request == '':
            # nothing returned, must be timeout
            self.handle_timeout()
            return
        try:
            self.process_request(request, client_address)
        except KeyboardInterrupt:
            traceback.print_exc()
        except SystemExit, code:
            # processing may fork (forked process will call exit)
            sys.exit( code )
        except:
            self.handle_error(request, client_address)


    def process_request(self, request, client_address):
        #
        # this function overrides base class's function implementing
        # execution of interval functions in threads
        # In the future this functionality will be provided by DispatchingWorker
        # so we will no longer need to override it
        #
        ticket = udp_server.UDPServer.process_request(self, request,
                                                      client_address)
        Trace.trace(6, "inquisitor:process_request %s; %s"%(request, ticket,))
        if not ticket:
            return
        try:
            function_name = ticket["work"]
        except (KeyError, AttributeError, TypeError), detail:
            ticket = {'status' : (e_errors.KEYERROR,
                                  "cannot find any named function")}
            msg = "%s process_request %s from %s" % \
                (detail, ticket, client_address)
            Trace.trace(6, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return
        try:
            Trace.trace(5,"process_request: function %s"%(function_name,))
            function = getattr(self,function_name)
        except (KeyError, AttributeError, TypeError), detail:
            ticket = {'status' : (e_errors.KEYERROR,
                                  "cannot find requested function `%s'"
                                  % (function_name,))}
            msg = "%s process_request %s %s from %s" % \
                (detail, ticket, function_name, client_address)
            Trace.trace(6, msg)
            Trace.log(e_errors.ERROR, msg)
            self.reply_to_caller(ticket)
            return
        # call the user function
	c = threading.activeCount()
	Trace.trace(5, "threads %s"%(c,))
	if c < self.max_threads:
	    Trace.trace(5, "threads %s"%(c,))
	    self.run_in_thread(function, (ticket,), after_function=self._done_cleanup)
	else:
            t = time.time()
	    function(ticket)
            Trace.trace(5,"process_request: function %s time %s"%(function_name,time.time()-t))

    def show(self, ticket):
	ticket["status"] = (e_errors.OK, None)
        (sfile, outage_d, offline_d, override_d)=(None,None,None,None)
        self.rw_lock.acquire_read()
        try:
            sfile, outage_d, offline_d, override_d = enstore_files.read_schedule_file(self.html_dir)
        finally:
            self.rw_lock.release_read()
        (dfile, seen_down_d) = (None,None)
        self.rw_lock.acquire_read()
        try:
            dfile, seen_down_d = enstore_files.read_seen_down_file(self.html_dir)
        finally:
            self.rw_lock.release_read()
	if sfile and ((sfile.opened != 0) or (sfile.exists() == 0)):
	    ticket["outage"] = outage_d
	    ticket["offline"] = offline_d
	    ticket["override"] = override_d
	else:
	    ticket["status"] = (e_errors.IOERROR, None)
	    Trace.log(e_errors.ERROR,
		      "Could not read file %s/%s"%(self.html_dir,
						   enstore_constants.OUTAGEFILE))
	if dfile and ((dfile.opened != 0) or (dfile.exists() == 0)):
	    ticket["seen_down"] = seen_down_d
        else:
	    ticket["status"] = (e_errors.IOERROR, None)
	    Trace.log(e_errors.ERROR,
		      "Could not read file %s/%s"%(self.html_dir,
						   enstore_constants.SEENDOWNFILE))
        self.send_reply(ticket)
        enstore_functions.inqTrace(enstore_constants.INQWORKDBG,
				   "show up/down status work from user")

    # this is the thread that reads the messages sent from the event relay and adds
    # them to the queue
    def read_er_messages(self):
        # setup the communications with the event relay task
        self.erc.no_select_fd()
        self.erc.no_interval()
        self.erc.start([event_relay_messages.ALIVE,
                        event_relay_messages.NEWCONFIGFILE,
                        event_relay_messages.ENCPXFER], self.resubscribe_rate)

        # start our heartbeat to the event relay process
        self.erc.start_heartbeat(enstore_constants.INQUISITOR,
                                 self.inquisitor.alive_interval)
        rcv_timeout = 5
        self.our_heartbeat = time.time()
        while 1:
            try:
                # read messages from the udp socket connected to the event_relay
                #enstore_functions.inqTrace(enstore_constants.INQERTHREAD,
                #                           "thread calling select")
                ##r, w, x, remaining_time = cleanUDP.Select(r, w, r+w, rcv_timeout)
                r, w, x = select.select([self.erc.sock,], [], [self.erc.sock,], rcv_timeout)
                #enstore_functions.inqTrace(enstore_constants.INQERTHREAD,
                #                           "thread  %s %s %s"%(r, w, x))
                # check if we have been signalled to exit
		if self.exit_now_event.isSet():
                    self.erc.unsubscribe()
		    enstore_functions.inqTrace(enstore_constants.INQERTHREAD,
					       "Exiting er read thread")
		    return
                if self.er_subscribe_event.isSet():
		    enstore_functions.inqTrace(enstore_constants.INQERTHREAD,
					       "Resubscribing to event relay")
                    self.erc.subscribe()
                    self.er_subscribe_event.clear()
                # see if we should send our heartbeat
                now = time.time()
                if now - self.our_heartbeat > self.erc.heartbeat_interval:
                    self.erc.heartbeat()
                    self.our_heartbeat = now
                if not r + w:
                    # timeout
                    continue
                # read and process the message
                msg = enstore_erc_functions.read_erc(self.erc)
                if msg:
                    if msg.type == event_relay_messages.ALIVE and \
                           not msg.server == self.inquisitor.name:
                        enstore_functions.inqTrace(enstore_constants.INQERTHREAD,
                                       "received event relay message type %s (%s)"%(msg.type,
                                                                                    msg.server))
                        #enstore_functions.inqTrace(enstore_constants.INQERTHREAD,
                        #                           "thread acquiring lock")
                        self.er_lock.acquire()
                        if self.server_d.has_key(msg.server):
                            self.server_er_msg[msg.server] = time.time()
                        #enstore_functions.inqTrace(enstore_constants.INQERTHREAD,
                        #                           "thread releasing lock")
                        self.er_lock.release()
                        self.er_alive_event.set()
                    elif msg.type == event_relay_messages.NEWCONFIGFILE:
                        enstore_functions.inqTrace(enstore_constants.INQERTHREAD,
                                             "received event relay message type %s"%(msg.type,))
                        self.er_config_event.set()
                    elif msg.type == event_relay_messages.ENCPXFER:
                        enstore_functions.inqTrace(enstore_constants.INQERTHREAD,
                                             "received event relay message type %s"%(msg.type,))
                        self.er_encp_event.set()
            except:
                # this is the event relay msg read thread.  catch anything, report it
		# and carry on.
		Trace.handle_error()
                # we may be here because of an error while quitting, check again
		if self.exit_now_event.isSet():
                    return
                #
                # Dmitry commented out the lines below so we do not see alarms in alarm page
                #
		#self.serve_forever_error(self.log_name+"ERRT")


class Inquisitor(InquisitorMethods, generic_server.GenericServer):

    def __init__(self, csc, html_file="", update_interval=NOVALUE, alive_rcv_to=NOVALUE,
                 alive_retries=NOVALUE, max_encp_lines=NOVALUE, refresh=NOVALUE, max_threads=NOVALUE):
	global server_map
	InquisitorMethods.__init__(self)
        generic_server.GenericServer.__init__(self, csc, MY_NAME)
        Trace.init(self.log_name)
        self.alarmc.rcv_timeout = 15
        self.alarmc.rcv_tries = 1
        # we do not want the register event relay with dispatching worker
        self.resubscribe_rate = 300
        self.startup_state = e_errors.OK
        self.last_time_for_periodic_tasks = time.time()

        # set an interval and retry that we will use the first time to get the
        # config information from the config server.  we do not use the
        # passed values because they might have been defaulted and we need to
        # look them up in the config file which we have not gotten yet.
        use_once_timeout = 5
        use_once_retry = 1
        config_d = self.csc.dump(use_once_timeout, use_once_retry)
        if e_errors.is_timedout(config_d):
            enstore_functions.inqTrace(enstore_constants.INQERRORDBG,
		     "inquisitor init - ERROR, getting config dict timed out")
            self.startup_state = e_errors.TIMEDOUT
            self.startup_text = enstore_constants.CONFIG_SERVER
            return
        self.config_d = config_d['dump']
        # these are the servers we will be monitoring, always get the inquisitor first
        self.inquisitor = monitored_server.MonitoredInquisitor(\
                                   self.config_d.get(enstore_constants.INQUISITOR, {}))

        self.server_d = {enstore_constants.INQUISITOR : self.inquisitor}
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
        # get max thread count
        self.max_threads = self.get_value('max_threads', max_threads)

        # get the keys that are associated with the web information
        self.www_server = self.config_d.get(enstore_constants.WWW_SERVER, {})
	server_map["media"] = self.www_server.get(www_server.MEDIA_TAG,
						  www_server.MEDIA_TAG_DEFAULT)

	# get the thresholds which determine when we need an extra web page or two
	self.page_thresholds = self.get_value(enstore_constants.PAGE_THRESHOLDS, NOVALUE)

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
            else:
                self.html_dir = enstore_files.default_dir
                html_file = enstore_files.default_status_html_file()
                encp_file = enstore_files.default_encp_html_file()
                config_file = enstore_files.default_config_html_file()

        # if no html refresh was entered on the command line, get it from
        # the configuration file.
        if refresh == NOVALUE:
            refresh = self.inquisitor.refresh
            if not refresh:
                refresh = DEFAULT_REFRESH

        self.system_tag = self.www_server.get(www_server.SYSTEM_TAG,
                                              www_server.SYSTEM_TAG_DEFAULT)

        # these are the files to which we will write, they are html files
        self.serverfile = enstore_files.HTMLStatusFile(html_file, refresh,
						       self.system_tag,
						       self.page_thresholds)
        self.encpfile = enstore_files.HTMLEncpStatusFile(encp_file, refresh,
                                                         self.system_tag)
        self.logfile = enstore_files.HTMLLogFile(self.logc.log_dir,
                                                 LOGHTMLFILE_NAME,
                                                 self.html_dir, self.system_tag)
        self.configfile = enstore_files.HTMLConfigFile(config_file,
                                                       self.system_tag)

        cdict = self.config_d.get(enstore_constants.ALARM_SERVER, {})
        self.alarm_server = monitored_server.MonitoredAlarmServer(cdict)
	self.servers_by_name[enstore_constants.ALARM_SERVER] = self.alarm_server

        cdict = self.config_d.get(enstore_constants.LOG_SERVER, {})
        self.log_server = monitored_server.MonitoredLogServer(cdict)
	self.servers_by_name[enstore_constants.LOG_SERVER] = self.log_server

        cdict = self.config_d.get(enstore_constants.FILE_CLERK, {})
        self.file_clerk = monitored_server.MonitoredFileClerk(cdict)
	self.servers_by_name[enstore_constants.FILE_CLERK] = self.file_clerk

        cdict = self.config_d.get(enstore_constants.VOLUME_CLERK, {})
        self.volume_clerk = monitored_server.MonitoredVolumeClerk(cdict)
	self.servers_by_name[enstore_constants.VOLUME_CLERK] = self.volume_clerk

        cdict = self.config_d.get(enstore_constants.INFO_SERVER, {})
        self.info_server = monitored_server.MonitoredInfoServer(cdict)
	self.servers_by_name[enstore_constants.INFO_SERVER] = self.info_server

        cdict = self.config_d.get(enstore_constants.CONFIG_SERVER, {})
        self.config_server = monitored_server.MonitoredConfigServer(cdict)
	self.servers_by_name[enstore_constants.CONFIG_SERVER] = self.config_server

        cdict = self.config_d.get(enstore_constants.RATEKEEPER, {})
        self.ratekeeper = monitored_server.MonitoredRatekeeper(cdict)
	self.servers_by_name[enstore_constants.RATEKEEPER] = self.ratekeeper

        cdict = self.config_d.get(enstore_constants.ACCOUNTING_SERVER, {})
        self.accounting_server = monitored_server.MonitoredAccountingServer(cdict)
	self.servers_by_name[enstore_constants.ACCOUNTING_SERVER] = self.accounting_server

        cdict = self.config_d.get(enstore_constants.DRIVESTAT_SERVER, {})
        self.drivestat_server = monitored_server.MonitoredDrivestatServer(cdict)
	self.servers_by_name[enstore_constants.DRIVESTAT_SERVER] = self.drivestat_server

        # Library Manager Director
        cdict = self.config_d.get(enstore_constants.LM_DIRECTOR, {})
        self.lm_director = monitored_server.MonitoredLMD(cdict)
	self.servers_by_name[enstore_constants.LM_DIRECTOR] = self.lm_director

        # Policy Engine Server and Migration Dispatcher
        cdict = self.config_d.get(enstore_constants.DISPATCHER, {})
        self.dispatcher = monitored_server.MonitoredDispatcher(cdict)
	self.servers_by_name[enstore_constants.DISPATCHER] = self.dispatcher

        for server_key in self.server_d.keys():
            server = self.server_d[server_key]
            server.hung_interval = self.inquisitor.get_hung_interval(server.name)

        for key in self.config_d.keys():
            self.add_new_server(key, self.config_d)

        dispatching_worker.DispatchingWorker.__init__(self,
                                                      (self.inquisitor.hostip,
                                                       self.inquisitor.port))

        # set up a signal handler to catch termination signals (SIGKILL) so we can
        # update our status before dying
        signal.signal(signal.SIGTERM, self.s_update_exit)

        # set an interval timer to periodically update the web pages
        self.add_interval_func(self.periodic_tasks, self.update_interval)
        self.add_interval_func(self.encp_periodic_tasks, ENCP_UPDATE_INTERVAL)
        self.add_interval_func(self.log_periodic_tasks, LOG_UPDATE_INTERVAL)
        self.add_interval_func(self.saag_periodic_tasks, OVERRIDE_UPDATE_INTERVAL)
        #self.add_interval_func(self.process_event_message, ER_UPDATE_INTERVAL)
        self.add_interval_func(self.resubscribe, self.resubscribe_rate)

        self.event_relay_msg = event_relay_messages.EventRelayAliveMsg(self.inquisitor.host,
                                                                       self.inquisitor.port)
        self.event_relay_msg.encode(self.inquisitor.name)

        # keep track of when we receive event relay messages.  maybe we can tell if
        # the event relay process goes down.
        self.event_relay = EventRelay()
        self.sent_event_relay_alarm = 0

        # setup the initial system page
        for server in self.server_d.keys():
            self.mark_no_info(self.server_d[server])
        self.mark_event_relay(NO_INFO_YET)

        # update the config page
        self.update_config_page(self.config_d)

        # update the encp page
        self.make_encp_html_file(time.time())

	# this event is used to tell the threads to exit
	self.exit_now_event = threading.Event()

	# start the thread that will do the file writing
	# sometimes it takes a very long time to output the server status file
	# especially if the library manager queue is very large.  so do this
	# work in a thread.
	self.serverfile_new = None
	self.new_server_status = 0
	self.server_status_file_event = threading.Event()
	self.server_status_file_thread = threading.Thread(group=None,
							  target=self.make_server_status_html_file,
							  name="MAKE_SERVER_STATUS_FILE",
							  args=())
	#self.server_status_file_thread.setDaemon(1)
	self.server_status_file_thread.start()

        # this is the thread that will read the event relay messages
        self.er_encp_event = threading.Event()
        self.er_config_event = threading.Event()
        self.er_alive_event = threading.Event()
        self.er_subscribe_event = threading.Event()
        self.er_thread = threading.Thread(group=None,
                                          target=self.read_er_messages,
                                          name="READ_ER_MESSAGES",
                                          args=())
        self.er_thread.start()


class InquisitorInterface(generic_server.GenericServerInterface):

    def __init__(self):
        # fill in the defaults for possible options
        self.html_file = ""
        self.alive_rcv_timeout = NOVALUE
        self.alive_retries = NOVALUE
        self.update_interval = NOVALUE
        self.max_encp_lines = NOVALUE
        self.refresh = NOVALUE
        generic_server.GenericServerInterface.__init__(self)

    inquisitor_options = {
        option.HTML_FILE:{option.HELP_STRING:"specifies the html file",
                          option.VALUE_TYPE:option.STRING,
                          option.VALUE_USAGE:option.REQUIRED,
                          option.USER_LEVEL:option.ADMIN,
                          },
        option.UPDATE_INTERVAL:{option.HELP_STRING:
                                "set the interval between updates of"
                                "the system server status web page",
                                option.VALUE_TYPE:option.INTEGER,
                                option.VALUE_USAGE:option.REQUIRED,
                                option.USER_LEVEL:option.ADMIN,
                          },
        option.MAX_ENCP_LINES:{option.HELP_STRING:"set the number of "
                               "displayed lines on the encp history web page",
                               option.VALUE_TYPE:option.INTEGER,
                               option.VALUE_USAGE:option.REQUIRED,
                               option.USER_LEVEL:option.ADMIN,
                               },
        option.REFRESH:{option.HELP_STRING:"set the refesh interval for "
                        "inquisitor creted web pages",
                        option.VALUE_TYPE:option.INTEGER,
                        option.VALUE_USAGE:option.REQUIRED,
                        option.USER_LEVEL:option.ADMIN,
                        },
        }

    def valid_dictionaries(self):
        return generic_server.GenericServerInterface.valid_dictionaries(self)+\
               (self.inquisitor_options, self.alive_rcv_options)


if __name__ == "__main__":
    Trace.init(string.upper(MY_NAME))
    enstore_functions.inqTrace(enstore_constants.INQSTARTDBG,
			       "inquisitor called with args %s"%(sys.argv,))

    # get interface
    intf = InquisitorInterface()
    # get the inquisitor
    inq = Inquisitor((intf.config_host, intf.config_port),
                     intf.html_file, intf.update_interval,
                     intf.alive_rcv_timeout, intf.alive_retries,
                     intf.max_encp_lines,intf.refresh)

    if inq.startup_state == e_errors.TIMEDOUT:
        enstore_functions.inqTrace(enstore_constants.INQERRORDBG,
		 "Inquisitor TIMED OUT when contacting %s"%(inq.startup_text,))
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
                Trace.handle_error()
                inq.serve_forever_error(inq.log_name)
                inq.do_dump()
            continue
    enstore_functions.inqTrace(enstore_constants.INQERRORDBG,
			       "Inquisitor finished (impossible)")
