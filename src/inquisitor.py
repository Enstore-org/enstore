#!/usr/bin/env python
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

# enstore imports
import setpath
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

server_map = {"log_server" : enstore_constants.LOGS,
	      "alarm_server" : enstore_constants.ALARMS,
	      "configuration" : enstore_constants.CONFIGS,
	      "file_clerk" : enstore_constants.FILEC,
	      "inquisitor" : enstore_constants.INQ,
	      "volume_clerk" : enstore_constants.VOLC,
	      "enstore" : enstore_constants.ENSTORE,
	      "network" : enstore_constants.NETWORK,
	      "alarms" : enstore_constants.ANYALARMS,
	      "media" : ""}
server_keys = server_map.keys()

# start of global variables
UP = 1
DOWN = 2
OUTAGE = 3
NOOUTAGE = 4
OVERRIDE = 5
NOOVERRIDE = 6

MY_NAME = "inquisitor"
LOGHTMLFILE_NAME = "enstore_logs.html"
TIMED_OUT_SP = "    "
DEFAULT_REFRESH = "60"
DEAD = "dead"
ALIVE = "alive"
NO_INFO_YET = "no info yet"

USER = 0
TIMEOUT=1

ENCP_UPDATE_INTERVAL = 60
LOG_UPDATE_INTERVAL = 300

DIVIDER = "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"

defaults = {'update_interval': 20,
            'alive_rcv_timeout': 5,
            'alive_retries': 2,
            'max_encp_lines': 50}

# delete a key from a dictionary if it exists
def delkey(key, dict):
    if dict.has_key(key):
	del dict[key]

# given a directory get a list of the files and their sizes
def get_file_list(dir, prefix):
    logfiles = {}
    files = os.listdir(dir)
    # pull out the files and get their sizes
    prefix_len = len(prefix)
    for file in files:
        if file[0:prefix_len] == prefix and (not file[-3:] == ".gz"):
            logfiles[file] = os.stat('%s/%s'%(dir,file))[stat.ST_SIZE]
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
        self.state = DEAD

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
        if self.state == DEAD:
            rtn = 1
        else:
            rtn = 0
        return rtn


class InquisitorMethods(dispatching_worker.DispatchingWorker):
    
    # look for the hung_rcv_timeout value for the specified key in the inquisitor
    # config info.  if it does not exist, return the default
    def get_hung_to(self, key):
        return self.inquisitor.get("hung_rcv_timeout", {}).get(key, 
                                               monitored_server.DEFAULT_HUNG_INTERVAL)

    def mark_event_relay(self, state):
        self.serverfile.output_etimedout(self.erc.event_relay_addr[0], 
                                         self.erc.event_relay_addr[1], state,
                                         time.time(), enstore_constants.EVENT_RELAY, 
                                         self.event_relay.last_alive)
        enstore_functions.inqTrace(enstore_constants.INQSERVERDBG, 
				   "mark event relay as %s"%(state,))

    def mark_server(self, state, server):
        if server.no_thread():
            self.serverfile.output_etimedout(server.host, server.port, state, 
                                             time.time(), server.name, 
                                             server.output_last_alive)
            self.new_server_status = 1
            enstore_functions.inqTrace(enstore_constants.INQSERVERDBG, 
				       "mark %s as %s"%(server.name, state,))

    # mark a server as not having sent a heartbeat yet.
    def mark_no_info(self, server):
        self.mark_server(NO_INFO_YET, server)

    # mark a server as having timed out, this happens if no alive message is
    # received from the event_relay for this server
    def mark_timed_out(self, server):
        self.mark_server("timed out", server)

    # mark a server as having possible hung, this happens if no alive message is
    # received from the event_relay for this server after server.hung_interval time
    def mark_dead(self, server):
        self.mark_server(DEAD, server)

    # called by the signal handling routines
    def s_update_exit(self, the_signal, frame):
        self.update_exit(0)

    def ok_to_monitor(self, config_dict):
        return not config_dict.has_key("inq_ignore")

    def is_server_known_down(self, server):
        # check to see if the server is known to be down by enstore.
        sfile, outage_d, offline_d, override_d = enstore_functions.read_schedule_file(self.html_dir)
        if offline_d.has_key(server.name):
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

    def make_server_status_html_file(self):
        if self.new_server_status:
            self.serverfile.open()
            self.serverfile.write()
            self.serverfile.close()
            self.serverfile.install()
            enstore_functions.inqTrace(enstore_constants.INQFILEDBG, 
				       "make new html server status file")
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

    def add_new_mv_lm_mc(self, key, config_d):
        if enstore_functions.is_mover(key):     
            cdict = config_d[key]
            if self.ok_to_monitor(cdict):
                self.server_d[key] = monitored_server.MonitoredMover(cdict, key,
								     self.csc)
        elif enstore_functions.is_media_changer(key):
            cdict = config_d[key]
            if self.ok_to_monitor(cdict):
                self.server_d[key] = monitored_server.MonitoredMediaChanger(cdict,
                                                                            key)
        elif enstore_functions.is_library_manager(key):
            cdict = config_d[key]
            if self.ok_to_monitor(cdict):
                self.server_d[key] = monitored_server.MonitoredLibraryManager(cdict,
                                                                              key,
									      self.csc)
        else:
            # nothing to see here
            return
        if self.ok_to_monitor(cdict):
            self.server_d[key].hung_interval = \
                                    self.inquisitor.get_hung_interval(self.server_d[key].name)
        else:
            self.serverfile.dont_monitor(key, cdict.get("host", ""), cdict.get("port", ""))

    def update_config_page(self, config):
        self.configfile.open()
        self.configfile.write(config)
        self.configfile.close()
        self.configfile.install()
        enstore_functions.inqTrace(enstore_constants.INQFILEDBG, 
				   "make new html config file")

    def stop_monitoring(self, server, skey):
        # set this so if there is a thread attempting to restart this
        # server, it will notice and abort the attempt.
        self.serverfile.dont_monitor(server.name, server.host, server.port)
        server.delete_me()
        del self.server_d[skey]

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
        d = self.csc.dump(self.alive_rcv_timeout, self.alive_retries)
        if enstore_functions.is_timedout(d):
            enstore_functions.inqTrace(enstore_constants.INQERRORDBG,
		"make_config_html_file - ERROR, getting config dict timed out")
            return
        # we may not have gotten the dict so check for it first before writing it.
        self.config_d = d.get('dump', {})
        self.update_config_page(self.config_d)

        # now update all of the internal information based on the new config info.
        self.update_variables_from_config(self.config_d)

    def handle_lmc_error(self, lib_man, time, state):
        status = enstore_functions.get_status(state)
        self.serverfile.output_error(lib_man.host, lib_man.port, status,
				     time, lib_man.name)
        enstore_functions.inqTrace(enstore_constants.INQERRORDBG, 
				   "lm client - ERROR: %s"%(status,))

    # get the library manager suspect volume list and output it
    def suspect_vols(self, lib_man, time):
	try:
	    state = safe_dict.SafeDict(lib_man.client.get_suspect_volumes())
	except (e_errors.TCP_EXCEPTION, socket.error), detail:
	    msg = "Error while getting suspect vols from %s (%s)"%(lib_man.name,
								   detail)
	    Trace.log(e_errors.ERROR, msg, e_errors.IOERROR)
	    return

        enstore_functions.inqTrace(enstore_constants.INQSERVERDBG,
		 "get new suspect vol list from %s"%(lib_man.name,))
        self.serverfile.output_suspect_vols(state, lib_man.name)
        if enstore_functions.is_timedout(state):
            self.serverfile.output_etimedout(lib_man.host, lib_man.port,
					     TIMED_OUT_SP, time, lib_man.name)
            enstore_functions.inqTrace(enstore_constants.INQERRORDBG, 
				       "suspect_vols - ERROR, timed out")
        elif not enstore_functions.is_ok(state):
            self.handle_lmc_error(lib_man, time, state)

    # get the library manager work queue and output it
    def work_queue(self, lib_man, time):
	try:
	    state = safe_dict.SafeDict(lib_man.client.getwork())
	except (e_errors.TCP_EXCEPTION, socket.error), detail:
	    msg = "Error while getting work queue from %s (%s)"%(lib_man.name, detail)
	    Trace.log(e_errors.ERROR, msg, e_errors.IOERROR)
	    return

        enstore_functions.inqTrace(enstore_constants.INQSERVERDBG,
				  "get new work queue from %s"%(lib_man.name,))
        self.serverfile.output_lmqueues(state, lib_man.name)
        if enstore_functions.is_timedout(state):
            self.serverfile.output_etimedout(lib_man.host, lib_man.port,
					     TIMED_OUT_SP, time, lib_man.name)
            enstore_functions.inqTrace(enstore_constants.INQERRORDBG, 
				       "work_queue - ERROR, timed out")
        elif not enstore_functions.is_ok(state):
            self.handle_lmc_error(lib_man, time, state)

    # get the library manager state and output it
    def lm_state(self, lib_man, time):
	try:
	    state = safe_dict.SafeDict(lib_man.client.get_lm_state())
	except (e_errors.TCP_EXCEPTION, socket.error), detail:
	    msg = "Error while getting state from %s (%s)"%(lib_man.name, detail)
	    Trace.log(e_errors.ERROR, msg, e_errors.IOERROR)
	    return
        enstore_functions.inqTrace(enstore_constants.INQSERVERDBG,
				   "get new state from %s"%(lib_man.name,))
        self.serverfile.output_lmstate(state, lib_man.name)
        if enstore_functions.is_timedout(state):
            self.serverfile.output_etimedout(lib_man.host, lib_man.port,
					     TIMED_OUT_SP, time, lib_man.name)
            enstore_functions.inqTrace(enstore_constants.INQERRORDBG, 
				       "lm_state - ERROR, timed out")
        elif not enstore_functions.is_ok(state):
            self.handle_lmc_error(lib_man, time, state)

    # get the information from the library manager(s)
    def update_library_manager(self, lib_man):
        # get a client and then check if the server is alive
        now = time.time()
        self.lm_state(lib_man, now)
        self.suspect_vols(lib_man, now)
        self.work_queue(lib_man, now)
        self.new_server_status = 1
        return

    # get the information from the mover
    def update_mover(self, mover):
        state = safe_dict.SafeDict(mover.client.status(self.alive_rcv_timeout, 
						       self.alive_retries))
        enstore_functions.inqTrace(enstore_constants.INQSERVERDBG, 
				   "get new state from %s"%(mover.name,))
        self.serverfile.output_moverstatus(state, mover.name)
        if enstore_functions.is_timedout(state):
            self.serverfile.output_etimedout(mover.host, mover.port, TIMED_OUT_SP,
					     time.time(), mover.name)
            enstore_functions.inqTrace(enstore_constants.INQERRORDBG, 
				       "mover_status - ERROR, timed out")
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
        enstore_functions.inqTrace(enstore_constants.INQERRORDBG, 
				   "exiting inquisitor due to request")
        self.erc.unsubscribe()
        os._exit(exit_code)

    # update any encp information from the log files
    def make_encp_html_file(self, now):
        self.last_encp_update = now
        self.encp_xfer_but_no_update = 0
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
            enstore_functions.inqTrace(enstore_constants.INQERRORDBG,
		     "update_encp - ERROR, getting log file name timed out")
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
                enstore_functions.inqTrace(enstore_constants.INQERRORDBG,
		   "update_encp - ERROR, getting last log file name timed out")
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
        enstore_functions.inqTrace(enstore_constants.INQFILEDBG, 
				   "make new html encp file")

    # examine each server we are monitoring to see if we have received an alive from
    # it recently (via the event relay)
    def check_last_alive(self):
        for skey in self.server_d.keys():
            server = self.server_d[skey]
            status = server.check_recent_alive(self.event_relay)
            if status == monitored_server.TIMEDOUT:
                self.mark_timed_out(server)
            elif status == monitored_server.HUNG:
                self.mark_dead(server)
                self.attempt_restart(server)

    def check_event_relay_last_alive(self):
        if self.event_relay.is_dead(time.time()):
            self.mark_event_relay(DEAD)
            if not self.sent_event_relay_alarm:
                Trace.alarm(e_errors.ERROR, 
                            e_errors.TIMEDOUT, {'server' : self.event_relay.name,
                                                'host' : self.erc.host })
                self.sent_event_relay_alarm = 1

    def server_is_alive(self, name):
        now = time.time()
        server = self.server_d.get(name, None)
        if server:
            self.serverfile.output_alive(server.host, server.port, ALIVE, 
                                         now, name)
            self.new_server_status = 1
            server.is_alive()
            server.did_restart_alarm = 0
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
        # if this is the first time alive after the event relay was thought to be deadm then
        # we must adjust the last alive times for all of the servers,  otherwise we will
        # think the server is dead immediately and not allow any time to receive the
        # alive message from it.  
        if not self.event_relay.is_alive():
            for server in self.server_d.keys():
                self.server_d[server].last_alive = now

        self.event_relay.alive(now)
        self.sent_event_relay_alarm = 0  
	msg = enstore_functions.read_erc(self.erc, fd)
        if msg:
            # ignore messages that originated with us
            if msg.type == event_relay_messages.ALIVE and not msg.server == self.inquisitor.name:
                enstore_functions.inqTrace(enstore_constants.INQEVTMSGDBG,
		      "received event relay message type %s (%s)"%(msg.type, 
								   msg.server))
                # check if this is one of the servers we are watching.
                if self.server_d.has_key(msg.server):
                    server = self.server_is_alive(msg.server)
                    # if server is a mover, we need to get some extra status
                    if enstore_functions.is_mover(msg.server):
                        self.update_mover(server)
                    # if server is a library_manager, we need to get some extra status
                    if enstore_functions.is_library_manager(msg.server):
                        self.update_library_manager(server)
            elif msg.type == event_relay_messages.NEWCONFIGFILE:
                enstore_functions.inqTrace(enstore_constants.INQEVTMSGDBG,
			 "received event relay message type %s"%(msg.type,))
                # a new config file was loaded into the config server, get it
                self.make_config_html_file()
            elif msg.type == event_relay_messages.ENCPXFER:
                enstore_functions.inqTrace(enstore_constants.INQEVTMSGDBG,
			 "received event relay message type %s"%(msg.type,))
                # an encp xfer completed - update the encp history page, but only do
                # this at most once per minute
                if now - self.last_encp_update > 60:
                    # it has been more than 1 minute since the last update, so do it.
                    self.make_encp_html_file(now)
                else:
                    # record the fact that we got an encp transfer notice, but it was too
                    # soon to do another update of the html file.  we will check this
                    # later to do the update if no other encp xfer comes thru to trigger
                    # an update.
                    self.encp_xfer_but_no_update = now

    # these are the things we do periodically, this routine is called when an
    # interval is up, from within dispatching_worker  -
    #      o generate a new log file page
    #      o generate a new encp history page
    #      o process any servers that have not reported in recently
    #      o generate a new server status web page
    def periodic_tasks(self, reason_called=TIMEOUT):
        enstore_functions.inqTrace(enstore_constants.INQEVTMSGDBG, 
				   "periodic timeout")
        # just output the inquisitor alive info, if we are doing this, we are alive.
        self.server_is_alive(self.inquisitor.name)
        # see if there are any servers in cardiac arrest (no heartbeat)
        self.check_last_alive()
        # check if we have received an event relay message recently
        self.check_event_relay_last_alive()
        self.make_server_status_html_file()

    def encp_periodic_tasks(self, reason_called=TIMEOUT):
        # the encp web page is updated when notice of an encp transfer is received, but
        # not more than 1/ minute.  thus if notice of 1 trnasfer arrives, the page is
        # updated and then another notice comes almost immediately, the page will not have
        # been updated to include this one. so here we
        # see if there has been notice of an encp transfer which happened too soon after
        # a previous transfer to trigger the web page to be updated.
        # if so then we must hand trigger the web page update
        enstore_functions.inqTrace(enstore_constants.INQEVTMSGDBG, 
				   "encp periodic timeout")
        if self.encp_xfer_but_no_update:
            now = time.time()
            if now - self.encp_xfer_but_no_update > ENCP_UPDATE_INTERVAL:
                self.make_encp_html_file(now)

    def log_periodic_tasks(self, reason_called=TIMEOUT):
        # update the web page that lists all the current log files
        enstore_functions.inqTrace(enstore_constants.INQEVTMSGDBG, 
				   "log periodic timeout")
        self.make_log_html_file()

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

    def get_value(self, aKey, aValue):
        if aValue == -1:         # nothing was entered on the command line 
            new_val = self.inquisitor.config.get(aKey, defaults.get(aKey))
            self.got_from_cmdline[aKey] = aValue
        else:
            self.got_from_cmdline[aKey] = aValue
            new_val = aValue
        return new_val

    # subscribe to the event relay
    def subscribe(self, ticket):
        ticket["status"] = (e_errors.OK, None)
        self.erc.subscribe()
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
	    return 0

    def update_schedule_file(self, ticket, func):
	ticket["status"] = (e_errors.OK, None)
	bad_servers = []
	server_l = string.split(ticket["servers"], ',')
        sfile, outage_d, offline_d, override_d = enstore_functions.read_schedule_file(self.html_dir)
	if sfile.opened != 0:
	    for key in server_l:
		# map the entered name to the name in the outage dictionary
		num, server = self.find_server_match(key)
		if num == 1:
		    key = server_map[server]
		elif not self.is_valid(key):
		    key = None
		    bad_servers.append(key)
		    ticket["status"] = (e_errors.DOESNOTEXIST, bad_servers)

		if key is not None:
		    # we found a match
		    if func == UP:
			delkey(key, offline_d)
		    elif func == DOWN:
			offline_d[key] = ticket["time"]
		    elif func == OUTAGE:
			outage_d[key] = ticket["time"]
		    elif func == NOOUTAGE:
			delkey(key, outage_d)
		    elif func == OVERRIDE:
			override_d[key] = ticket["saagStatus"]
		    elif func == NOOVERRIDE:
			delkey(key, override_d)
	    if not sfile.write(outage_d, offline_d, override_d):
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

    def show(self, ticket):
	ticket["status"] = (e_errors.OK, None)
        sfile, outage_d, offline_d, override_d = enstore_functions.read_schedule_file(self.html_dir)
	dfile, seen_down_d = enstore_functions.read_seen_down_file(self.html_dir)
	if sfile.opened != 0:
	    ticket["outage"] = outage_d
	    ticket["offline"] = offline_d
	    ticket["override"] = override_d
	else:
	    ticket["status"] = (e_errors.IOERROR, None)
	    Trace.log(e_errors.ERROR,
		      "Could not read file %s/%s"%(self.html_dir, 
						   enstore_constants.OUTAGEFILE))
	if dfile.opened != 0:
	    ticket["seen_down"] = seen_down_d
	else:
	    ticket["status"] = (e_errors.IOERROR, None)
	    Trace.log(e_errors.ERROR,
		      "Could not read file %s/%s"%(self.html_dir, 
						   enstore_constants.SEENDOWNFILE))
        self.send_reply(ticket)
        enstore_functions.inqTrace(enstore_constants.INQWORKDBG,
				   "show up/down status work from user")


class Inquisitor(InquisitorMethods, generic_server.GenericServer):

    def __init__(self, csc, html_file="", update_interval=-1, alive_rcv_to=-1, 
                 alive_retries=-1, max_encp_lines=-1, refresh=-1):
	global server_map
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

        # get the keys that are associated with the web information
        self.www_server = self.config_d.get(enstore_constants.WWW_SERVER, {})
	server_map["media"] = self.www_server.get(www_server.MEDIA_TAG, 
						  www_server.MEDIA_TAG_DEFAULT)

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

        cdict = self.config_d.get(enstore_constants.ALARM_SERVER, {})
        self.alarm_server = monitored_server.MonitoredAlarmServer(cdict)
        if self.ok_to_monitor(cdict):
            self.server_d[enstore_constants.ALARM_SERVER] = self.alarm_server
        else:
            self.serverfile.dont_monitor(enstore_constants.ALARM_SERVER,
                                         self.alarm_server.host,
                                         self.alarm_server.port)

        cdict = self.config_d.get(enstore_constants.LOG_SERVER, {})
        self.log_server = monitored_server.MonitoredLogServer(cdict)
        if self.ok_to_monitor(cdict):
            self.server_d[enstore_constants.LOG_SERVER]  = self.log_server
        else:
            self.serverfile.dont_monitor(enstore_constants.LOG_SERVER,
                                         self.log_server.host,
                                         self.log_server.port)

        cdict = self.config_d.get(enstore_constants.FILE_CLERK, {})
        self.file_clerk = monitored_server.MonitoredFileClerk(cdict)
        if self.ok_to_monitor(cdict):
            self.server_d[enstore_constants.FILE_CLERK]  = self.file_clerk
        else:
            self.serverfile.dont_monitor(enstore_constants.FILE_CLERK,
                                         self.file_clerk.host,
                                         self.file_clerk.port)

        cdict = self.config_d.get(enstore_constants.VOLUME_CLERK, {})
        self.volume_clerk = monitored_server.MonitoredVolumeClerk(cdict)
        if self.ok_to_monitor(cdict):
            self.server_d[enstore_constants.VOLUME_CLERK]  = self.volume_clerk
        else:
            self.serverfile.dont_monitor(enstore_constants.VOLUME_CLERK,
                                         self.volume_clerk.host,
                                         self.volume_clerk.port)

        cdict = self.config_d.get(enstore_constants.CONFIG_SERVER, {})
        self.config_server = monitored_server.MonitoredConfigServer(cdict)
        if self.ok_to_monitor(cdict):
            self.server_d[enstore_constants.CONFIG_SERVER]  = self.config_server
        else:
            self.serverfile.dont_monitor(enstore_constants.CONFIG_SERVER,
                                         self.config_server.host,
                                         self.config_server.port)

        for server_key in self.server_d.keys():
            server = self.server_d[server_key]
            server.hung_interval = self.inquisitor.get_hung_interval(server.name)

        self.lib_man_d = {}
        self.mover_d = {}
        self.media_changer_d = {}
        for key in self.config_d.keys():
            self.add_new_mv_lm_mc(key, self.config_d)
	
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

        self.event_relay_msg = event_relay_messages.EventRelayAliveMsg(self.inquisitor.host,
                                                                       self.inquisitor.port)
        self.event_relay_msg.encode(self.inquisitor.name)

        # setup the communications with the event relay task
        self.resubscribe_rate = 300
        self.erc.start([event_relay_messages.ALIVE,
                        event_relay_messages.NEWCONFIGFILE,
                        event_relay_messages.ENCPXFER], self.resubscribe_rate)

        # start our heartbeat to the event relay process
        self.erc.start_heartbeat(enstore_constants.INQUISITOR, 
                                 self.inquisitor.alive_interval)

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
