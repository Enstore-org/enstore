#
# This server uses the following data structures:
#
#   NAME                KEY TYPE               DESCRIPTION
#   self.last_alive{}   same as config dict    last time found alive
#   self.last_update{}  same as config dict    last time updated
#   self.reset{}        same as config dict    intervals reset on command line
#   self.intervals{}    same as config dict    intervals for updates
#
#   self.server_keys[]                         list of servers (and others) to
#                                                 ping
#   self.forked{}       same as config dict    if exists, means we forked to
#                                              restart that server. cleared
#                                              once the server is alive again.
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

# enstore imports
import timeofday
import traceback
import volume_clerk_client
import file_clerk_client
import library_manager_client
import media_changer_client
import mover_client
import dispatching_worker
import generic_server
import Trace
import e_errors
import enstore_files
import enstore_status
import enstore_plots
import enstore_html
import udp_client

def default_timeout():
    return 5

def default_server_interval():
    return 60

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

MY_NAME = "inquisitor"

CONFIG_DICT_TOUT = "can't get config dict"

LOGFILE_DIR = "logfile_dir"
LOGHTMLFILE_NAME = "enstore_logs.html"
PATROL_FILE = "en_patrol.html"

DID_IT = 0
TIMED_OUT = 1

SUFFIX = ".new"
SERVER_KEYWORD = "server"
TIMED_OUT_SP = "    "
DEFAULT_SERVER_INTERVAL = "default_server_interval"
DEFAULT_REFRESH = "60"

CONFIG_S = "configuration_server"
ALARM_S = "alarm_server"
LOG_S = "log_server"

NO_PING = -1
HUNG_TO_DEFAULT = 30
HUNG_RETRIES_DEFAULT = 30

class InquisitorMethods(dispatching_worker.DispatchingWorker):

    # look for the hung_rcv_timeout value for the specified key in the inquisitor
    # config info.  if it does not exist, return the default
    def get_hung_to(self, key):
	try:
	    t = self.csc.get(MY_NAME, self.alive_rcv_timeout, self.alive_retries)
	except errno.errorcode[errno.ETIMEDOUT]:
	    self.htmlfile.output_noconfigdict(CONFIG_DICT_TOUT, time, key)
	    Trace.trace(13,"get_hung_to - ERROR, getting config dict timed out ")
	    return HUNG_TO_DEFAULT
	return t.get("hung_rcv_timeout", {}).get(key, HUNG_TO_DEFAULT)

    # look for the hung_retries value for the specified key in the inquisitor
    # config info.  if it does not exist, return the default
    def get_hung_retries(self, key):
	try:
	    t = self.csc.get(MY_NAME, self.alive_rcv_timeout, self.alive_retries)
	except errno.errorcode[errno.ETIMEDOUT]:
	    self.htmlfile.output_noconfigdict(CONFIG_DICT_TOUT, time, key)
	    Trace.trace(13,"get_hung_retries - ERROR, getting config dict timed out ")
	    return HUNG_RETRIES_DEFAULT
	return t.get("hung_retries", {}).get(key, HUNG_RETRIES_DEFAULT)

    # get the alive status of the server and output it
    def alive_status(self, client, (host, port), time, key):
	stat = client.alive(key, self.alive_rcv_timeout, self.alive_retries)
	if not stat['status'] == ('TIMEDOUT', None):
	    self.htmlfile.output_alive(host, stat['address'][1], "alive", 
				       time, key)
	    self.last_alive[key] = time
	else:
            last_time = self.last_alive.get(key, -1)
	    self.htmlfile.output_etimedout(host, port, "timed out", time, 
					   key, last_time)
            Trace.trace(14,"alive_status - ERROR, alive timed out")
	    return TIMED_OUT
	return DID_IT

    # called by the signal handling routines
    def s_update_exit(self, the_signal, frame):
	self.update_exit(0)

    # check if a server is alive, if not, try to restart it.  this restarting
    # may be overridden in the config file on a per server basis.
    def alive_and_restart(self, client, (host, port), time, key):
        ret = self.alive_status(client, (host, port), time, key)
        if ret == TIMED_OUT:
            # could not communicate with the server.  do the following -
            #    1. make sure the server is still listed in the config file
            #    2. try sending the alive again but wait longer and retry more
            #    3. if still timed out, try to restart the server
            #    4. if not restarted, raise an alarm
            try:
                t = self.csc.get_uncached(key, self.alive_rcv_timeout,
                                          self.alive_retries)
            except errno.errorcode[errno.ETIMEDOUT]:
                self.htmlfile.output_noconfigdict(CONFIG_DICT_TOUT, time, key)
                Trace.trace(13,"alive_and_restart - ERROR, getting config dict timed out ")
                return TIMED_OUT
            if t['status'][0] == 'KEYERROR':
                # 1. do not monitor this server any more, remove from our dict
                self.remove_key(key)
            else:
                # 2. the server appears dead.  do not fork to restart if the
                #    last time we did the fork.
                if not self.forked.has_key(key):
                    # fork off here so that the child can do the longer wait to
                    # see if the server is really alive not just busy.  we must
                    # keep track of the fact that we forked off so the next
                    # time we find the server dead we do not fork again.
                    Trace.trace(8,"Inquisitor forking to restart %s"%(key,))
                    self.forked[key] = 1
		    pid = self.fork()
                    if not pid:
                        # we are the first child ############################
			# fork a second time and then exit.  the 2nd child will
			# actually do the work.  the 1st child (this one) exits
			# immediately so that the 2nd child is inherited by the
			# init process and does not become a zombie when it 
			# exits.
			pid2 = self.fork()        # getting the second child
			if not pid2:
			    # we are the second child #######################
			    Trace.init("INQ_CHILD")
			    # we need to get new udp clients so we don't collide
			    # with our parent.
			    client.u = udp_client.UDPClient()
			    if not key == CONFIG_S:
				self.csc.u = udp_client.UDPClient()
			    if not key == LOG_S:
				self.logc.u = udp_client.UDPClient()
			    if not key == ALARM_S:
				self.alarmc.u = udp_client.UDPClient()
			    # Check on server status but wait a long time
			    self.alive_rcv_timeout = t.get("hung_rcv_timeout", 
							   self.get_hung_to(key))
			    self.alive_retries = t.get("hung_retries", 
						       self.get_hung_retries(key))
			    ret = self.alive_status(client, (t['host'], t['port']),
						    time, key)
			    if ret == TIMED_OUT:
				# 3. if we raise an alarm we need to include the 
				#     following info.
				alarm_info = {'server' : key}
				if t.has_key("norestart"):
				    # do not restart, raise an alarm that the
				    # server is dead.
				    if not key == ALARM_S:
					Trace.alarm(e_errors.ERROR,
						    e_errors.SERVERDIED,
						    alarm_info)
				    else:
					Trace.log(e_errors.ERROR,
						  "%s died and will not be restarted"%(key,))
				else:
				    # we should try to restart the server.  try 3X
				    i = 0
				    self.alive_retries = 1
				    pid = os.getpid()
				    Trace.log(e_errors.INFO,
					      "Attempting restart of %s (%s)"%(key, pid))
				    # we need just the node name part of the host name
				    node = string.split(host, ".", 1)
				    alarm_info['node'] = node
				    while i < 3:
					Trace.trace(7, "Server restart: try %s"%(i,))
					os.system('enstore Erestart %s "--just %s"'%(node[0], key))
					# check if alive
					ret = self.alive_status(client,
								(host, port),
								time, key)
					if ret == DID_IT:
					    Trace.log(e_errors.INFO,
						      "Restarted %s"%(key,))
					    break
					else:
					    i = i + 1
				    else:
					# 4. we could not restart the server
					if not key == ALARM_S:
					    Trace.alarm(e_errors.ERROR, 
							e_errors.CANTRESTART,
							alarm_info)
					else:
					    Trace.log(e_errors.ERROR,
                                                  "Can't restart %s"%(key,))
			    del client.u
			    if not key == CONFIG_S:
				del self.csc.u
			    if not key == LOG_S:
				del self.logc.u
			    if not key == ALARM_S:
				del self.alarmc.u
			    os._exit(0)   # second child
			    # end of the second child ##################################
			else:
			    # add the second childs pid to the appropriate enstore file so the
			    # child can be killed automatically
			    os.system("tpid=`$ENSTORE_DIR/bin/en_get_pid_dir`;echo %s >> $tpid/`uname -n`-inquisitor_pids"%(pid2,))
			    os._exit(0)   # first child
		    else:
			# we are the original parent.  now we must wait for the first
			# child to exit so it does not become a zombie
			os.waitpid(pid, 0)
        else:
            # the server was alive, clear out any record that we forked to
            # restart it because apparently it worked.
            if self.forked.has_key(key):
                del self.forked[key]
        return ret

    # send alive to the server and handle any errors
    def do_alive_check(self, key, time, client):
	try:
	    t = self.csc.get(key, self.alive_rcv_timeout, self.alive_retries)
	except errno.errorcode[errno.ETIMEDOUT]:
	    self.htmlfile.output_noconfigdict(CONFIG_DICT_TOUT, time, key)
            Trace.trace(13,"do_alive_check - ERROR, getting config dict timed out ")
	    return TIMED_OUT

	ret = DID_IT
        if t['status'] == (e_errors.OK, None):
	    ret = self.alive_and_restart(client, (t['host'], t['port']),
                                         time, key)
	elif t['status'][0] == 'KEYERROR':
	    # do not monitor this server any more, remove him from our dict
	    self.remove_key(key)
	return ret

    # create the patrol page that has the patrol url on it. the url comes from
    # the configuration file
    def make_patrol_html_file(self, pdir):
	self.patrolhtmlfile.open()
	if pdir:
	    msg = (pdir, "Patrol")
	else:
	    msg = "No patrol_file key found in inquisitor part of config file."
	self.patrolhtmlfile.write(msg)
	self.patrolhtmlfile.close()

    # create an html file that has a link to all of the current log files
    def make_log_html_file(self, log_dirs):
        # add the web host to the dict of log directories if not already there
        for key in log_dirs.keys():
            if not log_dirs[key][0:5] == "http:" and \
               not log_dirs[key][0:5] == "file:":
                log_dirs[key] = self.www_host+log_dirs[key]

        # first get a list of all of the log files and their sizes
        if self.logc.log_dir:
	    # given a directory get a list of the files and their sizes
	    logfiles = get_file_list(self.logc.log_dir, enstore_files.LOG_PREFIX)
            if logfiles:
                # create the new log listing file.  create it with a different
                # extension than the real one, we will mv the new one to the
                # real name after its creation.
                self.loghtmlfile.open()
                self.loghtmlfile.write(self.http_log_file_path, logfiles,
                                       log_dirs, self.www_host)
                self.loghtmlfile.close()
                # now we must move the new file to it's real name. do a copy
                # and then a delete as this will work across disks
                os.system("cp %s/%s%s %s/%s"%(self.logc.log_dir,
                                              LOGHTMLFILE_NAME,
                                              SUFFIX, self.html_dir,
                                              LOGHTMLFILE_NAME))
                os.remove("%s/%s%s"%(self.logc.log_dir, LOGHTMLFILE_NAME,
                                     SUFFIX))

    # create the html file with the inquisitor plot information
    def	make_plot_html_page(self):
	self.plothtmlfile.open()
	# get the list of stamps and jpg files
	(jpgs, stamps, pss) = enstore_plots.find_jpg_files(self.html_dir)
	self.plothtmlfile.write(jpgs, stamps, pss)
	self.plothtmlfile.close()
	self.move_file(1, self.plothtmlfile_orig)

    # update the file that contains the configuration file information
    def make_config_html_file(self):
        self.confightmlfile.open()
	try:
            # get the config dictionary
	    self.csc.dump(self.alive_rcv_timeout, self.alive_retries)
	except errno.errorcode[errno.ETIMEDOUT]:
            Trace.trace(12,"make_config_html_file - ERROR, getting config dict timed out")
	    return
	# we may not have gotten the dict so check for it first before writing it.
	self.confightmlfile.write(self.csc.config_dump.get('dump', {}))
	self.confightmlfile.close()
	self.move_file(1, self.confightmlfile_orig)

    # update the miscellaneous status file.  the input dict is of the form -
    #            { filename : (command_to_do, text) , ... }
    def update_update_commands(self, key, thetime):
        self.mischtmlfile.open()
        if self.cmds_to_do:
            cmd_keys = self.cmds_to_do.keys()
            cmd_keys.sort()
	    data = []
            for hfile in cmd_keys:
                # we need to add some info for this command to the html file
                # and we need to execute the command.  if the filename is
                # 'NONE', then, do not add a link to the file.  if the
                # command is 'NONE', then do not execute anything.
                if not self.cmds_to_do[hfile] == "NONE":
		    # we do not want to hang in case the command hangs, so do a fork.
		    pid = self.fork()
                    if not pid:
                        # we are the first child ############################
			# fork a second time and then exit.  the 2nd child will
			# actually do the work.  the 1st child (this one) exits
			# immediately so that the 2nd child is inherited by the
			# init process and does not become a zombie when it 
			# exits.
			pid2 = self.fork()        # getting the second child
			if not pid2:
			    # we are the second child #######################
			    os.system("$ENSTORE_DIR/bin/run_misc_command \"%s\" 1>%s/%s 2>&1"%(self.cmds_to_do[hfile],
							 self.html_dir, hfile))
			    os._exit(0)   # second child
			    # end of the second child ##################################
			else:
			    os._exit(0)   # first child
		    else:
			# we are the original parent.  now we must wait for the first
			# child to exit so it does not become a zombie
			os.waitpid(pid, 0)

                if not hfile == "NONE":
		    data.append(hfile)
	    self.mischtmlfile.write((data, self.html_dir))
        self.mischtmlfile.close()
        self.move_file(1, self.mischtmlfile_orig)

    # get the library manager suspect volume list and output it
    def suspect_vols(self, lm, (host, port), key, time):
	try:
	    stat = lm.get_suspect_volumes()
	    self.htmlfile.output_suspect_vols(stat, key)
	except errno.errorcode[errno.ETIMEDOUT]:	
	    self.htmlfile.output_etimedout(host, port, TIMED_OUT_SP, time,
                                           key)
	    Trace.trace(13, "suspect_vols - ERROR, timed out")

    # get the library manager work queue and output it
    def work_queue(self, lm, (host, port), key, time):
	try:
	    stat = lm.getwork()
	    self.htmlfile.output_lmqueues(stat, key)
	except errno.errorcode[errno.ETIMEDOUT]:	
	    self.htmlfile.output_etimedout(host, port, TIMED_OUT_SP, time,
                                           key)
	    Trace.trace(13, "work_queue - ERROR, timed out")

    # get the library manager state and output it
    def lm_state(self, lm, (host, port), key, time):
	try:
	    stat = lm.get_lm_state()
	    self.htmlfile.output_lmstate(stat, key)
	except errno.errorcode[errno.ETIMEDOUT]:	
	    self.htmlfile.output_etimedout(host, port, TIMED_OUT_SP, time,
                                           key)
	    Trace.trace(13, "lm_state - ERROR, timed out")

    # get the library manager mover list and output it
    def mover_list(self, lm, (host, port), key, time):
	try:
	    stat = lm.getmoverlist()
	    self.htmlfile.output_lmmoverlist(stat, key)
	except errno.errorcode[errno.ETIMEDOUT]:	
	    self.htmlfile.output_etimedout(host, port, TIMED_OUT_SP, time,
                                           key)
	    Trace.trace(13, "mover_list - ERROR, timed out")

    # get the movers' status
    def mover_status(self, movc, (host, port), key, time):
	try:
	    stat = movc.status(self.alive_rcv_timeout, self.alive_retries)
	    self.htmlfile.output_moverstatus(stat, key)
	except errno.errorcode[errno.ETIMEDOUT]:	
	    self.htmlfile.output_etimedout(host, port, TIMED_OUT_SP, time,
                                           key)
	    Trace.trace(13, "mover_status - ERROR, timed out")

    # get the information from the configuration server
    def update_config_server(self, key, time):
	self.alive_and_restart(self.csc, self.csc.get_address(), 
                               time, key)

    # get the information from the library manager(s)
    def update_library_manager(self, key, time):
	# get info on this library_manager
	try:
	    t = self.csc.get(key, self.alive_rcv_timeout, self.alive_retries)
	except errno.errorcode[errno.ETIMEDOUT]:
	    self.htmlfile.output_noconfigdict(CONFIG_DICT_TOUT, time, key)
	    Trace.trace(12,"update_library_manager - ERROR, getting config dict timed out")
	    return
        if t['status'] == (e_errors.OK, None):
	    # get a client and then check if the server is alive
            lmc = library_manager_client.LibraryManagerClient(self.csc, key)
	    ret = self.alive_and_restart(lmc, (t['host'], t['port']), 
                                         time, key)
	    if ret == DID_IT:
                self.lm_state(lmc, (t['host'], t['port']), key, time)
	        self.suspect_vols(lmc, (t['host'], t['port']), key, time)
	        self.mover_list(lmc, (t['host'], t['port']), key, time)
	        self.work_queue(lmc, (t['host'], t['port']), key, time)
	    # get rid of this in preparation for the next time through
	    del lmc.u
	elif t['status'][0] == 'KEYERROR':
	    self.remove_key(key)

    # get the information from the movers
    def update_mover(self, key, time):
	# get info on this mover
	try:
	    t = self.csc.get(key, self.alive_rcv_timeout, self.alive_retries)
	except errno.errorcode[errno.ETIMEDOUT]:
	    self.htmlfile.output_noconfigdict(CONFIG_DICT_TOUT, time, key)
            Trace.trace(12,"update_mover - ERROR, getting config dict timed out")
	    return
        if t['status'] == (e_errors.OK, None):
	    movc = mover_client.MoverClient(self.csc, key)
	    ret = self.alive_and_restart(movc, (t['host'], t['port']),
                                         time, key)
	    if ret == DID_IT:
	        self.mover_status(movc, (t['host'], t['port']), key, time)
	    # get rid of this in preparation for the next time through
	    del movc.u

    # get the information from the media changer(s)
    def update_media_changer(self, key, time):
	# get info on this media changer
	try:
	    t = self.csc.get(key, self.alive_rcv_timeout, self.alive_retries)
	except errno.errorcode[errno.ETIMEDOUT]:
	    self.htmlfile.output_noconfigdict(CONFIG_DICT_TOUT, time, key)
            Trace.trace(12,"update_media_changer - ERROR, getting config dict timed out")
	    return
        if t['status'] == (e_errors.OK, None):
	    # get a client and then check if the server is alive
	    mcc = media_changer_client.MediaChangerClient(self.csc, key)
	    self.alive_and_restart(mcc, (t['host'], t['port']), time, key)
	    # get rid of this in preparation for the next time through
	    del mcc.u
	elif t['status'][0] == 'KEYERROR':
	    self.remove_key(key)

    # get the information from the file clerk
    def update_file_clerk(self, key, time):
	self.do_alive_check(key, time, self.fcc)

    # get the information from the log server
    def update_log_server(self, key, time):
	self.do_alive_check(key, time, self.logc)

    # get the information from the volume clerk server
    def update_volume_clerk(self, key, time):
	self.do_alive_check(key, time, self.vcc)

    # get the information from the alarm server
    def update_alarm_server(self, key, time):
	self.do_alive_check(key, time, self.alarmc)

    # pull various info out of the config file
    def update_random_info(self, conf_dict):
        self.rcv_timeout = conf_dict.get('timeout', default_timeout())
        self.alive_rcv_timeout = conf_dict.get('alive_rcv_timeout',
                                               default_alive_rcv_timeout())
        self.alive_retries = conf_dict.get('alive_retries',
                                           default_alive_retries())
        self.max_encp_lines = conf_dict.get('max_encp_lines',
                                            default_max_encp_lines())
        self.default_server_interval = conf_dict.get(DEFAULT_SERVER_INTERVAL,
                                                     default_server_interval())

    # get the information from the inquisitor
    def update_inquisitor(self, key, time):
	# get info on the inquisitor
	try:
	    t = self.csc.get_uncached(key, self.alive_rcv_timeout, 
				      self.alive_retries)
	except errno.errorcode[errno.ETIMEDOUT]:
	    self.htmlfile.output_noconfigdict(CONFIG_DICT_TOUT, time, key)
            Trace.trace(12,"update_inquisitor - ERROR, getting config dict timed out")
	    return

        # this will be used when creating the log html file
        self.www_host = t.get('www_host', "file:")
        self.http_log_file_path = t.get('http_log_file_path',
                                        self.logc.log_dir)
        
	# just output our info, if we are doing this, we are alive.
	self.htmlfile.output_alive(t['host'], t['port'], "alive", time, key)

        # update the web page that lists all the current log files
        self.make_log_html_file(t.get("user_log_dirs", {}))

        # if our dictionary contains a list of commands to do when we update
        # the inquisitor, save the commands
        self.cmds_to_do = t.get('update_commands', {})

        # update random info from the config file
        self.update_random_info(t)

	# we need to update the dict of servers that we are keeping track of.
	# however we cannot do it now as we may be in the middle of a loop
	# reading the keys of this dict.  so we just record the fact that this
	# needs to get done and we will do it later
	self.doupdate_server_dict = 1
	self.new_intervals = t.get('intervals', {})

	# clear out the cache in the config client so we can get new info on
	# any of the servers in case the info has changed.
	self.csc.clear()

        # update the configuration file web page
        self.make_config_html_file()

	# update the inquisitor plots web page
	self.make_plot_html_page()

	# update the page that has a link to patrol (if we have one)
	self.make_patrol_html_file(t.get("patrol_file", ""))
        
    # only change the status of the inquisitor on the system status page to
    # timed out, then exit.
    def update_exit(self, exit_code):
	# get info on the inquisitor
	key = MY_NAME
	try:
	    t = self.csc.get(key, self.alive_rcv_timeout, self.alive_retries)
	except errno.errorcode[errno.ETIMEDOUT]:
	    self.htmlfile.output_noconfigdict(CONFIG_DICT_TOUT, time.time(), key)
            Trace.trace(12,"update_exit - ERROR, getting config dict timed out")
	    return
	self.htmlfile.open()
	self.htmlfile.output_alive(t['host'], t['port'], "exiting", time.time(), key)
	# the above just stored the information, now write the page out
	self.htmlfile.write()
	self.htmlfile.close()
	self.move_file(1, self.htmlfile_orig)
	# Don't fear the reaper!!
	Trace.trace(10, "exiting inquisitor due to request")
	sys.exit(exit_code)

    # update any encp information from the log files
    def update_encp(self, key, time):
        encplines = []
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
            encpfile = enstore_files.EnDataFile(logfile, parsed_file+".encp", search_text,
						"", "|sort -r")
            encpfile.open('r')
            encplines = encpfile.read(self.max_encp_lines)
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
	        encplines = encplines + encpfile2.read(self.max_encp_lines-i)
	        encpfile2.close()
	# now we have some info, output it
	self.encpfile.write(encplines)

    # get the information about the blocksizes
    def update_blocksizes(self, key, time):
	try:
	    t = self.csc.get(key, self.alive_rcv_timeout, self.alive_retries)
	except errno.errorcode[errno.ETIMEDOUT]:
	    self.htmlfile.output_noconfigdict(CONFIG_DICT_TOUT, time, key)
            Trace.trace(12,"update_blocksizes - ERROR, getting config dict timed out")
	    return
        if t['status'] == (e_errors.OK, None):
	    self.htmlfile.output_blocksizes(t)
	elif t['status'][0] == 'KEYERROR':
	    self.remove_key(key)

    # we do not need to report these
    def update_database(self, key, time):
	pass

    def update_backup(self, key, time):
	pass

    # get the keys from the inquisitor part of the config file ready for use
    def prepare_keys(self):
	self.server_keys = self.intervals.keys()
	self.server_keys.sort()

    # delete the key (server) from the main looping list and from the text
    # output to the various files
    def remove_key(self, key):
        try:
            self.server_keys.remove(key)
        except ValueError:
            pass
        if self.forked.has_key(key):
            del self.forked[key]
	self.htmlfile.remove_key(key)
	self.encpfile.remove_key(key)

    # fix up the server list that we are keeping track of.  this is part of the
    # update of the inquisitor information
    def update_server_dict(self):
	self.intervals = self.new_intervals
	# now we must look thru the whole config file and use the default
	# server interval for any servers that were not included in the
	# inquisitors' 'intervals' dict element
	self.fill_in_default_intervals()
	# now look thru any server intervals that may have reset by hand and
	# keep the reset value
	for key in self.reset.keys():
	    if self.intervals.has_key(key):
	        self.intervals[key] = self.reset[key]
	    else:
	        del self.reset[key]
        # get a list of the servers that we will ping
	self.prepare_keys()

    # fill in a default interval for any servers that didn't have one specified
    def fill_in_default_intervals(self, ctime=time.time()):
	itk = "inq_interval"
	try:
            # get a list of the servers/keys in the config dictionary
	    csc_keys = self.csc.get_keys(self.alive_rcv_timeout, 
	                                 self.alive_retries)
	except errno.errorcode[errno.ETIMEDOUT]:
            Trace.trace(12,"fill_in_default_intervals - ERROR, getting config dict timed out")
	    return
        # for each server, get its config dict element and see if it specifies
        # an inquisitor interval. if so, use it.
	for a_key in csc_keys['get_keys']:
	    k = self.csc.get(a_key)
	    if k.has_key(itk):
                # we found inquisitor interval key in the server dict element
	        self.intervals[a_key] = k[itk]
                # if we have not yet kept track of the last successful update
                # of this server, set it to now.
	        if not self.last_update.has_key(a_key):
	            self.last_update[a_key] = ctime
	    else:
                # if neither the inq dict element or the individual server dict
                # element had an interval value, and we currently don't have 
                # one, use the default
	        if not self.intervals.has_key(a_key):
	            self.intervals[a_key] = self.default_server_interval
	            if not self.last_update.has_key(a_key):
	                self.last_update[a_key] = ctime
	# now get rid of any keys that are in intervals and not in the config file
	# make an exception for update_commands, config_server and encp. (since
        # they will never be in csc_keys. this takes care of the
        # case where a server was removed from the config dict.
	for a_key in self.intervals.keys():
	    if a_key not in csc_keys['get_keys']:
	        if a_key not in ["config_server", "encp","update_commands"]:
	            del self.intervals[a_key]
	            self.htmlfile.remove_key(a_key)
	# if there was no encp, config_server or update_commands intervals
        # specified in the 
	# inquisitor section of the config file then we will use the default
	if not self.intervals.has_key("config_server"):
	    self.intervals["config_server"] = self.default_server_interval
	if not self.intervals.has_key("encp"):
	    self.intervals["encp"] = self.default_server_interval
	if not self.intervals.has_key("update_commands"):
	    self.intervals["update_commands"] = self.default_server_interval

    # output a line if an update was requested of a server that we do not have
    # a function for
    def update_nofunc(self, server):
	# if the config file entry had a keyword 'inq_ignore' (set to 
	# anything), then this information is not to be displayed anyway.
	ticket = self.csc.get(server)
	if not ticket.has_key("inq_ignore"):
	    self.htmlfile.output_nofunc(server)
	else:
	    # we should not display this info, however we may have been
	    # displaying it until recently, so we need to remove it from the
	    # file info.
	    self.htmlfile.remove_key(server)
	    self.encpfile.remove_key(server)

    # update the enstore system status information
    def do_update(self, do_all=0):

	# open the html files and output the header to them
	self.htmlfile.open()
	self.encpfile.open()

	# we will need the current time to decide which servers to poke with
	# the soft cushions (i.e. - ping)
	ctime = time.time()

	# see which servers we need to get info from this time around
	did_some_work = 0
	did_some_encp_work = 0
	for key in self.server_keys:
	    if self.last_update.has_key(key):
	        delta = ctime - self.last_update[key]
	    else:
	        # the key was not in last_update although it was read in from
	        # the configuration file.  this means we have read in the
	        # configuration file and this is a new key, we have not checked
	        # this server before, so do it now
	        delta = self.intervals[key]

	    # see if we need to update the info on this server.  do not do it
	    # if the interval was set to NO_PING.  this 'disables' getting info on
	    # this server.  do it if either we were asked to get info on all 
	    # the servers or it has been longer than interval since we last
	    # gathered info on this server, or we were asked to get info on
            # this server specifically.
	    if self.intervals[key] != NO_PING:
	        if do_all or delta >= self.intervals[key] \
                   or self.update_request.has_key(key):
                    # clean up the update_request dict
                    if self.update_request.has_key(key):
                        del self.update_request[key]
                    
	            # time to ping this server. some keys are of the form
	            # name.real_key, so we have to get the real key to find the
	            # function to call
	            rkeyl = string.split(key, '.')
	            inq_func = "update_"+rkeyl[len(rkeyl)-1]

	            # make sure we support this type of server first
	            if InquisitorMethods.__dict__.has_key(inq_func):
	                if type(InquisitorMethods.__dict__[inq_func]) == types.FunctionType:
			    try:
				exec("self.%s(key, ctime)"%(inq_func,))
				# we just updated the server info so record the
				# current time as the last time updated.
				self.last_update[key] = ctime
			    except SystemExit, exit_code:
				raise SystemExit, exit_code
			    except:
				# there was a problem getting info from the server
				# change the color of the dispolayed info and
				# continmue with the next guy.  do not delete the
				# server from the list as the next time things may 
				# be ok. report the error too.
				e_errors.handle_error()
				self.serve_forever_error(self.log_name)
				self.htmlfile.set_alive_error_status(key)
                            # record the fact that we have done something. this
                            # will be used later to either update the html
                            # files or clean up.
	                    if inq_func == "update_encp":
	                        did_some_encp_work = 1
	                    else:
	                        did_some_work = 1
	                else:
	                    self.update_nofunc(key)
	            else:
	                # apparently we do not.
	                self.update_nofunc(key)

	# now that we are out of the above loop we can update the server dict
	# if we were asked to. we did not want to do it while doing the update
	# as we might change some intervals or servers in the list we were
	# processing
	if self.doupdate_server_dict:
	    self.update_server_dict()
	    self.doupdate_server_dict = 0
	        
	# only write the html status file if something was written to it this time
	if did_some_work:
	    self.htmlfile.write()

	# now we must close the html files and move them to themselves without
        # the suffix tacked on the end. i.e. the file becomes for example
        # inq.html not inq.html.new. only move the file if we actually did
        # something.  else, remove it.
	self.htmlfile.close()
	self.encpfile.close()
	self.move_file(did_some_work, self.htmlfile_orig)
	self.move_file(did_some_encp_work, self.encpfile_orig)

    # try to move the just written file to the displayed copy
    def move_file(self, flag, afile):
	try:
	    if flag:
                os.rename(afile+SUFFIX, afile)
	    else:
                os.remove(afile+SUFFIX)
	except OSError, msg:
            format = "%s %s %s, inquisitor update system error %s %s"%(
                timeofday.tod(), sys.argv, msg, afile, SUFFIX)
	    Trace.log(e_errors.ERROR, format)

    def handle_timeout(self):
	self.do_update(0)

    # our client said to update the enstore system status information
    def update(self, ticket):
	# if the ticket holds a server name then only update that one, else
	# update everything we know about
	if ticket.has_key(SERVER_KEYWORD):
	    if self.intervals.has_key(ticket[SERVER_KEYWORD]):
	        # mark as needing an update when call do_update
                self.update_request[ticket[SERVER_KEYWORD]] = 1
	        do_all = 0
	    else:
	        # we have no knowledge of this server, maybe it was a typo
	        ticket["status"] = (e_errors.DOESNOTEXIST, None)
		self.send_reply(ticket)
	        return
	else:
	    do_all = 1
	self.do_update(do_all)
        ticket["status"] = (e_errors.OK, None)
	self.send_reply(ticket)

    #  make all the plots
    def plot(self, ticket):
	# find out where the log files are located
	if ticket.has_key(LOGFILE_DIR):
	    lfd = ticket[LOGFILE_DIR]
	else:
	    t = self.csc.get("log_server")
	    lfd = t["log_file_path"]

        out_dir = ticket.get("out_dir", lfd)

        keep = ticket.get("keep", 0)
        pts_dir = ticket.get("keep_dir", "")
        
	self.encp_plot(ticket, lfd, keep, pts_dir, out_dir)
	self.mount_plot(ticket, lfd, keep, pts_dir, out_dir)
	ret_ticket = { 'status'   : (e_errors.OK, None) }
	self.send_reply(ret_ticket)

    def do_dump(self):
        Trace.trace(11, "last_update - %s"%(self.last_update,))
	Trace.trace(11, "last_alive - %s"%(self.last_alive,))
	Trace.trace(11, "intervals  - %s"%(self.intervals,))
	Trace.trace(11, "server_keys - %s"%(self.server_keys,))
	Trace.trace(11, "reset       - %s"%(self.reset,))
	Trace.trace(11, "htmlfile_orig - %s"%(self.htmlfile_orig,))
	Trace.trace(11, "encpfile_orig - %s"%(self.encpfile_orig,))
        print "last_update - %s"%(self.last_update,)
	print "last_alive - %s"%(self.last_alive,)
	print "intervals  - %s"%(self.intervals,)
	print "server_keys - %s"%(self.server_keys,)
	print "reset       - %s"%(self.reset,)
	print "htmlfile_orig - %s"%(self.htmlfile_orig,)
	print "encpfile_orig - %s"%(self.encpfile_orig,)
	print ""
	import pprint
	pprint.pprint(self.htmlfile.text)
	print ""

    # spill our guts
    def dump(self, ticket):
        ticket["status"] = (e_errors.OK, None)
	self.do_dump()
	self.send_reply(ticket)

    # set the select timeout
    def set_inq_timeout(self,ticket):
        ticket["status"] = (e_errors.OK, None)
        self.rcv_timeout = ticket["inq_timeout"]
	self.send_reply(ticket)

    def reset_inq_timeout(self,ticket):
        ticket["status"] = (e_errors.OK, None)
        t = self.csc.get(self.name, self.alive_rcv_timeout, self.alive_retries)
        self.rcv_timeout = t.get('timeout', default_timeout())
	self.send_reply(ticket)

    def get_inq_timeout(self, ticket):
        ret_ticket = { 'inq_timeout' : self.rcv_timeout,
                       'status'      : (e_errors.OK, None) }
	self.send_reply(ret_ticket)

    # set a new interval value.  if a server keyword was entered, set the
    # ping interval value for that server.  else, set the interval for the
    # inq server in the udp select.
    def set_interval(self,ticket):
        ticket["status"] = (e_errors.OK, None)
        if self.intervals.has_key(ticket[SERVER_KEYWORD]):
            self.intervals[ticket[SERVER_KEYWORD]] = ticket["interval"]
            self.reset[ticket[SERVER_KEYWORD]] = ticket["interval"]
	    if ticket["interval"] == NO_PING:
		# we will no longer ping this server
		self.remove_key(ticket[SERVER_KEYWORD])
        else:
            ticket["status"] = (e_errors.DOESNOTEXIST, None)
	self.send_reply(ticket)

    # reset the interval value to what was in the config file
    # if a server keyword was entered, reset the ping interval value for that
    # server.  else, reset the interval for the inq server in the udp select.
    def reset_interval(self,ticket):
        ticket["status"] = (e_errors.OK, None)
        if self.reset.has_key(ticket[SERVER_KEYWORD]):
            # delete the reset interval, so we will use the one from the
            # config file
            del self.reset[ticket[SERVER_KEYWORD]]
	self.send_reply(ticket)

    # update the inq status and exit
    def update_and_exit(self, ticket):
        ticket["status"] = (e_errors.OK, None)
	self.send_reply(ticket)
	self.update_exit(0)

    # set a new refresh value for the html files
    def set_refresh(self, ticket):
        ticket["status"] = (e_errors.OK, None)
        self.htmlfile.set_refresh(ticket['refresh'])
        self.encpfile.set_refresh(ticket['refresh'])
	self.send_reply(ticket)

    # return the current refresh value
    def get_refresh(self, ticket):
        ticket["status"] = (e_errors.OK, None)
        ticket["refresh"] = self.htmlfile.get_refresh()
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

    # get the current interval value
    # if a server keyword was entered, get the ping interval value for that
    # server.  else, get the interval for the inq server in the udp select.
    def get_interval(self, ticket):
        if self.intervals.has_key(ticket[SERVER_KEYWORD]):
            ret_ticket = {'interval' : self.intervals[ticket[SERVER_KEYWORD]],
                          SERVER_KEYWORD: ticket[SERVER_KEYWORD], 
                          'status'  : (e_errors.OK, None) }
        else:        
            ret_ticket = { 'interval' : NO_PING,
                           SERVER_KEYWORD  : ticket[SERVER_KEYWORD], 
                           'status'  : (e_errors.DOESNOTEXIST, None) }
	self.send_reply(ret_ticket)

    # make the mount plots (mounts per hour and mount latency
    def mount_plot(self, ticket, lfd, keep, pts_dir, out_dir):
	ofn = out_dir+"/mount_lines.txt"

	# parse the log files to get the media changer mount/dismount
	# information, put this info in a separate file
	# always add /dev/null to the end of the list of files to search thru 
	# so that grep always has > 1 file and will always print the name of 
	# the file at the beginning of the line.
	mountfile = enstore_files.EnMountDataFile(enstore_files.LOG_PREFIX+\
						  "* /dev/null", ofn, 
	                                "-e %s -e %s"%(Trace.MSG_MC_LOAD_REQ,
                                                       Trace.MSG_MC_LOAD_DONE),
                                                   lfd)

	# only extract the information from the newly created file that is
	# within the requested timeframe.
	mountfile.open('r')
	mountfile.timed_read(ticket)
	# now pull out the info we are going to plot from the lines
	mountfile.parse_data(ticket.get("mcs", []))
        mountfile.close()
        mountfile.cleanup(keep, pts_dir)

        # only do the plotting if we have some data
        if mountfile.data:
            # create the data files
            mphfile = enstore_plots.MphDataFile(out_dir)
            mphfile.open()
            mphfile.plot(mountfile.data)
            mphfile.close()
            mphfile.install(self.html_dir)
            mphfile.cleanup(keep, pts_dir)

            mlatfile = enstore_plots.MlatDataFile(out_dir)
            mlatfile.open()
            mlatfile.plot(mountfile.data)
            mlatfile.close()
            mlatfile.install(self.html_dir)
            mlatfile.cleanup(keep, pts_dir)

    # make the total transfers per unit of time and the bytes moved per day
    # plot
    def encp_plot(self, ticket, lfd, keep, pts_dir, out_dir):
	ofn = out_dir+"/bytes_moved.txt"

	# always add /dev/null to the end of the list of files to search thru 
	# so that grep always has > 1 file and will always print the name of 
	# the file at the beginning of the line.
	encpfile = enstore_files.EnEncpDataFile(enstore_files.LOG_PREFIX+\
						"* /dev/null",
						ofn,
						"-e %s"%(Trace.MSG_ENCP_XFER,),
						lfd)

	# only extract the information from the newly created file that is
	# within the requested timeframe.
	encpfile.open('r')
	encpfile.timed_read(ticket)
	# now pull out the info we are going to plot from the lines
	encpfile.parse_data(ticket.get("mcs", []))
        encpfile.close()
        encpfile.cleanup(keep, pts_dir)

        # only do the plotting if we have some data
        if encpfile.data:
            bpdfile = enstore_plots.BpdDataFile(out_dir)
            bpdfile.open()
            bpdfile.plot(encpfile.data)
            bpdfile.close()
            bpdfile.install(self.html_dir)

            xferfile = enstore_plots.XferDataFile(out_dir, bpdfile.ptsfile)
            xferfile.open()
            xferfile.plot(encpfile.data)
            xferfile.close()
            xferfile.install(self.html_dir)

            # delete any extraneous files. do it here because the xfer file
            # plotting needs the bpd data file
            bpdfile.cleanup(keep, pts_dir)
            xferfile.cleanup(keep, pts_dir)

class Inquisitor(InquisitorMethods, generic_server.GenericServer):

    def __init__(self, csc, timeout=-1, html_file="", alive_rcv_to=-1, 
		 alive_retries=-1, max_encp_lines=-1, refresh=-1):
        generic_server.GenericServer.__init__(self, csc, MY_NAME)
        Trace.init(self.log_name)
	self.name = MY_NAME
	# set an interval and retry that we will use the first time to get the
	# inquisitor information from the config server.  we do not use the
	# passed values because they might have been defaulted and we need to
	# look them up in the config file which we have not gotten yet.
	use_once_timeout = 5
	use_once_retry = 1
	keys = self.csc.get(self.name, use_once_timeout, use_once_retry)
	dispatching_worker.DispatchingWorker.__init__(self, (keys['hostip'], 
	                                              keys['port']))

	# initialize
	self.doupdate_server_dict = 0
	self.reset = {}
        self.update_request = {}
        self.forked = {}
        self.intervals = {}
        self.cmds_to_do = keys.get('update_commands', {})
        # if no timeout was entered on the command line, get it from the 
        # configuration file.  this variable is used in dispatching worker to
        # set how often the udp select times out.
        if timeout == -1:
            self.rcv_timeout = keys.get('timeout', default_timeout())
        else:
            self.rcv_timeout = timeout

	# if no alive timeout was entered on the command line, get it from the 
	# configuration file.
	if alive_rcv_to == -1:
            self.alive_rcv_timeout = keys.get('alive_rcv_timeout',
                                              default_alive_rcv_timeout())
	else:
	    self.alive_rcv_timeout = alive_rcv_to

	# if no alive retry # was entered on the command line, get it from the 
	# configuration file.
	if alive_retries == -1:
            self.alive_retries = keys.get('alive_retries',
                                          default_alive_retries())
	else:
	    self.alive_retries = alive_retries

	# if no max number of encp lines was entered on the command line, get 
	# it from the configuration file.
	if max_encp_lines == -1:
            self.max_encp_lines = keys.get('max_encp_lines',
                                           default_max_encp_lines())
	else:
	    self.max_encp_lines = max_encp_lines

	# get the directory where the files we create will go.  this should
	# be in the configuration file.
	if not html_file:
            if keys.has_key('html_file'):
	        self.html_dir = keys['html_file']
	        html_file = self.html_dir+"/"+\
                            enstore_files.status_html_file_name()
	        encp_file = self.html_dir+"/"+\
                            enstore_files.encp_html_file_name()
	        config_file = self.html_dir+"/"+\
                            enstore_files.config_html_file_name()
	        misc_file = self.html_dir+"/"+\
                            enstore_files.misc_html_file_name()
		plot_file = self.html_dir+"/"+\
                            enstore_files.plot_html_file_name()
	    else:
	        self.html_dir = enstore_files.default_dir
	        html_file = enstore_files.default_status_html_file()
	        encp_file = enstore_files.default_encp_html_file()
	        config_file = enstore_files.default_config_html_file()
	        misc_file = enstore_files.default_misc_html_file()
		plot_file = enstore_files.default_plot_html_file()

        # if no html refresh was entered on the command line, get it from
        # the configuration file.
        if refresh == -1:
            refresh = keys.get('refresh', DEFAULT_REFRESH)

	# add a suffix to the html file because we will write to this file and 
	# maintain another copy of the file (with the user entered name) to
	# be displayed.
	self.htmlfile = enstore_files.HTMLStatusFile(html_file+SUFFIX,refresh)
	self.htmlfile_orig = html_file
	self.encpfile = enstore_files.HTMLEncpStatusFile(encp_file+SUFFIX,refresh)
	self.encpfile_orig = encp_file
        self.loghtmlfile = enstore_files.HTMLLogFile(self.logc.log_dir+"/"+\
						     LOGHTMLFILE_NAME+SUFFIX)
        self.confightmlfile = enstore_files.HTMLConfigFile(config_file+SUFFIX)
        self.confightmlfile_orig = config_file
        self.mischtmlfile = enstore_files.HTMLMiscFile(misc_file+SUFFIX)
        self.mischtmlfile_orig = misc_file
	self.plothtmlfile = enstore_files.HTMLPlotFile(plot_file+SUFFIX)
        self.plothtmlfile_orig = plot_file
	self.patrolhtmlfile = enstore_files.HTMLPatrolFile("%s/%s"%(self.html_dir, PATROL_FILE))

	# get the interval for each of the servers from the configuration file.
	self.last_update = {}
	self.last_alive = {}
	if keys.has_key('intervals'):
	    self.intervals = keys['intervals']
	    # this dict records the last time that the associated server
	    # info was updated. everytime we get a particular servers' info we
	    # will update this time. start out at 0 so we do an update right
	    # away
	    for key in self.intervals.keys():
	        self.last_update[key] = 0

	# now we must look thru the whole config file and use the default
	# server interval for any servers that were not included in the
	# 'intervals' dict element
        self.default_server_interval = keys.get(DEFAULT_SERVER_INTERVAL,
                                               default_server_interval())
	self.fill_in_default_intervals(0)

	# get a file clerk client, volume clerk client.
	# connections to library manager client(s), media changer client(s)
	# and a connection to the movers will be gotten dynamically.
        # a log client, and alarm client were already gotten in the generic
	# server init.  these will be used to get the status
	# information from the servers. we do not need to pass a host and port
	# to the class instantiators because we are giving them a configuration
	# client and they do not need to connect to the configuration server.
	self.fcc = file_clerk_client.FileClient(self.csc)
	self.vcc = volume_clerk_client.VolumeClerkClient(self.csc)

	# get all the servers we are to keep tabs on
	self.prepare_keys()

	# set up a signal handler to catch termination signals (SIGKILL) so we can
	# update our status before dying
	signal.signal(signal.SIGTERM, self.s_update_exit)

class InquisitorInterface(generic_server.GenericServerInterface):

    def __init__(self):
	# fill in the defaults for possible options
	self.html_file = ""
	self.alive_rcv_timeout = -1
	self.alive_retries = -1
	self.inq_timeout = -1
	self.max_encp_lines = -1
	self.refresh = -1
	generic_server.GenericServerInterface.__init__(self)

    # define the command line options that are valid
    def options(self):
	return generic_server.GenericServerInterface.options(self)+\
	       ["html_file=","inq_timeout=", "max_encp_lines=", "refresh="]+\
	       self.alive_rcv_options()

if __name__ == "__main__":
    Trace.init(string.upper(MY_NAME))
    Trace.trace(6,"inquisitor called with args "+repr(sys.argv))

    # get interface
    intf = InquisitorInterface()

    # get the inquisitor
    inq = Inquisitor((intf.config_host, intf.config_port), 
                     intf.inq_timeout,intf.html_file,
                     intf.alive_rcv_timeout, intf.alive_retries,
	             intf.max_encp_lines,intf.refresh)
    # we no longer need the interface
    del intf

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
