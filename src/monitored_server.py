import time
import threading
import os

import enstore_constants
import enstore_functions
import mover_client
import library_manager_client
import e_errors
import Trace

DEFAULT_ALIVE_INTERVAL = 30
NO_HEARTBEAT = -1
DEFAULT_HUNG_INTERVAL = 90
DEFAULT_MOVER_HUNG_INTERVAL = 600
NO_TIMEOUT = 0
HUNG = 1
TIMEDOUT = 2
CONFIG = "config"

# thread states
FINISHED = 1
ACTIVE = 2

DIVIDER = "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"

def hack_restart_function(name, host):
    Trace.log(e_errors.WARNING, "running dbhang for %s on %s"%(name, host))
    os.system("enrsh -n %s '/home/enstore/dbhang %s'"%(host, name))

# this is really a hack, do this right when i am back full time on enstore
def hack_restart(name, host):
    restart_thread = threading.Thread(group=None,
				      target=hack_restart_function,
				      name="HACK_RESTART_%s"%(name,),
				      args=(name, host))
    restart_thread.setDaemon(1)
    restart_thread.start()

# get the alive_interval from the server or the default from the inquisitor
def get_alive_interval(csc, name, config={}):
    if not config:
	config = csc.get(name)
    alive_interval = config.get(enstore_constants.ALIVE_INTERVAL, None)
    if not alive_interval:
	# see if the default is in the inquisitor config dict
	iconfig = csc.get(enstore_constants.INQUISITOR)
	alive_interval = iconfig.get(enstore_constants.DEFAULT_ALIVE_INTERVAL,
				     DEFAULT_ALIVE_INTERVAL)
    return alive_interval

class MonitoredServer:

    def update_alive_interval(self):
	if not self.config.has_key(enstore_constants.ALIVE_INTERVAL):
	    self.alive_interval = DEFAULT_ALIVE_INTERVAL
	else:
	    self.alive_interval = self.config[enstore_constants.ALIVE_INTERVAL]
	self.twice_alive_interval = self.alive_interval + self.alive_interval

    def __init__(self, config, name, hung_interval=None):
	self.name = name
	# set this to now because we will check this before any of the servers 
	# heartbeats have been forwarded to us
	self.last_alive = enstore_constants.NEVER_ALIVE  # last time server was alive
	self.output_last_alive = enstore_constants.NEVER_ALIVE  # last time server was alive
	self.restart_thread = None          # thread id if trying to restart server
	#self.status_thread = None           # thread id if getting status from server
	self.config = config                # config file dictionary for this server
	if hung_interval is None:
	    hung_interval = DEFAULT_HUNG_INTERVAL
	self.hung_interval = hung_interval  # wait this long if server appears hung
	self.start_time = time.time()
	self.restart_failed = 0
	self.did_restart_alarm = 0
        self.state = NO_TIMEOUT
	self.server_status = None
	self.update_alive_interval()

    def __getattr__(self, attr):
	if attr[:2]=='__':
	    raise AttributeError, attr
	else:
	    return self.__dict__[CONFIG].get(attr, None)

    def __setattr__(self, attr, value):
	if self.__dict__.has_key(CONFIG) and self.__dict__[CONFIG].has_key(attr):
	    self.__dict__[CONFIG][attr] = value
	else:
	    self.__dict__[attr] = value

    def check_recent_alive(self, event_relay):
	if self.alive_interval == NO_HEARTBEAT:
	    # fake that everything is ok as we are not checking this server anyway
	    rtn = NO_TIMEOUT
	else:
	    now = time.time()
	    if self.last_alive == enstore_constants.NEVER_ALIVE:
		past_interval = now - self.start_time
	    else:
		past_interval = now - self.last_alive

	    if past_interval > self.hung_interval:
		# we can only determine REALLY if we are hung, if we know that the
		# event relay is still alive.  determine that first
		enstore_functions.inqTrace(enstore_constants.INQSERVERTIMESDBG,
		    "%s Past Interval: %s, Hung Interval: %s, ER Alive: %s"%(self.name, 
									     past_interval,
									     self.hung_interval,
									     event_relay.is_alive()))
		if event_relay.is_alive() and (past_interval > event_relay.heartbeat + self.alive_interval + \
					       self.hung_interval):
		    rtn = HUNG
		else:
		    rtn = TIMEDOUT
	    elif past_interval > self.twice_alive_interval:
		rtn = TIMEDOUT
	    else:
		rtn = NO_TIMEOUT
        self.state = rtn
	return rtn

    def is_alive(self):
	self.last_alive = time.time()
	self.output_last_alive = self.last_alive
	self.restart_failed = 0

    def cant_restart(self):
	self.restart_failed = 1

    def delete_me(self):
	self.delete = 1

    def no_thread(self):
	return not (self.restart_thread and self.restart_thread.isAlive())

    def restart_thread_state(self):
	if self.no_thread():
	    # there is no thread
	    return None
	elif self.restart_thread:
	    # there is one but it is not alive
	    return FINISHED
	else:
	    return ACTIVE

    def update_config(self, new_config):
	self.config = new_config
	self.update_alive_interval()

    def __repr__(self):
	import pprint
	return "%s : %s\n%s"%(self.name, pprint.pformat(self.__dict__), DIVIDER)

    def do_hack_restart(self):
	# this should be overridden by the servers that care
	pass

class MonitoredInquisitor(MonitoredServer):

    def update_default_alive_interval(self, config):
	global DEFAULT_ALIVE_INTERVAL, DEFAULT_HUNG_INTERVAL
	DEFAULT_ALIVE_INTERVAL = config.get('default_alive_interval', 
					    DEFAULT_ALIVE_INTERVAL)
	DEFAULT_HUNG_INTERVAL = config.get('default_hung_interval', 
					    DEFAULT_HUNG_INTERVAL)

    def update_config(self, new_config):
	self.update_default_alive_interval(new_config)
	MonitoredServer.update_config(self, new_config)

    def get_hung_interval(self, server_name, config=None):
	if config is None:
	    config = self.config
	return(config.get("hung_intervals", {}).get(server_name, 
						   DEFAULT_HUNG_INTERVAL))

    def __init__(self, config):
	self.update_default_alive_interval(config)
	MonitoredServer.__init__(self, config, enstore_constants.INQUISITOR)


class MonitoredAlarmServer(MonitoredServer):

    def __init__(self, config):
	MonitoredServer.__init__(self, config, enstore_constants.ALARM_SERVER)


class MonitoredLogServer(MonitoredServer):

    def __init__(self, config):
	MonitoredServer.__init__(self, config, enstore_constants.LOG_SERVER)


class MonitoredFileClerk(MonitoredServer):

    def __init__(self, config):
	MonitoredServer.__init__(self, config, enstore_constants.FILE_CLERK)

    def do_hack_restart(self):
	hack_restart(self.name, self.host)

class MonitoredVolumeClerk(MonitoredServer):

    def __init__(self, config):
	MonitoredServer.__init__(self, config, enstore_constants.VOLUME_CLERK)

    def do_hack_restart(self):
	hack_restart(self.name, self.host)

class MonitoredConfigServer(MonitoredServer):

    def update_config(self, new_config):
	if not new_config:
	    self.config = enstore_functions.get_config_server_info()
	else:
	    self.config = new_config

    def __init__(self, config):
	MonitoredServer.__init__(self, config, enstore_constants.CONFIG_SERVER)
	self.update_config(config)


class MonitoredMover(MonitoredServer):

    def __init__(self, config, name, csc):
	MonitoredServer.__init__(self, config, name, DEFAULT_MOVER_HUNG_INTERVAL)
	self.csc = csc
	self.client = mover_client.MoverClient(self.csc, self.name)


class MonitoredMediaChanger(MonitoredServer):

    pass

class MonitoredLibraryManager(MonitoredServer):

    def __init__(self, config, name, csc):
	MonitoredServer.__init__(self, config, name)
	self.time_bad = 0
	self.csc = csc
	self.client = library_manager_client.LibraryManagerClient(self.csc, self.name)
