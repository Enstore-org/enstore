import time

import enstore_constants
import enstore_functions

DEFAULT_ALIVE_INTERVAL = 30
NO_HEARTBEAT = -1
DEFAULT_HUNG_INTERVAL = 90
NO_TIMEOUT = 0
HUNG = 1
TIMEDOUT = 2
CONFIG = "config"

# thread states
FINISHED = 1
ACTIVE = 2

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

    def __init__(self, config, name, hung_interval=DEFAULT_HUNG_INTERVAL):
	self.name = name
	# set this to now because we will check this before any of the servers 
	# heartbeats have been forwarded to us
	self.last_alive = time.time()       # last time the server was found alive
	self.restart_thread = None          # thread id if trying to restart server
	self.config = config                # config file dictionary for this server
	self.hung_interval = hung_interval  # wait this long if server appears hung
	if not self.config.has_key(enstore_constants.ALIVE_INTERVAL):
	    self.alive_interval = DEFAULT_ALIVE_INTERVAL
	else:
	    self.alive_interval = self.config[enstore_constants.ALIVE_INTERVAL]
	self.twice_alive_interval = self.alive_interval + self.alive_interval

    def __getattr__(self, name):
	return self.__dict__[CONFIG].get(name, None)

    def __setattr__(self, name, value):
	if self.__dict__.has_key(CONFIG) and self.__dict__[CONFIG].has_key(name):
	    self.__dict__[CONFIG][name] = value
	else:
	    self.__dict__[name] = value

    def check_recent_alive(self):
	if self.alive_interval == NO_HEARTBEAT:
	    # fake that everything is ok as we are not checking this server anyway
	    rtn = NO_TIMEOUT
	else:
	    past_interval = time.time() - self.last_alive
	    if past_interval > self.hung_interval:
		rtn = HUNG
	    elif past_interval > self.twice_alive_interval:
		rtn = TIMEDOUT
	    else:
		rtn = NO_TIMEOUT
	return rtn

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

    def __repr__(self):
	import pprint
	print self.name+" : ",
	pprint.pprint(self.__dict__)
	print "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"

class MonitoredInquisitor(MonitoredServer):

    def get_hung_interval(self, server_name):
	return(self.config.get("hung_interval", {}).get(server_name, 
							DEFAULT_HUNG_INTERVAL))

    def __init__(self, config):
	global DEFAULT_ALIVE_INTERVAL
	DEFAULT_ALIVE_INTERVAL = config.get('default_alive_interval', 
					    DEFAULT_ALIVE_INTERVAL)
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


class MonitoredVolumeClerk(MonitoredServer):

    def __init__(self, config):
	MonitoredServer.__init__(self, config, enstore_constants.VOLUME_CLERK)


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

    pass

class MonitoredMediaChanger(MonitoredServer):

    pass

class MonitoredLibraryManager(MonitoredServer):

    pass
