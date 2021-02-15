from future.utils import raise_
import time
import threading
import os
import string

import enstore_constants
import enstore_functions
import migrator_client
import library_manager_client
import e_errors
import Trace

DEFAULT_ALIVE_INTERVAL = 30
NO_HEARTBEAT = -1
DEFAULT_HUNG_INTERVAL = 90
DEFAULT_MOVER_HUNG_INTERVAL = 600
DEFAULT_MIGRATOR_HUNG_INTERVAL = 600
NO_TIMEOUT = 0
HUNG = 1
TIMEDOUT = 2
CONFIG = "config"

# thread states
FINISHED = 1
ACTIVE = 2

DIVIDER = "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
DASH = "-"


def hack_restart_function(name, host):
    Trace.log(e_errors.WARNING, "running dbhang for %s on %s" % (name, host))
    os.system("enrsh -n %s '/home/enstore/dbhang %s'" % (host, name))

# this is really a hack, do this right when i am back full time on enstore


def hack_restart(name, host):
    restart_thread = threading.Thread(group=None,
                                      target=hack_restart_function,
                                      name="HACK_RESTART_%s" % (name,),
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
        if enstore_constants.ALIVE_INTERVAL not in self.config:
            self.alive_interval = DEFAULT_ALIVE_INTERVAL
        else:
            self.alive_interval = self.config[enstore_constants.ALIVE_INTERVAL]
        self.twice_alive_interval = self.alive_interval + self.alive_interval

    STATUS_FIELDS = {}

    def __init__(self, config, name, hung_interval=None):
        self.name = name
        # set this to now because we will check this before any of the servers
        # heartbeats have been forwarded to us
        self.last_alive = enstore_constants.NEVER_ALIVE  # last time server was alive
        self.output_last_alive = enstore_constants.NEVER_ALIVE  # last time server was alive
        self.restart_thread = None          # thread id if trying to restart server
        # self.status_thread = None           # thread id if getting status
        # from server
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
        self.status_keys = []

    def __getattr__(self, attr):
        if attr[:2] == '__':
            raise_(AttributeError, attr)
        else:
            return self.__dict__[CONFIG].get(attr, None)

    def __setattr__(self, attr, value):
        if CONFIG in self.__dict__ and attr in self.__dict__[CONFIG]:
            self.__dict__[CONFIG][attr] = value
        else:
            self.__dict__[attr] = value

    def check_status_ticket(self, status):
        # make sure this ticket has all of the fields we need
        for key in self.status_keys:
            if key not in status:
                status[key] = self.STATUS_FIELDS[key]

    def last_heartbeat(self):
        if self.alive_interval == NO_HEARTBEAT:
            # we are not watching this server
            rtn = None
        elif self.last_alive == enstore_constants.NEVER_ALIVE:
            # this server was never seen alive
            rtn = None
        else:
            rtn = self.last_alive
        return rtn

    def check_recent_alive(self, event_relay):
        if self.alive_interval == NO_HEARTBEAT:
            # fake that everything is ok as we are not checking this server
            # anyway
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
                                           "%s Past Interval: %s, Hung Interval: %s, ER Alive: %s" % (self.name,
                                                                                                      past_interval,
                                                                                                      self.hung_interval,
                                                                                                      event_relay.is_alive()))
                if event_relay.is_alive() and (past_interval >
                                               event_relay.heartbeat + self.alive_interval + self.hung_interval):
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
        return "%s : %s\n%s" % (
            self.name, pprint.pformat(self.__dict__), DIVIDER)

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


class MonitoredAccountingServer(MonitoredServer):

    def __init__(self, config):
        MonitoredServer.__init__(
            self, config, enstore_constants.ACCOUNTING_SERVER)


class MonitoredAlarmServer(MonitoredServer):

    def __init__(self, config):
        MonitoredServer.__init__(self, config, enstore_constants.ALARM_SERVER)


class MonitoredDrivestatServer(MonitoredServer):

    def __init__(self, config):
        MonitoredServer.__init__(
            self, config, enstore_constants.DRIVESTAT_SERVER)


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


class MonitoredInfoServer(MonitoredServer):

    def __init__(self, config):
        MonitoredServer.__init__(self, config, enstore_constants.INFO_SERVER)


class MonitoredConfigServer(MonitoredServer):

    def update_config(self, new_config):
        if not new_config:
            self.config = enstore_functions.get_config_server_info()
        else:
            self.config = new_config

    def __init__(self, config):
        MonitoredServer.__init__(self, config, enstore_constants.CONFIG_SERVER)
        self.update_config(config)


class MonitoredRatekeeper(MonitoredServer):

    def __init__(self, config):
        MonitoredServer.__init__(self, config, enstore_constants.RATEKEEPER)

# Monitored Library Manager Director


class MonitoredLMD(MonitoredServer):

    def __init__(self, config):
        MonitoredServer.__init__(self, config, enstore_constants.LM_DIRECTOR)


# Monitored Policy Engine Server and Migration Dispatcher
class MonitoredDispatcher(MonitoredServer):

    def __init__(self, config):
        MonitoredServer.__init__(self, config, enstore_constants.DISPATCHER)


class MonitoredMigrator(MonitoredServer):

    STATUS_FIELDS = {enstore_constants.STATE: (enstore_constants.UNKNOWN_S, enstore_constants.UNKNOWN_S),  # state, internal_state
                     enstore_constants.TRANSFERS_COMPLETED: DASH,
                     enstore_constants.TRANSFERS_FAILED: DASH,
                     enstore_constants.FILES: "",
                     enstore_constants.MODE: "",
                     enstore_constants.ID: "",
                     enstore_constants.STATUS: ()}

    def __init__(self, config, name, csc):
        MonitoredServer.__init__(
            self, config, name, DEFAULT_MIGRATOR_HUNG_INTERVAL)
        self.csc = csc
        self.status_keys = self.STATUS_FIELDS.keys()


class MonitoredMover(MonitoredServer):

    STATUS_FIELDS = {enstore_constants.STATE: enstore_constants.UNKNOWN_S,
                     enstore_constants.TRANSFERS_COMPLETED: DASH,
                     enstore_constants.TRANSFERS_FAILED: DASH,
                     enstore_constants.BYTES_READ: "-1",
                     enstore_constants.BYTES_WRITTEN: "-1",
                     enstore_constants.FILES: ("", ""),
                     enstore_constants.CURRENT_VOLUME: "",
                     enstore_constants.MODE: "",
                     enstore_constants.BYTES_TO_TRANSFER: "-1",
                     enstore_constants.CURRENT_LOCATION: "0",
                     enstore_constants.LAST_VOLUME: "",
                     enstore_constants.LAST_LOCATION: "0",
                     enstore_constants.STATUS: ()}

    def __init__(self, config, name, csc):
        MonitoredServer.__init__(
            self, config, name, DEFAULT_MOVER_HUNG_INTERVAL)
        self.csc = csc
        self.status_keys = self.STATUS_FIELDS.keys()


class MonitoredMediaChanger(MonitoredServer):

    pass


class MonitoredUDPProxyServer(MonitoredServer):

    pass


class MonitoredLibraryManager(MonitoredServer):

    STATUS_FIELDS = {enstore_constants.STATE: enstore_constants.UNKNOWN_S,
                     enstore_constants.SUSPECT_VOLUMES: [],
                     enstore_constants.MOVERS: [],
                     enstore_constants.ATMOVERS: [],
                     enstore_constants.PENDING_WORKS: {}}

    def __init__(self, config, name, csc):
        MonitoredServer.__init__(self, config, name)
        self.time_bad = 0
        self.csc = csc
        self.ff_stalled = {}
        self.stalled_time = 1800
        self.client = library_manager_client.LibraryManagerClient(
            self.csc, self.name)
        self.status_keys = self.STATUS_FIELDS.keys()

    def check_state(self, status):
        # make sure this ticket has all of the fields we need
        if enstore_constants.STATE not in status:
            status[enstore_constants.STATE] = self.STATUS_FIELDS[enstore_constants.STATE]

    def check_suspect_vols(self, status):
        # make sure this ticket has all of the fields we need
        if enstore_constants.SUSPECT_VOLUMES not in status:
            status[enstore_constants.SUSPECT_VOLUMES] = self.STATUS_FIELDS[enstore_constants.SUSPECT_VOLUMES]

    def check_active_vols(self, status):
        # make sure this ticket has all of the fields we need
        if enstore_constants.MOVERS not in status:
            status[enstore_constants.MOVERS] = self.STATUS_FIELDS[enstore_constants.MOVERS]

    def check_work_queue(self, status):
        # make sure this ticket has all of the fields we need
        if enstore_constants.ATMOVERS not in status:
            status[enstore_constants.ATMOVERS] = self.STATUS_FIELDS[enstore_constants.ATMOVERS]
        if enstore_constants.PENDING_WORKS not in status:
            status[enstore_constants.PENDING_WORKS] = self.STATUS_FIELDS[enstore_constants.PENDING_WORKS]

    def get_stalled_key(self, node, ff):
        return "%s,%s" % (node, ff)

    def split_stalled_key(self, key):
        return string.split(key, ',', 1)

    # determine if the queue has been stalled for a long time. since it may be in a transitionsl
    # state, we do not want to alarm every time there is discrepancy, only after several
    # continuous discrepansies.
    def is_really_stalled(self, node, ff):
        rtn = 0
        key = self.get_stalled_key(node, ff)
        if key in self.ff_stalled:
            # see if the time is long enough to warrant an alarm
            if time.time() - self.ff_stalled[key] > self.stalled_time:
                rtn = 1
        else:
            # not a stall yet, keep track of it though
            self.ff_stalled[key] = time.time()
        return rtn

    # there is no stall for the passed in file_family
    def no_stall(self, node, ff=""):
        if ff:
            key = self.get_stalled_key(node, ff)
            if key in self.ff_stalled:
                del(self.ff_stalled[key])
        else:
            # delete all the keys for this node
            keys = self.ff_stalled.keys()
            for key in keys:
                stalled_node, ff = self.split_stalled_key(key)
                if node == stalled_node:
                    del(self.ff_stalled[key])
