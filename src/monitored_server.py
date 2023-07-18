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
    """
    Restart a server by calling dbhang on supplied name/host. This is intended
    to be a short-term solution for restarting servers. However, the dbhang
    utility doesn't seem to exist on production infrastructure.

    Args:
        name: Name of server to restart
        host: Name of machine on which to perform restart
    """
    Trace.log(e_errors.WARNING, "running dbhang for %s on %s" % (name, host))
    # home/enstore/dbhang doesn't seem to really exist, even on clerks
    os.system("enrsh -n %s '/home/enstore/dbhang %s'" % (host, name))


# this is really a hack, do this right when I am back full time on enstore
def hack_restart(name, host):
    """
    Restart a remote server at supplied name/host. This is intended
    to be a short-term solution for restarting servers. Dispatches
    restart process in a thread.

    Args:
        name: Name of server to restart
        host: Name of machine on which to perform restart
    """
    restart_thread = threading.Thread(group=None,
                                      target=hack_restart_function,
                                      name="HACK_RESTART_%s" % (name,),
                                      args=(name, host))
    restart_thread.setDaemon(True)
    restart_thread.start()


# get the alive_interval from the server or the default from the inquisitor
def get_alive_interval(csc, _, __=None):
    """
    Get alive interval to be used for monitored servers.

    Args:
        csc: Configuration Server Client
        _: unused
        __: unused, default: None
    """
    alive_interval = config.get(enstore_constants.ALIVE_INTERVAL, None)
    if not alive_interval:
        # see if the default is in the inquisitor config dict
        iconfig = csc.get(enstore_constants.INQUISITOR)
        alive_interval = iconfig.get(enstore_constants.DEFAULT_ALIVE_INTERVAL,
                                     DEFAULT_ALIVE_INTERVAL)
    return alive_interval


class MonitoredServer:
    """
    Superclass for all monitored server types. Provides basic heartbeat
    functionality and hooks for restarting.
    """

    def update_alive_interval(self):
        """
        Set alive interval to value in config, or default. This can change
        when the config is updated.
        """
        if enstore_constants.ALIVE_INTERVAL not in self.config:
            self.alive_interval = DEFAULT_ALIVE_INTERVAL
        else:
            self.alive_interval = self.config[enstore_constants.ALIVE_INTERVAL]
        self.twice_alive_interval = self.alive_interval + self.alive_interval

    STATUS_FIELDS = {}

    def __init__(self, config, name, hung_interval=None):
        """
        Initialize values. Last alive are set to special case values from
        enstore_constants to signify we have not yet received a heartbeat.

        Args:
            config (dict): Initial config dictionary.
            name (str): Name of server to be monitored.
            hung_interval (int): How long, in seconds, to wait before declaring
                the server 'hung' (default: None).
        """
        self.delete = None
        self.alive_interval = None
        self.twice_alive_interval = None
        self.name = name
        self.last_alive = enstore_constants.NEVER_ALIVE  # last time server was alive
        self.output_last_alive = enstore_constants.NEVER_ALIVE  # last time server was alive
        self.restart_thread = None  # thread id if trying to restart server
        self.config = config  # config file dictionary for this server
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
        """
        Modified getattr that gets information from CONFIG dict rather
        than object directly.

        Args:
            attr (str): Attribute to get from config dict
        """
        if attr[:2] == '__':
            raise AttributeError(attr)
        else:
            return self.__dict__[CONFIG].get(attr, None)

    def __setattr__(self, attr, value):
        """
        Modified setattr that updates CONFIG dict rather
        than object directly.

        Args:
            attr (str): Attribute to set in config dict
            value (Object): Value to set
        """
        if CONFIG in self.__dict__ and attr in self.__dict__[CONFIG]:
            self.__dict__[CONFIG][attr] = value
        else:
            self.__dict__[attr] = value

    def check_status_ticket(self, status):
        """
        Fill in any missing fields of status ticket with defaults from
        STATUS_FIELDS.

        Args:
            status (dict): Ticket for which necessary fields should be filled.
        """
        # make sure this ticket has all the fields we need
        for key in self.status_keys:
            if key not in status:
                status[key] = self.STATUS_FIELDS[key]

    def last_heartbeat(self):
        """
        Return last response to a heartbeat received from this server.

        Returns:
            (int or None): Last heartbeat time or None if we are not monitoring
                this server, or it has not yet responded to a heartbeat.
        """
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
        """
        Check to see if this server appears to be hung or timing out based on
        the time since its most recent heartbeat response.

        Args:
            event_relay (inquisitor.EventRelay): Event Relay for the Enstore
                deployment. Event Relay issues can cause a server to fail to
                respond to health checks, so this is used to rule out such
                cases.

        Returns:
            (int): HUNG, TIMEDOUT, or NO_TIMEOUT - server appears to be hung,
                otherwise failing to respond to heartbeats, or OK,
                respectively.
        """
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
                                           "%s Past Interval: %s, Hung Interval: %s, ER Alive: %s" %
                                           (self.name, past_interval, self.hung_interval, event_relay.is_alive()))
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
        """
        Update last_alive time to now, and reset restart_failed.
        """
        self.last_alive = time.time()
        self.output_last_alive = self.last_alive
        self.restart_failed = 0

    def cant_restart(self):
        """
        Set restart_failed.
        """
        self.restart_failed = 1

    def delete_me(self):
        """
        Set signal to delete this server from monitored server set.
        """
        self.delete = 1

    def no_thread(self):
        """
        Return whether there is an active thread attempting to restart this
        server.

        Returns:
            (bool): Whether there is an active restart thread.
        """
        return not (self.restart_thread and self.restart_thread.isAlive())

    def restart_thread_state(self):
        """
        Get state of restart thread for this server.

        Returns:
            (int or None): FINISHED, ACTIVE, or None if no thread exists.
        """
        if self.no_thread():
            # there is no thread
            return None
        elif self.restart_thread:
            # there is one but it is not alive
            return FINISHED
        else:
            return ACTIVE

    def update_config(self, new_config):
        """
        Set config dict to provided arg value.

        Args:
            new_config (dict): Dictionary to set config to.
        """
        self.config = new_config
        self.update_alive_interval()

    def __repr__(self):
        """
        Modified repr that uses pprint to provide a better looking string
        representation of this object.

        Returns:
            (str): Well-formatted representation of this object.
        """
        import pprint
        return "%s : %s\n%s" % (self.name, pprint.pformat(self.__dict__), DIVIDER)

    def do_hack_restart(self):
        """
        Function to actually perform restart of this server. This should be
        overridden by servers for which calling hack_restart may be necessary.
        """
        pass


def update_default_alive_interval(config):
    """
    Update default alive and hung intervals based on values in supplied config.

    Args:
        config (dict): Dictionary containing new values for
            default_alive_interval and default_hung_interval.
    """
    global DEFAULT_ALIVE_INTERVAL, DEFAULT_HUNG_INTERVAL
    DEFAULT_ALIVE_INTERVAL = config.get('default_alive_interval',
                                        DEFAULT_ALIVE_INTERVAL)
    DEFAULT_HUNG_INTERVAL = config.get('default_hung_interval',
                                       DEFAULT_HUNG_INTERVAL)


class MonitoredInquisitor(MonitoredServer):
    """Implementation of MonitoredServer for the Inquisitor."""

    def update_config(self, new_config):
        """
        Update config for this inquisitor, including updating the default alive
        intervals for all monitored servers.

        Args:
            new_config (dict): New config dictionary, including values for
                default_alive_interval and default_hung_interval.
        """
        update_default_alive_interval(new_config)
        MonitoredServer.update_config(self, new_config)

    def get_hung_interval(self, server_name, config=None):
        """
        Get current hung interval for particular server.

        Args:
            server_name (str): Server to request hung_interval for.
            config (dict): Config in which to check for hung_interval.

        Returns:
            (int): Hung interval found in config or DEFAULT_HUNG_INTERVAL.
        """
        if config is None:
            config = self.config
        return (config.get("hung_intervals", {}).get(server_name,
                                                     DEFAULT_HUNG_INTERVAL))

    def __init__(self, config):
        """
        Set default alive values, call Super init.

         Args:
            new_config (dict): Initial config dictionary, including values for
                default_alive_interval and default_hung_interval.
        """
        update_default_alive_interval(config)
        MonitoredServer.__init__(self, config, enstore_constants.INQUISITOR)


class MonitoredAccountingServer(MonitoredServer):
    """Implementation of MonitoredServer for the Accounting Server."""

    def __init__(self, config):
        MonitoredServer.__init__(self, config, enstore_constants.ACCOUNTING_SERVER)


class MonitoredAlarmServer(MonitoredServer):
    """Implementation of MonitoredServer for the Alarm Server."""

    def __init__(self, config):
        MonitoredServer.__init__(self, config, enstore_constants.ALARM_SERVER)


class MonitoredDrivestatServer(MonitoredServer):
    """Implementation of MonitoredServer for the Drivestat Server."""

    def __init__(self, config):
        MonitoredServer.__init__(self, config, enstore_constants.DRIVESTAT_SERVER)


class MonitoredLogServer(MonitoredServer):
    """Implementation of MonitoredServer for the Log Server."""

    def __init__(self, config):
        MonitoredServer.__init__(self, config, enstore_constants.LOG_SERVER)


class MonitoredFileClerk(MonitoredServer):
    """Implementation of MonitoredServer for the File Clerk."""

    def __init__(self, config):
        MonitoredServer.__init__(self, config, enstore_constants.FILE_CLERK)

    def do_hack_restart(self):
        """Perform hack_restart for this server's name/host."""
        hack_restart(self.name, self.host)


class MonitoredVolumeClerk(MonitoredServer):
    """Implementation of MonitoredServer for the Volume Clerk."""

    def __init__(self, config):
        MonitoredServer.__init__(self, config, enstore_constants.VOLUME_CLERK)

    def do_hack_restart(self):
        """Perform hack_restart for this server's name/host."""
        hack_restart(self.name, self.host)


class MonitoredInfoServer(MonitoredServer):
    """Implementation of MonitoredServer for the Info Server."""

    def __init__(self, config):
        MonitoredServer.__init__(self, config, enstore_constants.INFO_SERVER)


class MonitoredConfigServer(MonitoredServer):
    """Implementation of MonitoredServer for the Config Server."""

    def update_config(self, new_config):
        """
        Set config to provided value, or to defaults for config server if an
        empty dict is provided.

        Args:
            new_config (dict): New dictionary to use as server config. Provide
                empty dict to use defaults for config server.
        """
        if not new_config:
            self.config = enstore_functions.get_config_server_info()
        else:
            self.config = new_config

    def __init__(self, config):
        MonitoredServer.__init__(self, config, enstore_constants.CONFIG_SERVER)
        self.update_config(config)


class MonitoredRatekeeper(MonitoredServer):
    """Implementation of MonitoredServer for the Ratekeeper."""

    def __init__(self, config):
        MonitoredServer.__init__(self, config, enstore_constants.RATEKEEPER)


# Monitored Library Manager Director
class MonitoredLMD(MonitoredServer):
    """Implementation of MonitoredServer for the Library Manager Director."""

    def __init__(self, config):
        MonitoredServer.__init__(self, config, enstore_constants.LM_DIRECTOR)


# Monitored Policy Engine Server and Migration Dispatcher
class MonitoredDispatcher(MonitoredServer):
    """Implementation of MonitoredServer for the Dispatcher."""

    def __init__(self, config):
        MonitoredServer.__init__(self, config, enstore_constants.DISPATCHER)


class MonitoredMigrator(MonitoredServer):
    """
    Implementation of MonitoredServer for the Migrator. Includes definition of
    STATUS_FIELDS which specifies the necessary fields in a status request
    message to the migrator server.
    """

    STATUS_FIELDS = {enstore_constants.STATE: (enstore_constants.UNKNOWN_S, enstore_constants.UNKNOWN_S),
                     # state, internal_state
                     enstore_constants.TRANSFERS_COMPLETED: DASH,
                     enstore_constants.TRANSFERS_FAILED: DASH,
                     enstore_constants.FILES: "",
                     enstore_constants.MODE: "",
                     enstore_constants.ID: "",
                     enstore_constants.STATUS: ()}

    def __init__(self, config, name, csc):
        MonitoredServer.__init__(self, config, name, DEFAULT_MIGRATOR_HUNG_INTERVAL)
        self.csc = csc
        self.status_keys = self.STATUS_FIELDS.keys()


class MonitoredMover(MonitoredServer):
    """
    Implementation of MonitoredServer for the Mover. Includes definition of
    STATUS_FIELDS which specifies the necessary fields in a status request
    message to the mover.
    """
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
        MonitoredServer.__init__(self, config, name, DEFAULT_MOVER_HUNG_INTERVAL)
        self.csc = csc
        self.status_keys = self.STATUS_FIELDS.keys()


class MonitoredMediaChanger(MonitoredServer):
    """Implementation of MonitoredServer for the Media Changer."""
    pass


class MonitoredUDPProxyServer(MonitoredServer):
    """Implementation of MonitoredServer for the UDP Proxy Server."""
    pass


def get_stalled_key(node, ff):
    """
    Get a deterministic identifier for a given node and file family, used in
    tracking of stalled file families.

    Args:
        node (str): Host of server.
        ff (str): File family.

    Returns:
        (string): Key representing (node, ff) pair.
    """
    return "%s,%s" % (node, ff)


def split_stalled_key(key):
    """
    Split provided identifier into the node and file family it represents.

    Args:
        key (str): Key representing (node, ff) pair.

    Returns:
        Tuple(string, string): Tuple containing node, file family.
    """
    return string.split(key, ',', 1)


class MonitoredLibraryManager(MonitoredServer):
    """
    Implementation of MonitoredServer for the Library Manager. Includes
    definition of STATUS_FIELDS which specifies the necessary fields in a
    status request message to the Library Manager.
    """
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
        self.client = library_manager_client.LibraryManagerClient(self.csc, self.name)
        self.status_keys = self.STATUS_FIELDS.keys()

    def check_state(self, status):
        """
        Fill in any missing fields of state request ticket with defaults from
        STATUS_FIELDS.

        Args:
            status (dict): Ticket for which necessary fields should be filled.
        """
        if enstore_constants.STATE not in status:
            status[enstore_constants.STATE] = self.STATUS_FIELDS[enstore_constants.STATE]

    def check_suspect_vols(self, status):
        """
        Fill in any missing fields of suspect volumes ticket with defaults from
        STATUS_FIELDS.

        Args:
            status (dict): Ticket for which necessary fields should be filled.
        """
        if enstore_constants.SUSPECT_VOLUMES not in status:
            status[enstore_constants.SUSPECT_VOLUMES] = self.STATUS_FIELDS[enstore_constants.SUSPECT_VOLUMES]

    def check_active_vols(self, status):
        """
        Fill in any missing fields of active volumes ticket with defaults from
        STATUS_FIELDS.

        Args:
            status (dict): Ticket for which necessary fields should be filled.
        """
        if enstore_constants.MOVERS not in status:
            status[enstore_constants.MOVERS] = self.STATUS_FIELDS[enstore_constants.MOVERS]

    def check_work_queue(self, status):
        """
        Fill in any missing fields of work queue ticket with defaults from
        STATUS_FIELDS.

        Args:
            status (dict): Ticket for which necessary fields should be filled.
        """
        if enstore_constants.ATMOVERS not in status:
            status[enstore_constants.ATMOVERS] = self.STATUS_FIELDS[enstore_constants.ATMOVERS]
        if enstore_constants.PENDING_WORKS not in status:
            status[enstore_constants.PENDING_WORKS] = self.STATUS_FIELDS[enstore_constants.PENDING_WORKS]

    def is_really_stalled(self, node, ff):
        """
        Determine if the queue has been stalled for a long time.
        We only want to alarm after several continuous reports of stalling, as
        reporting each time would be noisy.

        Args:
            node (str): Node to check.
            ff (str): File family to check.

        Return:
            (int): 1 if key has been stalled long enough to warrant an alarm,
                else 0.
        """
        rtn = 0
        key = get_stalled_key(node, ff)
        if key in self.ff_stalled:
            # see if the time is long enough to warrant an alarm
            if time.time() - self.ff_stalled[key] > self.stalled_time:
                rtn = 1
        else:
            # not a stall yet, keep track of it though
            self.ff_stalled[key] = time.time()
        return rtn

    def no_stall(self, node, ff=""):
        """
        Report that a node/ff pair has performed an operation successfully,
        and is not stalled.

        Args:
            node (str): Node name of successful operation.
            ff (str): File family of successful operation.
        """
        if ff:
            key = get_stalled_key(node, ff)
            if key in self.ff_stalled:
                del (self.ff_stalled[key])
        else:
            # delete all the keys for this node
            keys = self.ff_stalled.keys()
            for key in keys:
                stalled_node, ff = split_stalled_key(key)
                if node == stalled_node:
                    del (self.ff_stalled[key])
