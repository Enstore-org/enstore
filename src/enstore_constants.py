# this file contains constants used in several enstore modules

NETWORKFILE = "active_monitor.html"

FILE_PREFIX = "enplot_"
BPD = "bpd"
XFER = "xfer"
MLAT = "mlat"
MPH = "mph"
MPD = "mpd_total"
MPD_MONTH = "mpd_month"
UTIL = "Utilization"
SG = "sg"
LOG = ".log"
LOG_PREFIX = "LOG-"

# used by the inquisitor plot command
MPH_FILE = "%s%s"%(FILE_PREFIX, MPH)
MPD_FILE = "%s%s"%(FILE_PREFIX, MPD)
MPD_MONTH_FILE = "%s%s"%(FILE_PREFIX, MPD_MONTH)
MLAT_FILE = "%s%s"%(FILE_PREFIX, MLAT)
BPD_FILE = "%s%s"%(FILE_PREFIX, BPD)
BPD_FILE_R = "%s_r"%(BPD_FILE,)
BPD_FILE_W = "%s_w"%(BPD_FILE,)
XFER_FILE = "%s%s"%(FILE_PREFIX, XFER)
XFERLOG_FILE = "%s%s"%(XFER_FILE, LOG)
UTIL_FILE = "_%s"%(UTIL,)
SG_FILE = "%s%s"%(FILE_PREFIX, SG)
# ratekeeper generated plot data files
NULL_RATES = "null_rates"
REAL_RATES = "real_rates"

JPG = ".jpg"
PS = ".ps"
STAMP = "_stamp"

PID = "pid"
UID = "uid"
SOURCE = "source"
ALARM = "alarm"
ANYALARMS = "alarms"
URL = "url"
ROOT_ERROR = "root_error"

NULL_DRIVER = "NullDriver"

ALIVE_INTERVAL = "alive_interval"
DEFAULT_ALIVE_INTERVAL = "default_alive_interval"
CONFIG_SERVER_ALIVE_INTERVAL = 30 # there is none in config file
NOT_MONITORING = "not monitoring"
TIMED_OUT = "timed out"
DEAD = "dead"
FILE_LIST_NAME = "enstore_files.html"
PAGE_THRESHOLDS= "page_thresholds"
FILE_LIST = "file_list"

# server names used in enstore_up_down
LOGS = "Logger"
ALARMS = "Alarm Server"
CONFIGS = "Configuration Server"
FILEC = "File Clerk"
INQ = "Inquisitor"
VOLC = "Volume Clerk"
EV_RLY = "Event Relay"
UP_DOWN = "up_down"

# server names used in config file
LOG_SERVER = "log_server"
ALARM_SERVER = "alarm_server"
EVENT_RELAY = "event_relay"
FILE_CLERK = "file_clerk"
VOLUME_CLERK = "volume_clerk"
INQUISITOR = "inquisitor"
CONFIG_SERVER = "config_server"  # included for use by inquisitor
WWW_SERVER = "www_server"

SERVER_NAMES = {LOGS : LOG_SERVER,
		ALARMS : ALARM_SERVER,
		FILEC : FILE_CLERK,
		INQ : INQUISITOR,
		VOLC : VOLUME_CLERK,
		CONFIGS : CONFIG_SERVER,
		EV_RLY : EVENT_RELAY}

OUTAGEFILE = "enstore_outage.py"
SEENDOWNFILE = "enstore_seen_down.py"
SAAGHTMLFILE = "enstore_saag.html"
REALSAAGHTMLFILE = "enstore_saag_real.html"
REDSAAGHTMLFILE = "enstore_red.html"
BASENODE = "base_node"
UP = 0
WARNING = 2
DOWN = 1
SEEN_DOWN = 4
NOSCHEDOUT = -1
OFFLINE = 1
ENSTORE = "enstore"
NETWORK = "network"
TIME = "time"
UNKNOWN = "UNKNOWN TIME"

ACTION = 'action'
NODES = "nodes"
DO_ACTION_AFTER = "do_action_after"

RED = "red"

# these next 2 are related. both must be changed
SAAG_STATUS = ["red", "yellow", "green", "question"]
REAL_STATUS = [DOWN, WARNING, UP, SEEN_DOWN]

# dictionary keys for the system status information
STATUS = "status"
SUSPECT_VOLS = "suspect"
REJECT_REASON = "reject_reason"
TOTALPXFERS = "total_pend_xfers"
READPXFERS = "read_pend_xfers"
WRITEPXFERS = "write_pend_xfers"
TOTALONXFERS = "total_ong_xfers"
READONXFERS = "read_ong_xfers"
WRITEONXFERS = "write_ong_xfers"
PENDING = "pending"
WAM = "work at movers"
WORK = "work"
MOVERS = "movers"
ID = "id"
PORT = "port"
CURRENT = "current"
BASE = "base"
DELTA = "delta"
AGETIME = "agetime"
FILE = "file"
INFILE = "infile"
OUTFILE = "outfile"
BYTES = "bytes"
USERNAME = "username"
MODIFICATION = "mod"
NODE = "node"
SUBMITTED = "submitted"
DEQUEUED = "dequeued"
VOLUME_FAMILY = "volume_family"
FILE_FAMILY = "file_family"
FILE_FAMILY_WIDTH = "file_family_width"
DEVICE = "device"
EOD_COOKIE = "eod_cookie"
LOCATION_COOKIE = "location_cookie"
COMPLETED = "completed"
FAILED = "failed"
CUR_READ = "cur_read"
CUR_WRITE = "cur_write"
STATE = "state"
FILES = "files"
VOLUME = "volume"
LAST_READ = "last_read"
LAST_WRITE = "last_write"
WRITE = "write"
READ = "read"
LMSTATE = "lmstate"
READ_FROM_HSM = "read_from_hsm"
ACTIVE_VOLUMES = 'active_volumes'

ADMIN_QUEUE = "admin_queue"
WRITE_QUEUE = "write_queue"

NO_INFO = "------"
NO_WORK = "No work at movers"
NO_PENDING = "No pending work"
NEVER_ALIVE = -1

LIBRARY_MANAGER = "library_manager"
MOVER = "mover"
NOMOVER = "nomover"
MEDIA_CHANGER = "media_changer"
GENERIC_SERVERS = [ ALARM_SERVER, CONFIG_SERVER, EVENT_RELAY, FILE_CLERK, INQUISITOR,
		    LOG_SERVER, VOLUME_CLERK]

# Trace.trace output levels used by the inquisitor
INQERRORDBG = 1
INQPLOTDBG = 2
INQEVTMSGDBG = 3
INQRESTARTDBG = 4
INQSERVERDBG = 5
INQFILEDBG = 6
INQSTARTDBG = 8
INQWORKDBG = 9
INQSERVERTIMESDBG = 10
# other Trace.trace levels
DISPWORKDBG = 7

#List of well known ports (other than the config server).
EVENT_RELAY_PORT = 55510
MONITOR_PORT = 7499

EVENT_RELAY_HEARTBEAT = 60

#Pnfs interface info.
BFID_LAYER = 1
PARKED_LAYER = 2
#DEBUG_LAYER = 3
XREF_LAYER = 4
DUPLICATE_LAYER = 3

#Default encp retry values.
DEFAULT_ENCP_RETRIES = 3
DEFAULT_ENCP_RESUBMITIONS = None
