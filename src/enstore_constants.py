#!/usr/bin/env python

###############################################################################
#
# $Id$
#
# This file contains constants used in several enstore modules.
#
###############################################################################

DEFAULT_CONF_FILE = "/pnfs/enstore/.(config)(flags)/enstore.conf"
DEFAULT_CONF_HOST = "localhost"
DEFAULT_CONF_PORT = 7500

NETWORKFILE = "active_monitor.html"

FILE_PREFIX = "enplot_"
BPD = "bpd"
XFER = "xfer"
MLAT = "mlat"
MPH = "mph"
MPD = "mpd_total"
MPD_MONTH = "mpd_month"
BPD_MONTH = "bpd_month"
TOTAL_BPD = "total_bpd"
UTIL = "Utilization"
SG = "sg"
LOG = ".log"
LOG_PREFIX = "LOG-"
MAX_LOG_FILE_SIZE = "max_log_file_size"
NO_DEBUG_LOG = "no_debug_log"

IS_ALIVE = 1
IS_DEAD = 0

KB=1024L
MB=KB*KB
GB=KB*MB
TB=KB*GB
PB=KB*TB

SECS_PER_DAY = 86400
SECS_PER_HALF_DAY = 43200

#These strings are the beginning parts of the capacity and rate constants.
CAPACITY_PREFIX="CAP_"
RATE_PREFIX="RATE_"

# tape capacity in GB
CAP_9840=20
CAP_9940=60
CAP_9940B=200
CAP_LTO1=100
CAP_LTO2=200
CAP_LTO3=400
CAP_LTO4=800
CAP_T10000T2=5401
CAP_T10000T2D=8400

# tape rate in MB
RATE_9840=10.5
RATE_9940=10.5
RATE_9940B=27.0
RATE_LTO1=15.0
RATE_LTO2=27.0
RATE_LTO3=55.0
RATE_LTO4=120.0
RATE_T10000T2=240.0
RATE_T10000T2D=240.0

EXTRA_LINKS = "extra_links"
ENSTORE_PLOTS = "enstore_plots"

# used by the inquisitor plot command
MPH_FILE = "%s%s"%(FILE_PREFIX, MPH)
D_MPD_FILE = "-%s%s"%(FILE_PREFIX, MPD)
MPD_FILE = "%s%s"%(FILE_PREFIX, MPD)
MPD_MONTH_FILE = "%s%s"%(FILE_PREFIX, MPD_MONTH)
MLAT_FILE = "%s%s"%(FILE_PREFIX, MLAT)
TOTAL_BPD_FILE = "%s%s"%(FILE_PREFIX, TOTAL_BPD)
TOTAL_BPD_FILE_W = "%s_w"%(TOTAL_BPD_FILE,)
BPD_FILE = "%s%s"%(FILE_PREFIX, BPD)
BPD_FILE_D = "%s-"%(BPD_FILE,)
BPD_FILE_R = "%s_r"%(BPD_FILE,)
BPD_FILE_W = "%s_w"%(BPD_FILE,)
BPD_MONTH_FILE = "%s%s"%(FILE_PREFIX, BPD_MONTH)
BPD_MONTH_FILE_W = "%s_w_month"%(BPD_FILE,)
XFER_FILE = "%s%s"%(FILE_PREFIX, XFER)
XFERLOG_FILE = "%s%s"%(XFER_FILE, LOG)
UTIL_FILE = "_%s"%(UTIL,)
SG_FILE = "%s%s"%(FILE_PREFIX, SG)
BPD_SUBDIR = "bpd_per_mover"
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
REMEDY_TYPE = "remedy_type"
CONDITION = "condition"
ANYALARMS = "alarms"
URL = "url"
ROOT_ERROR = "root_error"
SEVERITY = "severity"
RA = "r_a"

NULL_DRIVER = "NullDriver"
DRIVE_ID = "drive_id"

VQFORMATED = "VOLUME_QUOTAS_FORMATED"

NO_CSC = 1
NO_ALARM = 2
NO_LOG = 4
NO_UDP = 8

ALIVE_INTERVAL = "alive_interval"
DEFAULT_ALIVE_INTERVAL = "default_alive_interval"
CONFIG_SERVER_ALIVE_INTERVAL = 30 # there is none in config file
NOT_MONITORING = "not monitoring"
DEAD = "dead"
NO_SUSPECT_VOLS = "CANNOT UPDATE SUSPECT VOLS"
NO_WORK_QUEUE = "CANNOT UPDATE WORK QUEUE"
NO_ACTIVE_VOLS = "CANNOT UPDATE ACTIVE VOLS"
NO_STATE = "CANNOT UPDATE STATUS"
TIMED_OUT = NO_STATE
FILE_LIST_NAME = "enstore_files.html"
PAGE_THRESHOLDS= "page_thresholds"
FILE_LIST = "file_list"
ENCP_IP = "encp_ip"
OVERALL_RATE = "overall_rate"
DISK_RATE = "disk_rate"
DRIVE_RATE = "drive_rate"

# server names used in enstore_up_down
ACCS = "Accounting Server"
DRVS = "Drivestat Server"
LOGS = "Logger"
ALARMS = "Alarm Server"
CONFIGS = "Configuration Server"
FILEC = "File Clerk"
INQ = "Inquisitor"
VOLC = "Volume Clerk"
PNFSA = "Pnfs Agent"
INFO = " Info Server"
EV_RLY = "Event Relay"
UP_DOWN = "up_down"
RATE = "Ratekeeper"
LMD = "Library Manager Director"
DISPR = "PE Server and Migr. Dispatcher"

# server names used in config file
###Note: enstore start/stop assumes that the spelling here matches that of
### the .py file of the matching server (save www_server).
ACCOUNTING_SERVER = "accounting_server"
DRIVESTAT_SERVER = "drivestat_server"
LOG_SERVER = "log_server"
ALARM_SERVER = "alarm_server"
EVENT_RELAY = "event_relay"
FILE_CLERK = "file_clerk"
INFO_SERVER = "info_server"
VOLUME_CLERK = "volume_clerk"
PNFS_AGENT = "pnfs_agent"
INQUISITOR = "inquisitor"
CONFIG_SERVER = "configuration_server"  # included for use by inquisitor
CONFIGURATION_SERVER = "configuration_server" # included for use by inquisitor
WWW_SERVER = "www_server"
RATEKEEPER = "ratekeeper"
MONITOR_SERVER = "monitor_server"
LM_DIRECTOR = "lm_director"
DISPATCHER = "dispatcher" # Policy Engine server and Migration Dispatcher

SERVER_NAMES = {ACCS: ACCOUNTING_SERVER,
                DRVS: DRIVESTAT_SERVER,
                LOGS : LOG_SERVER,
		ALARMS : ALARM_SERVER,
		FILEC : FILE_CLERK,
		INQ : INQUISITOR,
                INFO : INFO_SERVER,
		VOLC : VOLUME_CLERK,
		PNFSA : PNFS_AGENT,
		CONFIGS : CONFIG_SERVER,
		EV_RLY : EVENT_RELAY,
		RATE : RATEKEEPER,
                LMD: LM_DIRECTOR,
                DISPR: DISPATCHER,
                }

#The client names.
ACCOUNTING_CLIENT = "accounting_client"
ALARM_CLIENT = "ALARM_CLIENT"
CONFIG_CLIENT = "CONFIG_CLIENT"
CONFIGURATION_CLIENT = "CONFIG_CLIENT"
DRIVESTAT_CLIENT = "drivestat_client"
ENSTORE_RESTART = "ENSTORE_RESTART"
ENSTORE_START = "ENSTORE_START"
ENSTORE_STOP = "ENSTORE_STOP"
FILE_CLERK_CLIENT = "FILE_C_CLIENT"
INFO_CLIENT = "info_client"
INQUISITOR_CLIENT = "INQ_CLIENT"
LIBRARY_MANAGER_CLIENT = ".LM"
LOG_CLIENT = "LOG_CLIENT"
MEDIA_CHANGER_CLIENT = ".MC"
MIGRATOR_CLIENT = "MIGRATOR_CLIENT"
MONITOR_CLIENT = "MNTR_CLI"
#MOVER_CLIENT = ?
UDP_PROXY_CLIENT = "UDP_PROXY_CLIENT"
RATEKEEPER_CLIENT = "RATEKEEPER_CLIENT"
VOLUME_CLERK_CLIENT = "VOLUME_C_CLIENT"
PNFS_AGENT_CLIENT = "PNFS_A_CLIENT"

#The following are directory names appended by the plotter_main.py to
# the html_dir to put different types of plots in different sub-directories.
PLOTS_SUBDIR = ""  #plots/"
MOUNT_PLOTS_SUBDIR = "%s%s" % (PLOTS_SUBDIR, "")
RATEKEEPER_PLOTS_SUBDIR = "%s%s" % (PLOTS_SUBDIR, "")
DRIVE_UTILIZATION_PLOTS_SUBDIR = "%s%s" % (PLOTS_SUBDIR, "")
DRIVE_HOURS_PLOTS_SUBDIR = "%s%s" % (PLOTS_SUBDIR, "drive-hours")
DRIVE_HOURS_SEP_PLOTS_SUBDIR = "%s%s" % (PLOTS_SUBDIR, "drive-hours-sep")
FILES_RW_PLOTS_SUBDIR = "%s%s" % (PLOTS_SUBDIR, "files-rw")
FILES_RW_SEP_PLOTS_SUBDIR = "%s%s" % (PLOTS_SUBDIR, "files-rw-sep")
SLOT_USAGE_PLOTS_SUBDIR = "%s%s" % (PLOTS_SUBDIR, "")
PNFS_BACKUP_TIME_PLOTS_SUBDIR = "%s%s" % (PLOTS_SUBDIR, "")
FILE_FAMILY_ANALYSIS_PLOT_SUBDIR = "%s%s" % (PLOTS_SUBDIR, "file_family_usage")
ENCP_RATE_MULTI_PLOTS_SUBDIR = "%s%s" % (PLOTS_SUBDIR, "encp-rates")
QUOTA_PLOTS_SUBDIR = "%s%s" % (PLOTS_SUBDIR, "quotas")
TAPES_BURN_RATE_PLOTS_SUBDIR = "%s%s" % (PLOTS_SUBDIR, "burn-rate")
BPD_PER_MOVER_PLOTS_SUBDIR = "%s%s" % (PLOTS_SUBDIR, "bpd_per_mover")
XFER_SIZE_PLOTS_SUBDIR = "%s%s" % (PLOTS_SUBDIR, "xfer-size")
MIGRATION_SUMMARY_PLOTS_SUBDIR = "%s%s" % (PLOTS_SUBDIR, "migration_summary")
BYTES_PER_DAY_PLOTS_SUBDIR = "%s%s" % (PLOTS_SUBDIR, "bpd_per_mover")
MOVER_SUMMARY_PLOTS_SUBDIR = "%s%s" % (PLOTS_SUBDIR, "movers")
MOUNT_LATENCY_SUMMARY_PLOTS_SUBDIR = "%s%s" % (PLOTS_SUBDIR, "latencies")
MOUNTS_PER_ROBOT_PLOTS_SUBDIR = "%s%s" % (PLOTS_SUBDIR, "mounts-per-robot")
SFA_STATS_PLOTS_SUBDIR = "%s%s" % (PLOTS_SUBDIR, "sfa")

#The following are directory names appended by the html_main.py to
# the html_dir to put different types of pages in different sub-directories.
WEB_SUBDIR = "generated_html/"
OLD_WEB_SUBDIR = ""
WEEKLY_SUMMARY_SUBDIR = "%s%s" % (WEB_SUBDIR, "weekly_summary") #hardcoded!
SERVER_HTML_SUBDIR = "%s%s" % (OLD_WEB_SUBDIR, "")
TAPE_INVENTORY_SUBDIR = "%s%s" % (OLD_WEB_SUBDIR, "tape_inventory")
MISC_HTML_SUBDIR = "%s%s" % (WEB_SUBDIR, "miscellaneous")
CGI_BIN_SUBDIR = "%s%s" % (OLD_WEB_SUBDIR, "cgi-bin")


OUTAGEFILE = "enstore_outage.py"
SEENDOWNFILE = "enstore_seen_down.py"
SAAGHTMLFILE = "enstore_saag.html"
REALSAAGHTMLFILE = "enstore_saag_real.html"
REDSAAGHTMLFILE = "enstore_red.html"
ENSTORESTATUSFILE = "enstore_status_only.py"
STATUSONLYHTMLFILE = "enstore_status_only.html"
SAAGNETWORKHTMLFILE = "enstore_saag_network.html"
ALARM_HTML_FILE = "enstore_alarms.html"
LOG_HTML_FILE = "enstore_logs.html"
BASENODE = "base_node"
UP = 0
WARNING = 2
DOWN = 1
SEEN_DOWN = 4
NOSCHEDOUT = -1
OFFLINE = 1
ENONE = -1
ENSTORE = "enstore"
NETWORK = "network"
TIME = "time"
UNKNOWN = "UNKNOWN TIME"
UNKNOWN_S = "UNKNOWN"

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
SUSPECT_VOLUMES = "suspect_volumes"
REJECT_REASON = "reject_reason"
TOTALPXFERS = "total_pend_xfers"
READPXFERS = "read_pend_xfers"
WRITEPXFERS = "write_pend_xfers"
TOTALONXFERS = "total_ong_xfers"
READONXFERS = "read_ong_xfers"
WRITEONXFERS = "write_ong_xfers"
PENDING = "pending"
PENDING_WORKS = "pending_works"
WAM = "work at movers"
WORK = "work"
MOVERS = "movers"
ATMOVERS = "at movers"
MOVER_INTERFACE = 'mover_interface'
STORAGE_GROUP = 'storage_group'
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
TRANSFERS_COMPLETED = "transfers_completed"
TRANSFERS_FAILED = "transfers_failed"
BYTES_READ = "bytes_read"
BYTES_WRITTEN = "bytes_written"
MODE = "mode"
BYTES_TO_TRANSFER = "bytes_to_transfer"
CURRENT_VOLUME = "current_volume"
CURRENT_LOCATION = "current_location"
LAST_VOLUME = "last_volume"
LAST_LOCATION = "last_location"
FILES = "files"
VOLUME = "volume"
LAST_READ = "last_read"
LAST_WRITE = "last_write"
WRITE = "write"
READ = "read"
LMSTATE = "lmstate"
READ_FROM_HSM = "read_from_hsm"
ACTIVE_VOLUMES = 'active_volumes'
STORAGE_GROUP = 'storage_group'
MOVER_DOWN_PERCENTAGE = 'mover_down_percentage'
ADMIN_QUEUE = "admin_queue"
WRITE_QUEUE = "write_queue"
READ_QUEUE = "read_queue"

NO_INFO = "------"
NO_WORK = "No work at movers"
NO_PENDING = "No pending work"
NEVER_ALIVE = -1

LIBRARY_MANAGER = "library_manager"
MIGRATOR = "migrator"
MOVER = "mover"
DISK_MOVER = "DiskMover"
VC = "vc"
NOMOVER = "nomover"
MEDIA_CHANGER = "media_changer"
UDP_PROXY_SERVER="udp_proxy_server"
LM_DIRECTOR_CLIENT = "lm_director_client"
DISPATCHER_CLIENT = "dispatcher_client"

GENERIC_SERVERS = [ ACCOUNTING_SERVER, ALARM_SERVER, CONFIGURATION_SERVER,
                    DRIVESTAT_SERVER, EVENT_RELAY, FILE_CLERK, INFO_SERVER,
                    INQUISITOR, LOG_SERVER, PNFS_AGENT, RATEKEEPER,
                    VOLUME_CLERK, LM_DIRECTOR, DISPATCHER ]

# Trace.trace output levels used by the inquisitor
INQFILEDBG = 6
INQSTARTDBG = 8
INQWORKDBG = 9
INQSERVERTIMESDBG = 10
INQTHREADDBG = 11
INQHTMLDBG = 12
PLOTTING = 13
INQERRORDBG = 14
INQPLOTDBG = 15
INQEVTMSGDBG = 16
INQRESTARTDBG = 17
INQSERVERDBG = 18
INQERTHREAD = 19
# other Trace.trace levels
DISPWORKDBG = 7

#List of well known ports (other than the config server).
EVENT_RELAY_PORT = 55510
MONITOR_PORT = 7499

EVENT_RELAY_HEARTBEAT = 60

#Pnfs interface info.
BFID_LAYER = 1
#PARKED_LAYER = 2
DCACHE_LAYER = 2
#DEBUG_LAYER = 3
DUPLICATE_LAYER = 3
XREF_LAYER = 4


#Default encp retry values.
DEFAULT_ENCP_RETRIES = 3
DEFAULT_ENCP_RESUBMISSIONS = None

#Time constants (in seconds) for the mover to send 'transfer' messages.
MIN_TRANSFER_TIME = 1.0
MAX_TRANSFER_TIME = 5.0

#Allowed file deleted states
FILE_DELETED_FLAGS = ["y", "n","u","yes","no", "unknown"]

# Volume clerk related constants
SAFETY_FACTOR=1.05 # used to estimate volume space left
#MIN_LEFT=long(300*MB) # minimal size of free space allowed for volume
MIN_LEFT=0L # for now, this is disabled.


#Storage Filesystem class types.
PNFS="PNFS"
PNFS_AGENT="pnfs_agent"
CHIMERA="Chimera"
#find_pnfs_file requested filesystems constant type values.
BOTH="BOTH"
FS="FS"
NONFS="NONFS"

# UDP related
MAX_UDP_PACKET_SIZE = 64*KB-200
#MAX_UDP_PACKET_SIZE = 16*KB-200

#clerk related
PARALLEL_QUEUE_SIZE=100000
SEQUENTIAL_QUEUE_SIZE=100000
MAX_CONNECTION_FAILURE=5
MAX_THREADS=50
FILES_IN_TRANSITION_CHECK_INTERVAL=3600
ARCHIVING_FILES_IN_TRANSITION_CHECK_INTERVAL=86400
