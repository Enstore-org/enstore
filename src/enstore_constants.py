# this file contains constants used in several enstore modules

NETWORKFILE = "active_monitor.html"

FILE_PREFIX = "enplot_"
BPD = "bpd"
XFER = "xfer"
MLAT = "mlat"
MPH = "mph"
MPD = "mpd_total"
LOG = ".log"

MPH_FILE = "%s%s"%(FILE_PREFIX, MPH)
MPD_FILE = "%s%s"%(FILE_PREFIX, MPD)
MLAT_FILE = "%s%s"%(FILE_PREFIX, MLAT)
BPD_FILE = "%s%s"%(FILE_PREFIX, BPD)
XFER_FILE = "%s%s"%(FILE_PREFIX, XFER)
XFERLOG_FILE = "%s%s"%(XFER_FILE, LOG)

JPG = ".jpg"
PS = ".ps"
STAMP = "_stamp"

READ = 1
WRITE = READ + 1

PID = "pid"
UID = "uid"
SOURCE = "source"
ALARM = "alarm"
ANYALARMS = "alarms"
URL = "url"

# sever names used in enstore_up_down
LOGS = "Logger"
ALARMS = "Alarm Server"
CONFIGS = "Configuration Server"
FILEC = "File Clerk"
INQ = "Inquisitor"
VOLC = "Volume Clerk"

OUTAGEFILE = "enstore_outage.py"
SAAGHTMLFILE = "enstore_saag.html"
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
WORK = "work"
MOVER = "mover"
MOVERS = "movers"
ID = "id"
PORT = "port"
CURRENT = "current"
BASE = "base"
DELTA = "delta"
AGETIME = "agetime"
FILE = "file"
BYTES = "bytes"
MODIFICATION = "mod"
NODE = "node"
SUBMITTED = "submitted"
DEQUEUED = "dequeued"
VOLUME_FAMILY = "volume_family"
FILE_FAMILY = "file_family"
FILE_FAMILY_WIDTH = "ff_width"
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

NO_INFO = "------"
NO_WORK = "No work at movers"
NO_PENDING = "No pending work"

BLOCKSIZES = "blocksizes"
LIBRARY_MANAGER = "library_manager"
MOVER = "mover"
MEDIA_CHANGER = "media_changer"
GENERIC_SERVERS = ["alarm_server", "config_server", "file_clerk",
		   "inquisitor", "log_server", "volume_clerk"]

