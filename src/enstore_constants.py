# this file contains constants used in several enstore modules

FILE_PREFIX = "enplot_"
BPD = "bpd"
XFER = "xfer"
MLAT = "mlat"
MPH = "mph"
LOG = ".log"

MPH_FILE = "%s%s"%(FILE_PREFIX, MPH)
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

# dictionary keys for the system status information
STATUS = "status"
SUSPECT_VOLS = "suspect"
REJECT_REASON = "reject_reason"
PENDING = "pending"
WORK = "work"
MOVER = "mover"
MOVERS = "movers"
KNOWN_MOVERS = "known_movers"
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
FILE_FAMILY = "file_family"
FILE_FAMILY_WIDTH = "ff_width"
DEVICE = "device"
EOD_COOKIE = "eod_cookie"
LOCATION_COOKIE = "location_cookie"
COMPLETED = "completed"
CUR_READ = "cur_read"
CUR_WRITE = "cur_write"
STATE = "state"
FILES = "files"
VOLUME = "volume"
LAST_READ = "last_read"
LAST_WRITE = "last_write"
WRITE = "write"
READ = "read"
FOUND_LM = "found_lm"
BLOCKSIZES = "blocksizes"
LMSTATE = "lmstate"

NO_INFO = "------"
NO_WORK = "No work at movers"
NO_PENDING = "No pending work"

