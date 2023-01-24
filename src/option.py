#!/usr/bin/env python
"""
Example option group dictionaries:

To set a simple value: option.py --opt
   example_options = {
       'opt':{HELP_STRING:"some string text"}
       }
The preceding will do the same thing as the full dictionary example:
   example_options = {
       'opt':{HELP_STRING:"some string text"}
              DEFAULT_NAME:'opt',
              DEFAULT_VALUE:1,
              DEFAULT_TYPE:option.STRING,
              VALUE_USAGE:option.IGNORED,
              USER_LEVEL:option.USER,
              FORCE_SET_DEFAULT:0,
              EXTRA_VALUES:[],
        }
    }

To set two values, one saying the option was specifed as a boolean and another
to hold the actuall value (a filename for example): option.py --opt <filename>
    example_options = {
       'opt':{HELP_STRING:"some string text"}
              DEFAULT_NAME:'opt',
              DEFAULT_VALUE:option.DEFAULT,
              DEFAULT_TYPE:option.INTEGER,
              VALUE_NAME:'filename'
              VALUE_TYPE:option.STRING,
              VALUE_USAGE:option.REQUIRED,
              VALUE_LABEL:"filename",
              USER_LEVEL:option.USER,
              FORCE_SET_DEFAULT:option.FORCE,
              EXTRA_VALUES:[],
        }
    }

To accept multiple values: option.py --opt <filename> [filename2]
    example_options = {
       'opt':{HELP_STRING:"some string text"}
              DEFAULT_NAME:'opt',
              DEFAULT_VALUE:1,
              DEFAULT_TYPE':option.INTEGER,
              VALUE_NAME:'filename'
              VALUE_TYPE:option.STRING,
              VALUE_USAGE:option.REQUIRED,
              VALUE_LABEL:"filename",
              USER_LEVEL:option.USER,
              FORCE_SET_DEFAULT:1,
              EXTRA_VALUES:[{DEFAULT_NAME:"filename2",
                             DEFAULT_VALUE:"",
                             DEFAULT_TYPE:option.STRING,
                             VALUE_NAME:"filename2",
                             VALUE_TYPE:option.STRING,
                             VALUE_USAGE:option.OPTIONAL,
                             VALUE_LABEL:"filename2",
                               }]
        }
    }

To set multiple values without having any arguments
    example_options = {
        'opt':{option.HELP_STRING:"some string text",
               option.DEFAULT_TYPE:option.INTEGER,
               option.DEFAULT_VALUE:1,
               option.VALUE_USAGE:option.IGNORED,
               option.USER_LEVEL:option.ADMIN,
               #This will set an addition value.  It is weird
               # that DEFAULT_TYPE is used with VALUE_NAME,
               # but that is what will make it work.
               option.EXTRA_VALUES:[{option.DEFAULT_VALUE:0,
                                     option.DEFAULT_TYPE:option.INTEGER,
                                     option.VALUE_NAME:option.PRIORITY,
                                     option.VALUE_USAGE:option.IGNORED,
                                     }]
        }
    }

"""
############################################################################

# system imports
import os
import sys
import string
import pprint
import getopt
# import fcntl
import types

# enstore imports
import hostaddr
import Trace
import e_errors
import enstore_constants
import enstore_functions2

############################################################################

# DEFAULT_HOST = 'localhost'
# DEFAULT_PORT = '7500'

# default value
DEFAULT = 1

# default help string
BLANK = ""

# existence of command value
REQUIRED = "required"
OPTIONAL = "optional"
IGNORED = "ignored"

# command level
USER = "user"
USER2 = "user2"
ADMIN = "admin"
# Same as ADMIN, but is not included in help output.
# This is a valid USER_LEVEL for options, but not for
# the users actual user level.
HIDDEN = "hidden"

# variable type
INTEGER = "integer"
LONG = "long"
STRING = "string"
FLOAT = "float"
RANGE = "range"
# List of strings. Previously LIST, which is overwritten below
LIST_TYPE = "list"

# default action
FORCE_ACTION = 1  # Previously FORCE, which is overwritten below
NORMAL = 0

# strings to use in the dictionaries.
HELP_STRING = "help string"
DEFAULT_NAME = "default name"
DEFAULT_VALUE = "default value"
DEFAULT_TYPE = "default type"
DEFAULT_LABEL = "default label"  # This is a rare one...
VALUE_NAME = "value name"
VALUE_TYPE = "value type"
VALUE_USAGE = "value usage"
VALUE_LABEL = "value label"
SHORT_OPTION = "short option"
FORCE_SET_DEFAULT = "force set default"
USER_LEVEL = "user level"
EXTRA_VALUES = "extra values"

# internal use
EXTRA_OPTION = "extra_option"

############################################################################

# Note: This list is in alphabetical order, please keep it that way.
ADD = "add"  # volume
AGE_TIME = "age-time"  # encp
ALIVE = "alive"
ALL = "all"  # volume, start, stop
PACKAGE = "package"
PACKAGE_INFO = "pkginfo"
ARRAY_SIZE = "array-size"  # encp
ASSIGN_SG = "assign-sg"  # volume
BACKUP = "backup"  # volume, file
BFID = "bfid"  # pnfs, file
BFIDS = "bfids"  # file
BUFFER_SIZE = "buffer-size"  # encp
BUFFERED_TAPE_MARKS = "buffered-tape-marks"  # encp, migration
BYPASS_FILESYSTEM_MAX_FILESIZE_CHECK = "bypass-filesystem-max-filesize-check"
# encp
BYPASS_LABEL_CHECK = "bypass-label-check"  # volume
CAPTION_TITLE = "caption-title"  # enstore_make_log_plot
CAPTURE_TIMEOUT = "capture-timeout"  # entv
CAT = "cat"  # pnfs
CHECK = "check"  # volume, encp
CHECK_DATA = "check-data"  # migrate
CHECK_ONLY_META = "check-only-meta"  # migrate
CLEAN_DRIVE = "clean-drive"  # mover
CLEAR = "clear"  # volume
CLEAR_SG = "clear-sg"  # volume
CLIENT_NAME = "client-name"  # log, alarm
CONDITION = "condition"  # alarm_server
CONFIG_FILE = "config-file"  # configuration(c&s)
CONFIG_HOSTS = "config-hosts"  # eos
CONST = "const"  # pnfs
COPIES = "copies"  # encp
COPY = "copy"  # encp
COUNTERS = "counters"  # pnfs
COUNTERSN = "countersN"  # pnfs
CP = "cp"  # pnfs
CRC_CHECK = "crc-check"  # assert
CREATE = "create"  # quota
CURSOR = "cursor"  # pnfs
DATABASE = "database"  # pnfs
DATABASEN = "databaseN"  # pnfs
DATA_ACCESS_LAYER = "data-access-layer"  # encp
DBHOME = "dbHome"  # restore
DEBUG = "debug"  # migrate
DECR_FILE_COUNT = "decr-file-count"  # volume
DELAYED_DISMOUNT = "delayed-dismount"  # encp
DELETE = "delete"  # volume
DELETED = "deleted"  # file
DELETE_WORK = "delete-work"  # library
DELPRI = "delpri"  # encp
DESCRIPTION = "description"  # enstore_make_log_plot
DESTINATION_ONLY = "destination-only"  # migrate
DESTROY = "destroy"  # volume
DIRECT_IO = "direct-io"  # encp
DISABLE = "disable"  # quota
DISMOUNT = "dismount"  # media
DISPLAY = "display"  # entv
DO_ALARM = "do-alarm"
DO_LOG = "do-log"
DO_PRINT = "do-print"
DONT_ASK = "dont-ask"  # super_remove
DONT_ALARM = "dont-alarm"
DONT_LOG = "dont-log"
DONT_PRINT = "dont-print"
DONT_SHOW = "dont-show"  # entv
DOWN = "down"  # pnfs, inqusitor, mover
DUMP = "dump"  # pnfs, alarm, inquisitor, mover
DUPLICATE = "duplicate"  # pnfs
DUPLICATED = "duplicated"  # volume
DURATION = "duration"  # recent_file_listing
ECHO = "echo"  # pnfs
ECRC = "ecrc"  # encp
CKSM_VALUE = "cksm-value"  # encp
CKSM_SEED = "cksm-seed"  # encp
EJECT = "eject"  # media
ENABLE = "enable"  # quota
ENABLE_REDIRECTION = "enable-redirection"  # encp, migrate
ENCP = "encp"  # plotter
ENSTORE_STATE = "enstore-state"  # pnfs
EPHEMERAL = "ephemeral"  # encp
ERASE = "erase"  # volume, file
EXISTS = "exists"  # pnfs_agent
EXPORT = "export"  # volume
EXTERNAL_TRANSITIONS = "external-transitions"  # scanfiles
FILE = "file"  # info
FILE_FALLBACK = "file-fallback"  # configuration
FILE_FAMILY = "file-family"  # pnfs, encp, migrate
FILE_FAMILY_WIDTH = "file-family-width"  # pnfs, encp
FILE_FAMILY_WRAPPER = "file-family-wrapper"  # pnfs, encp, migrate
FILESIZE = "filesize"  # pnfs
FILE_THREADS = "file-threads"  # scanfiles
FIND_ALL_COPIES = "find-all-copies"  # file
FIND_COPIES = "find-copies"  # file
FIND_DUPLICATES = "find-duplicates"  # file
FIND_ORIGINAL = "find-original"  # file
FIND_SAME_FILE = "find-same-file"  # info
FIND_THE_ORIGINAL = "find-the-original"  # file
FORCE = "force"  # volume
FORGET_ALL_IGNORED_STORAGE_GROUPS = "forget-all-ignored-storage-groups"  # volume
FORGET_IGNORED_STORAGE_GROUP = "forget-ignored-storage-group"  # volume
FULL = "full"  # volume
GENERATE_MESSAGES_FILE = "generate-messages-file"  # entv
GET_ASSERTS = "get-asserts"  # library
GET_BFID = "get-bfid"  # encp
GET_BFIDS = "get-bfids"  # encp
GET_CACHE = "get-cache"  # encp
GET_CRCS = "get-crcs"  # file
GET_LAST_ALIVE = "get-last-alive"  # inquisitor
GET_LAST_LOGFILE_NAME = "get-last-logfile-name"  # log
GET_LOGFILE_NAME = "get-logfile-name"  # log
GET_LOGFILES = "get-logfiles"  # log
GET_MAX_ENCP_LINES = "get-max-encp-lines"  # inquisitor
GET_QUEUE = "get-queue"  # library
GET_REFRESH = "get-refresh"  # inquisitor
GET_SG_COUNT = "get-sg-count"  # volume
GET_SUSPECT_VOLS = "get-suspect-vols"  # library
GET_UPDATE_INTERVAL = "get-update-interval"  # inquisitor
GET_WORK = "get-work"  # library, media
GET_WORK_SORTED = "get-work-sorted"  # library
GVOL = "gvol"  # volume
HELP = "help"
HISTORY = "history"  # history
HOST = "host"  # monitor
HTML_DIR = "html-dir"  # monitor(server), eos
HTML_FILE = "html-file"  # inquisitor(server)
HTML_GEN_HOST = "html-gen-host"  # monitor, system
ID = "id"  # pnfs
IGNORE_FAIR_SHARE = "ignore-fair-share"  # encp
IGNORE_STORAGE_GROUP = "ignore-storage-group"  # volume
IMPORT = "import"  # volume
INFILE = "infile"  # scanfiles
INFO = "info"  # pnfs
INPUT_DIR = "input_dir"  # plotter
INSERT = "insert"  # media
IO = "io"  # pnfs
IOAREA = "ioarea"  # media
IS_UP = "is-up"  # inquisitor
JOUHOME = "jouHome"  # restore
JUST = "just"  # start, stop
JUST_FILES = "just-files"  # pnfs_agent
KEEP = "keep"  # plotter
KEEP_DIR = "keep-dir"  # plotter
KEEP_VOL = "keep-vol"  # super-remove
KEEP_DECLARATION_TIME = "keep-declaration-time"  # volume
LABEL = "label"  # plotter
LABELS = "labels"  # volume
LAYER = "layer"  # pnfs
LIBRARY = "library"  # pnfs, encp, migrate
LIST = "list"  # volume, file, get, MC, LM, M
LIST_CLEAN = "list-clean"  # media
LIST_DIR = "list-dir"  # pnfs_agent
LIST_DRIVES = "list-drives"  # media
LIST_FAILED_COPIES = "list-failed-copies"  # duplicate
LIST_LIBRARY_MANAGERS = "list-library-managers"  # configuration
LIST_MEDIA_CHANGERS = "list-media-changers"  # configuration
LIST_MIGRATORS = "list-migrators"  # configuration
LIST_MOVERS = "list-movers"  # configuration
LIST_SG_COUNT = "ls-sg-count"  # volume
LIST_SLOTS = "list-slots"  # media
LIST_VOLUMES = "list-volumes"  # media
LOAD = "load"  # configuration
LOG = "log"  # medaia(s)
LOGFILE_DIR = "logfile-dir"  # plotter
LS = "ls"  # pnfs
LS_ACTIVE = "ls-active"  # volume, file
MAKE_HTML = "make-html"  # up_down
MAKE_COPIES = "make-copies"  # duplicate
MAKE_FAILED_COPIES = "make-failed-copies"  # duplicate
MARK_BAD = "mark-bad"  # file
GET_CHILDREN = "children"  # file
FIELD = "field"  # file
REPLAY = "replay"  # file
MATCH_DIRECTORY_FILE_FAMILY = "match-directory-file-family"  # enmv
MATCH_VOLUME_FILE_FAMILY = "match-volume-file-family"  # enmv
MAX_ENCP_LINES = "max-encp-lines"  # inquisitor(c&s)
MAX_RETRY = "max-retry"  # encp
MAX_RESUBMIT = "max-resubmit"  # encp
MAX_WORK = "max-work"  # media(c&s)
MEDIA_VALIDATE = "media-validate"  # volume assert (for T10000 media only)
MESSAGE = "message"  # log, alarm
MESSAGES_FILE = "messages-file"  # entv
MIGRATED = "migrated"  # volume
MIGRATED_FROM = "migrated-from"  # migrate
MIGRATED_TO = "migrated-to"  # migrate
MIGRATION_ONLY = "migration-only"  # migrate
MKDIR = "mkdir"  # pnfs_agent
MKDIRS = "mkdirs"  # pnfs_agent
MMAP_IO = "mmap-io"  # encp
MMAP_SIZE = "mmap-size"  # encp
MODIFY = "modify"  # volume
MOUNT = "mount"  # media, plotter
MOUNT_POINT = "mount-point"  # pnfs
MOVER_DUMP = "mover_dump"  # mover
MOVER_TIMEOUT = "mover-timeout"  # assert, encp
MULTIPLE_COPY_ONLY = "multiple-copy-only"  # migrate
NAMEOF = "nameof"  # pnfs
NEW_LIBRARY = "new-library"  # volume
NO_ACCESS = "no-access"  # volume
NOCHECK = "nocheck"  # dbs
NO_CRC = "no-crc"  # encp
SINGLE_THREADED_ENCP = "single-threaded-encp"  # migration
NO_MAIL = "no-mail"  # up_down
NO_PLOT_HTML = "no-plot-html"  # plotter
NOT_ALLOWED = "not-allowed"  # volume
NOTIFY = "notify"  # notify
NOOUTAGE = "nooutage"  # inquisitor
NOOVERRIDE = "nooverride"  # inquisitor
OFFLINE = "offline"  # mover
ONLINE = "online"  # mover
OUTAGE = "outage"  # inquisitor
OUTOFDATE = "outofdate"  # plotter
OUTPUT_DIR = "output-dir"  # plotter, recent_file_listing
OVERRIDE = "override"  # inquisitor
OVERRIDE_DELETED = "override-deleted"  # encp
OVERRIDE_NOTALLOWED = "override-notallowed"  # encp
OVERRIDE_PATH = "override-path"  # encp
OVERRIDE_RO_MOUNT = "override-ro-mount"  # encp
PAGES = "pages"  # html
PARENT = "parent"  # pnfs
PATH = "path"  # pnfs
PLOT = "plot"  # plotter
PLOTS = "plots"  # html
PNFS_IS_AUTOMOUNTED = "pnfs-is-automounted"  # encp
PNFS_MOUNT_POINT = "pnfs-mount-point"  # encp
PNFS_STATE = "pnfs-state"  # pnfs
PORT = "port"  # monitor client, server
POSITION = "position"  # pnfs
PREFIX = "prefix"  # enstore_make_log_plot
PRIORITY = "priority"  # library, encp, migrate
PRINT = "print"  # conf
PRINT_QUEUE = "print-queue"  # library
PROC_LIMIT = "proc-limit"  # migrate
PROCEED_NUMBER = "proceed-number"  # migrate
PROFILE = "profile"  # entv
PUT_CACHE = "put-cache"  # encp
PTS_DIR = "pts_dir"  # plotter
PTS_NODES = "pts_nodes"  # plotter
PVOLS = "pvols"  # volume
QL = 'queue-length'  # library
QUERY = "query"  # info
RAISE = "raise"  # alarm
READ_ONLY = "read-only"  # volume
READ_TO_END_OF_TAPE = "read-to-end-of-tape"  # get, migrate
REASON = "reason"  # inquisitor
REBUILD_SG_COUNT = "rebuild-sg-count"  # volume
RECURSIVE = "recursive"  # file
RECYCLE = "recycle"  # volume
REFRESH = "refresh"  # inquisitor(c&s)
REMEDY_TYPE = "remedy_type"  # alarm_server
REMOVE = "remove"  # pnfs_agent, media
RESET_CNT = "reset-queue-counters"  # library manager
RESET_LIB = "reset-lib"  # volume
RESOLVE = "resolve"  # alarm
RESTORE = "restore"  # volume, file
RESTORE_ALL = "restore-all"  # dbs
RESUBMIT_TIMEOUT = "resubmit-timeout"  # encp
CONFIG_TIMEOUT = "config-timeout"  # encp
RETRIES = "retries"
RM = "rm"  # pnfs
RM_ACTIVE_VOL = "rm-active-vol"  # library
RMDIR = "rmdir"  # pnfs_agent
RM_SUSPECT_VOL = "rm-suspect-vol"  # library
ROOT_ERROR = "root-error"  # alarm
SAAG = "saag"  # html
SAAG_NETWORK = "saag-network"  # html
SAAG_STATUS = "saagstatus"  # inquisitor
SCAN = "scan"  # migrate
SCAN_VOLUMES = "scan-volumes"  # migrate (obsolete)
SENDTO = "sendto"  # mover
SEQUENTIAL_FILENAMES = "sequential-filenames"  # get
SET_CRCS = "set-crcs"  # file
SET_COMMENT = "set-comment"  # volume
SET_SG_COUNT = "set-sg-count"  # volume
SET_REQUESTED = "set-requested"  # quota
SET_AUTHORIZED = "set-authorized"  # quota
SET_QUOTA = "set-quota"  # quota
SEVERITY = "severity"  # alarm
SG = "sg"  # plotter
SHORTCUT = "shortcut"  # encp
SHOW = "show"  # configuration, inquisitor, media
SHOW_BAD = "show-bad"  # file
SHOW_BY_LIBRARY = "show-by-library"  # quota
SHOW_COPIES = "show-copies"  # info
SHOW_DRIVE = "show-drive"  # media
SHOW_FILE = "show-file"  # info
SHOWID = "showid"  # pnfs
SHOW_IGNORED_STORAGE_GROUPS = "show-ignored-storage-groups"  # volume
SHOW_QUOTA = "show-quota"  # volume
SHOW_ROBOT = "show-robot"  # media
SHOW_STATE = "show-state"  # volume, file, info
SHOW_VOLUME = "show-volume"  # media
SIZE = "size"  # pnfs
SKIP_BAD = "skip-bad"  # migrate
SKIP_DELETED_FILES = "skip-deleted-files"  # get
SKIP_PNFS = "skip-pnfs"  # super_remove, encp
SOURCE_ONLY = "source-only"  # migrate
SPOOL_DIR = "spool-dir"  # migrate
START_DRAINING = "start-draining"  # library
START_FROM = "start-from"  # volume assert
START_TIME = "start-time"  # plotter
STATUS = "status"  # mover, library
STOP_DRAINING = "stop-draining"  # library
STOP_TIME = "stop-time"  # plotter
STORAGE_GROUP = "storage-group"  # pnfs, encp, migrate
SUBSCRIBE = "subscribe"  # inquisitor
SUMMARY = "summary"  # monitor, configuration, up_down, duplicate
SYSTEM_HTML = "system-html"  # html
TAG = "tag"  # pnfs
TAGCHMOD = "tagchmod"  # pnfs
TAGCHOWN = "tagchown"  # pnfs
TAGECHO = "tagecho"  # pnfs
TAGRM = "tagrm"  # pnfs
TAGS = "tags"  # pnfs
THREADED = "threaded"  # encp
THREADED_IMPL = "threaded-impl"  # config server
TIME = "time"  # inquisitor
TIMEOUT = "timeout"
TIMESTAMP = "timestamp"  # configuration
TITLE = "title"  # enstore_make_log_plot
TITLE_GIF = "title_gif"  # plotter
TOTAL_BYTES = "total_bytes"  # plotter
TOUCH = "touch"  # volume
TRIM_OBSOLETE = "trim-obsolete"  # volume
UNMARK_BAD = "unmark-bad"  # file
UP = "up"  # pnfs, inquisitor, mover
UPDATE = "update"  # inquisitor
UPDATE_AND_EXIT = "update-and-exit"  # inquisitor
UPDATE_INTERVAL = "update-interval"  # inquisitor(c&s)
URL = "url"  # plotter
USAGE = "usage"
VERBOSE = "verbose"  # monitor, ensync, assert, encp
USE_DISK_FILES = "use-disk-files"  # migrate
USE_VOLUME_ASSERT = "use-volume-assert"  # migrate
VERSION = "version"  # encp, entv
VOL = "vol"  # volume
VOLS = "vols"  # volume, library
VOLUME = "volume"  # pnfs, encp
VOL1OK = "VOL1OK"  # volume
WARM_RESTART = "warm-restart"  # mover
WEB_HOST = "web-host"  # enstore_make_log_plot
WITH_DELETED = "with-deleted"  # migrate
WITH_FINAL_SCAN = "with-final-scan"  # migrate
WRITE_PROTECT_STATUS = "write-protect-status"  # volume
WRITE_PROTECT_ON = "write-protect-on"  # volume
WRITE_PROTECT_OFF = "write-protect-off"  # volume
XATTR = "xattr"  # fs
XATTRCHMOD = "xattrchmod"  # fs
XATTRCHOWN = "xattrchown"  # fs
XATTRRM = "xattrrm"  # fs
XATTRS = "xattrs"  # fs
XREF = "xref"  # pnfs

# these are this files test options
OPT = "opt"  # option
TEST = "test"  # option

# This list is the master list of options allowed.  This is in an attempt
# to keep the different spellings of options (ie. --host vs. --hostip vs --ip)
# in check.
valid_option_list = [
    ADD, AGE_TIME, ALIVE, ALL, ARRAY_SIZE, ASSIGN_SG,
    BACKUP, BFID, BFIDS, BUFFER_SIZE, BUFFERED_TAPE_MARKS,
    BYPASS_FILESYSTEM_MAX_FILESIZE_CHECK, BYPASS_LABEL_CHECK,
    CAPTION_TITLE, CAPTURE_TIMEOUT, CAT, CHECK, CHECK_DATA, CHECK_ONLY_META,
    CLEAN_DRIVE, CLEAR, CLEAR_SG, CLIENT_NAME,
    CONDITION, CONFIG_FILE, CONFIG_HOSTS, CONFIG_TIMEOUT, CONST, COPIES, COPY,
    COUNTERS, COUNTERSN, CP, CRC_CHECK, CREATE, CURSOR,
    DATA_ACCESS_LAYER, DATABASE, DATABASEN, DBHOME, DEBUG,
    DECR_FILE_COUNT, DELAYED_DISMOUNT, DELETE, DELETED, DELETE_WORK, DELPRI,
    DESCRIPTION, DESTINATION_ONLY, DESTROY,
    DIRECT_IO, DISABLE, DISMOUNT, DISPLAY,
    DO_ALARM, DONT_ASK, DONT_ALARM, DO_LOG, DONT_LOG, DO_PRINT, DONT_PRINT,
    DONT_SHOW,
    DOWN, DUMP, DUPLICATE, DUPLICATED, DURATION,
    ECHO, ECRC, CKSM_VALUE, CKSM_SEED, EJECT, ENABLE, ENABLE_REDIRECTION,
    ENCP, ENSTORE_STATE, EPHEMERAL, ERASE,
    EXISTS, EXPORT, EXTERNAL_TRANSITIONS,
    FILE, FILE_FALLBACK,
    FILE_FAMILY, FILE_FAMILY_WIDTH, FILE_FAMILY_WRAPPER, FILESIZE,
    FILE_THREADS, FIND_SAME_FILE, FORCE, PACKAGE, PACKAGE_INFO, FULL,
    FIND_ALL_COPIES, FIND_COPIES, FIND_DUPLICATES, FIND_ORIGINAL,
    FIND_SAME_FILE, FIND_THE_ORIGINAL,
    FORGET_ALL_IGNORED_STORAGE_GROUPS, FORGET_IGNORED_STORAGE_GROUP,
    GENERATE_MESSAGES_FILE,
    GET_ASSERTS, GET_BFID, GET_BFIDS, GET_CACHE, GET_CRCS, GET_LAST_ALIVE,
    GET_LAST_LOGFILE_NAME,
    GET_LOGFILE_NAME, GET_LOGFILES, GET_MAX_ENCP_LINES, GET_QUEUE,
    GET_REFRESH, GET_SUSPECT_VOLS, GET_UPDATE_INTERVAL, GET_WORK,
    GET_WORK_SORTED, GET_SG_COUNT,
    GVOL,
    HELP, HISTORY, HOST, HTML_DIR, HTML_FILE, HTML_GEN_HOST,
    ID, IGNORE_FAIR_SHARE, IGNORE_STORAGE_GROUP,
    IMPORT, INFILE, INFO, INPUT_DIR, INSERT, IO, IOAREA, IS_UP,
    JOUHOME, JUST, JUST_FILES,
    KEEP, KEEP_DIR, KEEP_VOL, KEEP_DECLARATION_TIME,
    LABEL, LABELS, LAYER, LIBRARY, LIST, LIST_CLEAN, LIST_DIR, LIST_DRIVES,
    LIST_FAILED_COPIES,
    LIST_LIBRARY_MANAGERS, LIST_MEDIA_CHANGERS, LIST_MIGRATORS, LIST_MOVERS,
    LIST_SG_COUNT, LIST_SLOTS, LIST_VOLUMES,
    LOAD, LOG, LOGFILE_DIR, LS, LS_ACTIVE,
    MAKE_HTML, MAKE_COPIES, MAKE_FAILED_COPIES, MARK_BAD, GET_CHILDREN, FIELD, REPLAY,
    MATCH_DIRECTORY_FILE_FAMILY, MATCH_VOLUME_FILE_FAMILY,
    MAX_ENCP_LINES, MAX_RESUBMIT, MAX_RETRY, MAX_WORK, MEDIA_VALIDATE,
    MESSAGE, MESSAGES_FILE, MIGRATED, MIGRATED_FROM, MIGRATED_TO,
    MIGRATION_ONLY,
    MKDIR, MKDIRS, MMAP_IO, MMAP_SIZE,
    MODIFY, MOUNT, MOUNT_POINT, MOVER_DUMP, MOVER_TIMEOUT,
    MULTIPLE_COPY_ONLY,
    NAMEOF, NEW_LIBRARY, NO_ACCESS, NOCHECK, NO_CRC, NOT_ALLOWED, NO_MAIL,
    NO_PLOT_HTML,
    NOTIFY, NOOUTAGE, NOOVERRIDE,
    OFFLINE, ONLINE, OPT, OUTAGE, OUTPUT_DIR, OUTOFDATE, OVERRIDE,
    OVERRIDE_DELETED, OVERRIDE_NOTALLOWED, OVERRIDE_PATH, OVERRIDE_RO_MOUNT,
    PAGES, PARENT, PATH, PLOT, PLOTS,
    PNFS_IS_AUTOMOUNTED, PNFS_MOUNT_POINT, PNFS_STATE, PORT, POSITION,
    PROC_LIMIT,
    PROCEED_NUMBER, PREFIX, PRIORITY, PRINT, PRINT_QUEUE, PROFILE, PTS_DIR,
    PTS_NODES, PUT_CACHE, PVOLS,
    QUERY, QL,
    RAISE, READ_ONLY, READ_TO_END_OF_TAPE, REASON, RECURSIVE, RECYCLE, REFRESH,
    REMEDY_TYPE, REMOVE, RESET_CNT, RESET_LIB, RESOLVE,
    RESTORE, RESTORE_ALL, RESUBMIT_TIMEOUT, RETRIES, REBUILD_SG_COUNT,
    RM, RM_ACTIVE_VOL, RMDIR, RM_SUSPECT_VOL, ROOT_ERROR,
    SAAG, SAAG_NETWORK, SAAG_STATUS,
    SCAN, SCAN_VOLUMES, SENDTO, SEQUENTIAL_FILENAMES,
    SET_CRCS, SET_COMMENT, SEVERITY, SG,
    SHORTCUT, SHOW, SHOW_BAD, SHOW_BY_LIBRARY, SHOW_DRIVE, SHOW_FILE,
    SHOW_COPIES,
    SHOWID, SHOW_IGNORED_STORAGE_GROUPS,
    SHOW_QUOTA, SHOW_ROBOT, SHOW_STATE, SHOW_VOLUME,
    SINGLE_THREADED_ENCP,
    SIZE, SKIP_BAD, SKIP_DELETED_FILES, SKIP_PNFS,
    SOURCE_ONLY,
    START_DRAINING, START_FROM, START_TIME, STATUS, STOP_DRAINING, STOP_TIME,
    SET_SG_COUNT,
    SET_REQUESTED, SET_AUTHORIZED, SET_QUOTA,
    SPOOL_DIR, STORAGE_GROUP, SUBSCRIBE, SUMMARY, SYSTEM_HTML,
    TAG, TAGCHMOD, TAGCHOWN, TAGECHO, TAGRM, TAGS,
    TEST, THREADED, THREADED_IMPL, TIME, TIMEOUT, TIMESTAMP, TITLE, TITLE_GIF,
    TOTAL_BYTES, TOUCH, TRIM_OBSOLETE,
    UNMARK_BAD, UP, UPDATE, UPDATE_AND_EXIT, UPDATE_INTERVAL, URL,
    USAGE, USE_DISK_FILES, USE_VOLUME_ASSERT,
    VERBOSE, VERSION, VOL, VOLS, VOLUME, VOL1OK,
    WARM_RESTART, WEB_HOST, WITH_DELETED, WITH_FINAL_SCAN,
    WRITE_PROTECT_STATUS, WRITE_PROTECT_ON, WRITE_PROTECT_OFF,
    XATTR, XATTRCHMOD, XATTRCHOWN, XATTRRM, XATTRS, XREF,
]

############################################################################
"""
used_default_config_host = 0
used_default_config_port = 0

def getenv(var, default=None):
    val = os.environ.get(var)
    if val is None:
        used_default = 1
        val = default
    else:
        used_default = 0
    return val, used_default

def default_host():
    val, used_default = getenv('ENSTORE_CONFIG_HOST', default=DEFAULT_HOST)
    if used_default:
        global used_default_config_host
        used_default_config_host = 1
    return val

def default_port():
    val, used_default = getenv('ENSTORE_CONFIG_PORT', default=DEFAULT_PORT)
    val = int(val)
    if used_default:
        global used_default_config_port
        used_default_config_port = 1
    return val
"""


def log_using_default(var, default):
    Trace.log(e_errors.INFO,
              "%s not set in environment or command line - reverting to %s"
              % (var, default))


def check_for_config_defaults():
    # check if we are using the default host and port.  if this is true
    # then nothing was set in the environment or passed on the command
    # line. warn the user.
    if enstore_functions2.used_default_host():
        log_using_default('ENSTORE_CONFIG_HOST',
                          enstore_constants.DEFAULT_CONF_HOST)
    if enstore_functions2.used_default_port():
        log_using_default('ENSTORE_CONFIG_PORT',
                          enstore_constants.DEFAULT_CONF_PORT)


def list2(value):
    return [value]


############################################################################

class Interface:

    def check_host(self, host):
        self.config_host = hostaddr.name_to_address(host)

    def __init__(self, args=None, user_mode=0):
        if args is None:
            args = sys.argv
        if not user_mode:  # Admin
            self.user_level = ADMIN
        elif user_mode == 2:
            self.user_level = USER2
        else:
            self.user_level = USER

        self.argv = args
        self.options = {}
        self.help = 0
        self.usage = 0

        self.options = {}
        self.option_list = []
        self.args = []  # Contains unprocessed arguments.
        self.some_args = []
        # self.config_options = {} #hack for old code

        # Override this to true for thread names to appear in the log file.
        # Remember to set the thread names to something meaningful, too.
        self.include_thread_name = 0

        apply(self.compile_options_dict, self.valid_dictionaries())

        self.check_option_names()

        self.parse_options()

        try:
            self.config_host = enstore_functions2.default_host()
            self.config_port = enstore_functions2.default_port()
        except ValueError, msg:
            sys.stderr.write("%s\n" % str(msg))
            sys.exit(1)

        if self.config_host == enstore_constants.DEFAULT_CONF_HOST:
            self.check_host(hostaddr.gethostinfo()[0])
        else:
            self.check_host(self.config_host)

        if hasattr(self, "help") and self.help:
            self.print_help()
        if hasattr(self, "usage") and self.usage:
            self.print_usage()

    ############################################################################

    parameters = []  # Don't put this in __init__().  It would clobber values.

    alive_rcv_options = {
        TIMEOUT: {HELP_STRING: "number of seconds to wait for alive response",
                  VALUE_NAME: "alive_rcv_timeout",
                  VALUE_USAGE: REQUIRED,
                  VALUE_TYPE: INTEGER,
                  VALUE_LABEL: "seconds",
                  USER_LEVEL: ADMIN,
                  FORCE_SET_DEFAULT: NORMAL},
        RETRIES: {HELP_STRING: "number of attempts to resend alive requests",
                  VALUE_NAME: "alive_retries",
                  VALUE_USAGE: REQUIRED,
                  VALUE_TYPE: INTEGER,
                  USER_LEVEL: ADMIN,
                  FORCE_SET_DEFAULT: NORMAL}
    }

    alive_options = alive_rcv_options.copy()
    alive_options[ALIVE] = {DEFAULT_VALUE: 1,
                            HELP_STRING:
                                "prints message if the server is up or down.",
                            VALUE_TYPE: INTEGER,
                            VALUE_NAME: "alive",
                            VALUE_USAGE: IGNORED,
                            USER_LEVEL: ADMIN,
                            SHORT_OPTION: "a",
                            FORCE_SET_DEFAULT: NORMAL
                            }
    help_options = {
        HELP: {DEFAULT_VALUE: 1,
               HELP_STRING: "prints this message",
               SHORT_OPTION: "h",
               FORCE_SET_DEFAULT: NORMAL},
        USAGE: {DEFAULT_VALUE: 1,
                HELP_STRING: "prints short help message",
                VALUE_USAGE: IGNORED,
                FORCE_SET_DEFAULT: NORMAL}
    }

    trace_options = {
        DO_PRINT: {VALUE_USAGE: REQUIRED,
                   VALUE_TYPE: RANGE,
                   HELP_STRING: "turns on more verbose output",
                   USER_LEVEL: ADMIN,
                   FORCE_SET_DEFAULT: NORMAL},
        DONT_PRINT: {VALUE_USAGE: REQUIRED,
                     VALUE_TYPE: RANGE,
                     HELP_STRING: "turns off more verbose output",
                     USER_LEVEL: ADMIN,
                     FORCE_SET_DEFAULT: NORMAL},
        DO_LOG: {VALUE_USAGE: REQUIRED,
                 VALUE_TYPE: RANGE,
                 HELP_STRING: "turns on more verbose logging",
                 USER_LEVEL: ADMIN,
                 FORCE_SET_DEFAULT: NORMAL},
        DONT_LOG: {VALUE_USAGE: REQUIRED,
                   VALUE_TYPE: RANGE,
                   HELP_STRING: "turns off more verbose logging",
                   USER_LEVEL: ADMIN,
                   FORCE_SET_DEFAULT: NORMAL},
        DO_ALARM: {VALUE_USAGE: REQUIRED,
                   VALUE_TYPE: RANGE,
                   HELP_STRING: "turns on more alarms",
                   USER_LEVEL: ADMIN,
                   FORCE_SET_DEFAULT: NORMAL},
        DONT_ALARM: {VALUE_USAGE: REQUIRED,
                     VALUE_TYPE: RANGE,
                     HELP_STRING: "turns off more alarms",
                     USER_LEVEL: ADMIN,
                     FORCE_SET_DEFAULT: NORMAL}
    }

    test_options = {
        'test': {DEFAULT_VALUE: 2,
                 DEFAULT_TYPE: INTEGER,
                 HELP_STRING: "test",
                 VALUE_USAGE: OPTIONAL,
                 SHORT_OPTION: "t",
                 USER_LEVEL: ADMIN
                 },
        'opt': {HELP_STRING: "some string text",
                USER_LEVEL: USER,
                DEFAULT_NAME: 'opt',
                DEFAULT_VALUE: DEFAULT,
                DEFAULT_TYPE: INTEGER,
                EXTRA_VALUES: [{VALUE_NAME: 'filename',
                                VALUE_TYPE: STRING,
                                VALUE_USAGE: REQUIRED,
                                VALUE_LABEL: "filename",
                                },
                               {DEFAULT_NAME: "filename2",
                                DEFAULT_VALUE: "",
                                DEFAULT_TYPE: STRING,
                                VALUE_NAME: "filename2",
                                VALUE_TYPE: STRING,
                                VALUE_USAGE: OPTIONAL,
                                VALUE_LABEL: "filename2",
                                }]
                }
    }

    ############################################################################

    # lines_of_text: list of strings where each item in the list is a line of
    #               text that will be used to output the help string.
    # text_string: the string that will be appended to the end of lines_of_text
    # filler_length: Minimum number of character columns to indent the
    #               text_string.
    @staticmethod
    def build_help_string(lines_of_text, text_string_line,
                          filler_length, num_of_cols):
        # Set this for the first loop below.
        use_existing_line = 1

        # Build the non-help string part of the command output. Assume
        # that option_names is less than 80 characters.
        # lines_of_text = []
        try:
            last_line = lines_of_text[-1]
        except IndexError:
            last_line = ""

        for text_string in text_string_line.split("\n"):
            # value_line_length is the number of character columns to space
            # over (or append to) before printing new characters.
            value_line_length = num_of_cols - max(len(last_line),
                                                  filler_length + 1)
            index = 0
            while index < len(text_string):
                # print "value_line_len:", value_line_length, "index:", index
                # calculate how much of the line can be used up without
                # splitting words on different lines.
                if (len(text_string) - index) < value_line_length:
                    new_index = len(text_string)
                else:
                    new_index = string.rfind(text_string, " ", index,
                                             index + value_line_length)

                if use_existing_line:  # use existing line
                    try:
                        del lines_of_text[-1]
                    except (TypeError, IndexError):
                        pass

                    temp_fill = filler_length - len(last_line)
                    if temp_fill < 0:
                        temp_fill = 0

                    temp = ("%s" % (last_line,)) + " " * temp_fill + \
                        text_string[index:new_index]
                    lines_of_text.append(temp)
                else:  # use new line
                    lines_of_text.append(" " * filler_length +
                                         text_string[index:new_index])
                # Reset this to the next indent point for future lines.
                value_line_length = num_of_cols - (filler_length - 1)
                index = new_index
                # Set this false to use a new line.
                use_existing_line = 0

    def print_help(self):
        # First print the usage line.
        print self.get_usage_line() + "\n"

        # num_of_cols - width of the terminal
        # comm_cols - length of option_names (aka "       --%-20s")
        num_of_cols = 80  # Assume this until python 2.1
        comm_cols = 29

        list_of_options = self.options.keys()
        list_of_options.sort()
        for opts in list_of_options:

            # Don't even print out options that the user doesn't have access to.
            option_level = self.options[opts].get(USER_LEVEL, USER)
            if self.user_level in [USER] and \
                    option_level in [ADMIN, USER2]:
                continue
            if self.user_level in [USER2] and \
                    option_level in [ADMIN]:
                continue
            if option_level in [HIDDEN]:
                # Hidden options should never be visible.
                continue

            # Snag all optional/required values that belong to this option.
            # Do this by getting the necessary fields from the dictionary.
            # (ie. "value_name"/"default_name" and "value_usage".)  Get the
            # list of extra options, if any.  Insert the values at the
            # beginning of the extras_args list.
            opt_arg = self.options[opts].get(
                VALUE_NAME,
                self.options[opts].get(DEFAULT_NAME, opts))
            opt_value = self.options[opts].get(VALUE_USAGE, IGNORED)
            opt_label = self.options[opts].get(VALUE_LABEL, opt_arg)
            extra_args = self.options[opts].get(EXTRA_VALUES, [])
            extra_args.insert(0, {VALUE_NAME: opt_arg,
                                  VALUE_USAGE: opt_value,
                                  VALUE_LABEL: opt_label})

            # Put together the string that specifies what the spelling of the
            # options are.  The two types are those with and without short
            # option equivalents.
            # ie: "   -a, --alive"
            if self.options[opts].get(SHORT_OPTION, None):
                # If option has a short argument equivalent.
                option_names = "   -%s, --%s" % \
                               (self.options[opts][SHORT_OPTION],
                                opts)
            else:
                # If option does not have a short argument equivalent.
                option_names = "       --%s" % (opts,)

            # Loop through the list generating the has_value string.  This
            # string is the list of values wrapped in [] or <> possible for
            # the option.
            # ie: "<VOLUME_NAME> <LIBRARY> <STORAGE_GROUP> <FILE_FAMILY>
            #     <WRAPPER> <MEDIA_TYPE> <VOLUME_BYTE_CAPACITY>"
            has_value = ""
            for opt_arg in extra_args:
                arg_str = string.upper(opt_arg.get(VALUE_LABEL,
                                                   opt_arg.get(VALUE_NAME, BLANK)))
                arg_str = arg_str.replace("-", "_")
                value = opt_arg.get(VALUE_USAGE, IGNORED)

                if value == REQUIRED:
                    has_value = has_value + "<" + arg_str + "> "
                elif value == OPTIONAL:
                    has_value = has_value + "[" + arg_str + "] "
                elif value == IGNORED and \
                        self.options[opts].get(VALUE_LABEL, None) is not None:
                    # This case may be true for switches that take zero
                    # or more (unknown number of) arguments that are
                    # processed via intf.args.
                    has_value = has_value + "[" + arg_str + "] "

            # Get and calculate various variables needed to format the output.
            # help_string - shorter than accessing the dictionary
            help_string = self.options[opts].get(HELP_STRING, BLANK)

            lines_of_text = []
            # Build the OPTION part of the command output. Assume
            # that option_names is less than 80 characters.
            self.build_help_string(lines_of_text, option_names,
                                   0, num_of_cols)
            # For those options with values, include them in the help string.
            if has_value:
                # Add extra padding space(s) for readability.
                has_value = " " + has_value + " "
                # has_value = "=" + has_value + " "

                # Build the VALUES part of the command output. Assume
                # that option_names is less than 80 characters.
                self.build_help_string(lines_of_text, has_value,
                                       len(option_names), num_of_cols)
            else:  # Insert spaces in case of long option name.
                self.build_help_string(lines_of_text, "  ",
                                       len(option_names), num_of_cols)
            # Build the HELP STRING part of the command output.  Assume
            # that option_names is less than 80 characters.
            self.build_help_string(lines_of_text, help_string,
                                   comm_cols, num_of_cols)

            for line in lines_of_text:
                print line
        sys.exit(0)

    def get_usage_line(self):  # , opts=None): #The opts is legacy from interface.py.

        short_opts = self.getopt_short_options()
        if short_opts:
            short_opts = "-" + short_opts
        else:
            short_opts = ""

        # Give these string an initial value.
        usage_string = "       " + os.path.basename(sys.argv[0])
        usage_line = ""

        list_of_options = self.options.keys()
        list_of_options.sort()
        for key in list_of_options:
            # Ignore admin options if in user mode.
            option_level = self.options[key].get(USER_LEVEL, USER)
            if self.user_level in [USER] and \
                    option_level in [ADMIN, USER2]:
                continue
            if self.user_level in [USER2] and \
                    option_level in [ADMIN]:
                continue
            if option_level in [HIDDEN]:
                # Hidden options should never be visible.
                continue

            # Determine if the option needs an "=" or "[=]" after it.
            has_value = self.options[key].get(VALUE_USAGE, IGNORED)
            if has_value == REQUIRED:
                has_value = "="
            elif has_value == OPTIONAL:
                has_value = "[=]"
            else:
                has_value = ""

            usage_line = usage_line + "--" + key + has_value + " "

        # Combine the short and long options together.
        switch_string = " "
        if short_opts or usage_line:
            switch_string = " [ " + short_opts + " " + usage_line + "] "

        # If there are a lot of options, don't confuse the user and only
        # report [OPTIONS]... instead.
        if len(usage_string) + len(switch_string) + \
                len(getattr(self, "parameter", [""])[0]) > 80:
            switch_string = " [OPTIONS]... "

        # usage_string = usage_string + self.format_parameters()
        full_usage_string = ""
        for parameter_set in self.parameters:
            full_usage_string = full_usage_string + usage_string + \
                                switch_string + parameter_set + "\n"
        if not full_usage_string:
            full_usage_string = usage_string + switch_string

        return "Usage: \n" + full_usage_string

    def format_parameters(self):
        param_string = ""
        param_list = []
        for parameter in self.parameters:
            if isinstance(parameter, types.ListType):
                param_string = ""
                for parameter2 in parameter:
                    param_string = param_string + " " + parameter2
                param_list.append(param_string)
            else:
                param_string = param_string + " " + parameter
        return param_string

    def print_usage(self, message=None):
        if message:
            print message

        print self.get_usage_line()

        if message is None:
            sys.exit(0)  # No error message was passed in.
        else:
            sys.exit(1)  # An error is known, exit as such.

    @staticmethod
    def missing_parameter(param):
        try:
            sys.stderr.write("ERROR: missing parameter %s\n" % (param,))
            sys.stderr.flush()
        except IOError:
            pass

    ############################################################################

    # This function returns the tuple containing the valid dictionaries used
    # in compile_options_dict().  Simply overload this function to
    # correctly set the valid option groups.
    def valid_dictionaries(self):
        return self.help_options, self.test_options

    # Compiles the dictionary groups into one massive dictionary named options.
    def compile_options_dict(self, *dictionaries):
        for i in range(0, len(dictionaries)):
            if not isinstance(dictionaries[i], types.DictionaryType):
                raise TypeError("Dictionary required, not %s." %
                                type(dictionaries[i]))
            for key in dictionaries[i].keys():
                if key not in self.options:
                    self.options[key] = dictionaries[i][key]

    # Verifies that the options used are in the list of options.  This is to
    # help cut down on the different combinations of spellings.
    def check_option_names(self):
        for opt in self.options.keys():
            if opt not in valid_option_list:
                msg = "Developer error.  Option '%s' not in valid option list."
                try:
                    sys.stderr.write(msg % (opt,) + "\n")
                    sys.stderr.flush()
                except IOError:
                    pass
                sys.exit(1)

    # Verifies that the number of left over options is correct.  If they are
    # not then the process is killed.
    def check_correct_count(self, num=0):
        length = len(self.args)
        if length > num:
            extras = string.join(self.args[-(length - num):], " ")
            msg = "%d extra arguments specified: %s\n" % (length - num, extras)
            try:
                sys.stderr.write(msg)
                sys.stderr.flush()
            except IOError:
                pass

    ############################################################################

    # Goes through the compiled option dictionary looking for short options
    # to format in the getopt.getopt() format.
    def getopt_short_options(self):
        temp = ""
        for opt in self.options.keys():
            short_opt = self.options[opt].get(SHORT_OPTION, None)
            # If their is a (valid) short option.
            if short_opt and len(short_opt) == 1:
                # If the user does not have permission to execute such an option
                # skip over it.
                option_level = self.options[opt].get(USER_LEVEL, USER)
                if self.user_level in [USER] and \
                        option_level in [ADMIN, HIDDEN, USER2]:
                    continue
                if self.user_level in [USER2] and \
                        option_level in [ADMIN, HIDDEN]:
                    continue

                temp = temp + short_opt

                if self.options[opt].get(VALUE_USAGE, None) in [REQUIRED]:
                    temp = temp + "="

        return temp

    # Goes through the compiled option dictionary pulling out long options
    # to format in the getopt.getopt() format.
    # The BC comment lines indicate backwards compatibility.  Some people
    # can't let go of VAX conventions.
    def getopt_long_options(self):
        temp = []
        for opt in self.options.keys():
            if self.options[opt].get(VALUE_USAGE, None) in [REQUIRED]:
                temp.append(opt + "=")
            else:
                # If the extra values section starts with a required element,
                # we also need to include it in the list of switches that
                # take a required arguement.
                extra_values = self.options[opt].get(EXTRA_VALUES, [])
                if len(extra_values) > 0 and \
                        extra_values[0].get(VALUE_USAGE, None) in [REQUIRED]:
                    temp.append(opt + "=")
                else:
                    temp.append(opt)

        return temp

    ############################################################################

    # Parse the command line.
    def parse_options(self):

        long_opts = self.getopt_long_options()
        short_opts = self.getopt_short_options()

        # If an argument uses an = for a value seperate it into two entries.
        self.split_on_equals(self.argv)
        if self.argv[0] == "enstore":  # just in case things change...
            argv = self.argv[2:]
        else:
            argv = self.argv[1:]

        # For backward compatibility, convert options with underscores to
        # dashes.  This must be done before the getopt since the getopt breaks
        # with underscores.  It should be noted that the use of underscores is
        # a VAX thing, and that dashes is the UNIX way of things.
        self.convert_underscores(argv)

        while argv:
            self.some_args = argv  # This is a second copy for next arg finding.

            # If the first thing is not an option (switch) place it with the
            # non-processed arguments and remove it from the list of args.
            # This is done, because getopt.getopt() breaks if the first thing
            # it sees does not begin with a "-" or "--".
            while len(argv) and not self.is_switch_option(argv[0]):
                self.args.append(argv[0])
                del argv[0]

            # There is a major problem with this method. Multiple entries on the
            # command line of the same command are not parsed properly.
            optlist = []
            try:
                optlist, argv = getopt.getopt(argv, short_opts, long_opts)
            except getopt.GetoptError, detail:
                self.print_usage(detail.msg)

            # copy in this way, to keep self.args out of a dir() listing.
            while len(argv) and not self.is_switch_option(argv[0]):
                self.args.append(argv[0])
                del argv[0]

            for optlist_arg in optlist:
                opt = optlist_arg[0]
                value = optlist_arg[1]
                if self.user_level in [USER]:
                    if self.is_admin_option(opt) or \
                            self.is_user2_option(opt):
                        # Deny access to admin commands if regular user.
                        self.print_usage("option %s is an administrator option" %
                                         (opt,))
                elif self.user_level in [USER2]:
                    if self.is_admin_option(opt):
                        # Deny access to admin commands if regular user.
                        self.print_usage("option %s is an administrator option" %
                                         (opt,))

                if self.is_long_option(opt):
                    # Option is a long option.  This means that the option is
                    # preceded by two dashes and can be any length.
                    self.long_option(opt[2:], value)

                elif self.is_short_option(opt):
                    # Option is a short option.  This means it is only
                    # one letter long and has one dash at the beginning
                    # of the option group.
                    self.short_option(opt[1:], value)

    ############################################################################

    # The option is a long option with value possible VALUE_USAGE.  Determine
    # if the option has been used in the correct manner.  If so, set the
    # value accordingly, if not print an error message.
    def long_option(self, long_opt, value):

        self.option_list.append(long_opt)  # Remember this order.

        if self.options[long_opt].get(VALUE_USAGE, None) == REQUIRED:
            # First, determine if the option, which has been determined to
            # require a sub option, is followed in the command line with
            # an option that does not begin with "-" or "--".
            if value is not None:
                self.set_value(long_opt, value)
            else:
                self.print_usage("Option %s requires value." % (long_opt,))

        elif self.options[long_opt].get(VALUE_USAGE, None) == OPTIONAL:
            next_arg = self.next_argument(long_opt)  # Used for optional.

            # First, determine if the option, which may or may not have a
            # sub option, is followed in the command line with
            # an option that does not begin with "-" or "--".
            if value:
                self.set_value(long_opt, value)

            # If the option has an optional value and it is present then
            # find the value (albeit the hard way), set the value and then
            # remove the value from the list of previously unprocessed
            # arguments (self.args).
            elif next_arg is not None and not self.is_option(next_arg):
                self.set_value(long_opt, next_arg)
                self.args.remove(next_arg)

            # Use the default value if none is specified.
            else:
                self.set_value(long_opt, None)  # Uses 'default'

        else:  # IGNORED
            # Do pass value, there might be an extra values section.
            if value:
                self.set_value(long_opt, value)
            else:
                self.set_value(long_opt, None)

    # Do the same thing with the short options that is done with the long
    # options. For all intents and purposes, this gets the long opt that
    # is the short option equivalent and uses the long opt.
    def short_option(self, short_opt, value):
        long_opt = self.short_to_long(short_opt)

        self.option_list.append(long_opt)  # Remember this order.

        if self.options[long_opt].get(VALUE_USAGE, None) == REQUIRED:
            # First, determine if the option, which has been determined to
            # require a sub option, is followed in the command line with
            # an option that does not begin with "-" or "--".
            if value:
                self.set_value(long_opt, value)
            else:
                self.print_usage("Option %s requires value." % (short_opt,))

        elif self.options[long_opt].get(VALUE_USAGE, None) == OPTIONAL:
            # First, determine if the option, which may or may not have a
            # sub option, is followed in the command line with
            # an option that does not begin with "-" or "--".
            if value:
                self.set_value(long_opt, value)

            # If the option has an optional value, and it is present, then
            # find the value (albeit the hard way), set the value, and then
            # remove the value from the list of previously unprocessed
            # arguments (self.args).
            elif self.next_argument(short_opt) is not None and \
                    not self.is_option(self.next_argument(short_opt)):
                next_arg = self.next_argument(short_opt)
                self.set_value(long_opt, next_arg)
                self.args.remove(next_arg)
            else:
                self.set_value(long_opt, None)  # Uses 'default'

        else:  # IGNORED
            self.set_value(long_opt, None)  # Uses 'default'

    ############################################################################

    # Options can be entered like "--option value" or "--option=value".
    # Parse the argv passed in and split the "=" values into spaced value.
    def split_on_equals(self, argv):
        args = []
        offset = 0
        # Write the values into a different list to avoid the problem of
        # putting extra options in the argv list that range() will miss.
        for i in range(len(argv)):
            # Make sure it is a switch.
            # Note: the replace operation is necessary to support _s.
            if self.is_option(argv[i].split("=", 1)[0].replace("_", "-")) and \
                    self.is_switch_option(argv[i]):
                split_option = string.split(argv[i], "=", 1)
                args[i + offset:i + offset + 1] = split_option
                offset = offset + 1
            else:
                args.append(argv[i])

        # Set the temporary list to be the real list.
        argv[:] = args

    def convert_underscores(self, argv):
        for i in range(0, len(argv)):
            if argv[i].find("_") >= 0:  # returns -1 on failure
                opt_with_dashes = argv[i].replace("_", "-")
                if self.is_long_option(opt_with_dashes) and \
                        opt_with_dashes[:2] == "--":
                    # sys.stderr.write("Option %s depreciated, " \
                    #                 "use %s instead.\n" %
                    #                 (argv[i], opt_with_dashes))
                    argv[i] = opt_with_dashes

    # This function is copied from the original interface code.  It parses,
    # a string into a list of integers (aka range).
    # Note: This should probably be looked at to be more robust.
    @staticmethod
    def parse_range(s):
        if ',' in s:
            s = string.split(s, ',')
        else:
            s = [s]
        r = []
        for t in s:
            if '-' in t:
                lo, hi = string.split(t, '-')
                lo, hi = int(lo), int(hi)
                r.extend(range(lo, hi + 1))
            else:
                r.append(int(t))
        return r

    # Take the passed in short option and return its long option equivalent.
    def short_to_long(self, short_opt):
        if not self.is_short_option(short_opt):
            return None
        for key in self.options.keys():
            if self.trim_short_option(short_opt) == \
                    self.options[key].get(SHORT_OPTION, None):
                return key  # in other words return the long opt.
        return None

    # Return the next argument in the argument list after the one specified
    # as argument.  If one does not exist, return None.
    # some_args is used to avoid problems with duplicate arguments on the
    # command line.
    def next_argument(self, argument):
        if not len(self.some_args) > 1:
            return None

        # Get a copy of the command line with values specified with equal
        # signs separated.
        self.split_on_equals(self.some_args)

        # Get the next option after the option passed in.
        for some_arg in self.some_args:
            # For comparison, change underscores to dashes, but only for
            # those that are options.  Also, use only things up to the first
            # "=" if present.
            compare_arg = some_arg.split("=")[0].replace("_", "-")
            if not self.is_long_option(compare_arg):
                # only use this old string for unknown options (aka values).
                compare_arg = some_arg

            # Since, it looks for things based on string.find() placing
            # the "--" or "-" before the value of argument is ok to
            # handle the substring problem that argument has the "--"
            # and "-" removed.
            if self.is_long_option(argument):
                compare_opt = "--" + argument
            elif self.is_short_option(argument):
                compare_opt = "-" + argument
            else:
                compare_opt = argument

            # Look for the current argument in the list.
            # compare_opt is the current index to find
            # compare_arg comes from the list of arguments.
            if compare_arg == compare_opt:
                # Now that the current item in the argument list is found,
                # make sure it isn't the last and return the next.
                index = self.some_args.index(some_arg)
                if index == len(self.some_args[1:]):  # Nothing can follow.
                    return None
                rtn = self.some_args[index + 1]

                self.some_args = self.some_args[index + 1:]
                return rtn

        return None
    ############################################################################
    # These options remove leading "-" or "--" as appropriate from opt
    # and return.

    def trim_option(self, opt):
        if self.is_long_option(opt):
            return self.trim_long_option(opt)
        elif self.is_short_option(opt):
            return self.trim_short_option(opt)
        else:
            return opt

    @staticmethod
    def trim_long_option(opt):
        # There must be at least 3 characters.  Two from "--" and one
        # alphanumeric character.
        if len(opt) >= 3 and opt[:2] == "--" and (opt[2] in string.letters or
                                                  opt[2] in string.digits):
            return opt[2:]
        else:
            return opt

    @staticmethod
    def trim_short_option(opt):
        if len(opt) and opt[0] == "-" and (opt[1] in string.letters or
                                           opt[1] in string.digits):
            return opt[1:]
        else:
            return opt

    ############################################################################
    # These options return 1 if opt is the correct type of option,
    # otherwise it return 0.

    def is_option(self, opt):
        return self.is_long_option(opt) or self.is_short_option(opt)

    def is_long_option(self, opt):
        opt_check = self.trim_long_option(opt)
        try:
            for key in self.options.keys():
                opt_length = len(opt_check)
                # If the option (switch) matches in part return true.
                # Uniqueness will be tested by getopt.getopt().
                if len(key) >= opt_length and key[:opt_length] == opt_check:
                    return 1
            return 0
        except TypeError:
            return 0

    def is_short_option(self, opt):
        opt_check = self.trim_short_option(opt)
        try:
            for key in self.options.keys():
                if self.options[key].get(SHORT_OPTION, None) == opt_check:
                    return 1
            return 0
        except TypeError:
            return 0

    @staticmethod
    def is_switch_option(opt):
        if len(opt) > 2 and opt[:2] == "--":
            return 1
        elif len(opt) > 1 and opt[0] == "-":
            return 1
        else:
            return 0

    ############################################################################
    # Returns whether the option is a user or admin command.  A return
    # value of one means it is that type, 0 otherwise.

    def is_user_option(self, opt):
        if self.is_long_option(opt):
            if self.options[self.trim_option(opt)].get(
                    USER_LEVEL, USER) == USER:
                return 1
        elif self.is_short_option(opt):
            long_opt = self.short_to_long(opt)
            if self.options[self.trim_option(long_opt)].get(
                    USER_LEVEL, USER) == USER:
                return 1
        return 0

    def is_user2_option(self, opt):
        if self.is_long_option(opt):
            if self.options[self.trim_option(opt)].get(
                    USER_LEVEL, USER) == USER2:
                return 1
        elif self.is_short_option(opt):
            long_opt = self.short_to_long(opt)
            if self.options[self.trim_option(long_opt)].get(
                    USER_LEVEL, USER) == USER2:
                return 1
        return 0

    def is_admin_option(self, opt):
        if self.is_long_option(opt):
            if self.options[self.trim_option(opt)].get(
                    USER_LEVEL, USER) in [ADMIN, HIDDEN]:
                return 1
        elif self.is_short_option(opt):
            long_opt = self.short_to_long(opt)
            if self.options[self.trim_option(long_opt)].get(
                    USER_LEVEL, USER) in [ADMIN, HIDDEN]:
                return 1
        return 0

    def is_hidden_option(self, opt):
        if self.is_long_option(opt):
            if self.options[self.trim_option(opt)].get(
                    USER_LEVEL, USER) in [HIDDEN]:
                return 1
        elif self.is_short_option(opt):
            long_opt = self.short_to_long(opt)
            if self.options[self.trim_option(long_opt)].get(
                    USER_LEVEL, USER) in [HIDDEN]:
                return 1
        return 0

    ############################################################################
    # These option return the correct value from the options dictionary
    # for a given long option long_opt and its dictionary opt_dict.  If
    # the dictionary doesn't have that particular value at hand, then
    # correctly determines the default value.

    @staticmethod
    def get_value_name(opt_dict, long_opt):
        # Determine what the variable's name is.  Use the command string
        # as the default if a "value_name" field is not specified.
        opt_name = opt_dict.get(VALUE_NAME, long_opt)

        # Convert command dashes to variable name underscores.
        opt_name = string.replace(opt_name, "-", "_")

        return opt_name

    @staticmethod
    def get_default_name(opt_dict, long_opt):
        # Determine what the default's name is.  Use the command string
        # as the default if a "default_name" field is not specified.
        if opt_dict.get(EXTRA_OPTION, None):
            opt_name = opt_dict.get(VALUE_NAME, long_opt)
        else:
            opt_name = opt_dict.get(DEFAULT_NAME, long_opt)

        # Convert command dashes to variable name underscores.
        opt_name = string.replace(opt_name, "-", "_")

        return opt_name

    @staticmethod
    def get_default_value(opt_dict, value):
        # Return the DEFAULT_VALUE for an option that takes no value.
        if opt_dict.get(VALUE_USAGE, IGNORED) == IGNORED:
            return opt_dict.get(DEFAULT_VALUE, DEFAULT)
        # Return the DEFAULT_VALUE for an option that takes an optional value.
        elif opt_dict.get(VALUE_USAGE, IGNORED) == OPTIONAL:
            if value is None and opt_dict.get(DEFAULT_VALUE, None):
                return opt_dict[DEFAULT_VALUE]
            elif value is None and opt_dict.get(FORCE_SET_DEFAULT, None):
                return opt_dict.get(DEFAULT_VALUE, DEFAULT)
            else:
                return value
        # Return the DEFAULT_VALUE for an option that must take a value.
        # Usually this will be set to the value passed in, unless
        # FORCE_SET_DEFAULT forces the setting of both values.
        else:  # REQUIRED
            if opt_dict.get(FORCE_SET_DEFAULT, None):
                return opt_dict.get(DEFAULT_VALUE, DEFAULT)
            else:
                return value

    def get_value_type(self, opt_dict):  # , value):
        try:
            if opt_dict.get(VALUE_TYPE, STRING) == INTEGER:
                return int  # int(value)
            elif opt_dict.get(VALUE_TYPE, STRING) == LONG:
                return long
            elif opt_dict.get(VALUE_TYPE, STRING) == FLOAT:
                return float  # float(value)
            elif opt_dict.get(VALUE_TYPE, STRING) == RANGE:
                return self.parse_range  # self.parse_range(value)
            elif opt_dict.get(VALUE_TYPE, STRING) == STRING:
                return str  # str(value)
            elif opt_dict.get(VALUE_TYPE, STRING) == LIST_TYPE:
                return list2  # private function
            else:
                return None  # value
        except ValueError:
            msg = "option %s requires type %s" % \
                  (opt_dict.get('option', ""), opt_dict.get(VALUE_TYPE, STRING))
            self.print_usage(msg)

        return None

    def get_default_type(self, opt_dict):  # , value):
        try:
            if opt_dict.get(DEFAULT_TYPE, STRING) == INTEGER:
                return int  # int(value)
            elif opt_dict.get(DEFAULT_TYPE, STRING) == LONG:
                return long
            elif opt_dict.get(DEFAULT_TYPE, STRING) == FLOAT:
                return float  # float(value)
            elif opt_dict.get(DEFAULT_TYPE, STRING) == RANGE:
                return self.parse_range  # self.parse_range(value)
            elif opt_dict.get(DEFAULT_TYPE, STRING) == STRING:
                return str  # str(value)
            elif opt_dict.get(VALUE_TYPE, STRING) == LIST_TYPE:
                return list2  # Private function
            else:
                return None  # value
        except ValueError:
            msg = "option %s requires type %s" % \
                  (opt_dict.get('option', ""), opt_dict.get(VALUE_TYPE, STRING))
            self.print_usage(msg)

        return None

    ############################################################################
    # These options set the values in the interface class.  set_value() is
    # the function that calls the others.  set_from_dictionary() takes the
    # specific dictionary (which is important when multiple arguments for
    # a single option exist) and sets the interface variables.  The last
    # function, set_extra_values(), handles when more than one argument
    # is parsed for an option.

    def set_value(self, long_opt, value):
        # Make sure the name gets put inside if it isn't there already.
        if not self.options[long_opt].get(VALUE_NAME, None):
            self.options[long_opt][VALUE_NAME] = long_opt

        value_usage = self.options[long_opt].get(VALUE_USAGE, IGNORED)
        extra_values = self.options[long_opt].get(EXTRA_VALUES, [])
        if value_usage == IGNORED and len(extra_values) > 0 and \
                extra_values[0].get(VALUE_USAGE, None) in (REQUIRED,):
            # We get here if the option/switch only defines required
            # arguments in the EXTRA_VALUES section.  We need to
            # pass value_is_used as false so that set_extra_values()
            # knows it still needs to assign the value's value.
            self.set_from_dictionary(self.options[long_opt], long_opt, None)
            # Some options may require more than one value.
            self.set_extra_values(long_opt, value)
        else:
            # Pass in the long option dictionary, long option and its value to
            # use them to set this interfaces variable.
            self.set_from_dictionary(self.options[long_opt], long_opt, value)
            # Some options may require more than one value.
            self.set_extra_values(long_opt, None)

    # Called from set_from_dictionary().
    def __set_value(self, opt_name, opt_typed_value):

        # Handle the LIST case specially.
        if isinstance(opt_typed_value, types.ListType):
            this_list = getattr(self, opt_name, [])
            if not isinstance(this_list, types.ListType):
                print "Developer Error: type of this_list is %s" \
                      % (type(this_list),)
                sys.exit(1)
            # Append the value to the list.
            use_opt_typed_value = this_list + opt_typed_value
        else:
            use_opt_typed_value = opt_typed_value

        setattr(self, opt_name, use_opt_typed_value)

    def set_from_dictionary(self, opt_dict, long_opt, value):

        # place this inside for some error reporting...
        opt_dict['option'] = "--" + long_opt

        # Set value for required situations.
        if value is None and opt_dict.get(VALUE_USAGE, IGNORED) in (REQUIRED,):
            msg = "option %s requires a value" % (long_opt,)
            self.print_usage(msg)

        if value is not None and \
                opt_dict.get(VALUE_USAGE, IGNORED) in (REQUIRED, OPTIONAL):
            try:
                # Get the name to set.
                opt_name = self.get_value_name(opt_dict, long_opt)
                # Get the value in the correct type to set.
                opt_type = self.get_value_type(opt_dict)  # , value)
                if opt_type is not None:
                    opt_typed_value = apply(opt_type, (value,))
                else:
                    opt_typed_value = value
            except SystemExit as msg:
                raise msg
            except SystemError:
                msg = sys.exc_info()[1]
                self.print_usage(str(msg))
                return

            self.__set_value(opt_name, opt_typed_value)

            # keep this list up to date for finding the next argument.
            if opt_dict.get(EXTRA_VALUES, None) and len(self.some_args) >= 3:
                self.some_args = self.some_args[1:]
            elif len(self.some_args) < 3:
                self.some_args = self.some_args[2:]
            elif opt_dict.get(EXTRA_VALUES, None) and \
                    not self.is_option(self.some_args[2]):
                self.some_args = self.some_args[2:]
            # elif self.some_args[1] == value:
            #     self.some_args = self.some_args[2:]
            elif opt_dict.get(VALUE_USAGE, IGNORED) in (REQUIRED,) \
                    and not opt_dict.get(EXTRA_OPTION, None):
                self.some_args = self.some_args[2:]
            else:
                self.some_args = self.some_args[1:]

        # Set value for non-existent optional value.
        elif value is None \
                and opt_dict.get(VALUE_USAGE, IGNORED) in (OPTIONAL,):
            try:
                # Get the name to set.
                opt_name = self.get_value_name(opt_dict, long_opt)
                # Get the value to set.
                value = self.get_default_value(opt_dict, value)
                # Get the value in the correct type to set.
                opt_type = self.get_default_type(opt_dict)  # , value)
                if value is None:
                    opt_typed_value = value
                elif opt_type is not None:
                    opt_typed_value = apply(opt_type, (value,))
                else:
                    opt_typed_value = value
            except SystemExit, msg:
                raise msg
            except SystemError:
                msg = sys.exc_info()[1]
                self.print_usage(str(msg))
                return

            self.__set_value(opt_name, opt_typed_value)

            # keep this list up to date for finding the next argument.
            self.some_args = self.some_args[1:]

        # There is no value or the default  should be forced set anyway.
        elif value is None \
                and opt_dict.get(VALUE_USAGE, IGNORED) in (IGNORED,) \
                or opt_dict.get(FORCE_SET_DEFAULT, None):
            try:
                # Get the name to set.
                opt_name = self.get_default_name(opt_dict, long_opt)
                # Get the value in the correct type to set.
                opt_value = self.get_default_value(opt_dict, value)
                # Get the value in the correct type to set.
                opt_type = self.get_default_type(opt_dict)  # , opt_value)
                if opt_type is not None:
                    opt_typed_value = apply(opt_type, (opt_value,))
                else:
                    opt_typed_value = opt_value
            except SystemExit, msg:
                raise msg
            except SystemError:
                msg = sys.exc_info()[1]
                self.print_usage(str(msg))
                return

            self.__set_value(opt_name, opt_typed_value)

            # keep this list up to date for finding the next argument.
            if opt_dict.get(EXTRA_VALUES, None) is None:
                self.some_args = self.some_args[1:]

        # For the cases where this needs to be set also.
        if opt_dict.get(FORCE_SET_DEFAULT, None):
            try:
                # Get the name to set.
                opt_name = self.get_default_name(opt_dict, long_opt)
                # Get the value in the correct type to set.
                opt_value = self.get_default_value(opt_dict, value)
                # Get the value in the correct type to set.
                opt_type = self.get_default_type(opt_dict)  # , opt_value)
                if opt_type is not None:
                    opt_typed_value = apply(opt_type, (opt_value,))
                else:
                    opt_typed_value = opt_value
            except SystemExit, msg:
                raise msg
            except SystemError:
                msg = sys.exc_info()[1]
                self.print_usage(str(msg))
                return

            self.__set_value(opt_name, opt_typed_value)

    def set_extra_values(self, opt, value):
        if self.is_short_option(opt):
            long_opt = self.short_to_long(opt)
        else:
            long_opt = opt

        extras = self.options[self.trim_option(long_opt)].get(
            EXTRA_VALUES, [])

        next_arg = None

        value_is_used = False
        for extra_option in extras:
            if value and not value_is_used:
                next_arg = value
            elif next_arg:
                next_arg = self.next_argument(next_arg)
            else:
                next_arg = self.next_argument(opt)

            if extra_option[VALUE_USAGE] == IGNORED:
                next_arg = None
            elif extra_option[VALUE_USAGE] in [REQUIRED, OPTIONAL] and \
                    next_arg is not None and self.is_option(next_arg) and \
                    self.is_switch_option(next_arg):
                next_arg = None

            extra_option[EXTRA_OPTION] = 1  # This is sometimes important...
            self.set_from_dictionary(extra_option, long_opt, next_arg)
            try:
                # Keep the list of arguments still to process correct.
                # If the first required argument is specified in extra_values
                # then we don't need to remove it from the list, because
                # it is already not there.
                if next_arg and (not value or (value and value_is_used)):
                    self.args.remove(next_arg)
            except ValueError:
                try:
                    sys.stderr.write("Problem processing argument %s.\n" %
                                     (next_arg,))
                    sys.stderr.flush()
                except IOError:
                    pass

            value_is_used = True  # Set this back to the default.


############################################################################

if __name__ == '__main__':
    intf = Interface()

    # print the options value
    for arg in dir(intf):
        if string.replace(arg, "_", "-") in intf.options.keys():
            print arg, type(getattr(intf, arg)), ": ",
            pprint.pprint(getattr(intf, arg))

    print

    # every other matched value
    for arg in dir(intf):
        if string.replace(arg, "_", "-") not in intf.options.keys():
            print arg, type(getattr(intf, arg)), ": ",
            pprint.pprint(getattr(intf, arg))

    print

    if intf.args:
        print "unprocessed args:", intf.args

    if getattr(intf, "help", None):
        intf.print_help()
    if getattr(intf, "usage", None):
        intf.print_usage()
