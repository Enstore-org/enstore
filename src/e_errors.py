###############################################################################
# src/$RCSfile$   $Revision$
#

import sys
import string

OK         = 'ok'
TIMEDOUT = 'TIMEDOUT'
KEYERROR = 'KEYERROR'
CONFIGDEAD = 'CONFIGDEAD'
DOESNOTEXIST = 'DOESNOTEXIST'
EEXIST = 'EEXIST'
WRONGPARAMETER = 'WRONGPARAMETER'
NOWORK = 'nowork'
NOMOVERS = 'nomovers'
MOUNTFAILED = 'MOUNTFAILED'
DISMOUNTFAILED = 'DISMOUNTFAILED'
MEDIA_IN_ANOTHER_DEVICE =  'media_in_another_device'
MEDIAERROR = 'MEDIAERROR'
USERERROR = 'USERERROR'
DRIVEERROR = 'DRIVEERROR'
UNKNOWNMEDIA = 'UNKNOWNMEDIATYPE'
NOVOLUME = 'NOVOLUME'
NOACCESS = 'NOACCESS'
NOTALLOWED = 'NOTALLOWED'
CONFLICT = 'CONFLICT'
TOOMANYSUSPVOLS = 'TOOMANYSUSPVOLS'
UNKNOWN = 'UNKNOWN'
NOALARM = 'NOALARM'
SERVERDIED = 'SERVERDIED'
CANTRESTART = 'CANTRESTART'
DELETED = 'DELETED'
NOSPACE = 'NOSPACE'
INPROGRESS = 'INPROGRESS'
VOL_SET_TO_FULL = 'VOLISSETTOFULL'
RECYCLE='RECYCLE_VOLUME'
QUOTAEXCEEDED='STORAGE_QUOTA_EXCEEDED'
CRC_ERROR='CRC MISMATCH'
NO_CRC_RETURNED = 'mover did not return CRC'
NET_ERROR="NET_ERROR"
RETRY="RETRY"
TOO_MANY_RETRIES="TOO MANY RETRIES"
TOO_MANY_RESUBMITS="TOO_MANY_RESUBMITS"
BROKEN="BROKEN"
EPROTO="PROTOCOL_ERROR"
IOERROR = "IO ERROR"
ENSTOREBALLRED = "ENSTORE BALL IS RED"
MALFORMED = "MALFORMED REQUEST"
RESUBMITTING = "RESUBMITTING"
VERSION_MISMATCH="VERSION MISMATCH"
WRONG_PNFS_FILE_SIZE="CANNOT SET PNFS FILE SIZE"
LOCKED="locked"
NOREAD="noread"
NOWRITE="nowrite"
OSERROR = "OS ERROR"

# Severity codes
# NOTE: IMPORTANT, THESE VALUES CORRESPOND TO "TRACE LEVELS" AND CHANGING
#       THEM WILL IMPACT OTHER PARTS OF THE SYSTEM
ALARM      = 0
ERROR      = 1
USER_ERROR = 2
WARNING    = 3
INFO       = 4
MISC       = 5

# severity translator
sevdict = { ALARM      : 'A',
            ERROR      : 'E',
            USER_ERROR : 'U',
            WARNING    : 'W',
            INFO       : 'I',
            MISC       : 'M'
            }

# Alarm severities
DEFAULT_SEVERITY = sevdict[WARNING]
DEFAULT_ROOT_ERROR = UNKNOWN

# Exceptions that are raised
#-------------------------------------
TCP_EXCEPTION = "TCP connection closed"
NOT_ALWD_EXCEPTION = "Not allowed"
CLEANUDP_EXCEPTION = "mis-use of class cleanUDP"
VM_PNFS_EXCEPTION = "Pnfs error"
VM_ENSTORE_EXCEPTION = "enstore error"
VM_CONF_EXCEPTION = "conf.sh failed"
NO_FC_EXCEPTION = "can't find file clerk in conf.sh"
NO_PNFS_EXCEPTION = "Not a full path "
POSIT_EXCEPTION = "XXX Positioning error"


# Tape Errors:
#--------------------------------------
# Write Error:
WRITE_NOTAPE    = 'WRITE_NOTAPE'
WRITE_TAPEBUSY  = 'WRITE_TAPEBUSY'
WRITE_BADMOUNT  = 'WRITE_BADMOUNT'
WRITE_BADSWMOUNT = 'WRITE_BADSWMOUNT'
WRITE_BADSPACE  = 'WRITE_BADSPACE'
WRITE_ERROR     = 'WRITE_ERROR'
WRITE_EOT       = 'WRITE_EOT'




# Read Errors:
READ_NOTAPE     = 'READ_NOTAPE'
READ_TAPEBUSY   = 'READ_TAPEBUSY'
READ_BADMOUNT   = 'READ_BADMOUNT'
READ_BADSWMOUNT = 'READ_BADSWMOUNT'
READ_BADLOCATE  = 'READ_BADLOCATE'
READ_ERROR      = 'READ_ERROR'
READ_EOT        = 'READ_EOT'
READ_EOD        = 'READ_EOD'

## Volume label errors
# read error trying to check VOL1 header, reading/writing hsm
READ_VOL1_READ_ERR = "READ_VOL1_READ_ERR"
WRITE_VOL1_READ_ERR = "WRITE_VOL1_READ_ERR"
# VOL1 header missing, reading/writing hsm
READ_VOL1_MISSING = "READ_VOL1_MISSING"
WRITE_VOL1_MISSING = "WRITE_VOL1_MISSING"
# VOL1 header present, but incorrect volume
READ_VOL1_WRONG = "READ_VOL1_WRONG"
WRITE_VOL1_WRONG = "WRITE_VOL1_WRONG"
EOV1_ERROR = "EOV1_ERROR"


#---------------------------------------

#Media changer errors:
MC_VOLNOTHOME = 9999   # not in home position in tower
MC_DRVNOTEMPTY = 9998  # drive already has volume mounted in it
MC_NONE =  9997        # code is None - very bad - CHECK ROBOT
MC_FAILCHKVOL = 9996   # check of volume failed
MC_VOLNOTFOUND = 9995  # volume not found
MC_FAILCHKDRV = 9994   # check of volume failed
MC_DRVNOTFOUND = 9993  # volume not found

#---------------------------------------

#Other Errors:
ENCP_GONE       = 'ENCP_GONE'
TCP_HUNG        = 'TCP_HUNG'
MOVER_CRASH     = 'MOVER_CRASH'
BELOW_THRESHOLD = 'BELOW_THRESHOLD'
ABOVE_THRESHOLD = 'ABOVE_THRESHOLD'


#V2 additions:
MOVER_BUSY = 'MOVER_BUSY'


## Retry policy:
## 3 strikes and you're out.  Also, locally-caught errors (permissions, no such file) are not retried.
non_retriable_errors = ( NOACCESS, # set by enstore
                         NOTALLOWED, #set by admin
                         USERERROR,
                         UNKNOWNMEDIA, # comes from volume clerk - unknown type
                         NOVOLUME, #unknown volume
                         DELETED,
                         QUOTAEXCEEDED,
                         TOO_MANY_RETRIES, #atttempts with failure
                         TOO_MANY_RESUBMITS, #attempts without trying
                         MALFORMED,
                         VERSION_MISMATCH, #ENCP to old
                         WRONG_PNFS_FILE_SIZE,
                         LOCKED,           # Library is locked for the access
                         NOREAD,           # Library is locked for the read access
                         NOWRITE,           # Library is locked for the write access

                         )

raise_alarm_errors = ( CRC_ERROR,
                       UNKNOWNMEDIA,
                       NOVOLUME,
                       QUOTAEXCEEDED)


# CLIENT PORTION OF 'MESS_TYPE' MESSAGE
ctypedict = {  "checkpoint"      : "CP",
               "fc"              : "FC",          # FILE CLERK
               "alarm_srv"       : "AS",          # ALARM SERVER
               "filsrv"          : "FS",
               "volsrv"          : "VS",
               "server"          : "SVR",
               "encp"            : "ENCP",
               "lm"              : "LM",          # LIBRARY MANAGER
               "vc"              : "VC",          # VOLUME CLERK
               "mc"              : "MC",          # MEDIA CHANGER
               "re"              : "RE",
               "mvr"             : "MVR",         # MOVER
               "backup"          : "BU" }

# FUNCTION PORTION OF 'MESS_TYPE' MESSAGE
# ******* NOTE ******* DON'T PUT 'OK' FOR STRING CHECK. THE STRING CHECK WILL
# ALWAYS FIND SOMETHING AND MARK THE MESSAGE AS BEING SUCCESSFUL WHEN THERE
# MAY HAVE BEEN AN ERROR. IT'S BEST TO PUT 'OK' IN THE DICTIONARY FOR EACH
# OCCURRENCE THAT IT IS NEEDED.
ftypedict = { "read"          : "READ",
              "r"             : "READ",
              "write"         : "WRITE",
              "writing"       : "WRITE",
              "w"             : "WRITE",
              "write_file"    : "WRITE",
              "write_wrapper" : "WRITE",
              "write_hsm"     : "WRITE_HSM",
              "exception"     : "EXCEPTION",
              "no_acc"        : "NO_ACC",
              "server_died"   : "SVR",
              "mount"         : "MOUNT",
              "mounting"      : "MOUNT",
              "load"          : "MOUNT",
              "loading"       : "MOUNT",
              "filedb"        : "FILEDB",
              "voldb"         : "VOLDB",
              "insert_vol"    : "INSERT",
              "insert"        : "INSERT",
              "cant_restart"  : "RESTART",
              "volume"        : "VOL",
              "vol"           : "VOL",
              "vol_err"       : "VOL",
              "file"          : "FILE",
              "system"        : "SYS",
              "unmount"       : "DISM",
              "dismount"      : "DISM",
              "dismounting"   : "DISM",
              "unload"        : "DISM",
              "offline/eject" : "DISM",
              "offline_eject" : "DISM",
              "eject"         : "DISM",
              "bind"          : "BIND",
              "unbind"        : "UNBIND",
              "unbind_vol"    : "UNBIND",
              "list"          : "LIST",
              "list."         : "LIST",
              "fd_xfers"      : "XFER",
              "fd_xfer"       : "XFER",
              "xfer"          : "XFER",
              "copy"          : "XFER",
              "copying"       : "XFER",
              "move"          : "XFER",
              "moving"        : "XFER",
              "->"            : "XFER" }

# SEVERITY PORTION OF 'MESS_TYPE' MESSAGE
stypedict = { "died"               : "DIED",
              "server_died"        : "DIED",
              "ts"                 : "TS",
              "write_file"         : "FILE",
              "write_wrapper"      : "WRAP",
              "q'd"                : "QUEUED",
              "queue"              : "QUEUED",
              "queued"             : "QUEUED",
              "check"              : "CHECK",
              "check_suc"          : "CHECK_SUC",
              "insert_vol"         : "VOL",
              "unbind_vol"         : "VOL",
              "open"               : "OPEN",
              "opening"            : "OPEN",
              "close"              : "CLOSE",
              "closing"            : "CLOSE",
              "request"            : "REQ",
              "requested"          : "REQ",
              "requesting"         : "REQ",
              "hurrah"             : "SUC",
              "success"            : "SUC",
              "successful"         : "SUC",
              "successful'"        : "SUC",
              "wrote"              : "SUC",
              "copied"             : "SUC",
              "xfered"             : "SUC",
              "xfer'd"             : "SUC",
              "added"              : "SUC",
              "ok"                 : "SUC",
              "'ok'"               : "SUC",
              "'ok',"              : "SUC",
              "('ok',"             : "SUC",
              "status('ok',"       : "SUC",
              "done"               : "SUC",
              "end"                : "SUC",
              "ending"             : "SUC",
              "completed"          : "SUC",
              "complete"           : "SUC",
              "finish"             : "SUC",
              "finishing"          : "SUC",
              "finished"           : "SUC",
              "performed"          : "SUC",
              "add"                : "ADD",
              "adding"             : "ADD",
              "start"              : "START",
              "begin"              : "START",
              "starting"           : "START",
              "beginning"          : "START",
              "perform"            : "START",
              "performing"         : "START",
              "work"               : "START",
              "stop"               : "STOP",
              "next"               : "NEXT",
              "restart"            : "RESTART",
              "update_client_info" : "UCI",
              "error"              : "ERR",
              "vol_err"            : "ERR",
              "cant_restart"       : "ERR",
              "fail"               : "ERR",
              "bad"                : "ERR",
              "not"                : "ERR",
              "can't"              : "ERR",
              "cant"               : "ERR",
              "e"                  : "ERR",
              "i"                  : "INFO",
              "a"                  : "ALARM",
              "w"                  : "WARNING",
              "u"                  : "USERERR",
              "m"                  : "MISCERR" }

def is_retriable(e):
    if e in non_retriable_errors:
        return 0
    elif e in raise_alarm_errors:
        return 0
    return 1

