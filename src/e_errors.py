###############################################################################
# src/$RCSfile$   $Revision$
#

TIMEDOUT = 'TIMEDOUT'
KEYERROR = 'KEYERROR'
OK         = 'ok'
DOESNOTEXIST = 'DOESNOTEXIST'
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
CONFLICT = 'CONFLICT'
TOOMANYSUSPVOLS = 'TOOMANYSUSPVOLS'
UNKNOWN = 'UNKNOWN'
NOALARM = 'NOALARM'
SERVERDIED = 'SERVERDIED'
CANTRESTART = 'CANTRESTART'
DELETED = 'DELETED'
NOSPACE = 'NOSPACE'
BROKENPIPE = 'BROKENPIPE'
if 0: print KEYERROR,OK,DOESNOTEXIST,WRONGPARAMETER,NOWORK,\
            NOMOVERS,MOUNTFAILED,DISMOUNTFAILED,\
            MEDIA_IN_ANOTHER_DEVICE,MEDIAERROR,USERERROR,\
            DRIVEERROR,UNKNOWNMEDIA,NOVOLUME,NOACCESS,CONFLICT,\
            TIMEDOUT,TOOMANYSUSPVOLS,UNKNOWN,NOALARM, SERVERDIED,\
            CANTRESTART, DELETED, NOSPACE # lint fix

# Severity codes
# NOTE: IMPORTANT, THESE VALUES CORRESPOND TO "TRACE LEVELS" AND CHNAGING
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

if 0: print ERROR,USER_ERROR,WARNING,INFO,MISC, \
   sevdict,DEFAULT_SEVERITY,DEFAULT_ROOT_ERROR  # lint fix

# Tape Errors:
#--------------------------------------
# Write Error:
WRITE_NOTAPE    = 'WRITE_NOTAPE"'
WRITE_TAPEBUSY  = 'WRITE_TAPEBUSY'
WRITE_BADMOUNT  = 'WRITE_BADMOUNT'
WRITE_BADSPACE  = 'WRITE_BADSPACE'
WRITE_ERROR     = 'WRITE_ERROR'
WRITE_EOT       = 'WRITE_EOT'
WRITE_UNLOAD    = 'WRITE_UNLOAD'
WRITE_NOBLANKS  = 'WRITE_NOBLANKS'

if 0: print WRITE_NOTAPE,WRITE_TAPEBUSY,WRITE_BADMOUNT,WRITE_BADSPACE,\
            WRITE_ERROR,WRITE_EOT,WRITE_UNLOAD,UNMOUNT,WRITE_NOBLANKS # lint fix

# Read Errors:
READ_NOTAPE     = 'READ_NOTAPE'
READ_TAPEBUSY   = 'READ_TAPEBUSY'
READ_BADMOUNT   = 'READ_BADMOUNT'
READ_BADLOCATE  = 'READ_BADLOCATE'
READ_ERROR      = 'READ_ERROR'
READ_COMPCRC    = 'READ_COMPCRC'
READ_EOT        = 'READ_EOT'
READ_EOD        = 'READ_EOD'
READ_UNLOAD     = 'READ_UNLOAD'

# common for read or write
UNMOUNT         = 'UNMOUNT'

if 0: print READ_NOTAPE,READ_TAPEBUSY,READ_BADMOUNT,READ_BADLOCATE,\
            READ_ERROR,READ_COMPCRC,READ_EOT,READ_EOD,READ_UNLOAD,UNMOUNT # lint fix

#---------------------------------------

#Other Errors:
ENCP_GONE       = 'ENCP_GONE'
TCP_HUNG        = 'TCP_HUNG'
MOVER_CRASH     = 'MOVER_CRASH'

if 0: print ENCP_GONE,TCP_HUNG,MOVER_CRASH #lint fix

non_retriable_errors = (NOMOVERS, NOACCESS,
                        WRONGPARAMETER, MOUNTFAILED, DISMOUNTFAILED,
                        USERERROR, UNKNOWNMEDIA, NOVOLUME,
                        WRITE_NOTAPE, WRITE_NOBLANKS,
                        READ_NOTAPE, READ_BADMOUNT,
                        READ_BADLOCATE, READ_UNLOAD,
                        UNMOUNT, DELETED,NOSPACE,BROKENPIPE)

def is_retriable(e):
    if e in non_retriable_errors:
        return 0
    return 1

# log traceback info
def handle_error(exc=None, value=None, tb=None):
    import Trace
    import traceback
    # store Trace back info
    if not exc:
	import sys
	exc, value, tb = sys.exc_type, sys.exc_value, sys.exc_traceback
    # log it
    for l in traceback.format_exception( exc, value, tb ):
	#print l[0:len(l)-1]
	Trace.log( ERROR, l[0:len(l)-1] )
    return exc, value, tb








