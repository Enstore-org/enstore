###############################################################################
# src/$RCSfile$   $Revision$
#

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
WRITE_UNMOUNT   = 'WRITE_UNMOUNT'
WRITE_NOBLANKS  = 'WRITE_NOBLANKS'

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
READ_UNMOUNT    = 'READ_UNMOUNT'

#---------------------------------------

#Other Errors:
ENCP_GONE       = 'ENCP_GONE'
TCP_HUNG        = 'TCP_HUNG'
MOVER_CRASH     = 'MOVER_CRASH'


def is_retriable(error):
    if error == NOMOVERS or \
       error == WRONGPARAMETER or \
       error == MOUNTFAILED or \
       error == USERERROR  or \
       error == UNKNOWNMEDIA or \
       error == NOVOLUME or \
       error == WRITE_NOTAPE or \
       error == WRITE_NOBLANKS or \
       error == READ_NOTAPE or \
       error == READ_BADMOUNT or \
       error == READ_BADLOCATE or \
       error == READ_UNLOAD or \
       error == READ_UNMOUNT:
	return 0
    return 1
    
    

