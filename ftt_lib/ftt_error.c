static char rcsid[] = "@(#)$Id$";
#include <stdarg.h>
#include <stdio.h>
#include <ftt_private.h>
#include <stdlib.h>

#ifdef WIN32
#include <windows.h>
#include <winioctl.h>

int ftt_describe_error_WIN();
#endif

int ftt_describe_error();
int ftt_verify_blank(ftt_descriptor d);

/*
** ftt_eprintf call...
** we'll make it more robust later...
*/
void
ftt_eprintf(char *format, ...)
{
  va_list args;

  va_start(args, format);
  vsprintf(ftt_eprint_buf, format, args);
  va_end(args);
}

char *
ftt_get_error(int *pn){
   if (pn != 0) {
	*pn = ftt_errno;
   }
   return ftt_eprint_buf;
}

char *ftt_ascii_error[] = {
/* FTT_SUCCESS		 0 */ "FTT_SUCCESS",
/* FTT_EPARTIALSTAT	 1 */ "FTT_EPARTIALSTAT",
/* FTT_EUNRECOVERED	 2 */ "FTT_EUNRECOVERED",
/* FTT_ENOTAPE		 3 */ "FTT_ENOTAPE",
/* FTT_ENOTSUPPORTED 	 4 */ "FTT_ENOTSUPPORTED",
/* FTT_EPERM		 5 */ "FTT_EPERM",
/* FTT_EFAULT		 6 */ "FTT_EFAULT",
/* FTT_ENOSPC		 7 */ "FTT_ENOSPC",
/* FTT_ENOENT		 8 */ "FTT_ENOENT",
/* FTT_EIO		 9 */ "FTT_EIO",
/* FTT_EBLKSIZE		10 */ "FTT_EBLKSIZE",
/* FTT_ENOEXEC		11 */ "FTT_ENOEXEC",
/* FTT_EBLANK           12 */ "FTT_EBLANK", 
/* FTT_EBUSY		13 */ "FTT_EBUSY",
/* FTT_ENODEV		14 */ "FTT_ENODEV",
/* FTT_ENXIO		15 */ "FTT_ENXIO",
/* FTT_ENFILE		16 */ "FTT_ENFILE",
/* FTT_EROFS		17 */ "FTT_EROFS",
/* FTT_EPIPE		18 */ "FTT_EPIPE",
/* FTT_ERANGE		19 */ "FTT_ERANGE",
/* FTT_ENOMEM		20 */ "FTT_ENOMEM",
/* FTT_ENOTTAPE		21 */ "FTT_ENOTTAPE",
/* FTT_E2SMALL		22 */ "FTT_E2SMALL",
/* FTT_ERWFS		23 */ "FTT_ERWFS",
/* FTT_EWRONGVOL	24 */ "FTT_EWRONGVOL",
/* FTT_EWRONGVOLTYP	25 */ "FTT_EWRONGVOLTYP",
/* FTT_ELEADER		26 */ "FTT_ELEADER",
/* FTT_EFILEMARK	27 */ "FTT_EFILEMARK",
/* FTT_ELOST		28 */ "FTT_ELOST",
/* FTT_ENOTBOT  	29 */ "FTT_ENOTBOT",
/* FTT_MAX_ERROR	30 */ "FTT_MAX_ERROR",
0 
};

static char *messages[] = {
	/* FTT_SUCCESS		 0 */
    "that no error has occurred",
	/* FTT_EPARTIALSTAT	 1 */ 
    "that not all of the statistics which should have been avaliable\n\
	on this drive and system were able to be obtained.",
	/* FTT_EUNRECOVERED	 2 */ 
    "\tWe are now not at an unknown tape position and are \n\
	unable to proceed without possibly damaging the tape.  This \n\
	message will repeat until a rewind of the tape is performed.\n",
	/* FTT_ENOTAPE		 3 */ 
    "that no tape is currently in the tape drive.",
	/* FTT_ENOTSUPPORTED 	 4 */ 
    "that the device/tape drive combination is not supported.",
	/* FTT_EPERM		 5 */ 
    "that you do not have permissions to access the system device.",
	/* FTT_EFAULT		 6 */ 
    "that you have provided an invalid buffer address.",
	/* FTT_ENOSPC		 7 */ 
    "that the data you requested will not fit in the provided buffer.",
	/* FTT_ENOENT		 8 */ 
    "that the system device for the mode and density you requested\n\
	does not exist on this system.",
	/* FTT_EIO		 9 */
    "that an unrecoverable error occurred due to bad or damaged tape \n\
	or bad or dirty tape heads.",
	/* FTT_EBLKSIZE		10 */ 
    "that the block size you tried to write is not appropriate for \n\
	this device and mode.",
	/* FTT_ENOEXEC		11 */ 
    "that the setuid executable needed to perform this operation on \n\
	this system is not avaliable.",
	/* FTT_EBLANK		12 */
    "that we encountered blank tape or end of tape.",
	/* FTT_EBUSY		13 */
    "that some other process is using the drive at this time.",
	/* FTT_ENODEV		14 */
    "that while the device for this mode and density appears in /dev,\n\
	the device driver is not configured for it.",
	/* FTT_ENXIO		15 */
    "that you have tried to go past the end of the tape.",
	/* FTT_ENFILE		16 */ 
    "that you have tried to open more files simultaneously than is \n\
	possible on this system.",
	/* FTT_EROFS		17 */ 
    "that the tape is write protected.",
	/* FTT_EPIPE		18 */
    "that the process which was invoked to perform this task on our \n\
	behalf died unexpectedly.",
	/* FTT_ERANGE		19 */
    "The buffer you provided for tape data was smaller than the data \n\
     block on the tape.",
	/* FTT_ENOMEM		20 */
    "that we were unable to allocate memory needed to perform the task.",
	/* FTT_ENOTTAPE		21 */
    "that the device specified was not a tape.",
	/* FTT_E2SMALL		22 */
    "that the block size issued is smaller than this device can handle.",
	/* FTT_ERWFS		23 */
    "that the tape was supposed to be write protected and is writable.",
	/* FTT_EWRONGVOL	24 */
    "The wrong tape volume was given to the volume verification",
	/* FTT_EWRONGVOLTYP	25 */
    "The wrong type of tape volume was given to the volume verification",
	/* FTT_ELEADER		26 */
    "Beginning of tape was encountered before completing the operation",
	/* FTT_EFILEMARK	27 */
    "A Filemark was encountered before completing the operation",
	/* FTT_ELOST	28 */
    "We do not yet know our current tape position.",
	/* FTT_ENOTBOT  	29 */ 
    "FTT_ENOTBOT",
	/* FTT_MAX_ERROR	30 */ 
    "FTT_MAX_ERROR",
    0
};

extern int errno;
static int keep_errno;

int
ftt_translate_error(ftt_descriptor d, int opn, char *op, int res, char *what, int recoverable) {
    int terrno;
    static ftt_stat sbuf;
    char *p;
    int save1, save2;

    keep_errno = errno;

    DEBUG3(stderr,"Entering ftt_translate_error -- opn == %d, op = %s, res=%d, what=%s recoverable=%d\n",
	opn,op, res, what, recoverable);

    if( 0 == d ) {
	ftt_eprintf("%s called with NULL ftt_descriptor\n", op);
	ftt_errno = FTT_EFAULT;
	return -1;
    }

    if (keep_errno == 75) {	/* linux gives this when out of buffers... */
	terrno = ENOMEM;
        ftt_errno = d->errortrans[opn][terrno];
	errno = keep_errno;
        return ftt_describe_error(d, opn, op, res, what, recoverable);
    }
    if (keep_errno >= MAX_TRANS_ERRNO) {
        terrno = MAX_TRANS_ERRNO - 1;
    } else {
	terrno = keep_errno;
    } 

    ftt_errno = d->errortrans[opn][terrno];

#   define CHECKS (FTT_OP_SKIPFM|FTT_OP_RSKIPFM|FTT_OP_SKIPREC|FTT_OP_RSKIPREC\
			|FTT_OP_READ)

    if ((0 == res && FTT_OPN_READ == opn && 0 !=(d->flags&FTT_FLAG_VERIFY_EOFS))
    		|| (-1 == res && ((1<<opn)&CHECKS) )) {
	/* 
	** save errno and ftt_errno so we can restore them 
	** after getting status 
	*/
        save1 = ftt_errno;
	save2 = errno;

	ftt_get_stats(d, &sbuf);
	errno = save2;

	if (0 != (p = ftt_extract_stats(&sbuf,FTT_SENSE_KEY)) && 8 == atoi(p)) {
	    DEBUG3(stderr, "Saw blank check sense key\n");
	    res = -1;
	    save1 = ftt_errno = FTT_EBLANK;
	} else {
	    DEBUG3(stderr, "Sense key was %s\n", p);
	    ftt_errno = save1;
	    if (0 != (p = ftt_extract_stats(&sbuf,FTT_BLOC_LOC))) {
		DEBUG3(stderr, "Current loc %s, last loc %d\n", p, d->last_pos);
		if ((d->last_pos > 0 && atoi(p) == d->last_pos) || atoi(p) == 0) {
		    ftt_errno = FTT_EBLANK;
		    res = -1;
		} else {
		    ftt_errno = save1;
		}
		d->last_pos = atoi(p);
	    } else if (0 != (p = ftt_extract_stats(&sbuf,FTT_REMAIN_TAPE))) {
		DEBUG3(stderr, "Current remain %s, last remain %d\n", p, d->last_pos);
		if (d->last_pos > 0 && atoi(p) == d->last_pos) {
		    ftt_errno = FTT_EBLANK;
		    res = -1;
		} else {
		    ftt_errno = save1;
		}
		d->last_pos = atoi(p);
	    } else {
		ftt_errno = save1;
	    }

	    /*  if we didn't do a SCSI read ourselves, and we're at BOT,  */
	    /*  but we do do some scsi operations,			  */
	    /*  have verify_blank double check the report so we know it's */
	    /*  not a mis-diagnosed error.                                */

	    if (FTT_EBLANK == ftt_errno && opn == FTT_OPN_READ &&
			d->current_file == 0 && d->current_block == 0 &&
			(d->scsi_ops & FTT_OP_READ) == 0 && d->scsi_ops != 0 ) {
		save1 = ftt_errno;
		res = ftt_verify_blank(d);
		if ( 0 <= res && ftt_errno == FTT_SUCCESS) {
		     ftt_errno = FTT_EIO;
		     res = -1;
		}
	    }
	}
	
        if (FTT_EBLANK == ftt_errno && atoi(ftt_extract_stats(&sbuf,FTT_BOT))) {
	    ftt_errno == FTT_ELEADER;
	}
    }
    if (FTT_EBLANK == ftt_errno && opn == FTT_OPN_WRITE || opn == FTT_OPN_WRITEFM ) {

	/* people don't take  "Blank" seriously on writes... */

	ftt_errno = FTT_EIO;
    }

    errno = keep_errno;
    return ftt_describe_error(d, opn, op, res, what, recoverable);
}

int
ftt_describe_error(ftt_descriptor d, int opn, char *op, int res, char *what, int recoverable) {

    if (0 <= res) {
	ftt_errno = FTT_SUCCESS;
	return res;
    }
    ftt_eprintf( "\
%s: doing %s on %s returned %d,\n\
	errno %d, => ftt error %s(%d), meaning \n\
	%s\n%s",

	what, op,  (d->which_is_open >= 0 ? 
				d->devinfo[d->which_is_open].device_name :
				d->basename),
	res, errno, ftt_ascii_error[ftt_errno], ftt_errno,
	messages[ftt_errno], recoverable ? "": messages[FTT_EUNRECOVERED] );

    DEBUG2(stderr, "ftt_translate_error -- message is:\n%s", ftt_eprint_buf);

    if (!recoverable) {
	d->unrecovered_error = opn < FTT_OPN_WRITEFM ? 1 : 2;
	d->current_valid = 0;
    }

    return res;
}

#ifdef WIN32

static int
ftt_WIN_error (DWORD res) {
	
	switch (res) {
		case NO_ERROR						: return FTT_SUCCESS;
		case ERROR_BEGINNING_OF_MEDIA		: return FTT_ELEADER;
		case ERROR_BUS_RESET				: return FTT_EUNRECOVERED;
		case ERROR_END_OF_MEDIA				: return FTT_ENOSPC;
		case ERROR_FILEMARK_DETECTED		: return FTT_EFILEMARK;
		case ERROR_SETMARK_DETECTED			: return FTT_ENOTSUPPORTED;
		case ERROR_NO_DATA_DETECTED			: return FTT_EBLANK;
		case ERROR_PARTITION_FAILURE		: return FTT_ENOTSUPPORTED;
		case ERROR_INVALID_BLOCK_LENGTH		: return FTT_EBLKSIZE;
		case ERROR_DEVICE_NOT_PARTITIONED	: return FTT_ENOTSUPPORTED;
		case ERROR_MEDIA_CHANGED			: return FTT_EUNRECOVERED;
		case ERROR_NO_MEDIA_IN_DRIVE		: return FTT_ENOTAPE;
		case ERROR_NOT_SUPPORTED			: return FTT_ENOTSUPPORTED;
		case ERROR_UNABLE_TO_LOCK_MEDIA		: return FTT_ENOTSUPPORTED;
		case ERROR_UNABLE_TO_UNLOAD_MEDIA	: return FTT_ENOTSUPPORTED;
		case ERROR_WRITE_PROTECT			: return FTT_EROFS;
	
		case ERROR_INVALID_FUNCTION			: return FTT_ENOENT;
		case ERROR_FILE_NOT_FOUND			: return FTT_ENOENT;
		case ERROR_PATH_NOT_FOUND			: return FTT_ENOENT;
		case ERROR_TOO_MANY_OPEN_FILES		: return FTT_ENFILE;
		case ERROR_ACCESS_DENIED			: return FTT_EPERM;
		case ERROR_INVALID_HANDLE			: return FTT_ENOENT;
		case ERROR_NOT_ENOUGH_MEMORY		: return FTT_ENOMEM;
		case ERROR_INVALID_BLOCK			: return FTT_ENOTSUPPORTED;
		case ERROR_BAD_ENVIRONMENT			: return FTT_EFAULT;
		case ERROR_BAD_FORMAT				: return FTT_EFAULT;
		case ERROR_INVALID_ACCESS			: return FTT_ENOENT;
		case ERROR_INVALID_DATA				: return FTT_ENOENT;
		case ERROR_OUTOFMEMORY				: return FTT_ENOMEM;
		case ERROR_INVALID_DRIVE			: return FTT_ENOENT;
		case ERROR_CURRENT_DIRECTORY		: return FTT_EPERM;
		case ERROR_NOT_SAME_DEVICE			: return FTT_ENOTSUPPORTED;
		case ERROR_NO_MORE_FILES			: return FTT_ENOENT;
		case ERROR_BAD_UNIT					: return FTT_ENOTSUPPORTED;
		case ERROR_NOT_READY				: return FTT_EIO; /* ?????????*/
		case ERROR_BAD_COMMAND				: return FTT_ENOENT;
		case ERROR_BAD_LENGTH				: return FTT_ENOENT;
		case ERROR_WRITE_FAULT				: return FTT_EIO;
		case ERROR_READ_FAULT				: return FTT_EIO;
		case ERROR_GEN_FAILURE				: return FTT_EFAULT;
		case ERROR_HANDLE_EOF				: return FTT_EIO;
		case ERROR_OPEN_FAILED				: return FTT_EFAULT;
		case ERROR_BUSY_DRIVE				: return FTT_EBUSY;
		case ERROR_BUSY						: return FTT_EBUSY;
		case ERROR_IO_PENDING				: return FTT_EBUSY;

		case ERROR_LOCK_VIOLATION			: return FTT_ENOTSUPPORTED;
		case ERROR_BAD_NETPATH				: return FTT_ENOTSUPPORTED;
		case ERROR_FILE_EXISTS				: return FTT_ENOTSUPPORTED;
		case ERROR_CANNOT_MAKE				: return FTT_ENOTSUPPORTED;
		case ERROR_FAIL_I24					: return FTT_ENOTSUPPORTED;
		case ERROR_INVALID_PARAMETER		: return FTT_EBLKSIZE; /* this is what you get */
		
		case ERROR_DRIVE_LOCKED				: return FTT_EPERM;
		case ERROR_BROKEN_PIPE				: return FTT_ENOTSUPPORTED;
		case ERROR_IO_DEVICE				: return FTT_EIO;

		case ERROR_EOM_OVERFLOW				: return FTT_EBLANK;
		case ERROR_POSSIBLE_DEADLOCK		: return FTT_EBUSY;

		case ERROR_SHARING_VIOLATION        : return FTT_EBUSY; /* the device is opened */
		default								: return FTT_ENOTSUPPORTED;
  }
}

 int
	ftt_translate_error_WIN(ftt_descriptor d, int opn, char *op, DWORD fres, char *what, int recoverable) {
    
    static ftt_stat sbuf;
    char *p;
    int save1, save2 ;

    DEBUG3(stderr,"Entering ftt_translate_error_WIN -- opn == %d, op = %s, res=%d, what=%s recoverable=%d\n",
	opn,op, fres, what, recoverable);

    if( 0 == d ) {
		ftt_eprintf("%s called with NULL ftt_descriptor\n", op);
		ftt_errno = FTT_EFAULT;
		return -1;
    }
	/* - instead of table - is easier this way - */

    ftt_errno = ftt_WIN_error(fres);


#   define CHECKS (FTT_OP_SKIPFM|FTT_OP_RSKIPFM|FTT_OP_SKIPREC|FTT_OP_RSKIPREC\
			|FTT_OP_READ)

    if ((NO_ERROR == fres && FTT_OPN_READ == opn && 0 !=(d->flags&FTT_FLAG_VERIFY_EOFS))
    		      || (NO_ERROR != fres && ((1<<opn)&CHECKS) )) {
		/* 
		** save errno and ftt_errno so we can restore them 
		** after getting status 
		*/
		save1 = ftt_errno;
		save2 = errno;
		
		ftt_get_stats(d, &sbuf);
		errno = save2;
		
		if (0 != (p = ftt_extract_stats(&sbuf,FTT_SENSE_KEY))) {
			if (8 == atoi(p)){
				fres = (DWORD) -1;
				save1 = ftt_errno = FTT_EBLANK;
			} else {
				ftt_errno = save1;
			}
		} else {
			ftt_errno = save1;
		}
		if (0 != (p = ftt_extract_stats(&sbuf,FTT_BLOC_LOC))) {
			DEBUG3(stderr, "Current loc %s, last loc %d\n", p, d->last_pos);
			if (d->last_pos > 0 && atoi(p) == d->last_pos) {
				ftt_errno = FTT_EBLANK;
				fres = (DWORD)-1;
			} else {
				ftt_errno = save1;
			}
			d->last_pos = atoi(p);
		} else if (0 != (p = ftt_extract_stats(&sbuf,FTT_REMAIN_TAPE))) {
			DEBUG3(stderr, "Current remain %s, last remain %d\n", p, d->last_pos);
			if (d->last_pos > 0 && atoi(p) == d->last_pos) {
				ftt_errno = FTT_EBLANK;
				fres = (DWORD)-1;
			} else {
				ftt_errno = save1;
			}
			d->last_pos = atoi(p);
		} else {
			ftt_errno = save1;
		}
    }
	
    return ftt_describe_error_WIN(d, opn, op, fres, what, recoverable);
}

int
ftt_describe_error_WIN(ftt_descriptor d, int opn, char *op, DWORD res, char *what, int recoverable) 
{
	if (NO_ERROR == res) {
		ftt_errno = FTT_SUCCESS;
		return 0;
    }
    ftt_eprintf( "\
%s: doing %s on %s returned %d,\n\
	errno %d, => ftt error %s(%d), meaning \n\
	%s\n%s",

	what, op,  (d->which_is_open >= 0 ? 
				d->devinfo[d->which_is_open].device_name :
				d->basename),
	res, errno, ftt_ascii_error[ftt_errno], ftt_errno,
	messages[ftt_errno], recoverable ? "": messages[FTT_EUNRECOVERED] );

    DEBUG2(stderr, "ftt_translate_error_WIN -- message is:\n%s", ftt_eprint_buf);

    if (!recoverable) {
		d->unrecovered_error = opn < FTT_OPN_WRITEFM ? 1 : 2;
		d->current_valid = 0;
    }

    return -1;
}
#endif                    /* WIN-NT trunslate error functions */

#ifdef TESTTABLES
main(){
	int i;
	for( i = 0; ftt_ascii_error[i] != 0 ; i++ ) {
		printf("%d -> %s -> %s\n", i, ftt_ascii_error[i], messages[i]);
	}
}
#endif
