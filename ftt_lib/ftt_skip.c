static char rcsid[] = "@(#)$Id$";

#include <stdlib.h>
#include <stdio.h>
#include <ftt_private.h>
#include <string.h>
#include <ftt_mtio.h> 

#ifdef WIN32
#include <malloc.h>
#include <io.h>
#include <process.h>
#include <windows.h>
#include <winioctl.h>

int ftt_translate_error_WIN();

#define geteuid() -1

#else
#include <unistd.h>
#endif

extern int errno;

int ftt_describe_error();

/*
** ioctlbuf is the tapeop strcture
** (struct mtio, struct tapeio, etc.)
** that we'll use to do all the I/O
*/
static struct tapeop ioctlbuf;

/*
** ftt_mtop has all of the common code for rewind, retenstion, etc.
** factored into it.  It starts async operations, and cleans up
** after them (using a single level of recursion).
**
** we then decide if the operation is a pass-thru, and do it that
** way if needed,
** otherwise we make sure the device is open,
** and if it has been successfully opened, we fill in an mtio
** block and perform the mtio call, performing the appropriate
** error translation if it fails.
*/
static int 
ftt_mtop(ftt_descriptor d, int n, int mtop, int opn, char *what, unsigned char *cdb) {
    int res;

    ENTERING("ftt_mtop");
    CKNULL("ftt_descriptor", d);
    CKNULL("operation name", what);
    CKNULL("operation SCSI CDB", cdb);
    DEBUG1(stderr,"ftt_mtop operation %d n %d to do %s\n", opn, n, what);


    if ( 0 != (d->scsi_ops & (1 << opn))) {
		DEBUG2(stderr, "SCSI pass-thru\n");
		if (opn == FTT_OPN_RSKIPREC || opn == FTT_OPN_RSKIPFM) {
			ftt_set_transfer_length(cdb,-n);
		} else {
			ftt_set_transfer_length(cdb,n);
		}
		res = ftt_do_scsi_command(d,what,cdb, 6, 0, 0, 120, 0);
		res = ftt_describe_error(d,opn,"a SCSI pass-through call", res, what, 0);
    
	} else {
	
		DEBUG2(stderr,"System Call\n");

		if ( 0 > (res = ftt_open_dev(d))) {
			DEBUG3(stderr,"open returned %d\n", res);
			return res;
		} else {

#ifndef WIN32

			ioctlbuf.tape_op = mtop;
			ioctlbuf.tape_count = n;
			res = ioctl(d->file_descriptor, FTT_TAPE_OP, &ioctlbuf);
			DEBUG3(stderr,"ioctl returned %d\n", res);
			res = ftt_translate_error(d, opn, "an mtio ioctl() call", res, what,0);
			/*
			** we do an lseek to reset the file offset counter
			** so the OS doesn't get hideously confused if it 
			** overflows...  We may need this to be a behavior
			** flag in the ftt_descriptor and device tables.
			*/
			(void) lseek(d->file_descriptor, 0L, 0);
#else
		{
			DWORD LowOff,fres;
			HANDLE fh = (HANDLE)d->file_descriptor;
			int Count=0;

			if (opn == FTT_OPN_RSKIPFM || opn == FTT_OPN_RSKIPREC ) { LowOff = (DWORD) -n;
			} else LowOff = (DWORD)n;

			if ( opn == FTT_OPN_RSKIPFM || opn == FTT_OPN_SKIPFM ) {
				fres = SetTapePosition(fh,TAPE_SPACE_FILEMARKS,0,LowOff,0,0);
			
			} else if ( opn == FTT_OPN_RSKIPREC || opn == FTT_OPN_SKIPREC ) {
				fres = SetTapePosition(fh,TAPE_SPACE_RELATIVE_BLOCKS,0,LowOff,0,0);
			} else if ( opn == FTT_OPN_WRITEFM ) {
				fres = WriteTapemark(fh,TAPE_LONG_FILEMARKS,n,0); /* can be LONG or SHORT */
			} else if ( opn == FTT_OPN_RETENSION ) {
				/* go to the end of tape */
				do {
					fres = SetTapePosition(fh,TAPE_SPACE_FILEMARKS,0,99999,0,0); 
				} while ( fres == NO_ERROR);
				fres = SetTapePosition(fh,TAPE_SPACE_END_OF_DATA,0,0,0,0);
				/* now rewind */
				do {
					fres = SetTapePosition(fh,TAPE_SPACE_FILEMARKS,0,(DWORD)-99999,0,0); 
				} while ( fres == NO_ERROR);
				fres = SetTapePosition(fh,TAPE_REWIND,0,0,0,0); 
				
			} else if ( opn == FTT_OPN_UNLOAD  ) {
				fres = PrepareTape(fh,TAPE_UNLOAD,0);
			} else if ( opn == FTT_OPN_REWIND  ) {
				/* this is the trick to avoid Bus reset */
				do {
					fres = SetTapePosition(fh,TAPE_SPACE_FILEMARKS,0,(DWORD)-99999,0,0); 
				} while ( fres == NO_ERROR);
				fres = SetTapePosition(fh,TAPE_REWIND,0,0,0,0); 
			} else if ( opn == FTT_OPN_ERASE   ) {
				fres = EraseTape(fh,TAPE_ERASE_LONG,0); /* can be SHORT */
			} else {
				fres = (DWORD)-1;
			}
			res = ftt_translate_error_WIN(d, opn, "win - tape functions ", fres, what,0);
		}	
#endif	
	  }
	}
    if (res < 0) {
                DEBUG0(stderr,"HARD error doing ftt_mtop operation %d n %d to do %s - error \n", opn, n, what,res);
		d->nharderrors++;
    }
    d->last_operation = (1 << opn);
    return res;
}

unsigned char ftt_cdb_skipfm[]	= {0x11, 0x01, 0x00, 0x00, 0x00, 0x00};
unsigned char ftt_cdb_skipbl[]	= {0x11, 0x00, 0x00, 0x00, 0x00, 0x00};
unsigned char ftt_cdb_rewind[]	= {0x01, 0x00, 0x00, 0x00, 0x00, 0x00};
unsigned char ftt_cdb_unload[]	= {0x1b, 0x00, 0x00, 0x00, 0x00, 0x00};
unsigned char ftt_cdb_retension[]= {0x1b, 0x00, 0x00, 0x00, 0x03, 0x00};
unsigned char ftt_cdb_erase[]	= {0x19, 0x01, 0x00, 0x00, 0x00, 0x00};
unsigned char ftt_cdb_writefm[]	= {0x10, 0x00, 0x00, 0x00, 0x00, 0x00};

/*
** The remaining calls just invoke mtop with the right options
*/
int
ftt_skip_fm(ftt_descriptor d, int n) {
    int res, res2;

    CKOK(d,"ftt_skip_fm",0,1);
    CKNULL("ftt_descriptor", d);

    if ( n < 0 ) {
        d->last_pos = -1;/* we skipped backwards, so this can't be valid */
		res = ftt_write_fm_if_needed(d); 	if (res < 0) {return res;}
    }

    res = ftt_skip_fm_internal(d,n); 
	if (res   < 0 ) {
		if ( ftt_errno == FTT_ELEADER )
			ftt_eprintf("ftt_skip_fm: At BOT after doing a skip filemark");
		else if (ftt_errno == FTT_EBLANK ) 
			ftt_eprintf("ftt_skip_fm: At EOT after doing a skip filemark");
		return res;
	}
	
    res2 = ftt_status(d,0);
    DEBUG3(stderr, "ftt_status returns %d after skip\n", res2);

    if ((res   < 0 && ftt_errno == FTT_ELEADER ) ||
		( res2 > 0 && (res2 & FTT_ABOT))) {
		d->unrecovered_error = 2;
		ftt_errno = FTT_ELEADER;
		ftt_eprintf("ftt_skip_fm: At BOT after doing a skip filemark");
		res =  -1;
    }
    if ((res   < 0 && ftt_errno == FTT_EBLANK ) ||
		( res2 > 0 && (res2 & FTT_AEOT) )) {
		d->unrecovered_error = 2;
		ftt_errno = FTT_EBLANK;
		ftt_eprintf("ftt_skip_fm: At EOT after doing a skip filemark");
		res = -1;
	}
	
    return res;
}

int
ftt_skip_fm_internal(ftt_descriptor d, int n) {

    d->current_file += n;
    d->current_block = 0;

    if (n < 0) {
	return ftt_mtop(d, -n,  FTT_TAPE_RSF,  FTT_OPN_RSKIPFM, "ftt_skip_fm", ftt_cdb_skipfm);
   } else {
	return ftt_mtop(d, n,  FTT_TAPE_FSF,  FTT_OPN_SKIPFM, "ftt_skip_fm", ftt_cdb_skipfm);
   }

}

int
ftt_skip_rec(ftt_descriptor d, int n){
    int res;

    CKOK(d,"ftt_skip_rec",0,0);
    CKNULL("ftt_descriptor", d);

    d->current_block += n;
    if ( n < 0 ) {
        d->last_pos = -1;/* we skipped backwards, so this can't be valid */
	    res = ftt_write_fm_if_needed(d);
	    if (res < 0){return res;}
        return ftt_mtop(d, -n, FTT_TAPE_RSR, FTT_OPN_RSKIPREC, "ftt_skip_rec", 
			ftt_cdb_skipbl);
    } else {
        return ftt_mtop(d, n, FTT_TAPE_FSR, FTT_OPN_SKIPREC, "ftt_skip_rec", 
			ftt_cdb_skipbl);
    }
}

int
ftt_rewind(ftt_descriptor d){
    int res, res2;

    CKOK(d,"ftt_rewind",0,2);
    CKNULL("ftt_descriptor", d);

    res = ftt_write_fm_if_needed(d);
    d->data_direction = FTT_DIR_READING;
    d->current_block = 0;
    d->current_file = 0;
    d->current_valid = 1;
    d->last_pos = -1;	/* we skipped backwards, so this can't be valid */
    /*
    ** we rewind twice in case the silly OS has the 
    ** asynchronous rewind bit turned on, in which case 
    ** the second one waits for the first one to complete.
    ** Also, rewinding twice doesn't hurt...
    */
    (void) ftt_mtop(d, 0, FTT_TAPE_REW, FTT_OPN_REWIND,
		"ftt_rewind", ftt_cdb_rewind);
    res2 = ftt_mtop(d, 0, FTT_TAPE_REW, FTT_OPN_REWIND,
	"ftt_rewind", ftt_cdb_rewind);

    /* we cleared unrecoverable errors if we succesfully rewound */
    /* and we're hosed if we didn't */
    d->unrecovered_error = (res2 < 0) ? 2 : 0;
    return res < 0 ? res : res2;
}

int
ftt_retension(ftt_descriptor d) {
    int res, res2;

    CKOK(d,"ftt_retension",0,2);
    CKNULL("ftt_descriptor", d);

    res = ftt_write_fm_if_needed(d);
    d->data_direction = FTT_DIR_READING;
    d->current_block = 0;
    d->current_file = 0;
    d->current_valid = 1;
    res2 = ftt_mtop(d, 0, FTT_TAPE_RETEN, FTT_OPN_RETENSION,
		"ftt_retension", ftt_cdb_retension);

    /* we cleared unrecoverable errors if we succesfully retensioned */
    /* and we're hosed if we didn't */
    d->unrecovered_error = (res2 < 0) ? 2 : 0;
    return res < 0 ? res : res2;
}

int
ftt_unload(ftt_descriptor d){
    int res, res2;

    CKOK(d,"ftt_unload",0,2);
    CKNULL("ftt_descriptor", d);

    d->data_direction = FTT_DIR_READING;
    res = ftt_write_fm_if_needed(d);
    d->current_block = 0;
    d->current_file = 0;
    d->current_valid = 1;
    res2 =  ftt_mtop(d, 0, FTT_TAPE_UNLOAD, FTT_OPN_UNLOAD,
			"ftt_unload", ftt_cdb_unload);

    /* we cleared unrecoverable errors if we succesfully unloaded  */
    /* and we're hosed if we didn't */
    d->unrecovered_error = (res2 < 0) ? 2 : 0;
    return res < 0 ? res : res2;
}

int
ftt_erase(ftt_descriptor d) {
    int res;


    CKOK(d,"ftt_erase",0,2);
    CKNULL("ftt_descriptor", d);

    /* currently erase hoses up on most platforms on most drives,
       due to timeout problems, etc.  So for the first release
       we're punting... */
    ftt_eprintf("Sorry, erase is not functioning properly in this release.");
    return FTT_ENOTSUPPORTED;

    d->current_block = 0;
    d->current_file = 0;
    d->current_valid = 1;

    if ((d->scsi_ops & FTT_OP_ERASE) && (d->flags & FTT_FLAG_SUID_SCSI) 
							&& 0 != geteuid()) {
        ftt_close_dev(d);
        switch(ftt_fork(d)){
        case -1:
                return -1;

        case 0:  /* child */
                fflush(stdout); /* make async_pf stdout */
                fflush(d->async_pf_parent);
                close(1);
                dup2(fileno(d->async_pf_parent),1);
                execlp("ftt_suid", "ftt_suid", "-e", d->basename, 0);

        default: /* parent */
                res = ftt_wait(d);
        }
    } else {
        res =  ftt_mtop(d, 0, FTT_TAPE_ERASE, FTT_OPN_ERASE,
		"ftt_erase", ftt_cdb_erase);
    }

    /* we cleared unrecoverable errors if we succesfully erased  */
    /* and we're hosed if we didn't */
    d->unrecovered_error = (res < 0) ? 2 : 0;
    return res;
}

int
ftt_writefm(ftt_descriptor d) {

    CKOK(d,"ftt_writefm",1,0);
    CKNULL("ftt_descriptor", d);

    if (d->flags & FTT_FLAG_CHK_BOT_AT_FMK) {

	/* 
	** call ftt_status to see if we're at BOT 
	** we should only do this check on machines that don't 
	** need to close the device to get the status.
	** Note that we need to check current_file and current_block
	** *first* because ftt_status will reset them if it notices
	** we're at BOT.
	*/
        (void)ftt_mtop(d, 0, FTT_TAPE_WEOF, FTT_OPN_WRITEFM,
		"write filemark 0 == flush", ftt_cdb_writefm);

	if ((d->current_file != 0 || d->current_block > 2) &&
		(ftt_status(d,0) & FTT_ABOT)) {
	    ftt_errno = FTT_EUNRECOVERED;
	    ftt_eprintf(
"ftt_writefm: supposed to be at file number %d block number %d, actually at BOT\n\
	indicating that there was a SCSI reset or other error which rewound\n\
	the tape behind our back.", d->current_file, d->current_block );
	    d->unrecovered_error = 2;
	    d->nresets++;
	    return -1;
	}
    }
    d->data_direction = FTT_DIR_WRITING;
    d->current_block = 0;
    d->current_file++;
    return ftt_mtop(d, 1, FTT_TAPE_WEOF, FTT_OPN_WRITEFM,
		"ftt_writefm", ftt_cdb_writefm);
}

int
ftt_skip_to_double_fm(ftt_descriptor d) {
    char *buf;
    int blocksize;
    int res;

    CKOK(d,"ftt_skip_to_double_fm",0,0);
    CKNULL("ftt_descriptor", d);

    blocksize = ftt_get_max_blocksize(d);
    buf = (char *)malloc(blocksize);
    if (buf == 0) {
	ftt_errno = FTT_ENOMEM;
	ftt_eprintf("ftt_skip_to_double_fm: unable to allocate %d byte read buffer, errno %d", blocksize, errno);
	return -1;
    }
	
    ftt_open_dev(d);
    do {
	res = ftt_skip_fm(d,1);		   if(res < 0) {free(buf);return res;}
	res = ftt_read(d, buf, blocksize); if(res < 0) {free(buf);return res;}
    } while ( res > 0 );
    /* res == 0 so we got an end of file after a skip filemark... */
    free(buf);
    return ftt_skip_fm(d,-1);
}

int
ftt_write_fm_if_needed(ftt_descriptor d) {
    int res=0;
    int savefile, saveblock, savedir;

    CKOK(d,"ftt_write_fm_if_needed",0,0);
    CKNULL("ftt_descriptor", d);

    if (FTT_OP_WRITE == d ->last_operation ||
	    FTT_OP_WRITEFM == d ->last_operation ) {

	savefile = d->current_file;
	saveblock = d->current_block;
	savedir = d->data_direction;
	DEBUG3(stderr,"Writing first filemark...\n");
	res = ftt_writefm(d); 			if (res < 0) { return res; } 
	DEBUG3(stderr,"Writing second filemark...\n");
	res = ftt_writefm(d); 			if (res < 0) { return res; }
        DEBUG3(stderr,"skipping -2 filemarks...\n");
        res = ftt_skip_fm_internal(d, -2);	if (res < 0) { return res; }
	d->last_operation = FTT_OP_SKIPFM;
	d->current_file = savefile;
	d->current_block = saveblock;
	d->data_direction = savedir;
    }
    return 0;
}

int
ftt_write2fm(ftt_descriptor d) {

    int res;
    CKOK(d,"ftt_write2fm",1,0);
    CKNULL("ftt_descriptor", d);

    if (d->flags & FTT_FLAG_CHK_BOT_AT_FMK) {

	/* 
	** call ftt_status to see if we're at BOT 
	** we should only do this check on machines that don't 
	** need to close the device to get the status.
	** Note that we need to check current_file and current_block
	** *first* because ftt_status will reset them if it notices
	** we're at BOT.
	*/
        (void)ftt_mtop(d, 0, FTT_TAPE_WEOF, FTT_OPN_WRITEFM,
		"write filemark 0 == flush", ftt_cdb_writefm);

	if ((d->current_file != 0 || d->current_block > 2) &&
		(ftt_status(d,0) & FTT_ABOT)) {
	    ftt_errno = FTT_EUNRECOVERED;
	    ftt_eprintf(
"ftt_write2fm: supposed to be at file number %d block number %d, actually at BOT\n\
	indicating that there was a SCSI reset or other error which rewound\n\
	the tape behind our back.", d->current_file, d->current_block );
	    d->unrecovered_error = 2;
	    d->nresets++;
	    return -1;
	}
    }
    d->data_direction = FTT_DIR_WRITING;
    d->current_block = 0;
    d->current_file += 2;
    res = ftt_mtop(d, 2, FTT_TAPE_WEOF, FTT_OPN_WRITEFM,
		"ftt_write2fm", ftt_cdb_writefm);
    /* we've done a double filemark, so we can forget we were writing */
    /* (see check in ftt_write_fm_if_needed, above) */
    d->last_operation = 0;
    return res;
}
