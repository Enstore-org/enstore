static char rcsid[] = "@(#)$Id$";
#include <stdio.h>
#include <stdlib.h>
#include <ftt_private.h>

#ifdef WIN32
#include <io.h>
#include <windows.h>
#include <winioctl.h>

int ftt_translate_error_WIN();

#endif

int	 ftt_describe_error();

int
ftt_get_position(ftt_descriptor d, int *file, int *block) {

    CKOK(d,"ftt_get_position",0,0);
    CKNULL("ftt_descriptor", d);

   if( file != 0 ){
      *file = d->current_file;
   }
   if( block != 0 ){
      *block = d->current_block;
   }
   if (d->current_valid) {
       return 0;
   } else {
       ftt_errno = FTT_ELOST;
       ftt_eprintf(
"ftt_get_position: unable to determine the current tape position,\n\
	until you do an ftt_rewind, or ftt_status or ftt_get_stats call at BOT.\n");
       return -1;
   }
}
unsigned char	ftt_cdb_read[]  = { 0x08, 0x00, 0x00, 0x00, 0x00, 0x00 },
	  	ftt_cdb_write[] = { 0x0a, 0x00, 0x00, 0x00, 0x00, 0x00 };

int
ftt_read( ftt_descriptor d, char *buf, int length ) {
    int res;

    CKOK(d,"ftt_read",0,0);
    CKNULL("ftt_descriptor", d);
    CKNULL("data buffer pointer", buf);
    
    if ( 0 != (d->scsi_ops & FTT_OP_READ)) {
	DEBUG2(stderr, "SCSI pass-thru\n");
	d->last_operation = FTT_OP_READ;
	if (d->default_blocksize == 0) {
		ftt_set_transfer_length(ftt_cdb_read,length);
	} else {
		ftt_set_transfer_length(ftt_cdb_read,length/d->default_blocksize);
	}
	res = ftt_do_scsi_command(d,"read",ftt_cdb_read, 6, 
				(unsigned char*)buf, length, 60, 0);
	res = ftt_describe_error(d, FTT_OPN_READ, "ftt_read", res, "a read SCSI command", 1);
    
	} else {
	
		DEBUG2(stderr,"System Call\n");
		if (0 != (d->last_operation &(FTT_OP_WRITE|FTT_OP_WRITEFM)) &&
			0 != (d->flags& FTT_FLAG_REOPEN_R_W)) {
			ftt_close_dev(d);
		}
		if ( 0 > (res = ftt_open_dev(d))) {
	    		return res;
		}
		d->last_operation = FTT_OP_READ;

#ifndef WIN32

		res = read(d->file_descriptor, buf, length);
		res = ftt_translate_error(d, FTT_OPN_READ, "ftt_read", res, "a read system call",1);
		if (res == FTT_EBLANK) {
			/* we read past end of tape, prevent further confusion on AIX */
			d->unrecovered_error = 1;
		}
#else 
		{ /* ---------------- this is the WIN-NT part -----------------*/
			DWORD	nread,Lerrno;	
			if ( ! ReadFile((HANDLE)d->file_descriptor,(LPVOID)buf,(DWORD)length,&nread,0) ) {
				Lerrno = GetLastError();
				if ( Lerrno == ERROR_FILEMARK_DETECTED ) {
					nread = (DWORD)0;
				} else {
					ftt_translate_error_WIN(d, FTT_OPN_READ, "ftt_read",
						GetLastError(), "a ReadFile call",1);
					nread = (DWORD)-1;
				}
			}
			res = (int)nread;
		}
#endif
	
	
		}
    if (0 == res) { /* end of file */
	if( d->flags & FTT_FLAG_FSF_AT_EOF){
	    ftt_skip_fm(d,1);
	} else if (d->flags & FTT_FLAG_REOPEN_AT_EOF) {
	    ftt_close_dev(d);
	    ftt_open_dev(d);
	} else {
	    /* fix file offset */
	    lseek(d->file_descriptor, 0L, 0);
	}
	d->current_block = 0;
	d->current_file++;
    } else if (res > 0){
	d->readlo += res;
	d->readkb += d->readlo >> 10;
	d->readlo &= (1<<10) - 1;
	d->current_block++;
    } else {
	d->nharderrors++;
    }
    d->nreads++;
    d->data_direction = FTT_DIR_READING;
    return res;
}

int
ftt_write( ftt_descriptor d, char *buf, int length ) {
    int res;

    CKOK(d,"ftt_write",1,0);
    CKNULL("ftt_descriptor", d);
    CKNULL("data buffer pointer", buf);

    if ( 0 != (d->scsi_ops & FTT_OP_WRITE)) {
	DEBUG2(stderr,"SCSI pass-thru\n");
	d->last_operation = FTT_OP_WRITE;
	if (d->default_blocksize == 0) {
		ftt_set_transfer_length(ftt_cdb_write,length);
	} else {
		ftt_set_transfer_length(ftt_cdb_write,length/d->default_blocksize);
	}
	res = ftt_do_scsi_command(d,"write",ftt_cdb_write, 6, 
				(unsigned char *)buf, length, 60,1);
	res = ftt_describe_error(d, FTT_OPN_WRITE, "ftt_write", res, "a write SCSI command", 0);
    } else {
		DEBUG2(stderr,"System Call\n");
		if (0 != (d->last_operation &(FTT_OP_READ)) &&
			0 != (d->flags& FTT_FLAG_REOPEN_R_W)) {
			ftt_close_dev(d);
		}
		if ( 0 > (res = ftt_open_dev(d))) {
			return res;
		}
		d->last_operation = FTT_OP_WRITE;

#ifndef WIN32

		res = write(d->file_descriptor, buf, length);
		res = ftt_translate_error(d, FTT_OPN_WRITE, "ftt_write", res, "a write() system call",0);
#else
		{ /* ---------------- this is the WIN-NT part -----------------*/
			DWORD	nwrt;	

			if ( !  WriteFile((HANDLE)d->file_descriptor,(LPVOID)buf,(DWORD)length,&nwrt,0 )) {
				ftt_translate_error_WIN(d, FTT_OPN_READ, "ftt_write",
					GetLastError(), "a WriteFile call",1);
				nwrt = (DWORD)-1;
			}
			res = (int)nwrt;
		}
#endif
	
	}
    if (res > 0) {
	d->writelo += res;
	d->writekb += d->writelo >> 10;
	d->writelo &= (1<<10) - 1;
	d->current_block++;
    } else {
	d->nharderrors++;
    }
    d->nwrites++;
    d->data_direction = FTT_DIR_WRITING;
    return res;
}
