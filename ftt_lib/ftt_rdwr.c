#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <ftt_private.h>
#include <ftt_cdb.h>

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
       ftt_errno = FTT_EPARTIALSTAT;
       ftt_eprintf(
"error: the ftt library is unable to determine the current tape position,\n\
	until you do an ftt_rewind, ftt_status, or ftt_get_stats call.\n");
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
    
    if ( 0 != (d->scsi_ops & FTT_OP_READ)){
	DEBUG2(stderr, "SCSI pass-thru\n");
	d->last_operation = FTT_OP_READ;
	if (d->default_blocksize == 0) {
		ftt_set_transfer_length(ftt_cdb_read,length);
	} else {
		ftt_set_transfer_length(ftt_cdb_read,length/d->default_blocksize);
	}
	res = ftt_do_scsi_command(d,"read",ftt_cdb_read, 6, 
				(unsigned char*)buf, length, 5, 0);
    } else {
	DEBUG2(stderr,"System Call\n");
	if (0 != (d->last_operation &(FTT_OP_WRITE|FTT_OP_WRITEFM)) &&
		0 != (d->flags& FTT_FLAG_REOPEN_R_W)) {
	    ftt_close_dev(d);
	}
	if ( 0 > ftt_open_dev(d)) {
	    return d->file_descriptor;
	}
	res = read(d->file_descriptor, buf, length);
	d->last_operation = FTT_OP_READ;
	res = ftt_translate_error(d, FTT_OPN_READ, "an ftt_read", res, "a read system call",1);
    }
    if (0 == res){ /* end of file */
	if( d->flags & FTT_FLAG_FSF_AT_EOF){
	    ftt_skip_fm(d,1);
	}
	d->current_block = 0;
	d->current_file++;
    } else if (res > 0){
	d->readlo += res;
	d->readkb += d->readlo >> 10;
	d->readlo &= (1<<10) - 1;
	d->current_block++;
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
				(unsigned char *)buf, length, 5,1);
    } else {
	DEBUG2(stderr,"System Call\n");
	if (0 != (d->last_operation &(FTT_OP_READ)) &&
		0 != (d->flags& FTT_FLAG_REOPEN_R_W)) {
	    ftt_close_dev(d);
	}
	if ( 0 > ftt_open_dev(d)) {
	    return d->file_descriptor;
	}
	res = write(d->file_descriptor, buf, length);
	d->last_operation = FTT_OP_WRITE;
	res = ftt_translate_error(d, FTT_OPN_WRITE, "an ftt_write", res, "a write() system call",1);
    }
    if (res > 0) {
	d->writelo += res;
	d->writekb += d->writelo >> 10;
	d->writelo &= (1<<10) - 1;
	d->current_block++;
    }
    d->nwrites++;
    d->data_direction = FTT_DIR_WRITING;
    return res;
}
