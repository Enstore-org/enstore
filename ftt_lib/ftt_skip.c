#include <stdio.h>
#include <unistd.h>
#include <ftt_private.h>
#include <ftt_mtio.h>
#include <string.h>

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
	ftt_set_transfer_length(cdb,n);
	res = ftt_do_scsi_command(d,what,cdb, 6, 0, 0, 120, 0);
    } else {
	DEBUG2(stderr,"System Call\n");

	if ( 0 > ftt_open_dev(d)) {
	    res=d->file_descriptor;
	    DEBUG3(stderr,"open returned %d\n", res);
	} else {
	    ioctlbuf.tape_op = mtop;
	    ioctlbuf.tape_count = n;
	    res = ioctl(d->file_descriptor, FTT_TAPE_OP, &ioctlbuf);
	    DEBUG3(stderr,"ioctl returned %d\n", res);
	    if ( res < 0 ) {
		res = ftt_translate_error(d, opn, "an mtio ioctl() call", res, what,0);
	    }
	}
    }
    d->last_operation = (1 << opn);
    return res;
}

unsigned char ftt_cdb_skipfm[]	= {0x11, 0x01, 0x00, 0x00, 0x00, 0x00};
unsigned char ftt_cdb_skipbl[]	= {0x11, 0x00, 0x00, 0x00, 0x00, 0x00};
unsigned char ftt_cdb_rewind[]	= {0x01, 0x00, 0x00, 0x00, 0x00, 0x00};
unsigned char ftt_cdb_unload[]	= {0x1b, 0x00, 0x00, 0x00, 0x00, 0x00};
unsigned char ftt_cdb_retension[]= {0x1b, 0x00, 0x00, 0x00, 0x03, 0x00};
unsigned char ftt_cdb_erase[]	= {0x19, 0x00, 0x00, 0x00, 0x00, 0x00};
unsigned char ftt_cdb_writefm[]	= {0x10, 0x00, 0x00, 0x00, 0x00, 0x00};
/*
** The remaining calls just invoke mtop with the right options
*/
int
ftt_skip_fm(ftt_descriptor d, int n) {

    CKOK(d,"ftt_skip_fm",0,0)
    CKNULL("ftt_descriptor", d);

    if ( n < 0 ) {
	ftt_write_fm_if_needed(d);
    }
    return ftt_skip_fm_internal(d,n);
}

int
ftt_skip_fm_internal(ftt_descriptor d, int n) {

    d->current_file += n;
    d->current_block = 0;
    return ftt_mtop(d, n,  FTT_TAPE_FSF, (n > 0 ? FTT_OPN_SKIPFM:FTT_OPN_RSKIPFM), 
			"an ftt_skip_fm", ftt_cdb_skipfm);
}

int
ftt_skip_rec(ftt_descriptor d, int n){

    CKOK(d,"ftt_skip_rec",0,0);
    CKNULL("ftt_descriptor", d);

    if ( n < 0 ) {
	ftt_write_fm_if_needed(d);
    }
    d->current_block += n;
    return ftt_mtop(d, n, FTT_TAPE_FSR, (n > 0 ? FTT_OPN_SKIPREC:FTT_OPN_RSKIPREC),
			"an ftt_skip_rec", ftt_cdb_skipbl);
}

int
ftt_rewind(ftt_descriptor d){
    int res;

    CKOK(d,"ftt_rewind",0,1);
    CKNULL("ftt_descriptor", d);

    ftt_write_fm_if_needed(d);
    d->data_direction = FTT_DIR_READING;
    d->current_block = 0;
    d->current_file = 0;
    d->current_valid = 1;
    /*
    ** we rewind twice in case the silly OS has the 
    ** asynchronous rewind bit turned on, in which case 
    ** the second one waits for the first one to complete.
    ** Also, rewinding twice doesn't hurt...
    */
    res = ftt_mtop(d, 0, FTT_TAPE_REW, FTT_OPN_REWIND,
		"an ftt_rewind", ftt_cdb_rewind);
    res = ftt_mtop(d, 0, FTT_TAPE_REW, FTT_OPN_REWIND,
	"an ftt_rewind", ftt_cdb_rewind);
    return res;
}

int
ftt_retension(ftt_descriptor d) {

    CKOK(d,"ftt_retension",0,1);
    CKNULL("ftt_descriptor", d);

    ftt_write_fm_if_needed(d);
    d->data_direction = FTT_DIR_READING;
    d->current_block = 0;
    d->current_file = 0;
    d->current_valid = 1;
    return ftt_mtop(d, 0, FTT_TAPE_RETEN, FTT_OPN_RETENSION,
		"an ftt_retension", ftt_cdb_retension);
}

int
ftt_unload(ftt_descriptor d){

    CKOK(d,"ftt_unload",0,1);
    CKNULL("ftt_descriptor", d);

    d->data_direction = FTT_DIR_READING;
    ftt_write_fm_if_needed(d);
    d->current_block = 0;
    d->current_file = 0;
    d->current_valid = 1;
    return ftt_mtop(d, 0, FTT_TAPE_UNLOAD, FTT_OPN_UNLOAD,
			"an ftt_unload", ftt_cdb_unload);
}

int
ftt_erase(ftt_descriptor d) {

    CKOK(d,"ftt_erase",0,1);
    CKNULL("ftt_descriptor", d);

    d->current_block = 0;
    d->current_file = 0;
    d->current_valid = 1;
    return ftt_mtop(d, 0, FTT_TAPE_ERASE, FTT_OPN_ERASE,
		"an ftt_erase", ftt_cdb_erase);
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
	if ((d->current_file != 0 || d->current_block > 2) &&
		(ftt_status(d,0) & FTT_ABOT)) {
	    ftt_errno = FTT_EUNRECOVERED;
	    ftt_eprintf("\tBefore writing a filemark, when we were supposed\n\
	to be at file number %d block number %d, we found ourselves at BOT\n\
	indicating that there was a SCSI reset or other error which rewound\n\
	the tape behind our back.");
	    d->unrecovered_error = 1;
	    return -1;
	}
    }
    d->data_direction = FTT_DIR_WRITING;
    d->current_block = 0;
    d->current_file++;
    return ftt_mtop(d, 1, FTT_TAPE_WEOF, FTT_OPN_WRITEFM,
		"an ftt_writefm", ftt_cdb_writefm);
}

int
ftt_skip_to_double_fm(ftt_descriptor d) {
    static char buf[65536];
    int res;

    CKOK(d,"ftt_skip_to_double_fm",0,0);
    CKNULL("ftt_descriptor", d);

    ftt_open_dev(d);
    do {
	res = ftt_skip_fm(d,1);			if(res < 0) return res;
	res = ftt_read(d, buf, 65536);		if(res < 0) return res;
    } while ( res > 0 );
    return ftt_skip_fm(d,-1);
}

int
ftt_write_fm_if_needed(ftt_descriptor d) {
    int n = 0, res;
    int savefile, saveblock;

    CKOK(d,"ftt_write_fm_if_needed",0,0);
    CKNULL("ftt_descriptor", d);

    if (FTT_OP_WRITE == d ->last_operation ||
	    FTT_OP_WRITEFM == d ->last_operation ) {

	savefile = d->current_file;
	saveblock = d->current_block;
	DEBUG3(stderr,"Writing first filemark...\n");
	res = ftt_writefm(d);
	if (res >= 0) {
		n--;
	}
	DEBUG3(stderr,"Writing second filemark...\n");
	res = ftt_writefm(d);
	if (res >= 0) {
		n--;
	}
	if ( n < 0 ) {
	    DEBUG3(stderr,"skipping %d filemarks...\n", n);
	    ftt_skip_fm_internal(d, n);
	}
	d->last_operation = FTT_OP_SKIPFM;
	d->current_file = savefile;
	d->current_block = saveblock;
    }
    return 0;
}
