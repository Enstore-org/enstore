#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <ftt_private.h>
extern int errno;

ftt_stat_buf
ftt_alloc_stat() {
    void *res;

    ENTERING("ftt_alloc_stat");
    res =  malloc(sizeof(ftt_stat));
    if (0 != res) {
	memset(res,0,sizeof(ftt_stat));
	return res;
    } else {
	ftt_eprintf("Unable to allocate statistics buffer errno %d\n", errno);
	ftt_errno = FTT_ENOMEM;
	return res;
    }
}

int
ftt_free_stat(ftt_stat_buf b) {
    int i;

    ENTERING("ftt_free_stat");
    CKNULL("statistics buffer pointer", b);

    for (i = 0; i < FTT_MAX_STAT; i++) {
	if (b->value[i]) {
		free(b->value[i]);
		b->value[i] = 0;
	}
    }
    free(b);
    return 0;
}

static char *
itoa(long n) {
	static char buf[128];

	sprintf(buf,"%ld", n);
	return buf;
}


/* set_stat
**
** handy routine to fill in the n-th slot in the stat buffer
*/
static void
set_stat( ftt_stat_buf b, int n, char *pcStart, char *pcEnd) {
    char save;

    /* clean out old value */
    if (b->value[n] != 0) {
	free(b->value[n]);
	b->value[n] = 0;
    }

    /* if null, leave it */
    if (pcStart == 0) {
	return;
    }

    /* null terminate if pcEnd points somewhere, copy the string, 
    ** and then put the byte back where we scribbled the null
    */
    if (pcEnd != 0) {
	save = *pcEnd;
	*pcEnd = 0;
    }
    b->value[n] = strdup(pcStart);
    if (pcEnd != 0) {
	*pcEnd = save;
    }
}

int ftt_numeric_tab[FTT_MAX_STAT] = {
    /*  FTT_VENDOR_ID		0 */ 0,
    /*  FTT_PRODUCT_ID		1 */ 0,
    /*  FTT_FIRMWARE		2 */ 0,
    /*  FTT_SERIAL_NUM		3 */ 0,
    /*  FTT_HOURS_ON		4 */ 1,
    /*  FTT_CLEANING_BIT	5 */ 0,
    /*  FTT_READ_COUNT		6 */ 1,
    /*  FTT_WRITE_COUNT		7 */ 1,
    /*  FTT_READ_ERRORS		8 */ 1,
    /*  FTT_WRITE_ERRORS	9 */ 1,
    /*  FTT_FTT_DENSITY		10 */ 0,
    /*  FTT_READ_COMP		11 */ 0,
    /*  FTT_FILE_NUMBER		12 */ 0,
    /*  FTT_BLOCK_NUMBER	13 */ 0,
    /*  FTT_BOT			14 */ 0,
    /*  FTT_READY		15 */ 0,
    /*  FTT_WRITE_PROT		16 */ 0,
    /*  FTT_FMK			17 */ 0,
    /*  FTT_EOM			18 */ 0,
    /*  FTT_PEOT		19 */ 0,
    /*  FTT_MEDIA_TYPE		20 */ 0,
    /*  FTT_BLOCK_SIZE		21 */ 0,
    /*  FTT_BLOCK_TOTAL		22 */ 0,
    /*  FTT_TRANS_DENSITY	23 */ 0,
    /*  FTT_TRANS_COMPRESS	24 */ 0,
    /*  FTT_REMAIN_TAPE		25 */ 0,
    /*  FTT_USER_READ		26 */ 1,
    /*  FTT_USER_WRITE		27 */ 1,
    /*  FTT_CONTROLLER		28 */ 0,
    /*  FTT_DENSITY		29 */ 0,
    /*  FTT_ILI			30 */ 0,
    /*  FTT_SCSI_ASC		31 */ 0,
    /*  FTT_SCSI_ASCQ		32 */ 0,
    /*  FTT_PF			33 */ 0,
    /*  FTT_CLEANED_BIT	        34 */ 0,
    /*  FTT_WRITE_COMP		35 */ 0,
    /*  FTT_TRACK_RETRY		36 */ 0,
    /*  FTT_UNDERRUN		37 */ 1,
    /*  FTT_MOTION_HOURS	38 */ 1,
    /*  FTT_POWER_HOURS		39 */ 1,
    /*  FTT_TUR_STATUS		40 */ 0,
    /*  FTT_BLOC_LOC		41 */ 1,
    /*  FTT_COUNT_ORIGIN	42 */ 0,
    /*  FTT_N_READS		43 */ 1,
    /*  FTT_N_WRITES		44 */ 1,
    /*  FTT_TNP			45 */ 0,
    /*  FTT_TRANS_SENSE_KEY	45 */ 0,
};

void
ftt_add_stats(ftt_stat_buf b1, ftt_stat_buf b2, ftt_stat_buf res){
    int i;

    ENTERING("ftt_add_stats");
    VCKNULL("statistics buffer pointer 1", b1);
    VCKNULL("statistics buffer pointer 2", b2);
    VCKNULL("statistics buffer pointer 3", res);

    for( i = 0; i < FTT_MAX_STAT; i++) {
        if( ftt_numeric_tab[i] && b1->value[i] && b2->value[i] ) {
 	    set_stat(res, i, itoa((long)atoi(b1->value[i]) + atoi(b2->value[i])),0);
        } else if ( b2->value[i] ) {
 	    set_stat(res, i, b2->value[i],0);
        } else if ( b1->value[i] ) {
 	    set_stat(res, i, b1->value[i],0);
	}
    }
}

void
ftt_sub_stats(ftt_stat_buf b1, ftt_stat_buf b2, ftt_stat_buf res){
    int i;

    ENTERING("ftt_sub_stats");
    VCKNULL("statistics buffer pointer 1", b1);
    VCKNULL("statistics buffer pointer 2", b2);
    VCKNULL("statistics buffer pointer 3", res);

    for( i = 0; i < FTT_MAX_STAT; i++) {
        if( ftt_numeric_tab[i] && b1->value[i] && b2->value[i] ) {
 	    set_stat(res, i, itoa((long)atoi(b1->value[i]) - atoi(b2->value[i])),0);
        } else if ( b1->value[i] ) {
 	    set_stat(res, i, b1->value[i],0);
        } else if ( b2->value[i] && ftt_numeric_tab[i] ) {
 	    set_stat(res, i, itoa(-(long)atoi(b2->value[i])),0);
        } else if ( b2->value[i] ) {
 	    set_stat(res, i, b2->value[i],0);
	}
    }
}

/*
** handy macros to increase readability:
** pack -- smoosh 4 bytes into an int, msb to lsb
** bit  -- return the nth bit of a byte
*/
#define pack(a,b,c,d) \
     (((long)(a)<<24) + ((long)(b)<<16) + ((long)(c)<<8) + (long)(d))

#define bit(n,byte) (long)(((byte)>>(n))&1)

/*
** unpack_ls does the error and total kb for read and write data
** you pass in the statbuf pointer, the log sense data buffer
** and the page code and statistic number for each.
*/
static void
decrypt_ls(ftt_stat_buf b,unsigned char *buf, int param, int stat, int divide) {
    static char printbuf[128];
    unsigned char *page;
    int thisparam, thislength;
    int i;
    int length;
    double value;

    DEBUG1(stderr,"entering decrypt_ls for parameter %d stat %d\n", param, stat);
    page = buf + 4;
    length = pack(0,0,buf[2],buf[3]);
    while( page < (buf + length) ) {
	thisparam = pack(0,0,page[0],page[1]);
	thislength = page[3];
	value = 0;
	for(i = 0; i < thislength ; i++) {
	    value = value * 256 + page[4+i];
	}
	DEBUG2(stderr, "parameter %d, length %d value %f\n", thisparam, thislength, value);
	if ( thisparam == param ) {
	    sprintf(printbuf, "%.0f", value / divide);
	    set_stat(b,stat,printbuf,0);
	}
	page += 4 + thislength;
    }
}

static int
ftt_get_stat_ops(char *name) {
    int i;
    DEBUG1(stderr, "entering get_stat_ops\n");
    for (i = 0; ftt_stat_op_tab[i].name != 0; i++ ) {
	if (ftt_matches(name, ftt_stat_op_tab[i].name)) {
            DEBUG2(stderr, "found stat_op\n");
	    return ftt_stat_op_tab[i].stat_ops;
	}
    }
    return 0;
}

int
ftt_get_stats(ftt_descriptor d, ftt_stat_buf b) {
    int res;
    int i;
    unsigned char buf[512];
    int tape_size, remain_tape, error_count;
    int n_blocks, block_length;
    int stat_ops;

    CKOK(d,"ftt_get_stats",0,0);
    CKNULL("ftt_descriptor", d);
    CKNULL("statistics buffer pointer", b);

    if ((d->flags & FTT_FLAG_SUID_SCSI) && 0 != geteuid()) {
	switch(ftt_fork(d)){
	case -1:
		return -1;

	case 0:  /* child */
		fflush(stdout);	/* make async_pf stdout */
		close(1);
		dup(fileno(d->async_pf));
		execlp("ftt_suid", "ftt_suid", "-s", d->basename, 0);

	default: /* parent */
		ftt_undump_stats(b,d->async_pf);
		res = ftt_wait(d);
	}
    }

    /* Things we know without asking, and the suid program won't know */
    set_stat(b,FTT_USER_READ,itoa((long)d->readkb), 0);
    set_stat(b,FTT_USER_WRITE,itoa((long)d->writekb), 0);
    set_stat(b,FTT_N_READS,itoa((long)d->nreads), 0);
    set_stat(b,FTT_N_WRITES,itoa((long)d->nwrites), 0);

    if ((d->flags & FTT_FLAG_SUID_SCSI) && 0 != geteuid()) {
	return res;
    }

    set_stat(b,FTT_CONTROLLER,d->controller, 0);

    if (d->current_valid==1) {
        /* we think we know where we are */
        if (d->current_block == 0 && d->current_file == 0) {
	    set_stat(b,FTT_BOT,"1",0);
	} else {
	    set_stat(b,FTT_BOT,"0",0);
	}
    }

    /* various mode checks */
    stat_ops = ftt_get_stat_ops(d->prod_id);

    if (stat_ops & FTT_DO_TUR) {
        static unsigned char cdb_tur[]	     = {0x00, 0x00, 0x00, 0x00, 0x00, 0x00};

	res = ftt_do_scsi_command(d,"Test Unit Ready", cdb_tur, 6, 0, 0, 10, 0);
	set_stat(b,FTT_TUR_STATUS,itoa((long)-res), 0);
	if (res < 0) {
	    set_stat(b,FTT_READY,"0",0);
	} else {
	    set_stat(b,FTT_READY,"1",0);
	}
    }
    if (stat_ops & FTT_DO_INQ) {
	static unsigned char cdb_inquiry[]   = {0x12, 0x00, 0x00, 0x00,   56, 0x00};

	/* basic scsi inquiry */
	res = ftt_do_scsi_command(d,"Inquiry", cdb_inquiry, 6, buf, 56, 10, 0);
	if(res < 0){
	    ftt_errno = FTT_EPARTIALSTAT;
	    return res;
	} else {
	    set_stat(b,FTT_VENDOR_ID,  (char *)buf+8,  (char *)buf+16);
	    set_stat(b,FTT_PRODUCT_ID, (char *)buf+16, (char *)buf+32);
	    set_stat(b,FTT_FIRMWARE,   (char *)buf+32, (char *)buf+36);
	    if ( 0 != strcmp(d->prod_id, ftt_extract_stats(b,FTT_PRODUCT_ID))) {
		char *tmp;

		/* update or product id and stat_ops if we were wrong */

		tmp = d->prod_id;
		d->prod_id = strdup(ftt_extract_stats(b,FTT_PRODUCT_ID));
		free(tmp);
		stat_ops = ftt_get_stat_ops(d->prod_id);
	    }
	}
    }
    if (stat_ops & FTT_DO_SN) {
        static unsigned char cdb_inq_w_sn[]  = {0x12, 0x01, 0x80, 0x00,   14, 0x00};

	/* scsi inquiry w/ serial number */
	res = ftt_do_scsi_command(d,"Inquiry", cdb_inq_w_sn, 6, buf, 14, 10, 0);
	if(res < 0){
	    ftt_errno = FTT_EPARTIALSTAT;
	    return res;
	} else {
	    set_stat(b,FTT_SERIAL_NUM, (char *)buf+4, (char *)buf+14);
	}
    }
    if (stat_ops & FTT_DO_MS) {
	static unsigned char cdb_mode_sense[]= {0x1a, 0x00, 0x00, 0x00,   18, 0x00};

	res = ftt_do_scsi_command(d,"mode sense",cdb_mode_sense, 6, buf, 18, 10, 0);
	if (res == -2) {
	    /* retry on a CHECK CONDITION, it may be okay */
	    res = ftt_do_scsi_command(d,"mode sense",cdb_mode_sense, 6, buf, 18, 10, 0);
	}
	if(res < 0){
	    ftt_errno = FTT_EPARTIALSTAT;
	    return res;
	} else {

	    set_stat(b,FTT_DENSITY,     itoa((long)buf[4]), 0);
	    set_stat(b,FTT_WRITE_PROT,  itoa(bit(7,buf[2])),0);
	    set_stat(b,FTT_MEDIA_TYPE,  itoa((long)buf[1]), 0);

	    n_blocks =     pack(0,buf[5],buf[6],buf[7]);
	    block_length = pack(0,buf[9],buf[10],buf[11]);
	    tape_size =    n_blocks;

	    set_stat(b,FTT_BLOCK_SIZE,  itoa((long)block_length),0);
	    set_stat(b,FTT_BLOCK_TOTAL, itoa((long)n_blocks),    0);

	    for ( i = 0; d->devinfo[i].device_name !=0 ; i++ ) {
		if( buf[4] == d->devinfo[i].hwdens ) {
		    set_stat(b,FTT_TRANS_DENSITY, itoa((long)d->devinfo[i].density),0);
		    set_stat(b,FTT_TRANS_COMPRESS, itoa((long)d->devinfo[i].mode),0);
		    break;
		}
	    }
	}
    }
    if (stat_ops & FTT_DO_RS) {
	static unsigned char cdb_req_sense[] = {0x03, 0x00, 0x00, 0x00,   30, 0x00};

	/* request sense data */
	res = ftt_do_scsi_command(d,"Req Sense", cdb_req_sense, 6, buf, 30, 10, 0);
	if(res < 0){
	    ftt_errno = FTT_EPARTIALSTAT;
	    return res;
	} else {
	    static char *sense_key_trans[] = {
		"NO SENSE", "NOT USED", "NOT READY", "MEDIUM ERROR",
		"HARDWARE ERROR", "ILLEGAL REQUEST", "UNIT ATTENTION",
		"DATA PROTECT", "BLANK CHECK", "EXABYTE", "COPY ABORTED",
		"ABORTED COMMAND", "NOT USED", "VOLUME OVERFLOW",
		"NOT USED", "RESERVED",
	    };
	    set_stat(b,FTT_SENSE_KEY, itoa(buf[2]&0xf), 0);
	    set_stat(b,FTT_TRANS_SENSE_KEY, sense_key_trans[buf[2]&0xf], 0);
	    set_stat(b,FTT_FMK, itoa(bit(7,buf[2])), 0);
	    set_stat(b,FTT_EOM, itoa(bit(6,buf[2])),0);
	    set_stat(b,FTT_ILI, itoa(bit(5,buf[2])),0);
	    set_stat(b,FTT_SCSI_ASC,itoa((long)buf[12]),0);
	    set_stat(b,FTT_SCSI_ASCQ,itoa((long)buf[13]),0);

	    /* ASC/ASCQ data parsing
	    **
	    ** these are the codes from the DLT book, because 
	    ** it appears from the book that we may sometimes
	    ** get them filled in with a sense code of 0 to
	    ** indicate end of tape, etc.
	    **
	    ** it is not clear that this has ever actually happened,
	    ** but we wanted to be complete.
	    */
	    switch( pack(0,0,buf[12],buf[13]) ){
	    case 0x0005: /* peot */
			set_stat(b,FTT_PEOT,"1",0);
			break;
	    case 0x0400: /* volume not mounted */
	    case 0x0401: /* rewinding or loading */
	    case 0x0402: /* load needed */
	    case 0x0403: /* manual intervention needed */
	    case 0x3a00: /* medium not present */
	    case 0x3a80: /* cartridge not present */
			set_stat(b,FTT_READY,"0",0);
			break;
	    case 0x0002: /* EOM encountered */
			set_stat(b,FTT_EOM,"1",0);
			break;
	    case 0x0004:
			set_stat(b,FTT_BOT,"1",0);
			d->current_file = 0;
			d->current_block = 0;
			d->current_valid = 1;
			break;
	    case 0x8002:
			set_stat(b,FTT_CLEANING_BIT,"1",0);
			break;
	    }

	    if (stat_ops & FTT_DO_EXBRS) {
		set_stat(b,FTT_BOT,         itoa(bit(0,buf[19])), 0);
		if(bit(0,buf[19])){
		    d->current_file = 0;
		    d->current_block = 0;
		    d->current_valid = 1;
		}
		set_stat(b,FTT_TNP,	    itoa(bit(1,buf[19])), 0);
		set_stat(b,FTT_PF,          itoa(bit(7,buf[19])), 0);
		set_stat(b,FTT_WRITE_PROT,  itoa(bit(5,buf[20])), 0);
		set_stat(b,FTT_PEOT,        itoa(bit(2,buf[21])), 0);
		set_stat(b,FTT_CLEANING_BIT,itoa(bit(3,buf[21])), 0);
		set_stat(b,FTT_CLEANED_BIT, itoa(bit(4,buf[21])), 0);

		remain_tape=pack(0,buf[23],buf[24],buf[25]);
		set_stat(b,FTT_REMAIN_TAPE,itoa((long)remain_tape),0);

		/* 
		** the following lies still allow reasonable results
		** from doing before/after deltas
		** we'll override them with log sense data if we have it.
		** The following is a fudge factor for the amount of
		** tape thats shows as the difference between tape size
		** and remaining tape on an EXB-8200 when rewound
		*/
#define 	EXB_FUDGE_FACTOR 1279
		error_count = pack(0,buf[16],buf[17],buf[18]);
		if (d->data_direction == FTT_DIR_READING) {
		    set_stat(b,FTT_READ_ERRORS,itoa(error_count),0);
		    set_stat(b,FTT_READ_COUNT,itoa(
			tape_size - remain_tape - EXB_FUDGE_FACTOR),0);
		    set_stat(b,FTT_WRITE_ERRORS,"0",0);
		    set_stat(b,FTT_WRITE_COUNT,"0",0);
		} else {
		    set_stat(b,FTT_WRITE_ERRORS,itoa(error_count),0);
		    set_stat(b,FTT_WRITE_COUNT,itoa(
			tape_size - remain_tape - EXB_FUDGE_FACTOR),0);
		    set_stat(b,FTT_READ_ERRORS,"0",0);
		    set_stat(b,FTT_READ_COUNT,"0",0);
		}
	        set_stat(b,FTT_COUNT_ORIGIN,"Exabyte Extended Sense",0);
	    }
	    if (stat_ops & FTT_DO_05RS) {
		set_stat(b,FTT_TRACK_RETRY, itoa((long)buf[26]), 0);
		set_stat(b,FTT_UNDERRUN,    itoa((long)buf[11]), 0);
	    }
	    if (stat_ops & FTT_DO_DLTRS) {
		set_stat(b,FTT_MOTION_HOURS,itoa(pack(0,0,buf[19],buf[20])),0);
		set_stat(b,FTT_POWER_HOURS, itoa(pack(buf[21],buf[22],buf[23],buf[24])),0);
	    }
	}
    }
    if (stat_ops & FTT_DO_LSRW) {
	static unsigned char cdb_log_senser[]= {0x4d, 0x00, 0x43, 0x00, 0x00, 
						0x00, 0x00, 0, 128, 0};
	static unsigned char cdb_log_sensew[]= {0x4d, 0x00, 0x42, 0x00, 0x00, 
						0x00, 0x00, 0, 128, 0};

	/* log sense read data */
	res = ftt_do_scsi_command(d,"Log Sense", cdb_log_senser, 10, 
				  buf, 128, 10, 0);
	if(res < 0){
	    ftt_errno = FTT_EPARTIALSTAT;
	    return res;
	} else {
	    decrypt_ls(b,buf,3,FTT_READ_ERRORS,1);
	    decrypt_ls(b,buf,5,FTT_READ_COUNT,1024);
	}

	/* log sense write data */
	res = ftt_do_scsi_command(d,"Log Sense", cdb_log_sensew, 10, 
				  buf, 128, 10, 0);
	if(res < 0){
	    ftt_errno = FTT_EPARTIALSTAT;
	    return res;
	} else {
	    decrypt_ls(b,buf,3,FTT_WRITE_ERRORS,1);
	    decrypt_ls(b,buf,5,FTT_WRITE_COUNT,1024);
	}
	set_stat(b,FTT_COUNT_ORIGIN,"Log Sense",0);
    }
    if (stat_ops & FTT_DO_LSC) {
	static unsigned char cdb_log_sensec[]= {0x4d, 0x00, 0x72, 0x00, 0x00, 
						0x00, 0x00, 0, 128, 0};

	/* log sense compression data */
	res = ftt_do_scsi_command(d,"Log Sense", cdb_log_sensec, 10, 
				  buf, 128, 10, 0);
	if(res < 0){
	    ftt_errno = FTT_EPARTIALSTAT;
	    return res;
	} else {
	    decrypt_ls(b,buf,0,FTT_READ_COMP,1);
	    decrypt_ls(b,buf,1,FTT_WRITE_COMP,1);
	}
    }
    if (stat_ops & FTT_DO_RP || stat_ops & FTT_DO_RP_SOMETIMES) {
	static unsigned char cdb_read_position[]= {0x34, 0x00, 0x00, 0x00, 0x00,
						0x00, 0x00, 0x00, 0x00, 0x00};

	res = ftt_do_scsi_command(d,"Read Position", cdb_read_position, 10, 
				  buf, 20, 10, 0);
	
	if (res < 0) {
	    if (!(stat_ops & FTT_DO_RP_SOMETIMES)) {
		ftt_errno = FTT_EPARTIALSTAT;
		return res;
	    }
	} else {
	    set_stat(b,FTT_BOT,     itoa(bit(7,buf[0])), 0);
	    if( bit(7,buf[0]) ) {
		d->current_file = 0;
		d->current_block = 0;
		d->current_valid = 1;
	    }
	    set_stat(b,FTT_PEOT,    itoa(bit(6,buf[0])), 0);
	    set_stat(b,FTT_BLOC_LOC,itoa(pack(buf[4],buf[5],buf[6],buf[7])),0);
	}
    }
    return 0;
}

int
ftt_clear_stats(ftt_descriptor d) {
    static unsigned char buf[256];
    int stat_ops, res;

    if ((d->flags & FTT_FLAG_SUID_SCSI) && 0 != geteuid()) {
	switch(ftt_fork(d)){
	case -1:
		return -1;

	case 0:  /* child */
		fflush(stdout);	/* make async_pf stdout */
		close(1);
		dup(fileno(d->async_pf));
		return execlp("ftt_suid", "ftt_suid", "-c", d->basename, 0);

	default: /* parent */
		return ftt_wait(d);
	}
    }

    stat_ops = ftt_get_stat_ops(d->prod_id);
    if (stat_ops & FTT_DO_TUR) {
        static unsigned char cdb_tur[]	     = {0x00, 0x00, 0x00, 0x00, 0x00, 0x00};

	res = ftt_do_scsi_command(d,"Test Unit Ready", cdb_tur, 6, 0, 0, 10, 0);
	res = ftt_do_scsi_command(d,"Test Unit Ready", cdb_tur, 6, 0, 0, 10, 0);
    }
    if (stat_ops & FTT_DO_INQ) {
	static unsigned char cdb_inquiry[]   = {0x12, 0x00, 0x00, 0x00,   56, 0x00};

	/* double check our id... */
	res = ftt_do_scsi_command(d,"Inquiry", cdb_inquiry, 6, buf, 56, 10, 0);
	buf[16] = 0;
	if ( 0 != strcmp((char *)d->prod_id,(char *)buf+8)) {
	    char *tmp;

	    /* update or product id and stat_ops if we were wrong */
	    tmp = d->prod_id;
	    d->prod_id = strdup((char *)buf+8);
	    free(tmp);
	    stat_ops = ftt_get_stat_ops(d->prod_id);
	}
    }
    if (stat_ops & FTT_DO_EXBRS) {
    	static unsigned char cdb_clear_rs[]  = { 0x03, 0x00, 0x00, 0x00, 30, 0x80 };
	res = ftt_do_scsi_command(d,"Clear Request Sense", cdb_clear_rs, 6, buf, 30, 10, 0);
    }
    if (stat_ops & FTT_DO_LSRW) {
        static unsigned char cdb_clear_ls[] = { 0x4c, 0x02, 0x40, 0x00, 0x00, 0x00, 
					0x00, 0x00, 0x00, 0x00};
	res = ftt_do_scsi_command(d,"Clear Request Sense", cdb_clear_ls, 10, buf, 256, 10, 0);
    }
    return res;
}

char *
ftt_extract_stats(ftt_stat_buf b, int stat){

    ENTERING("ftt_extract_stats");
    PCKNULL("statistics buffer pointer",b);

    if (stat < FTT_MAX_STAT && stat >= 0 ) {
	return b->value[stat];
    } else {
	ftt_eprintf("ftt_extract_stats was given an out of range statistic number.");
	ftt_errno= FTT_EFAULT;
	return 0;
    }
}
