static char rcsid[] = "@(#)$Id$";
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <ftt_private.h>
#include <ftt_dbd.h>

#ifdef WIN32
#include <io.h>
#include <process.h>
#include <windows.h>
#include <winioctl.h>

#define geteuid() -1
DWORD ftt_win_get_paramters();

#else
#include <unistd.h>
#endif

int ftt_open_io_dev();

extern int errno;

char *
ftt_get_prod_id(ftt_descriptor d) {
    return d->prod_id;
}

ftt_stat_buf
ftt_alloc_stat() {
    void *res;

    ENTERING("ftt_alloc_stat");
    res =  malloc(sizeof(ftt_stat));
    if (0 != res) {
	memset(res,0,sizeof(ftt_stat));
	return res;
    } else {
	ftt_eprintf("ftt_alloc_stat: unable to allocate statistics buffer, errno %d\n", errno);
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
#ifdef WIN32
	static char *
	ftt_itoa_Large(LARGE_INTEGER n) {
		static char buf[128];
		sprintf(buf,"%ld", n.LowPart);
		return buf;
	}
#endif

char *
ftt_itoa(long n) {
	static char buf[128];

	sprintf(buf,"%ld", n);
	return buf;
}

static char *
ftt_dtoa(double n) {
	static char buf[128];

	sprintf(buf,"%.0f", n);
	return buf;
}

/* set_stat
**
** handy routine to fill in the n-th slot in the stat buffer
*/
static void
set_stat( ftt_stat_buf b, int n, char *pcStart, char *pcEnd) {
    char save = 'n';
    int do_freeme = 0;
	char *freeme;

    /* clean out old value */
    if (b->value[n] != 0) {
		do_freeme = 1;
		freeme = b->value[n];
		b->value[n] = 0;
    }

    /* if null, leave it */
    if (pcStart != 0) {
	
		/* null terminate at pcEnd, copy the string, 
		** and then put the byte back where we scribbled the null
		** ... after eating blanks off the end
		*/
		if (pcEnd == 0) {
		pcEnd = pcStart + strlen(pcStart); /* pcEnd = pcStart if string has ) length */
		}
		if ( *pcEnd ) {
		  do {
		    pcEnd--;
		  } while(*pcEnd == ' ');
		  pcEnd++;
		  save = *pcEnd;
		  *pcEnd = 0;
		}
		DEBUG3(stderr,"Setting stat %d(%s) to %s\n",n,ftt_stat_names[n],pcStart);
		b->value[n] = strdup(pcStart);
		/* why is this "if" here??? I forget now... mengel*/
		if ( save != 'n' ) *pcEnd = save;
	}
    if (do_freeme) {
         free(freeme);
    }
}

int ftt_numeric_tab[FTT_MAX_STAT] = {
    /*  FTT_VENDOR_ID		0 */ 0,
    /*  FTT_PRODUCT_ID		1 */ 0,
    /*  FTT_FIRMWARE		2 */ 0,
    /*  FTT_SERIAL_NUM		3 */ 0,
    /*  FTT_CLEANING_BIT	5 */ 0,
    /*  FTT_READ_COUNT		6 */ 1,
    /*  FTT_WRITE_COUNT		7 */ 1,
    /*  FTT_READ_ERRORS		8 */ 1,
    /*  FTT_WRITE_ERRORS	9 */ 1,
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
    /*  FTT_SENSE_KEY		46 */ 0,
    /*  FTT_TRANS_SENSE_KEY	47 */ 0,
    /*  FTT_RETRIES		48 */ 1,
    /*  FTT_FAIL_RETRIES	49 */ 1,
    /*  FTT_RESETS		50 */ 1,
    /*  FTT_HARD_ERRORS		51 */ 1,
    /*  FTT_UNC_WRITE		50 */ 1,
    /*  FTT_UNC_READ		51 */ 1,
    /*  FTT_CMP_WRITE		52 */ 1,
    /*  FTT_CMP_READ		53 */ 1,
    /*  FTT_ERROR_CODE		54 */ 0,
    /*  FTT_CUR_PART		54 */ 0,
    /*  FTT_MOUNT_PART		54 */ 0,
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
 	    set_stat(res, i, ftt_itoa((long)atoi(b1->value[i]) + atoi(b2->value[i])),0);
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
 	    set_stat(res, i, ftt_itoa((long)atoi(b1->value[i]) - atoi(b2->value[i])),0);
        } else if ( b1->value[i] ) {
 	    set_stat(res, i, b1->value[i],0);
        } else if ( b2->value[i] && ftt_numeric_tab[i] ) {
 	    set_stat(res, i, ftt_itoa(-(long)atoi(b2->value[i])),0);
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
     (((unsigned long)(a)<<24) + ((unsigned long)(b)<<16) + ((unsigned long)(c)<<8) + (unsigned long)(d))

#define bit(n,byte) (unsigned long)(((byte)>>(n))&1)

/*
** unpack_ls does the error and total kb for read and write data
** you pass in the statbuf pointer, the log sense data buffer
** and the page code and statistic number for each.
*/
static double
decrypt_ls(ftt_stat_buf b,unsigned char *buf, int param, int stat, int divide) {
    static char printbuf[128];
    unsigned char *page;
    int thisparam, thislength;
    int i;
    int length;
    double value;

    DEBUG3(stderr,"entering decrypt_ls for parameter %d stat %d\n", param, stat);
    page = buf + 4;
    length = pack(0,0,buf[2],buf[3]);
    while( page < (buf + length) ) {
	thisparam = pack(0,0,page[0],page[1]);
	thislength = page[3];
	value = 0.0;
	for(i = 0; i < thislength ; i++) {
	    value = value * 256 + page[4+i];
	}
	DEBUG3(stderr, "parameter %d, length %d value %f\n", thisparam, thislength, value);
	if ( thisparam == param ) {
	    sprintf(printbuf, "%.0f", value / divide);
	    set_stat(b,stat,printbuf,0);
	    DEBUG3(stderr," stat %d - value %s = %.0f \n",stat,printbuf,value / divide);
	}
	page += 4 + thislength;
    }
}

int
ftt_get_stat_ops(char *name) {
    int i;
    DEBUG4(stderr, "Entering: get_stat_ops\n");
    if (*name == 0) {
	return 0; /* unknown device id */
    }
    for (i = 0; ftt_stat_op_tab[i].name != 0; i++ ) {
	if (ftt_matches(name, ftt_stat_op_tab[i].name)) {
            DEBUG2(stderr, "found stat_op == %x\n", i);
	    return ftt_stat_op_tab[i].stat_ops;
	}
    }
    return 0;
}

int
ftt_get_stats(ftt_descriptor d, ftt_stat_buf b) {
    int res;


#ifndef WIN32
    int hwdens;
    int failures = 0;
    int i;
    unsigned char buf[512];
    long int tape_size, error_count, data_count;
    double remain_tape;
    int n_blocks, block_length;
    int stat_ops;
#endif

    ENTERING("ftt_get_stats");
    CKNULL("ftt_descriptor", d);
    CKNULL("statistics buffer pointer", b);


    memset(b,0,sizeof(ftt_stat));


    if ((d->flags & FTT_FLAG_SUID_SCSI) && 0 != geteuid()) {
	ftt_close_dev(d);
	switch(ftt_fork(d)){
	case -1:
		return -1;

	case 0:  /* child */
		fflush(stdout);	/* make async_pf stdout */
		fflush(d->async_pf_parent);
		close(1);
		dup2(fileno(d->async_pf_parent),1);
		if (d->data_direction == FTT_DIR_WRITING) {
		     if (ftt_debug) {
		         execlp("ftt_suid", "ftt_suid", "-x",  "-w", "-s", d->basename, 0);
		     } else {
		          execlp("ftt_suid", "ftt_suid", "-w", "-s", d->basename, 0);
		     }
		} else {
		     if (ftt_debug) {
		         execlp("ftt_suid", "ftt_suid", "-x", "-s", d->basename, 0);
		     } else {
		         execlp("ftt_suid", "ftt_suid", "-s", d->basename, 0);
		     }
		}

	default: /* parent */
		ftt_undump_stats(b,d->async_pf_child);
		res = ftt_wait(d);
	}
    }

    /* Things we know without asking, and the suid program won't know */
    set_stat(b,FTT_FILE_NUMBER, ftt_itoa((long)d->current_file), 0);
    set_stat(b,FTT_BLOCK_NUMBER, ftt_itoa((long)d->current_block), 0);
    set_stat(b,FTT_USER_READ,ftt_itoa((long)d->readkb), 0);
    set_stat(b,FTT_USER_WRITE,ftt_itoa((long)d->writekb), 0);
    set_stat(b,FTT_N_READS,ftt_itoa((long)d->nreads), 0);
    set_stat(b,FTT_N_WRITES,ftt_itoa((long)d->nwrites), 0);
    set_stat(b,FTT_RETRIES,ftt_itoa((long)d->nretries), 0);
    set_stat(b,FTT_FAIL_RETRIES,ftt_itoa((long)d->nfailretries), 0);
    set_stat(b,FTT_RESETS,ftt_itoa((long)d->nresets), 0);
    set_stat(b,FTT_HARD_ERRORS,ftt_itoa((long)d->nharderrors), 0);

#ifndef WIN32

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
    if (0 == strncmp(d->controller,"SCSI",4)) {
        stat_ops = FTT_DO_TUR|FTT_DO_RS|FTT_DO_INQ;
    } else {
        stat_ops = 0;
    }

    /*
    ** First do a request sense, and check for any error conditions
    ** etc. that an inquiry might clear.
    ** Then we'll do an inquiry, and find out what kind of drive
    ** this *really* is, then *another* request sense, and check
    ** for drive specific data.
    */
    if (stat_ops & FTT_DO_TUR) {
        static unsigned char cdb_tur[]	     = {0x00, 0x00, 0x00, 0x00, 0x00, 0x00};

	res = ftt_do_scsi_command(d,"Test Unit Ready", cdb_tur, 6, 0, 0, 10, 0);
	set_stat(b,FTT_TUR_STATUS,ftt_itoa((long)-res), 0);
	if (res < 0) {
	    set_stat(b,FTT_READY,"0",0);
	} else {
	    set_stat(b,FTT_READY,"1",0);
	}
    }

    if (stat_ops & FTT_DO_RS) {
	static unsigned char cdb_req_sense[] = {0x03, 0x00, 0x00, 0x00,   18, 0x00};

	/* request sense data */
	res = ftt_do_scsi_command(d,"Req Sense", cdb_req_sense, 6, buf, 18, 10, 0);
	if(res < 0){
	    failures++;
	} else {
	    static char *sense_key_trans[] = {
		"NO_SENSE", "NOT_USED", "NOT_READY", "MEDIUM_ERROR",
		"HARDWARE_ERROR", "ILLEGAL_REQUEST", "UNIT_ATTENTION",
		"DATA_PROTECT", "BLANK_CHECK", "EXABYTE", "COPY_ABORTED",
		"ABORTED_COMMAND", "NOT_USED", "VOLUME_OVERFLOW",
		"NOT_USED", "RESERVED",
	    };
	    set_stat(b,FTT_ERROR_CODE, ftt_itoa((long)buf[0]&0xf), 0);
	    set_stat(b,FTT_SENSE_KEY, ftt_itoa((long)buf[2]&0xf), 0);
	    set_stat(b,FTT_TRANS_SENSE_KEY, sense_key_trans[buf[2]&0xf], 0);
	    set_stat(b,FTT_FMK, ftt_itoa((long)bit(7,buf[2])), 0);
	    set_stat(b,FTT_EOM, ftt_itoa((long)bit(6,buf[2])),0);
	    set_stat(b,FTT_ILI, ftt_itoa((long)bit(5,buf[2])),0);
	    set_stat(b,FTT_SCSI_ASC,ftt_itoa((long)buf[12]),0);
	    set_stat(b,FTT_SCSI_ASCQ,ftt_itoa((long)buf[13]),0);

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
			break;
	    case 0x8002:
			set_stat(b,FTT_CLEANING_BIT,"1",0);
			break;
	    }
	}
    }

    if (stat_ops & FTT_DO_INQ) {
	static unsigned char cdb_inquiry[]   = {0x12, 0x00, 0x00, 0x00,   56, 0x00};

	/* basic scsi inquiry */
	res = ftt_do_scsi_command(d,"Inquiry", cdb_inquiry, 6, buf, 56, 10, 0);
	if(res < 0){
	    failures++;
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


	    }
	    /*
	     * look up based on ANSI version *and* product id, so
	     * we can have generic SCSI-2 cases, etc.
	     */
	    sprintf(buf, "%d%s", buf[2] & 0x3, d->prod_id);
	    stat_ops = ftt_get_stat_ops(buf);
	}
    }

    /* 
    ** Get other vendor specific request sense available data now that we know for sure
    ** what kind of drive this is from the inquiry data.
    */

    if (stat_ops & FTT_DO_VSRS) {
	static unsigned char cdb_req_sense[] = {0x03, 0x00, 0x00, 0x00,   32, 0x00};

	/* request sense data */
	res = ftt_do_scsi_command(d,"Req Sense", cdb_req_sense, 6, buf, 32, 10, 0);
	if(res < 0){
	    failures++;
	} else {
	    if (stat_ops & FTT_DO_EXBRS) {
		set_stat(b,FTT_BOT,         ftt_itoa((long)bit(0,buf[19])), 0);
		set_stat(b,FTT_TNP,	        ftt_itoa((long)bit(1,buf[19])), 0);
		set_stat(b,FTT_PF,          ftt_itoa((long)bit(7,buf[19])), 0);
		set_stat(b,FTT_WRITE_PROT,  ftt_itoa((long)bit(5,buf[20])), 0);
		set_stat(b,FTT_PEOT,        ftt_itoa((long)bit(2,buf[21])), 0);
		set_stat(b,FTT_CLEANING_BIT,ftt_itoa((long)bit(3,buf[21])), 0);
		set_stat(b,FTT_CLEANED_BIT, ftt_itoa((long)bit(4,buf[21])), 0);

		remain_tape=(double)pack(0,buf[23],buf[24],buf[25]);
		error_count = pack(0,buf[16],buf[17],buf[18]);

		if (d->prod_id[5] == '9') {
		     
                     DEBUG2(stderr, "remain_tape 8900 case... \n");
		     /* 8900's count 16k blocks, not 1k blocks */
		     remain_tape *= 16.0;
		} else {
                     DEBUG2(stderr, "remain_tape non-8900 case... \n");
		     ;
		}
		set_stat(b,FTT_REMAIN_TAPE,ftt_dtoa(remain_tape),0);
		if (d->data_direction ==  FTT_DIR_READING) {
	            set_stat(b,FTT_READ_ERRORS,ftt_itoa((long)error_count),0);
		} else {
	            set_stat(b,FTT_WRITE_ERRORS,ftt_itoa((long)error_count),0);
		}
	    }
	    if (stat_ops & FTT_DO_05RS) {
		set_stat(b,FTT_TRACK_RETRY, ftt_itoa((long)buf[26]), 0);
		set_stat(b,FTT_UNDERRUN,    ftt_itoa((long)buf[11]), 0);
	    }
	    if (stat_ops & FTT_DO_DLTRS) {
		set_stat(b,FTT_MOTION_HOURS,ftt_itoa((long)pack(0,0,buf[19],buf[20])),0);
		set_stat(b,FTT_POWER_HOURS, ftt_itoa((long)pack(buf[21],buf[22],buf[23],buf[24])),0);
		set_stat(b,FTT_REMAIN_TAPE, ftt_dtoa((double)pack(buf[25],buf[26],buf[27],buf[28])*4),0); 
	    }
	}
    }
    if (stat_ops & FTT_DO_SN) {
        static unsigned char cdb_inq_w_sn[]  = {0x12, 0x01, 0x80, 0x00,   14, 0x00};

	/* scsi inquiry w/ serial number */
	res = ftt_do_scsi_command(d,"Inquiry", cdb_inq_w_sn, 6, buf, 14, 10, 0);
	if(res < 0){
	    failures++;
	} else {
	    set_stat(b,FTT_SERIAL_NUM, (char *)buf+4, (char *)buf+14);
	}
    }
    if (stat_ops & FTT_DO_MS) {

	static unsigned char cdb_mode_sense[]= {0x1a, 0x00, 0x00, 0x00,   12, 0x00};

	res = ftt_do_scsi_command(d,"mode sense",cdb_mode_sense, 6, buf, 12, 10, 0);
	if (res == -2) {
	    /* retry on a CHECK CONDITION, it may be okay */
	    res = ftt_do_scsi_command(d,"mode sense",cdb_mode_sense, 6, buf, 12, 10, 0);
	}
	if(res < 0){
	    failures++;
	} else {

	    hwdens = buf[4];
	    set_stat(b,FTT_DENSITY,  ftt_itoa((long)hwdens), 0);
	    set_stat(b,FTT_WRITE_PROT,  ftt_itoa((long)bit(7,buf[2])),0);
	    set_stat(b,FTT_MEDIA_TYPE,  ftt_itoa((long)buf[1]), 0);

	    n_blocks =     pack(0,buf[5],buf[6],buf[7]);
	    block_length = pack(0,buf[9],buf[10],buf[11]);
	    tape_size =    n_blocks;

	    set_stat(b,FTT_BLOCK_SIZE,  ftt_itoa((long)block_length),0);
	    set_stat(b,FTT_BLOCK_TOTAL, ftt_itoa((long)n_blocks),    0);

	    if (stat_ops & FTT_DO_EXBRS) {
		/* 
		** the following lies still allow reasonable results
		** from doing before/after deltas
		** we'll override them with log sense data if we have it.
		** The following is a fudge factor for the amount of
		** tape thats shows as the difference between tape size
		** and remaining tape on an EXB-8200 when rewound
		*/
#define 	EXB_8200_FUDGE_FACTOR 1279
		
		if (stat_ops & FTT_DO_EXB82FUDGE) {
			data_count = tape_size - remain_tape - EXB_8200_FUDGE_FACTOR;
		} else {
			data_count = tape_size - remain_tape;
		}
		if (d->data_direction ==  FTT_DIR_READING) {
		    set_stat(b,FTT_READ_COUNT,ftt_itoa(data_count),0);
		} else {
		    set_stat(b,FTT_WRITE_COUNT,ftt_itoa(data_count),0);
		}
		set_stat(b,FTT_COUNT_ORIGIN,"Exabyte_Extended_Sense",0);
	    }

	    for ( i = 0; d->devinfo[i].device_name !=0 ; i++ ) {
		if( buf[4] == d->devinfo[i].hwdens ) {
		    set_stat(b,FTT_TRANS_DENSITY, ftt_itoa((long)d->devinfo[i].density),0);
		    set_stat(b,FTT_TRANS_COMPRESS, ftt_itoa((long)d->devinfo[i].mode),0);
		    break;
		}
	    }
	}
    }
    if (stat_ops & FTT_DO_MS_Px0f) {
	static unsigned char cdb_mode_sense_p09[]= 
			{ 0x1a, 0x08, 0x0f, 0x00,   20, 0x00};

	res = ftt_do_scsi_command(d,"mode sense",cdb_mode_sense_p09, 
				  6, buf, 20, 10, 0);
	if(res < 0){
	    ftt_errno = FTT_EPARTIALSTAT;
	} else {
	    set_stat(b,FTT_TRANS_COMPRESS,     ftt_itoa((long)((buf[4+2]>>7)&1)), 0);
	}
    }
    if (stat_ops & FTT_DO_MS_Px10) {

	static unsigned char cdb_mode_sense_p10[]= 
			{ 0x1a, 0x08, 0x10, 0x00,   20, 0x00};


	res = ftt_do_scsi_command(d,"mode sense",cdb_mode_sense_p10, 
				  6, buf, 20, 10, 0);
	if(res < 0){
	    ftt_errno = FTT_EPARTIALSTAT;
	} else {
	    set_stat(b,FTT_TRANS_COMPRESS,     ftt_itoa((long)buf[4+14]), 0);
	}
	
    }
    if (stat_ops & FTT_DO_MS_Px20_EXB && hwdens == 0) {
	static unsigned char cdb_mode_sense_p20[]= 
			{ 0x1a, 0x08, 0x20, 0x00, 0x0a, 0x00};

	res = ftt_do_scsi_command(d,"mode sense",cdb_mode_sense_p20, 
				  6, buf, 20, 10, 0);
	if(res < 0){
	    ftt_errno = FTT_EPARTIALSTAT;
	} else {
	    set_stat(b,FTT_TRANS_DENSITY,     ftt_itoa(!bit(5,buf[7])), 0);
	    set_stat(b,FTT_TRANS_COMPRESS,    ftt_itoa( bit(6,buf[7])), 0);
	}
    }
    if (stat_ops & FTT_DO_LS) {
 	int npages;
	static char buf2[128];
	static unsigned char cdb_log_sense[]= {0x4d, 0x00, 0x00, 0x00, 0x00, 
						   0x00, 0x00, 0, 128, 0};


        /* check supported page list, we want 0x32 or 0x39... */
	cdb_log_sense[2] = 0;
	res = ftt_do_scsi_command(d,"Log Sense", cdb_log_sense, 10, 
				  buf2, 128, 10, 0);

        npages = pack(0,0,buf2[2],buf2[3]);
	for( i = 0; i <= npages; i++ ) {
	    int do_page;

	    do_page = buf2[4+i];
            switch( do_page ) {
	       	case 0x02:
		case 0x03:
	       	case 0x31: 
		case 0x32:
	       	case 0x39:

		    cdb_log_sense[2] = 0x40 | do_page;
		    res = ftt_do_scsi_command(d,"Log Sense", cdb_log_sense, 10, 
					      buf, 128, 10, 0);
		    if(res < 0) {
		    	failures++;
		    } else { 
		        switch( do_page ) {
		        case 0x02:
			    (void)decrypt_ls(b,buf,3,FTT_WRITE_ERRORS,1);
			    (void)decrypt_ls(b,buf,5,FTT_WRITE_COUNT,1024);
			    break;
		        case 0x03:
			    (void)decrypt_ls(b,buf,3,FTT_READ_ERRORS,1);
			    (void)decrypt_ls(b,buf,5,FTT_READ_COUNT,1024);
			    break;
		        case 0x31:
			    (void)decrypt_ls(b,buf,1,FTT_REMAIN_TAPE,1);
		        case 0x32:
			    (void)decrypt_ls(b,buf,0,FTT_READ_COMP,1);
			    (void)decrypt_ls(b,buf,1,FTT_WRITE_COMP,1);
			    (void)decrypt_ls(b,buf,3,FTT_UNC_READ,1);
			    (void)decrypt_ls(b,buf,5,FTT_CMP_READ,1);
			    (void)decrypt_ls(b,buf,7,FTT_UNC_WRITE,1);
			    (void)decrypt_ls(b,buf,9,FTT_CMP_WRITE,1);
		        case 0x39: {
			    double uw, ur, cw, cr;
			    uw = decrypt_ls(b,buf,5,FTT_UNC_WRITE,1024);
			    ur = decrypt_ls(b,buf,6,FTT_UNC_READ,1024);
			    cw = decrypt_ls(b,buf,7,FTT_CMP_WRITE,1024);
			    cr = decrypt_ls(b,buf,8,FTT_CMP_READ,1024);
                            if (ur != 0.0) {
			       set_stat(b,FTT_READ_COMP,  ftt_itoa((long)(100.0*cr/ur)), 0);
			    }
                            if (uw != 0.0) {
			       set_stat(b,FTT_WRITE_COMP, ftt_itoa((long)(100.0*cw/uw)), 0);}
			    }
		        }
                    }
		    break;
             }
	
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
	    }
	} else {
	    set_stat(b,FTT_BOT,     ftt_itoa(bit(7,buf[0])), 0);
	    set_stat(b,FTT_PEOT,    ftt_itoa(bit(6,buf[0])), 0);
	    set_stat(b,FTT_BLOC_LOC,ftt_itoa(pack(buf[4],buf[5],buf[6],buf[7])),0);
	    set_stat(b,FTT_CUR_PART,ftt_itoa(buf[1]),0);
	}
    }
    if (stat_ops & FTT_DO_MS_Px21 ) {
	static unsigned char cdb_ms21[6] = {0x1a, DBD, 0x21, 0x00, 10, 0x00};
        int loadpart;
        
	res = ftt_do_scsi_command(d,"Mode Sense, 0x21", cdb_ms21, 6, buf, 10, 10, 0);
	loadpart = (buf[BD_SIZE+3] >> 1) & 0x3f;
	set_stat(b,FTT_MOUNT_PART,ftt_itoa(loadpart),0);
    }
    if (failures > 0) {
	ftt_eprintf("ftt_get_stats, %d scsi requests failed\n", failures);
	ftt_errno = FTT_EPARTIALSTAT;
	return -1;
    } else {
        return 0;
    }
#else /* this is the WIN32 part */
	{
		DWORD fres,par,pos,pos2;
		
		HANDLE fh ;
		TAPE_GET_MEDIA_PARAMETERS gmp;
		TAPE_GET_DRIVE_PARAMETERS gdp;
		if ( ftt_open_io_dev(d) < 0 ) {
			ftt_eprintf("ftt_get_stats, Device is not opened \n");
				ftt_errno = FTT_EPARTIALSTAT;
				return -1;
		}
		fh = (HANDLE)d->file_descriptor;
		fres = ftt_win_get_paramters(d,&gmp,&gdp);
		if ( fres < 1100 ) {
			set_stat(b,FTT_BLOCK_SIZE,ftt_itoa(gmp.BlockSize),0);
			set_stat(b,FTT_WRITE_PROT,ftt_itoa((int)gmp.WriteProtected),0);
		
			if ( gdp.FeaturesLow & TAPE_DRIVE_TAPE_REMAINING ) {
				set_stat(b,FTT_REMAIN_TAPE, ftt_itoa_Large(gmp.Remaining),0);
			}
			
			set_stat(b,FTT_TRANS_COMPRESS,ftt_itoa(gdp.Compression),0);
			set_stat(b,FTT_TRANS_DENSITY,"0",0); /*this has to be 0*/

		}
		else {
			ftt_eprintf("ftt_get_stats, Getting Media & Drive Parameters Failed \n");
			ftt_errno = FTT_EPARTIALSTAT;
			return -1;
		}
		par = pos = pos = (DWORD)-1;
		fres = GetTapePosition(fh,TAPE_LOGICAL_POSITION,&par,&pos,&pos2);
		if ( pos >= 0 ) { 
			set_stat(b,FTT_BOT,ftt_itoa((pos == 0 )?1:0),0);
		}
	}
#endif
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
		dup(fileno(d->async_pf_parent));
		return execlp("ftt_suid", "ftt_suid", "-c", d->basename, 0);

	default: /* parent */
		return ftt_wait(d);
	}
    }


    if (0 == strncmp(d->controller,"SCSI",4)) {
        stat_ops = FTT_DO_RS|FTT_DO_INQ;
    } else {
	stat_ops = 0;
    }
    if (stat_ops & FTT_DO_TUR) {
        static unsigned char cdb_tur[]	     = {0x00, 0x00, 0x00, 0x00, 0x00, 0x00};

	res = ftt_do_scsi_command(d,"Test Unit Ready", cdb_tur, 6, 0, 0, 10, 0);
	res = ftt_do_scsi_command(d,"Test Unit Ready", cdb_tur, 6, 0, 0, 10, 0);
    }
    if (stat_ops & FTT_DO_INQ) {
	static unsigned char cdb_inquiry[]   = {0x12, 0x00, 0x00, 0x00,   56, 0x00};

	/* double check our id... */
	res = ftt_do_scsi_command(d,"Inquiry", cdb_inquiry, 6, buf, 56, 10, 0);
	buf[32] = 0;
	if ( 0 != strcmp((char *)d->prod_id,(char *)buf+16)) {
	    char *tmp;

	    /* update or product id and stat_ops if we were wrong */
	    tmp = d->prod_id;
	    d->prod_id = strdup((char *)buf+16);
	    free(tmp);
	    stat_ops = ftt_get_stat_ops(d->prod_id);
	}
    }
    if (stat_ops & FTT_DO_EXBRS) {
    	static unsigned char cdb_clear_rs[]  = { 0x03, 0x00, 0x00, 0x00, 30, 0x80 };
	res = ftt_do_scsi_command(d,"Clear Request Sense", cdb_clear_rs, 6, buf, 30, 10, 0);
	if (res < 0) return res;
    }
    if (stat_ops & FTT_DO_LS) {
        static unsigned char cdb_clear_ls[] = { 0x4c, 0x02, 0x40, 0x00, 0x00, 0x00, 
					0x00, 0x00, 0x00, 0x00};
	res = ftt_do_scsi_command(d,"Clear Request Sense", cdb_clear_ls, 10, 0, 0, 10, 1);
	if (res < 0) return res;
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

int
ftt_stats_status(ftt_descriptor d, int time_out) {
    static ftt_stat block;
    int res;
    char *p;

    res = ftt_get_stats(d,&block);
    if (res < 0) {
	if (ftt_errno == FTT_EBUSY) {
	    return FTT_BUSY;
	} else {
	    return res;
	}
    }

	while (time_out > 0 ) {
		p = ftt_extract_stats(&block, FTT_READY);
		if ( p && atoi(p)) {
			break;
		}
		sleep(1);
		time_out--;
		res = ftt_get_stats(d,&block);
	}
    res = 0;
    p = ftt_extract_stats(&block, FTT_BOT);
    if ( p && atoi(p)) {
	DEBUG3(stderr,"setting ABOT flag\n");
	res |= FTT_ABOT;
    }
    p = ftt_extract_stats(&block, FTT_EOM);
    if ( p && atoi(p)) {
	DEBUG3(stderr,"setting AEOT flag\n");
	res |= FTT_AEOT;
	res |= FTT_AEW;
    }
    p = ftt_extract_stats(&block, FTT_WRITE_PROT);
    if ( p && atoi(p)) {
	DEBUG3(stderr,"setting PROT flag\n");
	res |= FTT_PROT;
    }
    p = ftt_extract_stats(&block, FTT_READY);
    if ( p && atoi(p)) {
	DEBUG3(stderr,"setting ONLINE flag\n");
	res |= FTT_ONLINE;
    }
    return res;
}
