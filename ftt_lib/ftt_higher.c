#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "ftt_private.h"
extern int errno;

int
ftt_verify_vol_label(ftt_descriptor d, int type, char *vollabel, 
			int timeout, int rdonly) {
    char *buf;
    char label_buf[512];
    int res;
    char *pname;
    int len;
    int blocksize;

    ENTERING("ftt_verify_vol_label");
    CKNULL("ftt_descriptor", d);
    CKNULL("volume label", vollabel);

    if (type >= FTT_MAX_HEADER || type < 0) {
	ftt_errno = FTT_ENOTSUPPORTED;
	ftt_eprintf("ftt_verify_vol_label: unsupported type number %d", type);
	return -1;
    }

    res = ftt_status(d,timeout);	if (res < 0) return res;

    if (0 == (res & FTT_ONLINE)) {
	ftt_errno = FTT_ENOTAPE;
	ftt_eprintf("ftt_verify_vol_label: the drive is empty");
	return -1;
    }

    if (0 != (res & FTT_BUSY)) {
	ftt_errno = FTT_EBUSY;
	ftt_eprintf("ftt_verify_vol_label: the drive is busy");
	return -1;
    }

    if (0 != (res & FTT_PROT) && rdonly == FTT_RDWR) {
	ftt_eprintf("ftt_verify_vol_label: unexpected write protection");
	ftt_errno = FTT_EROFS;
	return -1;
    }

    if (0 == (res & FTT_PROT) && rdonly == FTT_RDONLY) {
	ftt_eprintf("ftt_verify_vol_label: missing expected write protection");
	ftt_errno = FTT_ERWFS;
	return -1;
    }

    res = ftt_rewind(d);  			if (res < 0) return res;

    if (type == FTT_DONTCHECK_HEADER) {
	return 0;
    } else {
	blocksize = ftt_get_max_blocksize(d);
	buf = malloc(blocksize);
	if (buf == 0) {
	    extern int errno;
	    ftt_errno = FTT_ENOMEM;
	    ftt_eprintf("ftt_verify_vol_label: Unable to allocate block to read header, errno %d",
			errno);
	    return -1;
	}
	memset(buf,0,blocksize);
	res = ftt_read(d,buf,blocksize); 	/* errors to guess_label */
	res = ftt_guess_label(buf,res,&pname, &len);if(res < 0) return res;
	if (type == res && (len == 0 || 
		(0 == strncmp(vollabel,pname,len) && len == strlen(vollabel)))){
	    return 0;
	}

	if (len > 512) len = 511;
	strncpy(label_buf,pname,len);
	label_buf[len] = 0;
	if (type == res) {
	    ftt_eprintf("ftt_verify_vol_label: expected vol '%s', but got '%s'.",
			vollabel, label_buf);
	    ftt_errno = FTT_EWRONGVOL;
	    res = -1;
	} else {
	ftt_eprintf("ftt_verify_vol_label: expected %s header, but got %s", 
		ftt_label_type_names[type], ftt_label_type_names[res]);
	    ftt_errno = FTT_EWRONGVOLTYP;
	    res = -1;
	}
        free(buf);
    }
    return res;
}

int
ftt_write_vol_label(ftt_descriptor d, int type, char *vollabel) {
    int res;
    static char buf[10240]; /* biggest blocksize of any label we support */
    int blocksize = 10240;

    CKOK(d,"ftt_write_vol_label",1,1);
    CKNULL("ftt_descriptor", d);
    CKNULL("volume label", vollabel);

    res = ftt_rewind(d);			if (res <  0) return res;
    res = ftt_format_label(buf,blocksize,vollabel, strlen(vollabel), type);
						if (res <  0) return res;
    /* next highest blocksize */
    if (d->default_blocksize != 0) {
	res = res + d->default_blocksize - 1 ;
	res = res - (res % d->default_blocksize);
    }
    res = ftt_write(d,buf,res);			if (res <  0) return res;
    ftt_close_dev(d);
    res = ftt_skip_fm(d,1);
    return res;
}

char *ftt_ascii_rewindflags[] = {
	"rewind on close",
	"retension on open",
	"byte swap",
	"read only",
	0
};

int
ftt_describe_dev(ftt_descriptor d, char *dev, FILE *pf) {
    int i;
    int j;
    int found;
    char *starter;
    char *dname;

    ENTERING("ftt_describe_dev");
    CKNULL("ftt_descriptor", d);
    CKNULL("device name", dev);
    CKNULL("stdio file handle", pf);

    found = 0;
    starter = dev;
    for (i = 0; d->devinfo[i].device_name !=0; i++) {
	dname = d->densitytrans[d->devinfo[i].density+1];
	if (dname == 0) {
	    dname = "unknown";
	}
	if (0 == strcmp(d->devinfo[i].device_name, dev)) {
	    if (d->devinfo[i].passthru) {
	        fprintf(pf, "%s SCSI pass-thru", starter);
	    } else {
	        fprintf(pf, "%s %s mode(%d), %s, (Density Code 0x%x), %s",
			starter,
			dname,
			d->devinfo[i].density, 
			d->devinfo[i].mode? "compressed":"uncompressed",
			d->devinfo[i].hwdens,
			d->devinfo[i].fixed? "fixed block":"variable block");
	   }
	    for (j = 0; ftt_ascii_rewindflags[j] != 0; j++) {
		if (d->devinfo[i].rewind & (1<<j)) {
		    fprintf(pf, ", %s", ftt_ascii_rewindflags[j]);
		}
	    }
	    fprintf(pf, "\n");
	    starter = "and also";
	    found = 1;
	}
    }
    if (found == 0) {
	ftt_eprintf("ftt_describe_dev: device name not associated with ftt descriptor");
	ftt_errno = FTT_ENOENT;
	return -1;
    }
    return 0;
}

#define LAST   1
#define TOTALS 0
ftt_stat_buf *
ftt_init_stats(ftt_descriptor d){
	ftt_stat_buf *res;
	int ires;

	ENTERING("ftt_init_stats");
	PCKNULL("ftt_descriptor",d);

	res = malloc(sizeof(ftt_stat_buf)*2);
	if (0 == res) {
	    ftt_eprintf("ftt_init_stats unable to allocate memory errno %d", errno);
	    ftt_errno = FTT_ENOMEM;
	    return 0;
	}
	res[LAST] = ftt_alloc_stat();
	res[TOTALS] = ftt_alloc_stat();
	ires = ftt_get_stats(d,res[LAST]);
	if (ires < 0) {
		ftt_free_stat(res[LAST]);
		ftt_free_stat(res[TOTALS]);
		free(res);
		return 0;
	}
	return res;
}

int
ftt_update_stats(ftt_descriptor d, ftt_stat_buf *bp){
	ftt_stat_buf delta, new_cur, tmp;
	int res;

        ENTERING("ftt_update_stats");
	CKNULL("ftt_descriptor", d);
	CKNULL("ftt_stat_buf pair pointer", bp);
	CKNULL("first ftt_stat_buf", bp[0]);
	CKNULL("second ftt_stat_buf", bp[1]);

	delta = ftt_alloc_stat(); 		if(delta == 0) return -1;
	new_cur = ftt_alloc_stat(); 		if(new_cur == 0) return -1;
	res = ftt_get_stats(d,new_cur); 	if(res < 0) return res;
	ftt_sub_stats(new_cur,bp[LAST],delta);
	if (ftt_debug > 2) {
		fprintf(stderr,"Old statistics");
		ftt_dump_stats(bp[LAST], stderr);
		fprintf(stderr,"New statistics");
		ftt_dump_stats(new_cur, stderr);
		fprintf(stderr,"delta statistics");
		ftt_dump_stats(delta, stderr);
		fprintf(stderr,"Old totals");
		ftt_dump_stats(bp[TOTALS], stderr);
	}
	ftt_add_stats(delta,bp[TOTALS],bp[TOTALS]);
	if (ftt_debug > 2){
		fprintf(stderr,"New totals");
		ftt_dump_stats(bp[TOTALS], stderr);
	}
	tmp = bp[LAST];
	bp[LAST] = new_cur;
	ftt_free_stat(tmp);
	ftt_free_stat(delta);
	return 0;
}

char *ftt_stat_names[] = {
 /* FTT_VENDOR_ID	0 */ "FTT_VENDOR_ID",
 /* FTT_PRODUCT_ID	1 */ "FTT_PRODUCT_ID",
 /* FTT_FIRMWARE	2 */ "FTT_FIRMWARE",
 /* FTT_SERIAL_NUM	3 */ "FTT_SERIAL_NUM",
 /* FTT_HOURS_ON	4 */ "FTT_HOURS_ON",
 /* FTT_CLEANING_BIT	5 */ "FTT_CLEANING_BIT",
 /* FTT_READ_COUNT	6 */ "FTT_READ_COUNT",
 /* FTT_WRITE_COUNT	7 */ "FTT_WRITE_COUNT",
 /* FTT_READ_ERRORS	8 */ "FTT_READ_ERRORS",
 /* FTT_WRITE_ERRORS	9 */ "FTT_WRITE_ERRORS",
 /* FTT_FTT_DENSITY	10 */ "FTT_FTT_DENSITY",
 /* FTT_READ_COMP	11 */ "FTT_READ_COMP",
 /* FTT_FILE_NUMBER	12 */ "FTT_FILE_NUMBER",
 /* FTT_BLOCK_NUMBER	13 */ "FTT_BLOCK_NUMBER",
 /* FTT_BOT		14 */ "FTT_BOT",
 /* FTT_READY		15 */ "FTT_READY",
 /* FTT_WRITE_PROT	16 */ "FTT_WRITE_PROT",
 /* FTT_FMK		17 */ "FTT_FMK",
 /* FTT_EOM		18 */ "FTT_EOM",
 /* FTT_PEOT		19 */ "FTT_PEOT",
 /* FTT_MEDIA_TYPE	20 */ "FTT_MEDIA_TYPE",
 /* FTT_BLOCK_SIZE	21 */ "FTT_BLOCK_SIZE",
 /* FTT_BLOCK_TOTAL	22 */ "FTT_BLOCK_TOTAL",
 /* FTT_TRANS_DENSITY	23 */ "FTT_TRANS_DENSITY",
 /* FTT_TRANS_COMPRESS	24 */ "FTT_TRANS_COMPRESS",
 /* FTT_REMAIN_TAPE	25 */ "FTT_REMAIN_TAPE",
 /* FTT_USER_READ	26 */ "FTT_USER_READ",
 /* FTT_USER_WRITE	27 */ "FTT_USER_WRITE",
 /* FTT_CONTROLLER	28 */ "FTT_CONTROLLER",
 /* FTT_DENSITY		29 */ "FTT_DENSITY",
 /* FTT_ILI		30 */ "FTT_ILI",
 /* FTT_SCSI_ASC	31 */ "FTT_SCSI_ASC",
 /* FTT_SCSI_ASCQ	32 */ "FTT_SCSI_ASCQ",
 /* FTT_PF		33 */ "FTT_PF",
 /* FTT_CLEANED_BIT     34 */ "FTT_CLEANED_BIT",
 /* FTT_WRITE_COMP	35 */ "FTT_WRITE_COMP",
 /* FTT_TRACK_RETRY	36 */ "FTT_TRACK_RETRY",
 /* FTT_UNDERRUN	37 */ "FTT_UNDERRUN",
 /* FTT_MOTION_HOURS	38 */ "FTT_MOTION_HOURS",
 /* FTT_POWER_HOURS	39 */ "FTT_POWER_HOURS",
 /* FTT_TUR_STATUS	40 */ "FTT_TUR_STATUS",
 /* FTT_BLOC_LOC	41 */ "FTT_BLOC_LOC",
 /* FTT_COUNT_ORIGIN	42 */ "FTT_COUNT_ORIGIN",
 /* FTT_N_READS		43 */ "FTT_N_READS",
 /* FTT_N_WRITES	44 */ "FTT_N_WRITES",
 /* FTT_TNP		45 */ "FTT_TNP",
 /* FTT_SENSE_KEY	46 */ "FTT_SENSE_KEY",
 /* FTT_TRANS_SENSE_KEY	47 */ "FTT_TRANS_SENSE_KEY",
 /* FTT_MAX_STAT	48 */ "FTT_MAX_STAT",
 0
};

int
ftt_dump_stats(ftt_stat_buf b, FILE *pf) {
	int i;

	ENTERING("ftt_dump_stats");
	CKNULL("statitics buffer pointer", b);
	CKNULL("stdio file handle", pf);

	for( i = 0 ; i < FTT_MAX_STAT; i++ ) {
		if(b->value[i] != 0) { 
			fprintf(pf, "%s is %s\n", 
				ftt_stat_names[i], b->value[i]);
		}
	}
	fprintf(pf, "- is -\n");
	return 0;
}

int
ftt_undump_stats(ftt_stat_buf b, FILE *pf) {
	int i;
	static char name[512], value[512];

	ENTERING("ftt_undump_stats");
	CKNULL("statitics buffer pointer", b);
	CKNULL("stdio file handle", pf);

	/* note that this only works 'cause we know what
	** order the entries were printed in by dump_stats.
	** therefore the next item on the input has to be
	** one of the upcoming entries in the table.
	** so we go through all the stats, and if the
	** line we have read in is that stat we set it
	** and get the next line.
	*/
	fscanf(pf, "%s is %[^\n]\n", name, value);
	for( i = 0 ; i < FTT_MAX_STAT; i++ ) {
	    if (0 != b->value[i]) {
		free(b->value[i]);
		b->value[i] = 0;
	    }
	    if (0 == strcmp(name,ftt_stat_names[i])) {
		b->value[i] = strdup(value);
		fscanf(pf, "%s is %s\n", name, value);
	    }
	}
	return 0;
}
