#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "ftt_private.h"

int
ftt_verify_vol_label(ftt_descriptor d, int type, char *vollabel, 
			int timeout, int rdonly) {
    char buf[65536];
    char label_buf[512];
    int res;
    char *pname;
    int len;

    ENTERING("ftt_verify_vol_label");
    CKNULL("ftt_descriptor", d);
    CKNULL("volume label", vollabel);

    res = ftt_status(d,timeout);	if (res < 0) return res;
    if (0 != (res & FTT_PROT) && rdonly == FTT_RDWR) {
	ftt_eprintf("ftt_verify_vol_label found unexpected write protection\n");
	ftt_errno = FTT_EROFS;
	return -1;
    }
    if (0 == (res & FTT_PROT) && rdonly == FTT_RDONLY) {
	ftt_eprintf("ftt_verify_vol_label did not find expected write protection\n");
	ftt_errno = FTT_ERWFS;
	return -1;
    }
    res = ftt_rewind(d);  			if (res < 0) return res;
    res = ftt_read(d,buf,65536); 		/* errors to guess_label */
    res = ftt_guess_label(buf,res,&pname, &len);if(res < 0) return res;
    if (type == res && 0 == strncmp(vollabel,pname,len) && len ==
    		strlen(vollabel)) {
	return 0;
    }
    if (len > 512) len = 511;
    strncpy(label_buf,pname,len);
    label_buf[len] = 0;
    ftt_eprintf("ftt_verify_vol_label expected type %d, vollabel %s, but\n\
	got type %d, vollabel %s.", type, vollabel, res, label_buf);
    if (type == res) {
        ftt_errno = FTT_EWRONGVOL;
    } else {
        ftt_errno = FTT_EWRONGVOLTYP;
    }
    return -1;
}


int
ftt_write_vol_label(ftt_descriptor d, int type, char *vollabel){
    int res;
    char buf[65536];

    CKOK(d,"ftt_write_vol_label",1,1);
    CKNULL("ftt_descriptor", d);
    CKNULL("volume label", vollabel);

    res = ftt_rewind(d);			if (res <  0) return res;
    res = ftt_format_label(buf,65536,vollabel, strlen(vollabel), type);
						if (res <  0) return res;
    res = ftt_write(d,buf,res);			if (res <  0) return res;
    ftt_close_dev(d);
    ftt_skip_fm(d,1);
}

int
ftt_describe_dev(ftt_descriptor d, char *dev, FILE *pf) {
    int i;
    int found;
    char *starter;

    ENTERING("ftt_describe_dev");
    CKNULL("ftt_descriptor", d);
    CKNULL("device name", dev);
    CKNULL("stdio file handle", pf);

    found = 0;
    starter = dev;
    for(i = 0; d->devinfo[i].device_name !=0; i++) {
	if( 0 == strcmp(d->devinfo[i].device_name, dev)) {
	    fprintf(pf, "%s supports density %d, mode %d, rewindflags %d\n",
			starter,
			d->devinfo[i].density, 
			d->devinfo[i].mode,
			d->devinfo[i].rewind);
	    starter = "and also";
	    found = 1;
	}
    }
    if (found == 0) {
	ftt_errno = FTT_ENOENT;
	ftt_eprintf("ftt_describe_dev was given a device name not associated with the device.");
	return -1;
    }
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
	    ftt_eprintf("ftt_init_stats unable to allocate memory");
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
 /* FTT_UNDERRUN	37 */ "FTT_UDERRUN",
 /* FTT_MOTION_HOURS	38 */ "FTT_MOTION_HOURS",
 /* FTT_POWER_HOURS	39 */ "FTT_POWER_HOURS",
 /* FTT_TUR_STATUS	40 */ "FTT_TUR_STATUS",
 /* FTT_BLOC_LOC	41 */ "FTT_BLOC_LOC",
 /* FTT_COUNT_ORIGIN	42 */ "FTT_COUNT_ORIGIN",
 /* FTT_N_READS		43 */ "FTT_N_READS",
 /* FTT_N_WRITES	44 */ "FTT_N_WRITES",
 /* FTT_MAX_STAT	45 */ "FTT_MAX_STAT",
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
	return 0;
}

int
ftt_undump_stats(ftt_stat_buf b, FILE *pf) {
	int i;
	static char name[512], value[512];

	ENTERING("ftt_undump_stats");
	CKNULL("statitics buffer pointer", b);
	CKNULL("stdio file handle", pf);

	fscanf(pf, "%s is %s\n", name, value);
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
