#include <stdio.h>
#include <string.h>
#include <ftt_private.h>

void 
ftt_set_transfer_length( unsigned char *cdb, int n ) {
	cdb[2]= n >> 16 & 0xff;
	cdb[3]= n >> 8 & 0xff;
	cdb[4]= n & 0xff;
}

int
ftt_do_scsi_command(ftt_descriptor d,char *pcOp,unsigned char *pcCmd, 
	int nCmd, unsigned char *pcRdWr, int nRdWr, int delay, int iswrite){
    int res;

    ENTERING("ftt_do_scsi_command");
    CKNULL("ftt_descriptor", d);
    CKNULL("Operation Name", pcOp);
    CKNULL("SCSI CDB", pcCmd);

    res = ftt_open_scsi_dev(d);  if (res < 0) return res;
    if ( !iswrite && nRdWr ) {
	memset(pcRdWr,0,nRdWr);
    }
    res = ftt_scsi_command(d->scsi_descriptor,pcOp, pcCmd, nCmd, pcRdWr, nRdWr, delay, iswrite);
    return res;
}

int
ftt_open_scsi_dev(ftt_descriptor d) {
    char *devname;

    /* can't have regular device and passthru open at same time */
    ftt_close_dev(d);

    if (d->scsi_descriptor < 0) {
        devname = ftt_get_scsi_devname(d);
	d->scsi_descriptor = ftt_scsi_open(devname);
	if (d->scsi_descriptor < 0) {
	    return ftt_translate_error(d,FTT_OPN_OPEN,"a SCSI open",
				d->scsi_descriptor,"ftt_scsi_open",1);
	}
    }
    return d->scsi_descriptor;
}

int
ftt_close_scsi_dev(ftt_descriptor d) {
    int res;

    DEBUG3(stderr,"Entering close_scsi_dev\n");
    if(d->scsi_descriptor > 0) {
        res = ftt_scsi_close(d->scsi_descriptor);
	d->scsi_descriptor = -1;
	return res;
    }
    return 0;
}

int
ftt_scsi_check(scsi_handle n,char *pcOp, int stat) {
    int res;
    static int recursive = 0;
    static char *errmsg =
	"ftt_scsi_command: %s command returned  a %d, \n\
	request sense data: \n\
	 %02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x";
    static unsigned char acSensebuf[19];

    static unsigned char acReqSense[]={ 0x03, 0x00, 0x00, 0x00, 
				     sizeof(acSensebuf), 0x00 };

    DEBUG2(stderr, "ftt_scsi_check called with status %d\n", stat);

    if (0 != n) {
	switch(stat){
	case 0x00:
	    ftt_errno = FTT_SUCCESS;
	    break;
	case 0x04:
	    ftt_errno = FTT_EBUSY;
	    break;
	case 0x02:
            if (!recursive) {
	        recursive = 1; /* keep from recursing if sense fails */
	        res = ftt_scsi_command(n,"sense",acReqSense, sizeof(acReqSense),
	  		               acSensebuf, sizeof(acSensebuf),5,0);
	        recursive = 0;
	    } else {
		res = -1;
	    }
	    if (res < 0) {
		 ftt_errno = FTT_EUNRECOVERED;
		 break;
	    }
	    ftt_eprintf(errmsg, pcOp, stat,
		    acSensebuf[0], acSensebuf[1],
		    acSensebuf[2], acSensebuf[3],
		    acSensebuf[4], acSensebuf[5],
		    acSensebuf[6], acSensebuf[7],
		    acSensebuf[8], acSensebuf[9],
		    acSensebuf[10], acSensebuf[12],
		    acSensebuf[13], acSensebuf[14],
		    acSensebuf[15]);
	    switch(acSensebuf[2]& 0xf) {
	    case 0x0:
		    ftt_errno =  FTT_SUCCESS;
		    return 0;
	    case 0x1:
		    ftt_errno = FTT_EIO;
		    break;
	    case 0x2:
		    ftt_errno = FTT_ENOTAPE;
		    break;
	    case 0x3:
	    case 0x4:
		    ftt_errno = FTT_EIO;
		    break;
	    case 0x5:
	    case 0x6:
		    ftt_errno = FTT_EUNRECOVERED;
		    break;
	    case 0x7:
		    ftt_errno = FTT_EROFS;
		    break;
	    case 0x8:
		    ftt_errno = FTT_EBLANK;
		    break;
	    }
	}
    } 
    return -stat;
}

char *
ftt_get_scsi_devname(ftt_descriptor d){
    int j;

    ENTERING("ftt_get_scsi_devname");
    PCKNULL("ftt_descriptor", d);

    for( j = 0; d->devinfo[j].device_name != 0 ; j++ ){
	if( d->devinfo[j].passthru ){
	    return  d->devinfo[j].device_name;
	}
    }
    return 0;
}
