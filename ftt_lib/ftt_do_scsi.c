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
    scsi_handle n;
    int res;
    char *devname;

    ENTERING("ftt_do_scsi_command");
    CKNULL("ftt_descriptor", d);
    CKNULL("Operation Name", pcOp);
    CKNULL("SCSI CDB", pcCmd);

    if ( !iswrite && nRdWr ) {
	memset(pcRdWr,0,nRdWr);
    }
    ftt_close_dev(d);
    devname = ftt_get_scsi_devname(d);
    n = ftt_scsi_open(devname);
    if (n < 0) {
	return ftt_translate_error(d,FTT_OPN_PASSTHRU,"a SCSI passthru",
				    n,"an ftt_scsi_open",1);
    }
    res = ftt_scsi_command(n,pcOp, pcCmd, nCmd, pcRdWr, nRdWr, delay, iswrite);
    ftt_scsi_close(n);
    return res;
}

int
ftt_scsi_check(scsi_handle n,char *pcOp, int stat) {
    int res;
    static int recursive = 0;
    static char *errmsg =
	"while performing a scsi passthru %s command, I received a\n\
	 SCSI status of %d, so I did a mode sense which gave me the \n\
	 data: \n\
	 %02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x";
    static unsigned char acSensebuf[19];

    static unsigned char acSense[]={ 0x03, 0x00, 0x00, 0x00, 
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
	        res = ftt_scsi_command(n,"sense",acSense, sizeof(acSense),
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
