static char rcsid[] = "@(#)$Id$";

#include <stdio.h>
#include <string.h>
#include <ftt_private.h>

#ifdef WIN32
#include <io.h>
#include <process.h>
#define geteuid() -1
#else
#include <unistd.h>
#endif

int ftt_close_scsi_dev(ftt_descriptor d) ;
int ftt_close_io_dev(ftt_descriptor d);
int ftt_get_stat_ops(char *name) ;
int ftt_describe_error();

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
    /* UNLESS the device we have default is also passthru... */

    if (!d->devinfo[d->which_is_default].passthru) {
	ftt_close_io_dev(d);

	if (d->scsi_descriptor < 0) {
	    devname = ftt_get_scsi_devname(d);
	    d->scsi_descriptor = ftt_scsi_open(devname);
	    if (d->scsi_descriptor < 0) {
		return ftt_translate_error(d,FTT_OPN_OPEN,"a SCSI open",
				    d->scsi_descriptor,"ftt_scsi_open",1);
	    }
	}
    } else {
       ftt_open_dev(d);
       d->scsi_descriptor = d->file_descriptor;
    }
    return d->scsi_descriptor;
}

int
ftt_close_scsi_dev(ftt_descriptor d) {
    int res;
    extern int errno;

    DEBUG3(stderr,"Entering close_scsi_dev\n");
    /* check if we're using the regular device */
    if(d->scsi_descriptor == d->file_descriptor) {
	d->scsi_descriptor = -1;
    }
    if(d->scsi_descriptor > 0 ) {
	DEBUG1(stderr,"Actually closing scsi device\n");
        res = ftt_scsi_close(d->scsi_descriptor);
	DEBUG2(stderr,"close returned %d, errno %d\n", res, errno);
	d->scsi_descriptor = -1;
	return res;
    }
    return 0;
}

int
ftt_scsi_check(scsi_handle n,char *pcOp, int stat, int len) {
    int res;
    static int recursive = 0;
    static char *errmsg =
	"ftt_scsi_command: %s command returned  a %d, \n\
request sense data: \n\
%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x\n";
    static unsigned char acSensebuf[19];

    static unsigned char acReqSense[]={ 0x03, 0x00, 0x00, 0x00, 
				     sizeof(acSensebuf), 0x00 };

    DEBUG2(stderr, "ftt_scsi_check called with status %d len %d\n", stat, len);

    if (0 != n) {
	switch(stat) {
	default:
	    ftt_errno = FTT_ENXIO;
	    ftt_eprintf("While attempting SCSI passthrough, we encountered an \n\
unrecoverable system error");
	    break;
	case 0x00:
	    ftt_errno = FTT_SUCCESS;
	    break;
	case 0x04:
	    ftt_errno = FTT_EBUSY;
	    ftt_eprintf("While attempting SCSI passthrough, we encountered a \n\
device which was not ready");
	    break;
	case 0x02:
            if (!recursive) {
	        recursive = 1; /* keep from recursing if sense fails */
	        res = ftt_scsi_command(n,"sense",acReqSense, sizeof(acReqSense),
	  		               acSensebuf, sizeof(acSensebuf),5,0);
		DEBUG3(stderr,"request sense returns res %d\n", res);
	        recursive = 0;
	    } else {
		return 0;
	    }
	    DEBUG3(stderr, errmsg, pcOp, stat,
		    acSensebuf[0], acSensebuf[1],
		    acSensebuf[2], acSensebuf[3],
		    acSensebuf[4], acSensebuf[5],
		    acSensebuf[6], acSensebuf[7],
		    acSensebuf[8], acSensebuf[9],
		    acSensebuf[10], acSensebuf[12],
		    acSensebuf[13], acSensebuf[14],
		    acSensebuf[15]);
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
	    default:
	    case 0x0:
		    if ( (acSensebuf[2]&0x20) && (acSensebuf[0]&0x80) ) {
			/* we have a valid, incorrect length indication */
			len -=  (acSensebuf[3] << 24) + 
				(acSensebuf[4] << 16) + 
				(acSensebuf[5] <<  8) +
				acSensebuf[6];
		        ftt_errno =  FTT_SUCCESS;
			/* XXX -- does this work in block mode? */
		    } else if ((acSensebuf[2]&0x80) && (acSensebuf[0]&0x80)){
			/* we read a filemark */
			len = 0;
		        ftt_errno =  FTT_SUCCESS;
		    } else if ((acSensebuf[2]&0x40) && (acSensebuf[0]&0x80)){
			/* we hit end of tape */
		        ftt_errno =  FTT_ENOSPC;
		    } else {
		        ftt_errno =  FTT_SUCCESS;
		    }
		    break;
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
		    ftt_errno = FTT_ENOTSUPPORTED;
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
    if (ftt_errno == FTT_SUCCESS) {
	return len;
    } else {
        return -stat;
    }
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

/* 
** force us to use scsi pass-through ops to do everything
*/
int
ftt_all_scsi(ftt_descriptor d) {
    ENTERING("ftt_all_scsi");
    PCKNULL("ftt_descriptor", d);

    if ((d->flags & FTT_FLAG_SUID_SCSI) && geteuid() != 0) {
	ftt_eprintf("ftt_all_scsi: Must be root on this platform to do scsi pass through!");
	ftt_errno = FTT_EPERM;
	return -1;
    }


    d->scsi_ops = 0xffffffff;
    return 0;
}

int
ftt_scsi_set_compression(ftt_descriptor d, int compression) {

    static unsigned char 
	mod_sen10[6] = { 0x1a, 0x00, 0x10, 0x00, 28, 0x00},
	mod_sel10[6] = { 0x15, 0x10, 0x00, 0x00, 28, 0x00},
	mod_sen0f[6] = { 0x1a, 0x00, 0x0f, 0x00, 28, 0x00},
	mod_sel0f[6] = { 0x15, 0x0f, 0x00, 0x00, 28, 0x00},
	buf [28];
    int res = 0;

    ENTERING("ftt_set_compression");
    CKNULL("ftt_descriptor", d);
    DEBUG4(stderr, "Entering: ftt_set_compression\n");

    if ((d->flags&FTT_FLAG_SUID_SCSI) == 0 || 0 == geteuid()) {
	if (ftt_get_stat_ops(d->prod_id) & FTT_DO_MS_Px0f) {
	    DEBUG2(stderr, "Using SCSI Mode sense 0x0f page to set compression\n");
	    res = ftt_open_scsi_dev(d);        
	    if(res < 0) return res;
	    res = ftt_do_scsi_command(d, "Mode sense", mod_sen0f, 6, buf, 28, 5, 0);
	    if(res < 0) return res;
	    buf[0] = 0;
	    /* enable outgoing compression */
	    buf[4 + 8 +  2] &= ~(1 << 7);
	    buf[4 + 8 +  2] |= (compression << 7);

	    res = ftt_do_scsi_command(d, "Mode Select", mod_sel0f, 6, buf, 28, 5, 1);
	    if(res < 0) return res;
	    res = ftt_close_scsi_dev(d);
	    if(res < 0) return res;
	}
	if (ftt_get_stat_ops(d->prod_id) & FTT_DO_MS_Px10) {
	    DEBUG2(stderr, "Using SCSI Mode sense 0x10 page to set compression\n");
	    res = ftt_open_scsi_dev(d);        
	    if(res < 0) return res;
	    res = ftt_do_scsi_command(d, "Mode sense", mod_sen10, 6, buf, 28, 5, 0);
	    if(res < 0) return res;
	    buf[0] = 0;
	    /* we shouldn't be changing density here but it shouldn't hurt */
	    /* yes it will! the setuid program doesn't know which density */
	    /* the parent process set... */
	    /* buf[4] = d->devinfo[d->which_is_default].hwdens; */
 	    buf[4 + 8 + 14] = compression;
	    res = ftt_do_scsi_command(d, "Mode Select", mod_sel10, 6, buf, 28, 5, 1);
	    if(res < 0) return res;
	    res = ftt_close_scsi_dev(d);
	    if(res < 0) return res;
	}
    } else {
        ftt_close_dev(d);
        ftt_close_scsi_dev(d);
	switch(ftt_fork(d)){

	static char s1[10];

	case -1:
		return -1;

	case 0:  /* child */
		fflush(stdout);	/* make async_pf stdout */
		close(1);
		dup2(fileno(d->async_pf_parent),1);
		sprintf(s1, "%d", compression);
		if (ftt_debug) {
		 execlp("ftt_suid", "ftt_suid", "-x", "-C", s1, d->basename, 0);
		} else {
		 execlp("ftt_suid", "ftt_suid", "-C", s1, d->basename, 0);
		}
		ftt_eprintf("ftt_set_compression: exec of ftt_suid failed");
		ftt_errno=FTT_ENOEXEC;
		ftt_report(d);

	default: /* parent */
		res = ftt_wait(d);
	}
    }
    return res;
}

int
ftt_scsi_locate( ftt_descriptor d, int blockno) {

    int res = 0;
    static unsigned char 
        locate_cmd[10] = {0x2b,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00};
     
    locate_cmd[3] = (blockno >> 24) & 0xff;
    locate_cmd[4] = (blockno >> 16) & 0xff;
    locate_cmd[5] = (blockno >> 8)  & 0xff; 
    locate_cmd[6] = blockno & 0xff;
    res = ftt_do_scsi_command(d,"Locate",locate_cmd,10,NULL,0,60,0);
    res = ftt_describe_error(d,0,"a SCSI pass-through call", res,"Locate", 0);

    return res;
}
