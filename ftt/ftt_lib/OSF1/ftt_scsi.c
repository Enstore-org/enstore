static char rcsid[] = "@(#)$Id$";
#include <stdio.h> 
#include <strings.h>
#include <ctype.h>
#include <ftt_private.h>

#include <sys/file.h>
#include <sys/types.h>
#include <sys/ioctl.h>
#include <sys/errno.h>
#include <sys/uio.h>
#include <sys/stat.h>

#include <io/common/iotypes.h>
#include <io/cam/cam.h>
#include <io/cam/dec_cam.h>
#include <io/cam/uagt.h>
#include <io/cam/scsi_all.h>

#define FD_MAX 24

typedef struct {
    int id; 
    int allocated;
    int targid; 
    int lun;
    int fd;
} osf_scsi_block, *osf_scsi_descriptor;

static osf_scsi_block open_devs[FD_MAX];

/*
** lookup_drive -- this is a drive lookup routine to 
** find the scsi bus, target id, and lun of a given device, so
** as we can deal with /dev/cam...
*/
static int
lookup_drive(char *pc, int *bus, int *targ, int *lun, char *idstring) {

        static char linebuf[512];
	struct stat sbuf;
	int dnum, scratch;
	FILE *pf;
	int which_entry = 0;
	int minor;
	int res;

	DEBUG2(stderr,"Converting drive %s\n", pc);

	stat(pc,&sbuf);
	/* minor = (int)minor(sbuf.st_rdev);		*/
	/* DEBUG2(stderr,"Minor dev is %d\n", minor) */
	*bus  = (sbuf.st_rdev >> 14 ) & 63;
	*targ = (sbuf.st_rdev >> 10 ) & 15;
	*lun  = (sbuf.st_rdev >>  6 ) & 15;
	DEBUG3(stderr,"Got bus %d, target %d, lun %d\n", *bus, *targ, *lun);
	idstring[0] = 0;
	return 1;
}

/*+ ftt_scsi_open
 *\subnam
 *	ftt_scsi_open
 *\subcall
 *	ftt_scsi_open(pcFile)
 *\subcalend
 *\subtxt
 *
 *
 *\arglist
 *\argn	pcFile	pointer to fnCmdame of the device
 *\arglend
-*/
scsi_handle
ftt_scsi_open(const char *pc) { 
    int slot, n, i;
    static char idbuf[512];

    DEBUG1(stderr, "Entering ftt_scsi_open\n");
    slot = -1;
    for( i = 1; i < FD_MAX ; i++) {
	if( open_devs[i].allocated == 0) {
	    DEBUG3(stderr, "Found slot %d\n", i);
	    slot = i;
	    break;
	}
    }
    if (slot == -1) {
	errno = ENOMEM;
	return -1;
    }
    if ( 0 == strncmp(pc, "/dev/cam/", 9) ) {

	DEBUG2(stderr,"trying /dev/cam/string parsing\n");
        /* /dev/cam/sc0l0d0 style pseudo-device name */

	n = sscanf(pc, "/dev/cam/sc%dd%dl%d",
		    &open_devs[slot].id, &open_devs[slot].targid,
		    &open_devs[slot].lun);
	if (n != 3) {
	    errno = EINVAL;
	    return -1;
	}

    } else {
        /* 
	** regular device; look up it's scsi id, etc.
	*/

	if (lookup_drive(pc,&open_devs[slot].id, &open_devs[slot].targid,
			    &open_devs[slot].lun, idbuf)) {
	    DEBUG2(stderr,"Got controller %d, id %d\n",
		    open_devs[slot].id, open_devs[slot].targid);
	} else {
	    errno = EINVAL;
	    return -1;
	}
    }
    if ((open_devs[slot].fd = open("/dev/cam", O_RDWR, 0)) < 0) {
	return -1;
    }
    open_devs[slot].allocated = 1;
    return (scsi_handle)slot;
}

/*+ ftt_scsi_close
 *\subnam
 *	ftt_scsi_close
 *\subcall
 *	ftt_scsi_close(n)
 *\subcalend
 *\subtxt
 *
 * use dslib to close a scsi port
 *
 *\arglist
 *\argn	n	File handle to close
 *\arglend
-*/
int 
ftt_scsi_close(scsi_handle n)
{
	open_devs[n].allocated = 0;
	return close(open_devs[(int)n].fd);
}

/*+ ftt_scsi_command
 *\subnam
 *	ftt_scsi_command
 *\subcall
 *	ftt_scsi_command(n, pcOp, pcCmd, nCmd, pcRdWr, nRdWr)
 *\subcallend
 *\subtxt
 *	send a scsi command and put the result in a buffer
 *	If we get an informational error response, we retry the command...
 *
 *\arglist
 *\argn n	File handle for scsi device
 *\argn pcOp	String describing operation for debugging
 *\argn pcCmd	Command frame buffer
 *\argn nCmd	number of bytes in command buffer
 *\argn pcRdWr	read/write buffer
 *\argn nRdWr   size of read/write buffer
 *\argn delay   seconds to allow for command to complete
 *\argn iswrite	flag to say if we write or read pcRdWr
 *\arglend
-*/
int
ftt_scsi_command(scsi_handle n, char *pcOp,unsigned char *pcCmd, int nCmd, unsigned char *pcRdWr, int nRdWr, int delay, int writeflag) {

    int res;
    static UAGT_CAM_CCB ua_ccb_sim_rel;
    static CCB_RELSIM ccb_sim_rel;
    static UAGT_CAM_CCB ua_ccb;
    static CCB_SCSIIO ccb;
    static int gotstatus = 0;
#   define SENSSIZ 64
    static char acSense[SENSSIZ];
    int successful, retries;

    if (gotstatus && pcCmd[0] == 0x03) {
	/* we already have mode sense data, so fake it */
	memcpy(pcRdWr, acSense, nRdWr);
	gotstatus = 0;
	return 0;
    }

    /* setup SIMQ_RELEASE request */
    ccb_sim_rel.cam_ch.my_addr = (struct ccb_header*) &ccb_sim_rel;
    ccb_sim_rel.cam_ch.cam_ccb_len = sizeof(CCB_RELSIM);
    ccb_sim_rel.cam_ch.cam_func_code = XPT_REL_SIMQ;
    ccb_sim_rel.cam_ch.cam_path_id    = open_devs[(int)n].id;
    ccb_sim_rel.cam_ch.cam_target_id  = open_devs[(int)n].targid;
    ccb_sim_rel.cam_ch.cam_target_lun = open_devs[(int)n].lun;
    ccb_sim_rel.cam_ch.cam_flags =  CAM_DIR_NONE;
    ua_ccb_sim_rel.uagt_ccb = (CCB_HEADER *)&ccb_sim_rel;
    ua_ccb_sim_rel.uagt_ccblen = sizeof(CCB_RELSIM);
    ua_ccb_sim_rel.uagt_buffer = 0;
    ua_ccb_sim_rel.uagt_buflen = 0;
    ua_ccb_sim_rel.uagt_snsbuf = 0;
    ua_ccb_sim_rel.uagt_snslen = 0;
    ua_ccb_sim_rel.uagt_cdb = 0;
    ua_ccb_sim_rel.uagt_cdblen = 0;

    /* setup actual SCSI_IO request */
    ccb.cam_ch.my_addr = (struct ccb_header *)&ccb;
    ccb.cam_ch.cam_ccb_len = sizeof(ccb);
    ccb.cam_ch.cam_func_code = XPT_SCSI_IO;
    ccb.cam_ch.cam_path_id    = open_devs[(int)n].id;
    ccb.cam_ch.cam_target_id  = open_devs[(int)n].targid;
    ccb.cam_ch.cam_target_lun = open_devs[(int)n].lun;
    ccb.cam_ch.cam_flags = nRdWr? (writeflag ? CAM_DIR_OUT : CAM_DIR_IN) : 
				  CAM_DIR_NONE;

    ccb.cam_data_ptr = pcRdWr;
    ccb.cam_dxfer_len = nRdWr;
    /* ccb.cam_timeout = delay <= 5 ? CAM_TIME_DEFAULT: CAM_TIME_INFINITY; */
#if 0
    ccb.cam_timeout = delay;
#else
    ccb.cam_timeout = CAM_TIME_INFINITY;
#endif
    ccb.cam_cdb_len = nCmd;
    ccb.cam_sense_ptr = (u_char *)acSense;
    ccb.cam_sense_len = SENSSIZ;

    memcpy(ccb.cam_cdb_io.cam_cdb_bytes, pcCmd, nCmd);

    ua_ccb.uagt_ccb = (CCB_HEADER*)&ccb;
    ua_ccb.uagt_ccblen = sizeof(CCB_SCSIIO);
    ua_ccb.uagt_buffer = pcRdWr;
    ua_ccb.uagt_buflen = nRdWr;
    ua_ccb.uagt_snsbuf = (u_char *)acSense;
    ua_ccb.uagt_snslen = SENSSIZ;
    ua_ccb.uagt_cdb = (CDB_UN *)NULL;
    ua_ccb.uagt_cdblen = 0;

    DEBUG2(stderr,"sending scsi frame:\n");
    DEBUGDUMP2(pcCmd,nCmd);
	if (writeflag && pcRdWr != 0 && nRdWr != 0){
		DEBUG4(stderr,"Read/Write buffer:\n");
		DEBUGDUMP4(pcRdWr,nRdWr);
	}
		
    successful = 0;
    retries = 0;
    do {
        DEBUG3(stderr, "try %d\n", retries); 
        res = ioctl(open_devs[(int)n].fd, UAGT_CAM_IO, &ua_ccb);
	if (res < 0) {
	    DEBUG1(stderr, "UAGT_CAM_IO ioctl failed\n");
	    return res;
	}
	if ((ccb.cam_ch.cam_status & CAM_STATUS_MASK) != CAM_REQ_CMP &&
	    (ccb.cam_ch.cam_status & CAM_STATUS_MASK) != CAM_REQ_CMP_ERR ) {
	    DEBUG2(stderr, "cam_status is not completed status=%x\n",
                    ccb.cam_ch.cam_status);
	    /* try to unfreeze the queue */
	    if (ccb.cam_ch.cam_status & CAM_SIM_QFRZN) {
	        DEBUG3(stderr, "unfreezing queue!\n");
		res = ioctl(open_devs[(int)n].fd,UAGT_CAM_IO, &ua_ccb_sim_rel);
		if (res < 0) {
	            DEBUG3(stderr, "unfreeze failed!\n");
		    return res;
		}
	    }
            if ((ccb.cam_ch.cam_status & CAM_STATUS_MASK) == CAM_BUSY){
		fflush(stderr);
		sleep(5);
            }
	} else {
	    successful = 1;
	}
        if (ftt_debug) fflush(stderr);
    } while (!successful && ++retries < 10);

    gotstatus = 0 != (ccb.cam_ch.cam_status & CAM_AUTOSNS_VALID);

    if (ccb.cam_ch.cam_status & CAM_SIM_QFRZN) {
	DEBUG3(stderr, "unfreezing queue!\n");
	res = ioctl(open_devs[(int)n].fd,UAGT_CAM_IO, &ua_ccb_sim_rel);
	if (res < 0) {
	    DEBUG3(stderr, "unfreeze failed!\n");
	    return res;
	}
    }

    if (! successful && ccb.cam_scsi_status == 0 ) {
	ccb.cam_scsi_status = 255; /* bogus system error status number */
    }
    res = ftt_scsi_check(n,pcOp,ccb.cam_scsi_status,nRdWr);

	if (!writeflag && res > 0 && pcRdWr != 0 && nRdWr != 0){
		DEBUG4(stderr,"Read/Write buffer:\n");
		DEBUGDUMP4(pcRdWr,res);
	}

    return res;
}
