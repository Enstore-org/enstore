#include <stdio.h> 
#include <strings.h>
#include <ctype.h>
#include <ftt_private.h>

#include <sys/file.h>
#include <sys/types.h>
#include <sys/ioctl.h>
#include <sys/errno.h>

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
    static char cmdbuf[512];
    FILE *pf;

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

	n = fscanf(pf, "/dev/cam/sc%dd%dl%d",
		    &open_devs[slot].id, &open_devs[slot].targid,
		    &open_devs[slot].lun);
	if (n != 3) {
	    errno = EINVAL;
	    return -1;
	}

    } else {

        /* 
	** regular device; look up it's scsi id, etc  with "file" 
	**
	** entries look like:
	** /dev/rmt0a:	character special (9/3078) SCSI #0 TZK08 tape #24 (SCSI ID #3) offline 
	*/

	DEBUG2(stderr,"Converting drive %s\n", pc);

	open_devs[slot].lun = 0;
	sprintf(cmdbuf, "file %s", pc);
	pf = popen(cmdbuf,"r");
	n = fscanf(pf, "%*[^S]SCSI #%d %*[^S]SCSI ID #%d", 
		    &open_devs[slot].id, &open_devs[slot].targid);
	pclose(pf);
	DEBUG2(stderr,"Got controller %d, id %d\n",
		    open_devs[slot].id, open_devs[slot].targid);
	if (n != 2) {
	    errno = EINVAL;
	    return -1;
	}
    }
    if ((open_devs[slot].fd = open("/dev/cam", O_RDWR, 0)) < 0) {
	return -1;
    }
    open_devs[i].allocated = 1;
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
ftt_scsi_command(scsi_handle n, char *pcOp,unsigned char *pcCmd, int nCmd, unsigned char *pcRdWr, int nRdWr, int delay, int iswrite){

    int res;
    static UAGT_CAM_CCB ua_ccb;
    static CCB_SCSIIO ccb;

    ccb.cam_ch.my_addr = (struct ccb_header *)&ccb;
    ccb.cam_ch.cam_ccb_len = sizeof(ccb);
    ccb.cam_ch.cam_func_code = XPT_SCSI_IO;
    ccb.cam_ch.cam_path_id    = open_devs[(int)n].id;
    ccb.cam_ch.cam_target_id  = open_devs[(int)n].targid;
    ccb.cam_ch.cam_target_lun = open_devs[(int)n].lun;
    ccb.cam_ch.cam_flags = CAM_DIS_AUTOSENSE | 
			   (iswrite ? CAM_DIR_OUT : CAM_DIR_IN);

    ccb.cam_data_ptr = pcRdWr;
    ccb.cam_dxfer_len = nRdWr;
    ccb.cam_timeout = delay;
    ccb.cam_cdb_len = nCmd;
    memcpy(ccb.cam_cdb_io.cam_cdb_bytes, pcCmd, nCmd);

    ua_ccb.uagt_ccb = (CCB_HEADER*)&ccb;
    ua_ccb.uagt_ccblen = sizeof(CCB_SCSIIO);
    ua_ccb.uagt_buffer = pcRdWr;
    ua_ccb.uagt_buflen = nRdWr;
    ua_ccb.uagt_snsbuf = (u_char *)NULL;
    ua_ccb.uagt_snslen = 0;
    ua_ccb.uagt_cdb = (CDB_UN *)NULL;
    ua_ccb.uagt_cdblen = 0;

    DEBUG2(stderr,"sending scsi frame:\n");
    DEBUGDUMP2(pcCmd,nCmd);
		
    res = ioctl(open_devs[(int)n].fd, UAGT_CAM_IO, &ua_ccb);
    if (res < 0) {
	return res;
    }
    res = ftt_scsi_check(n,pcOp,ccb.cam_scsi_status);

    if (pcRdWr != 0 && nRdWr != 0){
	DEBUG2(stderr,"Read/Write buffer:\n");
	DEBUGDUMP2(pcRdWr,nRdWr);
    }

    return res;
}
