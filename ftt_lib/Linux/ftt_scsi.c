static char rcsid[] = "@(#)$Id$";
/*
 ****************************Copyright Notice***********************************
 *             Copyright (c)1992 Universities Research Association, Inc.       *
 *                          and Marc W. Mengel                                 *
 *                         All Rights Reserved                                 *
 *******************************************************************************
 ***********************Government Sponsorship Notice***************************
 * This material resulted from work developed under a Government Contract and  *
 * is subject to the following license:  The Government retains a paid-up,     *
 * nonexclusive, irrevocable worldwide license to reproduce, prepare derivative*
 * works, perform publicly and display publicly by or for the Government,      *
 * including the right to distribute to other Government contractors.  Neither *
 * the United States nor the United States Department of Energy, nor any of    *
 * their employees, makes any warrenty, express or implied, or assumes any     *
 * legal liability or responsibility for the accuracy, completeness, or        *
 * usefulness of any information, apparatus, product, or process disclosed, or *
 * represents that its use would not infringe privately owned rights.          *
 *******************************************************************************
 */
#include <stdio.h>
#include <fcntl.h>
#include <dslib.h>
#include <string.h>
#include "ftt_private.h"

extern char ftt_acSensebuf[18];

/*+ ftt_scsi_open
 *\subnam
 *	ftt_scsi_open
 *\subcall
 *	ftt_scsi_open(pcFile)
 *\subcalend
 *\subtxt
 *
 *	use dslib(3) to open a scsi device port for use
 *	return the dslib filehandle converted to an integer
 *
 *\arglist
 *\argn	pcFile	pointer to fnCmdame of the device
 *\arglend
-*/
scsi_handle
ftt_scsi_open(const char *pc)
{
	register int fd;

	fd = open(pc, O_RDWR); 
	return fd;
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
ftt_scsi_close(scsi_handle n) {
	
	close(n);
	return 0;
}

/*+ ftt_scsi_command
 *\subnam
 *	ftt_scsi_command
 *\subcall
 *	ftt_scsi_command(n, buf, len)
 *\subcallend
 *\subtxt
 *	use dslib to send a scsi command and put the result in a buffer
 *	If we get an informational error response, we retry the command...
 *
 *\arglist
 *\argn n	File handle for scsi device
 *\argn	pcCmd	buffer containing command to send
 *\argn	nCmd	length of command buffer 
 *\argn	pcRdWr	buffer to for command results or write data
 *\argn	nRdWr	length of buffer 
 *\argn writeflag	Set to indicate write of RdWr buffer rather than read
 *\arglend
-*/
int
ftt_scsi_command(scsi_handle n, char *pcOp,unsigned char *pcCmd, int nCmd, unsigned char *pcRdWr, int nRdWr, int delay, int writeflag)
{
	int scsistat, res;
	static char buffer[1024];
	struct sg_header *sg_hd = &buffer;
	static int gotstatus, len;
	
	if (gotstatus && pcCmd[0] == 0x03) {
		/* we already have mode sense data, so fake it */
		memcpy(pcRdWr, 
		      sg_hd.sense , nRdWr);
		gotstatus = 0;
		return ftt_scsi_check(n,pcOp, 0, nRdWr);
	}
	sg_hd->reply_len = SCSI_OFF + writeflag ? nRdWr : 0;
	sg_hd->twelve_byte = nCmd;
	sg_hd->result = 0;
	len = SCSI_OFF + nCmd;
	memcpy( pcCmd, buffer+SCSI_OFF, nCmd );
	if (writeflag) {
	    memcpy(pcRdWr, buffer+SCSI_OFF+nCmd, nRdWr);
	    len += nRdWr;
	}
	res = write(n, buffer, len);
	if (res < 0) {
	    scsistat = 255;
	} else {
		len = SCSI_OFF + writeflag ? 0 : nRdWr;
		res = read(n, buffer, len);
		if (res < 0) {
		    scsistat = 255;
		} else {

			if ( !writeflag ) {
			    memcpy(buffer+SCSI_OFF, pcRdWr, nRdWr);
			}

			scsistat = hd->result;
			if (0 != scsistat) {
				gotstatus = 1;
			}
		}
	}
DEBUG3(stderr,"cmdsent %d datasent %d sensesent %d status %d ret %d msg %d\n",
	CMDSENT(dp), DATASENT(dp), SENSESENT(dp), STATUS(dp), 
	RET(dp), MSG(dp));

	res = ftt_scsi_check(n,pcOp,scsistat,nRdWr);

	if (pcRdWr != 0 && nRdWr != 0){
		DEBUG4(stderr,"Read/Write buffer:\n");
		DEBUGDUMP4(pcRdWr,nRdWr);
	}
	return res;
}
