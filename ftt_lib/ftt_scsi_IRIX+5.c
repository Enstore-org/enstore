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
ftt_scsi_open(char *pc)
{
	register struct dsreq *dp;

	DEBUG2(stderr,"entering ftt_scsi_open(%s,..)\n",pc);
	dp = dsopen(pc, O_RDWR);
	if (0 != dp) {
	    DEBUG2(stderr,"scsi open succeeded");
	    return (scsi_handle) dp;
	} else {
	    DEBUG2(stderr,"scsi open failed");
	    return (scsi_handle) -1;
	}
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
	
	register struct dsreq *dp = (struct dsreq *)n;

	dsclose(dp);
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
	register struct dsreq *dp = (struct dsreq *)n;
	int scsistat, res;
	static int gotstatus;
	
	if (gotstatus && pcCmd[0] == 0x03) {
		/* we already have mode sense data, so fake it */
		memcpy(pcRdWr, 
		      ((struct context *) dp->ds_private)->dsc_sense, nRdWr);
		return 0;
		gotstatus = 0;
	}
	dp->ds_cmdlen=nCmd;
	dp->ds_cmdbuf=pcCmd;
	filldsreq(dp, (unsigned char *)pcRdWr, nRdWr, 
		(writeflag?DSRQ_WRITE:DSRQ_READ)|DSRQ_SENSE);
	dp->ds_time = delay * 1000;	/* allow delay seconds */
	DEBUG2(stderr,"sending scsi frame:\n");
	DEBUGDUMP2(pcCmd,nCmd);
	scsistat = doscsireq(getfd(dp),dp);
	if (0 != scsistat) {
		gotstatus = 1;
	}
	res = ftt_scsi_check(n,pcOp,scsistat);

	if (pcRdWr != 0 && nRdWr != 0){
		DEBUG2(stderr,"Read/Write buffer:\n");
		DEBUGDUMP2(pcRdWr,nRdWr);
	}
	return res;
}
