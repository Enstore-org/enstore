
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
#include <string.h>
#include "ftt_private.h"
#include <sys/buf.h>
#include <sys/device.h>
#include <sys/devinfo.h>
#include <sys/ioctl.h>
#include <sys/scsi.h>
#include <sys/tape.h>
#include <sys/errno.h>

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
ftt_scsi_open(char *pc) { 
	int n;
	DEBUG2(stderr,"entering ftt_scsi_open(%s,..)\n",pc);
	n = openx(pc, O_RDWR, 0,SC_DIAGNOSTIC);
	return (scsi_handle)n;
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
	
	return close((int)n);
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
ftt_scsi_command(scsi_handle n, char *pcOp,unsigned char *pcCmd, int nCmd, unsigned char *pcRdWr, int nRdWr, int delay, int iswrite)
{
	int scsistat, res;
	static struct sc_iocmd scBuf;
	
	scBuf.data_length=nRdWr;
	scBuf.buffer = pcRdWr;
	scBuf.timeout_value = delay;
	scBuf.command_length = nCmd;
	scBuf.flags = nRdWr > 0 ? (iswrite ? B_WRITE : B_READ) : 0;
	bcopy(pcCmd, scBuf.scsi_cdb, nCmd);
	
	DEBUG2(stderr,"sending scsi frame:\n");
	DEBUGDUMP2(pcCmd,nCmd);
	scsistat = ioctl((int)n, STIOCMD,&scBuf);
	if (-1 == scsistat && errno != EIO) {
		res = 255;
	} else {
		scsistat = scBuf.scsi_bus_status;
		res = ftt_scsi_check(n,pcOp,scsistat);
	}
	if (pcRdWr != 0 && nRdWr != 0){
		DEBUG2(stderr,"got back:\n");
		DEBUGDUMP2(pcRdWr,nRdWr);
	}
	return res;
}
