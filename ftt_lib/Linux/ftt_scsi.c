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
#include <scsi/sg.h>
#include <string.h>
#include "ftt_private.h"
#include <assert.h>		/* assert */

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
ftt_scsi_command(
   scsi_handle 	n, 			/* result of scsi open */
   char*	pcOp,			/* ascii namne of cmd for disgs */
   unsigned char* pcCmd, 		/* scsi cmd */
   int 		nCmd, 			/* length of command */
   unsigned char* pcRdWr, 		/* write (if writeflag set) & read buf */
   int 		nRdWr, 			/* write&read len */
   int 		delay, 			/* time out */
   int 		writeflag)		/* the pcRdWr buf should be added to cmd */
{
	int scsistat, res;
	int len;
	static int gotstatus;

          /* sg_header is the first SCSI_OFF bytes of buffer */
#	define SCSI_OFF		sizeof(struct sg_header)
	static char buffer[2048];
	struct sg_header *sg_hd = (struct sg_header*) &buffer;
	
/* 	
        Linux returns the request sense data in the read packet, we save it each time 
	and return the data from the previous command if we get a request sense
 */
        DEBUG2(stderr,"sending scsi frame:\n");
        DEBUGDUMP2(pcCmd,nCmd);

	/* the system only gets 16 bytes of RS data on Linux, so if
	 * the requester wanted *more* than that, we have to re-ask
	 * anyhow.. 
	 */
	if (gotstatus && pcCmd[0] == 0x03 && nRdWr < 16) {
		/* we already have log sense data, so fake it */
		memcpy(  pcRdWr, sg_hd->sense_buffer, nRdWr );
		gotstatus = 0;
		return ftt_scsi_check(n,pcOp, 0, nRdWr);
	}
            /* delay is in secs, ioctl sets it to arg*10 millisecs */
        delay=delay*100;
        res = ioctl(n, SG_SET_TIMEOUT, &delay );
        if (res < 0) {
             perror("ftt_scsi_command - setting delay");
             return -1;
        }

          /* fill the sg_header */
	sg_hd->reply_len = sizeof(buffer)-SCSI_OFF;
	sg_hd->twelve_byte = (nCmd==12);

          /* copy the cmd to buffer following sg_head */        
	len = SCSI_OFF + nCmd;

          /* if we have data for the command, stuff it after the command */
	memcpy(buffer+SCSI_OFF, pcCmd, nCmd );
	if (writeflag) {
	    assert((SCSI_OFF+nCmd+nRdWr) <= sizeof(buffer));
	    memcpy(buffer+SCSI_OFF+nCmd, pcRdWr, nRdWr);
	    len += nRdWr;
	}
          /* finally, write the buffer */
	res = write(n, buffer, len);
        DEBUG2(stderr,"write() returned %d\n", res);
	if (res < 0) {
	    scsistat = 255;
	} else {
          /* and if it is successful, read the result */
	        sg_hd->sense_buffer[0] = 0;
		res = read(n, buffer, sizeof(buffer));
                DEBUG2(stderr,"read() returned %d\n", res);
		if (res < 0 || sg_hd->result || sg_hd->sense_buffer[0]) {
                    fprintf(stderr, "scsi passthru read result = 0x%x cmd=0x%x\n",
                             sg_hd->result, buffer[SCSI_OFF]);
		    if (sg_hd->result == 0x10)
			fprintf( stderr, "sg_hd->result == 0x10 cmd=0x%x!!!\n", *pcCmd );
                    fprintf(stderr, "scsi passthru sense "
                     "%x %x %x %x %x %x %x %x %x %x %x %x %x %x %x %x \n",
                       sg_hd->sense_buffer[0], sg_hd->sense_buffer[1],
                       sg_hd->sense_buffer[2], sg_hd->sense_buffer[3],
                       sg_hd->sense_buffer[4], sg_hd->sense_buffer[5],
                       sg_hd->sense_buffer[6], sg_hd->sense_buffer[7],
                       sg_hd->sense_buffer[8], sg_hd->sense_buffer[9],
                       sg_hd->sense_buffer[10], sg_hd->sense_buffer[11],
                       sg_hd->sense_buffer[12], sg_hd->sense_buffer[13],
                       sg_hd->sense_buffer[14], sg_hd->sense_buffer[15]);
		    scsistat = 255;
		    gotstatus = 1;
		} else {
		        if (!writeflag)
			{   res = res-SCSI_OFF;
			    if (res > nRdWr) res = nRdWr;
			    memcpy(pcRdWr, buffer+SCSI_OFF, res);
			}

			scsistat = sg_hd->result;
		}
	}


	if (pcRdWr != 0 && nRdWr != 0){
		DEBUG4(stderr,"Read/Write buffer:\n");
		DEBUGDUMP4(pcRdWr,res);
	}

	res = ftt_scsi_check(n,pcOp,scsistat,res);

	return res;
}
