/*

Author:    Marilyn Schweizer
Fermi National Accelerator Laboratory - Mail Station 369
P.O Box 500
Batavia, IL 60510

Copyright (c) 1992 Universities Research Association, Inc.
All Rights Reserved


This material resulted from work developed under a Government Contract and
is subject to the following license:  The Government retains a paid-up,
nonexclusive, irrevocable worldwide license to reproduce, prepare derivative
works, perform publicly and display publicly by or for the Government,
including the right to distribute to other Government contractors.  Neither
the United States nor the United States Department of Energy, nor any of
their employees, makes any warrenty, express or implied, or assumes any
legal liability or responsibility for the accuracy, completeness, or
represents that its use would not infringe privately owned rights.

*/

/************************************************************************/
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
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/param.h>
#include <sys/errno.h>
#include <strings.h>
#include <unistd.h>
#include "stt.h"

#if defined(IRIX) || defined(IRIX5)
#include <fcntl.h>
#include <dslib.h>
#include <sys/mtio.h>
#endif

#ifdef AIX
#include <fcntl.h>
#include <sys/ioctl.h>
#include <sys/scsi.h>
#include <sys/tape.h>
#endif

static int     	stt_stats_inquiry       (int fd, stt_stats *stats);
extern int 	errno;


/*==============================================================================
stt_stats_get	- get the device status
==============================================================================*/
int stt_stats_get(devfile,stats)
char *devfile;
stt_stats *stats;
{
int fd, rc;
#ifdef IRIX
struct mtget getbuf;
#endif

/* we need to be careful here. the size of data to be returned in
the scsi commands must be the maximum of the dlt or exabytes.
We are looking at exabyte 8200, 8500, 8505, and dlt 2000 and
4000.
--------------------------------------------------------------- */


static unsigned char acbuf[256];/* pointer to any command data buffer */
		/* non-paged mode sense */
#define stt_mod1_size 12
static char acModbuf_scsi1[] = { 0x1a, 0x00, 0x00, 0x00, stt_mod1_size, 0x00 };
		/* vendor unique page - 850x only */
#define stt_mod85_size 18
static char acModbuf_850x[]  = { 0x1a, 0x00, 0x20, 0x00, stt_mod85_size, 0x00 };
		/* request sense */
#define stt_rs_size 29
static char acRSbuf[]  	     = { 0x03, 0x00, 0x00, 0x00, stt_rs_size, 0x00 };
		/* test unit ready */
static char acTRbuf[]        = { 0x00, 0x00, 0x00, 0x00, 00, 0x00 };
		/* log sense */
#define stt_ls_size 256
static char acLSbuf[]	     = { 0x4d, 0x00, 0x40, 0x00, 0x00, 0x00, 0x00, 0x01,
		 0x00, 0x00};
		/* ascii sense keys - not all strings
		   are valid for all device types! */
static char *sensetab[] = {
"no sense key",    "recovered error",  "not ready", 	     "medium error", 
"hardware error",  "illegal request",  "unit attention",  "write protected",
"blank check",     "exabyte specific", "not used",	     "aborted command",
"not used",	      "volume overflow",  "miscompare",      "reserved" };
static char *tapeformat[] = {
"8500",	"8200",	"8500c", "8200c", "?", "?", "?", "?" };
static char scsi_dev[512];

/* let's initialize the status structure
------------------------------------- */

stt_stats_initstruct (stats);

/* check and resolve all symbolic links
------------------------------------ */

if (stt_uti_check_device (devfile,scsi_dev,&(stats->ret))) return 0;

/* Cannot do scsi opens on SCS VME tape controllers
under IRIX 4.0.5, so return limited information 
------------------------------------------------  */

#ifdef IRIX
fd = open(scsi_dev, O_RDONLY, 0);			/* open device */
if (fd < 0 ) 						/* error! */
   {
   sprintf(stats->ret.errmsg,"Open failed for %s with errno=%d",scsi_dev,errno);
   stats->ret.status = -1;
   return 0;
   } 
if (ioctl(fd, MTIOCGET,&getbuf) != 0) 			/* do ioctl */
   {							/* error! */
   sprintf(stats->ret.errmsg,"Ioctl failed for %s with errno=%d",scsi_dev,
      errno);
   close(fd);
   stats->ret.status = -1;
   return 0;
   }
close(fd);						/* close device */

/* Note, For a VME-SCSI device (e.g. jagd), can't use dslib so return 
only online status (unitready) and write protection flag 
------------------------------------------------------------------- */

if (getbuf.mt_type != MT_ISSCSI) 
   {
   if (getbuf.mt_type == MT_ISVSCSI) 
      strcpy(stats->stats.controller,"VME-SCSI");
   else 
      sprintf(stats->stats.controller,"%7d?",getbuf.mt_type);
   stats->stats.unitready = (getbuf.mt_dposn & MT_ONL) ? 0:1;
   stats->stats.allflags = (getbuf.mt_dposn & MT_WPROT) ? 0x2000:0;
   strcpy(stats->ret.errmsg,"Success");
   stats->ret.status = 0;
   return 0;
   }	
#endif


/* get the appropriate scsi device name to open 
-------------------------------------------- */

if (stt_uti_get_scsi(scsi_dev,scsi_dev,&(stats->ret))) return 0;

/* phew! made it this far. Let's open the scsi device
-------------------------------------------------- */

strcpy(stats->stats.controller,"SCSI");		/* set controller name */
if (stt_raw_open(scsi_dev, &fd)) 		/* open device */
   {						/* error! */
   sprintf(stats->ret.errmsg,"SCSI open of %s failed",scsi_dev);
   stats->ret.status = -1;
   return 0;
   }

/* test unit ready
if there's a check condition, we'll do it twice in case it was simply 
a unit attention error. It's easier to do it twice rather than getting
the request sense  now and verifying unit attention.
---------------------------------------------------------------------- */

if ((stats->stats.unitready =  
   stt_raw_command(fd,"test unit ready",acTRbuf,6,acbuf,0,5,0)) == 2)
   {
   stats->stats.unitready=stt_raw_command(fd,"test unit ready",acTRbuf,6,
      acbuf,0,5,0);
   }

/* inquiry - this will tell us how to interpret the rest of the data and
what commands to use to get them.
---------------------------------------------------------------------- */

if ((rc = stt_stats_inquiry(fd,stats)))
   {
   sprintf(stats->ret.errmsg,"SCSI inquiry cmd to %s failed with %d",
      scsi_dev,rc);
   stats->ret.status = -1;
   stt_raw_close(fd);
   return 0;
   }

/* request_sense
------------- */

if ((rc = stt_raw_command(fd,"request sense",acRSbuf,6,acbuf,stt_rs_size,5,0))) 
   {
   sprintf(stats->ret.errmsg,"SCSI request sense cmd to %s failed with %d",
      scsi_dev,rc);
   stats->ret.status = -1;
   stt_raw_close(fd);
   return 0;
   }
strncpy(stats->stats.sensekey,sensetab[acbuf[2] & 0x0F],16);	/* sense key */
stats->stats.sensekey[16] = '\0';

if (acbuf[0] & 0x80)					/* information bytes */
   stats->stats.unprocssd = ((acbuf[3]<<24)+(acbuf[4]<<16)+
      (acbuf[5]<<8) + acbuf[6]);
else
   stats->stats.unprocssd = 0;

stats->stats.sensecode = acbuf[12];			/* sense code */
stats->stats.sensequal = acbuf[13];			/*sense code qualifier*/

/* Put All Flags into a single int. This part is true no matter what the
device type.
---------------------------------------------------------------------  
Bits are: 
File Mark:      (acbuf[2] & 0x80) << 24
EOM:            (acbuf[2] & 0x40) << 24
ILI:            (acbuf[2] & 0x20) << 24
*/
stats->stats.allflags = (((acbuf[2] >> 4) << 24));	/* fmk, eom, ili */

if (strncmp(stats->stats.product,"EXB-",4) == 0)	/* exabytes only */
   {
   stats->stats.errors = 
   (acbuf[16]<<16)+(acbuf[17]<<8)+acbuf[18];		/* errors */
      stats->stats.tapeleft = (acbuf[23]<<16) + 
      (acbuf[24]<<8) + acbuf[25]; 			/* tape left */

/* Put All Flags into a single int 
------------------------------- 
Bits are: 
Reserved:       (acbuf[21] & 0x80) << 16 
Reserved:  (acbuf[21] & 0x40) << 16
Rvrs Rtrs Rqrd: (acbuf[21] & 0x20) << 16 
Cleaned:   (acbuf[21] & 0x10) << 16
Needs Cleaning: (acbuf[21] & 0x08) << 16
Physical EOT:   (acbuf[21] & 0x04) << 16
Wrt Spl Err Bk: (acbuf[21] & 0x02) << 16
Wrt Spl Err Op: (acbuf[21] & 0x01) << 16
Reserved:       (acbuf[20] & 0x80) << 8
Mark Det Err:   (acbuf[20] & 0x40) << 8
Write Prot:     (acbuf[20] & 0x20) << 8
Filemark Err:   (acbuf[20] & 0x10) << 8
Under Run Err:  (acbuf[20] & 0x08)
Servo Sys Err:  (acbuf[20] & 0x04) 
Splice Err:     (acbuf[20] & 0x02)
Formatter Err:  (acbuf[20] & 0x01) 
Power Fail:     (acbuf[19] & 0x80) 
Parity Err:     (acbuf[19] & 0x40) 
Buffer Err:     (acbuf[19] & 0x20) 
Media Err:      (acbuf[19] & 0x10) 
Err Overflow:   (acbuf[19] & 0x08) 
Motion Err:     (acbuf[19] & 0x04) 
No Tape Err:    (acbuf[19] & 0x02) 
BOT:            (acbuf[19] & 0x01) 
*/
   stats->stats.allflags += ((acbuf[2]>>4)<<24)+(acbuf[21]<<16)
			+(acbuf[20]<<8)+acbuf[19];
   }

else if (strncmp(stats->stats.product,"DLT",3) == 0)		/* dlt only */
   {
   stats->stats.field_flag = acbuf[15];
   stats->stats.field_ptr = (acbuf[16] << 8) + acbuf[17];
   stats->stats.internal_status = acbuf[18]; 
   }
if (strncmp(stats->stats.product,"DLT4000",7) == 0)		/* dlt4000 */
   {
   stats->stats.motion_hours = (acbuf[19] << 8) + acbuf[20]; 
   stats->stats.poweron_hours = (acbuf[21] << 24) + (acbuf[22] << 16) + 
		  (acbuf[23] << 8)  + acbuf[24];
   }

/* get mode sense information
-------------------------- */

if ((rc=stt_raw_command(fd,"mode sense",acModbuf_scsi1,6,acbuf,stt_mod1_size,5,
   0)))
   {
   sprintf(stats->ret.errmsg,"SCSI mode sense cmd to %s failed with %d",
      scsi_dev,rc);
   stats->ret.status = -1;
   stt_raw_close(fd);
   return 0;
   }

/* Make sure correct amount of data actually got returned 
------------------------------------------------- */
if (acbuf[0] >= 11) 
   {
   stats->stats.bufmod = ((acbuf[2] & 0x70)>>4);	/* buffered mode */
   stats->stats.write_prot = ((acbuf[2] & 0x80) >> 7);	/* write protected */
   stats->stats.density = acbuf[4];			/* density */
   stats->stats.blocks = ((acbuf[5]<<16)+(acbuf[6]<<8)
      +acbuf[7]); 					/* blocks */
   stats->stats.blksize = ((acbuf[9]<<16)+(acbuf[10]<<8)
      +acbuf[11]);					/* block size */
   if (strncmp(stats->stats.product,"EXB-",4) == 0)	/* exabytes only */
      {
      stats->stats.mediumtype = acbuf[1];		/* medium type */
      }
   }


/* mode sense for vendor unique page (8500 only) will get the tape format
information 
---------------------------------------------------------------------- */

if (strncmp(stats->stats.product,"EXB-850",7) == 0) 
   {
   if ((rc = stt_raw_command(fd,"mode sense",acModbuf_850x,6,acbuf,
      stt_mod85_size,5,0)))
      {
      sprintf(stats->ret.errmsg,"SCSI mode sense page cmd to %s failed with %d",
          scsi_dev,rc);
      stats->ret.status = -1;
      stt_raw_close(fd);
      return 0;
      }

   stats->stats.nodiscnt = (acbuf[14] >> 5 & 0x1);
   strncpy(stats->stats.rtf,tapeformat[(acbuf[15] >> 5) & 0x7],5);
   stats->stats.rtf[5] = '\0';
   strncpy(stats->stats.wtf,tapeformat[(acbuf[15] >> 2) & 0x7],5);
   stats->stats.wtf[5] = '\0';
   }

/* get the read and write error counters from the log sense information 
for everything except the 8200 and 8500
------------------------------------------------------------ */
if (strncmp(stats->stats.product,"EXB-820",7) &&	/* not an 8200 */
    strncmp(stats->stats.product,"EXB-8500",8)) 	/* not an 8500 */
   {
   unsigned char *nbytes_processed;			/* bytes processed */
   int pagelength = 0;					/* pagelength */
   int gotit;

   /* first get write error page
      -------------------------- */

   acLSbuf[2] = ((acLSbuf[2] & 0xD0) | 2);		/* set page code 2 */
   acLSbuf[6] = 0x2;					/* parameter */
   if ((rc = stt_raw_command(fd,"log sense",acLSbuf,10,acbuf,stt_ls_size,5,0)))
      {
      sprintf(stats->ret.errmsg,"SCSI log sense page cmd to %s failed with %d",
          scsi_dev,rc);
      stats->ret.status = -1;
      stt_raw_close(fd);
      return 0;
      }

   if ((acbuf[0] & 0x3F) != 2)				/* not right page*/
      {
      sprintf(stats->ret.errmsg,"SCSI log sense could not get write page %s ",
          scsi_dev);
      stats->ret.status = -1;
      stt_raw_close(fd);
      return 0;
      }

   if (acbuf[5] != 2)					/* not right parameter*/
      {
      sprintf(stats->ret.errmsg,"SCSI log sense could not get write errors %s ",
          scsi_dev);
      stats->ret.status = -1;
      stt_raw_close(fd);
      return 0;
      }

   /* this info is 4 bytes for dlts and 3 bytes for exabytes 8505. sigh
      ----------------------------------------------------------------- */

   if (!strncmp(stats->stats.product,"EXB-8505",8)) 	/* 8505 */
      {
      stats->stats.write_errors = (acbuf[9] << 16) + (acbuf[10] << 8) +
         acbuf[11];
      }
   else							/* dlt */
      {
      stats->stats.write_errors = (acbuf[9] << 24) + (acbuf[10] << 16) +
                        	  (acbuf[11] << 8) + acbuf[12];
      }

   /* get nbytes processed  - it probably would be easier to do another
      log sense to get this information, but we already have it in
      our data buffer ...
      ------------------------------------------------------------------ */

   pagelength = (acbuf[2] << 8) + acbuf[3];		/* total # bytes */
   nbytes_processed = &(acbuf[4]);			/* init pointer */
   gotit = 0;						/* init flag */

   do							/* for all params */
      {
      if (nbytes_processed[0] == 0 && nbytes_processed[1] == 5) 
         {
         gotit = 1;					/* got it */
         stats->stats.write_nbytes[0] = (nbytes_processed[4] << 24) +
				        (nbytes_processed[5] << 16) +
				        (nbytes_processed[6] << 8) +
				         nbytes_processed[7]; 
         stats->stats.write_nbytes[1] = (nbytes_processed[8] << 24) +
				        (nbytes_processed[9] << 16) +
				        (nbytes_processed[10] << 8) +
				         nbytes_processed[11]; 
         break;
         }
      nbytes_processed += (nbytes_processed[3] + 4);	/* try again */
      }
   while (nbytes_processed < acbuf + pagelength);	/* don't exceed max */

   if (!gotit)						/* didn't find it */
      {
      sprintf(stats->ret.errmsg,"SCSI log sense could not get write nbytes%s ",
          scsi_dev);
      stats->ret.status = -1;
      stt_raw_close(fd);
      return 0;
      }

   /* next get read error page
      -------------------------- */

   acLSbuf[2] = ((acLSbuf[2] & 0xD0) | 3);		/* set page code 2 */
   acLSbuf[6] = 0x2;					/* parameter */
   if ((rc = stt_raw_command(fd,"log sense",acLSbuf,10,acbuf,stt_ls_size,5,0)))
      {
      sprintf(stats->ret.errmsg,"SCSI log sense page cmd to %s failed with %d",
          scsi_dev,rc);
      stats->ret.status = -1;
      stt_raw_close(fd);
      return 0;
      }

   if ((acbuf[0] & 0x3F) != 3)				/* not right page*/
      {
      sprintf(stats->ret.errmsg,"SCSI log sense could not get write page %s ",
          scsi_dev);
      stats->ret.status = -1;
      stt_raw_close(fd);
      return 0;
      }

   if (acbuf[5] != 2)					/* not right parameter*/
      {
      sprintf(stats->ret.errmsg,"SCSI log sense could not get read errors %s ",
          scsi_dev);
      stats->ret.status = -1;
      stt_raw_close(fd);
      return 0;
      }

   /* this info is 4 bytes for dlts and 3 bytes for exabytes 8505. sigh
      ----------------------------------------------------------------- */

   if (!strncmp(stats->stats.product,"EXB-8505",8)) 	/* 8505 */
      {
      stats->stats.read_errors = (acbuf[9] << 16) + (acbuf[10] << 8) +
         acbuf[11];
      }
   else							/* dlt */
      {
      stats->stats.read_errors = (acbuf[9] << 24) + (acbuf[10] << 16) +
	   (acbuf[11] << 8) + acbuf[12];
      }

   /* get nbytes processed  - it probably would be easier to do another
      log sense to get this information, but we already have it in
      our data buffer ...
      ------------------------------------------------------------------ */

   pagelength = (acbuf[2] << 8) + acbuf[3];
   nbytes_processed = &(acbuf[4]);
   gotit = 0;

   do
      {
      if (nbytes_processed[0] == 0 && nbytes_processed[1] == 5) 
         {
         gotit = 1;
         stats->stats.read_nbytes[0] = (nbytes_processed[4] << 24) +
     				       (nbytes_processed[5] << 16) +
				       (nbytes_processed[6] << 8) +
				        nbytes_processed[7]; 
         stats->stats.read_nbytes[1] = (nbytes_processed[8] << 24) +
				       (nbytes_processed[9] << 16) +
				       (nbytes_processed[10] << 8) +
				        nbytes_processed[11]; 
         break;
         }
      nbytes_processed += (nbytes_processed[3] + 4);
      }
   while (nbytes_processed < acbuf + pagelength);

   if (!gotit)					/* not right parameter*/
      {
      sprintf(stats->ret.errmsg,"SCSI log sense could not get read nbytes%s ",
          scsi_dev);
      stats->ret.status = -1;
      stt_raw_close(fd);
      return 0;
      }
   }

/* get the number of bytes transferred to tape from the log sense page
   ------------------------------------------------------------------- */
if (!strncmp(stats->stats.product,"DLT",3))		/* dlt only */
   {
   int pagelength = 0;
   char *bytes_xfer;
   int gotit;

   /* first get compression page 
      -------------------------- */

   acLSbuf[2] = ((acLSbuf[2] & 0xD0) | 0x32);           /* set page code */
   acLSbuf[6] = 0x8;                                    /* parameter */
   if ((rc = stt_raw_command(fd,"log sense",acLSbuf,10,acbuf,stt_ls_size,5,0)))
      {
      sprintf(stats->ret.errmsg,"SCSI log sense page cmd to %s failed with %d",
         scsi_dev,rc);
      stats->ret.status = -1;
      stt_raw_close(fd);
      return 0;
      }

   if ((acbuf[0] & 0x3F) != 0x32)                       /* not right page*/
      {
      sprintf(stats->ret.errmsg,
         "SCSI log sense could not get compression page %s ", scsi_dev);
      stats->ret.status = -1;
      stt_raw_close(fd);
      return 0;
      }

   /* this is stupid. #ifdef out is the way that it should work. however,
      the dlt's don't support the real scsi standard yet on the compression
      page. You should be able to pass the index of the starting parameter
      that you want. However, the dlt always gives you starting from parameter
      0 (compression page only. the other pages work as documented). We need to 
      hunt down the information in the maze of parameters that are returned.
      ----------------------------------------------------------------------- */

   /* get bytes transferred to tape
      ----------------------------- */

   pagelength = (acbuf[2] << 8) + acbuf[3];
   bytes_xfer = &(acbuf[4]);
   gotit = 0;

   /* this gets compression information for elliot
      -------------------------------------------- */

#ifdef echeu
   do
      {
      if (bytes_xfer[0] == 0 && bytes_xfer[1] == 0)
         {
         gotit = 1;
         stats->stats.read_compress_ratio = (bytes_xfer[4]<<8) + bytes_xfer[5];
         break;
         }
      bytes_xfer += (bytes_xfer[3] + 4);
      }
   while (bytes_xfer < acbuf + pagelength);
 
   if (!gotit)                                     /* not right parameter*/
      {
      sprintf(stats->ret.errmsg,
	 "SCSI log sense could not get read compression ratio %s ", scsi_dev);
      stats->ret.status = -1;
      stt_raw_close(fd);
      return 0;
      }
   gotit = 0;

   do
      {
      if (bytes_xfer[0] == 0 && bytes_xfer[1] == 1)
         {
         gotit = 1;
         stats->stats.write_compress_ratio = (bytes_xfer[4]<<8) + bytes_xfer[5];
         break;
         }
      bytes_xfer += (bytes_xfer[3] + 4);
      }
   while (bytes_xfer < acbuf + pagelength);
 
   if (!gotit)                                     /* not right parameter*/
      {
      sprintf(stats->ret.errmsg,
	 "SCSI log sense could not get write compression ratio %s ", scsi_dev);
      stats->ret.status = -1;
      stt_raw_close(fd);
      return 0;
      }
   gotit = 0;
#endif

   do							/* loop over params */
      {
      if (bytes_xfer[0] == 0 && bytes_xfer[1] == 8)	/* param 8? */ 
         {
         gotit = 1;					/* found it */
         stats->stats.mbytes_written = (bytes_xfer[4] << 24) +
            (bytes_xfer[5] << 16) + (bytes_xfer[6] << 8) + bytes_xfer[7]; 
         break;
         }
      bytes_xfer += (bytes_xfer[3] + 4);		/* get next param */
      }
   while (bytes_xfer < acbuf + pagelength);		/* no more params */

   if (!gotit)						/* didn't find it */
      {
      sprintf(stats->ret.errmsg,"SCSI log sense could not get Mbytes xfer%s ",
            scsi_dev);
      stats->ret.status = -1;
      stt_raw_close(fd);
      return 0;
      }

   gotit = 0;						/* reset flag */
   do						/* look thru remaining params */
      {
      if (bytes_xfer[0] == 0 && bytes_xfer[1] == 9)	/* found param 9 */ 
         {
         gotit = 1;					/* set flag */
         stats->stats.bytes_written = (bytes_xfer[4] << 24) +
         	(bytes_xfer[5] << 16) + (bytes_xfer[6] << 8) + bytes_xfer[7]; 
         break;						/* done */
         }
      bytes_xfer += (bytes_xfer[3] + 4);		/* get next param */
      }
   while (bytes_xfer < acbuf + pagelength);		/* no more param */

   if (!gotit)						/* couldn't find it */
      {
      sprintf(stats->ret.errmsg,"SCSI log sense could not get bytes xfer%s ",
         scsi_dev);
      stats->ret.status = -1;
      stt_raw_close(fd);
      return 0;
      }

   /* this is the way it should be 
      ---------------------------- */

#if 0					
   if (acbuf[5] != 0x8)  				/* not right parameter*/
      {
      sprintf(stats->ret.errmsg,"SCSI log sense could not get bytes xfer %s ",
          scsi_dev);
      stats->ret.status = -1;
      stt_raw_close(fd);
      return 0;
      }
   stats->stats.mbytes_written = (acbuf[8] << 24) + (acbuf[9] << 16) +
   	  (acbuf[10] << 8) + acbuf[11];

   if (acbuf[13] != 0x9)  				/* not right parameter*/
      {
      sprintf(stats->ret.errmsg,"SCSI log sense could not get bytes xfer %s ",
          scsi_dev);
      stats->ret.status = -1;
      stt_raw_close(fd);
      return 0;
      }
   stats->stats.mbytes_written = (acbuf[16] << 24) + (acbuf[17] << 16) +
	      (acbuf[18] << 8) + acbuf[19];
#endif
   }

/* all done
-------- */

stt_raw_close(fd);
strcpy(stats->ret.errmsg,"Success");
stats->ret.status = 0;
return 0;
}


/*==============================================================================
stt_stats_clear - clear the accumulated stats
==============================================================================*/
int stt_stats_clear(char *devfile, stt_dev_return *ret_stats)
{
int 		fd, rc;
static char 	scsi_dev[512];
stt_stats	stats;
#ifdef IRIX
struct mtget 	getbuf;
#endif

/* we need to be careful here. the size of data to be returned in
the scsi commands must be the maximum of the dlt or exabytes.
We are looking at exabyte 8200, 8500, 8505, and dlt 2000 and
4000.
--------------------------------------------------------------- */


static unsigned char acbuf[256];/* pointer to any command data buffer */
					/* test unit ready */
static char acTRbuf[]        = { 0x00, 0x00, 0x00, 0x00, 00, 0x00 };
					/* request sense */
#define stt_rs_size 29
static char acRSbuf[]  	     = { 0x03, 0x00, 0x00, 0x00, stt_rs_size, 0x80 };
					/* log select */
#define stt_ls_size 256
static char acLSbuf[]	     = { 0x4c, 0x02, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00,
		 0x00, 0x00};

/* check and resolve all symbolic links
------------------------------------ */

if (stt_uti_check_device (devfile,scsi_dev,ret_stats)) return 0;

/* Cannot do scsi opens on SCS VME tape controllers
under IRIX 4.0.5, so we can't clear anything
------------------------------------------------  */

#ifdef IRIX
fd = open(scsi_dev, O_RDONLY, 0);			/* open device */
if (fd < 0 ) 						/* error! */
{
sprintf(ret_stats->errmsg,"Open failed for %s with errno=%d",scsi_dev,errno);
ret_stats->status = -1;
return 0;
} 
if (ioctl(fd, MTIOCGET,&getbuf) != 0) 			/* do ioctl */
{							/* error! */
sprintf(ret_stats->errmsg,"Ioctl failed for %s with errno=%d",
scsi_dev,errno);
close(fd);
ret_stats->status = -1;
return 0;
}
close(fd);						/* close device */

/* Note, For a VME-SCSI device (e.g. jagd), can't use dslib so return 
only online status (unitready) and write protection flag 
------------------------------------------------------------------- */

if (getbuf.mt_type != MT_ISSCSI) 
{
sprintf(ret_stats->errmsg,"Cannot use raw scsi driver for this device",
scsi_dev,errno);
close(fd);
ret_stats->status = -1;
return 0;
}	
#endif


/* get the appropriate scsi device name to open 
-------------------------------------------- */

if (stt_uti_get_scsi(scsi_dev,scsi_dev,ret_stats)) return 0;

/* phew! made it this far. Let's open the scsi device
-------------------------------------------------- */

if (stt_raw_open(scsi_dev, &fd)) 		/* open device */
{						/* error! */
sprintf(ret_stats->errmsg,"SCSI open of %s failed",scsi_dev);
ret_stats->status = -1;
return 0;
}

/* test unit ready
if there's a check condition, we'll do it twice in case it was simply 
a unit attention error. It's easier to do it twice rather than getting
the request sense  now and verifying unit attention.
---------------------------------------------------------------------- */

if ((stt_raw_command(fd,"test unit ready",acTRbuf,6,acbuf,0,5,0)) == 2)
{
rc=stt_raw_command(fd,"test unit ready",acTRbuf,6,acbuf,0,5,0);
}

/* inquiry - this will tell us how to interpret the rest of the data and
what commands to use to get them.
---------------------------------------------------------------------- */

if ((rc = stt_stats_inquiry(fd,&stats)))
{
sprintf(ret_stats->errmsg,"SCSI inquiry cmd to %s failed with %d",
scsi_dev,rc);
ret_stats->status = -1;
stt_raw_close(fd);
return 0;
}

/* issue request sense to clear some counters
------------------------------------------ */

if (!strncmp(stats.stats.product,"EXB",3))		/* exabyte only */
{
if ((rc=stt_raw_command(fd,"request sense",acRSbuf,6,acbuf,stt_rs_size,5,0)))
{
sprintf(ret_stats->errmsg,"SCSI mode sense cmd to %s failed with %d",
 scsi_dev,rc);
ret_stats->status = -1;
stt_raw_close(fd);
return 0;
}
}

/* reset log pages
--------------- */

if (!strncmp(stats.stats.product,"EXB-8505",7) ||	/* exabyte 8505 */
!strncmp(stats.stats.product,"DLT",3))		/* or dlt */
{
if ((rc = stt_raw_command(fd,"log select",acLSbuf,10,acbuf,stt_ls_size,5,0)))
{
sprintf(ret_stats->errmsg,"SCSI log select page cmd to %s failed with %d",
 scsi_dev,rc);
ret_stats->status = -1;
stt_raw_close(fd);
return 0;
}
}

stt_raw_close(fd);
strcpy(ret_stats->errmsg,"Success");
ret_stats->status = 0;
return 0;
}



/*==============================================================================
stt_stats_initstruct - routine to initialize the status structure.
		  marks everything invalid
==============================================================================*/
void stt_stats_initstruct (stt_stats *stats)
{
stats->ret.status = -1;
strcpy(stats->ret.errmsg,"?");
strcpy(stats->stats.controller,"?");
strcpy(stats->stats.vendor,"?");
strcpy(stats->stats.product,"?");
strcpy(stats->stats.firmware,"?");
strcpy(stats->stats.serial,"?");
stats->stats.unitready = -1;
strcpy(stats->stats.rtf,"?");
strcpy(stats->stats.wtf,"?");
stats->stats.bufmod = -1;
stats->stats.mediumtype = -1;
stats->stats.density = 0;
stats->stats.blocks = -1;
stats->stats.blksize = -1;
stats->stats.nodiscnt = -1;
strcpy(stats->stats.sensekey,"?");
stats->stats.unprocssd = 0;
stats->stats.sensecode = 0;
stats->stats.sensequal = 0;
stats->stats.tapeleft = -1;
stats->stats.errors = 0;
stats->stats.allflags = 0;
stats->stats.field_flag = 0;
stats->stats.field_ptr = -1;
stats->stats.internal_status = -1;
stats->stats.motion_hours = -1;
stats->stats.poweron_hours = -1;
stats->stats.write_prot = 0;
stats->stats.write_errors = -1;
stats->stats.write_nbytes[0] = -1;
stats->stats.write_nbytes[1] = -1;
stats->stats.read_errors = -1;
stats->stats.read_nbytes[0] = -1;
stats->stats.read_nbytes[1] = -1;
stats->stats.mbytes_written = -1;
stats->stats.bytes_written = -1;
#ifdef echeu
stats->stats.read_compress_ratio = -1;
stats->stats.write_compress_ratio = -1;
#endif
}

/*==============================================================================
   stt_stats_inquiry - internal utility routine to get the inquiry data
==============================================================================*/
static int stt_stats_inquiry (int fd, stt_stats *stats)
{
int	rc;
static unsigned char acbuf[256];/* pointer to any command data buffer */
			/* non-paged inquiry page */
#define stt_inq1_size 106
static char acInqbuf_scsi1[] = { 0x12, 0x00, 0x00, 0x00, stt_inq1_size, 0x00 };
			/* inquiry command with unit serial # page */
#define stt_inq2_size 14
static char acInqbuf_scsi2[] = { 0x12, 0x01, 0x80, 0x00, stt_inq2_size, 0x00 };

/* inquiry - this will tell us how to interpret the rest of the data and
what commands to use to get them.
---------------------------------------------------------------------- */

if ((rc=stt_raw_command(fd,"inquiry",acInqbuf_scsi1,6,acbuf,stt_inq1_size,5,0)))
   {
   return rc;
   }

strncpy(stats->stats.vendor,acbuf+8,8);			/* vendor id */
stats->stats.vendor[8] = '\0';

strncpy(stats->stats.product,acbuf+16,16);		/* product id */
stats->stats.product[16] = '\0';

strncpy(stats->stats.firmware,acbuf+32,4);		/* firmware id */
stats->stats.firmware[4] = '\0';

/* Get the serial number. We can't do this for exabytes 8200.  For exabyte 
85xx it is available as part of this data. However, the scsi2 standard
pushes it into a unit serial number page. We will use this since this
is the only way it's available for the dlt's.
------------------------------------------------------------------------ */

if (strncmp(stats->stats.product,"EXB-820",7)) 		/* not an 8200 */
   {
   if ((rc=stt_raw_command(fd,"inquiry",acInqbuf_scsi2,6,acbuf,stt_inq2_size,
      5,0))) 
      {
      stats->ret.status = -1;
      return rc;
      }
   strncpy(stats->stats.serial,acbuf+4,10);		/* serial number */
   stats->stats.serial[10] = '\0';
   }
return 0;
}
