static char rcsid[] = "@(#)$Id$";

#include <stdio.h>
#include <string.h>
#include <ftt_private.h>
#include <ctype.h>

#ifdef WIN32
#include <io.h>
#include <process.h>
#include <windows.h>

#define geteuid() -1
#define bzero ZeroMemory


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
    if(d->scsi_descriptor >= 0 ) {
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
    static unsigned char acSensebuf[18];

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
		DEBUG3(stderr,"request sense returns res %d:\n", res);
		DEBUG3(stderr, errmsg, pcOp, stat,
			acSensebuf[0], acSensebuf[1],
			acSensebuf[2], acSensebuf[3],
			acSensebuf[4], acSensebuf[5],
			acSensebuf[6], acSensebuf[7],
			acSensebuf[8], acSensebuf[9],
			acSensebuf[10], acSensebuf[12],
			acSensebuf[13], acSensebuf[14],
			acSensebuf[15]);
		recursive = 0;
	    } else {
		return 0;
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
	    DEBUG3(stderr, "Found slot %d, name %s\n", 
				       j,       d->devinfo[j].device_name);
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

#include "ftt_dbd.h"

static double pad;
int
ftt_scsi_set_compression(ftt_descriptor d, int compression) {

    /* getting evil alignment errors on IRIX6.5 */
    static unsigned char 
	mod_sen10[8] = { 0x1a, DBD, 0x10, 0x00, BD_SIZE+16, 0x00},
	mod_sel10[8] = { 0x15, 0x10, 0x00, 0x00, BD_SIZE+16, 0x00},
	mod_sen0f[8] = { 0x1a, DBD, 0x0f, 0x00, BD_SIZE+16, 0x00},
	mod_sel0f[8] = { 0x15, 0x10, 0x00, 0x00, BD_SIZE+16, 0x00},
	buf [32],
        opbuf[512];
    int res = 0;

    ENTERING("ftt_set_compression");
    CKNULL("ftt_descriptor", d);
    sprintf( opbuf , "2%s", d->prod_id);

    if ((d->flags&FTT_FLAG_SUID_SCSI) == 0 || 0 == geteuid()) {
	if (ftt_get_stat_ops(opbuf) & FTT_DO_MS_Px0f) {
	    DEBUG2(stderr, "Using SCSI Mode sense 0x0f page to set compression\n");
	    res = ftt_open_scsi_dev(d);        
	    if(res < 0) return res;
	    res = ftt_do_scsi_command(d, "Mode sense", mod_sen0f, 6, buf, BD_SIZE+16, 5, 0);
	    if(res < 0) return res;
	    buf[0] = 0;
	    buf[1] = 0;
	    /* enable outgoing compression */
	    buf[BD_SIZE + 2] &= ~(1 << 7);
	    buf[BD_SIZE + 2] |= (compression << 7);

	    res = ftt_do_scsi_command(d, "Mode Select", mod_sel0f, 6, buf, BD_SIZE+16, 120, 1);
	    if(res < 0) return res;
	    res = ftt_close_scsi_dev(d);
	    if(res < 0) return res;
	}
	if (ftt_get_stat_ops(opbuf) & FTT_DO_MS_Px10) {
	    DEBUG2(stderr, "Using SCSI Mode sense 0x10 page to set compression\n");
	    res = ftt_open_scsi_dev(d);        
	    if(res < 0) return res;
	    res = ftt_do_scsi_command(d, "Mode sense", mod_sen10, 6, buf, BD_SIZE+16, 5, 0);
	    if(res < 0) return res;
	    buf[0] = 0;
	    /* we shouldn't be changing density here but it shouldn't hurt */
	    /* yes it will! the setuid program doesn't know which density */
	    /* the parent process set... */
	    /* buf[BD_SIZE] = d->devinfo[d->which_is_default].hwdens; */
 	    buf[BD_SIZE + 14] = compression;
	    res = ftt_do_scsi_command(d, "Mode Select", mod_sel10, 6, buf, BD_SIZE+16, 120, 1);
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
extern ftt_itoa(long n);

int
ftt_scsi_locate( ftt_descriptor d, int blockno) {

    int res = 0;

    if ((d->flags & FTT_FLAG_SUID_SCSI) && 0 != geteuid()) {
	ftt_close_dev(d);
	switch(ftt_fork(d)){
	case -1:
		return -1;

	case 0:  /* child */
		fflush(stdout);	/* make async_pf stdout */
		fflush(d->async_pf_parent);
		close(1);
		dup2(fileno(d->async_pf_parent),1);
		if (ftt_debug) {
		    execlp("ftt_suid", "ftt_suid", "-x", "-l", ftt_itoa(blockno), d->basename, 0);
		} else {
		    execlp("ftt_suid", "ftt_suid", "-l", ftt_itoa(blockno), d->basename, 0);
		}

	default: /* parent */
		res = ftt_wait(d);
	}
    } else {
	static unsigned char 
	    locate_cmd[10] = {0x2b,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00};
	 
	locate_cmd[3] = (blockno >> 24) & 0xff;
	locate_cmd[4] = (blockno >> 16) & 0xff;
	locate_cmd[5] = (blockno >> 8)  & 0xff; 
	locate_cmd[6] = blockno & 0xff;
	res = ftt_do_scsi_command(d,"Locate",locate_cmd,10,NULL,0,300,0);
	res = ftt_describe_error(d,0,"a SCSI pass-through call", res, res,"Locate", 0);

    }
    return res;
}

/*
 definitions and functions to do a scsi inquire and print the formatted results
*/
typedef struct
{
  unsigned char inqd[8];
/*
 bit fields in the inquire buffer  occupy the first 8 bytes, inq is a ptr to inqure buf
*/
#define BITFLD(offset,hibit,lobit) ((*(((char*)inq)+offset)) & ((2 << (hibit+1)) - 1)) >> lobit
#define pqt BITFLD(0,7,5)
#define pdt BITFLD(0,4,0)
#define rmb BITFLD(1,7,7)
#define dtq BITFLD(1,6,0)
#define iso BITFLD(2,7,6)
#define ecma BITFLD(2,5,3)
#define ansi BITFLD(2,2,0)
#define aenc BITFLD(3,7,7)
#define trmiop BITFLD(3,6,6)
#define res0 BITFLD(3,5,4)
#define respfmt BITFLD(3,4,0)
#define ailen BITFLD(4,7,0)
#define reladr BITFLD(7,7,7)
#define wide32 BITFLD(7,6,6)
#define wide16 BITFLD(7,5,5)
#define synch BITFLD(7,4,4)
#define link BITFLD( 7,3,3)
#define cmdq BITFLD( 7,1,1)
#define softre BITFLD(7,0,0)

  char  vid[8];        /* vendor ID */
  char  pid[16];       /* product ID */
  char  prl[4];        /* product revision level*/
  char  vendsp[20];      /* vendor specific; typically firmware */
  char  res4[40];        /* reserved for scsi 3, etc. */
  char  vendsp2[159];    /* more vend spec (fill to 255 bytes) */
} inqdata;

#define hex(x) "0123456789ABCDEF" [ (x) & 0xF ]

/* print an array in hex format, only looks OK if nperline a multiple of 4, 
 * but that's OK.  value of space must be 0 <= space <= 3;
 */
void
hprint(unsigned char *s, int n, int nperline, int space, int ascii)
{
        int   i, x, startl;

        for(startl=i=0;i<n;i++)  {
                x = s[i];
                printf("%c%c", hex(x>>4), hex(x));
                if(space)
                        printf("%.*s", ((i%4)==3)+space, "    ");
                if ( i%nperline == (nperline - 1) ) {
                        if(ascii == 1) {
                                putchar('\t');
                                while(startl < i) {
                                        if(isprint(s[startl]))
                                                putchar(s[startl]);
                                        else
                                                putchar('.');
                                        startl++;
                                }
                        }
                        putchar('\n');
                        if(ascii>1 && i<(n-1))  /* hack hack */
                                printf("%.*s", ascii-1, "        ");
                }
        }
        if(space && (i%nperline))
                putchar('\n');
}


/* aenc, trmiop, reladr, wbus*, synch, linkq, softre are only valid if
 * if respfmt has the value 2 (or possibly larger values for future
 * versions of the SCSI standard). */

static char pdt_types[][16] = {
   "Disk", "Tape", "Printer", "Processor", "WORM", "CD-ROM",
   "Scanner", "Optical", "Jukebox", "Comm", "Unknown"
};
#define NPDT (sizeof pdt_types / sizeof pdt_types[0])

void
printinq(inqdata *inq)
{ 
   unsigned char special;
   int neednl = 1;
   printf("%-10s", pdt_types[(pdt < NPDT) ? pdt : NPDT-1]);
      printf("%12.8s", inq->vid);
      printf("%.16s", inq->pid);
      printf("%.4s", inq->prl);
   printf("\n");
      printf("ANSI vers %d, ISO ver: %d, ECMA ver: %d; ",
              ansi, iso, ecma);
      special = *(inq->vid-1 );
      if(respfmt >= 2 || special) {
         if(respfmt < 2)
                 printf("\nResponse format type %d, but has "
                   "SCSI-2 capability bits set\n", respfmt );

         printf("supports: ");
         if(aenc)
                 printf(" AENC");
         if(trmiop)
                 printf(" termiop");
         if(reladr)
                 printf(" reladdr");
         if(wide32)
                 printf(" 32bit");
         if(wide16)
                 printf(" 16bit");
         if(synch)
                 printf(" synch");
         if(synch)
                 printf(" linkedcmds");
         if(cmdq)
                 printf(" cmdqueing");
         if(softre)
                 printf(" softreset");
      }
      if(respfmt < 2) {
         if(special)
                 printf(".  ");
         printf("inquiry format is %s",
                 respfmt ? "SCSI 1" : "CCS");
      }
      printf("\nvendor specific data:\n");
      hprint(inq->vendsp, 20,  16, 1, 1);
      neednl = 0;
      printf("reserved (for SCSI 3) data:\n");
      hprint(inq->res4, 40, 16, 1, 1) ;
      printf("more vendor data\n");
      hprint(inq->vendsp2, 159, 16, 1, 1);
      if(neednl)
         putchar('\n');
}

int ftt_inquire(ftt_descriptor d) {

    static unsigned char 
	inquiry[6] = { 0x12, 0x00, 0x00, 0x00, 255, 0x00};
    inqdata inqbuf ;
    int res;

    ENTERING("ftt_get_inquire");
    CKNULL("ftt_descriptor", d);
    DEBUG2(stderr, "Entering ftt_inquire\n");
    DEBUG3(stderr, "Using SCSI inquire \n");
    res = ftt_open_scsi_dev(d);   
    if(res < 0) return res;
    res = ftt_do_scsi_command(d, "inquire", inquiry, 6, (char *)&inqbuf, sizeof(inqdata), 5, 0);
    if(res < 0) return res;

    printinq(&inqbuf); 

    return res;
}

/*
  Use mode sense 0x3f to get all modesense pages and print them
*/
int ftt_modesense(ftt_descriptor d) {

    static unsigned char 
	mod_sen3f[6] = { 0x1a, 0x00, 0x3f, 0x00, 255, 0x00},
	msbuf [255], *mptr;
    int res;
    int dlen;

    ENTERING("ftt_modesense");
    CKNULL("ftt_descriptor", d);
    DEBUG2(stderr, "Entering ftt_modesense\n");
    DEBUG3(stderr, "Using SCSI Mode sense 0x3f page to get all mode sense\n");
    res = ftt_open_scsi_dev(d);        
    if(res < 0) return res;
    res = ftt_do_scsi_command(d, "Mode sense", mod_sen3f, 6, msbuf, 255, 5, 0);
    if(res < 0) return res;

    dlen = msbuf[0];
    if(dlen < 4)
                return 1;
    mptr = msbuf;

    printf("Header:\n length %#x, med type %#x, dev spcfc %#x, blk desc len %#x\n", 
           msbuf[0], msbuf[1], msbuf[2], msbuf[3]);
    mptr += 4;
    dlen -= 4;
    if(msbuf[3])
       printf("Block Descriptors:\n ");
    while(msbuf[3] && dlen >= 8) {
       hprint(mptr, 8, 8, 1, 0);
       msbuf[3] -= 8;
       dlen -= 8;
       mptr += 8;
    }
    while(dlen >= 3) {
       int len;
       printf("Page %#x, length %#x:\n ", 0x3f & *mptr, mptr[1]);
       len = dlen > (mptr[1]+2) ? mptr[1] : dlen - 2;
       hprint(&mptr[2], mptr[1], 20, 1, 0);
       mptr += len + 2;
       dlen -= len + 2;
    }

    return res;
}
/*
 use log sense 0x0 to get a list of log sense pages, then get each page in turn
 and print it
*/
int ftt_logsense(ftt_descriptor d) {

    static unsigned char 
	logsense0h[10]={0x4d, 0x00, 0x40, 0x00, 0x00,0x00,0x00, 0x10, 0x00, 0x00},
        lslist[255],
	lsbuf [0x1000], *lptr;
    int res;
    int dlen;
    int pagelen, param_code, param_length, param_flags;
    unsigned long param_val;
    unsigned char *pageptr, *param_ptr;

    ENTERING("ftt_get_logsense");
    CKNULL("ftt_descriptor", d);
    DEBUG2(stderr, "Entering ftt_get_logsense\n");
    DEBUG3(stderr, "Using SCSI log sense 0x0 page to get get list of pages\n");
    res = ftt_open_scsi_dev(d);        
    if(res < 0) return res;
    res = ftt_do_scsi_command(d, "log sense", logsense0h, 10, lslist, 255, 5, 0);
    if(res < 0) return res;
    dlen = (lslist[2] << 8) + lslist[3];
    for(lptr=&lslist[4]; dlen-- > 0; lptr++) {
       if (*lptr == 0) 
          continue;
       memset(lsbuf, 0, 8);
       logsense0h[2]= 0x40 | *lptr;		/* cum values for page */
       printf ("Retrieving LOG SENSE PAGE %x \n",*lptr);
       res = ftt_do_scsi_command(d, "log sense", 
                    logsense0h, 10, lsbuf, 0x1000, 5, 0);
       if(res < 0) return res;
       printf ("CODE FLAG LENGTH   VAL BASE 10     VAL HEX - got page %x\n", lsbuf[0]);
       pagelen = (lsbuf[2]<<8) + lsbuf[3];
       pageptr = lsbuf + 4;
       while (pageptr < (lsbuf + pagelen)) {
          param_code   = (*pageptr << 8) + *(pageptr+1);
          param_length = *(pageptr+3);
          param_flags  = *(pageptr+2);
          for (param_ptr = pageptr+4, param_val=0; 
                   param_ptr < pageptr+4+param_length; param_ptr++)
              param_val = (param_val*256) + *param_ptr;
          printf("%4x %4x %4x %16d ", param_code, param_flags, param_length, param_val);
          for (param_ptr = pageptr+4; param_ptr < pageptr+4+param_length; param_ptr++)
              printf("%3x", *param_ptr);
          printf("\n");
          pageptr = pageptr + param_length + 4;
       }
    }
    return res;
}

