static char rcsid[] = "@(#)$Id$";
#include <sys/types.h>
#include <sys/fcntl.h>
#include <sys/ioctl.h>
#include <sys/mtio.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/utsname.h>
#include <string.h>
#include <ctype.h>
#include <ftt_private.h>

int
ftt_status(ftt_descriptor d, int time_out) {
    int res;
    static struct mtget buf;

    ENTERING("ftt_status");
    CKNULL("ftt_descriptor", d);

    ftt_close_scsi_dev(d);
    if (0 > (res = ftt_open_dev(d))) { 
	if( FTT_EBUSY == ftt_errno ){
	    return FTT_BUSY;
	} else {
	    return res;
	}
    }
    res = ioctl(d->file_descriptor,MTIOCGET,&buf);
    res = ftt_translate_error(d,FTT_OPN_STATUS,"ftt_status",
				res,"an MTIOCGET ioctl()",1);

    while ((0 <= res && !(buf.mt_dposn & MT_ONL) && time_out > 0)) {
	sleep(1);
	res = ioctl(d->file_descriptor,MTIOCGET,&buf);
	res = ftt_translate_error(d,FTT_OPN_STATUS,"ftt_status",
				res,"an MTIOCGET ioctl()",1);
	time_out--;
    }
    if (0 > res) {
	if (FTT_EBUSY == ftt_errno) {
	    return FTT_BUSY;
	} else {
	    return res;
	}
    }
    res = 0;
    /* ZZZ should figure out which of these two it is somehow... */
    if( buf.mt_dposn & MT_EOT )     res |= FTT_AEOT;
    if( buf.mt_dposn & MT_EOT )     res |= FTT_AEW;

    if (buf.mt_dposn & MT_BOT )     res |= FTT_ABOT;
    if (buf.mt_dposn & MT_WPROT )   res |= FTT_PROT;
    if (buf.mt_dposn & MT_ONL )     res |= FTT_ONLINE;

    return res;
}

int
ftt_set_compression(ftt_descriptor d, int compression) {
   return 0;
}

int
ftt_set_hwdens(ftt_descriptor d, int hwdens) {
   static struct mtop buf;
   buf->mt_op = MTSETDENSITY;
   buf->mt_count = hwdens;
   res = ioctl(d->file_descriptor,MTIOCTOP,&buf);
   return 0;
}

int
ftt_set_blocksize(ftt_descriptor d, int blocksize) {
    static struct mtop buf;
    static int recursing = 0;
    int res;

    DEBUG1(stderr,"entering ftt_set_hwdens_blocksize %d\n", blocksize);
    if (blocksize != 0) {
	/* 
	** the silly program won't let us set the blocksize to zero,
	** but since that gives us a different device node in 
	** ftt_open_logical, it ends up getting set to zero anyhow.
	*/
	buf.mt_op = MTSCSI_SETBLK;
	buf.mt_count = blocksize;
	res = ioctl(d->file_descriptor, MTIOCTOP, &buf);
	res = ftt_translate_error(d,FTT_OPN_STATUS,
				"an MTIOCTOP/MTSCSI_SETBLK ioctl()", res,
				"an ftt_open_dev",1);
    }
    return res;
}

int
ftt_get_hwdens(ftt_descriptor d, char *devname) {
    int res;
    static struct mtget buf;

    res = ioctl(d->file_descriptor,MTIOCGET,&buf);
    if (res < 0) {
        res = ftt_translate_error(d,FTT_OPN_OPEN,
				"an MTIOCGET ioctl()", res,
				"an ftt_open_dev",1);
   } else {
        res = (buf.mt_dsreg & MT_ST_DENSITY_MASK)>> MT_ST_DENSITY_SHIFT;
   }
   return res;
}
