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

    while ((0 <= res && !(buf.mt_dposn & MT_ONL) && time_out > 0) ||
	   (0 > res && FTT_BUSY == ftt_errno && time_out > 0)) {
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
    if (buf.mt_dposn & MT_FMK )     res |= FTT_AFM;
    if (buf.mt_dposn & MT_WPROT )   res |= FTT_PROT;
    if (buf.mt_dposn & MT_ONL )     res |= FTT_ONLINE;

    if (res & FTT_ABOT) {
	d->current_file = 0;
	d->current_block = 0;
	d->current_valid = 1;
    }
    return res;
}

int
ftt_set_hwdens_blocksize(ftt_descriptor d, int hwdens, int blocksize) {
    static struct mtop buf;
    static int recursing = 0;
    int res;

    /* ignore hwdens, 'cause we opened the right device node */

    if (recursing) {
	/* 
	** we need the device open before we do this, so we call
	** ftt_open_dev. of course, it is going to call *us* again.
	** so we have this recursive call bail-out. 
	*/
	return 0;
    }
    DEBUG1(stderr,"entering ftt_set_hwdens_blocksize %x %d\n", 
	   hwdens, blocksize);
    recursing = 1;
    if (0 > (res = ftt_open_dev(d))) { 
	return res;
    }
    recursing = 0;
    if (blocksize != 0) {
	/* 
	** the silly program won't let us set the blocksize to zero,
	** but since that gives us a different device node in 
	** ftt_open_logical, it ends up getting set to zero anyhow.
	*/
	buf.mt_op = MTSCSI_SETFIXED;
	buf.mt_count = blocksize;
	res = ioctl(d->file_descriptor, MTSPECOP, &buf);
	res = ftt_translate_error(d,FTT_OPN_STATUS,
				"an MTSPECOP/MTSCSI_SETFIXED ioctl()", res,
				"an ftt_open_dev",1);
    }
    return res;
}
