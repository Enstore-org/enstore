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

    while ((0 <= res && !(GMT_ONLINE(buf.mt_gstat)) && time_out > 0)) {
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
    if( GMT_EOT(buf.mt_gstat))     res |= FTT_AEOT;
    if( GMT_EOT(buf.mt_gstat))     res |= FTT_AEW;

    if (GMT_BOT(buf.mt_gstat))     res |= FTT_ABOT;
    if (GMT_WR_PROT(buf.mt_gstat)) res |= FTT_PROT;
    if (GMT_ONLINE(buf.mt_gstat))  res |= FTT_ONLINE;
    DEBUG2(stderr,"ftt_status: returning %x\n", res );

    return res;
}


int
ftt_set_compression(ftt_descriptor d, int compression) {
   return 0;
}

int
ftt_set_hwdens(ftt_descriptor d, int hwdens) {
   struct mtop buf;
   static int recursing = 0;
   int res;
   
   if ( !recursing ) {
       recursing = 1;
       if (ftt_open_dev(d)) {
            recursing = 0;

	    buf.mt_op = MTSETDENSITY;
	    buf.mt_count = hwdens;
	    res = ioctl(d->file_descriptor, MTIOCTOP, &buf);
#ifdef 1
	    res = ftt_translate_error(d,FTT_OPN_STATUS,
				    "an MTIOCTOP/MTSETDENSITY ioctl()", res,
				"an ftt_open_dev",1);
	    return res;
#else
	    return 0;
#endif
       }
   }
   return -1;
}

int
ftt_set_blocksize(ftt_descriptor d, int blocksize) {
    static struct mtop buf;
    static int recursing = 0;
    int res;

    if (recursing) {
	/* 
	** we need the device open before we do this, so we call
	** ftt_open_dev. of course, it is going to call *us* again.
	** so we have this recursive call bail-out. 
	*/
	return 0;
    }
    DEBUG1(stderr,"entering ftt_set_hwdens_blocksize %d\n", blocksize);
    recursing = 1;
    if (0 > (res = ftt_open_dev(d))) { 
	return res;
    }
    recursing = 0;

    buf.mt_op = MTSETBLK;
    buf.mt_count = blocksize;
    res = ioctl(d->file_descriptor, MTIOCTOP, &buf);

#ifdef 1
    res = ftt_translate_error(d,FTT_OPN_STATUS,
				"an MTIOCTOP/MTSETBLK ioctl()", res,
				"an ftt_open_dev",1);
    return res;
#else
    return 0;
#endif
}

int
ftt_get_hwdens(ftt_descriptor d, char *devname) {
    static int recursing = 0;
    struct mtget buf;
    int res;

    if (recursing) {
	/* 
	** we need the device open before we do this, so we call
	** ftt_open_dev. of course, it is going to call *us* again.
	** so we have this recursive call bail-out. 
	*/
	return 0;
    }
    recursing = 1;
    res = ftt_open_dev(d);
    recursing = 0; 	
    if (res < 0) return res;

    res = ioctl(d->file_descriptor, MTIOCGET, &buf);
#ifdef 1
    res = ftt_translate_error(d,FTT_OPN_STATUS,
				"an MTIOCGET ioctl()", res,
				"an ftt_open_dev",1);
    if (res < 0) return res;
#else
    return 0;
#endif

    res =  (buf.mt_dsreg >> 24) & 0xff;
    DEBUG2(stderr,"ftt_get_hwdens -- returning %d\n", res);
    return res;
}

