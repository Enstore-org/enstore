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
   return ftt_stats_status(d,time_out);
}

int
ftt_set_compression(ftt_descriptor d, int compression) {
   return 0;
}
int
ftt_set_hwdens(ftt_descriptor d, int hwdens) {
    /* ignore hwdens, 'cause we opened the right device node */
   return 0;
}
int
ftt_set_blocksize(ftt_descriptor d, int blocksize) {
    static struct mtop buf;
    int res=0;


    DEBUG1(stderr,"entering ftt_set_hwdens_blocksize %x %d\n", 
	   hwdens, blocksize);
    recursing = 1;
    if (0 > (res = ftt_open_io_dev(d))) { 
	return res;
    }
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

int
ftt_get_hwdens(ftt_descriptor d, char *devname) {
    int res;

    res = d->devinfo[d->which_is_default].hwdens;
    return res;
}
