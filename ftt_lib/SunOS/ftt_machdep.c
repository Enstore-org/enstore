static char rcsid[] = "@(#) $Id$";


#ifndef WIN32
#include <unistd.h>
#endif

#include <stdlib.h>
#include <sys/utsname.h>
#include <string.h>
#include <ctype.h>
#include <stdio.h>
#include <ftt_private.h>
#include <sys/mtio.h>

int ftt_open_io_dev(ftt_descriptor d);
int ftt_scsi_set_compression(ftt_descriptor d, int compression);

int
ftt_status(ftt_descriptor d, int time_out) {
    static ftt_stat block;
    int res;
    char *p;

    res = ftt_get_stats(d,&block);
    if (res < 0) {
	if (ftt_errno == FTT_EBUSY) {
	    return FTT_BUSY;
	} else {
	    return res;
	}
    }
       while (time_out > 0 ) {
                p = ftt_extract_stats(&block, FTT_READY);
                if ( p && atoi(p)) {
                        break;
                }
                sleep(1);
                time_out--;
                res = ftt_get_stats(d,&block);
        }

    res = 0;
    p = ftt_extract_stats(&block, FTT_BOT);
    if ( p && atoi(p)) {
	DEBUG3(stderr,"setting ABOT flag\n");
	res |= FTT_ABOT;
    }
    p = ftt_extract_stats(&block, FTT_EOM);
    if ( p && atoi(p)) {
	DEBUG3(stderr,"setting AEOT flag\n");
	res |= FTT_AEOT;
	res |= FTT_AEW;
    }
    p = ftt_extract_stats(&block, FTT_WRITE_PROT);
    if ( p && atoi(p)) {
	DEBUG3(stderr,"setting PROT flag\n");
	res |= FTT_PROT;
    }
    p = ftt_extract_stats(&block, FTT_READY);
    if ( p && atoi(p)) {
	DEBUG3(stderr,"setting ONLINE flag\n");
	res |= FTT_ONLINE;
    }
    return res;
}

int
ftt_set_hwdens(ftt_descriptor d, int hwdens) {
    /* ignore hwdens, 'cause we opened the right device node */
   return 0;
}

int 
ftt_set_compression(ftt_descriptor d, int compression) {
  return 0;
  /*    return ftt_scsi_set_compression(d, compression); */
}

int
ftt_set_blocksize(ftt_descriptor d, int blocksize) {

  return 0;
}

int
ftt_get_hwdens(ftt_descriptor d, char *devname) {
    int res;

    res = d->devinfo[d->which_is_default].hwdens;
    return res;
}
