static char rcsid[] = "@(#)$Id$";
#include <sys/utsname.h>
#include <string.h>
#include <ctype.h>
#include <stdio.h>
#include <ftt_private.h>
#include <sys/types.h>
#include <sys/ioctl.h>
#include <sys/mtio.h>
#include <sys/devio.h>

int
ftt_status(ftt_descriptor d, int time_out) {
    int res;
    static struct devget buf;

    ENTERING("ftt_status");
    CKNULL("ftt_descriptor", d);

    if (0 > (res = ftt_open_dev(d))) {
        if( FTT_EBUSY == ftt_errno ){
            return FTT_BUSY;
        } else {
            return res;
        }
    }
    res = ioctl(d->file_descriptor,DEVIOCGET,&buf);
    res = ftt_translate_error(d,FTT_OPN_STATUS,"ftt_status",
                                res,"an DEVIOCGET ioctl()",1);

    while ((0 <= res && (buf.stat & DEV_OFFLINE) && time_out > 0) ||
           (0 > res && FTT_BUSY == ftt_errno && time_out > 0)) {
        sleep(1);
        res = ioctl(d->file_descriptor,DEVIOCGET,&buf);
        res = ftt_translate_error(d,FTT_OPN_STATUS,"ftt_status",
                                  res,"an DEVIOCGET ioctl()", 1);

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
    if( buf.stat & DEV_EOM )       res |= FTT_AEOT;
    if( buf.stat & DEV_EOM )       res |= FTT_AEW;

    if (buf.stat & DEV_BOM )       res |= FTT_ABOT;
    if (buf.stat & DEV_WRTLCK )    res |= FTT_PROT;
    if (!(buf.stat & DEV_OFFLINE)) res |= FTT_ONLINE;

    return res;
}

int
ftt_set_hwdens(ftt_descriptor d, int hwdens) {
    /* ignore hwdens, 'cause we opened the right device node */
   return 0;
}


int
ftt_set_blocksize(ftt_descriptor d, int blocksize) {
    static struct mtop buf;
    static int recursing = 0;
    int res;

    return 0;
}

int
ftt_get_hwdens(ftt_descriptor d, char *devname) {
    int res;

    res = d->devinfo[d->which_is_default].hwdens;
    return res;
}

ftt_set_compression(ftt_descriptor d, int compression) {
    ftt_scsi_set_compression(d,compression);
}
