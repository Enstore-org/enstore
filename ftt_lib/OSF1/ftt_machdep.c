static char rcsid[] = "#(@)$Id$";
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
ftt_set_compression(ftt_descriptor d, int compression) {

    static char mod_sen[6] = { 0x1a, 0x00, 0x10, 0x00, 28, 0x00},
    		mod_sel[6] = { 0x15, 0x10, 0x00, 0x00, 28, 0x00},
	        buf [28];
    int res;

    DEBUG2(stderr, "Entering ftt_set_compression\n");
    if (0 == geteuid()) {
	if (ftt_get_stat_ops(d->prod_id) & FTT_DO_MS_Px10) {
	    DEBUG3(stderr, "Using SCSI Mode sense 0x10 page to set compression\n");
	    ftt_open_scsi_dev(d);
	    ftt_do_scsi_command(d, "Mode sense", mod_sen, 6, buf, 28, 5, 0);
	    buf[0] = 0;
	    /* we shouldn't be changing density here but it shouldn't hurt */
	    /* yes it will! the setuid program doesn't know which density */
	    /* the parent process set... */
	    /* buf[4] = d->devinfo[d->which_is_default].hwdens; */
	    buf[4 + 8 + 14] = compression;
	    res = ftt_do_scsi_command(d, "Mode Select", mod_sel, 6, buf, 28, 5, 1);
	    ftt_close_scsi_dev(d);
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
		execlp("ftt_suid", "ftt_suid", "-C", s1, d->basename, 0);

	default: /* parent */
		res = ftt_wait(d);
	}
    }
    return res;
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
