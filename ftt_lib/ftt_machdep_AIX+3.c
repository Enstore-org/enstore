#include <sys/utsname.h>
#include <string.h>
#include <ctype.h>
#include <stdio.h>
#include <ftt_private.h>


int
ftt_status(ftt_descriptor d, int time_out) {
    static ftt_stat block;
    int res;
    char *p;

    res = ftt_get_stat(d,&block);
    if (res < 0) {
	if (ftt_errno == FTT_EBUSY) {
	    return FTT_BUSY;
	} else {
	    return res;
	}
    }
    res = 0;
    p = ftt_extract_stat(&block, FTT_BOT);
    if ( p && atoi(p)) {
	res |= FTT_ABOT;
    }
    p = ftt_extract_stat(&block, FTT_EOM);
    if ( p && atoi(p)) {
	res |= FTT_AEOT;
	res |= FTT_AEW;
    }
    p = ftt_extract_stat(&block, FTT_WRITE_PROT);
    if ( p && atoi(p)) {
	res |= FTT_PROT;
    }
    p = ftt_extract_stat(&block, FTT_READY);
    if ( p && atoi(p)) {
	res |= FTT_ONLINE;
    }
}

ftt_set_hwdens_blocksize(ftt_descriptor d, int hwdens, int blocksize) {
    char *logical;

    logical = strrchr(d->basename, '/');
    if (logical != 0) {
	logical++
	sprintf(cmd, "chdev -l %s -a blocksize=%d\n", logical, blocksize);
	system(cmd);
    }
}
