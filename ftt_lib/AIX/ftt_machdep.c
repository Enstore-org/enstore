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

    res = ftt_get_stats(d,&block);
    if (res < 0) {
	if (ftt_errno == FTT_EBUSY) {
	    return FTT_BUSY;
	} else {
	    return res;
	}
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

ftt_set_hwdens_blocksize(ftt_descriptor d, int hwdens, int blocksize) {
    char *logical;
    static char cmd[512];
    int res;

    DEBUG2(stderr,"Entering ftt_set_hwdens_blocksize\n");
    if (0 == geteuid()) {
	logical = strrchr(d->basename, '/');
	DEBUG3(stderr,"Looking for last / in %s, found %s\n", d->basename, logical);
	if (logical != 0) {
	    logical++;
	    sprintf(cmd, "chdev -l %s -a block_size=%d -a density_set_2=%d >/dev/null 2>&1\n", 
			logical, blocksize, hwdens);
	    DEBUG3(stderr,"Running \"%s\" to change blocksize\n", cmd);
	    system(cmd);
	}
    } else {
	switch(ftt_fork(d)){
	static char s1[10], s2[10];

	case -1:
		return -1;

	case 0:  /* child */
		fflush(stdout);	/* make async_pf stdout */
		close(1);
		dup(fileno(d->async_pf));
		sprintf(s1, "%d", hwdens);
		sprintf(s2, "%d", blocksize);
		execlp("ftt_suid", "ftt_suid", "-b", s1, s2, d->basename, 0);

	default: /* parent */
		res = ftt_wait(d);
	}
    }
}
