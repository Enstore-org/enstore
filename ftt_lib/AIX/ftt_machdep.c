static char rcsid[] = "@(#)$Id$";
#include <sys/utsname.h>
#include <string.h>
#include <ctype.h>
#include <stdio.h>
#include <ftt_private.h>
#include <sys/wait.h>


static int
extract_logical(ftt_descriptor d, char *what) {
    FILE *pf;
    static char cmd[512], f1[20],f2[20],f3[20];
    int res;
    char *logical;

    DEBUG2(stderr, "In extract_logical for %s\n", what);
    logical = strrchr(d->basename, '/');
    DEBUG3(stderr,"Looking for last / in %s, found %s\n", d->basename, logical);

    ftt_eprintf("Unable to determine drive density!");
    ftt_errno = FTT_ENODEV;
    res = -1;
    if (0 != logical) {
	logical++;
	sprintf(cmd, "/usr/sbin/lsattr -E -l %s", logical);
	DEBUG3(stderr, "starting %s", cmd);
	pf = popen(cmd, "r");
	if (0 != pf) {
	    while (!feof(pf)){
		fscanf( pf, "%s %s %[^\n]\n", f1, f2, f3);
		DEBUG3(stderr, "got line with %s %s\n", f1, f2);
		if (0 == strcmp(what, f1)) {
		    ftt_eprintf("Ok");
		    ftt_errno = FTT_SUCCESS;
		    DEBUG3(stderr,"ftt_extract_logical: got value of %s\n", f2);
		    res = atoi(f2);
		}
	    }
	    pclose(pf);
	} else {
            ftt_eprintf("ftt_get_hwdens: popen of \"%s\" failed\n",cmd);
	    ftt_errno = FTT_ENOEXEC;
	    return -1;
	}
    } else {
	ftt_eprintf("ftt_extract_logical: unable to find logical device name in %s\n",
			    d->basename);
	ftt_errno = FTT_ENOTSUPPORTED;
	res = -1;
    }
    return res;
}

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

ftt_set_hwdens(ftt_descriptor d, int hwdens) {
    char *logical;
    static char cmd[512];
    int res;
    int minor;
    int which_dens;

    DEBUG2(stderr,"Entering ftt_set_hwdens\n");
    if (0 == geteuid()) {
	if (1 == sscanf(d->devinfo[d->which_is_default].device_name,
			"/dev/rmt%*d.%d", &minor) && minor > 3) {
	    which_dens = 2;
	} else {
	    which_dens = 1;
	}
	logical = strrchr(d->basename, '/');
	DEBUG3(stderr,"Looking for last / in %s, found %s\n", d->basename, logical);
	if (logical != 0) {
	    logical++;
	    sprintf(cmd, "/usr/sbin/chdev -l %s -a density_set_%d=%d >/dev/null\n", 
			 logical,  which_dens, hwdens);
	    DEBUG3(stderr,"Running \"%s\" to change density\n", cmd);
	    res = system(cmd);
	    if ( res != -1 && WIFEXITED(res) ) {
		res = -WEXITSTATUS(res);
	    } else {
		res = -1;
	    }
	    if (res < 0){
		ftt_errno = FTT_ENOEXEC;
		ftt_eprintf("ftt_set_hwdens: \"%s\" exited with code %d\n", 
				cmd, -res);
	    }
	} else {
	    ftt_eprintf("ftt_set_hwdens: unable to find logical device name in %s\n",
				d->basename);
	    ftt_errno = FTT_ENOTSUPPORTED;
	    res = -1;
	}
    } else {
        ftt_close_dev(d);

	switch(ftt_fork(d)){
	static char s1[10];

	case -1:
		res = -1;

	case 0:  /* child */
		fflush(stdout);	/* make async_pf stdout */
		close(1);
		dup2(fileno(d->async_pf_parent),1);
		sprintf(s1, "%d", hwdens);
		if (ftt_debug) {
		 execlp("ftt_suid", "ftt_suid", "-x", "-d", s1, d->devinfo[d->which_is_default].device_name, 0);
		} else {
		 execlp("ftt_suid", "ftt_suid", "-d", s1, d->devinfo[d->which_is_default].device_name, 0);
		}

		ftt_eprintf("ftt_set_hwdens: exec of ftt_suid failed");
		ftt_errno=FTT_ENOEXEC;
		ftt_report(d);

	default: /* parent */
		res = ftt_wait(d);
	}
    }
    return res;
}

ftt_set_compression(ftt_descriptor d, int compression) {
    return ftt_scsi_set_compression(d,compression);
}

ftt_set_blocksize(ftt_descriptor d, int blocksize) {
    char *logical;
    static char cmd[512], f1[20], f2[20], f3[20];
    int res;
    FILE *pf;

    ENTERING("ftt_set_blocksize");
    CKNULL("ftt_descriptor", d);
    if (0 == geteuid()) {
	logical = strrchr(d->basename, '/');
	DEBUG3(stderr,"Looking for last / in %s, found %s\n", d->basename, logical);
	if (logical != 0) {
	    logical++;

	    if (extract_logical(d, "block_size") == blocksize) {
		/* blocksize is already set to that value! */
		return 0;
	    }

	    sprintf(cmd, "/usr/sbin/chdev -l %s -a block_size=%d >/dev/null\n", 
			logical, blocksize);
	    DEBUG3(stderr,"Running \"%s\" to change blocksize\n", cmd);
	    res = system(cmd);
	    if ( res != -1 && WIFEXITED(res) ) {
		res = -WEXITSTATUS(res);
	    } else {
		res = -1;
	    }
	    if (res < 0){
		ftt_errno = FTT_ENOEXEC;
		ftt_eprintf("ftt_set_blocksize: \"%s\" exited with code %d\n", 
				cmd, res);
		return res;
	    }
	} else {
	    /* punt -- the drive isn't a /dev/rmt drive, so just return */
	    return 0;
	}
    } else {
        ftt_close_dev(d);
	switch(ftt_fork(d)){
	static char s1[10];

	case -1:
		return -1;

	case 0:  /* child */
		fflush(stdout);	/* make async_pf stdout */
		close(1);
		dup2(fileno(d->async_pf_parent),1);
		sprintf(s1, "%d", blocksize);
		execlp("ftt_suid", "ftt_suid", "-b", s1, d->basename, 0);

		ftt_eprintf("ftt_set_blocksize: exec of ftt_suid failed");
		ftt_errno=FTT_ENOEXEC;
		ftt_report(d);

	default: /* parent */
		res = ftt_wait(d);
	}
    }
    return res;
}

int
ftt_get_hwdens(ftt_descriptor d, char *devname) {
    int m, n = 0;

    sscanf(devname, "/dev/rmt%d.%d", &m, &n);
    DEBUG3(stderr,"ftt_get_hwdens: m, n are %d, %d\n", m, n);
    if (n < 4) {
        return extract_logical(d, "density_set_1");
    } else {
        return extract_logical(d, "density_set_2");
    }
}
