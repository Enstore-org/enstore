#include <stdio.h>
#include <unistd.h>
#include <wait.h>
#include <ftt_private.h>
#include <ftt_mtio.h>
#include <string.h>

int ftt_async_level;

/* set max async level */
/*
** this process starts an asynchronous process to do the actual operation
** if neccesary.  If successful it returns the result of fork().
*/
int
ftt_fork(ftt_descriptor d) {
    int fds[2];
    int res;

    CKOK(d,"ftt_fork",0,0);
    CKNULL("ftt_descriptor", d);

    res = pipe(fds);
    DEBUG3(stderr, "pipe returns %d and %d\n", fds[0], fds[1]);
    if (0 == res) {
	switch (res = fork()) {

	case 0:    /* child, fork again so no SIGCLD, zombies, etc. */
	    if(fork() == 0){
		   /* grandchild, send our pid up the pipe */
	        close(fds[0]);
	        d->async_pf = fdopen(fds[1],"w");
		setlinebuf(d->async_pf);
		fprintf(d->async_pf,"%d\n", (int)getpid());
		fflush(d->async_pf);
	    } else {
		exit(0);
	    }
	    break;

	default:     /* parent */
	    close(fds[1]);
	    d->async_pf = fdopen(fds[0],"r");
	    setlinebuf(d->async_pf);
	    res = fscanf(d->async_pf, "%d", &d->async_pid);
	    if (res == 0) {
		DEBUG3(stderr, "retrying read of pid from pipe\n");
	        res = fscanf(d->async_pf, "\n%d", &d->async_pid);
	    }
	    DEBUG3(stderr,"got pid %d\n", d->async_pid);
	    wait(0);
	    break;

	case -1:
	    res = ftt_translate_error(d, FTT_OPN_ASYNC, "ftt_fork", res, "a fork() system call to\n\
 	create a process to perform asynchronous actions", 1);
	    break;
	}
    } else {
	res = ftt_translate_error(d, FTT_OPN_ASYNC, "ftt_fork", res, "a pipe() system call to\n\
	create a channel to return asynchronous results", 1);
    }
    return res;
}

int
ftt_check(ftt_descriptor d) {
    
    CKOK(d,"ftt_check",0,0);
    CKNULL("ftt_descriptor", d);

    DEBUG3(stderr,"looking for pid %d\n", d->async_pid);
    if (d->async_pid != 0 && 0 == kill(d->async_pid, 0)) {
	ftt_eprintf("ftt_check called with background process still running\n");
	ftt_errno = FTT_EBUSY;
	return -1;
    } else {
       return 0;
    }
}

int
ftt_wait(ftt_descriptor d) {
    int len;

    ENTERING("ftt_wait");
    CKNULL("ftt_descriptor", d);

    DEBUG3(stderr,"async_pid is %d", d->async_pid );
    DEBUG3(stderr,"async_pf is %lx\n", (long)d->async_pf );
    ftt_eprintf("unable to rondezvous with background process\n");
    ftt_errno = FTT_ENXIO;
    if (0 != d->async_pid ) {
	fscanf(d->async_pf, "\n%d\n", &ftt_errno);
	len = fread(ftt_eprint_buf, FTT_EPRINT_BUF_SIZE - 1, 1, d->async_pf);
	if ( len > 0 ) {
	    ftt_eprint_buf[len] = 0;
	}
	if (ftt_errno != 0) {
	    return -1;
	} else {
	    return 0;
	}
    } else {
       return -1;
    }
}

void
ftt_report(ftt_descriptor d) {
    int e; char *p;

    ENTERING("ftt_report");
    VCKNULL("ftt_descriptor", d);

    p = ftt_get_error(&e);
    fprintf(d->async_pf, "%d\n%s", e, p);
    exit(0);
}
