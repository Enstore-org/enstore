#include <stdio.h>
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

    static char pidbuf[32];
    int fds[2];
    int res, len;

    CKOK(d,"ftt_fork",0,0);
    CKNULL("ftt_descriptor", d);

    res = pipe(fds);
    DEBUG3(stderr, "pipe returns %d and %d\n", fds[0], fds[1]);
    if (0 == res) {
	switch (res = fork()) {

	case 0:    /* child, fork again so no SIGCLD, zombies, etc. */
	    if(fork() == 0){
		   /* grandchild, send our pid up the pipe */
	        d->async_fd = fds[1];
	        close(fds[0]);
		sprintf(pidbuf,"%d", getpid());
		write(d->async_fd, pidbuf, strlen(pidbuf));
	    } else {
		exit(0);
	    }
	    break;

	default:     /* parent */
	    d->async_fd = fds[0];
	    len = read(d->async_fd, pidbuf, sizeof(pidbuf));
	    pidbuf[len] = 0;
	    d->async_pid = atoi(pidbuf);
	    DEBUG3(stderr,"got pid %d\n", d->async_pid);
	    close(fds[1]);
	    wait(0);
	    break;

	case -1:
	    res = ftt_translate_error(d, FTT_OPN_ASYNC, "ftt_fork", res, "a fork() system call to\n\\
 	create a process to perform asynchronous actions", 1);
	    break;
	}
    } else {
	res = ftt_translate_error(d, FTT_OPN_ASYNC, "ftt_fork", res, "a pipe() system call to\n\\
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
    static char buf[512], *p;
    int len;

    ENTERING("ftt_wait");
    CKNULL("ftt_descriptor", d);

    DEBUG3(stderr,"async_pid is %d", d->async_pid );
    DEBUG3(stderr,"async_fd is %d\n", d->async_fd );
    if (0 != d->async_pid ) {
	len = read(d->async_fd, buf, 512);
	if ( len > 0 ) {
	    buf[len] = 0;
	    DEBUG3(stderr,"read buffer is %d bytes, contains:\n%s", len, buf);
	    close(d->async_fd);
	    d->async_pid = 0;
	    d->async_fd = d->async_pid = 0;
	    p = strchr(buf,'\n');
	    if(p != 0) {
		*p = 0;
		ftt_eprintf("%s", p+1);
		ftt_errno = atoi(buf);
		DEBUG3(stderr,"picked out errno %d\n error string:\n%s\n",
			ftt_errno, ftt_eprint_buf);
		if (ftt_errno != 0) {
		    return -1;
		} else {
		    return 0;
		}
	    }
	    DEBUG3(stderr,"couldn't find newline!\n");
	    ftt_eprintf("unable to parse output from background process\n");
	    ftt_errno = FTT_EUNRECOVERED;
	    d->unrecovered_error = 1;
	    return -1;
	} else {
	    ftt_eprintf("unable to read from background process pipe\n");
	    ftt_errno = FTT_EUNRECOVERED;
	    d->unrecovered_error = 1;
	    return -1;
	}
    } else {
       ftt_eprintf("unable to rondezvous with background process\n");
       ftt_errno = FTT_ENXIO;
       return -1;
    }
}

void
ftt_report(ftt_descriptor d) {
    int e; char *p;

    ENTERING("ftt_report");
    VCKNULL("ftt_descriptor", d);

    fflush(stdout);
    close(1);
    dup(d->async_fd);
    p = ftt_get_error(&e);
    printf("%d\n%s", e, p);
    exit(0);
}
