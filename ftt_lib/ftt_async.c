static char rcsid[] = "@(#)$Id$";

#include <stdlib.h>
#include <stdio.h>
#include <sys/wait.h> 
#include <ftt_private.h>
#include <string.h>

#ifndef WIN32
#include <signal.h>
#include <unistd.h>
#endif

/*
** this process starts an asynchronous process to do ftt operations
** and sets up the pipe for ftt_report()ing status.
** If successful it returns the result of fork().
**
** this is sort of like popen(), except we don't exec anything else
** in the child.
*/
int
ftt_fork(ftt_descriptor d) {
    int fds[2];
    int res;

    CKOK(d,"ftt_fork",0,0);
    CKNULL("ftt_descriptor", d);

    ftt_close_dev(d);
    res = pipe(fds);
    DEBUG3(stderr, "pipe returns %d and %d\n", fds[0], fds[1]);
    if (0 == res) {
	switch (res = fork()) {

	case 0:    /* child, fork again so no SIGCLD, zombies, etc. */
	    if(fork() == 0){
		   /* grandchild, send our pid up the pipe */
	        close(fds[0]);
	        d->async_pf_parent = fdopen(fds[1],"w");
		fprintf(d->async_pf_parent,"%d\n", (int)getpid());
		fflush(d->async_pf_parent);
	    } else {
		exit(0);
	    }
	    break;

	default:     /* parent */
	    close(fds[1]);
	    waitpid(res,0,0);
	    d->async_pf_child = fdopen(fds[0],"r");
	    res = fscanf(d->async_pf_child, "%d", &d->async_pid);
	    if (res == 0) {
		DEBUG3(stderr, "retrying read of pid from pipe\n");
	        res = fscanf(d->async_pf_child, "\n%d", &d->async_pid);
	    }
	    DEBUG3(stderr,"got pid %d\n", d->async_pid);
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
    
    /*
    ** can't use CKOK 'cause it fails when theres an unwaited for task!
    */
    ENTERING("ftt_check");
    CKNULL("ftt_descriptor", d);

    DEBUG3(stderr,"looking for pid %d\n", d->async_pid);
    if (d->async_pid != 0 && 0 == kill(d->async_pid, 0)) {
	ftt_eprintf("ftt_check: background process still running\n");
	ftt_errno = FTT_EBUSY;
	return -1;
    } else {
       return 0;
    }
}

int
ftt_wait(ftt_descriptor d) {
    int len;

    /*
    ** can't use CKOK 'cause it fails when theres an unwaited for task!
    */
    ENTERING("ftt_wait");
    CKNULL("ftt_descriptor", d);

    DEBUG3(stderr,"async_pid is %d", d->async_pid );
    DEBUG3(stderr,"async_pf is %lx\n", (long)d->async_pf_child );
    ftt_eprintf("ftt_wait: unable to rendezvous with background process %d, ftt_errno FTT_ENXIO",
		d->async_pid);
    if (0 != d->async_pid ) {
	fscanf(d->async_pf_child, "\n%d\n", &ftt_errno);
	DEBUG3(stderr,"scanf of child pipe yeilds errno %d\n", ftt_errno);
	len = fread(ftt_eprint_buf, 1, FTT_EPRINT_BUF_SIZE - 1, d->async_pf_child);
	DEBUG3(stderr,"fread of child pipe returns %d\n", len);
	if ( len > 0 ) {
	    ftt_eprint_buf[len] = 0;
	}
	fclose(d->async_pf_child);
	d->async_pf_child = 0;
	d->async_pid = 0;
	if (ftt_errno != 0) {
	    return -1;
	} else {
	    return 0;
	}
    } else {
       ftt_eprintf("ftt_wait: there is no background process, ftt_errno FTT_ENXIO");
       ftt_errno = FTT_ENXIO;
       return -1;
    }
}

int
ftt_report(ftt_descriptor d) {
    int e; char *p;

    /*
    ** Can't use CKOK or ENTERING macro, 'cause it clears the errors we 
    ** want to report!
    */
    char *_name = "ftt_report";			
    DEBUG1(stderr,"Entering ftt_report");
    CKNULL("ftt_descriptor", d);

    if (d->async_pf_parent) {
	p = ftt_get_error(&e);
	p = strdup(p); /* don't lose messages! */
	ftt_close_dev(d);
	DEBUG3(stderr,"Writing ftt_errno %d  message %s to pipe\n", e, p);
	fprintf(d->async_pf_parent, "%d\n%s", e, p);
	fflush(d->async_pf_parent);
	exit(0);
    } else {
	ftt_eprintf("ftt_report: there is no connection to a parent process, ftt_errno FTT_ENXIO");
	ftt_errno = FTT_ENXIO;
	return -1;
    }
    return 0;
}
