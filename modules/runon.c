/* runon.c -- python interface to sgi runon function
 *
 * runon(n) -- force current process to run on cpu n
 * runonpid(n, p) -- force process pid to run on cpu n
 *
 * These routines are only available for sgi/irix machines. On other
 * platform, an error is generated.
 */

#ifdef __sgi
#include <sys/sysmp.h>
#include <sys/sysinfo.h>
#include <sys/systeminfo.h>
#endif

#include <errno.h>

/* sgi_runon -- python interface to runon */

int runon(cpu)
int cpu;
{
#ifdef __sgi
	if (sysmp(MP_MUSTRUN, cpu) == -1)
	    return errno;
	else
	    return 0;
#else
	return(ENOSYS);
#endif
}

pidrunon(cpu, pid)
int cpu;
int pid;
{
#ifdef __sgi
	if(sysmp(MP_MUSTRUN_PID, cpu, pid) == -1)
	    return(errno);
	else
	    return(0);
#else
	return(ENOSYS);
#endif
}
