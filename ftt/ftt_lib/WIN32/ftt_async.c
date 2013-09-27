static char rcsid[] = "@(#)$Id$";

#include <stdio.h>
#include <ftt_private.h>


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

    CKOK(d,"ftt_fork",0,0);
    CKNULL("ftt_descriptor", d);

    ftt_eprintf("ftt_fork: is not supported");
    return -1;
}

int
ftt_check(ftt_descriptor d) {
    
    /*
    ** can't use CKOK 'cause it fails when theres an unwaited for task!
    */
    ENTERING("ftt_check");
    CKNULL("ftt_descriptor", d);

	ftt_eprintf("ftt_check: is not supported");
	return -1;
}

int
ftt_wait(ftt_descriptor d) {

    /*
    ** can't use CKOK 'cause it fails when theres an unwaited for task!
    */
    ENTERING("ftt_wait");
    CKNULL("ftt_descriptor", d);
	ftt_eprintf("ftt_wait: is not supported");
	
	return -1;
 
}

int
ftt_report(ftt_descriptor d) {
   
    /*
    ** Can't use CKOK or ENTERING macro, 'cause it clears the errors we 
    ** want to report!
    */
	ftt_eprintf("ftt_report: is not supported");
	return -1;

}
