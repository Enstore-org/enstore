/* 
   $Id$
*/

#include "volume_import.h"


int
timestamp(char *buf)
{
    struct timeval tv;
    
    if (gettimeofday(&tv, (struct timezone *)0)){
	fprintf(stderr,"%s: ", progname);
	perror("gettimeofday");
	return -1;
    }
    
    sprintf(buf, "%d.%03d", (int)tv.tv_sec, (int)(tv.tv_usec/1000));
    return 0;
}


    
