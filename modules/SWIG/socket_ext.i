/* $Id$ */

%module socket_ext

%{
	#include <sys/socket.h>
	#include <errno.h>
	int bindtodev(int fd, char *dev){
	    int optlen;
	    int status=0;
	    optlen=strlen(dev)+1;
	    errno=0;
#ifdef SO_BINDTODEVICE
	    status=setsockopt(fd, SOL_SOCKET, SO_BINDTODEVICE, 
			      dev, optlen);
	    return status?errno:0;
#else
	    return ENOSYS;
#endif	   
	}
%}

int bindtodev(int fd, char *dev);




