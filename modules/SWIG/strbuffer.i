%module strbuffer
%{
#include <sys/types.h>          /* for IRIX+6.2 */
#include <sys/socket.h>
#ifdef linux  /*kludge*/
#ifndef MSG_DONTWAIT
#define MSG_DONTWAIT 0x40
#endif
#endif

#if defined(__osf__)	/* osf1 has MSG_NONBLOCK instead */
#define MSG_DONTWAIT MSG_NONBLOCK
#endif

    int buf_read(int fd, char *buf, int offset, int nbytes){
	return read(fd, buf+offset, nbytes);
    }
    int buf_read_string(char *src, char *buf, int offset, int nbytes){
	strncpy(buf+offset, src, nbytes);
	return nbytes;
    }
    int buf_write(int fd, char *buf, int offset, int nbytes){
	return write(fd, buf+offset, nbytes);
    }
    int buf_send(int sock, char *buf, int offset, int nbytes){
	return send(sock, buf+offset, nbytes, 0);
    }
    int buf_send_dontwait(int sock, char *buf, int offset, int nbytes){
	return send(sock, buf+offset, nbytes, MSG_DONTWAIT);
    }
    int buf_recv(int sock, char *buf, int offset, int nbytes){
	return recv(sock, buf+offset, nbytes, 0);
    }
%}

#ifdef SWIG_VERSION
/* SWIG_VERSION was first used in swig 1.3.11 and has hex value 0x010311. */

%{
/* Include in the generated wrapper file */
typedef char * cptr;
%}
/* Tell SWIG about it */
typedef char * cptr;

%typemap(in) cptr{
        $1 = PyString_AsString($input);
}

#else

%typedef char * cptr;

%typemap(python, in) cptr{
        $target= PyString_AsString($source);
}

#endif

int buf_read(int fd, cptr buf, int offset, int nbytes);
int buf_read_string(cptr src, cptr buf, int offset, int nbytes);
int buf_write(int fd, cptr buf, int offset, int nbytes);
int buf_send(int sock, cptr buf, int offset, int nbytes);
int buf_send_dontwait(int sock, cptr buf, int offset, int nbytes);
int buf_recv(int sock, cptr buf, int offset, int nbytes);

int errno;
