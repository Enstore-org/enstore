%module strbuffer
%{

#include <sys/socket.h>
#ifdef linux  /*kludge*/
#ifndef MSG_DONTWAIT
#define MSG_DONTWAIT 0x40
#endif
#endif
    int buf_read(int fd, char *buf, int offset, int nbytes){
	return read(fd, buf+offset, nbytes);
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

%typedef char * cptr;

%typemap(python, in) cptr{
        $target= PyString_AsString($source);
}

int buf_read(int fd, cptr buf, int offset, int nbytes);
int buf_write(int fd, cptr buf, int offset, int nbytes);
int buf_send(int sock, cptr buf, int offset, int nbytes);
int buf_send_dontwait(int sock, cptr buf, int offset, int nbytes);
int buf_recv(int sock, cptr buf, int offset, int nbytes);

