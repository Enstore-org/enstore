/* $Id$
 functions for controlling the tape drive
*/

#include "volume_import.h"

int open_tape(mode){
    verbage("%s: opening %s\n", progname, tape_device);

    if (!tape_device){
	fprintf(stderr,"%s: tape_device not specified\n", progname);
	return -1;
    }
    tape_fd = open(tape_device, mode);
    if (tape_fd<0){
	fprintf(stderr, "%s: ", progname);
	perror(tape_device);
	return -1;
    }
    return set_variable_blocksize();
}

int close_tape(){
    verbage("%s: closing %s\n", progname, tape_device);

    if (close(tape_fd)){
	fprintf(stderr,"%s: close_tape", progname);
	perror(tape_device);
	return -1;
    }
    return 0;
}

static int
check_tape_fd(char *msg){
    if (tape_fd<0){
	fprintf(stderr,"%s: %s: tape device not open\n", progname, msg);
	return -1;
    }
    return 0;
}

static int
check_tape_ioctl(int op, int count, char *msg){
    struct mtop mtop;

    if (check_tape_fd(msg))
	return -1;

    verbage("tape ioctl %d %d (%s)\n", op, count, msg);
    
    mtop.mt_op = op;
    mtop.mt_count = count;
    if (ioctl(tape_fd, MTIOCTOP, &mtop)){
	fprintf(stderr,"%s: %s", progname, msg);
	perror(tape_device);
    }
    return 0;
}

int 
rewind_tape(){
    verbage("%s: rewinding %s\n", progname, tape_device);
    return check_tape_ioctl(MTREW, 0, "rewind");
}

int 
set_variable_blocksize(){
    verbage("%s: setting variable blocksize on %s\n", progname,
			tape_device);
    return check_tape_ioctl(MTSETBLK, 0, "set block size");
}

int write_eof(int n){
    verbage("%s: writing %d eof markers on %s\n", progname,
			n, tape_device);
    return check_tape_ioctl(MTWEOF, n, "write eof");
}

int
write_tape(char *data, int count){
    int nbytes;
    int nwritten;
    int tot=0;

    verbage("%s: writing %d bytes to %s...", 
	      progname, count, tape_device);

    check_tape_fd("write");

    while (count){
	nbytes = count>blocksize ? blocksize:count;
	if ( (nwritten=write(tape_fd, data, nbytes)) != nbytes){
	    fprintf(stderr,"%s: short write %d!=%d\n", progname, 
		    nwritten, nbytes);
	    if (nwritten<=0){
		tot=nwritten; /*return*/
		fprintf(stderr, "%s: write_tape", progname);
		perror(tape_device);
		break;
	    }
	}
	data+=nwritten;
	tot+=nwritten;
	count-=nwritten;
    }
    verbage(" wrote %d bytes\n", tot);
    return tot;
}

int
read_tape(char *data, int count){
    int nbytes;
    int nread;
    int tot=0;

    verbage("%s: reading %d bytes from %s...", 
	      progname, count, tape_device);
    
    check_tape_fd("read");

    while (count){
	nbytes = count>blocksize ? blocksize:count;
	if ( (nread=write(tape_fd, data, nbytes)) != nbytes){
	    fprintf(stderr,"%s: short read %d!=%d\n", progname, 
		    nread, nbytes);
	    if (nread<=0){
		fprintf(stderr, "%s: read_tape", progname);
		perror(tape_device);
		break;
	    }
	}
	data+=nread;
	tot+=nread;
	count-=nread;
    }
    verbage(" read %d bytes\n", tot);
    return tot;
}

int 
write_vol1_header()
{
    char buf[81];
    int i,n;

    strcpy(buf, "VOL1");
    strcpy(buf+4, volume_label); /*volume label should already have been checked
				  * for proper length */
    n = strlen(volume_label);
    for (i=4+n; i<81; ++i)
	buf[i]=' ';
    buf[79]='0';
    buf[80]=0;

    verbage("writing VOL1 header %s\n", buf);

    if (write_tape(buf,80) != 80){
	fprintf(stderr, "%s: can't write tape label\n", progname);
	return -1;
    }
    return 0;
}


int 
write_eot1_header(int fileno)
{
    char buf[81];
    int i,n;

    sprintf(buf, "EOT1%07d", fileno);
    strcpy(buf+11, volume_label); /*volume label should already have been checked
				  * for proper length */
    n = strlen(volume_label);
    for (i=11+n; i<81; ++i)
	buf[i]=' ';
    buf[79]='0';
    buf[80]=0;

    verbage("writing EOT header %s\n", buf);
    
    if (write_tape(buf,80) != 80){
	fprintf(stderr, "%s: can't write tape label\n", progname);
	return -1;
    }
    return 0;   
}

