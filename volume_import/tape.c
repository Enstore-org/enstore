/* $Id$
 functions for controlling the tape drive
*/

#include "volume_import.h"

int open_tape(){
    verbage("%s: opening %s\n", progname, tape_device);

    if (!tape_device){
	fprintf(stderr,"%s: tape_device not specified\n", progname);
	return -1;
    }
    tape_fd = open(tape_device, 2);
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
	return -1;
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

int 
write_eof_marks(int n){
    verbage("%s: writing %d eof markers on %s\n", progname,
			n, tape_device);
    return check_tape_ioctl(MTWEOF, n, "write eof");
}

int
skip_eof_marks(int n){
    verbage("%s: skipping %d eof markers on %s\n", progname,
			n, tape_device);
    if (n==0) return 0;
    if (n>0)
	return check_tape_ioctl(MTFSF, n, "skip file mark");
    else
	return check_tape_ioctl(MTBSF, -n, "skip file mark backward");
} 

int
skip_records(int n){
    if (n==0)
	return 0;
    if (n<0){
	verbage("%s: skipping %d records backward on %s\n", progname,
		-n, tape_device);
	return check_tape_ioctl(MTBSR, -n, "skip record backward");
    } else {
	verbage("%s: skipping %d records on %s\n", progname, n, tape_device);
	return check_tape_ioctl(MTFSR, n, "skip record");
    }
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

    while (count>0){
	nbytes = count>blocksize ? blocksize:count;
	if ( (nread=read(tape_fd, data, nbytes)) != nbytes){
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
    
    sprintf(buf, "EOT%07d", fileno);
    strcpy(buf+10, volume_label); /*volume label should already have been checked
				   * for proper length */
    n = strlen(volume_label);
    for (i=10+n; i<81; ++i)
	buf[i]=' ';
    buf[79]='0';
    buf[80]=0;
    
    
    verbage("writing EOT1 header %s\n", buf);
    verbage("writing an extra file mark\n");
    if (write_eof_marks(1))
	return -1;
    if (write_tape(buf,80) != 80){
	fprintf(stderr, "%s: can't write tape label\n", progname);
	return -1;
    }
    verbage("positioning tape\n");
    return skip_records(-1);
}

int 
read_tape_label(char *label, int *type, int *fileno)
{
    char buf[80];
    char *cp;

    if (read_tape(buf,80)!=80)
	return -1;

    buf[79]=0;
    verbage("read_tape_label: %s\n", buf);

    if (!strncmp(buf,"VOL1",4)){
	*type=0;
	for (cp=buf+4; *cp && *cp!=' '; ++cp)
	    ;
	*cp = 0;
	strcpy(label,buf+4);
	return 0;
    } else if (!strncmp(buf,"EOT",3)){
	*type=1;
	if (sscanf(buf+3," %7d", fileno) != 1){
	    verbage("Can't parse filenumber\n");
	    return -1;
	} else {
	    for (cp=buf+10; *cp && *cp!=' '; ++cp)
		;
	    *cp = 0;
	    strcpy(label,buf+10);
	    return 0;
	}
    } else {
	buf[4]=0;
	fprintf(stderr,"%s: unknown label type %s\n",
		progname, buf);
	return -1;
    }
}
