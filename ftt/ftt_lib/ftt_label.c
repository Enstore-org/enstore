static char rcsid[] = "@(#)$Id$";
#include <stdio.h>
#include "ftt_private.h"
#include <string.h>

#define pack(a,b,c,d) (((a)<<24)|((b)<<16)|((c)<<8)|(d))

int ftt_format_label_version( char *, int, char *, int, int, char );

char *ftt_label_type_names[] = {
    /* FTT_ANSI_HEADER         0 */ "FTT_ANSI_HEADER",
    /* FTT_FMB_HEADER          1 */ "FTT_FMB_HEADER",
    /* FTT_TAR_HEADER          2 */ "FTT_TAR_HEADER",
    /* FTT_CPIO_HEADER         3 */ "FTT_CPIO_HEADER",
    /* FTT_UNKNOWN_HEADER      4 */ "FTT_UNKNOWN_HEADER",
    /* FTT_BLANK_HEADER        5 */ "FTT_BLANK_HEADER",
    /* FTT_DONTCHECK_HEADER    6 */ "FTT_DONTCHECK_HEADER",
    /* FTT_MAX_HEADER	       7 */ "FTT_MAX_HEADER", 
};

int
ftt_guess_label(char *buf, int length, char **vol, int *vlen) {
    char *p;
    
    /* don't clear errors yet, need to look at ftt_errno */

    char *_name = "ftt_guess_label";
    DEBUG1(stderr, "Entering %s\n", _name);
    CKNULL("label data buffer pointer", buf);

    if (-1 == length && ftt_errno == FTT_EBLANK) {
	/* read returned EBLANK... */
	ftt_eprintf("Ok\n");
	if (vol) *vol = "";
	if (vlen) *vlen = 0;
	ftt_errno = FTT_SUCCESS;
	return FTT_BLANK_HEADER;
    } else if ( -1 == length ) {
	return -1;
    } else if ( length < 80 ) {
	/* no known header is < 80 bytes long */
	ftt_eprintf("Ok\n");
	if (vol) *vol = "";
	if (vlen) *vlen = 0;
	ftt_errno = FTT_SUCCESS;
	return FTT_UNKNOWN_HEADER;
    }

    /* okay, now we can clear errors... */

    ftt_eprintf("Ok\n");
    ftt_errno = FTT_SUCCESS;

    /* pick the ones we can with the first 4 bytes */

    switch(pack(buf[0],buf[1],buf[2],buf[3])) {

    case pack('V','O','L','1'):
	if (vol) *vol = buf+4;
	/* trim blanks -- loop has to stop at least when it hits the '1' */
	p = buf+10;
	while (' ' == *p) {
	    p--;
	}
	if (vlen) *vlen = (p - (buf + 4)) + 1;
	return FTT_ANSI_HEADER;

    case pack('0','7','0','7'):
	if (vol)  *vol = buf + 0156;
	if (vlen) *vlen = strlen(*vol);
	return FTT_CPIO_HEADER;
    }

    /* check for a tar header */

    if (pack('u','s','t','a')==pack(buf[0401],buf[0402],buf[0403],buf[0404])) {
	if (vol) *vol = buf;
	if (vlen) *vlen = strlen(*vol);
	return FTT_TAR_HEADER;
    }

    /* check for an fmb header -- newline separated ascii */

    p = strchr(buf,'\n');
    if (0 != p && (length % 1024 == 0)) {
	if (vol) *vol = buf;
	if (vlen) *vlen = p - buf;
	return FTT_FMB_HEADER;
    }

    /* if all else failed, we don't know what it was... */
    if (vol) *vol = "";
    if (vlen) *vlen = 0;
    return FTT_UNKNOWN_HEADER;
}

void
ftt_to_upper( char *p ) {
   int i = 0;
   while( p[i] ) {
	p[i] = p[i] >= 'a' && p[i] <= 'z' ? p[i]-'a'+'A' : p[i];
	i++;
   }
}

int
ftt_format_label( char *buf, int length, char *vol, int vlen, int type) {
   return ftt_format_label_version(buf, length, vol, vlen, type, 0);
}

int
ftt_format_label_version( char *buf, int length, char *vol, int vlen, int type, char version) {

#define BLEN 512
    static char volbuf[BLEN];
    ENTERING("ftt_format_label");
    CKNULL("label buffer pointer", buf);
    
    if (vlen >= BLEN) {
	ftt_eprintf("volume label too long; maximum is %d", BLEN-1);
	ftt_errno = FTT_EFAULT;
	return -1;
    }
    memcpy( volbuf, vol, vlen );
    volbuf[vlen] = 0;

    switch(type) {
    case FTT_ANSI_HEADER:
	if ( version == 0 ) { /* default is 4 */
	    version = '4';
	}
	ftt_to_upper(volbuf);
	if (length >= 80) {
	    sprintf(buf, "VOL1%-6.6s%-1.1s%-13.13s%-13.13s%-14.14s%-28.28s%-1.1d", 
				volbuf, " ", " ", "ftt", " ", " " , version);
	    return 80;
	 } else {
	    ftt_errno = FTT_EBLKSIZE;
	    ftt_eprintf("ftt_format_label: the buffer size of %d is too small for the indicated header type.");
	    return -1;
	}
	break;

    case FTT_FMB_HEADER:
	if (length >= 2048) {
	    sprintf(buf, "%s\n%s\n%s\n%s\n",
			volbuf, "never", "cpio", "16k");
	    return 2048;
	 } else {
	    ftt_errno = FTT_EBLKSIZE;
	    ftt_eprintf("ftt_format_label: the buffer size of %d is too small for the indicated header type.");
	    return -1;
	}
	break;
    case FTT_CPIO_HEADER:
	 if (length >= 512) {
	     memset(buf, 0, (size_t)512); 
	     sprintf(buf, "070701000086f6000081a4000006c5000011ad0000000130f68764000000000000001e0000000500000000000000000000000a00000000%s", volbuf);
	     sprintf(buf + strlen(buf) +1 , "0007070100000000000000000000000000000000000000010000000000000000000000000000000000000000000000000000000b00000000TRAILER!!!");
	     return 512;
	 } else {
	    ftt_errno = FTT_EBLKSIZE;
	    ftt_eprintf("ftt_format_label: the buffer size of %d is too small for the indicated header type.");
	    return -1;
	 }
	 break;
    case FTT_TAR_HEADER:
	 if (length >= 10240) {
	     memset(buf, 0, (size_t)10240); 
	     sprintf(buf,     "%s", volbuf);
	     sprintf(buf+0144,"000644 ");
	     sprintf(buf+0154,"003305 ");
	     sprintf(buf+0164,"00000000000 06075503544 014150");
	     sprintf(buf+0232, " 0");
	     sprintf(buf+0401, "ustar");
	     sprintf(buf+0410, "00%s", "nobody");
	     sprintf(buf+0451, "00%s", "other");
	     return 10240;
	 } else {
	    ftt_errno = FTT_EBLKSIZE;
	    ftt_eprintf("ftt_format_label: the buffer size of %d is too small for the indicated header type.");
	    return -1;
	 }
    }

    ftt_errno = FTT_ENOTSUPPORTED;
    if ( type < FTT_MAX_HEADER ) {
      ftt_eprintf("ftt_format_label: unsupported label type %s\n",
		ftt_label_type_names[type]);
    } else{
      ftt_eprintf("ftt_format_label: unsupported label type %d\n",
		type);
    }
    return -1;
}
