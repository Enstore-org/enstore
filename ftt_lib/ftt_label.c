#include <stdio.h>
#include "ftt_private.h"
#include <string.h>

#define splice(a,b,c,d) (((a)<<24)|((b)<<16)|((c)<<8)|(d))

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
    
    ENTERING("ftt_guess_label");
    CKNULL("label data buffer pointer", buf);

    if (-1 == length && ftt_errno == FTT_EBLANK) {
	return FTT_BLANK_HEADER;
    }
    ftt_eprintf("Ok\n");
    ftt_errno = FTT_SUCCESS;
    if( 0 == buf ) {
	ftt_eprintf("ftt_guess_label called with NULL buffer pointer\n");
	ftt_errno = FTT_EFAULT;
	return -1;
    }

    switch(splice(buf[0],buf[1],buf[2],buf[3])) {

    case splice('V','O','L','1'):
	if (vol) *vol = buf+4;
	p = buf+10;
	while (' ' == *p) {
	    p--;
	}
	if (vlen) *vlen = (p - (buf + 4)) + 1;
	return FTT_ANSI_HEADER;

    case splice('0','7','0','7'):
	if (vol)  *vol = buf + 0156;
	if (vlen) *vlen = strlen(*vol);
	return FTT_CPIO_HEADER;
    }

    if (0 ==strcmp(buf+257, "ustar")) {
	if (vol) *vol = buf;
	if (vlen) *vlen = strlen(*vol);
	return FTT_TAR_HEADER;
    }

    p = strchr(buf,'\n');
    if (0 != p && (1024 == length || 2048 == length)) {
	if (vol) *vol = buf;
	if (vlen) *vlen = p - buf;
	return FTT_FMB_HEADER;
    }
    return FTT_UNKNOWN_HEADER;
}

int
ftt_format_label( char *buf, int length, char *vol, int vlen, int type) {

    ENTERING("ftt_format_label");
    CKNULL("label buffer pointer", buf);
    
    switch(type) {
    case FTT_ANSI_HEADER:
	if (length >= 80){
	    sprintf(buf, "VOL1%-6.6s%-1.1s%-13.13s%-13.13s%-14.14s%-28.28s%-1.1s", 
				vol, " ", " ", "ftt", " ", " " , "4");
	    return 80;
	}
	break;
    case FTT_FMB_HEADER:
	if (length >= 2048)
	    sprintf(buf,"%s\n%s\n%s\n%s\n",
			vol, "never", "cpio", "16k");
	    return 2048;
	break;
    }
    ftt_errno = FTT_ENOTSUPPORTED;
    if ( type < FTT_MAX_HEADER ) {
      ftt_eprintf("ftt_format_label called with an unsupported label type %s\n",
		ftt_label_type_names[type]);
    } else{
      ftt_eprintf("ftt_format_label called with an unsupported label type %d\n",
		type);
    }
    return -1;
}
