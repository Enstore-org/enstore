static char rcsid[] = "$Id$";
#include <stdio.h>
#include <sys/utsname.h>
#include <string.h>
#include <ctype.h>
#include <ftt_private.h>

char *
ftt_get_os() {
    struct utsname buf;

    uname(&buf);
    return ftt_make_os_name( buf.sysname, buf.release, buf.version);
}

char *
ftt_make_os_name(char *sys, char *release , char *version) {
    static char sysname[512];

    sprintf(sysname,"%s+%s.%s", sys, release, version);
    return sysname;
}

int
ftt_findslot (char *basename,char *os, char *drivid,  char *string, int *num) {
    int i;
    char *lastpart;

    DEBUG2(stderr,"Entering ftt_findslot %s %s %s\n", basename, os, drivid );

    /* tables now only deal with the last directory and file 
    ** component of the pathname 
    */ 

    lastpart = ftt_find_last_part(basename);

    DEBUG2(stderr,"looking at '%s' part of name\n", lastpart);

    for( i = 0; devtable[i].os !=0 ; i++ ) {
	if (ftt_matches(os, devtable[i].os) && 
		ftt_matches(drivid, devtable[i].drivid)) {
	   DEBUG3(stderr,"trying format \"%s\"\n", devtable[i].baseconv_in);


	   if (devtable[i].nconv == 
		     sscanf(lastpart,devtable[i].baseconv_in,string,num)) {
		     DEBUG3(stderr, "format Matches!\n");
		     return i;
	   }
	   DEBUG3(stderr, "format missed...\n");
	}
    }
    return -1;
}

extern char *
ftt_strip_to_basename(const char *basename,char *os) {
    static char buf[512];
    static char buf2[512];
    static char string[512];
    int bus,id;
    int i, res;
    int maxlinks=512;
    char *lastpart;
    char *p;

    DEBUG2(stderr, "Entering ftt_strip_to_basename\n");
    memset(buf,0, 512);
    memset(buf2,0, 512);
    memset(string,0, 512);

    strncpy(buf, basename, 512);
#ifdef DO_SKIP_SYMLINKS
    while( 0 <  readlink(buf, buf2, 512) && maxlinks-- >0 ) {
	if( buf2[0] == '/' ) {
	    /* absolute pathname, replace the whole buffer */
	    strncpy(buf,buf2,512);
	} else {
	    /* relative pathname, replace after last /, if any */
	    if ( 0 == (p = strrchr(buf,'/'))) {
	       p = buf;
	    } else {
	       p++;
	    }
	    strncpy(p, buf2, 512 - (p - buf));
	}
    }
#endif

    i = ftt_findslot(buf, os, "", string, &id);
    if (i < 0) {
	return 0;
    }
    /* tables now only deal with the last directory and file component of 
    ** the pathname 
    */
    lastpart = ftt_find_last_part(buf);

    /*
    ** first item in the format can be either a string or a digit;
    ** check for strings
    */
    if ( devtable[i].baseconv_out[1] == 's') {
	sprintf(lastpart, devtable[i].baseconv_out, string, id);
    } else {
	sprintf(lastpart, devtable[i].baseconv_out,*(int*)string, id);
    }
    return strdup(buf);
}

/*
** search for last 2 slashes in pathname,
** and return the pointer to the character after the next to last one.
** if there isn't one, return the pointer to the original string
*/
char *
ftt_find_last_part( char *p ) {
    char *s, *s1 = 0, *s2 = 0;

    s = p;
    while( s && *s ) {
	if( *s == '/' ) {
	    s2 = s1;
	    s1 = s;
	}
	s++;
    }
    if( s2 ) {
	return s2+1;
    } else {
	return p;
    }
}


/*
** get_driveid guesses the drive id the best it can from the available
** system configuration command(s).
** Returns a SCSI device id, or a prefix thereof
*/
extern char *
ftt_get_driveid(char *basename,char *os) {
    static char cmdbuf[255];
    static char output[255];
    static char string[255];
    int bus, id;
    FILE *pf;
    char *res = 0;
    int i;

    DEBUG2(stderr, "Entering ftt_get_driveid\n");
    i = ftt_findslot(basename, os, "",  string, &id);
    if (i < 0) {
	return 0;
    }
    if ( devtable[i].drivid[1] == 's') {
	sprintf(cmdbuf, devtable[i].drividcmd, string, id);
    } else {
	sprintf(cmdbuf, devtable[i].drividcmd,*(int*)string, id);
    }
    DEBUG3(stderr,"Running \"%s\" to get drivid\n", cmdbuf);
    pf = popen(cmdbuf, "r");
    if (pf) {
	res = fgets(output, 255,pf);
	pclose(pf);
	if (res != 0) {
	    output[strlen(output)-1] = 0;
	    res = strdup(output);
	}
    }
    DEBUG3(stderr, "returning %s\n", res);
    return res;
}

