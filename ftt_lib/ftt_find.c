static char rcsid[] = "@(#)$Id$";
#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <ftt_private.h>

#ifdef WIN32         /* this is Windows */

#include <process.h>
#include <windows.h>
#define geteuid() -1 
#define popen _popen
#define pclose _pclose


char * ftt_get_os() {
	char ver[20],rel[20];
	char *os = "WINNT";
    OSVERSIONINFO buf;
	buf.dwOSVersionInfoSize =sizeof(OSVERSIONINFO);
    GetVersionEx(&buf);
	if (buf.dwPlatformId != VER_PLATFORM_WIN32_NT ) os = "WIN32";
	sprintf(rel,"%d",buf.dwMajorVersion);
	sprintf(ver,"%d",buf.dwMinorVersion);
    return ftt_make_os_name( "WINNT", rel,ver);
}

#else                /* this is UNIX */

#include <sys/utsname.h>

char *
ftt_get_os() {
    struct utsname buf;

    uname(&buf);
    return ftt_make_os_name( buf.sysname, buf.release, buf.version);
}
#endif

char *
ftt_make_os_name(char *sys, char *release , char *version) {
    static char sysname[512];

    sprintf(sysname,"%s+%s.%s", sys, release, version);
    return sysname;
}

int
ftt_findslot (char *basename, char *os, char *drivid,  
			void *p1, void *p2, void *p3) {
    int i;
    char *lastpart;
    int res;

    DEBUG2(stderr,"Entering ftt_findslot %s %s %s\n", basename, os, drivid );

    /* tables now only deal with the last directory and file 
    ** component of the pathname 
    */ 

    lastpart = ftt_find_last_part(basename);

    DEBUG2(stderr,"looking at '%s' part of name\n", lastpart);

    for( i = 0; devtable[i].os !=0 ; i++ ) {
	if (ftt_matches(os, devtable[i].os) && 
		ftt_matches(drivid, devtable[i].drivid)) {
	   DEBUG3(stderr,"trying format \"%s\" against %s\n", 
		devtable[i].baseconv_in, lastpart);


           res = sscanf(lastpart,devtable[i].baseconv_in,p1,p2,p3);

	   if (devtable[i].nconv == res ) {
		     DEBUG3(stderr, "format Matches!\n");
		     return i;
	   }
	   DEBUG3(stderr, "format missed... got %d, not %d\n",
				res, devtable[i].nconv);
	}
    }
    return -1;
}

extern char *
ftt_strip_to_basename(const char *basename,char *os) {
    static char buf[512];
    static char buf2[512];
    static union { int n; char s[512];} s1, s2, s3;
 
    int i;
    int maxlinks=512;
    char *lastpart;

    DEBUG2(stderr, "Entering ftt_strip_to_basename\n");
    memset(buf,0, 512);
    memset(buf2,0, 512);
    memset(s1.s,0, 512);

    strncpy(buf, basename, 512);

#ifdef WIN32
	strlwr( buf);
#endif 

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

    i = ftt_findslot(buf, os, "", &s1, &s2, &s3);
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
	sprintf(lastpart, devtable[i].baseconv_out, s1.s, s2.n, s3.n);
    } else {
	sprintf(lastpart, devtable[i].baseconv_out, s1.n, s2.n, s3.n);
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
	char s_find = '/';

	/* -------------------- for Windows NT ------------------------------- */
#ifdef WIN32
	s_find = '\\';
#endif

    s = p;
    while( s && *s ) {
	if( *s == s_find ) {
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
    static char cmdbuf[512];
    static char output[512];
    static union { int n; char s[512];} s1, s2, s3;
    FILE *pf;
    char *res = 0;
    int i;

    DEBUG2(stderr, "Entering ftt_get_driveid\n");
    i = ftt_findslot(basename, os, "",  &s1, &s2, &s3);
    if (i < 0) {
	return 0;
    }
    if ( 0 != geteuid() && (devtable[i].flags & FTT_FLAG_SUID_DRIVEID) ) {

	DEBUG3( stderr, "Running ftt_suid...\n" );
	sprintf(cmdbuf, "ftt_suid -i %s", basename );
	pf = popen(cmdbuf, "r");
        if (pf != 0) {
	    res = fgets(output,512,pf);
	    pclose(pf);
	} else {
	    res = 0;
	}
    } else {
	if ( devtable[i].drividcmd[1] == 's') {
	    sprintf(cmdbuf, devtable[i].drividcmd, s1.s, s2.n, s3.n);
	} else {
	    sprintf(cmdbuf, devtable[i].drividcmd, s1.n, s2.n, s3.n);
	}
	DEBUG3(stderr,"Running \"%s\" to get drivid (lenght %d < 512 ) \n", cmdbuf,strlen(cmdbuf));
	pf = popen(cmdbuf, "r");
	if (pf) {
	    res = fgets(output, 512,pf);
	    pclose(pf);
	}
    }
    if (res != 0) {
	output[strlen(output)-1] = 0; /* stomp the newline */
	res = strdup(output);
    }
    DEBUG3(stderr, "returning %s\n", res);
    return res;
}


