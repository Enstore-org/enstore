#include <stdio.h>
#include <sys/utsname.h>
#include <string.h>
#include <ctype.h>
#include <ftt_private.h>

char *
ftt_get_os() {
    static char sysname[512];
    struct utsname buf;

    uname(&buf);
    sprintf(sysname,"%s+%s.%s", buf.sysname, buf.release, buf.version);
    return sysname;
}

int
ftt_findslot (char *basename,char *os, char *drivid, int *bus, int *id) {
    int i;

    DEBUG2(stderr,"Entering ftt_findslot %s %s %s\n", basename, os, drivid );
    for( i = 0; devtable[i].os !=0 ; i++ ) {
	if (ftt_matches(os, devtable[i].os) && 
		ftt_matches(drivid, devtable[i].drivid)) {
	   DEBUG3(stderr,"trying format \"%s\"\n", devtable[i].baseconv);
	   if (devtable[i].nconv == 
		     sscanf(basename,devtable[i].baseconv,bus,id)) {
		     DEBUG3(stderr, "format Matches!\n");
		     return i;
	   }
	   DEBUG3(stderr, "format missed...\n");
	}
    }
    return -1;
}

extern char *
ftt_strip_to_basename(char *basename,char *os) {
    char *pc;
    char buf[512];
    int bus,id;
    int i;

    DEBUG2(stderr, "Entering ftt_strip_to_basename\n");
    i = ftt_findslot(basename, os, "", &bus, &id);
    if (i < 0) {
	return 0;
    }
    sprintf(buf,devtable[i].baseconv, bus, id);
    return strdup(buf);
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
    int bus, id;
    FILE *pf;
    char *res = 0;
    int i;

    DEBUG2(stderr, "Entering ftt_get_driveid\n");
    i = ftt_findslot(basename, os, "",  &bus, &id);
    if (i < 0) {
	return 0;
    }
    sprintf(cmdbuf,devtable[i].drividcmd, bus, id);
    DEBUG3(stderr,"Running \"%s\" to get drivid\n", cmdbuf);
    pf = popen(cmdbuf, "r");
    if (pf) {
	fgets(output, 255,pf);
	pclose(pf);
	output[strlen(output)-1] = 0;
        res = strdup(output);
    }
    DEBUG3(stderr, "returning %s\n", res);
    return res;
}

