#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/fcntl.h>
#include <ftt_private.h>

ftt_descriptor
ftt_open(char *basename, int rdonly) {
    char *os, *drivid;
    ftt_descriptor res;
    
    ENTERING("ftt_open");
    PCKNULL("base name", basename);

    ftt_eprintf("Ok\n");
    ftt_errno = FTT_SUCCESS;
    DEBUG1(stderr,"entering ftt_open\n");

    os=ftt_get_os();
    DEBUG2(stderr,"os is %s\n", os);
    if( 0 == os ){
	ftt_eprintf("Unable to determine operating system type.\n");
	ftt_errno=FTT_ENOTSUPPORTED;
	return 0;
    }

    basename=ftt_strip_to_basename(basename, os);
    DEBUG2(stderr,"basename is %s\n", basename);
    if ( basename == 0 ) {
	ftt_eprintf("Unable to determine drive basename.\n");
	ftt_errno=FTT_ENOTSUPPORTED;
	return 0;
    }

    drivid=ftt_get_driveid(basename, os);
    DEBUG2(stderr,"drivid is %s\n", drivid);
    if( 0 == drivid ){
	ftt_eprintf("Unable to determine tape drive type.\n");
	ftt_errno=FTT_ENOTSUPPORTED;
	return 0;
    }

    res = ftt_open_logical(basename, os, drivid, rdonly);
    return res;
}

/* prefix match comparator -- returns true if either string is
**		a prefix of the other
*/
int
ftt_matches( char *s1, char *s2 ) {
    DEBUG3(stderr, "Matching '%s' against '%s'\n", s1, s2);
    while( 0 != *s1 && 0 != *s2 && *s1 == *s2){
        s1++;
        s2++;
    }
    DEBUG3(stderr, "Returning %d\n", *s1 == 0 || *s2 == 0);
    return *s1 == 0 || *s2 == 0;
}

/*
** ftt_open_logical -- create a descriptor table
*/
ftt_descriptor
ftt_open_logical(char *basename, char *os, char *drivid, int rdonly) {
    static char buf[512];
    static ftt_descriptor_buf d;
    int n1, n2, n3;
    int i,j;
    ftt_descriptor pd;

    /* find device type and os in table */

    ENTERING("ftt_open_logical");
    PCKNULL("base name",basename);
    basename = ftt_strip_to_basename(basename, os);
    if ( basename == 0 ) {
	ftt_eprintf("Unable to determine drive basename.\n");
	ftt_errno=FTT_ENOTSUPPORTED;
	return 0;
    }

    
    DEBUG1(stderr, "Entering ftt_open_logical\n");
    ftt_eprintf("Ok\n");

    i = ftt_findslot(basename, os, drivid, &n1, &n2);

    DEBUG3(stderr, "Picked entry %d numbers %d %d\n", i, n1, n2);

    /* if it wasn't found, it's not supported */

    if ( i < 0 ) {
        DEBUG3(stderr, "Unsupported...\n");
	ftt_eprintf("Device type %s on platform %s unsupported\n", drivid, os);
	ftt_errno=FTT_ENOTSUPPORTED;
	return 0;
    }

    /* string together device names and flags into our descriptor */

    d.controller = devtable[i].controller;
    d.which_is_open = -1;
    d.readonly = rdonly;
    d.scsi_ops = devtable[i].scsi_ops;
    d.flags = devtable[i].flags;
    d.errortrans = devtable[i].errortrans;
    d.basename = basename;
    d.prod_id = strdup(drivid);

    for( j = 0; devtable[i].devs[j].string != 0; j++ ) {
        sprintf(buf, devtable[i].devs[j].string, n1, n2);
	d.devinfo[j].device_name = strdup(buf);
	if( 0 == d.devinfo[j].device_name ) {
	    ftt_eprintf("out of memory allocating string for \"%s\" in ftt_open_logical\n" , buf);
	    ftt_errno = FTT_ENOMEM;
	    return 0;
	}
	d.devinfo[j].density = devtable[i].devs[j].density;
	d.devinfo[j].mode    = devtable[i].devs[j].mode;
	d.devinfo[j].hwdens  = devtable[i].devs[j].hwdens;
	d.devinfo[j].rewind  = devtable[i].devs[j].rewind;
	d.devinfo[j].fixed   = devtable[i].devs[j].fixed;
	d.devinfo[j].passthru= devtable[i].devs[j].passthru;
	d.devinfo[j].first   = devtable[i].devs[j].first;
    }
    d.devinfo[j].device_name = 0;

    pd = malloc(sizeof(ftt_descriptor_buf));
    if (pd == 0) {
	ftt_eprintf("out of memory allocating descriptor in ftt_open_logical\n");
	ftt_errno = FTT_ENOMEM;
	return 0;
    }
    free(basename);
    *pd = d;
    return pd;
}

ftt_close(ftt_descriptor d){
    int j;

    ENTERING("ftt_close");
    CKNULL("ftt_descriptor", d);

    ftt_close_dev(d);
    for(j = 0; 0 != d->devinfo[j].device_name ; j++ ){
	free(d->devinfo[j].device_name);
    }
    free(d);
    return 1;
}

int
ftt_open_dev(ftt_descriptor d) {
    int res;

    ENTERING("ftt_open_dev");
    CKNULL("ftt_descriptor" , d);
    
   
    if (d->which_is_open < 0) {
	res = ftt_set_hwdens_blocksize(d, 
		d->devinfo[d->which_is_default].hwdens, 
		d->default_blocksize);
	if (res < 0) {
	    return res;
	}
    }
    if (d->which_is_open < 0) {
        d->current_blocksize = d->default_blocksize;
	d->which_is_open = d->which_is_default;
        DEBUG1(stderr,"Actually opening\n");
	d->file_descriptor = open(d->devinfo[d->which_is_default].device_name,
		(d->readonly?O_RDONLY:O_RDWR)|FNONBLOCK,0);
	if ( d->file_descriptor < 0 ) {
	    d->file_descriptor = ftt_translate_error(d,FTT_OPN_OPEN, "an open() system call",
	    		d->file_descriptor, "an ftt_open_dev()",1);
	    d->which_is_open = -1;
	}
    }
    DEBUG2(stderr,"Returing %d\n", d->file_descriptor);
    return d->file_descriptor;
}

int
ftt_close_dev(ftt_descriptor d) {

    ENTERING("ftt_close_dev");
    CKNULL("ftt_descriptor", d);

    if ( d->which_is_open >= 0 ){
	ftt_write_fm_if_needed(d);
        DEBUG1(stderr,"Actually closing\n");
	close(d->file_descriptor);
	d->which_is_open = -1;
	d->file_descriptor = -1;
    }
}
