#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <ftt_private.h>

ftt_descriptor
ftt_open(const char *name, int rdonly) {
    char *basename;
    char *os, *drivid;
    ftt_descriptor res;
    
    ENTERING("ftt_open");
    PCKNULL("base name", name);

    os=ftt_get_os();
    DEBUG2(stderr,"os is %s\n", os);
    if( 0 == os ){
	ftt_eprintf("Unable to determine operating system type.\n");
	ftt_errno=FTT_ENOTSUPPORTED;
	return 0;
    }

    basename=ftt_strip_to_basename(name, os);
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
	ftt_errno=FTT_ENODEV;
	return 0;
    }

    res = ftt_open_logical(basename, os, drivid, rdonly);
    free(basename);
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
ftt_open_logical(const char *name, char *os, char *drivid, int rdonly) {
    static char buf[512];
    static char string[512];
    static ftt_descriptor_buf d;
    char *basename;
    int n1, n2;
    int i,j;
    ftt_descriptor pd;

    /* find device type and os in table */

    ENTERING("ftt_open_logical");
    PCKNULL("base name",name);
    PCKNULL("operating system name", os);
    PCKNULL("drive id prefix", drivid);

    basename = ftt_strip_to_basename(name, os);
    if ( basename == 0 ) {
	ftt_eprintf("Unable to determine drive basename.\n");
	ftt_errno=FTT_ENOTSUPPORTED;
	return 0;
    }

    /* look up in table, note that table order counts! */
    i = ftt_findslot(basename, os, drivid, &n1, &n2, string);

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
    d.max_blocksize = devtable[i].max_blocksize;
    d.prod_id = strdup(drivid);
    if( 0 == d.prod_id ) {
	ftt_eprintf("out of memory allocating string for \"%s\" in ftt_open_logical\n" , drivid);
	ftt_errno = FTT_ENOMEM;
	return 0;
    }

    for( j = 0; devtable[i].devs[j].device_name != 0; j++ ) {
        sprintf(buf, devtable[i].devs[j].device_name, n1, n2, string);
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
    memcpy(pd, &d, sizeof(d));
    return pd;
}

int
ftt_close(ftt_descriptor d){
    int j;
    int res;

    ENTERING("ftt_close");
    CKNULL("ftt_descriptor", d);

    /* valiant attempt at idiot proofing
    **
    ** When we close the descriptor, we shove a -3
    ** in the which_is_open field, which should never happen
    ** in normal operation.
    ** 
    ** if we see the -3 here, someone is trying to close us
    ** twice in a row...
    */
    if (d->which_is_open == -3) {
	ftt_errno = FTT_EFAULT;
	ftt_eprintf("ftt_close called twice on the same descriptor!\n");
	return -1;
    }
    d->which_is_open = -3;
    res = ftt_close_dev(d);
    for(j = 0; 0 != d->devinfo[j].device_name ; j++ ) {
	free(d->devinfo[j].device_name);
	d->devinfo[j].device_name = 0;
    }
    if (d->basename) {
	free(d->basename);
	d->basename = 0;
    } if (d->prod_id) {
	free(d->prod_id);
	d->prod_id = 0;
    }
    free(d);
    return res;
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
	d->which_is_open = d->which_is_default;
        DEBUG1(stderr,"Actually opening\n");

	/*
	** it looks like we should do a readonly open if we're a readonly,
	** descriptor and a read/write open if we're read/write descriptor.  
	** Unfortunately on some platforms a read/write open on a write 
	** protected tape will fail.  So to make it behave the same 
	** everywhere, we first open it readonly, check for write protection 
	** if we are read/write, and finally reopen it read/write if we are
	** a read/write desciptor.
	*/
	d->file_descriptor = open(d->devinfo[d->which_is_default].device_name,
				  O_RDONLY|FNONBLOCK, 0);
	if ( d->file_descriptor < 0 ) {
	    d->file_descriptor = ftt_translate_error(d,FTT_OPN_OPEN, "an open() system call",
	    		d->file_descriptor, "an ftt_open_dev()",1);
	    d->which_is_open = -1;
	}
	if (d->readonly == FTT_RDWR && (ftt_status(d,0) & FTT_PROT)) {
	     ftt_errno = FTT_EROFS;
	     ftt_eprintf("ftt_open_dev was called with a read/write ftt_descriptor and a write protected tape.");
	     return -1;
	}
	if (d->readonly == FTT_RDWR ) {
	    close(d->file_descriptor);
	    d->file_descriptor = open(
		d->devinfo[d->which_is_default].device_name,
	        O_RDWR|FNONBLOCK,0);
	    if ( d->file_descriptor < 0 ) {
		d->file_descriptor = ftt_translate_error(d,FTT_OPN_OPEN, "an open() system call",
			    d->file_descriptor, "an ftt_open_dev()",1);
		d->which_is_open = -1;
	    }
	}
    }
    DEBUG2(stderr,"Returing %ld\n", d->file_descriptor);
    return d->file_descriptor;
}

int
ftt_close_dev(ftt_descriptor d) {
    int res = 0;

    ENTERING("ftt_close_dev");
    CKNULL("ftt_descriptor", d);

    if ( d->which_is_open >= 0 ){
	ftt_write_fm_if_needed(d);
        DEBUG1(stderr,"Actually closing\n");
	res = close(d->file_descriptor);
	d->which_is_open = -1;
	d->file_descriptor = -1;
    }
    return res;
}
