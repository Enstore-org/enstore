static char rcsid[] = "@(#)$Id$";
#include <string.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "ftt_private.h"

extern int errno;

int 
ftt_get_data_direction(ftt_descriptor d) {
    ENTERING("ftt_get_data_direction");
    PCKNULL("ftt_descriptor", d);

    return d->data_direction;
}

char *
ftt_get_basename(ftt_descriptor d) {
    
    ENTERING("ftt_get_basename");
    PCKNULL("ftt_descriptor", d);

    return d->basename;
}

char *
ftt_density_to_name(ftt_descriptor d, int density){
    char *res;

    ENTERING("ftt_density_to_name");
    if (density + 1 < MAX_TRANS_DENSITY ) {
	res = d->densitytrans[density + 1];
    } else {
	res = 0;
    }
    if ( 0 == res ) {
	res = "unknown";
    }
    return res;
}

int
ftt_name_to_density(ftt_descriptor d, char *name){
    int res;

    ENTERING("ftt_name_to_density");
    CKNULL("density name", name);

    for (res = 0; d->densitytrans[res] != 0; res++) {
	if( ftt_matches(name, d->densitytrans[res])) {
	    return res - 1;
	}
    }
    ftt_errno = FTT_ENODEV;
    ftt_eprintf(
       "ftt_name_to_density: name %s is not appropriate for device %s\n",
       name,
       d->basename );
    return -1;
}

int
ftt_get_max_blocksize(ftt_descriptor d) {
    int result;

    ENTERING("ftt_get_max_blocksize");
    CKNULL("ftt_descriptor", d);

    result = d->devinfo[d->which_is_default].max_blocksize;

    /* round down to nearest fixed blocksize */
    if (d->default_blocksize != 0) {
        result = result - (result % d->default_blocksize);
    }

    return result;
}

char **
ftt_list_all(ftt_descriptor d) {
    static char *table[MAXDEVSLOTS];
    int i,j;

    ENTERING("ftt_list_all");
    PCKNULL("ftt_descriptor", d);

    for( i = 0,j = 0; j <= MAXDEVSLOTS  && d->devinfo[i].device_name != 0; i++ ){
	if (d->devinfo[i].first) {
	    table[j++] = d->devinfo[i].device_name;
	}
    }
    table[j++] = 0;
    return table;
}

int
ftt_chall(ftt_descriptor d, int uid, int gid, int mode) {
    static struct stat sbuf;
    char **pp;
    int res, rres;
    int i;

    ENTERING("ftt_chall");
    CKNULL("ftt_descriptor", d);

    rres = 0;
    pp = ftt_list_all(d);
    /* 
     * Do the stat on each device file if file doesn't exist
     * skip it but rport other errors 
     */
    for( i = 0; pp[i] != 0; i++){
        res = stat(pp[i],&sbuf);
	if (res < 0) {
	    if ( errno == ENOENT ) {
		continue; 
	    } else {
		rres = ftt_translate_error(d,FTT_OPN_CHALL,"ftt_chall",res,
						"stat",1);
		continue;
	    }
	}
	res = chmod(pp[i],mode);
	if (res < 0) {
            rres = ftt_translate_error(d,FTT_OPN_CHALL,"ftt_chall",res,"chmod",1);
	}
	res = chown(pp[i],uid,gid);
	if (res < 0) {
            rres = ftt_translate_error(d,FTT_OPN_CHALL,"ftt_chall",res,"chown",1);
	}
    }
    return rres;
}

static char *comptable[] = {"uncompressed", "compressed"};
char *
ftt_avail_mode(ftt_descriptor d, int density, int mode, int fixedblock) {
    int i;
    char *dname;

    ENTERING("ftt_avail_mode");
    PCKNULL("ftt_descriptor", d);
    
    for( i = 0; d->devinfo[i].device_name != 0; i++ ){
	if( d->devinfo[i].density == density &&
		    d->devinfo[i].mode == mode &&
		    d->devinfo[i].rewind == 0 &&
		    d->devinfo[i].fixed  == fixedblock) {
	    return d->devinfo[i].device_name;
	}
    }
    dname = ftt_density_to_name(d, density);
    ftt_eprintf("ftt_avail_mode: mode %s(%d) density %s(%d) %s is not avaliable on device %s", 
	    comptable[mode], mode, 
	    dname, density, 
	    fixedblock?"fixed block" : "variable block", d->basename);
    ftt_errno = FTT_ENODEV;
    return 0;
}

char *
ftt_get_mode(ftt_descriptor d, int *density, int* mode, int *blocksize){

    ENTERING("ftt_get_mode");
    PCKNULL("ftt_descriptor", d);

    if (density) *density = d->devinfo[d->which_is_default].density;
    if (mode)    *mode = d->devinfo[d->which_is_default].mode;
    if (blocksize) *blocksize = d->devinfo[d->which_is_default].fixed ? 
		    d->default_blocksize : 0;
    return d->devinfo[d->which_is_default].device_name;
}
char *
ftt_set_mode(ftt_descriptor d, int density, int mode, int blocksize) {
    int i;
    char *dname;

    ENTERING("ftt_set_mode");
    PCKNULL("ftt_descriptor", d);
    
    ftt_close_dev(d);
    d->density_is_set = 0;
    for( i = 0; d->devinfo[i].device_name != 0; i++ ){
	if (d->devinfo[i].density == density &&
		    d->devinfo[i].mode == mode &&
		    (d->devinfo[i].fixed == 0) == (blocksize == 0) && 
		    d->devinfo[i].rewind == 0) {

	    /* clear flag if we are switching density */

	    if (d->devinfo[i].hwdens != d->devinfo[d->which_is_default].hwdens){
		d->density_is_set = 0;
	    }

	    d->which_is_default = i;
	    d->default_blocksize = blocksize;
	    return d->devinfo[i].device_name;
	}
    }
    dname = ftt_density_to_name(d, density);
    ftt_eprintf("ftt_set_mode: mode %s(%d) density %s(%d) blocksize %d is not avaliable on device %s", 
	    comptable[mode], mode, 
	    dname , density, 
		blocksize, d->basename);
    ftt_errno = FTT_ENODEV;
    return 0;
}

int 
ftt_get_mode_dev(ftt_descriptor d, char *devname, int *density, 
			int *mode, int *blocksize, int *rewind) {
    int i;
    int hwdens;
    int found;

    ENTERING("ftt_get_mode_dev");
    CKNULL("ftt_descriptor", d);
    
    hwdens = ftt_get_hwdens(d,devname);
    for( i = 0; d->devinfo[i].device_name != 0; i++ ) {
	if (0 == strcmp(d->devinfo[i].device_name, devname)){
	    found = 1;
	    if (density)   *density = d->devinfo[i].density;
	    if (mode)      *mode = d->devinfo[i].mode;
	    if (blocksize) *blocksize =  d->devinfo[i].fixed;
	    if (rewind)    *rewind = d->devinfo[i].rewind;

	    if (d->devinfo[i].hwdens == hwdens) {
		/* hardware density match is a better match */
		/* otherwise keep looking */
		break; 
	    }
	}
    }
    if (found) {
	return 0;
    } else {
	ftt_eprintf("ftt_get_mode_dev: device name %s was not found in the ftt tables for basename %s\n",
	    devname, d->basename);
	ftt_errno = FTT_ENODEV;
	return -1;
    }
}

int 
ftt_set_mode_dev(ftt_descriptor d, char *devname, int force) {
    int i;

    ENTERING("ftt_set_mode_dev");
    CKNULL("ftt_descriptor", d);
    CKNULL("device name", devname);
    
    for( i = 0; d->devinfo[i].device_name != 0; i++ ){
	if (0 == strcmp(d->devinfo[i].device_name, devname)) {
	    d->which_is_default = i;
	    d->default_blocksize = -1;
	    return 0;
	}
    }
    if (force) { 
	/* not found in table, but force bit was set... */

        if (i >= MAXDEVSLOTS - 1){
	    /* there isn't room in the table for it */

	    ftt_errno = FTT_ENOMEM;
	    ftt_eprintf("ftt_set_mode_dev: tried to add a new device entry to the table when there was not room for it");
	    return -1;
	}
	/* clear flag if we are switching density */
	if (d->devinfo[i].hwdens != d->devinfo[d->which_is_default].hwdens) {
	    d->density_is_set = 0;
	}
	/* so add it to the table */
	d->devinfo[i].device_name = devname;
	d->which_is_default = i;

	/* and we know/set nothing ... */
	d->devinfo[i].mode = -1;
	d->devinfo[i].density = -1;
	d->devinfo[i].fixed = -1;
	d->default_blocksize = -1;

	/* make sure sentinel null is in table */
	d->devinfo[i+1].device_name = 0;

	return 0;
    }
    ftt_eprintf("ftt_set_mode_dev: device name %s was not found in the ftt tables for basename %s and the force bit was not set.",
	devname, d->basename);
    ftt_errno = FTT_ENODEV;
    return -1;
}

ftt_set_data_direction( ftt_descriptor d, int value ) {
    ENTERING("ftt_set_data_direction");
    CKNULL("ftt_descriptor", d);
    if (value != FTT_DIR_READING && value != FTT_DIR_WRITING ) {
	ftt_errno = FTT_ENXIO;
	ftt_eprintf("ftt_set_data_direction: an invalid value of %d was given for the data direction.", value);
	return -1;
    }
    d->data_direction = value;
    return 0;
}
