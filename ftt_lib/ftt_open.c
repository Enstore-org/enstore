static char rcsid[] = "@(#)$Id$";
extern char ftt_version[];
static char *rcslink = ftt_version;
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>
#include <ftt_private.h>

extern int errno;

#ifdef WIN32
#include <io.h>
#include <windows.h>
#include <winioctl.h>

int ftt_translate_error_WIN();

#else
#include <ctype.h>
#include <sys/file.h>
#include <unistd.h>
#endif

ftt_descriptor
ftt_open(const char *name, int rdonly) {
    static char alignname[512];
    char *basename;
    char *os, *drivid;
    ftt_descriptor res;
    
    ENTERING("ftt_open");
    PCKNULL("base name", name);

    strcpy(alignname, name);
    os=ftt_get_os();
    DEBUG2(stderr,"os is %s\n", os);
    if( 0 == os ){
	ftt_eprintf("ftt_open: unable to determine operating system type");
	ftt_errno=FTT_ENOTSUPPORTED;
	return 0;
    }

    basename=ftt_strip_to_basename(alignname, os);
    DEBUG2(stderr,"basename is %s\n", basename);
    if ( basename == 0 ) {
	ftt_eprintf("ftt_open: unable to determine drive basename.\n");
	ftt_errno=FTT_ENOTSUPPORTED;
	return 0;
    }

    drivid=ftt_get_driveid(basename, os);
    DEBUG2(stderr,"drivid is %s\n", drivid);
    if( 0 == drivid ){
	ftt_eprintf("ftt_open: Warning unable to determine tape drive type.\n");
	drivid=strdup("unknown");
    }

    res = ftt_open_logical(basename, os, drivid, rdonly);
    free(basename);
    return res;
}

/* prefix match comparator -- returns true if either string is
**		a prefix of the other
*/
int
ftt_matches( const char *s1, const char *s2 ) {
    DEBUG3(stderr, "Matching '%s' against '%s'\n", s1, s2);
    while( 0 != *s1 && 0 != *s2 && tolower(*s1) == tolower(*s2)){
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
    static union { int n; char s[512];} s1, s2, s3;
    static ftt_descriptor_buf d;
    char *basename;
    int i,j;
    ftt_descriptor pd;
    char *lastpart;

    /* find device type and os in table */

    ENTERING("ftt_open_logical");
    PCKNULL("base name",name);
    PCKNULL("operating system name", os);
    PCKNULL("drive id prefix", drivid);

    basename = ftt_strip_to_basename(name, os);
    if ( basename == 0 ) {
	ftt_eprintf("ftt_open_logical: unable to determine drive basename.\n");
	ftt_errno=FTT_ENOTSUPPORTED;
	return 0;
    }

    /* look up in table, note that table order counts! */
    i = ftt_findslot(basename, os, drivid, &s1, &s2, &s3);

    DEBUG3(stderr, "Picked entry %d number %d\n", i, s2.n);

    /* if it wasn't found, it's not supported */

    if ( i < 0 ) {
        DEBUG3(stderr, "Unsupported...\n");
	ftt_eprintf("ftt_open_logical: device type %s on platform %s unsupported\n", drivid, os);
	ftt_errno=FTT_ENOTSUPPORTED;
	return 0;
    }

    /* string together device names and flags into our descriptor */

    d.controller = devtable[i].controller;
    d.current_blocksize = -1;
    d.which_is_default = 0;
    d.which_is_open = -1;
    d.scsi_descriptor = -1;
    d.readonly = rdonly;
    d.scsi_ops = devtable[i].scsi_ops;
    d.flags = devtable[i].flags;
    d.errortrans = devtable[i].errortrans;
    d.densitytrans = devtable[i].densitytrans;
    d.basename = basename;
    d.prod_id = strdup(drivid);
    d.os = devtable[i].os;
    d.last_pos = -1;
    d.nretries = 0;
    d.nfailretries = 0;
    d.nresets = 0;
    d.nharderrors = 0;

    if( 0 == d.prod_id ) {
	ftt_eprintf("ftt_open_logical: out of memory allocating string for \"%s\" errno %d" , drivid, errno);
	ftt_errno = FTT_ENOMEM;
	return 0;
    }

    /*
    ** the tables only deal with the last 2 components of the path
    ** (that is the last directory and the filename compnent)
    ** [The last 2 components 'cause we turn /dev/rmt/xxx into /dev/scsi/xxx
    ** sometimes.]
    */
    strcpy(buf, basename);
    lastpart = ftt_find_last_part(buf);

    for( j = 0; devtable[i].devs[j].device_name != 0; j++ ) {
	/*
	** first item in the format can be either a string or a digit;
	** check for strings -- "%s..."
	** this ought to be more generic, but for now it's okay -- mengel
	*/
	if ( devtable[i].devs[j].device_name[1] == 's') {
            sprintf(lastpart, devtable[i].devs[j].device_name, s1.s, s2.n,s3.n);
	} else {
            sprintf(lastpart, devtable[i].devs[j].device_name, s1.n, s2.n,s3.n);
	}

	d.devinfo[j].device_name = strdup(buf);

	if( 0 == d.devinfo[j].device_name ) {
	    ftt_eprintf("fft_open_logical: out of memory allocating string for \"%s\" errno %d" , buf, errno);
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
        d.devinfo[j].max_blocksize = devtable[i].devs[j].max_blocksize;
    }
    d.devinfo[j].device_name = 0;

    pd = malloc(sizeof(ftt_descriptor_buf));
    if (pd == 0) {
	ftt_eprintf("ftt_open_logical: out of memory allocating descriptor, errno %d", errno);
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
	ftt_eprintf("ftt_close: called twice on the same descriptor!\n");
	return -1;
    }
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
    d->which_is_open = -3;
    free(d);
    return res;
}
/* This is internal function to make ftt_open_dev shorter and clear */
static int
ftt_open_status (ftt_descriptor d ) {
	int status_res = 0;
/*
	** it looks like we should just do a readonly open if we're a 
	** read-only, descriptor and a read/write open if we're read/write 
	** descriptor.  
	**
	** Unfortunately on some platforms a read/write open on a write 
	** protected tape will fail.  So to make it behave the same 
	** everywhere, if we are opening read/write, we first make it 
	** readonly, and check for write protection.  We let ftt_status
	** call us recursively if it needs the device open; we won't
	** recurse infinitely 'cause the recursive call will be readonly.
	** we dont go ahead and open it readonly 'cause status may need
	** the scsi device open instead.
	**
	** Also, we need to set density if the drive is opened read/write
	** and it is a different density than we currently have.  This
	** needs to fail if we're not at BOT.  If we're readonly, drives
	** will autosense, so we ignore the density we've been given.
	**
	** The more disgusting qualities of the following are due to
	** the fact that 
	** 1) changing density in mid-tape doesn't work
	** 2) setting densities on AIX causes the next
	**    open to rewind (thats right, the next *open*),
	**    even if you are setting it to the same density.
	**    therefore we go to great lengths to make sure
	**    we only change density if we need to, at BOT
	**    when we are read/write...
	*/
	if (d->readonly == FTT_RDWR ) {

			d->readonly = FTT_RDONLY;

	    /* note that this will lead to either a 1-deep recursive call
	       (which can't get here 'cause it is now read-only) to open 
	       the regular device, *or* a scsi open to get status that way */

		    status_res = ftt_status(d,0);
			DEBUG3(stderr,"ftt_status returned %d\n", status_res);

		/* close dev and scsi dev in case ftt_status used them... */
			ftt_close_dev(d);
	
		/* put back readonly flag */
			d->readonly = FTT_RDWR;

	    /* coimplain if neccesary */
			if (status_res & FTT_PROT) {
				ftt_errno = FTT_EROFS;
				ftt_eprintf("ftt_open_dev: called with a read/write ftt_descriptor and a write protected tape.");
				return -1;
			}
	    
		} /* end taking status */
	return status_res;
}
/* this is the internal function to make ftt_open_dev shorter and clear */
static int 
ftt_open_set_blocksize (ftt_descriptor d) {
	int res = 0;
	if (-1 != d->default_blocksize || d->default_blocksize != d->current_blocksize ) {
	    res = ftt_set_blocksize(d, d->default_blocksize);
	    if (res < 0) {
	       return res;
	    } 
		
	    d->current_blocksize = d->default_blocksize;
	}
	return 0;
}
/* this is the internal function to make ftt_open_dev shorter and clear */
static int 
ftt_open_set_mode (ftt_descriptor d,int status_res) {
	int res = 0;
	/*
	 * set density *regardless* of read/write, it may matter 
	 * mainly for OCS, who may be doing ocs_setdev before doing
	 * a mount -- the tape we have may be readonly, etc. but we
	 * may be setting it for the *next* tape
	 */
	if (d->flags & FTT_FLAG_NO_DENSITY) {
	     /* pretend we already did it ... */
	     d->density_is_set = 1;
	}

	if (!d->density_is_set) {
	    res = ftt_set_compression(d,d->devinfo[d->which_is_default].mode);
	    if (res < 0) {
			return res;
	    }
	    if (ftt_get_hwdens(d,d->devinfo[d->which_is_default].device_name) 
				!= d->devinfo[d->which_is_default].hwdens) {
			if ((status_res & FTT_ABOT)|| !(status_res & FTT_ONLINE)) {
				DEBUG3(stderr,"setting density...\n");
				res = ftt_set_hwdens(d, d->devinfo[d->which_is_default].hwdens);
				if (res < 0) {
					return res;
				}
				d->density_is_set = 1;
			} else {
				ftt_errno = FTT_ENOTBOT;
				ftt_eprintf("ftt_open_dev: Need to change tape density for writing, but not at BOT");
				return -1;
			}
	    } else {
			d->density_is_set = 1;
	    }
	}
	return 0;
}
/*
 * This function just open device 
 */
int
ftt_open_io_dev(ftt_descriptor d) {
	
    ENTERING("ftt_open_io_dev");
    CKNULL("ftt_descriptor", d);

    if (d->which_is_default < 0 ) {
		ftt_errno = FTT_EFAULT;
		ftt_eprintf("ftt_open_io_dev: called with invalid (closed?) ftt descriptor");
		return -1;
    }
	/* correnct  device is already open */
	if ( d->which_is_open == d->which_is_default ) return 0;

	/* different device is open - this shouldn't happend and this is why it is checked */
	if ( d->which_is_open >= 0 ) {
		ftt_errno = FTT_EFAULT;
			ftt_eprintf("ftt_open_io_dev: called when the different device is open");
		return -1;
    }

	d->which_is_open = d->which_is_default;
    DEBUG1(stderr,"Actually opening\n");

#ifndef WIN32

	d->file_descriptor = open(
		d->devinfo[d->which_is_default].device_name,
		(d->readonly?O_RDONLY:O_RDWR)|FNONBLOCK|O_EXCL,
		0);
	if ( d->file_descriptor < 0 ) { /* file wasn't open */
			d->file_descriptor = ftt_translate_error(d,FTT_OPN_OPEN, "an open() system call",
													 d->file_descriptor, "ftt_open_dev",1);

#else /* This is NT part */
	{
		HANDLE fh;
		fh =  CreateFile(d->devinfo[d->which_is_default].device_name,
						(d->readonly)? GENERIC_READ : GENERIC_WRITE | GENERIC_READ,	
						0,0,OPEN_EXISTING,0,NULL);
		d->file_descriptor = (int)fh;
	}
	if ( (HANDLE)d->file_descriptor ==  INVALID_HANDLE_VALUE ) {
		/* file wasn't open */
	    ftt_translate_error_WIN(d,FTT_OPN_OPEN, "CreateFile system call",GetLastError(), "ftt_open_dev",1);
#endif
		
	    d->which_is_open = -1;
		return -1;
	}
	return 0;
}
int
ftt_open_dev(ftt_descriptor d) {
    int status_res = 0;

    ENTERING("ftt_open_dev");
    CKNULL("ftt_descriptor" , d);

    if (d->which_is_default < 0 ) {
		ftt_errno = FTT_EFAULT;
		ftt_eprintf("ftt_open_dev: called with invalid (closed?) ftt descriptor");
		return -1;
    }

    /* can't have scsi passthru and regular device open at the same time */
    ftt_close_scsi_dev(d);


	if ( d->which_is_open >= 0 ) { 
		if ( d->which_is_open != d->which_is_default ) {
			/* different device is open -close it */
			if ( 0 > ftt_close_dev(d) ) return -1 ;
		}
	} else {
	/* Now no device is open */
		if ( 0 > ( status_res = ftt_open_status(d) )) {
			return status_res;
		}
		if (! (d->flags&FTT_FLAG_MODE_AFTER) ) { 
			if ( 0> ftt_open_set_mode (d,status_res)  ) return -1;
		}
		if (!(d->flags&FTT_FLAG_BSIZE_AFTER) ) {
			if ( 0 > ftt_open_set_blocksize(d) ) return -1;
		}
	}
	/* 
	** now we've checked for the ugly read-write with write protected
	** tape error, and set density if needed, we can go on and open the 
	** device with the appropriate flags.
	*/
	if ( 0 > ftt_open_io_dev(d) ) return -1;

    if ( d->flags&FTT_FLAG_MODE_AFTER ) {
		if ( 0 > ftt_open_set_mode (d,status_res) ) return -1;
	}
	if ( d->flags&FTT_FLAG_BSIZE_AFTER ) {
		if ( 0 > ftt_open_set_blocksize(d) ) return -1;
	}
	    
    DEBUG2(stderr,"Returing %d\n", d->file_descriptor);
    return d->file_descriptor;
}

/*
 * set compression, mode and  blocksize
 */
int
ftt_setdev(ftt_descriptor d) {

    int  status_res;

    ENTERING("ftt_setdev");
    CKNULL("ftt_descriptor",d);

	status_res = ftt_status(d,0);
	(void)ftt_close_dev(d);

	DEBUG3(stderr,"ftt_status returned %d\n", status_res);
	if (status_res < 0) {
		/* should we fail here or ??? */
		return status_res;
	}
	if ( 0 > ftt_open_set_mode(d,status_res) ) return -1;

	if ( 0 > ftt_open_set_blocksize(d)       ) return -1;

    return 0;
}

int
ftt_close_io_dev(ftt_descriptor d) {
    int res = 0;
    extern int errno;

    ENTERING("ftt_close_io_dev");
    CKNULL("ftt_descriptor", d);

    if ( d->which_is_open >= 0 ){
		ftt_write_fm_if_needed(d);
        DEBUG1(stderr,"Actually closing\n");

#ifndef WIN32
		res = close(d->file_descriptor); 
		DEBUG2(stderr,"close returns %d errno %d\n", res, errno);
#else
		res = (CloseHandle((HANDLE)d->file_descriptor)) ? 0 : -1;
		DEBUG2(stderr,"close returns %d errno %d\n", res, (int)GetLastError());
#endif

		d->which_is_open = -1;
		d->file_descriptor = -1;
    }
    return res;
}

int
ftt_close_dev(ftt_descriptor d) {
    int res;
	
	res = ftt_close_io_dev(d);
    if (res < 0) return res;
    res = ftt_close_scsi_dev(d);
    return res;
}
