/* $Id$
Input verification functions
*/

/* The verify_ functions will return 0, or else exit with a -1 */
/* All other functions return 0 on success, -1 on failure and won't exit */

#include "volume_import.h"

int 
verify_tape_device(){
    return 1; /*XXX*/
}

static int 
chkdir(char *path, int makeit){
    struct stat sbuf;
    int status;
    
    /* does path exist ?*/
    status = stat(path, &sbuf);
    if (status){	/* no */
	if (makeit){       /* try to make it */
	    if (mkdir(path, DEFAULT_PERM)){
		fprintf(stderr, "%s: cannot create directory: ", progname);
		perror(path);
		exit(-1);
	    }
	    if (verbose){
		printf("%s: created directory %s\n", progname, path);
	    }
	} else {           /* fatal error */
	    fprintf(stderr, "%s: ", progname);
	    perror(tape_db);
	}
    } else {            /*yes*/
	if (!S_ISDIR(sbuf.st_mode)){  /* is it a dir ? */
	    fprintf(stderr,"%s: %s is not a directory\n", progname, path);
	    exit(-1);
	}
	if ( (sbuf.st_mode & 0700) != 0700){ /* rwx for user? */
	    fprintf(stderr,"%s: insufficent permissions on directory %s\n",
		    progname, path);
	exit(-1);
	}
    }
    return 1;
}

int 
verify_tape_db(int makeit){ /* if arg is nonzero, create tape db if needed */
    char path[MAX_PATH_LEN];

    chkdir(tape_db, makeit);
    
    /* Check if "volumes" is a subdir of tape_db */
    sprintf(path, "%s/volumes", tape_db);
    chkdir(path, makeit);
    return 1;
}


int 
verify_file(char *pnfs_dir, char *filename){
    
    struct stat sbuf;
    int status;

    if (!pnfs_dir){
	fprintf(stderr,"%s: no pnfs directory given\n", progname);
	exit(-1);
    }
    if (strlen(pnfs_dir)<5 || strncmp(pnfs_dir,"/pnfs",5)){
	fprintf(stderr,"%s: pnfs_dir must start with /pnfs\n", progname);
	exit(-1);
    }

    status = stat(filename, &sbuf);
    if (status){
	fprintf(stderr,"%s: ", progname);
	perror(filename);
	exit(-1);
    }
    if (!S_ISREG(sbuf.st_mode)){
	fprintf(stderr,"%s: %s: not a regular file\n", progname, filename);
	exit(-1);
    }
    if ( (sbuf.st_mode & 0400)  != 0400){
	fprintf(stderr,"%s: %s: no read permission\n", progname, filename);
	exit(-1);
    }
    if ( (sbuf.st_size >= 2*GB) ){
	fprintf(stderr,"%s: %s: file size larger than 2GB\n", progname, 
		filename);
	exit(-1);
    }

    return 1;
}

int
check_volume_label_legal(){
    char *cp;
    if (!volume_label){ /* shouldn't happen, protect against it anyway */
	fprintf(stderr,"%s: no volume label given\n", progname);
	exit(-1);
    }
    if (strlen(volume_label)>MAX_LABEL_LEN){
	fprintf(stderr,"%s: volume label too long (%d character max)\n",
		progname, MAX_LABEL_LEN);
	exit(-1);
    }
    
    for (cp=volume_label; *cp; ++cp){
	if (*cp >='a' && *cp<='z')
	    continue;
	if (*cp >='A' && *cp<='Z')
	    continue;
	if (*cp >='0' && *cp<='9')
	    continue;
	switch (*cp){
	case '_':
	case '-':
	case '#':
	case '$':
	case '.':
	    continue;
	    break;
	default:
	    fprintf(stderr,"%s: illegal character %c in volume label\n", 
		    progname, *cp);
	    exit(-1);
	}
    }
    return 0;
}


/* check if  volume_label exists in the database */
int 
verify_db_volume(int new) /* if new, verify that the dir does *not* yet exist*/
{
    struct stat sbuf;
    int status;
    char path[MAX_PATH_LEN];  

    check_volume_label_legal(); /* need to make sure '/' is not in label! */

    sprintf(path,"%s/volumes/%s", tape_db, volume_label);
    status = stat(path, &sbuf);
    if (status){ /* it doesn't exist */
	if (new) 
	    return 1;
	
	fprintf(stderr,"%s: directory %s does not exist.\n%s",
		progname, path,
		"Has this volume been initialized?\n");
	exit(-1);
    } else { /* it exists */
	if (!new) 
	    return 1;
	fprintf(stderr,"%s: directory %s already exists.\n%s",
		progname, path,
		"Use <XXX> option to delete it\n");
	exit(-1);
    }
    return 1;
}

/* check that the volume label given matches what's on the tape */
int verify_tape_volume()
{
    return 1;
}

int 
verify_volume_label()
{
    verify_db_volume(0);
    verify_tape_volume();
    return 1;
}
    
