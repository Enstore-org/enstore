/* $Id$
Input verification functions
*/

#include "volume_import.h"

int 
verify_tape_device(){
    return 0; /*XXX*/
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
		return -1;
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
	    return -1;
	}
	if ( (sbuf.st_mode & 0700) != 0700){ /* rwx for user? */
	    fprintf(stderr,"%s: insufficent permissions on directory %s\n",
		    progname, path);
	    return -1;
	}
    }
    return 0;
}

int 
verify_tape_db(int makeit){ /* if arg is nonzero, create tape db if needed */
    char path[MAX_PATH_LEN];
    
    /* Make sure that "volumes" is a subdir of tape_db */
    sprintf(path, "%s/volumes", tape_db);
    
    return chkdir(tape_db, makeit)||chkdir(path, makeit);
}


int 
verify_file(char *pnfs_dir, char *filename){
    
    struct stat sbuf;
    int status;

    if (!pnfs_dir){
	fprintf(stderr,"%s: no pnfs directory given\n", progname);
	return -1;
    }
    if (strlen(pnfs_dir)<5 || strncmp(pnfs_dir,"/pnfs",5)){
	fprintf(stderr,"%s: pnfs_dir must start with /pnfs\n", progname);
	return -1;
    }

    status = stat(filename, &sbuf);
    if (status){
	fprintf(stderr,"%s: ", progname);
	perror(filename);
	return -1;
    }
    if (!S_ISREG(sbuf.st_mode)){
	fprintf(stderr,"%s: %s: not a regular file\n", progname, filename);
	return -1;
    }
    if ( (sbuf.st_mode & 0400)  != 0400){
	fprintf(stderr,"%s: %s: no read permission\n", progname, filename);
	return -1;
    }
    if ( (sbuf.st_size >= 2*GB) ){
	fprintf(stderr,"%s: %s: file size larger than 2GB\n", progname, 
		filename);
	return -1;
    }

    return 0;
}

int
check_volume_label_legal(){
    char *cp;
    if (!volume_label){ /* shouldn't happen, protect against it anyway */
	fprintf(stderr,"%s: no volume label given\n", progname);
	return -1;
    }
    if (strlen(volume_label)>MAX_LABEL_LEN){
	fprintf(stderr,"%s: volume label too long (%d character max)\n",
		progname, MAX_LABEL_LEN);
	return -1;
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
	    return -1;
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
    if (status){ /* it doesn't exist, make it */
	if (new){
	    if (chkdir(path, 1)
		||write_db_s(path,"next_file","0000000")
		) return -1;
	    return 0;
	}
	
	fprintf(stderr,"%s: directory %s does not exist.\n%s",
		progname, path,
		"Has this volume been initialized?\n");
	return -1;
    } else { /* it exists */
	if (!new) 
	    return 1;
	fprintf(stderr,"%s: directory %s already exists.\n%s",
		progname, path,
		"Use '-E' (erase) option to delete it\n");
	return -1;
    }
    return 0;
}

/* check that the volume label given matches what's on the tape */
int verify_tape_volume()
{
    char path[MAX_PATH_LEN];
    char label[80];
    /* verify_tape_db should already have been called, so we don't need to check that
       tape_db and volume_label are non-NULL */
    sprintf(path,"%s/volumes/%s", tape_db, volume_label);
    
    if (read_db_i(path, "next_file", &file_number)
	||open_tape())
	return -1;
    
    /* try to read a volume label, either VOL1 or EOT1 */
    if (read_tape(label,80) != 80){
	
    }
    
    close_tape(); /*XXX we open it again later...*/
    return 0;
}

int 
verify_volume_label()
{
    return 
 	verify_db_volume(0)
	|| verify_tape_volume();
}
    
