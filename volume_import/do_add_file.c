/* $Id$
   This does the bulk of the work of writing files to the tape and
   making database entries 
*/

#include "volume_import.h"

static void 
rm_rf(char *path){
    char cmd[MAX_PATH_LEN + 8];
    sprintf(cmd, "/bin/rm -rf %s", path);
    verbage("running %s\n", cmd);
    system(cmd); /* XXX I was lazy when I wrote this, there must be a nicer way */
}



int
do_add_file(char *destination, char *source)
{
    char dbpath[MAX_PATH_LEN];
    char buf[80];
    int size;
    struct stat sbuf;
    char *read_buffer;
    int nbytes;



    read_buffer = (char*)malloc(blocksize);
    if (read_buffer == (char*)0){
	fprintf(stderr,"%s: cannot allocate %d bytes for read buffer ", 
		progname, blocksize);
	return -1;
    }

    sprintf(dbpath,"%s/volumes/%s", tape_db, volume_label);
    if (read_db_i(dbpath, "next_file", &file_number)){
	return -1;
    }
    

    /* We already verified all the files when building up the file list, but there's 
     * always the possibility that a file was removed or otherwise changed between 
     * then and now */
    
    if (stat(source,&sbuf)){
	fprintf(stderr, "%s: ", progname);
	perror(source);
	return -1;
    }

    size = sbuf.st_size;
    early_checksum_size=min(size, EARLY_CHECKSUM_SIZE);

    if (cpio_start(source))
	return -1;
   /* terminate when nbytes=0, i.e. we've handled the last block */
    while ( (nbytes=cpio_next_block(read_buffer, blocksize)) ){
	if (nbytes<0){
	    break;
	}
	else {
	    if (write_tape(read_buffer, nbytes) != nbytes){
		nbytes=-1;
		if (errno==ENOSPC){
		    fprintf(stderr,"%s: tape full\n", progname);
		    sprintf(buf,"volumes/%s/tape_full",volume_label);
		    write_db_i(tape_db,buf,1);
		}
		break;
	    }
	}
    }
    
    if (nbytes<0){
	rewind_tape();
	return -1;
    }

    /*use some verify function to do this? */
    sprintf(dbpath,"%s/volumes/%s/%07d", tape_db, volume_label, file_number);
    
    if (mkdir(dbpath, 0775)){
	fprintf(stderr, "%s: cannot create directory ", progname);
	perror(dbpath);
	return -1;
    }
    
    /* Once we start writing into the database we need to make sure that if any 
     * error occurred, we completely undo the partial addition */
    
    sprintf(dbpath,"%s/volumes/%s/%07d", tape_db, volume_label, file_number);
    if (write_db_u(dbpath,"checksum", checksum)
	||write_db_i(dbpath,"blocksize",blocksize)
	||write_db_i(dbpath,"size", size) 
	||write_db_s(dbpath,"source", source)
	||write_db_s(dbpath,"destination", destination)
	||write_db_i(dbpath,"early_checksum_size", early_checksum_size)
	||write_db_u(dbpath,"early_checksum", early_checksum)
	) goto cleanup;
    
    sprintf(buf,"%07d", file_number+1);
    sprintf(dbpath, "%s/volumes/%s", tape_db, volume_label);
    if(write_db_s(dbpath, "next_file", buf))
	goto cleanup;
    
    return 0;
    
  cleanup:
    rm_rf(dbpath);
    return -1;
}
    
    
    
		    
		    
    
    
