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
do_add_file(char *pnfs_dir, char *filename)
{
    char path[MAX_PATH_LEN];
    char buf[10];
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

    
    sprintf(path,"%s/volumes/%s", tape_db, volume_label);
    if (read_db_i(path, "next_file", &file_number)){
	return -1;
    }
    
    sprintf(buf,"%07d", file_number+1);
    
    if (write_db_s(path, "next_file", buf)){
	perror(path);
	return -1;
    }
    
    
    /*use some verify function to do this XXX */
    sprintf(path,"%s/volumes/%s/%07d", tape_db, volume_label, file_number);
    
    if (mkdir(path, 0775)){
	fprintf(stderr, "%s: cannot create directory ", progname);
	perror(path);
	return -1;
    }
    

    /* We already verified all the files when building up the file list, but there's 
     * always the possibility that a file was removed or otherwise changed between 
     * then and now */
    
    if (stat(filename,&sbuf)){
	fprintf(stderr, "%s: ", progname);
	perror(filename);
	return -1;
    }

    size = sbuf.st_size;

    /* Once we start writing into the database we need to make sure that if any 
     * error occurred, we completely undo the partial addition */

    if (write_db_i(path,"size", size) 
	||write_db_s(path,"source", filename)
	||write_db_s(path,"pnfs_dir", pnfs_dir)
	||write_db_i(path,"early_checksum_size", 
		     early_checksum_size=min(size, EARLY_CHECKSUM_SIZE))
	||cpio_start(filename)
	) goto cleanup;
    
    /* terminate when nbytes=0, i.e. we've handled the last block */
    while ( (nbytes=cpio_next_block(read_buffer, blocksize)) ){
	if (nbytes<0){
	    break;
	}
	else {
	    if (write_tape(read_buffer, nbytes) != nbytes){
		nbytes=-1;
		break;
	    }
	}
    }
    
    if (nbytes<0
	||write_db_u(path,"early_checksum", early_checksum)
	||write_db_u(path,"checksum", checksum)
	) goto cleanup;
    
    return 0;
    
 cleanup:
    rm_rf(path);
    sprintf(path,"%s/volumes/%s", tape_db, volume_label);
    sprintf(buf,"%07d",file_number);
    write_db_s(path, "next_file", buf);
    return -1;
}
    
    
    
		    
		    
    
    
