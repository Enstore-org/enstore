/* $Id$
   This does the bulk of the work of writing files to the tape and
   making database entries 
*/

#include "volume_import.h"

static void 
rm_rf(char *path){
    char cmd[MAX_PATH_LEN + 8];
    sprintf(cmd, "/bin/rm -rf %s", path);
    system(cmd); /* XXX I was lazy when I wrote this, there must be a nicer way */
}



int
do_add_file(char *pnfs_dir, char *filename)
{
    int file_number; /* index into this volume */
    char path[MAX_PATH_LEN];
    int size;
    struct stat sbuf;
    FILE *fp;
    char *read_buffer;
    int nbytes;

    read_buffer = (char*)malloc(blocksize);
    if (read_buffer == (char*)0){
	fprintf(stderr,"%s: cannot allocate %d bytes for read buffer ", 
		progname, blocksize);
	return -1;
    }
	
    sprintf(path,"%s/volumes/%s/next_file", tape_db, volume_label);
    fp = fopen(path, "r");
    if (!fp){
	file_number = 0;
    } else {
	if (fscanf(fp,"%d", &file_number) != 1){
	    fprintf(stderr,"%s: %s exists but cannot read contents\n", progname,
		    path);
	    return -1;
	}
	fclose(fp);
    }


    fp = fopen(path, "w");
    if (!fp){
	fprintf(stderr, "%s: cannot write ", progname);
	perror(path);
	return -1;
    }
    fprintf(fp,"%07d\n",file_number+1);
    fclose(fp);

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
	||cpio_start(filename))
	goto cleanup;
    
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
	||write_db_u(path,"checksum", checksum))
	goto cleanup;
    
    return 0;
    
 cleanup:
    rm_rf(path);
    sprintf(path,"%s/volumes/%s/next_file", tape_db, volume_label);
    fp = fopen(path, "w");
    if (!fp){
	fprintf(stderr, "%s: cannot write ", progname);
	perror(path);
    } else {
	fprintf(fp,"%07d\n",file_number);
	fclose(fp);
    }
    return -1;
}
    
    
    
		    
		    
    
    
