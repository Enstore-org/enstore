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
    int bytes_read;
    int nbytes;
    int total_bytes = 0;
    unsigned int checksum = 0;
    int early_checksum_done = 0;
    int early_checksum_size;
    unsigned int early_checksum = 0;

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
    
    if (write_db_i(path,"size", size)) 
	goto cleanup;
    if (write_db_s(path,"source", filename))
	goto cleanup;
    if (write_db_s(path,"pnfs_dir", pnfs_dir))
	goto cleanup;
    early_checksum_size = min(size, EARLY_CHECKSUM_SIZE);
    if (write_db_i(path,"early_checksum_size", early_checksum_size)){
	goto cleanup;
    }
    
    if (! (fp=fopen(filename,"r")))
	goto cleanup;

    while (total_bytes<size){
	nbytes = min(size-total_bytes, blocksize);
	bytes_read = fread(read_buffer, 1, nbytes, fp);
	if (bytes_read < nbytes){
	    fprintf(stderr, "%s: %s: short read\n", progname, filename);
	    goto cleanup;
	}
	if (!early_checksum_done){
	    if (total_bytes+nbytes >= early_checksum_size){
		/* finish early checksum */
		early_checksum = adler32(early_checksum, read_buffer,
					  early_checksum_size - total_bytes);
		if (write_db_i(path,"early_checksum_size", 
			       early_checksum_size))
		    goto cleanup;
		if (write_db_i(path,"early_checksum", early_checksum))
		    goto cleanup;
		
		early_checksum_done = 1;
	    } else {
		early_checksum = adler32(early_checksum, read_buffer, nbytes);
	    }
	}
	checksum = adler32(checksum, read_buffer, nbytes);
	total_bytes += bytes_read;
    }
    
    write_db_i(path, "checksum", checksum);

    /* TODO:  the actual write to tape !*/

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
    
    
    
		    
		    
    
    
