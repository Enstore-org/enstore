/* $Id  
  Wrap file data in cpio odc format
*/

/* Functions in this module:
   cpio_start(filename, header_name)
   cpio_next_block(buf, blocksize)
*/

/* Note - this code is NOT reentrant/thread-safe.  There can
   only be one CPIO operation going on at a time.  Also it only
   works for regular files (no directories, FIFOs etc) 
*/

#include "volume_import.h"

#define CPIO_MAGIC 070707

static char cpio_header[76+MAX_PATH_LEN+1];
static int cpio_header_len;

static char *cpio_filename=NULL;
static int cpio_pos=0; /*Offset into the generated archive*/
static int early_checksum_done;
static unsigned int file_len;
static unsigned int file_bytes_left, file_bytes_read;
static int cpio_fd = -1;

static char *cpio_trailer = "\
070707000000000000000000000000000000000001\
0000000000000000000001300000000000TRAILER!!!";

static int cpio_trailer_len = 87; /*String above, +1 for terminator*/

void
bytecopy(char *to, char *from, int n){
    while (n-->0)*to++=*from++;
}


#ifdef TESTING
char *progname;
#endif


/* Return  0 on success, -1 on error */
int
cpio_start(char *filename, char *alt_name){
    struct stat sbuf;
    char *header_filename;
    
    header_filename = alt_name?alt_name:filename;

    if (stat(filename,&sbuf)){
	fprintf(stderr, "%s: ", progname);
	perror(filename);
	return -1;
    }

    cpio_filename = filename;
    file_bytes_left = file_len = sbuf.st_size;

    cpio_fd = open(cpio_filename, 0);
    if (cpio_fd<0){
	fprintf(stderr, "%s: ", progname);
	perror(filename);
	return -1;
    }
    
    /*Make the header*/
    sprintf(cpio_header,
	    "%06o%06o%06lo%06lo%06lo%06lo%06lo%06o%011lo%06lo%011lo%s",
	    CPIO_MAGIC,
	    (unsigned int)sbuf.st_dev & 0xFFFF,
	    (unsigned int)sbuf.st_ino & 0xFFFF,
	    (unsigned int)sbuf.st_mode& 0xFFFF,
	    (unsigned int)sbuf.st_uid & 0xFFFF,
	    (unsigned int)sbuf.st_gid & 0xFFFF,
	    (unsigned int)sbuf.st_nlink & 0xFFFF,
	    (unsigned int)sbuf.st_rdev & 0xFFFF,
	    (unsigned int)sbuf.st_mtime,
	    strlen(header_filename)+1,
	    (unsigned int)sbuf.st_size,
	    header_filename);

#ifdef DEBUG
    printf("%s\n",cpio_header);
#endif
    cpio_header_len = strlen(cpio_header)+1;
    checksum = early_checksum = early_checksum_done = 0;
    cpio_pos = 0;
    file_bytes_read = 0;
    return 0;
}

/*Returns 0 on success, -1 on error*/
static int
cpio_read_file(char *read_buffer, int nbytes){
    int bytes_read;

    if (nbytes<=0)
	return 0;
    if (nbytes>file_bytes_left){
	fprintf(stderr,
		"%s: read: requesting %d bytes from file %s, only %d bytes left\n",
		progname, nbytes, cpio_filename, file_bytes_left);
	nbytes=file_bytes_left;
    }
    bytes_read = read(cpio_fd, read_buffer, nbytes);
    if (bytes_read<0){
	fprintf(stderr, "%s: read:", progname);
	perror(cpio_filename);
	return -1;
    } else if (bytes_read != nbytes){
	fprintf(stderr,
		"%s: read: requested %d bytes from file %s, read %d bytes\n",
		progname, nbytes, cpio_filename, bytes_read);
    }
    
    if (!early_checksum_done){
	if (file_bytes_read+bytes_read >= early_checksum_size){
	    /* finish early checksum */
	    early_checksum = adler32(early_checksum, read_buffer,
				     early_checksum_size - file_bytes_read);
	    early_checksum_done = 1;
	} else {
	    early_checksum = adler32(early_checksum, read_buffer, nbytes);
	}
    }
    checksum = adler32(checksum, read_buffer, nbytes);
    file_bytes_read += bytes_read;
    file_bytes_left -= bytes_read;

    verbage("handled %d bytes, %d bytes left\nchecksums: %u, %u\n", file_bytes_read, 
	       file_bytes_left, early_checksum, checksum);

    return 0;
}

	
/* returns number of bytes read, 0 on last block, -1 on error */
int 
cpio_next_block(char *cpio_buffer, int cpio_buffer_len){
    
    int nbytes=0;
    int cpio_buffer_pos = 0;
    int trailer_pos = 0;

    if (cpio_pos<cpio_header_len){  
	/* In the header */
	nbytes = min(cpio_buffer_len, cpio_header_len-cpio_pos);
	bytecopy(cpio_buffer+cpio_buffer_pos, cpio_header+cpio_pos, nbytes);
	cpio_pos += nbytes;
	cpio_buffer_pos += nbytes;
    }
    
    if (file_bytes_left && cpio_buffer_pos < cpio_buffer_len){
	/* Room for some file data */
	nbytes = min(cpio_buffer_len-cpio_buffer_pos, file_bytes_left);
	if (cpio_read_file(cpio_buffer+cpio_buffer_pos, nbytes))
	    return -1;
	cpio_buffer_pos += nbytes;
	cpio_pos += nbytes;
    }
    
    if (!file_bytes_left && cpio_buffer_pos < cpio_buffer_len){
	/* Room for some trailer data */
	trailer_pos = cpio_pos-(cpio_header_len+file_len);
	nbytes = min(cpio_buffer_len-cpio_buffer_pos,  
		     cpio_trailer_len-trailer_pos);
	bytecopy(cpio_buffer+cpio_buffer_pos, cpio_trailer+trailer_pos, nbytes);
	cpio_buffer_pos+=nbytes;
	cpio_pos += nbytes;
    }
    return cpio_buffer_pos;
}






	

/***

    cpio odc format
    
    Offset             Field Name           Length          Notes
    0                        c_magic               6                     070707
    6                        c_dev                    6
    12                     c_ino                     6
    18                     c_mode                6
    24                     c_uid                     6
    30                     c_gid                     6 
    36                     c_nlink                  6
    42                     c_rdev                   6
    48                     c_mtime             11
    59                     c_namesize        6         count includes terminating NUL in pathname
    65                     c_filesize          11         must be 0 for FIFOs and directories
    76                     filename 

***/


#ifdef TESTING

int
main(int argc, char **argv){
    int n;
    int cpio_buffer_len;
    char *cpio_buffer;
    
    progname=argv[0];
    cpio_buffer_len=atoi(argv[1]);
    cpio_buffer = (char*)malloc(cpio_buffer_len);
    cpio_start(argv[2],NULL);
    while ((n=cpio_next_block(cpio_buffer,cpio_buffer_len))>0){
	write(1,cpio_buffer,n);
    }
    return 0;
}

#endif
