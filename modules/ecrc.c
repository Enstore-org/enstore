/*  $Id$ */

/* Macros for Large File Summit (LFS) conformance. */
#define _FILE_OFFSET_BITS 64
#define _LARGEFILE_SOURCE 1

#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>

#define BUF_SIZE 1048576L


extern unsigned int adler32(unsigned int, char *, unsigned int);

int main(int argc, char **argv)
{
    /*Declare variables.*/
    long buf_size = BUF_SIZE;   /*buffer size for the data blocks*/
    unsigned int crc;           /*used to hold the crc as it is updated*/
    long nb, rest, i;           /*loop control variables*/
    struct stat sb;             /*used with fstat()*/
    int f;                      /*the file descriptor*/
    char buf[BUF_SIZE];         /*the data buffer*/

    /*Make sure the user entered a file to check.*/
    if( (argc < 1) || (!argv[1]) )
    {
	printf("Usage %s <file_name>\n", argv[0]);
	exit(1);
    }
    
    /*Check the file.*/
    if((f = open(argv[1], O_RDONLY)) < 0)
    {
        printf("Unable to open file %s: %s\n", argv[1], strerror(errno));
	exit(1);
    }
    if(fstat(f, &sb) < 0)
    {
        printf("Unable to stat file %s: %s\n", argv[1], strerror(errno));
	exit(1);
    }
    if(!(S_ISREG(sb.st_mode)))
    {
        printf("Operation permitted only for regular file.\n");
        exit(1);
    }
    
    /*Initialize values used looping through reading in the file.*/
    nb = sb.st_size / buf_size;
    rest = sb.st_size % buf_size;
    crc = 0;

    /*Print a begin message with relavent information.*/
    printf("size %lld buf_size %ld blocks %ld rest %ld\n",
	   (long long)sb.st_size, buf_size, nb, rest);

    /*Read in the file in 'buf_size' sized blocks and calculate CRC.*/
    for (i = 0;i < nb; i++){
	read(f, buf, buf_size);
	crc = adler32(crc, buf, buf_size);
    }
    if (rest){
	read(f, buf, rest);
	crc = adler32(crc, buf,rest);
    }
    
    /*Print the caclulated CRC.*/
    printf("CRC %u\n", crc);

    return 0;
}
