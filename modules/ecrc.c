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
#include <limits.h>
#include <libgen.h>

#define BUF_SIZE 1048576L
/* This is the largest 16 bit prime number.  It is used for converting the
 * 1 seeded dcache CRCs with the 0 seeded enstore CRCs. */
#define BASE     65521

extern unsigned int adler32(unsigned int, char *, unsigned int);

#if 0
unsigned int convert_0_adler32_to_1_adler32(unsigned int crc, off_t filesize)
{
    size_t size;
    size_t s1, s2;
    
    /* Modulo the size with the largest 16 bit prime number. */
    size = (size_t)(filesize % BASE);
    /* Extract existing s1 and s2 from the 0 seeded adler32 crc. */
    s1 = (crc & 0xffff);
    s2 = ((crc >> 16) &  0xffff);
    /* Modify to reflect the corrected crc. */
    s1 = (s1 + 1) % BASE;
    s2 = (size + s2) % BASE;
    /* Return the 1 seeded adler32 crc. */
    return (s2 << 16) + s1;
}
#endif /* 0 */

int main(int argc, char **argv)
{
    /*Declare variables.*/
    long buf_size = BUF_SIZE;   /*buffer size for the data blocks*/
    unsigned int crc;           /*used to hold the crc as it is updated*/
    long nb, rest, i;           /*loop control variables*/
    struct stat sb;             /*used with fstat()*/
    int fd;                     /*the file descriptor*/
    char buf[BUF_SIZE];         /*the data buffer*/
    char abspath[PATH_MAX + 1]; /*used to hold program name*/
    int c;                      /*used with getopt(3)*/
    unsigned int adler32_seed = 0U; /*adler32 enstore seed value is zero*/
    int use_hex = 0;            /*use hex output if true, otherwise decimal*/
    int verbose = 0;            /*print out extra information*/
    unsigned int converted_crc; /*used when -a is used*/
    int use_capital_hex = 0;    /*output hexidecimal output in capitals*/

    /* Loop through the options looking for valid switches. */
    while((c = getopt(argc, argv, "01dhHav")) != EOF)
    {
       switch(c)
       {
	  /* -0   This switch is to use 0 seed for adler32. (Enstore uses) */
	  case '0':
	     adler32_seed = 0U;
	     break;
	  /* -1   This switch is to use 1 seed for adler32. (standard) */
	  case '1':
	     adler32_seed = 1U;
	     break;
	  /* -d  This switch is to use decimal output for crc. */
	  case 'd':
	     use_hex = 0;
	     break;
	  /* -h  This switch is to use hexadecimal output for crc. */
	  case 'h':
	     use_hex = 1;
	     use_capital_hex = 0;
	     break;
	  /* -h  This switch is to use hexadecimal output for crc. */
	  case 'H':
	     if(use_hex == 0)
	     {
		use_hex = 1;  /* If not specifed, turn this on. */
	     }
	     use_capital_hex = 1;
	     break;
	  /* -a This switch is to display all four combinations of CRC
	   * to the user. */
	  case 'a':
	     adler32_seed = 0U;
	     use_hex = -1;  /* reuse this variable */
	     break;
	  /* -v  This switch is to display extra information. */
	  case 'v':
	     verbose = 1;
	     break;
	  default:
	     break;
       }
    }
    
    /*Make sure the user entered a file to check.*/
    if(optind >= argc)
    {
        strncpy(abspath, argv[0], PATH_MAX);
	printf("Usage %s [-0 | -1] [-d | -h | -H | -a] [-v] <file_name>\n",
	       basename(abspath));
	exit(1);
    }
    
    /*Check the file.*/
    if((fd = open(argv[optind], O_RDONLY)) < 0)
    {
        printf("Unable to open file %s: %s\n", argv[optind], strerror(errno));
	exit(1);
    }
    if(fstat(fd, &sb) < 0)
    {
        printf("Unable to stat file %s: %s\n", argv[optind], strerror(errno));
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
    crc = adler32_seed;

    /*Print a begin message with relavent information.*/
    if(verbose)
    {
       printf("size %lld buf_size %ld blocks %ld rest %ld\n",
	      (long long)sb.st_size, buf_size, nb, rest);
    }
    
    /*Read in the file in 'buf_size' sized blocks and calculate CRC.*/
    for (i = 0; i < nb; i++){
        if(read(fd, buf, buf_size) < 0)
	{
	    printf("Error reading file: %s\n", strerror(errno));
	    exit(1);
	}
	crc = adler32(crc, buf, buf_size);
    }
    if (rest)
    {
        if(read(fd, buf, rest) < 0)
	{
	    printf("Error reading file: %s\n", strerror(errno));
	    exit(1);
	}
	crc = adler32(crc, buf, rest);
    }

    /*Cleanup.*/
    close(fd);
    
    /*Print the caclulated CRC.*/
    if(use_hex == -1)
    {
       if(use_capital_hex)
       {
	  printf("0 SEEDED CRC: %u (0x%-X)\n", crc, crc);
	  converted_crc = convert_0_adler32_to_1_adler32(crc, sb.st_size);
	  printf("1 SEEDED CRC: %u (0x%-X)\n", converted_crc, converted_crc);
       }
       else
       {
	  printf("0 SEEDED CRC: %u (0x%-x)\n", crc, crc);
	  converted_crc = convert_0_adler32_to_1_adler32(crc, sb.st_size);
	  printf("1 SEEDED CRC: %u (0x%-x)\n", converted_crc, converted_crc);
       }
    }
    else if(use_hex == 1)
    {
       if(use_capital_hex)
	  printf("CRC 0x%-X\n", crc);
       else
	  printf("CRC 0x%-x\n", crc);
    }
    else  /* use_hex == 0 */
       printf("CRC %u\n", crc);

    return 0;
}
