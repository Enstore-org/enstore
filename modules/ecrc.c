#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>

int main(int argc, char **argv)
{
    /*Declare variables.*/
    int buf_size=1024*1024;     /*buffer size for the data blocks*/
    unsigned int crc;           /*used to hold the crc as it is updated*/
    long nb, rest, i;           /*loop control variables*/
    struct stat sb;             /*used with fstat()*/
    int f;                      /*the file descriptor*/
    char buf[1024*1024];        /*the data buffer*/

    /*Make sure the user entered a file to check.*/
    if (!argv[1]){
	printf("usage %s file_name\n",argv[0]);
	exit(1);
    }
    
    /*Check the file.*/
    if((f = open(argv[1], O_RDONLY)) < 0)
    {
        printf("unable to open file %s: %s\n",argv[1],strerror(errno));
	exit(1);
    }
    if(fstat(f, &sb) < 0)
    {
        printf("unable to stat file %s: %s\n",argv[1],strerror(errno));
	exit(1);
    }

    /*Initialize values used looping through reading in the file.*/
    nb = sb.st_size/buf_size;
    rest = sb.st_size%buf_size;
    crc = 0;

    /*Print a begin message with relavent information.*/
    printf("size %ld buf_size %ld blocks %ld rest %ld\n",
	   sb.st_size, buf_size, nb, rest);

    /*Read in the file in 'buf_size' sized blocks and calculate CRC.*/
    for (i = 0;i < nb; i++){
	read(f, buf, buf_size);
	crc = adler32(crc, buf,buf_size);
    }
    if (rest){
	read(f, buf, rest);
	crc = adler32(crc, buf,rest);
    }
    
    /*Print the caclulated CRC.*/
    printf("CRC %u\n",crc);
	
    
    
}
