#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>

int main(int argc, char **argv)
{
    int buf_size=1024*1024;
    unsigned long crc;
    long nb, rest, i;
    struct stat sb;
    int f;
    char buf[1024*1024];

    if (!argv[1]){
	printf("usage %s file_name\n",argv[0]);
	exit(1);
    }
    f = open(argv[1], O_RDONLY);
    fstat(f, &sb);
    nb = sb.st_size/buf_size;
    rest = sb.st_size%buf_size;
    crc = 0l;
    printf("size %ld buf_size %ld blocks %ld rest %ld\n",sb.st_size, buf_size, nb, rest);
    for (i = 0;i < nb; i++){
	read(f, buf, buf_size);
	crc = adler32(crc, buf,buf_size);
    }
    if (rest){
	read(f, buf, rest);
	crc = adler32(crc, buf,rest);
    }
    printf("CRC %u\n",crc);
	
    
    
}
