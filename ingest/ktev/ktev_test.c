#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>
#include <unistd.h>
#include "dcap.h"

#define PNFS_ROOT "/pnfs/fs/moibenko/ktev/"
#define FILENAME_LEN 1024
#define O_BINARY 0
extern int getopt(int, char * const *, const char *);
/* for getopt */
extern char *optarg;
extern int optind;

extern char *volumeName;

int copyfile(int src, int dest, size_t bufsize, off_t *size)
{
	ssize_t n, m;
	char * cpbuf;
	size_t count;	
	off_t total_bytes = 0;
	size_t off;


	if ( ( cpbuf = malloc(bufsize) ) == NULL ) {
		perror("malloc");
		return -1;
	}

	do{	
		off = 0;
		do{	
			n = dc_read(src, cpbuf + off, bufsize - off);
			if( n <=0 ) break; 
			off += n;
		} while (off != bufsize );
		
		/* do not continue if read fails*/
		if (n < 0) {
			/* Read failed. */
			free(cpbuf);
			return -1;
		}

		if (off > 0) {
			count = 0;

			total_bytes += off;
			while ((count != off) && ((m = dc_write(dest, cpbuf+count, off-count)) > 0))
				count += m;
 
			if (m < 0) {
				/* Write failed. */
				free(cpbuf);
				return -1;
			}
		}

	} while (n != 0);
	
	if(size != NULL) {
		*size = total_bytes;
	}
	
	free(cpbuf);
	return 0;
}

static
void usage()
{
  fprintf(stderr,"ktev test program\n");
  fprintf(stderr,"must pass volume as only parameter\n");
}

static
void setVolumeName(const char *s)
{

   if( (s == NULL) || (getenv("VOLUME_NAME") != NULL) )
        return;

   if( volumeName != NULL )
        free(volumeName);
		
    volumeName = (char *)strdup(s);       
}

static 
int getFile(char *volumeName, char fileName[FILENAME_LEN])
{
  char *path = malloc(strlen(volumeName) + strlen(PNFS_ROOT) + 2);
  static DIR  *dir = 0;
  struct dirent *dirent;
  struct stat stat_buf;

  strcpy(path, PNFS_ROOT);
  strcat(path, volumeName);
  strcat(path, "/");

  if(!dir) 
    {
      printf("Path: %s\n", path);
      dir = opendir(path);
    }

  if(dir) 
    {
      dirent = readdir(dir);
      strncpy(fileName, path, FILENAME_LEN);
      strncat(fileName, dirent->d_name, FILENAME_LEN);
      stat(fileName, &stat_buf);
      while(dirent && S_ISDIR(stat_buf.st_mode) )
	{
	  dirent = readdir(dir);
	  strncpy(fileName, path, FILENAME_LEN);
	  strncat(fileName, dirent->d_name, FILENAME_LEN);
	  stat(fileName, &stat_buf);
	}
      return (int)dirent;
    }
  else 
    {
      return -1;
    }
}

int
main(int argc, char *argv[])
{
  int c,src,dest;
  char fileName[FILENAME_LEN];
  off_t size;

  if (argc < 2) {
    usage();
  }

  while( (c = getopt(argc, argv, "v:")) != EOF) {
	
    switch(c) {
    case 'v':				
      setVolumeName(optarg);
      break;
    }
  }

  printf("Going to process volume: %s\n", volumeName);

  while(getFile(volumeName, fileName))
    {
      printf("Found file: %s\n", fileName );
      src = dc_open(fileName,O_RDONLY | O_BINARY );
      dest = dc_open( "/export/ktev/scratch/file1", 
		      O_WRONLY|O_CREAT|O_TRUNC|O_BINARY, 
		      0666);

      
      return copyfile(src, dest, 1048570L, &size);;
    }
  
  return 0;
}
