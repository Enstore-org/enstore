#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>
#include <unistd.h>
#include "dcap.h"

#define PNFS_ROOT getenv("DCACHE_ROOT")?getenv("DCACHE_ROOT"):"/pnfs/ktev/migrated_from_dlt/"
#define FILENAME_LEN 1024
#define O_BINARY 0
extern int getopt(int, char * const *, const char *);
/* for getopt */
extern char *optarg;
extern int optind;

extern char *volumeName = NULL;

int get_record(char *buffer, int buffer_size, int src)
{

  int n;
  int record_length;
  n = dc_read(src, &record_length, sizeof(int));
  n = dc_read(src, buffer, record_length);
  n = dc_read(src, &record_length, sizeof(int));
  return n>0?record_length:n;
}

int put_record(char *buffer, int buffer_size, int dest)
{

  int n;
  int record_length;
  n = dc_write(dest, &record_length, sizeof(int));
  n = dc_write(dest, buffer, record_length);
  n = dc_write(dest, &record_length, sizeof(int));
  return n>0?record_length:n;
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
  char *path = malloc(strlen(volumeName) + strlen(PNFS_ROOT) + 11);
  char firstTwo[2], firstFour[4];
  static DIR  *dir = 0;
  struct dirent *dirent;
  struct stat stat_buf;

  strncpy(firstTwo, volumeName, 2);
  strncpy(firstFour, volumeName, 4);

  strcpy(path, PNFS_ROOT);
  strcat(path, "/");
  strncat(path, firstTwo, 2);
  strcat(path, "/");
  strncat(path, firstFour, 4);
  strcat(path, "/");
  strcat(path, volumeName);
  strcat(path, "/");

  if(!dir) 
    {
      printf("opening: %s\n", path);
      dir = opendir(path);
    }

  if(dir) 
    {
      dirent = readdir(dir);
      if(dirent)
	{
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
	return -2;
    }
  else 
    {
      return -1;
    }
}

int
main(int argc, char *argv[])
{
  int c,src,record_size;
  char fileName[FILENAME_LEN];
  char buffer[75000];

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

  while(getFile(volumeName, fileName) > 0)
    {
      printf("Found file: %s\n", fileName );
      src = dc_open(fileName,O_RDONLY | O_BINARY );

      record_size = get_record(buffer, 75000, src);
      while(record_size > 0) {
	printf("record_size: %d\n", record_size);
	record_size = get_record(buffer, 75000, src);
      }
      printf("final record size: %d\n", record_size);
    }
  
  return 0;
}
