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

char *volumeName = NULL;

int get_record(char *buffer, int buffer_size, int src)
{

  int n;
  int record_length;
  n = dc_read(src, &record_length, sizeof(int));
  printf("n: %d, record_length: %2d\n", n, record_length);
  n = dc_read(src, buffer, record_length);
  printf("n: %d, record_length: %2d\n", n, record_length);
  n = dc_read(src, &record_length, sizeof(int));
  printf("n: %d, record_length: %2d\n\n", n, record_length);
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
  int c,src,dest,record_size;
  char fileName[FILENAME_LEN];
  off_t size;
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

  while(getFile(volumeName, fileName))
    {
      printf("Found file: %s\n", fileName );
      src = dc_open(fileName,O_RDONLY | O_BINARY );

      record_size = get_record(buffer, 75000, src);
      while(record_size > 0) {
	record_size = get_record(buffer, 75000, src);
      }
    }
  
  return 0;
}
