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

char *volumeName = NULL;

int getRecord(char *buffer, int buffer_size, int src)
{
  int n;
  int record_length;
  n = dc_read(src, &record_length, sizeof(int));
  n = dc_read(src, buffer, record_length);
  n = dc_read(src, &record_length, sizeof(int));
  return n>0?record_length:n;
}

int putRecord(char *buffer, int buffer_size, int dest)
{

  int n;
  int record_length;
  n = dc_write(dest, &record_length, sizeof(int));
  n = dc_write(dest, buffer, record_length);
  n = dc_write(dest, &record_length, sizeof(int));
  return n>0?record_length:n;
}

void setVolumeName(const char *s)
{

   if( (s == NULL) || (getenv("VOLUME_NAME") != NULL) )
        return;

   if( volumeName != NULL )
        free(volumeName);
		
    volumeName = (char *)strdup(s);       
}

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
