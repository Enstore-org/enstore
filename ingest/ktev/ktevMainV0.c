#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>
#include <unistd.h>
#include "dcap.h"
#include "ktevApiV0.h"

#define O_BINARY 0
extern int getopt(int, char * const *, const char *);
/* for getopt */
extern char *optarg;
extern int optind;

extern char *volumeName;

static
void usage()
{
  fprintf(stderr,"ktev test program\n");
  fprintf(stderr,"must pass volume as only parameter\n");
}

int
main(int argc, char *argv[])
{
  int c,src,record_size,fileError;
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

  while((fileError = getFile(volumeName, fileName)) > 0)
    {
      src = dc_open(fileName,O_RDONLY | O_BINARY );

      record_size = getRecord(buffer, 75000, src);
      while(record_size > 0) {
	printf("record_size: %d\n", record_size);
	record_size = getRecord(buffer, 75000, src);
      }
    }
  if(fileError == -1) {
    return -1;
  }
  else{
    return 0;
  }
}
