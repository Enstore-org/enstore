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
  fprintf(stderr,"must pass volume as only parameter\nor volume and w for write tests");
}

int
main(int argc, char *argv[])
{
  int c,src,record_size,fileError;
  int write_test = 0;
  int files_written, records_written;
  char fileName[FILENAME_LEN];
  char buffer[75000];

  if (argc < 2) {
    usage();
  }

  while( (c = getopt(argc, argv, "v:w")) != EOF) {
	
    switch(c) {
    case 'v':				
      setVolumeName(optarg);
      break;
    case 'w':				
      write_test = 1;
      break;
    }
  }

  if( write_test ) {
    for(files_written=0; files_written < 11; files_written ++ ) {
      fileError = nextPutFile(volumeName, fileName);
      src = dc_open(fileName,O_WRONLY|O_CREAT|O_BINARY, S_IRUSR|S_IWUSR );
      for(records_written=0; records_written < 11; records_written++ ) {
	putRecord(buffer, 75000, src);
      }
    }
    return 0;
  }
  else {
    record_size = 0;
    while(record_size >= 0 && (fileError = nextGetFile(volumeName, fileName)) > 0)
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
    else if(record_size <0){
      return record_size;
    }
    else {
      return 0;
    }
  }
}
