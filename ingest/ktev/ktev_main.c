#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>
#include <unistd.h>
#include "dcap.h"
#include "ktevapi.h"

#define O_BINARY 0
/* for getopt */
extern char *optarg;
extern int optind;

extern char *volumeName;
extern errno;
static
void usage(name)
{
  fprintf(stderr,"ktev test program\n");
  fprintf(stderr,"usage:%s [-d local_dir] [-w] <-v vol_name>\n",name);
}

int
main(int argc, char *argv[])
{
  int c,src,record_size,fileError,recs, dst, fn, loc_copy=0;
  int write_test = 0;
  int files_written, records_written;
  char fileName[FILENAME_LEN], odn[FILENAME_LEN], ofn[FILENAME_LEN];
  char buffer[75000];

  if (argc < 2) {
    usage(argv[0]);
    return -1;
  }

  while( (c = getopt(argc, argv, "d:v:w")) != EOF) {
	
    switch(c) {
    case 'd':
	strcpy(odn,optarg);
	loc_copy = 1;
      break;
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
      fileError = PutFile(volumeName, fileName);
      src = dc_open(fileName,O_WRONLY|O_CREAT|O_BINARY, S_IRUSR|S_IWUSR );
      for(records_written=0; records_written < 11; records_written++ ) {
	putRecord(buffer, 75000, src);
      }
    }
    return 0;
  }
  else {
    record_size = 0;
    fn = 0;
    if (loc_copy) {
	strcat(odn,"/");
	strcat(odn,volumeName);

	printf("output dir %s\n",odn);
	if (!opendir(odn)){
	    if( mkdir(odn, S_IRUSR|S_IWUSR|S_IXUSR) < 0) {
		perror(strerror(errno));
		return -1;
	    }
	}
    }
    while(record_size >= 0 && (fileError = GetFile(volumeName, fileName)) > 0) {
	printf("filename %s\n", fileName);
	src = dc_open(fileName,O_RDONLY | O_BINARY );
	if (src < 0) {
	    perror("scandir");
	    return -1;
	}
	
	if (loc_copy) {
	    strcat(ofn, "/");
	    sprintf(ofn,"%s/f%d",odn,fn);
	    printf("destination file %s\n",ofn);
	    dst=open(ofn,O_RDWR | O_CREAT | O_BINARY,0666);
	    if (dst < 0){
		printf("destination file open error\n");
		perror(strerror(errno));
		return -1;
	    }
	}
	
	record_size = getRecord(buffer, 75000, src);
	recs = 0;
	while(record_size > 0) {
	  recs++;  
	  /*printf("record_size: %d\n", record_size);*/
	  record_size = getRecord(buffer, 75000, src);
	  if (loc_copy && record_size > 0){
	      write(dst, buffer, record_size);
	  }
	}
	printf("file %s has %d records\n", fileName, recs);
	dc_close(src);
	close(dst);
	fn++;
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
