#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>
#include <unistd.h>
#include <errno.h>

#include "dcap.h"
#include "ktevapi.h"

#define O_BINARY 0
/* for getopt */
extern char *optarg;
extern int optind;

/*extern char *volumeName;*/
extern errno;

static
void usage(char *name)
{
  fprintf(stderr,"ktev test program\n");
  fprintf(stderr,"usage:%s [-d local_dir] [-w] [-s] <-v vol_name>\n",name);
}

int
main(int argc, char *argv[])
{
  int c,src,record_size,fileError,recs, dst=0, fn, loc_copy=0;
  int write_test = 0, stage_test = 0;
  int files_written, records_written;
  char fileName[FILENAME_LEN], odn[FILENAME_LEN], ofn[FILENAME_LEN],volumeName[8];
  char buffer[75000];
  file_name *head,*tmp;
  int ret, staged_files, cont=1;


  printf("arg %s\n",argv[0]);
  if (argc < 2) {
    usage(argv[0]);
    return -1;
  }

  while( (c = getopt(argc, argv, "d:v:w:s")) != EOF) {
	
    switch(c) {
    case 'd':
	strcpy(odn,optarg);
	loc_copy = 1;
      break;
    case 'v':
      strcpy(volumeName,optarg);
      /*setVolumeName(optarg);*/
      break;
    case 'w':				
      write_test = 1;
      break;
    case 's':				
      stage_test = 1;
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
  else if (stage_test) {
      printf("stage test\n");
      head = GetFileList(volumeName); /* get file list */
      /* 
	 !!!! IT IS VERY IMPORTANT TO CALL FreeFileList when the 
	 list is no more needed. Otherwise there will be a memory leak !!! 
    */
    if (head){
	tmp = head;
	while (tmp) {
	    printf("%s\n", tmp->fileName);
	    tmp = tmp->next;
	}
    }
    else {
	printf("GetFileList failed\n");
	return -1;
    }
    /* make a prestage request */
    printf("stage files\n");
    ret = StageFiles(head, NULL);

    if (ret != 0) {
	printf("stage request failed\n");
    }
    else {
	while (cont) {
	    printf("sleep %d\n");
	    sleep(60);  /* allow files to be staged */
	    /* sleep or do something else */
	    printf("check\n");
	    ret = CheckFiles(head, NULL, &staged_files);
	    if (ret != 0) {
		if (errno == EAGAIN) {
		    printf("not all files were staged yet. Staged %d files\n", staged_files);
		}
		else {
		    perror("check file stage failed\n");
		    break;
		}
	    }
	    else {
		printf("all (%d) files were staged\n", staged_files);
		cont = 0; /* indicate the success of the stage */
		break;
	    }
	}
	/* read the files */
	c = 1;
	record_size = 0;
	fn = 0;
	if (loc_copy) {
	    strcat(odn,"/");
	    strcat(odn,volumeName);

	    printf("output dir %s\n",odn);
	    if (!opendir(odn)){
		if( mkdir(odn, S_IRUSR|S_IWUSR|S_IXUSR) < 0) {
		    perror(strerror(errno));
		    c = 0; /* error, can not proceed */
		}
	    }
	}
	if (c) {
	    tmp = head;
	    while (tmp) {
		printf("filename %s\n", tmp->fileName);
		src = dc_open(tmp->fileName,O_RDONLY | O_BINARY );
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
		if (dst) close(dst);
		fn++;
		tmp = tmp->next;

	  }
	}
		

    }
    printf("Free File List\n");
    FreeFileList(head);
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
    /*
    if(fileError == -1) {
      return -1;
    }
    else if(record_size <0){
      return record_size;
    }
    else {
      return 0;
    }
    */
    record_size = 0;
    fn = 0;
    if (loc_copy) {
	strcat(odn,"/");
	strcat(odn,"NZA002");

	printf("output dir %s\n",odn);
	if (!opendir(odn)){
	    if( mkdir(odn, S_IRUSR|S_IWUSR|S_IXUSR) < 0) {
		perror(strerror(errno));
		return -1;
	    }
	}
    }
    while(record_size >= 0 && (fileError = GetFile("NZA002", fileName)) > 0) {
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
