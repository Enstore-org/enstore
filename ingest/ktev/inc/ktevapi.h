#ifndef KTEVAPI_H
#define KTEVAPI_H
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>
#include <unistd.h>

#define FILENAME_LEN 1024

struct list_element {
    struct list_element *next;
    char *fileName;
};
typedef struct list_element file_name;

/* src and dest at int values returned from dc_open */
int getRecord(char *buffer, int buffer_size, int src);
int putRecord(char *buffer, int buffer_size, int dest);

void setVolumeName(const char *s);
int GetFile(char *volumeName, char fileName[FILENAME_LEN]);
int PutFile(char *volumeName, char fileName[FILENAME_LEN]);

/* return the list of the files on a "volume"
   NOTE! to avoid memory leaks FreeFileList must ne called
   after use of the list.
   Below is an example for using GetFileList
   .....
    file_name *head,*tmp;
    int ret, staged_files, cont=1;

    head = GetFileList("NZA003");
    if (head){
	tmp = head;
	while (tmp) {
	    printf("%s\n", tmp->fileName);
	    
	    tmp = tmp->next;
	}

	....
	
	### make a prestage request
	ret = StageFiles(head, NULL);
	if (ret != 0) {
	   printf("stage request failed\n");
      }
      else {
         while (cont) {
           sleep(600);  ### allow files to be staged
	     ### sleep or do something else
	     ret = CheckFiles(head, NULL, &staged_files);
	     if (ret != 0) {
	       if errno = EAGAIN {
	         printf("not all files are staged yet. Staged %d files\n", staged_files);
	       }
	       else {
	         perror("check file stage failed\n");
		   break;
		 }
	     }
	     else {
	       printf("all (%d) files were staged\n", staged_files);
	       break;
	     }
	   }
	 }
	 FreeFileList(head);




	
    }
*/
   
file_name *GetFileList(char *volume_name);
int FreeFileList(file_name *list_head);
int StageFiles(file_name *file_list, char *stage_on_host);
int CheckFiles(file_name *file_list, char *stage_on_host, int *staged_files);

#endif
