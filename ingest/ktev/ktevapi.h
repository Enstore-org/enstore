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

/* src and dest at int values returned from dc_open */
int getRecord(char *buffer, int buffer_size, int src);
int putRecord(char *buffer, int buffer_size, int dest);

void setVolumeName(const char *s);
int GetFile(char *volumeName, char fileName[FILENAME_LEN]);
int PutFile(char *volumeName, char fileName[FILENAME_LEN]);

#endif
