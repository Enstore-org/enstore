#ifndef KTEVAPIV0_H
#define KTEVAPIV0_H
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>
#include <unistd.h>
#include "dcap.h"

#define FILENAME_LEN 1024

int getRecord(char *buffer, int buffer_size, int src);
int putRecord(char *buffer, int buffer_size, int dest);
void setVolumeName(const char *s);
int getFile(char *volumeName, char fileName[FILENAME_LEN]);

#endif
