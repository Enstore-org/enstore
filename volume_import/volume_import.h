/* $Id$
prototypes and necessary headers for the volume import package
*/

#include <stdio.h>
#include <fcntl.h> 

#include <unistd.h> /*Portability?*/
#include <stdlib.h>
#include <string.h>

#include <sys/stat.h>
#include <sys/mtio.h>

#define MB 1024U*1024U
#define GB 1024U*1024U*1024U

#define MAX_PATH_LEN 4096  /* get this (portably) from system headers */

#define MAX_LABEL_LEN 70   /* maximum length of  volume label */

#define DEFAULT_PERM 0775  /* default permissions for newly created dirs and files*/

int do_add_file(char *pnfs_dir, char *filename);
int verify_file(char *pnfs_dir, char *filename);
int verify_tape_db();
int verify_tape_device();
int verify_volume_label();
int verify_db_volume();

unsigned int adler32(int adler, char *buf, int len);
int write_db_s(char *path, char *key, char *value);
int write_db_i(char *path, char *key, int value);

int open_tape();
int rewind_tape();
int read_tape(char *, int);
int write_tape(char *, int);
int write_vol1_header();
int write_eot1_header(int);
int set_variable_blocksize();
int write_eof(int);
int close_tape();

