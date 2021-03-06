/* $Id$
prototypes and necessary headers for the volume import package
*/

#include <stdio.h>
#include <fcntl.h> 

#include <errno.h>

#include <unistd.h> 
#include <stdlib.h>
#include <string.h>

#include <sys/stat.h>
#include <sys/ioctl.h>
#include <sys/mtio.h>
#include <sys/time.h>

#include <sys/types.h>
#include <dirent.h>  /*portability?*/

#define MAX_PATH_LEN 4096  /* get this (portably) from system headers */
#define MAX_LABEL_LEN 70   /* maximum length of  volume label */

#define DEFAULT_BLOCKSIZE 32768
#define EARLY_CHECKSUM_SIZE 65536
#define DEFAULT_PERM 0775  /* default permissions for newly created dirs and files*/


#define MB 1024U*1024U
#define GB 1024U*1024U*1024U



#define verbage if (verbose)printf
#define min(a,b)((a)<(b)?(a):(b))

int do_add_file(char *destination, char *source);
int verify_file(char *pnfs_dir, char *strip, char *filename);
int verify_tape_db(int);
int verify_tape_device(void);
int verify_volume_label(void);
int verify_db_volume(int);

unsigned int adler32(int adler, char *buf, int len);
int write_db_s(char *path, char *key, char *value);
int write_db_i(char *path, char *key, int value);
int write_db_u(char *path, char *key, unsigned  value);
int read_db_s(char *path, char *key, char *value, int warn);
int read_db_i(char *path, char *key, int *value, int warn);
int read_db_u(char *path, char *key, unsigned *value, int warn);

int open_tape(void);
int rewind_tape(void);
int read_tape(char *, int);
int write_tape(char *, int);
int write_vol1_header(void);
int write_eot1_header(int);
int set_variable_blocksize(void);
int write_eof_marks(int);
int skip_eof_marks(int);
int skip_records(int); 
int close_tape(void);
int read_tape_label(char *, int*, int*);
int cpio_start(char *, char *);
int cpio_next_block(char *, int);
int join_path(char *, char *, char *);
int strip_path(char *, char *, char *);
int timestamp(char *);
int write_tape_main(int, char **);
int init_tape_main(int, char **);
int dump_db_main(int, char **);

/* Global vars */

extern char *tape_device;
extern int tape_fd;
extern char *tape_db;
extern char *volume_label;
extern char *progname;
extern int file_number;
extern int blocksize;
extern int verbose;
extern int no_check;

extern unsigned int checksum, early_checksum;
extern unsigned int early_checksum_size;

