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

#define EARLY_CHECKSUM_SIZE 65536
#define DEFAULT_PERM 0775  /* default permissions for newly created dirs and files*/
#define verbage if (verbose)printf
#define min(a,b)((a)<(b)?(a):(b))

int do_add_file(char *pnfs_dir, char *filename);
int verify_file(char *pnfs_dir, char *filename);
int verify_tape_db(int);
int verify_tape_device(void);
int verify_volume_label(void);
int verify_db_volume(int);

unsigned int adler32(int adler, char *buf, int len);
int write_db_s(char *path, char *key, char *value);
int write_db_i(char *path, char *key, int value);
int write_db_u(char *path, char *key, unsigned  value);
int read_db_i(char *path, char *key, int *value);

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
int cpio_start(char *);
int cpio_next_block(char *, int);


/* Global vars */

extern char *tape_device;
extern int tape_fd;
extern char *tape_db;
extern char *volume_label;
extern char *progname;
int file_number;
extern int blocksize;
extern int verbose;


extern unsigned int checksum, early_checksum;
extern unsigned int early_checksum_size;

