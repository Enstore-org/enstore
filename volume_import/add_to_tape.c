#include <stdio.h>

#include <unistd.h> /*Portability?*/
#include <string.h>
#include <sys/stat.h>

char *tape_device = NULL;
char *tape_db = NULL;
char *volume_label = NULL;

char *getenv(char *);
char *malloc(int);

int force = 0;
int verbose = 0;
char *progname;
int blocksize = 4096;

#define MB 1024U*1024U
#define GB 1024U*1024U*1024U

extern int do_add_file(char *pnfs_dir, char *filename, int verbose, int force);

#define MAXPATHLEN 4096  /* get this (portably) from system headers */

void Usage()
{
    fprintf(stderr,
   "Usage: %s [-v] [-f] [-t tape-device] [-d tape-db] vol_label filelist [...]\n\
    each filelist is:  [-p pnfs-dir] file [...]\n\
    tape-device can be set using environment variable $TAPE\n\
    tape-db (db directory) can be set using environment variable $TAPE_DB\n", 
	    progname);
    
    exit(-1);
}

/* The verify_ functions will return 1, or else exit with a -1 */
/* All other functions return 0 on success and won't exit */

int 
verify_tape_device(){
    return 1; /*XXX*/
}

int 
verify_tape_db(){

    struct stat sbuf;
    int status;
    char path[MAXPATHLEN];

    status = stat(tape_db, &sbuf);
    if (status){
	fprintf(stderr,"%s: ",progname);
	perror(tape_db);
	exit(-1);
    }
    if (!S_ISDIR(sbuf.st_mode)){
	fprintf(stderr,"%s: %s is not a directory\n", progname, tape_db);
	exit(-1);
    }
    if ( (sbuf.st_mode & 0700) != 0700){
	fprintf(stderr,"%s: insufficent permissions on directory %s\n",
		progname, tape_db);
	exit(-1);
    }
    
    /* Check if "volumes" is a subdir of tape_db,  if it's not, try to create it */
    sprintf(path, "%s/volumes", tape_db);
    status = stat(path, &sbuf);
    if (status){
	if (mkdir(path, 0775)){
	    fprintf(stderr, "%s: cannot create directory: ", progname);
	    perror(path);
	    exit(-1);
	}
    } else if (!S_ISDIR(sbuf.st_mode)){
	fprintf(stderr, "%s: %s is not a directory\n", progname, path);
	exit(-1);
    }
    return 1;
}

int 
verify_file(char *pnfs_dir, char *filename){
    
    struct stat sbuf;
    int status;

    if (!pnfs_dir){
	fprintf(stderr,"%s: no pnfs directory given\n", progname);
	exit(-1);
    }
    if (strlen(pnfs_dir)<5 || strncmp(pnfs_dir,"/pnfs",5)){
	fprintf(stderr,"%s: pnfs_dir must start with /pnfs\n", progname);
	exit(-1);
    }

    status = stat(filename, &sbuf);
    if (status){
	fprintf(stderr,"%s: ", progname);
	perror(filename);
	exit(-1);
    }
    if (!S_ISREG(sbuf.st_mode)){
	fprintf(stderr,"%s: %s: not a regular file\n", progname, filename);
	exit(-1);
    }
    if ( (sbuf.st_mode & 0400)  != 0400){
	fprintf(stderr,"%s: %s: no read permission\n", progname, filename);
	exit(-1);
    }
    if ( (sbuf.st_size >= 2*GB) ){
	fprintf(stderr,"%s: %s: file size larger than 2GB\n", progname, 
		filename);
	exit(-1);
    }

    return 1;
}
    
int 
verify_volume_label()
{
    /*TODO check that this matches what is in the drive */
    
    struct stat sbuf;
    int status;
    char path[MAXPATHLEN];  


    /* check if it's in the database */
    sprintf(path,"%s/volumes/%s", tape_db, volume_label);
    status = stat(path, &sbuf);
    if (status) {
	fprintf(stderr,"%s: directory %s does not exist.\n%s",
		progname, path,
		"Has this volume been initialized?\n");
	exit(-1);
    }
    return 1;
}
    

/* Linked list implementation */
typedef struct _list_node{
    char *pnfs_dir;
    char *base_path;
    char *filename;
    struct _list_node *next;
} list_node;

list_node 
*make_list_node() {
    list_node *new_node;
    new_node = (list_node*)malloc(sizeof(list_node));
    if ((int)new_node <= 0) {
	fprintf(stderr,"%s: fatal error: cannot allocate memory\n", progname);
	exit(-1);
    }   
    new_node->pnfs_dir = NULL;
    new_node->base_path = NULL;
    new_node->filename = NULL;
    new_node->next = (list_node*)0;
    return new_node;
}

list_node *file_list = NULL;
list_node *last_node = NULL;

void append_to_file_list(char *pnfs_dir, char *filename)
{
    list_node *new_node = make_list_node();
    
    if (!file_list) {
	file_list = new_node;
    } else {
	last_node->next = new_node;
    }
    last_node = new_node;
    new_node->pnfs_dir = pnfs_dir;
    new_node->filename = filename;
}


	
int    
main(int argc, char **argv)
{
    int i;
    char *pnfs_dir = NULL;
    char *filename;
    list_node *node;
    int nfiles;

    tape_device = getenv("TAPE");
    tape_db = getenv("TAPE_DB");

    progname = argv[0];

    for (i=1; i<argc; ++i) {
	if (argv[i][0] == '-') {
	    switch (argv[i][1]) {
	    case 'f':
		force = 1;
		break;
		
	    case 'b':
		if (++i >= argc) {
		    fprintf(stderr, "%s: -b option requires an argument\n", progname);
		    Usage();
		} else if (sscanf(argv[i], "%d", &blocksize) != 1) {
		    fprintf(stderr, "%s: bad blocksize %s\n", progname, argv[i]);
		}
		break;
	    case 't':
		if (++i >= argc) {
		    fprintf(stderr, "%s: -t option requres an argument\n", progname);
		    Usage();
		} else 
		    tape_device = argv[i];
		break;
	    case 'd':
		if (++i >= argc) {
		    fprintf(stderr, "%s: -d option requres an argument\n", progname);
		    Usage();
		} else 
		    tape_db = argv[i];
		break;
	    case 'v':
		verbose = 1;
		break;
	    case 'p':
		fprintf(stderr, "%s: -p option must come after volume label\n", progname);
		Usage();
		break;
	    default:
		fprintf(stderr,"%s: unknown option %s\n", progname, argv[i]);
		Usage();
	    }
	} else
	    break;
    }
    
    if (i==argc) {
	fprintf(stderr,"%s: no volume label specified\n", progname);
	Usage();
    } else {
	volume_label = argv[i++];
    }

    if (i==argc) {
	fprintf(stderr,"%s: no files specified\n", progname);
	Usage();
    }

    if (!tape_device) {
	fprintf(stderr, "%s: no tape device specified\n", progname);
	Usage();
    }

    if (!tape_db) {
	fprintf(stderr, "%s: no tape db specified\n", progname);
	Usage();
    }

    verify_tape_device();
    verify_tape_db();
    verify_volume_label();
    
    for (; i<argc; ++i) {
	if (argv[i][0] == '-') {
	    if (argv[i][1] != 'p') {
		fprintf(stderr,"%s: unknown option %s\n", progname, argv[i]);
		Usage();
	    } else {
		if (i+1 >= argc) {
		    fprintf(stderr,"%s: -p option requires an argument\n", progname);
		    Usage();
		} 
		pnfs_dir = argv[++i];
		if (++i >= argc) {
		    fprintf(stderr,"%s: empty filelist\n", progname);
		    Usage();
		}
	    }
	}
	filename = argv[i];
	verify_file(pnfs_dir, filename);
	append_to_file_list(pnfs_dir, filename);
    }

    for (nfiles=0,node=file_list; node; ++nfiles,node=node->next) {
	printf("adding file %s to volume %s, pnfs_dir = %s, verbose=%d, force=%d\n",
	       node->filename, volume_label, node->pnfs_dir, 
	       verbose, force);
	if (do_add_file(node->pnfs_dir, node->filename, verbose, force)){
	    break;
	}
    }
    printf("handled %d file%c\n", nfiles, nfiles==1?' ':'s');
	       
    return 0;
}

