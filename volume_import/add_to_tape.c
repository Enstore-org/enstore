#include <stdio.h>

char *tape_device = NULL;
char *tape_database = NULL;
char *getenv(char *);
char *malloc(int);

int force = 0;
int verbose = 0;
char *progname;
int blocksize = 4096;

void Usage()
{
    fprintf(stderr,
	    "Usage: %s [-v] [-f] [-t tape-device] [-d tape-database] volume-label filelist [...]\n\
    each filelist is:  [-p pnfs-dir] file [...]\n\
    tape-device can be set using environment variable $TAPE\n\
    tape-database can be set using environment variable $TAPE_DATABASE\n", progname);
    
    exit(-1);
}

typedef struct _list_node{
    char *pnfs_dir;
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
	
    
int add_file(char *volume_label, char *pnfs_dir, char *filename)
{
    printf("requested pnfs_dir %s, file %s\n",    
	   pnfs_dir, filename);
    append_to_file_list(pnfs_dir, filename);
    return 0; /*success*/
}



int    
main(int argc, char **argv)
{
    int i;
    char *volume_label;
    char *pnfs_dir = NULL;
    char *filename;
    int status;
    list_node *node;
    int nfiles;

    tape_device = getenv("TAPE");
    tape_database = getenv("TAPE_DATABASE");

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
		    tape_database = argv[i];
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

    if (!tape_database) {
	fprintf(stderr, "%s: no tape database specified\n", progname);
	Usage();
    }

    

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
	status = add_file(volume_label, pnfs_dir, filename);
	if (status) {
	    fprintf(stderr,"%s: error adding file %s\n", progname, filename);
	    exit(-1);
	}
    }

    for (nfiles=0,node=file_list; node; ++nfiles,node=node->next) {
	printf("adding file %s to volume %s, pnfs_dir = %s, verbose=%d, force=%d\n",
	       node->filename, volume_label, node->pnfs_dir, 
	       verbose, force);
    }
    printf("handled %d files\n", nfiles);
	       
    return 0;
}

