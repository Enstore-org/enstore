/* $Id$
   Utility for users at remote sites to create tapes for the Enstore system
*/

#include "volume_import.h"


void Usage()
{
    fprintf(stderr,"\
Usage: %s [-v] [-f tape-device] [-d tape-db] vol_label filelist [...]\n\
    each filelist is:  [-p pnfs-dir] file [...]\n\
    tape-device can be set using environment variable $TAPE\n\
    tape-db (db directory) can be set using environment variable $TAPE_DB\n", 
	    progname);
    
    exit(-1);
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
		
	    case 'b':
		if (++i >= argc) {
		    fprintf(stderr, "%s: -b option requires an argument\n", 
			    progname);
		    Usage();
		} else if (sscanf(argv[i], "%d", &blocksize) != 1) {
		    fprintf(stderr, "%s: bad blocksize %s\n", 
			    progname, argv[i]);
		}
		break;
	    case 'f':
		if (++i >= argc) {
		    fprintf(stderr, "%s: -f option requres an argument\n", 
			    progname);
		    Usage();
		} else 
		    tape_device = argv[i];
		break;
	    case 'd':
		if (++i >= argc) {
		    fprintf(stderr, "%s: -d option requres an argument\n", 
			    progname);
		    Usage();
		} else 
		    tape_db = argv[i];
		break;
	    case 'v':
		verbose = 1;
		break;
	    case 'p':
		fprintf(stderr, "%s: -p option must come after volume label\n", 
			progname);
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
    verify_tape_db(0);
    verify_volume_label();
    
    for (; i<argc; ++i) {
	if (argv[i][0] == '-') {
	    if (argv[i][1] != 'p') {
		fprintf(stderr,"%s: unknown option %s\n", progname, argv[i]);
		Usage();
	    } else {
		if (i+1 >= argc) {
		    fprintf(stderr,"%s: -p option requires an argument\n", 
			    progname);
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
    
    if (open_tape(2)) /* || rewind_tape()) */
	exit(-2);
    
    for (nfiles=0,node=file_list; node; ++nfiles,node=node->next) {
	verbage("adding file %s to volume %s, pnfs_dir = %s\n",
		   node->filename, volume_label, node->pnfs_dir);
	if (do_add_file(node->pnfs_dir, node->filename)){
	    break;  /* XXX clean up this whole batch of additions ? */
	}
	if (write_eof(1)){
	    break;
	}
    }
    verbage("handled %d file%c\n", nfiles, nfiles==1?' ':'s');
    
    if (close_tape())
	exit(-2);
    
    return 0;
}

