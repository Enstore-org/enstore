/* $Id$
   Utility for users at remote sites to create tapes for the Enstore system
*/

#include "volume_import.h"


void WriteUsage()
{
    fprintf(stderr,"\
Usage: %s --write [--verbose] [--no-check] [--tape-device=dev] [--tape-db=dir] \n  --volume-label=label filelist [...]\n\
    each filelist is:  [--pnfs-dir=dir] [--strip-path=path] file [...]\n\
    tape-device can be set using environment variable $TAPE_DEVICE\n\
    tape-db (db directory) can be set using environment variable $TAPE_DB\n", 
	    progname);
    
    exit(-1);
}


/* Linked list implementation */
typedef struct _list_node{
    char destination[MAX_PATH_LEN];
    char source[MAX_PATH_LEN];
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
    new_node->next = (list_node*)0;
    return new_node;
}

list_node *file_list = NULL;
list_node *last_node = NULL;

void 
append_to_file_list(char *destination, char *source)
{
    list_node *new_node = make_list_node();
    
    if (!file_list) {
	file_list = new_node;
    } else {
	last_node->next = new_node;
    }
    last_node = new_node;
    strcpy(new_node->destination, destination);
    strcpy(new_node->source, source);
}


static char * 
match_opt(char *optname, char *arg)
{
    int n;
    char *cp;

    /* be friendly about _ vs - */
    for (cp=arg; *cp && *cp!='='; ++cp)
	if (*cp=='_')
	    *cp = '-';

    n = strlen(optname);
    if (strlen(arg)<n){
	return (char *)0;
    }
    if (!strncmp(optname, arg, n)){
	return arg+n;
    }
    return (char *)0;
}
	
int    
write_tape_main(int argc, char **argv)
{
    int i;
    char *pnfs_dir = NULL;
    char destination[MAX_PATH_LEN];
    char path[MAX_PATH_LEN];
    char *strip = NULL;
    char *source, *cp;
    list_node *node;
    int nfiles;
    int err_occurred = 0;

    tape_device = getenv("TAPE_DEVICE");
    tape_db = getenv("TAPE_DB");


    for (i=1; i<argc; ++i) {
	if (argv[i][0] == '-') { 
	    /* it's an option */
	    if (match_opt("--verbose", argv[i])) {
		verbose = 1;
	    } else if (match_opt("--no-check", argv[i])){
		no_check = 1;
	    } else if ((cp=match_opt("--tape-device=", argv[i]))) {
		tape_device = cp;
	    } else if ((cp=match_opt("--tape-db=",argv[i]))) {
		tape_db = cp;
            } else if ((cp=match_opt("--pnfs-dir=",argv[i]))){
		pnfs_dir = cp;
	    } else if ((cp=match_opt("--strip-path=", argv[i]))){
		strip = cp;
	    } else if ((cp=match_opt("--volume-label=", argv[i]))){
		if (volume_label){
		    fprintf(stderr,"%s: volume-label may be set only once\n",progname);
		    WriteUsage();
		} 
		volume_label = cp;
	    } else {
		fprintf(stderr,"%s: unknown option %s\n", progname, argv[i]);
		WriteUsage();
	    }
	} else {
	    /* it's a filename */
	    source = argv[i];
	    if (verify_file(pnfs_dir, strip, source))
		exit(-1);
	    if (strip){
		if(strip_path(path, strip, source))
		    exit(-1); /*this shouldn't happen, since we just did a verify */
	    } else {
		strcpy(path, source);
	    }
	    /* PNFS path must be non-NULL; this has already been checked in verify_file */
	    if (join_path(destination, pnfs_dir, path)){
		exit(-1);/*this shouldn't happen, since we just did a verify */
	    }
	    append_to_file_list(destination, source);
	}
    }

    if (!volume_label) {
	fprintf(stderr,"%s: no volume label specified\n", progname);
	WriteUsage();
    } 

    if (!tape_device) {
	fprintf(stderr, "%s: no tape device specified\n", progname);
	WriteUsage();
    }

    if (!tape_db) {
	fprintf(stderr, "%s: no tape db specified\n", progname);
	WriteUsage();
    }
    
    if (verify_tape_device()
	||verify_tape_db(0)
	||open_tape()
	||verify_volume_label())
	{
	    fprintf(stderr,"%s: failed\n",progname);
	    exit(-1);
	}

    fprintf(stderr,"Writing files\n");
    for (nfiles=0,node=file_list; node; ++nfiles,node=node->next) {
	fprintf(stderr,"Writing %s ...",node->source);
	fflush(stderr);
	if (do_add_file(node->destination, node->source)){
	    fprintf(stderr,"error\n");
	    ++err_occurred;
	    break;  /* XXX clean up this whole batch of additions ? */
	}
	fprintf(stderr,"ok\n");
	if (write_eof_marks(1)){
	    break;
	}
    }
    printf("Wrote %d file%c\n", nfiles, nfiles==1?' ':'s');
    
    err_occurred= (err_occurred
		   ||write_eot1_header(file_number+1)
		   ||close_tape());

    verbage("returning %d\n", err_occurred);
    return err_occurred;
    
}

