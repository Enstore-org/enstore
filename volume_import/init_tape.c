#include "volume_import.h"

/* Globals.  The other modules all include globals.h which declares these extern */
char *tape_device = NULL;
int tape_fd=-1;
char *tape_db = NULL;
char *volume_label = NULL;
char *progname;
int blocksize = 4096;
int verbose = 0;


void Usage()
{
    fprintf(stderr,
   "Usage: %s [-v] [-f tape-device] [-d tape-db] vol_label\n\
    tape-device can be set using environment variable $TAPE\n\
    tape-db (db directory) can be set using environment variable $TAPE_DB\n", 
	    progname);
    
    exit(-1);
}

    
int
main(int argc, char **argv)
{
    int i;
    
    tape_device = getenv("TAPE");
    tape_db = getenv("TAPE_DB");

    progname = argv[0];

    for (i=1; i<argc; ++i) {
	if (argv[i][0] == '-') {
	    switch (argv[i][1]) {
		
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

    if (!tape_device) {
	fprintf(stderr, "%s: no tape device specified\n", progname);
	Usage();
    }

    if (!tape_db) {
	fprintf(stderr, "%s: no tape db specified\n", progname);
	Usage();
    }

    verify_tape_device();
    verify_tape_db(1);
    verify_db_volume(1);

    if (open_tape(2))
	exit(-2);
    if (rewind_tape())
	exit(-2);
    if (write_vol1_header())
	exit(-2);
    if (write_eof(2))
	exit(-2);
    if (write_eot1_header(1))
	exit(-2);
    if (close_tape())
	exit(-2);

    /* all is well, add it to the db */
    
    return 0;
}

