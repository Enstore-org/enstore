/* $Id$
Utility to initialize tapes before use
Labels tape and  creates database branch for new volume
*/

#include "volume_import.h"

void InitUsage()
{
    fprintf(stderr,
   "Usage: %s --init [--verbose] [--erase] [--tape-device=device] \n\
   [--tape-db=db_dir] --volume_label=label\n\
    tape-device can be set using environment variable $TAPE_DEVICE\n\
    tape-db (db directory) can be set using environment variable $TAPE_DB\n", 
	    progname);
    
    exit(-1);
}

int clear_db_volume();
    
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
init_tape_main(int argc, char **argv)
{
    int i;
    int erase=0;
    char *cp;
    char label[80];
    char dbpath[MAX_PATH_LEN];
    char dbvalue[256];
    int label_type;
    int fno;

    tape_device = getenv("TAPE_DEVICE");
    tape_db = getenv("TAPE_DB");

    for (i=1; i<argc; ++i) {
	if (argv[i][0] == '-') {
	    /*it's an option*/
	    if (match_opt("--verbose", argv[i])) {
		verbose = 1;
	    } else if ((cp=match_opt("--erase",argv[i]))) {
		erase=1;
	    } else if ((cp=match_opt("--tape-device=", argv[i]))) {
		tape_device = cp;
	    } else if ((cp=match_opt("--tape-db=",argv[i]))) {
		tape_db = cp;
            } else if ((cp=match_opt("--volume-label=", argv[i]))){
		volume_label = cp;
	    } else {
		fprintf(stderr,"%s: unknown option %s\n", progname, argv[i]);
		InitUsage();
	    }
	}
	else {
	    InitUsage();
	}
    }

    if (!tape_device) {
	fprintf(stderr, "%s: no tape device specified\n", progname);
	InitUsage();
    }

    if (!tape_db) {
	fprintf(stderr, "%s: no tape db specified\n", progname);
	InitUsage();
    }

    if (!volume_label){
	fprintf(stderr, "%s: no volume label given\n", progname);
	InitUsage();
    }
    

    if (erase)
	clear_db_volume();
	    
    if (verify_tape_device()
	||verify_tape_db(1)
	||verify_db_volume(1))
	{
	    fprintf(stderr, "%s failed\n", progname); 
	    return -1; /* don't clear_db_volume here because the error
			  may be that db volume already exists*/
	}
    
    /* Record the hostname and starting time */
    sprintf(dbpath,"%s/volumes/%s", tape_db, volume_label);
    if (gethostname(dbvalue,256)){
	fprintf(stderr,"%s: can't get hostname\n", progname);
	goto cleanup;
    }
    if (write_db_s(dbpath,"hostname",dbvalue))
	goto cleanup;
    if (timestamp(dbvalue)){
	fprintf(stderr,"%s: can't get current time\n", progname);
	goto cleanup;
    }
    if (write_db_s(dbpath,"first_access",dbvalue))
	goto cleanup;
    if (write_db_s(dbpath,"last_access",dbvalue))
	goto cleanup;


    if (open_tape() 
	||rewind_tape())
	goto cleanup;
    
    /* check if it's already labeled */
    if (!erase){
	verbage("Checking for existing label\n");
	if (read_tape_label(label,&label_type, &fno)==0)
	    if (label_type==0){
		verbage("Got %s\n", label);
		fprintf(stderr,"This tape is already labeled\nLabel=%s\n", 
			label);
		fprintf(stderr,"Use the --erase option to relabel it\n");
		goto cleanup;
	    } else {
		verbage("Invalid label type\n");
	    }
	else {
	    verbage("Couldn't read tape label\n");
	    if (rewind_tape())
		goto cleanup;
	}
    }
    
    if (write_vol1_header()
	||write_eof_marks(1)
	||write_eot1_header(0)
	||close_tape())
	goto cleanup;
    /* all is well */
    return 0;
    
  cleanup:
    clear_db_volume();
    fprintf(stderr, "%s failed\n", progname);
    return -1;
}

int clear_db_volume(){
    char cmd[MAX_PATH_LEN + 8];
    sprintf(cmd, "/bin/rm -rf %s/volumes/%s", tape_db, volume_label);
    verbage("running %s\n",cmd);
    return system(cmd); /* XXX I was lazy when I wrote this, there must be a nicer way */
}




