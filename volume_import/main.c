/*
  $Id$
*/

#include "volume_import.h"

#include "version.h"

int
main(int argc, char **argv)
{
    progname = argv[0];
    
    ++argv; --argc;
    if (!argc)
	goto Usage;
    
    if (!strcmp(argv[0],"--write")) {
	return write_tape_main(argc, argv);
    } else if (!strcmp(argv[0], "--init")) {
	return init_tape_main(argc, argv);
    } else if (!strcmp(argv[0], "--dump-db") ||
	       !strcmp(argv[0], "--dump_db")) {
	return dump_db_main(argc, argv);
    } else if (!strcmp(argv[0], "--version")) {
	printf("%s version %s\n", progname, VERSION);
	exit(0);
    } else if (!strcmp(argv[0], "--read")) {
	printf("XXX Not yet implemented\n");
	exit(-1);
    } else {
      Usage:	fprintf(stderr, "Usage: %s --write | --init | --version | --dump-db [additional_args]\n", progname);
      exit(-1);
    }
}

	    
    
    
