/* $Id$
 Globals.  The other modules all include globals.h which declares these extern 
*/
char *tape_device = NULL;
int tape_fd = -1;
char *tape_db = NULL;
char *volume_label = NULL;
char *progname;
int blocksize = 4096;
int verbose = 0;
unsigned int checksum=0, early_checksum=0;
unsigned int early_checksum_size=0;


