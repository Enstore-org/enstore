/* $Id$
 Globals.  The other modules all include volume_import.h which declares these extern 
*/

#include "volume_import.h"

char *tape_device = NULL;
int tape_fd = -1;
char *tape_db = NULL;
char *volume_label = NULL;
char *progname;
int blocksize = DEFAULT_BLOCKSIZE;
int verbose = 0;
int file_number = 1;
unsigned int checksum=0, early_checksum=0;
unsigned int early_checksum_size=0;


