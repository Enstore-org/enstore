/* Ftt main test routine

Authors:        Margaret Votava
e-mail:         "votava@fnal.gov"
 
Revision history:-
=================
10-Oct-1995 MEV created 
 
Include files:-
===============
*/
#include <fcntl.h>
#include <getopt.h>



/*=============================================================================
==============================================================================*/

int main(int argc, char **argv)
{
char	*filename = NULL;				/* filename */
int	*fd;						/* fileid */
int 	opt;						/* opt flag */

/* Get command line arguments
   -------------------------- */
 
while ((opt = getopt(argc,argv,"f:")) != -1)
   {
   switch (opt) {
      case 'f':
         {
         filename = (char *) malloc ((strlen(optarg) + 1));
         strcpy(ftt_t_basename,optarg);
         break;
         }
      case '?':
      usage:
         printf( "Usage: ftt_test -f <basename>\n");
         exit(1);
      }
   }
if (!filename)
   { printf( "Usage: ftt_test -f <basename>\n"); exit(1); }

fd = open(filename,"w");
if (!fileid)
   { perror (filename); exit(1); }
fputs(fileid,"#\n"); 
fputs(fileid,"# Randomly generated test script to test ftt functionality\n");
fputs(fileid,"# File created on "); 
fputs(fileid,asctime); 
fputs(fileid,"#"); 

ftt_t_commandloop ("ftt_test> ", ftt_t_my_cmds);
exit(0);
}

