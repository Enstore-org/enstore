/* Ftt main test routine

Authors:        Margaret Votava
e-mail:         "votava@fnal.gov"
 
Revision history:-
=================
10-Oct-1995 MEV created 
 
Include files:-
===============
*/
#include <stdio.h>
#include <time.h>
#include <fcntl.h>
#include <stdlib.h>



/*=============================================================================
==============================================================================*/

int main(int argc, char **argv)
{
char	*filename = NULL;				/* filename */
FILE	*fd;						/* fileid */
int	opt;						/* command line opts */
int 	nfiles;						/* number of files */
int	nrecords;					/* number of records */
int 	fileno;						/* current fileno */
int	oldfileno;					/* old fileno */
int	blockno;					/* current blockno */
int	oldblockno;					/* old blockno */
int	*nrec_infile;					/* nrecs in each file */
int	i;						/* counter */
time_t 	mytime;						/* time */

/* Get command line arguments
   -------------------------- */
 
while ((opt = getopt(argc,argv,"f:")) != -1)
   {
   switch (opt) {
      case 'f':
         {
         filename = (char *) malloc ((strlen(optarg) + 1));
         strcpy(filename,optarg);
         break;
         }
      case '?':
      usage:
         printf( "Usage: ftt_random -f <filename>\n");
         exit(1);
      }
   }
if (!filename)
   { printf( "Usage: ftt_test -f <output_filename>\n"); exit(1); }


/* open file and write out beginning info
   -------------------------------------- */

/*fd = fopen(filename, O_RDWR | O_CREAT,	S_IRUSR | S_IWUSR | S_IXUSR  |
					S_IRGRP | S_IWGRP | S_IXGRP  |
					S_IROTH	|           S_IXOTH);
*/

mytime = time(NULL);
srandom ((int) mytime);

fd = fopen (filename, "w");
if (!fd) { perror (filename); exit(1); }		/* error on open */
fprintf(fd,"#!/bin/sh\n");
fprintf(fd,"# =============================================================\n");
fprintf(fd,"# Randomly generated test script to test ftt functionality\n");
fprintf(fd,"# File created on %s\n",asctime(localtime(&mytime))); 
fprintf(fd,"# =============================================================\n");
fprintf(fd,"# \n");
fprintf(fd,"# Get the tape device by looking in $1. If that's not set,\n");
fprintf(fd,"# try FTT_TAPE. If that's not set either, then exit.\n");
fprintf(fd,"export FTT_TAPE\n");
fprintf(fd,"FTT_TAPE=${1:-${FTT_TAPE:-\"\"}}\n");
fprintf(fd,"FTT_TAPE=${FTT_TAPE:?\"No device specified\"}\n");
fprintf(fd,"# =============================================================\n");
 
fprintf(fd,"# =============================================================\n");
fprintf(fd,"# Write the test data \n");
fprintf(fd,"# =============================================================\n");
fprintf(fd,"ftt_test << EOD\n");
fprintf(fd,"ftt_open\n");
fprintf(fd,"ftt_rewind\n");

#define FTT_MAXNFILE    10
#define FTT_MAXNBLOCK   6000
#define FTT_MAXPOSITION	20

/* write a random number of files and with a random number of records
   of random sizes!
   ------------------------------------------------------------------ */

nfiles = (1 + random())%FTT_MAXNFILE;
nrec_infile = (int *) malloc (sizeof(int)*nfiles);
for (i = 0; i < nfiles; i++)
   {
   nrecords = (1 + random())%FTT_MAXNBLOCK;
   nrec_infile[i] = nrecords;			/* remember how many records */
   fprintf(fd,"ftt_write_tblock -nblock %d -bsize 10 -delta 65000 \n",nrecords);
   fprintf(fd,"ftt_writefm\n"); 
   }

/* go back and position to to random spots
   --------------------------------------- */

fprintf(fd,"# =============================================================\n");
fprintf(fd,"# Pick some positions and skip there \n");
fprintf(fd,"# =============================================================\n");

fprintf(fd,"ftt_rewind\n");			/* rewind */
oldfileno = 0;					/* old block number */
oldblockno = 0;					/* old file number */
for (i = 0; i < FTT_MAXPOSITION; i++)
   {
   fileno = random()%nfiles;			/* pick a file number */
   blockno = random()%nrec_infile[fileno];	/* pick a block number */
   if (fileno != oldfileno)			/* need to skip files */
      {
      oldblockno = 0;
      if (fileno == 0)
         fprintf(fd,"ftt_rewind\n");		/* rewind  to skip */
      else
         fprintf(fd,"ftt_skip_fm %d\n",
            (fileno - oldfileno > 0) ? 
	       fileno - oldfileno : fileno- oldfileno - 1);
      if (fileno <= oldfileno && fileno)	/* backward skip */
         fprintf(fd,"ftt_verify_tblock -nblock 1 -filemark\n");
      }
   if (blockno != oldblockno)			/* need to skip records? */
      fprintf(fd,"ftt_skip_rec %d\n", blockno - oldblockno);


/* let's verify now
   ---------------- */

   fprintf(fd,"ftt_verify_position %d %d\n", fileno, blockno);
   fprintf(fd,"ftt_verify_tblock -nblock 1\n");
   blockno++;
   oldblockno = blockno;
   oldfileno = fileno;
   }
fprintf(fd,"ftt_close\n");
fprintf(fd,"EOD\n");
close(fd);
exit(0);
}
