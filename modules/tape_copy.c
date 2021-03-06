/* Copy program used in recovering overwritten data tapes
 
Authors:        Margaret Votava
e-mail:         "votava@fnal.gov"
 
Revision history:-
=================
18-Jan-1997 MEV created
 
Include files:-
===============
*/

/* $Id$ */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "ftt.h"

#ifdef WIN32
extern char *optarg;
int getopt();
#else
#include <unistd.h>
#endif

#define FTT_ERROR_REPORT(routine)   {           \
   ftt_errstr = ftt_get_error(&ftt_errno);      \
   fprintf (stderr,"%s \n",ftt_errstr);         \
   if (ifd) ftt_close(ifd);                     \
   if (ofd) ftt_close(ofd);                     \
   exit(1);                                     \
   }
 
int main(int argc, char **argv)
{
 
int 		opt;			/* command line option */
char            *indev= NULL;		/* input filename */
char            *outdev=NULL;		/* output filename */
ftt_descriptor  ifd = NULL,ofd = NULL;	/* in and out file descriptors */
char            label[80];		/* data buffer for 1-st record*/
char           *data;		        /* data buffer */
int             nfile,nblock;		/* counters */
char            *ftt_errstr;		/* error string */
int             len,status;		/* statuses */
int             bs; 
time_t          tm;
/* get the command line switches 
   -i for input file
   -o for output file
  ============================== */
while ((opt = getopt(argc,argv,"o:i:s:")) != -1)
   {
   switch (opt) {
      case 'i':
         { indev = strdup(optarg); break; }
      case 'o':
         { outdev = strdup(optarg); break; }
      case 's':
	  { bs = atoi(optarg); break; }
      case '?':
         printf( "Usage: copy -i <input device> -o <output device> -s <block size>\n");
         exit(1);
      }
   }
if (!(indev))
   {fprintf(stderr, "must specify an input device with -i\n"); exit(1); }
if (!(outdev))
   {fprintf(stderr, "must specify an output device with -o\n"); exit(1); }

/* open the drives and rewind the output device
   note that we do not set the mode on the output file, 
   but are using the default device
   ==================================================== */

ifd = ftt_open (indev,1);
if (!ifd) FTT_ERROR_REPORT("ftt_open - input");
 
ofd = ftt_open (outdev,0);
if (!ofd) FTT_ERROR_REPORT("ftt_open - output");
 
printf("rewind %s\n",indev);
status = ftt_rewind(ifd);
if (status == -1) FTT_ERROR_REPORT("ftt_rewind");

printf("rewind %s\n",outdev);
status = ftt_rewind(ofd);
if (status == -1) FTT_ERROR_REPORT("ftt_rewind");

/* do the copy by reading until we get an error
   ============================================ */

nfile = 1;
nblock = 0;
data = (char *)malloc(bs);
/* copy label */
printf("copy label\n");
len = ftt_read(ifd,label,80);
printf("read label returned %d\n",len);
if (len == 80)
    {
	status = ftt_write(ofd,label,len);
    }
else
    {
	exit(1);
    }
len = ftt_read(ifd, data, 1);
if (len != 0)
  {
    printf("something's wrong: expected EOF, len=%d\n", len);
    exit(1);
  }
status = ftt_writefm(ofd);
printf("copy data\n");
while ((len = ftt_read(ifd,data,bs)) >= 0)
   {
   if (len)
      {
      status = ftt_write(ofd,data,len);
      nblock++;
      }
   else
      {
      tm = time(NULL);
      status = ftt_writefm(ofd);
      fprintf (stderr," %s file %d had %d blocks\n",ctime(&tm),nfile,nblock);
      nfile++; nblock = 0;
      }
   /* printf("file#=%d block#=%d length=%d\n",nfile, nblock, len); */
   if (status != len)
      {
      FTT_ERROR_REPORT("ftt_write or writefm");
      }
   }
 free(data);
 
/* done copying
   ============ */

if (nblock)
      fprintf (stderr,"file %d had %d blocks\n",nfile,nblock);
   
fprintf (stderr,"copy complete\n");
FTT_ERROR_REPORT("ftt_read");
exit(0);
}

