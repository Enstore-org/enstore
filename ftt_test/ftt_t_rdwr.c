static char rcsid[] = "@(#)$Id$";
/* 


Authors:        Margaret Votava
e-mail:         "votava@fnal.gov"
 
Revision history:-
=================
17-Oct-1995 MEV created	Wrapping routine for test commands 
 
Include files:-
===============
*/

#include <time.h>
#include <math.h>
#include "ftt.h"
#include "ftt_t_parse.h"
#include "ftt_t_macros.h"

#ifdef WIN32
#include <windows.h>
#define sleep(x) Sleep(1000*x)
#define srandom  srand
#define random  rand
#endif
#define FTT_T_MAXDSIZE	32768

/* Prototypes
   ========== */

void ftt_t_block_fill 	(char *, int, int, int);
int  ftt_t_block_dump	(FILE *, int, char *, int, int, int);
int  ftt_t_block_undump	(FILE *, char *);
int  ftt_t_block_verify	(char *, int, int, int);


/* ============================================================================

ROUTINE: ftt_t_writefm
 	
	call ftt_writefm using the global file descriptor
==============================================================================*/
int	ftt_t_writefm (int argc, char **argv)
{
int 		status;				/* status */
int		estatus = 0;			/* expected error */
static char	*estatus_str;			/* expected status string */
ftt_t_argt	argt[] = {
 	{"-status",	FTT_T_ARGV_STRING,	NULL,		&estatus_str},
 	{NULL,		FTT_T_ARGV_END,		NULL,		NULL}};

/* parse command line
   ------------------ */

estatus_str = 0;
status = ftt_t_parse (&argc, argv, argt);
FTT_T_CHECK_PARSE (status, argt, argv[0]);
FTT_T_CHECK_ESTATUS (estatus_str, estatus);

status = ftt_writefm(ftt_t_fd);
FTT_T_CHECK_CALL (status,estatus);
return 0;

}


/* ============================================================================

ROUTINE: ftt_t_write2fm
 	
	call ftt_write2fm using the global file descriptor
==============================================================================*/
int	ftt_t_write2fm (int argc, char **argv)
{
int 		status;				/* status */
int		estatus = 0;			/* expected error */
static char	*estatus_str;			/* expected status string */
ftt_t_argt	argt[] = {
 	{"-status",	FTT_T_ARGV_STRING,	NULL,		&estatus_str},
 	{NULL,		FTT_T_ARGV_END,		NULL,		NULL}};

/* parse command line
   ------------------ */

estatus_str = 0;
status = ftt_t_parse (&argc, argv, argt);
FTT_T_CHECK_PARSE (status, argt, argv[0]);
FTT_T_CHECK_ESTATUS (estatus_str, estatus);

status = ftt_write2fm(ftt_t_fd);
FTT_T_CHECK_CALL (status,estatus);
return 0;

}


/* ============================================================================

ROUTINE: ftt_t_write_tblock
 	
	call writes a test data block to the global file descriptor
==============================================================================*/
int	ftt_t_write_tblock (int argc, char **argv)
{
char		wdata[FTT_T_MAXDSIZE];		/* write data */
int 		status;				/* status */
int 		estatus = 0;			/* expected status */
int		i;				/* counter */
static int	nblock;				/* number to write */
static int 	bsize;				/* block size */
static int 	alignmask;			/* align */
static int 	delta;				/* delta bsize */
static int 	ndelay;				/* delay between writes */
static char	*estatus_str;			/* expected status string */
ftt_t_argt	argt[] = {
 	{"-status",	FTT_T_ARGV_STRING,	NULL,		&estatus_str},
 	{"-nblocks",	FTT_T_ARGV_INT,		NULL,		&nblock},
 	{"-bsize",	FTT_T_ARGV_INT,		NULL,		&bsize},
 	{"-delay",	FTT_T_ARGV_INT,		NULL,		&ndelay},
 	{"-delta",	FTT_T_ARGV_INT,		NULL,		&delta},
 	{"-alignmask",	FTT_T_ARGV_INT,		NULL,		&alignmask},
 	{NULL,		FTT_T_ARGV_END,		NULL,		NULL}};

/* parse command line
   ------------------ */

estatus_str = 0; nblock = 1; bsize = 32768; alignmask = 3; 
delta = 0; ndelay = 0; 
status = ftt_t_parse (&argc, argv, argt);
FTT_T_CHECK_PARSE (status, argt, argv[0]);	/* check parse status */
FTT_T_CHECK_ESTATUS (estatus_str, estatus);	/* check expected status opt */

/* do the write(s)
   --------------- */

srandom ((int) time(NULL));
for (i = 0; i < nblock; i++)
   {
   int fileno, blockno;

/* get the random things
   --------------------- */

   int thisdelay = ndelay;			/* delay */
   int thissize = bsize;			/* no deltas */

   if (delta) 
      thissize = (bsize + random()%delta) & ~alignmask;

   if (thissize > FTT_T_MAXDSIZE)
      {
      fprintf (stderr, "Data size %d too big. Max size = %d\n",
         thissize, FTT_T_MAXDSIZE);
      return 1;
      }
   if (ndelay)
      thisdelay = (0 + random()%ndelay);

/* get the current position
   ------------------------ */

   status = ftt_get_position(ftt_t_fd, &fileno, &blockno);
   if (status)
      {
      int myerror;
      fprintf (stderr, "write test block could not get file position: \n%s\n",
	 ftt_get_error(&myerror));
      FTT_T_INC_NERROR();
      return 0;
      }

/* fill in the test block data
   --------------------------- */

   ftt_t_block_fill (wdata, thissize, fileno, blockno);

/* delay
   ----- */
   if (thisdelay) sleep (thisdelay);

/* finally, write out block
   ------------------------ */

   status = ftt_write (ftt_t_fd, wdata, thissize);
   FTT_T_CHECK_CALL (status,estatus);

   }

return 0;
}

/* ============================================================================

ROUTINE: ftt_t_verify_tblock
 	
	verifies a test data block on the global file descriptor
==============================================================================*/
int	ftt_t_verify_tblock (int argc, char **argv)
{
char		rdata[FTT_T_MAXDSIZE];		/* read data */
int 		status;				/* status */
int 		estatus = 0;			/* expected status */
int		i;				/* counter */
static int 	bsize;				/* block size */
static int	nblock;				/* number to read */
static int 	oddbyte;			/* read odd number of bytes */
static int 	ndelay;				/* delay between writes */
static int	filemark;			/* record is filemark */
static char	*estatus_str;			/* expected status string */
ftt_t_argt	argt[] = {
 	{"-status",	FTT_T_ARGV_STRING,	NULL,		&estatus_str},
 	{"-nblocks",	FTT_T_ARGV_INT,		NULL,		&nblock},
 	{"-delay",	FTT_T_ARGV_INT,		NULL,		&ndelay},
 	{"-oddbyte",	FTT_T_ARGV_CONSTANT,	(char *)TRUE,	&oddbyte},
 	{"-filemark",	FTT_T_ARGV_CONSTANT,	(char *)TRUE,	&filemark},
 	{"-maxbytes",	FTT_T_ARGV_INT,		NULL,		&bsize},
 	{NULL,		FTT_T_ARGV_END,		NULL,		NULL}};

/* parse command line
   ------------------ */

estatus_str = 0; nblock = 1; oddbyte = FALSE; ndelay = 0; filemark = FALSE;
bsize = FTT_T_MAXDSIZE;
status = ftt_t_parse (&argc, argv, argt);
FTT_T_CHECK_PARSE (status, argt, argv[0]);	/* check parse status */
FTT_T_CHECK_ESTATUS (estatus_str, estatus);	/* check expected status opt */
if (oddbyte)
   bsize = bsize & 1 ? bsize : bsize - 1;	/* make odd read */
else
   bsize = bsize & 1 ? bsize - 1 : bsize;	/* make even read */
if (bsize > FTT_T_MAXDSIZE)
   {
   fprintf (stderr, "maxbytes cannot be greater than %d\n",FTT_T_MAXDSIZE);
   return (1); 
   }

/* do the read(s)
   --------------- */

srandom ((int) time(NULL));
for (i = 0; i < nblock; i++)
   {
   int fileno, blockno;

/* get the random things
   --------------------- */

   int thisdelay = ndelay;			/* delay */
   int thissize = 0;

   if (ndelay)
      thisdelay = (0 + random()%ndelay);

/* get the current position
   ------------------------ */

   status = ftt_get_position(ftt_t_fd, &fileno, &blockno);
   if (status)
      {
      int myerror;
      fprintf (stderr, "verify test block could not get file position: \n%s\n",
	 ftt_get_error(&myerror));
      FTT_T_INC_NERROR();
      return 0;
      }

/* delay
   ----- */
   if (thisdelay) sleep (thisdelay);

/* clear the data 
   -------------- */

   memset(rdata,0,bsize);

/* read out block
   -------------- */

   thissize = ftt_read (ftt_t_fd, rdata, bsize);
   FTT_T_CHECK_CALL (thissize,estatus); 
   if (thissize < 0 && estatus == 0) 
      {
      if (filemark) 
         fprintf (stderr,"Verify of filemark failed\n");
      else
         fprintf (stderr,"Verify of block %d file %d failed.\n",blockno,fileno);
      }

/* fill in the test block data
   --------------------------- */

   if (filemark)
      {
      if (thissize > 0)
         {
         fprintf (stderr,"record is not a filemark\n");
         FTT_T_INC_NERROR();
         }
      }
   else if (thissize == 0)
      {
      fprintf (stderr,"record %d is a filemark\n",i);
      FTT_T_INC_NERROR();
      }
   else if (thissize > 0)
      {
      status = ftt_t_block_verify (rdata, thissize, fileno, blockno);
      if (status) 
	 FTT_T_INC_NERROR();
      }

   }
return 0;
}

/* ============================================================================

ROUTINE: ftt_t_dump
 	
	dumps a data block on the global file descriptor
==============================================================================*/
int	ftt_t_dump (int argc, char **argv)
{
char		rdata[FTT_T_MAXDSIZE];		/* read data */
static int	do_chars;			/* print char dump too */
static int	do_offsets;			/* print block offsets */
int 		status;				/* status */
int 		estatus = 0;			/* expected status */
int		i=0;				/* counter */
int 		bsize = FTT_T_MAXDSIZE;		/* block size */
static		int nfm = 0;			/* number of filemarks */
FILE		*outfile = stdout;		/* file to output to */
static char	*out_filename;			/* output file name */
static int	nblock;				/* number to read */
static char	*estatus_str;			/* expected status string */
ftt_t_argt	argt[] = {
 	{"-status",	FTT_T_ARGV_STRING,	NULL,		&estatus_str},
 	{"-nblocks",	FTT_T_ARGV_INT,		NULL,		&nblock},
 	{"-filename",	FTT_T_ARGV_STRING,	NULL,		&out_filename},
	{"-chars",	FTT_T_ARGV_CONSTANT,	(char *)TRUE,	&do_chars},
	{"-offsets",	FTT_T_ARGV_CONSTANT,	(char *)TRUE,	&do_offsets},
 	{NULL,		FTT_T_ARGV_END,		NULL,		NULL}};

/* parse command line
   ------------------ */

estatus_str = 0; nblock = -1; out_filename = NULL;
status = ftt_t_parse (&argc, argv, argt);
FTT_T_CHECK_PARSE (status, argt, argv[0]);	/* check parse status */
FTT_T_CHECK_ESTATUS (estatus_str, estatus);	/* check expected status opt */
bsize = bsize & 1 ? bsize - 1 : bsize;		/* make even read */

/* open file
   --------- */

if (out_filename)
   {
   outfile = fopen(out_filename,"w");
   if (outfile == 0)
      {
      perror("could not open file\n");
      fprintf (stderr,"   filename = %s\n",out_filename);
      return 1;
      }
   }

/* echo the undump command
   ------------------------ */

fprintf (outfile,"ftt_open\n");
fprintf (outfile,"ftt_undump\n");

/* do the read(s)
   --------------- */

while (nblock)
   {
   int thissize;
   i++;
   if (nblock >= 0) nblock--;

/* clear the data 
   -------------- */

   memset (rdata,0,bsize);

/* read out block
   -------------- */

   thissize = ftt_read (ftt_t_fd, rdata, bsize);
   if (thissize < 0 && outfile != stdout) fclose (outfile);
   FTT_T_CHECK_CALL (thissize,estatus); 
   if (thissize == 0)
      {
      nfm++;
      if (nfm == 1)
         {
	 fprintf (outfile,"\nend of file\n");
	 i = 0;
	 }
      else if (nfm == 2)
         {
	 fprintf (outfile,"\nend of file\n");
	 fprintf (outfile,"\nend of tape\n");
         break;
         }
      }
   else
      {
      nfm = 0;
      /* dump the test block*/
      ftt_t_block_dump (outfile, i, rdata, thissize, do_offsets, do_chars );
      }
   }
nfm = 0;
if (outfile != stdout) fclose (outfile);
return 0;
}

/* ============================================================================

ROUTINE: ftt_t_undump
 	
	undumps a data block on the global file descriptor
==============================================================================*/
int	ftt_t_undump (int argc, char **argv)
{
char		wdata[FTT_T_MAXDSIZE];		/* read data */
int 		status;				/* status */
int 		estatus = 0;			/* expected status */
int		length;
FILE            *infile = stdin;		/* file to input from */
static char     *in_filename;			/* input file name */
static char	*estatus_str;			/* expected status string */
ftt_t_argt	argt[] = {
 	{"-status",	FTT_T_ARGV_STRING,	NULL,		&estatus_str},
        {"-filename",	FTT_T_ARGV_STRING,      NULL,           &in_filename},
 	{NULL,		FTT_T_ARGV_END,		NULL,		NULL}};

/* parse command line
   ------------------ */

estatus_str = 0; in_filename = NULL;
status = ftt_t_parse (&argc, argv, argt);
FTT_T_CHECK_PARSE (status, argt, argv[0]);	/* check parse status */
FTT_T_CHECK_ESTATUS (estatus_str, estatus);	/* check expected status opt */

/* open file
   --------- */
 
if (in_filename)
   {
   infile = fopen(in_filename,"r");
   if (infile == 0)
      {
      perror("could not open file\n");
      fprintf (stderr,"   filename = %s\n",in_filename);
      return 1;
      }
   }

/* do the writes
   --------------- */

memset (wdata,0,FTT_T_MAXDSIZE);
while ((length = ftt_t_block_undump(infile,wdata)) >= 0)
   {
   if (length == 0) 
      {
      status = ftt_writefm(ftt_t_fd);
      if (status < 0 && infile != stdin) fclose(infile);
      FTT_T_CHECK_CALL(status,estatus);
      }
   else if (length >= 0)
      {
      status = ftt_write(ftt_t_fd,wdata,length);
      if (status < 0 && infile != stdin) fclose(infile);
      FTT_T_CHECK_CALL(status,estatus);
      }
   else
      break;						/* all done */
   memset (wdata,0,FTT_T_MAXDSIZE);
   }

if (infile != stdin) fclose(infile);
return 0;
}
