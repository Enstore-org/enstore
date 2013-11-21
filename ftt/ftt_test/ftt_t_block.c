static char rcsid[] = "@(#)$Id$";
/* routines for playing with blocks 

Authors:        Margaret Votava, Marc Mengel
e-mail:         "votava@fnal.gov, mengel@fnal.gov"
 
Revision history:-
=================
17-Oct-1995 MEV created	(stolen from mttest)
15-Mar-1996 MWM added pack/unpack macros rather than int pointers for OSF1
 
Include files:-
===============
*/
#include <stdio.h>
#include <string.h>

#ifndef TRUE
#define TRUE 1
#endif
#ifndef FALSE
#define FALSE 0
#endif

int ftt_dump();
/*
** The following two macros pack 4 bytes into an int and unpack
** an int into four bytes in a machine-independant fasion
*/
#define PACK(b4,b3,b2,b1) (((b4)<<24) | ((b3)<<16) | ((b2)<<8) | (b1))
#define UNPACK(n,b4,b3,b2,b1) (\
	((b4)=((n)>>24)&0xff), ((b3)=((n)>>16)&0xff), \
	((b2)=((n)>> 8)&0xff), ((b1)=((n)    )&0xff))

/*==============================================================================
ftt_t_block_fill 

	fills a test data block
==============================================================================*/
void ftt_t_block_fill (unsigned char *buff, int bsize, int fileno, int blockno)
{
int	i,j;				/* counter */


if (bsize > 3)  UNPACK(bsize,   buff[0],buff[1],buff[2],buff[3]);
if (bsize > 7)  UNPACK(fileno,  buff[4],buff[5],buff[6],buff[7]);
if (bsize > 11) UNPACK(blockno, buff[8],buff[9],buff[10],buff[11]);

for (i = 12, j = 0; i < (bsize); i++,j++)
   {
   buff[i] = (fileno + blockno + j) % 256;
   } 
}


/*==============================================================================
ftt_t_block_verify

	verifies a test data block
==============================================================================*/
int ftt_t_block_verify(unsigned char *buff, int bsize, int fileno, int blockno)
{
unsigned char	echar;			/* expected character */
int		i,j;			/* counter */
int		status = 0;		/* verify status */

if (bsize > 3)  if (PACK(buff[0],buff[1],buff[2],buff[3])!= bsize) 
   {
   status = 1;
   fprintf (stderr,"File %d, block %d: Verify error longword 0: Expected %x, got %x\n",
      fileno,blockno,bsize, PACK(buff[0],buff[1],buff[2],buff[3]));
   }
if (bsize > 7)  if (PACK(buff[4],buff[5],buff[6],buff[7]) != fileno)
   {
   status = 1;
   fprintf (stderr,"File %d, block %d: Verify error longword 1: Expected %x, got %x\n",
      fileno,blockno,fileno, PACK(buff[4],buff[5],buff[6],buff[7]));
   }
if (bsize > 11) if (PACK(buff[8],buff[9],buff[10],buff[11]) != blockno)
   {
   status = 1;
   fprintf (stderr,"File %d, block %d: Verify error longword 2: Expected %x, got %x\n",
      fileno,blockno,blockno, PACK(buff[8],buff[9],buff[10],buff[11]));
   }

if (status == 0) /* only check bytes if block numbers matched */
   {
   for (i = 12, j = 0; i < (bsize); i++,j++)
      {
      echar = (fileno + blockno + j) % 256;
      if (echar != buff[i])
	 {
	 status = 1;
	 fprintf (stderr,"File %d, block %d: Verify error byte %d: Expected %x, got %x\n",
	    fileno,blockno,i,echar, buff[i]);
	 }
      } 
   }
return status;
}


/*==============================================================================
ftt_t_block_dump

	dumps a test data block
==============================================================================*/
void ftt_t_block_dump(FILE *outfile,int bufferno,unsigned char *buff,int bsize,int do_offsets, int do_chars )
{

fprintf(outfile,"\n\nbuffer %d (%d bytes):\n",bufferno,bsize);
ftt_dump(outfile, buff, bsize, do_offsets, do_chars );

}

/*==============================================================================
ftt_t_block_undump

	undumps a test data block
==============================================================================*/
int ftt_t_block_undump(FILE *infile, unsigned char *buff)
{
int bsize, nbuffer;
int c;
int i;
char endstring[20];

while (TRUE) 
  {
  c = getc(infile);
  if (feof(infile)) return -1;				/* that's all folks! */
  if (c == 'b')						/* data */
      {
      int n;
      n = fscanf(infile,"uffer %d (%d bytes):", &nbuffer, &bsize);
      if (n != 2) continue;				/* didn't match string*/
      break;
      }
  else if (c == 'e')
      {
      int n;
      n = fscanf(infile,"nd of %s",endstring);
      if (!n) continue;					/* didn't match string*/
      if (!strcmp(endstring,"file")) return 0;		/* filemark */
      else if (!strcmp(endstring,"tape")) return -1;	/* eot */
      }
  continue;
  }

for (i = 0; i < bsize; i++)				/* copy data into buff*/
   {
   fscanf(infile,"%02x",&c);
   buff[i] = c;
   if (15 == (i & 15)) 
      {
      /* eat rest of line */
      while ( '\n' != (c = getc(infile)) && -1 != c ) ;
      }
   }
/* eat rest of line */
while ( '\n' != (c = getc(infile)) && -1 != c ) ;
return bsize;						/* return size */
}


