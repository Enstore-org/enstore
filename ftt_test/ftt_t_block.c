/* routines for playing with blocks 

Authors:        Margaret Votava
e-mail:         "votava@fnal.gov"
 
Revision history:-
=================
17-Oct-1995 MEV created	(stolen from mttest)
 
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


/*==============================================================================
ftt_t_block_fill 

	fills a test data block
==============================================================================*/
void ftt_t_block_fill (unsigned char *buff, int bsize, int fileno, int blockno)
{
int	*buff_int;
int	i,j;				/* counter */

buff_int = (int *)buff;

if (bsize > 3)  buff_int[0] = bsize;
if (bsize > 7)  buff_int[1] = fileno;
if (bsize > 11) buff_int[2] = blockno;

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
int		*buff_int;
unsigned char	echar;			/* expected character */
int		i,j;			/* counter */
int		status = 0;		/* verify status */

buff_int = (int *)buff;

if (bsize > 3)  if (buff_int[0] != bsize) 
   {
   status = 1;
   fprintf (stderr,"File %d, block %d: Verify error longword 0: Expected %x, got %x\n",
      fileno,blockno,bsize, buff_int[0]);
   }
if (bsize > 7)  if (buff_int[1] != fileno)
   {
   status = 1;
   fprintf (stderr,"File %d, block %d: Verify error longword 1: Expected %x, got %x\n",
      fileno,blockno,fileno, buff_int[1]);
   }
if (bsize > 11) if (buff_int[2] != blockno)
   {
   status = 1;
   fprintf (stderr,"File %d, block %d: Verify error longword 2: Expected %x, got %x\n",
      fileno,blockno,blockno, buff_int[2]);
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
void ftt_t_block_dump(FILE *outfile,int bufferno,unsigned char *buff,int bsize)
{
int		i;			/* counter */

fprintf(outfile,"\n\nbuffer %d (%d bytes):\n",bufferno,bsize);
for( i = 0 ; i < bsize ; i++ )
   fprintf(outfile, "%02x%c", buff[i], (i&0xf)==0xf ?'\n':' ');
if (bsize % 16) fprintf(outfile,"\n");


}

/*==============================================================================
ftt_t_block_undump

	dumps a test data block
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
   }
return bsize;						/* return size */
}


