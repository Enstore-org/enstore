static char rcsid[] = "@(#)$Id$";
/* 


Authors:        Margaret Votava
e-mail:         "votava@fnal.gov"
 
Revision history:-
=================
17-Oct-1995 MEV created	Wrapping routine for test commands 
24-Jan-1997 MEV	fixed bug in extract_stat
 
Include files:-
===============
*/

#include <stdlib.h>

#include "ftt.h"
#include "ftt_t_parse.h"
#include "ftt_t_macros.h"

#define FTT_T_MAXDSIZE 32768

void ftt_t_block_fill   (char *, int, int, int);

static ftt_stat_buf	statbuf = NULL;
static ftt_stat_buf	*delstat = NULL;
static ftt_statdb_buf	statbuf_db = NULL;
static ftt_statdb_buf	*delstatdb = NULL;


int	ftt_t_remain_stats (int argc, char **argv)
{
int		status;
int		estatus = 0;
unsigned int	i, j;
char		*stat_val1 = 0;		/* stat value */
char		*stat_val2 = 0;		/* stat value */
char 		*usr_write;
char		*peot; 
char		*eom;
char		*bot;
char		*block_no;
char		*file_no;
char		*write_cnt;
char 		*ucmp_write;
char		*cmp_write;
static char    	*estatus_str;           /* expected status string */
char 		wdata[FTT_T_MAXDSIZE];
static unsigned int	nblock = 40000;
static int	bsize = 32767;
int 		fileno, blockno, tst = 0;
long		remain, written;
static char     *out_filename;          /* output filename */
FILE            *outfile = stderr;       /* output file */
ftt_t_argt		argt[] = {
        {"-filename",   FTT_T_ARGV_STRING,      NULL,           &out_filename},
 	{"-status",	FTT_T_ARGV_STRING,	NULL,		&estatus_str},
 	{NULL,		FTT_T_ARGV_END,		NULL,		NULL}};

estatus_str = NULL; out_filename = NULL; peot = NULL; 
status = ftt_t_parse (&argc, argv, argt);
FTT_T_CHECK_PARSE (status, argt, argv[0]);	/* check parse status */
FTT_T_CHECK_ESTATUS (estatus_str, estatus);

/*out_filename = "stat.txt";  */

if (out_filename) {
   outfile = fopen(out_filename,"w");   
 if (outfile == 0)
      {
      perror("could not open file\n");
      fprintf (stderr,"   filename = %s\n",out_filename);
      return 1;
      }
fprintf (outfile,"Outfile %s\n",out_filename);
} 

for (i = 0; i< bsize; i++) {
    wdata[i] = (1 + random())%255;
}

/*fclose (outfile); 
return 0;   */
fflush(outfile);

/*return 0; */
/*status = ftt_rewind(ftt_t_fd);
FTT_T_CHECK_CALL (status,estatus); */
if (!statbuf) {
   statbuf = ftt_alloc_stat();	
   if (!statbuf) {
       char *errstring;
       errstring = ftt_get_error(&status);
       fprintf (stderr,"Could not allocate statdb buffer %s\n",errstring);
       fprintf (stderr,"%s\n",errstring);
       FTT_T_INC_NERROR();
       return 1;
    }
 }

   while (tst == 0) { 
        status = ftt_get_stats (ftt_t_fd, statbuf);
        FTT_T_CHECK_CALL (status,estatus);
        stat_val1 = ftt_extract_stats (statbuf, 23); 
        FTT_T_CHECK_CALL (status,estatus);

        for (j = 0; j <nblock; j++) {
            status = ftt_write (ftt_t_fd, wdata, bsize);    
            FTT_T_CHECK_WRITE (status,estatus,bsize);
        }

        status = ftt_get_stats (ftt_t_fd, statbuf);
        FTT_T_CHECK_CALL (status,estatus);
        write_cnt = ftt_extract_stats (statbuf, 6);
        file_no   = ftt_extract_stats (statbuf, 10);
        block_no  = ftt_extract_stats (statbuf, 11);
        bot       = ftt_extract_stats (statbuf, 12);
        ucmp_write= ftt_extract_stats (statbuf, 50); 
        cmp_write = ftt_extract_stats (statbuf, 52); 
        stat_val2 = ftt_extract_stats (statbuf, 23);
        eom       = ftt_extract_stats (statbuf, 16);
        peot      = ftt_extract_stats (statbuf, 17);

        status = ftt_writefm(ftt_t_fd);
        FTT_T_CHECK_CALL (status,estatus);
	status = ftt_get_position (ftt_t_fd, &fileno, &blockno);
        if (status) {
           int myerror;
           fprintf (stderr,"Remain test could not get file position:file %u block %u \n%s\n",
                   fileno, blockno, ftt_get_error(&myerror));
        }

        tst  = (int)atoi(peot); 
        remain = ((long)atoi(stat_val1) - (long)atoi(stat_val2));

        fprintf (outfile,"%u\t%s\t%s\t%lu\t%s\t%s\t%s\t%s\t%s\n",
                 fileno,stat_val2,write_cnt,remain,ucmp_write,cmp_write,bot,eom,peot); 




  }
 if (outfile != stderr) fclose(outfile);  

return 0;
}


