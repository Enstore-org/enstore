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

#include "ftt.h"
#include "ftt_t_parse.h"
#include "ftt_t_macros.h"


static ftt_stat_buf	statbuf = NULL;
static ftt_stat_buf	*delstat = NULL;


/* ============================================================================

ROUTINE: ftt_t_get_stats
 	
	call ftt_get_stats
==============================================================================*/
int	ftt_t_get_stats (int argc, char **argv)
{
int 		status;			/* status */
int		estatus = 0;		/* expected status */
static char	*estatus_str;		/* expected status string */
ftt_t_argt	argt[] = {
 	{"-status",	FTT_T_ARGV_STRING,	NULL,		&estatus_str},
 	{NULL,		FTT_T_ARGV_END,		NULL,		NULL}};

/* parse command line
   ------------------ */

estatus_str = NULL;
status = ftt_t_parse (&argc, argv, argt);
FTT_T_CHECK_PARSE (status, argt, argv[0]);	/* check parse status */
FTT_T_CHECK_ESTATUS (estatus_str, estatus);

/* get the stats
   ------------- */

if (!statbuf) 
   {
   statbuf = ftt_alloc_stat();
   if (!statbuf)
      {
      char *errstring;
      errstring = ftt_get_error(&status);
      fprintf (stderr,"Could not allocate stat buffer %s\n",errstring);
      fprintf (stderr, "%s\n",errstring);
      FTT_T_INC_NERROR();   
      return 1;
      }
   }

status = ftt_get_stats (ftt_t_fd,statbuf);                     
FTT_T_CHECK_CALL (status,estatus);

return 0;
}


/* ============================================================================

ROUTINE: ftt_t_extract_stats
 	
	call ftt_extract_stats
==============================================================================*/
int	ftt_t_extract_stats (int argc, char **argv)
{
int			status;
int			i;
char			*match = NULL;		/* stat match */
char			*stat_value;		/* stat value */
static char		*stat_name;		/* stat desired (ascii) */
ftt_t_argt		argt[] = {
 	{"<statistic>",	FTT_T_ARGV_STRING,	NULL,		&stat_name},
 	{NULL,		FTT_T_ARGV_END,		NULL,		NULL}};

/* parse command line
   ------------------ */

stat_name = NULL;
status = ftt_t_parse (&argc, argv, argt);
FTT_T_CHECK_PARSE (status, argt, argv[0]);	/* check parse status */

/* find out which stat they want
   ----------------------------- */

for (i = 0; i < FTT_MAX_STAT; i++)
   {
   if (!strcasecmp(stat_name,ftt_stat_names[i])) 
      {
      match = ftt_stat_names[i];
      break;
      }
   }

if (!match)
   {
   fprintf (stderr,"%s is not a valid statistic name.\n",stat_name);
   return 1;
   }

/* get the value
   ------------- */
stat_value = ftt_extract_stats (statbuf,i); 
if (0 == stat_value ) { stat_value = "(null)"; }
fprintf (stderr,"%s is %s\n",match,stat_value);

return 0;
}


/* ============================================================================

ROUTINE: ftt_t_dump_stats
 	
	call ftt_dump_stats
==============================================================================*/
int	ftt_t_dump_stats (int argc, char **argv)
{
int 		status;			/* status */
int		estatus = 0;		/* expected status */
FILE		*outfile = stderr;	/* output file */
static char	*estatus_str;		/* expected status string */
static char	*out_filename;		/* output filename */
ftt_t_argt	argt[] = {
 	{"-filename",	FTT_T_ARGV_STRING,	NULL,		&out_filename},
 	{"-status",	FTT_T_ARGV_STRING,	NULL,		&estatus_str},
 	{NULL,		FTT_T_ARGV_END,		NULL,		NULL}};

/* parse command line
   ------------------ */

estatus_str = NULL; out_filename = NULL;
status = ftt_t_parse (&argc, argv, argt);
FTT_T_CHECK_PARSE (status, argt, argv[0]);	/* check parse status */
FTT_T_CHECK_ESTATUS (estatus_str, estatus);

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

/* dump the stats
   -------------- */

if (!statbuf)
   {
   statbuf = ftt_alloc_stat();
   if (!statbuf)
      {
      char *errstring;
      errstring = ftt_get_error(&status);
      fprintf (stderr,"Could not allocate stat buffer %s\n",errstring);
      fprintf (stderr, "%s\n",errstring);
      FTT_T_INC_NERROR();   
      return 1;
      }
   }

status = ftt_dump_stats (statbuf,outfile); 
if (status == -1 && outfile != stderr) fclose (outfile);
FTT_T_CHECK_CALL (status,estatus);

if (outfile != stderr) fclose (outfile);
return 0;
}

/* ============================================================================

ROUTINE: ftt_t_free_stat
 	
	call ftt_free_stat
==============================================================================*/
int	ftt_t_free_stat (int argc, char **argv)
{
int 		status;			/* status */
int		estatus = 0;		/* expected status */
static char	*estatus_str;		/* expected status string */
ftt_t_argt	argt[] = {
 	{"-status",	FTT_T_ARGV_STRING,	NULL,		&estatus_str},
 	{NULL,		FTT_T_ARGV_END,		NULL,		NULL}};

/* parse command line
   ------------------ */

estatus_str = NULL;
status = ftt_t_parse (&argc, argv, argt);
FTT_T_CHECK_PARSE (status, argt, argv[0]);	/* check parse status */
FTT_T_CHECK_ESTATUS (estatus_str, estatus);

/* free the stat
   ------------- */

status = ftt_free_stat (statbuf); 
statbuf = 0;
FTT_T_CHECK_CALL (status,estatus);

return 0;
}

/* ============================================================================

ROUTINE: ftt_t_init_stats
 	
	call ftt_init_stats
==============================================================================*/
int	ftt_t_init_stats (int argc, char **argv)
{
int 		status;			/* status */
int		estatus = 0;		/* expected status */
static char	*estatus_str;		/* expected status string */
ftt_t_argt	argt[] = {
 	{"-status",	FTT_T_ARGV_STRING,	NULL,		&estatus_str},
 	{NULL,		FTT_T_ARGV_END,		NULL,		NULL}};

/* parse command line
   ------------------ */

estatus_str = NULL;
status = ftt_t_parse (&argc, argv, argt);
FTT_T_CHECK_PARSE (status, argt, argv[0]);	/* check parse status */
FTT_T_CHECK_ESTATUS (estatus_str, estatus);

/* init the stats
   -------------- */

delstat = ftt_init_stats (ftt_t_fd);                     
FTT_T_CHECK_CALL_OPEN (status,estatus);

return 0;
}

/* ============================================================================

ROUTINE: ftt_t_update_stats
 	
	call ftt_update_stats
==============================================================================*/
int	ftt_t_update_stats (int argc, char **argv)
{
int 		status;			/* status */
int		estatus = 0;		/* expected status */
char		*stat_value;		/* status value */
static int	partial;		/* partial display */
static int	errorrate;		/* error rate display */
static char	*estatus_str;		/* expected status string */
ftt_t_argt	argt[] = {
 	{"-part_display",FTT_T_ARGV_CONSTANT,	(char *)TRUE,	&partial},
 	{"-errors",     FTT_T_ARGV_CONSTANT,	(char *)TRUE,	&errorrate},
 	{"-status",	FTT_T_ARGV_STRING,	NULL,		&estatus_str},
 	{NULL,		FTT_T_ARGV_END,		NULL,		NULL}};

/* parse command line
   ------------------ */

estatus_str = NULL; partial = FALSE;
status = ftt_t_parse (&argc, argv, argt);
FTT_T_CHECK_PARSE (status, argt, argv[0]);	/* check parse status */
FTT_T_CHECK_ESTATUS (estatus_str, estatus);

/* init the stats
   -------------- */

status = ftt_update_stats (ftt_t_fd,delstat); 
FTT_T_CHECK_CALL(status,estatus);

/* dump it out
   ----------- */

if (partial)				/* display only counters */
   {
   int i;
   for (i = 0; ftt_stat_names[i] != 0; i++)
      {
      if (ftt_numeric_tab[i])
         {
         stat_value = ftt_extract_stats (delstat[0],i); 
	 if (stat_value == 0) 
	     {
	     stat_value = "(null)";
	     }
         fprintf (stderr,"%s is %s\n",ftt_stat_names[i],stat_value);
         }
      }
   }
else if (errorrate)
   {
   double errors, total ;

   stat_value = ftt_extract_stats(delstat[0],FTT_READ_COUNT);
   total = stat_value ? (double)(atoi(stat_value)) : 0.0;
   stat_value = ftt_extract_stats(delstat[0],FTT_READ_ERRORS);
   errors = stat_value ? (double)(atoi(stat_value)) : 0.0;
   if ( total != 0 ) 
      {
      fprintf (stderr, "Read %7.0f blocks, corrected %7.0f errors, error rate %6.2f%%\n",
		   total, errors, (errors / total ) * 100 );
      }
   stat_value = ftt_extract_stats(delstat[0],FTT_WRITE_COUNT);
   total = stat_value ? (double)(atoi(stat_value)) : 0.0;
   stat_value = ftt_extract_stats(delstat[0],FTT_WRITE_ERRORS);
   errors = stat_value ? (double)(atoi(stat_value)) : 0.0;
   if ( total != 0 ) 
      {
      fprintf (stderr, "Wrote %7.0f blocks, corrected %7.0f errors, error rate %6.2f%%\n",
		   total, errors, (errors / total ) * 100 );
      }
   }
else
   {
   status = ftt_dump_stats(delstat[0],stderr);
   FTT_T_CHECK_CALL(status,estatus);
   }

return 0;
}
