/* Ftt main test routine


Authors:        Margaret Votava
e-mail:         "votava@fnal.gov"
 
Revision history:-
=================
17-Oct-1995 MEV created	Wrapping routine for test commands 
 
Include files:-
===============
*/

#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <math.h>
#include <string.h>
#include <sys/utsname.h>			/* for uname */
#include "ftt.h"
#include "ftt_t_parse.h"
#include "ftt_t_macros.h"

#define FTT_T_MAXDSIZE	65535

/* Prototypes
   ========== */

void ftt_t_block_fill 	(char *, int, int, int);
int  ftt_t_block_dump	(FILE *, int, char *, int);
int  ftt_t_block_undump	(FILE *, char *);
int  ftt_t_block_verify	(char *, int, int, int);

/* Macros:-
   ======== */


#define FTT_T_INC_NERROR(){					\
nerror++;							\
if (nerror > max_error)						\
   {								\
   fprintf (stderr, "Maximum number of errors (%d) exceeded\n",	\
      max_error);						\
   exit(1);							\
   }								\
}

#define FTT_T_CHECK_PARSE(status,argt,cmdname){	\
   if (status == FTT_T_USAGE)			\
      {						\
      ftt_t_print_usage (argt,cmdname);		\
      return (0);				\
      }						\
   else if (status)				\
      {						\
      ftt_t_print_usage (argt,cmdname);		\
      return (status);				\
      }						\
   }

#define FTT_T_CHECK_CALL(status,estatus) {				\
   if ((int)(status) >= 0)						\
      {									\
      if (estatus != 0) 						\
	 {								\
         fprintf (stderr,"Command successful, but error %s expected\n",	\
	    ftt_ascii_error[estatus]);					\
	 return ((int)(status));					\
	 }								\
      }									\
   else									\
      {									\
      int error;							\
      char *errstring;							\
      errstring = ftt_get_error(&error);				\
      if ((int)(status) != estatus)					\
         {								\
 	 fprintf (stderr, "%s\n",errstring);				\
	 FTT_T_INC_NERROR();						\
	 return (error);						\
         }								\
      }									\
   }

#define FTT_T_CHECK_CALL_OPEN(status,estatus) {				\
   if ((int)(status))							\
      {									\
      if (estatus != 1) 						\
	 {								\
         fprintf (stderr,"Command successful, but error %s expected\n",	\
	    ftt_ascii_error[estatus]);					\
	 return ((int)(status));					\
	 }								\
      }									\
   else									\
      {									\
      int error;							\
      char *errstring;							\
      errstring = ftt_get_error(&error);				\
      if ((int)(status) != estatus)					\
         {								\
 	 fprintf (stderr, "%s\n",errstring);				\
	 FTT_T_INC_NERROR();						\
	 return (error);						\
         }								\
      }									\
   }

#define FTT_T_CHECK_ASYNC(astring,alevel) {				\
   if (astring)								\
      {									\
      if      (!strcasecmp("FTT_SYNC",astring))   alevel = FTT_SYNC;	\
      else if (!strcasecmp("FTT_ASYNC",astring))  alevel = FTT_ASYNC;	\
      else if (!strcasecmp("FTT_NOWAIT",astring)) alevel = FTT_NOWAIT;	\
      else 								\
         {								\
         fprintf (stderr,"Error: Invalid async level specified: %s\n",astring);\
         return 1;							\
         }								\
      }									\
   }

#define FTT_T_CHECK_ESTATUS(estring,estatus) {				\
   if (estring)								\
      {									\
      int i;								\
      for (i = 0; i <= FTT_ENOTTAPE; i++)				\
         if (!strcasecmp(ftt_ascii_error[i],estring))			\
	    {								\
	    estatus = i;						\
	    break;							\
	    }								\
      if (i > FTT_ENOTTAPE)						\
         {								\
         fprintf (stderr,"Invalid error code specified: %s\n",estring);	\
         return 1;							\
         }								\
      }									\
   }

/* ============================================================================

ROUTINE:
	ftt_t_date
 	
	print date to stderr
==============================================================================*/
int	ftt_t_date		(int argc, char **argv)
{

time_t	stime;			/* time */
int 		status;
ftt_t_argt	argt[] = {{NULL, FTT_T_ARGV_END, NULL, NULL}};

/* parse command line
   ------------------ */

status = ftt_t_parse (&argc, argv, argt);
FTT_T_CHECK_PARSE(status,argt,argv[0]);

/* echo date
   --------- */

stime = time(NULL);					/* display time */
fprintf (stderr, asctime(localtime(&stime)));
return 0;						/* success */
}


/* ============================================================================

ROUTINE:
	ftt_t_echo
 	
	print string to stderr
==============================================================================*/
int	ftt_t_echo		(int argc, char **argv)
{
int 		status;				/* status */
static char	*mystring;			/* string */
ftt_t_argt	argt[] = {
	{"<mystring>",	FTT_T_ARGV_STRING,	NULL,		&mystring},
	{NULL,		FTT_T_ARGV_END,		NULL,		NULL}};

/* parse command line
   ------------------ */

mystring = NULL;
status = ftt_t_parse (&argc, argv, argt);
FTT_T_CHECK_PARSE (status, argt, argv[0]);

/* echo string to stderr
   --------------------- */

fprintf (stderr,"%s\n",mystring);
return 0;
}

/* ============================================================================

ROUTINE:
	ftt_t_debug_level
 	
	set/display the debug level

==============================================================================*/
int	ftt_t_debug_level	(int argc, char **argv)
{
int 		status;			/* status */
static int	testflag;		/* test flag */
static int	level;			/* string */
ftt_t_argt	argt[] = {
	{"[level]",	FTT_T_ARGV_INT,		NULL,		&level},
	{"-test",	FTT_T_ARGV_CONSTANT,	(char *)TRUE,	&testflag},
	{NULL,		FTT_T_ARGV_END,		NULL,		NULL}};

/* parse command line
   ------------------ */

level = -1; testflag = FALSE;
status = ftt_t_parse (&argc, argv, argt);
FTT_T_CHECK_PARSE (status, argt, argv[0]);

/* either set or show debug level
   ------------------------------ */

if (level == -1)					/* show current level */
   {
   fprintf (stderr,"Current ftt debug level is: %d\n",ftt_debug);
   fprintf (stderr,"Current test debug level is: %d\n",ftt_t_debug);
   }
else							/* change level */
   {
   if (testflag)
      ftt_t_debug = level;
   else
      ftt_debug = level;
   }

return 0;
}


/* ============================================================================

ROUTINE: ftt_t_get_error
 	
	call ftt_get_error 
==============================================================================*/
int	ftt_t_get_error (int argc, char **argv)
{
int 		status;				/* status */
char		*estring;			/* error string */
ftt_t_argt	argt[] = {
 	{NULL,		FTT_T_ARGV_END,		NULL,		NULL}};

/* parse command line
   ------------------ */

status = ftt_t_parse (&argc, argv, argt);
FTT_T_CHECK_PARSE (status, argt, argv[0]);	/* check parse status */

/* get the error
   ------------- */
estring = ftt_get_error(&status);                     
fprintf (stderr, "Error: %s Error String: %s \n",
   ftt_ascii_error[status],estring);

return 0;
}


/* ============================================================================

ROUTINE: ftt_t_eprintf
 	
	call ftt_eprintf 
==============================================================================*/
int	ftt_t_eprintf (int argc, char **argv)
{
int 		status;				/* status */
static char	*estring;			/* error string */
ftt_t_argt	argt[] = {
 	{"<err_string>",FTT_T_ARGV_STRING,	NULL,		&estring},
 	{NULL,		FTT_T_ARGV_END,		NULL,		NULL}};

/* parse command line
   ------------------ */

estring = NULL;
status = ftt_t_parse (&argc, argv, argt);
FTT_T_CHECK_PARSE (status, argt, argv[0]);	/* check parse status */

status = ftt_eprintf(estring);                     
FTT_T_CHECK_CALL (status,0);
return 0;

}

/* ============================================================================

ROUTINE: ftt_t_get_position
 	
	call ftt_get_position
==============================================================================*/
int	ftt_t_get_position (int argc, char **argv)
{
int 		status;			/* status */
int		fileno;			/* file number */
int		blockno;		/* block number */
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

/* get the position
   ---------------- */
status = ftt_get_position(ftt_fd,&fileno,&blockno);                     
FTT_T_CHECK_CALL (status,estatus);
if (!status)
   fprintf (stderr, "File no: %d block no: %d\n",fileno,blockno);

return 0;
}


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
   FTT_T_CHECK_CALL_OPEN(statbuf,estatus);
   }

status = ftt_get_stats (ftt_fd,statbuf);                     
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
   -----------------------------

for (i = 0; i < FTT_MAX_STAT; i++)
   {
   if (!strcasecmp(stat_name,ftt_stat_name[i])) match = ftt_stat_name[i];
   }

if (!match)
   {
   fprintf (stderr,"%s is not a valid statistic name.\n",stat_name);
   return 1;
   }

/* get the value
   ------------- */
stat_value = ftt_extract_stats (statbuf,i); 
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
   FTT_T_CHECK_CALL_OPEN(statbuf,estatus);
   }

status = ftt_dump_stats (ftt_fd,outfile); 
if (status == -1 && outfile != stderr) close (outfile);
FTT_T_CHECK_CALL (status,estatus);

if (outfile != stderr) close (outfile);
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

ROUTINE: ftt_t_open
 	

/* ============================================================================

ROUTINE: ftt_t_open
 	
	call ftt open using the global file descriptor
==============================================================================*/
int	ftt_t_open	(int argc, char **argv)
{
int 		status;			/* status */
int		estatus = 1;		/* expected status */
static char	*estatus_str;		/* expected status string */
static int	readonly;		/* readonly */
static char	*basename;		/* basename */
ftt_t_argt	argt[] = {
 	{"[basename]",	FTT_T_ARGV_STRING,	NULL,		&basename},
	{"-readonly",	FTT_T_ARGV_CONSTANT,	(char *)TRUE,	&readonly},
 	{"-status",	FTT_T_ARGV_STRING,	NULL,		&estatus_str},
 	{NULL,		FTT_T_ARGV_END,		NULL,		NULL}};

/* parse command line
   ------------------ */

readonly = FALSE; basename = NULL; estatus_str = NULL;
status = ftt_t_parse (&argc, argv, argt);
FTT_T_CHECK_PARSE (status, argt, argv[0]);
FTT_T_CHECK_ESTATUS (estatus_str, estatus);

/* basename not passed, first look if it was on
   invocation line. if not, use $FTT_TAPE
   --------------------------------------------- */

if (!basename)
   {
   if (ftt_t_basename)
      {
      basename = ftt_t_basename;
      }
   else
      {
      basename = (char *)getenv("FTT_TAPE");
      if (!basename)
         {
         fprintf (stderr,"no basename was specified\n");
         return 1;
         }
      }
   }

/* do the open
   ----------- */

ftt_fd = ftt_open (basename,readonly);
FTT_T_CHECK_CALL_OPEN(ftt_fd,estatus);
return 0;

}


/* ============================================================================

ROUTINE: ftt_t_open_logical
 	
	call ftt open logical using the global file descriptor
==============================================================================*/
int	ftt_t_open_logical	(int argc, char **argv)
{
int 		status;			/* status */
int		estatus = 1;		/* expected status */
struct utsname	uname_buf;		/* uname buffer */
static char	*estatus_str;		/* expected status string */
static int	readonly;		/* readonly */
static char	*basename;		/* basename */
static char	*flavor;		/* os flavor */
static char	*driveid;		/* drive id */
ftt_t_argt	argt[] = {
 	{"[basename]",	FTT_T_ARGV_STRING,	NULL,		&basename},
 	{"-flavor",	FTT_T_ARGV_STRING,	NULL,		&flavor},
 	{"-driveid",	FTT_T_ARGV_STRING,	NULL,		&driveid},
	{"-readonly",	FTT_T_ARGV_CONSTANT,	(char *)TRUE,	&readonly},
 	{"-status",	FTT_T_ARGV_STRING,	NULL,		&estatus_str},
 	{NULL,		FTT_T_ARGV_END,		NULL,		NULL}};

/* parse command line
   ------------------ */

readonly = FALSE; basename = NULL; estatus_str = NULL; flavor = NULL; 
driveid = NULL;
status = ftt_t_parse (&argc, argv, argt);
FTT_T_CHECK_PARSE (status, argt, argv[0]);
FTT_T_CHECK_ESTATUS (estatus_str, estatus);

/* basename not passed, first look if it was on
   invocation line. if not, use $FTT_TAPE
   --------------------------------------------- */

if (!basename)
   {
   if (ftt_t_basename)
      {
      basename = ftt_t_basename;
      }
   else
      {
      basename = (char *)getenv("FTT_TAPE");
      if (!basename)
         {
         fprintf (stderr,"no basename was specified\n");
         return 1;
         }
      }
   }

/* drive id must be passed
   ----------------------- */

if (!driveid)
   {
   fprintf (stderr,"Drive id was not specified.\n");
   return 1;
   }

/* get flavor if not set
   --------------------- */

if (!flavor)
   {
   uname(&uname_buf);
   flavor = uname_buf.sysname;
   }

/* do the open
   ----------- */

ftt_fd = ftt_open_logical (basename,flavor,driveid,readonly);
FTT_T_CHECK_CALL_OPEN(ftt_fd,estatus);
return 0;

}

/* ============================================================================

ROUTINE: ftt_t_open_dev
 	
	call ftt_open_dev using the global file descriptor
==============================================================================*/
int	ftt_t_open_dev	(int argc, char **argv)
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

status = ftt_open_dev(ftt_fd);
FTT_T_CHECK_CALL (status,estatus);
return 0;

}


/* ============================================================================

ROUTINE: ftt_t_close
 	
	call ftt_close using the global file descriptor
==============================================================================*/
int	ftt_t_close	(int argc, char **argv)
{
int 		status;				/* status */
int		estatus = 0;			/* expected error */
static char	*estatus_str;		/* expected status string */
ftt_t_argt	argt[] = {
 	{"-status",	FTT_T_ARGV_STRING,	NULL,		&estatus_str},
 	{NULL,		FTT_T_ARGV_END,		NULL,		NULL}};

/* parse command line
   ------------------ */

estatus_str = 0;
status = ftt_t_parse (&argc, argv, argt);
FTT_T_CHECK_PARSE (status, argt, argv[0]);
FTT_T_CHECK_ESTATUS (estatus_str, estatus);

status = ftt_close (ftt_fd);
FTT_T_CHECK_CALL (status,estatus);
return 0;

}


/* ============================================================================

ROUTINE: ftt_t_close_dev
 	
	call ftt_close_dev using the global file descriptor
==============================================================================*/
int	ftt_t_close_dev	(int argc, char **argv)
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

status = ftt_close_dev(ftt_fd);
FTT_T_CHECK_CALL (status,estatus);
return 0;

}


/* ============================================================================

ROUTINE: ftt_t_allow_async
 	
	call ftt_allow_async using the global file descriptor
==============================================================================*/
int	ftt_t_allow_async (int argc, char **argv)
{
int 		status;				/* status */
int 		estatus = 0;			/* expected status */
static char	*estatus_str;			/* expected status string */
int		async = -1;			/* asych level */
static int	nrec;				/* number to skip */
static char	*async_str;			/* string pointer */
ftt_t_argt	argt[] = {
 	{"[async_level]",FTT_T_ARGV_STRING,	NULL,		&async_str},
 	{"-status",	FTT_T_ARGV_STRING,	NULL,		&estatus_str},
 	{NULL,		FTT_T_ARGV_END,		NULL,		NULL}};

/* parse command line
   ------------------ */

estatus_str = 0; nrec = 1; async_str = NULL;
status = ftt_t_parse (&argc, argv, argt);
FTT_T_CHECK_PARSE (status, argt, argv[0]);	/* check parse status */
FTT_T_CHECK_ESTATUS (estatus_str, estatus);	/* check expected status opt */
FTT_T_CHECK_ASYNC(async_str,async);		/* check asynch opt */

/* do the skip
   ----------- */

if (async == -1) 				/* display async level */
   {
#if 0
   if ( ((ftt_descriptor)ftt_fd)->async_max == FTT_SYNC)
      fprintf (stderr, "Current async level is FTT_SYNC.\n");
   else if ( ((ftt_descriptor) ftt_fd)->async_max == FTT_ASYNC)
      fprintf (stderr, "Current async level is FTT_ASYNC.\n");
   else if ( ((ftt_descriptor) ftt_fd)->async_max == FTT_NOWAIT)
      fprintf (stderr, "Current async level is FTT_NOWAIT.\n");
   else 
      fprintf (stderr, "Current async level is unknown.\n");
#endif
   fprintf (stderr, "I don't know how to display this yet\n");
   }
else
   {
   status = ftt_allow_async (ftt_fd,async);
   FTT_T_CHECK_CALL (status,estatus);
   }
return 0;

}

/* ============================================================================

ROUTINE: ftt_t_skip_rec
 	
	call ftt_skip_rec using the global file descriptor
==============================================================================*/
int	ftt_t_skip_rec	(int argc, char **argv)
{
int 		status;				/* status */
int 		estatus = 0;			/* expected status */
static char	*estatus_str;			/* expected status string */
int		async = FTT_SYNC;		/* asych level */
static int	nrec;				/* number to skip */
static char	*async_str;			/* string pointer */
ftt_t_argt	argt[] = {
	{"<nrecords>",	FTT_T_ARGV_INT,		NULL,		&nrec},
 	{"-async",	FTT_T_ARGV_STRING,	NULL,		&async_str},
 	{"-status",	FTT_T_ARGV_STRING,	NULL,		&estatus_str},
 	{NULL,		FTT_T_ARGV_END,		NULL,		NULL}};

/* parse command line
   ------------------ */

estatus_str = 0; nrec = 1; async_str = NULL;
status = ftt_t_parse (&argc, argv, argt);
FTT_T_CHECK_PARSE (status, argt, argv[0]);	/* check parse status */
FTT_T_CHECK_ESTATUS (estatus_str, estatus);	/* check expected status opt */
FTT_T_CHECK_ASYNC(async_str,async);		/* check asynch opt */

/* do the skip
   ----------- */

status = ftt_skip_rec (ftt_fd,nrec,async);
FTT_T_CHECK_CALL (status,estatus);
return 0;

}


/* ============================================================================

ROUTINE: ftt_t_skip_fm
 	
	call ftt_skip_fm using the global file descriptor
==============================================================================*/
int	ftt_t_skip_fm	(int argc, char **argv)
{
int 		status;				/* status */
int 		estatus = 0;			/* expected status */
int		async = FTT_SYNC;		/* asych level */
static int	nfm;				/* number to skip */
static char	*async_str;			/* string pointer */
static char	*estatus_str;			/* expected status string */
ftt_t_argt	argt[] = {
	{"<nfilemarks>",FTT_T_ARGV_INT,		NULL,		&nfm},
 	{"-async",	FTT_T_ARGV_STRING,	NULL,		&async_str},
 	{"-status",	FTT_T_ARGV_STRING,	NULL,		&estatus_str},
 	{NULL,		FTT_T_ARGV_END,		NULL,		NULL}};

/* parse command line
   ------------------ */

estatus_str = 0; nfm = 1; async_str = NULL;
status = ftt_t_parse (&argc, argv, argt);
FTT_T_CHECK_PARSE (status, argt, argv[0]);	/* check parse status */
FTT_T_CHECK_ESTATUS (estatus_str, estatus);	/* check expected status opt */
FTT_T_CHECK_ASYNC(async_str,async);		/* check asynch opt */

/* do the skip
   ----------- */

status = ftt_skip_fm (ftt_fd,nfm,async);
FTT_T_CHECK_CALL (status,estatus);
return 0;

}


/* ============================================================================

ROUTINE: ftt_t_skip_to_double_fm
 	
	call ftt_skip_to_double_fm using the global file descriptor
==============================================================================*/
int	ftt_t_skip_to_double_fm	(int argc, char **argv)
{
int 		status;				/* status */
int 		estatus = 0;			/* expected status */
int		async = FTT_SYNC;		/* asych level */
static char	*async_str;			/* string pointer */
static char	*estatus_str;			/* expected status string */
ftt_t_argt	argt[] = {
 	{"-async",	FTT_T_ARGV_STRING,	NULL,		&async_str},
 	{"-status",	FTT_T_ARGV_STRING,	NULL,		&estatus_str},
 	{NULL,		FTT_T_ARGV_END,		NULL,		NULL}};

/* parse command line
   ------------------ */

estatus_str = 0; async_str = NULL;
status = ftt_t_parse (&argc, argv, argt);
FTT_T_CHECK_PARSE (status, argt, argv[0]);	/* check parse status */
FTT_T_CHECK_ESTATUS (estatus_str, estatus);	/* check expected status opt */
FTT_T_CHECK_ASYNC(async_str,async);		/* check asynch opt */

/* do the skip
   ----------- */

#if 0
status = ftt_skip_to_double_fm (ftt_fd,async);
#endif
FTT_T_CHECK_CALL (status,estatus);
return 0;

}


/* ============================================================================

ROUTINE: ftt_t_rewind
 	
	call ftt_rewind using the global file descriptor
==============================================================================*/
int	ftt_t_rewind (int argc, char **argv)
{
int 		status;				/* status */
int 		estatus = 0;			/* expected status */
int		async = FTT_SYNC;		/* asych level */
static char	*async_str;			/* string pointer */
static char	*estatus_str;			/* expected status string */
ftt_t_argt	argt[] = {
 	{"-async",	FTT_T_ARGV_STRING,	NULL,		&async_str},
 	{"-status",	FTT_T_ARGV_STRING,	NULL,		&estatus_str},
 	{NULL,		FTT_T_ARGV_END,		NULL,		NULL}};

/* parse command line
   ------------------ */

estatus_str = 0; async_str = NULL;
status = ftt_t_parse (&argc, argv, argt);
FTT_T_CHECK_PARSE (status, argt, argv[0]);	/* check parse status */
FTT_T_CHECK_ESTATUS (estatus_str, estatus);	/* check expected status opt */
FTT_T_CHECK_ASYNC(async_str,async);		/* check asynch opt */

/* do the rewind
   ------------- */

status = ftt_rewind (ftt_fd,async);
FTT_T_CHECK_CALL (status,estatus);
return 0;

}


/* ============================================================================

ROUTINE: ftt_t_retension
 	
	call ftt_retension using the global file descriptor
==============================================================================*/
int	ftt_t_retension (int argc, char **argv)
{
int 		status;				/* status */
int 		estatus = 0;			/* expected status */
int		async = FTT_SYNC;		/* asych level */
static char	*async_str;			/* string pointer */
static char	*estatus_str;			/* expected status string */
ftt_t_argt	argt[] = {
 	{"-async",	FTT_T_ARGV_STRING,	NULL,		&async_str},
 	{"-status",	FTT_T_ARGV_STRING,	NULL,		&estatus_str},
 	{NULL,		FTT_T_ARGV_END,		NULL,		NULL}};

/* parse command line
   ------------------ */

estatus_str = 0; async_str = NULL;
status = ftt_t_parse (&argc, argv, argt);
FTT_T_CHECK_PARSE (status, argt, argv[0]);	/* check parse status */
FTT_T_CHECK_ESTATUS (estatus_str, estatus);	/* check expected status opt */
FTT_T_CHECK_ASYNC(async_str,async);		/* check asynch opt */

/* do the retension
   ---------------- */

status = ftt_retension (ftt_fd,async);
FTT_T_CHECK_CALL (status,estatus);
return 0;

}


/* ============================================================================

ROUTINE: ftt_t_erase
 	
	call ftt_erase using the global file descriptor
==============================================================================*/
int	ftt_t_erase (int argc, char **argv)
{
int 		status;				/* status */
int 		estatus = 0;			/* expected status */
int		async = FTT_SYNC;		/* asych level */
static char	*async_str;			/* string pointer */
static char	*estatus_str;			/* expected status string */
ftt_t_argt	argt[] = {
 	{"-async",	FTT_T_ARGV_STRING,	NULL,		&async_str},
 	{"-status",	FTT_T_ARGV_STRING,	NULL,		&estatus_str},
 	{NULL,		FTT_T_ARGV_END,		NULL,		NULL}};

/* parse command line
   ------------------ */

estatus_str = 0; async_str = NULL;
status = ftt_t_parse (&argc, argv, argt);
FTT_T_CHECK_PARSE (status, argt, argv[0]);	/* check parse status */
FTT_T_CHECK_ESTATUS (estatus_str, estatus);	/* check expected status opt */
FTT_T_CHECK_ASYNC(async_str,async);		/* check asynch opt */

/* do the erase
   ------------ */

status = ftt_erase (ftt_fd,async);
FTT_T_CHECK_CALL (status,estatus);
return 0;

}


/* ============================================================================

ROUTINE: ftt_t_unload
 	
	call ftt_unload using the global file descriptor
==============================================================================*/
int	ftt_t_unload (int argc, char **argv)
{
int 		status;				/* status */
int 		estatus = 0;			/* expected status */
int		async = FTT_SYNC;		/* asych level */
static char	*async_str;			/* string pointer */
static char	*estatus_str;			/* expected status string */
ftt_t_argt	argt[] = {
 	{"-async",	FTT_T_ARGV_STRING,	NULL,		&async_str},
 	{"-status",	FTT_T_ARGV_STRING,	NULL,		&estatus_str},
 	{NULL,		FTT_T_ARGV_END,		NULL,		NULL}};

/* parse command line
   ------------------ */

estatus_str = 0; async_str = NULL;
status = ftt_t_parse (&argc, argv, argt);
FTT_T_CHECK_PARSE (status, argt, argv[0]);	/* check parse status */
FTT_T_CHECK_ESTATUS (estatus_str, estatus);	/* check expected status opt */
FTT_T_CHECK_ASYNC(async_str,async);		/* check asynch opt */

/* do the unload
   ------------- */

status = ftt_unload (ftt_fd,async);
FTT_T_CHECK_CALL (status,estatus);
return 0;

}


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

status = ftt_writefm(ftt_fd);
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
 	{"-nblock",	FTT_T_ARGV_INT,		NULL,		&nblock},
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

   status = ftt_get_position(ftt_fd, &fileno, &blockno);
   if (status)
      {
      fprintf (stderr, "write test block could not get file position: %s\n",
	 ftt_ascii_error[status]);
      FTT_T_INC_NERROR();
      return status;
      }

/* fill in the test block data
   --------------------------- */

   ftt_t_block_fill (wdata, thissize, fileno, blockno);

/* delay
   ----- */
   if (thisdelay) sleep (thisdelay);

/* finally, write out block
   ------------------------ */

   status = ftt_write (ftt_fd, wdata, thissize);
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
int 		bsize = FTT_T_MAXDSIZE;		/* block size */
static int	nblock;				/* number to read */
static int 	oddbyte;			/* read odd number of bytes */
static int 	ndelay;				/* delay between writes */
static int	filemark;			/* record is filemark */
static char	*estatus_str;			/* expected status string */
ftt_t_argt	argt[] = {
 	{"-status",	FTT_T_ARGV_STRING,	NULL,		&estatus_str},
 	{"-nblock",	FTT_T_ARGV_INT,		NULL,		&nblock},
 	{"-delay",	FTT_T_ARGV_INT,		NULL,		&ndelay},
 	{"-oddbyte",	FTT_T_ARGV_CONSTANT,	(char *)TRUE,	&oddbyte},
 	{"-filemark",	FTT_T_ARGV_CONSTANT,	(char *)TRUE,	&filemark},
 	{NULL,		FTT_T_ARGV_END,		NULL,		NULL}};

/* parse command line
   ------------------ */

estatus_str = 0; nblock = 1; oddbyte = FALSE; ndelay = 0; filemark = FALSE;
status = ftt_t_parse (&argc, argv, argt);
FTT_T_CHECK_PARSE (status, argt, argv[0]);	/* check parse status */
FTT_T_CHECK_ESTATUS (estatus_str, estatus);	/* check expected status opt */
if (oddbyte)
   bsize = bsize & 1 ? bsize : bsize - 1;	/* make odd read */
else
   bsize = bsize & 1 ? bsize - 1 : bsize;	/* make even read */

/* do the read(s)
   --------------- */

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

   status = ftt_get_position(ftt_fd, &fileno, &blockno);
   if (status)
      {
      fprintf (stderr, "write test block could not get file position: %s\n",
	 ftt_ascii_error[status]);
      FTT_T_INC_NERROR();
      return status;
      }

/* delay
   ----- */
   if (thisdelay) sleep (thisdelay);

/* clear the data 
   -------------- */

   memset (rdata,0,bsize);

/* read out block
   -------------- */

   thissize = ftt_read (ftt_fd, rdata, bsize);
   if (thissize < 0) 
      fprintf (stderr, "Verify of block %d file %d failed.\n",blockno,fileno);
   FTT_T_CHECK_CALL (thissize,estatus); 

/* fill in the test block data
   --------------------------- */

   if (filemark)
      {
      if (thissize != 0)
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
   else
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
 	{"-nblock",	FTT_T_ARGV_INT,		NULL,		&nblock},
 	{"-filename",	FTT_T_ARGV_STRING,	NULL,		&out_filename},
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

   thissize = ftt_read (ftt_fd, rdata, bsize);
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
      ftt_t_block_dump (outfile, i, rdata, thissize);	/* dump the test block*/
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
      status = ftt_writefm(ftt_fd);
      if (status < 0 && infile != stdin) fclose(infile);
      FTT_T_CHECK_CALL(status,estatus);
      }
   else if (length >= 0)
      {
      status = ftt_write(ftt_fd,wdata,length);
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
