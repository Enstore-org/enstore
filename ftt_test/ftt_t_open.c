static char rcsid[] = "$Id$";
/* 


Authors:        Margaret Votava
e-mail:         "votava@fnal.gov"
 
Revision history:-
=================
17-Oct-1995 MEV created	Wrapping routine for test commands 
 
Include files:-
===============
*/

#include <sys/utsname.h>			/* for uname */
#include "ftt.h"
#include "ftt_t_parse.h"
#include "ftt_t_macros.h"

extern char	*ftt_t_basename;		/* basename on cmdline */
ftt_descriptor	ftt_t_fd = 0;			/* file descriptor */

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

ftt_t_fd = ftt_open (basename,readonly);
FTT_T_CHECK_CALL_OPEN(ftt_t_fd,estatus);
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

ftt_t_fd = ftt_open_logical (basename,flavor,driveid,readonly);
FTT_T_CHECK_CALL_OPEN(ftt_t_fd,estatus);
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

status = ftt_open_dev(ftt_t_fd);
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

status = ftt_close (ftt_t_fd);
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

status = ftt_close_dev(ftt_t_fd);
FTT_T_CHECK_CALL (status,estatus);
return 0;

}

