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
#include "ftt.h"
#include "ftt_t_parse.h"
#include "ftt_t_macros.h"

int ftt_clear_unrecovered();

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

ROUTINE:
	ftt_t_max_errors
 	
	set/display the maximum number of errors before exit

==============================================================================*/
int	ftt_t_max_errors	(int argc, char **argv)
{
int 		status;			/* status */
static int	maxerror;	
ftt_t_argt	argt[] = {
	{"[maxerror]",	FTT_T_ARGV_INT,		NULL,		&maxerror},
	{NULL,		FTT_T_ARGV_END,		NULL,		NULL}};

/* parse command line
   ------------------ */

maxerror = -1; 
status = ftt_t_parse (&argc, argv, argt);
FTT_T_CHECK_PARSE (status, argt, argv[0]);

/* either set or show debug level
   ------------------------------ */

if (maxerror == -1)					/* show */
   fprintf(stderr,"Current maximum number of errors is : %d\n",ftt_t_max_error);
else							/* change */
   ftt_t_max_error = maxerror;

return 0;
}

/* ============================================================================

ROUTINE: ftt_t_all_scsi
 	
	call ftt_all_scsi
==============================================================================*/
int	ftt_t_all_scsi (int argc, char **argv)
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

status = ftt_all_scsi(ftt_t_fd);                     
FTT_T_CHECK_CALL (status,estatus);

return 0;
}

/* ============================================================================

ROUTINE: ftt_t_clear_unrecovered
 	
	call ftt_clear_unrecovered
==============================================================================*/
int	ftt_t_clear_unrecovered (int argc, char **argv)
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

status = ftt_clear_unrecovered(ftt_t_fd);                     
FTT_T_CHECK_CALL (status,estatus);

return 0;
}

/* ============================================================================

ROUTINE: ftt_t_format_ait
 	
	call ftt_format_ait
==============================================================================*/
int	ftt_t_format_ait (int argc, char **argv)
{
int 		status;			/* status */
int		estatus = 0;		/* expected status */
static char	*estatus_str;		/* expected status string */
static int 	onflag;			/* flag... */
ftt_t_argt	argt[] = {
 	{"-on",	        FTT_T_ARGV_INT,		NULL,		&onflag},
 	{NULL,		FTT_T_ARGV_END,		NULL,		NULL}};
extern ftt_partbuf *parttab;

/* parse command line
   ------------------ */

estatus_str = NULL;
onflag = 1;
status = ftt_t_parse (&argc, argv, argt);
FTT_T_CHECK_PARSE (status, argt, argv[0]);	/* check parse status */
FTT_T_CHECK_ESTATUS (estatus_str, estatus);

status = ftt_format_ait(ftt_t_fd,onflag,parttab);
FTT_T_CHECK_CALL (status,estatus);

return 0;
}

/* ============================================================================

ROUTINE: ftt_t_modesense
 	
	call ftt_modesense
==============================================================================*/
int	ftt_t_modesense (int argc, char **argv)
{
int 		status;			/* status */
int		estatus = 0;		/* expected status */
static char	*estatus_str;		/* expected status string */
ftt_t_argt	argt[] = {
 	{NULL,		FTT_T_ARGV_END,		NULL,		NULL}};

/* parse command line
   ------------------ */

estatus_str = NULL;
status = ftt_t_parse (&argc, argv, argt);
FTT_T_CHECK_PARSE (status, argt, argv[0]);	/* check parse status */
FTT_T_CHECK_ESTATUS (estatus_str, estatus);

status = ftt_modesense(ftt_t_fd);                     
FTT_T_CHECK_CALL (status,estatus);

return 0;
}

/* ============================================================================

ROUTINE: ftt_t_logsense
 	
	call ftt_logsense
==============================================================================*/
int	ftt_t_logsense (int argc, char **argv)
{
int 		status;			/* status */
int		estatus = 0;		/* expected status */
static char	*estatus_str;		/* expected status string */
ftt_t_argt	argt[] = {
 	{NULL,		FTT_T_ARGV_END,		NULL,		NULL}};

/* parse command line
   ------------------ */

estatus_str = NULL;
status = ftt_t_parse (&argc, argv, argt);
FTT_T_CHECK_PARSE (status, argt, argv[0]);	/* check parse status */
FTT_T_CHECK_ESTATUS (estatus_str, estatus);

status = ftt_logsense(ftt_t_fd);                     
FTT_T_CHECK_CALL (status,estatus);

return 0;
}

/* ============================================================================

ROUTINE: ftt_t_inquire
 	
	call ftt_inquire
==============================================================================*/
int	ftt_t_inquire (int argc, char **argv)
{
int 		status;			/* status */
int		estatus = 0;		/* expected status */
static char	*estatus_str;		/* expected status string */
ftt_t_argt	argt[] = {
 	{NULL,		FTT_T_ARGV_END,		NULL,		NULL}};

/* parse command line
   ------------------ */

estatus_str = NULL;
status = ftt_t_parse (&argc, argv, argt);
FTT_T_CHECK_PARSE (status, argt, argv[0]);	/* check parse status */
FTT_T_CHECK_ESTATUS (estatus_str, estatus);

status = ftt_inquire(ftt_t_fd);                     
FTT_T_CHECK_CALL (status,estatus);

return 0;
}
