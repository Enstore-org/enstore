/* 


Authors:        Margaret Votava
e-mail:         "votava@fnal.gov"
 
Revision history:-
=================
17-Oct-1995 MEV created	Wrapping routine for test commands 
 
Include files:-
===============
*/

#include "ftt.h"
#include "ftt_t_parse.h"
#include "ftt_t_macros.h"

/* Global variables
   ================ */
 
int              ftt_t_max_error = 20;  	/* max number of errs */
int              ftt_t_nerror = 0;       	/* current error count*/


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

ftt_eprintf(estring);                     
return 0;

}
