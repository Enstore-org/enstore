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

#define FTT_T_CHECK_LABELTYPE(type_str,type)	{			\
if (type_str)								\
   {									\
   if (!strcasecmp(type_str,"FTT_ANSI_HEADER"))	type = FTT_ANSI_HEADER;\
   else if (!strcasecmp(type_str,"FTT_FMB_HEADER"))type = FTT_FMB_HEADER;\
   else if (!strcasecmp(type_str,"FTT_TAR_HEADER"))type = FTT_TAR_HEADER;\
   else if (!strcasecmp(type_str,"FTT_CPIO_HEADER"))type = FTT_CPIO_HEADER;\
   else if (!strcasecmp(type_str,"FTT_UNKNOWN_HEADER"))type=FTT_UNKNOWN_HEADER;\
   else if (!strcasecmp(type_str,"FTT_BLANK_HEADER"))			\
      type = FTT_BLANK_HEADER;						\
   else if (!strcasecmp(type_str,"FTT_DONTCHECK_HEADER"))		\
      type = FTT_DONTCHECK_HEADER;					\
   else									\
      {									\
      fprintf (stderr, "%s is an invalid type.\n",type_str);		\
      return 1;								\
      }									\
   }									\
}


/* ============================================================================

ROUTINE: ftt_t_verify_vol_label
 	
	call ftt_verify_vol_label
==============================================================================*/
int	ftt_t_verify_vol_label(int argc, char **argv)
{
int 		status;				/* status */
int		estatus = 0;			/* expected status */
int		type = FTT_ANSI_HEADER;		/* type */
static char	*type_str;			/* type */
static int	nsec;				/* timeout */
static int	readonly;			/* readonly flag */
static char	*label;				/* label to match */
static char	*estatus_str;			/* expected status string */
ftt_t_argt	argt[] = {
 	{"-type",	FTT_T_ARGV_STRING,	NULL,		&type_str},
 	{"-label",	FTT_T_ARGV_STRING,	NULL,		&label},
 	{"-timeout",	FTT_T_ARGV_INT,		NULL,		&nsec},
 	{"-readonly",	FTT_T_ARGV_INT,		NULL,		&readonly},
 	{"-status",	FTT_T_ARGV_STRING,	NULL,		&estatus_str},
 	{NULL,		FTT_T_ARGV_END,		NULL,		NULL}};

/* parse command line
   ------------------ */

estatus_str = NULL; type_str = NULL; nsec = 0; readonly = FALSE; label = NULL;
status = ftt_t_parse(&argc, argv, argt);
FTT_T_CHECK_PARSE (status, argt, argv[0]);	/* check parse status */
FTT_T_CHECK_ESTATUS (estatus_str, estatus);
FTT_T_CHECK_LABELTYPE(type_str,type);

status = ftt_verify_vol_label(ftt_t_fd,type,label,nsec,readonly); 
FTT_T_CHECK_CALL(status ,estatus);
return 0;
}

/* ============================================================================

ROUTINE: ftt_t_write_vol_label
 	
	call ftt_write_vol_label
==============================================================================*/
int	ftt_t_write_vol_label(int argc, char **argv)
{
int 		status;				/* status */
int		estatus = 0;			/* expected status */
int		type = FTT_ANSI_HEADER;		/* type */
static char	*type_str;			/* type */
static char	*label;				/* label to match */
static char	*estatus_str;			/* expected status string */
ftt_t_argt	argt[] = {
 	{"-type",	FTT_T_ARGV_STRING,	NULL,		&type_str},
 	{"-label",	FTT_T_ARGV_STRING,	NULL,		&label},
 	{"-status",	FTT_T_ARGV_STRING,	NULL,		&estatus_str},
 	{NULL,		FTT_T_ARGV_END,		NULL,		NULL}};

/* parse command line
   ------------------ */

estatus_str = NULL; type_str = NULL; label = NULL;
status = ftt_t_parse(&argc, argv, argt);
FTT_T_CHECK_PARSE (status, argt, argv[0]);	/* check parse status */
FTT_T_CHECK_ESTATUS (estatus_str, estatus);
FTT_T_CHECK_LABELTYPE(type_str,type);

status = ftt_write_vol_label(ftt_t_fd,type,label); 
FTT_T_CHECK_CALL(status ,estatus);
return 0;
}
