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

#include "ftt.h"
#include "ftt_t_parse.h"
#include "ftt_t_macros.h"


/* ============================================================================

ROUTINE: ftt_t_cur_part
 	
	call ftt_cur_part
==============================================================================*/
int	ftt_t_cur_part (int argc, char **argv)
{
int 		status;			/* status */
int		partno;			/* file number */
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

/* get the partition
   ---------------- */
partno = ftt_cur_part(ftt_t_fd);                     
fprintf (stderr, "Partition no: %d\n",partno);

return 0;
}


/* ============================================================================

ROUTINE: ftt_t_skip_part
 	
	call ftt_skip_part using the global file descriptor
==============================================================================*/
int	ftt_t_skip_part	(int argc, char **argv)
{
int 		status;				/* status */
int 		estatus = 0;			/* expected status */
static char	*estatus_str;			/* expected status string */
static int	async;				/* async flag */
static int	nrec;				/* number to skip */
ftt_t_argt	argt[] = {
	{"<nrecords>",	FTT_T_ARGV_INT,		NULL,		&nrec},
        {"-async",      FTT_T_ARGV_CONSTANT,    (char *)TRUE,   &async},
 	{"-status",	FTT_T_ARGV_STRING,	NULL,		&estatus_str},
 	{NULL,		FTT_T_ARGV_END,		NULL,		NULL}};

/* parse command line
   ------------------ */

estatus_str = 0; nrec = 1; async = FALSE;
status = ftt_t_parse (&argc, argv, argt);
FTT_T_CHECK_PARSE (status, argt, argv[0]);	/* check parse status */
FTT_T_CHECK_ESTATUS (estatus_str, estatus);	/* check expected status opt */

/* do the skip
   ----------- */

FTT_T_ASYNC (async,ftt_t_fd,ftt_skip_part(ftt_t_fd,nrec),status);
FTT_T_CHECK_CALL (status,estatus);
return 0;

}

static ftt_partbuf parttab;
void
ftt_t_dump_partitions() {
   ftt_dump_partitions(parttab,stdout);
}


/* ============================================================================

ROUTINE: ftt_t_get_partitions
 	
	call ftt_get_partitions using the global file descriptor and
                static parttab partition buffer.
==============================================================================*/
int ftt_t_get_partitions(int argc, char **argv)
{
int 		status;				/* status */
int 		estatus = 0;			/* expected status */
static char	*estatus_str;			/* expected status string */

if (!parttab) { parttab = ftt_alloc_parts(); }

FTT_T_CHECK_CALL (ftt_get_partitions(ftt_t_fd,parttab), estatus);
return 0;
}



/* ============================================================================

ROUTINE: ftt_t_writ_partitions
        call ftt_write_partitions using the global file descriptor and
                static parttab partition buffer.
==============================================================================*/
int ftt_t_write_partitions(int argc, char **argv)
{
int 		status;				/* status */
int 		estatus = 0;			/* expected status */
static char	*estatus_str;			/* expected status string */


if (!parttab) { printf("You must get a partition table first!\n"); return 0; }

FTT_T_CHECK_CALL (ftt_write_partitions(ftt_t_fd,parttab), estatus);
return 0;
}


/* ============================================================================
Set partition size...
==============================================================================*/

int
ftt_t_set_part_size(int argc, char **argv)
{
int 		status;				/* status */
int 		estatus = 0;			/* expected status */
static char	*estatus_str;			/* expected status string */
static int	size;				/* async flag */
static int	partno;				/* number to skip */
ftt_t_argt	argt[] = {
	{"<partno>",	FTT_T_ARGV_INT,		NULL,		&partno},
	{"<size>",	FTT_T_ARGV_INT,		NULL,		&size},
 	{NULL,		FTT_T_ARGV_END,		NULL,		NULL}};
if (!parttab) { printf("You must get a partition table first!\n"); return 0; }
status = ftt_t_parse (&argc, argv, argt);
FTT_T_CHECK_PARSE (status, argt, argv[0]);	/* check parse status */
ftt_set_part_size(parttab,partno,size);
return 0;
}
int
ftt_t_set_nparts(int argc, char **argv)
{
int 		status;				/* status */
int 		estatus = 0;			/* expected status */
static char	*estatus_str;			/* expected status string */
static int	partno;				/* number to skip */
ftt_t_argt	argt[] = {
	{"<nparts>",	FTT_T_ARGV_INT,		NULL,		&partno},
 	{NULL,		FTT_T_ARGV_END,		NULL,		NULL}};
if (!parttab) { printf("You must get a partition table first!\n"); return 0; }
status = ftt_t_parse (&argc, argv, argt);
FTT_T_CHECK_PARSE (status, argt, argv[0]);	/* check parse status */
ftt_set_nparts(parttab,partno);
return 0;
}
