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
status = ftt_get_position(ftt_t_fd,&fileno,&blockno);                     
FTT_T_CHECK_CALL(status,estatus);
if (!status)
   fprintf (stderr, "File no: %d block no: %d\n",fileno,blockno);

return 0;
}


/* ============================================================================

ROUTINE: ftt_t_wait
 	
	call ftt_t_wait
==============================================================================*/
int	ftt_t_wait (int argc, char **argv)
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

/* wait
   ---- */

status = ftt_wait(ftt_t_fd);                     
FTT_T_CHECK_CALL (status,estatus);

return 0;
}

/* ============================================================================

ROUTINE: ftt_t_check
 	
	call ftt_t_check
==============================================================================*/
int	ftt_t_check (int argc, char **argv)
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

/* check
   ----- */
status = ftt_check(ftt_t_fd);                     
FTT_T_CHECK_CALL (status,estatus);

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

FTT_T_ASYNC (async,ftt_t_fd,ftt_skip_rec(ftt_t_fd,nrec),status);
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
static int	async;				/* asych flag */
static int	nfm;				/* number to skip */
static char	*estatus_str;			/* expected status string */
ftt_t_argt	argt[] = {
	{"<nfilemarks>",FTT_T_ARGV_INT,		NULL,		&nfm},
 	{"-async",	FTT_T_ARGV_CONSTANT,	(char *)TRUE,	&async},
 	{"-status",	FTT_T_ARGV_STRING,	NULL,		&estatus_str},
 	{NULL,		FTT_T_ARGV_END,		NULL,		NULL}};

/* parse command line
   ------------------ */

estatus_str = 0; nfm = 1; async = FALSE;
status = ftt_t_parse (&argc, argv, argt);
FTT_T_CHECK_PARSE (status, argt, argv[0]);	/* check parse status */
FTT_T_CHECK_ESTATUS (estatus_str, estatus);	/* check expected status opt */

/* do the skip
   ----------- */

FTT_T_ASYNC (async,ftt_t_fd,ftt_skip_fm(ftt_t_fd,nfm),status);
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
static int	async;				/* async flag */
static char	*estatus_str;			/* expected status string */
ftt_t_argt	argt[] = {
 	{"-async",	FTT_T_ARGV_CONSTANT,	(char *)TRUE,	&async},
 	{"-status",	FTT_T_ARGV_STRING,	NULL,		&estatus_str},
 	{NULL,		FTT_T_ARGV_END,		NULL,		NULL}};

/* parse command line
   ------------------ */

estatus_str = 0; async = FALSE;
status = ftt_t_parse (&argc, argv, argt);
FTT_T_CHECK_PARSE (status, argt, argv[0]);	/* check parse status */
FTT_T_CHECK_ESTATUS (estatus_str, estatus);	/* check expected status opt */

/* do the skip
   ----------- */

FTT_T_ASYNC (async,ftt_t_fd,ftt_skip_to_double_fm(ftt_t_fd),status);
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
static int	async;				/* asych */
static char	*estatus_str;			/* expected status string */
ftt_t_argt	argt[] = {
 	{"-async",	FTT_T_ARGV_CONSTANT,	(char *)TRUE,	&async},
 	{"-status",	FTT_T_ARGV_STRING,	NULL,		&estatus_str},
 	{NULL,		FTT_T_ARGV_END,		NULL,		NULL}};

/* parse command line
   ------------------ */

estatus_str = 0; async = FALSE;
status = ftt_t_parse (&argc, argv, argt);
FTT_T_CHECK_PARSE (status, argt, argv[0]);	/* check parse status */
FTT_T_CHECK_ESTATUS (estatus_str, estatus);	/* check expected status opt */

/* do the rewind
   ------------- */

FTT_T_ASYNC (async,ftt_t_fd,ftt_rewind(ftt_t_fd),status);
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
static int	async;				/* asych flag */
static char	*estatus_str;			/* expected status string */
ftt_t_argt	argt[] = {
 	{"-async",	FTT_T_ARGV_CONSTANT,	(char *)TRUE,	&async},
 	{"-status",	FTT_T_ARGV_STRING,	NULL,		&estatus_str},
 	{NULL,		FTT_T_ARGV_END,		NULL,		NULL}};

/* parse command line
   ------------------ */

estatus_str = 0; async = FALSE;
status = ftt_t_parse (&argc, argv, argt);
FTT_T_CHECK_PARSE (status, argt, argv[0]);	/* check parse status */
FTT_T_CHECK_ESTATUS (estatus_str, estatus);	/* check expected status opt */

/* do the retension
   ---------------- */

FTT_T_ASYNC (async,ftt_t_fd,ftt_retension(ftt_t_fd),status);
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
static int	async;				/* asych flag */
static char	*estatus_str;			/* expected status string */
ftt_t_argt	argt[] = {
 	{"-async",	FTT_T_ARGV_CONSTANT,	(char *)TRUE,	&async},
 	{"-status",	FTT_T_ARGV_STRING,	NULL,		&estatus_str},
 	{NULL,		FTT_T_ARGV_END,		NULL,		NULL}};

/* parse command line
   ------------------ */

estatus_str = 0; async = FALSE;
status = ftt_t_parse (&argc, argv, argt);
FTT_T_CHECK_PARSE (status, argt, argv[0]);	/* check parse status */
FTT_T_CHECK_ESTATUS (estatus_str, estatus);	/* check expected status opt */

/* do the erase
   ------------ */

FTT_T_ASYNC (async,ftt_t_fd,ftt_erase(ftt_t_fd),status);
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
static int	async;				/* asych flag */
static char	*estatus_str;			/* expected status string */
ftt_t_argt	argt[] = {
 	{"-async",	FTT_T_ARGV_CONSTANT,	(char *)TRUE,	&async},
 	{"-status",	FTT_T_ARGV_STRING,	NULL,		&estatus_str},
 	{NULL,		FTT_T_ARGV_END,		NULL,		NULL}};

/* parse command line
   ------------------ */

estatus_str = 0; async = FALSE;
status = ftt_t_parse (&argc, argv, argt);
FTT_T_CHECK_PARSE (status, argt, argv[0]);	/* check parse status */
FTT_T_CHECK_ESTATUS (estatus_str, estatus);	/* check expected status opt */

/* do the unload
   ------------- */

FTT_T_ASYNC (async,ftt_t_fd,ftt_unload(ftt_t_fd),status);
FTT_T_CHECK_CALL (status,estatus);
return 0;

}


/* ============================================================================

ROUTINE: ftt_t_status
 	
	call ftt_status using the global file descriptor
==============================================================================*/
int	ftt_t_status (int argc, char **argv)
{
int 		status;				/* status */
int 		estatus = 0;			/* expected status */
static int	nsec;				/* timeout */
static char	*estatus_str;			/* expected status string */
ftt_t_argt	argt[] = {
 	{"-timeout",	FTT_T_ARGV_INT,		NULL,		&nsec},
 	{"-status",	FTT_T_ARGV_STRING,	NULL,		&estatus_str},
 	{NULL,		FTT_T_ARGV_END,		NULL,		NULL}};

/* parse command line
   ------------------ */

estatus_str = 0; nsec = 0;
status = ftt_t_parse (&argc, argv, argt);
FTT_T_CHECK_PARSE (status, argt, argv[0]);	/* check parse status */
FTT_T_CHECK_ESTATUS (estatus_str, estatus);	/* check expected status opt */

/* do the status 
   ------------- */

status = ftt_status(ftt_t_fd,nsec);
if (status == -1) 
   {
   FTT_T_CHECK_CALL (status,estatus);
   return 0;
   }
fprintf (stderr, 
   "At beginnining of tape:         %s\n",status & FTT_ABOT ? "true" : "false");
fprintf (stderr, 
   "Just after filemark:            %s\n",status & FTT_AFM ? "true" : "false");
fprintf (stderr, 
   "At physical end of tape:        %s\n",status & FTT_AEOT ? "true" : "false");
fprintf (stderr, 
   "At early warning mark near EOT: %s\n",status & FTT_AEW ? "true" : "false");
fprintf (stderr, 
   "Write protected tape:           %s\n",status & FTT_PROT ? "true" :"false");
fprintf (stderr, 
   "Tape loaded and online:         %s\n",status & FTT_ONLINE ?"true":"false");
fprintf (stderr, 
   "Tape busy and not responding:   %s\n",status & FTT_BUSY ? "true" :"false");

return 0;

}


/* ============================================================================

ROUTINE: ftt_t_test_status
 	
==============================================================================*/
int	ftt_t_test_status (int argc, char **argv)
{
int 		status;				/* status */
int 		estatus = 0;			/* expected status */
static int	nsec;				/* timeout */
static char	*estatus_str;			/* expected status string */
static int	abot, afm, aeot, aew, prot, online, busy;
ftt_t_argt	argt[] = {
 	{"-timeout",	FTT_T_ARGV_INT,		NULL,		&nsec},
 	{"-status",	FTT_T_ARGV_STRING,	NULL,		&estatus_str},
 	{"-FTT_ABOT",	FTT_T_ARGV_CONSTANT,	(char *) TRUE,	&abot},
 	{"-FTT_AFM",	FTT_T_ARGV_CONSTANT,	(char *) TRUE,	&afm},
 	{"-FTT_AEOT",	FTT_T_ARGV_CONSTANT,	(char *) TRUE,	&aeot},
 	{"-FTT_AEW",	FTT_T_ARGV_CONSTANT,	(char *) TRUE,	&aew},
 	{"-FTT_PROT",	FTT_T_ARGV_CONSTANT,	(char *) TRUE,	&prot},
 	{"-FTT_ONLINE",	FTT_T_ARGV_CONSTANT,	(char *) TRUE,	&online},
 	{"-FTT_BUSY",	FTT_T_ARGV_CONSTANT,	(char *) TRUE,	&busy},
 	{NULL,		FTT_T_ARGV_END,		NULL,		NULL}};

/* parse command line
   ------------------ */

abot = afm = aeot = aew = prot = online = busy = FALSE;
estatus_str = 0; nsec = 0;
status = ftt_t_parse (&argc, argv, argt);
FTT_T_CHECK_PARSE (status, argt, argv[0]);	/* check parse status */
FTT_T_CHECK_ESTATUS (estatus_str, estatus);	/* check expected status opt */

/* do the status 
   ------------- */

status = ftt_status(ftt_t_fd,nsec);
if (status == -1) 
   {
   FTT_T_CHECK_CALL (status,estatus);
   return 0;
   }
if (abot)
   if (!(status & FTT_ABOT)) 
      fprintf (stderr, "Expected to be at beginning of tape, but was not \n");
if (afm)
   if (!(status & FTT_AFM)) 
      fprintf (stderr, "Expected to be just after filemark, but was not \n");
if (aeot)
   if (!(status & FTT_AEOT)) 
      fprintf (stderr, "Expected to be at end of tape, but was not \n");
if (aew)
   if (!(status & FTT_AEW)) 
      fprintf (stderr, "Expected to be at early warning mark, but was not \n");
if (prot)
   if (!(status & FTT_PROT)) 
      fprintf (stderr, "Expected tape to be write protected, but was not \n");
if (online)
   if (!(status & FTT_ONLINE)) 
      fprintf (stderr, "Expected tape to be online, but was not \n");
if (busy)
   if (!(status & FTT_BUSY)) 
      fprintf (stderr, "Expected tape to be busy, but was not \n");

return 0;

}
/* ============================================================================
 
ROUTINE:
        ftt_t_verify_position
 
        verify the test position
 
==============================================================================*/
int     ftt_t_verify_position (int argc, char **argv)
{
int             status;                 /* status */
int             fileno_act;             /* actual file number */
int             blockno_act;            /* actual block number */
static int      fileno;                 /* file number */
static int      blockno;                /* block number */
ftt_t_argt      argt[] = {
        {"<fileno>",    FTT_T_ARGV_INT,         NULL,           &fileno},
        {"<blockno>",   FTT_T_ARGV_INT,         NULL,           &blockno},
        {NULL,          FTT_T_ARGV_END,         NULL,           NULL}};
 
/* parse command line
   ------------------ */
 
fileno = -1; blockno = -1;
status = ftt_t_parse (&argc, argv, argt);
FTT_T_CHECK_PARSE (status, argt, argv[0]);
 
/* check if the position matches
   ----------------------------- */
 
status = ftt_get_position(ftt_t_fd,&fileno_act,&blockno_act);
FTT_T_CHECK_CALL(status,0);
if (fileno != fileno_act)
   fprintf (stderr,"Error: File number mismatch: Got %d, expected %d\n",
      fileno_act,fileno);
if (blockno != blockno_act)
   fprintf (stderr,"Error: Block number mismatch: Got %d, expected %d\n",
      blockno_act,blockno);
 
return 0;
}
