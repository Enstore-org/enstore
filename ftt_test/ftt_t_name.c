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

#include <sys/types.h>
#include <sys/stat.h>
#include <grp.h>
#include <pwd.h>
#include "ftt.h"
#include "ftt_t_parse.h"
#include "ftt_t_macros.h"

void ftt_t_block_fill   (char *, int, int, int);
int  ftt_t_block_verify (char *, int, int, int);


/* ============================================================================

ROUTINE: ftt_t_get_basename
 	
	call ftt_get_basename
==============================================================================*/
int	ftt_t_get_basename (int argc, char **argv)
{
int 		status;			/* status */
char		*basename;		/* basename */
int		estatus = 1;		/* expected status */
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

basename = ftt_get_basename(ftt_t_fd);                     
FTT_T_CHECK_CALL_OPEN (basename,estatus);

fprintf (stderr, "Basename is %s\n",basename);
return 0;
}

/* ============================================================================

ROUTINE: ftt_t_list_all
 	
	call ftt_list_all
==============================================================================*/
int	ftt_t_list_all(int argc, char **argv)
{
int 		status;			/* status */
char		**allname;		/* allname */
int		i;			/* counter */
int		estatus = 1;		/* expected status */
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

allname = ftt_list_all(ftt_t_fd);                     
FTT_T_CHECK_CALL_OPEN (allname,estatus);

fprintf (stderr,"Static names for this device are: \n");
for (i = 0; allname[i]; i++)
   fprintf (stderr, "   %s\n",allname[i]);
return 0;
}

/* ============================================================================

ROUTINE: ftt_t_chall
 	
	call ftt_chall
==============================================================================*/
int	ftt_t_chall(int argc, char **argv)
{
int 		status;			/* status */
int		estatus = 0;		/* expected status */
char		**allname;		/* all names */
int		i;			/* counter */
int		uid;			/* uid */
int		gid;			/* gid */
struct group	*sgroup;		/* group structure */
struct passwd	*spasswd;		/* password structure */
static char	*uidstr;		/* uid string */
static char	*gidstr;		/* uid string */
static int	mode;			/* mode */
static char	*estatus_str;		/* expected status string */
ftt_t_argt	argt[] = {
 	{"<uid>",	FTT_T_ARGV_STRING,	NULL,		&uidstr},
 	{"<gid>",	FTT_T_ARGV_STRING,	NULL,		&gidstr},
 	{"<mode>",	FTT_T_ARGV_INT,		NULL,		&mode},
 	{"-status",	FTT_T_ARGV_STRING,	NULL,		&estatus_str},
 	{NULL,		FTT_T_ARGV_END,		NULL,		NULL}};

/* parse command line
   ------------------ */

estatus_str = NULL; uidstr = NULL; gidstr = NULL; mode = 0;
status = ftt_t_parse (&argc, argv, argt);
FTT_T_CHECK_PARSE (status, argt, argv[0]);	/* check parse status */
FTT_T_CHECK_ESTATUS (estatus_str, estatus);


/* get the gid value from the string
   --------------------------------- */

sgroup = getgrnam(gidstr);
if (!sgroup) 
   {
   fprintf(stderr,"could not find group %s\n",gidstr);
   FTT_T_INC_NERROR();
   return 0;
   }


/* get the gid value from the string
   --------------------------------- */

spasswd = getpwnam(uidstr);
if (!spasswd) 
   {
   fprintf(stderr,"could not find user %s\n",uidstr);
   FTT_T_INC_NERROR();
   return 0;
   }

/* finally do the chall
   -------------------- */

status = ftt_chall(ftt_t_fd,(int)spasswd->pw_uid,(int)sgroup->gr_gid,mode); 
FTT_T_CHECK_CALL(status ,estatus);
return 0;
}

/* ============================================================================

ROUTINE: ftt_t_avail_mode
 	
	call ftt_avail_mode
==============================================================================*/
int	ftt_t_avail_mode(int argc, char **argv)
{
int 		status;			/* status */
char		*filename;		/* filename */
int		estatus = 1;		/* expected status */
static int	density;		/* density */
static int	mode;			/* mode */
static int	blocksize;		/* blocksize */
static char	*estatus_str;		/* expected status string */
ftt_t_argt	argt[] = {
 	{"<density>",	FTT_T_ARGV_INT,		NULL,		&density},
 	{"<mode>",	FTT_T_ARGV_INT,		NULL,		&mode},
 	{"-blocksize",	FTT_T_ARGV_INT,		NULL,		&blocksize},
 	{"-status",	FTT_T_ARGV_STRING,	NULL,		&estatus_str},
 	{NULL,		FTT_T_ARGV_END,		NULL,		NULL}};

/* parse command line
   ------------------ */

estatus_str = NULL; blocksize = 0;
status = ftt_t_parse (&argc, argv, argt);
FTT_T_CHECK_PARSE (status, argt, argv[0]);	/* check parse status */
FTT_T_CHECK_ESTATUS (estatus_str, estatus);

filename = ftt_avail_mode(ftt_t_fd,density,mode,blocksize);
FTT_T_CHECK_CALL_OPEN (filename,estatus);

fprintf (stderr,"Available device is %s",filename);
return 0;
}

/* ============================================================================

ROUTINE: ftt_t_set_mode
 	
	call ftt_set_mode
==============================================================================*/
int	ftt_t_set_mode(int argc, char **argv)
{
int 		status;			/* status */
int		estatus = 1;		/* expected status */
char		*filename;		/* filename */
static int	density;		/* density */
static int	mode;			/* mode */
static int	blocksize;		/* blocksize */
static char	*estatus_str;		/* expected status string */
ftt_t_argt	argt[] = {
 	{"<density>",	FTT_T_ARGV_INT,		NULL,		&density},
 	{"<mode>",	FTT_T_ARGV_INT,		NULL,		&mode},
 	{"-blocksize",	FTT_T_ARGV_INT,		NULL,		&blocksize},
 	{"-status",	FTT_T_ARGV_STRING,	NULL,		&estatus_str},
 	{NULL,		FTT_T_ARGV_END,		NULL,		NULL}};

/* parse command line
   ------------------ */

estatus_str = NULL; blocksize = 0;
status = ftt_t_parse (&argc, argv, argt);
FTT_T_CHECK_PARSE (status, argt, argv[0]);	/* check parse status */
FTT_T_CHECK_ESTATUS (estatus_str, estatus);

filename = ftt_set_mode(ftt_t_fd,density,mode,blocksize);
FTT_T_CHECK_CALL_OPEN (filename,estatus);
fprintf(stderr,"The device name is %s\n",filename);

return 0;
}

/* ============================================================================

ROUTINE: ftt_t_get_mode
 	
	call ftt_get_mode
==============================================================================*/
int	ftt_t_get_mode(int argc, char **argv)
{
int 		status;			/* status */
char		*filename;		/* filename */
int		estatus = 1;		/* expected status */
int		density;		/* density */
int		mode;			/* mode */
int		blocksize;		/* blocksize */
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

filename = ftt_get_mode(ftt_t_fd,&density,&mode,&blocksize);
FTT_T_CHECK_CALL_OPEN (filename,estatus);
fprintf (stderr,"Device name = %s density = %d mode = %x blocking = %s\n",
   filename,density,mode,blocksize ? "fixed" : "variable");

return 0;
}

/* ============================================================================

ROUTINE: ftt_t_get_mode_dev
 	
	call ftt_get_mode_dev
==============================================================================*/
int	ftt_t_get_mode_dev(int argc, char **argv)
{
int 		status;			/* status */
int		estatus = 0;		/* expected status */
int		density;		/* density */
int		mode;			/* mode */
int		blocksize;		/* blocksize */
int		rewind;			/* rewind flag */
static char	*devname;		/* device name */
static char	*estatus_str;		/* expected status string */
ftt_t_argt	argt[] = {
 	{"<device_name>",FTT_T_ARGV_STRING,	NULL,		&devname},
 	{"-status",	FTT_T_ARGV_STRING,	NULL,		&estatus_str},
 	{NULL,		FTT_T_ARGV_END,		NULL,		NULL}};

/* parse command line
   ------------------ */

estatus_str = NULL; devname = NULL;
status = ftt_t_parse (&argc, argv, argt);
FTT_T_CHECK_PARSE (status, argt, argv[0]);	/* check parse status */
FTT_T_CHECK_ESTATUS (estatus_str, estatus);

status = ftt_get_mode_dev(ftt_t_fd,devname,&density,&mode,&blocksize,&rewind);
FTT_T_CHECK_CALL(status,estatus);
fprintf (stderr,"density = %d mode = %x blocksize = %d rewind = %x\n",
   density, mode, blocksize, rewind);

return 0;
}

/* ============================================================================

ROUTINE: ftt_t_set_mode_dev
 	
	call ftt_set_mode_dev
==============================================================================*/
int	ftt_t_set_mode_dev(int argc, char **argv)
{
int 		status;			/* status */
int		estatus = 0;		/* expected status */
static char	*devname;		/* device name */
static int	force;			/* force flag */
static int	blocksize;		/* block size */
static char	*estatus_str;		/* expected status string */
ftt_t_argt	argt[] = {
 	{"<device_name>",FTT_T_ARGV_STRING,	NULL,		&devname},
 	{"-force",	FTT_T_ARGV_CONSTANT,	(char *)TRUE,	&force},
 	{"-status",	FTT_T_ARGV_STRING,	NULL,		&estatus_str},
 	{NULL,		FTT_T_ARGV_END,		NULL,		NULL}};

/* parse command line
   ------------------ */

estatus_str = NULL; devname = NULL; blocksize = 0; force = FALSE;
status = ftt_t_parse (&argc, argv, argt);
FTT_T_CHECK_PARSE (status, argt, argv[0]);	/* check parse status */
FTT_T_CHECK_ESTATUS (estatus_str, estatus);

status = ftt_set_mode_dev(ftt_t_fd,devname,force);
FTT_T_CHECK_CALL(status,estatus);
return 0;
}

/* ============================================================================

ROUTINE: ftt_t_describe_dev
 	
	call ftt_describe_dev
==============================================================================*/
int	ftt_t_describe_dev(int argc, char **argv)
{
int 		status;			/* status */
int		estatus = 0;		/* expected status */
static char	*devname;		/* device name */
static char	*estatus_str;		/* expected status string */
ftt_t_argt	argt[] = {
 	{"<device_name>",FTT_T_ARGV_STRING,	NULL,		&devname},
 	{"-status",	FTT_T_ARGV_STRING,	NULL,		&estatus_str},
 	{NULL,		FTT_T_ARGV_END,		NULL,		NULL}};

/* parse command line
   ------------------ */

estatus_str = NULL; devname = NULL;
status = ftt_t_parse (&argc, argv, argt);
FTT_T_CHECK_PARSE (status, argt, argv[0]);	/* check parse status */
FTT_T_CHECK_ESTATUS (estatus_str, estatus);

status = ftt_describe_dev(ftt_t_fd,devname,stderr);
FTT_T_CHECK_CALL(status,estatus);
return 0;
}

/* ============================================================================

ROUTINE: ftt_t_verify_exist
 	
==============================================================================*/
int	ftt_t_verify_exist(int argc, char **argv)
{
int 		status;			/* status */
char		**allname;		/* all names */
char		*aname;			/* single name */
int		i;			/* counter */
struct stat	buf;			/* stat buf */

allname = ftt_list_all(ftt_t_fd);
for (i = 0; allname[i]; i++)
   {
   aname = allname[i];
   status = stat(aname,&buf);
   if (status < 0) perror(aname);
   }
return 0;
}

/* ============================================================================
 
ROUTINE:
        ftt_t_verify_mode
 
        tests all available modes for the device
 
==============================================================================*/
int     ftt_t_verify_modes (int argc, char **argv)
{
int             status;                                 /* status */
int             blocksize[] = {0,1024,2048,-1};		/* block size array */
int             mode[] =      {0,1,-1};			/* mode array */
int             density;                                /* density */
int             i,j,k;                                  /* counter */
int             bsize = 4096;                           /* block size */
char            dataptr[4096];                          /* data buffer */
char		*filename;				/* filename */
int nblock      = 100;                                  /* nblocks */
int fileno;                                             /* file number */
int blockno;                                            /* block number */
ftt_stat_buf	statbuf;				/* status buffer */
char		*statval_str;				/* stat value string */
int		statval;				/* stat value */
char		*hwdens_str;				/* hwdens value */
 
/* get the status buffer
   --------------------- */

statbuf = ftt_alloc_stat();
if (!statbuf)
   {
   char *errstring;
   errstring = ftt_get_error(&status);
   fprintf (stderr,"Could not allocate stat buffer %s\n",errstring);
   fprintf (stderr, "%s\n",errstring);
   FTT_T_INC_NERROR();
   return 0;
   }

for (density = 0; density < 10; density++)              /* all densities */
   {
   for (j = 0; mode[j] >= 0; j++)                        /* all compressions */
      {
      for (i = 0; blocksize[i] >= 0; i++)		/* fixed + var block */
         {

	/* if the mode is available, let's set it and go!
	   ---------------------------------------------- */

         if (!ftt_avail_mode(ftt_t_fd,density,mode[j],blocksize[i])) continue;
         status = ftt_rewind (ftt_t_fd);
         FTT_T_CHECK_CALL(status,0);
         filename = ftt_set_mode(ftt_t_fd,density,mode[j],blocksize[i]);
         FTT_T_CHECK_CALL_OPEN (filename,1);

	/* write data
	   ---------- */

	 fprintf(stderr,"Verify %s with density: %d, mode %d, blocksize %d\n",
	      filename,density,mode[j],blocksize[i]);
         for (k = 0; k < nblock; k++)                        /* write data*/
            {
            status = ftt_get_position(ftt_t_fd, &fileno, &blockno);
            FTT_T_CHECK_CALL (status,0);
            ftt_t_block_fill (dataptr, bsize, fileno, blockno);
            status = ftt_write (ftt_t_fd, dataptr, bsize);
            FTT_T_CHECK_CALL (status,0);
            }

	/* verify mode
	   ----------- */

	 status = ftt_get_stats (ftt_t_fd,statbuf);		/* verify mode*/
         FTT_T_CHECK_CALL (status,0);
	 statval_str = ftt_extract_stats (statbuf,FTT_TRANS_COMPRESS);
 	 statval = -1;
	 if (statval_str) statval = atoi(statval_str);
	 if (statval != mode[j])
	    {
	    fprintf (stderr,"Modes don't match: Expected %d Got: %d\n",
		  mode[j],statval);
	    /* return 0; */
	    }
	 statval_str = ftt_extract_stats (statbuf,FTT_TRANS_DENSITY);
 	 statval = -1;
	 if (statval_str) statval = atoi(statval_str);
	 if (statval != density)
	    {
	    hwdens_str = ftt_extract_stats (statbuf, FTT_DENSITY);
	    fprintf (stderr,"Densities don't match: Expected %d Got: %d(%s)\n",
		  density,statval,hwdens_str);
	    /* return 0; */
	    }
	 statval_str = ftt_extract_stats (statbuf,FTT_BLOCK_SIZE);
 	 statval = -1;
	 if (statval_str) statval = atoi(statval_str);
	 if (statval != blocksize[i])
	    {
	    fprintf (stderr,"Blocksizes don't match: Expected %d Got: %d\n",
		  blocksize[i],statval);
	    /* return 0; */
	    }

	/* verify data
	   ----------- */

         status = ftt_rewind (ftt_t_fd);
         FTT_T_CHECK_CALL (status,0);
         for (k = 0; k < nblock; k++)                        
            {
            status = ftt_get_position(ftt_t_fd, &fileno, &blockno);
            FTT_T_CHECK_CALL (status,0);
            memset(dataptr,0,bsize);
            status = ftt_read (ftt_t_fd, dataptr, bsize);
            FTT_T_CHECK_CALL (status,0);
            status = ftt_t_block_verify (dataptr, bsize, fileno, blockno);
            FTT_T_CHECK_CALL (status,0);
            }
         }
      }
   }
 
return 0;
}


/* ============================================================================
 
ROUTINE: ftt_t_list_supported
 
        call ftt_list_supported
==============================================================================*/
int     ftt_t_list_supported (int argc, char **argv)
{
FILE            *outfile = stderr;	/* file to output to */
static char     *out_filename;                  /* output file name */
int		status;

ftt_t_argt      argt[] = {
        {"-filename",   FTT_T_ARGV_STRING,      NULL,           &out_filename},
        {NULL,          FTT_T_ARGV_END,         NULL,           NULL}};
 
/* parse command line
   ------------------ */
 
out_filename = NULL;
status = ftt_t_parse (&argc, argv, argt);
FTT_T_CHECK_PARSE (status, argt, argv[0]);      /* check parse status */

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
 
ftt_list_supported (outfile);
if (outfile != stderr) close(outfile);
return 0;
}

