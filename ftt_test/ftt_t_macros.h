#ifndef FTTT
#define FTTT
/*****************************************************************************
Authors:        Margaret Votava
e-mail:         "votava@fnal.gov"
 
Header file with useful things for wrapper routines

Revision history:-
=================
6-Nov-1995 MEV created 

*/

#include <string.h>
#include <stdlib.h>
#include <stdio.h>

/* Global variables
   ================ */

extern int              ftt_t_debug;            /* debug flag */
extern char             *ftt_t_basename;        /* basename on cmdline*/
extern ftt_descriptor   ftt_t_fd;   		/* file descriptor */
extern int             	ftt_t_max_error;	/* max number of errs */
extern int              ftt_t_nerror;		/* current error count*/


/* Macros:-
   ======== */


#define FTT_T_INC_NERROR(){					\
ftt_t_nerror++;							\
if (ftt_t_nerror > ftt_t_max_error)				\
   {								\
   fprintf (stderr, "Maximum number of errors (%d) exceeded\n",	\
      ftt_t_max_error);						\
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
      if ((int)(error) != estatus)					\
         {								\
	 fprintf (stderr, "command failed with %s\n",			\
	    ftt_ascii_error[error]); 					\
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
      if ((int)(error) != estatus)					\
         {								\
	 fprintf (stderr, "command failed with %s\n",			\
	    ftt_ascii_error[error]); 					\
 	 fprintf (stderr, "%s\n",errstring);				\
	 FTT_T_INC_NERROR();						\
	 return (error);						\
         }								\
      }									\
   }

#define FTT_T_CHECK_ESTATUS(estring,estatus) {				\
   if (estring)								\
      {									\
      int i;								\
      for (i = 0; ftt_ascii_error[i]; i++)				\
         if (!strcasecmp(ftt_ascii_error[i],estring))			\
	    {								\
	    estatus = i;						\
	    break;							\
	    }								\
      if (!ftt_ascii_error[i])						\
         {								\
         fprintf (stderr,"Invalid error code specified: %s\n",estring);	\
         return 1;							\
         }								\
      }									\
   }

#define FTT_T_ASYNC(async_flag,fd,myroutine,mystatus)	\
   {					\
   if (async_flag)			\
      {					\
      int astatus;			\
      astatus = ftt_fork(fd);		\
      if (astatus < 0)			\
         {				\
         FTT_T_CHECK_CALL(astatus,0);	\
         return 0;			\
         }				\
      else if (astatus == 0)		\
         {				\
         (void) myroutine;		\
         ftt_report(fd);		\
         }				\
      else				\
         {				\
         return 0;			\
         }				\
      }					\
   else					\
      {					\
      mystatus = myroutine;		\
      }					\
   }

#endif
