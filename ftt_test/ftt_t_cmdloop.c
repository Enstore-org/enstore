static char rcsid[] = "@(#)$Id$";
/* 
 * cmdloop --
 *
 *   Interactive command loop, C and Tcl callable.
 *---------------------------------------------------------------------------
 * Copyright 1992 Karl Lehenbauer and Mark Diekhans.
 *
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for any purpose and without fee is hereby granted, provided
 * that the above copyright notice appear in all copies.  Karl Lehenbauer and
 * Mark Diekhans make no representations about the suitability of this
 * software for any purpose.  It is provided "as is" without express or
 * implied warranty.
 *---------------------------------------------------------------------------

Authors:        Margaret Votava
e-mail:         "votava@fnal.gov"
 
Revision history:-
=================
10-Oct-1995 MEV created - stolen from ftcl_CommandLoop
 
*/

/* == This is for cmd editing == */

#ifdef __sgi			/* Problems with ansi switch	*/

#ifndef __EXTENSIONS__
#define __EXTENSIONS__
#endif

#endif

#include <termio.h>
#include <signal.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
 
#include "ftt_t_cmdline.h"
#include "ftt_t_parse.h"
#include "ftt_t_cmdtable.h"

int ftt_t_cmdproc (char *, ftt_t_cmd_table_t *);

#define FTT_MAXCOMMAND		1024

int ftt_t_istty = FALSE;
int ftt_t_debug = 0;			/* debug level */

/*
 *----------------------------------------------------------------------
 *
 * Tcl_CommandLoop --
 *
 *   Run an interactive  
 *
 * Parameters:
 *
 *----------------------------------------------------------------------
 *
 */


void ftt_t_commandloop (char *prompt,ftt_t_cmd_table_t *cmdlist)
{
ftt_cmd_edithndl_t	cmdHandle;		/* command edit handle */
ftt_cmd_t		l_line;			/* single line */
char	   		cmdbuf[FTT_MAXCOMMAND];	/* complete command */
int 			lineEntered;		/* line entered flag */
int        		topLevel = TRUE;	/* top level flag */
int			cmd_length, line_length;/* string lengths */
int			line_number = 0;	/* line number */

#define ftt_reset_to_top() {		\
   lineEntered = FALSE;			\
   cmdbuf[0] = 0;			\
   topLevel = TRUE;			\
   l_line[0] = 0;			\
   }

/* ---------------------------------------------------------------------------*/

ftt_reset_to_top();				/* init var */
ftt_t_interrupt_dec(2);				/* init interrupt handler */
ftt_t_istty = isatty(fileno(stdin));		/* input from tty? */

while(1)					/* Loop forever */
   {
   if (ftt_t_interrupt_chk() )			/* signal came in */
      ftt_reset_to_top();			/* drop any pending command */

   clearerr (stdin);
   clearerr (stdout);

/* Get ready for line entry : if the command is not complete and is split 
   over multiple lines, send a continuation line prompt  
   ---------------------------------------------------------------------- */

   if (topLevel == FALSE) 
      {(ftt_t_linestart(&cmdHandle, "=> ") );}
   if (topLevel == TRUE) 
      {(ftt_t_linestart(&cmdHandle, prompt) );}


/* loop until line entry is complete 
   --------------------------------- */

   line_number++;
   do
      {

       ftt_t_getchar(&cmdHandle);			/* get char from stdin*/
       lineEntered = ftt_t_procchar(&cmdHandle, l_line);/* process char */

       } while (!lineEntered);				/* Complete line   */

   if (lineEntered == -1) break;			/* error! */

/* check if line is too long
   ------------------------- */

   cmd_length = strlen(cmdbuf); line_length = strlen(l_line);
   if ((cmd_length + line_length) > FTT_MAXCOMMAND)
      {
      fprintf (stderr,"ftt_test: Command is too long: %d characters\n",
	 cmd_length + line_length);
      ftt_reset_to_top();
      }

   strcat (cmdbuf,l_line);				/* append line to cmd */

/* check if this is a continuation line
   ------------------------------------ */

   if (l_line[line_length-1] == '\\')			/* continuation */
      {
      cmdbuf[cmd_length+line_length-1] = 0;		/* remove \ */
      topLevel = FALSE;					/* not top level */
      lineEntered = 0;					/* Resetting line */
      l_line[0] = 0;
      continue;  					/* get next line */
      }

   if (ftt_t_debug >= 1)
      fprintf (stderr,"command %d is: %s\n",
	 line_number,cmdbuf);          			/* debug message */
 
   if (ftt_t_cmdproc(cmdbuf,cmdlist) == -1) break;
   ftt_reset_to_top();					/* reset for top level*/

   } /* while(1)... */

}

