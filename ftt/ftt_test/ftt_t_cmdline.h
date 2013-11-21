#ifndef FTTTCMDLINE_HEADER
#define FTTTCMDLINE_HEADER
/*****************************************************************************
Authors:        Margaret Votava
e-mail:         "votava@fnal.gov"
 
Header file for command line editing and entry

Revision history:-
=================
10-Oct-1995 MEV created - stolen from ftcl_CmdLine.h

*/

#ifdef _ULTRIX
#include <sys/types.h>
#endif

#define FTT_MAXLINE	132
typedef char ftt_cmd_t[FTT_MAXLINE];

/*
** COMMAND EDIT STRUCTURE (HANDLE)
*/
typedef struct ftt_cmd_edithndl
   {
   int nextchar;	/* Integer value of entered character */
   int lndx;		/* Index to last byte in current line */
   int cndx;		/* Index to cursor in current line */
   int 	hndx;		/* Index to history list */
   ftt_cmd_t line;	/* Current command line string */
   ftt_cmd_t saveline;	/* Saves current line when line history is used */
   ftt_cmd_t savehdr;	/* Saves line header (prompt) */
   int firstcol;	/* First user enterable column of line (after prompt) */
   int linemodified;    /* Flag indicating when line has been modified (=1) */
   int escmode;		/* Escape sequence mode flag */
   } ftt_cmd_edithndl_t;

#define L_RIGHT 'C'
#define L_LEFT 'D'
#define L_ESC '\033'
#define L_EOF '\004'
#define L_EOL '\005'
#define L_CR '\r'
#define L_LF '\n'
#define L_BS '\b'
#define L_DEL '\177'
#define L_BELL '\007'
#define L_RUB '\025'
#define L_REDRAW '\022'
#define L_FWD '\006'
#define L_BWD '\002'


/*---------------------------------------------------------------------
**
** FUNCTION PROTOTYPES
*/
void ftt_t_linestart(ftt_cmd_edithndl_t *, char *);
int  ftt_t_getchar(ftt_cmd_edithndl_t *);
int  ftt_t_procchar(ftt_cmd_edithndl_t*, char *);

void ftt_t_cmdreset(void);
void ftt_t_interrupt_dec(int);
int  ftt_t_interrupt_chk(void);

#endif /* !FTTTCMDLINE_HEADER */
