#ifndef FTTTCMDTABLE
#define FTTTCMDTABLE
/*****************************************************************************
Authors:        Margaret Votava
e-mail:         "votava@fnal.gov"
 
Header file for command line editing and entry

Revision history:-
=================
17-Oct-1995 MEV created 

*/

typedef struct {
   char	*cmdname;
   int	(*func)(int argc, char **argv);
   } ftt_t_cmd_table_t;

#endif

