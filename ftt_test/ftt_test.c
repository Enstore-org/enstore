static char rcsid[] = "@(#)$Id$";
/* Ftt main test routine

Authors:        Margaret Votava
e-mail:         "votava@fnal.gov"
 
Revision history:-
=================
10-Oct-1995 MEV created 
 
Include files:-
===============
*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "ftt_t_cmdtable.h"


/* Globals:-
   ========= */

char	*ftt_t_basename=NULL;	/* basename entered on cmdline */

/* Prototypes:-
   ============ */

void ftt_t_commandloop (char *prompt, ftt_t_cmd_table_t *);

/* function prototypes for test routines 
   ------------------------------------- */
int ftt_t_all_scsi(int, char **);
int ftt_t_clear_unrecovered(int, char **);
int ftt_t_date(int, char **);		int ftt_t_echo(int, char **);
int ftt_t_debug_level(int, char **);
int ftt_t_get_error(int, char **); 	int ftt_t_eprintf(int, char **);
int ftt_t_get_position(int, char **);	int ftt_t_get_stats(int, char **);	
int ftt_t_extract_stats(int, char **);	int ftt_t_dump_stats(int, char **);
int ftt_t_free_stat(int, char **);	int ftt_t_init_stats(int, char **);	
int ftt_t_update_stats(int, char **);	int ftt_t_open(int, char **);		
int ftt_t_open_logical(int, char **); 	int ftt_t_open_dev(int, char **);
int ftt_t_close(int, char **); 		int ftt_t_close_dev(int, char **);
int ftt_t_wait(int, char **);		int ftt_t_check(int, char **);
int ftt_t_skip_rec(int, char **); 	int ftt_t_skip_fm(int, char **);
int ftt_t_skip_to_double_fm(int, char **); int ftt_t_rewind(int, char **);
int ftt_t_retension(int, char **); 	int ftt_t_erase(int, char **);
int ftt_t_unload(int, char **); 	int ftt_t_writefm(int, char **);
int ftt_t_write2fm(int, char **);
int ftt_t_write_tblock(int, char **); 	int ftt_t_verify_tblock(int, char **);
int ftt_t_dump(int, char **);         	int ftt_t_undump(int, char **);
int ftt_t_get_basename(int, char **); 	int ftt_t_list_all(int, char **);
int ftt_t_chall(int, char **);        	int ftt_t_avail_mode(int, char **);
int ftt_t_set_mode(int, char **);	int ftt_t_get_mode(int, char **);
int ftt_t_set_mode_dev(int, char **);	int ftt_t_get_mode_dev(int, char **);
int ftt_t_describe_dev(int, char **);  int ftt_t_verify_vol_label(int, char **);
int ftt_t_write_vol_label(int, char **);int ftt_t_status(int, char **);
int ftt_t_verify_position(int, char **);int ftt_t_max_errors(int, char **);
int ftt_t_verify_modes(int, char **);	int ftt_t_verify_exist(int, char **);
int ftt_t_test_status(int, char **);	int ftt_t_list_supported(int, char**);

/*=============================================================================
Routine:
   	main program for ftt_test. It's a pretty simply program. just call 
	the command loop with the prompt and list of valid commands.
==============================================================================*/

int main(int argc, char **argv)
{

int opt;

/* build the list of commands that we support and their corresponding
   function pointers. 
   ------------------------------------------------------------------ */
ftt_t_cmd_table_t ftt_t_my_cmds[] = {
	"ftt_all_scsi",		ftt_t_all_scsi,
	"ftt_date",		ftt_t_date,
	"ftt_echo", 		ftt_t_echo,
	"ftt_debug",		ftt_t_debug_level,
	"ftt_get_error",	ftt_t_get_error,
	"ftt_eprintf",		ftt_t_eprintf,
	"ftt_get_position",	ftt_t_get_position,
	"ftt_open",		ftt_t_open,
	"ftt_open_logical",	ftt_t_open_logical,
	"ftt_open_dev",		ftt_t_open_dev,
	"ftt_close",		ftt_t_close,
	"ftt_close_dev",	ftt_t_close_dev,
	"ftt_wait",		ftt_t_wait,
	"ftt_check",		ftt_t_check,
	"ftt_skip_rec",		ftt_t_skip_rec,
	"ftt_skip_fm",		ftt_t_skip_fm,
	"ftt_skip_to_double_fm",ftt_t_skip_to_double_fm,
	"ftt_rewind",		ftt_t_rewind,
	"ftt_retension",	ftt_t_retension,
	"ftt_erase",		ftt_t_erase,
	"ftt_unload",		ftt_t_unload,
	"ftt_write2fm",		ftt_t_write2fm,
	"ftt_writefm",		ftt_t_writefm,
	"ftt_write_tblock",	ftt_t_write_tblock,
	"ftt_verify_tblock",	ftt_t_verify_tblock,
	"ftt_dump",		ftt_t_dump,
	"ftt_undump",		ftt_t_undump,
	"ftt_get_basename",	ftt_t_get_basename,
	"ftt_list_all",		ftt_t_list_all,
	"ftt_chall",		ftt_t_chall,
	"ftt_avail_mode",	ftt_t_avail_mode,
	"ftt_set_mode",		ftt_t_set_mode,
	"ftt_get_mode",		ftt_t_get_mode,
	"ftt_get_mode_dev",	ftt_t_get_mode_dev,
	"ftt_set_mode_dev",	ftt_t_set_mode_dev,
	"ftt_describe_dev",	ftt_t_describe_dev,
	"ftt_get_stats",	ftt_t_get_stats,
	"ftt_extract_stats",	ftt_t_extract_stats,
	"ftt_dump_stats",	ftt_t_dump_stats,
	"ftt_free_stat",	ftt_t_free_stat,
	"ftt_init_stats",	ftt_t_init_stats,
	"ftt_update_stats",	ftt_t_update_stats,
	"ftt_verify_vol_label",	ftt_t_verify_vol_label,
	"ftt_write_vol_label",	ftt_t_write_vol_label,
	"ftt_status",		ftt_t_status,
	"ftt_verify_position",	ftt_t_verify_position,
	"ftt_max_error",	ftt_t_max_errors,
	"ftt_verify_modes",	ftt_t_verify_modes,
	"ftt_verify_exist",	ftt_t_verify_exist,
	"ftt_test_status",	ftt_t_test_status,
	"ftt_list_supported",	ftt_t_list_supported,
	NULL,			0};

/* Get command line arguments
   -------------------------- */
 
while ((opt = getopt(argc,argv,"f:")) != -1)
   {
   switch (opt) {
      case 'f':
         {
         int length;
         ftt_t_basename = (char *) malloc ((strlen(optarg) + 1));
         strcpy(ftt_t_basename,optarg);
         break;
         }
      case '?':
      usage:
         printf( "Usage: ftt_test -f <basename>\n");
         exit(1);
      }
   }
 

ftt_t_commandloop ("ftt_test> ", ftt_t_my_cmds);
exit(0);
}

