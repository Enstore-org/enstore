/*
 * ftcl_ParseArgv.h - Generic ftcl command-line parser. 
 *                    Heavily inspired by the Tk command line parser.
 *
 */

#ifndef _FTT_T_PARSE_H
#define _FTT_T_PARSE_H

#ifndef TRUE
#define TRUE 1
#endif

#ifndef FALSE
#define FALSE 0
#endif
/*
 * Structure to specify how to handle command line options
 */
typedef struct {
    char *key;          /* The key string that flags the option in the
                         * argv array. */
    int type;           /* Indicates option type;  see below. */
    void *src;          /* Value to be used in setting dst;  usage
                         * depends on type. */
    void *dst;          /* Address of value to be modified;  usage
                         * depends on type. */
} ftt_t_argt;

int ftt_t_parse(int *, char **, ftt_t_argt *);
int ftt_t_split(char *, int *, char ***);

void ftt_t_print_usage (ftt_t_argt *argTable, char *cmd_name);
void ftt_t_print_help (ftt_t_argt *argTable, char *cmd_name);
void ftt_t_print_arg (ftt_t_argt *argTable, char *cmd_name);

/* defines for return value of ftcl_ParseArgv */

#define FTT_T_SUCCESS   0 
#define FTT_T_BADSYNTAX	1
#define FTT_T_USAGE	2

/*
 * Legal values for the type field of a ftt_t_argt: see the user
 * documentation for details.
 */
 
#define FTT_T_ARGV_CONSTANT                0x64
#define FTT_T_ARGV_INT                     0x65
#define FTT_T_ARGV_STRING                  0x66
#define FTT_T_ARGV_DOUBLE                  0x68
#define FTT_T_ARGV_END                     0x6B

#define FTT_T_USAGE_BUFSIZE    1025
#define FTT_T_ARGINFO_BUFSIZE  8193

#endif

