/*  This file (trace.h) was created by Ron Rechenmacher <ron@fnal.gov> on
    Jul 24, 1998. "TERMS AND CONDITIONS" governing this file are in the README
    or COPYING file. If you do not have such a file, one can be obtained by
    contacting Ron or Fermi Lab in Batavia IL, 60510, phone: 630-840-3000.

    ./trace.h
    $Revision$
    */

#include <unistd.h>		/* pid_t, ref. getpid(2) */
#include <sys/types.h>		/* pid_t - unistd alone does not work */
#include <sys/time.h>		/* struct timeval, ref. gettimeofday(2) */
#include <stdarg.h>		/* varargs */
#include <sys/sem.h>		/* semop */

				/* time, lvl, msg, ... */
typedef void	(oper_func_t)( struct timeval*, int, const char*, va_list );

void
trace_init( const char*, const char*, oper_func_t*, oper_func_t* );
void
trace(  int lvl, const char	*msg, ... );
void
trace_init_trc( const char *key_file );
int
traceOnOff( int on, int mask, char *id, unsigned lvl1, unsigned lvl2 );

/* one main use for mode setting is stopping circQPut when an error occurs
   -- this should be done quickly (i.e. no file lookup, etc) */
#define TRACE_MODE		trc_cntl_sp->mode
#define TRACE_MODE_SET( mode )  (trc_cntl_sp->mode = mode)

char *
trc_basename(  char	*name, char	cc );

extern	int			trc_sem_id;
extern	pid_t			trc_pid;
extern	int			trc_tid;
extern	int			*trc_lvl_ip;
extern	struct s_trc_cntl	*trc_cntl_sp;
extern	struct s_trc_ent	*trc_ent_sp;
extern	struct sembuf		trc_sem_get_s, trc_sem_put_s;
extern	int			trc_super;
#define TRC_NUM_OPERATIONS	4 /* circQPut, usrOp1, usrOp2, prnt */
extern	int			trc_lvl_non_maskable[TRC_NUM_OPERATIONS];
extern	int			trc_mode_non_maskable;


/*
The balance between flexibility and efficiency ...
    o   Do not allow pointers to data (i.e. strings, %s)

               PROS                           CONS

if I use var args  ref stdarg(3)
          user can put any number*     uses stack
          of int or double

* up to max 6 int or 3 double

if I use inline
          no stack                     type matching (can not have generic
                                       function for both int and double and
				       variable number of them)

if I use MACRO
         can do some inline stuff      set number of params

USE MACRO W/ VARARGS - can at least do checks inline 
                                       non-standard


*/

#define TRC_DFLT_FIL	"/trace"
#define TRC_LCK_EXTN	".lck"
#define TRC_KEY_EXTN	".key"
#ifdef OSF1
# define TRC_BUF_SZ	0x400000 /* mmap -> shm_open(2)? -> shmget(2) -> SHMMAX */
#else
# define TRC_BUF_SZ	0x800000 /* mmap -> shm_open(2)? -> shmget(2) -> SHMMAX */
#endif
#define TRC_MAX_MSG	100
#define TRC_MAX_PIDS	200
#define TRC_MAX_PROCS	200
#define TRC_MAX_PARAMS	6
#define TRC_MAX_NAME	10
#define TRC_D2S(x)		#x
#define TRC_DEF_TO_STR(x)	TRC_D2S(x)

struct s_trc_ent
{
    pid_t		pid;
    int			tid;
    int			lvl;
    char		msg_a[TRC_MAX_MSG];
    int			params[TRC_MAX_PARAMS];
    struct timeval	time;	/* ref gettimeofday(2), ctime(3) */
};

struct s_trc_cntl
{
    int		mode;
    int		intl_lvl[TRC_NUM_OPERATIONS];
    int		last_idx;
    int		head_idx;
    int		tail_idx;
    int		full_flg;
    pid_t	pid_a[TRC_MAX_PIDS];	/* I will not have any way to
					   automatically remove enties. */
    int		lvl_a[ TRC_MAX_PIDS
		      +TRC_MAX_PROCS][TRC_NUM_OPERATIONS];/* Could just
							     have 1 global
							     lvl set or 
					   a set for when pid_a above is
					   filled??? */
				/* LETS USE TIDS, WHICH ARE USER SPECIFIED */
				/* search for pids using getpgid(2) */
    char	t_name_a[TRC_MAX_PROCS][TRC_MAX_NAME+1];/* only if specified
							 - do not know 
					    how to get command (w/o argv) */ 
};

/* This should be in a trace "system" header file. */
#define TRACE_QPUT( tp, lvl, msg, ap ) \
do\
{       int ii, idx;\
    semop( trc_sem_id, &trc_sem_get_s, 1 );\
    idx = trc_cntl_sp->head_idx;\
    if (++trc_cntl_sp->head_idx > trc_cntl_sp->last_idx)\
	trc_cntl_sp->head_idx = 0;\
    if      (trc_cntl_sp->full_flg)\
	trc_cntl_sp->tail_idx = trc_cntl_sp->head_idx;\
    else if (trc_cntl_sp->head_idx == trc_cntl_sp->tail_idx)\
	trc_cntl_sp->full_flg = 1;\
    gettimeofday( tp, 0 ); /* to prevent out of order times */\
    (trc_ent_sp+idx)->time = *(tp); /* in unlikely event that other modes\
				     take so long that other processes\
				     wrap trace buffer */\
    semop( trc_sem_id, &trc_sem_put_s, 1 );\
\
    strncpy( (trc_ent_sp+idx)->msg_a, msg, TRC_MAX_MSG );\
    for (ii=0; ii<TRC_MAX_PARAMS; ii++)\
	(trc_ent_sp+idx)->params[ii] = va_arg(ap,int);\
    (trc_ent_sp+idx)->pid = trc_pid;\
    (trc_ent_sp+idx)->tid = trc_tid;\
    (trc_ent_sp+idx)->lvl = lvl;\
}\
while (tp==0)/*IRIX warns "controlling expr. is const." if just 0 is used*/
