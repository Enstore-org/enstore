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



void
trace_init( const char	*name );
void
trace(  int		lvl
      , const char	*msg
      , ... );

#define TRACE_GET_MODE		trc_cntl_sp->mode
#define TRACE_SET_MODE( mode )  (trc_cntl_sp->mode = mode)



void
trace_init_trc( void );

extern int			trc_sem_id;
extern pid_t			trc_pid;
extern int			trc_tid;
extern struct s_trc_cntl	*trc_cntl_sp;
extern struct s_trc_ent		*trc_ent_sp;
extern struct sembuf		trc_sem_get_s, trc_sem_put_s;


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

#define TRC_LCK_FIL	"trace.lock"
#define TRC_BUF_FIL	"trace.buffer"
#define TRC_BUF_SZ	0x800000 /* mmap -> shm_open(2)? -> shmget(2) -> SHMMAX */
#define TRC_MAX_MSG	100
#define TRC_MAX_PIDS	200
#define TRC_MAX_PROCS	200
#define TRC_MAX_PARAMS	6
#define TRC_MAX_NAME	12

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
    int		intl_lvl;
    int		last_idx;
    int		head_idx;
    int		tail_idx;
    int		full_flg;
    pid_t	pid_a[TRC_MAX_PIDS];	/* I will not have any way to
					   automatically remove enties. */
    int		lvl_a[ TRC_MAX_PIDS
		      +TRC_MAX_PROCS];	/* Could just have 1 global lvl or
					   one for when pid_a above is
					   filled??? */
				/* LETS USE TIDS, WHICH ARE USER SPECIFIED */
				/* search for pids using getpgid(2) */
    char	t_name_a[TRC_MAX_PROCS][TRC_MAX_NAME+1];/* only if specified
							 - do not know 
					    how to get command (w/o argv) */ 
};

