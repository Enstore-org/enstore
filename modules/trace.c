/*  This file (trace.c) was created by Ron Rechenmacher <ron@fnal.gov> on
    Jul 22, 1998. "TERMS AND CONDITIONS" governing this file are in the README
    or COPYING file. If you do not have such a file, one can be obtained by
    contacting Ron or Fermi Lab in Batavia IL, 60510, phone: 630-840-3000.

    modules/trace.c
    $Revision$

    compile with: cc -g -Wall -c trace.c
    */

#include <stdarg.h>		/* varargs */
#include <stdlib.h>		/* getenv */
#include <unistd.h>		/* mmap, unlink, sleep, lseek, getpid */
#include <sys/mman.h>		/* mmap */
#include <sys/types.h>		/* open, stat, semop */
#include <sys/stat.h>		/* open, stat */
#include <fcntl.h>		/* open */
#include <sys/ipc.h>		/* semop, shmget */
#include <sys/sem.h>		/* semop */
#include <sys/shm.h>		/* shmget */
#include <stdio.h>		/* printf */
#include <string.h>		/* strncpy */
#include <errno.h>		/* errno */

#include "trace.h"

/* THESE COULE BE PASSED IN FROM THE COMPILER */
#ifndef TRC_F0_INITIAL
# define TRC_F0_INITIAL 0xffffffff; /* trace */
#endif
#ifndef TRC_F1_INITIAL
# define TRC_F1_INITIAL 0x00000001; /* i.e. alarm = lvl0 */
#endif
#ifndef TRC_F2_INITIAL
# define TRC_F2_INITIAL 0x0000003f; /* i.e. log   = lvl1-5, plus lvl0 */
#endif
#ifndef TRC_F3_INITIAL
# define TRC_F3_INITIAL 0x00000000; /* i.e. print */
#endif


int			trc_sem_id;
struct s_trc_cntl	*trc_cntl_sp;
struct s_trc_ent	*trc_ent_sp;
struct sembuf		trc_sem_get_s, trc_sem_put_s;
pid_t			trc_pid;
int			trc_tid;
int			*trc_lvl_ip;
void			trc_init_entry(int,const char*); /* fwd decl */
oper_func_t		trc_print; /* forward declaration */
oper_func_t		*trc_op1_fp=trc_print; /* default */
oper_func_t		*trc_op2_fp=trc_print; /* default */
int			trc_super=0;/* "super user" - for no "non-maskable" */
int			trc_lvl_non_maskable[TRC_NUM_OPERATIONS] = {0,1,0x3f};
int			trc_mode_non_maskable = 6;

/******************************************************************************
 *  Main init file called by user code.  Inits the pre-process globals
 *  trc_tid, trc_pid, trc_lvl_ip and trc_op{1,2}_fp (defined above).
 */

void
trace_init(  const char		*name
	   , const char		*key_file
	   , oper_func_t	*op1_fp
	   , oper_func_t	*op2_fp )
{
	int	ii;

    trace_init_trc( key_file );	/* init IPC */

    trc_pid = getpid();

    if (name[0] == '\0')
    {   
	for (ii=0; ii<TRC_MAX_PIDS; ii++)
	{   if (trc_cntl_sp->pid_a[ii] == -1) trc_tid = ii;
	}
	if (ii == TRC_MAX_PIDS)
	{   trc_tid = 0 + TRC_MAX_PIDS; /* FULL - USE THIS IDX */
	}
    }
    else
    {   /* make sure it is not a repeat (note 1st empty) */
	int first_empty;
	first_empty = -1;
	for (ii=0; ii<TRC_MAX_PROCS; ii++)
	{
	    if (strncmp(name,trc_cntl_sp->t_name_a[ii],TRC_MAX_NAME) == 0)
	    {   trc_tid = ii + TRC_MAX_PIDS;
		break;
	    }
	    else if (   (trc_cntl_sp->t_name_a[ii][0]=='\0')
		     && (first_empty==-1))
	    {   first_empty = ii;
	    }
	}
	if (ii == TRC_MAX_PROCS)
	{
	    if (first_empty != -1)
	    {   trc_tid = first_empty + TRC_MAX_PIDS;
	    }
	    else trc_tid = 0 + TRC_MAX_PIDS; /* FULL - USE THIS IDX */
	}
    }
    trc_init_entry( trc_tid, name );
    trc_lvl_ip = trc_cntl_sp->lvl_a[trc_tid];
    if (op1_fp) trc_op1_fp = op1_fp; /* these do get init-ed from - */
    if (op2_fp) trc_op2_fp = op2_fp; /* - trace_init_trc */
}   /* trace_init */


void
trc_init_entry(  int		tid
	       , const char	*name )
{
	int	ii;

    if (tid >= TRC_MAX_PIDS)
    {   /* name is meant to be used (this entry can be used by more than 1
	   process). */
	char *name_cp = trc_cntl_sp->t_name_a[tid-TRC_MAX_PIDS];
	if (!*name_cp)
	{   strncpy( name_cp , name, TRC_MAX_NAME );
	    for (ii=TRC_NUM_OPERATIONS; ii--; )
		trc_cntl_sp->lvl_a[tid][ii] =  trc_cntl_sp->intl_lvl[ii];
	}
    }
    else
    {   trc_cntl_sp->pid_a[tid] = trc_pid; /* this entry is alway
					      associated with this 1
					      process. */
	for (ii=TRC_NUM_OPERATIONS; ii--; )
	    trc_cntl_sp->lvl_a[tid][ii] =  trc_cntl_sp->intl_lvl[ii];
    }
}   /* trc_init_entry */


void
trc_print(  struct timeval	*tp
	  , int			lvl
	  , const char		*msg
	  , va_list		ap )
{
	char 	traceLvlStr[33]="                                ";
	char 	buf[TRC_MAX_MSG+200]; /* abritrary amount of space for
					 (potential) formatting of arguments */
    if (trc_tid >= TRC_MAX_PIDS)
    {   sprintf(  buf
		, "%10d.%06d %5d %" TRC_DEF_TO_STR(TRC_MAX_NAME) "s %s%s\n"
		, (int)tp->tv_sec, (int)tp->tv_usec
		, trc_pid
		, trc_cntl_sp->t_name_a[trc_tid-TRC_MAX_PIDS]
		, &traceLvlStr[31-lvl], msg );
    }
    else
    {   sprintf(  buf
		, "%10d.%06d %5d %" TRC_DEF_TO_STR(TRC_MAX_NAME) "d %s%s\n"
		, (int)tp->tv_sec, (int)tp->tv_usec
		, trc_pid
		, trc_tid
		, &traceLvlStr[31-lvl], msg );
    }
    vprintf( buf, ap );
}


/* Note: shared library problem -- "trace" conflicts with libcurses.so
         "trace" function. This is dependent upon whether or not python
         is configured to use libcurses.
         (I used
          `cd /usr/lib; for ss in *.so;do echo $ss;nm $ss|grep trace;done|less` 
          to verify that there was a conflict with trace)
 */

void
trace_(  int lvl
      , const char	*msg
      , ... )
{
	int		have_time_flg=0;
	struct timeval	tt;
	va_list		ap;

    if ((trc_cntl_sp->mode&1) && (trc_lvl_ip[0]&(1<<lvl)))
    {   /* circular queue put operation */
	va_start( ap, msg );
	TRACE_QPUT( &tt, lvl, msg, ap );
	va_end( ap );
	have_time_flg = 1;
    }
    if ((trc_cntl_sp->mode&2) && (trc_lvl_ip[1]&(1<<lvl)))
    {   if (!have_time_flg) { gettimeofday( &tt, 0 ); have_time_flg = 1; }
	va_start( ap, msg );
	(trc_op1_fp)( &tt, lvl, msg, ap );
	va_end( ap );
    }
    if ((trc_cntl_sp->mode&4) && (trc_lvl_ip[2]&(1<<lvl)))
    {   if (!have_time_flg) { gettimeofday( &tt, 0 ); have_time_flg = 1; }
	va_start( ap, msg );
	(trc_op2_fp)( &tt, lvl, msg, ap );
	va_end( ap );
    }
    if ((trc_cntl_sp->mode&8) && (trc_lvl_ip[3]&(1<<lvl)))
    {   if (!have_time_flg) { gettimeofday( &tt, 0 ); have_time_flg = 1; }
	trc_print( &tt, lvl, msg, ap );
	va_end( ap );
    }
}


/* 
EXAMPLES [start with IRIX man page]
     Input string   Output pointer
     _____________________________
     /usr/lib       lib
     /usr/          ""
     /              ""
*/

char *
trc_basename(  char	*name
	     , char	cc )
{
	int	size;

    size = strlen( name );
    while (size--)
	if (*(name+size) == cc) break;
    return (name+size+1);
}


/******************************************************************************
 * @+Public+@
 * ROUTINE: trace_init_trc:  Added by ron on 27-Jul-1998
 *
 * DESCRIPTION:		set up global vars
 *                      The lock file will be created in the same dir as the
 *                      key file. So, first determine where the key file is or
 *                      where it will be: dir and name.
 *                      Is dir absolute or relative?
 *                      If file does not end with .key, append it!
 *                      If key_file is "", then 
 *                          check env var TRACE_KEY
 *                      else
 *                          use default (def dir is .)
 *                      Check for any file with a .key extenstion???
 *                      Lock file will be key file with .lck substituted for
 *                      .key.
 *                      If key file exists, it's sanity is checked.
 *
 ******************************************************************************/


void
trace_init_trc( const char	*key_file_spec )
{							/* @-Public-@ */
	int			lck_fd, buf_fd;
	int			idx, sts;
	struct sembuf		sem_chk_s;
	struct stat		stat_s;
	char			lck_file[200]; /* arbitrary size */
	char			key_file[200]; /* arbitrary size */
	int			used_env_key_flg=0;

    if (*key_file_spec == '\0') /* check if "" */
    {   /* check env */
        key_file_spec = getenv( "TRACE_KEY" );
	if (key_file_spec == NULL) key_file_spec = ".";
	else                       used_env_key_flg = 1;
    }

    if ((stat(key_file_spec,&stat_s)==0) && S_ISDIR(stat_s.st_mode))
    {   /* working with dir */   
	strcpy( key_file, key_file_spec );
	if (key_file[strlen(key_file)-1] == '/')
	    key_file[strlen(key_file)-1] =  '\0'; /* strip off */
	strcat( key_file, TRC_DFLT_FIL );
    }
    else
    {   /* check dirname */
	char fil[200];	/* arbitrary size for file */
	char *c_p;

	if (key_file_spec[strlen(key_file_spec)-1] == '/')
	{   printf( "error with key file specification; %s: directory does "
		   "not exist\n", key_file_spec );
	    exit( 1 );
	}
	strcpy( key_file, key_file_spec );

	c_p = trc_basename( key_file, '/' );
	strcpy( fil, c_p );	/* save the filename in fil */
	*c_p = '\0';		/* truncate the filename from key_file */
	stat( key_file, &stat_s );
	if (!S_ISDIR(stat_s.st_mode))
	{   printf( "error with key file specification; %s: directory does "
		   "not exist\n", key_file );
	    if (used_env_key_flg)
		printf( "used TRACE_KEY env var.\n" );
	    exit( 1 );
	}
        if (strcmp(trc_basename(fil,'.')-1,TRC_KEY_EXTN) == 0)
	    *(trc_basename(fil,'.')-1) = '\0';
	strcat( key_file, fil );
    }
    strcpy( lck_file, key_file );
    strcat( lck_file, TRC_LCK_EXTN );
    strcat( key_file, TRC_KEY_EXTN );
    /* printf( "key_file is %s\n", key_file ); */

    /*  INITIALIZE -
        - first open lock file to check trace file and possible
	  create/modify */
    trc_sem_get_s.sem_num = 0;  trc_sem_put_s.sem_num = 0; /* 1st and only sem */
    trc_sem_get_s.sem_op  = -1; trc_sem_put_s.sem_op  = 1; /* get's dec, put's inc */
    trc_sem_get_s.sem_flg = 0;  trc_sem_put_s.sem_flg = 0; /* default block behavior */
    sem_chk_s = trc_sem_get_s;  sem_chk_s.sem_num = 1; /* too big, to causes EFBIG below */
    /* AFTER THIS FOR CODE BLOCK,
       trc_sem_id, trc_cntl_sp, and trc_ent_sp will be set correctly */
    for (idx=30; idx--; )
    {   int	r_sts;
	lck_fd = open( lck_file, O_CREAT|O_EXCL ); /* ref. open(2) */
	if (lck_fd != -1)
	{   int		shm_id;

	    /* I have the lock - ref. open(2) */
	    buf_fd = open( key_file, O_RDWR );
	    if (buf_fd == -1)
	    {   /* problem (could be permision???) - try creating */
		buf_fd = open( key_file, O_CREAT|O_RDWR, 0x1b6 );
		if (buf_fd == -1)
		{   perror( "can't create key file" );
		    unlink( lck_file );	/* clean up */
		    exit( 1 );
		}
	    }
	    r_sts = read( buf_fd, &shm_id, sizeof(shm_id) );
	    trc_cntl_sp = shmat( shm_id, 0,0 );/* no adr hint, no flgs */
	    if (   (r_sts!=sizeof(shm_id))
		|| (trc_cntl_sp==(struct s_trc_cntl *)-1))
	    {	int	ii;

		shm_id = shmget(  IPC_PRIVATE, TRC_BUF_SZ
				, IPC_CREAT|0x1ff/*or w/9bit perm*/ );
		if (shm_id == -1)
		{   perror( "shmget" );
		    unlink( lck_file ); /* clean up */
		    exit( 1 );
		}
		trc_cntl_sp = shmat( shm_id, 0,0 );/* no adr hint, no flgs */

		trc_cntl_sp->mode = 0x7;
		trc_cntl_sp->intl_lvl[0] = TRC_F0_INITIAL;
		trc_cntl_sp->intl_lvl[1] = TRC_F1_INITIAL;
		trc_cntl_sp->intl_lvl[2] = TRC_F2_INITIAL;
		trc_cntl_sp->intl_lvl[3] = TRC_F3_INITIAL;
		for (ii=TRC_MAX_PIDS; ii--; )
		    trc_cntl_sp->pid_a[ii] = -1;
		for (ii=TRC_MAX_PROCS; ii--; )
		    trc_cntl_sp->t_name_a[ii][TRC_MAX_NAME]
			= trc_cntl_sp->t_name_a[ii][0] = '\0';
		trc_init_entry( TRC_MAX_PROCS, "PROC_FULL" );

		trc_sem_id = -1;
		trc_cntl_sp->head_idx = trc_cntl_sp->tail_idx = 0;
		trc_cntl_sp->last_idx = (TRC_BUF_SZ-sizeof(*trc_cntl_sp))
		    / sizeof(struct s_trc_ent)
		    - 1; /* last is the last valid */
		trc_cntl_sp->full_flg = 0;

		lseek( buf_fd, 0, SEEK_SET );
		write( buf_fd, &shm_id, sizeof(shm_id) );
	    }

	    /* now chk the semaphore */
	    lseek( buf_fd, sizeof(shm_id), SEEK_SET ); /* index past shm_id */
	    r_sts = read( buf_fd, &trc_sem_id, sizeof(trc_sem_id) );
	    sts = semop( trc_sem_id, &sem_chk_s, 1 );
	    if (   (r_sts!=sizeof(trc_sem_id))
		|| ((sts==-1)&&(errno!=EFBIG)))
	    {   /* assume sem is no longer valid - i.e. reboot */
		trc_sem_id = semget( IPC_PRIVATE, 1, IPC_CREAT|0x1ff );
		/* init the sem (so others can get) */
		semop( trc_sem_id, &trc_sem_put_s, 1 );
		lseek( buf_fd, sizeof(shm_id), SEEK_SET ); /* index past shm_id */
		write( buf_fd, &trc_sem_id, sizeof(trc_sem_id) );
		/* msync(  (void *)trc_cntl_sp, sizeof(*trc_cntl_sp)
		   , MS_ASYNC|MS_INVALIDATE ); is not needed */
	    }

	    trc_ent_sp = (struct s_trc_ent *)(trc_cntl_sp+1);

	    close( buf_fd );
	    close( lck_fd );
	    unlink( lck_file ); /* let others at the buf file */
	    break;
	}
	sleep( 1 );
    }
    if (idx == -1)
    {   printf( "Fatal error initializing trace. You may need to remove the lock file:\n   %s\n",lck_file );
	exit (1);
    }
    return;	
}   /* trace_init_trc */


/***************************************************************************
 ***************************************************************************/

int
traceOnOff( int on, int mask, char *id_s, unsigned lvl1, unsigned lvl2 )
{
	unsigned	id_i, new_msk=0;
	char		*end_p;
	unsigned	old_lvl=0;
	int		ii;

    if (lvl1 > 31) lvl1 = 31;
    if (lvl2 > 31) lvl2 = 31;

    if (lvl1 > lvl2) new_msk = (1<<lvl1) | (1<<lvl2);
    else for (; (lvl1<=lvl2); lvl1++) new_msk |= (1<<lvl1);

    id_i = strtol(id_s,&end_p,10);
    if (end_p != (id_s+strlen(id_s)))	/* check if conversion worked */
    {   /* did not work - id_s must not have a pure number -
	   check for name */
	int	i;

	/* first check special case */
	if (  (strcmp(id_s,"global")==0)
	    ||(strcmp(id_s,"Global")==0)
	    ||(strcmp(id_s,"GLOBAL")==0))
	{
	    for (id_i=(TRC_MAX_PIDS+TRC_MAX_PROCS); id_i--; )
	    {   
		for (ii=TRC_NUM_OPERATIONS; ii--; )
		{   if (!(mask&(1<<ii))) continue;
		    old_lvl = trc_cntl_sp->lvl_a[id_i][ii];
		    if (on)  trc_cntl_sp->lvl_a[id_i][ii] |=  new_msk;
		    else     
		    {        trc_cntl_sp->lvl_a[id_i][ii] &= ~new_msk;
			 if (!trc_super)
			     trc_cntl_sp->lvl_a[id_i][ii] |= trc_lvl_non_maskable[ii];
		    }
		}
	    }
	    return (1);
	}
	if (  (strcmp(id_s,"initial")==0)
	    ||(strcmp(id_s,"Initial")==0)
	    ||(strcmp(id_s,"INITIAL")==0))
	{   
	    for (ii=TRC_NUM_OPERATIONS; ii--; )
	    {   if (!(mask&(1<<ii))) continue;
		old_lvl = trc_cntl_sp->intl_lvl[ii];
		if (on)  trc_cntl_sp->intl_lvl[ii] |=  new_msk;
		else     
		{        trc_cntl_sp->intl_lvl[ii] &= ~new_msk;
		     if (!trc_super)
			 trc_cntl_sp->intl_lvl[ii] |= trc_lvl_non_maskable[ii];
		}
	    }
	    return (1);
	}

	for (i=TRC_MAX_PROCS; i--; )
	{
	    if (strcmp(trc_cntl_sp->t_name_a[i],id_s) == 0)
		break;
	}
	if (i == -1)
	{   printf( "invalid trace proc\n" );
	    return (1);
	}
	id_i = i + TRC_MAX_PIDS;
    }

    /* at this point, either id_s was a number or it was a name that was
       converted to a number */

    if (id_i > (TRC_MAX_PIDS+TRC_MAX_PROCS-1))
	id_i = (TRC_MAX_PIDS+TRC_MAX_PROCS-1);

    for (ii=0; ii<TRC_NUM_OPERATIONS; ii++ )
    {   if (!(mask&(1<<ii))) continue;
	old_lvl = trc_cntl_sp->lvl_a[id_i][ii];
	if (on)  trc_cntl_sp->lvl_a[id_i][ii] |=  new_msk;
	else     
	{        trc_cntl_sp->lvl_a[id_i][ii] &= ~new_msk;
	     if (!trc_super)
		 trc_cntl_sp->lvl_a[id_i][ii] |= trc_lvl_non_maskable[ii];
	}
    }

    return (old_lvl);
}
