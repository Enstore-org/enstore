/*  This file (trace.c) was created by Ron Rechenmacher <ron@fnal.gov> on
    Jul 22, 1998. "TERMS AND CONDITIONS" governing this file are in the README
    or COPYING file. If you do not have such a file, one can be obtained by
    contacting Ron or Fermi Lab in Batavia IL, 60510, phone: 630-840-3000.

    modules/trace.c
    $Revision$

    compile with: cc -g -Wall -o trace trace.c
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

int			trc_sem_id;
struct s_trc_cntl	*trc_cntl_sp;
struct s_trc_ent	*trc_ent_sp;
struct sembuf		trc_sem_get_s, trc_sem_put_s;
pid_t			trc_pid;
int			trc_tid;



void
trace_init(  const char	*name
	   , const char	*key_file )
{
	int	ii;

    trace_init_trc( key_file );

    trc_pid = getpid();

    if (name[0] == '\0')
    {   
	for (ii=0; ii<TRC_MAX_PIDS; ii++)
	{
	    if (trc_cntl_sp->pid_a[ii] == -1)
	    {   trc_tid = ii;
		trc_cntl_sp->pid_a[ii] = trc_pid;
	    }
	}
	if (ii == TRC_MAX_PIDS)
	{   trc_tid = 0 + TRC_MAX_PIDS; /* FULL - USE THIS IDX */
	}
    }
    else
    {   /* make sure it is not a repeat (note 1st empty */
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
		strncpy(  trc_cntl_sp->t_name_a[first_empty], name
			, TRC_MAX_NAME );
	    }
	    else
	    {   trc_tid = 0 + TRC_MAX_PIDS; /* FULL - USE THIS IDX */
	    }
	}
    }
    trc_cntl_sp->lvl_a[trc_tid] =  trc_cntl_sp->intl_lvl;
}



void
trace_(  int lvl
      , const char	*msg
      , ... )
{
    if (trc_cntl_sp->lvl_a[trc_tid] & (1<<lvl))
    {   
	int	have_time_flg=0;
	switch (trc_cntl_sp->mode-1)
	{
	    va_list	ap;
	    int		ii, idx;
	case 0:
	    semop( trc_sem_id, &trc_sem_get_s, 1 );
	    idx = trc_cntl_sp->head_idx;
	    if (++trc_cntl_sp->head_idx > trc_cntl_sp->last_idx)
		trc_cntl_sp->head_idx = 0;
	    if      (trc_cntl_sp->full_flg)
		trc_cntl_sp->tail_idx = trc_cntl_sp->head_idx;
	    else if (trc_cntl_sp->head_idx == trc_cntl_sp->tail_idx)
		trc_cntl_sp->full_flg = 1;
	    gettimeofday( &(trc_ent_sp+idx)->time, 0 ); /* to prevent out of order times */
	    have_time_flg = 1;
	    semop( trc_sem_id, &trc_sem_put_s, 1 );
	
	    strncpy( (trc_ent_sp+idx)->msg_a, msg, TRC_MAX_MSG );
	    va_start( ap, msg );
	    for (ii=0; ii<TRC_MAX_PARAMS; ii++)
		(trc_ent_sp+idx)->params[ii] = va_arg(ap,int);
	    va_end( ap );
	    (trc_ent_sp+idx)->pid = trc_pid;
	    (trc_ent_sp+idx)->tid = trc_tid;
	    (trc_ent_sp+idx)->lvl = lvl;
	    break;
	case 2:
	    semop( trc_sem_id, &trc_sem_get_s, 1 );
	    idx = trc_cntl_sp->head_idx;
	    if (++trc_cntl_sp->head_idx > trc_cntl_sp->last_idx)
		trc_cntl_sp->head_idx = 0;
	    if      (trc_cntl_sp->full_flg)
		trc_cntl_sp->tail_idx = trc_cntl_sp->head_idx;
	    else if (trc_cntl_sp->head_idx == trc_cntl_sp->tail_idx)
		trc_cntl_sp->full_flg = 1;
	    gettimeofday( &(trc_ent_sp+idx)->time, 0 ); /* to prevent out of order times */
	    have_time_flg = 1;
	    semop( trc_sem_id, &trc_sem_put_s, 1 );
	
	    strncpy( (trc_ent_sp+idx)->msg_a, msg, TRC_MAX_MSG );
	    va_start( ap, msg );
	    for (ii=0; ii<TRC_MAX_PARAMS; ii++)
		(trc_ent_sp+idx)->params[ii] = va_arg(ap,int);
	    va_end( ap );
	    (trc_ent_sp+idx)->pid = trc_pid;
	    (trc_ent_sp+idx)->tid = trc_tid;
	    (trc_ent_sp+idx)->lvl = lvl;
	case 1:
	    if (trc_cntl_sp->tty_lvl & (1<<lvl))
	    {   char 	buf[TRC_MAX_MSG+200]; /* abritrary amount of space for
						 (potential) formatting of
						 arguments */
		char 	traceLvlStr[33]="                                ";
		struct timeval	tt, *tp;

		if (have_time_flg)
		{   tp = &(trc_ent_sp+idx)->time;
		}
		else
		{   tp = &tt;
		    gettimeofday( tp, 0 );
		}
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
		va_start( ap, msg );
		vprintf( buf, ap );
		va_end( ap );
	    }
	default:
	    break;
	}
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
    {
	if (*(name+size) == cc)
	    break;
    }
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
 *                      
 *
 ******************************************************************************/


void
trace_init_trc( const char *key_file_spec )
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
	if (key_file_spec == NULL)
	    key_file_spec = ".";
	else
	{   used_env_key_flg = 1;
	}
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
    for (idx=40; idx--; )
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

		trc_cntl_sp->mode = 1;
		trc_cntl_sp->tty_lvl  = 0x0000000f;
		trc_cntl_sp->intl_lvl = 0x0000ffff;
		for (ii=TRC_MAX_PIDS; ii--; )
		    trc_cntl_sp->pid_a[ii] = -1;
		for (ii=TRC_MAX_PROCS; ii--; )
		    trc_cntl_sp->t_name_a[ii][TRC_MAX_NAME]
			= trc_cntl_sp->t_name_a[ii][0] = '\0';
		strncpy(  trc_cntl_sp->t_name_a[0], "PROC_FULL_NAME"
			, TRC_MAX_NAME );


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
    {   printf( "fatal error initializing trace\n" );
	exit (1);
    }
    return;	
}   /* trace_init_trc */

