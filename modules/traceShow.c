/*  This file (traceShow.c) was created by Ron Rechenmacher <ron@fnal.gov> on
    Jul 24, 1998. "TERMS AND CONDITIONS" governing this file are in the README
    or COPYING file. If you do not have such a file, one can be obtained by
    contacting Ron or Fermi Lab in Batavia IL, 60510, phone: 630-840-3000.
    ./traceShow.c
    $Revision$
    */
/*
compile with: cc -g -Wall -o traceShow traceShow.c


could combine with options???
traceInfo -> traceShow*
traceMode -> traceShow*
traceOff -> traceShow*
traceOn -> traceShow*
traceReset -> traceShow*
traceShow*
*/


#include <stdlib.h>		/* atoi */
#include <string.h>		/* strcat */
#include <stdio.h>		/* printf, fflush */
#include <unistd.h>		/* isatty */
#include <termios.h>		/* ICANON */
#include <sys/ioctl.h>		/* ioctl */

#if defined(__osf__)
#include <sys/termio.h>         /* struct termio */
#endif

#if defined(sun)		/* struct termio -- Sun */
#include <termio.h>
#endif

#include <sys/ipc.h>		/* semop, shmget */
#include <sys/sem.h>		/* semop */

#include	"trace.h"		/* */


int	traceShow( int delta_t, int lines, int incHDR,int incLVL,int incINDT,int optRevr, int ct );
int	traceInfo( int start, int num );
int	traceReset( void );
int	tracePMC( int cntr, int val );
int	traceMode( int mode );

static	char	*version = "Release- $Revision$ $Date$ $Author$";

char	*trc_key_file = "";

#define OPT_ARG( x )	\
do\
{   if(++arg==argc){printf("arg required\n");return(1);}\
    x=argv[arg];\
} while (x==0)/*IRIX warns "controlling expr. is const." if just 0 is used*/

int
main(  int	argc
     , char	**argv )
{
    int	exit_sts;

    exit_sts = 0;

    if      (strcmp(trc_basename(argv[0],'/'),"traceShow") == 0)
    {   int	arg, delta_t=0, lines=0, incHDR=1,incLVL=0,incINDT=1,optRevr=0, optCt=0;
	for (arg=1; (arg<argc)&&(argv[arg][0]=='-'); arg++)
	{        if (strcmp(argv[arg],"-lvl") == 0)      incLVL=1;
	    else if (strcmp(argv[arg],"-nohdr") == 0)    incHDR=0;
	    else if (strcmp(argv[arg],"-r") == 0)        optRevr=1;
	    else if (strcmp(argv[arg],"-nr") == 0)       optRevr=0;
	    else if (strcmp(argv[arg],"-ct") == 0)       optCt=1;
	    else if (strcmp(argv[arg],"-key") == 0)      OPT_ARG(trc_key_file);
	    else if (strcmp(argv[arg],"--version") == 0) { printf( "%s\n", version ); return (0); }
	    else if (strcmp(argv[arg],"-noindent") == 0) incINDT=0;
	    else
	    {   fprintf(  stderr, "usage: %s [options] [delta_time [lines]]\n"
			, trc_basename(argv[0],'/') );
		fprintf(  stderr, "valid options: -nohdr,-lvl,-noindent,-r,-nr,-ct\n" );
		return (1);
	    }
	}
	if ((argc>arg) && (atoi(argv[arg])>=1)) delta_t=1;
	if (argc >arg+1)  if ((lines=atoi(argv[arg+1])) < 0) lines=0;
	trace_init_trc( trc_key_file );
	exit_sts = traceShow( delta_t, lines, incHDR,incLVL,incINDT,optRevr, optCt );
    }
    else if (strcmp(trc_basename(argv[0],'/'),"traceInfo") == 0)
    {   int	arg, start=0, num=TRC_MAX_PIDS+TRC_MAX_PROCS;
	for (arg=1; (arg<argc)&&(argv[arg][0]=='-'); arg++)
	{   if (strcmp(argv[arg],"--version") == 0) { printf( "%s\n", version ); return (0); }
	    else if (strcmp(argv[arg],"-key") == 0)      OPT_ARG(trc_key_file);
	    else
	    {   fprintf(  stderr, "usage: %s [options] [num_procs [start_proc]]\n"
			, trc_basename(argv[0],'/') );
		fprintf(  stderr, "valid option: --version\n" );
		return (1);
	    }
	}
	if (argc-arg >= 2)
	{   sscanf( argv[2],"%d",&num );
	    sscanf( argv[1],"%d",&start );
	}
	else if (argc-arg >= 1) sscanf( argv[1],"%d",&num );
	trace_init_trc( trc_key_file );
	exit_sts = traceInfo( start, num );
    }
    else if (strcmp(trc_basename(argv[0],'/'),"traceReset") == 0)
    {   int	arg;
	for (arg=1; (arg<argc)&&(argv[arg][0]=='-'); arg++)
	{   if (strcmp(argv[arg],"--version") == 0) { printf( "%s\n", version ); return (0); }
	    else if (strcmp(argv[arg],"-key") == 0)      OPT_ARG(trc_key_file);
	    else
	    {   fprintf(  stderr, "usage: %s [options]\n"
			, trc_basename(argv[0],'/') );
		fprintf(  stderr, "valid option: --version\n" );
		return (1);
	    }
	}
	trace_init_trc( trc_key_file );
	exit_sts = traceReset();
    }
    else if (strcmp(trc_basename(argv[0],'/'),"traceMode") == 0)
    {   int	arg, mode, optVerbose=0;
	for (arg=1; (arg<argc)&&(argv[arg][0]=='-'); arg++)
	{   if (strcmp(argv[arg],"--version") == 0) { printf( "%s\n", version ); return (0); }
	    else if (strcmp(argv[arg],"-v")   == 0)   optVerbose=1;
	    else if (strcmp(argv[arg],"-key") == 0)   OPT_ARG(trc_key_file);
	    else if (strcmp(argv[arg],"-super") == 0)    trc_super = 1;
	    else
	    {   fprintf(  stderr, "usage: %s [options] <0-15>\n"
			, trc_basename(argv[0],'/') );
		fprintf(  stderr, "valid option: -v, --version, -key\n" );
		return (1);
	    }
	}
	if ((argc-arg) == 0)
	{   trace_init_trc( trc_key_file );
	    printf( "%d\n", trc_cntl_sp->mode );
	    return (0);
	}
	if (   (argc-arg>1)
	    || (sscanf(argv[arg],"%d",&mode)!=1)
	    || ((mode<0)||(mode>15)))
	{   fprintf( stderr, "usage: %s [-v] <0-15>\n", trc_basename(argv[0],'/') );
	    return (1);
	}
	trace_init_trc( trc_key_file );
	exit_sts = traceMode( mode );
	if (optVerbose)  printf( "old val: %d new val: %d\n", exit_sts, mode );
	else             printf( "%d\n", exit_sts );
	exit_sts = 0;
	trc_super = 0;
    }
    else if (strncmp(trc_basename(argv[0],'/'),"traceO",6) == 0)
    {   unsigned	arg, lvl1, lvl2;
	char            *modes="1";
#       define          O_USAGE "<TIDorNANE> <lvl1> <lvl2>"
	for (arg=1; (arg<argc)&&(argv[arg][0]=='-'); arg++)
	{   if (strcmp(argv[arg],"--version") == 0) { printf( "%s\n", version ); return (0); }
	    else if (strcmp(argv[arg],"-key") == 0)      OPT_ARG(trc_key_file);
	    else if (strcmp(argv[arg],"-modes") == 0)    OPT_ARG(modes);
	    else if (strcmp(argv[arg],"-super") == 0)    trc_super = 1;
	    else
	    {   fprintf(  stderr, "usage: %s [options] " O_USAGE "\n"
			, trc_basename(argv[0],'/') );
		fprintf(  stderr, "valid option: --version, -key, -modes\n" );
		return (1);
	    }
	}
	if (   ((argc-arg)!=3)
	    || (sscanf(argv[2+arg-1],"%d",&lvl1)!=1)
	    || (sscanf(argv[3+arg-1],"%d",&lvl2)!=1))
	{   fprintf( stderr, "usage: %s [options] " O_USAGE "\n"
		    , trc_basename(argv[0],'/') );
	    return (1);
	}
	trace_init_trc( trc_key_file );
	exit_sts = traceOnOff(  (strcmp(trc_basename(argv[0],'/'),"traceOn")==0)?1:0
			      , atoi(modes), argv[1+arg-1], lvl1, lvl2 );
	printf( "old lvl: 0x%08x\n", exit_sts );
	trc_super = 0;
    }

    return (exit_sts);
}   /* main */


/***************************************************************************
 ***************************************************************************/

int	trace_get_press( void );
void	strncatCheck( char *str_buf, const char *msg, int num );

#define STD_OUT         1

int
traceShow( int delta_t, int lines, int incHDR,int incLVL,int incINDT, int optRevr, int ct )
{
	int	head, tmp;
	double  time, time_sav;
        char    str_buf[160+TRC_MAX_MSG], *c_p;
        char    traceLvlStr[33] = "                                ";
        int     line_count=0, c2=0;

    head = trc_cntl_sp->head_idx;
    if ((head==trc_cntl_sp->tail_idx) && !trc_cntl_sp->full_flg)
    {   printf( "empty\n" );
	return (0);
    }

    if (incHDR)
    {   /***/
	if (ct) printf( "      " );
	printf( "         timeStamp " );
	printf( " PID     TIDorName " );
	if (incLVL) printf( "lvl " );
	printf( "                        message                 \n" );
	/*-*/
	if (ct) printf( "------" );
	printf( "-------------------" );
	printf( "-----------------" );
	if (incLVL) printf( "----" );
	printf( "------------------------------------------------\n" );
	/***/
    }

    /* get time of the previous entry now - makes delta processing easier */
    /* look for biggest time */
    time_sav = 0;
    if (optRevr)
    {   head = trc_cntl_sp->tail_idx - 1;
	tmp = head + 1;
	if (tmp == (trc_cntl_sp->last_idx+1)) tmp = 0; /* recall, "last" is an entry */
	time_sav = (double)((trc_ent_sp+tmp)->time.tv_sec)
	    + (double)((trc_ent_sp+tmp)->time.tv_usec)/1000000;
    }
    else
    {   tmp = head - 1;
	if (tmp == -1) tmp = trc_cntl_sp->last_idx; /* recall, "last" is an entry */
	time_sav = (double)((trc_ent_sp+tmp)->time.tv_sec)
	    + (double)((trc_ent_sp+tmp)->time.tv_usec)/1000000;
    }
    do
    {   
	if (optRevr)
	{   head++;	/* head points to a free slot, head-- is where the info is */
	    if (head == (trc_cntl_sp->last_idx+1)) head = 0;;
	}
	else
	{   head--;	/* head points to a free slot, head-- is where the info is */
	    if (head == -1) head = trc_cntl_sp->last_idx;
	}

	time = (double)((trc_ent_sp+head)->time.tv_sec)
	    + (double)((trc_ent_sp+head)->time.tv_usec)/1000000;
	if (delta_t)
	{   
	    if (optRevr)
		time = time - time_sav;
	    else
		time = time_sav - time;
	    time_sav = (double)((trc_ent_sp+head)->time.tv_sec)
		+ (double)((trc_ent_sp+head)->time.tv_usec)/1000000;
	}

	c_p = str_buf;
	if (ct)
	{   c_p += sprintf( c_p, "%18s ", ctime(&(trc_ent_sp+head)->time.tv_sec) );
	    c_p -=2; /* strip off '\n' */
	    *c_p++ = ' ';
	    *c_p = '\0';
	}
	else
	    c_p += sprintf( c_p, "%18.6f ", time );
	c_p += sprintf( c_p, "%5d ", (trc_ent_sp+head)->pid );

	if ((trc_ent_sp+head)->tid >= TRC_MAX_PIDS)
	    c_p += sprintf(  c_p, "%" TRC_DEF_TO_STR(TRC_MAX_NAME) "s "
			   , trc_cntl_sp->t_name_a[ (trc_ent_sp+head)->tid
						   -TRC_MAX_PIDS] );
	else
	    c_p += sprintf(  c_p, "%" TRC_DEF_TO_STR(TRC_MAX_NAME) "d "
			   , (trc_ent_sp+head)->tid );
	if (incLVL) c_p += sprintf( c_p, " %2d ", (trc_ent_sp+head)->lvl );
	if (incINDT) c_p += sprintf( c_p, "%s", &traceLvlStr[32-(trc_ent_sp+head)->lvl] );
	strncatCheck( c_p, (trc_ent_sp+head)->msg_a, TRC_MAX_MSG );
	strcat( str_buf, "\n" );
	printf(  str_buf
	       , (trc_ent_sp+head)->params[0]
	       , (trc_ent_sp+head)->params[1]
	       , (trc_ent_sp+head)->params[2]
	       , (trc_ent_sp+head)->params[3]
	       , (trc_ent_sp+head)->params[4]
	       , (trc_ent_sp+head)->params[5] );

	++c2;
	if (isatty(STD_OUT) && (++line_count==16))
	{   printf("(%d) press q to quit, any other key to continue...",c2);
	    fflush( stdout );
	    if (trace_get_press() == 'q')
	    {   printf( "\n" );
		return (1);
	    }
	    printf( "\n" );
	    line_count=0;
	}
    } while (   (head!=(optRevr?(trc_cntl_sp->head_idx-1):trc_cntl_sp->tail_idx))
	     && (lines? --lines: 1));
    if (incHDR && c2) printf("entries: %d\n",c2);

    return (0);
}   /* traceShow */

int
trace_get_press( void )
{
	struct termio	arg_sav, arg;
	int		a;
	char		c;

    if (ioctl(0,TCGETA,&arg_sav) == -1)
    {   perror("ioctl");
        exit( 0 );
    }

    arg = arg_sav;
    arg.c_lflag &= ~ICANON;
    arg.c_lflag &= ~ECHO;
    arg.c_cc[4] = '\0';
    arg.c_cc[5] = '\0';
    if (ioctl(0,TCSETA,&arg) == -1)
    {   perror("ioctl");
        exit( 0 );
    }

    while (read(0,&c,1) == 0);
    a = (int) c;

    ioctl( 0, TCSETA, &arg_sav );
    return (a);
}   /* trace_get_press */

void
strncatCheck( char *str_buf, const char *msg, int num )
{
    const char	*cp;
    int		params;

    str_buf += strlen( str_buf );	/* go to end of str */

    cp = msg;
    params=0;	/* max params? */

    while ((*cp!='\0') && num--)
    {   if ((params<2) && (*cp=='%') && (*(cp - 1)!='\\'))
	{   if (*(cp+1) == 's')
	    {   
                strcpy( str_buf, "(modified %%s" );
		str_buf += strlen( str_buf );	/* go to end of str */
		*str_buf++ = *cp++;
		*str_buf++ = 'p'; cp++; /* replace/skip past 's' */
		params++;
		continue;
	    }
	    params++;
	}
	*str_buf++ = *cp++;
    }
    *str_buf = '\0';
}   /* strncatCheck */

/***************************************************************************
 ***************************************************************************/

int
traceInfo( int start, int num )
{
	int	i, ii, pid;
	int	begin, end, head, tail;
	int	ents_qued;

    begin = 0;
    end   = trc_cntl_sp->last_idx;
    head  = trc_cntl_sp->head_idx;
    tail  = trc_cntl_sp->tail_idx;

    if (trc_cntl_sp->full_flg)
	ents_qued = end-begin;
    else
    {   /* not full - should just always be head-begin?? */
	if (head < tail)
	    ents_qued = (end-head) + (tail-begin);
	else
	    ents_qued = head - tail;
    }
    printf( "traceCircQueBegin=%8d,  traceCircQueEnd=%8d\n", begin, end );
    printf( " entry size: %d bytes; buffer size: %d entries; max msg size: %d bytes\n", sizeof(struct s_trc_ent), end-begin, TRC_MAX_MSG );
    printf( " traceCircQueHead=%8d, traceCircQueTail=%8d Full=%d\n", head, tail, trc_cntl_sp->full_flg );
    printf( " entries queued: %5d\n", ents_qued );


    /* ADD #define FOR GLOBAL MODE MASK BIT DEFINITIONS */
    printf( "\
  initialLvl: (0x%08x,0x%08x,0x%08x,0x%08x)\n\
trace mode: %d  0=off (b3=print,b2=log,b1=alarm,b0=cirQ)\n"
	   , trc_cntl_sp->intl_lvl[0], trc_cntl_sp->intl_lvl[1]
	   , trc_cntl_sp->intl_lvl[2], trc_cntl_sp->intl_lvl[3]
	   , trc_cntl_sp->mode );

    if (start > (TRC_MAX_PIDS+TRC_MAX_PROCS-1))
	start = TRC_MAX_PIDS+TRC_MAX_PROCS-1;
    if (num)
    {
	printf( "\n\
                                       f1 (alarm)  f2 (log)\n\
(trace)TID    PID/NAME     QPut mask     mask        mask      print mask\n\
----------  ------------   ----------  ----------  ----------  ----------\n");

	for (i=start; (i<(start+num))&&(i<TRC_MAX_PIDS); i++)
	{   pid = trc_cntl_sp->pid_a[i];
	    if (pid != -1)
	    {   printf(  " %4d       %" TRC_DEF_TO_STR(TRC_MAX_NAME) "d   "
		       , i
		       , pid );
		for (ii=0; ii<TRC_NUM_OPERATIONS; ii++)
		{   printf( "  0x%08x", trc_cntl_sp->lvl_a[i][ii] );
		}
		printf( "\n" );
	    }
	}
	for (  i=TRC_MAX_PIDS
	     ; (i<(start+num))&&(i<(TRC_MAX_PIDS+TRC_MAX_PROCS))
	     ; i++)
	{   
	    if (trc_cntl_sp->t_name_a[i-TRC_MAX_PIDS][0])
	    {   printf(  " %4d       %" TRC_DEF_TO_STR(TRC_MAX_NAME) "s   "
		       , i
		       , trc_cntl_sp->t_name_a[i-TRC_MAX_PIDS] );
		for (ii=0; ii<TRC_NUM_OPERATIONS; ii++)
		{   printf( "  0x%08x", trc_cntl_sp->lvl_a[i][ii] );
		}
		printf( "\n" );
	    }
	}
    }

    return (0);
}   /* traceInfo */


/***************************************************************************
 ***************************************************************************/

int
traceReset( void )
{   
    semop( trc_sem_id, &trc_sem_get_s, 1 );
    trc_cntl_sp->tail_idx = trc_cntl_sp->head_idx;
    trc_cntl_sp->full_flg = 0;
    semop( trc_sem_id, &trc_sem_put_s, 1 );

    return (0);
}   /* traceReset */


/***************************************************************************
 ***************************************************************************/

int
traceMode( int mode )
{   
    register int	_r_;

    _r_ = trc_cntl_sp->mode;

    if (!trc_super) mode |= trc_mode_non_maskable;
    trc_cntl_sp->mode = mode;

    return (_r_);
}   /* traceMode */

