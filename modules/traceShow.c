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
#include <sys/ipc.h>		/* semop, shmget */
#include <sys/sem.h>		/* semop */

#include	"trace.h"		/* */


int	traceShow( int delta_t, int lines, int incHDR,int incLVL,int incINDT,int incCPU,int incPMC,int proc );
int	traceInfo( int start, int num );
int	traceReset( void );
int	tracePMC( int cntr, int val );
int	traceMode( int mode );
int	traceOnOff( int on, char *id, unsigned lvl1, unsigned lvl2 );

static	char	*version = "Release- $Revision$ $Date$ $Author$";

char	*trc_key_file = "";

#define OPT_ARG( x )	do{if(++arg==argc){printf("arg required\n");exit(1);} x=argv[arg]; } while (0)

int
main(  int	argc
     , char	**argv )
{
    int	exit_sts;

    exit_sts = 0;

    if      (strcmp(trc_basename(argv[0],'/'),"traceShow") == 0)
    {   int	arg, delta_t=0, lines=0, incHDR=1,incLVL=0,incINDT=1,incCPU=0,incPMC=0,proc=-1;
	for (arg=1; (arg<argc)&&(argv[arg][0]=='-'); arg++)
	{        if (strcmp(argv[arg],"-lvl") == 0)      incLVL=1;
	    else if (strcmp(argv[arg],"-nohdr") == 0)    incHDR=0;
	    else if (strcmp(argv[arg],"-key") == 0)      OPT_ARG(trc_key_file);
	    else if (strcmp(argv[arg],"--version") == 0) { printf( "%s\n", version ); exit (0); }
	    else if (strcmp(argv[arg],"-noindent") == 0) incINDT=0;
	    else
	    {   fprintf(  stderr, "usage: %s [options] [delta_time [lines]]\n"
			, trc_basename(argv[0],'/') );
		fprintf(  stderr, "valid options: nohdr,cpu,pmc,lvl,noindent\n" );
		exit( 1 );
	    }
	}
	if ((argc>arg) && (atoi(argv[arg])>=1)) delta_t=1;
	if (argc >arg+1)  if ((lines=atoi(argv[arg+1])) < 0) lines=0;
	exit_sts = traceShow( delta_t, lines, incHDR,incLVL,incINDT,incCPU,incPMC,proc );
    }
    else if (strcmp(trc_basename(argv[0],'/'),"traceInfo") == 0)
    {   int	arg, start=0, num=0;
	for (arg=1; (arg<argc)&&(argv[arg][0]=='-'); arg++)
	{   if (strcmp(argv[arg],"--version") == 0) { printf( "%s\n", version ); exit (0); }
	    else if (strcmp(argv[arg],"-key") == 0)      OPT_ARG(trc_key_file);
	    else
	    {   fprintf(  stderr, "usage: %s [options] [num_procs [start_proc]]\n"
			, trc_basename(argv[0],'/') );
		fprintf(  stderr, "valid option: --version\n" );
		exit( 1 );
	    }
	}
	if (argc-arg >= 2)
	{   sscanf( argv[2],"%d",&num );
	    sscanf( argv[1],"%d",&start );
	}
	else if (argc-arg >= 1)
	    sscanf( argv[1],"%d",&num );
	exit_sts = traceInfo( start, num );
    }
    else if (strcmp(trc_basename(argv[0],'/'),"traceReset") == 0)
    {   int	arg;
	for (arg=1; (arg<argc)&&(argv[arg][0]=='-'); arg++)
	{   if (strcmp(argv[arg],"--version") == 0) { printf( "%s\n", version ); exit (0); }
	    else if (strcmp(argv[arg],"-key") == 0)      OPT_ARG(trc_key_file);
	    else
	    {   fprintf(  stderr, "usage: %s [options]\n"
			, trc_basename(argv[0],'/') );
		fprintf(  stderr, "valid option: --version\n" );
		exit( 1 );
	    }
	}
	exit_sts = traceReset();
    }
    else if (strcmp(trc_basename(argv[0],'/'),"traceMode") == 0)
    {   int	arg, mode;
	for (arg=1; (arg<argc)&&(argv[arg][0]=='-'); arg++)
	{   if (strcmp(argv[arg],"--version") == 0) { printf( "%s\n", version ); exit (0); }
	    else if (strcmp(argv[arg],"-key") == 0)      OPT_ARG(trc_key_file);
	    else
	    {   fprintf(  stderr, "usage: %s [options] <0-5>\n"
			, trc_basename(argv[0],'/') );
		fprintf(  stderr, "valid option: --version\n" );
		exit( 1 );
	    }
	}
	if (   (argc-arg<1)
	    || (sscanf(argv[1],"%d",&mode)!=1)
	    || ((mode<0)||(mode>5)))
	{   fprintf( stderr, "usage: %s <0-5>\n", trc_basename(argv[0],'/') );
	    exit( 1 );
	}
	exit_sts = traceMode( mode );
    }
    else if (strncmp(trc_basename(argv[0],'/'),"traceO",6) == 0)
    {   unsigned	arg, lvl1, lvl2;
	for (arg=1; (arg<argc)&&(argv[arg][0]=='-'); arg++)
	{   if (strcmp(argv[arg],"--version") == 0) { printf( "%s\n", version ); exit (0); }
	    else if (strcmp(argv[arg],"-key") == 0)      OPT_ARG(trc_key_file);
	    else
	    {   fprintf(  stderr, "usage: %s [options] <0-5>\n"
			, trc_basename(argv[0],'/') );
		fprintf(  stderr, "valid option: --version\n" );
		exit( 1 );
	    }
	}
	if (   (argc-arg<3)
	    || (sscanf(argv[2],"%d",&lvl1)!=1)
	    || (sscanf(argv[3],"%d",&lvl2)!=1))
	{   fprintf( stderr, "usage: %s <TIDorNANE> <lvl1> <lvl2>\n", trc_basename(argv[0],'/') );
	    exit( 1 );
	}
	exit_sts = traceOnOff(  (strcmp(trc_basename(argv[0],'/'),"traceOn")==0)?1:0
			      , argv[1], lvl1, lvl2 );
    }

    exit( exit_sts );
}


/***************************************************************************
 ***************************************************************************/

int	trace_get_press( void );
void	strncatCheck( char *str_buf, const char *msg, int num );

#define STD_OUT         1

int
traceShow( int delta_t, int lines, int incHDR,int incLVL,int incINDT,int incCPU,int incPMC,int proc )
{
	int	head, tmp;
	long	time, time_sav;
        char    str_buf[160+TRC_MAX_MSG], *c_p;
        char    traceLvlStr[33] = "                                ";
        int     line_count=0, c2=0;

    trace_init_trc( trc_key_file );

    head = trc_cntl_sp->head_idx;
    if ((head==trc_cntl_sp->tail_idx) && !trc_cntl_sp->full_flg)
    {   printf( "empty\n" );
	return (0);
    }

    if (incHDR)
    {   /***/
	printf( "           timeStamp " );
	if (incCPU) printf( "CPU " );
	if (incPMC) printf( "               PMC0                 PMC1  " );
	printf( " PID     TIDorName " );
	if (incLVL) printf( "lvl " );
	printf( "                        message                 \n" );
	/*-*/
	printf( "---------------------" );
	if (incCPU) printf( "----" );
	if (incPMC) printf( "------------------------------------------" );
	printf( "-----------------" );
	if (incLVL) printf( "----" );
	printf( "------------------------------------------------\n" );
	/***/
    }

    /* get time of the previous entry now - makes delta processing easier */
    /* look for biggest time */
    time_sav = 0;

    tmp = head - 1;
    if (tmp == -1) tmp = trc_cntl_sp->last_idx; /* recall, "last" is an entry */
    time_sav = (trc_ent_sp+tmp)->time.tv_usec;
	

    do
    {   
	head--;	/* head points to a free slot, head-- is where the info is */
	if (head == -1) head = trc_cntl_sp->last_idx;

	if (delta_t)
	{   time = abs( (trc_ent_sp+head)->time.tv_usec - time_sav );
	    time_sav = (trc_ent_sp+head)->time.tv_usec;
	}
	else
	    time = (trc_ent_sp+head)->time.tv_usec;

	c_p = str_buf;
	c_p += sprintf( c_p, "%20lu ", time );
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
		return(1);
	    }
	    printf( "\n" );
	    line_count=0;
	}
    } while ((head!=trc_cntl_sp->tail_idx) && (lines? --lines: 1));
    if (incHDR && c2) printf("entries: %d\n",c2);

    return (0);
}

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
}

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
	    {   strcpy( str_buf, "(modified \%s)" );
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
}

/***************************************************************************
 ***************************************************************************/

int
traceInfo( int start, int num )
{
	int	i, pid;
	int	begin, end, head, tail;
	int	ents_qued;

    trace_init_trc( trc_key_file );

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


    printf( "\
  initialLvl: 0x%08x TtyLvl: 0x%08x\n\
trace mode: %d  0=off 1=cirQ   2=logMsg 3=cirQ/logMsg 4=USR       5=cirQ/USR\n"
	   , trc_cntl_sp->intl_lvl, 0
	   , trc_cntl_sp->mode );

    if (start > (TRC_MAX_PIDS+TRC_MAX_PROCS-1))
	start = TRC_MAX_PIDS+TRC_MAX_PROCS-1;
    if (num)
    {
	printf( "\n\
(trace)TID    PID/NAME        level\n\
----------  ------------   ----------\n");

	for (i=start; (i<(start+num))&&(i<TRC_MAX_PIDS); i++)
	{   pid = trc_cntl_sp->pid_a[i];
	    if (pid != -1)
		printf(  " %4d       %" TRC_DEF_TO_STR(TRC_MAX_NAME) "d    0x%08x\n"
		       , i
		       , pid
		       , trc_cntl_sp->lvl_a[i] );
	}
	for (  i=TRC_MAX_PIDS
	     ; (i<(start+num))&&(i<(TRC_MAX_PIDS+TRC_MAX_PROCS))
	     ; i++)
	{   
	    if (trc_cntl_sp->t_name_a[i-TRC_MAX_PIDS][0])
	    {   printf(  " %4d       %" TRC_DEF_TO_STR(TRC_MAX_NAME) "s    0x%08x\n"
		       , i
		       , trc_cntl_sp->t_name_a[i-TRC_MAX_PIDS]
		       , trc_cntl_sp->lvl_a[i] );
	    }
	}
    }

    return (0);
}


/***************************************************************************
 ***************************************************************************/

int
traceReset( void )
{   
    trace_init_trc( trc_key_file );

    semop( trc_sem_id, &trc_sem_get_s, 1 );
    trc_cntl_sp->tail_idx = trc_cntl_sp->head_idx;
    trc_cntl_sp->full_flg = 0;
    semop( trc_sem_id, &trc_sem_put_s, 1 );

    return (0);
}


/***************************************************************************
 ***************************************************************************/

int
traceMode( int mode )
{   
    register int	_r_;

    trace_init_trc( trc_key_file );

    _r_ = trc_cntl_sp->mode;

    trc_cntl_sp->mode = mode;
    printf( "old val: %d new val: %d\n", _r_, mode );

    return (_r_);
}


/***************************************************************************
 ***************************************************************************/

int
traceOnOff( int on, char *id_s, unsigned lvl1, unsigned lvl2 )
{
	unsigned	id_i, new_msk=0;
	char		*end_p;
	unsigned	old_lvl;

    trace_init_trc( trc_key_file );

    if (lvl1 > 31) lvl1 = 31;
    if (lvl2 > 31) lvl2 = 31;

    if (lvl1 > lvl2) new_msk = (1<<lvl1) | (1<<lvl2);
    else for (; (lvl1<=lvl2); lvl1++) new_msk |= (1<<lvl1);

    id_i = strtol(id_s,&end_p,10);
    if (end_p != (id_s+strlen(id_s)))	/* check if conversion worked */
    {   /* did not work - id_s must not have a pure number -
	   check for name */
	char	lcl_name[TRC_MAX_NAME+1]; int i;

	/* first check special case */
	if (  (strcmp(id_s,"global")==0)
	    ||(strcmp(id_s,"Global")==0)
	    ||(strcmp(id_s,"GLOBAL")==0))
	{
	    for (id_i=(TRC_MAX_PIDS+TRC_MAX_PROCS); id_i--; )
	    {   old_lvl = trc_cntl_sp->lvl_a[id_i];
		if (on)
		    trc_cntl_sp->lvl_a[id_i] |=  new_msk;
		else
		{   trc_cntl_sp->lvl_a[id_i] &= ~new_msk;
		}
	    }
	    return (1);
	}
	if (  (strcmp(id_s,"initial")==0)
	    ||(strcmp(id_s,"Initial")==0)
	    ||(strcmp(id_s,"INITIAL")==0))
	{   old_lvl = trc_cntl_sp->intl_lvl;
	    if (on)
		trc_cntl_sp->intl_lvl |=  new_msk;
	    else
	    {   trc_cntl_sp->intl_lvl &= ~new_msk;
	    }
	    printf( "old lvl: 0x%08x\n", old_lvl );
	    return (1);
	}
	if (  (strcmp(id_s,"tty")==0)
	    ||(strcmp(id_s,"Tty")==0)
	    ||(strcmp(id_s,"TTy")==0))
	{   old_lvl = trc_cntl_sp->tty_lvl;
	    if (on)
		trc_cntl_sp->tty_lvl |=  new_msk;
	    else
	    {   trc_cntl_sp->tty_lvl &= ~new_msk;
	    }
	    printf( "old lvl: 0x%08x\n", old_lvl );
	    return (1);
	}

	printf( "searching procs\n" );
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
    printf( "id = %d\n", id_i );

    old_lvl = trc_cntl_sp->lvl_a[id_i];
    if (on)
	trc_cntl_sp->lvl_a[id_i] |=  new_msk;
    else
	trc_cntl_sp->lvl_a[id_i] &= ~new_msk;

    printf( "old lvl: 0x%08x\n", old_lvl );
    return (old_lvl);
}
