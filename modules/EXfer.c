/*  This file (EXfer.c) was created by Ron Rechenmacher <ron@fnal.gov> on
    Apr 30, 1998. "TERMS AND CONDITIONS" governing this file are in the README
    or COPYING file. If you do not have such a file, one can be obtained by
    contacting Fermi Lab in Batavia IL, 60510, phone: 630-840-3000.
    */

#ifdef NO_READ
# include <sys/stat.h>		/* fstat, struct stat */
#endif
#include <sys/types.h>		/* read/write */
#include <sys/wait.h>		/* waitpid */
#include <stdio.h>		/* fread/fwrite */
#include <unistd.h>		/* read/write, fork, nice, close */
#include <sys/ipc.h>		/* shmxxx */
#include <sys/shm.h>		/* shmxxx */
#include <sys/sem.h>		/* semxxx */
#include <sys/msg.h>		/* msg{snd,rcv} */
#include <Python.h>		/* all the Py.... stuff */
#include <assert.h>             /* assert */
#include <errno.h>
#include <signal.h>		/* sigaction() and struct sigaction */
#if 1
# include <ftt.h>		/* ftt_read/write */
#else
/* from ftt_lib/ftt_private.h */
# include <../ftt_lib/ftt_defines.h>
# include <../ftt_lib/ftt_macros.h>
# include <../ftt_lib/ftt_scsi.h>
# include <../ftt_lib/ftt_types.h>
# include <../ftt_lib/ftt_common.h>
# define _FTT_H	/* trick ftt.h (nested from ETape.h) into thinking it has */
		/* already been included */
#endif
#include "ETape.h"		/* note potential #def _FTT_H trick above */

#define CKALLOC(malloc_call) if ( !(malloc_call) ) {PyErr_NoMemory(); return NULL;} 

/*
 *  Module description:
 *      Two methods:
 *	    1)  (dat_byts,dat_CRC,san_CRC) = obj.to_HSM(  frmDriverObj, to_DriverObj
 *						         , crc_fun, sanity_byts, header )
 *	    2)  (dataCRC,sanitySts) = obj.frmHSM(  frmDriverObj, to_DriverObj
 *						 , dataSz, sanitySz, sanityCRC )
 */
static char EXfer_Doc[] =  "EXfer is a module which Xfers data";

enum e_mtype
{   WrtSiz = 1			/* mtype must be non-zero */
  , SanCrc
  , DatCrc
  , DatByt
  , Err
  , Eof
};
struct s_msgdat
{   int		data;
    char	*c_p;
};
struct s_msg
{   enum e_mtype	mtype;	/* see man msgsnd or msgop(2) */
    struct s_msgdat	md;
};


/******************************************************************************
 * @+Public+@
 * ROUTINE: raise_exception:  Added by ron on 01-May-1998
 *
 * DESCRIPTION:		An error reporter which produces an error string and
 *			 raises an exception for python.
 *
 ******************************************************************************/

static PyObject *EXErrObject;


static PyObject *
raise_exception( char *msg )
{
	char		buf[200];
        PyObject	*v;
        int		i = errno;

#   ifdef EINTR
    if ((i==EINTR) && PyErr_CheckSignals()) return NULL;
#   endif

    sprintf( buf, "%s - %s", msg, strerror(i) );
    v = Py_BuildValue( "(is)", i, buf );
    if (v != NULL)
    {   PyErr_SetObject( EXErrObject, v );
	Py_DECREF(v);
    }
    return NULL;
}


/******************************************************************************
 * @+Public+@
 * ROUTINE: EXto_HSM:  Added by ron on 01-May-1998
 *
 * DESCRIPTION:		read from socket, write to HSM
 *
 * RETURN VALUES:	Tuple - (sanity_crc, total_crc).
 *
 ******************************************************************************/

static char EXto_HSM_Doc[] = "Xfers the from user to HSM (and crc)";

#ifdef OSF1
union semun {
  int val;
  struct semid_ds *buf;
  ushort_t *array;
};
#endif

static PyObject *
EXto_HSM(  PyObject	*self
	 , PyObject	*args )
{							/* @-Public-@ */
	int		idx;
	/* MAKE AN ENUM??? */
#	define		Frm	0	/* EXto_HSM Arg1 */
#	define		To_	1	/* EXto_HSM Arg2 */
#	define		Crc	2	/* EXto_HSM Arg3 */
	PyObject	*obj_pa[3];
	PyObject	*attrObj1_p, *attrObj2_p;
	int		sanity_byts;	/* EXto_HSM Arg 4*/
	int		san_crc=0, dat_crc=0, dat_byts=0;
	long		bytes_xferred[3];		/* default locations */
	long		*bytes_xferred_p[3];
	char		*str;
	int		fd_a[2]={0,0};
	FILE		*fp_a[2]={NULL,NULL};
	int		(*(frmFunc_p)[2])()={NULL,NULL};
	int		(*(to_Func_p)[2])()={NULL,NULL};
	char		*dat_buf;
	int		dat_buf_byts;
	/**/		/**/
	int		inc_size=512;   /* default for now -- override below */
	int		shmid, semid, msgqid;
	int		sts;
	char		*shmaddr;
	union semun	semun_u;
	struct sembuf	sops_wr_wr2rd;  /* allows read */
	struct sembuf	sops_rd_wr2rd;
	struct msqid_ds	msgctl_s;
	struct s_msg	msgbuf_s;	/* for reader and writer */
	int		ahead_idx = 0;
	int		writing_flg=1;
	int             rd_ahead=50;    /* arbitrary default */
	pid_t		pid;


    /*  Parse the arguments */
    PyArg_ParseTuple(  args, "OOOls#", &obj_pa[0], &obj_pa[1], &obj_pa[2]
		     , &sanity_byts, &dat_buf, &dat_buf_byts );

    for (idx=0; idx<2; idx++)
    {
	attrObj1_p = PyObject_GetAttrString( obj_pa[idx], "__class__" );
	if (attrObj1_p == 0)
	{   printf( "EXfer - invalid argument\n" );
	    return (Py_BuildValue("(iii)",0,0,0));
	}
	attrObj2_p = PyObject_GetAttrString( attrObj1_p, "__name__" );
	Py_DECREF( attrObj1_p );
	str       = PyString_AsString( attrObj2_p );
	Py_DECREF( attrObj2_p );
	if      (strcmp(str,"Mover") == 0)
	{   attrObj1_p = PyObject_GetAttrString( obj_pa[idx], "data_socket" );
	    if (attrObj1_p == 0)
	    {   printf( "EXfer - invalid argument\n" );
		return (Py_BuildValue("(iii)",0,0,0));
	    }
	    attrObj2_p = PyObject_CallMethod( attrObj1_p, "fileno", "" );
	    Py_DECREF( attrObj1_p );
	    fd_a[idx] = PyInt_AsLong(attrObj2_p);
	    if (idx == Frm)
		frmFunc_p[idx] = read;
	    else
		to_Func_p[idx] = write;
	    bytes_xferred_p[idx] = &bytes_xferred[idx]; bytes_xferred[idx] = 0;
	    /*printf( "EXfer p%d class is Mover, fd=%d\n", idx+1, fd_a[idx] );*/
	}
	else if (strcmp(str,"FTTDriver") == 0)
	{   attrObj1_p = PyObject_GetAttrString( obj_pa[idx], "ETdesc" );
	    /* struct s_ETdesc is kept at int member */
	    fd_a[idx] = PyInt_AsLong(attrObj1_p);
	    Py_DECREF( attrObj1_p );
	    if (idx == Frm)
		frmFunc_p[idx] = ftt_read;
	    else
		to_Func_p[idx] = ftt_write;
	    /*printf(  "EXfer p%d class is FTTDriver, devname=%s, block_size=%d\n", idx+1
		   , *(char **)((struct s_ETdesc *)fd_a[idx])->ftt_desc
		   , ((struct s_ETdesc *)fd_a[idx])->block_size );*/
	    if (idx == To_)  inc_size = ((struct s_ETdesc *)fd_a[idx])->block_size;
	    bytes_xferred_p[idx] = &((struct s_ETdesc *)fd_a[idx])->bytes_xferred;
	    fd_a[idx] = (int)((struct s_ETdesc *)fd_a[idx])->ftt_desc;
	}
	else if (strcmp(str,"RawDiskDriver") == 0 ||  strcmp(str,"DelayDriver") == 0 )
	{   attrObj1_p = PyObject_GetAttrString( obj_pa[idx], "df" );
	    if (attrObj1_p == 0)
	    {   printf( "EXfer - invalid argument\n" );
		return (Py_BuildValue("(iii)",0,0,0));
	    }
	    fp_a[idx] = PyFile_AsFile( attrObj1_p );
	    Py_DECREF( attrObj1_p );
	    if (idx == Frm)
		frmFunc_p[idx] = (void(*))fread;
	    else
		to_Func_p[idx] = (void(*))fwrite;
	    bytes_xferred_p[idx] = &bytes_xferred[idx]; bytes_xferred[idx] = 0;
	    /*printf( "EXfer p%d class is RawDiskDriver, fp=%p\n", idx+1, fp_a[idx] );*/
	}
	else
	{
	    printf( "EXfer p%d class is Unknown\n", idx+1 );
	}
    }

    /*printf( "EXfer sanity_byts=%d\n", sanity_byts );*/

    /* create private (to be inherited by child) shm seg */
    /* there does not seem to be a 4M size limitation */
    /* try 10x buffering */
    assert( inc_size < 0x400000 );
    rd_ahead = 0x400000 / inc_size;
    shmid = shmget( IPC_PRIVATE, inc_size*rd_ahead, IPC_CREAT|0x1ff/*or w/9bit perm*/ );
    /*printf( "EXfer to_HSM shmid=%d (size=inc*%d=%d bytes)\n", shmid, rd_ahead, inc_size*rd_ahead );*/
    shmaddr = shmat( shmid, 0, 0 );	/* no addr hint, no flags */
    /*printf( "EXfer shmaddr=%p\n", shmaddr );*/
    if (shmaddr == (char *)-1)
	perror( "shmat" );

    /* create msg Q for reader to send info to writer */
    msgqid = msgget( IPC_PRIVATE, IPC_CREAT|0x1ff );
    msgbuf_s.mtype = WrtSiz;
    msgctl( msgqid, IPC_STAT, &msgctl_s );
    msgctl_s.msg_qbytes = (rd_ahead+1) * sizeof(msgbuf_s.md.data);
    msgctl( msgqid, IPC_SET, &msgctl_s );

    /* create 1 semaphore for writer-to-allow-read   */
    semid = semget( IPC_PRIVATE, 1, IPC_CREAT|0x1ff );
    sops_rd_wr2rd.sem_num = 0;    sops_wr_wr2rd.sem_num = 0;  /* 1st and only sem */
    sops_rd_wr2rd.sem_op  = -1;   sops_wr_wr2rd.sem_op  = 1;  /* reader dec's, writer inc's */
    sops_rd_wr2rd.sem_flg = 0;    sops_wr_wr2rd.sem_flg = 0;  /* default - block behavior */

    /* init wr2rd */
    idx = sops_wr_wr2rd.sem_op;		/* save */
    sops_wr_wr2rd.sem_op  = rd_ahead;
    semop( semid, &sops_wr_wr2rd, 1 );
    sops_wr_wr2rd.sem_op  = idx;	/* restore to saved */

    /* fork off read (from) */
    if ((pid=fork()) == 0)
    {   /* child - does the reading */
	int	read_byts, shm_byts, just_red_byts;
	char	*crc_p;
	int	run_san_byts=0, run_dat_byts=0;	/* running total across reads */
	int	eof_flg=0;

	/* (nice the reading to make sure writing has higher priority???) */
	nice( 10 );
	/* first read will be of size inc_size minus dat_buf_byts */
	bcopy(dat_buf, shmaddr, dat_buf_byts);	/* order of arg diff from memcpy */
	shm_byts = dat_buf_byts;
	crc_p = shmaddr + dat_buf_byts;
	dat_byts = 0;
	while (!eof_flg)
	{
	    /* gain access to *blk* of shared mem */
	    do
	    {   sts = semop( semid, &sops_rd_wr2rd, 1 );
		if ((sts==-1) && (errno!=EINTR))
		{   perror( "semop - read" );
		    /* exit with error??? */
		}
		else if (sts == -1) /* interrupted system call */
		{   perror( "semop - read" );
		}
	    } while (sts != 0);

	    while (shm_byts < inc_size)
	    {   read_byts = inc_size - shm_byts;
		just_red_byts = (frmFunc_p[Frm])(  fd_a[Frm]
						  , shmaddr+(inc_size*ahead_idx)+shm_byts
						  , read_byts );
		if (just_red_byts == 0)
		{   eof_flg = 1;
		    break;	/* manual break out for eof */
		}
		else if (just_red_byts == -1)
		{   
		    perror( "EXfer.to_HSM - read" );
		    msgbuf_s.mtype = Err;
		    msgbuf_s.md.data = errno;
		    if (msgsnd(msgqid,(struct msgbuf *)&msgbuf_s,sizeof(msgbuf_s.md.data),0) == -1) /* normal blocking send */
			perror( "msgsnd - read" );
		    exit (0);
		}
		shm_byts += just_red_byts;	/* only differ for ... */
		dat_byts += just_red_byts;	/* ... 1st read */
	    }

	    run_dat_byts += dat_byts;
	    msgbuf_s.md.data = shm_byts;
	    /*printf( "EXfer reader sending %d bytes to writer\n", shm_byts );*/
	    if (msgsnd(msgqid,(struct msgbuf *)&msgbuf_s,sizeof(msgbuf_s.md.data),0) == -1) /* normal blocking send */
		                                      perror( "msgsnd - read" );

	    /* do crc here -- snd answer as last msg */
	    /* ref. python.../Modules/binascii.c:binascii_crc_hqx */
#          ifndef NO_CRC
	    attrObj1_p = PyObject_CallFunction( obj_pa[Crc], "s#i", crc_p, dat_byts, dat_crc );
	    dat_crc = PyInt_AsLong( attrObj1_p );
	    if (run_san_byts < sanity_byts)
	    {   run_san_byts += dat_byts;
		/* ADD CASE FOR EOF!!! i.e. short file */
		if (run_san_byts >= sanity_byts)
		{   /* OK we may hav more than we need -- we are done */
		    attrObj1_p = PyObject_CallFunction(  obj_pa[Crc], "s#i", crc_p
						      , dat_byts-(run_san_byts-sanity_byts)
						      , san_crc );
		    san_crc = PyInt_AsLong( attrObj1_p );
		    msgbuf_s.mtype = SanCrc;
		    msgbuf_s.md.data = san_crc;
		    if (msgsnd(msgqid,(struct msgbuf *)&msgbuf_s,sizeof(msgbuf_s.md.data),0) == -1) /* normal blocking send */
			perror( "msgsnd - read" );
		    msgbuf_s.mtype = WrtSiz; /* reset */
		}
		else
		{   /* just continue to sanity crc */
		    attrObj1_p = PyObject_CallFunction(  obj_pa[Crc], "s#i", crc_p
						      , dat_byts
						      , san_crc );
		    san_crc = PyInt_AsLong( attrObj1_p );
		}
	    }    
#          endif

	    if (++ahead_idx == rd_ahead) ahead_idx = 0;
	    shm_byts = dat_byts = 0;	/* the same from now on */
	    crc_p = shmaddr + (inc_size*ahead_idx);
	}

	msgbuf_s.mtype = DatCrc;
	msgbuf_s.md.data = dat_crc;
	if (msgsnd(msgqid,(struct msgbuf *)&msgbuf_s,sizeof(msgbuf_s.md.data),0) == -1) /* normal blocking send */
	    perror( "msgsnd - read" );
	msgbuf_s.mtype = DatByt;
	msgbuf_s.md.data = run_dat_byts;
	if (msgsnd(msgqid,(struct msgbuf *)&msgbuf_s,sizeof(msgbuf_s.md.data),0) == -1) /* normal blocking send */
	    perror( "msgsnd - read" );
	exit( 0 );
    }
    while (writing_flg)
    {
	/* read fifo - normal (blocking) */
	do
	{   sts = msgrcv(  msgqid, (struct msgbuf *)&msgbuf_s
			 , sizeof(msgbuf_s.md.data), 0, 0 );
	    if ((sts==-1) && (errno!=EINTR))
	    {   perror( "semop - wr_rd2wr" );
		/* exit with error??? */
	    }
	    if (sts == -1)/* && is EINTR - interrupted system call; stracing */
	    {   perror( "semop - wr_rd2wr" );
	    }
	} while (sts == -1);

	switch (msgbuf_s.mtype)
	{
	case WrtSiz:
#	    ifndef NO_WRITE
	    if (fd_a[To_]) /* if fd ... else fp */
	    {
		if (   (to_Func_p[To_])( fd_a[To_], shmaddr+(inc_size*ahead_idx)
					,msgbuf_s.md.data)
		    != msgbuf_s.md.data)
		    perror( "write" );
	    }
	    else
	    {
		if (   (to_Func_p[To_])(  shmaddr+(inc_size*ahead_idx), 1, msgbuf_s.md.data
					, fp_a[To_] )
		    != msgbuf_s.md.data)
		    perror( "write" );
	    }
#	    endif
	    *bytes_xferred_p += msgbuf_s.md.data;
	    /*printf( "EXfer writer recvd %d bytes from reader\n", msgbuf_s.md.data );*/
	    if (semop(semid,&sops_wr_wr2rd,1) == -1) perror( "semop - read" );
	    if (++ahead_idx == rd_ahead) ahead_idx = 0;
	    break;
	case SanCrc:
	    san_crc = msgbuf_s.md.data;
	    break;
	case DatCrc:
	    /*printf( "EXfer crc is %d\n", msgbuf_s.md.data );*/
	    dat_crc = msgbuf_s.md.data;
	    break;
	case Err:
	    semun_u.val = 0;/* just initialize ("arg" is not used for RMID)*/
	    (void)semctl( semid, 0, IPC_RMID, semun_u );
	    (void)shmdt( shmaddr );
	    (void)shmctl( shmid, IPC_RMID, 0 );
	    (void)msgctl( msgqid, IPC_RMID, 0 );
	    errno = msgbuf_s.md.data;
	    return raise_exception( "error reading from user" );
	    break;
	default:		/* assume DatByt */
	    dat_byts = msgbuf_s.md.data;
	    writing_flg = 0;	/* DONE! */
	    if (waitpid(pid,&sts,0) == -1)
		perror( "EXfer usrTo_ waitpid" );
	    break;
	}
    }

    semun_u.val = 0;/* just initialize ("arg" is not used for RMID)*/
    (void)semctl( semid, 0, IPC_RMID, semun_u );
    (void)shmdt( shmaddr );
    (void)shmctl( shmid, IPC_RMID, 0 );
    (void)msgctl( msgqid, IPC_RMID, 0 );

    return (Py_BuildValue("(iii)",dat_byts,dat_crc,san_crc));
}   /* EXto_HSM */


/******************************************************************************
 * @+Public+@
 * ROUTINE: EXusrTo_:  Added by ron on 01-May-1998
 *
 * DESCRIPTION:		read data from HSM, write to user (wrapper read
 *                      previously) 
 *
 * RETURN VALUES:	tuple - sanity_status, data_crc
 *
 ******************************************************************************/

static char EXusrTo__Doc[] = "Xfers the from HSM to user (and crc check)";

static PyObject *
EXusrTo_(  PyObject	*self
	 , PyObject	*args )
{							/* @-Public-@ */
	int		idx;
	/* MAKE AN ENUM??? */
#	define		Frm	0	/* EXto_HSM Arg1 */
#	define		To_	1	/* EXto_HSM Arg2 */
#	define		Crc	2	/* EXto_HSM Arg3 */
	PyObject	*obj_pa[3];
	int		crc_flg, dat_crc=0, dat_byts=0;
	long		filesize[3];		/* default locations */
	long		*filesize_p[3];
	PyObject	*attrObj_p;
	int		fd_a[2]={0,0};
	int		(*(frmFunc_p)[2])();
	int		(*(to_Func_p)[2])();
	/**/		/**/
	int		inc_size=512;	/* cheesy default */
	int		shmid, semid, msgqid;
	int		sts;
	char		*shmaddr;
	union semun	semun_u;
	struct sembuf	sops_wr_wr2rd;  /* allows read */
	struct sembuf	sops_rd_wr2rd;
	struct msqid_ds	msgctl_s;
	struct s_msg	msgbuf_s;	/* for reader and writer */
	int		ahead_idx = 0;
	int		writing_flg=1;
	int             rd_ahead=50;    /* arbitrary default */
	pid_t		pid;
#      ifdef NO_READ
	struct stat	stat_s;
#      endif


    /*printf( "EXfer.usrTo_ --- \n" );*/
    /*  Parse the arguments */
    PyArg_ParseTuple(  args, "OOOll", &obj_pa[0], &obj_pa[1], &obj_pa[2]
		     , &inc_size, &crc_flg );
    /*printf( "EXfer.usrTo_ after parse\n" );*/

    for (idx=0; idx<2; idx++)
    {
	attrObj_p = PyObject_CallMethod( obj_pa[idx], "fileno", "" );
	fd_a[idx] = PyInt_AsLong(attrObj_p);
	frmFunc_p[idx] = read;
	to_Func_p[idx] = write;
	filesize_p[idx] = &filesize[idx]; filesize[idx] = 0;
    }

    /*printf( "EXfer.usrTo_ crc_flg=%d\n", crc_flg );*/

    /* create private (to be inherited by child) shm seg */
    /* there does not seem to be a 4M size limitation */
    /* try 10x buffering */
    assert( inc_size < 0x400000 );
    rd_ahead = 0x400000 / inc_size;
    shmid = shmget( IPC_PRIVATE, inc_size*rd_ahead, IPC_CREAT|0x1ff/*or w/9bit perm*/ );
    /*printf( "EXfer usrTo_ shmid=%d (size=inc*%d=%d bytes)\n",shmid,rd_ahead,inc_size*rd_ahead);*/
    shmaddr = shmat( shmid, 0, 0 );	/* no addr hint, no flags */
    /*printf( "EXfer shmaddr=%p\n", shmaddr );*/
    if (shmaddr == (char *)-1)
	perror( "shmat" );

    /* create msg Q for reader to send info to writer */
    msgqid = msgget( IPC_PRIVATE, IPC_CREAT|0x1ff );
    msgbuf_s.mtype = WrtSiz;
    msgctl( msgqid, IPC_STAT, &msgctl_s );
    msgctl_s.msg_qbytes = (rd_ahead+1) * sizeof(msgbuf_s.md.data);
    msgctl( msgqid, IPC_SET, &msgctl_s );

    /* create 1 semaphore for writer-to-allow-read   */
    semid = semget( IPC_PRIVATE, 1, IPC_CREAT|0x1ff );
    sops_rd_wr2rd.sem_num = 0;    sops_wr_wr2rd.sem_num = 0;  /* 1st and only sem */
    sops_rd_wr2rd.sem_op  = -1;   sops_wr_wr2rd.sem_op  = 1;  /* reader dec's, writer inc's */
    sops_rd_wr2rd.sem_flg = 0;    sops_wr_wr2rd.sem_flg = 0;  /* default - block behavior */

    /* init wr2rd */
    idx = sops_wr_wr2rd.sem_op;		/* save */
    sops_wr_wr2rd.sem_op  = rd_ahead;
    semop( semid, &sops_wr_wr2rd, 1 );
    sops_wr_wr2rd.sem_op  = idx;	/* restore to saved */

    /* fork off read (from) */
    if ((pid=fork()) == 0)
    {   /* child - does the reading */
	int	read_byts, shm_byts, just_red_byts;
	char	*crc_p;
	int	run_dat_byts=0;	/* running total across reads */
	int	eof_flg=0;

	/* (nice the reading to make sure writing has higher priority???) */
	nice( 10 );
	/* first read will be of size inc_size minus dat_buf_byts */
	shm_byts = 0;
	crc_p = shmaddr;
	dat_byts = 0;
#      ifdef NO_READ
	if (fstat(fd_a[Frm],&stat_s) != 0)
	    perror( "EXfer usrTo_ fstat" );
#      endif
	while (!eof_flg)
	{
	    /* gain access to *blk* of shared mem */
	    if (semop(semid,&sops_rd_wr2rd,1) == -1)  perror( "semop - read" );

	    while (shm_byts < inc_size)
	    {   read_byts = inc_size - shm_byts;
#              ifdef NO_READ
		if ((run_dat_byts+dat_byts+read_byts) <= stat_s.st_size)/* if "space" for full read */
		    just_red_byts = read_byts;
		else
		    just_red_byts = stat_s.st_size - run_dat_byts - dat_byts; /* this will be 0 next time thru */
#              else
		just_red_byts = (frmFunc_p[Frm])(  fd_a[Frm]
						  , shmaddr+(inc_size*ahead_idx)+shm_byts
						  , read_byts );
#              endif
		if (just_red_byts <= 0)
		{   eof_flg = 1;
		    break;	/* manual break out for eof */
		}
		shm_byts += just_red_byts;	/* only differ for ... */
		dat_byts += just_red_byts;	/* ... 1st read */
	    }

	    run_dat_byts += dat_byts;
	    msgbuf_s.md.data = shm_byts;
	    /*printf( "EXfer reader sending %d bytes to writer\n", shm_byts );*/
	    if (msgsnd(msgqid,(struct msgbuf *)&msgbuf_s,sizeof(msgbuf_s.md.data),0) == -1) /* normal blocking send */
		                                      perror( "msgsnd - read" );

	    /* do crc here -- snd answer as last msg */
	    /* ref. python.../Modules/binascii.c:binascii_crc_hqx */
	    if (crc_flg)
	    {   attrObj_p = PyObject_CallFunction( obj_pa[Crc], "s#i", crc_p, dat_byts, dat_crc );
		dat_crc = PyInt_AsLong( attrObj_p );
	    }

	    if (++ahead_idx == rd_ahead) ahead_idx = 0;
	    shm_byts = dat_byts = 0;	/* the same from now on */
	    crc_p = shmaddr + (inc_size*ahead_idx);
	}

	msgbuf_s.mtype = DatCrc;
	msgbuf_s.md.data = dat_crc;
	if (msgsnd(msgqid,(struct msgbuf *)&msgbuf_s,sizeof(msgbuf_s.md.data),0) == -1) /* normal blocking send */
	    perror( "msgsnd - read" );
	msgbuf_s.mtype = DatByt;
	msgbuf_s.md.data = run_dat_byts;
	if (msgsnd(msgqid,(struct msgbuf *)&msgbuf_s,sizeof(msgbuf_s.md.data),0) == -1) /* normal blocking send */
	    perror( "msgsnd - read" );
	exit( 0 );
    }
    while (writing_flg)
    {
	/* read fifo - normal (blocking) */
	if (msgrcv(msgqid,(struct msgbuf *)&msgbuf_s,sizeof(msgbuf_s.md.data),0,0) == -1)
	                                         perror( "semop - wr_rd2wr" );
	switch (msgbuf_s.mtype)
	{
	case WrtSiz:
	    if (   (to_Func_p[To_])( fd_a[To_], shmaddr+(inc_size*ahead_idx)
				    ,msgbuf_s.md.data)
		!= msgbuf_s.md.data)
		perror( "write" );
	    *filesize_p += msgbuf_s.md.data;
	    /*printf( "EXfer writer recvd %d bytes from reader\n", msgbuf_s.md.data );*/
	    if (semop(semid,&sops_wr_wr2rd,1) == -1) perror( "semop - read" );
	    if (++ahead_idx == rd_ahead) ahead_idx = 0;
	    break;
	case DatCrc:
	    /*printf( "EXfer crc is %d\n", msgbuf_s.md.data );*/
	    dat_crc = msgbuf_s.md.data;
	    break;
	default:		/* assume DatByt */
	    dat_byts = msgbuf_s.md.data;
	    writing_flg = 0;	/* DONE! */
	    if (waitpid(pid,&sts,0) == -1)
		perror( "EXfer usrTo_ waitpid" );
	    break;
	}
    }

    semun_u.val = 0;/* just initialize ("arg" is not used for RMID)*/
    (void)semctl( semid, 0, IPC_RMID, semun_u );
    (void)shmdt( shmaddr );
    (void)shmctl( shmid, IPC_RMID, 0 );
    (void)msgctl( msgqid, IPC_RMID, 0 );

    return (Py_BuildValue("i",dat_crc));
}   /* EXusrTo_ */


/******************************************************************************
 * @+Public+@
 * ROUTINE: EXfd_xfer:  Added by ron on 01-May-1998
 *
 * DESCRIPTION:		read data from 1st fd, write to second
 *
 * RETURN VALUE:	int - crc
 *
 *****************************************************************************/

char		*g_shmaddr_p;
int		 g_shmsize;
struct sigaction g_oldSigAct_s;
union semun	 g_semun_u;
int		 g_shmid;
int		 g_semid;
int		 g_msgqid;

void
send_writer(  int	mtype
	    , int	d1
	    , char	*c1 )
{
	int		sts;
	struct s_msg	msg_s;

    msg_s.mtype = mtype;
    msg_s.md.data = d1; /* may not be used */
    msg_s.md.c_p = c1; /* may not be used */
    sts = msgsnd( g_msgqid,(struct msgbuf *)&msg_s,sizeof(struct s_msgdat),0 );
    if (sts == -1) perror( "reader_error" ); /* can not do much more */
    return;
}

void
do_read(  int 		rd_fd
	, int 		no_bytes
	, int 		blk_size 
	, PyObject	*crc_obj_tp )
{
	struct sembuf	 sops_rd_wr2rd;
	struct s_msg	 msgbuf_s;
	int		 crc_i=0;
	int		 shm_off=0;

    msgbuf_s.mtype = WrtSiz;

    sops_rd_wr2rd.sem_num = 0;  /* 1st and only sem */
    sops_rd_wr2rd.sem_op  = -1; /* reader dec's, writer inc's */
    sops_rd_wr2rd.sem_flg = 0;  /* default - block behavior */

    /* (nice the reading to make sure writing has higher priority???) */
    nice( 10 );

    while (no_bytes)
    {
	int	sts;
	/* gain access to *blk* of shared mem */
 semop_try:
	sts = semop( g_semid, &sops_rd_wr2rd, 1 );
	if ((sts==-1) && (errno==EINTR))
	{   printf( "interrupted system call; assume debugger attach???\n" );
	    goto semop_try;
	}
	if (sts == -1) { send_writer( Err, errno, 0 ); exit( 1 ); }

	/* Do not worry about reading an exact block as this is sending to
	   tape. Note: sts may be less than blk_size when reading from net,
	   but this should not cause reader to overwrite data previously
	   given to the writer. I could loop till I read a complete
	   blocksize - this would allow reader to further ahead of writer. */
	sts = read(  rd_fd, g_shmaddr_p+shm_off
		   , (no_bytes<blk_size)?no_bytes:blk_size );
	if (sts == -1) { send_writer( Err, errno, 0 ); exit( 1 ); }
	if (sts == 0) { send_writer( Eof, errno, 0 ); exit( 1 ); }

	send_writer( WrtSiz, sts, g_shmaddr_p+shm_off );

	if (crc_obj_tp)
	{   PyObject	*rr;
	    rr = PyObject_CallFunction(  crc_obj_tp, "s#i", g_shmaddr_p+shm_off
				       , sts, crc_i );
	    crc_i = PyInt_AsLong( rr );
	}

	no_bytes -= sts;
	shm_off += sts;
	if ((shm_off+blk_size) > g_shmsize) shm_off = 0;
    }

    /* could I check for eof??? - probably not if reading from net */

    send_writer( DatCrc, crc_i, 0 );

    exit (0);
    return;
}

void
g_ipc_cleanup( void )
{
    g_semun_u.val = 0;/* just initialize ("arg" is not used for RMID)*/
    (void)semctl( g_semid, 0, IPC_RMID, g_semun_u );
    (void)shmdt(  g_shmaddr_p );
    (void)shmctl( g_shmid, IPC_RMID, 0 );
    (void)msgctl( g_msgqid, IPC_RMID, 0 );

    return;
}

void
fd_xfer_SigHand( int xx )	/* sighand used below to get back to prompt -*/
{				/* when icc communication gets "stuck" 	     */
    printf( "fd_xfer_SigHand called\n" );
    xx = 0;

    g_ipc_cleanup();

    (g_oldSigAct_s.sa_handler)( 0 );

    return;
}

static char EXfd_xfer_Doc[] = "fd_xfer( fr_fd,to_fd,no_bytes,blk_siz,crc_fun )";

static PyObject *
EXfd_xfer(  PyObject	*self
	 , PyObject	*args )
{							/* @-Public-@ */
	int		 fr_fd;
	int		 to_fd;
	int		 no_bytes;
	int		 blk_size;
	PyObject	*crc_obj_tp;

	int		 sts;
	struct sigaction newSigAct_s;
	int		 rd_ahead_i;
	int		 crc_i;
	PyObject	*rr;
	int		 pid;
	struct msqid_ds	 msgctl_s;
	struct sembuf	 sops_wr_wr2rd;  /* allows read */

    sts = PyArg_ParseTuple(  args, "iiiiO", &fr_fd, &to_fd, &no_bytes
			   , &blk_size, &crc_obj_tp );
    if (!sts) return (NULL);

    /* see if we are crc-ing */
    if ((crc_obj_tp==Py_None) || PyInt_Check(crc_obj_tp)) crc_obj_tp = 0;

    /* set up the signal handler b4 we get the ipc stuff */
    newSigAct_s.sa_handler = fd_xfer_SigHand;
    newSigAct_s.sa_flags   = 0;
    sigemptyset( &newSigAct_s.sa_mask );
#   define DOSIGACT 0
#   if DOSIGACT == 1
    sigaction( SIGINT, &newSigAct_s, &g_oldSigAct_s );
#   endif

    assert( blk_size < 0x400000 );
    rd_ahead_i = 0x400000 / blk_size; /* do integer arithmatic */
    g_shmsize = blk_size * rd_ahead_i;
    g_shmid = shmget( IPC_PRIVATE, g_shmsize, IPC_CREAT|0x1ff/*or w/9bit perm*/ );
    g_shmaddr_p = shmat( g_shmid, 0, 0 );	/* no addr hint, no flags */
    if (g_shmaddr_p == (char *)-1)
	return (raise_exception("fd_xfer shmat"));

    /* create msg Q for reader to send info to writer */
    g_msgqid = msgget( IPC_PRIVATE, IPC_CREAT|0x1ff );
    msgctl( g_msgqid, IPC_STAT, &msgctl_s );
    msgctl_s.msg_qbytes = (rd_ahead_i+1) * sizeof(struct s_msgdat);
    msgctl( g_msgqid, IPC_SET, &msgctl_s );

    /* create 1 semaphore for writer-to-allow-read   */
    g_semid = semget( IPC_PRIVATE, 1, IPC_CREAT|0x1ff );

    /* == NOW DO THE WORK ===================================================*/

    sops_wr_wr2rd.sem_num = 0;  /* 1st and only sem */
    sops_wr_wr2rd.sem_op  = 1;  /* reader dec's, writer inc's */
    sops_wr_wr2rd.sem_flg = 0;  /* default - block behavior */

    /* init wr2rd */
    sts = sops_wr_wr2rd.sem_op;		/* save */
    sops_wr_wr2rd.sem_op  = rd_ahead_i;
    semop( g_semid, &sops_wr_wr2rd, 1 );
    sops_wr_wr2rd.sem_op  = sts;	/* restore to saved */

    /* fork off read (from) */
    if ((pid=fork()) == 0) do_read(  fr_fd, no_bytes, blk_size, crc_obj_tp );
    else
    {   int		 writing_flg=1;
	struct s_msg	 msg_s;
	
	while (writing_flg)
	{   /* read fifo - normal (blocking) */
	    sts = msgrcv(  g_msgqid, (struct s_msg *)&msg_s
			 , sizeof(struct s_msgdat), 0, 0 );
	    if (sts == -1) return (raise_exception("fd_xfer - writer msgrcv"));

	    switch (msg_s.mtype)
	    {
	    case WrtSiz:
		sts = 0;
		do
		{   msg_s.md.data -= sts;
		    sts = write( to_fd, msg_s.md.c_p, msg_s.md.data );
		    if (sts == -1)
		    {   g_ipc_cleanup();
			return (raise_exception("fd_xfer - write"));
		    }
		} while (sts != msg_s.md.data);

		sts = semop( g_semid, &sops_wr_wr2rd, 1 );
		if (sts == -1)
		{   g_ipc_cleanup();
		    return (raise_exception("fd_xfer - write - semop"));
		}
		break;
	    case Err:
		g_ipc_cleanup();
		errno = msg_s.md.data;
		return (raise_exception("fd_xfer - read error"));
		break;
	    case Eof:
		g_ipc_cleanup();
		return (raise_exception("fd_xfer - read EOF unexpected"));
		break;
	    default:		/* assume DatCrc */
		writing_flg = 0;	/* DONE! */
		crc_i = msg_s.md.data;
		break;
	    }
	}
    }
    /* == DONE WITH THE WORK - CLEANUP ======================================*/

    g_semun_u.val = 0;/* just initialize ("arg" is not used for RMID)*/
    (void)semctl( g_semid, 0, IPC_RMID, g_semun_u );
    (void)shmdt(  g_shmaddr_p );
    (void)shmctl( g_shmid, IPC_RMID, 0 );
    (void)msgctl( g_msgqid, IPC_RMID, 0 );

#   if DOSIGACT == 1
    sigaction( SIGINT, &g_oldSigAct_s, (void *)0 );
#   endif

    if (waitpid(pid,&sts,0) == -1)
	return (raise_exception("fd_xfer - waitpid"));

    if (crc_obj_tp)
	rr = Py_BuildValue( "i", crc_i );
    else
	rr = Py_BuildValue( "" );
    return (rr);
}   /* EXfd_xfer */


/* = = = = = = = = = = = = = = -  Python Module Definitions = = = = = = = = = = = = = = - */

/*  Module Methods table. 

    There is one entry with four items for for each method in the module

    Entry 1 - the method name as used  in python
          2 - the c implementation function
	  3 - flags 
	  4 - method documentation string
	  */

static PyMethodDef EXfer_Methods[] = {
    { "usrTo_",  EXusrTo_,  1, EXusrTo__Doc},
    { "to_HSM",  EXto_HSM,  1, EXto_HSM_Doc},
    { "fd_xfer",  EXfd_xfer,  1, EXfd_xfer_Doc},
    { 0, 0}        /* Sentinel */
};

/******************************************************************************
    Module initialization.   Python call the entry point init<module name>
    when the module is imported.  This should the only non-static entry point
    so it is exported to the linker.

    The Py_InitModule4 is not in the python 1.5 documentation but is copied
    from the oracle module.  It extends Py_InitModule with documentation
    and seems useful.

    First argument must be a the module name string.
    
    Second       - a list of the module methods

    Third	- a doumentation string for the module
  
    Fourth & Fifth - see Python/modsupport.c
    */

void
initEXfer()
{
	PyObject	*m, *d;

    m = Py_InitModule4(  "EXfer", EXfer_Methods, EXfer_Doc
		       , (PyObject*)NULL, PYTHON_API_VERSION );
    d = PyModule_GetDict(m);
    EXErrObject = PyErr_NewException("EXfer.error", NULL, NULL);
    if (EXErrObject != NULL)
	PyDict_SetItemString(d,"error",EXErrObject);
}

