/*  This file (EXfer.c) was created by Ron Rechenmacher <ron@fnal.gov> on
    Apr 30, 1998. "TERMS AND CONDITIONS" governing this file are in the README
    or COPYING file. If you do not have such a file, one can be obtained by
    contacting Fermi Lab in Batavia IL, 60510, phone: 630-840-3000.
    */

#include <sys/types.h>		/* read/write */
#include <stdio.h>		/* fread/fwrite */
#include <unistd.h>		/* read/write, fork, nice, close */
#include <sys/ipc.h>		/* shmxxx */
#include <sys/shm.h>		/* shmxxx */
#include <sys/sem.h>		/* semxxx */
#include <sys/msg.h>		/* msg{snd,rcv} */
#include <Python.h>		/* all the Py.... stuff */
#include <assert.h>             /* assert */
#if 1
# include <ftt.h>		/* ftt_read/write */
#else
/* from ftt_lib/ftt_private.h */
# include <errno.h>
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
};
struct s_msg
{
    enum e_mtype	mtype;	/* see man msgsnd */
    int			data;
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
#ifdef HAVE_STDARG_PROTOTYPES
raise_exception(  char		*method_name
		, ... )
#else
raise_exception( method_name, ETdesc, va_alist )
     char		*method_name;
     va_dcl;
#endif
{							/* @-Public-@ */
	char	errbuf[500];

    /*  dealloc and raise exception fix */
    sprintf(  errbuf, "Error in %s\n", method_name );
    PyErr_SetString( EXErrObject, errbuf );
    return (NULL);
}   /* raise_exception */


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
	int		sanity_byts;	/* EXto_HSM Arg 4*/
	int		san_crc=0, dat_crc=0, dat_byts=0;
	long		filesize[3];		/* default locations */
	long		*filesize_p[3];
	PyObject	*attrObj_p;
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
	struct sembuf	sops_wr_wr2rd;  /* allows read */
	struct sembuf	sops_rd_wr2rd;
	struct msqid_ds	msgctl_s;
	struct s_msg	msgbuf_s;	/* for reader and writer */
	int		ahead_idx = 0;
	int		writing_flg=1;
	int             rd_ahead=50;    /* arbitrary default */


    /*  Parse the arguments */
    PyArg_ParseTuple(  args, "OOOls#", &obj_pa[0], &obj_pa[1], &obj_pa[2]
		     , &sanity_byts, &dat_buf, &dat_buf_byts );

    for (idx=0; idx<2; idx++)
    {
	attrObj_p = PyObject_GetAttrString( obj_pa[idx], "__class__" );
	attrObj_p = PyObject_GetAttrString( attrObj_p, "__name__" );
	str       = PyString_AsString( attrObj_p );
	if      (strcmp(str,"Mover") == 0)
	{   attrObj_p = PyObject_GetAttrString( obj_pa[idx], "data_socket" );
	    attrObj_p = PyObject_CallMethod( attrObj_p, "fileno", "" );
	    fd_a[idx] = PyInt_AsLong(attrObj_p);
	    if (idx == Frm)
		frmFunc_p[idx] = read;
	    else
		to_Func_p[idx] = write;
	    filesize_p[idx] = &filesize[idx]; filesize[idx] = 0;
	    printf( "EXfer p%d class is Mover, fd=%d\n", idx+1, fd_a[idx] );
	}
	else if (strcmp(str,"FTTDriver") == 0)
	{   attrObj_p = PyObject_GetAttrString( obj_pa[idx], "ETdesc" );
	    /* struct s_ETdesc is kept at int member */
	    fd_a[idx] = PyInt_AsLong(attrObj_p);
	    if (idx == Frm)
		frmFunc_p[idx] = ftt_read;
	    else
		to_Func_p[idx] = ftt_write;
	    printf(  "EXfer p%d class is FTTDriver, devname=%s, block_size=%d\n", idx+1
		   , *(char **)((struct s_ETdesc *)fd_a[idx])->ftt_desc
		   , ((struct s_ETdesc *)fd_a[idx])->block_size );
	    if (idx == To_)  inc_size = ((struct s_ETdesc *)fd_a[idx])->block_size;
	    fd_a[idx] = (int)((struct s_ETdesc *)fd_a[idx])->ftt_desc;
	    filesize_p[idx] = &((struct s_ETdesc *)fd_a[idx])->filesize;
	}
	else if (strcmp(str,"RawDiskDriver") == 0)
	{   attrObj_p = PyObject_GetAttrString( obj_pa[idx], "df" );
	    fp_a[idx] = PyFile_AsFile( attrObj_p );
	    if (idx == Frm)
		frmFunc_p[idx] = (void(*))fread;
	    else
		to_Func_p[idx] = (void(*))fwrite;
	    filesize_p[idx] = &filesize[idx]; filesize[idx] = 0;
	    printf( "EXfer p%d class is RawDiskDriver, fd=%d\n", idx+1, fd_a[idx] );
	}
	else
	{
	    printf( "EXfer p%d class is Unknown\n", idx+1 );
	}
    }

    printf( "EXfer sanity_byts=%d\n", sanity_byts );

    /* create private (to be inherited by child) shm seg */
    /* there does not seem to be a 4M size limitation */
    /* try 10x buffering */
    assert( inc_size < 0x400000 );
    rd_ahead = 0x400000 / inc_size;
    shmid = shmget( IPC_PRIVATE, inc_size*rd_ahead, IPC_CREAT|0x1ff/*or w/9bit perm*/ );
    printf( "EXfer to_HSM shmid=%d (size=inc*%d=%d bytes)\n", shmid, rd_ahead, inc_size*rd_ahead );
    shmaddr = shmat( shmid, 0, 0 );	/* no addr hint, no flags */
    printf( "EXfer shmaddr=%p\n", shmaddr );
    if (shmaddr == (char *)-1)
	perror( "shmat" );

    /* create msg Q for reader to send info to writer */
    msgqid = msgget( IPC_PRIVATE, IPC_CREAT|0x1ff );
    msgbuf_s.mtype = WrtSiz;
    msgctl( msgqid, IPC_STAT, &msgctl_s );
    msgctl_s.msg_qbytes = (rd_ahead+1) * sizeof(msgbuf_s.data);
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
    if (fork() == 0)
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
	    if (semop(semid,&sops_rd_wr2rd,1) == -1)  perror( "semop - read" );

	    while (shm_byts < inc_size)
	    {   read_byts = inc_size - shm_byts;
		just_red_byts = (frmFunc_p[Frm])(  fd_a[Frm]
						  , shmaddr+(inc_size*ahead_idx)+shm_byts
						  , read_byts );
		if (just_red_byts == 0)
		{   eof_flg = 1;
		    break;	/* manual break out for eof */
		}
		shm_byts += just_red_byts;	/* only differ for ... */
		dat_byts += just_red_byts;	/* ... 1st read */
	    }

	    run_dat_byts += dat_byts;
	    msgbuf_s.data = shm_byts;
	    /*printf( "EXfer reader sending %d bytes to writer\n", shm_byts );*/
	    if (msgsnd(msgqid,(struct msgbuf *)&msgbuf_s,sizeof(msgbuf_s.data),0) == -1) /* normal blocking send */
		                                      perror( "msgsnd - read" );

	    /* do crc here -- snd answer as last msg */
	    /* ref. python.../Modules/binascii.c:binascii_crc_hqx */
	    attrObj_p = PyObject_CallFunction( obj_pa[Crc], "s#i", crc_p, dat_byts, dat_crc );
	    dat_crc = PyInt_AsLong( attrObj_p );
	    if (run_san_byts < sanity_byts)
	    {   run_san_byts += dat_byts;
		/* ADD CASE FOR EOF!!! i.e. short file */
		if (run_san_byts >= sanity_byts)
		{   /* OK we may hav more than we need -- we are done */
		    attrObj_p = PyObject_CallFunction(  obj_pa[Crc], "s#i", crc_p
						      , dat_byts-(run_san_byts-sanity_byts)
						      , san_crc );
		    san_crc = PyInt_AsLong( attrObj_p );
		    msgbuf_s.mtype = SanCrc;
		    msgbuf_s.data = san_crc;
		    if (msgsnd(msgqid,(struct msgbuf *)&msgbuf_s,sizeof(msgbuf_s.data),0) == -1) /* normal blocking send */
			perror( "msgsnd - read" );
		    msgbuf_s.mtype = WrtSiz; /* reset */
		}
		else
		{   /* just continue to sanity crc */
		    attrObj_p = PyObject_CallFunction(  obj_pa[Crc], "s#i", crc_p
						      , dat_byts
						      , san_crc );
		    san_crc = PyInt_AsLong( attrObj_p );
		}
	    }    

	    if (++ahead_idx == rd_ahead) ahead_idx = 0;
	    shm_byts = dat_byts = 0;	/* the same from now on */
	    crc_p = shmaddr + (inc_size*ahead_idx);
	}

	msgbuf_s.mtype = DatCrc;
	msgbuf_s.data = dat_crc;
	if (msgsnd(msgqid,(struct msgbuf *)&msgbuf_s,sizeof(sts),0) == -1) /* normal blocking send */
	    perror( "msgsnd - read" );
	msgbuf_s.mtype = DatByt;
	msgbuf_s.data = run_dat_byts;
	if (msgsnd(msgqid,(struct msgbuf *)&msgbuf_s,sizeof(sts),0) == -1) /* normal blocking send */
	    perror( "msgsnd - read" );
	exit( 0 );
    }
    while (writing_flg)
    {
	/* read fifo - normal (blocking) */
	if (msgrcv(msgqid,(struct msgbuf *)&msgbuf_s,sizeof(msgbuf_s.data),0,0) == -1)
	                                         perror( "semop - wr_rd2wr" );
	switch (msgbuf_s.mtype)
	{
	case WrtSiz:
	    if (fd_a[To_])
	    {
		if (   (to_Func_p[To_])( fd_a[To_], shmaddr+(inc_size*ahead_idx)
					,msgbuf_s.data)
		    != msgbuf_s.data)
		    perror( "write" );
	    }
	    else
	    {
		if (   (to_Func_p[To_])( fp_a[To_], 1, shmaddr+(inc_size*ahead_idx)
					,msgbuf_s.data)
		    != msgbuf_s.data)
		    perror( "write" );
	    }
	    *filesize_p += msgbuf_s.data;
	    /*printf( "EXfer writer recvd %d bytes from reader\n", msgbuf_s.data );*/
	    if (semop(semid,&sops_wr_wr2rd,1) == -1) perror( "semop - read" );
	    if (++ahead_idx == rd_ahead) ahead_idx = 0;
	    break;
	case SanCrc:
	    san_crc = msgbuf_s.data;
	    break;
	case DatCrc:
	    printf( "EXfer crc is %d\n", msgbuf_s.data );
	    dat_crc = msgbuf_s.data;
	    break;
	default:		/* assume DatByt */
	    dat_byts = msgbuf_s.data;
	    writing_flg = 0;	/* DONE! */
	    break;
	}
    }

    (void)semctl( semid, 0, IPC_RMID, 0 );
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
	int		crc_flg, san_crc=0, dat_crc=0, dat_byts=0;
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
	struct sembuf	sops_wr_wr2rd;  /* allows read */
	struct sembuf	sops_rd_wr2rd;
	struct msqid_ds	msgctl_s;
	struct s_msg	msgbuf_s;	/* for reader and writer */
	int		ahead_idx = 0;
	int		writing_flg=1;
	int             rd_ahead=50;    /* arbitrary default */


    printf( "EXfer.usrTo_ --- \n" );
    /*  Parse the arguments */
    PyArg_ParseTuple(  args, "OOOll", &obj_pa[0], &obj_pa[1], &obj_pa[2]
		     , &inc_size, &crc_flg );
    printf( "EXfer.usrTo_ after parse\n" );

    for (idx=0; idx<2; idx++)
    {
	attrObj_p = PyObject_CallMethod( obj_pa[idx], "fileno", "" );
	fd_a[idx] = PyInt_AsLong(attrObj_p);
	frmFunc_p[idx] = read;
	to_Func_p[idx] = write;
	filesize_p[idx] = &filesize[idx]; filesize[idx] = 0;
    }

    printf( "EXfer.usrTo_ crc_flg=%d\n", crc_flg );

    /* create private (to be inherited by child) shm seg */
    /* there does not seem to be a 4M size limitation */
    /* try 10x buffering */
    assert( inc_size < 0x400000 );
    rd_ahead = 0x400000 / inc_size;
    shmid = shmget( IPC_PRIVATE, inc_size*rd_ahead, IPC_CREAT|0x1ff/*or w/9bit perm*/ );
    printf( "EXfer usrTo_ shmid=%d (size=inc*%d=%d bytes)\n",shmid,rd_ahead,inc_size*rd_ahead);
    shmaddr = shmat( shmid, 0, 0 );	/* no addr hint, no flags */
    printf( "EXfer shmaddr=%p\n", shmaddr );
    if (shmaddr == (char *)-1)
	perror( "shmat" );

    /* create msg Q for reader to send info to writer */
    msgqid = msgget( IPC_PRIVATE, IPC_CREAT|0x1ff );
    msgbuf_s.mtype = WrtSiz;
    msgctl( msgqid, IPC_STAT, &msgctl_s );
    msgctl_s.msg_qbytes = (rd_ahead+1) * sizeof(msgbuf_s.data);
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
    if (fork() == 0)
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
	while (!eof_flg)
	{
	    /* gain access to *blk* of shared mem */
	    if (semop(semid,&sops_rd_wr2rd,1) == -1)  perror( "semop - read" );

	    while (shm_byts < inc_size)
	    {   read_byts = inc_size - shm_byts;
		just_red_byts = (frmFunc_p[Frm])(  fd_a[Frm]
						  , shmaddr+(inc_size*ahead_idx)+shm_byts
						  , read_byts );
		if (just_red_byts <= 0)
		{   eof_flg = 1;
		    break;	/* manual break out for eof */
		}
		shm_byts += just_red_byts;	/* only differ for ... */
		dat_byts += just_red_byts;	/* ... 1st read */
	    }

	    run_dat_byts += dat_byts;
	    msgbuf_s.data = shm_byts;
	    /*printf( "EXfer reader sending %d bytes to writer\n", shm_byts );*/
	    if (msgsnd(msgqid,(struct msgbuf *)&msgbuf_s,sizeof(msgbuf_s.data),0) == -1) /* normal blocking send */
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
	msgbuf_s.data = dat_crc;
	if (msgsnd(msgqid,(struct msgbuf *)&msgbuf_s,sizeof(sts),0) == -1) /* normal blocking send */
	    perror( "msgsnd - read" );
	msgbuf_s.mtype = DatByt;
	msgbuf_s.data = run_dat_byts;
	if (msgsnd(msgqid,(struct msgbuf *)&msgbuf_s,sizeof(sts),0) == -1) /* normal blocking send */
	    perror( "msgsnd - read" );
	exit( 0 );
    }
    while (writing_flg)
    {
	/* read fifo - normal (blocking) */
	if (msgrcv(msgqid,(struct msgbuf *)&msgbuf_s,sizeof(msgbuf_s.data),0,0) == -1)
	                                         perror( "semop - wr_rd2wr" );
	switch (msgbuf_s.mtype)
	{
	case WrtSiz:
	    if (   (to_Func_p[To_])( fd_a[To_], shmaddr+(inc_size*ahead_idx)
				    ,msgbuf_s.data)
		!= msgbuf_s.data)
		perror( "write" );
	    *filesize_p += msgbuf_s.data;
	    /*printf( "EXfer writer recvd %d bytes from reader\n", msgbuf_s.data );*/
	    if (semop(semid,&sops_wr_wr2rd,1) == -1) perror( "semop - read" );
	    if (++ahead_idx == rd_ahead) ahead_idx = 0;
	    break;
	case SanCrc:
	    san_crc = msgbuf_s.data;
	    break;
	case DatCrc:
	    printf( "EXfer crc is %d\n", msgbuf_s.data );
	    dat_crc = msgbuf_s.data;
	    break;
	default:		/* assume DatByt */
	    dat_byts = msgbuf_s.data;
	    writing_flg = 0;	/* DONE! */
	    break;
	}
    }

    (void)semctl( semid, 0, IPC_RMID, 0 );
    (void)shmdt( shmaddr );
    (void)shmctl( shmid, IPC_RMID, 0 );
    (void)msgctl( msgqid, IPC_RMID, 0 );
  
    return (Py_BuildValue("i",dat_crc));
}   /* EXusrTo_ */


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

