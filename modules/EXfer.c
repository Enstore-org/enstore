/*  This file (EXfer.c) was created by Ron Rechenmacher <ron@fnal.gov> on
    Apr 30, 1998. "TERMS AND CONDITIONS" governing this file are in the README
    or COPYING file. If you do not have such a file, one can be obtained by
    contacting Fermi Lab in Batavia IL, 60510, phone: 630-840-3000.
    */

#include <Python.h>		/* all the Py.... stuff */

#include <sys/stat.h>		/* fstat, struct stat */
#include <sys/types.h>		/* read/write */
#include <sys/wait.h>		/* waitpid */
#include <stdio.h>		/* fread/fwrite */
#include <unistd.h>		/* read/write, fork, nice, close */
#include <sys/ipc.h>		/* shmxxx */
#include <sys/shm.h>		/* shmxxx */
#include <sys/sem.h>		/* semxxx */
#include <sys/msg.h>		/* msg{snd,rcv} */
#include <assert.h>             /* assert */
#include <errno.h>
#include <signal.h>		/* sigaction() and struct sigaction */

#include "IPC.h"		/* struct s_IPCshmgetObject, IPCshmget_Type */

#if 0
# define PRINTF printf
#else
  static int dummy;
# define PRINTF dummy = 0 && printf
#endif


/* POSIXly correct systems don't define union semun in their system headers */

/* This is for Linux */

#ifdef _SEM_SEMUN_UNDEFINED
union semun
{
    int val;	               /* value for SETVAL */
    struct semid_ds *buf;      /* buffer for IPC_STAT & IPC_SET */
    unsigned short int *array; /* array for GETALL & SETALL */
    struct seminfo *__buf;     /* buffer for IPC_INFO */
};
#endif

/* This is for SunOS and OSF1 */

#if defined(sun) || defined(__osf__)
union semun
{
    int val;	               /* value for SETVAL */
    struct semid_ds *buf;      /* buffer for IPC_STAT & IPC_SET */
    unsigned short int *array; /* array for GETALL & SETALL */
    struct seminfo *__buf;     /* buffer for IPC_INFO */
};
#endif

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

/*checksumming is now being done here, instead of calling another module,
  in order to save a strcpy  -  cgw 1990428 */
unsigned int adler32(unsigned int, char *, int);


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
        PyObject	*v;
        int		i = errno;

#   ifdef EINTR
    if ((i==EINTR) && PyErr_CheckSignals()) return NULL;
#   endif

    /* note: format should be the same as in FTT.c */
    v = Py_BuildValue( "(s,i,s,i)", msg, i, strerror(i), getpid() );
    if (v != NULL)
    {   PyErr_SetObject( EXErrObject, v );
	Py_DECREF(v);
    }
    return NULL;
}


/******************************************************************************
 * @+Public+@
 * ROUTINE: EXfd_xfer:  Added by ron on 01-May-1998
 *
 * DESCRIPTION:		read data from 1st fd, write to second
 *
 * RETURN VALUE:	long - crc
 *
 *****************************************************************************/

static char		*g_shmaddr_p;
static int		 g_shmsize;
static struct sigaction	 g_oldSigAct_sa[32];
static int		 g_shmid;
static int		 g_semid;
static int		 g_msgqid;
static int		 g_pid=0;

static void
send_writer(  enum e_mtype	mtype
	    , int		d1
	    , char		*c1 )
{
	int		sts;
	struct s_msg	msg_s;

    msg_s.mtype = mtype;
    msg_s.md.data = d1; /* may not be used */
    msg_s.md.c_p = c1; /* may not be used */
 re_snd:
    sts = msgsnd( g_msgqid,(struct msgbuf *)&msg_s,sizeof(struct s_msgdat),0 );
    if ((sts==-1) && (errno==EINTR))
    {   PRINTF(  "EXfer (pid=%d) - redoing reader snd after interruption\n"
	       , getpid() );
	goto re_snd;
    }
    if (sts == -1)
    {   perror( "EXfer: fatal reader (send_writer) error" );
	/* can not do much more, but lets try exitting */
	exit( 1 );
    }
    return;
}

static int
do_read(  int 		rd_fd
	, int 		no_bytes
	, int 		blk_size 
        , int           crc_flag
	, unsigned int	crc_i
	, int		*read_bytes_ip )
{
	struct sembuf	 sops_rd_wr2rd;
	struct s_msg	 msgbuf_s;
	int		 shm_off=0;

    msgbuf_s.mtype = WrtSiz;

    sops_rd_wr2rd.sem_num = 0;  /* 1st and only sem */
    sops_rd_wr2rd.sem_op  = -1; /* reader dec's, writer inc's */
    sops_rd_wr2rd.sem_flg = 0;  /* default - block behavior */

    /* (nice the reading to make sure writing has higher priority???) */
    /* XXX is this the correct thing to do?  */
    nice( 10 );
    while (no_bytes)
    {
	int	sts;
	int bytes_to_read;
	/* gain access to *blk* of shared mem */
 semop_try:
	sts = semop( g_semid, &sops_rd_wr2rd, 1 );
	
	if ((sts==-1) && (errno==EINTR))
	{   PRINTF(  "(pid=%d) interrupted system call; assume ctl-Z OR debugger attach???\n"
		   , getpid() );
	    goto semop_try;
	}
	if (sts == -1) { send_writer( Err, errno, 0 ); return (1); }

	/* Do not worry about reading an exact block as this is sending to
	   tape. Note: sts may be less than blk_size when reading from net,
	   but this should not cause reader to overwrite data previously
	   given to the writer. I could loop till I read a complete
	   blocksize - this would allow reader to further ahead of writer. */
	bytes_to_read = (no_bytes<blk_size)?no_bytes:blk_size;
	errno=0;
	sts = read(  rd_fd, g_shmaddr_p+shm_off, bytes_to_read);
	if (sts!= bytes_to_read){
	    PRINTF("DEBUG: asked for %d, got %d\n",bytes_to_read, sts);
	    PRINTF("errno=%d\n",errno);
	}
	if (sts == -1) { send_writer( Err, errno, 0 ); return (1); }
	if (sts == 0) { send_writer( Eof, errno, 0 ); return (1); }

	*read_bytes_ip += sts;

	send_writer( WrtSiz, sts, g_shmaddr_p+shm_off );

	switch (crc_flag){
	case 0:
	    break;
	case 1:
	    crc_i=adler32(crc_i,g_shmaddr_p+shm_off, sts);
	    break;
	default:
	    printf("fd_xfer: invalid crc flag");
	    crc_i=0;
	}
	
	no_bytes -= sts;
	shm_off += sts;
	if ((shm_off+blk_size) > g_shmsize) shm_off = 0;
    }

    /* could I check for eof??? - probably not if reading from net */

    send_writer( DatCrc, crc_i, 0 );

    return (0);
}

static void
g_ipc_cleanup( void )
{
	union semun	 semun_u;

    semun_u.val = 0;/* just initialize ("arg" is not used for RMID)*/
    (void)semctl( g_semid, 0, IPC_RMID, semun_u );
    (void)shmdt(  g_shmaddr_p );
    (void)shmctl( g_shmid, IPC_RMID, 0 );
    (void)msgctl( g_msgqid, IPC_RMID, 0 );

    return;
}

static void
fd_xfer_SigHand( int sig )
{
    printf( "fd_xfer_SigHand (pid=%d) called (sig=%d)\n", getpid(), sig );

#   if 0
       SIGPIPE      13        A      Broken pipe: write to pipe with no readers
       #define EPIPE           32      /* Broken pipe */
#   endif

    /*          2                 13                15  */
    if ((sig==SIGINT) || (sig==SIGPIPE) || (sig==SIGTERM))
    {   /* only clean up when we are dead */
	/* do not clean up on SIGTSTP (when we are suspended) */
	g_ipc_cleanup();

	if (g_pid)
	{   int sts;
	    PRINTF(  "(pid=%d) attempt kill -9 of forked process %d\n"
		   , getpid(), g_pid );
	    kill( g_pid, 9 );
	    waitpid( g_pid, &sts, 0 );
	}
	exit( (1<<8) | sig );
    }


    if (  (g_oldSigAct_sa[sig].sa_handler!=SIG_DFL)
	&&(g_oldSigAct_sa[sig].sa_handler!=SIG_IGN) )
	(g_oldSigAct_sa[sig].sa_handler)( sig );
    else
	raise( sig );

    return;
}

static char EXfd_xfer_Doc[] = "\
fd_xfer( fr_fd, to_fd, no_bytes, blk_siz, crc_flag[, crc, shm] )";

static PyObject *
EXfd_xfer(  PyObject	*self
	  , PyObject	*args )
{							/* @-Public-@ */
	int		 fr_fd;
	int		 to_fd;
	int		 no_bytes;
	int		 blk_size;
	PyObject	*crc_obj_tp;
	PyObject	*crc_tp=Py_None;/* optional, ref. FTT.fd_xfer */
	PyObject	*shm_obj_tp=0;  /* optional, ref. FTT.fd_xfer */
	int              crc_flag=0; /*0: no CRC 1: Adler32 CRC >1: RFU */

	unsigned long    crc_i;
	int		 sts;
	struct sigaction newSigAct_sa[32];
	int		 rd_ahead_i;
	PyObject	*rr;
	struct msqid_ds	 msgctl_s;
	struct sembuf	 sops_wr_wr2rd;  /* allows read */
	int		 dummy=0;
	int		*read_bytes_ip=&dummy, *write_bytes_ip=&dummy;

    sts = PyArg_ParseTuple(  args, "iiiiO|OO", &fr_fd, &to_fd, &no_bytes
			   , &blk_size, &crc_obj_tp, &crc_tp, &shm_obj_tp );
    if (!sts) return (NULL);
    if      (crc_tp == Py_None)   crc_i = 0;
    else if (PyLong_Check(crc_tp)) crc_i = PyLong_AsUnsignedLong(crc_tp);
    else if (PyInt_Check(crc_tp)) crc_i = (unsigned)PyInt_AsLong( crc_tp );
    else return(raise_exception("fd_xfer - invalid crc param"));

    /* see if we are crc-ing */
    if (crc_obj_tp==Py_None) crc_flag=0;
    else if (PyInt_Check(crc_obj_tp)) crc_flag = PyInt_AsLong(crc_obj_tp);
    else return(raise_exception("fd_xfer - invalid crc param"));
    if (crc_flag>1 || crc_flag<0)
	printf("fd_xfer - invalid crc param");
    


    /* set up the signal handler b4 we get the ipc stuff */
    newSigAct_sa[0].sa_handler = fd_xfer_SigHand;
    newSigAct_sa[0].sa_flags   = 0 | SA_RESETHAND;
    sigemptyset( &(newSigAct_sa[0].sa_mask) );
#   define DOSIGACT 1
#   if DOSIGACT == 1
#   define SIGLIST { SIGINT, SIGTERM }
    /*sigaction( SIGINT, &newSigAct_s, &g_oldSigAct_s );*/
    {   int ii;
	/* do not include SIGPIPE -- we will not get EPIPE if it is included */
	int sigs[] = SIGLIST;
	for (ii=0; ii<(sizeof(sigs)/sizeof(sigs[0])); ii++)
	{   
	    newSigAct_sa[sigs[ii]] = newSigAct_sa[0];
	    sts = sigaction(  sigs[ii], &(newSigAct_sa[sigs[ii]])
			    , &(g_oldSigAct_sa[sigs[ii]]) );
	    if (sts == -1)
		printf( "(pid=%d) error with sig %d\n", getpid(), sigs[ii] );
	    else if (g_oldSigAct_sa[sigs[ii]].sa_handler == SIG_IGN)
	        PRINTF( "(pid=%d) sig %d was ignored\n", getpid(), sigs[ii] );
	    else if (g_oldSigAct_sa[sigs[ii]].sa_handler == SIG_DFL)
	        PRINTF(  "(pid=%d) sig %d had default handler\n", getpid()
		       , sigs[ii] );
	    else
	        PRINTF(  "(pid=%d) sig %d had non-default handler\n", getpid()
		       , sigs[ii] );
	}
    }
#   endif

    /* NOTE: FOR THE MOVER, A PARENT PROCESS WILL PASS A SHM OBJECT SO IT
       CAN MONITOR TRANSFER PROGRESS VIA SPECIFIC LOACATION WITHIN THE
       SHM. */
    assert( blk_size < 0x400000 );
    if ((shm_obj_tp==0) || (shm_obj_tp==Py_None) || PyInt_Check(shm_obj_tp))
    {   shm_obj_tp = 0;		/* flag that we were not passed a shm... */
	rd_ahead_i = 0x400000 / blk_size; /* do integer arithmatic */
	g_shmsize = blk_size * rd_ahead_i;
	g_shmid = shmget(  IPC_PRIVATE, g_shmsize
			 , IPC_CREAT|0x1ff/*or w/9bit perm*/ );
	g_shmaddr_p = shmat( g_shmid, 0, 0 );	/* no addr hint, no flags */
	if (g_shmaddr_p == (char *)-1)
	    return (raise_exception("fd_xfer shmat"));
	/* create msg Q for reader to send info to writer */
	g_msgqid = msgget( IPC_PRIVATE, IPC_CREAT|0x1ff );
	/* create 1 semaphore for writer-to-allow-read   */
	g_semid = semget( IPC_PRIVATE, 1, IPC_CREAT|0x1ff );
    }
    else /* SPECIFIC/SPECIAL SHM INITIALIZED WITH SEM AND MSG */
    {   struct s_IPCshmgetObject *s_p = (struct s_IPCshmgetObject *)shm_obj_tp;
	rd_ahead_i = (s_p->i_p[0]-(10*sizeof(int))) / blk_size; /* do integer arithmatic */
	g_shmsize = blk_size * rd_ahead_i;
	g_shmid = s_p->i_p[1];
	g_shmaddr_p = (char *)&(s_p->i_p[10]);
	g_msgqid = s_p->i_p[3];
	g_semid  = s_p->i_p[2];
	read_bytes_ip  = &(s_p->i_p[4]);
	write_bytes_ip = &(s_p->i_p[5]);
    }

    msgctl( g_msgqid, IPC_STAT, &msgctl_s );
#   if 0 /* the default is the max size -- we can not set bigger and do not
	    need smaller */
    msgctl_s.msg_qbytes = (rd_ahead_i+1) * sizeof(struct s_msgdat);
    sts = msgctl( g_msgqid, IPC_SET, &msgctl_s );
    if (sts == -1)
    {   if (!shm_obj_tp) g_ipc_cleanup();
	return (raise_exception("fd_xfer msgctl"));
    }
#   endif

    /* == NOW DO THE WORK ===================================================*/

    sops_wr_wr2rd.sem_num = 0;  /* 1st and only sem */
    sops_wr_wr2rd.sem_op  = 1;  /* reader dec's, writer inc's */
    sops_wr_wr2rd.sem_flg = 0;  /* default - block behavior */

    /* init wr2rd */
    sops_wr_wr2rd.sem_op  = rd_ahead_i;
    sts = semop( g_semid, &sops_wr_wr2rd, 1 );
    if (sts == -1)
    {   if (!shm_obj_tp) g_ipc_cleanup();
	return (raise_exception("fd_xfer semop"));
    }
    sops_wr_wr2rd.sem_op  = 1;  /* reader dec's, writer inc's */

    /* fork off read (from) */
    if ((g_pid=fork()) == 0)
    {   PRINTF( "hello from reading forked process %d\n", getpid() );
	sts = do_read(  fr_fd, no_bytes, blk_size, crc_flag, crc_i, read_bytes_ip );
	exit( sts );
    }
    else
    {   int		 writing_flg=1;
	struct s_msg	 msg_s;
	
	while (writing_flg)
	{   /* read fifo - normal (blocking) */
 re_rcv:
	    sts = msgrcv(  g_msgqid, (struct s_msg *)&msg_s
			     , sizeof(struct s_msgdat), 0, 0 );
	    if ((sts==-1) && (errno==EINTR))
	    {   PRINTF(  "EXfer - (pid=%d) redoing writer rcv after interruption\n"
		       , getpid() );
		goto re_rcv;
	    }
	    if (sts == -1)
	    {   if (!shm_obj_tp) g_ipc_cleanup();
		kill( g_pid, 9 );
		waitpid( g_pid, &sts, 0 );
		return (raise_exception("fd_xfer - writer msgrcv"));
	    }
	    switch (msg_s.mtype)
	    {
	    case WrtSiz:
		sts = 0;
		do
		{   msg_s.md.data -= sts;
		    sts = write( to_fd, msg_s.md.c_p, msg_s.md.data );
		    if (sts == -1)
		    {   if (!shm_obj_tp) g_ipc_cleanup();
			kill( g_pid, 9 );
			waitpid( g_pid, &sts, 0 );
			return (raise_exception("fd_xfer - write"));
		    }
		    *write_bytes_ip += sts;
		} while (sts != msg_s.md.data);

 re_semop:
		sts = semop( g_semid, &sops_wr_wr2rd, 1 );
		if ((sts==-1) && (errno==EINTR))
		{   PRINTF(  "EXfer - (pid=%d) redoing writer semop after interruption\n"
			   , getpid() );
		    goto re_semop;
		}
		if (sts == -1)
		{   if (!shm_obj_tp) g_ipc_cleanup();
		    kill( g_pid, 9 );
		    waitpid( g_pid, &sts, 0 );
		    return (raise_exception("fd_xfer - write - semop"));
		}
		break;
	    case Err:
		if (!shm_obj_tp) g_ipc_cleanup();
		waitpid( g_pid, &sts, 0 );
		errno = msg_s.md.data;
		return (raise_exception("fd_xfer - read error"));
	    case Eof:
		if (!shm_obj_tp) g_ipc_cleanup();
		waitpid( g_pid, &sts, 0 );
		/* NOTE: string must be the same as in FTT -- it is must in
		   mover.py */
		return (raise_exception("fd_xfer - read EOF unexpected"));
	    default:		/* assume DatCrc */
		writing_flg = 0;	/* DONE! */
		crc_i = (unsigned int)msg_s.md.data;
		break;
	    }
	}
    }
    /* == DONE WITH THE WORK - CLEANUP ======================================*/

#   if DOSIGACT == 1
    /* MUST DO IN FORKED PROCESS ALSO??? */
    /*sigaction( SIGINT, &g_oldSigAct_s, (void *)0 );*/
    {   int ii;
	int sigs[] = SIGLIST;
	for (ii=0; ii<(sizeof(sigs)/sizeof(sigs[0])); ii++)
	{   sts = sigaction( sigs[ii], &(g_oldSigAct_sa[sigs[ii]]),(void *)0 );
	    if (sts == -1)
		printf( "(pid=%d) error with sig %d\n", getpid(), sigs[ii] );
	}
    }
#   endif

    if (!shm_obj_tp) g_ipc_cleanup();
    if (waitpid(g_pid,&sts,0) == -1)
	return (raise_exception("fd_xfer - waitpid"));

    if (crc_flag)
	rr = PyLong_FromUnsignedLong( crc_i );
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
    { "fd_xfer",  EXfd_xfer,  1, EXfd_xfer_Doc},
    { 0, 0}        /* Sentinel */
};

static char EXfer_Doc[] =  "EXfer is a module which Xfers data";

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

