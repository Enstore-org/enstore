/*  This file (FTT.c) was created by Ron Rechenmacher <ron@fnal.gov> on
    Oct 26, 1998. "TERMS AND CONDITIONS" governing this file are in the README
    or COPYING file. If you do not have such a file, one can be obtained by
    contacting Ron or Fermi Lab in Batavia IL, 60510, phone: 630-840-3000.
    $RCSfile$
    $Revision$
    $Date$
    */

#include "Python.h"

#include <stdio.h>		/* sprintf */
#include <sys/wait.h>		/* waitpid */
#include <sys/ipc.h>		/* shmxxx */
#include <sys/shm.h>		/* shmxxx */
#include <sys/sem.h>		/* semxxx */
#include <sys/msg.h>		/* msg{snd,rcv} */
#include <assert.h>		/* assert */
#include <signal.h>		/* sigaction() and struct sigaction */

#include "ftt.h"
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

static	PyObject	*FTTErrObject;
	ftt_descriptor	g_ftt_desc_tp = 0;
	int		g_blocksize = 1024;
	char		g_mode_c;
	char		*g_buf_p;
	int		g_buf_bytes = 0; /* number of bytes in buffer */
	ftt_stat_buf	g_stbuf_tp;
	int		g_xferred_bytes=0;

/*****************************************************************************
 */

static PyObject *
raise_exception( char *msg, int ftt_err )
{
        PyObject	*v;
        int		i = errno;

#   ifdef EINTR
    if ((i==EINTR) && PyErr_CheckSignals()) return NULL;
#   endif

    /* note: the 1st part of format should be the same as in EXfer.c */
    v = Py_BuildValue( "(s,i,s,i,i)", msg, i, strerror(i), getpid(), ftt_err );
    if (v != NULL)
    {   PyErr_SetObject( FTTErrObject, v );
	Py_DECREF(v);
    }
    return NULL;
}


/*****************************************************************************
 */

static PyObject *
raise_ftt_exception( char *msg )
{
	char		buf[2000];
	char		*ss;
	int		ftt_errno;

    ss = ftt_get_error( &ftt_errno ); /* Zero says do not return errno into pointer */
    sprintf( buf, "%s - %s", msg, ss );
    return raise_exception( buf, ftt_errno );
}


/*****************************************************************************
 *  set_debug( p1 - int - debug
 *               )
 */
static char FTT_set_debug_doc[] = "set the ftt debug level";

static PyObject*
FTT_set_debug(  PyObject *self
	      , PyObject *args )
{
	int	sts;

    sts = PyArg_ParseTuple( args, "i", &ftt_debug );
    if (!sts) return (NULL);

    return (Py_BuildValue(""));	/* return None */
}


/*****************************************************************************
 *  set_blocksize( p1 - int - blocksize
 *               )
 */
static char FTT_set_blocksize_doc[] = "set the blocksize (so we do not have to pass it as param to open";

static PyObject*
FTT_set_blocksize(  PyObject *self
		  , PyObject *args )
{
	int	new_blocksize;
	int	sts;

    sts = PyArg_ParseTuple( args, "i", &new_blocksize );
    if (!sts) return (NULL);

    if (new_blocksize != g_blocksize)
    {   free( g_buf_p );
	g_buf_p = malloc( 2*new_blocksize );
	if (!g_buf_p) return (raise_exception("set_blocksize",0));
	g_blocksize = new_blocksize;
    }

    return (Py_BuildValue(""));	/* return None */
}   /* set_blocksize */


/*****************************************************************************
 *  get_blocksize()
 */
static char FTT_get_blocksize_doc[] = "set the blocksize (so we do not have to pass it as param to open";

static PyObject*
FTT_get_blocksize(  PyObject *self
		  , PyObject *args )
{
    return (Py_BuildValue("i",g_blocksize));
}   /* get_blocksize */


/*****************************************************************************
 *  open( p1 - string - dev_name
 *        p2 - string - mode ('r' for read or 'w' for write)
 *      )
 */
static char FTT_open_doc[] = "invoke ftt_open and ftt_open_dev";

static PyObject*
FTT_open(  PyObject *self
	 , PyObject *args )
{
	char	*dev_s;
	char	*mode_s;
	int	sts;		/* general status */
	int retry_count;

    sts = PyArg_ParseTuple( args, "ss", &dev_s, &mode_s );
    if (!sts) return (NULL);

    if (g_ftt_desc_tp) return (raise_exception("FTT_Open - already open",0));

    if (mode_s[0] == 'r')
    {   g_ftt_desc_tp = ftt_open( dev_s, FTT_RDONLY );
	g_mode_c = 'r';
    }
    else /* assume write */
    {   g_ftt_desc_tp = ftt_open( dev_s, FTT_RDWR );
	g_mode_c = 'w';
    }
    retry_count = 0;
    sts=-1;
    /* somebody else may be doing an "ftt_status" or some other messing around with the drive... */
    while (retry_count<10 && sts<0){
	sts = ftt_open_dev( g_ftt_desc_tp );
	++retry_count;
	if (sts<0) sleep(1);
    }
    if (sts < 0) return (raise_ftt_exception("FTT_Open"));

    return (Py_BuildValue(""));	/* return None */
}


/*****************************************************************************
 */
static char FTT_open_only_doc[] = "invoke ftt_open only";

static PyObject*
FTT_open_only(  PyObject *self
	 , PyObject *args )
{
	char	*dev_s;
	char	*mode_s;
	int	sts;		/* general status */

    sts = PyArg_ParseTuple( args, "ss", &dev_s, &mode_s );
    if (!sts) return (NULL);

    if (g_ftt_desc_tp) return (raise_exception("FTT_open_only - already open",0));

    if (mode_s[0] == 'r')
    {   g_ftt_desc_tp = ftt_open( dev_s, FTT_RDONLY );
	g_mode_c = 'r';
    }
    else /* assume write */
    {   g_ftt_desc_tp = ftt_open( dev_s, FTT_RDWR );
	g_mode_c = 'w';
    }

    if (!g_ftt_desc_tp) return (raise_ftt_exception("FTT_open_only"));

    return (Py_BuildValue(""));	/* return None */
}


/*****************************************************************************
 */
static char FTT_open_dev_doc[] = "invoke ftt_open_dev";

static PyObject*
FTT_open_dev(  PyObject *self
	 , PyObject *args )
{
	char	*dev_s;
	char	*mode_s;
	int	sts;		/* general status */

    sts = PyArg_ParseTuple( args, "ss", &dev_s, &mode_s );
    if (!sts) return (NULL);

    if (!g_ftt_desc_tp) return (raise_exception("FTT_open_dev - not open",0));

    sts = ftt_open_dev( g_ftt_desc_tp );
    if (sts == -1) return (raise_ftt_exception("FTT_open_dev"));

    return (Py_BuildValue(""));	/* return None */
}


/*****************************************************************************
 *  close()
 */
static char FTT_close_doc[] = "invoke ftt_close";

static PyObject*
FTT_close(  PyObject *self
	  , PyObject *args )
{
    if (!g_ftt_desc_tp) return (Py_BuildValue("")); /* already closed is OK */

    if (g_buf_bytes && (g_mode_c=='w'))
    {   /* write out partial block */
	int sts;
	sts = ftt_write( g_ftt_desc_tp,  g_buf_p, g_buf_bytes );
	if (sts != g_buf_bytes)
	    return (raise_ftt_exception("close - partial block write did not complete"));
    }
    g_buf_bytes = 0;		/* always zero g_buf - next open will start
				 over! */
    ftt_close( g_ftt_desc_tp );
    g_ftt_desc_tp = 0;		/* indicate that we are closed! */
    return (Py_BuildValue(""));
}



/*****************************************************************************
 *  buf = read( p1 - int - no_bytes
 *            )
 */
static char FTT_read_doc[] = "invoke ftt_read";

static PyObject*
FTT_read(  PyObject *self
	 , PyObject *args )
{
	int		no_bytes;
	PyObject        *no_bytes_obj;
	PyObject	*ret_tp;
	int		sts;	/* general status */

    sts = PyArg_ParseTuple( args, "O", &no_bytes_obj );

    if (!sts) return (NULL);

    if (PyLong_Check(no_bytes_obj)) no_bytes = PyLong_AsUnsignedLong(no_bytes_obj);
    else if (PyInt_Check(no_bytes_obj)) no_bytes = (unsigned)PyInt_AsLong(no_bytes_obj);
    else return(raise_exception("FTT_read - invalid no_bytes param",0));

    if (!g_ftt_desc_tp) return (raise_exception("FTT_read device not opened",0));

    if (no_bytes <= g_buf_bytes)
    {   /* we already have the data */
	int ii;

	ret_tp = Py_BuildValue( "s#", g_buf_p, no_bytes );
	for (ii=0; ii<(g_buf_bytes-no_bytes); ii++)
	    g_buf_p[ii] = g_buf_p[ii+no_bytes];
	g_buf_bytes -= no_bytes;
    }
    else if (no_bytes <= g_blocksize)
    {   /* do not have all the data, but can use global buffer */
	int ii, xx;

	while (g_buf_bytes < no_bytes)
	{   sts = ftt_read(  g_ftt_desc_tp, &g_buf_p[g_buf_bytes]
			   , g_blocksize );
	    if (sts == 0)
	    {   /* eof so give them what we have */
		break;
	    }
	    else if (sts == -1) return (raise_ftt_exception("read"));

	    g_buf_bytes += sts;
	}

	xx = (no_bytes<g_buf_bytes)? no_bytes: g_buf_bytes;
	ret_tp = Py_BuildValue( "s#", g_buf_p, xx );
	for (ii=0; ii<g_buf_bytes-xx; ii++)
	    g_buf_p[ii] = g_buf_p[xx+ii];
	g_buf_bytes -= xx;
    }
    else /* need to malloc an area (> g_blocksize) for all the data */
    {   char *buf_p = malloc( no_bytes+g_blocksize );
	int  bytes_to_give = g_buf_bytes;

	while (g_buf_bytes--) buf_p[g_buf_bytes] = g_buf_p[g_buf_bytes];
	while (bytes_to_give < no_bytes)
	{   sts = ftt_read(  g_ftt_desc_tp, &buf_p[bytes_to_give]
			   , g_blocksize );
	    if (sts == 0)
	    {   /* eof so give them what we have */
		ret_tp = Py_BuildValue( "s#", buf_p, bytes_to_give );
		break;
	    }
	    else if (sts == -1) return (raise_ftt_exception("read"));
	    bytes_to_give += sts;
	}

	ret_tp = Py_BuildValue( "s#", buf_p, no_bytes );

	/* save excess */
	g_buf_bytes = bytes_to_give - no_bytes;
	while (bytes_to_give-- > no_bytes)
	    g_buf_p[bytes_to_give-no_bytes] = buf_p[bytes_to_give];

	free( buf_p );
    }

    return (ret_tp);
}



/*****************************************************************************
 *  write( p1 - string/len - buffer to write
 *       )
 */
static char FTT_write_doc[] = "invoke ftt_write";

static PyObject*
FTT_write(  PyObject *self
	  , PyObject *args )
{
	char	*buf_p;
	int	no_bytes;
	int	sts;

    sts = PyArg_ParseTuple(args, "s#", &buf_p, &no_bytes );
    if (!sts) return (NULL);

    if (!g_ftt_desc_tp) return (raise_exception("FTT_write device not opened",0));

    if (g_buf_bytes)
    {   /* I have a *partial block* to deal with */
	if ((no_bytes+g_buf_bytes) <= (2*g_blocksize))
	{   /* I have room in global buffer */
	    int tt = no_bytes;
	    while (no_bytes--)
		g_buf_p[g_buf_bytes+no_bytes] = buf_p[no_bytes];
	    g_buf_bytes += tt;
	    if (g_buf_bytes >= g_blocksize)
	    {   int ii;
		sts = ftt_write(  g_ftt_desc_tp, g_buf_p, g_blocksize );
		if (sts != g_blocksize) return (raise_ftt_exception("write"));
		/* shift the bytes down */
		for (ii=0; ii<g_blocksize; ii++)
		    g_buf_p[ii] = g_buf_p[g_blocksize+ii];
		g_buf_bytes -= sts;
	    }
	}
	else
	{   /* copy enough bytes from user buf_p tp make a complete block,
	       xfer that block, then start again in user buf_p. Then
	       save remaining partial block in g_buf_p */
	    no_bytes -= g_blocksize-g_buf_bytes;
	    while (g_buf_bytes < g_blocksize)
		g_buf_p[g_buf_bytes++] = *(buf_p++);

	    sts = ftt_write(  g_ftt_desc_tp, g_buf_p, g_blocksize );
	    if (sts != g_blocksize) return (raise_ftt_exception("write"));

	    while (no_bytes >= g_blocksize)
	    {   sts = ftt_write(  g_ftt_desc_tp, buf_p, g_blocksize );
		if (sts != g_blocksize) return (raise_ftt_exception("write"));
		buf_p += sts;
		no_bytes -= sts;
	    }

	    g_buf_bytes = no_bytes;
	    while (no_bytes--) g_buf_p[no_bytes] = buf_p[no_bytes];
	}
    }
    else
    {   /* just write from user buffer, then save excess */
	int bytes_written = 0;
	while (no_bytes >= g_blocksize)
	{
	    sts = ftt_write(  g_ftt_desc_tp, &buf_p[bytes_written]
			    , g_blocksize );
	    if (sts != g_blocksize) return (raise_ftt_exception("write"));
	    no_bytes -= sts;
	    bytes_written += sts;
	}
	/* save left partial block */
	g_buf_bytes = no_bytes;
	while (no_bytes--) g_buf_p[no_bytes] = buf_p[bytes_written+no_bytes];
    }

    return (Py_BuildValue(""));
}   /* FTT_write */



/*****************************************************************************
 *  fd_xfer( p1 - int - fileno
 *           p2 - int - no_bytes to xfer
 *           p3 - obj - crc object
 *	     p4 - obj - shm object (see IPC.c)
 *         )
 */

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
    {   PRINTF(  "FTT (pid=%d) - redoing reader snd after interruption\n"
	       , getpid() );
	goto re_snd;
    }
    if (sts == -1)
    {   perror( "FTT: fatal reader (send_writer) error" );
        /* printf("%d %d %d %s %d \n",g_msgqid,mtype,d1,c1,sizeof(struct s_msgdat)); */
        /*   system("/usr/bin/ipcs"); */
	/* can not do much more, but lets try exitting */
	exit( 1 );
    }
    return;
}

static int
do_read(  int 		rd_fd
	, unsigned int	no_bytes
	, int 		blk_size /* is g_blocksize */
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
    nice( 10 ); /*XXX is this really a good idea */

    /* first copy the g_buf_bytes into the shm */
    {   int ii=g_buf_bytes;
	while (ii--) g_shmaddr_p[ii] = g_buf_p[ii];
    }

    while (no_bytes)
    {
	int	sts, shm_bytes=g_buf_bytes;

	/* gain access to *blk* of shared mem */
 semop_try:
	sts = semop( g_semid, &sops_rd_wr2rd, 1 );
	if ((sts==-1) && (errno==EINTR))
	{   PRINTF(  "(pid=%d) interrupted system call; assume ctl-Z OR debugger attach???\n"
		   , getpid() );
	    goto semop_try;
	}
	if (sts == -1) { printf("%s %d\n",__FILE__,__LINE__);send_writer( Err, errno, 0 ); return (1); }

	if (rd_fd) /* i.e. if 'w' to HSM */
	{   /* read from network OR a file -- but, as we will be writing to
	       HSM, make sure we get what we ask for */
	    /* it is ok to do partial block read from user! */
	    int	ask;
	    ask = (no_bytes<(blk_size-g_buf_bytes))
		? no_bytes
		: (blk_size-g_buf_bytes);

	    sts = 0;
	    do
	    {   ask -= sts;
		sts = read(  rd_fd, g_shmaddr_p+shm_off+shm_bytes
			   , ask );
		if (sts == -1) { printf("%s %d\n",__FILE__,__LINE__);send_writer( Err, errno, 0 ); return (1); }
		if (sts ==  0) { send_writer( Eof, errno, 0 ); return (1); }
		*read_bytes_ip += sts;
		shm_bytes += sts;
		no_bytes -= sts;
	    } while (ask != sts);
	    /* write (writing to tape) will store partial block in g_buf_p */
	    send_writer( WrtSiz, shm_bytes, g_shmaddr_p+shm_off );
	    /* do not crc g_buf_bytes */
	    switch (crc_flag){
	    case 0:
		break;
	    case 1:
		crc_i=adler32(crc_i,g_shmaddr_p+shm_off+g_buf_bytes,
			      shm_bytes-g_buf_bytes);
		break;
	    default:
		printf("fd_xfer: invalid crc flag");
	    }

	    shm_off += shm_bytes;
	    shm_bytes = 0;
	}
	else
	{   /* g_buf_bytes are to be used/crc-ed to fullfill user request */
	    int	user_bytes;
	    if (g_buf_bytes)
	    {   sts = g_buf_bytes; /* note: write will know what to do with
				      excess */
		user_bytes = (no_bytes<g_buf_bytes)? no_bytes: g_buf_bytes;
	    }
	    else
	    {
		sts = ftt_read(  g_ftt_desc_tp, g_shmaddr_p+shm_off
			       , g_blocksize );
		if (sts == -1)
		{   printf( "ftt_read error %s\n", ftt_get_error(0) );
		    printf("%s %d\n",__FILE__,__LINE__);send_writer( Err, errno, 0 ); return (1);
		}
		user_bytes = (no_bytes<g_blocksize)? no_bytes: g_blocksize;
	    }
	    send_writer( WrtSiz, sts, g_shmaddr_p+shm_off );
	    /* some or all of g_buf_bytes are to be crc-ed */
	    switch (crc_flag){
	    case 0:
		break;
	    case 1:
		crc_i=adler32(crc_i,g_shmaddr_p+shm_off, user_bytes);
		break;
	    default:
		printf("fd_xfer: invalid crc flag");
	    }
	    *read_bytes_ip += user_bytes;
	    no_bytes -= user_bytes;
	    shm_off += sts;
	}

	/* should be done with it; zero-ing is OK --
	   it is just a flag at this point */
	g_buf_bytes = 0;

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

static char FTT_fd_xfer_doc[] = "invoke ftt_fd_xfer";

static PyObject*
FTT_fd_xfer(  PyObject *self
	    , PyObject *args )
{
	int		fd;	    /* \   */
	int		no_bytes;   /*  \  */
	PyObject        *no_bytes_obj;
	PyObject	*crc_fun_tp;/*   } no optional args (compare to EXfer) */
	PyObject	*crc_tp;    /*  /  */
	PyObject	*shm_tp;    /* /   */

	unsigned long    crc_i=0;
	int              crc_flag=0;
	int		 sts;	/* general status */
	struct sigaction newSigAct_sa[32];
	int		 rd_ahead_i;
	PyObject	*rr;
	struct msqid_ds	 msgctl_s;
	struct sembuf	 sops_wr_wr2rd;  /* allows read */
	int		 dummy=0;
	int		*read_bytes_ip=&dummy, *write_bytes_ip=&dummy;

    sts = PyArg_ParseTuple(  args, "iOOOO", &fd, &no_bytes_obj, &crc_fun_tp, &crc_tp
			   , &shm_tp );
    if (!sts) return (NULL);

    if (PyLong_Check(no_bytes_obj)) no_bytes = PyLong_AsUnsignedLong(no_bytes_obj);
    else if (PyInt_Check(no_bytes_obj)) no_bytes = (unsigned)PyInt_AsLong(no_bytes_obj);
    else return(raise_exception("FTT_fd_xfer - invalid no_bytes param",0));


    if (!g_ftt_desc_tp) return (raise_exception("FTT_fd_xfer device not opened",0));

    if      (crc_tp == Py_None)   crc_i = 0;
    else if (PyLong_Check(crc_tp)) crc_i = PyLong_AsUnsignedLong(crc_tp);
    else if (PyInt_Check(crc_tp)) crc_i = (unsigned long) PyInt_AsLong( crc_tp );
    else return(raise_exception("fd_xfer - invalid crc param",0));

    if (crc_fun_tp==Py_None) crc_flag=0;
    else if (PyInt_Check(crc_fun_tp)) crc_flag = PyInt_AsLong(crc_fun_tp);
    else return raise_exception("FTT_fd_xfer: invalid crc flag",0);
    if (crc_flag<0 || crc_flag>1)
	return raise_exception("FTT_fd_xfer: invalid crc flag",0);

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

    assert( g_blocksize < 0x400000 );
    {   /* get IPC stuff from object */
	struct s_IPCshmgetObject *s_p = (struct s_IPCshmgetObject *)shm_tp;
	rd_ahead_i = (s_p->i_p[0]-(10*sizeof(int))) / g_blocksize; /* do integer arithmatic */
	g_shmsize = g_blocksize * rd_ahead_i;
	g_shmid = s_p->i_p[1];
	g_shmaddr_p = (char *)&(s_p->i_p[10]);
	g_msgqid = s_p->i_p[3];
	g_semid  = s_p->i_p[2];
	read_bytes_ip  = &(s_p->i_p[4]);
	write_bytes_ip = &(s_p->i_p[5]);
    }

    msgctl( g_msgqid, IPC_STAT, &msgctl_s );
    if (sts == -1) return (raise_exception("fd_xfer msgctl IPC_STAT",0));
#   if 0 /* the default is the max size -- we can not set bigger and do not
	    need smaller */
    msgctl_s.msg_qbytes = (rd_ahead_i+1) * sizeof(struct s_msgdat);
    sts = msgctl( g_msgqid, IPC_SET, &msgctl_s );
    if (sts == -1) return (raise_exception("fd_xfer msgctl IPC_SET",0));
#   endif

    /* == NOW DO THE WORK ===================================================*/

    sops_wr_wr2rd.sem_num = 0;  /* 1st and only sem */
    sops_wr_wr2rd.sem_op  = 1;  /* reader dec's, writer inc's */
    sops_wr_wr2rd.sem_flg = 0;  /* default - block behavior */

    /* init wr2rd */
    sops_wr_wr2rd.sem_op  = rd_ahead_i;
    sts = semop( g_semid, &sops_wr_wr2rd, 1 );
    if (sts == -1) return (raise_exception("fd_xfer semop",0));
    sops_wr_wr2rd.sem_op  = 1;  /* reader dec's, writer inc's */

    /* fork off read (from) */
    if ((g_pid=fork()) == 0)
    {   PRINTF( "hello from reading forked process %d\n", getpid() );
	sts = do_read( (g_mode_c=='r')?0:fd, no_bytes, g_blocksize, crc_flag
		      , crc_i, read_bytes_ip );
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
	    {   PRINTF(  "FTT - (pid=%d) redoing writer rcv after interruption\n"
		       , getpid() );
		goto re_rcv;
	    }
	    if (sts == -1) 
	    {   kill( g_pid, 9 );
		waitpid( g_pid, &sts, 0 );
		return (raise_exception("fd_xfer - writer msgrcv",0));
	    }

	    switch (msg_s.mtype)
	    {
	    case WrtSiz:
		if (g_mode_c == 'w')
		{   /* I know a read (i.e from net) can return less than */
		    /* requested, but when would a write??? */
		    if (msg_s.md.data != g_blocksize)
		    {   assert( msg_s.md.data < g_blocksize );
			sts = msg_s.md.data; /* use temporarily */
			while (sts--)
			    g_buf_p[sts] = msg_s.md.c_p[sts];
			g_buf_bytes = msg_s.md.data;
			/* NEXT MESSAGE SHOULD/BETTER BE CRC */
		    }
		    else
		    {   sts = ftt_write( g_ftt_desc_tp, msg_s.md.c_p, msg_s.md.data );
			if (sts != msg_s.md.data) return (raise_ftt_exception("fd_xfer - write"));
		    }
		    *write_bytes_ip += sts;	/* count up */
		    no_bytes -= sts; /* count down */
		}
		else
		{   /* writing to user */
		    /* recall cases:
		       1) user's request can be filled from g_buf_p (no need for reader to read from tape)
		       2) user's request can not filled from g_buf_p (reader needs to get *block* from tape)
		          2a) have just what we need
			  2b) have extra (save extra in g_buf_p)
		    */
		    sts = 0;
		    do
		    {   
			msg_s.md.data -= sts;
			msg_s.md.c_p  += sts;
			sts = write( fd, msg_s.md.c_p
				    , (no_bytes<msg_s.md.data)?no_bytes:msg_s.md.data );
			if (sts == -1) return (raise_exception("fd_xfer - write",0));
			if (no_bytes < msg_s.md.data)
			{   /* left over goes into g_buf_p */
			    int ii=msg_s.md.data-no_bytes;
			    while (ii--)
				g_buf_p[ii] = msg_s.md.c_p[no_bytes+ii];
			    g_buf_bytes = msg_s.md.data-no_bytes;
			}
			no_bytes -= sts; /* count down */
			*write_bytes_ip += sts;	/* count up */
		    } while ((sts!=msg_s.md.data) && (no_bytes > 0));
		}

 re_semop:
		sts = semop( g_semid, &sops_wr_wr2rd, 1 );
		if ((sts==-1) && (errno==EINTR))
		{   PRINTF(  "FTT - (pid=%d) redoing writer semop after interruption\n"
			   , getpid() );
		    goto re_semop;
		}
		if (sts == -1)
		{   kill( g_pid, 9 );
		    waitpid( g_pid, &sts, 0 );
		    return (raise_exception("fd_xfer - write - semop",0));
		}
		break;
	    case Err:
		waitpid( g_pid, &sts, 0 );
		errno = msg_s.md.data;
		return (raise_exception("fd_xfer - read error",0));
	    case Eof:
		waitpid( g_pid, &sts, 0 );
		/* NOTE: string must be the same as in EXfer -- it is must in
		   mover.py */
		return (raise_exception("fd_xfer - read EOF unexpected",0));
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

    if (waitpid(g_pid,&sts,0) == -1)
	return (raise_exception("fd_xfer - waitpid",0));

    if (crc_flag)
	rr = PyLong_FromUnsignedLong(crc_i);
    else
	rr = Py_BuildValue( "" );
    return (rr);
}



/*****************************************************************************
 *  writefm)
 */
static char FTT_writefm_doc[] = "invoke ftt_writefm";

static PyObject*
FTT_writefm(  PyObject *self
	  , PyObject *args )
{
	int	sts;

    if (!g_ftt_desc_tp) return (raise_exception("FTT_writefm device not opened",0));

    /* just like close */
    if (g_buf_bytes && (g_mode_c=='w'))
    {   /* write out partial block */
	sts = ftt_write( g_ftt_desc_tp,  g_buf_p, g_buf_bytes );
	if (sts != g_buf_bytes) return (raise_ftt_exception("FTT_writefm - partial block write"));
    }
    g_buf_bytes = 0;		/* always zero g_buf - next open will start
				 over! */

    sts = ftt_writefm( g_ftt_desc_tp );
    if (sts == -1) return (raise_ftt_exception("FTT_writefm"));

    return (Py_BuildValue(""));
}



/*****************************************************************************
 */
static char FTT_skip_fm_doc[] = "invoke ftt_skip_fm";

static PyObject*
FTT_skip_fm(  PyObject *self
	 , PyObject *args )
{
	int	skip;		/* no file marks to skip */
	int	sts;		/* general status */

    sts = PyArg_ParseTuple(args, "i", &skip );
    if (!sts) return (NULL);

    if (!g_ftt_desc_tp) return (raise_exception("FTT_skip_fm device not opened",0));

    sts = ftt_skip_fm( g_ftt_desc_tp, skip );
    if (sts == -1) return (raise_ftt_exception("FTT_skipfm"));
    return (Py_BuildValue(""));
}



/*****************************************************************************
 */
static char FTT_skip_rec_doc[] = "invoke ftt_skip_rec";

static PyObject*
FTT_skip_rec(  PyObject *self
	 , PyObject *args )
{
	int	skip;		/* no file marks to skip */
	int	sts;		/* general status */

    sts = PyArg_ParseTuple(args, "i", &skip );
    if (!sts) return (NULL);

    if (!g_ftt_desc_tp) return (raise_exception("FTT_skip_rec device not opened",0));

    sts = ftt_skip_rec( g_ftt_desc_tp, skip );
    if (sts == -1) return (raise_ftt_exception("FTT_skipfm"));
    return (Py_BuildValue(""));
}



/*****************************************************************************
 */
static char FTT_locate_doc[] = "invoke ftt_locate";

static PyObject*
FTT_locate(  PyObject *self
	   , PyObject *args )
{
	int	locate;		/* the block number to move to */
	int	part=0;
	int	sts;		/* general status */

    sts = PyArg_ParseTuple(args, "i|i", &locate, &part );
    if (!sts) return (NULL);

    if (!g_ftt_desc_tp) return (raise_exception("FTT_locate device not opened",0));

#   ifdef NO_PARTITION_SUPPORT
    sts = ftt_scsi_locate( g_ftt_desc_tp, locate );
#   else
    sts = ftt_scsi_locate( g_ftt_desc_tp, locate, part );
#   endif
    if (sts == -1) return (raise_ftt_exception("FTT_locate"));

    return (Py_BuildValue(""));
}



/*****************************************************************************
 */
static char FTT_rewind_doc[] = "invoke ftt_rewind";

static PyObject*
FTT_rewind(  PyObject *self
	   , PyObject *args )
{
	int	sts;		/* general status */

    /* if (!PyArg_NoArgs(args)) return (NULL); from Modules/socketmodule.c but
       why is it returning 0??? */

    if (!g_ftt_desc_tp) return (raise_exception("FTT_rewind device not opened",0));

    sts = ftt_rewind( g_ftt_desc_tp );
    if (sts == -1) return (raise_ftt_exception("FTT_rewind"));

    Py_INCREF(Py_None); return (Py_None);
}



/*****************************************************************************
 */
static char FTT_dump_stats_doc[] = "invoke ftt_dump_stats";

static PyObject*
FTT_dump_stats(  PyObject *self
	      , PyObject *args )
{
	int		sts;	/* general status */
	PyObject	*rr;
	FILE            *fptr;
	PyObject        *pyt_fptr = 0;
	

    if (!g_ftt_desc_tp) return (raise_exception("FTT_dump_stats device not opened",0));

    sts = PyArg_ParseTuple(args, "O", &pyt_fptr );
    if (!sts) return (NULL);
    
    if (!PyFile_Check(pyt_fptr)) {
        PyErr_SetString(PyExc_TypeError, "Need A File!");
	return NULL;
    }
    
    fptr = PyFile_AsFile(pyt_fptr);

#   define GG g_stbuf_tp
    sts = ftt_get_stats( g_ftt_desc_tp, GG );
    if (sts == -1) return raise_ftt_exception( "FTT_dump_stats" );
    rr = Py_BuildValue("i", ftt_dump_stats(GG,fptr));
    return (rr);
}


/*****************************************************************************
 */
static char FTT_get_statsAll_doc[] = "invoke ftt_get_statsAll";

static PyObject*
FTT_get_statsAll(  PyObject *self
	      , PyObject *args )
{
	int		sts, i;	/* general status */
	PyObject	*rr;
	char            *bvFormat;
	

    if (!g_ftt_desc_tp) return (raise_exception("FTT_get_statsAll device not opened",0));

#   define GG g_stbuf_tp
    sts = ftt_get_stats( g_ftt_desc_tp, GG );
    if (sts == -1) return raise_ftt_exception( "FTT_get_statsAll" );

    bvFormat = malloc(4*56+6);
    bvFormat = strncpy(bvFormat,"{s:i",5);
    for (i=0;i<56;i++) {
        bvFormat = strncat(bvFormat,",s:s",4);
    }
    bvFormat = strncat(bvFormat,"}",1);
    rr = Py_BuildValue( bvFormat
             , "xferred_bytes", g_xferred_bytes
             , "remain_tape",     ftt_extract_stats(GG,FTT_REMAIN_TAPE)
             , "n_reads",         ftt_extract_stats(GG,FTT_N_READS)
             , "read_errors",     ftt_extract_stats(GG,FTT_READ_ERRORS)
             , "file_number",     ftt_extract_stats(GG,FTT_FILE_NUMBER)
             , "block_number",    ftt_extract_stats(GG,FTT_BLOCK_NUMBER)
             , "bloc_loc",        ftt_extract_stats(GG,FTT_BLOC_LOC)
             , "serial_num",      ftt_extract_stats(GG,FTT_SERIAL_NUM)
             , "cleaning_bit",    ftt_extract_stats(GG,FTT_CLEANING_BIT)
             , "fmk",             ftt_extract_stats(GG,FTT_FMK)
             , "vendor_id",       ftt_extract_stats(GG,FTT_VENDOR_ID)
             , "product_id",      ftt_extract_stats(GG,FTT_PRODUCT_ID)
             , "firmware_rev",    ftt_extract_stats(GG,FTT_FIRMWARE)
             , "read_count",      ftt_extract_stats(GG,FTT_READ_COUNT)
             , "write_count",     ftt_extract_stats(GG,FTT_WRITE_COUNT)
             , "write_errors",    ftt_extract_stats(GG,FTT_WRITE_ERRORS)
             , "read_comp",       ftt_extract_stats(GG,FTT_READ_COMP)
             , "write_comp",      ftt_extract_stats(GG,FTT_WRITE_COMP)
             , "bot",             ftt_extract_stats(GG,FTT_BOT)
             , "ready",           ftt_extract_stats(GG,FTT_READY)
             , "write_prot",      ftt_extract_stats(GG,FTT_WRITE_PROT)
             , "eom",             ftt_extract_stats(GG,FTT_EOM)
             , "peot",            ftt_extract_stats(GG,FTT_PEOT)
             , "media_type",      ftt_extract_stats(GG,FTT_MEDIA_TYPE)
             , "block_size",      ftt_extract_stats(GG,FTT_BLOCK_SIZE)
             , "block_total",     ftt_extract_stats(GG,FTT_BLOCK_TOTAL)
             , "trans_density",   ftt_extract_stats(GG,FTT_TRANS_DENSITY)
             , "trans_compress",  ftt_extract_stats(GG,FTT_TRANS_COMPRESS)
             , "user_read",       ftt_extract_stats(GG,FTT_USER_READ)
             , "user_write",      ftt_extract_stats(GG,FTT_USER_WRITE)
             , "controller",      ftt_extract_stats(GG,FTT_CONTROLLER)
             , "density",         ftt_extract_stats(GG,FTT_DENSITY)
             , "invalid_length",  ftt_extract_stats(GG,FTT_ILI)
             , "scsi_asc",        ftt_extract_stats(GG,FTT_SCSI_ASC)
             , "scsi_ascq",       ftt_extract_stats(GG,FTT_SCSI_ASCQ)
             , "power_failed",    ftt_extract_stats(GG,FTT_PF)
             , "cleaned_bit",     ftt_extract_stats(GG,FTT_CLEANED_BIT)
             , "track_retry",     ftt_extract_stats(GG,FTT_TRACK_RETRY)
             , "underrun",        ftt_extract_stats(GG,FTT_UNDERRUN)
             , "motion_hours",    ftt_extract_stats(GG,FTT_MOTION_HOURS)
             , "power_hours",     ftt_extract_stats(GG,FTT_POWER_HOURS)
             , "tur_status",      ftt_extract_stats(GG,FTT_TUR_STATUS)
             , "count_origin",    ftt_extract_stats(GG,FTT_COUNT_ORIGIN)
             , "n_writes",        ftt_extract_stats(GG,FTT_N_WRITES)
             , "tape_not_present",ftt_extract_stats(GG,FTT_TNP)
             , "sense_key",       ftt_extract_stats(GG,FTT_SENSE_KEY)
             , "trans_sense_key", ftt_extract_stats(GG,FTT_TRANS_SENSE_KEY)
             , "retries",         ftt_extract_stats(GG,FTT_RETRIES)
             , "fail_retries",    ftt_extract_stats(GG,FTT_FAIL_RETRIES)
             , "resets",          ftt_extract_stats(GG,FTT_RESETS)
             , "hard_errors",     ftt_extract_stats(GG,FTT_HARD_ERRORS)
             , "unc_write",       ftt_extract_stats(GG,FTT_UNC_WRITE)
             , "unc_read",        ftt_extract_stats(GG,FTT_UNC_READ)
             , "cmp_write",       ftt_extract_stats(GG,FTT_CMP_WRITE)
             , "cmp_read",        ftt_extract_stats(GG,FTT_CMP_READ)
             , "error_code",      ftt_extract_stats(GG,FTT_ERROR_CODE)
             , "max_stat",        ftt_extract_stats(GG,FTT_MAX_STAT)
	      );
    free(bvFormat);
    return (rr);
}


/*****************************************************************************
 */
static char FTT_get_stats_doc[] = "invoke ftt_get_stats";

static PyObject*
FTT_get_stats(  PyObject *self
	      , PyObject *args )
{
	int		sts;	/* general status */
	PyObject	*rr;

    if (!g_ftt_desc_tp) return (raise_exception("FTT_get_stats device not opened",0));

#   define GG g_stbuf_tp
    sts = ftt_get_stats( g_ftt_desc_tp, GG );
    if (sts == -1) return raise_ftt_exception( "FTT_get_stats" );

    rr = Py_BuildValue(  "{s:i,s:s,s:s,s:s,s:s,s:s,s:s,s:s,s:s,s:s}"
		       , "xferred_bytes",g_xferred_bytes
		       , "remain_tape",  ftt_extract_stats(GG,FTT_REMAIN_TAPE)
		       , "n_reads",      ftt_extract_stats(GG,FTT_N_READS)
		       , "read_errors",  ftt_extract_stats(GG,FTT_READ_ERRORS)
		       , "file_number",  ftt_extract_stats(GG,FTT_FILE_NUMBER)
		       , "block_number", ftt_extract_stats(GG,FTT_BLOCK_NUMBER)
		       , "bloc_loc",     ftt_extract_stats(GG,FTT_BLOC_LOC)
		       , "serial_num",   ftt_extract_stats(GG,FTT_SERIAL_NUM)
		       , "cleaning_bit", ftt_extract_stats(GG,FTT_CLEANING_BIT)
		       , "fmk",          ftt_extract_stats(GG,FTT_FMK) );
    return (rr);
}



/*****************************************************************************
 */
static char FTT_status_doc[] = "invoke ftt_stats";

static PyObject*
FTT_status(  PyObject *self
	   , PyObject *args )
{
	int		timeout;
	int		sts;	/* general status */
	PyObject	*rr;

    sts = PyArg_ParseTuple(args, "i", &timeout );
    if (!sts) return (NULL);

    if (!g_ftt_desc_tp) return (raise_exception("FTT_status device not opened",0));

    sts = ftt_status( g_ftt_desc_tp, timeout );
    if (sts == -1) return raise_ftt_exception( "FTT_status" );

    rr = Py_BuildValue(  "{s:i,s:i,s:i,s:i,s:i,s:i}"
		       , "ABOT",   (sts&FTT_ABOT)?1:0
		       , "AEOT",   (sts&FTT_AEOT)?1:0
		       , "AEW",    (sts&FTT_AEW)?1:0
		       , "PROT",   (sts&FTT_PROT)?1:0
		       , "ONLINE", (sts&FTT_ONLINE)?1:0
		       , "BUSY",   (sts&FTT_BUSY)?1:0 );
    return (rr);
}



/*****************************************************************************
 */
static char FTT_unload_doc[] = "invokes ftt_unload";

static PyObject*
FTT_unload(  PyObject *self
	   , PyObject *args )
{
	int		sts;	/* general status */

    if (!g_ftt_desc_tp) return (raise_exception("FTT_unload device not opened",0));

    sts = ftt_unload( g_ftt_desc_tp );
    if (sts == -1) return (raise_ftt_exception("FTT_unload"));

    Py_INCREF(Py_None); return (Py_None);
}



/*****************************************************************************
 */
static char FTT_get_mode_doc[] = "invokes ftt_get_mode";

static PyObject*
FTT_get_mode(  PyObject *self
	     , PyObject *args )
{
	int		density, cmp, blocksize;
	char		*dev_name;
	PyObject	*rr;

    if (!g_ftt_desc_tp) return (raise_exception("FTT_get_mode device not opened",0));

    dev_name = ftt_get_mode( g_ftt_desc_tp, &density, &cmp, &blocksize );

    rr = Py_BuildValue(  "{s:s,s:i,s:i,s:i}"
		       , "system_device",dev_name
		       , "density",      density
		       , "compression",  cmp      
		       , "blocksize",    blocksize );
    return (rr);
}   /* FTT_get_mode */



/*****************************************************************************
 */
static char FTT_set_mode_doc[] = "invokes ftt_set_mode";

static PyObject*
FTT_set_mode(  PyObject *self
	     , PyObject *args )
{
	int		sts;	/* general status */
	int		density, cmp, blocksize;
	char		*dev_name;
	PyObject	*rr;

    sts = PyArg_ParseTuple(args, "iii", &density, &cmp, &blocksize );
    if (!sts) return (NULL);

    if (!g_ftt_desc_tp) return (raise_exception("FTT_set_mode device not opened",0));

    dev_name = ftt_set_mode( g_ftt_desc_tp, density, cmp, blocksize );

    rr = Py_BuildValue( "s", dev_name );
    return (rr);
}   /* FTT_set_mode */



/*****************************************************************************
 */
static char FTT_version_doc[] = "decifer version for $FTT_DIR at build";

#define STRsup(x) #x
#define STR(x) STRsup(x)

static PyObject*
FTT_version(  PyObject *self
	     , PyObject *args )
{
	PyObject	*rr=0;
	char		*ftt_path;
	int		i;
	char		*p_elem[2];

    /* attempt to go back 2 path elements.
       from there, try to match the [xv][0-9]+_[0-9]+ format */

    ftt_path = malloc( strlen(STR(FTT_DIR)) );
    strcpy( ftt_path, STR(FTT_DIR) );

    i = strlen( ftt_path );
    i--;
    /* remove trailing '/' */
    if (ftt_path[i] == '/')
    {   ftt_path[i] = '\0';
	i--;
    }

    /* get last to path elements */
    while (i && (ftt_path[i]!='/')) i--;
    p_elem[0] = &ftt_path[i+1];
    ftt_path[i] = '\0';
    while (i && (ftt_path[i]!='/')) i--;
    p_elem[1] = &ftt_path[i+1];

    /* check for [xv][0-9]+_[0-9]+ format */
    if (   (p_elem[1][0] == 'v')
	|| (p_elem[1][0] == 'x'))
    {   char *c_p = p_elem[1];
	while (*c_p)
	{   if (*c_p++ == '_')
	    {   rr = Py_BuildValue( "s", p_elem[1] );
		break;
	    }
	}
    }
    if (! rr) rr = Py_BuildValue( "s", p_elem[0] );

    free( ftt_path );
    return (rr);
}   /* FTT_version */



/*****************************************************************************
 */

static void
insint(  PyObject	*d
       , char		*name
       , int		value )
{
    PyObject *v = PyInt_FromLong((long) value);
    if (!v || PyDict_SetItemString(d,name,v))
	PyErr_Clear();

    Py_XDECREF(v);
}

static PyMethodDef FTT_Methods[] = {
    { "set_debug", FTT_set_debug, 1, FTT_set_debug_doc },
    { "set_blocksize", FTT_set_blocksize, 1, FTT_set_blocksize_doc },
    { "get_blocksize", FTT_get_blocksize, 1, FTT_get_blocksize_doc },
    { "open", FTT_open, 1, FTT_open_doc },
    { "open_only", FTT_open_only, 1, FTT_open_only_doc },
    { "open_dev", FTT_open_dev, 1, FTT_open_dev_doc },
    { "close", FTT_close, 1, FTT_close_doc },
    { "read", FTT_read, 1, FTT_read_doc },
    { "write", FTT_write, 1, FTT_write_doc },
    { "fd_xfer", FTT_fd_xfer, 1, FTT_fd_xfer_doc },
    { "writefm", FTT_writefm, 1, FTT_writefm_doc },
    { "skip_fm", FTT_skip_fm, 1, FTT_skip_fm_doc },
    { "skip_rec", FTT_skip_rec, 1, FTT_skip_rec_doc },
    { "locate", FTT_locate, 1, FTT_locate_doc },
    { "rewind", FTT_rewind, 1, FTT_rewind_doc },
    { "dump_stats", FTT_dump_stats, 1, FTT_dump_stats_doc },
    { "get_statsAll", FTT_get_statsAll, 1, FTT_get_statsAll_doc },
    { "get_stats", FTT_get_stats, 1, FTT_get_stats_doc },
    { "status", FTT_status, 1, FTT_status_doc },
    { "unload", FTT_unload, 1, FTT_unload_doc },
    { "get_mode", FTT_get_mode, 1, FTT_get_mode_doc },
    { "set_mode", FTT_set_mode, 1, FTT_set_mode_doc },
    { "version", FTT_version, 1, FTT_version_doc },
    { 0, 0 }        /* Sentinel */
};

static char FTT_Doc[] =  "interface to FTT";


void
initFTT()
{
	PyObject	*m, *d;

    m = Py_InitModule4(  "FTT", FTT_Methods, FTT_Doc, (PyObject*)NULL
		       , PYTHON_API_VERSION );
    d = PyModule_GetDict( m );

    FTTErrObject = PyErr_NewException("FTT.error", NULL, NULL);
    if (FTTErrObject != NULL)
	PyDict_SetItemString( d, "error", FTTErrObject );

    g_buf_p = malloc( 2*g_blocksize );
    g_stbuf_tp = ftt_alloc_stat();

#   define INSINT( dict, ftt ) insint( dict, #ftt, ftt )
    INSINT( d, FTT_SUCCESS );
    INSINT( d, FTT_EPARTIALSTAT );
    INSINT( d, FTT_EUNRECOVERED );
    INSINT( d, FTT_ENOTAPE );
    INSINT( d, FTT_ENOTSUPPORTED );
    INSINT( d, FTT_EPERM );
    INSINT( d, FTT_EFAULT );
    INSINT( d, FTT_ENOSPC );
    INSINT( d, FTT_ENOENT );
    INSINT( d, FTT_EIO );
    INSINT( d, FTT_EBLKSIZE );
    INSINT( d, FTT_ENOEXEC );
    INSINT( d, FTT_EBLANK );
    INSINT( d, FTT_EBUSY );
    INSINT( d, FTT_ENODEV );
    INSINT( d, FTT_ENXIO );
    INSINT( d, FTT_ENFILE );
    INSINT( d, FTT_EROFS );
    INSINT( d, FTT_EPIPE );
    INSINT( d, FTT_ERANGE );
    INSINT( d, FTT_ENOMEM );
    INSINT( d, FTT_ENOTTAPE );
    INSINT( d, FTT_E2SMALL );
    INSINT( d, FTT_ERWFS );
    INSINT( d, FTT_EWRONGVOL );
    INSINT( d, FTT_EWRONGVOLTYP );
    INSINT( d, FTT_ELEADER );
    INSINT( d, FTT_EFILEMARK );
    INSINT( d, FTT_ELOST );
    INSINT( d, FTT_ENOTBOT );

    return;
}
