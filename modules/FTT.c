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

#include "ftt.h"
#include "IPC.h"		/* struct s_IPCshmgetObject, IPCshmget_Type */

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
	char		buf[200];
	char		*ss;

    ss = ftt_get_error( 0 ); /* Zero says do not return errno into pointer */
    sprintf( buf, "%s - %s", msg, ss );
    return raise_exception( buf );
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
	if (!g_buf_p) return (raise_exception("set_blocksize"));
	g_blocksize = new_blocksize;
    }

    return (Py_BuildValue(""));	/* return None */
}


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

    sts = PyArg_ParseTuple( args, "ss", &dev_s, &mode_s );
    if (!sts) return (NULL);

    if (g_ftt_desc_tp) return (raise_exception("FTT_Open - already open"));

    if (mode_s[0] == 'r')
    {   g_ftt_desc_tp = ftt_open( dev_s, FTT_RDWR );
	g_mode_c = 'r';
    }
    else /* assume write */
    {   g_ftt_desc_tp = ftt_open( dev_s, FTT_RDWR );
	g_mode_c = 'w';
    }

    sts = ftt_open_dev( g_ftt_desc_tp );
    if (!sts) return (raise_ftt_exception("FTT_Open"));

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
	if (sts == -1) return (raise_ftt_exception("close - partial block write"));
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
	PyObject	*ret_tp;
	int		sts;	/* general status */

    sts = PyArg_ParseTuple( args, "i", &no_bytes );
    if (!sts) return (NULL);

    if (!g_ftt_desc_tp) return (raise_exception("FTT_read device not opened"));

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

    if (!g_ftt_desc_tp) return (raise_exception("FTT_write device not opened"));

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
		if (sts == -1) return (raise_ftt_exception("write"));
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
	    if (sts == -1) return (raise_ftt_exception("write"));

	    while (no_bytes >= g_blocksize)
	    {   sts = ftt_write(  g_ftt_desc_tp, buf_p, g_blocksize );
		if (sts == -1) return (raise_ftt_exception("write"));
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
	    if (sts == -1) return (raise_ftt_exception("write"));
	    no_bytes -= sts;
	    bytes_written += sts;
	}
	/* save left partial block */
	g_buf_bytes = no_bytes;
	while (no_bytes--) g_buf_p[no_bytes] = buf_p[bytes_written+no_bytes];
    }

    return (Py_BuildValue(""));
}



/*****************************************************************************
 *  fd_xfer( p1 - int - fileno
 *           p2 - int - no_bytes to xfer
 *           p3 - obj - crc object
 *	     p4 - obj - shm object (see IPC.c)
 *         )
 */

static char		*g_shmaddr_p;
static int		 g_shmsize;
static int		 g_shmid;
static int		 g_semid;
static int		 g_msgqid;

static void
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

static void
do_read(  int 		rd_fd
	, int 		no_bytes
	, int 		blk_size /* is g_blocksize */
	, PyObject	*crc_obj_tp
	, int		crc_i
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
    nice( 10 );

    /* first copy the g_buf_bytes into the shm */
    {   int ii=g_buf_bytes;
	while (ii--) g_shmaddr_p[ii] = g_buf_p[ii];
    }

    while (no_bytes > 0)
    {
	int	sts, shm_bytes=g_buf_bytes;

	/* gain access to *blk* of shared mem */
 semop_try:
	sts = semop( g_semid, &sops_rd_wr2rd, 1 );
	if ((sts==-1) && (errno==EINTR))
	{   printf( "interrupted system call; assume debugger attach???\n" );
	    goto semop_try;
	}
	if (sts == -1) { send_writer( Err, errno, 0 ); exit( 1 ); }

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
		if (sts == -1) { send_writer( Err, errno, 0 ); exit( 1 ); }
		if (sts ==  0) { send_writer( Eof, errno, 0 ); exit( 1 ); }
		*read_bytes_ip += sts;
		shm_bytes += sts;
		no_bytes -= sts;
	    } while (ask != sts);
	    /* write (writing to tape) will store partial block in g_buf_p */
	    send_writer( WrtSiz, shm_bytes, g_shmaddr_p+shm_off );
	    /* do not crc g_buf_bytes */
	    if (crc_obj_tp)
	    {   PyObject	*rr;
		char	xxx[377];
		strncpy( xxx, g_shmaddr_p+shm_off+g_buf_bytes, 377 ); xxx[376]='\0';
		rr = PyObject_CallFunction(  crc_obj_tp, "s#i"
					   , g_shmaddr_p+shm_off+g_buf_bytes
					   , shm_bytes-g_buf_bytes
					   , crc_i );
		crc_i = PyInt_AsLong( rr );
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
		    send_writer( Err, errno, 0 ); exit( 1 );
		}
		user_bytes = (no_bytes<g_blocksize)? no_bytes: g_blocksize;
	    }
	    send_writer( WrtSiz, sts, g_shmaddr_p+shm_off );
	    /* some or all of g_buf_bytes are to be crc-ed */
	    if (crc_obj_tp)
	    {   PyObject	*rr;
		char	xxx[377];
		strncpy( xxx, g_shmaddr_p+shm_off, 377 ); xxx[376]='\0';
		rr = PyObject_CallFunction(  crc_obj_tp, "s#i"
					   , g_shmaddr_p+shm_off
					   , user_bytes
					   , crc_i );
		crc_i = PyInt_AsLong( rr );
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

    exit (0);
    return;
}


static char FTT_fd_xfer_doc[] = "invoke ftt_fd_xfer";

static PyObject*
FTT_fd_xfer(  PyObject *self
	    , PyObject *args )
{
	int		fd;	    /* \   */
	int		no_bytes;   /*  \  */
	PyObject	*crc_fun_tp;/*   } no optional args (compare to EXfer) */
	PyObject	*crc_tp;    /*  /  */
	PyObject	*shm_tp;    /* /   */

	int		 crc_i;
	int		 sts;	/* general status */
	int		 rd_ahead_i;
	PyObject	*rr;
	int		 pid;
	struct msqid_ds	 msgctl_s;
	struct sembuf	 sops_wr_wr2rd;  /* allows read */
	int		 dummy=0;
	int		*read_bytes_ip=&dummy, *write_bytes_ip=&dummy;

    sts = PyArg_ParseTuple(  args, "iiOOO", &fd, &no_bytes, &crc_fun_tp, &crc_tp
			   , &shm_tp );
    if (!sts) return (NULL);

    if (!g_ftt_desc_tp) return (raise_exception("FTT_fd_xfer device not opened"));

    if      (crc_tp == Py_None)   crc_i = 0;
    else if (PyInt_Check(crc_tp)) crc_i = PyInt_AsLong( crc_tp );
    else return(raise_exception("fd_xfer - invalid crc param"));

    if ((crc_fun_tp==Py_None) || PyInt_Check(crc_fun_tp)) crc_fun_tp = 0;
#   if 0
    else if (PyFunction_Check(crc_fun_tp) || PyCFunction_Check(crc_fun_tp))
    {
    }
#   endif

    {   /* get IPC stuff from object */
	struct s_IPCshmgetObject *s_p = (struct s_IPCshmgetObject *)shm_tp;
	rd_ahead_i = (s_p->i_p[0]-(7*sizeof(int))) / g_blocksize; /* do integer arithmatic */
	g_shmsize = g_blocksize * rd_ahead_i;
	g_shmid = s_p->i_p[1];
	g_shmaddr_p = (char *)&(s_p->i_p[7]);
	g_msgqid = s_p->i_p[3];
	g_semid  = s_p->i_p[2];
	read_bytes_ip  = &(s_p->i_p[4]);
	write_bytes_ip = &(s_p->i_p[5]);
    }

    msgctl( g_msgqid, IPC_STAT, &msgctl_s );
    if (sts == -1) return (raise_exception("fd_xfer msgctl IPC_STAT"));
#   if 0 /* the default is the max size -- we can not set bigger and do not
	    need smaller */
    msgctl_s.msg_qbytes = (rd_ahead_i+1) * sizeof(struct s_msgdat);
    sts = msgctl( g_msgqid, IPC_SET, &msgctl_s );
    if (sts == -1) return (raise_exception("fd_xfer msgctl IPC_SET"));
#   endif

    /* == NOW DO THE WORK ===================================================*/

    sops_wr_wr2rd.sem_num = 0;  /* 1st and only sem */
    sops_wr_wr2rd.sem_op  = 1;  /* reader dec's, writer inc's */
    sops_wr_wr2rd.sem_flg = 0;  /* default - block behavior */

    /* init wr2rd */
    sops_wr_wr2rd.sem_op  = rd_ahead_i;
    sts = semop( g_semid, &sops_wr_wr2rd, 1 );
    if (sts == -1) return (raise_exception("fd_xfer semop"));
    sops_wr_wr2rd.sem_op  = 1;  /* reader dec's, writer inc's */

    /* fork off read (from) */
    if ((pid=fork()) == 0)
	do_read( (g_mode_c=='r')?0:fd, no_bytes, g_blocksize, crc_fun_tp
		, crc_i, read_bytes_ip );
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
		do  /* I know a read (i.e from net) can return less than */
		{   /* requested, but when would a write??? */
		    msg_s.md.data -= sts;
		    if (g_mode_c == 'w')
		    {
			if (msg_s.md.data != g_blocksize)
			{   assert( msg_s.md.data < g_blocksize );
			    sts = msg_s.md.data; /* use temporarily */
			    while (sts--)
				g_buf_p[sts] = msg_s.md.c_p[sts];
			    g_buf_bytes = msg_s.md.data;
			    /* NEXT MESSAGE SHOULD/BETTER BE CRC */
			    sts = msg_s.md.data; /* set for do-while test */
			}
			else
			{   sts = ftt_write( g_ftt_desc_tp, msg_s.md.c_p, msg_s.md.data );
			    if (sts == -1) return (raise_ftt_exception("fd_xfer - write"));
			}
			if (no_bytes < sts)
			{   *write_bytes_ip += no_bytes;
			    no_bytes = 0;
			}
			else
			{   *write_bytes_ip += sts;	/* count up */
			    no_bytes -= sts; /* count down */
			}
		    }
		    else
		    {   /* writing to user */
			sts = write( fd, msg_s.md.c_p
				    , (no_bytes<msg_s.md.data)?no_bytes:msg_s.md.data );
			if (sts == -1) return (raise_exception("fd_xfer - write"));
			if (no_bytes < msg_s.md.data)
			{   /* left over goes into g_buf_p */
			    int ii=msg_s.md.data-no_bytes;
			    while (ii--)
				g_buf_p[ii] = msg_s.md.c_p[no_bytes+ii];
			    g_buf_bytes = msg_s.md.data-no_bytes;
			}
			no_bytes -= sts; /* count down */
			*write_bytes_ip += sts;	/* count up */
		    }
		} while ((sts!=msg_s.md.data) && (no_bytes > 0));

		sts = semop( g_semid, &sops_wr_wr2rd, 1 );
		if (sts == -1) return (raise_exception("fd_xfer - write - semop"));
		break;
	    case Err:
		errno = msg_s.md.data;
		return (raise_exception("fd_xfer - read error"));
		break;
	    case Eof:
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

    if (waitpid(pid,&sts,0) == -1)
	return (raise_exception("fd_xfer - waitpid"));

    if (crc_fun_tp)
	rr = Py_BuildValue( "i", crc_i );
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

    if (!g_ftt_desc_tp) return (raise_exception("FTT_writefm device not opened"));

    /* just like close */
    if (g_buf_bytes && (g_mode_c=='w'))
    {   /* write out partial block */
	sts = ftt_write( g_ftt_desc_tp,  g_buf_p, g_buf_bytes );
	if (sts == -1) return (raise_ftt_exception("FTT_writefm - partial block write"));
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

    if (!g_ftt_desc_tp) return (raise_exception("FTT_skip_fm device not opened"));

    sts = ftt_skip_fm( g_ftt_desc_tp, skip );
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
	int	sts;		/* general status */

    sts = PyArg_ParseTuple(args, "i", &locate );
    if (!sts) return (NULL);

    if (!g_ftt_desc_tp) return (raise_exception("FTT_locate device not opened"));

    sts = ftt_scsi_locate( g_ftt_desc_tp, locate );
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

    if (!g_ftt_desc_tp) return (raise_exception("FTT_rewind device not opened"));

    sts = ftt_rewind( g_ftt_desc_tp );
    if (sts == -1) return (raise_ftt_exception("FTT_rewind"));

    Py_INCREF(Py_None); return (Py_None);
}



/*****************************************************************************
 */
static char FTT_flush_doc[] = "invoke ftt_flush";

static PyObject*
FTT_flush(  PyObject *self
	  , PyObject *args )
{

    if (!g_ftt_desc_tp) return (raise_exception("FTT_flush device not opened"));

    if (g_buf_bytes && (g_mode_c=='w'))
    {   /* write out partial block */
	int sts;
	sts = ftt_write( g_ftt_desc_tp,  g_buf_p, g_buf_bytes );
	if (sts == -1) return (raise_ftt_exception("FTT_flush - partial block write"));
	g_buf_bytes = 0;
    }

    return (Py_BuildValue(""));	/* None */
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

    if (!g_ftt_desc_tp) return (raise_exception("FTT_get_stats device not opened"));

#   define GG g_stbuf_tp
    sts = ftt_get_stats( g_ftt_desc_tp, GG );
    if (sts == -1) return raise_ftt_exception( "FTT_get_stats" );

    rr = Py_BuildValue(  "{s:s,s:s,s:s,s:s,s:s,s:s,s:i}"
		       , "remain_tape",  ftt_extract_stats(GG,FTT_REMAIN_TAPE)
		       , "n_reads",      ftt_extract_stats(GG,FTT_N_READS)
		       , "read_errors",  ftt_extract_stats(GG,FTT_READ_ERRORS)
		       , "file_number",  ftt_extract_stats(GG,FTT_FILE_NUMBER)
		       , "block_number", ftt_extract_stats(GG,FTT_BLOCK_NUMBER)
		       , "bloc_loc",     ftt_extract_stats(GG,FTT_BLOC_LOC)
		       , "xferred_bytes",g_xferred_bytes );
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

    if (!g_ftt_desc_tp) return (raise_exception("FTT_status device not opened"));

#   define GG g_stbuf_tp
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

static PyMethodDef FTT_Methods[] = {
    { "set_debug", FTT_set_debug, 1, FTT_set_debug_doc },
    { "set_blocksize", FTT_set_blocksize, 1, FTT_set_blocksize_doc },
    { "open", FTT_open, 1, FTT_open_doc },
    { "close", FTT_close, 1, FTT_close_doc },
    { "read", FTT_read, 1, FTT_read_doc },
    { "write", FTT_write, 1, FTT_write_doc },
    { "fd_xfer", FTT_fd_xfer, 1, FTT_fd_xfer_doc },
    { "writefm", FTT_writefm, 1, FTT_writefm_doc },
    { "skip_fm", FTT_skip_fm, 1, FTT_skip_fm_doc },
    { "locate", FTT_locate, 1, FTT_locate_doc },
    { "rewind", FTT_rewind, 1, FTT_rewind_doc },
    { "flush", FTT_flush, 1, FTT_flush_doc },
    { "get_stats", FTT_get_stats, 1, FTT_get_stats_doc },
    { "status", FTT_status, 1, FTT_status_doc },
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
    return;
}
