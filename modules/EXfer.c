#include <Python.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <stdio.h>
#include <unistd.h>
#include <assert.h>
#include <errno.h>
#if defined(IRIX) || defined(IRIX64) || defined(sun)
# include <alloca.h>
#endif

#include <sys/time.h>


/*checksumming is now being done here, instead of calling another module,
  in order to save a strcpy  -  cgw 1990428 */
unsigned int adler32(unsigned int, char *, int);

static PyObject *EXErrObject;

static PyObject *
raise_exception(char *msg)
{
        PyObject	*v;
        int		i = errno;

#   ifdef EINTR
    if ((i==EINTR) && PyErr_CheckSignals()) return NULL;
#   endif

    /* note: format should be the same as in FTT.c */
    v = Py_BuildValue("(s,i,s,i)", msg, i, strerror(i), getpid());
    if (v != NULL)
    {   PyErr_SetObject(EXErrObject, v);
	Py_DECREF(v);
    }
    return NULL;
}
static int
do_read_write(int rd_fd, int wr_fd, int no_bytes, int blk_size, int crc_flag, unsigned long *crc_p)
{
	char	*buffer;
	char	*b_p;
	int	sts;
	int	bytes_to_xfer;
	fd_set  fds;
	int     n_fds;
	struct  timeval timeout;
	
	buffer = (char *)alloca(blk_size);
	
	while (no_bytes) {
	    /* Do not worry about reading/writeing an exact block as this is
	       one the user end. But attempt blk_size reads. */
	    bytes_to_xfer = (no_bytes<blk_size)?no_bytes:blk_size;
	    
	    FD_ZERO(&fds);
	    FD_SET(rd_fd,&fds);
	    timeout.tv_sec = 15 * 60;
	    timeout.tv_usec = 0;
	    sts = select(rd_fd+1, &fds, NULL, NULL, &timeout);
	    if (sts == 0){
		/* timeout - treat as an EOF */
		return (-2);
	    }
	    sts = read(rd_fd, buffer, bytes_to_xfer);
	    if (sts == -1)
		{   /* return/break - read error */
		    return (-1);
		}
	    if (sts == 0)
		{   /* return/break - unexpected eof error */
		    return (-2);
		}
	    
	    /* call write (which should return async) and then crc */
	    bytes_to_xfer = sts;
	    b_p = buffer;
	    do {
		sts = write(wr_fd, b_p, bytes_to_xfer);
		if (sts == -1) {   /* return a write error */
		    return (-3);
		}
		/* checksum what ever we wrote. Presumably, the system is
		   delivering the data to the device (and the device is taking
		   the data from system mem and we can xsum while this is
		   happening. */
		switch (crc_flag) {
		case 0:  
		    break;
		case 1:  
		    *crc_p=adler32(*crc_p, b_p, sts); 
		    break;
		default:  
		    printf("fd_xfer: invalid crc flag"); 
		    *crc_p=0; 
		    break;
		}
		bytes_to_xfer -= sts;
		b_p += sts;
		no_bytes -= sts;
	    } while (bytes_to_xfer);	
	}
	return 0;
}

static char EXfd_xfer_Doc[] = "\
fd_xfer(fr_fd, to_fd, no_bytes, blk_siz, crc_flag[, crc])";

static PyObject *
EXfd_xfer(PyObject *self, PyObject *args)
{
    int		 fr_fd;
    int		 to_fd;
    unsigned long no_bytes;
    int		 blk_size;
    PyObject      *no_bytes_obj;
    PyObject	 *crc_obj_tp;
    PyObject	 *crc_tp=Py_None;/* optional, ref. FTT.fd_xfer */
    int           crc_flag=0; /*0: no CRC 1: Adler32 CRC >1: RFU */
    unsigned long crc_i;
    int sts;
    PyObject	*rr;
    
    sts = PyArg_ParseTuple(args, "iiOiO|O", &fr_fd, &to_fd, &no_bytes_obj, &blk_size, &crc_obj_tp, &crc_tp);
    if (!sts) return (NULL);
    if (crc_tp == Py_None)
	crc_i = 0;
    else if (PyLong_Check(crc_tp))
	crc_i = PyLong_AsUnsignedLong(crc_tp);
    else if (PyInt_Check(crc_tp))
	crc_i = (unsigned)PyInt_AsLong(crc_tp);
    else 
	return(raise_exception("fd_xfer - invalid crc param"));

    if (PyLong_Check(no_bytes_obj))
	no_bytes = PyLong_AsUnsignedLong(no_bytes_obj);
    else if (PyInt_Check(no_bytes_obj))
	no_bytes = (unsigned)PyInt_AsLong(no_bytes_obj);
    else
	return(raise_exception("fd_xfer - invalid no_bytes param"));
    
    /* see if we are crc-ing */
    if (crc_obj_tp==Py_None)
	crc_flag=0;
    else if (PyInt_Check(crc_obj_tp)) 
	crc_flag = PyInt_AsLong(crc_obj_tp);
    else 
	return(raise_exception("fd_xfer - invalid crc param"));
    if (crc_flag>1 || crc_flag<0)
	printf("fd_xfer - invalid crc param");

    assert(blk_size < 0x400000);
    sts = do_read_write(fr_fd, to_fd, no_bytes, blk_size, crc_flag, &crc_i);
    if (sts == -1)
	return (raise_exception("fd_xfer read error"));
    if (sts == -2)
	return (raise_exception("fd_xfer - read EOF unexpected"));
    if (sts == -3) 
	return (raise_exception("fd_xfer write error"));
    if (crc_flag) 
	rr = PyLong_FromUnsignedLong(crc_i);
    else          
	rr = Py_BuildValue("");
    return rr;
}


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

    First argument must be a the module name string.
    
    Second       - a list of the module methods

    Third	- a doumentation string for the module
  
    Fourth & Fifth - see Python/modsupport.c
    */

void
initEXfer()
{
    PyObject	*m, *d;
    
    m = Py_InitModule4("EXfer", EXfer_Methods, EXfer_Doc, 
		       (PyObject*)NULL, PYTHON_API_VERSION);
    d = PyModule_GetDict(m);
    EXErrObject = PyErr_NewException("EXfer.error", NULL, NULL);
    if (EXErrObject != NULL)
	PyDict_SetItemString(d,"error",EXErrObject);
}

