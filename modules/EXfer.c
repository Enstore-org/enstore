#include <Python.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <stdio.h>
#include <unistd.h>
#include <errno.h>
#include <alloca.h>
#include <sys/time.h>
#ifdef THREADED
#include <pthread.h>
#include <sched.h>
#endif

/***************************************************************************
 constants
**************************************************************************/

/*Number of buffer bins to use*/
#ifdef THREADED
#define ARRAY_SIZE 5
#endif

/* define DEBUG only for extra debugging output */
/*#define DEBUG*/

/***************************************************************************
 definitions
**************************************************************************/

#ifdef THREADED
struct transfer
{
  int fd;         /*file descriptor*/
  long long size; /*size in bytes*/
  int block_size; /*size of block*/
  int crc_flag;   /*crc flag - 0 or 1*/
};

struct return_values
{
  long long size;      /*bytes transfered*/
  unsigned long crc_i; /*checksum*/
  int exit_status;     /*error status*/
  int errno_val;           /*errno of any errors (zero otherwise) */
};
#endif

/***************************************************************************
 prototypes
**************************************************************************/

/*checksumming is now being done here, instead of calling another module,
  in order to save a strcpy  -  cgw 1990428 */
unsigned int adler32(unsigned int, char *, int);

void initEXfer(void);
static PyObject * raise_exception(char *msg);
static PyObject * EXfd_xfer(PyObject *self, PyObject *args);
static int do_read_write(int rd_fd, int wr_fd, long long no_bytes,
			 int blk_size, int crc_flag, unsigned long *crc_p);
#ifdef THREADED
void* thread_read(void *info);
void* thread_write(void *info);
void print_status(FILE*);
#endif

/***************************************************************************
 globals
**************************************************************************/

static PyObject *EXErrObject;

static char EXfer_Doc[] =  "EXfer is a module which Xfers data";

static char EXfd_xfer_Doc[] = "\
fd_xfer(fr_fd, to_fd, no_bytes, blk_siz, crc_flag[, crc])";

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

#ifdef THREADED
int *stored;   /*pointer to array of bytes copied per bin*/
char *buffer;  /*pointer to array of buffer bins*/
pthread_mutex_t *buffer_lock; /*pointer to array of bin mutex locks*/
pthread_mutex_t done_mutex; /*used to signal main thread a thread returned*/
pthread_cond_t done_cond;   /*used to signal main thread a thread returned*/
int read_done = 0, write_done = 0; /*flags to signal which thread finished*/
#ifdef DEBUG
pthread_mutex_t print_lock; /*order debugging output*/
#endif
#endif

/***************************************************************************
 functions
**************************************************************************/

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

#ifdef THREADED
static int
do_read_write(int rd_fd, int wr_fd, long long no_bytes, int blk_size, int crc_flag, unsigned long *crc_p)
{
  /*setup local variables*/
  struct transfer reads = {rd_fd, no_bytes, blk_size, crc_flag};
  struct transfer writes = {wr_fd, no_bytes, blk_size, crc_flag};
  /*is this necessary???*/
  struct return_values **read_val = 
    (struct return_values**)alloca(sizeof(struct return_values*));
  struct return_values **write_val = 
    (struct return_values**)alloca(sizeof(struct return_values*));
  pthread_t read_tid, write_tid;
  int exit_status;

  read_done = write_done = 0; /*don't forget to reset this again!*/

  /*allocate (and initalize) memory for the global pointers*/
  buffer = calloc(ARRAY_SIZE, blk_size);
  stored = calloc(ARRAY_SIZE, sizeof(int));
  buffer_lock = calloc(ARRAY_SIZE, sizeof(pthread_mutex_t));

  /* initalize the conditional variable signaled when a thread has finished. */
  pthread_cond_init(&done_cond, NULL);
  /* initalize the mutex for signaling when a thread has finished. */
  pthread_mutex_init(&done_mutex, NULL);
#ifdef DEBUG
  /* initalize the mutex for ordering debugging output. */
  pthread_mutex_init(&print_lock, NULL);
#endif

  /*Snag this mutex before spawning the new threads.  Otherwise, there is
    the possibility that the new threads will finish before the main thread
    can get to the pthread_cond_wait() to detect the threads exiting.*/
  pthread_mutex_lock(&done_mutex);

  /* get the threads going. */
  pthread_create(&write_tid, NULL, &thread_write, &writes);
  pthread_create(&read_tid, NULL, &thread_read, &reads);
  sched_yield();

  /*This screewy loop of code is used to detect if a thread has terminated.
     If an error occurs either thread could return in any order.  If
     pthread_join() could join with any thread returned this would not
     be so complicated.*/

  while(!read_done || !write_done)
  {
    /* wait until the condition variable is set and we have the mutex */
    /* Waiting indefinatly could be dangerous. */
    pthread_cond_wait(&done_cond, &done_mutex);

    if(read_done > 0) /*true when thread_read ends*/
    {
      pthread_join(read_tid, (void**)read_val);
      exit_status = (*(struct return_values*)(*read_val)).exit_status;
      if(exit_status)
	break; /*If an error occured, no sense continuing*/
      else
	read_done = -1;
    }
    if(write_done > 0) /*true when thread_write ends*/
    {
      pthread_join(write_tid, (void**)write_val);
      exit_status = (*(struct return_values*)(*write_val)).exit_status;
      if(exit_status)
	break; /*If an error occured, no sense continuing*/
      else
	write_done = -1;
    }

  }
  pthread_mutex_unlock(&done_mutex);

#ifdef DEBUG
  fprintf(stderr, "read_crc: %lu\n",
	  (*(struct return_values*)(*read_val)).crc_i);
  fflush(stderr);
  fprintf(stderr, "write_crc: %lu\n",
	  (*(struct return_values*)(*write_val)).crc_i);
  fflush(stderr);
#endif

  /* Set the crc value to the passed by reference variable and snag the
     exit status. */
  *crc_p = (*(struct return_values*)(*write_val)).crc_i;
  exit_status = (*(struct return_values*)(*write_val)).exit_status;
  
  /*free the dynamic memory*/
  free(stored);
  free(buffer);
  free(buffer_lock);

  /* The errno in the variable gets overridden by thread code if an error
     occurs. Set it back if necessary.*/
  if((*(struct return_values*)(*write_val)).errno_val)
  {
    errno = (*(struct return_values*)(*write_val)).errno_val;
  }
  else if((*(struct return_values*)(*read_val)).errno_val)
  {
    errno = (*(struct return_values*)(*read_val)).errno_val;
  }
  return exit_status;
}


void* thread_read(void *info)
{
  struct transfer read_info = *((struct transfer*)info);
  int rd_fd = read_info.fd;
  long long bytes = read_info.size;
  int crc_flag = read_info.crc_flag;
  int block_size = read_info.block_size;
  int bytes_to_transfer;
  int sts;
  int bin = 0;
  fd_set fds;
  struct timeval timeout;
  struct timeval start, end;
  struct return_values *retval = malloc(sizeof(struct return_values));
  unsigned long crc_i = 0;

  memset(retval, 0, sizeof(struct return_values));

  while(bytes)
  {
    /*Get status*/
    pthread_mutex_lock(&buffer_lock[bin]);
    if(stored[bin])
    {
      pthread_mutex_unlock(&buffer_lock[bin]);
      sched_yield();
      continue;
    }
    pthread_mutex_unlock(&buffer_lock[bin]);

    gettimeofday(&start, NULL);
    while(start.tv_sec - end.tv_sec < 15 && start.tv_usec - end.tv_usec < 0)
    {
      FD_ZERO(&fds);
      FD_SET(rd_fd,&fds);
      timeout.tv_sec = 15 * 60;
      timeout.tv_usec = 0;
      
      sts = select(rd_fd+1, &fds, NULL, NULL, &timeout);
      if (sts == 0)
      { /* timeout - treat as an EOF */
	pthread_mutex_lock(&done_mutex);
	retval->exit_status = (-2);
	read_done = 1;
	pthread_cond_signal(&done_cond);
	pthread_mutex_unlock(&done_mutex);
	return retval;
      }
    }
    /*Dertermine how much to read*/
    bytes_to_transfer = (bytes<block_size)?bytes:block_size;

    /* read in the data */
    sts = read(rd_fd, (buffer+bin*block_size), bytes_to_transfer);
    if (sts == -1)
    { /* return/break - read error */
      pthread_mutex_lock(&done_mutex);
      retval->errno_val = errno;
      retval->exit_status = (-1);
      read_done = 1;
      pthread_cond_signal(&done_cond);
      pthread_mutex_unlock(&done_mutex);
      return retval;
    }
    if (sts == 0)
    { /* return/break - unexpected eof error */
      pthread_mutex_lock(&done_mutex);
      retval->errno_val = errno;
      retval->exit_status = (-2);
      read_done = 1;
      pthread_cond_signal(&done_cond);
      pthread_mutex_unlock(&done_mutex);
      return retval;
    }
    
    switch (crc_flag)
    {
    case 0:  
	break;
    case 1:  
      crc_i=adler32(crc_i, (buffer+bin*block_size), sts);
      break;
    default:  
      printf("fd_xfer: invalid crc flag"); 
      crc_i=0; 
      break;
    }

    /*Set up status array*/
    pthread_mutex_lock(&buffer_lock[bin]);
    stored[bin] = sts; /*FULL;*/
    pthread_mutex_unlock(&buffer_lock[bin]);

    /*Determine where to put the data*/
    bin = (bin + 1) % ARRAY_SIZE;
    bytes -= sts;

#ifdef DEBUG
    pthread_mutex_lock(&print_lock);
    fprintf(stderr, "rbytes: %15lld sts: %10d crc: %lu | ", bytes, sts, crc_i);
    print_status(stderr);
    pthread_mutex_unlock(&print_lock);
#endif

  }

  pthread_mutex_lock(&done_mutex);
  retval->exit_status = (0);
  retval->crc_i = crc_i;
  retval->size = bytes;
  retval->errno_val = 0;
  read_done = 1;
  pthread_cond_signal(&done_cond);
  pthread_mutex_unlock(&done_mutex);
  return retval;
}



void* thread_write(void *info)
{
  struct transfer write_info = *((struct transfer*)info);
  int wr_fd = write_info.fd;
  long long bytes = write_info.size;
  int crc_flag = write_info.crc_flag;
  int block_size = write_info.block_size;
  int bytes_to_transfer;
  int sts;
  int bin = 0;
  struct return_values *retval = malloc(sizeof(struct return_values));
  unsigned long crc_i = 0;

  memset(retval, 0, sizeof(struct return_values));

  while(bytes)
  {
    /*Get status*/
    pthread_mutex_lock(&buffer_lock[bin]);
    if(stored[bin] == 0)
    {
      pthread_mutex_unlock(&buffer_lock[bin]);
      sched_yield();
      continue;
    }

    /*Dertermine how much to write*/
    bytes_to_transfer = (stored[bin]<block_size)?stored[bin]:block_size;
    
    /*unlock this mutex*/
    pthread_mutex_unlock(&buffer_lock[bin]);

    /* write out the data */
    sts = write(wr_fd, (buffer+bin*block_size), bytes_to_transfer);
    if (sts == -1)
    { /* return a write error */
      pthread_mutex_lock(&done_mutex);
      retval->errno_val = errno;
      retval->exit_status = (-3);
      write_done = 1;
      pthread_cond_signal(&done_cond);
      pthread_mutex_unlock(&done_mutex);
      return retval;
    }

    switch (crc_flag)
    {
    case 0:  
      break;
    case 1:  
      crc_i=adler32(crc_i, (buffer+bin*block_size), sts);
      /*to cause intentional crc errors, use the following line instead*/
      /*crc_i=adler32(crc_i, (buffer), sts);*/
      break;
    default:  
      printf("fd_xfer: invalid crc flag"); 
      crc_i=0; 
      break;
    }

    /*Set up status array*/
    pthread_mutex_lock(&buffer_lock[bin]);
    stored[bin] = 0;
    pthread_mutex_unlock(&buffer_lock[bin]);

    /*printf("bytes: %lld bytes_to_transfer: %d\n", bytes, bytes_to_transfer);
      fflush(stdout);*/

    /*Determine where to get the data*/
    bin = (bin + 1) % ARRAY_SIZE;
    bytes -= sts;

#ifdef DEBUG
    pthread_mutex_lock(&print_lock);
    fprintf(stderr, "wbytes: %15lld sts: %10d crc: %lu | ", bytes, sts, crc_i);
    print_status(stderr);
    pthread_mutex_unlock(&print_lock);
#endif

  }

  pthread_mutex_lock(&done_mutex);
  retval->exit_status = 0;
  retval->crc_i = crc_i;
  retval->size = bytes;
  retval->errno_val = 0;
  write_done = 1;
  pthread_cond_signal(&done_cond);
  pthread_mutex_unlock(&done_mutex);
  return retval;
}

void print_status(FILE* fd)
{
  int i;
  for(i = 0; i < ARRAY_SIZE; i++)
  {
    fprintf(fd, "%6d  ", stored[i]);
  }
  fprintf(fd, "\n");
}

#else
static int
do_read_write(int rd_fd, int wr_fd, long long no_bytes, int blk_size, int crc_flag, unsigned long *crc_p)
{
	char	       *buffer;
	char	       *b_p;
	ssize_t         sts;
	size_t	        bytes_to_xfer;
	fd_set          fds;
	struct timeval  timeout;
	
	buffer = (char *)alloca(blk_size);

	while (no_bytes) {
	    /* Do not worry about reading/writing an exact block as this is
	       one the user end. But attempt blk_size reads. */
	    bytes_to_xfer = (no_bytes<blk_size)?no_bytes:blk_size;
	    
	    FD_ZERO(&fds);
	    FD_SET(rd_fd,&fds);
	    timeout.tv_sec = 15 * 60;
	    timeout.tv_usec = 0;
	    errno=0;
	    sts = select(rd_fd+1, &fds, NULL, NULL, &timeout);
	    if (sts == 0){
		/* timeout - treat as an EOF */
		return (-2);
	    }
	    errno = 0;
	    sts = read(rd_fd, buffer, bytes_to_xfer);
	    if (sts == -1)
		{   /* return/break - read error */
		    return (-1);
		}
	    if (sts == 0)
		{   /* return/break - unexpected eof error */
		    return (-2);
		}
	    
	    bytes_to_xfer = sts;
	    b_p = buffer;
	    do {
	        errno=0;
	        sts = write(wr_fd, b_p, bytes_to_xfer);
	        if (sts != bytes_to_xfer){
		  fprintf(stderr, "write(%d, %#x, %lld) -> %lld, errno=%d\n",
			  wr_fd, (unsigned)b_p, (long long)bytes_to_xfer,
			  (long long)sts, errno);
		  fflush(stderr);
		}
		if (sts == -1) {   /* return a write error */
		    return (-3);
		}
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
#endif

static PyObject *
EXfd_xfer(PyObject *self, PyObject *args)
{
    int		 fr_fd;
    int		 to_fd;
    long long no_bytes;
    int		 blk_size;
    PyObject      *no_bytes_obj;
    PyObject	 *crc_obj_tp;
    PyObject	 *crc_tp=Py_None;/* optional, ref. FTT.fd_xfer */
    int           crc_flag=0; /*0: no CRC 1: Adler32 CRC >1: RFU */
    unsigned long crc_i;
    int sts;
    PyObject	*rr;
    
    sts = PyArg_ParseTuple(args, "iiOiO|O", &fr_fd, &to_fd, &no_bytes_obj,
			   &blk_size, &crc_obj_tp, &crc_tp);
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
	no_bytes = PyLong_AsLongLong(no_bytes_obj);
    else if (PyInt_Check(no_bytes_obj))
	no_bytes = (long long)PyInt_AsLong(no_bytes_obj);
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

    errno = 0;
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


/***************************************************************************
 inititalization
 **************************************************************************
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

