/* EXfer.c - Low level data transfer C modules for encp. */



#ifndef STAND_ALONE
#include <Python.h>
#endif
#include <sys/stat.h>
#include <sys/types.h>
#include <stdio.h>
#include <unistd.h>
#include <errno.h>
#include <malloc.h>
#include <alloca.h>
#include <sys/time.h>
#include <signal.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/resource.h>
/*#ifdef THREADED*/
#include <pthread.h>
#include <sys/mman.h>
/*#endif*/

/***************************************************************************
 constants
**************************************************************************/

/* return/break - read error */
#define READ_ERROR (-1)
/* timeout - treat as an EOF */
#define TIMEOUT_ERROR (-2)
/* return a write error */
#define WRITE_ERROR (-3)

/*Number of buffer bins to use*/
/*#ifdef THREADED*/
/*#define ARRAY_SIZE 3*/
#define THREAD_ERROR (-4)
#ifdef __linix__
#define __USE_BSD
#endif
/*#endif *//*THREADED*/

/* Define DEBUG only for extra debugging output */
/*#define DEBUG*/
/*#define PROFILE*/
#ifdef PROFILE
#define PROFILE_COUNT 25000
#endif

/* Number of seconds before timing out. (15min * 60sec/min = 900sec)*/
#define TIMEOUT 900

/* Macro to convert struct timeval into double. */
#define extract_time(t) (double)(t.tv_sec+(t.tv_usec/1000000.0))
/* Macro to take two usage structs and return the total time difference. */
#define rusage_elapsed_time(sru, eru) \
   ((extract_time(eru.ru_stime) + extract_time(eru.ru_utime)) - \
   (extract_time(sru.ru_stime) + extract_time(sru.ru_utime)))

/* Define memory mapped i/o lengths. */
#ifndef MADV_SEQUENTIAL
#define MADV_SEQUENTIAL -1
#endif
#ifndef MADV_WILLNEED
#define MADV_WILLNEED -1
#endif
/*Note: This needs to be a multiple of buffer_size. */
/*#define MMAPPED_IO_SIZE 100663296*/
/*#define MMAPPED_IO_SIZE 134217728*/

/***************************************************************************
 definitions
**************************************************************************/

/*#ifdef THREADED*/
struct transfer
{
  int fd;                 /*file descriptor*/
  void *mmap_ptr;         /*memory mapped i/o pointer*/
  off_t mmap_len;         /*length of memory mapped file offset*/
  long long size;         /*size in bytes*/
  int block_size;         /*size of block*/
  int array_size;         /*number of buffers to use*/
  long mmap_size;         /*mmap address space segment lengths*/
  struct timeval timeout; /*time to wait for data to be ready*/
  int crc_flag;           /*crc flag - 0 or 1*/
  int transfer_direction; /*positive means write, negative means read*/
  int direct_io;          /*is true if using direct io*/
  int mmap_io;            /*is true if useing memory mapped io*/
};
/*#endif*/ /*THREADED*/

#ifdef PROFILE
struct profile
{
  char whereami;
  struct timeval time;
  int status;
  int error;
};
#endif

struct return_values
{
  long long size;         /*bytes left to transfer*/
  unsigned int crc_ui;    /*checksum*/
  int exit_status;        /*error status*/
  int errno_val;          /*errno of any errors (zero otherwise)*/
  char* msg;              /*additional error message*/
  double read_time;       /*time spent reading data*/
  double write_time;      /*time spent writing data*/
  int line;               /*line number where error occured*/
  char* filename;         /*filename where error occured*/
};

/***************************************************************************
 prototypes
**************************************************************************/

/*checksumming is now being done here, instead of calling another module,
  in order to save a strcpy  -  cgw 1990428 */
unsigned int adler32(unsigned int, char *, int);

#ifndef STAND_ALONE
void initEXfer(void);
static PyObject * raise_exception(char *msg);
static PyObject * EXfd_xfer(PyObject *self, PyObject *args);
#endif
static struct return_values do_read_write(int rd_fd, int wr_fd,
					  long long bytes,
					  struct timeval timeout, int crc_flag,
					  int blk_size, int array_size,
					  long mmap_size,
					  int direct_io, int mmap_io,
					  unsigned int *crc_p);
static struct return_values do_read_write(int rd_fd, int wr_fd,
					  long long bytes,
					  struct timeval timeout, int crc_flag,
					  int blk_size, int array_size,
					  long mmap_size,
					  int direct_io, int mmap_io,
					  unsigned int *crc_p);
static struct return_values* pack_return_values(unsigned int crc_ui,
						int errno_val, int exit_status,
						long long bytes, char* msg,
						double read_time,
						double write_time,
						char *filename, int line);
static double elapsed_time(struct timeval* start_time,
			   struct timeval* end_time);
static long long get_fsync_threshold(long long bytes, int blk_size);
static long align_to_page(long value);
static long align_to_size(long value, long align);
struct return_values setup_mmap(int fd, struct transfer *info);
#ifdef PROFILE
void update_profile(int whereami, int sts, int sock,
		    struct profile *profile_data, long *profile_count);
void print_profile(struct profile *profile_data, int profile_count);
#endif /*PROFILE*/
/*#ifdef THREADED*/
static void* thread_read(void *info);
static void* thread_write(void *info);
void set_done_flag(int* done);
#ifdef DEBUG
static void print_status(FILE*, char, long long, unsigned int, int array_size);
#endif /*DEBUG*/
/*#endif*/ /*THREADED*/

/***************************************************************************
 globals
**************************************************************************/

#ifndef STAND_ALONE

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

#endif

/*#ifdef THREADED*/
int *stored;   /*pointer to array of bytes copied per bin*/
char *buffer;  /*pointer to array of buffer bins*/
pthread_mutex_t *buffer_lock; /*pointer to array of bin mutex locks*/
pthread_mutex_t done_mutex; /*used to signal main thread a thread returned*/
pthread_cond_t done_cond;   /*used to signal main thread a thread returned*/
pthread_cond_t next_cond;   /*used to signal peer thread to continue*/
int read_done = 0, write_done = 0; /*flags to signal which thread finished*/
#ifdef DEBUG
pthread_mutex_t print_lock; /*order debugging output*/
#endif
/*#endif*/

/***************************************************************************
 user defined functions
**************************************************************************/

/* Pack the arguments into a struct return_values. */
static struct return_values* pack_return_values(unsigned int crc_ui,
						int errno_val,
						int exit_status,
						long long bytes,
						char* message,
						double read_time,
						double write_time,
						char* filename, int line)
{
  struct return_values *retval;

  retval = (struct return_values*)malloc(sizeof(struct return_values));
  if(retval == NULL)
  {
    /* If we ever get here there are some very bad things going on. */
    fprintf(stderr, "Memory allocation failed.  Aborting.\n");
    fflush(stderr);
    exit(1);
  }

  retval->crc_ui = crc_ui;             /* Checksum */
  retval->errno_val = errno_val;       /* Errno value if error occured. */
  retval->exit_status = exit_status;   /* Exit status of the thread. */
  retval->size = bytes;                /* Bytes left to transfer. */
  retval->msg = message;               /* Additional error message. */
  retval->read_time = read_time;       /* Elapsed time spent reading. */
  retval->write_time = write_time;     /* Elapsed time spent writing. */
  retval->line = line;             
  retval->filename = filename;
  return retval;
}

static double elapsed_time(struct timeval* start_time,
			   struct timeval* end_time)
{
  double elapsed_time;  /* variable to hold the time difference */

  elapsed_time = (end_time->tv_sec - start_time->tv_sec) + 
    (end_time->tv_usec - start_time->tv_usec) / 1000000.0;

  return elapsed_time;
}

static long long get_fsync_threshold(long long bytes, int blk_size)
{
  long long temp_value;

  /* Find out what one percent of the file size is. */
  temp_value = (long long)(bytes / (double)100.0);

  /* Return the larger of the block size and 1 percent of the file size. */
  return (temp_value > blk_size) ? temp_value : blk_size;
}

/* A usefull function to round a value to the next full page. */
static long align_to_page(long value)
{
   return align_to_size(value, sysconf(_SC_PAGESIZE));
/*
   return ((value % sysconf(_SC_PAGESIZE)) ?
	   value + sysconf(_SC_PAGESIZE) - (value % sysconf(_SC_PAGESIZE)) :
	   value);
*/
}

static long align_to_size(long value, long align)
{
   return (value % align) ? (value + align - (value % align)) : value;
}


void set_done_flag(int* done)
{
  if(done != NULL)
  {
    /* Do not bother with checking return values for errors.  Should the
       pthread_* functions fail at this point, there is notthing else to
       do but set the global flag and return. */
    pthread_mutex_lock(&done_mutex);
    *done = 1;
    pthread_cond_signal(&done_cond);
    pthread_mutex_unlock(&done_mutex);
  }
}

/* Return 0 for false, >1 for true, <1 for error. */
int is_empty(int bin)
{
  int rtn = 0; /*hold return value*/

  /* Determine if the lock for the buffer_lock bin, bin, is ready. */
  if(pthread_mutex_lock(&buffer_lock[bin]) != 0)
  {
    return -1; /* If we fail here, we are likely to see it again. */
  }
  if(stored[bin] == 0)
  {
    rtn = 1;
  }
  if(pthread_mutex_unlock(&buffer_lock[bin]) != 0)
  {
    return -1; /* If we fail here, we are likely to see it again. */
  }

  return rtn;
}

struct return_values setup_mmap(int fd, struct transfer *info)
{
  struct stat file_info;        /* Information about the file to write to. */
  long long bytes = info->size; /* Number of bytes to transfer. */
  off_t mmap_len =              /* Offset needs to be mulitple of pagesize */
    align_to_size(info->mmap_size, info->block_size); /* and blocksize. */
  int advise_holder;

  /* Determine the length of the memory mapped segment. */
  mmap_len = (bytes<mmap_len)?bytes:mmap_len;
  /* Make sure that the memory map length is set correctly.  Even if
     this file descriptor can not do memory mapped i/o, the other
     transfer thread might. */
  info->mmap_len = mmap_len;
  info->mmap_ptr = MAP_FAILED;

  /* If the user did not select memory mapped i/o do not use it. */
  if(!info->mmap_io)
    return *pack_return_values(0, 0, 0, info->size, "", 0.0, 0.0, NULL, 0);

  /* Determine if the file descriptor is a real file. */
  errno = 0;
  if(fstat(fd, &file_info))
  {
    return *pack_return_values(0, errno, THREAD_ERROR, bytes,
			       "fstat failed", 0.0, 0.0,
			       __FILE__, __LINE__);
  }
  /* If the file is a local disk, use memory mapped i/o on it. */
  if(S_ISREG(file_info.st_mode))
  {

    if(info->transfer_direction > 0)  /* If true, it is a write. */
    {
      /* Set the size of the file. */
      errno = 0;
      if(ftruncate(fd, bytes) < 0)
	return *pack_return_values(0, errno, THREAD_ERROR, bytes,
				   "ftruncate failed", 0.0, 0.0,
				   __FILE__, __LINE__);
    }

    /* Create the memory mapped file. info->mmap_ptr will equal the
       starting memory address on success; MAP_FAILED on error. */
    info->mmap_ptr = mmap(NULL, mmap_len, PROT_WRITE | PROT_READ,
			  MAP_SHARED, fd, 0);

    if(info->mmap_ptr != MAP_FAILED)
    {
      if(info->transfer_direction > 0) /* If true, it is a write to disk. */
	advise_holder = MADV_SEQUENTIAL;
      else
	advise_holder = MADV_SEQUENTIAL | MADV_WILLNEED;
      
      /* Advise the system on the memory mapped i/o usage pattern. */
      errno = 0;
      if(madvise(info->mmap_ptr, mmap_len, advise_holder) < 0)
      {
	/* glibc versions prior to 2.2.x don't support the madvise function.
	   If it is found not to be supported, don't worry.  Use the
	   default read/write method.  This error sets errno to ENOSYS. */
	/* IRIX does not support use of MADV_SEQUENTIAL.  This error sets
	   errno to EINVAL. */

	/* Clear the memory mapped information. */
	munmap(info->mmap_ptr, info->mmap_len);
	info->mmap_ptr = MAP_FAILED;
      }
    }
  
    /* If mmap() or madvise() failed, reset the file to its original size. */
    if(info->mmap_ptr == MAP_FAILED)
    {
      errno = 0;
      if(ftruncate(fd, file_info.st_size) < 0)
	return *pack_return_values(0, errno, THREAD_ERROR, file_info.st_size,
				   "ftruncate failed", 0.0, 0.0,
				   __FILE__, __LINE__);
    }
  }
  return *pack_return_values(0, 0, 0, info->size, "", 0.0, 0.0, NULL, 0);
}


static struct return_values
do_read_write_threaded(int rd_fd, int wr_fd, long long bytes,
		       struct timeval timeout, int crc_flag,
		       int blk_size, int array_size, long mmap_size,
		       int direct_io, int mmap_io, unsigned int *crc_p)
{
  /*setup local variables*/
  struct transfer reads;   /* Information passed into read thread. */
  struct transfer writes;  /* Information passed into write thread. */
  struct return_values *read_val = NULL;  /*Incase of early thread ... */
  struct return_values *write_val = NULL; /*... error set to NULL.*/
  struct return_values *rtn_val = malloc(sizeof(struct return_values));
  struct return_values rtn_status; /* Store results from descriptor setup. */
  
  pthread_t read_tid, write_tid;
  /*int exit_status;*/
  int i;
  int p_rtn;

  /*Place the values into the struct.  Some compilers complained when this
    information was placed into the struct inline at initalization.  So it
    was moved here.*/
  reads.fd = rd_fd;
  reads.mmap_ptr = MAP_FAILED;
  reads.mmap_len = 0;
  reads.size = bytes;
  reads.block_size = align_to_page(blk_size);
  reads.array_size = array_size;
  reads.mmap_size = mmap_size;
  reads.timeout = timeout;
#ifdef DEBUG
  reads.crc_flag = crc_flag;
#else
  reads.crc_flag = 0;
#endif
  reads.transfer_direction = -1;
  reads.direct_io = direct_io;
  reads.mmap_io = mmap_io;
  writes.fd = wr_fd;
  writes.mmap_ptr = MAP_FAILED;
  writes.mmap_len = 0;
  writes.size = bytes;
  writes.block_size = align_to_page(blk_size);
  writes.array_size = array_size;
  writes.mmap_size = mmap_size;
  writes.timeout = timeout;
  writes.crc_flag = crc_flag;
  writes.transfer_direction = 1;
  writes.direct_io = direct_io;
  writes.mmap_io = mmap_io;

  /* Don't forget to reset these.  These are globals and need to be cleared
     between multi-file transfers. */
  read_done = write_done = 0;

  /* Detect (and setup if necessary) the use of memory mapped io. */
  rtn_status = setup_mmap(rd_fd, &reads);
  rtn_status = setup_mmap(wr_fd, &writes);

  /* Allocate and initialize the arrays */
  errno = 0;
  if((stored = calloc(array_size, sizeof(int))) ==  NULL)
    return *pack_return_values(0, errno, THREAD_ERROR, bytes,
			       "calloc failed", 0.0, 0.0,
			       __FILE__, __LINE__);
  errno = 0;
  if((buffer_lock = calloc(array_size, sizeof(pthread_mutex_t))) == NULL)
    return *pack_return_values(0, errno, THREAD_ERROR, bytes,
			       "calloc failed", 0.0, 0.0,
			       __FILE__, __LINE__);
  errno = 0;
  if((buffer = memalign(sysconf(_SC_PAGESIZE),
			array_size * align_to_page(blk_size))) == NULL)
    return *pack_return_values(0, errno, THREAD_ERROR, bytes,
			       "memalign failed", 0.0, 0.0,
			       __FILE__, __LINE__);


  /* initalize the conditional variable signaled when a thread has finished. */
  pthread_cond_init(&done_cond, NULL);
  /* initalize the conditional variable to signal peer thread to continue. */
  pthread_cond_init(&next_cond, NULL);
  /* initalize the mutex for signaling when a thread has finished. */
  pthread_mutex_init(&done_mutex, NULL);
#ifdef DEBUG
  /* initalize the mutex for ordering debugging output. */
  pthread_mutex_init(&print_lock, NULL);
#endif
  /* initalize the array of bin mutex locks. */
  for(i = 0; i < array_size; i++)
    pthread_mutex_init(&(buffer_lock[i]), NULL);

  /*Snag this mutex before spawning the new threads.  Otherwise, there is
    the possibility that the new threads will finish before the main thread
    can get to the pthread_cond_wait() to detect the threads exiting.*/
  if((p_rtn = pthread_mutex_lock(&done_mutex)) != 0)
    return *pack_return_values(0, p_rtn, THREAD_ERROR, bytes,
			       "mutex lock failed", 0.0, 0.0,
			       __FILE__, __LINE__);

  /* get the threads going. */
  if((p_rtn = pthread_create(&write_tid, NULL, &thread_write, &writes)) != 0)
    return *pack_return_values(0, p_rtn, THREAD_ERROR, bytes,
			       "write thread creation failed", 0.0, 0.0,
			       __FILE__, __LINE__);
  if((p_rtn = pthread_create(&read_tid, NULL, &thread_read, &reads)) != 0)
    return *pack_return_values(0, p_rtn, THREAD_ERROR, bytes,
			       "read thread creation failed", 0.0, 0.0,
			       __FILE__, __LINE__);

  /*This screewy loop of code is used to detect if a thread has terminated.
     If an error occurs either thread could return in any order.  If
     pthread_join() could join with any thread returned this would not
     be so complicated.*/

  while(!read_done || !write_done)
  {
    /* wait until the condition variable is set and we have the mutex */
    /* Waiting indefinatly could be dangerous. */

    if((p_rtn = pthread_cond_wait(&done_cond, &done_mutex)) != 0)
      return *pack_return_values(0, p_rtn, THREAD_ERROR, bytes,
				 "waiting for condition failed", 0.0, 0.0,
				 __FILE__, __LINE__);

    if(read_done > 0) /*true when thread_read ends*/
    {
      if((p_rtn = pthread_join(read_tid, (void**)&read_val)) != 0)
	return *pack_return_values(0, p_rtn, THREAD_ERROR, bytes,
				   "joining with read thread failed",
				   0.0, 0.0, __FILE__, __LINE__);

      if(read_val->exit_status)
      {
	fprintf(stderr,
		"Read thread exited with error(%d) '%s' from %s line %d.\n",
		read_val->errno_val, strerror(read_val->errno_val),
		read_val->filename, read_val->line);
	memcpy(rtn_val, read_val, sizeof(struct return_values));
	/*free(&(*write_val));*/
	pthread_kill(write_tid, SIGKILL);
	break; /*If an error occured, no sense continuing*/
      }
      else
	read_done = -1; /* Set to non-positive and non-zero value. */
    }
    if(write_done > 0) /*true when thread_write ends*/
    {
      if((p_rtn = pthread_join(write_tid, (void**)&write_val)) != 0)
	return *pack_return_values(0, p_rtn, THREAD_ERROR, bytes,
				   "joining with write thread failed",
				   0.0, 0.0, __FILE__, __LINE__);
      memcpy(rtn_val, write_val, sizeof(struct return_values));
      /*free(&(*read_val));*/
      if(write_val->exit_status)
      {
	fprintf(stderr,
		"Write thread exited with error(%d) '%s' from %s line %d.\n",
		write_val->errno_val, strerror(write_val->errno_val),
		write_val->filename, write_val->line);
	/*pthread_kill(read_tid, SIGKILL);*/
	break; /*If an error occured, no sense continuing*/
      }
      else
	write_done = -1; /* Set to non-positive and non-zero value. */
    }

  }
  pthread_mutex_unlock(&done_mutex);

  /* If there were no errors up to this point, rtn_val contains the write_val
     information. */
  
  /* MWZ:  This isn't relavent anymore. */
  if(rtn_val == NULL)
  {
    rtn_val = malloc(sizeof(struct return_values));
    rtn_val->exit_status = THREAD_ERROR;
    rtn_val->crc_ui = 0;
    rtn_val->errno_val = EILSEQ;
    rtn_val->size = 0;
    rtn_val->read_time = 0.0;
    rtn_val->write_time = 0.0;
  }

  /* Make sure that this information gets returned to encp. */
  if(read_val->read_time)
    rtn_val->read_time = read_val->read_time;
  if(write_val->write_time)
    rtn_val->write_time = write_val->write_time;

  /* Get the pointer to the checksum. */
  *crc_p = rtn_val->crc_ui;

  /* Print out an error message.  This information currently is not returned
     to encp.py. */
  if(rtn_val->exit_status)
  {
    fprintf(stderr, "Low-level transfer failure: [Errno %d] %s: higher "
	    "encp levels will process this error and retry if possible\n",
	    rtn_val->errno_val, strerror(rtn_val->errno_val));
    fflush(stderr);
  }

  /*free the address space*/
  if(reads.mmap_ptr != MAP_FAILED)
    munmap(reads.mmap_ptr, reads.mmap_len);
  if(writes.mmap_ptr != MAP_FAILED)
    munmap(writes.mmap_ptr, writes.mmap_len);
  
  /*free the dynamic memory*/
  free(stored);
  free(buffer);
  free(buffer_lock);
  free(&(*read_val));
  free(&(*write_val));

  return *rtn_val;
}

static void* thread_read(void *info)
{
  struct transfer *read_info = (struct transfer*)info; /* dereference */
  int rd_fd = read_info->fd;               /* File descriptor to read from. */
  long long bytes = read_info->size;       /* Number of bytes to transfer. */
  int crc_flag = read_info->crc_flag;      /* Flag to enable crc checking. */
  int block_size = read_info->block_size;  /* Bytes to transfer at one time. */
  int array_size = read_info->array_size;  /* Number of buffer bins. */
  struct timeval timeout = read_info->timeout; /* Time to wait for data. */
  int direct_io = read_info->direct_io;    /* True if using direct io. */
  /*int mmap_io = read_info->mmap_io;*/    /* True if using mem. mapped io. */
  size_t bytes_remaining;       /* Number of bytes to move in one loop. */
  size_t bytes_to_transfer;     /* Bytes to transfer in a single loop. */
  size_t bytes_transfered;      /* Bytes left to transfer in a sub loop. */
  int sts = 0;                  /* Return value from various C system calls. */
  int bin = 0;                  /* The current bin (bucket) to use. */
  unsigned int crc_ui = 0;      /* Calculated checksum. */
  fd_set fds;                   /* For use with select(2). */
  int p_rtn;                    /* Pthread return value. */

#ifdef DEBUG
  char debug_print;             /* Specifies what transfer occured.  R or r */
#endif
  struct stat file_info;        /* Information about the file to read from. */
  void *rd_mmap = read_info->mmap_ptr;   /* Pointer to memory mapped i/o. */
  off_t mmap_offset = 0;        /* Offset from beginning of mmapped segment. */
  int mmap_count = 0;           /* Number of mmapped segments done. */
  off_t mmap_len = read_info->mmap_len; /* Length of offset segment. */
  off_t mmap_left = mmap_len - mmap_offset; /* Bytes to next mmap segment. */
  struct timeval start_time;    /* Holds time measurement value. */
  struct timeval end_time;      /* Holds time measurement value. */
  struct rusage start_usage;    /* Hold time info from os billing. */
  struct rusage end_usage;      /* Hold time info from os billing. */
  struct timeval start_total;   /* Hold overall time measurment value. */
  struct timeval end_total;     /* Hold overall time measurment value. */
  double corrected_time = 0.0;  /* Corrected return time. */
  double transfer_time = 0.0;   /* Runing transfer time. */

  /* Initialize the running time incase of early failure. */
  memset(&start_time, 0, sizeof(struct timeval));
  memset(&end_time, 0, sizeof(struct timeval));
  /* Initialize the running time incase of early failure. */
  gettimeofday(&start_total, NULL);
  memcpy(&end_total, &start_total, sizeof(struct timeval));

  /* Get the thread's start time usage. */
  getrusage(RUSAGE_SELF, &start_usage);

  /* Determine if the file descriptor supports fsync(). */
  if(fstat(rd_fd, &file_info))
  {
    set_done_flag(&read_done);
    return pack_return_values(0, errno, READ_ERROR, bytes,
			      "fstat failed", 0.0, 0.0,
			      __FILE__, __LINE__);
  }

  while(bytes > 0)
  {
    /* If the file is a local disk, use memory mapped i/o on it. 
     Only advance to the next mmap segment when the previous one is done. */
    if(rd_mmap != MAP_FAILED && mmap_offset == mmap_len)
    {
      /* Unmap the current mapped memory segment. */
      errno = 0;
      if(munmap(rd_mmap, mmap_len) < 0)
      {
	set_done_flag(&read_done);
	corrected_time = elapsed_time(&start_time, &end_time);
	return pack_return_values(0, errno, READ_ERROR, bytes,
				  "munmap failed", 0.0, corrected_time,
				  __FILE__, __LINE__);
      }

      /* Reset these values for the next segment. */
      mmap_len = (bytes<mmap_len)?bytes:mmap_len;
      mmap_offset = 0;
      mmap_count += 1;
      mmap_left = mmap_len;
      read_info->mmap_len = mmap_len;  /* Remember this for munmap(). */

      /* Create the memory mapped file. */
      errno = 0;
      if((rd_mmap = mmap(NULL, mmap_len, PROT_WRITE | PROT_READ, MAP_SHARED,
			 rd_fd, mmap_count * mmap_len)) == (caddr_t)-1)
      {
	set_done_flag(&read_done);
	corrected_time = elapsed_time(&start_time, &end_time);
	return pack_return_values(0, errno, READ_ERROR, bytes,
				  "mmap failed", 0.0, corrected_time,
				  __FILE__, __LINE__);
      }
      
      /* Advise the system on the memory mapped i/o usage pattern. */
      errno = 0;
      if(madvise(rd_mmap, mmap_len, MADV_WILLNEED | MADV_SEQUENTIAL) < 0)
      {
	set_done_flag(&write_done);
	corrected_time = elapsed_time(&start_time, &end_time);
	return pack_return_values(0, errno, WRITE_ERROR, bytes,
				  "madvise failed", 0.0, corrected_time,
				  __FILE__, __LINE__);
      }
    }
    else if(mmap_offset == mmap_len)
    {
      /* Reset these values for the next segment. Even if this thread does
       not care about page allignment, the other thread might. */
      mmap_len = (bytes<mmap_len)?bytes:mmap_len;
      mmap_offset = 0;
      mmap_count += 1;
      mmap_left = mmap_len;
      read_info->mmap_len = mmap_len;  /* Remember this for munmap(). */
    }

    /* Determine if the lock for the buffer_lock bin, bin, is ready. */
    if((p_rtn = pthread_mutex_lock(&buffer_lock[bin])) != 0)
    {
      set_done_flag(&read_done);
      corrected_time = elapsed_time(&start_time, &end_time);
      return pack_return_values(0, p_rtn, THREAD_ERROR, bytes,
				"mutex lock failed", corrected_time, 0.0,
				__FILE__, __LINE__);
    }
    if(stored[bin])
    {
      /* This bin still needs to be used by the other thread.  Put this thread
	to sleep until the other thread is done with it. */
      if((p_rtn = pthread_cond_wait(&next_cond, &buffer_lock[bin])) != 0)
      {
	set_done_flag(&read_done);
	corrected_time = elapsed_time(&start_time, &end_time);
	return pack_return_values(0, p_rtn, THREAD_ERROR, bytes,
				  "waiting for condition failed",
				  corrected_time, 0.0, __FILE__, __LINE__);
      }
    }
    if((p_rtn = pthread_mutex_unlock(&buffer_lock[bin])) != 0)
    {
      set_done_flag(&read_done);
      corrected_time = elapsed_time(&start_time, &end_time);
      return pack_return_values(0, p_rtn, THREAD_ERROR, bytes,
				"mutex unlock failed", corrected_time, 0.0,
				__FILE__, __LINE__);
    }

    /* Number of bytes remaining for this loop. */
    bytes_remaining = (bytes < block_size) ? \
	((mmap_left < bytes) ? mmap_left : bytes) : \
      ((mmap_left < block_size) ? mmap_left : block_size);
    /* Do not worry about reading/writing an exact block as this is
       on the user end. But attempt block_size reads. */
    if(S_ISREG(file_info.st_mode) && direct_io)
      bytes_to_transfer = block_size;
    else
      /* Dertermine how much to read. Attempt block size read. */
      bytes_to_transfer = bytes_remaining;
    /* Set this to zero. */
    bytes_transfered = 0;

    while(bytes_remaining > 0)
    {
      /* Initialize select values. */
      errno = 0;
      FD_ZERO(&fds);
      FD_SET(rd_fd,&fds);
      timeout.tv_sec = read_info->timeout.tv_sec;
      timeout.tv_usec = read_info->timeout.tv_usec;

      /* Record the time that this thread goes to sleep waiting for select. */
      gettimeofday(&start_time, NULL);

      /* Wait for there to be data on the descriptor ready for reading. */
      sts = select(rd_fd+1, &fds, NULL, NULL, &timeout);
      if (sts == -1)
      {
	set_done_flag(&read_done);
	corrected_time = elapsed_time(&start_time, &end_time);
	return pack_return_values(0, errno, READ_ERROR, bytes,
				  "fd read error", corrected_time, 0.0,
				  __FILE__, __LINE__);
      }
      if (sts == 0)
      {
	set_done_flag(&read_done);
	corrected_time = elapsed_time(&start_time, &end_time);
	return pack_return_values(0, errno, TIMEOUT_ERROR, bytes,
				  "fd timeout", corrected_time, 0.0,
				  __FILE__, __LINE__);
      }

      /* Read in the data. */
      if(rd_mmap != MAP_FAILED)
      {
	errno = 0;
	memcpy((buffer + (bin * block_size)),
	       (void*)((unsigned int)rd_mmap + (unsigned int)mmap_offset),
	       bytes_to_transfer);
	sts = bytes_to_transfer;
      }
      else
      {
	errno = 0;
	sts = read(rd_fd, (buffer + (bin * block_size) + bytes_transfered),
		   bytes_to_transfer);

	if (sts == -1)
	{
	  set_done_flag(&read_done);
	  corrected_time = elapsed_time(&start_time, &end_time);
	  return pack_return_values(0, errno, READ_ERROR, bytes,
				    "fd read error", corrected_time, 0.0,
				    __FILE__, __LINE__);
	}
	if (sts == 0)
	{
	  set_done_flag(&read_done);
	  corrected_time = elapsed_time(&start_time, &end_time);
	  return pack_return_values(0, errno, TIMEOUT_ERROR, bytes,
				    "fd timeout", corrected_time, 0.0,
				    __FILE__, __LINE__);
	}
      }

      /* Record the time that this thread wakes up from waiting for the
	 condition variable. */
      gettimeofday(&end_time, NULL);
      /* Calculate wait time. */
      transfer_time += elapsed_time(&start_time, &end_time);


      /* Calculate the crc (if applicable). */
      switch (crc_flag)
      {
      case 0:  
	break;
      case 1:  
	crc_ui = adler32(crc_ui,
			(buffer + (bin * block_size) + bytes_transfered), sts);
	break;
      default:  
	crc_ui = 0; 
	break;
      }

      /* Update this nested loop's counting variables. */
      bytes_remaining -= sts;
      bytes_transfered += sts;
      bytes_to_transfer -= sts;
      mmap_offset += sts;
      mmap_left -= sts;
      
#ifdef DEBUG
      /* Print r if entire bin is transfered, R if bin partially transfered. */
      debug_print = (sts < bytes_to_transfer) ? 'R' : 'r';
      pthread_mutex_lock(&print_lock);
      print_status(stderr, debug_print, bytes - bytes_transfered, crc_ui,
		   array_size);
      pthread_mutex_unlock(&print_lock);
#endif /*DEBUG*/
    }

    /* Obtain the mutex lock for the specific buffer bin that is needed to
       clear the bin for writing. */
    if((p_rtn = pthread_mutex_lock(&buffer_lock[bin])) != 0)
    {
      set_done_flag(&read_done);
      corrected_time = elapsed_time(&start_time, &end_time);
      return pack_return_values(0, p_rtn, THREAD_ERROR, bytes,
				"mutex lock failed", corrected_time, 0.0, 
				__FILE__, __LINE__);
    }
    stored[bin] = bytes_transfered; /* Store the number of bytes in the bin. */
    /* If other thread sleeping, wake it up. */
    if((p_rtn = pthread_cond_signal(&next_cond)) != 0)
    {
      set_done_flag(&read_done);
      corrected_time = elapsed_time(&start_time, &end_time);
      return pack_return_values(0, p_rtn, THREAD_ERROR, bytes,
				"waiting for condition failed",
				corrected_time, 0.0, __FILE__, __LINE__);
    }
    /* Release the mutex lock for this bin. */
    if((p_rtn = pthread_mutex_unlock(&buffer_lock[bin])) != 0)
    {
      set_done_flag(&read_done);
      corrected_time = elapsed_time(&start_time, &end_time);
      return pack_return_values(0, p_rtn, THREAD_ERROR, bytes,
				"mutex unlock failed", 
				corrected_time, 0.0, __FILE__, __LINE__);
    }
    /* Determine where to put the data. */
    bin = (bin + 1) % array_size;
    /* Determine the number of bytes left to transfer. */
    bytes -= bytes_transfered;
  }

  if(rd_mmap != MAP_FAILED)
  {
    /* Unmap the final mapped memory segment. */
    errno = 0;
    if(munmap(rd_mmap, mmap_len) < 0)
    {
      set_done_flag(&read_done);
      corrected_time = elapsed_time(&start_time, &end_time);
      return pack_return_values(0, errno, READ_ERROR, bytes,
				"munmap failed", 0.0, corrected_time,
				__FILE__, __LINE__);
    }
  }
  
  /* Get the thread's time usage. */
  getrusage(RUSAGE_SELF, &end_usage);
  /* Get total end time. */
  gettimeofday(&end_total, NULL);

  set_done_flag(&read_done);

  if(S_ISREG(file_info.st_mode))
    corrected_time = elapsed_time(&start_total, &end_total);
  else
    corrected_time = rusage_elapsed_time(start_usage, end_usage)+transfer_time;

  return pack_return_values(crc_ui, 0, 0, bytes, "",
			    corrected_time, 0.0, NULL, 0);
}



static void* thread_write(void *info)
{
  struct transfer *write_info = (struct transfer*)info; /* dereference */
  int wr_fd = write_info->fd;              /* File descriptor to write to. */
  long long bytes = write_info->size;      /* Number of bytes to transfer. */
  int crc_flag = write_info->crc_flag;     /* Flag to enable crc checking. */
  int block_size = write_info->block_size; /* Bytes to transfer at one time. */
  int array_size = write_info->array_size; /* Number of buffer bins. */
  struct timeval timeout = write_info->timeout; /* Time to wait for data. */
  int direct_io = write_info->direct_io;   /* True if using direct io. */
  /*int mmap_io = write_info->mmap_io;*/   /* True if using mem. mapped io. */
  size_t bytes_remaining;       /* Number of bytes to move in one loop. */
  size_t bytes_to_transfer;     /* Bytes to transfer in a single loop. */
  size_t bytes_transfered;      /* Bytes left to transfer in a sub loop. */
  int sts = 0;                  /* Return value from various C system calls. */
  int bin = 0;                  /* The current bin (bucket) to use. */
  unsigned long crc_ui = 0;     /* Calculated checksum. */
  fd_set fds;                   /* For use with select(2). */
  int p_rtn;                    /* Pthread return value. */
#ifdef DEBUG
  char debug_print;             /* Specifies what transfer occured.  W or w */
#endif
  long long fsync_threshold = 0;/* Number of bytes to wait between fsync()s. */
  long long last_fsync = bytes; /* Number of bytes done though last fsync(). */
  struct stat file_info;        /* Information about the file to write to. */
  int do_threshold = 0;         /* Holds boolean true when using fsync(). */
  void *wr_mmap = write_info->mmap_ptr;   /* Pointer to memory mapped i/o. */
  off_t mmap_offset = 0;        /* Offset from beginning of mmapped segment. */
  int mmap_count = 0;           /* Number of mmapped segments done. */
  off_t mmap_len = write_info->mmap_len; /* Length of offset segment. */
  off_t mmap_left = mmap_len - mmap_offset; /* Bytes to next mmap segment. */
  struct timeval start_time;    /* Holds time measurement value. */
  struct timeval end_time;      /* Holds time measurement value. */
  struct rusage start_usage;    /* Hold time info from os billing. */
  struct rusage end_usage;      /* Hold time info from os billing. */
  struct timeval start_total;   /* Hold overall time measurment value. */
  struct timeval end_total;     /* Hold overall time measurment value. */
  double corrected_time = 0.0;  /* Corrected return time. */
  double transfer_time = 0.0;   /* Runing transfer time. */

  /* Initialize the running time incase of early failure. */
  memset(&start_time, 0, sizeof(struct timeval));
  memset(&end_time, 0, sizeof(struct timeval));
  /* Initialize the running time incase of early failure. */
  gettimeofday(&start_total, NULL);
  memcpy(&end_total, &start_total, sizeof(struct timeval));

  /* Get the thread's start time usage. */
  getrusage(RUSAGE_SELF, &start_usage);

  /* Get stat info. */
  errno = 0;
  if(fstat(wr_fd, &file_info) < 0)
  {
    set_done_flag(&write_done);
    corrected_time = elapsed_time(&start_time, &end_time);
    return pack_return_values(0, errno, WRITE_ERROR, bytes,
			      "fstat failed", 0.0, corrected_time,
			      __FILE__, __LINE__);
  }
  /* Determine if the file descriptor supports fsync(). */
  if(S_ISREG(file_info.st_mode))
  {
    /* Get the number of bytes to transfer between fsync() calls. */
    fsync_threshold = get_fsync_threshold(bytes, block_size);
    /* Set this boolean true. */
    do_threshold = 1;
  }

  while(bytes > 0)
  {
    if(wr_mmap != MAP_FAILED && mmap_offset == mmap_len)
    {
      /* Before unmapping the current region, sync the data to the disk. */
      /*errno = 0;
      if(msync(wr_mmap, mmap_len, MS_ASYNC) < 0)
      {
	set_done_flag(&write_done);
	corrected_time = elapsed_time(&start_time, &end_time);
	return pack_return_values(0, errno, WRITE_ERROR, bytes,
				  "msync failed", 0.0, corrected_time,
				  __FILE__, __LINE__);
				  }*/

      /* Unmap the current mapped memory segment. */
      errno = 0;
      if(munmap(wr_mmap, mmap_len) < 0)
      {
	set_done_flag(&write_done);
	corrected_time = elapsed_time(&start_time, &end_time);
	return pack_return_values(0, errno, WRITE_ERROR, bytes,
				  "munmap failed", 0.0, corrected_time,
				  __FILE__, __LINE__);
      }

      /* Reset these values for the next segment. */
      mmap_len = (bytes<mmap_len)?bytes:mmap_len;
      mmap_offset = 0;
      mmap_count += 1;
      mmap_left = mmap_len;
      write_info->mmap_len = mmap_len;  /* Remember this for munmap(). */

      /* Create the memory mapped file. */
      errno = 0;
      if((wr_mmap = mmap(NULL, mmap_len, PROT_WRITE | PROT_READ,
			 MAP_SHARED, wr_fd, 0)) == (caddr_t)-1)
      {
	set_done_flag(&write_done);
	corrected_time = elapsed_time(&start_time, &end_time);
	return pack_return_values(0, errno, WRITE_ERROR, bytes,
				  "mmap failed", 0.0, corrected_time,
				  __FILE__, __LINE__);
      }
      /* Advise the system on the memory mapped i/o usage pattern. */
      errno = 0;
      if(madvise(wr_mmap, mmap_len, MADV_SEQUENTIAL) < 0)
      {
	set_done_flag(&write_done);
	corrected_time = elapsed_time(&start_time, &end_time);
	return pack_return_values(0, errno, WRITE_ERROR, bytes,
				  "madvise failed", 0.0, corrected_time,
				  __FILE__, __LINE__);
      }
    }
    else if(mmap_offset == mmap_len)
    {
      /* Reset these values for the next segment. Even if this thread does
	 not care about page allignment, the other thread might. */
      mmap_len = (bytes<mmap_len)?bytes:mmap_len;
      mmap_offset = 0;
      mmap_count += 1;
      mmap_left = mmap_len;
      write_info->mmap_len = mmap_len;  /* Remember this for munmap(). */
    }

    /* Determine if the lock for the buffer_lock bin, bin, is ready. */
    if((p_rtn = pthread_mutex_lock(&buffer_lock[bin])) != 0)
    {
      set_done_flag(&write_done);
      corrected_time = elapsed_time(&start_time, &end_time);
      return pack_return_values(0, p_rtn, THREAD_ERROR, bytes,
				"mutex lock failed", 0.0, corrected_time,
				__FILE__, __LINE__);
    }
    if(stored[bin] == 0)
    {
      /* This bin still needs to be used by the other thread.  Put this thread
	to sleep until the other thread is done with it. */
      if((p_rtn = pthread_cond_wait(&next_cond, &buffer_lock[bin])) != 0)
      {
	set_done_flag(&write_done);
	corrected_time = elapsed_time(&start_time, &end_time);
	return pack_return_values(0, p_rtn, THREAD_ERROR, bytes,
				  "waiting for condition failed",
				  0.0, corrected_time, __FILE__, __LINE__);
      }
    }
    if((p_rtn = pthread_mutex_unlock(&buffer_lock[bin])) != 0)
    {
      set_done_flag(&write_done);
      corrected_time = elapsed_time(&start_time, &end_time);
      return pack_return_values(0, p_rtn, THREAD_ERROR, bytes,
				"mutex unlock failed", 0.0, corrected_time,
				__FILE__, __LINE__);
    }

    /* Number of bytes remaining for this loop. */
    bytes_remaining = stored[bin];
    /* Do not worry about reading/writing an exact block as this is
       on the user end. But attempt block_size reads. */
    if(S_ISREG(file_info.st_mode) && direct_io)
      bytes_to_transfer = block_size;
    else
      /* Dertermine how much to read. Attempt block size read. */
      bytes_to_transfer = bytes_remaining;
    /* Set this to zero. */
    bytes_transfered = 0;

    while(bytes_remaining > 0)
    {
      /* Wait for there to be room for the descriptor to write to. */
      errno = 0;
      FD_ZERO(&fds);
      FD_SET(wr_fd, &fds);
      timeout.tv_sec = write_info->timeout.tv_sec;
      timeout.tv_usec = write_info->timeout.tv_usec;

      /* Record the time that this thread goes to sleep waiting for the
	 condition variable. */
      gettimeofday(&start_time, NULL);

      sts = select(wr_fd+1, NULL, &fds, NULL, &timeout);
      
      if (sts == -1)
      {
	set_done_flag(&write_done);
	corrected_time = elapsed_time(&start_time, &end_time);
	return pack_return_values(0, p_rtn, WRITE_ERROR, bytes,
				  "fd write error", 0.0, corrected_time,
				  __FILE__, __LINE__);
      }
      if (sts == 0)
      {
	set_done_flag(&write_done);
	corrected_time = elapsed_time(&start_time, &end_time);
	return pack_return_values(0, p_rtn, TIMEOUT_ERROR, bytes, "fd timeout",
				  0.0, corrected_time, __FILE__, __LINE__);
      }

      /* Write out the data */
      if(wr_mmap != MAP_FAILED)
      {
	/* If file supports memory mapped i/o. */
	errno = 0;
	memcpy((void*)((unsigned int)wr_mmap + (unsigned int)mmap_offset),
	       (buffer + (bin * block_size)),
	       bytes_to_transfer);
	/* Schedule the data for sync to disk now. */
	msync((void*)((unsigned int)wr_mmap + (unsigned int)mmap_offset),
	      bytes_to_transfer, MS_ASYNC);
	sts = bytes_to_transfer;
      }
      else
      {
	/* When faster methods will not work, use read()/write(). */
	errno = 0;
	sts = write(wr_fd, (buffer + (bin * block_size) + bytes_transfered),
		    bytes_to_transfer);

	if (sts == -1)
	{
	  set_done_flag(&write_done);
	  corrected_time = elapsed_time(&start_time, &end_time);
	  return pack_return_values(0, errno, WRITE_ERROR, bytes,
				    "fd write error", 0.0, corrected_time, 
				    __FILE__, __LINE__);
	}
	
	/* Use write() with direct io. */
	if(S_ISREG(file_info.st_mode) && direct_io)
	{
	  /* Only apply after the last write() call. */
	  if(bytes_remaining != align_to_page(bytes_remaining))
	  {
	    /* Adjust the sts. */
	    sts = bytes_remaining;
	    /* Truncate size at end of transfer. */
	    ftruncate(wr_fd, write_info->size);
	  }
	}
      }

      /* Force the data to disk.  Don't let encp take up to much memory.
	 This isnt the most accurate way of doing this, however it is less
	 overhead. For accuracy this needs to be before the following
	 gettimeofday() call. */
      if(do_threshold)
      {
	if(last_fsync - bytes > fsync_threshold)
	{
	  last_fsync = bytes;
	  fsync(wr_fd);
	}
      }

      /* Record the time that this thread wakes up from waiting for the
	 condition variable. */
      gettimeofday(&end_time, NULL);
      transfer_time += elapsed_time(&start_time, &end_time);

      /* Calculate the crc (if applicable). */
      switch (crc_flag)
      {
      case 0:  
	break;
      case 1:  
	crc_ui = adler32(crc_ui,
			 (buffer + (bin * block_size) + bytes_transfered),sts);
	/*to cause intentional crc errors, use the following line instead*/
	/*crc_ui=adler32(crc_ui, (buffer), sts);*/
	break;
      default:  
	crc_ui=0; 
	break;
      }

      /* Update this nested loop's counting variables. */
      bytes_remaining -= sts;
      bytes_transfered += sts;
      bytes_to_transfer -= sts;
      mmap_offset += sts;
      mmap_left -= sts;

#ifdef DEBUG
      /* Print w if entire bin is transfered, W if bin partially transfered. */
      debug_print = (sts < bytes_to_transfer) ? 'W' : 'w';
      pthread_mutex_lock(&print_lock);
      print_status(stderr, debug_print, bytes - bytes_transfered, crc_ui,
		   array_size);
      pthread_mutex_unlock(&print_lock);
#endif /*DEBUG*/
    }

    /* Obtain the mutex lock for the specific buffer bin that is needed to
       clear the bin for writing. */
    if((p_rtn = pthread_mutex_lock(&buffer_lock[bin])) != 0)
    {
      set_done_flag(&write_done);
      corrected_time = elapsed_time(&start_time, &end_time);
      return pack_return_values(0, p_rtn, THREAD_ERROR, bytes,
				"mutex lock failed", 0.0, corrected_time, 
				__FILE__, __LINE__);
    }
    stored[bin] = 0; /* Set the number of bytes left in buffer to zero. */
    /* If other thread sleeping, wake it up. */
    if((p_rtn = pthread_cond_signal(&next_cond)) != 0)
    {
      set_done_flag(&write_done);
      corrected_time = elapsed_time(&start_time, &end_time);
      return pack_return_values(0, p_rtn, THREAD_ERROR, bytes,
				"waiting for condition failed",
				0.0, corrected_time, __FILE__, __LINE__);
    }
    /* Release the mutex lock for this bin. */
    if((p_rtn = pthread_mutex_unlock(&buffer_lock[bin])) != 0)
    {
      set_done_flag(&write_done);
      corrected_time = elapsed_time(&start_time, &end_time);
      return pack_return_values(0, p_rtn, THREAD_ERROR, bytes,
				"mutex unlock failed", 0.0, corrected_time,
				__FILE__, __LINE__);
    }

    /* Determine where to get the data. */
    bin = (bin + 1) % array_size;
    /* Determine the number of bytes left to transfer. */
    bytes -= bytes_transfered;
  }

  /* Count time in the final Xsync system calls. */
  gettimeofday(&start_time, NULL);

  if(wr_mmap != MAP_FAILED)
  {
    /* Tell OS to write out the data now. */
    errno = 0;
    if(msync(wr_mmap, mmap_len, MS_SYNC) < 0)
    {
      set_done_flag(&write_done);
      corrected_time = elapsed_time(&start_time, &end_time);
      return pack_return_values(0, errno, WRITE_ERROR, bytes,
				"msync failed", 0.0, corrected_time,
				__FILE__, __LINE__);
    }

    /* Unmap the final mapped memory segment. */
    errno = 0;
    if(munmap(wr_mmap, mmap_len) < 0)
    {
      set_done_flag(&write_done);
      corrected_time = elapsed_time(&start_time, &end_time);
      return pack_return_values(0, errno, WRITE_ERROR, bytes,
				"munmap failed", 0.0, corrected_time,
				__FILE__, __LINE__);
    }
  }
  else
  {
    fsync(wr_fd);
  }

  /* Count time in the final Xsync system calls. */
  gettimeofday(&end_time, NULL);
  transfer_time += elapsed_time(&start_time, &end_time);

  /* Get the thread's time usage. */
  getrusage(RUSAGE_SELF, &end_usage);
  /* Get total end time. */
  gettimeofday(&end_total, NULL);

  set_done_flag(&write_done);

  if(S_ISREG(file_info.st_mode))
    corrected_time = elapsed_time(&start_total, &end_total);
  else
    corrected_time = rusage_elapsed_time(start_usage, end_usage)+transfer_time;

  return pack_return_values(crc_ui, 0, 0, bytes, "",
			    0.0, corrected_time, NULL, 0);
}

#ifdef DEBUG
static void print_status(FILE* fp, char name, long long bytes,
			 unsigned int crc_ui, int array_size)
{
  int i;

  fprintf(stderr, "%cbytes: %15lld crc: %10u | ", name, bytes, crc_ui);

  for(i = 0; i < array_size; i++)
  {
    fprintf(fp, " %6d", stored[i]);
  }
  fprintf(fp, "\n");
}
#endif /*DEBUG*/

/*#else*/

static struct return_values
do_read_write(int rd_fd, int wr_fd, long long size, struct timeval timeout,
	      int crc_flag, int blk_size, int array_size, long mmap_size,
	      int direct_io, int mmap_io, unsigned int *crc_p)
{
  void *buffer;                 /* Location to read/write from/to. */
  /*void *b_p;*/                    /* Buffer pointer for parial writes. */
  ssize_t sts;                  /* Return status from read() and write(). */
  long long bytes = size;       /* Counter for bytes left to transfer. */
  int block_size = align_to_page(blk_size);  /* Align the buffers size. */
  size_t bytes_remaining;       /* Number of bytes to move in one loop. */
  size_t bytes_to_transfer;     /* Number of bytes to move in one loop. */
  size_t bytes_transfered;      /* Number of bytes moved in one loop. */
  fd_set fds;                   /* FD to write to. */
  struct timeval timeout_use;   /* Timeout for selet() operation. */
#ifdef PROFILE
  struct profile profile_data[PROFILE_COUNT]; /* profile data array */
  long profile_count = 0;       /* Index of profile array. */
#endif /*PROFILE*/
  struct timeval start_time;    /* Start of time the thread is active. */
  struct timeval end_time;      /* End of time the thread is active. */
  double time_elapsed;          /* Difference between start and end time. */
#ifdef DEBUG
  char debug_print;             /* Specifies what transfer occured.  W or w */
#endif
  long long fsync_threshold = 0;/* Number of bytes to wait between fsync()s. */
  long long last_fsync = bytes; /* Number of bytes done though last fsync(). */
  struct stat file_info;        /* Information about the file to write to. */
  int do_threshold = 0;         /* Holds boolean true when using fsync(). */

  /* Determine if the file descriptor supports fsync(). */
  if(fstat(wr_fd, &file_info))
  {
    fprintf(stderr, "fstat: %s\n", strerror(errno));
  }
  else
  {
    if(S_ISREG(file_info.st_mode))
    {
      /* Get the number of bytes to transfer between fsync() calls. */
      fsync_threshold = get_fsync_threshold(bytes, block_size);
      /* Set this boolean true. */
      do_threshold = 1;
    }
  }

  errno = 0;
  if((buffer = memalign(sysconf(_SC_PAGESIZE), block_size)) == NULL)
    return *pack_return_values(0, errno, THREAD_ERROR, bytes,
			       "memalign failed", 0.0, 0.0,
			       __FILE__, __LINE__);

  /* Get the time that the thread started to work on transfering data. */
  gettimeofday(&start_time, NULL);
  memcpy(&end_time, &start_time, sizeof(struct timeval));

  while(bytes > 0)
  {
    /* Number of bytes remaining for this loop. */
    bytes_remaining = (bytes<block_size)?bytes:block_size;
    /* Do not worry about reading/writing an exact block as this is
       on the user end. But attempt block_size reads. */
    if(direct_io)
      bytes_to_transfer = block_size;
    else
      bytes_to_transfer = bytes_remaining;
    /* Set this to zero. */
    bytes_transfered = 0;

    while(bytes_remaining > 0)
    {
      FD_ZERO(&fds);
      FD_SET(rd_fd,&fds);
      timeout_use.tv_sec = timeout.tv_sec;
      timeout_use.tv_usec = timeout.tv_usec;
      errno=0;
#ifdef PROFILE
      update_profile(1, bytes_to_transfer, wr_fd, profile_data, &profile_count);
#endif
      sts = select(rd_fd+1, &fds, NULL, NULL, &timeout_use);
#ifdef PROFILE
      update_profile(2, bytes_to_transfer, wr_fd, profile_data, &profile_count);
#endif
      if (sts == -1)
      { /* return/break - read error */
	fprintf(stderr, "Low-level I/O failure: " 
		"select(%d, [%d], [], [], {%ld, %ld}) [Errno %d] "
		"%s: higher encp levels will process this error "
		"and retry if possible\n",
		rd_fd+1, rd_fd, timeout.tv_sec, timeout.tv_usec,
		errno, strerror(errno));
	fflush(stderr);
	time_elapsed = elapsed_time(&start_time, &end_time);
	return *pack_return_values(0, errno, READ_ERROR, bytes,
				   "select error", time_elapsed, time_elapsed,
				   __FILE__, __LINE__);
      }
      if (sts == 0)
      {
	/* timeout - treat as an EOF */
	time_elapsed = elapsed_time(&start_time, &end_time);
	return *pack_return_values(0, errno, TIMEOUT_ERROR, bytes,
				   "fd timeout", time_elapsed, time_elapsed,
				   __FILE__, __LINE__);
      }

      errno = 0;
#ifdef PROFILE
      update_profile(3, bytes_to_transfer, wr_fd, profile_data, &profile_count);
#endif
      sts = read(rd_fd, (void*)((int)buffer + (int)bytes_transfered),
		 bytes_to_transfer);
#ifdef PROFILE
      update_profile(4, sts, wr_fd, profile_data, &profile_count);
#endif
      if (sts == -1)
      { /* return/break - read error */
	fprintf(stderr, "Low-level I/O failure: "
		"read(%d, %#x, %lld) -> %lld, [Errno %d] %s: "
		"higher encp levels will process this error "
		"and retry if possible\n",
		rd_fd, (unsigned)buffer, (long long)bytes_to_transfer,
		(long long)sts, errno, strerror(errno));
	fflush(stderr);
	time_elapsed = elapsed_time(&start_time, &end_time);
	return *pack_return_values(0, errno, READ_ERROR, bytes,
				   "read error", time_elapsed, time_elapsed, 
				   __FILE__, __LINE__);
      }
      if (sts == 0)
      { /* return/break - unexpected eof error */
	time_elapsed = elapsed_time(&start_time, &end_time);
	return *pack_return_values(0, errno, TIMEOUT_ERROR, bytes,
				   "fd timeout", time_elapsed, time_elapsed, 
				   __FILE__, __LINE__);
      }

      bytes_remaining -= sts;
      bytes_to_transfer -= sts;
      bytes_transfered += sts;

#ifdef DEBUG
      /* Print r if entire bin is transfered, R if bin partially transfered. */
      debug_print = (bytes_to_transfer > 0) ? 'R' : 'r';
      fprintf(stderr, "%cbytes: %15lld crc: %10u | sts: %d btt: %d\n", debug_print, bytes, *crc_p, sts, bytes_to_transfer);
#endif /*DEBUG*/
    }
    
    /* Initialize the write loop variables. */
    bytes_to_transfer = bytes_transfered;
    bytes_transfered = 0;

    while (bytes_to_transfer > 0)
    {
      FD_ZERO(&fds);
      FD_SET(wr_fd,&fds);
      timeout_use.tv_sec = timeout.tv_sec;
      timeout_use.tv_usec = timeout.tv_usec;
      errno=0;
#ifdef PROFILE
      update_profile(5, bytes_to_transfer, wr_fd, profile_data, &profile_count);
#endif
      sts = select(wr_fd+1, NULL, &fds, NULL, &timeout_use);
#ifdef PROFILE
      update_profile(6, bytes_to_transfer, wr_fd, profile_data, &profile_count);
#endif
      if (sts == -1)
      { /* return/break - write error */
	fprintf(stderr, "Low-level I/O failure: " 
		"select(%d, [], [%d], [], {%ld, %ld}) [Errno %d] "
		"%s: higher encp levels will process this error "
		"and retry if possible\n",
		wr_fd+1, wr_fd, timeout.tv_sec, timeout.tv_usec,
		errno, strerror(errno));
	fflush(stderr);
	time_elapsed = elapsed_time(&start_time, &end_time);
	return *pack_return_values(0, errno, WRITE_ERROR, bytes,
				   "select error", time_elapsed,
				   time_elapsed, __FILE__, __LINE__);
      }
      if (sts == 0)
      {	/* timeout - treat as an EOF */
	time_elapsed = elapsed_time(&start_time, &end_time);
	return *pack_return_values(0, errno, TIMEOUT_ERROR, bytes,
				   "fd timeout", time_elapsed, time_elapsed, 
				   __FILE__, __LINE__);
      }
      

      errno=0;
#ifdef PROFILE
      update_profile(7, bytes_to_transfer, wr_fd, profile_data, &profile_count);
#endif
      if(direct_io)
      {
	sts = write(wr_fd, buffer, align_to_page(bytes_to_transfer));
	/* Adjust the sts.  Should only apply to the last write() call. */
	if(sts > 0)
	  sts = bytes_to_transfer;
	/* Truncate size at end of transfer. */
	if((align_to_page(bytes_to_transfer) - bytes_to_transfer) ==
	   (align_to_page(size) - size))
	  ftruncate(wr_fd, size);
      }
      else
	sts = write(wr_fd, (void*)((int)buffer + (int)bytes_transfered),
		    bytes_to_transfer);
#ifdef PROFILE
      update_profile(8, sts, wr_fd, profile_data, &profile_count);
#endif
      if (sts == -1)
      {   /* return a write error */
	fprintf(stderr, "Low-level I/O failure: "
		"write(%d, %p, %lld) -> %ld, [Errno %d] %s:"
		"higher encp levels will process this error "
		"and retry if possible\n",
		wr_fd, (void*)((int)buffer + (int)bytes_transfered),
		(long long)bytes_to_transfer,
		(long)sts, errno, strerror(errno));
	fflush(stderr);
	time_elapsed = elapsed_time(&start_time, &end_time);
	return *pack_return_values(0, errno, WRITE_ERROR, bytes,
				   "fd writeerror", time_elapsed,
				   time_elapsed, __FILE__, __LINE__);
      }
      switch (crc_flag)
      {
      case 0:
	break;
      case 1:  
	*crc_p=adler32(*crc_p, (void*)((int)buffer + (int)bytes_transfered),
		       sts); 
	break;
      default:  
	fprintf(stderr, "fd_xfer: invalid crc flag"); 
	*crc_p=0; 
	break;
      }
      bytes_to_transfer -= sts;
      bytes_transfered += sts;
      /*b_p += sts;*/
      /*bytes -= sts;*/

#ifdef DEBUG
      /* Print w if entire bin is transfered, W if bin partially transfered. */
      debug_print = (bytes_to_transfer > 0) ? 'W' : 'w';
      fprintf(stderr, "%cbytes: %15lld crc: %10u | sts: %d btt: %d\n", debug_print, bytes, *crc_p, sts, bytes_to_transfer);
#endif /*DEBUG*/
    }

    bytes -= bytes_transfered;

    /* If the difference is great enough flush the write buffer.  Also,
       flush the buffer if it is the end of the file. */
    if(do_threshold && 
       (((last_fsync - bytes) > fsync_threshold) || (bytes == 0)))
    {
      last_fsync = bytes;
      
      if(fsync(wr_fd))
      {
	fprintf(stderr, "fsync: %s\n", strerror(errno));
      }
    }

  }

  /* Get the time that the thread finished to work on transfering data. */
  gettimeofday(&end_time, NULL);
  time_elapsed = elapsed_time(&start_time, &end_time);

  /* Release the buffer memory. */
  free(buffer);

#ifdef PROFILE
  print_profile(profile_data, profile_count);
#endif /*PROFILE*/

  return *pack_return_values(*crc_p, 0, 0, bytes, "", time_elapsed,
			     time_elapsed,0, 0);
}
/*#endif */

#ifdef PROFILE
void update_profile(int whereami, int sts, int sock,
		    struct profile *profile_data, long *profile_count)
{
  int size_var = sizeof(int);

  if(*profile_count < PROFILE_COUNT)
  {
    profile_data[*profile_count].whereami = whereami;
    profile_data[*profile_count].status = sts;
    gettimeofday(&(profile_data[*profile_count].time), NULL);
    getsockopt(sock, SOL_SOCKET, SO_ERROR,
	       &profile_data[*profile_count].error, &size_var); 
    
    (*profile_count)++;
  }
}

void print_profile(struct profile *profile_data, int profile_count)
{
  int i;

  for(i = 0; i < profile_count; i++)
    printf("%4d: sec: %11ld usec: %9ld  size: %10d  error: %3d\n",
	   profile_data[i].whereami,
	   profile_data[i].time.tv_sec,
	   profile_data[i].time.tv_usec,
	   profile_data[i].status,
	   profile_data[i].error);
}
#endif /*PROFILE*/

/***************************************************************************
 python defined functions
**************************************************************************/

#ifndef STAND_ALONE

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

static PyObject *
raise_exception2(struct return_values *rtn_val)
{
    PyObject	*v;
    int		i = rtn_val->errno_val;
  
#   ifdef EINTR
    if ((i==EINTR) && PyErr_CheckSignals()) return NULL;
#   endif

    /* note: format should be the same as in FTT.c */
    /* What does the above comment mean??? */
    v = Py_BuildValue("(s,i,s,i,O,O,O,s,i)",
		      rtn_val->msg, i, strerror(i), getpid(),
		      PyLong_FromLongLong(rtn_val->size),
		      PyFloat_FromDouble(rtn_val->read_time),
		      PyFloat_FromDouble(rtn_val->write_time),
		      rtn_val->filename, rtn_val->line);
    if (v != NULL)
    {   PyErr_SetObject(EXErrObject, v);
	Py_DECREF(v);
    }
    return NULL;
}

static PyObject *
EXfd_xfer(PyObject *self, PyObject *args)
{
    int		 fr_fd;
    int		 to_fd;
    long long    no_bytes;
    int		 block_size;
    int          array_size;
    long         mmap_size;
    int          direct_io;
    int          mmap_io;
    int          threaded_transfer;
    PyObject     *no_bytes_obj;
    PyObject	 *crc_obj_tp;
    PyObject	 *crc_tp=Py_None;/* optional, ref. FTT.fd_xfer */
    PyObject     *mmap_size_obj;
    int          crc_flag=0; /*0: no CRC 1: Adler32 CRC >1: RFU */
    unsigned int crc_ui;
    struct timeval timeout = {0, 0};
    int sts;
    PyObject	*rr;
    struct return_values transfer_sts;
    
    sts = PyArg_ParseTuple(args, "iiOOiiiOiii|O", &fr_fd, &to_fd,
			   &no_bytes_obj, &crc_obj_tp, &timeout.tv_sec,
			   &block_size, &array_size, &mmap_size_obj,
			   &direct_io, &mmap_io, &threaded_transfer, &crc_tp);
    if (!sts) return (NULL);
    if (crc_tp == Py_None)
	crc_ui = 0;
    else if (PyLong_Check(crc_tp))
	crc_ui = PyLong_AsUnsignedLong(crc_tp);
    else if (PyInt_Check(crc_tp))
	crc_ui = (unsigned)PyInt_AsLong(crc_tp);
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
	fprintf(stderr, "fd_xfer - invalid crc param");

    /* determine mmap array size */
    if (PyLong_Check(mmap_size_obj))
	mmap_size = PyLong_AsLong(mmap_size_obj);
    else if (PyInt_Check(mmap_size_obj))
	mmap_size = (long long)PyInt_AsLong(mmap_size_obj);
    else
	return(raise_exception("fd_xfer - invalid mmap_size param"));

    errno = 0;
    if(threaded_transfer)
      transfer_sts = do_read_write_threaded(fr_fd, to_fd, no_bytes, timeout,
					    crc_flag, block_size, array_size,
					    mmap_size,
					    direct_io, mmap_io,  &crc_ui);
    else
      transfer_sts = do_read_write(fr_fd, to_fd, no_bytes, timeout,
				   crc_flag, block_size, array_size,
				   mmap_size,
				   direct_io, mmap_io,  &crc_ui);

    if (transfer_sts.exit_status != 0)
        return (raise_exception2(&transfer_sts));

    rr = Py_BuildValue("(i,O,O,i,s,O,O,s,i)",
		       transfer_sts.exit_status, 
		       PyLong_FromUnsignedLong(transfer_sts.crc_ui),
		       PyLong_FromLongLong(transfer_sts.size),
		       transfer_sts.errno_val, transfer_sts.msg,
		       PyFloat_FromDouble(transfer_sts.read_time),
		       PyFloat_FromDouble(transfer_sts.write_time),
		       transfer_sts.filename, transfer_sts.line);

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

#else
/* Stand alone version of exfer is prefered. */

int main(int argc, char **argv)
{
  int fd_in, fd_out;
  struct stat file_info;
  long long size;
  struct timeval timeout = {60, 0};
  struct return_values transfer_sts;
  unsigned int crc_ui;
  int flags = 0;
  int opt;
  int          block_size = 256*1024;
  int          array_size = 3;
  long         mmap_size = 96*1024*1024;
  int          direct_io = 0;
  int          mmap_io= 0;
  int          threaded_transfer = 0;
  
  while((opt = getopt(argc, argv, "tmda:b:l:")) != -1)
  {
    switch(opt)
    {
    case 't':  /* threaded transfer */
      threaded_transfer = 1;
      break;
    case 'm':  /* memory mapped i/o */
      mmap_io = 1;
      break;
    case 'd':  /* direct i/o */
      direct_io = 1;
      flags |= O_DIRECT;
      break;
    case 'a':  /* array size */
      errno = 0;
      if((array_size = (int)strtol(optarg, NULL, 0)) == 0)
      {
	printf("invalid array size(%s): %s\n", optarg, strerror(errno));
	return 1;
      }
      break;
    case 'b':  /* block size */
      errno = 0;
      if((block_size = (int)strtol(optarg, NULL, 0)) == 0)
      {
	printf("invalid array size(%s): %s\n", optarg, strerror(errno));
	return 1;
      }
      break;
    case 'l':  /*mmap length */
      errno = 0;
      if((mmap_size = strtol(optarg, NULL, 0)) == 0)
      {
	printf("invalid mmap size(%s): %s\n", optarg, strerror(errno));
	return 1;
      }
      break;
    default:
      printf("Unknown: %d\n", opt);
    }
  }

  /* Check the number of arguments from the command line. */
  if(argc < 3)
  {
    printf("Usage: test_disk [-tmd] <file1> <files2>\n");
    return 1;
  }
  
  /* Open the input file. */
  errno = 0;
  if((fd_in = open(argv[optind], O_RDONLY | flags)) < 0)
  {
    printf("input open(%s): %s\n", argv[optind], strerror(errno));
    return 1;
  }
  
  /* Open the output file. */
  errno = 0;
  if((fd_out = open(argv[optind+1], O_WRONLY | O_CREAT | O_TRUNC | flags,
		    S_IRUSR | S_IWUSR | S_IRGRP)) < 0)
  {
    printf("output open(%s): %s\n", argv[optind+1], strerror(errno));
    return 1;
  }

  /* Get the file size. */
  errno = 0;
  if(fstat(fd_in, &file_info))
  {
    printf("fstat(): %s\n", strerror(errno));
  }

  /* If reading from /dev/zero, set the size. */
  if(file_info.st_size == 0)
    size = 1024*1024*1024;  /* 1GB */
  else
    size = file_info.st_size;

  /* Do the transfer test. */
  errno = 0;
  if(threaded_transfer)
    transfer_sts = do_read_write_threaded(fd_in, fd_out,
					  size,
					  timeout,
					  1, /*crc flag*/
					  block_size, array_size, mmap_size,
					  direct_io, mmap_io, 
					  &crc_ui);
  else
    transfer_sts = do_read_write(fd_in, fd_out,
				 size,
				 timeout,
				 1, /*crc flag*/
				 block_size, array_size, mmap_size,
				 direct_io, mmap_io, 
				 &crc_ui);
  
  printf("Read rate: %f  Write rate: %f\n",
	 size/(1024*1024)/transfer_sts.read_time,
	 size/(1024*1024)/transfer_sts.write_time);
}

#endif
