#include <Python.h>
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
#ifdef THREADED
#include <pthread.h>
#endif

/***************************************************************************
 constants
**************************************************************************/

/* xfs extention */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

/* return/break - read error */
#define READ_ERROR (-1)
/* timeout - treat as an EOF */
#define TIMEOUT_ERROR (-2)
/* return a write error */
#define WRITE_ERROR (-3)

/*Number of buffer bins to use*/
#ifdef THREADED
#define ARRAY_SIZE 5
#define THREAD_ERROR (-4)
#endif /*THREADED*/

/* Define DEBUG only for extra debugging output */
/*#define DEBUG*/
/*#define PROFILE*/
#ifdef PROFILE
#define PROFILE_COUNT 25000
#endif

/* Number of seconds before timing out. (15min * 60sec/min = 900sec)*/
#define TIMEOUT 900

/***************************************************************************
 definitions
**************************************************************************/

#ifdef THREADED
struct transfer
{
  int fd;                 /*file descriptor*/
  long long size;         /*size in bytes*/
  int block_size;         /*size of block*/
  struct timeval timeout; /*time to wait for data to be ready*/
  int crc_flag;           /*crc flag - 0 or 1*/
};
#endif /*THREADED*/

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

void initEXfer(void);
static PyObject * raise_exception(char *msg);
static PyObject * EXfd_xfer(PyObject *self, PyObject *args);
static struct return_values do_read_write(int rd_fd, int wr_fd,
					  long long bytes, int blk_size,
					  struct timeval timeout,
					  int crc_flag, unsigned int *crc_p);
static struct return_values* pack_return_values(unsigned int crc_ui,
						int errno_val, int exit_status,
						long long bytes, char* msg,
						double read_time,
						double write_time,
						char *filename, int line);
static double elapsed_time(struct timeval* start_time,
			   struct timeval* end_time);
static long long get_fsync_threshold(long long bytes, int blk_size);
#ifdef PROFILE
void update_profile(int whereami, int sts, int sock,
		    struct profile *profile_data, long *profile_count);
void print_profile(struct profile *profile_data, int profile_count);
#endif /*PROFILE*/
#ifdef THREADED
static void* thread_read(void *info);
static void* thread_write(void *info);
void set_done_flag(int* done);
#ifdef DEBUG
static void print_status(FILE*, char, long long, unsigned int);
#endif /*DEBUG*/
#endif /*THREADED*/

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
pthread_cond_t next_cond;   /*used to signal peer thread to continue*/
int read_done = 0, write_done = 0; /*flags to signal which thread finished*/
#ifdef DEBUG
pthread_mutex_t print_lock; /*order debugging output*/
#endif
#endif

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

#ifdef THREADED

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

static struct return_values
do_read_write(int rd_fd, int wr_fd, long long bytes, int blk_size,
	      struct timeval timeout, int crc_flag, unsigned int *crc_p)
{
  /*setup local variables*/
  struct transfer reads;   /* Information passed into read thread. */
  struct transfer writes;  /* Information passed into write thread. */
  struct return_values *read_val = NULL;  /*Incase of early thread ... */
  struct return_values *write_val = NULL; /*... error set to NULL.*/
  struct return_values *rtn_val = malloc(sizeof(struct return_values));

  struct stat file_info;        /* Information about the file to write to. */

#ifdef F_DIOINFO
  struct dioattr memalign_val;  /* Information about xfs memory alignment. */
#endif

  pthread_t read_tid, write_tid;
  /*int exit_status;*/
  int i;
  int p_rtn;

  /*Place the values into the struct.  Some compilers complained when this
    information was placed into the struct inline at initalization.  So it
    was moved here.*/
  reads.fd = rd_fd;
  reads.size = bytes;
  reads.block_size = blk_size;
  reads.timeout = timeout;
#ifdef DEBUG
  reads.crc_flag = crc_flag;
#else
  reads.crc_flag = 0;
#endif
  writes.fd = wr_fd;
  writes.size = bytes;
  writes.block_size = blk_size;
  writes.timeout = timeout;
  writes.crc_flag = crc_flag;

  /* Don't forget to reset these.  These are globals and need to be cleared
     between multi-file transfers. */
  read_done = write_done = 0;

  /* Determine if the file descriptor is a real file. */
  if(fstat(wr_fd, &file_info))
  {
    fprintf(stderr, "fstat: %s\n", strerror(errno));
  }
  else
  {
    if(S_ISREG(file_info.st_mode))
    {
#ifdef O_DIRECT
      /* If xfs extension supported use direct i/o to avoid kernel
	 buffering. */
      fcntl(wr_fd, F_SETFL, O_SYNC | O_DIRECT);
#else
      fcntl(wr_fd, F_SETFL, O_SYNC);
#endif
#ifdef F_DIOINFO
      /* If xfs extension supported. */
      fcntl(wr_fd, F_DIOINFO, &memalign_val);
#endif
    }
  }

  /* Determine if the file descriptor is a real file. */
  if(fstat(rd_fd, &file_info))
  {
    fprintf(stderr, "fstat: %s\n", strerror(errno));
  }
  else
  {
    if(S_ISREG(file_info.st_mode))
    {
#ifdef O_DIRECT
      /* If xfs extension supported use direct i/o to avoid kernel
	 buffering. */
      fcntl(rd_fd, F_SETFL, O_SYNC | O_DIRECT);
#else
      fcntl(rd_fd, F_SETFL, O_SYNC);
#endif
#ifdef F_DIOINFO
      fcntl(rd_fd, F_DIOINFO, &memalign_val);
#endif
    }
  }

#if defined( O_DIRECT ) && defined( F_DIOINFO )
  /* If xfs direct i/o is available make sure the memory is aligned. */
  buffer = memalign(memalign_val.d_mem, ARRAY_SIZE * blk_size);
  memset(buffer, 0, ARRAY_SIZE * blk_size);
#elif defined( O_DIRECT )
  /* If xfs direct i/o is available make sure the memory is aligned.  If
   this code is compiled use best guess alignment size. */
  buffer = memalign(4096, ARRAY_SIZE * blk_size);
  memset(buffer, 0, ARRAY_SIZE * blk_size);
#else
  /*allocate (and initalize) memory for the global pointers*/
  buffer = calloc(ARRAY_SIZE, blk_size);
#endif
  stored = calloc(ARRAY_SIZE, sizeof(int));
  buffer_lock = calloc(ARRAY_SIZE, sizeof(pthread_mutex_t));

  /* initalize the conditional variable signaled when a thread has finished. */
  pthread_cond_init(&done_cond, NULL);
  /* initalize the conditional vaasdfriable to signal peer thread to continue. */
  pthread_cond_init(&next_cond, NULL);
  /* initalize the mutex for signaling when a thread has finished. */
  pthread_mutex_init(&done_mutex, NULL);
#ifdef DEBUG
  /* initalize the mutex for ordering debugging output. */
  pthread_mutex_init(&print_lock, NULL);
#endif
  /* initalize the array of bin mutex locks. */
  for(i = 0; i < ARRAY_SIZE; i++)
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
  struct timeval timeout = read_info->timeout; /* Time to wait for data. */
  int bytes_to_transfer;        /* Bytes to transfer in a single loop. */
  int sts;                      /* Return value from various C system calls. */
  int bin = 0;                  /* The current bin (bucket) to use. */
  unsigned int crc_ui = 0;      /* Calculated checksum. */
  fd_set fds;                   /* For use with select(2). */
  int p_rtn;                    /* Pthread return value. */
  struct timeval start_time;    /* Start of time the thread is active. */
  struct timeval end_time;      /* End of time the thread is active. */
  struct timeval start_wait;    /* Begin waiting for condition variable. */
  struct timeval end_wait;      /* Stop waiting for condition variable. */
  double wait_time = 0.0;       /* Time waiting for condition variable. */
  double corrected_time = 0.0;

  /* Get the time that the thread started to work on reading. */
  gettimeofday(&start_time, NULL);
  memcpy(&end_time, &start_time, sizeof(struct timeval));

  while(bytes > 0)
  {
    /* Determine if the lock for the buffer_lock bin, bin, is ready. */
    if((p_rtn = pthread_mutex_lock(&buffer_lock[bin])) != 0)
    {
      set_done_flag(&read_done);
      corrected_time = elapsed_time(&start_time, &end_time) - wait_time;
      return pack_return_values(0, p_rtn, THREAD_ERROR, bytes,
				"mutex lock failed", corrected_time, 0.0,
				__FILE__, __LINE__);
    }
    if(stored[bin])
    {
      /* Record the time that this thread goes to sleep waiting for the
	 condition variable. */
      gettimeofday(&start_wait, NULL);

      /* This bin still needs to be used by the other thread.  Put this thread
	to sleep until the other thread is done with it. */
      if((p_rtn = pthread_cond_wait(&next_cond, &buffer_lock[bin])) != 0)
      {
	set_done_flag(&read_done);
	corrected_time = elapsed_time(&start_time, &end_time) - wait_time;
	return pack_return_values(0, p_rtn, THREAD_ERROR, bytes,
				  "waiting for condition failed",
				  corrected_time, 0.0, __FILE__, __LINE__);
      }
      
      /* Record the time that this thread wakes up from waiting for the
	 condition variable. */
      gettimeofday(&end_wait, NULL);
      /* Calculate wait time. */
      wait_time += elapsed_time(&start_wait, &end_wait);
    }
    if((p_rtn = pthread_mutex_unlock(&buffer_lock[bin])) != 0)
    {
      set_done_flag(&read_done);
      corrected_time = elapsed_time(&start_time, &end_time) - wait_time;
      return pack_return_values(0, p_rtn, THREAD_ERROR, bytes,
				"mutex unlock failed", corrected_time, 0.0,
				__FILE__, __LINE__);
    }

    /* Wait for there to be data on the descriptor ready for reading. */
    errno = 0;
    FD_ZERO(&fds);
    FD_SET(rd_fd,&fds);
    timeout.tv_sec = read_info->timeout.tv_sec;
    timeout.tv_usec = read_info->timeout.tv_usec;
    sts = select(rd_fd+1, &fds, NULL, NULL, &timeout);
    if (sts == -1)
    {
      set_done_flag(&read_done);
      corrected_time = elapsed_time(&start_time, &end_time) - wait_time;
      return pack_return_values(0, errno, READ_ERROR, bytes, "fd read error", 
				corrected_time, 0.0, __FILE__, __LINE__);
    }
    if (sts == 0)
    {
      set_done_flag(&read_done);
      corrected_time = elapsed_time(&start_time, &end_time) - wait_time;
      return pack_return_values(0, errno, TIMEOUT_ERROR, bytes, "fd timeout", 
				corrected_time, 0.0, __FILE__, __LINE__);
    }

    /* Dertermine how much to read. Attempt block size read. */
    bytes_to_transfer = (bytes<block_size)?bytes:block_size;

    /* Read in the data. */
    errno = 0;
    sts = read(rd_fd, (buffer+bin*block_size), bytes_to_transfer);
    if (sts == -1)
    {
      set_done_flag(&read_done);
      corrected_time = elapsed_time(&start_time, &end_time) - wait_time;
      return pack_return_values(0, errno, READ_ERROR, bytes, "fd read error",
				corrected_time, 0.0, __FILE__, __LINE__);
    }
    if (sts == 0)
    {
      set_done_flag(&read_done);
      corrected_time = elapsed_time(&start_time, &end_time) - wait_time;
      return pack_return_values(0, errno, TIMEOUT_ERROR, bytes, "fd timeout",
				corrected_time, 0.0, __FILE__, __LINE__);
    }
    /* Calculate the crc (if applicable). */
    switch (crc_flag)
    {
    case 0:  
      break;
    case 1:  
      crc_ui = adler32(crc_ui, (buffer+bin*block_size), sts);
      break;
    default:  
      crc_ui = 0; 
      break;
    }

    /* Obtain the mutex lock for the specific buffer bin that is needed to
       clear the bin for writing. */
    if((p_rtn = pthread_mutex_lock(&buffer_lock[bin])) != 0)
    {
      set_done_flag(&read_done);
      corrected_time = elapsed_time(&start_time, &end_time) - wait_time;
      return pack_return_values(0, p_rtn, THREAD_ERROR, bytes,
				"mutex lock failed", corrected_time, 0.0, 
				__FILE__, __LINE__);
    }
    stored[bin] = sts; /* Store the number of bytes in the bin. */
    /* If other thread sleeping, wake it up. */
    if((p_rtn = pthread_cond_signal(&next_cond)) != 0)
    {
      set_done_flag(&read_done);
      corrected_time = elapsed_time(&start_time, &end_time) - wait_time;
      return pack_return_values(0, p_rtn, THREAD_ERROR, bytes,
				"waiting for condition failed",
				corrected_time, 0.0, __FILE__, __LINE__);
    }

#ifdef DEBUG
    pthread_mutex_lock(&print_lock);
    print_status(stderr, 'r', bytes, crc_ui);
    pthread_mutex_unlock(&print_lock);
#endif

    /* Release the mutex lock for this bin. */
    if((p_rtn = pthread_mutex_unlock(&buffer_lock[bin])) != 0)
    {
      set_done_flag(&read_done);
      corrected_time = elapsed_time(&start_time, &end_time) - wait_time;
      return pack_return_values(0, p_rtn, THREAD_ERROR, bytes,
				"mutex unlock failed", 
				corrected_time, 0.0, __FILE__, __LINE__);
    }
    /* Determine where to put the data. */
    bin = (bin + 1) % ARRAY_SIZE;
    /* Determine the number of bytes left to transfer. */
    bytes -= sts;
  }

  /* Get the time that the thread finished to work on reading. */
  gettimeofday(&end_time, NULL);

  set_done_flag(&read_done);
  corrected_time = elapsed_time(&start_time, &end_time) - wait_time;
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
  struct timeval timeout = write_info->timeout; /* Time to wait for data. */
  int bytes_to_transfer;        /* Bytes to transfer in a single loop. */
  int bytes_transfered;         /* Bytes left to transfer in a sub loop. */
  int sts = 0;                  /* Return value from various C system calls. */
  int bin = 0;                  /* The current bin (bucket) to use. */
  unsigned long crc_ui = 0;     /* Calculated checksum. */
  fd_set fds;                   /* For use with select(2). */
  int p_rtn;                    /* Pthread return value. */
#ifdef DEBUG
  char debug_print;             /* Specifies what transfer occured.  W or w */
#endif
  struct timeval start_time;    /* Start of time the thread is active. */
  struct timeval end_time;      /* End of time the thread is active. */
  struct timeval start_wait;    /* Begin waiting for condition variable. */
  struct timeval end_wait;      /* Stop waiting for condition variable. */
  double wait_time = 0.0;       /* Time waiting for condition variable. */
  double corrected_time = 0.0;
  int skipped_first_pass = 0;   /* Skips first buffer in timing rates. */
  long long fsync_threshold = 0;/* Number of bytes to wait between fsync()s. */
  long long last_fsync = bytes; /* Number of bytes done though last fsync(). */
  struct stat file_info;        /* Information about the file to write to. */
  int do_threshold = 0;         /* Holds boolean true when using fsync(). */

  /* Initialize these values in case of error there difference will be zero. */
  memset(&start_time, 0, sizeof(struct timeval));
  memset(&end_time, 0, sizeof(struct timeval));

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

  while(bytes > 0)
  {
    /* Throw away the first buffer in time calcultions. */
    if(!skipped_first_pass)
    {
      skipped_first_pass = 1; /* Set the boolean to true. */

      /* Get the time that the thread started to work on reading. */
      gettimeofday(&start_time, NULL);
      memcpy(&end_time, &start_time, sizeof(struct timeval));
    }

    /* Determine if the lock for the buffer_lock bin, bin, is ready. */
    if((p_rtn = pthread_mutex_lock(&buffer_lock[bin])) != 0)
    {
      set_done_flag(&write_done);
      corrected_time = elapsed_time(&start_time, &end_time) - wait_time;
      return pack_return_values(0, p_rtn, THREAD_ERROR, bytes,
				"mutex lock failed", 0.0, corrected_time,
				__FILE__, __LINE__);
    }
    if(stored[bin] == 0)
    {
      /* Record the time that this thread goes to sleep waiting for the
	 condition variable. */
      gettimeofday(&start_wait, NULL);

      /* This bin still needs to be used by the other thread.  Put this thread
	to sleep until the other thread is done with it. */
      if((p_rtn = pthread_cond_wait(&next_cond, &buffer_lock[bin])) != 0)
      {
	set_done_flag(&write_done);
	corrected_time = elapsed_time(&start_time, &end_time) - wait_time;
	return pack_return_values(0, p_rtn, THREAD_ERROR, bytes,
				  "waiting for condition failed",
				  0.0, corrected_time, __FILE__, __LINE__);
      }
      /* Record the time that this thread wakes up from waiting for the
	 condition variable. */
      gettimeofday(&end_wait, NULL);
      /* Calculate wait time. */
      wait_time += elapsed_time(&start_wait, &end_wait);
    }
    if((p_rtn = pthread_mutex_unlock(&buffer_lock[bin])) != 0)
    {
      set_done_flag(&write_done);
      corrected_time = elapsed_time(&start_time, &end_time) - wait_time;
      return pack_return_values(0, p_rtn, THREAD_ERROR, bytes,
				"mutex unlock failed", 0.0, corrected_time,
				__FILE__, __LINE__);
    }

    /* Dertermine how much to write.  Attempt block size write. */
    bytes_to_transfer = (stored[bin]<block_size)?stored[bin]:block_size;
    bytes_transfered = 0;

    while(bytes_to_transfer > 0)
    {
      /* Wait for there to be room for the descriptor to write to. */
      errno = 0;
      FD_ZERO(&fds);
      FD_SET(wr_fd, &fds);
      timeout.tv_sec = write_info->timeout.tv_sec;
      timeout.tv_usec = write_info->timeout.tv_usec;
      sts = select(wr_fd+1, NULL, &fds, NULL, &timeout);

      if (sts == -1)
      {
	set_done_flag(&write_done);
	corrected_time = elapsed_time(&start_time, &end_time) - wait_time;
	return pack_return_values(0, p_rtn, WRITE_ERROR, bytes,
				  "fd write error", 0.0, corrected_time,
				  __FILE__, __LINE__);
      }
      if (sts == 0)
      {
	set_done_flag(&write_done);
	corrected_time = elapsed_time(&start_time, &end_time) - wait_time;
	return pack_return_values(0, p_rtn, TIMEOUT_ERROR, bytes, "fd timeout",
				  0.0, corrected_time, __FILE__, __LINE__);
      }

      /* Write out the data */
      errno = 0;
      sts = write(wr_fd, (buffer + (bin * block_size) + bytes_transfered),
		  bytes_to_transfer);
      if (sts == -1)
      {
	set_done_flag(&write_done);
	corrected_time = elapsed_time(&start_time, &end_time) - wait_time;
	return pack_return_values(0, p_rtn, WRITE_ERROR, bytes,
				  "fd write error", 0.0, corrected_time, 
				  __FILE__, __LINE__);
      }

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
      
#ifdef DEBUG
      /* Print w if entire bin is transfered, W if bin partially transfered. */
      debug_print = (sts < bytes_to_transfer) ? 'W' : 'w';
      pthread_mutex_lock(&print_lock);
      print_status(stderr, debug_print, bytes - bytes_transfered, crc_ui);
      pthread_mutex_unlock(&print_lock);
#endif /*DEBUG*/

      /* Update this nested loop's counting variables. */
      bytes_transfered += sts;
      bytes_to_transfer -= sts;
    }

    /* Obtain the mutex lock for the specific buffer bin that is needed to
       clear the bin for writing. */
    if((p_rtn = pthread_mutex_lock(&buffer_lock[bin])) != 0)
    {
      set_done_flag(&write_done);
      corrected_time = elapsed_time(&start_time, &end_time) - wait_time;
      return pack_return_values(0, p_rtn, THREAD_ERROR, bytes,
				"mutex lock failed", 0.0, corrected_time, 
				__FILE__, __LINE__);
    }
    stored[bin] = 0; /* Set the number of bytes left in buffer to zero. */
    /* If other thread sleeping, wake it up. */
    if((p_rtn = pthread_cond_signal(&next_cond)) != 0)
    {
      set_done_flag(&write_done);
      corrected_time = elapsed_time(&start_time, &end_time) - wait_time;
      return pack_return_values(0, p_rtn, THREAD_ERROR, bytes,
				"waiting for condition failed",
				0.0, corrected_time, __FILE__, __LINE__);
    }
    /* Release the mutex lock for this bin. */
    if((p_rtn = pthread_mutex_unlock(&buffer_lock[bin])) != 0)
    {
      set_done_flag(&write_done);
      corrected_time = elapsed_time(&start_time, &end_time) - wait_time;
      return pack_return_values(0, p_rtn, THREAD_ERROR, bytes,
				"mutex unlock failed", 0.0, corrected_time,
				__FILE__, __LINE__);
    }

    /* Determine where to get the data. */
    bin = (bin + 1) % ARRAY_SIZE;
    /* Determine the number of bytes left to transfer. */
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

  /* Get the time that the thread finished to work on reading. */
  gettimeofday(&end_time, NULL);

  set_done_flag(&write_done);
  corrected_time = elapsed_time(&start_time, &end_time) - wait_time;
  return pack_return_values(crc_ui, 0, 0, bytes, "",
			    0.0, corrected_time, NULL, 0);
}

#ifdef DEBUG
static void print_status(FILE* fp, char name, long long bytes,
			 unsigned long crc_ui)
{
  int i;

  fprintf(stderr, "%cbytes: %15lld crc: %10lu | ", name, bytes, crc_ui);

  for(i = 0; i < ARRAY_SIZE; i++)
  {
    fprintf(fp, " %6d", stored[i]);
  }
  fprintf(fp, "\n");
}
#endif /*DEBUG*/

#else

static struct return_values
do_read_write(int rd_fd, int wr_fd, long long bytes, int blk_size, 
	      struct timeval timeout, int crc_flag, unsigned int *crc_p)
{
  char	          *buffer;       /* Location to read/write from/to. */
  char	          *b_p;          /* Buffer pointer for parial writes. */
  ssize_t         sts;           /* Return status from read() and write(). */
  size_t	  bytes_to_xfer; /* Number of bytes to move in one loop. */
  fd_set          fds;           /* FD to write to. */
  struct timeval  timeout_use;   /* Timeout for selet() operation. */
#ifdef PROFILE
  struct profile profile_data[PROFILE_COUNT]; /* profile data array */
  long profile_count = 0;    /* Count variable for index of profile array. */
#endif /*PROFILE*/
  struct timeval start_time;    /* Start of time the thread is active. */
  struct timeval end_time;      /* End of time the thread is active. */
  double time_elapsed;          /* Difference between start and end time. */
  long long fsync_threshold = 0;/* Number of bytes to wait between fsync()s. */
  long long last_fsync = bytes; /* Number of bytes done though last fsync(). */
  struct stat file_info;        /* Information about the file to write to. */
  int do_threshold = 0;         /* Holds boolean true when using fsync(). */

  buffer = (char *)alloca(blk_size);

  /* Get the time that the thread started to work on transfering data. */
  gettimeofday(&start_time, NULL);
  memcpy(&end_time, &start_time, sizeof(struct timeval));

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
      fsync_threshold = get_fsync_threshold(bytes, blk_size);
      /* Set this boolean true. */
      do_threshold = 1;
    }
  }

  while(bytes > 0)
  {
    /* Do not worry about reading/writing an exact block as this is
       on the user end. But attempt blk_size reads. */
    bytes_to_xfer = (bytes<blk_size)?bytes:blk_size;
	    
    FD_ZERO(&fds);
    FD_SET(rd_fd,&fds);
    timeout_use.tv_sec = timeout.tv_sec;
    timeout_use.tv_usec = timeout.tv_usec;
    errno=0;
#ifdef PROFILE
    update_profile(1, bytes_to_xfer, wr_fd, profile_data, &profile_count);
#endif
    sts = select(rd_fd+1, &fds, NULL, NULL, &timeout_use);
#ifdef PROFILE
    update_profile(2, bytes_to_xfer, wr_fd, profile_data, &profile_count);
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
      return *pack_return_values(0, errno, READ_ERROR, bytes, "fd read error",
				 time_elapsed, time_elapsed,
				 __FILE__, __LINE__);
    }
    if (sts == 0)
    {
      /* timeout - treat as an EOF */
      time_elapsed = elapsed_time(&start_time, &end_time);
      return *pack_return_values(0, errno, TIMEOUT_ERROR, bytes, "fd timeout",
				 time_elapsed, time_elapsed,
				 __FILE__, __LINE__);
    }

    errno = 0;
#ifdef PROFILE
    update_profile(3, bytes_to_xfer, wr_fd, profile_data, &profile_count);
#endif
    sts = read(rd_fd, buffer, bytes_to_xfer);
#ifdef PROFILE
    update_profile(4, sts, wr_fd, profile_data, &profile_count);
#endif
    if (sts == -1)
    { /* return/break - read error */
      fprintf(stderr, "Low-level I/O failure: "
	      "read(%d, %#x, %lld) -> %lld, [Errno %d] %s: "
	      "higher encp levels will process this error "
	      "and retry if possible\n",
	      rd_fd, (unsigned)buffer, (long long)bytes_to_xfer,
	      (long long)sts, errno, strerror(errno));
      fflush(stderr);
      time_elapsed = elapsed_time(&start_time, &end_time);
      return *pack_return_values(0, errno, READ_ERROR, bytes, "fd read error",
				 time_elapsed, time_elapsed, 
				 __FILE__, __LINE__);
    }
    if (sts == 0)
    { /* return/break - unexpected eof error */
      time_elapsed = elapsed_time(&start_time, &end_time);
      return *pack_return_values(0, errno, TIMEOUT_ERROR, bytes, "fd timeout",
				 time_elapsed, time_elapsed, 
				 __FILE__, __LINE__);
    }
	    
    bytes_to_xfer = sts;
    b_p = buffer;
    do
    {


      FD_ZERO(&fds);
      FD_SET(wr_fd,&fds);
      timeout_use.tv_sec = timeout.tv_sec;
      timeout_use.tv_usec = timeout.tv_usec;
      errno=0;
#ifdef PROFILE
      update_profile(5, bytes_to_xfer, wr_fd, profile_data, &profile_count);
#endif
      sts = select(wr_fd+1, NULL, &fds, NULL, &timeout_use);
#ifdef PROFILE
      update_profile(6, bytes_to_xfer, wr_fd, profile_data, &profile_count);
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
				   "fd write error", time_elapsed,
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
      update_profile(7, bytes_to_xfer, wr_fd, profile_data, &profile_count);
#endif
      sts = write(wr_fd, b_p, bytes_to_xfer);
#ifdef PROFILE
      update_profile(8, sts, wr_fd, profile_data, &profile_count);
#endif
      if (sts == -1)
      {   /* return a write error */
	fprintf(stderr, "Low-level I/O failure: "
		"write(%d, %#x, %lld) -> %lld, [Errno %d] %s:"
		"higher encp levels will process this error "
		"and retry if possible\n",
		wr_fd, (unsigned)b_p, (long long)bytes_to_xfer,
		(long long)sts, errno, strerror(errno));
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
	*crc_p=adler32(*crc_p, b_p, sts); 
	break;
      default:  
	printf("fd_xfer: invalid crc flag"); 
	*crc_p=0; 
	break;
      }
      bytes_to_xfer -= sts;
      b_p += sts;
      bytes -= sts;
    } while (bytes_to_xfer);

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

#ifdef PROFILE
  print_profile(profile_data, profile_count);
#endif /*PROFILE*/

  return *pack_return_values(*crc_p, 0, 0, bytes, "", time_elapsed,
			     time_elapsed,0, 0);
}
#endif 

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
    v = Py_BuildValue("(s,i,s,i,O,f,f,s,i)",
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
    int		 blk_size;
    PyObject     *no_bytes_obj;
    PyObject	 *crc_obj_tp;
    PyObject	 *crc_tp=Py_None;/* optional, ref. FTT.fd_xfer */
    int          crc_flag=0; /*0: no CRC 1: Adler32 CRC >1: RFU */
    unsigned int crc_ui;
    struct timeval timeout = {0, 0};
    int sts;
    PyObject	*rr;
    struct return_values transfer_sts;
    
    sts = PyArg_ParseTuple(args, "iiOiOi|O", &fr_fd, &to_fd, &no_bytes_obj,
			   &blk_size, &crc_obj_tp, &timeout.tv_sec, &crc_tp);
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
	printf("fd_xfer - invalid crc param");

    errno = 0;
    transfer_sts = do_read_write(fr_fd, to_fd, no_bytes, blk_size, timeout,
				 crc_flag, &crc_ui);
    
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

