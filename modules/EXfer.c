/* EXfer.c - Low level data transfer C modules for encp. */

/* $Id$*/

/* A little hack for linux so direct i/o will work. */
#ifdef __linux__
#define _GNU_SOURCE
#endif

/* Macros for Large File Summit (LFS) conformance. */
/*#define _FILE_OFFSET_BITS 64
#define _LARGEFILE_SOURCE 1*/

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
#include <setjmp.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/resource.h>
#include <pthread.h>
#include <sys/mman.h>
#include <stdarg.h>
#include <string.h>
#include <limits.h>
#if __STDC__ && __STDC_VERSION__ >= 199901L
#include <stdbool.h>  /* C99 implimentations have these. */
#include <stdint.h>
#endif /*__STDC_VERSION__ */
#ifndef __osf__
#include <inttypes.h> /* Must handle osf definitions later.  Currently, only
			 intptr_t and uintptr_t are used.  However, when more
			 distributions are fully C99 compliant, even more
		         definitions from this file should be used. */
#endif /* __osf__ */
#ifdef __sgi
#include <sys/sysmp.h>
#endif

/***************************************************************************
 constants and macros
**************************************************************************/

/* OSF1 V4 defines MAP_FAILED incorrectly. */
#ifdef __osf__
# undef MAP_FAILED
# define MAP_FAILED ((void*)-1L)
#endif /*__osf__*/ 

/* return/break - read error */
#define READ_ERROR (-1)
/* timeout - treat as an EOF */
#define TIMEOUT_ERROR (-2)
/* return a write error */
#define WRITE_ERROR (-3)
/* return a thread error */
#define THREAD_ERROR (-4)
/* return a memory error */
#define MEMORY_ERROR (-5)
/* return a timing error */
#define TIME_ERROR (-6)
/* return a file error */
#define FILE_ERROR (-7)
/* return a signal error */
#define SIGNAL_ERROR (-8)

/* Define DEBUG only for extra debugging output */
/*#define DEBUG*/
/* Define DEBUG_REVERT for direct i/o and mmap i/o not supported messages. */
/*#define DEBUG_REVERT*/

/* Define PROFILE only for extra time output */
/* This profile only works for the non-threaded implementation.  It works
   best if the block size is changed on the encp command line to something
   small (i.e. less than the page size). */
/*#define PROFILE*/
#ifdef PROFILE
#define PROFILE_COUNT 25000
#endif

/* Macro to convert struct timeval into double. */
#define extract_time(t) ((double)(t->tv_sec+(t->tv_usec/1000000.0)))

/* Define memory mapped i/o advise constants on systems without them. */
#ifndef MADV_SEQUENTIAL
#define MADV_SEQUENTIAL -1
#endif
#ifndef MADV_WILLNEED
#define MADV_WILLNEED -1
#endif

/* Set the size of readback chunks to 1MB. */
#define ECRC_READBACK_BUFFER_SIZE 1048576

/* This is the minimum rate that must be maintained within a single call to
   read()/write().  Currently this is 1/2 MB/s. */
#define MINIMUM_RATE 524288

#ifdef DEBUG_REVERT
const char generic_direct_io_error[] = 
  "Using direct i/o failed.  Reverting to POSIX based i/o.\n";
const char kernel_direct_io_error[] = 
  "Direct i/o is not supported in the kernel.  "
  "Reverting to POSIX based i/o.\n";
const char filesystem_direct_io_error[] = 
  "Direct i/o is not supported by the filesystem.  "
  "Reverting to POSIX based i/o.\n";

const char generic_mmap_io_error[] = 
  "Using mmapped i/o failed.  Reverting to POSIX based i/o.\n";
const char kernel_mmap_io_error[] = 
  "Memory mapped i/o is not supported in the kernel.  "
  "Reverting to POSIX based i/o.\n";
const char filesystem_mmap_io_error[] = 
  "Memory mapped i/o is not supported by the filesystem.  "
  "Reverting to POSIX based i/o.\n";
#endif /*DEBUG_REVERT*/

/***************************************************************************
 definitions
**************************************************************************/

#if (! __STDC__) || (__STDC_VERSION__ < 199901L)
typedef unsigned char bool; /* Only define this for pre-C99 implimentations. */
#endif

#ifdef __osf__
/* All supported OSes except osf1v40d have a <inttypes.h> file.  Most were
   written to various drafts of the POSIX/XOPEN 2001 standard.  Newer OSF1
   machines have this file (although it is missing mandatory things).  The
   main thing that is needed are the following two integer types for
   pointers. */
typedef signed long intptr_t;
typedef unsigned long uintptr_t;
#endif

/* This is the struct that holds all the information about one direction
   of a transfer. */
struct transfer
{
  int fd;                 /*file descriptor*/

  off_t size;             /*size in bytes*/
  off_t bytes;            /*bytes left to transfer*/
  size_t block_size;      /*size of block*/
  size_t array_size;      /*number of buffers to use*/
  size_t mmap_size;       /*mmap address space segment lengths*/

  void *mmap_ptr;         /*memory mapped i/o pointer*/
  size_t mmap_len;        /*length of memory mapped file offset segment*/
  size_t mmap_offset;     /*offset from beginning of current mmapped segment */
  size_t mmap_left;       /* Bytes still remaining in current segment. */
  int mmap_count;         /* Number of mmapped segments done. */

  off_t fsync_threshold;  /* Number of bytes to wait between fsync()s. 
			        It is the max of block_size, mmap_size and
				1% of the filesize. */
  off_t last_fsync;       /* Number of bytes done though last fsync(). */

  struct timeval timeout; /*time to wait for data to be ready*/
  struct timeval start_transfer_function; /*time last read/write was started.*/
  double transfer_time;   /*time spent transfering data*/

  bool crc_flag;          /*crc flag - 0 or 1*/
  unsigned int crc_ui;    /*checksum*/
  
  int transfer_direction; /*positive means write, negative means read*/
  
  bool direct_io;         /*is true if using direct io*/
  bool mmap_io;           /*is true if using memory mapped io*/
  bool threaded;          /*is true if using threaded implementation*/
  
  pthread_t thread_id;    /*the thread id (if doing MT transfer)*/
#ifdef __sgi
  int cpu_affinity;       /*NICs are tied to CPUs on IRIX nodes*/
#endif
  short int done;         /*is zero initially, set to one when the (transfer)
			    thread exits and he main thread sets it to -1 when
			    it has collected the thread*/
  
  int exit_status;        /*error status*/
  int errno_val;          /*errno of any errors (zero otherwise)*/
  char* msg;              /*additional error message*/
  int line;               /*line number where error occured*/
  char* filename;         /*filename where error occured*/
};

/* Two pointers for use by the monitor thread. */
struct monitor
{
  struct transfer *read_info;  /* Pointer to the read direction struct. */
  struct transfer *write_info; /* Pointer to the write direction struct. */
};

#ifdef PROFILE
struct profile
{
  char whereami;
  struct timeval time;
  int status;
  int error;
};
#endif


/***************************************************************************
 prototypes
**************************************************************************/

/*checksumming is now being done here, instead of calling another module,
  in order to save a strcpy  -  cgw 19990428 */
unsigned int adler32(unsigned int, char *, unsigned int);

#ifndef STAND_ALONE
void initEXfer(void);
static PyObject * raise_exception(char *msg);
static PyObject * EXfd_xfer(PyObject *self, PyObject *args);
static PyObject * EXfd_ecrc(PyObject *self, PyObject *args);
#endif

/* do_read_write_threaded() and do_read_write():
   These functions take two paramaters to the read and write transfer structs.
   The dfference is that the first one will run each half of the transfer
   in their own thread, while the later runs everything in a single thread. */
static void do_read_write_threaded(struct transfer *reads,
				   struct transfer *writes);
static void do_read_write(struct transfer *reads, struct transfer *writes);

/* pack_return_values():
   The first paramater is the transfer struct for the direction of the
   transfer that is exiting.  Values in this struct will be modified with
   the values from the other paramaters.

   For the non-threaded version it is expected that the calling function
   will call this function twice, one for each direction.  If there is
   no error, dummy values should be specified for errno_val (0),
   exit_status (0), msg (NULL), transfer_time (0.0), filename (NULL) and
   line (0).  I there is an error, then crc_ui is set to zero. */
static struct transfer* pack_return_values(struct transfer *info,
					   unsigned int crc_ui,
					   int errno_val, int exit_status,
					   char* msg,
					   double transfer_time,
					   char *filename, int line);
/* elapsed_time():
   Return the difference between the two struct timeval{}s as a floating
   point number in seconds.  The first paramater, start_time, is the older. */
static double elapsed_time(struct timeval* start_time,
			   struct timeval* end_time);

/* rusage_elapsed_time():
   Do the same as elapsed_time(), except that the structs passed in are
   struct rusage{}.  These structs do contain a struct timeval{}. */
static double rusage_elapsed_time(struct rusage *sru, struct rusage *eru);

/* get_fsync_threshold():
   Returnthe number of bytes that need to be transfered before the next
   syncing the file to disk (write to file only). */
static long long get_fsync_threshold(struct transfer *info);

/* get_fsync_waittime():
   Uses the get_fsync_threshold() function for calculating the time to wait for
   another thread to exit.  It is possible that the file is residing in the
   buffer cache and the kernel has not started to flush the file out to disk.
   In this senerio the longest operation the the waiting thread would need
   to wait is the time of an entire sync of the part of the file still
   in the file buffer cache.  If the un-stopping thread takes longer than
   this return value in seconds to complete, there is likely a problem.
   Most often this has turned out that the un-stopping thread was stuck in
   the kernel (D state). */
static unsigned int get_fsync_waittime(struct transfer *info);

/* align_to_page() and align_to_size():
   The align_to_size() function takes the value paramater and returns the
   smallest size that is a multimple of the align parmamater. The
   align_to_page() function does the same thing except that the alignment
   amount is the systems page size.  Assumes unsigned values. */
static size_t align_to_page(size_t value);
static size_t align_to_size(size_t value, size_t align);

/* max() and min():
   Return either the maximum or minimum of 2 or 3 items. */
static unsigned long long min2ull(unsigned long long min1,
				  unsigned long long min2);
static unsigned long long min3ull(unsigned long long min1,
				  unsigned long long min2,
				  unsigned long long min3);
static unsigned long long max2ull(unsigned long long max1,
				  unsigned long long max2);
static unsigned long long max3ull(unsigned long long max1,
				  unsigned long long max2,
				  unsigned long long max3);

/* setup_*io():
   These functions take the struct of one direction of the transfer and
   will attempt to initialize the struct for use with each type of i/o
   optimization.  Posix i/o is the default and should be called regardless
   if the other optimizations are used.  If Mmap i/o was specified, but
   the underlying filesystem does not support it, it will revert to
   direct/posix i/o.  If direct i/o is used on a filesystem that does not
   support it, an error will be returned from EXfer.  The return value
   is -1 on error and 0 on success. */
static int setup_mmap_io(struct transfer *info);
static int setup_direct_io(struct transfer *info);
static int setup_posix_io(struct transfer *info);

/* reinit_mmap_io() and finish_mmap_io():
   The first function will unmap the current mmap segment and open the next
   mmap segment.  The finish_mmap_io() function will close the last mmap segment.
   The return value is -1 on error and zero on success. */
static int reinit_mmap_io(struct transfer *info);
static int finish_mmap_io(struct transfer *info);

/* finish_write():
   Performs any extra completion operations.  Mostly this means using the
   appropriate syncing function for posix, direct or mmapped i/o.
   The return value is -1 on error and zero on success. */
static int finish_write(struct transfer *info);

/* do_select():
   Wait for the FD to become ready for read or write depending on the
   direction of the transfer the transfer struct paramater specifies.
   This is really just a shell around select(), since only on FD is used. */
static int do_select(struct transfer *info);

/* *read() and *write():
   These funcions are wrappers around the reading and writing functions.
   The posix versions also perform the direct i/o versions.  The first
   paramater is a pointer to the base of the buffer array.  The second is
   the amount of data that this call to the function should worry about.
   The last paramater is the struct of this half of the transfer.  The
   return value is the amount of data read/written or -1 for error. */
static ssize_t mmap_read(void *dst, size_t bytes_to_transfer,
			 struct transfer *info);
static ssize_t mmap_write(void *src, size_t bytes_to_transfer,
			  struct transfer *info);
static ssize_t posix_read(void *dst, size_t bytes_to_transfer,
			  struct transfer* info);
static ssize_t posix_write(void *src, size_t bytes_to_transfer,
			   struct transfer* info);

/* thread_init():
   Initialize the global mutex locks and condition variables. */
static int thread_init(struct transfer *info);

/* thread_wait():
   If the other read/write thread is slow, wait for the specified bin to become
   available.  Return 1 on error and 0 on success. */
static int thread_wait(size_t bin, struct transfer *info);

/* thread_signal():
   Set the bin (or bucket) with index bin to the amount specified by bytes.
   This function will also 'raise' a condional variable signal to wake
   up the other read/write thread.  Return 1 on error and 0 on success. */
static int thread_signal(size_t bin, size_t bytes, struct transfer *info);

/* thread_collect():
   The first paramater is a thread id as returned by pthread_create().  This
   thread will be 'canceled'.  In posix talk, canceled is to thread as
   killed is to process.  The wait_time is the number of seconds to wait
   for the thread to stop.  If the thread is waiting in the kernel 
   the longest span of time would be returned from get_fsync_waittime().
   If the thread is still 'alive' after this time it is assumed hung and
   abandoned.  Return 1 on error and 0 on success. */
static int thread_collect(pthread_t tid, unsigned int wait_time);

/* thread_read() and thread_write():
   These are the functions passed to pthread_create() for performing the
   read/write portion of the threaded transfer.  The return value is the
   pointer to a struct transfer{} with the completion items filled in. */
static void* thread_read(void *info);
static void* thread_write(void *info);

/* thread_monitor():
   This is the function that is passed to pthread_create() for the purpose
   of starting a thread that monitors the read and write thread.  If
   one of the threads stops/get stuck/hangs this will attempt to cancel
   remaining threads and return an error from EXfer. */
static void* thread_monitor(void *monitor);

/* ecrc_readback():
   Performs a crc readback test on reads from enstore.  It takes a file
   descriptor of the output file as paramater.  It then rewindes the file and
   turns off direct i/o (Both Linux and IRIX do not give reasons for the
   errors). The file is then read from begining to end and at the same time
   the crc value is recalculated again. The crc value is returned.  On error,
   the crc will returned as zero and errno will be set.   Thus, to fully
   detect an error, set errno to zero first. */
static unsigned int ecrc_readback(int fd);

/* is_stored_empty():
   Returns true if the bin (aka bucket) 'bin' in the 'stored' global variable,
   is empty.  False if it is full. */
static int is_stored_empty(unsigned int bin);

/* buffer_empty() and buffer_full():
   If all the values in the 'stored' global are zero, buffer_empty() return
   true; false otherwise.  If all the values in stored are zero,
   buffer_empty() returns true; false otherwise. */
static int buffer_empty(size_t array_size);
static int buffer_full(size_t array_size);

/* sig_alarm():
   Used by thread_collect() to handle SIGALRM raised when a thread survies
   a pthread_cancel(). */
static void sig_alarm(int sig_num);

#ifdef PROFILE
static void update_profile(int whereami, int sts, int sock,
		    struct profile *profile_data, long *profile_count);
static void print_profile(struct profile *profile_data, int profile_count);
#endif /*PROFILE*/
#ifdef DEBUG
static void print_status(FILE *fp, unsigned int bytes_transfered,
			 unsigned int bytes_remaining, struct transfer *info);
#endif /*DEBUG*/

/***************************************************************************
 globals
**************************************************************************/

#ifndef STAND_ALONE

static PyObject *EXErrObject;

static char EXfer_Doc[] =  "EXfer is a module which Xfers data";

static char EXfd_xfer_Doc[] = "\
fd_xfer(fr_fd, to_fd, no_bytes, blk_siz, crc_flag[, crc])";
static char EXfd_ecrc_Doc[] = "\
unsigned int ecrc(crc, &start_addr, memory_size)";

/*  Module Methods table. 

    There is one entry with four items for for each method in the module

    Entry 1 - the method name as used  in python
          2 - the c implementation function
	  3 - flags 
	  4 - method documentation string
	  */

static PyMethodDef EXfer_Methods[] = {
    { "fd_xfer",  EXfd_xfer,  1, EXfd_xfer_Doc},
    { "ecrc", EXfd_ecrc, 1, EXfd_ecrc_Doc},
    { 0, 0}        /* Sentinel */
};

#endif

static size_t *stored;               /*pointer to array of bytes in each bin*/
static char *buffer;                 /*pointer to array of buffer bins*/
static pthread_mutex_t *buffer_lock; /*pointer to array of bin mutex locks*/
static pthread_mutex_t done_mutex;   /*main thread waits for an exited thread*/
static pthread_mutex_t monitor_mutex;/*used to sync the monitoring*/
static pthread_cond_t done_cond;     /*main thread waits for an exited thread*/
static pthread_cond_t next_cond;     /*used to signal peer thread to continue*/
#ifdef DEBUG
static pthread_mutex_t print_lock;   /*order debugging output*/
#endif
static sigjmp_buf alarm_join;        /*handle detection of hung threads*/

/***************************************************************************
 user defined functions
**************************************************************************/

static void* page_aligned_malloc(size_t size)
{
   /* Memory alignment is not very portable yet.  Posix defines the
      posix_memalign() function.  BSD (long ago) defined the valloc()
      function and SYSV had memalign(). */

   /* 6-18-2003: MWZ:
      These are the functions defined for various platforms:

      FL7.1 and earlier: valloc and memalign (No man pages though.)

      FL7.3 and later: valloc, memalign and posix_memalign

      IRIX 6.5: valloc and memalign
      
      Solaris 2.6, 2.7, 2.8: valloc and memalign

      OSF1 v40d: valloc
   */
   
#if defined ( _POSIX_ADVISORY_INFO ) && _POSIX_ADVISORY_INFO >= 200112L
   void* mem_p;
   int error;
   
   if((error = posix_memalign(&mem_p,
			      (size_t)sysconf(_SC_PAGESIZE), size)) != 0)
   {
      errno = error;
      return NULL;
   }
   return mem_p;
#elif defined ( __osf__ )
   return valloc(size);
#else
   return memalign((size_t)sysconf(_SC_PAGESIZE), size);
#endif
}

static void sig_alarm(int sig_num)
{
   if(sig_num != SIGALRM)
      return;  /* Should never happen. */
   
   /* Return execution to collect_thread(). */
   siglongjmp(alarm_join, 1);
}

/* Return 0 for false, >1 for true, <1 for error. */
static int is_stored_empty(unsigned int bin)
{
  int rtn = 0; /*hold return value*/

  pthread_testcancel(); /* Don't continue if the thread should stop now. */
  
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

  pthread_testcancel(); /* Don't continue if the thread should stop now. */
  
  return rtn;
}

static int buffer_empty(size_t array_size)
{
  unsigned int i;   /*loop counting*/
  int rtn = -1; /*return*/ 

  for(i = 0; i < array_size; i++)
  {
    if(!is_stored_empty(i))
    {
      rtn = 0;
      break;
    }
    rtn = 1;
  }
  
  return rtn;
}

static int buffer_full(size_t array_size)
{
  unsigned int i;   /*loop counting*/
  int rtn = -1; /*return*/
  
  for(i = 0; i < array_size; i++)
  {
    if(is_stored_empty(i))
    {
      rtn = 0;
      break;
    }
    rtn = 1;
  }
  
  return rtn;
}

/* Pack the arguments into a struct trasnfer{}. */
static struct transfer* pack_return_values(struct transfer* retval,
					   unsigned int crc_ui,
					   int errno_val,
					   int exit_status,
					   char* message,
					   double transfer_time,
					   char* filename, int line)
{
  pthread_testcancel(); /* Don't continue if the thread should stop now. */

  /* Do not bother with checking return values for errors.  Should the
     pthread_* functions fail at this point, there is notthing else to
     do but raise the condition variable and return. */
  pthread_mutex_lock(&done_mutex);

  retval->crc_ui = crc_ui;               /* Checksum */
  retval->errno_val = errno_val;         /* Errno value if error occured. */
  retval->exit_status = exit_status;     /* Exit status of the thread. */
  retval->msg = message;                 /* Additional error message. */
  retval->transfer_time = transfer_time; /* Duration of the transfer. */
  retval->line = line;                   /* Line number an error occured on. */
  retval->filename = filename;           /* Filename an error occured on. */
  retval->done = 1;                      /* Flag saying transfer half done. */

  /* Putting the following here is just the lazy thing to do. */
  /* For this code to work this must be executed after setting retval->done
     to 1 above. */
  pthread_cond_signal(&done_cond);

  pthread_mutex_unlock(&done_mutex);

  pthread_testcancel(); /* Don't continue if the thread should stop now. */
  
  return retval;
}

/* Return the time difference between to gettimeofday() calls. */
static double elapsed_time(struct timeval* start_time,
			   struct timeval* end_time)
{
  double time_elapsed;  /* variable to hold the time difference */

  time_elapsed = (extract_time(end_time) - extract_time(start_time));

  return time_elapsed;
}

/* Function to take two usage structs and return the total time difference. */
static double rusage_elapsed_time(struct rusage *sru, struct rusage *eru)
{
  return ((extract_time((&(eru->ru_stime)))+extract_time((&(eru->ru_utime)))) -
	  (extract_time((&(sru->ru_stime)))+extract_time((&(sru->ru_utime)))));
}

/* Return how many bytes need to be sent between fsync() calls. */
static long long get_fsync_threshold(struct transfer *info)
{
  unsigned long long temp_value;

  /* Find out what one percent of the file size is. */
  temp_value = (unsigned long long)((double)info->bytes / (double)100.0);

  /* Return the largest of these values:
     1) One percent of the filesize.
     2) The block (aka buffer) size.
     3) The memory mapped segment size. */
  return (long long)max3ull((unsigned long long)temp_value,
			    (unsigned long long)info->block_size,
			    (unsigned long long)info->mmap_size);
}

/* Returns the number of seconds to wait for another thread. */
static unsigned int get_fsync_waittime(struct transfer *info)
{
  /* Don't use info->fsync_threshold; it may not be initalized yet. */
  
  /* Calculate the amount of time to wait for the amount of data transfered
     between syncs will take assuming a minumum rate requirement. */
  return (unsigned int)(get_fsync_threshold(info)/(double)MINIMUM_RATE) + 1U;
  
  /* To cause intentional DEVICE_ERRORs use the following line instead. */
  /*return (time_t)(info->block_size/(float)MINIMUM_RATE) + 1U;*/
}

/* A usefull function to round a value to the next full page. */
static size_t align_to_page(size_t value)
{
   return align_to_size(value, (size_t)sysconf(_SC_PAGESIZE));
}

/* A usefull function to round a value to the next full required
   alignment size. */
static size_t align_to_size(size_t value, size_t align)
{
   return (value % align) ? (value + align - (value % align)) : value;
}

/* Returns the smaller of the two values. */
static unsigned long long min2ull(unsigned long long min1,
				  unsigned long long min2)
{
   return (min1 < min2) ? min1 : min2;
}

/* Returns the smaller of the three values. */
static unsigned long long min3ull(unsigned long long min1,
				  unsigned long long min2,
				  unsigned long long min3)
{
   return min2ull(min2ull(min1, min2), min3);
}

/* Returns the larger of the two values. */
static unsigned long long max2ull(unsigned long long max1,
				  unsigned long long max2)
{
   return (max1 > max2) ? max1 : max2;
}

/* Returns the largerer of the three values. */
static unsigned long long max3ull(unsigned long long max1,
				  unsigned long long max2,
				  unsigned long long max3)
{
return max2ull(max2ull(max1, max2), max3);
}

/***************************************************************************/
/***************************************************************************/

#ifdef DEBUG
static void print_status(FILE* fp, unsigned int bytes_transfered,
			 unsigned int bytes_remaining, struct transfer *info)
{
  unsigned int i;
  char debug_print;
  char direction;

  pthread_mutex_lock(&print_lock);

  /* Print F if entire bin is transfered, P if bin partially transfered. */
  debug_print = (bytes_remaining) ? 'P' : 'F';
  /* Print W if write R if read. */
  direction = (info->transfer_direction > 0) ? 'W' : 'R';
  
  (void)fprintf(fp, "%c%c bytes: %15llu crc: %10u | ",
	  direction, debug_print,
	  (unsigned long long)info->bytes, info->crc_ui);

  for(i = 0; i < info->array_size; i++)
  {
    (void)fprintf(fp, " %6d", stored[i]);
  }
  (void)fprintf(fp, "\n");

  pthread_mutex_unlock(&print_lock);

}
#endif /*DEBUG*/

#ifdef PROFILE
static void update_profile(int whereami, int sts, int sock,
		    struct profile *profile_data, long *profile_count)
{
  int size_var = sizeof(int);
  struct stat file_info;

  if(*profile_count < PROFILE_COUNT)
  {
    profile_data[*profile_count].whereami = whereami;
    profile_data[*profile_count].status = sts;
    gettimeofday(&(profile_data[*profile_count].time), NULL);
    if(fstat(sock, &file_info) == 0)
    {
      if(S_ISSOCK(file_info.st_mode))
	getsockopt(sock, SOL_SOCKET, SO_ERROR,
		   &profile_data[*profile_count].error, &size_var); 
    }
    (*profile_count)++;
  }
}

static void print_profile(struct profile *profile_data, int profile_count)
{
  int i;

  for(i = 0; i < profile_count; i++)
    (void)printf("%4d: sec: %11ld usec: %9ld  size: %10d  error: %3d\n",
	   profile_data[i].whereami,
	   profile_data[i].time.tv_sec,
	   profile_data[i].time.tv_usec,
	   profile_data[i].status,
	   profile_data[i].error);
}
#endif /*PROFILE*/


/***************************************************************************/
/***************************************************************************/

/* Return 1 on error, 0 on success. */
static int setup_mmap_io(struct transfer *info)
{
  int fd = info->fd;            /* The file descriptor in question. */
  struct stat file_info;        /* Information about the file to write to. */
  off_t bytes = info->size;     /* Number of bytes to transfer. (signed) */
  size_t mmap_len;              /* map_size adjusted to be memory aligned. */
  int advise_holder;            /* Contains the or-ed values for madvise. */
  int mmap_permissions;         /* Hold the mmap_permisssions. */

  /* Determine the length of the memory mapped segment.  This value needs
     to be a multiple of the page size and a multiple of the block_size, but
     since the block_size is already aligned to the page size, just aligning
     to the block_size is sufficent. */
  mmap_len = align_to_size(info->mmap_size, info->block_size);
  /*mmap_len = ((unsigned long long)bytes < mmap_len) ? bytes : mmap_len;*/
  mmap_len = (size_t)min2ull((unsigned long long)bytes,
			     (unsigned long long)mmap_len);
  /* Make sure that the memory map length is set correctly.  Even if
     this file descriptor can not do memory mapped i/o, the other
     transfer thread might. */
  info->mmap_len = mmap_len;
  info->mmap_ptr = MAP_FAILED;
  info->mmap_left = info->mmap_len;

  /* If the user did not select memory mapped i/o do not use it. */
  if(!info->mmap_io)
  {
     return 0;
  }

  /* Determine if the file descriptor is a real file. */
  errno = 0;
  if(fstat(fd, &file_info))
  {
    pack_return_values(info, 0, errno, FILE_ERROR, "fstat failed",
		       0.0, __FILE__, __LINE__);
    return 1;
  }
  /* If the file descriptor is not a file, don't continue. */
  if(!S_ISREG(file_info.st_mode))
  {
     info->mmap_io = (bool)0U;
     return 0;
  }

  /* When opening a mmapped i/o region for writing, the file must already
     be there and already have the correct size. */
  if(info->transfer_direction > 0)  /* If true, it is a write. */
  {
     /* Set the size of the file. */
     errno = 0;
     if(ftruncate(fd, bytes) < 0)
     {
	pack_return_values(info, 0, errno, FILE_ERROR, "ftruncate failed",
			   0.0, __FILE__, __LINE__);
	return 1;
     }
  }

  /* Determine the user permissions necessary for mmap io to work. */
  if(info->transfer_direction > 0) /* If true, it is a write. */
     mmap_permissions = PROT_WRITE | PROT_READ;
  else
     mmap_permissions = PROT_READ;
  
  /* Create the memory mapped file. info->mmap_ptr will equal the
     starting memory address on success; MAP_FAILED on error. */
  errno = 0;
  if((info->mmap_ptr = mmap(NULL, mmap_len, mmap_permissions,
			    MAP_SHARED, fd, (off_t)0)) == MAP_FAILED)
  {
     if(errno == ENODEV)
     {
	/* There probably should be a write to stderr here.  The message
	   should say something like, "using mmapped i/o failed, reverting
	   to posix based i/o." */
#ifdef DEBUG_REVERT
        (void)write(STDERR_FILENO, filesystem_mmap_io_error,
	      strlen(filesystem_mmap_io_error));
#endif /*DEBUG_REVERT*/
        info->mmap_io = (bool)0U;
	return 0;
     }
     else
     {
	if(info->transfer_direction > 0)  /* If true, it is a write. */
	{
	   /* If mmap() or madvise() failed on the write half of the transfer,
	      set the filesize back to the original size.  On writes we don't,
	      care about any file corruption (yet) because we have not written
	      anything out.
	      
	      There is a good reason why it is set back to the original size.
	      The original filesize on a user initiated transfer is 0 bytes.
	      However, dcache sets the filesize in pnfs to the correct size;
	      before it starts the encp. */

	  errno = 0;
	  if(ftruncate(fd, file_info.st_size) < 0)
	  {
	     pack_return_values(info, 0, errno, FILE_ERROR,
				"ftruncate failed", 0.0, __FILE__, __LINE__);
	     return 1;
	  }
	}
	
	pack_return_values(info, 0, errno, FILE_ERROR,
			   "mmap failed", 0.0, __FILE__, __LINE__);
	return 1;
     }
  }

  /* Turn on the SEQUENTIAL advise hints.  For writes also turn on the 
     WILLNEED hint. */
#if defined ( _POSIX_ADVISORY_INFO ) && _POSIX_ADVISORY_INFO >= 200112L
  advise_holder = POSIX_MADV_SEQUENTIAL;
  if(info->transfer_direction < 0) /* If true, it is a read from disk. */
    advise_holder |= POSIX_MADV_WILLNEED;
#else
  advise_holder = MADV_SEQUENTIAL;
  if(info->transfer_direction < 0) /* If true, it is a read from disk. */
    advise_holder |= MADV_WILLNEED;
#endif

  /* Advise the system on the memory mapped i/o usage pattern. */
  errno = 0;
#if defined ( _POSIX_ADVISORY_INFO ) && _POSIX_ADVISORY_INFO >= 200112L
  if(posix_madvise(info->mmap_ptr, info->mmap_len, advise_holder < 0)
#else
  if(madvise(info->mmap_ptr, info->mmap_len, advise_holder) < 0)
#endif /* _POSIX_ADVISORY_INFO */
  {
     /* glibc versions prior to 2.4 don't support the madvise function.
	If it is found not to be supported, don't worry.  Use the
	default read/write method.  This error sets errno to ENOSYS. */
     /* IRIX does not support use of MADV_SEQUENTIAL.  This error sets
	errno to EINVAL. */

     if(errno != ENOSYS && errno != EINVAL) /* A real error occured. */
     {
	/* Clear the memory mapped information. */
	if(munmap(info->mmap_ptr, info->mmap_len) < 0)
	{
	   info->mmap_ptr = MAP_FAILED; /* Set this explicitly. */
	   pack_return_values(info, 0, errno, FILE_ERROR,
			      "munmap failed", 0.0, __FILE__, __LINE__);
	   /* don't return here, munmap and ftruncate must execute. */
	}
	
	if(info->transfer_direction > 0)  /* If true, it is a write. */
	{
	   /* If madvise() failed on the write half of the transfer, set
	      the filesize back to the original size.  On writes to local
	      disk we don't, care about any file corruption (yet) because
	      we have not written anything out.
	      
	      There is a good reason why it is set back to the original size.
	      The original filesize on a user initiated transfer is 0 bytes.
	      However, dcache sets the filesize in pnfs to the correct size;
	      before it starts the encp. */
	   
	   errno = 0;
	   if(ftruncate(fd, file_info.st_size) < 0)
	   {
	      info->mmap_ptr = MAP_FAILED; /* Set this explicitly. */
	      pack_return_values(info, 0, errno, FILE_ERROR,
				 "ftruncate failed", 0.0, __FILE__, __LINE__);
	      /* don't return here, munmap and ftruncate must execute. */
	   }
	}

	/* If this is true either munmap, ftruncate or both have failed too. */
	if(info->mmap_ptr == MAP_FAILED)
	   return 1;
	else
	{
	   info->mmap_ptr = MAP_FAILED; /* Set this explicitly. */
	   
	   pack_return_values(info, 0, errno, FILE_ERROR,
			      "madvise failed", 0.0, __FILE__, __LINE__);
	   return 1;
	}
     }
  }
  return 0;
}

/* Return 1 on error, 0 on success. */
static int reinit_mmap_io(struct transfer *info)
{
  int advise_holder = 0; /* Advise hints for madvise. */
  size_t bytes_in_segment; 

  /* If the file is a local disk, use memory mapped i/o on it. 
     Only advance to the next mmap segment when the previous one is done. */
  if(info->mmap_ptr != MAP_FAILED &&
     info->mmap_offset == info->mmap_len)
  {
    /* Force the data to be written out to disk. */
    errno = 0;
    if(msync(info->mmap_ptr, info->mmap_len, MS_SYNC) < 0)
    {
      pack_return_values(info, 0, errno, FILE_ERROR,
			 "munmap failed", 0.0, __FILE__, __LINE__);
      return 1;
    }
     
    /* Unmap the current mapped memory segment. */
    errno = 0;
    if(munmap(info->mmap_ptr, info->mmap_len) < 0)
    {
      pack_return_values(info, 0, errno, FILE_ERROR,
			 "munmap failed", 0.0, __FILE__, __LINE__);
      return 1;
    }

    /* Reset these values for the next segment. */
    /*info->mmap_len = ((unsigned long long)info->bytes < info->mmap_len) ?
      (size_t)info->bytes : info->mmap_len;*/
    info->mmap_len = (size_t)min2ull((unsigned long long)info->bytes,
				     (unsigned long long)info->mmap_len);
    info->mmap_offset = 0;
    info->mmap_count += 1;
    info->mmap_left = info->mmap_len;

    /* Normally, bytes_in_segment is equal to info->mmap_len, but on the
       last loop info->mmap_len is less than (possibly equal to) what it
       was on previous loops.  But for calculating the offset for the
       following mmap() call, we need this "full" mmap_size value. */
    bytes_in_segment = align_to_page(info->mmap_size);
    
    /* Create the memory mapped file. */
    errno = 0;
    if((info->mmap_ptr = mmap(NULL, info->mmap_len, PROT_WRITE | PROT_READ,
			      MAP_SHARED, info->fd,
			      (off_t)info->mmap_count*(off_t)bytes_in_segment))
       == MAP_FAILED)
    {
      pack_return_values(info, 0, errno, FILE_ERROR,
			 "mmap failed", 0.0, __FILE__, __LINE__);
      return 1;
    }
    
    /* Turn on the SEQUENTIAL advise hints.  For writes also turn on the 
       WILLNEED hint. */
#if defined ( _POSIX_ADVISORY_INFO ) && _POSIX_ADVISORY_INFO >= 200112L
    advise_holder = POSIX_MADV_SEQUENTIAL;
    if(info->transfer_direction < 0) /* If true, it is a read from disk. */
       advise_holder |= POSIX_MADV_WILLNEED;
#else
    advise_holder = MADV_SEQUENTIAL;
    if(info->transfer_direction < 0) /* If true, it is a read from disk. */
       advise_holder |= MADV_WILLNEED;
#endif

  /* Advise the system on the memory mapped i/o usage pattern. */
    errno = 0;
#if defined ( _POSIX_ADVISORY_INFO ) && _POSIX_ADVISORY_INFO >= 200112L
    if(posix_madvise(info->mmap_ptr, info->mmap_len, advise_holder < 0)
#else
       if(madvise(info->mmap_ptr, info->mmap_len, advise_holder) < 0)
#endif /* _POSIX_ADVISORY_INFO */
    {
       /* If madvise is not implimented, don't fail. */
       if(errno != ENOSYS && errno != EINVAL)
       {
	  /* Clear the memory mapped information. */
	  if(munmap(info->mmap_ptr, info->mmap_len) < 0)
	  {
	     info->mmap_ptr = MAP_FAILED; /* Set this explicitly. */
	     pack_return_values(info, 0, errno, FILE_ERROR,
				"munmap failed", 0.0, __FILE__, __LINE__);
	     return 1;
	  }
	  
	  pack_return_values(info, 0, errno, FILE_ERROR,
			     "madvise failed", 0.0, __FILE__, __LINE__);
	  return 1;
       }
    }
  }
  else if(info->mmap_offset == info->mmap_len)
  {
    /* Reset these values for the next segment. Even if this thread does
       not care about page allignment, the other thread might. */
     /*info->mmap_len = ((unsigned long long)info->bytes < info->mmap_len) ?
       info->bytes : info->mmap_len;
     */
    info->mmap_len = (size_t)min2ull((unsigned long long)info->bytes,
				     (unsigned long long)info->mmap_len);
    info->mmap_offset = 0;
    info->mmap_count += 1;
    info->mmap_left = info->mmap_len;
  }

  return 0;
}

/* Return 1 on error, 0 on success. */
static int finish_mmap_io(struct transfer *info)
{
  if(info->mmap_ptr != MAP_FAILED)
  {
    /* Unmap the final mapped memory segment. */
    errno = 0;
    if(munmap(info->mmap_ptr, info->mmap_len) < 0)
    {
      pack_return_values(info, 0, errno, FILE_ERROR,
			 "munmap failed", 0.0, __FILE__, __LINE__);
      return 1;
    }
  }
  return 0;
}

/* Return 1 on error, 0 on success. */
static int finish_write(struct transfer *info)
{
  if(info->mmap_ptr != MAP_FAILED)
  {
    /* Tell OS to write out the data now. */
    errno = 0;
    if(msync(info->mmap_ptr, info->mmap_len, MS_SYNC) < 0)
    {
      pack_return_values(info, 0, errno, WRITE_ERROR,
			 "msync failed", 0.0, __FILE__, __LINE__);
      return 1;
    }
  }
  else
  {
    /* If the file descriptor supports fsync force the data to be flushed to
       disk.  This can obviously fail for things like fsync-ing sockets, thus
       EINVAL errors are ignored. */
    errno = 0;
    if(fsync(info->fd) < 0)
    {
       if(errno != EINVAL)
       {
	  pack_return_values(info, 0, errno, WRITE_ERROR,
			 "fsync failed", 0.0, __FILE__, __LINE__);
	  return 1;
       }
    }
  }

  return 0;
}

/* Return 1 on error, 0 on success. */
static int setup_direct_io(struct transfer *info)
{
  struct stat file_info;  /* Information about the file to read/write from. */
#ifdef O_DIRECT
  int new_fcntl;       /* Holder of FD flags or-ed with O_DIRECT. */
  int rtn_fcntl;       /* Stores the original FD flags. */
# ifdef linux 
  int test_fcntl;      /* Compares original flags with new flags. */
  void* temp_buffer;   /* Some achitectures require additional tests... */
# endif /* linux */
#endif /* O_DIRECT */
  
  /* If direct io was specified, check if it may work. */
  if(info->direct_io == 0)
  {
     return 0;
  }
  
  /* Stat the file.  The mode is used to check if it is a regular file. */
  if(fstat(info->fd, &file_info))
  {
     pack_return_values(info, 0, errno, FILE_ERROR, "fstat failed", 0.0,
			__FILE__, __LINE__);
     return 1;
  }
  /* Direct IO can only work on regular files.  Even if direct io is 
     turned on the filesystem still has to support it. */
  if(! S_ISREG(file_info.st_mode))
  {
     info->direct_io = 0;
     return 0;
  }

#ifdef O_DIRECT
  /* If the system supports direct i/o attempt to turn it on. */

  /* Get the current file descriptor flags. */
  errno = 0;
  if((rtn_fcntl = fcntl(info->fd, F_GETFL, 0)) < 0)
  {
     pack_return_values(info, 0, errno, FILE_ERROR,
			"fcntl(F_GETFL) failed", 0.0, __FILE__, __LINE__);
     return 1;
  }

  new_fcntl = rtn_fcntl | O_DIRECT;  /* turn on O_DIRECT */
  errno = 0;
  /* Set the new file descriptor flags. */
  if(fcntl(info->fd, F_SETFL, (long)new_fcntl) < 0)
  {
     if(errno == EINVAL) /* If true, direct i/o is not supported. */
     {
	/* There probably should be a write to stderr here.  The message
	   should say something like, "using direct i/o failed, reverting
	   to posix based i/o." */
#ifdef DEBUG_REVERT
        (void)write(STDERR_FILENO, generic_direct_io_error, 
	      strlen(generic_direct_io_error));
#endif /*DEBUG_REVERT*/
	info->direct_io = 0;
	return 0;
     }
     else
     {
	pack_return_values(info, 0, errno, FILE_ERROR,
			   "fcntl(F_SETFL) failed", 0.0, __FILE__, __LINE__);
	return 1;
     }
  }

# ifdef linux

  /* Even though direct i/o has been supported since the 2.4.10 Linux kernel,
     Redhat for there 9.0 release (8.0 maybe too?) (2.4.20 Redhat kernel)
     applied a patch that turned of direct i/o.  Stock kernels leave direct
     i/o on.  The problem with how Redhat did it, is that the fcntl(F_SETFL)
     call above does not return an error.  Thus, to detect this kernel
     and turn off direct i/o recheck the FD flags. */
  
  /*Get the current file descriptor flags.*/
  errno = 0;
  if((test_fcntl = fcntl(info->fd, F_GETFL, 0)) < 0)
  {
     pack_return_values(info, 0, errno, FILE_ERROR,
			   "fcntl(F_GETFL) failed", 0.0, __FILE__, __LINE__);
     return 1;
  }

  /* Test to see if the fcntl(F_SETFL) function really succeded. */
  if((test_fcntl & O_DIRECT) == 0)
  {
     /* There probably should be a write to stderr here.  The message
	should say something like, "using direct i/o failed, reverting
	to posix based i/o." */
#ifdef DEBUG_REVERT
     (void)write(STDERR_FILENO, kernel_direct_io_error,
	   strlen(kernel_direct_io_error));
#endif /*DEBUG_REVERT*/
     info->direct_io = 0;
     return 0;
  }

  /* Get some aligned memory for the following test(s). */
  temp_buffer = page_aligned_malloc((size_t)sysconf(_SC_PAGESIZE));
  
  /* 2.4.9 kernels (FL7.1) and ealier do not support direct io.  The test for
     running on one of these older Linux kernels is to write a non-page
     aligned amount of data.  If successful (return value > 0) then direct
     i/o is not supported. */

  errno = 0;
  if(write(info->fd, temp_buffer, (size_t)50U) > 0)
  {
     free(temp_buffer);

     /* Clear the FD of the 0_DIRECT flag. */
     if(fcntl(info->fd, F_SETFL, rtn_fcntl) < 0)
     {
	pack_return_values(info, 0, errno, FILE_ERROR,
			   "fcntl(F_SETFL) failed", 0.0, __FILE__, __LINE__);
	return 1;
     }
     
     /* There probably should be a write to stderr here.  The message
	should say something like, "using direct i/o failed, reverting
	to posix based i/o." */
#ifdef DEBUG_REVERT
     (void)write(STDERR_FILENO, kernel_direct_io_error,
	   strlen(kernel_direct_io_error));
#endif /*DEBUG_REVERT*/
     info->direct_io = 0;
     return 0;
  }
  
  /* FL7.3 has a direct i/o bug that requires a confusing work around.
     The check (within the kernel) to see if the opened file can really
     do direct i/o is done during the write call and not during the
     fcntl/open call.  Hence, the attempt at the following write() call.
     If a properly aligned write is done and it succedes (return value != -1),
     then direct i/o is available.
  */

  errno = 0;
  if(write(info->fd, temp_buffer, (size_t)sysconf(_SC_PAGESIZE)) 
     == (ssize_t)-1)
  {
     free(temp_buffer);

     /* Clear the FD of the 0_DIRECT flag. */
     if(fcntl(info->fd, F_SETFL, rtn_fcntl) < 0)
     {
	pack_return_values(info, 0, errno, FILE_ERROR,
			   "fcntl(F_SETFL) failed", 0.0, __FILE__, __LINE__);
	return 1;
     }

     /* There probably should be a write to stderr here.  The message
	should say something like, "using direct i/o failed, reverting
	to posix based i/o." */
#ifdef DEBUG_REVERT
     (void)write(STDERR_FILENO, filesystem_direct_io_error,
	   strlen(filesystem_direct_io_error));
#endif /*DEBUG_REVERT*/
     info->direct_io = 0;
     return 0;
  }

  free(temp_buffer); /* Don't forget to free this memory. */

  /* Rewind to the beginning of the file. */
  if(lseek(info->fd, 0, SEEK_SET) < (off_t)0)
  {
     pack_return_values(info, 0, errno, FILE_ERROR,
			"lseek failed", 0.0, __FILE__, __LINE__);
     return 1;
  }

# endif /*linux*/
  
#else /* O_DIRECT is not even defined on the system. */
# ifdef DEBUG_REVERT
  (void)write(STDERR_FILENO, kernel_direct_io_error,
	      strlen(kernel_direct_io_error));
# endif /*DEBUG_REVERT*/
  info->direct_io = 0;
#endif /*O_DIRECT*/
  return 0;
}

/* Return 1 on error, 0 on success. */
static int setup_posix_io(struct transfer *info)
{
  struct stat file_info;  /* Information about the file to read/write from. */

  /* Stat the file.  The mode is used to check if it is a regular file. */
  if(fstat(info->fd, &file_info))
  {
    pack_return_values(info, 0, errno, FILE_ERROR, "fstat failed",
		       0.0, __FILE__, __LINE__);
    return 1;
  }

  if(S_ISREG(file_info.st_mode))
  {
    /* Get the number of bytes to transfer between fsync() calls. */
    info->fsync_threshold = get_fsync_threshold(info);
    /* Set the current number of bytes remaining since last fsync to
       the size of the file. */
    info->last_fsync = info->size;
  }
  else
  {
    /* Get the number of bytes to transfer between fsync() calls. */
    info->fsync_threshold = 0;
    /* Set the current number of bytes remaining since last fsync to
       the size of the file. */
    info->last_fsync = 0;
  }

  return 0;
}

/***************************************************************************/
/***************************************************************************/

/* Handle waiting for the file descriptor. Return non-zero on error and
   zero on success. */
static int do_select(struct transfer *info)
{
  fd_set fds;                   /* For use with select(2). */
  struct timeval timeout;       /* Time to wait for data. */
  int sts = 0;                  /* Return value from various C system calls. */

  /* Initialize select values. */
  errno = 0;
  FD_ZERO(&fds);
  FD_SET(info->fd, &fds);
  timeout.tv_sec = info->timeout.tv_sec;
  timeout.tv_usec = info->timeout.tv_usec;
  
  /* Wait for there to be data on the descriptor ready for reading. */
  if(info->transfer_direction > 0)  /*write*/
  {
     sts = select(info->fd+1, NULL, &fds, NULL, &timeout);
     if(sts < 0)
	pack_return_values(info, 0, errno, WRITE_ERROR,
			   "fd select error", 0.0, __FILE__, __LINE__);
  }
  else if(info->transfer_direction < 0)  /*read*/
  {
     sts = select(info->fd+1, &fds, NULL, NULL, &timeout);
     if(sts < 0)
	pack_return_values(info, 0, errno, READ_ERROR,
			   "fd select error", 0.0, __FILE__, __LINE__);
  }
  
  if(sts == 0)
     pack_return_values(info, 0, errno, TIMEOUT_ERROR,
			"fd select timeout", 0.0, __FILE__, __LINE__);
  
  if (sts <= 0)
    return 1;

  return 0;
}


static ssize_t mmap_read(void *dst, size_t bytes_to_transfer,
			 struct transfer *info)
{
  (void)memcpy(dst,
	       (void*)((uintptr_t)info->mmap_ptr+(uintptr_t)info->mmap_offset),
	       bytes_to_transfer);
  
  return (ssize_t)bytes_to_transfer;
}

static ssize_t mmap_write(void *src, size_t bytes_to_transfer,
			  struct transfer *info)
{
  int sync_type = 0;            /* Type of msync() to perform. */

  /* If file supports memory mapped i/o perform the memory to memory copy
     that reads/writes from/to the file. */
  errno = 0;
  (void)memcpy(
	    (void*)((uintptr_t)info->mmap_ptr + (uintptr_t)info->mmap_offset),
	    src, bytes_to_transfer);

  /* If this is the very end of the file, don't just set the dirty pages
     to be written to disk, wait for them to be written out to disk. */
  if((info->bytes - bytes_to_transfer) == 0)
  {
    sync_type = MS_SYNC;
  }
  else
    sync_type = MS_ASYNC;

  pthread_testcancel(); /* Any syncing action will take time. */

  /* Schedule the data for sync to disk now. */
  if(msync((void*)((uintptr_t)info->mmap_ptr + (uintptr_t)info->mmap_offset),
	   bytes_to_transfer, sync_type) < 0)
  {
     pack_return_values(info, 0, errno, WRITE_ERROR,
			"msync error", 0.0, __FILE__, __LINE__);
     return -1;
  }
  pthread_testcancel(); /* Any syncing action will take time. */

  return (ssize_t)bytes_to_transfer;
}

/* Act like the posix read() call.  But return all interpreted errors with -1.
   Also, set error values appropratly when detected. */
static ssize_t posix_read(void *dst, size_t bytes_to_transfer,
			  struct transfer* info)
{
  ssize_t sts = 0;  /* Return value from various C system calls. */
  
  /* If direct io was specified, make sure the location is page aligned. */
  if(info->direct_io)
  {
    bytes_to_transfer = align_to_page(bytes_to_transfer);
  }

  errno = 0;
  pthread_testcancel();  /* On Linux, read() isn't a cancelation point. */
  sts = read(info->fd, dst, bytes_to_transfer);
  pthread_testcancel();
  
  if (sts < 0)
  {
    pack_return_values(info, 0, errno, READ_ERROR,
		       "fd read error", 0.0, __FILE__, __LINE__);
    return -1;
  }
  if (sts == 0)
  {
    pack_return_values(info, 0, errno, TIMEOUT_ERROR,
		       "fd read timeout", 0.0, __FILE__, __LINE__);
    return -1;
  }
  return sts;
}

/* Act like the posix write() call.  But return all interpreted errors with -1.
   Also, set error values appropratly when detected. */
static ssize_t posix_write(void *src, size_t bytes_to_transfer,
			   struct transfer* info)
{
  ssize_t sts = 0;  /* Return value from various C system calls. */

  /* If direct io was specified, make sure the location is page aligned. */
  if(info->direct_io)
  {
    bytes_to_transfer = align_to_page(bytes_to_transfer);
  }

  /* When faster methods will not work, use read()/write(). */
  errno = 0;
  pthread_testcancel();  /* On Linux, write() isn't a cancelation point. */
  sts = write(info->fd, src, bytes_to_transfer);
  pthread_testcancel();

  if (sts == -1)
  {
    pack_return_values(info, 0, errno, WRITE_ERROR,
		       "fd write error", 0.0, __FILE__, __LINE__);
    return -1;
  }
  if (sts == 0)
  {
    pack_return_values(info, 0, errno, TIMEOUT_ERROR,
		       "fd write timeout", 0.0, __FILE__, __LINE__);
    return -1;
  }
  
  /* Use with direct io. */
  if(info->direct_io)
  {
    /* Only apply after the last write() call.  Also, if the size of the
       file was a multiple of the alignment used, then everything is correct
       and attempting to do this file size 'fix' is unnecessary. */
    if((long long)info->bytes <= (long long)sts)
    {
      /* Adjust the write() return value.  After the last call to write()
	 for the file this is/can be too long.  It needs to be shrunk down
	 to the number of bytes written that we actually care about. */
      sts = (ssize_t)info->bytes;
      /* Truncate size at end of transfer.  For direct io all writes must be
	 a multiple of the page size.  The last write must be truncated down
	 to the correct size. */
      if(ftruncate(info->fd, info->size) < 0)
      {
	 pack_return_values(info, 0, errno, WRITE_ERROR,
			    "ftruncate failed", 0.0, __FILE__, __LINE__);
	 return -1;
      }
    }
  }
  else /* posix i/o */
  {
    /* Force the data to disk.  Don't let encp take up to much memory.
       This isnt the most accurate way of doing this, however it is less
       overhead. */
    if(info->fsync_threshold)
    {
       /* If the number of bytes of data transfered since the last sync has
	  passed, do the fdatasync() and record amount completed. */
      
      if((info->last_fsync - info->bytes) > info->fsync_threshold)
      {
	info->last_fsync = info->bytes - sts;
	pthread_testcancel(); /* Any sync action will take time. */
	errno = 0;
	if(fdatasync(info->fd)) /* Sync the data. */
	{
	   if(errno != EINVAL)
	   {
	      pack_return_values(info, 0, errno, WRITE_ERROR,
				 "fdatasync failed", 0.0, __FILE__, __LINE__);
	      return -1;
	   }
	}
      }
      /* If the entire file is transfered, do the fsync(). */
      else if((info->bytes - sts) == 0)
      {
	info->last_fsync = info->bytes - sts;
	pthread_testcancel(); /* Any syncing action will take time. */
	if(fsync(info->fd)) /* Sync the data and metadata. */
	{
	   if(errno != EINVAL)
	   {
	      pack_return_values(info, 0, errno, WRITE_ERROR,
				 "fsync failed", 0.0, __FILE__, __LINE__);
	      return -1;
	   }
	}
      }
    }
    pthread_testcancel(); /* Any syncing action will take time. */
  }

  return sts;
}

/***************************************************************************/
/***************************************************************************/

static int thread_init(struct transfer *info)
{
  int p_rtn;                    /* Pthread return value. */
  size_t i;

  /* Initalize all the condition varaibles and mutex locks. */

  /* initalize the conditional variable signaled when a thread has finished. */
  if((p_rtn = pthread_cond_init(&done_cond, NULL)) != 0)
  {
    pack_return_values(info, 0, p_rtn, THREAD_ERROR,
		       "cond init failed", 0.0, __FILE__, __LINE__);
    return 1;
  }
  /* initalize the conditional variable to signal peer thread to continue. */
  if((p_rtn = pthread_cond_init(&next_cond, NULL)) != 0)
  {
    pack_return_values(info, 0, p_rtn, THREAD_ERROR,
		       "cond init failed", 0.0, __FILE__, __LINE__);
    return 1;
  }
  /* initalize the mutex for signaling when a thread has finished. */
  if((p_rtn = pthread_mutex_init(&done_mutex, NULL)) != 0)
  {
    pack_return_values(info, 0, p_rtn, THREAD_ERROR,
		       "mutex init failed", 0.0, __FILE__, __LINE__);
    return 1;
  }
  /* initalize the mutex for syncing the monitoring operations. */
  if((p_rtn = pthread_mutex_init(&monitor_mutex, NULL)) != 0)
  {
    pack_return_values(info, 0, p_rtn, THREAD_ERROR,
		       "mutex init failed", 0.0, __FILE__, __LINE__);
    return 1;
  }
#ifdef DEBUG
  /* initalize the mutex for ordering debugging output. */
  if((p_rtn = pthread_mutex_init(&print_lock, NULL)) != 0)
  {
    pack_return_values(info, 0, p_rtn, THREAD_ERROR,
		       "mutex init failed", 0.0, __FILE__, __LINE__);
    return 1;
  }
#endif
  /* initalize the array of bin mutex locks. */
  for(i = 0; i < info->array_size; i++)
    if((p_rtn = pthread_mutex_init(&(buffer_lock[i]), NULL)) != 0)
    {
      pack_return_values(info, 0, p_rtn, THREAD_ERROR,
			 "mutex init failed", 0.0, __FILE__, __LINE__);
      return 1;
    }
  
  return 0;
}

/* The first parameter is the bin to wait on.  Last paramater is the transfer
   struct for this half of the transfer. */
static int thread_wait(size_t bin, struct transfer *info)
{
  int p_rtn;                    /* Pthread return value. */
  struct timeval cond_wait_tv;  /* Absolute time to wait for cond. variable. */
  struct timespec cond_wait_ts; /* Absolute time to wait for cond. variable. */
  int expected = (info->transfer_direction < 0); /*0 = writes; 1 = reads*/
  
  pthread_testcancel(); /* Don't grab a mutex if we should't use it. */

  /* Determine if the lock for the buffer_lock bin, bin, is ready. */
  if((p_rtn = pthread_mutex_lock(&buffer_lock[bin])) != 0)
  {
    pack_return_values(info, 0, p_rtn, THREAD_ERROR,
		       "mutex lock failed", 0.0, __FILE__, __LINE__);
    return 1;
  }
  /* If the stored bin is still full (stored[bin] > 0 == 1) when writing or
     still empty (stored[bin] == 0) when reading, then wait for the other
     thread to catch up. */
  if((stored[bin] > 0) == expected)  /*if(!stored[bin] == !expected)*/
  {
    /* Determine the absolute time to wait in pthread_cond_timedwait(). */
    if(gettimeofday(&cond_wait_tv, NULL) < 0)
    {
      pack_return_values(info, 0, errno, TIME_ERROR,
			 "gettimeofday failed", 0.0, __FILE__, __LINE__);
      return 1;
    }
    cond_wait_ts.tv_sec = cond_wait_tv.tv_sec + info->timeout.tv_sec;
    cond_wait_ts.tv_nsec = cond_wait_tv.tv_usec * 1000;

    for( ; ; ) /* continue looping */
    {
       /* This bin still needs to be used by the other thread.  Put this thread
	  to sleep until the other thread is done with it. */
       if((p_rtn = pthread_cond_timedwait(&next_cond, &buffer_lock[bin],
					  &cond_wait_ts)) != 0)
       {
	  /* If the wait was interupted, go back and re-enter the
	     pthread_cond_timedwait() function. */
	  if(p_rtn == EINTR)
	     continue;
	  
	  pthread_mutex_unlock(&buffer_lock[bin]);
	  pack_return_values(info, 0, p_rtn, THREAD_ERROR,
			     "waiting for condition failed",
			     0.0, __FILE__, __LINE__);
	  return 1;
       }

       /* If we get here, pthread_cond_timedwait() returned 0 (success). */
       break;
    }
  }
  if((p_rtn = pthread_mutex_unlock(&buffer_lock[bin])) != 0)
  {
    pack_return_values(info, 0, p_rtn, THREAD_ERROR,
		       "mutex unlock failed", 0.0,
		       __FILE__, __LINE__);
    return 1;
  }

  /* Determine if the main thread sent the signal to indicate the other
     thread exited early from an error. If this value is still non-zero/zero,
     then assume there was an error. */
  if((stored[bin] > 0) == expected)  /*if(!stored[bin] == !expected)*/
  {
    pack_return_values(info, 0, ECANCELED, THREAD_ERROR,
		       "waiting for condition failed",
		       0.0, __FILE__, __LINE__);
    return 1;
  }
  
  return 0;
}

static int thread_signal(size_t bin, size_t bytes, struct transfer *info)
{
  int p_rtn;                    /* Pthread return value. */

  pthread_testcancel(); /* Don't grab a mutex if we should't use it. */
    
  /* Obtain the mutex lock for the specific buffer bin that is needed to
     clear the bin for writing. */
  if((p_rtn = pthread_mutex_lock(&buffer_lock[bin])) != 0)
  {
    pack_return_values(info, 0, p_rtn, THREAD_ERROR,
		       "mutex lock failed", 0.0, __FILE__, __LINE__);
    return 1;
  }
  
  /* Set the number of bytes in the buffer. After a write this is set
     to zero, and after a read it is set to the amount read. */
  /* Does this really belong here??? */
  stored[bin] = bytes;

  /* If other thread sleeping, wake it up. */
  if((p_rtn = pthread_cond_signal(&next_cond)) != 0)
  {
    pack_return_values(info, 0, p_rtn, THREAD_ERROR,
		       "waiting for condition failed",
		       0.0, __FILE__, __LINE__);
    return 1;
  }
  /* Release the mutex lock for this bin. */
  if((p_rtn = pthread_mutex_unlock(&buffer_lock[bin])) != 0)
  {
    pack_return_values(info, 0, p_rtn, THREAD_ERROR,
		       "mutex unlock failed", 0.0, __FILE__, __LINE__);
    return 1;
  }
  
  return 0;
}

/* WARNING: Only use thread_collect()  from the main thread.  Also, no other
   thread is allowd to use SIGALRM, sleep, pause, usleep.  Note: nanosleep()
   by posix definition is guarenteed not to use the alarm signal. */

static int thread_collect(pthread_t tid, unsigned int wait_time)
{
  int rtn;

  errno = 0;
  
  /* We don't want to leave the thread behind.  However, if something
     very bad occured that may be the only choice. */
  if(signal(SIGALRM, sig_alarm) != SIG_ERR)
  {
    /* If the alarm times off, the thread will not go away.  Probably, it
       is waiting in the kernel. */
    if(sigsetjmp(alarm_join, 0) == 0)
    {
      /* The only error returned is when the thread to cancel does not exist
	 (anymore).  Since the point is to stop it, if it is already stopped
	 then there is not a problem ignoring the error. */
      pthread_cancel(tid);

      /* Set the alarm to determine if the thread is still alive. */
      (void)alarm(wait_time);

      /* Collect the killed thread.  If this function fails to collect the
	 canceled thread it is because that thread is stuck in the kernel
	 waiting for i/o and cannot be killed.  On linux, a ps shows the
	 state of the thread as being in the 'D' state. */
      rtn = pthread_join(tid, (void**)NULL);

      /* Either an error occured or (more likely) the thread was joined by
	 this point.  Either way turn off the alarm. */
      (void)alarm(0);
      
      return rtn;
    }
    else
      return EINTR;
  }
  return errno;
}

/***************************************************************************/
/***************************************************************************/

/* Take the file descriptor of a file and return the files CRC. */
static unsigned int ecrc_readback(int fd)
{
  size_t buffer_size = align_to_page(ECRC_READBACK_BUFFER_SIZE);
                              /*buffer size for the readback data block*/
  void* readback_buffer;      /*the readback data buffer*/
  unsigned int crc = 0;       /*used to hold the crc as it is updated*/
  off_t nb, i;                /*loop control variables*/
  size_t rest;                /*the last part of the file < buffer_size*/
  struct stat stat_info;      /*used with fstat()*/
  ssize_t rtn;                /*system call return status*/
#ifdef O_DIRECT
  int getfl_fcntl;            /*fcntl() system call return status*/
  int setfl_fcntl;            /*new value to pass to fcntl()s third arg*/
#endif

  /* Stat to get the filesize. */
  if(fstat(fd, &stat_info) < 0)
     return 0;
  /* Verify this is a real file. */
  if(!S_ISREG(stat_info.st_mode))
  {
     errno = EINVAL;
     return 0;
  }
  
  /* Set to beginning of file. */
  if(lseek(fd, (off_t)0, SEEK_SET) != 0)
     return 0;
  
#ifdef O_DIRECT
  /* If O_DIRECT was used on the file descriptor, we need to turn it off.
     This simplifies the reading of the last part of the file that does
     not fit into an entire buffer_size sized space. */
  
  /*Get the current file descriptor flags.*/
  if((getfl_fcntl = fcntl(fd, F_GETFL, 0)) < 0)
     return 0;
  setfl_fcntl = getfl_fcntl & (~O_DIRECT);  /* turn off O_DIRECT */
  /*Set the new file descriptor flags.*/
  if(fcntl(fd, F_SETFL, setfl_fcntl) < 0)
    return 0;
#endif

  /*Initialize values used looping through reading in the file.*/
  nb = (off_t)(stat_info.st_size / buffer_size);
  rest = (size_t)(stat_info.st_size % buffer_size);

  /* Obtain page aligned buffer. */
  errno = 0;
  if((readback_buffer = page_aligned_malloc(buffer_size)) == NULL)
  {
     if(errno == 0)
     {
	errno = ENOMEM;
     }
     
     return 0;
  }
  
  /* Read in the file in 'buffer_size' sized blocks and calculate CRC. */
  for(i = 0; i < nb; i++)
  {
    if((rtn = read(fd, readback_buffer, buffer_size)) < 0) /* test for error */
    {
       free(readback_buffer);
       return 0;
    }
    else if(rtn != (ssize_t)buffer_size)             /* test for amount read */
    {
       free(readback_buffer);
       errno = EIO;
       return 0;
    }

    /* calc. the crc */
    crc = adler32(crc, readback_buffer, (unsigned int)buffer_size);
  }
  if(rest)
  {
    /* If one wanted to use direct i/o (or mmapped i/o with more work) for
       the paranoid ecrc readback test then the following read() would have 
       to have the 'rest' variable contain a page aligned value.  Most other
       values are already page aligned should someone wish this to be
       possible. */

    if((rtn = read(fd, readback_buffer, rest)) < 0)       /* test for error */
    {
       free(readback_buffer);
       return 0;
    }
    else if(rtn != (ssize_t)rest)                   /* test for amount read */
    {
       free(readback_buffer);
       errno = EIO;
       return 0;
    }

    /* calc. the crc */
    crc = adler32(crc, readback_buffer, (unsigned int)rest);
  }

  free(readback_buffer);
  
#ifdef O_DIRECT
  /*Set the original file descriptor flags.*/
  if(fcntl(fd, F_SETFL, getfl_fcntl) < 0)
     return 0;
#endif

  return crc;
}

/***************************************************************************/
/***************************************************************************/

static void do_read_write_threaded(struct transfer *reads,
				   struct transfer *writes)
{
  size_t array_size = reads->array_size;  /* Number of buffer bins. */
  size_t block_size = reads->block_size;  /* Align the buffers size. */
  size_t i;                            /* Loop counting. */
  int volatile p_rtn = 0;              /* pthread_*() return values. */
  pthread_t monitor_tid;               /* Thread id numbers. */
  struct timeval cond_wait_tv;  /* Absolute time to wait for cond. variable. */
  struct timespec cond_wait_ts; /* Absolute time to wait for cond. variable. */
  struct monitor monitor_info;  /* Stuct pointing to both transfer stucts. */
  pthread_attr_t read_attr;     /* Set any non-default thread attributes. */
  pthread_attr_t write_attr;    /* Set any non-default thread attributes. */

  /* Set the values for passing to the monitor thread. */
  monitor_info.read_info = reads;
  monitor_info.write_info = writes;

  /* Initialize the thread attributes to the system defaults. */

  /* Initialize the read thread attributes. */
  if((p_rtn = pthread_attr_init(&read_attr)) != 0)
  {
    pack_return_values(reads, 0, p_rtn, THREAD_ERROR,
		       "pthread_attr_init failed", 0.0, __FILE__, __LINE__);
    /*pack_return_values(writes, 0, p_rtn, THREAD_ERROR,
      "pthread_attr_init failed", 0.0, __FILE__, __LINE__);*/
    return;
  }
  
  /* Initialize the write thread attributes. */
  if((p_rtn = pthread_attr_init(&write_attr)) != 0)
  {
    /*pack_return_values(reads, 0, p_rtn, THREAD_ERROR,
      "pthread_attr_init failed", 0.0, __FILE__, __LINE__);*/
    pack_return_values(writes, 0, p_rtn, THREAD_ERROR,
		       "pthread_attr_init failed", 0.0, __FILE__, __LINE__);
    return;
  }

#if defined ( __sgi ) && defined ( PTHREAD_SCOPE_BOUND_NP )
  /* On IRIX/SGI, one can set a thread to run on a specifid CPU.  Posix
     says pthread_attr_setscope() only support PTHREAD_SCOPE_PROCESS and 
     PTHREAD_SCOPE_SYSTEM.  IRIX by default uses
     PTHREAD_SCOPE_PROCESS.  To change this to PTHREAD_SCOPE_SYSTEM requires
     root privledge.  IRIX supports a non-standerd scope called
     PTHREAD_SCOPE_BOUND_NP (6.5.9 kernels and later) that does not need
     root privledge to set.  The pthread_setrunon_np() function requires
     that a process have PTHREAD_SCOPE_SYSTEM or PTHREAD_SCOPE_BOUND_NP
     scope to set the cpu affinity. 

     For the purpose of thorough documentation the following functions set
     cpu/processor affinity on various architectures.  In all cases there
     is an equivalent 'get' functionality.
     
     IRIX:
     sysmp() with MP_SETMUSTRUN command  (If setting current process root 
                                          privledge not needed)
     pthread_setrunon_np() (about privledges; see above)

     Linux (2.5.8 and later kernels):
     sched_setaffinity() (If setting current process root privledge not needed)

     SunOS:
     processor_bind() (If setting current process root privledge not needed)
     [See processor_bind() for get functionality too.]

     OSF1:
     bind_to_cpu()    (If setting current process root privledge not needed)
     bind_to_cpu_id() (If setting current process root privledge not needed)
     [See getsysinfo() with GSI_CURRENT_CPU command for get functionality.]
  */

  /* Set the read thread scope to allow pthread_setrunon_np() to work. */
  if((p_rtn = pthread_attr_setscope(&read_attr, PTHREAD_SCOPE_BOUND_NP)) != 0)
  {
    pack_return_values(reads, 0, p_rtn, THREAD_ERROR,
		       "pthread_attr_init failed", 0.0, __FILE__, __LINE__);
    /*pack_return_values(writes, 0, p_rtn, THREAD_ERROR,
      "pthread_attr_init failed", 0.0, __FILE__, __LINE__);*/
    return;
  }

  /* Set the read thread scope to allow pthread_setrunon_np() to work. */
  if((p_rtn = pthread_attr_setscope(&read_attr, PTHREAD_SCOPE_BOUND_NP)) != 0)
  {
    pack_return_values(reads, 0, p_rtn, THREAD_ERROR,
		       "pthread_attr_init failed", 0.0, __FILE__, __LINE__);
    /*pack_return_values(writes, 0, p_rtn, THREAD_ERROR,
      "pthread_attr_init failed", 0.0, __FILE__, __LINE__);*/
    return;
  }

  /* Remember the affinity for use later.  If no cpu affinity exists, the
     return value is -1. Since, the process is not threaded yet, we don't 
     need to worry about the thread calls yet. */
  reads->cpu_affinity = writes->cpu_affinity = sysmp(MP_GETMUSTRUN);
#endif /* PTHREAD_SCOPE_BOUND_NP */

  /* Do stuff to the file descriptors. */

  /* Detect (and setup if necessary) the use of memory mapped io. */
  if(setup_mmap_io(reads))
    return;
  if(setup_mmap_io(writes))
    return;
  /* Detect (and setup if necessary) the use of direct io. */
  if(setup_direct_io(reads))
    return;
  if(setup_direct_io(writes))
    return;
  /* Detect (and setup if necessary) the use of posix io. */
  if(setup_posix_io(reads))
    return;
  if(setup_posix_io(writes))
    return;

  /* Allocate and initialize the arrays */

  /* Allocate and set to zeros the array that holds the number of bytes
     currently sitting in a bin. */
  errno = 0;
  if((stored = calloc(array_size, sizeof(int))) ==  NULL)
  {
    pack_return_values(reads, 0, errno, MEMORY_ERROR,
		       "calloc failed", 0.0, __FILE__, __LINE__);
    pack_return_values(writes, 0, errno, MEMORY_ERROR,
		       "calloc failed", 0.0, __FILE__, __LINE__);
    return;
  }
  /* Allocate and set to zeros the array of mutex locks for each buffer bin. */
  errno = 0;
  if((buffer_lock = calloc(array_size, sizeof(pthread_mutex_t))) == NULL)
  {
    pack_return_values(reads, 0, errno, MEMORY_ERROR,
		       "calloc failed", 0.0, __FILE__, __LINE__);
    pack_return_values(writes, 0, errno, MEMORY_ERROR,
		       "calloc failed", 0.0, __FILE__, __LINE__);
    return;
  }
  /* Allocate page aligned memory for the actuall data buffer. */
  errno = 0;
  if((buffer = page_aligned_malloc(
     array_size * align_to_page(block_size))) == NULL)
  {
    pack_return_values(reads, 0, errno, MEMORY_ERROR,
		       "memalign failed", 0.0, __FILE__, __LINE__);
    pack_return_values(writes, 0, errno, MEMORY_ERROR,
		       "memalign failed", 0.0, __FILE__, __LINE__);
    return;
  }

  if(thread_init(reads))
  {
    /* Since this error is for both reads and writes, copy it over to 
       the writes struct. */
    (void)memcpy(writes, reads, sizeof(reads));
    return;
  }
  /*Snag this mutex before spawning the new threads.  Otherwise, there is
    the possibility that the new threads will finish before the main thread
    can get to the pthread_cond_timedwait() to detect the threads exiting.*/
  if((p_rtn = pthread_mutex_lock(&done_mutex)) != 0)
  {
    pack_return_values(reads, 0, p_rtn, THREAD_ERROR,
		       "mutex lock failed", 0.0, __FILE__, __LINE__);
    pack_return_values(writes, 0, p_rtn, THREAD_ERROR,
		       "mutex lock failed", 0.0, __FILE__, __LINE__);
    return;
  }
  
  /* Get the threads going. */

  /* Start the thread that 'writes' the file. */
  if((p_rtn = pthread_create(&(writes->thread_id), &write_attr,
			     &thread_write, writes)) != 0)
  {
    pack_return_values(reads, 0, p_rtn, THREAD_ERROR,
		       "write thread creation failed", 0.0, __FILE__,__LINE__);
    pack_return_values(writes, 0, p_rtn, THREAD_ERROR,
		       "write thread creation failed", 0.0, __FILE__,__LINE__);
    return;
  }

  /* Start the thread that 'reads' the file. */
  if((p_rtn = pthread_create(&(reads->thread_id), &read_attr,
			     &thread_read, reads)) != 0)
  {
    /* Don't let this thread continue on forever. */
    (void)thread_collect(writes->thread_id, get_fsync_waittime(writes));

    pack_return_values(reads, 0, p_rtn, THREAD_ERROR,
		       "monitor thread creation failed", 0.0,
		       __FILE__, __LINE__);
    pack_return_values(writes, 0, p_rtn, THREAD_ERROR,
		       "monitor thread creation failed", 0.0,
		       __FILE__, __LINE__);
    return;
  }

  /* Start the thread that monitors the read and write threads. */
  if((p_rtn = pthread_create(&monitor_tid, NULL, &thread_monitor,
			     &monitor_info)) != 0)
  {
    /* Don't let these threads continue on forever. */
    (void)thread_collect(writes->thread_id, get_fsync_waittime(writes));
    (void)thread_collect(reads->thread_id, get_fsync_waittime(reads));

    pack_return_values(reads, 0, p_rtn, THREAD_ERROR,
		       "read thread creation failed", 0.0, __FILE__, __LINE__);
    pack_return_values(writes, 0, p_rtn, THREAD_ERROR,
		       "read thread creation failed", 0.0, __FILE__, __LINE__);
    return;
  }

  /* Determine the absolute time to wait in pthread_cond_timedwait(). */
  if(gettimeofday(&cond_wait_tv, NULL) < 0)
  {
    /* Don't let these threads continue on forever. */
    (void)thread_collect(writes->thread_id, get_fsync_waittime(writes));
    (void)thread_collect(reads->thread_id, get_fsync_waittime(reads));
    (void)thread_collect(monitor_tid, get_fsync_waittime(writes));

    pack_return_values(reads, 0, p_rtn, TIME_ERROR,
		       "read thread creation failed", 0.0, __FILE__, __LINE__);
    pack_return_values(writes, 0, p_rtn, TIME_ERROR,
		       "read thread creation failed", 0.0, __FILE__, __LINE__);
    return;
  }
  cond_wait_ts.tv_sec = cond_wait_tv.tv_sec + (60 * 60 * 6); /*wait 6 hours*/
  cond_wait_ts.tv_nsec = cond_wait_tv.tv_usec * 1000;

  /*This screewy loop of code is used to detect if a thread has terminated.
     If an error occurs either thread could return in any order.  If
     pthread_join() could join with any thread returned this would not
     be so complicated.*/
  while(!(reads->done && writes->done))
  {

    /* wait until the condition variable is set and we have the mutex */
    for( ; ; ) /* continue looping */
    {
       if((p_rtn = pthread_cond_timedwait(&done_cond, &done_mutex,
					  &cond_wait_ts)) != 0)
       {
	  /* If the wait was interupted, resume. */
	  if(p_rtn == EINTR)
	     continue;
	  
	  /* Don't let these threads continue on forever. */
	  (void)thread_collect(writes->thread_id, get_fsync_waittime(writes));
	  (void)thread_collect(reads->thread_id, get_fsync_waittime(reads));
	  (void)thread_collect(monitor_tid, get_fsync_waittime(writes));
	  
	  pack_return_values(reads, 0, p_rtn, THREAD_ERROR,
			     "waiting for condition failed", 0.0,
			     __FILE__, __LINE__);
	  pack_return_values(writes, 0, p_rtn, THREAD_ERROR,
			     "waiting for condition failed", 0.0,
			     __FILE__, __LINE__);
	  return;
       }

       /* If we get here, pthread_cond_timedwait() returned 0 (success). */
       break;
    }
    
    if(reads->done > 0) /*true when thread_read ends*/
    {
      if((p_rtn = thread_collect(reads->thread_id,
				 get_fsync_waittime(reads))) != 0)
      {
	/* If the error was EINTR, skip this handling.  The thread is hung
	 and it is knowningly being abandoned. */
	if(p_rtn != EINTR)
	{
	  /* Don't let these threads continue on forever. */
	  (void)thread_collect(writes->thread_id, get_fsync_waittime(writes));
	  (void)thread_collect(monitor_tid, get_fsync_waittime(writes));
	  
	  /* Since, pack_return_values aquires this mutex, release it. */
	  pthread_mutex_unlock(&done_mutex);

	  pack_return_values(reads, 0, p_rtn, THREAD_ERROR,
			     "joining with read thread failed",
			     0.0, __FILE__, __LINE__);
	  pack_return_values(writes, 0, p_rtn, THREAD_ERROR,
			     "joining with read thread failed",
			     0.0, __FILE__, __LINE__);
	  return;
	}
      }
      if(reads->exit_status)
      {
	/*(void)fprintf(stderr,
	  "Read thread exited with error(%d) '%s' from %s line %d.\n",
	  reads->errno_val, strerror(reads->errno_val),
	  reads->filename, reads->line);*/

	/* Signal the other thread there was an error. We need to lock the
	   mutex associated with the next bin to be used by the other thread.
	   Since, we don't know which one, get them all. */
	for(i = 0; i < array_size; i++)
	{
	   pthread_mutex_trylock(&(buffer_lock[i]));
	}
	pthread_cond_signal(&next_cond);
	for(i = 0; i < array_size; i++)
	{
	  pthread_mutex_unlock(&(buffer_lock[i]));
	}
      }
      reads->done = -1; /* Set to non-positive and non-zero value. */
    }
    if(writes->done > 0) /*true when thread_write ends*/
    {
      if((p_rtn = thread_collect(writes->thread_id,
				 get_fsync_waittime(writes))) != 0)
      {
	/* If the error was EINTR, skip this handling.  The thread is hung
	 and it is knowningly being abandoned. */
	if(p_rtn != EINTR)
	{
	  /* Don't let these threads continue on forever. */
	  (void)thread_collect(reads->thread_id, get_fsync_waittime(reads));
	  (void)thread_collect(monitor_tid, get_fsync_waittime(writes));
	  
	  /* Since, pack_return_values aquires this mutex, release it. */
	  pthread_mutex_unlock(&done_mutex);

	  pack_return_values(reads, 0, p_rtn, THREAD_ERROR,
			     "joining with write thread failed",
			     0.0, __FILE__, __LINE__);
	  pack_return_values(writes, 0, p_rtn, THREAD_ERROR,
			     "joining with write thread failed",
			     0.0, __FILE__, __LINE__);
	  return;
	}
      }
      if(writes->exit_status)
      {
	/*(void)fprintf(stderr,
	  "Write thread exited with error(%d) '%s' from %s line %d.\n",
	  writes->errno_val, strerror(writes->errno_val),
	  writes->filename, writes->line);*/

	/* Signal the other thread there was an error. We need to lock the
	   mutex associated with the next bin to be used by the other thread.
	   Since, we don't know which one, get them all.*/
	for(i = 0; i < array_size; i++)
	{
	  pthread_mutex_trylock(&(buffer_lock[i]));
	}
	pthread_cond_signal(&next_cond);
	for(i = 0; i < array_size; i++)
	{
	  pthread_mutex_unlock(&(buffer_lock[i]));
	}
      }
      writes->done = -1; /* Set to non-positive and non-zero value. */
    }
  }
  pthread_mutex_unlock(&done_mutex);

  /* Don't let this thread continue on forever. */
  (void)thread_collect(monitor_tid, get_fsync_waittime(writes));

  /*free the address space, this should only be done here if an error occured*/
  if(reads->mmap_ptr != MAP_FAILED)
  {
     if(munmap(reads->mmap_ptr, reads->mmap_len) < 0)
     {
	pack_return_values(reads, 0, errno, FILE_ERROR,
			   "munmap failed",
			   0.0, __FILE__, __LINE__);
	return;
     }
  }
  if(writes->mmap_ptr != MAP_FAILED)
  {
     if(munmap(writes->mmap_ptr, writes->mmap_len) < 0)
     {
	pack_return_values(writes, 0, errno, FILE_ERROR,
			   "munmap failed",
			   0.0, __FILE__, __LINE__);
	return;
     }
  }
  
  /* Print out an error message.  This information currently is not returned
     to encp.py. */
  if(reads->exit_status)
  {
    (void)fprintf(stderr, "Low-level read transfer failure: [Errno %d] %s: \n"
		  "\terror type: %d  filename: %s  line: %d\n\tHigher encp "
		  "levels will process this error and retry if possible.\n",
		  reads->errno_val, strerror(reads->errno_val),
		  reads->exit_status, reads->filename, reads->line);
    while(fflush(stderr) == EOF)
    {
       if(errno == EINTR) /* If a signal interupted things, try again... */
	  continue;
       else /* ... otherwise give up. */
	  break;
    }
  }
  if(writes->exit_status)
  {
    (void)fprintf(stderr, "Low-level write transfer failure: [Errno %d] %s: \n"
		  "\terror type: %d  filename: %s  line: %d\n\tHigher encp "
		  "levels will process this error and retry if possible.\n",
		  writes->errno_val, strerror(writes->errno_val),
		  writes->exit_status, writes->filename, writes->line);
    while(fflush(stderr) == EOF)
    {
       if(errno == EINTR) /* If a signal interupted things, try again... */
	  continue;
       else  /* ... otherwise give up. */
	  break;
    }
  }

  /*free the dynamic memory*/
  free(stored);
  free(buffer);
  free(buffer_lock);

  return;
}

static void* thread_monitor(void *monitor_info)
{
  struct transfer *read_info = ((struct monitor *)monitor_info)->read_info;
  struct transfer *write_info = ((struct monitor *)monitor_info)->write_info;
  struct timespec sleep_time;  /* Time to wait in nanosleep. */
  struct timeval start_read;   /* Old time to remember during nanosleep. */
  struct timeval start_write;  /* Old time to remember during nanosleep. */
  sigset_t sigs_to_block;      /* Signal set of those to block. */

  /* Block this signal.  Only the main thread should use/recieve it. */
  if(sigemptyset(&sigs_to_block) < 0)
    pthread_exit(NULL);
  if(sigaddset(&sigs_to_block, SIGALRM) < 0)
    pthread_exit(NULL);
  if(sigprocmask(SIG_BLOCK, &sigs_to_block, NULL) < 0)
    pthread_exit(NULL);

  /* This is the maximum time a read/write call is allowed to take. If it
     takes longer than this then it has not been able to achive a minimum 
     rate of 0.5 MB/S. */
  sleep_time.tv_sec = get_fsync_waittime(read_info);
  sleep_time.tv_nsec = 0;

  pthread_testcancel(); /* Don't grab a mutex if we should't use it. */

  if(pthread_mutex_lock(&done_mutex))
    pthread_exit(NULL);

  while(!read_info->done && !write_info->done)
  {
    if(pthread_mutex_unlock(&done_mutex))
      pthread_exit(NULL);

    pthread_testcancel(); /* Don't grab a mutex if we should't use it. */

    if(pthread_mutex_lock(&monitor_mutex))
      pthread_exit(NULL);

    /* Grab the currently recorded start time. */
    (void)memcpy(&start_read, &(read_info->start_transfer_function),
		 sizeof(struct timeval));
    (void)memcpy(&start_write, &(write_info->start_transfer_function),
		 sizeof(struct timeval));

    if(pthread_mutex_unlock(&monitor_mutex))
      pthread_exit(NULL);
    
    for( ; ; ) /* continue looping */
    {
      
      pthread_testcancel(); /* Don't sleep if main thread is waiting. */

      /* Wait for the amount of time that it would take to transfer the buffer
	 at 0.5 MB/S. */
      if(nanosleep(&sleep_time, NULL) < 0)
      {
	 pthread_testcancel(); /* Don't sleep if main thread is waiting. */
	 
	 /* If the nanosleep was interupted we want to keep going. */
	 if(errno == EINTR)
	    continue;
	 else
	    pthread_exit(NULL);
      }
      /* We successfully slept the full allotted time. */
      break;
    }
    
    pthread_testcancel(); /* Don't grab a mutex if we should't use it. */
    
    if(pthread_mutex_lock(&monitor_mutex))
      pthread_exit(NULL);
    if(pthread_mutex_lock(&done_mutex))
      pthread_exit(NULL);

    pthread_testcancel(); /* Don't continue if we should stop now. */
    
    /* Check the old time versus the new time to make sure it has changed.
       Also, check if the other thread has something to do (which means both
       are going equally slow/fast) and if the time is cleared; this is
       to avoid false positves. */

    if(!read_info->done && buffer_empty(read_info->array_size) &&
       (read_info->start_transfer_function.tv_sec > 0) &&
       (read_info->start_transfer_function.tv_usec > 0) &&
       (start_read.tv_sec == read_info->start_transfer_function.tv_sec) && 
       (start_read.tv_usec == read_info->start_transfer_function.tv_usec))
    {
      /* Tell the 'hung' thread to exit.  If we don't, then if/when it does
	 continue the memory locations have already been freed and will cause
	 a segmentation violation. */
      pthread_cancel(read_info->thread_id);
       
      /* Specify the following since we can't use pack_return_values() here. */
      read_info->crc_ui = 0;             /* Checksum */
      read_info->errno_val = EBUSY    ;  /* Errno value if error occured. */
      read_info->exit_status = 1;        /* Exit status of the thread. */
      read_info->msg = "write() system call stuck inside kernel too long";
                                          /* Additional error message. */
      read_info->transfer_time = 0.0;
      read_info->line = __LINE__;
      read_info->filename = __FILE__;
      read_info->done = 1;

      /* Signal the other thread there was an error. */
      pthread_cancel(write_info->thread_id);

      /* Specify the following since we can't use pack_return_values() here. */
      write_info->crc_ui = 0;             /* Checksum */
      write_info->errno_val = ECANCELED;  /* Errno value if error occured. */
      write_info->exit_status = THREAD_ERROR; /* Exit status of the thread. */
      write_info->msg = "thread was terminated";
                                          /* Additional error message. */
      write_info->transfer_time = 0.0;
      write_info->line = __LINE__;
      write_info->filename = __FILE__;
      write_info->done = 1;

      /* Tell the main thread to stop waiting (discover the other threads
	 failure) and error out nicely. */
      pthread_cond_signal(&done_cond);

      pthread_mutex_unlock(&monitor_mutex);
      pthread_mutex_unlock(&done_mutex);

      return NULL;
    }
    if(!write_info->done && buffer_full(write_info->array_size) &&
       (write_info->start_transfer_function.tv_sec > 0) &&
       (write_info->start_transfer_function.tv_usec > 0) &&
       (start_write.tv_sec == write_info->start_transfer_function.tv_sec) && 
       (start_write.tv_usec == write_info->start_transfer_function.tv_usec))
    {
      /* Tell the 'hung' thread to exit.  If we don't, then if/when it does
	 continue the memory locations have already been freed and will cause
	 a segmentation violation. */
      pthread_cancel(write_info->thread_id);
      /* Specify the following since we can't use pack_return_values() here. */
      write_info->crc_ui = 0;             /* Checksum */
      write_info->errno_val = EBUSY;      /* Errno value if error occured. */
      write_info->exit_status = THREAD_ERROR; /* Exit status of the thread. */
      write_info->msg = "write() system call stuck inside kernel too long";
                                          /* Additional error message. */
      write_info->transfer_time = 0.0;
      write_info->line = __LINE__;
      write_info->filename = __FILE__;
      write_info->done = 1;
      
      /* Signal the other thread there was an error. */
      pthread_cancel(read_info->thread_id);
      /* Specify the following since we can't use pack_return_values() here. */
      read_info->crc_ui = 0;             /* Checksum */
      read_info->errno_val = ECANCELED;  /* Errno value if error occured. */
      read_info->exit_status = THREAD_ERROR; /* Exit status of the thread. */
      read_info->msg = "thread was terminated";
                                         /* Additional error message. */
      read_info->transfer_time = 0.0;
      read_info->line = __LINE__;
      read_info->filename = __FILE__;
      read_info->done = 1;

      /* Tell the main thread to stop waiting (discover the other threads
	 failure) and error out nicely. */
      pthread_cond_signal(&done_cond);

      pthread_mutex_unlock(&monitor_mutex);
      pthread_mutex_unlock(&done_mutex);

      return NULL;
    }

    if(pthread_mutex_unlock(&monitor_mutex))
      pthread_exit(NULL);

  }
  
  pthread_mutex_unlock(&done_mutex);

  return NULL;
}

static void* thread_read(void *info)
{
  struct transfer *read_info = (struct transfer*)info; /* dereference */
  size_t bytes_remaining;       /* Number of bytes to move in one loop. */
  size_t bytes_transfered;      /* Bytes left to transfer in a sub loop. */
  int sts = 0;                  /* Return value from various C system calls. */
  size_t bin = 0U;              /* The current bin (bucket) to use. */
  unsigned int crc_ui = 0U;     /* Calculated checksum. */
  struct stat file_info;        /* Information about the file to read from. */
  struct timeval start_time;    /* Holds time measurement value. */
  struct timeval end_time;      /* Holds time measurement value. */
  struct rusage start_usage;    /* Hold time info from os billing. */
  struct rusage end_usage;      /* Hold time info from os billing. */
  struct timeval start_total;   /* Hold overall time measurment value. */
  struct timeval end_total;     /* Hold overall time measurment value. */
  double corrected_time = 0.0;  /* Corrected return time. */
  double transfer_time = 0.0;   /* Runing transfer time. */
  sigset_t sigs_to_block;       /* Signal set of those to block. */
#if defined ( __sgi ) && defined ( PTHREAD_SCOPE_BOUND_NP )
  int cpu_error;                /* If setrunon fails remember the error. */
#endif /* PTHREAD_SCOPE_BOUND_NP */

  /* Block this signal.  Only the main thread should use/recieve it. */
  if(sigemptyset(&sigs_to_block))
  {
    pack_return_values(read_info, 0, errno, SIGNAL_ERROR,
		       "sigemptyset failed", 0.0, __FILE__, __LINE__);
    return NULL;
  }
  if(sigaddset(&sigs_to_block, SIGALRM))
  {
    pack_return_values(read_info, 0, errno, SIGNAL_ERROR,
		       "sigaddset failed", 0.0, __FILE__, __LINE__);
    return NULL;
  }
  if(sigprocmask(SIG_BLOCK, &sigs_to_block, NULL) < 0)
  {
    pack_return_values(read_info, 0, errno, SIGNAL_ERROR,
		       "sigprocmask failed", 0.0, __FILE__, __LINE__);
    return NULL;
  }

  /* Initialize the time variables. */

  /* Initialize the running time incase of early failure. */
  (void)memset(&start_time, 0, sizeof(struct timeval));
  (void)memset(&end_time, 0, sizeof(struct timeval));
  /* Initialize the running time incase of early failure. */
  if(gettimeofday(&start_total, NULL) < 0)
  {
    pack_return_values(read_info, 0, errno, TIME_ERROR, "gettimeofday failed",
		       0.0, __FILE__, __LINE__);
    return NULL;
  }
  (void)memcpy(&end_total, &start_total, sizeof(struct timeval));
  /* Initialize the thread's start time usage. */
  errno = 0;
  if(getrusage(RUSAGE_SELF, &start_usage) < 0)
  {
    pack_return_values(read_info, 0, errno, TIME_ERROR, "getrusage failed",
		       0.0, __FILE__, __LINE__);
    return NULL;
  }

  /* Stat the file.  The mode is used to check if it is a regular file. */
  errno = 0;
  if(fstat(read_info->fd, &file_info))
  {
    pack_return_values(read_info, 0, errno, FILE_ERROR, "fstat failed", 0.0,
		       __FILE__, __LINE__);
    return NULL;
  }

#if defined ( __sgi ) && defined ( PTHREAD_SCOPE_BOUND_NP )
  /* Make sure that the cpu affinity that the main thread may have is applied
     to the thread FD that is a socket. */
  if(read_info->cpu_affinity >= 0 && S_ISSOCK(file_info.st_mode))
  {
    if((cpu_error = pthread_setrunon_np(read_info->cpu_affinity)) != 0)
      fprintf(stderr, "CPU affinity non-fatal error: %s\n",
	      strerror(cpu_error));
  }
#endif /* PTHREAD_SCOPE_BOUND_NP */

  while(read_info->bytes > 0)
  {
    /* If the mmapped memory segment is finished, get the next. */
    if(reinit_mmap_io(read_info))
      return NULL;

    /* If the other thread is slow, wait for it. */
    if(thread_wait(bin, read_info))
      return NULL;

    /* Determine the number of bytes to transfer during this inner loop. */
    bytes_remaining = (size_t)min3ull((unsigned long long) read_info->bytes,
				   (unsigned long long) read_info->block_size,
				   (unsigned long long) read_info->mmap_left);
    /* Set this to zero. */
    bytes_transfered = 0U;

    while(bytes_remaining > 0U)
    {
      /* Record the time to start waiting for the read to occur. */
      if(gettimeofday(&start_time, NULL) < 0)
      {
	pack_return_values(read_info, 0, errno, TIME_ERROR,
			   "gettimeofday failed", 0.0, __FILE__, __LINE__);
	return NULL;
      }

      /* Handle calling select to wait on the descriptor. */
      if(do_select(read_info))
	return NULL;

      pthread_testcancel(); /* Don't grab a mutex if we should't use it. */

      /* In case something happens, make sure that the monitor thread can
	 determine that the transfer is stuck. */
      if(pthread_mutex_lock(&monitor_mutex))
      {
	pack_return_values(read_info, 0, errno, THREAD_ERROR,
			   "mutex lock failed", 0.0, __FILE__, __LINE__);
	return NULL;
      }
      if(gettimeofday(&(read_info->start_transfer_function), NULL) < 0)
      {
	pack_return_values(read_info, 0, errno, TIME_ERROR,
			   "gettimeofday failed", 0.0, __FILE__, __LINE__);
	return NULL;
      }
      if(pthread_mutex_unlock(&monitor_mutex))
      {
	pack_return_values(read_info, 0, errno, THREAD_ERROR,
			   "mutex unlock failed", 0.0, __FILE__, __LINE__);
	return NULL;
      }
      
      /* Read in the data. */
      if(read_info->mmap_ptr != MAP_FAILED)
      {
	sts = mmap_read((buffer + (bin * read_info->block_size)),
			bytes_remaining, read_info);
      }
      else
      {
	/* Does double duty in that it also does the direct io read. */
	sts = posix_read(
	           (buffer + (bin * read_info->block_size) + bytes_transfered),
		   bytes_remaining, read_info);
	if(sts < 0)
	  return NULL;
      }
      
      pthread_testcancel(); /* Don't grab a mutex if we should't use it. */

      /* Since the read call returned, clear the timeval struct. */
      if(pthread_mutex_lock(&monitor_mutex))
      {
	pack_return_values(read_info, 0, errno, THREAD_ERROR,
			   "mutex lock failed", 0.0, __FILE__, __LINE__);
	return NULL;
      }
      read_info->start_transfer_function.tv_sec = 0;
      read_info->start_transfer_function.tv_usec = -1;
      if(pthread_mutex_unlock(&monitor_mutex))
      {
	pack_return_values(read_info, 0, errno, THREAD_ERROR,
			   "mutex unlock failed", 0.0, __FILE__, __LINE__);
	return NULL;
      }

      /* Record the time the read operation completes. */
      if(gettimeofday(&end_time, NULL) < 0)
      {
	pack_return_values(read_info, 0, errno, TIME_ERROR,
			   "gettimeofday failed", 0.0, __FILE__, __LINE__);
	return NULL;
      }
      /* Calculate wait time. */
      transfer_time += elapsed_time(&start_time, &end_time);

      /* Calculate the crc (if applicable). */
      switch (read_info->crc_flag)
      {
      case 0:  
	break;
      case 1:  
	crc_ui = adler32(crc_ui,
		 (buffer + (bin * read_info->block_size) + bytes_transfered),
			 (unsigned int)sts);
	read_info->crc_ui = crc_ui;
	break;
      default:  
	crc_ui = 0;
	read_info->crc_ui = crc_ui; 
	break;
      }

      /* Update this nested loop's counting variables. */
      bytes_remaining -= sts;
      bytes_transfered += sts;
      read_info->mmap_offset += sts;
      read_info->mmap_left -= sts;
      
#ifdef DEBUG
      print_status(stderr, bytes_transfered, bytes_remaining, read_info);
#endif /*DEBUG*/
    }

    if(thread_signal(bin, bytes_transfered, read_info))
       return NULL;

    /* Determine where to put the data. */
    bin = (bin + 1U) % read_info->array_size;
    pthread_testcancel(); /* Don't grab a mutex if we should't use it. */
    /* Determine the number of bytes left to transfer. */
    if(pthread_mutex_lock(&monitor_mutex))
    {
      pack_return_values(read_info, 0, errno, THREAD_ERROR,
			 "mutex lock failed", 0.0, __FILE__, __LINE__);
      return NULL;
    }
    read_info->bytes -= bytes_transfered;
    if(pthread_mutex_unlock(&monitor_mutex))
    {
      pack_return_values(read_info, 0, errno, THREAD_ERROR,
			 "mutex unlock failed", 0.0, __FILE__, __LINE__);
      return NULL;
    }
  }

  /* Sync the data to disk and other 'completion' steps. */
  if(finish_mmap_io(read_info))
    return NULL;

  /* Get total end time. */
  if(gettimeofday(&end_total, NULL) < 0)
  {
    pack_return_values(read_info, 0, errno, TIME_ERROR,
		       "gettimeofday failed", 0.0, __FILE__, __LINE__);
    return NULL;
  }
  /* Get the thread's time usage. */
  errno = 0;
  if(getrusage(RUSAGE_SELF, &end_usage) < 0)
  {
    pack_return_values(read_info, 0, errno, TIME_ERROR,
		       "getrusage failed", 0.0, __FILE__, __LINE__);
    return NULL;
  }

  /* If the descriptor is for a regular file returning the total time passed
     for use in the rate calculation appears accurate.  Unfortunatly, this
     method doesn't seem to return accurate time/rate information for sockets.
     Instead socket information seems most accurate by adding the total
     CPU time usage to the time spent in select() and read()/write(). */

  if(S_ISREG(file_info.st_mode))
    corrected_time = elapsed_time(&start_total, &end_total);
  else
    corrected_time = rusage_elapsed_time(&start_usage, &end_usage) +
      transfer_time;

  pack_return_values(read_info, read_info->crc_ui, 0, 0, "",
		     corrected_time, NULL, 0);
  return NULL;
}


static void* thread_write(void *info)
{
  struct transfer *write_info = (struct transfer*)info; /* dereference */
  size_t bytes_remaining;       /* Number of bytes to move in one loop. */
  size_t bytes_transfered;      /* Bytes left to transfer in a sub loop. */
  int sts = 0;                  /* Return value from various C system calls. */
  size_t bin = 0U;              /* The current bin (bucket) to use. */
  unsigned int crc_ui = 0U;     /* Calculated checksum. */
  struct stat file_info;        /* Information about the file to write to. */
  struct timeval start_time;    /* Holds time measurement value. */
  struct timeval end_time;      /* Holds time measurement value. */
  struct rusage start_usage;    /* Hold time info from os billing. */
  struct rusage end_usage;      /* Hold time info from os billing. */
  struct timeval start_total;   /* Hold overall time measurment value. */
  struct timeval end_total;     /* Hold overall time measurment value. */
  double corrected_time = 0.0;  /* Corrected return time. */
  double transfer_time = 0.0;   /* Runing transfer time. */
  sigset_t sigs_to_block;       /* Signal set of those to block. */
#if defined ( __sgi ) && defined ( PTHREAD_SCOPE_BOUND_NP )
  int cpu_error;                /* If setrunon fails remember the error. */
#endif /* PTHREAD_SCOPE_BOUND_NP */

  /* Block this signal.  Only the main thread should use/recieve it. */
  if(sigemptyset(&sigs_to_block) < 0)
  {
    pack_return_values(write_info, 0, errno, SIGNAL_ERROR,
		       "sigemptyset failed", 0.0, __FILE__, __LINE__);
    return NULL;
  }
  if(sigaddset(&sigs_to_block, SIGALRM) < 0)
  {
    pack_return_values(write_info, 0, errno, SIGNAL_ERROR,
		       "sigaddset failed", 0.0, __FILE__, __LINE__);
    return NULL;
  }
  if(sigprocmask(SIG_BLOCK, &sigs_to_block, NULL) < 0)
  {
    pack_return_values(write_info, 0, errno, SIGNAL_ERROR,
		       "sigprocmask failed", 0.0, __FILE__, __LINE__);
    return NULL;
  }
  
  /* Initialize the time variables. */

  /* Initialize the running time incase of early failure. */
  (void)memset(&start_time, 0, sizeof(struct timeval));
  (void)memset(&end_time, 0, sizeof(struct timeval));
  /* Initialize the running time incase of early failure. */
  if(gettimeofday(&start_total, NULL) < 0)
  {
    pack_return_values(write_info, 0, errno, TIME_ERROR,
		       "gettimeofday failed", 0.0, __FILE__, __LINE__);
    return NULL;
  }
  (void)memcpy(&end_total, &start_total, sizeof(struct timeval));
  /* Get the thread's start time usage. */
  if(getrusage(RUSAGE_SELF, &start_usage) < 0)
  {
    pack_return_values(write_info, 0, errno, TIME_ERROR, "getrusage failed",
		       0.0, __FILE__, __LINE__);
    return NULL;
  }

  /* Get stat info. */
  errno = 0;
  if(fstat(write_info->fd, &file_info) < 0)
  {
    pack_return_values(write_info, 0, errno, FILE_ERROR,
		       "fstat failed", 0.0, __FILE__, __LINE__);
    return NULL;
  }

#if defined ( __sgi ) && defined ( PTHREAD_SCOPE_BOUND_NP )
  /* Make sure that the cpu affinity that the main thread may have is applied
     to the thread FD that is a socket. */
  if(write_info->cpu_affinity >= 0 && S_ISSOCK(file_info.st_mode))
  {
    if((cpu_error = pthread_setrunon_np(write_info->cpu_affinity)) != 0)
      fprintf(stderr, "CPU affinity non-fatal error: %s\n",
	      strerror(cpu_error));
  }
#endif /* PTHREAD_SCOPE_BOUND_NP */

  while(write_info->bytes > 0)
  {
    /* If the mmapped memory segment is finished, get the next. */
    if(reinit_mmap_io(write_info))
      return NULL;

    /* If the other thread is slow, wait for it. */
    if(thread_wait(bin, write_info))
      return NULL;

    /* Determine the number of bytes to transfer during this inner loop. */
    bytes_remaining = stored[bin];
    /* Set this to zero. */
    bytes_transfered = 0U;

    while(bytes_remaining > 0U)
    {
      /* Record the time to start waiting for the read to occur. */
      if(gettimeofday(&start_time, NULL) < 0)
      {
	pack_return_values(write_info, 0, errno, TIME_ERROR,
			   "gettimeofday failed", 0.0, __FILE__, __LINE__);
	return NULL;
      }

      /* Handle calling select to wait on the descriptor. */
      if(do_select(write_info))
	return NULL;
      
      pthread_testcancel(); /* Don't grab a mutex if we should't use it. */

      /* In case something happens, make sure that the monitor thread can
	 determine that the transfer is stuck. */
      if(pthread_mutex_lock(&monitor_mutex))
      {
	pack_return_values(write_info, 0, errno, THREAD_ERROR,
			   "mutex lock failed", 0.0, __FILE__, __LINE__);
	return NULL;
      }
      if(gettimeofday(&(write_info->start_transfer_function), NULL) < 0)
      {
	pack_return_values(write_info, 0, errno, TIME_ERROR,
			   "gettimeofday failed", 0.0, __FILE__, __LINE__);
	return NULL;
      }
      if(pthread_mutex_unlock(&monitor_mutex))
      {
	pack_return_values(write_info, 0, errno, THREAD_ERROR,
			   "mutex unlock failed", 0.0, __FILE__, __LINE__);
	return NULL;
      }

      if(write_info->mmap_ptr != MAP_FAILED)
      {
	sts = mmap_write(
	          (buffer + (bin * write_info->block_size) + bytes_transfered),
		  bytes_remaining, write_info);
      }
      else
      {
	/* Does double duty in that it also does the direct io write. */
	sts = posix_write(
		  (buffer + (bin * write_info->block_size) + bytes_transfered),
		  bytes_remaining, write_info);
	if(sts < 0)
	  return NULL;
      }

      pthread_testcancel(); /* Don't grab a mutex if we should't use it. */

      /* Since the write call returned, clear the timeval struct. */
      if(pthread_mutex_lock(&monitor_mutex))
      {
	pack_return_values(write_info, 0, errno, THREAD_ERROR,
			   "mutex lock failed", 0.0, __FILE__, __LINE__);
	return NULL;
      }
      write_info->start_transfer_function.tv_sec = 0;
      write_info->start_transfer_function.tv_usec = -1;
      if(pthread_mutex_unlock(&monitor_mutex))
      {
	pack_return_values(write_info, 0, errno, THREAD_ERROR,
			   "mutex unlock failed", 0.0, __FILE__, __LINE__);
	return NULL;
      }

      /* Record the time that this thread wakes up from waiting for the
	 condition variable. */
      if(gettimeofday(&end_time, NULL) < 0)
      {
	pack_return_values(write_info, 0, errno, TIME_ERROR,
			   "gettimeofday failed", 0.0, __FILE__, __LINE__);
	return NULL;
      }
      /* Get total end time. */
      transfer_time += elapsed_time(&start_time, &end_time);

      /* Calculate the crc (if applicable). */
      switch (write_info->crc_flag)
      {
      case 0:
	break;
      case 1:
	crc_ui = adler32(crc_ui,
		 (buffer + (bin * write_info->block_size) + bytes_transfered),
			 (unsigned int)sts);
	/*to cause intentional crc errors, use the following line instead*/
	/*crc_ui=adler32(crc_ui, (buffer), sts);*/
	write_info->crc_ui = crc_ui;
	break;
      default:
	crc_ui=0U;
	write_info->crc_ui = crc_ui;
	break;
      }

      /* Update this nested loop's counting variables. */
      bytes_remaining -= sts;
      bytes_transfered += sts;
      write_info->mmap_offset += sts;
      write_info->mmap_left -= sts;

#ifdef DEBUG
      print_status(stderr, bytes_transfered, bytes_remaining, write_info);
#endif /*DEBUG*/
    }

    if(thread_signal(bin, 0, write_info))
       return NULL;

    /* Determine where to get the data. */
    bin = (bin + 1U) % write_info->array_size;
    pthread_testcancel(); /* Don't grab a mutex if we should't use it. */
    /* Determine the number of bytes left to transfer. */
    if(pthread_mutex_lock(&monitor_mutex))
    {
      pack_return_values(write_info, 0, errno, THREAD_ERROR,
			 "mutex lock failed", 0.0, __FILE__, __LINE__);
      return NULL;
    }
    write_info->bytes -= bytes_transfered;    
    if(pthread_mutex_unlock(&monitor_mutex))
    {
      pack_return_values(write_info, 0, errno, THREAD_ERROR,
			 "mutex unlock failed", 0.0, __FILE__, __LINE__);
      return NULL;
    }
  }

  /* If mmapped io was used, unmap the last segment. */
  if(finish_mmap_io(write_info))
    return NULL;

  /* Get total end time. */
  if(gettimeofday(&end_total, NULL) < 0)
  {
    pack_return_values(write_info, 0, errno, TIME_ERROR,
		       "gettimeofday failed", 0.0, __FILE__, __LINE__);
    return NULL;
  }
  /* Get the thread's time usage. */
  errno = 0;
  if(getrusage(RUSAGE_SELF, &end_usage) < 0)
  {
    pack_return_values(write_info, 0, errno, TIME_ERROR,
		       "getrusage failed", 0.0, __FILE__, __LINE__);
    return NULL;
  }

  /* If the descriptor is for a regular file returning the total time passed
     for use in the rate calculation appears accurate.  Unfortunatly, this
     method doesn't seem to return accurate time/rate information for sockets.
     Instead socket information seems most accurate by adding the total
     CPU time usage to the time spent in select() and read()/write(). */

  if(S_ISREG(file_info.st_mode))
    corrected_time = elapsed_time(&start_total, &end_total);
  else
    corrected_time = rusage_elapsed_time(&start_usage, &end_usage) + 
      transfer_time;

  pack_return_values(info, crc_ui, 0, 0, "", corrected_time, NULL, 0);
  return NULL;
}

/***************************************************************************/
/***************************************************************************/

static void do_read_write(struct transfer *read_info,
			  struct transfer *write_info)
{
  ssize_t sts;                  /* Return status from read() and write(). */
  size_t bytes_remaining;       /* Number of bytes to move in one loop. */
  size_t bytes_transfered;      /* Number of bytes moved in one loop. */
#ifdef PROFILE
  struct profile profile_data[PROFILE_COUNT]; /* profile data array */
  long profile_count = 0;       /* Index of profile array. */
#endif /*PROFILE*/
  struct timeval start_time;    /* Start of time the thread is active. */
  struct timeval end_time;      /* End of time the thread is active. */
  double time_elapsed;          /* Difference between start and end time. */
  unsigned int crc_ui = 0;      /* Calculated checksum. */

#ifdef PROFILE
  (void)memset(profile_data, 0, sizeof(profile_data));
#endif /*PROFILE*/

  /* Detect (and setup if necessary) the use of memory mapped io. */
  if(setup_mmap_io(read_info))
    return;
  if(setup_mmap_io(write_info))
    return;
  /* Detect (and setup if necessary) the use of direct io. */
  if(setup_direct_io(read_info))
    return;
  if(setup_direct_io(write_info))
    return;
  /* Detect (and setup if necessary) the use of posix io. */
  if(setup_posix_io(read_info))
    return;
  if(setup_posix_io(write_info))
    return;

  /* Allocate and initialize the arrays */
  
  /* Allocate page aligned memory for the actuall data buffer. */
  errno = 0;
  if((buffer = page_aligned_malloc(read_info->block_size)) == NULL)
  {
    pack_return_values(read_info, 0, errno, MEMORY_ERROR, "memalign failed",
		       0.0, __FILE__, __LINE__);
    pack_return_values(write_info, 0, errno, MEMORY_ERROR, "memalign failed",
		       0.0, __FILE__, __LINE__);
    return;
  }
#ifdef DEBUG
  /* Allocate and set to zeros the array (that is one element in length)
     that holds the number of bytes currently sitting in a bin. */
  errno = 0;
  if((stored = malloc(sizeof(int))) == NULL)
  {
    pack_return_values(read_info, 0, errno, MEMORY_ERROR, "malloc failed",
		       0.0, __FILE__, __LINE__);
    pack_return_values(write_info, 0, errno, MEMORY_ERROR, "malloc failed",
		       0.0, __FILE__, __LINE__);
    return;
  }
  *stored = 0;
#endif /*DEBUG*/

  /* Get the time that the thread started to work on transfering data. */
  if(gettimeofday(&start_time, NULL) < 0)
  {
    pack_return_values(read_info, 0, errno, TIME_ERROR,
		       "gettimeofday failed", 0.0, __FILE__, __LINE__);
    pack_return_values(write_info, 0, errno, TIME_ERROR,
		       "gettimeofday failed", 0.0, __FILE__, __LINE__);
    return;
  }
  (void)memcpy(&end_time, &start_time, sizeof(struct timeval));

  while(read_info->bytes > 0 && write_info->bytes > 0)
  {
    /* Since, either one could use mmap io, this needs to be done on both
       every time. */
    if(reinit_mmap_io(read_info))
      return;
    if(reinit_mmap_io(write_info))
      return;

    /* Number of bytes remaining for this loop. */
    bytes_remaining = (size_t)min3ull((unsigned long long)read_info->bytes,
				    (unsigned long long)read_info->block_size,
				    (unsigned long long)read_info->mmap_left);
    /* Set this to zero. */
    bytes_transfered = 0U;

    while(bytes_remaining > 0U)
    {
#ifdef PROFILE
      update_profile(1, bytes_remaining, read_info->fd,
		     profile_data, &profile_count);
#endif /*PROFILE*/

      /* Handle calling select to wait on the descriptor. */
      if(do_select(read_info))
	return;

#ifdef PROFILE
      update_profile(2, bytes_remaining, read_info->fd,
		     profile_data, &profile_count);
#endif /*PROFILE*/

#ifdef PROFILE
      update_profile(3, bytes_remaining, read_info->fd,
		     profile_data, &profile_count);
#endif /*PROFILE*/

      /* Read in the data. */
      if(read_info->mmap_ptr != MAP_FAILED)
      {
	sts = mmap_read(buffer, bytes_remaining, read_info);
      }
      else
      {
	/* Does double duty in that it also does the direct io read. */
	sts = posix_read((buffer + bytes_transfered),
			 bytes_remaining, read_info);
	if(sts < 0)
	  return;
      }

#ifdef PROFILE
      update_profile(4, sts, read_info->fd,
		     profile_data, &profile_count);
#endif /*PROFILE*/

      /* Update this nested loop's counting variables. */
      bytes_remaining -= sts;
      bytes_transfered += sts;
      read_info->mmap_offset += sts;
      read_info->bytes -= sts;

#ifdef DEBUG
      *stored = bytes_transfered;
      print_status(stderr, bytes_transfered, bytes_remaining, read_info);
#endif /*DEBUG*/
    }

    /* Initialize the write loop variables. */
    bytes_remaining = bytes_transfered;
    bytes_transfered = 0U;

    while (bytes_remaining > 0U)
    {

#ifdef PROFILE
      update_profile(5, bytes_remaining, write_info->fd,
		     profile_data, &profile_count);
#endif /*PROFILE*/

      /* Handle calling select to wait on the descriptor. */
      if(do_select(write_info))
	return;

#ifdef PROFILE
      update_profile(6, bytes_remaining, write_info->fd,
		     profile_data, &profile_count);
#endif /*PROFILE*/

#ifdef PROFILE
      update_profile(7, bytes_remaining, write_info->fd,
		     profile_data, &profile_count);
#endif /*PROFILE*/

      if(write_info->mmap_ptr != MAP_FAILED)
      {
	sts = mmap_write((void*)((uintptr_t)buffer + bytes_transfered),
			 bytes_remaining, write_info);
      }
      else
      {
	/* Does double duty in that it also does the direct io read. */
	sts = posix_write((void*)((uintptr_t)buffer + bytes_transfered),
			  bytes_remaining, write_info);
	if(sts < 0)
	  return;
      }

#ifdef PROFILE
      update_profile(8, sts, write_info->fd,
		     profile_data, &profile_count);
#endif /*PROFILE*/

      switch (write_info->crc_flag)
      {
      case 0:
	break;
      case 1:  
	crc_ui=adler32(crc_ui, (void*)((uintptr_t)buffer + bytes_transfered),
		       (unsigned int)sts);
	/*write_info->crc_ui = crc_ui;*/
	break;
      default:
	crc_ui = 0;
	/*write_info->crc_ui = crc_ui;*/
	break;
      }

      /* Handle calling select to wait on the descriptor. */
      bytes_remaining -= sts;
      bytes_transfered += sts;
      write_info->mmap_offset += sts;
      write_info->bytes -= sts;

#ifdef DEBUG
      *stored = 0;
      write_info->crc_ui = crc_ui;
      print_status(stderr, bytes_transfered, bytes_remaining, write_info);
#endif /*DEBUG*/
    }
  }
  
  /* Sync the data to disk and other 'completion' steps.  There is not
     a finish_read() function, since reading does not require calling any
     *sync() function. */
  if(finish_write(write_info))
    return;
  if(finish_mmap_io(read_info))
    return;
  if(finish_mmap_io(write_info))
    return;

  /* Get the time that the thread finished to work on transfering data. */
  if(gettimeofday(&end_time, NULL) < 0)
  {
    pack_return_values(read_info, 0, errno, TIME_ERROR,
		       "gettimeofday failed", 0.0, __FILE__, __LINE__);
    pack_return_values(write_info, 0, errno, TIME_ERROR,
		       "gettimeofday failed", 0.0, __FILE__, __LINE__);
    return;
  }
  time_elapsed = elapsed_time(&start_time, &end_time);

  /* Release the buffer memory. */
  free(buffer);
#ifdef DEBUG
  free(stored);
#endif /*DEBUG*/

#ifdef PROFILE
  print_profile(profile_data, profile_count);
#endif /*PROFILE*/

  pack_return_values(write_info, crc_ui, 0, 0, "", time_elapsed, NULL, 0);
  pack_return_values(read_info, crc_ui, 0, 0, "", time_elapsed, NULL, 0);
  return;
}

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
raise_exception2(struct transfer *rtn_val)
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
		      PyFloat_FromDouble(rtn_val->transfer_time),
		      PyFloat_FromDouble(rtn_val->transfer_time),
		      rtn_val->filename, rtn_val->line);
    if (v != NULL)
    {   PyErr_SetObject(EXErrObject, v);
	Py_DECREF(v);
    }
    return NULL;
}

static PyObject *
EXfd_ecrc(PyObject *self, PyObject *args)
{
  int fd;                     /*the file descriptor*/
  unsigned int crc;
  int sts;
  
  /* Get the parameter. */
  sts = PyArg_ParseTuple(args, "i", &fd);
  if (!sts)
     return(raise_exception("fd_ecrc - invalid parameter"));

  errno = 0;
  crc = (unsigned long)ecrc_readback(fd);
  
  if(errno == 0)
     return PyLong_FromUnsignedLong(crc);
  else
     return raise_exception("fd_ecrc - error");
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
    struct transfer reads;
    struct transfer writes;
    
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
    if (crc_flag > 1 || crc_flag < 0)
	return(raise_exception("fd_xfer - invalid crc param"));

    /* determine mmap array size */
    if (PyLong_Check(mmap_size_obj))
	mmap_size = PyLong_AsLong(mmap_size_obj);
    else if (PyInt_Check(mmap_size_obj))
	mmap_size = (long long)PyInt_AsLong(mmap_size_obj);
    else
	return(raise_exception("fd_xfer - invalid mmap_size param"));

    /*Place the values into the struct.  Some compilers complained when this
      information was placed into the struct inline at initalization.  So it
      was moved here.*/
    (void)memset(&reads, 0, sizeof(reads));
    (void)memset(&writes, 0, sizeof(writes));
    reads.fd = fr_fd;
    reads.mmap_ptr = MAP_FAILED;
    reads.mmap_len = 0;
    reads.size = no_bytes;
    reads.bytes = no_bytes;
    reads.block_size = align_to_page(block_size);
    if(threaded_transfer)
      reads.array_size = array_size;
    else
      reads.array_size = 1;
    reads.mmap_size = mmap_size;
    reads.timeout = timeout;
#ifdef DEBUG
    reads.crc_flag = 1; /*crc_flag;*/
#else
    reads.crc_flag = 0;
#endif
    reads.transfer_direction = -1; /*read*/
    reads.direct_io = direct_io;
    reads.mmap_io = mmap_io;
    writes.fd = to_fd;
    writes.mmap_ptr = MAP_FAILED;
    writes.mmap_len = 0;
    writes.size = no_bytes;
    writes.bytes = no_bytes;
    writes.block_size = align_to_page(block_size);
    if(threaded_transfer)
      writes.array_size = array_size;
    else
      writes.array_size = 1;
    writes.mmap_size = mmap_size;
    writes.timeout = timeout;
    writes.crc_flag = crc_flag;
    writes.transfer_direction = 1; /*write*/
    writes.direct_io = direct_io;
    writes.mmap_io = mmap_io;

    errno = 0;
    if(threaded_transfer)
      do_read_write_threaded(&reads, &writes);
    else
      do_read_write(&reads, &writes);

    /* If the write error is ECANCELED then use the read error, because
       this indicates that the read thread exited first and the ECANCELED
       from the write thread means it knew to exit early. */

    if (writes.exit_status != 0 && writes.errno_val != ECANCELED)
        return (raise_exception2(&writes));
    else if (reads.exit_status != 0)
        return (raise_exception2(&reads));

    rr = Py_BuildValue("(i,O,O,i,s,O,O,s,i)",
		       writes.exit_status, 
		       PyLong_FromUnsignedLong(writes.crc_ui),
		       PyLong_FromLongLong(writes.size),
		       writes.errno_val, writes.msg,
		       PyFloat_FromDouble(reads.transfer_time),
		       PyFloat_FromDouble(writes.transfer_time),
		       writes.filename, writes.line);

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

#define ON_OFF(value) ((char*)((value) ? "on" : "off"))

int main(int argc, char **argv)
{
  int fd_in, fd_out;
  struct stat file_info;
  long long size;
  struct timeval timeout = {60, 0};
  unsigned int crc_ui;
  int flags_in = 0;
  int flags_out = 0;
  int opt;
  int          verbose = 0;
  size_t       block_size = 256*1024;
  size_t       array_size = 3;
  size_t       mmap_size = 96*1024*1024;
  int          direct_io = 0;
  int          mmap_io= 0;
  int          threaded_transfer = 0;
  int          ecrc = 0;
  struct transfer reads;
  struct transfer writes;
  char abspath[PATH_MAX + 1];
  int direct_io_index = 0;
  int direct_io_in = 0;
  int direct_io_out = 0;
  int mmap_io_index = 0;
  int mmap_io_in = 0;
  int mmap_io_out = 0;
  int first_file_optind = 0;
  int second_file_optind = 0;

  
  opterr = 0;
  while(optind < argc)
  {
    /* The + for the first character in optstring is need on Linux machines
     to tell getopt to use the posix compliant version of getopt(). */
    while(((opt = getopt(argc, argv, "+evtmda:b:l:")) != -1))
    {
      switch(opt)
      {
      case 'v':
	verbose = 1; /* print out extra information. */
	break;
      case 'e':
	ecrc = 1;
        break;
      case 't':  /* threaded transfer */
	threaded_transfer = 1;
	break;
      case 'm':  /* memory mapped i/o */
	mmap_io += 1;
	if(mmap_io_index == 0)
	  mmap_io_index = optind - 1;
	break;
      case 'd':  /* direct i/o */
	direct_io += 1;
	if(direct_io_index == 0)
	  direct_io_index = optind - 1;
	break;
      case 'a':  /* array size */
	errno = 0;
	if((array_size = (size_t)strtoul(optarg, NULL, 0)) == 0)
	{
	  (void)fprintf(stderr, "invalid array size(%s): %s\n",
			 optarg, strerror(errno));
	  return 1;
	}
	break;
      case 'b':  /* block size */
	errno = 0;
	if((block_size = (size_t)strtoul(optarg, NULL, 0)) == 0)
	{
	  (void)fprintf(stderr, "invalid block size(%s): %s\n",
		       optarg, strerror(errno));
	  return 1;
	}
	break;
      case 'l':  /* mmap length */
	errno = 0;
	if((mmap_size = (size_t)strtoul(optarg, NULL, 0)) == 0)
	{
	  (void)fprintf(stderr, "invalid mmap size(%s): %s\n",
		       optarg, strerror(errno));
	  return 1;
	}
	break;
      default:
	(void)fprintf(stderr, "Unknown option: -%c\n", optopt);
      }
    }
    /* Remember the index for the first non-option argument found. */
    if((argv[optind] != NULL) && (argv[optind][0] != '-') && 
	(first_file_optind == 0))
       first_file_optind = optind;
    /* Remember the index for the second non-option argument found. */
    else if((argv[optind] != NULL) && (argv[optind][0] != '-') && 
	    (second_file_optind == 0))
       second_file_optind = optind;
    
    /* When a filename is found, getopt() stops processing the command line.
       This bumps the optind up one so it can continue. */
    if(optind < argc)
      optind++;
  }

  /* Determine if the direct io was for the input file, output file or both. */
  if((mmap_io == 1) && (mmap_io_index < first_file_optind))
     mmap_io_in = 1;
  else if(mmap_io == 1)
     mmap_io_out = 1;
  else if(mmap_io > 1)
     mmap_io_in = mmap_io_out = 1;

  /* Determine if the mmap io was for the input file, output file or both. */
  if((direct_io == 1) && (direct_io_index < first_file_optind))
     direct_io_in = 1;
  else if(direct_io == 1)
     direct_io_out = 1;
  else if(direct_io > 1)
     direct_io_in = direct_io_out = 1;

  /* Determine the flags for the input file descriptor. */
  flags_in = 0;
/*#ifdef O_DIRECT
  if(direct_io_in)
     flags_in |= O_DIRECT;
     #endif*/
  if(mmap_io_in)
     flags_in |= O_RDWR;
  else
     flags_in |= O_RDONLY;
  
  /* Determine the flags for the output file descriptor. */
  flags_out |= O_CREAT | O_TRUNC;
/*#ifdef O_DIRECT
  if(direct_io_out)
     flags_out |= O_DIRECT;
     #endif*/ 
  if(mmap_io_out || ecrc)
     flags_out |= O_RDWR;
  else
     flags_out |= O_WRONLY;
  
  /* Check the number of arguments from the command line. */
  if(argc < 3)
  {
    (void)fprintf(stderr,
		  "Usage: %s [-edmtva:b:l:] <source_file> [-dm] <dest_file>\n",
		  argv[0]);
    return 1;
  }

  if(verbose)
  {
     (void)printf("Threaded: %s\n", ON_OFF(threaded_transfer));
     (void)printf("Ecrc: %s\n", ON_OFF(ecrc));
     (void)printf("Block size: %d\n", block_size);
     (void)printf("Array size: %d\n", array_size);
     (void)printf("Mmap size: %u\n", mmap_size);
     (void)printf("Direct i/o in: %s\n", ON_OFF(direct_io_in));
     (void)printf("Mmap i/o in: %s\n", ON_OFF(mmap_io_in));
     (void)printf("Direct i/o out: %s\n", ON_OFF(direct_io_out));
     (void)printf("Mmap i/o out: %s\n", ON_OFF(mmap_io_out));
  }

  /* Check the input file. */
  if(argv[first_file_optind] == NULL)
  {
     (void)fprintf(stderr, "input file not specified.\n");
     return 1;
  }
  errno = 0;
  if(stat(argv[first_file_optind], &file_info) < 0)
  {
     (void)fprintf(stderr, "input stat(%s): %s\n",
		   argv[first_file_optind], strerror(errno));
     return 1;
  }
  errno = 0;
  if(realpath(argv[first_file_optind], abspath) == NULL)
  {
     (void)fprintf(stderr, "input file(%s): %s\n",
		   argv[first_file_optind], strerror(errno));
     return 1;
  }
  errno = 0;
  if(!S_ISREG(file_info.st_mode) && (strcmp(abspath, "/dev/zero") != 0))
  {
     (void)fprintf(stderr, "input file %s is not a regular file\n", abspath);
     return 1;
  }
  /* If reading from /dev/zero, set the size.  Otherwise, remember the size
     of the file. */
  if(strcmp(abspath, "/dev/zero") == 0)
    size = 1024*1024*1024;  /* 1GB */
  else
    size = file_info.st_size;
  
  /* Open the input file. */
  errno = 0;
  if((fd_in = open(abspath, flags_in)) < 0)
  {
    (void)fprintf(stderr, "input open(%s): %s\n", abspath, strerror(errno));
    return 1;
  }

  if(verbose)
  {
     (void)fprintf(stderr, "The input file: %s\n", abspath);
  }
  
  /* Check the output file. */
  if(argv[second_file_optind] == NULL)
  {
     (void)fprintf(stderr, "output file not specified.\n");
     return 1;
  }

  /* Open the output file. */
  errno = 0;
  if((fd_out = open(argv[second_file_optind], flags_out,
		    S_IRUSR | S_IWUSR | S_IRGRP)) < 0)
  {
     (void)fprintf(stderr, "output open(%s): %s\n",
		   argv[second_file_optind], strerror(errno));
     return 1;
  }
  errno = 0;
  if(realpath(argv[second_file_optind], abspath) == NULL)
  {
     (void)fprintf(stderr, "output file(%s): %s\n",
		   argv[second_file_optind], strerror(errno));
     return 1;
  }

  /* Check the output file. */
  errno = 0;
  if(stat(abspath, &file_info) < 0)
  {
     (void)fprintf(stderr, "output stat(%s): %s\n", abspath, strerror(errno));
     return 1;
  }
  errno = 0;
  if(!S_ISREG(file_info.st_mode) && (strcmp(abspath, "/dev/null") != 0))
  {
     (void)fprintf(stderr, "output file %s is not a regular file\n", abspath);
     return 1;
  }
  
  if(verbose)
  {
     (void)printf("The output file: %s\n", abspath);
  }
  
  /*Place the values into the struct.  Some compilers complained when this
    information was placed into the struct inline at initalization.  So it
    was moved here.*/
  (void)memset(&reads, 0, sizeof(reads));
  (void)memset(&writes, 0, sizeof(writes));
  reads.fd = fd_in;
  reads.mmap_ptr = MAP_FAILED;
  reads.mmap_len = 0;
  reads.size = size;
  reads.bytes = size;
  reads.block_size = align_to_page(block_size);
  reads.array_size = array_size;
  reads.mmap_size = mmap_size;
  reads.timeout = timeout;
#ifdef DEBUG
  reads.crc_flag = 1;
#else
  reads.crc_flag = 0;
#endif
  reads.transfer_direction = -1; /* negitive means read */
  reads.direct_io = (bool)direct_io_in;
  reads.mmap_io = (bool)mmap_io_in;
  writes.fd = fd_out;
  writes.mmap_ptr = MAP_FAILED;
  writes.mmap_len = 0;
  writes.size = size;
  writes.bytes = size;
  writes.block_size = align_to_page(block_size);
  writes.array_size = array_size;
  writes.mmap_size = mmap_size;
  writes.timeout = timeout;
  writes.crc_flag = 1;
  writes.transfer_direction = 1; /* positive means write */
  writes.direct_io = (bool)direct_io_out;
  writes.mmap_io = (bool)mmap_io_out;

  /* Do the transfer test. */
  errno = 0;
  if(threaded_transfer)
     do_read_write_threaded(&reads, &writes);
  else
    do_read_write(&reads, &writes);

  if (writes.exit_status != 0 && writes.errno_val != ECANCELED)
  {
     (void)printf("Write error [ errno %d ]: %s: %s  File: %s  Line: %d\n",
		  writes.errno_val, strerror(writes.errno_val), writes.msg,
		  writes.filename, writes.line);
     return 1;
  }
  else if (reads.exit_status != 0)
  {
     (void)printf("Read error [ errno %d ]: %s: %s:  File: %s  Line: %d\n",
		  reads.errno_val, strerror(reads.errno_val), reads.msg,
		  reads.filename, reads.line);
     return 1;
  }
  else
  {
     if(verbose)
	(void)printf("Read time: %f  Write time: %f  Size: %lld  CRC: %u\n",
		     reads.transfer_time, writes.transfer_time,
		     size, writes.crc_ui);
     (void)printf("Read rate: %f MB/s Write rate: %f MB/s\n",
		  (double)(size/(1024*1024)/reads.transfer_time),
		  (double)(size/(1024*1024)/writes.transfer_time));
  }

  if(ecrc)
  {
     errno = 0;
     crc_ui = ecrc_readback(writes.fd);
     if((crc_ui == 0U) && (errno != 0U))
     {
	(void)printf("Error performing ecrc readback check: %s\n",
		     strerror(errno));
     }
     else
     {
	if(crc_ui != writes.crc_ui)
	   (void)printf("CRC mismatch: original: %u  readback: %u\n",
			writes.crc_ui, crc_ui);
     }
  }
  
  return 0;
}

#endif
