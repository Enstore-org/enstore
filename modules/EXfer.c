/* EXfer.c - Low level data transfer C modules for encp. */

/* $Id$*/


#ifndef STAND_ALONE
#include <Python.h>
#else
#define _GNU_SOURCE
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

/***************************************************************************
 constants and macros
**************************************************************************/

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

/* A little hack for linux. */
#ifdef __linix__
#define __USE_BSD
#endif

/* Define DEBUG only for extra debugging output */
/*#define DEBUG*/

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

#define ECRC_READBACK_BUFFER_SIZE (1024*1024)

/***************************************************************************
 definitions
**************************************************************************/

struct transfer
{
  int fd;                 /*file descriptor*/

  unsigned long long size;  /*size in bytes*/
  unsigned long long bytes; /*bytes left to transfer*/
  unsigned int block_size;  /*size of block*/
  int array_size;         /*number of buffers to use*/
  long mmap_size;         /*mmap address space segment lengths*/

  void *mmap_ptr;         /*memory mapped i/o pointer*/
  off_t mmap_len;         /*length of memory mapped file offset*/
  off_t mmap_offset;      /*Offset from beginning of mmapped segment. */
  off_t mmap_left;        /* Bytes to next mmap segment. */
  int mmap_count;         /* Number of mmapped segments done. */

  long long fsync_threshold; /* Number of bytes to wait between fsync()s. 
			        It is the max of block_size, mmap_size and
				1% of the filesize. */
  long long last_fsync;      /* Number of bytes done though last fsync(). */

  struct timeval timeout; /*time to wait for data to be ready*/
  struct timeval start_transfer_function; /*time last read/write was started.*/
  double transfer_time;   /*time spent transfering data*/

  int crc_flag;           /*crc flag - 0 or 1*/
  unsigned int crc_ui;    /*checksum*/
  
  int transfer_direction; /*positive means write, negative means read*/
  
  int direct_io;          /*is true if using direct io*/
  int mmap_io;            /*is true if using memory mapped io*/
  int threaded;           /*is true if using threaded implementation*/
  
  short int done;         /*is true if this part of the transfer is finished.*/
  pthread_t thread_id;    /*the thread id (if doing MT transfer)*/
  
  int exit_status;        /*error status*/
  int errno_val;          /*errno of any errors (zero otherwise)*/
  char* msg;              /*additional error message*/
  int line;               /*line number where error occured*/
  char* filename;         /*filename where error occured*/

};

struct monitor
{
  struct transfer *read_info;
  struct transfer *write_info;
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
unsigned int adler32(unsigned int, char *, int);

#ifndef STAND_ALONE
void initEXfer(void);
static PyObject * raise_exception(char *msg);
static PyObject * EXfd_xfer(PyObject *self, PyObject *args);
static PyObject * EXfd_ecrc(PyObject *self, PyObject *args);
#endif
void do_read_write_threaded(struct transfer *reads, struct transfer *writes);
void do_read_write(struct transfer *reads, struct transfer *writes);
static struct transfer* pack_return_values(struct transfer *info,
					   unsigned int crc_ui,
					   int errno_val, int exit_status,
					   char* msg,
					   double transfer_time,
					   char *filename, int line);
static double elapsed_time(struct timeval* start_time,
			   struct timeval* end_time);
static double rusage_elapsed_time(struct rusage *sru, struct rusage *eru);
static long long get_fsync_threshold(struct transfer *info);
static time_t get_fsync_waittime(struct transfer *info);
static long align_to_page(long value);
static long align_to_size(long value, long align);
unsigned long long min(int num, ...);
unsigned long long max(int num, ...);
static int setup_mmap_io(struct transfer *info);
static int setup_direct_io(struct transfer *info);
static int setup_posix_io(struct transfer *info);
static int reinit_mmap_io(struct transfer *info);
static int finish_mmap(struct transfer *info);
static int finish_write(struct transfer *info);
static int do_select(struct transfer *info);
static ssize_t mmap_read(void *dst, size_t bytes_to_transfer,
			 struct transfer *info);
static ssize_t mmap_write(void *src, size_t bytes_to_transfer,
			  struct transfer *info);
static ssize_t posix_read(void *dst, size_t bytes_to_transfer,
			  struct transfer* info);
static ssize_t posix_write(void *src, size_t bytes_to_transfer,
			   struct transfer* info);
int thread_init(struct transfer *info);
int thread_wait(int bin, struct transfer *info);
int thread_signal(int bin, size_t bytes, struct transfer *info);
int thread_collect(int tid, time_t wait_time);
static void* thread_read(void *info);
static void* thread_write(void *info);
static void* thread_monitor(void *monitor);
unsigned int ecrc_readback(int fd);
#ifdef PROFILE
void update_profile(int whereami, int sts, int sock,
		    struct profile *profile_data, long *profile_count);
void print_profile(struct profile *profile_data, int profile_count);
#endif /*PROFILE*/
#ifdef DEBUG
static void print_status(FILE *fp, int, int, struct transfer *info);
			 /*char, long long, unsigned int, int array_size);*/
#endif /*DEBUG*/
static int buffer_empty(int array_size);
static int buffer_full(int array_size);
static void sig_alarm(int sig_num);

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

int *stored;   /*pointer to array of bytes copied per bin*/
char *buffer;  /*pointer to array of buffer bins*/
pthread_mutex_t *buffer_lock; /*pointer to array of bin mutex locks*/
pthread_mutex_t done_mutex; /*used to signal main thread a thread returned*/
pthread_mutex_t monitor_mutex; /*used to sync the monitoring*/
pthread_cond_t done_cond;   /*used to signal main thread a thread returned*/
pthread_cond_t next_cond;   /*used to signal peer thread to continue*/
#ifdef DEBUG
pthread_mutex_t print_lock; /*order debugging output*/
#endif
static sigjmp_buf alarm_join;

/***************************************************************************
 user defined functions
**************************************************************************/

static void sig_alarm(int sig_num)
{
  siglongjmp(alarm_join, 1);
}

static int buffer_empty(int array_size)
{
  int i;  /*loop counting*/

  for(i = 0; i < array_size; i++)
  {
    pthread_testcancel(); /* Don't grab a mutex if we should't use it. */
    pthread_mutex_lock(&(buffer_lock[i]));
    if(stored[i])
    {
      pthread_mutex_unlock(&(buffer_lock[i]));
      return 0;
    }
    pthread_mutex_unlock(&(buffer_lock[i]));
  }
  
  return 1;
}

static int buffer_full(int array_size)
{
  int i;  /*loop counting*/

  for(i = 0; i < array_size; i++)
  {
    pthread_testcancel(); /* Don't grab a mutex if we should't use it. */
    pthread_mutex_lock(&(buffer_lock[i]));
    if(!stored[i])
    {
      pthread_mutex_unlock(&(buffer_lock[i]));
      return 0;
    }
    pthread_mutex_unlock(&(buffer_lock[i]));
  }

  return 1;
}

/* Pack the arguments into a struct return_values. */
static struct transfer* pack_return_values(struct transfer* retval,
					   unsigned int crc_ui,
					   int errno_val,
					   int exit_status,
					   char* message,
					   double transfer_time,
					   char* filename, int line)
{
  pthread_testcancel(); /* Don't grab a mutex if we should't use it. */

  /* Do not bother with checking return values for errors.  Should the
     pthread_* functions fail at this point, there is notthing else to
     do but set the global flag and return. */
  pthread_mutex_lock(&done_mutex);

  retval->crc_ui = crc_ui;             /* Checksum */
  retval->errno_val = errno_val;       /* Errno value if error occured. */
  retval->exit_status = exit_status;   /* Exit status of the thread. */
  retval->msg = message;               /* Additional error message. */
  retval->transfer_time = transfer_time;
  retval->line = line;             
  retval->filename = filename;
  retval->done = 1;

  /* Putting the following here is just the lazy thing to do. */
  /* For this code to work this must be executed after setting retval->done
     to 1 above. */
  pthread_cond_signal(&done_cond);

  pthread_mutex_unlock(&done_mutex);

  return retval;
}

static double elapsed_time(struct timeval* start_time,
			   struct timeval* end_time)
{
  double elapsed_time;  /* variable to hold the time difference */

  elapsed_time = (extract_time(end_time) - extract_time(start_time));

  return elapsed_time;
}

/* Function to take two usage structs and return the total time difference. */
static double rusage_elapsed_time(struct rusage *sru, struct rusage *eru)
{
  return ((extract_time((&(eru->ru_stime)))+extract_time((&(eru->ru_utime)))) -
	  (extract_time((&(sru->ru_stime)))+extract_time((&(sru->ru_utime)))));
}

static long long get_fsync_threshold(struct transfer *info)
{
  long long temp_value;

  /* Find out what one percent of the file size is. */
  temp_value = (long long)(info->bytes / (double)100.0);

  /* Return the largest of these values. */
  return (long long)max(3, (unsigned long long)temp_value,
			(unsigned long long)info->block_size,
			(unsigned long long)info->mmap_size);
}

/* Returns the number of seconds to wait for another thread. */
static time_t get_fsync_waittime(struct transfer *info)
{
  /* Don't use info->fsync_threshold; it may not be initalized yet. */
  
  /* Calculate the amount of time to wait for the amount of data transfered
     between syncs will take assuming a minumum rate requirement. */
  return (time_t)(get_fsync_threshold(info)/524288.0) + 1;
  
  /* To cause intentional DEVICE_ERRORs use the following line instead. */
  /*return (time_t)(info->block_size/524288.0) + 1;*/
}

/* A usefull function to round a value to the next full page. */
static long align_to_page(long value)
{
   return align_to_size(value, sysconf(_SC_PAGESIZE));
}

/* A usefull function to round a vlue to the next full required
   alignment size. */
static long align_to_size(long value, long align)
{
   return (value % align) ? (value + align - (value % align)) : value;
}

/* Return 0 for false, >1 for true, <1 for error. */
int is_empty(int bin)
{
  int rtn = 0; /*hold return value*/

  pthread_testcancel(); /* Don't grab a mutex if we should't use it. */

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

/*First argument is the number of arguments to follow.
  The rest are the arguments to find the min of.*/
unsigned long long min(int num, ...)
{
  va_list ap;
  int i;
#ifdef ULLONG_MAX
  unsigned long long min_val = ULLONG_MAX;
#else
  unsigned long long min_val = ULONG_MAX; /*Note: should be ULLONG_MAX */
#endif
  unsigned long long current;

  va_start(ap, num);

  for(i = 0; i < num; i++)
  {
    if((current = va_arg(ap, unsigned long long)) < min_val)
      min_val = current;
  }
  return min_val;
}

/*First argument is the number of arguments to follow.
  The rest are the arguments to find the max of.*/
unsigned long long max(int num, ...)
{
  va_list ap;
  int i;
  unsigned long long max_val = 0;
  unsigned long long current;

  va_start(ap, num);

  for(i = 0; i < num; i++)
  {
    if((current = va_arg(ap, unsigned long long)) > max_val)
      max_val = current;
  }
  return max_val;
}

/***************************************************************************/
/***************************************************************************/

#ifdef DEBUG
static void print_status(FILE* fp, int bytes_transfered,
			 int bytes_remaining, struct transfer *info)
{
  int i;
  char debug_print;
  char direction;

  /* Print F if entire bin is transfered, P if bin partially transfered. */
  debug_print = (bytes_remaining) ? 'P' : 'F';
  /* Print W if write R if read. */
  direction = (info->transfer_direction > 0) ? 'W' : 'R';
  
  pthread_mutex_lock(&print_lock);

  fprintf(fp, "%c%c bytes: %15lld crc: %10u | ",
	  direction, debug_print, info->bytes, info->crc_ui);

  for(i = 0; i < info->array_size; i++)
  {
    fprintf(fp, " %6d", stored[i]);
  }
  fprintf(fp, "\n");

  pthread_mutex_unlock(&print_lock);

}
#endif /*DEBUG*/

#ifdef PROFILE
void update_profile(int whereami, int sts, int sock,
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


/***************************************************************************/
/***************************************************************************/

static int setup_mmap_io(struct transfer *info)
{
  int fd = info->fd;            /* The file descriptor in question. */
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
  /* If the file is a local disk, use memory mapped i/o on it. */
  if(S_ISREG(file_info.st_mode))
  {

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
      {
	pack_return_values(info, 0, errno, FILE_ERROR,
			   "ftruncate failed", 0.0, __FILE__, __LINE__);
	return 1;
      }
    }
  }

  return 0;
}

static int reinit_mmap_io(struct transfer *info)
{
  int advise_value = 0; /* Advise hints for madvise. */

  /* If the file is a local disk, use memory mapped i/o on it. 
     Only advance to the next mmap segment when the previous one is done. */
  if(info->mmap_ptr != MAP_FAILED && info->mmap_offset == info->mmap_len)
  {
    /* Unmap the current mapped memory segment. */
    errno = 0;
    if(munmap(info->mmap_ptr, info->mmap_len) < 0)
    {
      pack_return_values(info, 0, errno, FILE_ERROR,
			 "munmap failed", 0.0, __FILE__, __LINE__);
      return 1;
    }

    /* Reset these values for the next segment. */
    info->mmap_len = (info->bytes<info->mmap_len)?info->bytes:info->mmap_len;
    info->mmap_offset = 0;
    info->mmap_count += 1;
    info->mmap_left = info->mmap_len;
    
    /* Create the memory mapped file. */
    errno = 0;
    if((info->mmap_ptr = mmap(NULL, info->mmap_len, PROT_WRITE | PROT_READ,
			      MAP_SHARED, info->fd,
			      info->mmap_count * info->mmap_len)) 
       == (caddr_t)-1)
    {
      pack_return_values(info, 0, errno, FILE_ERROR,
			 "mmap failed", 0.0, __FILE__, __LINE__);
      return 1;
    }
    
    if(info->transfer_direction > 0) /*write*/
      advise_value |= MADV_SEQUENTIAL;
    else if(info->transfer_direction < 0)
      advise_value |= (MADV_SEQUENTIAL | MADV_WILLNEED);
    
    /* Advise the system on the memory mapped i/o usage pattern. */
    errno = 0;
    if(madvise(info->mmap_ptr, info->mmap_len, advise_value) < 0)
    {
      pack_return_values(info, 0, errno, FILE_ERROR,
			 "madvise failed", 0.0, __FILE__, __LINE__);
      return 1;
    }
  }
  else if(info->mmap_offset == info->mmap_len)
  {
    /* Reset these values for the next segment. Even if this thread does
       not care about page allignment, the other thread might. */
    info->mmap_len = (info->bytes<info->mmap_len)?info->bytes:info->mmap_len;
    info->mmap_offset = 0;
    info->mmap_count += 1;
    info->mmap_left = info->mmap_len;
  }

  return 0;
}

static int finish_mmap(struct transfer *info)
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
       any errors are ignored. */
    fsync(info->fd);
  }

  return 0;
}


static int setup_direct_io(struct transfer *info)
{
  struct stat file_info;  /* Information about the file to read/write from. */
  int sts, rtn_fcntl;
  

  /* If direct io was specified, check if it may work. */
  if(info->direct_io)
  {
    /* Determine if the file descriptor supports fsync(). */
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
    }
    else
    {
#ifdef O_DIRECT
      /* If the system supports direct i/o attempt to turn it on. */
  
      /*Get the current file descriptor flags.*/
      if((rtn_fcntl = fcntl(info->fd, F_GETFL, 0)) < 0)
      {
	pack_return_values(info, 0, errno, FILE_ERROR,
			   "fcntl(F_GETFL) failed", 0.0, __FILE__, __LINE__);
	return 1;
      }
      sts = rtn_fcntl | O_DIRECT;  /* turn on O_DIRECT */
      /*Set the new file descriptor flags.*/
      if(fcntl(info->fd, F_SETFL, sts) < 0)
      {
	pack_return_values(info, 0, errno, FILE_ERROR,
			   "fcntl(F_SETFL) failed", 0.0, __FILE__, __LINE__);
	return 1;
      }
#else
      info->direct_io = 0;
#endif
    }
  }

  return 0;
}

static int setup_posix_io(struct transfer *info)
{
  struct stat file_info;  /* Information about the file to read/write from. */

  /* Determine if the file descriptor supports fsync(). */
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
  FD_SET(info->fd,&fds);
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
  memcpy(dst, (void*)((unsigned int)info->mmap_ptr + 
		      (unsigned int)info->mmap_offset),
	 bytes_to_transfer);
  
 return bytes_to_transfer;
}

static ssize_t mmap_write(void *src, size_t bytes_to_transfer,
			  struct transfer *info)
{
  int sync_type = 0;            /* Type of msync() to perform. */

  /* If file supports memory mapped i/o. */
  errno = 0;
  memcpy((void*)((unsigned int)info->mmap_ptr +
		 (unsigned int)info->mmap_offset),
	 src,
	 bytes_to_transfer);

  /* If this is the very end of the file, don't just set the dirty pages
     to be written to disk, wait for them to be written out to disk. */
  if((info->bytes - bytes_to_transfer) == 0)
    sync_type = MS_SYNC;
  else
    sync_type = MS_ASYNC;

  pthread_testcancel(); /* Any syncing action will take time. */

  /* Schedule the data for sync to disk now. */
  msync((void*)((unsigned int)info->mmap_ptr +
		(unsigned int)info->mmap_offset),
	bytes_to_transfer, sync_type);
  
  pthread_testcancel(); /* Any syncing action will take time. */

  return bytes_to_transfer;
}

/* Act like the posix read() call.  But return all interpreted errors with -1.
   Also, set error values appropratly when detected. */
static ssize_t posix_read(void *dst, size_t bytes_to_transfer,
			  struct transfer* info)
{
  int sts = 0;                  /* Return value from various C system calls. */
  
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

static ssize_t posix_write(void *src, size_t bytes_to_transfer,
			   struct transfer* info)
{
  int sts = 0;                  /* Return value from various C system calls. */

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
    if(info->bytes <= sts)
    {
      /* Adjust the sts. */
      sts = ((int)((signed long long)info->bytes));
      /* Truncate size at end of transfer.  For direct io all writes must be
	 a multiple of the page size.  The last write must be truncated down
	 to the correct size. */
      ftruncate(info->fd, info->size);
    }
  }
  else
  {
    /* Force the data to disk.  Don't let encp take up to much memory.
       This isnt the most accurate way of doing this, however it is less
       overhead. */
    if(info->fsync_threshold)
    {
      /* If the amount of data transfered between fsync()s has passed,
	 do the fsync and record amount completed. */
      
      if((info->last_fsync - info->bytes/* - sts*/) > info->fsync_threshold)
      {
	info->last_fsync = info->bytes - sts;
	pthread_testcancel(); /* Any sync action will take time. */
	/*fsync(info->fd);*/
	sync();
      }
      /* If the entire file is transfered, do the fsync(). */
      else if((info->bytes - sts) == 0)
      {
	info->last_fsync = info->bytes - sts;
	pthread_testcancel(); /* Any syncing action will take time. */
	/*fsync(info->fd);*/
	sync();
      }
    }
    pthread_testcancel(); /* Any syncing action will take time. */
  }

  return sts;
}

/***************************************************************************/
/***************************************************************************/

int thread_init(struct transfer *info)
{
  int p_rtn;                    /* Pthread return value. */
  int i;

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
  /* initalize the mutex for signaling when a thread has finished. */
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

/* The first parameter is the bin to wait on.  The second parameter should
   be zero if waiting for the bin to be empty, non zero if needs to contain
   data.  Last paramater is the transfer struct for this half of the 
   transfer. */
int thread_wait(int bin, struct transfer *info)
{
  int p_rtn;                    /* Pthread return value. */
  struct timeval cond_wait_tv;  /* Absolute time to wait for cond. variable. */
  struct timespec cond_wait_ts; /* Absolute time to wait for cond. variable. */
  int expected;
  
  if(info->transfer_direction > 0)  /*write*/
    expected = 1;
  else                              /*read*/
    expected = 0;

  pthread_testcancel(); /* Don't grab a mutex if we should't use it. */

  /* Determine if the lock for the buffer_lock bin, bin, is ready. */
  if((p_rtn = pthread_mutex_lock(&buffer_lock[bin])) != 0)
  {
    pack_return_values(info, 0, p_rtn, THREAD_ERROR,
		       "mutex lock failed", 0.0, __FILE__, __LINE__);
    return 1;
  }
  /* If they don't match then wait. */
  if(!stored[bin] != !expected)
  {
    /* Determine the absolute time to wait in pthread_cond_timedwait(). */
    gettimeofday(&cond_wait_tv, NULL);
    cond_wait_ts.tv_sec = cond_wait_tv.tv_sec + info->timeout.tv_sec;
    cond_wait_ts.tv_nsec = cond_wait_tv.tv_usec * 1000;

  wait_for_condition:

    /* This bin still needs to be used by the other thread.  Put this thread
       to sleep until the other thread is done with it. */
    if((p_rtn = pthread_cond_timedwait(&next_cond, &buffer_lock[bin],
				       &cond_wait_ts)) != 0)
    {
      /* If the wait was interupted, resume. */
      if(p_rtn == EINTR)
	goto wait_for_condition;

      pthread_mutex_unlock(&buffer_lock[bin]);
      pack_return_values(info, 0, p_rtn, THREAD_ERROR,
			 "waiting for condition failed",
			 0.0, __FILE__, __LINE__);
      return 1;
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
  if(!stored[bin] != !expected)
  {
    pack_return_values(info, 0, ECANCELED, THREAD_ERROR,
		       "waiting for condition failed",
		       0.0, __FILE__, __LINE__);
    return 1;
  }
  
  return 0;
}

int thread_signal(int bin, size_t bytes, struct transfer *info)
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

/* WARNING: Only use this function from the main thread.  Also, no other
   thread is allowd to use SIGALRM, sleep, pause, usleep.  Note: nanosleep()
   by posix definition is guarenteed not to use the alarm signal. */

int thread_collect(int tid, time_t wait_time)
{
  int rtn;

  errno = 0;
  
  /* We don't want to leave the thread behind.  However, if something
     very bad occured that maybe the only choice. */
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

      /* Collect the killed thread. */
      alarm(wait_time);
      rtn = pthread_join(tid, (void**)NULL);
      alarm(0);
      
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
unsigned int ecrc_readback(int fd)
{
  int buffer_size=ECRC_READBACK_BUFFER_SIZE;/*buffer size for the data blocks*/
  char buffer[ECRC_READBACK_BUFFER_SIZE];   /*the data buffer*/
  unsigned int crc = 0;       /*used to hold the crc as it is updated*/
  long nb, rest, i;           /*loop control variables*/
  struct stat stat_info;      /*used with fstat()*/
  int sts;                    /*system call return status*/
  int rtn;                    /*system call return status*/
  int rtn_fcntl;              /*fcntl() system call return status*/

  if(fstat(fd, &stat_info) < 0)   /* To get the filesize. */
     /*return(raise_exception("fd_ecrc - file fstat failed"));*/
     return 0;
  if(!S_ISREG(stat_info.st_mode))
  {
     errno = EINVAL;
     return 0;
  }
  if(lseek(fd, 0, SEEK_SET) != 0) /* Set to beginning of file. */
     /*return(raise_exception("fd_ecrc - file lseek failed"));*/
     return 0;

#ifdef O_DIRECT
  /* If O_DIRECT was used on the file descriptor, we need to turn it off.
   It seems that (on Linux anyway) writing a file with direct i/o then
   rewinding and rereading the file causes the reads to return the wrong
   data. On an IRIX machine, rewinding an O_DIRECT opened file and rereading
   results in an EINVAL error. */
  
  /*Get the current file descriptor flags.*/
  if((rtn_fcntl = fcntl(fd, F_GETFL, 0)) < 0)
     /*return(raise_exception("fd_ecrc - file fcntl(F_GETFL) failed"));*/
     return 0;
  sts = rtn_fcntl & (~O_DIRECT);  /* turn off O_DIRECT */
  /*Set the new file descriptor flags.*/
  if(fcntl(fd, F_SETFL, sts) < 0)
     /*return(raise_exception("fd_ecrc - file fcntl(F_SETFL) failed"));*/
     return 0;
#endif

  /*Initialize values used looping through reading in the file.*/
  nb = stat_info.st_size / buffer_size;
  rest = stat_info.st_size % buffer_size;

  /*Read in the file in 'buf_size' sized blocks and calculate CRC.*/
  for (i = 0; i < nb; i++)
  {
    if((rtn = read(fd, buffer, buffer_size)) < 0)   /* test for error */
       /*return(raise_exception("fd_ecrc - file read failed"));*/
       return 0;
    else if(rtn != buffer_size)                     /* test for amount read */
    {
       errno = EIO;
       /*return(raise_exception("fd_ecrc - partial buffer read"));*/
       return 0;
    }
    
    crc = adler32(crc, buffer, buffer_size);  /* calc. the crc */
  }
  if (rest)
  {
    if((rtn = read(fd, buffer, rest)) < 0)          /* test for error */
       /*return(raise_exception("fd_ecrc - file read failed"));*/
       return 0;
    else if(rtn != rest)                            /* test for amount read */
    {
       errno = EIO;
       /*return(raise_exception("fd_ecrc - partial buffer read"));*/
       return 0;
    }
    
    crc = adler32(crc, buffer, rest);  /* calc. the crc */
  }

#ifdef O_DIRECT
  /*Set the original file descriptor flags.*/
  if(fcntl(fd, F_SETFL, rtn_fcntl) < 0)
     /*return(raise_exception("fd_ecrc - file fcntl(F_SETFL) failed"));*/
     return 0;
#endif

  /*return PyLong_FromUnsignedLong((unsigned long)crc);*/
  return (unsigned long)crc;
}

/***************************************************************************/
/***************************************************************************/

void do_read_write_threaded(struct transfer *reads, struct transfer *writes)
{
  int array_size = reads->array_size;  /* Number of buffer bins. */
  int block_size = reads->block_size;  /* Align the buffers size. */
  int i;                               /* Loop counting. */
  int p_rtn = 0;                       /* pthread* return values. */
  pthread_t monitor_tid;               /* Thread id numbers. */
  struct timeval cond_wait_tv;  /* Absolute time to wait for cond. variable. */
  struct timespec cond_wait_ts; /* Absolute time to wait for cond. variable. */
  struct monitor monitor_info;  /* Stuct pointing to both transfer stucts. */

  monitor_info.read_info = reads;
  monitor_info.write_info = writes;

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
  errno = 0;
  if((stored = calloc(array_size, sizeof(int))) ==  NULL)
  {
    pack_return_values(reads, 0, errno, MEMORY_ERROR,
		       "calloc failed", 0.0, __FILE__, __LINE__);
    pack_return_values(writes, 0, errno, MEMORY_ERROR,
		       "calloc failed", 0.0, __FILE__, __LINE__);
    return;
  }
  errno = 0;
  if((buffer_lock = calloc(array_size, sizeof(pthread_mutex_t))) == NULL)
  {
    pack_return_values(reads, 0, errno, MEMORY_ERROR,
		       "calloc failed", 0.0, __FILE__, __LINE__);
    pack_return_values(writes, 0, errno, MEMORY_ERROR,
		       "calloc failed", 0.0, __FILE__, __LINE__);
    return;
  }
  errno = 0;
  if((buffer = memalign(sysconf(_SC_PAGESIZE),
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
    memcpy(writes, reads, sizeof(reads));
    return;
  }
  /*Snag this mutex before spawning the new threads.  Otherwise, there is
    the possibility that the new threads will finish before the main thread
    can get to the pthread_cond_wait() to detect the threads exiting.*/
  if((p_rtn = pthread_mutex_lock(&done_mutex)) != 0)
  {
    pack_return_values(reads, 0, p_rtn, THREAD_ERROR,
		       "mutex lock failed", 0.0, __FILE__, __LINE__);
    pack_return_values(writes, 0, p_rtn, THREAD_ERROR,
		       "mutex lock failed", 0.0, __FILE__, __LINE__);
    return;
  }
  
  /* get the threads going. */
  if((p_rtn = pthread_create(&(writes->thread_id), NULL,
			     &thread_write, writes)) != 0)
  {
    pack_return_values(reads, 0, p_rtn, THREAD_ERROR,
		       "write thread creation failed", 0.0, __FILE__,__LINE__);
    pack_return_values(writes, 0, p_rtn, THREAD_ERROR,
		       "write thread creation failed", 0.0, __FILE__,__LINE__);
    return;
  }
  if((p_rtn = pthread_create(&(reads->thread_id), NULL,
			     &thread_read, reads)) != 0)
  {
    /* Don't let this thread continue on forever. */
    thread_collect(writes->thread_id, get_fsync_waittime(writes));

    pack_return_values(reads, 0, p_rtn, THREAD_ERROR,
		       "monitor thread creation failed", 0.0,
		       __FILE__, __LINE__);
    pack_return_values(writes, 0, p_rtn, THREAD_ERROR,
		       "monitor thread creation failed", 0.0,
		       __FILE__, __LINE__);
    return;
  }
  if((p_rtn = pthread_create(&monitor_tid, NULL, &thread_monitor,
			     &monitor_info)) != 0)
  {
    /* Don't let these threads continue on forever. */
    thread_collect(writes->thread_id, get_fsync_waittime(writes));
    thread_collect(reads->thread_id, get_fsync_waittime(reads));

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
    thread_collect(writes->thread_id, get_fsync_waittime(writes));
    thread_collect(reads->thread_id, get_fsync_waittime(reads));
    thread_collect(monitor_tid, get_fsync_waittime(writes));

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
  while(!reads->done || !writes->done)
  {
  wait_for_condition:

    /* wait until the condition variable is set and we have the mutex */
    /* Waiting indefinatly could be dangerous. */

    if((p_rtn = pthread_cond_timedwait(&done_cond, &done_mutex,
				       &cond_wait_ts)) != 0)
    {
      /* If the wait was interupted, resume. */
      if(p_rtn == EINTR)
	goto wait_for_condition;

      /* Don't let these threads continue on forever. */
      thread_collect(writes->thread_id, get_fsync_waittime(writes));
      thread_collect(reads->thread_id, get_fsync_waittime(reads));
      thread_collect(monitor_tid, get_fsync_waittime(writes));
      
      pack_return_values(reads, 0, p_rtn, THREAD_ERROR,
			 "waiting for condition failed", 0.0,
			 __FILE__, __LINE__);
      pack_return_values(writes, 0, p_rtn, THREAD_ERROR,
			 "waiting for condition failed", 0.0,
			 __FILE__, __LINE__);
      return;
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
	  thread_collect(writes->thread_id, get_fsync_waittime(writes));
	  thread_collect(monitor_tid, get_fsync_waittime(writes));
	  
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
	/*fprintf(stderr,
		"Read thread exited with error(%d) '%s' from %s line %d.\n",
		reads->errno_val, strerror(reads->errno_val),
		reads->filename, reads->line);*/

	/* Signal the other thread there was an error. We need to lock the
	   mutex associated with the next bin to be used by the other thread.
	   Since, we don't know which one, get them all. */
	for(i = 0; i < array_size; i++)
	    pthread_mutex_trylock(&(buffer_lock[i]));
	pthread_cond_signal(&next_cond);
	for(i = 0; i < array_size; i++)
	    pthread_mutex_unlock(&(buffer_lock[i]));
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
	  thread_collect(reads->thread_id, get_fsync_waittime(reads));
	  thread_collect(monitor_tid, get_fsync_waittime(writes));
	  
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
	/*fprintf(stderr,
		"Write thread exited with error(%d) '%s' from %s line %d.\n",
		writes->errno_val, strerror(writes->errno_val),
		writes->filename, writes->line);*/

	/* Signal the other thread there was an error. We need to lock the
	   mutex associated with the next bin to be used by the other thread.
	   Since, we don't know which one, get them all.*/
	for(i = 0; i < array_size; i++)
	  pthread_mutex_trylock(&(buffer_lock[i]));
	pthread_cond_signal(&next_cond);
	for(i = 0; i < array_size; i++)
	  pthread_mutex_unlock(&(buffer_lock[i]));
      }
      writes->done = -1; /* Set to non-positive and non-zero value. */
    }
  }
  pthread_mutex_unlock(&done_mutex);

  /* Don't let this thread continue on forever. */
  thread_collect(monitor_tid, get_fsync_waittime(writes));

  /* Print out an error message.  This information currently is not returned
     to encp.py. */
  if(reads->exit_status)
  {
    fprintf(stderr, "Low-level read transfer failure: [Errno %d] %s: \n"
	    "\terror type: %d  filename: %s  line: %d\n\tHigher "
	    "encp levels will process this error and retry if possible.\n",
	    reads->errno_val, strerror(reads->errno_val),
	    reads->exit_status, reads->filename, reads->line);
    fflush(stderr);
  }
  if(writes->exit_status)
  {
    fprintf(stderr, "Low-level write transfer failure: [Errno %d] %s: \n"
	    "\terror type: %d  filename: %s  line: %d\n\tHigher "
	    "encp levels will process this error and retry if possible.\n",
	    writes->errno_val, strerror(writes->errno_val),
	    writes->exit_status, writes->filename, writes->line);
    fflush(stderr);
  }

  /*free the address space, this should only be done here if an error occured*/
  if(reads->mmap_ptr != MAP_FAILED)
    munmap(reads->mmap_ptr, reads->mmap_len);
  if(writes->mmap_ptr != MAP_FAILED)
    munmap(writes->mmap_ptr, writes->mmap_len);
  
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
  sigemptyset(&sigs_to_block);
  sigaddset(&sigs_to_block, SIGALRM);
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
    memcpy(&start_read, &(read_info->start_transfer_function),
	   sizeof(struct timeval));
    memcpy(&start_write, &(write_info->start_transfer_function),
	   sizeof(struct timeval));

    if(pthread_mutex_unlock(&monitor_mutex))
      pthread_exit(NULL);
    
    while(1)
    {
    go_to_sleep:
      
      pthread_testcancel(); /* Don't sleep if main thread is waiting. */

      /* Wait for the amount of time that it would take to transfer the buffer
	 at 0.5 MB/S. */
      if(nanosleep(&sleep_time, NULL) < 0)
      {
	pthread_testcancel(); /* Don't sleep if main thread is waiting. */

	/* If the nanosleep was interupted we want to keep going. */
	if(errno == EINTR)
	  goto go_to_sleep;
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
      /* Setting this to -1 tells the main thread to ignore this thread. */
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
  int bin = 0;                  /* The current bin (bucket) to use. */
  unsigned int crc_ui = 0;      /* Calculated checksum. */
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

  /* Block this signal.  Only the main thread should use/recieve it. */
  sigemptyset(&sigs_to_block);
  sigaddset(&sigs_to_block, SIGALRM);
  if(sigprocmask(SIG_BLOCK, &sigs_to_block, NULL) < 0)
  {
    pack_return_values(info, 0, errno, READ_ERROR, "sigprocmask failed", 0.0,
		       __FILE__, __LINE__);
    return NULL;
  }

  /* Initialize the time variables. */

  /* Initialize the running time incase of early failure. */
  memset(&start_time, 0, sizeof(struct timeval));
  memset(&end_time, 0, sizeof(struct timeval));
  /* Initialize the running time incase of early failure. */
  gettimeofday(&start_total, NULL);
  memcpy(&end_total, &start_total, sizeof(struct timeval));
  /* Initialize the thread's start time usage. */
  errno = 0;
  if(getrusage(RUSAGE_SELF, &start_usage))
  {
    pack_return_values(info, 0, errno, TIME_ERROR, "getrusage failed", 0.0,
		       __FILE__, __LINE__);
    return NULL;
  }
  
  /* Determine if the file descriptor supports fsync(). */
  errno = 0;
  if(fstat(read_info->fd, &file_info))
  {
    pack_return_values(info, 0, errno, FILE_ERROR, "fstat failed", 0.0,
		       __FILE__, __LINE__);
    return NULL;
  }

  while(read_info->bytes > 0)
  {
    /* If the mmapped memory segment is finished, get the next. */
    if(reinit_mmap_io(read_info))
      return NULL;

    /* If the other thread is slow, wait for it. */
    if(thread_wait(bin, read_info))
      return NULL;

    /* Determine the number of bytes to transfer during this inner loop. */
    bytes_remaining = min(3, (unsigned long long) read_info->bytes,
			  (unsigned long long) read_info->block_size,
			  (unsigned long long) read_info->mmap_left);
    /* Set this to zero. */
    bytes_transfered = 0;

    while(bytes_remaining > 0)
    {
      /* Record the time to start waiting for the read to occur. */
      if(gettimeofday(&start_time, NULL))
      {
	pack_return_values(read_info, 0, errno, TIME_ERROR,
			   "gettimeofday failed", 0.0, __FILE__, __LINE__);
	return NULL;
      }

      /* Handle calling select to wait on the descriptor. */
      if(do_select(info))
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
      if(gettimeofday(&(read_info->start_transfer_function), NULL))
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
			bytes_remaining, info);
      }
      else
      {
	/* Does double duty in that it also does the direct io read. */
	sts = posix_read(
	           (buffer + (bin * read_info->block_size) + bytes_transfered),
		   bytes_remaining, info);
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
      gettimeofday(&end_time, NULL);
      /* Calculate wait time. */
      transfer_time += elapsed_time(&start_time, &end_time);

      /* Calculate the crc (if applicable). */
      switch (read_info->crc_flag)
      {
      case 0:  
	break;
      case 1:  
	crc_ui = adler32(crc_ui,
	     (buffer + (bin * read_info->block_size) + bytes_transfered), sts);
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
    bin = (bin + 1) % read_info->array_size;
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
  if(finish_mmap(info))
    return NULL;

  /* Get total end time. */
  if(gettimeofday(&end_total, NULL))
  {
    pack_return_values(read_info, 0, errno, TIME_ERROR,
		       "gettimeofday failed", 0.0, __FILE__, __LINE__);
    return NULL;
  }
  /* Get the thread's time usage. */
  errno = 0;
  if(getrusage(RUSAGE_SELF, &end_usage))
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

  pack_return_values(info, read_info->crc_ui, 0, 0, "",
		     corrected_time, NULL, 0);
  return NULL;
}


static void* thread_write(void *info)
{
  struct transfer *write_info = (struct transfer*)info; /* dereference */
  size_t bytes_remaining;       /* Number of bytes to move in one loop. */
  size_t bytes_transfered;      /* Bytes left to transfer in a sub loop. */
  int sts = 0;                  /* Return value from various C system calls. */
  int bin = 0;                  /* The current bin (bucket) to use. */
  unsigned long crc_ui = 0;     /* Calculated checksum. */
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

  /* Block this signal.  Only the main thread should use/recieve it. */
  sigemptyset(&sigs_to_block);
  sigaddset(&sigs_to_block, SIGALRM);
  if(sigprocmask(SIG_BLOCK, &sigs_to_block, NULL) < 0)
  {
    pack_return_values(info, 0, errno, READ_ERROR, "sigprocmask failed", 0.0,
		       __FILE__, __LINE__);
    return NULL;
  }
  
  /* Initialize the time variables. */

  /* Initialize the running time incase of early failure. */
  memset(&start_time, 0, sizeof(struct timeval));
  memset(&end_time, 0, sizeof(struct timeval));
  /* Initialize the running time incase of early failure. */
  gettimeofday(&start_total, NULL);
  memcpy(&end_total, &start_total, sizeof(struct timeval));
  /* Get the thread's start time usage. */
  if(getrusage(RUSAGE_SELF, &start_usage))
  {
    pack_return_values(info, 0, errno, TIME_ERROR, "getrusage failed", 0.0,
		       __FILE__, __LINE__);
    return NULL;
  }

  /* Get stat info. */
  errno = 0;
  if(fstat(write_info->fd, &file_info) < 0)
  {
    pack_return_values(info, 0, errno, FILE_ERROR,
		       "fstat failed", 0.0, __FILE__, __LINE__);
    return NULL;
  }

  while(write_info->bytes > 0)
  {
    /* If the mmapped memory segment is finished, get the next. */
    if(reinit_mmap_io(info))
      return NULL;

    /* If the other thread is slow, wait for it. */
    if(thread_wait(bin, write_info))
      return NULL;

    /* Determine the number of bytes to transfer during this inner loop. */
    bytes_remaining = stored[bin];
    /* Set this to zero. */
    bytes_transfered = 0;

    while(bytes_remaining > 0)
    {
      /* Record the time to start waiting for the read to occur. */
      if(gettimeofday(&start_time, NULL))
      {
	pack_return_values(write_info, 0, errno, TIME_ERROR,
			   "gettimeofday failed", 0.0, __FILE__, __LINE__);
	return NULL;
      }

      /* Handle calling select to wait on the descriptor. */
      if(do_select(info))
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
      if(gettimeofday(&(write_info->start_transfer_function), NULL))
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
		  bytes_remaining, info);
      }
      else
      {
	/* Does double duty in that it also does the direct io read. */
	sts = posix_write(
		  (buffer + (bin * write_info->block_size) + bytes_transfered),
		  bytes_remaining, info);
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
      if(gettimeofday(&end_time, NULL))
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
	     (buffer + (bin * write_info->block_size) + bytes_transfered),sts);
	/*to cause intentional crc errors, use the following line instead*/
	/*crc_ui=adler32(crc_ui, (buffer), sts);*/
	write_info->crc_ui = crc_ui;
	break;
      default:
	crc_ui=0;
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
    bin = (bin + 1) % write_info->array_size;
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
  if(finish_mmap(write_info))
    return NULL;

  /* Get total end time. */
  if(gettimeofday(&end_total, NULL))
  {
    pack_return_values(write_info, 0, errno, TIME_ERROR,
		       "gettimeofday failed", 0.0, __FILE__, __LINE__);
    return NULL;
  }
  /* Get the thread's time usage. */
  errno = 0;
  if(getrusage(RUSAGE_SELF, &end_usage))
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

void do_read_write(struct transfer *read_info, struct transfer *write_info)
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
  memset(profile_data, 0, sizeof(profile_data));
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

  errno = 0;
  if((buffer = memalign(sysconf(_SC_PAGESIZE), read_info->block_size)) == NULL)
  {
    pack_return_values(read_info, 0, errno, MEMORY_ERROR, "memalign failed",
		       0.0, __FILE__, __LINE__);
    pack_return_values(write_info, 0, errno, MEMORY_ERROR, "memalign failed",
		       0.0, __FILE__, __LINE__);
    return;
  }
#ifdef DEBUG
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
  memcpy(&end_time, &start_time, sizeof(struct timeval));

  while(read_info->bytes > 0 && write_info->bytes > 0)
  {
    /* Since, either one could use mmap io, this needs to be done on both
       every time. */
    if(reinit_mmap_io(read_info))
      return;
    if(reinit_mmap_io(write_info))
      return;

    /* Number of bytes remaining for this loop. */
    bytes_remaining = min(3, (unsigned long long)read_info->bytes,
			  (unsigned long long)read_info->block_size,
			  (unsigned long long)read_info->mmap_left);
    /* Set this to zero. */
    bytes_transfered = 0;

    while(bytes_remaining > 0)
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
      read_info->bytes -= sts;

#ifdef DEBUG
      *stored = bytes_transfered;
      print_status(stderr, bytes_transfered, bytes_remaining, read_info);
#endif /*DEBUG*/
    }

    /* Initialize the write loop variables. */
    bytes_remaining = bytes_transfered;
    bytes_transfered = 0;

    while (bytes_remaining > 0)
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
	sts = mmap_write(buffer, bytes_remaining, write_info);
      }
      else
      {
	/* Does double duty in that it also does the direct io read. */
	sts = posix_write((buffer + bytes_transfered),
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
	crc_ui=adler32(crc_ui,(void*)((int)buffer+(int)bytes_transfered),sts);
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
      write_info->bytes -= sts;

#ifdef DEBUG
      *stored = 0;
      write_info->crc_ui = crc_ui;
      print_status(stderr, bytes_transfered, bytes_remaining, write_info);
#endif /*DEBUG*/
    }
  }
  /* Sync the data to disk and other 'completion' steps. */
  if(finish_write(write_info))
    return;
  if(finish_mmap(read_info))
    return;
  if(finish_mmap(write_info))
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
#endif

#ifdef PROFILE
  print_profile(profile_data, profile_count);
#endif /*PROFILE*/

  pack_return_values(write_info, crc_ui, 0, 0, "", time_elapsed, NULL, 0);
  pack_return_values(read_info, crc_ui, 0, 0, "", time_elapsed, NULL, 0);
  return;
}
/*#endif */

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
    if (crc_flag>1 || crc_flag<0)
	fprintf(stderr, "fd_xfer - invalid crc param");

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
    memset(&reads, 0, sizeof(reads));
    memset(&writes, 0, sizeof(writes));
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
  int flags = 0;
  int flags_in = 0;
  int flags_out = 0;
  int opt;
  int          verbose = 0;
  int          block_size = 256*1024;
  int          array_size = 3;
  long         mmap_size = 96*1024*1024;
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
  int i;

  
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
	  printf("invalid block size(%s): %s\n", optarg, strerror(errno));
	  return 1;
	}
	break;
      case 'l':  /* mmap length */
	errno = 0;
	if((mmap_size = strtol(optarg, NULL, 0)) == 0)
	{
	  printf("invalid mmap size(%s): %s\n", optarg, strerror(errno));
	  return 1;
	}
	break;
      default:
	printf("Unknown option: -%c\n", optopt);
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
  if(mmap_io_in && direct_io_in)
     flags_in |= O_RDWR | O_DIRECT;
  else if(mmap_io_in)
     flags_in |= O_RDWR;
  else if(direct_io_in)
     flags_in |= O_RDONLY | O_DIRECT;
  else
     flags_in |= O_RDONLY;

  /* Determine the flags for the output file descriptor. */
  if(mmap_io_out && direct_io_out)
     flags_out |= O_RDWR | O_DIRECT | O_CREAT | O_TRUNC;
  else if(mmap_io_out)
     flags_out |= O_RDWR | O_CREAT | O_TRUNC;
  else if(direct_io_out)
     flags_out |= O_WRONLY | O_DIRECT | O_CREAT | O_TRUNC;
  else
     flags_out |= O_WRONLY | O_CREAT | O_TRUNC;

  /* Check the number of arguments from the command line. */
  if(argc < 3)
  {
    printf("Usage: %s [-edmtva:b:l:] <source_file> [-dm] <dest_file>\n",
	    argv[0]);
    return 1;
  }

  if(verbose)
  {
     printf("Threaded: %s\n", ON_OFF(threaded_transfer));
     printf("Ecrc: %s\n", ON_OFF(ecrc));
     printf("Block size: %d\n", block_size);
     printf("Array size: %d\n", array_size);
     printf("Mmap size: %d\n", mmap_size);
     printf("Direct i/o in: %s\n", ON_OFF(direct_io_in));
     printf("Mmap i/o in: %s\n", ON_OFF(mmap_io_in));
     printf("Direct i/o out: %s\n", ON_OFF(direct_io_out));
     printf("Mmap i/o out: %s\n", ON_OFF(mmap_io_out));
  }

  /* Check the input file. */
  if(argv[first_file_optind] == NULL)
  {
     printf("input file not specified.\n");
     return 1;
  }
  errno = 0;
  if(stat(argv[first_file_optind], &file_info) < 0)
  {
     printf("input stat(%s): %s\n", argv[first_file_optind], strerror(errno));
     return 1;
  }
  errno = 0;
  if(realpath(argv[first_file_optind], abspath) == NULL)
  {
     printf("input file(%s): %s\n", argv[first_file_optind], strerror(errno));
     return 1;
  }
  errno = 0;
  if(!S_ISREG(file_info.st_mode) && (strcmp(abspath, "/dev/zero") != 0))
  {
     printf("input file %s is not a regular file\n", abspath);
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
    printf("input open(%s): %s\n", abspath, strerror(errno));
    return 1;
  }

  if(verbose)
  {
     printf("The input file: %s\n", abspath);
  }
  
  /* Check the output file. */
  if(argv[second_file_optind] == NULL)
  {
     printf("output file not specified.\n");
     return 1;
  }

  /* Open the output file. */
  errno = 0;
  if((fd_out = open(argv[second_file_optind], flags_out,
		    S_IRUSR | S_IWUSR | S_IRGRP)) < 0)
  {
     printf("output open(%s): %s\n",
	    argv[second_file_optind], strerror(errno));
     return 1;
  }
  errno = 0;
  if(realpath(argv[second_file_optind], abspath) == NULL)
  {
     printf("output file(%s): %s\n",
	    argv[second_file_optind], strerror(errno));
     return 1;
  }

  /* Check the output file. */
  errno = 0;
  if(stat(abspath, &file_info) < 0)
  {
     printf("output stat(%s): %s\n", abspath, strerror(errno));
     return 1;
  }
  errno = 0;
  if(!S_ISREG(file_info.st_mode) && (strcmp(abspath, "/dev/null") != 0))
  {
     printf("output file %s is not a regular file\n", abspath);
     return 1;
  }
  
  if(verbose)
  {
     printf("The output file: %s\n", abspath);
  }
  
  /*Place the values into the struct.  Some compilers complained when this
    information was placed into the struct inline at initalization.  So it
    was moved here.*/
  memset(&reads, 0, sizeof(reads));
  memset(&writes, 0, sizeof(writes));
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
  reads.direct_io = direct_io_in;
  reads.mmap_io = mmap_io_in;
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
  writes.direct_io = direct_io_out;
  writes.mmap_io = mmap_io_out;

  /* Do the transfer test. */
  errno = 0;
  if(threaded_transfer)
     do_read_write_threaded(&reads, &writes);
  else
    do_read_write(&reads, &writes);

  if (writes.exit_status != 0 && writes.errno_val != ECANCELED)
  {
     printf("Write error [ errno %d ]: %s: %s  File: %s  Line: %d\n",
	    writes.errno_val, strerror(writes.errno_val), writes.msg,
	    writes.filename, writes.line);
     return 1;
  }
  else if (reads.exit_status != 0)
  {
     printf("Read error [ errno %d ]: %s: %s:  File: %s  Line: %d\n",
	    reads.errno_val, strerror(reads.errno_val), reads.msg,
	    reads.filename, reads.line);
     return 1;
  }
  else
  {
     if(verbose)
	printf("Read time: %f  Write time: %f  Size: %lld  CRC: %u\n",
	       reads.transfer_time, writes.transfer_time, size, writes.crc_ui);
     printf("Read rate: %f MB/s Write rate: %f MB/s\n",
	    size/(1024*1024)/reads.transfer_time,
	    size/(1024*1024)/writes.transfer_time);
  }

  if(ecrc)
  {
     errno = 0;
     crc_ui = ecrc_readback(writes.fd);
     if((crc_ui == 0) && (errno != 0))
	printf("Error performing ecrc readback check: %s\n", strerror(errno));
     else
	if(crc_ui != writes.crc_ui)
	   printf("CRC mismatch: original: %u  readback: %u\n",
		  writes.crc_ui, crc_ui);
  }
  
  return 0;
}

#endif
