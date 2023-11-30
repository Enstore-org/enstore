/* EXfer.c - Low level data transfer C modules for encp. */

/* $Id$ */

/* A little hack for Linux so direct i/o will work. */
#if defined(__linux__) && !defined(_GNU_SOURCE)
#  define _GNU_SOURCE
#endif

/* A little hack for OSF1 to use POSIX sockets (AKA define socklen_t). */
#if defined(__osf__) && !defined(_POSIX_PII_SOCKET)
#  define _POSIX_PII_SOCKET
#endif

/* A little hack for SunOS to use the BSD FIONREAD ioctl(). */
#if defined(__sun) && !defined(BSD_COMP)
#  define BSD_COMP
#endif

/* A little hack for IRIX to use its F_DIOINFO ioctl() (for DIRECT I/O).
 * Some versions of IRIX standards.h header file do not define _SGIAPI
 * correctly. */
#if defined(__sgi)
#  include <standards.h>
#  undef _SGIAPI
#  define _SGIAPI 1
/* The redefining of the xopen constants was recommended by the python
 * developers. */
#  undef _NO_XOPEN4
#  define _NO_XOPEN4 1
#  undef _NO_XOPEN5
#  define _NO_XOPEN5 1
#endif

/* Macros for Large File Summit (LFS) conformance. */
#define _FILE_OFFSET_BITS 64
#define _LARGEFILE_SOURCE 1

#ifndef STAND_ALONE
#  include <Python.h>
#endif
#include <sys/stat.h>
#include <sys/types.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/wait.h>
#include <errno.h>
#ifndef __APPLE__
#  include <malloc.h>
#endif /* __APPLE__ */
/*#include <alloca.h>*/
#include <sys/time.h>
#include <signal.h>
#include <setjmp.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <sys/ioctl.h>
#include <stdlib.h>
#include <sys/resource.h>
#include <pthread.h>
#include <sys/mman.h>
#include <stdarg.h>
#include <string.h>
#include <limits.h>
#include <libgen.h>
#if __STDC__ && __STDC_VERSION__ >= 199901L
#  include <stdbool.h>  /* C99 implimentations have these. */
#  include <stdint.h>
#endif /* __STDC_VERSION__ */
#ifndef __osf__
#  include <inttypes.h> /* Must handle osf definitions later.  Currently, only
			 * intptr_t and uintptr_t are used.  However, when more
			 * distributions are fully C99 compliant, even more
			 * definitions from this file should be used. */
#endif /* __osf__ */
#ifdef __sgi
#  include <sys/sysmp.h>
#endif
#ifdef STAND_ALONE
#  if __linux__
#    include <sys/mount.h>
#  endif /* __linux__ */
#endif /* STAND_ALONE */


#ifdef HAVE_XFS_XQM_H
   #include <xfs/xqm.h>
#endif
#if defined(HAVE_SYS_FS_UFS_QUOTA_H)
   /* Only SunOS can get here? */
   #include <sys/fs/ufs_quota.h>
#endif
#if defined(HAVE_SYS_QUOTA_H)
   #include <sys/quota.h>
   #include <linux/version.h>
   /* for kernels 2 and 3 */
   #if defined(_LINUX_QUOTA_VERSION) && _LINUX_QUOTA_VERSION-0 > 1
      /* There is some incompatibility between Linux quota version 1 and 2. */
      #define dqb_curblocks dqb_curspace
   #endif
   /* for kernels 4+  */
   #if defined(LINUX_VERSION_MAJOR) && LINUX_VERSION_MAJOR-0 > 3
      #define dqb_curblocks dqb_curspace
   #endif
#endif
#ifdef __APPLE__
   #include <sys/quota.h>
   /* On apple, the quotas are done for bytes, not blocks like on Linux,
    * SunOS, IRIX, etc. */
   #define dqb_curblocks dqb_curbytes
   #define dqb_fhardlimit dqb_ihardlimit
   #define dqb_fsoftlimit dqb_isoftlimit
   #define dqb_curfiles dqb_curinodes
   #define dqb_btimelimit dqb_btime
   #define dqb_ftimelimit dqb_itime
#endif
/***************************************************************************
 * constants and macros
 **************************************************************************/

/* OSF1 V4 defines these macros incorrectly. */
#ifdef __osf__
# ifdef MAP_FAILED
#  undef MAP_FAILED
#  define MAP_FAILED ((void*)-1L)
# endif

# ifdef _POSIX_FSYNC
#  undef _POSIX_FSYNC
#  define _POSIX_FSYNC 199506L
# endif

# ifdef _POSIX_SYNCHRONIZED_IO
#  undef _POSIX_SYNCHRONIZED_IO
#  define _POSIX_SYNCHRONIZED_IO 199506L
# endif
#endif /*__osf__*/

/* This is the largest size a size_t type can hold.  It is defined in C99
 * (stdint.h) but not all implimintations define it yet.  This is the
 * largest size that can fit into into a signed 32bit integer which should
 * suffice for our purposes in most cases. */
#ifndef SIZE_MAX
#define SIZE_MAX 2147483647L
#endif /* SIZE_MAX */

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
 * best if the block size is changed on the encp command line to something
 * small (i.e. less than the page size). */
/*#define PROFILE*/
#ifdef PROFILE
#define PROFILE_COUNT 25000
#endif

#define MILLION 1000000.0

/* Macro to convert struct timeval into double. */
#define extract_time(t) ((double)(t->tv_sec+(t->tv_usec/MILLION)))

/* Define memory mapped i/o advise constants on systems without them. */
/*#ifndef MADV_SEQUENTIAL
#define MADV_SEQUENTIAL -1
#endif
#ifndef MADV_WILLNEED
#define MADV_WILLNEED -1
#endif
#ifndef MADV_DONTNEED
#define MADV_DONTNEED -1
#endif*/

/* Define the NAME_MAX constant if Solaris does not define it. */
#if defined(__sun) && !defined(NAME_MAX)
/* In reality we should use pathconf(path, _PC_NAME_MAX) to get this value.
 * Page 48 of Advanced Programming in the Unix Environment, Second Edition,
 * states that if we assume the UFS filesystem, then 255 is fine. */
#define NAME_MAX 255
#endif

/* Set the size of readback chunks to 1MB. */
#define ECRC_READBACK_BUFFER_SIZE 1048576

/* This is the minimum rate that must be maintained within a single call to
 * read()/write().  Currently this is 1/2 MB/s. */
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
const char filesize_mmap_io_error[] =
  "Writing to memory mapped i/o requires the filesize be known in advance.  "
  "Reverting to POSIX based i/o.\n";


const char no_mmap_threaded_implimentation[] =
  "Multithreaded memory mapped i/o to memory mapped i/o is not supported.  "
  "Reverting to single threaded implemenation.\n";

const char no_mandatory_file_locking[] =
   "Mandatory file locking not supported.  Reverting to advisory file locking.\n";
const char unknown_mandatory_file_locking[] =
   "Unable to determine if mandatory file locking available.\n";

#endif /*DEBUG_REVERT*/

#define EMPTY_MEMORY     0U
#define MMAP_MEMORY      1U
#define MALLOC_MEMORY    2U

#define ZERO ((size_t)0ULL) /* For filling struct buffer "stored" values
			     * in a 32bit/64bit safe way. */

/***************************************************************************
 * definitions
 **************************************************************************/

#if (! __STDC__) || (__STDC_VERSION__ < 199901L)
typedef unsigned char bool; /* Only define this for pre-C99 implimentations. */
#endif

#ifdef __osf__
/*
 * All supported OSes except osf1v40d have a <inttypes.h> file.  Most were
 * written to various drafts of the POSIX/XOPEN 2001 standard.  Newer OSF1
 * machines have this file (although it is missing mandatory things).  The
 * main thing that is needed are the following two integer types for
 * pointers.
 */
typedef signed long intptr_t;
typedef unsigned long uintptr_t;
#endif

#if (defined(__mips) || defined(__sun))
/*
 * Older IRIX 6.5 boxes to not define socklen_t.  Newer ones do and also define
 * the macro _SOCKLEN_T that we can use to determine if socklen_t is defined
 * already or we need to do so here.
 *
 * The same goes for SunOS too.  Newer ones define it (2.8), older ones do
 * (2.6) not.
 */
#if defined(STAND_ALONE) && !defined(_SOCKLEN_T)
/*
 * Only worry about this for the STAND_ALONE executable.  The Python.h include
 * takes care of this for EXfer.so.
 */
#define _SOCKLEN_T
typedef int socklen_t;
#endif /* STAND_ALONE && !_SOCKLEN_T */
#endif /* __mips || __sun */

/* This is the struct that holds all the information about one direction
 * of a transfer. */
struct transfer
{
  int fd;                 /*file descriptor*/

  off_t size;             /*size in bytes*/
  off_t bytes_to_go;      /*bytes left to transfer*/
  off_t bytes_transfered; /*bytes transfered*/
  size_t block_size;      /*size of block*/
  size_t array_size;      /*number of buffers to use*/
  size_t mmap_size;       /*mmap address space segment lengths*/

  off_t fsync_threshold;  /* Number of bytes to wait between fsync()s.
			   *     It is the max of block_size, mmap_size and
			   *	 1% of the filesize. */
  off_t last_fsync;       /* Number of bytes done though last fsync(). */

  struct timeval timeout; /*time to wait for data to be ready*/
  struct timeval start_transfer_function; /*time last read/write was started.*/
  double transfer_time;   /*time spent transfering data*/

  bool crc_flag;          /*crc flag - 0 or 1*/
  unsigned int crc_ui;    /*checksum*/

  int transfer_direction; /*positive means write, negative means read*/

  bool direct_io;         /*is true if using direct io*/
  bool mmap_io;           /*is true if using memory mapped io*/
  bool synchronous_io;    /*is true if using synchronous io*/
  bool d_synchronous_io;  /*is true if using synchronous io*/
  bool r_synchronous_io;  /*is true if using synchronous io*/
  bool threaded;          /*is true if using threaded implementation*/
  bool advisory_locking;  /*is true if advisory locking should be used*/
  bool mandatory_locking; /*is true if manditory locking should be used*/

  bool other_mmap_io;     /*is true if other direction using memory mapped io*/
  bool other_fd;          /*contians other direction fd*/

  pthread_t thread_id;    /*the thread id (if doing MT transfer)*/
#ifdef __sgi
  int cpu_affinity;       /*NICs are tied to CPUs on IRIX nodes*/
#endif
  short int done;         /* Is zero initially, set to one when the (transfer)
			   * thread exits and the main thread sets it to -1
			   * when it has collected the thread. */
  short int other_thread_done; /* Is zero initially, set to one when the
				* main thread collects the other thread. */

  int exit_status;        /*error status*/
  int errno_val;          /*errno of any errors (zero otherwise)*/
  char* msg;              /*additional error message*/
  int line;               /*line number where error occured*/
  char* filename;         /*filename where error occured*/
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

/* Pointers to a set of variables for managing the in memory buffering. */
struct buffer
{
size_t *stored;               /*pointer to array of bytes in each bin*/
char **buffer;                /*pointer to array of buffer bins*/
size_t *buffer_type;          /*type of items in buffer array*/
pthread_mutex_t *buffer_lock; /*pointer to array of bin mutex locks*/
};

/* Various locks and related stuff. */
struct locks
{
pthread_mutex_t done_mutex;   /*main thread waits for an exited thread*/
pthread_mutex_t monitor_mutex;/*used to sync the monitoring*/
pthread_cond_t done_cond;     /*main thread waits for an exited thread*/
pthread_cond_t next_cond;     /*used to signal peer thread to continue*/
#ifdef DEBUG
pthread_mutex_t print_lock;   /*order debugging output*/
#endif
};

/* Two pointers for use by the monitor thread. */
struct t_monitor
{
  struct transfer *read_info;  /* Pointer to the read direction struct. */
  struct transfer *write_info; /* Pointer to the write direction struct. */
  struct locks *thread_locks;  /* Pointer to pthread locking objects. */
  struct buffer *mem_buff;     /* Pointer to memory buffer objects. */
};

/***************************************************************************
 * prototypes
 **************************************************************************/

/* checksumming is now being done here, instead of calling another module,
 *  in order to save a strcpy  -  cgw 19990428 */
unsigned int adler32(unsigned int, char *, unsigned int);

#ifndef STAND_ALONE
void initEXfer(void);
static PyObject * raise_exception(char *msg);
static PyObject * EXfd_xfer(PyObject *self, PyObject *args);
static PyObject * EXfd_ecrc(PyObject *self, PyObject *args);
static PyObject * EXfd_quotas(PyObject *self, PyObject *args);
#endif

/*
 * do_read_write_threaded() and do_read_write():
 * These functions take two parameters to the read and write transfer structs.
 * The dfference is that the first one will run each half of the transfer
 * in their own thread, while the later runs everything in a single thread.
 */
static void do_read_write_threaded(struct transfer *reads,
				   struct transfer *writes);
static void do_read_write(struct transfer *reads, struct transfer *writes);

/*
 * pack_return_values():
 * The first parameter is the transfer struct for the direction of the
 * transfer that is exiting.  Values in this struct will be modified with
 * the values from the other parameters.
 *
 * For the non-threaded version it is expected that the calling function
 * will call this function twice, one for each direction.  If there is
 * no error, dummy values should be specified for errno_val (0),
 * exit_status (0), msg (NULL), transfer_time (0.0), filename (NULL) and
 * line (0).  I there is an error, then crc_ui is set to zero.
 */
static struct transfer* pack_return_values(struct transfer *info,
					   unsigned int crc_ui,
					   int errno_val, int exit_status,
					   char* msg,
					   double transfer_time,
					   char *filename, int line,
					   struct locks *thread_locks);

/*
 * elapsed_time():
 * Return the difference between the two struct timeval{}s as a floating
 * point number in seconds.  The first parameter, start_time, is the older.
 */
static double elapsed_time(struct timeval* start_time,
			   struct timeval* end_time);

/*
 * rusage_elapsed_time():
 * Do the same as elapsed_time(), except that the structs passed in are
 * struct rusage{}.  These structs do contain a struct timeval{}.
 */
static double rusage_elapsed_time(struct rusage *sru, struct rusage *eru);

/*
 * get_fsync_threshold():
 * Returnthe number of bytes that need to be transfered before the next
 * syncing the file to disk (write to file only).
 */
static long long get_fsync_threshold(struct transfer *info);

/*
 * get_fsync_waittime():
 * Uses the get_fsync_threshold() function for calculating the time to wait for
 * another thread to exit.  It is possible that the file is residing in the
 * buffer cache and the kernel has not started to flush the file out to disk.
 * In this senerio the longest operation the the waiting thread would need
 * to wait is the time of an entire sync of the part of the file still
 * in the file buffer cache.  If the un-stopping thread takes longer than
 * this return value in seconds to complete, there is likely a problem.
 * Most often this has turned out that the un-stopping thread was stuck in
 * the kernel (D state).
 */
static unsigned int get_fsync_waittime(struct transfer *info);

/*
 * align_to_page() and align_to_size():
 * The align_to_size() function takes the value parameter and returns the
 * smallest size that is a multimple of the align parmamater. The
 * align_to_page() function does the same thing except that the alignment
 * amount is the systems page size.  Assumes unsigned values.
 */
static size_t align_to_page(size_t value);
static size_t align_to_size(size_t value, size_t align);

/*
 * max() and min():
 * Return either the maximum or minimum of 2 or 3 items.
 */
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

/*
 * setup_*io():
 * These functions take the struct of one direction of the transfer and
 * will attempt to initialize the struct for use with each type of i/o
 * optimization.  Posix i/o is the default and should be called regardless
 * if the other optimizations are used.  If Mmap i/o was specified, but
 * the underlying filesystem does not support it, it will revert to
 * direct/posix i/o.  If direct i/o is used on a filesystem that does not
 * support it, an error will be returned from EXfer.  The return value
 * is -1 on error and 0 on success.
 */
static int setup_mmap_io(struct transfer *info);
static int setup_direct_io(struct transfer *info);
static int setup_posix_io(struct transfer *info);

/*
 * get_next_segment() and cleanup_segment():
 * The first function will create a new entry in the global buffer.  If
 * mmap io is used then it is memory mapped.  Otherwise it is a page aligned
 * section of memory.
 *
 * The second function, cleanup_segment(), cleans up the memory allocated
 * by get_next_segment().
 *
 * They both work on the buffer bucket named in the parameter "bin".
 * get_next_segment() should only be passed the read direction struct
 * transfer variable.  cleanup_segment() should only be passed the write
 * direction transfer struct.
 */
static void* get_next_segment(int bin, struct buffer *mem_buff,
			      struct transfer *info,
			      struct locks *thread_locks);
static int cleanup_segment(int bin, struct buffer *mem__buff,
			   struct transfer *info,
			   struct locks *thread_locks);

/*
 * get_segments() and cleanup_segments():
 * These function are similar to get_next_segment() and cleanup_segment().
 * However, they are only used in the case when memory mapped io is
 * copied to another memory mapped i/o region.  They are implimented using
 * get_next_segment() and cleanup_segment(). */
static void* get_next_segments(struct buffer *mem_buff,
			       struct transfer *info,
			       struct locks *thread_locks);
static int cleanup_segments(struct buffer *mem_buff,
			    struct transfer *info,
			    struct locks *thread_locks);

/*
 * remove_lock():
 * If the file descriptor passed in the struct still has the file locked,
 * free the lock. */
static int remove_lock(struct transfer *info, struct locks *thread_locks);
/* This version does not call pack_return_values() and is inteaded to
 * only be called from pack_return_values().  The nr stands for
 * Non-Recursive.*/
static int remove_lock_nr(struct transfer *info);

/*
 * finish_write() and finish_read():
 * Performs any extra completion operations.  Mostly this means using the
 * appropriate syncing function for posix, direct or mmapped i/o.
 * The return value is -1 on error and zero on success.
 */
static int finish_write(struct transfer *info, struct locks *thread_locks);
static int finish_read(struct transfer *info, struct locks *thread_locks);

/*
 * do_select():
 * Wait for the FD to become ready for read or write depending on the
 * direction of the transfer the transfer struct parameter specifies.
 * This is really just a shell around select(), since only on FD is used.
 */
static int do_select(struct transfer *info, struct locks *thread_locks);

/*
 * *read() and *write():
 * These funcions are wrappers around the reading and writing functions.
 * The posix versions also perform the direct i/o versions.  The first
 * parameter is a pointer to the base of the buffer array.  The second is
 * the amount of data that this call to the function should worry about.
 * The last parameter is the struct of this half of the transfer.  The
 * return value is the amount of data read/written or -1 for error.
 */
static ssize_t mmap_read(void *dst, size_t bytes_to_transfer,
			 struct transfer *info, struct locks *thread_locks);
static ssize_t mmap_write(void *src, size_t bytes_to_transfer,
			  struct transfer *info, struct locks *thread_locks);
static ssize_t posix_read(void *dst, size_t bytes_to_transfer,
			  struct transfer* info, struct locks *thread_locks);
static ssize_t posix_write(void *src, size_t bytes_to_transfer,
			   struct transfer* info, struct locks *thread_locks);

/*
 * thread_init():
 * Initialize the local mutex locks and condition variables.
 */
static int thread_init(struct transfer *info, struct locks *thread_locks);

/*
 * thread_destroy():
 * Destroy the local mutex locks and condition variables.
 */
static int thread_destroy(struct transfer *info, struct locks *thread_locks);


/*
 * thread_wait():
 * If the other read/write thread is slow, wait for the specified bin to become
 * available.  Return 1 on error and 0 on success.
 */
static int thread_wait(size_t bin, double *thread_wait_time,
		       struct buffer *mem_buff,
		       struct transfer *info, struct locks *thread_locks);

/*
 * thread_signal():
 * Set the bin (or bucket) with index bin to the amount specified by bytes.
 * This function will also 'raise' a condional variable signal to wake
 * up the other read/write thread.  Return 1 on error and 0 on success.
 */
static int thread_signal(size_t bin, size_t bytes, struct buffer *mem_buff,
			 struct transfer *info, struct locks *thread_locks);

/*
 * thread_collect():
 * The first parameter is a thread id as returned by pthread_create().  This
 * thread will be 'canceled'.  In posix talk, canceled is to thread as
 * killed is to process.  The wait_time is the number of seconds to wait
 * for the thread to stop.  If the thread is waiting in the kernel
 * the longest span of time would be returned from get_fsync_waittime().
 * If the thread is still 'alive' after this time it is assumed hung and
 * abandoned.  Return 1 on error and 0 on success.
 */
static int thread_collect(pthread_t tid, unsigned int wait_time);

/*
 * thread_read() and thread_write():
 * These are the functions passed to pthread_create() for performing the
 * read/write portion of the threaded transfer.  The return value is the
 * pointer to a struct transfer{} with the completion items filled in.
 */
static void* thread_read(void *info);
static void* thread_write(void *info);

/*
 * thread_monitor():
 * This is the function that is passed to pthread_create() for the purpose
 * of starting a thread that monitors the read and write thread.  If
 * one of the threads stops/get stuck/hangs this will attempt to cancel
 * remaining threads and return an error from EXfer.
 */
static void* thread_monitor(void *monitor);

/*
 * ecrc_readback():
 * Performs a crc readback test on reads from enstore.  It takes a file
 * descriptor of the output file as parameter.  It then rewindes the file and
 * turns off direct i/o (Both Linux and IRIX do not give reasons for the
 * errors). The file is then read from begining to end and at the same time
 * the crc value is recalculated again. The crc value is returned.  On error,
 * the crc will returned as zero and errno will be set.   Thus, to fully
 * detect an error, set errno to zero first.
 */
static unsigned int ecrc_readback(int fd);

#ifdef Q_GETQUOTA
/*
 * Given a pathname, place in block_device the device on which
 * the file is located.  The arguement bd_len is the length of the
 * character array block_device.  The mount_point and mp_len do the same
 * thing as block_device, but instead for the mount_point of the filesystem.
 *
 * block_device and mount_point should be at least PATH_MAX + 1 in length.
 * Returns 0 on success, -1 on error.  Errno is set from lower system call.
 */
static int get_bd_name(char *name, char *block_device, size_t bd_len,
		       char *mount_point, size_t mp_len);

/*
 * get_quotas():
 *
 * First arguement is a string containing the name of a block device.
 * Examples: /dev/dsk/dks20d125s6
 *           /dev/hda3
 * Second arguement is eiter USER_QUOTA or GROUP_QUOTA.
 * Third arguement is the memory address of a struct dqblk variable where
 * the quota information is returned from.
 *
 * Returns 0 on success, -1 on error.  Errno is set.
 */
int get_quotas(char *block_device, int type, struct dqblk* my_quota);
#endif /* Q_GETQUOTA */

/*
 * is_stored_empty():
 * Returns true if the bin (aka bucket) 'bin' in the 'stored' global variable,
 * is empty.  False if it is full.
 */
static int is_stored_empty(unsigned int bin, struct buffer *mem_buff);

/*
 * buffer_empty() and buffer_full():
 * If all the values in the 'stored' global are zero, buffer_empty() return
 * true; false otherwise.  If all the values in stored are zero,
 * buffer_empty() returns true; false otherwise.
 */
static int buffer_empty(size_t array_size, struct buffer *mem_buff);
static int buffer_full(size_t array_size, struct buffer *mem_buff);

/*
 * sig_alarm():
 * Used by thread_collect() to handle SIGALRM raised when a thread survies
 * a pthread_cancel().
 */
static void sig_alarm(int sig_num);

#ifdef PROFILE
static void update_profile(int whereami, int sts, int sock,
		    struct profile *profile_data, long *profile_count);
static void print_profile(struct profile *profile_data, int profile_count);
#endif /*PROFILE*/
#ifdef DEBUG
static void print_status(FILE *fp, unsigned int bytes_transfered,
			 unsigned int bytes_remaining, struct buffer *mem_buff,
			 struct transfer *info,
                         struct locks *thread_locks);
#endif /*DEBUG*/

/*
 * These invalidate cache function attempt to remove the input file from
 * the file cache.  Different methods work on different OSes.
 *
 * They all take the filename of the input file as parameter. */
#ifdef STAND_ALONE
static int invalidate_cache_posix(char* abspath);
#  ifdef __linux__
static int invalidate_cache_linux(char* abspath);
#  endif /* __linux__ */
#  ifdef __sgi
static int invalidate_cache_irix(char* abspath);
#  endif /* __sgi */
#endif /* STAND_ALONE */

/* Useful for printing socket information to stderr. */
static int print_socket_info(int fd);

/***************************************************************************
 * globals
 **************************************************************************/

/*
 * In general globals variables in a threaded program cause problems.  Those
 * listed here are used between the sets of threads created by
 * do_read_write_threaded().  All variables that are used within a set of
 * threads created in do_read_write_threaded() are themselves created in
 * do_read_write_threaded().
 */

static pthread_mutex_t collect_mutex; /*used to sync the monitoring*/
static sigjmp_buf alarm_join;         /*handle detection of hung threads*/


#ifndef STAND_ALONE

/*
 * The following python globals are used by python for interfacing this
 * C code to appear as a python module to the python interpreter.
 */

static PyObject *EXErrObject;

static char EXfer_Doc[] =  "EXfer is a module which Xfers data";

static char EXfd_xfer_Doc[] = "\
fd_xfer(fr_fd, to_fd, no_bytes, blk_siz, crc_flag[, crc])";
static char EXfd_ecrc_Doc[] = "\
unsigned int ecrc(crc, &start_addr, memory_size)";
static char EXfd_quotas_Doc[] = "\
unsigned int get_quotas(char *block_device, int type, struct dqblk* my_quota)";

/*  Module Methods table.
 *
 *  There is one entry with four items for for each method in the module
 *
 *  Entry 1 - the method name as used  in python
 *        2 - the c implementation function
 *        3 - flags
 *	  4 - method documentation string
 */

static PyMethodDef EXfer_Methods[] = {
    { "fd_xfer",  EXfd_xfer,  1, EXfd_xfer_Doc},
    { "ecrc", EXfd_ecrc, 1, EXfd_ecrc_Doc},
    { "quotas", EXfd_quotas, 1, EXfd_quotas_Doc},
    { 0, 0}        /* Sentinel */
};

#endif /* ! STAND_ALONE */

/***************************************************************************
 user defined functions
**************************************************************************/

static void* page_aligned_malloc(size_t size)
{
   /* Memory alignment is not very portable yet.  Posix defines the
    * posix_memalign() function.  BSD (long ago) defined the valloc()
    * function and SYSV had memalign(). */

   /* 6-18-2003: MWZ:
    * These are the functions defined for various platforms:
    *
    * FL7.1 and earlier: valloc and memalign (No man pages though.)
    *
    * FL7.3 and later: valloc, memalign and posix_memalign
    *
    * IRIX 6.5: valloc and memalign
    *
    * Solaris 2.6, 2.7, 2.8: valloc and memalign
    *
    * OSF1 v40d: valloc
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
#elif defined ( __osf__ ) || defined ( __APPLE__ )
   return valloc(size);
#else
   return memalign((size_t)sysconf(_SC_PAGESIZE), size);
#endif
}

static void sig_alarm(int sig_num)
{
   if(sig_num != SIGALRM)
      return;  /* Should never happen. */

   /* Return execution to thread_collect(). */
   siglongjmp(alarm_join, 1);
}

static int is_other_thread_done(struct transfer* info,
				struct locks *thread_locks)
{
  int rtn;

  if((pthread_mutex_lock(&(thread_locks->done_mutex))) != 0)
  {
     return -1;
  }

  rtn = info->other_thread_done;

  if((pthread_mutex_unlock(&(thread_locks->done_mutex))) != 0)
  {
     return -1;
  }

  return rtn;
}

/* Return 0 for false, >1 for true, <1 for error. */
static int is_stored_empty(unsigned int bin, struct buffer *mem_buff)
{
  int rtn = 0; /*hold return value*/

  pthread_testcancel(); /* Don't continue if the thread should stop now. */

  /* Determine if the lock for the buffer_lock bin, bin, is ready. */
  if(pthread_mutex_lock(&(mem_buff->buffer_lock)[bin]) != 0)
  {
    return -1; /* If we fail here, we are likely to see it again. */
  }
  if(mem_buff->stored[bin] == (size_t)0ULL)
  {
    rtn = 1;
  }
  if(pthread_mutex_unlock(&(mem_buff->buffer_lock)[bin]) != 0)
  {
    return -1; /* If we fail here, we are likely to see it again. */
  }

  pthread_testcancel(); /* Don't continue if the thread should stop now. */

  return rtn;
}

static int buffer_empty(size_t array_size, struct buffer *mem_buff)
{
  unsigned int i;   /*loop counting*/
  int rtn = -1; /*return*/

  for(i = 0; i < array_size; i++)
  {
    if(!is_stored_empty(i, mem_buff))
    {
      rtn = 0;
      break;
    }
    rtn = 1;
  }

  return rtn;
}

static int buffer_full(size_t array_size, struct buffer *mem_buff)
{
  unsigned int i;   /*loop counting*/
  int rtn = -1; /*return*/

  for(i = 0; i < array_size; i++)
  {
    if(is_stored_empty(i, mem_buff))
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
					   char* filename, int line,
					   struct locks *thread_locks)
{
  if(thread_locks && &(thread_locks->done_mutex))
  {
    pthread_testcancel(); /* Don't continue if the thread should stop now. */

    /* Do not bother with checking return values for errors.  Should the
     * pthread_* functions fail at this point, there is notthing else to
     * do but raise the condition variable and return. */
    pthread_mutex_lock(&(thread_locks->done_mutex));
  }

  retval->crc_ui = crc_ui;               /* Checksum */
  retval->errno_val = errno_val;         /* Errno value if error occured. */
  retval->exit_status = exit_status;     /* Exit status of the thread. */
  retval->msg = message;                 /* Additional error message. */
  retval->transfer_time = transfer_time; /* Duration of the transfer. */
  retval->line = line;                   /* Line number an error occured on. */
  retval->filename = filename;           /* Filename an error occured on. */
  retval->done = 1;                      /* Flag saying transfer half done. */

  remove_lock_nr(retval); /* If necessary remove the file lock. */

  if(thread_locks && &(thread_locks->done_mutex))
  {
     /* Putting the following here is just the lazy thing to do. */
     /* For this code to work this must be executed after setting retval->done
      * to 1 above. */
     pthread_cond_signal(&(thread_locks->done_cond));

     pthread_mutex_unlock(&(thread_locks->done_mutex));

     pthread_testcancel(); /* Don't continue if the thread should stop now. */
  }

  return retval;
}

/* Cconvert double into struct timeval.*/
void build_time(struct timeval* time_to_set, double time)
{
   time_to_set->tv_sec = (time_t)(time);
   time_to_set->tv_usec = (long)((time - (time_t)time) * MILLION);
}

/* Return the time difference between two gettimeofday() calls. */
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
  temp_value = (unsigned long long)((double)info->bytes_to_go / (double)100.0);

  /* Return the largest of these values:
   * 1) One percent of the filesize.
   * 2) The block (aka buffer) size.
   * 3) The memory mapped segment size. */
  return (long long)max3ull((unsigned long long)temp_value,
			    (unsigned long long)info->block_size,
			    (unsigned long long)info->mmap_size);
}

/* Returns the number of seconds to wait for another thread. */
static unsigned int get_fsync_waittime(struct transfer *info)
{
  /* Don't use info->fsync_threshold; it may not be initalized yet. */

  /* Calculate the amount of time to wait for the amount of data transfered
   * between syncs will take assuming a minumum rate requirement. */
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
 * alignment size. */
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
			 unsigned int bytes_remaining, struct buffer *mem_buff,
			 struct transfer *info,
                         struct locks *thread_locks)
{
  unsigned int i;
  char debug_print;
  char direction;

  if(thread_locks && &(thread_locks->print_lock))
  {
     pthread_mutex_lock(&(thread_locks->print_lock));
  }

  /* Print F if entire bin is transfered, P if bin partially transfered. */
  debug_print = (bytes_remaining) ? 'P' : 'F';
  /* Print W if write R if read. */
  direction = (info->transfer_direction > 0) ? 'W' : 'R';

  (void)fprintf(fp, "%c%c bytes: %15llu crc: %10u | ",
	  direction, debug_print,
	  (unsigned long long)info->bytes_transfered, info->crc_ui);

  for(i = 0; i < info->array_size; i++)
  {
     (void)fprintf(fp, " %6u", (unsigned int)mem_buff->stored[i]);
  }
  (void)fprintf(fp, "\n");

  if(thread_locks && &(thread_locks->print_lock))
  {
     pthread_mutex_unlock(&(thread_locks->print_lock));
  }

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

static int print_socket_info(int fd)
{
   struct stat sock_stats;

   /* Make sure this is a socket. */
   if(fstat(fd, &sock_stats) < 0)
   {
      return -1;
   }
   if(!S_ISSOCK(sock_stats.st_mode))
   {
      return -1;
   }

   /* Get any pending socket errors. */
   {
      int socket_error;
      socklen_t socklen;
      char error_message[2048];

      socket_error = 0;
      socklen = sizeof(socket_error);
      if(getsockopt(fd, SOL_SOCKET, SO_ERROR,
		    (void*) (&socket_error), &socklen) < 0)
      {
	 (void) snprintf(error_message, 2047,
			 "posix_read: getsockopt() failed: %d\n", errno);
	 (void) write(STDERR_FILENO, error_message, strlen(error_message));
      }
      if(socket_error)
      {
	 (void) snprintf(error_message, 2047,
			 "posix_read: pending socket error: %d\n", socket_error);
	 (void) write(STDERR_FILENO, error_message, strlen(error_message));
      }
   }

   /* Determine if the socket is still connected. */
   {
      struct sockaddr peer;
      socklen_t socklen;
      char error_message[2048];

      socklen = sizeof(peer);
      if(getpeername(fd, &peer, &socklen) < 0)
      {
	 (void) snprintf(error_message, 2047,
			 "posix_read: getpeername() failed: %d\n", errno);
	 (void) write(STDERR_FILENO, error_message, strlen(error_message));
      }
   }

   /* Get the number of bytes in the receive socket buffer. */
   {
      int nbytes;
      char error_message[2048];

      if(ioctl(fd, FIONREAD, &nbytes) < 0)
      {
	 (void) snprintf(error_message, 2047,
			 "posix_read: ioctl(FIONREAD) failed: %d\n", errno);
	 (void) write(STDERR_FILENO, error_message, strlen(error_message));
      }
      else
      {
	 (void) fprintf(stderr, "posix_read: ioctl(FIONREAD): %d\n",
			nbytes);
      }
   }

   /* Get the socket state. */
   {
#ifdef __linux__
      FILE        *proc_net_tcp_fp;
      char        line[2048];
      char        inode[50];  /* for string comparision */
      long        state = 0;
      char        error_message[2048];
      char states[][12] = {"UNKNOWN",
			   "ESTABLISHED",
			   "SYN_SENT",
			   "SYN_RECV",
			   "FIN_WAIT1",
			   "FIN_WAIT2",
			   "TIME_WAIT",
			   "CLOSE",
			   "CLOSE_WAIT",
			   "LAST_ACK",
			   "LISTEN",
			   "CLOSING"
      };

      /* Open the /proc file for read. */
      if((proc_net_tcp_fp = fopen("/proc/net/tcp", "r")) == NULL)
      {
	 (void) snprintf(error_message, 2047,
			 "posix_read: open failed: %d\n", errno);
	 (void) write(STDERR_FILENO, error_message, strlen(error_message));
	 goto skip;
      }

      /* Get the inode (as a string for comparison). */
      (void) snprintf(inode, 49, "%lu",
		      (unsigned long) (sock_stats.st_ino));

      errno = 0;
      /* Obtain the line we want from /proc. */
      while(fgets(line, 2047, proc_net_tcp_fp) != NULL)
      {
	 if(strstr(line, inode) != NULL)
	 {
#if 0
	    /* Usefull for debugging. */
	    (void) write(STDERR_FILENO, line, strlen(line));
#endif
	    /* When the line with the matching inode is found, pull out the
	     * state info (bytes 33-37) and convert to a long. */
	    state = strtol(&(line[33]), NULL, 16);
	    break;
	 }
      }

      fclose(proc_net_tcp_fp);

      if(state)
      {
	 if(state > 0 && state <= 11)
	    (void) fprintf(stderr, "posix_read: socket state: %s\n",
			   states[state]);
      }
#else
   goto skip;
#endif /* __linux__ */
   }
  skip: /* Jump here if we got an error opening /proc/net/tcp. */

   return 0;
}

/***************************************************************************/
/***************************************************************************/

/* Return 1 on error, 0 on success. */
static int setup_mmap_io(struct transfer *info)
{
  int fd = info->fd;            /* The file descriptor in question. */
  struct stat file_info;        /* Information about the file to write to. */
  off_t bytes = info->size;     /* Number of bytes to transfer. (signed) */
  size_t mmap_len;              /* map_size adjusted to be memory aligned. */
  int mmap_permissions;         /* Hold the mmap_permisssions. */
  void* mmap_ptr;

  /* Make sure that the memory map region size is set correctly.  Even if
   * this file descriptor can not do memory mapped i/o, the other
   * transfer thread might. */
  info->mmap_size = align_to_page(info->mmap_size);
  mmap_len = (size_t)min2ull((unsigned long long)bytes,
			     (unsigned long long)info->mmap_size);

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
		       0.0, __FILE__, __LINE__, NULL);
    return 1;
  }
  /* If the file descriptor is not a file, don't continue. */
  if(!S_ISREG(file_info.st_mode))
  {
#ifdef DEBUG_REVERT
        (void)write(STDERR_FILENO, filesystem_mmap_io_error,
	      strlen(filesystem_mmap_io_error));
#endif /*DEBUG_REVERT*/
     info->mmap_io = (bool)0U;
     return 0;
  }

  /* When opening a mmapped i/o region for writing, the file must already
   * be there and already have the correct size. */
  if(info->transfer_direction > 0)  /* If true, it is a write. */
  {
     /* If the size of the transfer is not known, we need revert to another
      * transfer method.  The output filesize is required to be set,
      * either by ftruncate() or lseek() with write(1), before mmap() is
      * called. */
     if(info->size == -1)
     {
#ifdef DEBUG_REVERT
        (void)write(STDERR_FILENO, filesize_mmap_io_error,
	      strlen(filesize_mmap_io_error));
#endif /*DEBUG_REVERT*/
	info->mmap_io = (bool)0U;
	return 0;
     }

     /* Set the size of the file. */
     errno = 0;
     if(ftruncate(fd, bytes) < 0)
     {
	pack_return_values(info, 0, errno, FILE_ERROR, "ftruncate failed",
			   0.0, __FILE__, __LINE__, NULL);
	return 1;
     }
  }

  /* Determine the user permissions necessary for mmap io to work. */
  if(info->transfer_direction > 0) /* If true, it is a write. */
     mmap_permissions = PROT_WRITE | PROT_READ;
  else
     mmap_permissions = PROT_READ;

  /* Create the memory mapped file. info->mmap_ptr will equal the
   * starting memory address on success; MAP_FAILED on error. */
  errno = 0;
  if((mmap_ptr = mmap(NULL, mmap_len, mmap_permissions,
		      MAP_SHARED, fd, (off_t)0)) == MAP_FAILED)
  {
     if(errno == ENODEV)
     {
	/* There probably should be a write to stderr here.  The message
	 * should say something like, "using mmapped i/o failed, reverting
	 * to posix based i/o." */
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
	   /*
	    * If mmap()  failed on the write half of the transfer,
	    * set the filesize back to the original size.  On writes we
	    * don't, care about any file corruption (yet) because we
	    * have not written anything out.
	    *
	    * There is a good reason why it is set back to the original size.
	    * The original filesize on a user initiated transfer is 0 bytes.
	    * However, dcache sets the filesize in pnfs to the correct size;
	    * before it starts the encp.
	    */

	  errno = 0;
	  if(ftruncate(fd, file_info.st_size) < 0)
	  {
	     pack_return_values(info, 0, errno, FILE_ERROR,
				"ftruncate failed", 0.0, __FILE__, __LINE__,
		                NULL);
	     return 1;
	  }
	}

	pack_return_values(info, 0, errno, FILE_ERROR,
			   "mmap failed", 0.0, __FILE__, __LINE__, NULL);
	return 1;
     }
  }

  /* Clear the memory map since we only tested if it was possible. */
  errno = 0;
  if(munmap(mmap_ptr, mmap_len))
  {
     pack_return_values(info, 0, errno, FILE_ERROR,
			"munmap failed", 0.0, __FILE__, __LINE__, NULL);
     return 1;
  }

  return 0;
}

/* Returns NULL on error, the new memory address of the read direction
 * is returned on success. */
static void* get_next_segments(struct buffer *mem_buff,
			       struct transfer *info,
			       struct locks *thread_locks)
{
   if(info->mmap_io && info->other_mmap_io)
   {
      info->other_mmap_io = 0;
      if(get_next_segment(1, mem_buff, info, thread_locks) == NULL)
      {
	 return NULL;
      }
      info->other_mmap_io = 1;

      info->mmap_io = 0;
      if(get_next_segment(0, mem_buff, info, thread_locks) == NULL)
      {
	 return NULL;
      }
      info->mmap_io = 1;
   }
   return mem_buff->buffer[1];
}

/* Returns NULL on error, the new memory address is returned on success. */
static void* get_next_segment(int bin, struct buffer *mem_buff,
			      struct transfer *info,
			      struct locks *thread_locks)
{
  int advise_holder = 0; /* Advise hints for madvise. */
  size_t mmap_len;
  void* mmap_ptr;
  size_t mmap_io = ZERO;
  int fd = -1;
  int mmap_permissions = 0;

  /* This fuction should only be passed the read version of the transfer
   * struct. */
  if(info->transfer_direction > 0)
  {
     pack_return_values(info, 0, errno, FILE_ERROR,
			"read values only", 0.0, __FILE__, __LINE__,
	                thread_locks);
     return NULL;
  }

  /* If we are going to use memory mapped io. */
  if(info->mmap_io || info->other_mmap_io)
  {
     /* Determine if the read is via mmap io or the write is. */
     if(info->mmap_io)  /*Reads*/
     {
	mmap_io = info->mmap_io;
	fd = info->fd;
	mmap_permissions = PROT_READ;
#if defined ( POSIX_MADV_SEQUENTIAL ) && defined ( POSIX_MADV_WILLNEED )
	advise_holder = POSIX_MADV_SEQUENTIAL | POSIX_MADV_WILLNEED;
#elif defined ( MADV_SEQUENTIAL ) && defined ( MADV_WILLNEED )
	advise_holder = MADV_SEQUENTIAL | MADV_WILLNEED;
#endif
     }
     else if(info->other_mmap_io) /*Writes*/
     {
	mmap_io = info->other_mmap_io;
	fd = info->other_fd;
	mmap_permissions = PROT_WRITE | PROT_READ;
#if defined ( POSIX_MADV_SEQUENTIAL )
	advise_holder = POSIX_MADV_SEQUENTIAL;
#elif defined ( MADV_SEQUENTIAL )
	advise_holder = MADV_SEQUENTIAL;
#endif
     }

     /* Get the mmap info. */
     mmap_len = (size_t)min2ull((unsigned long long)info->bytes_to_go,
				(unsigned long long)info->mmap_size);

     /* Create the memory mapped file. */
     errno = 0;
     if((mmap_ptr = mmap(NULL, mmap_len, mmap_permissions, MAP_SHARED, fd,
			 info->size - info->bytes_to_go)) == MAP_FAILED)
     {
	pack_return_values(info, 0, errno, FILE_ERROR,
			   "mmap failed", 0.0, __FILE__, __LINE__,
			   thread_locks);
	return NULL;
     }

     /* Advise the system on the memory mapped i/o usage pattern. */
     errno = 0;
#if defined ( _POSIX_ADVISORY_INFO ) && _POSIX_ADVISORY_INFO >= 200112L
     if(posix_madvise(mmap_ptr, mmap_len, advise_holder) < 0)
#else
     if(madvise(mmap_ptr, mmap_len, advise_holder) < 0)
#endif /* _POSIX_ADVISORY_INFO */
     {
	/* If madvise is not implimented (ENOSYS), don't fail.  On IRIX
	* we expect EINVAL. */
	if(errno != ENOSYS && errno != EINVAL)
	{
	   /* Clear the memory mapped information. */
	   if(munmap(mmap_ptr, mmap_len) < 0)
	   {
	      mmap_ptr = MAP_FAILED; /* Set this explicitly. */
	      pack_return_values(info, 0, errno, FILE_ERROR,
				 "munmap failed", 0.0, __FILE__, __LINE__,
		                 thread_locks);
	      return NULL;
	   }

	   pack_return_values(info, 0, errno, FILE_ERROR,
			      "madvise failed", 0.0, __FILE__, __LINE__,
	                      thread_locks);
	   return NULL;
	}
     }

     mem_buff->buffer[bin] = mmap_ptr;
     mem_buff->buffer_type[bin] = MMAP_MEMORY;
  }
  else
  {
     /* If not using mmemory mapped io obtain some page aligned memory. */

     if((mmap_ptr = page_aligned_malloc(info->block_size)) == NULL)
     {
	pack_return_values(info, 0, errno, MEMORY_ERROR,
			   "memalign failed", 0.0, __FILE__, __LINE__,
	                   thread_locks);
	return NULL;
     }

     mem_buff->buffer[bin]  = mmap_ptr;
     mem_buff->buffer_type[bin] = MALLOC_MEMORY;
  }

  return mem_buff->buffer[bin];
}

/* Free the buffer bins for mmap to mmap transfer.  Return 0 for success
 * and 1 for failure. */
static int cleanup_segments(struct buffer *mem_buff,
			    struct transfer *info,
			    struct locks *thread_locks)
{
   if(info->mmap_io && info->other_mmap_io)
   {
      info->other_mmap_io = 0;
      if(cleanup_segment(0, mem_buff, info, thread_locks) > 0)
      {
	 return 1;
      }
      info->other_mmap_io = 1;

      info->mmap_io = 0;
      if(cleanup_segment(1, mem_buff, info, thread_locks) > 0)
      {
	 return 1;
      }
      info->mmap_io = 1;
   }

   return 0;
}

/* Free up the buffer bin specified.  Return 0 for success and 1 for
 * failure. */
static int cleanup_segment(int bin, struct buffer *mem_buff,
			   struct transfer *info,
			   struct locks *thread_locks)
{
   void* mmap_ptr = mem_buff->buffer[bin];
   /* Note: Always make sure that info-> bytes gets updated after
    * cleanup_segment() is called.  Otherwise the wrong size gets
    * unmapped and that causes errors. */
   size_t mmap_len = (size_t)min2ull((unsigned long long)info->bytes_to_go,
				     (unsigned long long)info->mmap_size);

   /* If the file is a local disk, use memory mapped i/o on it.
    * Only advance to the next mmap segment when the previous one is done. */
   if(mem_buff->buffer_type[bin] == MMAP_MEMORY)
   {
      /* Force the data to be written out to disk. */
      errno = 0;
      if(msync(mmap_ptr, mmap_len, MS_SYNC | MS_INVALIDATE) < 0)
      {
	 pack_return_values(info, 0, errno, FILE_ERROR,
			    "msync failed", 0.0, __FILE__, __LINE__,
	                    thread_locks);
	 return 1;
      }

      /* If the file descriptor supports madvise, tell the kernel that
       * the memory range will not be needed anymore. */
#if defined ( _POSIX_ADVISORY_INFO ) && _POSIX_ADVISORY_INFO >= 200112L
      if(posix_madvise(mmap_ptr, mmap_len, POSIX_MADV_DONTNEED) < 0)
#else
#  ifdef MADV_DONTNEED
      if(madvise(mmap_ptr, mmap_len, MADV_DONTNEED) < 0)
#  else
      if(0)
#  endif
#endif /* _POSIX_ADVISORY_INFO */
      {
	 if(errno != EINVAL && errno != ENOSYS)
	 {
	    pack_return_values(info, 0, errno, WRITE_ERROR,
			       "madvise failed", 0.0, __FILE__, __LINE__,
	                       thread_locks);
	    return 1;
	 }
      }

      /* Unmap the current mapped memory segment. */
      errno = 0;
      if(munmap(mmap_ptr, mmap_len) < 0)
      {
	 pack_return_values(info, 0, errno, FILE_ERROR,
			    "munmap failed", 0.0, __FILE__, __LINE__,
	                    thread_locks);
	 return 1;
      }

      mem_buff->buffer[bin] = NULL;
      mem_buff->buffer_type[bin] = EMPTY_MEMORY;
      return 0;
   }
   else
   {
      free(mmap_ptr);
      mem_buff->buffer[bin] = NULL;
      mem_buff->buffer_type[bin] = EMPTY_MEMORY;
      return 0;
   }
}

/* Removes a file lock. (not a mutex lock). */
static int remove_lock(struct transfer *info, struct locks *thread_locks)
{
#ifdef F_SETLK
   struct flock filelock;
   int rtn_fcntl;

   /* Now that we are done with this file, release the lock. */
   if(info->advisory_locking || info->mandatory_locking)
   {
      filelock.l_whence = SEEK_SET;
      filelock.l_start = 0L;
      filelock.l_type = F_UNLCK;
      filelock.l_len = 0L;

      /* Unlock the file. */
      errno = 0;
      if((rtn_fcntl = fcntl(info->fd, F_SETLK, &filelock)) < 0)
      {
	 pack_return_values(info, 0, errno, FILE_ERROR,
			    "fcntl(F_SETLK) failed", 0.0,
			    __FILE__, __LINE__, thread_locks);
	 return 1;
      }
   }
#endif /* F_SETLK */
   return 0;
}

/* Removes a file lock. (not a mutex lock). */
/* This version does not call pack_return_values() and is inteaded to
 * only be called from pack_return_values().  The nr stands for
 * Non-Recursive.*/
static int remove_lock_nr(struct transfer *info)
{
#ifdef F_SETLK
   struct flock filelock;
   int rtn_fcntl;

   /* Now that we are done with this file, release the lock. */
   if(info->advisory_locking || info->mandatory_locking)
   {
      filelock.l_whence = SEEK_SET;
      filelock.l_start = 0L;
      filelock.l_type = F_UNLCK;
      filelock.l_len = 0L;

      /* Unlock the file. */
      errno = 0;
      if((rtn_fcntl = fcntl(info->fd, F_SETLK, &filelock)) < 0)
      {
	 return 1;
      }
   }
#endif /* F_SETLK */
   return 0;
}

/* Return 1 on error, 0 on success. */
static int finish_read(struct transfer *info, struct locks *thread_locks)
{
   return remove_lock(info, thread_locks);
}

/* Return 1 on error, 0 on success. */
static int finish_write(struct transfer *info, struct locks *thread_locks)
{
  int rtn_fcntl;

  /* Only worry about this for posix io. */
  if(!info->mmap_io && !info->direct_io)
  {
    /* If the file descriptor supports fsync force the data to be flushed to
     * disk.  This can obviously fail for things like fsync-ing sockets, thus
     * EINVAL errors are ignored. */
    errno = 0;
#if defined ( _POSIX_FSYNC ) && _POSIX_FSYNC > 0L
    if(fsync(info->fd) < 0)
    {
       if(errno != EINVAL && errno != EROFS)
       {
	  pack_return_values(info, 0, errno, WRITE_ERROR,
			     "fsync failed", 0.0, __FILE__, __LINE__,
	                     thread_locks);
	  return 1;
       }
    }
#else
    /* If all else fails, force this to sync all data. */
    sync();
#endif /*_POSIX_FSYNC*/

#if defined ( _POSIX_ADVISORY_INFO ) && _POSIX_ADVISORY_INFO >= 200112L
    /* If the file descriptor supports fadvise, tell the kernel that
     * the file will not be needed anymore. */
    if(posix_fadvise(info->fd, 0, info->size, POSIX_FADV_DONTNEED) < 0)
    {
       if(errno != EINVAL && errno != ESPIPE && errno != ENOSYS)
       {
	  pack_return_values(info, 0, errno, WRITE_ERROR,
			     "fadvise failed", 0.0, __FILE__, __LINE__,
	                     thread_locks);
	  return 1;
       }
    }
#endif /*_POSIX_ADVISORY_INFO*/
  }

  rtn_fcntl = remove_lock(info, thread_locks);
  if(rtn_fcntl)
     return rtn_fcntl;

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
  int rtn;             /* Holds the return value from read()/write(). */
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
			__FILE__, __LINE__, NULL);
     return 1;
  }
  /* Direct IO can only work on regular files.  Even if direct io is
   * turned on the filesystem still has to support it. */
  if(! S_ISREG(file_info.st_mode))
  {
#ifdef DEBUG_REVERT
     (void)write(STDERR_FILENO, generic_direct_io_error,
		 strlen(generic_direct_io_error));
#endif /*DEBUG_REVERT*/
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
			"fcntl(F_GETFL) failed", 0.0, __FILE__, __LINE__,
	                NULL);
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
	 * should say something like, "using direct i/o failed, reverting
	 * to posix based i/o." */
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
			   "fcntl(F_SETFL) failed", 0.0, __FILE__, __LINE__,
	                   NULL);
	return 1;
     }
  }

# ifdef linux

  /*
   * Even though direct i/o has been supported since the 2.4.10 Linux kernel,
   * Redhat for there 9.0 release (8.0 maybe too?) (2.4.20 Redhat kernel)
   * applied a patch that turned of direct i/o.  Stock kernels leave direct
   * i/o on.  The problem with how Redhat did it, is that the fcntl(F_SETFL)
   * call above does not return an error.  Thus, to detect this kernel
   * and turn off direct i/o recheck the FD flags.
   */

  /*Get the current file descriptor flags.*/
  errno = 0;
  if((test_fcntl = fcntl(info->fd, F_GETFL, 0)) < 0)
  {
     pack_return_values(info, 0, errno, FILE_ERROR,
			"fcntl(F_GETFL) failed", 0.0, __FILE__, __LINE__,
	                NULL);
     return 1;
  }

  /* Test to see if the fcntl(F_SETFL) function really succeded. */
  if((test_fcntl & O_DIRECT) == 0)
  {
     /* There probably should be a write to stderr here.  The message
      *	should say something like, "using direct i/o failed, reverting
      *	to posix based i/o." */
#ifdef DEBUG_REVERT
     (void)write(STDERR_FILENO, kernel_direct_io_error,
	   strlen(kernel_direct_io_error));
#endif /*DEBUG_REVERT*/
     info->direct_io = 0;
     return 0;
  }

  /* Get some aligned memory for the following test(s). */
  temp_buffer = page_aligned_malloc((size_t)sysconf(_SC_PAGESIZE));

  /*
   * 2.4.9 kernels (FL7.1) and ealier do not support direct io.  The test for
   * running on one of these older Linux kernels is to write a non-page
   * aligned amount of data.  If successful (return value > 0) then direct
   * i/o is not supported.
   */

  errno = 0;
  errno = 0;
  if(info->transfer_direction > 0) /* write */
  {
     rtn = write(info->fd, temp_buffer, (size_t)50ULL);
  }
  else /* read */
  {
     rtn = read(info->fd, temp_buffer, (size_t)50ULL);
  }
  if(rtn > 0)
  {
     free(temp_buffer);

     /* Clear the FD of the 0_DIRECT flag. */
     if(fcntl(info->fd, F_SETFL, rtn_fcntl) < 0)
     {
	pack_return_values(info, 0, errno, FILE_ERROR,
			   "fcntl(F_SETFL) failed", 0.0, __FILE__, __LINE__,
	                   NULL);
	return 1;
     }

     /* There probably should be a write to stderr here.  The message
      *	should say something like, "using direct i/o failed, reverting
      *	to posix based i/o." */
#ifdef DEBUG_REVERT
     (void)write(STDERR_FILENO, kernel_direct_io_error,
	   strlen(kernel_direct_io_error));
#endif /*DEBUG_REVERT*/
     info->direct_io = 0;
     return 0;
  }

  /*
   * FL7.3 has a direct i/o bug that requires a confusing work around.
   * The check (within the kernel) to see if the opened file can really
   * do direct i/o is done during the write call and not during the
   * fcntl/open call.  Hence, the attempt at the following write() call.
   * If a properly aligned write is done and it succedes (return value != -1),
   * then direct i/o is available.
   */

  errno = 0;
  if(info->transfer_direction > 0) /* write */
  {
     rtn = write(info->fd, temp_buffer, (size_t)sysconf(_SC_PAGESIZE));
  }
  else /* read */
  {
     rtn = read(info->fd, temp_buffer, (size_t)sysconf(_SC_PAGESIZE));
  }
  if(rtn == (ssize_t)-1)
  {
     free(temp_buffer);

     /* Clear the FD of the 0_DIRECT flag. */
     if(fcntl(info->fd, F_SETFL, rtn_fcntl) < 0)
     {
	pack_return_values(info, 0, errno, FILE_ERROR,
			   "fcntl(F_SETFL) failed", 0.0, __FILE__, __LINE__,
	                   NULL);
	return 1;
     }

     /* There probably should be a write to stderr here.  The message
      * should say something like, "using direct i/o failed, reverting
      * to posix based i/o." */
#ifdef DEBUG_REVERT
     printf("errno: %d\n", errno);
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
			"lseek failed", 0.0, __FILE__, __LINE__, NULL);
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
  int new_fcntl = 0;      /* Holder of FD flags or-ed with O_[DR]SYNC. */
  int rtn_fcntl;          /* Stores the original FD flags. */
  struct flock filelock;  /* Stores the locking request. */

  /* Stat the file.  The mode is used to check if it is a regular file. */
  if(fstat(info->fd, &file_info))
  {
    pack_return_values(info, 0, errno, FILE_ERROR, "fstat failed",
		       0.0, __FILE__, __LINE__, NULL);
    return 1;
  }

  /*
   * The fsync() and fdatasync() calls only make sense if the file is
   * a regular file.  By setting these values to zero for other types,
   * we don't waste time by checking if the file is a regular file
   * each time.
   */

  if(S_ISREG(file_info.st_mode))
  {
#ifdef O_SYNC
     if(info->synchronous_io)
	new_fcntl |= O_SYNC;
#endif /* O_SYNC */
#ifdef O_DSYNC
     if(info->d_synchronous_io)
	new_fcntl |= O_DSYNC;
#endif /* O_DSYNC */
#ifdef O_RSYNC
     if(info->r_synchronous_io)
	new_fcntl |= O_RSYNC;
#endif /* O_RSYNC */

     if(new_fcntl)
     {
	/* Get the current file descriptor flags. */
	errno = 0;
	if((rtn_fcntl = fcntl(info->fd, F_GETFL, 0)) < 0)
	{
	   pack_return_values(info, 0, errno, FILE_ERROR,
			      "fcntl(F_GETFL) failed", 0.0,
			      __FILE__, __LINE__, NULL);
	   return 1;
	}

	/* Set the 0_[DR]SYNC flag(s). */
	errno = 0;
	if(fcntl(info->fd, F_SETFL, new_fcntl & rtn_fcntl) < 0)
	{
	   pack_return_values(info, 0, errno, FILE_ERROR,
			      "fcntl(F_SETFL) failed", 0.0,
			      __FILE__, __LINE__, NULL);
	   return 1;
	}
     }

#ifdef F_SETLK
     if(info->mandatory_locking)
     {
	/* Set the file permissions for mandatory locking. */
#if 0
	errno = 0;
	if(fchmod(info->fd, (file_info.st_mode & ~S_IXGRP) | S_ISGID) < 0)
	{
	   pack_return_values(info, 0, errno, FILE_ERROR,
			      "fcntl(F_SETFL) failed", 0.0,
			      __FILE__, __LINE__, NULL);
	   return 1;
	}
#endif /* 0 */
     }
     if(info->advisory_locking || info->mandatory_locking)
     {
	filelock.l_whence = SEEK_SET;
	filelock.l_start = 0L;
	if(info->transfer_direction > 0)  /* If true, it is a write. */
	   filelock.l_type = F_WRLCK;
	else /* read */
	   filelock.l_type = F_RDLCK;
	filelock.l_len = 0L;

	/* Get the requested file lock. */
	errno = 0;
	if((rtn_fcntl = fcntl(info->fd, F_SETLK, &filelock)) < 0)
	{
	   pack_return_values(info, 0, errno, FILE_ERROR,
			      "fcntl(F_SETLK) failed", 0.0,
			      __FILE__, __LINE__, NULL);
	   return 1;
	}
     }
#endif /* F_SETLK */

    /* Get the number of bytes to transfer between fsync() calls. */
    info->fsync_threshold = get_fsync_threshold(info);
    /* Set the current number of bytes remaining since last fsync to
     * the size of the file. */
    info->last_fsync = info->size;
  }
  else /* not a regular file */
  {
    /* Get the number of bytes to transfer between fsync() calls. */
    info->fsync_threshold = 0;
    /* Set the current number of bytes remaining since last fsync to
     * the size of the file. */
    info->last_fsync = 0;
    /* Only regular files support file locking. */
    info->advisory_locking = 0;
  }

  return 0;
}

/***************************************************************************/
/***************************************************************************/

/* Handle waiting for the file descriptor. Return non-zero on error and
 * zero on success. */
static int do_select(struct transfer *info, struct locks *thread_locks)
{
  fd_set fds;                   /* For use with select(2). */
  struct timeval timeout;       /* Time to wait for data. */
  struct timeval current_time;  /* Store the current time. */
  struct timeval start_time;    /* The time we started waiting. */
  int sts = 0;                  /* Return value from various C system calls. */
  double delta_time;            /* Difference between two gettimeofday(). */
  double start_delta;           /* Timeout value as floating point. */

  /* Initialize select values. */
  FD_ZERO(&fds);
  FD_SET(info->fd, &fds);

  /* Convert the timeout time into a double. */
  start_delta = extract_time((&(info->timeout)));

  /* get the start time */
  if(gettimeofday(&start_time, NULL))
  {
     pack_return_values(info, 0, errno, TIME_ERROR,
			"gettimeofday failed", 0.0, __FILE__, __LINE__,
			thread_locks);
     return 1;
  }

  while(1)
  {
     errno = 0;

     /* get the current time */
     if(gettimeofday(&current_time, NULL))
     {
	pack_return_values(info, 0, errno, TIME_ERROR,
			   "gettimeofday failed", 0.0, __FILE__, __LINE__,
			   thread_locks);
	return 1;
     }

     /* Set in timeout the current timeout time. */
     delta_time = elapsed_time(&start_time, &current_time);
     build_time(&timeout, start_delta - delta_time);

     /* Wait for there to be data on the descriptor ready for reading. */
     if(info->transfer_direction > 0)  /*write*/
     {
	sts = select(info->fd+1, NULL, &fds, NULL, &timeout);
	if(sts < 0 && errno == EINTR)
	   continue;
	if(sts < 0)
	   pack_return_values(info, 0, errno, WRITE_ERROR,
			      "fd select error", 0.0, __FILE__, __LINE__,
			      thread_locks);
     }
     else if(info->transfer_direction < 0)  /*read*/
     {
	sts = select(info->fd+1, &fds, NULL, NULL, &timeout);
	if(sts < 0 && errno == EINTR)
	   continue;
	if(sts < 0)
	   pack_return_values(info, 0, errno, READ_ERROR,
			      "fd select error", 0.0, __FILE__, __LINE__,
			      thread_locks);
     }

     if(sts == 0)
	pack_return_values(info, 0, ETIMEDOUT, TIMEOUT_ERROR,
			   "fd select timeout", 0.0, __FILE__, __LINE__,
			   thread_locks);

     if (sts <= 0)
	return 1;

     return 0;
  }
}


static ssize_t mmap_read(void *dst, size_t bytes_to_transfer,
			 struct transfer *info, struct locks *thread_locks)
{
  void* mmap_ptr = dst;

  /* Advise the system on the memory mapped i/o usage pattern. */
  errno = 0;
#if defined ( _POSIX_ADVISORY_INFO ) && _POSIX_ADVISORY_INFO >= 200112L
  if(posix_madvise(mmap_ptr, bytes_to_transfer, POSIX_MADV_WILLNEED) < 0)
#else
#  ifdef MADV_WILLNEED
  if(madvise(mmap_ptr, bytes_to_transfer, MADV_WILLNEED) < 0)
#  else
  if(0)
#  endif
#endif /* _POSIX_ADVISORY_INFO */
  {
     /* This is only a hint.  Don't worry on error. */
  }

  return (ssize_t)bytes_to_transfer;
}

static ssize_t mmap_write(void *src, size_t bytes_to_transfer,
			  struct transfer *info, struct locks *thread_locks)
{
  pthread_testcancel(); /* Any syncing action will take time. */

  /* Schedule the data for sync to disk now. */
  if(msync(src, bytes_to_transfer, MS_ASYNC) < 0)
  {
     pack_return_values(info, 0, errno, WRITE_ERROR,
			"msync error", 0.0, __FILE__, __LINE__, thread_locks);
     return -1;
  }
  pthread_testcancel(); /* Any syncing action will take time. */

  return (ssize_t)bytes_to_transfer;
}

/* Act like the posix read() call.  But return all interpreted errors with -1.
 * Also, set error values appropratly when detected. */
static ssize_t direct_read(void *dst, size_t bytes_to_transfer,
			   struct transfer* info, struct locks *thread_locks)
{
  ssize_t sts = 0;  /* Return value from various C system calls. */
  struct stat stats;
#if defined ( O_DIRECT ) && defined ( F_DIOINFO )
  int rtn_fcntl;
  struct dioattr direct_io_info;
#endif /*O_DIRECT and F_DIOINFO*/

  /* The variable bytes_to_transfer is passed in by value.  Changing it
   * here only lasts as long as this function does. */

  if(info->direct_io)
  {
    /* If direct i/o was specified, make sure the location is page aligned. */
    bytes_to_transfer = align_to_page(bytes_to_transfer);

#if defined ( O_DIRECT ) && defined ( F_DIOINFO )

    /*
     * SGIs have some limits on the size of read()s and write()s that can be
     * done with direct i/o.  This fcntl() call obtains those limits.  The
     * "struct dioattr" data type contains three items: d_mem, d_miniosz and
     * d_maxiosz.  See "man fcntl" for details.
     */
    if((rtn_fcntl = fcntl(info->fd, F_DIOINFO, &direct_io_info)) < 0)
    {
      pack_return_values(info, 0, errno, FILE_ERROR,
			 "fcntl(F_GETFL) failed", 0.0, __FILE__, __LINE__,
	                 thread_locks);
      return 1;
    }

    /* If the size of bytes_to_transfer is outside the range of d_miniosz
     * and d_maxiosz, adjust them to fit inside. */
    if(bytes_to_transfer < direct_io_info.d_miniosz)
    {
      bytes_to_transfer = direct_io_info.d_miniosz;
    }
    else if(bytes_to_transfer > direct_io_info.d_maxiosz)
    {
      bytes_to_transfer = direct_io_info.d_maxiosz;
    }
#endif /*O_DIRECT and F_DIOINFO*/
  }

  errno = 0;
  pthread_testcancel();  /* On Linux, read() isn't a cancelation point. */
  sts = read(info->fd, dst, bytes_to_transfer);
  pthread_testcancel();

  if (sts < 0)
  {
    pack_return_values(info, 0, errno, READ_ERROR,
		       "fd read error", 0.0, __FILE__, __LINE__, thread_locks);
    return -1;
  }
  if (sts == 0)
  {
    if(fstat(info->fd, &stats) == 0)
    {
       if(S_ISSOCK(stats.st_mode))
       {
	  /* If the connection is closed, give better error. */
	  errno = ENOTCONN;
       }
    }

    pack_return_values(info, 0, errno, TIMEOUT_ERROR,
		       "fd read timeout", 0.0, __FILE__, __LINE__,
		       thread_locks);
    return -1;
  }
  return sts;
}

/* Act like the posix read() call.  But return all interpreted errors with -1.
 * Also, set error values appropratly when detected. */
static ssize_t posix_read(void *dst, size_t bytes_to_transfer,
			  struct transfer* info, struct locks *thread_locks)
{
  ssize_t sts = 0;  /* Return value from various C system calls. */
  int remember_errno;
  struct stat stats;

  errno = 0;
  pthread_testcancel();  /* On Linux, read() isn't a cancelation point. */
  sts = read(info->fd, dst, bytes_to_transfer);
  pthread_testcancel();

  if (sts < 0)
  {
    pack_return_values(info, 0, errno, READ_ERROR,
		       "fd read error", 0.0, __FILE__, __LINE__, thread_locks);
    return -1;
  }
#if 1
  if (sts == 0 && info->bytes_to_go > 0U)
  {
    if(fstat(info->fd, &stats) == 0)
    {
       if(S_ISSOCK(stats.st_mode))
       {
	  /* Store this so that we can restore it after print_socket_info(). */
	  remember_errno = errno;

	  /* Print out socket information to standard error. */
	  print_socket_info(info->fd);

	  /*
	   * If the connection is closed, give better error.
	   * Set the errno back to what it was before print_socket_info()
	   * was called.
	   */
	  errno = ((remember_errno == 0) ? ENOTCONN : remember_errno);
       }
    }

    pack_return_values(info, 0, errno, TIMEOUT_ERROR,
		       "fd read timeout", 0.0, __FILE__, __LINE__,
                       thread_locks);
    return -1;
  }
#endif

  return sts;
}

/* Act like the posix write() call.  But return all interpreted errors with -1.
 * Also, set error values appropratly when detected. */
static ssize_t direct_write(void *src, size_t bytes_to_transfer,
			    struct transfer* info, struct locks *thread_locks)
{
  ssize_t sts = 0;  /* Return value from various C system calls. */
  struct stat stats;
  size_t use_bytes_to_transfer; /* Memaligned number of bytes_to_transfer. */
  off_t end_of_file;
  size_t size_diff;
#if defined ( O_DIRECT ) && defined ( F_DIOINFO )
  int rtn_fcntl;
  struct dioattr direct_io_info;
#endif /*O_DIRECT and F_DIOINFO*/

  /* The variable bytes_to_transfer is passed in by value.  Changing it
   * here only lasts as long as this function does. */

  if(info->direct_io)
  {
    /* If direct io was specified, make sure the location is page aligned. */
    use_bytes_to_transfer = align_to_page(bytes_to_transfer);

#if defined ( O_DIRECT ) && defined ( F_DIOINFO )

    /*
     * SGIs have some limits on the size of read()s and write()s that can be
     * done with direct i/o.  This fcntl() call obtains those limits.  The
     * "struct dioattr" data type contains three items: d_mem, d_miniosz and
     * d_maxiosz.  See "man fcntl" for details.
     */
    if((rtn_fcntl = fcntl(info->fd, F_DIOINFO, &direct_io_info)) < 0)
    {
      pack_return_values(info, 0, errno, FILE_ERROR,
			 "fcntl(F_GETFL) failed", 0.0, __FILE__, __LINE__,
	                 thread_locks);
      return 1;
    }

    /* If the size of bytes_to_transfer is outside the range of d_miniosz
     * and d_maxiosz, adjust them to fit inside. */
    if(bytes_to_transfer < direct_io_info.d_miniosz)
    {
      use_bytes_to_transfer = direct_io_info.d_miniosz;
    }
    else if(bytes_to_transfer > direct_io_info.d_maxiosz)
    {
      use_bytes_to_transfer = direct_io_info.d_maxiosz;
    }

#endif /*O_DIRECT and F_DIOINFO*/
  }
  else
  {
     use_bytes_to_transfer = bytes_to_transfer; /* Should never get here. */
  }

  /* Determine the size difference between the amount of data we care about
   * writing and the size that direct i/o says we write extra to be
   * page alligned. */
  size_diff = use_bytes_to_transfer - bytes_to_transfer;

  /* When faster methods will not work, use read()/write(). */
  errno = 0;
  pthread_testcancel();  /* On Linux, write() isn't a cancelation point. */
  sts = write(info->fd, src, use_bytes_to_transfer);
  pthread_testcancel();

  if (sts == -1)
  {
    pack_return_values(info, 0, errno, WRITE_ERROR,
		       "fd write error", 0.0, __FILE__, __LINE__,
                       thread_locks);
    return -1;
  }
  if (sts == 0)
  {
    if(fstat(info->fd, &stats) == 0)
      if(S_ISSOCK(stats.st_mode))
	/* If the connection is closed, give better error. */
	errno = ENOTCONN;

    pack_return_values(info, 0, errno, TIMEOUT_ERROR,
		       "fd write timeout", 0.0, __FILE__, __LINE__,
                       thread_locks);
    return -1;
  }

  /* Only apply after the last write() call.  Also, if the size of the
   * file was a multiple of the alignment used, then everything is correct
   * and attempting to do this file size 'fix' is unnecessary. */
  if(size_diff)
  {
     /* We use lseek() here instead of stat() to mitigate the chance that
      * another process is writing to this file too. */
     if( (end_of_file = lseek(info->fd, 0, SEEK_CUR)) != -1 )
     {
	/* Adjust the write() return value.  After the last call to write()
	 * for the file this is/can be too long.  It needs to be shrunk down
	 *	to the number of bytes written that we actually care about. */
	sts = (ssize_t)bytes_to_transfer;
	/* Truncate size at end of transfer.  For direct io all writes must be
	 *	a multiple of the page size.  The last write must be truncated
	 *      down to the correct size. */
	if(ftruncate(info->fd, end_of_file - size_diff) < 0)
	{
	   pack_return_values(info, 0, errno, WRITE_ERROR,
			      "ftruncate failed", 0.0, __FILE__, __LINE__,
	                      thread_locks);
	   return -1;
	}
     }
  }

  return sts;
}

/* Act like the posix write() call.  But return all interpreted errors with -1.
 * Also, set error values appropratly when detected. */
static ssize_t posix_write(void *src, size_t bytes_to_transfer,
			   struct transfer* info, struct locks *thread_locks)
{
  ssize_t sts = 0;  /* Return value from various C system calls. */
  struct stat stats;

  /* When faster methods will not work, use read()/write(). */
  errno = 0;
  pthread_testcancel();  /* On Linux, write() isn't a cancelation point. */
  sts = write(info->fd, src, bytes_to_transfer);
  pthread_testcancel();

  if (sts == -1)
  {
    pack_return_values(info, 0, errno, WRITE_ERROR,
		       "fd write error", 0.0, __FILE__, __LINE__,
		       thread_locks);
    return -1;
  }

  /* Just grab this now. */
  if(fstat(info->fd, &stats) < 0)
  {
     pack_return_values(info, 0, errno, FILE_ERROR,
			"fstat error", 0.0, __FILE__, __LINE__, thread_locks);
     return -1;
  }

  if (sts == 0)
  {
    if(S_ISSOCK(stats.st_mode))
       errno = ENOTCONN; /* If the connection is closed, give better error. */

    pack_return_values(info, 0, errno, TIMEOUT_ERROR,
		       "fd write timeout", 0.0, __FILE__, __LINE__,
                       thread_locks);
    return -1;
  }

  /* The rest of this function only applies to regular files. */
  if(!S_ISREG(stats.st_mode))
  {
     return sts;
  }

  /*
   * Force the data to disk.  Don't let encp take up to much memory.
   * This isnt the most accurate way of doing this, however it is less
   * overhead.  This will only be evaluated to true when the file is
   * a regular file.
   */
  if(info->fsync_threshold)
  {
     /* If the number of bytes of data transfered since the last sync has
      *	passed, do the fdatasync() and record amount completed. */
     if((info->last_fsync - info->bytes_to_go) > info->fsync_threshold)
     {
	info->last_fsync = info->bytes_to_go - sts;
	pthread_testcancel(); /* Any sync action will take time. */
	errno = 0;
#if defined ( _POSIX_SYNCHRONIZED_IO ) && _POSIX_SYNCHRONIZED_IO > 0L
	if(fdatasync(info->fd)) /* Sync the data. */
	{
	   if(errno != EINVAL)
	   {
	      pack_return_values(info, 0, errno, WRITE_ERROR,
				 "fdatasync failed", 0.0, __FILE__, __LINE__,
		                 thread_locks);
	      return -1;
	   }
	}
#elif defined ( _POSIX_FSYNC ) && _POSIX_FSYNC > 0L
	if(fsync(info->fd)) /* Sync the data. */
	{
	   if(errno != EINVAL)
	   {
	      pack_return_values(info, 0, errno, WRITE_ERROR,
				 "fsync failed", 0.0, __FILE__, __LINE__,
		                 thread_locks);
	      return -1;
	   }
	}
#else
	/* If all else fails, force this to sync all data. */
	sync();
#endif /*_POSIX_SYNCHRONIZED_IO*/

#if defined ( _POSIX_ADVISORY_INFO ) && _POSIX_ADVISORY_INFO >= 200112L
	/* If the file descriptor supports fadvise, tell the kernel that
	 * the file will not be needed anymore. */
	if(posix_fadvise(info->fd, 0, info->last_fsync,
			 POSIX_FADV_DONTNEED) < 0)
	{
	   if(errno != EINVAL && errno != ESPIPE && errno != ENOSYS)
	   {
	      pack_return_values(info, 0, errno, WRITE_ERROR,
				 "fadvise failed", 0.0, __FILE__, __LINE__,
		                 thread_locks);
	      return -1;
	   }
	}
#endif /*_POSIX_ADVISORY_INFO*/
     }
  }
  return sts;
}

/***************************************************************************/
/***************************************************************************/

static int thread_init(struct transfer *info, struct locks *thread_locks)
{
  int p_rtn;                    /* Pthread return value. */

  /* Initalize all the condition varaibles and mutex locks. */

  /* initalize the conditional variable signaled when a thread has finished. */
  if((p_rtn = pthread_cond_init(&(thread_locks->done_cond), NULL)) != 0)
  {
    pack_return_values(info, 0, p_rtn, THREAD_ERROR,
		       "cond init failed", 0.0, __FILE__, __LINE__,
                       thread_locks);
    return 1;
  }
  /* initalize the conditional variable to signal peer thread to continue. */
  if((p_rtn = pthread_cond_init(&(thread_locks->next_cond), NULL)) != 0)
  {
    pack_return_values(info, 0, p_rtn, THREAD_ERROR,
		       "cond init failed", 0.0, __FILE__, __LINE__,
                       thread_locks);
    return 1;
  }
  /* initalize the mutex for signaling when a thread has finished. */
  if((p_rtn = pthread_mutex_init(&(thread_locks->done_mutex), NULL)) != 0)
  {
    pack_return_values(info, 0, p_rtn, THREAD_ERROR,
		       "mutex init failed", 0.0, __FILE__, __LINE__,
                       thread_locks);
    return 1;
  }
  /* initalize the mutex for syncing the monitoring operations. */
  if((p_rtn = pthread_mutex_init(&(thread_locks->monitor_mutex), NULL)) != 0)
  {
    pack_return_values(info, 0, p_rtn, THREAD_ERROR,
		       "mutex init failed", 0.0, __FILE__, __LINE__,
                       thread_locks);
    return 1;
  }
#ifdef DEBUG
  /* initalize the mutex for ordering debugging output. */
  if((p_rtn = pthread_mutex_init(&(thread_locks->print_lock), NULL)) != 0)
  {
    pack_return_values(info, 0, p_rtn, THREAD_ERROR,
		       "mutex init failed", 0.0, __FILE__, __LINE__,
                       thread_locks);
    return 1;
  }
#endif

  return 0;
}

static int thread_destroy(struct transfer *info, struct locks *thread_locks)
{
  int p_rtn;                    /* Pthread return value. */
  /* initalize the conditional variable signaled when a thread has finished. */
  if((p_rtn = pthread_cond_destroy(&(thread_locks->done_cond))) != 0)
  {
    pack_return_values(info, 0, p_rtn, THREAD_ERROR,
		       "cond destory failed", 0.0, __FILE__, __LINE__,
                       thread_locks);
    return 1;
  }
  /* initalize the conditional variable to signal peer thread to continue. */
  if((p_rtn = pthread_cond_destroy(&(thread_locks->next_cond))) != 0)
  {
    pack_return_values(info, 0, p_rtn, THREAD_ERROR,
		       "cond destory failed", 0.0, __FILE__, __LINE__,
                       thread_locks);
    return 1;
  }
  /* initalize the mutex for signaling when a thread has finished. */
  if((p_rtn = pthread_mutex_destroy(&(thread_locks->done_mutex))) != 0)
  {
    pack_return_values(info, 0, p_rtn, THREAD_ERROR,
		       "mutex destory failed", 0.0, __FILE__, __LINE__,
                       thread_locks);
    return 1;
  }
  /* initalize the mutex for syncing the monitoring operations. */
  if((p_rtn = pthread_mutex_destroy(&(thread_locks->monitor_mutex))) != 0)
  {
    pack_return_values(info, 0, p_rtn, THREAD_ERROR,
		       "mutex destory failed", 0.0, __FILE__, __LINE__,
                       thread_locks);
    return 1;
  }
#ifdef DEBUG
  /* initalize the mutex for ordering debugging output. */
  if((p_rtn = pthread_mutex_destroy(&(thread_locks->print_lock))) != 0)
  {
    pack_return_values(info, 0, p_rtn, THREAD_ERROR,
		       "mutex destory failed", 0.0, __FILE__, __LINE__,
                       thread_locks);
    return 1;
  }
#endif

  return 0;
}

/* The first parameter is the bin to wait on.  The second argument returns
 * the amount of time the current thread spends waiting for the other
 * thread to complete.  The third parameter is a pointer to the array of
 * pointers to the memory buffers.  The fourth parameter is the transfer struct
 * for this half of the transfer.  The fifth is the pointer to the struct
 * of locks. */
static int thread_wait(size_t bin, double *thread_wait_time,
		       struct buffer *mem_buff,
		       struct transfer *info, struct locks *thread_locks)
{
  int p_rtn;                    /* Pthread return value. */
  struct timeval cond_wait_tv;  /* Absolute time to wait for cond. variable. */
  struct timespec cond_wait_ts; /* Absolute time to wait for cond. variable. */
  int expected = (info->transfer_direction < 0); /*0 = writes; 1 = reads*/
  struct timeval thread_wait_end; /* Time waiting on other thread. */

  pthread_testcancel(); /* Don't grab a mutex if we should't use it. */

  /* Determine if the lock for the buffer_lock bin, bin, is ready. */
  if((p_rtn = pthread_mutex_lock(&(mem_buff->buffer_lock[bin]))) != 0)
  {
    pack_return_values(info, 0, p_rtn, THREAD_ERROR,
		       "mutex lock failed", 0.0, __FILE__, __LINE__,
                       thread_locks);
    return 1;
  }

  /* If the stored bin is still full (stored[bin] > 0 == 1) when writing or
   * still empty (stored[bin] == 0) when reading, then wait for the other
   * thread to catch up. */
  if((mem_buff->stored[bin] > ZERO) == expected) /*if(!stored[bin] == !expected)*/
  {
    if(info->size == -1 && is_other_thread_done(info, thread_locks))
    {
       /* For file transfers of unknown length, only the write thread
	* should be able to get here.  The write thread should get here
	* only when it has written out the contents of all of the memory
	* buffers and the read thread has completed. */
       return 0;
    }
    /* Determine the absolute time to wait in pthread_cond_timedwait(). */
    if(gettimeofday(&cond_wait_tv, NULL) < 0)
    {
      pack_return_values(info, 0, errno, TIME_ERROR,
			 "gettimeofday failed", 0.0, __FILE__, __LINE__,
	                 thread_locks);
      return 1;
    }
    cond_wait_ts.tv_sec = cond_wait_tv.tv_sec + info->timeout.tv_sec;
    cond_wait_ts.tv_nsec = cond_wait_tv.tv_usec * 1000;

    for( ; ; ) /* continue looping */
    {
       /* This bin still needs to be used by the other thread.  Put this thread
	* to sleep until the other thread is done with it. */
       if((p_rtn = pthread_cond_timedwait(&(thread_locks->next_cond),
					  &(mem_buff->buffer_lock[bin]),
					  &cond_wait_ts)) != 0)
       {
	  /* If the wait was interupted, go back and re-enter the
	   * pthread_cond_timedwait() function. */
	  if(p_rtn == EINTR)
	     continue;

	  pthread_mutex_unlock(&(mem_buff->buffer_lock[bin]));
	  pack_return_values(info, 0, p_rtn, THREAD_ERROR,
			     "waiting for condition failed",
			     0.0, __FILE__, __LINE__, thread_locks);
	  return 1;
       }

       /* Need to determine how much time was spent waiting on the other
	* thread.  */
       (void)gettimeofday(&thread_wait_end, NULL);
       *thread_wait_time += elapsed_time(&cond_wait_tv, &thread_wait_end);

       /* If we get here, pthread_cond_timedwait() returned 0 (success). */
       break;
    }
  }
  if((p_rtn = pthread_mutex_unlock(&(mem_buff->buffer_lock[bin]))) != 0)
  {
    pack_return_values(info, 0, p_rtn, THREAD_ERROR,
		       "mutex unlock failed", 0.0,
		       __FILE__, __LINE__, thread_locks);
    return 1;
  }

  /* Determine if the main thread sent the signal to indicate the other
   * thread exited early from an error. If this value is still non-zero/zero,
   * then assume there was an error. */
  if((mem_buff->stored[bin] > ZERO) == expected) /*if(!stored[bin] == !expected)*/
  {
     if(info->size == -1 && is_other_thread_done(info, thread_locks))
    {
       /* For file transfers of unknown length, only the write thread
	* should be able to get here.  The write thread should get here
	* only when it has written out the contents of all of the memory
	* buffers and the read thread has completed. */
       return 0;
    }
    pack_return_values(info, 0, ECANCELED, THREAD_ERROR,
		       "waiting for condition failed",
		       0.0, __FILE__, __LINE__, thread_locks);
    return 1;
  }

  return 0;
}

static int thread_signal(size_t bin, size_t bytes, struct buffer *mem_buff,
			 struct transfer *info, struct locks *thread_locks)
{
  int p_rtn;                    /* Pthread return value. */

  pthread_testcancel(); /* Don't grab a mutex if we should't use it. */

  /* Obtain the mutex lock for the specific buffer bin that is needed to
   * clear the bin for writing. */
  if((p_rtn = pthread_mutex_lock(&(mem_buff->buffer_lock[bin]))) != 0)
  {
    pack_return_values(info, 0, p_rtn, THREAD_ERROR,
		       "mutex lock failed", 0.0, __FILE__, __LINE__,
                       thread_locks);
    return 1;
  }

  /* Set the number of bytes in the buffer. After a write this is set
   * to zero, and after a read it is set to the amount read. */
  /* Does this really belong here??? */
  mem_buff->stored[bin] = bytes;

  /* If other thread sleeping, wake it up. */
  if((p_rtn = pthread_cond_signal(&(thread_locks->next_cond))) != 0)
  {
    pack_return_values(info, 0, p_rtn, THREAD_ERROR,
		       "waiting for condition failed",
		       0.0, __FILE__, __LINE__, thread_locks);
    return 1;
  }
  /* Release the mutex lock for this bin. */
  if((p_rtn = pthread_mutex_unlock(&(mem_buff->buffer_lock[bin]))) != 0)
  {
    pack_return_values(info, 0, p_rtn, THREAD_ERROR,
		       "mutex unlock failed", 0.0, __FILE__, __LINE__,
                       thread_locks);
    return 1;
  }

  return 0;
}

/*
 * WARNING: Only use thread_collect()  from the main thread.  Also, no other
 * thread is allowd to use SIGALRM, sleep, pause, usleep.  Note: nanosleep()
 * by posix definition is guarenteed not to use the alarm signal.
 */

static int thread_collect(pthread_t tid, unsigned int wait_time)
{
   int rtn, p_rtn;
   sigset_t sigs_to_block;      /* Signal set of those to block. */
   void* old_signal_handler;

   /* Put SIGALRM in a list of signals to change their blocking status.  Do
    * this before grabbing the mutex to avoid the complexity of having to
    * unlock the mutex on an error. */
  if(sigemptyset(&sigs_to_block) < 0)
    return errno;
  if(sigaddset(&sigs_to_block, SIGALRM) < 0)
    return errno;

  /* If we call do_read_write_threaded() in seperate threads within the
   * same process, we need to be sure that only one "EXfer main thread"
   * can use alarm() at one time. */
  if((p_rtn = pthread_mutex_lock(&collect_mutex)) != 0)
  {
    return p_rtn;
  }

  errno = 0;

  /* We don't want to leave the thread behind.  However, if something
   * very bad occured that may be the only choice. */
  if((old_signal_handler = signal(SIGALRM, sig_alarm)) != SIG_ERR)
  {
    /* If the alarm times off, the thread will not go away.  Probably, it
     * is waiting in the kernel. */
    if(sigsetjmp(alarm_join, 0) == 0)
    {
      /* Now that the handler is set, we need to allow the thread to
       * receive the SIGALRM signal.  As long as the first argument
       * (SIG_UNBLOCK) is correct, this function should never fail. */
      (void) pthread_sigmask(SIG_UNBLOCK, &sigs_to_block, NULL);

      /* The only error returned is when the thread to cancel does not exist
       * (anymore).  Since the point is to stop it, if it is already stopped
       * then there is not a problem ignoring the error. */
      (void)pthread_cancel(tid);

      /* Set the alarm to determine if the thread is still alive. */
      (void)alarm(wait_time);

      /* Collect the killed thread.  If this function fails to collect the
       * canceled thread it is because that thread is stuck in the kernel
       * waiting for i/o and cannot be killed.  On linux, a ps shows the
       * state of the thread as being in the 'D' state. */
      rtn = pthread_join(tid, (void**)NULL);

      /* Either an error occured or (more likely) the thread was joined by
       * this point.  Either way turn off the alarm. */
      (void)alarm(0);

      /* Tell this thread to block SIGARLM again.  Ignore any error, since
       * we still need to unlock the mutex on an error here. */
      (void) pthread_sigmask(SIG_BLOCK, &sigs_to_block, NULL);

    }
    else
    {
       rtn = EINTR;
    }

    /* Reset the signal handler. */
    if(signal(SIGALRM, old_signal_handler) != SIG_ERR)
    {
       rtn = errno;
    }
  }
  else
  {
    rtn = errno;
  }

  /* If we call do_read_write_threaded() in seperate threads within the
   * same process, we need to be sure that only one "EXfer main thread"
   * can use alarm() at one time. */
  if((p_rtn = pthread_mutex_unlock(&collect_mutex)) != 0)
  {
    return p_rtn;
  }

  return rtn;
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
  /*
   * If O_DIRECT was used on the file descriptor, we need to turn it off.
   * This simplifies the reading of the last part of the file that does
   * not fit into an entire buffer_size sized space.
   */

  /* Get the current file descriptor flags. */
  if((getfl_fcntl = fcntl(fd, F_GETFL, 0)) < 0)
     return 0;
  setfl_fcntl = getfl_fcntl & (~O_DIRECT);  /* turn off O_DIRECT */
  /* Set the new file descriptor flags. */
  if(fcntl(fd, F_SETFL, setfl_fcntl) < 0)
    return 0;
#endif

  /* Initialize values used looping through reading in the file. */
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
    /*
     * If one wanted to use direct i/o (or mmapped i/o with more work) for
     * the paranoid ecrc readback test then the following read() would have
     * to have the 'rest' variable contain a page aligned value.  Most other
     * values are already page aligned should someone wish this to be
     * possible.
     */

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
  /* Set the original file descriptor flags. */
  if(fcntl(fd, F_SETFL, getfl_fcntl) < 0)
     return 0;
#endif

  return crc;
}

/***************************************************************************/
/***************************************************************************/

/*
 * Given a pathname, place in block_device the device on which
 * the file is located.  The arguement bd_len is the length of the
 * character array block_device.  The mount_point and mp_len do the same
 * thing as block_device, but instead for the mount_point of the filesystem.
 *
 * block_device and mount_point should be at least PATH_MAX + 1 in length.
 * Returns 0 on success, -1 on error.  Errno is set from lower system call.
 *
 * Note: The use of dirname() and basename() make this a non-threadsafe
 * function.
*/
static int get_bd_name(char *name, char *block_device, size_t bd_len,
		       char *mount_point, size_t mp_len)
{
   /* for reading /etc/mtab */
   FILE   *mtab_fp;
   char   mtab_line[3 * PATH_MAX + 3];
   void   *index, *index2;
   size_t substring_length;
   char   cur_block_device[PATH_MAX + 1];
   char   cur_mount_point[PATH_MAX + 1];
   void*  start_from;

   struct stat mp_stat; /* mount point stat */
   struct stat bd_stat; /* block device stat */

#ifdef __sun
   const int TAB = 9;
   int separator = TAB;
#else
   const int SPACE = 32;
   int separator = SPACE;
#endif

   char   filename[PATH_MAX + 1];
   struct stat filestat;

   size_t bd_test_len; /* Does found block device fit in block_device? */
   size_t mp_test_len; /* Does found mount point fit in mount_point? */

   /*
    * Resolve the name given on the command line to the full absolute
    * path name for the file.
    */
   if(realpath(name, filename) == NULL)
   {
      if(errno == ENOENT)
      {
	 /* It may be true that the file does not exist.  So, lets try and
	  * find the directory. */
	 char tmp_copy[PATH_MAX + 1];
	 char* dname;

	 strncpy(tmp_copy, name, PATH_MAX);
	 dname = (char*)dirname(tmp_copy);
	 if(realpath(dname, filename) == NULL)
	 {
	    return -1;
	 }
      }
      else
      {
	 return -1;
      }
   }
   if(stat(filename, &filestat) < 0)
   {
      return -1;
   }

   /*
    * Read in the /etc/mtab file looking for the mount point that matches
    * the file specified in the command line.  We are looking for the
    * block device name that matches the mount point.
    */

#ifdef __sun
   mtab_fp = fopen("/etc/mnttab", "r");
#else
   mtab_fp = fopen("/etc/mtab", "r");
#endif
   while(fgets(mtab_line, 2047, mtab_fp) != NULL)
   {
      start_from = mtab_line;
      if((index = strchr(start_from, separator)) != NULL)
      {
	 /* copy out the block disk device */
	 substring_length = (size_t) index - (size_t) mtab_line;
	 (void) strncpy(cur_block_device, mtab_line,
			(size_t) substring_length);
	 cur_block_device[substring_length] = (char) 0;

	 start_from = (void*) ((size_t) index + 1);
	 if((index2 = strchr(start_from, separator)) != NULL)
	 {
	    /* copy out the mount point */
	    substring_length = (size_t) index2 - (size_t) start_from;
	    (void) strncpy(cur_mount_point, start_from,
			   (size_t) substring_length);
	    cur_mount_point[substring_length] = (char) 0;
	 }

	 /* Get the stat of the mount point. */
	 if(stat(cur_mount_point, &mp_stat) < 0)
	 {
	    continue;
	 }

	 /* Determine if the current mount point's device id read from
	  * the /etc/mtab file matches the device id of the file passed
	  * in by the user. */

	 if(mp_stat.st_dev == filestat.st_dev)
	 {
	    /* Get the stat of the block device. */
	    if(stat(cur_block_device, &bd_stat) < 0)
	    {
	       continue;
	    }

	    /* Determine if the current block device is indeed a block
	     * special file.  Some OSes and filesystems (i.e. IRIX with
	     * losf) will have the wrong devices being matched by just
	     * comparing the device ids of the mount point (done above). */
	    if(S_ISBLK(bd_stat.st_mode))
	    {
	       bd_test_len = strlen(cur_block_device);
	       mp_test_len = strlen(cur_mount_point);
	       if(bd_test_len < bd_len && mp_test_len < mp_len)
	       {
		  (void) strncpy(mount_point, cur_mount_point,
		                 mp_test_len + 1);
		  (void) strncpy(block_device, cur_block_device,
				 bd_test_len + 1);

		  fclose(mtab_fp);  /* Close file to prevent resource leak. */
		  return 0;
	       }
	       else
	       {
		  errno = ERANGE;
		  fclose(mtab_fp);  /* Close file to prevent resource leak. */
		  return -1;
	       }
	    }
	 }
      }
   }

   errno = ESRCH;
   fclose(mtab_fp);  /* Close file to prevent resource leak. */
   return -1;
}


/* These are two quota specific defines.  They represent getting quota
 * information based an uid and gid, respectively. */
#define USER_QUOTA  0
#define GROUP_QUOTA 1

/* Some OSes use different names for the dqblk struct's fields. */
#if defined(__linux__) || defined(__bsdi__)
#define dqb_btimelimit dqb_btime

#define dqb_fhardlimit dqb_ihardlimit
#define dqb_fsoftlimit dqb_isoftlimit
#define dqb_curfiles   dqb_curinodes
#define dqb_ftimelimit dqb_itime
#endif

/* These are the indexes for the quota output. */
const unsigned long BLOCK_HARD_LIMIT = 1U;
const unsigned long BLOCK_SOFT_LIMIT = 2U;
const unsigned long CURRENT_BLOCKS   = 3U;
const unsigned long FILE_HARD_LIMIT  = 4U;
const unsigned long FILE_SOFT_LIMIT  = 5U;
const unsigned long CURRENT_FILES    = 6U;
const unsigned long BLOCK_TIME_LIMIT = 7U;
const unsigned long FILE_TIME_LIMIT  = 8U;

/*
 * get_quotas():
 *
 * First arguement is a string containing the name of a block device.
 * Examples: /dev/dsk/dks20d125s6
 *           /dev/hda3
 * Second arguement is eiter USER_QUOTA or GROUP_QUOTA.
 * Third arguement is the memory address of a struct dqblk variable where
 * the quota information is returned from.
 *
 * Returns 0 on success, -1 on error.  Errno is set.
 */

#ifdef Q_QUOTACTL

/*
 * Use the Q_QUOTACTL to obtain the quota information.  (Sunos)
 */

int get_quotas(char *block_device, int type, struct dqblk* my_quota)
{
   /* In this case block_device really refers to the name of the quotas
    * file at the top of the file system. */

   int qf_fd;  /* quota file fd */
   char quota_filename[PATH_MAX + 1];
   struct quotctl quota_ioctl;

   {
       int op;
       uid_t uid;
       caddr_t addr;
     };

   if(type == USER_QUOTA)
   {
      quota_ioctl.op = Q_GETQUOTA;
      quota_ioctl.uid = geteuid();
      quota_ioctl.addr = (caddr_t) my_quota;
   }
   else
   {
      /* SunOS does not support group quotas. */
      errno = EINVAL;
      return -1;
   }

   (void) snprintf(quota_filename, PATH_MAX, "%s/%s", block_device, "quotas");

   if((qf_fd = open(quota_filename, O_RDONLY)) < 0)
   {
      return -1;
   }

   if(ioctl(qf_fd, Q_QUOTACTL, &quota_ioctl) < 0)
   {
      (void) close(qf_fd);
      return -1;
   }

   return 0;
}
#elif defined(Q_GETQUOTA)

/*
 * Use the quotactl() system call to obtain the quota information.
 */

int get_quotas(char *block_device, int type, struct dqblk* my_quota)
{
   int cmd;
   unsigned int gen_id;  /* generic id */
#ifdef Q_XGETQUOTA
   int remember_errno;
#endif

   if(type != USER_QUOTA && type != GROUP_QUOTA)
   {
      errno = EINVAL;
      return -1;
   }

#ifdef QCMD
   if(type == USER_QUOTA)
   {
      cmd = QCMD(Q_GETQUOTA, USRQUOTA);  /* user */
      gen_id = geteuid();
   }
   else
   {
      cmd = QCMD(Q_GETQUOTA, GRPQUOTA);  /* group */
      gen_id = getegid();
   }
#else
   if(type == USER_QUOTA)
   {
      cmd = Q_GETQUOTA;  /* user */
      gen_id = geteuid();
   }
   else
   {
#  ifdef Q_GETGQUOTA
      cmd = Q_GETGQUOTA; /* group */
      gen_id = getegid();
#  else
      errno = EINVAL;
      return -1;
#  endif /* Q_GETGQUOTA */
   }
#endif /* QCMD */

#ifdef __APPLE__
   if(quotactl(block_device, cmd, gen_id, (caddr_t) my_quota) == 0)
#else
   if(quotactl(cmd, block_device, gen_id, (caddr_t) my_quota) == 0)
#endif
   {
      return 0;
   }


#ifdef Q_XGETQUOTA
/*
 * We need to check for quotas a little bit differently if this is an
 * XFS filesystem.
 */

   remember_errno = errno;  /* Remember this incase it is the better error. */


   if(errno == EINVAL || errno == ENOTSUP)
   {
      struct fs_disk_quota my_disk_quota; /* for XFS */

#ifdef QCMD
      if(type == USER_QUOTA)
      {
	 cmd = QCMD(Q_XGETQUOTA, USRQUOTA);  /* user */
	 gen_id = geteuid();
      }
      else
      {
	 cmd = QCMD(Q_XGETQUOTA, GRPQUOTA);  /* group */
	 gen_id = getegid();
      }
#else
      if(type == USER_QUOTA)
      {
	 cmd = Q_XGETQUOTA;  /* user */
	 gen_id = geteuid();
      }
      else
      {
#  ifdef Q_XGETGQUOTA
	 cmd = Q_XGETGQUOTA; /* group */
	 gen_id = getegid();
#  else
	 errno = EINVAL;
	 return -1;
#  endif /* Q_XGETGQUOTA */
      }
#endif /* QCMD */

      if(quotactl(cmd, block_device, gen_id, (caddr_t) &my_disk_quota) == 0)
      {

	 /* Store the equivalent fields in the fs_disk_quota struct into
	  * the dqblk struct.
	  */

	 my_quota->dqb_bhardlimit = my_disk_quota.d_blk_hardlimit;
	 my_quota->dqb_bsoftlimit = my_disk_quota.d_blk_softlimit;
	 my_quota->dqb_curblocks = my_disk_quota.d_bcount;
	 my_quota->dqb_btimelimit = my_disk_quota.d_btimer;

	 my_quota->dqb_fhardlimit = my_disk_quota.d_ino_hardlimit;
	 my_quota->dqb_fsoftlimit = my_disk_quota.d_ino_softlimit;
	 my_quota->dqb_curfiles = my_disk_quota.d_icount;
	 my_quota->dqb_ftimelimit = my_disk_quota.d_itimer;

	 return 0;
      }
   }

   if(errno == EINVAL || errno == ENOTSUP)
   {
      errno = remember_errno;
   }


#endif /* Q_XGETQUOTA */

   return -1;
}

#else

/*
 * Niether Q_QUOTACTL or Q_GETQUOTA are defined.  Return error stating that
 * we are out of luck with respect to any quotas.
 */

/* Since quota header files were not included, we need to fake this type. */
typedef struct dqblk {int dummy;} dqblk;

int get_quotas(char *block_device, int type, struct dqblk* my_quota)
{
   errno = ENOTSUP;
   return -1;
}

#endif /* Q_QUOTACTL */

/***************************************************************************/
/***************************************************************************/

static int buffer_init(struct buffer *mem_buff, struct transfer *info,
		       struct locks *thread_locks)
{
  int p_rtn;                    /* Pthread return value. */
  size_t i;

  /* Allocate and set to zeros the array that holds the number of bytes
   * currently sitting in a bin. */
  errno = 0;
  if((mem_buff->stored = calloc(info->array_size, sizeof(size_t))) ==  NULL)
  {
    pack_return_values(info, 0, errno, MEMORY_ERROR,
		       "calloc failed", 0.0, __FILE__, __LINE__,
		       thread_locks);
    return 1;
  }
  /* Allocate and set to zeros the array of mutex locks for each buffer bin. */
  errno = 0;
  if((mem_buff->buffer_lock = calloc(info->array_size, sizeof(pthread_mutex_t))) == NULL)
  {
    pack_return_values(info, 0, errno, MEMORY_ERROR,
		       "calloc failed", 0.0, __FILE__, __LINE__,
		       thread_locks);
    return 1;
  }

  /* If mmap io is used, use the buffer variable for a different purpose. */
  errno = 0;
  if((mem_buff->buffer = calloc(info->array_size, sizeof(char *))) == NULL)
  {
     pack_return_values(info, 0, errno, MEMORY_ERROR,
			"memalign failed", 0.0, __FILE__, __LINE__,
			thread_locks);
     return 1;
  }

  /* Allocate and set to zeros the memory type array. */
  errno = 0;
  if((mem_buff->buffer_type = calloc(info->array_size, sizeof(size_t))) == NULL)
  {
    pack_return_values(info, 0, errno, MEMORY_ERROR,
		       "calloc failed", 0.0, __FILE__, __LINE__,
		       thread_locks);
    return 1;
  }

  /* initalize the array of bin mutex locks. */
  for(i = 0; i < info->array_size; i++)
    if((p_rtn = pthread_mutex_init(&(mem_buff->buffer_lock[i]), NULL)) != 0)
    {
      pack_return_values(info, 0, p_rtn, THREAD_ERROR,
			 "mutex init failed", 0.0, __FILE__, __LINE__,
	                 thread_locks);
      return 1;
    }

  return 0;
}

/***************************************************************************/
/***************************************************************************/

static void do_read_write_threaded(struct transfer *reads,
				   struct transfer *writes)
{
  size_t array_size = reads->array_size;  /* Number of buffer bins. */
  size_t i;                            /* Loop counting. */
  int volatile p_rtn = 0;              /* pthread_*() return values. */
  pthread_t monitor_tid;               /* Thread id numbers. */
  struct timeval cond_wait_tv;  /* Absolute time to wait for cond. variable. */
  struct timespec cond_wait_ts; /* Absolute time to wait for cond. variable. */
  struct t_monitor monitor_info;/* Stuct pointing to both transfer stucts. */
  pthread_attr_t read_attr;     /* Set any non-default thread attributes. */
  pthread_attr_t write_attr;    /* Set any non-default thread attributes. */
  sigset_t sigs_to_block;      /* Signal set of those to block. */

  struct buffer mem_buff;       /* Pointers to memory buffer structures. */
  struct locks thread_locks;    /* Pointers to thread locking structures. */

  /* Initialize the mutex locks so we can use them. */
  if(thread_init(reads, &thread_locks))
  {
    /* Since this error is for both reads and writes, copy it over to
     * the writes struct. */
    (void)memcpy(writes, reads, sizeof(reads));
    return;
  }

  /* Set the values for passing to the threads. */
  monitor_info.read_info = reads;
  monitor_info.write_info = writes;
  monitor_info.thread_locks = &thread_locks;
  monitor_info.mem_buff = &mem_buff;

  /* Block this signal.  Only the main thread should use/receive it from
   * inside of thread_collect().  The python code should already block this
   * for us, but if do_read_write_threaded() is itself called from multiple
   * threads (A.K.A. multi-threaded migration), then we need to make sure
   * only the one thread calling thread_collect() can receive SIGARLM. */
  if(sigemptyset(&sigs_to_block) < 0)
  {
    pack_return_values(reads, 0, errno, SIGNAL_ERROR,
		       "sigemptyset failed", 0.0, __FILE__, __LINE__,
                       &thread_locks);
    pack_return_values(writes, 0, errno, SIGNAL_ERROR,
		       "sigemptyset failed", 0.0, __FILE__, __LINE__,
                       &thread_locks);
    return;
  }
  if(sigaddset(&sigs_to_block, SIGALRM) < 0)
  {
    pack_return_values(reads, 0, errno, SIGNAL_ERROR,
		       "sigaddset failed", 0.0, __FILE__, __LINE__,
                       &thread_locks);
    pack_return_values(writes, 0, errno, SIGNAL_ERROR,
		       "sigaddset failed", 0.0, __FILE__, __LINE__,
                       &thread_locks);
    return;
  }
  if(pthread_sigmask(SIG_BLOCK, &sigs_to_block, NULL) < 0)
  {
    pack_return_values(reads, 0, errno, SIGNAL_ERROR,
		       "pthread_sigmask failed", 0.0, __FILE__, __LINE__,
                       &thread_locks);
    pack_return_values(writes, 0, errno, SIGNAL_ERROR,
		       "pthread_sigmask failed", 0.0, __FILE__, __LINE__,
                       &thread_locks);
    return;
  }

  /* Initialize the thread attributes to the system defaults. */

  /* Initialize the read thread attributes. */
  if((p_rtn = pthread_attr_init(&read_attr)) != 0)
  {
    pack_return_values(reads, 0, p_rtn, THREAD_ERROR,
		       "pthread_attr_init failed", 0.0, __FILE__, __LINE__,
                       &thread_locks);
    return;
  }

  /* Initialize the write thread attributes. */
  if((p_rtn = pthread_attr_init(&write_attr)) != 0)
  {
    pack_return_values(writes, 0, p_rtn, THREAD_ERROR,
		       "pthread_attr_init failed", 0.0, __FILE__, __LINE__,
                       &thread_locks);
    return;
  }

#if defined ( __sgi ) && defined ( PTHREAD_SCOPE_BOUND_NP )
  /*
   * On IRIX/SGI, one can set a thread to run on a specifid CPU.  Posix
   * says pthread_attr_setscope() only support PTHREAD_SCOPE_PROCESS and
   * PTHREAD_SCOPE_SYSTEM.  IRIX by default uses
   * PTHREAD_SCOPE_PROCESS.  To change this to PTHREAD_SCOPE_SYSTEM requires
   * root privledge.  IRIX supports a non-standerd scope called
   * PTHREAD_SCOPE_BOUND_NP (6.5.9 kernels and later) that does not need
   * root privledge to set.  The pthread_setrunon_np() function requires
   * that a process have PTHREAD_SCOPE_SYSTEM or PTHREAD_SCOPE_BOUND_NP
   * scope to set the cpu affinity.
   *
   * For the purpose of thorough documentation the following functions set
   * cpu/processor affinity on various architectures.  In all cases there
   * is an equivalent 'get' functionality.
   *
   * IRIX:
   * sysmp() with MP_SETMUSTRUN command  (If setting current process root
   *                                      privledge not needed)
   * pthread_setrunon_np() (about privledges; see above)
   *
   * Linux (2.5.8 and later kernels):
   * sched_setaffinity() (If setting current process root privledge not needed)
   *
   * SunOS:
   * processor_bind() (If setting current process root privledge not needed)
   * [See processor_bind() for get functionality too.]
   *
   * OSF1:
   * bind_to_cpu()    (If setting current process root privledge not needed)
   * bind_to_cpu_id() (If setting current process root privledge not needed)
   * [See getsysinfo() with GSI_CURRENT_CPU command for get functionality.]
   */

  /* Set the read thread scope to allow pthread_setrunon_np() to work. */
  if((p_rtn = pthread_attr_setscope(&read_attr, PTHREAD_SCOPE_BOUND_NP)) != 0)
  {
    pack_return_values(reads, 0, p_rtn, THREAD_ERROR,
		       "pthread_attr_init failed", 0.0, __FILE__, __LINE__,
                       &thread_locks);
    return;
  }

  /* Set the read thread scope to allow pthread_setrunon_np() to work. */
  if((p_rtn = pthread_attr_setscope(&write_attr, PTHREAD_SCOPE_BOUND_NP)) != 0)
  {
    pack_return_values(writes, 0, p_rtn, THREAD_ERROR,
		       "pthread_attr_init failed", 0.0, __FILE__, __LINE__,
                       &thread_locks);
    return;
  }

  /*
   * Remember the affinity for use later.  If no cpu affinity exists, the
   * return value is -1. Since, the process is not threaded yet, we don't
   * need to worry about the thread calls yet.
   */
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

  /* Unfortunatly, the crc_flag struct members, may not be correct.
   * They need to be adjusted in cases of memory mapped io being used. */
  /* When writing to disk with memory mapped io we need to turn on the
   * CRC calulation during the reads. */
  reads->crc_flag |= (writes->crc_flag && writes->mmap_io);
  /* When writing to disk with memory mapped io we need to turn off
   * the CRC calculation during the writes. */
  writes->crc_flag = writes->crc_flag && !(writes->mmap_io);

  reads->other_mmap_io = writes->mmap_io; /*is true if using memory mapped io*/
  writes->other_mmap_io = reads->mmap_io; /*is true if using memory mapped io*/
  reads->other_fd = writes->fd; /*necessary for mmap io*/
  writes->other_fd = reads->fd; /*necessary for mmap io*/

  if(reads->mmap_io && writes->mmap_io)
  {
     /* Doing a memory mapped io to memory mapped io copy is not very
      *	conducive to multithreading.  Revert to the single threaded
      * implementation. */

#ifdef DEBUG_REVERT
     (void)write(STDERR_FILENO, no_mmap_threaded_implimentation,
		 strlen(no_mmap_threaded_implimentation));
#endif /*DEBUG_REVERT*/
     do_read_write(reads, writes);
     return;
  }

  /* Allocate and initialize the buffer arrays. */
  if(buffer_init(&mem_buff, reads, &thread_locks))
  {
    /* Since this error is for both reads and writes, copy it over to
     * the writes struct. */
    (void)memcpy(writes, reads, sizeof(reads));
    return;
  }

  /* Snag this mutex before spawning the new threads.  Otherwise, there is
   * the possibility that the new threads will finish before the main thread
   * can get to the pthread_cond_timedwait() to detect the threads exiting. */
  if((p_rtn = pthread_mutex_lock(&(thread_locks.done_mutex))) != 0)
  {
    pack_return_values(reads, 0, p_rtn, THREAD_ERROR,
		       "mutex lock failed", 0.0, __FILE__, __LINE__,
                       &thread_locks);
    pack_return_values(writes, 0, p_rtn, THREAD_ERROR,
		       "mutex lock failed", 0.0, __FILE__, __LINE__,
                       &thread_locks);
    return;
  }

  /* Get the threads going. */

  /* Start the thread that 'writes' the file. */
  if((p_rtn = pthread_create(&(writes->thread_id), &write_attr,
			     &thread_write, &monitor_info)) != 0)
  {
    pack_return_values(reads, 0, p_rtn, THREAD_ERROR,
		       "write thread creation failed", 0.0, __FILE__, __LINE__,
                       &thread_locks);
    pack_return_values(writes, 0, p_rtn, THREAD_ERROR,
		       "write thread creation failed", 0.0, __FILE__, __LINE__,
                       &thread_locks);
    return;
  }

  /* Start the thread that 'reads' the file. */
  if((p_rtn = pthread_create(&(reads->thread_id), &read_attr,
			     &thread_read, &monitor_info)) != 0)
  {
    /* Don't let this thread continue on forever. */
    (void)thread_collect(writes->thread_id, get_fsync_waittime(writes));

    pack_return_values(reads, 0, p_rtn, THREAD_ERROR,
		       "monitor thread creation failed", 0.0,
		       __FILE__, __LINE__, &thread_locks);
    pack_return_values(writes, 0, p_rtn, THREAD_ERROR,
		       "monitor thread creation failed", 0.0,
		       __FILE__, __LINE__, &thread_locks);
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
		       "read thread creation failed", 0.0, __FILE__, __LINE__,
                       &thread_locks);
    pack_return_values(writes, 0, p_rtn, THREAD_ERROR,
		       "read thread creation failed", 0.0, __FILE__, __LINE__,
                       &thread_locks);
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
		       "read thread creation failed", 0.0, __FILE__, __LINE__,
                       &thread_locks);
    pack_return_values(writes, 0, p_rtn, TIME_ERROR,
		       "read thread creation failed", 0.0, __FILE__, __LINE__,
                       &thread_locks);
    return;
  }
  cond_wait_ts.tv_sec = cond_wait_tv.tv_sec + (60 * 60 * 24); /*wait 24 hours*/
  cond_wait_ts.tv_nsec = cond_wait_tv.tv_usec * 1000;

  /*
   * This screewy loop of code is used to detect if a thread has terminated.
   * If an error occurs either thread could return in any order.  If
   * pthread_join() could join with any thread returned this would not
   * be so complicated.
   */
  while(!(reads->done && writes->done))
  {

    /* wait until the condition variable is set and we have the mutex */
    for( ; ; ) /* continue looping */
    {
       if((p_rtn = pthread_cond_timedwait(&(thread_locks.done_cond),
					  &(thread_locks.done_mutex),
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
			     __FILE__, __LINE__, &thread_locks);
	  pack_return_values(writes, 0, p_rtn, THREAD_ERROR,
			     "waiting for condition failed", 0.0,
			     __FILE__, __LINE__, &thread_locks);
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
	 * and it is knowningly being abandoned. */
	if(p_rtn != EINTR)
	{
	  /* Don't let these threads continue on forever. */
	  (void)thread_collect(writes->thread_id, get_fsync_waittime(writes));
	  (void)thread_collect(monitor_tid, get_fsync_waittime(writes));

	  /* Since, pack_return_values aquires this mutex, release it. */
	  pthread_mutex_unlock(&(thread_locks.done_mutex));

	  pack_return_values(reads, 0, p_rtn, THREAD_ERROR,
			     "joining with read thread failed",
			     0.0, __FILE__, __LINE__, &thread_locks);
	  pack_return_values(writes, 0, p_rtn, THREAD_ERROR,
			     "joining with read thread failed",
			     0.0, __FILE__, __LINE__, &thread_locks);
	  return;
	}
      }
      if(reads->exit_status)
      {
#if 0
	(void)fprintf(stderr,
	  "Read thread exited with error(%d) '%s' from %s line %d.\n",
	  reads->errno_val, strerror(reads->errno_val),
	  reads->filename, reads->line);*/
#endif

	/* Signal the other thread there was an error. We need to lock the
	 * mutex associated with the next bin to be used by the other thread.
	 * Since, we don't know which one, get them all. */
	for(i = 0; i < array_size; i++)
	{
	   pthread_mutex_trylock(&(mem_buff.buffer_lock[i]));
	}
	pthread_cond_signal(&(thread_locks.next_cond));
	for(i = 0; i < array_size; i++)
	{
	  pthread_mutex_unlock(&(mem_buff.buffer_lock[i]));
	}
      }
      reads->done = -1; /* Set to non-positive and non-zero value. */
      writes->other_thread_done = 1; /* Set true for write thread to know. */
    }
    if(writes->done > 0) /*true when thread_write ends*/
    {
      if((p_rtn = thread_collect(writes->thread_id,
				 get_fsync_waittime(writes))) != 0)
      {
	/* If the error was EINTR, skip this handling.  The thread is hung
	 * and it is knowningly being abandoned. */
	if(p_rtn != EINTR)
	{
	  /* Don't let these threads continue on forever. */
	  (void)thread_collect(reads->thread_id, get_fsync_waittime(reads));
	  (void)thread_collect(monitor_tid, get_fsync_waittime(writes));

	  /* Since, pack_return_values aquires this mutex, release it. */
	  pthread_mutex_unlock(&(thread_locks.done_mutex));

	  pack_return_values(reads, 0, p_rtn, THREAD_ERROR,
			     "joining with write thread failed",
			     0.0, __FILE__, __LINE__, &thread_locks);
	  pack_return_values(writes, 0, p_rtn, THREAD_ERROR,
			     "joining with write thread failed",
			     0.0, __FILE__, __LINE__, &thread_locks);
	  return;
	}
      }
      if(writes->exit_status)
      {
#if 0
	(void)fprintf(stderr,
	  "Write thread exited with error(%d) '%s' from %s line %d.\n",
	  writes->errno_val, strerror(writes->errno_val),
	  writes->filename, writes->line);*/
#endif

	/* Signal the other thread there was an error. We need to lock the
	 * mutex associated with the next bin to be used by the other thread.
	 * Since, we don't know which one, get them all.*/
	for(i = 0; i < array_size; i++)
	{
	  pthread_mutex_trylock(&(mem_buff.buffer_lock[i]));
	}
	pthread_cond_signal(&(thread_locks.next_cond));
	for(i = 0; i < array_size; i++)
	{
	  pthread_mutex_unlock(&(mem_buff.buffer_lock[i]));
	}
      }
      writes->done = -1; /* Set to non-positive and non-zero value. */
      reads->other_thread_done = 1; /* Set true for read thread to know. */
    }
  }
  pthread_mutex_unlock(&(thread_locks.done_mutex));

  /* Don't let this thread continue on forever. */
  (void)thread_collect(monitor_tid, get_fsync_waittime(writes));

  /*free the address space, this should only be done here if an error occured*/
  for(i = 0; i < array_size; i++)
  {
     if(mem_buff.buffer[i] != NULL)
     {
	if(mem_buff.buffer_type[i] == MALLOC_MEMORY)
	{
	   free(mem_buff.buffer[i]);
	}
	else if(mem_buff.buffer_type[i] == MMAP_MEMORY)
	{
	   /* If there is an error, there isn't much we can do. */
	   (void)munmap(mem_buff.buffer[i], reads->mmap_size);
	}
	else
	{
	   (void)fprintf(stderr, "Memory leak occured.\n");
	}
     }
  }

  /* When writing to disk using mmap io, we need to copy this value to the
   * writes struct. */
  if(writes->mmap_io && !(reads->mmap_io))
  {
     writes->crc_ui = reads->crc_ui;
  }

  /* Print out an error message.  This information currently is not returned
   * to encp.py. */
  if(reads->exit_status)
  {
    (void)fprintf(stderr,
		  "Low-level read transfer failure: [Errno %d] %s: \n"
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

  /* Free the dynamic memory. */
  free(mem_buff.stored);
  if(!(writes->mmap_io || reads->mmap_io))
  {
     free(mem_buff.buffer);
  }
  free(mem_buff.buffer_lock);

  /* Destory the mutex locks to avoid resource leaks. */
  if(thread_destroy(reads, &thread_locks))
  {
    /* Since this error is for both reads and writes, copy it over to
     * the writes struct. */
    (void)memcpy(writes, reads, sizeof(reads));
    return;
  }

  return;
}

static void* thread_monitor(void *monitor_info)
{
  struct transfer *read_info = ((struct t_monitor *)monitor_info)->read_info;
  struct transfer *write_info = ((struct t_monitor *)monitor_info)->write_info;
  struct locks *thread_locks = ((struct t_monitor*)monitor_info)->thread_locks;
  struct buffer *mem_buff = ((struct t_monitor *)monitor_info)->mem_buff;

  struct timespec sleep_time;  /* Time to wait in nanosleep. */
  struct timeval start_read;   /* Old time to remember during nanosleep. */
  struct timeval start_write;  /* Old time to remember during nanosleep. */
  sigset_t sigs_to_block;      /* Signal set of those to block. */

  /* Block this signal.  Only the main thread should use/receive it. */
  if(sigemptyset(&sigs_to_block) < 0)
    pthread_exit(NULL);
  if(sigaddset(&sigs_to_block, SIGALRM) < 0)
    pthread_exit(NULL);
  if(pthread_sigmask(SIG_BLOCK, &sigs_to_block, NULL) < 0)
    pthread_exit(NULL);

  /* This is the maximum time a read/write call is allowed to take. If it
   * takes longer than this then it has not been able to achive a minimum
   * rate of 0.5 MB/S. */
  sleep_time.tv_sec = get_fsync_waittime(read_info);
  sleep_time.tv_nsec = 0;

  pthread_testcancel(); /* Don't grab a mutex if we should't use it. */

  if(pthread_mutex_lock(&(thread_locks->done_mutex)))
    pthread_exit(NULL);

  while(!read_info->done && !write_info->done)
  {
     if(pthread_mutex_unlock(&(thread_locks->done_mutex)))
      pthread_exit(NULL);

    pthread_testcancel(); /* Don't grab a mutex if we should't use it. */

    if(pthread_mutex_lock(&(thread_locks->monitor_mutex)))
      pthread_exit(NULL);

    /* Grab the currently recorded start time. */
    (void)memcpy(&start_read, &(read_info->start_transfer_function),
		 sizeof(struct timeval));
    (void)memcpy(&start_write, &(write_info->start_transfer_function),
		 sizeof(struct timeval));

    if(pthread_mutex_unlock(&(thread_locks->monitor_mutex)))
      pthread_exit(NULL);

    for( ; ; ) /* continue looping */
    {

      pthread_testcancel(); /* Don't sleep if main thread is waiting. */

      /* Wait for the amount of time that it would take to transfer the buffer
       * at 0.5 MB/S. */
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

    if(pthread_mutex_lock(&(thread_locks->monitor_mutex)))
      pthread_exit(NULL);
    if(pthread_mutex_lock(&(thread_locks->done_mutex)))
      pthread_exit(NULL);

    pthread_testcancel(); /* Don't continue if we should stop now. */

    /* Check the old time versus the new time to make sure it has changed.
     * Also, check if the other thread has something to do (which means both
     * are going equally slow/fast) and if the time is cleared; this is
     * to avoid false positves. */

    if(!read_info->done && buffer_empty(read_info->array_size, mem_buff) &&
       (read_info->start_transfer_function.tv_sec > 0) &&
       (read_info->start_transfer_function.tv_usec > 0) &&
       (start_read.tv_sec == read_info->start_transfer_function.tv_sec) &&
       (start_read.tv_usec == read_info->start_transfer_function.tv_usec))
    {
      /* Tell the 'hung' thread to exit.  If we don't, then if/when it does
       * continue the memory locations have already been freed and will cause
       * a segmentation violation. */
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
       * failure) and error out nicely. */
      pthread_cond_signal(&(thread_locks->done_cond));

      pthread_mutex_unlock(&(thread_locks->monitor_mutex));
      pthread_mutex_unlock(&(thread_locks->done_mutex));

      return NULL;
    }
    if(!write_info->done && buffer_full(write_info->array_size, mem_buff) &&
       (write_info->start_transfer_function.tv_sec > 0) &&
       (write_info->start_transfer_function.tv_usec > 0) &&
       (start_write.tv_sec == write_info->start_transfer_function.tv_sec) &&
       (start_write.tv_usec == write_info->start_transfer_function.tv_usec))
    {
      /* Tell the 'hung' thread to exit.  If we don't, then if/when it does
       * continue the memory locations have already been freed and will cause
       * a segmentation violation. */
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
       * failure) and error out nicely. */
      pthread_cond_signal(&(thread_locks->done_cond));

      pthread_mutex_unlock(&(thread_locks->monitor_mutex));
      pthread_mutex_unlock(&(thread_locks->done_mutex));

      return NULL;
    }

    if(pthread_mutex_unlock(&(thread_locks->monitor_mutex)))
      pthread_exit(NULL);

  }

  pthread_mutex_unlock(&(thread_locks->done_mutex));

  return NULL;
}

static void* thread_read(void *info)
{
  struct transfer *read_info = ((struct t_monitor *)info)->read_info;
  struct locks *thread_locks = ((struct t_monitor *)info)->thread_locks;
  struct buffer *mem_buff = ((struct t_monitor *)info)->mem_buff;

  size_t segment_to_read;       /* Number of bytes to move in one loop. */
  size_t segment_read;          /* Number of bytes read in a sub loop. */
  /*int sts = 0;*/              /* Return value from various C system calls. */
  int rsts = -1;                /* Return value from read(). */
  size_t bin = ZERO;            /* The current bin (bucket) to use. */
  unsigned int crc_ui = 0U;     /* Calculated checksum. */
  void *read_to_addr;           /* Holder for the read to memory address. */
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
  double thread_wait_time = 0.0; /* Time spent waiting on the other thread. */

  /* Block this signal.  Only the main thread should use/receive it. */
  if(sigemptyset(&sigs_to_block))
  {
    pack_return_values(read_info, 0, errno, SIGNAL_ERROR,
		       "sigemptyset failed", 0.0, __FILE__, __LINE__,
                       thread_locks);
    return NULL;
  }
  if(sigaddset(&sigs_to_block, SIGALRM))
  {
    pack_return_values(read_info, 0, errno, SIGNAL_ERROR,
		       "sigaddset failed", 0.0, __FILE__, __LINE__,
                       thread_locks);
    return NULL;
  }
  if(pthread_sigmask(SIG_BLOCK, &sigs_to_block, NULL) < 0)
  {
    pack_return_values(read_info, 0, errno, SIGNAL_ERROR,
		       "pthread_sigmask failed", 0.0, __FILE__, __LINE__,
                       thread_locks);
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
		       0.0, __FILE__, __LINE__, thread_locks);
    return NULL;
  }
  (void)memcpy(&end_total, &start_total, sizeof(struct timeval));
  /* Initialize the thread's start time usage. */
  errno = 0;
  if(getrusage(RUSAGE_SELF, &start_usage) < 0)
  {
    pack_return_values(read_info, 0, errno, TIME_ERROR, "getrusage failed",
		       0.0, __FILE__, __LINE__, thread_locks);
    return NULL;
  }

  /* Stat the file.  The mode is used to check if it is a regular file. */
  errno = 0;
  if(fstat(read_info->fd, &file_info))
  {
    pack_return_values(read_info, 0, errno, FILE_ERROR, "fstat failed", 0.0,
		       __FILE__, __LINE__, thread_locks);
    return NULL;
  }

#if defined ( __sgi ) && defined ( PTHREAD_SCOPE_BOUND_NP )
  /* Make sure that the cpu affinity that the main thread may have is applied
   * to the thread FD that is a socket. */
  if(read_info->cpu_affinity >= 0 && S_ISSOCK(file_info.st_mode))
  {
    if((cpu_error = pthread_setrunon_np(read_info->cpu_affinity)) != 0)
       (void)fprintf(stderr, "CPU affinity non-fatal error: %s\n",
		     strerror(cpu_error));
  }
#endif /* PTHREAD_SCOPE_BOUND_NP */

  while( (read_info->bytes_to_go > 0) ||
	 (read_info->size == -1) ) /* && rsts != 0) )*/
  {
    /* If the other thread is slow, wait for it. */
    if(thread_wait(bin, &thread_wait_time, mem_buff, read_info, thread_locks))
    {
       return NULL;
    }
    /* Allocate the next buffer to place data into. */
    if(get_next_segment(bin, mem_buff, read_info, thread_locks) == NULL)
    {
       return NULL;
    }

    /* Number of bytes remaining for this loop. */
    if(read_info->mmap_io)
    {
       if(read_info->bytes_to_go > -1)
	  segment_to_read = (size_t)min2ull(
	     (unsigned long long)read_info->bytes_to_go,
	     (unsigned long long)read_info->mmap_size);
       else
	  segment_to_read = (unsigned long long)read_info->mmap_size;
    }
    else
    {
       if(read_info->bytes_to_go > -1)
	  segment_to_read = (size_t)min2ull(
	  (unsigned long long)read_info->bytes_to_go,
	  (unsigned long long)read_info->block_size);
       else
	  segment_to_read = (unsigned long long)read_info->block_size;
    }

    /* Set this to zero. */
    segment_read = ZERO;

    while(segment_to_read > ZERO)
    {
      /* Record the time to start waiting for the read to occur. */
      if(gettimeofday(&start_time, NULL) < 0)
      {
	pack_return_values(read_info, 0, errno, TIME_ERROR,
			   "gettimeofday failed", 0.0, __FILE__, __LINE__,
	                   thread_locks);
	return NULL;
      }

      /* Handle calling select to wait on the descriptor. */
      if(do_select(read_info, thread_locks))
	return NULL;

      pthread_testcancel(); /* Don't grab a mutex if we should't use it. */

      /* In case something happens, make sure that the monitor thread can
       * determine that the transfer is stuck. */
      if(pthread_mutex_lock(&(thread_locks->monitor_mutex)))
      {
	pack_return_values(read_info, 0, errno, THREAD_ERROR,
			   "mutex lock failed", 0.0, __FILE__, __LINE__,
	                   thread_locks);
	return NULL;
      }
      if(gettimeofday(&(read_info->start_transfer_function), NULL) < 0)
      {
	pack_return_values(read_info, 0, errno, TIME_ERROR,
			   "gettimeofday failed", 0.0, __FILE__, __LINE__,
	                   thread_locks);
	return NULL;
      }
      if(pthread_mutex_unlock(&(thread_locks->monitor_mutex)))
      {
	pack_return_values(read_info, 0, errno, THREAD_ERROR,
			   "mutex unlock failed", 0.0, __FILE__, __LINE__,
	                   thread_locks);
	return NULL;
      }

      /* Depending on the mechanism used to write out the data, different
       * memory locations need to be read into.  For POSIX and Direct I/O
       * the data is read into the buffer; for memory mapped writes it
       * is read directly into the memory mapped file. */
      /* These values will change with each iteration, don't get over
       * ambitous and try to move this out of the loop. */
      read_to_addr = (void*)((uintptr_t)mem_buff->buffer[bin] + segment_read);

      /* Read in the data. */
      if(read_info->mmap_io)
      {
	 rsts = mmap_read(read_to_addr, segment_to_read, read_info,
			  thread_locks);
      }
      else if(read_info->direct_io)
      {
	 rsts = direct_read(read_to_addr, segment_to_read, read_info,
			    thread_locks);
      }
      else
      {
	 rsts = posix_read(read_to_addr, segment_to_read, read_info,
	                   thread_locks);
      }

      if(rsts < 0)
	 return NULL;

      pthread_testcancel(); /* Don't grab a mutex if we should't use it. */

      /* Since the read call returned, clear the timeval struct. */
      if(pthread_mutex_lock(&(thread_locks->monitor_mutex)))
      {
	pack_return_values(read_info, 0, errno, THREAD_ERROR,
			   "mutex lock failed", 0.0, __FILE__, __LINE__,
	                   thread_locks);
	return NULL;
      }
      read_info->start_transfer_function.tv_sec = 0;
      read_info->start_transfer_function.tv_usec = -1;
      if(pthread_mutex_unlock(&(thread_locks->monitor_mutex)))
      {
	pack_return_values(read_info, 0, errno, THREAD_ERROR,
			   "mutex unlock failed", 0.0, __FILE__, __LINE__,
	                   thread_locks);
	return NULL;
      }

      /* Record the time the read operation completes. */
      if(gettimeofday(&end_time, NULL) < 0)
      {
	pack_return_values(read_info, 0, errno, TIME_ERROR,
			   "gettimeofday failed", 0.0, __FILE__, __LINE__,
	                   thread_locks);
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
	crc_ui = adler32(crc_ui, read_to_addr, (unsigned int)rsts);
	read_info->crc_ui = crc_ui;
	break;
      default:
	crc_ui = 0;
	read_info->crc_ui = crc_ui;
	break;
      }

      /* Update this nested loop's counting variables. */
      if(rsts == 0)
	 segment_to_read = ZERO;
      else
	 segment_to_read -= rsts;
      segment_read += rsts;

#ifdef DEBUG
      print_status(stderr, segment_read, segment_to_read, mem_buff, read_info,
	           thread_locks);
#endif /*DEBUG*/
    }

    /* Tell the other thread to go. */
    if(thread_signal(bin, segment_read, mem_buff, read_info, thread_locks))
       return NULL;

    /* Determine where to put the data. */
    bin = (bin + 1U) % read_info->array_size;
    pthread_testcancel(); /* Don't grab a mutex if we should't use it. */
    /* Determine the number of bytes left to transfer. */
    if(pthread_mutex_lock(&(thread_locks->monitor_mutex)))
    {
      pack_return_values(read_info, 0, errno, THREAD_ERROR,
			 "mutex lock failed", 0.0, __FILE__, __LINE__,
	                 thread_locks);
      return NULL;
    }
    read_info->bytes_to_go -= segment_read;
    read_info->bytes_transfered += segment_read;
    if(pthread_mutex_unlock(&(thread_locks->monitor_mutex)))
    {
      pack_return_values(read_info, 0, errno, THREAD_ERROR,
			 "mutex unlock failed", 0.0, __FILE__, __LINE__,
	                 thread_locks);
      return NULL;
    }

    if(read_info->size == -1 && segment_read == 0)
    {
       /* This will allow the read loop to exit when we are transfering
	* a file of previously unknown size. */
       read_info->size = read_info->bytes_transfered;
    }
  }

  /* Sync the data to disk and other 'completion' steps. */
  if(finish_read(read_info, thread_locks))
    return NULL;

  /* Get total end time. */
  if(gettimeofday(&end_total, NULL) < 0)
  {
    pack_return_values(read_info, 0, errno, TIME_ERROR,
		       "gettimeofday failed", 0.0, __FILE__, __LINE__,
                       thread_locks);
    return NULL;
  }
  /* Get the thread's time usage. */
  errno = 0;
#ifdef RUSAGE_THREAD
  if(getrusage(RUSAGE_THREAD, &end_usage) < 0)
#elif RUSAGE_LWP
  if(getrusage(RUSAGE_LWP, &end_usage) < 0)
#else
  if(getrusage(RUSAGE_SELF, &end_usage) < 0)
#endif
  {
    pack_return_values(read_info, 0, errno, TIME_ERROR,
		       "getrusage failed", 0.0, __FILE__, __LINE__,
                       thread_locks);
    return NULL;
  }

  /*
   * If the descriptor is for a regular file returning the total time passed
   * for use in the rate calculation appears accurate.  Unfortunatly, this
   * method doesn't seem to return accurate time/rate information for sockets.
   * Instead socket information seems most accurate by adding the total
   * CPU time usage to the time spent in select() and read()/write().
   */
  /*
   * June 2009: Modified to be the wall clock time minus the cumulative
   * time spent waiting for the other thread for all types of files.
   * For getrusage() POSIX says that the RUSAGE_SELF times are for the
   * process, not just the thread.  The old Linux threads appears to have
   * reported times for the thread, while the newer NPTL reports for
   * the entire process (which for newer Linux kernels resulted in low
   * network rates being reported).
   */

#if (defined(RUSAGE_THREAD) || defined(RUSAGE_LWP)) && 0
  /* Only use rusage method for socket if we can get this information on
   * a per-thread basis.
   * RUSAGE_THREAD is the Linux name.  RUSAGE_LWP is the Solaris name. */
  if(S_ISSOCK(file_info.st_mode))
  {
    corrected_time = rusage_elapsed_time(&start_usage, &end_usage) +
      transfer_time;
  }
  else
#endif
  {
    corrected_time = elapsed_time(&start_total, &end_total) - thread_wait_time;
  }

  pack_return_values(read_info, read_info->crc_ui, 0, 0, "",
		     corrected_time, NULL, 0, thread_locks);

  return NULL;
}


static void* thread_write(void *info)
{
  struct transfer *write_info = ((struct t_monitor *)info)->write_info;
  struct locks *thread_locks = ((struct t_monitor *)info)->thread_locks;
  struct buffer *mem_buff = ((struct t_monitor *)info)->mem_buff;

  size_t segment_to_write;      /* Number of bytes to write in one loop. */
  size_t segment_written;       /* Number of bytes witten in a sub loop. */
  /*int sts = 0;*/              /* Return value from various C system calls. */
  int wsts = -1;                /* Return value from write(). */
  size_t bin = ZERO;            /* The current bin (bucket) to use. */
  unsigned int crc_ui = 0U;     /* Calculated checksum. */
  void *write_from_addr;        /* Holder for the write from memory address. */
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
  double thread_wait_time = 0.0; /* Time spent waiting on the other thread. */

  /* Block this signal.  Only the main thread should use/receive it. */
  if(sigemptyset(&sigs_to_block) < 0)
  {
    pack_return_values(write_info, 0, errno, SIGNAL_ERROR,
		       "sigemptyset failed", 0.0, __FILE__, __LINE__,
                       thread_locks);
    return NULL;
  }
  if(sigaddset(&sigs_to_block, SIGALRM) < 0)
  {
    pack_return_values(write_info, 0, errno, SIGNAL_ERROR,
		       "sigaddset failed", 0.0, __FILE__, __LINE__,
                       thread_locks);
    return NULL;
  }
  if(pthread_sigmask(SIG_BLOCK, &sigs_to_block, NULL) < 0)
  {
    pack_return_values(write_info, 0, errno, SIGNAL_ERROR,
		       "pthread_sigmask failed", 0.0, __FILE__, __LINE__,
                       thread_locks);
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
		       "gettimeofday failed", 0.0, __FILE__, __LINE__,
                       thread_locks);
    return NULL;
  }
  (void)memcpy(&end_total, &start_total, sizeof(struct timeval));
  /* Get the thread's start time usage. */
  if(getrusage(RUSAGE_SELF, &start_usage) < 0)
  {
    pack_return_values(write_info, 0, errno, TIME_ERROR, "getrusage failed",
		       0.0, __FILE__, __LINE__, thread_locks);
    return NULL;
  }

  /* Get stat info. */
  errno = 0;
  if(fstat(write_info->fd, &file_info) < 0)
  {
    pack_return_values(write_info, 0, errno, FILE_ERROR,
		       "fstat failed", 0.0, __FILE__, __LINE__, thread_locks);
    return NULL;
  }

#if defined ( __sgi ) && defined ( PTHREAD_SCOPE_BOUND_NP )
  /* Make sure that the cpu affinity that the main thread may have is applied
   * to the thread FD that is a socket. */
  if(write_info->cpu_affinity >= 0 && S_ISSOCK(file_info.st_mode))
  {
    if((cpu_error = pthread_setrunon_np(write_info->cpu_affinity)) != 0)
       (void)fprintf(stderr, "CPU affinity non-fatal error: %s\n",
		     strerror(cpu_error));
  }
#endif /* PTHREAD_SCOPE_BOUND_NP */

  while( (write_info->bytes_to_go > 0) ||
	 (write_info->size == -1) ) /* && wsts != 0) )*/
  {
    /* If the other thread is slow, wait for it. */
    if(thread_wait(bin, &thread_wait_time, mem_buff, write_info, thread_locks))
    {
       return NULL;
    }

    /* Number of bytes remaining for this loop. */
    segment_to_write = mem_buff->stored[bin];
    /* Set this to zero. */
    segment_written = ZERO;

    while(segment_to_write > ZERO)
    {
      /* Record the time to start waiting for the read to occur. */
      if(gettimeofday(&start_time, NULL) < 0)
      {
	pack_return_values(write_info, 0, errno, TIME_ERROR,
			   "gettimeofday failed", 0.0, __FILE__, __LINE__,
	                   thread_locks);
	return NULL;
      }

      /* Handle calling select to wait on the descriptor. */
      if(do_select(write_info, thread_locks))
	return NULL;

      pthread_testcancel(); /* Don't grab a mutex if we should't use it. */

      /* In case something happens, make sure that the monitor thread can
       * determine that the transfer is stuck. */
      if(pthread_mutex_lock(&(thread_locks->monitor_mutex)))
      {
	pack_return_values(write_info, 0, errno, THREAD_ERROR,
			   "mutex lock failed", 0.0, __FILE__, __LINE__,
	                   thread_locks);
	return NULL;
      }
      if(gettimeofday(&(write_info->start_transfer_function), NULL) < 0)
      {
	pack_return_values(write_info, 0, errno, TIME_ERROR,
			   "gettimeofday failed", 0.0, __FILE__, __LINE__,
	                   thread_locks);
	return NULL;
      }
      if(pthread_mutex_unlock(&(thread_locks->monitor_mutex)))
      {
	pack_return_values(write_info, 0, errno, THREAD_ERROR,
			   "mutex unlock failed", 0.0, __FILE__, __LINE__,
	                   thread_locks);
	return NULL;
      }

      write_from_addr = mem_buff->buffer[bin] + segment_written;

      if(write_info->mmap_io)
      {
	 wsts = mmap_write(write_from_addr, segment_to_write, write_info,
	                   thread_locks);
      }
      else if(write_info->direct_io)
      {
	 wsts = direct_write(write_from_addr, segment_to_write, write_info,
	                     thread_locks);
      }
      else
      {
	 wsts = posix_write(write_from_addr, segment_to_write, write_info,
	                    thread_locks);
      }

      if(wsts < 0)
	 return NULL;

      pthread_testcancel(); /* Don't grab a mutex if we should't use it. */

      /* Since the write call returned, clear the timeval struct. */
      if(pthread_mutex_lock(&(thread_locks->monitor_mutex)))
      {
	pack_return_values(write_info, 0, errno, THREAD_ERROR,
			   "mutex lock failed", 0.0, __FILE__, __LINE__,
	                   thread_locks);
	return NULL;
      }
      write_info->start_transfer_function.tv_sec = 0;
      write_info->start_transfer_function.tv_usec = -1;
      if(pthread_mutex_unlock(&(thread_locks->monitor_mutex)))
      {
	pack_return_values(write_info, 0, errno, THREAD_ERROR,
			   "mutex unlock failed", 0.0, __FILE__, __LINE__,
	                   thread_locks);
	return NULL;
      }

      /* Record the time that this thread wakes up from waiting for the
       * condition variable. */
      if(gettimeofday(&end_time, NULL) < 0)
      {
	pack_return_values(write_info, 0, errno, TIME_ERROR,
			   "gettimeofday failed", 0.0, __FILE__, __LINE__,
	                   thread_locks);
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
	crc_ui = adler32(crc_ui, write_from_addr, (unsigned int)wsts);
	/*to cause intentional crc errors, use the following line instead*/
	/*crc_ui=adler32(crc_ui, buffer, sts);*/
	write_info->crc_ui = crc_ui;
	break;
      default:
	crc_ui=0U;
	write_info->crc_ui = crc_ui;
	break;
      }

      /* Update this nested loop's counting variables. */
      segment_to_write -= wsts;
      segment_written += wsts;

#ifdef DEBUG
      print_status(stderr, segment_written, segment_to_write, mem_buff,
		   write_info, thread_locks);
#endif /*DEBUG*/
    }

    /* We must remember that cleanup_segment() needs to be called before
     * write_info->bytes gets updated. */
    if(cleanup_segment(bin, mem_buff, write_info, thread_locks))
       return NULL;

    /* Tell the other thread to go if we have more bytes to go for a
     * file of known size. */
    /* The test for segment_written not being equal to zero is to exclude
     * calling thread_signal() when we have written all the bytes read for
     * a file of unknown length.  Calling thread_signal() beyond the
     * amount of data read by thread_read() would block, becuase the main
     * thread has just grabbed *all* the buffer locks.*/
    if(segment_written != ZERO)
       if(thread_signal(bin, 0, mem_buff, write_info, thread_locks))
	  return NULL;

    /* Determine where to get the data. */
    bin = (bin + 1U) % write_info->array_size;
    pthread_testcancel(); /* Don't grab a mutex if we should't use it. */
    /* Determine the number of bytes left to transfer. */
    if(pthread_mutex_lock(&(thread_locks->monitor_mutex)))
    {
      pack_return_values(write_info, 0, errno, THREAD_ERROR,
			 "mutex lock failed", 0.0, __FILE__, __LINE__,
	                 thread_locks);
      return NULL;
    }
    write_info->bytes_to_go -= segment_written;
    write_info->bytes_transfered += segment_written;
    if(pthread_mutex_unlock(&(thread_locks->monitor_mutex)))
    {
      pack_return_values(write_info, 0, errno, THREAD_ERROR,
			 "mutex unlock failed", 0.0, __FILE__, __LINE__,
	                 thread_locks);
      return NULL;
    }

    if(write_info->size == -1 && segment_written == 0)
    {
       /* This will allow the write loop to exit when we are transfering
	* a file of previously unknown size. */
       write_info->size = write_info->bytes_transfered;
    }
  }

  /* Sync the data to disk and other 'completion' steps. */
  if(finish_write(write_info, thread_locks))
    return NULL;

  /* Get total end time. */
  if(gettimeofday(&end_total, NULL) < 0)
  {
    pack_return_values(write_info, 0, errno, TIME_ERROR,
		       "gettimeofday failed", 0.0, __FILE__, __LINE__,
                       thread_locks);
    return NULL;
  }
  /* Get the thread's time usage. */
  errno = 0;
#ifdef RUSAGE_THREAD
  if(getrusage(RUSAGE_THREAD, &end_usage) < 0)
#elif RUSAGE_LWP
  if(getrusage(RUSAGE_LWP, &end_usage) < 0)
#else
  if(getrusage(RUSAGE_SELF, &end_usage) < 0)
#endif
  {
    pack_return_values(write_info, 0, errno, TIME_ERROR,
		       "getrusage failed", 0.0, __FILE__, __LINE__,
                       thread_locks);
    return NULL;
  }

  /*
   * If the descriptor is for a regular file returning the total time passed
   * for use in the rate calculation appears accurate.  Unfortunatly, this
   * method doesn't seem to return accurate time/rate information for sockets.
   * Instead socket information seems most accurate by adding the total
   * CPU time usage to the time spent in select() and read()/write().
   */
  /*
   * June 2009: Modified to be the wall clock time minus the cumulative
   * time spent waiting for the other thread for all types of files.
   * For getrusage() POSIX says that the RUSAGE_SELF times are for the
   * process, not just the thread.  The old Linux threads appears to have
   * reported times for the thread, while the newer NPTL reports for
   * the entire process (which for newer Linux kernels resulted in low
   * network rates being reported).
   */

#if (defined(RUSAGE_THREAD) || defined(RUSAGE_LWP)) && 0
  /* Only use rusage method for socket if we can get this information on
   * a per-thread basis.
   * RUSAGE_THREAD is the Linux & AIX name.  RUSAGE_LWP is the Solaris name. */
  if(S_ISSOCK(file_info.st_mode))
  {
    corrected_time = rusage_elapsed_time(&start_usage, &end_usage) +
      transfer_time;
  }
  else
#endif
  {
    corrected_time = elapsed_time(&start_total, &end_total) - thread_wait_time;
  }

  pack_return_values(write_info, crc_ui, 0, 0, "", corrected_time, NULL, 0,
                     thread_locks);

  return NULL;
}

/***************************************************************************/
/***************************************************************************/

static void do_read_write(struct transfer *read_info,
			  struct transfer *write_info)
{
  ssize_t wsts = -1;            /* Return status from write(). */
  ssize_t rsts = -1;            /* Return status from read(). */
  size_t segment_to_read;       /* Number of bytes to read in one loop. */
  size_t segment_to_write;      /* Number of bytes to write in one loop. */
  size_t segment_read;          /* Number of bytes read in one loop. */
  size_t segment_written;       /* Number of bytes written in one loop. */
#ifdef PROFILE
  struct profile profile_data[PROFILE_COUNT]; /* profile data array */
  long profile_count = 0;       /* Index of profile array. */
#endif /*PROFILE*/
  struct timeval start_time;    /* Start of time the thread is active. */
  struct timeval end_time;      /* End of time the thread is active. */
  double time_elapsed;          /* Difference between start and end time. */
  unsigned int crc_ui = 0;      /* Calculated checksum. */
  unsigned int r_crc_ui = 0;    /* Calculated checksum. */
  void *read_to_addr;           /* Holder for the read to memory address. */
  void *write_from_addr;        /* Holder for the write from memory address. */

  struct buffer mem_buff;       /* Pointers to memory buffer structures. */
  struct locks thread_locks;    /* Pointers to thread locking structures. */

  /* Initialize the thread information to zeros. */
  memset(&thread_locks, 0, sizeof(thread_locks));

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

  /* Allocate and initialize the buffer arrays. */
  if(buffer_init(&mem_buff, read_info, &thread_locks))
  {
    /* Since this error is for both reads and writes, copy it over to
     * the writes struct. */
    (void)memcpy(write_info, read_info, sizeof(read_info));
    return;
  }

  /* Get the time that the thread started to work on transfering data. */
  if(gettimeofday(&start_time, NULL) < 0)
  {
    pack_return_values(read_info, 0, errno, TIME_ERROR,
		       "gettimeofday failed", 0.0, __FILE__, __LINE__,
                       &thread_locks);
    pack_return_values(write_info, 0, errno, TIME_ERROR,
		       "gettimeofday failed", 0.0, __FILE__, __LINE__,
                       &thread_locks);
    return;
  }
  (void)memcpy(&end_time, &start_time, sizeof(struct timeval));

  /* Unfortunatly, the crc_flag struct members, may not be correct.
   * They need to be adjusted in cases of memory mapped io being used. */
  /* When writing to disk with memory mapped io we need to turn on the
   * CRC calulation during the reads. */
  read_info->crc_flag |= (write_info->crc_flag && write_info->mmap_io);
  /* When writing to disk with memory mapped io we need to turn off
   * the CRC calculation during the writes. */
  write_info->crc_flag = write_info->crc_flag && !(write_info->mmap_io);
  /* Set these four values for the situation when mmap io is in use. */
  read_info->other_mmap_io = write_info->mmap_io;
  write_info->other_mmap_io = read_info->mmap_io;
  read_info->other_fd = write_info->fd;
  write_info->other_fd = read_info->fd;

  while( (read_info->bytes_to_go > 0 && write_info->bytes_to_go > 0) ||
	 (read_info->size == -1) )
  {
    /* Get the next memory (either malloc/memalign or mmap) segment. */

    if(read_info->mmap_io && write_info->mmap_io)
    {
       /* Allocate the next mmap() regions. */
       if(get_next_segments(&mem_buff, read_info, &thread_locks) == 0)
       {
	  return;
       }
    }
    /* Allocate the next buffer to place data into. */
    else if(get_next_segment(0, &mem_buff, read_info, &thread_locks) == NULL)
    {
       return;
    }

    /* Number of bytes remaining for this loop. */
    if(read_info->mmap_io || write_info->mmap_io)
    {
       if(read_info->bytes_to_go > -1)
	  segment_to_read = (size_t)min2ull(
	     (unsigned long long)read_info->bytes_to_go,
	     (unsigned long long)read_info->mmap_size);
       else
	  segment_to_read = (unsigned long long)read_info->mmap_size;
    }
    else
    {
       if(read_info->bytes_to_go > -1)
	  segment_to_read = (size_t)min2ull(
	  (unsigned long long)read_info->bytes_to_go,
	  (unsigned long long)read_info->block_size);
       else
	  segment_to_read = (unsigned long long)read_info->block_size;
    }

    /* Set this to zero. */
    segment_read = ZERO;

    while(segment_to_read > ZERO)
    {
#ifdef PROFILE
      update_profile(1, segment_to_read, read_info->fd,
		     profile_data, &profile_count);
#endif /*PROFILE*/

      /* Handle calling select to wait on the descriptor. */
      if(do_select(read_info, &thread_locks))
	return;

#ifdef PROFILE
      update_profile(2, segment_to_read, read_info->fd,
		     profile_data, &profile_count);
#endif /*PROFILE*/

#ifdef PROFILE
      update_profile(3, segment_to_read, read_info->fd,
		     profile_data, &profile_count);
#endif /*PROFILE*/

      /*
       * Depending on the mechanism used to write out the data, different
       * memory locations need to be read into.  For POSIX and Direct I/O
       * the data is read into the buffer; for memory mapped writes it
       * is read directly into the memory mapped file.
       */
      /* These values will change with each iteration, don't get over
       * ambitous and try to move this out of the loop. */
      read_to_addr = (void*)((uintptr_t)mem_buff.buffer[0] + segment_read);

      /* Read in the data. */
      if(read_info->mmap_io && write_info->mmap_io)
      {
	 /* In this case buffer[0] holds the destination mmap address
	  * and buffer[1] holds the source mmap address. */
	 (void)memcpy(read_to_addr,
		      (void*)((uintptr_t)mem_buff.buffer[1] + segment_read),
		      segment_to_read);
	 rsts = segment_to_read;
      }
      else if(read_info->mmap_io)
      {
	 /* Tells kernel to preread the file into cache. */
	 rsts = mmap_read(read_to_addr, segment_to_read, read_info,
			  &thread_locks);
      }
      else if(read_info->direct_io)
      {
	 rsts = direct_read(read_to_addr, segment_to_read, read_info,
	                    &thread_locks);
      }
      else
      {
	 rsts = posix_read(read_to_addr, segment_to_read, read_info,
	                   &thread_locks);
      }

      if(rsts < 0)
	 return;

      switch (read_info->crc_flag)
      {
      case 0:
	break;
	 case 1:
	 r_crc_ui = adler32(r_crc_ui, read_to_addr, (unsigned int)rsts);
	 break;
      default:
	r_crc_ui = 0;
	break;
      }

#ifdef PROFILE
      update_profile(4, rsts, read_info->fd,
		     profile_data, &profile_count);
#endif /*PROFILE*/

      /* Update this nested loop's counting variables. */
      if(rsts == 0)
	 segment_to_read = ZERO;
      else
	 segment_to_read -= rsts;
      segment_read += rsts;

#ifdef DEBUG
      *(mem_buff.stored) = segment_read;
      read_info->crc_ui = r_crc_ui;
      print_status(stderr, segment_read, segment_to_read, &mem_buff,
		   read_info, &thread_locks);
#endif /*DEBUG*/
    }

    read_info->bytes_to_go -= segment_read;
    read_info->bytes_transfered += segment_read;

    /* Initialize the write loop variables. */
    segment_to_write = segment_read;
    /* Set this to zero. */
    segment_written = ZERO;

    while (segment_to_write > ZERO)
    {
#ifdef PROFILE
      update_profile(5, segment_to_write, write_info->fd,
		     profile_data, &profile_count);
#endif /*PROFILE*/

      /* Handle calling select to wait on the descriptor. */
      if(do_select(write_info, &thread_locks))
	return;

#ifdef PROFILE
      update_profile(6, segment_to_write, write_info->fd,
		     profile_data, &profile_count);
#endif /*PROFILE*/

#ifdef PROFILE
      update_profile(7, segment_to_write, write_info->fd,
		     profile_data, &profile_count);
#endif /*PROFILE*/

      /*
       * Depending on the mechanism used to read in the data, different
       * memory locations need to be written from.  For POSIX and Direct I/O
       * the data is written from the buffer; for memory mapped reads it
       * is read directly from the memory mapped file.
       */
      /* These values will change with each iteration, don't get over
       * ambitous and try to move this out of the loop. */
      write_from_addr = (void*)((uintptr_t)mem_buff.buffer[0] + segment_written);

      if(write_info->mmap_io)
      {
	 /* Tells the kernel to get the information to disk ASAP. */
	 wsts = mmap_write(write_from_addr, segment_to_write, write_info,
	                   &thread_locks);
      }
      else if(write_info->direct_io)
      {
	 wsts = direct_write(write_from_addr, segment_to_write, write_info,
	                     &thread_locks);
      }
      else
      {
	 wsts = posix_write(write_from_addr, segment_to_write, write_info,
	                    &thread_locks);
      }

      if(wsts < 0)
	 return;

#ifdef PROFILE
      update_profile(8, wsts, write_info->fd,
		     profile_data, &profile_count);
#endif /*PROFILE*/

      switch (write_info->crc_flag)
      {
      case 0:
	break;
	 case 1:
	 crc_ui=adler32(crc_ui, write_from_addr, (unsigned int)wsts);
	 break;
      default:
	crc_ui = 0;
	break;
      }

      /* Handle calling select to wait on the descriptor. */
      segment_to_write -= wsts;
      segment_written += wsts;

#ifdef DEBUG
      *(mem_buff.stored) = segment_written;
      write_info->crc_ui = crc_ui;
      print_status(stderr, segment_written, segment_to_write, &mem_buff,
		   write_info, &thread_locks);
#endif /*DEBUG*/
    }

    /* We must remember that cleanup_segment() needs to be called before
     * write_info->bytes gets updated. */
    if(read_info->mmap_io && write_info->mmap_io)
    {
       if(cleanup_segments(&mem_buff, write_info, &thread_locks))
	  return;
    }
    else if(cleanup_segment(0, &mem_buff, write_info, &thread_locks))
       return;

    write_info->bytes_to_go -= segment_written;
    write_info->bytes_transfered += segment_written;
  }

  /* Sync the data to disk and other 'completion' steps. */
  if(finish_write(write_info, &thread_locks))
    return;
  if(finish_read(read_info, &thread_locks))
    return;

  /* Get the time that the thread finished to work on transfering data. */
  if(gettimeofday(&end_time, NULL) < 0)
  {
    pack_return_values(read_info, 0, errno, TIME_ERROR,
		       "gettimeofday failed", 0.0, __FILE__, __LINE__,
                       &thread_locks);
    pack_return_values(write_info, 0, errno, TIME_ERROR,
		       "gettimeofday failed", 0.0, __FILE__, __LINE__,
                       &thread_locks);
    return;
  }
  time_elapsed = elapsed_time(&start_time, &end_time);

  free(mem_buff.buffer);
#ifdef DEBUG
  free(mem_buff.stored);
#endif /*DEBUG*/

#ifdef PROFILE
  print_profile(profile_data, profile_count);
#endif /*PROFILE*/

  /* For memory mapped io, remember the correct value twice. */
  if(write_info->mmap_io)
     crc_ui = r_crc_ui;
  else if(read_info->mmap_io)
     r_crc_ui = crc_ui;

  pack_return_values(write_info, crc_ui, 0, 0, "", time_elapsed, NULL, 0,
                     &thread_locks);
  pack_return_values(read_info, r_crc_ui, 0, 0, "", time_elapsed, NULL, 0,
                     &thread_locks);
  return;
}

/***************************************************************************
 * python defined functions
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
		      PyLong_FromLongLong(rtn_val->bytes_to_go),
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
EXfd_quotas(PyObject *self, PyObject *args)
{
#ifdef Q_GETQUOTA
  char *file_target;
  struct dqblk user_quota;
  struct dqblk group_quota;
  char correct_block_device[PATH_MAX + 1];
  char correct_mount_point[PATH_MAX + 1];
  int sts;
  int user_rc, group_rc;  /* rc == Return Code */
  PyObject *my_user_quotas;
  PyObject *my_group_quotas;
  PyObject *quota_list = PyList_New(0);

  /* Get the parameter. */
  sts = PyArg_ParseTuple(args, "s", &file_target);
  if (!sts)
     return(raise_exception("fd_quotas - invalid parameter"));

  (void) memset(&user_quota, 0, sizeof(struct dqblk));
  (void) memset(&group_quota, 0, sizeof(struct dqblk));

#ifdef __APPLE__
   /* MacOS X has a different quotactl() call.  We don't need to find
    * the block device file name. */
   memcpy(correct_block_device, file_target, PATH_MAX + 1);
#else
  /*
   * Obtain the quotas of the filesystem file_target is on.
   */

  if(get_bd_name(file_target, correct_block_device, (size_t) PATH_MAX,
		 correct_mount_point, (size_t) PATH_MAX) < 0)
  {
     if(errno == ESRCH)
     {
	/* AFS filesystems will give this error, though there are other
	 * ways to get this. */

        /* Quotas are not available. */
        return PyList_New(0);
     }
     else
     {
        char message[10000];
	snprintf(message, (size_t) 10000,
		 "fd_quotas - block device not found: %s: %s",
		 strerror(errno), file_target);
	return(raise_exception(message));
     }
  }
#endif /* __APPLE__ */

  /*
   * Obtain the quotas for the file.  First look for any user quotas.
   * Then look for any group quotas.
   */

  if((user_rc = get_quotas(correct_block_device,
			  USER_QUOTA, &user_quota)) == 0)
  {
     my_user_quotas = Py_BuildValue("(O,O,O,O,O,O,O,O)",
	PyLong_FromUnsignedLong((unsigned long)user_quota.dqb_bhardlimit),
	PyLong_FromUnsignedLong((unsigned long)user_quota.dqb_bsoftlimit),
	PyLong_FromUnsignedLong((unsigned long)user_quota.dqb_curblocks),
	PyLong_FromUnsignedLong((unsigned long)user_quota.dqb_fhardlimit),
	PyLong_FromUnsignedLong((unsigned long)user_quota.dqb_fsoftlimit),
	PyLong_FromUnsignedLong((unsigned long)user_quota.dqb_curfiles),
	PyLong_FromUnsignedLong((unsigned long)user_quota.dqb_btimelimit),
	PyLong_FromUnsignedLong((unsigned long)user_quota.dqb_ftimelimit)
	);

     PyList_Append(quota_list, my_user_quotas);
  }
  if((group_rc = get_quotas(correct_block_device,
			   GROUP_QUOTA, &group_quota)) == 0)
  {
     my_group_quotas = Py_BuildValue("(O,O,O,O,O,O,O,O)",
	PyLong_FromUnsignedLong((unsigned long)group_quota.dqb_bhardlimit),
	PyLong_FromUnsignedLong((unsigned long)group_quota.dqb_bsoftlimit),
	PyLong_FromUnsignedLong((unsigned long)group_quota.dqb_curblocks),
	PyLong_FromUnsignedLong((unsigned long)group_quota.dqb_fhardlimit),
	PyLong_FromUnsignedLong((unsigned long)group_quota.dqb_fsoftlimit),
	PyLong_FromUnsignedLong((unsigned long)group_quota.dqb_curfiles),
	PyLong_FromUnsignedLong((unsigned long)group_quota.dqb_btimelimit),
	PyLong_FromUnsignedLong((unsigned long)group_quota.dqb_ftimelimit)
	);

     PyList_Append(quota_list, my_group_quotas);
  }

  return quota_list;
#else
  /* Quotas are not available. */
  return PyList_New(0);
#endif /* Q_GETQUOTA */
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
    else if (no_bytes_obj == Py_None)
        no_bytes = -1; /* File size not know, likely a FIFO for input. */
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

    /* Place the values into the struct.  Some compilers complained when this
     * information was placed into the struct inline at initalization.  So it
     * was moved here. */
    (void)memset(&reads, 0, sizeof(reads));
    (void)memset(&writes, 0, sizeof(writes));
    reads.fd = fr_fd;
    reads.size = no_bytes;
    reads.bytes_to_go = no_bytes;
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
    reads.advisory_locking = 1;
    writes.fd = to_fd;
    writes.size = no_bytes;
    writes.bytes_to_go = no_bytes;
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
    writes.advisory_locking = 1;

    Py_BEGIN_ALLOW_THREADS
    errno = 0;
    if(threaded_transfer)
      do_read_write_threaded(&reads, &writes);
    else
      do_read_write(&reads, &writes);
    Py_END_ALLOW_THREADS

#if 0
    printf("read crc: %ud\n", reads.crc_ui);
    printf("write crc: %ud\n", writes.crc_ui);
#endif

    /*
     * If the write error is ECANCELED then use the read error, because
     * this indicates that the read thread exited first and the ECANCELED
     * from the write thread means it knew to exit early.
     */

    if (writes.exit_status != 0 && writes.errno_val != ECANCELED)
        return (raise_exception2(&writes));
    else if (reads.exit_status != 0)
        return (raise_exception2(&reads));

    rr = Py_BuildValue("(i,O,O,i,s,O,O,s,i)",
		       writes.exit_status,
		       PyLong_FromUnsignedLong(writes.crc_ui),
                       PyLong_FromLongLong(writes.bytes_to_go),
		       writes.errno_val, writes.msg,
		       PyFloat_FromDouble(reads.transfer_time),
		       PyFloat_FromDouble(writes.transfer_time),
		       writes.filename, writes.line);

    return rr;
}


/***************************************************************************
 * inititalization
 **************************************************************************
 *   Module initialization.   Python call the entry point init<module name>
 *   when the module is imported.  This should the only non-static entry point
 *   so it is exported to the linker.
 *
 *   First argument must be a the module name string.
 *
 *   Second       - a list of the module methods
 *
 *   Third	- a doumentation string for the module
 *
 *   Fourth & Fifth - see Python/modsupport.c
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

    /* Add members to the modules.  These members are the positional
     * indexes for the quota tuple. */
    PyModule_AddObject(m, "BLOCK_HARD_LIMIT",
		       PyLong_FromUnsignedLong(BLOCK_HARD_LIMIT));
    PyModule_AddObject(m, "BLOCK_SOFT_LIMIT",
		       PyLong_FromUnsignedLong(BLOCK_SOFT_LIMIT));
    PyModule_AddObject(m, "CURRENT_BLOCKS",
		       PyLong_FromUnsignedLong(CURRENT_BLOCKS));
    PyModule_AddObject(m, "FILE_HARD_LIMIT",
		       PyLong_FromUnsignedLong(FILE_HARD_LIMIT));
    PyModule_AddObject(m, "FILE_SOFT_LIMIT",
		       PyLong_FromUnsignedLong(FILE_SOFT_LIMIT));
    PyModule_AddObject(m, "CURRENT_FILES",
		       PyLong_FromUnsignedLong(CURRENT_FILES));
    PyModule_AddObject(m, "BLOCK_TIME_LIMIT",
		       PyLong_FromUnsignedLong(BLOCK_TIME_LIMIT));
    PyModule_AddObject(m, "FILE_TIME_LIMIT",
		       PyLong_FromUnsignedLong(FILE_TIME_LIMIT));
}

#else
/* Stand alone version of exfer is prefered. */

/***************************************************************************/

static int pages_in_core(char* abspath)
{
#if defined ( __linux__ ) || defined ( __sun ) || defined ( __APPLE__ )
   struct stat file_info;
   size_t size;
   void *mmap_ptr;
   int fd;
   size_t page_size;
   unsigned long vector_size;
#ifdef __linux__
   unsigned char *vec;
#else
   char *vec;
#endif
   unsigned long in_core = 0;
   unsigned long i;

   if((fd = open(abspath, O_RDWR)) < 0)
   {
      if(errno == EPERM)
      {
	 return 0;
      }
      return 1;
   }

   if(fstat(fd, &file_info) < 0)
   {
      (void)close(fd);
      return 1;
   }
   if(!S_ISREG(file_info.st_mode))
   {
      (void)close(fd);
      return 1;
   }

   size = min2ull((unsigned long long) SIZE_MAX,
		  (unsigned long long) file_info.st_size);

  /* Start by opening the entire file (or SIZE_MAX if it is to big).*/
  errno = 0;
  if((mmap_ptr = mmap(NULL, size, PROT_READ | PROT_WRITE,
		      MAP_SHARED, fd, (off_t)0)) == MAP_FAILED)
  {
     if(errno == ENODEV || errno == EPERM)
     {
	return 0;
     }
     return 1;
  }

  /* Determine how many pages of the input file are currently in the core. */
  page_size = (size_t)sysconf(_SC_PAGESIZE);
  vector_size = (size + page_size - 1) / page_size;
  if((vec = calloc(1, vector_size)) != NULL)
  {
     if(mincore(mmap_ptr, size, vec) == 0)
     {
	for(i = 0; i <= vector_size; i++)
	{
	   in_core += (vec[i] & 1U);
	}
	printf("%llu pages of input file out of %llu (%.2f%%) are in core.\n",
	       (unsigned long long)(in_core),
	       (unsigned long long)vector_size,
	       (double)in_core / (double)vector_size * 100);
     }
     free(vec);
  }

  /* cleanup */
  if(munmap(mmap_ptr, size) < 0)
  {
     return 1;
  }

  (void) close(fd);

#endif /* __linux__ & __sun */
  return 0;
}

/* This section contains various functions for clearing file buffer caches. */

static int invalidate_cache_posix(char* abspath)
{
   struct stat file_info;
   size_t size;
   void *mmap_ptr;
   int fd;
#if defined ( __linux__ ) || defined ( __sun )
   size_t page_size;
   unsigned long vector_size;
   unsigned char *vec;
   unsigned long in_core = 0;
   unsigned long i;
#endif

   if((fd = open(abspath, O_RDWR)) < 0)
   {
      if(errno == EPERM)
      {
	 return 0;
      }
      return 1;
   }

   if(fstat(fd, &file_info) < 0)
   {
      (void)close(fd);
      return 1;
   }
   if(!S_ISREG(file_info.st_mode))
   {
      (void)close(fd);
      return 1;
   }

#if defined ( _POSIX_ADVISORY_INFO ) && _POSIX_ADVISORY_INFO >= 200112L
  /* If the file descriptor supports fadvise, tell the kernel to nuke
   * the file's buffer cache. */
   if(posix_fadvise(fd, (off_t)0ULL, (off_t)0ULL, POSIX_FADV_DONTNEED) < 0)
  {
     if(errno != EINVAL && errno != ESPIPE && errno != ENOSYS)
     {
	(void)close(fd);
	return 1;
     }
  }
#else
  size = min2ull((unsigned long long) SIZE_MAX,
		 (unsigned long long) file_info.st_size);

  /* Start by opening the entire file (or SIZE_MAX if it is to big).*/
  errno = 0;
  if((mmap_ptr = mmap(NULL, size, PROT_READ | PROT_WRITE,
		      MAP_SHARED, fd, (off_t)0ULL)) == MAP_FAILED)
  {
     if(errno == ENODEV || errno == EPERM)
     {
	return 0;
     }
     return 1;
  }

  /* Create some dirty pages. */
  /*(void) memmove(mmap_ptr, mmap_ptr, size);*/

  /* This trick came from a Sun Microsystems web page.  The only issue there
   * might be is that Sun's man page (and SGI's too) state that
   * MS_INVALIDATE removes all pages of the file from memory.  While POSIX
   * says that it shall remove the out-of-date pages from cache, and doesn't
   * say anything about clean pages. */
  (void) msync(mmap_ptr, size, MS_SYNC | MS_INVALIDATE);

  /* cleanup */
  if(munmap(mmap_ptr, size) < 0)
  {
     return 1;
  }

  (void) close(fd);
#endif /*_POSIX_ADVISORY_INFO*/

  return 0;
}

/* This function must be called before any other fd to file named in
 * abspath is opened. */
#ifdef __sgi
static int invalidate_cache_irix(char* abspath)
{
#ifdef O_LCINVAL
  struct stat file_info;
  int rtn_fcntl = 0;
  int new_fcntl = 0;
  int fd;

  if((fd = open(abspath, O_RDONLY)) < 0)
     return 1;

  if(fstat(fd, &file_info) < 0)
  {
     (void)close(fd);
     return 1;
  }
  if(!S_ISREG(file_info.st_mode))
  {
     (void)close(fd);
     return 1;
  }

  /* Get the current file descriptor flags. */
  errno = 0;
  if((rtn_fcntl = fcntl(fd, F_GETFL, 0)) < 0)
  {
     (void)fprintf(stderr, "Unable to flush buffer cache: %s.\n",
		   strerror(errno));
     (void)close(fd);
     return 1;
  }

  new_fcntl = rtn_fcntl | O_LCINVAL;  /* turn on O_LCINVAL */

  /* Set the new file descriptor flags. */
  errno = 0;
  if(fcntl(fd, F_SETFL, (long)new_fcntl) < 0)
  {
     (void)fprintf(stderr, "Unable to flush buffer cache: %s.\n",
		   strerror(errno));
     (void)close(fd);
     return 1;
  }

  /* This is where the magic occurs for O_LCFLUSH.  When the last descriptor
   * closes the file, that file's buffer cash is cleared.  Make sure this
   * is called before the real descriptor is opened. */
  errno = 0;
  if(close(fd) < -1)
  {
     (void)fprintf(stderr, "Unable to flush buffer cache: %s.\n",
		   strerror(errno));
     return 1;
  }
#endif /* O_LCINVAL */

  return 0;
}
#endif /* __sgi */

#ifdef __linux__
static int invalidate_cache_linux(char* abspath)
{
#ifdef BLKFLSBUF
  char mountpoint[PATH_MAX + 1];
  char* mtpt;
  char dircopy[PATH_MAX + 1];
  struct stat mtpt_info;
  struct stat file_info;

  FILE* mtab_fp;
  char mountline[1024];
  void* ptr1;
  void* ptr2;
  char device_name[PATH_MAX + 1];

  int device_fd;

  if(geteuid() != 0) /* Skip this if we are not root. */
     return 1;

  if(stat(abspath, &file_info) < 0)
     return 1;
  if(!S_ISREG(file_info.st_mode))
     return 1;

  (void)memcpy(dircopy, abspath, strlen(abspath) + 1);
  (void)memcpy(mountpoint, abspath, strlen(abspath) + 1);
  while((mtpt = dirname(dircopy)) != NULL)
  {
     if(stat(mtpt, &mtpt_info) < 0)
     {
	(void)fprintf(stderr, "Unable to flush buffer cache: %s.\n",
		      strerror(errno));
	(void)memset(mountpoint, 0, 4096);
	break;
     }

     if(mtpt_info.st_dev != file_info.st_dev)
     {
	break;
     }
     else if(strcmp(mtpt, "/") == 0)
     {
	break;
     }
     (void)memmove(dircopy, mtpt, strlen(mtpt) + 1);
     (void)memcpy(mountpoint, mtpt, strlen(mtpt) + 1);
  }

  if(strlen(mountpoint) > 0)
  {
     /*printf("Mountpoint is: %s\n", mountpoint);*/

     if((mtab_fp = fopen("/etc/mtab", "r")) == NULL)
     {
	(void)fprintf(stderr, "Unable to flush buffer cache: %s.\n",
		      strerror(errno));
	return 1;
     }
     else
     {
	(void)memset(device_name, 0 , PATH_MAX);
	while(fgets(mountline, 1023, mtab_fp) != NULL)
	{
	   if((ptr1 = strtok(mountline, " ")) != NULL)
	      if((ptr2 = strtok(NULL, " ")) != NULL)
		 if(strcmp(ptr2, mountpoint) == 0)
		 {
		    (void)memcpy(device_name, ptr1, strlen(ptr1) + 1);
		    break;
		 }

	}
	(void)fclose(mtab_fp);
     }
  }

  if(strlen(device_name) > 0)
  {
     /*printf("Device name: %s\n", device_name);*/

     errno = 0;
     if((device_fd = open(device_name, O_RDONLY)) < 0)
     {
	if(errno != EINVAL)
	{
	   (void)fprintf(stderr, "Unable to flush buffer cache: %s.\n",
			 strerror(errno));
	   return 1;
	}
     }
     else
     {
	sync();

	errno = 0;
	if(ioctl(device_fd, BLKFLSBUF, 0) < 0)
	{
	   if(errno != EINVAL)
	   {
	      (void)fprintf(stderr, "Unable to flush buffer cache: %s.\n",
			    strerror(errno));
	      (void)close(device_fd);
	      return 1;
	   }
	}

	(void)close(device_fd);
     }
  }

#endif
  return 0;
}
#endif /* __linux__ */

/***************************************************************************/

/* Determine if a filesystem supports mandatory file locks. */
int mandatory_lock_test(char *filepath, int verbose)
{
   int pid;                /* pid from fork().  (mandatory lock test) */
   int wait_rtn;           /* Exit status from child.  (mandatory lock test) */

   char *directory, *filename, *dir_s, *file_s;
   char use_filename[NAME_MAX] = "..";
   char use_pathname[PATH_MAX] = "";
   char buffer[5];

   int fd;
   int rtn_fcntl, rtn_read;
   struct flock filelock;
   struct stat file_stat;

   /*
    * Get the temporary filename for our test file.
    */

   dir_s = strdup(filepath);
   file_s = strdup(filepath);

   if(dir_s == NULL || file_s == NULL)
   {
      if(verbose)
      {
	 (void)fprintf(stderr,
		       "Mandatory write test memory allocation failed: %s\n",
		       strerror(errno));
      }
      return 2;
   }

   directory = dirname(dir_s);
   filename = basename(file_s);

   strncat(use_filename, filename, NAME_MAX - 2);

   strncat(use_pathname, directory, PATH_MAX);
   strncat(use_pathname, "/", 1);
   strncat(use_pathname, use_filename, NAME_MAX);

   free(dir_s);
   free(file_s);

   /* Try opening/creating the sample file. */
   if((fd = open(use_pathname, O_RDWR | O_CREAT | O_NONBLOCK,
		 S_IRUSR | S_IWUSR | S_IRGRP | S_ISGID)) < 0)
   {
      if(verbose)
      {
	 (void)fprintf(stderr,
		       "Mandatory write test open() failed: %s\n",
		       strerror(errno));
      }
      return 2;
   }
   if(fstat(fd, &file_stat) < 0)
   {
      if(verbose)
      {
	 (void)fprintf(stderr,
		       "Mandatory write test fstat() failed: %s\n",
		       strerror(errno));
      }
      return 2;
   }
   if(fchmod(fd, (file_stat.st_mode & ~S_IXGRP) | S_ISGID) < 0)
   {
      if(verbose)
      {
	 (void)fprintf(stderr,
		       "Mandatory write test fchmod() failed: %s\n",
		       strerror(errno));
      }
      return 2;
   }

   /* Try getting the requested file lock. */
   errno = 0;
   filelock.l_whence = SEEK_SET;
   filelock.l_start = 0L;
   filelock.l_type = F_WRLCK;
   filelock.l_len = 0L;
   if((rtn_fcntl = fcntl(fd, F_SETLK, &filelock)) < 0)
   {
      if(verbose)
      {
	 (void)fprintf(stderr,
		       "Mandatory write test fcntl(F_SETLK) failed: %s\n",
		       strerror(errno));
      }
      return 2;
   }

   /*
    * Fork a child.  This child will attempt read the file we have just
    * locked.  If it succeeds, then no manditory locking.  If it gets
    * EAGAIN or EACCES then we have manditory locking.  Anything else
    * means got some other error.
    */
   if((pid = fork()) < 0)
   {
      if(verbose)
      {
	 (void)fprintf(stderr,
		       "Mandatory write test fork() failed: %s\n",
		       strerror(errno));
      }
      return 2;
   }
   else if(pid == 0) /* CHILD */
   {
      if((rtn_read = read(fd, buffer, 2)) < 0)
      {
	 if(errno == EAGAIN || errno == EACCES)
	 {
	    /* Mandatory locks supported. */
	    (void)close(fd);
	    exit(0);
	 }
	 (void)close(fd);
	 exit(2); /* Got another error. */
      }
      /* Mandatory locks not supported. */
      (void)close(fd);
      exit(1);
   }
   else /* PARENT */
   {
      (void)waitpid(pid, &wait_rtn, 0);

      /* Do some cleanup. */
      (void)close(fd);
      (void)unlink(use_pathname);

      /* Process the exit status of the child process. */
      if(WIFEXITED(wait_rtn))
      {
	 return WEXITSTATUS(wait_rtn);
      }
      else if(WIFSIGNALED(wait_rtn))
      {
	 if(verbose)
	 {
	    (void)fprintf(stderr,
			  "Mandatory write test child process failed with "
			  "signal: %d\n", WTERMSIG(wait_rtn));
	 }
	 return 2;
      }
   }
   return 2; /* Is this possible? */
}


/***************************************************************************/

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
  int mode_out = 0;
  int opt;
  int rtn;
  int          cache             = 0;
  int          verbose           = 0;
  size_t       block_size        = 256*1024;
  size_t       array_size        = 3;
  size_t       mmap_size         = 256*1024;
  int          direct_io         = 0;
  int          mmap_io           = 0;
  int          synchronous_io    = 0;
  int          d_synchronous_io  = 0;
  int          r_synchronous_io  = 0;
  int          threaded_transfer = 0;
  int          advisory_locking  = 0;
  int          mandatory_locking = 0;
  int          ecrc              = 0;
  struct transfer reads;
  struct transfer writes;
  char abspath[PATH_MAX + 1];
  int direct_io_index            = 0;
  int direct_io_in               = 0;
  int direct_io_out              = 0;
  int mmap_io_index              = 0;
  int mmap_io_in                 = 0;
  int mmap_io_out                = 0;
  int synchronous_io_in          = 0;
  int synchronous_io_out         = 0;
  int synchronous_io_index       = 0;
  int d_synchronous_io_in        = 0;
  int d_synchronous_io_out       = 0;
  int d_synchronous_io_index     = 0;
  int r_synchronous_io_in        = 0;
  int r_synchronous_io_out       = 0;
  int r_synchronous_io_index     = 0;
  int advisory_locking_in        = 0;
  int advisory_locking_out       = 0;
  int advisory_locking_index     = 0;
  int mandatory_locking_in       = 0;
  int mandatory_locking_out      = 0;
  int mandatory_locking_index    = 0;
  int first_file_optind          = 0;
  int second_file_optind         = 0;

  opterr = 0;
  while(optind < argc)
  {
    /* The + for the first character in optstring is need on Linux machines
     * to tell getopt to use the posix compliant version of getopt(). */
    while(((opt = getopt(argc, argv, "+cevtmdSDRAMa:b:l:")) != -1))
    {
      switch(opt)
      {
      case 'c':
	 cache = 1;  /* clear out the file cache before the test. */
	 break;
      case 'v':
	verbose = 1; /* print out extra information. */
	break;
      case 'e': /* perform a complete reread and CRC check of the data. */
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
      case 'S': /* synchronous i/o */
	synchronous_io += 1;
	if(synchronous_io_index == 0)
	  synchronous_io_index = optind - 1;
	break;
      case 'D': /* d_synchronous i/o */
	d_synchronous_io += 1;
	if(d_synchronous_io_index == 0)
	  d_synchronous_io_index = optind - 1;
	break;
      case 'R': /* r_synchronous i/o */
	r_synchronous_io += 1;
	if(r_synchronous_io_index == 0)
	  r_synchronous_io_index = optind - 1;
	break;
      case 'A': /* advisory locking */
	advisory_locking += 1;
	if(advisory_locking_index == 0)
	  advisory_locking_index = optind - 1;
	break;
      case 'M': /* mandatory locking */
	mandatory_locking += 1;
	if(mandatory_locking_index == 0)
	  mandatory_locking_index = optind - 1;
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
     * This bumps the optind up one so it can continue. */
    if(optind < argc)
      optind++;
  }

  /* Determine if the mmap io was for the input file, output file or both. */
  if((mmap_io == 1) && (mmap_io_index < first_file_optind))
     mmap_io_in = 1;
  else if(mmap_io == 1)
     mmap_io_out = 1;
  else if(mmap_io > 1)
     mmap_io_in = mmap_io_out = 1;

  /* Determine if the direct io was for the input file, output file or both. */
  if((direct_io == 1) && (direct_io_index < first_file_optind))
     direct_io_in = 1;
  else if(direct_io == 1)
     direct_io_out = 1;
  else if(direct_io > 1)
     direct_io_in = direct_io_out = 1;

  /* Determine if the sync. io was for the input file, output file or both. */
  if((synchronous_io == 1) && (synchronous_io_index < first_file_optind))
     synchronous_io_in = 1;
  else if(synchronous_io == 1)
     synchronous_io_out = 1;
  else if(synchronous_io > 1)
     synchronous_io_in = synchronous_io_out = 1;

  /* Determine if the dsync. io was for the input file, output file or both. */
  if((d_synchronous_io == 1) && (d_synchronous_io_index < first_file_optind))
     d_synchronous_io_in = 1;
  else if(d_synchronous_io == 1)
     d_synchronous_io_out = 1;
  else if(d_synchronous_io > 1)
     d_synchronous_io_in = d_synchronous_io_out = 1;

  /* Determine if the rsync. io was for the input file, output file or both. */
  if((r_synchronous_io == 1) && (r_synchronous_io_index < first_file_optind))
     r_synchronous_io_in = 1;
  else if(r_synchronous_io == 1)
     r_synchronous_io_out = 1;
  else if(r_synchronous_io > 1)
     r_synchronous_io_in = r_synchronous_io_out = 1;

  /* Determine if the advisory locking was for the input file, output file
   * or both. */
  if((advisory_locking == 1) && (advisory_locking_index < first_file_optind))
     advisory_locking_in = 1;
  else if(advisory_locking == 1)
     advisory_locking_out = 1;
  else if(advisory_locking > 1)
     advisory_locking_in = advisory_locking_out = 1;

  /* Determine if the advisory locking was for the input file, output file
   * or both. */
  if((mandatory_locking == 1) && (mandatory_locking_index < first_file_optind))
     mandatory_locking_in = 1;
  else if(mandatory_locking == 1)
     mandatory_locking_out = 1;
  else if(mandatory_locking > 1)
     mandatory_locking_in = mandatory_locking_out = 1;

  /* Determine the flags for the input file descriptor. */
  flags_in |= O_RDONLY;

  /* Determine the flags for the output file descriptor. */
  flags_out |= O_CREAT | O_TRUNC;

  if(mmap_io_out || ecrc)
     flags_out |= O_RDWR;
  else
     flags_out |= O_WRONLY;

  mode_out = S_IRUSR | S_IWUSR | S_IRGRP;
  if(mandatory_locking_out)
     mode_out |= S_ISGID;

  /* Check the number of arguments from the command line. */
  if(argc < 3)
  {
    (void)strncpy(abspath, argv[0], PATH_MAX);
    (void)fprintf(stderr,
		  "Usage: %s [-cevt] [-a <# of buffers>] [-b <buffer size>]\n"
		  "       [-l <mmap buffer size>] "
		  "[-dmSDRAM] <source_file> [-dmSDRAM] <dest_file>\n",
		  basename(abspath));
    return 1;
  }

  /*
   * Determine if filesystems in question support mandatory file locking.
   */
  if(mandatory_locking_in)
  {
     rtn = mandatory_lock_test(argv[first_file_optind], verbose);
     if(rtn)
     {
	/*
	 * revert to using advisory locks
	 */

	char *string_ptr;

	if(rtn == 1)
	   string_ptr = (char*) no_mandatory_file_locking;
	else
	   string_ptr = (char*) unknown_mandatory_file_locking;

#ifdef DEBUG_REVERT
	(void)fprintf(stderr, "%s:  %s", argv[first_file_optind], string_ptr);
#endif /*DEBUG_REVERT*/
	mandatory_locking_in = 0;
	advisory_locking_in = 1;
     }
  }
  if(mandatory_locking_out)
  {
     rtn = mandatory_lock_test(argv[second_file_optind], verbose);
     if(rtn)
     {
	/*
	 * revert to using advisory locks
	 */

	char *string_ptr;

	if(rtn == 1)
	   string_ptr = (char*) no_mandatory_file_locking;
	else
	   string_ptr = (char*) unknown_mandatory_file_locking;

#ifdef DEBUG_REVERT
	(void)fprintf(stderr, "%s:  %s", argv[second_file_optind], string_ptr);
#endif /*DEBUG_REVERT*/
	mandatory_locking_out = 0;
	advisory_locking_out = 1;
     }
  }

  if(verbose)
  {
     (void)printf("Invalidate cache: %s\n", ON_OFF(cache));
     (void)printf("Threaded: %s\n", ON_OFF(threaded_transfer));
     (void)printf("Ecrc: %s\n", ON_OFF(ecrc));
     (void)printf("Block size: %d\n", block_size);
     (void)printf("Array size: %d\n", array_size);
     (void)printf("Mmap size: %u\n", mmap_size);
     (void)printf("Direct i/o in: %s\n", ON_OFF(direct_io_in));
     (void)printf("Mmap i/o in: %s\n", ON_OFF(mmap_io_in));
     (void)printf("Synchronous i/o in: %s\n", ON_OFF(synchronous_io_in));
     (void)printf("D Synchronous i/o in: %s\n", ON_OFF(d_synchronous_io_in));
     (void)printf("R Synchronous i/o in: %s\n", ON_OFF(r_synchronous_io_in));
     (void)printf("Advisory locking in: %s\n", ON_OFF(advisory_locking_in));
     (void)printf("Mandatory locking in: %s\n", ON_OFF(mandatory_locking_in));
     (void)printf("Direct i/o out: %s\n", ON_OFF(direct_io_out));
     (void)printf("Mmap i/o out: %s\n", ON_OFF(mmap_io_out));
     (void)printf("Synchronous i/o out: %s\n", ON_OFF(synchronous_io_out));
     (void)printf("D Synchronous i/o out: %s\n", ON_OFF(d_synchronous_io_out));
     (void)printf("R Synchronous i/o out: %s\n", ON_OFF(r_synchronous_io_out));
     (void)printf("Advisory locking out: %s\n", ON_OFF(advisory_locking_out));
     (void)printf("Mandatory locking out: %s\n", ON_OFF(mandatory_locking_out));
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
  if( (strcmp(argv[first_file_optind], "/dev/zero") == 0) &&
      (strcmp(argv[first_file_optind], "/dev/random") == 0) &&
      (strcmp(argv[first_file_optind], "/dev/urandom") == 0) )
  {
     strncpy(abspath, argv[first_file_optind], PATH_MAX);

      /* If reading from /dev/zero, set the size.  Otherwise, remember the size
       * of the file. */
     size = 1024*1024*1024;  /* 1GB */
  }
  else if ( (strncmp(argv[first_file_optind], "/dev/fd/", 8) == 0) )
  {
     strncpy(abspath, argv[first_file_optind], PATH_MAX);

     size = -1LL;
  }
  else
  {
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
	(void)fprintf(stderr,
		      "input file %s is not a regular file\n", abspath);
	return 1;
     }

     size = file_info.st_size;
  }

  /* Blow away the files cache. */
  if(cache)
  {
     (void) invalidate_cache_posix(abspath);
#ifdef __linux__
     (void) invalidate_cache_linux(abspath);
#endif /* __linux__ */
#ifdef __sgi
     (void) invalidate_cache_irix(abspath);
#endif /* __sgi */
  }

  if(verbose)
  {
     (void) pages_in_core(abspath);
  }

  /* Open the input file. */
  errno = 0;
  if((fd_in = open(abspath, flags_in)) < 0)
  {
    (void)fprintf(stderr, "input open(%s): %s\n", abspath, strerror(errno));
    return 1;
  }

  /* Check the input file. */
  if(mandatory_locking_in)
  {
     errno = 0;
     if(fstat(fd_in, &file_info) < 0)
     {
	(void)fprintf(stderr, "input stat(%s): %s\n", abspath, strerror(errno));
	return 1;
     }
     errno = 0;
     if(S_ISREG(file_info.st_mode))
     {
	if((file_info.st_mode & S_IXGRP) || !(file_info.st_mode & S_ISGID))
	{
	   (void)fprintf(stderr, "input file %s error:\n"
			 "  1) mandatory file locking requested\n"
			 "  2) group exectute bit is %s.  Should be off.\n"
			 "  3) set-group-id bit is %s.  Should be on.\n",
			 abspath,
			 ON_OFF(file_info.st_mode & S_IXGRP),
			 ON_OFF((file_info.st_mode & S_ISGID)));
	   return 1;
	}
     }
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
  if((fd_out = open(argv[second_file_optind], flags_out, mode_out)) < 0)
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
  if(fstat(fd_out, &file_info) < 0)
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
  if(mandatory_locking_out)
  {
     errno = 0;
     if(S_ISREG(file_info.st_mode))
     {
	if((file_info.st_mode & S_IXGRP) || !(file_info.st_mode & S_ISGID))
	{
	   (void)fprintf(stderr, "pitput file %s error:\n"
			 "  1) mandatory file locking requested\n"
			 "  2) group exectute bit is %s.  Should be off.\n"
			 "  3) set-group-id bit is %s.  Should be on.\n",
			 abspath,
			 ON_OFF(file_info.st_mode & S_IXGRP),
			 ON_OFF((file_info.st_mode & S_ISGID)));
	   return 1;
	}
     }
  }

  if(verbose)
  {
     (void)printf("The output file: %s\n", abspath);
  }

  /* Place the values into the struct.  Some compilers complained when this
   * information was placed into the struct inline at initalization.  So it
   * was moved here. */
  (void)memset(&reads, 0, sizeof(reads));
  (void)memset(&writes, 0, sizeof(writes));
  reads.fd = fd_in;
  reads.size = size;
  reads.bytes_to_go = size;
  reads.block_size = align_to_page(block_size);
  if(threaded_transfer)
    reads.array_size = array_size;
  else
    reads.array_size = 1;
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
  reads.synchronous_io = (bool)synchronous_io_in;
  reads.d_synchronous_io = (bool)d_synchronous_io_in;
  reads.r_synchronous_io = (bool)r_synchronous_io_in;
  reads.advisory_locking = (bool)advisory_locking_in;
  reads.mandatory_locking = (bool)mandatory_locking_in;
  writes.fd = fd_out;
  writes.size = size;
  writes.bytes_to_go = size;
  writes.block_size = align_to_page(block_size);
  if(threaded_transfer)
    writes.array_size = array_size;
  else
    writes.array_size = 1;
  writes.mmap_size = mmap_size;
  writes.timeout = timeout;
  writes.crc_flag = 1;
  writes.transfer_direction = 1; /* positive means write */
  writes.direct_io = (bool)direct_io_out;
  writes.mmap_io = (bool)mmap_io_out;
  writes.synchronous_io = (bool)synchronous_io_out;
  writes.d_synchronous_io = (bool)d_synchronous_io_out;
  writes.r_synchronous_io = (bool)r_synchronous_io_out;
  writes.advisory_locking = (bool)advisory_locking_out;
  writes.mandatory_locking = (bool)mandatory_locking_out;

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
		     reads.transfer_time,
		     writes.transfer_time,
		     writes.bytes_transfered,
		     writes.crc_ui);
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
