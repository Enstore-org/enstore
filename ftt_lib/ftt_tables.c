static char rcsid[] = "@(#)$Id$";
#include <stdio.h>
#include <ftt_private.h>

int ftt_trans_open[MAX_TRANS_ERRNO] = {
	/*    0 NOERROR	*/	FTT_SUCCESS,
	/*    1	EPERM	*/	FTT_EPERM,
	/*    2	ENOENT	*/	FTT_ENOENT,
	/*    3	ESRCH	*/	FTT_ENOENT,
	/*    4	EINTR	*/	FTT_ENOENT,
	/*    5	EIO	*/	FTT_EIO,
	/*    6	ENXIO	*/	FTT_ENODEV,
	/*    7	E2BIG	*/	FTT_EFAULT,
	/*    8	ENOEXEC	*/	FTT_EFAULT,
	/*    9	EBADF	*/	FTT_ENOENT,
	/*   10	ECHILD	*/	FTT_ENOTSUPPORTED,
	/*   11	EAGAIN	*/	FTT_ENOTAPE,
	/*   12	ENOMEM	*/	FTT_ENOMEM,
	/*   13	EACCES	*/	FTT_EPERM,
	/*   14	EFAULT	*/	FTT_ENOENT,
	/*   15	ENOTBLK	*/	FTT_ENOTSUPPORTED,
	/*   16	EBUSY	*/	FTT_EBUSY,
	/*   17	EEXIST	*/	FTT_ENOTSUPPORTED,
	/*   18	EXDEV	*/	FTT_ENOTSUPPORTED,
	/*   19	ENODEV	*/	FTT_ENODEV,
	/*   20	ENOTDIR	*/	FTT_ENOENT,
	/*   21	EISDIR	*/	FTT_ENODEV,
	/*   22	EINVAL	*/	FTT_ENOENT,
	/*   23	ENFILE	*/	FTT_ENFILE,
	/*   24	EMFILE	*/	FTT_ENFILE,
	/*   25	ENOTTY	*/	FTT_ENOTTAPE,
	/*   26	ETXTBSY	*/	FTT_ENOENT,
	/*   27	EFBIG	*/	FTT_EBLKSIZE,
	/*   28	ENOSPC	*/	FTT_EFAULT,
	/*   29	ESPIPE	*/	FTT_ENOTSUPPORTED,
	/*   30	EROFS	*/	FTT_EROFS,
	/*   31	EMLINK	*/	FTT_ENOTSUPPORTED,
	/*   32 EMPIPE  */	FTT_EPIPE,
	/*   33 EDOM  */	FTT_ENOTTAPE,
	/*   34 ERANGE  */	FTT_ENOTSUPPORTED,
	/*   35 ENOMSG  */	FTT_ENOTSUPPORTED,
	/*   36 EIDRM  */	FTT_ENOTSUPPORTED,
	/*   37 ECHRNG  */	FTT_ENOTSUPPORTED,
	/*   38 EL2NSYNC  */	FTT_ENOTSUPPORTED,
	/*   39 EL3HLT  */	FTT_ENOTSUPPORTED,
	/*   40 EL3RST  */	FTT_ENOTSUPPORTED,
	/*   41 ELNRNG  */	FTT_ENOTSUPPORTED,
	/*   42 EUNATCH  */	FTT_ENOTSUPPORTED,
	/*   43 ENOCSI  */	FTT_ENOTSUPPORTED,
	/*   44 EL2HLT  */	FTT_ENOTSUPPORTED,
	/*   45 EDEADLK  */	FTT_EBUSY,
/* AIX specific */
	/*   46 ENOTREADY*/	FTT_ENOTAPE,
	/*   47 EWRPROTECT */	FTT_EROFS,
	/*   48 EFORMAT */	FTT_EBLANK,
};

int ftt_trans_in[MAX_TRANS_ERRNO] = {
	/*    0 NOERROR	*/	FTT_SUCCESS,
	/*    1	EPERM	*/	FTT_EPERM,
	/*    2	ENOENT	*/	FTT_ENOENT,
	/*    3	ESRCH	*/	FTT_ENOENT,
	/*    4	EINTR	*/	FTT_EUNRECOVERED,
	/*    5	EIO	*/	FTT_EIO,
	/*    6	ENXIO	*/	FTT_ENXIO,
	/*    7	E2BIG	*/	FTT_EBLKSIZE,
	/*    8	ENOEXEC	*/	FTT_ENOEXEC,
	/*    9	EBADF	*/	FTT_ENOENT,
	/*   10	ECHILD	*/	FTT_ENOTSUPPORTED,
	/*   11	EAGAIN	*/	FTT_ENOTAPE,
	/*   12	ENOMEM	*/	FTT_ERANGE,
	/*   13	EACCES	*/	FTT_EPERM,
	/*   14	EFAULT	*/	FTT_EFAULT,
	/*   15	ENOTBLK	*/	FTT_ENOTSUPPORTED,
	/*   16	EBUSY	*/	FTT_EBUSY,
	/*   17	EEXIST	*/	FTT_ENOTSUPPORTED,
	/*   18	EXDEV	*/	FTT_ENOTSUPPORTED,
	/*   19	ENODEV	*/	FTT_ENOENT,
	/*   20	ENOTDIR	*/	FTT_ENOENT,
	/*   21	EISDIR	*/	FTT_ENOENT,
	/*   22	EINVAL	*/	FTT_ERANGE,
	/*   23	ENFILE	*/	FTT_ENFILE,
	/*   24	EMFILE	*/	FTT_ENFILE,
	/*   25	ENOTTY	*/	FTT_ENOTTAPE,
	/*   26	ETXTBSY	*/	FTT_ENOENT,
	/*   27	EFBIG	*/	FTT_EBLKSIZE,
	/*   28	ENOSPC	*/	FTT_EBLANK,
	/*   29	ESPIPE	*/	FTT_ENOTSUPPORTED,
	/*   30	EROFS	*/	FTT_EROFS,
	/*   31	EMLINK	*/	FTT_ENOTSUPPORTED,
	/*   32 EMPIPE  */	FTT_EPIPE,
	/*   33 EDOM  */	FTT_ENOTTAPE,
	/*   34 ERANGE  */	FTT_ENOTSUPPORTED,
	/*   35 ENOMSG  */	FTT_ENOTSUPPORTED,
	/*   36 EIDRM  */	FTT_ENOTSUPPORTED,
	/*   37 ECHRNG  */	FTT_ENOTSUPPORTED,
	/*   38 EL2NSYNC  */	FTT_ENOTSUPPORTED,
	/*   39 EL3HLT  */	FTT_ENOTSUPPORTED,
	/*   40 EL3RST  */	FTT_ENOTSUPPORTED,
	/*   41 ELNRNG  */	FTT_ENOTSUPPORTED,
	/*   42 EUNATCH  */	FTT_ENOTSUPPORTED,
	/*   43 ENOCSI  */	FTT_ENOTSUPPORTED,
	/*   44 EL2HLT  */	FTT_ENOTSUPPORTED,
	/*   45 EDEADLK  */	FTT_EBUSY,
/* AIX specific */
	/*   46 ENOTREADY*/	FTT_ENOTAPE,
	/*   47 EWRPROTECT */	FTT_EROFS,
	/*   48 EFORMAT */	FTT_EBLANK,
};
int ftt_trans_in_AIX[MAX_TRANS_ERRNO] = {
	/*    0 NOERROR	*/	FTT_SUCCESS,
	/*    1	EPERM	*/	FTT_EPERM,
	/*    2	ENOENT	*/	FTT_ENOENT,
	/*    3	ESRCH	*/	FTT_ENOENT,
	/*    4	EINTR	*/	FTT_EUNRECOVERED,
	/*    5	EIO	*/	FTT_EBLANK,
	/*    6	ENXIO	*/	FTT_ENXIO,
	/*    7	E2BIG	*/	FTT_EBLKSIZE,
	/*    8	ENOEXEC	*/	FTT_ENOEXEC,
	/*    9	EBADF	*/	FTT_ENOENT,
	/*   10	ECHILD	*/	FTT_ENOTSUPPORTED,
	/*   11	EAGAIN	*/	FTT_ENOTAPE,
	/*   12	ENOMEM	*/	FTT_ERANGE,
	/*   13	EACCES	*/	FTT_EPERM,
	/*   14	EFAULT	*/	FTT_EFAULT,
	/*   15	ENOTBLK	*/	FTT_ENOTSUPPORTED,
	/*   16	EBUSY	*/	FTT_EBUSY,
	/*   17	EEXIST	*/	FTT_ENOTSUPPORTED,
	/*   18	EXDEV	*/	FTT_ENOTSUPPORTED,
	/*   19	ENODEV	*/	FTT_ENOENT,
	/*   20	ENOTDIR	*/	FTT_ENOENT,
	/*   21	EISDIR	*/	FTT_ENOENT,
	/*   22	EINVAL	*/	FTT_EBLKSIZE,
	/*   23	ENFILE	*/	FTT_ENFILE,
	/*   24	EMFILE	*/	FTT_ENFILE,
	/*   25	ENOTTY	*/	FTT_ENOTTAPE,
	/*   26	ETXTBSY	*/	FTT_ENOENT,
	/*   27	EFBIG	*/	FTT_EBLKSIZE,
	/*   28	ENOSPC	*/	FTT_EBLANK,
	/*   29	ESPIPE	*/	FTT_ENOTSUPPORTED,
	/*   30	EROFS	*/	FTT_EROFS,
	/*   31	EMLINK	*/	FTT_EIO,
	/*   32 EMPIPE  */	FTT_EPIPE,
	/*   33 EDOM  */	FTT_ENOTTAPE,
	/*   34 ERANGE  */	FTT_ENOTSUPPORTED,
	/*   35 ENOMSG  */	FTT_ENOTSUPPORTED,
	/*   36 EIDRM  */	FTT_ENOTSUPPORTED,
	/*   37 ECHRNG  */	FTT_ENOTSUPPORTED,
	/*   38 EL2NSYNC  */	FTT_ENOTSUPPORTED,
	/*   39 EL3HLT  */	FTT_ENOTSUPPORTED,
	/*   40 EL3RST  */	FTT_ENOTSUPPORTED,
	/*   41 ELNRNG  */	FTT_ENOTSUPPORTED,
	/*   42 EUNATCH  */	FTT_ENOTSUPPORTED,
	/*   43 ENOCSI  */	FTT_ENOTSUPPORTED,
	/*   44 EL2HLT  */	FTT_ENOTSUPPORTED,
	/*   45 EDEADLK  */	FTT_EBUSY,
/* AIX specific */
	/*   46 ENOTREADY*/	FTT_ENOTAPE,
	/*   47 EWRPROTECT */	FTT_EROFS,
	/*   48 EFORMAT */	FTT_EBLANK,
};

int ftt_trans_skiprec[MAX_TRANS_ERRNO] = {
	/*    0 NOERROR	*/	FTT_SUCCESS,
	/*    1	EPERM	*/	FTT_EPERM,
	/*    2	ENOENT	*/	FTT_ENOENT,
	/*    3	ESRCH	*/	FTT_ENOENT,
	/*    4	EINTR	*/	FTT_EUNRECOVERED,
	/*    5	EIO	*/	FTT_EFILEMARK,
	/*    6	ENXIO	*/	FTT_ENXIO,
	/*    7	E2BIG	*/	FTT_EBLKSIZE,
	/*    8	ENOEXEC	*/	FTT_ENOEXEC,
	/*    9	EBADF	*/	FTT_ENOENT,
	/*   10	ECHILD	*/	FTT_ENOTSUPPORTED,
	/*   11	EBUSY	*/	FTT_EBUSY,
	/*   12	ENOMEM	*/	FTT_ENOMEM,
	/*   13	EACCES	*/	FTT_EPERM,
	/*   14	EFAULT	*/	FTT_EFAULT,
	/*   15	ENOTBLK	*/	FTT_ENOTSUPPORTED,
	/*   16	EBUSY	*/	FTT_EBUSY,
	/*   17	EEXIST	*/	FTT_ENOTSUPPORTED,
	/*   18	EXDEV	*/	FTT_ENOTSUPPORTED,
	/*   19	ENODEV	*/	FTT_ENOENT,
	/*   20	ENOTDIR	*/	FTT_ENOENT,
	/*   21	EISDIR	*/	FTT_ENOENT,
	/*   22	EINVAL	*/	FTT_ENOENT,
	/*   23	ENFILE	*/	FTT_ENFILE,
	/*   24	EMFILE	*/	FTT_ENFILE,
	/*   25	ENOTTY	*/	FTT_ENOTTAPE,
	/*   26	ETXTBSY	*/	FTT_ENOENT,
	/*   27	EFBIG	*/	FTT_EFILEMARK,
	/*   28	ENOSPC	*/	FTT_EFILEMARK,
	/*   29	ESPIPE	*/	FTT_ENOTSUPPORTED,
	/*   30	EROFS	*/	FTT_EROFS,
	/*   31	EMLINK	*/	FTT_ENOTSUPPORTED,
	/*   32 EMPIPE  */	FTT_EPIPE,
	/*   33 EDOM  */	FTT_ENOTTAPE,
	/*   34 ERANGE  */	FTT_ENOTSUPPORTED,
	/*   35 ENOMSG  */	FTT_ENOTSUPPORTED,
	/*   36 EIDRM  */	FTT_ENOTSUPPORTED,
	/*   37 ECHRNG  */	FTT_ENOTSUPPORTED,
	/*   38 EL2NSYNC  */	FTT_ENOTSUPPORTED,
	/*   39 EL3HLT  */	FTT_ENOTSUPPORTED,
	/*   40 EL3RST  */	FTT_ENOTSUPPORTED,
	/*   41 ELNRNG  */	FTT_ENOTSUPPORTED,
	/*   42 EUNATCH  */	FTT_ENOTSUPPORTED,
	/*   43 ENOCSI  */	FTT_ENOTSUPPORTED,
	/*   44 EL2HLT  */	FTT_ENOTSUPPORTED,
	/*   45 EDEADLK  */	FTT_EBUSY,
/* AIX specific */
	/*   46 ENOTREADY*/	FTT_ENOTAPE,
	/*   47 EWRPROTECT */	FTT_EROFS,
	/*   48 EFORMAT */	FTT_EBLANK,
};
int ftt_trans_skipf[MAX_TRANS_ERRNO] = {
	/*    0 NOERROR	*/	FTT_SUCCESS,
	/*    1	EPERM	*/	FTT_EPERM,
	/*    2	ENOENT	*/	FTT_ENOENT,
	/*    3	ESRCH	*/	FTT_ENOENT,
	/*    4	EINTR	*/	FTT_EUNRECOVERED,
	/*    5	EIO	*/	FTT_EBLANK,
	/*    6	ENXIO	*/	FTT_ENXIO,
	/*    7	E2BIG	*/	FTT_EBLKSIZE,
	/*    8	ENOEXEC	*/	FTT_ENOEXEC,
	/*    9	EBADF	*/	FTT_ENOENT,
	/*   10	ECHILD	*/	FTT_ENOTSUPPORTED,
	/*   11	EAGAIN	*/	FTT_ENOTAPE,
	/*   12	ENOMEM	*/	FTT_ENOMEM,
	/*   13	EACCES	*/	FTT_EPERM,
	/*   14	EFAULT	*/	FTT_EFAULT,
	/*   15	ENOTBLK	*/	FTT_ENOTSUPPORTED,
	/*   16	EBUSY	*/	FTT_EBUSY,
	/*   17	EEXIST	*/	FTT_ENOTSUPPORTED,
	/*   18	EXDEV	*/	FTT_ENOTSUPPORTED,
	/*   19	ENODEV	*/	FTT_ENOENT,
	/*   20	ENOTDIR	*/	FTT_ENOENT,
	/*   21	EISDIR	*/	FTT_ENOENT,
	/*   22	EINVAL	*/	FTT_ENOENT,
	/*   23	ENFILE	*/	FTT_ENFILE,
	/*   24	EMFILE	*/	FTT_ENFILE,
	/*   25	ENOTTY	*/	FTT_ENOTTAPE,
	/*   26	ETXTBSY	*/	FTT_ENOENT,
	/*   27	EFBIG	*/	FTT_EBLANK,
	/*   28	ENOSPC	*/	FTT_EBLANK,
	/*   29	ESPIPE	*/	FTT_ENOTSUPPORTED,
	/*   30	EROFS	*/	FTT_EROFS,
	/*   31	EMLINK	*/	FTT_ENOTSUPPORTED,
	/*   32 EMPIPE  */	FTT_EPIPE,
	/*   33 EDOM  */	FTT_ENOTTAPE,
	/*   34 ERANGE  */	FTT_ENOTSUPPORTED,
	/*   35 ENOMSG  */	FTT_ENOTSUPPORTED,
	/*   36 EIDRM  */	FTT_ENOTSUPPORTED,
	/*   37 ECHRNG  */	FTT_ENOTSUPPORTED,
	/*   38 EL2NSYNC  */	FTT_ENOTSUPPORTED,
	/*   39 EL3HLT  */	FTT_ENOTSUPPORTED,
	/*   40 EL3RST  */	FTT_ENOTSUPPORTED,
	/*   41 ELNRNG  */	FTT_ENOTSUPPORTED,
	/*   42 EUNATCH  */	FTT_ENOTSUPPORTED,
	/*   43 ENOCSI  */	FTT_ENOTSUPPORTED,
	/*   44 EL2HLT  */	FTT_ENOTSUPPORTED,
	/*   45 EDEADLK  */	FTT_EBUSY,
/* AIX specific */
	/*   46 ENOTREADY*/	FTT_ENOTAPE,
	/*   47 EWRPROTECT */	FTT_EROFS,
	/*   48 EFORMAT */	FTT_EBLANK,
};
int ftt_trans_skipf_AIX[MAX_TRANS_ERRNO] = {
	/*    0 NOERROR	*/	FTT_SUCCESS,
	/*    1	EPERM	*/	FTT_EPERM,
	/*    2	ENOENT	*/	FTT_ENOENT,
	/*    3	ESRCH	*/	FTT_ENOENT,
	/*    4	EINTR	*/	FTT_EUNRECOVERED,
	/*    5	EIO	*/	FTT_EBLANK,
	/*    6	ENXIO	*/	FTT_ENXIO,
	/*    7	E2BIG	*/	FTT_EBLKSIZE,
	/*    8	ENOEXEC	*/	FTT_ENOEXEC,
	/*    9	EBADF	*/	FTT_ENOENT,
	/*   10	ECHILD	*/	FTT_ENOTSUPPORTED,
	/*   11	EAGAIN	*/	FTT_ENOTAPE,
	/*   12	ENOMEM	*/	FTT_ENOMEM,
	/*   13	EACCES	*/	FTT_EPERM,
	/*   14	EFAULT	*/	FTT_EFAULT,
	/*   15	ENOTBLK	*/	FTT_ENOTSUPPORTED,
	/*   16	EBUSY	*/	FTT_EBUSY,
	/*   17	EEXIST	*/	FTT_ENOTSUPPORTED,
	/*   18	EXDEV	*/	FTT_ENOTSUPPORTED,
	/*   19	ENODEV	*/	FTT_ENOENT,
	/*   20	ENOTDIR	*/	FTT_ENOENT,
	/*   21	EISDIR	*/	FTT_ENOENT,
	/*   22	EINVAL	*/	FTT_ENOENT,
	/*   23	ENFILE	*/	FTT_ENFILE,
	/*   24	EMFILE	*/	FTT_ENFILE,
	/*   25	ENOTTY	*/	FTT_ENOTTAPE,
	/*   26	ETXTBSY	*/	FTT_ENOENT,
	/*   27	EFBIG	*/	FTT_EBLANK,
	/*   28	ENOSPC	*/	FTT_EBLANK,
	/*   29	ESPIPE	*/	FTT_ENOTSUPPORTED,
	/*   30	EROFS	*/	FTT_EROFS,
	/*   31	EMLINK	*/	FTT_EBLANK,
	/*   32 EMPIPE  */	FTT_EPIPE,
	/*   33 EDOM  */	FTT_ENOTTAPE,
	/*   34 ERANGE  */	FTT_ENOTSUPPORTED,
	/*   35 ENOMSG  */	FTT_ENOTSUPPORTED,
	/*   36 EIDRM  */	FTT_ENOTSUPPORTED,
	/*   37 ECHRNG  */	FTT_ENOTSUPPORTED,
	/*   38 EL2NSYNC  */	FTT_ENOTSUPPORTED,
	/*   39 EL3HLT  */	FTT_ENOTSUPPORTED,
	/*   40 EL3RST  */	FTT_ENOTSUPPORTED,
	/*   41 ELNRNG  */	FTT_ENOTSUPPORTED,
	/*   42 EUNATCH  */	FTT_ENOTSUPPORTED,
	/*   43 ENOCSI  */	FTT_ENOTSUPPORTED,
	/*   44 EL2HLT  */	FTT_ENOTSUPPORTED,
	/*   45 EDEADLK  */	FTT_EBUSY,
/* AIX specific */
	/*   46 ENOTREADY*/	FTT_ENOTAPE,
	/*   47 EWRPROTECT */	FTT_EROFS,
	/*   48 EFORMAT */	FTT_EBLANK,
};
int ftt_trans_skipr[MAX_TRANS_ERRNO] = {
	/*    0 NOERROR	*/	FTT_SUCCESS,
	/*    1	EPERM	*/	FTT_EPERM,
	/*    2	ENOENT	*/	FTT_ENOENT,
	/*    3	ESRCH	*/	FTT_ENOENT,
	/*    4	EINTR	*/	FTT_EUNRECOVERED,
	/*    5	EIO	*/	FTT_ELEADER,
	/*    6	ENXIO	*/	FTT_ENXIO,
	/*    7	E2BIG	*/	FTT_EBLKSIZE,
	/*    8	ENOEXEC	*/	FTT_ENOEXEC,
	/*    9	EBADF	*/	FTT_ENOENT,
	/*   10	ECHILD	*/	FTT_ENOTSUPPORTED,
	/*   11	EAGAIN	*/	FTT_ENOTAPE,
	/*   12	ENOMEM	*/	FTT_ENOMEM,
	/*   13	EACCES	*/	FTT_EPERM,
	/*   14	EFAULT	*/	FTT_EFAULT,
	/*   15	ENOTBLK	*/	FTT_ENOTSUPPORTED,
	/*   16	EBUSY	*/	FTT_EBUSY,
	/*   17	EEXIST	*/	FTT_ENOTSUPPORTED,
	/*   18	EXDEV	*/	FTT_ENOTSUPPORTED,
	/*   19	ENODEV	*/	FTT_ENOENT,
	/*   20	ENOTDIR	*/	FTT_ENOENT,
	/*   21	EISDIR	*/	FTT_ENOENT,
	/*   22	EINVAL	*/	FTT_ENOENT,
	/*   23	ENFILE	*/	FTT_ENFILE,
	/*   24	EMFILE	*/	FTT_ENFILE,
	/*   25	ENOTTY	*/	FTT_ENOTTAPE,
	/*   26	ETXTBSY	*/	FTT_ENOENT,
	/*   27	EFBIG	*/	FTT_ELEADER,
	/*   28	ENOSPC	*/	FTT_ELEADER,
	/*   29	ESPIPE	*/	FTT_ENOTSUPPORTED,
	/*   30	EROFS	*/	FTT_EROFS,
	/*   31	EMLINK	*/	FTT_ENOTSUPPORTED,
	/*   32 EMPIPE  */	FTT_EPIPE,
	/*   33 EDOM  */	FTT_ENOTTAPE,
	/*   34 ERANGE  */	FTT_ENOTSUPPORTED,
	/*   35 ENOMSG  */	FTT_ENOTSUPPORTED,
	/*   36 EIDRM  */	FTT_ENOTSUPPORTED,
	/*   37 ECHRNG  */	FTT_ENOTSUPPORTED,
	/*   38 EL2NSYNC  */	FTT_ENOTSUPPORTED,
	/*   39 EL3HLT  */	FTT_ENOTSUPPORTED,
	/*   40 EL3RST  */	FTT_ENOTSUPPORTED,
	/*   41 ELNRNG  */	FTT_ENOTSUPPORTED,
	/*   42 EUNATCH  */	FTT_ENOTSUPPORTED,
	/*   43 ENOCSI  */	FTT_ENOTSUPPORTED,
	/*   44 EL2HLT  */	FTT_ENOTSUPPORTED,
	/*   45 EDEADLK  */	FTT_EBUSY,
/* AIX specific */
	/*   46 ENOTREADY*/	FTT_ENOTAPE,
	/*   47 EWRPROTECT */	FTT_EROFS,
	/*   48 EFORMAT */	FTT_EBLANK,
};

int ftt_trans_skipr_AIX[MAX_TRANS_ERRNO] = {
	/*    0 NOERROR	*/	FTT_SUCCESS,
	/*    1	EPERM	*/	FTT_EPERM,
	/*    2	ENOENT	*/	FTT_ENOENT,
	/*    3	ESRCH	*/	FTT_ENOENT,
	/*    4	EINTR	*/	FTT_EUNRECOVERED,
	/*    5	EIO	*/	FTT_ELEADER,
	/*    6	ENXIO	*/	FTT_ENXIO,
	/*    7	E2BIG	*/	FTT_EBLKSIZE,
	/*    8	ENOEXEC	*/	FTT_ENOEXEC,
	/*    9	EBADF	*/	FTT_ENOENT,
	/*   10	ECHILD	*/	FTT_ENOTSUPPORTED,
	/*   11	EAGAIN	*/	FTT_ENOTAPE,
	/*   12	ENOMEM	*/	FTT_ENOMEM,
	/*   13	EACCES	*/	FTT_EPERM,
	/*   14	EFAULT	*/	FTT_EFAULT,
	/*   15	ENOTBLK	*/	FTT_ENOTSUPPORTED,
	/*   16	EBUSY	*/	FTT_EBUSY,
	/*   17	EEXIST	*/	FTT_ENOTSUPPORTED,
	/*   18	EXDEV	*/	FTT_ENOTSUPPORTED,
	/*   19	ENODEV	*/	FTT_ENOENT,
	/*   20	ENOTDIR	*/	FTT_ENOENT,
	/*   21	EISDIR	*/	FTT_ENOENT,
	/*   22	EINVAL	*/	FTT_ENOENT,
	/*   23	ENFILE	*/	FTT_ENFILE,
	/*   24	EMFILE	*/	FTT_ENFILE,
	/*   25	ENOTTY	*/	FTT_ENOTTAPE,
	/*   26	ETXTBSY	*/	FTT_ENOENT,
	/*   27	EFBIG	*/	FTT_ELEADER,
	/*   28	ENOSPC	*/	FTT_ELEADER,
	/*   29	ESPIPE	*/	FTT_ENOTSUPPORTED,
	/*   30	EROFS	*/	FTT_EROFS,
	/*   31	EMLINK	*/	FTT_ELEADER,
	/*   32 EMPIPE  */	FTT_EPIPE,
	/*   33 EDOM  */	FTT_ENOTTAPE,
	/*   34 ERANGE  */	FTT_ENOTSUPPORTED,
	/*   35 ENOMSG  */	FTT_ENOTSUPPORTED,
	/*   36 EIDRM  */	FTT_ENOTSUPPORTED,
	/*   37 ECHRNG  */	FTT_ENOTSUPPORTED,
	/*   38 EL2NSYNC  */	FTT_ENOTSUPPORTED,
	/*   39 EL3HLT  */	FTT_ENOTSUPPORTED,
	/*   40 EL3RST  */	FTT_ENOTSUPPORTED,
	/*   41 ELNRNG  */	FTT_ENOTSUPPORTED,
	/*   42 EUNATCH  */	FTT_ENOTSUPPORTED,
	/*   43 ENOCSI  */	FTT_ENOTSUPPORTED,
	/*   44 EL2HLT  */	FTT_ENOTSUPPORTED,
	/*   45 EDEADLK  */	FTT_EBUSY,
/* AIX specific */
	/*   46 ENOTREADY*/	FTT_ENOTAPE,
	/*   47 EWRPROTECT */	FTT_EROFS,
	/*   48 EFORMAT */	FTT_EBLANK,
};

int ftt_trans_skiprew_AIX[MAX_TRANS_ERRNO] = {
	/*    0 NOERROR	*/	FTT_SUCCESS,
	/*    1	EPERM	*/	FTT_EPERM,
	/*    2	ENOENT	*/	FTT_ENOENT,
	/*    3	ESRCH	*/	FTT_ENOENT,
	/*    4	EINTR	*/	FTT_EUNRECOVERED,
	/*    5	EIO	*/	FTT_ENOTAPE,
	/*    6	ENXIO	*/	FTT_ENOTAPE,
	/*    7	E2BIG	*/	FTT_EBLKSIZE,
	/*    8	ENOEXEC	*/	FTT_ENOEXEC,
	/*    9	EBADF	*/	FTT_ENOENT,
	/*   10	ECHILD	*/	FTT_ENOTSUPPORTED,
	/*   11	EAGAIN	*/	FTT_ENOTAPE,
	/*   12	ENOMEM	*/	FTT_ENOMEM,
	/*   13	EACCES	*/	FTT_EPERM,
	/*   14	EFAULT	*/	FTT_EFAULT,
	/*   15	ENOTBLK	*/	FTT_ENOTSUPPORTED,
	/*   16	EBUSY	*/	FTT_EBUSY,
	/*   17	EEXIST	*/	FTT_ENOTSUPPORTED,
	/*   18	EXDEV	*/	FTT_ENOTSUPPORTED,
	/*   19	ENODEV	*/	FTT_ENOENT,
	/*   20	ENOTDIR	*/	FTT_ENOENT,
	/*   21	EISDIR	*/	FTT_ENOENT,
	/*   22	EINVAL	*/	FTT_ENOENT,
	/*   23	ENFILE	*/	FTT_ENFILE,
	/*   24	EMFILE	*/	FTT_ENFILE,
	/*   25	ENOTTY	*/	FTT_ENOTTAPE,
	/*   26	ETXTBSY	*/	FTT_ENOENT,
	/*   27	EFBIG	*/	FTT_ENOTAPE,
	/*   28	ENOSPC	*/	FTT_ENOTAPE,
	/*   29	ESPIPE	*/	FTT_ENOTSUPPORTED,
	/*   30	EROFS	*/	FTT_ENOTAPE,
	/*   31	EMLINK	*/	FTT_ELEADER,
	/*   32 EMPIPE  */	FTT_EPIPE,
	/*   33 EDOM  */	FTT_ENOTTAPE,
	/*   34 ERANGE  */	FTT_ENOTSUPPORTED,
	/*   35 ENOMSG  */	FTT_ENOTSUPPORTED,
	/*   36 EIDRM  */	FTT_ENOTSUPPORTED,
	/*   37 ECHRNG  */	FTT_ENOTSUPPORTED,
	/*   38 EL2NSYNC  */	FTT_ENOTSUPPORTED,
	/*   39 EL3HLT  */	FTT_ENOTSUPPORTED,
	/*   40 EL3RST  */	FTT_ENOTSUPPORTED,
	/*   41 ELNRNG  */	FTT_ENOTSUPPORTED,
	/*   42 EUNATCH  */	FTT_ENOTSUPPORTED,
	/*   43 ENOCSI  */	FTT_ENOTSUPPORTED,
	/*   44 EL2HLT  */	FTT_ENOTSUPPORTED,
	/*   45 EDEADLK  */	FTT_EBUSY,
/* AIX specific */
	/*   46 ENOTREADY*/	FTT_ENOTAPE,
	/*   47 EWRPROTECT */	FTT_EROFS,
	/*   48 EFORMAT */	FTT_EBLANK,
};

int ftt_trans_out_AIX[MAX_TRANS_ERRNO] = {
	/*    0 NOERROR	*/	FTT_SUCCESS,
	/*    1	EPERM	*/	FTT_EROFS,
	/*    2	ENOENT	*/	FTT_ENOENT,
	/*    3	ESRCH	*/	FTT_ENOENT,
	/*    4	EINTR	*/	FTT_EUNRECOVERED,
	/*    5	EIO	*/	FTT_EIO,
	/*    6	ENXIO	*/	FTT_ENXIO,
	/*    7	E2BIG	*/	FTT_EBLKSIZE,
	/*    8	ENOEXEC	*/	FTT_ENOEXEC,
	/*    9	EBADF	*/	FTT_EROFS,
	/*   10	ECHILD	*/	FTT_ENOTSUPPORTED,
	/*   11	EAGAIN	*/	FTT_ENOTAPE,
	/*   12	ENOMEM	*/	FTT_ENOMEM,
	/*   13	EACCES	*/	FTT_EPERM,
	/*   14	EFAULT	*/	FTT_EFAULT,
	/*   15	ENOTBLK	*/	FTT_ENOTSUPPORTED,
	/*   16	EBUSY	*/	FTT_EBUSY,
	/*   17	EEXIST	*/	FTT_ENOTSUPPORTED,
	/*   18	EXDEV	*/	FTT_ENOTSUPPORTED,
	/*   19	ENODEV	*/	FTT_ENOENT,
	/*   20	ENOTDIR	*/	FTT_ENOENT,
	/*   21	EISDIR	*/	FTT_ENOENT,
	/*   22	EINVAL	*/	FTT_EBLKSIZE,
	/*   23	ENFILE	*/	FTT_ENFILE,
	/*   24	EMFILE	*/	FTT_ENFILE,
	/*   25	ENOTTY	*/	FTT_ENOTTAPE,
	/*   26	ETXTBSY	*/	FTT_ENOENT,
	/*   27	EFBIG	*/	FTT_EBLKSIZE,
	/*   28	ENOSPC	*/	FTT_ENOSPC,
	/*   29	ESPIPE	*/	FTT_ENOTSUPPORTED,
	/*   30	EROFS	*/	FTT_EROFS,
	/*   31	EMLINK	*/	FTT_EIO,
	/*   32 EMPIPE  */	FTT_EPIPE,
	/*   33 EDOM  */	FTT_ENOTTAPE,
	/*   34 ERANGE  */	FTT_ENOTSUPPORTED,
	/*   35 ENOMSG  */	FTT_ENOTSUPPORTED,
	/*   36 EIDRM  */	FTT_ENOTSUPPORTED,
	/*   37 ECHRNG  */	FTT_ENOTSUPPORTED,
	/*   38 EL2NSYNC  */	FTT_ENOTSUPPORTED,
	/*   39 EL3HLT  */	FTT_ENOTSUPPORTED,
	/*   40 EL3RST  */	FTT_ENOTSUPPORTED,
	/*   41 ELNRNG  */	FTT_ENOTSUPPORTED,
	/*   42 EUNATCH  */	FTT_ENOTSUPPORTED,
	/*   43 ENOCSI  */	FTT_ENOTSUPPORTED,
	/*   44 EL2HLT  */	FTT_ENOTSUPPORTED,
	/*   45 EDEADLK  */	FTT_EBUSY,
/* AIX specific */
	/*   46 ENOTREADY*/	FTT_ENOTAPE,
	/*   47 EWRPROTECT */	FTT_EROFS,
	/*   48 EFORMAT */	FTT_EBLANK,
};

int ftt_trans_out[MAX_TRANS_ERRNO] = {
	/*    0 NOERROR	*/	FTT_SUCCESS,
	/*    1	EPERM	*/	FTT_EROFS,
	/*    2	ENOENT	*/	FTT_ENOENT,
	/*    3	ESRCH	*/	FTT_ENOENT,
	/*    4	EINTR	*/	FTT_EUNRECOVERED,
	/*    5	EIO	*/	FTT_EIO,
	/*    6	ENXIO	*/	FTT_ENXIO,
	/*    7	E2BIG	*/	FTT_EBLKSIZE,
	/*    8	ENOEXEC	*/	FTT_ENOEXEC,
	/*    9	EBADF	*/	FTT_EROFS,
	/*   10	ECHILD	*/	FTT_ENOTSUPPORTED,
	/*   11	EAGAIN	*/	FTT_ENOTAPE,
	/*   12	ENOMEM	*/	FTT_ENOMEM,
	/*   13	EACCES	*/	FTT_EPERM,
	/*   14	EFAULT	*/	FTT_EFAULT,
	/*   15	ENOTBLK	*/	FTT_ENOTSUPPORTED,
	/*   16	EBUSY	*/	FTT_EBUSY,
	/*   17	EEXIST	*/	FTT_ENOTSUPPORTED,
	/*   18	EXDEV	*/	FTT_ENOTSUPPORTED,
	/*   19	ENODEV	*/	FTT_ENOENT,
	/*   20	ENOTDIR	*/	FTT_ENOENT,
	/*   21	EISDIR	*/	FTT_ENOENT,
	/*   22	EINVAL	*/	FTT_EBLKSIZE,
	/*   23	ENFILE	*/	FTT_ENFILE,
	/*   24	EMFILE	*/	FTT_ENFILE,
	/*   25	ENOTTY	*/	FTT_ENOTTAPE,
	/*   26	ETXTBSY	*/	FTT_ENOENT,
	/*   27	EFBIG	*/	FTT_EBLKSIZE,
	/*   28	ENOSPC	*/	FTT_ENXIO,
	/*   29	ESPIPE	*/	FTT_ENOTSUPPORTED,
	/*   30	EROFS	*/	FTT_EROFS,
	/*   31	EMLINK	*/	FTT_ENOTSUPPORTED,
	/*   32 EMPIPE  */	FTT_EPIPE,
	/*   33 EDOM  */	FTT_ENOTTAPE,
	/*   34 ERANGE  */	FTT_ENOTSUPPORTED,
	/*   35 ENOMSG  */	FTT_ENOTSUPPORTED,
	/*   36 EIDRM  */	FTT_ENOTSUPPORTED,
	/*   37 ECHRNG  */	FTT_ENOTSUPPORTED,
	/*   38 EL2NSYNC  */	FTT_ENOTSUPPORTED,
	/*   39 EL3HLT  */	FTT_ENOTSUPPORTED,
	/*   40 EL3RST  */	FTT_ENOTSUPPORTED,
	/*   41 ELNRNG  */	FTT_ENOTSUPPORTED,
	/*   42 EUNATCH  */	FTT_ENOTSUPPORTED,
	/*   43 ENOCSI  */	FTT_ENOTSUPPORTED,
	/*   44 EL2HLT  */	FTT_ENOTSUPPORTED,
	/*   45 EDEADLK  */	FTT_EBUSY,
/* AIX specific */
	/*   46 ENOTREADY*/	FTT_ENOTAPE,
	/*   47 EWRPROTECT */	FTT_EROFS,
	/*   48 EFORMAT */	FTT_EBLANK,
};

int ftt_trans_chall[MAX_TRANS_ERRNO] = {
	/*    0 NOERROR	*/	FTT_SUCCESS,
	/*    1	EPERM	*/	FTT_EPERM,
	/*    2	ENOENT	*/	FTT_ENOENT,
	/*    3	ESRCH	*/	FTT_ENOENT,
	/*    4	EINTR	*/	FTT_EUNRECOVERED,
	/*    5	EIO	*/	FTT_EIO,
	/*    6	ENXIO	*/	FTT_ENXIO,
	/*    7	E2BIG	*/	FTT_EBLKSIZE,
	/*    8	ENOEXEC	*/	FTT_EPERM,
	/*    9	EBADF	*/	FTT_EPERM,
	/*   10	ECHILD	*/	FTT_ENOTSUPPORTED,
	/*   11	EAGAIN	*/	FTT_ENOTAPE,
	/*   12	ENOMEM	*/	FTT_ENOMEM,
	/*   13	EACCES	*/	FTT_EPERM,
	/*   14	EFAULT	*/	FTT_EFAULT,
	/*   15	ENOTBLK	*/	FTT_ENOTSUPPORTED,
	/*   16	EBUSY	*/	FTT_EBUSY,
	/*   17	EEXIST	*/	FTT_ENOENT,
	/*   18	EXDEV	*/	FTT_ENOENT,
	/*   19	ENODEV	*/	FTT_ENOENT,
	/*   20	ENOTDIR	*/	FTT_ENOENT,
	/*   21	EISDIR	*/	FTT_ENOENT,
	/*   22	EINVAL	*/	FTT_ENOENT,
	/*   23	ENFILE	*/	FTT_ENFILE,
	/*   24	EMFILE	*/	FTT_ENFILE,
	/*   25	ENOTTY	*/	FTT_ENOTTAPE,
	/*   26	ETXTBSY	*/	FTT_ENOENT,
	/*   27	EFBIG	*/	FTT_EBLKSIZE,
	/*   28	ENOSPC	*/	FTT_EIO,
	/*   29	ESPIPE	*/	FTT_ENOTSUPPORTED,
	/*   30	EROFS	*/	FTT_EPERM,
	/*   31	EMLINK	*/	FTT_ENOTSUPPORTED,
	/*   32 EMPIPE  */	FTT_EPIPE,
	/*   33 EDOM  */	FTT_ENOTTAPE,
	/*   34 ERANGE  */	FTT_ENOTSUPPORTED,
	/*   35 ENOMSG  */	FTT_ENOTSUPPORTED,
	/*   36 EIDRM  */	FTT_ENOTSUPPORTED,
	/*   37 ECHRNG  */	FTT_ENOTSUPPORTED,
	/*   38 EL2NSYNC  */	FTT_ENOTSUPPORTED,
	/*   39 EL3HLT  */	FTT_ENOTSUPPORTED,
	/*   40 EL3RST  */	FTT_ENOTSUPPORTED,
	/*   41 ELNRNG  */	FTT_ENOTSUPPORTED,
	/*   42 EUNATCH  */	FTT_ENOTSUPPORTED,
	/*   43 ENOCSI  */	FTT_ENOTSUPPORTED,
	/*   44 EL2HLT  */	FTT_ENOTSUPPORTED,
	/*   45 EDEADLK  */	FTT_EBUSY,
/* AIX specific */
	/*   46 ENOTREADY*/	FTT_ENOTAPE,
	/*   47 EWRPROTECT */	FTT_EROFS,
	/*   48 EFORMAT */	FTT_EBLANK,
};
int *ftt_trans_table_AIX[] = {
    /* none...but just to be safe0 */ ftt_trans_in,
    /* FTT_OPN_READ		 1 */ ftt_trans_in_AIX,
    /* FTT_OPN_WRITE		 2 */ ftt_trans_out_AIX,
    /* FTT_OPN_WRITEFM		 3 */ ftt_trans_out_AIX,
    /* FTT_OPN_SKIPREC		 4 */ ftt_trans_skiprec,
    /* FTT_OPN_SKIPFM		 5 */ ftt_trans_skipf_AIX,
    /* FTT_OPN_REWIND		 6 */ ftt_trans_skiprew_AIX,
    /* FTT_OPN_UNLOAD		 7 */ ftt_trans_skipf,
    /* FTT_OPN_RETENSION	 8 */ ftt_trans_skipf,
    /* FTT_OPN_ERASE		 9 */ ftt_trans_out,
    /* FTT_OPN_STATUS		10 */ ftt_trans_in,
    /* FTT_OPN_GET_STATUS	11 */ ftt_trans_in,
    /* FTT_OPN_ASYNC 		12 */ ftt_trans_in,
    /* FTT_OPN_PASSTHRU     13 */ ftt_trans_out,
    /* FTT_OPN_CHALL        14 */ ftt_trans_chall,
    /* FTT_OPN_OPEN         15 */ ftt_trans_open,
    /* FTT_OPN_RSKIPREC		16 */ ftt_trans_skiprec,
    /* FTT_OPN_RSKIPFM		17 */ ftt_trans_skipr_AIX,
};
int *ftt_trans_table[] = {
    /* none...but just to be safe0 */ ftt_trans_in,
    /* FTT_OPN_READ		     1 */ ftt_trans_in,
    /* FTT_OPN_WRITE		 2 */ ftt_trans_out,
    /* FTT_OPN_WRITEFM		 3 */ ftt_trans_out,
    /* FTT_OPN_SKIPREC		 4 */ ftt_trans_skiprec,
    /* FTT_OPN_SKIPFM		 5 */ ftt_trans_skipf,
    /* FTT_OPN_REWIND		 6 */ ftt_trans_skipr,
    /* FTT_OPN_UNLOAD		 7 */ ftt_trans_skipf,
    /* FTT_OPN_RETENSION	 8 */ ftt_trans_skipf,
    /* FTT_OPN_ERASE		 9 */ ftt_trans_out,
    /* FTT_OPN_STATUS		10 */ ftt_trans_in,
    /* FTT_OPN_GET_STATUS	11 */ ftt_trans_in,
    /* FTT_OPN_ASYNC 		12 */ ftt_trans_in,
    /* FTT_OPN_PASSTHRU     13 */ ftt_trans_out,
    /* FTT_OPN_CHALL        14 */ ftt_trans_chall,
    /* FTT_OPN_OPEN         15 */ ftt_trans_open,
    /* FTT_OPN_RSKIPREC		16 */ ftt_trans_skiprec,
    /* FTT_OPN_RSKIPFM		17 */ ftt_trans_skipr,
};

char *Redwood_density_trans[MAX_TRANS_DENSITY] = {
	"16-track",
	"48-track",
	"SD-3",
	"unknown",
	0
};

char *Generic_density_trans[MAX_TRANS_DENSITY] = {
	"unknown",
	"low",
	"med",
	"hi",
	0
};

char *Exabyte_density_trans[MAX_TRANS_DENSITY] = {
	"unknown",
	"8200",
	"8500",
	"8900",
	0
};

char *DLT_density_trans[MAX_TRANS_DENSITY] = {
	"unknown",
	"6667bpi",
	"10000bpi",
	"2.6G 42500bpi 24trk",
	"6G 42500bpi 56trk",
	"10G 62500bpi 64trk",
	"20G 62500bpi 128trk",
	0
};


/*
** commands to popen to read device id-s
*/
static char AIXfind[] =
    "IFS=\" \t\n\";\
     PATH=\"/bin:/usr/bin:/sbin:/usr/sbin\";\
     /usr/sbin/lscfg -v -l rmt%d | \
       grep 'Machine Type' |       \
       sed -e 's/.*\\.//'";

static char IRIXfind[] =
    "IFS=\" \t\n\";\
     PATH=\"/bin:/usr/bin:/sbin:/usr/sbin\";\
     /sbin/hinv | \
       grep 'Tape drive: unit %2$d on SCSI controller %1$d:' | \
       sed -e 's/.*: *//' -e 's/8mm(\\(.*\\).) cartridge */EXB-\\1/'";

static char IRIXfindVME[] =
    "IFS=\" \t\n\";\
     PATH=\"/bin:/usr/bin:/sbin:/usr/sbin\";\
     /sbin/hinv | \
       grep 'Tape drive: unit %2$d on VME-SCSI controller %1$d:' | \
       sed -e 's/.*:  *//' -e 's/8mm(\\(.*\\).) cartridge */EXB-\\1/'";

static char OSF1find[] = 
   "IFS=\" \t\n\"\n\
    PATH=\"/usr/sbin:/sbin:/etc:/bin:/usr/bin\"\n\
    export PATH \n\
    minor_num=`ls -l /dev/rmt%dh | sed -e 's/.*, *\\([0-9]*\\).*/\\1/'` \n\
    tmp=`expr $minor_num /  1024` \n\
    bus=`expr $tmp / 16` \n\
    target=`expr $tmp %% 16` \n\
    scu show edt bus $bus target $target lun 0 full | \n\
	    grep 'Product Id' | \n\
	    sed -e 's/.*: //' \n";

static char SunOSfind_devices[] = 
   "#s\n\
    IFS=\" \t\n\";\
    PATH=\"/bin:/usr/bin:/sbin:/usr/sbin\";\
    dir='%s';\
    id=\"`dmesg | grep st@%d | sort -u | sed -e 's;^\\(st[0-9]*\\).*$;\\1;'`\" \n\
    line=\"`dmesg | grep \\^${id}: `\" \n\
    case \"$line\" in \n\
    *Vendor*)  \n\
	echo \"$line\" | sed -e 's/.*Product..\\([^ ]*\\).*>/\\1/' | tail -1\n\
	;; \n\
    *) \n\
	echo \"$line\" | sed -e 's/[^<]*<[^ ]* \\([^ ]*\\).*>/\\1/' | tail -1\n\
	;; \n\
    esac \n";

static char SunOSfind_dev[] =
   "IFS=\" \t\n\";\
    PATH=\"/bin:/usr/bin:/sbin:/usr/sbin\";\
    drive=%d\n\
    id=\"`ls -l /dev/rmt/${drive} | sed -e 's;.*/\\(st@[0-9]*\\).*;\\1;'`\" \n\
    id=\"`dmesg | grep ${id} | sort -u | sed -e 's;^\\(st[0-9]*\\).*$;\\1;'`\" \n\
    line=\"`dmesg | grep \\^${id}: `\" \n\
    case \"$line\" in \n\
    *Vendor*)  \n\
        echo \"$line\" | sed -e 's/.*Product..\\([^ ]*\\).*>/\\1/' | tail -1 \n\
        ;; \n\
    *) \n\
        echo \"$line\" | sed -e 's/[^<]*<[^ ]* \\([^ ]*\\).*>/\\1/' | tail -1 \n\
        ;; \n\
    esac \n";

static char Win32find_dev[] = "";

/*
** device id's
*/

/* The following tables are based on:
**
**
**typedef struct {
**    char *os;                    OS+Version (i.e. IRIX+5.3) string 
**    char *drivid;                SCSI Drive-id prefix 
**    char *controller;            controller name string 
**    long flags;                  FTT_FLAG_XXX bits for behavior 
**    long scsi_ops;               FTT_OP_XXX bits for ops to use SCSI 
**    int **errortrans;            errortrans[FTT_OPN_XXX][errno]->ftt_errno
**
**    char **densitytrans;         density names 
**    char *baseconv_in;           basename parser scanf string 
**    char *baseconv_out;          basename parser scanf string 
**    int nconv;                   number of items scanf should return 
**    char *drividcmd;             printf this to get shell command->driveid
**    ftt_devinfo devs[MAXDEVSLOTS];  drive specs with printf strings 
**} ftt_dev_entry;
**
** Note -- ftt_findslot searches this table with a prefix string match,
**         and takes the first match.  Therefore if you put a null string
**         or one type that is a prefix of the other (for either OS name
**         or drive-id) that will prevent longer strings from matching
**	   further down the table, for example, if you have
**         {"OSNAME", "", ...},
**         {"OSNAME", "EXB-8200", ...},...
**	   the first one will always match before the second one, and
**         you might as well not have it.  So put your null-string
**         default cases *last*.
*/
#define EXB_MAX_BLKSIZE 245760
#define IRIX_MAX_BLKSIZE 131072
/* 
  SunOS 5.4 and up max block size is limited by the device max-blocksize
  for DLT it is 0x400000 for modes 2.6 G and 6 G
           and  0xffffff for 10 G and 20 G
	
  other devices can be limited to 65535

*/
#define SUN_MAX_BLKSIZE EXB_MAX_BLKSIZE

#define WIN_MAX_BLKSIZE 120000

/*
================================================================================
               THE device TABLE 
================================================================================
*/

ftt_dev_entry devtable[] = {

/*
================================================================================
               SunOS devices
================================================================================
*/

/*
   For the SunOS check the st configiration file to be sure that all densities
   are set the way ftt is expecting: 

   st configuration file:  /kernel/drv/st.conf

   EXB-8200       =       1,0x28,0,0x8c79,1,0x00,0;                 <-- EXB-8200 SunOS 5.5.1  default
   EXB-850X       =       1,0x29,0,0xce39,4,0x14,0x15,0x8c,0x90,1;  <-- EXB-850x (modified)

   EXB_8900       =       1,0x35,0,0xD639,1,0x27,0;                 <-- EXB-8900 (not suported)

   DLT            =       1,0x38,0,0xd639 ,4,0x17,0x18,0x80,0x81,2;
   DLT4           =       1,0x38,0,0xd639 ,4,0x80,0x81,0x82,0x83,2;
   DLT7           =       1,0x38,0,0x1D639,4,0x82,0x83,0x84,0x85,2; 


  check also /usr/include/sys/mtio.h for:

  #define MT_ISEXABYTE    0x28             sun: SCSI Exabyte 8mm cartridge 
  #define MT_ISEXB8500    0x29             sun: SCSI Exabyte 8500 8mm cart 

  #define MT_IS8MM        0x35             generic 8mm tape drive 
  #define MT_ISOTHER      0x36             generic other type of tape drive 

  #define MT_ISDLT        0x38             SUN/Quantum DLT drives 

   */

/* macros for the densites for SunOS  - 4 densities (L,M,H,U and C ) and default (D) */
/*
    Example for EXB-8200

     #define SunOS_L  0,  0,   0         Low density  den, mode,  hwd
     #define SunOS_M  0,  0,   0       
     #define SunOS_H  0,  0,   0
     #define SunOS_U  0,  0,   0
     #define SunOS_D  SunOS_L
*/

/* macro for standard rmt devices for the drives  */
#define SunOS_dev \
       { SunOS_str"lbn",   SunOS_L,    0,  0,                 0, 0, SUN_MAX_BLKSIZE}, \
       { SunOS_str"lbn",   SunOS_L,    0,  1,                 0, 0, SUN_MAX_BLKSIZE}, \
       { SunOS_str"lb",    SunOS_L,    0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE}, \
       { SunOS_str"ln",    SunOS_L,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE}, \
       { SunOS_str"l",     SunOS_L,    0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE}, \
\
       { SunOS_str"mbn",   SunOS_M,    0,  0,                 0, 0, SUN_MAX_BLKSIZE}, \
       { SunOS_str"mbn",   SunOS_M,    0,  1,                 0, 0, SUN_MAX_BLKSIZE}, \
       { SunOS_str"mb",    SunOS_M,    0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE}, \
       { SunOS_str"mn",    SunOS_M,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE}, \
       { SunOS_str"m",     SunOS_M,    0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE}, \
\
       { SunOS_str"hbn",   SunOS_H,    0,  0,                 0, 0, SUN_MAX_BLKSIZE}, \
       { SunOS_str"hbn",   SunOS_H,    0,  1,                 0, 0, SUN_MAX_BLKSIZE}, \
       { SunOS_str"hb",    SunOS_H,    0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE}, \
       { SunOS_str"hn",    SunOS_H,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE}, \
       { SunOS_str"h",     SunOS_H,    0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE}, \
\
       { SunOS_str"ubn",   SunOS_U,    0,  0,                 0, 0, SUN_MAX_BLKSIZE}, \
       { SunOS_str"ubn",   SunOS_U,    0,  1,                 0, 0, SUN_MAX_BLKSIZE}, \
       { SunOS_str"ub",    SunOS_U,    0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE}, \
       { SunOS_str"un",    SunOS_U,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE}, \
       { SunOS_str"u",     SunOS_U,    0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE}, \
\
       { SunOS_str"cbn",   SunOS_U,    0,  0,                 0, 1, SUN_MAX_BLKSIZE}, \
       { SunOS_str"cbn",   SunOS_U,    0,  1,                 0, 0, SUN_MAX_BLKSIZE}, \
       { SunOS_str"cb",    SunOS_U,    0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE}, \
       { SunOS_str"cn",    SunOS_U,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE}, \
       { SunOS_str"c",     SunOS_U,    0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE}, \
\
       { SunOS_str"bn",    SunOS_D,    0,  0,                 0, 1, SUN_MAX_BLKSIZE}, \
       { SunOS_str"bn",    SunOS_D,    0,  1,                 0, 0, SUN_MAX_BLKSIZE}, \
       { SunOS_str"b",     SunOS_D,    0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE}, \
       { SunOS_str"n",     SunOS_D,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE}, \
       { SunOS_str"",      SunOS_D,    0,  0, FTT_RDNW|FTT_RWOC, 0, SUN_MAX_BLKSIZE}, \

/* flags for SunOS */

#define SunOS_flags  FTT_FLAG_SUID_SCSI | FTT_FLAG_VERIFY_EOFS | FTT_FLAG_BSIZE_AFTER     

/*
------------------------------------------------------------------------------------------
  SunOS  EXB-8200         densities:  0 , 0, 0 , 0
------------------------------------------------------------------------------------------
*/
#define SunOS_L  0,  0,   0
#define SunOS_M  0,  0,   0
#define SunOS_H  0,  0,   0
#define SunOS_U  0,  0,   0
#define SunOS_D  SunOS_L

#define SunOS_EXB_8200 \
    /*   string          den mod hwd   pas fxd rewind            1st */                   \
    /*   ======          === === ===   === === ======            === */                   \
    /* Default, pass-thru */                                                              \
       { SunOS_str"lbn",  SunOS_L,      0,  0,                 0, 1, SUN_MAX_BLKSIZE},    \
       { SunOS_str"lbn",  SunOS_L,      1,  0,                 0, 0, SUN_MAX_BLKSIZE},    \
    /* Usable Variable */                                                                 \
       { SunOS_str"lbn",  SunOS_L,      0,  0,                  0, 0, SUN_MAX_BLKSIZE},   \
       { SunOS_str"mbn",  SunOS_M,      0,  0,                  0, 1, SUN_MAX_BLKSIZE},   \
       { SunOS_str"hbn",  SunOS_H,      0,  0,                  0, 1, SUN_MAX_BLKSIZE},   \
       { SunOS_str"ubn",  SunOS_U,      0,  0,                  0, 1, SUN_MAX_BLKSIZE},   \
    /* Densitites */                                                                      \
       { SunOS_str"", 	  SunOS_L,      0,  0,  FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE},   \
    /* Descriptive */                                                                     \
       SunOS_dev                                                                          \
       { 0 }

    {"SunOS+5", "EXB-8200", "SCSI", SunOS_flags, FTT_OP_GET_STATUS, 
     ftt_trans_table, Exabyte_density_trans,
        "rmt/%d", "rmt/%d", 1, SunOSfind_dev, {

#define SunOS_str "rmt/%d"
	  SunOS_EXB_8200,
    }},
    {"SunOS+5", "EXB-8200", "SCSI", SunOS_flags, FTT_OP_GET_STATUS, 
     ftt_trans_table, Exabyte_density_trans,
       "%[^/]/st@%d,0:", "%s/st@%d,0:", 2, SunOSfind_devices, {

#undef  SunOS_str
#define SunOS_str "%s/st@%d,0:"
	SunOS_EXB_8200,
    }},

#undef SunOS_L
#undef SunOS_M
#undef SunOS_H
#undef SunOS_U
#undef SunOS_D
#undef SunOS_str
/*
------------------------------------------------------------------------------------------
  SunOS  EXB-850x      densities  0x14, 0x15, 0x8c , 0x90  and 0x15  
------------------------------------------------------------------------------------------
*/
#define SunOS_L  0,  0,   0x14
#define SunOS_M  1,  0,   0x15
#define SunOS_H  0,  1,   0x8c
#define SunOS_U  1,  1,   0x90
#define SunOS_D  SunOS_M

#define SunOS_EXB_85 \
    /*   string          den mod hwd   pas fxd rewind            1st */              \
    /*   ======          === === ===   === === ======            === */              \
    /* Default, passthru  */                                                         \
       { SunOS_str"mbn",  SunOS_M,  0,  0,                 0, 1, SUN_MAX_BLKSIZE},   \
       { SunOS_str"mbn",  SunOS_M,  1,  0,                 0, 0, SUN_MAX_BLKSIZE},   \
    /* Usable Variable */                                                            \
       { SunOS_str"lbn",  SunOS_L,  0,  0,                 0, 1, SUN_MAX_BLKSIZE},   \
       { SunOS_str"mbn",  SunOS_M,  0,  0,                 0, 0, SUN_MAX_BLKSIZE},   \
       { SunOS_str"hbn",  SunOS_H,  0,  0,                 0, 1, SUN_MAX_BLKSIZE},   \
       { SunOS_str"ubn",  SunOS_U,  0,  0,                 0, 1, SUN_MAX_BLKSIZE},   \
    /* Densitites */                                                                 \
       { SunOS_str"",    1,  0,     0,  0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE},   \
    /* Descriptive */                                                                \
       SunOS_dev                                                                     \
       { 0 }


    {"SunOS+5", "EXB-85", "SCSI", SunOS_flags, FTT_OP_GET_STATUS, 
      ftt_trans_table, Exabyte_density_trans,
     "rmt/%d", "rmt/%d", 1, SunOSfind_dev, {
#define SunOS_str "rmt/%d"
      SunOS_EXB_85,
    }},

    {"SunOS+5", "EXB-85", "SCSI", SunOS_flags, FTT_OP_GET_STATUS, 
     ftt_trans_table, Exabyte_density_trans,
       "%[^/]/st@%d,0:", "%s/st@%d,0:", 2, SunOSfind_devices, {
#undef  SunOS_str
#define SunOS_str "%s/st@%d,0:"
	SunOS_EXB_85,
	 
    }},

/*
------------------------------------------------------------------------------------------
  SunOS  EXB-8900 - is not supported
------------------------------------------------------------------------------------------
*/
    {"SunOS+5", "EXB-8900", "SCSI", SunOS_flags, FTT_OP_GET_STATUS,
      ftt_trans_table,Exabyte_density_trans,
      "rmt/%d", "rmt/%d", 1, SunOSfind_dev, {
    /*   string          den mod hwd   pas fxd rewind            1st */
    /*   ======          === === ===   === === ======            === */
    /* Default, passthru  */
       { "rmt/%dhbn", 	  2,  1, 0x27,  0,  0,                 0, 1, SUN_MAX_BLKSIZE},
    /* Usable */
    /* Densitites */
       { "rmt/%dub", 	  1,  1, 0x90,  0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dhb", 	  1,  0, 0x15,  0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dmb", 	  0,  1, 0x8c,  0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dlb", 	  0,  0, 0x14,  0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%db", 	  1,  0, 0x15,  0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE},
    /* Descriptive */
       { "rmt/%dubn", 	  1,  1, 0x90,  0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dmbn", 	  1,  0, 0x15,  1,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dbn", 	  1,  0,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dun", 	  1,  0,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dhn", 	  1,  0,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dmn", 	  1,  0,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dln", 	  1,  0,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "rmt/%du", 	  1,  0,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dh", 	  1,  0,  0,    0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dm", 	  1,  0,  0,    0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dl", 	  1,  0,  0,    0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%d", 	  1,  0,  0,    0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dc", 	  1,  0,  0,    0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dcb", 	  1,  1,  0,    0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dcbn", 	  1,  1,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dcn", 	  1,  1,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dn", 	  1,  0,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { 0 },
    }},

#undef SunOS_L
#undef SunOS_M
#undef SunOS_H
#undef SunOS_U
#undef SunOS_D
#undef SunOS_str
/*
------------------------------------------------------------------------------------------
  SunOS  DLT4000      densities  0x80, 0x81, 0x82 , 0x83  and 0x82 default  
------------------------------------------------------------------------------------------
*/
#define SunOS_L  2,  0,   0x17
#define SunOS_M  3,  0,   0x18
#define SunOS_H  5,  0,   0x82
#define SunOS_U  5,  1,   0x83
#define SunOS_D  SunOS_H

       /* device files templet for DLT4000 */
 
#define SunOS_DLT4 \
    /*   string          den mod hwd   pas fxd rewind            1st */            \
    /*   ======          === === ===   === === ======            === */            \
    /* Default, Passthru  */                                                       \
       { SunOS_str"hbn",    SunOS_H,  0,  0,                 0, 1, SUN_MAX_BLKSIZE}, \
       { SunOS_str"hbn",    SunOS_H,  1,  0,                 0, 0, SUN_MAX_BLKSIZE}, \
    /* Usable Variable */                                                          \
       { SunOS_str"lbn",    SunOS_L,  0,  0,                 0, 1, SUN_MAX_BLKSIZE}, \
       { SunOS_str"mbn",    SunOS_M,  0,  0,                 0, 1, SUN_MAX_BLKSIZE}, \
       { SunOS_str"hbn", 4, 0, 0x80,  0,  0,                 0, 0, SUN_MAX_BLKSIZE}, \
       { SunOS_str"hbn", 4, 0, 0x80,  0,  1,                 0, 0, SUN_MAX_BLKSIZE}, \
       { SunOS_str"ubn",    SunOS_U,  0,  0,                 0, 1, SUN_MAX_BLKSIZE}, \
       { SunOS_str"ubn", 4, 1, 0x81,  0,  0,                 0, 0, SUN_MAX_BLKSIZE}, \
       { SunOS_str"ubn", 4, 1, 0x81,  0,  1,                 0, 0, SUN_MAX_BLKSIZE}, \
    /* Densitites */                                                               \
       { SunOS_str"",    0,  0, 0x0A,  0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE}, \
       { SunOS_str"",    1,  0, 0x16,  0,  0, FTT_RDNW|FTT_RWOC, 0, SUN_MAX_BLKSIZE}, \
       { SunOS_str"",    2,  0, 0x17,  0,  0, FTT_RDNW|FTT_RWOC, 0, SUN_MAX_BLKSIZE}, \
       { SunOS_str"",    3,  0, 0x18,  0,  0, FTT_RDNW|FTT_RWOC, 0, SUN_MAX_BLKSIZE}, \
       { SunOS_str"",    4, -1, 0x19,  0,  0, FTT_RDNW|FTT_RWOC, 0, SUN_MAX_BLKSIZE}, \
       { SunOS_str"",    5, -1, 0x1A,  0,  0, FTT_RDNW|FTT_RWOC, 0, SUN_MAX_BLKSIZE}, \
       { SunOS_str"",    5,  0,    0,  0,  0, FTT_RDNW|FTT_RWOC, 0, SUN_MAX_BLKSIZE}, \
    /* Descriptive */                                                              \
       SunOS_dev                                                                   \
       { 0 }  


    {"SunOS+5", "DLT4", "SCSI", SunOS_flags, FTT_OP_GET_STATUS,ftt_trans_table, DLT_density_trans,
       "rmt/%d", "rmt/%d", 1, SunOSfind_dev, {
	 /* rmt device files */
#define SunOS_str "rmt/%d"
	SunOS_DLT4,
    }},
    {"SunOS+5", "DLT4", "SCSI", SunOS_flags, FTT_OP_GET_STATUS,ftt_trans_table, DLT_density_trans,
      "%[^/]/st@%d,0:", "%s/st@%d", 2, SunOSfind_devices, {
	 /* st device files */
#undef  SunOS_str
#define SunOS_str "%s/st@%d,0:"
	SunOS_DLT4,
    }},

#undef SunOS_L
#undef SunOS_M
#undef SunOS_H
#undef SunOS_U
#undef SunOS_D
#undef SunOS_str
/*
------------------------------------------------------------------------------------------
  SunOS  DLT2000      densities  0x17, 0x18, 0x80 , 0x81  and 0x80 default  
------------------------------------------------------------------------------------------
*/
#define SunOS_L  2,  0,   0x17
#define SunOS_M  3,  0,   0x18
#define SunOS_H  4,  0,   0x80
#define SunOS_U  4,  1,   0x81
#define SunOS_D  SunOS_H

#define SunOS_DLT \
    /*   string          den mod hwd   pas fxd rewind            1st */               \
    /*   ======          === === ===   === === ======            === */               \
    /* Default, Passthru  */                                                          \
       { SunOS_str"hbn",     SunOS_H,  0,  0,                 0, 1, SUN_MAX_BLKSIZE}, \
       { SunOS_str"hbn",     SunOS_H,  1,  0,                 0, 0, SUN_MAX_BLKSIZE}, \
    /* Usable Variable */                                                             \
       { SunOS_str"lbn",     SunOS_L,  0,  0,                 0, 1, SUN_MAX_BLKSIZE}, \
       { SunOS_str"mbn",     SunOS_M,  0,  0,                 0, 1, SUN_MAX_BLKSIZE}, \
       { SunOS_str"hbn",     SunOS_H,  0,  0,                 0, 0, SUN_MAX_BLKSIZE}, \
       { SunOS_str"ubn",     SunOS_U,  0,  0,                 0, 1, SUN_MAX_BLKSIZE}, \
    /* Densitites */                                                                  \
       { SunOS_str"",    0,  0, 0x0A,  0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE}, \
       { SunOS_str"",    1,  0, 0x16,  0,  0, FTT_RDNW|FTT_RWOC, 0, SUN_MAX_BLKSIZE}, \
       { SunOS_str"",    2,  0, 0x17,  0,  0, FTT_RDNW|FTT_RWOC, 0, SUN_MAX_BLKSIZE}, \
       { SunOS_str"",    3,  0, 0x18,  0,  0, FTT_RDNW|FTT_RWOC, 0, SUN_MAX_BLKSIZE}, \
       { SunOS_str"",    4,  0, 0x19,  0,  0, FTT_RDNW|FTT_RWOC, 0, SUN_MAX_BLKSIZE}, \
       { SunOS_str"",    4,  1, 0x19,  0,  0, FTT_RDNW|FTT_RWOC, 0, SUN_MAX_BLKSIZE}, \
       { SunOS_str"",    4,  0,    0,  0,  0, FTT_RDNW|FTT_RWOC, 0, SUN_MAX_BLKSIZE}, \
    /* Descriptive */                                                                 \
       SunOS_dev                                                                      \
       { 0 }

   {"SunOS+5", "DLT", "SCSI", SunOS_flags, FTT_OP_GET_STATUS,ftt_trans_table, DLT_density_trans,
    "rmt/%d", "rmt/%d", 1, SunOSfind_dev, {

#define SunOS_str "rmt/%d"
	SunOS_DLT,
    }},

    {"SunOS+5", "DLT", "SCSI", SunOS_flags, FTT_OP_GET_STATUS,ftt_trans_table, DLT_density_trans,
    "%[^/]/st@%d,0:", "%s/st@%d", 2, SunOSfind_devices, {

#undef  SunOS_str
#define SunOS_str "%s/st@%d,0:"
	SunOS_DLT,
    }},

#undef SunOS_L
#undef SunOS_M
#undef SunOS_H
#undef SunOS_U
#undef SunOS_D
#undef SunOS_str

/*
------------------------------------------------------------------------------------------
  SunOS  Generic drive       densities:  0 , 0, 0 , 0
------------------------------------------------------------------------------------------
*/
#define SunOS_L  0,  0,   0
#define SunOS_M  0,  0,   0
#define SunOS_H  0,  0,   0
#define SunOS_U  0,  0,   0
#define SunOS_D  SunOS_L

#define SunOS_gen \
    /*   string          den mod hwd   pas fxd rewind            1st */             \
    /*   ======          === === ===   === === ======            === */             \
    /* Default, pass-thru */                                                        \
       { SunOS_str"lbn",     SunOS_L,   0,  0,                 0, 1, SUN_MAX_BLKSIZE}, \
       { SunOS_str"lbn",     SunOS_L,   1,  0,                 0, 0, SUN_MAX_BLKSIZE}, \
    /* Usable Variable */                                                           \
       { SunOS_str"lbn",     SunOS_L,   0,  0,                  0, 0, SUN_MAX_BLKSIZE}, \
       { SunOS_str"mbn",     SunOS_M,   0,  0,                  0, 1, SUN_MAX_BLKSIZE}, \
       { SunOS_str"hbn",     SunOS_H,   0,  0,                  0, 1, SUN_MAX_BLKSIZE}, \
       { SunOS_str"ubn",     SunOS_U,   0,  0,                  0, 1, SUN_MAX_BLKSIZE}, \
    /* Densitites */                                                                \
       { SunOS_str"", 	      SunOS_L,  0,  0,  FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE}, \
    /* Descriptive */                                                               \
       SunOS_dev                                                                    \
       { 0 }


    {"SunOS+5", "", "SCSI", SunOS_flags, FTT_OP_GET_STATUS,ftt_trans_table, Generic_density_trans,
       "rmt/%d", "rmt/%d", 1, SunOSfind_dev, {

#define SunOS_str "rmt/%d"
	SunOS_gen,
    }},
    {"SunOS+5", "", "SCSI", SunOS_flags, FTT_OP_GET_STATUS,ftt_trans_table, Generic_density_trans,
       "%[^/]/st@%d,0:", "%s/st@%d,0:", 2, SunOSfind_devices, {

#undef  SunOS_str
#define SunOS_str "%s/st@%d,0:"
	SunOS_gen,
    }},


/*
================================================================================
               OSF1 devices
================================================================================
*/

    {"OSF1", "EXB-8200", "SCSI", FTT_FLAG_SUID_DRIVEID|FTT_FLAG_SUID_SCSI|FTT_FLAG_BSIZE_AFTER, 
     FTT_OP_GET_STATUS, ftt_trans_table, Exabyte_density_trans,
	"dev/%*[nr]mt%d", "dev/rmt%d", 1, OSF1find,  {
    /*   string                  den mod hwd   pas fxd rewind            1st */
    /*   ======                  === === ===   === === ======            === */
    /* Default, Passthru  */
        { "dev/nrmt%dl",         0,  0,0x00, 0,  0,                 0, 1, EXB_MAX_BLKSIZE},
        { "dev/nrmt%dl",        -1,  0,  -1, 1,  0,                 0, 0, EXB_MAX_BLKSIZE},
    /* Descritptive */
        { "dev/rmt%dl",          0,  0,0x00, 0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%dm",          0,  0,0x00, 0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
        { "dev/nrmt%dm",         0,  0,0x00, 0,  0,                 0, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%dh",          0,  0,0x00, 0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
        { "dev/nrmt%dh",         0,  0,0x00, 0,  0,                 0, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%da",          0,  0,0x00, 0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
        { "dev/nrmt%da",         0,  0,0x00, 0,  0,                 0, 1, EXB_MAX_BLKSIZE},
	{ 0 },
     }},
    {"OSF1", "EXB-8900", "SCSI", FTT_FLAG_SUID_DRIVEID|FTT_FLAG_SUID_SCSI|FTT_FLAG_BSIZE_AFTER, FTT_OP_GET_STATUS, ftt_trans_table, Exabyte_density_trans,
	"dev/%*[nr]mt%d", "dev/rmt%d", 1, OSF1find,  {
    /*   string                  den mod hwd   pas fxd rewind            1st */
    /*   ======                  === === ===   === === ======            === */
    /* Default, Passthru  */
        { "dev/nrmt%dl",         2,  1,0x27, 0,  0,                 0, 1, EXB_MAX_BLKSIZE},
        { "dev/nrmt%dl",        -1,  0,  -1, 1,  0,                 0, 0, EXB_MAX_BLKSIZE},
    /* Descritptive */
        { "dev/rmt%dl",          2,  1,0x27, 0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%dm",          2,  1,0x27, 0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
        { "dev/nrmt%dm",         2,  1,0x27, 0,  0,                 0, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%dh",          2,  1,0x27, 0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
        { "dev/nrmt%dh",         2,  1,0x27, 0,  0,                 0, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%da",          2,  1,0x27, 0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
        { "dev/nrmt%da",         2,  1,0x27, 0,  0,                 0, 1, EXB_MAX_BLKSIZE},
    /* densities */
        { "dev/nrmt%da",         1,  1,0x90, 0,  0,          FTT_RDNW, 0, EXB_MAX_BLKSIZE},
        { "dev/nrmt%da",         0,  1,0x8c, 0,  0,          FTT_RDNW, 0, EXB_MAX_BLKSIZE},
        { "dev/nrmt%da",         1,  0,0x15, 0,  0,          FTT_RDNW, 0, EXB_MAX_BLKSIZE},
        { "dev/nrmt%da",         0,  0,0x14, 0,  0,          FTT_RDNW, 0, EXB_MAX_BLKSIZE},
	{ 0 },
     }},
    {"OSF1", "EXB-8500", "SCSI", FTT_FLAG_SUID_DRIVEID|FTT_FLAG_SUID_SCSI|FTT_FLAG_BSIZE_AFTER, FTT_OP_GET_STATUS, ftt_trans_table, Exabyte_density_trans,
	"dev/%*[nr]mt%d","dev/rmt%d", 1, OSF1find,  {
    /*   string                  den mod hwd   pas fxd rewind            1st */
    /*   ======                  === === ===   === === ======            === */
    /* Default, passthru */
        { "dev/nrmt%dh",         1,  0,0x15, 0,  0,                 0, 1, EXB_MAX_BLKSIZE},
        { "dev/nrmt%dh",        -1,  0,  -1, 1,  0,                 0, 0, EXB_MAX_BLKSIZE},
    /* Usable */
        { "dev/nrmt%dm",         1,  0,0x15, 0,  0,          FTT_RDNW, 1, EXB_MAX_BLKSIZE},
        { "dev/nrmt%dl",         0,  0,0x14, 0,  0,          FTT_RDNW, 1, EXB_MAX_BLKSIZE},
        { "dev/nrmt%da",         1,  0,0x15, 0,  0,                 0, 1, EXB_MAX_BLKSIZE},
    /* Descriptive */
        { "dev/rmt%dm",          1,  0,0x15, 0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%dh",          1,  0,0x15, 0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%dl",          0,  0,0x14, 0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%da",          1,  0,0x15, 0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%dh",          1,  0,0x00, 0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
	{ 0 },
     }},
#ifdef OSF1KERNELTABLES
    {"OSF1", "EXB-8505", "SCSI", FTT_FLAG_SUID_DRIVEID|FTT_FLAG_SUID_SCSI|FTT_FLAG_BSIZE_AFTER, FTT_OP_GET_STATUS, ftt_trans_table, Exabyte_density_trans,
	"dev/%*[nr]mt%d","dev/rmt%d", 1, OSF1find,  {
    /*   string                  den mod hwd   pas fxd rewind            1st */
    /*   ======                  === === ===   === === ======            === */
    /* Default, passthru */
        { "dev/nrmt%dh",         1,  0,0x15, 0,  0,                 0, 1, EXB_MAX_BLKSIZE},
        { "dev/nrmt%dh",        -1,  0,  -1, 1,  0,                 0, 0, EXB_MAX_BLKSIZE},
    /* Usable */
        { "dev/nrmt%dl",         0,  0,0x14, 0,  0,          FTT_RDNW, 1, EXB_MAX_BLKSIZE},
        { "dev/nrmt%dm",         0,  1,0x8c, 0,  0,          FTT_RDNW, 1, EXB_MAX_BLKSIZE},
        { "dev/nrmt%da",         1,  1,0x90, 0,  0,          FTT_RDNW, 1, EXB_MAX_BLKSIZE},
    /* Descriptive */
        { "dev/rmt%dl",          0,  0,0x14, 0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%dm",          0,  1,0x8c, 0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%dh",          1,  0,0x15, 0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%da",          1,  1,0x90, 0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%dh",          1,  0,0x00, 0,  0,          FTT_RWOC, 0, EXB_MAX_BLKSIZE},
	{ 0 },
     }},
#else
    {"OSF1", "EXB-8505", "SCSI", FTT_FLAG_SUID_DRIVEID|FTT_FLAG_SUID_SCSI|FTT_FLAG_BSIZE_AFTER, FTT_OP_GET_STATUS, ftt_trans_table, Exabyte_density_trans,
	"dev/%*[nr]mt%d","dev/rmt%d", 1, OSF1find,  {
    /*   string                  den mod hwd   pas fxd rewind            1st */
    /*   ======                  === === ===   === === ======            === */
    /* Default, passthru */
        { "dev/nrmt%dh",         1,  0,0x15, 0,  0,                 0, 1, EXB_MAX_BLKSIZE},
        { "dev/nrmt%dh",        -1,  0,  -1, 1,  0,                 0, 0, EXB_MAX_BLKSIZE},
    /* Usable */
        { "dev/nrmt%dm",         1,  0,0x15, 0,  0,          FTT_RDNW, 1, EXB_MAX_BLKSIZE},
        { "dev/nrmt%dl",         0,  0,0x14, 0,  0,          FTT_RDNW, 1, EXB_MAX_BLKSIZE},
        { "dev/nrmt%da",         1,  0,0x15, 0,  0,                 0, 1, EXB_MAX_BLKSIZE},
    /* Descriptive */
        { "dev/rmt%dm",          1,  0,0x15, 0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%dh",          1,  0,0x15, 0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%dl",          0,  0,0x14, 0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%da",          1,  0,0x15, 0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
	{ 0 },
     }},
#endif
    {"OSF1", "DLT", "SCSI", FTT_FLAG_SUID_DRIVEID|FTT_FLAG_SUID_SCSI|FTT_FLAG_BSIZE_AFTER, FTT_OP_GET_STATUS, ftt_trans_table, DLT_density_trans,
	"dev/%*[nr]mt%d","dev/rmt%d", 1, OSF1find,  {
    /*   string                  den mod hwd   pas fxd rewind            1st */
    /*   ======                  === === ===   === === ======            === */
    /* Default, Passthru */
        { "dev/nrmt%dh",         5,  0,0x1A, 0,  0,                 0, 1, EXB_MAX_BLKSIZE},
        { "dev/nrmt%dh",        -1,  0,  -1, 1,  0,                 0, 0, EXB_MAX_BLKSIZE},
    /* Usable */
        { "dev/nrmt%dl",         2,  0,0x17, 0,  0,                 0, 1, EXB_MAX_BLKSIZE},
        { "dev/nrmt%dl",         3,  0,0x18, 0,  0,                 0, 1, EXB_MAX_BLKSIZE},
        { "dev/nrmt%dm",         4,  0,0x19, 0,  0,                 0, 1, EXB_MAX_BLKSIZE},
        { "dev/nrmt%dm",         4,  1,0x19, 0,  0,                 0, 1, EXB_MAX_BLKSIZE},
        { "dev/nrmt%da",         5,  1,0x1A, 0,  0,                 0, 1, EXB_MAX_BLKSIZE},
    /* Other Densities */
        { "dev/rmt%dh",          0,  0,0x0A, 0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%dh",          1,  0,0x16, 0,  0,          FTT_RWOC, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%dh",          2,  0,0x17, 0,  0,          FTT_RWOC, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%dh",          3,  0,0x18, 0,  0,          FTT_RWOC, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%dh",          4,  0,0x19, 0,  0,          FTT_RWOC, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%dh",          5,  0,0x1A, 0,  0,          FTT_RWOC, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%dh",          5,  0,0x80, 0,  0,          FTT_RWOC, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%dh",          5,  1,0x81, 0,  0,          FTT_RWOC, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%dh",          5,  0,0x82, 0,  0,          FTT_RWOC, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%dh",          5,  1,0x83, 0,  0,          FTT_RWOC, 0, EXB_MAX_BLKSIZE},
    /* Descriptive */
        { "dev/rmt%dh",          5,  0,0x82, 0,  0,          FTT_RWOC, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%dl",          4,  0,0x19, 0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%dm",          4,  1,0x19, 0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%dh",          5,  0,0x1A, 0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%da",          5,  1,0x1A, 0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
	{ 0 },
     }},
    {"OSF1", "", "SCSI", FTT_FLAG_SUID_DRIVEID|FTT_FLAG_SUID_SCSI|FTT_FLAG_BSIZE_AFTER, FTT_OP_GET_STATUS, ftt_trans_table, Generic_density_trans,
	"dev/%*[nr]mt%d","dev/rmt%d", 1, OSF1find,  {
    /*   string                  den mod hwd   pas fxd rewind            1st */
    /*   ======                  === === ===   === === ======            === */
    /* Default, Passthru */
        { "dev/nrmt%dh",         2,  0,0x00, 0,  0,                 0, 1, EXB_MAX_BLKSIZE},
        { "dev/nrmt%dh",        -1,  0,  -1, 1,  0,                 0, 0, EXB_MAX_BLKSIZE},
    /* Usable */
        { "dev/nrmt%dl",         0,  0,0x00, 0,  0,                 0, 1, EXB_MAX_BLKSIZE},
        { "dev/nrmt%dm",         1,  1,0x00, 0,  0,                 0, 1, EXB_MAX_BLKSIZE},
        { "dev/nrmt%da",         3,  1,0x00, 0,  0,                 0, 1, EXB_MAX_BLKSIZE},
    /* Descriptive */
        { "dev/rmt%dh",          2,  0,0x00, 0,  0,          FTT_RWOC, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%dl",          0,  0,0x00, 0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%dm",          1,  1,0x00, 0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%dh",          2,  0,0x00, 0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%da",          3,  1,0x00, 0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
	{ 0 },
     }},
    {"AIX", "EXB-8900", "SCSI", FTT_FLAG_HOLD_SIGNALS|FTT_FLAG_SUID_SCSI, 
	FTT_OP_STATUS|FTT_OP_GET_STATUS,ftt_trans_table_AIX, Exabyte_density_trans,
	"dev/rmt%d", "dev/rmt%d", 1, AIXfind,  {
    /*   string                  den mod hwd   pas fxd rewind            1st */
    /*   ======                  === === ===   === === ======            === */
    /* Default, Passthru  */
        { "dev/rmt%d.5",        2,  1, 0x27, 0,  0,                 0, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.5",       -1,  0,   -1, 1,  0,                 0, 0, EXB_MAX_BLKSIZE},
    /* Variable useable */
        { "dev/rmt%d.5",        0,  0, 0x14, 0,  0,          FTT_RDNW, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.5",        0,  1, 0x90, 0,  0,          FTT_RDNW, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.5",        1,  0, 0x15, 0,  0,          FTT_RDNW, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.5",        1,  1, 0x8c, 0,  0,          FTT_RDNW, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.5",        2,  1, 0x27, 0,  0,                 0, 0, EXB_MAX_BLKSIZE},
    /* Fixed useable */
        { "dev/rmt%d.5",        0,  0, 0x14, 0,  1,          FTT_RDNW, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.5",        0,  1, 0x90, 0,  1,          FTT_RDNW, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.5",        1,  0, 0x15, 0,  1,          FTT_RDNW, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.5",        1,  1, 0x8c, 0,  1,          FTT_RDNW, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.5",        2,  1, 0x27, 0,  1,                 0, 0, EXB_MAX_BLKSIZE},
    /* Descriptive/translators */
        { "dev/rmt%d",         -1,  0, 0x00, 0,  1, FTT_RWOC|       0, 1, 512},
        { "dev/rmt%d.1",       -1,  0, 0x00, 0,  1,          FTT_RDNW, 1, 512},
        { "dev/rmt%d.1",        0,  0, 0x14, 0,  1,          FTT_RDNW, 0, 512},
        { "dev/rmt%d.1",        0,  1, 0x15, 0,  1,          FTT_RDNW, 0, 512},
        { "dev/rmt%d.1",        1,  0, 0x8c, 0,  1,          FTT_RDNW, 0, 512},
        { "dev/rmt%d.1",        1,  1, 0x90, 0,  1,          FTT_RDNW, 0, 512},
        { "dev/rmt%d.1",        2,  0, 0x27, 0,  1,          FTT_RDNW, 0, 512},
        { "dev/rmt%d.1",        2,  1, 0x27, 0,  1,          FTT_RDNW, 0, 512},
        { "dev/rmt%d.2",       -1,  0, 0x00, 0,  1, FTT_RWOC|FTT_RTOO, 1, 512},
        { "dev/rmt%d.3",       -1,  0, 0x00, 0,  1,          FTT_RTOO, 1, 512},
        { "dev/rmt%d.4",        0,  0, 0x14, 0,  0, FTT_RWOC|FTT_RDNW, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.6",        0,  0, 0x14, 0,  0, FTT_RWOC|FTT_RTOO, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.7",        0,  0, 0x14, 0,  0,        0|FTT_RTOO, 1, EXB_MAX_BLKSIZE},
        { 0, },
    }},
    {"AIX", "EXB-8505", "SCSI", FTT_FLAG_HOLD_SIGNALS|FTT_FLAG_SUID_SCSI, 
	FTT_OP_STATUS|FTT_OP_GET_STATUS,ftt_trans_table_AIX, Exabyte_density_trans,
	"dev/rmt%d","dev/rmt%d", 1, AIXfind,  {
    /*   string                  den mod hwd   pas fxd rewind            1st */
    /*   ======                  === === ===   === === ======            === */
        { "dev/rmt%d.1",        1,  0, 0x15, 0,  0,                 0, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.1",       -1,  0,   -1, 1,  0,                 0, 0, EXB_MAX_BLKSIZE},
    /* Usable, variable */
        { "dev/rmt%d.5",        0,  0, 0x14, 0,  0,                 0, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.5",        0,  1, 0x90, 0,  0,                 0, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.1",        1,  1, 0x8c, 0,  0,                 0, 0, EXB_MAX_BLKSIZE},
    /* Usable Fixed */
        { "dev/rmt%d.5",        0,  0, 0x14, 0,  1,                 0, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.5",        0,  1, 0x90, 0,  1,                 0, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.1",        1,  0, 0x15, 0,  1,                 0, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.1",        1,  1, 0x8c, 0,  1,                 0, 0, EXB_MAX_BLKSIZE},
   /* Descriptive */
        { "dev/rmt%d.4",        0,  0, 0x14, 0,  1, FTT_RWOC|       0, 1, 512},
        { "dev/rmt%d.1",        1,  0, 0x00, 0,  1,                 0, 0, 512},
        { "dev/rmt%d.5",        0,  0, 0x14, 0,  1,          FTT_RDNW, 0, 512},
        { "dev/rmt%d.5",        0,  1, 0x15, 0,  1,          FTT_RDNW, 0, 512},
        { "dev/rmt%d.1",        1,  0, 0x8c, 0,  1,          FTT_RDNW, 0, 512},
        { "dev/rmt%d.1",        1,  1, 0x90, 0,  1,          FTT_RDNW, 0, 512},
        { "dev/rmt%d.6",        0,  0, 0x14, 0,  1, FTT_RWOC|FTT_RTOO, 1, 512},
        { "dev/rmt%d.7",        0,  0, 0x14, 0,  1,          FTT_RTOO, 1, 512},
        { "dev/rmt%d",          1,  0, 0x00, 0,  0, FTT_RWOC|       0, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.2",        1,  0, 0x00, 0,  0, FTT_RWOC|FTT_RTOO, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.3",        1,  0, 0x00, 0,  0,        0|FTT_RTOO, 1, EXB_MAX_BLKSIZE},
        { 0, },
    }},
    {"AIX", "EXB-8500", "SCSI", FTT_FLAG_HOLD_SIGNALS|FTT_FLAG_SUID_SCSI, 
	FTT_OP_STATUS|FTT_OP_GET_STATUS,ftt_trans_table_AIX, Exabyte_density_trans,
	"dev/rmt%d","dev/rmt%d", 1, AIXfind,  {
    /*   string                den mod hwd   pas fxd rewind            1st */
    /*   ======                === === ===   === === ======            === */
    /* Default, passthru */
        { "dev/rmt%d.1",        1,  0, 0x15, 0,  0,                 0, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.1",       -1,  0,   -1, 1,  0,                 0, 0, EXB_MAX_BLKSIZE},
    /* Variable Usable */
        { "dev/rmt%d.5",        0,  0, 0x14, 0,  0,                 0, 1, EXB_MAX_BLKSIZE},
    /* Fixed Usable */
        { "dev/rmt%d.5",        0,  0, 0x14, 0,  1,                 0, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.1",        1,  0, 0x15, 0,  1,                 0, 0, EXB_MAX_BLKSIZE},
    /* Descriptive */
        { "dev/rmt%d.4",        0,  0, 0x14, 0,  1, FTT_RWOC|       0, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.6",        0,  0, 0x14, 0,  1, FTT_RWOC|FTT_RTOO, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.7",        0,  0, 0x14, 0,  1,          FTT_RTOO, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%d",          1,  0, 0x00, 0,  1, FTT_RWOC|       0, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%d",          1,  0, 0x15, 0,  1, FTT_RWOC|       0, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.2",        1,  0, 0x15, 0,  1, FTT_RWOC|FTT_RTOO, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.3",        1,  0, 0x15, 0,  1,        0|FTT_RTOO, 1, EXB_MAX_BLKSIZE},
        { 0, },
    }},
    {"AIX", "SD-3", "SCSI", FTT_FLAG_HOLD_SIGNALS|FTT_FLAG_SUID_SCSI|FTT_FLAG_NO_DENSITY,
FTT_OP_STATUS|FTT_OP_GET_STATUS,ftt_trans_table_AIX, Redwood_density_trans,
	"dev/rmt%d","dev/rmt%d", 1, AIXfind,  {
    /*   string                den mod hwd   pas fxd rewind         1st */
    /*   ======                === === ===   === === ======         === */
    /* Default */
        { "dev/rmt%d.1",        0,  0, 0x09, 0,  0,                 0, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.1",        1,  0, 0x28, 0,  0,                 0, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.1",        2,  0, 0x2b, 0,  0,                 0, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.1",       -1,  0,   -1, 1,  0,                 0, 0, EXB_MAX_BLKSIZE},
    /* Fixed Usable */
        { "dev/rmt%d.1",        0,  0, 0x00, 0,  1,                 0, 0, EXB_MAX_BLKSIZE},
    /* Descriptive */
        { "dev/rmt%d",          0,  0, 0x00, 0,  1, FTT_RWOC|       0, 1, 512},
        { "dev/rmt%d.1",        0,  0, 0x00, 0,  1,                 0, 0, 512},
        { "dev/rmt%d.2",        0,  0, 0x00, 0,  1, FTT_RWOC|FTT_RTOO, 1, 512},
        { "dev/rmt%d.3",        0,  0, 0x00, 0,  1,          FTT_RTOO, 1, 512},
        { "dev/rmt%d.4",        0,  0, 0x00, 0,  1, FTT_RWOC|       0, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.5",        0,  0, 0x00, 0,  1,                 0, 1, 512},
        { "dev/rmt%d.6",        0,  0, 0x00, 0,  1, FTT_RWOC|FTT_RTOO, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.7",        0,  0, 0x00, 0,  1,        0|FTT_RTOO, 1, EXB_MAX_BLKSIZE},
        { 0, },
    }},
    {"AIX", "EXB-8200", "SCSI", FTT_FLAG_HOLD_SIGNALS|FTT_FLAG_SUID_SCSI, 
FTT_OP_STATUS|FTT_OP_GET_STATUS,ftt_trans_table_AIX, Exabyte_density_trans,
	"dev/rmt%d","dev/rmt%d", 1, AIXfind,  {
    /*   string                  den mod hwd   pas fxd rewind         1st */
    /*   ======                  === === ===   === === ======         === */
    /* Default */
        { "dev/rmt%d.1",        0,  0, 0x00, 0,  0,                 0, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.1",       -1,  0,   -1, 1,  0,                 0, 0, EXB_MAX_BLKSIZE},
    /* Fixed Usable */
        { "dev/rmt%d.1",        0,  0, 0x00, 0,  1,                 0, 0, EXB_MAX_BLKSIZE},
    /* Descriptive */
        { "dev/rmt%d",          0,  0, 0x00, 0,  1, FTT_RWOC|       0, 1, 512},
        { "dev/rmt%d.1",        0,  0, 0x00, 0,  1,                 0, 1, 512},
        { "dev/rmt%d.2",        0,  0, 0x00, 0,  1, FTT_RWOC|FTT_RTOO, 1, 512},
        { "dev/rmt%d.3",        0,  0, 0x00, 0,  1,          FTT_RTOO, 1, 512},
        { "dev/rmt%d.4",        0,  0, 0x00, 0,  1, FTT_RWOC|       0, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.5",        0,  0, 0x00, 0,  1,                 0, 1, 512},
        { "dev/rmt%d.6",        0,  0, 0x00, 0,  1, FTT_RWOC|FTT_RTOO, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.7",        0,  0, 0x00, 0,  1,        0|FTT_RTOO, 1, EXB_MAX_BLKSIZE},
        { 0, },
    }},
    {"AIX", "DLT4", "SCSI", FTT_FLAG_HOLD_SIGNALS|FTT_FLAG_SUID_SCSI, 
	FTT_OP_STATUS|FTT_OP_GET_STATUS|FTT_OP_ERASE,ftt_trans_table_AIX, DLT_density_trans,
	"dev/rmt%d", "dev/rmt%d", 1, AIXfind,  {
    /*   string                  den mod hwd   pas fxd rewind            1st */
    /*   ======                  === === ===   === === ======            === */
    /* Default, passthru */
        { "dev/rmt%d.5",        5,  0, 0x1A, 0,  0,                          0, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.5",       -1,  0, 0x00, 1,  0,                          0, 0, EXB_MAX_BLKSIZE},
    /* Other Densities */
        { "dev/rmt%d.5",        4,  0, 0x80, 0,  0,                          0, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.5",        4,  1, 0x81, 0,  0,                          0, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.5",        5,  0, 0x82, 0,  0,                          0, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.5",        5,  1, 0x83, 0,  0,                          0, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.5",        0,  0, 0x0A, 0,  0,                   FTT_RDNW, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.5",        1,  0, 0x16, 0,  0,                   FTT_RDNW, 0, EXB_MAX_BLKSIZE},
    /* Variable useable */
        { "dev/rmt%d.5",        5,  1, 0x1A, 0,  0,                          0, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.5",        4,  0, 0x19, 0,  0,                          0, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.5",        4,  1, 0x19, 0,  0,                          0, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.5",        3,  0, 0x18, 0,  0,                          0, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.5",        2,  0, 0x17, 0,  0,                          0, 0, EXB_MAX_BLKSIZE},
    /* Fixed Usable */
        { "dev/rmt%d.5",        5,  0, 0x1A, 0,  1,                          0, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.5",        4,  0, 0x19, 0,  1,                          0, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.5",        5,  1, 0x1A, 0,  1,                          0, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.5",        4,  1, 0x19, 0,  1,                          0, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.5",        3,  0, 0x18, 0,  1,                          0, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.5",        2,  0, 0x17, 0,  1,                          0, 0, EXB_MAX_BLKSIZE},
    /* Descriptive */
        { "dev/rmt%d",         -1,  0, 0x00, 0,  1, FTT_RWOC|       0, 	 1, 512},
        { "dev/rmt%d.1",       -1,  0, 0x00, 0,  1,                 0, 	 1, 512},
        { "dev/rmt%d.2",       -1,  0, 0x00, 0,  1, FTT_RWOC|FTT_RTOO, 	 1, 512},
        { "dev/rmt%d.3",       -1,  0, 0x00, 0,  1,          FTT_RTOO, 	 1, 512},
        { "dev/rmt%d.4",        5,  0, 0x1A, 0,  0, FTT_RWOC|       0|       0, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.6",        5,  0, 0x1A, 0,  0, FTT_RWOC|FTT_RTOO|       0, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.7",        5,  0, 0x1A, 0,  0,          FTT_RTOO|       0, 1, EXB_MAX_BLKSIZE},
        { 0, },
    }},
    {"AIX", "DLT2", "SCSI", FTT_FLAG_HOLD_SIGNALS|FTT_FLAG_SUID_SCSI, 
	FTT_OP_STATUS|FTT_OP_GET_STATUS|FTT_OP_ERASE,ftt_trans_table_AIX, DLT_density_trans,
	"dev/rmt%d","dev/rmt%d", 1, AIXfind,  {
    /*   string                  den mod hwd   pas fxd rewind                   1st blk */
    /*   ======                  === === ===   === === ======                   === === */
    /* Default, passthru */
        { "dev/rmt%d.5",        4,  0, 0x19, 0,  0,                          0, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.5",       -1,  0, 0x00, 1,  0,                          0, 0, EXB_MAX_BLKSIZE},
    /* Variable useable */
        { "dev/rmt%d.5",        4,  1, 0x19, 0,  0,                          0, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.5",        3,  0, 0x18, 0,  0,                          0, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.5",        2,  0, 0x17, 0,  0,                          0, 0, EXB_MAX_BLKSIZE},
    /* Fixed Usable */
        { "dev/rmt%d.5",        4,  0, 0x19, 0,  1,                          0, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.5",        4,  1, 0x19, 0,  1,                          0, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.5",        3,  0, 0x18, 0,  1,                          0, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.5",        2,  0, 0x17, 0,  1,                          0, 0, EXB_MAX_BLKSIZE},
    /* Other Densities */
        { "dev/rmt%d.5",        0,  0, 0x0A, 0,  0,                   FTT_RDNW, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.5",        1,  0, 0x16, 0,  0,                   FTT_RDNW, 0, EXB_MAX_BLKSIZE},
    /* Descriptive */
        { "dev/rmt%d",         -1,  0, 0x00, 0,  1, FTT_RWOC|       0, 	 1, 512},
        { "dev/rmt%d.1",       -1,  0, 0x00, 0,  1,                 0, 	 1, 512},
        { "dev/rmt%d.2",       -1,  0, 0x00, 0,  1, FTT_RWOC|FTT_RTOO, 	 1, 512},
        { "dev/rmt%d.3",       -1,  0, 0x00, 0,  1,          FTT_RTOO, 	 1, 512},
        { "dev/rmt%d.4",        4,  0, 0x19, 0,  0, FTT_RWOC|       0|       0, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.6",        4,  0, 0x19, 0,  0, FTT_RWOC|FTT_RTOO|       0, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.7",        4,  0, 0x19, 0,  0,          FTT_RTOO|       0, 1, EXB_MAX_BLKSIZE},
        { 0, },
    }},
    {"AIX", "", "SCSI", FTT_FLAG_HOLD_SIGNALS|FTT_FLAG_SUID_SCSI, 
	FTT_OP_STATUS|FTT_OP_GET_STATUS,ftt_trans_table_AIX, Generic_density_trans,
	"dev/rmt%d","dev/rmt%d", 1, AIXfind,  {
    /*   string                den mod hwd   pas fxd rewind            1st */
    /*   ======                === === ===   === === ======            === */
    /* Default, passthru */
        { "dev/rmt%d.1",        1,  0, 0x00, 0,  0,                 0, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.1",       -1,  0,   -1, 1,  0,                 0, 0, EXB_MAX_BLKSIZE},
    /* Variable Usable */
        { "dev/rmt%d.5",        0,  0, 0x00, 0,  0,                 0, 1, EXB_MAX_BLKSIZE},
    /* Fixed Usable */
        { "dev/rmt%d.5",        0,  0, 0x00, 0,  1,                 0, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.1",        1,  0, 0x00, 0,  1,                 0, 0, EXB_MAX_BLKSIZE},
    /* Descriptive */
        { "dev/rmt%d.4",        0,  0, 0x00, 0,  1, FTT_RWOC|       0, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.6",        0,  0, 0x00, 0,  1, FTT_RWOC|FTT_RTOO, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.7",        0,  0, 0x00, 0,  1,          FTT_RTOO, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%d",          1,  0, 0x00, 0,  1, FTT_RWOC|       0, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%d",          1,  0, 0x00, 0,  1, FTT_RWOC|       0, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.2",        1,  0, 0x00, 0,  1, FTT_RWOC|FTT_RTOO, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.3",        1,  0, 0x00, 0,  1,        0|FTT_RTOO, 1, EXB_MAX_BLKSIZE},
        { 0, },
    }},
    /* note that %*[rmt] matches "rmt" or "mt" (or mrt, so sue me) -- mengel */
    /*      all of which we re-write into rmt */
    {"IRIX", "EXB-82","SCSI",  FTT_FLAG_CHK_BOT_AT_FMK|FTT_FLAG_BSIZE_AFTER, 
	FTT_OP_GET_STATUS, ftt_trans_table, Exabyte_density_trans,
	"%*[rmt]/tps%dd%d%*[nrsv.]890%d","rmt/tps%dd%d.8900", 3, IRIXfind,  {
	    /*   string             den mod hwd  pas fxd rewind        sf,1st */
	    /*   ======             === === ===  === === ======        ==  = */
    /* Default, Passthru */
	{ "rmt/tps%dd%dnrv",         2,  1,0x27, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "scsi/sc%dd%dl0",         -1,  0,  -1, 1,  0,                 0, 1, EXB_MAX_BLKSIZE},
    /* Usable Variable */
	{ "rmt/tps%dd%dnrnsv",       2,  1,0x27, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
    /* Usable Fixed */
	{ "rmt/tps%dd%dnr",          2,  1,0x27, 0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrns",        2,  1,0x27, 0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
    /* Descriptive */
	{ "rmt/tps%dd%dstat",       -1,  0,  -1, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%d",            2,  1,0x27, 0,  1,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%d",            1,  0,0x15, 0,  1,          FTT_RWOC, 0, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%d",            1,  1,0x90, 0,  1,          FTT_RWOC, 0, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%d",            0,  0,0x14, 0,  1,          FTT_RWOC, 0, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%d",            0,  1,0x8c, 0,  1,          FTT_RWOC, 0, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnr",          2,  1,0x27, 0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrs",         2,  1,0x27, 0,  1,          FTT_BTSW, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrsv",        2,  1,0x27, 0,  0,          FTT_BTSW, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dns",          2,  1,0x27, 0,  1,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnsv",         2,  1,0x27, 0,  0,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%ds",           2,  1,0x27, 0,  1, FTT_BTSW|FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dsv",          2,  1,0x27, 0,  0, FTT_BTSW|FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dv",           2,  1,0x27, 0,  0,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ 0,},
    }},
    {"IRIX", "DLT", "SCSI", FTT_FLAG_CHK_BOT_AT_FMK|FTT_FLAG_BSIZE_AFTER, 
	FTT_OP_GET_STATUS|FTT_OP_ERASE, ftt_trans_table, DLT_density_trans,
	"%*[rmt]/tps%dd%d","rmt/tps%dd%d", 2, IRIXfind, {
    /*   string                    den mod hwd   pas fxd rewind            ,1st*/
    /*   ======                    === === ===   === === ======            === */
    /* Default, passthru */
	{ "rmt/tps%dd%dnrv",   5,  0,0x1A,   0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "scsi/sc%dd%dl0",   -1,  0,  -1,   1,  0,                 0, 1, EXB_MAX_BLKSIZE},
    /* Usable Variable */
	{ "rmt/tps%dd%dnrvc",  5,  1,0x1A, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrnsvc",5,  1,0x1A, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrv",   4,  0,0x19, 0,  0,                 0, 0, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrnsvc",4,  1,0x19, 0,  0,                 0, 0, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrv",   3,  0,0x18, 0,  0,                 0, 0, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrv",   2,  0,0x17, 0,  0,                 0, 0, IRIX_MAX_BLKSIZE},
    /* Usable Fixed */
	{ "rmt/tps%dd%dnr",    5,  0,0x1A, 0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrc",   5,  1,0x1A, 0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnr",    4,  0,0x19, 0,  1,                 0, 0, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrc",   4,  1,0x19, 0,  1,                 0, 0, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnr",    3,  0,0x18, 0,  1,                 0, 0, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnr",    3,  0,0x17, 0,  1,                 0, 0, IRIX_MAX_BLKSIZE},
    /* Other Densities */
	{ "rmt/tps%dd%d",      4,  0,0x80, 0,  1,          FTT_RDNW, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dc",     4,  1,0x81, 0,  1,          FTT_RDNW, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%d",      5,  0,0x82, 0,  1,          FTT_RDNW, 0, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dc",     5,  1,0x83, 0,  1,          FTT_RDNW, 0, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%d",      0,  0,0x0A, 0,  1,          FTT_RDNW, 0, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%d",      1,  0,0x16, 0,  1,          FTT_RDNW, 0, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%d",      2,  0,0x17, 0,  1,          FTT_RDNW, 0, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%d",      3,  0,0x18, 0,  1,          FTT_RDNW, 0, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%d",      4,  0,0x19, 0,  1,          FTT_RDNW, 0, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%d",      5,  0,0x1A, 0,  1,          FTT_RDNW, 0, IRIX_MAX_BLKSIZE},
    /* Descriptive */
	{ "rmt/tps%dd%dstat", -1,  0,  -1, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrns",  5,  0,0x1A, 0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrnsc", 5,  1,0x1A, 0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrnsv", 5,  0,0x1A, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrs" ,  5,  0,0x1A, 0,  1,          FTT_BTSW, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrsc" , 5,  1,0x1A, 0,  1,          FTT_BTSW, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrsv",  5,  0,0x1A, 0,  0,          FTT_BTSW, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrsvc", 5,  1,0x1A, 0,  0,          FTT_BTSW, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dns",    5,  0,0x1A, 0,  1,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnsc",   5,  1,0x1A, 0,  1,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnsv",   5,  0,0x1A, 0,  0,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnsvc",  5,  1,0x1A, 0,  0,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%ds",     5,  0,0x1A, 0,  1, FTT_BTSW|FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dsc",    5,  1,0x1A, 0,  1, FTT_BTSW|FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dsv",    5,  0,0x1A, 0,  0, FTT_BTSW|FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dsvc",   5,  1,0x1A, 0,  0, FTT_BTSW|FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dv",     5,  0,0x1A, 0,  0,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dvc",    5,  1,0x1A, 0,  0,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
        { 0,},
    }},
    {"IRIX", "EXB-85", "SCSI", FTT_FLAG_CHK_BOT_AT_FMK|FTT_FLAG_BSIZE_AFTER, 
	FTT_OP_GET_STATUS, ftt_trans_table, Exabyte_density_trans,
	"%*[rmt]/tps%dd%d","rmt/tps%dd%d",  2, IRIXfind,  {
	    /*   string                  den mod hwd   pas fxd rewind            1st */
	    /*   ======                  === === ===   === === ======            === */
     /* Default, passthru */
	{ "rmt/tps%dd%dnrnsv.8500",  1,  0,0x15, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "scsi/sc%dd%dl0",         -1,  0,  -1, 1,  0,                 0, 1, EXB_MAX_BLKSIZE},
     /* Usable Variable */
	{ "rmt/tps%dd%dnrnsv.8200",  0,  0,0x14, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrnsv.8200c", 0,  1,0x8C, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrnsv.8500c", 1,  1,0x90, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
     /* Usable Fixed */
	{ "rmt/tps%dd%dnrns.8200",   0,  0,0x14, 0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrns.8200c",  0,  1,0x8C, 0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrns.8500",   1,  0,0x15, 0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrns.8500c",  1,  1,0x90, 0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
     /* Descriptive */
	{ "rmt/tps%dd%dstat",       -1,  0,  -1, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%d",            1,  0,0x15, 0,  1,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%d.8200",       0,  0,0x8C, 0,  1,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%d.8200c",      0,  0,0x14, 0,  1,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%d.8500",       1,  0,0x15, 0,  1,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%d.8500c",      1,  0,0x90, 0,  1,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnr",          1,  0,0x15, 0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnr.8200",     0,  0,0x14, 0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnr.8200c",    0,  1,0x8C, 0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnr.8500",     1,  0,0x15, 0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnr.8500c",    1,  1,0x90, 0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrns",        1,  0,0x15, 0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrnsv",       1,  0,0x15, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrnsv.8200",  0,  0,0x14, 0,  0,                 0, 0, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrnsv.8200c", 0,  1,0x8C, 0,  0,                 0, 0, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrnsv.8500",  1,  0,0x15, 0,  0,                 0, 0, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrnsv.8500c", 1,  1,0x90, 0,  0,                 0, 0, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrs",         1,  0,0x15, 0,  1,          FTT_BTSW, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrs.8200",    0,  0,0x14, 0,  1,          FTT_BTSW, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrs.8200c",   0,  1,0x8C, 0,  1,          FTT_BTSW, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrs.8500",    1,  0,0x15, 0,  1,          FTT_BTSW, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrs.8500c",   1,  1,0x90, 0,  1,          FTT_BTSW, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrsv",        1,  0,0x15, 0,  0,          FTT_BTSW, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrsv.8200",   0,  0,0x14, 0,  0,          FTT_BTSW, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrsv.8200c",  0,  1,0x8C, 0,  0,          FTT_BTSW, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrsv.8500",   1,  0,0x15, 0,  0,          FTT_BTSW, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrsv.8500c",  1,  1,0x90, 0,  0,          FTT_BTSW, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrv",         1,  0,0x15, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrv.8200",    1,  0,0x15, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrv.8200c",   1,  1,0x8C, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrv.8500",    1,  0,0x15, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrv.8500c",   1,  1,0x90, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dns",          1,  0,0x15, 0,  1,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dns.8200",     0,  0,0x14, 0,  1,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dns.8200c",    0,  1,0x8C, 0,  1,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dns.8500",     1,  0,0x15, 0,  1,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dns.8500c",    1,  1,0x90, 0,  1,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnsv",         1,  0,0x15, 0,  0,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnsv.8200",    0,  0,0x14, 0,  0,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnsv.8200c",   0,  1,0x8C, 0,  0,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnsv.8500",    1,  0,0x15, 0,  0,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnsv.8500c",   1,  1,0x90, 0,  0,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%ds",           1,  0,0x15, 0,  1, FTT_BTSW|FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%ds.8200",      0,  0,0x14, 0,  1, FTT_BTSW|FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%ds.8200c",     0,  1,0x8C, 0,  1, FTT_BTSW|FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%ds.8500",      1,  0,0x15, 0,  1, FTT_BTSW|FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%ds.8500c",     1,  1,0x90, 0,  1, FTT_BTSW|FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dsv",          1,  0,0x15, 0,  0, FTT_BTSW|FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dsv.8200",     0,  0,0x14, 0,  0, FTT_BTSW|FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dsv.8200c",    0,  1,0x8C, 0,  0, FTT_BTSW|FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dsv.8500",     1,  0,0x15, 0,  0, FTT_BTSW|FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dsv.8500c",    1,  1,0x90, 0,  0, FTT_BTSW|FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dv",           1,  0,0x15, 0,  0,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dv.8200",      0,  0,0x14, 0,  0,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dv.8200c",     0,  1,0x8C, 0,  0,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dv.8500",      1,  0,0x15, 0,  0,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dv.8500c",     1,  1,0x90, 0,  0,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dv.8500",      1,  0,0x00, 0,  0,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
        { 0,},
    }},
    {"IRIX", "EXB-82","SCSI",  FTT_FLAG_CHK_BOT_AT_FMK|FTT_FLAG_BSIZE_AFTER, 
	FTT_OP_GET_STATUS, ftt_trans_table, Exabyte_density_trans,
	"%*[rmt]/tps%dd%d","rmt/tps%dd%d", 2, IRIXfind,  {
	    /*   string                  den mod hwd  pas fxd rewind   sf,1st */
	    /*   ======                  === === ===  === === ======   === */
    /* Default, Passthru */
	{ "rmt/tps%dd%dnrv",         0,  0,   0, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "scsi/sc%dd%dl0",         -1,  0,  -1, 1,  0,                 0, 1, EXB_MAX_BLKSIZE},
    /* Usable Variable */
	{ "rmt/tps%dd%dnrnsv",       0,  0,   0, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
    /* Usable Fixed */
	{ "rmt/tps%dd%dnrns",        0,  0,   0, 0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
    /* Descriptive */
	{ "rmt/tps%dd%dstat",       -1,  0,  -1, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%d",            0,  0,   0, 0,  1,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnr",          0,  0,   0, 0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrs",         0,  0,   0, 0,  1,          FTT_BTSW, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrsv",        0,  0,   0, 0,  0,          FTT_BTSW, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dns",          0,  0,   0, 0,  1,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnsv",         0,  0,   0, 0,  0,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%ds",           0,  0,   0, 0,  1, FTT_BTSW|FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dsv",          0,  0,   0, 0,  0, FTT_BTSW|FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dv",           0,  0,   0, 0,  0,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ 0,},
    }},
    {"IRIX", "EXB-85", "JAG", FTT_FLAG_CHK_BOT_AT_FMK|FTT_FLAG_BSIZE_AFTER, 
	FTT_OP_GET_STATUS, ftt_trans_table, Exabyte_density_trans,
	"%*[rmt]/jag%dd%d","rmt/jag%dd%d", 2, IRIXfindVME, {
	    /*   string                  den mod hwd pas fxd rewind            1st */
	    /*   ======                  === === === === === ======            === */
    /* Default, passthru */
	{ "rmt/jag%dd%dnrv.8500",    1,  0,0x15, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "scsi/jag%dd%dl0",        -1,  0,  -1, 1,  0,                 0, 1, IRIX_MAX_BLKSIZE},
    /* Usable Variable */
	{ "rmt/jag%dd%dnrv.8200",    0,  0,0x14, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
    /* Usable Fixed */
	{ "rmt/jag%dd%dnr.8500",     1,  0,0x15, 0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/jag%dd%dnr.8200",     0,  0,0x14, 0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
    /* Descriptive */
	{ "rmt/jag%dd%dstat",       -1,  0,0x15, 0,  1,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/jag%dd%d",            1,  0,0x15, 0,  1,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/jag%dd%d.8200",       0,  0,0x14, 0,  1,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/jag%dd%d.8500",       1,  0,0x15, 0,  1,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/jag%dd%dnr",          1,  0,0x15, 0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/jag%dd%dnrv",         1,  0,0x15, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/jag%dd%dv",           1,  0,0x15, 0,  0,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/jag%dd%dv.8200",      0,  0,0x14, 0,  0,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/jag%dd%dv.8500",      1,  0,0x15, 0,  0,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/jag%dd%dv",           1,  0,0x00, 0,  0,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
        { 0,},
    }},
    {"IRIX", "EXB-82","JAG",  FTT_FLAG_CHK_BOT_AT_FMK|FTT_FLAG_BSIZE_AFTER, 
	FTT_OP_GET_STATUS, ftt_trans_table, Exabyte_density_trans,
	"%*[rmt]/ja8900g%dd%d","rmt/ja8900g%dd%d", 2, IRIXfindVME, {
	    /*   string             den mod hwd  pas fxd rewind        sf,1st */
	    /*   ======             === === ===  === === ======        ==  = */
    /* Default, Passthru */
	{ "rmt/jag%dd%dnrv",         2,  1,0x27, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "scsi/sc%dd%dl0",         -1,  0,  -1, 1,  0,                 0, 1, EXB_MAX_BLKSIZE},
    /* Usable Variable */
	{ "rmt/jag%dd%dnrnsv",       2,  1,0x27, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
    /* Usable Fixed */
	{ "rmt/jag%dd%dnr",          2,  1,0x27, 0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/jag%dd%dnrns",        2,  1,0x27, 0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
    /* Descriptive */
	{ "rmt/jag%dd%dstat",       -1,  0,  -1, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/jag%dd%d",            2,  1,0x27, 0,  1,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/jag%dd%d",            1,  0,0x15, 0,  1,          FTT_RWOC, 0, IRIX_MAX_BLKSIZE},
	{ "rmt/jag%dd%d",            1,  1,0x90, 0,  1,          FTT_RWOC, 0, IRIX_MAX_BLKSIZE},
	{ "rmt/jag%dd%d",            0,  0,0x14, 0,  1,          FTT_RWOC, 0, IRIX_MAX_BLKSIZE},
	{ "rmt/jag%dd%d",            0,  1,0x8c, 0,  1,          FTT_RWOC, 0, IRIX_MAX_BLKSIZE},
	{ "rmt/jag%dd%dnr",          2,  1,0x27, 0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/jag%dd%dnrs",         2,  1,0x27, 0,  1,          FTT_BTSW, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/jag%dd%dnrsv",        2,  1,0x27, 0,  0,          FTT_BTSW, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/jag%dd%dns",          2,  1,0x27, 0,  1,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/jag%dd%dnsv",         2,  1,0x27, 0,  0,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/jag%dd%ds",           2,  1,0x27, 0,  1, FTT_BTSW|FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/jag%dd%dsv",          2,  1,0x27, 0,  0, FTT_BTSW|FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/jag%dd%dv",           2,  1,0x27, 0,  0,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ 0,},
    }},
    {"IRIX", "EXB-82","JAG",  FTT_FLAG_CHK_BOT_AT_FMK|FTT_FLAG_BSIZE_AFTER, 
	FTT_OP_GET_STATUS, ftt_trans_table, Exabyte_density_trans,
	"%*[rmt]/jag%dd%d","rmt/jag%dd%d", 2, IRIXfindVME, {
    /*   string                          den mod hwd pas fxd rewind         sf,1st */
    /*   ======                          === === === === === ======         === */
	/* Default, passthru */
	{ "rmt/jag%dd%dnrv",         0,  0,   0, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "scsi/jag%dd%dl0",        -1,  0,  -1, 1,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	/* Usable Fixed */
	{ "rmt/jag%dd%dnr",          0,  0,   0, 0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
	/* Descriptive */
	{ "rmt/jag%dd%dstat",       -1,  0,   0, 0,  1,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/jag%dd%d",            0,  0,   0, 0,  1,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/jag%dd%dv",           0,  0,   0, 0,  0,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ 0,},
    }},
    {"IRIX", "DLT","JAG",  FTT_FLAG_CHK_BOT_AT_FMK|FTT_FLAG_BSIZE_AFTER, 
	FTT_OP_GET_STATUS, ftt_trans_table, DLT_density_trans,
	"%*[rmt]/jag%dd%d","rmt/jag%dd%d", 2, IRIXfindVME, {
    /*   string                     den mod hwd pas fxd rewind         sf,1st */
    /*   ======                     === === === === === ======         === */
	/* Default, passthru */
	{ "rmt/jag%dd%dnrv",         5,  0, 0x1A,0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "scsi/jag%dd%dl0",        -1,  0,  -1, 1,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	/* Usable Fixed */
	{ "rmt/jag%dd%dnr",          5,  0, 0x1A,0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/jag%dd%dnr",          4,  0, 0x19,0,  1,                 0, 0, IRIX_MAX_BLKSIZE},
	{ "rmt/jag%dd%dnr",          3,  0, 0x18,0,  1,                 0, 0, IRIX_MAX_BLKSIZE},
	{ "rmt/jag%dd%dnr",          2,  0, 0x17,0,  1,                 0, 0, IRIX_MAX_BLKSIZE},
	/* Descriptive */
	{ "rmt/jag%dd%dstat",       -1,  0,  -1, 0,  1,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/jag%dd%d",            5,  0, 0x1A,0,  1,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/jag%dd%dv",           5,  0, 0x1A,0,  0,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ 0,},
    }},
    {"IRIX", "","JAG",  FTT_FLAG_CHK_BOT_AT_FMK|FTT_FLAG_BSIZE_AFTER, 
	FTT_OP_GET_STATUS, ftt_trans_table, Generic_density_trans,
	"%*[rmt]/jag%dd%d","rmt/jag%dd%d", 2, IRIXfindVME, {
    /*   string                          den mod hwd pas fxd rewind         sf,1st */
    /*   ======                          === === === === === ======         === */
	/* Default, passthru */
	{ "rmt/jag%dd%dnrv",         0,  0,   0, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "scsi/jag%dd%dl0",        -1,  0,  -1, 1,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	/* Usable Fixed */
	{ "rmt/jag%dd%dnr",          0,  0,   0, 0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
	/* Descriptive */
	{ "rmt/jag%dd%dstat",       -1,  0,   0, 0,  1,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/jag%dd%d",            0,  0,   0, 0,  1,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/jag%dd%dv",           0,  0,   0, 0,  0,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ 0,},
    }},
    {"IRIX", "","SCSI",  FTT_FLAG_CHK_BOT_AT_FMK|FTT_FLAG_BSIZE_AFTER, 
	FTT_OP_GET_STATUS, ftt_trans_table, Generic_density_trans,
	"%*[rmt]/tps%dd%d","rmt/tps%dd%d", 2, IRIXfind,  {
	    /*   string                  den mod hwd  pas fxd rewind            sf,1st */
	    /*   ======                  === === ===  === === ======            === */
    /* Default, Passthru */
	{ "rmt/tps%dd%dnrv",         0,  0,   0, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "scsi/sc%dd%dl0",         -1,  0,  -1, 1,  0,                 0, 1, EXB_MAX_BLKSIZE},
    /* Usable Variable */
	{ "rmt/tps%dd%dnrnsv",       0,  0,   0, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
    /* Usable Fixed */
	{ "rmt/tps%dd%dnrns",        0,  0,   0, 0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
    /* Descriptive */
	{ "rmt/tps%dd%dstat",       -1,  0,  -1, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%d",            0,  0,   0, 0,  1,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnr",          0,  0,   0, 0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrs",         0,  0,   0, 0,  1,          FTT_BTSW, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrsv",        0,  0,   0, 0,  0,          FTT_BTSW, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dns",          0,  0,   0, 0,  1,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnsv",         0,  0,   0, 0,  0,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%ds",           0,  0,   0, 0,  1, FTT_BTSW|FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dsv",          0,  0,   0, 0,  0, FTT_BTSW|FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dv",           0,  0,   0, 0,  0,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ 0,},
    }},
{"WINNT", "", "", FTT_FLAG_BSIZE_AFTER|FTT_FLAG_MODE_AFTER,
	FTT_OP_GET_STATUS, ftt_trans_table, Exabyte_density_trans,
	".\\tape%d",".\\tape%d", 1, Win32find_dev, {
	 /* string		den mod hwd pas fxd rewind                   1st */
	 /* ======		=== === === === === ======                   === */
	{ ".\\tape%d",	 0,  0,	  0, 0,  0,                 0, 1, WIN_MAX_BLKSIZE},
	{ ".\\tape%d",	 0,  1,	  0, 0,  0,                 0, 0, WIN_MAX_BLKSIZE},
	{ ".\\tape%d",	 0,  0,	  0, 0,  1,                 0, 0, WIN_MAX_BLKSIZE},
	{ ".\\tape%d",	 0,  1,	  0, 0,  1,                 0, 0, WIN_MAX_BLKSIZE},
	{ ".\\tape%d",	-1,  0,	 -1, 1,  0,                 0, 0, WIN_MAX_BLKSIZE},
    { 0,},
    }},
#ifdef TABLE_TEST_TESTER
/* 
** the following two entries are for testing the table tester 
** these should get lots of errors
*/
    {"TABLE_TEST", "TABLE_TEST","NONE",  0, 0, 0, 0,
	"x/x%dx%d","x/x%dx%d", 2, IRIXfindVME, {
	/*   string      den mod hwd pas fxd rewind 1st */
	/*   ======      === === === === === ====== === */
	{ "x/x%dx%d",      0, 0,  0,  0,  0,  0,     0, SUN_MAX_BLKSIZE},
	{ "x/x%dx%d",      0, 0,  0,  0,  0,  0,     0, SUN_MAX_BLKSIZE},
	{ "x/x%d/%d/%d/%d",0, 0,  0,  0,  0,  0,     1, SUN_MAX_BLKSIZE},
	{ "x/x%d%d%d",     0, 0,  0,  0,  0,  0,     1, SUN_MAX_BLKSIZE},
	{ "x/x%g%d%g",     0, 0,  0,  0,  0,  0,     1, SUN_MAX_BLKSIZE},
	{ 0 , },
    }},
    {"TABLE_TEST", "TABLE_TEST","NONE",  0, 0, 0, 0,
	"x/x%dx%d","x/x%dx%d", 2, IRIXfindVME, {
	/*   string   den mod hwd pas fxd rewind 1st */
	/*   ======   === === === === === ====== === */
	{ "foo%d%d",   0,  0,  0,  0,  0,  0,     1, SUN_MAX_BLKSIZE},
	{ 0 , },
    }},
#endif
/* 
** Generic we-dont-know-what-it-is device
*/
    {"", "", "unknown", FTT_FLAG_REOPEN_AT_EOF, 0, ftt_trans_table, 
	Generic_density_trans, "%s", "%s", 1, "echo", {
	/*   string   den mod hwd    pas fxd rewind 1st */
	/*   ======   === === ===    === === ====== === */
	{ "%s",      0,  0,  0,  0,  0,  0,     1, SUN_MAX_BLKSIZE},
	{ 0,},
    }},
    {0, },
};

int devtable_size = sizeof(devtable);


ftt_stat_entry ftt_stat_op_tab[] = {
    {"EXB-8200", 
	FTT_DO_TUR|FTT_DO_INQ|FTT_DO_MS|FTT_DO_RS|FTT_DO_EXBRS|
	FTT_DO_EXB82FUDGE},

    {"EXB-8510", 
	FTT_DO_TUR|FTT_DO_INQ|FTT_DO_MS|FTT_DO_RS|
	FTT_DO_EXBRS|FTT_DO_SN|FTT_DO_RP_SOMETIMES},

    {"EXB-8500", 
	FTT_DO_TUR|FTT_DO_INQ|FTT_DO_MS|FTT_DO_RS|
	FTT_DO_EXBRS|FTT_DO_05RS|FTT_DO_SN|FTT_DO_RP_SOMETIMES},

    {"EXB-8505", 
	FTT_DO_TUR|FTT_DO_INQ|FTT_DO_MS|FTT_DO_MS_Px20_EXB|FTT_DO_RS|
	FTT_DO_EXBRS| FTT_DO_05RS|FTT_DO_SN|FTT_DO_LSRW|
	FTT_DO_RP_SOMETIMES},

    {"EXB-8205", 
	FTT_DO_TUR|FTT_DO_INQ|FTT_DO_MS|FTT_DO_MS_Px20_EXB|FTT_DO_RS|
	FTT_DO_EXBRS| FTT_DO_05RS|FTT_DO_SN|FTT_DO_LSRW|
	FTT_DO_RP_SOMETIMES},

    {"EXB-8900", 
	FTT_DO_TUR|FTT_DO_INQ|FTT_DO_MS|FTT_DO_RS|
	FTT_DO_EXBRS|FTT_DO_05RS|FTT_DO_SN|
	FTT_DO_LSRW|FTT_DO_RP_SOMETIMES},

    {"SD-3", 
	FTT_DO_TUR|FTT_DO_INQ|FTT_DO_MS|FTT_DO_RS|
	FTT_DO_LSRW|FTT_DO_RP},

    {"DLT",      
	FTT_DO_TUR|FTT_DO_INQ|FTT_DO_MS|FTT_DO_MS_Px10|FTT_DO_RS|
	FTT_DO_DLTRS|FTT_DO_SN|FTT_DO_LSRW|FTT_DO_LSC|FTT_DO_RP},

    {0,0}
};
